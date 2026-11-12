"""A file-backed store for the ledger: an append-only JSON Lines file, stdlib only.

``FileLedgerStore`` is a sink for ``DecisionLedger`` plus a loader that trusts nothing it reads: every line carries a
checksum, and the ledger is rebuilt through ``DecisionLedger.from_export`` so every hash and the chain are recomputed.
The README says what this does and does not give (durability across a process kill, not integrity by itself; fsync
semantics; one writer per directory).
"""
import errno
import hashlib
import json
import os
import tempfile
import threading
from typing import Any, Callable, List, Optional, Tuple

from .ledger import (
    _GENESIS_SEED,
    EXPORT_SCHEMA,
    DecisionLedger,
    LedgerCheckpoint,
    LedgerEntry,
    _decision_to_dict,
    _canonical_subset,
    _entry_from_dict,
    _hash_dict,
)
from .signing import strict_utf8

try:  # POSIX only; where it is missing the store takes no cross-process lock (one writer is then purely a convention)
    import fcntl
except ImportError:  # pragma: no cover
    fcntl = None  # type: ignore[assignment]

LEDGER_FILE = "ledger.jsonl"
_UNSET: Any = object()  # "load(attach=True) was not told what to compare with"
LOCK_SUFFIX = ".lock"
LINE_VERSION = 1
CHECKPOINT_SIGNATURE_DOMAIN = b"delta-ledger-checkpoint-signature/1\n"
_LINE_KEYS = frozenset({"v", "entry", "sum"})


class StoreError(Exception):
    """The store refused an operation and changed nothing it should not have."""


class StoreCorruptError(StoreError):
    """The file is damaged somewhere other than its final line, or does not rebuild into a verifying ledger."""


def _canonical(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def _sum(entry: Any) -> str:
    return hashlib.sha256(_canonical(entry).encode("ascii")).hexdigest()


def _no_constant(name: str) -> Any:
    raise ValueError(f"{name} is not valid JSON")


def _no_duplicates(pairs: List[Tuple[str, Any]]) -> dict:
    out: dict = {}
    for key, value in pairs:
        if key in out:
            raise ValueError(f"duplicate key {key!r}")
        out[key] = value
    return out


def _loads(raw: bytes) -> Any:
    return json.loads(raw.decode("utf-8"), parse_constant=_no_constant, object_pairs_hook=_no_duplicates)


def _entry_to_dict(entry: LedgerEntry) -> dict:
    """An entry exactly as ``DecisionLedger.export()`` renders it."""
    return {
        "audit_id": entry.audit_id,
        "timestamp": entry.timestamp.isoformat(),
        "previous_hash": entry.previous_hash,
        "immutable_hash": entry.immutable_hash,
        "run_id": entry.run_id,
        "event_id": entry.event_id,
        "decision": _decision_to_dict(entry.decision),
    }


def _render_line(entry: LedgerEntry) -> Tuple[bytes, dict]:
    """The bytes to append for an entry, and its rendered dict. Refuses an entry that would not load back."""
    if type(entry) is not LedgerEntry:
        raise StoreError(f"the store records LedgerEntry only; got {type(entry).__name__}")
    try:
        rendered = _entry_to_dict(entry)
        text = _canonical(rendered)
        # What is written must be what load accepts: judge the round-tripped form, not the live object.
        rendered = json.loads(text)
        _entry_from_dict(0, rendered, False)
    except (ValueError, TypeError, AttributeError) as exc:
        raise StoreError(f"this entry cannot be stored: {exc}") from exc
    line = _canonical({"v": LINE_VERSION, "entry": rendered, "sum": _sum(rendered)})
    return line.encode("ascii") + b"\n", rendered


def _check_line(raw: bytes) -> dict:
    """The entry dict of one stored line, or ``ValueError`` saying why the line is not good."""
    try:
        obj = _loads(raw)
    except (ValueError, UnicodeDecodeError) as exc:
        raise ValueError(f"not valid JSON ({exc})") from exc
    if type(obj) is not dict or set(obj) != _LINE_KEYS:
        raise ValueError("not an object with exactly the keys v, entry and sum")
    if type(obj["v"]) is not int or obj["v"] != LINE_VERSION:
        raise ValueError(f"unknown line version {obj['v']!r}")
    if type(obj["entry"]) is not dict:
        raise ValueError("entry is not an object")
    if type(obj["sum"]) is not str or obj["sum"] != _sum(obj["entry"]):
        raise ValueError("the checksum does not match the entry")
    return obj["entry"]


def _fsync_dir(directory: str) -> None:
    fd = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _make_directory(directory: str) -> None:
    """``os.makedirs``, then fsync the parent of every directory this call created, so the new names are durable."""
    directory = os.path.abspath(directory)
    missing = []
    probe = directory
    while not os.path.isdir(probe):
        missing.append(probe)
        parent = os.path.dirname(probe)
        if parent == probe:
            break
        probe = parent
    os.makedirs(directory, exist_ok=True)
    for created in reversed(missing):
        _fsync_dir(os.path.dirname(created))


def _write_all(fd: int, data: bytes) -> None:
    view = memoryview(data)
    while view:
        written = os.write(fd, view)
        view = view[written:]


def _genesis() -> str:
    return hashlib.sha256(_GENESIS_SEED).hexdigest()


def _verify_stored_hash(raw: dict) -> None:
    """``ValueError`` unless ``raw`` parses as an entry and its ``immutable_hash`` is what its content hashes to."""
    entry = _entry_from_dict(0, raw, False)
    computed = hashlib.sha256(
        (entry.previous_hash + _hash_dict(_canonical_subset(
            entry.audit_id, entry.timestamp, entry.decision, entry.run_id, entry.event_id))).encode()
    ).hexdigest()
    if computed != entry.immutable_hash:
        raise ValueError("immutable_hash does not match the entry's content")


class FileLedgerStore:
    """``ledger.jsonl`` in ``directory``: one line per entry, ``{"v": 1, "entry": ..., "sum": ...}``.

    Use ``DecisionLedger(sink=store.sink)`` to record, ``load`` to rebuild, ``repair`` to cut a torn final line.
    One writer per directory is assumed (see the README).
    """

    def __init__(self, directory: Any) -> None:
        self.directory = os.fspath(directory)
        self.path = os.path.join(self.directory, LEDGER_FILE)
        self._lock = threading.Lock()

    def _refuse_symlink(self) -> None:
        """A write never follows a symbolic link: the line would land outside the store and still be acknowledged."""
        if os.path.islink(self.path):
            raise StoreError(f"{self.path} is a symbolic link; the store does not write through it")

    def _open_no_follow(self, flags: int, mode: int = 0o644) -> int:
        """Open the ledger file for writing without following a link, so a link swapped in after ``_refuse_symlink`` is
        still refused (``O_NOFOLLOW`` makes that open fail with ELOOP). A link in a parent directory is followed."""
        try:
            return os.open(self.path, flags | getattr(os, "O_NOFOLLOW", 0), mode)
        except OSError as exc:
            if exc.errno == errno.ELOOP:
                raise StoreError(f"{self.path} is a symbolic link; the store does not write through it") from exc
            raise

    # ------------------------------------------------------------------ writing
    def sink(self, entry: LedgerEntry) -> None:
        """Append one entry and fsync it (and the directory, when the file is created) before returning.

        Idempotent by ``audit_id``: when the last line holds the same entry nothing is written (the file is still
        fsynced, so a retry after a failed fsync does not acknowledge an unsynced line). The same ``audit_id`` with
        different content, an entry that does not continue the last line's hash, a torn final line (run ``repair``)
        or a damaged final line is a ``StoreError`` and nothing is written.
        """
        line, rendered = _render_line(entry)
        with self._lock:
            _make_directory(self.directory)
            self._refuse_symlink()
            created = not os.path.exists(self.path)
            fd = self._open_no_follow(os.O_RDWR | os.O_APPEND | os.O_CREAT, 0o644)
            try:
                if fcntl is not None:
                    fcntl.flock(fd, fcntl.LOCK_EX)
                size = os.fstat(fd).st_size
                previous, only_line = self._last_entry(fd, size)
                if previous is not None and previous["audit_id"] == rendered["audit_id"]:
                    if previous != rendered:
                        raise StoreError(f"audit_id {rendered['audit_id']} is already stored with different content")
                    os.fsync(fd)
                    if only_line:
                        _fsync_dir(self.directory)  # the first attempt may be the one whose directory fsync failed
                    return
                expected = previous["immutable_hash"] if previous is not None else _genesis()
                if rendered["previous_hash"] != expected:
                    raise StoreError(
                        "the entry does not continue the stored chain (its previous_hash is not the last stored "
                        "entry's hash): another writer, an unreconciled entry or a ledger that was not loaded from this store"
                    )
                try:
                    _write_all(fd, line)
                except BaseException:
                    try:
                        os.ftruncate(fd, size)
                    except OSError:
                        pass
                    raise
                os.fsync(fd)
                if created or size == 0:
                    _fsync_dir(self.directory)
            finally:
                os.close(fd)

    @staticmethod
    def _last_lines(fd: int, size: int, count: int) -> List[bytes]:
        """The last ``count`` complete lines of the file (fewer when it has fewer), oldest first."""
        pos, buf = size - 1, b""  # size - 1 is the final newline, already checked
        while buf.count(b"\n") < count and pos > 0:
            start = max(0, pos - 8192)
            buf = os.pread(fd, pos - start, start) + buf
            pos = start
        pieces = buf.split(b"\n")
        if pos > 0:
            pieces = pieces[1:]  # the first piece is the end of a line that starts earlier
        return pieces[-count:]

    @classmethod
    def _last_entry(cls, fd: int, size: int) -> Tuple[Optional[dict], bool]:
        """The last stored entry, verified the way ``load`` would: parsed by the entry parser, its hash recomputed,
        and linked to the line before it. Anything else is a ``StoreError``; the sink checks the last lines only, so a file damaged earlier is still appended to: ``load`` or ``reconcile`` is what finds it.
        Returns ``(entry, only_line)``; ``only_line`` is true when the file holds exactly that one line."""
        if size == 0:
            return None, False
        if os.pread(fd, 1, size - 1) != b"\n":
            raise StoreError("the file ends in a torn line: run repair() before recording more")
        lines = cls._last_lines(fd, size, 2)
        try:
            entries = [_check_line(raw) for raw in lines]
        except ValueError as exc:
            raise StoreError(f"the last stored line is damaged ({exc}): run repair() before recording more") from exc
        expected = entries[0].get("immutable_hash") if len(entries) == 2 else _genesis()
        try:
            if len(entries) == 2:
                _verify_stored_hash(entries[0])
            _verify_stored_hash(entries[-1])
            if entries[-1]["previous_hash"] != expected:
                raise ValueError("previous_hash does not continue the chain")
        except (ValueError, KeyError, TypeError) as exc:
            raise StoreError(
                f"the last stored lines do not form a verifying chain ({exc}); load() would refuse this file, "
                "so nothing more is recorded to it"
            ) from exc
        return entries[-1], len(entries) == 1

    # ------------------------------------------------------------------ reading
    def _scan(self) -> Tuple[List[dict], int, dict]:
        """``(entries, valid_bytes, tail)``. Raises ``StoreCorruptError`` for damage before the final line. Never writes."""
        try:
            with open(self.path, "rb") as fh:
                data = fh.read()
        except FileNotFoundError:
            return [], 0, {"count": 0, "bytes": 0}
        complete = data.endswith(b"\n")
        pieces = data.split(b"\n")
        unterminated = pieces.pop()  # b"" when the file ends with a newline
        lines = pieces
        entries: List[dict] = []
        offset = 0
        tail_bytes = 0
        for number, raw in enumerate(lines, start=1):
            try:
                entries.append(_check_line(raw))
            except ValueError as exc:
                if number == len(lines) and complete:
                    tail_bytes = len(raw) + 1
                    break
                raise StoreCorruptError(f"line {number} of {self.path} is damaged before the end of the file: {exc}") from exc
            offset += len(raw) + 1
        if not complete:
            tail_bytes = len(unterminated)
        return entries, offset, {"count": 1 if tail_bytes else 0, "bytes": tail_bytes}

    def load(
        self,
        require_context: bool = False,
        *,
        attach: bool = False,
        against: Any = _UNSET,
        verify: Optional[Callable[[bytes, bytes], bool]] = None,
    ) -> Tuple[DecisionLedger, dict]:
        """Rebuild the ledger from the file, trusting nothing: ``(ledger, report)``. Never writes.

        Every line's checksum is verified and the ledger is rebuilt through ``DecisionLedger.from_export``, so every
        hash and the chain are recomputed. A torn FINAL line (no trailing newline, invalid JSON or a bad checksum) is
        dropped and reported in ``report["recovered_tail"]`` as ``{"count", "bytes"}``; damage anywhere earlier, or a
        file that does not rebuild into a verifying chain, raises ``StoreCorruptError`` naming the line. A missing file
        is an empty ledger. ``attach=True`` makes the returned ledger record to this store, so a restarted process can
        carry on; the default returns a ledger with no sink.

        ``attach=True`` must be told what to compare the file with, because a ledger rebuilt from a file that lost or
        rewrote acknowledged entries would otherwise carry on from the damage and later checkpoints would launder it:
        ``against=<CheckpointStore>`` runs the same comparison as ``reconcile`` (``verify`` is passed to the checkpoint
        read) and refuses to attach unless the status is ``OK``, ``UNWITNESSED_TAIL`` or ``EMPTY`` (``LOST``,
        ``DIVERGED`` and ``NO_CHECKPOINT`` raise ``StoreError`` naming the status; to bootstrap a store that has entries
        and no checkpoint, load without attaching, write ``ledger.checkpoint()`` to the checkpoint store, then attach).
        ``against=None`` attaches with no comparison and says so by name. Without ``attach``, ``against`` is only
        reported (``report["reconcile"]``) and does not refuse a status (an unreadable or unverifiable checkpoint still
        raises ``StoreError``). The comparison can only vouch for entries up to the checkpoint's length: entries recorded
        after the last checkpoint (``UNWITNESSED_TAIL``) can still be lost or rewritten unseen.
        """
        if against is not _UNSET and against is not None and not isinstance(against, CheckpointStore):
            raise TypeError(f"against must be a CheckpointStore or None; got {type(against).__name__}")
        if attach and against is _UNSET:
            raise StoreError(
                "load(attach=True) must be told what to compare the file with: against=<CheckpointStore> (refuses a "
                "store that lost or rewrote acknowledged entries) or against=None (no comparison)"
            )
        entries, valid_bytes, tail = self._scan()
        head = entries[-1]["immutable_hash"] if entries and type(entries[-1].get("immutable_hash")) is str else _genesis()
        try:
            ledger = DecisionLedger.from_export(
                {"schema": EXPORT_SCHEMA, "require_context": require_context, "chain_head": head, "entries": entries}
            )
        except ValueError as exc:
            text = str(exc)
            where = ""
            if text.startswith("entry "):
                digits = text[len("entry "):].split(":", 1)[0]
                if digits.isdigit():
                    where = f"line {int(digits) + 1} of {self.path}: "
            raise StoreCorruptError(f"{where}{text}") from exc
        report = {"entries": len(entries), "valid_bytes": valid_bytes, "recovered_tail": tail}
        if against is not _UNSET and against is not None:
            compared = _compare(ledger, tail, against, verify)
            report["reconcile"] = compared
            if attach and compared["status"] not in ("OK", "UNWITNESSED_TAIL", "EMPTY"):
                raise StoreError(
                    f"refused to attach: reconcile says {compared['status']} ({_describe(compared)}); the file does not "
                    "continue the history the independent checkpoint vouches for"
                )
        if attach:
            ledger._sink = self.sink
        return ledger, report

    def repair(self) -> dict:
        """Truncate a torn final line, and only that, then fsync. Returns the load report plus ``repaired``.

        Refuses (``StoreCorruptError``) when the damage is not confined to the final line: nothing is truncated then.
        """
        _, report = self.load()
        tail = report["recovered_tail"]
        with self._lock:
            if tail["count"]:
                self._refuse_symlink()
                fd = self._open_no_follow(os.O_RDWR)
                try:
                    if fcntl is not None:
                        fcntl.flock(fd, fcntl.LOCK_EX)
                    if os.fstat(fd).st_size != report["valid_bytes"] + tail["bytes"]:
                        raise StoreError("the file changed while it was being repaired; nothing was truncated")
                    os.ftruncate(fd, report["valid_bytes"])
                    os.fsync(fd)
                finally:
                    os.close(fd)
        report = dict(report)
        report["repaired"] = bool(tail["count"])
        return report


class CheckpointStore:
    """One checkpoint in one file, which only helps if it lives on storage the ledger file's attacker cannot rewrite.

    Independence is the deployer's responsibility. A checkpoint kept beside the ledger file it protects is not
    independent: whoever can rewrite one can rewrite the other, and ``reconcile`` will then agree with the rewrite.
    ``write`` replaces the file atomically and only moves forward; with a ``sign`` callable it stores a signature
    over the domain tag and the canonical ``{"length", "head"}``. delta holds no key: ``sign`` and ``verify`` are the
    deployer's. Concurrent writers are serialised by an exclusive flock on a lock file beside the checkpoint (POSIX; see the README).
    """

    def __init__(self, path: Any) -> None:
        self.path = os.fspath(path)
        self._lock = threading.Lock()

    def exists(self) -> bool:
        return os.path.exists(self.path)

    @staticmethod
    def _signing_input(checkpoint: LedgerCheckpoint) -> bytes:
        strict_utf8("checkpoint head", checkpoint.head)
        return CHECKPOINT_SIGNATURE_DOMAIN + _canonical({"length": checkpoint.length, "head": checkpoint.head}).encode("ascii")

    def _parse(self) -> Tuple[LedgerCheckpoint, Optional[bytes]]:
        try:
            with open(self.path, "rb") as fh:
                raw = fh.read()
        except FileNotFoundError:
            raise StoreError(f"no checkpoint at {self.path}") from None
        try:
            obj = _loads(raw)
        except (ValueError, UnicodeDecodeError) as exc:
            raise StoreError(f"the checkpoint file is not valid JSON: {exc}") from exc
        if type(obj) is not dict or set(obj) not in ({"v", "length", "head"}, {"v", "length", "head", "signature"}):
            raise StoreError("the checkpoint file must be an object with the keys v, length, head and optionally signature")
        if type(obj["v"]) is not int or obj["v"] != LINE_VERSION:
            raise StoreError(f"unknown checkpoint version {obj['v']!r}")
        try:
            checkpoint = LedgerCheckpoint.from_dict({"length": obj["length"], "head": obj["head"]})
        except ValueError as exc:
            raise StoreError(f"the checkpoint file is mistyped: {exc}") from exc
        if checkpoint.length < 1:
            raise StoreError("the checkpoint file names no entry (length 0)")
        signature = None
        if "signature" in obj:
            text = obj["signature"]
            try:
                if type(text) is not str or not text:
                    raise ValueError
                signature = bytes.fromhex(text)
            except ValueError:
                raise StoreError("the checkpoint signature must be a non-empty hex string") from None
        return checkpoint, signature

    def read(self, *, verify: Optional[Callable[[bytes, bytes], bool]] = None) -> LedgerCheckpoint:
        """The stored checkpoint, typed. Any corruption, wrong type or extra key is a ``StoreError``.

        With ``verify`` (callable(signing_input, signature) -> bool) a missing signature, a verifier that raises, or an
        answer other than exactly ``True`` is a ``StoreError`` naming the signature.
        """
        checkpoint, signature = self._parse()
        if verify is not None:
            if not callable(verify):
                raise TypeError(f"verify must be callable; got {type(verify).__name__}")
            if signature is None:
                raise StoreError("the checkpoint is not signed but a signature check was asked for")
            try:
                ok = verify(self._signing_input(checkpoint), signature) is True
            except Exception:
                ok = False
            if not ok:
                raise StoreError("the checkpoint signature does not verify")
        return checkpoint

    def write(
        self,
        checkpoint: LedgerCheckpoint,
        *,
        sign: Optional[Callable[[bytes], bytes]] = None,
        verify: Optional[Callable[[bytes, bytes], bool]] = None,
        replace_corrupt: bool = False,
        history: Optional[DecisionLedger] = None,
    ) -> None:
        """Replace the stored checkpoint atomically (temp file in the same directory, fsync, ``os.replace``, fsync of
        the directory). Forward-only: a lower length, or the same length with a different head, is a ``StoreError``;
        the same value is accepted. A corrupt existing file is also refused rather than overwritten.

        ``verify`` (callable(signing_input, signature) -> bool) judges the existing file too: one that does not parse,
        is unsigned or fails ``verify`` is corrupt, and ``StoreCorruptError`` is raised instead of treating its length as
        a floor (a planted huge checkpoint would otherwise block every later write). The remedy is an explicit
        ``replace_corrupt=True``, which replaces a corrupt file without the forward-only comparison; a file that is
        sound still gates forward-only either way.

        Forward-only by length alone is not enough: a ledger rebuilt from a file that lost or rewrote entries can be
        recorded past the old length and checkpointed. ``history`` (the ledger the new checkpoint was taken from) closes
        that: the new checkpoint must be the head of ``history`` at its own length, and when a sound checkpoint is
        stored, ``history`` must hold at least that many entries with the stored head at the stored length, that is, the
        new checkpoint extends the stored one. A checkpoint taken with ``DecisionLedger.checkpoint()`` carries the entries it
        was taken from and is checked the same way without ``history``; one built by hand or read from a dict has no lineage
        and is compared by length only."""
        if history is not None and not isinstance(history, DecisionLedger):
            raise TypeError(f"history must be a DecisionLedger; got {type(history).__name__}")
        if not isinstance(checkpoint, LedgerCheckpoint):
            raise TypeError(f"expected a LedgerCheckpoint; got {type(checkpoint).__name__}")
        # The history to check against: the one given, else the one the checkpoint was taken from (none for a checkpoint
        # built by hand or read from a dict, which is then compared by length only).
        source: Any = history.entries if history is not None else checkpoint.lineage
        try:
            checkpoint = LedgerCheckpoint.from_dict(checkpoint.to_dict())
        except ValueError as exc:
            raise StoreError(f"this checkpoint cannot be stored: {exc}") from exc
        if checkpoint.length < 1:
            raise StoreError("a checkpoint of an empty ledger names no entry and proves nothing; it is not stored")
        body = {"v": LINE_VERSION, "length": checkpoint.length, "head": checkpoint.head}
        if sign is not None:
            if not callable(sign):
                raise TypeError(f"sign must be callable; got {type(sign).__name__}")
            signature: Any = sign(self._signing_input(checkpoint))
            if type(signature) is not bytes:
                raise TypeError(f"the signer must return exactly bytes; got {type(signature).__name__}")
            if not signature:
                raise ValueError("the signer returned an empty signature")
            body["signature"] = signature.hex()
        data = (_canonical(body) + "\n").encode("ascii")
        directory = os.path.dirname(os.path.abspath(self.path))
        with self._lock:
            _make_directory(directory)
            lock_fd = os.open(self.path + LOCK_SUFFIX, os.O_RDWR | os.O_CREAT, 0o600)
            try:
                if fcntl is not None:
                    fcntl.flock(lock_fd, fcntl.LOCK_EX)  # held from the forward-only check through the directory fsync
                self._replace_forward_only(checkpoint, data, directory, verify, replace_corrupt, source)
            finally:
                os.close(lock_fd)  # closing releases the flock

    def _replace_forward_only(
        self, checkpoint: LedgerCheckpoint, data: bytes, directory: str,
        verify: Optional[Callable[[bytes, bytes], bool]], replace_corrupt: bool,
        source: Any = None,
    ) -> None:
        """The check and the replace; the caller holds the lock. ``source`` is the entries the new checkpoint is checked against."""
        current = None
        if os.path.exists(self.path):
            try:
                current = self.read(verify=verify) if verify is not None else self._parse()[0]
            except StoreError as exc:
                if verify is None and not replace_corrupt:
                    raise  # unchanged behaviour: a corrupt existing file is refused
                if not replace_corrupt:
                    raise StoreCorruptError(
                        f"the existing checkpoint at {self.path} is corrupt ({exc}); pass replace_corrupt=True to replace it"
                    ) from exc
        if current is not None:
            if checkpoint.length < current.length:
                raise StoreError(f"refused: length {checkpoint.length} is lower than the stored {current.length}")
            if checkpoint.length == current.length and checkpoint.head != current.head:
                raise StoreError(f"refused: length {checkpoint.length} is stored with a different head")
        if source is not None:
            try:
                entries = tuple(source)  # one read, so a list that answers differently later cannot matter
                if checkpoint.length > len(entries) or entries[checkpoint.length - 1].immutable_hash != checkpoint.head:
                    raise StoreError(
                        f"refused: the checkpoint at length {checkpoint.length} is not the head of the history it was taken from "
                        "(or of the given history) at that length"
                    )
                if current is not None and (
                    current.length > len(entries) or entries[current.length - 1].immutable_hash != current.head
                ):
                    raise StoreError(
                        f"refused: the new checkpoint does not extend the stored one (its history has {len(entries)} "
                        f"entries and does not hold the stored head at length {current.length})"
                    )
            except (AttributeError, IndexError, TypeError) as exc:
                raise StoreError(f"refused: the history given for this checkpoint cannot be read as ledger entries: {exc}") from exc
        fd, temp = tempfile.mkstemp(dir=directory, prefix=os.path.basename(self.path) + ".", suffix=".tmp")  # O_EXCL, 0600
        try:
            try:
                _write_all(fd, data)
                os.fsync(fd)
            finally:
                os.close(fd)
            os.replace(temp, self.path)
        except BaseException:
            try:
                os.unlink(temp)
            except OSError:
                pass
            raise
        _fsync_dir(directory)


def reconcile(
    store: FileLedgerStore,
    checkpoint_store: CheckpointStore,
    *,
    require_context: bool = False,
    verify: Optional[Callable[[bytes, bytes], bool]] = None,
) -> dict:
    """Compare the loaded file with the separately kept checkpoint. ``{"status": ..., ...}``; never writes or repairs.

    ``OK``: the checkpoint is covered by the loaded ledger and equals its prefix head. ``UNWITNESSED_TAIL``: the same,
    and ``unwitnessed`` entries follow the checkpoint (nothing independent vouches for them). ``LOST``: the checkpoint
    is ahead of the file, so entries acknowledged up to it are missing (``missing`` counts them). ``DIVERGED``: the
    head at the checkpoint's length differs, so the history was rewritten. ``NO_CHECKPOINT``: entries but no checkpoint
    file. ``EMPTY``: no entries and no checkpoint. Mid-file damage raises ``StoreCorruptError``, a corrupt checkpoint
    file raises ``StoreError`` (``verify``, when given, is passed to ``CheckpointStore.read``).

    This is only as strong as the checkpoint's independence: a checkpoint the file's writer can also rewrite agrees
    with whatever it was rewritten to.
    """
    ledger, report = store.load(require_context)
    return _compare(ledger, report["recovered_tail"], checkpoint_store, verify)


def _compare(
    ledger: DecisionLedger, recovered_tail: dict, checkpoint_store: CheckpointStore,
    verify: Optional[Callable[[bytes, bytes], bool]],
) -> dict:
    """The comparison ``reconcile`` returns and ``FileLedgerStore.load(against=...)`` acts on."""
    length = len(ledger)
    result: dict = {
        "status": "", "ledger_length": length, "checkpoint_length": None, "unwitnessed": 0,
        "recovered_tail": recovered_tail,
    }
    if not checkpoint_store.exists():
        result["status"] = "EMPTY" if length == 0 else "NO_CHECKPOINT"
        return result
    checkpoint = checkpoint_store.read(verify=verify)
    result["checkpoint_length"] = checkpoint.length
    if checkpoint.length > length:
        result["status"] = "LOST"
        result["missing"] = checkpoint.length - length
    elif ledger.entries[checkpoint.length - 1].immutable_hash != checkpoint.head:
        result["status"] = "DIVERGED"
    elif checkpoint.length < length:
        result["status"] = "UNWITNESSED_TAIL"
        result["unwitnessed"] = length - checkpoint.length
    else:
        result["status"] = "OK"
    return result


def _describe(result: dict) -> str:
    if result["status"] == "LOST":
        return f"the checkpoint is at length {result['checkpoint_length']}, the file holds {result['ledger_length']}"
    if result["status"] == "DIVERGED":
        return f"the file's head at length {result['checkpoint_length']} differs from the checkpoint's"
    return f"the file holds {result['ledger_length']} entries and no checkpoint is stored"


__all__ = [
    "FileLedgerStore", "CheckpointStore", "StoreError", "StoreCorruptError", "reconcile",
    "LEDGER_FILE", "CHECKPOINT_SIGNATURE_DOMAIN",
]
