"""A file-backed store for the ledger: an append-only JSON Lines file, stdlib only.

``FileLedgerStore`` is a sink for ``DecisionLedger`` plus a loader that trusts nothing it reads: every line carries a
checksum, and the ledger is rebuilt through ``DecisionLedger.from_export`` so every hash and the chain are recomputed.
The README says what this does and does not give (durability across a process kill, not integrity by itself; fsync
semantics; one writer per directory).
"""
import hashlib
import json
import os
import threading
from typing import Any, Callable, List, Optional, Tuple

from .ledger import (
    _GENESIS_SEED,
    EXPORT_SCHEMA,
    DecisionLedger,
    LedgerCheckpoint,
    LedgerEntry,
    _decision_to_dict,
    _entry_from_dict,
)

try:  # POSIX only; where it is missing the store takes no cross-process lock (one writer is then purely a convention)
    import fcntl
except ImportError:  # pragma: no cover
    fcntl = None  # type: ignore[assignment]

LEDGER_FILE = "ledger.jsonl"
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


def _write_all(fd: int, data: bytes) -> None:
    view = memoryview(data)
    while view:
        written = os.write(fd, view)
        view = view[written:]


def _genesis() -> str:
    return hashlib.sha256(_GENESIS_SEED).hexdigest()


class FileLedgerStore:
    """``ledger.jsonl`` in ``directory``: one line per entry, ``{"v": 1, "entry": ..., "sum": ...}``.

    Use ``DecisionLedger(sink=store.sink)`` to record, ``load`` to rebuild, ``repair`` to cut a torn final line.
    One writer per directory is assumed (see the README).
    """

    def __init__(self, directory: Any) -> None:
        self.directory = os.fspath(directory)
        self.path = os.path.join(self.directory, LEDGER_FILE)
        self._lock = threading.Lock()

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
            os.makedirs(self.directory, exist_ok=True)
            created = not os.path.exists(self.path)
            fd = os.open(self.path, os.O_RDWR | os.O_APPEND | os.O_CREAT, 0o644)
            try:
                if fcntl is not None:
                    fcntl.flock(fd, fcntl.LOCK_EX)
                size = os.fstat(fd).st_size
                previous = self._last_entry(fd, size)
                if previous is not None and previous["audit_id"] == rendered["audit_id"]:
                    if previous != rendered:
                        raise StoreError(f"audit_id {rendered['audit_id']} is already stored with different content")
                    os.fsync(fd)
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
    def _last_entry(fd: int, size: int) -> Optional[dict]:
        if size == 0:
            return None
        if os.pread(fd, 1, size - 1) != b"\n":
            raise StoreError("the file ends in a torn line: run repair() before recording more")
        pos, tail = size - 1, b""
        while True:
            start = max(0, pos - 8192)
            chunk = os.pread(fd, pos - start, start)
            idx = chunk.rfind(b"\n")
            if idx >= 0:
                tail = chunk[idx + 1:] + tail
                break
            tail = chunk + tail
            pos = start
            if start == 0:
                break
        try:
            return _check_line(tail)
        except ValueError as exc:
            raise StoreError(f"the last stored line is damaged ({exc}): run repair() before recording more") from exc

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

    def load(self, require_context: bool = False, *, attach: bool = False) -> Tuple[DecisionLedger, dict]:
        """Rebuild the ledger from the file, trusting nothing: ``(ledger, report)``. Never writes.

        Every line's checksum is verified and the ledger is rebuilt through ``DecisionLedger.from_export``, so every
        hash and the chain are recomputed. A torn FINAL line (no trailing newline, invalid JSON or a bad checksum) is
        dropped and reported in ``report["recovered_tail"]`` as ``{"count", "bytes"}``; damage anywhere earlier, or a
        file that does not rebuild into a verifying chain, raises ``StoreCorruptError`` naming the line. A missing file
        is an empty ledger. ``attach=True`` makes the returned ledger record to this store, so a restarted process can
        carry on; the default returns a ledger with no sink.
        """
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
        if attach:
            ledger._sink = self.sink
        report = {"entries": len(entries), "valid_bytes": valid_bytes, "recovered_tail": tail}
        return ledger, report

    def repair(self) -> dict:
        """Truncate a torn final line, and only that, then fsync. Returns the load report plus ``repaired``.

        Refuses (``StoreCorruptError``) when the damage is not confined to the final line: nothing is truncated then.
        """
        _, report = self.load()
        tail = report["recovered_tail"]
        with self._lock:
            if tail["count"]:
                fd = os.open(self.path, os.O_RDWR)
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
    """One checkpoint in one file, meant to live on storage the ledger's writer cannot rewrite.

    Independence is the deployer's responsibility. A checkpoint kept beside the ledger file it protects is not
    independent: whoever can rewrite one can rewrite the other, and ``reconcile`` will then agree with the rewrite.
    ``write`` replaces the file atomically and only moves forward; with a ``sign`` callable it stores a signature
    over the domain tag and the canonical ``{"length", "head"}``. delta holds no key: ``sign`` and ``verify`` are the
    deployer's. One writer per file is assumed.
    """

    def __init__(self, path: Any) -> None:
        self.path = os.fspath(path)
        self._lock = threading.Lock()

    def exists(self) -> bool:
        return os.path.exists(self.path)

    @staticmethod
    def _signing_input(checkpoint: LedgerCheckpoint) -> bytes:
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

    def write(self, checkpoint: LedgerCheckpoint, *, sign: Optional[Callable[[bytes], bytes]] = None) -> None:
        """Replace the stored checkpoint atomically (temp file in the same directory, fsync, ``os.replace``, fsync of
        the directory). Forward-only: a lower length, or the same length with a different head, is a ``StoreError``;
        the same value is accepted. A corrupt existing file is also refused rather than overwritten."""
        if not isinstance(checkpoint, LedgerCheckpoint):
            raise TypeError(f"expected a LedgerCheckpoint; got {type(checkpoint).__name__}")
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
            os.makedirs(directory, exist_ok=True)
            if os.path.exists(self.path):
                current, _ = self._parse()
                if checkpoint.length < current.length:
                    raise StoreError(f"refused: length {checkpoint.length} is lower than the stored {current.length}")
                if checkpoint.length == current.length and checkpoint.head != current.head:
                    raise StoreError(f"refused: length {checkpoint.length} is stored with a different head")
            temp = f"{self.path}.tmp-{os.getpid()}-{threading.get_ident()}"
            fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
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


__all__ = ["FileLedgerStore", "CheckpointStore", "StoreError", "StoreCorruptError", "LEDGER_FILE", "CHECKPOINT_SIGNATURE_DOMAIN"]
