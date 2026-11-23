"""The outside anchor: signed ledger checkpoints published, one file each, to a separate protected repository.

A checkpoint is only evidence if it is kept where the ledger's writer cannot rewrite it. The anchor is that place: a
separate git repository (for example a protected GitHub repository) whose main branch the ledger writer's account cannot
force-push, delete or rewrite. delta writes the files into a local clone; the operator commits and pushes them (see the
README for the steps). delta runs no git and holds no key.

What is published, and nothing else: ``schema``, ``length``, ``head`` and ``signature`` (hex). Never a Decision, never
an entry, never any data. The signature is over the same bytes ``CheckpointStore`` signs (domain tag
``delta-ledger-checkpoint-signature/1`` and the canonical ``{"length", "head"}``), so one key and one verifier serve
both.

Layout: ``<anchor>/checkpoints/<length, 20 digits>.json``, one canonical-JSON line each. A file is created once and
never replaced (``os.link`` refuses an existing name). Publishing only moves forward, and only along one history: the
new checkpoint must be the head of the given ledger at its length, and that ledger must hold the newest published head
at the newest published length.

Limits: the anchor only helps if its protection is real (branch protection the ledger writer cannot turn off) and the
verifier reads it from the remote, not from the writer's clone. A key holder can sign any checkpoint; the anchor makes a
published one hard to take back, it does not make the key holder honest. This is a step toward a public transparency log
(like Sigstore's Rekor), where a third party, not the repository owner, keeps the append-only record.
"""
import json
import os
import re
import tempfile
from collections.abc import Callable
from typing import Any, Dict, List, Optional

from .ledger import DecisionLedger, LedgerCheckpoint
from .store import CheckpointStore, StoreError

ANCHOR_SCHEMA = "delta-anchor-checkpoint/1"
ANCHOR_DIRECTORY = "checkpoints"
_NAME = re.compile(r"^([0-9]{20})\.json$")
_KEYS = frozenset({"schema", "length", "head", "signature"})


class AnchorError(StoreError):
    """The anchor holds something that is not a sound, signed, forward-only run of checkpoints, or a publish was refused."""


def _canonical(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def _signing_input(checkpoint: LedgerCheckpoint) -> bytes:
    # The bytes CheckpointStore signs, so a checkpoint signed for the store verifies here with the same key.
    return CheckpointStore._signing_input(checkpoint)


def _callable(name: str, fn: Any) -> None:
    if not callable(fn):
        raise TypeError(f"{name} must be callable; got {type(fn).__name__}")


def anchor_record(checkpoint: LedgerCheckpoint, sign: Callable[[bytes], bytes]) -> Dict[str, Any]:
    """The record published for ``checkpoint``: exactly ``schema``, ``length``, ``head`` and ``signature`` (hex)."""
    if not isinstance(checkpoint, LedgerCheckpoint):
        raise TypeError(f"expected a LedgerCheckpoint; got {type(checkpoint).__name__}")
    _callable("sign", sign)
    checkpoint = LedgerCheckpoint.from_dict(checkpoint.to_dict())
    if checkpoint.length < 1:
        raise AnchorError("a checkpoint of an empty ledger names no entry and is not published")
    signature: Any = sign(_signing_input(checkpoint))
    if type(signature) is not bytes:
        raise TypeError(f"the signer must return exactly bytes; got {type(signature).__name__}")
    if not signature:
        raise ValueError("the signer returned an empty signature")
    return {"schema": ANCHOR_SCHEMA, "length": checkpoint.length, "head": checkpoint.head, "signature": signature.hex()}


def _parse(name: str, raw: bytes, verify: Callable[[bytes, bytes], bool]) -> LedgerCheckpoint:
    try:
        obj = json.loads(raw.decode("ascii"))
    except (ValueError, UnicodeDecodeError) as exc:
        raise AnchorError(f"{name}: not ASCII JSON ({exc})") from None
    if type(obj) is not dict or set(obj) != _KEYS:
        raise AnchorError(f"{name}: a published checkpoint has exactly the keys {sorted(_KEYS)}")
    if obj["schema"] != ANCHOR_SCHEMA or type(obj["schema"]) is not str:
        raise AnchorError(f"{name}: unknown schema {obj['schema']!r}")
    if raw != (_canonical(obj) + "\n").encode("ascii"):
        raise AnchorError(f"{name}: not in canonical form")
    try:
        checkpoint = LedgerCheckpoint.from_dict({"length": obj["length"], "head": obj["head"]})
    except ValueError as exc:
        raise AnchorError(f"{name}: {exc}") from None
    if checkpoint.length < 1:
        raise AnchorError(f"{name}: length 0 names no entry")
    if name != f"{checkpoint.length:020d}.json":
        raise AnchorError(f"{name}: the file name does not match its length {checkpoint.length}")
    text = obj["signature"]
    try:
        if type(text) is not str or not text or text != text.lower():
            raise ValueError
        signature = bytes.fromhex(text)
    except ValueError:
        raise AnchorError(f"{name}: the signature must be a non-empty lowercase hex string") from None
    try:
        ok = verify(_signing_input(checkpoint), signature) is True
    except Exception:
        ok = False
    if not ok:
        raise AnchorError(f"{name}: the signature does not verify")
    return checkpoint


def read_anchor(anchor: Any, *, verify: Callable[[bytes, bytes], bool]) -> List[LedgerCheckpoint]:
    """Every published checkpoint, oldest first, each signature checked. An empty list when nothing is published.

    ``verify`` is required: only signed checkpoints count. Any file in ``checkpoints/`` that is not a sound published
    checkpoint (bad name, extra key, not canonical, bad signature) is an ``AnchorError`` naming the file; names starting
    with a dot are ignored and never count. A missing
    ``checkpoints/`` directory reads as nothing published. Lengths are strictly increasing by construction (one file
    per length); whether they lie along one history is what ``check_against_anchor`` tests.
    """
    _callable("verify", verify)
    directory = os.path.join(os.fspath(anchor), ANCHOR_DIRECTORY)
    if not os.path.isdir(directory):
        return []
    out = []
    for name in sorted(os.listdir(directory)):
        if name.startswith("."):
            continue  # a leftover temp file or .gitkeep is never a published checkpoint, so it never counts
        path = os.path.join(directory, name)
        if not _NAME.match(name) or os.path.islink(path) or not os.path.isfile(path):
            raise AnchorError(f"{name}: not a published checkpoint file (expected <20-digit length>.json)")
        with open(path, "rb") as fh:
            out.append(_parse(name, fh.read(), verify))
    return out


def publish_checkpoint(
    anchor: Any,
    checkpoint: LedgerCheckpoint,
    *,
    sign: Callable[[bytes], bytes],
    verify: Callable[[bytes, bytes], bool],
    history: Optional[DecisionLedger] = None,
) -> Optional[str]:
    """Write ``checkpoint``, signed, into the anchor clone. Returns the path written, or ``None`` when the same
    checkpoint is already published.

    Refused (``AnchorError``) when the anchor already holds anything unsound, when the length is not beyond the newest
    published one (the same length with the same head is the no-op; with another head it is a fork), or when it does not
    extend the newest published one: ``history`` (the ledger the checkpoint was taken from) must hold the newest
    published head at its length and have ``checkpoint`` as its head at ``checkpoint.length``. The first publish needs
    no history. The file is created once with ``os.link`` (never replacing a name) after an fsync. Commit and push it
    yourself; delta runs no git.
    """
    if history is not None and not isinstance(history, DecisionLedger):
        raise TypeError(f"history must be a DecisionLedger; got {type(history).__name__}")
    published = read_anchor(anchor, verify=verify)
    record = anchor_record(checkpoint, sign)
    try:
        ok = verify(_signing_input(checkpoint), bytes.fromhex(record["signature"])) is True
    except Exception:
        ok = False
    if not ok:
        raise AnchorError("the signer and the verifier do not agree on this checkpoint; nothing was published")
    if published:
        latest = published[-1]
        same = [p for p in published if p.length == checkpoint.length]
        if same:
            if same[0].head == checkpoint.head:
                return None
            raise AnchorError(f"refused: length {checkpoint.length} is already published with a different head (a fork)")
        if checkpoint.length < latest.length:
            raise AnchorError(f"refused: length {checkpoint.length} is behind the newest published {latest.length}")
        if history is None:
            raise AnchorError("refused: pass history=the ledger the checkpoint was taken from, to show it extends "
                              "the newest published checkpoint")
        entries = history.entries
        if len(entries) < latest.length or entries[latest.length - 1].immutable_hash != latest.head:
            raise AnchorError(f"refused: the history does not contain the newest published checkpoint "
                              f"(length {latest.length}); it does not extend it")
    if history is not None:
        entries = history.entries
        if checkpoint.length > len(entries) or entries[checkpoint.length - 1].immutable_hash != checkpoint.head:
            raise AnchorError(f"refused: the checkpoint is not the head of the history at length {checkpoint.length}")
    directory = os.path.join(os.fspath(anchor), ANCHOR_DIRECTORY)
    os.makedirs(directory, exist_ok=True)
    final = os.path.join(directory, f"{checkpoint.length:020d}.json")
    fd, tmp = tempfile.mkstemp(dir=directory, prefix=".publish-", suffix=".tmp")
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write((_canonical(record) + "\n").encode("ascii"))
            fh.flush()
            os.fsync(fh.fileno())
        try:
            os.link(tmp, final)
        except FileExistsError:
            raise AnchorError(f"refused: {os.path.basename(final)} appeared while publishing; nothing was replaced") from None
    finally:
        os.unlink(tmp)
    dfd = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(dfd)
    finally:
        os.close(dfd)
    return final


def check_against_anchor(ledger: DecisionLedger, anchor: Any, *, verify: Callable[[bytes, bytes], bool]) -> Dict[str, Any]:
    """Compare a ledger with every checkpoint published in the anchor. Returns ``{"status", ...}``:

    - ``OK``: the chain verifies and every published checkpoint is in it; the newest one is the ledger's head.
    - ``UNWITNESSED_TAIL``: as OK, but the ledger has ``unwitnessed`` entries after the newest published checkpoint,
      which nothing outside vouches for yet.
    - ``LOST``: every published checkpoint the ledger is long enough to hold is in it, but the newest published one is
      beyond its end: entries the anchor witnessed are missing.
    - ``DIVERGED``: the chain does not verify, or a published checkpoint names a different head at its length
      (``first_diverged`` is that length): the history the anchor witnessed was rewritten.
    - ``NO_ANCHOR``: nothing is published, so nothing outside vouches for any entry.

    Every report also has ``published`` (how many), ``anchored_length`` (the newest published length, 0 for none) and
    ``ledger_length``. An unsound anchor is an ``AnchorError``. To check an export, rebuild it first with
    ``DecisionLedger.from_export``. Read the anchor from the remote, not from a clone the ledger writer controls.
    """
    if type(ledger) is not DecisionLedger:
        raise TypeError(f"expected exactly a DecisionLedger; got {type(ledger).__name__}")
    published = read_anchor(anchor, verify=verify)
    entries = ledger.entries
    report: Dict[str, Any] = {"published": len(published), "anchored_length": published[-1].length if published else 0,
                              "ledger_length": len(entries)}
    if not ledger.verify_chain_integrity():
        report.update(status="DIVERGED", first_diverged=None, reason="the ledger's chain does not verify")
        return report
    if not published:
        report["status"] = "NO_ANCHOR"
        return report
    for cp in published:
        if cp.length <= len(entries) and entries[cp.length - 1].immutable_hash != cp.head:
            report.update(status="DIVERGED", first_diverged=cp.length,
                          reason=f"the ledger's head at length {cp.length} is not the one published")
            return report
    latest = published[-1]
    if latest.length > len(entries):
        report["status"] = "LOST"
        return report
    report["unwitnessed"] = len(entries) - latest.length
    report["status"] = "OK" if report["unwitnessed"] == 0 else "UNWITNESSED_TAIL"
    return report
