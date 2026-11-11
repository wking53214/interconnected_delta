"""DecisionLedger: append-only, SHA256-chained record of beta.Decisions.

Extracted from PERCEIVE's ImmutableAuditLedger (perceive_consolidated.py:
579-633) -- a real, working genesis-hash + chain + verify mechanism,
generalized from PERCEIVE's specific entry shape (request_snapshot/
evaluated_gates/policy_outputs/final_verdict/manifest_version/
manifest_hash) to chain any beta.Decision instead.

Faithfully preserved:
  - Genesis hash: sha256(b"<PREFIX>_GENESIS") seeds the chain.
  - immutable_hash = sha256(previous_hash + sha256(canonical_subset)).
  - verify_chain_integrity() recomputes the whole chain from genesis
    and compares, rather than trusting stored hashes -- the same
    "recompute, don't trust" posture as PERCEIVE's version.

CHANGED from the source, and from an earlier version of this file
(adversarial review caught both problems below):

  - The canonical subset now hashes ALL of a Decision's informational
    fields (entity_id, decision_id, decision, timestamp, confidence,
    triggered_by_locks, triggered_by_keys, newly_triggered_locks,
    decision_fingerprint, reasoning, reversal_conditions, instructions)
    plus audit_id, not just a 4-field subset. An earlier version hashed
    only entity_id/decision_id/decision/decision_fingerprint on the
    theory that decision_fingerprint already "captures" the rest --
    but decision_fingerprint is just a stored STRING field; nothing
    forces it to still match if reasoning/instructions/confidence/etc.
    are swapped afterward without recomputing it. A ledger that doesn't
    itself hash those fields can't detect that swap. This is the whole
    point of a tamper-evident ledger, so it now hashes the fields
    directly rather than trusting decision_fingerprint's internal
    consistency.
  - audit_id is generated from decision content (a hash of entry
    position + decision_fingerprint), not wall-clock time like the
    source's `sha256(f"{n}:{datetime.now(timezone.utc).isoformat()}")`.
    This trades the source's ingestion-time provenance for determinism:
    replaying the same decisions produces the same audit_ids and the
    same chain, which the test suite relies on
    (test_two_ledgers_with_identical_decisions_produce_identical_chains).
    If wall-clock ingestion provenance matters more than replay
    determinism for a given deployment, this would need revisiting.
"""

import dataclasses
import hashlib
import json
import re
import sys
import threading
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from types import FrameType
from typing import Callable, List, Optional

from beta import Decision

from .context import check_context

_GENESIS_SEED = b"INTERCONNECTED_DELTA_GENESIS"
_HEX64 = re.compile(r"[0-9a-f]{64}")
_TUPLE_FIELDS = ("triggered_by_locks", "triggered_by_keys", "newly_triggered_locks", "reversal_conditions")


@dataclass(frozen=True)
class LedgerCheckpoint:
    """The length and head of a ledger at one moment. Only as trustworthy as the place it is kept."""

    length: int
    head: str

    def to_dict(self) -> dict:
        """A plain JSON-able dict: ``{"length": int, "head": str}``."""
        return {"length": self.length, "head": self.head}

    @classmethod
    def from_dict(cls, data: Mapping) -> "LedgerCheckpoint":
        """The checkpoint ``to_dict`` wrote. A dict with exactly the keys ``length`` and ``head``, ``length`` exactly an
        ``int`` that is not negative (a bool is not one) and ``head`` exactly a ``str`` of 64 lowercase hex digits;
        anything else is a ``ValueError``. A length of 0 is a well-formed checkpoint that proves nothing: verifying
        against it always fails."""
        if type(data) is not dict:
            raise ValueError(f"a checkpoint is a dict; got {type(data).__name__}")
        if set(data) != {"length", "head"}:
            raise ValueError(f"a checkpoint has exactly the keys length and head; got {sorted(map(str, data))}")
        length, head = data["length"], data["head"]
        if type(length) is not int or length < 0:
            raise ValueError(f"checkpoint length must be a non-negative int; got {length!r}")
        if type(head) is not str or not _HEX64.fullmatch(head):
            raise ValueError("checkpoint head must be a str of 64 lowercase hex digits")
        return cls(length, head)


class DuplicateRecordError(ValueError):
    """The event (or the run) already has a Decision recorded: an event is recorded once, whatever run it is brought back in."""


class StaleDecisionError(ValueError):
    """The Decision is older than the newest Decision already recorded for the same entity."""


class ContextRequiredError(ValueError):
    """This ledger requires every Decision to be recorded under a pipeline context, and none was given."""


class LedgerCorruptError(ValueError):
    """The ledger's head does not match its last entry, so nothing more is appended to it."""


@dataclass(frozen=True)
class LedgerEntry:
    audit_id: str
    timestamp: datetime
    decision: Decision
    previous_hash: str
    immutable_hash: str = ""
    # The pipeline run this entry was recorded under, when it was given a context. Hashed into the chain only when
    # set, so an entry recorded without one hashes exactly as it always did.
    run_id: Optional[str] = None
    event_id: Optional[str] = None


_STR_FIELDS = (
    "entity_id", "decision_id", "decision", "decision_fingerprint", "reasoning", "instructions",
    "content_fingerprint", "keyset_fingerprint", "confidence_basis",
)


def _check_exact_types(decision: Decision, timestamp: Optional[datetime]) -> None:
    """Every string field and tuple element must be exactly ``str``, confidence exactly ``float`` or ``int`` (never a
    bool) and the times exactly ``datetime``: a subclass can lie about its
    own equality, ordering or text, and so be judged one way and recorded as another."""
    for name in _STR_FIELDS:
        if type(getattr(decision, name)) is not str:
            raise TypeError(f"Decision.{name} must be exactly a str; got {type(getattr(decision, name)).__name__}")
    for name in _TUPLE_FIELDS:
        for item in getattr(decision, name):
            if type(item) is not str:
                raise TypeError(f"every element of Decision.{name} must be exactly a str; got {type(item).__name__}")
    if type(decision.confidence) not in (float, int):
        raise TypeError(f"Decision.confidence must be exactly a float or an int; got {type(decision.confidence).__name__}")
    if type(decision.timestamp) is not datetime:
        raise TypeError(f"Decision.timestamp must be exactly a datetime; got {type(decision.timestamp).__name__}")
    if timestamp is not None and type(timestamp) is not datetime:
        raise TypeError(f"timestamp must be exactly a datetime; got {type(timestamp).__name__}")


def _canonical_subset(
    entry_audit_id: str,
    timestamp: datetime,
    decision: Decision,
    run_id: Optional[str] = None,
    event_id: Optional[str] = None,
) -> dict:
    subset = _decision_subset(entry_audit_id, timestamp, decision)
    # The KeySet the Decision's lock results came from, hashed into the chain only when it has one, so a Decision
    # recorded without it hashes exactly as before.
    if getattr(decision, "keyset_fingerprint", ""):
        subset["keyset_fingerprint"] = decision.keyset_fingerprint
    # beta's fingerprint over every informational field of the Decision, hashed only when set, so a Decision without
    # one hashes exactly as before.
    if getattr(decision, "content_fingerprint", ""):
        subset["content_fingerprint"] = decision.content_fingerprint
    if run_id is not None:
        subset["run_id"] = run_id
    if event_id is not None:
        subset["event_id"] = event_id
    return subset


def _decision_subset(entry_audit_id: str, timestamp: datetime, decision: Decision) -> dict:
    return {
        "audit_id": entry_audit_id,
        "entry_timestamp": timestamp.isoformat(),
        "entity_id": decision.entity_id,
        "decision_id": decision.decision_id,
        "decision": decision.decision,
        "decision_timestamp": decision.timestamp.isoformat(),
        "confidence": decision.confidence,
        "triggered_by_locks": list(decision.triggered_by_locks),
        "triggered_by_keys": list(decision.triggered_by_keys),
        "newly_triggered_locks": list(decision.newly_triggered_locks),
        "decision_fingerprint": decision.decision_fingerprint,
        "reasoning": decision.reasoning,
        "reversal_conditions": list(decision.reversal_conditions),
        "instructions": decision.instructions,
    }


EXPORT_SCHEMA = "ledger-export/1"
_DECISION_STR_KEYS = (
    "entity_id", "decision_id", "decision", "decision_fingerprint", "audit_hash", "reasoning", "instructions",
    "confidence_basis", "keyset_fingerprint", "content_fingerprint",
)
_DECISION_KEYS = frozenset(_DECISION_STR_KEYS) | {"timestamp", "confidence", *_TUPLE_FIELDS}
_ENTRY_KEYS = frozenset({"audit_id", "timestamp", "previous_hash", "immutable_hash", "run_id", "event_id", "decision"})


def _decision_to_dict(decision: Decision) -> dict:
    out = {name: getattr(decision, name) for name in _DECISION_STR_KEYS}
    out["timestamp"] = decision.timestamp.isoformat()
    out["confidence"] = decision.confidence
    for name in _TUPLE_FIELDS:
        out[name] = list(getattr(decision, name))
    return out


def _parse_time(value: object, where: str) -> datetime:
    if type(value) is not str:
        raise ValueError(f"{where} must be an isoformat str; got {type(value).__name__}")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{where} is not an isoformat time: {value!r}") from exc
    if type(parsed) is not datetime or parsed.isoformat() != value:
        raise ValueError(f"{where} is not in canonical isoformat: {value!r}")
    return parsed


def _entry_from_dict(i: int, raw: object, require_context: bool) -> LedgerEntry:
    where = f"entry {i}"
    if type(raw) is not dict or set(raw) != _ENTRY_KEYS:
        got = sorted(map(str, raw)) if type(raw) is dict else type(raw).__name__
        raise ValueError(f"{where}: must be a dict with exactly the keys {sorted(_ENTRY_KEYS)}; got {got}")
    for name in ("audit_id", "previous_hash", "immutable_hash"):
        if type(raw[name]) is not str:
            raise ValueError(f"{where}: {name} must be a str")
    for name in ("previous_hash", "immutable_hash"):
        if not _HEX64.fullmatch(raw[name]):
            raise ValueError(f"{where}: {name} must be 64 lowercase hex digits")
    for name in ("run_id", "event_id"):
        if raw[name] is not None and type(raw[name]) is not str:
            raise ValueError(f"{where}: {name} must be a str or null")
    stamp = _parse_time(raw["timestamp"], f"{where}: timestamp")
    d = raw["decision"]
    if type(d) is not dict or set(d) != _DECISION_KEYS:
        got = sorted(map(str, d)) if type(d) is dict else type(d).__name__
        raise ValueError(f"{where}: decision must be a dict with exactly the keys {sorted(_DECISION_KEYS)}; got {got}")
    fields: dict = {}
    for name in _DECISION_STR_KEYS:
        if type(d[name]) is not str:
            raise ValueError(f"{where}: decision.{name} must be a str; got {type(d[name]).__name__}")
        fields[name] = d[name]
    for name in _TUPLE_FIELDS:
        if type(d[name]) is not list or any(type(item) is not str for item in d[name]):
            raise ValueError(f"{where}: decision.{name} must be a list of str")
        fields[name] = tuple(d[name])
    if type(d["confidence"]) not in (float, int):
        raise ValueError(f"{where}: decision.confidence must be a float or an int; got {type(d['confidence']).__name__}")
    fields["confidence"] = d["confidence"]
    fields["timestamp"] = _parse_time(d["timestamp"], f"{where}: decision.timestamp")
    try:
        decision = Decision(**fields)
        _check_exact_types(decision, stamp)
        Decision.validate(decision, check_fingerprint=require_context)
    except (ValueError, TypeError) as exc:
        raise ValueError(f"{where}: the Decision is not valid: {exc}") from exc
    return LedgerEntry(
        audit_id=raw["audit_id"], timestamp=stamp, decision=decision, previous_hash=raw["previous_hash"],
        immutable_hash=raw["immutable_hash"], run_id=raw["run_id"], event_id=raw["event_id"],
    )


def verify_export(
    data: Mapping, checkpoint: Optional[object] = None, *, require_context: Optional[bool] = None
) -> "DecisionLedger":
    """Verify an ``export()`` and return the rebuilt ledger, or raise ``ValueError`` naming the first failure.

    Without ``checkpoint`` this is integrity only, no rollback detection: it shows the export is a consistent chain,
    not that it is the whole one (an attacker who can rewrite the export can truncate it, or rewrite and re-chain it).
    ``checkpoint`` (a ``LedgerCheckpoint`` or its ``to_dict`` form) must come from storage the producer cannot rewrite;
    the export must then contain the history it names (``verify_against``). ``require_context``, when given, must equal
    the flag in the export: a consumer that requires context should say so, because the flag travels with the export.
    """
    ledger = DecisionLedger.from_export(data)
    if require_context is not None:
        if type(require_context) is not bool:
            raise TypeError("require_context must be a bool or None")
        if ledger.require_context is not require_context:
            raise ValueError(f"the export says require_context={ledger.require_context}; the verifier requires {require_context}")
    if checkpoint is not None:
        if type(checkpoint) is dict:
            checkpoint = LedgerCheckpoint.from_dict(checkpoint)
        if not isinstance(checkpoint, LedgerCheckpoint):
            raise TypeError(f"checkpoint must be a LedgerCheckpoint or its dict form; got {type(checkpoint).__name__}")
        if type(checkpoint.length) is int and checkpoint.length > len(ledger):
            raise ValueError(
                f"the export holds {len(ledger)} entries but the held checkpoint names {checkpoint.length}: the export was truncated or is another ledger"
            )
        if not ledger.verify_against(checkpoint):
            raise ValueError("the export does not contain the history the held checkpoint recorded (empty, foreign or rewritten)")
    return ledger


def _hash_dict(d: dict) -> str:
    return hashlib.sha256(json.dumps(d, sort_keys=True, default=str).encode()).hexdigest()


class DecisionLedger:
    def __init__(self, require_context: bool = False, sink: Optional[Callable[[LedgerEntry], None]] = None) -> None:
        # sink: an optional callable(entry) called with each entry after it is built and before any state changes, so a
        # caller can write it somewhere durable first. If it raises, nothing was recorded and the exception propagates.
        # This ledger supplies the hook; it does not supply the storage.
        if sink is not None and not callable(sink):
            raise TypeError(f"sink must be callable or None; got {type(sink).__name__}")
        self._sink = sink
        # require_context=True: every append must carry a pipeline context. Left False the ledger still records an
        # unbound Decision as it always did, but such an entry has no run, and CNS's strict check refuses it.
        self.require_context = require_context
        self._lock = threading.Lock()
        # An append whose commit was cut short (head and entries not both written); set only under the lock, undone by
        # _heal before the next read or write. See _append_locked.
        self._pending: Optional[tuple] = None
        self.entries: List[LedgerEntry] = []
        self.chain_head: str = hashlib.sha256(_GENESIS_SEED).hexdigest()

    def append(
        self,
        decision: Decision,
        timestamp: Optional[datetime] = None,
        *,
        context: Optional[Mapping] = None,
    ) -> LedgerEntry:
        """Record a Decision. Refuses, and changes nothing, unless the Decision is internally consistent.

        With a ``context`` (schema, entity_id, event_id, run_id) the entry is recorded under that run: the
        Decision must be about the context's entity, its fingerprint must match its own fields, and a Decision
        with the same fingerprint already recorded in the same run is refused (DuplicateRecordError). The run
        and event are hashed into the chain. Without a context behaviour is as it always was, plus the
        consistency check.

        Atomic: the entry and every check are built first; only then are the head and the entry list updated,
        together, under the lock. A failure at any point leaves head, entries and length exactly as they were.
        """
        # Exactly a beta.Decision, judged by beta's own validate: an object that supplies its own validate (a subclass
        # that does nothing, a namespace with a lambda) would otherwise choose whether it is judged at all.
        if self._inside_sink():
            raise RuntimeError("append from inside a sink is not allowed")
        if type(decision) is not Decision:
            raise TypeError(f"delta records beta.Decision only; got {type(decision).__name__}")
        # Copy first, reading each caller-supplied iterable exactly once, then judge and record that copy only: an
        # iterable that answers differently on a later read cannot be stored differently from what was validated.
        decision = dataclasses.replace(decision, **{name: tuple(getattr(decision, name)) for name in _TUPLE_FIELDS})
        _check_exact_types(decision, timestamp)
        Decision.validate(decision, check_fingerprint=context is not None)
        with self._lock:
            return self._append_locked(decision, timestamp, context)

    def _append_locked(
        self, decision: Decision, timestamp: Optional[datetime], context: Optional[Mapping]
    ) -> LedgerEntry:
        self._heal()
        self._check_head()
        run_id = event_id = None
        if context is None and self.require_context:
            raise ContextRequiredError("this ledger requires a context: record the Decision under a pipeline run")
        if self.require_context and not decision.content_fingerprint:
            raise ValueError(
                "a ledger that requires a context refuses a Decision whose content_fingerprint is empty: "
                "blanking it removes the integrity claim instead of changing it (downgrade)"
            )
        if context is not None:
            ctx = check_context(context)
            if decision.entity_id != ctx["entity_id"]:
                raise ValueError(
                    f"the Decision is about {decision.entity_id!r} but the context is about {ctx['entity_id']!r}"
                )
            run_id, event_id = ctx["run_id"], ctx["event_id"]
            self._check_event_policy(run_id, event_id)
            self._check_not_replay(decision)
            self._check_not_stale(decision)
        ts = timestamp if timestamp is not None else decision.timestamp
        if timestamp is not None:
            # Only a caller-supplied entry time is judged: an entry that takes its Decision's time keeps the
            # behaviour it always had (the per-entity stale rule governs Decision times).
            self._check_not_backdated(ts)
            # Then: a Decision cannot be recorded before it was made. Mixing aware and naive times cannot be
            # compared, and an entry whose date cannot be placed against its Decision is refused, not guessed at.
            try:
                before = ts < decision.timestamp
            except TypeError:
                raise ValueError(
                    "the entry timestamp and the Decision's must be all timezone-aware or all naive"
                ) from None
            if before:
                raise ValueError(
                    f"the entry is dated before the Decision it records ({ts.isoformat()} < "
                    f"{decision.timestamp.isoformat()})"
                )
        audit_id = hashlib.sha256(
            f"{len(self.entries)}:{decision.decision_fingerprint}".encode()
        ).hexdigest()[:16]

        canonical = _canonical_subset(audit_id, ts, decision, run_id, event_id)
        combined = self.chain_head + _hash_dict(canonical)
        immutable_hash = hashlib.sha256(combined.encode()).hexdigest()

        entry = LedgerEntry(
            audit_id=audit_id,
            timestamp=ts,
            decision=decision,
            previous_hash=self.chain_head,
            immutable_hash=immutable_hash,
            run_id=run_id,
            event_id=event_id,
        )
        if self._sink is not None:
            self._call_sink(entry)
        # Every fallible step is above. The two writes below are one commit. The marker is set first, so a commit that is
        # cut short at any point, even by a second interrupt inside the rollback, is recorded as pending and is undone
        # by _heal (before the next append, snapshot or verification) instead of leaving an entry the head does not name.
        previous_head, length = self.chain_head, len(self.entries)
        self._pending = (previous_head, length, entry)
        try:
            self.entries.append(entry)
            self.chain_head = immutable_hash
        except BaseException:
            self._heal()
            raise
        self._pending = None
        return entry

    def _call_sink(self, entry: LedgerEntry) -> None:
        # "Inside a sink" is read from the call stack (see _inside_sink), so there is no flag to be left set by an
        # interrupt: the guard ends exactly when this call does.
        self._sink(entry)  # type: ignore[misc]

    def _inside_sink(self) -> bool:
        """Whether this thread is inside this ledger's sink call: the lock is held then and is not reentrant."""
        frame: Optional[FrameType] = sys._getframe(1)
        while frame is not None:
            if frame.f_code is _CALL_SINK_CODE and frame.f_locals.get("self") is self:
                return True
            frame = frame.f_back
        return False

    def _heal(self) -> None:
        """Undo a commit that was cut short, or acknowledge one that completed. Idempotent: the marker is cleared last,
        so an interrupt inside the healing leaves it to be healed again. Called with the lock held."""
        pending = self._pending
        if pending is None:
            return
        previous_head, length, entry = pending
        done = len(self.entries) == length + 1 and self.entries[-1] is entry and self.chain_head == entry.immutable_hash
        if not done:
            del self.entries[length:]
            self.chain_head = previous_head
        self._pending = None

    def _check_head(self) -> None:
        """O(1): the head must name the last entry (or genesis for an empty ledger). A full re-verification on every
        append would make recording quadratic; this catches a head and an entry list that were left disagreeing, not a
        chain someone rewrote and re-headed (that is what a checkpoint held outside the ledger is for)."""
        want = self.entries[-1].immutable_hash if self.entries else hashlib.sha256(_GENESIS_SEED).hexdigest()
        if self.chain_head != want:
            raise LedgerCorruptError(
                "the ledger's head does not match its last entry; nothing more is appended to a chain that does not verify"
            )

    def _check_not_backdated(self, ts: datetime) -> None:
        """A caller-supplied entry timestamp is never earlier than the entry before it (equal is allowed)."""
        if not self.entries:
            return
        previous = self.entries[-1].timestamp
        try:
            earlier = ts < previous
        except TypeError as exc:
            raise ValueError(
                f"entry timestamps must be all timezone-aware or all naive; got {ts!r} after {previous!r}"
            ) from exc
        if earlier:
            raise ValueError(
                f"backdated entry: {ts.isoformat()} is earlier than the previous entry ({previous.isoformat()})"
            )

    def _check_event_policy(self, run_id: str, event_id: str) -> None:
        """A run belongs to exactly one event, and an event has exactly one Decision."""
        for earlier in self.entries:
            if earlier.run_id == run_id:
                if earlier.event_id != event_id:
                    raise ValueError(f"run {run_id!r} belongs to event {earlier.event_id!r}, not {event_id!r}")
                raise DuplicateRecordError(f"run {run_id!r} already recorded a Decision for event {event_id!r}")
            if earlier.event_id == event_id:
                raise DuplicateRecordError(
                    f"event {event_id!r} already has a Decision recorded under run {earlier.run_id!r}; "
                    f"an event is recorded once, whatever run it is brought back in (decide again under a new event)"
                )

    @staticmethod
    def _identity(decision: Decision) -> tuple:
        return (decision.entity_id, decision.decision_fingerprint, decision.timestamp)

    def _check_not_replay(self, decision: Decision) -> None:
        """The same Decision (entity, fingerprint, time) is not recorded again under a new event or run: a fresh id
        does not make it a new decision. The content fingerprint is deliberately not part of the key: blanking it (a
        legacy Decision) must not turn a replay into a new record. Reached only when the run and event are
        both new, so the event and run messages above are unchanged."""
        want = self._identity(decision)
        for earlier in self.entries:
            if self._identity(earlier.decision) == want:
                raise DuplicateRecordError(
                    f"replay: this Decision is already recorded under event {earlier.event_id!r}, run {earlier.run_id!r}; "
                    f"a new event or run id does not make it a new decision"
                )

    def _check_not_stale(self, decision: Decision) -> None:
        """A Decision is never older than the newest one recorded for its entity."""
        for earlier in self.entries:
            if earlier.decision.entity_id != decision.entity_id:
                continue
            try:
                older = decision.timestamp < earlier.decision.timestamp
            except TypeError as exc:
                raise ValueError(
                    "Decision timestamps must be all timezone-aware or all naive; "
                    f"got {decision.timestamp!r} after {earlier.decision.timestamp!r}"
                ) from exc
            if older:
                raise StaleDecisionError(
                    f"the Decision ({decision.timestamp.isoformat()}) is older than the newest Decision recorded for "
                    f"{decision.entity_id!r} ({earlier.decision.timestamp.isoformat()})"
                )

    def snapshot(self) -> tuple:
        """``(entries, chain_head, length)`` taken together, once, under the ledger's lock.

        ``entries`` is a tuple, so what is read from it later cannot change; the head and the length belong to those
        very entries. A verifier that must state facts about the ledger (verification_receipt) reads this once and
        uses only these values. Called from inside a sink (this thread already holds the lock, and nothing else can
        append meanwhile) it reads without taking the lock again.
        """
        if self._inside_sink():
            entries = tuple(self.entries)
            return entries, self.chain_head, len(entries)
        with self._lock:
            self._heal()
            entries = tuple(self.entries)
            return entries, self.chain_head, len(entries)

    @staticmethod
    def _verify_snapshot(entries: tuple, head: str, rejudge: bool = False) -> bool:
        """The chain over one snapshot: every entry's recorded types and hash, linked from genesis, ending at ``head``.

        With ``rejudge`` every stored Decision is also put through ``Decision.validate(check_fingerprint=True)``, as it
        was when recorded, so a field no hash covers (confidence_basis) or a fingerprint that no longer fits the
        Decision is found too."""
        expected = hashlib.sha256(_GENESIS_SEED).hexdigest()
        for entry in entries:
            if type(entry) is not LedgerEntry or type(entry.decision) is not Decision:
                return False
            try:
                _check_exact_types(entry.decision, entry.timestamp)
            except TypeError:
                return False
            if rejudge:
                try:
                    Decision.validate(entry.decision, check_fingerprint=True)
                except (ValueError, TypeError):
                    return False
            canonical = _canonical_subset(
                entry.audit_id, entry.timestamp, entry.decision, entry.run_id, entry.event_id
            )
            combined = entry.previous_hash + _hash_dict(canonical)
            computed = hashlib.sha256(combined.encode()).hexdigest()
            if computed != entry.immutable_hash or entry.previous_hash != expected:
                return False
            expected = entry.immutable_hash
        return expected == head

    @staticmethod
    def _checkpoint_fits(entries: tuple, checkpoint: "LedgerCheckpoint") -> bool:
        length, head = checkpoint.length, checkpoint.head
        if type(length) is not int or length < 1 or length > len(entries):
            return False
        if type(head) is not str or not _HEX64.fullmatch(head):
            return False
        return entries[length - 1].immutable_hash == head

    def verify_chain_integrity(self) -> bool:
        entries, head, _ = self.snapshot()
        return self._verify_snapshot(entries, head, self.require_context)

    def checkpoint(self) -> LedgerCheckpoint:
        """The ledger's current length and head, to be held somewhere its holder cannot rewrite.

        verify_chain_integrity compares the chain with the ledger's own chain_head, so whoever can edit the
        entries can reset the head, or recompute the whole chain, and it still verifies. A checkpoint held
        elsewhere does not move with them. This module cannot say where to keep it: that is the caller's.
        """
        # One locked snapshot (healing a pending commit first), so the length and head are a pair that belongs together.
        _, head, length = self.snapshot()
        return LedgerCheckpoint(length, head)

    def verify_against(self, checkpoint: LedgerCheckpoint) -> bool:
        """Whether the whole chain verifies AND still contains the history the checkpoint recorded.

        The ledger may have grown since; it may not have been truncated, reordered or rewritten up to the
        checkpoint. False for any checkpoint that does not fit: a length that is not an int (a bool is not), is
        negative or is beyond the ledger, a head that is not exactly a ``str`` of 64 lowercase hex digits (an object
        that merely compares equal does not count), or a wrong head.

        A checkpoint of length 0 always returns False. It names no entry, so it is the same for every ledger and
        carries no evidence about any of them; accepting it would let a rewritten ledger pass against it.
        """
        if not isinstance(checkpoint, LedgerCheckpoint):
            raise TypeError(f"expected a LedgerCheckpoint; got {type(checkpoint).__name__}")
        entries, chain_head, _ = self.snapshot()
        length, head = checkpoint.length, checkpoint.head
        if not self._checkpoint_fits(entries, LedgerCheckpoint(length, head)):
            return False
        return self._verify_snapshot(entries, chain_head, self.require_context)

    def export(self) -> dict:
        """The whole ledger as a plain JSON-able dict, for a verifier in another process (``from_export``).

        ``{"schema": "ledger-export/1", "require_context", "chain_head", "entries": [...]}``. Each entry carries every
        ``LedgerEntry`` field (timestamps as isoformat text) and the whole Decision (tuples as lists). It is built from
        one locked snapshot, and refused (``ValueError``) when that snapshot does not verify, so what is written is
        what verified. It carries no signature; see the README for what a consumer must hold independently.
        """
        entries, head, _ = self.snapshot()
        rejudge = self.require_context
        if not self._verify_snapshot(entries, head, rejudge):
            raise ValueError("the ledger's chain does not verify, so it is not exported")
        out = []
        for e in entries:
            if type(e.decision.audit_hash) is not str:
                raise ValueError("Decision.audit_hash must be exactly a str to be exported")
            if any(v is not None and type(v) is not str for v in (e.run_id, e.event_id)):
                raise ValueError("an entry's run_id and event_id must be exactly a str or None to be exported")
            out.append({
                "audit_id": e.audit_id,
                "timestamp": e.timestamp.isoformat(),
                "previous_hash": e.previous_hash,
                "immutable_hash": e.immutable_hash,
                "run_id": e.run_id,
                "event_id": e.event_id,
                "decision": _decision_to_dict(e.decision),
            })
        return {"schema": EXPORT_SCHEMA, "require_context": rejudge, "chain_head": head, "entries": out}

    @classmethod
    def from_export(cls, data: Mapping) -> "DecisionLedger":
        """Rebuild a ledger from ``export()``'s dict (after ``json.loads`` if it was text), trusting none of it.

        Every field's type is checked exactly, every hash is recomputed from the content and must equal the stored one,
        each entry must start from the one before (genesis for the first), the head must be the last hash, and on a
        ledger exported with ``require_context`` every Decision is put through ``Decision.validate(check_fingerprint=True)``.
        The first failure is a ``ValueError`` naming the entry and the field. It does not replay the append policies
        (events, runs, staleness) and it holds no checkpoint: without one a truncated or wholly rewritten and
        re-chained export still verifies. Use ``delta.verify_export`` to check a held checkpoint too.
        """
        if type(data) is not dict:
            raise ValueError(f"an export is a dict; got {type(data).__name__}")
        if set(data) != {"schema", "require_context", "chain_head", "entries"}:
            raise ValueError(f"an export has exactly the keys schema, require_context, chain_head and entries; got {sorted(map(str, data))}")
        if data["schema"] != EXPORT_SCHEMA or type(data["schema"]) is not str:
            raise ValueError(f"unknown export schema {data['schema']!r}; expected {EXPORT_SCHEMA!r}")
        if type(data["require_context"]) is not bool:
            raise ValueError("require_context must be a bool")
        head = data["chain_head"]
        if type(head) is not str or not _HEX64.fullmatch(head):
            raise ValueError("chain_head must be a str of 64 lowercase hex digits")
        if type(data["entries"]) is not list:
            raise ValueError("entries must be a list")
        require_context = data["require_context"]
        expected = hashlib.sha256(_GENESIS_SEED).hexdigest()
        built = []
        for i, raw in enumerate(data["entries"]):
            entry = _entry_from_dict(i, raw, require_context)
            if entry.previous_hash != expected:
                raise ValueError(f"entry {i}: previous_hash does not continue the chain (an entry is missing, moved or edited)")
            computed = hashlib.sha256(
                (entry.previous_hash + _hash_dict(_canonical_subset(
                    entry.audit_id, entry.timestamp, entry.decision, entry.run_id, entry.event_id))).encode()
            ).hexdigest()
            if computed != entry.immutable_hash:
                raise ValueError(f"entry {i}: immutable_hash does not match the entry's content (the entry or its hash was edited)")
            expected = entry.immutable_hash
            built.append(entry)
        if expected != head:
            raise ValueError("chain_head is not the hash of the last entry (the export was truncated, extended or re-headed)")
        ledger = cls(require_context=require_context)
        ledger.entries = built
        ledger.chain_head = head
        return ledger

    def __len__(self) -> int:
        # Under the lock and after healing, like snapshot(), so a half-done commit is never counted.
        return self.snapshot()[2]


_CALL_SINK_CODE = DecisionLedger._call_sink.__code__
