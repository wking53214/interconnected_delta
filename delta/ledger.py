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
import threading
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Optional

from beta import Decision

from .context import check_context

_GENESIS_SEED = b"INTERCONNECTED_DELTA_GENESIS"
_TUPLE_FIELDS = ("triggered_by_locks", "triggered_by_keys", "newly_triggered_locks", "reversal_conditions")


@dataclass(frozen=True)
class LedgerCheckpoint:
    """The length and head of a ledger at one moment. Only as trustworthy as the place it is kept."""

    length: int
    head: str


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


def _hash_dict(d: dict) -> str:
    return hashlib.sha256(json.dumps(d, sort_keys=True, default=str).encode()).hexdigest()


class DecisionLedger:
    def __init__(self, require_context: bool = False) -> None:
        # require_context=True: every append must carry a pipeline context. Left False the ledger still records an
        # unbound Decision as it always did, but such an entry has no run, and CNS's strict check refuses it.
        self.require_context = require_context
        self._lock = threading.Lock()
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
        if type(decision) is not Decision:
            raise TypeError(f"delta records beta.Decision only; got {type(decision).__name__}")
        Decision.validate(decision, check_fingerprint=context is not None)
        # Store a copy whose list-typed fields are tuples, so a list the caller still holds cannot reach the record.
        decision = dataclasses.replace(decision, **{name: tuple(getattr(decision, name)) for name in _TUPLE_FIELDS})
        with self._lock:
            return self._append_locked(decision, timestamp, context)

    def _append_locked(
        self, decision: Decision, timestamp: Optional[datetime], context: Optional[Mapping]
    ) -> LedgerEntry:
        self._check_head()
        run_id = event_id = None
        if context is None and self.require_context:
            raise ContextRequiredError("this ledger requires a context: record the Decision under a pipeline run")
        if context is not None:
            ctx = check_context(context)
            if decision.entity_id != ctx["entity_id"]:
                raise ValueError(
                    f"the Decision is about {decision.entity_id!r} but the context is about {ctx['entity_id']!r}"
                )
            run_id, event_id = ctx["run_id"], ctx["event_id"]
            self._check_event_policy(run_id, event_id)
            self._check_not_stale(decision)
        ts = timestamp if timestamp is not None else decision.timestamp
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
        # Every fallible step is above. The two updates below are made together: if either is interrupted the other
        # is put back, so head, entries and length are exactly as before and the next append works.
        previous_head, length = self.chain_head, len(self.entries)
        try:
            self.entries.append(entry)
            self.chain_head = immutable_hash
        except BaseException:
            del self.entries[length:]
            self.chain_head = previous_head
            raise
        return entry

    def _check_head(self) -> None:
        """O(1): the head must name the last entry (or genesis for an empty ledger). A full re-verification on every
        append would make recording quadratic; this catches a head and an entry list that were left disagreeing, not a
        chain someone rewrote and re-headed (that is what a checkpoint held outside the ledger is for)."""
        want = self.entries[-1].immutable_hash if self.entries else hashlib.sha256(_GENESIS_SEED).hexdigest()
        if self.chain_head != want:
            raise LedgerCorruptError(
                "the ledger's head does not match its last entry; nothing more is appended to a chain that does not verify"
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

    def verify_chain_integrity(self) -> bool:
        expected = hashlib.sha256(_GENESIS_SEED).hexdigest()
        for entry in self.entries:
            canonical = _canonical_subset(
                entry.audit_id, entry.timestamp, entry.decision, entry.run_id, entry.event_id
            )
            combined = entry.previous_hash + _hash_dict(canonical)
            computed = hashlib.sha256(combined.encode()).hexdigest()
            if computed != entry.immutable_hash or entry.previous_hash != expected:
                return False
            expected = entry.immutable_hash
        return expected == self.chain_head

    def checkpoint(self) -> LedgerCheckpoint:
        """The ledger's current length and head, to be held somewhere its holder cannot rewrite.

        verify_chain_integrity compares the chain with the ledger's own chain_head, so whoever can edit the
        entries can reset the head, or recompute the whole chain, and it still verifies. A checkpoint held
        elsewhere does not move with them. This module cannot say where to keep it: that is the caller's.
        """
        return LedgerCheckpoint(len(self.entries), self.chain_head)

    def verify_against(self, checkpoint: LedgerCheckpoint) -> bool:
        """Whether the whole chain verifies AND still contains the history the checkpoint recorded.

        The ledger may have grown since; it may not have been truncated, reordered or rewritten up to the
        checkpoint. False for any checkpoint that does not fit (negative, beyond the ledger, wrong head).
        """
        if not isinstance(checkpoint, LedgerCheckpoint):
            raise TypeError(f"expected a LedgerCheckpoint; got {type(checkpoint).__name__}")
        length = checkpoint.length
        if isinstance(length, bool) or not isinstance(length, int) or length < 0 or length > len(self.entries):
            return False
        if not self.verify_chain_integrity():
            return False
        anchor = hashlib.sha256(_GENESIS_SEED).hexdigest() if length == 0 else self.entries[length - 1].immutable_hash
        return anchor == checkpoint.head

    def __len__(self) -> int:
        return len(self.entries)
