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

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Optional

from beta import Decision

_GENESIS_SEED = b"INTERCONNECTED_DELTA_GENESIS"


@dataclass(frozen=True)
class LedgerEntry:
    audit_id: str
    timestamp: datetime
    decision: Decision
    previous_hash: str
    immutable_hash: str = ""


def _canonical_subset(entry_audit_id: str, timestamp: datetime, decision: Decision) -> dict:
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
    def __init__(self) -> None:
        self.entries: List[LedgerEntry] = []
        self.chain_head: str = hashlib.sha256(_GENESIS_SEED).hexdigest()

    def append(self, decision: Decision, timestamp: Optional[datetime] = None) -> LedgerEntry:
        ts = timestamp if timestamp is not None else decision.timestamp
        audit_id = hashlib.sha256(
            f"{len(self.entries)}:{decision.decision_fingerprint}".encode()
        ).hexdigest()[:16]

        canonical = _canonical_subset(audit_id, ts, decision)
        combined = self.chain_head + _hash_dict(canonical)
        immutable_hash = hashlib.sha256(combined.encode()).hexdigest()

        entry = LedgerEntry(
            audit_id=audit_id,
            timestamp=ts,
            decision=decision,
            previous_hash=self.chain_head,
            immutable_hash=immutable_hash,
        )
        self.chain_head = immutable_hash
        self.entries.append(entry)
        return entry

    def verify_chain_integrity(self) -> bool:
        expected = hashlib.sha256(_GENESIS_SEED).hexdigest()
        for entry in self.entries:
            canonical = _canonical_subset(entry.audit_id, entry.timestamp, entry.decision)
            combined = entry.previous_hash + _hash_dict(canonical)
            computed = hashlib.sha256(combined.encode()).hexdigest()
            if computed != entry.immutable_hash or entry.previous_hash != expected:
                return False
            expected = entry.immutable_hash
        return True

    def __len__(self) -> int:
        return len(self.entries)
