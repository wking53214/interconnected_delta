"""DecisionLedger: append-only, SHA256-chained record of beta.Decisions.

Extracted from PERCEIVE's ImmutableAuditLedger (perceive_consolidated.py:
579-633) -- a real, working genesis-hash + chain + verify mechanism,
generalized from PERCEIVE's specific entry shape (request_snapshot/
evaluated_gates/policy_outputs/final_verdict/manifest_version/
manifest_hash) to chain any beta.Decision instead.

Faithfully preserved:
  - Genesis hash: sha256(b"<PREFIX>_GENESIS") seeds the chain.
  - Each entry hashes ONLY a canonical subset of its fields (source
    hashes audit_id/timestamp/evaluated_gates/final_verdict -- NOT
    request_snapshot/policy_outputs/manifest_version/manifest_hash,
    which ride along stored but outside the tamper-evidence hash). This
    module's canonical subset is decision_id/decision/entity_id/
    decision_fingerprint/timestamp -- beta.Decision's own
    decision_fingerprint already captures triggered_by_locks/keys, so
    there's no need to separately hash those again here.
  - immutable_hash = sha256(previous_hash + sha256(canonical_subset)).
  - verify_chain_integrity() recomputes the whole chain from genesis
    and compares, rather than trusting stored hashes -- the same
    "recompute, don't trust" posture as PERCEIVE's version.
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
        "timestamp": timestamp.isoformat(),
        "entity_id": decision.entity_id,
        "decision_id": decision.decision_id,
        "decision": decision.decision,
        "decision_fingerprint": decision.decision_fingerprint,
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
