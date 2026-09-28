"""DecisionObligationTracker: bridges beta.Decision to the OutcomeObligation lifecycle.

NEW code, not extracted. outcome_v1.py's derive_open_obligations operates
on generic decision-ROW dicts (current_hash, timestamp, an
outcome_obligation declaration string, domain, subject_id) pulled from a
ledger table -- sentinel_os's decisions aren't beta.Decision objects, so
there's no existing adapter to extract from. This module is that adapter,
using obligation.py's lifecycle functions exactly as-is underneath.

Maturation policy is declarative and keyed by decision_id, matching the
whole project's config-over-inline-logic posture (zeta.LockSpec,
beta.DecisionRule): register once which decision_ids owe a maturing
obligation and what kind, rather than a branch per decision type.
"""

import threading
from typing import Dict, List, Optional

from beta import Decision

from .obligation import (
    MaturationRule,
    OutcomeObligation,
    abandon,
    horizon_honored,
    open_obligation,
    resolve,
    stay_open,
)


class DecisionObligationTracker:
    """Opens/tracks/resolves OutcomeObligations for beta.Decisions.

    Not every decision owes one: a decision_id with no registered
    MaturationRule genuinely owes nothing later (an IVR call's quality
    is settled at hangup) -- matches outcome_v1's own posture of
    skipping rows with no declared obligation rather than inventing one.

    Thread-safe for concurrent open_for_decision calls: a lock guards
    the check-then-insert, so two callers racing to open the same
    obligation_id get one success and one clear ValueError, never both
    silently "succeeding" with one write lost.
    """

    def __init__(self, maturation_policy: Optional[Dict[str, MaturationRule]] = None) -> None:
        self.maturation_policy: Dict[str, MaturationRule] = dict(maturation_policy or {})
        self._obligations: Dict[str, OutcomeObligation] = {}
        self._lock = threading.Lock()

    def register_maturation(self, decision_id: str, rule: MaturationRule) -> None:
        self.maturation_policy[decision_id] = rule

    def open_for_decision(self, decision: Decision, domain: str, opened_at: float) -> Optional[OutcomeObligation]:
        """Open an obligation for this decision, if its decision_id has a
        registered maturation rule. Returns None (opens nothing) if not --
        this is the expected, common case, not an error."""
        rule = self.maturation_policy.get(decision.decision_id)
        if rule is None:
            return None
        obligation_id = f"{decision.decision_fingerprint}:{rule.kind}"
        with self._lock:
            if obligation_id in self._obligations:
                raise ValueError(
                    f"Obligation '{obligation_id}' already opened -- "
                    f"open_for_decision must not be called twice for the same decision"
                )
            obligation = open_obligation(
                obligation_id=obligation_id,
                decision_fingerprint=decision.decision_fingerprint,
                domain=domain,
                rule=rule,
                opened_at=opened_at,
                subject_id=decision.entity_id,
            )
            self._obligations[obligation_id] = obligation
        return obligation

    def resolve_obligation(self, obligation_id: str, resolved_at: float,
                            resolved_value, provenance: str,
                            favorable: Optional[bool] = None,
                            method: Optional[str] = None) -> OutcomeObligation:
        with self._lock:
            current = self._require_locked(obligation_id)
            updated = resolve(current, resolved_at, resolved_value, provenance, favorable, method)
            self._obligations[obligation_id] = updated
        return updated

    def keep_open(self, obligation_id: str, reason_code: str) -> OutcomeObligation:
        with self._lock:
            current = self._require_locked(obligation_id)
            updated = stay_open(current, reason_code)
            self._obligations[obligation_id] = updated
        return updated

    def abandon_obligation(self, obligation_id: str, reason_code: str, at: float) -> OutcomeObligation:
        with self._lock:
            current = self._require_locked(obligation_id)
            updated = abandon(current, reason_code, at)
            self._obligations[obligation_id] = updated
        return updated

    def get(self, obligation_id: str) -> OutcomeObligation:
        with self._lock:
            return self._require_locked(obligation_id)

    def all(self) -> List[OutcomeObligation]:
        with self._lock:
            return list(self._obligations.values())

    def portfolio_status(self, now: float) -> Dict[str, int]:
        return horizon_honored(self.all(), now)

    def _require_locked(self, obligation_id: str) -> OutcomeObligation:
        """Caller must already hold self._lock (threading.Lock is not
        reentrant, so this must never acquire it itself)."""
        try:
            return self._obligations[obligation_id]
        except KeyError:
            raise KeyError(f"Unknown obligation_id '{obligation_id}'") from None
