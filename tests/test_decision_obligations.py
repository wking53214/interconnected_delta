from datetime import datetime

import pytest
from beta import Decision

from delta.decision_obligations import DecisionObligationTracker
from delta.obligation import (
    OUTCOME_OPEN,
    OUTCOME_RESOLVED,
    PROVENANCE_VERIFIED,
    REASON_DATA_SOURCE_UNREACHABLE,
    REASON_DECISION_SUPERSEDED,
    MaturationRule,
)

T0 = datetime(2026, 1, 1)
DAY = 86400.0


def make_decision(decision_id="loan_approve", fp="fp-1", entity_id="applicant1"):
    return Decision(
        entity_id=entity_id, decision_id=decision_id, decision="APPROVED",
        timestamp=T0, confidence=1.0, decision_fingerprint=fp,
    )


def test_open_for_decision_returns_none_when_no_rule_registered():
    tracker = DecisionObligationTracker()
    result = tracker.open_for_decision(make_decision(decision_id="ivr_call_end"), domain="ivr", opened_at=0.0)
    assert result is None


def test_open_for_decision_opens_when_rule_registered():
    tracker = DecisionObligationTracker()
    tracker.register_maturation("loan_approve", MaturationRule(kind="loan_performance", horizon_seconds=730 * DAY))

    obligation = tracker.open_for_decision(make_decision(), domain="mortgage", opened_at=0.0)

    assert obligation is not None
    assert obligation.state == OUTCOME_OPEN
    assert obligation.subject_id == "applicant1"
    assert obligation.decision_fingerprint == "fp-1"
    assert obligation in tracker.all()


def test_open_for_decision_rejects_duplicate_open():
    tracker = DecisionObligationTracker()
    tracker.register_maturation("loan_approve", MaturationRule(kind="loan_performance", horizon_seconds=DAY))
    decision = make_decision()
    tracker.open_for_decision(decision, domain="mortgage", opened_at=0.0)
    with pytest.raises(ValueError, match="already opened"):
        tracker.open_for_decision(decision, domain="mortgage", opened_at=0.0)


def test_resolve_obligation_updates_tracked_state():
    tracker = DecisionObligationTracker()
    tracker.register_maturation("loan_approve", MaturationRule(kind="loan_performance", horizon_seconds=DAY))
    obligation = tracker.open_for_decision(make_decision(), domain="mortgage", opened_at=0.0)

    resolved = tracker.resolve_obligation(
        obligation.obligation_id, resolved_at=DAY + 1,
        resolved_value={"paid_on_time": True}, provenance=PROVENANCE_VERIFIED, favorable=True,
    )

    assert resolved.state == OUTCOME_RESOLVED
    assert tracker.get(obligation.obligation_id).state == OUTCOME_RESOLVED


def test_keep_open_restates_reason():
    tracker = DecisionObligationTracker()
    tracker.register_maturation("loan_approve", MaturationRule(kind="loan_performance", horizon_seconds=DAY))
    obligation = tracker.open_for_decision(make_decision(), domain="mortgage", opened_at=0.0)

    restated = tracker.keep_open(obligation.obligation_id, REASON_DATA_SOURCE_UNREACHABLE)
    assert restated.reason_code == REASON_DATA_SOURCE_UNREACHABLE


def test_abandon_obligation():
    tracker = DecisionObligationTracker()
    tracker.register_maturation("loan_approve", MaturationRule(kind="loan_performance", horizon_seconds=DAY))
    obligation = tracker.open_for_decision(make_decision(), domain="mortgage", opened_at=0.0)

    dropped = tracker.abandon_obligation(obligation.obligation_id, REASON_DECISION_SUPERSEDED, at=DAY)
    from delta.obligation import OUTCOME_ABANDONED
    assert dropped.state == OUTCOME_ABANDONED


def test_get_unknown_obligation_raises_keyerror():
    tracker = DecisionObligationTracker()
    with pytest.raises(KeyError):
        tracker.get("nonexistent")


def test_portfolio_status_reflects_all_obligations():
    tracker = DecisionObligationTracker()
    tracker.register_maturation("loan_approve", MaturationRule(kind="loan_performance", horizon_seconds=DAY))
    tracker.open_for_decision(make_decision(decision_id="loan_approve", fp="fp1", entity_id="a1"), domain="mortgage", opened_at=0.0)
    tracker.open_for_decision(make_decision(decision_id="loan_approve", fp="fp2", entity_id="a2"), domain="mortgage", opened_at=0.0)

    status = tracker.portfolio_status(now=DAY * 2)
    assert status["OPEN"] == 2
    assert status["overdue"] == 2


def test_multiple_decision_ids_with_different_rules():
    tracker = DecisionObligationTracker()
    tracker.register_maturation("loan_approve", MaturationRule(kind="loan_performance", horizon_seconds=730 * DAY))
    tracker.register_maturation("claim_settle", MaturationRule(kind="claim_cost", horizon_seconds=365 * DAY))

    loan_ob = tracker.open_for_decision(make_decision(decision_id="loan_approve", fp="fpA"), domain="mortgage", opened_at=0.0)
    claim_ob = tracker.open_for_decision(make_decision(decision_id="claim_settle", fp="fpB"), domain="insurance", opened_at=0.0)

    assert loan_ob.obligation_kind == "loan_performance"
    assert claim_ob.obligation_kind == "claim_cost"
