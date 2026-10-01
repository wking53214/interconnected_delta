"""Two worked examples:

1. The pediatric alpha -> zeta -> beta pipeline, extended through delta's
   ledger. Clinical escalation/discharge decisions resolve immediately
   (there's nothing to mature later), so this shows the LEDGER half of
   delta but deliberately does NOT open an obligation -- matching the
   original private implementation's own posture that a domain whose
   outcome is known at decision time genuinely owes nothing later.

2. A synthetic mortgage-style scenario, hand-built directly as
   beta.Decision objects (no mortgage alpha/zeta wiring exists yet --
   delta operates on beta.Decision regardless of how it was produced).
   This exercises the part example 1 doesn't: a decision that DOES
   mature later, gets resolved months afterward, and feeds a cohort of
   resolved outcomes into the four-fifths fairness screen.

Run: PYTHONPATH=.:/path/to/zeta:/path/to/beta:/path/to/alpha python3 examples/full_pipeline.py
"""

from datetime import datetime, timedelta

from zeta import Combination, LockEvaluator, LockRegistry, LockSpec
from alpha import VitalsObservation, observe
from beta import Decision, DecisionEngine, DecisionRule, DecisionRuleRegistry

from delta import (
    DecisionLedger,
    DecisionObligationTracker,
    MaturationRule,
    PROVENANCE_VERIFIED,
    CohortDecision,
    check_statistical_outcome_equity,
    to_cohort_decision,
)


def example_1_pediatric_ledger():
    print("=" * 70)
    print("EXAMPLE 1: pediatric pipeline -> delta's ledger (no obligation)")
    print("=" * 70)

    lock_registry = LockRegistry([
        LockSpec(lock_id="sepsis_lock", required_keys=("septic_shock", "respiratory_distress", "hypovolemic_shock"),
                  combination=Combination.OR, dwell_threshold=3, force=True, lock_seconds=3600),
        LockSpec(lock_id="abnormal_vitals_lock",
                  required_keys=("critical_o2", "warning_o2", "tachycardia", "bradycardia", "tachypnea", "fever", "hypothermia"),
                  combination=Combination.OR, dwell_threshold=2),
    ])
    decision_registry = DecisionRuleRegistry([
        DecisionRule(decision_id="escalate_sepsis", decision="ESCALATE", open_locks=("sepsis_lock",),
                      priority=10, reasoning_template="{decision}: sepsis pattern detected",
                      instructions="Go to emergency room immediately."),
        DecisionRule(decision_id="hold_abnormal", decision="HOLD", open_locks=("abnormal_vitals_lock",),
                      closed_locks=("sepsis_lock",), priority=5, reasoning_template="{decision}: monitoring",
                      instructions="Continue inpatient monitoring."),
        DecisionRule(decision_id="discharge_safe", decision="DISCHARGE_SAFE",
                      closed_locks=("sepsis_lock", "abnormal_vitals_lock"), priority=0,
                      reasoning_template="{decision}: vitals normal", instructions="Discharge with follow-up."),
    ])

    lock_evaluator = LockEvaluator(lock_registry)
    decision_engine = DecisionEngine(decision_registry, lock_registry)
    ledger = DecisionLedger()
    obligations = DecisionObligationTracker()  # no maturation rules registered -- pediatric decisions resolve at discharge

    t0 = datetime(2026, 10, 1, 8, 0, 0)
    vitals = VitalsObservation(heart_rate=148, oxygen_saturation=90.0, respiratory_rate=36, temperature=39.6, age_months=14)
    keys = observe(vitals)
    lock_results = lock_evaluator.evaluate_all("patient_1", keys, t0)
    decision = decision_engine.decide("patient_1", keys, lock_results, t0)

    entry = ledger.append(decision)
    print(f"\nDecision: {decision.decision}  (fingerprint {decision.decision_fingerprint[:12]}...)")
    print(f"Ledger entry: audit_id={entry.audit_id}  immutable_hash={entry.immutable_hash[:12]}...")
    assert ledger.verify_chain_integrity(), "ledger chain failed to verify"
    print("Chain verifies: True")

    obligation = obligations.open_for_decision(decision, domain="clinical", opened_at=t0.timestamp())
    print(f"Obligation opened: {obligation}  (None is correct -- clinical escalation resolves at discharge, not later)")


def example_2_mortgage_obligation_and_fairness():
    print("\n" + "=" * 70)
    print("EXAMPLE 2: mortgage-style decision -> obligation matures -> fairness screen")
    print("=" * 70)

    ledger = DecisionLedger()
    obligations = DecisionObligationTracker()
    obligations.register_maturation("mortgage_approve", MaturationRule(kind="loan_performance", horizon_seconds=730 * 86400))

    t0 = datetime(2026, 1, 1)

    # Simulate 40 mortgage approvals across two demographic groups, with
    # group_b's eventual repayment outcomes deliberately worse -- enough
    # to trip the four-fifths screen. group_distribution here stands in
    # for what a real BISG estimate would provide.
    cohort_inputs = []
    for i in range(20):
        cohort_inputs.append(("group_a", i < 18, f"applicant_a{i}"))  # 18/20 = 90% favorable
    for i in range(20):
        cohort_inputs.append(("group_b", i < 10, f"applicant_b{i}"))  # 10/20 = 50% favorable

    resolved_cohort = []
    for idx, (group, will_repay, applicant_id) in enumerate(cohort_inputs):
        decision = Decision(
            entity_id=applicant_id, decision_id="mortgage_approve", decision="APPROVED",
            timestamp=t0 + timedelta(days=idx), confidence=0.9,
            decision_fingerprint=f"fp-{applicant_id}",
            reasoning="Credit profile met approval thresholds.",
        )
        ledger.append(decision)
        obligation = obligations.open_for_decision(decision, domain="mortgage", opened_at=decision.timestamp.timestamp())

        # 24 months later, the loan's performance is known.
        resolved = obligations.resolve_obligation(
            obligation.obligation_id,
            resolved_at=(decision.timestamp + timedelta(days=730)).timestamp(),
            resolved_value={"paid_on_time": will_repay},
            provenance=PROVENANCE_VERIFIED,
            favorable=will_repay,
        )
        resolved_cohort.append(to_cohort_decision(resolved, {group: 1.0}))

    assert ledger.verify_chain_integrity(), "ledger chain failed to verify"
    print(f"\nLedger entries: {len(ledger)}, chain verifies: True")
    print(f"Portfolio status: {obligations.portfolio_status(now=(t0 + timedelta(days=1000)).timestamp())}")

    findings = check_statistical_outcome_equity(resolved_cohort)
    print(f"\nFairness screen findings: {len(findings)}")
    for f in findings:
        print(f"  [{f.classification}] group={f.evidence.get('group')} "
              f"rate={f.evidence.get('group_favorable_rate')} "
              f"vs highest={f.evidence.get('highest_group_favorable_rate')} "
              f"(ratio {f.evidence.get('ratio')}, threshold {f.evidence.get('threshold')})")


if __name__ == "__main__":
    example_1_pediatric_ledger()
    example_2_mortgage_obligation_and_fairness()
