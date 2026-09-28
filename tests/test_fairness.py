import pytest

from delta.fairness import (
    CohortDecision,
    check_statistical_outcome_equity,
    cohort_favorable,
    to_cohort_decision,
)
from delta.obligation import (
    OUTCOME_OPEN,
    PROVENANCE_VERIFIED,
    MaturationRule,
    OutcomeIntegrityError,
    open_obligation,
    resolve,
)

DAY = 86400.0


def resolved_obligation(favorable, subject_id="s1", fp="fp1"):
    rule = MaturationRule(kind="x", horizon_seconds=DAY)
    ob = open_obligation("o1", fp, "d", rule, opened_at=0.0, subject_id=subject_id)
    return resolve(ob, resolved_at=DAY + 1, resolved_value={"x": 1},
                    provenance=PROVENANCE_VERIFIED, favorable=favorable)


# --- cohort_favorable ---

def test_cohort_favorable_true():
    assert cohort_favorable(resolved_obligation(True)) is True


def test_cohort_favorable_false():
    assert cohort_favorable(resolved_obligation(False)) is False


def test_cohort_favorable_rejects_unresolved():
    rule = MaturationRule(kind="x", horizon_seconds=DAY)
    ob = open_obligation("o1", "fp1", "d", rule, opened_at=0.0)
    with pytest.raises(OutcomeIntegrityError, match="RESOLVED"):
        cohort_favorable(ob)


def test_cohort_favorable_rejects_ambiguous():
    ob = resolved_obligation(None)
    with pytest.raises(OutcomeIntegrityError, match="ambiguous"):
        cohort_favorable(ob)


# --- to_cohort_decision ---

def test_to_cohort_decision_uses_subject_id():
    ob = resolved_obligation(True, subject_id="applicant42")
    cd = to_cohort_decision(ob, {"asian": 1.0})
    assert cd.subject_id == "applicant42"
    assert cd.favorable_outcome is True
    assert cd.group_distribution == {"asian": 1.0}


def test_to_cohort_decision_falls_back_to_fingerprint_when_no_subject_id():
    ob = resolved_obligation(True, subject_id=None, fp="fp-xyz")
    cd = to_cohort_decision(ob, {"asian": 1.0})
    assert cd.subject_id == "fp-xyz"


# --- check_statistical_outcome_equity ---

def test_returns_indeterminate_below_minimum_cohort_size():
    cohort = [CohortDecision(f"s{i}", True, {"a": 1.0}) for i in range(10)]
    findings = check_statistical_outcome_equity(cohort)
    assert len(findings) == 1
    assert findings[0].classification == "indeterminate_insufficient_cohort"


def test_returns_indeterminate_below_two_group_coverage():
    # 30 decisions, but all in ONE group -> can't compare rates.
    cohort = [CohortDecision(f"s{i}", i % 2 == 0, {"a": 1.0}) for i in range(30)]
    findings = check_statistical_outcome_equity(cohort)
    assert len(findings) == 1
    assert findings[0].classification == "indeterminate_insufficient_group_coverage"


def test_returns_empty_when_rates_are_equal():
    cohort = []
    for i in range(30):
        cohort.append(CohortDecision(f"a{i}", i % 2 == 0, {"group_a": 1.0}))
    for i in range(30):
        cohort.append(CohortDecision(f"b{i}", i % 2 == 0, {"group_b": 1.0}))
    findings = check_statistical_outcome_equity(cohort)
    assert findings == []


def test_flags_four_fifths_adverse_impact():
    cohort = []
    # group_a: 90% favorable (27/30)
    for i in range(30):
        cohort.append(CohortDecision(f"a{i}", i < 27, {"group_a": 1.0}))
    # group_b: 50% favorable (15/30) -- ratio 0.5/0.9 = 0.556 < 0.8
    for i in range(30):
        cohort.append(CohortDecision(f"b{i}", i < 15, {"group_b": 1.0}))

    findings = check_statistical_outcome_equity(cohort)
    assert len(findings) == 1
    assert findings[0].classification == "four_fifths_adverse_impact"
    assert findings[0].evidence["group"] == "group_b"


def test_probability_weighted_bisg_style_distribution_contributes_partially():
    # A record with a BISG-style posterior spread across two groups
    # should contribute PARTIAL weight to each, not be hard-assigned.
    cohort = []
    for i in range(15):
        cohort.append(CohortDecision(f"a{i}", True, {"group_a": 0.7, "group_b": 0.3}))
    for i in range(15):
        cohort.append(CohortDecision(f"b{i}", False, {"group_a": 0.3, "group_b": 0.7}))
    # Enough total weight to clear the 1.0-effective-decision floor for both groups.
    findings = check_statistical_outcome_equity(cohort)
    # Just confirming this runs without raising and produces a real comparison
    # (not asserting a specific verdict -- the point is mixed-distribution input works).
    assert isinstance(findings, list)


# --- Input validation caught by adversarial review ---

def test_cohort_decision_rejects_negative_probability():
    with pytest.raises(ValueError, match="negative"):
        CohortDecision("s1", True, {"group_a": -0.1})


def test_cohort_decision_rejects_distribution_summing_over_one():
    with pytest.raises(ValueError, match="exceeds 1.0"):
        CohortDecision("s1", True, {"group_a": 0.7, "group_b": 0.6})


def test_cohort_decision_allows_distribution_summing_to_exactly_one():
    cd = CohortDecision("s1", True, {"group_a": 0.6, "group_b": 0.4})
    assert cd.group_distribution == {"group_a": 0.6, "group_b": 0.4}


def test_cohort_decision_rejects_non_numeric_probability():
    with pytest.raises(ValueError, match="must be a number"):
        CohortDecision("s1", True, {"group_a": "high"})


def test_empty_distribution_records_do_not_count_toward_minimum_cohort_size():
    # Regression (adversarial-review major): 30 raw records, but only 5
    # carry any real group_distribution signal -- must be treated as an
    # insufficient (5, not 30) cohort, not silently pass the size gate.
    cohort = [CohortDecision(f"s{i}", True, {"group_a": 1.0}) for i in range(5)]
    cohort += [CohortDecision(f"empty{i}", True, {}) for i in range(25)]

    findings = check_statistical_outcome_equity(cohort)
    assert len(findings) == 1
    assert findings[0].classification == "indeterminate_insufficient_cohort"
    assert findings[0].evidence["informative_cohort_size"] == 5
    assert findings[0].evidence["cohort_size"] == 30


def test_negligible_weight_group_excluded():
    cohort = []
    for i in range(30):
        cohort.append(CohortDecision(f"a{i}", True, {"group_a": 1.0}))
    # One single decision with a tiny sliver of weight in group_c -- total
    # weight for group_c is 0.01, well under the 1.0 floor to be compared.
    cohort.append(CohortDecision("c0", True, {"group_a": 0.99, "group_c": 0.01}))

    findings = check_statistical_outcome_equity(cohort)
    for f in findings:
        assert f.evidence.get("group") != "group_c"
