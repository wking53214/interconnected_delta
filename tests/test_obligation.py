import pytest

from delta.obligation import (
    OUTCOME_ABANDONED,
    OUTCOME_OPEN,
    OUTCOME_RESOLVED,
    OUTCOME_STATES,
    PROVENANCE_ESTIMATED,
    PROVENANCE_VERIFIED,
    REASON_DECISION_SUPERSEDED,
    REASON_GENUINELY_AMBIGUOUS,
    REASON_NOT_YET_DUE,
    MaturationRule,
    OutcomeIntegrityError,
    OutcomeObligation,
    abandon,
    horizon_honored,
    is_overdue,
    open_obligation,
    resolve,
    stay_open,
    validate_obligation,
)

DAY = 86400.0


# --- MaturationRule ---

@pytest.mark.parametrize("declaration,kind,seconds", [
    ("loan_performance@24mo", "loan_performance", 24 * 30 * DAY),
    ("claim_cost@1y", "claim_cost", 365 * DAY),
    ("followup@7d", "followup", 7 * DAY),
    ("check@1h", "check", 3600.0),
])
def test_maturation_rule_parse(declaration, kind, seconds):
    rule = MaturationRule.parse(declaration)
    assert rule.kind == kind
    assert rule.horizon_seconds == seconds


def test_maturation_rule_declaration_round_trips():
    rule = MaturationRule.parse("loan_performance@24mo")
    assert rule.declaration() == "loan_performance@24mo"


def test_maturation_rule_rejects_unparseable_declaration():
    with pytest.raises(ValueError):
        MaturationRule.parse("not a valid declaration")


def test_maturation_rule_rejects_zero_horizon():
    with pytest.raises(ValueError):
        MaturationRule.parse("x@0d")


def test_maturation_rule_rejects_unknown_unit():
    with pytest.raises(ValueError):
        MaturationRule.parse("x@5w")  # weeks not a supported unit


# --- open_obligation ---

def test_open_obligation_computes_expected_by_from_rule():
    rule = MaturationRule(kind="loan_performance", horizon_seconds=730 * DAY)
    obligation = open_obligation("obl1", "fp-abc", "mortgage", rule, opened_at=1000.0, subject_id="patient1")
    assert obligation.state == OUTCOME_OPEN
    assert obligation.reason_code == REASON_NOT_YET_DUE
    assert obligation.expected_by == 1000.0 + 730 * DAY
    validate_obligation(obligation)  # must not raise


# --- resolve / validation ---

def test_resolve_requires_valid_provenance():
    rule = MaturationRule(kind="x", horizon_seconds=DAY)
    obligation = open_obligation("obl1", "fp-abc", "d", rule, opened_at=0.0)
    resolved = resolve(obligation, resolved_at=DAY + 1, resolved_value={"paid": True},
                        provenance=PROVENANCE_VERIFIED, favorable=True)
    assert resolved.state == OUTCOME_RESOLVED
    assert resolved.favorable is True
    assert resolved.reason_code is None


def test_resolve_rejects_invalid_provenance():
    rule = MaturationRule(kind="x", horizon_seconds=DAY)
    obligation = open_obligation("obl1", "fp-abc", "d", rule, opened_at=0.0)
    with pytest.raises(OutcomeIntegrityError):
        resolve(obligation, resolved_at=DAY + 1, resolved_value={"paid": True},
                provenance="just trust me", favorable=True)


def test_resolve_estimated_requires_method():
    rule = MaturationRule(kind="x", horizon_seconds=DAY)
    obligation = open_obligation("obl1", "fp-abc", "d", rule, opened_at=0.0)
    with pytest.raises(OutcomeIntegrityError, match="ESTIMATED"):
        resolve(obligation, resolved_at=DAY + 1, resolved_value={"x": 1},
                provenance=PROVENANCE_ESTIMATED, favorable=True, method=None)


def test_resolve_estimated_with_method_succeeds():
    rule = MaturationRule(kind="x", horizon_seconds=DAY)
    obligation = open_obligation("obl1", "fp-abc", "d", rule, opened_at=0.0)
    resolved = resolve(obligation, resolved_at=DAY + 1, resolved_value={"x": 1},
                        provenance=PROVENANCE_ESTIMATED, favorable=False, method="cohort_average")
    assert resolved.resolution_method == "cohort_average"


def test_resolve_requires_nonempty_resolved_value():
    rule = MaturationRule(kind="x", horizon_seconds=DAY)
    obligation = open_obligation("obl1", "fp-abc", "d", rule, opened_at=0.0)
    with pytest.raises(OutcomeIntegrityError):
        resolve(obligation, resolved_at=DAY + 1, resolved_value={},
                provenance=PROVENANCE_VERIFIED, favorable=True)


def test_resolve_allows_favorable_none_for_genuine_ambiguity():
    rule = MaturationRule(kind="x", horizon_seconds=DAY)
    obligation = open_obligation("obl1", "fp-abc", "d", rule, opened_at=0.0)
    resolved = resolve(obligation, resolved_at=DAY + 1, resolved_value={"note": "unclear"},
                        provenance=PROVENANCE_VERIFIED, favorable=None)
    assert resolved.favorable is None
    validate_obligation(resolved)  # must not raise -- None is a legitimate resolved state


# --- stay_open ---

def test_stay_open_restates_reason():
    rule = MaturationRule(kind="x", horizon_seconds=DAY)
    obligation = open_obligation("obl1", "fp-abc", "d", rule, opened_at=0.0)
    restated = stay_open(obligation, REASON_GENUINELY_AMBIGUOUS)
    assert restated.state == OUTCOME_OPEN
    assert restated.reason_code == REASON_GENUINELY_AMBIGUOUS


def test_stay_open_rejects_invalid_reason():
    rule = MaturationRule(kind="x", horizon_seconds=DAY)
    obligation = open_obligation("obl1", "fp-abc", "d", rule, opened_at=0.0)
    with pytest.raises(OutcomeIntegrityError):
        stay_open(obligation, "made_up_reason")


# --- abandon ---

def test_abandon_requires_valid_reason():
    rule = MaturationRule(kind="x", horizon_seconds=DAY)
    obligation = open_obligation("obl1", "fp-abc", "d", rule, opened_at=0.0)
    dropped = abandon(obligation, REASON_DECISION_SUPERSEDED, at=DAY)
    assert dropped.state == OUTCOME_ABANDONED
    assert dropped.favorable is None


def test_abandon_rejects_invalid_reason():
    rule = MaturationRule(kind="x", horizon_seconds=DAY)
    obligation = open_obligation("obl1", "fp-abc", "d", rule, opened_at=0.0)
    with pytest.raises(OutcomeIntegrityError):
        abandon(obligation, "not_a_real_reason", at=DAY)


# --- is_overdue ---

def test_is_overdue_true_past_horizon():
    rule = MaturationRule(kind="x", horizon_seconds=DAY)
    obligation = open_obligation("obl1", "fp-abc", "d", rule, opened_at=0.0)
    assert is_overdue(obligation, now=DAY + 1) is True


def test_is_overdue_false_before_horizon():
    rule = MaturationRule(kind="x", horizon_seconds=DAY)
    obligation = open_obligation("obl1", "fp-abc", "d", rule, opened_at=0.0)
    assert is_overdue(obligation, now=DAY - 1) is False


def test_is_overdue_false_once_resolved():
    rule = MaturationRule(kind="x", horizon_seconds=DAY)
    obligation = open_obligation("obl1", "fp-abc", "d", rule, opened_at=0.0)
    resolved = resolve(obligation, resolved_at=DAY + 1, resolved_value={"x": 1},
                        provenance=PROVENANCE_VERIFIED, favorable=True)
    assert is_overdue(resolved, now=DAY * 100) is False


# --- horizon_honored ---

# --- Cross-field validation gaps caught by adversarial review ---

def test_validate_rejects_open_with_resolution_provenance_set():
    # A directly-constructed, self-contradictory record (bypassing the
    # helper functions) that claims to be OPEN but also carries a
    # resolution_provenance -- must be caught.
    obligation = OutcomeObligation(
        obligation_id="o1", decision_fingerprint="fp1", domain="d", obligation_kind="x",
        opened_at=0.0, expected_by=DAY, state=OUTCOME_OPEN, reason_code=REASON_NOT_YET_DUE,
        resolution_provenance=PROVENANCE_VERIFIED,
    )
    with pytest.raises(OutcomeIntegrityError):
        validate_obligation(obligation)


def test_validate_rejects_open_with_resolution_method_set():
    obligation = OutcomeObligation(
        obligation_id="o1", decision_fingerprint="fp1", domain="d", obligation_kind="x",
        opened_at=0.0, expected_by=DAY, state=OUTCOME_OPEN, reason_code=REASON_NOT_YET_DUE,
        resolution_method="some_method",
    )
    with pytest.raises(OutcomeIntegrityError):
        validate_obligation(obligation)


def test_validate_rejects_resolved_at_before_opened_at():
    obligation = OutcomeObligation(
        obligation_id="o1", decision_fingerprint="fp1", domain="d", obligation_kind="x",
        opened_at=100.0, expected_by=200.0, state=OUTCOME_RESOLVED, reason_code=None,
        resolved_at=50.0,  # before opened_at=100.0
        resolved_value={"x": 1}, resolution_provenance=PROVENANCE_VERIFIED, favorable=True,
    )
    with pytest.raises(OutcomeIntegrityError, match="cannot be before"):
        validate_obligation(obligation)


def test_validate_rejects_abandoned_without_resolved_at():
    obligation = OutcomeObligation(
        obligation_id="o1", decision_fingerprint="fp1", domain="d", obligation_kind="x",
        opened_at=0.0, expected_by=DAY, state=OUTCOME_ABANDONED,
        reason_code=REASON_DECISION_SUPERSEDED, resolved_at=None,
    )
    with pytest.raises(OutcomeIntegrityError):
        validate_obligation(obligation)


def test_validate_rejects_abandoned_with_resolved_value():
    obligation = OutcomeObligation(
        obligation_id="o1", decision_fingerprint="fp1", domain="d", obligation_kind="x",
        opened_at=0.0, expected_by=DAY, state=OUTCOME_ABANDONED,
        reason_code=REASON_DECISION_SUPERSEDED, resolved_at=DAY,
        resolved_value={"note": "not actually resolved"},
    )
    with pytest.raises(OutcomeIntegrityError):
        validate_obligation(obligation)


def test_validate_rejects_abandoned_with_resolution_provenance():
    obligation = OutcomeObligation(
        obligation_id="o1", decision_fingerprint="fp1", domain="d", obligation_kind="x",
        opened_at=0.0, expected_by=DAY, state=OUTCOME_ABANDONED,
        reason_code=REASON_DECISION_SUPERSEDED, resolved_at=DAY,
        resolution_provenance=PROVENANCE_VERIFIED,
    )
    with pytest.raises(OutcomeIntegrityError):
        validate_obligation(obligation)


def test_valid_abandoned_obligation_passes():
    # abandon() produces exactly this shape -- confirms the new checks
    # don't reject the helper function's own legitimate output.
    rule = MaturationRule(kind="x", horizon_seconds=DAY)
    obligation = open_obligation("o1", "fp1", "d", rule, opened_at=0.0)
    dropped = abandon(obligation, REASON_DECISION_SUPERSEDED, at=DAY)
    validate_obligation(dropped)  # must not raise


def test_horizon_honored_counts_by_state_and_overdue():
    rule = MaturationRule(kind="x", horizon_seconds=DAY)
    open_ob = open_obligation("o1", "fp1", "d", rule, opened_at=0.0)
    resolved_ob = resolve(
        open_obligation("o2", "fp2", "d", rule, opened_at=0.0),
        resolved_at=DAY + 1, resolved_value={"x": 1}, provenance=PROVENANCE_VERIFIED, favorable=True,
    )
    abandoned_ob = abandon(open_obligation("o3", "fp3", "d", rule, opened_at=0.0), REASON_DECISION_SUPERSEDED, at=DAY)

    counts = horizon_honored([open_ob, resolved_ob, abandoned_ob], now=DAY * 2)
    assert counts["OPEN"] == 1
    assert counts["RESOLVED"] == 1
    assert counts["ABANDONED"] == 1
    assert counts["overdue"] == 1  # only open_ob is overdue at DAY*2
