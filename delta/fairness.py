"""Fairness bridge: resolved OutcomeObligations -> cohort-level
disparate-impact screening.

Extracted from sentinel_os's regulatory_checks.py: CohortDecision and
check_statistical_outcome_equity are copied faithfully --
  - the EEOC four-fifths rule (29 CFR 1607.4(D)): a group's favorable-
    outcome rate below 80% of the highest-rate group's is evidence of
    adverse impact;
  - the same minimum cohort size (30) before any statistical claim is
    made -- below it, this returns an INDETERMINATE finding, never a
    spurious pass or flag from too few observations;
  - the same PROBABILITY-WEIGHTED rate calculation, so a self-reported
    (single-category) record and a BISG-estimated (full posterior)
    record combine in one cohort with no special-casing: a self-report
    is just a distribution with all its mass on one category;
  - groups with less than 1.0 effective weight in the cohort are
    excluded from the comparison -- not enough signal to say anything.

NOT extracted, and never will be: group_distribution (race/ethnicity ->
probability) is not computed here. It's the caller's job, sourced from
wherever the caller's demographic estimation lives (e.g. BISG). This
module only consumes it -- same domain-blind posture as the source,
which never guesses what counts as "favorable" either.

Simplified from the source: RegulatoryFinding here drops the
`regulation`/RegulationCheckProfile fields the source's version carries
(tying a finding to a specific named regulation's check profile) --
that type wasn't read closely enough to extract faithfully, so it's
omitted rather than guessed at. A caller needing that binding attaches
it externally.
"""

from dataclasses import dataclass
from typing import Dict, List, Mapping

from .obligation import OUTCOME_RESOLVED, OutcomeIntegrityError, OutcomeObligation

MIN_COHORT_SIZE_FOR_STATISTICAL_TEST = 30
FOUR_FIFTHS_THRESHOLD = 0.8

ACTION_FLAG = "FLAG"


@dataclass(frozen=True)
class CohortDecision:
    subject_id: str
    favorable_outcome: bool
    group_distribution: Mapping[str, float]


@dataclass(frozen=True)
class RegulatoryFinding:
    check: str
    subject_id: str
    action: str
    classification: str
    score: float
    evidence: Dict


def cohort_favorable(obligation: OutcomeObligation) -> bool:
    """The two testability rules a RESOLVED obligation needs before it
    can enter a cohort test: must be RESOLVED, and must not be
    genuinely ambiguous (favorable=None)."""
    if obligation.state != OUTCOME_RESOLVED:
        raise OutcomeIntegrityError(obligation.obligation_id, [
            f"cannot enter a cohort test in state {obligation.state!r}; only "
            f"RESOLVED outcomes are testable -- substituting a default for an "
            f"unresolved one would put an unmeasured decision into a statistical finding"
        ])
    if obligation.favorable is None:
        raise OutcomeIntegrityError(obligation.obligation_id, [
            "resolved with favorable=None (genuinely ambiguous); a cohort test "
            "needs a favorable/unfavorable call, and coercing an ambiguous "
            "outcome to either one fabricates the input to a fairness statistic"
        ])
    return bool(obligation.favorable)


def to_cohort_decision(obligation: OutcomeObligation, group_distribution: Mapping[str, float]) -> CohortDecision:
    """Turn a RESOLVED obligation into the unit check_statistical_outcome_equity tests."""
    favorable_outcome = cohort_favorable(obligation)
    return CohortDecision(
        subject_id=str(obligation.subject_id or obligation.decision_fingerprint),
        favorable_outcome=favorable_outcome,
        group_distribution=group_distribution,
    )


def check_statistical_outcome_equity(
        cohort: List[CohortDecision],
        check_name: str = "statistical_outcome_equity_four_fifths",
) -> List[RegulatoryFinding]:
    """Four-fifths-rule disparate-impact screen across a COHORT of decisions.

    Returns [] when clean, one FLAG finding per group whose weighted
    favorable-outcome rate falls below 80% of the highest-rate group's,
    or one FLAG (indeterminate) finding when the cohort is too small or
    too few groups have coverage to compare.
    """
    if len(cohort) < MIN_COHORT_SIZE_FOR_STATISTICAL_TEST:
        return [RegulatoryFinding(
            check=check_name,
            subject_id=f"cohort:{len(cohort)}",
            action=ACTION_FLAG,
            classification="indeterminate_insufficient_cohort",
            score=0.0,
            evidence={
                "cohort_size": len(cohort),
                "minimum_required": MIN_COHORT_SIZE_FOR_STATISTICAL_TEST,
                "detail": "cohort is too small for a statistical disparate-impact "
                          "comparison to mean anything -- reporting indeterminate "
                          "rather than a spurious pass or flag",
            },
        )]

    weighted_favorable: Dict[str, float] = {}
    weighted_total: Dict[str, float] = {}
    for decision in cohort:
        for group, prob in decision.group_distribution.items():
            if prob <= 0:
                continue
            weighted_total[group] = weighted_total.get(group, 0.0) + prob
            if decision.favorable_outcome:
                weighted_favorable[group] = weighted_favorable.get(group, 0.0) + prob

    rates = {
        group: weighted_favorable.get(group, 0.0) / total
        for group, total in weighted_total.items()
        if total >= 1.0  # negligible-weight groups excluded
    }
    if len(rates) < 2:
        return [RegulatoryFinding(
            check=check_name,
            subject_id=f"cohort:{len(cohort)}",
            action=ACTION_FLAG,
            classification="indeterminate_insufficient_group_coverage",
            score=0.0,
            evidence={
                "cohort_size": len(cohort),
                "groups_with_sufficient_weight": sorted(rates),
                "detail": "fewer than two groups have enough estimated weight in "
                          "this cohort to compare -- cannot compute a four-fifths "
                          "ratio between groups that don't both have signal here",
            },
        )]

    highest_rate = max(rates.values())
    findings: List[RegulatoryFinding] = []
    if highest_rate > 0:
        for group, rate in sorted(rates.items()):
            ratio = rate / highest_rate
            if ratio < FOUR_FIFTHS_THRESHOLD:
                findings.append(RegulatoryFinding(
                    check=check_name,
                    subject_id=f"cohort:{len(cohort)}",
                    action=ACTION_FLAG,
                    classification="four_fifths_adverse_impact",
                    score=round(1.0 - ratio, 4),
                    evidence={
                        "group": group,
                        "group_favorable_rate": round(rate, 4),
                        "highest_group_favorable_rate": round(highest_rate, 4),
                        "ratio": round(ratio, 4),
                        "threshold": FOUR_FIFTHS_THRESHOLD,
                    },
                ))
    return findings
