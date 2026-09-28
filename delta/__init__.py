"""delta: records beta.Decisions, tracks their outcome obligations,
verifies them, and feeds resolved outcomes into cohort-level fairness
screening.

Three pieces, two different kinds of provenance:

  - ledger.py: DecisionLedger, generalized from PERCEIVE's
    ImmutableAuditLedger (a real, working SHA256 hash chain) to chain
    beta.Decisions instead of PERCEIVE's specific entry shape.

  - obligation.py: MaturationRule / OutcomeObligation / the OPEN ->
    RESOLVED|ABANDONED lifecycle, extracted near-verbatim from
    sentinel_os's outcome_v1.py -- already a mature, rigorously
    validated, domain-blind module. Not reinvented, just relinked to
    Decision.decision_fingerprint instead of a raw ledger row's hash.

  - fairness.py: CohortDecision / check_statistical_outcome_equity,
    extracted from sentinel_os's regulatory_checks.py -- the real EEOC
    four-fifths disparate-impact screen (29 CFR 1607.4(D)), already
    wired to consume RESOLVED obligations, just never fed real
    decisions before.

  - decision_obligations.py: the one genuinely NEW piece -- a thin
    adapter (DecisionObligationTracker) linking beta.Decision to the
    obligation lifecycle, since outcome_v1.py has no existing adapter
    for beta.Decision specifically (it worked on generic decision rows).

See each module's docstring for exactly what's extracted vs. new.
"""

from .ledger import DecisionLedger, LedgerEntry
from .obligation import (
    ABANDONED_REASONS,
    OPEN_REASONS,
    OUTCOME_ABANDONED,
    OUTCOME_OPEN,
    OUTCOME_RESOLVED,
    OUTCOME_STATES,
    PROVENANCE_ATTESTED,
    PROVENANCE_ESTIMATED,
    PROVENANCE_STAMPS,
    PROVENANCE_VERIFIED,
    REASON_DATA_SOURCE_UNREACHABLE,
    REASON_DECISION_SUPERSEDED,
    REASON_GENUINELY_AMBIGUOUS,
    REASON_INSUFFICIENT_COHORT,
    REASON_NOT_YET_DUE,
    REASON_RETENTION_EXPIRED,
    REASON_SUBJECT_WITHDREW,
    MaturationRule,
    OutcomeIntegrityError,
    OutcomeObligation,
    horizon_honored,
    is_overdue,
)
from .decision_obligations import DecisionObligationTracker
from .fairness import (
    FOUR_FIFTHS_THRESHOLD,
    MIN_COHORT_SIZE_FOR_STATISTICAL_TEST,
    CohortDecision,
    RegulatoryFinding,
    check_statistical_outcome_equity,
    cohort_favorable,
    to_cohort_decision,
)

__all__ = [
    "DecisionLedger",
    "LedgerEntry",
    "MaturationRule",
    "OutcomeObligation",
    "OutcomeIntegrityError",
    "DecisionObligationTracker",
    "is_overdue",
    "horizon_honored",
    "OUTCOME_OPEN",
    "OUTCOME_RESOLVED",
    "OUTCOME_ABANDONED",
    "OUTCOME_STATES",
    "OPEN_REASONS",
    "ABANDONED_REASONS",
    "REASON_NOT_YET_DUE",
    "REASON_INSUFFICIENT_COHORT",
    "REASON_DATA_SOURCE_UNREACHABLE",
    "REASON_GENUINELY_AMBIGUOUS",
    "REASON_SUBJECT_WITHDREW",
    "REASON_DECISION_SUPERSEDED",
    "REASON_RETENTION_EXPIRED",
    "PROVENANCE_VERIFIED",
    "PROVENANCE_ATTESTED",
    "PROVENANCE_ESTIMATED",
    "PROVENANCE_STAMPS",
    "CohortDecision",
    "RegulatoryFinding",
    "check_statistical_outcome_equity",
    "cohort_favorable",
    "to_cohort_decision",
    "FOUR_FIFTHS_THRESHOLD",
    "MIN_COHORT_SIZE_FOR_STATISTICAL_TEST",
]

__version__ = "0.1.0"
