"""OutcomeObligation: a durable, typed record of what a Decision still owes.

Extracted faithfully from sentinel_os's outcome_v1.py -- an already
mature, well-tested, domain-blind module. This is NOT a reinvention;
the state machine, validation rules, and provenance discipline below
match the source exactly, adapted only to link against beta.Decision
(via decision_fingerprint) instead of a raw decision-row dict.

THE PROVENANCE RULE (from the source, unchanged): every claim is
stamped verified, attested, or estimated -- never interchangeable.
An unknown obligation states WHY it's unknown (a bounded reason, never
free text) and WHAT would close it (the horizon).

TWO RECORDS, TWO LIFESPANS (from the source, unchanged): a Decision
closes permanently at decision time. An Obligation is separate and can
stay open indefinitely, maturing on its own declared schedule. The
Obligation points AT the Decision (by decision_fingerprint); the
Decision never points at the Obligation, because it's already closed.

is_overdue is computed, never stored -- two timestamps and a
comparison can't be quietly set to False by whoever wants a clean
report.
"""

import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Optional, Tuple

# --- Provenance stamps (event_v1.py:69-71 in the source, values copied exactly) ---
PROVENANCE_VERIFIED = "verified"
PROVENANCE_ATTESTED = "attested"
PROVENANCE_ESTIMATED = "estimated"
PROVENANCE_STAMPS: Tuple[str, ...] = (PROVENANCE_VERIFIED, PROVENANCE_ATTESTED, PROVENANCE_ESTIMATED)

# --- Lifecycle states ---
OUTCOME_OPEN = "OPEN"
OUTCOME_RESOLVED = "RESOLVED"
OUTCOME_ABANDONED = "ABANDONED"
OUTCOME_STATES: Tuple[str, ...] = (OUTCOME_OPEN, OUTCOME_RESOLVED, OUTCOME_ABANDONED)

# --- Bounded reason vocabularies (never free text) ---
REASON_NOT_YET_DUE = "not_yet_due"
REASON_INSUFFICIENT_COHORT = "insufficient_cohort"
REASON_DATA_SOURCE_UNREACHABLE = "data_source_unreachable"
REASON_GENUINELY_AMBIGUOUS = "genuinely_ambiguous"
OPEN_REASONS: Tuple[str, ...] = (
    REASON_NOT_YET_DUE,
    REASON_INSUFFICIENT_COHORT,
    REASON_DATA_SOURCE_UNREACHABLE,
    REASON_GENUINELY_AMBIGUOUS,
)

REASON_SUBJECT_WITHDREW = "subject_withdrew"
REASON_DECISION_SUPERSEDED = "decision_superseded"
REASON_RETENTION_EXPIRED = "retention_expired"
ABANDONED_REASONS: Tuple[str, ...] = (
    REASON_SUBJECT_WITHDREW,
    REASON_DECISION_SUPERSEDED,
    REASON_RETENTION_EXPIRED,
)

# Duration units: months/years are FIXED-LENGTH approximations
# (30 / 365 days), stated explicitly rather than hidden -- "24mo" is
# 730 days, not 24 calendar months.
_UNIT_SECONDS: Dict[str, float] = {
    "s": 1.0, "m": 60.0, "h": 3600.0, "d": 86400.0,
    "mo": 86400.0 * 30, "y": 86400.0 * 365,
}
_DECLARATION_RE = re.compile(r"^([A-Za-z][A-Za-z0-9_]*)@(\d+)(s|m|h|d|mo|y)$")


class OutcomeIntegrityError(Exception):
    """An obligation failed validation. Carries every violation found."""

    def __init__(self, obligation_id: str, violations: List[str]):
        self.obligation_id = obligation_id
        self.violations = list(violations)
        lines = "\n".join(f"  - {v}" for v in self.violations)
        super().__init__(
            f"Outcome obligation '{obligation_id}' failed integrity validation "
            f"({len(self.violations)} violation(s)):\n{lines}"
        )


@dataclass(frozen=True)
class MaturationRule:
    """When an obligation of a given kind comes due.

    The declaration string ("loan_performance@24mo") is stable enough
    to hash into a decision at decision time and independently re-parse
    later.
    """

    kind: str
    horizon_seconds: float

    def declaration(self) -> str:
        for suffix in ("y", "mo", "d", "h", "m", "s"):
            size = _UNIT_SECONDS[suffix]
            if self.horizon_seconds >= size and self.horizon_seconds % size == 0:
                return f"{self.kind}@{int(self.horizon_seconds // size)}{suffix}"
        return f"{self.kind}@{int(self.horizon_seconds)}s"

    @classmethod
    def parse(cls, declaration: str) -> "MaturationRule":
        match = _DECLARATION_RE.match(str(declaration).strip())
        if not match:
            raise ValueError(
                f"unparseable maturation declaration {declaration!r}; expected "
                f"'<kind>@<count><unit>' with unit in "
                f"{sorted(_UNIT_SECONDS)} (e.g. 'loan_performance@24mo')"
            )
        kind, count, unit = match.group(1), int(match.group(2)), match.group(3)
        if count <= 0:
            raise ValueError(
                f"maturation horizon must be positive, got {count!r} in "
                f"{declaration!r} -- a zero-length obligation is not an "
                f"obligation, it is a decision that already closed"
            )
        return cls(kind=kind, horizon_seconds=count * _UNIT_SECONDS[unit])


@dataclass(frozen=True)
class OutcomeObligation:
    """One durable obligation attached to one closed Decision.

    decision_fingerprint links back to the beta.Decision this is owed
    on (the source links via a ledger row's current_hash; here that
    role is played by Decision.decision_fingerprint, which is what
    DecisionLedger chains on too).
    """

    obligation_id: str
    decision_fingerprint: str
    domain: str
    obligation_kind: str
    opened_at: float
    expected_by: float
    state: str = OUTCOME_OPEN
    reason_code: Optional[str] = REASON_NOT_YET_DUE
    resolved_at: Optional[float] = None
    resolved_value: Optional[Dict[str, Any]] = None
    resolution_provenance: Optional[str] = None
    resolution_method: Optional[str] = None
    favorable: Optional[bool] = None
    subject_id: Optional[str] = None
    detail: Dict[str, Any] = field(default_factory=dict)


def open_obligation(obligation_id: str, decision_fingerprint: str, domain: str,
                     rule: MaturationRule, opened_at: float,
                     subject_id: Optional[str] = None,
                     detail: Optional[Mapping[str, Any]] = None) -> OutcomeObligation:
    """Open an obligation from a maturation rule. The horizon is
    computed, never passed in, so expected_by cannot drift from the
    rule that justifies it."""
    return OutcomeObligation(
        obligation_id=str(obligation_id),
        decision_fingerprint=str(decision_fingerprint),
        domain=str(domain),
        obligation_kind=rule.kind,
        opened_at=float(opened_at),
        expected_by=float(opened_at) + rule.horizon_seconds,
        state=OUTCOME_OPEN,
        reason_code=REASON_NOT_YET_DUE,
        subject_id=None if subject_id is None else str(subject_id),
        detail=dict(detail or {}),
    )


def resolve(obligation: OutcomeObligation, resolved_at: float,
            resolved_value: Mapping[str, Any], provenance: str,
            favorable: Optional[bool] = None,
            method: Optional[str] = None) -> OutcomeObligation:
    """Close an obligation with a real result. Returns a NEW obligation;
    the original stays frozen (append-only)."""
    closed = OutcomeObligation(
        obligation_id=obligation.obligation_id,
        decision_fingerprint=obligation.decision_fingerprint,
        domain=obligation.domain,
        obligation_kind=obligation.obligation_kind,
        opened_at=obligation.opened_at,
        expected_by=obligation.expected_by,
        state=OUTCOME_RESOLVED,
        reason_code=None,
        resolved_at=float(resolved_at),
        resolved_value=dict(resolved_value),
        resolution_provenance=str(provenance),
        resolution_method=None if method is None else str(method),
        favorable=favorable,
        subject_id=obligation.subject_id,
        detail=dict(obligation.detail),
    )
    validate_obligation(closed)
    return closed


def stay_open(obligation: OutcomeObligation, reason_code: str) -> OutcomeObligation:
    """Restate an open obligation with a more specific reason."""
    restated = OutcomeObligation(
        obligation_id=obligation.obligation_id,
        decision_fingerprint=obligation.decision_fingerprint,
        domain=obligation.domain,
        obligation_kind=obligation.obligation_kind,
        opened_at=obligation.opened_at,
        expected_by=obligation.expected_by,
        state=OUTCOME_OPEN,
        reason_code=str(reason_code),
        favorable=None,
        subject_id=obligation.subject_id,
        detail=dict(obligation.detail),
    )
    validate_obligation(restated)
    return restated


def abandon(obligation: OutcomeObligation, reason_code: str, at: float) -> OutcomeObligation:
    """Declare an obligation will never resolve. Recorded, not deleted."""
    dropped = OutcomeObligation(
        obligation_id=obligation.obligation_id,
        decision_fingerprint=obligation.decision_fingerprint,
        domain=obligation.domain,
        obligation_kind=obligation.obligation_kind,
        opened_at=obligation.opened_at,
        expected_by=obligation.expected_by,
        state=OUTCOME_ABANDONED,
        reason_code=str(reason_code),
        resolved_at=float(at),
        favorable=None,
        subject_id=obligation.subject_id,
        detail=dict(obligation.detail),
    )
    validate_obligation(dropped)
    return dropped


def validate_obligation(obligation: OutcomeObligation) -> None:
    """Fail-loud validation. Raises OutcomeIntegrityError with the
    complete violation list, or returns."""
    violations: List[str] = []

    for label, value in (("obligation_id", obligation.obligation_id),
                          ("decision_fingerprint", obligation.decision_fingerprint),
                          ("domain", obligation.domain),
                          ("obligation_kind", obligation.obligation_kind)):
        if not str(value).strip():
            violations.append(f"{label} must be a non-empty string")

    if obligation.state not in OUTCOME_STATES:
        violations.append(f"state must be one of {list(OUTCOME_STATES)}, got {obligation.state!r}")

    for label, value in (("opened_at", obligation.opened_at), ("expected_by", obligation.expected_by)):
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            violations.append(f"{label} must be a number, got {type(value).__name__}")
    if (isinstance(obligation.opened_at, (int, float))
            and isinstance(obligation.expected_by, (int, float))
            and obligation.expected_by <= obligation.opened_at):
        violations.append(
            f"expected_by ({obligation.expected_by!r}) must be after opened_at "
            f"({obligation.opened_at!r}) -- an obligation that matures before it "
            f"opens has no horizon to be honored or missed"
        )

    if obligation.state == OUTCOME_OPEN:
        if obligation.reason_code not in OPEN_REASONS:
            violations.append(
                f"an OPEN obligation needs a typed reason from {list(OPEN_REASONS)}, "
                f"got {obligation.reason_code!r}"
            )
        if obligation.resolved_at is not None or obligation.resolved_value is not None:
            violations.append("an OPEN obligation carries no resolution; it is open")
        if obligation.favorable is not None:
            violations.append(
                f"an OPEN obligation cannot already be favorable={obligation.favorable!r} "
                f"-- a verdict without a resolution behind it is a guess dressed as a measurement"
            )
        if obligation.resolution_provenance is not None:
            violations.append(
                f"an OPEN obligation cannot carry a resolution_provenance="
                f"{obligation.resolution_provenance!r} -- nothing has been resolved yet"
            )
        if obligation.resolution_method is not None:
            violations.append(
                f"an OPEN obligation cannot carry a resolution_method="
                f"{obligation.resolution_method!r} -- nothing has been resolved yet"
            )

    elif obligation.state == OUTCOME_RESOLVED:
        if obligation.reason_code is not None:
            violations.append(
                f"a RESOLVED obligation carries no open-reason, got {obligation.reason_code!r}"
            )
        if obligation.resolved_at is None:
            violations.append("a RESOLVED obligation must record resolved_at")
        elif (isinstance(obligation.resolved_at, (int, float))
                and isinstance(obligation.opened_at, (int, float))
                and obligation.resolved_at < obligation.opened_at):
            violations.append(
                f"resolved_at ({obligation.resolved_at!r}) cannot be before "
                f"opened_at ({obligation.opened_at!r}) -- a resolution cannot "
                f"predate the obligation it resolves"
            )
        if not isinstance(obligation.resolved_value, dict) or not obligation.resolved_value:
            violations.append(
                "a RESOLVED obligation must record a non-empty resolved_value -- "
                "closing with nothing recorded is closing with nothing established"
            )
        if obligation.resolution_provenance not in PROVENANCE_STAMPS:
            violations.append(
                f"a RESOLVED obligation must stamp its resolution with one of "
                f"{list(PROVENANCE_STAMPS)}, got {obligation.resolution_provenance!r}"
            )
        if (obligation.resolution_provenance == PROVENANCE_ESTIMATED
                and not str(obligation.resolution_method or "").strip()):
            violations.append(
                "an ESTIMATED resolution must name its method -- an estimated "
                "outcome that will not say how it was estimated is a proxy metric "
                "dressed as a measurement"
            )

    elif obligation.state == OUTCOME_ABANDONED:
        if obligation.reason_code not in ABANDONED_REASONS:
            violations.append(
                f"an ABANDONED obligation needs a typed reason from {list(ABANDONED_REASONS)}, "
                f"got {obligation.reason_code!r}"
            )
        if obligation.favorable is not None:
            violations.append("an ABANDONED obligation has no favorability; nothing resolved")
        if obligation.resolved_at is None:
            violations.append(
                "an ABANDONED obligation must record when it was abandoned (resolved_at) "
                "-- a decision to stop watching still happened at a specific time"
            )
        if obligation.resolved_value is not None:
            violations.append(
                "an ABANDONED obligation carries no resolved_value -- abandonment is "
                "not a resolution, it's a declaration that no resolution is coming"
            )
        if obligation.resolution_provenance is not None:
            violations.append(
                f"an ABANDONED obligation cannot carry a resolution_provenance="
                f"{obligation.resolution_provenance!r} -- nothing was resolved"
            )
        if obligation.resolution_method is not None:
            violations.append(
                f"an ABANDONED obligation cannot carry a resolution_method="
                f"{obligation.resolution_method!r} -- nothing was resolved"
            )

    if violations:
        raise OutcomeIntegrityError(obligation.obligation_id, violations)


def is_overdue(obligation: OutcomeObligation, now: float) -> bool:
    """Whether an open obligation has blown past its declared horizon.
    Computed, never stored."""
    return obligation.state == OUTCOME_OPEN and float(now) > obligation.expected_by


def horizon_honored(obligations: List[OutcomeObligation], now: float) -> Dict[str, int]:
    """Portfolio answer to 'is this system honoring its own horizons'."""
    counts: Dict[str, int] = {state: 0 for state in OUTCOME_STATES}
    counts["overdue"] = 0
    reasons: Dict[str, int] = {}
    for obligation in obligations:
        counts[obligation.state] = counts.get(obligation.state, 0) + 1
        if is_overdue(obligation, now):
            counts["overdue"] += 1
        if obligation.state == OUTCOME_OPEN and obligation.reason_code:
            reasons[obligation.reason_code] = reasons.get(obligation.reason_code, 0) + 1
    for reason, count in reasons.items():
        counts[f"open:{reason}"] = count
    return counts
