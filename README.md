# interconnected_delta

**Role in the governed action stack:** CUSTODY — record Decisions, track outcome obligations, verify later, fairness-screen cohorts.

```text
α Alpha (Keys) → ζ Zeta (Locks) → β Beta (Decision) → δ Delta (custody)
```

Part of the composable decision spine. Live orchestrated path: [observe-perceive](https://github.com/wking53214/observe-perceive). Domain ledger/twin runtime: [sentinel_os](https://github.com/wking53214/sentinel_os).

This is where the real-time decision half (α/ζ/β) meets mature obligation and fairness machinery already present in `sentinel_os`.

---

Records `beta.Decision`s in a hash-chained ledger, tracks their outcome
obligations, verifies them when they mature, and feeds resolved outcomes
into cohort-level fairness screening — the fourth and final stage:
`alpha` detects Keys, `zeta` evaluates Locks, `beta` decides, `delta`
records and verifies.

## Why this exists, and why it's different from alpha/zeta/beta

Alpha and zeta extracted patterns that were **duplicated and never
unified** across the source (Locks reinvented 5 times, a verdict shape
reinvented 3 times). Delta is a different kind of extraction: the
target systems here are **already mature, already unified, and already
correct** — they just never had `beta.Decision` to connect to. Delta's
job is mostly wiring, not invention.

| Module | Provenance |
|---|---|
| `ledger.py` | **Extracted.** Generalizes PERCEIVE's `ImmutableAuditLedger` (`perceive_consolidated.py:579-633`) — a real, working genesis + SHA256 chain + recompute-to-verify mechanism — from PERCEIVE's specific entry shape to any `beta.Decision`. |
| `obligation.py` | **Extracted near-verbatim.** `MaturationRule`, `OutcomeObligation`, and the OPEN → RESOLVED/ABANDONED lifecycle come from `sentinel_os`'s `outcome_v1.py`, already a mature, rigorously validated, domain-blind module (see its own "Provenance Rule": every claim stamped verified/attested/estimated, never a mushy "probably"). Not reinvented — relinked to `Decision.decision_fingerprint` instead of a raw ledger row's hash. |
| `fairness.py` | **Extracted.** `CohortDecision` and `check_statistical_outcome_equity` come from `sentinel_os`'s `regulatory_checks.py` — a real, working EEOC four-fifths disparate-impact screen (29 CFR 1607.4(D)), already wired to consume resolved obligations. It had just never been fed a real decision before. |
| `decision_obligations.py` | **New.** The one piece that didn't already exist: a thin adapter (`DecisionObligationTracker`) linking `beta.Decision` to the obligation lifecycle. `outcome_v1.py` worked on generic decision-row dicts, not `beta.Decision` objects — there was no existing adapter to extract. |

## What this reveals about the rest of the codebase

Before this session, `sentinel_os` had a mature judgment-time system
(`Cassette.judge()` — was this outcome good, scored after the fact) and
a mature outcome-obligation system (`outcome_v1.py` — track what's owed,
verify it later, feed fairness testing) — but **no real-time
decision-time system** connecting to either of them. `alpha/zeta/beta`
built that missing half. `delta` is where the two halves actually meet.

## What's NOT solved here

`group_distribution` (race/ethnicity → probability) is required by
`to_cohort_decision`/`check_statistical_outcome_equity` and is
**deliberately not computed by this module** — same domain-blind
posture as the source, which never guesses who's in which demographic
group any more than it guesses what counts as "favorable." The
source's intended input for this is a BISG-based sealed estimation
channel; an earlier audit in this project found that estimator is
still abstract/unimplemented, gated behind a research-mode flag. That
gap is real, pre-existing, and outside delta's scope to close — delta
just refuses to fabricate a stand-in for it.

`RegulatoryFinding` here also drops the `regulation`/
`RegulationCheckProfile` fields the source's version carries (tying a
finding to a specific regulation's check profile). That type wasn't
read closely enough during extraction to reproduce faithfully, so it's
omitted rather than guessed at.

## API

```python
from datetime import datetime
from delta import DecisionLedger, DecisionObligationTracker, MaturationRule, PROVENANCE_VERIFIED

ledger = DecisionLedger()
obligations = DecisionObligationTracker()
obligations.register_maturation("mortgage_approve", MaturationRule(kind="loan_performance", horizon_seconds=730 * 86400))

# decision is a beta.Decision, from beta.DecisionEngine.decide(...)
entry = ledger.append(decision)
assert ledger.verify_chain_integrity()

obligation = obligations.open_for_decision(decision, domain="mortgage", opened_at=decision.timestamp.timestamp())
# ... 24 months later, once the loan's performance is known ...
resolved = obligations.resolve_obligation(
    obligation.obligation_id, resolved_at=..., resolved_value={"paid_on_time": True},
    provenance=PROVENANCE_VERIFIED, favorable=True,
)

from delta import to_cohort_decision, check_statistical_outcome_equity
cohort = [to_cohort_decision(o, group_distribution={"group_a": 1.0}) for o in obligations.all() if o.state == "RESOLVED"]
findings = check_statistical_outcome_equity(cohort)  # [] if clean, else four-fifths FLAG findings
```

See `examples/full_pipeline.py` for two worked scenarios: the pediatric
alpha→zeta→beta→delta pipeline (where decisions resolve immediately, so
no obligation opens — that's correct, not a gap), and a synthetic
mortgage cohort where an obligation matures 24 months later and the
fairness screen correctly flags a disparate-impact pattern.

## Where this fits

```
interconnected_alpha  -- raw vitals -> named Keys
interconnected_zeta   -- Keys -> Locks -> open/closed decisions
interconnected_beta   -- Lock states -> a Decision + narrative
interconnected_delta  -- (this repo) records, tracks obligations, verifies, fairness-screens
```

## Tests

```
pip install -e ".[dev]"
pytest
```

69 tests. Depends on `zeta` and `beta`.
