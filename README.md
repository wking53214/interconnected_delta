# interconnected_delta (δ)

Records `beta.Decision`s in a hash-chained ledger, tracks outcome obligations, screens cohorts for disparate impact. Version `0.1.0`. Depends on ζ and β (git). Python ≥ 3.9.

## 1. Pipeline Position & Role

**CUSTODY** of the extracted spine. Fourth stage.

```text
α Alpha (Keys) → ζ Zeta (Locks) → β Beta (Decision) → δ Delta (this repo)
```

This is where the real-time decision half (α/ζ/β) meets mature obligation and fairness machinery already present in [`sentinel_os`](https://github.com/wking53214/sentinel_os). Domain-blind production custody remains sentinel_os (Postgres, twin, sealed demographic channel). δ is the in-process extract that can consume a `beta.Decision`.

## 2. Full System Scope & Architectural Depth

Unlike α/ζ/β (unifying duplicated patterns), δ is mostly **wiring of already-correct machinery** that never had `beta.Decision` as an input.

| Module | Provenance |
|---|---|
| `ledger.py` | Extracted from PERCEIVE `ImmutableAuditLedger` (`perceive_consolidated.py:579-633`): genesis + SHA-256 chain + recompute-to-verify. Generalized from PERCEIVE's entry shape to any `beta.Decision`. |
| `obligation.py` | Near-verbatim from sentinel_os `outcome_v1.py`: `MaturationRule`, `OutcomeObligation`, OPEN → RESOLVED / ABANDONED / stay-open. Bound to `Decision.decision_fingerprint` instead of a raw ledger row hash. Provenance rule: claims are verified / attested / estimated — never "probably". |
| `fairness.py` | Extracted from sentinel_os `regulatory_checks.py`: EEOC four-fifths screen (29 CFR 1607.4(D)). `CohortDecision` + `check_statistical_outcome_equity`. |
| `decision_obligations.py` | **New.** `DecisionObligationTracker` adapter. `outcome_v1` operated on generic row dicts. |

### Ledger

- Genesis seed → `chain_head`.
- Canonical subset of a Decision (ids, timestamp, fingerprint, narrative, trigger names) is hashed; `immutable_hash = SHA-256(chain_head + hash(canonical))`.
- Verification **recomputes** the chain. Stored hashes are not trusted.
- In-memory `List[LedgerEntry]`.

### Obligations

Not every decision owes one. A `decision_id` with no registered `MaturationRule` opens nothing (IVR quality settled at hangup is the motivating case). `open_for_decision` is thread-safe (check-then-insert under a lock). Duplicate open of the same `decision_fingerprint:kind` raises.

### Fairness

Four-fifths screening on **weighted** group distributions. Cohort size uses **informative** rows (those with non-zero group weight), not raw `len(cohort)` — a cohort of 30 of which 2 carry weight must not pass the size gate as if it had 30 observations. Fewer than two groups with sufficient weight → indeterminate, not a spurious pass. A four-fifths flag is a **screening signal, not a legal determination**.

## 3. What It Does NOT Do / Non-Goals

- Does **not** replace sentinel_os (no Postgres, no twin replica, no sealed demographic channel, no BISG, no cassette loader).
- Does **not** execute or authorize.
- Does **not** crypto-shred, KMS-sign, or GDPR-erase. Hash chain is SHA-256 over plaintext canonical fields.
- Does **not** hold lock state (ζ) or detect Keys (α).
- Does **not** perform post-ACD agent routing.
- Does **not** auto-open obligations for unknown `decision_id`s.

## 4. Brutally Honest Current Status & Gaps

| Gap | Detail |
|---|---|
| In-memory ledger | Process death loses the chain. No file backend (observe-perceive `execution_guard` has JSONL; this does not). |
| Dual custody | Live orchestrator writes Conservation Kernel ledger + `ExecutionLedger`, not `delta.DecisionLedger`. sentinel_os writes Postgres. Three custody stories. |
| Unpinned git deps | `zeta @ git+.../interconnected_zeta`, `beta @ git+.../interconnected_beta`. |
| Fairness inputs | `group_distribution` must be supplied by the caller. δ does not estimate race/ethnicity (that's sentinel_os `bisg_estimator`, and even that is a proxy). |
| `MIN_COHORT_SIZE_FOR_STATISTICAL_TEST` / `FOUR_FIFTHS_THRESHOLD` | Named constants. Four-fifths is the regulatory 0.8; cohort minimum is a tunable. |
| No independent twin | sentinel_os twin is not ported. |
| Actor self-report | Not used as ground truth here, but δ also does not independently observe outcomes — `resolve()` trusts caller-supplied `resolved_value` + `provenance` string. |

## 5. Core Invariants & Guarantees

- Recomputation over trust for the hash chain.
- Fail-closed on unknown obligation ids.
- Duplicate obligation open is an error, not a silent overwrite.
- Indeterminate fairness beats a spurious pass on small/uninformative cohorts.
- Actor-supplied outcomes are recorded with provenance stamps; they are not promoted to "verified" without the caller saying so.

## 6. Inputs, Outputs & Type Contracts

```python
from delta import DecisionLedger, DecisionObligationTracker, check_statistical_outcome_equity
from delta.obligation import MaturationRule, OutcomeObligation
from delta.fairness import CohortDecision, RegulatoryFinding
# LedgerEntry: audit_id, timestamp, decision, immutable_hash, previous_hash
# OutcomeObligation: obligation_id, decision_fingerprint, domain, kind, opened_at, subject_id, status, ...
```

`DecisionLedger.append(decision, timestamp=None) -> LedgerEntry`  
`DecisionLedger.verify() -> bool` (recomputes)

## 7. Stack Integration Topology

```text
β.Decision ──┬── δ.DecisionLedger.append          (hash chain)
             └── δ.DecisionObligationTracker      (if MaturationRule registered)
                        │  resolve / abandon / stay_open
                        ▼
               CohortDecision[] → four-fifths screen
```

Production-shaped sibling (not imported): [`sentinel_os`](https://github.com/wking53214/sentinel_os) `outcome_v1.py`, `regulatory_checks.py`, `governance/ledger_postgres.py`.  
Example: `examples/full_pipeline.py`.

Proprietary. Copyright (c) 2026 William King. All rights reserved. See LICENSE.
