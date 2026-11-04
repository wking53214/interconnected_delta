# interconnected_delta (δ)

Records `beta.Decision`s in a hash-chained ledger, tracks outcome obligations, screens cohorts for disparate impact. Version `0.1.0`. Depends on ζ and β (git). Python ≥ 3.10.

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
- `append` records exactly a `beta.Decision` (anything else, including a subclass or an object that merely has a `validate`, is a `TypeError`) and calls `Decision.validate()` on it first, with or without a context: a Decision whose confidence basis contradicts its keys, whose fields have the wrong types, or whose confidence is not a finite number in [0, 1], is refused and the ledger is unchanged. `append` holds a lock, so concurrent appends keep one chain; the public `entries` list itself is not protected.
- **Pipeline run.** `append(decision, context=...)` takes a context (`schema`, `entity_id`, `event_id`, `run_id`, see `delta/context.py`). With it the Decision must be about the context's entity, its `decision_fingerprint` must match the Decision's own fields, and a Decision with the same fingerprint already recorded in the same run, or for the same `event_id` in any run, raises `DuplicateRecordError` (an event is recorded once, whatever run it is brought back in; a different Decision for the same event is recorded). The entry carries `run_id` and `event_id`, hashed into the chain only when set, so an entry recorded without a context hashes exactly as before. Without a context a repeat is accepted, as it always was.
- **Checkpoint.** `checkpoint()` returns a frozen `LedgerCheckpoint(length, head)`; `verify_against(checkpoint)` is true only if the whole chain verifies and the entry at that length still hashes to that head. The ledger may grow past a checkpoint; truncation, reordering or a rewrite up to it fails. `verify_chain_integrity()` alone cannot tell: whoever can edit the entries can reset `chain_head`, or recompute the whole chain, and it verifies. That gap is measured, and `tests/test_ledger_checkpoint.py` records it. A checkpoint is only as trustworthy as the place it is kept, which delta does not provide.

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
| In-memory ledger | Process death loses the chain. No file backend (observe-perceive `execution_guard` has JSONL; this does not). No atomic append, no partial-write recovery, no external anchoring. Tamper-evidence holds only relative to a checkpoint kept outside the ledger. This is a reference ledger; production custody is sentinel_os. |
| Dual custody | Live orchestrator writes Conservation Kernel ledger + `ExecutionLedger`, not `delta.DecisionLedger`. sentinel_os writes Postgres. Three custody stories. |
| Git dependencies | `zeta` and `beta` are pinned by commit in `pyproject.toml`; `tests/test_sibling_pins.py` holds the pins, so moving one is a reviewed change. They are not on a package index. |
| Context is optional | Without a `context`, a repeat is accepted and no run is recorded. A caller that needs run boundaries must pass one; CNS's strict trace refuses an entry that has none. |
| Fairness inputs | `group_distribution` must be supplied by the caller. δ does not estimate race/ethnicity (that's sentinel_os `bisg_estimator`, and even that is a proxy). |
| `MIN_COHORT_SIZE_FOR_STATISTICAL_TEST` / `FOUR_FIFTHS_THRESHOLD` | Named constants. Four-fifths is the regulatory 0.8; cohort minimum is a tunable. |
| No independent twin | sentinel_os twin is not ported. |
| Actor self-report | Not used as ground truth here, but δ also does not independently observe outcomes — `resolve()` trusts caller-supplied `resolved_value` + `provenance` string. |

## 5. Core Invariants & Guarantees

- Recomputation over trust for the hash chain.
- Nothing is recorded that fails `Decision.validate()`; a failed append changes nothing.
- A Decision is recorded once per pipeline run and once per event (when a context is given).
- Fail-closed on unknown obligation ids.
- Duplicate obligation open is an error, not a silent overwrite.
- Indeterminate fairness beats a spurious pass on small/uninformative cohorts.
- Actor-supplied outcomes are recorded with provenance stamps; they are not promoted to "verified" without the caller saying so.

## 6. Inputs, Outputs & Type Contracts

```python
from delta import DecisionLedger, DecisionObligationTracker, check_statistical_outcome_equity
from delta.obligation import MaturationRule, OutcomeObligation
from delta.fairness import CohortDecision, RegulatoryFinding
# LedgerEntry: audit_id, timestamp, decision, previous_hash, immutable_hash, run_id, event_id (the last two None without a context)
# OutcomeObligation: obligation_id, decision_fingerprint, domain, kind, opened_at, subject_id, status, ...
```

`DecisionLedger.append(decision, timestamp=None, *, context=None) -> LedgerEntry`  
`DecisionLedger.verify_chain_integrity() -> bool` (recomputes the chain)  
`DecisionLedger.checkpoint() -> LedgerCheckpoint` and `DecisionLedger.verify_against(checkpoint) -> bool`

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

## Optional CNS connection

delta does not need CNS. It has no runtime dependency on it, nothing in the package imports it when it loads, and the ledger, obligations and fairness screen never touch it (`tests/test_cns_independence.py` checks this with CNS blocked). `delta/cns_connector.py` is the one place that knows CNS exists. Install it with `pip install 'delta[cns] @ git+https://github.com/wking53214/interconnected_delta'`; without CNS its functions raise `CnsNotInstalled` and nothing else changes.

It makes delta's two primary records **bindable**: a `LedgerEntry` (the recorded `beta.Decision`, the time it was recorded and its place in the chain: `audit_id`, `previous_hash`, `immutable_hash`) and an `OutcomeObligation` (every field, in whatever state it is in). `entry_content` / `obligation_content` return the deterministic content, `entry_digest` / `obligation_digest` are `cns.gate.subject_digest` over it, and `binds(result, record)` says whether a `cns.gate.GateResult` was issued against exactly that record. A verdict about one record does not bind to another, so the same decision recorded twice binds to its own place in the chain. Set-like tuples on the decision are sorted so order cannot change a digest; the decision's `audit_hash` is not bound.

It is **binding only**, and deliberately not a gate. There is no mapping from an entry or an obligation to `PASS`, `RETRY` or `TERMINAL_BREACH`: a ledger entry is a custody record and an obligation is a lifecycle state, and turning either into a gate outcome would invent a meaning delta does not have. Fairness findings are not bound (a four-fifths flag is a screening signal, derived from obligations that can be). It also does not verify the chain; `verify_chain_integrity` does that.

**Binding an entry to a run.** `entry_content`, `entry_digest` and `binds` take an optional `context` and `decision_digest` (beta's digest of the Decision, bound to the same run). They come together or not at all. The entry must have been recorded under that run and event and be about that entity, or the connector raises; the content then gains `context` and `decision_digest`. Without them every existing digest is unchanged. An entry's `run_id` and `event_id` appear in its content whenever it has them.

The CNS digest is separate from the ledger's `immutable_hash` chain and from the obligation's `decision_fingerprint` link, both unchanged and unaware of CNS. It binds a verdict to the record exactly as stored, chain hashes included.

Apache-2.0.
