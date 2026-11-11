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
- In-memory `List[LedgerEntry]`. See "Integrity, availability, durability" below for what that does and does not give.
- `append` records exactly a `beta.Decision` (anything else, including a subclass or an object that merely has a `validate`, is a `TypeError`) and calls `Decision.validate()` on it first, with or without a context: a Decision whose confidence basis contradicts its keys, whose fields have the wrong types, or whose confidence is not a finite number in [0, 1], is refused and the ledger is unchanged. `append` holds a lock, so concurrent appends keep one chain; the public `entries` list itself is not protected.
- **Pipeline run and the event policy.** `append(decision, context=...)` takes a context (`schema`, `entity_id`, `event_id`, `run_id`, see `delta/context.py`). With it the Decision must be about the context's entity and its `decision_fingerprint` must match its fields. **A run belongs to exactly one event, and an event has exactly one Decision**, whatever it says and whatever run it is brought back in: a second Decision for a recorded event raises `DuplicateRecordError`, and a run that names a different event than it already recorded raises `ValueError`. To decide again, use a new event. A Decision under a context is also never older than the newest one recorded for its entity (`StaleDecisionError`; equal instants are fine; mixing timezone-aware and naive timestamps is refused). The entry carries `run_id` and `event_id`, hashed into the chain only when set, so an entry recorded without a context hashes exactly as before. Without a context a repeat or an older Decision is accepted, as it always was, and CNS's strict check refuses such an entry because it has no run.
- **Requiring a context.** `DecisionLedger(require_context=True)` refuses any append without one (`ContextRequiredError`), for callers that want the bound path enforced. The default keeps recording unbound Decisions as it always did; an unbound entry is not equivalent to a verified one.
- **No downgrade.** A ledger built with `require_context=True` also refuses a Decision whose `content_fingerprint` is empty (`ValueError` naming `content_fingerprint` and "downgrade"), because blanking the field removes the integrity claim instead of changing it. A ledger without `require_context` stays lenient: a legacy Decision with no `content_fingerprint` is still recorded.
- **KeySet lineage.** A Decision that records `keyset_fingerprint` (beta sets it when its lock results came from the KeySet it was given) has it hashed into the chain, so it cannot be changed afterwards; a Decision without one hashes as before.
- **A corrupt head stops recording.** Every append first checks, in O(1), that `chain_head` names the last entry (`LedgerCorruptError`). That catches a head and an entry list left disagreeing; it does not catch a chain someone rewrote and re-headed, which is what a checkpoint held outside the ledger is for.
- **Checkpoint.** `checkpoint()` returns a frozen `LedgerCheckpoint(length, head)`; `verify_against(checkpoint)` is true only if the whole chain verifies and the entry at that length still hashes to that head. The ledger may grow past a checkpoint; truncation, reordering or a rewrite up to it fails. `verify_chain_integrity()` alone cannot tell: whoever can edit the entries can reset `chain_head`, or recompute the whole chain, and it verifies. That gap is measured, and `tests/test_ledger_checkpoint.py` records it. A checkpoint is only as trustworthy as the place it is kept, which delta does not provide. `verify_against` refuses (returns `False`) a checkpoint whose `length` is not an `int` (a `bool` is not), is below 1 or beyond the ledger, or whose `head` is not exactly a `str` of 64 lowercase hex digits; **a checkpoint of length 0 always returns `False`**, because it names no entry, is identical for every ledger and so is no evidence about any of them. A caller's `(0, genesis)` or an object whose `__eq__` always answers yes no longer passes.

### New refusals and guarantees (hardening round 4)

Each item below has a test in `tests/test_ledger_r4.py` (or the file named).

- **Checkpoint typing.** See the Checkpoint paragraph above: empty, mistyped and look-alike checkpoints are `False`.
- **Backdated entry.** `append(decision, timestamp=t)` raises `ValueError("backdated ...")` if the supplied `t` is earlier than the previous entry's timestamp; equal is allowed. Only a caller-supplied `timestamp` is judged. An entry that takes its Decision's own time is as it always was (the per-entity stale rule governs Decision times, and only under a context).
- **Replay.** Under a context, after the same-event and same-run refusals (which fire first and keep their messages) have passed, `append` raises `DuplicateRecordError` with "replay" in the message, and leaves the ledger unchanged, when an entry already in the ledger has the same `(entity_id, decision_fingerprint, timestamp)`. Nothing else is compared. In particular `content_fingerprint` is **not** part of the identity: a Decision edited in any other field (`reasoning`, `instructions`, `confidence` and so on) with its `content_fingerprint` recomputed to match, or blanked, and brought back under a new event and a new run is still refused as a replay, because `decision_fingerprint` covers only the entity, `decision_id`, `decision` and the trigger names and so does not change. A Decision is a different one only if its entity, its `decision_fingerprint` or its timestamp differs. Without a context nothing is checked: a repeat is accepted. (`tests/test_ledger_r4.py::test_content_fingerprint_is_part_of_the_replay_identity_only_when_set` and `tests/test_ledger_r5.py` pin this.)
- **Context snapshot.** `check_context` reads the mapping once (`dict(context)`), validates that snapshot, and returns it. Every value must be exactly `str`, not a subclass. The ledger records values from the snapshot only, so a mapping that answers differently on later reads cannot change what is recorded.
- **Stored copy.** `append` stores a copy of the Decision whose four tuple fields are tuples, so a list the caller still holds cannot reach the stored Decision (and the chain keeps verifying).
- **`content_fingerprint`.** beta's whole-Decision fingerprint is hashed into the chain and bound by the CNS connector only when the Decision has one, so every existing hash and digest is unchanged. A Decision recorded under a context is refused if its `content_fingerprint` does not match its fields.

### Round 8 refusals and guarantees

Each item has a test in `tests/test_ledger_r8.py` or `tests/test_ledger_export.py`.

- **Exact types.** `append` requires `type(x) is str` for `entity_id`, `decision_id`, `decision`, `decision_fingerprint`, `reasoning`, `instructions`, `content_fingerprint`, `keyset_fingerprint`, `confidence_basis` and `audit_hash` and for every element of the four tuple fields (each of those four given as a list or tuple; a bare `str` is a `TypeError`, not stored as its characters); `confidence` exactly `float` or `int` (not a `bool`, not a subclass); the times exactly `datetime`. A `TypeError` names the field and the ledger is unchanged. A subclass can print one text and hash another, so the chain hash and a digest built from the same Decision could otherwise bind different text. Verification applies the same check to every stored entry, so a subclass planted after recording fails `verify_chain_integrity`.
- **One snapshot.** `snapshot()` returns `(entries_tuple, chain_head, length)` taken together under the ledger lock (from inside a sink, which already holds the lock, it reads without taking it again). `verify_chain_integrity`, `verify_against` and `verification_receipt` each read it once and use only those values; `checkpoint()` and `len()` are taken from it too, so they wait for a commit in progress and heal a cut-short one first. The receipt also recomputes the entry's own hash from the snapshotted entry and requires it to equal the `entry_hash` it states, so an entries list that answers differently on a later read, or an append that lands while the receipt is built, cannot put a value on it that was not verified.
- **Re-judging stored Decisions.** On a ledger built with `require_context=True`, `verify_chain_integrity()` also runs `Decision.validate(check_fingerprint=True)` on every stored Decision, so a field no hash covers (`confidence_basis` edited after recording) or a fingerprint that no longer fits its Decision is found. A ledger without `require_context` keeps its old verification. The flag is a public attribute: whoever can edit the ledger can clear it, which is why an exported ledger carries it and `verify_export` can pin it (below).
- **`audit_hash`.** delta never populates or hashes a Decision's `audit_hash`, and the CNS connector does not bind it. It must be exactly a `str` to be appended (as `export` requires), is exported as stored and an edit to it is **not** detected. Treat it as a pointer field beta leaves for a recorder, not as evidence.

### Export and verification in another process

`DecisionLedger.export()` returns a plain JSON-able dict `{"schema": "ledger-export/1", "require_context": bool, "chain_head": str, "entries": [...]}`; each entry has `audit_id`, `timestamp` (isoformat text), `previous_hash`, `immutable_hash`, `run_id`, `event_id` and the whole Decision (`timestamp` as isoformat, tuples as lists, every Decision field). It is built from one snapshot and refused (`ValueError`) when that snapshot does not verify. `LedgerCheckpoint.to_dict()` / `from_dict(d)` do the same for a checkpoint (exact keys `length` and `head`, exact types; a length of 0 is well formed and proves nothing).

A consumer needs only `delta.verify_export(data, checkpoint=None, *, require_context=None)` (or `DecisionLedger.from_export(data)`, which it calls). It checks the schema and every key set and type exactly, recomputes every hash from the content (it trusts no stored hash), requires each entry's `previous_hash` to continue the chain from genesis and the head to be the last hash, runs `Decision.validate(check_fingerprint=True)` on every Decision when the export says `require_context`, and raises `ValueError` naming the first failing entry and field. It does not replay the append policies (event, run, replay, staleness): it checks the chain and each Decision, not the history of how it was written. Given a `checkpoint` (a `LedgerCheckpoint` or its dict form) it also requires the export to contain the history it names (`verify_against`), so a truncated or rewritten export fails. **Without a checkpoint this is integrity only, no rollback detection:** a truncated export, or one rewritten and re-chained from genesis, still verifies. The checkpoint must come from storage the producer cannot rewrite. `require_context=True` on the verifier refuses an export whose flag was cleared.

**What the chain hash covers.** `immutable_hash = SHA-256(previous_hash + SHA-256(canonical JSON of the following))`:

- always: `audit_id`, the entry `timestamp` (isoformat), and the Decision's `entity_id`, `decision_id`, `decision`, `timestamp` (isoformat), `confidence`, `triggered_by_locks`, `triggered_by_keys`, `newly_triggered_locks`, `decision_fingerprint`, `reasoning`, `reversal_conditions`, `instructions`;
- only when set: the Decision's `keyset_fingerprint` and `content_fingerprint`, and the entry's `run_id` and `event_id`;
- not covered: the Decision's `audit_hash` and `confidence_basis`, and the entry's own `immutable_hash` (it is the result). `confidence_basis` is derived from `triggered_by_keys`, which is covered, and is checked by `Decision.validate`; `audit_hash` is not checked by anything.

### Atomic append and the sink hook

`append` builds the entry and runs every check first. Only then does it update the head and the entry list, together, under the lock; if either update is interrupted (tested: a failing `entries.append`, a failing head update, a failing hash step) the other is put back, and head, entries, length and the event policy are exactly as before, and the next append works. If the rollback is itself interrupted (a second `KeyboardInterrupt` inside it), the commit is marked pending before the first write and undone, or acknowledged if it had completed, before the next append, snapshot or verification, so no later call to `append`, `snapshot`, `checkpoint`, `len`, `export` or a verification method ever sees an entry the head does not name: each takes the lock and heals first (tested with an interrupt at every executable line of `append` and its helpers, and with a checkpoint and a `len` taken after a double fault and during a commit). Reading the public attributes `entries` and `chain_head` directly takes no lock and does not heal, so between a cut-short commit and the next locked call they can disagree. The "inside a sink" guard is read from the call stack, not stored in a flag, so an interrupt cannot leave it set. An interrupt after the commit but before `append` returns leaves the entry recorded although the caller saw the exception; retrying the same Decision then raises `DuplicateRecordError` under a context. This is atomicity against exceptions inside the process. It is not crash safety: a killed process loses the whole in-memory ledger.

`DecisionLedger(sink=callable)` calls `sink(entry)` with the exact entry that will be stored, after it is built and before any state changes. If the sink raises, nothing was recorded and the exception propagates; the sink is not called for a refused append. Its purpose is to let a caller write each entry somewhere durable before the ledger accepts it. delta supplies the hook, not the storage. If the sink succeeds and a later step then fails, the sink has seen an entry the ledger did not record; the caller must reconcile (tested).

**Sink contract.** The sink runs after the entry is built and every check has passed, and before the entry is committed. So a sink may be handed an entry that is never committed: if it raises, or the commit is interrupted, the write it made stays and the ledger has no such entry. A sink must therefore be **idempotent**, keyed by the entry's `audit_id` or `immutable_hash` (a retry after a failure hands it the same entry, so it can deduplicate); it must not call or modify the ledger (an `append` from inside a sink raises `RuntimeError`; `snapshot` and verification are allowed); and it is not a transaction. An adapter that writes to a durable store owns: the storage and its durability, deduplication by `audit_id` or `immutable_hash`, reconciling store entries the ledger never committed (an entry in the store with no matching entry in the ledger after a failed append), and holding checkpoints somewhere the writer cannot rewrite. delta supplies the hook and the ordering guarantee, nothing more.

### Integrity, availability, durability

These are three different properties and delta gives only the first, and only conditionally.

- **Integrity** (tamper evidence): the chain detects edits, truncation and reordering **given a checkpoint that was taken earlier and is held outside the process and outside the reach of whoever can edit the ledger**. Without one, whoever can edit the entries can reset the head or recompute the chain and it verifies.
- **Availability** (the ledger can still be written after a fault): an exception, including a `KeyboardInterrupt` at any line, inside `append` or inside the sink leaves the ledger writable and consistent (head, entries and the sink guard agree; a commit cut short is undone before the next call). A head that disagrees with its last entry still stops all recording (`LedgerCorruptError`); there is no repair path.
- **Durability** (survives a restart): the in-memory `DecisionLedger` provides none. `FileLedgerStore` (see Durable store) adds durability against the death of the process, tested with a real `SIGKILL`; it is not proven against an OS crash or power loss. A restart with no store loses the ledger, and a freshly created ledger verifies cleanly, accepts the same event again, and fails `verify_against` only for a caller that kept a non-empty checkpoint taken before the restart (tested).

### Signing

`delta.signing` is a hook, not a signer. `receipt_signing_input(receipt)` returns the bytes to sign: the domain tag `delta-ledger-receipt-signature/1\n` followed by the receipt as canonical ASCII JSON (sorted keys, no spaces, no NaN). It accepts only an exact `dict` with exactly the ten receipt keys, each of its exact type, and refuses anything else naming the field, so a subclass or a stray key cannot change what is signed. `sign_receipt(receipt, sign)` calls the `sign` callable the deployer hands in and refuses a result that is not exactly non-empty `bytes`. `verify_receipt_signature(receipt, signature, verify)` is `True` only when the deployer's `verify(signing_input, signature)` answers exactly `True`; an exception, any other answer, an empty or non-`bytes` signature, or a receipt that cannot be signed is `False`. delta imports no cryptography and holds no key. The tests use Ed25519 from `cryptography` as the deployer's signer and verifier, in the tests only.

**What a valid signature proves:** the holder of the signing key produced this receipt, for this entry digest, ledger head and checkpoint, and it has not been changed since (a one-field change to any of the ten fields invalidates it; tested).

**What it does not prove:** that the Decision was right; that the key holder is honest or careful (a key holder can sign a receipt for a ledger it rewrote); that the checkpoint inside the receipt was independently custodied (the receipt states the checkpoint, it cannot show where it was kept); or that the key is still trusted.

Key custody, trust, rotation and revocation belong to the deployer's `sign` and `verify` callables. delta defines none of them.

A checkpoint can be signed the same way (`CheckpointStore.write(cp, sign=...)`, `read(verify=...)`, domain tag `delta-ledger-checkpoint-signature/1\n`). That signature has the same limits and one more: it carries no freshness, so an old validly signed checkpoint is still validly signed. Forward-only is enforced by `CheckpointStore.write`, which anyone with file access can bypass.

### Durable store

`delta.FileLedgerStore(directory)` keeps `ledger.jsonl`, one JSON object per line: `{"v": 1, "entry": <the entry exactly as export() renders it>, "sum": <sha256 of the entry's canonical JSON>}`. Use `DecisionLedger(sink=store.sink)` to record. Standard library only.

- **`sink(entry)`** appends one line, flushes and `os.fsync`s the file, and fsyncs the directory when the file is created (tested by counting `os.fsync` calls). It is idempotent by `audit_id`: if the last line holds the same entry it writes nothing (and still fsyncs, so a retry never acknowledges a line whose first fsync failed); the same `audit_id` with different content raises `StoreError`. It also refuses an entry whose `previous_hash` is not the last stored entry's hash, and refuses to append after a torn or damaged final line until `repair()` is run. A write that fails part-way is truncated back.
- **`load(require_context=False)`** returns `(ledger, report)`. Each line's `sum` is checked and the ledger is rebuilt with `DecisionLedger.from_export`, so every hash and the chain are recomputed and nothing stored is trusted (a line edited with its `sum` fixed up is refused; so is a reordered or gapped file). A torn final line (no trailing newline, invalid JSON or a bad `sum`) is dropped and reported as `report["recovered_tail"] = {"count", "bytes"}`. Damage anywhere before the final line raises `StoreCorruptError` naming the line. `load` never writes. `require_context` is not stored in the file: say it again at load, and a ledger recorded with it but loaded without it is judged less strictly. `load(..., attach=True)` returns a ledger that records to the same store, for a restarted process to carry on.
- **`repair()`** truncates a torn final line, and only that, then fsyncs. It refuses, changing nothing, when the damage is not confined to the final line.
- **`CheckpointStore(path)`** is a separate file for the checkpoint: replaced atomically (temp file in the same directory, fsync, `os.replace`, directory fsync; a crash before the replace leaves the old file, tested by making `os.replace` raise), forward-only (a lower length, or the same length with a different head, is refused), typed on read (corrupt, mistyped or extra-key files are refused), and optionally signed (see Signing). **It is meant to live on separate storage. Independence is the deployer's responsibility: a checkpoint beside the file it protects is not independent.** Whoever can rewrite the ledger file can rewrite the checkpoint beside it, and `reconcile` will then agree (this is a test that is written to say so).
- **A planted checkpoint can deny writes.** Whoever can write the checkpoint file can plant an unsigned (or wrongly signed) checkpoint with a huge length. Forward-only would then treat that length as a floor and refuse every honest write, and `reconcile` would report `LOST` for as long as it stays. Pass `verify=` to `CheckpointStore.write` (the same callable `read` takes): an existing file that does not parse, is unsigned or fails `verify` is then corrupt and `write` raises `StoreCorruptError` instead of using its length. The remedy is explicit: the operator inspects the file and calls `write(cp, sign=..., verify=..., replace_corrupt=True)`, which replaces a corrupt file without the forward-only comparison (a sound file still gates forward-only). This is a denial of service the store reports, not one it prevents: anyone who can write the file can still delete or replace it.
- **`reconcile(store, checkpoint_store)`** loads (without repairing) and compares, returning `{"status", ...}`: `OK`; `UNWITNESSED_TAIL` (OK plus `unwitnessed`, the count of entries after the checkpoint, which nothing independent vouches for); `LOST` (the checkpoint is ahead of the file, so acknowledged entries are missing, including when a torn tail cut them off); `DIVERGED` (the history at the checkpoint's length differs, e.g. rewritten and re-chained); `NO_CHECKPOINT`; `EMPTY`. Mid-file damage raises `StoreCorruptError`.

**The phantom.** The sink runs before the ledger commits, so the file can hold an entry the in-memory ledger rolled back, or one whose `append` raised after the sink returned. Such an entry was never acknowledged to the caller, but `load` includes it. A retry of the same Decision reproduces the same `audit_id` and is idempotent (tested: one line). A different Decision after a phantom is refused by the sink (its `previous_hash` does not continue the stored chain) rather than forking the file; recover by loading the store and carrying on from the loaded ledger.

**Limits, stated plainly.**
- The kill test is a real `SIGKILL` of a child process: every entry whose `append` had returned was found on load, with at most one further (unacknowledged) entry. **No OS-crash or power-loss test was run, and a process kill does not prove survival of either.**
- `fsync` asks the OS to put the data on the device. It does not guarantee the device's own write cache is flushed (that depends on the filesystem, mount options and hardware), and on some platforms `os.fsync` does not flush the drive cache at all. Directory fsync on file creation is what makes the new name durable. Only Linux was tested. The directory fsync needs a platform that can open a directory; Windows is untested.
- One writer per directory is assumed. Tested: two stores on one directory, taken in turn, with the second writer's ledger not built from the first's entries: the second is refused (`StoreError`), nothing interleaved. On POSIX the sink also holds an exclusive `flock` while it checks the last line and appends. **Two processes racing to append at the same moment is untested**, as is concurrent `CheckpointStore.write` from two processes.
- A torn-line rule has a cost: a final line corrupted on purpose (bad `sum`) is dropped like a torn one. `report["recovered_tail"]` and `reconcile` (`LOST` or `UNWITNESSED_TAIL` against a checkpoint) are how that is noticed.
- `load` reads the whole file into memory.

### What each guarantee is

| Property | What provides it | What it is not |
|---|---|---|
| Integrity (tamper evidence) | The hash chain, recomputed on load and on `from_export`, plus a checkpoint held where the ledger's writer cannot rewrite it (`reconcile`, `verify_export`) | Not given by the file or the line checksums: those catch accidents, not an attacker who recomputes them |
| Availability (can still write after a fault) | `append` atomicity and healing in process; `repair()` and an idempotent sink after a torn write | Not high availability: a phantom or a stored-chain mismatch stops the sink until it is reconciled |
| Durability (survives restart or crash) | `FileLedgerStore.sink`: fsync before the call returns; tested with a real `SIGKILL` | Not proven for OS crash or power loss; depends on the filesystem and device |
| Authenticity (who produced a receipt) | Only the deployer's `sign` and `verify` over `verification_receipt` output (Signing) | Not given by the store; the store is not a database and does not make receipts authentic |

These are four different properties. Having one says nothing about the others.

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
| In-memory ledger, optional file store | `DecisionLedger` itself is in memory. `FileLedgerStore` (see Durable store) persists it across a process kill, but no OS-crash or power-loss test was run, one writer per directory is assumed, and tamper-evidence still holds only relative to a checkpoint kept outside the ledger's writer. `append` is atomic against faults inside the process (see below). This is a reference ledger; production custody is sentinel_os. |
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
- A run belongs to one event and an event has one Decision; a Decision is never older than the newest recorded for its entity (when a context is given).
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
`DecisionLedger(require_context=False, sink=None)`  
`DecisionLedger.snapshot() -> (entries, chain_head, length)`  
`DecisionLedger.export() -> dict`, `DecisionLedger.from_export(data) -> DecisionLedger`, `delta.verify_export(data, checkpoint=None, *, require_context=None) -> DecisionLedger`  
`LedgerCheckpoint.to_dict() -> dict`, `LedgerCheckpoint.from_dict(d) -> LedgerCheckpoint`  
`delta.cns_connector.verification_receipt(ledger, checkpoint, *, context, decision_digest, index=-1) -> dict`

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

**Ledger receipt.** `verification_receipt(ledger, checkpoint, *, context, decision_digest, index=-1)` raises `ValueError` unless `ledger.verify_chain_integrity()` and `ledger.verify_against(checkpoint)` are both true (so an empty, foreign, rolled-back or mistyped checkpoint, or a broken chain, yields nothing), and unless `0 <= index < checkpoint.length`: the independent checkpoint must cover the entry, so a checkpoint must be taken after the entry it covers, and an entry newer than the checkpoint has no independent evidence and gets no receipt. The ledger must be exactly a `DecisionLedger` (a subclass is a `TypeError`, because it could override what the receipt reports). It is read once as one locked snapshot and the checkpoint once; `ledger_length` and `ledger_head` are derived from the snapshotted entries themselves, and only those values are verified and stated. It returns a plain dict of exactly ten fields: `schema` `"ledger-receipt/1"`, `entry_digest` (`cns.gate.subject_digest` of `entry_content` for the entry at `index`, so it changes with the Decision, the run, the context and the decision digest), the resolved non-negative `index`, `entry_hash` (the entry's own `immutable_hash`) and `previous_hash` (the entry's own `previous_hash`), `ledger_length`, `ledger_head`, `checkpoint_length`, `checkpoint_head` and a validated copy of the `context`. It is what delta shows a verifier; the ledger itself is not handed to CNS. **It is not signed.** It states that, at the moment it was made, the ledger verified and contained the history of the checkpoint it was given, including the entry. A caller who fabricates a ledger and a consistent checkpoint, or simply builds the dict by hand, is not stopped by anything here; the receipt is only as good as where the checkpoint was kept.

The CNS digest is separate from the ledger's `immutable_hash` chain and from the obligation's `decision_fingerprint` link, both unchanged and unaware of CNS. It binds a verdict to the record exactly as stored, chain hashes included.

Apache-2.0.
