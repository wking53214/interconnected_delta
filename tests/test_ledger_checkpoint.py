"""A checkpoint lets a holder outside the ledger detect truncation and rewrites the chain alone cannot.

`verify_chain_integrity` recomputes the chain from genesis and compares it with the ledger's own `chain_head`.
Whoever can edit the entries can also reset `chain_head`, or recompute the whole chain after a change, and it
still verifies. A `(length, head)` pair held somewhere the ledger's holder cannot rewrite does not: the ledger
may grow past it, but the entry at that length must still hash to that head.

What this does NOT give you: the checkpoint is only as trustworthy as where it is kept. The ledger is in memory
and loses everything on a restart; production custody is sentinel_os.
"""
import dataclasses
import hashlib
from datetime import datetime

import pytest
from beta import Decision, decision_fingerprint

from delta import DecisionLedger, LedgerCheckpoint

T0 = datetime(2026, 1, 1, 12, 0, 0)


def decision(i):
    base = dict(entity_id="p1", decision_id="r", decision="ESCALATE", timestamp=T0, confidence=0.9,
                triggered_by_locks=("sepsis_lock",), triggered_by_keys=(f"k{i}",))
    base["decision_fingerprint"] = decision_fingerprint({
        "entity_id": "p1", "decision_id": "r", "decision": "ESCALATE",
        "triggered_by_locks": ["sepsis_lock"], "triggered_by_keys": [f"k{i}"]})
    return Decision(**base)


def ledger_of(n):
    ledger = DecisionLedger()
    for i in range(n):
        ledger.append(decision(i))
    return ledger


def test_a_checkpoint_records_the_length_and_head_and_is_frozen():
    ledger = ledger_of(3)
    cp = ledger.checkpoint()
    assert (cp.length, cp.head) == (3, ledger.chain_head)
    with pytest.raises(dataclasses.FrozenInstanceError):
        cp.length = 1  # type: ignore[misc]


def test_an_empty_ledgers_checkpoint_is_genesis_and_verifies():
    ledger = DecisionLedger()
    cp = ledger.checkpoint()
    assert cp.length == 0 and cp.head == hashlib.sha256(b"INTERCONNECTED_DELTA_GENESIS").hexdigest()
    assert not ledger.verify_against(cp)


def test_the_ledger_may_grow_past_a_checkpoint():
    ledger = ledger_of(3)
    cp = ledger.checkpoint()
    ledger.append(decision(3))
    assert ledger.verify_against(cp) and ledger.verify_against(ledger.checkpoint())


def test_deletion_reordering_and_edits_fail_against_a_checkpoint():
    cp = ledger_of(3).checkpoint()
    ledger = ledger_of(3)
    del ledger.entries[1]
    assert not ledger.verify_against(cp)
    ledger = ledger_of(3)
    ledger.entries[0], ledger.entries[1] = ledger.entries[1], ledger.entries[0]
    assert not ledger.verify_against(cp)
    ledger = ledger_of(3)
    first = ledger.entries[0]
    ledger.entries[0] = dataclasses.replace(first, decision=dataclasses.replace(first.decision, instructions="X"))
    assert not ledger.verify_against(cp)


def test_tail_truncation_is_caught_even_when_the_head_is_reset_to_match():
    cp = ledger_of(3).checkpoint()
    ledger = ledger_of(3)
    ledger.entries.pop()
    ledger.chain_head = ledger.entries[-1].immutable_hash
    assert ledger.verify_chain_integrity()  # the chain alone cannot tell: this is the gap
    assert not ledger.verify_against(cp)


def test_a_complete_recompute_of_a_tampered_chain_is_caught():
    original = ledger_of(3)
    cp = original.checkpoint()
    rewritten = DecisionLedger()
    for i, entry in enumerate(original.entries):
        forged = dataclasses.replace(entry.decision, instructions="DO NOTHING") if i == 1 else entry.decision
        rewritten.append(forged, entry.timestamp)
    assert rewritten.verify_chain_integrity()  # internally consistent, so the chain alone accepts it
    assert not rewritten.verify_against(cp)


def test_a_checkpoint_from_a_different_ledger_does_not_verify():
    assert not ledger_of(2).verify_against(ledger_of(3).checkpoint())
    other = DecisionLedger()
    for i in range(3):
        other.append(decision(i + 10))
    assert not other.verify_against(ledger_of(3).checkpoint())


def test_a_checkpoint_that_does_not_match_a_broken_ledger_is_never_satisfied():
    ledger = ledger_of(3)
    cp = ledger.checkpoint()
    ledger.entries[1] = dataclasses.replace(ledger.entries[1], immutable_hash="0" * 64)
    assert not ledger.verify_chain_integrity() and not ledger.verify_against(cp)


def test_a_malformed_checkpoint_is_refused():
    ledger = ledger_of(1)
    with pytest.raises(TypeError, match="LedgerCheckpoint"):
        ledger.verify_against((1, ledger.chain_head))  # type: ignore[arg-type]
    for bad in (LedgerCheckpoint(-1, ledger.chain_head), LedgerCheckpoint(5, ledger.chain_head),
                LedgerCheckpoint(1, "not a head"), LedgerCheckpoint("1", ledger.chain_head)):  # type: ignore[arg-type]
        assert not ledger.verify_against(bad)
