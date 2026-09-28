from datetime import datetime, timedelta

import pytest
from beta import Decision

from delta.ledger import DecisionLedger

T0 = datetime(2026, 1, 1, 12, 0, 0)


def make_decision(entity_id="patient1", decision_id="d1", decision="ESCALATE", fp="fp-abc", ts=T0):
    return Decision(
        entity_id=entity_id,
        decision_id=decision_id,
        decision=decision,
        timestamp=ts,
        confidence=1.0,
        decision_fingerprint=fp,
    )


def test_append_returns_entry_with_chained_hash():
    ledger = DecisionLedger()
    entry = ledger.append(make_decision())
    assert entry.immutable_hash != ""
    assert ledger.chain_head == entry.immutable_hash  # chain_head advances to the new entry
    assert len(ledger) == 1


def test_genesis_hash_seeds_first_entry():
    ledger = DecisionLedger()
    genesis = ledger.chain_head
    entry = ledger.append(make_decision())
    assert entry.previous_hash == genesis


def test_chain_head_advances_with_each_append():
    ledger = DecisionLedger()
    e1 = ledger.append(make_decision(decision_id="d1", fp="fp1"))
    e2 = ledger.append(make_decision(decision_id="d2", fp="fp2"))
    assert e2.previous_hash == e1.immutable_hash
    assert e1.immutable_hash != e2.immutable_hash


def test_verify_chain_integrity_true_for_untampered_chain():
    ledger = DecisionLedger()
    for i in range(5):
        ledger.append(make_decision(decision_id=f"d{i}", fp=f"fp{i}", ts=T0 + timedelta(hours=i)))
    assert ledger.verify_chain_integrity() is True


def test_verify_chain_integrity_false_if_entry_tampered():
    ledger = DecisionLedger()
    ledger.append(make_decision(decision_id="d1", fp="fp1"))
    ledger.append(make_decision(decision_id="d2", fp="fp2"))

    # Tamper: swap in a different decision on an existing entry without
    # recomputing the hash (simulates a modified stored row).
    tampered_decision = make_decision(decision_id="d1-TAMPERED", fp="fp1")
    ledger.entries[0] = type(ledger.entries[0])(
        audit_id=ledger.entries[0].audit_id,
        timestamp=ledger.entries[0].timestamp,
        decision=tampered_decision,
        previous_hash=ledger.entries[0].previous_hash,
        immutable_hash=ledger.entries[0].immutable_hash,
    )
    assert ledger.verify_chain_integrity() is False


def test_verify_chain_integrity_false_if_hash_forged():
    ledger = DecisionLedger()
    ledger.append(make_decision())
    entry = ledger.entries[0]
    forged = type(entry)(
        audit_id=entry.audit_id,
        timestamp=entry.timestamp,
        decision=entry.decision,
        previous_hash=entry.previous_hash,
        immutable_hash="0" * 64,
    )
    ledger.entries[0] = forged
    assert ledger.verify_chain_integrity() is False


def test_empty_ledger_verifies_true():
    ledger = DecisionLedger()
    assert ledger.verify_chain_integrity() is True


def test_two_ledgers_with_identical_decisions_produce_identical_chains():
    def build():
        ledger = DecisionLedger()
        ledger.append(make_decision(decision_id="d1", fp="fp1", ts=T0))
        ledger.append(make_decision(decision_id="d2", fp="fp2", ts=T0 + timedelta(hours=1)))
        return ledger.chain_head

    assert build() == build()
