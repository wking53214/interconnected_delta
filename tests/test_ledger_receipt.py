"""The ledger receipt: what delta shows a verifier about one entry, and what it refuses to show.

Needs cns (DELTA_REQUIRE_CNS=1 in CI). The receipt is not signed: nothing here claims a fabricated, self-consistent
receipt is stopped, only that delta never issues one from an empty, foreign or broken state.
"""
import dataclasses
import os
from datetime import datetime

import pytest

if os.environ.get("DELTA_REQUIRE_CNS") == "1":
    import cns.gate as gate
else:
    gate = pytest.importorskip("cns.gate")

from beta import Decision, decision_fingerprint  # noqa: E402

from delta import DecisionLedger, LedgerCheckpoint  # noqa: E402
from delta.cns_connector import entry_content, verification_receipt  # noqa: E402
from delta.context import CONTEXT_SCHEMA  # noqa: E402

T0 = datetime(2026, 1, 1, 12, 0, 0)
CTX = {"schema": CONTEXT_SCHEMA, "entity_id": "p1", "event_id": "ev-1", "run_id": "run-1"}
DD = gate.subject_digest({"label": "beta decision digest"})
KEYS = ("schema", "entry_digest", "index", "entry_hash", "previous_hash", "ledger_length", "ledger_head", "checkpoint_length", "checkpoint_head", "context")


def decision(i=0, **over):
    base = dict(entity_id="p1", decision_id="r", decision="ESCALATE", timestamp=T0, confidence=0.9,
                triggered_by_locks=("sepsis_lock",), triggered_by_keys=(f"k{i}",))
    base.update(over)
    base["decision_fingerprint"] = decision_fingerprint({
        "entity_id": base["entity_id"], "decision_id": base["decision_id"], "decision": base["decision"],
        "triggered_by_locks": list(base["triggered_by_locks"]), "triggered_by_keys": list(base["triggered_by_keys"])})
    return Decision(**base)


def recorded(n=1):
    ledger = DecisionLedger(require_context=True)
    for i in range(n):
        ledger.append(decision(i), context={**CTX, "event_id": f"ev-{i + 1}", "run_id": f"run-{i + 1}"})
    return ledger


def ctx_n(n):
    return {**CTX, "event_id": f"ev-{n}", "run_id": f"run-{n}"}


def test_an_honest_ledger_and_its_own_checkpoint_yield_a_receipt():
    ledger = recorded()
    cp = ledger.checkpoint()
    receipt = verification_receipt(ledger, cp, context=CTX, decision_digest=DD)
    assert set(receipt) == set(KEYS) and type(receipt) is dict
    assert receipt["schema"] == "ledger-receipt/1"
    assert receipt["entry_digest"] == gate.subject_digest(entry_content(ledger.entries[0], context=CTX, decision_digest=DD))
    assert (receipt["index"], receipt["ledger_length"], receipt["ledger_head"]) == (0, 1, ledger.chain_head)
    assert (receipt["checkpoint_length"], receipt["checkpoint_head"]) == (cp.length, cp.head)
    assert receipt["context"] == CTX and receipt["context"] is not CTX


def test_a_receipt_after_the_ledger_grew_past_the_checkpoint_names_the_requested_entry():
    ledger = recorded(1)
    cp = ledger.checkpoint()
    ledger.append(decision(1), context=ctx_n(2))
    first = verification_receipt(ledger, cp, context=CTX, decision_digest=DD, index=0)
    last = verification_receipt(ledger, ledger.checkpoint(), context=ctx_n(2), decision_digest=DD)
    assert (first["index"], last["index"], last["ledger_length"], last["checkpoint_length"]) == (0, 1, 2, 2)
    assert first["entry_digest"] != last["entry_digest"]


def test_an_empty_checkpoint_is_refused():
    ledger = recorded()
    with pytest.raises(ValueError, match="checkpoint"):
        verification_receipt(ledger, LedgerCheckpoint(0, DecisionLedger().chain_head), context=CTX, decision_digest=DD)


def test_a_foreign_checkpoint_is_refused():
    other = DecisionLedger(require_context=True)
    other.append(decision(0, decision="DISCHARGE_SAFE"), context=CTX)
    with pytest.raises(ValueError, match="checkpoint"):
        verification_receipt(recorded(), other.checkpoint(), context=CTX, decision_digest=DD)


def test_a_broken_chain_is_refused():
    ledger = recorded(2)
    cp = ledger.checkpoint()
    first = ledger.entries[0]
    ledger.entries[0] = dataclasses.replace(first, decision=dataclasses.replace(first.decision, instructions="edited"))
    with pytest.raises(ValueError, match="chain"):
        verification_receipt(ledger, cp, context=ctx_n(2), decision_digest=DD)


def test_a_mistyped_checkpoint_is_refused():
    ledger = recorded()
    with pytest.raises(ValueError, match="checkpoint"):
        verification_receipt(ledger, LedgerCheckpoint(True, ledger.chain_head), context=CTX, decision_digest=DD)  # type: ignore[arg-type]


def test_the_entry_digest_changes_with_the_decision_the_run_and_the_context():
    base = verification_receipt(recorded(), recorded().checkpoint(), context=CTX, decision_digest=DD)["entry_digest"]
    # a different Decision
    other = DecisionLedger(require_context=True)
    other.append(decision(0, reasoning="changed"), context=CTX)
    assert verification_receipt(other, other.checkpoint(), context=CTX, decision_digest=DD)["entry_digest"] != base
    # a different run and event (and so a different context)
    again = DecisionLedger(require_context=True)
    again.append(decision(0), context=ctx_n(7))
    assert verification_receipt(again, again.checkpoint(), context=ctx_n(7), decision_digest=DD)["entry_digest"] != base
    # a different decision digest
    ledger = recorded()
    changed = gate.subject_digest({"label": "other"})
    assert verification_receipt(ledger, ledger.checkpoint(), context=CTX, decision_digest=changed)["entry_digest"] != base
    # a context the entry was not recorded under cannot be put on the receipt at all
    with pytest.raises(ValueError):
        verification_receipt(ledger, ledger.checkpoint(), context=ctx_n(7), decision_digest=DD)


@pytest.mark.parametrize("index", [1, 5, -2, -9])
def test_an_index_out_of_range_is_refused(index):
    ledger = recorded()
    with pytest.raises(ValueError, match="out of range"):
        verification_receipt(ledger, ledger.checkpoint(), context=CTX, decision_digest=DD, index=index)


@pytest.mark.parametrize("index", [True, 0.0, "0", None])
def test_a_non_int_index_is_refused(index):
    ledger = recorded()
    with pytest.raises(TypeError):
        verification_receipt(ledger, ledger.checkpoint(), context=CTX, decision_digest=DD, index=index)  # type: ignore[arg-type]


def test_a_negative_index_is_resolved_to_a_non_negative_one():
    ledger = recorded(3)
    r = verification_receipt(ledger, ledger.checkpoint(), context=ctx_n(2), decision_digest=DD, index=-2)
    assert r["index"] == 1


def test_building_a_receipt_does_not_change_the_ledger():
    ledger = recorded(2)
    head, n = ledger.chain_head, len(ledger)
    verification_receipt(ledger, ledger.checkpoint(), context=ctx_n(2), decision_digest=DD)
    assert (ledger.chain_head, len(ledger), ledger.verify_chain_integrity()) == (head, n, True)
