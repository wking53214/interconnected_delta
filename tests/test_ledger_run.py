"""The ledger knows which pipeline run a Decision belongs to, refuses a repeat, and validates what it records.

A Decision appended twice was two valid entries, and the chain could not say which run either came from. With a
context the entry carries run_id and event_id in its hashed subset (so the chain protects them), a second
Decision with the same fingerprint in the same run is refused, and the Decision must be about the context's
entity and internally consistent. Without a context nothing about an entry or its hash changes.
"""
import dataclasses
from datetime import datetime

import pytest
from beta import Decision, decision_fingerprint

from delta import DecisionLedger, DuplicateRecordError
from delta.ledger import _canonical_subset

T0 = datetime(2026, 1, 1, 12, 0, 0)
CTX = {"schema": "spine-context/1", "entity_id": "p1", "event_id": "ev-1", "run_id": "run-1"}


def decision(keys=("septic_shock",), **over):
    base = dict(entity_id="p1", decision_id="r", decision="ESCALATE", timestamp=T0, confidence=0.9,
                triggered_by_locks=("sepsis_lock",), triggered_by_keys=tuple(keys))
    base.update(over)
    base["decision_fingerprint"] = over.get("decision_fingerprint") or decision_fingerprint({
        "entity_id": base["entity_id"], "decision_id": base["decision_id"], "decision": base["decision"],
        "triggered_by_locks": list(base["triggered_by_locks"]), "triggered_by_keys": list(base["triggered_by_keys"])})
    return Decision(**base)


# --- nothing changes without a context --------------------------------------------------------------------------

def test_an_entry_recorded_without_a_context_has_no_run_and_the_old_hash_subset():
    entry = DecisionLedger().append(decision())
    assert entry.run_id is None and entry.event_id is None
    assert "run_id" not in _canonical_subset("a", T0, decision())


# --- a context is recorded and protected by the chain -----------------------------------------------------------

def test_an_entry_recorded_with_a_context_carries_its_run_and_event():
    ledger = DecisionLedger()
    entry = ledger.append(decision(), context=dict(CTX))
    assert (entry.run_id, entry.event_id) == ("run-1", "ev-1")
    assert ledger.verify_chain_integrity()


@pytest.mark.parametrize("field, value", [("run_id", "run-2"), ("event_id", "ev-2"), ("run_id", None), ("event_id", None)])
def test_changing_or_removing_the_run_or_event_of_a_recorded_entry_breaks_the_chain(field, value):
    ledger = DecisionLedger()
    ledger.append(decision(), context=dict(CTX))
    ledger.entries[0] = dataclasses.replace(ledger.entries[0], **{field: value})
    assert not ledger.verify_chain_integrity()


def test_the_run_changes_the_entry_hash():
    a, b = DecisionLedger(), DecisionLedger()
    assert a.append(decision(), context=dict(CTX)).immutable_hash != \
        b.append(decision(), context={**CTX, "run_id": "run-2"}).immutable_hash
    assert a.entries[0].immutable_hash != DecisionLedger().append(decision()).immutable_hash


# --- replay -----------------------------------------------------------------------------------------------------

def test_the_same_decision_in_the_same_run_is_refused_and_leaves_the_ledger_unchanged():
    ledger = DecisionLedger()
    ledger.append(decision(), context=dict(CTX))
    head, size = ledger.chain_head, len(ledger)
    with pytest.raises(DuplicateRecordError, match="run 'run-1' already recorded"):
        ledger.append(decision(), context=dict(CTX))
    assert (ledger.chain_head, len(ledger)) == (head, size) and ledger.verify_chain_integrity()


def test_the_same_decision_for_another_event_in_another_run_is_recorded():
    ledger = DecisionLedger()
    ledger.append(decision(), context=dict(CTX))
    ledger.append(decision(), context={**CTX, "run_id": "run-2", "event_id": "ev-2"})
    assert len(ledger) == 2 and ledger.verify_chain_integrity()


def test_without_a_context_repeats_are_still_accepted_as_before():
    ledger = DecisionLedger()
    ledger.append(decision())
    ledger.append(decision())
    assert len(ledger) == 2


# --- the Decision must fit the context, and be consistent -------------------------------------------------------

def test_a_decision_about_another_entity_than_the_context_is_refused():
    ledger = DecisionLedger()
    with pytest.raises(ValueError, match="Decision is about 'p2' but the context is about 'p1'"):
        ledger.append(decision(entity_id="p2"), context=dict(CTX))
    assert len(ledger) == 0


@pytest.mark.parametrize("bad, exc, match", [
    ({k: v for k, v in CTX.items() if k != "run_id"}, ValueError, "exactly"),
    ({**CTX, "run_id": ""}, ValueError, "non-empty"),
    ({**CTX, "event_id": 3}, TypeError, "must be a str"),
    ({**CTX, "schema": "spine-context/7"}, ValueError, "unsupported context schema"),
    ("run-1", TypeError, "mapping"),
])
def test_a_malformed_context_is_refused(bad, exc, match):
    ledger = DecisionLedger()
    with pytest.raises(exc, match=match):
        ledger.append(decision(), context=bad)
    assert len(ledger) == 0


def _bypassed(**over):
    d = object.__new__(Decision)
    fields = dict(entity_id="p1", decision_id="r", decision="ESCALATE", timestamp=T0, confidence=0.9,
                  triggered_by_locks=("sepsis_lock",), triggered_by_keys=("septic_shock",), newly_triggered_locks=(),
                  decision_fingerprint="x", audit_hash="", reasoning="", reversal_conditions=(), instructions="",
                  confidence_basis="keys")
    fields.update(over)
    for k, v in fields.items():
        object.__setattr__(d, k, v)
    return d


def test_a_contradictory_basis_that_bypassed_construction_is_refused_at_recording():
    ledger = DecisionLedger()
    with pytest.raises(ValueError, match="implies 'keys'"):
        ledger.append(_bypassed(confidence_basis="absence"))
    with pytest.raises(ValueError, match="implies 'keys'"):
        ledger.append(_bypassed(confidence_basis="absence"), context=dict(CTX))
    assert len(ledger) == 0


@pytest.mark.parametrize("bad", [float("nan"), 2.0, -0.5])
def test_an_impossible_confidence_is_refused_at_recording(bad):
    ledger = DecisionLedger()
    with pytest.raises(ValueError, match="confidence must be a finite number"):
        ledger.append(decision(confidence=bad))
    assert len(ledger) == 0


def test_with_a_context_the_fingerprint_must_match_the_decision():
    ledger = DecisionLedger()
    forged = dataclasses.replace(decision(), triggered_by_keys=("ghost",), confidence_basis="")
    with pytest.raises(ValueError, match="decision_fingerprint does not match"):
        ledger.append(forged, context=dict(CTX))
    assert len(ledger) == 0
    ledger.append(forged)  # without a context the fingerprint is not checked, as before
    assert len(ledger) == 1


# --- the run is part of what the CNS entry digest binds ---------------------------------------------------------

def test_the_entry_content_carries_the_run_only_when_the_entry_has_one():
    from delta.cns_connector import entry_content

    plain = DecisionLedger().append(decision())
    run = DecisionLedger().append(decision(), context=dict(CTX))
    assert not {"run_id", "event_id"} & set(entry_content(plain))
    assert (entry_content(run)["run_id"], entry_content(run)["event_id"]) == ("run-1", "ev-1")
