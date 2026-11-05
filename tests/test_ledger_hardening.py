"""Four ways an independent adversarial review got past the ledger, each now refused.

1. Anything with a `validate` attribute was recorded: a Decision subclass whose validate does nothing, or a plain
   namespace. The ledger called the object's own validate, so the object chose whether to be judged.
2. The same event could be recorded again by giving it a new run id.
3. Concurrent appends corrupted the chain without an error: 2400 appends from 8 threads left a chain that did not verify.
4. (digest format, see test_cns_context.py)
"""
import dataclasses
import threading
import types
from datetime import datetime

import pytest
from beta import Decision, decision_fingerprint

from delta import DecisionLedger, DuplicateRecordError

T0 = datetime(2026, 1, 1, 12, 0, 0)
CTX = {"schema": "spine-context/1", "entity_id": "p1", "event_id": "ev-1", "run_id": "run-1"}


def decision(keys=("septic_shock",)):
    base = dict(entity_id="p1", decision_id="r", decision="ESCALATE", timestamp=T0, confidence=0.9,
                triggered_by_locks=("sepsis_lock",), triggered_by_keys=tuple(keys))
    base["decision_fingerprint"] = decision_fingerprint({
        "entity_id": "p1", "decision_id": "r", "decision": "ESCALATE",
        "triggered_by_locks": ["sepsis_lock"], "triggered_by_keys": list(keys)})
    return Decision(**base)


# --- 1. the ledger judges the Decision itself, not whatever the object says about itself ---------------------------

def test_a_decision_subclass_whose_validate_does_nothing_is_refused():
    class Quiet(Decision):
        def validate(self, *, check_fingerprint=False):  # an object that opts out of being judged
            return None

    quiet = object.__new__(Quiet)
    for f in dataclasses.fields(Decision):
        object.__setattr__(quiet, f.name, getattr(decision(), f.name))
    object.__setattr__(quiet, "confidence_basis", "absence")  # contradicts its keys
    ledger = DecisionLedger()
    with pytest.raises(TypeError, match="records beta.Decision only"):
        ledger.append(quiet)
    with pytest.raises(TypeError, match="records beta.Decision only"):
        ledger.append(quiet, context=CTX)
    assert len(ledger) == 0


def test_an_object_that_merely_looks_like_a_decision_is_refused():
    fake = types.SimpleNamespace(validate=lambda **_: None, entity_id="p1", decision_fingerprint="x")
    ledger = DecisionLedger()
    with pytest.raises(TypeError, match="records beta.Decision only"):
        ledger.append(fake)
    assert len(ledger) == 0


def test_a_real_decision_is_still_recorded():
    assert len(_ledger_with(decision())) == 1


def _ledger_with(d):
    ledger = DecisionLedger()
    ledger.append(d)
    return ledger


# --- 2. an event is recorded once, whatever run it is brought back in ----------------------------------------------

def test_the_same_event_and_decision_under_a_new_run_id_is_refused():
    ledger = DecisionLedger()
    ledger.append(decision(), context=CTX)
    head, size = ledger.chain_head, len(ledger)
    with pytest.raises(DuplicateRecordError, match="event 'ev-1' already has a Decision recorded under run 'run-1'"):
        ledger.append(decision(), context={**CTX, "run_id": "run-2"})
    assert (ledger.chain_head, len(ledger)) == (head, size)


def test_a_new_event_is_recorded_but_a_different_decision_for_the_same_event_is_refused():
    ledger = DecisionLedger()
    ledger.append(decision(), context=CTX)
    ledger.append(decision(keys=("fever",)), context={**CTX, "run_id": "run-2", "event_id": "ev-2"})
    with pytest.raises(DuplicateRecordError):
        ledger.append(decision(keys=("respiratory_distress",)), context={**CTX, "run_id": "run-3"})
    assert len(ledger) == 2 and ledger.verify_chain_integrity()


# --- 3. appends from several threads leave a chain that verifies ---------------------------------------------------

def test_concurrent_appends_leave_a_chain_that_verifies():
    ledger = DecisionLedger()
    errors = []

    def work(worker):
        try:
            for i in range(300):
                ledger.append(decision(keys=(f"k{worker}_{i}",)))
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=work, args=(w,)) for w in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert errors == [] and len(ledger) == 2400 and ledger.verify_chain_integrity()
