"""Round 5: reproduced review defects. Each test fails on the code before the step that fixes it."""
import dataclasses
import hashlib
from datetime import datetime

import pytest
from beta import Decision, decision_fingerprint
from beta.fingerprint import content_fingerprint

from delta import DecisionLedger, DuplicateRecordError
from delta.context import CONTEXT_SCHEMA

T0 = datetime(2026, 1, 1, 12, 0, 0)
CTX = {"schema": CONTEXT_SCHEMA, "entity_id": "p1", "event_id": "ev-1", "run_id": "run-1"}


def ctx(n):
    return {**CTX, "event_id": f"ev-{n}", "run_id": f"run-{n}"}


def decision(i=0, **over):
    base = dict(entity_id="p1", decision_id="r", decision="ESCALATE", timestamp=T0, confidence=0.9,
                triggered_by_locks=("sepsis_lock",), triggered_by_keys=(f"k{i}",))
    base.update(over)
    base["decision_fingerprint"] = decision_fingerprint({
        "entity_id": base["entity_id"], "decision_id": base["decision_id"], "decision": base["decision"],
        "triggered_by_locks": list(base["triggered_by_locks"]), "triggered_by_keys": list(base["triggered_by_keys"])})
    return Decision(**base)


def state(ledger):
    return (ledger.chain_head, len(ledger.entries), list(ledger.entries), ledger.verify_chain_integrity())


# --- Step 12: replay does not depend on content_fingerprint ---------------------------------------------------------

def test_a_replay_with_a_blanked_content_fingerprint_is_refused():
    ledger = DecisionLedger(require_context=True)
    d0 = decision()
    d = dataclasses.replace(d0, content_fingerprint=content_fingerprint(d0))
    ledger.append(d, context=ctx(1))
    before = state(ledger)
    legacy = dataclasses.replace(d, content_fingerprint="")
    with pytest.raises(DuplicateRecordError, match="replay"):
        ledger.append(legacy, context=ctx(2))
    assert state(ledger) == before


# --- Step 13: exact types, validate then copy -----------------------------------------------------------------------

class Flip(list):
    """Honest for the first reads, forged afterwards."""

    reads = 0

    def __iter__(self):
        type(self).reads += 1
        return iter(["forged_key"]) if type(self).reads > 2 else list.__iter__(self)


class LiarStr(str):
    def __eq__(self, other):
        return False

    def __ne__(self, other):
        return False

    __hash__ = str.__hash__


class NeverLess(datetime):
    def __lt__(self, other):
        return False


def test_a_list_subclass_cannot_be_stored_differently_from_what_was_validated():
    Flip.reads = 0
    d = decision()
    object.__setattr__(d, "triggered_by_keys", Flip(d.triggered_by_keys))
    ledger = DecisionLedger(require_context=True)
    try:
        entry = ledger.append(d, context=CTX)
    except (TypeError, ValueError):
        assert state(ledger)[1] == 0
        return
    assert entry.decision.triggered_by_keys == ("k0",)
    assert ledger.verify_chain_integrity()


def test_an_entity_id_subclass_with_lying_equality_is_refused():
    d = dataclasses.replace(decision(), entity_id=LiarStr("p1"))
    ledger = DecisionLedger(require_context=True)
    before = state(ledger)
    with pytest.raises(TypeError, match="entity_id"):
        ledger.append(d, context=CTX)
    assert state(ledger) == before


def test_a_str_subclass_in_a_tuple_field_is_refused():
    d = dataclasses.replace(decision(), triggered_by_keys=(LiarStr("k0"),))
    ledger = DecisionLedger()
    with pytest.raises(TypeError, match="triggered_by_keys"):
        ledger.append(d)
    assert len(ledger.entries) == 0


def test_a_datetime_subclass_as_the_decision_time_is_refused():
    d = dataclasses.replace(decision(), timestamp=NeverLess(2026, 1, 1, 12))
    ledger = DecisionLedger()
    with pytest.raises(TypeError, match="Decision.timestamp"):
        ledger.append(d)
    assert len(ledger.entries) == 0


def test_a_datetime_subclass_as_the_entry_time_cannot_backdate():
    ledger = DecisionLedger(require_context=True)
    ledger.append(decision(0), context=ctx(1))
    before = state(ledger)
    with pytest.raises(TypeError, match="timestamp"):
        ledger.append(decision(1), timestamp=NeverLess(1970, 1, 1), context=ctx(2))
    assert state(ledger) == before
