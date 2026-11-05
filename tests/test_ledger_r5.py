"""Round 5: reproduced review defects. Each test fails on the code before the step that fixes it."""
import dataclasses
import hashlib
from datetime import datetime

import pytest
from beta import Decision, decision_fingerprint
from beta.fingerprint import content_fingerprint

from delta import DecisionLedger, DuplicateRecordError, LedgerCheckpoint
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


# --- Step 14: a sink may not append ---------------------------------------------------------------------------------

def _in_thread(fn, timeout=5):
    """Run fn in a daemon thread and fail (rather than hang the suite) if it does not return in time."""
    import threading

    box = {}

    def run():
        try:
            box["result"] = fn()
        except BaseException as exc:  # noqa: BLE001
            box["error"] = exc

    t = threading.Thread(target=run, daemon=True)
    t.start()
    t.join(timeout)
    assert not t.is_alive(), "deadlocked"
    return box


def test_a_sink_that_appends_is_refused_not_deadlocked():
    holder = {}
    ledger = DecisionLedger(sink=lambda entry: holder["ledger"].append(decision(1), context=ctx(2)))
    holder["ledger"] = ledger
    before = state(ledger)
    box = _in_thread(lambda: ledger.append(decision(0), context=ctx(1)))
    assert isinstance(box.get("error"), RuntimeError)
    assert "append from inside a sink is not allowed" in str(box["error"])
    assert state(ledger) == before


def test_the_sink_flag_is_cleared_and_another_thread_still_appends():
    import threading

    seen = []
    ledger = DecisionLedger(sink=seen.append)
    ledger.append(decision(0), context=ctx(1))
    box = _in_thread(lambda: ledger.append(decision(1), context=ctx(2)))
    assert "error" not in box
    other = {}
    t = threading.Thread(target=lambda: other.setdefault("e", ledger.append(decision(2), context=ctx(3))))
    t.start()
    t.join(5)
    assert not t.is_alive() and len(ledger.entries) == 3 and len(seen) == 3


# --- Step 15: the checkpoint is read once --------------------------------------------------------------------------

class Lying(LedgerCheckpoint):
    """Honest on the first read of each attribute, and lying on every later one."""

    def __getattribute__(self, name):
        if name in ("length", "head"):
            counts = object.__getattribute__(self, "_counts")
            counts[name] = counts.get(name, 0) + 1
            if counts[name] > 1:
                return {"length": 7, "head": "f" * 64}[name]
        return object.__getattribute__(self, name)


def _lying(cp):
    liar = Lying(cp.length, cp.head)
    object.__setattr__(liar, "_counts", {})
    return liar


def test_verify_against_reads_the_checkpoint_once():
    ledger = DecisionLedger(require_context=True)
    ledger.append(decision(), context=ctx(1))
    liar = _lying(ledger.checkpoint())
    assert ledger.verify_against(liar) is True
    assert object.__getattribute__(liar, "_counts") == {"length": 1, "head": 1}


def test_a_receipt_states_exactly_the_values_that_were_verified():
    pytest.importorskip("cns.gate")
    from delta.cns_connector import verification_receipt

    ledger = DecisionLedger(require_context=True)
    ledger.append(decision(), context=ctx(1))
    honest = ledger.checkpoint()
    liar = _lying(honest)
    try:
        receipt = verification_receipt(ledger, liar, context=ctx(1), decision_digest="a" * 64)
    except ValueError:
        return
    assert (receipt["checkpoint_length"], receipt["checkpoint_head"]) == (honest.length, honest.head)
    assert object.__getattribute__(liar, "_counts") == {"length": 1, "head": 1}


# --- Step 16: the checkpoint must cover the entry; the receipt carries the entry's own hashes -----------------------

def test_a_receipt_has_exactly_ten_fields_including_the_entry_hashes():
    pytest.importorskip("cns.gate")
    from delta.cns_connector import verification_receipt

    ledger = DecisionLedger(require_context=True)
    ledger.append(decision(0), context=ctx(1))
    ledger.append(decision(1), context=ctx(2))
    r = verification_receipt(ledger, ledger.checkpoint(), context=ctx(1), decision_digest="a" * 64, index=0)
    assert list(r) == ["schema", "entry_digest", "index", "entry_hash", "previous_hash", "ledger_length",
                       "ledger_head", "checkpoint_length", "checkpoint_head", "context"]
    assert r["entry_hash"] == ledger.entries[0].immutable_hash
    assert r["previous_hash"] == ledger.entries[0].previous_hash


def test_an_entry_newer_than_the_checkpoint_gets_no_receipt():
    pytest.importorskip("cns.gate")
    from delta.cns_connector import verification_receipt

    ledger = DecisionLedger(require_context=True)
    ledger.append(decision(0), context=ctx(1))
    held = ledger.checkpoint()
    ledger.append(decision(1), context=ctx(2))
    with pytest.raises(ValueError, match="not covered"):
        verification_receipt(ledger, held, context=ctx(2), decision_digest="a" * 64)


def test_replacing_the_entry_the_checkpoint_does_not_cover_is_refused():
    pytest.importorskip("cns.gate")
    from delta.cns_connector import verification_receipt

    ledger = DecisionLedger(require_context=True)
    ledger.append(decision(0), context=ctx(1))
    held = ledger.checkpoint()  # length 1: covers entry 0 only
    ledger.append(decision(1), context=ctx(2))
    ledger.entries.pop()
    ledger.chain_head = ledger.entries[-1].immutable_hash  # drop entry 1
    ledger.append(decision(2), context=ctx(2))  # a different Decision in its place
    assert ledger.verify_chain_integrity() and ledger.verify_against(held)
    with pytest.raises(ValueError, match="not covered"):
        verification_receipt(ledger, held, context=ctx(2), decision_digest="a" * 64, index=1)
