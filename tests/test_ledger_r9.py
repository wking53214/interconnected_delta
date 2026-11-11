"""Round 9: defects found by independent review of the round 8 hardening. Each test fails on the code before the
intervention that fixes it.

Intervention 1 (N3): checkpoint() and len() take the lock and heal first.
"""
import dataclasses
import inspect
import sys
import threading

import pytest

import test_ledger_r8 as H
from delta import DecisionLedger
from delta.ledger import LedgerCheckpoint


# --- Intervention 1 (N3): checkpoint() and len() heal first -----------------------------------------------------------

class _DoubleFault(list):
    """An entries list whose append lands and then raises, and whose rollback (del) raises once as well."""

    def __init__(self, items):
        super().__init__(items)
        self.stage = 2

    def append(self, item):
        list.append(self, item)
        if self.stage == 2:
            self.stage = 1
            raise KeyboardInterrupt("after entries.append")

    def __delitem__(self, key):
        if self.stage == 1:
            self.stage = 0
            raise KeyboardInterrupt("inside the heal")
        list.__delitem__(self, key)


def _double_faulted():
    ledger = H.recorded(1)
    ledger.entries = _DoubleFault(ledger.entries)
    with pytest.raises(KeyboardInterrupt):
        ledger.append(H.decision(1), context=H.ctx_n(2))
    assert ledger._pending is not None  # the commit is left half done
    return ledger


def test_n3_checkpoint_after_a_double_fault_heals_first_and_fits_its_ledger():
    ledger = _double_faulted()
    cp = ledger.checkpoint()
    assert (cp.length, cp.head) == (1, ledger.entries[0].immutable_hash)
    assert ledger.verify_against(cp)


def test_n3_len_after_a_double_fault_heals_first():
    ledger = _double_faulted()
    assert len(ledger) == 1


def test_n3_a_checkpoint_taken_while_a_commit_is_in_progress_waits_for_it():
    ledger = H.recorded(1)
    src, start = inspect.getsourcelines(DecisionLedger._append_locked)
    commit = start + next(i for i, s in enumerate(src) if "self.chain_head = immutable_hash" in s)
    paused, go = threading.Event(), threading.Event()
    code = DecisionLedger._append_locked.__code__

    def tracer(frame, event, arg):
        if frame.f_code is not code:
            return None

        def local(frame, event, arg):
            if event == "line" and frame.f_lineno == commit:
                paused.set()
                go.wait(5)
            return local
        return local

    def appender():
        sys.settrace(tracer)
        try:
            ledger.append(H.decision(1), context=H.ctx_n(2))
        finally:
            sys.settrace(None)

    got = {}
    t1 = threading.Thread(target=appender)
    t1.start()
    assert paused.wait(5)
    t2 = threading.Thread(target=lambda: got.update(cp=ledger.checkpoint(), n=len(ledger)))
    t2.start()
    t2.join(0.3)  # fixed: blocked on the lock the appender holds; before: returns at once with a torn pair
    go.set()
    t1.join(5)
    t2.join(5)
    cp = got["cp"]
    assert (cp.length, got["n"]) == (2, 2)
    assert cp.head == ledger.entries[1].immutable_hash
    assert ledger.verify_against(cp)


# --- Intervention 2 (N9): a receipt is built from the entries tuple, never from what a subclass reports ---------------

def test_n9_a_ledger_subclass_that_reports_a_false_length_gets_no_receipt():
    from delta.cns_connector import verification_receipt

    class Liar(DecisionLedger):
        def snapshot(self):
            entries, head, _ = super().snapshot()
            return entries, head, 999

    ledger = Liar(require_context=True)
    for i in range(2):
        ledger.append(H.decision(i), context=H.ctx_n(i + 1))
    cp = LedgerCheckpoint(2, ledger.chain_head)  # honest; checkpoint() itself would repeat the subclass's length
    with pytest.raises(TypeError, match="DecisionLedger"):
        verification_receipt(ledger, cp, context=H.ctx_n(1), decision_digest=H.DD, index=0)


def test_n9_the_receipt_states_the_length_and_head_of_the_entries_it_verified():
    from delta.cns_connector import verification_receipt

    ledger = H.recorded(2)
    cp = ledger.checkpoint()
    receipt = verification_receipt(ledger, cp, context=H.ctx_n(1), decision_digest=H.DD, index=0)
    assert receipt["ledger_length"] == 2
    assert receipt["ledger_head"] == ledger.entries[-1].immutable_hash


# --- Intervention 3 (N7): append refuses an audit_hash that export would refuse ---------------------------------------

class _Str(str):
    pass


@pytest.mark.parametrize("bad", [123, None, b"x", _Str("x")])
def test_n7_append_refuses_an_audit_hash_that_is_not_exactly_a_str(bad):
    ledger = DecisionLedger(require_context=True)
    decision = dataclasses.replace(H.decision(0), audit_hash=bad)
    with pytest.raises(TypeError, match="audit_hash"):
        ledger.append(decision, context=H.ctx_n(1))
    assert len(ledger) == 0 and ledger.verify_chain_integrity()


def test_n7_append_and_export_agree_on_a_str_audit_hash():
    ledger = DecisionLedger(require_context=True)
    ledger.append(dataclasses.replace(H.decision(0), audit_hash="ptr-1"), context=H.ctx_n(1))
    assert ledger.export()["entries"][0]["decision"]["audit_hash"] == "ptr-1"


# --- Intervention 4: a str given for a tuple field is refused, not stored as its characters --------------------------

_TUPLE_FIELDS = ("triggered_by_locks", "triggered_by_keys", "newly_triggered_locks", "reversal_conditions")


def _with_field(name, value):
    from beta import Decision

    base = H.decision(0)
    decision = object.__new__(Decision)  # beta's constructor may not take it; the ledger must refuse it either way
    for f in dataclasses.fields(Decision):
        object.__setattr__(decision, f.name, getattr(base, f.name))
    object.__setattr__(decision, name, value)
    return decision


@pytest.mark.parametrize("name", _TUPLE_FIELDS)
@pytest.mark.parametrize("bad", ["abc", _Str("abc")])
def test_a_str_for_a_tuple_field_is_refused(name, bad):
    ledger = DecisionLedger()
    with pytest.raises(TypeError, match=name):
        ledger.append(_with_field(name, bad))
    assert len(ledger) == 0


@pytest.mark.parametrize("name", _TUPLE_FIELDS)
@pytest.mark.parametrize("good", [["a", "b"], ("a", "b")])
def test_a_list_or_tuple_of_str_is_still_accepted_for_a_tuple_field(name, good):
    ledger = DecisionLedger()
    entry = ledger.append(_with_field(name, good))
    assert getattr(entry.decision, name) == ("a", "b")


# --- Intervention 5: the README's replay rule, pinned (README-only change; these passed before it) ----------------------

def test_replay_identity_is_entity_fingerprint_and_time_only():
    from beta import content_fingerprint

    def edited(**over):
        d = H.decision(0, **over)
        return dataclasses.replace(d, content_fingerprint=content_fingerprint(d))

    ledger = DecisionLedger(require_context=True)
    ledger.append(H.decision(0), context=H.ctx_n(1))
    for n, over in enumerate([{"instructions": "Send home."}, {"reasoning": "other"}, {"confidence": 0.5}], start=2):
        with pytest.raises(Exception, match="replay"):
            ledger.append(edited(**over), context=H.ctx_n(n))
    assert len(ledger) == 1
    ledger.append(H.decision(0, timestamp=H.T0.replace(hour=13)), context=H.ctx_n(9))  # another time: a new Decision
    assert len(ledger) == 2


# --- Regression of Intervention 2 (found by the CNS runner scenario "a ledger rolled back without resetting its head") --
# N9 first derived the receipt's head from the last entry of the snapshot, so the chain check compared the entries with a
# head computed from the entries themselves and an entry removed without resetting chain_head verified. The head the
# ledger holds must be the one checked.

def test_a_receipt_is_refused_when_an_entry_was_removed_and_the_head_was_not_reset_even_for_a_prefix_checkpoint():
    from delta.cns_connector import verification_receipt

    ledger = H.recorded(3)
    older_prefix = LedgerCheckpoint(2, ledger.entries[1].immutable_hash)  # honest checkpoint of the first two entries
    ledger.entries.pop()  # the last entry is gone; chain_head still names it
    assert ledger.chain_head != ledger.entries[-1].immutable_hash
    with pytest.raises(ValueError, match="chain does not verify"):
        verification_receipt(ledger, older_prefix, context=H.ctx_n(1), decision_digest=H.DD, index=0)
