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
