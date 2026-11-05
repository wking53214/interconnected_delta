"""Round 8: reproduced round 7 defects. Each test fails on the code before the intervention that fixes it.

Intervention 1 (F-02): snapshot() and verification_receipt built from one locked snapshot.
"""
import dataclasses
import os
import threading
from collections.abc import Mapping
from datetime import datetime

import pytest

if os.environ.get("DELTA_REQUIRE_CNS") == "1":
    import cns.gate as gate
else:
    gate = pytest.importorskip("cns.gate")

from beta import Decision, content_fingerprint, decision_fingerprint  # noqa: E402

from delta import DecisionLedger  # noqa: E402
from delta.cns_connector import entry_content, verification_receipt  # noqa: E402
from delta.context import CONTEXT_SCHEMA  # noqa: E402

T0 = datetime(2026, 1, 1, 12, 0, 0)
CTX = {"schema": CONTEXT_SCHEMA, "entity_id": "p1", "event_id": "ev-1", "run_id": "run-1"}
DD = gate.subject_digest({"label": "beta decision digest"})


def ctx_n(n):
    return {**CTX, "event_id": f"ev-{n}", "run_id": f"run-{n}"}


def decision(i=0, **over):
    base = dict(entity_id="p1", decision_id="r", decision="ESCALATE", timestamp=T0, confidence=0.9,
                triggered_by_locks=("sepsis_lock",), triggered_by_keys=(f"k{i}",))
    base.update(over)
    base["decision_fingerprint"] = decision_fingerprint({
        "entity_id": base["entity_id"], "decision_id": base["decision_id"], "decision": base["decision"],
        "triggered_by_locks": list(base["triggered_by_locks"]), "triggered_by_keys": list(base["triggered_by_keys"])})
    d = Decision(**base)
    return dataclasses.replace(d, content_fingerprint=content_fingerprint(d))


def recorded(n=1):
    ledger = DecisionLedger(require_context=True)
    for i in range(n):
        ledger.append(decision(i), context=ctx_n(i + 1))
    return ledger


# --- Intervention 1 (F-02): one locked snapshot -----------------------------------------------------------------------

def test_snapshot_is_entries_head_and_length_taken_together():
    ledger = recorded(2)
    entries, head, length = ledger.snapshot()
    assert type(entries) is tuple and entries == tuple(ledger.entries)
    assert (head, length) == (ledger.chain_head, 2)


def test_snapshot_inside_a_sink_does_not_deadlock():
    seen = []
    ledger = DecisionLedger(sink=lambda e: seen.append((ledger.snapshot()[2], ledger.verify_chain_integrity())))
    ledger.append(decision())
    assert seen == [(0, True)]


def test_o1_an_entries_list_that_changes_between_reads_cannot_put_unverified_content_on_a_receipt():
    ledger = recorded()
    cp = ledger.checkpoint()
    honest = list(ledger.entries)
    forged = [dataclasses.replace(honest[0], decision=dataclasses.replace(honest[0].decision, instructions="Send home."))]

    class Shifting(list):
        reads = 0

        def __iter__(self):
            type(self).reads += 1
            return iter(honest if type(self).reads <= 2 else forged)

    ledger.entries = Shifting(honest)
    forged_digest = gate.subject_digest(entry_content(forged[0], context=CTX, decision_digest=DD))
    honest_digest = gate.subject_digest(entry_content(honest[0], context=CTX, decision_digest=DD))
    try:
        receipt = verification_receipt(ledger, cp, context=CTX, decision_digest=DD)
    except ValueError:
        return
    assert receipt["entry_digest"] == honest_digest != forged_digest


def test_o1_an_entries_list_whose_first_read_is_forged_yields_no_receipt():
    ledger = recorded()
    cp = ledger.checkpoint()
    honest = list(ledger.entries)
    forged = [dataclasses.replace(honest[0], decision=dataclasses.replace(honest[0].decision, instructions="Send home."))]

    class ForgedFirst(list):
        def __iter__(self):
            return iter(forged)

    ledger.entries = ForgedFirst(honest)
    with pytest.raises(ValueError, match="does not verify"):
        verification_receipt(ledger, cp, context=CTX, decision_digest=DD)


def test_o3_an_append_during_receipt_issue_cannot_leave_a_head_that_is_not_the_stated_length():
    ledger = recorded(2)
    cp = ledger.checkpoint()
    extra = decision(5)

    class Appending(Mapping):
        done = False

        def __getitem__(self, k):
            return ctx_n(1)[k]

        def __iter__(self):
            if not Appending.done:
                Appending.done = True
                ledger.append(extra, context=ctx_n(3))
            return iter(ctx_n(1))

        def __len__(self):
            return 4

    receipt = verification_receipt(ledger, cp, context=Appending(), decision_digest=DD, index=0)
    assert receipt["ledger_head"] == ledger.entries[receipt["ledger_length"] - 1].immutable_hash
    assert Appending.done and len(ledger) == 3


def test_o3_receipts_issued_while_another_thread_appends_are_always_coherent():
    import sys
    ledger = recorded(1)
    cp = ledger.checkpoint()
    stop = threading.Event()
    old = sys.getswitchinterval()
    sys.setswitchinterval(1e-6)

    def writer():
        i = 2
        while not stop.is_set() and i < 150:
            ledger.append(decision(i, timestamp=datetime(2026, 1, 1, 12, 0, 0)), context=ctx_n(i))
            i += 1

    t = threading.Thread(target=writer)
    t.start()
    bad = 0
    try:
        while t.is_alive():
            r = verification_receipt(ledger, cp, context=CTX, decision_digest=DD, index=0)
            hashes = [e.immutable_hash for e in ledger.entries]
            if hashes[r["ledger_length"] - 1] != r["ledger_head"]:
                bad += 1
    finally:
        stop.set()
        t.join()
        sys.setswitchinterval(old)
    assert bad == 0


def test_o6_an_entry_timestamp_that_is_a_datetime_subclass_fails_verification_and_yields_no_receipt():
    class LyingTime(datetime):
        def isoformat(self, *a, **k):
            return T0.isoformat()

    ledger = recorded()
    cp = ledger.checkpoint()
    ledger.entries[0] = dataclasses.replace(ledger.entries[0], timestamp=LyingTime(1970, 1, 1))
    assert ledger.verify_chain_integrity() is False
    assert ledger.verify_against(cp) is False
    with pytest.raises(ValueError, match="does not verify"):
        verification_receipt(ledger, cp, context=CTX, decision_digest=DD)


# --- Intervention 2 (F-01): exact types for every Decision string field, tuple element and confidence -------------------

class LyingStr(str):
    def __format__(self, spec):
        return "Emergency room now."

    def __str__(self):
        return "Emergency room now."


class LyingFloat(float):
    def __repr__(self):
        return "0.99"

    def __float__(self):
        return 0.99


class LyingInt(int):
    pass


def _state(ledger):
    return (ledger.chain_head, len(ledger.entries), list(ledger.entries))


@pytest.mark.parametrize("name", ["reasoning", "instructions"])
def test_o4_a_str_subclass_in_a_narrative_field_is_refused_and_names_the_field(name):
    ledger = DecisionLedger(require_context=True)
    d = decision()
    d = dataclasses.replace(d, **{name: LyingStr("Send home.")})
    d = dataclasses.replace(d, content_fingerprint=content_fingerprint(d))
    before = _state(ledger)
    with pytest.raises(TypeError, match=name):
        ledger.append(d, context=CTX)
    assert _state(ledger) == before


def test_a_str_subclass_in_a_fingerprint_field_is_refused():
    for name in ("content_fingerprint", "keyset_fingerprint", "decision_fingerprint"):
        ledger = DecisionLedger()
        d = decision()
        value = LyingStr(getattr(d, name) or "0" * 64)
        d = dataclasses.replace(d, **{name: value})
        with pytest.raises(TypeError, match=name):
            ledger.append(d)
        assert len(ledger) == 0


def test_a_str_subclass_confidence_basis_is_refused():
    ledger = DecisionLedger()
    d = dataclasses.replace(decision(), confidence_basis=LyingStr("keys"))
    with pytest.raises(TypeError, match="confidence_basis"):
        ledger.append(d)
    assert len(ledger) == 0


def test_o5_a_float_subclass_confidence_is_refused():
    ledger = DecisionLedger(require_context=True)
    d = dataclasses.replace(decision(), confidence=LyingFloat(0.01))
    d = dataclasses.replace(d, content_fingerprint=content_fingerprint(d))
    before = _state(ledger)
    with pytest.raises(TypeError, match="confidence"):
        ledger.append(d, context=CTX)
    assert _state(ledger) == before


def test_an_int_subclass_and_a_bool_confidence_are_refused_but_an_exact_int_and_float_are_recorded():
    for bad in (LyingInt(1), True):
        ledger = DecisionLedger()
        with pytest.raises((TypeError, ValueError), match="confidence"):
            ledger.append(dataclasses.replace(decision(), confidence=bad))
        assert len(ledger) == 0
    ledger = DecisionLedger()
    ledger.append(dataclasses.replace(decision(0), confidence=1))
    ledger.append(dataclasses.replace(decision(1), confidence=0.5))
    assert len(ledger) == 2


def test_a_str_subclass_tuple_element_in_every_tuple_field_is_refused():
    for name in ("triggered_by_locks", "triggered_by_keys", "newly_triggered_locks", "reversal_conditions"):
        ledger = DecisionLedger()
        d = dataclasses.replace(decision(), **{name: (LyingStr("x"),)})
        with pytest.raises((TypeError, ValueError), match=name):
            ledger.append(d)
        assert len(ledger) == 0


def test_a_stored_decision_edited_to_hold_a_subclass_fails_verification():
    ledger = recorded()
    cp = ledger.checkpoint()
    ledger.entries[0] = dataclasses.replace(ledger.entries[0], decision=dataclasses.replace(
        ledger.entries[0].decision, instructions=LyingStr(ledger.entries[0].decision.instructions)))
    assert ledger.verify_chain_integrity() is False
    assert ledger.verify_against(cp) is False


# --- Intervention 3 (F-12): verification re-judges every stored Decision on a require_context ledger -----------------

def _rechain(ledger):
    """What a writer does: recompute every hash from genesis and reset the head."""
    import hashlib

    from delta.ledger import _GENESIS_SEED, _canonical_subset, _hash_dict
    prev, out = hashlib.sha256(_GENESIS_SEED).hexdigest(), []
    for e in ledger.entries:
        h = hashlib.sha256((prev + _hash_dict(_canonical_subset(
            e.audit_id, e.timestamp, e.decision, e.run_id, e.event_id))).encode()).hexdigest()
        out.append(dataclasses.replace(e, previous_hash=prev, immutable_hash=h))
        prev = h
    ledger.entries[:] = out
    ledger.chain_head = prev


def test_x3_o7_a_confidence_basis_edited_after_recording_fails_verification_and_yields_no_receipt():
    ledger = recorded()
    cp = ledger.checkpoint()
    assert ledger.verify_chain_integrity() is True
    object.__setattr__(ledger.entries[0].decision, "confidence_basis", "absence")
    assert ledger.verify_chain_integrity() is False
    assert ledger.verify_against(cp) is False
    with pytest.raises(ValueError, match="does not verify"):
        verification_receipt(ledger, cp, context=CTX, decision_digest=DD)


def test_an_edited_and_rechained_content_fingerprint_mismatch_fails_on_a_require_context_ledger():
    ledger = recorded(2)
    object.__setattr__(ledger.entries[1].decision, "confidence_basis", "absence")
    object.__setattr__(ledger.entries[1].decision, "triggered_by_keys", ())
    _rechain(ledger)
    assert ledger.verify_chain_integrity() is False


def test_an_edited_decision_fingerprint_rechained_fails_on_a_require_context_ledger():
    ledger = recorded()
    object.__setattr__(ledger.entries[0].decision, "decision_fingerprint", "a" * 64)
    _rechain(ledger)
    assert ledger.verify_chain_integrity() is False


def test_a_ledger_without_require_context_keeps_its_old_verification():
    ledger = DecisionLedger()
    ledger.append(decision())
    object.__setattr__(ledger.entries[0].decision, "confidence_basis", "absence")
    assert ledger.verify_chain_integrity() is True


# --- Intervention 5 (F-15): an interrupt at any line of append leaves head, entries and the sink guard consistent -------

def _executable_lines(fn):
    return sorted({line for _, _, line in fn.__code__.co_lines() if line is not None})


def _recording_functions():
    names = ("append", "_append_locked", "_call_sink", "_heal")
    return [getattr(DecisionLedger, n) for n in names if hasattr(DecisionLedger, n)]


def _consistent(ledger):
    entries = list(ledger.entries)
    want = entries[-1].immutable_hash if entries else DecisionLedger().chain_head
    return ledger.chain_head == want and ledger.verify_chain_integrity()


def _inject(code_to_lines, thunk):
    """Run thunk with a KeyboardInterrupt raised once at the first time any listed (code, line) is reached."""
    fired = []

    def tracer(frame, event, arg):
        lines = code_to_lines.get(frame.f_code)
        if lines is None:
            return None

        def local(frame, event, arg):
            if event == "line" and frame.f_lineno in lines and not fired:
                fired.append(frame.f_lineno)
                raise KeyboardInterrupt(f"injected at {frame.f_lineno}")
            return local
        return local

    sys.settrace(tracer)
    try:
        thunk()
    except KeyboardInterrupt:
        pass
    finally:
        sys.settrace(None)
    return bool(fired)


import sys  # noqa: E402


def _fresh(with_sink):
    seen = []
    ledger = DecisionLedger(require_context=True, sink=(seen.append if with_sink else None))
    ledger.append(decision(0), context=ctx_n(1))
    return ledger, seen


def test_d43_an_interrupt_at_every_line_of_append_leaves_the_ledger_consistent_and_writable():
    checked = 0
    for fn in _recording_functions():
        for line in _executable_lines(fn):
            ledger, seen = _fresh(with_sink=True)
            before = len(ledger)
            fired = _inject({fn.__code__: {line}}, lambda: ledger.append(decision(1), context=ctx_n(2)))
            if not fired:
                continue
            checked += 1
            where = f"{fn.__name__}:{line}"
            assert _consistent(ledger), where
            assert len(ledger) in (before, before + 1), where
            # a later append works (the sink guard is not stuck), whatever happened to the interrupted one
            ledger.append(decision(2), context=ctx_n(3))
            assert _consistent(ledger), where
            assert len(ledger) in (before + 1, before + 2), where
            # a retry of the interrupted Decision is either accepted (it was not recorded) or a replay (it was)
            try:
                ledger.append(decision(1), context=ctx_n(2))
            except ValueError as exc:
                assert "already" in str(exc) or "replay" in str(exc), where
            assert _consistent(ledger), where
    assert checked >= 20


def test_d43_the_sink_guard_still_refuses_a_reentrant_append_after_an_interrupt_in_the_sink_call():
    ledger, seen = _fresh(with_sink=False)
    errors = []
    state = {"n": 0}

    def sink(entry):
        state["n"] += 1
        try:
            ledger.append(decision(9), context=ctx_n(9))
        except RuntimeError as exc:
            errors.append(str(exc))

    ledger._sink = sink
    lines = {fn.__code__: set(_executable_lines(fn)) for fn in _recording_functions() if fn.__name__ == "_call_sink"}
    if lines:
        _inject(lines, lambda: ledger.append(decision(1), context=ctx_n(2)))
    else:
        state["n"] = 0
    ledger.append(decision(3), context=ctx_n(4))
    assert errors and "inside a sink" in errors[0]


def test_d43_a_second_interrupt_inside_the_rollback_cannot_leave_an_entry_the_head_does_not_name():
    ledger, _ = _fresh(with_sink=False)

    class Faulty(list):
        armed = True

        def append(self, item):
            list.append(self, item)
            if Faulty.armed:
                raise KeyboardInterrupt("interrupted after entries.append")

        def __delitem__(self, key):
            if Faulty.armed:
                Faulty.armed = False
                raise KeyboardInterrupt("interrupted again inside the rollback")
            list.__delitem__(self, key)

    ledger.entries = Faulty(ledger.entries)
    before = (len(ledger), ledger.chain_head)
    with pytest.raises(KeyboardInterrupt):
        ledger.append(decision(1), context=ctx_n(2))
    # the next call heals the partial state before anything else, then records
    entry = ledger.append(decision(1), context=ctx_n(2))
    assert len(ledger) == before[0] + 1 and ledger.chain_head == entry.immutable_hash and _consistent(ledger)


def test_d43_an_interrupt_at_the_commit_line_and_again_in_the_rollback_self_heals_on_the_next_append():
    import inspect
    ledger, _ = _fresh(with_sink=False)

    class DelFaults(list):
        armed = True

        def __delitem__(self, key):
            if DelFaults.armed:
                DelFaults.armed = False
                raise KeyboardInterrupt("interrupted again inside the rollback")
            list.__delitem__(self, key)

    ledger.entries = DelFaults(ledger.entries)
    fn = DecisionLedger._append_locked
    src, start = inspect.getsourcelines(fn)
    lines = {start + i for i, text in enumerate(src) if "self.chain_head = immutable_hash" in text}
    assert len(lines) == 1
    assert _inject({fn.__code__: lines}, lambda: ledger.append(decision(1), context=ctx_n(2)))
    assert not DelFaults.armed  # the rollback itself was interrupted
    entry = ledger.append(decision(1), context=ctx_n(2))
    assert len(ledger) == 2 and ledger.chain_head == entry.immutable_hash and _consistent(ledger)


def test_a_raising_sink_leaves_the_guard_clear_and_the_ledger_writable():
    boom = {"on": True}

    def sink(entry):
        if boom["on"]:
            raise OSError("disk full")

    ledger = DecisionLedger(sink=sink)
    with pytest.raises(OSError):
        ledger.append(decision(0))
    boom["on"] = False
    ledger.append(decision(0))
    assert len(ledger) == 1
