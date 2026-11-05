"""Hardening round 4: context snapshot, stored-copy normalisation, atomic append, sink, checkpoint typing, backdating, replay.

Every case here was a reproduced attack row (context TOCTOU, aliasing, interrupted append, empty and typed-confusion
checkpoints, backdating, replay under a fresh event). Each test names what must now be refused and what must not change.
"""
import dataclasses
import hashlib
from collections.abc import Mapping
from datetime import datetime, timedelta

import pytest
from beta import Decision, decision_fingerprint

from delta import DecisionLedger, DuplicateRecordError, LedgerCheckpoint
from delta.context import CONTEXT_SCHEMA, check_context

T0 = datetime(2026, 1, 1, 12, 0, 0)
GENESIS = hashlib.sha256(b"INTERCONNECTED_DELTA_GENESIS").hexdigest()
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


# --- Step 2: context snapshot ---------------------------------------------------------------------------------------

class Shifty(Mapping):
    """Valid for the first `flip` reads, then serves other values."""

    def __init__(self, good, bad, flip):
        self.good, self.bad, self.flip, self.n = good, bad, flip, 0

    def __getitem__(self, k):
        self.n += 1
        return (self.good if self.n <= self.flip else self.bad)[k]

    def __iter__(self):
        return iter(self.good)

    def __len__(self):
        return len(self.good)


BAD = {"schema": "spine-context/2", "entity_id": "p1", "event_id": 7, "run_id": ""}


@pytest.mark.parametrize("flip", [0, 1, 4, 5, 9, 20])
def test_a_context_that_changes_between_reads_never_records_the_changed_values(flip):
    ledger = DecisionLedger(require_context=True)
    try:
        ledger.append(decision(), context=Shifty(dict(CTX), BAD, flip))
    except (TypeError, ValueError):
        assert len(ledger) == 0
    for e in ledger.entries:
        assert (e.run_id, e.event_id) == ("run-1", "ev-1")
    assert ledger.verify_chain_integrity()


def test_check_context_reads_the_mapping_once_and_returns_the_snapshot():
    shifty = Shifty(dict(CTX), BAD, 4)
    out = check_context(shifty)
    assert out == CTX and type(out) is dict
    out["run_id"] = "x"
    assert check_context(CTX)["run_id"] == "run-1"


@pytest.mark.parametrize("field", ["entity_id", "event_id", "run_id", "schema"])
def test_str_subclass_context_values_are_refused(field):
    class Sneaky(str):
        pass

    bad = {**CTX, field: Sneaky(CTX[field])}
    with pytest.raises(TypeError, match=field):
        check_context(bad)
    ledger = DecisionLedger(require_context=True)
    with pytest.raises(TypeError):
        ledger.append(decision(), context=bad)
    assert len(ledger) == 0


# --- Step 3: stored Decision is a normalised copy -------------------------------------------------------------------

def test_mutating_the_callers_lists_cannot_reach_the_stored_decision():
    ledger = DecisionLedger(require_context=True)
    rc = ["Recheck in 1h"]
    keys = ["k0"]
    d = decision()
    # beta coerces lists to tuples at construction; put the aliases back the way an attacker (or old Decision) could.
    object.__setattr__(d, "reversal_conditions", rc)
    object.__setattr__(d, "triggered_by_keys", keys)
    entry = ledger.append(d, context=CTX)
    held = ledger.checkpoint()
    snapshot = entry.decision
    rc.append("Ignore all alarms")
    keys.append("evil")
    assert ledger.entries[0].decision == snapshot
    assert ledger.entries[0].decision.reversal_conditions == ("Recheck in 1h",)
    assert isinstance(ledger.entries[0].decision.triggered_by_keys, tuple)
    assert ledger.verify_chain_integrity() is True
    assert ledger.verify_against(held) is True


def test_the_returned_entry_decision_is_the_stored_one():
    ledger = DecisionLedger()
    entry = ledger.append(decision())
    assert entry is ledger.entries[0]
    assert entry.decision is ledger.entries[0].decision


# --- Step 4: append is atomic ---------------------------------------------------------------------------------------

class FailingAppend(list):
    """A list whose append fails once, as the interruption in attack row 16a did."""

    armed = True

    def append(self, item):
        if self.armed:
            self.armed = False
            raise KeyboardInterrupt("simulated interruption inside entries.append")
        return super().append(item)


class FailingHead(DecisionLedger):
    """A ledger whose chain_head cannot be set once, after the entry is already in the list."""

    armed = False

    @property
    def chain_head(self):
        return self.__dict__["_head"]

    @chain_head.setter
    def chain_head(self, value):
        if self.armed and value != self.__dict__.get("_head"):
            self.armed = False
            raise KeyboardInterrupt("simulated interruption while updating the head")
        self.__dict__["_head"] = value


def _after_failure_is_unchanged_and_usable(ledger, run, exc):
    before = state(ledger)
    with pytest.raises(exc):
        run()
    assert state(ledger) == before
    assert ledger.verify_chain_integrity() is True
    ledger.append(decision(9), context=ctx(9))
    assert len(ledger) == len(before[2]) + 1 and ledger.verify_chain_integrity() is True


@pytest.mark.parametrize("prior", [0, 2])
def test_an_interruption_inside_entries_append_changes_nothing(prior):
    ledger = DecisionLedger(require_context=True)
    for i in range(prior):
        ledger.append(decision(i), context=ctx(i))
    ledger.entries = FailingAppend(ledger.entries)
    _after_failure_is_unchanged_and_usable(ledger, lambda: ledger.append(decision(5), context=ctx(5)), KeyboardInterrupt)
    # the event whose append failed was never recorded, so it can still be recorded
    ledger.append(decision(5), context=ctx(5))
    assert ledger.verify_chain_integrity()


@pytest.mark.parametrize("prior", [0, 2])
def test_an_interruption_while_updating_the_head_changes_nothing(prior):
    ledger = FailingHead(require_context=True)
    for i in range(prior):
        ledger.append(decision(i), context=ctx(i))
    ledger.armed = True
    _after_failure_is_unchanged_and_usable(ledger, lambda: ledger.append(decision(5), context=ctx(5)), KeyboardInterrupt)


def test_a_failing_hash_step_changes_nothing(monkeypatch):
    import delta.ledger as mod

    ledger = DecisionLedger(require_context=True)
    ledger.append(decision(0), context=ctx(0))
    real = mod._hash_dict
    before = state(ledger)

    def broken(d):
        raise RuntimeError("hash failed")

    monkeypatch.setattr(mod, "_hash_dict", broken)
    with pytest.raises(RuntimeError):
        ledger.append(decision(1), context=ctx(1))
    monkeypatch.setattr(mod, "_hash_dict", real)
    assert state(ledger) == before
    ledger.append(decision(1), context=ctx(1))
    assert len(ledger) == 2 and ledger.verify_chain_integrity()


def test_a_refused_append_changes_nothing_and_the_event_stays_free():
    ledger = DecisionLedger(require_context=True)
    ledger.append(decision(0), context=ctx(0))
    before = state(ledger)
    with pytest.raises(DuplicateRecordError):
        ledger.append(decision(1), context={**ctx(1), "event_id": "ev-0"})
    assert state(ledger) == before
    ledger.append(decision(1), context=ctx(1))
    assert len(ledger) == 2


# --- Step 5: the sink hook ------------------------------------------------------------------------------------------

def test_the_sink_sees_the_exact_entry_that_is_stored_and_sees_it_before_state_changes():
    seen, at_call = [], []
    ledger = DecisionLedger(require_context=True, sink=lambda e: (seen.append(e), at_call.append((len(ledger), ledger.chain_head))))
    entry = ledger.append(decision(0), context=ctx(0))
    assert seen == [entry] and seen[0] is entry and ledger.entries[0] is entry
    assert at_call == [(0, GENESIS)]


def test_a_raising_sink_leaves_the_ledger_unchanged_and_the_next_append_works():
    calls = []

    def sink(entry):
        calls.append(entry)
        if len(calls) == 2:
            raise OSError("disk full")

    ledger = DecisionLedger(require_context=True, sink=sink)
    ledger.append(decision(0), context=ctx(0))
    before = state(ledger)
    with pytest.raises(OSError, match="disk full"):
        ledger.append(decision(1), context=ctx(1))
    assert state(ledger) == before
    again = ledger.append(decision(1), context=ctx(1))
    assert len(ledger) == 2 and ledger.verify_chain_integrity() and calls[-1] is again


def test_the_sink_is_not_called_for_a_refused_append():
    calls = []
    ledger = DecisionLedger(require_context=True, sink=calls.append)
    ledger.append(decision(0), context=ctx(0))
    calls.clear()
    with pytest.raises(DuplicateRecordError):
        ledger.append(decision(1), context={**ctx(1), "event_id": "ev-0"})
    with pytest.raises(Exception):
        ledger.append(decision(1))  # no context on a require_context ledger
    with pytest.raises(TypeError):
        ledger.append(object())
    assert calls == []


def test_a_sink_must_be_callable_and_none_is_the_default():
    with pytest.raises(TypeError):
        DecisionLedger(sink="not callable")
    assert len(DecisionLedger(sink=None).append(decision(0)).audit_id) == 16


# --- Step 6: verify_against takes only a real, typed, non-empty checkpoint -----------------------------------------

class AlwaysEqual:
    def __eq__(self, other):
        return True

    def __ne__(self, other):
        return False

    __hash__ = None


def _grown(n):
    ledger = DecisionLedger(require_context=True)
    for i in range(n):
        ledger.append(decision(i), context=ctx(i))
    return ledger


def _rewritten(ledger):
    """The attack: a shadow ledger holding a different entry, swapped in with its head."""
    shadow = DecisionLedger(require_context=True)
    forged = shadow.append(decision(7, decision="DISCHARGE_SAFE"), context=CTX)
    ledger.entries[:] = [forged]
    ledger.chain_head = forged.immutable_hash
    return ledger


def test_an_empty_checkpoint_carries_no_evidence_even_against_a_rewritten_ledger():  # attack row 11b
    ledger = _rewritten(_grown(1))
    assert ledger.verify_chain_integrity() is True
    assert ledger.verify_against(LedgerCheckpoint(0, GENESIS)) is False
    assert DecisionLedger().verify_against(DecisionLedger().checkpoint()) is False


def test_a_head_object_that_always_compares_equal_is_refused():  # attack row 11c
    ledger = _rewritten(_grown(1))
    assert ledger.verify_against(LedgerCheckpoint(1, AlwaysEqual())) is False  # type: ignore[arg-type]
    assert ledger.verify_against(LedgerCheckpoint(0, AlwaysEqual())) is False  # type: ignore[arg-type]


@pytest.mark.parametrize("length", [True, 1.0, "1", None, -1, 99])
def test_a_length_that_is_not_a_real_in_range_int_is_refused(length):
    ledger = _grown(1)
    assert ledger.verify_against(LedgerCheckpoint(length, ledger.chain_head)) is False  # type: ignore[arg-type]


def test_a_head_that_is_not_an_exact_lowercase_hex_str_is_refused():
    ledger = _grown(1)

    class S(str):
        pass

    head = ledger.chain_head
    for bad in (S(head), head.upper(), head[:63], head + "0", head[:-1] + "g", b"x", None, 5):
        assert ledger.verify_against(LedgerCheckpoint(1, bad)) is False  # type: ignore[arg-type]
    assert ledger.verify_against(LedgerCheckpoint(1, head)) is True


def test_an_honest_checkpoint_still_verifies_after_the_ledger_grows():
    ledger = _grown(2)
    cp = ledger.checkpoint()
    ledger.append(decision(5), context=ctx(5))
    assert ledger.verify_against(cp) is True
    assert ledger.verify_against(ledger.checkpoint()) is True


def test_a_checkpoint_from_another_ledger_is_rejected():
    mine, other = _grown(2), DecisionLedger(require_context=True)
    for i in range(2):
        other.append(decision(i, decision="DISCHARGE_SAFE"), context=ctx(i))
    assert mine.verify_against(other.checkpoint()) is False


def test_rollback_is_rejected_against_a_later_checkpoint():
    ledger = _grown(3)
    later = ledger.checkpoint()
    del ledger.entries[2:]
    ledger.chain_head = ledger.entries[-1].immutable_hash
    assert ledger.verify_chain_integrity() is True
    assert ledger.verify_against(later) is False


# --- Step 7: a supplied entry timestamp may not be earlier than the previous entry's -------------------------------

def test_an_equal_entry_timestamp_is_allowed():
    ledger = DecisionLedger(require_context=True)
    ledger.append(decision(0), context=ctx(0), timestamp=T0)
    ledger.append(decision(1), context=ctx(1), timestamp=T0)
    ledger.append(decision(2), context=ctx(2), timestamp=T0 + timedelta(seconds=1))
    assert len(ledger) == 3 and ledger.verify_chain_integrity()


def test_an_earlier_entry_timestamp_is_refused_and_changes_nothing():
    ledger = DecisionLedger(require_context=True)
    ledger.append(decision(0), context=ctx(0), timestamp=T0)
    before = state(ledger)
    with pytest.raises(ValueError, match="backdated"):
        ledger.append(decision(1), context=ctx(1), timestamp=T0 - timedelta(seconds=1))
    assert state(ledger) == before
    ledger.append(decision(1), context=ctx(1), timestamp=T0)
    assert len(ledger) == 2


def test_mixing_naive_and_aware_entry_timestamps_is_a_value_error_not_a_type_error():
    from datetime import timezone

    ledger = DecisionLedger()
    ledger.append(decision(0), timestamp=T0)
    with pytest.raises(ValueError, match="timezone"):
        ledger.append(decision(1), timestamp=T0.replace(tzinfo=timezone.utc))
    assert len(ledger) == 1


def test_an_entry_without_a_supplied_timestamp_keeps_its_old_behaviour():
    ledger = DecisionLedger()
    ledger.append(decision(0, timestamp=T0 + timedelta(days=1)))
    ledger.append(decision(1, timestamp=T0))
    assert len(ledger) == 2


# --- Step 8: replay of the same Decision under a fresh event or run ------------------------------------------------

def test_the_same_decision_under_a_fresh_event_and_run_is_a_replay_and_changes_nothing():  # attack row 12b
    ledger = DecisionLedger(require_context=True)
    ledger.append(decision(0), context=ctx(0))
    before = state(ledger)
    with pytest.raises(DuplicateRecordError, match="replay"):
        ledger.append(decision(0), context=ctx(1))
    assert state(ledger) == before
    ledger.append(decision(1), context=ctx(1))  # a different decision under that event is fine
    assert len(ledger) == 2


def test_a_replay_is_caught_for_an_equal_copy_not_only_the_same_object():
    ledger = DecisionLedger(require_context=True)
    ledger.append(decision(0), context=ctx(0))
    with pytest.raises(DuplicateRecordError, match="replay"):
        ledger.append(dataclasses.replace(decision(0)), context=ctx(1))


def test_a_different_timestamp_or_entity_or_content_fingerprint_is_not_a_replay():
    ledger = DecisionLedger(require_context=True)
    ledger.append(decision(0), context=ctx(0))
    ledger.append(decision(0, timestamp=T0 + timedelta(hours=1)), context=ctx(1))
    ledger.append(decision(0, entity_id="p2"), context={**ctx(2), "entity_id": "p2"})
    assert len(ledger) == 3


def _with_cf(reasoning):
    from beta import content_fingerprint

    d = decision(0, reasoning=reasoning)
    return dataclasses.replace(d, content_fingerprint=content_fingerprint(d))


def test_content_fingerprint_is_part_of_the_replay_identity_only_when_set():
    ledger = DecisionLedger(require_context=True)
    ledger.append(_with_cf("first"), context=ctx(0))
    ledger.append(_with_cf("second"), context=ctx(1))  # same decision_fingerprint and time, other content
    with pytest.raises(DuplicateRecordError, match="replay"):
        ledger.append(_with_cf("second"), context=ctx(2))
    legacy = DecisionLedger(require_context=True)
    legacy.append(decision(0, reasoning="first"), context=ctx(0))
    with pytest.raises(DuplicateRecordError, match="replay"):
        legacy.append(decision(0, reasoning="second"), context=ctx(1))  # legacy: fingerprint and time are all there is


def test_the_existing_event_and_run_messages_are_unchanged():
    ledger = DecisionLedger(require_context=True)
    ledger.append(decision(0), context=ctx(0))
    with pytest.raises(DuplicateRecordError, match="already recorded a Decision for event"):
        ledger.append(decision(0), context=ctx(0))
    with pytest.raises(DuplicateRecordError, match="an event is recorded once"):
        ledger.append(decision(0), context={**ctx(0), "run_id": "run-9"})
    with pytest.raises(ValueError, match="belongs to event"):
        ledger.append(decision(0), context={**ctx(0), "event_id": "ev-9"})


def test_without_a_context_repeats_are_unchanged():
    ledger = DecisionLedger()
    ledger.append(decision(0))
    ledger.append(decision(0))
    assert len(ledger) == 2


# --- Step 9: content_fingerprint is hashed, present-when-set ------------------------------------------------------

def _legacy():
    return decision(0, reasoning="why")


def _with_cf_of(d):
    from beta import content_fingerprint

    return dataclasses.replace(d, content_fingerprint=content_fingerprint(d))


def test_a_decision_without_a_content_fingerprint_hashes_exactly_as_before():
    # Fixed value computed with the code as it was before this step (commit of Step 1), not with this code.
    entry = DecisionLedger().append(_legacy())
    assert entry.immutable_hash == "136b7c2286898e734035881d9eece8881a6dac9156afb977b103a97ef5c9b8bb"


def test_with_a_content_fingerprint_the_entry_hash_differs_and_follows_its_value():
    plain = DecisionLedger().append(_legacy())
    with_cf = DecisionLedger().append(_with_cf_of(_legacy()))
    assert with_cf.immutable_hash != plain.immutable_hash
    ledger = DecisionLedger()
    ledger.append(_with_cf_of(_legacy()))
    first = ledger.entries[0]
    ledger.entries[0] = dataclasses.replace(first, decision=dataclasses.replace(first.decision, content_fingerprint="d" * 64))
    assert ledger.verify_chain_integrity() is False


def test_a_tampered_content_fingerprint_is_refused_when_recorded_with_a_context():
    ledger = DecisionLedger(require_context=True)
    bad = dataclasses.replace(_with_cf_of(_legacy()), reasoning="edited after the fact")
    before = state(ledger)
    with pytest.raises(ValueError, match="content_fingerprint"):
        ledger.append(bad, context=CTX)
    assert state(ledger) == before
    ledger.append(_with_cf_of(_legacy()), context=CTX)
    assert len(ledger) == 1
