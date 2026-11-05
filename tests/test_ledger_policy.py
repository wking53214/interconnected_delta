"""What the ledger refuses once a Decision is bound to a run: the event policy, stale Decisions, a required context,
and a head that no longer matches the chain.

Found by the Phase 0 rebaseline (executed against the branch): a different Decision for an event already recorded was
accepted (two contradictory verdicts for one event), an older Decision was recorded after a newer one for the same
entity, one run could span events (the duplicate rule then refused a legitimate case for the wrong reason), and an append
onto a chain whose head no longer matched its last entry was accepted.

Policy, stated once: a run belongs to exactly one event; an event has exactly one Decision; within a ledger a Decision is
never older than the newest one recorded for its entity. A caller who needs to decide again uses a new event.
"""
import dataclasses
from datetime import datetime, timedelta, timezone

import pytest
from beta import Decision, content_fingerprint, decision_fingerprint

from delta import (ContextRequiredError, DecisionLedger, DuplicateRecordError, LedgerCorruptError,
                   StaleDecisionError)

T0 = datetime(2026, 1, 1, 12, 0, 0)
CTX = {"schema": "spine-context/1", "entity_id": "p1", "event_id": "ev-1", "run_id": "run-1"}


def ctx(n=1, entity="p1"):
    return {"schema": "spine-context/1", "entity_id": entity, "event_id": f"ev-{n}", "run_id": f"run-{n}"}


def decision(keys=("septic_shock",), when=T0, entity="p1"):
    base = dict(entity_id=entity, decision_id="r", decision="ESCALATE", timestamp=when, confidence=0.9,
                triggered_by_locks=("sepsis_lock",), triggered_by_keys=tuple(keys))
    base["decision_fingerprint"] = decision_fingerprint({
        "entity_id": entity, "decision_id": "r", "decision": "ESCALATE",
        "triggered_by_locks": ["sepsis_lock"], "triggered_by_keys": list(keys)})
    d = Decision(**base)
    return dataclasses.replace(d, content_fingerprint=content_fingerprint(d))


# --- one Decision per event, one event per run ---------------------------------------------------------------------

def test_a_different_decision_for_an_event_already_recorded_is_refused():
    ledger = DecisionLedger()
    ledger.append(decision(), context=ctx(1))
    head, size = ledger.chain_head, len(ledger)
    with pytest.raises(DuplicateRecordError, match="event 'ev-1' already has a Decision recorded under run 'run-1'"):
        ledger.append(decision(keys=("respiratory_distress",)), context={**ctx(1), "run_id": "run-9"})
    assert (ledger.chain_head, len(ledger)) == (head, size)


def test_the_same_run_cannot_span_two_events():
    ledger = DecisionLedger()
    ledger.append(decision(), context=ctx(1))
    with pytest.raises(ValueError, match="run 'run-1' belongs to event 'ev-1', not 'ev-2'"):
        ledger.append(decision(keys=("respiratory_distress",)), context={**ctx(1), "event_id": "ev-2"})


def test_a_second_decision_in_the_same_run_and_event_is_refused_whatever_it_says():
    ledger = DecisionLedger()
    ledger.append(decision(), context=ctx(1))
    with pytest.raises(DuplicateRecordError, match="run 'run-1' already recorded a Decision for event 'ev-1'"):
        ledger.append(decision(keys=("respiratory_distress",)), context=ctx(1))


def test_a_new_event_in_a_new_run_is_recorded():
    ledger = DecisionLedger()
    for n in (1, 2, 3):
        ledger.append(decision(keys=(f"k{n}",), when=T0 + timedelta(minutes=n)), context=ctx(n))
    assert len(ledger) == 3 and ledger.verify_chain_integrity()


# --- never older than the newest Decision for the same entity ------------------------------------------------------

def test_an_older_decision_after_a_newer_one_for_the_same_entity_is_refused():
    ledger = DecisionLedger()
    ledger.append(decision(when=T0 + timedelta(days=30)), context=ctx(1))
    with pytest.raises(StaleDecisionError, match="older than the newest Decision recorded for 'p1'"):
        ledger.append(decision(keys=("k2",), when=T0), context=ctx(2))
    assert len(ledger) == 1


def test_the_same_instant_is_not_older():
    ledger = DecisionLedger()
    ledger.append(decision(), context=ctx(1))
    ledger.append(decision(keys=("k2",)), context=ctx(2))
    assert len(ledger) == 2


def test_another_entity_has_its_own_clock():
    ledger = DecisionLedger()
    ledger.append(decision(when=T0 + timedelta(days=30)), context=ctx(1))
    ledger.append(decision(when=T0, entity="p2"), context=ctx(2, entity="p2"))
    assert len(ledger) == 2


def test_mixing_aware_and_naive_timestamps_is_refused_not_compared():
    ledger = DecisionLedger()
    ledger.append(decision(when=T0), context=ctx(1))
    with pytest.raises(ValueError, match="all timezone-aware or all naive"):
        ledger.append(decision(keys=("k2",), when=datetime(2026, 1, 2, tzinfo=timezone.utc)), context=ctx(2))


def test_the_stale_rule_applies_under_a_context_only_so_unbound_recording_is_as_it_was():
    ledger = DecisionLedger()
    ledger.append(decision(when=T0 + timedelta(days=30)))
    ledger.append(decision(keys=("k2",), when=T0))
    assert len(ledger) == 2


# --- a ledger that requires a context ------------------------------------------------------------------------------

def test_a_ledger_that_requires_a_context_refuses_an_unbound_append_and_records_a_bound_one():
    ledger = DecisionLedger(require_context=True)
    with pytest.raises(ContextRequiredError, match="requires a context"):
        ledger.append(decision())
    assert len(ledger) == 0
    ledger.append(decision(), context=ctx(1))
    assert len(ledger) == 1


def test_a_ledger_that_does_not_require_one_still_records_unbound_as_before():
    assert len(_one(DecisionLedger())) == 1


def _one(ledger):
    ledger.append(decision())
    return ledger


# --- appending onto a chain whose head no longer matches --------------------------------------------------------------

def test_an_append_onto_a_head_that_does_not_match_the_last_entry_is_refused():
    ledger = DecisionLedger()
    ledger.append(decision(), context=ctx(1))
    ledger.entries.pop()  # the entry is gone, the head still names it
    with pytest.raises(LedgerCorruptError, match="head does not match"):
        ledger.append(decision(keys=("k2",)), context=ctx(2))


def test_a_tampered_head_on_an_empty_ledger_is_refused():
    ledger = DecisionLedger()
    ledger.chain_head = "0" * 64
    with pytest.raises(LedgerCorruptError, match="head does not match"):
        ledger.append(decision())


def test_an_intact_ledger_keeps_appending_and_a_refusal_changes_nothing():
    ledger = DecisionLedger()
    ledger.append(decision(), context=ctx(1))
    with pytest.raises(DuplicateRecordError):
        ledger.append(decision(keys=("k2",)), context=ctx(1))
    ledger.append(decision(keys=("k3",), when=T0 + timedelta(minutes=1)), context=ctx(3))
    assert len(ledger) == 2 and ledger.verify_chain_integrity()


def test_the_documented_limit_holds_a_consistent_rewrite_still_passes_the_head_check():
    """The head check is O(1): it catches a head that disagrees with the last entry, not a chain someone rewrote and
    re-headed. That is what a checkpoint held outside the ledger is for (see test_ledger_checkpoint.py)."""
    ledger = DecisionLedger()
    ledger.append(decision(), context=ctx(1))
    ledger.append(decision(keys=("k2",), when=T0 + timedelta(minutes=1)), context=ctx(2))
    ledger.entries.pop()
    ledger.chain_head = ledger.entries[-1].immutable_hash
    ledger.append(decision(keys=("k3",), when=T0 + timedelta(minutes=2)), context=ctx(3))
    assert ledger.verify_chain_integrity() and len(ledger) == 2


def test_errors_are_value_errors_so_a_caller_catching_the_old_ones_still_refuses():
    for exc in (DuplicateRecordError, StaleDecisionError, ContextRequiredError, LedgerCorruptError):
        assert issubclass(exc, ValueError)
    assert dataclasses.is_dataclass(Decision)
