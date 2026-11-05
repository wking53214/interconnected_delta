"""F11: a ledger entry is never dated before the Decision it records."""

from datetime import datetime, timedelta, timezone

import pytest
from beta import Decision

from delta.ledger import DecisionLedger

T0 = datetime(2026, 1, 1, 12, 0, 0)


def decision():
    return Decision(entity_id="p1", decision_id="d1", decision="ESCALATE", timestamp=T0, confidence=1.0,
                    decision_fingerprint="fp-1")


def test_an_entry_dated_before_its_decision_is_refused_and_changes_nothing():
    ledger = DecisionLedger()
    head = ledger.chain_head
    for earlier in (timedelta(days=30), timedelta(seconds=1)):
        with pytest.raises(ValueError, match="dated before the Decision it records"):
            ledger.append(decision(), T0 - earlier)
    assert len(ledger) == 0 and ledger.chain_head == head


def test_mixing_aware_and_naive_times_is_refused_rather_than_guessed_at():
    ledger = DecisionLedger()
    with pytest.raises(ValueError, match="all timezone-aware or all naive"):
        ledger.append(decision(), datetime(2026, 1, 2, tzinfo=timezone.utc))
    assert len(ledger) == 0


def test_the_same_instant_and_a_later_one_are_fine():
    assert DecisionLedger().append(decision(), T0).timestamp == T0
    assert DecisionLedger().append(decision(), T0 + timedelta(hours=1)).timestamp == T0 + timedelta(hours=1)


def test_an_entry_that_takes_its_decisions_time_is_unchanged():
    assert DecisionLedger().append(decision()).timestamp == T0
