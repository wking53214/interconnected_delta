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
