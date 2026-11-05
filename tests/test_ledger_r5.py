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
