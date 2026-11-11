"""Shared helpers for the store tests (and the child processes they start)."""
import dataclasses
from datetime import datetime

from beta import Decision, content_fingerprint, decision_fingerprint

from delta.context import CONTEXT_SCHEMA

T0 = datetime(2026, 1, 1, 12, 0, 0)


def make_decision(i=0, entity="p1"):
    base = {"entity_id": entity, "decision_id": "r", "decision": "ESCALATE", "timestamp": T0, "confidence": 0.9,
            "triggered_by_locks": ("sepsis_lock",), "triggered_by_keys": (f"k{i}",)}
    base["decision_fingerprint"] = decision_fingerprint({
        "entity_id": entity, "decision_id": "r", "decision": "ESCALATE",
        "triggered_by_locks": ["sepsis_lock"], "triggered_by_keys": [f"k{i}"]})
    d = Decision(**base)
    return dataclasses.replace(d, content_fingerprint=content_fingerprint(d))


def ctx_n(n, entity="p1"):
    return {"schema": CONTEXT_SCHEMA, "entity_id": entity, "event_id": f"ev-{n}", "run_id": f"run-{n}"}
