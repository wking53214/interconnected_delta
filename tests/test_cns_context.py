"""delta binds a recorded LedgerEntry to the pipeline run it was recorded under and to the Decision it recorded.

The entry digest covered the Decision and its place in the chain but not which run it belonged to, so an entry
could not be tied to the Decision beta produced for that run. With a context and ``decision_digest`` (beta's
digest of the Decision, bound to the run) the content names the run, and the entry must itself have been
recorded under that run.

These skip where ``cns`` is not installed. CI installs the extra and sets DELTA_REQUIRE_CNS=1, so there they cannot skip.
"""
import os
from datetime import datetime

import pytest

if os.environ.get("DELTA_REQUIRE_CNS") == "1":  # CI: a missing cns is a failure, not a skip
    import cns.gate as gate
else:
    gate = pytest.importorskip("cns.gate")

from beta import Decision, decision_fingerprint  # noqa: E402

from delta import DecisionLedger  # noqa: E402
from delta.cns_connector import ENTRY_SUBJECT, binds, entry_content, entry_digest  # noqa: E402
from delta.context import CONTEXT_SCHEMA  # noqa: E402

T0 = datetime(2026, 1, 1, 12, 0, 0)
CTX = {"schema": CONTEXT_SCHEMA, "entity_id": "p1", "event_id": "ev-1", "run_id": "run-1"}


def d(label):
    return gate.subject_digest({"label": label})


def decision(**over):
    base = dict(entity_id="p1", decision_id="r", decision="ESCALATE", timestamp=T0, confidence=0.9,
                triggered_by_locks=("sepsis_lock",), triggered_by_keys=("septic_shock",))
    base.update(over)
    base["decision_fingerprint"] = decision_fingerprint({
        "entity_id": base["entity_id"], "decision_id": base["decision_id"], "decision": base["decision"],
        "triggered_by_locks": list(base["triggered_by_locks"]), "triggered_by_keys": list(base["triggered_by_keys"])})
    return Decision(**base)


def recorded(context=CTX, **over):
    return DecisionLedger().append(decision(**over), context=dict(context) if context else None)


def run(**over):
    base = dict(context=dict(CTX), decision_digest=d("decision"))
    base.update(over)
    return base


def test_without_a_context_the_entry_content_is_unchanged():
    content = entry_content(recorded(context=None))
    assert not {"context", "decision_digest"} & set(content)


def test_with_a_context_the_content_names_the_run_and_the_decision_digest():
    content = entry_content(recorded(), **run())
    assert content["context"] == CTX and content["decision_digest"] == d("decision")


@pytest.mark.parametrize("change", [
    {"decision_digest": d("another decision")},
])
def test_a_different_decision_digest_changes_the_entry_digest(change):
    entry = recorded()
    assert entry_digest(entry, **run()) != entry_digest(entry, **run(**change))
    assert entry_digest(entry, **run()) != entry_digest(entry)


def test_an_entry_recorded_under_another_run_than_the_context_is_refused():
    entry = recorded(context={**CTX, "run_id": "run-2"})
    with pytest.raises(ValueError, match="recorded under run 'run-2' but the context is run 'run-1'"):
        entry_content(entry, **run())


def test_an_entry_recorded_under_another_event_than_the_context_is_refused():
    entry = recorded(context={**CTX, "event_id": "ev-2"})
    with pytest.raises(ValueError, match="recorded under event 'ev-2' but the context is event 'ev-1'"):
        entry_content(entry, **run())


def test_an_entry_recorded_without_a_run_cannot_be_bound_to_one():
    with pytest.raises(ValueError, match="was recorded under no run"):
        entry_content(recorded(context=None), **run())


def test_an_entry_about_another_entity_than_the_context_is_refused():
    entry = recorded(context={**CTX, "entity_id": "p2"}, entity_id="p2")
    with pytest.raises(ValueError, match="entry's Decision is about 'p2' but the context is about 'p1'"):
        entry_content(entry, **run())


def test_a_context_without_its_decision_digest_is_refused_and_so_is_the_reverse():
    with pytest.raises(ValueError, match="a context needs decision_digest"):
        entry_content(recorded(), context=dict(CTX))
    with pytest.raises(ValueError, match="decision_digest needs a context"):
        entry_content(recorded(), decision_digest=d("decision"))


@pytest.mark.parametrize("bad, exc, match", [
    ({k: v for k, v in CTX.items() if k != "run_id"}, ValueError, "exactly"),
    ({**CTX, "run_id": ""}, ValueError, "non-empty"),
    ({**CTX, "run_id": 1}, TypeError, "must be a str"),
    ({**CTX, "schema": "spine-context/3"}, ValueError, "unsupported context schema"),
])
def test_a_malformed_context_is_refused(bad, exc, match):
    with pytest.raises(exc, match=match):
        entry_content(recorded(), **run(context=bad))


@pytest.mark.parametrize("bad, exc", [("A" * 64, ValueError), ("beef", ValueError), (5, TypeError)])
def test_a_malformed_decision_digest_is_refused(bad, exc):
    with pytest.raises(exc):
        entry_content(recorded(), **run(decision_digest=bad))


def test_binds_checks_a_verdict_issued_against_the_run_bound_entry():
    entry = recorded()
    verdict = gate.GateResult(gate="g", position=gate.GatePosition.OMEGA, outcome=gate.GateOutcome.PASS,
                              reason="issued for the test", subject=ENTRY_SUBJECT,
                              subject_digest=entry_digest(entry, **run()))
    assert binds(verdict, entry, **run())
    assert not binds(verdict, entry)
    assert not binds(verdict, entry, **run(decision_digest=d("other")))


def test_a_run_cannot_be_given_for_an_obligation():
    from delta.cns_connector import OBLIGATION_SUBJECT  # noqa: F401
    from delta.obligation import OutcomeObligation

    verdict = gate.GateResult(gate="g", position=gate.GatePosition.OMEGA, outcome=gate.GateOutcome.PASS,
                              reason="x", subject="s", subject_digest="d")
    obligation = object.__new__(OutcomeObligation)
    with pytest.raises(ValueError, match="apply only to a ledger entry"):
        binds(verdict, obligation, **run())


@pytest.mark.parametrize("bad", ["a" * 64 + "\n", "a" * 64 + " ", " " + "a" * 64])
def test_a_decision_digest_with_whitespace_around_it_is_refused(bad):
    """re.match with `$` accepts a trailing newline, so a 65 character string passed as a full SHA-256."""
    with pytest.raises(ValueError, match="full lowercase SHA-256"):
        entry_content(recorded(), **run(decision_digest=bad))
