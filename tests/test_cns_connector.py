"""delta's ledger entries and outcome obligations made bindable to CNS verdicts (binding only, no gate).

These skip where ``cns`` is not installed. CI installs the extra and sets
DELTA_REQUIRE_CNS=1, so there they cannot skip.
"""
import dataclasses
import math
import os
from datetime import datetime, timedelta, timezone

import pytest
from beta import Decision, DecisionEngine, DecisionRule, DecisionRuleRegistry
from zeta import Combination, Key, KeySet, LockEvaluator, LockRegistry, LockSpec

if os.environ.get("DELTA_REQUIRE_CNS") == "1":  # CI: a missing cns is a failure, not a skip
    import cns.gate as gate
else:
    gate = pytest.importorskip("cns.gate")

from delta import DecisionLedger, DecisionObligationTracker, LedgerEntry, MaturationRule, OutcomeObligation  # noqa: E402
from delta.cns_connector import (  # noqa: E402
    ENTRY_SUBJECT, OBLIGATION_SUBJECT, CnsNotInstalled, binds, cns_available, entry_content, entry_digest,
    obligation_content, obligation_digest)
from delta.obligation import OUTCOME_ABANDONED, PROVENANCE_VERIFIED, REASON_DATA_SOURCE_UNREACHABLE  # noqa: E402

T0 = datetime(2026, 1, 1, 12, 0, 0)
DAY = 86400.0


# ------------------------------------------------------------ real decisions, from the real beta engine

def lock_registry():
    return LockRegistry([
        LockSpec(lock_id="sepsis_lock", required_keys=("septic_shock", "respiratory_distress"),
                 combination=Combination.OR, dwell_threshold=1, force=True),
        LockSpec(lock_id="abnormal_vitals_lock", required_keys=("fever", "tachycardia"),
                 combination=Combination.OR, dwell_threshold=1, force=True),
    ])


def rules():
    return DecisionRuleRegistry([
        DecisionRule(decision_id="escalate_sepsis", decision="ESCALATE", open_locks=("sepsis_lock",),
                     priority=10, reversal_conditions=("sepsis_lock closes", "clinician overrides"),
                     instructions="Go to emergency room immediately."),
        DecisionRule(decision_id="hold_abnormal", decision="HOLD", open_locks=("abnormal_vitals_lock",),
                     closed_locks=("sepsis_lock",), priority=5, instructions="Continue monitoring."),
        DecisionRule(decision_id="discharge_safe", decision="DISCHARGE_SAFE",
                     closed_locks=("sepsis_lock", "abnormal_vitals_lock"), priority=0,
                     instructions="Discharge with standard follow-up."),
    ])


def decide(present, entity_id="patient1", when=T0):
    registry = lock_registry()
    keys = KeySet([Key(name=name, present=True, confidence=conf) for name, conf in present])
    results = LockEvaluator(registry).evaluate_all(entity_id, keys, when)
    return DecisionEngine(rules(), registry).decide(entity_id, keys, results, when)


@pytest.fixture
def ledger():
    """A real ledger holding three real decisions."""
    book = DecisionLedger()
    for present in ([("septic_shock", 0.95), ("respiratory_distress", 0.5)], [("fever", 0.8)], []):
        book.append(decide(present))
    return book


@pytest.fixture
def entry(ledger):
    return ledger.entries[0]


def tracker_with_obligation(decision=None):
    decision = decision or decide([("septic_shock", 0.95)])
    tracker = DecisionObligationTracker({"escalate_sepsis": MaturationRule(kind="recovery", horizon_seconds=30 * DAY)})
    opened = tracker.open_for_decision(decision, domain="clinical", opened_at=1000.0)
    return tracker, opened


@pytest.fixture
def opened():
    return tracker_with_obligation()[1]


def result_for(subject, digest):
    return gate.GateResult(gate="test.gate", position=gate.GatePosition.OMEGA, outcome=gate.GateOutcome.PASS,
                           reason="issued for the test", subject=subject, subject_digest=digest)


def only_cns_types(value):
    if value is None or isinstance(value, (str, int, float, bool)):
        return True
    if isinstance(value, list):
        return all(only_cns_types(v) for v in value)
    if isinstance(value, dict):
        return all(isinstance(k, str) and only_cns_types(v) for k, v in value.items())
    return False


# ------------------------------------------------------------ the real ledger, end to end

def test_the_entries_come_from_a_real_ledger_of_real_decisions(ledger):
    assert len(ledger) == 3 and ledger.verify_chain_integrity()
    assert [e.decision.decision for e in ledger.entries] == ["ESCALATE", "HOLD", "DISCHARGE_SAFE"]
    assert all(isinstance(e, LedgerEntry) and e.decision.decision_fingerprint for e in ledger.entries)


def test_each_entry_has_its_own_digest_and_a_verdict_binds_only_its_own(ledger):
    digests = [entry_digest(e) for e in ledger.entries]
    assert len(set(digests)) == 3
    verdict = result_for(ENTRY_SUBJECT, digests[1])
    assert [binds(verdict, e) for e in ledger.entries] == [False, True, False]


def test_the_same_decision_recorded_twice_binds_to_its_place_in_the_chain():
    decision = decide([("septic_shock", 0.95)])
    book = DecisionLedger()
    first, second = book.append(decision), book.append(decision)
    assert first.decision == second.decision
    assert entry_digest(first) != entry_digest(second)           # audit_id and the chain hashes differ
    assert not binds(result_for(ENTRY_SUBJECT, entry_digest(first)), second)


def test_recording_into_a_second_ledger_in_the_same_order_gives_the_same_digests(ledger):
    again = DecisionLedger()
    for present in ([("septic_shock", 0.95), ("respiratory_distress", 0.5)], [("fever", 0.8)], []):
        again.append(decide(present))
    assert [entry_digest(e) for e in again.entries] == [entry_digest(e) for e in ledger.entries]


def test_later_entries_do_not_change_the_digest_of_an_earlier_one():
    book = DecisionLedger()
    first = book.append(decide([("septic_shock", 0.95)]))
    before = entry_digest(first)
    book.append(decide([("fever", 0.8)]))
    assert entry_digest(first) == before and book.verify_chain_integrity()


def test_an_entry_tampered_in_place_no_longer_binds_to_the_verdict_on_the_original(ledger):
    verdict = result_for(ENTRY_SUBJECT, entry_digest(ledger.entries[0]))
    original = ledger.entries[0]
    ledger.entries[0] = dataclasses.replace(original, decision=dataclasses.replace(original.decision, instructions="Wait."))
    assert not ledger.verify_chain_integrity()
    assert not binds(verdict, ledger.entries[0])


def test_computing_a_digest_leaves_the_ledger_alone(ledger):
    head, hashes = ledger.chain_head, [e.immutable_hash for e in ledger.entries]
    for e in ledger.entries:
        entry_digest(e)
        entry_content(e)["decision"]["triggered_by_keys"].append("x")
    assert ledger.chain_head == head and [e.immutable_hash for e in ledger.entries] == hashes
    assert ledger.verify_chain_integrity()


# ------------------------------------------------------------ entry digest

def test_the_entry_digest_is_cns_subject_digest_of_the_content(entry):
    assert entry_digest(entry) == gate.subject_digest(entry_content(entry))
    assert len(entry_digest(entry)) == 64


def test_the_entry_digest_is_deterministic(entry):
    assert entry_digest(entry) == entry_digest(entry)
    assert entry_content(entry) == entry_content(entry)


def test_the_entry_digest_is_not_the_ledger_hash_or_the_fingerprint(entry):
    assert entry_digest(entry) not in (entry.immutable_hash, entry.decision.decision_fingerprint)


ENTRY_CHANGES = {
    "audit_id": "0" * 16,
    "timestamp": T0 + timedelta(minutes=5),
    "previous_hash": "a" * 64,
    "immutable_hash": "b" * 64,
}
DECISION_CHANGES = {
    "entity_id": "patient2",
    "decision_id": "other_rule",
    "decision": "HOLD",
    "timestamp": T0 + timedelta(seconds=1),
    "confidence": 0.5,
    "triggered_by_locks": ("sepsis_lock", "extra_lock"),
    "triggered_by_keys": ("septic_shock", "extra_key"),
    "newly_triggered_locks": ("sepsis_lock", "abnormal_vitals_lock"),
    "decision_fingerprint": "0" * 64,
    "reasoning": "a different explanation",
    "reversal_conditions": ("sepsis_lock closes",),
    "instructions": "Do something else.",
}
DECISION_NOT_BOUND = {"audit_hash"}


def test_every_entry_and_decision_field_is_either_bound_or_deliberately_not():
    assert {f.name for f in dataclasses.fields(LedgerEntry)} == set(ENTRY_CHANGES) | {"decision"}
    assert {f.name for f in dataclasses.fields(Decision)} == set(DECISION_CHANGES) | DECISION_NOT_BOUND
    content = entry_content(LedgerEntry("a", T0, Decision("e", "d", "x", T0, 1.0), "p", "h"))
    assert set(content) == set(ENTRY_CHANGES) | {"decision"}
    assert set(content["decision"]) == set(DECISION_CHANGES)


@pytest.mark.parametrize("name", sorted(ENTRY_CHANGES))
def test_the_entry_digest_changes_when_any_entry_field_changes(entry, name):
    changed = dataclasses.replace(entry, **{name: ENTRY_CHANGES[name]})
    assert changed != entry and entry_digest(changed) != entry_digest(entry)
    assert not binds(result_for(ENTRY_SUBJECT, entry_digest(entry)), changed)


@pytest.mark.parametrize("name", sorted(DECISION_CHANGES))
def test_the_entry_digest_changes_when_any_field_of_the_recorded_decision_changes(entry, name):
    decision = dataclasses.replace(entry.decision, **{name: DECISION_CHANGES[name]})
    changed = dataclasses.replace(entry, decision=decision)
    assert changed != entry and entry_digest(changed) != entry_digest(entry)
    assert not binds(result_for(ENTRY_SUBJECT, entry_digest(entry)), changed)


# ------------------------------------------------------------ what does not matter (entry)

def test_the_order_of_the_decisions_set_like_fields_does_not_change_the_entry_digest(entry):
    a = dataclasses.replace(entry.decision, triggered_by_locks=("b", "a"), triggered_by_keys=("z", "y"),
                            newly_triggered_locks=("q", "p"))
    b = dataclasses.replace(entry.decision, triggered_by_locks=("a", "b"), triggered_by_keys=("y", "z"),
                            newly_triggered_locks=("p", "q"))
    assert entry_digest(dataclasses.replace(entry, decision=a)) == entry_digest(dataclasses.replace(entry, decision=b))
    assert entry_content(dataclasses.replace(entry, decision=a))["decision"]["triggered_by_keys"] == ["y", "z"]


def test_the_order_of_reversal_conditions_is_authored_narrative_and_does_matter(entry):
    swapped = dataclasses.replace(entry.decision, reversal_conditions=tuple(reversed(entry.decision.reversal_conditions)))
    assert len(entry.decision.reversal_conditions) == 2
    assert entry_digest(dataclasses.replace(entry, decision=swapped)) != entry_digest(entry)


def test_the_decisions_audit_hash_does_not_change_the_entry_digest(entry):
    stamped = dataclasses.replace(entry, decision=dataclasses.replace(entry.decision, audit_hash="f" * 64))
    assert entry_digest(stamped) == entry_digest(entry)


def test_an_int_and_a_float_confidence_bind_alike(entry):
    as_int = dataclasses.replace(entry, decision=dataclasses.replace(entry.decision, confidence=1))
    as_float = dataclasses.replace(entry, decision=dataclasses.replace(entry.decision, confidence=1.0))
    assert entry_digest(as_int) == entry_digest(as_float)


def test_the_same_instant_in_two_offsets_is_one_digest_and_naive_differs_from_aware(entry):
    utc = dataclasses.replace(entry, timestamp=datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc))
    plus_one = dataclasses.replace(entry, timestamp=datetime(2026, 1, 1, 13, 0, tzinfo=timezone(timedelta(hours=1))))
    naive = dataclasses.replace(entry, timestamp=datetime(2026, 1, 1, 12, 0))
    assert entry_digest(utc) == entry_digest(plus_one)
    assert entry_digest(naive) != entry_digest(utc)
    decision_utc = dataclasses.replace(entry, decision=dataclasses.replace(
        entry.decision, timestamp=datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)))
    decision_plus = dataclasses.replace(entry, decision=dataclasses.replace(
        entry.decision, timestamp=datetime(2026, 1, 1, 13, 0, tzinfo=timezone(timedelta(hours=1)))))
    assert entry_digest(decision_utc) == entry_digest(decision_plus)


# ------------------------------------------------------------ obligation digest

def test_the_obligation_comes_from_the_real_tracker(opened):
    assert isinstance(opened, OutcomeObligation) and opened.state == "OPEN"
    assert opened.obligation_id.endswith(":recovery") and opened.subject_id == "patient1"


def test_the_obligation_digest_is_cns_subject_digest_of_the_content(opened):
    assert obligation_digest(opened) == gate.subject_digest(obligation_content(opened))
    assert obligation_digest(opened) == obligation_digest(tracker_with_obligation()[1])


def test_the_obligation_digest_follows_the_lifecycle():
    tracker, opened = tracker_with_obligation()
    open_verdict = result_for(OBLIGATION_SUBJECT, obligation_digest(opened))
    restated = tracker.keep_open(opened.obligation_id, REASON_DATA_SOURCE_UNREACHABLE)
    resolved = tracker.resolve_obligation(opened.obligation_id, 1000.0 + DAY, {"recovered": True, "days": 3},
                                          PROVENANCE_VERIFIED, favorable=True)
    dropped = tracker_with_obligation()[0].abandon_obligation(opened.obligation_id, "subject_withdrew", 2000.0)
    assert dropped.state == OUTCOME_ABANDONED
    states = [opened, restated, resolved, dropped]
    assert len({obligation_digest(o) for o in states}) == 4
    assert binds(open_verdict, opened) and not any(binds(open_verdict, o) for o in states[1:])
    assert only_cns_types(obligation_content(resolved)) and obligation_content(resolved)["resolved_value"] == {
        "recovered": True, "days": 3}


OBLIGATION_CHANGES = {
    "obligation_id": "other:recovery",
    "decision_fingerprint": "0" * 64,
    "domain": "other_domain",
    "obligation_kind": "other_kind",
    "opened_at": 1001.0,
    "expected_by": 5000.0,
    "state": OUTCOME_ABANDONED,
    "reason_code": "subject_withdrew",
    "resolved_at": 2000.0,
    "resolved_value": {"recovered": False},
    "resolution_provenance": PROVENANCE_VERIFIED,
    "resolution_method": "chart review",
    "favorable": False,
    "subject_id": "patient2",
    "detail": {"note": "changed"},
}


def test_every_obligation_field_is_bound():
    assert {f.name for f in dataclasses.fields(OutcomeObligation)} == set(OBLIGATION_CHANGES)
    assert set(obligation_content(tracker_with_obligation()[1])) == set(OBLIGATION_CHANGES)


@pytest.mark.parametrize("name", sorted(OBLIGATION_CHANGES))
def test_the_obligation_digest_changes_when_any_field_changes(opened, name):
    changed = dataclasses.replace(opened, **{name: OBLIGATION_CHANGES[name]})
    assert changed != opened and obligation_digest(changed) != obligation_digest(opened)
    assert not binds(result_for(OBLIGATION_SUBJECT, obligation_digest(opened)), changed)


def test_an_unset_value_and_an_empty_one_are_different_content(opened):
    assert obligation_digest(dataclasses.replace(opened, resolved_value=None)) != \
        obligation_digest(dataclasses.replace(opened, resolved_value={}))
    assert obligation_digest(dataclasses.replace(opened, favorable=None)) != \
        obligation_digest(dataclasses.replace(opened, favorable=False))
    assert obligation_digest(dataclasses.replace(opened, resolved_at=None)) != \
        obligation_digest(dataclasses.replace(opened, resolved_at=0.0))


def test_the_order_of_keys_in_the_free_form_mappings_does_not_change_the_digest(opened):
    a = dataclasses.replace(opened, detail={"a": 1, "b": {"x": 1, "y": 2}}, resolved_value={"p": 1, "q": 2})
    b = dataclasses.replace(opened, detail={"b": {"y": 2, "x": 1}, "a": 1}, resolved_value={"q": 2, "p": 1})
    assert obligation_digest(a) == obligation_digest(b)


def test_tuples_and_lists_bind_alike_and_sets_are_sorted(opened):
    # {8, 1} iterates as 8, 1 in CPython, so a set that was not sorted would differ from the list [1, 8]
    as_tuple = dataclasses.replace(opened, detail={"tags": ("a", "b"), "ids": {8, 1}, "frozen": frozenset({8, 1})})
    as_list = dataclasses.replace(opened, detail={"tags": ["a", "b"], "ids": [1, 8], "frozen": [1, 8]})
    assert list({8, 1}) == [8, 1]
    assert obligation_digest(as_tuple) == obligation_digest(as_list)
    assert obligation_content(as_tuple)["detail"] == {"tags": ["a", "b"], "ids": [1, 8], "frozen": [1, 8]}
    assert obligation_digest(dataclasses.replace(opened, detail={"tags": ["b", "a"]})) != \
        obligation_digest(dataclasses.replace(opened, detail={"tags": ["a", "b"]}))      # a list keeps its order


def test_a_set_of_mixed_types_is_still_ordered_deterministically(opened):
    a = dataclasses.replace(opened, detail={"s": {1, "a", None, 2.5}})
    b = dataclasses.replace(opened, detail={"s": {2.5, None, "a", 1}})
    assert obligation_digest(a) == obligation_digest(b)


def test_integer_and_float_times_bind_alike(opened):
    as_int = dataclasses.replace(opened, opened_at=1000, expected_by=1000 + 30 * 86400)
    assert obligation_digest(as_int) == obligation_digest(opened)


def test_the_obligation_content_is_a_copy(opened):
    content = obligation_content(opened)
    content["detail"]["tampered"] = True
    assert "tampered" not in opened.detail and "tampered" not in obligation_content(opened)["detail"]


@pytest.mark.parametrize("field, value", [
    ("detail", {1: "non-string key"}),
    ("detail", {"when": T0}),
    ("detail", {"thing": object()}),
    ("detail", {"nested": [{"bad": b"bytes"}]}),
    ("resolved_value", {"s": [object()]}),
    ("opened_at", "soon"),
    ("expected_by", True),
    ("resolved_at", "later"),
])
def test_a_value_cns_cannot_describe_is_a_type_error_not_a_guess(opened, field, value):
    with pytest.raises(TypeError):
        obligation_content(dataclasses.replace(opened, **{field: value}))


@pytest.mark.parametrize("bad", [math.nan, math.inf])
def test_a_number_that_cannot_be_bound_is_refused_not_hashed(opened, bad):
    with pytest.raises(TypeError):
        obligation_digest(dataclasses.replace(opened, detail={"x": bad}))
    with pytest.raises(TypeError):
        obligation_digest(dataclasses.replace(opened, opened_at=bad))


# ------------------------------------------------------------ content shape

def test_the_entry_content_holds_only_types_cns_accepts(ledger):
    for e in ledger.entries:
        content = entry_content(e)
        assert isinstance(content, dict) and only_cns_types(content)
        assert isinstance(content["decision"], dict) and isinstance(content["decision"]["confidence"], float)
        assert isinstance(content["timestamp"], str) and isinstance(content["decision"]["timestamp"], str)


def test_the_obligation_content_holds_only_types_cns_accepts(opened):
    assert only_cns_types(obligation_content(opened))
    tracker, first = tracker_with_obligation()
    resolved = tracker.resolve_obligation(first.obligation_id, 2000.0, {"v": (1, 2), "w": {"k": None}}, PROVENANCE_VERIFIED)
    assert only_cns_types(obligation_content(resolved))


@pytest.mark.parametrize("bad", [math.nan, math.inf])
def test_a_decision_confidence_that_cannot_be_bound_is_refused(entry, bad):
    broken = dataclasses.replace(entry, decision=dataclasses.replace(entry.decision, confidence=bad))
    with pytest.raises(TypeError):
        entry_digest(broken)


@pytest.mark.parametrize("change", [{"timestamp": "2026-01-01"}, {"timestamp": None}])
def test_an_entry_with_a_malformed_timestamp_is_a_type_error(entry, change):
    with pytest.raises(TypeError):
        entry_content(dataclasses.replace(entry, **change))
    with pytest.raises(TypeError):
        entry_content(dataclasses.replace(entry, decision=dataclasses.replace(entry.decision, **change)))


@pytest.mark.parametrize("bad", ["ESCALATE", None, {"entity_id": "x"}, object()])
def test_an_entry_that_does_not_hold_a_decision_is_a_type_error(entry, bad):
    with pytest.raises(TypeError, match="beta.Decision"):
        entry_content(dataclasses.replace(entry, decision=bad))


# ------------------------------------------------------------ wrong types

@pytest.mark.parametrize("make", [
    lambda: None, lambda: "entry", lambda: 7, lambda: {"audit_id": "x"}, lambda: object(),
    lambda: decide([("septic_shock", 0.95)]),                       # a Decision is not a ledger entry
    lambda: tracker_with_obligation()[1],                           # an obligation is not a ledger entry
    lambda: DecisionLedger(),
])
def test_something_that_is_not_a_ledger_entry_is_a_type_error(make):
    bad = make()
    with pytest.raises(TypeError, match="LedgerEntry"):
        entry_content(bad)
    with pytest.raises(TypeError, match="LedgerEntry"):
        entry_digest(bad)


@pytest.mark.parametrize("make", [
    lambda: None, lambda: "obligation", lambda: 7, lambda: {"obligation_id": "x"}, lambda: object(),
    lambda: decide([("septic_shock", 0.95)]),
    lambda: LedgerEntry("a", T0, decide([]), "p", "h"),             # an entry is not an obligation
    lambda: MaturationRule(kind="k", horizon_seconds=1.0),
])
def test_something_that_is_not_an_obligation_is_a_type_error(make):
    bad = make()
    with pytest.raises(TypeError, match="OutcomeObligation"):
        obligation_content(bad)
    with pytest.raises(TypeError, match="OutcomeObligation"):
        obligation_digest(bad)


@pytest.mark.parametrize("make", [
    lambda: None, lambda: "x", lambda: object(), lambda: decide([]), lambda: DecisionLedger(),
    lambda: MaturationRule(kind="k", horizon_seconds=1.0),
])
def test_binds_refuses_a_record_that_is_neither_an_entry_nor_an_obligation(make):
    with pytest.raises(TypeError, match="LedgerEntry or"):
        binds(result_for(ENTRY_SUBJECT, "x"), make())


@pytest.mark.parametrize("bad", [None, "PASS", {"subject": ENTRY_SUBJECT}, object()])
def test_a_result_that_is_not_a_gate_result_is_a_type_error(entry, opened, bad):
    for record in (entry, opened):
        with pytest.raises(TypeError, match="GateResult"):
            binds(bad, record)


# ------------------------------------------------------------ binds

def test_binds_is_true_for_the_record_the_result_was_issued_against(entry, opened):
    assert binds(result_for(ENTRY_SUBJECT, entry_digest(entry)), entry) is True
    assert binds(result_for(OBLIGATION_SUBJECT, obligation_digest(opened)), opened) is True


def test_binds_is_false_for_a_different_record(ledger, opened):
    assert binds(result_for(ENTRY_SUBJECT, entry_digest(ledger.entries[0])), ledger.entries[1]) is False
    other = dataclasses.replace(opened, domain="elsewhere")
    assert binds(result_for(OBLIGATION_SUBJECT, obligation_digest(opened)), other) is False


def test_binds_is_false_for_an_unbound_or_hand_built_result(entry, opened):
    hand_built = gate.GateResult(gate="x", position=gate.GatePosition.OMEGA, outcome=gate.GateOutcome.PASS)
    for record, digest in ((entry, entry_digest(entry)), (opened, obligation_digest(opened))):
        assert not hand_built.bound() and binds(hand_built, record) is False
        assert binds(result_for("", digest), record) is False
        assert binds(result_for(ENTRY_SUBJECT, ""), record) is False
        assert binds(result_for("", digest), record, subject="") is False   # no subject recorded, even against an empty label
        assert binds(result_for("", ""), record, subject="") is False


def test_binds_checks_the_subject_label(entry, opened):
    for record, digest, default in ((entry, entry_digest(entry), ENTRY_SUBJECT),
                                    (opened, obligation_digest(opened), OBLIGATION_SUBJECT)):
        assert binds(result_for("something.else", digest), record) is False
        assert binds(result_for("something.else", digest), record, subject="something.else") is True
        assert binds(result_for(default, digest), record, subject="something.else") is False


def test_each_record_type_has_its_own_default_label(entry, opened):
    assert binds(result_for(OBLIGATION_SUBJECT, entry_digest(entry)), entry) is False
    assert binds(result_for(ENTRY_SUBJECT, obligation_digest(opened)), opened) is False
    assert ENTRY_SUBJECT != OBLIGATION_SUBJECT


def test_a_verdict_on_an_entry_never_binds_to_an_obligation_or_the_reverse(entry, opened):
    on_entry = result_for(ENTRY_SUBJECT, entry_digest(entry))
    on_obligation = result_for(OBLIGATION_SUBJECT, obligation_digest(opened))
    assert not binds(on_entry, opened) and not binds(on_obligation, entry)


def test_binds_does_not_depend_on_the_outcome(entry):
    digest = entry_digest(entry)
    for outcome in gate.GateOutcome:
        verdict = gate.GateResult(gate="g", position=gate.GatePosition.OMEGA, outcome=outcome,
                                  subject=ENTRY_SUBJECT, subject_digest=digest)
        assert binds(verdict, entry) is True


def test_the_subject_labels_are_stable():
    assert (ENTRY_SUBJECT, OBLIGATION_SUBJECT) == ("delta.ledger_entry", "delta.outcome_obligation")


# ------------------------------------------------------------ availability

def test_cns_is_available_here_and_the_error_type_is_an_import_error():
    assert cns_available() is True
    assert issubclass(CnsNotInstalled, ImportError)
