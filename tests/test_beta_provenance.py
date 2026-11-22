"""delta keeps beta's provenance stamp and its two digests.

beta stamps every Decision with how it was produced ("evaluated" when its engine ran the lock evaluation itself,
"supplied-test" when a test fed it lock results) and with digests of the lock results and the prior lock state it
rests on. beta's content_fingerprint covers all three, so a ledger that dropped them on export could not load its own
export back. These tests hold delta to keeping them: through export, through the file store, and in the chain hash.
"""
import json
from datetime import datetime

import pytest
from beta import DecisionEngine, DecisionRule, DecisionRuleRegistry
from store_support import ctx_n, make_decision
from zeta import Combination, Key, KeySet, LockRegistry, LockSpec

from delta import DecisionLedger
from delta.ledger import _decision_to_dict
from delta.store import LEDGER_FILE, FileLedgerStore, StoreCorruptError

T0 = datetime(2026, 1, 1, 12, 0, 0)
PROVENANCE_FIELDS = ("provenance", "lock_results_digest", "prior_state_digest")


def engine_decision(present=True):
    registry = LockRegistry([LockSpec(lock_id="sepsis_lock", required_keys=("septic_shock",),
                                      combination=Combination.OR, dwell_threshold=1)])
    rules = DecisionRuleRegistry([
        DecisionRule(decision_id="escalate", decision="ESCALATE", open_locks=("sepsis_lock",), priority=1,
                     reasoning_template="{decision}"),
        DecisionRule(decision_id="discharge", decision="DISCHARGE", closed_locks=("sepsis_lock",), priority=0,
                     reasoning_template="{decision}"),
    ])
    keys = KeySet([Key(name="septic_shock", present=present, confidence=0.9 if present else 0.0)])
    return DecisionEngine(rules, registry).decide_from_keys("p1", keys, T0)


def recorded(decision):
    ledger = DecisionLedger(require_context=True)
    ledger.append(decision, context=ctx_n(1))
    return ledger


def test_an_engine_decision_carries_the_stamp_and_both_digests():
    d = engine_decision()
    assert d.provenance == "evaluated" and d.lock_results_digest and d.prior_state_digest


def test_an_engine_decision_survives_export_and_reload():
    d = engine_decision()
    exported = recorded(d).export()
    for name in PROVENANCE_FIELDS:
        assert exported["entries"][0]["decision"][name] == getattr(d, name)
    reloaded = DecisionLedger.from_export(exported)
    assert reloaded.entries[0].decision == d
    assert reloaded.verify_chain_integrity()


def test_an_engine_decision_survives_the_file_store(tmp_path):
    d = engine_decision()
    store = FileLedgerStore(tmp_path / "led")
    DecisionLedger(require_context=True, sink=store.sink).append(d, context=ctx_n(1))
    loaded, _ = FileLedgerStore(tmp_path / "led").load(require_context=True)
    assert loaded.entries[0].decision == d


@pytest.mark.parametrize("name, value", [
    ("provenance", ""),
    ("lock_results_digest", "0" * 64),
    ("prior_state_digest", "0" * 64),
])
def test_changing_a_provenance_field_in_an_export_breaks_the_chain(name, value):
    exported = recorded(engine_decision()).export()
    exported["entries"][0]["decision"][name] = value
    with pytest.raises(ValueError):
        DecisionLedger.from_export(exported)


def test_stripping_the_provenance_keys_from_a_stored_line_is_refused(tmp_path):
    # The legacy shape (no provenance keys) is still read, but a Decision that had them set cannot pass as it: the
    # chain hash covers them. The line's checksum is recomputed, so only the chain hash can catch this.
    from delta.store import _canonical, _sum
    store = FileLedgerStore(tmp_path / "led")
    DecisionLedger(sink=store.sink).append(engine_decision(), context=ctx_n(1))
    path = tmp_path / "led" / LEDGER_FILE
    line = json.loads(path.read_bytes())
    for name in PROVENANCE_FIELDS:
        del line["entry"]["decision"][name]
    line["sum"] = _sum(line["entry"])
    path.write_bytes(_canonical(line).encode("ascii") + b"\n")
    with pytest.raises((ValueError, StoreCorruptError)):
        FileLedgerStore(tmp_path / "led").load()


def test_a_decision_without_provenance_hashes_and_exports_as_before():
    d = make_decision(0)
    assert d.provenance == "" and not d.lock_results_digest and not d.prior_state_digest
    exported = recorded(d).export()
    entry = exported["entries"][0]
    for name in PROVENANCE_FIELDS:
        del entry["decision"][name]  # the shape delta wrote before it kept these fields
    reloaded = DecisionLedger.from_export(exported)
    assert reloaded.entries[0].decision == d
    assert reloaded.chain_head == recorded(d).chain_head


def test_the_export_always_writes_all_three_fields():
    for d in (engine_decision(), make_decision(0)):
        assert set(PROVENANCE_FIELDS) <= set(_decision_to_dict(d))


# ------------------------------------------------------------ a supplied-test Decision is never recorded

def supplied_test_decision():
    from zeta import LockEvaluator
    registry = LockRegistry([LockSpec(lock_id="sepsis_lock", required_keys=("septic_shock",),
                                      combination=Combination.OR, dwell_threshold=1)])
    rules = DecisionRuleRegistry([
        DecisionRule(decision_id="escalate", decision="ESCALATE", open_locks=("sepsis_lock",), priority=1,
                     reasoning_template="{decision}"),
        DecisionRule(decision_id="discharge", decision="DISCHARGE", closed_locks=("sepsis_lock",), priority=0,
                     reasoning_template="{decision}"),
    ])
    keys = KeySet([Key(name="septic_shock", present=True, confidence=0.9)])
    results = LockEvaluator(registry).evaluate_all("p1", keys, T0)
    return DecisionEngine(rules, registry).decide("p1", keys, results, T0, test_only=True)


@pytest.mark.parametrize("require_context", [False, True])
def test_append_refuses_a_supplied_test_decision_and_records_nothing(require_context):
    from delta import SuppliedTestDecisionRefused
    d = supplied_test_decision()
    assert d.provenance == "supplied-test"
    calls = []
    ledger = DecisionLedger(require_context=require_context, sink=calls.append)
    head = ledger.chain_head
    with pytest.raises(SuppliedTestDecisionRefused):
        ledger.append(d, context=ctx_n(1))
    assert ledger.entries == [] and ledger.chain_head == head and calls == []


def test_the_file_store_never_sees_a_supplied_test_decision(tmp_path):
    from delta import SuppliedTestDecisionRefused
    store = FileLedgerStore(tmp_path / "led")
    with pytest.raises(SuppliedTestDecisionRefused):
        DecisionLedger(sink=store.sink).append(supplied_test_decision(), context=ctx_n(1))
    path = tmp_path / "led" / LEDGER_FILE
    assert not path.exists() or path.read_bytes() == b""


def test_loading_an_export_that_holds_a_supplied_test_decision_is_refused():
    from delta import SuppliedTestDecisionRefused
    d = supplied_test_decision()
    exported = recorded(engine_decision()).export()
    # Put the supplied-test Decision where the evaluated one was. The refusal comes before any hash is checked.
    exported["entries"][0]["decision"] = _decision_to_dict(d)
    with pytest.raises(SuppliedTestDecisionRefused):
        DecisionLedger.from_export(exported)


def test_evaluated_and_unstamped_decisions_are_still_recorded():
    for d in (engine_decision(), make_decision(0)):
        assert len(recorded(d).entries) == 1
