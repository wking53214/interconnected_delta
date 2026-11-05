"""delta carries the KeySet fingerprint a Decision records, and holds its own copy of beta's projection to beta's.

delta keeps its own copy of how a Decision is described for binding (beta's connector may not be installed when it is
written). When beta added keyset_fingerprint the two copies drifted, and only the integration runner noticed. This holds
them together in delta's own suite: for a Decision with every field set, delta's embedded content equals beta's.
"""
import dataclasses
import os
from datetime import datetime

import pytest
from beta import Decision, decision_fingerprint

from delta import DecisionLedger
from delta.ledger import _canonical_subset

if os.environ.get("DELTA_REQUIRE_CNS") == "1":
    import cns.gate  # noqa: F401
else:
    pytest.importorskip("cns.gate")

from beta.cns_connector import decision_content as beta_content  # noqa: E402

from delta.cns_connector import entry_content  # noqa: E402

T0 = datetime(2026, 1, 1, 12, 0, 0)
FP = "c" * 64


def decision(**over):
    base = dict(entity_id="p1", decision_id="r", decision="ESCALATE", timestamp=T0, confidence=0.9,
                triggered_by_locks=("sepsis_lock",), triggered_by_keys=("septic_shock",), newly_triggered_locks=("sepsis_lock",),
                reasoning="why", reversal_conditions=("closes",), instructions="go", keyset_fingerprint=FP)
    base.update(over)
    base["decision_fingerprint"] = decision_fingerprint({
        "entity_id": base["entity_id"], "decision_id": base["decision_id"], "decision": base["decision"],
        "triggered_by_locks": list(base["triggered_by_locks"]), "triggered_by_keys": list(base["triggered_by_keys"])})
    return Decision(**base)


def test_deltas_embedded_decision_equals_betas_for_every_field_set():
    d = decision()
    entry = DecisionLedger().append(d)
    assert entry_content(entry)["decision"] == beta_content(d)


def test_the_two_copies_agree_without_a_keyset_fingerprint_too():
    d = decision(keyset_fingerprint="")
    assert entry_content(DecisionLedger().append(d))["decision"] == beta_content(d)
    assert "keyset_fingerprint" not in beta_content(d)


def test_the_chain_hash_covers_the_keyset_fingerprint_only_when_there_is_one():
    assert "keyset_fingerprint" in _canonical_subset("a", T0, decision())
    assert "keyset_fingerprint" not in _canonical_subset("a", T0, decision(keyset_fingerprint=""))


def test_changing_the_recorded_keyset_fingerprint_breaks_the_chain():
    ledger = DecisionLedger()
    ledger.append(decision())
    first = ledger.entries[0]
    ledger.entries[0] = dataclasses.replace(first, decision=dataclasses.replace(first.decision, keyset_fingerprint="d" * 64))
    assert not ledger.verify_chain_integrity()


def test_an_entry_recorded_without_one_hashes_as_it_always_did():
    a, b = DecisionLedger(), DecisionLedger()
    ea = a.append(decision(keyset_fingerprint=""))
    legacy = dataclasses.replace(decision(keyset_fingerprint=""))
    assert ea.immutable_hash == b.append(legacy).immutable_hash


def _with_content_fingerprint(**over):
    from beta import content_fingerprint

    d = decision(**over)
    return dataclasses.replace(d, content_fingerprint=content_fingerprint(d))


def test_deltas_embedded_decision_equals_betas_with_a_content_fingerprint_too():
    d = _with_content_fingerprint()
    assert d.content_fingerprint
    entry = DecisionLedger().append(d)
    assert entry_content(entry)["decision"] == beta_content(d)
    assert entry_content(entry)["decision"]["content_fingerprint"] == d.content_fingerprint


def test_the_chain_hash_covers_the_content_fingerprint_only_when_there_is_one():
    assert "content_fingerprint" in _canonical_subset("a", T0, _with_content_fingerprint())
    assert "content_fingerprint" not in _canonical_subset("a", T0, decision())
