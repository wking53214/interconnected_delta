"""Round 8, intervention 4 (F-14): export, LedgerCheckpoint.to_dict/from_dict, DecisionLedger.from_export, verify_export.

A consumer in another process needs only the public API: the subprocess test below is the proof. Needs beta only.
"""
import copy
import dataclasses
import json
import subprocess
import sys
from datetime import datetime, timedelta, timezone

import pytest
from beta import Decision, content_fingerprint, decision_fingerprint

import delta
from delta import DecisionLedger, LedgerCheckpoint, verify_export
from delta.context import CONTEXT_SCHEMA

T0 = datetime(2026, 1, 1, 12, 0, 0)


def ctx_n(n, entity="p1"):
    return {"schema": CONTEXT_SCHEMA, "entity_id": entity, "event_id": f"ev-{n}", "run_id": f"run-{n}"}


def decision(i=0, **over):
    base = dict(entity_id="p1", decision_id="r", decision="ESCALATE", timestamp=T0 + timedelta(minutes=i), confidence=0.9,
                triggered_by_locks=("sepsis_lock",), triggered_by_keys=(f"k{i}",), reasoning="because", instructions="act",
                reversal_conditions=("if better",))
    base.update(over)
    base["decision_fingerprint"] = decision_fingerprint({
        "entity_id": base["entity_id"], "decision_id": base["decision_id"], "decision": base["decision"],
        "triggered_by_locks": list(base["triggered_by_locks"]), "triggered_by_keys": list(base["triggered_by_keys"])})
    d = Decision(**base)
    return dataclasses.replace(d, content_fingerprint=content_fingerprint(d))


def ledger_of(n=3, require_context=True):
    ledger = DecisionLedger(require_context=require_context)
    for i in range(n):
        ledger.append(decision(i), context=ctx_n(i + 1))
    return ledger


def exported(n=3):
    ledger = ledger_of(n)
    return ledger, json.loads(json.dumps(ledger.export())), ledger.checkpoint()


def test_round_trip_through_json_text_rebuilds_an_equal_verifying_ledger_that_keeps_recording():
    ledger, data, _ = exported()
    rebuilt = DecisionLedger.from_export(data)
    assert rebuilt.entries == ledger.entries and rebuilt.chain_head == ledger.chain_head
    assert rebuilt.require_context is True and rebuilt.verify_chain_integrity()
    more = decision(7)
    a, b = ledger.append(more, context=ctx_n(9)), rebuilt.append(more, context=ctx_n(9))
    assert a == b and ledger.chain_head == rebuilt.chain_head


def test_round_trip_keeps_aware_times_entry_times_unbound_entries_and_int_confidence():
    ledger = DecisionLedger()
    aware = datetime(2026, 1, 1, 12, 0, 0, 123456, tzinfo=timezone(timedelta(hours=-5)))
    ledger.append(decision(0, timestamp=aware, confidence=1))
    ledger.append(decision(1, timestamp=aware + timedelta(hours=1)), aware + timedelta(hours=2))
    data = json.loads(json.dumps(ledger.export()))
    rebuilt = DecisionLedger.from_export(data)
    assert rebuilt.entries == ledger.entries and rebuilt.chain_head == ledger.chain_head
    assert type(rebuilt.entries[0].decision.confidence) is int and rebuilt.entries[0].run_id is None
    assert DecisionLedger.from_export(DecisionLedger().export()).entries == []


def test_a_checkpoint_round_trips_and_rejects_every_malformed_form():
    cp = ledger_of().checkpoint()
    assert LedgerCheckpoint.from_dict(json.loads(json.dumps(cp.to_dict()))) == cp
    h = cp.head
    for bad in (None, [], {"length": 1}, {"length": 1, "head": h, "x": 1}, {"length": True, "head": h},
                {"length": 1.0, "head": h}, {"length": -1, "head": h}, {"length": 1, "head": h.upper()},
                {"length": 1, "head": h[:10]}, {"length": 1, "head": None}):
        with pytest.raises(ValueError):
            LedgerCheckpoint.from_dict(bad)


def _load(path):
    with open(path) as fh:
        return json.load(fh)


def _dump(data, path):
    with open(path, "w") as fh:
        json.dump(data, fh)


def test_a_consumer_in_another_process_needs_only_the_public_api(tmp_path):
    producer = (
        "import json, sys\n"
        "from datetime import datetime\n"
        "from beta import Decision, content_fingerprint, decision_fingerprint\n"
        "import dataclasses\n"
        "from delta import DecisionLedger\n"
        "L = DecisionLedger(require_context=True)\n"
        "for i in range(3):\n"
        "    fp = decision_fingerprint({'entity_id': 'p1', 'decision_id': 'r', 'decision': 'ESCALATE', 'triggered_by_locks': ['l'], 'triggered_by_keys': ['k%d' % i]})\n"
        "    d = Decision(entity_id='p1', decision_id='r', decision='ESCALATE', timestamp=datetime(2026, 1, 1, 12, i), confidence=0.5,\n"
        "                 triggered_by_locks=('l',), triggered_by_keys=('k%d' % i,), decision_fingerprint=fp)\n"
        "    d = dataclasses.replace(d, content_fingerprint=content_fingerprint(d))\n"
        "    L.append(d, context={'schema': SCHEMA, 'entity_id': 'p1', 'event_id': 'e%d' % i, 'run_id': 'r%d' % i})\n"
        "open(sys.argv[1], 'w').write(json.dumps(L.export()))\n"
        "open(sys.argv[2], 'w').write(json.dumps(L.checkpoint().to_dict()))\n"
    ).replace('SCHEMA', repr(CONTEXT_SCHEMA))
    consumer = (
        "import json, sys\n"
        "import delta\n"
        "from delta import LedgerCheckpoint\n"
        "data = json.load(open(sys.argv[1]))\n"
        "cp = LedgerCheckpoint.from_dict(json.load(open(sys.argv[2])))\n"
        "try:\n"
        "    L = delta.verify_export(data, cp, require_context=True)\n"
        "except ValueError as exc:\n"
        "    print('REFUSED', exc); sys.exit(3)\n"
        "print('VERIFIED', len(L), L.chain_head)\n"
    )
    (tmp_path / "producer.py").write_text(producer)
    (tmp_path / "consumer.py").write_text(consumer)
    exp, cpf = str(tmp_path / "export.json"), str(tmp_path / "cp.json")
    subprocess.run([sys.executable, str(tmp_path / "producer.py"), exp, cpf], check=True)
    ok = subprocess.run([sys.executable, str(tmp_path / "consumer.py"), exp, cpf], capture_output=True, text=True)
    assert ok.returncode == 0 and ok.stdout.startswith("VERIFIED 3 ")
    # the same consumer refuses an edited export and a truncated one (against the held checkpoint)
    data = _load(exp)
    data["entries"][1]["decision"]["instructions"] = "Send home."
    _dump(data, exp)
    bad = subprocess.run([sys.executable, str(tmp_path / "consumer.py"), exp, cpf], capture_output=True, text=True)
    assert bad.returncode == 3 and "entry 1" in bad.stdout
    data = _load(exp)
    data["entries"][1]["decision"]["instructions"] = ""
    _dump(data, exp)
    assert subprocess.run([sys.executable, str(tmp_path / "consumer.py"), exp, cpf]).returncode == 0
    del data["entries"][2]
    data["chain_head"] = data["entries"][1]["immutable_hash"]
    _dump(data, exp)
    cut = subprocess.run([sys.executable, str(tmp_path / "consumer.py"), exp, cpf], capture_output=True, text=True)
    assert cut.returncode == 3 and "truncated" in cut.stdout


# --- tamper cases -----------------------------------------------------------------------------------------------------

def test_an_edited_entry_is_refused_naming_the_entry():
    _, data, _ = exported()
    data["entries"][1]["decision"]["instructions"] = "Send home."
    with pytest.raises(ValueError, match=r"entry 1: "):
        verify_export(data)
    plain = DecisionLedger()
    plain.append(decision(0))
    plain.append(decision(1))
    pdata = json.loads(json.dumps(plain.export()))
    pdata["entries"][1]["decision"]["instructions"] = "Send home."
    with pytest.raises(ValueError, match=r"entry 1: immutable_hash does not match"):
        verify_export(pdata)


@pytest.mark.parametrize("path", [("confidence",), ("reasoning",), ("timestamp",), ("triggered_by_keys",), ("decision",)])
def test_every_hashed_decision_field_edit_is_refused(path):
    _, data, _ = exported()
    d = data["entries"][0]["decision"]
    d[path[0]] = {"confidence": 0.1, "reasoning": "x", "timestamp": "2026-01-02T00:00:00", "triggered_by_keys": ["z"],
                  "decision": "NO_ACTION"}[path[0]]
    with pytest.raises(ValueError):
        verify_export(data)


def test_an_edited_hash_is_refused():
    _, data, _ = exported()
    data["entries"][2]["immutable_hash"] = "0" * 64
    with pytest.raises(ValueError, match="entry 2: immutable_hash"):
        verify_export(data)


def test_a_removed_middle_entry_is_refused():
    _, data, _ = exported()
    del data["entries"][1]
    with pytest.raises(ValueError, match="entry 1: previous_hash"):
        verify_export(data)


def test_a_truncated_export_passes_alone_and_is_refused_against_a_held_checkpoint():
    _, data, cp = exported()
    data["entries"].pop()
    data["chain_head"] = data["entries"][-1]["immutable_hash"]
    assert len(verify_export(data)) == 2  # integrity only, no rollback detection
    with pytest.raises(ValueError, match="truncated"):
        verify_export(data, cp)
    with pytest.raises(ValueError, match="truncated"):
        verify_export(data, cp.to_dict())


def test_a_head_that_is_not_the_last_hash_is_refused():
    _, data, _ = exported()
    data["entries"].pop()
    with pytest.raises(ValueError, match="chain_head"):
        verify_export(data)


def test_a_wrong_checkpoint_is_refused_and_the_right_one_and_an_older_one_pass():
    ledger, data, cp = exported()
    foreign = DecisionLedger(require_context=True)
    foreign.append(decision(0, decision="NO_ACTION"), context=ctx_n(1))
    with pytest.raises(ValueError, match="history the held checkpoint"):
        verify_export(data, LedgerCheckpoint(1, foreign.chain_head))
    with pytest.raises(ValueError, match="history the held checkpoint"):
        verify_export(data, LedgerCheckpoint(3, "a" * 64))
    assert len(verify_export(data, cp)) == 3
    assert len(verify_export(data, LedgerCheckpoint(2, data["entries"][1]["immutable_hash"]))) == 3


def test_an_empty_checkpoint_is_refused():
    _, data, _ = exported()
    genesis = DecisionLedger().chain_head
    with pytest.raises(ValueError, match="history the held checkpoint"):
        verify_export(data, LedgerCheckpoint(0, genesis))
    with pytest.raises(ValueError):
        verify_export(data, {"length": 0})


def test_a_rewritten_and_rechained_export_passes_alone_but_not_against_a_held_checkpoint():
    ledger, data, cp = exported()
    forged = DecisionLedger(require_context=True)
    for i in range(3):
        forged.append(decision(i, instructions="Send home."), context=ctx_n(i + 1))
    fdata = json.loads(json.dumps(forged.export()))
    assert len(verify_export(fdata)) == 3
    with pytest.raises(ValueError, match="history the held checkpoint"):
        verify_export(fdata, cp)


def test_extra_and_missing_fields_are_refused_at_every_level():
    for mutate in (
        lambda d: d.update(extra=1),
        lambda d: d.pop("chain_head"),
        lambda d: d["entries"][0].update(extra=1),
        lambda d: d["entries"][0].pop("run_id"),
        lambda d: d["entries"][0]["decision"].update(extra=1),
        lambda d: d["entries"][0]["decision"].pop("confidence_basis"),
        lambda d: d["entries"][0]["decision"].pop("content_fingerprint"),
    ):
        _, data, _ = exported()
        mutate(data)
        with pytest.raises(ValueError, match="keys"):
            verify_export(data)


def test_a_wrong_schema_and_wrong_types_are_refused():
    for mutate, match in (
        (lambda d: d.update(schema="ledger-export/2"), "schema"),
        (lambda d: d.update(schema=None), "schema"),
        (lambda d: d.update(require_context=1), "require_context"),
        (lambda d: d.update(chain_head=None), "chain_head"),
        (lambda d: d.update(entries=tuple(d["entries"])), "entries"),
        (lambda d: d["entries"][0].update(timestamp=5), "timestamp"),
        (lambda d: d["entries"][0].update(timestamp="2026-01-01 12:00:00"), "isoformat"),
        (lambda d: d["entries"][0]["decision"].update(confidence=True), "confidence"),
        (lambda d: d["entries"][0]["decision"].update(confidence="0.9"), "confidence"),
        (lambda d: d["entries"][0]["decision"].update(triggered_by_keys="k0"), "triggered_by_keys"),
        (lambda d: d["entries"][0]["decision"].update(reasoning=None), "reasoning"),
        (lambda d: d["entries"][0].update(run_id=7), "run_id"),
    ):
        _, data, _ = exported()
        mutate(data)
        with pytest.raises(ValueError, match=match):
            verify_export(data)
    with pytest.raises(ValueError):
        verify_export([])
    with pytest.raises(ValueError):
        verify_export(None)


def test_a_require_context_export_is_rejudged_and_the_flag_can_be_pinned_by_the_verifier():
    ledger, data, _ = exported()
    # a re-chained Decision whose content_fingerprint no longer fits it is refused on a require_context export
    forged = DecisionLedger(require_context=False)
    d = dataclasses.replace(decision(0), instructions="Send home.")  # content_fingerprint left stale
    forged.append(d)
    fdata = forged.export()
    assert len(verify_export(fdata)) == 1
    fdata["require_context"] = True
    with pytest.raises(ValueError, match="content_fingerprint"):
        verify_export(fdata)
    # a flipped flag is caught by a verifier that requires context
    flipped = copy.deepcopy(data)
    flipped["require_context"] = False
    with pytest.raises(ValueError, match="require_context"):
        verify_export(flipped, require_context=True)
    assert len(verify_export(data, require_context=True)) == 3


def test_a_ledger_that_does_not_verify_is_not_exported():
    ledger = ledger_of()
    ledger.entries[0] = dataclasses.replace(ledger.entries[0], audit_id="x" * 16)
    with pytest.raises(ValueError, match="does not verify"):
        ledger.export()


def test_the_public_api_names():
    assert delta.verify_export is verify_export and "verify_export" in delta.__all__
