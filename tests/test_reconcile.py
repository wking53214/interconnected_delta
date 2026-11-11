"""reconcile(): the loaded file against a separately kept checkpoint, one status per outcome.

The cross-process tests run a producer and a consumer as separate Python processes. They need cns for the receipt
(DELTA_REQUIRE_CNS=1 in CI).
"""
import json
import os
import shutil
import subprocess
import sys
import textwrap

import pytest
from store_support import ctx_n, make_decision

if os.environ.get("DELTA_REQUIRE_CNS") == "1":
    import cns.gate  # noqa: F401
else:
    pytest.importorskip("cns.gate")

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from delta import DecisionLedger
from delta.store import (
    CheckpointStore,
    FileLedgerStore,
    StoreCorruptError,
    StoreError,
    reconcile,
)

HERE = os.path.dirname(os.path.abspath(__file__))


def fill(store, n, start=0, ledger=None):
    ledger = ledger or DecisionLedger(require_context=True, sink=store.sink)
    for i in range(start, start + n):
        ledger.append(make_decision(i), context=ctx_n(i + 1))
    return ledger


def lines(store):
    return open(store.path, "rb").read().split(b"\n")[:-1]


def setup(tmp_path, n, witnessed=None):
    store = FileLedgerStore(tmp_path / "ledger")
    cps = CheckpointStore(tmp_path / "custody" / "checkpoint.json")
    ledger = fill(store, witnessed or n)
    cps.write(ledger.checkpoint())
    if witnessed:
        fill(store, n - witnessed, start=witnessed, ledger=ledger)
    return store, cps, ledger


def test_ok(tmp_path):
    store, cps, ledger = setup(tmp_path, 4)
    result = reconcile(store, cps, require_context=True)
    assert result["status"] == "OK" and result["unwitnessed"] == 0
    assert result["ledger_length"] == 4 and result["checkpoint_length"] == 4


def test_unwitnessed_tail_counts_the_entries_after_the_checkpoint(tmp_path):
    store, cps, _ = setup(tmp_path, 5, witnessed=2)
    result = reconcile(store, cps, require_context=True)
    assert result["status"] == "UNWITNESSED_TAIL" and result["unwitnessed"] == 3
    assert result["ledger_length"] == 5 and result["checkpoint_length"] == 2


def test_lost_when_the_file_is_truncated_below_the_checkpoint(tmp_path):
    store, cps, _ = setup(tmp_path, 4)
    kept = lines(store)[:2]
    with open(store.path, "wb") as fh:
        fh.write(b"".join(x + b"\n" for x in kept))
    result = reconcile(store, cps, require_context=True)
    assert result["status"] == "LOST" and result["ledger_length"] == 2 and result["checkpoint_length"] == 4
    assert result["missing"] == 2


def test_lost_when_the_whole_file_is_gone_or_empty(tmp_path):
    store, cps, _ = setup(tmp_path, 3)
    os.remove(store.path)
    assert reconcile(store, cps)["status"] == "LOST"
    open(store.path, "wb").close()
    result = reconcile(store, cps)
    assert result["status"] == "LOST" and result["ledger_length"] == 0


def test_lost_when_the_acknowledged_entry_is_the_one_a_torn_tail_cut_off(tmp_path):
    store, cps, _ = setup(tmp_path, 3)
    original = lines(store)
    with open(store.path, "wb") as fh:
        fh.write(b"".join(x + b"\n" for x in original[:2]) + original[2][:25])
    result = reconcile(store, cps, require_context=True)
    assert result["status"] == "LOST" and result["recovered_tail"]["count"] == 1


def test_diverged_when_an_entry_is_rewritten_and_rechained_at_the_same_length(tmp_path):
    store, cps, _ = setup(tmp_path, 4)
    other = FileLedgerStore(tmp_path / "forged")
    forged = DecisionLedger(require_context=True, sink=other.sink)
    for i in range(4):
        forged.append(make_decision(i if i != 1 else 99), context=ctx_n(i + 1))
    shutil.copy(other.path, store.path)
    assert FileLedgerStore(os.path.dirname(store.path)).load(require_context=True)[0].verify_chain_integrity()
    result = reconcile(store, cps, require_context=True)
    assert result["status"] == "DIVERGED"


def test_diverged_when_the_checkpoint_is_shorter_than_a_rewritten_file(tmp_path):
    store, cps, _ = setup(tmp_path, 5, witnessed=3)
    other = FileLedgerStore(tmp_path / "forged")
    forged = DecisionLedger(require_context=True, sink=other.sink)
    for i in range(5):
        forged.append(make_decision(i if i != 0 else 77), context=ctx_n(i + 1))
    shutil.copy(other.path, store.path)
    assert reconcile(store, cps, require_context=True)["status"] == "DIVERGED"


def test_no_checkpoint(tmp_path):
    store = FileLedgerStore(tmp_path / "ledger")
    fill(store, 2)
    result = reconcile(store, CheckpointStore(tmp_path / "custody" / "checkpoint.json"), require_context=True)
    assert result["status"] == "NO_CHECKPOINT" and result["ledger_length"] == 2


def test_empty(tmp_path):
    result = reconcile(FileLedgerStore(tmp_path / "ledger"), CheckpointStore(tmp_path / "cp.json"))
    assert result["status"] == "EMPTY" and result["ledger_length"] == 0


def test_mid_file_damage_raises_and_is_not_a_status(tmp_path):
    store, cps, _ = setup(tmp_path, 3)
    data = lines(store)
    data[0] = b"garbage"
    with open(store.path, "wb") as fh:
        fh.write(b"".join(x + b"\n" for x in data))
    with pytest.raises(StoreCorruptError, match="line 1"):
        reconcile(store, cps, require_context=True)


def test_a_corrupt_checkpoint_file_raises_rather_than_being_a_status(tmp_path):
    store, cps, _ = setup(tmp_path, 2)
    with open(cps.path, "wb") as fh:
        fh.write(b"garbage")
    with pytest.raises(StoreError):
        reconcile(store, cps, require_context=True)


def test_reconcile_never_writes_or_repairs(tmp_path):
    store, cps, _ = setup(tmp_path, 3)
    with open(store.path, "ab") as fh:
        fh.write(b"torn")
    before = (open(store.path, "rb").read(), open(cps.path, "rb").read(), os.stat(store.path).st_mtime_ns)
    result = reconcile(store, cps, require_context=True)
    assert result["status"] == "OK" and result["recovered_tail"]["count"] == 1
    assert (open(store.path, "rb").read(), open(cps.path, "rb").read(), os.stat(store.path).st_mtime_ns) == before


def test_a_signed_checkpoint_is_verified_through_reconcile(tmp_path):
    key = Ed25519PrivateKey.generate()
    pub = key.public_key()

    def verify(data, signature):
        try:
            pub.verify(signature, data)
        except InvalidSignature:
            return False
        return True

    store = FileLedgerStore(tmp_path / "ledger")
    cps = CheckpointStore(tmp_path / "cp.json")
    cps.write(fill(store, 2).checkpoint(), sign=key.sign)
    assert reconcile(store, cps, require_context=True, verify=verify)["status"] == "OK"
    cps2 = CheckpointStore(tmp_path / "cp2.json")
    cps2.write(fill(FileLedgerStore(tmp_path / "l2"), 2).checkpoint())
    with pytest.raises(StoreError, match="signature"):
        reconcile(FileLedgerStore(tmp_path / "l2"), cps2, require_context=True, verify=verify)


# ---------------------------------------------------------------------------- across processes

PRODUCER = textwrap.dedent("""
    import sys
    sys.path.insert(0, {tests!r})
    from store_support import ctx_n, make_decision
    from delta import DecisionLedger
    from delta.store import CheckpointStore, FileLedgerStore
    ledger_dir, checkpoint_path, n = sys.argv[1], sys.argv[2], int(sys.argv[3])
    store = FileLedgerStore(ledger_dir)
    ledger = DecisionLedger(require_context=True, sink=store.sink)
    for i in range(n):
        ledger.append(make_decision(i), context=ctx_n(i + 1))
    CheckpointStore(checkpoint_path).write(ledger.checkpoint())
""").format(tests=HERE)

CONSUMER = textwrap.dedent("""
    import json, sys
    import cns.gate as gate
    from delta import verify_export
    from delta.context import CONTEXT_SCHEMA
    from delta.cns_connector import verification_receipt
    from delta.store import CheckpointStore, FileLedgerStore, reconcile
    ledger_dir, checkpoint_path = sys.argv[1], sys.argv[2]
    store, cps = FileLedgerStore(ledger_dir), CheckpointStore(checkpoint_path)
    out = {"status": reconcile(store, cps, require_context=True)["status"]}
    ledger, _ = store.load(require_context=True)
    checkpoint = cps.read()
    try:
        verify_export(ledger.export(), checkpoint, require_context=True)
        out["verify_export"] = "ok"
    except ValueError as exc:
        out["verify_export"] = "refused: " + str(exc)[:60]
    ctx = {"schema": CONTEXT_SCHEMA, "entity_id": "p1", "event_id": "ev-1", "run_id": "run-1"}
    try:
        receipt = verification_receipt(ledger, checkpoint, context=ctx, decision_digest=gate.subject_digest({"x": 1}), index=0)
        out["receipt"] = "ok" if receipt["checkpoint_length"] == checkpoint.length else "wrong"
    except ValueError as exc:
        out["receipt"] = "refused: " + str(exc)[:60]
    print(json.dumps(out))
""")


def run(code, *args):
    done = subprocess.run([sys.executable, "-c", code, *map(str, args)], capture_output=True, text=True, timeout=120)
    assert done.returncode == 0, done.stderr
    return done.stdout


def produce(tmp_path, n=4):
    ledger_dir, cp_path = tmp_path / "ledger", tmp_path / "custody" / "checkpoint.json"
    run(PRODUCER, ledger_dir, cp_path, n)
    return ledger_dir, cp_path


def consume(ledger_dir, cp_path):
    return json.loads(run(CONSUMER, ledger_dir, cp_path))


def test_cross_process_restart_reconciles_ok_and_the_loaded_ledger_verifies_and_yields_a_receipt(tmp_path):
    ledger_dir, cp_path = produce(tmp_path)
    assert consume(ledger_dir, cp_path) == {"status": "OK", "verify_export": "ok", "receipt": "ok"}


def test_cross_process_truncated_file_is_lost(tmp_path):
    ledger_dir, cp_path = produce(tmp_path)
    store = FileLedgerStore(ledger_dir)
    kept = lines(store)[:2]
    with open(store.path, "wb") as fh:
        fh.write(b"".join(x + b"\n" for x in kept))
    out = consume(ledger_dir, cp_path)
    assert out["status"] == "LOST"
    assert out["verify_export"].startswith("refused") and out["receipt"].startswith("refused")


def test_cross_process_rewritten_and_rechained_file_with_the_original_checkpoint_is_diverged(tmp_path):
    ledger_dir, cp_path = produce(tmp_path)
    forged_store = FileLedgerStore(tmp_path / "forged")
    forged = DecisionLedger(require_context=True, sink=forged_store.sink)
    for i in range(4):
        forged.append(make_decision(i if i != 2 else 50), context=ctx_n(i + 1))
    shutil.copy(forged_store.path, FileLedgerStore(ledger_dir).path)
    out = consume(ledger_dir, cp_path)
    assert out["status"] == "DIVERGED"
    assert out["verify_export"].startswith("refused") and out["receipt"].startswith("refused")


def test_cross_process_a_replaced_checkpoint_in_the_same_directory_is_NOT_detected(tmp_path):
    """NOT DETECTED, by design: this documents a limit, it is not a guarantee.

    The attacker rewrites and re-chains the ledger file AND replaces the checkpoint with a fresh one taken from the
    tampered ledger, and both live in the same directory (the attacker can write both). Every check then passes:
    reconcile says OK, verify_export accepts the checkpoint and a receipt is issued. delta cannot tell a checkpoint
    the legitimate writer kept from one the attacker made, because it is only as independent as where it is kept.
    Independence (separate storage the ledger's writer cannot rewrite, or a signature by a key the writer does not
    hold) is the deployer's responsibility. The forward-only rule in CheckpointStore.write does not help: it is
    application logic, and an attacker with file access does not call it.
    """
    ledger_dir = tmp_path / "shared"
    cp_path = ledger_dir / "checkpoint.json"
    run(PRODUCER, ledger_dir, cp_path, 4)
    assert consume(ledger_dir, cp_path)["status"] == "OK"
    forged_store = FileLedgerStore(tmp_path / "forged")
    forged = DecisionLedger(require_context=True, sink=forged_store.sink)
    for i in range(4):
        forged.append(make_decision(i if i != 2 else 50), context=ctx_n(i + 1))
    shutil.copy(forged_store.path, FileLedgerStore(ledger_dir).path)
    os.remove(cp_path)  # the attacker does not go through CheckpointStore.write
    CheckpointStore(cp_path).write(forged.checkpoint())
    assert consume(ledger_dir, cp_path) == {"status": "OK", "verify_export": "ok", "receipt": "ok"}
