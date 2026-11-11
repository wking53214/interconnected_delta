"""Fixes for the independent review of the store and signing code (one section per finding)."""
import json
import os

import pytest
from store_support import ctx_n, make_decision

from delta import DecisionLedger
from delta.store import FileLedgerStore, StoreCorruptError, StoreError, _canonical, _sum


def fill(store, n, start=0):
    ledger = DecisionLedger(require_context=True, sink=store.sink)
    for i in range(start, start + n):
        ledger.append(make_decision(i), context=ctx_n(i + 1))
    return ledger


# ---------------------------------------------------------------- F3: the sink never builds on a line load would refuse
def test_f3_a_duplicated_final_line_is_refused_at_append_time(tmp_path):
    store = FileLedgerStore(tmp_path)
    ledger = fill(store, 2)
    data = open(store.path, "rb").read()
    last = data.split(b"\n")[-2] + b"\n"
    with open(store.path, "ab") as fh:
        fh.write(last)
    before = open(store.path, "rb").read()
    fresh = DecisionLedger.from_export(ledger.export())
    fresh._sink = store.sink
    with pytest.raises(StoreError, match="chain"):
        fresh.append(make_decision(2), context=ctx_n(3))
    assert open(store.path, "rb").read() == before
    assert len(fresh) == 2


def test_f3_a_final_line_whose_hash_was_edited_with_a_fixed_sum_is_refused_at_append_time(tmp_path):
    store = FileLedgerStore(tmp_path)
    ledger = fill(store, 2)
    lines = open(store.path, "rb").read().split(b"\n")[:-1]
    obj = json.loads(lines[-1])
    obj["entry"]["immutable_hash"] = "0" * 64
    obj["sum"] = _sum(obj["entry"])
    lines[-1] = _canonical(obj).encode()
    with open(store.path, "wb") as fh:
        fh.write(b"\n".join(lines) + b"\n")
    with pytest.raises(StoreError):
        ledger.append(make_decision(2), context=ctx_n(3))


# ---------------------------------------------------------------- F4: a malformed final line is StoreError, not KeyError
def test_f4_an_empty_entry_on_the_final_line_is_a_store_error(tmp_path):
    store = FileLedgerStore(tmp_path)
    ledger = fill(store, 2)
    with open(store.path, "ab") as fh:
        fh.write(_canonical({"v": 1, "entry": {}, "sum": _sum({})}).encode() + b"\n")
    before = open(store.path, "rb").read()
    with pytest.raises(StoreError):
        ledger.append(make_decision(2), context=ctx_n(3))
    assert open(store.path, "rb").read() == before
    with pytest.raises(StoreCorruptError):
        store.load(True)


# ---------------------------------------------------------------- F2: the retry also syncs the directory
def test_f2_a_retry_after_a_failed_directory_fsync_is_not_acknowledged_until_the_directory_syncs(tmp_path, monkeypatch):
    import delta.store as st
    calls = {"n": 0, "ok": 0}
    real = st._fsync_dir

    def flaky(directory):
        calls["n"] += 1
        if calls["n"] == 1:
            raise OSError(5, "EIO on directory fsync")
        calls["ok"] += 1
        real(directory)

    monkeypatch.setattr(st, "_fsync_dir", flaky)
    store = FileLedgerStore(tmp_path / "led")
    ledger = DecisionLedger(require_context=True, sink=store.sink)
    with pytest.raises(OSError):
        ledger.append(make_decision(0), context=ctx_n(1))
    assert len(ledger) == 0
    ledger.append(make_decision(0), context=ctx_n(1))
    assert len(ledger) == 1
    assert calls["ok"] == 1


def test_f2_a_retry_whose_directory_fsync_fails_again_is_not_acknowledged(tmp_path, monkeypatch):
    import delta.store as st

    def always(directory):
        raise OSError(5, "EIO on directory fsync")

    monkeypatch.setattr(st, "_fsync_dir", always)
    store = FileLedgerStore(tmp_path / "led")
    ledger = DecisionLedger(require_context=True, sink=store.sink)
    for _ in range(2):
        with pytest.raises(OSError):
            ledger.append(make_decision(0), context=ctx_n(1))
    assert len(ledger) == 0


# ---------------------------------------------------------------- F10: a directory the store creates is made durable in its parent
def test_f10_creating_the_ledger_directory_fsyncs_its_parent(tmp_path, monkeypatch):
    import delta.store as st
    synced = []
    real = st._fsync_dir
    monkeypatch.setattr(st, "_fsync_dir", lambda d: (synced.append(os.path.abspath(d)), real(d))[1])
    store = FileLedgerStore(tmp_path / "a" / "led")
    DecisionLedger(require_context=True, sink=store.sink).append(make_decision(0), context=ctx_n(1))
    assert str(tmp_path) in synced  # the parent of the first created level
    assert str(tmp_path / "a") in synced  # the parent of the ledger directory
    assert str(tmp_path / "a" / "led") in synced


def test_f10_creating_the_checkpoint_directory_fsyncs_its_parent(tmp_path, monkeypatch):
    import delta.store as st
    from delta import LedgerCheckpoint
    synced = []
    real = st._fsync_dir
    monkeypatch.setattr(st, "_fsync_dir", lambda d: (synced.append(os.path.abspath(d)), real(d))[1])
    cp = st.CheckpointStore(tmp_path / "a" / "cp" / "checkpoint.json")
    cp.write(LedgerCheckpoint(1, "0" * 64))
    assert str(tmp_path) in synced
    assert str(tmp_path / "a") in synced
    assert str(tmp_path / "a" / "cp") in synced


# ---------------------------------------------------------------- F6: the checkpoint temp file is unpredictable and private
def test_f6_a_symlink_planted_at_the_old_predictable_temp_name_is_not_followed(tmp_path):
    import hashlib
    import stat
    import threading
    from delta import LedgerCheckpoint
    from delta.store import CheckpointStore
    victim = tmp_path / "victim.txt"
    victim.write_text("IMPORTANT DATA\n")
    store = CheckpointStore(tmp_path / "checkpoint.json")
    os.symlink(victim, f"{store.path}.tmp-{os.getpid()}-{threading.get_ident()}")
    store.write(LedgerCheckpoint(1, hashlib.sha256(b"a").hexdigest()))
    assert victim.read_text() == "IMPORTANT DATA\n"
    assert not os.path.islink(store.path)
    assert stat.S_IMODE(os.stat(store.path).st_mode) == 0o600
    assert store.read().length == 1


def test_f6_the_temp_file_comes_from_mkstemp_in_the_same_directory(tmp_path, monkeypatch):
    import hashlib
    import tempfile
    from delta import LedgerCheckpoint
    from delta.store import CheckpointStore
    seen = []
    real = tempfile.mkstemp
    monkeypatch.setattr(tempfile, "mkstemp", lambda *a, **k: (seen.append(k.get("dir")), real(*a, **k))[1])
    store = CheckpointStore(tmp_path / "sub" / "checkpoint.json")
    store.write(LedgerCheckpoint(1, hashlib.sha256(b"a").hexdigest()))
    assert seen == [str(tmp_path / "sub")]
    assert sorted(os.listdir(tmp_path / "sub")) == ["checkpoint.json", "checkpoint.json.lock"]  # no temp file is left


# ---------------------------------------------------------------- F7: forward-only holds across store objects
def test_f7_two_store_objects_cannot_move_the_checkpoint_backwards(tmp_path, monkeypatch):
    import hashlib
    import threading
    import time
    import delta.store as st
    from delta import LedgerCheckpoint
    H = lambda c: hashlib.sha256(c.encode()).hexdigest()  # noqa: E731
    path = tmp_path / "checkpoint.json"
    a, b = st.CheckpointStore(path), st.CheckpointStore(path)
    a.write(LedgerCheckpoint(5, H("h5")))
    real = st._write_all
    errors = []
    threads = []

    def run_b():
        try:
            b.write(LedgerCheckpoint(10, H("h10")))
        except Exception as exc:  # pragma: no cover - reported below
            errors.append(exc)

    def interleave(fd, data):
        monkeypatch.setattr(st, "_write_all", real)
        if b'"length":6' in data:  # a is between its forward-only check and its replace
            thread = threading.Thread(target=run_b)
            threads.append(thread)
            thread.start()
            time.sleep(0.3)  # b gets to run now unless a holds the lock
        real(fd, data)

    monkeypatch.setattr(st, "_write_all", interleave)
    a.write(LedgerCheckpoint(6, H("h6")))
    for thread in threads:
        thread.join()
    assert errors == []
    assert a.read().length == 10


def test_f7_a_lower_write_from_a_second_object_is_refused(tmp_path):
    import hashlib
    from delta import LedgerCheckpoint
    path = tmp_path / "checkpoint.json"
    a, b = st_checkpoint(path), st_checkpoint(path)
    b.write(LedgerCheckpoint(10, hashlib.sha256(b"h10").hexdigest()))
    with pytest.raises(StoreError, match="lower"):
        a.write(LedgerCheckpoint(6, hashlib.sha256(b"h6").hexdigest()))
    assert a.read().length == 10


def st_checkpoint(path):
    from delta.store import CheckpointStore
    return CheckpointStore(path)


def test_f7_the_lock_is_held_from_the_check_through_the_directory_fsync(tmp_path, monkeypatch):
    import fcntl
    import hashlib
    import delta.store as st
    from delta import LedgerCheckpoint
    path = tmp_path / "checkpoint.json"
    store = st.CheckpointStore(path)
    store.write(LedgerCheckpoint(1, hashlib.sha256(b"h1").hexdigest()))
    states = []
    real = st._fsync_dir

    def probe(directory):
        fd = os.open(str(path) + ".lock", os.O_RDWR)
        try:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                states.append("free")
            except BlockingIOError:
                states.append("held")
        finally:
            os.close(fd)
        real(directory)

    monkeypatch.setattr(st, "_fsync_dir", probe)
    store.write(LedgerCheckpoint(2, hashlib.sha256(b"h2").hexdigest()))
    assert states == ["held"]
    fd = os.open(str(path) + ".lock", os.O_RDWR)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)  # released after the write
    finally:
        os.close(fd)


# ---------------------------------------------------------------- F8: an existing checkpoint that fails verify is corrupt, not a floor
def _signer():
    import hashlib
    return (lambda b: hashlib.sha256(b"K" + b).digest(), lambda d, s: s == hashlib.sha256(b"K" + d).digest())


def test_f8_a_planted_unsigned_huge_checkpoint_is_corrupt_not_a_floor(tmp_path):
    import hashlib
    from delta import LedgerCheckpoint
    from delta.store import CheckpointStore
    sign, verify = _signer()
    path = tmp_path / "checkpoint.json"
    path.write_text(json.dumps({"v": 1, "length": 10**9, "head": hashlib.sha256(b"x").hexdigest()}))
    store = CheckpointStore(path)
    real = LedgerCheckpoint(3, hashlib.sha256(b"real").hexdigest())
    before = path.read_bytes()
    with pytest.raises(StoreCorruptError):
        store.write(real, sign=sign, verify=verify)
    assert path.read_bytes() == before
    store.write(real, sign=sign, verify=verify, replace_corrupt=True)
    assert store.read(verify=verify) == real


def test_f8_a_bad_signature_on_the_existing_file_is_corrupt(tmp_path):
    import hashlib
    from delta import LedgerCheckpoint
    from delta.store import CheckpointStore
    sign, verify = _signer()
    store = CheckpointStore(tmp_path / "checkpoint.json")
    store.write(LedgerCheckpoint(2, hashlib.sha256(b"h2").hexdigest()), sign=lambda b: b"forged")
    with pytest.raises(StoreCorruptError):
        store.write(LedgerCheckpoint(3, hashlib.sha256(b"h3").hexdigest()), sign=sign, verify=verify)


def test_f8_a_verified_existing_file_still_gates_forward_only(tmp_path):
    import hashlib
    from delta import LedgerCheckpoint
    from delta.store import CheckpointStore
    sign, verify = _signer()
    store = CheckpointStore(tmp_path / "checkpoint.json")
    store.write(LedgerCheckpoint(5, hashlib.sha256(b"h5").hexdigest()), sign=sign, verify=verify)
    with pytest.raises(StoreError, match="lower"):
        store.write(LedgerCheckpoint(4, hashlib.sha256(b"h4").hexdigest()), sign=sign, verify=verify)
    store.write(LedgerCheckpoint(6, hashlib.sha256(b"h6").hexdigest()), sign=sign, verify=verify)
    assert store.read(verify=verify).length == 6


# ---------------------------------------------------------------- F5: signing inputs refuse strings that are not strict UTF-8
def _receipt(run_id="run"):
    return {"schema": "s", "entry_digest": "d", "index": 0, "entry_hash": "h", "previous_hash": "p",
            "ledger_length": 1, "ledger_head": "lh", "checkpoint_length": 1, "checkpoint_head": "ch",
            "context": {"schema": "c", "entity_id": "e", "event_id": "ev", "run_id": run_id}}


def test_f5_a_surrogate_pair_and_the_astral_character_no_longer_share_a_signing_input():
    from delta.signing import receipt_signing_input
    astral = receipt_signing_input(_receipt("\U0001F600"))  # a real character still signs
    assert astral.endswith(b"}")
    with pytest.raises(ValueError, match="context.run_id"):
        receipt_signing_input(_receipt(chr(0xD83D) + chr(0xDE00)))


@pytest.mark.parametrize("field", ["schema", "entry_digest", "entry_hash", "previous_hash", "ledger_head", "checkpoint_head"])
def test_f5_a_lone_surrogate_in_any_top_level_string_is_refused_naming_the_field(field):
    from delta.signing import receipt_signing_input
    receipt = _receipt()
    receipt[field] = "a" + chr(0xD800)
    with pytest.raises(ValueError, match=field):
        receipt_signing_input(receipt)


@pytest.mark.parametrize("field", ["schema", "entity_id", "event_id", "run_id"])
def test_f5_a_lone_surrogate_in_any_context_string_is_refused_naming_the_field(field):
    from delta.signing import receipt_signing_input
    receipt = _receipt()
    receipt["context"][field] = chr(0xDC00)
    with pytest.raises(ValueError, match=f"context.{field}"):
        receipt_signing_input(receipt)


def test_f5_verify_receipt_signature_is_false_for_such_a_receipt():
    from delta.signing import verify_receipt_signature
    assert verify_receipt_signature(_receipt(chr(0xD800)), b"sig", lambda d, s: True) is False


def test_f5_the_checkpoint_signing_input_refuses_a_non_utf8_head():
    from delta import LedgerCheckpoint
    from delta.store import CheckpointStore
    with pytest.raises(ValueError, match="head"):
        CheckpointStore._signing_input(LedgerCheckpoint(1, chr(0xD800)))


# ---------------------------------------------------------------- F1: audit_hash is bound into the entry hash when it is set
def _rechain(lines, edited_index):
    """Rewrite stored lines so every hash from ``edited_index`` on is recomputed over the (edited) content."""
    import hashlib
    from delta.ledger import _canonical_subset, _entry_from_dict, _hash_dict, _GENESIS_SEED
    previous = hashlib.sha256(_GENESIS_SEED).hexdigest()
    out = []
    for i, raw in enumerate(lines):
        obj = json.loads(raw)
        if i >= edited_index:
            obj["entry"]["previous_hash"] = previous
            entry = _entry_from_dict(i, obj["entry"], False)
            obj["entry"]["immutable_hash"] = hashlib.sha256(
                (previous + _hash_dict(_canonical_subset(
                    entry.audit_id, entry.timestamp, entry.decision, entry.run_id, entry.event_id))).encode()
            ).hexdigest()
            obj["sum"] = _sum(obj["entry"])
        previous = obj["entry"]["immutable_hash"]
        out.append(_canonical(obj).encode())
    return out


def test_f1_an_entry_without_an_audit_hash_hashes_exactly_as_before():
    import hashlib
    from datetime import datetime
    from delta.ledger import _GENESIS_SEED, _canonical_subset, _hash_dict
    previous = hashlib.sha256(_GENESIS_SEED).hexdigest()
    subset = _canonical_subset("aid-0001", datetime(2026, 1, 1, 12, 0, 5), make_decision(0), "run-1", "ev-1")
    assert "audit_hash" not in subset
    got = hashlib.sha256((previous + _hash_dict(subset)).encode()).hexdigest()
    assert got == "dc7dbb986127631ee5469f02596669092b624c854a61ace55cc6edc8c3fc39a8"  # computed before this change


def test_f1_a_set_audit_hash_is_hashed_and_changes_the_subset():
    import dataclasses
    from datetime import datetime
    from delta.ledger import _canonical_subset
    stamp = datetime(2026, 1, 1, 12, 0, 5)
    plain = _canonical_subset("aid-0001", stamp, make_decision(0), "run-1", "ev-1")
    bound = _canonical_subset("aid-0001", stamp, dataclasses.replace(make_decision(0), audit_hash="ptr-1"), "run-1", "ev-1")
    assert bound.pop("audit_hash") == "ptr-1"
    assert bound == plain


def _stored_with_audit_hash(tmp_path, n=3):
    import dataclasses
    store = FileLedgerStore(tmp_path)
    ledger = DecisionLedger(require_context=True, sink=store.sink)
    for i in range(n):
        ledger.append(dataclasses.replace(make_decision(i), audit_hash=f"ptr-{i}"), context=ctx_n(i + 1))
    return store, ledger


def _edit_audit_hash(store, index, value, rechain=False):
    lines = open(store.path, "rb").read().split(b"\n")[:-1]
    obj = json.loads(lines[index])
    obj["entry"]["decision"]["audit_hash"] = value
    obj["sum"] = _sum(obj["entry"])
    lines[index] = _canonical(obj).encode()
    if rechain:
        lines = _rechain(lines, index)
    with open(store.path, "wb") as fh:
        fh.write(b"\n".join(lines) + b"\n")


def test_f1_an_audit_hash_edited_with_a_fixed_sum_is_refused_by_load_and_from_export(tmp_path):
    store, ledger = _stored_with_audit_hash(tmp_path)
    _edit_audit_hash(store, 1, "forged")
    with pytest.raises(StoreCorruptError, match="immutable_hash"):
        store.load(True)
    export = ledger.export()
    export["entries"][1]["decision"]["audit_hash"] = "forged"
    with pytest.raises(ValueError, match="immutable_hash"):
        DecisionLedger.from_export(export)


def test_f1_reconcile_does_not_report_ok_for_an_edited_audit_hash(tmp_path):
    from delta.store import CheckpointStore, reconcile
    store, ledger = _stored_with_audit_hash(tmp_path / "led")
    cps = CheckpointStore(tmp_path / "cp" / "checkpoint.json")
    cps.write(ledger.checkpoint())
    _edit_audit_hash(store, 1, "forged")
    with pytest.raises(StoreCorruptError):  # the stored hash no longer matches: refused before any status
        reconcile(store, cps, require_context=True)
    _edit_audit_hash(store, 1, "forged", rechain=True)  # an attacker who also re-chains is caught by the checkpoint
    assert reconcile(store, cps, require_context=True)["status"] == "DIVERGED"


# ---------------------------------------------------------------- F9 and F11: what the README now says, held by tests
def test_f9_the_phantom_retry_is_idempotent_only_for_the_same_entry_timestamp(tmp_path):
    from datetime import datetime
    store = FileLedgerStore(tmp_path)
    ledger = DecisionLedger(require_context=True, sink=store.sink)
    ledger.append(make_decision(0), context=ctx_n(1))

    class Boom(Exception):
        pass

    real = ledger._sink

    def sink_then_fail(entry):
        real(entry)
        raise Boom()

    ledger._sink = sink_then_fail
    with pytest.raises(Boom):
        ledger.append(make_decision(1), datetime(2026, 1, 1, 12, 0, 1), context=ctx_n(2))
    ledger._sink = real
    ledger.append(make_decision(1), datetime(2026, 1, 1, 12, 0, 1), context=ctx_n(2))  # same timestamp: one line
    assert len(store.load(True)[0]) == 2
    ledger3, _ = store.load(True, attach=True)
    ledger3._sink = sink_then_fail
    with pytest.raises(Boom):
        ledger3.append(make_decision(2), datetime(2026, 1, 1, 12, 0, 2), context=ctx_n(3))
    ledger3._sink = real
    with pytest.raises(StoreError):  # a fresh timestamp is a different entry
        ledger3.append(make_decision(2), datetime(2026, 1, 1, 12, 0, 3), context=ctx_n(3))


def test_f11_an_old_signed_checkpoint_replayed_after_a_tail_rewrite_reads_unwitnessed_tail(tmp_path):
    import hashlib
    from delta.store import CheckpointStore, reconcile
    sign = lambda b: hashlib.sha256(b"K" + b).digest()  # noqa: E731
    verify = lambda d, s: s == hashlib.sha256(b"K" + d).digest()  # noqa: E731
    store = FileLedgerStore(tmp_path / "led")
    ledger = fill(store, 2)
    cps = CheckpointStore(tmp_path / "cp.json")
    cps.write(ledger.checkpoint(), sign=sign)
    old = open(cps.path, "rb").read()
    for i in range(2, 5):
        ledger.append(make_decision(i), context=ctx_n(i + 1))
    cps.write(ledger.checkpoint(), sign=sign)
    os.remove(store.path)
    fork = DecisionLedger(require_context=True, sink=FileLedgerStore(tmp_path / "led").sink)
    for i in range(2):
        fork.append(make_decision(i), context=ctx_n(i + 1))
    for i in range(2, 5):
        fork.append(make_decision(100 + i), context=ctx_n(100 + i))
    with open(cps.path, "wb") as fh:
        fh.write(old)
    result = reconcile(store, cps, require_context=True, verify=verify)
    assert result["status"] == "UNWITNESSED_TAIL" and result["unwitnessed"] == 3  # no freshness: not detected
