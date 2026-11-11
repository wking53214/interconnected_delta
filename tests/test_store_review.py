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
