"""FileLedgerStore: a durable sink and a loader that trusts nothing.

The SIGKILL test is a real process kill. It shows an acknowledged entry survives the death of the process; it does
not show survival of an OS crash or power loss (nothing here can).
"""
import json
import os
import random
import signal
import subprocess
import sys
import textwrap

import pytest
from store_support import ctx_n, make_decision

from delta import DecisionLedger
from delta.store import LEDGER_FILE, FileLedgerStore, StoreCorruptError, StoreError

HERE = os.path.dirname(os.path.abspath(__file__))


def fill(store, n, require_context=True, start=0):
    ledger = DecisionLedger(require_context=require_context, sink=store.sink)
    for i in range(start, start + n):
        ledger.append(make_decision(i), context=ctx_n(i + 1))
    return ledger


def raw_lines(store):
    with open(store.path, "rb") as fh:
        return fh.read().split(b"\n")[:-1]


def write_bytes(store, data):
    with open(store.path, "wb") as fh:
        fh.write(data)


def test_round_trip_append_then_load_in_a_fresh_object(tmp_path):
    store = FileLedgerStore(tmp_path / "led")
    ledger = fill(store, 4)
    loaded, report = FileLedgerStore(tmp_path / "led").load(require_context=True)
    assert loaded is not ledger and loaded.verify_chain_integrity()
    assert loaded.export() == ledger.export()
    assert loaded.entries == ledger.entries and loaded.chain_head == ledger.chain_head
    assert report["entries"] == 4 and report["recovered_tail"] == {"count": 0, "bytes": 0}


def test_a_stored_line_holds_the_export_rendering_and_its_sum(tmp_path):
    store = FileLedgerStore(tmp_path)
    ledger = fill(store, 2)
    for line, rendered in zip(raw_lines(store), ledger.export()["entries"]):
        obj = json.loads(line)
        assert set(obj) == {"v", "entry", "sum"} and obj["v"] == 1 and obj["entry"] == rendered
        import hashlib
        canon = json.dumps(rendered, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)
        assert obj["sum"] == hashlib.sha256(canon.encode()).hexdigest()


def test_a_missing_file_loads_as_an_empty_ledger_and_creates_nothing(tmp_path):
    store = FileLedgerStore(tmp_path / "nope")
    ledger, report = store.load()
    assert len(ledger) == 0 and report["entries"] == 0
    assert not (tmp_path / "nope").exists()


def test_a_loaded_ledger_can_carry_on_recording_to_the_same_store(tmp_path):
    store = FileLedgerStore(tmp_path)
    fill(store, 2)
    loaded, _ = store.load(require_context=True, attach=True)
    loaded.append(make_decision(2), context=ctx_n(3))
    again, _ = FileLedgerStore(tmp_path).load(require_context=True)
    assert len(again) == 3 and again.chain_head == loaded.chain_head
    detached, _ = store.load(require_context=True)
    assert detached._sink is None


@pytest.mark.parametrize("n", [3, 8])
def test_a_real_sigkill_never_loses_an_acknowledged_entry(tmp_path, n):
    directory = tmp_path / "led"
    child = textwrap.dedent(f"""
        import sys
        sys.path.insert(0, {HERE!r})
        from store_support import ctx_n, make_decision
        from delta import DecisionLedger
        from delta.store import FileLedgerStore
        store = FileLedgerStore({str(directory)!r})
        ledger = DecisionLedger(require_context=True, sink=store.sink)
        for i in range(100000):
            entry = ledger.append(make_decision(i), context=ctx_n(i + 1))
            print(entry.audit_id, flush=True)
    """)
    rng = random.Random()
    for attempt in range(3):
        if directory.exists():
            for f in directory.iterdir():
                f.unlink()
        kill_after = rng.randint(n, n * 6)
        proc = subprocess.Popen([sys.executable, "-c", child], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        acked = []
        while len(acked) < kill_after:
            line = proc.stdout.readline()
            if not line:
                pytest.fail(f"child died early: {proc.stderr.read().decode()}")
            acked.append(line.decode().strip())
        proc.send_signal(signal.SIGKILL)
        rest = proc.stdout.read()
        proc.wait()
        assert proc.returncode == -signal.SIGKILL
        acked += [x for x in rest.decode().split("\n")[:-1] if x]  # a partial last line (no newline) is not an ack
        ledger, report = FileLedgerStore(directory).load(require_context=True)
        have = [e.audit_id for e in ledger.entries]
        assert have[: len(acked)] == acked, f"an acknowledged entry is missing (kill_after={kill_after})"
        assert len(have) in (len(acked), len(acked) + 1), f"{len(have)} stored, {len(acked)} acknowledged"
        assert ledger.verify_chain_integrity()
        assert report["recovered_tail"]["count"] in (0, 1)


def test_a_torn_final_line_is_dropped_and_reported_in_each_shape(tmp_path):
    store = FileLedgerStore(tmp_path)
    fill(store, 3)
    good = open(store.path, "rb").read()
    lines = good.split(b"\n")[:-1]
    prefix = b"".join(x + b"\n" for x in lines[:2])
    shapes = {
        "truncated mid-line": prefix + lines[2][: len(lines[2]) // 2],
        "garbage appended": good + b'{"v":1,"entry":{"aud',
        "no trailing newline": good[:-1],
        "complete garbage line": good + b"not json at all\n",
        "bad sum on the last line": prefix + lines[2].replace(b'"sum":"', b'"sum":"0') + b"\n",
    }
    for name, data in shapes.items():
        write_bytes(store, data)
        before = os.stat(store.path).st_mtime_ns
        ledger, report = store.load(require_context=True)
        assert os.stat(store.path).st_mtime_ns == before and open(store.path, "rb").read() == data, name
        assert ledger.verify_chain_integrity(), name
        tail = report["recovered_tail"]
        assert tail["count"] == 1 and tail["bytes"] > 0, name
        if name in ("garbage appended", "complete garbage line"):
            assert len(ledger) == 3, name
        else:
            assert len(ledger) == 2, name
        assert report["valid_bytes"] == sum(len(x) + 1 for x in lines[: len(ledger)]), name


def test_damage_before_the_last_line_raises_naming_the_line(tmp_path):
    store = FileLedgerStore(tmp_path)
    fill(store, 4)
    lines = raw_lines(store)
    for index, damaged in ((0, b"garbage"), (1, lines[1][:40]), (2, lines[2].replace(b'"sum":"', b'"sum":"f'))):
        broken = list(lines)
        broken[index] = damaged
        write_bytes(store, b"".join(x + b"\n" for x in broken))
        with pytest.raises(StoreCorruptError, match=f"line {index + 1}\\b"):
            store.load()


def test_a_damaged_line_followed_by_a_torn_tail_is_mid_file_damage(tmp_path):
    store = FileLedgerStore(tmp_path)
    fill(store, 3)
    lines = raw_lines(store)
    write_bytes(store, lines[0] + b"\n" + b"garbage\n" + lines[2][:30])
    with pytest.raises(StoreCorruptError, match="line 2"):
        store.load()


def test_a_mutated_line_with_a_valid_sum_is_refused_by_the_recomputation(tmp_path):
    import hashlib
    store = FileLedgerStore(tmp_path)
    fill(store, 3)
    lines = raw_lines(store)
    obj = json.loads(lines[1])
    obj["entry"]["decision"]["reasoning"] = "rewritten"
    canon = json.dumps(obj["entry"], sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)
    obj["sum"] = hashlib.sha256(canon.encode()).hexdigest()
    lines[1] = json.dumps(obj, sort_keys=True, separators=(",", ":")).encode()
    write_bytes(store, b"".join(x + b"\n" for x in lines))
    with pytest.raises(StoreCorruptError, match="line 2"):
        store.load()


def test_a_mutated_final_line_with_a_valid_sum_is_corruption_not_a_torn_tail(tmp_path):
    import hashlib
    store = FileLedgerStore(tmp_path)
    fill(store, 2)
    lines = raw_lines(store)
    obj = json.loads(lines[1])
    obj["entry"]["decision"]["confidence"] = 0.1
    canon = json.dumps(obj["entry"], sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)
    obj["sum"] = hashlib.sha256(canon.encode()).hexdigest()
    lines[1] = json.dumps(obj, sort_keys=True, separators=(",", ":")).encode()
    write_bytes(store, b"".join(x + b"\n" for x in lines))
    with pytest.raises(StoreCorruptError, match="line 2"):
        store.load()


def test_a_reordered_file_is_refused(tmp_path):
    store = FileLedgerStore(tmp_path)
    fill(store, 3)
    lines = raw_lines(store)
    write_bytes(store, b"".join(x + b"\n" for x in (lines[0], lines[2], lines[1])))
    with pytest.raises(StoreCorruptError):
        store.load()


def test_a_dropped_middle_line_is_refused(tmp_path):
    store = FileLedgerStore(tmp_path)
    fill(store, 3)
    lines = raw_lines(store)
    write_bytes(store, b"".join(x + b"\n" for x in (lines[0], lines[2])))
    with pytest.raises(StoreCorruptError, match="line 2"):
        store.load()


def test_load_never_writes(tmp_path):
    store = FileLedgerStore(tmp_path)
    fill(store, 2)
    write_bytes(store, open(store.path, "rb").read() + b"torn")
    snapshot = (open(store.path, "rb").read(), os.stat(store.path).st_mtime_ns, sorted(os.listdir(tmp_path)))
    store.load()
    store.load(require_context=True)
    assert (open(store.path, "rb").read(), os.stat(store.path).st_mtime_ns, sorted(os.listdir(tmp_path))) == snapshot


def test_load_opens_the_file_for_reading_only(tmp_path, monkeypatch):
    store = FileLedgerStore(tmp_path)
    fill(store, 1)
    write_bytes(store, open(store.path, "rb").read() + b"torn")
    opened = []
    real_open = open

    def watch(file, mode="r", *a, **k):
        opened.append(mode)
        return real_open(file, mode, *a, **k)

    monkeypatch.setattr("builtins.open", watch)
    monkeypatch.setattr(os, "open", lambda *a, **k: pytest.fail("load used os.open"))
    monkeypatch.setattr(os, "ftruncate", lambda *a, **k: pytest.fail("load truncated"))
    store.load()
    assert opened and set(opened) == {"rb"}


def test_repair_truncates_only_a_torn_tail(tmp_path):
    store = FileLedgerStore(tmp_path)
    fill(store, 3)
    good = open(store.path, "rb").read()
    write_bytes(store, good + b'{"v":1,"entry"')
    report = store.repair()
    assert open(store.path, "rb").read() == good
    assert report["repaired"] is True and report["recovered_tail"]["count"] == 1
    assert store.repair()["repaired"] is False and open(store.path, "rb").read() == good


def test_repair_of_a_clean_file_changes_nothing(tmp_path):
    store = FileLedgerStore(tmp_path)
    fill(store, 2)
    before = open(store.path, "rb").read()
    assert store.repair()["repaired"] is False
    assert open(store.path, "rb").read() == before


def test_repair_refuses_mid_file_damage_and_leaves_the_file_alone(tmp_path):
    store = FileLedgerStore(tmp_path)
    fill(store, 3)
    lines = raw_lines(store)
    data = lines[0] + b"\n" + b"garbage\n" + lines[2] + b"\n" + b"torn"
    write_bytes(store, data)
    with pytest.raises(StoreCorruptError):
        store.repair()
    assert open(store.path, "rb").read() == data


def test_repair_fsyncs(tmp_path, monkeypatch):
    store = FileLedgerStore(tmp_path)
    fill(store, 2)
    write_bytes(store, open(store.path, "rb").read() + b"torn")
    calls = []
    real = os.fsync
    monkeypatch.setattr(os, "fsync", lambda fd: (calls.append(fd), real(fd))[1])
    store.repair()
    assert len(calls) == 1


def test_the_sink_refuses_to_append_after_a_torn_tail_until_repaired(tmp_path):
    store = FileLedgerStore(tmp_path)
    ledger = fill(store, 2)
    write_bytes(store, open(store.path, "rb").read() + b"torn")
    with pytest.raises(StoreError, match="repair"):
        ledger.append(make_decision(2), context=ctx_n(3))
    assert len(ledger) == 2
    store.repair()
    ledger.append(make_decision(2), context=ctx_n(3))
    assert len(store.load(require_context=True)[0]) == 3


def test_sink_is_idempotent_by_audit_id(tmp_path):
    store = FileLedgerStore(tmp_path)
    ledger = fill(store, 2)
    before = open(store.path, "rb").read()
    store.sink(ledger.entries[-1])
    assert open(store.path, "rb").read() == before


def test_sink_raises_on_the_same_audit_id_with_different_content(tmp_path):
    import dataclasses
    store = FileLedgerStore(tmp_path)
    ledger = fill(store, 2)
    before = open(store.path, "rb").read()
    last = ledger.entries[-1]
    other = dataclasses.replace(last, run_id="another-run")
    with pytest.raises(StoreError, match="different content"):
        store.sink(other)
    assert open(store.path, "rb").read() == before


def test_sink_refuses_an_entry_that_does_not_continue_the_stored_chain(tmp_path):
    store = FileLedgerStore(tmp_path)
    fill(store, 2)
    before = open(store.path, "rb").read()
    fresh = DecisionLedger(require_context=True)
    entry = fresh.append(make_decision(7), context=ctx_n(9))
    with pytest.raises(StoreError, match="continue"):
        store.sink(entry)
    assert open(store.path, "rb").read() == before


def test_sink_refuses_a_non_entry_and_an_entry_that_would_not_load(tmp_path):
    import dataclasses
    store = FileLedgerStore(tmp_path)
    with pytest.raises(StoreError):
        store.sink({"audit_id": "x"})
    fresh = DecisionLedger(require_context=True)
    entry = fresh.append(make_decision(0), context=ctx_n(1))
    bad = dataclasses.replace(entry, immutable_hash="not-hex")
    with pytest.raises(StoreError):
        store.sink(bad)
    assert not os.path.exists(store.path) or open(store.path, "rb").read() == b""


def test_a_retry_after_the_sink_wrote_and_the_ledger_raised_leaves_one_line(tmp_path):
    store = FileLedgerStore(tmp_path)
    state = {"fail": True}

    def wrote_then_failed(entry):
        store.sink(entry)
        if state["fail"]:
            state["fail"] = False
            raise RuntimeError("the sink wrote, then something failed")

    ledger = DecisionLedger(require_context=True, sink=wrote_then_failed)
    with pytest.raises(RuntimeError):
        ledger.append(make_decision(0), context=ctx_n(1))
    assert len(ledger) == 0 and len(raw_lines(store)) == 1  # the phantom: on disk, not in the ledger
    entry = ledger.append(make_decision(0), context=ctx_n(1))
    lines = raw_lines(store)
    assert len(lines) == 1 and json.loads(lines[0])["entry"]["audit_id"] == entry.audit_id
    assert store.load(require_context=True)[0].export() == ledger.export()


def test_a_phantom_that_the_retry_does_not_reproduce_blocks_recording_rather_than_forking_the_chain(tmp_path):
    store = FileLedgerStore(tmp_path)
    state = {"fail": True}

    def wrote_then_failed(entry):
        store.sink(entry)
        if state["fail"]:
            state["fail"] = False
            raise RuntimeError("boom")

    ledger = DecisionLedger(require_context=True, sink=wrote_then_failed)
    with pytest.raises(RuntimeError):
        ledger.append(make_decision(0), context=ctx_n(1))
    with pytest.raises(StoreError):
        ledger.append(make_decision(5), context=ctx_n(2))  # a different Decision: a different audit_id
    assert len(raw_lines(store)) == 1
    loaded, _ = store.load(require_context=True)
    assert len(loaded) == 1 and loaded.verify_chain_integrity()  # load includes the phantom


def test_fsync_is_called_for_the_file_and_the_directory_on_creation(tmp_path, monkeypatch):
    import stat
    kinds = []
    real = os.fsync

    def spy(fd):
        kinds.append("dir" if stat.S_ISDIR(os.fstat(fd).st_mode) else "file")
        return real(fd)

    monkeypatch.setattr(os, "fsync", spy)
    store = FileLedgerStore(tmp_path / "led")
    ledger = DecisionLedger(require_context=True, sink=store.sink)
    ledger.append(make_decision(0), context=ctx_n(1))
    assert kinds == ["dir", "file", "dir"]  # F10: the parent of the directory the store created is synced first
    kinds.clear()
    ledger.append(make_decision(1), context=ctx_n(2))
    assert kinds == ["file"]
    kinds.clear()
    store.sink(ledger.entries[-1])  # an idempotent retry still syncs: the earlier fsync may be the one that failed
    assert kinds == ["file"]


def test_an_fsync_failure_is_not_acknowledged(tmp_path, monkeypatch):
    store = FileLedgerStore(tmp_path)
    ledger = DecisionLedger(require_context=True, sink=store.sink)

    def broken(fd):
        raise OSError(5, "I/O error")

    monkeypatch.setattr(os, "fsync", broken)
    with pytest.raises(OSError):
        ledger.append(make_decision(0), context=ctx_n(1))
    assert len(ledger) == 0


def test_a_failed_write_is_rolled_back_so_no_torn_line_is_left(tmp_path, monkeypatch):
    store = FileLedgerStore(tmp_path)
    ledger = fill(store, 1)
    good = open(store.path, "rb").read()
    real = os.write

    def short_then_fail(fd, data):
        real(fd, bytes(data[:10]))
        raise OSError(28, "No space left on device")

    monkeypatch.setattr(os, "write", short_then_fail)
    with pytest.raises(OSError):
        ledger.append(make_decision(1), context=ctx_n(2))
    monkeypatch.undo()
    assert open(store.path, "rb").read() == good
    ledger.append(make_decision(1), context=ctx_n(2))
    assert len(store.load(require_context=True)[0]) == 2


def test_a_second_writer_with_its_own_ledger_is_refused_not_interleaved(tmp_path):
    """Two writers on one directory, taken in turn (concurrent processes racing are NOT tested)."""
    a = FileLedgerStore(tmp_path)
    b = FileLedgerStore(tmp_path)
    ledger_a = DecisionLedger(require_context=True, sink=a.sink)
    ledger_b = DecisionLedger(require_context=True, sink=b.sink)
    ledger_a.append(make_decision(0), context=ctx_n(1))
    with pytest.raises(StoreError, match="continue"):
        ledger_b.append(make_decision(1), context=ctx_n(2))
    ledger_a.append(make_decision(1), context=ctx_n(2))
    assert len(a.load(require_context=True)[0]) == 2
    assert LEDGER_FILE in os.listdir(tmp_path)
