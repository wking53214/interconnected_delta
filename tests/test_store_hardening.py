"""Small hardening of the store found by independent re-checks: a symbolic link swapped in after the pre-check is still
refused by the open, and ``against=`` is typed. (The history requirement on ``CheckpointStore.write`` is tested in
``test_store_history.py``.)"""
import os

import pytest
from store_support import ctx_n, make_decision

from delta import DecisionLedger
from delta.store import CheckpointStore, FileLedgerStore, StoreError


def fill(store, n):
    ledger = DecisionLedger(require_context=True, sink=store.sink)
    for i in range(n):
        ledger.append(make_decision(i), context=ctx_n(i + 1))
    return ledger


def test_a_symlink_swapped_in_after_the_check_is_still_refused_by_the_open(tmp_path, monkeypatch):
    store = FileLedgerStore(tmp_path / "ledger")
    os.makedirs(store.directory)
    outside = tmp_path / "outside.jsonl"
    outside.write_bytes(b"")
    os.symlink(outside, store.path)
    monkeypatch.setattr(os.path, "islink", lambda path: False)  # the pre-check is bypassed, as a race would
    ledger = DecisionLedger(require_context=True, sink=store.sink)
    with pytest.raises(StoreError, match="symbolic link"):
        ledger.append(make_decision(0), context=ctx_n(1))
    assert outside.read_bytes() == b""
    assert len(ledger) == 0


def test_repair_open_also_refuses_a_link_swapped_in_after_the_check(tmp_path, monkeypatch):
    store = FileLedgerStore(tmp_path / "ledger")
    fill(store, 2)
    real = tmp_path / "moved.jsonl"
    os.replace(store.path, real)
    with open(real, "ab") as fh:
        fh.write(b'{"v": 1, "entry"')  # a torn final line, so repair has something to cut
    os.symlink(real, store.path)
    before = real.read_bytes()
    monkeypatch.setattr(os.path, "islink", lambda path: False)
    with pytest.raises(StoreError, match="symbolic link"):
        store.repair()
    assert real.read_bytes() == before


def test_against_must_be_a_checkpoint_store_or_none(tmp_path):
    store = FileLedgerStore(tmp_path / "ledger")
    fill(store, 2)
    with pytest.raises(TypeError, match="against"):
        store.load(True, attach=True, against="checkpoint.json")
    store.load(True, against=CheckpointStore(tmp_path / "custody" / "checkpoint.json"))  # a real one is fine
