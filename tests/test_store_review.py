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
