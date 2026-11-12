"""A checkpoint may replace the stored one only if it extends it, and a restart may not attach to a store that
reconcile says lost or rewrote entries (an independent review's High finding).

Before this change a writer could rebuild its ledger from a file with acknowledged entries deleted, record past the old
checkpoint's length, and write a checkpoint the store accepted: forward-only compared lengths, never history.
"""
import os

import pytest
from store_support import cp_from, ctx_n, ledger_of, make_decision

from delta import DecisionLedger
from delta.store import CheckpointStore, FileLedgerStore, StoreError, reconcile


def fill(store, n, start=0, ledger=None):
    ledger = ledger or DecisionLedger(require_context=True, sink=store.sink)
    for i in range(start, start + n):
        ledger.append(make_decision(i), context=ctx_n(i + 1))
    return ledger


def setup(tmp_path, n):
    store = FileLedgerStore(tmp_path / "ledger")
    cps = CheckpointStore(tmp_path / "custody" / "checkpoint.json")
    ledger = fill(store, n)
    cps.write(ledger.checkpoint())
    return store, cps, ledger


def keep_first_lines(store, count):
    with open(store.path, "rb") as fh:
        kept = fh.read().split(b"\n")[:count]
    with open(store.path, "wb") as fh:
        fh.write(b"\n".join(kept) + b"\n")


# ---- the restart path: load(attach=True) must be told what to compare with ----------------------------------------

def test_attach_without_saying_what_to_compare_with_is_refused(tmp_path):
    store, cps, _ = setup(tmp_path, 3)
    with pytest.raises(StoreError, match="against"):
        store.load(True, attach=True)


def test_attach_to_a_store_that_lost_acknowledged_entries_is_refused(tmp_path):
    store, cps, _ = setup(tmp_path, 3)
    keep_first_lines(store, 1)
    assert reconcile(store, cps, require_context=True)["status"] == "LOST"
    with pytest.raises(StoreError, match="LOST"):
        store.load(True, attach=True, against=cps)


def test_attach_to_a_store_whose_history_was_rewritten_is_refused(tmp_path):
    store, cps, _ = setup(tmp_path, 3)
    # a different first entry, re-chained: a different history of the same length
    os.remove(store.path)
    other = DecisionLedger(require_context=True, sink=store.sink)
    for i in (50, 51, 52):
        other.append(make_decision(i), context=ctx_n(i + 1))
    assert reconcile(store, cps, require_context=True)["status"] == "DIVERGED"
    with pytest.raises(StoreError, match="DIVERGED"):
        store.load(True, attach=True, against=cps)


def test_the_laundering_sequence_no_longer_ends_in_ok(tmp_path):
    """The review's attack end to end: delete two acknowledged entries, restart the documented way, record past the old
    length and write a checkpoint. The restart is refused, so there is no checkpoint to launder the loss."""
    store, cps, _ = setup(tmp_path, 3)
    keep_first_lines(store, 1)
    with pytest.raises(StoreError):
        store.load(True, attach=True, against=cps)
    assert cps.read().length == 3  # the independent checkpoint is untouched
    assert reconcile(store, cps, require_context=True)["status"] == "LOST"


def test_attach_is_allowed_when_reconcile_is_ok_or_has_an_unwitnessed_tail(tmp_path):
    store, cps, _ = setup(tmp_path, 3)
    ledger, _ = store.load(True, attach=True, against=cps)
    fill(store, 1, start=3, ledger=ledger)  # now one entry past the checkpoint
    ledger2, _ = store.load(True, attach=True, against=cps)  # UNWITNESSED_TAIL is allowed
    assert len(ledger2) == 4
    fill(store, 1, start=4, ledger=ledger2)
    cps.write(ledger2.checkpoint(), history=ledger2)
    assert reconcile(store, cps, require_context=True)["status"] == "OK"


def test_attach_to_entries_with_no_checkpoint_is_refused_but_a_checkpoint_first_works(tmp_path):
    store = FileLedgerStore(tmp_path / "ledger")
    cps = CheckpointStore(tmp_path / "custody" / "checkpoint.json")
    fill(store, 2)
    with pytest.raises(StoreError, match="NO_CHECKPOINT"):
        store.load(True, attach=True, against=cps)
    ledger, _ = store.load(True)  # no attach: read only
    cps.write(ledger.checkpoint())
    store.load(True, attach=True, against=cps)


def test_attach_to_an_empty_store_with_no_checkpoint_is_allowed(tmp_path):
    store = FileLedgerStore(tmp_path / "ledger")
    cps = CheckpointStore(tmp_path / "custody" / "checkpoint.json")
    ledger, _ = store.load(True, attach=True, against=cps)
    assert len(ledger) == 0


def test_against_none_attaches_without_a_comparison_and_says_so_in_the_name(tmp_path):
    store, cps, _ = setup(tmp_path, 3)
    keep_first_lines(store, 1)
    ledger, _ = store.load(True, attach=True, against=None)
    assert len(ledger) == 1


def test_against_without_attach_changes_nothing(tmp_path):
    store, cps, _ = setup(tmp_path, 3)
    keep_first_lines(store, 1)
    ledger, report = store.load(True, against=cps)
    assert len(ledger) == 1 and report["entries"] == 1


# ---- the write path: history= shows the new checkpoint extends the stored one ------------------------------------

def test_a_checkpoint_that_does_not_extend_the_stored_one_is_refused_with_history(tmp_path):
    store, cps, _ = setup(tmp_path, 3)  # stored checkpoint: length 3
    rebuilt = DecisionLedger(require_context=True)  # a different history, longer than the stored length
    for i in (50, 51, 52, 53):
        rebuilt.append(make_decision(i), context=ctx_n(i + 1))
    with pytest.raises(StoreError, match="extend"):
        cps.write(rebuilt.checkpoint(), history=rebuilt)
    assert cps.read().length == 3


def test_a_checkpoint_that_extends_the_stored_one_is_accepted_with_history(tmp_path):
    store, cps, ledger = setup(tmp_path, 3)
    fill(store, 2, start=3, ledger=ledger)
    cps.write(ledger.checkpoint(), history=ledger)
    assert cps.read().length == 5


def test_a_history_shorter_than_the_stored_checkpoint_is_refused(tmp_path):
    store, cps, ledger = setup(tmp_path, 3)
    short = DecisionLedger(require_context=True)
    short.append(make_decision(0), context=ctx_n(1))
    with pytest.raises(StoreError, match="history"):
        cps.write(ledger.checkpoint(), history=short)


def test_the_new_checkpoint_must_itself_be_the_history_head_at_its_length(tmp_path):
    store, cps, ledger = setup(tmp_path, 3)
    fill(store, 2, start=3, ledger=ledger)
    other = DecisionLedger(require_context=True)
    for i in (50, 51, 52, 53, 54):
        other.append(make_decision(i), context=ctx_n(i + 1))
    with pytest.raises(StoreError, match="history"):
        cps.write(ledger.checkpoint(), history=other)


def test_history_must_be_a_ledger(tmp_path):
    store, cps, ledger = setup(tmp_path, 3)
    with pytest.raises(TypeError, match="history"):
        cps.write(ledger.checkpoint(), history=[1, 2, 3])


def test_the_first_checkpoint_needs_no_stored_one_to_extend_but_must_match_history(tmp_path):
    store = FileLedgerStore(tmp_path / "ledger")
    cps = CheckpointStore(tmp_path / "custody" / "checkpoint.json")
    ledger = fill(store, 2)
    other = DecisionLedger(require_context=True)
    other.append(make_decision(90), context=ctx_n(91))
    other.append(make_decision(91), context=ctx_n(92))
    with pytest.raises(StoreError, match="history"):
        cps.write(ledger.checkpoint(), history=other)
    cps.write(ledger.checkpoint(), history=ledger)


# ---- a symlinked ledger file is not followed on a write --------------------------------------------------------

def test_a_sound_stored_checkpoint_cannot_be_replaced_without_history(tmp_path):
    grown = ledger_of(5)
    store = CheckpointStore(tmp_path / "checkpoint.json")
    store.write(cp_from(grown, 3))
    with pytest.raises(StoreError, match="does not extend the stored one"):
        store.write(cp_from(grown, 5))  # no history: the store cannot tell this from a rebuilt ledger
    assert store.read() == cp_from(grown, 3)
    store.write(cp_from(grown, 5), history=grown)
    assert store.read() == cp_from(grown, 5)


def test_the_sink_does_not_follow_a_symlinked_ledger_file(tmp_path):
    store = FileLedgerStore(tmp_path / "ledger")
    os.makedirs(store.directory)
    outside = tmp_path / "outside.jsonl"
    outside.write_bytes(b"")
    os.symlink(outside, store.path)
    ledger = DecisionLedger(require_context=True, sink=store.sink)
    with pytest.raises(StoreError, match="symbolic link"):
        ledger.append(make_decision(0), context=ctx_n(1))
    assert outside.read_bytes() == b""
    assert len(ledger) == 0


def test_repair_does_not_follow_a_symlinked_ledger_file(tmp_path):
    store, cps, _ = setup(tmp_path, 2)
    real = tmp_path / "moved.jsonl"
    os.replace(store.path, real)
    with open(real, "ab") as fh:
        fh.write(b'{"v": 1, "entry"')  # a torn final line, so repair has something to cut
    os.symlink(real, store.path)
    before = real.read_bytes()
    with pytest.raises(StoreError, match="symbolic link"):
        store.repair()
    assert real.read_bytes() == before
