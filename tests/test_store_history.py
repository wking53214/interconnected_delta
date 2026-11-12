"""A checkpoint may replace the stored one only if it extends it, and a restart may not attach to a store that
reconcile says lost or rewrote entries (an independent review's High finding).

Before this change a writer could rebuild its ledger from a file with acknowledged entries deleted, record past the old
checkpoint's length, and write a checkpoint the store accepted: forward-only compared lengths, never history.
"""
import os

import pytest
from store_support import ctx_n, make_decision

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


# ---- the default path: a checkpoint taken from a ledger remembers it, so write() checks the extension itself ----------

def test_the_readme_recipe_after_the_file_was_deleted_cannot_launder_the_loss(tmp_path):
    """A restarted writer that follows the recipe (a fresh ledger recording to the same store), records past the old
    length and checkpoints WITHOUT history=. The checkpoint came from that ledger, so it is the history it extends."""
    store, cps, _ = setup(tmp_path, 3)
    os.remove(store.path)
    assert reconcile(store, cps, require_context=True)["status"] == "LOST"
    restarted = DecisionLedger(require_context=True, sink=store.sink)
    for i in range(10, 14):
        restarted.append(make_decision(i), context=ctx_n(i + 1))
    with pytest.raises(StoreError, match="extend"):
        cps.write(restarted.checkpoint())
    assert cps.read().length == 3
    assert reconcile(store, cps, require_context=True)["status"] != "OK"


def test_a_checkpoint_taken_from_the_honest_ledger_still_extends_the_stored_one(tmp_path):
    store, cps, ledger = setup(tmp_path, 3)
    fill(store, 2, start=3, ledger=ledger)
    cps.write(ledger.checkpoint())  # no history=: the checkpoint carries its own
    assert cps.read().length == 5


def test_a_checkpoint_built_by_hand_or_loaded_from_a_dict_is_compared_by_length_only(tmp_path):
    from delta.ledger import LedgerCheckpoint
    store, cps, ledger = setup(tmp_path, 3)
    fill(store, 1, start=3, ledger=ledger)
    taken = ledger.checkpoint()
    cps.write(LedgerCheckpoint(taken.length, taken.head), length_only=True)  # no lineage: only the explicit flag allows it
    assert cps.read().length == 4


def test_a_checkpoint_keeps_its_value_equality_and_dict_form_with_a_lineage(tmp_path):
    from delta.ledger import LedgerCheckpoint
    store, cps, ledger = setup(tmp_path, 2)
    taken = ledger.checkpoint()
    assert taken == LedgerCheckpoint(taken.length, taken.head)
    assert hash(taken) == hash(LedgerCheckpoint(taken.length, taken.head))
    assert taken.to_dict() == {"length": taken.length, "head": taken.head}
    assert "lineage" not in repr(taken)


# ---- O_NOFOLLOW is what stops a link swapped in after the check -----------------------------------------------------

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


def test_repair_open_also_refuses_a_link_swapped_in_after_the_check(tmp_path, monkeypatch):
    store, cps, _ = setup(tmp_path, 2)
    real = tmp_path / "moved.jsonl"
    os.replace(store.path, real)
    with open(real, "ab") as fh:
        fh.write(b'{"v": 1, "entry"')
    os.symlink(real, store.path)
    before = real.read_bytes()
    monkeypatch.setattr(os.path, "islink", lambda path: False)
    with pytest.raises(StoreError, match="symbolic link"):
        store.repair()
    assert real.read_bytes() == before


# ---- small things the re-check found -----------------------------------------------------------------------------

def test_against_must_be_a_checkpoint_store_or_none(tmp_path):
    store, cps, _ = setup(tmp_path, 2)
    with pytest.raises(TypeError, match="against"):
        store.load(True, attach=True, against="checkpoint.json")


# ---- a checkpoint with no lineage may not replace a stored one unless the caller says it is compared by length only ----

def test_a_checkpoint_round_tripped_through_json_is_refused_over_a_stored_one(tmp_path):
    from delta.ledger import LedgerCheckpoint
    store, cps, ledger = setup(tmp_path, 3)
    fill(store, 1, start=3, ledger=ledger)
    bare = LedgerCheckpoint.from_dict(ledger.checkpoint().to_dict())  # what a checkpoint sent as JSON becomes
    with pytest.raises(StoreError, match="length_only"):
        cps.write(bare)
    assert cps.read().length == 3


def test_a_checkpoint_read_back_from_the_store_is_refused_over_a_stored_one(tmp_path):
    store, cps, ledger = setup(tmp_path, 3)
    with pytest.raises(StoreError, match="length_only"):
        cps.write(cps.read())


def test_the_laundering_sequence_with_a_json_round_trip_is_refused(tmp_path):
    from delta.ledger import LedgerCheckpoint
    store, cps, _ = setup(tmp_path, 3)
    os.remove(store.path)
    restarted = DecisionLedger(require_context=True, sink=store.sink)
    for i in range(10, 14):
        restarted.append(make_decision(i), context=ctx_n(i + 1))
    bare = LedgerCheckpoint.from_dict(restarted.checkpoint().to_dict())
    with pytest.raises(StoreError):
        cps.write(bare)
    assert reconcile(store, cps, require_context=True)["status"] != "OK"


def test_length_only_is_the_explicit_way_to_write_a_checkpoint_without_lineage(tmp_path):
    from delta.ledger import LedgerCheckpoint
    store, cps, ledger = setup(tmp_path, 3)
    fill(store, 1, start=3, ledger=ledger)
    bare = LedgerCheckpoint.from_dict(ledger.checkpoint().to_dict())
    cps.write(bare, length_only=True)
    assert cps.read().length == 4


def test_the_first_checkpoint_needs_no_lineage_and_no_flag(tmp_path):
    from delta.ledger import LedgerCheckpoint
    store = FileLedgerStore(tmp_path / "ledger")
    cps = CheckpointStore(tmp_path / "custody" / "checkpoint.json")
    ledger = fill(store, 2)
    cps.write(LedgerCheckpoint.from_dict(ledger.checkpoint().to_dict()))
    assert cps.read().length == 2


def test_history_given_makes_a_lineage_less_checkpoint_acceptable_when_it_extends(tmp_path):
    from delta.ledger import LedgerCheckpoint
    store, cps, ledger = setup(tmp_path, 3)
    fill(store, 1, start=3, ledger=ledger)
    bare = LedgerCheckpoint.from_dict(ledger.checkpoint().to_dict())
    cps.write(bare, history=ledger)
    assert cps.read().length == 4


def test_length_only_does_not_waive_a_history_that_is_given(tmp_path):
    store, cps, ledger = setup(tmp_path, 3)
    other = DecisionLedger(require_context=True)
    for i in (50, 51, 52, 53):
        other.append(make_decision(i), context=ctx_n(i + 1))
    with pytest.raises(StoreError, match="extend"):
        cps.write(other.checkpoint(), history=other, length_only=True)


# ---- two safeguards the third review found untested ------------------------------------------------------------

def test_a_lineage_is_read_once(tmp_path):
    """A lineage that is not a plain tuple and answers differently on a second read: the first read is what is checked."""
    from delta.ledger import LedgerCheckpoint
    store, cps, ledger = setup(tmp_path, 3)
    fill(store, 1, start=3, ledger=ledger)
    honest = ledger.checkpoint()

    class Once:
        reads = 0

        def __iter__(self):
            Once.reads += 1
            return iter(honest.lineage if Once.reads == 1 else ())

    cps.write(LedgerCheckpoint(honest.length, honest.head, Once()))  # type: ignore[arg-type]
    assert Once.reads == 1 and cps.read().length == 4


@pytest.mark.parametrize("junk", [[1, 2, 3], (None,) * 5, "abcdef", 7])
def test_a_lineage_that_is_not_ledger_entries_is_a_store_error(tmp_path, junk):
    from delta.ledger import LedgerCheckpoint
    store, cps, ledger = setup(tmp_path, 3)
    fill(store, 1, start=3, ledger=ledger)
    taken = ledger.checkpoint()
    with pytest.raises(StoreError, match="history"):
        cps.write(LedgerCheckpoint(taken.length, taken.head, junk))  # type: ignore[arg-type]
