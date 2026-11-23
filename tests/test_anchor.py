"""The outside anchor: signed checkpoints published one file each, forward only, and a ledger checked against them.

The anchor here is a temporary directory standing in for a local clone of the protected anchor repository.
`cryptography` Ed25519 is used ONLY as the operator's signer and verifier.
"""
import json
import os

import pytest
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from store_support import cp_from, ledger_of

import delta
from delta import (
    AnchorError,
    CheckpointStore,
    DecisionLedger,
    LedgerCheckpoint,
    anchor_record,
    check_against_anchor,
    publish_checkpoint,
    read_anchor,
)
from delta.anchor import ANCHOR_SCHEMA


def pair():
    key = Ed25519PrivateKey.generate()
    pub = key.public_key()

    def verify(data, signature):
        try:
            pub.verify(signature, data)
        except InvalidSignature:
            return False
        return True

    return key.sign, verify


@pytest.fixture
def keys():
    return pair()


def files(anchor):
    return sorted(os.listdir(os.path.join(anchor, "checkpoints")))


# Publishing.

def test_the_helpers_are_exported():
    for name in ("AnchorError", "anchor_record", "check_against_anchor", "publish_checkpoint", "read_anchor"):
        assert name in delta.__all__


def test_only_length_head_and_signature_are_published(tmp_path, keys):
    sign, verify = keys
    ledger = ledger_of(3)
    path = publish_checkpoint(tmp_path, ledger.checkpoint(), sign=sign, verify=verify)
    assert os.path.basename(path) == "00000000000000000003.json"
    obj = json.loads(open(path, "rb").read())
    assert set(obj) == {"schema", "length", "head", "signature"}
    assert obj["schema"] == ANCHOR_SCHEMA and obj["length"] == 3 and obj["head"] == ledger.checkpoint().head
    raw = open(path, "rb").read()
    for entry in ledger.entries:  # no Decision, entry or data is published
        assert entry.decision.entity_id.encode() not in raw and entry.audit_id.encode() not in raw


def test_the_signature_is_the_one_checkpoint_store_makes(tmp_path, keys):
    sign, verify = keys
    ledger = ledger_of(2)
    store = CheckpointStore(tmp_path / "cp.json")
    store.write(ledger.checkpoint(), sign=sign)
    record = anchor_record(ledger.checkpoint(), sign)
    assert verify(CheckpointStore._signing_input(ledger.checkpoint()), bytes.fromhex(record["signature"])) is True
    assert store.read(verify=verify) == ledger.checkpoint()


def test_publishing_moves_forward_along_one_history(tmp_path, keys):
    sign, verify = keys
    grown = ledger_of(5)
    publish_checkpoint(tmp_path, cp_from(grown, 2), sign=sign, verify=verify)
    publish_checkpoint(tmp_path, cp_from(grown, 4), sign=sign, verify=verify, history=grown)
    publish_checkpoint(tmp_path, cp_from(grown, 5), sign=sign, verify=verify, history=grown)
    assert [cp.length for cp in read_anchor(tmp_path, verify=verify)] == [2, 4, 5]


def test_the_same_checkpoint_again_is_a_no_op(tmp_path, keys):
    sign, verify = keys
    ledger = ledger_of(2)
    publish_checkpoint(tmp_path, ledger.checkpoint(), sign=sign, verify=verify)
    before = open(tmp_path / "checkpoints" / files(tmp_path)[0], "rb").read()
    assert publish_checkpoint(tmp_path, ledger.checkpoint(), sign=sign, verify=verify) is None
    assert open(tmp_path / "checkpoints" / files(tmp_path)[0], "rb").read() == before


def test_a_lower_length_is_refused(tmp_path, keys):
    sign, verify = keys
    grown = ledger_of(4)
    publish_checkpoint(tmp_path, cp_from(grown, 4), sign=sign, verify=verify)
    with pytest.raises(AnchorError, match="behind"):
        publish_checkpoint(tmp_path, cp_from(grown, 2), sign=sign, verify=verify, history=grown)


def test_the_same_length_with_another_head_is_a_fork_and_refused(tmp_path, keys):
    sign, verify = keys
    publish_checkpoint(tmp_path, cp_from(ledger_of(3), 3), sign=sign, verify=verify)
    other = LedgerCheckpoint(3, "a" * 64)
    with pytest.raises(AnchorError, match="fork"):
        publish_checkpoint(tmp_path, other, sign=sign, verify=verify)


def test_a_rebuilt_ledger_cannot_publish_past_what_it_lost(tmp_path, keys):
    sign, verify = keys
    publish_checkpoint(tmp_path, cp_from(ledger_of(3), 3), sign=sign, verify=verify)
    rebuilt = DecisionLedger(require_context=True)  # lost the first three entries, then grew past them
    from store_support import ctx_n, make_decision
    for i in range(5):
        rebuilt.append(make_decision(100 + i), context=ctx_n(100 + i))
    with pytest.raises(AnchorError, match="does not extend"):
        publish_checkpoint(tmp_path, rebuilt.checkpoint(), sign=sign, verify=verify, history=rebuilt)
    with pytest.raises(AnchorError, match="history"):
        publish_checkpoint(tmp_path, rebuilt.checkpoint(), sign=sign, verify=verify)


def test_a_checkpoint_that_is_not_the_history_s_head_is_refused(tmp_path, keys):
    sign, verify = keys
    with pytest.raises(AnchorError, match="not the head"):
        publish_checkpoint(tmp_path, LedgerCheckpoint(2, "b" * 64), sign=sign, verify=verify, history=ledger_of(3))


def test_an_empty_ledger_is_not_published(tmp_path, keys):
    sign, verify = keys
    with pytest.raises(AnchorError, match="empty"):
        publish_checkpoint(tmp_path, LedgerCheckpoint(0, "0" * 64), sign=sign, verify=verify)


def test_a_signer_the_verifier_rejects_publishes_nothing(tmp_path, keys):
    sign, _ = keys
    _, other_verify = pair()
    with pytest.raises(AnchorError, match="do not agree"):
        publish_checkpoint(tmp_path, ledger_of(1).checkpoint(), sign=sign, verify=other_verify)
    assert not os.path.exists(tmp_path / "checkpoints") or files(tmp_path) == []


@pytest.mark.parametrize("bad", [lambda d: "sig", lambda d: b""])
def test_a_signer_that_returns_anything_but_bytes_is_refused(tmp_path, keys, bad):
    _, verify = keys
    with pytest.raises((TypeError, ValueError)):
        publish_checkpoint(tmp_path, ledger_of(1).checkpoint(), sign=bad, verify=verify)


def test_a_name_that_appears_while_publishing_is_never_replaced(tmp_path, keys, monkeypatch):
    sign, verify = keys
    ledger = ledger_of(1)
    real_link = os.link

    def racing_link(src, dst):
        with open(dst, "wb") as fh:
            fh.write(b"planted\n")
        return real_link(src, dst)

    monkeypatch.setattr(os, "link", racing_link)
    with pytest.raises(AnchorError, match="appeared"):
        publish_checkpoint(tmp_path, ledger.checkpoint(), sign=sign, verify=verify)
    assert open(tmp_path / "checkpoints" / "00000000000000000001.json", "rb").read() == b"planted\n"
    assert [n for n in os.listdir(tmp_path / "checkpoints") if n.startswith(".")] == []


# Reading.

def test_nothing_published_reads_as_empty(tmp_path, keys):
    assert read_anchor(tmp_path, verify=keys[1]) == []


def test_verify_is_required(tmp_path):
    with pytest.raises(TypeError):
        read_anchor(tmp_path, verify=None)


def write_raw(anchor, name, obj, canonical=True):
    os.makedirs(os.path.join(anchor, "checkpoints"), exist_ok=True)
    text = json.dumps(obj, sort_keys=True, separators=(",", ":")) if canonical else json.dumps(obj, indent=1)
    with open(os.path.join(anchor, "checkpoints", name), "w") as fh:
        fh.write(text + "\n")


@pytest.mark.parametrize("mutate,match", [
    (lambda o: o.update(signature=("00" * 64)), "does not verify"),
    (lambda o: o.update(head="c" * 64), "does not verify"),
    (lambda o: o.update(decision="ESCALATE"), "exactly the keys"),
    (lambda o: o.pop("signature"), "exactly the keys"),
    (lambda o: o.update(schema="other/1"), "unknown schema"),
    (lambda o: o.update(signature=o["signature"].upper()), "lowercase hex"),
    (lambda o: o.update(length=True), "length"),
])
def test_an_unsound_published_file_is_refused_by_name(tmp_path, keys, mutate, match):
    sign, verify = keys
    record = anchor_record(ledger_of(2).checkpoint(), sign)
    mutate(record)
    write_raw(tmp_path, "00000000000000000002.json", record)
    with pytest.raises(AnchorError, match=match):
        read_anchor(tmp_path, verify=verify)


def test_a_file_named_for_another_length_is_refused(tmp_path, keys):
    sign, verify = keys
    write_raw(tmp_path, "00000000000000000009.json", anchor_record(ledger_of(2).checkpoint(), sign))
    with pytest.raises(AnchorError, match="does not match its length"):
        read_anchor(tmp_path, verify=verify)


def test_a_non_canonical_file_and_a_stray_file_are_refused(tmp_path, keys):
    sign, verify = keys
    write_raw(tmp_path, "00000000000000000002.json", anchor_record(ledger_of(2).checkpoint(), sign), canonical=False)
    with pytest.raises(AnchorError, match="canonical"):
        read_anchor(tmp_path, verify=verify)
    os.remove(tmp_path / "checkpoints" / "00000000000000000002.json")
    (tmp_path / "checkpoints" / "notes.txt").write_text("hi")
    with pytest.raises(AnchorError, match="notes.txt"):
        read_anchor(tmp_path, verify=verify)


def test_dot_files_never_count(tmp_path, keys):
    sign, verify = keys
    publish_checkpoint(tmp_path, ledger_of(1).checkpoint(), sign=sign, verify=verify)
    (tmp_path / "checkpoints" / ".gitkeep").write_text("")
    (tmp_path / "checkpoints" / ".publish-x.tmp").write_text("garbage")
    assert [cp.length for cp in read_anchor(tmp_path, verify=verify)] == [1]


def test_a_planted_huge_checkpoint_signed_by_another_key_blocks_nothing_quietly(tmp_path, keys):
    sign, verify = keys
    attacker_sign, _ = pair()
    write_raw(tmp_path, f"{10**9:020d}.json", anchor_record(LedgerCheckpoint(10**9, "d" * 64), attacker_sign))
    with pytest.raises(AnchorError, match="does not verify"):
        publish_checkpoint(tmp_path, ledger_of(1).checkpoint(), sign=sign, verify=verify)


# Checking a ledger against the anchor.

def anchored(tmp_path, keys, ledger, *lengths):
    sign, verify = keys
    for k in lengths:
        publish_checkpoint(tmp_path, cp_from(ledger, k), sign=sign, verify=verify, history=ledger)
    return verify


def test_ok_when_the_newest_published_checkpoint_is_the_ledger_s_head(tmp_path, keys):
    ledger = ledger_of(4)
    verify = anchored(tmp_path, keys, ledger, 2, 4)
    report = check_against_anchor(ledger, tmp_path, verify=verify)
    assert report == {"status": "OK", "published": 2, "anchored_length": 4, "ledger_length": 4, "unwitnessed": 0}


def test_unwitnessed_tail_when_the_ledger_grew_past_the_anchor(tmp_path, keys):
    ledger = ledger_of(5)
    verify = anchored(tmp_path, keys, ledger, 3)
    report = check_against_anchor(ledger, tmp_path, verify=verify)
    assert report["status"] == "UNWITNESSED_TAIL" and report["unwitnessed"] == 2


def test_lost_when_the_ledger_is_shorter_than_what_was_published(tmp_path, keys):
    verify = anchored(tmp_path, keys, ledger_of(5), 2, 5)
    report = check_against_anchor(ledger_of(3), tmp_path, verify=verify)
    assert report["status"] == "LOST" and report["anchored_length"] == 5 and report["ledger_length"] == 3


def test_diverged_when_a_witnessed_entry_was_rewritten_and_rechained(tmp_path, keys):
    from store_support import ctx_n, make_decision
    verify = anchored(tmp_path, keys, ledger_of(4), 2, 4)
    rewritten = DecisionLedger(require_context=True)
    rewritten.append(make_decision(0), context=ctx_n(1))
    rewritten.append(make_decision(77), context=ctx_n(2))  # entry 2 rewritten, chain recomputed
    rewritten.append(make_decision(2), context=ctx_n(3))
    rewritten.append(make_decision(3), context=ctx_n(4))
    assert rewritten.verify_chain_integrity()
    report = check_against_anchor(rewritten, tmp_path, verify=verify)
    assert report["status"] == "DIVERGED" and report["first_diverged"] == 2


def test_diverged_when_the_chain_does_not_verify(tmp_path, keys, monkeypatch):
    ledger = ledger_of(2)
    verify = anchored(tmp_path, keys, ledger, 2)
    monkeypatch.setattr(DecisionLedger, "verify_chain_integrity", lambda self: False)
    assert check_against_anchor(ledger, tmp_path, verify=verify)["status"] == "DIVERGED"


def test_no_anchor_when_nothing_is_published(tmp_path, keys):
    report = check_against_anchor(ledger_of(2), tmp_path, verify=keys[1])
    assert report == {"status": "NO_ANCHOR", "published": 0, "anchored_length": 0, "ledger_length": 2}


def test_an_unsound_anchor_is_an_error_not_a_status(tmp_path, keys):
    sign, verify = keys
    write_raw(tmp_path, "00000000000000000001.json", anchor_record(ledger_of(1).checkpoint(), pair()[0]))
    with pytest.raises(AnchorError):
        check_against_anchor(ledger_of(1), tmp_path, verify=verify)


def test_an_export_is_checked_after_rebuilding_it(tmp_path, keys):
    ledger = ledger_of(3)
    verify = anchored(tmp_path, keys, ledger, 3)
    rebuilt = DecisionLedger.from_export(json.loads(json.dumps(ledger.export())))
    assert check_against_anchor(rebuilt, tmp_path, verify=verify)["status"] == "OK"


def test_only_an_exact_ledger_is_checked(tmp_path, keys):
    class Sneaky(DecisionLedger):
        pass

    with pytest.raises(TypeError):
        check_against_anchor(Sneaky(), tmp_path, verify=keys[1])
