"""CheckpointStore: a separate file, replaced atomically, only ever forward, optionally signed.

Ed25519 from `cryptography` appears here only as the deployer's signer and verifier.
"""
import json
import os

import pytest
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from store_support import ctx_n, make_decision

from delta import DecisionLedger, LedgerCheckpoint
from delta.store import CHECKPOINT_SIGNATURE_DOMAIN, CheckpointStore, StoreError


def cp_at(n):
    ledger = DecisionLedger(require_context=True)
    for i in range(n):
        ledger.append(make_decision(i), context=ctx_n(i + 1))
    return ledger.checkpoint()


def diverged_at(n):
    ledger = DecisionLedger(require_context=True)
    for i in range(n):
        ledger.append(make_decision(i + 100), context=ctx_n(i + 1))
    return ledger.checkpoint()


@pytest.fixture
def keys():
    key = Ed25519PrivateKey.generate()
    pub = key.public_key()

    def verify(data, signature):
        try:
            pub.verify(signature, data)
        except InvalidSignature:
            return False
        return True

    return key.sign, verify


def test_write_then_read_round_trips_a_typed_checkpoint(tmp_path):
    store = CheckpointStore(tmp_path / "cp" / "checkpoint.json")
    cp = cp_at(3)
    store.write(cp)
    got = store.read()
    assert type(got) is LedgerCheckpoint and got == cp and got.length == 3


def test_a_crash_between_the_temp_write_and_the_replace_leaves_the_old_checkpoint(tmp_path, monkeypatch):
    store = CheckpointStore(tmp_path / "checkpoint.json")
    store.write(cp_at(2))
    before = open(store.path, "rb").read()

    def crash(src, dst):
        raise OSError("crash before replace")

    monkeypatch.setattr(os, "replace", crash)
    with pytest.raises(OSError):
        store.write(cp_at(4))
    monkeypatch.undo()
    assert open(store.path, "rb").read() == before and store.read() == cp_at(2)
    assert os.listdir(tmp_path) == ["checkpoint.json"]  # the temp file is not left behind


def test_the_write_fsyncs_the_temp_file_and_then_the_directory(tmp_path, monkeypatch):
    import stat
    kinds = []
    real = os.fsync

    def spy(fd):
        kinds.append("dir" if stat.S_ISDIR(os.fstat(fd).st_mode) else "file")
        return real(fd)

    monkeypatch.setattr(os, "fsync", spy)
    CheckpointStore(tmp_path / "checkpoint.json").write(cp_at(1))
    assert kinds == ["file", "dir"]


def test_the_replace_happens_between_the_file_fsync_and_the_directory_fsync(tmp_path, monkeypatch):
    order = []
    real_fsync, real_replace = os.fsync, os.replace
    monkeypatch.setattr(os, "fsync", lambda fd: (order.append("fsync"), real_fsync(fd))[1])
    monkeypatch.setattr(os, "replace", lambda a, b: (order.append("replace"), real_replace(a, b))[1])
    CheckpointStore(tmp_path / "checkpoint.json").write(cp_at(1))
    assert order == ["fsync", "replace", "fsync"]


def test_forward_only_a_lower_length_is_refused(tmp_path):
    store = CheckpointStore(tmp_path / "checkpoint.json")
    store.write(cp_at(3))
    with pytest.raises(StoreError, match="lower"):
        store.write(cp_at(2))
    assert store.read() == cp_at(3)


def test_forward_only_the_same_length_with_a_different_head_is_refused(tmp_path):
    store = CheckpointStore(tmp_path / "checkpoint.json")
    store.write(cp_at(3))
    with pytest.raises(StoreError, match="head"):
        store.write(diverged_at(3))
    assert store.read() == cp_at(3)


def test_forward_only_the_same_value_and_a_longer_one_are_accepted(tmp_path):
    store = CheckpointStore(tmp_path / "checkpoint.json")
    store.write(cp_at(3))
    store.write(cp_at(3))
    store.write(cp_at(5))
    assert store.read() == cp_at(5)


def test_an_empty_ledger_checkpoint_and_a_non_checkpoint_are_refused(tmp_path):
    store = CheckpointStore(tmp_path / "checkpoint.json")
    with pytest.raises(StoreError):
        store.write(DecisionLedger().checkpoint())
    with pytest.raises(TypeError):
        store.write({"length": 1, "head": "0" * 64})
    assert not os.path.exists(store.path)


def test_a_missing_file_reads_as_an_error_and_exists_says_so(tmp_path):
    store = CheckpointStore(tmp_path / "checkpoint.json")
    assert store.exists() is False
    with pytest.raises(StoreError, match="no checkpoint"):
        store.read()
    store.write(cp_at(1))
    assert store.exists() is True


GOOD = {"v": 1, "length": 2, "head": "a" * 64}


@pytest.mark.parametrize("content", [
    b"", b"not json", b"[]", b"null", b'{"v":1,"length":2}',
    json.dumps({**GOOD, "extra": 1}).encode(),
    json.dumps({**GOOD, "length": "2"}).encode(),
    json.dumps({**GOOD, "length": True}).encode(),
    json.dumps({**GOOD, "length": -1}).encode(),
    json.dumps({**GOOD, "length": 0}).encode(),
    json.dumps({**GOOD, "length": 2.0}).encode(),
    json.dumps({**GOOD, "head": "A" * 64}).encode(),
    json.dumps({**GOOD, "head": "a" * 63}).encode(),
    json.dumps({**GOOD, "head": 5}).encode(),
    json.dumps({**GOOD, "v": 2}).encode(),
    json.dumps({**GOOD, "v": True}).encode(),
    json.dumps({**GOOD, "signature": 5}).encode(),
    json.dumps({**GOOD, "signature": ""}).encode(),
    json.dumps({**GOOD, "signature": "zz"}).encode(),
    b'{"v":1,"length":2,"length":2,"head":"' + b"a" * 64 + b'"}',
    b'{"v":1,"length":NaN,"head":"' + b"a" * 64 + b'"}',
    b"\xff\xfe",
])
def test_a_corrupt_or_mistyped_file_is_refused_on_read(tmp_path, content):
    store = CheckpointStore(tmp_path / "checkpoint.json")
    with open(store.path, "wb") as fh:
        fh.write(content)
    with pytest.raises(StoreError):
        store.read()


def test_a_corrupt_existing_file_is_not_silently_overwritten(tmp_path):
    store = CheckpointStore(tmp_path / "checkpoint.json")
    with open(store.path, "wb") as fh:
        fh.write(b"garbage")
    with pytest.raises(StoreError):
        store.write(cp_at(2))
    assert open(store.path, "rb").read() == b"garbage"


def test_signed_write_and_verified_read_with_a_real_signer(tmp_path, keys):
    sign, verify = keys
    store = CheckpointStore(tmp_path / "checkpoint.json")
    cp = cp_at(3)
    store.write(cp, sign=sign)
    assert store.read(verify=verify) == cp
    assert store.read() == cp  # reading without a verifier ignores the signature


def test_the_signature_is_over_the_domain_tag_and_the_canonical_length_and_head(tmp_path):
    seen = []
    cp = cp_at(2)
    CheckpointStore(tmp_path / "checkpoint.json").write(cp, sign=lambda data: seen.append(data) or b"sig")
    assert CHECKPOINT_SIGNATURE_DOMAIN == b"delta-ledger-checkpoint-signature/1\n"
    body = json.dumps({"length": cp.length, "head": cp.head}, sort_keys=True, separators=(",", ":")).encode()
    assert seen == [CHECKPOINT_SIGNATURE_DOMAIN + body]


def test_a_tampered_checkpoint_or_signature_is_refused(tmp_path, keys):
    sign, verify = keys
    store = CheckpointStore(tmp_path / "checkpoint.json")
    store.write(cp_at(3), sign=sign)
    original = json.loads(open(store.path).read())
    other = cp_at(4)
    forged = [
        {**original, "length": other.length, "head": other.head},
        {**original, "length": 2},
        {**original, "head": "b" * 64},
        {**original, "signature": original["signature"][:-2] + ("00" if original["signature"][-2:] != "00" else "11")},
        {**original, "signature": original["signature"][:-2]},
    ]
    for obj in forged:
        with open(store.path, "w") as fh:
            fh.write(json.dumps(obj))
        with pytest.raises(StoreError, match="signature"):
            store.read(verify=verify)


def test_a_signature_from_another_key_is_refused(tmp_path, keys):
    sign, _ = keys
    other = Ed25519PrivateKey.generate().public_key()

    def verify_other(data, signature):
        try:
            other.verify(signature, data)
        except InvalidSignature:
            return False
        return True

    store = CheckpointStore(tmp_path / "checkpoint.json")
    store.write(cp_at(2), sign=sign)
    with pytest.raises(StoreError, match="signature"):
        store.read(verify=verify_other)


def test_an_unsigned_file_is_refused_when_a_verifier_is_given(tmp_path, keys):
    _, verify = keys
    store = CheckpointStore(tmp_path / "checkpoint.json")
    store.write(cp_at(2))
    with pytest.raises(StoreError, match="signature"):
        store.read(verify=verify)


@pytest.mark.parametrize("answer", [1, "yes", None, 0])
def test_a_verifier_that_does_not_answer_exactly_true_is_refused(tmp_path, keys, answer):
    sign, _ = keys
    store = CheckpointStore(tmp_path / "checkpoint.json")
    store.write(cp_at(2), sign=sign)
    with pytest.raises(StoreError, match="signature"):
        store.read(verify=lambda d, s: answer)


def test_a_verifier_that_raises_is_refused_not_propagated(tmp_path, keys):
    sign, _ = keys
    store = CheckpointStore(tmp_path / "checkpoint.json")
    store.write(cp_at(2), sign=sign)

    def boom(d, s):
        raise RuntimeError("kms down")

    with pytest.raises(StoreError, match="signature"):
        store.read(verify=boom)


@pytest.mark.parametrize("bad", ["str", b"", None, bytearray(b"x")])
def test_a_signer_returning_anything_but_non_empty_bytes_is_refused_and_nothing_is_written(tmp_path, bad):
    store = CheckpointStore(tmp_path / "checkpoint.json")
    with pytest.raises((TypeError, ValueError)):
        store.write(cp_at(2), sign=lambda data: bad)
    assert not os.path.exists(store.path)


def test_a_signed_write_that_is_the_same_value_replaces_an_unsigned_file(tmp_path, keys):
    sign, verify = keys
    store = CheckpointStore(tmp_path / "checkpoint.json")
    store.write(cp_at(2))
    store.write(cp_at(2), sign=sign)
    assert store.read(verify=verify) == cp_at(2)
