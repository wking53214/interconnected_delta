"""The receipt signing hook: delta builds the bytes to sign and calls what it is handed; it never holds a key.

`cryptography` Ed25519 is used here ONLY as the deployer's signer and verifier. Needs cns (DELTA_REQUIRE_CNS=1 in CI)
because the receipts are real ones from verification_receipt.
"""
import copy
import dataclasses
import os
import random
from datetime import datetime

import pytest

if os.environ.get("DELTA_REQUIRE_CNS") == "1":
    import cns.gate as gate
else:
    gate = pytest.importorskip("cns.gate")

from cryptography.exceptions import InvalidSignature  # noqa: E402
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey  # noqa: E402

from beta import Decision, content_fingerprint, decision_fingerprint  # noqa: E402

import delta  # noqa: E402
from delta import DecisionLedger  # noqa: E402
from delta.cns_connector import verification_receipt  # noqa: E402
from delta.context import CONTEXT_SCHEMA  # noqa: E402
from delta.signing import (  # noqa: E402
    RECEIPT_SIGNATURE_DOMAIN,
    receipt_signing_input,
    sign_receipt,
    verify_receipt_signature,
)

T0 = datetime(2026, 1, 1, 12, 0, 0)
CTX = {"schema": CONTEXT_SCHEMA, "entity_id": "p1", "event_id": "ev-1", "run_id": "run-1"}
DD = gate.subject_digest({"label": "beta decision digest"})
KEYS = ("schema", "entry_digest", "index", "entry_hash", "previous_hash", "ledger_length", "ledger_head",
        "checkpoint_length", "checkpoint_head", "context")


def decision(i=0):
    base = dict(entity_id="p1", decision_id="r", decision="ESCALATE", timestamp=T0, confidence=0.9,
                triggered_by_locks=("sepsis_lock",), triggered_by_keys=(f"k{i}",))
    base["decision_fingerprint"] = decision_fingerprint({
        "entity_id": "p1", "decision_id": "r", "decision": "ESCALATE",
        "triggered_by_locks": ["sepsis_lock"], "triggered_by_keys": [f"k{i}"]})
    d = Decision(**base)
    return dataclasses.replace(d, content_fingerprint=content_fingerprint(d))


@pytest.fixture
def receipt():
    ledger = DecisionLedger(require_context=True)
    ledger.append(decision(0), context=CTX)
    return verification_receipt(ledger, ledger.checkpoint(), context=CTX, decision_digest=DD)


@pytest.fixture
def keypair():
    key = Ed25519PrivateKey.generate()
    pub = key.public_key()

    def verify(data, signature):
        try:
            pub.verify(signature, data)
        except InvalidSignature:
            return False
        return True

    return key.sign, verify


def test_the_hook_is_exported_from_the_package():
    for name in ("receipt_signing_input", "sign_receipt", "verify_receipt_signature"):
        assert name in delta.__all__ and getattr(delta, name) is getattr(delta.signing, name)


def test_round_trip_over_a_real_receipt(receipt, keypair):
    sign, verify = keypair
    assert set(receipt) == set(KEYS)
    signature = sign_receipt(receipt, sign)
    assert type(signature) is bytes and len(signature) == 64
    assert verify_receipt_signature(receipt, signature, verify) is True


def test_the_signing_input_starts_with_the_domain_tag_and_is_ascii_canonical_json(receipt):
    data = receipt_signing_input(receipt)
    assert RECEIPT_SIGNATURE_DOMAIN == b"delta-ledger-receipt-signature/1\n"
    assert type(data) is bytes and data.startswith(RECEIPT_SIGNATURE_DOMAIN)
    body = data[len(RECEIPT_SIGNATURE_DOMAIN):]
    import json
    assert body == json.dumps(receipt, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode("ascii")


def test_the_signing_input_is_deterministic_whatever_the_dict_order(receipt):
    items = list(receipt.items())
    random.Random(7).shuffle(items)
    shuffled = dict(items)
    ctx_items = list(receipt["context"].items())
    ctx_items.reverse()
    shuffled["context"] = dict(ctx_items)
    assert list(shuffled) != list(receipt) or list(shuffled["context"]) != list(receipt["context"])
    assert receipt_signing_input(shuffled) == receipt_signing_input(receipt)


@pytest.mark.parametrize("field", KEYS)
def test_a_one_field_change_invalidates_the_signature(receipt, keypair, field):
    sign, verify = keypair
    signature = sign_receipt(receipt, sign)
    changed = copy.deepcopy(receipt)
    value = changed[field]
    if field == "context":
        changed["context"]["run_id"] = "run-2"
    elif type(value) is int:
        changed[field] = value + 1
    else:
        changed[field] = value[:-1] + ("0" if value[-1] != "0" else "1")
    assert changed != receipt
    assert verify_receipt_signature(changed, signature, verify) is False


@pytest.mark.parametrize("field", ("schema", "entity_id", "event_id", "run_id"))
def test_a_one_field_change_in_the_context_invalidates_the_signature(receipt, keypair, field):
    sign, verify = keypair
    signature = sign_receipt(receipt, sign)
    changed = copy.deepcopy(receipt)
    changed["context"][field] += "x"
    assert verify_receipt_signature(changed, signature, verify) is False


def test_the_wrong_key_does_not_verify(receipt, keypair):
    sign, _ = keypair
    other = Ed25519PrivateKey.generate().public_key()

    def verify_other(data, signature):
        try:
            other.verify(signature, data)
        except InvalidSignature:
            return False
        return True

    assert verify_receipt_signature(receipt, sign_receipt(receipt, sign), verify_other) is False


def test_a_truncated_or_empty_signature_is_false(receipt, keypair):
    sign, verify = keypair
    signature = sign_receipt(receipt, sign)
    assert verify_receipt_signature(receipt, signature[:-1], verify) is False
    assert verify_receipt_signature(receipt, signature[:10], verify) is False
    assert verify_receipt_signature(receipt, b"", verify) is False


def test_an_empty_signature_never_reaches_the_verifier(receipt):
    calls = []
    assert verify_receipt_signature(receipt, b"", lambda d, s: calls.append(1) or True) is False
    assert calls == []


@pytest.mark.parametrize("bad", ["sig", None, 5, bytearray(b"x" * 64), memoryview(b"x" * 64), [1, 2]])
def test_a_non_bytes_signature_is_false_even_if_the_verifier_would_say_yes(receipt, bad):
    assert verify_receipt_signature(receipt, bad, lambda d, s: True) is False


def test_a_bytes_subclass_signature_is_false(receipt):
    class B(bytes):
        pass

    assert verify_receipt_signature(receipt, B(b"x" * 64), lambda d, s: True) is False


@pytest.mark.parametrize("bad", ["a string", "", None, bytearray(b"abc"), 12])
def test_a_signer_returning_anything_but_non_empty_bytes_is_refused(receipt, bad):
    with pytest.raises((TypeError, ValueError)):
        sign_receipt(receipt, lambda data: bad)


def test_a_signer_returning_empty_bytes_is_refused(receipt):
    with pytest.raises(ValueError):
        sign_receipt(receipt, lambda data: b"")


def test_a_signer_returning_a_bytes_subclass_is_refused(receipt):
    class B(bytes):
        pass

    with pytest.raises(TypeError):
        sign_receipt(receipt, lambda data: B(b"abc"))


def test_a_signer_that_raises_propagates(receipt):
    def boom(data):
        raise RuntimeError("kms down")

    with pytest.raises(RuntimeError):
        sign_receipt(receipt, boom)


def test_the_signer_is_handed_exactly_the_signing_input(receipt):
    seen = []
    sign_receipt(receipt, lambda data: seen.append(data) or b"sig")
    assert seen == [receipt_signing_input(receipt)]


def test_a_signer_or_verifier_that_is_not_callable_is_a_type_error(receipt):
    with pytest.raises(TypeError):
        sign_receipt(receipt, "key")
    with pytest.raises(TypeError):
        verify_receipt_signature(receipt, b"x", "key")


def test_a_verifier_that_raises_yields_false(receipt):
    def boom(data, signature):
        raise RuntimeError("boom")

    assert verify_receipt_signature(receipt, b"x" * 64, boom) is False


@pytest.mark.parametrize("answer", [1, "yes", None, b"ok", [True], 0, False])
def test_a_verifier_answer_that_is_not_exactly_true_is_false(receipt, answer):
    assert verify_receipt_signature(receipt, b"x" * 64, lambda d, s: answer) is False


def test_the_verifier_is_handed_the_signing_input_and_the_signature(receipt):
    seen = []
    verify_receipt_signature(receipt, b"abc", lambda d, s: seen.append((d, s)) or True)
    assert seen == [(receipt_signing_input(receipt), b"abc")]


def test_a_receipt_that_cannot_be_signed_does_not_verify_either(receipt):
    bad = dict(receipt, extra=1)
    assert verify_receipt_signature(bad, b"x" * 64, lambda d, s: True) is False


def test_a_receipt_with_an_extra_key_is_refused(receipt):
    with pytest.raises(ValueError, match="extra"):
        receipt_signing_input({**receipt, "extra": "x"})


def test_a_receipt_missing_a_key_is_refused(receipt):
    short = dict(receipt)
    del short["ledger_head"]
    with pytest.raises(ValueError, match="ledger_head"):
        receipt_signing_input(short)


def test_a_receipt_that_is_not_an_exact_dict_is_refused(receipt):
    class D(dict):
        pass

    with pytest.raises(TypeError):
        receipt_signing_input(D(receipt))
    with pytest.raises(TypeError):
        receipt_signing_input(list(receipt.items()))


@pytest.mark.parametrize("field", ["schema", "entry_digest", "entry_hash", "previous_hash", "ledger_head", "checkpoint_head"])
def test_a_str_subclass_field_is_refused_naming_the_field(receipt, field):
    class S(str):
        pass

    bad = dict(receipt)
    bad[field] = S(receipt[field])
    with pytest.raises(TypeError, match=field):
        receipt_signing_input(bad)


@pytest.mark.parametrize("field", ["index", "ledger_length", "checkpoint_length"])
def test_a_bool_or_non_int_index_field_is_refused_naming_the_field(receipt, field):
    for bad_value in (True, 1.0, "1", None):
        bad = dict(receipt)
        bad[field] = bad_value
        with pytest.raises(TypeError, match=field):
            receipt_signing_input(bad)


def test_an_int_subclass_field_is_refused(receipt):
    class MyInt(int):
        pass

    bad = dict(receipt, index=MyInt(0))
    with pytest.raises(TypeError, match="index"):
        receipt_signing_input(bad)


def test_a_negative_count_is_refused(receipt):
    with pytest.raises(ValueError, match="index"):
        receipt_signing_input(dict(receipt, index=-1))


def test_a_context_str_subclass_is_refused(receipt):
    class S(str):
        pass

    bad = dict(receipt, context={**receipt["context"], "run_id": S("run-1")})
    with pytest.raises(TypeError, match="context"):
        receipt_signing_input(bad)


def test_a_context_that_is_not_an_exact_dict_of_four_is_refused(receipt):
    class D(dict):
        pass

    with pytest.raises(TypeError, match="context"):
        receipt_signing_input(dict(receipt, context=D(receipt["context"])))
    with pytest.raises(ValueError, match="context"):
        receipt_signing_input(dict(receipt, context={**receipt["context"], "extra": "x"}))
    with pytest.raises(ValueError, match="context"):
        receipt_signing_input(dict(receipt, context={"schema": "s"}))
    with pytest.raises(TypeError, match="context"):
        receipt_signing_input(dict(receipt, context={**receipt["context"], "run_id": 3}))


def test_a_non_ascii_value_is_escaped_not_emitted_raw(receipt):
    data = receipt_signing_input(dict(receipt, context={**receipt["context"], "entity_id": "pé"}))
    data.decode("ascii")
    assert b"\\u00e9" in data
