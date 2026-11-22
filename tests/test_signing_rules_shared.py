"""delta, zeta and beta keep their own copies of one set of signing rules, and the copies agree byte for byte.

The rules: a domain label ``<repo>-<object>-signature/<version>`` plus a newline, then canonical ASCII JSON (sorted keys,
no spaces, ``ensure_ascii``, no NaN), with every string checked as strict UTF-8 first. delta is where the rules started;
zeta (lock results) and beta (its own checker for zeta's lock results) copied them. This test reads all three copies.
"""
import inspect
import json
import re
from datetime import datetime

import pytest

import beta.signing as beta_signing
import zeta.signing as zeta_signing
from zeta import Key, KeySet, LockEvaluator, LockRegistry, LockSpec

from delta import signing as delta_signing
from delta.store import CHECKPOINT_SIGNATURE_DOMAIN, _canonical

DOMAIN_SHAPE = re.compile(rb"^[a-z]+-[a-z]+(-[a-z]+)*-signature/[0-9]+\n$")


def canonical(obj):
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def test_the_three_strict_utf8_checks_are_one_function_character_for_character():
    ours = inspect.getsource(delta_signing.strict_utf8)
    assert inspect.getsource(zeta_signing.strict_utf8) == ours
    assert inspect.getsource(beta_signing.strict_utf8) == ours


def test_every_domain_label_has_the_one_shape_and_no_two_collide():
    labels = [delta_signing.RECEIPT_SIGNATURE_DOMAIN, CHECKPOINT_SIGNATURE_DOMAIN,
              zeta_signing.LOCK_RESULT_SIGNATURE_DOMAIN]
    assert all(DOMAIN_SHAPE.match(label) for label in labels)
    assert len(set(labels)) == len(labels)
    assert beta_signing.LOCK_RESULT_SIGNATURE_DOMAIN == zeta_signing.LOCK_RESULT_SIGNATURE_DOMAIN


def test_delta_s_canonical_form_is_the_one_the_lock_result_rules_use():
    obj = {"b": ["café", "\U0001f600"], "a": True, "c": 3}
    assert _canonical(obj) == canonical(obj)
    registry = LockRegistry([LockSpec(lock_id="l", required_keys=("k",))])
    result = LockEvaluator(registry).evaluate(
        "p1", KeySet([Key(name="k", present=True, reason="café \U0001f600")]), "l", datetime(2026, 1, 1))
    data = zeta_signing.lock_result_signing_input(result)
    domain = zeta_signing.LOCK_RESULT_SIGNATURE_DOMAIN
    assert data[len(domain):] == _canonical(json.loads(data[len(domain):])).encode("ascii")
    assert beta_signing.lock_result_signing_input(result) == data


@pytest.mark.parametrize("module", [delta_signing, zeta_signing, beta_signing])
def test_each_copy_refuses_a_surrogate_the_same_way(module):
    with pytest.raises(ValueError, match="does not encode as strict UTF-8"):
        module.strict_utf8("field", chr(0xD83D) + chr(0xDE00))  # the surrogate pair that spells U+1F600
    module.strict_utf8("field", "\U0001f600")
