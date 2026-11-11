"""A signing hook for ledger receipts. delta builds the bytes to sign and calls what it is handed; it holds no key.

``sign`` and ``verify`` are callables the deployer supplies (an Ed25519 private key's ``sign``, a KMS wrapper, a
verifier bound to a public key). Key custody, trust, rotation and revocation belong to those callables, not to delta.
This module imports nothing from CNS and nothing cryptographic.
"""
import json
from collections.abc import Callable, Mapping
from typing import Any

RECEIPT_SIGNATURE_DOMAIN = b"delta-ledger-receipt-signature/1\n"

_STR_FIELDS = ("schema", "entry_digest", "entry_hash", "previous_hash", "ledger_head", "checkpoint_head")
_INT_FIELDS = ("index", "ledger_length", "checkpoint_length")
_RECEIPT_KEYS = frozenset(_STR_FIELDS) | frozenset(_INT_FIELDS) | {"context"}
_CONTEXT_KEYS = frozenset({"schema", "entity_id", "event_id", "run_id"})


def receipt_signing_input(receipt: Mapping) -> bytes:
    """The exact bytes a receipt is signed over: the domain tag, then the receipt as canonical ASCII JSON.

    The receipt must be an exact ``dict`` with exactly the ten receipt keys, each of its exact type (``str``; ``int``
    that is neither a bool nor negative; ``context`` an exact ``dict`` of four exact ``str``). Anything else is a
    ``TypeError`` or ``ValueError`` naming the field, so a subclass or a stray key cannot change what is signed.
    """
    if type(receipt) is not dict:
        raise TypeError(f"a receipt is an exact dict; got {type(receipt).__name__}")
    for key in receipt:
        if type(key) is not str:
            raise TypeError(f"a receipt key must be an exact str; got {type(key).__name__}")
    if set(receipt) != _RECEIPT_KEYS:
        extra, missing = sorted(set(receipt) - _RECEIPT_KEYS), sorted(_RECEIPT_KEYS - set(receipt))
        raise ValueError(f"a receipt has exactly the ten keys; extra {extra}, missing {missing}")
    for name in _STR_FIELDS:
        if type(receipt[name]) is not str:
            raise TypeError(f"receipt field {name} must be an exact str; got {type(receipt[name]).__name__}")
    for name in _INT_FIELDS:
        if type(receipt[name]) is not int:
            raise TypeError(f"receipt field {name} must be an exact int; got {type(receipt[name]).__name__}")
        if receipt[name] < 0:
            raise ValueError(f"receipt field {name} must not be negative")
    context = receipt["context"]
    if type(context) is not dict:
        raise TypeError(f"receipt field context must be an exact dict; got {type(context).__name__}")
    if any(type(k) is not str for k in context) or set(context) != _CONTEXT_KEYS:
        raise ValueError(f"receipt field context has exactly the keys {sorted(_CONTEXT_KEYS)}")
    for key, value in context.items():
        if type(value) is not str:
            raise TypeError(f"receipt field context.{key} must be an exact str; got {type(value).__name__}")
    body = json.dumps(receipt, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)
    return RECEIPT_SIGNATURE_DOMAIN + body.encode("ascii")


def sign_receipt(receipt: Mapping, sign: Callable[[bytes], bytes]) -> bytes:
    """Sign a receipt with the deployer's ``sign`` callable (bytes in, bytes out). The result must be exactly a
    non-empty ``bytes``; a ``str``, a subclass or empty bytes is refused. delta never sees a key."""
    if not callable(sign):
        raise TypeError(f"sign must be callable; got {type(sign).__name__}")
    signature: Any = sign(receipt_signing_input(receipt))
    if type(signature) is not bytes:
        raise TypeError(f"the signer must return exactly bytes; got {type(signature).__name__}")
    if not signature:
        raise ValueError("the signer returned an empty signature")
    return signature


def verify_receipt_signature(receipt: Mapping, signature: bytes, verify: Callable[[bytes, bytes], bool]) -> bool:
    """Whether ``verify(signing_input, signature)`` answers exactly ``True``. Any other answer, any exception from
    ``verify``, a signature that is not exactly non-empty ``bytes``, or a receipt that cannot be signed, is ``False``."""
    if not callable(verify):
        raise TypeError(f"verify must be callable; got {type(verify).__name__}")
    if type(signature) is not bytes or not signature:
        return False
    try:
        data = receipt_signing_input(receipt)
    except (TypeError, ValueError):
        return False
    try:
        return verify(data, signature) is True
    except Exception:
        return False
