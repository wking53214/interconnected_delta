"""Optional, binding-only connector to CNS (``cns.gate``).

delta is independent. It has no runtime dependency on CNS, imports nothing from
CNS when it loads, and its whole suite passes with CNS absent. This module is
the one place that knows CNS exists, and it asks for CNS only when one of its
functions is called. Without CNS installed those calls raise
:class:`CnsNotInstalled` with the install command; nothing else in delta is
affected.

What this binds
---------------
CNS's shared gate contract is ``cns.gate.GateResult``. A result that records a
``subject`` and a ``subject_digest`` can later be checked against the content it
was issued for: ``GateResult.binds(subject, digest)`` is true only when the
verdict was issued against exactly that content. This module makes delta's two
primary records bindable in that way, so that a CNS verdict about a record can
be checked against the record:

* a :class:`delta.ledger.LedgerEntry`: the recorded ``beta.Decision`` together
  with its place in the chain (``audit_id``, ``previous_hash``,
  ``immutable_hash``) and the time it was recorded;
* a :class:`delta.obligation.OutcomeObligation`: what a decision still owes,
  in whatever state it is in (open, resolved or abandoned).

Binding only
------------
This is deliberately not a gate and it invents no verdict. A ledger entry is a
custody record and an obligation is a lifecycle state; neither is a pass or a
breach, and mapping them to ``PASS``, ``RETRY`` or ``TERMINAL_BREACH`` would
assign them a meaning delta does not have. Whoever issues a verdict about a
record owns that meaning; this module only lets them say which record it was
about. Fairness screening (``RegulatoryFinding``) is not bound: it is a
screening signal, not a legal determination, and is derived from obligations
that can be bound.

Separate from delta's own hash chain
------------------------------------
The ledger's ``immutable_hash`` chain and the obligation's
``decision_fingerprint`` link are untouched by this module and unaware of it.
The digest here is an additional, separate binding:
``cns.gate.subject_digest`` over the record as an output. It does not verify
the chain (use ``DecisionLedger.verify_chain_integrity`` for that); it binds a
verdict to the entry exactly as stored, chain hashes included.

What the content holds, and what it leaves out
----------------------------------------------
A ledger entry binds every field of the entry and of its decision, except the
decision's ``audit_hash`` (a pointer into a ledger, never part of what was
decided). This module deliberately does not reuse beta's connector or the
ledger's private canonical form: beta may be installed without a connector, and
the ledger's hashed subset is its own business. The decision's set-like tuples
(``triggered_by_locks``, ``triggered_by_keys``, ``newly_triggered_locks``) are
sorted so order cannot change the digest; ``reversal_conditions`` keeps its
authored order. An obligation binds every field. Its free-form ``resolved_value``
and ``detail`` mappings are copied as given (sets become sorted lists, tuples
become lists, keys must be strings); values CNS cannot describe raise
``TypeError``. Fields typed as floats (confidence and the obligation's times)
are rendered as floats, so ``1`` and ``1.0`` bind alike (CNS tells an int from
a float); inside the free-form mappings values are bound as given. Aware
timestamps are rendered in UTC; naive and aware stay distinct. A NaN or
infinite number cannot be bound and raises ``TypeError`` from CNS.

Install with the extra:
``pip install 'delta[cns] @ git+https://github.com/wking53214/interconnected_delta'``.
"""

import importlib
from collections.abc import Mapping
from datetime import datetime, timezone
from types import ModuleType
from typing import Any, Dict, Optional

from beta import Decision

from .ledger import LedgerEntry
from .obligation import OutcomeObligation

__all__ = [
    "CnsNotInstalled",
    "ENTRY_SUBJECT",
    "OBLIGATION_SUBJECT",
    "binds",
    "cns_available",
    "entry_content",
    "entry_digest",
    "obligation_content",
    "obligation_digest",
]

INSTALL_HINT = "pip install 'delta[cns] @ git+https://github.com/wking53214/interconnected_delta'"

ENTRY_SUBJECT = "delta.ledger_entry"
OBLIGATION_SUBJECT = "delta.outcome_obligation"


class CnsNotInstalled(ImportError):
    """Raised by this module's functions when ``cns`` cannot be imported."""


def _cns_gate() -> ModuleType:
    """Import ``cns.gate`` on demand, or say exactly what is missing."""
    try:
        return importlib.import_module("cns.gate")
    except ImportError as exc:
        raise CnsNotInstalled(
            "delta.cns_connector needs the CNS package (cns.gate), which is not "
            f"installed. Install it with: {INSTALL_HINT}. delta itself works "
            "without it."
        ) from exc


def cns_available() -> bool:
    """Whether the CNS gate contract can be imported in this environment."""
    try:
        _cns_gate()
    except CnsNotInstalled:
        return False
    return True


def _instant(value: Any, label: str) -> str:
    """ISO text of a timestamp; an aware one is rendered in UTC."""
    if not isinstance(value, datetime):
        raise TypeError(f"{label} must be a datetime; got {type(value).__name__}")
    if value.tzinfo is not None and value.utcoffset() is not None:
        value = value.astimezone(timezone.utc)
    return value.isoformat()


def _number(value: Any, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{label} must be a number; got {type(value).__name__}")
    return float(value)


def _maybe_number(value: Any, label: str) -> Optional[float]:
    return None if value is None else _number(value, label)


def _plain(value: Any, label: str) -> Any:
    """A free-form value as content CNS accepts: str-keyed dicts and lists of scalars."""
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Mapping):
        out = {}  # type: Dict[str, Any]
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError(f"{label} has a key that is not a str: {key!r}")
            out[key] = _plain(item, label)
        return out
    if isinstance(value, (list, tuple)):
        return [_plain(item, label) for item in value]
    if isinstance(value, (set, frozenset)):
        return sorted((_plain(item, label) for item in value), key=repr)
    raise TypeError(f"{label} holds a {type(value).__name__}, which cannot be bound")


def _decision_content(decision: Any) -> Dict[str, Any]:
    if not isinstance(decision, Decision):
        raise TypeError(f"a ledger entry must hold a beta.Decision; got {type(decision).__name__}")
    return {
        "entity_id": decision.entity_id,
        "decision_id": decision.decision_id,
        "decision": decision.decision,
        "timestamp": _instant(decision.timestamp, "a Decision timestamp"),
        "confidence": _number(decision.confidence, "a Decision confidence"),
        "triggered_by_locks": sorted(decision.triggered_by_locks),
        "triggered_by_keys": sorted(decision.triggered_by_keys),
        "newly_triggered_locks": sorted(decision.newly_triggered_locks),
        "decision_fingerprint": decision.decision_fingerprint,
        "reasoning": decision.reasoning,
        "reversal_conditions": list(decision.reversal_conditions),
        "instructions": decision.instructions,
    }


def entry_content(entry: LedgerEntry) -> Dict[str, Any]:
    """The deterministic content a CNS verdict about ``entry`` is bound to.

    The recorded decision, the time it was recorded and its place in the chain.
    Only str, int, float, bool, None, list and dict with str keys, the types
    ``cns.gate.subject_digest`` accepts. Needs no CNS to build. Raises
    ``TypeError`` for anything that is not a :class:`delta.ledger.LedgerEntry`.
    """
    if not isinstance(entry, LedgerEntry):
        raise TypeError(f"expected a delta.ledger.LedgerEntry; got {type(entry).__name__}")
    return {
        "audit_id": entry.audit_id,
        "timestamp": _instant(entry.timestamp, "a ledger entry timestamp"),
        "previous_hash": entry.previous_hash,
        "immutable_hash": entry.immutable_hash,
        "decision": _decision_content(entry.decision),
    }


def obligation_content(obligation: OutcomeObligation) -> Dict[str, Any]:
    """The deterministic content a CNS verdict about ``obligation`` is bound to.

    Every field of the obligation. Needs no CNS to build. Raises ``TypeError``
    for anything that is not a :class:`delta.obligation.OutcomeObligation`.
    """
    if not isinstance(obligation, OutcomeObligation):
        raise TypeError(f"expected a delta.obligation.OutcomeObligation; got {type(obligation).__name__}")
    return {
        "obligation_id": obligation.obligation_id,
        "decision_fingerprint": obligation.decision_fingerprint,
        "domain": obligation.domain,
        "obligation_kind": obligation.obligation_kind,
        "opened_at": _number(obligation.opened_at, "opened_at"),
        "expected_by": _number(obligation.expected_by, "expected_by"),
        "state": obligation.state,
        "reason_code": obligation.reason_code,
        "resolved_at": _maybe_number(obligation.resolved_at, "resolved_at"),
        "resolved_value": _plain(obligation.resolved_value, "resolved_value"),
        "resolution_provenance": obligation.resolution_provenance,
        "resolution_method": obligation.resolution_method,
        "favorable": obligation.favorable,
        "subject_id": obligation.subject_id,
        "detail": _plain(obligation.detail, "detail"),
    }


def entry_digest(entry: LedgerEntry) -> str:
    """``cns.gate.subject_digest`` of :func:`entry_content`. Pass it, with ``ENTRY_SUBJECT``, to ``GateResult.binds``."""
    content = entry_content(entry)
    return _cns_gate().subject_digest(content)


def obligation_digest(obligation: OutcomeObligation) -> str:
    """``cns.gate.subject_digest`` of :func:`obligation_content`. Pass it, with ``OBLIGATION_SUBJECT``, to ``GateResult.binds``."""
    content = obligation_content(obligation)
    return _cns_gate().subject_digest(content)


def binds(result: Any, record: Any, *, subject: Optional[str] = None) -> bool:
    """Whether ``result`` (a ``cns.gate.GateResult``) was issued against exactly ``record``.

    ``record`` is a ``LedgerEntry`` (default subject ``ENTRY_SUBJECT``) or an
    ``OutcomeObligation`` (default ``OBLIGATION_SUBJECT``); anything else is a
    ``TypeError``. False for a different record, for a different ``subject``
    label, and for a result that records no subject or digest (an unbound or
    hand-built one). Says nothing about what the verdict was.
    """
    gate = _cns_gate()
    if not isinstance(result, gate.GateResult):
        raise TypeError(f"expected a cns.gate.GateResult; got {type(result).__name__}")
    if isinstance(record, LedgerEntry):
        default, digest = ENTRY_SUBJECT, entry_digest(record)
    elif isinstance(record, OutcomeObligation):
        default, digest = OBLIGATION_SUBJECT, obligation_digest(record)
    else:
        raise TypeError(
            f"expected a delta.ledger.LedgerEntry or delta.obligation.OutcomeObligation; "
            f"got {type(record).__name__}")
    return bool(result.binds(default if subject is None else subject, digest))
