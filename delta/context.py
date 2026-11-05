"""The pipeline context delta records and binds: one run, named the same way by every stage.

A plain mapping of four non-empty strings, validated here and nowhere else in delta. CNS publishes the contract
(cns_composition.spine_trace.context_problems) and its integration runner holds alpha's, zeta's, beta's and delta's
copies of this check to it, so they cannot drift unnoticed. This module imports nothing from CNS.
"""

import re
from collections.abc import Mapping
from typing import Any, Dict

CONTEXT_SCHEMA = "spine-context/1"
_CONTEXT_FIELDS = ("schema", "entity_id", "event_id", "run_id")
_SHA256 = re.compile(r"[0-9a-f]{64}")


def check_context(context: Any) -> Dict[str, str]:
    """A validated snapshot of a pipeline context: exactly the four fields, all non-empty plain strings, a known schema.

    The mapping is read once (``dict(context)``) and the snapshot is what is validated and returned, so a mapping that
    answers differently on a later read cannot change what the caller goes on to record. Every value must be exactly
    ``str``, not a subclass, because a subclass can lie about its own equality, hash or text.
    """
    if not isinstance(context, Mapping):
        raise TypeError(f"a pipeline context must be a mapping; got {type(context).__name__}")
    snapshot = dict(context)
    if set(snapshot) != set(_CONTEXT_FIELDS):
        raise ValueError(f"a pipeline context has exactly the fields {list(_CONTEXT_FIELDS)}; got {sorted(map(str, snapshot))}")
    for name in _CONTEXT_FIELDS:
        if type(snapshot[name]) is not str:
            raise TypeError(f"context field {name} must be a str; got {type(snapshot[name]).__name__}")
        if not snapshot[name]:
            raise ValueError(f"context field {name} must be non-empty")
    if snapshot["schema"] != CONTEXT_SCHEMA:
        raise ValueError(f"unsupported context schema {snapshot['schema']!r}; this connector speaks {CONTEXT_SCHEMA!r}")
    return snapshot


def check_digest(value: Any, what: str) -> str:
    """``value`` if it is a full lowercase SHA-256 digest; a TypeError or ValueError otherwise."""
    if not isinstance(value, str):
        raise TypeError(f"{what} must be a str digest; got {type(value).__name__}")
    if not _SHA256.fullmatch(value):
        raise ValueError(f"{what} is not a full lowercase SHA-256 digest")
    return value
