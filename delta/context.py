"""The pipeline context delta records and binds: one run, named the same way by every stage.

A plain mapping of four non-empty strings, validated here and nowhere else in delta. CNS publishes the contract
(cns_composition.pipeline_context) and holds alpha's, zeta's, beta's and delta's copies of this check to it, so
they cannot drift. This module imports nothing from CNS.
"""

import re
from collections.abc import Mapping
from typing import Any, Dict

CONTEXT_SCHEMA = "spine-context/1"
_CONTEXT_FIELDS = ("schema", "entity_id", "event_id", "run_id")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


def check_context(context: Any) -> Dict[str, str]:
    """A validated copy of a pipeline context: exactly the four fields, all non-empty strings, a known schema."""
    if not isinstance(context, Mapping):
        raise TypeError(f"a pipeline context must be a mapping; got {type(context).__name__}")
    if set(context) != set(_CONTEXT_FIELDS):
        raise ValueError(f"a pipeline context has exactly the fields {list(_CONTEXT_FIELDS)}; got {sorted(map(str, context))}")
    for name in _CONTEXT_FIELDS:
        if not isinstance(context[name], str):
            raise TypeError(f"context field {name} must be a str; got {type(context[name]).__name__}")
        if not context[name]:
            raise ValueError(f"context field {name} must be non-empty")
    if context["schema"] != CONTEXT_SCHEMA:
        raise ValueError(f"unsupported context schema {context['schema']!r}; this connector speaks {CONTEXT_SCHEMA!r}")
    return {name: context[name] for name in _CONTEXT_FIELDS}


def check_digest(value: Any, what: str) -> str:
    """``value`` if it is a full lowercase SHA-256 digest; a TypeError or ValueError otherwise."""
    if not isinstance(value, str):
        raise TypeError(f"{what} must be a str digest; got {type(value).__name__}")
    if not _SHA256.match(value):
        raise ValueError(f"{what} is not a full lowercase SHA-256 digest")
    return value
