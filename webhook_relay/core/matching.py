"""Event type filters: exact names, trailing-wildcard prefixes (`payment.*`) or `*` for all."""

from __future__ import annotations

import re
from collections.abc import Iterable

_EVENT_TYPE_RE = re.compile(r"^[a-z0-9_]+(\.[a-z0-9_]+)*$")
_FILTER_RE = re.compile(r"^(\*|[a-z0-9_]+(\.[a-z0-9_]+)*(\.\*)?)$")


def is_valid_event_type(value: str) -> bool:
    return bool(_EVENT_TYPE_RE.match(value))


def is_valid_filter(value: str) -> bool:
    return bool(_FILTER_RE.match(value))


def matches(event_type: str, pattern: str) -> bool:
    if pattern == "*":
        return True
    if pattern.endswith(".*"):
        prefix = pattern[:-2]
        return event_type == prefix or event_type.startswith(prefix + ".")
    return event_type == pattern


def matches_any(event_type: str, patterns: Iterable[str]) -> bool:
    """Empty filter list means "all events"."""
    patterns = list(patterns)
    if not patterns:
        return True
    return any(matches(event_type, p) for p in patterns)
