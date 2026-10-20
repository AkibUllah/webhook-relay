"""Retry schedule: 10s, 1m, 5m, 30m, 2h, 12h with +/- jitter so retries do not synchronise."""

from __future__ import annotations

import random
from collections.abc import Callable, Sequence
from datetime import datetime, timedelta

DEFAULT_SCHEDULE: tuple[int, ...] = (10, 60, 300, 1800, 7200, 43200)


def parse_schedule(value: str | Sequence[int]) -> tuple[int, ...]:
    if isinstance(value, str):
        parts = [p.strip() for p in value.split(",") if p.strip()]
        schedule = tuple(int(p) for p in parts)
    else:
        schedule = tuple(int(v) for v in value)
    if not schedule or any(s <= 0 for s in schedule):
        raise ValueError("backoff schedule must contain positive seconds")
    return schedule


def delay_for_attempt(failed_attempts: int, schedule: Sequence[int] = DEFAULT_SCHEDULE) -> int:
    """Seconds to wait after `failed_attempts` failures (1-based); the last value repeats."""
    if failed_attempts < 1:
        raise ValueError("failed_attempts must be >= 1")
    index = min(failed_attempts, len(schedule)) - 1
    return int(schedule[index])


def apply_jitter(
    seconds: float, fraction: float = 0.1, rng: Callable[[], float] = random.random
) -> float:
    """Spread `seconds` uniformly within +/- fraction. fraction=0 disables jitter."""
    if not 0 <= fraction < 1:
        raise ValueError("jitter fraction must be in [0, 1)")
    if fraction == 0:
        return float(seconds)
    offset = (rng() * 2 - 1) * fraction
    return float(seconds) * (1 + offset)


def next_attempt_at(
    now: datetime,
    failed_attempts: int,
    schedule: Sequence[int] = DEFAULT_SCHEDULE,
    jitter_fraction: float = 0.1,
    rng: Callable[[], float] = random.random,
) -> datetime:
    delay = apply_jitter(delay_for_attempt(failed_attempts, schedule), jitter_fraction, rng)
    return now + timedelta(seconds=delay)


def describe_schedule(schedule: Sequence[int]) -> str:
    def fmt(seconds: int) -> str:
        for unit, size in (("h", 3600), ("m", 60)):
            if seconds % size == 0 and seconds >= size:
                return f"{seconds // size}{unit}"
        return f"{seconds}s"

    return ", ".join(fmt(s) for s in schedule)
