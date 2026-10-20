"""Per-endpoint circuit breaker.

closed --(N consecutive failures)--> open --(cooldown elapsed)--> half_open
half_open --(success)--> closed            half_open --(failure)--> open

While open, no requests are sent to the endpoint; deliveries stay queued and resume
automatically once the breaker allows a probe request.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from enum import Enum


class CircuitState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass(frozen=True)
class Circuit:
    state: CircuitState = CircuitState.CLOSED
    consecutive_failures: int = 0
    opened_at: datetime | None = None

    def record_success(self) -> Circuit:
        return Circuit(state=CircuitState.CLOSED, consecutive_failures=0, opened_at=None)

    def record_failure(self, threshold: int, now: datetime) -> Circuit:
        failures = self.consecutive_failures + 1
        if self.state is CircuitState.HALF_OPEN or failures >= threshold:
            return Circuit(state=CircuitState.OPEN, consecutive_failures=failures, opened_at=now)
        return replace(self, consecutive_failures=failures)

    def allows_request(self, cooldown: timedelta, now: datetime) -> tuple[bool, Circuit]:
        """Return (allowed, updated circuit). Open breakers move to half-open after cooldown."""
        if self.state is CircuitState.CLOSED:
            return True, self
        if self.state is CircuitState.HALF_OPEN:
            return True, self
        if self.opened_at is None or now - self.opened_at >= cooldown:
            return True, replace(self, state=CircuitState.HALF_OPEN)
        return False, self

    def reset(self) -> Circuit:
        return Circuit()

    def retry_after(self, cooldown: timedelta, now: datetime) -> float | None:
        if self.state is not CircuitState.OPEN or self.opened_at is None:
            return None
        remaining = (self.opened_at + cooldown) - now
        return max(0.0, remaining.total_seconds())
