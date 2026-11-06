"""Per-endpoint request rate limit as a token bucket.

The bucket refills at `rate` tokens per second and holds at most one second's worth (never less
than one token, so limits below 1/s still send). One request costs one token, which gives a
sustained `rate` requests per second with short bursts of up to `capacity`: over any T seconds
an endpoint receives at most `capacity + rate * T` requests.

Like `Circuit` this is a pure value object. The dispatcher restores it from the endpoint row
while holding the row lock and writes it back, which is what makes one limit hold across all
workers rather than per process.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime

# Timestamps are stored to the microsecond, so a wait of exactly one token's time can come up a
# hair short after a round trip through the database.
_EPSILON = 1e-6


@dataclass(frozen=True)
class TokenBucket:
    rate: float
    tokens: float
    updated_at: datetime | None = None

    @classmethod
    def restore(cls, rate: float, tokens: float | None, updated_at: datetime | None) -> TokenBucket:
        """Rebuild a stored bucket. A bucket that has never been used starts full."""
        return cls(rate, max(1.0, rate) if tokens is None else tokens, updated_at)

    @property
    def capacity(self) -> float:
        return max(1.0, self.rate)

    def refill(self, now: datetime) -> TokenBucket:
        """Add the tokens earned since the last refill. A clock that stepped backwards, or a
        worker running slightly behind another, earns nothing rather than a negative amount."""
        if self.updated_at is None:
            return replace(self, tokens=min(self.tokens, self.capacity), updated_at=now)
        elapsed = (now - self.updated_at).total_seconds()
        if elapsed <= 0:
            return replace(self, tokens=min(self.tokens, self.capacity))
        tokens = min(self.capacity, self.tokens + elapsed * self.rate)
        return replace(self, tokens=tokens, updated_at=now)

    def take(self) -> tuple[bool, TokenBucket]:
        """Return (allowed, updated bucket), spending a token when one is available."""
        if self.tokens + _EPSILON < 1:
            return False, self
        return True, replace(self, tokens=max(0.0, self.tokens - 1))

    def wait_seconds(self, position: int = 1) -> float:
        """Seconds until the bucket can serve the `position`-th request in line."""
        return max(0.0, (position - self.tokens) / self.rate)
