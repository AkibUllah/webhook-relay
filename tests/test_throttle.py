from datetime import timedelta

import pytest

from tests.conftest import T0
from webhook_relay.core.throttle import TokenBucket


def _full(rate: float) -> TokenBucket:
    return TokenBucket.restore(rate, None, None).refill(T0)


def _drain(bucket: TokenBucket) -> tuple[int, TokenBucket]:
    taken = 0
    while True:
        allowed, bucket = bucket.take()
        if not allowed:
            return taken, bucket
        taken += 1


def test_a_new_bucket_starts_full_with_one_seconds_worth():
    assert _drain(_full(5))[0] == 5
    assert _full(5).capacity == 5


@pytest.mark.parametrize("rate", [0.1, 0.5, 1])
def test_slow_limits_hold_one_token(rate):
    bucket = _full(rate)
    assert bucket.capacity == 1
    assert _drain(bucket)[0] == 1


def test_refill_earns_tokens_over_time_up_to_capacity():
    _, empty = _drain(_full(4))
    half_second = empty.refill(T0 + timedelta(seconds=0.5))
    assert half_second.tokens == pytest.approx(2)
    assert half_second.refill(T0 + timedelta(hours=1)).tokens == 4


def test_refill_after_exactly_one_tokens_wait_allows_a_request():
    _, empty = _drain(_full(3))
    wait = timedelta(seconds=empty.wait_seconds())
    allowed, _ = empty.refill(T0 + wait).take()
    assert allowed


def test_a_clock_running_behind_earns_nothing():
    _, empty = _drain(_full(2))
    behind = empty.refill(T0 - timedelta(seconds=30))
    assert behind.tokens == 0
    assert behind.updated_at == T0


def test_lowering_the_rate_trims_saved_tokens():
    restored = TokenBucket.restore(2, 50.0, T0)
    assert restored.refill(T0).tokens == 2
    assert TokenBucket.restore(2, 50.0, None).refill(T0).tokens == 2


def test_wait_seconds_spaces_the_queue_at_the_rate():
    _, empty = _drain(_full(2))
    assert empty.wait_seconds(1) == 0.5
    assert empty.wait_seconds(2) == 1.0
    assert empty.wait_seconds(5) == 2.5
    assert TokenBucket(rate=2, tokens=1.5).wait_seconds(1) == 0


def test_requests_stay_within_capacity_plus_rate_over_any_window():
    bucket = _full(10)
    sent = 0
    for tick in range(301):  # thirty seconds in 100ms steps, always asking for more
        moment = T0 + timedelta(milliseconds=100 * tick)
        taken, bucket = _drain(bucket.refill(moment))
        sent += taken
    assert sent == pytest.approx(10 + 10 * 30, abs=1)
