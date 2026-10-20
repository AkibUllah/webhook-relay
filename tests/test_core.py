from datetime import timedelta

import pytest

from tests.conftest import T0
from webhook_relay.core.backoff import (
    DEFAULT_SCHEDULE,
    apply_jitter,
    delay_for_attempt,
    describe_schedule,
    next_attempt_at,
    parse_schedule,
)
from webhook_relay.core.circuit import Circuit, CircuitState
from webhook_relay.core.matching import is_valid_event_type, is_valid_filter, matches, matches_any
from webhook_relay.core.signing import (
    SignatureError,
    build_signature_header,
    generate_secret,
    verify_signature,
)


def test_backoff_schedule_matches_spec():
    assert DEFAULT_SCHEDULE == (10, 60, 300, 1800, 7200, 43200)
    assert describe_schedule(DEFAULT_SCHEDULE) == "10s, 1m, 5m, 30m, 2h, 12h"
    assert [delay_for_attempt(n) for n in range(1, 7)] == [10, 60, 300, 1800, 7200, 43200]
    assert delay_for_attempt(99) == 43200  # last value repeats
    with pytest.raises(ValueError):
        delay_for_attempt(0)


def test_parse_schedule():
    assert parse_schedule("10, 60,300") == (10, 60, 300)
    assert parse_schedule([5, 6]) == (5, 6)
    for bad in ("", "0,10", "-1"):
        with pytest.raises(ValueError):
            parse_schedule(bad)


def test_jitter_bounds_and_next_attempt():
    assert apply_jitter(100, 0.0) == 100
    assert apply_jitter(100, 0.1, rng=lambda: 0.0) == pytest.approx(90)
    assert apply_jitter(100, 0.1, rng=lambda: 1.0) == pytest.approx(110)
    assert apply_jitter(100, 0.1, rng=lambda: 0.5) == pytest.approx(100)
    with pytest.raises(ValueError):
        apply_jitter(1, 1.0)
    assert next_attempt_at(T0, 2, jitter_fraction=0) == T0 + timedelta(seconds=60)


def test_signature_roundtrip_and_failures():
    secret = generate_secret()
    assert secret.startswith("whsec_")
    body = b'{"id":"evt_1"}'
    header = build_signature_header(secret, body, timestamp=1_800_000_000)
    verify_signature(secret, body, header, now=1_800_000_100)
    with pytest.raises(SignatureError, match="mismatch"):
        verify_signature(secret, b'{"id":"evt_2"}', header, now=1_800_000_100)
    with pytest.raises(SignatureError, match="tolerance"):
        verify_signature(secret, body, header, now=1_800_000_301)
    with pytest.raises(SignatureError):
        verify_signature(secret, body, None)
    with pytest.raises(SignatureError):
        verify_signature(secret, body, "t=x,v1=00", now=1)
    with pytest.raises(SignatureError):
        verify_signature(secret, body, "nonsense", now=1)


def test_circuit_opens_after_threshold_and_recovers():
    c = Circuit()
    c = c.record_failure(3, T0)
    c = c.record_failure(3, T0)
    assert c.state is CircuitState.CLOSED and c.consecutive_failures == 2
    c = c.record_failure(3, T0)
    assert c.state is CircuitState.OPEN and c.opened_at == T0

    allowed, c = c.allows_request(timedelta(seconds=120), T0 + timedelta(seconds=30))
    assert not allowed and c.state is CircuitState.OPEN
    assert c.retry_after(timedelta(seconds=120), T0 + timedelta(seconds=30)) == 90

    allowed, c = c.allows_request(timedelta(seconds=120), T0 + timedelta(seconds=120))
    assert allowed and c.state is CircuitState.HALF_OPEN

    reopened = c.record_failure(3, T0 + timedelta(seconds=121))
    assert reopened.state is CircuitState.OPEN  # one failure in half-open re-opens
    assert reopened.consecutive_failures == 4

    closed = c.record_success()
    assert closed == Circuit()
    assert closed.retry_after(timedelta(seconds=120), T0) is None


def test_event_matching_rules():
    assert matches("payment.captured", "payment.captured")
    assert matches("payment.captured", "payment.*")
    assert matches("payment", "payment.*")
    assert matches("payment.refund.created", "payment.*")
    assert not matches("payments.captured", "payment.*")
    assert matches("anything.at_all", "*")
    assert matches_any("order.paid", [])  # no filters -> everything
    assert matches_any("order.paid", ["payment.*", "order.*"])
    assert not matches_any("order.paid", ["payment.*"])


def test_event_type_and_filter_validation():
    assert is_valid_event_type("payment.captured")
    assert is_valid_event_type("kyc.applicant.approved")
    assert not is_valid_event_type("Payment.Captured")
    assert not is_valid_event_type("payment..captured")
    assert not is_valid_event_type("payment captured")
    assert is_valid_filter("*") and is_valid_filter("payment.*") and is_valid_filter("order.paid")
    assert not is_valid_filter("*.paid") and not is_valid_filter("payment.*.x")
