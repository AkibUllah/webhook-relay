"""HMAC-SHA256 request signatures.

    X-Relay-Signature: t=<unix seconds>,v1=<hex hmac of "<t>.<raw body>">

Consumers verify with `verify_signature(secret, body, header)`; the timestamp is part of the
signed string so a captured request cannot be replayed outside the tolerance window.
"""

from __future__ import annotations

import hashlib
import hmac
import secrets
import time

SIGNATURE_HEADER = "X-Relay-Signature"


class SignatureError(Exception):
    """Raised by `verify_signature` when the header is missing, stale or wrong."""


def generate_secret(prefix: str = "whsec_") -> str:
    return prefix + secrets.token_urlsafe(32)


def compute_signature(secret: str, timestamp: int, body: bytes) -> str:
    signed = f"{timestamp}.".encode() + body
    return hmac.new(secret.encode("utf-8"), signed, hashlib.sha256).hexdigest()


def build_signature_header(secret: str, body: bytes, timestamp: int | None = None) -> str:
    ts = int(time.time()) if timestamp is None else timestamp
    return f"t={ts},v1={compute_signature(secret, ts, body)}"


def parse_signature_header(header: str) -> tuple[int, list[str]]:
    timestamp: int | None = None
    signatures: list[str] = []
    for part in header.split(","):
        key, sep, value = part.strip().partition("=")
        if not sep:
            raise SignatureError("malformed signature header")
        if key == "t":
            try:
                timestamp = int(value)
            except ValueError as exc:
                raise SignatureError("malformed timestamp") from exc
        elif key == "v1":
            signatures.append(value)
    if timestamp is None or not signatures:
        raise SignatureError("signature header needs t= and v1=")
    return timestamp, signatures


def verify_signature(
    secret: str,
    body: bytes,
    header: str | None,
    tolerance_seconds: int = 300,
    now: int | None = None,
) -> None:
    if not header:
        raise SignatureError("missing signature header")
    timestamp, candidates = parse_signature_header(header)
    current = int(time.time()) if now is None else now
    if abs(current - timestamp) > tolerance_seconds:
        raise SignatureError("timestamp outside tolerance")
    expected = compute_signature(secret, timestamp, body)
    if not any(hmac.compare_digest(expected, c) for c in candidates):
        raise SignatureError("signature mismatch")
