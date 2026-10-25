# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- CodeQL code scanning workflow and Dependabot configuration (pip, Docker, GitHub Actions).
- API reference and terminal screenshots in the README.
- Roadmap tracked as GitHub issues under the `v0.2.0` milestone.

### Fixed

- Package version (`pyproject.toml`, `webhook_relay.__version__`, `/health`) now reports 0.2.0 to match the released tag.

## [0.2.0] - 2026-10-25

### Added

- Due deliveries are claimed with `FOR UPDATE OF deliveries SKIP LOCKED`, so several worker replicas can share one PostgreSQL queue without blocking on or double-sending the same rows.
- Each claim records `claimed_by` and a `claimed_until` lease; deliveries held by a crashed worker are picked up again once the lease expires and counted as `reclaimed` in `/metrics`.
- Settings `WORKER_ID` and `CLAIM_LEASE_SECONDS`.
- Delivery responses expose the current claim owner and lease; replay clears a stale claim.
- CI job running the claiming suite against a real PostgreSQL service, including an exactly-once test with two workers.

### Upgrading

Existing databases need two new columns:

```sql
ALTER TABLE deliveries ADD COLUMN claimed_by VARCHAR(128);
ALTER TABLE deliveries ADD COLUMN claimed_until TIMESTAMPTZ;
CREATE INDEX ix_deliveries_claimed_until ON deliveries (claimed_until);
```

## [0.1.0] - 2026-10-25

First public release.

### Added

- Endpoint registry with generated signing secrets, event-type filters (`payment.*`, `*`), active flag and secret rotation.
- `POST /v1/events` fan-out to matching endpoints; client-supplied event ids are idempotent.
- Delivery worker with a configurable backoff schedule (10s → 1m → 5m → 30m → 2h → 12h), jitter, max attempts and dead-lettering.
- `X-Relay-Signature: t=<ts>,v1=<hmac-sha256>` plus event/delivery/attempt headers, and a `verify_signature` helper for receivers.
- Per-endpoint circuit breaker with half-open probe and manual reset; `410 Gone` deactivates the endpoint.
- Full attempt history and replay of any delivery; dead-letter list and filtered delivery listing.
- Worker in-process or standalone (`python -m webhook_relay.worker`) with graceful shutdown.
- `/health`, JSON `/metrics` and a server-rendered `/dashboard`.
- Docker image, docker compose with PostgreSQL, CI on Python 3.10/3.11, Pages API reference, Codespaces and Render kits.

[Unreleased]: https://github.com/AkibUllah/webhook-relay/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/AkibUllah/webhook-relay/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/AkibUllah/webhook-relay/releases/tag/v0.1.0
