<!-- topics: fastapi,python,webhooks,reliability,retry,circuit-breaker,hmac,event-driven,sqlalchemy -->
# webhook-relay

A reliable webhook delivery service: publish an event once, and it is fanned out to every subscribed endpoint with HMAC signatures, exponential backoff, per-endpoint circuit breakers, dead-letter handling and one-click replay.

[![CI](https://github.com/AkibUllah/webhook-relay/actions/workflows/ci.yml/badge.svg)](https://github.com/AkibUllah/webhook-relay/actions/workflows/ci.yml)
[![CodeQL](https://github.com/AkibUllah/webhook-relay/actions/workflows/codeql.yml/badge.svg)](https://github.com/AkibUllah/webhook-relay/actions/workflows/codeql.yml)
[![Release](https://img.shields.io/github/v/release/AkibUllah/webhook-relay)](https://github.com/AkibUllah/webhook-relay/releases)
![Python](https://img.shields.io/badge/python-3.10%20%7C%203.11-blue)
![FastAPI](https://img.shields.io/badge/FastAPI-0.110%2B-009688)
![License](https://img.shields.io/badge/license-MIT-green)

## Try it live

| | |
|---|---|
| API reference | https://akibullah.github.io/webhook-relay/ — Swagger UI built from the exported OpenAPI document (static; no backend behind it) |
| Run in your browser | [![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://codespaces.new/AkibUllah/webhook-relay?quickstart=1) — starts API + worker + PostgreSQL with `docker compose` and seeds one endpoint and one event; open forwarded port 8000 at `/dashboard` (admin key `change-me` for `/v1/*`). |
| One-click deploy | [![Deploy to Render](https://render.com/images/deploy-to-render-button.svg)](https://render.com/deploy?repo=https://github.com/AkibUllah/webhook-relay) — `render.yaml` provisions the web service (worker in-process) and a free PostgreSQL; `API_KEY` is generated, read it from the service's environment tab. |
| Local | `docker compose up` (see Quick Start) |

## Screenshots

![Swagger UI API reference](docs/images/api-reference.png)

Live API reference on GitHub Pages: endpoint registry, event publishing, deliveries and dead letters.

![curl session against a local relay](docs/images/terminal-demo.png)

A real run against a local relay with a short backoff schedule: one event fans out to a healthy receiver (delivered first try) and an offline one (retried, then dead-lettered); duplicate publish is a no-op; metrics at the end.

## Why this exists

Every payment and e-commerce platform I have worked on had to tell other systems that something happened: a bKash payment was captured, an order shipped, a KYC applicant was approved. The first version is always a `requests.post` in the request handler, and it breaks the first time the receiver is down for a deploy. Then retries get bolted on, then someone asks "did merchant X receive event Y, and can we send it again?", and nobody can answer. This service is that answer extracted into one place: durable deliveries with an attempt history, a retry schedule that backs off sensibly, a breaker that stops hammering a dead endpoint, and a dashboard that shows what is stuck.

## Features

- Endpoint registry: URL, signing secret (generated if omitted, shown once), description, event-type filters (`payment.*`, `order.paid`, `*`), active flag, secret rotation
- `POST /v1/events` fans out to every active endpoint whose filters match; client-supplied event ids are idempotent (re-publishing is a 200 no-op)
- Delivery worker with the schedule 10s → 1m → 5m → 30m → 2h → 12h (configurable), ±jitter, configurable max attempts, dead-letter after exhaustion
- `X-Relay-Signature: t=<ts>,v1=<hmac-sha256>` on every request plus event/delivery/attempt headers; a `verify_signature` helper for receivers
- Per-endpoint circuit breaker: opens after N consecutive failures, blocks sends for a cooldown, half-open probe, auto-close on success, manual reset endpoint; `410 Gone` deactivates the endpoint
- At-least-once semantics with full attempt history (status code, response excerpt, error, latency) and replay of any delivery, including succeeded ones
- Worker runs inside the API process (asyncio task) or as a separate process via `python -m webhook_relay.worker`, with graceful shutdown
- Horizontally scalable workers on PostgreSQL: due rows are claimed with `FOR UPDATE SKIP LOCKED`, so replicas get disjoint batches, and each claim carries a lease (`claimed_by` / `claimed_until`) so deliveries held by a crashed worker are picked up again automatically
- Per-endpoint limits: optional `max_rps` and `max_in_flight` hold across all workers because they are enforced in PostgreSQL at claim time. Throttled deliveries wait in `pending` and never use up a retry attempt
- Isolated endpoints: every endpoint gets its own lane and an even share of each batch, so one slow receiver or one burst cannot hold up the others
- Retention: with `RETENTION_DAYS` set, succeeded deliveries older than the cutoff are deleted with their attempts, plus events left without deliveries. Dead letters and pending or in-flight rows are never touched. The worker purges hourly in bounded batches, with `SKIP LOCKED` on PostgreSQL so it never waits on the worker or a replay; `python -m webhook_relay.worker --purge-once` runs it from cron instead
- Dead-letter list, delivery filtering by status/endpoint/event, `X-Total-Count` pagination headers
- `/health`, `/metrics` (JSON counters: deliveries by status, success rate, open circuits, worker stats) and a server-rendered `/dashboard`
- Admin API key, structured error envelope, SQLite for dev/tests and PostgreSQL via docker compose

## Architecture

```mermaid
flowchart LR
    P[Producer service] -- "POST /v1/events {id, type, data}" --> API[FastAPI]
    API --> EV[(events)]
    API -- "match filters" --> FAN[fan-out]
    FAN --> D1[(delivery A)]
    FAN --> D2[(delivery B)]
    W[Dispatcher worker<br/>poll due deliveries] --> D1 & D2
    W -- "circuit check" --> CB{breaker}
    CB -- closed / half-open --> LIM{limits}
    CB -- open --> WAIT[stay pending until cooldown]
    LIM -- within max_rps and max_in_flight --> SEND[signed POST<br/>one lane per endpoint]
    LIM -- over --> HOLD[stay pending, no attempt used]
    SEND -- 2xx --> OK[succeeded]
    SEND -- error --> RETRY[attempt++ → next_attempt_at<br/>10s,1m,5m,30m,2h,12h]
    RETRY -- max attempts --> DLQ[dead letter]
    DLQ -- "POST /replay" --> D1
    OK & RETRY --> ATT[(delivery_attempts)]
    DASH[/dashboard, /metrics/] --> EV & D1 & ATT
```

Design notes:

- **Deliveries are rows, not tasks.** A delivery is created in the same transaction as the event, so a crash after `201 Created` cannot lose a notification. The worker simply queries `status='pending' AND next_attempt_at <= now`.
- **Claims are leased, not owned forever.** Claiming locks due rows with `FOR UPDATE OF deliveries SKIP LOCKED`; concurrent workers skip rows another transaction holds instead of waiting or double-sending. Each claimed delivery records the worker id and a lease deadline (by default long enough for a whole batch to time out). If a worker is killed mid-batch, its `delivering` rows become due again when the lease expires, are counted as `reclaimed` in `/metrics`, and are redelivered, which is consistent with at-least-once semantics.
- **Limits live in the database, not the worker.** A token bucket in worker memory would give each replica its own allowance. Instead a claim for a limited endpoint locks the endpoint row, counts live `delivering` rows and spends tokens stored on that row, all in one short transaction, so two workers cannot both take the last slot. Workers pass over a locked endpoint with `SKIP LOCKED` and `FOR NO KEY UPDATE` leaves event fan-out unblocked. Endpoints without limits take no lock.
- **A slow endpoint only delays itself.** Claiming hands each endpoint with due work an even share of the batch, with unused slots rolling over, and each endpoint's deliveries run on their own lane. The worker claims again while a lane is busy, so new work for other endpoints waits one poll interval at most. No database connection is held during a request.
- **Time is injected.** The dispatcher takes a `clock`; tests advance it to walk through the whole retry schedule and the circuit cooldown without sleeping.
- **The breaker is per endpoint and pure.** `core.circuit.Circuit` is an immutable value object; the dispatcher reads it from the endpoint row, applies success/failure, writes it back. Blocked deliveries stay pending with `next_attempt_at` pushed to the cooldown boundary so the loop does not spin.
- **Signatures cover the timestamp.** The signed string is `"<t>.<body>"`, and receivers check `t` against a tolerance window, so a captured request cannot be replayed later.
- **Replay keeps history.** Replaying resets the attempt counter but keeps every previous attempt row and increments `replay_count`, so an audit trail survives operator action.

## Tech Stack

| Layer | Choice |
| --- | --- |
| API | FastAPI, Pydantic v2, Jinja2 dashboard |
| Persistence | SQLAlchemy 2.0; SQLite (dev/tests), PostgreSQL 16 (compose, psycopg 3) |
| Worker | asyncio loop + httpx.AsyncClient with injectable transport |
| Config | pydantic-settings / `.env` |
| Tooling | pytest + pytest-asyncio, ruff, Makefile, GitHub Actions (3.10/3.11 + docker build) |

## Quick Start

### Docker Compose (API + dedicated worker + PostgreSQL)

```bash
git clone https://github.com/AkibUllah/webhook-relay.git && cd webhook-relay
cp .env.example .env
docker compose up --build -d
curl -s localhost:8000/health
open http://localhost:8000/dashboard
```

### Local (SQLite, worker inside the API)

```bash
make install
make dev                       # http://127.0.0.1:8000, docs at /docs
```

Register an endpoint, publish an event, watch it deliver:

```bash
export KEY=change-me   # API_KEY from .env (leave API_KEY empty to disable auth locally)

curl -s -X POST localhost:8000/v1/endpoints -H "X-API-Key: $KEY" -H "Content-Type: application/json" \
  -d '{"url":"https://webhook.site/your-id","event_types":["payment.*"],"description":"Shop backend"}'

curl -s -X POST localhost:8000/v1/events -H "X-API-Key: $KEY" -H "Content-Type: application/json" \
  -d '{"id":"evt-order-4521","type":"payment.captured","data":{"payment_id":"pay_1","amount_minor":150000,"currency":"BDT"}}'

curl -s "localhost:8000/v1/deliveries?status=dead" -H "X-API-Key: $KEY"
```

Verify a delivery on the receiving side:

```python
from webhook_relay.core.signing import verify_signature, SignatureError

try:
    verify_signature(
        secret, request.body, request.headers["X-Relay-Signature"], tolerance_seconds=300
    )
except SignatureError:
    return 400
```

## API

All `/v1` routes require `X-API-Key` when `API_KEY` is set. Errors are `{"error": {"code", "message"}}`.

| Method | Path | Purpose |
| --- | --- | --- |
| `POST` | `/v1/endpoints` | Register an endpoint (`url`, optional `secret`, `event_types`, `description`, `max_rps`, `max_in_flight`); returns the secret once |
| `GET` | `/v1/endpoints` · `/v1/endpoints/{id}` | List / fetch (secret never returned) |
| `PATCH` | `/v1/endpoints/{id}` | Update URL, filters, description, `is_active`, `max_rps`, `max_in_flight` (`null` removes a limit); `rotate_secret: true` returns a new secret |
| `DELETE` | `/v1/endpoints/{id}` | Deactivate (delivery history kept) |
| `POST` | `/v1/endpoints/{id}/reset-circuit` | Close an open breaker manually |
| `POST` | `/v1/events` | Publish; `201` with deliveries, or `200` + `duplicate: true` for a known `id` |
| `GET` | `/v1/events` · `/v1/events/{id}` | List (filter `type`) / fetch with deliveries |
| `GET` | `/v1/deliveries` | Filter by `status`, `endpoint_id`, `event_id`; includes attempts |
| `GET` | `/v1/deliveries/{id}` | One delivery with full attempt history |
| `POST` | `/v1/deliveries/{id}/replay` | Re-queue for immediate send (`202`) |
| `GET` | `/v1/dead-letters` | Deliveries that exhausted `MAX_ATTEMPTS` |
| `GET` | `/health` | DB check, worker state |
| `GET` | `/metrics` | JSON counters |
| `GET` | `/dashboard` | HTML status page (auto-refresh) |

Request headers sent to endpoints: `X-Relay-Signature`, `X-Relay-Event`, `X-Relay-Event-Id`, `X-Relay-Delivery-Id`, `X-Relay-Attempt`, `X-Relay-Timestamp`. Body:

```json
{"id":"evt-order-4521","type":"payment.captured","created_at":"2026-10-21T09:00:00+00:00",
 "delivery_id":"dlv_…","attempt":1,"data":{"payment_id":"pay_1","amount_minor":150000,"currency":"BDT"}}
```

## Project Structure

```
webhook_relay/
├── main.py               # app factory, lifespan starts the worker task
├── worker.py             # python -m webhook_relay.worker
├── config.py · database.py · models.py · schemas.py
├── core/
│   ├── backoff.py        # schedule, jitter, next_attempt_at
│   ├── signing.py        # X-Relay-Signature build/verify
│   ├── circuit.py        # Circuit value object (closed/open/half_open)
│   ├── throttle.py       # TokenBucket value object behind max_rps
│   └── matching.py       # event-type filters
├── services/
│   ├── events.py         # publish + fan-out, idempotent ids
│   ├── endpoints.py · deliveries.py (replay, dead letters)
│   ├── dispatcher.py     # the worker: fair claim, limits, lanes, record, retry, breaker, hourly purge
│   ├── retention.py      # batched deletion of old succeeded deliveries and orphaned events
│   └── metrics.py
├── api/                  # endpoints, events, deliveries, ops (+ dashboard)
└── templates/dashboard.html
tests/                    # 119 tests; MockTransport targets, injected clock
```

## Scaling workers

Run as many worker processes as you need against the same PostgreSQL database:

```bash
docker compose up -d --scale worker=3
```

| Setting | Default | Meaning |
|---|---|---|
| `WORKER_ID` | `<hostname>-<pid>` | Recorded on each claimed delivery as `claimed_by` |
| `CLAIM_LEASE_SECONDS` | `BATCH_SIZE × HTTP_TIMEOUT_SECONDS + 60` | How long a claim is reserved before another worker may take it over |

Upgrading an existing database from 0.1 (tables are created with `create_all`, which does not alter existing tables):

```sql
ALTER TABLE deliveries ADD COLUMN claimed_by VARCHAR(128);
ALTER TABLE deliveries ADD COLUMN claimed_until TIMESTAMPTZ;
CREATE INDEX ix_deliveries_claimed_until ON deliveries (claimed_until);
```

## Rate limits and concurrency caps

Protect a receiver that can only take so much. Both limits are optional, set per endpoint, and shared by all workers:

```bash
curl -s -X PATCH localhost:8000/v1/endpoints/$ID -H "X-API-Key: $KEY" -H "Content-Type: application/json" \
  -d '{"max_rps": 5, "max_in_flight": 2}'
```

| Field | Range | Meaning |
|---|---|---|
| `max_rps` | above 0 up to 10000, fractions allowed | Sustained requests per second. One second's worth can go out as a burst, so over any T seconds an endpoint receives at most `max(1, max_rps) + max_rps × T` requests. `0.5` means one every two seconds |
| `max_in_flight` | 1 to 10000 | Requests open at once. A lane sends this many in parallel; without the setting it sends one at a time |

In a `PATCH`, leaving a field out keeps it and `null` removes it. With neither set, claiming and delivery work as they always have, apart from the lanes and batch sharing below.

Waiting is not failing. A delivery over `max_rps` stays `pending` and its `next_attempt_at` moves to the slot the bucket can serve it in, spaced at the allowed rate. One over `max_in_flight` stays due and untouched until a slot frees up. Neither sends a request, writes an attempt, changes `attempt_count` or counts toward `MAX_ATTEMPTS`, however long the wait. Retries and replays go through the same limits. Rate-limited endpoints claim in creation order, so a held delivery keeps its place in a backlog.

Things to plan for:

- Only as many rows as fit in the endpoint's share of a batch are rescheduled in one round, so in a large backlog younger rows can still show an old `next_attempt_at` until they are reached.
- A slot under `max_in_flight` is held until the attempt is recorded. If a worker dies its slots come back when the claim lease runs out, so a low cap with the default lease can stall for minutes; set `CLAIM_LEASE_SECONDS` lower if that matters.
- Tokens are spent at claim time using each worker's clock. Keep workers on NTP. A worker running behind earns no extra tokens.
- The limits rely on row locks, so they are global on PostgreSQL. SQLite has no row locks: run a single worker there, as with claiming.

| Setting | Default | Meaning |
|---|---|---|
| `DISPATCH_CONCURRENCY` | `20` | Requests one worker keeps open at once across all endpoints. Lower it to protect the worker, raise it when many endpoints are slow at the same time |

These appear in `/metrics`. Like everything under `worker`, the two skip counters belong to the process that runs the loop: with a dedicated worker the API reports zero and the worker logs its totals on exit.

| Metric | Meaning |
|---|---|
| `endpoints_limited` | Endpoints with at least one limit set |
| `worker.skipped_rate_limited` | Deliveries held back by `max_rps` |
| `worker.skipped_in_flight` | Times an endpoint was passed over because `max_in_flight` was reached |

Existing databases need the new columns and index (tables are created with `create_all`, which does not alter existing tables):

```sql
ALTER TABLE endpoints ADD COLUMN max_rps DOUBLE PRECISION;
ALTER TABLE endpoints ADD COLUMN max_in_flight INTEGER;
ALTER TABLE endpoints ADD COLUMN rate_tokens DOUBLE PRECISION;
ALTER TABLE endpoints ADD COLUMN rate_updated_at TIMESTAMPTZ;
CREATE INDEX ix_deliveries_endpoint_status_next ON deliveries (endpoint_id, status, next_attempt_at);
```

## Retention

Every delivery and its attempts are kept forever unless you set `RETENTION_DAYS`. On a busy relay `delivery_attempts` grows fastest, and old succeeded rows only slow down listings and the dashboard.

| Setting | Default | Meaning |
|---|---|---|
| `RETENTION_DAYS` | unset (keep everything) | Delete succeeded deliveries whose `delivered_at` is older than this, with their attempts, and events older than this that have no deliveries left |
| `RETENTION_BATCH_SIZE` | `2000` | Rows deleted per transaction |
| `RETENTION_INTERVAL_SECONDS` | `3600` | How often the worker loop purges |

What is never purged: dead-lettered deliveries (they wait for a replay or a human), `pending` and `delivering` rows, and any event that still has one of those. A delivery replayed while a purge runs is skipped: on PostgreSQL candidate rows are locked with `SKIP LOCKED`, and every DELETE re-checks the conditions. Purged counts appear under `worker.retention` in `/metrics`.

To schedule it outside the worker (for example from cron while workers run with `RETENTION_DAYS` unset):

```bash
python -m webhook_relay.worker --purge-once            # uses RETENTION_DAYS
python -m webhook_relay.worker --purge-once --days 30  # one-off override
# {"cutoff": "2026-10-03T02:00:00+00:00", "deliveries": 1840, "attempts": 2311, "events": 1840, "batches": 3}
```

Once an event is purged its id can be published again and will fan out as new; keep `RETENTION_DAYS` longer than any window in which your producers retry with the same event id.

Existing databases also need the index the purge query uses:

```sql
CREATE INDEX ix_deliveries_status_delivered_at ON deliveries (status, delivered_at);
```

## Testing

```bash
make test
make lint
```

Covered: the retry schedule and jitter bounds; signature build/verify including tampering, stale timestamps and malformed headers; every circuit-breaker transition; filter matching and validation; endpoint CRUD with secret reveal/rotation; fan-out to matching active endpoints only; idempotent event ids; delivery listing/filters; the dispatcher end to end (signed request contents, success path, failure walk through 10s/1m/5m to dead letter, breaker opening and recovering after cooldown, half-open re-open and manual reset, independent fan-out outcomes, replay of dead and succeeded deliveries, `410` deactivation, batch size, inactive endpoints) and the in-process worker lifecycle. Retention: only old succeeded rows go (dead, pending and in-flight stay), the exclusive cutoff, events kept while any delivery remains and removed once orphaned, bounded batches, a replayed delivery surviving the purge, the hourly gate in the worker loop and its `/metrics` counters, `--purge-once` and its arguments, and the query plan using the new index.

Claiming has its own suite: lease defaults, owner/lease recording and release, recovery of deliveries abandoned by a crashed worker, and two PostgreSQL-only tests that prove concurrent claimers receive disjoint batches and that two workers deliver every event exactly once. A third PostgreSQL test shows the purge skipping a row another transaction holds locked.

Limits and isolation: the token bucket on its own, in-flight caps, spaced rate-limited rows that use no attempt however long they wait, retries paying for tokens, limits shared between workers, fair batch sharing, lanes that keep a slow endpoint from delaying the others in a batch and across claims, and handing back unsent rows at shutdown. On PostgreSQL, worker threads with separate connections race for the same endpoint and the cap and bucket still come out exact, including end to end, without blocking event fan-out.

The PostgreSQL tests run in CI against a real service and locally with:

```bash
docker run -d --rm --name relay-pg -e POSTGRES_USER=relay -e POSTGRES_PASSWORD=relay -p 55432:5432 postgres:16-alpine
TEST_DATABASE_URL=postgresql://relay:relay@localhost:55432/relay pytest tests/test_claiming.py tests/test_retention.py tests/test_throttling_postgres.py -k postgres
```

The other tests use an in-memory SQLite engine, an `httpx.MockTransport` as the receiving side and an injected clock, so they run in a couple of seconds with no network.

## Roadmap

- [ ] Alembic migrations (schema is created with `create_all` today) (#4)
- [ ] Optional OpenTelemetry spans around each attempt (#5)

## License

MIT — see [LICENSE](LICENSE).
