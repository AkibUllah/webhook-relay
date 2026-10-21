#!/usr/bin/env bash
# Codespaces start hook: bring up API + worker + PostgreSQL and seed a demo endpoint and event.
set -euo pipefail
cd "$(dirname "$0")/.."

docker compose up -d --build

echo "waiting for the API on :8000 ..."
for _ in $(seq 1 60); do
  if curl -fsS http://127.0.0.1:8000/health >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

KEY=${API_KEY:-change-me}   # docker-compose.yml default
if [ ! -f .demo-seeded ]; then
  curl -fsS -X POST http://127.0.0.1:8000/v1/endpoints \
    -H "X-API-Key: $KEY" -H "Content-Type: application/json" \
    -d '{"url":"https://httpbin.org/post","event_types":["payment.*"],"description":"Demo receiver (httpbin echo)"}' \
    >/dev/null
  curl -fsS -X POST http://127.0.0.1:8000/v1/events \
    -H "X-API-Key: $KEY" -H "Content-Type: application/json" \
    -d '{"id":"evt-demo-4521","type":"payment.captured","data":{"payment_id":"pay_demo_1","amount_minor":150000,"currency":"BDT"}}' \
    >/dev/null
  date -u +%FT%TZ > .demo-seeded
fi

cat <<MSG

webhook-relay is up.
  Dashboard:   forwarded port 8000, path /dashboard
  API docs:    forwarded port 8000, path /docs   (X-API-Key: $KEY)
  Seeded:      one endpoint (httpbin.org echo) and one payment.captured event
MSG
