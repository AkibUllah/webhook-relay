from tests.conftest import publish, register


def test_health_and_metrics_and_dashboard(client, auth):
    health = client.get("/health").json()
    assert health["status"] == "ok" and health["database"] == "ok"
    assert health["worker_running"] is False

    metrics = client.get("/metrics").json()
    assert metrics["events_total"] == 0
    assert metrics["deliveries_by_status"] == {
        "pending": 0,
        "delivering": 0,
        "succeeded": 0,
        "dead": 0,
    }

    page = client.get("/dashboard")
    assert page.status_code == 200
    assert "No endpoints registered" in page.text

    register(client, auth, description="Shop backend")
    publish(client, auth)
    page = client.get("/dashboard").text
    assert "Shop backend" in page and "payment.captured" in page


def test_api_key_is_required_on_v1(client):
    assert client.get("/v1/endpoints").status_code == 401
    assert client.get("/v1/endpoints", headers={"X-API-Key": "wrong"}).status_code == 401
    assert client.get("/health").status_code == 200  # ops routes stay open


def test_endpoint_crud_and_secret_handling(client, auth):
    created = register(client, auth, event_types=["payment.*", "Order.Paid"])
    assert created["secret"] == "s3cr3t-s3cr3t-s3cr3t"  # revealed once on create
    assert created["event_types"] == ["order.paid", "payment.*"]  # normalised + sorted
    assert created["circuit_state"] == "closed"

    generated = register(client, auth, path="/other", secret=None)
    assert generated["secret"].startswith("whsec_")

    listed = client.get("/v1/endpoints", headers=auth)
    assert listed.headers["X-Total-Count"] == "2"
    assert all(e["secret"] is None for e in listed.json())
    assert client.get(f"/v1/endpoints/{created['id']}", headers=auth).json()["secret"] is None

    patched = client.patch(
        f"/v1/endpoints/{created['id']}",
        json={"description": "renamed", "event_types": ["*"], "rotate_secret": True},
        headers=auth,
    ).json()
    assert patched["description"] == "renamed"
    assert patched["event_types"] == ["*"]
    assert patched["secret"].startswith("whsec_") and patched["secret"] != created["secret"]

    assert client.delete(f"/v1/endpoints/{created['id']}", headers=auth).status_code == 204
    assert client.get(f"/v1/endpoints/{created['id']}", headers=auth).json()["is_active"] is False
    assert client.get("/v1/endpoints/ep_missing", headers=auth).status_code == 404


def test_endpoint_validation(client, auth):
    bad_url = client.post("/v1/endpoints", json={"url": "not-a-url"}, headers=auth)
    assert bad_url.status_code == 422
    assert bad_url.json()["error"]["code"] == "validation_error"
    bad_filter = client.post(
        "/v1/endpoints",
        json={"url": "https://x.example/h", "event_types": ["*.paid"]},
        headers=auth,
    )
    assert bad_filter.status_code == 422
    short_secret = client.post(
        "/v1/endpoints", json={"url": "https://x.example/h", "secret": "short"}, headers=auth
    )
    assert short_secret.status_code == 422


def test_publish_fans_out_to_matching_active_endpoints_only(client, auth):
    payments = register(client, auth, path="/payments", event_types=["payment.*"])
    everything = register(client, auth, path="/all")
    register(client, auth, path="/orders", event_types=["order.paid"])
    inactive = register(client, auth, path="/inactive", is_active=False)

    event = publish(client, auth, type="payment.captured")
    targets = {d["endpoint_id"] for d in event["deliveries"]}
    assert targets == {payments["id"], everything["id"]}
    assert inactive["id"] not in targets
    assert all(d["status"] == "pending" and d["attempt_count"] == 0 for d in event["deliveries"])

    fetched = client.get(f"/v1/events/{event['id']}", headers=auth).json()
    assert fetched["type"] == "payment.captured"
    assert len(fetched["deliveries"]) == 2
    listed = client.get("/v1/events", params={"type": "payment.captured"}, headers=auth)
    assert listed.headers["X-Total-Count"] == "1"


def test_event_ids_are_idempotent(client, auth):
    register(client, auth)
    first = client.post(
        "/v1/events", json={"id": "evt-order-4521", "type": "order.paid"}, headers=auth
    )
    second = client.post(
        "/v1/events", json={"id": "evt-order-4521", "type": "order.paid"}, headers=auth
    )
    assert first.status_code == 201 and first.json()["duplicate"] is False
    assert second.status_code == 200 and second.json()["duplicate"] is True
    assert second.json()["id"] == "evt-order-4521"
    deliveries = client.get("/v1/deliveries", params={"event_id": "evt-order-4521"}, headers=auth)
    assert deliveries.headers["X-Total-Count"] == "1"


def test_event_type_validation(client, auth):
    bad = client.post("/v1/events", json={"type": "Payment Captured"}, headers=auth)
    assert bad.status_code == 422
    assert client.get("/v1/events/evt_missing", headers=auth).status_code == 404


def test_delivery_listing_filters_and_404(client, auth):
    ep = register(client, auth)
    event = publish(client, auth)
    delivery_id = event["deliveries"][0]["id"]

    one = client.get(f"/v1/deliveries/{delivery_id}", headers=auth).json()
    assert one["event_type"] == "payment.captured"
    assert one["endpoint_url"] == "https://merchant.example.com/hooks"
    assert one["attempts"] == []

    by_endpoint = client.get("/v1/deliveries", params={"endpoint_id": ep["id"]}, headers=auth)
    assert by_endpoint.headers["X-Total-Count"] == "1"
    assert client.get("/v1/deliveries", params={"status": "dead"}, headers=auth).json() == []
    assert client.get("/v1/deliveries", params={"status": "bogus"}, headers=auth).status_code == 422
    assert client.get("/v1/deliveries/dlv_missing", headers=auth).status_code == 404
    assert client.get("/v1/dead-letters", headers=auth).json() == []
