"""Claiming shares a batch between endpoints, so one backlog cannot crowd out the others."""

from collections import Counter
from datetime import timedelta

from sqlalchemy import text

from tests.conftest import T0, publish, register
from webhook_relay.models import Delivery, Endpoint, Event
from webhook_relay.services.dispatcher import Dispatcher


def _queue(app, **due_by_path: int) -> dict[str, str]:
    """Create endpoints with due deliveries. Earlier paths hold the older rows."""
    ids: dict[str, str] = {}
    with app.state.session_factory() as db:
        for rank, (path, count) in enumerate(due_by_path.items()):
            endpoint = Endpoint(
                url=f"https://merchant.example.com/{path}", secret="s" * 24, event_types=["*"]
            )
            db.add(endpoint)
            db.flush()
            ids[path] = endpoint.id
            for i in range(count):
                event = Event(id=f"evt_{path}_{i}", type="a.b", payload={}, created_at=T0)
                db.add(event)
                db.add(
                    Delivery(
                        event_id=event.id,
                        endpoint_id=endpoint.id,
                        status="pending",
                        next_attempt_at=T0 - timedelta(hours=10 - rank, seconds=count - i),
                        created_at=T0,
                    )
                )
        db.commit()
    return ids


def _worker(app, settings, **overrides) -> Dispatcher:
    return Dispatcher(app.state.session_factory, settings.model_copy(update=overrides))


def _claim(app, worker, clock) -> Counter:
    with app.state.session_factory() as db:
        claimed = worker._claim_due(db, clock())
    return Counter(d.endpoint.url.rsplit("/", 1)[1] for d in claimed)


def test_a_backlog_does_not_crowd_out_younger_endpoints(app, client, settings, clock):
    _queue(app, heavy=40, light=1)
    worker = _worker(app, settings, batch_size=5)
    assert _claim(app, worker, clock)["light"] == 1


def test_an_endpoint_alone_in_the_queue_gets_the_whole_batch(app, client, settings, clock):
    _queue(app, only=40)
    assert _claim(app, _worker(app, settings, batch_size=5), clock) == {"only": 5}


def test_unused_share_rolls_over_to_the_next_endpoint(app, client, settings, clock):
    ids = _queue(app, a=1, b=40)
    worker = _worker(app, settings, batch_size=6)
    worker._last_served = max(ids.values())  # the next turn starts at the lowest id
    small_first = ids["a"] < ids["b"]
    assert _claim(app, worker, clock) == ({"a": 1, "b": 5} if small_first else {"a": 1, "b": 3})


def test_every_endpoint_gets_a_turn_when_slots_run_short(app, client, settings, clock):
    _queue(app, a=9, b=9, c=9, d=9)
    worker = _worker(app, settings, batch_size=2)
    turns = [_claim(app, worker, clock) for _ in range(2)]
    assert all(sum(turn.values()) == 2 for turn in turns)
    assert sorted(turns[0] + turns[1]) == ["a", "b", "c", "d"]


def test_rows_claim_oldest_first_within_an_endpoint(app, client, settings, clock):
    _queue(app, only=4)
    worker = Dispatcher(app.state.session_factory, settings.model_copy(update={"batch_size": 2}))
    with app.state.session_factory() as db:
        claimed = worker._claim_due(db, clock())
    assert [d.event_id for d in claimed] == ["evt_only_0", "evt_only_1"]


async def test_busy_endpoints_are_left_out_of_a_claim(app, client, auth, dispatcher, clock):
    first = register(client, auth, path="/one")
    register(client, auth, path="/two")
    publish(client, auth)
    with app.state.session_factory() as db:
        claimed = dispatcher._claim_due(db, clock(), busy=frozenset({first["id"]}))
    assert [d.endpoint.url for d in claimed] == ["https://merchant.example.com/two"]


def test_claim_query_uses_the_endpoint_status_index(app, client):
    with app.state.session_factory() as db:
        plan = db.execute(
            text(
                "EXPLAIN QUERY PLAN SELECT id FROM deliveries WHERE endpoint_id = 'ep_1' "
                "AND status = 'pending' AND next_attempt_at <= :now "
                "ORDER BY next_attempt_at LIMIT 10"
            ),
            {"now": T0},
        ).all()
    detail = " ".join(str(row[-1]) for row in plan)
    assert "ix_deliveries_endpoint_status_next" in detail, detail
    assert "TEMP B-TREE" not in detail
