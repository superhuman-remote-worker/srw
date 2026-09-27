"""Two independent orchestrator feeds sharing a PostgreSQL notification channel."""

import asyncio
import json
from collections import defaultdict
from uuid import uuid4

import pytest

from orchestrator.services.notification_feed import NotificationFeedService


class _Bus:
    def __init__(self):
        self.listeners = defaultdict(set)
        self.connections = []

    def connect(self):
        conn = _Connection(self)
        self.connections.append(conn)
        return conn

    def notify(self, channel, payload):
        for conn, callback in tuple(self.listeners[channel]):
            if not conn.closed:
                callback(conn, 1, channel, payload)


class _Connection:
    def __init__(self, bus):
        self.bus = bus
        self.closed = False

    async def add_listener(self, channel, callback):
        self.bus.listeners[channel].add((self, callback))

    async def remove_listener(self, channel, callback):
        self.bus.listeners[channel].discard((self, callback))

    async def fetchval(self, _query):
        if self.closed:
            raise ConnectionError("LISTEN connection lost")
        return 1


class _Pool:
    def __init__(self, bus):
        self.bus = bus
        self.connections = []

    async def acquire(self):
        conn = self.bus.connect()
        self.connections.append(conn)
        return conn

    async def release(self, conn):
        conn.closed = True


class _DB:
    def __init__(self, bus):
        self._pool = _Pool(bus)

    async def notify_channel(self, channel, payload):
        self._pool.bus.notify(channel, payload)


async def _started(feed, db):
    stop = asyncio.Event()
    task = asyncio.create_task(feed.run_lifecycle_bridge(db, stop))
    await asyncio.wait_for(feed.lifecycle_bridge_ready.wait(), 1)
    return stop, task


async def _stopped(*instances):
    for stop, _task in instances:
        stop.set()
    await asyncio.wait_for(asyncio.gather(*(task for _stop, task in instances)), 2)


@pytest.mark.asyncio
async def test_two_replicas_deliver_one_generation_hint_only_to_its_owner(monkeypatch):
    from orchestrator.services import session_lifecycle

    bus = _Bus()
    first, second = NotificationFeedService(), NotificationFeedService()
    starts = await asyncio.gather(_started(first, _DB(bus)), _started(second, _DB(bus)))
    owner, stranger, thread, generation = (str(uuid4()) for _ in range(4))
    local = first.subscribe_sse(owner)
    remote = second.subscribe_sse(owner)
    wrong_owner = second.subscribe_sse(stranger)
    try:
        monkeypatch.setattr(session_lifecycle, "notification_feed", first)
        session_lifecycle.emit(
            owner,
            thread,
            "provisioning",
            session_runtime_generation=generation,
            backend="vm",
        )
        expected = {
            "type": "session.lifecycle",
            "thread_id": thread,
            "state": "provisioning",
            "session_runtime_generation": generation,
            "backend": "vm",
        }
        assert await asyncio.wait_for(local.get(), 1) == expected
        assert await asyncio.wait_for(remote.get(), 1) == expected
        assert local.empty()  # publisher's own LISTEN delivery must not duplicate
        assert wrong_owner.empty()
    finally:
        await _stopped(*starts)
    # If the bridge task exits after once running, a late HTTP request must
    # not inherit a stream that appears healthy without a LISTEN consumer.
    after_exit = second.subscribe_sse(owner)
    assert await asyncio.wait_for(after_exit.get(), 1) is None


@pytest.mark.asyncio
async def test_lost_listener_closes_sse_and_reconnected_replica_gets_next_hint(
    monkeypatch,
):
    from orchestrator.services import notification_feed as module

    monkeypatch.setattr(module, "_LISTENER_CHECK_SECONDS", 0.01)
    monkeypatch.setattr(module, "_RECONNECT_MIN_SECONDS", 0.1)
    bus = _Bus()
    first, second = NotificationFeedService(), NotificationFeedService()
    first_db, second_db = _DB(bus), _DB(bus)
    starts = await asyncio.gather(
        _started(first, first_db), _started(second, second_db)
    )
    owner, thread, generation = (str(uuid4()) for _ in range(3))
    old_stream = second.subscribe_sse(owner)
    try:
        second_connection = second_db._pool.connections[0]
        second_connection.closed = True
        assert await asyncio.wait_for(old_stream.get(), 1) is None
        # A stream opened in the outage must not stay apparently healthy.
        gap_stream = second.subscribe_sse(owner)
        assert await asyncio.wait_for(gap_stream.get(), 1) is None

        async def reconnected():
            while (
                len(second_db._pool.connections) < 2
                or not second.lifecycle_bridge_ready.is_set()
            ):
                await asyncio.sleep(0.005)

        await asyncio.wait_for(reconnected(), 1)
        new_stream = second.subscribe_sse(owner)
        first.publish_lifecycle(
            owner, thread, "provisioning", session_runtime_generation=generation
        )
        assert (await asyncio.wait_for(new_stream.get(), 1))[
            "session_runtime_generation"
        ] == generation
    finally:
        await _stopped(*starts)


@pytest.mark.asyncio
async def test_bounded_publisher_overflow_forces_remote_resync(monkeypatch):
    from orchestrator.services import notification_feed as module

    monkeypatch.setattr(module, "_OUTBOUND_LIMIT", 1)
    bus = _Bus()
    first, second = NotificationFeedService(), NotificationFeedService()
    first_db, second_db = _DB(bus), _DB(bus)
    gate = asyncio.Event()
    entered = asyncio.Event()
    sent = []
    original_notify = first_db.notify_channel

    async def blocked_notify(channel, payload):
        entered.set()
        await gate.wait()
        sent.append(json.loads(payload)["kind"])
        await original_notify(channel, payload)

    first_db.notify_channel = blocked_notify
    starts = await asyncio.gather(
        _started(first, first_db), _started(second, second_db)
    )
    owner, thread = str(uuid4()), str(uuid4())
    remote = second.subscribe_sse(owner)
    try:
        first.publish_lifecycle(
            owner, thread, "provisioning", session_runtime_generation=str(uuid4())
        )
        await asyncio.wait_for(entered.wait(), 1)  # one in-flight NOTIFY
        first.publish_lifecycle(
            owner, thread, "provisioning", session_runtime_generation=str(uuid4())
        )
        first.publish_lifecycle(
            owner, thread, "provisioning", session_runtime_generation=str(uuid4())
        )  # full queue
        gate.set()

        async def until_reset():
            while await remote.get() is not None:
                pass

        await asyncio.wait_for(until_reset(), 1)
        assert sent[:2] == ["lifecycle", "resync"]
    finally:
        gate.set()
        await _stopped(*starts)


@pytest.mark.asyncio
async def test_legacy_or_sensitive_progress_stays_local():
    bus = _Bus()
    first, second = NotificationFeedService(), NotificationFeedService()
    starts = await asyncio.gather(_started(first, _DB(bus)), _started(second, _DB(bus)))
    owner, thread = str(uuid4()), str(uuid4())
    local = first.subscribe_sse(owner)
    remote = second.subscribe_sse(owner)
    try:
        first.publish_lifecycle(
            owner, thread, "failed", reason="private failure detail"
        )
        assert (await asyncio.wait_for(local.get(), 1))[
            "reason"
        ] == "private failure detail"
        assert remote.empty()  # no canonical generation: cannot wake a retired tab
    finally:
        await _stopped(*starts)


@pytest.mark.asyncio
async def test_malformed_wire_payload_is_ignored_without_interrupting_next_hint():
    feed = NotificationFeedService()
    owner, thread, generation = (str(uuid4()) for _ in range(3))
    stream = feed.subscribe_sse(owner)
    valid = {
        "v": 1,
        "origin": str(uuid4()),
        "kind": "lifecycle",
        "user_id": owner,
        "thread_id": thread,
        "session_runtime_generation": generation,
        "state": "ready",
    }
    for malformed in (
        {**valid, "state": []},
        {**valid, "state": {}},
        {**valid, "v": True},
        {**valid, "backend": "unknown"},
    ):
        feed._receive_lifecycle_wire(json.dumps(malformed))
    assert stream.empty()
    feed._receive_lifecycle_wire(json.dumps(valid))
    assert (await asyncio.wait_for(stream.get(), 1))["state"] == "ready"


@pytest.mark.asyncio
async def test_listener_loss_finishes_actual_sse_stream(monkeypatch):
    from orchestrator.services import notification_api
    from orchestrator.services import notification_feed as module

    monkeypatch.setattr(module, "_LISTENER_CHECK_SECONDS", 0.01)
    bus = _Bus()
    feed = NotificationFeedService()
    db = _DB(bus)
    started = await _started(feed, db)
    monkeypatch.setattr(module, "notification_feed", feed)
    response = await notification_api.notification_sse_events(
        None, user_id=str(uuid4())
    )
    stream = response.body_iterator
    try:
        assert await asyncio.wait_for(anext(stream), 1) == ": open\n\n"
        db._pool.connections[0].closed = True
        with pytest.raises(StopAsyncIteration):
            await asyncio.wait_for(anext(stream), 1)
        assert feed.active_connections == 0
    finally:
        await _stopped(started)
