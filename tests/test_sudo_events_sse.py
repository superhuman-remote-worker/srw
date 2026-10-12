"""F6 — per-user filtering on `GET /api/sudo/events` SSE stream.

The endpoint is `control_seams.sudo_sse_events`. We test the integration by:
  1. Pre-loading the SSE queue with synthetic events
  2. Driving the handler with a `Request` mock whose `is_disconnected`
     flips True after the events have been consumed
  3. Iterating the StreamingResponse body and asserting which events
     made it through the filter

The filter itself (`user_can_access_job_or_thread`) has unit coverage in
test_security_access.py; this file confirms the wiring at the endpoint.
"""

import asyncio
import json
from contextlib import ExitStack
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


def _patch_caller_and_db(user: dict, db):
    """Same patch stack used across F2/F3/F5/F6 test files."""
    stack = ExitStack()
    stack.enter_context(
        patch(
            "orchestrator.security.auth.require_approved_user",
            AsyncMock(return_value=user),
        )
    )
    stack.enter_context(
        patch(
            "orchestrator.security.access.require_approved_user",
            AsyncMock(return_value=user),
        )
    )
    stack.enter_context(patch("orchestrator.main.app.state.resources.postgres_db", db))
    return stack


async def _drive_sse(handler_coro, queue, max_events: int):
    """Run the endpoint, consume up to ``max_events`` SSE frames.

    The handler runs in a background task while we drain the
    StreamingResponse body. After ``max_events`` event-lines we stop
    iterating, which lets the handler block on ``queue.get()`` and the
    test ends via teardown rather than via disconnect.
    """
    response = await handler_coro
    body = response.body_iterator
    chunks: list[bytes] = []
    yielded = 0
    async for chunk in body:
        chunks.append(chunk)
        if "event:" in (chunk if isinstance(chunk, str) else chunk.decode()):
            yielded += 1
            if yielded >= max_events:
                break
    return b"".join(c.encode() if isinstance(c, str) else c for c in chunks)


def _build_request_mock(queue_drained_event: asyncio.Event):
    """A Request whose ``is_disconnected`` flips True once events drain.

    The handler checks this at the top of each iteration, so once we set
    it the next loop ends cleanly without needing to cancel the task.
    """

    req = MagicMock()

    async def _is_disconnected():
        return queue_drained_event.is_set()

    req.is_disconnected = _is_disconnected
    req.cookies = {}
    req.headers = {}
    return req


class TestSudoSseFilter:
    @pytest.mark.asyncio
    async def test_thread_owner_sees_thread_event(
        self, user_a, thread_a, thread_b, fake_db
    ):
        from tests._b09_control_seams import sudo_sse_events

        queue: asyncio.Queue = asyncio.Queue()
        await queue.put(
            ("new_request", {"id": "owned", "thread_id": str(thread_a["id"])})
        )
        await queue.put(
            ("new_request", {"id": "other", "thread_id": str(thread_b["id"])})
        )
        request = MagicMock(cookies={}, headers={})
        request.is_disconnected = AsyncMock(side_effect=lambda: queue.empty())

        with _patch_caller_and_db(user_a, fake_db):
            with patch(
                "orchestrator.services.sudo_gate.sudo_gate.subscribe_sse", lambda: queue
            ):
                with patch(
                    "orchestrator.services.sudo_gate.sudo_gate.unsubscribe_sse",
                    lambda q: None,
                ):
                    response = await sudo_sse_events(request)
                    chunks = [chunk async for chunk in response.body_iterator]

        text = b"".join(
            chunk.encode() if isinstance(chunk, str) else chunk for chunk in chunks
        ).decode()
        assert '"id": "owned"' in text
        assert '"id": "other"' not in text

    @pytest.mark.asyncio
    async def test_thread_owner_can_list_and_get_thread_sudo_request(
        self, user_a, thread_a, fake_db
    ):
        from tests._b09_control_seams import get_sudo_request, list_sudo_requests

        row = {
            "id": "thread-sudo",
            "job_id": None,
            "thread_id": str(thread_a["id"]),
        }
        request = MagicMock(cookies={}, headers={})
        with _patch_caller_and_db(user_a, fake_db):
            with patch(
                "orchestrator.services.sudo_gate.sudo_gate.list_requests",
                AsyncMock(return_value=[row]),
            ):
                listed = await list_sudo_requests(
                    request,
                    job_id=None,
                    status=None,
                    request_type=None,
                    limit=50,
                )
            with patch(
                "orchestrator.services.sudo_gate.sudo_gate.get_request",
                AsyncMock(return_value=row),
            ):
                fetched = await get_sudo_request(request, "thread-sudo")

        assert listed == [row]
        assert fetched == row

    @pytest.mark.asyncio
    async def test_user_sees_only_own_jobs_events(self, user_a, job_a, job_b, fake_db):
        """user_a should see events for job_a (owned), not for job_b."""
        from tests._b09_control_seams import sudo_sse_events

        queue: asyncio.Queue = asyncio.Queue()
        await queue.put(("new_request", {"id": "r1", "job_id": str(job_a["id"])}))
        await queue.put(("new_request", {"id": "r2", "job_id": str(job_b["id"])}))
        await queue.put(("new_request", {"id": "r3", "job_id": None}))

        request = MagicMock()
        request.cookies = {}
        request.headers = {}

        # Drive the iter ourselves. Empty queue → asyncio.TimeoutError →
        # keepalive comment. We stop after seeing exactly one event line.
        async def _is_disconnected():
            return queue.empty()

        request.is_disconnected = _is_disconnected

        with _patch_caller_and_db(user_a, fake_db):
            with patch(
                "orchestrator.services.sudo_gate.sudo_gate.subscribe_sse", lambda: queue
            ):
                with patch(
                    "orchestrator.services.sudo_gate.sudo_gate.unsubscribe_sse",
                    lambda q: None,
                ):
                    response = await sudo_sse_events(request)
                    out = bytearray()
                    async for chunk in response.body_iterator:
                        out.extend(
                            chunk if isinstance(chunk, bytes) else chunk.encode()
                        )

        text = out.decode()
        # Only r1 (user_a's job) made it through. r2 (user_b's) dropped;
        # r3 (orphan) dropped — non-admin can't see orphans.
        assert '"id": "r1"' in text
        assert '"id": "r2"' not in text
        assert '"id": "r3"' not in text

    @pytest.mark.asyncio
    async def test_admin_sees_all_events_including_orphans(
        self, user_admin, job_a, job_b, fake_db
    ):
        from tests._b09_control_seams import sudo_sse_events

        queue: asyncio.Queue = asyncio.Queue()
        await queue.put(("new_request", {"id": "r1", "job_id": str(job_a["id"])}))
        await queue.put(("new_request", {"id": "r2", "job_id": str(job_b["id"])}))
        await queue.put(("new_request", {"id": "r3", "job_id": None}))

        request = MagicMock()
        request.cookies = {}
        request.headers = {}

        async def _is_disconnected():
            return queue.empty()

        request.is_disconnected = _is_disconnected

        with _patch_caller_and_db(user_admin, fake_db):
            with patch(
                "orchestrator.services.sudo_gate.sudo_gate.subscribe_sse", lambda: queue
            ):
                with patch(
                    "orchestrator.services.sudo_gate.sudo_gate.unsubscribe_sse",
                    lambda q: None,
                ):
                    response = await sudo_sse_events(request)
                    out = bytearray()
                    async for chunk in response.body_iterator:
                        out.extend(
                            chunk if isinstance(chunk, bytes) else chunk.encode()
                        )

        text = out.decode()
        assert '"id": "r1"' in text
        assert '"id": "r2"' in text
        assert '"id": "r3"' in text

    @pytest.mark.asyncio
    async def test_request_decided_events_filtered_by_job_id(
        self, user_a, job_a, job_b, fake_db
    ):
        """`request_decided` events now carry job_id (F6 fix to sudo_gate)."""
        from tests._b09_control_seams import sudo_sse_events

        queue: asyncio.Queue = asyncio.Queue()
        await queue.put(
            (
                "request_decided",
                {
                    "id": "r1",
                    "job_id": str(job_a["id"]),
                    "status": "approved",
                    "decided_by": "admin",
                },
            )
        )
        await queue.put(
            (
                "request_decided",
                {
                    "id": "r2",
                    "job_id": str(job_b["id"]),
                    "status": "denied",
                    "decided_by": "admin",
                    "reason": "no",
                },
            )
        )

        request = MagicMock()
        request.cookies = {}
        request.headers = {}

        async def _is_disconnected():
            return queue.empty()

        request.is_disconnected = _is_disconnected

        with _patch_caller_and_db(user_a, fake_db):
            with patch(
                "orchestrator.services.sudo_gate.sudo_gate.subscribe_sse", lambda: queue
            ):
                with patch(
                    "orchestrator.services.sudo_gate.sudo_gate.unsubscribe_sse",
                    lambda q: None,
                ):
                    response = await sudo_sse_events(request)
                    out = bytearray()
                    async for chunk in response.body_iterator:
                        out.extend(
                            chunk if isinstance(chunk, bytes) else chunk.encode()
                        )

        text = out.decode()
        # Parse SSE frames to confirm exactly one event_decided came through
        events = [
            line for line in text.split("\n\n") if "event: request_decided" in line
        ]
        assert len(events) == 1
        # And its payload is for r1 (user_a's job)
        data_line = next(
            line for line in events[0].split("\n") if line.startswith("data:")
        )
        payload = json.loads(data_line[len("data: ") :])
        assert payload["id"] == "r1"


class TestSudoGateEventsCarryJobId:
    """sudo_gate broadcasts now include job_id so the SSE filter has
    something to evaluate. Tested directly against the gate module to
    catch the next dev accidentally dropping the field again.
    """

    @pytest.mark.asyncio
    async def test_approve_request_event_has_job_id(self):
        from orchestrator.services.sudo_gate import SudoGateService

        gate = SudoGateService()
        gate._db = AsyncMock()
        gate._db.acquire = MagicMock()
        captured: list[tuple] = []
        original_broadcast = gate._broadcast_sse

        async def capture(evt_type, data):
            captured.append((evt_type, data))
            await original_broadcast(evt_type, data)

        gate._broadcast_sse = capture

        # Stub the DB lookup and the SQL update path.
        gate._get_request = AsyncMock(
            return_value={
                "id": "req-1",
                "job_id": "00000000-0000-0000-0000-00000000aaaa",
                "status": "pending",
                "nats_reply_subject": None,
            }
        )
        gate._finalize_request = AsyncMock()

        await gate.approve_request("req-1", reason="ok", decided_by="admin")

        decided = [e for e in captured if e[0] == "request_decided"]
        assert decided, "expected a request_decided event"
        assert "job_id" in decided[0][1]
        assert decided[0][1]["job_id"] == "00000000-0000-0000-0000-00000000aaaa"
