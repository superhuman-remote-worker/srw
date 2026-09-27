"""Real-PostgreSQL proofs: a retried durable event gets its terminal receipt.

knowledge-base/knowledge/issues/settled_event_retry_to_agent_input_is_refused_retryable.md

An internal sender re-sends a stable event ``delivery_id`` to a pinned
runtime's ``POST /api/input`` after losing the response. Once the delivery was
admitted (its turn running) or settled, the retry must receive the existing
duplicate acknowledgement — never another transcript row, delivery claim,
queue item or turn, and never a retryable refusal that would make the sender
retry forever. The runtime derives a turn number only to number a row it may
create (``turn_count + 1``); by the time a retry arrives the session's counter
has moved on.

Conflicting identities and payloads keep their refusals: another role or
source, another runtime generation, an asserted turn number that differs from
the receipt, and a stale session identity.

The runtime is the attached pinned session of the input characterization
suite (its own ``PostgresDB`` on a migrated database, a seeded reciprocal
binding); requests go through the transport's HTTP input handler and the loop
callbacks exactly as the runtime wires them.
"""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest

import agent.api.persistent_app as pa
from agent.api import session_http
from agent.api.session_contract import DurableInputUnavailable, SessionIdentityMismatch
from shared.persistent_input_delivery import InputDeliveryConflict
from tests.test_session_input_characterization import (
    _drain,
    _input_view,
    _loop_callbacks,
    _operations,
    _restart_input_process,
    _restore_runtime_globals,  # noqa: F401 - autouse fixture
    pinned_runtime,  # noqa: F401 - fixture
)
from tests.test_stateless_input_delivery_real_postgres import (
    _schema_applied,  # noqa: F401 - fixture
    db,  # noqa: F401 - fixture
    pg_dsn,  # noqa: F401 - fixture
)

INTERNAL_KEY = "replay-internal-key"
WAKE = "[wake] the job you started has finished"


async def _post_input(body: dict, *, key: str = INTERNAL_KEY) -> tuple[int, dict]:
    request = SimpleNamespace(
        json=AsyncMock(return_value=body), headers={"X-Internal-Key": key}
    )
    response = await session_http.handle_input(
        request, pa.session_transport_bindings().http
    )
    return response.status_code, json.loads(response.body)


def _event(delivery_id: str, content: str = WAKE) -> dict:
    return {
        "content": content,
        "role": "event",
        "delivery_id": delivery_id,
        "session_identity_fingerprint": (
            pa._current_pinned_session_identity_fingerprint()
        ),
    }


async def _ledger(runtime, delivery_id: str) -> dict:
    async with runtime.db.acquire() as conn:
        delivery = await conn.fetchrow(
            "SELECT state, claim_generation, admitted_turn_number, message_id "
            "FROM thread_input_deliveries WHERE delivery_id = $1",
            UUID(delivery_id),
        )
        messages = await conn.fetch(
            "SELECT id, role, content, turn_number FROM thread_messages "
            "WHERE thread_id = $1 ORDER BY seq",
            runtime.thread_id,
        )
        deliveries = await conn.fetchval(
            "SELECT count(*) FROM thread_input_deliveries WHERE thread_id = $1",
            runtime.thread_id,
        )
    return {
        "delivery": dict(delivery) if delivery is not None else None,
        "messages": [dict(row) for row in messages],
        "deliveries": int(deliveries),
    }


async def _run_turn(callbacks, delivery_id: str, generation: int, turn: int) -> None:
    """The loop consumes the item as ``turn`` and settles it."""

    pa._session.turn_count = turn
    assert await callbacks.admit_input_delivery(delivery_id, generation, turn)
    assert await callbacks.settle_input_delivery(delivery_id, generation)


@pytest.fixture
def internal_key(monkeypatch):
    monkeypatch.setenv("MCP_INTERNAL_KEY", INTERNAL_KEY)


@pytest.mark.asyncio
async def test_a_settled_event_retried_after_later_turns_is_a_duplicate(
    pinned_runtime,  # noqa: F811 - fixture
    monkeypatch,
    internal_key,
):
    delivery_id = str(uuid4())
    captured, parked = await _loop_callbacks(monkeypatch)
    callbacks = captured["callbacks"]
    try:
        status, first = await _post_input(_event(delivery_id))
        assert (status, first["delivery_state"], first["duplicate"]) == (
            202,
            "queued",
            False,
        )
        [item] = _drain(_input_view().queue)
        generation = item["claim_generation"]
        await _run_turn(callbacks, delivery_id, generation, turn=1)
        # Two more turns ran before the lost response is retried.
        pa._session.turn_count = 3
        before = await _ledger(pinned_runtime, delivery_id)

        status, retry = await _post_input(_event(delivery_id))

        assert status == 200
        assert retry["accepted"] is True
        assert retry["duplicate"] is True
        assert retry["retryable"] is False
        assert retry["delivery_state"] == "settled"
        assert retry["delivery_id"] == delivery_id
        assert retry["message_id"] == first["message_id"]
        assert _drain(_input_view().queue) == []
        assert await _ledger(pinned_runtime, delivery_id) == before
        assert before["delivery"]["state"] == "settled"
        assert before["delivery"]["admitted_turn_number"] == 1
        assert [m["turn_number"] for m in before["messages"]] == [1]
    finally:
        parked.set()
        await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_concurrent_retries_before_during_and_after_execution(
    pinned_runtime,  # noqa: F811 - fixture
    monkeypatch,
    internal_key,
):
    delivery_id = str(uuid4())
    captured, parked = await _loop_callbacks(monkeypatch)
    callbacks = captured["callbacks"]
    try:
        # Before execution: one admission, one queue item.
        queued = await asyncio.gather(
            *[_post_input(_event(delivery_id)) for _ in range(4)]
        )
        assert [status for status, _ in queued] == [202] * 4
        assert sum(1 for _, body in queued if not body["duplicate"]) == 1
        [item] = _drain(_input_view().queue)
        generation = item["claim_generation"]

        # During execution: the turn started, so the counter already names it.
        pa._session.turn_count = 1
        assert await callbacks.admit_input_delivery(delivery_id, generation, 1)
        during = await asyncio.gather(
            *[_post_input(_event(delivery_id)) for _ in range(4)]
        )
        assert [status for status, _ in during] == [200] * 4
        assert {body["delivery_state"] for _, body in during} == {"admitted"}
        assert all(body["duplicate"] for _, body in during)

        # After settlement and two later turns.
        assert await callbacks.settle_input_delivery(delivery_id, generation)
        pa._session.turn_count = 3
        after = await asyncio.gather(
            *[_post_input(_event(delivery_id)) for _ in range(4)]
        )
        assert [status for status, _ in after] == [200] * 4
        assert {body["delivery_state"] for _, body in after} == {"settled"}
        assert all(body["duplicate"] for _, body in after)

        assert _drain(_input_view().queue) == []
        ledger = await _ledger(pinned_runtime, delivery_id)
        assert ledger["deliveries"] == 1
        assert ledger["delivery"]["claim_generation"] == generation
        assert ledger["delivery"]["state"] == "settled"
        assert [(m["role"], m["turn_number"]) for m in ledger["messages"]] == [
            ("event", 1)
        ]
    finally:
        parked.set()
        await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_conflicting_replays_keep_their_refusals(
    pinned_runtime,  # noqa: F811 - fixture
    monkeypatch,
    internal_key,
):
    delivery_id = str(uuid4())
    captured, parked = await _loop_callbacks(monkeypatch)
    callbacks = captured["callbacks"]
    try:
        status, _ = await _post_input(_event(delivery_id))
        assert status == 202
        [item] = _drain(_input_view().queue)
        await _run_turn(callbacks, delivery_id, item["claim_generation"], turn=1)
        pa._session.turn_count = 3
        before = await _ledger(pinned_runtime, delivery_id)

        # The same identity as a human input (another source contract).
        with pytest.raises(DurableInputUnavailable):
            await _operations().accept_input(WAKE, delivery_id=delivery_id)

        # A stale session identity is refused before anything is read.
        with pytest.raises(SessionIdentityMismatch):
            await _operations().accept_input(
                WAKE,
                role="event",
                delivery_id=delivery_id,
                expected_session_identity_fingerprint="sha256:" + "0" * 64,
            )

        # An orchestrator-side persist that asserts another turn conflicts.
        with pytest.raises(InputDeliveryConflict):
            await pinned_runtime.db.persist_thread_input_delivery(
                thread_id=str(pinned_runtime.thread_id),
                delivery_id=delivery_id,
                role="event",
                content=WAKE,
                source="officer_wake",
                turn_number=2,
            )
        # ... while the same assertion of the receipt's own turn replays it.
        receipt = await pinned_runtime.db.persist_thread_input_delivery(
            thread_id=str(pinned_runtime.thread_id),
            delivery_id=delivery_id,
            role="event",
            content=WAKE,
            source="officer_wake",
            turn_number=1,
        )
        assert (receipt["state"], receipt["transcript_inserted"]) == (
            "settled",
            False,
        )

        # A later runtime generation does not own the earlier life's receipt.
        _restart_input_process(monkeypatch)
        status, body = await _post_input(_event(delivery_id))
        assert (status, body["error"], body["retryable"]) == (
            503,
            "durable_input_unavailable",
            True,
        )

        assert await _ledger(pinned_runtime, delivery_id) == before
        assert _drain(_input_view().queue) == []
    finally:
        parked.set()
        await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_a_rerendered_wake_replays_the_stored_text(
    pinned_runtime,  # noqa: F811 - fixture
    monkeypatch,
    internal_key,
):
    """The source-specific rule is unchanged: an Officer wake may be
    re-rendered between sends; the receipt keeps the stored transcript."""

    delivery_id = str(uuid4())
    captured, parked = await _loop_callbacks(monkeypatch)
    callbacks = captured["callbacks"]
    try:
        await _post_input(_event(delivery_id))
        [item] = _drain(_input_view().queue)
        await _run_turn(callbacks, delivery_id, item["claim_generation"], turn=1)
        pa._session.turn_count = 2

        status, body = await _post_input(
            _event(delivery_id, content=WAKE + " (re-rendered)")
        )

        assert (status, body["duplicate"], body["delivery_state"]) == (
            200,
            True,
            "settled",
        )
        ledger = await _ledger(pinned_runtime, delivery_id)
        assert [m["content"] for m in ledger["messages"]] == [WAKE]
    finally:
        parked.set()
        await asyncio.sleep(0)
