"""An event admitted by a stateless executor that died is served again (K4).

Design: knowledge-base/knowledge/features/parallel_subagents.md §8 ("Recovery
turn killed": one final answer in the end), §5.3 and D3 (a recovery turn that
delegated again is settled against its continuation instead), §9.

Found live by the WP5 gate, scenario K4: a session delegated four children and
its executor was killed. The successor settled the batch (one continuation),
admitted the continuation and was killed while streaming the answer. The next
executor saw no pending input, completed the unit past the continuation, and
the request was lost: an event is pending only while its delivery is
persisted/queued/deferred, and an admission is never settled by anyone else.

The serving executor here is the real claim path (``_serve_claim``: the
skip-if-answered legs, the attach-time watermark, pending selection, the
restored-copy strip, the delivery claim, the settled close and completion) on a
migrated Postgres. Only the attach, the loop turn and the interrupt watchers
are the harness's; the loop turn makes the durable writes a real one makes
(admission, answer, settlement). A dying executor is the reaper stealing its
expired lease.
"""

from __future__ import annotations

import asyncio
import json
from contextlib import AsyncExitStack
from types import SimpleNamespace
from typing import Any, Awaitable, Callable
from uuid import UUID, uuid4

import asyncpg
import pytest

import agent.api.persistent_app as pa
import agent.api.turn_executor as te
from agent.api.lease_context import LeaseHandle, current_lease
from agent.api.persistent_app import _db_rows_to_lc_messages
from orchestrator.routers import thread_transport
from orchestrator.services import run_queue_reaper as reaper
from shared import run_queue
from shared.persistent_input_delivery import (
    claim_stateless_input_delivery,
    persist_input_delivery,
    settle_answered_stateless_admission,
    transition_stateless_input_delivery,
)
from shared.run_queue import (
    UNIT_KIND_SESSION_TURN,
    ClaimedUnit,
    claim_unit,
    reap_expired,
    record_input_seq,
)
from shared.session_retirement import acknowledge_session_claim_quiesced
from shared.session_subagent_authority import session_subagent_delivery_id
from tests.test_b10_session_queries_real_postgres import _request, _transport
from tests.test_session_subagent_batch_recovery_pg import THIRD, THIRD_UID, _Agent
from tests.test_session_subagent_batch_settle_pg import (
    POD,
    POD_UID,
    S1,
    SUCCESSOR,
    SUCCESSOR_UID,
    _consumed,
    _continuations,
    _fresh_pool,
    _seed,
    _stateless_parent,
    _successor,
    _tool_rows,
)
from tests.test_subagent_thread_migration import (  # noqa: F401  (scratch_pg_dsn fixture)
    _agent_db,
    _orchestrator_db,
    _stamp_stateless_claim,
    scratch_pg_dsn,
)
from tests.test_turn_executor import (
    _INPUT_SAVED_ATTRS,
    _PA_SAVED_ATTRS,
    FakeSession,
    Harness,
)


@pytest.fixture(scope="module")
def pg_dsn(scratch_pg_dsn: str) -> str:  # noqa: F811 (pytest fixture param)
    """The migration test's scratch Postgres (testcontainers), reused as-is."""
    return scratch_pg_dsn


@pytest.fixture
def harness(monkeypatch):
    saved = {name: getattr(pa, name) for name in _PA_SAVED_ATTRS}
    saved_input = {
        name: getattr(pa._session_input, name) for name in _INPUT_SAVED_ATTRS
    }
    h = Harness(monkeypatch)
    try:
        yield h
    finally:
        for name, value in saved.items():
            setattr(pa, name, value)
        for name, value in saved_input.items():
            setattr(pa._session_input, name, value)


ANSWER = "The comparison: Austria keeps 41,200 EUR of 60,000 EUR gross."
WAKE = "[wake] the nightly report job finished"


# ---------------------------------------------------------------------------
# The serving executor: the real claim path on Postgres
# ---------------------------------------------------------------------------


class _Executor:
    """A stateless executor pod: ``_serve_claim`` against the real queue,
    delivery and transcript SQL; the harness fakes attach and the loop."""

    def __init__(
        self,
        harness: Harness,
        monkeypatch,
        pool: asyncpg.Pool,
        *,
        pod: str,
        pod_uid: str,
        answer: str = ANSWER,
    ):
        self.h = harness
        self.pool = pool
        self.agent_db = _agent_db(pool)
        self.pod, self.pod_uid = pod, pod_uid
        self.answer = answer
        # One entry per turn the loop ran: the injected input and the ids of
        # the restored context the model would have read before it.
        self.served: list[dict[str, Any]] = []
        # The message ids each fresh attach restored, before the strip.
        self.restored: list[list[str]] = []
        self.before_attach: Callable[[], Awaitable[None]] | None = None
        self.mid_turn: Callable[[dict], Awaitable[None]] | None = None
        pa._agent = SimpleNamespace(postgres_conn=self.agent_db)
        monkeypatch.setattr(te, "complete_unit", run_queue.complete_unit)
        monkeypatch.setattr(
            te, "open_interrupt_admission", run_queue.open_interrupt_admission
        )
        monkeypatch.setattr(
            te, "close_interrupt_admission", run_queue.close_interrupt_admission
        )
        monkeypatch.setattr(pa, "_attach_session", self._attach)
        monkeypatch.setattr(harness, "_fake_loop", self._loop)
        harness.executor = te.StatelessTurnExecutor(
            pod_name=pod,
            pod_uid=pod_uid,
            abort_grace_seconds=0.05,
            worker_enabled=False,
            bg_task_enabled=False,
            completion_commands_enabled=False,
            audit_writer=None,
        )

    async def _attach(self, **kwargs) -> None:
        thread_id = kwargs["thread_id"]
        if self.before_attach is not None:
            # The attach-time foreground recovery (recover_orphans).
            await self.before_attach()
        rows = await self.agent_db.get_thread_messages_history(thread_id, limit=None)
        session = FakeSession(self.h.calls["shell_owner_token"])
        session.messages = _db_rows_to_lc_messages(rows)
        self.restored.append([str(m.id) for m in session.messages])
        session.turn_count = max((int(r["turn_number"] or 0) for r in rows), default=0)
        self.h.sessions.append(session)
        pa._session = session
        pa._thread_id = thread_id
        pa._session_input._queue = asyncio.Queue()
        pa._turn_tool_execution_identity = None

    async def _loop(self) -> None:
        while True:
            item = await pa._session_input.queue.get()
            turn_id = int(pa._session.turn_count) + 1
            pa._session.turn_count = turn_id
            self.served.append(
                {
                    "item": dict(item),
                    "turn": turn_id,
                    "context": [str(m.id) for m in pa._session.messages],
                }
            )
            await pa._turn_start_external_hook(turn_id)
            thread_id = pa._thread_id
            token = self.h.executor._lease.lease_token
            delivery = item.get("delivery_id")
            if delivery is not None:
                assert await self.agent_db.transition_stateless_input_delivery(
                    thread_id=thread_id,
                    delivery_id=delivery,
                    lease_token=token,
                    executor_id=self.pod,
                    pod_uid=self.pod_uid,
                    claim_generation=item["claim_generation"],
                    transition="admitted",
                    turn_number=turn_id,
                )
                if self.mid_turn is not None:
                    await self.mid_turn(item)
            await self.agent_db.save_thread_message(
                thread_id=thread_id, role="ai", content=self.answer, turn_number=turn_id
            )
            if delivery is not None:
                assert await self.agent_db.transition_stateless_input_delivery(
                    thread_id=thread_id,
                    delivery_id=delivery,
                    lease_token=token,
                    executor_id=self.pod,
                    pod_uid=self.pod_uid,
                    claim_generation=item["claim_generation"],
                    transition="settled",
                )
            pa._turn_event_open = False
            pa._turn_complete_external_hook(turn_id)

    async def claim(self, thread_id: UUID) -> ClaimedUnit | None:
        async with self.pool.acquire() as conn:
            claim = await claim_unit(
                conn,
                unit_kind=UNIT_KIND_SESSION_TURN,
                pod_name=self.pod,
                prefer_unit_id=thread_id,
            )
            if claim is not None:
                # What the claim bundle stamps for the exact claimant.
                await _stamp_stateless_claim(
                    conn,
                    thread_id,
                    token=claim.lease_token,
                    pod=self.pod,
                    pod_uid=self.pod_uid,
                )
        return claim

    async def serve_once(self, thread_id: UUID) -> ClaimedUnit:
        claim = await self.claim(thread_id)
        assert claim is not None
        await self.h.executor._serve_claim(claim)
        return claim

    async def drive(self, thread_id: UUID, *, max_claims: int = 6) -> int:
        """Claim and serve until the unit is not claimable."""

        for claims in range(max_claims):
            claim = await self.claim(thread_id)
            if claim is None:
                return claims
            await self.h.executor._serve_claim(claim)
        raise AssertionError(f"unit did not settle within {max_claims} claims")

    def served_ids(self) -> list[str]:
        return [entry["item"]["id"] for entry in self.served]


async def _die(pool: asyncpg.Pool, thread_id: UUID) -> None:
    """The claimant is killed: heartbeats stop, the reaper steals the lease."""

    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE run_queue SET leased_until = now() - interval '1 hour' "
            "WHERE unit_id = $1 AND state = 'leased'",
            thread_id,
        )
        stolen = await reap_expired(
            conn,
            unit_kind=UNIT_KIND_SESSION_TURN,
            grace_seconds=0.0,
            backoff_base_seconds=0.0,
            jitter=0.0,
        )
    assert [unit.unit_id for unit in stolen] == [thread_id]


async def _admit(
    pool: asyncpg.Pool,
    thread_id: UUID,
    delivery_id: Any,
    *,
    lease: dict,
    turn_number: int,
) -> dict:
    """What an executor does before its model call: claim the event
    delivery for its lease, then cross provider admission."""

    async with pool.acquire() as conn:
        async with conn.transaction():
            claimed = await claim_stateless_input_delivery(
                conn,
                thread_id=thread_id,
                delivery_id=delivery_id,
                lease_token=lease["lease_token"],
                executor_id=lease["executor_id"],
                pod_uid=lease["executor_pod_uid"],
            )
        assert claimed is not None
        async with conn.transaction():
            assert await transition_stateless_input_delivery(
                conn,
                thread_id=thread_id,
                delivery_id=delivery_id,
                lease_token=lease["lease_token"],
                executor_id=lease["executor_id"],
                pod_uid=lease["executor_pod_uid"],
                claim_generation=claimed["claim_generation"],
                transition="admitted",
                turn_number=turn_number,
            )
    return claimed


async def _delivery(pool: asyncpg.Pool, delivery_id: Any) -> asyncpg.Record:
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            "SELECT state, claim_generation, owner_run_queue_lease_token, "
            "owner_executor, admitted_turn_number, settled_at "
            "FROM thread_input_deliveries WHERE delivery_id = $1",
            UUID(str(delivery_id)),
        )


async def _queue(pool: asyncpg.Pool, thread_id: UUID) -> asyncpg.Record:
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            "SELECT state, input_seq, consumed_seq, lease_token, "
            "attempts_since_completion, park_reason FROM run_queue "
            "WHERE unit_id = $1",
            thread_id,
        )


async def _answers(pool: asyncpg.Pool, thread_id: UUID) -> list[str]:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT content FROM thread_messages WHERE thread_id = $1 "
            "AND role = 'ai' AND COALESCE(content, '') <> '' "
            "AND rewound_at IS NULL ORDER BY seq",
            thread_id,
        )
    return [row["content"] for row in rows]


async def _pending(pool: asyncpg.Pool, thread_id: UUID, consumed: int) -> list:
    async with pool.acquire() as conn:
        return await conn.fetch(te._PENDING_INPUT_SQL, thread_id, consumed, 10)


def _lease(authority: dict) -> dict:
    return {
        "lease_token": authority["lease_token"],
        "executor_id": authority["executor_id"],
        "executor_pod_uid": authority["executor_pod_uid"],
    }


async def _batch_continuation_admitted_by_a_dead_executor(
    pool, orchestrator, tmp_path, stack, monkeypatch, *, typed: bool
):
    """K4 up to the second kill: four children, the batch executor killed,
    executor A settles the batch into one continuation, admits it and is
    killed before any answer is durable."""

    seed = await _seed(pool, orchestrator, S1, typed_during_the_batch=typed)
    first_lease = await _successor(pool, seed)
    agent = _Agent(pool, orchestrator, seed, first_lease, tmp_path, stack)
    await agent.connect(monkeypatch)
    await agent.runtime().recover_orphans()
    assert agent.settle_requests() == 1
    (continuation,) = await _continuations(pool, seed)
    assert continuation["state"] == "queued"
    assert await _consumed(pool, seed) == seed.input_seq
    first = await _admit(
        pool,
        seed.session,
        continuation["delivery_id"],
        lease=_lease(first_lease),
        turn_number=1,
    )
    await _die(pool, seed.session)
    return seed, agent, continuation, first


# ---------------------------------------------------------------------------
# K4: the recovery turn is killed; the next executor serves the continuation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("typed", [False, True], ids=["alone", "input_typed_behind"])
async def test_a_continuation_admitted_by_a_killed_executor_is_served_again(
    pg_dsn: str, tmp_path, monkeypatch, harness, typed: bool
) -> None:
    pool = await _fresh_pool(pg_dsn)
    try:
        async with AsyncExitStack() as stack:
            orchestrator = _orchestrator_db(pool)
            (
                seed,
                _agent,
                continuation,
                first,
            ) = await _batch_continuation_admitted_by_a_dead_executor(
                pool, orchestrator, tmp_path, stack, monkeypatch, typed=typed
            )
            delivery_id = continuation["delivery_id"]
            assert (await _delivery(pool, delivery_id))["state"] == "admitted"
            assert await _answers(pool, seed.session) == []

            executor = _Executor(
                harness, monkeypatch, pool, pod=THIRD, pod_uid=THIRD_UID
            )

            checked: list[str] = []

            async def mid_turn(item: dict) -> None:
                # The current lease's own admission is never selected again,
                # neither by the pending query nor by the pre-attach check,
                # and its delivery cannot be claimed a second time.
                checked.append(item["id"])
                lease = executor.h.executor._lease
                ids = [
                    str(row["id"])
                    for row in await _pending(pool, seed.session, seed.input_seq)
                ]
                assert str(continuation["message_id"]) not in ids
                async with pool.acquire() as conn:
                    assert not await conn.fetchval(
                        te._PENDING_EVENT_EXISTS_SQL, seed.session
                    )
                    async with conn.transaction():
                        assert (
                            await claim_stateless_input_delivery(
                                conn,
                                thread_id=seed.session,
                                delivery_id=delivery_id,
                                lease_token=lease.lease_token,
                                executor_id=THIRD,
                                pod_uid=THIRD_UID,
                            )
                            is None
                        )

            executor.mid_turn = mid_turn
            claim = await executor.serve_once(seed.session)

            # The continuation is served again, first, and only once.
            (served,) = executor.served
            assert served["item"]["id"] == str(continuation["message_id"])
            assert served["item"]["delivery_id"] == str(delivery_id)
            assert served["item"]["role"] == "event"
            assert served["item"]["supersedes_input_seq"] == seed.input_seq
            # The restored copy of the admitted row was stripped: the model
            # reads it once, as this turn's input, after the four results.
            assert checked == [str(continuation["message_id"])]
            assert str(continuation["message_id"]) in executor.restored[0]
            assert str(continuation["message_id"]) not in served["context"]
            results = [str(row["id"]) for row in await _tool_rows(pool, seed)]
            assert len(results) == len(seed.call_ids)
            assert set(results) <= set(served["context"])
            stored = await _delivery(pool, delivery_id)
            assert stored["state"] == "settled"
            assert stored["claim_generation"] == first["claim_generation"] + 1
            assert stored["owner_run_queue_lease_token"] == claim.lease_token
            assert stored["owner_executor"] == THIRD
            # The human watermark stays on the superseded input.
            assert await _consumed(pool, seed) == seed.input_seq

            await executor.drive(seed.session)

            expected = [str(continuation["message_id"])]
            if typed:
                expected.append(str(seed.typed_id))
            assert executor.served_ids() == expected
            assert await _answers(pool, seed.session) == [ANSWER] * len(expected)
            assert len(await _continuations(pool, seed)) == 1
            queue = await _queue(pool, seed.session)
            assert queue["state"] == "done"
            assert queue["consumed_seq"] == continuation["seq"]
            assert queue["input_seq"] == continuation["seq"]
            assert await _pending(pool, seed.session, queue["consumed_seq"]) == []
    finally:
        await harness.cleanup()
        await pool.close()


# ---------------------------------------------------------------------------
# D3: the killed recovery turn had delegated; the settle supersedes it
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_killed_recovery_turn_that_delegated_is_settled_not_served_again(
    pg_dsn: str, tmp_path, monkeypatch, harness
) -> None:
    pool = await _fresh_pool(pg_dsn)
    try:
        async with AsyncExitStack() as stack:
            orchestrator = _orchestrator_db(pool)
            seed = await _seed(pool, orchestrator, S1, typed_during_the_batch=True)
            first_lease = await _successor(pool, seed)
            agent = _Agent(pool, orchestrator, seed, first_lease, tmp_path, stack)
            await agent.connect(monkeypatch)
            await agent.runtime().recover_orphans()
            (first,) = await _continuations(pool, seed)
            await _admit(
                pool,
                seed.session,
                first["delivery_id"],
                lease=_lease(first_lease),
                turn_number=1,
            )
            # The recovery turn delegates two more children (D3), then its
            # executor is killed.
            again_ai = uuid4()
            again = [f"call_again_{index}_{uuid4().hex[:6]}" for index in range(2)]
            async with pool.acquire() as conn:
                await conn.execute(
                    "INSERT INTO thread_messages "
                    "(id, thread_id, role, content, tool_calls, turn_number) "
                    "VALUES ($1, $2, 'ai', '', $3::jsonb, 1)",
                    again_ai,
                    seed.session,
                    json.dumps(
                        [
                            {"id": call_id, "name": "delegate_agent", "args": {}}
                            for call_id in again
                        ]
                    ),
                )
            for index, call_id in enumerate(again):
                assert await orchestrator.create_session_subagent_thread(
                    parent_thread_id=str(seed.session),
                    parent_authority=first_lease,
                    handle=f"explorer-{index:04x}",
                    subagent_type="explorer",
                    parent_tool_call_id=call_id,
                    parent_input_message_id=str(first["message_id"]),
                    parent_ai_message_id=str(again_ai),
                    parent_iteration=1,
                )
            await _die(pool, seed.session)

            executor = _Executor(
                harness, monkeypatch, pool, pod=THIRD, pod_uid=THIRD_UID
            )

            async def recover() -> None:
                # The successor's attach recovers the recovery turn's children
                # against the continuation it was serving.
                token = executor.h.executor._lease.lease_token
                agent.authority = {
                    **first_lease,
                    "lease_token": token,
                    "executor_id": THIRD,
                    "executor_pod_uid": THIRD_UID,
                }
                await agent.runtime().recover_orphans()

            executor.before_attach = recover
            await executor.drive(seed.session)

            assert agent.settle_requests() == 2
            first_row, second = await _continuations(pool, seed)
            assert second["supersedes_input_seq"] == first_row["seq"]
            assert [first_row["state"], second["state"]] == ["settled", "settled"]
            # The new continuation, then the input typed during the first
            # batch; the continuation the dead executor admitted never again.
            assert executor.served_ids() == [
                str(second["message_id"]),
                str(seed.typed_id),
            ]
            assert await _answers(pool, seed.session) == [ANSWER, ANSWER]
            queue = await _queue(pool, seed.session)
            assert queue["state"] == "done"
            assert queue["consumed_seq"] == second["seq"]
    finally:
        await harness.cleanup()
        await pool.close()


# ---------------------------------------------------------------------------
# The killed turn had already reached its end: settled, never served again
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("durable_end", ["final_answer_row", "final_reconcile"])
async def test_an_admission_whose_turn_already_ended_is_settled_not_served(
    pg_dsn: str, tmp_path, monkeypatch, harness, durable_end: str
) -> None:
    pool = await _fresh_pool(pg_dsn)
    try:
        async with AsyncExitStack() as stack:
            orchestrator = _orchestrator_db(pool)
            seed = await _seed(pool, orchestrator, S1)
            first_lease = await _successor(pool, seed)
            agent = _Agent(pool, orchestrator, seed, first_lease, tmp_path, stack)
            await agent.connect(monkeypatch)
            await agent.runtime().recover_orphans()
            (continuation,) = await _continuations(pool, seed)
            await _admit(
                pool,
                seed.session,
                continuation["delivery_id"],
                lease=_lease(first_lease),
                turn_number=1,
            )
            agent_db = _agent_db(pool)
            if durable_end == "final_answer_row":
                # The loop's incremental writer persisted the final answer;
                # the executor died before the reconcile and the settle.
                await agent_db.save_thread_message(
                    thread_id=str(seed.session),
                    role="ai",
                    content="Killed after this answer was durable.",
                    turn_number=1,
                )
            else:
                # The authoritative final reconcile committed (its last
                # message carried a tool call, so no row alone proves the
                # end); the executor died before the loop's settle.
                handle = LeaseHandle()
                handle.update(
                    str(seed.session),
                    first_lease["lease_token"],
                    executor_id=SUCCESSOR,
                    pod_uid=SUCCESSOR_UID,
                )
                reset = current_lease.set(handle)
                try:
                    assert await agent_db.save_thread_messages(
                        str(seed.session),
                        [
                            {
                                "id": str(uuid4()),
                                "role": "ai",
                                "content": "Done; saving the summary.",
                                "tool_calls": [
                                    {"id": "t_end", "name": "write_file", "args": {}}
                                ],
                                "turn_number": 1,
                            }
                        ],
                        turn_input_message_id=str(continuation["message_id"]),
                        turn_number=1,
                        memory_scope_kind="thread",
                        memory_scope_id=str(seed.session),
                    )
                finally:
                    current_lease.reset(reset)
            answers_before = await _answers(pool, seed.session)
            await _die(pool, seed.session)

            executor = _Executor(
                harness, monkeypatch, pool, pod=THIRD, pod_uid=THIRD_UID
            )
            await executor.drive(seed.session)

            assert executor.served == []
            assert (await _delivery(pool, continuation["delivery_id"]))[
                "state"
            ] == "settled"
            assert await _answers(pool, seed.session) == answers_before
            queue = await _queue(pool, seed.session)
            assert queue["state"] == "done"
            assert queue["consumed_seq"] == continuation["seq"]
    finally:
        await harness.cleanup()
        await pool.close()


# ---------------------------------------------------------------------------
# Any stateless event: an officer wake admitted by a dead executor
# ---------------------------------------------------------------------------


async def _wake_thread(pool: asyncpg.Pool) -> tuple[UUID, dict]:
    _, session, _ = await _stateless_parent(pool)
    async with pool.acquire() as conn:
        async with conn.transaction():
            wake = await persist_input_delivery(
                conn,
                thread_id=session,
                delivery_id=uuid4(),
                role="event",
                content=WAKE,
                source="officer_wake",
                turn_number=None,
            )
    assert wake["state"] == "queued"
    return session, wake


async def _claim_as(pool, session: UUID, pod: str, pod_uid: str) -> dict:
    async with pool.acquire() as conn:
        claim = await claim_unit(
            conn, unit_kind=UNIT_KIND_SESSION_TURN, pod_name=pod, prefer_unit_id=session
        )
        assert claim is not None
        await _stamp_stateless_claim(
            conn, session, token=claim.lease_token, pod=pod, pod_uid=pod_uid
        )
    return {
        "lease_token": claim.lease_token,
        "executor_id": pod,
        "executor_pod_uid": pod_uid,
    }


@pytest.mark.asyncio
async def test_an_officer_wake_admitted_by_a_dead_executor_is_served_again(
    pg_dsn: str, monkeypatch, harness
) -> None:
    pool = await _fresh_pool(pg_dsn)
    try:
        session, wake = await _wake_thread(pool)
        first = await _admit(
            pool,
            session,
            wake["delivery_id"],
            lease=await _claim_as(pool, session, POD, POD_UID),
            turn_number=int(wake["turn_number"]),
        )
        await _die(pool, session)

        executor = _Executor(
            harness, monkeypatch, pool, pod=SUCCESSOR, pod_uid=SUCCESSOR_UID
        )
        claim = await executor.claim(session)
        assert claim is not None
        async with pool.acquire() as conn:
            # Skip-if-answered runs before attach; the owed admission keeps
            # it from completing the unit.
            assert await conn.fetchval(te._PENDING_EVENT_EXISTS_SQL, session)
        await executor.h.executor._serve_claim(claim)
        assert await executor.drive(session) == 0

        (served,) = executor.served
        assert served["item"]["id"] == str(wake["message_id"])
        assert served["item"]["role"] == "event"
        assert str(wake["message_id"]) in executor.restored[0]
        assert str(wake["message_id"]) not in served["context"]
        stored = await _delivery(pool, wake["delivery_id"])
        assert stored["state"] == "settled"
        assert stored["claim_generation"] == first["claim_generation"] + 1
        assert await _answers(pool, session) == [ANSWER]
        queue = await _queue(pool, session)
        assert queue["state"] == "done"
        assert queue["consumed_seq"] == wake["seq"]
    finally:
        await harness.cleanup()
        await pool.close()


async def _steal_through_the_production_reaper(
    pool: asyncpg.Pool, thread_id: UUID
) -> None:
    """The claimant is killed and the orchestrator reaper steals its lease the
    way production does: an exact claimant becomes claim-loss debt and the
    unit sits behind a claim-loss hold until that claimant is proven gone."""

    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE run_queue SET leased_until = now() - interval '1 hour' "
            "WHERE unit_id = $1 AND state = 'leased'",
            thread_id,
        )
        stolen = await reap_expired(
            conn,
            unit_kind=UNIT_KIND_SESSION_TURN,
            grace_seconds=0.0,
            backoff_base_seconds=0.0,
            jitter=0.0,
            session_steal=reaper._try_steal_session_with_claim_loss,
        )
    assert [unit.unit_id for unit in stolen] == [thread_id]


@pytest.mark.asyncio
async def test_repeated_kills_of_one_event_turn_park_at_max_attempts(
    pg_dsn: str,
) -> None:
    """Every successor serves the owed admission again; the claim count since
    the last completion bounds it (parallel_subagents.md §8, "The unit keeps
    dying"). Through the production steal each kill leaves a claim-loss hold,
    and once the dead claimant is proven gone the unit shows the reason it was
    parked for, and its owner may retry it."""

    pool = await _fresh_pool(pg_dsn)
    try:
        session, wake = await _wake_thread(pool)
        orchestrator = _orchestrator_db(pool)
        generations = []
        for index in range(10):
            pod, pod_uid = f"executor-{index}", f"executor-uid-{index}"
            async with pool.acquire() as conn:
                claim = await claim_unit(
                    conn,
                    unit_kind=UNIT_KIND_SESSION_TURN,
                    pod_name=pod,
                    prefer_unit_id=session,
                )
            if claim is None:
                break
            async with pool.acquire() as conn:
                await _stamp_stateless_claim(
                    conn, session, token=claim.lease_token, pod=pod, pod_uid=pod_uid
                )
                ids = [
                    row["id"]
                    for row in await conn.fetch(
                        te._PENDING_INPUT_SQL, session, claim.consumed_seq, 10
                    )
                ]
            assert [str(i) for i in ids] == [str(wake["message_id"])]
            admitted = await _admit(
                pool,
                session,
                wake["delivery_id"],
                lease={
                    "lease_token": claim.lease_token,
                    "executor_id": pod,
                    "executor_pod_uid": pod_uid,
                },
                turn_number=int(wake["turn_number"]),
            )
            generations.append(admitted["claim_generation"])
            await _steal_through_the_production_reaper(pool, session)
            held = await _queue(pool, session)
            assert (held["state"], held["park_reason"]) == ("parked", "claim_loss_hold")
            # The dead claimant is proven gone (its own drain ACK, or the
            # reconciler observing its exact Pod UID terminated).
            assert await acknowledge_session_claim_quiesced(
                pool,
                thread_id=session,
                previous_lease_token=claim.lease_token,
                leased_by=pod,
                pod_uid=pod_uid,
            )
            released = await _queue(pool, session)
            if released["state"] == "parked":
                break
            # Released to its intended state, the hold's reason goes too.
            async with pool.acquire() as conn:
                parked_at = await conn.fetchval(
                    "SELECT parked_at FROM run_queue WHERE unit_id = $1", session
                )
            assert (released["state"], released["park_reason"], parked_at) == (
                "queued",
                None,
                None,
            )
        queue = await _queue(pool, session)
        assert (queue["state"], queue["park_reason"]) == (
            "parked",
            "reaper_max_attempts",
        )
        async with pool.acquire() as conn:
            max_attempts = await conn.fetchval(
                "SELECT max_attempts FROM run_queue WHERE unit_id = $1", session
            )
            owner = await conn.fetchval(
                "SELECT user_id FROM threads WHERE id = $1", session
            )
        assert len(generations) == max_attempts
        assert generations == list(range(1, max_attempts + 1))
        assert (await _delivery(pool, wake["delivery_id"]))["state"] == "admitted"

        # The owner's retry is accepted, and the next claim serves the event.
        revived = await thread_transport.thread_queue_retry(
            str(session),
            _request(),
            dependencies=_transport(orchestrator, owner, {"id": session}),
        )
        assert revived["state"] == "queued"
        assert revived["park_reason"] == "reaper_max_attempts"
        queue = await _queue(pool, session)
        assert (queue["state"], queue["park_reason"]) == ("queued", None)
        assert queue["attempts_since_completion"] == 0
        async with pool.acquire() as conn:
            claim = await claim_unit(
                conn,
                unit_kind=UNIT_KIND_SESSION_TURN,
                pod_name="executor-after-retry",
                prefer_unit_id=session,
            )
            assert claim is not None
            rows = await conn.fetch(
                te._PENDING_INPUT_SQL, session, claim.consumed_seq, 10
            )
        assert [str(row["id"]) for row in rows] == [str(wake["message_id"])]
    finally:
        await pool.close()


# ---------------------------------------------------------------------------
# The per-child recovery of an event turn leaves an owed event for replay
# ---------------------------------------------------------------------------


async def _wake_turn_delegated_one_child(pool, orchestrator, *, final_answer: bool):
    """A wake's turn delegated one child, which completed; its ToolMessage is
    durable in the parent transcript, then the executor was killed (with or
    without a final answer)."""

    session, wake = await _wake_thread(pool)
    lease = await _claim_as(pool, session, POD, POD_UID)
    await _admit(
        pool,
        session,
        wake["delivery_id"],
        lease=lease,
        turn_number=int(wake["turn_number"]),
    )
    authority = {
        "version": 1,
        "execution_lane": "stateless",
        "parent_thread_id": str(session),
        **lease,
    }
    ai_id, call_id = uuid4(), f"call_after_the_wake_{uuid4().hex[:8]}"
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO thread_messages "
            "(id, thread_id, role, content, tool_calls, turn_number) "
            "VALUES ($1, $2, 'ai', '', $3::jsonb, 1)",
            ai_id,
            session,
            json.dumps([{"id": call_id, "name": "delegate_agent", "args": {}}]),
        )
    child = await orchestrator.create_session_subagent_thread(
        parent_thread_id=str(session),
        parent_authority=authority,
        handle="reader-0001",
        subagent_type="reader",
        parent_tool_call_id=call_id,
        parent_input_message_id=str(wake["message_id"]),
        parent_ai_message_id=str(ai_id),
        parent_iteration=1,
    )
    ended = await orchestrator.terminalize_session_subagent_thread(
        parent_thread_id=str(session),
        parent_authority=authority,
        thread_id=child["thread_id"],
        runtime_generation=child["runtime_generation"],
        subagent_status="completed",
        outcome="completed",
        turns=3,
        tokens=900,
    )
    assert ended is not None and ended["result"] == "applied"
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO thread_messages "
            "(id, thread_id, role, content, tool_call_id, turn_number) "
            "VALUES ($1, $2, 'tool', '[subagent reader-0001] the report', $3, 1)",
            uuid4(),
            session,
            call_id,
        )
        if final_answer:
            await conn.execute(
                "INSERT INTO thread_messages "
                "(id, thread_id, role, content, turn_number) "
                "VALUES ($1, $2, 'ai', 'The nightly report is fine.', 1)",
                uuid4(),
                session,
            )
    await _die(pool, session)
    return session, wake, child


def _recover_child(child: dict, claim: ClaimedUnit, session: UUID, pod, uid) -> dict:
    return dict(
        parent_thread_id=str(session),
        parent_authority={
            "version": 1,
            "execution_lane": "stateless",
            "parent_thread_id": str(session),
            "lease_token": claim.lease_token,
            "executor_id": pod,
            "executor_pod_uid": uid,
        },
        thread_id=child["thread_id"],
        runtime_generation=child["runtime_generation"],
        subagent_status="completed",
        outcome="completed",
        turns=3,
        tokens=900,
        delivery_id=str(
            session_subagent_delivery_id(
                UUID(child["thread_id"]), UUID(child["runtime_generation"])
            )
        ),
        message="[subagent reader-0001 · reader · completed] transcript envelope",
        foreground_orphan_recovery=True,
    )


@pytest.mark.asyncio
async def test_per_child_recovery_leaves_an_unanswered_event_to_be_served_again(
    pg_dsn: str, monkeypatch, harness
) -> None:
    """The child's ToolMessage is durable and the turn has no answer: the
    per-child recovery (a stale listing, a racing call, an older agent)
    answers ``already_delivered`` and must neither consume nor settle the
    event: no watermark and no continuation stands for it, so the next turn
    serves it again, with the report in the transcript."""

    pool = await _fresh_pool(pg_dsn)
    try:
        orchestrator = _orchestrator_db(pool)
        session, wake, child = await _wake_turn_delegated_one_child(
            pool, orchestrator, final_answer=False
        )
        executor = _Executor(
            harness, monkeypatch, pool, pod=SUCCESSOR, pod_uid=SUCCESSOR_UID
        )
        claim = await executor.claim(session)
        assert claim is not None
        recovered = await orchestrator.terminalize_session_subagent_thread(
            **_recover_child(child, claim, session, SUCCESSOR, SUCCESSOR_UID)
        )
        assert recovered["result"] == "already_delivered"
        assert (await _delivery(pool, wake["delivery_id"]))["state"] == "admitted"
        assert (await _queue(pool, session))["consumed_seq"] < wake["seq"]

        await executor.h.executor._serve_claim(claim)
        assert await executor.drive(session) == 0

        (served,) = executor.served
        assert served["item"]["id"] == str(wake["message_id"])
        assert str(wake["message_id"]) not in served["context"]
        assert (await _delivery(pool, wake["delivery_id"]))["state"] == "settled"
        assert await _answers(pool, session) == [ANSWER]
        queue = await _queue(pool, session)
        assert (queue["state"], queue["consumed_seq"]) == ("done", wake["seq"])
    finally:
        await harness.cleanup()
        await pool.close()


@pytest.mark.asyncio
async def test_per_child_recovery_consumes_an_event_its_turn_answered(
    pg_dsn: str, monkeypatch, harness
) -> None:
    """Unchanged: when the turn did answer, the per-child recovery consumes
    the event (watermark and settlement) and nothing is served again."""

    pool = await _fresh_pool(pg_dsn)
    try:
        orchestrator = _orchestrator_db(pool)
        session, wake, child = await _wake_turn_delegated_one_child(
            pool, orchestrator, final_answer=True
        )
        executor = _Executor(
            harness, monkeypatch, pool, pod=SUCCESSOR, pod_uid=SUCCESSOR_UID
        )
        claim = await executor.claim(session)
        assert claim is not None
        recovered = await orchestrator.terminalize_session_subagent_thread(
            **_recover_child(child, claim, session, SUCCESSOR, SUCCESSOR_UID)
        )
        assert recovered["result"] == "already_delivered"
        assert (await _delivery(pool, wake["delivery_id"]))["state"] == "settled"
        assert (await _queue(pool, session))["consumed_seq"] == wake["seq"]

        await executor.h.executor._serve_claim(claim)
        assert await executor.drive(session) == 0

        assert executor.served == []
        assert await _answers(pool, session) == ["The nightly report is fine."]
        assert (await _queue(pool, session))["state"] == "done"
    finally:
        await harness.cleanup()
        await pool.close()


# ---------------------------------------------------------------------------
# What must not change
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_human_input_of_a_dead_executor_is_served_again_as_before(
    pg_dsn: str, monkeypatch, harness
) -> None:
    pool = await _fresh_pool(pg_dsn)
    try:
        owner, session, _ = await _stateless_parent(pool)
        human_id = uuid4()
        async with pool.acquire() as conn:
            human_seq = await conn.fetchval(
                "INSERT INTO thread_messages (id, thread_id, role, content, "
                "turn_number) VALUES ($1, $2, 'human', 'hello', 1) RETURNING seq",
                human_id,
                session,
            )
            await conn.execute("UPDATE threads SET total_turns=1 WHERE id=$1", session)
            await record_input_seq(
                conn,
                unit_id=session,
                unit_kind=UNIT_KIND_SESSION_TURN,
                input_seq=int(human_seq),
                fair_key=str(owner),
            )
        await _claim_as(pool, session, POD, POD_UID)
        await _die(pool, session)
        rows = await _pending(pool, session, int(human_seq) - 1)
        assert [(row["id"], row["delivery_state"]) for row in rows] == [
            (human_id, None)
        ]
        assert rows[0]["admission_answered"] is False

        executor = _Executor(
            harness, monkeypatch, pool, pod=SUCCESSOR, pod_uid=SUCCESSOR_UID
        )
        assert await executor.drive(session) == 1
        (served,) = executor.served
        assert served["item"] == {"content": "hello", "id": str(human_id)}
        assert str(human_id) not in served["context"]
        queue = await _queue(pool, session)
        assert (queue["state"], queue["consumed_seq"]) == ("done", human_seq)
    finally:
        await harness.cleanup()
        await pool.close()


@pytest.mark.asyncio
async def test_an_unanswered_admission_below_the_watermark_is_not_replayed(
    pg_dsn: str,
) -> None:
    """History stranded before this rule: the watermark already passed the
    event. It is not replayed into a newer conversation: it is not owed, the
    unit is not held open for it, and the claim and the settle refuse it."""

    pool = await _fresh_pool(pg_dsn)
    try:
        session, wake = await _wake_thread(pool)
        await _admit(
            pool,
            session,
            wake["delivery_id"],
            lease=await _claim_as(pool, session, POD, POD_UID),
            turn_number=int(wake["turn_number"]),
        )
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE run_queue SET consumed_seq = $2 WHERE unit_id = $1",
                session,
                int(wake["seq"]),
            )
        await _die(pool, session)
        lease = await _claim_as(pool, session, SUCCESSOR, SUCCESSOR_UID)
        async with pool.acquire() as conn:
            assert (
                await conn.fetch(te._PENDING_INPUT_SQL, session, int(wake["seq"]), 10)
                == []
            )
            assert not await conn.fetchval(te._PENDING_EVENT_EXISTS_SQL, session)
            async with conn.transaction():
                assert (
                    await claim_stateless_input_delivery(
                        conn,
                        thread_id=session,
                        delivery_id=wake["delivery_id"],
                        lease_token=lease["lease_token"],
                        executor_id=SUCCESSOR,
                        pod_uid=SUCCESSOR_UID,
                    )
                    is None
                )
                assert (
                    await settle_answered_stateless_admission(
                        conn,
                        thread_id=session,
                        delivery_id=wake["delivery_id"],
                        lease_token=lease["lease_token"],
                        executor_id=SUCCESSOR,
                        pod_uid=SUCCESSOR_UID,
                    )
                    is None
                )
        assert (await _delivery(pool, wake["delivery_id"]))["state"] == "admitted"
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_an_answered_admission_below_the_watermark_is_settled(
    pg_dsn: str, monkeypatch, harness
) -> None:
    """The turn ran to its end and its close checkpointed the event, but the
    loop's settle lost the race with the completion. The next claim settles
    the admission (an unsettled delivery blocks rewind) and serves nothing,
    even though the skip-if-answered watermark leg would otherwise apply."""

    pool = await _fresh_pool(pg_dsn)
    try:
        session, wake = await _wake_thread(pool)
        await _admit(
            pool,
            session,
            wake["delivery_id"],
            lease=await _claim_as(pool, session, POD, POD_UID),
            turn_number=int(wake["turn_number"]),
        )
        await _agent_db(pool).save_thread_message(
            thread_id=str(session),
            role="ai",
            content="The report job finished; nothing failed.",
            turn_number=int(wake["turn_number"]),
        )
        async with pool.acquire() as conn:
            # The close checkpointed the event; completion then took the row
            # off 'leased' before the loop's settle ran.
            await conn.execute(
                "UPDATE run_queue SET consumed_seq = $2 WHERE unit_id = $1",
                session,
                int(wake["seq"]),
            )
        await _die(pool, session)

        executor = _Executor(
            harness, monkeypatch, pool, pod=SUCCESSOR, pod_uid=SUCCESSOR_UID
        )
        claim = await executor.claim(session)
        assert claim is not None
        assert claim.consumed_seq == claim.input_seq == wake["seq"]
        async with pool.acquire() as conn:
            assert await conn.fetchval(te._PENDING_EVENT_EXISTS_SQL, session)
        await executor.h.executor._serve_claim(claim)
        assert await executor.drive(session) == 0

        assert executor.served == []
        assert (await _delivery(pool, wake["delivery_id"]))["state"] == "settled"
        assert await _answers(pool, session) == [
            "The report job finished; nothing failed."
        ]
        queue = await _queue(pool, session)
        assert (queue["state"], queue["consumed_seq"]) == ("done", wake["seq"])
        async with pool.acquire() as conn:
            assert not await conn.fetchval(
                "SELECT EXISTS (SELECT 1 FROM thread_input_deliveries "
                "WHERE thread_id=$1 AND state NOT IN ('settled','cancelled'))",
                session,
            )
    finally:
        await harness.cleanup()
        await pool.close()


@pytest.mark.asyncio
async def test_a_superseded_admission_is_never_owed(
    pg_dsn: str, tmp_path, monkeypatch
) -> None:
    """A continuation that supersedes the admitted event takes its place, even
    if the event's own delivery was left unsettled (the settle that writes the
    continuation settles it; this is the guard behind that)."""

    pool = await _fresh_pool(pg_dsn)
    try:
        async with AsyncExitStack() as stack:
            orchestrator = _orchestrator_db(pool)
            (
                seed,
                _agent,
                continuation,
                _first,
            ) = await _batch_continuation_admitted_by_a_dead_executor(
                pool, orchestrator, tmp_path, stack, monkeypatch, typed=False
            )
            async with pool.acquire() as conn:
                async with conn.transaction():
                    successor = await persist_input_delivery(
                        conn,
                        thread_id=seed.session,
                        delivery_id=uuid4(),
                        role="event",
                        content="[continuation of the continuation]",
                        source="subagent",
                        turn_number=1,
                        allow_stateless_subagent_event=True,
                        supersedes_input_seq=int(continuation["seq"]),
                    )
            lease = await _claim_as(pool, seed.session, THIRD, THIRD_UID)
            rows = await _pending(pool, seed.session, seed.input_seq)
            assert [str(row["id"]) for row in rows] == [str(successor["message_id"])]
            async with pool.acquire() as conn:
                async with conn.transaction():
                    assert (
                        await claim_stateless_input_delivery(
                            conn,
                            thread_id=seed.session,
                            delivery_id=continuation["delivery_id"],
                            lease_token=lease["lease_token"],
                            executor_id=THIRD,
                            pod_uid=THIRD_UID,
                        )
                        is None
                    )
    finally:
        await pool.close()
