"""A graceful shutdown during a session's delegation batch, on Postgres (WP3d).

Design: knowledge-base/knowledge/features/parallel_subagents.md §6.3 (the
Shutdown bullet), §7 (Shutdown row), §8 ("Graceful shutdown during the
batch": children stopped, no results, claim released, successor settles,
unit not parked), §9.

The dying executor is the real one (``StatelessTurnExecutor.stop`` and its
dispose step) around the real loop turn, runtime, ledger, in-process
orchestrator over HTTP and Postgres; only the physical session detach is
reduced to what it does to this turn (cancel the loop, quiesce the runtime).
The successor is the real recovery (``recover_orphans``) and the settle.
"""

from __future__ import annotations

import asyncio
import json
import shutil
from contextlib import AsyncExitStack
from types import SimpleNamespace
from typing import Any, List
from unittest.mock import Mock
from uuid import UUID, uuid4

import pytest
from langchain_core.messages import (
    AIMessage,
    HumanMessage,
    SystemMessage,
    ToolMessage,
)

import agent.api.persistent_app as pa
import agent.api.turn_executor as te
import agent.persistent_graph as persistent_graph
from agent.core.thread_messages import _serialize_message_row
from agent.persistent_graph import PERSIST_ROLE_KEY, _execute_turn
from agent.subagents import SessionHost, SubagentRuntime
from agent.subagents.batch_recovery import INTERRUPTED_HEADER
from agent.subagents.session_persistence import SessionSubagentLedger
from agent.tools.delegation.delegate_agent import create_delegate_agent_tools
from shared.run_queue import (
    UNIT_KIND_SESSION_TURN,
    claim_unit,
    record_input_seq,
)
from shared.session_subagent_batch import not_started_result_text
from tests._fake_chat_model import FakeChatModel, text_turn
from tests._fanout_gate import set_session_fanout
from tests.test_persistent_delegation_batch import _callbacks, _config, _context_manager
from tests.test_session_input_runtime import _runtime as _input_runtime
from tests.test_session_input_runtime import _World
from tests.test_session_subagent_batch_recovery_pg import _Agent, _run_turn, _serve
from tests.test_session_subagent_batch_settle_pg import (
    INPUT,
    POD,
    POD_UID,
    SUCCESSOR,
    SUCCESSOR_UID,
    Seed,
    _assert_invariants,
    _consumed,
    _continuations,
    _fresh_pool,
    _metrics,
    _seed,
    _stateless_parent,
    _tool_rows,
)
from tests.test_subagent_thread_migration import (  # noqa: F401  (scratch_pg_dsn fixture)
    _agent_db,
    _orchestrator_db,
    _stamp_stateless_claim,
    scratch_pg_dsn,
)


@pytest.fixture(scope="module")
def pg_dsn(scratch_pg_dsn: str) -> str:  # noqa: F811 (pytest fixture param)
    """The migration test's scratch Postgres (testcontainers), reused as-is."""
    return scratch_pg_dsn


@pytest.fixture(autouse=True)
def _offline_token_counts(monkeypatch):
    """Real children count tokens; never fetch tokenizer assets here."""
    from agent.core import context
    from shared.runtime.core import chunk_planner

    monkeypatch.setattr(context, "TIKTOKEN_AVAILABLE", False)
    monkeypatch.setattr(chunk_planner, "TIKTOKEN_AVAILABLE", False)
    monkeypatch.setattr(persistent_graph, "_DELEGATION_INTERRUPT_POLL_S", 0.01)


async def _recoverable(pool, seed: Seed, opened: list[tuple[str, str]]) -> bool:
    async with pool.acquire() as conn:
        return await conn.fetchval(
            te._DELEGATION_BATCH_RECOVERABLE_SQL,
            seed.session,
            -1,
            [child_id for _, child_id in opened],
            [call_id for call_id, _ in opened],
        )


async def _durable(pool, seed: Seed) -> bool:
    async with pool.acquire() as conn:
        return await conn.fetchval(te._TOOL_EFFECTS_DURABLE_SQL, seed.session, -1)


# ---------------------------------------------------------------------------
# The release condition, as durable facts
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_release_condition_reads_the_batch_and_its_child_rows(
    pg_dsn: str,
) -> None:
    pool = await _fresh_pool(pg_dsn)
    try:
        orchestrator = _orchestrator_db(pool)
        seed = await _seed(pool, orchestrator, ["completed", "running", "none", "none"])
        ended, running, queued, _ = seed.call_ids
        opened = [
            (call_id, seed.children[call_id]["thread_id"])
            for call_id in (ended, running)
        ]

        # Four open delegate calls: today's check parks, the batch releases.
        assert await _durable(pool, seed) is False
        assert await _recoverable(pool, seed, opened) is True
        # Nothing opened (every call still queued, or a pending approval).
        assert await _recoverable(pool, seed, []) is True
        # An opened child without its durable row: never assumed.
        assert await _recoverable(pool, seed, [*opened, (queued, str(uuid4()))]) is (
            False
        )
        # A row that belongs to another call is not this call's row.
        assert await _recoverable(pool, seed, [(queued, opened[0][1])]) is False
        # Calls before the claim's watermark are not this turn's.
        async with pool.acquire() as conn:
            assert (
                await conn.fetchval(
                    te._DELEGATION_BATCH_RECOVERABLE_SQL,
                    seed.session,
                    seed.ai_seq,
                    [],
                    [],
                )
                is False
            )

        # Any other open call keeps today's park, alone or beside the batch.
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO thread_messages "
                "(id, thread_id, role, content, tool_calls, turn_number) "
                "VALUES ($1, $2, 'ai', '', $3::jsonb, 1)",
                uuid4(),
                seed.session,
                json.dumps(
                    [
                        {"id": "call_shell", "name": "shell_execute", "args": {}},
                        {"id": "call_more", "name": "delegate_agent", "args": {}},
                    ]
                ),
            )
        assert await _recoverable(pool, seed, opened) is False
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO thread_messages "
                "(id, thread_id, role, content, tool_call_id, turn_number) "
                "VALUES ($1, $2, 'tool', 'ran', 'call_shell', 1)",
                uuid4(),
                seed.session,
            )
        # The other call has its result, but it shared a row with an open
        # delegate call: not a delegation-only batch.
        assert await _recoverable(pool, seed, opened) is False
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO thread_messages "
                "(id, thread_id, role, content, tool_call_id, turn_number) "
                "VALUES ($1, $2, 'tool', 'refused', 'call_more', 1)",
                uuid4(),
                seed.session,
            )
        # Answered calls (a finished earlier round) never block the release.
        assert await _recoverable(pool, seed, opened) is True

        # Every call answered: the ordinary branch applies, not this one.
        async with pool.acquire() as conn:
            for call_id in seed.call_ids:
                await conn.execute(
                    "INSERT INTO thread_messages "
                    "(id, thread_id, role, content, tool_call_id, turn_number) "
                    "VALUES ($1, $2, 'tool', 'result', $3, 1)",
                    uuid4(),
                    seed.session,
                    call_id,
                )
        assert await _durable(pool, seed) is True
        assert await _recoverable(pool, seed, opened) is False
    finally:
        await pool.close()


# ---------------------------------------------------------------------------
# The §8 row: four calls, cap two, the executor shuts down gracefully
# ---------------------------------------------------------------------------


REPORT = "Austria: take-home pay at 60,000 EUR gross is 41,200 EUR."


class _Child(FakeChatModel):
    """The first child to reach its provider answers at once; every later one
    stays in its first provider call until it is cancelled. (Which call gets
    a slot first follows the ledger lookups, so the test reads it back from
    the durable rows.)"""

    def __init__(self, started: List[str]):
        super().__init__([])
        self.started = started

    async def astream(self, messages, **kw):
        self.calls.append(list(messages))
        brief = " ".join(str(m.content) for m in messages if m.type == "human")
        self.started.append(brief)
        if len(self.started) == 1:
            for chunk in text_turn(REPORT):
                await asyncio.sleep(0)
                yield chunk
            return
        await asyncio.Event().wait()
        yield  # pragma: no cover


async def _stateless_turn(pool) -> Seed:
    """A stateless session whose executor claimed the user's input."""

    owner, session, _ = await _stateless_parent(pool)
    input_id = uuid4()
    async with pool.acquire() as conn:
        input_seq = await conn.fetchval(
            "INSERT INTO thread_messages (id, thread_id, role, content, "
            "turn_number) VALUES ($1, $2, 'human', $3, 1) RETURNING seq",
            input_id,
            session,
            INPUT,
        )
        await conn.execute("UPDATE threads SET total_turns=1 WHERE id=$1", session)
        await record_input_seq(
            conn,
            unit_id=session,
            unit_kind=UNIT_KIND_SESSION_TURN,
            input_seq=int(input_seq),
            fair_key=str(owner),
        )
        claim = await claim_unit(
            conn, unit_kind=UNIT_KIND_SESSION_TURN, pod_name=POD, prefer_unit_id=session
        )
        assert claim is not None
        await _stamp_stateless_claim(
            conn, session, token=claim.lease_token, pod=POD, pod_uid=POD_UID
        )
    seed = Seed(
        lane="stateless",
        session=session,
        owner=owner,
        input_id=input_id,
        input_seq=int(input_seq),
        ai_id=uuid4(),
        ai_seq=0,
        call_ids=[f"call_country_{index}_{uuid4().hex[:8]}" for index in range(4)],
        authority={
            "version": 1,
            "execution_lane": "stateless",
            "parent_thread_id": str(session),
            "lease_token": claim.lease_token,
            "executor_id": POD,
            "executor_pod_uid": POD_UID,
        },
    )
    seed.claim = claim  # type: ignore[attr-defined]
    return seed


async def _child_rows(pool, seed: Seed) -> dict[str, Any]:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, runtime_generation, status, subagent_status, "
            "subagent_outcome, report_path, subagent_handle AS handle, "
            "parent_tool_call_id "
            "FROM threads WHERE kind='subagent' AND parent_thread_id=$1",
            seed.session,
        )
    return {row["parent_tool_call_id"]: row for row in rows}


async def _queue_row(pool, seed: Seed):
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            "SELECT state, input_seq, consumed_seq, lease_token, park_reason "
            "FROM run_queue WHERE unit_id=$1",
            seed.session,
        )


@pytest.mark.asyncio
async def test_a_graceful_shutdown_during_a_batch_is_settled_by_the_successor(
    pg_dsn: str, tmp_path, monkeypatch
) -> None:
    set_session_fanout(monkeypatch)
    pool = await _fresh_pool(pg_dsn)
    try:
        async with AsyncExitStack() as stack:
            orchestrator = _orchestrator_db(pool)
            seed = await _stateless_turn(pool)
            claim = seed.claim  # type: ignore[attr-defined]
            before = await _queue_row(pool, seed)
            assert before["consumed_seq"] < seed.input_seq

            # --- The dying executor's session: parent turn, runtime, tool ---
            dying = _Agent(
                pool, orchestrator, seed, seed.authority, tmp_path / "dying", stack
            )
            await dying.connect(monkeypatch)
            context = dying.context
            context.config["delegation"]["session_max_concurrent"] = 2
            context._current_input_message_id = str(seed.input_id)
            context._current_turn_count = 1
            started: List[str] = []
            host = SessionHost(
                thread_id=str(seed.session),
                agent_type="persistent",
                tool_context=context,
                postgres=dying.agent_db,
                admission_fn=lambda: True,
                effect_authority_fn=lambda: True,
                settlement_authority_fn=lambda: True,
            )
            ledger = SessionSubagentLedger.from_context(context)
            assert ledger is not None
            runtime = SubagentRuntime.from_context(
                context,
                host,
                ledger=ledger,
                llm_factory=lambda _config, _limits: _Child(started),
                driver_kwargs={
                    "watcher_poll_interval": 0.01,
                    "archiver": None,
                    "archive_fn": lambda **kwargs: None,
                },
            )
            context._parent_host = host
            context.subagent_runtime = runtime
            tool = create_delegate_agent_tools(context)[0]

            # The session's real input owner, inside turn 1 with the batch's
            # tools in flight.
            owner = _input_runtime(
                _World(
                    thread_id=str(seed.session),
                    turn_open=True,
                    turn_count=1,
                    tool_inflight=True,
                )
            )
            owner.begin_attach()

            persisted: List[Any] = []

            async def persist(message) -> bool:
                await dying.agent_db.save_thread_message(
                    str(seed.session), **_serialize_message_row(message, 1)
                )
                persisted.append(message)
                return True

            calls = [
                {
                    "name": "delegate_agent",
                    "id": call_id,
                    "args": {
                        "description": f"country {index}",
                        "prompt": f"Take-home pay in country {index}.",
                        "subagent_type": "explorer",
                        "run_in_background": False,
                    },
                }
                for index, call_id in enumerate(seed.call_ids)
            ]
            parent = FakeChatModel(
                [
                    [AIMessage(content="", id=str(seed.ai_id), tool_calls=calls)],
                    text_turn("never asked"),
                ]
            )
            context_manager = _context_manager()
            context_manager.record_provider_usage = Mock()

            async def authority() -> bool:
                return True

            turn = asyncio.create_task(
                _execute_turn(
                    llm_with_tools=parent,
                    tool_map={"delegate_agent": tool},
                    context_manager=context_manager,
                    messages=[
                        SystemMessage(content="Compare take-home pay."),
                        HumanMessage(content=INPUT, id=str(seed.input_id)),
                    ],
                    callbacks=_callbacks(
                        persist_message=persist,
                        require_delegation_persistence=True,
                        before_provider_admission=lambda: True,
                        before_provider_execution=authority,
                        check_interrupt=owner.check_interrupt,
                        peek_interrupt_cause=owner.peek_interrupt_cause,
                        hard_interrupt_event=owner.hard_interrupt_event,
                    ),
                    llm_timeout=10,
                    auxiliary_llm=None,
                    config=_config(),
                    tool_context=context,
                    turn_id=1,
                )
            )

            # One child finished, the next two run, the last call is queued.
            deadline = asyncio.get_running_loop().time() + 15
            while True:
                rows = await _child_rows(pool, seed)
                states = sorted(str(row["subagent_status"]) for row in rows.values())
                # A row opens before its child's first provider call.
                if states == ["completed", "running", "running"] and len(started) == 3:
                    break
                assert asyncio.get_running_loop().time() < deadline, states
                await asyncio.sleep(0.02)
            assert len(started) == 3
            (finished,) = [
                c for c, r in rows.items() if r["subagent_status"] == "completed"
            ]
            running = [c for c in seed.call_ids if c in rows and c != finished]
            (queued,) = [c for c in seed.call_ids if c not in rows]

            # --- The executor: this claim, this turn, a tool effect crossed ---
            executor = te.StatelessTurnExecutor(
                pod_name=POD,
                pod_uid=POD_UID,
                abort_grace_seconds=0.3,
                worker_enabled=False,
                bg_task_enabled=False,
                completion_commands_enabled=False,
                audit_writer=None,
            )
            executor._activate_lease(str(seed.session), claim.lease_token)
            executor._tool_effect_identity = (str(seed.session), claim.lease_token, 1)
            monkeypatch.setattr(
                pa, "_agent", SimpleNamespace(postgres_conn=dying.agent_db)
            )
            monkeypatch.setattr(pa, "_session", SimpleNamespace(tool_context=context))
            monkeypatch.setattr(pa, "_thread_id", str(seed.session))
            monkeypatch.setattr(pa, "_interrupt_owner_lease_token", claim.lease_token)
            monkeypatch.setattr(pa, "_interrupt_owner_turn_id", 1)
            monkeypatch.setattr(pa, "_session_input", owner)
            monkeypatch.setattr(pa, "_pending_cloud_push_task", None)
            detached: List[str] = []

            async def detach(reason: str) -> None:
                # What the physical detach does to this turn: cancel the loop
                # (its batch joins every child), then quiesce the runtime.
                detached.append(reason)
                turn.cancel()
                await asyncio.gather(turn, return_exceptions=True)
                await runtime.quiesce("session background work quiescing")

            monkeypatch.setattr(executor, "_detach_cached_session", detach)

            async def serving() -> None:
                # The claim loop waiting on the turn, as ``_await_turn`` does
                # (never cancelling the loop itself); ``stop``'s cancel lands
                # here and disposes of the exact claim.
                try:
                    await asyncio.wait({turn})
                except asyncio.CancelledError:
                    await executor._shutdown_dispose_cancelled_claim(claim)
                    raise

            executor._task = asyncio.create_task(serving())
            await asyncio.wait_for(executor.stop(timeout=0.05), timeout=20)

            # The shutdown abort did not stop the batch: no child was asked
            # for a partial answer, and the abort is still pending.
            assert owner.interrupt_cause == "shutdown"
            assert len(started) == 3
            assert runtime.foreground_batch_stopped is False
            assert detached == ["shutdown_cancelled_claim"]
            assert turn.cancelled()

            # No result was written by the dying executor.
            assert [m for m in persisted if isinstance(m, ToolMessage)] == []
            assert await _tool_rows(pool, seed) == []
            # The children keep a crash's durable state.
            rows = await _child_rows(pool, seed)
            assert rows[finished]["subagent_status"] == "completed"
            assert rows[finished]["report_path"] == (
                f".subagents/{rows[finished]['handle']}/report.md"
            )
            assert [rows[call]["subagent_status"] for call in running] == [
                "running",
                "running",
            ]
            assert queued not in rows
            # Released, not parked; the input is not consumed.
            unit = await _queue_row(pool, seed)
            assert unit["state"] == "queued"
            assert unit["park_reason"] is None
            assert unit["consumed_seq"] == before["consumed_seq"]
            assert unit["input_seq"] == seed.input_seq

            # --- The successor claims the unit and settles the batch ---
            async with pool.acquire() as conn:
                # The release's backoff has elapsed.
                await conn.execute(
                    "UPDATE run_queue SET run_after = now() WHERE unit_id=$1",
                    seed.session,
                )
                successor_claim = await claim_unit(
                    conn,
                    unit_kind=UNIT_KIND_SESSION_TURN,
                    pod_name=SUCCESSOR,
                    prefer_unit_id=seed.session,
                )
                assert successor_claim is not None
                await _stamp_stateless_claim(
                    conn,
                    seed.session,
                    token=successor_claim.lease_token,
                    pod=SUCCESSOR,
                    pod_uid=SUCCESSOR_UID,
                )
            authority_2 = {
                **seed.authority,
                "lease_token": successor_claim.lease_token,
                "executor_id": SUCCESSOR,
                "executor_pod_uid": SUCCESSOR_UID,
            }
            async with pool.acquire() as conn:
                seed.ai_seq = await conn.fetchval(
                    "SELECT seq FROM thread_messages WHERE id=$1", seed.ai_id
                )
            seed.children = {
                call_id: {
                    "thread_id": str(row["id"]),
                    "runtime_generation": str(row["runtime_generation"]),
                    "handle": row["handle"],
                }
                for call_id, row in rows.items()
            }
            successor = _Agent(
                pool, orchestrator, seed, authority_2, tmp_path / "successor", stack
            )
            await successor.connect(monkeypatch)
            # One workspace: the finished child's spilled report is there.
            shutil.copytree(dying.root / ".subagents", successor.root / ".subagents")
            recovered = await successor.runtime().recover_orphans()

            assert successor.settle_requests() == 1
            assert sorted(entry["status"] for entry in recovered) == [
                "completed",
                "interrupted",
                "interrupted",
            ]
            results = {row["tool_call_id"]: row for row in await _tool_rows(pool, seed)}
            # One result per call, in provider order.
            assert list(results) == seed.call_ids
            assert REPORT in results[finished]["content"]
            for call_id in running:
                assert results[call_id]["content"].startswith(INTERRUPTED_HEADER)
            assert results[queued]["content"] == not_started_result_text()
            # The cockpit's class per call.
            classes = {call: _metrics(results[call])["class"] for call in seed.call_ids}
            assert classes == {
                finished: "completed",
                running[0]: "interrupted",
                running[1]: "interrupted",
                queued: "not_started",
            }
            rows = await _child_rows(pool, seed)
            for call_id in running:
                assert (
                    rows[call_id]["subagent_status"],
                    rows[call_id]["subagent_outcome"],
                ) == ("interrupted", "interrupted:parent_restart")
            (continuation,) = await _continuations(pool, seed)
            assert _metrics(continuation)["finished"] == 1
            assert _metrics(continuation)["interrupted"] == 2
            assert _metrics(continuation)["not_started"] == 1
            assert await _consumed(pool, seed) == seed.input_seq
            await _assert_invariants(
                pool, orchestrator, seed, authority_2, settled=True
            )

            # --- The next turn answers ---
            pending = await successor.pending()
            assert [row["id"] for row in pending] == [str(continuation["message_id"])]
            target = pending[0]
            restored = await successor.restored()
            continuation_input = HumanMessage(
                content=target["content"], id=target["id"]
            )
            continuation_input.additional_kwargs[PERSIST_ROLE_KEY] = "event"
            provider_input = await _run_turn(
                successor, restored, continuation_input, "The comparison: ..."
            )
            assert [message.type for message in provider_input] == (
                ["system", "human", "ai"] + ["tool"] * 4 + ["human"]
            )
            assert [m.tool_call_id for m in provider_input[3:7]] == seed.call_ids
            await _serve(
                successor, target, lease=authority_2, answer="The comparison: ..."
            )
            assert await successor.pending() == []
            async with pool.acquire() as conn:
                answers = await conn.fetch(
                    "SELECT content FROM thread_messages WHERE thread_id=$1 "
                    "AND role='ai' AND content <> '' ORDER BY seq",
                    seed.session,
                )
            assert [row["content"] for row in answers] == ["The comparison: ..."]
            assert len(await _continuations(pool, seed)) == 1
            assert UUID(str(continuation["message_id"]))
    finally:
        await pool.close()
