"""Agent recovery of an abandoned session delegation turn, end to end (WP2).

Design: knowledge-base/knowledge/features/parallel_subagents.md §6.2 (the
agent side), §9 (invariants 7, 9, 10, 12) and D3.

A stateless session delegated four briefs in one assistant message. When its
executor died, one child had finished (its report spilled to the workspace),
one was still running, two had never started, and the user had typed more
input. The successor runs the agent's own recovery (``recover_orphans``)
against the real orchestrator, over HTTP in process, on a real Postgres, then
serves the recovery turn to a fake model that records what it was sent.

Also here: the executor close after a recovery turn whose superseded input is
an event, for the D3 re-delegation and for today's single-child recovery of a
turn that a server event started.
"""

from __future__ import annotations

import json
from collections import Counter
from contextlib import AsyncExitStack
from typing import Any
from unittest.mock import Mock
from uuid import UUID, uuid4

import httpx
import pytest
from langchain_core.messages import HumanMessage, SystemMessage

import agent.subagents.runtime as runtime_module
from agent.api.orchestrator_client import OrchestratorClient
from agent.api.persistent_app import _db_rows_to_lc_messages
from agent.api.turn_executor import (
    _PENDING_INPUT_SQL,
    completed_input_checkpoint,
    strip_restored_pending_humans,
)
from agent.core.context import (
    repair_tool_pairing,
    sanitize_history_for_provider_boundary,
)
from agent.core.thread_messages import _persist_one_message
from agent.persistent_graph import PERSIST_ROLE_KEY, _execute_turn
from agent.subagents import SessionHost, SubagentRuntime
from agent.subagents.batch_recovery import INTERRUPTED_HEADER
from agent.subagents.session_persistence import SessionSubagentLedger
from shared.persistent_input_delivery import (
    claim_stateless_input_delivery,
    persist_input_delivery,
    transition_stateless_input_delivery,
)
from shared.run_queue import (
    UNIT_KIND_SESSION_TURN,
    claim_unit,
    close_interrupt_admission,
    complete_unit,
    open_interrupt_admission,
    record_input_seq,
    release_unit,
)
from shared.session_subagent_batch import (
    RECOVERY_METRICS_KEY,
    not_started_result_text,
    session_subagent_batch_delivery_id,
)
from tests._fake_chat_model import FakeChatModel, text_turn
from tests.test_persistent_delegation_batch import _callbacks, _config, _context_manager
from tests.test_session_subagent_batch_settle_pg import (
    S1,
    TYPED_DURING_THE_BATCH,
    Seed,
    _consumed,
    _continuations,
    _fresh_pool,
    _seed,
    _snapshot,
    _successor,
    _tool_rows,
)
from tests.test_subagent_runtime import make_parent
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


MODEL = "claude-opus-5-5"
REPORT = (
    "Austria: take-home pay at 60,000 EUR gross is 41,200 EUR. "
    "Source: BMF Brutto-Netto-Rechner 2026."
)
LAST_WORDS = "Germany: income tax done, still checking the solidarity surcharge."
THIRD, THIRD_UID = "stateless-executor-3", "stateless-executor-pod-3"


class _Agent:
    """The successor's agent side: a session parent and its recovery runtime."""

    def __init__(self, pool, orchestrator, seed: Seed, authority, tmp_path, stack):
        self.pool = pool
        self.orchestrator = orchestrator
        self.seed = seed
        self.authority = authority
        self.tmp_path = tmp_path
        self.stack = stack
        self.agent_db = _agent_db(pool)
        self.requests: list[str] = []
        self.client: OrchestratorClient | None = None
        self.context = None
        self.root = None

    async def connect(self, monkeypatch) -> None:
        from orchestrator import main
        from orchestrator.security import access

        monkeypatch.setattr(main.app.state.resources, "postgres_db", self.orchestrator)
        monkeypatch.setattr(access, "_INTERNAL_KEY", "batch-recovery-internal-key")

        async def record(request):
            self.requests.append(request.url.path)

        client = OrchestratorClient(
            "http://orchestrator.test", "127.0.0.1", 8002, "successor", "session_base"
        )
        client._client = await self.stack.enter_async_context(
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=main.app),
                headers={"X-Internal-Key": "batch-recovery-internal-key"},
                event_hooks={"request": [record]},
            )
        )
        self.client = client
        context, root = make_parent(self.tmp_path)
        context._subagent_parent_kind = "session"
        context._subagent_execution_lane = "stateless"
        context._thread_id = str(self.seed.session)
        context._job_id = str(self.seed.session)
        context.postgres_db = self.agent_db
        context.orchestrator_client = client
        context.provider_admission = lambda: True
        # No attach advertisement: the listed plans alone prove that the
        # orchestrator settles a batch (the flag gates creating wider ones).
        context._session_subagent_batch_settle_contract = False
        context._session_parent_authority_provider = lambda: self.authority
        self.context, self.root = context, root

    def runtime(self) -> SubagentRuntime:
        """A fresh runtime, as a fresh attach builds one."""

        def no_provider(*_args, **_kwargs):
            raise AssertionError("recovery constructed a provider")

        host = SessionHost(
            thread_id=str(self.seed.session),
            agent_type="persistent",
            tool_context=self.context,
            postgres=self.agent_db,
            admission_fn=lambda: True,
            effect_authority_fn=lambda: True,
            settlement_authority_fn=lambda: True,
        )
        ledger = SessionSubagentLedger.from_context(self.context)
        assert ledger is not None
        runtime = SubagentRuntime.from_context(
            self.context, host, ledger=ledger, llm_factory=no_provider
        )
        self.context._parent_host = host
        self.context.subagent_runtime = runtime
        return runtime

    def settle_requests(self) -> int:
        return sum(path.endswith("/settle-batch") for path in self.requests)

    async def restored(self) -> list:
        """What an attach restores, with the executor's pending strip."""

        rows = await self.agent_db.get_thread_messages_history(
            thread_id=str(self.seed.session), limit=1000, newest_first=True
        )
        restored = repair_tool_pairing(_db_rows_to_lc_messages(rows))
        restored = sanitize_history_for_provider_boundary(restored, MODEL)
        pending = await self.pending()
        strip_restored_pending_humans(restored, pending)
        return restored

    async def pending(self) -> list[dict[str, Any]]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                _PENDING_INPUT_SQL,
                self.seed.session,
                await _consumed(self.pool, self.seed) or -1,
                10,
            )
        return [
            {
                "id": str(row["id"]),
                "seq": row["seq"],
                "role": row["role"],
                "content": row["content"],
                "turn_number": row["turn_number"],
                "delivery_id": (
                    str(row["delivery_id"]) if row["delivery_id"] else None
                ),
                "supersedes_input_seq": row["supersedes_input_seq"],
                "supersedes_input_role": row["supersedes_input_role"],
            }
            for row in rows
        ]


async def _run_turn(agent: _Agent, restored: list, user_input: HumanMessage, answer):
    """One loop turn on the restored history; the fake model records its input."""

    model = FakeChatModel([text_turn(answer)])
    messages = [SystemMessage(content="You compare take-home pay."), *restored]
    messages.append(user_input)
    context_manager = _context_manager()
    context_manager.record_provider_usage = Mock()
    result = await _execute_turn(
        llm_with_tools=model,
        tool_map={},
        context_manager=context_manager,
        messages=messages,
        callbacks=_callbacks(),
        llm_timeout=10,
        auxiliary_llm=None,
        config=_config(),
        tool_context=agent.context,
        turn_id=1,
    )
    assert result.error is None
    (provider_input,) = model.calls
    return provider_input


async def _serve(agent: _Agent, target: dict, *, lease: dict, answer: str) -> None:
    """The durable edges of serving one event input: claim, admit, answer,
    settle (what the loop and the executor write around the provider call)."""

    async with agent.pool.acquire() as conn:
        async with conn.transaction():
            claimed = await claim_stateless_input_delivery(
                conn,
                thread_id=agent.seed.session,
                delivery_id=UUID(target["delivery_id"]),
                lease_token=lease["lease_token"],
                executor_id=lease["executor_id"],
                pod_uid=lease["executor_pod_uid"],
            )
        assert claimed is not None
        for transition in ("admitted", "settled"):
            async with conn.transaction():
                assert await transition_stateless_input_delivery(
                    conn,
                    thread_id=agent.seed.session,
                    delivery_id=UUID(target["delivery_id"]),
                    lease_token=lease["lease_token"],
                    executor_id=lease["executor_id"],
                    pod_uid=lease["executor_pod_uid"],
                    claim_generation=claimed["claim_generation"],
                    transition=transition,
                    turn_number=1 if transition == "admitted" else None,
                )
            if transition == "admitted":
                await conn.execute(
                    "INSERT INTO thread_messages (id, thread_id, role, content, "
                    "turn_number) VALUES ($1, $2, 'ai', $3, 1)",
                    uuid4(),
                    agent.seed.session,
                    answer,
                )


async def _seed_crashed_batch(pool, orchestrator, tmp_path) -> tuple[Seed, dict]:
    """S1 with typed input, a spilled report and a live child's transcript."""

    seed = await _seed(pool, orchestrator, S1, typed_during_the_batch=True)
    ended_call, live_call = seed.call_ids[:2]
    agent_db = _agent_db(pool)
    # The live child's durable transcript: its last assistant text, then a
    # tool call whose output must never become its result.
    live_child = seed.children[live_call]["thread_id"]
    await agent_db.save_thread_message(
        thread_id=live_child, role="human", content="brief", turn_number=1
    )
    await agent_db.save_thread_message(
        thread_id=live_child, role="ai", content=LAST_WORDS, turn_number=1
    )
    await agent_db.save_thread_message(
        thread_id=live_child,
        role="ai",
        content="",
        tool_calls=[{"id": "t1", "name": "read_file", "args": {"path": "x"}}],
        turn_number=2,
    )
    await agent_db.save_thread_message(
        thread_id=live_child,
        role="tool",
        content="RAW TOOL OUTPUT",
        tool_call_id="t1",
        turn_number=2,
    )
    authority = await _successor(pool, seed)
    return seed, authority


# ---------------------------------------------------------------------------
# The gate: four members, one settle, each report once
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_recovery_settles_the_batch_and_the_model_reads_each_report_once(
    pg_dsn: str, tmp_path, monkeypatch
) -> None:
    pool = await _fresh_pool(pg_dsn)
    try:
        async with AsyncExitStack() as stack:
            orchestrator = _orchestrator_db(pool)
            seed, authority = await _seed_crashed_batch(pool, orchestrator, tmp_path)
            agent = _Agent(pool, orchestrator, seed, authority, tmp_path, stack)
            await agent.connect(monkeypatch)
            (agent.root / ".subagents" / "reader-0000").mkdir(parents=True)
            (agent.root / ".subagents" / "reader-0000" / "report.md").write_text(REPORT)

            def no_child(*_args, **_kwargs):
                raise AssertionError("recovery built a child")

            monkeypatch.setattr(runtime_module, "build_child", no_child)
            runtime = agent.runtime()

            recovered = await runtime.recover_orphans()

            # One settle for the turn; the single-child endpoint never ran.
            assert agent.settle_requests() == 1
            assert not any(path.endswith("/terminal") for path in agent.requests)
            ended_call, live_call, *never = seed.call_ids
            assert [entry["thread_id"] for entry in recovered] == [
                seed.children[ended_call]["thread_id"],
                seed.children[live_call]["thread_id"],
            ]
            assert [entry["status"] for entry in recovered] == [
                "completed",
                "interrupted",
            ]
            delivery_id = str(
                session_subagent_batch_delivery_id(seed.session, seed.input_id)
            )
            assert {entry["delivery_id"] for entry in recovered} == {delivery_id}
            rows = await _tool_rows(pool, seed)
            assert [row["tool_call_id"] for row in rows] == seed.call_ids
            assert {row["turn_number"] for row in rows} == {1}
            by_call = {row["tool_call_id"]: row["content"] for row in rows}
            assert REPORT in by_call[ended_call]
            assert by_call[live_call].startswith(INTERRUPTED_HEADER)
            assert LAST_WORDS in by_call[live_call]
            assert "RAW TOOL OUTPUT" not in by_call[live_call]
            assert [by_call[call] for call in never] == [not_started_result_text()] * 2
            (continuation,) = await _continuations(pool, seed)
            assert str(continuation["delivery_id"]) == delivery_id
            assert await _consumed(pool, seed) == seed.input_seq
            assert (
                await orchestrator.list_live_session_subagent_threads(
                    str(seed.session), parent_authority=authority
                )
                == []
            )

            # Running recovery again is a no-op: the same runtime has converged,
            # and a fresh one (a later attach) finds nothing to settle.
            before = await _snapshot(pool, seed)
            assert await runtime.recover_orphans() == []
            assert await agent.runtime().recover_orphans() == []
            assert agent.settle_requests() == 1
            assert await _snapshot(pool, seed) == before

            # The recovery turn: the executor serves the continuation first;
            # the input typed during the batch is still pending behind it.
            pending = await agent.pending()
            assert [row["id"] for row in pending] == [
                str(continuation["message_id"]),
                str(seed.typed_id),
            ]
            target = pending[0]
            assert target["supersedes_input_seq"] == seed.input_seq
            assert target["supersedes_input_role"] == "human"
            assert (
                completed_input_checkpoint(target, claim_consumed_seq=seed.input_seq)
                == seed.input_seq
            )
            restored = await agent.restored()
            continuation_input = HumanMessage(
                content=target["content"], id=target["id"]
            )
            continuation_input.additional_kwargs[PERSIST_ROLE_KEY] = "event"
            # The loop re-saves its input row at turn start; the continuation's
            # recovery metrics (the cockpit reads them) survive that write.
            await _persist_one_message(
                agent.agent_db, str(seed.session), continuation_input, 1
            )
            (continuation,) = await _continuations(pool, seed)
            metrics = continuation["metrics"]
            metrics = json.loads(metrics) if isinstance(metrics, str) else metrics
            assert metrics[RECOVERY_METRICS_KEY]["kind"] == "continuation"
            assert metrics[RECOVERY_METRICS_KEY]["calls"] == 4

            provider_input = await _run_turn(
                agent, restored, continuation_input, "The comparison: ..."
            )

            assert [message.type for message in provider_input] == (
                ["system", "human", "ai"] + ["tool"] * 4 + ["human"]
            )
            call_message = provider_input[2]
            assert [call["id"] for call in call_message.tool_calls] == seed.call_ids
            assert [message.tool_call_id for message in provider_input[3:7]] == (
                seed.call_ids
            )
            assert provider_input[-1].content == target["content"]
            text = [str(message.content) for message in provider_input]
            assert sum(REPORT in part for part in text) == 1
            assert sum(LAST_WORDS in part for part in text) == 1
            assert sum(INTERRUPTED_HEADER in part for part in text) == 1
            assert not any(TYPED_DURING_THE_BATCH in part for part in text)

            # The next turn: call, results, continuation, its answer, then
            # the input the user typed during the batch.
            await _serve(agent, target, lease=authority, answer="The comparison: ...")
            pending = await agent.pending()
            assert [row["id"] for row in pending] == [str(seed.typed_id)]
            restored = await agent.restored()
            provider_input = await _run_turn(
                agent,
                restored,
                HumanMessage(content=TYPED_DURING_THE_BATCH, id=str(seed.typed_id)),
                "Austria is included above.",
            )
            assert [message.type for message in provider_input] == (
                ["system", "human", "ai"] + ["tool"] * 4 + ["human", "ai", "human"]
            )
            assert provider_input[7].content == target["content"]
            assert provider_input[-1].content == TYPED_DURING_THE_BATCH
            counts = Counter(str(message.content) for message in provider_input)
            assert counts[target["content"]] == 1
            assert counts[TYPED_DURING_THE_BATCH] == 1
            assert sum(REPORT in str(m.content) for m in provider_input) == 1
    finally:
        await pool.close()


# ---------------------------------------------------------------------------
# D3: a recovery turn that delegated again, and the executor close
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_recovery_turn_that_delegates_again_closes_without_skipping_input(
    pg_dsn: str, tmp_path, monkeypatch
) -> None:
    pool = await _fresh_pool(pg_dsn)
    try:
        async with AsyncExitStack() as stack:
            orchestrator = _orchestrator_db(pool)
            seed = await _seed(
                pool, orchestrator, ["completed", "none"], typed_during_the_batch=True
            )
            second_lease = await _successor(pool, seed)
            agent = _Agent(pool, orchestrator, seed, second_lease, tmp_path, stack)
            await agent.connect(monkeypatch)
            await agent.runtime().recover_orphans()
            (first,) = await _continuations(pool, seed)

            # The recovery turn is admitted and delegates two more children.
            async with pool.acquire() as conn:
                async with conn.transaction():
                    claimed = await claim_stateless_input_delivery(
                        conn,
                        thread_id=seed.session,
                        delivery_id=first["delivery_id"],
                        lease_token=second_lease["lease_token"],
                        executor_id=second_lease["executor_id"],
                        pod_uid=second_lease["executor_pod_uid"],
                    )
                async with conn.transaction():
                    assert await transition_stateless_input_delivery(
                        conn,
                        thread_id=seed.session,
                        delivery_id=first["delivery_id"],
                        lease_token=second_lease["lease_token"],
                        executor_id=second_lease["executor_id"],
                        pod_uid=second_lease["executor_pod_uid"],
                        claim_generation=claimed["claim_generation"],
                        transition="admitted",
                        turn_number=1,
                    )
                again_ai = uuid4()
                again = [f"call_again_{index}" for index in range(2)]
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
                    parent_authority=second_lease,
                    handle=f"explorer-{index:04x}",
                    subagent_type="explorer",
                    parent_tool_call_id=call_id,
                    parent_input_message_id=str(first["message_id"]),
                    parent_ai_message_id=str(again_ai),
                    parent_iteration=1,
                )

            # That executor dies too; a third one recovers against the
            # continuation it was serving.
            async with pool.acquire() as conn:
                await release_unit(
                    conn, unit_id=seed.session, lease_token=second_lease["lease_token"]
                )
                claim = await claim_unit(
                    conn,
                    unit_kind=UNIT_KIND_SESSION_TURN,
                    pod_name=THIRD,
                    prefer_unit_id=seed.session,
                )
                await _stamp_stateless_claim(
                    conn,
                    seed.session,
                    token=claim.lease_token,
                    pod=THIRD,
                    pod_uid=THIRD_UID,
                )
            agent.authority = {
                **second_lease,
                "lease_token": claim.lease_token,
                "executor_id": THIRD,
                "executor_pod_uid": THIRD_UID,
            }
            recovered = await agent.runtime().recover_orphans()
            assert agent.settle_requests() == 2
            assert [entry["status"] for entry in recovered] == ["interrupted"] * 2
            first, second = await _continuations(pool, seed)
            assert second["supersedes_input_seq"] == first["seq"]
            assert [first["state"], second["state"]] == ["settled", "queued"]
            # The human watermark stays on the first input.
            assert await _consumed(pool, seed) == seed.input_seq
            assert seed.input_seq < seed.typed_seq < first["seq"]

            pending = await agent.pending()
            assert [row["id"] for row in pending] == [
                str(second["message_id"]),
                str(seed.typed_id),
            ]
            target = pending[0]
            assert target["supersedes_input_role"] == "event"
            # The executor closes that recovery turn without a checkpoint.
            assert (
                completed_input_checkpoint(target, claim_consumed_seq=seed.input_seq)
                is None
            )
            async with pool.acquire() as conn:
                assert await open_interrupt_admission(
                    conn, unit_id=seed.session, lease_token=claim.lease_token, turn_id=1
                )
            await _serve(agent, target, lease=agent.authority, answer="Done.")
            async with pool.acquire() as conn:
                # The checkpoint the executor used to compute (the superseded
                # event's seq) is refused: that event's delivery belonged to
                # the dead executor's lease. Accepted, it would have moved the
                # watermark past the typed input.
                assert not await close_interrupt_admission(
                    conn,
                    unit_id=seed.session,
                    lease_token=claim.lease_token,
                    turn_id=1,
                    completed_input_seq=first["seq"],
                )
                assert await close_interrupt_admission(
                    conn,
                    unit_id=seed.session,
                    lease_token=claim.lease_token,
                    turn_id=1,
                    completed_input_seq=None,
                )
                state = await complete_unit(
                    conn,
                    unit_id=seed.session,
                    lease_token=claim.lease_token,
                    consumed_seq=seed.input_seq,
                )
            assert state == "queued"
            assert await _consumed(pool, seed) == seed.input_seq
            assert [row["id"] for row in await agent.pending()] == [str(seed.typed_id)]
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_single_child_recovery_of_an_event_turn_closes_without_a_checkpoint(
    pg_dsn: str,
) -> None:
    """Today's single-child path has the same close: a turn that a server
    event started (an officer wake: the one server event the stateless lane
    accepts) delegated one child and died. The recovery event supersedes the
    EVENT; the checkpoint the executor used to compute is refused, and closing
    without one keeps the typed input owed."""

    pool = await _fresh_pool(pg_dsn)
    try:
        orchestrator = _orchestrator_db(pool)
        owner, session = uuid4(), uuid4()
        wake_delivery, ai_id, call_id = uuid4(), uuid4(), "call_after_the_wake"
        lease = {"executor_id": "executor-1", "executor_pod_uid": "pod-1"}
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO users (id, display_name) VALUES ($1, 'owner')", owner
            )
            await conn.execute(
                "INSERT INTO threads (id, user_id, title, status, execution_lane) "
                "VALUES ($1, $2, 'Job watcher', 'active', 'stateless')",
                session,
                owner,
            )
            async with conn.transaction():
                wake = await persist_input_delivery(
                    conn,
                    thread_id=session,
                    delivery_id=wake_delivery,
                    role="event",
                    content="[wake] the nightly report job finished",
                    source="officer_wake",
                    turn_number=1,
                )
            claim = await claim_unit(
                conn,
                unit_kind=UNIT_KIND_SESSION_TURN,
                pod_name=lease["executor_id"],
                prefer_unit_id=session,
            )
            await _stamp_stateless_claim(
                conn,
                session,
                token=claim.lease_token,
                pod=lease["executor_id"],
                pod_uid=lease["executor_pod_uid"],
            )
            async with conn.transaction():
                claimed = await claim_stateless_input_delivery(
                    conn,
                    thread_id=session,
                    delivery_id=wake_delivery,
                    lease_token=claim.lease_token,
                    executor_id=lease["executor_id"],
                    pod_uid=lease["executor_pod_uid"],
                )
                assert await transition_stateless_input_delivery(
                    conn,
                    thread_id=session,
                    delivery_id=wake_delivery,
                    lease_token=claim.lease_token,
                    executor_id=lease["executor_id"],
                    pod_uid=lease["executor_pod_uid"],
                    claim_generation=claimed["claim_generation"],
                    transition="admitted",
                    turn_number=1,
                )
            await conn.execute(
                "INSERT INTO thread_messages "
                "(id, thread_id, role, content, tool_calls, turn_number) "
                "VALUES ($1, $2, 'ai', '', $3::jsonb, 1)",
                ai_id,
                session,
                json.dumps([{"id": call_id, "name": "delegate_agent", "args": {}}]),
            )
            typed_id = uuid4()
            typed_seq = await conn.fetchval(
                "INSERT INTO thread_messages (id, thread_id, role, content, "
                "turn_number) VALUES ($1, $2, 'human', 'and summarise it', 2) "
                "RETURNING seq",
                typed_id,
                session,
            )
            await record_input_seq(
                conn,
                unit_id=session,
                unit_kind=UNIT_KIND_SESSION_TURN,
                input_seq=int(typed_seq),
                fair_key=str(owner),
            )
        first = {
            "version": 1,
            "execution_lane": "stateless",
            "parent_thread_id": str(session),
            "lease_token": claim.lease_token,
            **lease,
        }
        child = await orchestrator.create_session_subagent_thread(
            parent_thread_id=str(session),
            parent_authority=first,
            handle="reader-0001",
            subagent_type="reader",
            parent_tool_call_id=call_id,
            parent_input_message_id=str(wake["message_id"]),
            parent_ai_message_id=str(ai_id),
            parent_iteration=1,
        )

        # The executor dies; the successor recovers the child on its own.
        async with pool.acquire() as conn:
            await release_unit(conn, unit_id=session, lease_token=claim.lease_token)
            successor = await claim_unit(
                conn,
                unit_kind=UNIT_KIND_SESSION_TURN,
                pod_name="executor-2",
                prefer_unit_id=session,
            )
            await _stamp_stateless_claim(
                conn,
                session,
                token=successor.lease_token,
                pod="executor-2",
                pod_uid="pod-2",
            )
        second = {
            **first,
            "lease_token": successor.lease_token,
            "executor_id": "executor-2",
            "executor_pod_uid": "pod-2",
        }
        recovery = await orchestrator.terminalize_session_subagent_thread(
            parent_thread_id=str(session),
            parent_authority=second,
            thread_id=child["thread_id"],
            runtime_generation=child["runtime_generation"],
            subagent_status="interrupted",
            outcome="interrupted:parent_restart",
            message="[subagent reader-0001 · reader · interrupted:parent_restart]",
            foreground_orphan_recovery=True,
        )
        assert recovery["result"] == "applied"
        wake_seq = int(wake["seq"])
        assert recovery["supersedes_input_seq"] == wake_seq
        async with pool.acquire() as conn:
            consumed = await conn.fetchval(
                "SELECT consumed_seq FROM run_queue WHERE unit_id=$1", session
            )
            rows = await conn.fetch(_PENDING_INPUT_SQL, session, consumed, 10)
        target = {**dict(rows[0]), "seq": rows[0]["seq"]}
        assert [row["id"] for row in rows] == [rows[0]["id"], typed_id]
        assert target["supersedes_input_seq"] == wake_seq
        assert target["supersedes_input_role"] == "event"
        assert completed_input_checkpoint(target, claim_consumed_seq=consumed) is None

        async with pool.acquire() as conn:
            assert await open_interrupt_admission(
                conn, unit_id=session, lease_token=successor.lease_token, turn_id=1
            )
            # Before the fix: max(superseded seq, watermark) — refused.
            assert not await close_interrupt_admission(
                conn,
                unit_id=session,
                lease_token=successor.lease_token,
                turn_id=1,
                completed_input_seq=max(wake_seq, int(consumed or -1)),
            )
            assert await close_interrupt_admission(
                conn,
                unit_id=session,
                lease_token=successor.lease_token,
                turn_id=1,
                completed_input_seq=None,
            )
            assert (
                await complete_unit(
                    conn,
                    unit_id=session,
                    lease_token=successor.lease_token,
                    consumed_seq=consumed,
                )
                == "queued"
            )
            assert (
                await conn.fetchval(
                    "SELECT consumed_seq FROM run_queue WHERE unit_id=$1", session
                )
                == consumed
            )
            assert int(typed_seq) > int(consumed)
    finally:
        await pool.close()


# ---------------------------------------------------------------------------
# The turn-start re-save keeps what the orchestrator wrote
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_metrics_of_non_ai_rows_survive_an_upsert_without_metrics(
    pg_dsn: str,
) -> None:
    pool = await _fresh_pool(pg_dsn)
    try:
        agent_db = _agent_db(pool)
        orchestrator = _orchestrator_db(pool)
        seed = await _seed(pool, orchestrator, ["none"])
        event_id, ai_id = str(uuid4()), str(uuid4())
        for row_id, role in ((event_id, "event"), (ai_id, "ai")):
            await agent_db.save_thread_message(
                thread_id=str(seed.session),
                role=role,
                content="x",
                turn_number=1,
                metrics={"kept": role},
                id=row_id,
            )
            await agent_db.save_thread_message(
                thread_id=str(seed.session),
                role=role,
                content="x",
                turn_number=1,
                id=row_id,
            )
        async with pool.acquire() as conn:
            stored = {
                str(row["id"]): row["metrics"]
                for row in await conn.fetch(
                    "SELECT id, metrics FROM thread_messages WHERE id = ANY($1)",
                    [UUID(event_id), UUID(ai_id)],
                )
            }
        event_metrics = stored[event_id]
        event_metrics = (
            json.loads(event_metrics)
            if isinstance(event_metrics, str)
            else event_metrics
        )
        assert event_metrics == {"kept": "event"}
        # An AI row still takes what its writer brings (the turn reconcile).
        assert stored[ai_id] is None
    finally:
        await pool.close()


# ---------------------------------------------------------------------------
# Mixed versions: an older agent recovered part of the turn one child at a time
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_members_left_by_an_older_agent_are_closed_and_leave_the_list(
    pg_dsn: str, tmp_path, monkeypatch
) -> None:
    """An agent without batch recovery recovered the finished child on its own
    (one continuation event) and died before the live one. The batch settle
    answers ``idempotent``: the input already has a continuation. The new
    agent closes the owed child the way the older one would have, so nothing
    stays listed and rewind is not blocked for good."""

    from shared.thread_rewind import LIVE_SESSION_CHILD_EXISTS_SQL

    pool = await _fresh_pool(pg_dsn)
    try:
        async with AsyncExitStack() as stack:
            orchestrator = _orchestrator_db(pool)
            seed, authority = await _seed_crashed_batch(pool, orchestrator, tmp_path)
            ended_call, live_call = seed.call_ids[:2]
            ended = seed.children[ended_call]
            older = await orchestrator.terminalize_session_subagent_thread(
                parent_thread_id=str(seed.session),
                parent_authority=authority,
                thread_id=ended["thread_id"],
                runtime_generation=ended["runtime_generation"],
                subagent_status="completed",
                outcome="completed",
                message="[subagent reader-0000 · reader · completed] the report",
                foreground_orphan_recovery=True,
            )
            # The child had ended before the crash, so the single-child path
            # answers "idempotent" for the child while it writes the event.
            assert older["result"] in {"applied", "idempotent"}
            assert older["delivery_state"]

            agent = _Agent(pool, orchestrator, seed, authority, tmp_path, stack)
            await agent.connect(monkeypatch)
            recovered = await agent.runtime().recover_orphans()

            assert agent.settle_requests() == 1  # answered idempotent
            terminal = [path for path in agent.requests if path.endswith("/terminal")]
            assert len(terminal) == 1
            assert [entry["thread_id"] for entry in recovered] == [
                seed.children[live_call]["thread_id"]
            ]
            assert (
                await orchestrator.list_live_session_subagent_threads(
                    str(seed.session), parent_authority=authority
                )
                == []
            )
            async with pool.acquire() as conn:
                assert not await conn.fetchval(
                    LIVE_SESSION_CHILD_EXISTS_SQL, seed.session
                )
            continuations = await _continuations(pool, seed)
            assert [row["supersedes_input_seq"] for row in continuations] == [
                seed.input_seq,
                seed.input_seq,
            ]
            # A later attach has nothing left to recover.
            assert await agent.runtime().recover_orphans() == []
            assert agent.settle_requests() == 1
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_a_report_the_successor_cannot_read_comes_from_the_transcript(
    pg_dsn: str, tmp_path, monkeypatch
) -> None:
    """On the ``none`` tier the spill lives in the dead executor's scratch
    directory. The finished child's report reaches the parent from its stored
    transcript instead of a permanent "report unavailable"."""

    pool = await _fresh_pool(pg_dsn)
    try:
        async with AsyncExitStack() as stack:
            orchestrator = _orchestrator_db(pool)
            seed, authority = await _seed_crashed_batch(pool, orchestrator, tmp_path)
            ended_call = seed.call_ids[0]
            ended_child = seed.children[ended_call]["thread_id"]
            agent_db = _agent_db(pool)
            await agent_db.save_thread_message(
                thread_id=ended_child, role="human", content="brief", turn_number=1
            )
            await agent_db.save_thread_message(
                thread_id=ended_child, role="ai", content=REPORT, turn_number=1
            )
            agent = _Agent(pool, orchestrator, seed, authority, tmp_path, stack)
            await agent.connect(monkeypatch)  # no spill file in this workspace

            await agent.runtime().recover_orphans()

            by_call = {
                row["tool_call_id"]: row["content"]
                for row in await _tool_rows(pool, seed)
            }
            assert REPORT in by_call[ended_call]
            assert "Report source: the child's stored transcript" in by_call[ended_call]
            assert "report unavailable" not in by_call[ended_call]
    finally:
        await pool.close()
