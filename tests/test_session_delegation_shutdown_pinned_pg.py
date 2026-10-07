"""A graceful pinned shutdown during a session's delegation batch, on Postgres.

Design: knowledge-base/knowledge/features/parallel_subagents.md §14.1 (the
graceful-shutdown bullet), §14.2 P4 and D7.

The dying runtime is the real one: the loop turn, the subagent runtime, the
session ledger over the in-process orchestrator and Postgres, and the pinned
runtime's real termination owner, whose fence the children read. The fence's
first activation (preStop) hands the batch over, quiescence is reached with
the turn still open, and the termination then does what it does to this turn
(``quiesce`` the runtime without settlement authority, cancel the loop)
before the orchestrator retires the session (Begin, authorize, the agent's
local-quiescence receipt, the soft settlement). Resume binds a successor,
whose real recovery (``recover_orphans``) settles the turn: one result per
call and one continuation. Nothing here depends on the forced-stop detector
(P0b).
"""

from __future__ import annotations

import asyncio
import shutil
from contextlib import AsyncExitStack
from types import SimpleNamespace
from typing import Any, List
from unittest.mock import Mock
from uuid import uuid4

import pytest
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage

import agent.api.persistent_app as pa
import agent.persistent_graph as persistent_graph
from agent.core.thread_messages import _serialize_message_row
from agent.persistent_graph import PERSIST_ROLE_KEY, _execute_turn
from agent.subagents import SessionHost, SubagentRuntime
from agent.subagents.batch_recovery import INTERRUPTED_HEADER
from agent.subagents.session_persistence import SessionSubagentLedger
from agent.tools.delegation.delegate_agent import create_delegate_agent_tools
from shared.persistent_input_delivery import (
    message_row_id,
    persist_input_delivery,
    transition_input_delivery,
)
from shared.session_subagent_batch import (
    batch_continuation_text,
    not_started_result_text,
)
from tests._fake_chat_model import FakeChatModel, text_turn
from tests._fanout_gate import set_session_fanout
from tests.test_persistent_delegation_batch import _callbacks, _config, _context_manager
from tests.test_session_subagent_batch_recovery_pg import (
    _Agent,
    _pinned_agent,
    _pinned_claim,
    _pinned_restored,
    _pinned_source_state,
    _run_turn,
)
from tests.test_session_subagent_batch_settle_pg import (
    INPUT,
    Seed,
    _continuations,
    _fresh_pool,
    _metrics,
    _pinned_parent,
    _tool_rows,
)
from tests.test_subagent_thread_migration import (  # noqa: F401  (scratch_pg_dsn fixture)
    _orchestrator_db,
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


@pytest.fixture
def fence(monkeypatch, tmp_path):
    """The pinned runtime's real termination owner, unfenced."""

    owner = pa._session_termination
    monkeypatch.delenv("STATELESS_EXECUTOR", raising=False)
    monkeypatch.setattr(owner, "termination_sentinel_path", tmp_path / "terminating")
    monkeypatch.setattr(owner, "termination_admission_fenced", False)
    monkeypatch.setattr(owner, "termination_fence_reason", None)
    monkeypatch.setattr(
        pa,
        "_session_input",
        SimpleNamespace(awaiting_input=False, wake_parked_wait=lambda _item: None),
    )
    monkeypatch.setattr(pa, "_tool_inflight", True)
    monkeypatch.setattr(pa, "_turn_event_open", True)
    monkeypatch.setattr(pa, "_loop_task", None)
    return owner


def _open() -> bool:
    return not pa._session_termination.termination_admission_closed()


RESTART_ROW = (
    "ended",
    "interrupted",
    "interrupted:parent_restart",
    "the parent runtime restarted",
)
NOT_STARTED_ROW = (
    "ended",
    "interrupted",
    "interrupted:not_started",
    "the parent runtime restarted",
)


def _row_end(row) -> tuple:
    return (
        row["status"],
        row["subagent_status"],
        row["subagent_outcome"],
        row["subagent_error"],
    )


REPORT = "Austria: take-home pay at 60,000 EUR gross is 41,200 EUR."
LAST_WORDS = "Germany: income tax done, now checking the solidarity surcharge."


class _Child(FakeChatModel):
    """The first child to reach its provider answers at once. Every later one
    waits in its first provider call until ``release``, then says how far it
    got and asks for a tool, whose effect the closed fence refuses. (Which
    call gets a slot first follows the ledger lookups, so the test reads it
    back from the durable rows.)"""

    def __init__(self, started: List[str], release: asyncio.Event):
        super().__init__([])
        self.started = started
        self.release = release
        self.first = False

    async def astream(self, messages, **kw):
        self.calls.append(list(messages))
        if len(self.calls) == 1:
            self.started.append(
                " ".join(str(m.content) for m in messages if m.type == "human")
            )
            self.first = len(self.started) == 1
        if self.first:
            for chunk in text_turn(REPORT):
                await asyncio.sleep(0)
                yield chunk
            return
        await self.release.wait()
        yield AIMessage(
            content=LAST_WORDS,
            tool_calls=[
                {
                    "id": f"read-{uuid4().hex[:6]}",
                    "name": "read_file",
                    "args": {"path": "notes/hello.md"},
                    "type": "tool_call",
                }
            ],
            usage_metadata={
                "input_tokens": 100,
                "output_tokens": 8,
                "total_tokens": 108,
            },
            response_metadata={"finish_reason": "tool_calls"},
        )


async def _pinned_turn(pool, orchestrator, calls: int) -> Seed:
    """A pinned session whose runtime admitted the user's input."""

    owner, session, authority = await _pinned_parent(pool, orchestrator)
    delivery_id = uuid4()
    async with pool.acquire() as conn:
        source = await persist_input_delivery(
            conn,
            thread_id=session,
            delivery_id=delivery_id,
            role="human",
            content=INPUT,
            source="direct_human",
            turn_number=1,
            agent_id=authority["agent_id"],
            pod_uid=authority["pod_uid"],
            runtime_generation=authority["session_runtime_generation"],
            session_runtime_generation=authority["session_runtime_generation"],
            runtime_attach_token=authority["runtime_attach_token"],
        )
        assert await transition_input_delivery(
            conn,
            delivery_id=delivery_id,
            agent_id=authority["agent_id"],
            pod_uid=authority["pod_uid"],
            runtime_generation=authority["session_runtime_generation"],
            session_runtime_generation=authority["session_runtime_generation"],
            runtime_attach_token=authority["runtime_attach_token"],
            claim_generation=source["claim_generation"],
            transition="admitted",
            turn_number=1,
        )
    return Seed(
        lane="pinned",
        session=session,
        owner=owner,
        input_id=message_row_id(delivery_id),
        input_seq=int(source["seq"]),
        ai_id=uuid4(),
        ai_seq=0,
        call_ids=[f"call_country_{index}_{uuid4().hex[:8]}" for index in range(calls)],
        authority=authority,
        source_delivery_id=delivery_id,
    )


async def _child_rows(pool, seed: Seed) -> dict[str, Any]:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, runtime_generation, status, subagent_status, "
            "subagent_outcome, subagent_error, report_path, total_turns, "
            "subagent_handle AS handle, parent_tool_call_id "
            "FROM threads WHERE kind='subagent' AND parent_thread_id=$1",
            seed.session,
        )
    return {row["parent_tool_call_id"]: row for row in rows}


async def _retire(pool, orchestrator, seed: Seed) -> dict[str, Any]:
    """The orchestrator's side of the agent's own End: Begin, authorize,
    the local-quiescence receipt, the soft settlement. Returns the context."""

    authority = seed.authority
    begun = await orchestrator.begin_pinned_thread_retirement(
        str(seed.session),
        permanent=False,
        settle_status="ended",
        expected_runtime_generation=authority["session_runtime_generation"],
        expected_agent_id=authority["agent_id"],
        expected_attach_token=authority["runtime_attach_token"],
        initiator="agent",
        authorize_immediately=True,
    )
    assert begun["state"] == "pending", begun
    assert begun["authorized_at"] is not None
    receipt = await orchestrator.acknowledge_pinned_thread_local_quiescence(
        str(seed.session),
        expected_runtime_generation=begun["generation"],
        expected_retirement_token=begun["token"],
        expected_agent_id=authority["agent_id"],
        expected_attach_token=authority["runtime_attach_token"],
        expected_settle_status="ended",
        # A sandbox session with no bound workspace: the agent runtime only.
        expected_quiescence_protocol="agent_runtime_zero_v1",
        expected_workspace_generation=None,
        expected_workspace_runtime_incarnation=None,
    )
    assert receipt is not None
    assert await orchestrator.settle_pinned_thread_retirement(
        str(seed.session),
        token=begun["token"],
        generation=begun["generation"],
        final_status="ended",
    )
    return begun


async def _resume(pool, orchestrator, seed: Seed) -> dict[str, Any]:
    """Resume: the ended session is reopened and a new runtime is bound."""

    agent_id = uuid4()
    pod_uid, pod_name, attempt = f"pod-{uuid4()}", f"parent-{uuid4()}", str(uuid4())
    async with pool.acquire() as conn:
        generation = await conn.fetchval(
            "UPDATE threads SET status='created' WHERE id=$1 "
            "RETURNING runtime_generation",
            seed.session,
        )
    assert await orchestrator.reserve_pinned_agent_pod_provision_intent(
        str(seed.session),
        expected_runtime_generation=str(generation),
        attempt_id=attempt,
        pod_name=pod_name,
        provisioner="agent",
        namespace="test",
    )
    assert await orchestrator.publish_pinned_agent_pod_provision_intent(
        str(seed.session),
        expected_runtime_generation=str(generation),
        attempt_id=attempt,
        pod_name=pod_name,
        pod_uid=pod_uid,
        namespace="test",
    )
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                "INSERT INTO agents (id, config_name, status, metadata, hostname, "
                "pod_uid) VALUES ($1, 'session_base', 'session', '{}'::jsonb, $2, $3)",
                agent_id,
                pod_name,
                pod_uid,
            )
            binding = await conn.fetchrow(
                "UPDATE threads SET agent_id=$2, status='active' WHERE id=$1 "
                "RETURNING runtime_generation, runtime_attach_token",
                seed.session,
                agent_id,
            )
            await conn.execute(
                "UPDATE agents SET thread_id=$2 WHERE id=$1", agent_id, seed.session
            )
    return {
        **seed.authority,
        "agent_id": str(agent_id),
        "pod_uid": pod_uid,
        "session_runtime_generation": str(binding["runtime_generation"]),
        "runtime_attach_token": str(binding["runtime_attach_token"]),
    }


class _Dying:
    """The dying pinned runtime: its session, runtime, ledger and open turn."""

    def __init__(self, agent: _Agent, runtime: SubagentRuntime, turn, persisted):
        self.agent = agent
        self.runtime = runtime
        self.turn = turn
        self.persisted = persisted


async def _dying_turn(
    pool,
    orchestrator,
    seed: Seed,
    tmp_path,
    stack,
    monkeypatch,
    *,
    cap: int,
    started: List[str],
    release: asyncio.Event,
    open_gate: tuple[int, asyncio.Event, asyncio.Event] | None = None,
) -> _Dying:
    """The real loop turn delegating every call of ``seed``, under the real
    runtime and session ledger, with children reading the real fence.

    ``open_gate`` ``(n, entered, allow)`` holds the n-th child's durable
    create until ``allow`` is set."""

    dying = _Agent(pool, orchestrator, seed, seed.authority, tmp_path / "dying", stack)
    await dying.connect(monkeypatch)
    context = dying.context
    context._subagent_execution_lane = "pinned"
    context.config["delegation"]["session_max_concurrent"] = cap
    context._current_input_message_id = str(seed.input_id)
    context._current_turn_count = 1
    host = SessionHost(
        thread_id=str(seed.session),
        agent_type="persistent",
        tool_context=context,
        postgres=dying.agent_db,
        admission_fn=_open,
        effect_authority_fn=_open,
        settlement_authority_fn=_open,
    )
    ledger = SessionSubagentLedger.from_context(context)
    assert ledger is not None
    if open_gate is not None:
        nth, entered, allow = open_gate
        real_open, opens = ledger.open, []

        async def gated_open(subagent_id: str, **fields: Any):
            opens.append(subagent_id)
            if len(opens) == nth:
                entered.set()
                await allow.wait()
            return await real_open(subagent_id, **fields)

        ledger.open = gated_open  # type: ignore[method-assign]
    runtime = SubagentRuntime.from_context(
        context,
        host,
        ledger=ledger,
        llm_factory=lambda _config, _limits: _Child(started, release),
        driver_kwargs={
            "watcher_poll_interval": 0.01,
            "archiver": None,
            "archive_fn": lambda **kwargs: None,
        },
    )
    context._parent_host = host
    context.subagent_runtime = runtime
    runtime._recovery_complete = True  # the attach recovered nothing
    tool = create_delegate_agent_tools(context)[0]
    monkeypatch.setattr(
        pa,
        "_session",
        SimpleNamespace(tool_context=context, auxiliary_llm=None, memory_service=None),
    )
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
        return _open()

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
                before_provider_admission=_open,
                before_provider_execution=authority,
            ),
            llm_timeout=10,
            auxiliary_llm=None,
            config=_config(),
            tool_context=context,
            turn_id=1,
        )
    )
    monkeypatch.setattr(pa, "_loop_task", turn)
    return _Dying(dying, runtime, turn, persisted)


async def _until_rows(pool, seed: Seed, states: list[str], started, n: int):
    deadline = asyncio.get_running_loop().time() + 15
    while True:
        rows = await _child_rows(pool, seed)
        now = sorted(str(row["subagent_status"]) for row in rows.values())
        if now == states and len(started) == n:
            return rows
        assert asyncio.get_running_loop().time() < deadline, now
        await asyncio.sleep(0.02)


async def _terminate_and_resume(
    pool, orchestrator, seed: Seed, dying: _Dying, tmp_path, stack, monkeypatch
) -> _Agent:
    """SIGTERM's terminate("shutdown") on this turn, the orchestrator's
    retirement, and Resume. Nothing is written for the handed-over calls."""

    snapshot = await _child_rows(pool, seed)
    # Begin's quiesce: the fence revoked settlement authority, and every
    # handed-over call is settled; then the loop is cancelled.
    await dying.runtime.quiesce("parent session retiring as ended")
    dying.turn.cancel()
    await asyncio.gather(dying.turn, return_exceptions=True)
    assert dying.turn.cancelled()
    assert [m for m in dying.persisted if isinstance(m, ToolMessage)] == []
    assert await _tool_rows(pool, seed) == []
    assert await _child_rows(pool, seed) == snapshot

    # The orchestrator retires the session; nothing refuses, nothing changes.
    await _retire(pool, orchestrator, seed)
    assert await _child_rows(pool, seed) == snapshot
    assert await _pinned_source_state(pool, seed) == "admitted"

    seed.authority = await _resume(pool, orchestrator, seed)
    async with pool.acquire() as conn:
        seed.ai_seq = await conn.fetchval(
            "SELECT seq FROM thread_messages WHERE id=$1", seed.ai_id
        )
    successor = await _pinned_agent(
        pool, orchestrator, seed, tmp_path / "successor", stack, monkeypatch
    )
    # One workspace: a finished child's spilled report is there.
    if (dying.agent.root / ".subagents").exists():
        shutil.copytree(dying.agent.root / ".subagents", successor.root / ".subagents")
    return successor


async def _next_turn(pool, seed: Seed, successor: _Agent, continuation) -> list:
    """The continuation is served next; the provider reads each result once."""

    claimed = await _pinned_claim(pool, seed)
    assert [row["message_id"] for row in claimed] == [str(continuation["message_id"])]
    restored = await _pinned_restored(successor)
    continuation_input = HumanMessage(
        content=claimed[0]["content"], id=claimed[0]["message_id"]
    )
    continuation_input.additional_kwargs[PERSIST_ROLE_KEY] = "event"
    provider_input = await _run_turn(
        successor, restored, continuation_input, "The comparison: ..."
    )
    calls = len(seed.call_ids)
    assert [message.type for message in provider_input] == (
        ["system", "human", "ai"] + ["tool"] * calls + ["human"]
    )
    assert [m.tool_call_id for m in provider_input[3 : 3 + calls]] == seed.call_ids
    return provider_input


@pytest.mark.asyncio
async def test_a_graceful_pinned_shutdown_hands_a_mixed_batch_to_the_successor(
    pg_dsn: str, tmp_path, monkeypatch, fence
) -> None:
    """Five calls, cap 3: one child finished before the fence, two run, one
    row is being opened, one call is queued. After the graceful shutdown and
    Resume, one settle answers every call: the report, two INTERRUPTED from
    their transcripts, two NOT STARTED (the server's text; the successor
    sends none), and one continuation that counts them so."""

    set_session_fanout(monkeypatch)
    pool = await _fresh_pool(pg_dsn)
    try:
        async with AsyncExitStack() as stack:
            orchestrator = _orchestrator_db(pool)
            seed = await _pinned_turn(pool, orchestrator, 5)
            started: List[str] = []
            release, entered, allow = asyncio.Event(), asyncio.Event(), asyncio.Event()
            dying = await _dying_turn(
                pool,
                orchestrator,
                seed,
                tmp_path,
                stack,
                monkeypatch,
                cap=3,
                started=started,
                release=release,
                open_gate=(4, entered, allow),
            )

            # One child finished; two run; the fourth create is held; the
            # fifth call is queued behind the cap.
            rows = await _until_rows(
                pool, seed, ["completed", "running", "running"], started, 3
            )
            await asyncio.wait_for(entered.wait(), 5)
            (finished,) = [
                c for c, r in rows.items() if r["subagent_status"] == "completed"
            ]
            running = [c for c in seed.call_ids if c in rows and c != finished]
            assert fence.termination_quiescent() is False

            # --- preStop: the fence, then the wait for a settled boundary ---
            assert fence.activate_termination_admission_fence("kubernetes_prestop")
            release.set()
            allow.set()
            assert await fence.wait_for_termination_quiescence(15.0) is True
            assert not dying.turn.done()

            # The parent wrote no result; every opened child is terminal.
            assert await _tool_rows(pool, seed) == []
            rows = await _child_rows(pool, seed)
            (opened_late,) = [c for c in rows if c not in {finished, *running}]
            (queued,) = [c for c in seed.call_ids if c not in rows]
            assert rows[finished]["subagent_outcome"] == "completed"
            assert rows[finished]["report_path"] == (
                f".subagents/{rows[finished]['handle']}/report.md"
            )
            for call_id in running:
                assert _row_end(rows[call_id]) == RESTART_ROW
            assert _row_end(rows[opened_late]) == NOT_STARTED_ROW
            assert rows[opened_late]["total_turns"] == 0
            assert len(started) == 3  # the late child made no provider call

            successor = await _terminate_and_resume(
                pool, orchestrator, seed, dying, tmp_path, stack, monkeypatch
            )
            listed = await orchestrator.list_live_session_subagent_recovery(
                str(seed.session), parent_authority=seed.authority
            )
            (plan,) = listed["recovery_turns"]
            assert plan["parent_input_message_id"] == str(seed.input_id)
            by_call = {call["tool_call_id"]: call for call in plan["calls"]}
            assert list(by_call) == seed.call_ids
            assert {c: by_call[c]["needs_message"] for c in seed.call_ids} == {
                finished: True,
                running[0]: True,
                running[1]: True,
                opened_late: False,
                queued: False,
            }
            snapshot = await _child_rows(pool, seed)

            recovered = await successor.runtime().recover_orphans()

            assert successor.settle_requests() == 1
            assert sorted(entry["status"] for entry in recovered) == [
                "completed",
                "interrupted",
                "interrupted",
                "interrupted",
            ]
            results = {row["tool_call_id"]: row for row in await _tool_rows(pool, seed)}
            # One result per call, in provider order.
            assert list(results) == seed.call_ids
            assert REPORT in results[finished]["content"]
            for call_id in running:
                assert results[call_id]["content"].startswith(INTERRUPTED_HEADER)
                assert LAST_WORDS in results[call_id]["content"]
            for call_id in (opened_late, queued):
                assert results[call_id]["content"] == not_started_result_text()
            assert {c: _metrics(results[c])["class"] for c in seed.call_ids} == {
                finished: "completed",
                running[0]: "interrupted",
                running[1]: "interrupted",
                opened_late: "not_started",
                queued: "not_started",
            }
            (continuation,) = await _continuations(pool, seed)
            assert continuation["supersedes_input_seq"] == seed.input_seq
            assert continuation["content"] == batch_continuation_text(
                calls=5, interrupted=2, not_started=2, declined=0, retired=0
            )
            metrics = _metrics(continuation)
            assert (
                metrics["calls"],
                metrics["finished"],
                metrics["interrupted"],
                metrics["not_started"],
            ) == (5, 1, 2, 2)
            assert await _pinned_source_state(pool, seed) == "settled"
            assert await _child_rows(pool, seed) == snapshot  # nothing re-ended
            assert await successor.runtime().recover_orphans() == []
            assert successor.settle_requests() == 1

            await _next_turn(pool, seed, successor, continuation)
    finally:
        await pool.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(("cap", "n"), [(1, 3), (2, 5), (2, 7)])
async def test_a_batch_wider_than_twice_its_cap_is_settled_whole(
    pg_dsn: str, tmp_path, monkeypatch, fence, cap, n
) -> None:
    """Every child under the cap runs at the fence and more than ``cap``
    calls are queued behind it. All of them are held (a held call keeps no
    slot), the retirement passes, and the successor's one settle reports the
    running ones INTERRUPTED and every queued one NOT STARTED."""

    set_session_fanout(monkeypatch)
    pool = await _fresh_pool(pg_dsn)
    try:
        async with AsyncExitStack() as stack:
            orchestrator = _orchestrator_db(pool)
            seed = await _pinned_turn(pool, orchestrator, n)
            started: List[str] = ["(no first child)"]  # every child waits
            release = asyncio.Event()
            dying = await _dying_turn(
                pool,
                orchestrator,
                seed,
                tmp_path,
                stack,
                monkeypatch,
                cap=cap,
                started=started,
                release=release,
            )
            rows = await _until_rows(pool, seed, ["running"] * cap, started, cap + 1)
            ran = [c for c in seed.call_ids if c in rows]

            assert fence.activate_termination_admission_fence("kubernetes_prestop")
            release.set()
            assert await fence.wait_for_termination_quiescence(15.0) is True
            assert len(dying.runtime._successor_waiters) == n
            assert set(await _child_rows(pool, seed)) == set(ran)

            successor = await _terminate_and_resume(
                pool, orchestrator, seed, dying, tmp_path, stack, monkeypatch
            )
            recovered = await successor.runtime().recover_orphans()

            assert successor.settle_requests() == 1
            assert [entry["status"] for entry in recovered] == ["interrupted"] * cap
            results = {row["tool_call_id"]: row for row in await _tool_rows(pool, seed)}
            assert list(results) == seed.call_ids
            assert {c: _metrics(results[c])["class"] for c in seed.call_ids} == {
                c: ("interrupted" if c in ran else "not_started") for c in seed.call_ids
            }
            for call_id in ran:
                assert results[call_id]["content"].startswith(INTERRUPTED_HEADER)
            (continuation,) = await _continuations(pool, seed)
            assert continuation["content"] == batch_continuation_text(
                calls=n, interrupted=cap, not_started=n - cap, declined=0, retired=0
            )
            assert await _pinned_source_state(pool, seed) == "settled"
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_a_graceful_pinned_shutdown_hands_a_single_call_to_the_successor(
    pg_dsn: str, tmp_path, monkeypatch, fence
) -> None:
    """Without fan-out (the pinned lane's state until it is enabled) a turn
    delegates one call. The same hand-over leaves it to the single-child
    recovery, which delivers the child's interrupted evidence as the one
    continuation and settles the source."""

    pool = await _fresh_pool(pg_dsn)
    try:
        async with AsyncExitStack() as stack:
            orchestrator = _orchestrator_db(pool)
            seed = await _pinned_turn(pool, orchestrator, 1)
            started: List[str] = ["(no first child)"]  # every child waits
            release = asyncio.Event()
            dying = await _dying_turn(
                pool,
                orchestrator,
                seed,
                tmp_path,
                stack,
                monkeypatch,
                cap=1,
                started=started,
                release=release,
            )
            await _until_rows(pool, seed, ["running"], started, 2)

            assert fence.activate_termination_admission_fence("kubernetes_prestop")
            release.set()
            assert await fence.wait_for_termination_quiescence(15.0) is True
            assert not dying.turn.done()
            assert await _tool_rows(pool, seed) == []
            ((call_id, row),) = (await _child_rows(pool, seed)).items()
            assert call_id == seed.call_ids[0]
            assert _row_end(row) == RESTART_ROW

            successor = await _terminate_and_resume(
                pool, orchestrator, seed, dying, tmp_path, stack, monkeypatch
            )
            recovered = await successor.runtime().recover_orphans()

            assert successor.settle_requests() == 0  # the single-child path
            assert [entry["status"] for entry in recovered] == ["interrupted"]
            # Today's single-child shape: the child's evidence is the one
            # continuation event; the call itself gets no tool row.
            assert await _tool_rows(pool, seed) == []
            (continuation,) = await _continuations(pool, seed)
            assert continuation["role"] == "event"
            assert continuation["supersedes_input_seq"] == seed.input_seq
            assert "interrupted:parent_restart" in continuation["content"]
            assert LAST_WORDS[:20] in continuation["content"]
            assert await _pinned_source_state(pool, seed) == "settled"
            assert await successor.runtime().recover_orphans() == []

            claimed = await _pinned_claim(pool, seed)
            assert [row["message_id"] for row in claimed] == [
                str(continuation["message_id"])
            ]
    finally:
        await pool.close()
