"""Characterize current session fan-out policy through real tool and ledger paths."""

from __future__ import annotations

import asyncio
from contextlib import AsyncExitStack
import json
from pathlib import Path
import socket
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from urllib.parse import urlsplit
from uuid import UUID, uuid4

import asyncpg
import httpx
import pytest
import pytest_asyncio
import orchestrator
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage
from testcontainers.postgres import PostgresContainer

from agent.api.orchestrator_client import OrchestratorClient
from agent.core.thread_messages import _serialize_message_row
from agent.database.postgres_db import PostgresDB as AgentDB
from agent.persistent_graph import PermissionOutcome, _execute_turn
from agent.subagents import SessionHost, SubagentRuntime
from agent.subagents.session_persistence import SessionSubagentLedger
from agent.tools.delegation.delegate_agent import create_delegate_agent_tools
from orchestrator.database.migrate import run_migrations
from orchestrator.database.postgres import PostgresDB
from shared.session_subagent_authority import (
    SessionParentAuthority,
    SessionParentAuthorityRefused,
)
from tests._fake_chat_model import FakeChatModel, text_turn
from tests.test_delegate_agent_tool import make_parent
from tests.test_persistent_delegation_batch import _callbacks, _config, _context_manager

ROOT = Path(orchestrator.__file__).resolve().parents[2]
MIGRATIONS = ROOT / "src/orchestrator/database/migrations/app"
REFUSAL = (
    "Error: sessions may delegate only one child per parent turn. "
    "Re-issue one delegate_agent call."
)
pytestmark = pytest.mark.asyncio


@pytest.fixture(scope="module")
def pg_dsn():
    with PostgresContainer("postgres:16") as postgres:
        yield postgres.get_connection_url().replace("postgresql+psycopg2", "postgresql")


@pytest_asyncio.fixture(scope="module")
async def migrated_dsn(pg_dsn):
    async with asyncpg.create_pool(pg_dsn, min_size=1, max_size=2) as pool:
        await run_migrations(pool, MIGRATIONS)
    return pg_dsn


@pytest.fixture(autouse=True)
def deterministic_local_io(migrated_dsn, tmp_path, monkeypatch):
    """Only the owned database may use IP sockets during each test.

    Model/provider behavior is scripted; token budgets are not characterized
    here. Use the existing deterministic approximate counters in both the real
    child ContextManager and return-envelope planner. S9 separately certifies
    actual tokenizer assets/counting. Cold caches plus a recorded tripwire keep
    a caught download error from quietly weakening this isolation contract.
    """
    import tiktoken.load
    import tiktoken.registry

    from agent.core import context
    from shared.runtime.core import chunk_planner

    monkeypatch.setattr(context, "TIKTOKEN_AVAILABLE", False)
    monkeypatch.setattr(chunk_planner, "TIKTOKEN_AVAILABLE", False)
    monkeypatch.setattr(tiktoken.registry, "ENCODINGS", {})
    monkeypatch.setattr(chunk_planner, "_ENCODING_CACHE", {})
    monkeypatch.setenv("TIKTOKEN_CACHE_DIR", str(tmp_path / "empty-tokenizer-cache"))
    parsed = urlsplit(migrated_dsn)
    allowed_hosts = {parsed.hostname}
    allowed_hosts.update(
        row[4][0] for row in socket.getaddrinfo(parsed.hostname, parsed.port)
    )
    attempts = []

    def refuse(operation):
        attempts.append(operation)
        raise RuntimeError(f"Unexpected external I/O: {operation}")

    def no_tokenizer_download(_path):
        refuse("tokenizer asset download")

    connect = socket.socket.connect
    connect_ex = socket.socket.connect_ex
    getaddrinfo = socket.getaddrinfo

    def check_address(sock, address):
        # Testcontainers uses Unix sockets to its owned Docker daemon; its
        # database/Ryuk setup and teardown run outside this function fixture.
        if sock.family == socket.AF_UNIX:
            return
        if (
            isinstance(address, tuple)
            and address[0] in allowed_hosts
            and address[1] == parsed.port
        ):
            return
        refuse("non-database socket connect")

    def local_connect(sock, address):
        check_address(sock, address)
        return connect(sock, address)

    def local_connect_ex(sock, address):
        check_address(sock, address)
        return connect_ex(sock, address)

    def local_getaddrinfo(host, port, *args, **kwargs):
        if host not in allowed_hosts or str(port) != str(parsed.port):
            refuse("non-database DNS lookup")
        return getaddrinfo(host, port, *args, **kwargs)

    monkeypatch.setattr(tiktoken.load, "read_file", no_tokenizer_download)
    monkeypatch.setattr(socket.socket, "connect", local_connect)
    monkeypatch.setattr(socket.socket, "connect_ex", local_connect_ex)
    monkeypatch.setattr(socket, "getaddrinfo", local_getaddrinfo)
    yield
    assert attempts == [], "An external-I/O error was caught inside the runtime"


@pytest_asyncio.fixture
async def parent(migrated_dsn, monkeypatch):
    from orchestrator import main
    from orchestrator.security import access

    async with AsyncExitStack() as resources:
        db = PostgresDB(migrated_dsn, min_connections=1, max_connections=5)
        resources.push_async_callback(db.close)
        await db.connect()
        agent = AgentDB.__new__(AgentDB)
        agent._pool = db._pool
        agent._queries = {}
        owner, thread_id, agent_id = uuid4(), uuid4(), uuid4()
        pod_uid, pod_name, attempt = f"pod-{uuid4()}", f"parent-{uuid4()}", str(uuid4())
        async with db.acquire() as conn:
            await conn.execute(
                "INSERT INTO users (id, display_name) VALUES ($1, $2)",
                owner,
                "Test owner",
            )
            generation = await conn.fetchval(
                "INSERT INTO threads (id, user_id, title, status) "
                "VALUES ($1, $2, 'Delegation baseline', 'active') RETURNING runtime_generation",
                thread_id,
                owner,
            )
        assert await db.reserve_pinned_agent_pod_provision_intent(
            str(thread_id),
            expected_runtime_generation=str(generation),
            attempt_id=attempt,
            pod_name=pod_name,
            provisioner="agent",
            namespace="test",
        )
        assert await db.publish_pinned_agent_pod_provision_intent(
            str(thread_id),
            expected_runtime_generation=str(generation),
            attempt_id=attempt,
            pod_name=pod_name,
            pod_uid=pod_uid,
            namespace="test",
        )
        async with db.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    "INSERT INTO agents (id, config_name, status, metadata, hostname, pod_uid) "
                    "VALUES ($1, 'session_base', 'session', '{}'::jsonb, $2, $3)",
                    agent_id,
                    pod_name,
                    pod_uid,
                )
                binding = await conn.fetchrow(
                    "UPDATE threads SET agent_id=$2 WHERE id=$1 RETURNING runtime_generation, runtime_attach_token",
                    thread_id,
                    agent_id,
                )
                await conn.execute(
                    "UPDATE agents SET thread_id=$2 WHERE id=$1", agent_id, thread_id
                )
        authority = SessionParentAuthority(
            execution_lane="pinned",
            parent_thread_id=thread_id,
            agent_id=agent_id,
            pod_uid=pod_uid,
            session_runtime_generation=binding["runtime_generation"],
            runtime_attach_token=binding["runtime_attach_token"],
        )
        assert await agent.session_parent_authority_current(authority) is True
        monkeypatch.setattr(main.app.state.resources, "postgres_db", db)
        monkeypatch.setattr(access, "_INTERNAL_KEY", "draft-test-internal-key")
        requests = []

        async def record_request(request):
            requests.append((request.method, request.url.path))

        client = OrchestratorClient(
            "http://orchestrator.test", "127.0.0.1", 8002, "test-parent", "session_base"
        )
        client._client = await resources.enter_async_context(
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=main.app),
                headers={"X-Internal-Key": "draft-test-internal-key"},
                event_hooks={"request": [record_request]},
            )
        )
        yield SimpleNamespace(
            db=db,
            agent=agent,
            thread_id=str(thread_id),
            authority=authority,
            client=client,
            requests=requests,
        )


async def _children(parent):
    async with parent.db.acquire() as conn:
        return await conn.fetch(
            "SELECT id, runtime_generation, status, parent_tool_call_id, "
            "subagent_status, metadata FROM threads "
            "WHERE kind='subagent' AND parent_thread_id=$1 ORDER BY parent_tool_call_id",
            UUID(parent.thread_id),
        )


def _runtime(parent, tmp_path, *, cap):
    ctx, _ = make_parent(tmp_path, max_concurrent=cap)
    ctx._subagent_parent_kind = "session"
    ctx._thread_id = parent.thread_id
    ctx._job_id = parent.thread_id
    ctx._job_metadata = {"thread_id": parent.thread_id}
    ctx._current_turn_count = 1
    ctx._session_parent_authority_provider = lambda: parent.authority
    ctx.postgres_db = parent.agent
    ctx.orchestrator_client = parent.client
    ctx.provider_admission = lambda: True
    models = []

    def child_model(_config, _limits):
        model = FakeChatModel([text_turn("child evidence")])
        models.append(model)
        return model

    async def exact_authority():
        return await parent.agent.session_parent_authority_current(parent.authority)

    host = SessionHost(
        thread_id=parent.thread_id,
        agent_type="persistent",
        tool_context=ctx,
        postgres=parent.agent,
        admission_fn=lambda: True,
        effect_authority_fn=exact_authority,
        settlement_authority_fn=exact_authority,
    )
    ledger = SessionSubagentLedger.from_context(ctx)
    assert ledger is not None
    runtime = SubagentRuntime.from_context(
        ctx,
        host,
        ledger=ledger,
        llm_factory=child_model,
        driver_kwargs={
            "watcher_poll_interval": 0.01,
            "archiver": None,
            "archive_fn": lambda **kwargs: None,
        },
    )
    ctx._parent_host = host
    ctx.subagent_runtime = runtime
    tool = create_delegate_agent_tools(ctx)[0]
    return ctx, runtime, ledger, tool, models, exact_authority


async def _run(parent, tmp_path, *, cap, count, batch_sizes=None, permissions=None):
    ctx, runtime, ledger, tool, models, exact_authority = _runtime(
        parent, tmp_path, cap=cap
    )
    human = HumanMessage(content="Inspect two independent things.", id=str(uuid4()))
    ctx._current_input_message_id = human.id
    await parent.agent.save_thread_message(
        parent.thread_id, **_serialize_message_row(human, 1)
    )
    calls = [
        {
            "name": "delegate_agent",
            "id": f"call-{index}",
            "args": {
                "description": f"Inspect {index}",
                "prompt": f"Report evidence {index}.",
                "subagent_type": "explorer",
                "run_in_background": False,
            },
        }
        for index in range(count)
    ]
    if batch_sizes is None:
        batch_sizes = [count]
    assert sum(batch_sizes) == count
    responses = []
    offset = 0
    for size in batch_sizes:
        responses.append(
            [
                AIMessage(
                    content="",
                    id=str(uuid4()),
                    tool_calls=calls[offset : offset + size],
                )
            ]
        )
        offset += size
    llm = FakeChatModel([*responses, text_turn("parent done")])
    persisted = []

    async def persist(message):
        row = await parent.agent.save_thread_message(
            parent.thread_id, **_serialize_message_row(message, 1)
        )
        assert row["id"] and isinstance(row["seq"], int)
        persisted.append((message, row))
        return True

    callbacks = _callbacks(
        persist_message=persist,
        require_delegation_persistence=True,
        before_provider_admission=lambda: True,
        before_provider_execution=exact_authority,
    )
    if permissions is not None:
        callbacks.permission_check = AsyncMock(side_effect=permissions)
    messages = [
        SystemMessage(content="Delegate the requested independent tasks."),
        human,
    ]
    context_manager = _context_manager()
    context_manager.record_provider_usage = Mock()
    try:
        async with asyncio.timeout(15):
            result = await _execute_turn(
                llm_with_tools=llm,
                tool_map={"delegate_agent": tool},
                context_manager=context_manager,
                messages=messages,
                callbacks=callbacks,
                llm_timeout=10,
                auxiliary_llm=None,
                config=_config(),
                tool_context=ctx,
                turn_id=1,
            )
        return SimpleNamespace(
            context=ctx,
            runtime=runtime,
            ledger=ledger,
            models=models,
            messages=messages,
            result=result,
            persisted=persisted,
            calls=calls,
            callbacks=callbacks,
            parent_model=llm,
        )
    finally:
        await runtime.close()


@pytest.mark.parametrize("cap", [1, 2])
async def test_successive_single_delegate_batches_complete_in_one_parent_turn(
    parent, tmp_path, cap
):
    run = await _run(parent, tmp_path, cap=cap, count=2, batch_sizes=[1, 1])
    outputs = [message for message in run.messages if isinstance(message, ToolMessage)]
    assert [message.tool_call_id for message in outputs] == ["call-0", "call-1"]
    assert all("child evidence" in message.content for message in outputs)
    assert all(not message.content.startswith("Error:") for message in outputs)
    assert run.result.error is None
    assert run.result.tool_calls_made == 2
    assert len(run.parent_model.calls) == 3
    assert len(run.models) == 2
    assert all(len(model.calls) == 1 for model in run.models)
    children = await _children(parent)
    assert [row["parent_tool_call_id"] for row in children] == ["call-0", "call-1"]
    assert all(row["subagent_status"] == "completed" for row in children)
    assert all(row["status"] == "ended" for row in children)
    async with parent.db.acquire() as conn:
        rows = await conn.fetch(
            "SELECT role, tool_calls, tool_call_id, turn_number FROM thread_messages "
            "WHERE thread_id=$1 ORDER BY seq",
            UUID(parent.thread_id),
        )
    assistant_batches = [
        [call["id"] for call in json.loads(row["tool_calls"])]
        for row in rows
        if row["role"] == "ai" and row["tool_calls"]
    ]
    assert assistant_batches == [["call-0"], ["call-1"]]
    assert [row["tool_call_id"] for row in rows if row["role"] == "tool"] == [
        "call-0",
        "call-1",
    ]
    assert {row["turn_number"] for row in rows} == {1}


@pytest.mark.parametrize("approved_index", [0, 1])
async def test_partly_approved_batch_runs_only_approved_child_in_provider_order(
    parent, tmp_path, approved_index
):
    permissions = [PermissionOutcome.DECLINED, PermissionOutcome.DECLINED]
    permissions[approved_index] = PermissionOutcome.APPROVED
    run = await _run(parent, tmp_path, cap=2, count=2, permissions=permissions)
    outputs = [message for message in run.messages if isinstance(message, ToolMessage)]
    assert [message.tool_call_id for message in outputs] == ["call-0", "call-1"]
    assert "child evidence" in outputs[approved_index].content
    assert not outputs[approved_index].content.startswith("Error:")
    assert outputs[1 - approved_index].content == "User declined this tool call."
    assert run.result.error is None
    assert run.result.tool_calls_made == 1
    assert run.runtime.batch_size == 1
    assert len(run.models) == 1
    assert len(run.models[0].calls) == 1
    assert run.callbacks.permission_check.await_count == 2
    children = await _children(parent)
    assert len(children) == 1
    assert children[0]["parent_tool_call_id"] == f"call-{approved_index}"
    assert children[0]["subagent_status"] == "completed"
    assert children[0]["status"] == "ended"
    async with parent.db.acquire() as conn:
        rows = await conn.fetch(
            "SELECT tool_call_id, content, turn_number FROM thread_messages "
            "WHERE thread_id=$1 AND role='tool' ORDER BY seq",
            UUID(parent.thread_id),
        )
    assert [(row["tool_call_id"], row["content"]) for row in rows] == [
        (message.tool_call_id, message.content) for message in outputs
    ]
    assert {row["turn_number"] for row in rows} == {1}


@pytest.mark.parametrize("cap", [2, 1])
async def test_two_approved_session_calls_are_refused_before_runtime_and_durable_create(
    parent, tmp_path, cap
):
    run = await _run(parent, tmp_path, cap=cap, count=2)
    outputs = [message for message in run.messages if isinstance(message, ToolMessage)]
    assert [(message.tool_call_id, message.content) for message in outputs] == [
        ("call-0", REFUSAL),
        ("call-1", REFUSAL),
    ]
    assert run.result.tool_calls_made == 2
    assert run.runtime.batch_size == 2
    assert run.runtime.max_concurrent == cap
    assert run.models == []
    assert run.ledger.rows == {}
    assert parent.requests == []
    assert await _children(parent) == []
    assert run.callbacks.permission_check.await_count == 2
    async with parent.db.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, role, tool_calls, tool_call_id, content, turn_number FROM thread_messages "
            "WHERE thread_id=$1 ORDER BY seq",
            UUID(parent.thread_id),
        )
    parent_calls = [row for row in rows if row["role"] == "ai" and row["tool_calls"]]
    assert len(parent_calls) == 1
    assert [call["id"] for call in json.loads(parent_calls[0]["tool_calls"])] == [
        "call-0",
        "call-1",
    ]
    assert parent_calls[0]["turn_number"] == 1
    assert [
        (row["tool_call_id"], row["content"]) for row in rows if row["role"] == "tool"
    ] == [("call-0", REFUSAL), ("call-1", REFUSAL)]


async def test_single_session_delegate_reaches_real_ledger_http_and_child_provider(
    parent, tmp_path
):
    run = await _run(parent, tmp_path, cap=2, count=1)
    outputs = [message for message in run.messages if isinstance(message, ToolMessage)]
    assert len(outputs) == 1
    assert outputs[0].tool_call_id == "call-0"
    assert "child evidence" in outputs[0].content
    assert not outputs[0].content.startswith("Error:")
    assert len(run.models) == 1
    assert len(run.models[0].calls) == 1
    children = await _children(parent)
    assert len(children) == 1
    assert children[0]["parent_tool_call_id"] == "call-0"
    assert children[0]["subagent_status"] == "completed"
    child_id = str(children[0]["id"])
    assert children[0]["status"] == "ended"
    assert str(children[0]["runtime_generation"]) == run.ledger.generations[child_id]
    async with parent.db.acquire() as conn:
        parent_rows = await conn.fetch(
            "SELECT role, tool_call_id, content, turn_number FROM thread_messages "
            "WHERE thread_id=$1 ORDER BY seq",
            UUID(parent.thread_id),
        )
        child_rows = await conn.fetch(
            "SELECT role, content, turn_number FROM thread_messages "
            "WHERE thread_id=$1 ORDER BY seq",
            UUID(child_id),
        )
    assert [
        (row["tool_call_id"], row["content"], row["turn_number"])
        for row in parent_rows
        if row["role"] == "tool"
    ] == [("call-0", outputs[0].content, 1)]
    assert [
        (row["role"], row["content"], row["turn_number"])
        for row in child_rows
        if row["role"] in {"human", "ai"}
    ] == [("human", "Report evidence 0.", 1), ("ai", "child evidence", 1)]
    assert parent.requests == [
        ("POST", f"/api/agents/threads/{parent.thread_id}/subagents")
    ]
    assert len(run.ledger.rows) == 1


async def _direct_ledger_admission(parent, tmp_path, *, stale=False):
    """Bypass only the model-facing adapter to characterize the lower boundary."""
    ctx, runtime, ledger, _tool, models, _authority = _runtime(parent, tmp_path, cap=2)
    human = HumanMessage(content="Two requests", id=str(uuid4()))
    assistant = AIMessage(
        content="",
        id=str(uuid4()),
        tool_calls=[
            {"id": f"call-{index}", "name": "delegate_agent", "args": {}}
            for index in range(2)
        ],
    )
    for message in (human, assistant):
        await parent.agent.save_thread_message(
            parent.thread_id, **_serialize_message_row(message, 1)
        )
    ctx._current_input_message_id = human.id
    ctx._current_ai_message_id = assistant.id
    if stale:
        parent.authority = parent.authority.model_copy(
            update={"runtime_attach_token": uuid4()}
        )
    children = [str(uuid4()), str(uuid4())]

    async def open_child(index):
        return await ledger.open(
            children[index],
            status="running",
            parent_thread_id=parent.thread_id,
            parent_job_id=None,
            parent_input_message_id=human.id,
            parent_ai_message_id=assistant.id,
            parent_tool_call_id=f"call-{index}",
            handle=f"explorer-{index}",
            subagent_type="explorer",
            isolation="shared",
            write_policy="none",
            run_in_background=False,
        )

    try:
        if stale:
            with pytest.raises(SessionParentAuthorityRefused) as refused:
                await open_child(0)
            assert refused.value.reason == "pinned_parent_not_current"
            assert await _children(parent) == []
            assert ledger.rows == {}
            assert len(parent.requests) == 1
        else:
            receipts = await asyncio.wait_for(
                asyncio.gather(open_child(0), open_child(1)), timeout=5
            )
            assert [receipt["thread_id"] for receipt in receipts] == children
            assert all(UUID(receipt["runtime_generation"]) for receipt in receipts)
            rows = await _children(parent)
            assert [str(row["id"]) for row in rows] == children
            assert [str(row["runtime_generation"]) for row in rows] == [
                receipt["runtime_generation"] for receipt in receipts
            ]
            assert [row["parent_tool_call_id"] for row in rows] == ["call-0", "call-1"]
            assert all(row["subagent_status"] == "running" for row in rows)
            assert {
                json.loads(row["metadata"])["subagent"]["parent_ai_message_id"]
                for row in rows
            } == {assistant.id}
            assert {
                json.loads(row["metadata"])["subagent"]["parent_iteration"]
                for row in rows
            } == {1}
            assert {
                json.loads(row["metadata"])["subagent"]["parent_input_message_id"]
                for row in rows
            } == {human.id}
            assert len(parent.requests) == 2
            assert len(ledger.rows) == 2
        # These calls prove HTTP/DB admission only: no runtime scheduling or
        # provider invocation is inferred from direct durable-ledger admission.
        assert models == []
    finally:
        await runtime.close()


async def test_ledger_http_database_can_admit_two_calls_from_one_exact_parent_message(
    parent, tmp_path
):
    await _direct_ledger_admission(parent, tmp_path)


async def test_ledger_http_database_refuses_stale_parent_before_child_insert(
    parent, tmp_path
):
    await _direct_ledger_admission(parent, tmp_path, stale=True)
