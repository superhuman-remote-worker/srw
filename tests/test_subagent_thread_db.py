"""The subagent thread accessors on BOTH pools (U3 WP3, plan B.1 / D).

Strict asyncpg seams in the ``test_save_thread_message_columns.py`` idiom:
``PostgresDB.__new__`` + a mocked ``acquire``, asserting the SQL columns and
the ``$n`` argument order — because a column the SQL names but the arguments
do not carry (or vice versa) is exactly what a fake pool never notices.

Orchestrator side: ``create_subagent_thread`` (the row derived from the
job), ``list_subagent_threads``, ``get_subagent_thread_by_call``, the
always-on ``kind = 'session'`` gate on ``list_threads``, and ``delete_job``
ending live children before the cascade. Agent side:
``update_subagent_thread`` (the ``kind = 'subagent'`` guard) and
``get_subagent_thread_by_call``.
"""

from __future__ import annotations

import json
from contextlib import asynccontextmanager
from functools import partial
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest

import orchestrator.database.postgres as orchestrator_postgres
import agent.database.postgres_db as agent_postgres
from orchestrator.database.postgres import PostgresDB as OrchestratorDB
from agent.database.postgres_db import PostgresDB as AgentDB
from shared.subagent_parent_authority import ParentExecutionAuthority

JOB = UUID("aaaaaaaa-1111-4222-8333-444444444444")
CHILD = UUID("bbbbbbbb-1111-4222-8333-444444444444")
GENERATION = UUID("dddddddd-1111-4222-8333-444444444444")
NEXT_GENERATION = UUID("eeeeeeee-1111-4222-8333-444444444444")
DELIVERY = UUID("ffffffff-1111-4222-8333-444444444444")
AGENT = UUID("99999999-1111-4222-8333-444444444444")
AUTHORITY = ParentExecutionAuthority(
    execution_lane="pinned",
    parent_job_id=JOB,
    agent_id=AGENT,
    pod_uid="pod-test",
    dispatch_process_generation="process-test",
)


@pytest.fixture(autouse=True)
def _exact_parent_authority_gate(monkeypatch):
    """These SQL-shape tests isolate child accessors; the shared gate has its
    own lock-order tests in ``test_subagent_parent_authority.py``."""

    gate = AsyncMock(
        return_value={
            "id": JOB,
            "status": "processing",
            "execution_lane": "pinned",
            "assigned_agent_id": AGENT,
        }
    )
    monkeypatch.setattr(
        orchestrator_postgres, "require_parent_execution_authority", gate
    )
    monkeypatch.setattr(agent_postgres, "require_parent_execution_authority", gate)
    settlement_gate = AsyncMock(
        return_value={
            "id": JOB,
            "status": "cancelled",
            "execution_lane": "pinned",
            "assigned_agent_id": None,
        }
    )
    monkeypatch.setattr(
        orchestrator_postgres,
        "require_parent_execution_settlement_authority",
        settlement_gate,
    )
    monkeypatch.setattr(
        agent_postgres,
        "require_parent_execution_settlement_authority",
        settlement_gate,
    )
    gate.settlement_gate = settlement_gate
    return gate


def _compact(sql: str) -> str:
    return " ".join(sql.split())


def _conn() -> AsyncMock:
    conn = AsyncMock()
    transaction = MagicMock()
    transaction.__aenter__ = AsyncMock(return_value=transaction)
    transaction.__aexit__ = AsyncMock(return_value=False)
    conn.transaction = MagicMock(return_value=transaction)
    return conn


def _orchestrator_db(conn) -> OrchestratorDB:
    db = OrchestratorDB.__new__(OrchestratorDB)
    db._pool = MagicMock()
    db._connection_string = "test"
    db._queries = {}

    @asynccontextmanager
    async def acquire():
        yield conn

    db.acquire = acquire
    db.create_subagent_thread = partial(
        db.create_subagent_thread, parent_authority=AUTHORITY
    )
    db.list_live_subagent_threads = partial(
        db.list_live_subagent_threads, parent_authority=AUTHORITY
    )
    db.get_subagent_thread = partial(db.get_subagent_thread, parent_authority=AUTHORITY)
    db.get_subagent_thread_by_call = partial(
        db.get_subagent_thread_by_call, parent_authority=AUTHORITY
    )
    db.reopen_subagent_thread = partial(
        db.reopen_subagent_thread, parent_authority=AUTHORITY
    )
    db.terminalize_subagent_thread_and_enqueue = partial(
        db.terminalize_subagent_thread_and_enqueue, parent_authority=AUTHORITY
    )
    return db


def _agent_db(conn) -> AgentDB:
    db = AgentDB.__new__(AgentDB)
    db._pool = MagicMock()
    db._queries = {}

    @asynccontextmanager
    async def acquire():
        yield conn

    db.acquire = acquire
    db.update_subagent_thread = partial(
        db.update_subagent_thread,
        parent_job_id=str(JOB),
        parent_authority=AUTHORITY,
    )
    db.list_live_subagent_threads = partial(
        db.list_live_subagent_threads, parent_authority=AUTHORITY
    )
    db.get_subagent_thread = partial(db.get_subagent_thread, parent_authority=AUTHORITY)
    db.get_subagent_thread_by_call = partial(
        db.get_subagent_thread_by_call, parent_authority=AUTHORITY
    )
    return db


# ---------------------------------------------------------------------------
# Orchestrator: create_subagent_thread
# ---------------------------------------------------------------------------


class TestCreateSubagentThread:
    @pytest.mark.asyncio
    async def test_the_row_is_derived_from_the_job_in_one_insert(self):
        conn = _conn()
        conn.fetchrow = AsyncMock(
            return_value={"id": CHILD, "runtime_generation": GENERATION}
        )
        db = _orchestrator_db(conn)

        created = await db.create_subagent_thread(
            parent_job_id=str(JOB),
            thread_id=str(CHILD),
            handle="implementer-7f3a",
            subagent_type="implementer",
            parent_tool_call_id="call-1",
            isolation="worktree",
            write_policy="owned_paths",
            owned_paths=["src/**", "tests/test_parser.py"],
            brief_description="implement   the parser",
            parent_iteration=12,
            fork=True,
        )

        assert created == {
            "thread_id": str(CHILD),
            "runtime_generation": str(GENERATION),
        }
        sql = _compact(conn.fetchrow.call_args[0][0])
        args = conn.fetchrow.call_args[0][1:]
        assert sql.startswith("INSERT INTO threads (")
        # The column list and the SELECT list must line up: every identity
        # fact the row needs, none from the caller that the job owns.
        assert (
            "INSERT INTO threads ( id, kind, user_id, project_id, title, status, "
            "permission_mode, narration_mode, execution_lane, metadata, "
            "parent_job_id, parent_thread_id, parent_tool_call_id, "
            "subagent_handle, subagent_type, subagent_status )" in sql
        )
        assert (
            "SELECT $1, 'subagent', j.user_id, j.project_id, $3, $8, "
            "'autonomous', 'silent', 'pinned', $4::jsonb, j.id, NULL, $5, $6, "
            "$7, $9 FROM jobs j WHERE j.id = $2" in sql
        )
        assert "ON CONFLICT DO NOTHING RETURNING id, runtime_generation" in sql
        assert args[0] == CHILD and args[1] == JOB
        assert args[2] == "implementer-7f3a: implement the parser"
        metadata = json.loads(args[3])
        assert metadata["datasource_ids"] == []
        assert metadata["subagent"] == {
            "type": "implementer",
            "handle": "implementer-7f3a",
            "isolation": "worktree",
            "write_policy": "owned_paths",
            "owned_paths": ["src/**", "tests/test_parser.py"],
            "brief_description": "implement the parser",
            "parent_iteration": 12,
            "fork": True,
            "run_in_background": False,
        }
        assert args[4] == "call-1"
        assert args[5] == "implementer-7f3a"
        assert args[6] == "implementer"
        assert args[7:] == ("active", "running")
        assert conn.fetchrow.await_count == 1

    @pytest.mark.asyncio
    async def test_a_fresh_id_is_minted_when_none_is_given(self):
        conn = _conn()
        conn.fetchrow = AsyncMock(
            side_effect=lambda sql, *a: {
                "id": a[0],
                "runtime_generation": GENERATION,
            }
        )
        db = _orchestrator_db(conn)
        created = await db.create_subagent_thread(
            parent_job_id=str(JOB), handle="h-0001", subagent_type="explorer"
        )
        assert UUID(created["thread_id"])  # a valid uuid the caller did not choose
        assert created["runtime_generation"] == str(GENERATION)
        args = conn.fetchrow.call_args[0][1:]
        assert args[2] == "subagent h-0001"  # no brief → the handle titles it
        assert args[4] is None

    @pytest.mark.asyncio
    async def test_queued_create_is_durable_before_it_can_run(self):
        conn = _conn()
        conn.fetchrow = AsyncMock(
            return_value={"id": CHILD, "runtime_generation": GENERATION}
        )
        db = _orchestrator_db(conn)
        await db.create_subagent_thread(
            parent_job_id=str(JOB),
            thread_id=str(CHILD),
            handle="h-0001",
            subagent_type="explorer",
            initial_status="queued",
        )
        assert conn.fetchrow.call_args.args[-2:] == ("created", "queued")

    @pytest.mark.asyncio
    async def test_a_missing_job_yields_none_after_one_probe(self):
        """No row back means either no job or an already-created id — the
        follow-up SELECT tells them apart."""
        conn = _conn()
        conn.fetchrow = AsyncMock(side_effect=[None, None])
        db = _orchestrator_db(conn)
        assert (
            await db.create_subagent_thread(
                parent_job_id=str(JOB),
                thread_id=str(CHILD),
                handle="h-0001",
                subagent_type="explorer",
            )
            is None
        )
        probe = _compact(conn.fetchrow.call_args_list[1].args[0])
        assert "SELECT id, runtime_generation FROM threads" in probe
        assert "parent_job_id = $2" in probe
        args = conn.fetchrow.call_args_list[1].args[1:]
        assert args[:-1] == (CHILD, JOB, "h-0001", "explorer", None, False)
        assert json.loads(args[-1]) == {
            "brief_description": "",
            "fork": False,
            "handle": "h-0001",
            "isolation": "shared",
            "owned_paths": [],
            "parent_iteration": None,
            "run_in_background": False,
            "type": "explorer",
            "write_policy": "none",
        }

    @pytest.mark.asyncio
    async def test_a_retried_create_returns_the_existing_id(self):
        conn = _conn()
        conn.fetchrow = AsyncMock(
            side_effect=[
                None,
                {"id": CHILD, "runtime_generation": GENERATION},
            ]
        )
        db = _orchestrator_db(conn)
        assert await db.create_subagent_thread(
            parent_job_id=str(JOB),
            thread_id=str(CHILD),
            handle="h-0001",
            subagent_type="explorer",
        ) == {
            "thread_id": str(CHILD),
            "runtime_generation": str(GENERATION),
        }
        probe = _compact(conn.fetchrow.call_args_list[1].args[0])
        assert "run_in_background" in probe
        assert "metadata->'subagent' = $7::jsonb" in probe
        args = conn.fetchrow.call_args_list[1].args
        assert args[-2] is False
        assert json.loads(args[-1])["owned_paths"] == []

    @pytest.mark.asyncio
    async def test_malformed_ids_never_reach_sql(self):
        conn = _conn()
        db = _orchestrator_db(conn)
        assert (
            await db.create_subagent_thread(
                parent_job_id="not-a-uuid", handle="h", subagent_type="explorer"
            )
            is None
        )
        assert (
            await db.create_subagent_thread(
                parent_job_id=str(JOB),
                thread_id="nope",
                handle="h",
                subagent_type="explorer",
            )
            is None
        )
        with pytest.raises(ValueError, match="cannot also carry parent_thread_id"):
            await db.create_subagent_thread(
                parent_job_id=str(JOB),
                parent_thread_id="nope",
                handle="h",
                subagent_type="explorer",
            )
        conn.fetchrow.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_handle_and_a_type_are_required(self):
        db = _orchestrator_db(_conn())
        with pytest.raises(ValueError):
            await db.create_subagent_thread(
                parent_job_id=str(JOB), handle="", subagent_type="explorer"
            )
        with pytest.raises(ValueError):
            await db.create_subagent_thread(
                parent_job_id=str(JOB), handle="h-0001", subagent_type="  "
            )
        with pytest.raises(ValueError):
            await db.create_subagent_thread(
                parent_job_id=str(JOB),
                handle="h-0001",
                subagent_type="explorer",
                initial_status="completed",
            )


# ---------------------------------------------------------------------------
# Orchestrator: the reads
# ---------------------------------------------------------------------------


class TestOrchestratorReads:
    @pytest.mark.asyncio
    async def test_list_subagent_threads_walks_the_job_in_spawn_order(self):
        conn = _conn()
        conn.fetch = AsyncMock(return_value=[{"id": CHILD, "subagent_handle": "h"}])
        db = _orchestrator_db(conn)
        rows = await db.list_subagent_threads(str(JOB))
        assert rows == [{"id": CHILD, "subagent_handle": "h"}]
        sql = _compact(conn.fetch.call_args[0][0])
        assert "FROM threads WHERE kind = 'subagent' AND parent_job_id = $1" in sql
        assert "ORDER BY created_at, id" in sql
        for column in (
            "runtime_generation",
            "subagent_handle",
            "subagent_type",
            "subagent_status",
            "subagent_outcome",
            "subagent_error",
            "report_path",
            "parent_tool_call_id",
            "total_turns",
            "total_tokens",
            "metadata",
            "created_at",
            "ended_at",
        ):
            assert column in sql, column
        assert conn.fetch.call_args[0][1] == JOB

    @pytest.mark.asyncio
    async def test_list_subagent_threads_refuses_a_bad_id_without_sql(self):
        conn = _conn()
        db = _orchestrator_db(conn)
        assert await db.list_subagent_threads("job-42") == []
        conn.fetch.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_get_by_call_is_keyed_on_job_and_call_newest_first(self):
        conn = _conn()
        conn.fetchrow = AsyncMock(return_value={"id": CHILD})
        db = _orchestrator_db(conn)
        assert await db.get_subagent_thread_by_call(str(JOB), " call-1 ") == {
            "id": CHILD
        }
        sql = _compact(conn.fetchrow.call_args[0][0])
        assert (
            "WHERE kind = 'subagent' AND parent_job_id = $1 AND "
            "parent_tool_call_id = $2 ORDER BY created_at DESC, id DESC LIMIT 1" in sql
        )
        assert conn.fetchrow.call_args[0][1:] == (JOB, "call-1")

    @pytest.mark.asyncio
    async def test_get_by_call_needs_both_keys(self):
        conn = _conn()
        db = _orchestrator_db(conn)
        assert await db.get_subagent_thread_by_call(str(JOB), "") is None
        assert await db.get_subagent_thread_by_call("nope", "call-1") is None
        conn.fetchrow.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_live_list_is_doubly_fenced_and_includes_generation(self):
        conn = _conn()
        conn.fetch = AsyncMock(
            return_value=[{"id": CHILD, "runtime_generation": GENERATION}]
        )
        db = _orchestrator_db(conn)
        assert await db.list_live_subagent_threads(str(JOB)) == [
            {"id": CHILD, "runtime_generation": GENERATION}
        ]
        sql = _compact(conn.fetch.call_args.args[0])
        assert "status IN ('created', 'active')" in sql
        assert "subagent_status IN ('queued', 'running')" in sql
        assert "runtime_generation" in sql

    @pytest.mark.asyncio
    async def test_exact_child_lookup_is_scoped_to_parent(self):
        conn = _conn()
        conn.fetchrow = AsyncMock(
            return_value={"id": CHILD, "runtime_generation": GENERATION}
        )
        db = _orchestrator_db(conn)
        row = await db.get_subagent_thread(str(JOB), str(CHILD))
        assert row == {"id": CHILD, "runtime_generation": GENERATION}
        sql = _compact(conn.fetchrow.call_args.args[0])
        assert "id = $1" in sql and "parent_job_id = $2" in sql
        assert conn.fetchrow.call_args.args[1:] == (CHILD, JOB)


class TestReopenSubagentThread:
    @pytest.mark.asyncio
    async def test_ended_to_created_returns_the_trigger_rotated_generation(self):
        conn = _conn()
        conn.fetchval = AsyncMock(return_value=JOB)
        conn.fetchrow = AsyncMock(
            side_effect=[
                {"status": "ended", "runtime_generation": GENERATION},
                {"runtime_generation": NEXT_GENERATION},
            ]
        )
        db = _orchestrator_db(conn)

        assert await db.reopen_subagent_thread(
            parent_job_id=str(JOB),
            thread_id=str(CHILD),
            runtime_generation=str(GENERATION),
        ) == {
            "result": "reopened",
            "thread_id": str(CHILD),
            "runtime_generation": str(NEXT_GENERATION),
        }
        lock_sql = _compact(conn.fetchrow.call_args_list[0].args[0])
        reopen_sql = _compact(conn.fetchrow.call_args_list[1].args[0])
        assert lock_sql.endswith("FOR UPDATE")
        assert "SET status = 'created'" in reopen_sql
        assert "subagent_status = 'queued'" in reopen_sql
        assert "RETURNING runtime_generation" in reopen_sql

    @pytest.mark.asyncio
    async def test_stale_generation_never_takes_the_reopen_edge(self):
        conn = _conn()
        conn.fetchval = AsyncMock(return_value=JOB)
        conn.fetchrow = AsyncMock(
            return_value={"status": "ended", "runtime_generation": NEXT_GENERATION}
        )
        db = _orchestrator_db(conn)
        result = await db.reopen_subagent_thread(
            parent_job_id=str(JOB),
            thread_id=str(CHILD),
            runtime_generation=str(GENERATION),
        )
        assert result["result"] == "stale"
        assert conn.fetchrow.await_count == 1


class TestTerminalizeSubagentAndEnqueue:
    timestamp = "2026-09-01T01:02:03+00:00"

    @pytest.mark.asyncio
    async def test_child_terminalization_and_reply_append_share_one_transaction(self):
        conn = _conn()
        conn.fetchrow = AsyncMock(
            side_effect=[
                {"status": "processing", "context": {"keep": "yes"}},
                {
                    "runtime_generation": GENERATION,
                    "status": "active",
                    "subagent_handle": "explorer-7f3a",
                },
            ]
        )
        conn.fetchval = AsyncMock(return_value=CHILD)
        conn.execute = AsyncMock(return_value="UPDATE 1")
        db = _orchestrator_db(conn)

        result = await db.terminalize_subagent_thread_and_enqueue(
            parent_job_id=str(JOB),
            thread_id=str(CHILD),
            runtime_generation=str(GENERATION),
            delivery_id=str(DELIVERY),
            message="child report",
            timestamp=self.timestamp,
            subagent_status="completed",
            outcome="completed",
            turns=3,
            tokens=1200,
        )

        assert result["result"] == "applied"
        assert result["delivery"] == {
            "id": str(DELIVERY),
            "source": "subagent",
            "thread_id": str(CHILD),
            "handle": "explorer-7f3a",
            "run_generation": str(GENERATION),
            "message": "child report",
            "timestamp": self.timestamp,
        }
        parent_lock = _compact(conn.fetchrow.call_args_list[0].args[0])
        child_lock = _compact(conn.fetchrow.call_args_list[1].args[0])
        terminal = _compact(conn.fetchval.call_args.args[0])
        append = _compact(conn.execute.call_args.args[0])
        assert (
            parent_lock == "SELECT status, context FROM jobs WHERE id = $1 FOR UPDATE"
        )
        assert child_lock.endswith("FOR UPDATE")
        assert "runtime_generation = $3" in terminal
        assert "status <> 'ended'" in terminal
        assert "'{queued_replies}'" in append
        queued = json.loads(conn.execute.call_args.args[1])
        assert queued == [result["delivery"]]

    @pytest.mark.asyncio
    async def test_cancelled_parent_settles_child_without_dead_lane_b_delivery(self):
        conn = _conn()
        conn.fetchrow = AsyncMock(
            side_effect=[
                {"status": "cancelled", "context": {"keep": "yes"}},
                {
                    "runtime_generation": GENERATION,
                    "status": "active",
                    "subagent_handle": "reader-aa09",
                    "subagent_status": "running",
                    "subagent_outcome": None,
                    "total_turns": 1,
                    "total_tokens": 10,
                    "report_path": None,
                    "subagent_error": None,
                },
            ]
        )
        conn.fetchval = AsyncMock(return_value=CHILD)
        db = _orchestrator_db(conn)

        result = await db.terminalize_subagent_thread_and_enqueue(
            parent_job_id=str(JOB),
            thread_id=str(CHILD),
            runtime_generation=str(GENERATION),
            delivery_id=str(DELIVERY),
            message="cancelled child evidence",
            timestamp=self.timestamp,
            subagent_status="interrupted",
            outcome="interrupted:parent_cancelled",
            turns=1,
            tokens=10,
        )

        assert result == {
            "result": "applied",
            "thread_id": str(CHILD),
            "runtime_generation": str(GENERATION),
            "delivery_id": str(DELIVERY),
            "delivery_state": "suppressed",
        }
        conn.execute.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "lane,state", [("queued_replies", "queued"), ("consumed_replies", "consumed")]
    )
    async def test_retry_is_idempotent_before_or_after_consumption(self, lane, state):
        reply = {
            "id": str(DELIVERY),
            "source": "subagent",
            "thread_id": str(CHILD),
            "handle": "explorer-7f3a",
            "run_generation": str(GENERATION),
            "message": "child report",
            "timestamp": self.timestamp,
        }
        conn = _conn()
        conn.fetchrow = AsyncMock(
            return_value={"status": "processing", "context": {lane: [reply]}}
        )
        db = _orchestrator_db(conn)
        result = await db.terminalize_subagent_thread_and_enqueue(
            parent_job_id=str(JOB),
            thread_id=str(CHILD),
            runtime_generation=str(GENERATION),
            delivery_id=str(DELIVERY),
            message="child report",
            timestamp=self.timestamp,
            subagent_status="completed",
        )
        assert result["result"] == "idempotent"
        assert result["delivery_state"] == state
        assert conn.fetchrow.await_count == 1
        conn.fetchval.assert_not_awaited()
        conn.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_stale_generation_cannot_terminalize_or_deliver(self):
        conn = _conn()
        conn.fetchrow = AsyncMock(
            side_effect=[
                {"status": "processing", "context": {}},
                {
                    "runtime_generation": NEXT_GENERATION,
                    "status": "active",
                    "subagent_handle": "explorer-7f3a",
                },
            ]
        )
        db = _orchestrator_db(conn)
        result = await db.terminalize_subagent_thread_and_enqueue(
            parent_job_id=str(JOB),
            thread_id=str(CHILD),
            runtime_generation=str(GENERATION),
            delivery_id=str(DELIVERY),
            message="stale report",
            timestamp=self.timestamp,
            subagent_status="completed",
        )
        assert result["result"] == "stale"
        conn.fetchval.assert_not_awaited()
        conn.execute.assert_not_awaited()


class TestListThreadsSessionGate:
    """``list_threads`` is the sessions page's query; child rows must never
    reach it, whatever the caller filters on."""

    @pytest.mark.asyncio
    async def test_the_kind_gate_is_the_only_condition_without_filters(self):
        conn = _conn()
        conn.fetch = AsyncMock(return_value=[])
        db = _orchestrator_db(conn)
        await db.list_threads()
        sql = _compact(conn.fetch.call_args[0][0])
        assert "FROM threads WHERE kind = 'session' ORDER BY created_at DESC" in sql
        assert conn.fetch.call_args[0][1:] == ()

    @pytest.mark.asyncio
    async def test_the_kind_gate_precedes_every_caller_filter(self):
        conn = _conn()
        conn.fetch = AsyncMock(return_value=[])
        db = _orchestrator_db(conn)
        await db.list_threads(user_id="u1", project_id="p1", status="active")
        sql = _compact(conn.fetch.call_args[0][0])
        assert (
            "WHERE kind = 'session' AND (user_id = $1 OR user_id IS NULL) "
            "AND project_id = $2 AND status = $3" in sql
        )
        assert conn.fetch.call_args[0][1:] == ("u1", "p1", "active")


class TestDeleteJobEndsLiveChildren:
    @pytest.mark.asyncio
    async def test_children_are_ended_in_the_transaction_before_the_delete(self):
        """The cascade fires the pinned delete authority on every child row,
        and only an ended, authority-free row may go — so a child a hard kill
        left 'active' would otherwise abort the whole delete."""
        conn = _conn()
        executed: list[str] = []

        async def execute(sql, *args):
            text = _compact(sql)
            executed.append(text)
            if text.startswith("DELETE FROM jobs"):
                return "DELETE 1"
            return "UPDATE 0"

        conn.execute = AsyncMock(side_effect=execute)

        async def fetchrow(sql, *args):
            text = _compact(sql)
            if text.startswith("SELECT * FROM vm_job_creation_owners"):
                # This ordinary Job has no VM creation history.
                return None
            assert text.startswith("SELECT status, completion_outcome_kind,")
            return {"status": "processing", "completion_outcome_kind": None}

        conn.fetchrow = AsyncMock(side_effect=fetchrow)
        conn.fetchval = AsyncMock(return_value=None)
        db = _orchestrator_db(conn)

        assert await db.delete_job(str(JOB)) is True

        ending = [s for s in executed if s.startswith("UPDATE threads")]
        assert len(ending) == 1
        (end_children,) = ending
        assert "SET status = 'ended'" in end_children
        assert "ended_at = COALESCE(ended_at, CURRENT_TIMESTAMP)" in end_children
        assert "ELSE 'cancelled'" in end_children
        assert (
            "COALESCE( subagent_outcome, 'cancelled:parent_deleted' )" in end_children
        )
        assert (
            "WHERE kind = 'subagent' AND parent_job_id = $1 AND status <> 'ended'"
            in end_children
        )
        assert executed.index(end_children) < executed.index(
            "DELETE FROM jobs WHERE id = $1"
        )
        update_call = next(
            c
            for c in conn.execute.await_args_list
            if _compact(c.args[0]).startswith("UPDATE threads")
        )
        assert update_call.args[1] == JOB


# ---------------------------------------------------------------------------
# Agent side
# ---------------------------------------------------------------------------


class TestAgentParentAuthorityProbe:
    @pytest.mark.asyncio
    async def test_it_checks_only_the_captured_parent_authority(
        self, _exact_parent_authority_gate
    ):
        conn = _conn()
        db = _agent_db(conn)

        assert await db.parent_execution_authority_current(AUTHORITY) is True

        _exact_parent_authority_gate.assert_awaited_once_with(
            conn,
            AUTHORITY,
            parent_job_id=AUTHORITY.parent_job_id,
            mutation=False,
        )
        conn.fetchrow.assert_not_awaited()
        conn.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_settlement_probe_uses_the_preemption_aware_gate(
        self, _exact_parent_authority_gate
    ):
        conn = _conn()
        db = _agent_db(conn)

        assert await db.parent_execution_settlement_authority_current(AUTHORITY) is True

        _exact_parent_authority_gate.assert_not_awaited()
        _exact_parent_authority_gate.settlement_gate.assert_awaited_once_with(
            conn,
            AUTHORITY,
            parent_job_id=AUTHORITY.parent_job_id,
            mutation=False,
        )


class TestAgentSubagentTranscript:
    @pytest.mark.asyncio
    async def test_bulk_seed_locks_generation_then_inserts_in_order(self):
        conn = _conn()
        conn.fetchrow = AsyncMock(
            return_value={"runtime_generation": GENERATION, "status": "active"}
        )
        db = _agent_db(conn)
        messages = [
            {
                "id": "msg_child_ai",
                "role": "ai",
                "content": "calling",
                "tool_calls": [{"name": "read_file", "args": {}, "id": "call_child"}],
                "turn_number": 0,
                "tool_call_id": None,
                "additional_kwargs": {
                    "tool_calls": [{"id": "call_child", "type": "function"}]
                },
                "provider_raw": {
                    "_srw_subagent_fork_seed_v1": {
                        "type": "ai",
                        "data": {"id": "msg_child_ai"},
                    }
                },
            },
            {
                "id": "msg_child_tool",
                "role": "tool",
                "content": "file",
                "turn_number": 0,
                "tool_call_id": "call_child",
            },
        ]

        assert await db.save_subagent_thread_messages(
            str(CHILD),
            parent_job_id=str(JOB),
            parent_authority=AUTHORITY,
            runtime_generation=str(GENERATION),
            messages=messages,
        )

        child_sql = _compact(conn.fetchrow.await_args.args[0])
        assert child_sql.endswith("FOR UPDATE")
        assert conn.fetchrow.await_args.args[1:] == (CHILD, JOB)
        batch_sql, batch_args = conn.executemany.await_args.args
        assert "RETURNING id, seq" not in batch_sql
        assert [row[2] for row in batch_args] == ["ai", "tool"]
        assert json.loads(batch_args[0][4])[0]["id"] == "call_child"
        assert batch_args[1][7] == "call_child"
        assert (
            json.loads(batch_args[0][12])["_srw_subagent_fork_seed_v1"]["data"]["id"]
            == "msg_child_ai"
        )
        assert json.loads(batch_args[0][13])["tool_calls"][0]["id"] == "call_child"
        # Message ids are deterministically represented as UUID PKs; they are
        # not randomly reminted by the persistence boundary.
        assert batch_args[0][0] == agent_postgres._coerce_row_id("msg_child_ai")
        assert batch_args[1][0] == agent_postgres._coerce_row_id("msg_child_tool")

    @pytest.mark.asyncio
    async def test_stale_generation_inserts_none_of_the_seed(self):
        conn = _conn()
        conn.fetchrow = AsyncMock(
            return_value={"runtime_generation": NEXT_GENERATION, "status": "active"}
        )
        db = _agent_db(conn)

        assert not await db.save_subagent_thread_messages(
            str(CHILD),
            parent_job_id=str(JOB),
            parent_authority=AUTHORITY,
            runtime_generation=str(GENERATION),
            messages=[{"id": "m", "role": "human", "content": "seed"}],
        )
        conn.executemany.assert_not_awaited()


class TestUpdateSubagentThread:
    @pytest.mark.asyncio
    async def test_every_field_lands_on_its_column_under_the_kind_guard(self):
        conn = _conn()
        conn.execute = AsyncMock(return_value="UPDATE 1")
        db = _agent_db(conn)

        assert await db.update_subagent_thread(
            str(CHILD),
            runtime_generation=str(GENERATION),
            status="ended",
            subagent_status="capped",
            outcome="capped:turns",
            turns=40,
            tokens=200_000,
            report_path=".subagents/explorer-0001/report.md",
            error=None,
            ended=True,
        )
        sql = _compact(conn.execute.call_args[0][0])
        args = conn.execute.call_args[0][1:]
        assert sql.startswith("UPDATE threads SET")
        assert "status = COALESCE($2::text, status)" in sql
        assert "subagent_status = COALESCE($3::text, subagent_status)" in sql
        assert "subagent_outcome = COALESCE($4::text, subagent_outcome)" in sql
        assert "total_turns = COALESCE($5::integer, total_turns)" in sql
        assert "total_tokens = COALESCE($6::integer, total_tokens)" in sql
        assert "report_path = COALESCE($7::text, report_path)" in sql
        assert "subagent_error = COALESCE($8::text, subagent_error)" in sql
        assert (
            "ended_at = CASE WHEN $9::boolean THEN COALESCE(ended_at, "
            "CURRENT_TIMESTAMP) ELSE ended_at END" in sql
        )
        assert "last_activity = CURRENT_TIMESTAMP" in sql
        assert "runtime_generation = $10::uuid" in sql
        assert "parent_job_id = $11::uuid" in sql
        assert sql.endswith("AND status <> 'ended'")
        assert args == (
            str(CHILD),
            "ended",
            "capped",
            "capped:turns",
            40,
            200_000,
            ".subagents/explorer-0001/report.md",
            None,
            True,
            str(GENERATION),
            str(JOB),
        )

    @pytest.mark.asyncio
    async def test_terminal_update_uses_preemption_settlement_authority(
        self, _exact_parent_authority_gate
    ):
        conn = _conn()
        conn.execute = AsyncMock(return_value="UPDATE 1")
        db = _agent_db(conn)

        assert await db.update_subagent_thread(
            str(CHILD),
            runtime_generation=str(GENERATION),
            status="ended",
            subagent_status="interrupted",
            outcome="interrupted:parent_cancelled",
            ended=True,
        )

        _exact_parent_authority_gate.assert_not_awaited()
        _exact_parent_authority_gate.settlement_gate.assert_awaited_once_with(
            conn,
            AUTHORITY,
            parent_job_id=JOB,
            mutation=True,
        )

    @pytest.mark.asyncio
    async def test_omitted_fields_are_none_so_coalesce_keeps_the_column(self):
        conn = _conn()
        conn.execute = AsyncMock(return_value="UPDATE 1")
        db = _agent_db(conn)
        await db.update_subagent_thread(
            str(CHILD), runtime_generation=str(GENERATION), status="active"
        )
        args = conn.execute.call_args[0][1:]
        assert args == (
            str(CHILD),
            "active",
            None,
            None,
            None,
            None,
            None,
            None,
            False,
            str(GENERATION),
            str(JOB),
        )

    @pytest.mark.asyncio
    async def test_a_session_row_or_unknown_id_reports_false(self):
        conn = _conn()
        conn.execute = AsyncMock(return_value="UPDATE 0")
        db = _agent_db(conn)
        assert not await db.update_subagent_thread(
            str(CHILD), runtime_generation=str(GENERATION), status="ended"
        )

    @pytest.mark.asyncio
    async def test_a_bad_generation_fails_before_sql(self):
        conn = _conn()
        db = _agent_db(conn)
        assert not await db.update_subagent_thread(
            str(CHILD), runtime_generation="not-a-generation", status="active"
        )
        conn.execute.assert_not_awaited()


class TestAgentGetByCall:
    @pytest.mark.asyncio
    async def test_the_lookup_matches_the_orchestrators(self):
        conn = _conn()
        conn.fetchrow = AsyncMock(
            return_value={"id": CHILD, "subagent_status": "completed"}
        )
        db = _agent_db(conn)
        row = await db.get_subagent_thread_by_call(str(JOB), "call-1")
        assert row == {"id": CHILD, "subagent_status": "completed"}
        sql = _compact(conn.fetchrow.call_args[0][0])
        assert (
            "FROM threads WHERE kind = 'subagent' AND parent_job_id = $1::uuid AND "
            "parent_tool_call_id = $2 ORDER BY created_at DESC, id DESC LIMIT 1" in sql
        )
        for column in (
            "runtime_generation",
            "subagent_handle",
            "subagent_type",
            "subagent_status",
            "subagent_outcome",
            "subagent_error",
            "report_path",
            "total_turns",
            "total_tokens",
            "created_at",
            "ended_at",
        ):
            assert column in sql, column
        assert conn.fetchrow.call_args[0][1:] == (str(JOB), "call-1")

    @pytest.mark.asyncio
    async def test_an_empty_key_is_none_without_sql(self):
        conn = _conn()
        db = _agent_db(conn)
        assert await db.get_subagent_thread_by_call(str(JOB), "   ") is None
        assert await db.get_subagent_thread_by_call("", "call-1") is None
        # A non-uuid parent (a test host's "parent-job") is "no row", never a
        # DataError out of the uuid bind — the orchestrator's guard, mirrored.
        assert await db.get_subagent_thread_by_call("parent-job", "call-1") is None
        conn.fetchrow.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_no_row_is_none(self):
        conn = _conn()
        conn.fetchrow = AsyncMock(return_value=None)
        db = _agent_db(conn)
        assert await db.get_subagent_thread_by_call(str(JOB), "call-1") is None


def test_both_pools_read_the_same_columns():
    """A replayed envelope (agent pool) and a cockpit row (orchestrator pool)
    are built from the same facts."""
    assert AgentDB._SUBAGENT_THREAD_COLUMNS == OrchestratorDB._SUBAGENT_THREAD_COLUMNS
    assert uuid4()  # keep the import honest for the fixtures above
