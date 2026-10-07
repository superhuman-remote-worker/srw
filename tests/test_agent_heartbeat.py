"""Tests for orchestrator/database/postgres.py heartbeat handler.

Phase 0 stopgap: an orchestrator-set 'draining' status must survive an
agent-reported status on the next heartbeat. Phase 1 replaces this with
a separate intent column, but in the meantime this guard plus the
'drained' reaper category are what make `_drain_stale_image_agents`
actually delete pods rather than flicker a status field nothing reads.
"""

from contextlib import asynccontextmanager
import json
from unittest.mock import AsyncMock, patch
from uuid import UUID

import pytest

from orchestrator.database.postgres import PostgresDB


def _make_db_with_mocked_acquire(
    prev_status: str,
    update_result: str = "UPDATE 1",
    intents: dict | None = None,
):
    """Build a real PostgresDB with acquire() replaced by a mocked conn.

    The replacement uses an `asynccontextmanager` so the production
    `async with db.acquire() as conn:` shape still works.
    """
    with patch.dict("os.environ", {"DATABASE_URL": "postgresql://test"}):
        db = PostgresDB()
    conn = AsyncMock()
    row = {"status": prev_status, "intents": intents if intents is not None else {}}
    conn.fetchrow = AsyncMock(return_value=row)
    conn.execute = AsyncMock(return_value=update_result)

    @asynccontextmanager
    async def fake_acquire():
        yield conn

    db.acquire = fake_acquire
    return db, conn


class TestHeartbeatPreservesWarmAttachReservation:
    """A mixed-version pool heartbeat cannot erase status=session."""

    @pytest.mark.asyncio
    async def test_bound_session_rejects_ready_heartbeat(self):
        db, conn = _make_db_with_mocked_acquire(prev_status="session")
        conn.fetchrow.return_value["thread_id"] = "22222222-2222-2222-2222-222222222222"

        result = await db.heartbeat(
            agent_id="00000000-0000-0000-0000-000000000001",
            status="ready",
        )

        assert result["effective_status"] == "session"
        sql = conn.execute.await_args.args[0]
        assert "status = 'session' AND thread_id IS NOT NULL" in sql
        assert "= 'ready' THEN 'session'" in sql

    @pytest.mark.asyncio
    async def test_unbound_session_status_can_return_ready(self):
        db, conn = _make_db_with_mocked_acquire(prev_status="session")
        conn.fetchrow.return_value["thread_id"] = None

        result = await db.heartbeat(
            agent_id="00000000-0000-0000-0000-000000000001",
            status="ready",
        )

        assert result["effective_status"] == "ready"


class TestHeartbeatPinnedRuntimeAuthority:
    """A delayed G1 beat cannot mutate a same-agent G2 binding."""

    @staticmethod
    def _bind_row(conn, *, generation: str, attach_token: str | None) -> None:
        conn.fetchrow.return_value.update(
            {
                "thread_id": "22222222-2222-2222-2222-222222222222",
                "execution_lane": "pinned",
                "thread_runtime_generation": UUID(generation),
                "thread_runtime_attach_token": (
                    UUID(attach_token) if attach_token is not None else None
                ),
                "runtime_retirement_token": None,
                "thread_status": "active",
                "thread_metadata": {"protected_cloud": True},
            }
        )

    @pytest.mark.asyncio
    async def test_stale_generation_never_updates_liveness_or_metrics(self):
        db, conn = _make_db_with_mocked_acquire(prev_status="session")
        self._bind_row(
            conn,
            generation="33333333-3333-3333-3333-333333333333",
            attach_token="44444444-4444-4444-4444-444444444444",
        )

        result = await db.heartbeat(
            agent_id="00000000-0000-0000-0000-000000000001",
            status="session",
            metrics={"graph_progress": 9},
            session_runtime_generation="55555555-5555-5555-5555-555555555555",
            session_runtime_attach_token="44444444-4444-4444-4444-444444444444",
        )

        assert result == {
            "authority_refused": True,
            "thread_id": "22222222-2222-2222-2222-222222222222",
        }
        conn.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_exact_identity_is_rechecked_inside_final_update(self):
        db, conn = _make_db_with_mocked_acquire(prev_status="session")
        generation = "33333333-3333-3333-3333-333333333333"
        token = "44444444-4444-4444-4444-444444444444"
        self._bind_row(conn, generation=generation, attach_token=token)

        result = await db.heartbeat(
            agent_id="00000000-0000-0000-0000-000000000001",
            status="session",
            session_runtime_generation=generation,
            session_runtime_attach_token=token,
        )

        assert result is not None and not result.get("authority_refused")
        sql, *params = conn.execute.await_args.args
        assert "thread.runtime_retirement_token IS NULL" in sql
        assert "thread.runtime_generation =" in sql
        assert "thread.runtime_attach_token" in sql
        assert UUID(generation) in params
        assert UUID(token) in params

    @pytest.mark.asyncio
    async def test_protected_runtime_missing_identity_fails_closed(self):
        db, conn = _make_db_with_mocked_acquire(prev_status="session")
        self._bind_row(
            conn,
            generation="33333333-3333-3333-3333-333333333333",
            attach_token=None,
        )

        result = await db.heartbeat(
            agent_id="00000000-0000-0000-0000-000000000001",
            status="session",
        )

        assert result and result.get("authority_refused") is True
        conn.execute.assert_not_awaited()


class TestHeartbeatPreservesDraining:
    """Heartbeat must not let an agent overwrite an orchestrator-set drain."""

    @pytest.mark.asyncio
    async def test_draining_preserved_when_agent_reports_ready(self):
        db, _ = _make_db_with_mocked_acquire(prev_status="draining")
        result = await db.heartbeat(
            agent_id="00000000-0000-0000-0000-000000000001",
            status="ready",
        )
        assert result is not None
        assert result["previous_status"] == "draining"
        assert result["effective_status"] == "draining"

    @pytest.mark.asyncio
    async def test_draining_preserved_when_agent_reports_working(self):
        # Future-proof: even if a working agent's drain bit gets set, the
        # next heartbeat (still reporting working) must not flip it back.
        db, _ = _make_db_with_mocked_acquire(prev_status="draining")
        result = await db.heartbeat(
            agent_id="00000000-0000-0000-0000-000000000001",
            status="working",
            current_job_id="11111111-1111-1111-1111-111111111111",
        )
        assert result["effective_status"] == "draining"

    @pytest.mark.asyncio
    async def test_normal_transition_unaffected(self):
        db, _ = _make_db_with_mocked_acquire(prev_status="working")
        result = await db.heartbeat(
            agent_id="00000000-0000-0000-0000-000000000001",
            status="ready",
        )
        assert result["previous_status"] == "working"
        assert result["effective_status"] == "ready"

    @pytest.mark.asyncio
    async def test_sql_uses_case_expression(self):
        # Belt-and-braces: assert the CASE is in the issued SQL so a
        # future refactor that drops it triggers a test failure rather
        # than a silent regression.
        db, conn = _make_db_with_mocked_acquire(prev_status="working")
        await db.heartbeat(
            agent_id="00000000-0000-0000-0000-000000000001",
            status="ready",
        )
        sql = conn.execute.call_args[0][0]
        assert "WHEN status = 'draining'" in sql

    @pytest.mark.asyncio
    async def test_returns_none_when_agent_missing(self):
        db, conn = _make_db_with_mocked_acquire(prev_status="ready")
        conn.fetchrow.return_value = None
        result = await db.heartbeat(
            agent_id="00000000-0000-0000-0000-000000000001",
            status="ready",
        )
        assert result is None


class TestHeartbeatPreservesOffline:
    """Heartbeat must not let a late grace-period heartbeat resurrect an
    agent that the lifecycle reconciler has already marked offline. See
    the 2026-05-24 stale-pod regression (thread 352144ea): the dying
    pod's last in-flight heartbeat flipped its row back to ready,
    ``_find_idle_persistent_agent`` matched it (most-recent
    ``last_heartbeat`` puts it at the top of the pool query), and the
    new thread got bound to a pod that disappeared seconds later.
    """

    @pytest.mark.asyncio
    async def test_offline_preserved_when_agent_reports_ready(self):
        db, _ = _make_db_with_mocked_acquire(prev_status="offline")
        result = await db.heartbeat(
            agent_id="00000000-0000-0000-0000-000000000001",
            status="ready",
        )
        assert result is not None
        assert result["previous_status"] == "offline"
        assert result["effective_status"] == "offline"

    @pytest.mark.asyncio
    async def test_offline_preserved_when_agent_reports_working(self):
        db, _ = _make_db_with_mocked_acquire(prev_status="offline")
        result = await db.heartbeat(
            agent_id="00000000-0000-0000-0000-000000000001",
            status="working",
            current_job_id="11111111-1111-1111-1111-111111111111",
        )
        assert result["effective_status"] == "offline"

    @pytest.mark.asyncio
    async def test_offline_preserved_when_metrics_present(self):
        # Same guard must apply to the metrics-bearing branch — both SQL
        # paths share the CASE expression that pins offline.
        db, _ = _make_db_with_mocked_acquire(prev_status="offline")
        result = await db.heartbeat(
            agent_id="00000000-0000-0000-0000-000000000001",
            status="ready",
            metrics={"cpu": 0.1},
        )
        assert result["effective_status"] == "offline"

    @pytest.mark.asyncio
    async def test_sql_pins_offline_in_case_expression(self):
        # Belt-and-braces: the issued SQL must include the offline branch
        # of the CASE, so a future refactor that drops it triggers a
        # test failure rather than a silent regression.
        db, conn = _make_db_with_mocked_acquire(prev_status="offline")
        await db.heartbeat(
            agent_id="00000000-0000-0000-0000-000000000001",
            status="ready",
        )
        sql = conn.execute.call_args[0][0]
        assert "WHEN status = 'offline'" in sql


class TestHeartbeatAuxDegraded:
    """aux Phase 2: heartbeat persists the auxiliary-model degraded flag from
    the AuxHealth summary the agent carries in metrics.aux. See
    knowledge-base/knowledge/issues/surface_silent_aux_failures.md.
    """

    @pytest.mark.asyncio
    async def test_true_sets_column(self):
        db, conn = _make_db_with_mocked_acquire(prev_status="ready")
        await db.heartbeat(
            agent_id="00000000-0000-0000-0000-000000000001",
            status="ready",
            aux_degraded=True,
        )
        sql, *params = conn.execute.call_args[0]
        assert "aux_degraded = " in sql
        assert any(p is True for p in params)

    @pytest.mark.asyncio
    async def test_false_clears_column(self):
        # A recovered agent reports degraded=False — the column must be
        # actively set false, not left stale at a previous true.
        db, conn = _make_db_with_mocked_acquire(prev_status="ready")
        await db.heartbeat(
            agent_id="00000000-0000-0000-0000-000000000001",
            status="ready",
            aux_degraded=False,
        )
        sql, *params = conn.execute.call_args[0]
        assert "aux_degraded = " in sql
        assert any(p is False for p in params)

    @pytest.mark.asyncio
    async def test_none_omits_column(self):
        # Older agent builds / not-yet-wired aux LLM omit the flag — the
        # persisted value must be left untouched (no aux_degraded clause).
        db, conn = _make_db_with_mocked_acquire(prev_status="ready")
        await db.heartbeat(
            agent_id="00000000-0000-0000-0000-000000000001",
            status="ready",
            aux_degraded=None,
        )
        sql = conn.execute.call_args[0][0]
        assert "aux_degraded" not in sql

    @pytest.mark.asyncio
    async def test_param_indices_aligned_with_all_clauses_present(self):
        # The dynamic param builder must keep $N indices contiguous and
        # matched to positional args when the metadata merge, the
        # working→ready last_completed_at clause, and aux_degraded all fire.
        # This is the index-shuffling hazard the builder replaced.
        import re

        db, conn = _make_db_with_mocked_acquire(prev_status="working")
        await db.heartbeat(
            agent_id="00000000-0000-0000-0000-000000000001",
            status="ready",
            current_job_id="11111111-1111-1111-1111-111111111111",
            metrics={"aux": {"degraded": True}},
            aux_degraded=True,
        )
        sql, *params = conn.execute.call_args[0]
        placeholders = {int(m) for m in re.findall(r"\$(\d+)", sql)}
        assert placeholders == set(range(1, len(params) + 1))
        assert "metadata = metadata ||" in sql
        assert "last_completed_at = CURRENT_TIMESTAMP" in sql
        assert "aux_degraded = " in sql


class TestHeartbeatGraphProgress:
    """Graph-progress markers in heartbeat metadata are tracked with a timestamp."""

    @pytest.mark.asyncio
    async def test_graph_progress_changes_writes_seen_at_timestamp(self):
        db, conn = _make_db_with_mocked_acquire(
            prev_status="ready",
            intents={},
        )
        conn.fetchrow.return_value["metadata"] = {"graph_progress": 1}

        await db.heartbeat(
            agent_id="00000000-0000-0000-0000-000000000001",
            status="ready",
            metrics={"graph_progress": 2},
        )

        sql, *params = conn.execute.call_args[0]
        # Graph-progress timestamp is encoded in the payload, not SQL text.
        metadata_payloads = [
            json.loads(p)
            for p in params
            if isinstance(p, str) and p.startswith("{") and "graph_progress" in p
        ]
        assert metadata_payloads, "metadata payload missing"
        latest = metadata_payloads[-1]
        assert latest["graph_progress"] == 2
        assert "graph_progress_seen_at" in latest

    @pytest.mark.asyncio
    async def test_graph_progress_unchanged_skips_seen_at_timestamp(self):
        db, conn = _make_db_with_mocked_acquire(
            prev_status="ready",
            intents={},
        )
        conn.fetchrow.return_value["metadata"] = {"graph_progress": 3}

        await db.heartbeat(
            agent_id="00000000-0000-0000-0000-000000000001",
            status="ready",
            metrics={"graph_progress": 3},
        )

        sql, *params = conn.execute.call_args[0]
        # The SQL text stays stable; only the payload differs when progress is
        # unchanged.
        metadata_payloads = [
            json.loads(p)
            for p in params
            if isinstance(p, str) and p.startswith("{") and "graph_progress" in p
        ]
        assert metadata_payloads, "metadata payload missing"
        latest = metadata_payloads[-1]
        assert latest["graph_progress"] == 3
        assert "graph_progress_seen_at" not in latest


class TestHeartbeatRecipientAuthority:
    """Heartbeat metrics cannot overwrite the registration generation."""

    @pytest.mark.asyncio
    async def test_process_generation_is_stripped_from_metadata_merge(self):
        db, conn = _make_db_with_mocked_acquire(prev_status="ready")

        await db.heartbeat(
            agent_id="00000000-0000-0000-0000-000000000001",
            status="ready",
            metrics={
                "dispatch_process_generation": "forged-generation",
                "memory_mb": 128,
            },
        )

        _, *params = conn.execute.call_args[0]
        metadata_payloads = [
            json.loads(value)
            for value in params
            if isinstance(value, str) and value.startswith("{")
        ]
        assert metadata_payloads
        assert metadata_payloads[-1] == {"memory_mb": 128}


class TestHeartbeatReturnsBoundThread:
    """The handler slides a thread-bound runtime-actor grant on liveness, so
    it needs the agent's bound thread. It rides the row heartbeat already
    reads, at zero marginal DB cost.
    knowledge/issues/officer_runtime_grant_expires_after_24h_and_dies_silently.md
    """

    @pytest.mark.asyncio
    async def test_bound_thread_is_selected_and_returned(self):
        db, conn = _make_db_with_mocked_acquire(prev_status="ready")
        conn.fetchrow.return_value["thread_id"] = "22222222-2222-2222-2222-222222222222"
        result = await db.heartbeat(
            agent_id="00000000-0000-0000-0000-000000000001",
            status="ready",
        )
        assert result["thread_id"] == "22222222-2222-2222-2222-222222222222"
        assert "thread_id" in conn.fetchrow.call_args[0][0]

    @pytest.mark.asyncio
    async def test_thread_id_is_none_for_a_stateless_worker_agent(self):
        db, _ = _make_db_with_mocked_acquire(prev_status="ready")
        result = await db.heartbeat(
            agent_id="00000000-0000-0000-0000-000000000001",
            status="ready",
        )
        assert result["thread_id"] is None
        assert result["execution_lane"] is None

    @pytest.mark.asyncio
    async def test_bound_thread_lane_is_returned(self):
        """The handler advertises a pinned session's fan-out switch from it
        (parallel_subagents.md §14.2 P5), off the same row read."""
        db, conn = _make_db_with_mocked_acquire(prev_status="ready")
        conn.fetchrow.return_value.update(
            {
                "thread_id": "22222222-2222-2222-2222-222222222222",
                "execution_lane": "stateless",
            }
        )
        result = await db.heartbeat(
            agent_id="00000000-0000-0000-0000-000000000001",
            status="ready",
        )
        assert result["execution_lane"] == "stateless"
        assert "execution_lane" in conn.fetchrow.call_args[0][0]


class TestHeartbeatReturnsIntents:
    """Phase 1c: heartbeat result includes orchestrator-set intents so
    the FastAPI handler can surface them to the agent in the response."""

    @pytest.mark.asyncio
    async def test_intents_propagate_when_present(self):
        db, _ = _make_db_with_mocked_acquire(
            prev_status="ready",
            intents={"should_drain": True, "drain_reason": "stale_image"},
        )
        result = await db.heartbeat(
            agent_id="00000000-0000-0000-0000-000000000001",
            status="ready",
        )
        assert result["intents"] == {
            "should_drain": True,
            "drain_reason": "stale_image",
        }

    @pytest.mark.asyncio
    async def test_intents_empty_when_unset(self):
        db, _ = _make_db_with_mocked_acquire(prev_status="ready", intents={})
        result = await db.heartbeat(
            agent_id="00000000-0000-0000-0000-000000000001",
            status="ready",
        )
        assert result["intents"] == {}

    @pytest.mark.asyncio
    async def test_intents_parsed_when_db_returns_jsonb_string(self):
        # Some asyncpg paths surface JSONB columns as raw strings.
        db, _ = _make_db_with_mocked_acquire(prev_status="ready")
        # Override the fetchrow to return the string-encoded variant.
        import json

        conn = AsyncMock()
        conn.fetchrow = AsyncMock(
            return_value={
                "status": "ready",
                "intents": json.dumps({"should_drain": True}),
            }
        )
        conn.execute = AsyncMock(return_value="UPDATE 1")

        @asynccontextmanager
        async def fake_acquire():
            yield conn

        db.acquire = fake_acquire
        result = await db.heartbeat(
            agent_id="00000000-0000-0000-0000-000000000001",
            status="ready",
        )
        assert result["intents"] == {"should_drain": True}
