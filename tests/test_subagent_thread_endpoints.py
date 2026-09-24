"""The two subagent thread endpoints (U3 WP3, plan B.1 / B.10).

``POST /api/agents/jobs/{job_id}/subagents`` — internal (X-Internal-Key): the
agent-side ledger creates a child's ``threads`` row here; the orchestrator
derives owner and project from the job and provisions nothing.

``GET /api/jobs/{job_id}/subagents`` — the per-job roster, gated by
``require_job_access`` like ``/subjobs`` (whose test module this mirrors):
seeing the job is seeing its children. The authorization tests run the REAL
gate against the shared ``fake_db`` graph (conftest) so the owner / stranger /
unknown-job outcomes are the helper's, not a mock's.
"""

from __future__ import annotations

import importlib
import inspect
import json
import uuid
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import orchestrator.main as m
from orchestrator.routers import agent_child_threads as _child_routes
from orchestrator.routers import job_inspection as job_inspection_routes
from orchestrator.application import jobs as jobs_composition
from orchestrator.application import sessions as sessions_composition
from orchestrator.schemas import agent_child_threads as agent_child_threads_module
from orchestrator.security import access as access_module
from shared import subagent_parent_authority as subagent_parent_authority_module
import fastapi as fastapi_module

UTC = timezone.utc
NOW = datetime(2026, 8, 29, 12, 0, 0, tzinfo=UTC)
GENERATION = uuid.UUID("dddddddd-1111-4222-8333-444444444444")
NEXT_GENERATION = uuid.UUID("eeeeeeee-1111-4222-8333-444444444444")
DELIVERY = uuid.UUID("ffffffff-1111-4222-8333-444444444444")
AGENT = uuid.UUID("99999999-1111-4222-8333-444444444444")


@pytest.fixture(autouse=True)
def _child_thread_routes_resolve_from_main(monkeypatch):
    """R1.B06: these routes moved to ``routers/agent_child_threads``.

    They resolve their collaborators from ``request.app.state``, and the cases
    below hand the handlers bare ``SimpleNamespace()`` / ``MagicMock()``
    requests. Resolving through main's real factory keeps every
    ``monkeypatch.setattr(m, ...)`` below steering exactly what it steered when
    these were main functions — including the internal gate, which is now
    ``dependencies.require_internal`` inside the route body.
    """
    monkeypatch.setattr(
        _child_routes,
        "get_agent_child_thread_dependencies",
        lambda request: sessions_composition.agent_child_threads_dependencies(
            m.app.state.resources
        ),
    )


def _authority(job_id: str):
    return subagent_parent_authority_module.ParentExecutionAuthority(
        execution_lane="pinned",
        parent_job_id=job_id,
        agent_id=AGENT,
        pod_uid="pod-test",
        dispatch_process_generation="process-test",
    )


_DEFAULT_METADATA = object()


def _row(
    *,
    thread_id: str | None = None,
    status: str = "completed",
    outcome: str | None = "completed",
    metadata: Any = _DEFAULT_METADATA,
    **overrides: Any,
) -> dict[str, Any]:
    row = {
        "id": uuid.UUID(thread_id) if thread_id else uuid.uuid4(),
        "kind": "subagent",
        "parent_job_id": uuid.uuid4(),
        "parent_thread_id": None,
        "parent_tool_call_id": "call-1",
        "subagent_handle": "explorer-7f3a",
        "subagent_type": "explorer",
        "subagent_status": status,
        "subagent_outcome": outcome,
        "subagent_error": None,
        "report_path": ".subagents/explorer-7f3a/report.md",
        "status": "ended",
        "title": "explorer-7f3a: find the secret",
        "total_turns": 3,
        "total_tokens": 1200,
        "runtime_generation": GENERATION,
        "metadata": (
            metadata
            if metadata is not _DEFAULT_METADATA
            else {
                "datasource_ids": [],
                "subagent": {
                    "type": "explorer",
                    "handle": "explorer-7f3a",
                    "isolation": "shared",
                    "write_policy": "none",
                    "brief_description": "find the secret",
                    "parent_iteration": 4,
                    "fork": False,
                    "run_in_background": False,
                },
            }
        ),
        "created_at": NOW - timedelta(minutes=5),
        "last_activity": NOW,
        "ended_at": NOW,
    }
    row.update(overrides)
    return row


# ---------------------------------------------------------------------------
# POST /api/agents/jobs/{job_id}/subagents
# ---------------------------------------------------------------------------


@pytest.fixture
def create_env(monkeypatch):
    job_id = str(uuid.uuid4())
    child_id = str(uuid.uuid4())
    db = SimpleNamespace(
        create_subagent_thread=AsyncMock(
            return_value={
                "thread_id": child_id,
                "runtime_generation": str(GENERATION),
            }
        )
    )
    gate = AsyncMock(return_value=None)
    monkeypatch.setattr(m.app.state.resources, "postgres_db", db)
    monkeypatch.setattr(access_module, "require_internal", gate)

    def body(**kw):
        kw.setdefault("parent_authority", _authority(job_id))
        kw.setdefault("handle", "explorer-7f3a")
        kw.setdefault("subagent_type", "explorer")
        return agent_child_threads_module.AgentSubagentThreadCreateRequest(**kw)

    async def call(**kw):
        return await _child_routes.agent_create_subagent_thread(
            SimpleNamespace(), job_id, body(**kw)
        )

    return SimpleNamespace(
        job_id=job_id, child_id=child_id, db=db, gate=gate, body=body, call=call
    )


class TestCreateEndpoint:
    def test_it_is_a_pure_internal_route(self, create_env):
        params = set(
            inspect.signature(_child_routes.agent_create_subagent_thread).parameters
        )
        assert params == {"request", "job_id", "body"}

    @pytest.mark.asyncio
    async def test_the_internal_key_is_checked_before_anything(self, create_env):
        create_env.gate.side_effect = fastapi_module.HTTPException(
            status_code=401, detail="no"
        )
        with pytest.raises(fastapi_module.HTTPException) as excinfo:
            await create_env.call()
        assert excinfo.value.status_code == 401
        create_env.db.create_subagent_thread.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_the_real_gate_fails_closed_without_the_header(self, monkeypatch):
        """``is_internal_call`` compares the header with the configured key
        and returns False when none is configured — a request without the
        header is 401 either way."""
        from orchestrator.security.access import require_internal

        monkeypatch.setattr(access_module, "require_internal", require_internal)
        monkeypatch.setattr(
            m.app.state.resources,
            "postgres_db",
            SimpleNamespace(create_subagent_thread=AsyncMock()),
        )
        request = MagicMock()
        request.headers = {}
        with pytest.raises(fastapi_module.HTTPException) as excinfo:
            await _child_routes.agent_create_subagent_thread(
                request,
                str(uuid.uuid4()),
                agent_child_threads_module.AgentSubagentThreadCreateRequest(
                    parent_authority=_authority(str(uuid.uuid4())),
                    handle="explorer-7f3a",
                    subagent_type="explorer",
                ),
            )
        assert excinfo.value.status_code == 401
        m.app.state.resources.postgres_db.create_subagent_thread.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_every_body_field_reaches_the_accessor_and_the_id_comes_back(
        self, create_env
    ):
        subagent_id = uuid.uuid4()
        payload = await create_env.call(
            subagent_id=subagent_id,
            parent_tool_call_id="call-1",
            isolation="worktree",
            write_policy="owned_paths",
            owned_paths=["src/**"],
            brief_description="implement the parser",
            parent_iteration=12,
            fork=True,
            run_in_background=True,
            initial_status="running",
        )
        assert payload == {
            "thread_id": create_env.child_id,
            "runtime_generation": str(GENERATION),
            "status": "created",
        }
        create_env.db.create_subagent_thread.assert_awaited_once_with(
            parent_job_id=create_env.job_id,
            parent_authority=_authority(create_env.job_id),
            thread_id=str(subagent_id),
            handle="explorer-7f3a",
            subagent_type="explorer",
            parent_tool_call_id="call-1",
            isolation="worktree",
            write_policy="owned_paths",
            owned_paths=["src/**"],
            brief_description="implement the parser",
            parent_iteration=12,
            fork=True,
            run_in_background=True,
            initial_status="running",
        )

    @pytest.mark.asyncio
    async def test_the_defaults_are_the_b1_defaults(self, create_env):
        await create_env.call()
        kwargs = create_env.db.create_subagent_thread.await_args.kwargs
        assert kwargs["thread_id"] is None
        assert kwargs["parent_tool_call_id"] is None
        assert kwargs["isolation"] == "shared"
        assert kwargs["write_policy"] == "none"
        assert kwargs["brief_description"] == ""
        assert kwargs["parent_iteration"] is None
        assert kwargs["fork"] is False
        assert kwargs["run_in_background"] is False
        assert kwargs["initial_status"] == "running"

    @pytest.mark.asyncio
    async def test_queued_is_an_explicit_create_state(self, create_env):
        await create_env.call(initial_status="queued")
        assert (
            create_env.db.create_subagent_thread.await_args.kwargs["initial_status"]
            == "queued"
        )

    def test_the_body_refuses_an_empty_handle_or_type(self, create_env):
        with pytest.raises(ValueError):
            create_env.body(handle="")
        with pytest.raises(ValueError):
            create_env.body(subagent_type="")
        with pytest.raises(ValueError):
            create_env.body(subagent_id="not-a-uuid")

    def test_worker_body_rejects_a_thread_parent(self, create_env):
        with pytest.raises(ValueError):
            create_env.body(parent_thread_id=uuid.uuid4())

    @pytest.mark.asyncio
    async def test_a_missing_job_is_404(self, create_env):
        create_env.db.create_subagent_thread.return_value = None
        with pytest.raises(fastapi_module.HTTPException) as excinfo:
            await create_env.call()
        assert excinfo.value.status_code == 404

    @pytest.mark.asyncio
    async def test_a_value_error_is_400_and_anything_else_500(self, create_env):
        create_env.db.create_subagent_thread.side_effect = ValueError("bad")
        with pytest.raises(fastapi_module.HTTPException) as excinfo:
            await create_env.call()
        assert excinfo.value.status_code == 400
        create_env.db.create_subagent_thread.side_effect = RuntimeError("down")
        with pytest.raises(fastapi_module.HTTPException) as excinfo:
            await create_env.call()
        assert excinfo.value.status_code == 500

    @pytest.mark.asyncio
    async def test_stale_parent_execution_is_a_typed_409(self, create_env):
        create_env.db.create_subagent_thread.side_effect = (
            subagent_parent_authority_module.ParentExecutionAuthorityRefused(
                "pinned_process_not_current"
            )
        )
        with pytest.raises(fastapi_module.HTTPException) as excinfo:
            await create_env.call()
        assert excinfo.value.status_code == 409
        assert excinfo.value.detail == {
            "code": "parent_execution_authority_refused",
            "reason": "pinned_process_not_current",
        }


class TestGenerationEndpoints:
    @pytest.mark.asyncio
    async def test_live_list_and_exact_lookup_publish_the_generation(self, monkeypatch):
        job_id = str(uuid.uuid4())
        child_id = uuid.uuid4()
        row = _row(
            thread_id=str(child_id),
            parent_job_id=job_id,
            status="running",
            outcome=None,
        )
        db = SimpleNamespace(
            list_live_subagent_threads=AsyncMock(return_value=[row]),
            get_subagent_thread=AsyncMock(return_value=row),
        )
        gate = AsyncMock(return_value=None)
        monkeypatch.setattr(m.app.state.resources, "postgres_db", db)
        monkeypatch.setattr(access_module, "require_internal", gate)

        query = agent_child_threads_module.AgentSubagentThreadQueryRequest(
            parent_authority=_authority(job_id)
        )
        live = await _child_routes.agent_list_live_subagent_threads(
            SimpleNamespace(), job_id, query
        )
        exact = await _child_routes.agent_get_subagent_thread(
            SimpleNamespace(), job_id, child_id, query
        )

        assert live["subagents"][0]["runtime_generation"] == str(GENERATION)
        assert live["subagents"][0]["parent_job_id"] == job_id
        assert exact["thread_id"] == str(child_id)
        assert exact["runtime_generation"] == str(GENERATION)
        db.list_live_subagent_threads.assert_awaited_once_with(
            job_id, parent_authority=query.parent_authority
        )
        db.get_subagent_thread.assert_awaited_once_with(
            job_id, str(child_id), parent_authority=query.parent_authority
        )
        assert gate.await_count == 2

    @pytest.mark.asyncio
    async def test_reopen_returns_the_rotated_generation_and_maps_stale_to_409(
        self, monkeypatch
    ):
        job_id = str(uuid.uuid4())
        child_id = uuid.uuid4()
        db = SimpleNamespace(
            reopen_subagent_thread=AsyncMock(
                return_value={
                    "result": "reopened",
                    "thread_id": str(child_id),
                    "runtime_generation": str(NEXT_GENERATION),
                }
            )
        )
        monkeypatch.setattr(m.app.state.resources, "postgres_db", db)
        monkeypatch.setattr(
            access_module, "require_internal", AsyncMock(return_value=None)
        )
        body = agent_child_threads_module.AgentSubagentThreadReopenRequest(
            runtime_generation=GENERATION,
            parent_authority=_authority(job_id),
        )

        result = await _child_routes.agent_reopen_subagent_thread(
            SimpleNamespace(), job_id, child_id, body
        )
        assert result["runtime_generation"] == str(NEXT_GENERATION)
        db.reopen_subagent_thread.assert_awaited_once_with(
            parent_job_id=job_id,
            thread_id=str(child_id),
            runtime_generation=str(GENERATION),
            parent_authority=body.parent_authority,
        )

        db.reopen_subagent_thread.return_value = {
            "result": "stale",
            "thread_id": str(child_id),
            "runtime_generation": str(NEXT_GENERATION),
        }
        with pytest.raises(fastapi_module.HTTPException) as excinfo:
            await _child_routes.agent_reopen_subagent_thread(
                SimpleNamespace(), job_id, child_id, body
            )
        assert excinfo.value.status_code == 409

    @pytest.mark.asyncio
    async def test_terminal_delivery_is_explicit_and_passes_every_field(
        self, monkeypatch
    ):
        job_id = str(uuid.uuid4())
        child_id = uuid.uuid4()
        applied = {
            "result": "applied",
            "thread_id": str(child_id),
            "runtime_generation": str(GENERATION),
            "delivery_id": str(DELIVERY),
            "delivery_state": "queued",
        }
        db = SimpleNamespace(
            terminalize_subagent_thread_and_enqueue=AsyncMock(return_value=applied)
        )
        monkeypatch.setattr(m.app.state.resources, "postgres_db", db)
        monkeypatch.setattr(
            access_module, "require_internal", AsyncMock(return_value=None)
        )
        body = agent_child_threads_module.AgentSubagentThreadTerminalRequest(
            runtime_generation=GENERATION,
            parent_authority=_authority(job_id),
            delivery_id=DELIVERY,
            message="child report",
            timestamp=NOW,
            subagent_status="completed",
            outcome="completed",
            turns=3,
            tokens=1200,
            report_path=".subagents/explorer-7f3a/report.md",
        )

        assert (
            await _child_routes.agent_terminalize_subagent_thread(
                SimpleNamespace(), job_id, child_id, body
            )
            == applied
        )
        db.terminalize_subagent_thread_and_enqueue.assert_awaited_once_with(
            parent_job_id=job_id,
            thread_id=str(child_id),
            runtime_generation=str(GENERATION),
            parent_authority=body.parent_authority,
            delivery_id=str(DELIVERY),
            message="child report",
            timestamp=NOW,
            subagent_status="completed",
            outcome="completed",
            turns=3,
            tokens=1200,
            report_path=".subagents/explorer-7f3a/report.md",
            error=None,
        )

    @pytest.mark.asyncio
    async def test_terminal_stale_is_409_and_invalid_intent_is_400(self, monkeypatch):
        job_id = str(uuid.uuid4())
        child_id = uuid.uuid4()
        db = SimpleNamespace(
            terminalize_subagent_thread_and_enqueue=AsyncMock(
                return_value={"result": "stale"}
            )
        )
        monkeypatch.setattr(m.app.state.resources, "postgres_db", db)
        monkeypatch.setattr(
            access_module, "require_internal", AsyncMock(return_value=None)
        )
        body = agent_child_threads_module.AgentSubagentThreadTerminalRequest(
            runtime_generation=GENERATION,
            parent_authority=_authority(job_id),
            delivery_id=DELIVERY,
            message="child report",
            timestamp=NOW,
            subagent_status="completed",
        )
        with pytest.raises(fastapi_module.HTTPException) as excinfo:
            await _child_routes.agent_terminalize_subagent_thread(
                SimpleNamespace(), job_id, child_id, body
            )
        assert excinfo.value.status_code == 409

        db.terminalize_subagent_thread_and_enqueue.side_effect = ValueError("bad")
        with pytest.raises(fastapi_module.HTTPException) as excinfo:
            await _child_routes.agent_terminalize_subagent_thread(
                SimpleNamespace(), job_id, child_id, body
            )
        assert excinfo.value.status_code == 400


# ---------------------------------------------------------------------------
# GET /api/jobs/{job_id}/subagents
# ---------------------------------------------------------------------------


@pytest.fixture
def roster_env(monkeypatch):
    job_id = str(uuid.uuid4())
    job = {"id": job_id, "status": "processing", "created_at": NOW}
    db = SimpleNamespace(list_subagent_threads=AsyncMock(return_value=[]))
    guard = AsyncMock(return_value=({"id": "u"}, job))
    monkeypatch.setattr(m.app.state.resources, "postgres_db", db)
    monkeypatch.setattr(access_module, "require_job_access", guard)

    async def call():
        return await job_inspection_routes.get_job_subagents(
            SimpleNamespace(),
            job_id,
            dependencies=jobs_composition.job_inspection_dependencies(
                m.app.state.resources
            ),
        )

    return SimpleNamespace(job_id=job_id, db=db, guard=guard, call=call)


class TestRosterShape:
    def test_the_route_takes_no_filter_parameters(self):
        """The roster is a property of the job, not of the caller's view —
        pinned on the signature exactly as the subjobs roster is."""
        params = set(
            inspect.signature(job_inspection_routes.get_job_subagents).parameters
        )
        assert params == {"request", "job_id", "dependencies"}

    @pytest.mark.asyncio
    async def test_it_reads_the_job_walk_not_the_sessions_list(self, roster_env):
        assert not hasattr(roster_env.db, "list_threads")
        await roster_env.call()
        roster_env.db.list_subagent_threads.assert_awaited_once_with(roster_env.job_id)

    @pytest.mark.asyncio
    async def test_a_childless_job_answers_zero(self, roster_env):
        assert await roster_env.call() == {
            "job_id": roster_env.job_id,
            "count": 0,
            "subagents": [],
        }

    @pytest.mark.asyncio
    async def test_the_row_contract(self, roster_env):
        child_id = str(uuid.uuid4())
        roster_env.db.list_subagent_threads.return_value = [
            _row(thread_id=child_id, parent_job_id=roster_env.job_id)
        ]
        payload = await roster_env.call()
        assert payload["count"] == 1
        row = payload["subagents"][0]
        assert row == {
            "thread_id": child_id,
            "parent_job_id": roster_env.job_id,
            "runtime_generation": str(GENERATION),
            "handle": "explorer-7f3a",
            "subagent_type": "explorer",
            "status": "completed",
            "thread_status": "ended",
            "outcome": "completed",
            "error": None,
            "turns": 3,
            "tokens": 1200,
            "report_path": ".subagents/explorer-7f3a/report.md",
            "parent_tool_call_id": "call-1",
            "parent_input_message_id": None,
            "parent_ai_message_id": None,
            "parent_thread_id": None,
            "description": "find the secret",
            "isolation": "shared",
            "write_policy": "none",
            "owned_paths": [],
            "parent_iteration": 4,
            "fork": False,
            "run_in_background": False,
            "started_at": NOW - timedelta(minutes=5),
            "ended_at": NOW,
            "last_activity": NOW,
        }
        assert isinstance(row["thread_id"], str)
        assert "metadata" not in row

    @pytest.mark.asyncio
    async def test_metadata_may_arrive_as_a_json_string(self, roster_env):
        """asyncpg hands JSONB back as text on this pool."""
        raw = _row()
        raw["metadata"] = json.dumps(raw["metadata"])
        roster_env.db.list_subagent_threads.return_value = [raw]
        row = (await roster_env.call())["subagents"][0]
        assert row["description"] == "find the secret"
        assert row["isolation"] == "shared"

    @pytest.mark.asyncio
    async def test_broken_or_absent_metadata_degrades_to_empty_spawn_facts(
        self, roster_env
    ):
        roster_env.db.list_subagent_threads.return_value = [
            _row(metadata="{not json"),
            _row(metadata=None),
            _row(metadata={"subagent": "nope"}),
        ]
        for row in (await roster_env.call())["subagents"]:
            assert row["description"] == ""
            assert row["isolation"] is None and row["write_policy"] is None
            assert row["parent_iteration"] is None and row["fork"] is False

    @pytest.mark.asyncio
    async def test_a_running_child_and_a_failed_one_are_both_listed(self, roster_env):
        parent_thread = uuid.uuid4()
        roster_env.db.list_subagent_threads.return_value = [
            _row(
                status="running",
                outcome=None,
                ended_at=None,
                total_turns=None,
                total_tokens=None,
                report_path=None,
            ),
            _row(
                status="error",
                outcome="error",
                subagent_error="boom",
                parent_thread_id=parent_thread,
            ),
        ]
        rows = (await roster_env.call())["subagents"]
        assert [r["status"] for r in rows] == ["running", "error"]
        assert rows[0]["turns"] == 0 and rows[0]["tokens"] == 0
        assert rows[0]["ended_at"] is None and rows[0]["report_path"] is None
        assert rows[1]["error"] == "boom"
        assert rows[1]["parent_thread_id"] == str(parent_thread)

    @pytest.mark.asyncio
    async def test_a_broken_walk_is_a_500_not_an_empty_roster(self, roster_env):
        roster_env.db.list_subagent_threads.side_effect = RuntimeError("lost")
        with pytest.raises(fastapi_module.HTTPException) as excinfo:
            await roster_env.call()
        assert excinfo.value.status_code == 500


class TestRosterAuthorization:
    """Through the REAL ``require_job_access`` on the conftest graph."""

    async def _run(self, user, db, job_id):
        # Patch the auth resolver in the module the gate actually lives in:
        # ``security.access`` and ``orchestrator.security.access`` are two
        # module objects for one file under pytest's dual sys.path roots,
        # and which one ``orchestrator.main`` bound is an import-order fact.
        # Awaited INSIDE the patches — a returned coroutine would run after
        # they were undone.
        gate_module = importlib.import_module(
            access_module.require_job_access.__module__
        )
        with (
            patch.object(
                gate_module, "require_approved_user", AsyncMock(return_value=user)
            ),
            patch.object(m.app.state.resources, "postgres_db", db),
        ):
            return await job_inspection_routes.get_job_subagents(
                MagicMock(),
                job_id,
                dependencies=jobs_composition.job_inspection_dependencies(
                    m.app.state.resources
                ),
            )

    @pytest.mark.asyncio
    async def test_the_job_owner_reads_the_roster(self, user_a, job_a, fake_db):
        fake_db.list_subagent_threads = AsyncMock(return_value=[_row()])
        payload = await self._run(user_a, fake_db, str(job_a["id"]))
        assert payload["count"] == 1
        fake_db.list_subagent_threads.assert_awaited_once_with(str(job_a["id"]))

    @pytest.mark.asyncio
    async def test_a_stranger_is_403_and_never_reaches_the_walk(
        self, user_b, job_a, fake_db
    ):
        fake_db.list_subagent_threads = AsyncMock(return_value=[_row()])
        with pytest.raises(fastapi_module.HTTPException) as excinfo:
            await self._run(user_b, fake_db, str(job_a["id"]))
        assert excinfo.value.status_code == 403
        fake_db.list_subagent_threads.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_project_member_reads_it_like_the_owner(
        self, user_b, job_a, fake_db
    ):
        """Seeing the job is seeing its children — the project rule of
        ``require_job_access``, not a new one."""
        fake_db.get_user_role_in_project = AsyncMock(return_value="member")
        fake_db.list_subagent_threads = AsyncMock(return_value=[])
        payload = await self._run(user_b, fake_db, str(job_a["id"]))
        assert payload["count"] == 0

    @pytest.mark.asyncio
    async def test_an_unknown_job_is_404(self, user_a, fake_db):
        fake_db.list_subagent_threads = AsyncMock(return_value=[])
        with pytest.raises(fastapi_module.HTTPException) as excinfo:
            await self._run(user_a, fake_db, str(uuid.uuid4()))
        assert excinfo.value.status_code == 404
        fake_db.list_subagent_threads.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_an_admin_reads_any_job(self, user_admin, job_b, fake_db):
        fake_db.list_subagent_threads = AsyncMock(return_value=[])
        payload = await self._run(user_admin, fake_db, str(job_b["id"]))
        assert payload["job_id"] == str(job_b["id"])
