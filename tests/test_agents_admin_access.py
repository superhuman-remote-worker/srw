"""G4 — agents endpoints admin-only + /api/me/active-jobs projection.

Pre-G4 the agents read endpoints exposed the full fleet — including
pod IPs and hostnames — to any approved user. G4 admin-gates them and
adds a stripped-projection endpoint so non-admins can still see what's
running for them, without infrastructure metadata.

* `GET /api/agents`              → admin only
* `GET /api/agents/{id}`         → admin only
* `GET /api/agents/{id}/system-info` → admin only
* `DELETE /api/agents/{id}`      → admin, or X-Internal-Key (agent
  self-deregistration on graceful exit)
* `GET /api/me/active-jobs`      → any approved user; returns caller's
  visible jobs filtered to active statuses (created / processing /
  paused / pending_review). Uses the G1 visibility helpers, so MCP
  project scope still narrows the result.
"""

from contextlib import ExitStack
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException

from orchestrator.database.postgres import JobQueryResult
from orchestrator.application import jobs as jobs_composition
from orchestrator.application import sessions as sessions_composition
import functools


def _patch_caller_and_db(user: dict, db):
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


def _scoped(user: dict, scope: str) -> dict:
    out = dict(user)
    out["scopes"] = [scope]
    out["auth_method"] = "mcp"
    return out


# =============================================================================
# Agents endpoints — admin only
# =============================================================================


class TestAgentsAdminOnly:
    @pytest.mark.asyncio
    async def test_list_agents_non_admin_403(self, user_a, fake_db, fake_request):
        from orchestrator.routers.agent_registration import list_agents
        import orchestrator.main as _orch_main

        fake_request.app.state.agent_registration_dependencies_factory = (
            functools.partial(
                sessions_composition.agent_registration_dependencies,
                _orch_main.app.state.resources,
            )
        )

        fake_db.list_agents = AsyncMock(
            side_effect=AssertionError("list_agents called past gate")
        )
        with _patch_caller_and_db(user_a, fake_db):
            with pytest.raises(HTTPException) as exc:
                await list_agents(fake_request, status=None, limit=100)
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_list_agents_admin_passes(self, user_admin, fake_db, fake_request):
        from orchestrator.routers.agent_registration import list_agents
        import orchestrator.main as _orch_main

        fake_request.app.state.agent_registration_dependencies_factory = (
            functools.partial(
                sessions_composition.agent_registration_dependencies,
                _orch_main.app.state.resources,
            )
        )

        fake_db.list_agents = AsyncMock(
            return_value=[{"id": "a1", "pod_ip": "10.0.0.1"}]
        )
        with _patch_caller_and_db(user_admin, fake_db):
            result = await list_agents(fake_request, status=None, limit=100)
        assert result == [{"id": "a1", "pod_ip": "10.0.0.1"}]

    @pytest.mark.asyncio
    async def test_get_agent_non_admin_403(self, user_a, fake_db, fake_request):
        from orchestrator.routers.agent_registration import get_agent
        import orchestrator.main as _orch_main

        fake_request.app.state.agent_registration_dependencies_factory = (
            functools.partial(
                sessions_composition.agent_registration_dependencies,
                _orch_main.app.state.resources,
            )
        )

        fake_db.get_agent = AsyncMock(
            side_effect=AssertionError("get_agent called past gate")
        )
        with _patch_caller_and_db(user_a, fake_db):
            with pytest.raises(HTTPException) as exc:
                await get_agent(fake_request, "agent-1")
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_get_agent_admin_passes(self, user_admin, fake_db, fake_request):
        from orchestrator.routers.agent_registration import get_agent
        import orchestrator.main as _orch_main

        fake_request.app.state.agent_registration_dependencies_factory = (
            functools.partial(
                sessions_composition.agent_registration_dependencies,
                _orch_main.app.state.resources,
            )
        )

        fake_db.get_agent = AsyncMock(return_value={"id": "agent-1"})
        with _patch_caller_and_db(user_admin, fake_db):
            result = await get_agent(fake_request, "agent-1")
        assert result == {"id": "agent-1"}

    @pytest.mark.asyncio
    async def test_get_agent_system_info_non_admin_403(
        self, user_a, fake_db, fake_request
    ):
        from orchestrator.routers.agent_registration import get_agent_system_info
        import orchestrator.main as _orch_main

        fake_request.app.state.agent_registration_dependencies_factory = (
            functools.partial(
                sessions_composition.agent_registration_dependencies,
                _orch_main.app.state.resources,
            )
        )

        fake_db.get_agent = AsyncMock(
            side_effect=AssertionError("get_agent called past gate")
        )
        with _patch_caller_and_db(user_a, fake_db):
            with pytest.raises(HTTPException) as exc:
                await get_agent_system_info(fake_request, "agent-1")
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_delete_agent_non_admin_403(self, user_a, fake_db, fake_request):
        from orchestrator.routers.agent_registration import delete_agent
        import orchestrator.main as _orch_main

        fake_request.app.state.agent_registration_dependencies_factory = (
            functools.partial(
                sessions_composition.agent_registration_dependencies,
                _orch_main.app.state.resources,
            )
        )

        fake_db.delete_agent = AsyncMock(
            side_effect=AssertionError("delete_agent called past gate")
        )
        with _patch_caller_and_db(user_a, fake_db):
            with pytest.raises(HTTPException) as exc:
                await delete_agent(fake_request, "agent-1")
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_delete_agent_admin_passes(self, user_admin, fake_db, fake_request):
        from orchestrator.routers.agent_registration import delete_agent
        import orchestrator.main as _orch_main

        fake_request.app.state.agent_registration_dependencies_factory = (
            functools.partial(
                sessions_composition.agent_registration_dependencies,
                _orch_main.app.state.resources,
            )
        )

        fake_db.delete_agent = AsyncMock(return_value=True)
        with _patch_caller_and_db(user_admin, fake_db):
            result = await delete_agent(fake_request, "agent-1")
        assert result == {"status": "deleted"}

    @pytest.mark.asyncio
    async def test_delete_agent_internal_key_bypasses_admin_gate(
        self, fake_db, fake_request
    ):
        """An agent deregistering itself on graceful exit carries only
        X-Internal-Key — no user resolves, so admin auth must not run."""
        from orchestrator.routers.agent_registration import delete_agent
        import orchestrator.main as _orch_main

        fake_request.app.state.agent_registration_dependencies_factory = (
            functools.partial(
                sessions_composition.agent_registration_dependencies,
                _orch_main.app.state.resources,
            )
        )

        fake_db.delete_agent = AsyncMock(return_value=True)
        with ExitStack() as stack:
            stack.enter_context(
                patch(
                    "orchestrator.security.access.is_internal_call",
                    lambda request: True,
                )
            )
            stack.enter_context(
                patch(
                    "orchestrator.security.auth.require_approved_user",
                    AsyncMock(side_effect=AssertionError("user auth consulted")),
                )
            )
            stack.enter_context(
                patch("orchestrator.main.app.state.resources.postgres_db", fake_db)
            )
            result = await delete_agent(fake_request, "agent-1")
        assert result == {"status": "deleted"}
        fake_db.delete_agent.assert_awaited_once_with("agent-1")


# =============================================================================
# /api/me/active-jobs — per-user projection
# =============================================================================


class TestListMyActiveJobs:
    @pytest.mark.asyncio
    async def test_non_admin_filters_to_active_statuses(
        self, user_a, fake_db, fake_request
    ):
        import orchestrator.main
        from orchestrator.routers.job_inspection import list_my_active_jobs

        # Mix of in-flight and terminal — gate should drop completed/failed.
        rows = [
            {"id": "j1", "status": "processing"},
            {"id": "j2", "status": "completed"},
            {"id": "j3", "status": "paused"},
            {"id": "j4", "status": "failed"},
            {"id": "j5", "status": "created"},
            {"id": "j6", "status": "pending_review"},
        ]
        fake_db.query_jobs = AsyncMock(return_value=JobQueryResult(jobs=rows))
        with _patch_caller_and_db(user_a, fake_db):
            result = await list_my_active_jobs(
                fake_request,
                limit=100,
                dependencies=jobs_composition.job_inspection_dependencies(
                    orchestrator.main.app.state.resources
                ),
            )
        kept_ids = {r["id"] for r in result}
        assert kept_ids == {"j1", "j3", "j5", "j6"}

    @pytest.mark.asyncio
    async def test_non_admin_uses_visibility_or_clause(
        self, user_a, fake_db, fake_request
    ):
        import orchestrator.main
        from orchestrator.routers.job_inspection import list_my_active_jobs

        fake_db.query_jobs = AsyncMock(return_value=JobQueryResult(jobs=[]))
        with _patch_caller_and_db(user_a, fake_db):
            await list_my_active_jobs(
                fake_request,
                limit=100,
                dependencies=jobs_composition.job_inspection_dependencies(
                    orchestrator.main.app.state.resources
                ),
            )
        kwargs = fake_db.query_jobs.call_args.kwargs
        assert kwargs["owner_user_id"] == str(user_a["id"])
        # user_a owns project_a → that one project_id appears.
        assert len(kwargs["visible_project_ids"]) == 1
        assert kwargs["scope_project_id"] is None

    @pytest.mark.asyncio
    async def test_admin_personal_view_uses_an_owner_filter(
        self, user_admin, fake_db, fake_request
    ):
        """Admin gets their personal active set (not the full fleet) — for that they use /api/agents."""
        import orchestrator.main
        from orchestrator.routers.job_inspection import list_my_active_jobs

        fake_db.query_jobs = AsyncMock(return_value=JobQueryResult(jobs=[]))
        with _patch_caller_and_db(user_admin, fake_db):
            await list_my_active_jobs(
                fake_request,
                limit=100,
                dependencies=jobs_composition.job_inspection_dependencies(
                    orchestrator.main.app.state.resources
                ),
            )
        fake_db.query_jobs.assert_awaited_once()
        kwargs = fake_db.query_jobs.call_args.kwargs
        assert kwargs["user_id"] == str(user_admin["id"])
        # Admin path = no OR-clause; the owner filter alone bounds the view.
        assert kwargs["owner_user_id"] is None

    @pytest.mark.asyncio
    async def test_unauthenticated_baseline(self, fake_db, fake_request):
        import orchestrator.main
        from orchestrator.routers.job_inspection import list_my_active_jobs

        with (
            patch(
                "orchestrator.security.auth.require_approved_user",
                AsyncMock(side_effect=HTTPException(status_code=401)),
            ),
            patch("orchestrator.main.app.state.resources.postgres_db", fake_db),
        ):
            with pytest.raises(HTTPException) as exc:
                await list_my_active_jobs(
                    fake_request,
                    limit=100,
                    dependencies=jobs_composition.job_inspection_dependencies(
                        orchestrator.main.app.state.resources
                    ),
                )
        assert exc.value.status_code == 401

    @pytest.mark.asyncio
    async def test_mcp_project_scope_narrows(
        self, user_a, project_a, fake_db, fake_request
    ):
        import orchestrator.main
        from orchestrator.routers.job_inspection import list_my_active_jobs

        scoped = _scoped(user_a, f"project:{project_a['id']}")
        fake_db.query_jobs = AsyncMock(return_value=JobQueryResult(jobs=[]))
        with _patch_caller_and_db(scoped, fake_db):
            await list_my_active_jobs(
                fake_request,
                limit=100,
                dependencies=jobs_composition.job_inspection_dependencies(
                    orchestrator.main.app.state.resources
                ),
            )
        kwargs = fake_db.query_jobs.call_args.kwargs
        assert kwargs["scope_project_id"] == str(project_a["id"])

    @pytest.mark.asyncio
    async def test_admin_get_agent_short_circuit_with_mcp_scope(
        self, user_admin, project_a, fake_db, fake_request
    ):
        """Admin with MCP scope keeps the admin path but gains scope_project_id."""
        import orchestrator.main
        from orchestrator.routers.job_inspection import list_my_active_jobs

        scoped = _scoped(user_admin, f"project:{project_a['id']}")
        fake_db.query_jobs = AsyncMock(return_value=JobQueryResult(jobs=[]))
        with _patch_caller_and_db(scoped, fake_db):
            await list_my_active_jobs(
                fake_request,
                limit=100,
                dependencies=jobs_composition.job_inspection_dependencies(
                    orchestrator.main.app.state.resources
                ),
            )
        kwargs = fake_db.query_jobs.call_args.kwargs
        assert kwargs["scope_project_id"] == str(project_a["id"])
