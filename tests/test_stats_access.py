"""G5 — stats endpoints scoped to caller's visibility.

Pre-G5 the 5 stats endpoints returned system-wide aggregates over the
entire `jobs` table (and snapshot storage / agent fleet) to any
approved user. G5:

* `GET /api/stats/jobs`       → visibility-scoped (admin: full; non-admin: OR-clause)
* `GET /api/stats/daily`      → visibility-scoped
* `GET /api/stats/stuck`      → visibility-scoped
* `GET /api/stats/agents`     → admin only (fleet infra)
* `GET /api/snapshots/stats`  → admin only (storage infra)

The underlying postgres methods (`get_job_statistics`,
`get_daily_statistics`, `detect_stuck_jobs`) take optional
``owner_user_id`` / ``visible_project_ids`` / ``scope_project_id``
kwargs that compose into the same OR-clause G1's `query_jobs`
uses.
"""

from contextlib import ExitStack
import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from orchestrator.routers import usage_reporting as usage_reporting_routes
from orchestrator.security.access import (
    log_security_event,
    mcp_scope_project_id,
    require_admin as require_admin_gate,
    user_visible_project_ids,
)
from orchestrator.services import usage_reporting as usage_reporting_service
from orchestrator.application import jobs as jobs_composition
from orchestrator.application import workspace as workspace_composition


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


def _usage_dependencies(
    user: dict, db
) -> usage_reporting_routes.UsageReportingDependencies:
    """Compose the stats router's ports over the fixture graph.

    The visibility ports are the real ones bound to the fake store, because the
    behavior under test is exactly which narrowing kwargs reach postgres — a
    stubbed resolver would assert the stub.
    """
    resolve_user = AsyncMock(return_value=user)

    async def require_admin(request):
        return await require_admin_gate(
            request, db, resolve_user=resolve_user, audit=log_security_event
        )

    return usage_reporting_routes.UsageReportingDependencies(
        store=db,
        reports=usage_reporting_service.UsageReportingDependencies(
            store=db,
            audit_reader=SimpleNamespace(is_available=False),
            logger=logging.getLogger("test-stats-access"),
            visible_project_ids=lambda actor: user_visible_project_ids(actor, db),
            scope_project_id=mcp_scope_project_id,
        ),
        require_admin=require_admin,
        metering_settings=SimpleNamespace(v2_reads_enabled=False),
        require_approved_user=resolve_user,
    )


def _scoped(user: dict, scope: str) -> dict:
    out = dict(user)
    out["scopes"] = [scope]
    out["auth_method"] = "mcp"
    return out


# =============================================================================
# /api/stats/jobs — visibility-scoped
# =============================================================================


#: Calling a FastAPI handler directly leaves unpassed parameters as ``Query``
#: objects rather than their defaults, so tests supply the full signature.
_STATS_JOBS_DEFAULTS: dict = {
    "origin": None,
    "project_id": None,
    "has_project": None,
    "include_archived_projects": False,
    "search": None,
    "as_of": None,
}


async def _stats_jobs(fake_request, **overrides):
    import orchestrator.main
    from orchestrator.routers.job_reads import get_job_statistics

    return await get_job_statistics(
        fake_request,
        **{**_STATS_JOBS_DEFAULTS, **overrides},
        dependencies=jobs_composition.job_reads_dependencies(
            orchestrator.main.app.state.resources
        ),
    )


#: Visibility narrowing keys. An admin without an MCP scope must carry none of
#: them; that is what makes it the full-fleet view.
_VISIBILITY_KEYS = {"owner_user_id", "visible_project_ids", "scope_project_id"}


class TestStatsJobs:
    @pytest.mark.asyncio
    async def test_non_admin_passes_visibility_args(
        self, user_a, fake_db, fake_request
    ):
        fake_db.get_job_statistics = AsyncMock(return_value={"total_jobs": 3})
        with _patch_caller_and_db(user_a, fake_db):
            result = await _stats_jobs(fake_request)
        kwargs = fake_db.get_job_statistics.call_args.kwargs
        assert kwargs["owner_user_id"] == str(user_a["id"])
        assert len(kwargs["visible_project_ids"]) == 1  # owns project_a
        assert kwargs["scope_project_id"] is None
        assert result == {"total_jobs": 3}

    @pytest.mark.asyncio
    async def test_admin_no_visibility_args(self, user_admin, fake_db, fake_request):
        fake_db.get_job_statistics = AsyncMock(return_value={})
        with _patch_caller_and_db(user_admin, fake_db):
            await _stats_jobs(fake_request)
        kwargs = fake_db.get_job_statistics.call_args.kwargs
        # No narrowing key at all — filters may be present, visibility must not.
        assert _VISIBILITY_KEYS & kwargs.keys() == set()

    @pytest.mark.asyncio
    async def test_admin_with_mcp_project_scope(
        self, user_admin, project_a, fake_db, fake_request
    ):
        scoped = _scoped(user_admin, f"project:{project_a['id']}")
        fake_db.get_job_statistics = AsyncMock(return_value={})
        with _patch_caller_and_db(scoped, fake_db):
            await _stats_jobs(fake_request)
        kwargs = fake_db.get_job_statistics.call_args.kwargs
        assert _VISIBILITY_KEYS & kwargs.keys() == {"scope_project_id"}
        assert kwargs["scope_project_id"] == str(project_a["id"])

    @pytest.mark.asyncio
    async def test_non_admin_with_mcp_scope(
        self, user_a, project_a, fake_db, fake_request
    ):
        scoped = _scoped(user_a, f"project:{project_a['id']}")
        fake_db.get_job_statistics = AsyncMock(return_value={})
        with _patch_caller_and_db(scoped, fake_db):
            await _stats_jobs(fake_request)
        kwargs = fake_db.get_job_statistics.call_args.kwargs
        assert kwargs["scope_project_id"] == str(project_a["id"])

    @pytest.mark.asyncio
    async def test_status_is_never_a_parameter(self, user_a, fake_db, fake_request):
        """Disjunctive faceting: the status selection must not narrow the
        counts, or selecting one status drops every other chip to zero."""
        import inspect

        from orchestrator.routers.job_reads import get_job_statistics

        assert "status" not in inspect.signature(get_job_statistics).parameters
        fake_db.get_job_statistics = AsyncMock(return_value={})
        with _patch_caller_and_db(user_a, fake_db):
            await _stats_jobs(fake_request)
        assert "statuses" not in fake_db.get_job_statistics.call_args.kwargs

    @pytest.mark.asyncio
    async def test_list_filters_reach_the_counts(
        self, user_a, project_a, fake_db, fake_request
    ):
        """Chips must summarise the set the list is paging, so every other
        filter — including the archived default and the watermark — applies."""
        from datetime import datetime, timezone

        pinned = datetime(2026, 8, 20, 12, 0, tzinfo=timezone.utc)
        fake_db.get_job_statistics = AsyncMock(return_value={})
        with _patch_caller_and_db(user_a, fake_db):
            await _stats_jobs(
                fake_request,
                project_id=[str(project_a["id"])],
                search="abc",
                as_of=pinned,
            )
        kwargs = fake_db.get_job_statistics.call_args.kwargs
        assert kwargs["project_ids"] == [str(project_a["id"])]
        assert kwargs["search"] == "abc"
        assert kwargs["as_of"] == pinned
        assert kwargs["include_archived_projects"] is False

    @pytest.mark.asyncio
    async def test_invisible_project_filter_is_403(
        self, user_a, project_b, fake_db, fake_request
    ):
        """The counts endpoint must refuse exactly what the list refuses, or
        the two disagree in a way neither endpoint's own tests would catch."""
        fake_db.get_job_statistics = AsyncMock(return_value={})
        with _patch_caller_and_db(user_a, fake_db):
            with pytest.raises(HTTPException) as exc:
                await _stats_jobs(fake_request, project_id=[str(project_b["id"])])
        assert exc.value.status_code == 403
        fake_db.get_job_statistics.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_unauthenticated_baseline(self, fake_db, fake_request):
        import orchestrator.main
        from orchestrator.routers.job_reads import get_job_statistics

        with (
            patch(
                "orchestrator.security.auth.require_approved_user",
                AsyncMock(side_effect=HTTPException(status_code=401)),
            ),
            patch("orchestrator.main.app.state.resources.postgres_db", fake_db),
        ):
            with pytest.raises(HTTPException) as exc:
                await get_job_statistics(
                    fake_request,
                    **_STATS_JOBS_DEFAULTS,
                    dependencies=jobs_composition.job_reads_dependencies(
                        orchestrator.main.app.state.resources
                    ),
                )
        assert exc.value.status_code == 401


# =============================================================================
# /api/stats/daily — same shape
# =============================================================================


class TestStatsDaily:
    @pytest.mark.asyncio
    async def test_non_admin_passes_visibility(self, user_a, fake_db, fake_request):
        fake_db.get_daily_statistics = AsyncMock(return_value=[])
        await usage_reporting_routes.get_daily_statistics(
            fake_request, days=7, dependencies=_usage_dependencies(user_a, fake_db)
        )
        kwargs = fake_db.get_daily_statistics.call_args.kwargs
        assert kwargs["days"] == 7
        assert kwargs["owner_user_id"] == str(user_a["id"])

    @pytest.mark.asyncio
    async def test_admin_no_visibility_args(self, user_admin, fake_db, fake_request):
        fake_db.get_daily_statistics = AsyncMock(return_value=[])
        await usage_reporting_routes.get_daily_statistics(
            fake_request, days=30, dependencies=_usage_dependencies(user_admin, fake_db)
        )
        kwargs = fake_db.get_daily_statistics.call_args.kwargs
        assert kwargs == {"days": 30}


# =============================================================================
# /api/stats/stuck — same shape
# =============================================================================


class TestStatsStuck:
    # E3 (officer_supervision_surface §5): the route now lists processing
    # jobs (get_processing_jobs, no updated_at filter) and derives stuckness
    # from the shared liveness computation; visibility kwargs are unchanged.

    @pytest.mark.asyncio
    async def test_non_admin_passes_visibility(self, user_a, fake_db, fake_request):
        fake_db.get_processing_jobs = AsyncMock(return_value=[])
        await usage_reporting_routes.get_stuck_jobs(
            fake_request,
            threshold_minutes=60,
            dependencies=_usage_dependencies(user_a, fake_db),
        )
        kwargs = fake_db.get_processing_jobs.call_args.kwargs
        assert kwargs["owner_user_id"] == str(user_a["id"])

    @pytest.mark.asyncio
    async def test_admin_no_visibility_args(self, user_admin, fake_db, fake_request):
        fake_db.get_processing_jobs = AsyncMock(return_value=[])
        await usage_reporting_routes.get_stuck_jobs(
            fake_request,
            threshold_minutes=60,
            dependencies=_usage_dependencies(user_admin, fake_db),
        )
        kwargs = fake_db.get_processing_jobs.call_args.kwargs
        assert "owner_user_id" not in kwargs

    @pytest.mark.asyncio
    async def test_default_threshold_is_server_owned_and_reported(
        self, user_a, fake_db, fake_request, monkeypatch
    ):
        monkeypatch.setenv("JOB_LIVENESS_STALL_MINUTES", "47")
        fake_db.get_processing_jobs = AsyncMock(return_value=[])
        result = await usage_reporting_routes.get_stuck_jobs(
            fake_request,
            threshold_minutes=None,
            dependencies=_usage_dependencies(user_a, fake_db),
        )
        assert result == {
            "jobs": [],
            "threshold_minutes": 47,
            "threshold_source": "deployment_default",
        }

    @pytest.mark.asyncio
    @pytest.mark.parametrize("threshold", [30, 60])
    async def test_explicit_override_is_reported(
        self, threshold, user_a, fake_db, fake_request
    ):
        fake_db.get_processing_jobs = AsyncMock(return_value=[])
        result = await usage_reporting_routes.get_stuck_jobs(
            fake_request,
            threshold_minutes=threshold,
            dependencies=_usage_dependencies(user_a, fake_db),
        )
        assert result["threshold_minutes"] == threshold
        assert result["threshold_source"] == "request_override"


# =============================================================================
# /api/stats/agents + /api/snapshots/stats — admin only
# =============================================================================


class TestStatsAdminOnly:
    @pytest.mark.asyncio
    async def test_agents_stats_non_admin_403(self, user_a, fake_db, fake_request):
        fake_db.list_agents = AsyncMock(
            side_effect=AssertionError("list_agents called past gate")
        )
        with pytest.raises(HTTPException) as exc:
            await usage_reporting_routes.get_agent_statistics(
                fake_request, dependencies=_usage_dependencies(user_a, fake_db)
            )
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_agents_stats_admin_passes(self, user_admin, fake_db, fake_request):
        fake_db.list_agents = AsyncMock(
            return_value=[{"status": "ready"}, {"status": "working"}]
        )
        result = await usage_reporting_routes.get_agent_statistics(
            fake_request, dependencies=_usage_dependencies(user_admin, fake_db)
        )
        assert result["total"] == 2
        assert result["ready"] == 1
        assert result["working"] == 1

    @pytest.mark.asyncio
    async def test_snapshot_stats_non_admin_403(self, user_a, fake_db, fake_request):
        import orchestrator.main
        from orchestrator.routers.workspace_access import get_snapshot_stats

        sentinel = MagicMock(side_effect=AssertionError("snapshot called past gate"))
        with (
            _patch_caller_and_db(user_a, fake_db),
            patch("orchestrator.services.snapshot_service.snapshot_service", sentinel),
        ):
            with pytest.raises(HTTPException) as exc:
                await get_snapshot_stats(
                    fake_request,
                    dependencies=workspace_composition.workspace_access_dependencies(
                        orchestrator.main.app.state.resources
                    ),
                )
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_snapshot_stats_admin_passes(self, user_admin, fake_db, fake_request):
        import orchestrator.main
        from orchestrator.routers.workspace_access import get_snapshot_stats

        fake_svc = MagicMock()
        fake_svc.get_storage_stats = AsyncMock(return_value={"total_bytes": 0})
        with (
            _patch_caller_and_db(user_admin, fake_db),
            patch("orchestrator.services.snapshot_service.snapshot_service", fake_svc),
        ):
            result = await get_snapshot_stats(
                fake_request,
                dependencies=workspace_composition.workspace_access_dependencies(
                    orchestrator.main.app.state.resources
                ),
            )
        assert result == {"total_bytes": 0}


# =============================================================================
# Postgres helper: _visibility_clause — direct unit coverage
# =============================================================================


class TestVisibilityClauseHelper:
    def test_no_visibility_args_returns_empty(self):
        from orchestrator.database.postgres import PostgresDB

        db = PostgresDB.__new__(PostgresDB)
        frag, vals, idx = db._visibility_clause(
            owner_user_id=None,
            visible_project_ids=None,
            scope_project_id=None,
        )
        assert frag == ""
        assert vals == []
        assert idx == 1

    def test_owner_only_emits_or_clause(self, user_a):
        from orchestrator.database.postgres import PostgresDB

        db = PostgresDB.__new__(PostgresDB)
        frag, vals, idx = db._visibility_clause(
            owner_user_id=str(user_a["id"]),
            visible_project_ids=[],
            scope_project_id=None,
        )
        assert "user_id = $1" in frag
        assert "ANY($2::uuid[])" in frag
        assert len(vals) == 2
        assert idx == 3

    def test_scope_appends_and_clause(self, user_a, project_a):
        from orchestrator.database.postgres import PostgresDB

        db = PostgresDB.__new__(PostgresDB)
        frag, vals, _idx = db._visibility_clause(
            owner_user_id=str(user_a["id"]),
            visible_project_ids=[],
            scope_project_id=str(project_a["id"]),
        )
        assert frag.count("AND") == 1  # one AND between the two clauses
        assert "project_id = $3" in frag
        assert len(vals) == 3

    def test_table_alias_prefix(self, user_a):
        from orchestrator.database.postgres import PostgresDB

        db = PostgresDB.__new__(PostgresDB)
        frag, _vals, _idx = db._visibility_clause(
            owner_user_id=str(user_a["id"]),
            visible_project_ids=[],
            scope_project_id=None,
            table_alias="j",
        )
        assert "j.user_id" in frag
        assert "j.project_id" in frag
