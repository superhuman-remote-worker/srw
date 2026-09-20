"""G1 — multi-tenancy gates on the jobs read family.

Covers the 30 read endpoints under ``/api/jobs`` and ``/api/jobs/{id}/...``
that G1 brings under the visibility model:

* ``list_jobs`` (admin-only cross-user filter; visibility OR for non-admins;
  MCP project scope narrowing)
* ``get_job`` (owner / project-member / admin)
* The get-by-id read endpoints (audit, llm-requests, citations, memories,
  todos, repo, bulk caches, etc.) — every one calls
  ``require_job_access`` before doing any work.

Tests share the 3-user / 2-project fixture from ``conftest.py``. The
``_patch_caller_and_db`` helper mirrors the F2-F6 pattern of replacing
both ``main.require_approved_user`` AND ``security.access.require_approved_user``
(handlers call the main copy; ``require_job_access`` itself calls the
access-module copy).
"""

from contextlib import ExitStack
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException


# =============================================================================
# Patch helpers
# =============================================================================


def _patch_caller_and_db(user: dict, db):
    """Stack the patches every endpoint test needs."""
    stack = ExitStack()
    stack.enter_context(
        patch("orchestrator.main.require_approved_user", AsyncMock(return_value=user))
    )
    stack.enter_context(
        patch(
            "orchestrator.security.access.require_approved_user",
            AsyncMock(return_value=user),
        )
    )
    stack.enter_context(patch("orchestrator.main.postgres_db", db))
    return stack


def _point_at_messaging_factories(fake_request):
    """Point the extracted ``/api/jobs/{id}/messages*`` routes at the
    application's own dependency objects.

    ``orchestrator.routers.messaging`` resolves its collaborators through
    ``request.app.state.*_dependencies_factory``. Each factory is evaluated per
    call, so it picks up the ``main.postgres_db`` patch installed by
    ``_patch_caller_and_db`` — which is also the store the route's
    ``require_job_access`` gate runs against.
    """
    from orchestrator.main import (
        _inbound_reply_dependencies,
        _message_thread_read_dependencies,
    )

    state = fake_request.app.state
    state.message_thread_read_dependencies_factory = (
        lambda: _message_thread_read_dependencies()
    )
    state.inbound_reply_dependencies_factory = lambda: _inbound_reply_dependencies()
    return fake_request


def _patch_audit_unavailable():
    """Make ``main.audit_reader.is_available`` False so list_jobs skips enrichment."""
    fake_reader = MagicMock()
    fake_reader.is_available = False
    return patch("orchestrator.main.audit_reader", fake_reader)


def _scoped(user: dict, scope: str) -> dict:
    """Return a copy of ``user`` carrying the given MCP scope string."""
    out = dict(user)
    out["scopes"] = [scope]
    out["auth_method"] = "mcp"
    return out


# =============================================================================
# list_jobs — visibility model + admin-only cross-user filter
# =============================================================================


#: Calling a FastAPI handler directly leaves unpassed parameters as ``Query``
#: objects rather than their defaults, so every test has to supply the full
#: signature. Centralising it here means adding a filter is a one-line change
#: instead of an edit to every case.
_LIST_JOBS_DEFAULTS: dict = {
    "status": None,
    "origin": None,
    "project_id": None,
    "has_project": None,
    "include_archived_projects": False,
    "search": None,
    "as_of": None,
    "user_id": None,
    "limit": 100,
    "offset": 0,
    "include_total": True,
}


async def _list_jobs(fake_request, **overrides):
    from orchestrator.main import list_jobs

    return await list_jobs(fake_request, **{**_LIST_JOBS_DEFAULTS, **overrides})


def _result(jobs=None, **kwargs):
    from orchestrator.database.postgres import JobQueryResult

    return JobQueryResult(jobs=list(jobs or []), **kwargs)


class TestListJobs:
    """The single ``query_jobs`` call must carry the caller's visibility.

    Before the fold these tests asserted *which* of two helpers ran, which
    only ever proved a branch was taken. The helper choice was the mechanism;
    the visibility kwargs are the contract, so that is what they pin now —
    ``owner_user_id is None`` means "admin, full fleet" and a set
    ``owner_user_id`` means "emit the G1 OR-clause".
    """

    @pytest.mark.asyncio
    async def test_non_admin_gets_the_visibility_or_clause(
        self, user_a, fake_db, fake_request
    ):
        """Caller's own + project-member jobs via OR-clause."""
        fake_db.query_jobs = AsyncMock(return_value=_result())
        with _patch_caller_and_db(user_a, fake_db), _patch_audit_unavailable():
            await _list_jobs(fake_request)

        fake_db.query_jobs.assert_awaited_once()
        kwargs = fake_db.query_jobs.call_args.kwargs
        assert kwargs["owner_user_id"] == str(user_a["id"])
        # user_a is the owner of project_a → that one project_id appears.
        assert len(kwargs["visible_project_ids"]) == 1
        assert kwargs["scope_project_id"] is None

    @pytest.mark.asyncio
    async def test_non_admin_with_empty_membership_still_sees_own_jobs(
        self, user_a, fake_db, fake_request
    ):
        """Empty visible_project_ids → OR-clause still returns own-user jobs."""
        # Strip user_a's project memberships.
        fake_db.get_projects_for_user = AsyncMock(return_value=[])
        fake_db.query_jobs = AsyncMock(return_value=_result())
        with _patch_caller_and_db(user_a, fake_db), _patch_audit_unavailable():
            await _list_jobs(fake_request)

        kwargs = fake_db.query_jobs.call_args.kwargs
        assert kwargs["owner_user_id"] == str(user_a["id"])
        assert kwargs["visible_project_ids"] == []

    @pytest.mark.asyncio
    async def test_admin_gets_no_visibility_clause(
        self, user_admin, fake_db, fake_request
    ):
        """``owner_user_id=None`` is what makes it the full-fleet view."""
        fake_db.query_jobs = AsyncMock(return_value=_result())
        with _patch_caller_and_db(user_admin, fake_db), _patch_audit_unavailable():
            await _list_jobs(fake_request)

        fake_db.query_jobs.assert_awaited_once()
        kwargs = fake_db.query_jobs.call_args.kwargs
        assert kwargs["owner_user_id"] is None
        assert kwargs["visible_project_ids"] is None
        assert kwargs["user_id"] is None
        assert kwargs["scope_project_id"] is None

    @pytest.mark.asyncio
    async def test_admin_cross_user_query_passes_user_id_through(
        self, user_admin, user_a, fake_db, fake_request
    ):
        fake_db.query_jobs = AsyncMock(return_value=_result())
        with _patch_caller_and_db(user_admin, fake_db), _patch_audit_unavailable():
            await _list_jobs(fake_request, user_id=str(user_a["id"]))

        kwargs = fake_db.query_jobs.call_args.kwargs
        assert kwargs["user_id"] == str(user_a["id"])
        assert kwargs["owner_user_id"] is None

    @pytest.mark.asyncio
    async def test_non_admin_cross_user_query_403(
        self, user_a, user_b, fake_db, fake_request
    ):
        with _patch_caller_and_db(user_a, fake_db), _patch_audit_unavailable():
            with pytest.raises(HTTPException) as exc:
                await _list_jobs(fake_request, user_id=str(user_b["id"]))
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_non_admin_self_query_is_accepted_but_not_forwarded(
        self, user_a, fake_db, fake_request
    ):
        """``?user_id=<self>`` must not reach the query as an owner filter.

        AND-ing it onto the OR-clause would narrow the result to
        own-jobs-only and silently drop the caller's project rows — a
        read *regression* dressed up as a redundant filter.
        """
        fake_db.query_jobs = AsyncMock(return_value=_result())
        with _patch_caller_and_db(user_a, fake_db), _patch_audit_unavailable():
            await _list_jobs(fake_request, user_id=str(user_a["id"]))

        fake_db.query_jobs.assert_awaited_once()
        kwargs = fake_db.query_jobs.call_args.kwargs
        assert kwargs["user_id"] is None
        assert kwargs["owner_user_id"] == str(user_a["id"])

    @pytest.mark.asyncio
    async def test_status_filter_passes_through(self, user_a, fake_db, fake_request):
        fake_db.query_jobs = AsyncMock(return_value=_result())
        with _patch_caller_and_db(user_a, fake_db), _patch_audit_unavailable():
            await _list_jobs(fake_request, status=["completed"], limit=50)

        kwargs = fake_db.query_jobs.call_args.kwargs
        assert kwargs["statuses"] == ["completed"]
        assert kwargs["limit"] == 50

    @pytest.mark.asyncio
    async def test_mcp_project_scope_narrows_admin(
        self, user_admin, project_a, fake_db, fake_request
    ):
        """An MCP `project:<uuid>` scope adds AND project_id = <pid> for admins."""
        scoped = _scoped(user_admin, f"project:{project_a['id']}")
        fake_db.query_jobs = AsyncMock(return_value=_result())
        with _patch_caller_and_db(scoped, fake_db), _patch_audit_unavailable():
            await _list_jobs(fake_request)

        kwargs = fake_db.query_jobs.call_args.kwargs
        assert kwargs["scope_project_id"] == str(project_a["id"])
        assert kwargs["owner_user_id"] is None

    @pytest.mark.asyncio
    async def test_mcp_project_scope_narrows_non_admin(
        self, user_a, project_a, fake_db, fake_request
    ):
        scoped = _scoped(user_a, f"project:{project_a['id']}")
        fake_db.query_jobs = AsyncMock(return_value=_result())
        with _patch_caller_and_db(scoped, fake_db), _patch_audit_unavailable():
            await _list_jobs(fake_request)

        kwargs = fake_db.query_jobs.call_args.kwargs
        assert kwargs["scope_project_id"] == str(project_a["id"])
        assert kwargs["owner_user_id"] == str(user_a["id"])

    @pytest.mark.asyncio
    async def test_unauthenticated_baseline(self, fake_db, fake_request):
        """If `require_approved_user` raises, no DB call happens."""
        fake_db.query_jobs = AsyncMock(return_value=_result())
        with (
            patch(
                "orchestrator.main.require_approved_user",
                AsyncMock(side_effect=HTTPException(status_code=401)),
            ),
            patch("orchestrator.main.postgres_db", fake_db),
            _patch_audit_unavailable(),
        ):
            with pytest.raises(HTTPException) as exc:
                await _list_jobs(fake_request)
        assert exc.value.status_code == 401
        fake_db.query_jobs.assert_not_awaited()


# =============================================================================
# list_jobs — the paging envelope and its filters
# =============================================================================


class TestListJobsEnvelope:
    """Pins the *wire shape*, which the kwargs assertions above cannot see.

    Every test in ``TestListJobs`` stubs the query and asserts on
    ``call_args``, so the response could change shape under all of them and
    they would stay green. These are the ones that notice.
    """

    @pytest.mark.asyncio
    async def test_response_is_an_envelope_not_a_bare_list(
        self, user_a, fake_db, fake_request
    ):
        row = {
            "id": "11111111-1111-4111-8111-111111111111",
            "description": "Ship the report",
            "status": "completed",
            "config_name": "worker_base",
            "project_id": None,
            "user_id": str(user_a["id"]),
        }
        fake_db.query_jobs = AsyncMock(
            return_value=_result([dict(row)], total=806, has_more=True)
        )
        with _patch_caller_and_db(user_a, fake_db), _patch_audit_unavailable():
            body = await _list_jobs(fake_request, limit=25, offset=25)

        assert isinstance(body, dict)
        assert [j["id"] for j in body["jobs"]] == [row["id"]]
        assert body["total"] == 806
        assert body["total_is_capped"] is False
        assert body["has_more"] is True
        assert body["limit"] == 25
        assert body["offset"] == 25
        # Enrichment the route is responsible for, not the query.
        assert body["jobs"][0]["audit_count"] is None

    @pytest.mark.asyncio
    async def test_filters_echo_makes_server_side_defaults_visible(
        self, user_a, fake_db, fake_request
    ):
        """An MCP caller that never read the docs must be able to see that
        archived-project jobs were hidden, or it will conclude the job does
        not exist rather than widening the query."""
        fake_db.query_jobs = AsyncMock(return_value=_result())
        with _patch_caller_and_db(user_a, fake_db), _patch_audit_unavailable():
            body = await _list_jobs(fake_request, status=["failed"], search="abc")

        assert body["filters"]["include_archived_projects"] is False
        assert body["filters"]["status"] == ["failed"]
        assert body["filters"]["search"] == "abc"

    @pytest.mark.asyncio
    async def test_as_of_is_generated_when_absent_and_echoed_back(
        self, user_a, fake_db, fake_request
    ):
        """Paging without a watermark lets concurrent inserts shift the
        window, so the server mints one and the client carries it."""
        fake_db.query_jobs = AsyncMock(return_value=_result())
        with _patch_caller_and_db(user_a, fake_db), _patch_audit_unavailable():
            body = await _list_jobs(fake_request)

        assert body["as_of"]
        assert fake_db.query_jobs.call_args.kwargs["as_of"] is not None

    @pytest.mark.asyncio
    async def test_supplied_as_of_is_passed_through_unchanged(
        self, user_a, fake_db, fake_request
    ):
        from datetime import datetime, timezone

        pinned = datetime(2026, 8, 20, 12, 0, tzinfo=timezone.utc)
        fake_db.query_jobs = AsyncMock(return_value=_result())
        with _patch_caller_and_db(user_a, fake_db), _patch_audit_unavailable():
            body = await _list_jobs(fake_request, as_of=pinned)

        assert fake_db.query_jobs.call_args.kwargs["as_of"] == pinned
        # Zulu, not "+00:00": the value is meant to be pasted straight back
        # into a query string, where '+' would decode as a space and 422.
        assert body["as_of"] == "2026-08-20T12:00:00Z"
        assert "+" not in body["as_of"]

    @pytest.mark.asyncio
    async def test_include_total_false_is_forwarded(
        self, user_a, fake_db, fake_request
    ):
        fake_db.query_jobs = AsyncMock(return_value=_result(total=None))
        with _patch_caller_and_db(user_a, fake_db), _patch_audit_unavailable():
            body = await _list_jobs(fake_request, include_total=False, offset=100)

        assert fake_db.query_jobs.call_args.kwargs["include_total"] is False
        assert body["total"] is None

    @pytest.mark.asyncio
    async def test_capped_total_is_reported_as_capped(
        self, user_a, fake_db, fake_request
    ):
        fake_db.query_jobs = AsyncMock(
            return_value=_result(total=10_000, total_is_capped=True, has_more=True)
        )
        with _patch_caller_and_db(user_a, fake_db), _patch_audit_unavailable():
            body = await _list_jobs(fake_request)

        assert body["total"] == 10_000
        assert body["total_is_capped"] is True


# =============================================================================
# list_jobs — filter validation
# =============================================================================


class TestListJobsFilterValidation:
    @pytest.mark.asyncio
    async def test_unknown_status_is_422_not_an_empty_page(
        self, user_a, fake_db, fake_request
    ):
        """Silently returning zero rows for a typo reads as data loss."""
        fake_db.query_jobs = AsyncMock(return_value=_result())
        with _patch_caller_and_db(user_a, fake_db), _patch_audit_unavailable():
            with pytest.raises(HTTPException) as exc:
                await _list_jobs(fake_request, status=["completed", "donezo"])

        assert exc.value.status_code == 422
        assert "donezo" in exc.value.detail
        fake_db.query_jobs.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_repeated_status_values_are_deduped(
        self, user_a, fake_db, fake_request
    ):
        fake_db.query_jobs = AsyncMock(return_value=_result())
        with _patch_caller_and_db(user_a, fake_db), _patch_audit_unavailable():
            await _list_jobs(fake_request, status=["failed", "failed", "paused"])

        assert fake_db.query_jobs.call_args.kwargs["statuses"] == ["failed", "paused"]

    @pytest.mark.asyncio
    async def test_offset_beyond_the_ceiling_is_400(
        self, user_a, fake_db, fake_request
    ):
        """A runaway MCP loop must not be able to table-scan the fleet."""
        from orchestrator.main import JOBS_MAX_OFFSET

        fake_db.query_jobs = AsyncMock(return_value=_result())
        with _patch_caller_and_db(user_a, fake_db), _patch_audit_unavailable():
            with pytest.raises(HTTPException) as exc:
                await _list_jobs(fake_request, offset=JOBS_MAX_OFFSET + 1)

        assert exc.value.status_code == 400
        fake_db.query_jobs.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_project_id_none_selects_the_projectless_bucket(
        self, user_a, fake_db, fake_request
    ):
        fake_db.query_jobs = AsyncMock(return_value=_result())
        with _patch_caller_and_db(user_a, fake_db), _patch_audit_unavailable():
            await _list_jobs(fake_request, project_id=["none"])

        kwargs = fake_db.query_jobs.call_args.kwargs
        assert kwargs["has_project"] is False
        assert kwargs["project_ids"] is None

    @pytest.mark.asyncio
    async def test_project_id_none_mixed_with_real_ids_is_422(
        self, user_a, project_a, fake_db, fake_request
    ):
        """ "these projects OR no project" is a union the AND-composed filter
        set cannot express; refusing beats returning one arm."""
        fake_db.query_jobs = AsyncMock(return_value=_result())
        with _patch_caller_and_db(user_a, fake_db), _patch_audit_unavailable():
            with pytest.raises(HTTPException) as exc:
                await _list_jobs(
                    fake_request, project_id=["none", str(project_a["id"])]
                )

        assert exc.value.status_code == 422
        fake_db.query_jobs.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_visible_project_filter_reaches_the_query(
        self, user_a, project_a, fake_db, fake_request
    ):
        fake_db.query_jobs = AsyncMock(return_value=_result())
        with _patch_caller_and_db(user_a, fake_db), _patch_audit_unavailable():
            await _list_jobs(fake_request, project_id=[str(project_a["id"])])

        assert fake_db.query_jobs.call_args.kwargs["project_ids"] == [
            str(project_a["id"])
        ]

    @pytest.mark.asyncio
    async def test_filtering_by_an_invisible_project_is_403(
        self, user_a, project_b, fake_db, fake_request
    ):
        """403 rather than an empty page, matching /api/datasources/eligible.

        This is a *narrowing* filter, so it is not a security boundary — the
        visibility OR-clause already bounds the result and an unauthorized id
        would merely return zero rows. It exists so the caller is told why.
        """
        fake_db.query_jobs = AsyncMock(return_value=_result())
        with _patch_caller_and_db(user_a, fake_db), _patch_audit_unavailable():
            with pytest.raises(HTTPException) as exc:
                await _list_jobs(fake_request, project_id=[str(project_b["id"])])

        assert exc.value.status_code == 403
        fake_db.query_jobs.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_admin_may_filter_by_any_project(
        self, user_admin, project_b, fake_db, fake_request
    ):
        """Admins see the full fleet, so the visibility check must not run for
        them — gating it on membership would 403 an admin out of their own
        fleet view."""
        fake_db.query_jobs = AsyncMock(return_value=_result())
        with _patch_caller_and_db(user_admin, fake_db), _patch_audit_unavailable():
            await _list_jobs(fake_request, project_id=[str(project_b["id"])])

        assert fake_db.query_jobs.call_args.kwargs["project_ids"] == [
            str(project_b["id"])
        ]

    @pytest.mark.asyncio
    async def test_mcp_scope_cannot_be_widened_by_a_project_filter(
        self, user_a, project_a, project_b, fake_db, fake_request
    ):
        scoped = _scoped(user_a, f"project:{project_a['id']}")
        fake_db.query_jobs = AsyncMock(return_value=_result())
        with _patch_caller_and_db(scoped, fake_db), _patch_audit_unavailable():
            with pytest.raises(HTTPException) as exc:
                await _list_jobs(fake_request, project_id=[str(project_b["id"])])

        assert exc.value.status_code == 403
        fake_db.query_jobs.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_too_many_project_filters_is_422(self, user_a, fake_db, fake_request):
        """Repeated UUIDs hit nginx's URL ceiling around 40 values, where the
        failure is a truncated request rather than a clear error."""
        from orchestrator.main import JOBS_MAX_PROJECT_FILTERS

        many = [
            f"{i:08d}-0000-4000-8000-000000000000"
            for i in range(JOBS_MAX_PROJECT_FILTERS + 1)
        ]
        fake_db.query_jobs = AsyncMock(return_value=_result())
        with _patch_caller_and_db(user_a, fake_db), _patch_audit_unavailable():
            with pytest.raises(HTTPException) as exc:
                await _list_jobs(fake_request, project_id=many)

        assert exc.value.status_code == 422
        fake_db.query_jobs.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_malformed_project_id_is_422_not_500(
        self, user_a, fake_db, fake_request
    ):
        fake_db.query_jobs = AsyncMock(return_value=_result())
        with _patch_caller_and_db(user_a, fake_db), _patch_audit_unavailable():
            with pytest.raises(HTTPException) as exc:
                await _list_jobs(fake_request, project_id=["not-a-uuid"])

        assert exc.value.status_code == 422

    @pytest.mark.asyncio
    async def test_archived_projects_are_excluded_by_default(
        self, user_a, fake_db, fake_request
    ):
        fake_db.query_jobs = AsyncMock(return_value=_result())
        with _patch_caller_and_db(user_a, fake_db), _patch_audit_unavailable():
            await _list_jobs(fake_request)

        assert fake_db.query_jobs.call_args.kwargs["include_archived_projects"] is False

    @pytest.mark.asyncio
    async def test_archived_projects_can_be_opted_back_in(
        self, user_a, fake_db, fake_request
    ):
        fake_db.query_jobs = AsyncMock(return_value=_result())
        with _patch_caller_and_db(user_a, fake_db), _patch_audit_unavailable():
            await _list_jobs(fake_request, include_archived_projects=True)

        assert fake_db.query_jobs.call_args.kwargs["include_archived_projects"] is True


# =============================================================================
# get_job — owner / project-member / admin / scope
# =============================================================================


class TestGetJob:
    @pytest.mark.asyncio
    async def test_owner_passes(self, user_a, job_a, fake_db, fake_request):
        from orchestrator.main import get_job

        with _patch_caller_and_db(user_a, fake_db), _patch_audit_unavailable():
            result = await get_job(fake_request, str(job_a["id"]))
        # The handler enriches with audit_count=None when the audit store is unavailable
        assert result["id"] == job_a["id"]
        assert "audit_count" in result

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("canonical_owner", "retryable"), [(True, True), (False, False)]
    )
    async def test_detail_retry_action_requires_canonical_recovery_owner(
        self,
        canonical_owner,
        retryable,
        user_a,
        job_a,
        fake_db,
        fake_request,
    ):
        from orchestrator.main import get_job

        job_a["_workspace_recovery"] = {
            "operation_id": "44444444-4444-4444-8444-444444444444",
            "state": "paused_attention",
            "reason_code": "prior_runtime_unfenced",
            "started_at": "2026-09-16T08:00:00Z",
            "deadline_at": "2026-09-16T08:15:00Z",
            "canonical_owner": canonical_owner,
        }
        with _patch_caller_and_db(user_a, fake_db), _patch_audit_unavailable():
            result = await get_job(fake_request, str(job_a["id"]))

        assert result["workspace_recovery"]["retryable"] is retryable
        assert "canonical_owner" not in result["workspace_recovery"]

    @pytest.mark.asyncio
    async def test_cross_user_403(self, user_b, job_a, fake_db, fake_request):
        from orchestrator.main import get_job

        with _patch_caller_and_db(user_b, fake_db), _patch_audit_unavailable():
            with pytest.raises(HTTPException) as exc:
                await get_job(fake_request, str(job_a["id"]))
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_missing_404(self, user_a, fake_db, fake_request):
        from orchestrator.main import get_job

        with _patch_caller_and_db(user_a, fake_db), _patch_audit_unavailable():
            with pytest.raises(HTTPException) as exc:
                await get_job(fake_request, "00000000-0000-0000-0000-000000000999")
        assert exc.value.status_code == 404

    @pytest.mark.asyncio
    async def test_admin_bypass(self, user_admin, job_a, fake_db, fake_request):
        from orchestrator.main import get_job

        with _patch_caller_and_db(user_admin, fake_db), _patch_audit_unavailable():
            result = await get_job(fake_request, str(job_a["id"]))
        assert result["id"] == job_a["id"]

    @pytest.mark.asyncio
    async def test_project_member_passes(
        self, user_a, user_b, job_a, fake_db, fake_request
    ):
        """user_b added as viewer on project_a → can see job_a."""
        from orchestrator.main import get_job
        from uuid import UUID

        async def member_lookup(pid, uid):
            # user_a is owner of project_a (default); user_b is also a viewer.
            if UUID(str(uid)) == user_b["id"]:
                return "viewer"
            if UUID(str(uid)) == user_a["id"]:
                return "owner"
            return None

        fake_db.get_user_role_in_project = AsyncMock(side_effect=member_lookup)
        with _patch_caller_and_db(user_b, fake_db), _patch_audit_unavailable():
            result = await get_job(fake_request, str(job_a["id"]))
        assert result["id"] == job_a["id"]

    @pytest.mark.asyncio
    async def test_mcp_project_scope_mismatch_403(
        self, user_admin, job_a, project_b, fake_db, fake_request
    ):
        """Even an admin with a `project:<other>` scope is denied for an outside job."""
        from orchestrator.main import get_job

        scoped = _scoped(user_admin, f"project:{project_b['id']}")
        with _patch_caller_and_db(scoped, fake_db), _patch_audit_unavailable():
            with pytest.raises(HTTPException) as exc:
                await get_job(fake_request, str(job_a["id"]))
        assert exc.value.status_code == 403


# =============================================================================
# Gated read endpoints — gate fires before service is hit
# =============================================================================
#
# Picks one endpoint per backend so we prove the gate is wired everywhere:
#   audit store (PG) → get_job_audit, get_job_llm_requests
#   gitea (todos)    → get_job_todos, get_current_todos,
#                       list_todo_archives, get_archived_todos
#   gitea (repo)     → list_repo_contents, get_repo_file
#   vector_db (PG)   → list_job_citations, list_job_memories,
#                       get_citation_stats, get_memory_stats,
#                       search_job_sources
#   snapshot svc     → get_job_snapshot
#   agent proxy      → get_job_shell_state (gates before status check)
#   misc             → get_frozen_job_data, get_job_progress, get_job_version
#                       list_message_threads
#
# Each test calls the endpoint with a cross-user caller; the gate must
# raise 403 before any downstream service mock is touched.


def _make_dud(name: str):
    """Return a magic that explodes on attribute access.

    If the gate ever lets the call through, the downstream code blows up
    in a recognisable way — proving the gate is the only thing stopping
    the request.
    """
    return MagicMock(side_effect=AssertionError(f"{name} called past the gate"))


class TestGatedReadEndpoints:
    @pytest.mark.asyncio
    async def test_get_job_audit_blocked_cross_user(
        self, user_b, job_a, fake_db, fake_request
    ):
        from orchestrator.main import _job_audit_dependencies
        from orchestrator.routers.job_audit import get_job_audit

        with (
            _patch_caller_and_db(user_b, fake_db),
            patch("orchestrator.main.audit_reader", _make_dud("audit_reader")),
        ):
            with pytest.raises(HTTPException) as exc:
                await get_job_audit(
                    fake_request,
                    str(job_a["id"]),
                    dependencies=_job_audit_dependencies(),
                )
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_get_job_llm_requests_blocked_cross_user(
        self, user_b, job_a, fake_db, fake_request
    ):
        from orchestrator.main import _job_diagnostics_dependencies
        from orchestrator.routers.job_diagnostics import get_job_llm_requests

        with (
            _patch_caller_and_db(user_b, fake_db),
            patch("orchestrator.main.audit_reader", _make_dud("audit_reader")),
        ):
            with pytest.raises(HTTPException) as exc:
                await get_job_llm_requests(
                    fake_request,
                    str(job_a["id"]),
                    dependencies=_job_diagnostics_dependencies(),
                )
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_get_job_todos_blocked_cross_user(
        self, user_b, job_a, fake_db, fake_request
    ):
        from orchestrator.main import _job_artifacts_dependencies
        from orchestrator.routers.job_artifacts import get_job_todos

        with (
            _patch_caller_and_db(user_b, fake_db),
            patch("orchestrator.main.gitea_client", _make_dud("gitea_client")),
        ):
            with pytest.raises(HTTPException) as exc:
                await get_job_todos(
                    fake_request,
                    str(job_a["id"]),
                    dependencies=_job_artifacts_dependencies(),
                )
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_get_current_todos_blocked_cross_user(
        self, user_b, job_a, fake_db, fake_request
    ):
        from orchestrator.main import _job_artifacts_dependencies
        from orchestrator.routers.job_artifacts import get_current_todos

        with (
            _patch_caller_and_db(user_b, fake_db),
            patch("orchestrator.main.gitea_client", _make_dud("gitea_client")),
        ):
            with pytest.raises(HTTPException) as exc:
                await get_current_todos(
                    fake_request,
                    str(job_a["id"]),
                    dependencies=_job_artifacts_dependencies(),
                )
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_list_todo_archives_blocked_cross_user(
        self, user_b, job_a, fake_db, fake_request
    ):
        from orchestrator.main import _job_artifacts_dependencies
        from orchestrator.routers.job_artifacts import list_todo_archives

        with (
            _patch_caller_and_db(user_b, fake_db),
            patch("orchestrator.main.gitea_client", _make_dud("gitea_client")),
        ):
            with pytest.raises(HTTPException) as exc:
                await list_todo_archives(
                    fake_request,
                    str(job_a["id"]),
                    dependencies=_job_artifacts_dependencies(),
                )
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_get_archived_todos_blocked_cross_user(
        self, user_b, job_a, fake_db, fake_request
    ):
        from orchestrator.main import _job_artifacts_dependencies
        from orchestrator.routers.job_artifacts import get_archived_todos

        with (
            _patch_caller_and_db(user_b, fake_db),
            patch("orchestrator.main.gitea_client", _make_dud("gitea_client")),
        ):
            with pytest.raises(HTTPException) as exc:
                await get_archived_todos(
                    fake_request,
                    str(job_a["id"]),
                    "todos_phase_1_tactical_20260730_120000.md",
                    dependencies=_job_artifacts_dependencies(),
                )
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_list_repo_contents_blocked_cross_user(
        self, user_b, job_a, fake_db, fake_request
    ):
        from orchestrator.main import _job_repo_dependencies
        from orchestrator.routers.job_repo import list_repo_contents

        with (
            _patch_caller_and_db(user_b, fake_db),
            patch("orchestrator.main.gitea_client", _make_dud("gitea_client")),
        ):
            with pytest.raises(HTTPException) as exc:
                await list_repo_contents(
                    fake_request,
                    str(job_a["id"]),
                    path="",
                    ref=None,
                    dependencies=_job_repo_dependencies(),
                )
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_get_repo_file_blocked_cross_user(
        self, user_b, job_a, fake_db, fake_request
    ):
        from orchestrator.main import _job_repo_dependencies
        from orchestrator.routers.job_repo import get_repo_file

        with (
            _patch_caller_and_db(user_b, fake_db),
            patch("orchestrator.main.gitea_client", _make_dud("gitea_client")),
        ):
            with pytest.raises(HTTPException) as exc:
                await get_repo_file(
                    fake_request,
                    str(job_a["id"]),
                    path="README.md",
                    ref=None,
                    dependencies=_job_repo_dependencies(),
                )
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_list_job_citations_blocked_cross_user(
        self, user_b, job_a, fake_db, fake_request
    ):
        from orchestrator.main import _citations_dependencies
        from orchestrator.routers.citations import list_job_citations

        with (
            _patch_caller_and_db(user_b, fake_db),
            patch("orchestrator.main.vector_db", _make_dud("vector_db")),
        ):
            with pytest.raises(HTTPException) as exc:
                await list_job_citations(
                    fake_request,
                    str(job_a["id"]),
                    source_id=None,
                    status=None,
                    limit=50,
                    offset=0,
                    dependencies=_citations_dependencies(),
                )
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_list_job_memories_blocked_cross_user(
        self, user_b, job_a, fake_db, fake_request
    ):
        from orchestrator.main import _citations_dependencies
        from orchestrator.routers.citations import list_job_memories

        with (
            _patch_caller_and_db(user_b, fake_db),
            patch("orchestrator.main.vector_db", _make_dud("vector_db")),
        ):
            with pytest.raises(HTTPException) as exc:
                await list_job_memories(
                    fake_request,
                    str(job_a["id"]),
                    memory_type=None,
                    source=None,
                    search=None,
                    sort_by="created_at",
                    sort_order="desc",
                    limit=50,
                    offset=0,
                    dependencies=_citations_dependencies(),
                )
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_get_memory_stats_blocked_cross_user(
        self, user_b, job_a, fake_db, fake_request
    ):
        from orchestrator.main import _citations_dependencies
        from orchestrator.routers.citations import get_memory_stats

        with (
            _patch_caller_and_db(user_b, fake_db),
            patch("orchestrator.main.vector_db", _make_dud("vector_db")),
        ):
            with pytest.raises(HTTPException) as exc:
                await get_memory_stats(
                    fake_request,
                    str(job_a["id"]),
                    dependencies=_citations_dependencies(),
                )
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_get_citation_stats_blocked_cross_user(
        self, user_b, job_a, fake_db, fake_request
    ):
        from orchestrator.main import _citations_dependencies
        from orchestrator.routers.citations import get_citation_stats

        with (
            _patch_caller_and_db(user_b, fake_db),
            patch("orchestrator.main.vector_db", _make_dud("vector_db")),
        ):
            with pytest.raises(HTTPException) as exc:
                await get_citation_stats(
                    fake_request,
                    str(job_a["id"]),
                    dependencies=_citations_dependencies(),
                )
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_search_job_sources_blocked_cross_user(
        self, user_b, job_a, fake_db, fake_request
    ):
        from orchestrator.main import _citations_dependencies
        from orchestrator.routers.citations import search_job_sources

        with (
            _patch_caller_and_db(user_b, fake_db),
            patch("orchestrator.main.vector_db", _make_dud("vector_db")),
        ):
            with pytest.raises(HTTPException) as exc:
                await search_job_sources(
                    fake_request,
                    str(job_a["id"]),
                    query="q",
                    mode="keyword",
                    source_type=None,
                    tags=None,
                    top_k=10,
                    dependencies=_citations_dependencies(),
                )
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_get_job_snapshot_blocked_cross_user(
        self, user_b, job_a, fake_db, fake_request
    ):
        from orchestrator.main import _workspace_access_dependencies
        from orchestrator.routers.workspace_access import get_job_snapshot

        with (
            _patch_caller_and_db(user_b, fake_db),
            patch("orchestrator.main.snapshot_service", _make_dud("snapshot_service")),
        ):
            with pytest.raises(HTTPException) as exc:
                await get_job_snapshot(
                    fake_request,
                    str(job_a["id"]),
                    dependencies=_workspace_access_dependencies(),
                )
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_get_job_shell_state_blocked_cross_user(
        self, user_b, job_a, fake_db, fake_request
    ):
        from orchestrator.main import _job_diagnostics_dependencies
        from orchestrator.routers.job_diagnostics import get_job_shell_state

        # Mark the job as not-processing — if the gate let through, we'd
        # see 400 from the status check instead of 403.
        fake_db.get_job = AsyncMock(return_value={**job_a, "status": "completed"})
        with _patch_caller_and_db(user_b, fake_db):
            with pytest.raises(HTTPException) as exc:
                await get_job_shell_state(
                    fake_request,
                    str(job_a["id"]),
                    dependencies=_job_diagnostics_dependencies(),
                )
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_get_frozen_job_data_blocked_cross_user(
        self, user_b, job_a, fake_db, fake_request
    ):
        from orchestrator.main import _workspace_access_dependencies
        from orchestrator.routers.workspace_access import get_frozen_job_data

        with _patch_caller_and_db(user_b, fake_db):
            with pytest.raises(HTTPException) as exc:
                await get_frozen_job_data(
                    fake_request,
                    str(job_a["id"]),
                    dependencies=_workspace_access_dependencies(),
                )
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_get_job_progress_blocked_cross_user(
        self, user_b, job_a, fake_db, fake_request
    ):
        from orchestrator.main import _job_inspection_dependencies
        from orchestrator.routers.job_inspection import get_job_progress

        fake_db.get_job_progress = AsyncMock(
            side_effect=AssertionError("progress called past the gate")
        )
        with _patch_caller_and_db(user_b, fake_db):
            with pytest.raises(HTTPException) as exc:
                await get_job_progress(
                    fake_request,
                    str(job_a["id"]),
                    dependencies=_job_inspection_dependencies(),
                )
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_get_job_version_blocked_cross_user(
        self, user_b, job_a, fake_db, fake_request
    ):
        from orchestrator.main import _job_audit_dependencies
        from orchestrator.routers.job_audit import get_job_version

        with (
            _patch_caller_and_db(user_b, fake_db),
            patch("orchestrator.main.audit_reader", _make_dud("audit_reader")),
        ):
            with pytest.raises(HTTPException) as exc:
                await get_job_version(
                    fake_request,
                    str(job_a["id"]),
                    dependencies=_job_audit_dependencies(),
                )
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_list_message_threads_blocked_cross_user(
        self, user_b, job_a, fake_db, fake_request
    ):
        from orchestrator.routers.messaging import list_message_threads

        fake_db.get_message_threads = AsyncMock(
            side_effect=AssertionError("threads called past the gate")
        )
        _point_at_messaging_factories(fake_request)
        with _patch_caller_and_db(user_b, fake_db):
            with pytest.raises(HTTPException) as exc:
                await list_message_threads(fake_request, str(job_a["id"]))
        assert exc.value.status_code == 403


# =============================================================================
# Positive paths — owner gets through, gate doesn't over-block
# =============================================================================


class TestGatedReadEndpointsHappyPath:
    @pytest.mark.asyncio
    async def test_get_job_audit_owner_reaches_audit_store(
        self, user_a, job_a, fake_db, fake_request
    ):
        from orchestrator.main import _job_audit_dependencies
        from orchestrator.routers.job_audit import get_job_audit

        fake_reader = MagicMock()
        fake_reader.is_available = True
        fake_reader.get_job_audit = AsyncMock(return_value={"entries": [], "total": 0})
        with (
            _patch_caller_and_db(user_a, fake_db),
            patch("orchestrator.main.audit_reader", fake_reader),
        ):
            await get_job_audit(
                fake_request, str(job_a["id"]), dependencies=_job_audit_dependencies()
            )
        fake_reader.get_job_audit.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_get_job_todos_owner_reaches_gitea(
        self, user_a, job_a, fake_db, fake_request
    ):
        """Owner gets the Gitea-backed todo state (todos.yaml + archives)."""
        from orchestrator.main import _job_artifacts_dependencies
        from orchestrator.routers.job_artifacts import get_job_todos

        # Legacy-fallback repo resolution: no project jobs-repo rows → job-{id}.
        fake_db.get_project_repositories = AsyncMock(return_value=[])

        async def list_contents(repo_name, path="", ref=None):
            if path == "":
                return [
                    {"name": "plan.md", "path": "plan.md", "type": "file", "size": 12},
                    {"name": "archive", "path": "archive", "type": "dir", "size": 0},
                ]
            if path == "archive":
                return [
                    {
                        "name": "todos_phase_1_tactical_20260730_120000.md",
                        "path": "archive/todos_phase_1_tactical_20260730_120000.md",
                        "type": "file",
                        "size": 100,
                    }
                ]
            return None

        fake_gitea = MagicMock()
        fake_gitea.is_initialized = True
        fake_gitea.list_contents = AsyncMock(side_effect=list_contents)
        fake_gitea.get_file_content = AsyncMock(
            return_value="todos:\n  - id: 1\n    content: Verify the fix end-to-end\n"
        )
        with (
            _patch_caller_and_db(user_a, fake_db),
            patch("orchestrator.main.gitea_client", fake_gitea),
        ):
            result = await get_job_todos(
                fake_request,
                str(job_a["id"]),
                dependencies=_job_artifacts_dependencies(),
            )

        assert result["has_workspace"] is True
        assert result["current"]["todos"][0]["content"] == "Verify the fix end-to-end"
        assert result["current"]["is_current"] is True
        assert (
            result["archives"][0]["filename"]
            == "todos_phase_1_tactical_20260730_120000.md"
        )
        assert result["archives"][0]["phase_name"] == "phase_1_tactical"
        assert result["archives"][0]["timestamp"] == "2026-07-30T12:00:00"

    @pytest.mark.asyncio
    async def test_get_job_todos_gitea_down_degrades_to_empty(
        self, user_a, job_a, fake_db, fake_request
    ):
        """House rule: Gitea being down must never 500 the cockpit todo view."""
        from orchestrator.main import _job_artifacts_dependencies
        from orchestrator.routers.job_artifacts import get_job_todos

        fake_gitea = MagicMock()
        fake_gitea.is_initialized = False
        with (
            _patch_caller_and_db(user_a, fake_db),
            patch("orchestrator.main.gitea_client", fake_gitea),
        ):
            result = await get_job_todos(
                fake_request,
                str(job_a["id"]),
                dependencies=_job_artifacts_dependencies(),
            )
        assert result == {
            "job_id": str(job_a["id"]),
            "current": None,
            "archives": [],
            "has_workspace": False,
        }

    @pytest.mark.asyncio
    async def test_get_job_progress_admin_reaches_db(
        self, user_admin, job_a, fake_db, fake_request
    ):
        """E1/E3: the route now composes the DB basis with the shared
        liveness verdict — the gate still runs first and the DB payload is
        preserved, with state/reasons/sources merged on top."""
        from orchestrator.main import _job_inspection_dependencies
        from orchestrator.routers.job_inspection import get_job_progress

        fake_db.get_job_progress = AsyncMock(return_value={"status": "ok"})
        with _patch_caller_and_db(user_admin, fake_db):
            result = await get_job_progress(
                fake_request,
                str(job_a["id"]),
                dependencies=_job_inspection_dependencies(),
            )
        assert result["status"] == "ok"
        # job_a is 'created' → honest waiting verdict, never a fabricated 0%.
        assert result["state"] == "waiting"
        assert result["reasons"] == ["awaiting workspace provisioning and dispatch"]
        assert "progress_percent" not in result or result["progress_percent"] is None


# =============================================================================
# P4c — job mutation gates
# =============================================================================
# Each test calls a P4c-gated mutation with a cross-user caller; the gate
# raises 403 before the downstream service is touched. Endpoints called by
# the agent (cancel/pause/resume/approve/subjob-merge/agent-release/
# messages/send and POST /api/jobs itself) are NOT in scope here — they move
# to P4b alongside the other Track-B agent-internal endpoints.


class TestJobMutationGates:
    @pytest.mark.asyncio
    async def test_delete_job_snapshot_blocked_cross_user(
        self, user_b, job_a, fake_db, fake_request
    ):
        from orchestrator.main import _workspace_access_dependencies
        from orchestrator.routers.workspace_access import delete_job_snapshot

        with (
            _patch_caller_and_db(user_b, fake_db),
            patch("orchestrator.main.snapshot_service", _make_dud("snapshot_service")),
        ):
            with pytest.raises(HTTPException) as exc:
                await delete_job_snapshot(
                    fake_request,
                    str(job_a["id"]),
                    dependencies=_workspace_access_dependencies(),
                )
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_toggle_snapshot_pin_blocked_cross_user(
        self, user_b, job_a, fake_db, fake_request
    ):
        from orchestrator.main import _workspace_access_dependencies
        from orchestrator.routers.workspace_access import toggle_snapshot_pin

        with (
            _patch_caller_and_db(user_b, fake_db),
            patch("orchestrator.main.snapshot_service", _make_dud("snapshot_service")),
        ):
            with pytest.raises(HTTPException) as exc:
                await toggle_snapshot_pin(
                    fake_request,
                    str(job_a["id"]),
                    dependencies=_workspace_access_dependencies(),
                )
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_start_ide_session_blocked_cross_user(
        self, user_b, job_a, fake_db, fake_request
    ):
        from orchestrator.main import _ide_dependencies
        from orchestrator.routers.ide import start_ide_session

        with (
            _patch_caller_and_db(user_b, fake_db),
            patch(
                "orchestrator.main.ide_session_service",
                _make_dud("ide_session_service"),
            ),
        ):
            with pytest.raises(HTTPException) as exc:
                await start_ide_session(
                    fake_request,
                    str(job_a["id"]),
                    dependencies=_ide_dependencies(),
                )
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_get_ide_session_blocked_cross_user(
        self, user_b, job_a, fake_db, fake_request
    ):
        from orchestrator.main import _ide_dependencies
        from orchestrator.routers.ide import get_ide_session

        with (
            _patch_caller_and_db(user_b, fake_db),
            patch(
                "orchestrator.main.ide_session_service",
                _make_dud("ide_session_service"),
            ),
        ):
            with pytest.raises(HTTPException) as exc:
                await get_ide_session(
                    fake_request,
                    str(job_a["id"]),
                    dependencies=_ide_dependencies(),
                )
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_stop_ide_session_blocked_cross_user(
        self, user_b, job_a, fake_db, fake_request
    ):
        from orchestrator.main import _ide_dependencies
        from orchestrator.routers.ide import stop_ide_session

        with (
            _patch_caller_and_db(user_b, fake_db),
            patch(
                "orchestrator.main.ide_session_service",
                _make_dud("ide_session_service"),
            ),
        ):
            with pytest.raises(HTTPException) as exc:
                await stop_ide_session(
                    fake_request,
                    str(job_a["id"]),
                    dependencies=_ide_dependencies(),
                )
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_get_source_annotations_blocked_cross_user(
        self, user_b, job_a, fake_db, fake_request
    ):
        from orchestrator.main import _citations_dependencies
        from orchestrator.routers.citations import get_source_annotations

        with (
            _patch_caller_and_db(user_b, fake_db),
            patch("orchestrator.main.vector_db", _make_dud("vector_db")),
        ):
            with pytest.raises(HTTPException) as exc:
                await get_source_annotations(
                    fake_request,
                    str(job_a["id"]),
                    1,
                    dependencies=_citations_dependencies(),
                )
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_get_source_tags_blocked_cross_user(
        self, user_b, job_a, fake_db, fake_request
    ):
        from orchestrator.main import _citations_dependencies
        from orchestrator.routers.citations import get_source_tags

        with (
            _patch_caller_and_db(user_b, fake_db),
            patch("orchestrator.main.vector_db", _make_dud("vector_db")),
        ):
            with pytest.raises(HTTPException) as exc:
                await get_source_tags(
                    fake_request,
                    str(job_a["id"]),
                    1,
                    dependencies=_citations_dependencies(),
                )
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_get_job_logs_blocked_cross_user(
        self, user_b, job_a, fake_db, fake_request
    ):
        from orchestrator.main import _job_diagnostics_dependencies
        from orchestrator.routers.job_diagnostics import get_job_logs

        with (
            _patch_caller_and_db(user_b, fake_db),
            patch(
                "orchestrator.main.workspace_service", _make_dud("workspace_service")
            ),
        ):
            with pytest.raises(HTTPException) as exc:
                await get_job_logs(
                    fake_request,
                    str(job_a["id"]),
                    dependencies=_job_diagnostics_dependencies(),
                )
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_upgrade_job_to_vm_blocked_cross_user(
        self, user_b, job_a, fake_db, fake_request
    ):
        from tests._b09_control_seams import upgrade_job_to_vm

        # The gate runs before the body, so no downstream service patch needed —
        # the in-body postgres_db.get_job() call would itself fail the dud test
        # if we patched it. Just rely on the gate to fire first.
        with _patch_caller_and_db(user_b, fake_db):
            with pytest.raises(HTTPException) as exc:
                await upgrade_job_to_vm(fake_request, str(job_a["id"]))
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_promote_job_blocked_cross_user(
        self, user_b, job_a, fake_db, fake_request
    ):
        from orchestrator.main import _projects_dependencies
        from orchestrator.routers.projects import promote_job
        from orchestrator.schemas.projects import PromoteRequest

        body = PromoteRequest(
            name="hijack",
            description=None,
            goal=None,
            user_id=str(user_b["id"]),
        )
        with _patch_caller_and_db(user_b, fake_db):
            with pytest.raises(HTTPException) as exc:
                await promote_job(
                    fake_request,
                    str(job_a["id"]),
                    body,
                    dependencies=_projects_dependencies(),
                )
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_promote_job_forces_caller_user_id(
        self, user_a, user_b, job_a, fake_db, fake_request
    ):
        """Owner promotes their own job; the gate accepts user_a, and the
        handler must overwrite body.user_id with caller.id so a malicious body
        can't grant ownership of the new project to someone else."""
        from orchestrator.main import _projects_dependencies
        from orchestrator.routers.projects import promote_job
        from orchestrator.schemas.projects import PromoteRequest

        body = PromoteRequest(
            name="hijack-attempt",
            description=None,
            goal=None,
            user_id=str(user_b["id"]),  # malicious — caller is user_a
        )
        # job_a has status "created"; promote_job requires "completed" and
        # raises 400 before any project-creation work happens. That natural
        # 400 means we passed the gate AND the body mutation ran — we then
        # assert body.user_id was forced.
        with _patch_caller_and_db(user_a, fake_db):
            with pytest.raises(HTTPException) as exc:
                await promote_job(
                    fake_request,
                    str(job_a["id"]),
                    body,
                    dependencies=_projects_dependencies(),
                )
        assert exc.value.status_code == 400
        assert str(body.user_id) == str(user_a["id"])

    @pytest.mark.asyncio
    async def test_reply_to_agent_message_blocked_cross_user(
        self, user_b, job_a, fake_db, fake_request
    ):
        from orchestrator.main import MessageReplyRequest
        from orchestrator.routers.messaging import reply_to_agent_message

        body = MessageReplyRequest(message="hi", urgent=False)
        _point_at_messaging_factories(fake_request)
        with _patch_caller_and_db(user_b, fake_db):
            with pytest.raises(HTTPException) as exc:
                await reply_to_agent_message(
                    fake_request, str(job_a["id"]), "thread-x", body
                )
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_delete_job_blocked_cross_user(
        self, user_b, job_a, fake_db, fake_request
    ):
        from tests._b09_control_seams import delete_job

        with (
            _patch_caller_and_db(user_b, fake_db),
            patch("orchestrator.main.gitea_client", _make_dud("gitea_client")),
            patch("orchestrator.main.vector_db", _make_dud("vector_db")),
        ):
            with pytest.raises(HTTPException) as exc:
                await delete_job(fake_request, str(job_a["id"]))
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_delete_job_blocked_for_non_owner_member(
        self, user_a, user_b, job_a, fake_db, fake_request
    ):
        """Plain project membership isn't enough — must be job owner OR
        project owner OR admin. user_b is a member of project_a here but
        only as 'editor', so the gate-pass should still be denied."""
        from tests._b09_control_seams import delete_job

        # Make user_b a non-owner member of project_a so require_job_access
        # passes but the secondary role check fails.
        original_role = fake_db.get_user_role_in_project

        async def role_lookup(pid, uid):
            if str(uid) == str(user_b["id"]) and str(pid) == str(job_a["project_id"]):
                return "editor"
            return await original_role(pid, uid)

        fake_db.get_user_role_in_project = role_lookup
        with (
            _patch_caller_and_db(user_b, fake_db),
            patch("orchestrator.main.gitea_client", _make_dud("gitea_client")),
            patch("orchestrator.main.vector_db", _make_dud("vector_db")),
        ):
            with pytest.raises(HTTPException) as exc:
                await delete_job(fake_request, str(job_a["id"]))
        assert exc.value.status_code == 403
        assert "owner" in exc.value.detail.lower()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("has_claim", [False, True])
    async def test_delete_job_tears_down_workspace_before_row_delete(
        self, user_a, job_a, fake_db, fake_request, has_claim
    ):
        """Owner-path delete must release workspace/VM resources and the S3
        snapshots while the row still exists — once the row is gone the
        lifecycle reconciler can no longer reap the pod (no-bound-row is
        treated as in-flight provisioning). See
        knowledge-history/done/deleted_job_orphans_workspace_pod.md."""
        from tests._b09_control_seams import delete_job

        calls: list[str] = []

        async def _cleanup(jid):
            calls.append("workspace")
            return []

        async def _row_delete(jid, **kwargs):
            assert kwargs == {
                "deletion_actor_user_id": str(user_a["id"]),
                "deletion_reason": "authorized_api_delete",
                "return_claim_state": True,
            }
            calls.append("row")
            return {
                "deleted": True,
                "ticket_claim_retained": has_claim,
            }

        fake_db.delete_job = AsyncMock(side_effect=_row_delete)
        fake_db.job_has_durable_ticket_claim = AsyncMock(
            side_effect=AssertionError("deletion must not perform a post-commit read")
        )
        fake_db.has_child_jobs = AsyncMock(return_value=False)
        cleanup = AsyncMock(side_effect=_cleanup)
        fake_snapshot = MagicMock()
        fake_snapshot.is_available = True
        fake_snapshot.delete_snapshot = AsyncMock(return_value=True)
        fake_gitea = MagicMock()
        fake_gitea.is_initialized = False

        with (
            _patch_caller_and_db(user_a, fake_db),
            patch(
                "orchestrator.main.thread_retirement_operations.ThreadRetirementOperations.archive_and_cleanup_workspace",
                cleanup,
            ),
            patch("orchestrator.main.snapshot_service", fake_snapshot),
            patch("orchestrator.main.gitea_client", fake_gitea),
            patch("orchestrator.main.vector_db", _make_dud("vector_db")),
        ):
            result = await delete_job(fake_request, str(job_a["id"]))

        assert result["status"] == "deleted"
        assert result["ticket_claim_retained"] is has_claim
        assert result["ticket_rearmed"] is False
        assert ("remains durable" in result["message"]) is has_claim
        cleanup.assert_awaited_once_with(str(job_a["id"]))
        fake_snapshot.delete_snapshot.assert_awaited_once_with(str(job_a["id"]))
        fake_db.job_has_durable_ticket_claim.assert_not_awaited()
        assert calls == ["workspace", "row"]

    @pytest.mark.asyncio
    async def test_delete_job_with_children_409s_before_teardown(
        self, user_a, job_a, fake_db, fake_request
    ):
        """A parent with surviving child rows can't be row-deleted (FK) — the
        endpoint must fail fast BEFORE tearing down the workspace, or the
        failed delete would leave the job alive with its pod gone."""
        from tests._b09_control_seams import delete_job

        fake_db.has_child_jobs = AsyncMock(return_value=True)
        cleanup = AsyncMock()

        with (
            _patch_caller_and_db(user_a, fake_db),
            patch(
                "orchestrator.main.thread_retirement_operations.ThreadRetirementOperations.archive_and_cleanup_workspace",
                cleanup,
            ),
            patch("orchestrator.main.gitea_client", _make_dud("gitea_client")),
            patch("orchestrator.main.vector_db", _make_dud("vector_db")),
        ):
            with pytest.raises(HTTPException) as exc:
                await delete_job(fake_request, str(job_a["id"]))

        assert exc.value.status_code == 409
        cleanup.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_assign_job_to_agent_blocked_non_admin(
        self, user_a, job_a, fake_db, fake_request
    ):
        """Even the job owner can't manually assign — admin-only override."""
        from tests import _b09_control_seams as control_seams

        with _patch_caller_and_db(user_a, fake_db):
            with pytest.raises(HTTPException) as exc:
                await control_seams.assign_job_to_agent(
                    fake_request, str(job_a["id"]), "some-agent"
                )
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_assign_job_to_agent_admin_passes_gate(
        self, user_admin, job_a, fake_db, fake_request
    ):
        """Admin clears the gate; the in-body failure is a 500 (not a 403),
        proving the gate didn't fire."""
        from tests import _b09_control_seams as control_seams

        fake_db.get_job = AsyncMock(side_effect=RuntimeError("past gate ok"))
        with _patch_caller_and_db(user_admin, fake_db):
            with pytest.raises(HTTPException) as exc:
                await control_seams.assign_job_to_agent(
                    fake_request, str(job_a["id"]), "some-agent"
                )
        assert exc.value.status_code == 500
        assert "past gate ok" in exc.value.detail


# =============================================================================
# P4f — VM lifecycle gates
# =============================================================================
# VMs are per-job, so they follow job visibility. The 3 endpoints all gate
# on `body.job_id` / path `job_id` via `require_job_access`. DELETE adds the
# destructive owner-or-admin check (mirrors DELETE /api/jobs/{id} from P4c).


class TestVmLifecycleGates:
    @pytest.mark.asyncio
    async def test_create_vm_blocked_cross_user(
        self, user_b, job_a, fake_db, fake_request
    ):
        from orchestrator.schemas.workspaces import VMCreateRequest
        from tests._b09_control_seams import create_vm

        body = VMCreateRequest(
            job_id=str(job_a["id"]),
            agent_config="scholar",
        )
        with (
            _patch_caller_and_db(user_b, fake_db),
            patch("orchestrator.main.vm_provisioner", _make_dud("vm_provisioner")),
        ):
            with pytest.raises(HTTPException) as exc:
                await create_vm(fake_request, body)
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_get_vm_status_blocked_cross_user(
        self, user_b, job_a, fake_db, fake_request
    ):
        from tests._b09_control_seams import get_vm_status

        with (
            _patch_caller_and_db(user_b, fake_db),
            patch("orchestrator.main.vm_provisioner", _make_dud("vm_provisioner")),
        ):
            with pytest.raises(HTTPException) as exc:
                await get_vm_status(fake_request, str(job_a["id"]))
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_delete_vm_blocked_cross_user(
        self, user_b, job_a, fake_db, fake_request
    ):
        from tests._b09_control_seams import delete_vm

        with (
            _patch_caller_and_db(user_b, fake_db),
            patch("orchestrator.main.vm_provisioner", _make_dud("vm_provisioner")),
        ):
            with pytest.raises(HTTPException) as exc:
                await delete_vm(fake_request, str(job_a["id"]))
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_delete_vm_blocked_for_non_owner_member(
        self, user_a, user_b, job_a, fake_db, fake_request
    ):
        """Same shape as `delete_job` — plain editor membership must not
        be enough to delete the VM."""
        from tests._b09_control_seams import delete_vm

        original_role = fake_db.get_user_role_in_project

        async def role_lookup(pid, uid):
            if str(uid) == str(user_b["id"]) and str(pid) == str(job_a["project_id"]):
                return "editor"
            return await original_role(pid, uid)

        fake_db.get_user_role_in_project = role_lookup
        with (
            _patch_caller_and_db(user_b, fake_db),
            patch("orchestrator.main.vm_provisioner", _make_dud("vm_provisioner")),
        ):
            with pytest.raises(HTTPException) as exc:
                await delete_vm(fake_request, str(job_a["id"]))
        assert exc.value.status_code == 403
        assert "owner" in exc.value.detail.lower()
