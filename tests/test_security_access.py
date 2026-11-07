"""Tests for orchestrator/security/access.py.

The 3-user fixture (`user_a`, `user_b`, `user_admin`, `fake_db`, ...) is
defined in conftest.py — see the comment block there for the topology.
Every test below exercises the doc's invariants from §"Principles":
owner passes, non-member is denied, admin always passes.
"""

from unittest.mock import AsyncMock, patch
from uuid import UUID

import pytest
from fastapi import HTTPException

from orchestrator.security import access


# =============================================================================
# Helpers: stub require_approved_user so we don't traverse the full auth path
# =============================================================================
#
# All access.py dependencies call `require_approved_user(request, db)` to
# resolve the caller. Tests inject the desired user via a patch on that
# import inside `security.access`.


def _patch_caller(user: dict):
    """Context manager: make `require_approved_user` return ``user``."""
    return patch.object(access, "require_approved_user", AsyncMock(return_value=user))


# =============================================================================
# user_visible_project_ids
# =============================================================================


class TestUserVisibleProjectIds:
    @pytest.mark.asyncio
    async def test_admin_returns_all_sentinel(self, user_admin, fake_db):
        result = await access.user_visible_project_ids(user_admin, fake_db)
        assert result == "all"
        # Admin bypass means we never even hit the DB
        fake_db.get_projects_for_user.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_user_returns_their_projects(self, user_a, project_a, fake_db):
        result = await access.user_visible_project_ids(user_a, fake_db)
        assert result == {project_a["id"]}

    @pytest.mark.asyncio
    async def test_user_with_no_projects_returns_empty_set(self, fake_db):
        loner = {
            "id": UUID("99999999-9999-9999-9999-999999999999"),
            "is_admin": False,
            "is_approved": True,
        }
        result = await access.user_visible_project_ids(loner, fake_db)
        assert result == set()


# =============================================================================
# user_visible_jobs_clause
# =============================================================================


class TestUserVisibleJobsClause:
    def test_admin_clause_is_true(self, user_admin):
        clause, params = access.user_visible_jobs_clause(user_admin)
        assert clause == "TRUE"
        assert params == {}

    def test_user_clause_binds_uid_and_empty_projects(self, user_a):
        clause, params = access.user_visible_jobs_clause(user_a)
        assert ":uid" in clause and ":projects" in clause
        assert "jobs.user_id" in clause
        assert "jobs.project_id" in clause
        assert params["uid"] == user_a["id"]
        # Caller is expected to fill in the real project list — helper is sync.
        assert params["projects"] == []

    def test_custom_table_alias_and_params(self, user_a):
        clause, params = access.user_visible_jobs_clause(
            user_a, table_alias="j", user_param="me", projects_param="pids"
        )
        assert "j.user_id = :me" in clause
        assert "j.project_id = ANY(:pids)" in clause
        assert "me" in params and "pids" in params


# =============================================================================
# require_project_member — role hierarchy and admin bypass
# =============================================================================


class TestRequireProjectMember:
    @pytest.mark.asyncio
    async def test_owner_passes_viewer_check(
        self, user_a, project_a, fake_db, fake_request
    ):
        with _patch_caller(user_a):
            user, project = await access.require_project_member(
                fake_request, fake_db, str(project_a["id"])
            )
        assert user is user_a
        assert project is project_a

    @pytest.mark.asyncio
    async def test_owner_passes_owner_check(
        self, user_a, project_a, fake_db, fake_request
    ):
        with _patch_caller(user_a):
            user, project = await access.require_project_member(
                fake_request, fake_db, str(project_a["id"]), min_role="owner"
            )
        assert user is user_a

    @pytest.mark.asyncio
    async def test_non_member_403(self, user_b, project_a, fake_db, fake_request):
        with _patch_caller(user_b):
            with pytest.raises(HTTPException) as exc:
                await access.require_project_member(
                    fake_request, fake_db, str(project_a["id"])
                )
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_missing_project_404(self, user_a, fake_db, fake_request):
        with _patch_caller(user_a):
            with pytest.raises(HTTPException) as exc:
                await access.require_project_member(
                    fake_request,
                    fake_db,
                    "ffffffff-ffff-ffff-ffff-ffffffffffff",
                )
        assert exc.value.status_code == 404

    @pytest.mark.asyncio
    async def test_admin_bypasses_membership_check(
        self, user_admin, project_a, fake_db, fake_request
    ):
        with _patch_caller(user_admin):
            user, project = await access.require_project_member(
                fake_request, fake_db, str(project_a["id"]), min_role="owner"
            )
        assert user is user_admin
        assert project is project_a
        # Admin path short-circuits before consulting the role table.
        fake_db.get_user_role_in_project.assert_not_awaited()


# =============================================================================
# require_project_owner — narrower wrapper, owner role required exactly
# =============================================================================


class TestRequireProjectOwner:
    @pytest.mark.asyncio
    async def test_owner_passes(self, user_a, project_a, fake_db, fake_request):
        with _patch_caller(user_a):
            user, project = await access.require_project_owner(
                fake_request, fake_db, str(project_a["id"])
            )
        assert user is user_a
        assert project is project_a

    @pytest.mark.asyncio
    async def test_non_owner_403(self, user_b, project_a, fake_db, fake_request):
        with _patch_caller(user_b):
            with pytest.raises(HTTPException) as exc:
                await access.require_project_owner(
                    fake_request, fake_db, str(project_a["id"])
                )
        assert exc.value.status_code == 403
        assert "owner" in exc.value.detail.lower()

    @pytest.mark.asyncio
    async def test_admin_bypasses(self, user_admin, project_a, fake_db, fake_request):
        with _patch_caller(user_admin):
            user, project = await access.require_project_owner(
                fake_request, fake_db, str(project_a["id"])
            )
        assert user is user_admin


# =============================================================================
# require_job_access — owner OR project member OR admin
# =============================================================================


class TestRequireJobAccess:
    @pytest.mark.asyncio
    async def test_job_owner_passes(self, user_a, job_a, fake_db, fake_request):
        with _patch_caller(user_a):
            user, job = await access.require_job_access(
                fake_request, fake_db, str(job_a["id"])
            )
        assert user is user_a
        assert job is job_a

    @pytest.mark.asyncio
    async def test_other_user_403(self, user_b, job_a, fake_db, fake_request):
        with _patch_caller(user_b):
            with pytest.raises(HTTPException) as exc:
                await access.require_job_access(fake_request, fake_db, str(job_a["id"]))
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_admin_bypasses(self, user_admin, job_b, fake_db, fake_request):
        with _patch_caller(user_admin):
            user, job = await access.require_job_access(
                fake_request, fake_db, str(job_b["id"])
            )
        assert user is user_admin
        assert job is job_b

    @pytest.mark.asyncio
    async def test_missing_job_404(self, user_a, fake_db, fake_request):
        with _patch_caller(user_a):
            with pytest.raises(HTTPException) as exc:
                await access.require_job_access(
                    fake_request,
                    fake_db,
                    "deaddead-dead-dead-dead-deaddeaddead",
                )
        assert exc.value.status_code == 404

    @pytest.mark.asyncio
    async def test_project_member_can_access_jobs_in_project(
        self, user_a, job_a, fake_db, fake_request
    ):
        # user_a is owner of project_a, so they already pass via owner-of-job.
        # Simulate a *different* job in project_a owned by user_admin, so
        # user_a's only path through is project membership.
        admin_owned_job = {
            "id": UUID("c1c1c1c1-c1c1-c1c1-c1c1-c1c1c1c1c1c1"),
            "user_id": UUID("33333333-3333-3333-3333-333333333333"),
            "project_id": job_a["project_id"],
            "status": "created",
        }
        original = fake_db.get_job.side_effect

        async def patched(jid):
            if str(jid) == str(admin_owned_job["id"]):
                return admin_owned_job
            return await original(jid)

        fake_db.get_job.side_effect = patched
        with _patch_caller(user_a):
            user, job = await access.require_job_access(
                fake_request, fake_db, str(admin_owned_job["id"])
            )
        assert job["id"] == admin_owned_job["id"]


# =============================================================================
# user_can_access_job — bool variant used by the SSE event filter (F6)
# =============================================================================


class TestUserCanAccessJob:
    @pytest.mark.asyncio
    async def test_admin_always_true(self, user_admin, fake_db):
        # Even with bogus id, admin short-circuits to True without a DB call.
        assert await access.user_can_access_job(user_admin, fake_db, "anything")
        fake_db.get_job.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_owner_true(self, user_a, job_a, fake_db):
        assert await access.user_can_access_job(user_a, fake_db, str(job_a["id"]))

    @pytest.mark.asyncio
    async def test_other_user_false(self, user_b, job_a, fake_db):
        assert not await access.user_can_access_job(user_b, fake_db, str(job_a["id"]))

    @pytest.mark.asyncio
    async def test_project_member_true(self, user_a, job_a, fake_db):
        """user_a owns project_a; an admin-owned job in project_a is
        still accessible to user_a via the membership path."""
        admin_job = {
            "id": UUID("d1d1d1d1-d1d1-d1d1-d1d1-d1d1d1d1d1d1"),
            "user_id": UUID("33333333-3333-3333-3333-333333333333"),
            "project_id": job_a["project_id"],
            "status": "created",
        }
        original = fake_db.get_job.side_effect

        async def patched(jid):
            if str(jid) == str(admin_job["id"]):
                return admin_job
            return await original(jid)

        fake_db.get_job.side_effect = patched
        assert await access.user_can_access_job(user_a, fake_db, str(admin_job["id"]))

    @pytest.mark.asyncio
    async def test_none_job_id_false_for_non_admin(self, user_a, fake_db):
        """Orphan events (no job_id) fail closed for non-admins.

        This is the SSE-filter use case: `request_decided` events used
        to lack ``job_id`` entirely; F6 added it but old in-flight events
        could still arrive with None. Drop them for non-admins.
        """
        assert not await access.user_can_access_job(user_a, fake_db, None)
        assert not await access.user_can_access_job(user_a, fake_db, "")
        # Admin still passes (separate test above, kept here for clarity)
        # so the full matrix is in one place.

    @pytest.mark.asyncio
    async def test_unknown_job_id_false(self, user_a, fake_db):
        assert not await access.user_can_access_job(
            user_a, fake_db, "ffffffff-ffff-ffff-ffff-ffffffffffff"
        )


# =============================================================================
# user_can_access_ide_entity — accepts job OR thread UUID, returns bool
# =============================================================================


class TestUserCanAccessIdeEntity:
    @pytest.mark.asyncio
    async def test_admin_always_true(self, user_admin, fake_db):
        result = await access.user_can_access_ide_entity(
            user_admin, fake_db, "any-id-at-all"
        )
        assert result is True

    @pytest.mark.asyncio
    async def test_job_owner_true(self, user_a, job_a, fake_db):
        result = await access.user_can_access_ide_entity(
            user_a, fake_db, str(job_a["id"])
        )
        assert result is True

    @pytest.mark.asyncio
    async def test_other_user_jobs_false(self, user_b, job_a, fake_db):
        result = await access.user_can_access_ide_entity(
            user_b, fake_db, str(job_a["id"])
        )
        assert result is False

    @pytest.mark.asyncio
    async def test_thread_owner_true(self, user_a, thread_a, fake_db):
        result = await access.user_can_access_ide_entity(
            user_a, fake_db, str(thread_a["id"])
        )
        assert result is True

    @pytest.mark.asyncio
    async def test_other_user_threads_false(self, user_b, thread_a, fake_db):
        result = await access.user_can_access_ide_entity(
            user_b, fake_db, str(thread_a["id"])
        )
        assert result is False

    @pytest.mark.asyncio
    async def test_unknown_id_false(self, user_a, fake_db):
        result = await access.user_can_access_ide_entity(
            user_a, fake_db, "unknown-but-shaped-like-uuid"
        )
        assert result is False


# =============================================================================
# user_can_access_job_or_thread — citations carry job_id == thread_id for
# sessions; this is the gate behind /api/citations/{id}{,/snapshot,/drift}
# =============================================================================


class TestUserCanAccessJobOrThread:
    @pytest.mark.asyncio
    async def test_admin_always_true(self, user_admin, fake_db):
        assert (
            await access.user_can_access_job_or_thread(
                user_admin, fake_db, "any-id-at-all"
            )
            is True
        )

    @pytest.mark.asyncio
    async def test_job_owner_true(self, user_a, job_a, fake_db):
        assert (
            await access.user_can_access_job_or_thread(
                user_a, fake_db, str(job_a["id"])
            )
            is True
        )

    @pytest.mark.asyncio
    async def test_other_user_job_false(self, user_b, job_a, fake_db):
        assert (
            await access.user_can_access_job_or_thread(
                user_b, fake_db, str(job_a["id"])
            )
            is False
        )

    @pytest.mark.asyncio
    async def test_thread_owner_true(self, user_a, thread_a, fake_db):
        # The session-citation case: job_id is actually a thread id the user owns.
        assert (
            await access.user_can_access_job_or_thread(
                user_a, fake_db, str(thread_a["id"])
            )
            is True
        )

    @pytest.mark.asyncio
    async def test_other_user_thread_false(self, user_b, thread_a, fake_db):
        assert (
            await access.user_can_access_job_or_thread(
                user_b, fake_db, str(thread_a["id"])
            )
            is False
        )

    @pytest.mark.asyncio
    async def test_unknown_id_false(self, user_a, fake_db):
        assert (
            await access.user_can_access_job_or_thread(
                user_a, fake_db, "neither-job-nor-thread"
            )
            is False
        )

    @pytest.mark.asyncio
    async def test_none_entity_false(self, user_a, fake_db):
        # Orphan citation (job_id NULL) → caller passes None → fail closed.
        assert (
            await access.user_can_access_job_or_thread(user_a, fake_db, None) is False
        )


# =============================================================================
# require_sudo_request_authority — owner of related job's project, or admin
# =============================================================================


class TestRequireSudoRequestAuthority:
    @pytest.mark.asyncio
    async def test_unknown_request_404(self, user_a, fake_db, fake_request):
        with patch("orchestrator.services.sudo_gate.sudo_gate") as gate:
            gate.get_request = AsyncMock(return_value=None)
            with _patch_caller(user_a):
                with pytest.raises(HTTPException) as exc:
                    await access.require_sudo_request_authority(
                        fake_request, fake_db, "no-such-request"
                    )
        assert exc.value.status_code == 404

    @pytest.mark.asyncio
    async def test_admin_passes(self, user_admin, job_a, fake_db, fake_request):
        sudo_req = {"id": "req-1", "job_id": str(job_a["id"])}
        with patch("orchestrator.services.sudo_gate.sudo_gate") as gate:
            gate.get_request = AsyncMock(return_value=sudo_req)
            with _patch_caller(user_admin):
                result = await access.require_sudo_request_authority(
                    fake_request, fake_db, "req-1"
                )
        assert result is sudo_req

    @pytest.mark.asyncio
    async def test_project_owner_of_related_job_passes(
        self, user_a, job_a, fake_db, fake_request
    ):
        # user_a is owner of project_a, job_a is in project_a → user_a passes
        # even though they're not the job owner.
        sudo_req = {"id": "req-1", "job_id": str(job_a["id"])}
        with patch("orchestrator.services.sudo_gate.sudo_gate") as gate:
            gate.get_request = AsyncMock(return_value=sudo_req)
            with _patch_caller(user_a):
                result = await access.require_sudo_request_authority(
                    fake_request, fake_db, "req-1"
                )
        assert result is sudo_req

    @pytest.mark.asyncio
    async def test_unrelated_user_403(self, user_b, job_a, fake_db, fake_request):
        sudo_req = {"id": "req-1", "job_id": str(job_a["id"])}
        with patch("orchestrator.services.sudo_gate.sudo_gate") as gate:
            gate.get_request = AsyncMock(return_value=sudo_req)
            with _patch_caller(user_b):
                with pytest.raises(HTTPException) as exc:
                    await access.require_sudo_request_authority(
                        fake_request, fake_db, "req-1"
                    )
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_orphan_request_admin_only(self, user_a, fake_db, fake_request):
        """Sudo request with no job_id is admin-only."""
        sudo_req = {"id": "req-1", "job_id": None}
        with patch("orchestrator.services.sudo_gate.sudo_gate") as gate:
            gate.get_request = AsyncMock(return_value=sudo_req)
            with _patch_caller(user_a):
                with pytest.raises(HTTPException) as exc:
                    await access.require_sudo_request_authority(
                        fake_request, fake_db, "req-1"
                    )
        assert exc.value.status_code == 403


# =============================================================================
# apply_mcp_scope — additional WHERE filter on top of user visibility
# =============================================================================


class TestApplyMcpScope:
    def test_no_scopes_is_noop(self, user_a):
        clause, params = access.apply_mcp_scope(user_a)
        assert clause == ""
        assert params == {}

    def test_scope_user_is_noop(self, user_a):
        user_a["scopes"] = ["user"]
        clause, params = access.apply_mcp_scope(user_a)
        assert clause == ""
        assert params == {}

    def test_scope_all_is_noop(self, user_a):
        # 'all' is not a magic admin-bypass at the query level — actual admin
        # status comes from the realm role, not the scope. See doc Q#3.
        user_a["scopes"] = ["all"]
        clause, params = access.apply_mcp_scope(user_a)
        assert clause == ""
        assert params == {}

    def test_scope_project_narrows_to_one_project(self, user_a, project_a):
        user_a["scopes"] = [f"project:{project_a['id']}"]
        clause, params = access.apply_mcp_scope(user_a)
        assert "project_id" in clause
        assert params["scope_project"] == project_a["id"]

    def test_malformed_project_scope_fails_closed(self, user_a):
        user_a["scopes"] = ["project:not-a-uuid"]
        clause, params = access.apply_mcp_scope(user_a)
        # An unsatisfiable bind — caller's outer query returns no rows.
        assert "project_id" in clause
        assert params["scope_project"] is None


# =============================================================================
# Project lifecycle — archived projects refuse new work
# =============================================================================
#
# knowledge-base/knowledge/features/project_and_job_list_filtering.md §4.2/§4.3.
# The guard flag is layer 1 of two: good errors at the HTTP edge. Layer 2
# (the five creation seams an X-Internal-Key caller reaches without ever
# touching these guards) is pinned in
# tests/test_archived_projects_refuse_new_work.py.


@pytest.fixture
def archived_project(project_a):
    project_a["status"] = "archived"
    return project_a


class TestProjectIsArchived:
    def test_archived_status(self):
        assert access.project_is_archived({"status": "archived"}) is True

    def test_case_is_not_load_bearing(self):
        assert access.project_is_archived({"status": "Archived"}) is True
        assert access.project_is_archived({"status": " ARCHIVED "}) is True

    def test_active_is_not_archived(self):
        assert access.project_is_archived({"status": "active"}) is False

    def test_missing_null_and_unknown_status_fail_toward_live(self):
        # Fail toward showing: a project whose status we cannot classify must
        # keep working rather than silently becoming read-only.
        assert access.project_is_archived({}) is False
        assert access.project_is_archived({"status": None}) is False
        assert access.project_is_archived({"status": "paused"}) is False
        assert access.project_is_archived(None) is False


class TestNormalizeProjectStatuses:
    def test_none_defaults_to_active_only(self):
        assert access.normalize_project_statuses(None) == ["active"]

    def test_empty_list_defaults_to_active_only(self):
        # Omission must never widen to "everything".
        assert access.normalize_project_statuses([]) == ["active"]
        assert access.normalize_project_statuses(["", "   "]) == ["active"]

    def test_lowercases_trims_and_dedupes_preserving_order(self):
        assert access.normalize_project_statuses(
            [" Archived ", "ACTIVE", "archived"]
        ) == ["archived", "active"]

    def test_non_sequence_is_treated_as_unsupplied(self):
        # Handlers get called directly (without the param) all over this
        # suite, leaving the unresolved fastapi Query default in place.
        assert access.normalize_project_statuses(object()) == ["active"]


class TestProjectStatusFilterSql:
    def test_requested_statuses_bind_to_the_param(self):
        sql = access.project_status_filter_sql("$1")
        assert "$1::text[]" in sql
        assert "COALESCE(status, 'active')" in sql

    def test_unknown_statuses_always_survive(self):
        # The default filter must hide exactly `archived`, not every row whose
        # state we cannot classify.
        sql = access.project_status_filter_sql("$1")
        assert "NOT IN ('active', 'archived')" in sql

    def test_column_is_qualifiable(self):
        sql = access.project_status_filter_sql("$3", column="p.status")
        assert "COALESCE(p.status, 'active')" in sql


@pytest.fixture
def deleting_project(project_a):
    project_a["deletion_requested_at"] = "2026-11-07T10:00:00+00:00"
    return project_a


class TestRequireProjectDeleting:
    """A project whose delete was decided answers only its own delete."""

    @pytest.mark.asyncio
    async def test_every_call_is_refused_reads_too(
        self, user_a, deleting_project, fake_db, fake_request
    ):
        with _patch_caller(user_a):
            with pytest.raises(HTTPException) as exc:
                await access.require_project_member(
                    fake_request, fake_db, str(deleting_project["id"])
                )
        assert exc.value.status_code == 409
        assert exc.value.detail == access.PROJECT_DELETING_DETAIL

    @pytest.mark.asyncio
    async def test_the_owner_gate_refuses_too(
        self, user_admin, deleting_project, fake_db, fake_request
    ):
        with _patch_caller(user_admin):
            with pytest.raises(HTTPException) as exc:
                await access.require_project_owner(
                    fake_request, fake_db, str(deleting_project["id"])
                )
        assert exc.value.status_code == 409

    @pytest.mark.asyncio
    async def test_the_delete_itself_passes(
        self, user_admin, deleting_project, fake_db, fake_request
    ):
        with _patch_caller(user_admin):
            _, project = await access.require_project_owner(
                fake_request,
                fake_db,
                str(deleting_project["id"]),
                allow_deleting=True,
            )
        assert project is deleting_project

    @pytest.mark.asyncio
    async def test_authorization_ranks_above_the_deletion(
        self, user_b, deleting_project, fake_db, fake_request
    ):
        with _patch_caller(user_b):
            with pytest.raises(HTTPException) as exc:
                await access.require_project_member(
                    fake_request, fake_db, str(deleting_project["id"])
                )
        assert exc.value.status_code == 403


class TestRequireProjectMemberArchived:
    @pytest.mark.asyncio
    async def test_default_allows_archived(
        self, user_a, archived_project, fake_db, fake_request
    ):
        # ~30 read endpoints ride this default. An archive you cannot open is
        # a trap, not a lifecycle state.
        with _patch_caller(user_a):
            user, project = await access.require_project_member(
                fake_request, fake_db, str(archived_project["id"])
            )
        assert user is user_a
        assert project is archived_project

    @pytest.mark.asyncio
    async def test_write_site_gets_409_with_a_plain_sentence(
        self, user_a, archived_project, fake_db, fake_request
    ):
        with _patch_caller(user_a):
            with pytest.raises(HTTPException) as exc:
                await access.require_project_member(
                    fake_request,
                    fake_db,
                    str(archived_project["id"]),
                    min_role="editor",
                    allow_archived=False,
                )
        assert exc.value.status_code == 409
        # Plain string, not a {code, message} dict — the cockpit's generic
        # error path types `detail` as a string.
        assert isinstance(exc.value.detail, str)
        assert exc.value.detail == access.PROJECT_ARCHIVED_DETAIL
        assert "Unarchive it" in exc.value.detail

    @pytest.mark.asyncio
    async def test_refusal_is_not_logged_as_an_access_denial(
        self, user_a, archived_project, fake_db, fake_request
    ):
        # `security_events` exists to detect UUID-probing. An archived refusal
        # handed to an authorized member is a lifecycle conflict, and filing it
        # under access_denied would blunt that detector.
        with _patch_caller(user_a):
            with pytest.raises(HTTPException):
                await access.require_project_member(
                    fake_request,
                    fake_db,
                    str(archived_project["id"]),
                    allow_archived=False,
                )
        fake_db.record_security_event.assert_awaited_once()
        kwargs = fake_db.record_security_event.await_args.kwargs
        assert kwargs["event_type"] == "project_archived_write"

    @pytest.mark.asyncio
    async def test_authorization_ranks_above_lifecycle(
        self, user_b, archived_project, fake_db, fake_request
    ):
        # A non-member must not be able to use the 409/403 split as an oracle
        # for whether a project exists and what state it is in.
        with _patch_caller(user_b):
            with pytest.raises(HTTPException) as exc:
                await access.require_project_member(
                    fake_request,
                    fake_db,
                    str(archived_project["id"]),
                    allow_archived=False,
                )
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_missing_project_still_404s_before_the_lifecycle_check(
        self, user_a, fake_db, fake_request
    ):
        with _patch_caller(user_a):
            with pytest.raises(HTTPException) as exc:
                await access.require_project_member(
                    fake_request,
                    fake_db,
                    "ffffffff-ffff-ffff-ffff-ffffffffffff",
                    allow_archived=False,
                )
        assert exc.value.status_code == 404

    @pytest.mark.asyncio
    async def test_admin_is_refused_too(
        self, user_admin, archived_project, fake_db, fake_request
    ):
        # Admin bypasses *authorization*, not the lifecycle: creating new work
        # on a historical record is the mistake, whoever is asking.
        with _patch_caller(user_admin):
            with pytest.raises(HTTPException) as exc:
                await access.require_project_member(
                    fake_request,
                    fake_db,
                    str(archived_project["id"]),
                    allow_archived=False,
                )
        assert exc.value.status_code == 409

    @pytest.mark.asyncio
    async def test_unknown_status_passes_a_write_gate(
        self, user_a, project_a, fake_db, fake_request
    ):
        project_a["status"] = None
        with _patch_caller(user_a):
            _, project = await access.require_project_member(
                fake_request, fake_db, str(project_a["id"]), allow_archived=False
            )
        assert project is project_a


class TestRequireProjectOwnerArchived:
    @pytest.mark.asyncio
    async def test_default_allows_archived_for_teardown(
        self, user_a, archived_project, fake_db, fake_request
    ):
        # DELETE, detach, decommission and the unarchive PATCH all ride this.
        with _patch_caller(user_a):
            _, project = await access.require_project_owner(
                fake_request, fake_db, str(archived_project["id"])
            )
        assert project is archived_project

    @pytest.mark.asyncio
    async def test_write_site_gets_409(
        self, user_a, archived_project, fake_db, fake_request
    ):
        with _patch_caller(user_a):
            with pytest.raises(HTTPException) as exc:
                await access.require_project_owner(
                    fake_request,
                    fake_db,
                    str(archived_project["id"]),
                    allow_archived=False,
                )
        assert exc.value.status_code == 409

    @pytest.mark.asyncio
    async def test_non_owner_still_gets_403(
        self, user_b, archived_project, fake_db, fake_request
    ):
        with _patch_caller(user_b):
            with pytest.raises(HTTPException) as exc:
                await access.require_project_owner(
                    fake_request,
                    fake_db,
                    str(archived_project["id"]),
                    allow_archived=False,
                )
        assert exc.value.status_code == 403
