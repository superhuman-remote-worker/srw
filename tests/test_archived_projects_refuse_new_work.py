"""An archived project is a historical record: it takes no new work.

Companion to ``tests/test_formatters_archived_projects.py``, which pins the
*presentation* half of the same incident. That mitigation was the only thing
that shipped on 2026-08-15; the server still accepted every write, and the
shipped UI copy (``projectDetail.settings.archiveDesc``: "Mark this project as
archived. Jobs will no longer be created.") stayed false. This file pins the
server half.

**The incident.** "Better Resavio" was split, leaving "Better Resavio
(pre-split archive)" behind under a near-identical name. An officer was
commissioned onto the ARCHIVE and ran a full watch there, dispatching three
workers against a project that exists only as a historical record. Two things
had to be true for that to happen, and both are regression-tested below: the
commission endpoint accepted the archived project, and the officer auto-pull
tick kept dispatching against it.

**Why the HTTP guard is not enough.** ``require_project_member`` is skipped
entirely for ``X-Internal-Key`` callers — which is all MCP traffic, all agent
delegation and the bench sweeper — so five of the seven paths that create work
never touch it. Those are layer 2, and each one is pinned here:

* ``POST /api/jobs`` → the unconditional ``get_project`` inside ``create_job``
* ``POST /api/persistent/threads`` → the ``archived`` verdict
  (``tests/test_thread_project_verdicts.py``)
* automation fire, cron **and** owner "Run now" → ``create_job_from_automation``
* project-loop materialisation → ``create_loop_job``
* officer auto-pull tick → ``tick_officer``

Background/service paths **skip and log**; they never raise. A cron tick has no
HTTP caller to receive a 409, and disabling the automation would silently
destroy the user's configuration.

Design: knowledge-base/knowledge/features/project_and_job_list_filtering.md §4.3.
"""

from __future__ import annotations

import logging
from contextlib import ExitStack
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from orchestrator.security import access as access_module


PROJECT_ID = "68137e29-6b1f-4f1b-a0c1-4e6dc2be3f9a"  # the pre-split archive
OFFICER_THREAD_ID = "a3333333-3333-4333-8333-333333333333"


def _officer_post_request(request, db):
    """Wire the Post's dependencies onto the request the route reads them from.

    The commission declaration resolves its store through
    ``request.app.state`` now, so patching ``orchestrator.main.postgres_db``
    alone would leave the archived-project gate reading the real one.
    """
    from orchestrator.services.officer_post_lifecycle import (
        OfficerPostLifecycleDependencies,
    )
    from orchestrator.services.officer_post_policy import (
        OfficerPostPolicyDependencies,
    )

    request.app.state.officer_post_lifecycle_dependencies_factory = (
        lambda: OfficerPostLifecycleDependencies(
            store=db,
            persistent_provisioner=MagicMock(),
            persistent_thread_recycler=None,
            policy=OfficerPostPolicyDependencies(
                auto_pull_release_enabled=lambda: False
            ),
            kick_officer_event_drain=MagicMock(),
            deliver_officer_note=AsyncMock(),
            create_thread=AsyncMock(),
            end_thread_flow=AsyncMock(),
        )
    )
    return request


def _patch_caller_and_db(user: dict, db):
    """Stack the patches every endpoint test needs (see test_project_access)."""
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


@pytest.fixture
def archived(project_a):
    """``project_a`` from conftest, archived."""
    project_a["status"] = "archived"
    return project_a


# =============================================================================
# The 2026-08-15 incident — both halves
# =============================================================================


class TestOfficerCannotBeCommissionedOntoAnArchive:
    """Entry point of the incident: the commission endpoint."""

    @pytest.mark.asyncio
    async def test_commission_is_refused(self, user_a, archived, fake_db, fake_request):
        from orchestrator.routers.officers import commission_project_officer

        fake_request = _officer_post_request(fake_request, fake_db)
        with _patch_caller_and_db(user_a, fake_db):
            with pytest.raises(HTTPException) as exc:
                await commission_project_officer(
                    fake_request, str(archived["id"]), None
                )

        assert exc.value.status_code == 409
        assert exc.value.detail == access_module.PROJECT_ARCHIVED_DETAIL
        # Nothing was touched: the refusal lands before the capability gate,
        # before the standing-post lookup, and before the kit is written.
        fake_db.get_officer_thread_for_project.assert_not_awaited()
        fake_db.update_project_officer_post.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_commission_still_works_on_a_live_project(
        self, user_a, project_a, fake_db, fake_request
    ):
        """The refusal must be about the archive, not about the endpoint."""
        from orchestrator.routers.officers import commission_project_officer

        fake_request = _officer_post_request(fake_request, fake_db)
        fake_db.user_can_run_unattended_operations = AsyncMock(return_value=False)
        with _patch_caller_and_db(user_a, fake_db):
            with pytest.raises(HTTPException) as exc:
                await commission_project_officer(
                    fake_request, str(project_a["id"]), None
                )
        # Reached the NEXT gate (the unattended_operations grant), which is
        # exactly as far as this fixture can go.
        assert exc.value.status_code == 403


class TestOfficerTickSkipsAnArchivedProject:
    """The path the incident actually ran on: the auto-pull tick."""

    def _officer_row(self):
        return {
            "id": OFFICER_THREAD_ID,
            "project_id": PROJECT_ID,
            "metadata": {},
            "user_id": "11111111-1111-1111-1111-111111111111",
        }

    @pytest.mark.asyncio
    async def test_tick_skips_and_logs(self, caplog):
        from orchestrator.services.officer_backlog import tick_officer

        db = MagicMock()
        db.get_project = AsyncMock(
            return_value={"id": PROJECT_ID, "status": "archived"}
        )
        vector_db = MagicMock()

        with caplog.at_level(
            logging.INFO, logger="orchestrator.services.officer_backlog"
        ):
            counts = await tick_officer(db, vector_db, self._officer_row())

        assert counts["dispatched"] == 0
        assert counts["skipped"] == 1
        # Silent skips are how a fleet goes wrong without anyone noticing.
        assert any(
            "skip=project-archived" in record.getMessage() for record in caplog.records
        )
        # It stands down BEFORE any backlog work: no preflight recovery, no
        # pool pull, no capacity lineage read.
        db.list_stale_officer_claims.assert_not_called()
        db.get_officer_capacity_lineage.assert_not_called()

    @pytest.mark.asyncio
    async def test_tick_proceeds_on_a_live_project(self):
        """The skip must be about the archive, not about the tick."""
        from orchestrator.services.officer_backlog import tick_officer

        db = MagicMock()
        db.get_project = AsyncMock(return_value={"id": PROJECT_ID, "status": "active"})
        db.list_officer_job_preflights = AsyncMock(return_value=[])

        counts = await tick_officer(db, MagicMock(), self._officer_row())

        # auto_pull is off in this bare metadata, so the tick stops after the
        # preflight recovery — but it got PAST the lifecycle gate, which is
        # the only thing being asserted.
        assert counts["skipped"] == 0
        db.get_project.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_a_null_status_project_is_not_treated_as_archived(self):
        from orchestrator.services.officer_backlog import tick_officer

        db = MagicMock()
        db.get_project = AsyncMock(return_value={"id": PROJECT_ID, "status": None})
        db.list_officer_job_preflights = AsyncMock(return_value=[])

        counts = await tick_officer(db, MagicMock(), self._officer_row())

        assert counts["skipped"] == 0


# =============================================================================
# POST /api/jobs — internal callers matter more than cockpit ones
# =============================================================================


class TestCreateJobRefusesArchivedProjects:
    @pytest.mark.asyncio
    async def test_internal_key_caller_is_refused(
        self, user_a, archived, fake_db, fake_request
    ):
        """This is the path that matters: MCP and agent delegation live here,
        and they skip ``require_project_member`` entirely."""
        from orchestrator.schemas.job_create import JobCreate
        from tests._b09_control_seams import create_job

        fake_request.headers = {
            "X-Internal-Key": "secret",
            "X-MCP-User-Id": str(user_a["id"]),
        }
        body = JobCreate(
            description="work on the archive", project_id=str(archived["id"])
        )

        with (
            patch.object(access_module, "_INTERNAL_KEY", "secret"),
            patch("orchestrator.main.app.state.resources.postgres_db", fake_db),
            patch(
                "orchestrator.security.auth.require_approved_user",
                AsyncMock(return_value=user_a),
            ),
            patch(
                "orchestrator.application.access.enforce_readiness_gate",
                AsyncMock(return_value=None),
            ),
            patch(
                "orchestrator.security.access.require_project_member",
                AsyncMock(side_effect=AssertionError("the guard is skipped here")),
            ),
            pytest.raises(HTTPException) as exc,
        ):
            await create_job(fake_request, body)

        assert exc.value.status_code == 409
        assert exc.value.detail == access_module.PROJECT_ARCHIVED_DETAIL
        fake_db.create_job.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_cockpit_caller_gets_the_same_refusal(
        self, user_a, archived, fake_db, fake_request
    ):
        from orchestrator.schemas.job_create import JobCreate
        from tests._b09_control_seams import create_job

        fake_request.headers = {}
        body = JobCreate(
            description="work on the archive", project_id=str(archived["id"])
        )

        with (
            patch.object(access_module, "_INTERNAL_KEY", "secret"),
            patch("orchestrator.main.app.state.resources.postgres_db", fake_db),
            patch(
                "orchestrator.security.auth.require_approved_user",
                AsyncMock(return_value=user_a),
            ),
            patch(
                "orchestrator.security.access.require_approved_user",
                AsyncMock(return_value=user_a),
            ),
            patch(
                "orchestrator.application.access.enforce_readiness_gate",
                AsyncMock(return_value=None),
            ),
            pytest.raises(HTTPException) as exc,
        ):
            await create_job(fake_request, body)

        assert exc.value.status_code == 409
        assert exc.value.detail == access_module.PROJECT_ARCHIVED_DETAIL
        fake_db.create_job.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_projectless_job_is_unaffected(self, user_a, fake_db, fake_request):
        """No project, no lifecycle question — and no extra DB round-trip."""
        from orchestrator.schemas.job_create import JobCreate
        from tests._b09_control_seams import create_job

        fake_request.headers = {}
        user = dict(user_a)
        fake_db.get_user = AsyncMock(return_value={"id": user["id"]})
        # The endpoint redacts the created row before returning it, so the
        # insert has to hand back a row shape and not a bare mock.
        fake_db.create_job = AsyncMock(
            return_value={
                "id": "0f7c9a3e-1d2b-4c5a-8e9f-0a1b2c3d4e5f",
                "user_id": str(user["id"]),
                "project_id": None,
                "description": "personal job",
                "status": "created",
                "context": {},
                "config_override": {},
            }
        )
        body = JobCreate(description="personal job")

        with (
            patch("orchestrator.main.app.state.resources.postgres_db", fake_db),
            patch(
                "orchestrator.security.auth.require_approved_user",
                AsyncMock(return_value=user),
            ),
            patch(
                "orchestrator.application.access.enforce_readiness_gate",
                AsyncMock(return_value=None),
            ),
            patch(
                "orchestrator.services.deployment_gates.is_experts_db_enabled",
                lambda: False,
            ),
        ):
            await create_job(fake_request, body)

        fake_db.create_job.assert_awaited()
        fake_db.get_project.assert_not_awaited()


# =============================================================================
# Automations — cron skips and stays enabled; "Run now" is refused
# =============================================================================


def _automation_row(project_id: str | None = PROJECT_ID) -> dict:
    return {
        "id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        "owner_id": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
        "project_id": project_id,
        "name": "weekly-report",
        "expert": "scholar",
        "prompt": "Draft the weekly report",
        "config_override": {},
        "autonomy": "review",
        "priority": 5,
    }


class TestAutomationFireOnAnArchivedProject:
    @pytest.mark.asyncio
    async def test_service_skips_and_logs_rather_than_raising(self, caplog):
        from orchestrator.services.automations import create_job_from_automation

        db = MagicMock()
        db.create_job = AsyncMock()
        db.get_project = AsyncMock(
            return_value={"id": PROJECT_ID, "status": "archived"}
        )

        with caplog.at_level(logging.INFO, logger="orchestrator.services.automations"):
            job = await create_job_from_automation(db, _automation_row())

        assert job is None
        db.create_job.assert_not_awaited()
        assert any("archived" in record.getMessage() for record in caplog.records)

    @pytest.mark.asyncio
    async def test_run_now_turns_the_skip_into_a_409(self):
        """The service can't raise (its other caller is a cron tick with
        nobody to answer), but an owner clicking a button that silently does
        nothing is worse than a 409 naming the one lever that fixes it."""
        from orchestrator.routers.automations import (
            AutomationsDependencies,
            run_now,
        )

        db = MagicMock()
        db.get_project = AsyncMock(
            return_value={"id": PROJECT_ID, "status": "archived"}
        )
        request = MagicMock()
        # The route takes its store on a dependency object; a ``sys.modules``
        # stub of ``orchestrator.main`` no longer reaches it.
        dependencies = AutomationsDependencies(
            store=db,
            gitea_client=MagicMock(),
            main_cloud_router=MagicMock(),
            trigger_dispatch=MagicMock(),
        )

        with (
            patch(
                "orchestrator.routers.automations.require_approved_user", AsyncMock()
            ),
            patch(
                "orchestrator.routers.automations._resolve_automation_or_404",
                AsyncMock(return_value=_automation_row()),
            ),
            patch(
                "orchestrator.routers.automations.create_job_from_automation",
                AsyncMock(return_value=None),
            ),
            pytest.raises(HTTPException) as exc,
        ):
            await run_now(
                request,
                "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                dependencies=dependencies,
            )

        assert exc.value.status_code == 409
        assert exc.value.detail == access_module.PROJECT_ARCHIVED_DETAIL


# =============================================================================
# Project loops — the sweeper advance has no HTTP caller either
# =============================================================================


class TestLoopMaterializationOnAnArchivedProject:
    def _loop(self) -> dict:
        return {
            "id": "1111aaaa-1111-4111-8111-111111111111",
            "project_id": PROJECT_ID,
            "owner_id": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
            "goal": "improve the thing",
            "role_sequence": ["scholar"],
            "max_iterations": 5,
            "remaining_iterations": 4,
        }

    @pytest.mark.asyncio
    async def test_create_loop_job_skips_and_logs(self, caplog):
        from orchestrator.services.project_loops import create_loop_job

        db = MagicMock()
        db.create_job = AsyncMock()
        db.get_project = AsyncMock(
            return_value={"id": PROJECT_ID, "status": "archived"}
        )

        with caplog.at_level(
            logging.WARNING, logger="orchestrator.services.project_loops"
        ):
            job = await create_loop_job(db, self._loop(), role="scholar", iteration=1)

        assert job is None
        db.create_job.assert_not_awaited()
        assert any("archived" in record.getMessage() for record in caplog.records)


# =============================================================================
# Quiesce on archive — the parked-jobs half (§4.5)
# =============================================================================


def _db_with_conn(conn):
    """A PostgresDB whose ``acquire()`` yields ``conn`` (see
    tests/test_cloud_ro_mounts_db.py, which established this shape)."""
    from contextlib import asynccontextmanager

    from orchestrator.database.postgres import PostgresDB

    db = PostgresDB.__new__(PostgresDB)
    db._pool = MagicMock()
    db._connection_string = "test"
    db._queries = {}

    @asynccontextmanager
    async def acquire():
        yield conn

    db.acquire = acquire
    return db


class TestParkProjectJobsForArchive:
    @pytest.mark.asyncio
    async def test_only_unclaimed_created_or_paused_jobs_are_parked(self):
        import json

        conn = AsyncMock()
        conn.fetch = AsyncMock(return_value=[{"id": "j1"}, {"id": "j2"}])

        parked = await _db_with_conn(conn).park_project_jobs_for_archive(PROJECT_ID)

        assert parked == 2
        sql, project_uuid, freeze = conn.fetch.await_args.args
        assert "status IN ('created', 'paused')" in sql
        # A job the dispatcher already claimed is in-flight work, not new
        # work. Killing it is not what "archive" means.
        assert "assigned_agent_id IS NULL" in sql
        assert "'processing'" not in sql
        # Already-parked rows keep their own freeze reason.
        assert "freeze_data IS NULL" in sql
        # freeze_data non-NULL is exactly what claim_job_for_agent and
        # get_dispatchable_jobs require to be NULL.
        assert json.loads(freeze)["freeze_type"] == "project_archived"

    @pytest.mark.asyncio
    async def test_the_freeze_type_is_in_no_auto_redispatch_set(self):
        """Unarchive must not auto-resume: implicitly re-animating automation
        is where the surprises live."""
        from shared.job_freeze_types import (
            AUTO_CONTINUE_FREEZE_TYPES,
            AUTO_REDISPATCH_FREEZE_TYPES,
            ERROR_IMMUNE_FREEZE_TYPES,
            SUBJOB_REDISPATCH_FREEZE_TYPES,
        )

        for group in (
            AUTO_REDISPATCH_FREEZE_TYPES,
            AUTO_CONTINUE_FREEZE_TYPES,
            ERROR_IMMUNE_FREEZE_TYPES,
            SUBJOB_REDISPATCH_FREEZE_TYPES,
        ):
            assert "project_archived" not in group

    @pytest.mark.asyncio
    async def test_a_malformed_project_id_parks_nothing(self):
        conn = AsyncMock()
        parked = await _db_with_conn(conn).park_project_jobs_for_archive("not-a-uuid")
        assert parked == 0
        conn.fetch.assert_not_awaited()


class TestAgentSubjobFromAThreadOnAnArchivedProject:
    """``_resolve_internal_job_creation_scope`` runs BEFORE ``create_job``'s
    own check, so the archived refusal has to survive its generic wrapper."""

    @pytest.mark.asyncio
    async def test_the_archived_409_is_not_flattened_into_the_generic_403(
        self, user_a, archived, fake_db, fake_request, thread_a
    ):
        from orchestrator.schemas.job_create import JobCreate
        from tests._b09_control_seams import create_job

        fake_request.headers = {"X-Internal-Key": "secret"}
        thread_a["user_id"] = user_a["id"]
        thread_a["project_id"] = archived["id"]
        # The funnel classifies as the thread's OWNER, not the caller, so the
        # owner has to be a real dict or the is_admin branch is taken by
        # accident and the membership half is never exercised.
        fake_db.get_user = AsyncMock(return_value=user_a)
        body = JobCreate(description="subjob", thread_id=str(thread_a["id"]))

        with (
            patch.object(access_module, "_INTERNAL_KEY", "secret"),
            patch("orchestrator.main.app.state.resources.postgres_db", fake_db),
            patch(
                "orchestrator.application.access.enforce_readiness_gate",
                AsyncMock(return_value=None),
            ),
            patch(
                "orchestrator.services.thread_mount_rows.thread_project_ids",
                AsyncMock(return_value=[str(archived["id"])]),
            ),
            pytest.raises(HTTPException) as exc,
        ):
            await create_job(fake_request, body)

        assert exc.value.status_code == 409
        assert exc.value.detail == access_module.PROJECT_ARCHIVED_DETAIL
        fake_db.create_job.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_genuinely_unresolvable_origin_still_gets_the_generic_403(
        self, fake_db, fake_request
    ):
        """The non-disclosure the wrapper exists for must survive intact."""
        from orchestrator.schemas.job_create import JobCreate
        from tests._b09_control_seams import create_job

        fake_request.headers = {"X-Internal-Key": "secret"}
        body = JobCreate(description="originless internal attempt")

        with (
            patch.object(access_module, "_INTERNAL_KEY", "secret"),
            patch("orchestrator.main.app.state.resources.postgres_db", fake_db),
            patch(
                "orchestrator.application.access.enforce_readiness_gate",
                AsyncMock(return_value=None),
            ),
            pytest.raises(HTTPException) as exc,
        ):
            await create_job(fake_request, body)

        assert exc.value.status_code == 403
        assert exc.value.detail == "Internal job origin scope is unavailable"
