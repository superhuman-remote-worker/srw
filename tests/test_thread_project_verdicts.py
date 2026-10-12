"""Per-item project attachment verdicts (config-drift reporting layer)."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException

from orchestrator.services.thread_project_authorization import ProjectVerdict
from tests import _b09_control_seams as control_seams
from orchestrator.application import sessions as sessions_composition
from orchestrator.services import (
    thread_project_authorization as thread_project_authorization_module,
)


USER = {"id": "11111111-1111-4111-8111-111111111111", "is_admin": False}
PROJECT_ALIVE = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
PROJECT_GONE = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
PROJECT_NO_ROLE = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"


@pytest.mark.asyncio
async def test_classify_projects_reports_deleted_and_revoked():
    projects = {
        PROJECT_ALIVE: {"id": PROJECT_ALIVE},
        PROJECT_NO_ROLE: {"id": PROJECT_NO_ROLE},
    }
    roles = {PROJECT_ALIVE: "owner"}

    with patch("orchestrator.main.app.state.resources.postgres_db") as db:
        db.get_project = AsyncMock(side_effect=lambda pid: projects.get(pid))
        db.get_user_role_in_project = AsyncMock(
            side_effect=lambda pid, uid: roles.get(pid)
        )

        verdicts = await control_seams.classify_thread_project_ids(
            USER, [PROJECT_ALIVE, PROJECT_GONE, PROJECT_NO_ROLE]
        )

    assert verdicts == [
        ProjectVerdict(PROJECT_ALIVE, False, None),
        ProjectVerdict(PROJECT_GONE, True, "deleted"),
        ProjectVerdict(PROJECT_NO_ROLE, True, "revoked"),
    ]


@pytest.mark.asyncio
async def test_admin_is_allowed_on_any_existing_project():
    with patch("orchestrator.main.app.state.resources.postgres_db") as db:
        db.get_project = AsyncMock(return_value={"id": PROJECT_ALIVE})
        db.get_user_role_in_project = AsyncMock(return_value=None)

        verdicts = await control_seams.classify_thread_project_ids(
            {"id": USER["id"], "is_admin": True}, [PROJECT_ALIVE]
        )

    assert verdicts == [ProjectVerdict(PROJECT_ALIVE, False, None)]


@pytest.mark.asyncio
async def test_admin_still_denied_on_deleted_project():
    with patch("orchestrator.main.app.state.resources.postgres_db") as db:
        db.get_project = AsyncMock(return_value=None)
        db.get_user_role_in_project = AsyncMock(return_value=None)

        verdicts = await control_seams.classify_thread_project_ids(
            {"id": USER["id"], "is_admin": True}, [PROJECT_GONE]
        )

    assert verdicts == [ProjectVerdict(PROJECT_GONE, True, "deleted")]


PROJECT_ARCHIVED = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"


# ---------------------------------------------------------------------------
# Archived — the lifecycle verdict
# ---------------------------------------------------------------------------
#
# knowledge-base/knowledge/features/project_and_job_list_filtering.md §4.3.
# This funnel is the only path POST /api/persistent/threads takes, and — for
# free — the one _resolve_internal_job_creation_scope takes for agent-spawned
# subjobs, so the verdict covers both.


@pytest.mark.asyncio
async def test_archived_project_is_denied():
    with patch("orchestrator.main.app.state.resources.postgres_db") as db:
        db.get_project = AsyncMock(
            return_value={"id": PROJECT_ARCHIVED, "status": "archived"}
        )
        db.get_user_role_in_project = AsyncMock(return_value="owner")

        verdicts = await control_seams.classify_thread_project_ids(
            USER, [PROJECT_ARCHIVED]
        )

    assert verdicts == [ProjectVerdict(PROJECT_ARCHIVED, True, "archived")]


@pytest.mark.asyncio
async def test_admin_is_denied_on_an_archived_project_too():
    # Admin bypasses authorization, not the lifecycle.
    with patch("orchestrator.main.app.state.resources.postgres_db") as db:
        db.get_project = AsyncMock(
            return_value={"id": PROJECT_ARCHIVED, "status": "archived"}
        )
        db.get_user_role_in_project = AsyncMock(return_value=None)

        verdicts = await control_seams.classify_thread_project_ids(
            {"id": USER["id"], "is_admin": True}, [PROJECT_ARCHIVED]
        )

    assert verdicts == [ProjectVerdict(PROJECT_ARCHIVED, True, "archived")]


@pytest.mark.asyncio
async def test_revoked_outranks_archived():
    # A caller with no membership must not learn that the project happens to
    # be archived — that would make the verdict an existence oracle.
    with patch("orchestrator.main.app.state.resources.postgres_db") as db:
        db.get_project = AsyncMock(
            return_value={"id": PROJECT_ARCHIVED, "status": "archived"}
        )
        db.get_user_role_in_project = AsyncMock(return_value=None)

        verdicts = await control_seams.classify_thread_project_ids(
            USER, [PROJECT_ARCHIVED]
        )

    assert verdicts == [ProjectVerdict(PROJECT_ARCHIVED, True, "revoked")]


@pytest.mark.asyncio
async def test_null_status_is_not_archived():
    with patch("orchestrator.main.app.state.resources.postgres_db") as db:
        db.get_project = AsyncMock(return_value={"id": PROJECT_ALIVE, "status": None})
        db.get_user_role_in_project = AsyncMock(return_value="editor")

        verdicts = await control_seams.classify_thread_project_ids(
            USER, [PROJECT_ALIVE]
        )

    assert verdicts == [ProjectVerdict(PROJECT_ALIVE, False, None)]


@pytest.mark.asyncio
async def test_archived_is_acknowledgeable_so_a_live_session_can_resume():
    """Without this, blocking_denials() would route an archived attachment
    through the "invalid rather than merely unavailable" refusal — telling the
    owner their session is corrupt when their project was merely archived."""
    from orchestrator.services.config_drift import (
        ACKNOWLEDGEABLE_REASONS,
        blocking_denials,
    )

    assert "archived" in ACKNOWLEDGEABLE_REASONS
    assert (
        blocking_denials([], [ProjectVerdict(PROJECT_ARCHIVED, True, "archived")]) == []
    )


# ---------------------------------------------------------------------------
# _authorize_thread_project_ids — the enforcing wrapper
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_authorize_raises_409_when_every_denial_is_archived():
    import orchestrator.main
    from orchestrator.security.access import PROJECT_ARCHIVED_DETAIL

    with patch("orchestrator.main.app.state.resources.postgres_db") as db:
        db.get_project = AsyncMock(
            return_value={"id": PROJECT_ARCHIVED, "status": "archived"}
        )
        db.get_user_role_in_project = AsyncMock(return_value="owner")

        with pytest.raises(HTTPException) as exc:
            await thread_project_authorization_module.authorize_thread_project_ids(
                USER,
                [PROJECT_ARCHIVED],
                dependencies=sessions_composition.thread_project_authorization_dependencies(
                    orchestrator.main.app.state.resources
                ),
            )

    # The caller is a member of every archived project named, so nothing is
    # disclosed by saying which lever to pull.
    assert exc.value.status_code == 409
    assert exc.value.detail == PROJECT_ARCHIVED_DETAIL


@pytest.mark.asyncio
async def test_a_mixed_denial_keeps_the_non_disclosing_403():
    import orchestrator.main

    projects = {PROJECT_ARCHIVED: {"id": PROJECT_ARCHIVED, "status": "archived"}}
    with patch("orchestrator.main.app.state.resources.postgres_db") as db:
        db.get_project = AsyncMock(side_effect=lambda pid: projects.get(pid))
        db.get_user_role_in_project = AsyncMock(return_value="owner")

        with pytest.raises(HTTPException) as exc:
            await thread_project_authorization_module.authorize_thread_project_ids(
                USER,
                [PROJECT_ARCHIVED, PROJECT_GONE],
                dependencies=sessions_composition.thread_project_authorization_dependencies(
                    orchestrator.main.app.state.resources
                ),
            )

    assert exc.value.status_code == 403
    assert exc.value.detail == "One or more attached projects are unavailable"
