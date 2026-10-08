"""R1.B06 lane B — project attachment verdicts, authorization and KB scope.

The refusal shapes are the contract here: one generic 403 for a mixed denial
(so the endpoint is not a project-existence oracle), a 409 only when *every*
denial is ``archived``, and an acknowledged id that is dropped ONLY while it is
still denied.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException

from orchestrator.schemas.thread_admission import ThreadCreateRequest
from orchestrator.security.access import PROJECT_ARCHIVED_DETAIL
from orchestrator.services.thread_project_authorization import (
    ProjectVerdict,
    ThreadProjectAuthorizationDependencies,
    authorize_thread_project_ids,
    classify_thread_project_ids,
    revalidate_thread_project_ids,
    thread_creation_project_ids,
    thread_has_knowledge_scope,
)

P1 = "11111111-1111-4111-8111-111111111111"
P2 = "22222222-2222-4222-8222-222222222222"
USER = {"id": "u-1", "is_admin": False}


def _deps(**store: Any) -> ThreadProjectAuthorizationDependencies:
    defaults: dict[str, Any] = {
        "get_user": AsyncMock(return_value=USER),
        "get_project": AsyncMock(return_value={"id": P1}),
        "get_user_role_in_project": AsyncMock(return_value="editor"),
        "get_datasource": AsyncMock(return_value=None),
    }
    defaults.update(store)
    return ThreadProjectAuthorizationDependencies(store=SimpleNamespace(**defaults))


class TestClassify:
    @pytest.mark.asyncio
    async def test_missing_project_is_deleted(self):
        deps = _deps(get_project=AsyncMock(return_value=None))
        assert await classify_thread_project_ids(USER, [P1], dependencies=deps) == [
            ProjectVerdict(P1, True, "deleted")
        ]

    @pytest.mark.asyncio
    async def test_absent_role_is_revoked_and_outranks_archived(self):
        """Authorization outranks lifecycle: a non-member never learns archived."""
        deps = _deps(
            get_project=AsyncMock(return_value={"id": P1, "status": "archived"}),
            get_user_role_in_project=AsyncMock(return_value=None),
        )
        verdicts = await classify_thread_project_ids(USER, [P1], dependencies=deps)
        assert verdicts == [ProjectVerdict(P1, True, "revoked")]

    @pytest.mark.asyncio
    async def test_admin_skips_the_membership_read_but_still_sees_archived(self):
        role = AsyncMock(return_value=None)
        deps = _deps(
            get_project=AsyncMock(return_value={"id": P1, "status": "archived"}),
            get_user_role_in_project=role,
        )
        verdicts = await classify_thread_project_ids(
            {"id": "admin", "is_admin": True}, [P1], dependencies=deps
        )
        assert verdicts == [ProjectVerdict(P1, True, "archived")]
        role.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_duplicates_collapse_and_order_is_preserved(self):
        deps = _deps()
        verdicts = await classify_thread_project_ids(
            USER, [P2, P1, P2], dependencies=deps
        )
        assert [v.project_id for v in verdicts] == [P2, P1]


class TestAuthorize:
    @pytest.mark.asyncio
    async def test_empty_selection_needs_no_store_read(self):
        get_project = AsyncMock()
        deps = _deps(get_project=get_project)
        assert await authorize_thread_project_ids(USER, [], dependencies=deps) == []
        get_project.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_all_archived_is_409_with_the_actionable_detail(self):
        deps = _deps(
            get_project=AsyncMock(return_value={"id": P1, "status": "archived"})
        )
        with pytest.raises(HTTPException) as exc:
            await authorize_thread_project_ids(USER, [P1, P2], dependencies=deps)
        assert exc.value.status_code == 409
        assert exc.value.detail == PROJECT_ARCHIVED_DETAIL

    @pytest.mark.asyncio
    async def test_a_mixed_denial_stays_the_generic_403(self):
        """One non-archived denial must not be reported as an archive problem."""

        async def _project(pid: str) -> dict[str, Any] | None:
            return {"id": pid, "status": "archived"} if pid == P1 else None

        deps = _deps(get_project=AsyncMock(side_effect=_project))
        with pytest.raises(HTTPException) as exc:
            await authorize_thread_project_ids(USER, [P1, P2], dependencies=deps)
        assert exc.value.status_code == 403
        assert exc.value.detail == "One or more attached projects are unavailable"


class TestRevalidate:
    @pytest.mark.asyncio
    async def test_userless_thread_keeps_trusted_passthrough_with_dedup(self):
        get_user = AsyncMock()
        deps = _deps(get_user=get_user)
        assert await revalidate_thread_project_ids(
            {"id": "t", "user_id": None}, [P1, P1, P2], dependencies=deps
        ) == [P1, P2]
        get_user.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_legacy_two_project_session_still_revalidates_both(self):
        """Sessions created before the one-project rule keep resuming: the
        rule lives in thread_creation_project_ids, not on this path."""
        deps = _deps()
        assert await revalidate_thread_project_ids(
            {"id": "t", "user_id": "u-1", "metadata": {}}, [P1, P2], dependencies=deps
        ) == [P1, P2]

    @pytest.mark.asyncio
    async def test_vanished_owner_fails_closed_with_the_generic_403(self):
        deps = _deps(get_user=AsyncMock(return_value=None))
        with pytest.raises(HTTPException) as exc:
            await revalidate_thread_project_ids(
                {"id": "t", "user_id": "gone"}, [P1], dependencies=deps
            )
        assert exc.value.status_code == 403
        assert exc.value.detail == "One or more attached projects are unavailable"

    @pytest.mark.asyncio
    async def test_acknowledged_id_is_dropped_only_while_still_denied(self):
        thread = {
            "id": "t",
            "user_id": "u-1",
            "metadata": {"config_drift_ack": {f"project:{P1}": {}}},
        }
        deps = _deps(get_project=AsyncMock(return_value=None))
        # P1 is acknowledged AND still denied -> narrowed out, leaving nothing.
        assert (
            await revalidate_thread_project_ids(thread, [P1], dependencies=deps) == []
        )

    @pytest.mark.asyncio
    async def test_acknowledged_id_returns_automatically_once_available(self):
        thread = {
            "id": "t",
            "user_id": "u-1",
            "metadata": {"config_drift_ack": {f"project:{P1}": {}}},
        }
        deps = _deps()  # clean verdict
        assert await revalidate_thread_project_ids(thread, [P1], dependencies=deps) == [
            P1
        ]

    @pytest.mark.asyncio
    async def test_unacknowledged_denial_still_fails_the_whole_selection_closed(self):
        thread = {
            "id": "t",
            "user_id": "u-1",
            "metadata": {"config_drift_ack": {f"project:{P1}": {}}},
        }

        async def _project(pid: str) -> dict[str, Any] | None:
            return None if pid == P2 else {"id": pid}

        deps = _deps(get_project=AsyncMock(side_effect=_project))
        with pytest.raises(HTTPException) as exc:
            await revalidate_thread_project_ids(thread, [P1, P2], dependencies=deps)
        assert exc.value.status_code == 403


ONE_PROJECT_DETAIL = (
    "A Session can belong to one project at most. Pick one project, or none."
)


class TestCreationProjectIds:
    def test_no_project_is_allowed(self):
        assert thread_creation_project_ids(ThreadCreateRequest(), USER) == []
        body = ThreadCreateRequest(project_ids=[])
        assert thread_creation_project_ids(body, USER) == []

    def test_one_project_is_allowed(self):
        body = ThreadCreateRequest(project_ids=[P1])
        assert thread_creation_project_ids(body, USER) == [P1]

    def test_legacy_project_id_is_appended_once(self):
        body = ThreadCreateRequest(project_ids=[P1], project_id=P1)
        assert thread_creation_project_ids(body, USER) == [P1]

    def test_two_projects_are_refused_with_the_rule(self):
        body = ThreadCreateRequest(project_ids=[P1, P2])
        with pytest.raises(HTTPException) as exc:
            thread_creation_project_ids(body, USER)
        assert exc.value.status_code == 422
        assert exc.value.detail == ONE_PROJECT_DETAIL

    def test_legacy_project_id_cannot_add_a_second_project(self):
        body = ThreadCreateRequest(project_ids=[P2], project_id=P1)
        with pytest.raises(HTTPException) as exc:
            thread_creation_project_ids(body, USER)
        assert exc.value.status_code == 422
        assert exc.value.detail == ONE_PROJECT_DETAIL

    def test_a_repeated_project_counts_once(self):
        body = ThreadCreateRequest(project_ids=[P1, P1])
        assert thread_creation_project_ids(body, USER) == [P1]

    def test_mcp_scope_binds_on_omission(self):
        user = {"id": "u", "scopes": [f"project:{P1}"]}
        assert thread_creation_project_ids(ThreadCreateRequest(), user) == [P1]

    def test_mcp_scope_refuses_a_different_project(self):
        user = {"id": "u", "scopes": [f"project:{P1}"]}
        with pytest.raises(HTTPException) as exc:
            thread_creation_project_ids(ThreadCreateRequest(project_ids=[P2]), user)
        assert exc.value.status_code == 403
        assert exc.value.detail == "Access denied by MCP token scope"

    def test_mcp_scope_refuses_an_additional_project(self):
        """The token-scope 403 outranks the one-project 422: a scoped token
        naming its own project plus another learns only that the scope
        refused it."""
        user = {"id": "u", "scopes": [f"project:{P1}"]}
        with pytest.raises(HTTPException) as exc:
            thread_creation_project_ids(ThreadCreateRequest(project_ids=[P1, P2]), user)
        assert exc.value.status_code == 403
        assert exc.value.detail == "Access denied by MCP token scope"


class TestKnowledgeScope:
    @pytest.mark.asyncio
    async def test_project_scope_alone_opts_in_without_reading_connectors(self):
        get_datasource = AsyncMock()
        deps = _deps(get_datasource=get_datasource)
        assert await thread_has_knowledge_scope(
            project_ids=[P1], datasource_ids=["d"], dependencies=deps
        )
        get_datasource.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_kb_connector_opts_in(self):
        deps = _deps(get_datasource=AsyncMock(return_value={"type": "kb"}))
        assert await thread_has_knowledge_scope(
            project_ids=[], datasource_ids=["d"], dependencies=deps
        )

    @pytest.mark.asyncio
    async def test_the_type_matches_exactly_like_the_driver_registry(self):
        # Every write path stores the driver's own lowercase type; a type the
        # registry would not serve gets no system key either.
        deps = _deps(get_datasource=AsyncMock(return_value={"type": "KB"}))
        assert not await thread_has_knowledge_scope(
            project_ids=[], datasource_ids=["d"], dependencies=deps
        )

    @pytest.mark.asyncio
    async def test_unrelated_connector_keeps_the_system_key_out(self):
        deps = _deps(get_datasource=AsyncMock(return_value={"type": "database"}))
        assert not await thread_has_knowledge_scope(
            project_ids=None, datasource_ids=["d"], dependencies=deps
        )
