"""F5 — multi-tenancy gates on project knowledge endpoints.

Covers:
    GET    /api/projects/{id}/knowledge/summary
    GET    /api/projects/{id}/knowledge
    GET    /api/projects/{id}/knowledge/{note_id}
    POST   /api/projects/{id}/knowledge/search
    PATCH  /api/projects/{id}/knowledge/{note_id}
    DELETE /api/projects/{id}/knowledge/{note_id}
    POST   /api/projects/{id}/knowledge/export

Reads (summary, list, note, search) require viewer-or-higher project
membership; reindex, export, PATCH and DELETE require editor; a PATCH that adds
or removes an officer-only tag (``ready``, ``parallel-safe``) or targets a
charter note requires the owner or an admin (G1/G1b, D9). Tests focus on the
gate itself: cross-user 403, missing-project 404, member-passes-and-hits-the-
inner-code. We don't exercise the vector-DB / Neo4j paths beyond confirming
the gate fired (an inner 500 / 503 after the gate counts as success — it
proves the gate let the caller through).
"""

import logging
from contextlib import ExitStack
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from orchestrator.routers.knowledge import KnowledgeDependencies
from orchestrator.services.kb_task_registry import KbDatasourceTaskRegistry
from orchestrator.services.knowledge_index import KnowledgeIndexDependencies
from orchestrator.services.knowledge_operations import KnowledgeOperationDependencies


_LOGGER = logging.getLogger("tests.knowledge_access")


def _patch_caller(user: dict):
    """Resolve the caller for the real ``require_project_member`` gate.

    The gate itself stays real — it is the subject of this file — and it
    reaches the caller through ``orchestrator.security.access``'s own module
    global, so this one patch is what the whole stack needs. (Before R1.B03
    the same stack also patched ``orchestrator.main.require_approved_user``
    and ``orchestrator.main.postgres_db``; the handlers now take their store
    and their gates through ``KnowledgeDependencies``, so those two patches
    would intercept nothing — the store arrives as ``_deps(db).store`` and the
    gate as the dataclass default, which is the same real function.)
    """
    stack = ExitStack()
    stack.enter_context(
        patch(
            "orchestrator.security.access.require_approved_user",
            AsyncMock(return_value=user),
        )
    )
    return stack


def _deps(db, *, vector_db=None, graph=None) -> KnowledgeDependencies:
    """Router dependencies with the REAL member gate (the dataclass default).

    ``store`` is the auth store the gate reads (formerly
    ``orchestrator.main.postgres_db``); ``vector_db`` and ``graph`` are the
    collaborators the operation reads below the gate (formerly
    ``orchestrator.main.vector_db`` and ``orchestrator.main._get_knowledge_graph``).
    """
    vector = MagicMock() if vector_db is None else vector_db
    return KnowledgeDependencies(
        store=db,
        operations=KnowledgeOperationDependencies(
            store=db,
            vector_db=vector,
            gitea_client=MagicMock(),
            logger=_LOGGER,
            graph=SimpleNamespace(get=lambda: None) if graph is None else graph,
            knowledge_index=KnowledgeIndexDependencies(
                store=db,
                vector_db=vector,
                gitea_client=MagicMock(),
                logger=_LOGGER,
                tasks=KbDatasourceTaskRegistry(),
                inject_system_kb_embedding_profile=AsyncMock(return_value=None),
            ),
        ),
    )


# Endpoints that don't need a body parameter — share negative-path coverage.
_READ_ENDPOINTS = [
    ("get_knowledge_summary", ("project_id",)),
    ("list_knowledge_notes", ("project_id",)),
]


class TestKnowledgeGateNegativePaths:
    """Cross-user 403 and missing-project 404 on every endpoint."""

    @pytest.mark.asyncio
    async def test_summary_cross_user_403(
        self, user_b, project_a, fake_db, fake_request
    ):
        from orchestrator.routers.knowledge import get_knowledge_summary

        with _patch_caller(user_b):
            with pytest.raises(HTTPException) as exc:
                await get_knowledge_summary(
                    fake_request, str(project_a["id"]), dependencies=_deps(fake_db)
                )
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_list_cross_user_403(self, user_b, project_a, fake_db, fake_request):
        from orchestrator.routers.knowledge import list_knowledge_notes

        with _patch_caller(user_b):
            with pytest.raises(HTTPException) as exc:
                await list_knowledge_notes(
                    fake_request, str(project_a["id"]), dependencies=_deps(fake_db)
                )
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_get_note_cross_user_403(
        self, user_b, project_a, fake_db, fake_request
    ):
        from orchestrator.routers.knowledge import get_knowledge_note

        with _patch_caller(user_b):
            with pytest.raises(HTTPException) as exc:
                await get_knowledge_note(
                    fake_request,
                    str(project_a["id"]),
                    "note-1",
                    dependencies=_deps(fake_db),
                )
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_search_cross_user_403(
        self, user_b, project_a, fake_db, fake_request
    ):
        from orchestrator.routers.knowledge import search_knowledge
        from orchestrator.schemas.knowledge import KnowledgeSearchRequest

        with _patch_caller(user_b):
            with pytest.raises(HTTPException) as exc:
                await search_knowledge(
                    fake_request,
                    str(project_a["id"]),
                    KnowledgeSearchRequest(query="anything"),
                    dependencies=_deps(fake_db),
                )
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_patch_note_cross_user_403(
        self, user_b, project_a, fake_db, fake_request
    ):
        from orchestrator.routers.knowledge import update_knowledge_note
        from orchestrator.schemas.knowledge import KnowledgeNoteUpdate

        with _patch_caller(user_b):
            with pytest.raises(HTTPException) as exc:
                await update_knowledge_note(
                    fake_request,
                    str(project_a["id"]),
                    "note-1",
                    KnowledgeNoteUpdate(status="resolved"),
                    dependencies=_deps(fake_db),
                )
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_delete_note_cross_user_403(
        self, user_b, project_a, fake_db, fake_request
    ):
        from orchestrator.routers.knowledge import delete_knowledge_note

        with _patch_caller(user_b):
            with pytest.raises(HTTPException) as exc:
                await delete_knowledge_note(
                    fake_request,
                    str(project_a["id"]),
                    "note-1",
                    dependencies=_deps(fake_db),
                )
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_export_cross_user_403(
        self, user_b, project_a, fake_db, fake_request
    ):
        from orchestrator.routers.knowledge import export_knowledge

        with _patch_caller(user_b):
            with pytest.raises(HTTPException) as exc:
                await export_knowledge(
                    fake_request, str(project_a["id"]), dependencies=_deps(fake_db)
                )
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_missing_project_404(self, user_a, fake_db, fake_request):
        """The gate raises 404 before any knowledge query runs."""
        from orchestrator.routers.knowledge import get_knowledge_summary

        with _patch_caller(user_a):
            with pytest.raises(HTTPException) as exc:
                await get_knowledge_summary(
                    fake_request,
                    "ffffffff-ffff-ffff-ffff-ffffffffffff",
                    dependencies=_deps(fake_db),
                )
        assert exc.value.status_code == 404


# =============================================================================
# Positive paths: member passes the gate. We don't exercise the vector-DB
# code below the gate; an inner exception confirms the gate let the caller
# through (anything other than 403/404 means we made it past the gate).
# =============================================================================


def _make_dud_vector_db():
    """Stand-in for ``vector_db``. ``acquire()`` returns a context manager
    whose ``__aenter__`` raises, so the inner code can't query but we
    *did* reach the inner code — proves the gate let us through."""
    dud = AsyncMock()
    cm = AsyncMock()
    cm.__aenter__.side_effect = RuntimeError("vector_db patched out")
    dud.acquire = lambda: cm
    return dud


class TestKnowledgeGatePositivePaths:
    @pytest.mark.asyncio
    async def test_summary_member_passes_gate(
        self, user_a, project_a, fake_db, fake_request
    ):
        from orchestrator.routers.knowledge import get_knowledge_summary

        deps = _deps(fake_db, vector_db=_make_dud_vector_db())
        with _patch_caller(user_a):
            with pytest.raises(HTTPException) as exc:
                await get_knowledge_summary(
                    fake_request, str(project_a["id"]), dependencies=deps
                )
        # 500 = gate passed, vector_db blew up as planned. NOT 403/404.
        assert exc.value.status_code == 500

    @pytest.mark.asyncio
    async def test_admin_bypasses_membership(
        self, user_admin, project_a, fake_db, fake_request
    ):
        from orchestrator.routers.knowledge import list_knowledge_notes

        deps = _deps(fake_db, vector_db=_make_dud_vector_db())
        with _patch_caller(user_admin):
            with pytest.raises(HTTPException) as exc:
                await list_knowledge_notes(
                    fake_request, str(project_a["id"]), dependencies=deps
                )
        # Same shape — admin made it past, inner code failed by design.
        assert exc.value.status_code == 500

    @pytest.mark.asyncio
    async def test_get_note_member_passes_gate(
        self, user_a, project_a, fake_db, fake_request
    ):
        from orchestrator.routers.knowledge import get_knowledge_note

        deps = _deps(fake_db, vector_db=_make_dud_vector_db())
        with _patch_caller(user_a):
            with pytest.raises(HTTPException) as exc:
                await get_knowledge_note(
                    fake_request, str(project_a["id"]), "note-1", dependencies=deps
                )
        assert exc.value.status_code == 500

    @pytest.mark.asyncio
    async def test_search_member_passes_gate(
        self, user_a, project_a, fake_db, fake_request
    ):
        from orchestrator.routers.knowledge import search_knowledge
        from orchestrator.schemas.knowledge import KnowledgeSearchRequest

        deps = _deps(fake_db, vector_db=_make_dud_vector_db())
        with _patch_caller(user_a):
            with pytest.raises(HTTPException) as exc:
                await search_knowledge(
                    fake_request,
                    str(project_a["id"]),
                    KnowledgeSearchRequest(query="q"),
                    dependencies=deps,
                )
        assert exc.value.status_code == 500

    @pytest.mark.asyncio
    async def test_patch_note_member_passes_gate(
        self, user_a, project_a, fake_db, fake_request
    ):
        from orchestrator.routers.knowledge import update_knowledge_note
        from orchestrator.schemas.knowledge import KnowledgeNoteUpdate

        deps = _deps(fake_db, vector_db=_make_dud_vector_db())
        with _patch_caller(user_a):
            with pytest.raises(HTTPException) as exc:
                await update_knowledge_note(
                    fake_request,
                    str(project_a["id"]),
                    "note-1",
                    KnowledgeNoteUpdate(status="resolved"),
                    dependencies=deps,
                )
        assert exc.value.status_code == 500

    @pytest.mark.asyncio
    async def test_delete_note_member_passes_gate(
        self, user_a, project_a, fake_db, fake_request
    ):
        from orchestrator.routers.knowledge import delete_knowledge_note

        deps = _deps(fake_db, vector_db=_make_dud_vector_db())
        with _patch_caller(user_a):
            with pytest.raises(HTTPException) as exc:
                await delete_knowledge_note(
                    fake_request, str(project_a["id"]), "note-1", dependencies=deps
                )
        # The gate passed (not 403); the mutation then failed downstream. Since
        # kb_gardening G2 the delete goes through the ledgered materialize op
        # first, whose failure surfaces as 502 (intent store unavailable here).
        assert exc.value.status_code in (500, 502)

    @pytest.mark.asyncio
    async def test_export_member_passes_gate(
        self, user_a, project_a, fake_db, fake_request
    ):
        """Export checks Neo4j availability *after* the gate. Without
        the graph attached it returns 503 — that confirms the gate let
        us through."""
        from orchestrator.routers.knowledge import export_knowledge

        deps = _deps(fake_db, graph=SimpleNamespace(get=lambda: None))
        with _patch_caller(user_a):
            with pytest.raises(HTTPException) as exc:
                await export_knowledge(
                    fake_request, str(project_a["id"]), dependencies=deps
                )
        assert exc.value.status_code == 503


def _as_member(fake_db, role: str) -> None:
    """Give every non-admin caller ``role`` in every project."""
    fake_db.get_user_role_in_project = AsyncMock(return_value=role)


class TestKnowledgeRoleTiers:
    """A viewer reads; an editor mutates; dispatch tags need the owner."""

    @pytest.mark.asyncio
    async def test_viewer_cannot_delete_reindex_export_or_patch(
        self, user_b, project_a, fake_db, fake_request
    ):
        from orchestrator.routers.knowledge import (
            delete_knowledge_note,
            export_knowledge,
            reindex_knowledge,
            update_knowledge_note,
        )
        from orchestrator.schemas.knowledge import KnowledgeNoteUpdate

        _as_member(fake_db, "viewer")
        deps = _deps(fake_db, vector_db=_make_dud_vector_db())
        pid = str(project_a["id"])
        calls = [
            lambda: delete_knowledge_note(fake_request, pid, "n", dependencies=deps),
            lambda: reindex_knowledge(fake_request, pid, dependencies=deps),
            lambda: export_knowledge(fake_request, pid, dependencies=deps),
            lambda: update_knowledge_note(
                fake_request,
                pid,
                "n",
                KnowledgeNoteUpdate(status="resolved"),
                dependencies=deps,
            ),
        ]
        with _patch_caller(user_b):
            for call in calls:
                with pytest.raises(HTTPException) as exc:
                    await call()
                assert exc.value.status_code == 403
                assert "editor" in str(exc.value.detail)

    @pytest.mark.asyncio
    async def test_viewer_still_searches(
        self, user_b, project_a, fake_db, fake_request
    ):
        from orchestrator.routers.knowledge import search_knowledge
        from orchestrator.schemas.knowledge import KnowledgeSearchRequest

        _as_member(fake_db, "viewer")
        deps = _deps(fake_db, vector_db=_make_dud_vector_db())
        with _patch_caller(user_b):
            with pytest.raises(HTTPException) as exc:
                await search_knowledge(
                    fake_request,
                    str(project_a["id"]),
                    KnowledgeSearchRequest(query="q"),
                    dependencies=deps,
                )
        assert exc.value.status_code == 500  # past the gate

    @pytest.mark.asyncio
    async def test_editor_passes_the_gate_for_a_status_change(
        self, user_b, project_a, fake_db, fake_request
    ):
        from orchestrator.routers.knowledge import update_knowledge_note
        from orchestrator.schemas.knowledge import KnowledgeNoteUpdate

        _as_member(fake_db, "editor")
        deps = _deps(fake_db, vector_db=_make_dud_vector_db())
        with _patch_caller(user_b):
            with pytest.raises(HTTPException) as exc:
                await update_knowledge_note(
                    fake_request,
                    str(project_a["id"]),
                    "n",
                    KnowledgeNoteUpdate(status="resolved"),
                    dependencies=deps,
                )
        assert exc.value.status_code == 500  # past the gate

    @pytest.mark.asyncio
    async def test_editor_cannot_arm_ready(
        self, user_b, project_a, fake_db, fake_request
    ):
        from orchestrator.routers.knowledge import update_knowledge_note
        from orchestrator.schemas.knowledge import KnowledgeNoteUpdate

        _as_member(fake_db, "editor")
        deps = _deps(fake_db, vector_db=_make_dud_vector_db())
        with _patch_caller(user_b):
            with pytest.raises(HTTPException) as exc:
                await update_knowledge_note(
                    fake_request,
                    str(project_a["id"]),
                    "n",
                    KnowledgeNoteUpdate(add_tags=["ready"]),
                    dependencies=deps,
                )
        assert exc.value.status_code == 403
        assert "ready" in exc.value.detail

    @pytest.mark.asyncio
    async def test_owner_may_arm_ready(self, user_a, project_a, fake_db, fake_request):
        from orchestrator.routers.knowledge import update_knowledge_note
        from orchestrator.schemas.knowledge import KnowledgeNoteUpdate

        deps = _deps(fake_db, vector_db=_make_dud_vector_db())
        with _patch_caller(user_a):  # user_a owns project_a
            with pytest.raises(HTTPException) as exc:
                await update_knowledge_note(
                    fake_request,
                    str(project_a["id"]),
                    "n",
                    KnowledgeNoteUpdate(add_tags=["ready"]),
                    dependencies=deps,
                )
        assert exc.value.status_code == 500  # past both checks


class TestInternalServiceGate:
    """G2: the KB write lane refuses a call the MCP server makes for a user."""

    def _request(self, headers: dict):
        return SimpleNamespace(headers=headers)

    @pytest.mark.asyncio
    async def test_bare_internal_key_passes(self):
        from orchestrator.security import access

        with patch.object(access, "_INTERNAL_KEY", "k"):
            await access.require_internal_service(
                self._request({"X-Internal-Key": "k"})
            )

    @pytest.mark.asyncio
    async def test_delegated_user_is_refused(self):
        from orchestrator.security import access

        request = self._request({"X-Internal-Key": "k", "X-MCP-User-Id": "u-1"})
        with patch.object(access, "_INTERNAL_KEY", "k"):
            with pytest.raises(HTTPException) as exc:
                await access.require_internal_service(request)
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_missing_or_wrong_key_is_401(self):
        from orchestrator.security import access

        with patch.object(access, "_INTERNAL_KEY", "k"):
            for headers in ({}, {"X-Internal-Key": "wrong"}):
                with pytest.raises(HTTPException) as exc:
                    await access.require_internal_service(self._request(headers))
                assert exc.value.status_code == 401

    def test_the_kb_write_lane_uses_it(self):
        from orchestrator.routers.knowledge import KnowledgeDependencies
        from orchestrator.security.access import require_internal_service

        assert (
            KnowledgeDependencies.__dataclass_fields__[
                "require_internal_service"
            ].default
            is require_internal_service
        )
