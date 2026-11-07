"""HTTP adapters for the project knowledge base.

The application owns stores and lifecycle; each request resolves its factory
from the app handling it, so importing this router never imports application
startup.

Route order is load-bearing: ``/knowledge/summary`` is declared before
``/knowledge/{note_id}``, or the note-id route swallows it.

Gate tiers: reads are ``require_project_member`` at viewer; reindex, export,
PATCH and DELETE need editor (a viewer used to be able to purge notes and arm
``ready``, G1/G1b); adding or removing an officer-only tag (``ready``,
``parallel-safe``) or changing a charter note needs the project owner or an
admin (D9). ``materialize``, its projection report and the purge lane are
``require_internal_service``: the bare ``X-Internal-Key`` of an agent/operator
caller, refused when the MCP server forwards a delegated user (G2). A PATCH
additionally refuses an archived project because editing a note is a content
mutation while deleting one is teardown.

Design: knowledge-base/knowledge/features/knowledge_base_mcp_for_chat_uis.md §4.1.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Awaitable, Callable

from fastapi import APIRouter, Depends, Query, Request

from orchestrator.schemas.knowledge import (
    KnowledgeDeleteRequest,
    KnowledgeMaterializeRequest,
    KnowledgeNoteUpdate,
    KnowledgeProjectionRequest,
    KnowledgeSearchRequest,
)
from orchestrator.security.access import (
    require_internal_service,
    require_project_member,
)
from orchestrator.services import knowledge_operations

router = APIRouter()


@dataclass(frozen=True)
class KnowledgeDependencies:
    """Per-app auth store, operations and explicit authorization gates."""

    store: Any
    operations: knowledge_operations.KnowledgeOperationDependencies
    require_project_member: Callable[..., Awaitable[Any]] = require_project_member
    require_internal_service: Callable[..., Awaitable[Any]] = require_internal_service


def get_knowledge_dependencies(request: Request) -> KnowledgeDependencies:
    """Resolve collaborators only from the application handling this request."""
    return request.app.state.knowledge_dependencies_factory()


@router.get("/api/projects/{project_id}/knowledge/summary")
async def get_knowledge_summary(
    request: Request,
    project_id: str,
    *,
    dependencies: KnowledgeDependencies = Depends(get_knowledge_dependencies),
) -> dict[str, Any]:
    """Get knowledge base summary statistics for a project. F5: member-only."""
    await dependencies.require_project_member(request, dependencies.store, project_id)

    return await knowledge_operations.get_knowledge_summary(
        project_id, dependencies=dependencies.operations
    )


@router.get("/api/projects/{project_id}/knowledge")
async def list_knowledge_notes(
    request: Request,
    project_id: str,
    note_type: str | None = Query(default=None, alias="type"),
    status: str | None = Query(default=None),
    tag: str | None = Query(default=None),
    job_id: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    *,
    dependencies: KnowledgeDependencies = Depends(get_knowledge_dependencies),
) -> dict[str, Any]:
    """List knowledge notes for a project with optional filters. F5: member-only."""
    await dependencies.require_project_member(request, dependencies.store, project_id)

    return await knowledge_operations.list_knowledge_notes(
        project_id,
        note_type=note_type,
        status=status,
        tag=tag,
        job_id=job_id,
        limit=limit,
        offset=offset,
        dependencies=dependencies.operations,
    )


@router.get("/api/projects/{project_id}/knowledge/{note_id}")
async def get_knowledge_note(
    request: Request,
    project_id: str,
    note_id: str,
    *,
    dependencies: KnowledgeDependencies = Depends(get_knowledge_dependencies),
) -> dict[str, Any]:
    """Get a single knowledge note with full content. F5: member-only."""
    await dependencies.require_project_member(request, dependencies.store, project_id)
    return await knowledge_operations.get_knowledge_note(
        project_id, note_id, dependencies=dependencies.operations
    )


@router.post("/api/projects/{project_id}/knowledge/reindex")
async def reindex_knowledge(
    request: Request,
    project_id: str,
    full: bool = False,
    *,
    dependencies: KnowledgeDependencies = Depends(get_knowledge_dependencies),
) -> dict[str, Any]:
    """Rebuild/refresh the KB chunk index from the vault repo (slice 3 PR3).

    The ``kb reindex --full`` operator hatch (§5): ``full=true`` re-embeds the
    whole vault (model/chunker migration, corrupt-index recovery); the default
    incremental run only touches notes whose git blob changed. Editor or
    higher: a full run re-embeds the whole vault.
    """
    await dependencies.require_project_member(
        request, dependencies.store, project_id, min_role="editor"
    )
    return await knowledge_operations.reindex_knowledge(
        project_id, full=full, dependencies=dependencies.operations
    )


@router.post("/api/projects/{project_id}/knowledge/materialize")
async def materialize_knowledge_note(
    request: Request,
    project_id: str,
    body: KnowledgeMaterializeRequest,
    *,
    dependencies: KnowledgeDependencies = Depends(get_knowledge_dependencies),
) -> dict[str, Any]:
    """Commit one rendered note to ``knowledge/<slug>.md`` in the project's KB
    repo. **Internal** (P4b) — requires ``X-Internal-Key``.

    Step 3 of knowledge-base/knowledge/features/knowledge_base_repo_separation.md: the agent used
    to write this file into its workspace checkout, which welded the vault to
    the jobs repo and skipped entirely wherever there was no git (persistent
    sessions, lite tiers, repo-less projects). The write is server-side now —
    one note, one commit, into whichever repo ``resolve_kb_repo`` picks.

    Always answers 200 with the service's ``{status, reason, repo, branch,
    path, operation, indexed, index_reason}``; the failure vocabulary lives in
    the body, not the HTTP code. ``indexed`` reports whether the note is
    searchable already — ``false`` with an ``index_reason`` means the commit
    landed and the sweep will finish the job.
    """
    await dependencies.require_internal_service(request)
    return await knowledge_operations.materialize_knowledge_note(
        project_id, body, dependencies=dependencies.operations
    )


@router.post("/api/projects/{project_id}/knowledge/materialize/{intent_id}/projection")
async def record_knowledge_projection(
    request: Request,
    project_id: str,
    intent_id: str,
    body: KnowledgeProjectionRequest,
    *,
    dependencies: KnowledgeDependencies = Depends(get_knowledge_dependencies),
) -> dict[str, Any]:
    """Record projection truth after a canonical internal knowledge write."""
    await dependencies.require_internal_service(request)
    return await knowledge_operations.record_knowledge_projection(
        project_id, intent_id, body, dependencies=dependencies.operations
    )


@router.post("/api/projects/{project_id}/knowledge/search")
async def search_knowledge(
    request: Request,
    project_id: str,
    body: KnowledgeSearchRequest,
    *,
    dependencies: KnowledgeDependencies = Depends(get_knowledge_dependencies),
) -> dict[str, Any]:
    """Hybrid search over project knowledge base. F5: member-only."""
    await dependencies.require_project_member(request, dependencies.store, project_id)

    return await knowledge_operations.search_knowledge(
        project_id, body, dependencies=dependencies.operations
    )


@router.patch("/api/projects/{project_id}/knowledge/{note_id}")
async def update_knowledge_note(
    request: Request,
    project_id: str,
    note_id: str,
    body: KnowledgeNoteUpdate,
    *,
    dependencies: KnowledgeDependencies = Depends(get_knowledge_dependencies),
) -> dict[str, Any]:
    """Update a knowledge note's status or tags. Editor or higher.

    Officer-only tags (``ready`` arms backlog dispatch, ``parallel-safe``
    exempts a ticket from the singleton rule) and charter notes additionally
    need the project owner or an admin (D9); the check runs only when the
    request touches one of them.
    """
    # An archived project is read-only, and a note edit is a content mutation
    # rather than teardown — deleting a note stays allowed, editing one does not.
    user, _project = await dependencies.require_project_member(
        request, dependencies.store, project_id, min_role="editor", allow_archived=False
    )

    async def owner_authority() -> bool:
        if user.get("is_admin"):
            return True
        role = await dependencies.store.get_user_role_in_project(
            project_id, str(user["id"])
        )
        return role == "owner"

    return await knowledge_operations.update_knowledge_note(
        project_id,
        note_id,
        body,
        dependencies=dependencies.operations,
        owner_authority=owner_authority,
    )


@router.delete("/api/projects/{project_id}/knowledge/{note_id}")
async def delete_knowledge_note(
    request: Request,
    project_id: str,
    note_id: str,
    *,
    dependencies: KnowledgeDependencies = Depends(get_knowledge_dependencies),
) -> dict[str, Any]:
    """Purge a knowledge note: file-removal commit, then both index stores.

    Editor or higher. A human's delete is immediate and unconditional (no
    compare-and-swap token) — that authority is the point of the cockpit
    button. Before kb_gardening G2 this removed only the index row, and the
    next sweep resurrected the note from the file it never touched.
    """
    await dependencies.require_project_member(
        request, dependencies.store, project_id, min_role="editor"
    )
    return await knowledge_operations.delete_knowledge_note(
        project_id, note_id, dependencies=dependencies.operations
    )


@router.post("/api/projects/{project_id}/knowledge/delete")
async def delete_knowledge_note_internal(
    request: Request,
    project_id: str,
    body: KnowledgeDeleteRequest,
    *,
    dependencies: KnowledgeDependencies = Depends(get_knowledge_dependencies),
) -> dict[str, Any]:
    """Purge one note from the KB repo and its index. **Internal** — requires
    ``X-Internal-Key``. Same result vocabulary as the materialize endpoint
    (HTTP 200 always; ``status``/``reason`` carry the outcome) plus
    ``row_deleted``. Agents do not call this directly — ``kb_delete`` is a
    tombstone — it exists for the purge lane and for operator tooling.
    """
    await dependencies.require_internal_service(request)
    return await knowledge_operations.delete_knowledge_note_internal(
        project_id, body, dependencies=dependencies.operations
    )


@router.post("/api/projects/{project_id}/knowledge/export")
async def export_knowledge(
    request: Request,
    project_id: str,
    *,
    dependencies: KnowledgeDependencies = Depends(get_knowledge_dependencies),
) -> dict[str, Any]:
    """Export project knowledge base as Obsidian-compatible markdown files.

    Editor or higher, as the MCP capability contract declares: the export
    reads nothing a viewer cannot read note by note, but it writes a copy of
    the whole vault to the orchestrator's disk, which is not a viewer's call.
    """
    _, project = await dependencies.require_project_member(
        request, dependencies.store, project_id, min_role="editor"
    )

    return await knowledge_operations.export_knowledge(
        project_id, project, dependencies=dependencies.operations
    )
