"""``/api/persistent/threads`` — session creation, preview and list.

Extracted from ``orchestrator.main`` (R1.B06 lane B). Two route declarations,
moved with their handler names, paths, methods, parameter order and docstrings
intact — the docstring is the published OpenAPI description, so it is part of
the route's identity and not editorial text.

Neither declaration carried ``tags``, ``response_model``, ``status_code`` or a
``dependencies`` list, and neither acquires one here. Auth is performed inside
the handler (``require_approved_user``) exactly as before, which is why it
arrives through the dependency object rather than through ``Depends``.

The read-only preview added for quick chat shares the creation admission plan.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request

from orchestrator.schemas.thread_admission import ThreadCreateRequest
from orchestrator.services import thread_admission
from orchestrator.services.vm_idle_public import read_vm_idle_states

# No `tags=` and no prefix: the two declarations this replaces carried neither,
# and either would change the published OpenAPI operation for a route whose
# identity this batch is required to leave untouched.
router = APIRouter()


def get_thread_admission_dependencies(
    request: Request,
) -> thread_admission.ThreadAdmissionDependencies:
    """Resolve collaborators only from the application handling this request."""
    return request.app.state.thread_admission_dependencies_factory()


@router.post("/api/persistent/threads/preview")
async def preview_thread_creation(
    request_body: ThreadCreateRequest, request: Request
) -> dict[str, Any]:
    """Resolve session workspace and connector selection without creating work.

    Uses the create path's read-only admission plan. The returned IDs are a
    reviewable selection, not authorization: creation revalidates them against
    the current project, workspace and connector policies.
    """
    dependencies = get_thread_admission_dependencies(request)
    user = await dependencies.require_approved_user(request, dependencies.store)
    plan = await thread_admission.resolve_thread_creation_plan(
        request_body, user, dependencies=dependencies
    )
    return {
        "project_ids": plan.effective_project_ids,
        "workspace_backend": plan.thread_backend,
        "datasource_ids": plan.selected_datasource_ids,
    }


@router.post("/api/persistent/threads")
async def create_thread(
    request_body: ThreadCreateRequest, request: Request
) -> dict[str, Any]:
    """Create a new persistent thread with a concrete resolved expert."""
    dependencies = get_thread_admission_dependencies(request)
    # The gate runs here, in the declaration's own body, because
    # `scripts/check_endpoint_auth.py` reads the audited gate from the route it
    # is declared on and does not follow a call into a service module. A handler
    # that only delegates is reported `unscoped` and rewrites
    # `policy/endpoint_inventory.txt`. The resolved principal is handed down so
    # this is the same single approval read it always was.
    user = await dependencies.require_approved_user(request, dependencies.store)
    return await thread_admission.create_thread(
        request_body,
        request,
        dependencies=dependencies,
        user=user,
    )


@router.get("/api/persistent/threads")
async def list_threads(
    request: Request,
    project_id: str | None = None,
    status: str | None = None,
) -> dict[str, Any]:
    """List persistent threads for the authenticated user."""
    dependencies = get_thread_admission_dependencies(request)
    user = await dependencies.require_approved_user(request, dependencies.store)
    result = await thread_admission.list_threads(
        request,
        project_id,
        status,
        dependencies=dependencies,
        user=user,
    )
    states = await read_vm_idle_states(
        dependencies.store,
        owner_kind="thread",
        owner_ids=[str(thread["id"]) for thread in result["threads"]],
    )
    for thread in result["threads"]:
        thread["workspace_lifecycle"] = states.get(str(thread["id"]))
    return result


__all__ = [
    "create_thread",
    "get_thread_admission_dependencies",
    "list_threads",
    "preview_thread_creation",
    "router",
]
