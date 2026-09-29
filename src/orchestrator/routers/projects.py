"""HTTP adapters for projects, their members, repositories and connector links.

Every handler resolves its collaborators from the application handling the
request, so importing this router never imports application startup. The
authorization gates are dataclass fields rather than module imports: the
composed application injects the call-time bindings it wants, and
``scripts/check_endpoint_auth.py`` still reads the gate each route depends on
straight off the call.

Two routes escalate *conditionally*, and both keep the escalation exactly where
it was rather than hoisting it to the top of the handler — hoisting would change
which status code a request that trips two rules receives:

* ``PATCH /api/projects/{project_id}`` reaches the admin gate only for
  ``network_tier``, after the archived-project refusal and the override
  validation;
* ``DELETE /api/projects/{project_id}/datasources/{datasource_id}`` reaches the
  project-owner gate only when the caller is neither an admin nor the
  connector's owner, after the MCP scope check and the native-KB refusal.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from fastapi import APIRouter, Depends, Query, Request

from orchestrator.schemas.datasources import ProjectDatasourceSettings
from orchestrator.schemas.projects import (
    ExternalKnowledgeBase,
    ProjectCreate,
    ProjectMemberAdd,
    ProjectMemberUpdate,
    ProjectRepositoryCreate,
    ProjectRepositoryUpdate,
    ProjectUpdate,
    ProjectWorkspaceDefaultsUpdate,
    PromoteRequest,
)
from orchestrator.security.access import (
    require_job_access,
    require_project_member,
    require_project_owner,
)
from orchestrator.security.auth import require_approved_user
from orchestrator.services import projects

router = APIRouter()


@dataclass(frozen=True)
class ProjectsDependencies:
    """Per-app auth store, operations and authorization gates.

    ``require_admin`` has no module default: the application composes it with
    its own store, user resolver and audit sink, exactly as the other
    admin-touching routers do.
    """

    store: Any
    operations: projects.ProjectDependencies
    require_admin: Callable[..., Awaitable[Any]]
    require_approved_user: Callable[..., Awaitable[Any]] = require_approved_user
    require_project_member: Callable[..., Awaitable[Any]] = require_project_member
    require_project_owner: Callable[..., Awaitable[Any]] = require_project_owner
    require_job_access: Callable[..., Awaitable[Any]] = require_job_access


def get_projects_dependencies(request: Request) -> ProjectsDependencies:
    """Resolve collaborators only from the application handling this request."""
    return request.app.state.projects_dependencies_factory()


# =============================================================================
# Project lifecycle
# =============================================================================


@router.post("/api/projects")
async def create_project(
    body: ProjectCreate,
    request: Request,
    *,
    dependencies: ProjectsDependencies = Depends(get_projects_dependencies),
) -> dict[str, Any]:
    """Create a new project with the requesting user as owner."""
    user = await dependencies.require_approved_user(request, dependencies.store)
    return await projects.create_project(
        body, user=user, dependencies=dependencies.operations
    )


@router.post("/api/projects/{project_id}/knowledge/repository")
async def attach_project_knowledge_repository(
    request: Request,
    project_id: str,
    body: ExternalKnowledgeBase,
    *,
    dependencies: ProjectsDependencies = Depends(get_projects_dependencies),
) -> dict[str, Any]:
    """Attach an external GitHub live vault to an existing repo-less project.

    Replacing or migrating an existing knowledge-role repository is
    deliberately not implicit: v1 has no approved note/history migration.
    """
    caller, project = await dependencies.require_project_owner(
        request, dependencies.store, project_id, allow_archived=False
    )
    return await projects.attach_project_knowledge_repository(
        project_id,
        body,
        caller=caller,
        project=project,
        dependencies=dependencies.operations,
    )


@router.get("/api/projects")
async def list_projects(
    request: Request,
    user_id: str | None = Query(default=None),
    status: list[str] | None = Query(
        default=None,
        description=(
            "Project lifecycle status(es) to include (repeatable). "
            "Defaults to 'active' — archived projects are excluded unless "
            "asked for, e.g. ?status=archived or ?status=active&status=archived"
        ),
    ),
    *,
    dependencies: ProjectsDependencies = Depends(get_projects_dependencies),
) -> list[dict[str, Any]]:
    """List projects visible to the caller.

    Visibility model (G2):
        * Admins see the full list, optionally narrowed by ``?user_id=`` or
          by an MCP ``project:<uuid>`` token scope.
        * Non-admins see only the projects they're a member of
          (``get_projects_for_user(caller)``), narrowed by any MCP scope.
        * A non-admin passing ``?user_id=`` for anyone other than themselves
          is rejected (403). Self-query is allowed but redundant.

    Lifecycle (knowledge-base/knowledge/features/project_and_job_list_filtering.md
    §4.2): archived projects are excluded by default on BOTH branches. The
    server does this, not the client — a filter applied in Angular still pays
    for the rows in Postgres and leaves every other consumer unprotected.
    The old admin-branch ``status != 'deleted'`` predicate was dead code:
    ``valid_project_status`` has no such value (deletion is a hard row
    delete), so it never excluded anything.

    ``get_projects_for_user`` keeps its LIMIT 100. With archived excluded that
    ceiling stops being reachable for realistic accounts; if it ever is, the
    projects grid needs paging too (out of scope, phase 1).
    """
    caller = await dependencies.require_approved_user(request, dependencies.store)
    return await projects.list_projects(
        caller=caller,
        user_id=user_id,
        status=status,
        dependencies=dependencies.operations,
    )


@router.get("/api/projects/{project_id}")
async def get_project(
    request: Request,
    project_id: str,
    *,
    dependencies: ProjectsDependencies = Depends(get_projects_dependencies),
) -> dict[str, Any]:
    """Get a single project by ID.

    The warm path is pure Postgres — cloud/Keycloak reconciliation and
    identity resolution run as throttled background repairs, never on the
    request (knowledge-base/knowledge/issues/project_page_open_blocks_on_cloud_heal.md
    measured 2.3-5s per page open when they were inline).
    """
    _, project = await dependencies.require_project_member(
        request, dependencies.store, project_id
    )
    return await projects.get_project(
        project_id, project=project, dependencies=dependencies.operations
    )


@router.patch("/api/projects/{project_id}")
async def update_project(
    project_id: str,
    body: ProjectUpdate,
    request: Request,
    *,
    dependencies: ProjectsDependencies = Depends(get_projects_dependencies),
) -> dict[str, Any]:
    """Update a project. Caller must be a project owner or admin.

    ALLOW-listed for archived projects because it is the unarchive path — but
    **status-only** while archived (§4.3a). The handler is generic: it also
    covers ``name``, ``goal`` and ``default_config_override``, and that last
    one is merged under *every job in the project*, so leaving it open would
    stop "archived" meaning read-only in the way the shipped UI copy promises.
    The guard flag cannot express this — it fires before the body is
    inspected — so it is a body-level check here. Renaming an archived project
    is a legitimate want; it just needs an unarchive first, which is one click.

    Setting ``status='archived'`` additionally quiesces the project's children
    and reports what it stopped (§4.5).
    """
    # H5: pre-fix, anyone could rename any project, change its goal, or
    # toggle cloud-storage settings.
    # Indexed, not unpacked: several existing tests stand the whole gate up as
    # a bare AsyncMock, which returns a MagicMock rather than a 2-tuple, and
    # `a, b = ...` would blow up on it. Same reasoning as routers/contacts.py.
    project = (
        await dependencies.require_project_owner(
            request, dependencies.store, project_id
        )
    )[1]

    async def escalate_admin() -> Any:
        return await dependencies.require_admin(request)

    return await projects.update_project(
        project_id,
        body,
        project=project,
        escalate_admin=escalate_admin,
        dependencies=dependencies.operations,
    )


@router.get("/api/projects/{project_id}/workspace-defaults")
async def get_project_workspace_defaults(
    project_id: str,
    request: Request,
    *,
    dependencies: ProjectsDependencies = Depends(get_projects_dependencies),
) -> dict[str, Any]:
    """The Project's workspace defaults: stored, effective, and any problems."""
    from orchestrator.services import project_workspace_defaults_view

    _, project = await dependencies.require_project_member(
        request, dependencies.store, project_id
    )
    return await project_workspace_defaults_view.read_view(dependencies.store, project)


@router.put("/api/projects/{project_id}/workspace-defaults")
async def put_project_workspace_defaults(
    project_id: str,
    body: ProjectWorkspaceDefaultsUpdate,
    request: Request,
    *,
    dependencies: ProjectsDependencies = Depends(get_projects_dependencies),
) -> dict[str, Any]:
    """Save the Project's workspace defaults. Caller must be a project owner
    or admin, the same rule as PATCH /api/projects/{project_id}."""
    from orchestrator.services import project_workspace_defaults_view

    user, project = await dependencies.require_project_owner(
        request, dependencies.store, project_id, allow_archived=False
    )
    return await project_workspace_defaults_view.update_view(
        dependencies.store, project, user, body
    )


@router.delete("/api/projects/{project_id}")
async def delete_project(
    project_id: str,
    request: Request,
    *,
    dependencies: ProjectsDependencies = Depends(get_projects_dependencies),
) -> dict[str, str]:
    """Delete a project. Caller must be a project owner or admin. Cannot delete default projects."""
    # H5: pre-fix, anyone could cascade-delete any project (repos,
    # Keycloak groups, cloud folders, knowledge index, ...).
    _, project = await dependencies.require_project_owner(
        request, dependencies.store, project_id
    )
    return await projects.delete_project(
        project_id, project=project, dependencies=dependencies.operations
    )


# =============================================================================
# Project members
# =============================================================================


@router.get("/api/projects/{project_id}/members")
async def list_project_members(
    request: Request,
    project_id: str,
    *,
    dependencies: ProjectsDependencies = Depends(get_projects_dependencies),
) -> list[dict[str, Any]]:
    """List members of a project with user info."""
    await dependencies.require_project_member(request, dependencies.store, project_id)
    return await projects.list_project_members(
        project_id, dependencies=dependencies.operations
    )


@router.post("/api/projects/{project_id}/members")
async def add_project_member(
    project_id: str,
    body: ProjectMemberAdd,
    request: Request,
    *,
    dependencies: ProjectsDependencies = Depends(get_projects_dependencies),
) -> dict[str, Any]:
    """Add a member to a project. Caller must be a project owner or admin."""
    # H3: pre-fix, anyone could invite themselves as owner of any project
    # and then access everything in it. This is the foundational
    # privilege-escalation path that opens every other gate.
    _, project = await dependencies.require_project_owner(
        request, dependencies.store, project_id, allow_archived=False
    )
    return await projects.add_project_member(
        project_id, body, project=project, dependencies=dependencies.operations
    )


@router.patch("/api/projects/{project_id}/members/{user_id}")
async def update_project_member(
    project_id: str,
    user_id: str,
    body: ProjectMemberUpdate,
    request: Request,
    *,
    dependencies: ProjectsDependencies = Depends(get_projects_dependencies),
) -> dict[str, str]:
    """Update a member's role in a project. Caller must be a project owner or admin."""
    # H3: role changes are sensitive — restrict to owners/admins.
    await dependencies.require_project_owner(
        request, dependencies.store, project_id, allow_archived=False
    )
    return await projects.update_project_member(
        project_id, user_id, body, dependencies=dependencies.operations
    )


@router.delete("/api/projects/{project_id}/members/{user_id}")
async def remove_project_member(
    project_id: str,
    user_id: str,
    request: Request,
    *,
    dependencies: ProjectsDependencies = Depends(get_projects_dependencies),
) -> dict[str, str]:
    """Remove a member from a project. Owner/admin can remove anyone; any member can remove themselves. Cannot remove the last owner."""
    # H3: pre-fix, anyone could remove anyone (only the last-owner check
    # was enforced). Allow self-removal so members can leave projects — which
    # is why this route authenticates rather than demanding project ownership
    # up front; the owner check runs inside, only for a cross-user removal.
    caller = await dependencies.require_approved_user(request, dependencies.store)
    return await projects.remove_project_member(
        project_id, user_id, caller=caller, dependencies=dependencies.operations
    )


# =============================================================================
# Project repositories
# =============================================================================


@router.get("/api/projects/{project_id}/repositories")
async def list_project_repositories(
    request: Request,
    project_id: str,
    role: str | None = Query(default=None),
    *,
    dependencies: ProjectsDependencies = Depends(get_projects_dependencies),
) -> list[dict[str, Any]]:
    """List repositories attached to a project."""
    await dependencies.require_project_member(request, dependencies.store, project_id)
    return await projects.list_project_repositories(
        project_id, role=role, dependencies=dependencies.operations
    )


@router.post("/api/projects/{project_id}/repositories")
async def add_project_repository(
    request: Request,
    project_id: str,
    body: ProjectRepositoryCreate,
    *,
    dependencies: ProjectsDependencies = Depends(get_projects_dependencies),
) -> dict[str, Any]:
    """Attach a repository to a project. Owner or admin only (creates managed Gitea repo)."""
    await dependencies.require_project_owner(
        request, dependencies.store, project_id, allow_archived=False
    )
    return await projects.add_project_repository(
        project_id, body, dependencies=dependencies.operations
    )


@router.patch("/api/projects/{project_id}/repositories/{repo_id}")
async def update_project_repository(
    request: Request,
    project_id: str,
    repo_id: str,
    body: ProjectRepositoryUpdate,
    *,
    dependencies: ProjectsDependencies = Depends(get_projects_dependencies),
) -> dict[str, str]:
    """Update a project repository. Owner or admin only."""
    await dependencies.require_project_owner(
        request, dependencies.store, project_id, allow_archived=False
    )
    return await projects.update_project_repository(
        project_id, repo_id, body, dependencies=dependencies.operations
    )


@router.delete("/api/projects/{project_id}/repositories/{repo_id}")
async def remove_project_repository(
    request: Request,
    project_id: str,
    repo_id: str,
    *,
    dependencies: ProjectsDependencies = Depends(get_projects_dependencies),
) -> dict[str, str]:
    """Remove a repository from a project. Owner or admin only. Cannot remove the jobs repo."""
    await dependencies.require_project_owner(request, dependencies.store, project_id)
    return await projects.remove_project_repository(
        project_id, repo_id, dependencies=dependencies.operations
    )


# =============================================================================
# Project Datasources (N:M)
# =============================================================================


@router.get("/api/projects/{project_id}/linkable-datasources")
async def list_project_linkable_datasources(
    request: Request,
    project_id: str,
    q: str | None = Query(default=None, max_length=200),
    limit: int = Query(default=50, ge=1, le=100),
    cursor: str | None = Query(default=None),
    *,
    dependencies: ProjectsDependencies = Depends(get_projects_dependencies),
) -> dict[str, Any]:
    """Page connectors the caller may newly link to the target project."""
    user, _ = await dependencies.require_project_owner(
        request, dependencies.store, project_id
    )
    return await projects.list_project_linkable_datasources(
        project_id,
        user=user,
        q=q,
        limit=limit,
        cursor=cursor,
        dependencies=dependencies.operations,
    )


@router.get("/api/projects/{project_id}/datasources")
async def list_project_datasources(
    request: Request,
    project_id: str,
    *,
    dependencies: ProjectsDependencies = Depends(get_projects_dependencies),
) -> list[dict[str, Any]]:
    """List connectors linked to a project. F3: project membership required."""
    await dependencies.require_project_member(request, dependencies.store, project_id)
    return await projects.list_project_datasources(
        project_id, dependencies=dependencies.operations
    )


@router.post("/api/projects/{project_id}/datasources/{datasource_id}")
async def link_datasource_to_project(
    request: Request,
    project_id: str,
    datasource_id: str,
    body: ProjectDatasourceSettings | None = None,
    *,
    dependencies: ProjectsDependencies = Depends(get_projects_dependencies),
) -> dict[str, str]:
    """Link an existing connector to a project.

    F3: caller must be project owner of the target project AND must be
    able to see the connector (admin / creator / member of one of its
    projects). Prevents a project owner from probing for stranger
    connectors by guessing UUIDs.

    Optionally pass project-level overrides (read_only, description).
    Also creates a knowledge entry so agents discover the connector.
    """
    user, _ = await dependencies.require_project_owner(
        request, dependencies.store, project_id, allow_archived=False
    )
    return await projects.link_datasource_to_project(
        project_id,
        datasource_id,
        body,
        user=user,
        dependencies=dependencies.operations,
    )


@router.patch("/api/projects/{project_id}/datasources/{datasource_id}")
async def update_project_datasource(
    request: Request,
    project_id: str,
    datasource_id: str,
    body: ProjectDatasourceSettings,
    *,
    dependencies: ProjectsDependencies = Depends(get_projects_dependencies),
) -> dict[str, str]:
    """Update project-level settings for a linked connector. F3: project owner only.

    Pass null to clear an override and fall back to connector defaults.
    """
    user, _ = await dependencies.require_project_owner(
        request, dependencies.store, project_id, allow_archived=False
    )
    return await projects.update_project_datasource(
        project_id,
        datasource_id,
        body,
        user=user,
        dependencies=dependencies.operations,
    )


@router.delete("/api/projects/{project_id}/datasources/{datasource_id}")
async def unlink_datasource_from_project(
    request: Request,
    project_id: str,
    datasource_id: str,
    *,
    dependencies: ProjectsDependencies = Depends(get_projects_dependencies),
) -> dict[str, str]:
    """Unlink a connector from a project. F3: project owner only.

    Also removes the knowledge entry.
    """
    user = await dependencies.require_approved_user(request, dependencies.store)
    _, needs_project_owner = await projects.resolve_datasource_unlink(
        project_id, datasource_id, user=user, dependencies=dependencies.operations
    )
    if needs_project_owner:
        await dependencies.require_project_owner(
            request, dependencies.store, project_id
        )
    return await projects.unlink_datasource_from_project(
        project_id, datasource_id, user=user, dependencies=dependencies.operations
    )


# =============================================================================
# Project job records and promotion
# =============================================================================


@router.get("/api/projects/{project_id}/job-records")
async def list_project_job_records(
    request: Request,
    project_id: str,
    limit: int = Query(default=200, ge=1, le=500),
    *,
    dependencies: ProjectsDependencies = Depends(get_projects_dependencies),
) -> list[dict[str, Any]]:
    """Return orchestrator-owned terminal history for a project."""
    await dependencies.require_project_member(request, dependencies.store, project_id)
    return await projects.list_project_job_records(
        project_id, limit=limit, dependencies=dependencies.operations
    )


@router.get("/api/jobs/{job_id}/change-record")
async def get_job_change_record(
    request: Request,
    job_id: str,
    *,
    dependencies: ProjectsDependencies = Depends(get_projects_dependencies),
) -> dict[str, Any]:
    """Return the immutable structured terminal record for one visible job."""
    await dependencies.require_job_access(request, dependencies.store, job_id)
    return await projects.get_job_change_record(
        job_id, dependencies=dependencies.operations
    )


@router.post("/api/jobs/{job_id}/promote")
async def promote_job(
    request: Request,
    job_id: str,
    body: PromoteRequest,
    *,
    dependencies: ProjectsDependencies = Depends(get_projects_dependencies),
) -> dict[str, Any]:
    """Promote a default-project job into a dedicated project.

    Creates a new project, provisions its cloud/knowledge resources, and moves
    the completed job into it. The job keeps its isolated execution repository;
    no project workspace repository is created.

    P4c: ``body.user_id`` is forced to the caller (mirrors F2 — no cross-user
    promotion).
    """
    caller, _ = await dependencies.require_job_access(
        request, dependencies.store, job_id
    )
    return await projects.promote_job(
        job_id, body, caller=caller, dependencies=dependencies.operations
    )
