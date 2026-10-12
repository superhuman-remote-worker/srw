"""HTTP adapters for expert, skill and default catalogues."""

from dataclasses import dataclass
from typing import Annotated, Any, Literal, Protocol
from collections.abc import Callable

from fastapi import APIRouter, Depends, File, Request, Response, UploadFile

from orchestrator.schemas.expert_catalog import (
    ExpertCreate,
    ExpertUpdate,
    SkillCreate,
    SkillUpdate,
    ExpertDefaultSetRequest,
    ExpertDefaultForkRequest,
)
from orchestrator.services.expert_catalog import ExpertCatalogService
from orchestrator.services.expert_authoring import ExpertAuthoringService
from orchestrator.services.expert_catalog_contracts import ExpertWritePolicy


class ApprovedUser(Protocol):
    async def __call__(self, request: Request) -> dict[str, Any]: ...


class AdminUser(Protocol):
    async def __call__(self, request: Request) -> dict[str, Any]: ...


class ProjectAccess(Protocol):
    async def __call__(
        self,
        request: Request,
        project_id: str,
        *,
        allow_archived: bool = True,
    ) -> tuple[dict[str, Any], dict[str, Any]]: ...


@dataclass(frozen=True)
class ExpertCatalogRouteDependencies:
    catalog: ExpertCatalogService
    authoring: ExpertAuthoringService
    require_approved_user: ApprovedUser
    require_admin: AdminUser
    require_project_member: ProjectAccess
    require_project_owner: ProjectAccess
    write_policy_factory: Callable[[Request], ExpertWritePolicy]


def get_expert_catalog_dependencies(request: Request) -> ExpertCatalogRouteDependencies:
    return request.app.state.expert_catalog_dependencies_factory()


CatalogDeps = Annotated[
    ExpertCatalogRouteDependencies, Depends(get_expert_catalog_dependencies)
]
router = APIRouter()


@router.get("/api/experts")
async def list_experts(
    request: Request, type: str | None = None, *, deps: CatalogDeps
) -> list[dict[str, Any]]:
    """List experts: bundled (disk) + DB rows visible to the caller (owned +
    project-linked + global), each tagged with ``source``. **P4e** — approved
    users only.

    ``type`` narrows by ROLE OR TAG (``expert_type == type or type in tags``,
    U1 B.4) — ``?type=worker`` / ``?type=session`` list as before plus any
    row tagged for that role; ``?type=subagent`` lists the subagent library
    (``config/subagents/*``, ``source: library``) and every expert tagged
    ``subagent``. Without ``type`` the listing is unchanged: bundled experts
    + DB rows, never the library. A bundled expert's role is inferred from
    its chain root; every entry's ``tags`` includes its role.
    """
    user = await deps.require_approved_user(request)
    return await deps.catalog.list_experts(type=type, user=user)


@router.post("/api/experts/reload")
async def reload_experts(request: Request, *, deps: CatalogDeps) -> dict[str, Any]:
    """Force reload of expert configurations cache. **Admin only** (P4d) —
    reloads expert YAML from disk."""
    await deps.require_admin(request)
    return await deps.catalog.reload_experts()


@router.get("/api/experts/{expert_id}")
async def get_expert(
    request: Request,
    expert_id: str,
    type: Literal["worker", "session"] | None = None,
    account_defaults: bool = False,
    role: Literal["worker", "session", "subagent"] | None = None,
    *,
    deps: CatalogDeps,
) -> dict[str, Any]:
    """Get full expert detail including merged config and instructions content.

    **P4e** — gated to approved users (shared catalog metadata, not per-user).

    Returns the expert's configuration (merged with defaults) and the raw
    instructions.md content, enabling the cockpit to pre-populate the job
    creation form.

    ``account_defaults=true`` folds the caller's account fallback layer into
    ``config`` at the precedence ``resolve_config`` uses. The New Session / New
    Job forms pass it so what they render is what create/dispatch will resolve;
    the expert editor must NOT, or a personal preference could be saved into a
    shared expert. See ``_load_expert_detail``.

    ``role`` resolves the expert in that role (a cross-role picker: a session
    expert previewed as the worker a job will run) — see ``_load_expert_detail``.
    ``type`` only selects the base behind the public base ids (``defaults`` /
    ``worker_base`` / ``session_base``); ``role`` wins over it when both are
    given. A subagent-library id (``config/subagents/<name>``, listed under
    ``?type=subagent``) is served here too, on the subagent overlay by default.
    """
    user = await deps.require_approved_user(request)
    return await deps.catalog.get_expert(
        expert_id=expert_id,
        type=type,
        account_defaults=account_defaults,
        role=role,
        user=user,
    )


@router.post("/api/experts")
async def create_expert(
    request: Request, body: ExpertCreate, *, deps: CatalogDeps
) -> dict[str, Any]:
    """Create an owned DB expert. Slice 1: hard-deny validated, no grants yet.
    The stored ``tags`` always carry the role (``tags ∪ {expert_type}``, U1)."""
    deps.catalog.require_experts_db()
    user = await deps.require_approved_user(request)
    return await deps.authoring.create_expert(
        body=body, user=user, write_policy=deps.write_policy_factory(request)
    )


@router.put("/api/experts/{expert_id}")
async def update_expert(
    request: Request, expert_id: str, body: ExpertUpdate, *, deps: CatalogDeps
) -> dict[str, Any]:
    """Update an owned DB expert (owner or admin). Bundled experts have no row."""
    deps.catalog.require_experts_db()
    user = await deps.require_approved_user(request)
    return await deps.authoring.update_expert(
        expert_id=expert_id,
        body=body,
        user=user,
        write_policy=deps.write_policy_factory(request),
    )


@router.delete("/api/experts/{expert_id}")
async def delete_expert(
    request: Request, expert_id: str, *, deps: CatalogDeps
) -> dict[str, Any]:
    """Delete an owned DB expert (owner or admin). Blocks (409) while
    live-referenced (decision 15)."""
    deps.catalog.require_experts_db()
    user = await deps.require_approved_user(request)
    return await deps.authoring.delete_expert(expert_id=expert_id, user=user)


@router.post("/api/experts/{expert_id}/duplicate")
async def duplicate_expert(
    request: Request, expert_id: str, *, deps: CatalogDeps
) -> dict[str, Any]:
    """Fork any visible expert (bundled or DB) into an owned copy — 'start from
    scholar' (decision 4: copy, not live link)."""
    deps.catalog.require_experts_db()
    user = await deps.require_approved_user(request)
    return await deps.authoring.duplicate_expert(
        expert_id=expert_id, user=user, write_policy=deps.write_policy_factory(request)
    )


@router.get("/api/experts/{expert_id}/export")
async def export_expert(
    request: Request, expert_id: str, *, deps: CatalogDeps
) -> dict[str, Any]:
    """Serialize an expert to a portable bundle (decision 27). DB experts export
    their raw fragment; bundled experts export their on-disk config."""
    deps.catalog.require_experts_db()
    user = await deps.require_approved_user(request)
    return await deps.authoring.export_expert(expert_id=expert_id, user=user)


@router.post("/api/experts/import")
async def import_expert(
    request: Request, body: ExpertCreate, *, deps: CatalogDeps
) -> dict[str, Any]:
    """Create an owned expert from a posted bundle (decision 27). Same validation
    as create; fork-on-import (name collision -> suffix)."""
    deps.catalog.require_experts_db()
    user = await deps.require_approved_user(request)
    return await deps.authoring.import_expert(
        body=body, user=user, write_policy=deps.write_policy_factory(request)
    )


@router.get("/api/expert-defaults")
async def get_my_expert_defaults(
    request: Request, project_id: str | None = None, *, deps: CatalogDeps
) -> dict[str, Any]:
    """Effective and editable personal defaults for the current user."""
    deps.catalog.require_experts_db()
    user = await deps.require_approved_user(request)
    if project_id:
        await deps.require_project_member(request, project_id)
    return await deps.authoring.get_my_expert_defaults(project_id=project_id, user=user)


@router.put("/api/expert-defaults/{expert_type}")
async def set_my_expert_default(
    request: Request,
    expert_type: Literal["worker", "session"],
    body: ExpertDefaultSetRequest,
    *,
    deps: CatalogDeps,
) -> dict[str, Any]:
    deps.catalog.require_experts_db()
    user = await deps.require_approved_user(request)
    return await deps.authoring.set_my_expert_default(
        expert_type=expert_type, body=body, user=user
    )


@router.delete("/api/expert-defaults/{expert_type}")
async def clear_my_expert_default(
    request: Request, expert_type: Literal["worker", "session"], *, deps: CatalogDeps
) -> dict[str, Any]:
    """Clear is intentionally allowed even after the grant is revoked."""
    deps.catalog.require_experts_db()
    user = await deps.require_approved_user(request)
    return await deps.authoring.clear_my_expert_default(
        expert_type=expert_type, user=user
    )


@router.post("/api/expert-defaults/{expert_type}/fork")
async def fork_my_expert_default(
    request: Request,
    expert_type: Literal["worker", "session"],
    body: ExpertDefaultForkRequest,
    *,
    deps: CatalogDeps,
) -> dict[str, Any]:
    """Atomically copy a visible expert and select the owned copy as default.

    Two independent 403 gates precede any write, and neither is optional:
    `personal_defaults_allowed` (this route's own switch — a personal default
    may be disabled while user-defined experts generally are not) and then
    the `user_experts` kill switch inside `_enforce_expert_save_prelude` (the
    same switch every expert-write route shares). Do not reorder or merge
    them.
    """
    deps.catalog.require_experts_db()
    user = await deps.require_approved_user(request)
    return await deps.authoring.fork_my_expert_default(
        expert_type=expert_type,
        body=body,
        user=user,
        write_policy=deps.write_policy_factory(request),
    )


@router.get("/api/admin/expert-defaults")
async def get_application_expert_defaults(
    request: Request, *, deps: CatalogDeps
) -> dict[str, Any]:
    deps.catalog.require_experts_db()
    await deps.require_admin(request)
    return await deps.authoring.get_application_expert_defaults()


@router.put("/api/admin/expert-defaults/{expert_type}")
async def set_application_expert_default(
    request: Request,
    expert_type: Literal["worker", "session"],
    body: ExpertDefaultSetRequest,
    *,
    deps: CatalogDeps,
) -> dict[str, Any]:
    deps.catalog.require_experts_db()
    admin = await deps.require_admin(request)
    return await deps.authoring.set_application_expert_default(
        expert_type=expert_type, body=body, admin=admin
    )


@router.put("/api/projects/{project_id}/expert-defaults/{expert_type}")
async def set_project_expert_default(
    request: Request,
    project_id: str,
    expert_type: Literal["worker", "session"],
    body: ExpertDefaultSetRequest,
    *,
    deps: CatalogDeps,
) -> dict[str, Any]:
    deps.catalog.require_experts_db()
    user, _project = await deps.require_project_owner(
        request, project_id, allow_archived=False
    )
    return await deps.authoring.set_project_expert_default(
        project_id=project_id, expert_type=expert_type, body=body, user=user
    )


@router.delete("/api/projects/{project_id}/expert-defaults/{expert_type}")
async def clear_project_expert_default(
    request: Request,
    project_id: str,
    expert_type: Literal["worker", "session"],
    *,
    deps: CatalogDeps,
) -> dict[str, Any]:
    deps.catalog.require_experts_db()
    user, _project = await deps.require_project_owner(request, project_id)
    return await deps.authoring.clear_project_expert_default(
        project_id=project_id, expert_type=expert_type, user=user
    )


@router.post("/api/skills")
async def create_skill(
    request: Request, body: SkillCreate, *, deps: CatalogDeps
) -> dict[str, Any]:
    """Create an owned DB skill from its file tree (Slice 1: deny-scan validated)."""
    deps.catalog.require_skills_db()
    user = await deps.require_approved_user(request)
    return await deps.authoring.create_skill(body=body, user=user)


@router.get("/api/skills")
async def list_skills(request: Request, *, deps: CatalogDeps) -> list[dict[str, Any]]:
    """List skills: bundled (disk) + DB rows visible to the caller (owned + global),
    each tagged with ``source``. Read-only; tags-and-concatenates (precedence is a
    Slice-2 resolver concern)."""
    user = await deps.require_approved_user(request)
    return await deps.catalog.list_skills(user=user)


@router.post("/api/skills/reload")
async def reload_skills(request: Request, *, deps: CatalogDeps) -> dict[str, Any]:
    """Force reload of bundled skill cache. **Admin only**."""
    await deps.require_admin(request)
    return await deps.catalog.reload_skills()


@router.get("/api/skills/{skill_id}")
async def get_skill(
    request: Request, skill_id: str, *, deps: CatalogDeps
) -> dict[str, Any]:
    """Full skill detail (metadata + file tree). DB skill by UUID, else bundled.
    A DB skill the caller may not see (not owned, not global, caller not admin)
    is 404, like a missing one."""
    user = await deps.require_approved_user(request)
    return await deps.catalog.get_skill(skill_id=skill_id, user=user)


@router.put("/api/skills/{skill_id}")
async def update_skill(
    request: Request, skill_id: str, body: SkillUpdate, *, deps: CatalogDeps
) -> dict[str, Any]:
    """Update an owned DB skill (owner or admin). Bundled skills are read-only.
    ``name`` is immutable — an edited SKILL.md whose frontmatter name differs is
    rejected (rename = create a new skill)."""
    deps.catalog.require_skills_db()
    user = await deps.require_approved_user(request)
    return await deps.authoring.update_skill(skill_id=skill_id, body=body, user=user)


@router.delete("/api/skills/{skill_id}")
async def delete_skill(
    request: Request, skill_id: str, *, deps: CatalogDeps
) -> dict[str, Any]:
    """Delete an owned DB skill (owner or admin). Files cascade away."""
    deps.catalog.require_skills_db()
    user = await deps.require_approved_user(request)
    return await deps.authoring.delete_skill(skill_id=skill_id, user=user)


@router.post("/api/skills/{skill_id}/duplicate")
async def duplicate_skill(
    request: Request, skill_id: str, *, deps: CatalogDeps
) -> dict[str, Any]:
    """Fork any visible skill (bundled or DB) into an owned copy."""
    deps.catalog.require_skills_db()
    user = await deps.require_approved_user(request)
    return await deps.authoring.duplicate_skill(skill_id=skill_id, user=user)


@router.get("/api/skills/{skill_id}/export")
async def export_skill(
    request: Request, skill_id: str, *, deps: CatalogDeps
) -> Response:
    """Serialize a skill to a native zipped directory (drops into .claude/skills)."""
    deps.catalog.require_skills_db()
    user = await deps.require_approved_user(request)
    archive = await deps.authoring.export_skill(skill_id=skill_id, user=user)
    return Response(
        content=archive.content,
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{archive.name}.zip"'},
    )


@router.post("/api/skills/import")
async def import_skill(
    request: Request, file: UploadFile = File(...), *, deps: CatalogDeps
) -> dict[str, Any]:
    """Create an owned skill from an uploaded skill zip (fork-on-name-collision)."""
    deps.catalog.require_skills_db()
    user = await deps.require_approved_user(request)
    return await deps.authoring.import_skill(user=user, archive=await file.read())


@router.get("/api/projects/{project_id}/experts")
async def list_project_experts(
    request: Request, project_id: str, *, deps: CatalogDeps
) -> list[dict[str, Any]]:
    """List DB-backed experts linked to a project.

    During the safe migration, an old project's ``experts/`` directory remains
    a read-only fallback when it has no structured links yet.
    """
    await deps.require_project_member(request, project_id)
    return await deps.catalog.list_project_experts(project_id=project_id)


@router.get("/api/projects/{project_id}/experts/{expert_name}")
async def get_project_expert(
    request: Request, project_id: str, expert_name: str, *, deps: CatalogDeps
) -> dict[str, Any]:
    """Get full detail for a linked expert, with a legacy Git fallback."""
    caller, _ = await deps.require_project_member(request, project_id)
    return await deps.catalog.get_project_expert(
        project_id=project_id, expert_name=expert_name, caller=caller
    )
