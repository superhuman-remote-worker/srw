"""Prepare a job's project and expert configuration before write admission.

The caller supplies an authenticated/upload-authorized scope and a sanitized
command. This stage performs no writes or provisioning; all later Officer,
datasource, grant and transactional authority checks still apply. The application
owns the lazy catalogue cache, current feature gates and collaborator lifecycles.
HTTP exceptions remain the compatibility contract for this extraction.
"""

from dataclasses import dataclass, replace
import json
import logging
from typing import Any, Awaitable, Callable, Protocol

from fastapi import HTTPException

from orchestrator.schemas.job_create import JobCreate
from orchestrator.services.config_overrides import (
    deep_merge_dicts,
    validated_config_name,
)
from orchestrator.services.default_experts import (
    DefaultExpertUnavailable,
    ExpertSelection,
    ExpertSelectionError,
)
from orchestrator.services.job_admission_scope import (
    _INTERNAL_JOB_SCOPE_DENIED,
    JobAdmissionOrigin,
    JobAdmissionScope,
)
from orchestrator.services.project_status import (
    PROJECT_ARCHIVED_DETAIL,
    project_is_archived,
)
from orchestrator.services.work_categories import default_expert, normalize_category
from shared.expert_reference import ExpertReferenceConflict, resolve_expert_selection
from shared.runtime.core.loader import canonical_config_name
from shared.workspace_contract import (
    WorkspaceContractError,
    configured_workspace_backend,
)

logger = logging.getLogger(__name__)


class JobConfigStore(Protocol):
    async def get_user(self, user_id: str) -> dict[str, Any] | None: ...

    async def get_project(self, project_id: str) -> dict[str, Any] | None: ...


class RequireJobProjectAccess(Protocol):
    async def __call__(
        self,
        principal: dict[str, Any] | None,
        project_id: str | None,
        *,
        denial_detail: str,
    ) -> None: ...


class ResolveWorkerExpert(Protocol):
    async def __call__(
        self,
        *,
        user_id: str,
        project_id: str | None,
        explicit_expert_id: str | None,
        is_admin: bool,
    ) -> ExpertSelection: ...


@dataclass(frozen=True)
class JobAdmissionConfigDependencies:
    store: JobConfigStore
    require_project_access: RequireJobProjectAccess
    bundled_expert_exists: Callable[[str], bool]
    experts_db_enabled: Callable[[], bool]
    user_experts_enabled: Callable[[], Awaitable[bool]]
    resolve_worker_expert: ResolveWorkerExpert


@dataclass(frozen=True)
class JobAdmissionConfig:
    """Prepared configuration, not permission to INSERT a job."""

    context: dict[str, Any]
    project_id: str | None
    config_name: str
    config_override: dict[str, Any] | None
    expert_id: str | None
    request_config_override: dict[str, Any] | None
    requested_workspace_backend: str | None
    root_creation: bool
    workspace_selection: dict[str, Any] | None = None
    # Nobody chose this worker: no expert was named and no project/personal
    # default applied, leaving the deployment-wide fallback (the application
    # default, or the bare worker base). Only such a root job takes its work
    # category's default expert — see apply_work_expert_default.
    expert_is_fallback: bool = False


async def prepare_job_admission_config(
    *,
    command: JobCreate,
    scope: JobAdmissionScope,
    origin: JobAdmissionOrigin,
    dependencies: JobAdmissionConfigDependencies,
) -> JobAdmissionConfig:
    if origin not in ("user_rest", "internal_rest"):
        raise ValueError(f"Unknown job admission origin: {origin}")
    job = command
    context = dict(scope.context)
    effective_user_id = scope.user_id
    # Resolve project_id: authoritative internal origin / public request,
    # then the user's default only when no thread/parent constrained scope.
    project_id = scope.project_id
    if not project_id and effective_user_id and not scope.origin_bound:
        try:
            user = await dependencies.store.get_user(effective_user_id)
            if user and user.get("default_project_id"):
                project_id = str(user["default_project_id"])
        except Exception as e:
            logger.warning(
                f"Failed to resolve default project for user {effective_user_id}: {e}"
            )

    await dependencies.require_project_access(
        scope.principal,
        project_id,
        denial_detail=(
            _INTERNAL_JOB_SCOPE_DENIED
            if origin == "internal_rest"
            else "Project role 'editor' or higher required"
        ),
    )

    # Resolve project config fallback plus the authoritative DB expert
    # selection.  Root jobs persist a concrete expert id; internal
    # children/specialists keep their explicit/inherited selector and never
    # silently acquire a user's current default.
    project = None
    # One catalogue, one selector: `expert` takes a bundled slug or a DB
    # expert UUID and resolves to the (base config, DB overlay) pair this
    # funnel persists. The deprecated aliases go through the same helper,
    # so the "two experts in one call" refusal is stated once — see
    # knowledge-base/knowledge/issues/experts_one_catalogue_two_selection_paths.md.
    try:
        expert_choice = resolve_expert_selection(
            expert=job.expert,
            config_name=job.config_name,
            expert_id=job.expert_id,
        )
    except ExpertReferenceConflict as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if expert_choice.kind == "bundled" and job.expert:
        # `expert` means "an entry from the catalogue", so a slug that is
        # not in it is a typo, not a deployment config. Refuse now: the
        # alternative is a job that provisions and only fails when the
        # agent cannot load its config. `config_name` keeps accepting
        # non-catalogue deployment configs, unvalidated, as it always did.
        if not dependencies.bundled_expert_exists(expert_choice.config_name):
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Unknown expert '{expert_choice.config_name}'. Use "
                    "list_experts (GET /api/experts) to see the selectable "
                    "experts; pass a bundled expert id or a DB expert UUID."
                ),
            )
    explicit_expert_id = expert_choice.expert_id
    # Write boundary for the pod entrypoint's one caller-controlled word.
    # `config_name` still accepts non-catalogue deployment configs (the
    # branch above only vets `expert`), so this is the only charset check
    # a job's stored selector ever gets — and jobs.config_name is read back
    # by dispatch, resume, subjob grafting and every recovery path.
    config_name = canonical_config_name(
        validated_config_name(expert_choice.config_name) or "worker_base"
    )
    # The resolver can never emit both halves; assert it rather than trust
    # it, because "a DB expert layered over someone else's bundled base"
    # is a config nobody reviewed.
    if explicit_expert_id and config_name != "worker_base":
        raise HTTPException(
            status_code=400,
            detail=(
                "expert_id cannot be combined with a bundled worker "
                "config_name; select one expert source"
            ),
        )
    request_config_override = job.config_override
    try:
        requested_workspace_backend = configured_workspace_backend(
            request_config_override
        )
    except WorkspaceContractError as exc:
        raise HTTPException(
            status_code=422,
            detail={"code": exc.code, "message": exc.detail},
        ) from exc
    project_default_override: dict[str, Any] | None = None
    if project_id:
        project = await dependencies.store.get_project(project_id)
        if not project:
            raise HTTPException(
                status_code=404, detail=f"Project '{project_id}' not found"
            )
        # Layer 2 of the archived-project refusal (§4.3 of
        # knowledge-base/knowledge/features/project_and_job_list_filtering.md).
        # It has to be HERE and not on the guard above: an X-Internal-Key
        # caller — all MCP traffic, all agent delegation, the bench
        # sweeper — skips require_project_member entirely, so the flag
        # would only ever cover the cockpit. This load is unconditional
        # across both paths. Critic/scholar/curator subjobs call
        # postgres_db.create_job directly and stay exempt by construction:
        # finishing in-flight work is not new work.
        if project_is_archived(project):
            raise HTTPException(status_code=409, detail=PROJECT_ARCHIVED_DETAIL)
        project_default_override = (
            None
            if project.get("manifest_composed")
            else project.get("default_config_override")
        )
        if project_default_override:
            # asyncpg may return JSONB as a string — parse it
            if isinstance(project_default_override, str):
                project_default_override = json.loads(project_default_override)

    config_override = project_default_override
    resolved_expert_id = explicit_expert_id
    # A worker launched from an interactive thread is still a user-level
    # root job (the thread supplies scope/datasources, not a worker parent).
    # Only actual worker children/specialists carry parent_job_id.
    root_creation = not job.parent_job_id
    should_resolve_default = (
        root_creation
        and bool(effective_user_id)
        and config_name == "worker_base"
        and dependencies.experts_db_enabled()
        and await dependencies.user_experts_enabled()
    )
    should_validate_explicit = (
        bool(explicit_expert_id)
        and bool(effective_user_id)
        and dependencies.experts_db_enabled()
    )
    selection = None
    try:
        if should_resolve_default or should_validate_explicit:
            principal = scope.principal
            selection = await dependencies.resolve_worker_expert(
                user_id=str(effective_user_id),
                project_id=project_id,
                explicit_expert_id=explicit_expert_id,
                is_admin=bool((principal or {}).get("is_admin")),
            )
            from orchestrator.services.manifest_runtime_ownership import (
                require_srw_expert_configuration,
            )

            require_srw_expert_configuration(selection.expert)
            resolved_expert_id = str(selection.expert["id"])
            config_name = "worker_base"
            if selection.project_override and not (project or {}).get(
                "manifest_composed"
            ):
                config_override = deep_merge_dicts(
                    config_override or {}, selection.project_override
                )
            context["expert_selection"] = {
                "source": selection.source,
                "expert_id": resolved_expert_id,
            }
        elif (
            root_creation
            and config_name == "worker_base"
            and project
            and project.get("default_config_name")
            and not dependencies.experts_db_enabled()
        ):
            # Emergency compatibility mode only.  In normal operation the
            # typed project_experts pointer supersedes this legacy slug.
            config_name = canonical_config_name(project["default_config_name"])
    except ExpertSelectionError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except DefaultExpertUnavailable as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    if selection is None and expert_choice.kind == "bundled":
        # Same field the DB path stamps, so "who did this dispatcher pick,
        # and did it pick at all?" has one answer whichever store the
        # expert lives in. Reading exactly this key across eight jobs is
        # how the two-path defect was diagnosed.
        context["expert_selection"] = {
            "source": "bundled",
            "expert": expert_choice.reference,
        }
    # The application default never carries a project overlay today; the
    # guard keeps it that way for this flag, because a later category swap
    # cannot un-merge one from config_override.
    expert_is_fallback = (
        root_creation
        and expert_choice.kind == "default"
        and config_name == "worker_base"
        and (
            selection is None
            or (selection.source == "application" and not selection.project_override)
        )
    )

    if request_config_override:
        config_override = deep_merge_dicts(
            config_override or {}, request_config_override
        )

    workspace_selection = None
    workspace_supplied = "workspace" in job.model_fields_set
    if workspace_supplied and not root_creation:
        raise HTTPException(
            422,
            "Child jobs inherit their parent workspace; select it on the root execution.",
        )
    if root_creation and (
        workspace_supplied or (project or {}).get("manifest_composed")
    ):
        from orchestrator.services.manifest_workspace_selection import (
            select_execution_workspace,
        )
        from shared.runtime.core.workspace_selection import bind_execution_workspace

        if not effective_user_id:
            raise HTTPException(422, "Workspace selection requires an execution owner.")
        workspace_config, workspace_selection = await select_execution_workspace(
            dependencies.store,
            scope.principal or {"id": str(effective_user_id)},
            project_id=project_id,
            role="worker",
            workspace=job.workspace,
            supplied=workspace_supplied,
            # Duplicate-selection validation concerns authored request fields;
            # an inherited legacy Project backend must yield to this choice.
            config_override=request_config_override
            if workspace_supplied
            else config_override,
        )
        config_override = bind_execution_workspace(
            config_override or {}, workspace_config
        )
        if workspace_supplied:
            requested_workspace_backend = workspace_config["backend"]

    return JobAdmissionConfig(
        context=context,
        project_id=project_id,
        config_name=config_name,
        config_override=config_override,
        expert_id=resolved_expert_id,
        request_config_override=request_config_override,
        requested_workspace_backend=requested_workspace_backend,
        root_creation=root_creation,
        workspace_selection=workspace_selection,
        expert_is_fallback=expert_is_fallback,
    )


def apply_work_expert_default(
    config: JobAdmissionConfig,
    *,
    context: dict[str, Any],
    ticket_expert: str | None,
    requested_category: str | None,
    slot_category: str | None,
    bundled_expert_exists: Callable[[str], bool],
) -> JobAdmissionConfig:
    """Staff an unchosen worker from what the work asks for, ahead of the fallback.

    A category is a property of the WORK, and ``default_expert`` maps it onto
    a worker that can produce that work — an executor gets a shell. The
    backlog tick always applied it; a job created directly (Officer
    hand-dispatch, the only mode in use while auto-pull is off) never did and
    fell through to the application default, a worker with ``shell: []``.
    See knowledge-base/knowledge/issues/category_expert_default_skipped_on_direct_dispatch.md.

    Precedence: a named expert, then a project or personal default (both are
    someone's deliberate choice), then the claimed ticket's ``expert:`` pin,
    then the category default, then the deployment-wide application default.
    The pin-over-category pair is ``work_categories.resolve_expert``'s. The
    slot's category is the contract the worker is held to (§6), so it decides
    over an explicit ``work_category`` that contradicts it.
    ``CATEGORY_EXPERTS`` membership is not consulted: it is warn-not-forbid,
    and nothing here refuses. Runs after the Officer stage because only that
    stage knows the slot and the ticket; ``context`` is the dict the later
    stages carry.
    """
    if not config.expert_is_fallback:
        return config
    category = normalize_category(slot_category) or normalize_category(
        requested_category
    )
    if ticket_expert:
        expert, source = ticket_expert, "ticket"
    elif category is not None:
        expert, source = default_expert(category), "category"
    else:
        return config
    if not bundled_expert_exists(expert):
        # Same reasoning as the explicit-slug check: a job whose config the
        # agent cannot load fails only after provisioning. The fallback is
        # the job this caller would have got before this default existed.
        logger.warning(
            "%s default expert %r for %s work is not installed; "
            "keeping the fallback expert",
            source,
            expert,
            category or "uncategorized",
        )
        return config
    selection: dict[str, Any] = {"source": source, "expert": expert}
    if category is not None:
        selection["category"] = category
    context["expert_selection"] = selection
    return replace(config, config_name=expert, expert_id=None, expert_is_fallback=False)
