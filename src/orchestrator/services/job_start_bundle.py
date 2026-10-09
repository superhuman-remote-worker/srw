"""The canonical worker start bundle and its repository preparation.

Extracted verbatim from ``orchestrator.main`` (R1.B05 lane J, census group
``R_DISPATCH``). ``build_job_start_request`` is the one seam both worker
delivery lanes share — the pinned push dispatcher and the stateless claim-bundle
endpoint — and it deliberately performs no delivery and no assignment. Each
caller keeps its own lane authority; the only lane-shaped knob here is
``persist_dispatch_state``, which the stateless assembler turns off because
credential resolution can outlive its queue lease, making the whole build
read-only from a stale claimant's point of view.

This module is **worker** preparation. Session preparation
(``_assemble_session_attach_payload``) is a deliberately different path and is
not merged into this one; the two share only the datasource payload builders in
``services.agent_datasource_payload``.

Refusals in this function are ordered and each one is fail-closed:
connector reauthorization, then the workspace runtime contract, then repository
transport authority, then the lite-tier/repository combination, then the
"no SSH remote" backstop, then the dispatch grant PEP (a ``GrantDenied`` is
caught *below* the generic resolve fallback so a denial is never downgraded to
an unchecked ``config_override``), then unroutable model slots. None may be
reordered or relaxed: several of them are the only thing standing between a
revoked connector or a moved workspace and a credential-bearing bundle.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Mapping, Protocol

from fastapi import HTTPException

from orchestrator.logging_config import bind_log_context, reset_log_context
from orchestrator.schemas.job_runtime import JobStartRequest
from orchestrator.security.access import externalize_gitea_url, redact_config_override
from orchestrator.services import connector_bind_time, connector_credential_leases
from orchestrator.services.config_resolver import unrouted_model_slots
from orchestrator.services.datasource_policy import SHELL_WORKSPACE_DETAIL
from orchestrator.services.job_datasource_selection import shell_connector_names
from orchestrator.services.job_workspace_runtime import (
    JobWorkspaceRuntimeDependencies,
    apply_sticky_sudo_denial,
    get_container_context,
    get_vm_context,
    inject_matching_workspace_config,
)
from orchestrator.services.managed_repository_authority import (
    ManagedRepositoryAuthorityError,
)
from orchestrator.services.manifest_execution_snapshot import (
    apply_srw_delivery_bindings,
    read_execution,
    srw_snapshot_config,
)
from shared.backend_kinds import LITE_BACKENDS
from shared.connectors.builtin import needs_knowledge_profile
from orchestrator.services.model_availability import (
    WHERE_JOB,
    ModelUnavailable,
    unavailable_slots,
)
from orchestrator.services.job_mutation_target import (
    FRESH_PINNED_RECIPIENT_ATTESTATION_ATTEMPTS,
    FRESH_PINNED_RECIPIENT_ATTESTATION_DELAY_S,
    PinnedJobMutationTarget,
)
from shared.runtime.core.loader import (
    canonical_config_name,
    deep_merge,
    normalize_llm_tiers,
)
from shared.workspace_contract import WORKSPACE_RUNTIME_CONTEXT_KEY


class JobStartBundleStore(Protocol):
    async def fetchrow(self, query: str, *args: Any) -> Any: ...

    async def get_project_repositories(
        self, project_id: str
    ) -> list[dict[str, Any]]: ...

    async def update_job_status(self, job_id: str, **kwargs: Any) -> Any: ...

    async def get_expert_by_id(self, expert_id: str) -> dict[str, Any] | None: ...

    async def store_resolved_config(
        self, job_id: str, resolved_config: dict[str, Any]
    ) -> Any: ...


@dataclass(frozen=True)
class JobStartBundleDependencies:
    """Per-invocation collaborators for worker bundle assembly.

    ``workspace_runtime`` is the nested dependency object of the module that
    owns workspace tier decisions. Everything else is a callable, including the
    other lane-J helpers this function calls: each of them is a name the
    application still exposes and its callers' tests steer, so resolving them
    in this module's namespace instead would silently bypass a patch (§P3).
    """

    store: JobStartBundleStore
    logger: logging.Logger
    forge: Any
    workspace_runtime: JobWorkspaceRuntimeDependencies

    # Lane J seams the application still owns (and its tests steer).
    inject_dispatch_credentials: Callable[..., Awaitable[dict[str, Any]]]
    resolve_authorized_job_datasources: Callable[
        [dict[str, Any]], Awaitable[list[dict[str, Any]]]
    ]
    job_project_repositories: Callable[
        [str | None], Awaitable[list[dict[str, Any]] | None]
    ]
    apply_cloud_storage_override: Callable[[list[dict[str, Any]], dict[str, Any]], None]
    build_datasources_payload: Callable[
        [list[dict[str, Any]]], list[dict[str, Any]] | None
    ]
    build_datasource_tool_override: Callable[
        [list[dict[str, Any]], dict[str, Any] | None], dict[str, Any]
    ]
    build_workspace_ssh_identities: Callable[
        [list[dict[str, Any]]], list[dict[str, Any]] | None
    ]

    # Repository authority (B04-era managed repository service).
    prepare_job_primary_repository_authority: Callable[..., Awaitable[Any]]
    prepare_project_repository_authority: Callable[..., Awaitable[Any]]
    authorize_job_repository_transport: Callable[..., Awaitable[Any]]

    # Runtime actor minting and blob credential injection.
    mint_worker_runtime_actor: Callable[..., Awaitable[Any]]
    inject_blob_credentials: Callable[..., Awaitable[dict[str, Any]]]

    # Lane P — feature gates, grants, lite workspace configuration.
    grant_denied_error: type[BaseException]
    lite_workspace_config_error: type[BaseException]
    backend_from_override: Callable[[Any], str | None]
    inject_lite_workspace_config: Callable[..., dict[str, Any]]
    is_experts_db_enabled: Callable[[], bool]
    user_experts_enabled: Callable[[], Awaitable[bool]]
    enforce_dispatch_grants: Callable[..., Awaitable[None]]
    grant_violations_detail: Callable[[list[str]], str]
    resolve_default_models: Callable[[Any], Awaitable[dict[str, Any]]]
    prefetch_roster_refs: Callable[..., Awaitable[Any]]

    # Lane C — registry model overrides.
    seed_registry_model_overrides: Callable[..., Awaitable[dict[str, Any] | None]]

    # B01 — skills in scope.
    gather_in_scope_skills: Callable[..., Awaitable[Any]]

    # Configuration resolution + deployment topology.
    resolve_config: Callable[..., dict[str, Any]]
    vm_workspaces_on_pod_network: Callable[[], bool]


def redispatch_livelock_trip(job: Mapping[str, Any]) -> Mapping[str, Any] | None:
    """Return the hidden active lease-recovery trip, if this row has one."""

    context = job.get("context") or {}
    if isinstance(context, str):
        try:
            context = json.loads(context)
        except (json.JSONDecodeError, TypeError):
            return None
    recovery = context.get("_lease_recovery") if isinstance(context, Mapping) else None
    if isinstance(recovery, Mapping) and recovery.get("state") == "tripped":
        return recovery
    return None


def mask_repository_transport(url: str | None) -> str:
    """Describe a repository URL for a log line without leaking userinfo."""
    if not url:
        return "none"
    from urllib.parse import urlsplit

    try:
        parts = urlsplit(str(url))
    except ValueError:
        return "unparseable"
    host = parts.hostname or "?"
    port = f":{parts.port}" if parts.port else ""
    return f"{parts.scheme}://{host}{port}{parts.path}"


async def job_project_repositories(
    project_id: str | None,
    *,
    dependencies: JobStartBundleDependencies,
) -> list[dict[str, Any]] | None:
    """Return the raw internal project-repository payload for dispatch."""

    if not project_id:
        return None
    repos = await dependencies.store.get_project_repositories(str(project_id))
    return [
        {
            "id": str(repository["id"]),
            "project_id": str(repository["project_id"]),
            "name": repository["name"],
            "role": repository["role"],
            "repo_url": repository.get("repo_url"),
            "read_only": repository["read_only"],
            "branch": repository.get("branch", "main"),
            "clone_path": repository.get("clone_path"),
            "credentials": repository.get("credentials"),
            "is_managed": bool(repository.get("is_managed")),
        }
        for repository in repos
    ] or None


async def prepare_job_repository_before_claim(
    job: Mapping[str, Any],
    *,
    dependencies: JobStartBundleDependencies,
) -> bool:
    """Adopt/prove historical managed Git authority before processing CAS.

    This is intentionally separate from runtime bundle assembly: the latter
    occurs after a pinned job has been claimed, while migration 0176 requires
    a proven scoped key at the claim boundary so an old replica cannot claim
    and dispatch a credential-bearing URL during a rolling upgrade.
    """

    postgres_db = dependencies.store
    gitea_client = dependencies.forge
    logger = dependencies.logger

    try:
        await dependencies.prepare_job_primary_repository_authority(
            postgres_db, gitea_client, job
        )
        if job.get("project_id"):
            repositories = await postgres_db.get_project_repositories(
                str(job["project_id"])
            )
            for repository in repositories:
                if not repository.get("is_managed"):
                    continue
                role = str(repository.get("role") or "")
                if role in {"knowledge", "jobs"}:
                    continue
                await dependencies.prepare_project_repository_authority(
                    postgres_db, gitea_client, repository
                )
        return True
    except ManagedRepositoryAuthorityError as exc:
        logger.warning(
            "Repository authority is not ready for job %s (%s)",
            job.get("id"),
            exc.code,
        )
        return False
    except Exception as exc:
        # This seam runs before the processing/lease CAS. An unavailable
        # database or forge must leave the job unclaimed rather than turn an
        # optional authority-tier outage into a credential-bearing dispatch
        # or a route-local 500. Log only the exception class: transport
        # exceptions can contain repository URLs in their message text.
        logger.warning(
            "Repository authority preparation failed for job %s (%s)",
            job.get("id"),
            type(exc).__name__,
        )
        return False


async def unavailable_models_before_claim(
    job: Mapping[str, Any],
    *,
    dependencies: JobStartBundleDependencies,
) -> ModelUnavailable | None:
    """The model refusal :func:`build_job_start_request` would raise, asked
    before the dispatcher claims the job (unavailable_model_handling.md S4).

    The start bundle refuses only after the claim, and pinned delivery and the
    stateless claim build it without persisting while completion commands own
    job status. There the refusal only logs: the job stays ``processing``, is
    re-queued when its lease expires and is claimed again on a fresh agent,
    forever. Asked here, the dispatcher fails it with a status CAS like its
    other pre-claim refusals.

    Reads what the bundle will run: the job's frozen execution snapshot when
    it has one (its account defaults already fell back when it was admitted,
    D2). A historical job without one is checked for its explicit pins only,
    its ``config_override`` over its expert's own configuration; an account
    default still falls back when such a job is resolved, so it is never
    refused here. Only the registry is asked; nothing is credentialed.

    ``None`` when every model can run, and when the check cannot tell (no
    registry source installed yet, or the check itself failed): the start
    bundle's refusal stays the backstop.
    """
    from shared.runtime.core import model_registry

    if not model_registry.lookups_registered():
        return None
    job_id = str(job["id"])
    postgres_db = dependencies.store
    try:
        execution_snapshot = await read_execution(postgres_db, "Job", job_id)
        if execution_snapshot is not None:
            frozen_blob, _frozen_policy = srw_snapshot_config(execution_snapshot)
            config = frozen_blob.get("agent")
        else:
            config = job.get("config_override") or {}
            if isinstance(config, str):
                config = json.loads(config)
            if dependencies.is_experts_db_enabled():
                # As resolve_config takes the request layer: a pre-U1 phase
                # tier is lifted into llm.model, or dropped beside an explicit
                # one. The flat path (experts DB off) delivers and checks the
                # tiers as they are.
                config = normalize_llm_tiers(config, source="request-override")
                expert_row = (
                    await postgres_db.get_expert_by_id(str(job["expert_id"]))
                    if job.get("expert_id")
                    else None
                )
                if expert_row is not None:
                    from shared.runtime.core.expert_resolution import (
                        build_expert_config,
                    )

                    expert_config, _prompts = build_expert_config({}, expert_row)
                    config = deep_merge(expert_config, config)
        found = await unavailable_slots(
            config,
            user_id=str(job["user_id"]) if job.get("user_id") else None,
            store=postgres_db,
            resolve_model=model_registry.resolve_model,
        )
    except Exception:  # noqa: BLE001 — the start bundle's refusal is the backstop
        dependencies.logger.warning(
            "Dispatch: model check before the claim failed for job %s; the start "
            "bundle decides",
            job_id,
            exc_info=True,
        )
        return None
    return ModelUnavailable(found) if found else None


async def _fail_unavailable_model(
    job_id: str,
    unavailable: ModelUnavailable,
    postgres_db: Any,
    persist_dispatch_state: bool,
    logger: logging.Logger,
) -> None:
    """Fail a job whose configured model cannot run, naming it
    (unavailable_model_handling.md §5)."""
    logger.error(
        "Dispatch: job %s refused, unavailable model slot(s): %s",
        job_id,
        [entry.as_dict() for entry in unavailable.entries],
    )
    if persist_dispatch_state:
        await postgres_db.update_job_status(
            job_id,
            status="failed",
            error_message=unavailable.message(where=WHERE_JOB),
        )


async def build_job_start_request(
    job: dict,
    *,
    persist_dispatch_state: bool = True,
    deliver_connector_leases: bool = True,
    dependencies: JobStartBundleDependencies,
) -> "JobStartRequest | None":
    """Build the canonical credential-complete worker start bundle.

    Both the pinned push dispatcher and stateless claim-bundle endpoint use
    this exact seam. It deliberately performs no delivery or assignment;
    callers retain their lane-specific authority. Pinned dispatch preserves
    the historical status/cache writes on a refused bundle. Stateless claim
    assembly disables those writes because credential resolution can outlive
    its queue lease; the final exact-token recheck then makes the whole build
    read-only from a stale claimant's point of view. It also turns off
    ``deliver_connector_leases`` and puts the lease tokens into the bundle
    inside its claim transaction instead.
    """
    postgres_db = dependencies.store
    gitea_client = dependencies.forge
    logger = dependencies.logger

    job_id = str(job["id"])
    _log_token = bind_log_context(job_id=job_id)
    try:
        # Extract upload IDs from context if present
        job_context = job.get("context") or {}
        if isinstance(job_context, str):
            job_context = json.loads(job_context)
        upload_id = job_context.get("upload_id")
        config_upload_id = job_context.get("config_upload_id")
        instructions_upload_id = job_context.get("instructions_upload_id")
        instructions = job_context.get("instructions")
        git_remote_url = job_context.get("git_remote_url")

        # Parse config_override if stored as string
        config_override = job.get("config_override")
        if isinstance(config_override, str):
            config_override = json.loads(config_override)
        execution_snapshot = await read_execution(postgres_db, "Job", job_id)
        frozen_blob = frozen_policy = None
        if execution_snapshot is not None:
            frozen_blob, frozen_policy = srw_snapshot_config(execution_snapshot)
            # New work reads the admitted specification. The old columns are
            # a historical compatibility projection, never a second source.
            config_override = dict(frozen_policy)

        # Build remaining context (fields not extracted as dedicated params)
        extracted_keys = {
            "upload_id",
            "config_upload_id",
            "instructions_upload_id",
            "instructions",
            "git_remote_url",
        }
        remaining_context = {
            k: v for k, v in job_context.items() if k not in extracted_keys
        }

        # Pass worktree_path to agent via context (for git worktree creation)
        if job.get("worktree_path"):
            remaining_context["worktree_path"] = job["worktree_path"]

        # Resolve project repositories if this is a project job
        repositories_payload = None
        if job.get("project_id"):
            try:
                repositories_payload = await dependencies.job_project_repositories(
                    str(job["project_id"])
                )
            except Exception as e:
                logger.warning(
                    f"Dispatch: failed to resolve project repos for job {job_id}: {e}"
                )

            # Legacy compatibility only: pre-migration project jobs stored a
            # branch but no per-job remote. A newly-created root whose isolated
            # repo provisioning failed has no branch; never attach that job to
            # the old shared jobs repo as an accidental fallback.
            if repositories_payload and not git_remote_url and job.get("branch_name"):
                jobs_repo = next(
                    (r for r in repositories_payload if r["role"] == "jobs"), None
                )
                if jobs_repo and jobs_repo.get("repo_url"):
                    git_remote_url = jobs_repo["repo_url"]

        # Reauthorize the complete materialized set immediately before any
        # credential payload is built. Scope/link/membership revocation fails
        # the job as one data contract; it is never silently reduced.
        try:
            resolved_ds = await dependencies.resolve_authorized_job_datasources(job)
        except HTTPException:
            logger.warning(
                "Dispatch: connector_unavailable for job %s", job_id, exc_info=True
            )
            if persist_dispatch_state:
                await postgres_db.update_job_status(
                    job_id,
                    status="failed",
                    error_message="connector_unavailable",
                )
            return None

        has_knowledge_scope = bool(job.get("project_id")) or any(
            needs_knowledge_profile(ds) for ds in (resolved_ds or [])
        )
        dependencies.apply_cloud_storage_override(resolved_ds, job_context)
        datasources_payload = dependencies.build_datasources_payload(resolved_ds)

        # Apply datasource-driven tool override (inject/strip db tool categories)
        if resolved_ds:
            config_override = dependencies.build_datasource_tool_override(
                resolved_ds, config_override
            )

        # Resolve exactly one runtime from the persisted assignment. Runtime
        # context order is never authority: a ready opposite-tier residue is
        # reported but cannot overwrite the selected backend.
        config_override, workspace_decision = inject_matching_workspace_config(
            job,
            config_override,
            replace_endpoint=True,
            dependencies=dependencies.workspace_runtime,
        )
        workspace_runtime_projection = workspace_decision.safe_projection()
        remaining_context[WORKSPACE_RUNTIME_CONTEXT_KEY] = workspace_runtime_projection
        if not workspace_decision.ready:
            msg = (
                "Workspace contract refused dispatch: "
                f"{workspace_decision.reason or workspace_decision.state}"
            )
            logger.error("Dispatch: job %s refused — %s", job_id, msg)
            if persist_dispatch_state and workspace_decision.state in {
                "invalid",
                "failed",
                "mismatch",
            }:
                await postgres_db.update_job_status(
                    job_id=job_id, status="failed", error_message=msg
                )
            return None

        managed_repository_credentials: list[dict[str, Any]] | None = None
        workspace_ssh_identities: list[dict[str, Any]] | None = None
        if workspace_decision.effective_backend in {"sandbox", "vm"}:
            # Connector keys only ever reach a shell-capable workspace.
            workspace_ssh_identities = dependencies.build_workspace_ssh_identities(
                resolved_ds
            )
            try:
                (
                    git_remote_url,
                    repositories_payload,
                    managed_repository_credentials,
                ) = await dependencies.authorize_job_repository_transport(
                    postgres_db,
                    gitea_client,
                    job,
                    repositories_payload,
                    backend=workspace_decision.effective_backend,
                )
            except ManagedRepositoryAuthorityError as exc:
                logger.warning(
                    "Dispatch: repository authority unavailable for job %s (%s)",
                    job_id,
                    exc.code,
                )
                return None

        if workspace_decision.effective_backend == "vm":
            if not dependencies.vm_workspaces_on_pod_network():
                if git_remote_url and not git_remote_url.startswith("ssh://srw-repo-"):
                    git_remote_url = externalize_gitea_url(git_remote_url)
                for repository in repositories_payload or []:
                    if not repository.get("is_managed") and repository.get("repo_url"):
                        repository["repo_url"] = externalize_gitea_url(
                            repository["repo_url"]
                        )
            logger.info(
                "Dispatch: injected attested VM workspace config for job %s",
                job_id,
            )
        elif workspace_decision.effective_backend == "sandbox":
            container_ctx = get_container_context(job)
            logger.info(
                "Dispatch: injected attested sandbox workspace config for job %s "
                "(provisioner=%s)",
                job_id,
                container_ctx.get("provisioner", "k8s"),
            )
        if workspace_decision.effective_backend in {"sandbox", "vm"}:
            logger.info(
                "Dispatch: repository transport for job %s: %s (%d managed "
                "credential(s))",
                job_id,
                mask_repository_transport(git_remote_url),
                len(managed_repository_credentials or []),
            )

        # Sticky sudo denial (vm_upgrade denied / resumed without VM): block
        # sudo with the operator's reason instead of re-freezing into a new
        # approval loop.
        config_override = apply_sticky_sudo_denial(job, config_override)

        # Inject lite workspace config (virtual/none — no SSH, no provisioning).
        # The user's config_override already names the backend; here we attach
        # the object-store mounts (virtual) with deployment-sourced credentials,
        # in-flight only. A repository datasource needs a real workspace to
        # clone into, so reject the combination up front (§4/§7).
        # Defense-in-depth: the submit-time guard already rejects an explicitly
        # selected repo, and create_job filters repos out of an *inherited*
        # lite selection — but this re-checks the fully resolved set, covering
        # resume / VM-resume and any future path that could attach a repo the
        # submit guard never saw.
        lite_backend = dependencies.backend_from_override(config_override)
        if lite_backend in LITE_BACKENDS:
            shell_names = shell_connector_names(resolved_ds, lite_backend)
            if shell_names:
                msg = (
                    "workspace.backend is a lite tier (virtual/none) but a "
                    f"connector requiring a shell is attached ({', '.join(shell_names)}). "
                    f"{SHELL_WORKSPACE_DETAIL} — use backend='sandbox' or 'vm'."
                )
                logger.error("Dispatch: job %s rejected — %s", job_id, msg)
                if persist_dispatch_state:
                    await postgres_db.update_job_status(
                        job_id=job_id, status="failed", error_message=msg
                    )
                return None
            try:
                config_override = dependencies.inject_lite_workspace_config(
                    config_override, prefix=f"jobs/{job_id}/"
                )
            except dependencies.lite_workspace_config_error as exc:
                logger.error("Dispatch: job %s lite-config error: %s", job_id, exc)
                if persist_dispatch_state:
                    await postgres_db.update_job_status(
                        job_id=job_id, status="failed", error_message=str(exc)
                    )
                return None
            logger.info(
                "Dispatch: job %s using lite workspace (backend=%s, no pod)",
                job_id,
                config_override["workspace"]["backend"],
            )

        # Override workspace_path with worktree_path for subjobs on shared backends
        worktree_path = job.get("worktree_path")
        if worktree_path and config_override:
            ws = config_override.get("workspace", {})
            remote = ws.get("remote", {})
            if remote:
                remote["workspace_path"] = worktree_path
                logger.info(
                    f"Dispatch: using worktree path {worktree_path} for job {job_id}"
                )

        # Backstop: never dispatch a workspace-backed job with no SSH remote.
        # A sandbox/vm backend without a `remote` block hard-fails the agent at
        # init_workspace (0 tokens, no log) — the failure mode from
        # knowledge-base/knowledge/issues/subjob_inherits_stale_workspace_container_snapshot.md. The
        # auto-assign dispatcher now resolves inherited workspaces up front, but
        # this guards every other dispatch path (manual assign, future callers):
        # fail fast with a diagnosable message instead of a cryptic agent crash.
        # `remote` is only ever injected into config_override (VM/container
        # blocks above); lite tiers (virtual/none) set an explicit backend and
        # legitimately have no remote, so they're exempt.
        _ws_final = (config_override or {}).get("workspace", {})
        _backend_final = _ws_final.get("backend")
        if _backend_final not in LITE_BACKENDS and not _ws_final.get("remote"):
            msg = (
                "Workspace backend requires SSH credentials but none were "
                f"resolved at dispatch (backend={_backend_final or 'sandbox (default)'}). "
                "For a subjob this usually means the parent's workspace container/VM "
                "was not ready; it should be held until ready rather than dispatched."
            )
            logger.error("Dispatch: job %s refused — %s", job_id, msg)
            if persist_dispatch_state:
                await postgres_db.update_job_status(
                    job_id=job_id, status="failed", error_message=msg
                )
            return None

        # Current work delivers its frozen snapshot with current authorization
        # and transient credentials. Only historical jobs without a snapshot
        # enter the former loader/compatibility branch below.
        resolved_config: dict[str, Any] | None = None
        if execution_snapshot is not None:
            try:
                _resolved, _policy = apply_srw_delivery_bindings(
                    frozen_blob, frozen_policy, config_override
                )
                if await dependencies.user_experts_enabled():
                    await dependencies.enforce_dispatch_grants(
                        _policy,
                        runner_user_id=str(job["user_id"])
                        if job.get("user_id")
                        else None,
                        project_ids=[str(job["project_id"])]
                        if job.get("project_id")
                        else [],
                        runner_kind=str(job.get("runner_kind") or "user"),
                    )
                resolved_config = await dependencies.inject_blob_credentials(
                    _resolved,
                    lambda co: dependencies.inject_dispatch_credentials(
                        job,
                        co,
                        include_kb_profile=has_knowledge_scope,
                    ),
                )
                if persist_dispatch_state:
                    await postgres_db.store_resolved_config(
                        job_id, redact_config_override(_resolved)
                    )
            except dependencies.grant_denied_error as gd:
                if persist_dispatch_state:
                    await postgres_db.update_job_status(
                        job_id,
                        status="failed",
                        error_message=dependencies.grant_violations_detail(
                            gd.violations
                        ),
                    )
                return None
            except ModelUnavailable as unavailable:
                await _fail_unavailable_model(
                    job_id, unavailable, postgres_db, persist_dispatch_state, logger
                )
                return None
        elif dependencies.is_experts_db_enabled():
            try:
                expert_row = None
                if job.get("expert_id"):
                    expert_row = await postgres_db.get_expert_by_id(
                        str(job["expert_id"])
                    )
                _base_name = canonical_config_name(
                    job.get("config_name") or "worker_base"
                )
                # Default-model floor (model names only): the base config carries
                # a placeholder model; the effective default is the user's pinned
                # model else the system capability default. Resolution applies it
                # below the expert; inject_blob_credentials adds the transport.
                _base_defaults = await dependencies.resolve_default_models(
                    job.get("user_id")
                )
                _cap: dict = {}
                _skills_payload = await dependencies.gather_in_scope_skills(
                    str(job["user_id"]) if job.get("user_id") else None,
                    [str(job["project_id"])] if job.get("project_id") else None,
                )
                # Per-model registry overrides (context_window; later
                # max_output_tokens) must reach the matrix as explicit llm keys,
                # else the blob bakes the family window and the admin cap is
                # silently dropped (see seed_registry_model_overrides).
                _req_override = await dependencies.seed_registry_model_overrides(
                    config_override,
                    user_id=str(job["user_id"]) if job.get("user_id") else None,
                )
                _resolved = dependencies.resolve_config(
                    base_config_name=_base_name,
                    base_defaults=_base_defaults,
                    expert_row=expert_row,
                    request_override=_req_override,
                    expert_type="worker",
                    capture=_cap,
                    skills=_skills_payload,
                    db_refs=await dependencies.prefetch_roster_refs(
                        expert_row=expert_row,
                        overrides=(config_override,),
                        user_id=str(job["user_id"]) if job.get("user_id") else None,
                        project_ids=[str(job["project_id"])]
                        if job.get("project_id")
                        else [],
                    ),
                )
                # Bound skills are delivered deterministically (instructions channel);
                # strip them from the model-invoked catalog so they aren't double-offered.
                from shared.runtime.core.skill_resolution import filter_bound_skills

                filter_bound_skills(_resolved)
                # Dispatch PEP (decision 9): the merged config must fit the runner's
                # grants. GrantDenied is caught BELOW the generic fallback so a denial
                # is never downgraded to the unchecked config_override (fail closed).
                if await dependencies.user_experts_enabled():
                    await dependencies.enforce_dispatch_grants(
                        _cap["merged_fragment"],
                        runner_user_id=str(job["user_id"])
                        if job.get("user_id")
                        else None,
                        project_ids=[str(job["project_id"])]
                        if job.get("project_id")
                        else [],
                        runner_kind=str(job.get("runner_kind") or "user"),
                    )
                resolved_config = await dependencies.inject_blob_credentials(
                    _resolved,
                    lambda co: dependencies.inject_dispatch_credentials(
                        job,
                        co,
                        include_kb_profile=has_knowledge_scope,
                    ),
                )
                if persist_dispatch_state:
                    await postgres_db.store_resolved_config(
                        job_id, redact_config_override(resolved_config)
                    )
                logger.info(
                    "Dispatch: resolved config for job %s (expert_id=%s)",
                    job_id,
                    job.get("expert_id"),
                )
            except dependencies.grant_denied_error as gd:
                logger.warning("Dispatch denied for job %s: %s", job_id, gd)
                if persist_dispatch_state:
                    await postgres_db.update_job_status(
                        job_id,
                        status="failed",
                        error_message=dependencies.grant_violations_detail(
                            gd.violations
                        ),
                    )
                return None
            except ModelUnavailable as unavailable:
                # Fail closed: the flat fallback would run the same model.
                await _fail_unavailable_model(
                    job_id, unavailable, postgres_db, persist_dispatch_state, logger
                )
                return None
            except Exception:
                logger.exception(
                    "Dispatch: resolve_config failed for job %s; falling back "
                    "to config_name + config_override",
                    job_id,
                )
                resolved_config = None

        # Resolve API keys, model routing, and capability defaults.
        # Same helper drives both first-dispatch and resume so an orphaned
        # job re-dispatched to a fresh agent doesn't lose its credentials.
        # (Still injected into config_override for the no-blob fallback path.)
        try:
            config_override = await dependencies.inject_dispatch_credentials(
                job,
                config_override,
                include_kb_profile=has_knowledge_scope,
                # Only the config the agent runs refuses: with a delivered blob
                # this flat copy is not sent.
                strict=resolved_config is None,
            )
        except ModelUnavailable as unavailable:
            await _fail_unavailable_model(
                job_id, unavailable, postgres_db, persist_dispatch_state, logger
            )
            return None
        # Log injected env-key NAMES (never values) so a missing credential —
        # e.g. EMBEDDING_API_KEY, which silently disables memory + KB — is
        # greppable at dispatch (embedding_key_missing_silently_disables_memory_and_kb.md).
        logger.info(
            "Dispatch: job %s injected env_key names=%s",
            job_id,
            sorted((config_override.get("env_keys") or {}).keys()),
        )

        # Fail fast on a pinned model with no resolvable transport rather than
        # letting the agent silently fall back to api.openai.com and 401/404 with
        # an opaque error (eec20eeb). Only the blob path is validated; the
        # no-blob fallback keeps its legacy behaviour.
        if resolved_config:
            _unrouted = unrouted_model_slots(resolved_config)
            if _unrouted:
                msg = (
                    "Pinned model(s) have no resolvable endpoint or provider after "
                    f"dispatch resolution: {', '.join(_unrouted)}. Set the model's "
                    "endpoint/provider key (Admin → Providers / Models) or pin a "
                    "different model."
                )
                logger.error(
                    "Dispatch: job %s has unroutable model slot(s) — %s",
                    job_id,
                    _unrouted,
                )
                if persist_dispatch_state:
                    await postgres_db.update_job_status(
                        job_id, status="failed", error_message=msg
                    )
                return None

        # Credential leases (connector drivers C2): a lease connector's entry
        # carries a lease token, never its upstream credential. Issued or
        # delivered again only after every refusal above, so a refused
        # bundle mints nothing. The stateless claim defers this to its claim
        # transaction, where the run_queue lease is re-checked.
        if deliver_connector_leases:
            lease_owner = connector_credential_leases.job_lease_owner(job)
            # A service driver's image is looked up first, outside any
            # transaction (D5); the delivery then only applies it. A
            # registered driver's bind is never waited for here: the
            # dispatcher's preflight held the job until it was bound (D6), and
            # until each provider-minted credential was live (C5).
            await connector_credential_leases.prepare_lease_delivery(
                postgres_db, datasources_payload, owner=lease_owner, bind_wait=0
            )
            try:
                await connector_credential_leases.deliver_connector_leases_with(
                    postgres_db,
                    datasources_payload,
                    owner=lease_owner,
                )
            except connector_bind_time.BindTimeError as exc:
                # A bind still running or retrying waits for a later dispatch;
                # one that failed for good fails the job with its reason.
                logger.warning("Dispatch: job %s connector binds: %s", job_id, exc)
                if persist_dispatch_state and isinstance(
                    exc, connector_bind_time.BindTimeRefused
                ):
                    await postgres_db.update_job_status(
                        job_id, status="failed", error_message=str(exc)[:1000]
                    )
                return None

        # Build job start request. resolved_config and config_override are
        # mutually exclusive on the wire: a delivered blob is complete, so we
        # send config_override=None to keep the agent from flat-merging an
        # override on top of the resolved layers (the degradation we set out to
        # fix).
        runtime_actor = await dependencies.mint_worker_runtime_actor(
            postgres_db,
            project_id=str(job["project_id"]) if job.get("project_id") else None,
            user_id=str(job["user_id"]) if job.get("user_id") else None,
        )
        job_start = JobStartRequest(
            job_id=job_id,
            description=job["description"],
            upload_id=upload_id,
            config_upload_id=config_upload_id,
            instructions_upload_id=instructions_upload_id,
            instructions=instructions,
            document_path=job.get("document_path"),
            config_name=canonical_config_name(job.get("config_name") or "worker_base"),
            config_override=None if resolved_config else config_override,
            resolved_config=resolved_config,
            git_remote_url=git_remote_url,
            context=remaining_context if remaining_context else None,
            datasources=datasources_payload,
            repositories=repositories_payload,
            managed_repository_credentials=managed_repository_credentials,
            workspace_ssh_identities=workspace_ssh_identities,
            branch_name=job.get("branch_name"),
            project_id=str(job["project_id"]) if job.get("project_id") else None,
            runtime_actor=runtime_actor.to_payload(),
            workspace_runtime=workspace_runtime_projection,
            workspace_provisioner=(
                (str(get_container_context(job).get("provisioner") or "") or None)
                if workspace_decision.effective_backend == "sandbox"
                else (
                    str(get_vm_context(job).get("provisioner") or "vm")
                    if workspace_decision.effective_backend == "vm"
                    else None
                )
            ),
        )

        return job_start

    except Exception as e:
        logger.error(
            "Dispatch: failed to build start bundle for job %s: %s",
            job_id,
            e,
            exc_info=True,
        )
        return None
    finally:
        reset_log_context(_log_token)


__all__ = [
    "FRESH_PINNED_RECIPIENT_ATTESTATION_ATTEMPTS",
    "FRESH_PINNED_RECIPIENT_ATTESTATION_DELAY_S",
    "JobStartBundleDependencies",
    "JobStartBundleStore",
    "PinnedJobMutationTarget",
    "build_job_start_request",
    "job_project_repositories",
    "mask_repository_transport",
    "prepare_job_repository_before_claim",
    "redispatch_livelock_trip",
    "unavailable_models_before_claim",
]
