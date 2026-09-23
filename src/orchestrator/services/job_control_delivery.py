"""Pinned job delivery and dispatcher pause operations.

Every stateful authority is supplied by the application. Completion mode is
read at each decision so legacy and command modes keep one serialization point.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from typing import Any, Callable

from fastapi import HTTPException
from orchestrator.security.access import (
    externalize_gitea_url,
    vm_workspaces_on_pod_network,
)
from orchestrator.services.config_resolver import (
    inject_blob_credentials,
    resolve_config,
)
from orchestrator.services.container_provisioner import WorkspaceRuntimeAuthorityError
from orchestrator.services.grant_enforcement import GrantDenied
from orchestrator.services.managed_repository_authority import (
    ManagedRepositoryAuthorityError,
)
from orchestrator.services.manifest_runtime_ownership import uses_srw_runtime
from orchestrator.services.workspace_tier_policy import LiteWorkspaceConfigError
from shared.backend_kinds import LITE_BACKENDS
from shared.runtime.core.loader import canonical_config_name
from shared.workspace_contract import WORKSPACE_RUNTIME_CONTEXT_KEY
from shared.pinned_job_delivery import (
    pinned_job_delivery_proof, pinned_job_projection_digest,
)
from shared.vm_lifecycle_auth import (
    LifecycleAuthConfigurationError, configured_secret,
)


async def _pinned_vm_delivery_intent(
    dependencies: "JobDeliveryDependencies", *, job_id: str, agent_id: str,
    recipient: Any, payload: dict[str, Any],
    consumed_context: dict[str, Any] | None = None,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    """Add a replayable exact delivery ID only for a supported VM recipient."""

    if (
        os.getenv("WORKSPACE_IDLE_RELEASE_ENABLED", "false").lower() != "true"
        or not isinstance(payload.get("workspace_runtime"), dict)
        or payload["workspace_runtime"].get("assigned_backend") != "vm"
        or payload["workspace_runtime"].get("effective_backend") != "vm"
        or payload["workspace_runtime"].get("state") != "ready"
        or not getattr(recipient, "expected_pod_uid", None)
    ):
        return payload, None
    try:
        secret = configured_secret()
    except LifecycleAuthConfigurationError:
        secret = None
    if secret is None:
        return payload, None
    digest = pinned_job_projection_digest(payload)
    intent = await dependencies.store.prepare_pinned_job_delivery(
        job_id, agent_id, recipient=recipient, projection_digest=digest,
        consumed_context=consumed_context,
    )
    if intent is None:
        return None, None
    proof = pinned_job_delivery_proof(
        secret, delivery_id=str(intent["id"]), agent_id=agent_id,
        process_generation=recipient.expected_process_generation,
        pod_uid=recipient.expected_pod_uid, projection_digest=digest,
    )
    return {
        **payload,
        "pinned_delivery_id": str(intent["id"]),
        "pinned_projection_digest": digest,
        "pinned_delivery_proof": proof,
    }, intent


@dataclass(frozen=True, slots=True)
class JobDeliveryDependencies:
    store: Any
    logger: logging.Logger
    completion_commands_enabled: Callable[[], bool]
    http_client_factory: Callable[..., Any]
    completion_control: Any
    pause_pending_job_ids: set[str]
    gitea_client: Any
    workspace_context_keys: dict[str, str]
    prepare_job_workspace_runtime: Callable[..., Any]
    attest_pinned_k8s_job_workspace: Callable[..., Any]
    build_job_start_request: Callable[..., Any]
    pinned_k8s_job_workspace_authority_is_current: Callable[..., Any]
    prepare_pinned_job_mutation_target: Callable[..., Any]
    redispatch_livelock_trip: Callable[..., Any]
    bind_log_context: Callable[..., Any]
    reset_log_context: Callable[..., Any]
    resume_missing_workspace: Callable[..., Any]
    resolve_authorized_job_datasources: Callable[..., Any]
    apply_cloud_storage_override: Callable[..., Any]
    build_datasources_payload: Callable[..., Any]
    job_project_repositories: Callable[..., Any]
    build_datasource_tool_override: Callable[..., Any]
    inject_matching_workspace_config: Callable[..., Any]
    get_container_context: Callable[..., Any]
    get_vm_context: Callable[..., Any]
    authorize_job_repository_transport: Callable[..., Any]
    apply_sticky_sudo_denial: Callable[..., Any]
    backend_from_override: Callable[..., Any]
    repository_datasource_names: Callable[..., Any]
    inject_lite_workspace_config: Callable[..., Any]
    is_experts_db_enabled: Callable[..., Any]
    user_experts_enabled: Callable[..., Any]
    enforce_dispatch_grants: Callable[..., Any]
    gather_in_scope_skills: Callable[..., Any]
    seed_registry_model_overrides: Callable[..., Any]
    resolve_default_models: Callable[..., Any]
    prefetch_roster_refs: Callable[..., Any]
    inject_dispatch_credentials: Callable[..., Any]
    grant_violations_detail: Callable[..., Any]
    mint_worker_runtime_actor: Callable[..., Any]
    resume_reject_should_requeue: Callable[..., Any]


class JobDeliveryOperations:
    """Bind one application's delivery collaborators for scheduler callers."""

    def __init__(self, dependencies: JobDeliveryDependencies) -> None:
        self.dependencies = dependencies

    async def dispatch(self, job: dict[str, Any], agent: dict[str, Any]) -> bool:
        return await dispatch_job_to_agent(job, agent, dependencies=self.dependencies)

    async def resume(self, job: dict[str, Any], agent: dict[str, Any]) -> bool:
        return await resume_job_on_agent(job, agent, dependencies=self.dependencies)

    async def initiate_pause(self, job: dict[str, Any]) -> None:
        await initiate_pause(job, dependencies=self.dependencies)


async def dispatch_job_to_agent(
    job: dict[str, Any], agent: dict[str, Any], *, dependencies: JobDeliveryDependencies
) -> bool:
    """Build and push a fresh job to a registered pinned agent."""
    job_id = str(job["id"])
    agent_id = str(agent["id"])
    if not uses_srw_runtime(job):
        return False

    # Defense in depth for callers that bypass get_dispatchable_jobs (notably
    # the manual admin assignment endpoint). A stateless row must never be
    # POSTed to a pinned agent; its sole claim authority is run_queue (§5.4.4).
    if job.get("execution_lane", "pinned") != "pinned":
        dependencies.logger.warning(
            "Dispatch: refusing pinned start for %s job %s",
            job.get("execution_lane"),
            job_id,
        )
        return False
    if dependencies.redispatch_livelock_trip(job) is not None:
        dependencies.logger.error(
            "Dispatch: refusing circuit-tripped redispatch-livelock job %s",
            job_id,
        )
        return False
    if not agent.get("pod_ip"):
        dependencies.logger.warning(
            "Agent %s has no pod IP — skipping dispatch", agent_id
        )
        return False

    (
        workspace_action,
        job,
        workspace_reason,
    ) = await dependencies.prepare_job_workspace_runtime(job)
    if workspace_action != "proceed":
        dependencies.logger.warning(
            "Dispatch: job %s waiting for workspace authority recovery (%s)",
            job_id,
            workspace_reason or workspace_action,
        )
        return False

    durable_job = job
    try:
        job, workspace_authority = await dependencies.attest_pinned_k8s_job_workspace(
            job
        )
    except WorkspaceRuntimeAuthorityError as exc:
        dependencies.logger.warning(
            "Dispatch: refusing unattested Kubernetes workspace for job %s (%s)",
            job_id,
            exc,
        )
        return False

    _log_token = dependencies.bind_log_context(job_id=job_id, agent_id=agent_id)
    try:
        job_start = await dependencies.build_job_start_request(
            job,
            persist_dispatch_state=not dependencies.completion_commands_enabled(),
        )
        if job_start is None:
            return False

        if workspace_authority is not None:
            attested = workspace_authority.attestation
            job_start = job_start.model_copy(
                update={
                    "workspace_provisioner": "k8s",
                    "workspace_generation": attested.workspace_generation,
                    "workspace_runtime_incarnation": attested.runtime_incarnation,
                    "workspace_ssh_host_key_fingerprint": (
                        attested.ssh_host_key_fingerprint
                    ),
                    "workspace_owner_kind": workspace_authority.owner.kind,
                    "workspace_owner_id": workspace_authority.owner.id,
                }
            )

        if not await dependencies.pinned_k8s_job_workspace_authority_is_current(
            durable_job, workspace_authority
        ):
            dependencies.logger.warning(
                "Dispatch: workspace authority changed while assembling job %s",
                job_id,
            )
            return False
        if not await dependencies.store.managed_repository_authorities_are_current(
            job_start.managed_repository_credentials
        ):
            dependencies.logger.warning(
                "Dispatch: repository authority changed while assembling job %s",
                job_id,
            )
            return False

        if not await dependencies.pinned_k8s_job_workspace_authority_is_current(
            durable_job, workspace_authority
        ):
            dependencies.logger.warning(
                "Dispatch: workspace authority changed at delivery for job %s",
                job_id,
            )
            return False

        target = await dependencies.prepare_pinned_job_mutation_target(
            agent_id=agent_id,
            job_id=job_id,
            require_idle=True,
        )
        if target is None:
            return False
        job_start = job_start.model_copy(update={"recipient": target.recipient})
        delivery_payload, delivery_intent = await _pinned_vm_delivery_intent(
            dependencies, job_id=job_id, agent_id=agent_id,
            recipient=target.recipient,
            payload=job_start.model_dump(mode="json", exclude_none=True),
        )
        if delivery_payload is None:
            return False
        agent_url = (
            f"http://{target.agent['pod_ip']}:{target.agent['pod_port']}/job/start"
        )
        async with dependencies.http_client_factory(timeout=30.0) as client:
            response = await client.post(
                agent_url,
                json=delivery_payload,
            )
        if response.status_code not in (200, 202):
            dependencies.logger.warning(
                "Dispatch: agent %s rejected job %s (status=%s)",
                agent_id,
                job_id,
                response.status_code,
            )
            return False

        accepted_delivery_id = None
        if delivery_intent is not None:
            try:
                acknowledgement = response.json()
            except (ValueError, TypeError):
                acknowledgement = {}
            if (
                isinstance(acknowledgement, dict)
                and acknowledgement.get("pinned_delivery_id") == str(delivery_intent["id"])
                and acknowledgement.get("pinned_projection_digest")
                    == delivery_payload["pinned_projection_digest"]
            ):
                accepted_delivery_id = str(delivery_intent["id"])
            else:
                dependencies.logger.warning(
                    "Dispatch: pinned delivery acknowledgment unavailable for job %s",
                    job_id,
                )

        if dependencies.completion_commands_enabled():
            if not await dependencies.store.confirm_pinned_job_dispatch(
                job_id, agent_id,
                pinned_delivery_id=accepted_delivery_id,
                pinned_projection_digest=(
                    delivery_payload.get("pinned_projection_digest")
                    if accepted_delivery_id else None
                ),
            ):
                dependencies.logger.warning(
                    "Dispatch: stale success from agent %s for job %s; "
                    "control/ownership changed",
                    agent_id,
                    job_id,
                )
                return False
        elif not await dependencies.store.update_job_status(
            job_id=job_id,
            status="processing",
            assigned_agent_id=agent_id,
            # The pre-POST claim already wrote this. A pause or cancel that
            # won while the start was in flight must not be overwritten.
            expected_status="processing",
        ):
            dependencies.logger.warning(
                "Dispatch: job %s changed while agent %s accepted it; not "
                "reasserting processing",
                job_id,
                agent_id,
            )
            return False
        await dependencies.store.heartbeat(
            agent_id=agent_id,
            status="working",
            current_job_id=job_id,
        )
        dependencies.logger.info(
            "Dispatch: assigned job %s (priority=%s) to agent %s",
            job_id,
            job.get("priority", "?"),
            agent_id,
        )
        return True
    except Exception as exc:
        dependencies.logger.error(
            "Dispatch: failed to assign job %s to agent %s: %s",
            job_id,
            agent_id,
            exc,
            exc_info=True,
        )
        return False
    finally:
        dependencies.reset_log_context(_log_token)


async def resume_job_on_agent(
    job: dict[str, Any], agent: dict[str, Any], *, dependencies: JobDeliveryDependencies
) -> bool:
    """Resume a paused job on an agent. Returns True on success."""

    job_id = str(job["id"])
    agent_id = str(agent["id"])
    if not uses_srw_runtime(job):
        return False

    # Same coexistence fence as the fresh-start helper. Resume is a direct
    # POST to a registered pod and therefore belongs exclusively to pinned jobs.
    if job.get("execution_lane", "pinned") != "pinned":
        dependencies.logger.warning(
            "Resume dispatch: refusing pinned resume for %s job %s",
            job.get("execution_lane"),
            job_id,
        )
        return False

    if dependencies.redispatch_livelock_trip(job) is not None:
        dependencies.logger.error(
            "Resume dispatch: refusing circuit-tripped redispatch-livelock job %s",
            job_id,
        )
        return False

    if not agent.get("pod_ip"):
        dependencies.logger.warning(
            f"Agent {agent_id} has no pod IP — skipping resume dispatch"
        )
        return False

    (
        workspace_action,
        job,
        workspace_reason,
    ) = await dependencies.prepare_job_workspace_runtime(job)
    if workspace_action != "proceed":
        dependencies.logger.warning(
            "Resume dispatch: job %s waiting for workspace authority recovery (%s)",
            job_id,
            workspace_reason or workspace_action,
        )
        return False

    durable_job = job
    try:
        job, workspace_authority = await dependencies.attest_pinned_k8s_job_workspace(
            job
        )
    except WorkspaceRuntimeAuthorityError as exc:
        dependencies.logger.warning(
            "Resume dispatch: refusing unattested Kubernetes workspace for job %s (%s)",
            job_id,
            exc,
        )
        return False

    # Never ship a workspace-backed job with no workspace to dial: the VM and
    # container blocks below inject `remote` only when the context says 'ready',
    # so without this the agent gets a backend and no host and hard-fails at
    # init_workspace (0 tokens, no log). Returning False is this function's
    # "caller should queue" contract — the dispatcher is the only thing that
    # provisions, so the job has to go back to it.
    missing_workspace = dependencies.resume_missing_workspace(job)
    if missing_workspace:
        dependencies.logger.warning(
            "Resume dispatch: job %s has no live %s workspace — refusing direct "
            "resume so the dispatcher can re-provision it",
            job_id,
            missing_workspace,
        )
        # Shed the parked context too, or the job never actually heals: the
        # dispatcher reads the same context to decide what to build, sees a
        # 'failed' VM, and parks it again. On the dispatcher's own resume path
        # (no shed anywhere else) that would bounce the job every tick without
        # ever rebuilding the workspace. Best-effort — refusing the resume is
        # valid on its own, and this runs outside the try below, so letting a
        # shed error escape would break callers that expect a bool.
        if not dependencies.completion_commands_enabled():
            try:
                await dependencies.store.shed_workspace_context(
                    job_id, dependencies.workspace_context_keys[missing_workspace]
                )
            except Exception:
                dependencies.logger.warning(
                    "Resume dispatch: could not shed stale %s context for job %s",
                    missing_workspace,
                    job_id,
                    exc_info=True,
                )
        return False

    try:
        # Resume is another credential-delivery boundary. Preserve the stored
        # set, reauthorize it as a whole, and fail closed on any revoked row.
        try:
            resolved_ds = await dependencies.resolve_authorized_job_datasources(job)
        except HTTPException:
            dependencies.logger.warning(
                "Resume dispatch: connector_unavailable for job %s",
                job_id,
                exc_info=True,
            )
            if not dependencies.completion_commands_enabled():
                await dependencies.store.update_job_status(
                    job_id,
                    status="failed",
                    error_message="connector_unavailable",
                )
            return False

        has_knowledge_scope = bool(job.get("project_id")) or any(
            str(ds.get("type") or "").lower() == "kb" for ds in (resolved_ds or [])
        )
        job_context = job.get("context") or {}
        if isinstance(job_context, str):
            job_context = json.loads(job_context)
        dependencies.apply_cloud_storage_override(resolved_ds, job_context)
        datasources_payload = dependencies.build_datasources_payload(resolved_ds)
        try:
            repositories_payload = await dependencies.job_project_repositories(
                str(job["project_id"]) if job.get("project_id") else None
            )
        except Exception:
            dependencies.logger.warning(
                "Resume dispatch: project repositories unavailable for job %s",
                job_id,
                exc_info=True,
            )
            return False

        config_override = job.get("config_override")
        if isinstance(config_override, str):
            config_override = json.loads(config_override)

        from orchestrator.services.manifest_execution_snapshot import (
            apply_srw_delivery_bindings,
            read_execution,
            srw_snapshot_config,
        )

        execution_snapshot = await read_execution(dependencies.store, "Job", job_id)
        frozen_blob = frozen_policy = None
        if execution_snapshot is not None:
            frozen_blob, frozen_policy = srw_snapshot_config(execution_snapshot)
            config_override = dict(frozen_policy)

        if resolved_ds:
            config_override = dependencies.build_datasource_tool_override(
                resolved_ds, config_override
            )

        config_override, workspace_decision = (
            dependencies.inject_matching_workspace_config(
                job, config_override, replace_endpoint=True
            )
        )
        if not workspace_decision.ready:
            dependencies.logger.warning(
                "Resume dispatch: job %s refused by workspace contract (%s)",
                job_id,
                workspace_decision.reason or workspace_decision.state,
            )
            return False
        if workspace_decision.effective_backend == "vm":
            dependencies.logger.info(
                "Resume dispatch: injected attested VM workspace config for job %s",
                job_id,
            )
        elif workspace_decision.effective_backend == "sandbox":
            container_ctx = dependencies.get_container_context(job)
            worktree_path = job.get("worktree_path")
            if worktree_path:
                config_override["workspace"]["remote"]["workspace_path"] = worktree_path
            dependencies.logger.info(
                "Resume dispatch: injected attested sandbox workspace config for "
                "job %s (provisioner=%s)",
                job_id,
                container_ctx.get("provisioner", "k8s"),
            )

        try:
            (
                git_remote_url,
                repositories_payload,
                managed_repository_credentials,
            ) = await dependencies.authorize_job_repository_transport(
                dependencies.store,
                dependencies.gitea_client,
                job,
                repositories_payload,
                backend=str(workspace_decision.effective_backend),
            )
        except ManagedRepositoryAuthorityError as exc:
            dependencies.logger.warning(
                "Resume dispatch: repository authority unavailable for job %s (%s)",
                job_id,
                exc.code,
            )
            return False
        if workspace_decision.effective_backend == "vm":
            if not vm_workspaces_on_pod_network():
                if git_remote_url and not git_remote_url.startswith("ssh://srw-repo-"):
                    git_remote_url = externalize_gitea_url(git_remote_url)
                for repository in repositories_payload or []:
                    if not repository.get("is_managed") and repository.get("repo_url"):
                        repository["repo_url"] = externalize_gitea_url(
                            repository["repo_url"]
                        )

        # Sticky sudo denial (vm_upgrade denied / resumed without VM): block
        # sudo with the operator's reason instead of re-freezing into a new
        # approval loop.
        config_override = dependencies.apply_sticky_sudo_denial(job, config_override)

        # Re-inject lite workspace config on resume. Mounts + credentials are
        # injected in-flight and never persisted to jobs.config_override, so the
        # paused row only carries the bare backend — without this a resumed
        # `virtual` job would reach the agent with no mounts and fail to build
        # its backend. (Same rationale as the credential re-injection above.)
        if dependencies.backend_from_override(config_override) in LITE_BACKENDS:
            repo_names = dependencies.repository_datasource_names(resolved_ds)
            if repo_names:
                msg = (
                    "workspace.backend is a lite tier (virtual/none) but a "
                    f"connector requiring a shell is attached ({', '.join(repo_names)}). "
                    "Repository and credential connectors need a full workspace — use "
                    "backend='sandbox' or 'vm'."
                )
                dependencies.logger.error(
                    "Resume dispatch: job %s rejected — %s", job_id, msg
                )
                if not dependencies.completion_commands_enabled():
                    await dependencies.store.update_job_status(
                        job_id=job_id, status="failed", error_message=msg
                    )
                return False
            try:
                config_override = dependencies.inject_lite_workspace_config(
                    config_override, prefix=f"jobs/{job_id}/"
                )
            except LiteWorkspaceConfigError as exc:
                dependencies.logger.error(
                    "Resume dispatch: job %s lite-config error: %s", job_id, exc
                )
                if not dependencies.completion_commands_enabled():
                    await dependencies.store.update_job_status(
                        job_id=job_id, status="failed", error_message=str(exc)
                    )
                return False

        # Resolve the same complete, layered config used for a fresh dispatch,
        # after datasource/workspace overlays have been attached. The runtime
        # user-experts switch controls grant enforcement only; it must not make
        # a resumed job fall back to the generic pod's boot config. The env flag
        # remains the compatibility switch for resolved-config delivery.
        experts_db_enabled = dependencies.is_experts_db_enabled()
        resolved_resume_supported = False
        if experts_db_enabled or execution_snapshot is not None:
            ready_url = f"http://{agent['pod_ip']}:{agent['pod_port']}/ready"
            try:
                async with dependencies.http_client_factory(timeout=5.0) as client:
                    ready_response = await client.get(ready_url)
                if ready_response.status_code == 200:
                    ready_payload = ready_response.json()
                    resolved_resume_supported = bool(
                        (ready_payload.get("capabilities") or {}).get(
                            "resolved_config_resume"
                        )
                    )
            except Exception as exc:
                # Rolling-upgrade compatibility is fail-safe: an old agent (or
                # one whose readiness surface cannot be probed) receives the
                # same credential-injected flat override it understood before.
                dependencies.logger.info(
                    "Resume dispatch: agent %s resolved-config capability "
                    "unavailable (%s); using legacy payload",
                    agent_id,
                    type(exc).__name__,
                )
        user_experts_enabled = await dependencies.user_experts_enabled()
        resolved_config: dict[str, Any] | None = None
        if execution_snapshot is not None:
            if not resolved_resume_supported:
                dependencies.logger.info(
                    "Resume requires a snapshot-capable SRW recipient for job %s",
                    job_id,
                )
                return False
            try:
                _resolved, _policy = apply_srw_delivery_bindings(
                    frozen_blob, frozen_policy, config_override
                )
                if user_experts_enabled:
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
                resolved_config = await inject_blob_credentials(
                    _resolved,
                    lambda co: dependencies.inject_dispatch_credentials(
                        job, co, include_kb_profile=has_knowledge_scope
                    ),
                )
            except GrantDenied as gd:
                dependencies.logger.warning("Resume denied for job %s: %s", job_id, gd)
                return False
        elif resolved_resume_supported or user_experts_enabled:
            try:
                _rbase = canonical_config_name(job.get("config_name") or "worker_base")
                _rcap: dict = {}
                _skills_payload = await dependencies.gather_in_scope_skills(
                    str(job["user_id"]) if job.get("user_id") else None,
                    [str(job["project_id"])] if job.get("project_id") else None,
                )
                _req_override = await dependencies.seed_registry_model_overrides(
                    config_override,
                    user_id=str(job["user_id"]) if job.get("user_id") else None,
                )
                _rexpert_row = (
                    await dependencies.store.get_expert_by_id(str(job["expert_id"]))
                    if job.get("expert_id")
                    else None
                )
                _resolved = resolve_config(
                    base_config_name=_rbase,
                    base_defaults=await dependencies.resolve_default_models(
                        job.get("user_id")
                    ),
                    expert_row=_rexpert_row,
                    request_override=_req_override,
                    expert_type="worker",
                    capture=_rcap,
                    skills=_skills_payload,
                    db_refs=await dependencies.prefetch_roster_refs(
                        expert_row=_rexpert_row,
                        overrides=(config_override,),
                        user_id=str(job["user_id"]) if job.get("user_id") else None,
                        project_ids=[str(job["project_id"])]
                        if job.get("project_id")
                        else [],
                    ),
                )
                from shared.runtime.core.skill_resolution import filter_bound_skills

                filter_bound_skills(_resolved)

                # Resume PEP (decision 9, B3): a grant revoked since dispatch
                # must still block the resume. Keep this check before credential
                # injection and delivery, and fail closed on an explicit denial.
                if user_experts_enabled:
                    await dependencies.enforce_dispatch_grants(
                        _rcap["merged_fragment"],
                        runner_user_id=(
                            str(job["user_id"]) if job.get("user_id") else None
                        ),
                        project_ids=[str(job["project_id"])]
                        if job.get("project_id")
                        else [],
                        runner_kind=str(job.get("runner_kind") or "user"),
                    )

                if experts_db_enabled and resolved_resume_supported:
                    resolved_config = await inject_blob_credentials(
                        _resolved,
                        lambda co: dependencies.inject_dispatch_credentials(
                            job,
                            co,
                            include_kb_profile=has_knowledge_scope,
                        ),
                    )
            except GrantDenied as gd:
                dependencies.logger.warning(
                    "Resume denied for job %s: %s", job.get("id"), gd
                )
                if not dependencies.completion_commands_enabled():
                    await dependencies.store.update_job_status(
                        str(job["id"]),
                        status="failed",
                        error_message=dependencies.grant_violations_detail(
                            gd.violations
                        ),
                    )
                return False

        # The blob and flat fallback are mutually exclusive on the wire. Inject
        # credentials once into whichever representation will actually be sent:
        # inject_blob_credentials for the preferred path, the legacy flat
        # override only when resolved-config delivery is disabled.
        if resolved_config is None:
            config_override = await dependencies.inject_dispatch_credentials(
                job,
                config_override,
                include_kb_profile=has_knowledge_scope,
            )
            injected_env_keys = (config_override.get("env_keys") or {}).keys()
        else:
            injected_env_keys = (
                (resolved_config.get("agent") or {}).get("env_keys") or {}
            ).keys()
        dependencies.logger.info(
            "Dispatch (resume): job %s injected env_key names=%s",
            job_id,
            sorted(injected_env_keys),
        )

        # Extract queued feedback (stored by the resume endpoint when no agent
        # was available). The pop only mutates this local copy — the DB keys are
        # dropped AFTER the agent accepts (below), so a rejected resume no longer
        # permanently loses the feedback/delegation payload.
        job_context = job.get("context") or {}
        if isinstance(job_context, str):
            job_context = json.loads(job_context)
        original_context = dict(job_context)
        queued_feedback = job_context.pop("queued_feedback", None)
        queued_feedback_reason = job_context.pop("queued_feedback_reason", None)
        delegation_results = job_context.pop("delegation_results", None)
        consumed_context = {}
        if queued_feedback:
            consumed_context.update({
                key: original_context[key]
                for key in (
                    "queued_feedback", "queued_feedback_reason",
                    "queued_feedback_delivery_id",
                ) if key in original_context
            })
        if delegation_results:
            consumed_context.update({
                key: original_context[key]
                for key in ("delegation_results", "delegation_results_delivery_id")
                if key in original_context
            })

        # The per-job Gitea remote. Without it the agent's pod-handoff clone
        # (resume onto a fresh workspace with no snapshot) can never fire and
        # the job silently restarts from a blank tree
        # (knowledge-base/knowledge/issues/resume_fresh_workspace_no_clone_fallback.md). VM
        # workspaces cannot resolve the cluster-internal Gitea host, so
        # mirror the fresh path's VM-scoped rewrite (F29).
        runtime_actor = await dependencies.mint_worker_runtime_actor(
            dependencies.store,
            project_id=str(job["project_id"]) if job.get("project_id") else None,
            user_id=str(job["user_id"]) if job.get("user_id") else None,
        )
        resume_payload = {
            "job_id": job_id,
            "config_name": canonical_config_name(
                job.get("config_name") or "worker_base"
            ),
            "config_upload_id": job_context.get("config_upload_id"),
            "config_override": None if resolved_config else config_override,
            "resolved_config": resolved_config,
            "datasources": datasources_payload,
            "project_id": str(job["project_id"]) if job.get("project_id") else None,
            "previous_status": job.get("status"),
            "git_remote_url": git_remote_url,
            "repositories": repositories_payload,
            "managed_repository_credentials": managed_repository_credentials,
            "runtime_actor": runtime_actor.to_payload(),
            WORKSPACE_RUNTIME_CONTEXT_KEY: workspace_decision.safe_projection(),
            "workspace_provisioner": (
                "k8s"
                if workspace_authority is not None
                else (
                    str(
                        dependencies.get_container_context(job).get("provisioner") or ""
                    )
                    or None
                    if workspace_decision.effective_backend == "sandbox"
                    else (
                        str(dependencies.get_vm_context(job).get("provisioner") or "vm")
                        if workspace_decision.effective_backend == "vm"
                        else None
                    )
                )
            ),
        }
        if workspace_authority is not None:
            attested = workspace_authority.attestation
            resume_payload.update(
                {
                    "workspace_generation": attested.workspace_generation,
                    "workspace_runtime_incarnation": attested.runtime_incarnation,
                    "workspace_ssh_host_key_fingerprint": (
                        attested.ssh_host_key_fingerprint
                    ),
                    "workspace_owner_kind": workspace_authority.owner.kind,
                    "workspace_owner_id": workspace_authority.owner.id,
                }
            )
        if queued_feedback:
            resume_payload["feedback"] = queued_feedback
            if queued_feedback_reason:
                resume_payload["feedback_reason"] = queued_feedback_reason
        if delegation_results:
            resume_payload["delegation_results"] = delegation_results

        if not await dependencies.pinned_k8s_job_workspace_authority_is_current(
            durable_job, workspace_authority
        ):
            dependencies.logger.warning(
                "Resume dispatch: workspace authority changed while assembling job %s",
                job_id,
            )
            return False
        if not await dependencies.store.managed_repository_authorities_are_current(
            managed_repository_credentials
        ):
            dependencies.logger.warning(
                "Resume dispatch: repository authority changed while assembling job %s",
                job_id,
            )
            return False

        if not await dependencies.pinned_k8s_job_workspace_authority_is_current(
            durable_job, workspace_authority
        ):
            dependencies.logger.warning(
                "Resume dispatch: workspace authority changed at delivery for job %s",
                job_id,
            )
            return False

        target = await dependencies.prepare_pinned_job_mutation_target(
            agent_id=agent_id,
            job_id=job_id,
            require_idle=True,
        )
        if target is None:
            return False
        resume_payload["recipient"] = target.recipient.model_dump(mode="json")
        delivery_payload, delivery_intent = await _pinned_vm_delivery_intent(
            dependencies, job_id=job_id, agent_id=agent_id,
            recipient=target.recipient,
            payload={k: v for k, v in resume_payload.items() if v is not None},
            consumed_context=consumed_context,
        )
        if delivery_payload is None:
            return False
        agent_url = (
            f"http://{target.agent['pod_ip']}:{target.agent['pod_port']}/job/resume"
        )
        async with dependencies.http_client_factory(timeout=30.0) as client:
            response = await client.post(
                agent_url,
                json=delivery_payload,
            )

        if response.status_code not in (200, 202):
            dependencies.logger.warning(
                "Dispatch: agent %s rejected resume for job %s (status=%s)",
                agent_id,
                job_id,
                response.status_code,
            )
            if dependencies.resume_reject_should_requeue(response.status_code):
                # The agent's DB 'ready' was stale — its pod is non-idle (a
                # zombie, or still finishing prior work) and rejected with 409.
                # Demote it out of the ready pool so the next attempt doesn't
                # re-pick the same agent. See
                # knowledge-history/done/worker_pod_state_zombie_on_cancel.md.
                try:
                    async with dependencies.store.acquire() as conn:
                        await conn.execute(
                            "UPDATE agents SET status = 'working' "
                            "WHERE id = $1::uuid AND status = 'ready'",
                            agent_id,
                        )
                except Exception as demote_err:
                    dependencies.logger.warning(
                        f"Could not demote stale agent {agent_id}: {demote_err}"
                    )
            return False

        accepted_delivery_id = None
        if delivery_intent is not None:
            try:
                acknowledgement = response.json()
            except (ValueError, TypeError):
                acknowledgement = {}
            if (
                isinstance(acknowledgement, dict)
                and acknowledgement.get("pinned_delivery_id") == str(delivery_intent["id"])
                and acknowledgement.get("pinned_projection_digest")
                    == delivery_payload["pinned_projection_digest"]
            ):
                accepted_delivery_id = str(delivery_intent["id"])
            else:
                dependencies.logger.warning(
                    "Resume dispatch: pinned delivery acknowledgment unavailable for job %s",
                    job_id,
                )

        # Agent accepted — drop the keys we consumed, once ownership is
        # confirmed below, so a future resume won't re-inject them.
        # `context - text[]` (not a full-dict rewrite) preserves any concurrent
        # merge into other context keys.
        if dependencies.completion_commands_enabled():
            if not await dependencies.store.confirm_pinned_job_dispatch(
                job_id,
                agent_id,
                consumed_context=consumed_context,
                pinned_delivery_id=accepted_delivery_id,
                pinned_projection_digest=(
                    delivery_payload.get("pinned_projection_digest")
                    if accepted_delivery_id else None
                ),
            ):
                dependencies.logger.warning(
                    "Resume dispatch: stale success from agent %s for job %s; "
                    "control/ownership changed",
                    agent_id,
                    job_id,
                )
                return False
        elif not await dependencies.store.update_job_status(
            job_id=job_id,
            status="processing",
            assigned_agent_id=agent_id,
            # The pre-POST claim already wrote this. An operator pause (or a
            # cancel) that won while the resume was in flight keeps its row.
            expected_status="processing",
        ):
            dependencies.logger.warning(
                "Resume dispatch: job %s changed while agent %s accepted it; "
                "not reasserting processing",
                job_id,
                agent_id,
            )
            return False
        elif consumed_context:
            # Consumed only once ownership is confirmed, like the command
            # path: a lost CAS leaves the feedback for the next resume.
            await dependencies.store.delete_job_context_keys(
                job_id, list(consumed_context)
            )

        await dependencies.store.heartbeat(
            agent_id=agent_id,
            status="working",
            current_job_id=job_id,
        )

        dependencies.logger.info(
            f"Dispatch: resumed job {job_id} (priority={job.get('priority', '?')}) on agent {agent_id}"
        )
        return True

    except Exception as e:
        dependencies.logger.error(
            f"Dispatch: failed to resume job {job_id} on agent {agent_id}: {e}"
        )
        return False


async def initiate_pause(
    job: dict[str, Any], *, dependencies: JobDeliveryDependencies
) -> None:
    """Request graceful pause of a running job. Non-blocking (fire-and-forget).

    The agent will finish its current node, save checkpoint, and become available.
    The actual dispatch of the high-priority job happens on the next dispatcher cycle.
    """

    if not uses_srw_runtime(job):
        return
    job_id = str(job["id"])
    agent_id = str(job.get("assigned_agent_id", ""))

    if not job.get("pod_ip"):
        dependencies.logger.warning(
            f"Preempt: no pod IP for job {job_id} agent — cannot pause"
        )
        return

    pause_claim = None
    release_pause_claim = False
    try:
        if dependencies.completion_commands_enabled():
            pause_claim = await dependencies.completion_control.claim_pause(
                job_id,
                source="dispatcher_preempt",
                expected_agent_id=(
                    str(job["assigned_agent_id"])
                    if job.get("assigned_agent_id")
                    else None
                ),
            )
        target = await dependencies.prepare_pinned_job_mutation_target(
            agent_id=agent_id,
            job_id=job_id,
            require_idle=False,
        )
        if target is None:
            return
        agent_url = (
            f"http://{target.agent['pod_ip']}:{target.agent['pod_port']}/job/pause"
        )
        async with dependencies.http_client_factory(timeout=130.0) as client:
            response = await client.post(
                agent_url,
                json={"recipient": target.recipient.model_dump(mode="json")},
            )

        if response.status_code == 200:
            release_pause_claim = True
            dependencies.logger.info(
                f"Preempt: pause request sent for job {job_id} on agent {agent_id}"
            )
            if not dependencies.completion_commands_enabled():
                await dependencies.store.pause_job(job_id)
        elif response.status_code == 408:
            dependencies.logger.warning(
                "Preempt: pause timed out for job %s; retaining bounded control hold",
                job_id,
            )
            if not dependencies.completion_commands_enabled():
                await dependencies.store.pause_job(job_id)
        else:
            dependencies.logger.warning(
                f"Preempt: agent returned {response.status_code} for pause of job {job_id}"
            )

    except HTTPException as exc:
        dependencies.logger.info(
            "Preempt: pause claim lost for job %s: %s", job_id, exc.detail
        )
    except Exception as e:
        dependencies.logger.warning(f"Preempt: failed to pause job {job_id}: {e}")
    finally:
        if pause_claim is not None and release_pause_claim:
            await dependencies.completion_control.abort(pause_claim)
        elif pause_claim is not None:
            dependencies.logger.warning(
                "Preempt: retaining pause control hold for job %s until bounded expiry",
                job_id,
            )
        dependencies.pause_pending_job_ids.discard(job_id)
