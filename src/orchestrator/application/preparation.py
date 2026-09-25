"""Composition for configuration, credentials, payloads and preparation (R1.B05).

Session configuration resolution, dispatch credentials, the job start bundle,
session attach payloads, execution lanes, datasource selection and payloads,
job workspace runtime and authority, grant enforcement and VM permission,
thread mount rows, thread workspace delivery and stateless workspace
scheduling.
"""

from __future__ import annotations

import functools
import logging
import os

from orchestrator.application import (
    catalogue as catalogue_composition,
    completion as completion_composition,
    sessions as sessions_composition,
    workspace as workspace_composition,
)
from orchestrator.application.resources import ApplicationResources, bound
from orchestrator.security import access
from orchestrator.services import (
    agent_cloud_mounts,
    agent_datasource_payload,
    agent_toolset_probe,
    config_resolver,
    container_provisioner as container_provisioner_module,
    deployment_gates,
    dispatch_credentials,
    grant_enforcement,
    job_datasource_selection,
    job_dispatch_credentials,
    job_start_bundle,
    job_workspace_authority,
    job_workspace_runtime,
    managed_repository_authority,
    protected_cloud_engage,
    runtime_actor,
    session_attach_payload,
    session_class_policy,
    session_config_resolution,
    session_provisioner,
    session_runtime_identity,
    stateless_workspace_scheduler,
    subjob_completion as subjob_completion_operations,
    thread_datasource_authorization as thread_datasource_authorization_service,
    thread_mount_rows,
    thread_project_authorization as thread_project_authorization_service,
    thread_workspace_delivery,
    virtual_workspace,
    vm_provisioner as vm_provisioner_module,
    vm_workspace_policy,
    workspace_lifecycle,
    workspace_suspension,
    workspace_tier_policy,
)
from orchestrator.services.cloud import identity
from orchestrator.services.grant_enforcement import GrantDenied
from orchestrator.services.workspace_tier_policy import LiteWorkspaceConfigError
from shared.runtime.core import model_registry

logger = logging.getLogger(__name__)


def stateless_workspace_schedule_dependencies(
    resources: ApplicationResources,
) -> stateless_workspace_scheduler.StatelessWorkspaceScheduleDependencies:
    """Rebuilt per call, except the registry: that is the one field which must
    be the *same* object across builds, or single-flighting would not."""

    return stateless_workspace_scheduler.StatelessWorkspaceScheduleDependencies(
        store=resources.postgres_db,
        provisioner=container_provisioner_module.container_provisioner,
        suspension=workspace_suspension.workspace_suspension_service,
        registry=resources.stateless_workspace_ensure_registry,
        ensure_session_workspace=session_provisioner.ensure_session_workspace,
    )


def session_config_dependencies(
    resources: ApplicationResources,
) -> session_config_resolution.SessionConfigDependencies:
    """Rebuilt per call; every operation is bound to its owning service.

    Each binding names the owner (``bound(grant_enforcement.…)``) and is built
    when this factory runs, so a patch of the owner is seen by the next build;
    capturing them once would leave such a patch green and inert (§P3).
    """

    return session_config_resolution.SessionConfigDependencies(
        store=resources.postgres_db,
        is_experts_db_enabled=deployment_gates.is_experts_db_enabled,
        user_experts_enabled=bound(
            grant_enforcement.user_experts_enabled,
            grant_enforcement_dependencies,
            resources,
        ),
        resolve_runner_grants=bound(
            grant_enforcement.resolve_runner_grants,
            grant_enforcement_dependencies,
            resources,
        ),
        enforce_dispatch_grants=bound(
            grant_enforcement.enforce_dispatch_grants,
            grant_enforcement_dependencies,
            resources,
        ),
        gather_in_scope_skills=(
            lambda *args, **kwargs: catalogue_composition.expert_catalog_service(
                resources
            ).gather_in_scope_skills(*args, **kwargs)
        ),
        seed_registry_model_overrides=bound(
            dispatch_credentials.seed_registry_model_overrides,
            dispatch_credential_dependencies,
            resources,
        ),
        inject_thread_dispatch_credentials=bound(
            dispatch_credentials.inject_thread_dispatch_credentials,
            dispatch_credential_dependencies,
            resources,
        ),
        thread_project_ids=bound(
            thread_mount_rows.thread_project_ids, thread_mount_dependencies, resources
        ),
        thread_has_knowledge_scope=bound(
            thread_project_authorization_service.thread_has_knowledge_scope,
            sessions_composition.thread_project_authorization_dependencies,
            resources,
        ),
    )


def agent_toolset_dependencies(
    resources: ApplicationResources,
) -> agent_toolset_probe.AgentToolsetDependencies:
    return agent_toolset_probe.AgentToolsetDependencies(store=resources.postgres_db)


def dispatch_credential_dependencies(
    resources: ApplicationResources,
) -> dispatch_credentials.DispatchCredentialDependencies:
    """Rebuilt per call. ``resolve_model`` is read from its owning module
    (``model_registry``) when the factory runs, so a patch of the owner steers
    the next build (§P3)."""

    return dispatch_credentials.DispatchCredentialDependencies(
        store=resources.postgres_db,
        logger=logger,
        resolve_model=model_registry.resolve_model,
    )


def job_dispatch_credential_dependencies(
    resources: ApplicationResources,
) -> job_dispatch_credentials.DispatchCredentialDependencies:
    """Composition only: every injector is B05 lane C's, bound to its owner
    in ``dispatch_credentials`` with that module's own dependency object."""

    return job_dispatch_credentials.DispatchCredentialDependencies(
        store=resources.postgres_db,
        logger=logger,
        resolve_model=model_registry.resolve_model,
        inject_model_credentials=bound(
            dispatch_credentials.inject_model_credentials,
            dispatch_credential_dependencies,
            resources,
        ),
        inject_env_key_credentials=bound(
            dispatch_credentials.inject_env_key_credentials,
            dispatch_credential_dependencies,
            resources,
        ),
        inject_search_credentials=bound(
            dispatch_credentials.inject_search_credentials,
            dispatch_credential_dependencies,
            resources,
        ),
        inject_system_kb_embedding_profile=bound(
            dispatch_credentials.inject_system_kb_embedding_profile,
            dispatch_credential_dependencies,
            resources,
        ),
        dispatch_llm_provider_fallback=dispatch_credentials.dispatch_llm_provider_fallback,
        nested_model_slots=dispatch_credentials.nested_model_slots,
    )


def job_start_bundle_dependencies(
    resources: ApplicationResources,
) -> job_start_bundle.JobStartBundleDependencies:
    """Rebuilt per call. ``mint_worker_runtime_actor``,
    ``authorize_job_repository_transport`` and ``inject_blob_credentials`` are
    fields rather than imports in the service, read from their owners when this
    factory runs, so a patch of the owner steers the job-start owner (§P3)."""

    return job_start_bundle.JobStartBundleDependencies(
        store=resources.postgres_db,
        logger=logger,
        forge=resources.gitea_client,
        workspace_runtime=job_workspace_runtime_dependencies(resources),
        inject_dispatch_credentials=bound(
            job_dispatch_credentials.inject_dispatch_credentials,
            job_dispatch_credential_dependencies,
            resources,
        ),
        resolve_authorized_job_datasources=bound(
            job_datasource_selection.resolve_authorized_job_datasources,
            job_datasource_selection_dependencies,
            resources,
        ),
        job_project_repositories=bound(
            job_start_bundle.job_project_repositories,
            job_start_bundle_dependencies,
            resources,
        ),
        apply_cloud_storage_override=agent_datasource_payload.apply_cloud_storage_override,
        build_datasources_payload=bound(
            agent_datasource_payload.build_datasources_payload,
            datasource_payload_dependencies,
            resources,
        ),
        build_datasource_tool_override=bound(
            agent_datasource_payload.build_datasource_tool_override,
            datasource_payload_dependencies,
            resources,
        ),
        prepare_job_primary_repository_authority=(
            managed_repository_authority.prepare_job_primary_repository_authority
        ),
        prepare_project_repository_authority=managed_repository_authority.prepare_project_repository_authority,
        authorize_job_repository_transport=managed_repository_authority.authorize_job_repository_transport,
        mint_worker_runtime_actor=runtime_actor.mint_worker_runtime_actor,
        inject_blob_credentials=config_resolver.inject_blob_credentials,
        grant_denied_error=GrantDenied,
        lite_workspace_config_error=LiteWorkspaceConfigError,
        backend_from_override=workspace_tier_policy.backend_from_override,
        inject_lite_workspace_config=workspace_tier_policy.inject_lite_workspace_config,
        is_experts_db_enabled=deployment_gates.is_experts_db_enabled,
        user_experts_enabled=bound(
            grant_enforcement.user_experts_enabled,
            grant_enforcement_dependencies,
            resources,
        ),
        enforce_dispatch_grants=bound(
            grant_enforcement.enforce_dispatch_grants,
            grant_enforcement_dependencies,
            resources,
        ),
        grant_violations_detail=grant_enforcement.grant_violations_detail,
        resolve_default_models=bound(
            session_config_resolution.resolve_default_models,
            session_config_dependencies,
            resources,
        ),
        prefetch_roster_refs=bound(
            session_config_resolution.prefetch_roster_refs,
            session_config_dependencies,
            resources,
        ),
        seed_registry_model_overrides=bound(
            dispatch_credentials.seed_registry_model_overrides,
            dispatch_credential_dependencies,
            resources,
        ),
        gather_in_scope_skills=(
            lambda *args, **kwargs: catalogue_composition.expert_catalog_service(
                resources
            ).gather_in_scope_skills(*args, **kwargs)
        ),
        resolve_config=config_resolver.resolve_config,
        vm_workspaces_on_pod_network=access.vm_workspaces_on_pod_network,
    )


async def capture_session_delivery(
    resources: ApplicationResources, thread, resolved, status, *, project_ids
):
    from orchestrator.services.manifest_session_delivery import capture_session_delivery

    return await capture_session_delivery(
        resources.postgres_db, thread, resolved, status, project_ids=project_ids
    )


def session_attach_payload_dependencies(
    resources: ApplicationResources,
) -> session_attach_payload.SessionAttachPayloadDependencies:
    """Rebuilt per call; every collaborator is read from its owner then."""

    return session_attach_payload.SessionAttachPayloadDependencies(
        store=resources.postgres_db,
        GrantDenied=GrantDenied,
        LiteWorkspaceConfigError=LiteWorkspaceConfigError,
        await_protected_cloud_runtime_ready=bound(
            protected_cloud_engage._await_protected_cloud_runtime_ready,
            workspace_composition.protected_cloud_engage_dependencies,
            resources,
        ),
        build_datasource_tool_override=bound(
            agent_datasource_payload.build_datasource_tool_override,
            datasource_payload_dependencies,
            resources,
        ),
        build_datasources_payload=bound(
            agent_datasource_payload.build_datasources_payload,
            datasource_payload_dependencies,
            resources,
        ),
        build_protected_cloud_mount=agent_cloud_mounts._build_protected_cloud_mount,
        inject_lite_workspace_config=workspace_tier_policy.inject_lite_workspace_config,
        mint_thread_runtime_actor=runtime_actor.mint_thread_runtime_actor,
        protected_mount_selection_identity=protected_cloud_engage._protected_mount_selection_identity,
        require_pinned_status_identity=deployment_gates.require_pinned_status_identity,
        resolve_authorized_thread_datasources=bound(
            thread_datasource_authorization_service.resolve_authorized_thread_datasources,
            sessions_composition.thread_datasource_authorization_dependencies,
            resources,
        ),
        resolve_session_config=bound(
            session_config_resolution.resolve_session_config,
            session_config_dependencies,
            resources,
        ),
        revalidate_thread_project_ids=bound(
            thread_project_authorization_service.revalidate_thread_project_ids,
            sessions_composition.thread_project_authorization_dependencies,
            resources,
        ),
        ro_mount_matches_protected_selection=protected_cloud_engage._ro_mount_matches_protected_selection,
        thread_accepts_runtime=session_runtime_identity.thread_accepts_runtime,
        thread_project_ids=bound(
            thread_mount_rows.thread_project_ids, thread_mount_dependencies, resources
        ),
        capture_session_config=functools.partial(capture_session_delivery, resources),
    )


def execution_lane_dependencies(
    resources: ApplicationResources,
) -> session_class_policy.ExecutionLaneDependencies:
    """The pool gate is passed as a **callable**, not a value: it reads this
    application's deployment settings on every use (§P1)."""

    return session_class_policy.ExecutionLaneDependencies(
        stateless_session_enabled=lambda: resources.settings.stateless_session_enabled,
        container_provisioner=container_provisioner_module.container_provisioner,
        virtual_workspace_rclone_spec=virtual_workspace.virtual_workspace_rclone_spec,
    )


def job_datasource_selection_dependencies(
    resources: ApplicationResources,
) -> job_datasource_selection.JobDatasourceSelectionDependencies:
    """``revalidate_selection`` is the same module's operation bound through
    this factory: it is consumed by a *different* function in that module, so
    the binding cannot recurse, and the owner is read per build."""

    return job_datasource_selection.JobDatasourceSelectionDependencies(
        store=resources.postgres_db,
        authorize_thread_datasource_selection=bound(
            thread_datasource_authorization_service.authorize_thread_datasource_selection,
            sessions_composition.thread_datasource_authorization_dependencies,
            resources,
        ),
        backend_from_override=workspace_tier_policy.backend_from_override,
        revalidate_selection=bound(
            job_datasource_selection.revalidate_job_datasource_selection,
            job_datasource_selection_dependencies,
            resources,
        ),
    )


def job_workspace_runtime_dependencies(
    resources: ApplicationResources,
) -> job_workspace_runtime.JobWorkspaceRuntimeDependencies:
    """``vm_mode`` and the worker gate are **callables** — one is a provisioner
    attribute that changes at runtime, the other an import-time B11 flag."""

    return job_workspace_runtime.JobWorkspaceRuntimeDependencies(
        store=resources.postgres_db,
        vm_mode=lambda: vm_provisioner_module.vm_provisioner.mode,
        workspace_provisioner=container_provisioner_module.container_provisioner,
        vm_workspaces_on_pod_network=access.vm_workspaces_on_pod_network,
        stateless_worker_enabled=lambda: resources.settings.stateless_worker_enabled,
        backend_from_override=workspace_tier_policy.backend_from_override,
    )


def job_workspace_authority_dependencies(
    resources: ApplicationResources,
) -> job_workspace_authority.JobWorkspaceAuthorityDependencies:
    """``resolve_inherited_workspace``, ``fail_subjob_and_unblock_parent`` and
    ``workspace_runtime_unchanged_before_delivery`` are that module's own
    operations bound through this factory: each is consumed by a *different*
    function there, so a patch of the owner steers the sibling that calls it."""

    return job_workspace_authority.JobWorkspaceAuthorityDependencies(
        store=resources.postgres_db,
        logger=logger,
        workspace_provisioner=container_provisioner_module.container_provisioner,
        vm_provisioner=vm_provisioner_module.vm_provisioner,
        vm_mode=lambda: vm_provisioner_module.vm_provisioner.mode,
        # `ensure_workspace`, not `ensure_session_workspace`: the scholar
        # parent path provisions a *job* workspace and passes
        # `current_status=`, which the session helper does not accept.
        ensure_workspace=workspace_lifecycle.ensure_workspace,
        workspace_suspension=workspace_suspension.workspace_suspension_service,
        handle_scholar_completion=(
            lambda job, actions: subjob_completion_operations.handle_scholar_completion(
                job,
                actions,
                dependencies=completion_composition.scholar_completion_dependencies(
                    resources
                ),
            )
        ),
        handle_delegation_child_completion=(
            lambda job, actions: (
                subjob_completion_operations.handle_delegation_child_completion(
                    job,
                    actions,
                    dependencies=completion_composition.delegation_completion_dependencies(
                        resources
                    ),
                )
            )
        ),
        resolve_inherited_workspace=bound(
            job_workspace_authority.resolve_subjob_inherited_workspace,
            job_workspace_authority_dependencies,
            resources,
        ),
        fail_subjob_and_unblock_parent=bound(
            job_workspace_authority.fail_subjob_and_unblock_parent,
            job_workspace_authority_dependencies,
            resources,
        ),
        workspace_runtime_unchanged_before_delivery=(
            bound(
                job_workspace_authority.workspace_runtime_unchanged_before_delivery,
                job_workspace_authority_dependencies,
                resources,
            )
        ),
    )


def grant_enforcement_dependencies(
    resources: ApplicationResources,
) -> grant_enforcement.GrantEnforcementDependencies:
    """Rebuilt per call; every collaborator is read from its owner then."""

    return grant_enforcement.GrantEnforcementDependencies(
        store=resources.postgres_db,
        user_experts_enabled=bound(
            grant_enforcement.user_experts_enabled,
            grant_enforcement_dependencies,
            resources,
        ),
        resolve_runner_grants=bound(
            grant_enforcement.resolve_runner_grants,
            grant_enforcement_dependencies,
            resources,
        ),
        enforce_dispatch_grants=bound(
            grant_enforcement.enforce_dispatch_grants,
            grant_enforcement_dependencies,
            resources,
        ),
        check_vm_permission=bound(
            vm_workspace_policy.check_vm_permission,
            vm_permission_dependencies,
            resources,
        ),
    )


def vm_permission_dependencies(
    resources: ApplicationResources,
) -> vm_workspace_policy.VmPermissionDependencies:
    return vm_workspace_policy.VmPermissionDependencies(store=resources.postgres_db)


def datasource_payload_dependencies(
    resources: ApplicationResources,
) -> agent_datasource_payload.DatasourcePayloadDependencies:
    """The two connector gates are **callables**: they are deployment env
    reads made on every use."""

    return agent_datasource_payload.DatasourcePayloadDependencies(
        logger=logger,
        mcp_datasources_enabled=deployment_gates.mcp_datasources_enabled,
        mcp_stdio_enabled=deployment_gates.mcp_stdio_enabled,
    )


def thread_mount_dependencies(
    resources: ApplicationResources,
) -> thread_mount_rows.ThreadMountDependencies:
    """Rebuilt per call: ``postgres_db`` and ``main_cloud_router`` are rebound
    during ``lifespan``, and the two payload collaborators belong to B06 and to
    B05's job-preparation lane."""

    return thread_mount_rows.ThreadMountDependencies(
        store=resources.postgres_db,
        cloud_router=resources.main_cloud_router,
        resolve_user_identity_cached=identity.resolve_user_identity_cached,
        externalize_gitea_url=access.externalize_gitea_url,
        resolve_authorized_thread_datasources=bound(
            thread_datasource_authorization_service.resolve_authorized_thread_datasources,
            sessions_composition.thread_datasource_authorization_dependencies,
            resources,
        ),
        build_datasources_payload=bound(
            agent_datasource_payload.build_datasources_payload,
            datasource_payload_dependencies,
            resources,
        ),
        cloud_workspace_driver=cloud_workspace_driver,
    )


def cloud_workspace_driver() -> str:
    return os.getenv("CLOUD_WORKSPACE_DRIVER", "sync").strip().lower() or "sync"


def thread_workspace_delivery_dependencies(
    resources: ApplicationResources,
) -> thread_workspace_delivery.ThreadWorkspaceDeliveryDependencies:
    """Rebuilt per call; every callable is read from its owner when this
    factory runs, so a patch of the owner steers the service."""

    return thread_workspace_delivery.ThreadWorkspaceDeliveryDependencies(
        store=resources.postgres_db,
        cloud_router=resources.main_cloud_router,
        gitea_client=resources.gitea_client,
        container_provisioner=container_provisioner_module.container_provisioner,
        GrantDenied=GrantDenied,
        LiteWorkspaceConfigError=LiteWorkspaceConfigError,
        backend_from_override=workspace_tier_policy.backend_from_override,
        build_agent_cloud_mount=bound(
            agent_cloud_mounts._build_agent_cloud_mount,
            workspace_composition.agent_cloud_mount_dependencies,
            resources,
        ),
        build_agent_cloud_sync=bound(
            agent_cloud_mounts._build_agent_cloud_sync,
            workspace_composition.agent_cloud_mount_dependencies,
            resources,
        ),
        build_protected_cloud_mount=agent_cloud_mounts._build_protected_cloud_mount,
        cloud_workspace_driver=cloud_workspace_driver,
        grant_violations_detail=grant_enforcement.grant_violations_detail,
        inject_lite_workspace_config=workspace_tier_policy.inject_lite_workspace_config,
        inject_thread_dispatch_credentials=bound(
            dispatch_credentials.inject_thread_dispatch_credentials,
            dispatch_credential_dependencies,
            resources,
        ),
        protected_cloud_delivery_state=bound(
            protected_cloud_engage._protected_cloud_delivery_state,
            workspace_composition.protected_cloud_engage_dependencies,
            resources,
        ),
        protected_workspace_wait_payload=protected_cloud_engage._protected_workspace_wait_payload,
        require_pinned_status_identity=deployment_gates.require_pinned_status_identity,
        resolve_session_config=bound(
            session_config_resolution.resolve_session_config,
            session_config_dependencies,
            resources,
        ),
        resolve_thread_datasources=bound(
            thread_mount_rows.resolve_thread_datasources,
            thread_mount_dependencies,
            resources,
        ),
        resolve_thread_repositories=bound(
            thread_mount_rows.resolve_thread_repositories,
            thread_mount_dependencies,
            resources,
        ),
        revalidate_thread_project_ids=bound(
            thread_project_authorization_service.revalidate_thread_project_ids,
            sessions_composition.thread_project_authorization_dependencies,
            resources,
        ),
        ro_mount_matches_protected_selection=protected_cloud_engage._ro_mount_matches_protected_selection,
        schedule_stateless_workspace_ensure=bound(
            stateless_workspace_scheduler.schedule_stateless_workspace_ensure,
            stateless_workspace_schedule_dependencies,
            resources,
        ),
        thread_accepts_runtime=session_runtime_identity.thread_accepts_runtime,
        thread_project_ids=bound(
            thread_mount_rows.thread_project_ids, thread_mount_dependencies, resources
        ),
        thread_workspace_backend=workspace_tier_policy.thread_workspace_backend,
        virtual_workspace_rclone_spec=virtual_workspace.virtual_workspace_rclone_spec,
        vm_workspaces_on_pod_network=access.vm_workspaces_on_pod_network,
        require_internal=access.require_internal,
        capture_session_config=functools.partial(capture_session_delivery, resources),
    )
