"""Composition for job and thread controls and retirement (R1.B09).

Shared pinned retirement, job control/delivery/mutation operations and their
routes, thread retirement, Resume, End, rewind, pinned job mutation targets,
job assignment, and the reconciliation that consumes retirement (the stale
agent detector and the pinned Kubernetes reconcilers).
"""

from __future__ import annotations

import asyncio
import functools
import logging

import httpx

from orchestrator import logging_config, services
from orchestrator.application import (
    access as access_composition,
    catalogue as catalogue_composition,
    completion as completion_composition,
    jobs as jobs_composition,
    preparation as preparation_composition,
    sessions as sessions_composition,
    workflows as workflows_composition,
    workspace as workspace_composition,
)
from orchestrator.application.resources import ApplicationResources, bound
from orchestrator.database import postgres
from orchestrator.routers import (
    job_assignment,
    job_controls as job_control_routes,
    job_lifecycle as job_lifecycle_routes,
    thread_lifecycle as thread_lifecycle_routes,
    thread_rewind as thread_rewind_routes,
)
from orchestrator.security import access, auth
from orchestrator.services import (
    agent_cloud_mounts,
    agent_datasource_payload,
    agent_provisioner as agent_provisioner_module,
    commissioned_officer_provisioning as commissioned_officer_provisioning_service,
    config_resolver,
    container_provisioner as container_provisioner_module,
    deployment_gates,
    dispatch_credentials,
    docker_provisioner as docker_provisioner_module,
    grant_enforcement,
    ide_session,
    job_control_delivery,
    job_controls,
    job_datasource_selection,
    job_dispatch_credentials,
    job_dispatcher,
    job_freeze_notifications as job_freeze_notification_service,
    job_mutation_controls,
    job_start_bundle,
    job_workspace_authority,
    job_workspace_runtime,
    managed_repository_authority,
    officer_conference as officer_conference_service,
    officer_post_lifecycle as officer_post_lifecycle_service,
    persistent_provisioner as persistent_provisioner_module,
    pinned_k8s_reconciliation as pinned_k8s_reconciliation_service,
    protected_cloud_engage,
    runtime_actor,
    session_attach_binding as session_attach_binding_service,
    session_attach_recovery as session_attach_recovery_service,
    session_class_policy,
    session_config_resolution,
    session_runtime_identity,
    session_wake,
    snapshot_service as snapshot_service_module,
    stale_agent_detector as stale_agent_detector_service,
    stateless_workspace_scheduler,
    subjob_completion as subjob_completion_operations,
    subjob_output as subjob_output_operations,
    sudo_gate as sudo_gate_module,
    thread_mount_rows,
    thread_project_authorization as thread_project_authorization_service,
    thread_resume,
    thread_retirement,
    thread_rewind as thread_rewind_operations,
    thread_workspace_delivery,
    vm_provisioner as vm_provisioner_module,
    vm_workspace_policy,
    vm_workspace_recovery_store as vm_workspace_recovery_store_module,
    workspace,
    workspace_suspension,
    workspace_tier_policy,
)
from orchestrator.services.job_mutation_target import (
    PinnedJobMutationTarget as _PinnedJobMutationTarget,
)
from orchestrator.services.job_workspace_runtime import (
    WORKSPACE_CONTEXT_KEYS as _WORKSPACE_CONTEXT_KEYS,
)
from orchestrator.services.pinned_retirement import (
    PinnedRetirementDependencies,
    PinnedRetirementOperations,
)
from shared.runtime.core import loader as loader_module

logger = logging.getLogger(__name__)

LATE_CLOUD_SETUP_ATTACH_TIMEOUT_S = 15


def pinned_retirement_operations(
    resources: ApplicationResources,
) -> PinnedRetirementOperations:
    """Bind shared retirement authority to this application's collaborators."""

    return PinnedRetirementOperations(
        PinnedRetirementDependencies(
            store=resources.postgres_db,
            agent_provisioner=agent_provisioner_module.agent_provisioner,
            persistent_provisioner=persistent_provisioner_module.persistent_provisioner,
            container_provisioner=container_provisioner_module.container_provisioner,
            docker_provisioner=docker_provisioner_module.docker_provisioner,
            vm_provisioner=vm_provisioner_module.vm_provisioner,
            recovery_store=vm_workspace_recovery_store_module.VMWorkspaceRecoveryStore(
                resources.postgres_db
            ),
            session_router=resources.session_router,
            resolve_protected_reader_backend=functools.partial(
                protected_cloud_engage._resolve_protected_reader_backend,
                dependencies=workspace_composition.protected_cloud_engage_dependencies(
                    resources
                ),
            ),
            resolve_ssh_key_path=services.resolve_ssh_key_path,
            logger=logger,
        )
    )


def stale_agent_detector_dependencies(
    resources: ApplicationResources,
) -> stale_agent_detector_service.StaleAgentDetectorDependencies:
    """Bind agent reconciliation and the durable retirement retry (R1.B11).

    Retirement operations are providers: they are recomposed per call from
    current application state, exactly as the former in-module calls did.
    """

    return stale_agent_detector_service.StaleAgentDetectorDependencies(
        store=resources.postgres_db,
        agent_provisioner=agent_provisioner_module.agent_provisioner,
        docker_provisioner=docker_provisioner_module.docker_provisioner,
        audit_reader=resources.audit_reader,
        completion_commands_enabled=resources.settings.completion_commands_enabled,
        trigger_dispatch=bound(
            job_dispatcher.trigger_dispatch,
            jobs_composition.job_dispatch_dependencies,
            resources,
        ),
        schedule_attach_abort_successor=bound(
            session_attach_recovery_service.schedule_attach_abort_successor,
            sessions_composition.session_attach_recovery_dependencies,
            resources,
        ),
        thread_retirement_operations=functools.partial(
            thread_retirement_operations, resources
        ),
        pinned_retirement_operations=functools.partial(
            pinned_retirement_operations, resources
        ),
    )


def pinned_k8s_reconciliation_dependencies(
    resources: ApplicationResources,
) -> pinned_k8s_reconciliation_service.PinnedK8sReconciliationDependencies:
    return pinned_k8s_reconciliation_service.PinnedK8sReconciliationDependencies(
        store=resources.postgres_db,
        agent_provisioner=agent_provisioner_module.agent_provisioner,
        persistent_provisioner=persistent_provisioner_module.persistent_provisioner,
        container_provisioner=container_provisioner_module.container_provisioner,
    )


async def prepare_pinned_job_mutation_target(
    resources: ApplicationResources,
    *,
    agent_id: str,
    job_id: str,
    require_idle: bool,
) -> _PinnedJobMutationTarget | None:
    from orchestrator.services import job_mutation_target

    return await job_mutation_target.prepare_pinned_job_mutation_target(
        agent_id=agent_id,
        job_id=job_id,
        require_idle=require_idle,
        dependencies=job_mutation_target.PinnedJobMutationTargetDependencies(
            store=resources.postgres_db,
            agent_provisioner=agent_provisioner_module.agent_provisioner,
            logger=logger,
            http_client_factory=httpx.AsyncClient,
            sleep=asyncio.sleep,
        ),
    )


def job_control_operations(
    resources: ApplicationResources,
) -> job_controls.JobControlOperations:
    """Compose VM, sudo, Resume and approval controls around B08's boundary."""

    return job_controls.JobControlOperations(
        job_controls.JobControlDependencies(
            store=resources.postgres_db,
            logger=logger,
            completion_control=resources.completion_control_boundary,
            completion_commands_enabled=lambda: resources.settings.completion_commands_enabled,
            completion_control_active_sql=postgres._completion_control_active_sql,
            completion_control_owned_active_sql=postgres._completion_control_owned_active_sql,
            sudo_gate=sudo_gate_module.sudo_gate,
            workspace=workspace.workspace_service,
            snapshots=snapshot_service_module.snapshot_service,
            ide_sessions=ide_session.ide_session_service,
            vm_provisioner=vm_provisioner_module.vm_provisioner,
            forge=resources.gitea_client,
            vector_store=resources.vector_db,
            subjob_output=subjob_output_operations,
            subjob_output_dependencies=functools.partial(
                completion_composition.subjob_output_dependencies, resources
            ),
            authorize_runtime_actor_request=runtime_actor.authorize_runtime_actor_request,
            redispatch_livelock_trip=job_start_bundle.redispatch_livelock_trip,
            user_experts_enabled=bound(
                grant_enforcement.user_experts_enabled,
                preparation_composition.grant_enforcement_dependencies,
                resources,
            ),
            resolve_default_models=bound(
                session_config_resolution.resolve_default_models,
                preparation_composition.session_config_dependencies,
                resources,
            ),
            prefetch_roster_refs=bound(
                session_config_resolution.prefetch_roster_refs,
                preparation_composition.session_config_dependencies,
                resources,
            ),
            resolve_config=config_resolver.resolve_config,
            canonical_config_name=loader_module.canonical_config_name,
            enforce_dispatch_grants=bound(
                grant_enforcement.enforce_dispatch_grants,
                preparation_composition.grant_enforcement_dependencies,
                resources,
            ),
            grant_violations_detail=grant_enforcement.grant_violations_detail,
            prepare_job_workspace_runtime=bound(
                job_workspace_authority.prepare_job_workspace_runtime,
                preparation_composition.job_workspace_authority_dependencies,
                resources,
            ),
            resume_missing_workspace=lambda *args, **kwargs: (
                job_workspace_runtime.resume_missing_workspace(
                    *args,
                    **kwargs,
                    dependencies=preparation_composition.job_workspace_runtime_dependencies(
                        resources
                    ),
                )
            ),
            workspace_context_keys=_WORKSPACE_CONTEXT_KEYS,
            prepare_job_repository_before_claim=(
                bound(
                    job_start_bundle.prepare_job_repository_before_claim,
                    preparation_composition.job_start_bundle_dependencies,
                    resources,
                )
            ),
            resume_job_on_agent=lambda job, agent: job_delivery_operations(
                resources
            ).resume(job, agent),
            trigger_dispatch=bound(
                job_dispatcher.trigger_dispatch,
                jobs_composition.job_dispatch_dependencies,
                resources,
            ),
            resolve_job_notifications=lambda *args, **kwargs: (
                job_freeze_notification_service.resolve_job_notifications(
                    *args,
                    **kwargs,
                    dependencies=workflows_composition.job_freeze_notification_dependencies(
                        resources
                    ),
                )
            ),
            maybe_wake_session=session_wake.maybe_wake_session,
            kick_session_wake_drain=session_wake.kick_drain,
            get_container_context=job_workspace_runtime.get_container_context,
            get_vm_context=job_workspace_runtime.get_vm_context,
            recovery_store=vm_workspace_recovery_store_module.VMWorkspaceRecoveryStore(
                resources.postgres_db
            ),
        )
    )


def job_delivery_operations(
    resources: ApplicationResources,
) -> job_control_delivery.JobDeliveryOperations:
    """Compose pinned start/resume delivery without owning scheduler tasks."""

    return job_control_delivery.JobDeliveryOperations(
        job_control_delivery.JobDeliveryDependencies(
            store=resources.postgres_db,
            logger=logger,
            completion_commands_enabled=lambda: resources.settings.completion_commands_enabled,
            http_client_factory=httpx.AsyncClient,
            completion_control=resources.completion_control_boundary,
            pause_pending_job_ids=resources.job_dispatch_state.pause_pending_job_ids,
            gitea_client=resources.gitea_client,
            workspace_context_keys=_WORKSPACE_CONTEXT_KEYS,
            prepare_job_workspace_runtime=bound(
                job_workspace_authority.prepare_job_workspace_runtime,
                preparation_composition.job_workspace_authority_dependencies,
                resources,
            ),
            attest_pinned_k8s_job_workspace=lambda *args, **kwargs: (
                job_workspace_authority.attest_pinned_k8s_job_workspace(
                    *args,
                    **kwargs,
                    dependencies=preparation_composition.job_workspace_authority_dependencies(
                        resources
                    ),
                )
            ),
            build_job_start_request=lambda *args, **kwargs: (
                job_start_bundle.build_job_start_request(
                    *args,
                    **kwargs,
                    dependencies=preparation_composition.job_start_bundle_dependencies(
                        resources
                    ),
                )
            ),
            pinned_k8s_job_workspace_authority_is_current=lambda *args, **kwargs: (
                job_workspace_authority.pinned_k8s_job_workspace_authority_is_current(
                    *args,
                    **kwargs,
                    dependencies=preparation_composition.job_workspace_authority_dependencies(
                        resources
                    ),
                )
            ),
            prepare_pinned_job_mutation_target=functools.partial(
                prepare_pinned_job_mutation_target, resources
            ),
            redispatch_livelock_trip=job_start_bundle.redispatch_livelock_trip,
            bind_log_context=logging_config.bind_log_context,
            reset_log_context=logging_config.reset_log_context,
            resume_missing_workspace=lambda *args, **kwargs: (
                job_workspace_runtime.resume_missing_workspace(
                    *args,
                    **kwargs,
                    dependencies=preparation_composition.job_workspace_runtime_dependencies(
                        resources
                    ),
                )
            ),
            resolve_authorized_job_datasources=bound(
                job_datasource_selection.resolve_authorized_job_datasources,
                preparation_composition.job_datasource_selection_dependencies,
                resources,
            ),
            apply_cloud_storage_override=agent_datasource_payload.apply_cloud_storage_override,
            build_datasources_payload=bound(
                agent_datasource_payload.build_datasources_payload,
                preparation_composition.datasource_payload_dependencies,
                resources,
            ),
            job_project_repositories=bound(
                job_start_bundle.job_project_repositories,
                preparation_composition.job_start_bundle_dependencies,
                resources,
            ),
            build_datasource_tool_override=bound(
                agent_datasource_payload.build_datasource_tool_override,
                preparation_composition.datasource_payload_dependencies,
                resources,
            ),
            inject_matching_workspace_config=lambda *args, **kwargs: (
                job_workspace_runtime.inject_matching_workspace_config(
                    *args,
                    **kwargs,
                    dependencies=preparation_composition.job_workspace_runtime_dependencies(
                        resources
                    ),
                )
            ),
            get_container_context=job_workspace_runtime.get_container_context,
            get_vm_context=job_workspace_runtime.get_vm_context,
            authorize_job_repository_transport=managed_repository_authority.authorize_job_repository_transport,
            apply_sticky_sudo_denial=job_workspace_runtime.apply_sticky_sudo_denial,
            backend_from_override=workspace_tier_policy.backend_from_override,
            repository_datasource_names=job_datasource_selection.repository_datasource_names,
            inject_lite_workspace_config=workspace_tier_policy.inject_lite_workspace_config,
            is_experts_db_enabled=deployment_gates.is_experts_db_enabled,
            user_experts_enabled=bound(
                grant_enforcement.user_experts_enabled,
                preparation_composition.grant_enforcement_dependencies,
                resources,
            ),
            enforce_dispatch_grants=bound(
                grant_enforcement.enforce_dispatch_grants,
                preparation_composition.grant_enforcement_dependencies,
                resources,
            ),
            gather_in_scope_skills=(
                lambda *args, **kwargs: catalogue_composition.expert_catalog_service(
                    resources
                ).gather_in_scope_skills(*args, **kwargs)
            ),
            seed_registry_model_overrides=bound(
                dispatch_credentials.seed_registry_model_overrides,
                preparation_composition.dispatch_credential_dependencies,
                resources,
            ),
            resolve_default_models=bound(
                session_config_resolution.resolve_default_models,
                preparation_composition.session_config_dependencies,
                resources,
            ),
            prefetch_roster_refs=bound(
                session_config_resolution.prefetch_roster_refs,
                preparation_composition.session_config_dependencies,
                resources,
            ),
            inject_dispatch_credentials=bound(
                job_dispatch_credentials.inject_dispatch_credentials,
                preparation_composition.job_dispatch_credential_dependencies,
                resources,
            ),
            grant_violations_detail=grant_enforcement.grant_violations_detail,
            mint_worker_runtime_actor=runtime_actor.mint_worker_runtime_actor,
            resume_reject_should_requeue=(job_controls.resume_reject_should_requeue),
        )
    )


def job_mutation_operations(
    resources: ApplicationResources,
) -> job_mutation_controls.JobControlOperations:
    """Compose destructive job controls around shared mutation authority."""

    return job_mutation_controls.JobControlOperations(
        job_mutation_controls.JobControlDependencies(
            store=resources.postgres_db,
            logger=logger,
            completion_commands_enabled=lambda: resources.settings.completion_commands_enabled,
            completion_control=resources.completion_control_boundary,
            manifest_cancel=lambda job_id: catalogue_composition.manifest_execution_service(
                resources
            ).cancel(job_id),
            prepare_pinned_mutation_target=functools.partial(
                prepare_pinned_job_mutation_target, resources
            ),
            archive_and_cleanup_workspace=(
                thread_retirement_operations(resources).archive_and_cleanup_workspace
            ),
            http_client_factory=httpx.AsyncClient,
            handle_scholar_completion=lambda job: (
                subjob_completion_operations.handle_scholar_completion(
                    job,
                    [],
                    dependencies=completion_composition.scholar_completion_dependencies(
                        resources
                    ),
                )
            ),
            maybe_wake_session=lambda job_id, status: session_wake.maybe_wake_session(
                resources.postgres_db, job_id, status
            ),
            kick_session_wake_drain=lambda: session_wake.kick_drain(
                resources.postgres_db
            ),
            trigger_dispatch=bound(
                job_dispatcher.trigger_dispatch,
                jobs_composition.job_dispatch_dependencies,
                resources,
            ),
            resolve_job_notifications=lambda *args, **kwargs: (
                job_freeze_notification_service.resolve_job_notifications(
                    *args,
                    **kwargs,
                    dependencies=workflows_composition.job_freeze_notification_dependencies(
                        resources
                    ),
                )
            ),
            snapshot_service=snapshot_service_module.snapshot_service,
            gitea_client=resources.gitea_client,
            revoke_and_delete_managed_repository=(
                managed_repository_authority.revoke_and_delete_managed_repository
            ),
            vector_db=resources.vector_db,
        )
    )


def job_control_route_dependencies(
    resources: ApplicationResources,
) -> job_control_routes.JobControlRouteDependencies:
    return job_control_routes.JobControlRouteDependencies(
        operations=job_control_operations(resources),
        store=resources.postgres_db,
        require_admin=functools.partial(access_composition.require_admin, resources),
        require_job_access=access.require_job_access,
        require_internal_or_job_access=access.require_internal_or_job_access,
        require_approved_user=auth.require_approved_user,
        require_sudo_request_authority=access.require_sudo_request_authority,
        user_can_access_job_or_thread=access.user_can_access_job_or_thread,
        mcp_scope_project_id=access.mcp_scope_project_id,
    )


def job_mutation_route_dependencies(
    resources: ApplicationResources,
) -> job_lifecycle_routes.JobControlRouteDependencies:
    return job_lifecycle_routes.JobControlRouteDependencies(
        operations=job_mutation_operations(resources),
        store=resources.postgres_db,
        require_job_access=access.require_job_access,
        require_internal_or_job_access=access.require_internal_or_job_access,
        require_internal=access.require_internal,
    )


def thread_retirement_operations(
    resources: ApplicationResources,
) -> thread_retirement.ThreadRetirementOperations:
    """Compose shared pinned/stateless retirement with captured authority."""

    return thread_retirement.ThreadRetirementOperations(
        thread_retirement.ThreadRetirementDependencies(
            store=resources.postgres_db,
            agent_provisioner=agent_provisioner_module.agent_provisioner,
            persistent_provisioner=persistent_provisioner_module.persistent_provisioner,
            container_provisioner=container_provisioner_module.container_provisioner,
            vm_provisioner=vm_provisioner_module.vm_provisioner,
            recovery_store=vm_workspace_recovery_store_module.VMWorkspaceRecoveryStore(
                resources.postgres_db
            ),
            docker_provisioner=docker_provisioner_module.docker_provisioner,
            workspace_suspension_service=workspace_suspension.workspace_suspension_service,
            snapshot_service=snapshot_service_module.snapshot_service,
            gitea_client=resources.gitea_client,
            main_cloud_router=resources.main_cloud_router,
            pinned_retirement=pinned_retirement_operations(resources),
            build_agent_cloud_mount=lambda *args, **kwargs: (
                agent_cloud_mounts._build_agent_cloud_mount(
                    *args,
                    **kwargs,
                    dependencies=workspace_composition.agent_cloud_mount_dependencies(
                        resources
                    ),
                )
            ),
            get_container_context=job_workspace_runtime.get_container_context,
            get_vm_context=job_workspace_runtime.get_vm_context,
            vm_needs_release=vm_workspace_policy.vm_needs_release,
            thread_uses_pinned_execution=session_runtime_identity.thread_uses_pinned_execution,
            threads_suspending=resources.threads_suspending,
            require_stateless_end_workspace=session_class_policy.require_stateless_end_workspace,
            decommission_officer_post=lambda *args, **kwargs: (
                officer_post_lifecycle_service.decommission_officer_post(
                    *args,
                    **kwargs,
                    dependencies=workflows_composition.officer_post_lifecycle_dependencies(
                        resources
                    ),
                )
            ),
            conclude_conference_if_any=bound(
                officer_conference_service.conclude_conference_if_any,
                workflows_composition.officer_conference_dependencies,
                resources,
            ),
            logger=logger,
        )
    )


def thread_resume_operations(
    resources: ApplicationResources,
    retirement: thread_retirement.ThreadRetirementOperations | None = None,
) -> thread_resume.ThreadResumeOperations:
    """Compose Resume while retaining application-owned task registries."""

    retirement = retirement or thread_retirement_operations(resources)
    return thread_resume.ThreadResumeOperations(
        thread_resume.ThreadResumeDependencies(
            store=resources.postgres_db,
            agent_provisioner=agent_provisioner_module.agent_provisioner,
            persistent_provisioner=persistent_provisioner_module.persistent_provisioner,
            container_provisioner=container_provisioner_module.container_provisioner,
            workspace_suspension_service=workspace_suspension.workspace_suspension_service,
            main_cloud_router=resources.main_cloud_router,
            officer_conference_service=officer_conference_service,
            retirement=retirement,
            late_cloud_setup_tasks=resources.late_cloud_setup_tasks,
            late_cloud_setup_attach_timeout_s=(
                lambda: LATE_CLOUD_SETUP_ATTACH_TIMEOUT_S
            ),
            classify_thread_project_ids=lambda *args, **kwargs: (
                thread_project_authorization_service.classify_thread_project_ids(
                    *args,
                    **kwargs,
                    dependencies=sessions_composition.thread_project_authorization_dependencies(
                        resources
                    ),
                )
            ),
            resolve_session_config=bound(
                session_config_resolution.resolve_session_config,
                preparation_composition.session_config_dependencies,
                resources,
            ),
            thread_project_ids=bound(
                thread_mount_rows.thread_project_ids,
                preparation_composition.thread_mount_dependencies,
                resources,
            ),
            require_stateless_workspace=session_class_policy.require_stateless_workspace,
            require_supported_protected_session_class=(
                lambda *args, **kwargs: (
                    session_config_resolution.require_supported_protected_session_class(
                        *args,
                        **kwargs,
                        dependencies=preparation_composition.session_config_dependencies(
                            resources
                        ),
                    )
                )
            ),
            thread_workspace_backend=workspace_tier_policy.thread_workspace_backend,
            hold_officer_for_conference=bound(
                officer_conference_service.hold_officer_for_conference,
                workflows_composition.officer_conference_dependencies,
                resources,
            ),
            is_protected_cloud_mode_enabled=deployment_gates.is_protected_cloud_mode_enabled,
            schedule_protected_engage=bound(
                protected_cloud_engage._schedule_protected_engage,
                workspace_composition.protected_cloud_engage_dependencies,
                resources,
            ),
            should_skip_session_folder=bound(
                thread_mount_rows.should_skip_session_folder,
                preparation_composition.thread_mount_dependencies,
                resources,
            ),
            await_protected_cloud_runtime_ready=bound(
                protected_cloud_engage._await_protected_cloud_runtime_ready,
                workspace_composition.protected_cloud_engage_dependencies,
                resources,
            ),
            find_idle_persistent_agent=bound(
                session_attach_binding_service.find_idle_persistent_agent,
                sessions_composition.session_attach_binding_dependencies,
                resources,
            ),
            thread_has_knowledge_scope=bound(
                thread_project_authorization_service.thread_has_knowledge_scope,
                sessions_composition.thread_project_authorization_dependencies,
                resources,
            ),
            inject_thread_dispatch_credentials=bound(
                dispatch_credentials.inject_thread_dispatch_credentials,
                preparation_composition.dispatch_credential_dependencies,
                resources,
            ),
            send_session_attach=bound(
                session_attach_binding_service.send_session_attach,
                sessions_composition.session_attach_binding_dependencies,
                resources,
            ),
            emit_session_provisioning_failure=bound(
                commissioned_officer_provisioning_service.emit_session_provisioning_failure,
                sessions_composition.commissioned_officer_dependencies,
                resources,
            ),
            thread_uses_pinned_execution=session_runtime_identity.thread_uses_pinned_execution,
            schedule_stateless_workspace_ensure=bound(
                stateless_workspace_scheduler.schedule_stateless_workspace_ensure,
                preparation_composition.stateless_workspace_schedule_dependencies,
                resources,
            ),
            create_task=asyncio.create_task,
            agent_get_thread_workspace_locked=lambda *args, **kwargs: (
                thread_workspace_delivery.agent_get_thread_workspace_locked(
                    *args,
                    **kwargs,
                    dependencies=preparation_composition.thread_workspace_delivery_dependencies(
                        resources
                    ),
                )
            ),
            inject_lite_workspace_config=workspace_tier_policy.inject_lite_workspace_config,
            logger=logger,
        )
    )


def thread_lifecycle_dependencies(
    resources: ApplicationResources,
) -> thread_lifecycle_routes.ThreadLifecycleRouteDependencies:
    retirement = thread_retirement_operations(resources)
    return thread_lifecycle_routes.ThreadLifecycleRouteDependencies(
        store=resources.postgres_db,
        retirement=retirement,
        resume=thread_resume_operations(resources, retirement),
        require_thread_owner=access.require_thread_owner,
    )


def thread_rewind_dependencies(
    resources: ApplicationResources,
) -> thread_rewind_routes.ThreadRewindDependencies:
    return thread_rewind_routes.ThreadRewindDependencies(
        store=resources.postgres_db,
        service=thread_rewind_operations.ThreadRewindService(
            resources.postgres_db,
            deployment_gates.stateless_idle_conversation_rewind_enabled,
        ),
        require_thread_owner=access.require_thread_owner,
    )


def job_assignment_dependencies(
    resources: ApplicationResources,
) -> job_assignment.JobAssignmentDependencies:
    """Rebuilt per call; the completion-control operations are B08's four
    injected callables, the same boundary B04 established rather than a
    second control authority."""

    return job_assignment.JobAssignmentDependencies(
        store=resources.postgres_db,
        logger=logger,
        require_admin=functools.partial(access_composition.require_admin, resources),
        vm_mode=lambda: vm_provisioner_module.vm_provisioner.mode,
        completion_commands_enabled=lambda: resources.settings.completion_commands_enabled,
        prepare_job_workspace_runtime=bound(
            job_workspace_authority.prepare_job_workspace_runtime,
            preparation_composition.job_workspace_authority_dependencies,
            resources,
        ),
        prepare_job_repository_before_claim=bound(
            job_start_bundle.prepare_job_repository_before_claim,
            preparation_composition.job_start_bundle_dependencies,
            resources,
        ),
        resume_missing_workspace=lambda *args, **kwargs: (
            job_workspace_runtime.resume_missing_workspace(
                *args,
                **kwargs,
                dependencies=preparation_composition.job_workspace_runtime_dependencies(
                    resources
                ),
            )
        ),
        guard_completion_control=resources.completion_control_boundary.guard,
        claim_completion_control=resources.completion_control_boundary.claim,
        abort_completion_control_claim=resources.completion_control_boundary.abort,
        completion_resume_guard_kwargs=resources.completion_control_boundary.resume_guard_kwargs,
        dispatch_job_to_agent=lambda job, agent: job_delivery_operations(
            resources
        ).dispatch(job, agent),
        resume_job_on_agent=lambda job, agent: job_delivery_operations(
            resources
        ).resume(job, agent),
        trigger_dispatch=bound(
            job_dispatcher.trigger_dispatch,
            jobs_composition.job_dispatch_dependencies,
            resources,
        ),
    )
