"""Composition for job completion (R1.B08).

Subjob output, scholar and delegation completion, verification, completion
effects, the intact legacy completion workflow and recovery, and the
application's one ``CompletionRuntime`` / ``CompletionControlBoundary``
(``install_completion_runtime``).
"""

from __future__ import annotations

import asyncio
import functools
import logging
import os
from typing import Any

from orchestrator.application import (
    controls as controls_composition,
    jobs as jobs_composition,
    preparation as preparation_composition,
    sessions as sessions_composition,
    workflows as workflows_composition,
)
from orchestrator.application.resources import ApplicationResources, bound
from orchestrator.security import access
from orchestrator.services import (
    completion_effects as completion_effect_operations,
    completion_recovery as completion_recovery_operations,
    config_overrides,
    container_provisioner as container_provisioner_module,
    curation_final_pass as curation_final_pass_service,
    job_completion as job_completion_operations,
    job_datasource_selection,
    job_dispatcher,
    job_freeze_notifications as job_freeze_notification_service,
    job_workspace_runtime,
    legacy_job_completion as legacy_job_completion_operations,
    managed_repository_authority,
    manifest_runtime_ownership,
    notification_service as notification_service_module,
    project_loop_advance as project_loop_advance_service,
    session_config_resolution,
    session_wake,
    subjob_completion as subjob_completion_operations,
    subjob_output as subjob_output_operations,
    sudo_gate as sudo_gate_module,
    thread_project_authorization as thread_project_authorization_service,
    verification_workflow as verification_operations,
    vm_provisioner as vm_provisioner_module,
    vm_workspace_policy,
    vm_workspace_recovery_store as vm_workspace_recovery_store_module,
    workspace_tier_policy,
)
from orchestrator.services.completion_runtime import (
    CompletionAlertDependencies,
    CompletionAlerts,
    CompletionControlBoundary,
    CompletionRuntime,
    CompletionRuntimeDependencies,
)
from orchestrator.services.completion_session_memory import (
    SessionMemoryDependencies,
    SessionMemoryRuntime,
)
from shared import workspace_contract

logger = logging.getLogger(__name__)

_COMPLETION_S36_EXACT_ABSENCE_TIMEOUT_SECONDS = 45.0


def subjob_output_dependencies(
    resources: ApplicationResources,
) -> subjob_output_operations.SubjobOutputDependencies:
    return subjob_output_operations.SubjobOutputDependencies(
        store=resources.postgres_db,
        forge=resources.gitea_client,
    )


def scholar_completion_dependencies(
    resources: ApplicationResources,
) -> subjob_completion_operations.ScholarCompletionDependencies:
    return subjob_completion_operations.ScholarCompletionDependencies(
        store=resources.postgres_db,
        forge=resources.gitea_client,
        trigger_dispatch=bound(
            job_dispatcher.trigger_dispatch,
            jobs_composition.job_dispatch_dependencies,
            resources,
        ),
        resolve_workspace_backend=(
            lambda job: workspace_contract.resolve_workspace_contract(
                job
            ).assigned_backend
        ),
        is_lite_config_override=workspace_tier_policy.is_lite_config_override,
        should_provision_parent_container=bound(
            job_workspace_runtime.scholar_should_provision_parent_container,
            preparation_composition.job_workspace_runtime_dependencies,
            resources,
        ),
        revalidate_datasource_selection=bound(
            job_datasource_selection.revalidate_job_datasource_selection,
            preparation_composition.job_datasource_selection_dependencies,
            resources,
        ),
        datasource_selection_provenance=job_datasource_selection.datasource_selection_provenance,
        prepare_primary_repository_authority=functools.partial(
            managed_repository_authority.prepare_job_primary_repository_authority,
            resources.postgres_db,
            resources.gitea_client,
        ),
        completion_resume_guard_kwargs=(
            lambda: resources.completion_control_boundary.resume_guard_kwargs()
        ),
        maybe_wake_session=(
            lambda job_id, status: session_wake.maybe_wake_session(
                resources.postgres_db, job_id, status
            )
        ),
        kick_session_wake_drain=lambda: session_wake.kick_drain(resources.postgres_db),
        notify_review_returned=notification_service_module.notification_service.record_review_returned,
    )


def delegation_completion_dependencies(
    resources: ApplicationResources,
) -> subjob_completion_operations.DelegationCompletionDependencies:
    return subjob_completion_operations.DelegationCompletionDependencies(
        store=resources.postgres_db,
        trigger_dispatch=bound(
            job_dispatcher.trigger_dispatch,
            jobs_composition.job_dispatch_dependencies,
            resources,
        ),
        completion_resume_guard_kwargs=(
            lambda: resources.completion_control_boundary.resume_guard_kwargs()
        ),
    )


def verification_dependencies(
    resources: ApplicationResources,
) -> verification_operations.VerificationDependencies:
    from orchestrator.database.postgres import _stateless_resume_context
    from shared import worker_queue
    from shared.run_queue import unpark_unit

    return verification_operations.VerificationDependencies(
        store=resources.postgres_db,
        transaction=verification_operations.VerificationTransactionPorts(
            revalidate_datasource_selection=bound(
                job_datasource_selection.revalidate_job_datasource_selection,
                preparation_composition.job_datasource_selection_dependencies,
                resources,
            ),
            datasource_selection_provenance=job_datasource_selection.datasource_selection_provenance,
            resolve_workspace_contract=workspace_contract.resolve_workspace_contract,
            deep_merge_dicts=config_overrides.deep_merge_dicts,
            is_lite_config_override=workspace_tier_policy.is_lite_config_override,
            enqueue_worker_batch_wake=worker_queue.enqueue_worker_batch_wake,
            reset_worker_batch_attempts=worker_queue.reset_worker_batch_attempts,
            unpark_unit=unpark_unit,
            stateless_resume_context=_stateless_resume_context,
        ),
        effects=verification_operations.VerificationEffectPorts(
            forge=resources.gitea_client,
            notifier=notification_service_module.notification_service,
            prepare_job_repository_authority=functools.partial(
                managed_repository_authority.prepare_job_primary_repository_authority,
                resources.postgres_db,
                resources.gitea_client,
            ),
            trigger_dispatch=bound(
                job_dispatcher.trigger_dispatch,
                jobs_composition.job_dispatch_dependencies,
                resources,
            ),
            maybe_wake_session=session_wake.maybe_wake_session,
            kick_session_wake_drain=session_wake.kick_drain,
            trigger_curation_final_pass=(
                lambda *args, **kwargs: (
                    curation_final_pass_service.trigger_curation_final_pass(
                        *args,
                        **kwargs,
                        dependencies=workflows_composition.curation_final_pass_dependencies(
                            resources
                        ),
                    )
                )
            ),
            set_target_to_autonomy_status=(
                lambda job_id: subjob_completion_operations.set_target_to_autonomy_status(
                    job_id,
                    dependencies=scholar_completion_dependencies(resources),
                )
            ),
            escalate_target=(
                lambda job_id,
                job,
                reason: subjob_completion_operations.escalate_target(
                    job_id,
                    job,
                    reason,
                    dependencies=scholar_completion_dependencies(resources),
                )
            ),
            internal_resume_job=lambda *args, **kwargs: (
                controls_composition.job_control_operations(
                    resources
                ).internal_resume_job(*args, **kwargs)
            ),
        ),
    )


def completion_effect_dependencies(
    resources: ApplicationResources,
) -> completion_effect_operations.CompletionEffectDependencies:
    return completion_effect_operations.CompletionEffectDependencies(
        store=resources.postgres_db,
        container_provisioner=container_provisioner_module.container_provisioner,
        vm_provisioner=vm_provisioner_module.vm_provisioner,
        get_container_context=job_workspace_runtime.get_container_context,
        get_vm_context=job_workspace_runtime.get_vm_context,
        archive_and_cleanup_workspace=(
            controls_composition.thread_retirement_operations(
                resources
            ).archive_and_cleanup_workspace
        ),
        s36_exact_absence_timeout_seconds=(
            lambda: _COMPLETION_S36_EXACT_ABSENCE_TIMEOUT_SECONDS
        ),
        logger=logger,
        recovery_store=vm_workspace_recovery_store_module.VMWorkspaceRecoveryStore(
            resources.postgres_db
        ),
    )


def legacy_completion_dependencies(
    resources: ApplicationResources,
) -> legacy_job_completion_operations.LegacyCompletionDependencies:
    verification = verification_dependencies(resources)
    scholar = scholar_completion_dependencies(resources)
    delegation = delegation_completion_dependencies(resources)
    output = subjob_output_dependencies(resources)
    return legacy_job_completion_operations.LegacyCompletionDependencies(
        persistence=legacy_job_completion_operations.LegacyPersistenceDependencies(
            store=resources.postgres_db,
            vector_store=resources.vector_db,
            forge=resources.gitea_client,
        ),
        workspace=legacy_job_completion_operations.LegacyWorkspaceDependencies(
            container_provisioner=container_provisioner_module.container_provisioner,
            vm_provisioner=vm_provisioner_module.vm_provisioner,
            recovery_store=vm_workspace_recovery_store_module.VMWorkspaceRecoveryStore(
                resources.postgres_db
            ),
            cloud_router=resources.main_cloud_router,
            sudo_gate=sudo_gate_module.sudo_gate,
            get_container_context=job_workspace_runtime.get_container_context,
            get_vm_context=job_workspace_runtime.get_vm_context,
            get_infra_transient_context=job_workspace_runtime.get_infra_transient_context,
            job_needs_vm=job_workspace_runtime.job_needs_vm,
            check_vm_permission=bound(
                vm_workspace_policy.check_vm_permission,
                preparation_composition.vm_permission_dependencies,
                resources,
            ),
            capture_freeze_snapshot=lambda *args, **kwargs: (
                controls_composition.job_control_operations(
                    resources
                ).capture_workspace_snapshot_for_freeze(*args, **kwargs)
            ),
            unmerged_pr_gate_reason=lambda *args, **kwargs: (
                controls_composition.job_control_operations(
                    resources
                ).unmerged_pr_gate_reason(*args, **kwargs)
            ),
        ),
        verification=legacy_job_completion_operations.LegacyVerificationDependencies(
            handle_critic_verdict=functools.partial(
                verification_operations.handle_critic_verdict_on_complete,
                dependencies=verification,
            ),
            materialize_critic_verdict=functools.partial(
                verification_operations.materialize_critic_verdict_transactional,
                dependencies=verification,
            ),
            run_critic_verdict_followups=functools.partial(
                verification_operations.run_critic_verdict_followups,
                dependencies=verification,
            ),
            trigger_verification=functools.partial(
                verification_operations.trigger_verification_on_complete,
                dependencies=verification,
            ),
            materialize_verification_critic=functools.partial(
                verification_operations.materialize_verification_critic_transactional,
                dependencies=verification,
            ),
            run_verification_critic_handoff=functools.partial(
                verification_operations.run_verification_critic_handoff,
                dependencies=verification,
            ),
            verification_rounds=verification_operations.verification_rounds,
        ),
        subjobs=legacy_job_completion_operations.LegacySubjobDependencies(
            graft_completed_subjob=functools.partial(
                subjob_output_operations.maybe_graft_completed_subjob,
                dependencies=output,
            ),
            handle_scholar_completion=functools.partial(
                subjob_completion_operations.handle_scholar_completion,
                dependencies=scholar,
            ),
            handle_delegation_completion=functools.partial(
                subjob_completion_operations.handle_delegation_child_completion,
                dependencies=delegation,
            ),
        ),
        post_commit=legacy_job_completion_operations.LegacyPostCommitDependencies(
            internal_resume_job=lambda *args, **kwargs: (
                controls_composition.job_control_operations(
                    resources
                ).internal_resume_job(*args, **kwargs)
            ),
            resume_job_without_vm=lambda *args, **kwargs: (
                controls_composition.job_control_operations(
                    resources
                ).resume_job_without_vm_internal(*args, **kwargs)
            ),
            notify_operator_freeze=(
                lambda *args, **kwargs: (
                    job_freeze_notification_service.notify_operator_freeze(
                        *args,
                        **kwargs,
                        dependencies=workflows_composition.job_freeze_notification_dependencies(
                            resources
                        ),
                    )
                )
            ),
            trigger_curation_final_pass=(
                lambda *args, **kwargs: (
                    curation_final_pass_service.trigger_curation_final_pass(
                        *args,
                        **kwargs,
                        dependencies=workflows_composition.curation_final_pass_dependencies(
                            resources
                        ),
                    )
                )
            ),
            advance_project_loop=(
                lambda *args, **kwargs: (
                    project_loop_advance_service.advance_project_loop(
                        *args,
                        **kwargs,
                        dependencies=workflows_composition.project_loop_dependencies(
                            resources
                        ),
                    )
                )
            ),
            prepare_project_loop_advance=(
                lambda *args, **kwargs: (
                    project_loop_advance_service.prepare_atomic_project_loop_advance(
                        *args,
                        **kwargs,
                        dependencies=workflows_composition.project_loop_dependencies(
                            resources
                        ),
                    )
                )
            ),
            materialize_project_loop_advance=(
                lambda *args, **kwargs: (
                    project_loop_advance_service.materialize_prepared_project_loop_advance(
                        *args,
                        **kwargs,
                        dependencies=workflows_composition.project_loop_dependencies(
                            resources
                        ),
                    )
                )
            ),
            execute_project_loop_handoff=(
                lambda *args, **kwargs: (
                    project_loop_advance_service.execute_persisted_project_loop_handoff(
                        *args,
                        **kwargs,
                        dependencies=workflows_composition.project_loop_dependencies(
                            resources
                        ),
                    )
                )
            ),
            project_loop_handoff_error_output=(
                project_loop_advance_service.project_loop_handoff_error_output
            ),
            maybe_wake_session=session_wake.maybe_wake_session,
            trigger_dispatch=bound(
                job_dispatcher.trigger_dispatch,
                jobs_composition.job_dispatch_dependencies,
                resources,
            ),
            kick_session_wake_drain=session_wake.kick_drain,
        ),
        effects=legacy_job_completion_operations.LegacyCompletionEffectOperations(
            run=completion_effect_operations.run_completion_effect,
            run_workspace_teardown=(
                lambda *args, **kwargs: (
                    completion_effect_operations.run_completion_workspace_teardown(
                        *args,
                        **kwargs,
                        dependencies=completion_effect_dependencies(resources),
                    )
                )
            ),
            dedup_key=completion_effect_operations.completion_effect_dedup_key,
        ),
        require_internal=access.require_internal,
        require_srw_runtime=manifest_runtime_ownership.require_srw_runtime,
        completion_commands_enabled=lambda: resources.settings.completion_commands_enabled,
        logger=logger,
    )


async def run_persisted_completion(
    resources: ApplicationResources, effect_runner: Any
) -> dict[str, Any]:
    return await job_completion_operations.run_persisted_completion_workflow(
        effect_runner,
        dependencies=job_completion_operations.PersistedCompletionDependencies(
            legacy_complete=bound(
                legacy_job_completion_operations.complete_job_legacy,
                legacy_completion_dependencies,
                resources,
            ),
        ),
    )


def job_completion_dependencies(
    resources: ApplicationResources,
) -> job_completion_operations.JobCompletionDependencies:
    async def accept_completion_command(*args: Any, **kwargs: Any) -> Any:
        from orchestrator.services.job_completion_commands import (
            accept_completion_command as operation,
        )

        return await operation(*args, **kwargs)

    return job_completion_operations.JobCompletionDependencies(
        store=resources.postgres_db,
        require_internal=access.require_internal,
        commands_enabled=lambda: resources.settings.completion_commands_enabled,
        status_reorder_enabled=lambda: resources.settings.completion_status_reorder_enabled,
        inline_delay_seconds=lambda: resources.settings.completion_finalizer_inline_delay_seconds,
        accept_command=accept_completion_command,
        finalizer=resources.completion_runtime.finalizer,
        legacy_complete=bound(
            legacy_job_completion_operations.complete_job_legacy,
            legacy_completion_dependencies,
            resources,
        ),
        logger=logger,
        sleep=asyncio.sleep,
    )


def completion_recovery_dependencies(
    resources: ApplicationResources,
) -> completion_recovery_operations.CompletionRecoveryDependencies:
    return completion_recovery_operations.CompletionRecoveryDependencies(
        store=resources.postgres_db,
        completion_commands_enabled=lambda: resources.settings.completion_commands_enabled,
        trigger_dispatch=bound(
            job_dispatcher.trigger_dispatch,
            jobs_composition.job_dispatch_dependencies,
            resources,
        ),
        completion_resume_guard_kwargs=(
            lambda: resources.completion_control_boundary.resume_guard_kwargs()
        ),
        completion_dispatch_guard_kwargs=(
            lambda: resources.completion_control_boundary.dispatch_guard_kwargs()
        ),
        wait_for_stateless_cancel_settle=lambda job_id: (
            controls_composition.job_mutation_operations(
                resources
            ).wait_for_stateless_cancel_settle(job_id)
        ),
        notify_operator_freeze=(
            lambda *args, **kwargs: (
                job_freeze_notification_service.notify_operator_freeze(
                    *args,
                    **kwargs,
                    dependencies=workflows_composition.job_freeze_notification_dependencies(
                        resources
                    ),
                )
            )
        ),
        handle_scholar_completion=(
            lambda job, actions: subjob_completion_operations.handle_scholar_completion(
                job,
                actions,
                dependencies=scholar_completion_dependencies(resources),
            )
        ),
        handle_delegation_child_completion=(
            lambda job, actions: (
                subjob_completion_operations.handle_delegation_child_completion(
                    job,
                    actions,
                    dependencies=delegation_completion_dependencies(resources),
                )
            )
        ),
    )


def install_completion_runtime(resources: ApplicationResources) -> None:
    """Wire the application's one completion runtime and control boundary.

    Called once while the resources are built (before any request or task):
    the runtime's workflow and alert callbacks close over ``resources``, so the
    runtime, its control boundary, the completion alerts and the session-memory
    runtime all belong to exactly this application.
    """

    resources.completion_alerts = CompletionAlerts(
        CompletionAlertDependencies(
            store=resources.postgres_db,
            notify_all_officers=session_wake.notify_all_officers,
            kick_officer_event_drain=session_wake.kick_event_drain,
        )
    )
    resources.completion_runtime = CompletionRuntime(
        CompletionRuntimeDependencies(
            store=resources.postgres_db,
            workflow=functools.partial(run_persisted_completion, resources),
            commands_enabled=lambda: resources.settings.completion_commands_enabled,
            status_reorder_enabled=(
                lambda: resources.settings.completion_status_reorder_enabled
            ),
            sweep_alert=resources.completion_alerts.sweep,
            resolution_alert=resources.completion_alerts.resolution,
            monitor_alert=resources.completion_alerts.monitor,
            max_queued_session_age_seconds=(
                lambda: float(
                    os.getenv("STATELESS_SESSION_QUEUED_AGE_ALARM_S", "60") or "60"
                )
            ),
            logger=logger,
        )
    )
    resources.completion_control_boundary = CompletionControlBoundary(
        resources.completion_runtime
    )
    resources.session_memory_runtime = SessionMemoryRuntime(
        SessionMemoryDependencies(
            store=resources.postgres_db,
            vector_store=resources.vector_db,
            authorize_thread_project_ids=(
                lambda *args, **kwargs: bound(
                    thread_project_authorization_service.authorize_thread_project_ids,
                    sessions_composition.thread_project_authorization_dependencies,
                    resources,
                )(*args, **kwargs)
            ),
            resolve_session_config=(
                lambda *args, **kwargs: bound(
                    session_config_resolution.resolve_session_config,
                    preparation_composition.session_config_dependencies,
                    resources,
                )(*args, **kwargs)
            ),
        )
    )
