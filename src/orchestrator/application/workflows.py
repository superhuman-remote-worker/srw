"""Composition for message routing, notifications, Officers and loops (R1.B07).

Project loops, curation, loop plans, automations, the notification API and
actions, the Officer post, conference, paging and watchdog, agent messaging,
inbound replies, guidance, pending actions and freeze notifications.
"""

from __future__ import annotations

import logging
from typing import Any

from orchestrator.application import (
    controls as controls_composition,
    jobs as jobs_composition,
    preparation as preparation_composition,
    projects as projects_composition,
    sessions as sessions_composition,
)
from orchestrator.application.resources import ApplicationResources, bound
from orchestrator.routers import (
    automations as automations_router_module,
    project_loops as project_loops_router_module,
)
from orchestrator.schemas.job_controls import JobApproveRequest, JobResumeRequest
from orchestrator.security import access
from orchestrator.services import (
    agent_messaging as agent_messaging_service,
    curation_final_pass as curation_final_pass_service,
    grant_enforcement,
    inbound_reply as inbound_reply_service,
    job_dispatcher,
    job_freeze_notifications as job_freeze_notification_service,
    job_guidance as job_guidance_service,
    knowledge_index as knowledge_index_operations,
    loop_plan_filing as loop_plan_filing_service,
    message_thread_reads as message_thread_read_service,
    notification_actions as notification_action_service,
    notification_api as notification_api_service,
    notification_service as notification_service_module,
    officer_conference as officer_conference_service,
    officer_message_actions as officer_message_action_service,
    officer_paging as officer_paging_service,
    officer_post_lifecycle as officer_post_lifecycle_service,
    officer_post_policy as officer_post_policy_service,
    officer_post_views as officer_post_view_service,
    officer_watchdog as officer_watchdog_service,
    pending_actions as pending_actions_service,
    persistent_provisioner as persistent_provisioner_module,
    project_loop_advance as project_loop_advance_service,
    project_loop_spawn as project_loop_spawn_service,
    runtime_actor,
    session_wake,
    sudo_gate as sudo_gate_module,
    thread_admission as thread_admission_service,
    thread_permissions as thread_permission_operations,
    vm_workspace_policy,
)

logger = logging.getLogger(__name__)


async def provision_cron_job_repo(
    resources: ApplicationResources, job_row: dict[str, Any], db: Any
) -> None:
    """The Gitea/cloud provisioning adapter the cron dispatcher is handed.

    Parity with the ``POST /api/jobs`` handler and with automation run-now; the
    dispatcher keeps it best-effort, so a Gitea outage leaves the fired job
    repo-less rather than undoing the committed fire.
    """
    from orchestrator.services.job_provisioning import provision_job_repo

    await provision_job_repo(
        job_row=job_row,
        gitea_client=resources.gitea_client,
        postgres_db=db,
        main_cloud_router=resources.main_cloud_router,
    )


def project_loop_dependencies(
    resources: ApplicationResources,
) -> project_loop_spawn_service.ProjectLoopDependencies:
    """R1.B07 lane L. One dependency object for the whole loop engine. The
    completion flag and sweep router are callables (§P1) read from this
    application's settings and completion runtime on use; the knowledge
    reindex is B03's operation reached through its own dependency object."""
    return project_loop_spawn_service.ProjectLoopDependencies(
        store=resources.postgres_db,
        vector_store=resources.vector_db,
        notifier=notification_service_module.notification_service,
        gitea_client=resources.gitea_client,
        main_cloud_router=resources.main_cloud_router,
        trigger_dispatch=bound(
            job_dispatcher.trigger_dispatch,
            jobs_composition.job_dispatch_dependencies,
            resources,
        ),
        kick_officer_event_drain=session_wake.kick_event_drain,
        enforce_dispatch_grants=bound(
            grant_enforcement.enforce_dispatch_grants,
            preparation_composition.grant_enforcement_dependencies,
            resources,
        ),
        reindex_project_kb=(
            lambda project_id: knowledge_index_operations.reindex_project_kb(
                project_id,
                dependencies=projects_composition.knowledge_index_dependencies(
                    resources
                ),
            )
        ),
        completion_commands_enabled=lambda: resources.settings.completion_commands_enabled,
        completion_sweep_router=resources.completion_runtime.sweep_router,
    )


def curation_final_pass_dependencies(
    resources: ApplicationResources,
) -> curation_final_pass_service.CurationFinalPassDependencies:
    return curation_final_pass_service.CurationFinalPassDependencies(
        store=resources.postgres_db,
        trigger_dispatch=bound(
            job_dispatcher.trigger_dispatch,
            jobs_composition.job_dispatch_dependencies,
            resources,
        ),
        internal_resume_job=lambda *args, **kwargs: (
            controls_composition.job_control_operations(resources).internal_resume_job(
                *args, **kwargs
            )
        ),
    )


def loop_plan_filing_dependencies(
    resources: ApplicationResources,
) -> loop_plan_filing_service.LoopPlanFilingDependencies:
    return loop_plan_filing_service.LoopPlanFilingDependencies(
        store=resources.postgres_db,
        vector_store=resources.vector_db,
    )


def automations_dependencies(
    resources: ApplicationResources,
) -> automations_router_module.AutomationsDependencies:
    """R1.B07 caller closure: the automations router's collaborators, as one
    object built per request."""
    return automations_router_module.AutomationsDependencies(
        store=resources.postgres_db,
        gitea_client=resources.gitea_client,
        main_cloud_router=resources.main_cloud_router,
        trigger_dispatch=bound(
            job_dispatcher.trigger_dispatch,
            jobs_composition.job_dispatch_dependencies,
            resources,
        ),
    )


def project_loops_dependencies(
    resources: ApplicationResources,
) -> project_loops_router_module.ProjectLoopsDependencies:
    """R1.B07 caller closure: the project-loops router's collaborators, as one
    object built per request. The three loop operations are bound to the owning
    service with its own dependency object, so the router and the completion
    hook share one engine."""
    return project_loops_router_module.ProjectLoopsDependencies(
        store=resources.postgres_db,
        vector_store=resources.vector_db,
        spawn_loop_stage=(
            lambda *args, **kwargs: project_loop_spawn_service.spawn_loop_stage(
                *args, **kwargs, dependencies=project_loop_dependencies(resources)
            )
        ),
        writeback_loop_stage=(
            lambda *args, **kwargs: project_loop_spawn_service.writeback_loop_stage(
                *args, **kwargs, dependencies=project_loop_dependencies(resources)
            )
        ),
        resume_project_loop=(
            lambda *args, **kwargs: project_loop_advance_service.resume_project_loop(
                *args, **kwargs, dependencies=project_loop_dependencies(resources)
            )
        ),
        check_vm_permission=bound(
            vm_workspace_policy.check_vm_permission,
            preparation_composition.vm_permission_dependencies,
            resources,
        ),
    )


def notification_api_dependencies(
    resources: ApplicationResources,
) -> notification_api_service.NotificationApiDependencies:
    return notification_api_service.NotificationApiDependencies(
        store=resources.postgres_db,
        notifier=notification_service_module.notification_service,
    )


def notification_action_dependencies(
    resources: ApplicationResources,
) -> notification_action_service.NotificationActionDependencies:
    """R1.B07 lane N. Built once at startup and captured by the registered
    closures, so every field is either a singleton the application owns for its
    whole life or a callable that resolves its own dependencies per call. The
    job-control handlers (B09), the permission decision (B10) and the two reply
    operations (lane M) all arrive as ports and are never re-implemented."""
    return notification_action_service.NotificationActionDependencies(
        store=resources.postgres_db,
        notifier=notification_service_module.notification_service,
        sudo_gate=sudo_gate_module.sudo_gate,
        kick_officer_event_drain=session_wake.kick_event_drain,
        deliver_officer_note=session_wake.deliver_officer_note,
        route_inbound_reply=(
            lambda *args, **kwargs: inbound_reply_service.route_inbound_reply(
                *args, **kwargs, dependencies=inbound_reply_dependencies(resources)
            )
        ),
        resolve_job_notifications=lambda *args, **kwargs: (
            job_freeze_notification_service.resolve_job_notifications(
                *args,
                **kwargs,
                dependencies=job_freeze_notification_dependencies(resources),
            )
        ),
        resume_job_internal=lambda *args, **kwargs: (
            controls_composition.job_control_operations(resources).resume_job_internal(
                *args, **kwargs
            )
        ),
        approve_job_internal=lambda *args, **kwargs: (
            controls_composition.job_control_operations(resources).approve_job_internal(
                *args, **kwargs
            )
        ),
        apply_vm_upgrade_decision=lambda *args, **kwargs: (
            controls_composition.job_control_operations(
                resources
            ).apply_vm_upgrade_decision(*args, **kwargs)
        ),
        decide_permission_request=(
            lambda *args,
            **kwargs: thread_permission_operations.decide_permission_request(
                resources.postgres_db, *args, **kwargs
            )
        ),
        job_resume_request=JobResumeRequest,
        job_approve_request=JobApproveRequest,
    )


def officer_post_policy_dependencies(
    resources: ApplicationResources,
) -> officer_post_policy_service.OfficerPostPolicyDependencies:
    """The auto-pull release fence, read live (§P1)."""
    return officer_post_policy_service.OfficerPostPolicyDependencies(
        auto_pull_release_enabled=lambda: resources.settings.officer_auto_pull_release_enabled,
    )


def officer_conference_dependencies(
    resources: ApplicationResources,
) -> officer_conference_service.OfficerConferenceDependencies:
    return officer_conference_service.OfficerConferenceDependencies(
        store=resources.postgres_db,
        kick_officer_event_drain=session_wake.kick_event_drain,
    )


def officer_post_view_dependencies(
    resources: ApplicationResources,
) -> officer_post_view_service.OfficerPostViewDependencies:
    """R1.B07 lane O. The two deployment flags are callables that read this
    application's settings on use; the conference lookup is a constructed port
    so the card and the create funnel share one reading of it."""
    return officer_post_view_service.OfficerPostViewDependencies(
        store=resources.postgres_db,
        vector_store=resources.vector_db,
        usage_ledger=resources.usage_ledger,
        persistent_provisioner=persistent_provisioner_module.persistent_provisioner,
        auto_pull_release_enabled=lambda: resources.settings.officer_auto_pull_release_enabled,
        persistent_agent_reconciliation_enabled=(
            lambda: resources.settings.persistent_agent_reconciliation_enabled
        ),
        find_open_conference_thread=bound(
            officer_conference_service.find_open_conference_thread,
            officer_conference_dependencies,
            resources,
        ),
    )


def officer_post_lifecycle_dependencies(
    resources: ApplicationResources,
) -> officer_post_lifecycle_service.OfficerPostLifecycleDependencies:
    """R1.B07 lane O. ``create_thread`` (B06's one session funnel) and
    ``end_thread_flow`` (B09's stand-down) are consumed as ports, never
    re-implemented."""
    return officer_post_lifecycle_service.OfficerPostLifecycleDependencies(
        store=resources.postgres_db,
        persistent_provisioner=persistent_provisioner_module.persistent_provisioner,
        persistent_thread_recycler=resources.persistent_thread_recycler,
        policy=officer_post_policy_dependencies(resources),
        kick_officer_event_drain=session_wake.kick_event_drain,
        deliver_officer_note=session_wake.deliver_officer_note,
        create_thread=bound(
            thread_admission_service.create_thread,
            sessions_composition.thread_admission_dependencies,
            resources,
        ),
        end_thread_flow=lambda *args, **kwargs: (
            controls_composition.thread_retirement_operations(
                resources
            ).end_thread_flow(*args, **kwargs)
        ),
    )


def officer_paging_dependencies(
    resources: ApplicationResources,
) -> officer_paging_service.OfficerPagingDependencies:
    return officer_paging_service.OfficerPagingDependencies(
        store=resources.postgres_db,
        notifier=notification_service_module.notification_service,
    )


def officer_watchdog_dependencies(
    resources: ApplicationResources,
) -> officer_watchdog_service.OfficerWatchdogDependencies:
    """R1.B07 lane O. Built once when the task starts, so the recycler — which
    startup assigns after the provisioners — arrives as a callable the tick
    re-reads from the application's resources."""
    return officer_watchdog_service.OfficerWatchdogDependencies(
        store=resources.postgres_db,
        persistent_provisioner=persistent_provisioner_module.persistent_provisioner,
        persistent_thread_recycler=lambda: resources.persistent_thread_recycler,
        kick_officer_event_drain=session_wake.kick_event_drain,
        dispatch_officer_page=bound(
            officer_paging_service.dispatch_officer_page,
            officer_paging_dependencies,
            resources,
        ),
        conclude_conference_if_any=bound(
            officer_conference_service.conclude_conference_if_any,
            officer_conference_dependencies,
            resources,
        ),
        officer_runtime_verification_enabled=(
            lambda: resources.settings.officer_runtime_verification_enabled
        ),
        persistent_agent_reconciliation_enabled=(
            lambda: resources.settings.persistent_agent_reconciliation_enabled
        ),
    )


def agent_messaging_dependencies(
    resources: ApplicationResources,
) -> agent_messaging_service.AgentMessagingDependencies:
    """R1.B07 lane M. ``completion_commands_enabled`` is a callable that reads
    this application's settings on use (§P1)."""
    return agent_messaging_service.AgentMessagingDependencies(
        store=resources.postgres_db,
        notifier=notification_service_module.notification_service,
        require_internal=access.require_internal,
        completion_commands_enabled=lambda: resources.settings.completion_commands_enabled,
        kick_officer_event_drain=session_wake.kick_event_drain,
    )


def inbound_reply_dependencies(
    resources: ApplicationResources,
) -> inbound_reply_service.InboundReplyDependencies:
    """R1.B07 lane M. The completion-control guard and the resume funnel are
    B08's and B09's authorities; this batch consumes them, never re-derives."""
    return inbound_reply_service.InboundReplyDependencies(
        store=resources.postgres_db,
        notifier=notification_service_module.notification_service,
        guard_completion_control=resources.completion_control_boundary.guard,
        completion_dispatch_guard_kwargs=(
            resources.completion_control_boundary.dispatch_guard_kwargs
        ),
        internal_resume_job=lambda *args, **kwargs: (
            controls_composition.job_control_operations(resources).internal_resume_job(
                *args, **kwargs
            )
        ),
        kick_officer_event_drain=session_wake.kick_event_drain,
    )


def officer_message_action_dependencies(
    resources: ApplicationResources,
) -> officer_message_action_service.OfficerMessageActionDependencies:
    """R1.B07 lane M. The reply lane arrives as two constructed ports so the
    officer actions deliver through the existing funnel rather than a copy."""
    return officer_message_action_service.OfficerMessageActionDependencies(
        store=resources.postgres_db,
        notifier=notification_service_module.notification_service,
        require_internal=access.require_internal,
        authorize_runtime_actor_request=runtime_actor.authorize_runtime_actor_request,
        route_inbound_reply=(
            lambda *args, **kwargs: inbound_reply_service.route_inbound_reply(
                *args, **kwargs, dependencies=inbound_reply_dependencies(resources)
            )
        ),
        record_route_reply_resolution=(
            lambda *args, **kwargs: (
                inbound_reply_service.record_route_reply_resolution(
                    *args, **kwargs, dependencies=inbound_reply_dependencies(resources)
                )
            )
        ),
    )


def job_guidance_dependencies(
    resources: ApplicationResources,
) -> job_guidance_service.JobGuidanceDependencies:
    return job_guidance_service.JobGuidanceDependencies(
        store=resources.postgres_db,
        require_internal=access.require_internal,
    )


def message_thread_read_dependencies(
    resources: ApplicationResources,
) -> message_thread_read_service.MessageThreadReadDependencies:
    return message_thread_read_service.MessageThreadReadDependencies(
        store=resources.postgres_db
    )


def pending_actions_dependencies(
    resources: ApplicationResources,
) -> pending_actions_service.PendingActionsDependencies:
    return pending_actions_service.PendingActionsDependencies(
        store=resources.postgres_db,
        cache=resources.pending_actions_cache,
    )


def job_freeze_notification_dependencies(
    resources: ApplicationResources,
) -> job_freeze_notification_service.JobFreezeNotificationDependencies:
    return job_freeze_notification_service.JobFreezeNotificationDependencies(
        notifier=notification_service_module.notification_service,
    )
