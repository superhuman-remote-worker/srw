"""Composition for workspace access, files, IDE proxy and cloud (R1.B04).

IDE, workspace access, thread files, job repository/diff/review, cloud stage,
cloud mounts, protected-cloud engage, cloud diff and main-cloud settings. The
cloud task registry is the application's; ``rebind_main_cloud_router`` is the
seam that replaces the application's cloud router.
"""

from __future__ import annotations

import asyncio
import functools
import logging
import os
from typing import Any

from orchestrator.application import (
    access as access_composition,
    catalogue as catalogue_composition,
    completion as completion_composition,
    controls as controls_composition,
    preparation as preparation_composition,
    sessions as sessions_composition,
    workflows as workflows_composition,
)
from orchestrator.application.resources import ApplicationResources, bound
from orchestrator.routers import (
    agent_cloud_stage as agent_cloud_stage_routes,
    ide as ide_routes,
    job_diff as job_diff_routes,
    job_repo as job_repo_routes,
    job_review as job_review_routes,
    main_cloud_settings as main_cloud_settings_routes,
    sessions as sessions_routes,
    shared_browser as shared_browser_routes,
    thread_cloud_diff as thread_cloud_diff_routes,
    thread_files as thread_files_routes,
    workspace_access as workspace_access_routes,
)
from orchestrator.schemas.thread_admission import ThreadCreateRequest, TrustedThreadSeed
from orchestrator.services import (
    agent_cloud_mounts,
    agent_provisioner as agent_provisioner_module,
    container_provisioner as container_provisioner_module,
    deployment_gates,
    grant_enforcement,
    ide_proxy,
    ide_session,
    job_diff_review,
    job_export,
    job_repo_reads,
    job_review_session,
    main_cloud_settings as main_cloud_settings_operations,
    project_loop_advance as project_loop_advance_service,
    protected_cloud_engage,
    session_class_policy,
    session_provisioner,
    snapshot_service as snapshot_service_module,
    subjob_output as subjob_output_operations,
    thread_admission as thread_admission_service,
    thread_cloud_diff as thread_cloud_diff_operations,
    thread_mount_rows,
    thread_workspace_delivery,
    vm_provisioner as vm_provisioner_module,
    vm_workspace_recovery_store as vm_workspace_recovery_store_module,
    workspace,
    workspace_access as workspace_access_operations,
    workspace_suspension,
    workspace_tier_policy,
)

logger = logging.getLogger(__name__)


def ide_dependencies(resources: ApplicationResources) -> ide_routes.IdeDependencies:
    """Bind the IDE session and proxy services this application owns."""
    from orchestrator.services.vm_ide_transport import VMIDETransport

    return ide_routes.IdeDependencies(
        store=resources.postgres_db,
        ide_sessions=ide_session.ide_session_service,
        ide_proxy=ide_proxy.ide_proxy_service,
        vm_ide_transport=VMIDETransport(vm_provisioner_module.vm_provisioner),
    )


def workspace_access_dependencies(
    resources: ApplicationResources,
) -> workspace_access_routes.WorkspaceAccessDependencies:
    """Compose snapshot reads, forge access grants and workspace provisioning.

    The repository resolver belongs to B08's output service and is injected
    with the application-owned store and forge collaborators.
    """
    return workspace_access_routes.WorkspaceAccessDependencies(
        store=resources.postgres_db,
        forge=resources.gitea_client,
        workspace=workspace.workspace_service,
        snapshots=snapshot_service_module.snapshot_service,
        operations=workspace_access_operations.WorkspaceOperationDependencies(
            store=resources.postgres_db,
            forge=resources.gitea_client,
            container_provisioner=container_provisioner_module.container_provisioner,
            enforce_job_workspace_upgrade_grants=(
                bound(
                    grant_enforcement.enforce_job_workspace_upgrade_grants,
                    preparation_composition.grant_enforcement_dependencies,
                    resources,
                )
            ),
        ),
        resolve_job_repo=(
            lambda job_id: subjob_output_operations.resolve_job_repo(
                job_id,
                dependencies=completion_composition.subjob_output_dependencies(
                    resources
                ),
            )
        ),
        require_admin=functools.partial(access_composition.require_admin, resources),
    )


def thread_files_dependencies(
    resources: ApplicationResources,
) -> thread_files_routes.ThreadFilesDependencies:
    """Bind both workspace provisioners plus B05's backend/lane resolvers."""
    from orchestrator.services.vm_ide_transport import VMIDETransport

    return thread_files_routes.ThreadFilesDependencies(
        store=resources.postgres_db,
        container_provisioner=container_provisioner_module.container_provisioner,
        vm_provisioner=vm_provisioner_module.vm_provisioner,
        thread_workspace_backend=workspace_tier_policy.thread_workspace_backend,
        require_stateless_workspace=session_class_policy.require_stateless_workspace,
        vm_ide_transport=VMIDETransport(vm_provisioner_module.vm_provisioner),
    )


def vm_idle_service(resources: ApplicationResources) -> Any:
    """The VM idle release/wake adapter the workspace idle sweeper drives."""
    from orchestrator.services.vm_idle_lifecycle import VMIdleLifecycleService

    return VMIdleLifecycleService(
        resources.postgres_db,
        vm_provisioner_module.vm_provisioner,
        vm_workspace_recovery_store_module.VMWorkspaceRecoveryStore(
            resources.postgres_db
        ),
        claimant=f"{os.getenv('HOSTNAME', 'orchestrator')}:vm-idle",
        agent_provisioner=agent_provisioner_module.agent_provisioner,
        thread_retirement=controls_composition.thread_retirement_operations(resources),
        thread_workspace_suspension=workspace_suspension.workspace_suspension_service,
        thread_prepare=lambda thread_id, operation_id: (
            sessions_routes.prepare_woken_pinned_session(
                thread_id,
                operation_id,
                dependencies=sessions_composition.sessions_dependencies(resources),
            )
        ),
        terminal_publication_handler=lambda operation: (
            controls_composition.job_control_operations(
                resources
            ).publish_terminal_review(operation)
        ),
    )


def job_repo_dependencies(
    resources: ApplicationResources,
) -> job_repo_routes.JobRepoDependencies:
    """Forge reads for one job's workspace repository."""
    return job_repo_routes.JobRepoDependencies(
        store=resources.postgres_db,
        repo_reads=job_repo_reads.JobRepoReadDependencies(
            forge=resources.gitea_client,
            resolve_job_repo=(
                lambda job_id: subjob_output_operations.resolve_job_repo(
                    job_id,
                    dependencies=completion_composition.subjob_output_dependencies(
                        resources
                    ),
                )
            ),
        ),
    )


def job_diff_dependencies(
    resources: ApplicationResources,
) -> job_diff_routes.JobDiffDependencies:
    """Diff review, including the completion-control authority B08 owns.

    The four control callables are injected, never re-derived: accept/reject
    must claim, guard and abort through the *same* authority the completion
    endpoint uses, and a second implementation of that policy is how two
    writers end up disagreeing about a terminal job.
    """
    return job_diff_routes.JobDiffDependencies(
        store=resources.postgres_db,
        diff_review=job_diff_review.JobDiffReviewDependencies(
            store=resources.postgres_db,
            vector_store=resources.vector_db,
            forge=resources.gitea_client,
            cloud_router=resources.main_cloud_router,
            get_completion_control=resources.completion_runtime.control,
            guard_completion_control=resources.completion_control_boundary.guard,
            claim_completion_control=resources.completion_control_boundary.claim,
            abort_completion_control_claim=resources.completion_control_boundary.abort,
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
        ),
    )


def job_review_dependencies(
    resources: ApplicationResources,
) -> job_review_routes.JobReviewDependencies:
    """Cloud export plus the review-session create B06 still owns."""
    return job_review_routes.JobReviewDependencies(
        store=resources.postgres_db,
        export=job_export.JobExportDependencies(
            store=resources.postgres_db,
            forge=resources.gitea_client,
            cloud_router=resources.main_cloud_router,
            resolve_job_repo=(
                lambda job_id: subjob_output_operations.resolve_job_repo(
                    job_id,
                    dependencies=completion_composition.subjob_output_dependencies(
                        resources
                    ),
                )
            ),
        ),
        review_session=job_review_session.JobReviewSessionDependencies(
            store=resources.postgres_db,
            create_thread=bound(
                thread_admission_service.create_thread,
                sessions_composition.thread_admission_dependencies,
                resources,
            ),
            thread_create_request=ThreadCreateRequest,
            trusted_thread_seed=TrustedThreadSeed,
            bundled_expert_bundle=(
                lambda *args, **kwargs: catalogue_composition.expert_catalog_service(
                    resources
                ).bundled_expert_bundle(*args, **kwargs)
            ),
        ),
    )


def agent_cloud_stage_dependencies(
    resources: ApplicationResources,
) -> agent_cloud_stage_routes.AgentCloudStageDependencies:
    """Agent-facing stage trigger and the two retirement reads beside it."""
    return agent_cloud_stage_routes.AgentCloudStageDependencies(
        store=resources.postgres_db,
        snapshots=snapshot_service_module.snapshot_service,
        vm_provisioner=vm_provisioner_module.vm_provisioner,
        cloud_tasks=resources.cloud_task_registry,
        is_protected_cloud_mode_enabled=deployment_gates.is_protected_cloud_mode_enabled,
        require_pinned_workspace_credential_owner=(
            bound(
                thread_workspace_delivery.require_pinned_workspace_credential_owner,
                preparation_composition.thread_workspace_delivery_dependencies,
                resources,
            )
        ),
    )


def agent_cloud_mount_dependencies(
    resources: ApplicationResources,
) -> agent_cloud_mounts.AgentCloudMountDependencies:
    """Collaborators for one cloud-payload build.

    Rebuilt per call: ``postgres_db`` and ``main_cloud_router`` are rebound
    during ``lifespan``, and the protected-mode flag is read live.
    """
    return agent_cloud_mounts.AgentCloudMountDependencies(
        store=resources.postgres_db,
        cloud_router=resources.main_cloud_router,
        cloud_tasks=resources.cloud_task_registry,
        is_protected_cloud_mode_enabled=deployment_gates.is_protected_cloud_mode_enabled,
        cloud_workspace_driver=preparation_composition.cloud_workspace_driver,
        slugify_mount_name=thread_mount_rows.slugify_mount_name,
    )


def protected_cloud_engage_dependencies(
    resources: ApplicationResources,
) -> protected_cloud_engage.ProtectedCloudEngageDependencies:
    """Collaborators for one protected-cloud engage/await/report.

    ``is_protected_cloud_mode_enabled`` is the *same* callable the mount and
    diff dependencies get, so the flag cannot disagree with itself inside one
    request.
    """
    return protected_cloud_engage.ProtectedCloudEngageDependencies(
        store=resources.postgres_db,
        cloud_router=resources.main_cloud_router,
        cloud_tasks=resources.cloud_task_registry,
        is_protected_cloud_mode_enabled=deployment_gates.is_protected_cloud_mode_enabled,
        thread_workspace_backend=workspace_tier_policy.thread_workspace_backend,
    )


def thread_cloud_diff_dependencies(
    resources: ApplicationResources,
) -> thread_cloud_diff_routes.ThreadCloudDiffRouteDependencies:
    """Owner-facing cloud-diff review over the protected-cloud engage ports."""
    return thread_cloud_diff_routes.ThreadCloudDiffRouteDependencies(
        store=resources.postgres_db,
        operations=thread_cloud_diff_operations.ThreadCloudDiffDependencies(
            store=resources.postgres_db,
            cloud_router=resources.main_cloud_router,
            snapshot_service=snapshot_service_module.snapshot_service,
            vm_provisioner=vm_provisioner_module.vm_provisioner,
            cloud_tasks=resources.cloud_task_registry,
            protected_cloud=protected_cloud_engage_dependencies(resources),
            is_protected_cloud_mode_enabled=deployment_gates.is_protected_cloud_mode_enabled,
        ),
    )


def rebind_main_cloud_router(resources: ApplicationResources, router: Any) -> None:
    """Rebind the application's cloud router.

    Nothing calls this today — ``MainCloudRouter`` is mutated in place by
    ``replace_active`` and ``main_cloud_router`` is assigned exactly once. The
    seam exists so a service that *does* swap the object replaces the
    application's resource rather than holding its own copy.
    """
    resources.main_cloud_router = router


def main_cloud_settings_dependencies(
    resources: ApplicationResources,
) -> main_cloud_settings_routes.MainCloudSettingsRouteDependencies:
    """Admin-only main-cloud configuration; installation authority preserved."""
    return main_cloud_settings_routes.MainCloudSettingsRouteDependencies(
        operations=main_cloud_settings_operations.MainCloudSettingsDependencies(
            store=resources.postgres_db,
            cloud_router=resources.main_cloud_router,
            rebind_cloud_router=functools.partial(rebind_main_cloud_router, resources),
            thread_mount_dependencies=functools.partial(
                preparation_composition.thread_mount_dependencies, resources
            ),
        ),
        require_admin=functools.partial(access_composition.require_admin, resources),
    )


def shared_browser_dependencies(
    resources: ApplicationResources,
) -> shared_browser_routes.SharedBrowserDependencies:
    """The shared-browser routes' one application collaborator (R1.B12).

    The workspace reconcile the ``open`` route kicks is fire-and-forget, as it
    was when the router imported these three names from ``orchestrator.main``.
    """

    def kick_workspace_provisioning(thread_id: str, db: Any) -> None:
        asyncio.create_task(
            session_provisioner.ensure_session_workspace(
                thread_id,
                db=db,
                provisioner=container_provisioner_module.container_provisioner,
                suspension=workspace_suspension.workspace_suspension_service,
            )
        )

    return shared_browser_routes.SharedBrowserDependencies(
        kick_workspace_provisioning=kick_workspace_provisioning
    )
