"""Router dependency factories and router registration, in their fixed order.

``bind_router_dependencies`` publishes, on ``app.state``, the dependency
factory every router reads per request (``request.app.state.<name>()``); each
factory builds the router's dependency object from this application's
resources at call time. ``include_routers`` registers the routers in exactly
the order the former module used — Starlette matches routes first-match, so
the order is part of the API (for example the connector router's literal
``/api/projects/linkable-datasource-targets`` must precede the project
router's ``/api/projects/{project_id}``). ``scripts/check_endpoint_auth.py``
reads ``include_routers`` statically, so it stays a straight-line sequence of
``app.include_router`` calls.
"""

from __future__ import annotations

import os

from fastapi import FastAPI

from orchestrator.application import (
    administration as administration_composition,
    catalogue as catalogue_composition,
    completion as completion_composition,
    controls as controls_composition,
    jobs as jobs_composition,
    preparation as preparation_composition,
    projects as projects_composition,
    sessions as sessions_composition,
    transport as transport_composition,
    workflows as workflows_composition,
    workspace as workspace_composition,
)
from orchestrator.application.resources import ApplicationResources
from orchestrator.auth import bff_router
from orchestrator.graph_routes import router as graph_router
from orchestrator.routers import (
    access_tokens as access_token_routes,
    actions as actions_routes,
    agent_child_threads as agent_child_threads_routes,
    agent_cloud_stage as agent_cloud_stage_routes,
    agent_officer as agent_officer_routes,
    agent_registration as agent_registration_routes,
    agent_thread_status as agent_thread_status_routes,
    agent_thread_workspace as agent_thread_workspace_routes,
    automations_router,
    canvases_router,
    capacity as capacity_routes,
    citations as citations_routes,
    config_catalog as config_catalog_routes,
    datasources as datasources_routes,
    diagnostics as diagnostics_routes,
    expert_catalog as expert_catalog_routes,
    ide as ide_routes,
    identity as identity_routes,
    infrastructure_admin as infrastructure_admin_routes,
    internal_canvases_router,
    job_artifacts as job_artifacts_routes,
    job_assignment as job_assignment_routes,
    job_audit as job_audit_routes,
    job_completion as job_completion_routes,
    job_controls as job_control_routes,
    job_diagnostics as job_diagnostics_routes,
    job_diff as job_diff_routes,
    job_inspection as job_inspection_routes,
    job_lifecycle as job_lifecycle_routes,
    job_reads as job_reads_routes,
    job_repo as job_repo_routes,
    job_review as job_review_routes,
    knowledge as knowledge_routes,
    loop_plan as loop_plan_routes,
    main_cloud_settings as main_cloud_settings_routes,
    manifests as manifest_routes,
    media as media_routes,
    messaging as messaging_routes,
    model_catalog as model_catalog_routes,
    notifications as notification_routes,
    officer_runtime_verification as officer_runtime_verification_routes,
    officers as officer_routes,
    product_capabilities_router,
    project_jobs as project_jobs_routes,
    project_loops_router,
    projects as projects_routes,
    provider_catalog as provider_catalog_routes,
    provider_credentials as provider_credentials_routes,
    run_queue_admin as run_queue_admin_routes,
    shared_browser_router,
    ssh_access as ssh_access_routes,
    subscription_management as subscription_management_routes,
    system_readiness as system_readiness_routes,
    system_settings as system_settings_routes,
    thread_admission as thread_admission_routes,
    thread_cloud_diff as thread_cloud_diff_routes,
    thread_config as thread_config_routes,
    thread_files as thread_files_routes,
    thread_history as thread_history_routes,
    thread_lifecycle as thread_lifecycle_routes,
    thread_permissions as thread_permission_routes,
    thread_rewind as thread_rewind_routes,
    thread_session as thread_session_routes,
    thread_transport as thread_transport_routes,
    unit_claim as unit_claim_routes,
    usage_reporting as usage_reporting_routes,
    user_administration as user_administration_routes,
    verification as verification_routes,
    vm_creation_retry_authority as vm_creation_retry_authority_routes,
    vm_guest_router,
    vm_resource_inventory as vm_resource_inventory_routes,
    vm_workspace_cleanup_authority as vm_workspace_cleanup_authority_routes,
    voice as voice_routes,
    wopi_router,
    workspace_access as workspace_access_routes,
)
from orchestrator.routers.bench import router as bench_router
from orchestrator.routers.contacts import (
    ContactsDependencies,
    project_router as contacts_project_router,
    router as contacts_router,
)
from orchestrator.routers.preferences import (
    PreferencesDependencies,
    router as preferences_router,
)
from orchestrator.routers.sessions import router as sessions_router
from orchestrator.routers.tables import TablesDependencies, router as tables_router
from orchestrator.security import access
from orchestrator.services import (
    expert_catalog,
    vm_workspace_recovery_store as vm_workspace_recovery_store_module,
)
from orchestrator.services.vm_creation_retry_store import VMCreationRetryStore
from orchestrator.uploads import router as uploads_router


def bind_router_dependencies(app: FastAPI, resources: ApplicationResources) -> None:
    """Publish every router's per-request dependency factory on ``app.state``."""
    app.state.catalogue_resources = resources.catalogue_resources
    app.state.contacts_dependencies = ContactsDependencies(db=resources.postgres_db)
    app.state.store = resources.postgres_db
    app.state.job_reads_dependencies_factory = (
        lambda: jobs_composition.job_reads_dependencies(resources)
    )
    app.state.provider_catalog_dependencies_factory = (
        lambda: catalogue_composition.provider_catalog_dependencies(resources)
    )
    app.state.model_catalog_dependencies_factory = (
        lambda: catalogue_composition.model_catalog_dependencies(resources)
    )
    app.state.config_catalog_dependencies_factory = (
        lambda: catalogue_composition.config_catalog_dependencies(resources)
    )
    app.state.manifest_dependencies_factory = (
        lambda: catalogue_composition.manifest_dependencies(resources)
    )
    app.state.job_inspection_dependencies_factory = (
        lambda: jobs_composition.job_inspection_dependencies(resources)
    )
    app.state.job_audit_dependencies_factory = (
        lambda: jobs_composition.job_audit_dependencies(resources)
    )
    app.state.job_artifacts_dependencies_factory = (
        lambda: jobs_composition.job_artifacts_dependencies(resources)
    )
    app.state.diagnostics_dependencies_factory = (
        lambda: administration_composition.diagnostics_dependencies(resources)
    )
    app.state.capacity_dependencies_factory = (
        lambda: administration_composition.capacity_dependencies(resources)
    )
    app.state.identity_dependencies_factory = (
        lambda: administration_composition.identity_dependencies(resources)
    )
    app.state.access_token_dependencies_factory = (
        lambda: administration_composition.access_token_dependencies(resources)
    )
    app.state.ssh_access_dependencies_factory = (
        lambda: administration_composition.ssh_access_dependencies(resources)
    )
    app.state.usage_reporting_dependencies_factory = (
        lambda: administration_composition.usage_reporting_dependencies(resources)
    )
    app.state.infrastructure_admin_dependencies_factory = (
        lambda: administration_composition.infrastructure_admin_dependencies(resources)
    )
    app.state.provider_credentials_dependencies_factory = (
        lambda: administration_composition.provider_credentials_dependencies(resources)
    )
    app.state.subscription_management_dependencies_factory = (
        lambda: administration_composition.subscription_management_dependencies(
            resources
        )
    )
    app.state.voice_dependencies_factory = (
        lambda: administration_composition.voice_dependencies(resources)
    )
    app.state.system_settings_dependencies_factory = (
        lambda: administration_composition.system_settings_dependencies(resources)
    )
    app.state.user_administration_dependencies_factory = (
        lambda: administration_composition.user_administration_dependencies(resources)
    )
    app.state.job_diagnostics_dependencies_factory = (
        lambda: administration_composition.job_diagnostics_dependencies(resources)
    )
    app.state.datasources_dependencies_factory = (
        lambda: projects_composition.datasources_dependencies(resources)
    )
    app.state.projects_dependencies_factory = (
        lambda: projects_composition.projects_dependencies(resources)
    )
    app.state.knowledge_dependencies_factory = (
        lambda: projects_composition.knowledge_dependencies(resources)
    )
    app.state.citations_dependencies_factory = (
        lambda: projects_composition.citations_dependencies(resources)
    )
    app.state.media_dependencies_factory = (
        lambda: projects_composition.media_dependencies(resources)
    )
    app.state.ide_dependencies_factory = lambda: workspace_composition.ide_dependencies(
        resources
    )
    app.state.workspace_access_dependencies_factory = (
        lambda: workspace_composition.workspace_access_dependencies(resources)
    )
    app.state.thread_files_dependencies_factory = (
        lambda: workspace_composition.thread_files_dependencies(resources)
    )
    app.state.job_repo_dependencies_factory = (
        lambda: workspace_composition.job_repo_dependencies(resources)
    )
    app.state.job_diff_dependencies_factory = (
        lambda: workspace_composition.job_diff_dependencies(resources)
    )
    app.state.job_review_dependencies_factory = (
        lambda: workspace_composition.job_review_dependencies(resources)
    )
    app.state.agent_cloud_stage_dependencies_factory = (
        lambda: workspace_composition.agent_cloud_stage_dependencies(resources)
    )
    app.state.thread_cloud_diff_dependencies_factory = (
        lambda: workspace_composition.thread_cloud_diff_dependencies(resources)
    )
    app.state.main_cloud_settings_dependencies_factory = (
        lambda: workspace_composition.main_cloud_settings_dependencies(resources)
    )
    app.state.expert_catalog_state = resources.expert_catalog_state
    app.state.thread_workspace_delivery_dependencies_factory = (
        lambda: preparation_composition.thread_workspace_delivery_dependencies(
            resources
        )
    )
    app.state.agent_registration_dependencies_factory = (
        lambda: sessions_composition.agent_registration_dependencies(resources)
    )
    app.state.agent_child_threads_dependencies_factory = (
        lambda: sessions_composition.agent_child_threads_dependencies(resources)
    )
    app.state.officer_runtime_verification_dependencies_factory = (
        lambda: sessions_composition.officer_runtime_verification_dependencies(
            resources
        )
    )
    app.state.thread_admission_dependencies_factory = (
        lambda: sessions_composition.thread_admission_dependencies(resources)
    )
    app.state.thread_config_dependencies_factory = (
        lambda: sessions_composition.thread_config_update_dependencies(resources)
    )
    app.state.unit_claim_bundle_dependencies_factory = (
        lambda: sessions_composition.unit_claim_bundle_dependencies(resources)
    )
    app.state.run_queue_admin_dependencies_factory = (
        lambda: sessions_composition.run_queue_admin_dependencies(resources)
    )
    app.state.sessions_dependencies_factory = (
        lambda: sessions_composition.sessions_dependencies(resources)
    )
    app.state.agent_thread_status_dependencies_factory = (
        lambda: sessions_composition.agent_thread_status_dependencies(resources)
    )
    app.state.agent_messaging_dependencies_factory = (
        lambda: workflows_composition.agent_messaging_dependencies(resources)
    )
    app.state.inbound_reply_dependencies_factory = (
        lambda: workflows_composition.inbound_reply_dependencies(resources)
    )
    app.state.officer_message_action_dependencies_factory = (
        lambda: workflows_composition.officer_message_action_dependencies(resources)
    )
    app.state.job_guidance_dependencies_factory = (
        lambda: workflows_composition.job_guidance_dependencies(resources)
    )
    app.state.message_thread_read_dependencies_factory = (
        lambda: workflows_composition.message_thread_read_dependencies(resources)
    )
    app.state.pending_actions_dependencies_factory = (
        lambda: workflows_composition.pending_actions_dependencies(resources)
    )
    app.state.officer_post_view_dependencies_factory = (
        lambda: workflows_composition.officer_post_view_dependencies(resources)
    )
    app.state.officer_post_lifecycle_dependencies_factory = (
        lambda: workflows_composition.officer_post_lifecycle_dependencies(resources)
    )
    app.state.officer_paging_dependencies_factory = (
        lambda: workflows_composition.officer_paging_dependencies(resources)
    )
    app.state.notification_api_dependencies_factory = (
        lambda: workflows_composition.notification_api_dependencies(resources)
    )
    app.state.loop_plan_filing_dependencies_factory = (
        lambda: workflows_composition.loop_plan_filing_dependencies(resources)
    )
    app.state.job_completion_dependencies_factory = (
        lambda: completion_composition.job_completion_dependencies(resources)
    )
    app.state.verification_route_dependencies_factory = (
        lambda: verification_routes.VerificationRouteDependencies(
            workflow=completion_composition.verification_dependencies(resources),
            require_internal=access.require_internal,
        )
    )
    app.state.automations_dependencies_factory = (
        lambda: workflows_composition.automations_dependencies(resources)
    )
    app.state.project_loops_dependencies_factory = (
        lambda: workflows_composition.project_loops_dependencies(resources)
    )
    app.state.job_control_dependencies_factory = (
        lambda: controls_composition.job_control_route_dependencies(resources)
    )
    app.state.job_control_route_dependencies_factory = (
        lambda: controls_composition.job_mutation_route_dependencies(resources)
    )
    app.state.job_lifecycle_route_dependencies_factory = (
        lambda: jobs_composition.job_lifecycle_route_dependencies(resources)
    )
    app.state.thread_lifecycle_dependencies_factory = (
        lambda: controls_composition.thread_lifecycle_dependencies(resources)
    )
    app.state.thread_rewind_dependencies_factory = (
        lambda: controls_composition.thread_rewind_dependencies(resources)
    )
    app.state.job_assignment_dependencies_factory = (
        lambda: controls_composition.job_assignment_dependencies(resources)
    )
    app.state.expert_catalog_dependencies_factory = (
        lambda: catalogue_composition.expert_catalog_dependencies(resources)
    )
    app.state.tables_dependencies = TablesDependencies(db=resources.postgres_db)
    app.state.preferences_dependencies = PreferencesDependencies(
        db=resources.postgres_db,
        role_base=lambda role: expert_catalog.role_base_or_empty(role),
        environ=os.environ,
    )
    app.state.bench_dependencies_factory = lambda: jobs_composition.bench_dependencies(
        resources
    )
    app.state.thread_session_dependencies_factory = (
        lambda: transport_composition.thread_session_dependencies(resources)
    )
    app.state.thread_history_dependencies_factory = (
        lambda: transport_composition.thread_history_dependencies(resources)
    )
    app.state.thread_transport_dependencies_factory = (
        lambda: transport_composition.thread_transport_dependencies(resources)
    )
    app.state.thread_permission_dependencies_factory = (
        lambda: transport_composition.thread_permission_dependencies(resources)
    )
    app.state.shared_browser_dependencies_factory = lambda: (
        workspace_composition.shared_browser_dependencies(resources)
    )
    app.state.system_readiness_dependencies_factory = (
        lambda: system_readiness_routes.SystemReadinessDependencies(
            store=resources.postgres_db
        )
    )


def configure_process_routers(resources: ApplicationResources) -> None:
    """Bind the three VM authority routers' module-level stores.

    These routers keep process-wide configuration rather than reading
    ``app.state``; the most recently built application configures them.
    """
    vm_workspace_cleanup_authority_routes.configure(
        store_factory=lambda: vm_workspace_recovery_store_module.VMWorkspaceRecoveryStore(
            resources.postgres_db
        )
    )
    vm_creation_retry_authority_routes.configure(
        store_factory=lambda: VMCreationRetryStore(resources.postgres_db)
    )
    vm_resource_inventory_routes.configure_from_environment(resources.postgres_db)


def include_routers(app: FastAPI) -> None:
    """Register every router in its fixed order (route matching is first-match)."""
    app.include_router(bench_router)
    app.include_router(bff_router)
    app.include_router(graph_router)
    app.include_router(uploads_router)
    app.include_router(automations_router)
    app.include_router(canvases_router)
    app.include_router(internal_canvases_router)
    app.include_router(wopi_router)
    app.include_router(project_loops_router)
    app.include_router(product_capabilities_router)
    app.include_router(shared_browser_router)
    app.include_router(vm_guest_router)
    app.include_router(sessions_router)
    app.include_router(thread_rewind_routes.router)
    app.include_router(contacts_router)
    app.include_router(contacts_project_router)
    app.include_router(tables_router)
    app.include_router(preferences_router)
    app.include_router(job_reads_routes.router)
    app.include_router(provider_catalog_routes.router)
    app.include_router(model_catalog_routes.router)
    app.include_router(config_catalog_routes.router)
    app.include_router(manifest_routes.router)
    app.include_router(job_inspection_routes.router)
    app.include_router(job_audit_routes.router)
    app.include_router(job_artifacts_routes.router)
    app.include_router(diagnostics_routes.router)
    app.include_router(identity_routes.router)
    app.include_router(access_token_routes.router)
    app.include_router(ssh_access_routes.router)
    app.include_router(usage_reporting_routes.router)
    app.include_router(infrastructure_admin_routes.router)
    app.include_router(provider_credentials_routes.router)
    app.include_router(subscription_management_routes.router)
    app.include_router(voice_routes.router)
    app.include_router(system_settings_routes.router)
    app.include_router(vm_workspace_cleanup_authority_routes.router)
    app.include_router(vm_creation_retry_authority_routes.router)
    app.include_router(vm_resource_inventory_routes.router)
    app.include_router(capacity_routes.router)
    app.include_router(user_administration_routes.router)
    app.include_router(job_diagnostics_routes.router)
    app.include_router(expert_catalog_routes.router)
    app.include_router(datasources_routes.router)
    app.include_router(projects_routes.router)
    app.include_router(knowledge_routes.router)
    app.include_router(citations_routes.router)
    app.include_router(media_routes.router)
    app.include_router(ide_routes.router)
    app.include_router(workspace_access_routes.router)
    app.include_router(thread_files_routes.router)
    app.include_router(job_repo_routes.router)
    app.include_router(job_diff_routes.router)
    app.include_router(job_review_routes.router)
    app.include_router(agent_cloud_stage_routes.router)
    app.include_router(thread_cloud_diff_routes.router)
    app.include_router(main_cloud_settings_routes.router)
    app.include_router(agent_thread_workspace_routes.router)
    app.include_router(job_assignment_routes.router)
    app.include_router(unit_claim_routes.router)
    app.include_router(thread_admission_routes.router)
    app.include_router(agent_registration_routes.router)
    app.include_router(agent_child_threads_routes.router)
    app.include_router(officer_runtime_verification_routes.router)
    app.include_router(thread_config_routes.router)
    app.include_router(run_queue_admin_routes.router)
    app.include_router(agent_thread_status_routes.router)
    app.include_router(messaging_routes.router)
    app.include_router(actions_routes.router)
    app.include_router(officer_routes.router)
    app.include_router(agent_officer_routes.router)
    app.include_router(notification_routes.router)
    app.include_router(loop_plan_routes.router)
    app.include_router(job_lifecycle_routes.router)
    app.include_router(job_control_routes.router)
    app.include_router(job_completion_routes.router)
    app.include_router(verification_routes.router)
    app.include_router(thread_session_routes.router)
    app.include_router(thread_lifecycle_routes.end_router)
    app.include_router(thread_lifecycle_routes.resume_router)
    app.include_router(thread_lifecycle_routes.rewind_router)
    app.include_router(thread_history_routes.router)
    app.include_router(thread_transport_routes.router)
    app.include_router(thread_permission_routes.router)
    app.include_router(system_readiness_routes.router)
    app.include_router(project_jobs_routes.router)
