"""Composition for agent authority, session admission and attach (R1.B06).

Session attach binding and recovery, the sessions routes, provision-or-assign,
pinned session mutation targets, commissioned Officer provisioning, agent
registration, child threads and runtime verification, thread authorization,
admission and configuration, claim bundles, the run-queue admin and thread
status.
"""

from __future__ import annotations

import functools
import logging

from orchestrator.application import (
    access as access_composition,
    controls as controls_composition,
    jobs as jobs_composition,
    preparation as preparation_composition,
    workflows as workflows_composition,
    workspace as workspace_composition,
)
from orchestrator.application.resources import ApplicationResources, bound
from orchestrator.routers import sessions as sessions_routes
from orchestrator.security import access, auth
from orchestrator.services import (
    agent_child_threads as agent_child_threads_service,
    agent_cloud_mounts,
    agent_datasource_payload,
    agent_provisioner as agent_provisioner_module,
    agent_registration as agent_registration_service,
    agent_thread_status as agent_thread_status_service,
    commissioned_officer_provisioning as commissioned_officer_provisioning_service,
    config_resolver,
    container_provisioner as container_provisioner_module,
    deployment_gates,
    dispatch_credentials,
    docker_provisioner as docker_provisioner_module,
    grant_enforcement,
    job_datasource_selection,
    job_dispatcher,
    managed_repository_authority,
    officer_conference as officer_conference_service,
    officer_post_policy as officer_post_policy_service,
    officer_post_views as officer_post_view_service,
    officer_runtime_verification as officer_runtime_verification_service,
    persistent_provisioner as persistent_provisioner_module,
    pinned_agent_authority,
    pinned_session_mutation_target as pinned_session_mutation_target_service,
    protected_cloud_engage,
    provision_or_assign as provision_or_assign_service,
    run_queue_admin as run_queue_admin_service,
    runtime_actor,
    runtime_actor_verification,
    session_attach_binding as session_attach_binding_service,
    session_attach_payload,
    session_attach_recovery as session_attach_recovery_service,
    session_class_policy,
    session_config_resolution,
    session_create_overrides,
    session_provisioner,
    session_runtime_identity,
    session_wake,
    stateless_claimant_attestation,
    stateless_workspace_scheduler,
    thread_admission as thread_admission_service,
    thread_config_update as thread_config_update_service,
    thread_datasource_authorization as thread_datasource_authorization_service,
    thread_mount_rows,
    thread_project_authorization as thread_project_authorization_service,
    thread_projection as thread_projection_operations,
    unit_claim_bundle as unit_claim_bundle_service,
    vm_provisioner as vm_provisioner_module,
    vm_workspace_policy,
    vm_workspace_recovery_store as vm_workspace_recovery_store_module,
    workspace_suspension,
    workspace_tier_policy,
)

logger = logging.getLogger(__name__)


def session_attach_binding_dependencies(
    resources: ApplicationResources,
) -> session_attach_binding_service.SessionAttachBindingDependencies:
    return session_attach_binding_service.SessionAttachBindingDependencies(
        store=resources.postgres_db,
        gitea_client=resources.gitea_client,
        agent_provisioner=agent_provisioner_module.agent_provisioner,
        persistent_provisioner=persistent_provisioner_module.persistent_provisioner,
        reserve_pinned_warm_agent_binding=pinned_agent_authority.reserve_pinned_warm_agent_binding,
        release_pinned_warm_binding_protection=pinned_agent_authority.release_pinned_warm_binding_protection,
        await_protected_cloud_runtime_ready=bound(
            protected_cloud_engage._await_protected_cloud_runtime_ready,
            workspace_composition.protected_cloud_engage_dependencies,
            resources,
        ),
        prepare_thread_repository_authority=managed_repository_authority.prepare_thread_repository_authority,
        assemble_session_attach_payload=(
            lambda *args, **kwargs: (
                session_attach_payload.assemble_session_attach_payload(
                    *args,
                    **kwargs,
                    dependencies=preparation_composition.session_attach_payload_dependencies(
                        resources
                    ),
                )
            )
        ),
        schedule_attach_abort_successor=bound(
            session_attach_recovery_service.schedule_attach_abort_successor,
            session_attach_recovery_dependencies,
            resources,
        ),
        prepare_pinned_session_mutation_target=bound(
            pinned_session_mutation_target_service.prepare_pinned_session_mutation_target,
            pinned_session_mutation_target_dependencies,
            resources,
        ),
        pinned_session_mutation_target_is_current=(
            bound(
                pinned_session_mutation_target_service.pinned_session_mutation_target_is_current,
                pinned_session_mutation_target_dependencies,
                resources,
            )
        ),
        reserve_session_attach_binding=bound(
            session_attach_binding_service.reserve_session_attach_binding,
            session_attach_binding_dependencies,
            resources,
        ),
        release_session_attach_binding=bound(
            session_attach_binding_service.release_session_attach_binding,
            session_attach_binding_dependencies,
            resources,
        ),
        send_session_attach_locked=bound(
            session_attach_binding_service.send_session_attach_locked,
            session_attach_binding_dependencies,
            resources,
        ),
    )


def sessions_dependencies(
    resources: ApplicationResources,
) -> sessions_routes.SessionsDependencies:
    """Compose the two ``/api/sessions`` endpoints' collaborators.

    ``_await_late_cloud_setup`` stays an injected callable: it reads this
    module's in-process late-setup task registry, which is a composition
    concern rather than a session-admission one. The rest reach their owning
    services directly.
    """
    return sessions_routes.SessionsDependencies(
        store=resources.postgres_db,
        agent_provisioner=agent_provisioner_module.agent_provisioner,
        container_provisioner=container_provisioner_module.container_provisioner,
        workspace_suspension_service=workspace_suspension.workspace_suspension_service,
        session_router=resources.session_router,
        session_tokens=resources.session_tokens,
        ensure_session_workspace=session_provisioner.ensure_session_workspace,
        await_late_cloud_setup=lambda thread_id: (
            controls_composition.thread_resume_operations(
                resources
            ).await_late_cloud_setup(thread_id)
        ),
        await_protected_cloud_runtime_ready=(
            lambda thread_id, **kwargs: (
                protected_cloud_engage._await_protected_cloud_runtime_ready(
                    thread_id,
                    **kwargs,
                    dependencies=workspace_composition.protected_cloud_engage_dependencies(
                        resources
                    ),
                )
            )
        ),
        session_grant_violations=(
            lambda *args, **kwargs: session_config_resolution.session_grant_violations(
                *args,
                **kwargs,
                dependencies=preparation_composition.session_config_dependencies(
                    resources
                ),
            )
        ),
        session_endpoint_violations=(
            lambda *args, **kwargs: (
                session_config_resolution.session_endpoint_violations(
                    *args,
                    **kwargs,
                    dependencies=preparation_composition.session_config_dependencies(
                        resources
                    ),
                )
            )
        ),
        find_idle_persistent_agent=(
            lambda: session_attach_binding_service.find_idle_persistent_agent(
                dependencies=session_attach_binding_dependencies(resources)
            )
        ),
        send_session_attach=(
            lambda *args, **kwargs: (
                session_attach_binding_service.send_session_attach(
                    *args,
                    **kwargs,
                    dependencies=session_attach_binding_dependencies(resources),
                )
            )
        ),
    )


def provision_or_assign_dependencies(
    resources: ApplicationResources,
) -> provision_or_assign_service.ProvisionOrAssignDependencies:
    """Compose the create-path binder's collaborators.

    Every callable below reaches its owning service directly, carrying that
    service's own dependency object built at call time.
    """
    return provision_or_assign_service.ProvisionOrAssignDependencies(
        store=resources.postgres_db,
        agent_provisioner=agent_provisioner_module.agent_provisioner,
        await_protected_cloud_runtime_ready=(
            lambda thread_id: (
                protected_cloud_engage._await_protected_cloud_runtime_ready(
                    thread_id,
                    dependencies=workspace_composition.protected_cloud_engage_dependencies(
                        resources
                    ),
                )
            )
        ),
        session_grant_violations=(
            lambda *args, **kwargs: session_config_resolution.session_grant_violations(
                *args,
                **kwargs,
                dependencies=preparation_composition.session_config_dependencies(
                    resources
                ),
            )
        ),
        session_endpoint_violations=(
            lambda *args, **kwargs: (
                session_config_resolution.session_endpoint_violations(
                    *args,
                    **kwargs,
                    dependencies=preparation_composition.session_config_dependencies(
                        resources
                    ),
                )
            )
        ),
        find_idle_persistent_agent=(
            lambda: session_attach_binding_service.find_idle_persistent_agent(
                dependencies=session_attach_binding_dependencies(resources)
            )
        ),
        send_session_attach=(
            lambda *args, **kwargs: (
                session_attach_binding_service.send_session_attach(
                    *args,
                    **kwargs,
                    dependencies=session_attach_binding_dependencies(resources),
                )
            )
        ),
    )


def session_attach_recovery_dependencies(
    resources: ApplicationResources,
) -> session_attach_recovery_service.SessionAttachRecoveryDependencies:
    return session_attach_recovery_service.SessionAttachRecoveryDependencies(
        store=resources.postgres_db,
        container_provisioner=container_provisioner_module.container_provisioner,
        docker_provisioner=docker_provisioner_module.docker_provisioner,
        workspace_suspension_service=workspace_suspension.workspace_suspension_service,
        ensure_session_workspace=session_provisioner.ensure_session_workspace,
        thread_project_ids=bound(
            thread_mount_rows.thread_project_ids,
            preparation_composition.thread_mount_dependencies,
            resources,
        ),
        reconcile_attach_abort_successor=bound(
            session_attach_recovery_service.reconcile_attach_abort_successor,
            session_attach_recovery_dependencies,
            resources,
        ),
        provision_or_assign=bound(
            provision_or_assign_service.provision_or_assign,
            provision_or_assign_dependencies,
            resources,
        ),
        successor_tasks=resources.attach_abort_successor_tasks,
    )


def pinned_session_mutation_target_dependencies(
    resources: ApplicationResources,
) -> pinned_session_mutation_target_service.PinnedSessionMutationTargetDependencies:
    return pinned_session_mutation_target_service.PinnedSessionMutationTargetDependencies(
        store=resources.postgres_db,
        agent_provisioner=agent_provisioner_module.agent_provisioner,
        persistent_provisioner=persistent_provisioner_module.persistent_provisioner,
        attest_pinned_session_mutation_pod=bound(
            pinned_session_mutation_target_service.attest_pinned_session_mutation_pod,
            pinned_session_mutation_target_dependencies,
            resources,
        ),
        pinned_session_mutation_target_is_current=(
            bound(
                pinned_session_mutation_target_service.pinned_session_mutation_target_is_current,
                pinned_session_mutation_target_dependencies,
                resources,
            )
        ),
    )


def commissioned_officer_dependencies(
    resources: ApplicationResources,
) -> commissioned_officer_provisioning_service.CommissionedOfficerDependencies:
    return commissioned_officer_provisioning_service.CommissionedOfficerDependencies(
        store=resources.postgres_db,
        persistent_provisioner=persistent_provisioner_module.persistent_provisioner,
        emit_session_provisioning_failure=bound(
            commissioned_officer_provisioning_service.emit_session_provisioning_failure,
            commissioned_officer_dependencies,
            resources,
        ),
    )


def agent_registration_dependencies(
    resources: ApplicationResources,
) -> agent_registration_service.AgentRegistrationDependencies:
    return agent_registration_service.AgentRegistrationDependencies(
        store=resources.postgres_db,
        gitea_client=resources.gitea_client,
        logger=logger,
        require_internal=access.require_internal,
        require_admin=functools.partial(access_composition.require_admin, resources),
        is_internal_call=access.is_internal_call,
        log_security_event=access.log_security_event,
        completion_commands_enabled=lambda: resources.settings.completion_commands_enabled,
        require_pinned_status_identity=deployment_gates.require_pinned_status_identity,
        thread_uses_pinned_execution=session_runtime_identity.thread_uses_pinned_execution,
        thread_accepts_runtime=session_runtime_identity.thread_accepts_runtime,
        protected_cloud_delivery_state=bound(
            protected_cloud_engage._protected_cloud_delivery_state,
            workspace_composition.protected_cloud_engage_dependencies,
            resources,
        ),
        bind_registered_persistent_agent=bound(
            session_attach_binding_service.bind_registered_persistent_agent,
            session_attach_binding_dependencies,
            resources,
        ),
        slide_thread_grant_on_liveness=runtime_actor.slide_thread_grant_on_liveness,
        trigger_dispatch=bound(
            job_dispatcher.trigger_dispatch,
            jobs_composition.job_dispatch_dependencies,
            resources,
        ),
    )


def agent_child_threads_dependencies(
    resources: ApplicationResources,
) -> agent_child_threads_service.AgentChildThreadDependencies:
    return agent_child_threads_service.AgentChildThreadDependencies(
        store=resources.postgres_db,
        gitea_client=resources.gitea_client,
        container_provisioner=container_provisioner_module.container_provisioner,
        logger=logger,
        require_internal=access.require_internal,
        is_experts_db_enabled=deployment_gates.is_experts_db_enabled,
        resolve_config=config_resolver.resolve_config,
        prefetch_roster_refs=bound(
            session_config_resolution.prefetch_roster_refs,
            preparation_composition.session_config_dependencies,
            resources,
        ),
        resolve_session_account_defaults=(
            lambda *args, **kwargs: (
                session_config_resolution.resolve_session_account_defaults(
                    *args,
                    **kwargs,
                    dependencies=preparation_composition.session_config_dependencies(
                        resources
                    ),
                )
            )
        ),
        backend_from_override=workspace_tier_policy.backend_from_override,
    )


def officer_runtime_verification_dependencies(
    resources: ApplicationResources,
) -> officer_runtime_verification_service.OfficerRuntimeVerificationDependencies:
    return officer_runtime_verification_service.OfficerRuntimeVerificationDependencies(
        store=resources.postgres_db,
        logger=logger,
        require_internal=access.require_internal,
        require_admin=functools.partial(access_composition.require_admin, resources),
        log_security_event=access.log_security_event,
        officer_runtime_verification_enabled=(
            lambda: resources.settings.officer_runtime_verification_enabled
        ),
        authorize_runtime_actor_request=runtime_actor.authorize_runtime_actor_request,
        refresh_runtime_actor_exchange=runtime_actor.refresh_runtime_actor_exchange,
        create_runtime_verification_plan=runtime_actor_verification.create_plan,
        get_runtime_verification_plan=runtime_actor_verification.get_plan,
        transition_runtime_verification_plan=runtime_actor_verification.transition_plan,
        kick_officer_event_drain=session_wake.kick_event_drain,
    )


def thread_datasource_authorization_dependencies(
    resources: ApplicationResources,
) -> thread_datasource_authorization_service.ThreadDatasourceAuthorizationDependencies:
    return thread_datasource_authorization_service.ThreadDatasourceAuthorizationDependencies(
        store=resources.postgres_db,
        thread_project_ids=bound(
            thread_mount_rows.thread_project_ids,
            preparation_composition.thread_mount_dependencies,
            resources,
        ),
    )


def thread_project_authorization_dependencies(
    resources: ApplicationResources,
) -> thread_project_authorization_service.ThreadProjectAuthorizationDependencies:
    return thread_project_authorization_service.ThreadProjectAuthorizationDependencies(
        store=resources.postgres_db,
    )


def thread_config_update_dependencies(
    resources: ApplicationResources,
) -> thread_config_update_service.ThreadConfigUpdateDependencies:
    return thread_config_update_service.ThreadConfigUpdateDependencies(
        store=resources.postgres_db,
        vm_provisioner=vm_provisioner_module.vm_provisioner,
        container_provisioner=container_provisioner_module.container_provisioner,
        recovery_store=vm_workspace_recovery_store_module.VMWorkspaceRecoveryStore(
            resources.postgres_db
        ),
        enforce_workspace_upgrade_grants=(
            lambda *args, **kwargs: (
                grant_enforcement.enforce_workspace_upgrade_grants(
                    *args,
                    **kwargs,
                    dependencies=preparation_composition.grant_enforcement_dependencies(
                        resources
                    ),
                )
            )
        ),
        require_internal=access.require_internal,
        require_thread_owner=access.require_thread_owner,
        thread_project_ids=bound(
            thread_mount_rows.thread_project_ids,
            preparation_composition.thread_mount_dependencies,
            resources,
        ),
        authorize_thread_datasource_selection=bound(
            thread_datasource_authorization_service.authorize_thread_datasource_selection,
            thread_datasource_authorization_dependencies,
            resources,
        ),
        build_datasource_tool_override=bound(
            agent_datasource_payload.build_datasource_tool_override,
            preparation_composition.datasource_payload_dependencies,
            resources,
        ),
        datasource_selection_provenance=job_datasource_selection.datasource_selection_provenance,
        enforce_session_create_grants=bound(
            grant_enforcement.enforce_session_create_grants,
            preparation_composition.grant_enforcement_dependencies,
            resources,
        ),
        inject_model_credentials=bound(
            dispatch_credentials.inject_model_credentials,
            preparation_composition.dispatch_credential_dependencies,
            resources,
        ),
        log_security_event=access.log_security_event,
    )


def thread_admission_dependencies(
    resources: ApplicationResources,
) -> thread_admission_service.ThreadAdmissionDependencies:
    return thread_admission_service.ThreadAdmissionDependencies(
        store=resources.postgres_db,
        gitea_client=resources.gitea_client,
        main_cloud_router=resources.main_cloud_router,
        agent_provisioner=agent_provisioner_module.agent_provisioner,
        container_provisioner=container_provisioner_module.container_provisioner,
        docker_provisioner=docker_provisioner_module.docker_provisioner,
        persistent_provisioner=persistent_provisioner_module.persistent_provisioner,
        vm_provisioner=vm_provisioner_module.vm_provisioner,
        enforce_readiness_gate=functools.partial(
            access_composition.enforce_readiness_gate, resources
        ),
        require_approved_user=auth.require_approved_user,
        is_experts_db_enabled=deployment_gates.is_experts_db_enabled,
        user_experts_enabled=bound(
            grant_enforcement.user_experts_enabled,
            preparation_composition.grant_enforcement_dependencies,
            resources,
        ),
        datasource_defaults_on_omission=deployment_gates.datasource_defaults_on_omission,
        is_protected_cloud_mode_enabled=deployment_gates.is_protected_cloud_mode_enabled,
        authorize_thread_project_ids=bound(
            thread_project_authorization_service.authorize_thread_project_ids,
            thread_project_authorization_dependencies,
            resources,
        ),
        authorize_thread_datasource_selection=bound(
            thread_datasource_authorization_service.authorize_thread_datasource_selection,
            thread_datasource_authorization_dependencies,
            resources,
        ),
        resolve_session_account_defaults=(
            lambda *args, **kwargs: (
                session_config_resolution.resolve_session_account_defaults(
                    *args,
                    **kwargs,
                    dependencies=preparation_composition.session_config_dependencies(
                        resources
                    ),
                )
            )
        ),
        prefetch_roster_refs=bound(
            session_config_resolution.prefetch_roster_refs,
            preparation_composition.session_config_dependencies,
            resources,
        ),
        resolve_thread_execution_lane=(
            lambda *args, **kwargs: (
                session_class_policy.resolve_thread_execution_lane(
                    *args,
                    **kwargs,
                    dependencies=preparation_composition.execution_lane_dependencies(
                        resources
                    ),
                )
            )
        ),
        build_thread_mount_rows=(
            lambda *args, **kwargs: thread_mount_rows.build_thread_mount_rows(
                *args,
                **kwargs,
                dependencies=preparation_composition.thread_mount_dependencies(
                    resources
                ),
            )
        ),
        should_skip_session_folder=bound(
            thread_mount_rows.should_skip_session_folder,
            preparation_composition.thread_mount_dependencies,
            resources,
        ),
        enforce_session_create_grants=bound(
            grant_enforcement.enforce_session_create_grants,
            preparation_composition.grant_enforcement_dependencies,
            resources,
        ),
        check_vm_permission=bound(
            vm_workspace_policy.check_vm_permission,
            preparation_composition.vm_permission_dependencies,
            resources,
        ),
        resolve_cloud_session_url=bound(
            agent_cloud_mounts._resolve_cloud_session_url,
            workspace_composition.agent_cloud_mount_dependencies,
            resources,
        ),
        validated_post_owned_officer_create_fragment=(
            lambda *args, **kwargs: (
                session_create_overrides.validated_post_owned_officer_create_fragment(
                    *args,
                    **kwargs,
                    validated_officer_post_patch=officer_post_policy_service.validated_officer_post_patch,
                )
            )
        ),
        enforce_officer_auto_pull_release=bound(
            officer_post_policy_service.enforce_officer_auto_pull_release,
            workflows_composition.officer_post_policy_dependencies,
            resources,
        ),
        can_manage_project_officer=(
            lambda *args, **kwargs: (
                officer_post_view_service.can_manage_project_officer(
                    *args,
                    **kwargs,
                    dependencies=workflows_composition.officer_post_view_dependencies(
                        resources
                    ),
                )
            )
        ),
        find_open_conference_thread=bound(
            officer_conference_service.find_open_conference_thread,
            workflows_composition.officer_conference_dependencies,
            resources,
        ),
        inherit_conference_brain=officer_conference_service.inherit_conference_brain,
        hold_officer_for_conference=bound(
            officer_conference_service.hold_officer_for_conference,
            workflows_composition.officer_conference_dependencies,
            resources,
        ),
        provision_commissioned_officer=bound(
            commissioned_officer_provisioning_service.provision_commissioned_officer,
            commissioned_officer_dependencies,
            resources,
        ),
        end_thread_flow=lambda *args, **kwargs: (
            controls_composition.thread_retirement_operations(
                resources
            ).end_thread_flow(*args, **kwargs)
        ),
        schedule_stateless_workspace_ensure=bound(
            stateless_workspace_scheduler.schedule_stateless_workspace_ensure,
            preparation_composition.stateless_workspace_schedule_dependencies,
            resources,
        ),
        schedule_protected_engage=bound(
            protected_cloud_engage._schedule_protected_engage,
            workspace_composition.protected_cloud_engage_dependencies,
            resources,
        ),
        record_protected_error=bound(
            protected_cloud_engage._record_protected_error,
            workspace_composition.protected_cloud_engage_dependencies,
            resources,
        ),
        find_idle_persistent_agent=bound(
            session_attach_binding_service.find_idle_persistent_agent,
            session_attach_binding_dependencies,
            resources,
        ),
        send_session_attach=bound(
            session_attach_binding_service.send_session_attach,
            session_attach_binding_dependencies,
            resources,
        ),
        provision_or_assign=bound(
            provision_or_assign_service.provision_or_assign,
            provision_or_assign_dependencies,
            resources,
        ),
        redact_thread_metadata=thread_projection_operations.redact_thread_metadata,
    )


def unit_claim_bundle_dependencies(
    resources: ApplicationResources,
) -> unit_claim_bundle_service.UnitClaimBundleDependencies:
    """R1.B06 root lane. The four ``*_dependencies`` entries are B05 *factories*
    bound to this application, not bound operations: passing them lets the
    service call the B05 operations directly with a fresh dependency object."""
    return unit_claim_bundle_service.UnitClaimBundleDependencies(
        db=resources.postgres_db,
        require_internal=access.require_internal,
        send_session_attach=bound(
            session_attach_binding_service.send_session_attach,
            session_attach_binding_dependencies,
            resources,
        ),
        thread_has_knowledge_scope=bound(
            thread_project_authorization_service.thread_has_knowledge_scope,
            thread_project_authorization_dependencies,
            resources,
        ),
        thread_project_ids=bound(
            thread_mount_rows.thread_project_ids,
            preparation_composition.thread_mount_dependencies,
            resources,
        ),
        resolve_background_push_workspace=lambda thread: (
            controls_composition.thread_resume_operations(
                resources
            ).resolve_background_push_workspace(thread)
        ),
        session_attach_payload_dependencies=functools.partial(
            preparation_composition.session_attach_payload_dependencies, resources
        ),
        job_workspace_authority_dependencies=functools.partial(
            preparation_composition.job_workspace_authority_dependencies, resources
        ),
        job_start_bundle_dependencies=functools.partial(
            preparation_composition.job_start_bundle_dependencies, resources
        ),
        dispatch_credential_dependencies=functools.partial(
            preparation_composition.dispatch_credential_dependencies, resources
        ),
        recovery_store=vm_workspace_recovery_store_module.VMWorkspaceRecoveryStore(
            resources.postgres_db
        ),
        attest_stateless_claimant=stateless_claimant_attestation.build_claimant_attestor(),
    )


def run_queue_admin_dependencies(
    resources: ApplicationResources,
) -> run_queue_admin_service.RunQueueAdminDependencies:
    """R1.B06 root lane. ``COMPLETION_COMMANDS_ENABLED`` is read through a
    lambda, never captured: the port contract's P1 exists because a module that
    freezes an import-time flag answers with whatever value happened to be set
    when it was first imported."""
    return run_queue_admin_service.RunQueueAdminDependencies(
        db=resources.postgres_db,
        require_admin=functools.partial(access_composition.require_admin, resources),
        completion_commands_enabled=lambda: resources.settings.completion_commands_enabled,
        get_completion_command_resolution=resources.completion_runtime.command_resolution,
    )


def agent_thread_status_dependencies(
    resources: ApplicationResources,
) -> agent_thread_status_service.AgentThreadStatusDependencies:
    """R1.B06 root lane. B09 keeps the retirement decisions and B07 the
    conference hold; both arrive as callables so this batch never owns them."""
    return agent_thread_status_service.AgentThreadStatusDependencies(
        db=resources.postgres_db,
        persistent_thread_recycler=resources.persistent_thread_recycler,
        require_internal=access.require_internal,
        thread_accepts_runtime=session_runtime_identity.thread_accepts_runtime,
        release_session_attach_binding=bound(
            session_attach_binding_service.release_session_attach_binding,
            session_attach_binding_dependencies,
            resources,
        ),
        acknowledge_retiring_failed_attach=bound(
            session_attach_binding_service.acknowledge_retiring_failed_attach,
            session_attach_binding_dependencies,
            resources,
        ),
        schedule_attach_abort_successor=bound(
            session_attach_recovery_service.schedule_attach_abort_successor,
            session_attach_recovery_dependencies,
            resources,
        ),
        begin_pinned_thread_retirement=(
            lambda *args, **kwargs: controls_composition.pinned_retirement_operations(
                resources
            ).begin_pinned_thread_retirement(*args, **kwargs)
        ),
        end_thread_flow=lambda *args, **kwargs: (
            controls_composition.thread_retirement_operations(
                resources
            ).end_thread_flow(*args, **kwargs)
        ),
        suspend_thread_resources=lambda thread_id: (
            controls_composition.thread_retirement_operations(
                resources
            ).suspend_thread_resources(thread_id)
        ),
        conclude_conference_if_any=bound(
            officer_conference_service.conclude_conference_if_any,
            workflows_composition.officer_conference_dependencies,
            resources,
        ),
    )
