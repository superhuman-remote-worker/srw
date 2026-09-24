"""Composition for reporting, identity and operational administration (R1.B02).

Usage and infrastructure reporting, identity, access tokens, SSH access,
provider credentials, subscriptions, voice, system settings, user
administration, diagnostics and capacity. Each factory builds one router's
dependency object from the application's resources, per request, so values
startup assigns later (the usage ledger, the metering bootstrap) are seen.
"""

from __future__ import annotations

import functools
import logging
import os

from orchestrator.application import (
    access as access_composition,
    controls as controls_composition,
    preparation as preparation_composition,
    projects as projects_composition,
)
from orchestrator.application.resources import ApplicationResources, bound
from orchestrator.routers import (
    access_tokens as access_token_routes,
    capacity as capacity_routes,
    diagnostics as diagnostics_routes,
    identity as identity_routes,
    infrastructure_admin as infrastructure_admin_routes,
    job_diagnostics as job_diagnostics_routes,
    provider_credentials as provider_credentials_routes,
    ssh_access as ssh_access_routes,
    subscription_management as subscription_management_routes,
    system_settings as system_settings_routes,
    usage_reporting as usage_reporting_routes,
    user_administration as user_administration_routes,
    voice as voice_routes,
)
from orchestrator.security import access, auth
from orchestrator.seed import llm_config
from orchestrator.services import (
    access_tokens as access_token_operations,
    deployment_gates,
    diagnostics as diagnostics_operations,
    email,
    grant_enforcement,
    infrastructure_admin as infrastructure_admin_operations,
    job_diagnostics as job_diagnostics_operations,
    notification_service as notification_service_module,
    project_provisioning as project_provisioning_operations,
    provider_credentials as provider_credentials_operations,
    snapshot_service as snapshot_service_module,
    ssh_access as ssh_access_operations,
    subscription_management as subscription_management_operations,
    system_settings as system_settings_operations,
    usage_reporting as usage_reporting_operations,
    user_administration as user_administration_operations,
    vm_provisioner as vm_provisioner_module,
    voice as voice_operations,
    workspace,
    workspace_suspension,
)
from orchestrator.services.cloud import UserId

logger = logging.getLogger(__name__)


def provider_credentials_dependencies(
    resources: ApplicationResources,
) -> provider_credentials_routes.ProviderCredentialsDependencies:
    """Resolve the credential store per invocation.

    ``postgres_db`` is rebound during ``lifespan``; a factory that captured it
    at import would bind the unconnected instance forever.
    """
    return provider_credentials_routes.ProviderCredentialsDependencies(
        store=resources.postgres_db,
        operations=provider_credentials_operations.ProviderCredentialDependencies(
            store=resources.postgres_db
        ),
        require_approved_user=auth.require_approved_user,
    )


def subscription_management_dependencies(
    resources: ApplicationResources,
) -> subscription_management_routes.SubscriptionManagementDependencies:
    """Compose the subscription adapters over the current store and logger."""
    return subscription_management_routes.SubscriptionManagementDependencies(
        operations=subscription_management_operations.SubscriptionManagementDependencies(
            store=resources.postgres_db,
            logger=logger,
            ensure_proxy_endpoint=llm_config.ensure_subscription_proxy_endpoint,
        ),
        require_admin=functools.partial(access_composition.require_admin, resources),
    )


def voice_dependencies(
    resources: ApplicationResources,
) -> voice_routes.VoiceDependencies:
    """Resolve the metering ledger per invocation — ``usage_ledger`` is ``None``
    until ``lifespan`` builds it."""
    return voice_routes.VoiceDependencies(
        store=resources.postgres_db,
        operations=voice_operations.VoiceDependencies(
            store=resources.postgres_db,
            logger=logger,
            ledger=resources.usage_ledger,
        ),
        require_approved_user=auth.require_approved_user,
        require_thread_owner=access.require_thread_owner,
    )


def system_settings_dependencies(
    resources: ApplicationResources,
) -> system_settings_routes.SystemSettingsDependencies:
    return system_settings_routes.SystemSettingsDependencies(
        operations=system_settings_operations.SystemSettingsDependencies(
            store=resources.postgres_db
        ),
        require_admin=functools.partial(access_composition.require_admin, resources),
    )


def capacity_dependencies(
    resources: ApplicationResources,
) -> capacity_routes.CapacityDependencies:
    """Admin capacity read (capacity_ux_and_queue_autoscaling.md §2)."""
    from orchestrator.services.stateless_capacity import capacity_snapshot
    from orchestrator.services.vm_resource_capacity import vm_capacity_snapshot

    return capacity_routes.CapacityDependencies(
        snapshot=lambda: capacity_snapshot(resources.postgres_db),
        require_admin=functools.partial(access_composition.require_admin, resources),
        vm_snapshot=lambda: vm_capacity_snapshot(resources.postgres_db),
    )


def user_administration_dependencies(
    resources: ApplicationResources,
) -> user_administration_routes.UserAdministrationDependencies:
    """Compose grant, capability and user-administration ports.

    Cloud routing, notification authority and the deployment feature flags stay
    owned by this application; the router receives them as callables so a
    per-request resolution always sees the current binding.
    """
    return user_administration_routes.UserAdministrationDependencies(
        store=resources.postgres_db,
        operations=user_administration_operations.UserAdministrationDependencies(
            store=resources.postgres_db,
            logger=logger,
            main_cloud_router=resources.main_cloud_router,
            user_id_type=UserId,
            provision_default_project_knowledge=(
                lambda user, project: (
                    project_provisioning_operations.provision_default_project_knowledge(
                        user,
                        project,
                        dependencies=projects_composition.project_provisioning_dependencies(
                            resources
                        ),
                    )
                )
            ),
            ensure_user_provisioned=auth.ensure_user_provisioned,
            notification_service=notification_service_module.notification_service,
            grant_project_ids=bound(
                grant_enforcement.grant_project_ids,
                preparation_composition.grant_enforcement_dependencies,
                resources,
            ),
            is_protected_cloud_mode_enabled=deployment_gates.is_protected_cloud_mode_enabled,
            datasource_scope_auto_attach_v1_enabled=(
                deployment_gates.datasource_scope_auto_attach_v1_enabled
            ),
            datasource_defaults_on_omission=deployment_gates.datasource_defaults_on_omission,
        ),
        require_admin=functools.partial(access_composition.require_admin, resources),
        require_approved_user=auth.require_approved_user,
    )


def job_diagnostics_dependencies(
    resources: ApplicationResources,
) -> job_diagnostics_routes.JobDiagnosticsDependencies:
    """Resolve the log, archive and audit readers per invocation.

    ``audit_reader`` and the workspace/snapshot services are application
    singletons that tests replace wholesale; binding them at import would pin
    the pre-lifespan objects.
    """
    return job_diagnostics_routes.JobDiagnosticsDependencies(
        store=resources.postgres_db,
        operations=job_diagnostics_operations.JobDiagnosticsDependencies(
            workspace=workspace.workspace_service,
            snapshots=snapshot_service_module.snapshot_service,
            audit_reader=resources.audit_reader,
            prepare_pinned_job_mutation_target=functools.partial(
                controls_composition.prepare_pinned_job_mutation_target, resources
            ),
        ),
        require_job_access=access.require_job_access,
        require_thread_owner=access.require_thread_owner,
    )


def usage_reporting_dependencies(
    resources: ApplicationResources,
) -> usage_reporting_routes.UsageReportingDependencies:
    """Compose the reporting ports without freezing a pre-startup ``None``.

    ``usage_ledger``, ``usage_rollup``, ``usage_cloud_estimator`` and the typed
    v2 collaborators are all assigned during ``lifespan``. Resolve them per
    invocation; a factory that captured them at import would report "metering is
    off" forever. Store lifecycle and visibility policy stay owned here.
    """
    store = resources.postgres_db
    return usage_reporting_routes.UsageReportingDependencies(
        store=store,
        reports=usage_reporting_operations.UsageReportingDependencies(
            store=store,
            audit_reader=resources.audit_reader,
            logger=logger,
            usage_ledger=resources.usage_ledger,
            usage_rollup=resources.usage_rollup,
            usage_cloud_estimator=resources.usage_cloud_estimator,
            infrastructure_usage_v2=resources.metering.infrastructure_usage_v2,
            infrastructure_usage_rollup=resources.metering.infrastructure_usage_rollup,
            visible_project_ids=lambda actor: access.user_visible_project_ids(
                actor, store
            ),
            scope_project_id=access.mcp_scope_project_id,
        ),
        require_admin=functools.partial(access_composition.require_admin, resources),
        metering_settings=resources.metering.infrastructure_metering_settings,
        scope_project_id=access.mcp_scope_project_id,
        require_approved_user=auth.require_approved_user,
        require_job_access=access.require_job_access,
        user_can_access_job_or_thread=access.user_can_access_job_or_thread,
    )


def infrastructure_admin_dependencies(
    resources: ApplicationResources,
) -> infrastructure_admin_routes.InfrastructureAdminDependencies:
    """Resolve the activation stores per invocation; every one is ``None`` at import.

    The three readiness values are wiring facts settled during ``lifespan``, so a
    request reads the state this process actually booted with rather than
    re-deriving it. ``leader_generation`` defaults to the service's own fence.
    """
    return infrastructure_admin_routes.InfrastructureAdminDependencies(
        operations=infrastructure_admin_operations.InfrastructureAdminDependencies(
            store=resources.postgres_db,
            logger=logger,
            settings=resources.metering.infrastructure_metering_settings,
            durable_compute_activation_keys=(
                resources.metering.infrastructure_durable_compute_activation_keys
            ),
            durable_reporting_policy_ready=(
                resources.metering.infrastructure_durable_reporting_policy_ready
            ),
            storage_source_activation_ready=(
                resources.metering.infrastructure_storage_source_activation_ready
            ),
            storage_assets=resources.metering.infrastructure_storage_assets,
            compute_activation=resources.metering.infrastructure_compute_activation,
            workspace_cutover=resources.metering.infrastructure_workspace_cutover,
            usage_materializer=resources.metering.infrastructure_usage_materializer,
            coverage_waivers=resources.metering.infrastructure_coverage_waivers,
            ingestion_service=resources.metering.infrastructure_ingestion_service,
            audit=access.log_security_event,
            scope_project_id=access.mcp_scope_project_id,
        ),
        require_admin=functools.partial(access_composition.require_admin, resources),
    )


def identity_dependencies(
    resources: ApplicationResources,
) -> identity_routes.IdentityDependencies:
    return identity_routes.IdentityDependencies(
        store=resources.postgres_db,
        get_current_user=auth.get_current_user,
    )


def access_token_dependencies(
    resources: ApplicationResources,
) -> access_token_routes.AccessTokenDependencies:
    return access_token_routes.AccessTokenDependencies(
        store=resources.postgres_db,
        tokens=access_token_operations.AccessTokenDependencies(
            store=resources.postgres_db
        ),
        require_approved_user=auth.require_approved_user,
        require_internal=access.require_internal,
    )


def ssh_access_dependencies(
    resources: ApplicationResources,
) -> ssh_access_routes.SshAccessDependencies:
    """Resolve the SSH collaborators per invocation.

    ``postgres_db``, ``notification_service``, ``logger`` and
    ``_session_jwt_secret`` are application globals; reading them in the body
    (never as defaults) is what keeps a later assignment visible. The host-key
    memo is application-owned and built once at module level -- building it
    here would hand every request an empty cache and silently delete the
    memoization this unauthenticated endpoint depends on.
    """
    from orchestrator.services.vm_idle_access import VMIdleAccessStore

    return ssh_access_routes.SshAccessDependencies(
        store=resources.postgres_db,
        operations=ssh_access_operations.SshAccessDependencies(
            store=resources.postgres_db,
            session_jwt_secret=resources.session_jwt_secret,
            notifier=notification_service_module.notification_service,
            logger=logger,
            host_keys=resources.ssh_gateway_host_key_cache,
            thread_is_vm_tier=workspace_suspension._thread_is_vm_tier,
        ),
        require_approved_user=auth.require_approved_user,
        require_internal=access.require_internal,
        require_personal_scope=access.require_personal_scope,
        user_can_access_ide_entity=access.user_can_access_ide_entity,
        vm_access_store=VMIdleAccessStore(resources.postgres_db),
        vm_provisioner=vm_provisioner_module.vm_provisioner,
    )


def diagnostics_dependencies(
    resources: ApplicationResources,
) -> diagnostics_routes.DiagnosticsDependencies:
    return diagnostics_routes.DiagnosticsDependencies(
        operations=diagnostics_operations.DiagnosticDependencies(
            workspace=workspace.workspace_service,
            email_renderer=email.email_service,
            getenv=os.getenv,
        ),
        require_admin=functools.partial(access_composition.require_admin, resources),
    )
