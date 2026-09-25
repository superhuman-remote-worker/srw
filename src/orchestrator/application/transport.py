"""Composition for session transport, projections, permissions and attention (R1.B10).

Thread session and tool views, history, pinned forwarding, stateless input,
the transport routes (with the application's turn locks), attention and
permission decisions.
"""

from __future__ import annotations

import functools
import logging

from orchestrator.application import (
    controls as controls_composition,
    preparation as preparation_composition,
    sessions as sessions_composition,
    workspace as workspace_composition,
)
from orchestrator.application.resources import ApplicationResources, bound
from orchestrator.routers import (
    thread_history as thread_history_routes,
    thread_permissions as thread_permission_routes,
    thread_session as thread_session_routes,
    thread_transport as thread_transport_routes,
)
from orchestrator.security import access, auth
from orchestrator.services import (
    agent_cloud_mounts,
    agent_toolset_probe,
    commissioned_officer_provisioning as commissioned_officer_provisioning_service,
    container_provisioner as container_provisioner_module,
    email,
    grant_enforcement,
    notification_service as notification_service_module,
    persistent_provisioner as persistent_provisioner_module,
    pinned_forwarding as pinned_forwarding_operations,
    protected_cloud_engage,
    session_attention as session_attention_operations,
    session_config_resolution,
    session_tool_view as session_tool_view_operations,
    stateless_input_admission as stateless_input_operations,
    stateless_workspace_scheduler,
    workspace_suspension,
)

logger = logging.getLogger(__name__)


def session_tool_view_dependencies(
    resources: ApplicationResources,
) -> session_tool_view_operations.SessionToolViewDependencies:
    return session_tool_view_operations.SessionToolViewDependencies(
        store=resources.postgres_db,
        user_experts_enabled=bound(
            grant_enforcement.user_experts_enabled,
            preparation_composition.grant_enforcement_dependencies,
            resources,
        ),
        resolve_runner_grants=bound(
            grant_enforcement.resolve_runner_grants,
            preparation_composition.grant_enforcement_dependencies,
            resources,
        ),
        acknowledged_grant_strip=bound(
            session_config_resolution.acknowledged_grant_strip,
            preparation_composition.session_config_dependencies,
            resources,
        ),
        prefetch_roster_refs=bound(
            session_config_resolution.prefetch_roster_refs,
            preparation_composition.session_config_dependencies,
            resources,
        ),
        agent_toolset_measurement=bound(
            agent_toolset_probe.agent_toolset_measurement,
            preparation_composition.agent_toolset_dependencies,
            resources,
        ),
        session_config_dependencies=functools.partial(
            preparation_composition.session_config_dependencies, resources
        ),
    )


def thread_session_dependencies(
    resources: ApplicationResources,
) -> thread_session_routes.ThreadSessionDependencies:
    return thread_session_routes.ThreadSessionDependencies(
        store=resources.postgres_db,
        require_thread_owner=access.require_thread_owner,
        require_approved_user=auth.require_approved_user,
        resolve_cloud_session_url=bound(
            agent_cloud_mounts._resolve_cloud_session_url,
            workspace_composition.agent_cloud_mount_dependencies,
            resources,
        ),
        resolve_session_config=bound(
            session_config_resolution.resolve_session_config,
            preparation_composition.session_config_dependencies,
            resources,
        ),
        enforce_session_create_grants=bound(
            grant_enforcement.enforce_session_create_grants,
            preparation_composition.grant_enforcement_dependencies,
            resources,
        ),
        tool_view=session_tool_view_dependencies(resources),
    )


def thread_history_dependencies(
    resources: ApplicationResources,
) -> thread_history_routes.ThreadHistoryDependencies:
    return thread_history_routes.ThreadHistoryDependencies(
        store=resources.postgres_db,
        vector_db=resources.vector_db,
        require_thread_owner=access.require_thread_owner,
    )


def pinned_forwarding_dependencies(
    resources: ApplicationResources,
) -> pinned_forwarding_operations.PinnedForwardingDependencies:
    return pinned_forwarding_operations.PinnedForwardingDependencies(
        store=resources.postgres_db,
        workspace_suspension=workspace_suspension.workspace_suspension_service,
        protected_cloud_delivery_state=bound(
            protected_cloud_engage._protected_cloud_delivery_state,
            workspace_composition.protected_cloud_engage_dependencies,
            resources,
        ),
    )


def stateless_input_dependencies(
    resources: ApplicationResources,
) -> stateless_input_operations.StatelessInputDependencies:
    return stateless_input_operations.StatelessInputDependencies(
        store=resources.postgres_db,
        schedule_stateless_workspace_ensure=bound(
            stateless_workspace_scheduler.schedule_stateless_workspace_ensure,
            preparation_composition.stateless_workspace_schedule_dependencies,
            resources,
        ),
    )


def thread_transport_dependencies(
    resources: ApplicationResources,
) -> thread_transport_routes.ThreadTransportDependencies:
    return thread_transport_routes.ThreadTransportDependencies(
        store=resources.postgres_db,
        require_thread_owner=access.require_thread_owner,
        require_approved_user=auth.require_approved_user,
        forwarding=pinned_forwarding_dependencies(resources),
        stateless_input=stateless_input_dependencies(resources),
        turn_locks=resources.thread_turn_locks,
    )


def magic_link_cockpit_url() -> str:
    return email.email_service.cockpit_url or "http://localhost:4200"


def session_attention_dependencies(
    resources: ApplicationResources,
) -> session_attention_operations.SessionAttentionDependencies:
    """Attention sleep, permission reminders and permission-decision wake.

    The recycler is read through a provider because startup assigns it after
    the provisioners; retirement operations are recomposed per call.
    """
    return session_attention_operations.SessionAttentionDependencies(
        store=resources.postgres_db,
        container_provisioner=container_provisioner_module.container_provisioner,
        workspace_suspension=workspace_suspension.workspace_suspension_service,
        persistent_provisioner=persistent_provisioner_module.persistent_provisioner,
        persistent_thread_recycler=lambda: resources.persistent_thread_recycler,
        emit_session_provisioning_failure=bound(
            commissioned_officer_provisioning_service.emit_session_provisioning_failure,
            sessions_composition.commissioned_officer_dependencies,
            resources,
        ),
        thread_retirement_operations=functools.partial(
            controls_composition.thread_retirement_operations, resources
        ),
        notification_service=notification_service_module.notification_service,
        cockpit_url=magic_link_cockpit_url,
    )


def thread_permission_dependencies(
    resources: ApplicationResources,
) -> thread_permission_routes.ThreadPermissionDependencies:
    return thread_permission_routes.ThreadPermissionDependencies(
        store=resources.postgres_db,
        require_thread_owner=access.require_thread_owner,
        notification_service=notification_service_module.notification_service,
        cockpit_url=magic_link_cockpit_url,
        wake_after_permission_decision=functools.partial(
            session_attention_operations.wake_after_permission_decision,
            dependencies=session_attention_dependencies(resources),
        ),
    )
