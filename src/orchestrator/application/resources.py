"""What one orchestrator application owns, and how composition binds operations.

``ApplicationResources`` is the per-application state that ``orchestrator.main``
used to keep as module globals: the stores, the clients whose lifetime the
application manages, the registries and locks request paths share, the
completion runtime, and the collaborators startup assigns later. One instance
is built per ``create_app()`` and published as ``app.state.resources``.

It is composition state, not a service locator: only the composition modules
in ``orchestrator.application`` read it, and each builds one domain's typed
``*Dependencies`` from it. Domain services and routers never receive it.
Process-wide service singletons (provisioners, snapshot and suspension
services, sudo gate, NATS bridge, notification, e-mail, IMAP, IDE and
workspace services) are deliberately absent: their modules own them, and
composition reads them from the owning module at call time.
"""

from __future__ import annotations

import asyncio
import functools
import inspect
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, TypeVar

if TYPE_CHECKING:
    from orchestrator.application.settings import DeploymentSettings
    from orchestrator.database import AuditStore, PostgresDB
    from orchestrator.services.catalogue_resources import CatalogueResources
    from orchestrator.services.cloud import MainCloudRouter
    from orchestrator.services.cloud_pricing import CloudCostEstimator
    from orchestrator.services.cloud_task_registry import CloudTaskRegistry
    from orchestrator.services.completion_runtime import (
        CompletionAlerts,
        CompletionControlBoundary,
        CompletionRuntime,
    )
    from orchestrator.services.completion_session_memory import SessionMemoryRuntime
    from orchestrator.services.expert_catalog_contracts import ExpertCatalogState
    from orchestrator.services.gitea import GiteaClient
    from orchestrator.services.infrastructure_metering.bootstrap import (
        InfrastructureMeteringBootstrap,
    )
    from orchestrator.services.job_dispatcher import JobDispatchState
    from orchestrator.services.kb_task_registry import KbDatasourceTaskRegistry
    from orchestrator.services.keycloak_admin import KeycloakGroupSync
    from orchestrator.services.knowledge_projection import KnowledgeGraphHandle
    from orchestrator.services.persistent_recycler import PersistentThreadRecycler
    from orchestrator.services.project_provisioning import ProjectRepairState
    from orchestrator.services.session_router import SessionRouterService
    from orchestrator.services.session_tokens import SessionTokenService
    from orchestrator.services.ssh_access import SshGatewayHostKeyCache
    from orchestrator.services.stateless_workspace_scheduler import (
        StatelessWorkspaceEnsureRegistry,
    )
    from orchestrator.services.thread_turn_locks import ThreadTurnLocks
    from orchestrator.services.usage_ledger import UsageLedger
    from orchestrator.services.usage_rollup import UsageRollup

T = TypeVar("T")


@dataclass(eq=False)
class ApplicationResources:
    """Per-application state, grouped by who manages its lifetime."""

    settings: DeploymentSettings

    # -- stores: built unconnected; opened by ``open_stores``, closed by
    # ``stop_application``. ``audit_reader`` is the audit store under the name
    # its read paths use (a test may replace either independently).
    postgres_db: PostgresDB
    vector_db: PostgresDB
    audit_db: PostgresDB | None
    audit_store: AuditStore
    audit_reader: Any

    # -- clients the application initializes and closes. ``main_cloud_router``
    # is mutated in place by installation authority; ``rebind`` replaces it.
    gitea_client: GiteaClient
    keycloak_groups: KeycloakGroupSync
    main_cloud_router: MainCloudRouter
    session_router: SessionRouterService
    session_tokens: SessionTokenService | None
    session_jwt_secret: str
    knowledge_graph: KnowledgeGraphHandle
    #: One host-key parse memo per application: an unauthenticated endpoint
    #: reads through it, so it must outlive the request and belong to exactly
    #: one application.
    ssh_gateway_host_key_cache: SshGatewayHostKeyCache

    # -- registries, locks and caches request paths share. Each must be the
    # same object across requests, and none may be shared by two applications.
    job_dispatch_state: JobDispatchState
    kb_datasource_tasks: KbDatasourceTaskRegistry
    cloud_task_registry: CloudTaskRegistry
    project_repair_state: ProjectRepairState
    stateless_workspace_ensure_registry: StatelessWorkspaceEnsureRegistry
    attach_abort_successor_tasks: dict[tuple[str, str, str, str], asyncio.Task[None]]
    #: Threads with a suspend in flight: two triggers can race on one thread
    #: within a second, and the loser must not delete the agent pod twice.
    threads_suspending: set[str]
    #: The pending-actions 5 s count cache the operation reads through.
    pending_actions_cache: dict[str, dict[str, Any]]
    #: Resume-time session-folder provisioning tasks attach paths await.
    late_cloud_setup_tasks: dict[str, asyncio.Task[None]]
    #: Per-turn pinned input locks (the stateless lane never uses them).
    thread_turn_locks: ThreadTurnLocks
    expert_catalog_state: ExpertCatalogState
    catalogue_resources: CatalogueResources

    # -- completion: one runtime and one control boundary per application,
    # wired right after construction because their callbacks close over this
    # object (see ``orchestrator.application.completion``).
    completion_alerts: CompletionAlerts = field(init=False)
    completion_runtime: CompletionRuntime = field(init=False)
    completion_control_boundary: CompletionControlBoundary = field(init=False)
    session_memory_runtime: SessionMemoryRuntime = field(init=False)

    # -- assigned during startup; every factory reads them per call, so a
    # value built later in startup (or never, without the audit tier) is seen
    # exactly as the former ``orchestrator.main`` globals were.
    metering: InfrastructureMeteringBootstrap = field(init=False)
    shutdown_event: asyncio.Event | None = None
    persistent_thread_recycler: PersistentThreadRecycler | None = None
    usage_ledger: UsageLedger | None = None
    usage_rollup: UsageRollup | None = None
    usage_cloud_estimator: CloudCostEstimator | None = None


def bound(
    operation: Callable[..., T],
    dependencies: Callable[[ApplicationResources], Any],
    resources: ApplicationResources,
) -> Callable[..., T]:
    """``operation`` with ``dependencies=dependencies(resources)`` built per call.

    This is how a consumer binds another domain's operation: the owner is named
    directly, and its dependency object is rebuilt on every call exactly as the
    former per-name forwarding functions rebuilt it. The operation is evaluated
    when the enclosing factory runs, so a patch of the owner applies. An async
    operation stays a coroutine function, so an error building the dependencies
    still surfaces when the call is awaited, not when it is scheduled.
    """

    if inspect.iscoroutinefunction(operation):

        @functools.wraps(operation)
        async def call_async(*args: Any, **kwargs: Any) -> Any:
            return await operation(
                *args, **kwargs, dependencies=dependencies(resources)
            )

        return call_async

    @functools.wraps(operation)
    def call(*args: Any, **kwargs: Any) -> Any:
        return operation(*args, **kwargs, dependencies=dependencies(resources))

    return call


__all__ = ["ApplicationResources", "bound"]
