"""Orchestrator application construction.

``create_app()`` is the composition root: it builds one application's
resources, binds every router's dependency factory to them, installs the HTTP
stack and registers the routers in their fixed order. ``orchestrator.main``
calls it once at import (``uvicorn orchestrator.main:app``); tests and
operator entrypoints may build their own.

Importing this package has no side effects. ``create_app()`` performs exactly
the construction ``orchestrator.main`` used to perform at import: no network
connection, no task, no migration. Startup work belongs to the lifespan
(``orchestrator.application.lifecycle``).

Dependency direction: ``orchestrator.main`` → this package → routers,
services, schemas, database, security and ``shared``. Nothing below imports
this package; the stateless-wake operator harness is the one explicit
exception, and it only builds resources (``build_application_resources``).
"""

from __future__ import annotations

import json
import logging
import os

from fastapi import FastAPI

from orchestrator.application import (
    completion as completion_composition,
    http,
    lifecycle,
    routes,
)
from orchestrator.application.resources import ApplicationResources
from orchestrator.application.settings import DeploymentSettings
from orchestrator.database import (
    MIGRATIONS_AUDIT_DIR,
    MIGRATIONS_VECTOR_DIR,
    AuditStore,
    PostgresDB,
)
from orchestrator.security.auth import set_provisioning_backends
from orchestrator.services import ssh_access as ssh_access_operations
from orchestrator.services.catalogue_resources import CatalogueResources
from orchestrator.services.cloud import MainCloudRouter, build_backend
from orchestrator.services.cloud_task_registry import CloudTaskRegistry
from orchestrator.services.expert_catalog_contracts import ExpertCatalogState
from orchestrator.services.gitea import GiteaClient
from orchestrator.services.infrastructure_metering import InfrastructureMeteringSettings
from orchestrator.services.infrastructure_metering.bootstrap import (
    InfrastructureMeteringBootstrap,
)
from orchestrator.services.job_dispatcher import JobDispatchState
from orchestrator.services.kb_task_registry import KbDatasourceTaskRegistry
from orchestrator.services.keycloak_admin import KeycloakGroupSync
from orchestrator.services.knowledge_projection import KnowledgeGraphHandle
from orchestrator.services.project_provisioning import ProjectRepairState
from orchestrator.services.session_router import SessionRouterService
from orchestrator.services.session_tokens import SessionTokenService
from orchestrator.services.stateless_workspace_scheduler import (
    StatelessWorkspaceEnsureRegistry,
)
from orchestrator.services.thread_turn_locks import ThreadTurnLocks
from shared.db_url import build_postgres_url

logger = logging.getLogger(__name__)


def _session_ingress_annotations() -> dict:
    raw = os.environ.get("SESSION_INGRESS_ANNOTATIONS", "{}")
    try:
        annotations = json.loads(raw)
        if not isinstance(annotations, dict):
            annotations = {}
    except (ValueError, TypeError):
        logger.warning(
            "SESSION_INGRESS_ANNOTATIONS env not valid JSON: %r — falling back to {}",
            raw,
        )
        annotations = {}
    return annotations


def build_application_resources(
    settings: DeploymentSettings | None = None,
) -> ApplicationResources:
    """Construct one application's resources without connecting anything.

    Raises when the vector database credentials are missing, as the former
    module import did. The audit tier is optional: without its credentials the
    audit store stays inactive and reads degrade.
    """

    settings = (
        settings if settings is not None else DeploymentSettings.from_environment()
    )

    postgres_db = PostgresDB()
    # Vector DB — separate pgvector instance for citations, memories and the
    # knowledge index.
    vector_url = build_postgres_url("VECTOR_POSTGRES", fallback_env="VECTOR_DB_URL")
    if not vector_url:
        raise RuntimeError(
            "Vector DB credentials missing — set VECTOR_POSTGRES_USER + "
            "VECTOR_POSTGRES_PASSWORD (with VECTOR_POSTGRES_HOST/PORT/DB from "
            "ConfigMap), or fall back to VECTOR_DB_URL"
        )
    vector_db = PostgresDB(
        connection_string=vector_url,
        migrations_dir=MIGRATIONS_VECTOR_DIR,
        env_prefix="VECTOR_POSTGRES",
        default_min_connections=1,
        default_max_connections=5,
    )
    # Audit DB — the observability tier (llm_requests / agent_audit /
    # chat_history). NON-load-bearing: without credentials the orchestrator
    # runs without it (no migrations, no partition maintenance) and reads and
    # writes degrade. Skip silently, never raise.
    audit_url = build_postgres_url("AUDIT_POSTGRES", fallback_env="AUDIT_DB_URL")
    audit_db = (
        PostgresDB(
            connection_string=audit_url,
            migrations_dir=MIGRATIONS_AUDIT_DIR,
            env_prefix="AUDIT_POSTGRES",
            default_min_connections=1,
            default_max_connections=4,
        )
        if audit_url
        else None
    )
    # Audit READS are served by the Postgres AuditStore. It is null-safe:
    # is_available stays False (the read endpoints' degraded shapes, never a
    # crash) until connect() runs on a real DSN in the lifespan.
    audit_store = AuditStore(audit_url)

    # Session router — see knowledge-base/knowledge/features/direct_session_websockets.md
    session_router = SessionRouterService(
        namespace=os.environ.get("SESSION_INGRESS_NAMESPACE", "default"),
        ingress_host=os.environ.get("SESSION_INGRESS_HOST", "api.example.com"),
        ingress_class=os.environ.get("SESSION_INGRESS_CLASS", "traefik"),
        annotations=_session_ingress_annotations(),
        tls_secret_name=os.environ.get("SESSION_INGRESS_TLS_SECRET") or None,
        single_origin=os.environ.get("SESSION_INGRESS_SINGLE_ORIGIN", "").lower()
        in {"1", "true"},
        db=postgres_db,
    )
    session_jwt_secret = os.environ.get("SESSION_JWT_SECRET", "")
    if session_jwt_secret:
        session_tokens = SessionTokenService(
            secret=session_jwt_secret,
            ttl_seconds=int(os.environ.get("SESSION_JWT_TTL_S", "60")),
        )
    else:
        # Allow boot without session_tokens (e.g. during chart install before
        # the Secret is set). GET /connection fails at runtime with a clear
        # error.
        session_tokens = None
        logger.warning(
            "SESSION_JWT_SECRET not set — direct WS session endpoints will fail"
        )

    resources = ApplicationResources(
        settings=settings,
        postgres_db=postgres_db,
        vector_db=vector_db,
        audit_db=audit_db,
        audit_store=audit_store,
        audit_reader=audit_store,
        gitea_client=GiteaClient(),
        keycloak_groups=KeycloakGroupSync(),
        main_cloud_router=MainCloudRouter(build_backend()),
        session_router=session_router,
        session_tokens=session_tokens,
        session_jwt_secret=session_jwt_secret,
        # Lazy Neo4j handle: nothing connects until the first ``.get()``.
        knowledge_graph=KnowledgeGraphHandle(logger=logger),
        ssh_gateway_host_key_cache=ssh_access_operations.SshGatewayHostKeyCache(
            logger=logger
        ),
        job_dispatch_state=JobDispatchState(),
        kb_datasource_tasks=KbDatasourceTaskRegistry(),
        cloud_task_registry=CloudTaskRegistry(),
        project_repair_state=ProjectRepairState(),
        stateless_workspace_ensure_registry=StatelessWorkspaceEnsureRegistry(),
        attach_abort_successor_tasks={},
        threads_suspending=set(),
        pending_actions_cache={},
        late_cloud_setup_tasks={},
        thread_turn_locks=ThreadTurnLocks(),
        expert_catalog_state=ExpertCatalogState(),
        catalogue_resources=CatalogueResources(
            config_dir=lambda: catalogue_config_dir()
        ),
    )
    # Every infrastructure-metering gate defaults off until startup decides
    # which paths this process runs (``lifecycle.open_stores``).
    resources.metering = InfrastructureMeteringBootstrap(
        capabilities=None,
        infrastructure_metering_settings=InfrastructureMeteringSettings(),
        infrastructure_usage_v2=None,
        infrastructure_usage_rollup=None,
        infrastructure_inventory_store=None,
        infrastructure_ingestion_service=None,
        infrastructure_workspace_cutover=None,
        infrastructure_usage_materializer=None,
        infrastructure_usage_day_sealer=None,
        infrastructure_metering_runtime=None,
        infrastructure_coverage_waivers=None,
        infrastructure_storage_assets=None,
        infrastructure_storage_mapping=None,
        infrastructure_compute_activation=None,
        infrastructure_compute_scope_diagnostics={},
        infrastructure_durable_compute_activation_keys=frozenset(),
        infrastructure_durable_reporting_policy_ready=False,
        infrastructure_storage_source_activation_ready=False,
    )
    completion_composition.install_completion_runtime(resources)
    return resources


def catalogue_config_dir():
    from orchestrator.application import catalogue as catalogue_composition

    return catalogue_composition.get_config_dir()


def create_app(*, settings: DeploymentSettings | None = None) -> FastAPI:
    """Build one orchestrator application."""

    resources = build_application_resources(settings)

    # The JIT/approval paths in ``security.auth`` run as detached tasks with no
    # request scope, so they take the optional provisioning backends through
    # two named callables (R1.B02). Process-wide: the most recently built
    # application's collaborators answer. Resolved per call, so a later rebind
    # of the cloud router is visible.
    set_provisioning_backends(
        cloud_router=lambda: resources.main_cloud_router,
        forge=lambda: resources.gitea_client,
    )

    app = FastAPI(
        title="Debug Cockpit API",
        description="Backend API for the Superhuman Remote Worker Cockpit",
        version="0.1.0",
        lifespan=lifecycle.lifespan,
        default_response_class=http.CustomJSONResponse,
    )
    app.state.resources = resources
    routes.bind_router_dependencies(app, resources)
    http.install(app)
    routes.configure_process_routers(resources)
    routes.include_routers(app)
    return app


__all__ = [
    "ApplicationResources",
    "DeploymentSettings",
    "build_application_resources",
    "create_app",
]
