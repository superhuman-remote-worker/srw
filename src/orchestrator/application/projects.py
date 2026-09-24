"""Composition for projects, datasources, knowledge and citations (R1.B03).

The knowledge index dependencies are shared by datasource CRUD, project
provisioning and the knowledge commands, so the advisory-claim ordering and
the delete-before-late-write fence have one implementation; the KB task
registry and the project repair state are the application's.
"""

from __future__ import annotations

import functools
import logging

from orchestrator.application import (
    access as access_composition,
    preparation as preparation_composition,
)
from orchestrator.application.resources import ApplicationResources, bound
from orchestrator.routers import (
    citations as citations_routes,
    datasources as datasources_routes,
    knowledge as knowledge_routes,
    media as media_routes,
    projects as projects_routes,
)
from orchestrator.security import access, auth
from orchestrator.services import (
    citations as citations_operations,
    datasource_config,
    datasources as datasources_operations,
    deployment_gates,
    dispatch_credentials,
    knowledge_index as knowledge_index_operations,
    knowledge_operations as knowledge_operations_module,
    knowledge_projection as knowledge_projection_operations,
    project_provisioning as project_provisioning_operations,
    projects as projects_operations,
    session_tool_policy,
    snapshot_service as snapshot_service_module,
)

logger = logging.getLogger(__name__)


def knowledge_index_dependencies(
    resources: ApplicationResources,
) -> knowledge_index_operations.KnowledgeIndexDependencies:
    """Collaborators for one KB index operation.

    Shared by three consumers — datasource CRUD, project provisioning's vault
    adoption, and the knowledge reindex/materialize commands — so that the
    advisory-claim ordering and the delete-before-late-write fence have exactly
    one implementation. ``tasks`` is the single app-owned registry.
    """
    return knowledge_index_operations.KnowledgeIndexDependencies(
        store=resources.postgres_db,
        vector_db=resources.vector_db,
        gitea_client=resources.gitea_client,
        logger=logger,
        tasks=resources.kb_datasource_tasks,
        inject_system_kb_embedding_profile=bound(
            dispatch_credentials.inject_system_kb_embedding_profile,
            preparation_composition.dispatch_credential_dependencies,
            resources,
        ),
    )


def datasources_dependencies(
    resources: ApplicationResources,
) -> datasources_routes.DatasourcesDependencies:
    """Compose the connector CRUD adapters over the current stores."""
    return datasources_routes.DatasourcesDependencies(
        store=resources.postgres_db,
        operations=datasources_operations.DatasourceDependencies(
            store=resources.postgres_db,
            vector_db=resources.vector_db,
            knowledge_index=knowledge_index_dependencies(resources),
            mcp_datasources_enabled=deployment_gates.mcp_datasources_enabled,
            validate_mcp_datasource=datasource_config.validate_mcp_datasource,
        ),
        require_approved_user=auth.require_approved_user,
        require_project_member=access.require_project_member,
        require_project_owner=access.require_project_owner,
        require_datasource_access=access.require_datasource_access,
        require_datasource_owner=access.require_datasource_owner,
        require_job_access=access.require_job_access,
    )


def project_provisioning_dependencies(
    resources: ApplicationResources,
) -> project_provisioning_operations.ProjectProvisioningDependencies:
    """Optional-tier provisioning ports: forge, Keycloak groups, main cloud.

    ``repair`` is the one long-lived value here — the per-project heal locks
    must be the *same* map across requests or two concurrent heals would each
    create a Space.
    """
    return project_provisioning_operations.ProjectProvisioningDependencies(
        store=resources.postgres_db,
        forge=resources.gitea_client,
        keycloak_groups=resources.keycloak_groups,
        main_cloud_router=resources.main_cloud_router,
        logger=logger,
        repair=resources.project_repair_state,
        knowledge_index=knowledge_index_dependencies(resources),
    )


def projects_dependencies(
    resources: ApplicationResources,
) -> projects_routes.ProjectsDependencies:
    """Compose the project lifecycle, membership and repository adapters.

    ``with_validated_tool_overrides`` is injected rather than duplicated: job
    create, session create and project create must keep answering identically
    about which tool categories a stored override may name.
    """
    return projects_routes.ProjectsDependencies(
        store=resources.postgres_db,
        operations=projects_operations.ProjectDependencies(
            store=resources.postgres_db,
            vector_db=resources.vector_db,
            forge=resources.gitea_client,
            keycloak_groups=resources.keycloak_groups,
            main_cloud_router=resources.main_cloud_router,
            logger=logger,
            provisioning=project_provisioning_dependencies(resources),
            with_validated_tool_overrides=session_tool_policy.with_validated_tool_overrides,
        ),
        require_admin=functools.partial(access_composition.require_admin, resources),
        require_approved_user=auth.require_approved_user,
        require_project_member=access.require_project_member,
        require_project_owner=access.require_project_owner,
        require_job_access=access.require_job_access,
    )


def knowledge_projection_dependencies(
    resources: ApplicationResources,
) -> knowledge_projection_operations.KnowledgeProjectionDependencies:
    """Ports for the connector-knowledge projection.

    ``store`` here is the **vector** pool: this projection writes only
    ``knowledge_index``, and the graph leg goes through ``graph``.
    """
    return knowledge_projection_operations.KnowledgeProjectionDependencies(
        store=resources.vector_db,
        logger=logger,
        graph=resources.knowledge_graph,
    )


def knowledge_dependencies(
    resources: ApplicationResources,
) -> knowledge_routes.KnowledgeDependencies:
    """Compose the project knowledge reads and commands."""
    return knowledge_routes.KnowledgeDependencies(
        store=resources.postgres_db,
        operations=knowledge_operations_module.KnowledgeOperationDependencies(
            store=resources.postgres_db,
            vector_db=resources.vector_db,
            gitea_client=resources.gitea_client,
            logger=logger,
            graph=resources.knowledge_graph,
            knowledge_index=knowledge_index_dependencies(resources),
        ),
        require_project_member=access.require_project_member,
        require_internal=access.require_internal,
    )


def citations_dependencies(
    resources: ApplicationResources,
) -> citations_routes.CitationsDependencies:
    """Compose the source, citation and memory reads."""
    return citations_routes.CitationsDependencies(
        store=resources.postgres_db,
        operations=citations_operations.CitationDependencies(
            store=resources.postgres_db,
            vector_db=resources.vector_db,
            snapshot_service=snapshot_service_module.snapshot_service,
            main_cloud_router=resources.main_cloud_router,
            logger=logger,
        ),
        require_approved_user=auth.require_approved_user,
        require_job_access=access.require_job_access,
        require_project_member=access.require_project_member,
        require_internal=access.require_internal,
        user_can_access_any_job=access.user_can_access_any_job,
        user_can_access_job_or_thread=access.user_can_access_job_or_thread,
    )


def media_dependencies(
    resources: ApplicationResources,
) -> media_routes.MediaDependencies:
    """The media proxy needs only the store its approved-user gate reads."""
    return media_routes.MediaDependencies(
        store=resources.postgres_db,
        require_approved_user=auth.require_approved_user,
    )
