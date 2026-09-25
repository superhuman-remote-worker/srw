"""Composition for catalogues and manifests (R1.B01).

Expert/skill catalogue service, provider, model and configuration catalogues,
manifest routes and manifest execution, and the configuration directory the
bundled catalogue reads.
"""

from __future__ import annotations

import functools
import logging
import os
from pathlib import Path
from typing import Any

from fastapi import HTTPException, Request

import orchestrator
from orchestrator.application import (
    access as access_composition,
    controls as controls_composition,
    jobs as jobs_composition,
    preparation as preparation_composition,
    sessions as sessions_composition,
    workflows as workflows_composition,
)
from orchestrator.application.resources import ApplicationResources, bound
from orchestrator.routers import (
    config_catalog as config_catalog_routes,
    expert_catalog as expert_catalog_routes,
    manifests as manifest_routes,
    model_catalog as model_catalog_routes,
    provider_catalog as provider_catalog_routes,
)
from orchestrator.security import access, auth
from orchestrator.services import (
    agent_provisioner as agent_provisioner_module,
    catalogue_resources,
    config_overrides,
    container_provisioner as container_provisioner_module,
    deployment_gates,
    discovery as discovery_service,
    family_matcher,
    grant_enforcement,
    job_dispatcher,
    llm_endpoint_probe,
    officer_post_policy as officer_post_policy_service,
    session_config_resolution,
    session_tool_policy,
    subscription_discovery,
    thread_datasource_authorization as thread_datasource_authorization_service,
    vm_provisioner as vm_provisioner_module,
)
from orchestrator.services.config_catalog import ConfigCatalogService
from orchestrator.services.expert_authoring import ExpertAuthoringService
from orchestrator.services.expert_catalog import ExpertCatalogService
from orchestrator.services.expert_catalog_contracts import (
    ExpertCatalogDependencies,
    ExpertWritePolicy,
)
from orchestrator.services.model_catalog import ModelCatalogService
from orchestrator.services.provider_catalog import ProviderCatalogService

logger = logging.getLogger(__name__)


def get_config_dir() -> Path:
    """The bundled configuration directory (``CONFIG_DIR``, source tree, image).

    Resolved from the ``orchestrator`` package anchor, which sits at the same
    depth as the former ``orchestrator/main.py`` caller, so the source-tree
    candidate is still the repository's ``config/``.
    """
    return catalogue_resources.resolve_config_dir(orchestrator.__file__)


def provider_catalog_dependencies(
    resources: ApplicationResources,
) -> provider_catalog_routes.ProviderCatalogDependencies:
    return provider_catalog_routes.ProviderCatalogDependencies(
        service=ProviderCatalogService(
            store=resources.postgres_db,
            discovery=discovery_service,
            probe=llm_endpoint_probe.probe_endpoint_models,
            subscriptions=subscription_discovery,
        ),
        require_admin=functools.partial(access_composition.require_admin, resources),
    )


def model_catalog_dependencies(
    resources: ApplicationResources,
) -> model_catalog_routes.ModelCatalogDependencies:
    from shared.runtime.core import loader

    catalogue = resources.catalogue_resources

    async def approved_user(request: Request) -> dict[str, Any]:
        return await auth.require_approved_user(request, resources.postgres_db)

    return model_catalog_routes.ModelCatalogDependencies(
        service=ModelCatalogService(
            store=resources.postgres_db,
            probe=llm_endpoint_probe.probe_endpoint_models,
            get_config_dir=catalogue.get_config_dir,
            load_settings_matrix=catalogue.load_settings_matrix,
            settings_for_family=loader.bundled_settings_for_family,
            family_detector=family_matcher.detect_family,
            reasoning_capability=loader.reasoning_capability,
        ),
        require_admin=functools.partial(access_composition.require_admin, resources),
        require_approved_user=approved_user,
    )


def config_catalog_dependencies(
    resources: ApplicationResources,
) -> config_catalog_routes.ConfigCatalogDependencies:
    from shared.runtime.core import loader

    return config_catalog_routes.ConfigCatalogDependencies(
        service=ConfigCatalogService(
            store=resources.postgres_db,
            # Preserve the original catalogue's loader project-root path;
            # do not substitute _get_config_dir if its override differs.
            project_root=loader.get_project_root,
            settings_for_family=loader.bundled_settings_for_family,
            guardrails_for_family=loader.bundled_guardrails_for_family,
            prompt_resolver=loader.PromptMatrixResolver,
            instruction_resolver=loader.InstructionMatrixResolver,
        ),
        require_admin=functools.partial(access_composition.require_admin, resources),
    )


def manifest_dependencies(
    resources: ApplicationResources,
) -> manifest_routes.ManifestDependencies:
    from orchestrator.services.manifest_resources import ManifestResourceService
    from orchestrator.services.manifests import ManifestService

    async def approved_user(request: Request) -> dict[str, Any]:
        return await auth.require_approved_user(request, resources.postgres_db)

    async def admit_manifest(prepared, resource, user, *, request=None):
        return await manifest_execution_service(resources).admit(
            prepared, resource, user, request=request
        )

    async def activate_project(prepared, user, *, request=None, validate_only):
        from orchestrator.services.manifest_projects import validate_project_activation

        return await validate_project_activation(
            resources.postgres_db,
            prepared,
            user,
            request=request,
            validate_only=validate_only,
            validate_post_patch=officer_post_policy_service.validated_officer_post_patch,
            enforce_auto_pull=bound(
                officer_post_policy_service.enforce_officer_auto_pull_release,
                workflows_composition.officer_post_policy_dependencies,
                resources,
            ),
        )

    return manifest_routes.ManifestDependencies(
        service=ManifestService(),
        require_approved_user=approved_user,
        resources=ManifestResourceService(
            resources.postgres_db,
            admit_job=admit_manifest,
            project_activation=activate_project,
        ),
        execution=functools.partial(manifest_execution_service, resources),
        trigger_dispatch=bound(
            job_dispatcher.trigger_dispatch,
            jobs_composition.job_dispatch_dependencies,
            resources,
        ),
    )


def manifest_execution_service(resources: ApplicationResources):
    from kubernetes.client import NetworkingV1Api

    from orchestrator.services.generic_harness_runtime import GenericHarnessRuntime
    from orchestrator.services.manifest_execution import ManifestExecutionService
    from orchestrator.services.manifest_workspace_runtime import (
        ManifestWorkspaceRuntime,
    )
    from orchestrator.services.manifest_workspaces import ManifestWorkspaceService

    if not agent_provisioner_module.agent_provisioner._k8s_available:
        raise HTTPException(503, "Kubernetes manifest hosting is unavailable.")
    network_api = NetworkingV1Api()
    namespace = os.environ.get(
        "MANIFEST_NAMESPACE",
        agent_provisioner_module.agent_provisioner._namespace + "-native",
    )
    workspace_namespace = namespace
    workspaces = ManifestWorkspaceService(
        resources.postgres_db,
        ManifestWorkspaceRuntime(
            container_provisioner_module.container_provisioner._core_api,
            network_api,
            namespace=workspace_namespace,
        ),
        namespace=workspace_namespace,
        default_image=container_provisioner_module.container_provisioner._workspace_image,
        storage_class_name=container_provisioner_module.container_provisioner._storage_class,
        harness_namespace=namespace,
        vm_provisioner=vm_provisioner_module.vm_provisioner,
    )
    return ManifestExecutionService(
        resources.postgres_db,
        runtime=GenericHarnessRuntime(
            agent_provisioner_module.agent_provisioner._core_api,
            network_api,
            namespace=namespace,
        ),
        namespace=namespace,
        workspace=workspaces,
        srw_image=agent_provisioner_module.agent_provisioner._agent_image,
        authorize_datasources=bound(
            thread_datasource_authorization_service.authorize_thread_datasource_selection,
            sessions_composition.thread_datasource_authorization_dependencies,
            resources,
        ),
        cancel_srw=lambda job, **guard: controls_composition.job_mutation_operations(
            resources
        ).cancel(str(job["id"]), job=job, **guard),
        native_hosting_enabled=os.environ.get(
            "MANIFEST_NETWORK_ISOLATION_VERIFIED", "false"
        ).lower()
        == "true",
        harness_egress=os.environ.get("MANIFEST_HARNESS_EGRESS", "[]"),
    )


def expert_catalog_service(resources: ApplicationResources) -> ExpertCatalogService:
    """Bind current stores/policy to this application's shared catalogue state."""
    catalogue = resources.catalogue_resources
    from orchestrator.services.manifest_store import ManifestStore

    return ExpertCatalogService(
        ExpertCatalogDependencies(
            store=resources.postgres_db,
            manifests=ManifestStore(resources.postgres_db)
            if getattr(resources.postgres_db, "manifests_ready", False) is True
            else None,
            state=resources.expert_catalog_state,
            get_config_dir=catalogue.get_config_dir,
            load_settings_matrix=catalogue.load_settings_matrix,
            experts_enabled=deployment_gates.is_experts_db_enabled,
            skills_enabled=deployment_gates.is_skills_db_enabled,
            account_defaults_layer=bound(
                session_config_resolution.account_defaults_layer,
                preparation_composition.session_config_dependencies,
                resources,
            ),
            visible_project_ids=access.user_visible_project_ids,
            with_validated_tool_overrides=session_tool_policy.with_validated_tool_overrides,
            looks_like_uuid=config_overrides.looks_like_uuid,
            forge=resources.gitea_client,
        )
    )


def expert_catalog_dependencies(
    resources: ApplicationResources,
) -> expert_catalog_routes.ExpertCatalogRouteDependencies:
    """Compose catalogue HTTP guards and request-bound canonical save policy."""
    from functools import partial

    catalog = expert_catalog_service(resources)
    authoring = ExpertAuthoringService(
        store=resources.postgres_db,
        catalog=catalog,
        resolve_default_models=bound(
            session_config_resolution.resolve_default_models,
            preparation_composition.session_config_dependencies,
            resources,
        ),
        prefetch_roster_refs=bound(
            session_config_resolution.prefetch_roster_refs,
            preparation_composition.session_config_dependencies,
            resources,
        ),
    )

    async def approved_user(request: Request) -> dict[str, Any]:
        return await auth.require_approved_user(request, resources.postgres_db)

    async def project_member(
        request: Request, project_id: str, *, allow_archived: bool = True
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        return await access.require_project_member(
            request, resources.postgres_db, project_id, allow_archived=allow_archived
        )

    async def project_owner(
        request: Request, project_id: str, *, allow_archived: bool = True
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        return await access.require_project_owner(
            request, resources.postgres_db, project_id, allow_archived=allow_archived
        )

    return expert_catalog_routes.ExpertCatalogRouteDependencies(
        catalog=catalog,
        authoring=authoring,
        require_approved_user=approved_user,
        require_admin=functools.partial(access_composition.require_admin, resources),
        require_project_member=project_member,
        require_project_owner=project_owner,
        write_policy_factory=lambda request: ExpertWritePolicy(
            enforce_save=partial(
                bound(
                    grant_enforcement.enforce_expert_save,
                    preparation_composition.grant_enforcement_dependencies,
                    resources,
                ),
                request,
            ),
            enforce_save_prelude=partial(
                bound(
                    grant_enforcement.enforce_expert_save_prelude,
                    preparation_composition.grant_enforcement_dependencies,
                    resources,
                ),
                request,
            ),
            strip_save_grants=bound(
                grant_enforcement.strip_save_grants,
                preparation_composition.grant_enforcement_dependencies,
                resources,
            ),
        ),
    )
