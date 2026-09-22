"""Shared job-admission operation for authenticated application callers.

Transport adapters authenticate and sanitize reserved public/internal markers.
This operation owns the ordered preparation and creation stages. Factories bind
each stage only when reached; the application retains policy helpers, stores,
credentials, provisioning and lifecycle. HTTP errors and row redaction remain
the compatibility contract for existing callers.
"""

from dataclasses import dataclass
import logging
from typing import Any, Awaitable, Callable

from fastapi import HTTPException

from orchestrator.schemas.job_create import JobCreate
from orchestrator.services.datasource_policy_errors import (
    DatasourceMaterializationAuthorizationError,
    DatasourcePolicyConflictError,
)
from orchestrator.services.job_admission_scope import (
    JobAdmissionActor,
    JobAdmissionOrigin,
    JobAdmissionScopeDependencies,
    prepare_job_admission_scope,
)
from orchestrator.services.job_admission_config import (
    JobAdmissionConfigDependencies,
    apply_work_expert_default,
    prepare_job_admission_config,
)
from orchestrator.services.job_admission_officer import (
    JobAdmissionOfficerDependencies,
    prepare_job_admission_officer,
)
from orchestrator.services.job_admission_workspace import (
    JobAdmissionWorkspaceDependencies,
    prepare_job_admission_workspace,
)
from orchestrator.services.job_admission_datasources import (
    JobAdmissionDatasourcesDependencies,
    prepare_job_admission_datasources,
)
from orchestrator.services.job_admission_delivery import (
    JobAdmissionDeliveryDependencies,
    prepare_job_admission_delivery,
)
from orchestrator.services.job_admission_creation import (
    JobAdmissionCreationDependencies,
    JobAdmissionCreationInputs,
    create_admitted_job,
)


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class JobAdmissionDependencies:
    validate_tool_overrides: Callable[[dict[str, Any] | None], dict[str, Any] | None]
    enforce_readiness: Callable[[], Awaitable[None]]
    scope: Callable[[], JobAdmissionScopeDependencies]
    config: Callable[[], JobAdmissionConfigDependencies]
    officer: Callable[[], JobAdmissionOfficerDependencies]
    workspace: Callable[[], JobAdmissionWorkspaceDependencies]
    datasources: Callable[[], JobAdmissionDatasourcesDependencies]
    delivery: Callable[[], JobAdmissionDeliveryDependencies]
    creation: Callable[[], JobAdmissionCreationDependencies]
    redact_result: Callable[[dict[str, Any]], dict[str, Any]]


async def admit_job(
    *,
    command: JobCreate,
    actor: JobAdmissionActor,
    origin: JobAdmissionOrigin,
    dependencies: JobAdmissionDependencies,
) -> dict[str, Any]:
    """Create admitted work from an authenticated, marker-sanitized command."""
    # Validate both HTTP and trusted in-process calls before readiness/scope.
    # Readiness and tool errors stay outside the historical 500 translation.
    command.config_override = dependencies.validate_tool_overrides(
        command.config_override
    )
    await dependencies.enforce_readiness()
    try:
        scope = await prepare_job_admission_scope(
            command=command,
            actor=actor,
            origin=origin,
            dependencies=dependencies.scope(),
        )
        config_dependencies = dependencies.config()
        config = await prepare_job_admission_config(
            command=command,
            scope=scope,
            origin=origin,
            dependencies=config_dependencies,
        )
        officer = await prepare_job_admission_officer(
            command=command,
            config=config,
            dependencies=dependencies.officer(),
        )
        if config.expert_is_fallback:
            # Needs the slot's category and the ticket's pin, which only the
            # Officer stage knows.
            config = apply_work_expert_default(
                config,
                context=officer.context,
                ticket_expert=officer.ticket_expert,
                requested_category=command.work_category,
                slot_category=(
                    officer.preparation.category
                    if officer.preparation is not None
                    else None
                ),
                bundled_expert_exists=config_dependencies.bundled_expert_exists,
            )
        workspace_selection = config.workspace_selection
        if workspace_selection and workspace_selection.get("project_revision"):
            from orchestrator.services.manifest_workspace_selection import (
                srw_workspace_config,
            )
            from shared.workspace_contract import configured_workspace_backend

            if (
                configured_workspace_backend(officer.config_override)
                != srw_workspace_config(workspace_selection["resolved"])["backend"]
            ):
                workspace_selection = None
        lane = await prepare_job_admission_workspace(
            context=officer.context,
            config_override=officer.config_override,
            effective_user_id=scope.user_id,
            project_id=config.project_id,
            requested_lane=command.execution_lane,
            root_creation=config.root_creation,
            dependencies=dependencies.workspace(),
        )
        datasources = await prepare_job_admission_datasources(
            command=command,
            config_override=officer.config_override,
            selection_actor=scope.principal
            if origin == "internal_rest"
            else actor.principal,
            effective_user_id=scope.user_id,
            project_id=config.project_id,
            internal_call=origin == "internal_rest",
            internal_origin_bound=scope.origin_bound,
            dependencies=dependencies.datasources(),
        )
        delivery = await prepare_job_admission_delivery(
            command=command,
            context=officer.context,
            datasource_ids=datasources.datasource_ids,
            target_project_ids=datasources.target_project_ids,
            officer_preparation=officer.preparation,
            ticket_ready_at=officer.ticket_ready_at,
            dependencies=dependencies.delivery(),
        )
        result = await create_admitted_job(
            command=command,
            inputs=JobAdmissionCreationInputs(
                context=officer.context,
                config_name=config.config_name,
                expert_id=config.expert_id,
                config_override=officer.config_override,
                requested_workspace_backend=config.requested_workspace_backend,
                workspace_selection=workspace_selection,
                root_creation=config.root_creation,
                effective_user_id=scope.user_id,
                project_id=config.project_id,
                datasource_ids=datasources.datasource_ids,
                policy_revisions=datasources.policy_revisions,
                provenance=datasources.provenance,
                target_project_ids=datasources.target_project_ids,
                execution_lane=lane,
                delivery_contract=delivery,
                officer_preparation=officer.preparation,
                ticket_ready_at=officer.ticket_ready_at,
            ),
            dependencies=dependencies.creation(),
        )
        return dependencies.redact_result(result)
    except DatasourceMaterializationAuthorizationError as exc:
        raise HTTPException(
            status_code=403,
            detail="Work owner is no longer authorized",
        ) from exc
    except DatasourcePolicyConflictError as exc:
        raise HTTPException(
            status_code=409,
            detail="Connector policy changed while creating work; retry the request",
        ) from exc
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Failed to create job: %s", e)
        raise HTTPException(status_code=500, detail=str(e)) from e
