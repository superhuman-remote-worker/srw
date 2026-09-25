"""Composition for job admission, reads and dispatch.

Job admission stages and the REST admission adapter (R1.B05/B06), job reads,
inspection, audit and artifacts (R1.B01), the benchmark runner's creation and
cancel operations, and the dispatch scheduler's dependencies (R1.B11).
"""

from __future__ import annotations

import functools
import logging
from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import HTTPException, Request

from orchestrator import uploads
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
from orchestrator.database.postgres import JOB_STATUS_FILTER_VALUES, KNOWN_JOB_ORIGINS
from orchestrator.routers import (
    job_artifacts as job_artifacts_routes,
    job_audit as job_audit_routes,
    job_inspection as job_inspection_routes,
    job_lifecycle as job_lifecycle_routes,
    job_reads as job_reads_routes,
)
from orchestrator.schemas.job_create import JobCreate
from orchestrator.security import access, auth
from orchestrator.services import (
    agent_provisioner as agent_provisioner_module,
    container_provisioner as container_provisioner_module,
    default_experts,
    deployment_gates,
    docker_provisioner as docker_provisioner_module,
    grant_enforcement,
    job_admission,
    job_artifacts as job_artifacts_operations,
    job_create_ingress,
    job_datasource_selection,
    job_dispatcher,
    job_evidence as job_evidence_operations,
    job_inspection as job_inspection_operations,
    job_projection,
    job_queries,
    job_reads,
    job_start_bundle,
    job_workspace_authority,
    job_workspace_runtime,
    project_loop_spawn as project_loop_spawn_service,
    session_tool_policy,
    subjob_completion as subjob_completion_operations,
    subjob_output as subjob_output_operations,
    thread_datasource_authorization as thread_datasource_authorization_service,
    thread_mount_rows,
    thread_project_authorization as thread_project_authorization_service,
    vm_provisioner as vm_provisioner_module,
    vm_workspace_policy,
    workspace_suspension,
    workspace_tier_policy,
)
from orchestrator.services.container_provisioner import (
    WORKSPACE_RUNTIME_INCARNATION_KEY,
)
from orchestrator.services.job_admission import JobAdmissionDependencies
from orchestrator.services.job_admission_config import JobAdmissionConfigDependencies
from orchestrator.services.job_admission_creation import (
    JobAdmissionCreationDependencies,
)
from orchestrator.services.job_admission_datasources import (
    JobAdmissionDatasourcesDependencies,
)
from orchestrator.services.job_admission_delivery import (
    JobAdmissionDeliveryDependencies,
)
from orchestrator.services.job_admission_officer import JobAdmissionOfficerDependencies
from orchestrator.services.job_admission_scope import (
    _INTERNAL_JOB_SCOPE_DENIED,
    JobAdmissionActor,
    JobAdmissionScopeDependencies,
)
from orchestrator.services.job_admission_workspace import (
    JobAdmissionWorkspaceDependencies,
)
from orchestrator.services.job_mutation_controls import JobCancelResponse

logger = logging.getLogger(__name__)


def job_dispatch_dependencies(
    resources: ApplicationResources,
) -> job_dispatcher.JobDispatchDependencies:
    """Bind dispatch scheduling to this application's collaborators.

    Rebuilt per trigger (and once per lifespan for the periodic loop), so the
    deployment flags and collaborators are the ones current at that moment;
    the dispatch state is the one application-owned object.
    """

    return job_dispatcher.JobDispatchDependencies(
        state=resources.job_dispatch_state,
        store=resources.postgres_db,
        completion_control_boundary=resources.completion_control_boundary,
        agent_provisioner=agent_provisioner_module.agent_provisioner,
        vm_provisioner=vm_provisioner_module.vm_provisioner,
        container_provisioner=container_provisioner_module.container_provisioner,
        docker_provisioner=docker_provisioner_module.docker_provisioner,
        workspace_suspension=workspace_suspension.workspace_suspension_service,
        auto_assign_enabled=resources.settings.auto_assign_enabled,
        stateless_worker_enabled=resources.settings.stateless_worker_enabled,
        manifest_execution_service=functools.partial(
            catalogue_composition.manifest_execution_service, resources
        ),
        prepare_job_workspace_runtime=bound(
            job_workspace_authority.prepare_job_workspace_runtime,
            preparation_composition.job_workspace_authority_dependencies,
            resources,
        ),
        fail_subjob_and_unblock_parent=bound(
            job_workspace_authority.fail_subjob_and_unblock_parent,
            preparation_composition.job_workspace_authority_dependencies,
            resources,
        ),
        check_vm_permission=bound(
            vm_workspace_policy.check_vm_permission,
            preparation_composition.vm_permission_dependencies,
            resources,
        ),
        fail_vm_parked_job=bound(
            job_workspace_runtime.fail_vm_parked_job,
            preparation_composition.job_workspace_runtime_dependencies,
            resources,
        ),
        job_needs_sandbox=bound(
            job_workspace_runtime.job_needs_sandbox,
            preparation_composition.job_workspace_runtime_dependencies,
            resources,
        ),
        provision_parent_workspace_for_scholar=bound(
            job_workspace_authority.provision_parent_workspace_for_scholar,
            preparation_composition.job_workspace_authority_dependencies,
            resources,
        ),
        prepare_job_repository_before_claim=bound(
            job_start_bundle.prepare_job_repository_before_claim,
            preparation_composition.job_start_bundle_dependencies,
            resources,
        ),
        job_delivery_operations=functools.partial(
            controls_composition.job_delivery_operations, resources
        ),
    )


def job_lifecycle_route_dependencies(
    resources: ApplicationResources,
) -> job_lifecycle_routes.JobLifecycleRouteDependencies:
    return job_lifecycle_routes.JobLifecycleRouteDependencies(
        store=resources.postgres_db,
        logger=logger,
        is_internal_call=access.is_internal_call,
        require_approved_user=auth.require_approved_user,
        require_project_member=access.require_project_member,
        require_internal=access.require_internal,
        strip_raw_officer_claim_context=job_create_ingress.strip_raw_officer_claim_context,
        strip_public_job_reserved_markers=job_create_ingress.strip_public_job_reserved_markers,
        admit_job=job_admission.admit_job,
        job_admission_dependencies=lambda request: job_admission_dependencies(
            resources, lambda: job_admission_scope_dependencies(resources, request)
        ),
        graft_subjob_output=lambda job_id: (
            subjob_output_operations.graft_subjob_output(
                job_id,
                dependencies=completion_composition.subjob_output_dependencies(
                    resources
                ),
            )
        ),
    )


def resolve_submitted_job_origin(
    *,
    context: dict[str, Any] | None,
    parent_job_id: Any,
    thread_id: Any,
) -> str:
    """Classify a job arriving through ``POST /api/jobs``.

    That endpoint is not "the user path". It is the shared funnel for human
    submissions, session launches, delegation/critic children forwarded over
    the internal key, and the job bench — all of which arrive with the same
    request shape, which is why origin has to be resolved here rather than
    assumed. (Officer admissions also pass through, but they branch earlier
    and are stamped by ``admit_and_create_job``.)

    Bench is recognised by ``context['bench']``, which
    ``services/bench.py::build_bench_job_payload`` already sets — otherwise
    benchmark traffic is byte-identical to a normal internal submission and
    would land in every user's job list and spend attribution.

    The order mirrors migration 0172's backfill so historic rows and new ones
    are classified the same way.
    """
    if context and "bench" in context:
        return "bench"
    if parent_job_id:
        return "subjob"
    if thread_id:
        return "session"
    return "user"


def job_inspection_dependencies(
    resources: ApplicationResources,
) -> job_inspection_routes.JobInspectionDependencies:
    return job_inspection_routes.JobInspectionDependencies(
        store=resources.postgres_db,
        inspections=job_inspection_operations.JobInspectionDependencies(
            store=resources.postgres_db,
            audit_reader=resources.audit_reader,
            user_visible_project_ids=access.user_visible_project_ids,
            mcp_scope_project_id=access.mcp_scope_project_id,
            active_job_statuses=job_inspection_operations.ME_ACTIVE_JOB_STATUSES,
        ),
        require_approved_user=auth.require_approved_user,
        require_job_access=access.require_job_access,
        require_thread_owner=access.require_thread_owner,
        require_internal=access.require_internal,
    )


def job_audit_dependencies(
    resources: ApplicationResources,
) -> job_audit_routes.JobAuditDependencies:
    return job_audit_routes.JobAuditDependencies(
        store=resources.postgres_db,
        audit_reader=resources.audit_reader,
        require_admin=functools.partial(access_composition.require_admin, resources),
        require_approved_user=auth.require_approved_user,
        require_job_access=access.require_job_access,
    )


def job_artifacts_dependencies(
    resources: ApplicationResources,
) -> job_artifacts_routes.JobArtifactDependencies:
    return job_artifacts_routes.JobArtifactDependencies(
        store=resources.postgres_db,
        artifacts=job_artifacts_operations.JobArtifactDependencies(
            store=resources.postgres_db,
            forge=resources.gitea_client,
            resolve_job_repo=(
                lambda job_id: subjob_output_operations.resolve_job_repo(
                    job_id,
                    dependencies=completion_composition.subjob_output_dependencies(
                        resources
                    ),
                )
            ),
            evidence=job_evidence_operations,
        ),
        require_job_access=access.require_job_access,
    )


def job_reads_dependencies(
    resources: ApplicationResources,
) -> job_reads_routes.JobReadsDependencies:
    """Compose read/auth ports without evaluating store methods before auth.

    Main's legacy direct callers patch these application collaborators. Resolve
    them per invocation; independently mounted routers supply their own factory.
    Store lifecycle, canonical filter vocabularies and cloud/workspace authority
    remain owned by this application.
    """
    store = resources.postgres_db
    audit = resources.audit_reader
    queries = job_queries.JobQueryDependencies(
        query_jobs=lambda **kwargs: store.query_jobs(**kwargs),
        get_job_statistics=lambda **kwargs: store.get_job_statistics(**kwargs),
        visible_project_ids=lambda actor: access.user_visible_project_ids(actor, store),
        scope_project_id=access.mcp_scope_project_id,
        audit_available=lambda: audit.is_available,
        audit_counts=lambda ids: audit.get_audit_counts(ids),
        project_job=lambda job: with_cloud_review_mode(
            resources, redact_job_config_override(job)
        ),
        now=lambda: datetime.now(timezone.utc),
        status_filter_values=JOB_STATUS_FILTER_VALUES,
        known_origins=KNOWN_JOB_ORIGINS,
    )
    reads = job_reads.JobReadDependencies(
        store=store,
        audit_reader=audit,
        redact_job=redact_job_config_override,
        with_cloud_review_mode=functools.partial(with_cloud_review_mode, resources),
        status_filter_values=JOB_STATUS_FILTER_VALUES,
    )
    return job_reads_routes.JobReadsDependencies(
        store=store,
        queries=queries,
        reads=reads,
        require_approved_user=auth.require_approved_user,
        require_job_access=access.require_job_access,
        require_project_member=access.require_project_member,
    )


def redact_job_config_override(job: dict[str, Any]) -> dict[str, Any]:
    """Compatibility projection for admission and other existing job callers."""
    return job_projection.redact_job_config_override(
        job,
        vm_mode=lambda: vm_provisioner_module.vm_provisioner.mode,
        runtime_incarnation_key=WORKSPACE_RUNTIME_INCARNATION_KEY,
        redact_config_override=access.redact_config_override,
    )


def resolve_exported_folder_url(
    resources: ApplicationResources, handle_str: str | None
) -> Optional[str]:
    """Resolve export URLs through the application-owned cloud router."""
    return job_projection.resolve_exported_folder_url(
        handle_str,
        resolve_backend=lambda backend_id: resources.main_cloud_router.for_backend(
            backend_id
        ),
    )


def with_cloud_review_mode(
    resources: ApplicationResources, job: dict[str, Any]
) -> dict[str, Any]:
    """Compatibility projection for existing job and admission callers."""
    return job_projection.with_cloud_review_mode(
        job,
        resolve_folder_url=functools.partial(resolve_exported_folder_url, resources),
    )


def job_admission_scope_dependencies(
    resources: ApplicationResources,
    request: Request,
) -> JobAdmissionScopeDependencies:
    """Bind the existing application collaborators without moving their lifecycle."""
    db = resources.postgres_db
    authenticate = auth.require_approved_user

    async def authenticate_forwarded_user() -> tuple[dict[str, Any], str | None]:
        principal = await authenticate(request, db)
        scoped_project = access.mcp_scope_project_id(principal)
        return principal, str(scoped_project) if scoped_project is not None else None

    return JobAdmissionScopeDependencies(
        store=db,
        thread_project_ids=bound(
            thread_mount_rows.thread_project_ids,
            preparation_composition.thread_mount_dependencies,
            resources,
        ),
        revalidate_thread_project_ids=bound(
            thread_project_authorization_service.revalidate_thread_project_ids,
            sessions_composition.thread_project_authorization_dependencies,
            resources,
        ),
        authenticate_forwarded_user=authenticate_forwarded_user,
        authorize_upload_reference=uploads.authorize_upload_reference,
    )


def bundled_job_expert_exists(
    resources: ApplicationResources, config_name: str
) -> bool:
    """Read the application-owned catalogue only when an explicit slug needs it."""
    catalog = catalogue_composition.expert_catalog_service(resources)
    if catalog.state.experts is None:
        catalog.state.experts = catalog.scan_experts()
    return any(e.id == config_name for e in catalog.state.experts)


def job_admission_config_dependencies(
    resources: ApplicationResources,
) -> JobAdmissionConfigDependencies:
    """Bind current collaborators; gates and the catalogue remain deferred."""
    from functools import partial

    from orchestrator.services.job_admission_work_expert import (
        preview_expert_refusals,
    )

    return JobAdmissionConfigDependencies(
        store=resources.postgres_db,
        require_project_access=functools.partial(require_job_project_access, resources),
        bundled_expert_exists=functools.partial(bundled_job_expert_exists, resources),
        experts_db_enabled=deployment_gates.is_experts_db_enabled,
        user_experts_enabled=bound(
            grant_enforcement.user_experts_enabled,
            preparation_composition.grant_enforcement_dependencies,
            resources,
        ),
        resolve_worker_expert=partial(
            default_experts.resolve_root_expert,
            resources.postgres_db,
            expert_type="worker",
        ),
        preview_expert_refusals=partial(preview_expert_refusals, resources.postgres_db),
    )


def job_admission_officer_dependencies(
    resources: ApplicationResources,
) -> JobAdmissionOfficerDependencies:
    """Capture application stores; defer ticket imports and reads until needed."""
    from functools import partial

    from orchestrator.services.officer_admission import prepare_officer_admission

    db = resources.postgres_db
    ticket_db = resources.vector_db

    async def fetch_ticket(project_id: str, note_id: str) -> dict[str, Any] | None:
        from orchestrator.services.project_backlog import fetch_ticket_state

        return await fetch_ticket_state(ticket_db, project_id, note_id)

    return JobAdmissionOfficerDependencies(
        store=db,
        prepare_officer=partial(prepare_officer_admission, db),
        fetch_ticket=fetch_ticket,
    )


def job_admission_workspace_dependencies(
    resources: ApplicationResources,
) -> JobAdmissionWorkspaceDependencies:
    """Bind existing policy owners without evaluating flags or capabilities."""
    return JobAdmissionWorkspaceDependencies(
        store=resources.postgres_db,
        needs_vm=job_workspace_runtime.job_needs_vm,
        needs_sandbox=bound(
            job_workspace_runtime.job_needs_sandbox,
            preparation_composition.job_workspace_runtime_dependencies,
            resources,
        ),
        check_vm_permission=bound(
            vm_workspace_policy.check_vm_permission,
            preparation_composition.vm_permission_dependencies,
            resources,
        ),
        resolve_execution_lane=bound(
            job_workspace_runtime.resolve_requested_job_execution_lane,
            preparation_composition.job_workspace_runtime_dependencies,
            resources,
        ),
        stateless_default_enabled=lambda: resources.settings.stateless_worker_default_enabled,
        stateless_enabled=lambda: resources.settings.stateless_worker_enabled,
        vm_workspaces_on_pod_network=access.vm_workspaces_on_pod_network,
        provisioner=container_provisioner_module.container_provisioner,
        enforce_grants=bound(
            grant_enforcement.enforce_job_create_grants,
            preparation_composition.grant_enforcement_dependencies,
            resources,
        ),
    )


def job_admission_datasources_dependencies(
    resources: ApplicationResources,
) -> JobAdmissionDatasourcesDependencies:
    """Bind current connector authorities without evaluating selection defaults."""
    from functools import partial

    from orchestrator.services.datasource_policy import default_datasource_selection

    return JobAdmissionDatasourcesDependencies(
        backend_from_override=workspace_tier_policy.backend_from_override,
        inherit_parent_ids=bound(
            job_datasource_selection.inherit_parent_datasource_ids,
            preparation_composition.job_datasource_selection_dependencies,
            resources,
        ),
        filter_implicit_lite_ids=bound(
            job_datasource_selection.filter_implicit_lite_datasource_ids,
            preparation_composition.job_datasource_selection_dependencies,
            resources,
        ),
        authorize_selection=bound(
            thread_datasource_authorization_service.authorize_thread_datasource_selection,
            sessions_composition.thread_datasource_authorization_dependencies,
            resources,
        ),
        default_selection=partial(default_datasource_selection, resources.postgres_db),
        defaults_on_omission=deployment_gates.datasource_defaults_on_omission,
        selection_provenance=job_datasource_selection.datasource_selection_provenance,
    )


def job_admission_delivery_dependencies(
    resources: ApplicationResources,
) -> JobAdmissionDeliveryDependencies:
    """Bind the existing delivery-refusal transaction to the current store."""
    from functools import partial

    from orchestrator.services.officer_admission import (
        record_rejected_ticket_delivery_requirement,
    )

    db = resources.postgres_db
    return JobAdmissionDeliveryDependencies(
        store=db,
        record_rejected_ticket_delivery_requirement=partial(
            record_rejected_ticket_delivery_requirement, db
        ),
    )


def job_admission_creation_dependencies(
    resources: ApplicationResources,
) -> JobAdmissionCreationDependencies:
    """Bind creation owners; defer provisioning imports until their operation."""
    db, forge, cloud = (
        resources.postgres_db,
        resources.gitea_client,
        resources.main_cloud_router,
    )

    async def admit_officer(**kwargs):
        from orchestrator.services.officer_admission import admit_and_create_job

        return await admit_and_create_job(db, **kwargs)

    async def activate_officer(job_row, **kwargs):
        from orchestrator.services.officer_preflight import ensure_officer_job_activated

        return await ensure_officer_job_activated(db, job_row, **kwargs)

    async def provision_repo(*, job_row):
        from orchestrator.services.job_provisioning import provision_job_repo

        return await provision_job_repo(
            job_row=job_row,
            gitea_client=forge,
            postgres_db=db,
            main_cloud_router=cloud,
        )

    return JobAdmissionCreationDependencies(
        store=db,
        admit_officer=admit_officer,
        activate_officer=activate_officer,
        provision_officer=bound(
            project_loop_spawn_service.provision_officer_ticket_repo,
            workflows_composition.project_loop_dependencies,
            resources,
        ),
        provision_repo=provision_repo,
        spawn_scholar=(
            lambda *args, **kwargs: subjob_completion_operations.spawn_scholar_subjob(
                *args,
                **kwargs,
                dependencies=completion_composition.scholar_completion_dependencies(
                    resources
                ),
            )
        ),
        resolve_origin=resolve_submitted_job_origin,
        trigger_dispatch=bound(
            job_dispatcher.trigger_dispatch, job_dispatch_dependencies, resources
        ),
    )


def job_admission_dependencies(
    resources: ApplicationResources, scope_factory
) -> JobAdmissionDependencies:
    """Construct the shared operation without eagerly binding later stages."""
    return JobAdmissionDependencies(
        validate_tool_overrides=session_tool_policy.with_validated_tool_overrides,
        enforce_readiness=functools.partial(
            access_composition.enforce_readiness_gate, resources
        ),
        scope=scope_factory,
        config=functools.partial(job_admission_config_dependencies, resources),
        officer=functools.partial(job_admission_officer_dependencies, resources),
        workspace=functools.partial(job_admission_workspace_dependencies, resources),
        datasources=functools.partial(
            job_admission_datasources_dependencies, resources
        ),
        delivery=functools.partial(job_admission_delivery_dependencies, resources),
        creation=functools.partial(job_admission_creation_dependencies, resources),
        redact_result=redact_job_config_override,
    )


async def create_bench_job(
    resources: ApplicationResources, creator_id: str, command: JobCreate
) -> dict[str, Any]:
    """Compose trusted in-process admission with deferred creator revalidation."""
    from functools import partial

    from orchestrator.services.job_admission_creator import authenticate_job_creator

    job_create_ingress.strip_raw_officer_claim_context(command)

    def scope_factory() -> JobAdmissionScopeDependencies:
        db = resources.postgres_db
        return JobAdmissionScopeDependencies(
            store=db,
            thread_project_ids=bound(
                thread_mount_rows.thread_project_ids,
                preparation_composition.thread_mount_dependencies,
                resources,
            ),
            revalidate_thread_project_ids=bound(
                thread_project_authorization_service.revalidate_thread_project_ids,
                sessions_composition.thread_project_authorization_dependencies,
                resources,
            ),
            authenticate_forwarded_user=partial(
                authenticate_job_creator, creator_id, db
            ),
            authorize_upload_reference=uploads.authorize_upload_reference,
        )

    return await job_admission.admit_job(
        command=command,
        actor=JobAdmissionActor(forwarded_user_id=creator_id),
        origin="internal_rest",
        dependencies=job_admission_dependencies(resources, scope_factory),
    )


async def cancel_bench_job(
    resources: ApplicationResources, job_id: str, caller: dict[str, Any]
) -> JobCancelResponse:
    """Revalidate one run member, then invoke the application control operation."""

    job = await resources.postgres_db.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")
    if not await access.user_can_access_job(caller, resources.postgres_db, job_id):
        raise HTTPException(status_code=403, detail="Not authorized to access this job")
    return await controls_composition.job_mutation_operations(resources).cancel(
        job_id, job=job
    )


def bench_dependencies(resources: ApplicationResources):
    """Bind the benchmark task to this application's creation operation."""
    from orchestrator.routers.bench import BenchDependencies
    from orchestrator.services.bench import BenchStore

    return BenchDependencies(
        store=BenchStore(resources.postgres_db),
        create_job=functools.partial(create_bench_job, resources),
        validate_tool_overrides=session_tool_policy.with_validated_tool_overrides,
        audit_reader=resources.audit_reader,
        forge=resources.gitea_client,
        resolve_job_repo=(
            lambda job_id: subjob_output_operations.resolve_job_repo(
                job_id,
                dependencies=subjob_output_operations.SubjobOutputDependencies(
                    store=resources.postgres_db,
                    forge=resources.gitea_client,
                ),
            )
        ),
        cancel_job=functools.partial(cancel_bench_job, resources),
    )


async def require_job_project_access(
    resources: ApplicationResources,
    principal: dict[str, Any] | None,
    project_id: str | None,
    *,
    denial_detail: str = _INTERNAL_JOB_SCOPE_DENIED,
) -> None:
    """Require current editor access for a user-bound project job."""
    if principal is None or project_id is None or principal.get("is_admin"):
        return
    scoped_project = access.mcp_scope_project_id(principal)
    if scoped_project is not None and str(scoped_project) != str(project_id):
        raise HTTPException(status_code=403, detail=denial_detail)
    role = await resources.postgres_db.get_user_role_in_project(
        str(project_id), str(principal["id"])
    )
    if role not in {"editor", "owner"}:
        raise HTTPException(status_code=403, detail=denial_detail)
