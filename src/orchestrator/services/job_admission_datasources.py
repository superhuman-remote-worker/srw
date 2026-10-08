"""Prepare one authorized datasource selection and its audit provenance.

The caller supplies the resolved principal and work scope. Existing connector
policy and inheritance authorities remain bound operations; this stage controls
only their admission order and explicit/inherited/default choice. It owns no
store, credentials, transaction, or application lifecycle.
"""

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Protocol
from uuid import UUID

from fastapi import HTTPException

if TYPE_CHECKING:
    from orchestrator.schemas.job_create import JobCreate


class InheritParentDatasourceIds(Protocol):
    async def __call__(
        self, *, thread_id: str | None, parent_job_id: str | None
    ) -> list[str]: ...


class AuthorizeDatasourceSelection(Protocol):
    async def __call__(
        self,
        actor: dict[str, Any] | None,
        datasource_ids: list[str],
        *,
        workspace_backend: str | None,
        target_project_ids: list[str],
        effective_work_owner_id: str | None,
        trusted_system_inheritance: bool,
        legacy_job_id: str | None,
    ) -> tuple[list[str], dict[str, int]]: ...


class ResolveConnectorRefs(Protocol):
    async def __call__(
        self,
        execution: Any,
        *,
        owner_id: str | None,
        project_id: str | None,
    ) -> list[str]: ...


class DatasourceSelectionProvenance(Protocol):
    async def __call__(
        self,
        *,
        datasource_ids: list[str],
        policy_revisions: dict[str, int],
        origin: str,
        effective_work_owner_id: str | None,
        actor: dict[str, Any] | None,
        project_ids: list[str],
        creation_path: str,
    ) -> dict[str, Any]: ...


@dataclass(frozen=True)
class JobAdmissionDatasourcesDependencies:
    backend_from_override: Callable[[dict[str, Any] | None], str | None]
    inherit_parent_ids: InheritParentDatasourceIds
    filter_implicit_lite_ids: Callable[[list[str], str | None], Awaitable[list[str]]]
    authorize_selection: AuthorizeDatasourceSelection
    default_selection: Callable[
        [str, list[str], str | None], Awaitable[tuple[list[str], dict[str, int]]]
    ]
    defaults_on_omission: Callable[[], bool]
    selection_provenance: DatasourceSelectionProvenance
    # ``execution.connectors`` refs to connector ids (services.connector_refs).
    resolve_connector_refs: ResolveConnectorRefs | None = None


@dataclass(frozen=True)
class JobAdmissionDatasources:
    """Exact selection and policy snapshot for later delivery and persistence."""

    datasource_ids: list[str]
    policy_revisions: dict[str, int]
    provenance: dict[str, Any]
    target_project_ids: list[str]


async def prepare_job_admission_datasources(
    *,
    command: "JobCreate",
    config_override: dict[str, Any] | None,
    selection_actor: dict[str, Any] | None,
    effective_user_id: str | None,
    project_id: str | None,
    internal_call: bool,
    internal_origin_bound: bool,
    dependencies: JobAdmissionDatasourcesDependencies,
) -> JobAdmissionDatasources:
    job = command
    # Resolve one complete attachment set before persistence. Presence —
    # not truthiness — distinguishes an explicit [] from omission.
    target_project_ids = [project_id] if project_id else []
    lite_backend = dependencies.backend_from_override(config_override)
    selection_was_supplied = "datasource_ids" in job.model_fields_set
    requested_datasource_ids = job.datasource_ids or []
    if job.execution is not None:
        # Connector refs are an explicit selection of the ids they name: the
        # same branch, the same authorization, the same snapshot.
        if dependencies.resolve_connector_refs is None:
            raise HTTPException(503, "Connector references are unavailable.")
        requested_datasource_ids = await dependencies.resolve_connector_refs(
            job.execution,
            owner_id=str(effective_user_id) if effective_user_id else None,
            project_id=project_id,
        )
        selection_was_supplied = True
    trusted_system_origin = bool(
        internal_call and internal_origin_bound and selection_actor is None
    )
    # Inheritance is for DELEGATION; project defaults are for DISPATCH.
    #
    # A parented subjob (critic, curator, pre-job scholar, a legacy delegation child)
    # must never exceed its parent's connectors, so ``parent_job_id`` keeps
    # inheriting unconditionally. But a thread that *commissions* fresh
    # project work — an officer, a session — is not delegating its own
    # charge, and it only landed in the inheritance branch because it
    # happens to be a thread. That mis-classification made
    # ``use_datasource_defaults`` unreachable for every thread-originated
    # job: the branch below it was never evaluated, so the flag the client
    # already sends on omission (orch_surface/client.py) was silently
    # dropped.
    #
    # Cost of that, found live on Better Resavio 2026-08-15: the officer's
    # own thread had been created without a selection, which persists as
    # ``datasource_ids: []`` (origin ``omitted_compat``). Every job he
    # commissioned faithfully inherited the empty list, so his workers got
    # no KurortEngine checkout, could not clone/commit/push, and he
    # correctly refused to dispatch further against a candidate that could
    # never be produced — a full night idle on one absent field.
    wants_dispatch_defaults = bool(
        effective_user_id and job.use_datasource_defaults and not job.parent_job_id
    )

    if selection_was_supplied:
        selection_origin = "explicit"
        trusted_explicit_reuse = False
        if trusted_system_origin and effective_user_id is None:
            # An ownerless internal caller has no ambient connector
            # authority. It may narrow an authoritative thread/parent
            # selection, but it cannot turn the trusted-inheritance seam
            # into an arbitrary UUID capability.
            inherited_ids = await dependencies.inherit_parent_ids(
                thread_id=job.thread_id,
                parent_job_id=job.parent_job_id,
            )
            try:
                requested_set = {
                    str(UUID(str(value))) for value in requested_datasource_ids
                }
                inherited_set = {str(UUID(str(value))) for value in inherited_ids}
            except (TypeError, ValueError) as exc:
                raise HTTPException(
                    status_code=403,
                    detail="One or more selected connectors are unavailable",
                ) from exc
            if not requested_set.issubset(inherited_set):
                raise HTTPException(
                    status_code=403,
                    detail="One or more selected connectors are unavailable",
                )
            trusted_explicit_reuse = True
        (
            selected_ds_ids,
            selected_ds_revisions,
        ) = await dependencies.authorize_selection(
            selection_actor,
            requested_datasource_ids,
            workspace_backend=lite_backend,
            target_project_ids=target_project_ids,
            effective_work_owner_id=(
                str(effective_user_id) if effective_user_id else None
            ),
            trusted_system_inheritance=trusted_explicit_reuse,
            legacy_job_id=str(job.parent_job_id) if job.parent_job_id else None,
        )
    elif (job.thread_id or job.parent_job_id) and not wants_dispatch_defaults:
        selection_origin = "inherited"
        inherited_ids = await dependencies.inherit_parent_ids(
            thread_id=job.thread_id, parent_job_id=job.parent_job_id
        )
        inherited_ids = await dependencies.filter_implicit_lite_ids(
            inherited_ids, lite_backend
        )
        (
            selected_ds_ids,
            selected_ds_revisions,
        ) = await dependencies.authorize_selection(
            selection_actor,
            inherited_ids,
            workspace_backend=None,
            target_project_ids=target_project_ids,
            effective_work_owner_id=(
                str(effective_user_id) if effective_user_id else None
            ),
            trusted_system_inheritance=trusted_system_origin,
            legacy_job_id=str(job.parent_job_id) if job.parent_job_id else None,
        )
    elif effective_user_id and (
        job.use_datasource_defaults or dependencies.defaults_on_omission()
    ):
        selection_origin = "default"
        try:
            (
                selected_ds_ids,
                selected_ds_revisions,
            ) = await dependencies.default_selection(
                str(effective_user_id),
                target_project_ids,
                lite_backend,
            )
        except Exception as exc:
            from orchestrator.services.datasource_policy import (
                DatasourceUnavailableError,
            )

            if isinstance(exc, DatasourceUnavailableError):
                raise HTTPException(
                    status_code=403,
                    detail="One or more selected connectors are unavailable",
                ) from exc
            raise
    else:
        selection_origin = "omitted_compat" if effective_user_id else "system_empty"
        selected_ds_ids = []
        selected_ds_revisions = {}

    selection_provenance = await dependencies.selection_provenance(
        datasource_ids=selected_ds_ids,
        policy_revisions=selected_ds_revisions,
        origin=selection_origin,
        effective_work_owner_id=(str(effective_user_id) if effective_user_id else None),
        actor=selection_actor,
        project_ids=target_project_ids,
        creation_path="internal_rest" if internal_call else "user_rest",
    )

    return JobAdmissionDatasources(
        datasource_ids=selected_ds_ids,
        policy_revisions=selected_ds_revisions,
        provenance=selection_provenance,
        target_project_ids=target_project_ids,
    )
