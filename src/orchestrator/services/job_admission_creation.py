"""Persist prepared job admission and sequence its existing provisioning owners.

The inputs carry the already authenticated scope and prepared policies. Officer
locks, rechecks, ticket claims and insertion still belong to ``officer_admission``;
the normal store retains its creation authority. Bound application callbacks own
provisioning, scholar creation and dispatch. This stage starts no lifecycle and
returns the raw row for the caller's existing redaction and outer error mapping.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import logging
from typing import Any, Awaitable, Callable, Protocol, TYPE_CHECKING

from fastapi import HTTPException

from orchestrator.services.officer_admission import (
    OfficerAdmissionConflict,
    SlotAdmissionError,
)

if TYPE_CHECKING:
    from orchestrator.schemas.job_create import JobCreate
    from orchestrator.services.job_admission_workspace import ExecutionLane
    from orchestrator.services.officer_admission import OfficerAdmissionPreparation
    from orchestrator.services.officer_preflight import OfficerPreflightOutcome

logger = logging.getLogger(__name__)


class JobCreationStore(Protocol):
    async def create_job(self, **kwargs: Any) -> dict[str, Any]: ...

    async def get_job(self, job_id: str) -> dict[str, Any] | None: ...


class AdmitOfficer(Protocol):
    async def __call__(
        self,
        *,
        preparation: OfficerAdmissionPreparation,
        job_kwargs: dict[str, Any],
        ticket_note_id: str | None,
        ticket_ready_at: datetime | None,
        ticket_claim_source: str,
        strict_provisioning: bool,
    ) -> dict[str, Any]: ...


class ProvisionOfficer(Protocol):
    async def __call__(
        self, job_row: dict[str, Any], *, category: str | None = None
    ) -> None: ...


class ActivateOfficer(Protocol):
    async def __call__(
        self,
        job_row: dict[str, Any],
        *,
        provision: ProvisionOfficer,
        category: str | None,
        trigger_dispatch: Callable[[], None],
    ) -> OfficerPreflightOutcome: ...


class ProvisionJobRepo(Protocol):
    async def __call__(self, *, job_row: dict[str, Any]) -> dict[str, Any]: ...


class ResolveJobOrigin(Protocol):
    def __call__(
        self,
        *,
        context: dict[str, Any],
        parent_job_id: str | None,
        thread_id: str | None,
    ) -> str: ...


@dataclass(frozen=True)
class JobAdmissionCreationDependencies:
    store: JobCreationStore
    admit_officer: AdmitOfficer
    activate_officer: ActivateOfficer
    provision_officer: ProvisionOfficer
    provision_repo: ProvisionJobRepo
    spawn_scholar: Callable[
        [dict[str, Any], str, dict[str, Any] | None, dict[str, Any]],
        Awaitable[dict[str, Any] | None],
    ]
    resolve_origin: ResolveJobOrigin
    trigger_dispatch: Callable[[], None]


@dataclass(frozen=True)
class JobAdmissionCreationInputs:
    """Prepared values; nested dictionaries retain their existing references."""

    context: dict[str, Any]
    config_name: str
    expert_id: str | None
    config_override: dict[str, Any] | None
    requested_workspace_backend: str | None
    root_creation: bool
    effective_user_id: str | None
    project_id: str | None
    datasource_ids: list[str]
    policy_revisions: dict[str, int]
    provenance: dict[str, Any]
    target_project_ids: list[str]
    execution_lane: ExecutionLane | None
    delivery_contract: dict[str, Any] | None
    officer_preparation: OfficerAdmissionPreparation | None
    ticket_ready_at: datetime | None
    workspace_selection: dict[str, Any] | None = None
    # A validated inline Expert definition; frozen by the INSERT's snapshot.
    expert_row: dict[str, Any] | None = None


async def create_admitted_job(
    *,
    command: JobCreate,
    inputs: JobAdmissionCreationInputs,
    dependencies: JobAdmissionCreationDependencies,
) -> dict[str, Any]:
    # Only authenticated root session creations carry the wake backref. Child
    # jobs may inherit thread scope, but their completion belongs to the parent.
    # The caller strips public thread markers before preparing these inputs.
    creating_thread_id = (
        str(command.thread_id) if (command.thread_id and inputs.root_creation) else None
    )

    create_kwargs = {
        "description": command.description,
        "document_path": command.document_path,
        "document_dir": command.document_dir,
        "config_name": inputs.config_name,
        "expert_id": inputs.expert_id,
        "config_override": inputs.config_override,
        "context": inputs.context if inputs.context else None,
        "user_id": inputs.effective_user_id,
        "project_id": inputs.project_id,
        "parent_job_id": command.parent_job_id,
        "priority": command.priority,
        "creation_order": command.creation_order,
        "worktree_path": command.worktree_path,
        "delegation_context": command.delegation_context,
        "created_by_thread_id": creating_thread_id,
        "wake_on_complete": bool(creating_thread_id),
        "datasource_ids": inputs.datasource_ids,
        "datasource_selection_provenance": inputs.provenance,
        "datasource_policy_revisions": inputs.policy_revisions,
        "authority_user_id": (
            str(inputs.effective_user_id) if inputs.effective_user_id else None
        ),
        "authority_project_ids": (
            inputs.target_project_ids if inputs.effective_user_id else None
        ),
        "execution_lane": inputs.execution_lane,
        "origin": dependencies.resolve_origin(
            context=inputs.context,
            parent_job_id=command.parent_job_id,
            thread_id=creating_thread_id,
        ),
        "requested_workspace_backend": inputs.requested_workspace_backend,
        "workspace_assignment_source": (
            "request"
            if inputs.requested_workspace_backend is not None
            else "resolved_config"
        ),
        "delivery_contract": inputs.delivery_contract,
    }
    if inputs.workspace_selection is not None:
        create_kwargs["workspace_selection"] = inputs.workspace_selection
    if inputs.expert_row is not None:
        create_kwargs["expert_row"] = inputs.expert_row
    if inputs.officer_preparation is not None:
        try:
            result = await dependencies.admit_officer(
                preparation=inputs.officer_preparation,
                job_kwargs=create_kwargs,
                ticket_note_id=str(command.ticket) if command.ticket else None,
                ticket_ready_at=inputs.ticket_ready_at,
                ticket_claim_source="manual",
                strict_provisioning=True,
            )
        except OfficerAdmissionConflict as exc:
            raise HTTPException(
                status_code=409,
                detail={"code": exc.code, "message": exc.detail, **exc.fields},
            ) from exc
        except SlotAdmissionError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
    else:
        result = await dependencies.store.create_job(**create_kwargs)

    officer_preflight_activated = True
    if inputs.officer_preparation is not None:
        preflight = await dependencies.activate_officer(
            result,
            provision=dependencies.provision_officer,
            category=inputs.officer_preparation.category,
            trigger_dispatch=dependencies.trigger_dispatch,
        )
        officer_preflight_activated = preflight.activated
        result = await dependencies.store.get_job(str(result["id"])) or result
        result["provisioning_preflight"] = {
            "state": preflight.state,
            "activated": preflight.activated,
            "retryable": preflight.retryable,
            "phase": preflight.phase,
            "error": preflight.error,
        }
    else:
        await dependencies.provision_repo(job_row=result)

    # Preserve best-effort scholar creation after provisioning, including the
    # fresh row lookup and its error suppression. Eligibility uses the command's
    # parent flag rather than the separate root-creation preparation value.
    if not command.parent_job_id and officer_preflight_activated:
        try:
            fresh_job = await dependencies.store.get_job(str(result["id"]))
            if fresh_job:
                scholar_result = await dependencies.spawn_scholar(
                    fresh_job,
                    inputs.config_name,
                    inputs.config_override,
                    inputs.context,
                )
                if scholar_result:
                    result["scholar_job_id"] = str(scholar_result["id"])
        except Exception as e:
            logger.warning(f"Failed to spawn scholar for job {result['id']}: {e}")

    # Officer activation owns its dispatch callback. Ordinary dispatch happens
    # only after provisioning and the best-effort scholar attempt have settled.
    if inputs.officer_preparation is None:
        dependencies.trigger_dispatch()

    return result
