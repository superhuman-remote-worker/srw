"""Shared job-admission operation for authenticated application callers.

Transport adapters authenticate and sanitize reserved public/internal markers.
This operation owns the ordered preparation and creation stages. Factories bind
each stage only when reached; the application retains policy helpers, stores,
credentials, provisioning and lifecycle. HTTP errors and row redaction remain
the compatibility contract for existing callers.
"""

import asyncio
import copy
from dataclasses import dataclass, replace
import logging
from typing import Any, Awaitable, Callable

from fastapi import HTTPException

from orchestrator.schemas.job_create import JobCreate
from orchestrator.services.connector_refs import refuse_connector_selector_conflict
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
    admit_expert_harness,
    prepare_job_admission_config,
)
from orchestrator.services.job_admission_officer import (
    JobAdmissionOfficerDependencies,
    prepare_job_admission_officer,
)
from orchestrator.services.job_admission_work_expert import (
    apply_work_expert_default,
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
from orchestrator.services.live.admission import (
    CONTEXT_KEY as LIVE_CONTEXT_KEY,
    LiveAdmissionDependencies,
    hand_off,
    prepare_live_session,
    refuse_session_contradictions,
)
from shared.job_phases import (
    PHASES_KEY,
    is_phases_value,
    normalize_phases,
    plans_in_phases,
)


logger = logging.getLogger(__name__)


def admit_job_phases(command: JobCreate) -> None:
    """Check the job's ``phases`` setting and fold the request field into ``config_override``.

    The setting is an ordinary config key (``config_override.phases``), so the
    resolver freezes it with the rest of the job's configuration. The typed
    ``phases`` field is written there before the config stage reads the
    override; a different value already in the override is refused (400), as
    is a value that is neither true nor false. A null value there means "what
    the configuration says" and is dropped. A live-session job always plans
    in phases (it starts in a tactical phase, where ``job_complete`` is
    refused), so an explicit ``false`` on one is refused (422).
    """
    override = command.config_override
    requested: bool | None = None
    if isinstance(override, dict) and PHASES_KEY in override:
        if override[PHASES_KEY] in (None, ""):
            # Null means "what the configuration says". Left in the override
            # it would delete the expert's own value at the merge.
            override = {k: v for k, v in override.items() if k != PHASES_KEY}
        else:
            try:
                requested = normalize_phases(override[PHASES_KEY])
            except ValueError as exc:
                raise HTTPException(400, f"config_override.{exc}") from None
            override = {**override, PHASES_KEY: requested}
        command.config_override = override
    if command.phases is not None:
        if requested is not None and requested != command.phases:
            raise HTTPException(
                400,
                f"phases is {command.phases!r} but config_override.phases is "
                f"{requested!r}; state it once",
            )
        command.config_override = {
            **(override if isinstance(override, dict) else {}),
            PHASES_KEY: command.phases,
        }
        requested = command.phases
    if requested is False and command.live_session:
        raise HTTPException(
            422,
            "A live-session job always plans in phases: it starts in a "
            "tactical phase and finishes in a strategic one. Leave phases "
            "unset or set it to true.",
        )


async def stamp_job_phases(
    *,
    command: JobCreate,
    config_name: str | None,
    expert_row: dict[str, Any] | None,
    config_override: dict[str, Any] | None,
    harness_image: Any,
    expert_id: str | None = None,
    load_expert: Callable[[str], Awaitable[dict[str, Any] | None]] | None = None,
) -> dict[str, Any] | None:
    """Return ``config_override`` with the job's effective ``phases`` on it.

    The value is decided once, when the job is created: the job's own
    override, else the project default merged under it, else the expert, else
    the framework's default. A later edit of the expert does not change a job
    that exists, and the job list can show the value without resolving the
    expert. A live-session job always plans in phases. A job on a custom
    harness image runs its own loop and gets no value. When the expert's
    configuration does not resolve here the override is returned unchanged:
    dispatch resolves it again and reports that error where it belongs.
    """
    if harness_image is not None:
        return config_override
    needs_expert = not command.live_session and not is_phases_value(
        (config_override or {}).get(PHASES_KEY)
    )
    if needs_expert and expert_row is None and expert_id and load_expert is not None:
        # A stored expert is named by id here; its row is read at dispatch.
        try:
            expert_row = await load_expert(str(expert_id))
        except Exception as exc:
            logger.warning(
                "Admission: expert %s could not be read for the job's phases "
                "setting (%s: %s); the job carries none",
                expert_id,
                type(exc).__name__,
                exc,
            )
            return config_override
        if expert_row is None:
            logger.warning(
                "Admission: expert %s was not found for the job's phases "
                "setting; the job carries none",
                expert_id,
            )
            return config_override
    return await resolved_phases_override(
        live_session=bool(command.live_session),
        config_name=config_name,
        expert_row=expert_row,
        config_override=config_override,
    )


async def resolved_phases_override(
    *,
    live_session: bool,
    config_name: str | None,
    expert_row: dict[str, Any] | None,
    config_override: dict[str, Any] | None,
) -> dict[str, Any] | None:
    """``config_override`` with the effective ``phases`` of a job on SRW's own worker.

    Shared by REST admission (:func:`stamp_job_phases`) and manifest admission
    (``manifest_execution``), so a job states its value however it was created.
    """
    override = dict(config_override or {})
    if live_session:
        override[PHASES_KEY] = True
        return override
    if is_phases_value(override.get(PHASES_KEY)):
        override[PHASES_KEY] = normalize_phases(override[PHASES_KEY])
        return override

    from orchestrator.services.config_resolver import resolve_config
    from shared.runtime.core.loader import canonical_config_name

    try:
        blob = await asyncio.to_thread(
            resolve_config,
            base_config_name=canonical_config_name(config_name or "worker_base"),
            expert_row=copy.deepcopy(expert_row) if expert_row else None,
            request_override=copy.deepcopy(config_override)
            if config_override
            else None,
            expert_type="worker",
            db_refs={},
        )
    except Exception as exc:
        logger.warning(
            "Admission: the job's phases setting could not be resolved (%s: %s); "
            "the job carries none",
            type(exc).__name__,
            exc,
        )
        return config_override
    override[PHASES_KEY] = plans_in_phases(blob.get("agent"))
    return override


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
    # A live-session job's session (live_session_jobs §4.2); None where the
    # composition offers no live connectors (a live job is then refused).
    live: Callable[[], LiveAdmissionDependencies] | None = None
    # Reads a stored expert by id, for the job's effective `phases`
    # (stamp_job_phases). A job that names a stored expert carries only the id
    # through admission; without this the value falls back to the base's.
    load_expert: Callable[[str], Awaitable[dict[str, Any] | None]] | None = None


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
    # Before the config stage reads config_override (prepare_job_admission_config).
    admit_job_phases(command)
    refuse_connector_selector_conflict(command)
    if refuse_session_contradictions(command) is not None and dependencies.live is None:
        raise HTTPException(422, "Live-session jobs are not available here")
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
        if config.expert_source != "caller":
            # The ticket's pin and the slot's category exist only after the
            # Officer stage; see job_admission_work_expert.
            config, officer = await apply_work_expert_default(
                config,
                officer,
                requested_category=command.work_category,
                owner_id=scope.user_id,
                preview_refusals=config_dependencies.preview_expert_refusals,
            )
        if config.pending_harness_expert is not None:
            # The default was kept: its harness is this job's, so its launch
            # rule and grant apply now (never to a displaced default).
            config = replace(
                config,
                harness_image=await admit_expert_harness(
                    config.pending_harness_expert,
                    principal=scope.principal,
                    user_id=scope.user_id,
                    project_id=config.project_id,
                    dependencies=config_dependencies,
                ),
                pending_harness_expert=None,
            )
        # After the expert is final (a ticket's pin or a work default can
        # replace it) and before anything reads the override for this job.
        officer = replace(
            officer,
            config_override=await stamp_job_phases(
                command=command,
                config_name=config.config_name,
                expert_row=config.expert_row,
                config_override=officer.config_override,
                harness_image=config.harness_image,
                expert_id=config.expert_id,
                load_expert=dependencies.load_expert,
            ),
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
            # An officer's ticket is unattended: no outbox among its defaults.
            unattended=officer.preparation is not None,
        )
        live = None
        context = officer.context
        if dependencies.live is not None:
            if (
                config.harness_image is not None
                and refuse_session_contradictions(command) is not None
            ):
                # Refused before prepare_live_session writes the session row.
                raise HTTPException(
                    422,
                    "A live-session job runs the SRW harness (its voice subagent "
                    "is SRW's); this Expert's custom harness image cannot take a "
                    "live session.",
                )
            live_dependencies = dependencies.live()
            live = await prepare_live_session(
                command=command,
                datasource_ids=datasources.datasource_ids,
                effective_user_id=scope.user_id,
                project_id=config.project_id,
                config_name=config.config_name,
                expert_id=config.expert_id,
                expert_row=config.expert_row,
                config_override=officer.config_override,
                dependencies=live_dependencies,
            )
            if live is not None:
                if officer.preparation is not None:
                    raise HTTPException(
                        422, "A live-session job cannot claim an officer ticket"
                    )
                context = {**(context or {}), LIVE_CONTEXT_KEY: live.context_value}
        delivery = await prepare_job_admission_delivery(
            command=command,
            context=context,
            datasource_ids=datasources.datasource_ids,
            target_project_ids=datasources.target_project_ids,
            officer_preparation=officer.preparation,
            ticket_ready_at=officer.ticket_ready_at,
            dependencies=dependencies.delivery(),
        )
        result = await create_admitted_job(
            command=command,
            inputs=JobAdmissionCreationInputs(
                context=context,
                config_name=config.config_name,
                expert_id=config.expert_id,
                expert_row=config.expert_row,
                harness_image=config.harness_image,
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
                on_created=live.on_created if live is not None else None,
            ),
            dependencies=dependencies.creation(),
        )
        if live is not None:
            session = await hand_off(live, result, live_dependencies.pods)
            result[LIVE_CONTEXT_KEY] = {
                **live.context_value,
                "status": session.get("status"),
            }
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
