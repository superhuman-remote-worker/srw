"""``/api/automations`` — CRUD + run-now / pause / resume / runs.

First ``APIRouter`` in the project. R1.B07 closed its eleven late
``from orchestrator.main import ...`` sites: the store, the forge/cloud clients
and the dispatch nudge now arrive on :class:`AutomationsDependencies`, resolved
from the application handling the request, so two applications in one process
cannot share one store no matter which of them answered. The tool-override
validator is imported from its owning service instead of being reached through
the application module — it is a pure helper, and going through ``main`` bought
a second hop and nothing else.

``trigger_dispatch`` is the nudge the handlers that create jobs (run-now) fire
so the new job is picked up by the auto-assign loop without waiting for its 30s
tick.

Spec: knowledge-base/knowledge/features/automations_v0.md §Endpoints.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Callable

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from pydantic import BaseModel, Field

from orchestrator.security.access import PROJECT_ARCHIVED_DETAIL, require_project_member
from orchestrator.security.auth import require_approved_user
from orchestrator.services.automations import (
    create_job_from_automation,
    validate_automation_expert_selection,
)
from orchestrator.services.cron_dispatcher import (
    compute_initial_next_run,
    validate_cron_expr,
    validate_timezone,
)
from orchestrator.services.config_overrides import refuse_caller_transport_keys
from orchestrator.services.default_experts import ExpertSelectionError
from orchestrator.services.session_tool_policy import with_validated_tool_overrides
from shared.runtime.core.loader import canonical_config_name

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/automations", tags=["Automations"])


@dataclass
class AutomationsDependencies:
    """Collaborators for one automation request, resolved per invocation.

    ``trigger_dispatch`` is the auto-assign nudge (B11 owns the scheduler);
    ``gitea_client`` and ``main_cloud_router`` are the application's forge and
    cloud singletons, used only by run-now's best-effort repo provisioning.
    """

    store: Any
    gitea_client: Any
    main_cloud_router: Any
    trigger_dispatch: Callable[[], None]


def get_automations_dependencies(request: Request) -> AutomationsDependencies:
    """Resolve collaborators only from the application handling this request."""
    return request.app.state.automations_dependencies_factory()


# ---------------------------------------------------------------------------
# Request models
# ---------------------------------------------------------------------------


class AutomationCreate(BaseModel):
    """Request body for ``POST /api/automations``.

    v0 only accepts cron triggers; ``trigger_type`` is omitted from the
    public surface and forced server-side. Event triggers ship in v0.5.
    """

    name: str = Field(..., min_length=1, max_length=200)
    description: str | None = Field(None, max_length=2000)
    cron_expr: str = Field(..., min_length=1, max_length=200)
    timezone: str = Field("UTC", max_length=64)
    catchup_window_seconds: int = Field(86400, ge=0, le=7 * 86400)

    expert: str = Field(..., min_length=1, max_length=120)
    expert_id: str | None = Field(None, max_length=64)
    prompt: str = Field(..., min_length=1)
    config_override: dict[str, Any] | None = None
    autonomy: str = Field("review", pattern=r"^(full|review|partial|guided|dependent)$")
    priority: int = Field(5, ge=0, le=10)

    enabled: bool = True
    max_chain_depth: int = Field(10, ge=1, le=100)
    max_fires_per_day: int = Field(100, ge=1, le=10_000)

    project_id: str | None = None


class AutomationUpdate(BaseModel):
    """Request body for ``PATCH /api/automations/{id}``.

    All fields optional. Server recomputes ``next_run_at`` when
    ``cron_expr`` / ``timezone`` / ``enabled`` changes so a pause-then-resume
    or a cron edit lands atomically.
    """

    name: str | None = Field(None, min_length=1, max_length=200)
    description: str | None = Field(None, max_length=2000)
    cron_expr: str | None = Field(None, min_length=1, max_length=200)
    timezone: str | None = Field(None, max_length=64)
    catchup_window_seconds: int | None = Field(None, ge=0, le=7 * 86400)
    expert: str | None = Field(None, min_length=1, max_length=120)
    expert_id: str | None = Field(None, max_length=64)
    prompt: str | None = Field(None, min_length=1)
    config_override: dict[str, Any] | None = None
    autonomy: str | None = Field(
        None, pattern=r"^(full|review|partial|guided|dependent)$"
    )
    priority: int | None = Field(None, ge=0, le=10)
    enabled: bool | None = None
    max_chain_depth: int | None = Field(None, ge=1, le=100)
    max_fires_per_day: int | None = Field(None, ge=1, le=10_000)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _resolve_automation_or_404(
    db: Any,
    automation_id: str,
    caller: dict[str, Any],
    request: Request,
    *,
    min_project_role: str = "viewer",
) -> dict[str, Any]:
    """Fetch an automation and enforce ACL.

    Visibility rules: caller is the owner, OR caller is a project member
    (at ``min_project_role`` or higher) of the automation's project, OR
    caller is admin. Raises 404 if the row doesn't exist (don't leak
    existence to non-members) and 403 on access denied.
    """
    row = await db.get_automation(automation_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Automation not found")

    if caller.get("is_admin"):
        return row
    if str(row["owner_id"]) == str(caller["id"]):
        return row
    if row.get("project_id"):
        # require_project_member raises 403/404 on its own
        await require_project_member(
            request, db, str(row["project_id"]), min_role=min_project_role
        )
        return row
    raise HTTPException(status_code=404, detail="Automation not found")


# ---------------------------------------------------------------------------
# CRUD endpoints
# ---------------------------------------------------------------------------


@router.post("", status_code=status.HTTP_201_CREATED)
async def create_automation(
    request: Request,
    body: AutomationCreate,
    *,
    dependencies: AutomationsDependencies = Depends(get_automations_dependencies),
) -> dict[str, Any]:
    """Create a new automation (cron-only in v0).

    The caller becomes ``owner_id``. If ``project_id`` is set the caller
    must be at least an editor of that project. ``next_run_at`` is seeded
    here from the cron expression so the first dispatcher tick after
    create can fire it without an extra round-trip.
    """
    caller = await require_approved_user(request, dependencies.store)

    # Boundary validation — surface bad cron / tz as 400s rather than 500s
    try:
        validate_cron_expr(body.cron_expr)
        validate_timezone(body.timezone)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    # An automation's config_override is stored raw and handed STRAIGHT to
    # db.create_job by create_job_from_automation — it never crosses
    # POST /api/jobs, so the validator there does not see it, and every cron
    # fire re-plants whatever is stored. Validate it at the only boundary it
    # does cross: this one. Transport/credential keys are refused here too —
    # a standing order that re-plants a caller base_url on every fire would
    # otherwise pair it with the deployment's stored key at each dispatch.
    refuse_caller_transport_keys(body.config_override)
    validated_override = with_validated_tool_overrides(body.config_override)

    if body.project_id:
        # Editor or higher needed to scope an automation to a project —
        # an automation creates jobs that show up on the project page.
        # allow_archived=False for the same reason: this is a standing order
        # to create future work, which is exactly what archiving withdraws.
        await require_project_member(
            request,
            dependencies.store,
            body.project_id,
            min_role="editor",
            allow_archived=False,
        )

    try:
        expert = await validate_automation_expert_selection(
            dependencies.store,
            owner_id=str(caller["id"]),
            project_id=body.project_id,
            expert=body.expert,
            expert_id=body.expert_id,
            caller_is_admin=bool(caller.get("is_admin")),
        )
    except ExpertSelectionError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    next_run = (
        compute_initial_next_run(body.cron_expr, body.timezone)
        if body.enabled
        else None
    )

    row = await dependencies.store.create_automation(
        owner_id=str(caller["id"]),
        project_id=body.project_id,
        name=body.name,
        description=body.description,
        trigger_type="cron",
        cron_expr=body.cron_expr,
        timezone=body.timezone,
        catchup_window_seconds=body.catchup_window_seconds,
        enabled=body.enabled,
        expert=expert,
        expert_id=body.expert_id,
        prompt=body.prompt,
        config_override=validated_override or {},
        autonomy=body.autonomy,
        priority=body.priority,
        max_chain_depth=body.max_chain_depth,
        max_fires_per_day=body.max_fires_per_day,
        next_run_at=next_run,
    )
    return row


@router.get("")
async def list_automations(
    request: Request,
    project_id: str | None = Query(None, description="Filter by project"),
    *,
    dependencies: AutomationsDependencies = Depends(get_automations_dependencies),
) -> list[dict[str, Any]]:
    """List automations visible to the caller.

    Default scope = caller's own automations (admins included — there is
    no "see everyone's automations" view in v0; use a per-user view-as
    toggle to inspect another user's). With ``project_id`` set, returns
    all automations on that project (across owners) provided the caller
    is a project member; otherwise 403.
    """
    caller = await require_approved_user(request, dependencies.store)

    if project_id is not None:
        # Membership check enforces visibility for cross-owner project view.
        await require_project_member(
            request, dependencies.store, project_id, min_role="viewer"
        )
        return await dependencies.store.list_automations(project_id=project_id)

    return await dependencies.store.list_automations(owner_id=str(caller["id"]))


@router.get("/{automation_id}")
async def get_automation(
    request: Request,
    automation_id: str,
    *,
    dependencies: AutomationsDependencies = Depends(get_automations_dependencies),
) -> dict[str, Any]:
    """Fetch a single automation. 404 if not visible to the caller."""
    caller = await require_approved_user(request, dependencies.store)
    return await _resolve_automation_or_404(
        dependencies.store, automation_id, caller, request
    )


@router.patch("/{automation_id}")
async def update_automation(
    request: Request,
    automation_id: str,
    body: AutomationUpdate,
    *,
    dependencies: AutomationsDependencies = Depends(get_automations_dependencies),
) -> dict[str, Any]:
    """Partial update. Recomputes ``next_run_at`` when cron / tz / enabled
    changes; pause (enabled=false) clears it.
    """
    caller = await require_approved_user(request, dependencies.store)
    row = await _resolve_automation_or_404(
        dependencies.store, automation_id, caller, request, min_project_role="editor"
    )

    fields = body.model_dump(exclude_unset=True)
    if not fields:
        return row

    # Same reason as create: this override is replayed into db.create_job on
    # every fire, bypassing the POST /api/jobs validator entirely.
    if "config_override" in fields:
        refuse_caller_transport_keys(fields["config_override"])
        fields["config_override"] = with_validated_tool_overrides(
            fields["config_override"]
        )

    if "cron_expr" in fields:
        try:
            validate_cron_expr(fields["cron_expr"])
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
    if "timezone" in fields:
        try:
            validate_timezone(fields["timezone"])
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    selection_changed = bool({"expert", "expert_id"} & set(fields))
    if "expert" in fields:
        fields["expert"] = canonical_config_name(fields["expert"])
        # Selecting a bundled expert through a partial PATCH also unpins a
        # previously selected DB expert unless the request explicitly supplied
        # its own expert_id (which validation below rejects as ambiguous).
        if fields["expert"] != "worker_base" and "expert_id" not in fields:
            fields["expert_id"] = None
    if selection_changed:
        effective_expert = fields.get("expert", row["expert"])
        effective_expert_id = fields.get("expert_id", row.get("expert_id"))
        try:
            fields["expert"] = await validate_automation_expert_selection(
                dependencies.store,
                owner_id=str(row["owner_id"]),
                project_id=str(row["project_id"]) if row.get("project_id") else None,
                expert=effective_expert,
                expert_id=effective_expert_id,
                caller_is_admin=bool(caller.get("is_admin")),
            )
        except ExpertSelectionError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    # Recompute next_run_at when the schedule shape or enable state changes.
    # Pull the effective post-patch values from `fields` falling back to row.
    schedule_changed = bool({"cron_expr", "timezone", "enabled"} & set(fields))
    if schedule_changed:
        enabled_after = fields.get("enabled", row["enabled"])
        if enabled_after:
            cron_after = fields.get("cron_expr", row["cron_expr"])
            tz_after = fields.get("timezone", row["timezone"])
            fields["next_run_at"] = compute_initial_next_run(cron_after, tz_after)
        else:
            fields["next_run_at"] = None

    updated = await dependencies.store.update_automation(automation_id, **fields)
    return updated or row


@router.delete(
    "/{automation_id}", status_code=status.HTTP_204_NO_CONTENT, response_model=None
)
async def delete_automation(
    request: Request,
    automation_id: str,
    *,
    dependencies: AutomationsDependencies = Depends(get_automations_dependencies),
) -> None:
    """Hard-delete. Past spawned jobs survive — the back-link in
    ``jobs.context['automation_id']`` becomes orphaned, intentional.
    """
    caller = await require_approved_user(request, dependencies.store)
    await _resolve_automation_or_404(
        dependencies.store, automation_id, caller, request, min_project_role="editor"
    )
    await dependencies.store.delete_automation(automation_id)


# ---------------------------------------------------------------------------
# Action endpoints
# ---------------------------------------------------------------------------


@router.post("/{automation_id}/run-now")
async def run_now(
    request: Request,
    automation_id: str,
    *,
    dependencies: AutomationsDependencies = Depends(get_automations_dependencies),
) -> dict[str, Any]:
    """Fire the automation immediately, regardless of schedule.

    Does NOT touch ``next_run_at`` — the next scheduled fire still happens
    on time. Useful for "test this thing I just edited" and for the
    cockpit's Run-now button.
    """
    from orchestrator.services.job_provisioning import provision_job_repo

    caller = await require_approved_user(request, dependencies.store)
    row = await _resolve_automation_or_404(
        dependencies.store, automation_id, caller, request, min_project_role="editor"
    )

    job = await create_job_from_automation(
        dependencies.store, row, trigger_kind="manual"
    )
    if job is None:
        # The service skips-and-logs rather than raising, because its other
        # caller is a cron tick with nobody to answer. Run-now DOES have a
        # caller, and an owner clicking a button that silently does nothing
        # is worse than a 409 naming the one lever that fixes it.
        raise HTTPException(status_code=409, detail=PROJECT_ARCHIVED_DETAIL)

    # Provision the job's Gitea repo/branch + creator access grant (parity
    # with the POST /api/jobs handler). Best-effort — a Gitea outage logs
    # and leaves the job repo-less rather than failing the fire.
    try:
        await provision_job_repo(
            job_row=job,
            gitea_client=dependencies.gitea_client,
            postgres_db=dependencies.store,
            main_cloud_router=dependencies.main_cloud_router,
        )
    except Exception:
        logger.exception(
            "run-now: repo provisioning failed for job %s (non-fatal)",
            job.get("id"),
        )

    # Best-effort nudge to the auto-assign dispatcher so the new job
    # doesn't sit idle for up to 30s waiting on the scheduled tick.
    try:
        dependencies.trigger_dispatch()
    except Exception:
        logger.exception("run-now: trigger_dispatch raised (non-fatal)")

    return {"automation_id": automation_id, "job": job}


@router.post("/{automation_id}/pause")
async def pause_automation(
    request: Request,
    automation_id: str,
    *,
    dependencies: AutomationsDependencies = Depends(get_automations_dependencies),
) -> dict[str, Any]:
    """Set ``enabled=false`` and clear ``next_run_at`` so the dispatcher
    stops considering this automation. Reversible via ``resume``.
    """
    caller = await require_approved_user(request, dependencies.store)
    await _resolve_automation_or_404(
        dependencies.store, automation_id, caller, request, min_project_role="editor"
    )
    return await dependencies.store.update_automation(
        automation_id,
        enabled=False,
        next_run_at=None,
    )


@router.post("/{automation_id}/resume")
async def resume_automation(
    request: Request,
    automation_id: str,
    *,
    dependencies: AutomationsDependencies = Depends(get_automations_dependencies),
) -> dict[str, Any]:
    """Set ``enabled=true`` and recompute ``next_run_at`` from now."""
    caller = await require_approved_user(request, dependencies.store)
    row = await _resolve_automation_or_404(
        dependencies.store, automation_id, caller, request, min_project_role="editor"
    )
    next_run = compute_initial_next_run(row["cron_expr"], row["timezone"])
    return await dependencies.store.update_automation(
        automation_id,
        enabled=True,
        next_run_at=next_run,
    )


@router.get("/{automation_id}/runs")
async def list_runs(
    request: Request,
    automation_id: str,
    limit: int = Query(50, ge=1, le=500),
    *,
    dependencies: AutomationsDependencies = Depends(get_automations_dependencies),
) -> list[dict[str, Any]]:
    """List jobs spawned by this automation, newest first.

    Joins on ``jobs.context->>'automation_id'`` which the dispatcher
    writes at fire time (``services/automations.py``).
    """
    caller = await require_approved_user(request, dependencies.store)
    await _resolve_automation_or_404(dependencies.store, automation_id, caller, request)
    return await dependencies.store.list_automation_runs(automation_id, limit=limit)
