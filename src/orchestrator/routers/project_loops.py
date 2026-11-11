"""``/api/projects/{project_id}/loop`` — start / inspect / control the project
self-improvement loop.

One active loop per project. **Start** spawns the first job (``role_sequence[0]``,
normally the scholar); the completion hook drives the
Scholar→Critic→Execution rotation from there, one job at a time, until the
iteration / deadline / consecutive-failure budget stops it.

R1.B07 closed this router's nine late ``from orchestrator.main import ...``
sites. The two stores, the three loop operations and the VM permission check
now arrive on :class:`ProjectLoopsDependencies`, resolved from the application
handling the request. ACL stays where it was: ``require_project_member`` in
each declaration body.

Spec: knowledge-base/knowledge/features/project_self_improvement_loop.md.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from pydantic import BaseModel, Field

from orchestrator.security.access import require_project_member
from orchestrator.security.auth import require_approved_user
from orchestrator.services.project_backlog import PRIORITY_WORDS, fetch_backlog

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/projects", tags=["Project Loops"])


@dataclass
class ProjectLoopsDependencies:
    """Collaborators for one loop-control request, resolved per invocation.

    The three loop operations arrive as constructed ports rather than imports
    so this router shares one reading of the engine with the completion hook,
    and ``check_vm_permission`` is B05's policy, consumed and never re-derived.
    """

    store: Any
    vector_store: Any
    spawn_loop_stage: Callable[..., Awaitable[tuple[list[dict[str, Any]], int]]]
    writeback_loop_stage: Callable[..., Awaitable[dict[str, Any] | None]]
    resume_project_loop: Callable[[str], Awaitable[dict[str, Any] | None]]
    check_vm_permission: Callable[..., Awaitable[Any]]


def get_project_loops_dependencies(request: Request) -> ProjectLoopsDependencies:
    """Resolve collaborators only from the application handling this request."""
    return request.app.state.project_loops_dependencies_factory()


# Workspace tiers a loop may pin for every spawned job. Mirrors the validated
# set in migration 0041 and the backends the dispatcher understands. `vm` is the
# headline case; lite tiers are accepted but conflict with a repository
# datasource at dispatch (the loop usually attaches one).
_LOOP_WORKSPACE_BACKENDS = frozenset({"sandbox", "vm", "virtual", "none"})


async def _require_unattended_operations(
    store, caller: dict[str, Any], project_id: str
) -> None:
    """403 unless the caller holds ``unattended_operations`` on this project.

    Applied to the three verbs that put unattended work in motion — start,
    resume, and convert-to-officer — and deliberately NOT to read, pause or
    stop. Nobody should ever be locked out of *halting* a loop by a grant that
    was revoked while it ran; the fail-closed direction here is "no new work",
    not "no control". Admins bypass inside the DB helper.

    The spawn choke point (``services.project_loop_spawn.spawn_loop_stage``)
    re-reads the same grant,
    so this is the loud, synchronous half of a gate that also holds against a
    revocation landing mid-run. Spec:
    knowledge-history/done/unattended_operations_grant.md.
    """
    if await store.user_can_run_unattended_operations(caller, project_id):
        return
    raise HTTPException(
        status_code=403,
        detail=(
            "Project loops require the unattended_operations capability grant. "
            "Ask an administrator to grant it (Admin → Grants) for your user "
            "or for this project."
        ),
    )


class ProjectLoopStart(BaseModel):
    """Request body for ``POST /api/projects/{project_id}/loop``.

    At least one of ``max_iterations`` / ``run_until`` must be set — a loop can
    never be unbounded on both (enforced here and by a DB CHECK). ``goal`` is
    snapshotted from the project unless ``goal_override`` is given.
    """

    model: str | None = Field(None, max_length=200)
    # Each entry is either a single role name (a one-job stage) or a list of
    # role names (a fan-out stage that runs concurrently and barriers before the
    # loop rotates — e.g. [["scholar", "product-qa"], "critic"]). Fan-out stages
    # must be analysis-only (validated at start). loop_parallel_stages.md.
    role_sequence: list[str | list[str]] | None = None
    max_iterations: int | None = Field(None, ge=1, le=1000)
    run_until: datetime | None = None
    acceptance_criteria: str | None = Field(None, max_length=8000)
    user_prompt: str | None = Field(None, max_length=8000)
    goal_override: str | None = Field(None, max_length=8000)
    max_consecutive_failures: int = Field(3, ge=1, le=50)
    # Optional per-loop workspace tier for every spawned job (mirrors `model`).
    # Blank/None = default sandbox; "vm" gives every role a root VM.
    workspace_backend: str | None = Field(None, max_length=20)
    # Scheduling mode: 'standard' (default — the role_sequence stage list,
    # one stage per turn), 'campaign' (the checkpoint critic may expand the
    # execution slot into a multi-stage campaign via a filed plan), or
    # 'officer' (centurion.md §7 — no mechanical advance: each concluded turn
    # wakes the project's centurion, who decides what runs next; requires an
    # enabled officer thread; role_sequence and iteration budgets are ignored).
    # Start-time only; a live loop converts via POST .../loop/scheduling.
    # knowledge-base/knowledge/features/loop_unified_engine.md.
    scheduling: str = Field("standard", pattern="^(standard|campaign|officer)$")
    # Optional per-loop campaign guardrail overrides ({max_stages,
    # max_extensions, abort_failures}); values above the config hard ceilings
    # are rejected, not clamped — fail loud at start.
    campaign_caps: dict[str, int] | None = None


@router.post("/{project_id}/loop", status_code=status.HTTP_201_CREATED)
async def start_project_loop(
    request: Request,
    project_id: str,
    body: ProjectLoopStart,
    *,
    dependencies: ProjectLoopsDependencies = Depends(get_project_loops_dependencies),
) -> dict[str, Any]:
    """Start a self-improvement loop on a project and spawn its first job.

    Editor or higher required. Rejects if the project already has an active
    (running|paused) loop. The first job is the first role in ``role_sequence``;
    everything after is driven by the completion hook.
    """
    from orchestrator.services.project_loops import validate_role_sequence

    caller = await require_approved_user(request, dependencies.store)
    await require_project_member(
        request, dependencies.store, project_id, min_role="editor", allow_archived=False
    )
    await _require_unattended_operations(dependencies.store, caller, project_id)

    # Budget: at least one stop axis must be set (hard floor under runaway) —
    # except officer scheduling, which is naturally unbounded: the centurion
    # and his budgets are the brake, not an iteration count (0075 relaxes the
    # DB CHECK the same way).
    if (
        body.scheduling != "officer"
        and body.max_iterations is None
        and body.run_until is None
    ):
        raise HTTPException(
            status_code=400,
            detail="Set max_iterations and/or run_until — a loop cannot be "
            "unbounded on both.",
        )

    run_until = body.run_until
    if run_until is not None:
        if run_until.tzinfo is None:
            run_until = run_until.replace(tzinfo=timezone.utc)
        if run_until <= datetime.now(timezone.utc):
            raise HTTPException(
                status_code=400, detail="run_until must be in the future."
            )

    roles = body.role_sequence or ["scholar", "critic", "developer"]
    # Grammar: non-empty; every entry normalizes to ≥1 role; fan-out (multi-role)
    # stages are analysis-only (no execution role racing the merge).
    try:
        validate_role_sequence(roles)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    # Campaign scheduling: the template additionally needs exactly one
    # single-role critic checkpoint followed (cyclically) by a single-role
    # execution slot, and any cap overrides must sit under the hard ceilings.
    # Fail loud at start — a doomed campaign-scheduled loop must not degrade
    # silently.
    campaign_caps: dict[str, int] | None = None
    if body.scheduling == "campaign":
        from orchestrator.services.project_loops import (
            planner_slots,
            validate_campaign_caps,
        )

        try:
            planner_slots(roles)
            if body.campaign_caps is not None:
                campaign_caps = validate_campaign_caps(body.campaign_caps)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e
    elif body.campaign_caps is not None:
        raise HTTPException(
            status_code=400,
            detail="campaign_caps only applies to scheduling='campaign'",
        )

    # Workspace tier override (mirrors `model`). Normalise blank → None; reject
    # unknown tiers up front. If `vm`, enforce the same VM permission the
    # dispatcher would (kill-switch + can_use_vm) so a doomed loop fails loud at
    # start instead of every spawned job dying at dispatch.
    workspace_backend = (body.workspace_backend or "").strip().lower() or None
    if (
        workspace_backend is not None
        and workspace_backend not in _LOOP_WORKSPACE_BACKENDS
    ):
        raise HTTPException(
            status_code=400,
            detail="workspace_backend must be one of "
            f"{sorted(_LOOP_WORKSPACE_BACKENDS)}.",
        )
    if workspace_backend == "vm":
        await dependencies.check_vm_permission(caller, job_needs_vm=True)

    # Officer scheduling needs someone to wake: a commissioned post
    # (officer_post.md §4 — the lookup reads project_officers.thread_id, and
    # requires the linked thread live). Fail loud at start — an officer loop
    # with a vacant post would conclude turns into a void.
    if body.scheduling == "officer":
        officer = await dependencies.store.get_officer_thread_for_project(project_id)
        if not officer:
            raise HTTPException(
                status_code=400,
                detail="scheduling='officer' requires a commissioned officer "
                "on this project's post — provision one first.",
            )

    if await dependencies.store.get_active_project_loop(project_id):
        raise HTTPException(
            status_code=409,
            detail="Project already has an active loop. Stop it before starting another.",
        )

    project = await dependencies.store.get_project(project_id)
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")
    if not project.get("main_cloud_folder_handle") or not project.get(
        "main_cloud_backend"
    ):
        raise HTTPException(
            status_code=409,
            detail=(
                "Project loops require a provisioned project cloud folder. "
                "Configure the project folder before starting the loop."
            ),
        )
    goal = body.goal_override if body.goal_override is not None else project.get("goal")

    loop = await dependencies.store.create_project_loop(
        project_id=project_id,
        owner_id=str(caller["id"]),
        goal=goal,
        acceptance_criteria=body.acceptance_criteria,
        user_prompt=body.user_prompt,
        model=body.model,
        role_sequence=roles,
        max_iterations=body.max_iterations,
        run_until=run_until,
        max_consecutive_failures=body.max_consecutive_failures,
        workspace_backend=workspace_backend,
        scheduling=body.scheduling,
        campaign_caps=campaign_caps,
    )

    if body.scheduling == "officer":
        # No first spawn: empty stage pointers are the officer loop's steady
        # state. Wake the centurion instead — the loop now exists and every
        # dispatch is his call.
        from orchestrator.services.session_wake import kick_event_drain, notify_officer

        await notify_officer(
            dependencies.store,
            project_id,
            source="loop",
            dedup_key=f"started:{str(loop['id'])[:8]}",
            payload={
                "loop_id": str(loop["id"]),
                "summary": (
                    "officer-scheduled loop started — nothing was spawned; "
                    "dispatch from the backlog when ready"
                ),
            },
        )
        kick_event_drain(dependencies.store)
        return loop

    # Spawn the first stage (1 job for a single-role entry, N concurrent jobs
    # for a fan-out) and point the loop at it. If the spawn fails, mark the loop
    # failed and surface a 502 — don't leave a running loop with no in-flight
    # job/stage (the advance hook would never fire).
    try:
        jobs, new_total = await dependencies.spawn_loop_stage(
            loop,
            stage=roles[0],
            seq_index=0,
            base_total=0,
            remaining=loop.get("remaining_iterations"),
        )
    except Exception as e:
        logger.exception("Failed to spawn first stage for loop %s", loop["id"])
        await dependencies.store.update_project_loop(
            str(loop["id"]),
            status="failed",
            last_error=f"first spawn failed: {e}",
            stop_reason="failures",
        )
        raise HTTPException(
            status_code=502, detail=f"Loop created but first job failed to start: {e}"
        ) from e

    updated = await dependencies.writeback_loop_stage(
        str(loop["id"]),
        jobs=jobs,
        seq_index=0,
        remaining=loop.get("remaining_iterations"),
        total=new_total,
        consecutive=0,
        last_error=None,
    )
    return updated or loop


@router.get("/{project_id}/loop")
async def get_project_loop(
    request: Request,
    project_id: str,
    *,
    dependencies: ProjectLoopsDependencies = Depends(get_project_loops_dependencies),
) -> dict[str, Any]:
    """Return the project's current loop.

    Prefers the active (running|paused) loop; falls back to the most recent
    terminal one so the cockpit can show the outcome (status + stop_reason)
    after an unattended run finished. 404 only if the project never had a loop.
    """
    await require_approved_user(request, dependencies.store)
    await require_project_member(
        request, dependencies.store, project_id, min_role="viewer"
    )
    loop = await dependencies.store.get_active_project_loop(project_id)
    if not loop:
        recent = await dependencies.store.list_project_loops(project_id=project_id)
        loop = recent[0] if recent else None
    if not loop:
        raise HTTPException(status_code=404, detail="No loop for this project")
    return loop


@router.post("/{project_id}/loop/pause")
async def pause_project_loop(
    request: Request,
    project_id: str,
    *,
    dependencies: ProjectLoopsDependencies = Depends(get_project_loops_dependencies),
) -> dict[str, Any]:
    """Pause the loop: the in-flight job finishes, but no next job is spawned."""
    await require_approved_user(request, dependencies.store)
    await require_project_member(
        request, dependencies.store, project_id, min_role="editor"
    )
    loop = await dependencies.store.get_active_project_loop(project_id)
    if not loop:
        raise HTTPException(status_code=404, detail="No active loop for this project")
    if loop["status"] != "running":
        return loop
    return await dependencies.store.update_project_loop(
        str(loop["id"]), status="paused"
    )


@router.post("/{project_id}/loop/resume")
async def resume_project_loop(
    request: Request,
    project_id: str,
    *,
    dependencies: ProjectLoopsDependencies = Depends(get_project_loops_dependencies),
) -> dict[str, Any]:
    """Resume a paused loop, re-kicking the rotation if its job already finished."""
    caller = await require_approved_user(request, dependencies.store)
    await require_project_member(
        request, dependencies.store, project_id, min_role="editor", allow_archived=False
    )
    # Resume re-kicks the rotation, so it is a start, not a control action.
    await _require_unattended_operations(dependencies.store, caller, project_id)
    loop = await dependencies.store.get_active_project_loop(project_id)
    if not loop:
        raise HTTPException(status_code=404, detail="No active loop for this project")
    if loop["status"] != "paused":
        return loop
    resumed = await dependencies.resume_project_loop(str(loop["id"]))
    return resumed or loop


class ProjectLoopScheduling(BaseModel):
    """Body for ``POST /api/projects/{project_id}/loop/scheduling``."""

    scheduling: str = Field(..., pattern="^officer$")


@router.post("/{project_id}/loop/scheduling")
async def convert_project_loop_scheduling(
    request: Request,
    project_id: str,
    body: ProjectLoopScheduling,
    *,
    dependencies: ProjectLoopsDependencies = Depends(get_project_loops_dependencies),
) -> dict[str, Any]:
    """Convert the live loop to officer scheduling.

    ``scheduling`` is start-time-only everywhere else; this is the one
    sanctioned mutation, and v1 converts in ONE direction (→ officer) — going
    back means stopping the loop and starting a fresh standard one, which
    re-establishes an iteration budget. Guards (atomic, in the UPDATE's WHERE
    via ``convert_project_loop_to_officer``): active loop, no in-flight
    campaign (flipping mid-campaign orphans the campaign cursor), not already
    officer; plus an enabled centurion on the project. An in-flight TURN is
    fine — its completion hits the officer branch and wakes him.
    """
    # Design: knowledge-base/knowledge/features/centurion.md §7/S8.
    from orchestrator.services.session_wake import kick_event_drain, notify_officer

    caller = await require_approved_user(request, dependencies.store)
    await require_project_member(
        request, dependencies.store, project_id, min_role="editor", allow_archived=False
    )
    await _require_unattended_operations(dependencies.store, caller, project_id)

    # Post commissioned? (officer_post.md §4 — row-backed via the flipped
    # lookup, which also requires the linked thread live.)
    officer = await dependencies.store.get_officer_thread_for_project(project_id)
    if not officer:
        raise HTTPException(
            status_code=400,
            detail="scheduling='officer' requires a commissioned officer "
            "on this project's post — provision one first.",
        )

    loop = await dependencies.store.get_active_project_loop(project_id)
    if not loop:
        raise HTTPException(status_code=404, detail="No active loop for this project")

    updated = await dependencies.store.convert_project_loop_to_officer(str(loop["id"]))
    if not updated:
        # The guarded UPDATE matched nothing — re-read for a specific reason.
        current = await dependencies.store.get_project_loop(str(loop["id"])) or loop
        if (current.get("scheduling") or "standard") == "officer":
            return current  # idempotent: already converted
        if current.get("campaign"):
            raise HTTPException(
                status_code=409,
                detail="Loop has an in-flight campaign — let it conclude (or "
                "stop the loop) before converting to officer scheduling.",
            )
        raise HTTPException(
            status_code=409,
            detail="Loop is not in a convertible state (must be running or paused).",
        )

    await notify_officer(
        dependencies.store,
        project_id,
        source="loop",
        dedup_key=f"converted:{str(loop['id'])[:8]}",
        payload={
            "loop_id": str(loop["id"]),
            "summary": (
                "loop converted to officer scheduling — no further "
                "auto-advance; dispatch is yours from the next concluded turn"
            ),
        },
    )
    kick_event_drain(dependencies.store)
    return updated


@router.post("/{project_id}/loop/stop")
async def stop_project_loop(
    request: Request,
    project_id: str,
    *,
    dependencies: ProjectLoopsDependencies = Depends(get_project_loops_dependencies),
) -> dict[str, Any]:
    """Stop the loop permanently. The in-flight job finishes on its own; the
    advance hook is a no-op once status is terminal.
    """
    await require_approved_user(request, dependencies.store)
    await require_project_member(
        request, dependencies.store, project_id, min_role="editor"
    )
    loop = await dependencies.store.get_active_project_loop(project_id)
    if not loop:
        raise HTTPException(status_code=404, detail="No active loop for this project")
    return await dependencies.store.update_project_loop(
        str(loop["id"]),
        status="stopped",
        stop_reason="user",
        current_job_id=None,
        current_stage_jobs=[],
    )


@router.get("/{project_id}/loop/jobs")
async def list_project_loop_jobs(
    request: Request,
    project_id: str,
    limit: int = Query(100, ge=1, le=500),
    *,
    dependencies: ProjectLoopsDependencies = Depends(get_project_loops_dependencies),
) -> list[dict[str, Any]]:
    """List the loop's spawned jobs, newest first.

    Prefers the active (running|paused) loop but falls back to the most recent
    terminal one — the cockpit polls this during the graceful-stop wind-down
    window (loop stopped, last job still finishing) to drive the wind-down
    notice, so a stopped loop must still return its jobs. Mirrors the
    active-or-most-recent fallback in ``get_project_loop``. 404 only if the
    project never had a loop.
    """
    await require_approved_user(request, dependencies.store)
    await require_project_member(
        request, dependencies.store, project_id, min_role="viewer"
    )
    loop = await dependencies.store.get_active_project_loop(project_id)
    if not loop:
        recent = await dependencies.store.list_project_loops(project_id=project_id)
        loop = recent[0] if recent else None
    if not loop:
        raise HTTPException(status_code=404, detail="No loop for this project")
    return await dependencies.store.list_project_loop_jobs(str(loop["id"]), limit=limit)


@router.get("/{project_id}/backlog")
async def get_project_backlog(
    request: Request,
    project_id: str,
    *,
    dependencies: ProjectLoopsDependencies = Depends(get_project_loops_dependencies),
) -> dict[str, Any]:
    """The project's ticket pool -- what the loop's overseer is shown.

    Priority goes out as a word (``high``/``normal``/``low``); the 0/1/2
    storage rank is a ``services.project_backlog`` implementation detail the
    cockpit never needs to know exists. Priority is a non-binding label here
    too -- this endpoint hands back exactly what ``fetch_backlog`` returns,
    in its existing order, and never filters or gates on it.

    A ticket the active loop is already working
    (``campaign.initiative_note_id``) keeps ``status='active'`` in the KB, so
    it's excluded from the pool and surfaced separately as ``in_progress``
    instead -- the cockpit must never see it listed twice. "No active loop"
    and "active loop with no campaign yet" both degrade to an empty-ish
    payload rather than a 404 or a raise -- unlike ``get_project_loop``, the
    pool itself is still a real, answerable question with nothing running.

    Viewer or higher required (a read, like the other GETs on this router).
    """
    await require_approved_user(request, dependencies.store)
    await require_project_member(
        request, dependencies.store, project_id, min_role="viewer"
    )

    loop = await dependencies.store.get_active_project_loop(project_id)
    campaign = (loop or {}).get("campaign") or {}
    in_progress_id = campaign.get("initiative_note_id")

    rows, counts = await fetch_backlog(
        dependencies.vector_store, project_id, exclude_note_id=in_progress_id, limit=200
    )
    return {
        "total": sum(counts.values()),
        "counts": {
            word: counts.get(rank, 0) for rank, word in sorted(PRIORITY_WORDS.items())
        },
        "in_progress": (
            {"note_id": in_progress_id, "title": campaign.get("title") or ""}
            if in_progress_id
            else None
        ),
        "items": [
            {
                "note_id": r["note_id"],
                "note_type": r["note_type"],
                "title": r["title"],
                "priority": PRIORITY_WORDS.get(int(r["priority"]), "normal"),
            }
            for r in rows
        ],
    }
