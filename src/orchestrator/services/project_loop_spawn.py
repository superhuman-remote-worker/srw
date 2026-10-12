"""Raising loop work: spawning a stage, pointing the loop at it, recording it.

Extracted verbatim from ``orchestrator.main`` (R1.B07 lane L, census group
``J_project_loops``; loop_unified_engine.md, loop_parallel_stages.md). This is
the lower half of the loop engine — everything that *creates* a member and
everything that *records* what a finished member did. The advance decisions
live in :mod:`orchestrator.services.project_loop_advance`, which imports this
module's dependency object.

The properties that travel with it and are not re-derived:

* **Every loop spawn funnels through :func:`spawn_loop_stage`**, which is why
  the ``unattended_operations`` re-check lives there rather than at the three
  call sites: revoking the grant under a running loop halts it at the next
  advance instead of letting it spend unattended forever.
* **The owner's grant is the one that must still hold** — the owner is the
  principal the spawned jobs run as, not whoever clicked start.
* **A stage that skipped every role raises** into the same halt the revoked
  grant uses; returning an empty stage would leave a "running" loop with
  nothing in flight, which never advances.
* **Job create and repo/cloud provisioning raise; only the dispatch nudge is
  best effort.** An unprovisioned job is sealed ``failed`` rather than left
  runnable against a void.
* **A fan-out stage disables the memory assembler** on its members, because
  >1 analysis role against one shared project store is a read-modify-write
  race.
* :func:`record_loop_job_outcome` reads the orchestrator's own persisted
  record, never the agent's prose, and fires the "delivered nothing" alarm
  only when NO path delivered — neither the project cloud nor a pull request.
* :func:`notify_loop_event` is a ``low`` category: no email, no push. A
  durable-command replay carries a deterministic dedup key and refuses a
  payload that differs from the stored row.

``ProjectLoopDependencies`` is the one dependency object the whole lane shares.
The B05 grant enforcement, the B03 knowledge reindex, the B08 completion-sweep
router and the B11 dispatch nudge all arrive on it as ports.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Mapping
from uuid import uuid4

from shared.content_redaction import sanitize_text

logger = logging.getLogger(__name__)


@dataclass
class ProjectLoopDependencies:
    """Collaborators for one loop operation, resolved per invocation.

    ``completion_commands_enabled`` and ``completion_sweep_router`` are
    callables because both are B08-owned values the application rebinds at
    runtime and suites rebind on ``orchestrator.main`` (§P1).
    """

    store: Any
    vector_store: Any
    notifier: Any
    gitea_client: Any
    main_cloud_router: Any
    trigger_dispatch: Callable[[], None]
    kick_officer_event_drain: Callable[[Any], None]
    enforce_dispatch_grants: Callable[..., Awaitable[Any]]
    reindex_project_kb: Callable[[str], Awaitable[Any]]
    completion_commands_enabled: Callable[[], bool]
    completion_sweep_router: Callable[[], Any]


def loop_deadline_passed(run_until: Any) -> bool:
    """True if the project loop's run_until deadline has passed (tz-aware)."""
    if run_until is None:
        return False
    if isinstance(run_until, str):
        try:
            run_until = datetime.fromisoformat(run_until)
        except ValueError:
            return False
    if run_until.tzinfo is None:
        run_until = run_until.replace(tzinfo=timezone.utc)
    return datetime.now(timezone.utc) >= run_until


def officer_slot_category(
    officer_meta: dict[str, Any], slot_name: str | None
) -> str | None:
    """The work category a named slot pins, if it is a pool at all."""
    if not slot_name:
        return None
    from orchestrator.services.officer_slots import roster_from_meta
    from orchestrator.services.work_categories import normalize_category

    roster = roster_from_meta(officer_meta) or {}
    spec = roster.get(slot_name) or {}
    return normalize_category(spec.get("category")) if isinstance(spec, dict) else None


async def enforce_officer_ticket_grants(
    config_override: dict | None,
    *,
    user_id: str | None,
    project_ids: list[str],
    dependencies: ProjectLoopDependencies,
) -> None:
    """Create-time PEP for auto-pulled ticket jobs.

    Deliberately NOT ``_enforce_job_create_grants``: that one resolves grants as
    ``runner_kind='user'`` and raises an HTTPException, neither of which fits
    here. A tick job is dispatched as ``lifecycle``, whose grant class raises
    the autonomy ceiling to full while keeping the owner's capability grants —
    check it under the same class it will run under, or the autonomy exemption
    the tick stamps would be refused at create on every project whose owner has
    a review ceiling. ``GrantDenied`` propagates as itself so the tick can skip
    the pool and log, rather than a 422 escaping into a background loop.
    """
    if not user_id or not config_override:
        return
    await dependencies.enforce_dispatch_grants(
        config_override,
        runner_user_id=user_id,
        project_ids=project_ids,
        runner_kind="lifecycle",
    )


async def provision_officer_ticket_repo(
    job_row: dict[str, Any],
    *,
    category: str | None = None,
    dependencies: ProjectLoopDependencies,
) -> None:
    """Repo/cloud provisioning for one auto-pulled ticket job.

    The adapter the officer backlog tick injects, so that service never imports
    main.

    ``loop_floor`` is set for EXECUTORS ONLY, and the distinction is not a
    detail: that flag raises unless the project's cloud baseline is fully
    provisioned. Executors deliver files under ``projects/<slug>/`` and must
    fail loudly rather than write into a void — but a researcher's deliverable
    is a KB decision note and a tester's is issue tickets, neither of which
    touches the project cloud folder at all. Requiring a cloud baseline for
    them would make every research and critique ticket undispatchable on a
    project that has no cloud folder yet, which is precisely the
    infrastructure-shaped version of the bias this feature exists to remove.
    Found live: the first k3d dispatch sealed itself on exactly this.
    """
    from orchestrator.services.job_provisioning import provision_job_repo
    from orchestrator.services.work_categories import EXECUTOR

    await provision_job_repo(
        job_row=job_row,
        gitea_client=dependencies.gitea_client,
        postgres_db=dependencies.store,
        main_cloud_router=dependencies.main_cloud_router,
        loop_floor=(category == EXECUTOR),
        require_repository=True,
    )


async def spawn_loop_job(
    loop: dict[str, Any],
    *,
    role: str,
    iteration: int,
    seq_index: int | None = None,
    remaining_iterations: int | None = None,
    disable_memory_assembler: bool = False,
    extra_context: dict[str, Any] | None = None,
    park_until: datetime | None = None,
    dependencies: ProjectLoopDependencies,
) -> dict[str, Any] | None:
    """Create + provision + dispatch one bare project-loop job.

    Returns ``None`` when ``create_loop_job`` skipped the spawn (archived
    project); ``spawn_loop_stage`` turns an entirely-skipped stage into the
    same halt the revoked-grant path uses.

    Shared by the loop start endpoint and the ``advance_project_loop`` hook
    (both via ``spawn_loop_stage``). Mirrors the automation run-now path:
    ``create_loop_job`` does the DB write, then we provision the Gitea repo and
    nudge the dispatcher. Raises on a failed job create or isolated-repo/cloud
    baseline provisioning (the caller fails the loop); only the final dispatch
    nudge is best-effort. ``seq_index`` / ``remaining_iterations`` are
    stamped into the job's context for the torn-advance heal (see
    ``create_loop_job``). ``disable_memory_assembler`` (set for fan-out stage
    members) turns off the TTL-curation assembler so concurrent members don't
    race the shared project store. The real ticket pool (knowledge-base/knowledge/superpowers/specs/
    2026-07-26-project-backlog-pipeline-design.md) is fetched here — the single
    funnel every loop spawn passes through — and handed to ``create_loop_job``
    pre-rendered. The recent structured job history is injected alongside it;
    either lookup failing costs its context block, never the job.
    """
    from orchestrator.services.job_provisioning import provision_job_repo
    from orchestrator.services.project_loops import (
        create_loop_job,
        render_loop_job_history,
    )

    # Hand the work pool over rather than making the agent hunt for it. Every
    # loop spawn funnels through here. Non-fatal: a KB outage costs the block,
    # not the job.
    backlog_block: str | None = None
    history_block: str | None = None
    project_id = loop.get("project_id")
    if project_id and dependencies.vector_store is not None:
        from orchestrator.services.project_backlog import (
            fetch_backlog,
            render_backlog_block,
        )

        campaign = loop.get("campaign") or {}
        in_progress_id = campaign.get("initiative_note_id")
        try:
            rows, counts = await fetch_backlog(
                dependencies.vector_store,
                str(project_id),
                exclude_note_id=in_progress_id,
            )
            in_progress = None
            if in_progress_id:
                # No "priority" key: the campaign dict carries no real rank
                # for its initiative note, and asserting a guessed one (fix
                # round 1, Finding 2) would render a genuinely-high ticket as
                # "[normal]". render_backlog_block omits the tag when the key
                # is absent rather than defaulting it.
                in_progress = {
                    "note_id": in_progress_id,
                    "title": campaign.get("title") or "",
                }
            backlog_block = render_backlog_block(rows, counts, in_progress=in_progress)
        except Exception:
            logger.warning(
                "loop %s: backlog fetch failed — spawning without the block",
                str(loop.get("id"))[:8],
                exc_info=True,
            )

    if project_id:
        try:
            history_rows = await dependencies.store.list_project_job_change_records(
                str(project_id), limit=20
            )
            history_block = render_loop_job_history(history_rows)
        except Exception:
            logger.warning(
                "loop %s: structured history fetch failed — spawning without it",
                str(loop.get("id"))[:8],
                exc_info=True,
            )

    job = await create_loop_job(
        dependencies.store,
        loop,
        role=role,
        iteration=iteration,
        seq_index=seq_index,
        remaining_iterations=remaining_iterations,
        disable_memory_assembler=disable_memory_assembler,
        extra_context=extra_context,
        backlog_block=backlog_block,
        history_block=history_block,
        park_until=park_until,
    )
    if job is None:
        # Skipped (archived project) — nothing to provision or dispatch.
        return None

    try:
        await provision_job_repo(
            job_row=job,
            gitea_client=dependencies.gitea_client,
            postgres_db=dependencies.store,
            main_cloud_router=dependencies.main_cloud_router,
            # Every loop role receives an isolated repo; the floor keeps scratch
            # out of the project-cloud diff.
            loop_floor=True,
        )
    except Exception:
        logger.exception(
            "project loop %s: repo/cloud provisioning failed for job %s",
            loop.get("id"),
            job.get("id"),
        )
        try:
            await dependencies.store.update_job_status(
                str(job["id"]),
                status="failed",
                error_message="loop repo/cloud provisioning failed",
            )
            job["status"] = "failed"
        except Exception:
            logger.exception(
                "project loop %s: failed to seal unprovisioned job %s",
                loop.get("id"),
                job.get("id"),
            )
        raise

    try:
        dependencies.trigger_dispatch()
    except Exception:
        logger.exception(
            "project loop: dependencies.trigger_dispatch raised (non-fatal)"
        )

    return job


async def spawn_loop_stage(
    loop: dict[str, Any],
    *,
    stage: Any,
    seq_index: int,
    base_total: int,
    remaining: int | None,
    extra_context: dict[str, Any] | None = None,
    park_until: datetime | None = None,
    dependencies: ProjectLoopDependencies,
) -> tuple[list[dict[str, Any]], int]:
    """Spawn every role in ONE loop stage and return (jobs, new_total_jobs_run).

    A single-role stage (``"scholar"``) spawns one job; a fan-out stage
    (``["scholar", "product-qa"]``) spawns one job per role, concurrently. Each
    job's ``loop_iteration`` is the post-stage cumulative job count
    (``base_total + width``) so members of a stage share it; ``seq_index`` and
    ``remaining`` are stamped for the heal. Raises if any job fails to create
    (the caller marks the loop failed — a half-spawned stage with no barrier
    would wedge). knowledge-base/knowledge/features/loop_parallel_stages.md.

    Every loop spawn funnels through here — the start endpoint's first stage,
    the rotation advance, and the campaign advance — which is why the
    ``unattended_operations`` re-check lives here rather than at the three call
    sites. Revoking the grant under a running loop therefore halts it at the
    next advance instead of letting it spend unattended forever; both advance
    callers catch this and stop the loop with the reason in ``last_error``.
    """
    from orchestrator.services.project_loops import normalize_stage

    # Fail closed on a grant revoked mid-run. The owner is the principal the
    # spawned jobs run as, so the owner's grant is the one that must still hold
    # — not the grant of whoever originally clicked start.
    owner_id = loop.get("owner_id")
    owner = await dependencies.store.get_user(str(owner_id)) if owner_id else None
    if owner and not await dependencies.store.user_can_run_unattended_operations(
        owner, str(loop.get("project_id") or "") or None
    ):
        raise PermissionError(
            "unattended_operations: the loop owner no longer holds the "
            "unattended_operations grant"
        )

    roles = normalize_stage(stage)
    new_total = int(base_total) + len(roles)
    # A fan-out stage runs >1 analysis role concurrently against the one shared
    # project store — its members must not each run the TTL-curation assembler
    # (read-modify-write race). A single-role stage keeps the assembler on.
    is_fan_out = len(roles) > 1
    jobs: list[dict[str, Any]] = []
    for role in roles:
        job = await spawn_loop_job(
            loop,
            dependencies=dependencies,
            role=role,
            iteration=new_total,
            seq_index=seq_index,
            remaining_iterations=remaining,
            disable_memory_assembler=is_fan_out,
            extra_context=extra_context,
            park_until=park_until,
        )
        if job is not None:
            jobs.append(job)
    if roles and not jobs:
        # Every role skipped — today that means the project was archived under
        # the running loop. Raise into the same halt the revoked-grant check
        # above uses: both advance callers catch it and stop the loop with the
        # reason in ``last_error``. Returning an empty stage instead would
        # leave a "running" loop with nothing in flight, which never advances.
        raise PermissionError(
            "project archived: the loop's project no longer accepts new work"
        )
    return jobs, new_total


# Sentinel: "leave the campaign column untouched" for writeback_loop_stage —
# None is a meaningful value there (clear the active campaign).
WB_UNSET: Any = object()


async def notify_loop_event(
    loop: dict[str, Any],
    *,
    job_id: str,
    event_type: str,
    subject: str,
    message: str,
    dedup_turn_identity: str | None = None,
    note_id: str | None = None,
    authority_check: Callable[[], Awaitable[None]] | None = None,
    dependencies: ProjectLoopDependencies,
) -> None:
    """Surface a loop event to the loop's owner as an in-app feed row.

    ``loop_event`` is a ``low`` category: no email, no push — resolved Q3 of
    knowledge-base/knowledge/features/loop_campaign_scheduling.md. Best-effort: a
    notification must never break an advance.

    Durable-command handoffs replay after response loss: with a
    ``dedup_turn_identity`` the dedup key is deterministic, so the replay lands
    on the same row and broadcasts nothing (record() is idempotent);
    ``authority_check`` runs before the write and again after a *new* row, as
    the old bell-row helper did. Legacy callers get a random key.
    """
    owner_id = loop.get("owner_id")
    if not owner_id:
        return
    # Loop agents write these (a KB note title, a job's error) — audit OC-05.
    # Redacted before the replay bound cuts them, and deterministically, so a
    # durable replay still renders byte-identical text.
    subject = sanitize_text(subject)
    message = sanitize_text(message)
    loop_id = str(loop.get("id") or "")
    project_id = str(loop.get("project_id")) if loop.get("project_id") else None
    if dedup_turn_identity is not None:
        from orchestrator.services.project_loop_atomic import bounded_replay_text

        event_type = bounded_replay_text(event_type, limit_bytes=96)
        subject = bounded_replay_text(subject, limit_bytes=256)
        message = bounded_replay_text(message, limit_bytes=1024)
        if authority_check is not None:
            await authority_check()
        dedup_key = ":".join(
            (
                "loop",
                loop_id,
                str(dedup_turn_identity),
                str(job_id),
                str(event_type),
                str(note_id or "-"),
            )
        )
    else:
        dedup_key = f"loop:{loop_id}:{job_id}:{event_type}:{uuid4()}"
    try:
        result = await dependencies.notifier.record(
            recipient_id=str(owner_id),
            category="loop_event",
            dedup_key=dedup_key,
            subject=subject,
            body=message,
            source_kind="loop",
            source_id=loop_id or str(job_id),
            action_params={
                "loop_id": loop_id,
                "project_id": project_id,
                "job_id": str(job_id),
            },
            payload={
                "loop_id": loop_id,
                "project_id": project_id,
                "job_id": str(job_id),
                "event_type": event_type,
            },
        )
    except Exception:
        logger.warning("loop notify: feed write failed (non-fatal)", exc_info=True)
        return
    if dedup_turn_identity is None:
        return
    if not result.inserted:
        # A durable-command replay must be byte-identical: the same turn
        # identity carrying a different subject/message is a handoff bug, and
        # the old bell-row helper refused it. record() tolerates text drift
        # for ordinary producers, so the check lives here.
        stored = await dependencies.store.get_notification(result.notification_id)
        if stored and (
            stored.get("subject") != subject or stored.get("body") != message
        ):
            raise RuntimeError("loop notification replay carried a different payload")
        return
    if authority_check is not None:
        await authority_check()


async def notify_loop_user_questions(
    loop: dict[str, Any],
    job: dict[str, Any],
    *,
    dedup_turn_identity: str | None = None,
    durable: bool = False,
    authority_check: Callable[[], Awaitable[None]] | None = None,
    dependencies: ProjectLoopDependencies,
) -> None:
    """Surface ``user-question``-tagged KB notes a completed loop job wrote.

    The loop's constitution tells agents that human-gated concerns (legal
    review, budgets, third-party access) become `user-question` notes instead
    of fictional blockers — this is the delivery half: one bell notification
    per note (capped) so the operator sees the question without KB
    archaeology. Best-effort; a missing/down vector store is silent
    (KB failures are non-fatal by convention).

    The scan is on ``knowledge_index.job_id``, which records the job that
    *last wrote* the row, not the one that authored the note: every canonical
    write stamps it. So this can fire for a note THIS job merely edited —
    a question another job filed, still open, re-surfaced by an edit — and
    will not fire for a question this job filed that a later job has since
    rewritten. The dedup key below covers a replay of ONE turn, not a second
    job re-surfacing the same note later. Restoring author provenance is a
    separate, already-filed follow-up.
    """
    project_id = loop.get("project_id")
    if not project_id or dependencies.vector_store is None:
        return
    if authority_check is not None:
        await authority_check()
    try:
        async with dependencies.vector_store.acquire() as conn:
            rows = await conn.fetch(
                "SELECT note_id, title FROM knowledge_index "
                "WHERE job_id = $1::uuid AND project_id = $2::uuid "
                "AND 'user-question' = ANY(tags) "
                "ORDER BY indexed_at DESC LIMIT 5",
                str(job["id"]),
                str(project_id),
            )
    except Exception:
        if durable:
            raise
        logger.debug(
            "loop notify: user-question scan unavailable (non-fatal)",
            exc_info=True,
        )
        return
    for r in rows:
        if authority_check is not None:
            await authority_check()
        await notify_loop_event(
            loop,
            dependencies=dependencies,
            job_id=str(job["id"]),
            event_type="loop_user_question",
            subject=f"Loop question: {str(r['title'])[:120]}",
            message=(
                f"A loop agent filed a question for you (KB note "
                f"'{r['note_id']}'). It proceeded on its best assumption — "
                "answer via the KB or the loop's steering fields when "
                "convenient."
            ),
            dedup_turn_identity=dedup_turn_identity,
            note_id=str(r["note_id"]),
            authority_check=authority_check,
        )


async def writeback_loop_stage(
    loop_id: str,
    *,
    jobs: list[dict[str, Any]],
    seq_index: int,
    remaining: int | None,
    total: int,
    consecutive: int,
    last_error: str | None,
    campaign: Any = WB_UNSET,
    dependencies: ProjectLoopDependencies,
) -> dict[str, Any] | None:
    """Point a loop at a freshly-spawned turn.

    Every turn is barrier-tracked: ``current_stage_jobs`` holds the members
    (width 1 included) and the atomic barrier drains it when the last one
    finishes. ``current_job_id`` is a display-only mirror — the member's id
    for a width-1 turn (cockpit links, MCP formatters), NULL for fan-out
    turns. Mirrors the counters the advance always wrote.

    ``campaign`` (campaign-mode loops) rides the SAME row update as the
    pointer, so the queue-cursor/status mutation and the stage pointer can
    never tear apart from each other (knowledge-base/knowledge/features/loop_campaign_scheduling.md).
    """
    ids = [str(j["id"]) for j in jobs]
    common = dict(
        seq_index=seq_index,
        remaining_iterations=remaining,
        consecutive_failures=consecutive,
        total_jobs_run=total,
        last_error=last_error,
    )
    if campaign is not WB_UNSET:
        common["campaign"] = campaign
    return await dependencies.store.update_project_loop(
        loop_id,
        current_job_id=(ids[0] if len(ids) == 1 else None),
        current_stage_jobs=ids,
        **common,
    )


async def record_loop_job_outcome(
    job: dict[str, Any],
    *,
    ctx: dict[str, Any] | None,
    loop: dict[str, Any],
    loop_id: str,
    actions: list[str],
    failed: bool,
    last_error: str | None,
    durable: bool = False,
    authority_check: Callable[[], Awaitable[None]] | None = None,
    dependencies: ProjectLoopDependencies,
) -> tuple[str, str | None]:
    """Persist a loop member's delivery outcome and refresh its knowledge.

    Project files have already taken the cloud-delivery path before a successful
    member becomes terminal. This hook never merges the isolated job repo into
    a project repo. It writes one structured database record, flags an execution
    turn that produced no cloud changes, and triggers the independent knowledge
    index refresh. Best effort — never raises. Returns
    ``(delivery_status, delivery_sha)``.

    See knowledge-base/knowledge/features/project_jobs_repo_retirement.md.
    """
    from orchestrator.services.job_records import (
        job_delivered_nothing,
        persisted_pull_request,
    )
    from orchestrator.services.project_loops import (
        is_loop_execution_role,
        write_loop_retro,
    )

    blocked_undelivered = job.get("completion_outcome_kind") == "blocked_undelivered"
    delivery = (ctx or {}).get("loop_cloud_delivery") or {}
    if not isinstance(delivery, dict):
        delivery = {}
    delivery_status = str(
        job.get("merge_status")
        or delivery.get("delivery_status")
        or (
            "blocked-undelivered"
            if blocked_undelivered
            else ("none" if failed else "no-changes")
        )
    )
    delivery_sha = delivery.get("delivery_sha")
    delivery_notes = [str(note) for note in (delivery.get("notes") or [])]

    # Delivery guard. "Did the project cloud folder change?" is the wrong
    # question for a project whose code compounds into a source repository:
    # `no-changes` is the honest, permanent answer there, and the delivered
    # artefact is a pushed branch with a pull request open against it. Asking
    # only the cloud question flags every successful code turn as empty;
    # asking whether `main` moved is worse still, since review-based delivery
    # deliberately leaves `main` alone. So the alarm fires only when NO path
    # delivered. Reads the orchestrator's own persisted record — never the
    # agent's prose — and a stale or malformed record fails loud rather than
    # silently reporting a delivery that may not exist.
    # knowledge-base/knowledge/features/better_resavio_restart_status.md §6a.
    completed_role = (ctx or {}).get("loop_role")
    if (
        not failed
        and not blocked_undelivered
        and is_loop_execution_role(completed_role)
    ):
        pull_request = persisted_pull_request(job)
        if job_delivered_nothing(job, delivery_status=delivery_status):
            logger.warning(
                "project loop %s: execution job %s completed without "
                "project-cloud changes and without a pull request",
                loop_id,
                str(job["id"])[:8],
            )
            actions.append(
                f"project loop {str(loop_id)[:8]}: execution job "
                f"{str(job['id'])[:8]} delivered nothing "
                f"(no project-cloud changes, no pull request)"
            )
        elif pull_request is not None:
            # The positive case is worth an action line too: without it the
            # only loop-visible trace of a source-repo delivery is silence,
            # which reads exactly like the failure it replaced.
            actions.append(
                f"project loop {str(loop_id)[:8]}: execution job "
                f"{str(job['id'])[:8]} delivered {pull_request.repo} "
                f"PR #{pull_request.number} ({pull_request.head})"
            )

    # Knowledge is independent of the job execution repo. Refresh the dedicated
    # project vault after every successful member; the up-to-date short-circuit
    # makes a no-op cheap and the leader sweep remains the recovery path.
    if not failed and not blocked_undelivered and loop.get("project_id"):

        async def _kb_reindex_after_job(pid: str) -> None:
            try:
                await dependencies.reindex_project_kb(pid)
            except Exception:
                logger.warning(
                    "post-job kb_reindex failed (non-fatal)",
                    exc_info=True,
                )

        if durable:
            if authority_check is not None:
                await authority_check()
            await dependencies.reindex_project_kb(str(loop["project_id"]))
            if authority_check is not None:
                await authority_check()
        else:
            asyncio.create_task(_kb_reindex_after_job(str(loop["project_id"])))

    try:
        if durable and authority_check is not None:
            await authority_check()
        recorded = await write_loop_retro(
            dependencies.store,
            job,
            ctx=ctx or {},
            merge_status=delivery_status,
            merged_sha=str(delivery_sha) if delivery_sha else None,
            failed=failed,
            outcome_kind=("blocked_undelivered" if blocked_undelivered else None),
            error=last_error,
            merge_notes=delivery_notes,
            vector_db=dependencies.vector_store,
        )
        if durable and authority_check is not None:
            await authority_check()
        if durable and not recorded:
            # ``ON CONFLICT (job_id) DO NOTHING`` is a successful replay; the
            # writer also returns False after a swallowed dependency error, so
            # distinguish them with the authoritative record table.
            existing = await dependencies.store.get_job_change_record(str(job["id"]))
            if existing is None:
                raise RuntimeError(
                    f"project loop {loop_id}: durable job record was not persisted"
                )
    except Exception:
        if durable:
            raise
        logger.exception(
            "project loop %s: structured record write failed (non-fatal)", loop_id
        )
    return delivery_status, str(delivery_sha) if delivery_sha else None


async def prepare_atomic_loop_spawn_blocks(
    loop: Mapping[str, Any], *, dependencies: ProjectLoopDependencies
) -> tuple[str | None, str | None]:
    """Prepare vector/DB kickoff reads before the short S32 transaction."""

    from orchestrator.services.project_backlog import (
        fetch_backlog,
        render_backlog_block,
    )
    from orchestrator.services.project_loops import render_loop_job_history

    project_id = loop.get("project_id")
    backlog_block: str | None = None
    history_block: str | None = None
    if project_id and dependencies.vector_store is not None:
        campaign = loop.get("campaign") or {}
        in_progress_id = campaign.get("initiative_note_id")
        try:
            rows, counts = await fetch_backlog(
                dependencies.vector_store,
                str(project_id),
                exclude_note_id=in_progress_id,
            )
            in_progress = (
                {
                    "note_id": in_progress_id,
                    "title": campaign.get("title") or "",
                }
                if in_progress_id
                else None
            )
            backlog_block = render_backlog_block(
                rows,
                counts,
                in_progress=in_progress,
            )
        except Exception:
            logger.warning(
                "loop %s: atomic-advance backlog preflight failed; spawning "
                "without the block",
                str(loop.get("id"))[:8],
                exc_info=True,
            )
    if project_id:
        try:
            history_rows = await dependencies.store.list_project_job_change_records(
                str(project_id), limit=20
            )
            history_block = render_loop_job_history(history_rows)
        except Exception:
            logger.warning(
                "loop %s: atomic-advance history preflight failed; spawning "
                "without the block",
                str(loop.get("id"))[:8],
                exc_info=True,
            )
    return backlog_block, history_block


def loop_stop_reason(
    loop: dict[str, Any], *, next_remaining: int | None, consecutive: int
) -> str | None:
    """Which stop axis (if any) trips after this advance — budget / deadline /
    failures. Re-checked every advance; shared by the single-role and
    parallel-stage paths."""
    if next_remaining is not None and next_remaining <= 0:
        return "budget"
    if loop_deadline_passed(loop.get("run_until")):
        return "deadline"
    if consecutive >= int(loop.get("max_consecutive_failures") or 3):
        return "failures"
    return None


__all__ = [
    "ProjectLoopDependencies",
    "WB_UNSET",
    "enforce_officer_ticket_grants",
    "loop_deadline_passed",
    "loop_stop_reason",
    "notify_loop_event",
    "notify_loop_user_questions",
    "officer_slot_category",
    "prepare_atomic_loop_spawn_blocks",
    "provision_officer_ticket_repo",
    "record_loop_job_outcome",
    "spawn_loop_job",
    "spawn_loop_stage",
    "writeback_loop_stage",
]
