"""The Officer Post's lifecycle: commission, decommission, hold, release, edit.

Extracted verbatim from ``orchestrator.main`` (R1.B07 lane O, census group
``S_OFFICER``; officer_post.md §5/§7, O3/O4).

:func:`decommission_officer_post` is the authoritative commissioned → vacant
transition and the rest of this module funnels through it. PostgreSQL reads the
thread state again under post → thread row locks; the caller's ``thread`` is
identity only. Harvest, wake folding, durable route fallback, one incarnation
append, unlink and the server-owned disable/end are **one transaction**, and
route notification delivery happens only after that commit — a failed notifier
leaves ``user_delivery_at`` null for the reconciler.

Orderings preserved literally:

* **Commission parses the money-spending authority first.** A closed release
  fence is a no-write refusal, not a half-completed commission; the capability
  gate then runs before anything mutates, so a missing
  ``unattended_operations`` grant leaves the kit untouched.
* **A stale ended/disabled link completes its handoff atomically** before a
  successor is prepared — registration never performs a partial fold.
* **The final locked confirmation linearizes the response** against an
  immediately racing decommission: whichever holds the post first defines the
  truthful result.
* **Hold stamps no ``thread_id`` key**, and that absence is what keeps the
  watchdog's stale-conference-hold self-heal from ever releasing a maintenance
  hold.
* **Release refuses while the runtime recycle owns the hold.**
* **Patch writes the durable row always**, merges into thread metadata only
  when commissioned, and never lets ``communication_policy`` touch the thread.

Other batches' authorities arrive as constructed ports and are never
re-implemented here: ``create_thread`` is B06's one session funnel (commission
is a caller of it, not a second create path), ``end_thread_flow`` is B09's
stand-down, and ``deliver_officer_note`` is the wake service's. The release
fence arrives as a nested :class:`OfficerPostPolicyDependencies` so the flag is
read live.
"""

from __future__ import annotations

import copy
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Mapping, Optional
from uuid import UUID

from fastapi import HTTPException, Request

from orchestrator.database.postgres import (
    OfficerPostLifecycleConflict,
    project_officer_harvested_state,
)
from orchestrator.schemas.officer_post import (
    OfficerDecommissionRequest,
    OfficerHoldRequest,
    OfficerNoteRequest,
)
from orchestrator.schemas.thread_admission import ThreadCreateRequest
from orchestrator.services import officer_notices
from orchestrator.services.agent_pod_entrypoint import InvalidConfigNameError
from orchestrator.services.officer_metadata import thread_officer_meta
from orchestrator.services.officer_notices import OfficerNoticeDependencies
from orchestrator.services.officer_post_policy import (
    OFFICER_CONFIG_NAME,
    OFFICER_NOTE_MAX_CHARS,
    OFFICER_PERMISSION_MODE,
    OFFICER_POST_EFFECTS,
    OfficerPostPolicyDependencies,
    check_officer_sleep_bounds,
    enforce_officer_auto_pull_release,
    format_legate_note,
    validated_officer_post_patch,
)
from orchestrator.services.session_create_overrides import (
    OFFICER_POST_OWNED_CREATE_KEYS,
    SESSION_OFFICER_OVERRIDE_KEYS,
)

logger = logging.getLogger(__name__)


@dataclass
class OfficerPostLifecycleDependencies:
    """Collaborators for one Post lifecycle transition, per invocation."""

    store: Any
    persistent_provisioner: Any
    persistent_thread_recycler: Any
    policy: OfficerPostPolicyDependencies
    kick_officer_event_drain: Callable[[Any], None]
    deliver_officer_note: Callable[..., Awaitable[str]]
    create_thread: Callable[..., Awaitable[dict[str, Any]]]
    end_thread_flow: Callable[..., Awaitable[dict[str, Any]]]

    def notice_dependencies(self) -> OfficerNoticeDependencies:
        """Dependencies for the sibling one-way delivery module."""
        return OfficerNoticeDependencies(store=self.store)


async def decommission_officer_post(
    thread: dict[str, Any],
    *,
    reason: str,
    force: bool = False,
    allow_orphan_retirement: bool = False,
    retirement: Mapping[str, Any] | None = None,
    dependencies: OfficerPostLifecycleDependencies,
) -> Optional[dict[str, Any]]:
    """Run the authoritative commissioned -> vacant database transition.

    PostgreSQL reads the thread state again under post -> thread row locks;
    the caller's ``thread`` is identity only. Harvest, wake folding, durable
    route fallback, one incarnation append, unlink and server-owned
    disable/end are one transaction. Route notification delivery happens only
    after that commit and a failed notifier leaves ``user_delivery_at`` null
    for the reconciler.
    """
    project_id = thread.get("project_id")
    if not project_id:
        return None
    project_id = str(project_id)
    thread_id = str(thread["id"])
    retirement_kwargs: dict[str, Any] = {}
    if retirement is not None:
        retirement_kwargs = {
            "retirement_token": str(retirement.get("token")),
            "retirement_generation": str(retirement.get("generation")),
            "retirement_settle_status": str(
                (retirement.get("context") or {}).get("settle_status") or ""
            ),
        }
    summary = await dependencies.store.decommission_project_officer(
        project_id,
        thread_id,
        reason=reason,
        force=force,
        allow_orphan_retirement=allow_orphan_retirement,
        **retirement_kwargs,
    )

    routes = summary.get("routes") or []
    delivered = (
        await officer_notices.deliver_staged_officer_routes(
            routes,
            reason="officer_decommissioned",
            dependencies=dependencies.notice_dependencies(),
        )
        if summary.get("transitioned")
        else 0
    )
    summary["routes_staged"] = len(routes)
    summary["routes_delivered"] = delivered
    summary.pop("routes", None)
    if summary.get("blocked_by_in_flight"):
        logger.info(
            "officer post %s: no-force decommission held by %d in-flight jobs",
            project_id[:8],
            len(summary.get("in_flight_jobs") or []),
        )
    elif summary.get("transitioned"):
        logger.info(
            "officer post %s: decommissioned thread %s (reason=%s, harvested=%s, "
            "queue folded=%d deleted=%d)",
            project_id[:8],
            thread_id[:8],
            reason,
            summary.get("harvested", False),
            summary.get("folded", 0),
            summary.get("deleted", 0),
        )
    else:
        logger.info(
            "officer post %s: decommission no-op for thread %s "
            "(already_decommissioned=%s orphan_retired=%s)",
            project_id[:8],
            thread_id[:8],
            summary.get("already_decommissioned", False),
            summary.get("orphan_retired", False),
        )
    return summary


def officer_in_flight_decommission_response(
    summary: dict[str, Any],
) -> dict[str, Any]:
    jobs = list(summary.get("in_flight_jobs") or [])
    return {
        "status": "in_flight",
        "warning": (
            f"{len(jobs)} job(s) in flight on this post. Decommission leaves "
            "them running; retry with force=true to proceed."
        ),
        "in_flight_jobs": jobs,
    }


async def recycle_project_officer(
    request: Request,
    project_id: str,
    *,
    dependencies: OfficerPostLifecycleDependencies,
) -> dict[str, Any]:
    """Recycle only the commissioned Officer's disposable runtime pod.

    The existing project owner/admin policy is authoritative.  This is not an
    Officer tool and runtime actors cannot use it to recycle themselves.
    """

    officer = await dependencies.store.get_officer_thread_for_project(project_id)
    if officer is None:
        raise HTTPException(status_code=409, detail="The Officer Post is vacant")
    recycler = dependencies.persistent_thread_recycler
    if recycler is None or not dependencies.persistent_provisioner.is_available:
        raise HTTPException(
            status_code=503, detail="Persistent runtime lifecycle is unavailable"
        )
    thread_id = str(officer["id"])
    # The recycler provisions from the STORED threads.config_name, so a row
    # poisoned before that column was validated on write reaches the pod
    # entrypoint's allow-list here and raises. That is a bad row, not a broken
    # server: answer it with the validator's own sentence and a 4xx the
    # operator can act on, instead of letting the generic Exception handler
    # turn it into an opaque 500.
    try:
        result = await recycler.request_and_reconcile(
            thread_id=thread_id,
            reason="operator_requested",
            expected_build_sha=dependencies.persistent_provisioner.expected_build_sha,
            observation=await recycler.observe(thread_id),
            expected_project_id=project_id,
        )
    except InvalidConfigNameError as exc:
        logger.warning(
            "Officer runtime recycle refused for thread %s: %s", thread_id, exc
        )
        raise HTTPException(
            status_code=422,
            detail=(
                f"This Officer's stored session config cannot be booted: {exc} "
                "Decommission and re-commission the post to reset it."
            ),
        ) from exc
    if result.state in {"blocked", "cancelled"}:
        raise HTTPException(
            status_code=409,
            detail="Officer runtime recycle could not acquire current authority",
        )
    return {"thread_id": thread_id, **result.safe_view()}


def require_officer_commission_project_id(project_id: str) -> None:
    """Fail closed on an unparseable project id, before anything is read.

    It ran in exactly this position before the extraction — the first
    statement of ``commission_project_officer``, ahead of the ownership gate —
    because the gate's own UUID cast would answer a DB-layer 500 instead of
    this 422. The route declaration calls it first so that order survives the
    gate moving into the router; the operation calls it again so the fence
    holds for any other caller. Parsing a UUID twice costs nothing.
    """
    try:
        UUID(str(project_id))
    except (TypeError, ValueError):
        raise HTTPException(
            status_code=422,
            detail=(
                "officer commission requires one valid project id — got "
                f"'{project_id}'. The officer's post binds exactly one "
                "project knowledge base (officer_knowledge_plane.md §3.1)."
            ),
        )


async def commission_project_officer(
    request: Request,
    project_id: str,
    body: dict[str, Any] | None = None,
    *,
    dependencies: OfficerPostLifecycleDependencies,
    user: dict[str, Any],
    project: dict[str, Any],
) -> dict[str, Any]:
    """Raise an officer onto the project's post (officer_post.md §5).

    Auth: project admin (owner or platform admin) — the kit belongs to the
    century, not to whoever clicked provision (§11 Q1, decided). Optional
    body: the same partial kit as PATCH; validated and merged into the row
    FIRST, so the thread is created from the durable record. A stale
    ended/disabled link completes the authoritative handoff before provisioning;
    the new thread then goes through the one create funnel, whose registration
    claim links it and 409s rivals. The continuity brief is his first wake:
    vacant-since/until, a pointer at his restored state + charter, and the
    while-vacant ledger.

    Project-binding invariant (officer_knowledge_plane.md §3.1, K1): a
    commissioned background officer has exactly one project — the sole native
    writable KB — and no override may replace that write target. This
    endpoint satisfies most of it by construction: the project comes from the
    URL, ``ThreadCreateRequest`` carries a single ``project_id``, and the kit
    patch vocabulary (``SESSION_OFFICER_OVERRIDE_KEYS`` + workspace/tools/
    llm/interactive passthrough) has no project or datasource channel. The
    guard below rejects an unparseable project id (fail-closed 422 instead of
    a DB-layer 500); the agent-side attach re-checks the resolved bindings
    and refuses to boot a mis-bound officer.
    """
    require_officer_commission_project_id(project_id)

    # Parse the money-spending authority before stale-link handoff or any
    # other lifecycle mutation. A closed release fence must be a no-write
    # refusal, not a half-completed commission.
    fragment, comm_patch, _effects = validated_officer_post_patch(body)
    requested_officer_patch = fragment.get("officer") or {}
    if requested_officer_patch.get("auto_pull") is True:
        enforce_officer_auto_pull_release(True, dependencies=dependencies.policy)

    # Capability gate, BEFORE anything mutates. The config PDP would also catch
    # this downstream (``evaluate`` refuses ``officer.enabled`` without the
    # grant, which is what covers a hand-rolled thread create), but only after
    # ``update_project_officer_post`` below has already written the kit — a 422
    # on a half-applied commission. Fail here, loudly, with nothing touched.
    if not await dependencies.store.user_can_run_unattended_operations(
        user, project_id
    ):
        raise HTTPException(
            status_code=403,
            detail=(
                "Commissioning an officer requires the unattended_operations "
                "capability grant. Ask an administrator to grant it "
                "(Admin → Grants) for your user or for this project."
            ),
        )

    standing = await dependencies.store.get_officer_thread_for_project(project_id)
    if standing:
        raise HTTPException(
            status_code=409,
            detail=(
                "already commissioned: this project's post is held by thread "
                f"{standing['id']} — decommission him before raising another "
                "officer."
            ),
        )

    post = await dependencies.store.get_or_create_project_officer(project_id)
    if post is None:
        raise HTTPException(status_code=404, detail="Project post not found")

    row_officer_before = (post.get("config_override") or {}).get("officer") or {}
    effective_auto_pull = (
        requested_officer_patch.get("auto_pull")
        if "auto_pull" in requested_officer_patch
        else row_officer_before.get("auto_pull")
    )
    enforce_officer_auto_pull_release(
        effective_auto_pull, dependencies=dependencies.policy
    )

    # A stale ended/disabled link is not a live commission, but it still owns
    # an unfinished handoff. Complete that handoff atomically before preparing
    # a successor; registration itself never performs a partial fold.
    stale_link = post.get("thread_id")
    if stale_link:
        stale_thread = await dependencies.store.get_thread(str(stale_link)) or {
            "id": str(stale_link),
            "project_id": project_id,
        }
        try:
            await decommission_officer_post(
                stale_thread,
                reason="retired",
                force=True,
                dependencies=dependencies,
            )
        except OfficerPostLifecycleConflict as exc:
            raise HTTPException(status_code=409, detail=exc.detail) from exc
        post = await dependencies.store.get_project_officer(project_id) or post

    check_officer_sleep_bounds(
        (post.get("config_override") or {}).get("officer") or {},
        fragment.get("officer") or {},
    )
    try:
        post_generation = post.get("updated_at")
        if post_generation is None:
            raise OfficerPostLifecycleConflict(
                "commission_generation_missing",
                "Officer Post has no lifecycle generation; retry commission.",
            )
        updated = await dependencies.store.update_project_officer_post(
            project_id,
            config_updates=fragment or None,
            communication_policy_patch=comm_patch,
            expected_vacant_updated_at=post_generation,
        )
    except OfficerPostLifecycleConflict as exc:
        status = 400 if exc.code == "invalid_config" else 409
        raise HTTPException(status_code=status, detail=exc.detail) from exc
    if updated is None:
        raise HTTPException(status_code=404, detail="Project post not found")
    post = updated["post"] or post

    # Build the funnel request from the row. llm/interactive ride the
    # request's own bridges (the funnel rebuilds config_override from
    # validated fragments only); the officer block is sanitized to the
    # create-time vocabulary with enabled forced on — commission is the one
    # legitimate writer of a live officer class.
    row_cfg = post.get("config_override") or {}
    row_officer = row_cfg.get("officer") or {}
    officer_fragment = {
        k: v
        for k, v in row_officer.items()
        # Nulls are cleared fields (PATCH null-as-clear) — an omitted key is
        # how the funnel spells "default", so they must not travel.
        # Auto-pull, the century spend ceiling, and the complete roster travel
        # only through the private durable-snapshot seam. Keeping them out of
        # this public-shaped payload makes an accidental future direct call to
        # the generic validator fail closed instead of acquiring Post authority.
        if k in SESSION_OFFICER_OVERRIDE_KEYS
        and k not in OFFICER_POST_OWNED_CREATE_KEYS
        and k != "conference"
        and v is not None
    }
    officer_fragment["enabled"] = True
    create_override: dict[str, Any] = {"officer": officer_fragment}
    for passthrough in ("workspace", "tools"):
        sub = row_cfg.get(passthrough)
        if isinstance(sub, dict) and sub:
            create_override[passthrough] = sub
    # The commissioning request owns infrastructure, independently of Centurion.
    create_override.setdefault("workspace", {}).setdefault("backend", "none")
    row_llm = row_cfg.get("llm") or {}
    row_interactive = row_cfg.get("interactive") or {}
    create_request = ThreadCreateRequest(
        project_id=project_id,
        title=f"Centurion — {project.get('name') or project_id[:8]}",
        # The expert IS the officer's job surface. Without this the request
        # falls to ThreadCreateRequest's ``session_base`` default, and a
        # commissioned officer boots with research/citation tools and NO
        # job_control plane — he cannot dispatch, steer, approve or read
        # evidence, which is the whole of his charge. Found live on the
        # Resavio change of command (2026-08-15): the endpoint-commissioned
        # officer had 34 tools and not one could create a job, while the
        # July officer — provisioned by hand with config_name=centurion —
        # had the full 49. Commissioning selects no workspace above; Centurion
        # supplies the reviewed knowledge grant and behavioral settings; ``officer.enabled`` stays false there and is flipped by
        # the thread override above, which is the documented split.
        config_name=OFFICER_CONFIG_NAME,
        config_override=create_override,
        model=row_llm.get("model"),
        reasoning_level=row_llm.get("reasoning_level"),
        temperature=row_llm.get("temperature"),
        # A background officer is HEADLESS: there is no session for a human to
        # answer a permission prompt in. Falling to the create default
        # (``supervised``) gates every tool call on an approval that can never
        # arrive — the officer issues his calls, the first one parks the turn
        # ("Permission gate unanswered … parking turn; N call(s) left
        # ungated"), and the rest are stripped as orphans on the next pass. He
        # cycles turns forever executing NOTHING: no reads, no dispatches, no
        # sleep filed, empty assistant text, and not one tool result persisted.
        # Observed live 2026-08-15 alongside the config_name defect above, and
        # far harder to see, because every symptom looks like a model problem.
        # The row may still pin a stricter mode deliberately; only the absent
        # case is defaulted.
        permission_mode=row_interactive.get("permission_mode")
        or OFFICER_PERMISSION_MODE,
        # Third field of this shape, after config_name and permission_mode:
        # the endpoint hand-builds the funnel request and omits what the UI
        # supplies (buildConferenceThreadCreateBody sends exactly this for the
        # conference thread). Without it the post gets only the project's own
        # connectors (``native_default``), which is then inherited by
        # anything that legitimately reads the officer's own selection. His
        # tier is lite, so default_datasource_selection correctly withholds
        # clone-based repositories here — dispatch resolves defaults again
        # against the *worker's* backend, which is where the repo attaches.
        use_datasource_defaults=True,
    )
    create_request._officer_post_config_snapshot = copy.deepcopy(row_cfg)
    created = await dependencies.create_thread(create_request, request)
    thread_id = str(created["thread_id"])

    # Registration, state restore, while-vacant drain, and brief INSERT were
    # one post-locked transaction inside the create funnel. This final locked
    # confirmation linearizes the response against an immediately racing
    # decommission: whichever holds the post first defines the truthful result.
    continuity = create_request._officer_commission_result
    if (
        continuity is None
        or not await dependencies.store.confirm_project_officer_incarnation(
            project_id, thread_id
        )
    ):
        raise HTTPException(
            status_code=409,
            detail=(
                "Officer commission was superseded by a concurrent lifecycle "
                "transition; re-read the post before retrying."
            ),
        )
    brief_enqueued = bool(continuity.get("brief_enqueued"))
    if brief_enqueued:
        dependencies.kick_officer_event_drain(dependencies.store)
    else:
        logger.warning(
            "officer commission: brief wake did not enqueue for thread %s",
            thread_id[:8],
        )

    return {
        "status": "commissioned",
        "thread_id": thread_id,
        "title": create_request.title,
        "brief_enqueued": brief_enqueued,
        "while_vacant": len(continuity.get("while_vacant") or []),
        "while_vacant_dropped": int(continuity.get("while_vacant_dropped") or 0),
        "state_restored": bool(continuity.get("state_restored")),
    }


async def decommission_project_officer(
    request: Request,
    project_id: str,
    body: OfficerDecommissionRequest | None = None,
    *,
    dependencies: OfficerPostLifecycleDependencies,
) -> dict[str, Any]:
    """Stand the officer down and keep everything he had (officer_post.md §5).

    Auth: project admin. Returns a 200 warning result listing in-flight jobs unless
    ``force`` — and force only acknowledges the warning: jobs are LEFT
    RUNNING either way; their completions land on the vacant post's ledger.
    The actual hygiene (harvest → queue fold → unlink → incarnation) runs
    inside the shared ``end_thread`` stand-down, so this endpoint and a
    direct thread DELETE are one funnel.
    """
    body = body or OfficerDecommissionRequest()
    reason = (body.reason or "").strip() or "decommissioned"

    post = await dependencies.store.get_or_create_project_officer(project_id)
    if post is None:
        raise HTTPException(status_code=404, detail="Project post not found")
    linked_tid = post.get("thread_id")
    if not linked_tid:
        # Idempotent lifecycle command: a lost first response may be retried.
        # The post row is already the durable proof of vacancy; do not append
        # a synthetic second incarnation.
        return {
            "status": "decommissioned",
            "already_vacant": True,
            "incarnations": post.get("incarnations") or [],
        }
    linked_tid = str(linked_tid)

    thread = await dependencies.store.get_thread(linked_tid)
    if thread is None:
        # The FK normally clears this link on hard delete, but a stale/missing
        # fixture is repaired through the SAME atomic handoff, not two writes.
        try:
            summary = await decommission_officer_post(
                {"id": linked_tid, "project_id": project_id},
                reason=reason,
                force=body.force,
                dependencies=dependencies,
            )
        except OfficerPostLifecycleConflict as exc:
            raise HTTPException(status_code=409, detail=exc.detail) from exc
        if summary and summary.get("blocked_by_in_flight"):
            return officer_in_flight_decommission_response(summary)
        return {
            "status": "decommissioned",
            "thread_id": linked_tid,
            "note": "thread row was already gone; link cleared",
            **{
                key: value
                for key, value in (summary or {}).items()
                if key not in {"incarnation", "post"}
            },
        }

    if thread.get("status") == "ended":
        # Crash-ended incarnation under the page-and-wait policy: the thread
        # is already down — run the post hygiene directly, no end flow.
        try:
            summary = (
                await decommission_officer_post(
                    thread, reason=reason, force=body.force, dependencies=dependencies
                )
                or {}
            )
        except OfficerPostLifecycleConflict as exc:
            raise HTTPException(status_code=409, detail=exc.detail) from exc
        if summary.get("blocked_by_in_flight"):
            return officer_in_flight_decommission_response(summary)
        return {
            "status": "decommissioned",
            "thread_id": linked_tid,
            "already_ended": True,
            **{k: v for k, v in summary.items() if k != "incarnation"},
        }

    flow_result = await dependencies.end_thread_flow(
        linked_tid,
        thread,
        permanent=False,
        force=body.force,
        officer_retire_reason=reason,
        officer_post_required=True,
        include_officer_handoff=True,
    )
    if flow_result.get("status") == "in_flight":
        return flow_result
    handoff = flow_result.pop("_officer_handoff", {}) or {}

    fresh_post = await dependencies.store.get_project_officer(project_id) or {}
    return {
        "status": "decommissioned",
        "thread_id": linked_tid,
        "in_flight_jobs": handoff.get("in_flight_jobs") or [],
        "harvested": bool(project_officer_harvested_state(fresh_post.get("state"))),
        "incarnations": fresh_post.get("incarnations") or [],
    }


async def hold_project_officer(
    request: Request,
    project_id: str,
    body: OfficerHoldRequest | None = None,
    *,
    dependencies: OfficerPostLifecycleDependencies,
) -> dict[str, Any]:
    """Maintenance hold — pause ≠ retire (officer_post.md §5, decided 08-01).

    Auth: project admin. Stamps ``officer.hold = {kind, since, note}`` on the
    THREAD (hold is runtime state; a vacant post 400s) with — critically —
    NO ``thread_id`` key: that absence is what keeps the watchdog's
    stale-conference-hold self-heal from ever releasing it. One key, four
    effects, all pre-wired by the conference machinery: drain skips him,
    dispatches 409, watchdog stands down, nothing self-heals.
    """
    officer = await dependencies.store.get_officer_thread_for_project(project_id)
    if not officer:
        raise HTTPException(
            status_code=400, detail="The post is vacant — nothing to hold"
        )
    officer_tid = str(officer["id"])
    existing = thread_officer_meta(officer).get("hold")
    if existing:
        raise HTTPException(
            status_code=400,
            detail=(
                "already held "
                f"(kind={existing.get('kind') if isinstance(existing, dict) else '?'})"
                " — release before holding again"
            ),
        )
    hold = {
        "kind": "maintenance",
        "since": datetime.now(timezone.utc).isoformat(),
        "note": ((body.note if body else None) or "").strip(),
    }
    try:
        hold_result = await dependencies.store.set_project_officer_hold(
            project_id,
            expected_thread_id=officer_tid,
            hold=hold,
            route_reason="officer_hold",
        )
    except OfficerPostLifecycleConflict as exc:
        raise HTTPException(status_code=409, detail=exc.detail) from exc
    staged_routes = hold_result.get("routes") or []
    delivered_routes = await officer_notices.deliver_staged_officer_routes(
        staged_routes,
        reason="officer_hold",
        dependencies=dependencies.notice_dependencies(),
    )
    notified = await officer_notices.inject_officer_notice(
        officer,
        "[maintenance hold — the Legate has stood you down. Take no "
        "scheduling actions; your timers and events queue durably and arrive "
        "when the hold is released. Legate messages still reach you.]",
        dependencies=dependencies.notice_dependencies(),
    )
    logger.info(
        "officer %s: maintenance hold stamped (project %s)",
        officer_tid[:8],
        project_id[:8],
    )
    return {
        "status": "held",
        "thread_id": officer_tid,
        "held": hold,
        "notified": notified,
        "drained_blocking_routes": len(staged_routes),
        "delivered_blocking_routes": delivered_routes,
    }


async def release_project_officer(
    request: Request,
    project_id: str,
    *,
    dependencies: OfficerPostLifecycleDependencies,
) -> dict[str, Any]:
    """Release the officer's hold (officer_post.md §5). Auth: project admin.

    Clears via the established lever — deep-merge ``{"officer": {"hold":
    None}}`` → JSON null, which every reader (watchdog, wake claim, dispatch
    fence) treats as unheld. Queued events drain within one ~20s tick; the
    kick below just makes it immediate.
    """
    officer = await dependencies.store.get_officer_thread_for_project(project_id)
    if not officer:
        raise HTTPException(
            status_code=400, detail="The post is vacant — nothing to release"
        )
    officer_tid = str(officer["id"])
    hold = thread_officer_meta(officer).get("hold")
    if not hold:
        raise HTTPException(status_code=400, detail="The officer is not held")
    if isinstance(hold, dict) and hold.get("_persistent_recycle_generation"):
        raise HTTPException(
            status_code=409,
            detail=(
                "The Officer runtime recycle owns this maintenance hold; "
                "it releases only after replacement authority is healthy"
            ),
        )
    try:
        await dependencies.store.set_project_officer_hold(
            project_id,
            expected_thread_id=officer_tid,
            hold=None,
        )
    except OfficerPostLifecycleConflict as exc:
        raise HTTPException(status_code=409, detail=exc.detail) from exc
    dependencies.kick_officer_event_drain(dependencies.store)
    notified = await officer_notices.inject_officer_notice(
        officer,
        "[hold released — resume your duties. Events queued during the hold "
        "arrive with your next wake.]",
        dependencies=dependencies.notice_dependencies(),
    )
    logger.info(
        "officer %s: hold released (project %s, was kind=%s)",
        officer_tid[:8],
        project_id[:8],
        hold.get("kind") if isinstance(hold, dict) else "?",
    )
    return {"status": "released", "thread_id": officer_tid, "notified": notified}


async def send_project_officer_note(
    request: Request,
    project_id: str,
    body: OfficerNoteRequest,
    *,
    dependencies: OfficerPostLifecycleDependencies,
    user: dict[str, Any],
) -> dict[str, Any]:
    """Send the project's officer a one-way note (officer_legate_channel.md).

    Auth: project owner — a note carries command authority, the same bar as
    hold/release. The reply, if he has one, arrives in his log or as a page;
    this endpoint deliberately has no ask-and-wait leg.

    The response states which durable acceptance happened rather than a bare
    200: ``queued`` is a wake row whose stable identity is claimed by the
    exact current runtime, and ``held`` is fenced behind a hold that must lift
    first. Durable acceptance is never reported as provider admission.
    """
    message = (body.message or "").strip()
    if not message:
        raise HTTPException(status_code=400, detail="message must not be empty")
    if len(message) > OFFICER_NOTE_MAX_CHARS:
        raise HTTPException(
            status_code=400,
            detail=(
                f"message must be at most {OFFICER_NOTE_MAX_CHARS} characters "
                f"(got {len(message)}) — put the long form in the knowledge "
                "base and point him at it"
            ),
        )
    officer = await dependencies.store.get_officer_thread_for_project(project_id)
    if not officer:
        raise HTTPException(
            status_code=409,
            detail=(
                "The post is vacant — commission an officer before sending him orders"
            ),
        )
    officer_tid = str(officer["id"])
    text = format_legate_note(user, message)
    delivered = await dependencies.deliver_officer_note(
        dependencies.store, officer, text
    )
    if delivered == "queued":
        dependencies.kick_officer_event_drain(dependencies.store)
    next_wake_at = None
    if delivered != "live":
        timer = await dependencies.store.get_pending_officer_timer(officer_tid)
        next_wake_at = (timer or {}).get("fire_at")
    hold = thread_officer_meta(officer).get("hold") or None
    logger.info(
        "officer %s: legate note %s (project %s, %d chars)",
        officer_tid[:8],
        delivered,
        project_id[:8],
        len(message),
    )
    return {
        "delivered": delivered,
        "thread_id": officer_tid,
        "project_id": project_id,
        "next_wake_at": next_wake_at,
        "held": hold,
    }


async def patch_project_officer(
    request: Request,
    project_id: str,
    body: dict[str, Any],
    *,
    dependencies: OfficerPostLifecycleDependencies,
) -> dict[str, Any]:
    """Edit the post — the missing form (officer_post.md §7). Auth: project
    owner or system admin (the kit belongs to the century, §11 decision 1).

    Writes the durable row always; when commissioned, merges the fragment into
    thread metadata (with an explicitly supplied ``slots`` map replacing the
    whole roster) and injects a one-line notice —
    deliberately NOT a wake (the next sitrep's capacity line carries the
    truth). Shrinking below in-flight is drain semantics, decided: the 409
    lives at the next dispatch, running jobs are untouched.
    ``communication_policy`` is row-only and never touches the thread.
    """
    fragment, comm_patch, effects = validated_officer_post_patch(body)
    if (fragment.get("officer") or {}).get("auto_pull") is True:
        enforce_officer_auto_pull_release(True, dependencies=dependencies.policy)
    if not fragment and comm_patch is None:
        raise HTTPException(
            status_code=400,
            detail=(
                "Nothing to update — send at least one of "
                f"{sorted(OFFICER_POST_EFFECTS)}"
            ),
        )
    post = await dependencies.store.get_or_create_project_officer(project_id)
    if post is None:
        raise HTTPException(status_code=404, detail="Project post not found")
    check_officer_sleep_bounds(
        (post.get("config_override") or {}).get("officer") or {},
        fragment.get("officer") or {},
    )
    try:
        update = await dependencies.store.update_project_officer_post(
            project_id,
            config_updates=fragment or None,
            communication_policy_patch=comm_patch,
        )
    except OfficerPostLifecycleConflict as exc:
        status = 400 if exc.code == "invalid_config" else 409
        raise HTTPException(status_code=status, detail=exc.detail) from exc
    if update is None:
        raise HTTPException(status_code=404, detail="Project post not found")
    post = update["post"] or post
    officer = update.get("thread")
    applied_to_thread = bool(update.get("applied_to_thread"))
    if officer and fragment and applied_to_thread:
        changed = ", ".join(sorted(k for k in effects if k != "communication_policy"))
        await officer_notices.inject_officer_notice(
            officer,
            f"[post updated — {changed}. No action needed; your next sitrep "
            "reflects the change.]",
            dependencies=dependencies.notice_dependencies(),
        )

    return {
        "status": "updated",
        "commissioned": bool(officer),
        "applied_to_thread": bool(applied_to_thread),
        "effects": effects,
        "config_override": post.get("config_override") or {},
        "communication_policy": post.get("communication_policy") or {},
    }


__all__ = [
    "OfficerPostLifecycleDependencies",
    "commission_project_officer",
    "decommission_officer_post",
    "decommission_project_officer",
    "hold_project_officer",
    "officer_in_flight_decommission_response",
    "patch_project_officer",
    "recycle_project_officer",
    "release_project_officer",
    "require_officer_commission_project_id",
    "send_project_officer_note",
]
