"""The Officer Post's read surfaces: the roster line and the project card.

Extracted verbatim from ``orchestrator.main`` (R1.B07 lane O, census group
``S_OFFICER``; officer_post.md §4/§8). Two reads and the five projections they
share, kept together because they render the same durable row in two shapes.

* :func:`list_officers` — every post the caller can see, vacant ones included.
  It also carries the **auto-pull downgrade preflight**: a system
  administrator's all-project roster is the supported evidence that a
  pre-BP-01 binary is safe to roll back to, which is why the strict-boolean
  reading (:func:`roster_officer_view`) refuses to let a legacy string or
  number collapse into an apparently valid ``False``.
* :func:`get_project_officer_summary` — the cockpit's officer card. The
  ``officer`` block is ALWAYS present so the editor seeds from one place: live
  thread metadata when commissioned, the durable row's config when vacant.
  ``commissioned`` means a LIVE thread holds the post, never that the link
  column is non-null (OC-03) — the old ``bool(thread_id)`` form returned
  ``commissioned: true`` with an empty officer block and wedged the card.
* Kit utilization is **lineage-aware** and goes through the shared admission
  helper, not a local copy of its query: a stale copy would show the Legate a
  free slot the funnel then refuses with a 409.
* Ready depth per pool is computed through the tick's own eligibility path, so
  the card cannot promise dispatches the tick will not make; a KB outage leaves
  it absent rather than showing a false zero.

Every runtime value arrives on :class:`OfficerPostViewDependencies`. The two
deployment flags are **callables** (§P1) because suites rebind them on
``orchestrator.main``, and ``find_open_conference_thread`` is a constructed port
onto the sibling conference module rather than an import, so the card and the
create funnel share one reading of "is there an open conference".
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable
from uuid import UUID

from fastapi import Request

from orchestrator.security.access import user_visible_project_ids
from orchestrator.services.officer_metadata import thread_officer_meta
from orchestrator.services.persistent_recycler import persistent_recycle_view
from orchestrator.services.usage_ledger import llm_tokens_from_rows

logger = logging.getLogger(__name__)


@dataclass
class OfficerPostViewDependencies:
    """Collaborators for one Post read, resolved per invocation."""

    store: Any
    vector_store: Any
    usage_ledger: Any
    persistent_provisioner: Any
    auto_pull_release_enabled: Callable[[], bool]
    persistent_agent_reconciliation_enabled: Callable[[], bool]
    find_open_conference_thread: Callable[[str], Awaitable[dict[str, Any] | None]]


def roster_officer_view(
    row: dict[str, Any], *, dependencies: OfficerPostViewDependencies
) -> dict[str, Any]:
    """One roster line from a ``project_officers`` join row."""
    from orchestrator.services.officer_backlog import auto_pull_enabled

    raw_metadata = row.get("metadata")
    metadata = raw_metadata or {}
    metadata_shape_valid = isinstance(metadata, dict)
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
            metadata_shape_valid = isinstance(metadata, dict)
        except (json.JSONDecodeError, TypeError):
            metadata = {}
            metadata_shape_valid = False
    if not metadata_shape_valid:
        metadata = {}
    config_override = metadata.get("config_override") or {}
    config_override_shape_valid = isinstance(config_override, dict)
    if not config_override_shape_valid:
        config_override = {}
    officer_cfg = config_override.get("officer") or {}
    officer_cfg_shape_valid = isinstance(officer_cfg, dict)
    if not officer_cfg_shape_valid:
        officer_cfg = {}
    raw_post_config_override = row.get("post_config_override")
    post_config_override = raw_post_config_override
    post_config_shape_valid = isinstance(post_config_override, dict)
    if isinstance(post_config_override, str):
        try:
            post_config_override = json.loads(post_config_override)
            post_config_shape_valid = isinstance(post_config_override, dict)
        except (json.JSONDecodeError, TypeError):
            post_config_override = {}
            post_config_shape_valid = False
    if not post_config_shape_valid:
        post_config_override = {}
    officer_missing = object()
    durable_officer_cfg = post_config_override.get("officer", officer_missing)
    durable_officer_shape_valid = durable_officer_cfg is officer_missing or isinstance(
        durable_officer_cfg, dict
    )
    if durable_officer_cfg is officer_missing:
        durable_officer_cfg = {}
    if not durable_officer_shape_valid:
        durable_officer_cfg = {}
    llm_cfg = config_override.get("llm") or {}
    if not isinstance(llm_cfg, dict):
        llm_cfg = {}
    thread_id = row.get("thread_id")
    thread_status = row.get("thread_status")
    linked = bool(thread_id)
    commissioned = linked and thread_status not in (None, "ended")

    def _strict_auto_pull(config: dict[str, Any]) -> tuple[bool | None, bool]:
        if "auto_pull" not in config:
            return False, True
        value = config.get("auto_pull")
        if type(value) is bool:
            return value, True
        # A legacy string/number is not evidence that a downgrade is safe.
        # Do not let Python's ``1.0 == True`` equality collapse it into an
        # apparently valid boolean.
        return None, False

    durable_auto_pull, durable_auto_pull_scalar_valid = _strict_auto_pull(
        durable_officer_cfg
    )
    durable_auto_pull_valid = (
        post_config_shape_valid
        and durable_officer_shape_valid
        and durable_auto_pull_scalar_valid
    )
    if linked:
        runtime_auto_pull, runtime_auto_pull_scalar_valid = _strict_auto_pull(
            officer_cfg
        )
        runtime_auto_pull_valid = (
            commissioned
            and str(row.get("thread_project_id") or "")
            == str(row.get("project_id") or "")
            and metadata_shape_valid
            and config_override_shape_valid
            and officer_cfg_shape_valid
            and officer_cfg.get("enabled") is True
            and runtime_auto_pull_scalar_valid
        )
    else:
        runtime_auto_pull, runtime_auto_pull_valid = None, True
    mirror_consistent = (
        durable_auto_pull_valid
        and runtime_auto_pull_valid
        and (runtime_auto_pull is None or runtime_auto_pull == durable_auto_pull)
    )
    return {
        "project_id": str(row.get("project_id")),
        "project_name": row.get("project_name"),
        "thread_id": str(thread_id) if thread_id else None,
        "thread_status": thread_status,
        "commissioned": commissioned,
        "held": officer_cfg.get("hold") or None,
        "next_wake_at": iso_or_none(row.get("next_wake_at")),
        "pending_events": int(row.get("pending_events") or 0),
        "in_flight_jobs": int(row.get("in_flight_jobs") or 0),
        "auto_pull": (
            auto_pull_enabled(officer_cfg)
            if linked
            else auto_pull_enabled(durable_officer_cfg)
        ),
        # Safe, credential-free rollout evidence. A system administrator's
        # all-project roster is the supported downgrade preflight: pre-BP-01
        # binaries must not return until every durable value and every current
        # runtime mirror is false and mutually consistent.
        "auto_pull_durable": durable_auto_pull,
        "auto_pull_durable_valid": durable_auto_pull_valid,
        "auto_pull_runtime": runtime_auto_pull,
        "auto_pull_runtime_valid": runtime_auto_pull_valid,
        "auto_pull_mirror_consistent": mirror_consistent,
        "auto_pull_enable_available": dependencies.auto_pull_release_enabled(),
        "model": llm_cfg.get("model"),
        "last_activity_at": iso_or_none(row.get("last_agent_activity")),
    }


def iso_or_none(value: Any) -> str | None:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value) if value else None


async def list_officers(
    request: Request,
    *,
    dependencies: OfficerPostViewDependencies,
    user: dict[str, Any],
) -> dict[str, Any]:
    """Every post the caller can see, vacant ones included — the roster.

    Discovery for a Legate (or an assistant holding his credentials) who has
    more projects than officers: one call answers which projects have an
    officer, whether he is awake, held or vacant, and whether anything is
    waiting on him. Per-slot kit utilization stays on the per-project card,
    which computes it lineage-aware; this read stays cheap.
    """
    visible = await user_visible_project_ids(user, dependencies.store)
    if visible != "all" and not visible:
        return {
            "officers": [],
            "total": 0,
            "auto_pull_downgrade": {
                "scope": "visible_projects",
                "safe": False,
                "release_fence_closed": not dependencies.auto_pull_release_enabled(),
                "durable_enabled": 0,
                "runtime_enabled": 0,
                "invalid_values": 0,
                "mirror_mismatches": 0,
                "reason": "all_projects_admin_scope_required",
            },
        }
    rows = await dependencies.store.list_project_officer_posts(
        None if visible == "all" else sorted(str(pid) for pid in visible)
    )
    officers = [roster_officer_view(row, dependencies=dependencies) for row in rows]
    all_projects = visible == "all"
    durable_enabled = sum(bool(row["auto_pull_durable"]) for row in officers)
    runtime_enabled = sum(row["auto_pull_runtime"] is True for row in officers)
    invalid_values = sum(
        not row["auto_pull_durable_valid"] or not row["auto_pull_runtime_valid"]
        for row in officers
    )
    mirror_mismatches = sum(not row["auto_pull_mirror_consistent"] for row in officers)
    downgrade_safe = (
        all_projects
        and not dependencies.auto_pull_release_enabled()
        and durable_enabled == 0
        and runtime_enabled == 0
        and invalid_values == 0
        and mirror_mismatches == 0
    )
    return {
        "officers": officers,
        "total": len(officers),
        "auto_pull_downgrade": {
            "scope": "all_projects" if all_projects else "visible_projects",
            "safe": downgrade_safe,
            "release_fence_closed": not dependencies.auto_pull_release_enabled(),
            "durable_enabled": durable_enabled,
            "runtime_enabled": runtime_enabled,
            "invalid_values": invalid_values,
            "mirror_mismatches": mirror_mismatches,
            "reason": (
                None
                if downgrade_safe
                else (
                    "all_projects_admin_scope_required"
                    if not all_projects
                    else (
                        "release_fence_open"
                        if dependencies.auto_pull_release_enabled()
                        else "auto_pull_not_fully_disabled"
                    )
                )
            ),
        },
    }


async def can_manage_project_officer(
    user: dict[str, Any], project_id: str, *, dependencies: OfficerPostViewDependencies
) -> bool:
    """Server-owned owner/admin capability for Officer mutations."""
    if user.get("is_admin"):
        return True
    return (
        await dependencies.store.get_user_role_in_project(project_id, str(user["id"]))
        == "owner"
    )


async def officer_spend_today(
    thread_id: str | None, ceiling: int, *, dependencies: OfficerPostViewDependencies
) -> dict[str, Any]:
    """Today's session-token spend against the officer's daily ceiling (§8).

    The exact call the ceiling brake makes per wake
    (``session_wake._officer_ceiling_deferral``), reused for the card:
    ``usage_ledger.query_usage`` over today's UTC window, ``ref_id`` =
    officer thread, tokens-unit categories summed. ``tokens`` is 0 for a
    vacant post (nothing metered), None when metering is unavailable — the
    card can render an honest dash instead of a fake zero.
    """
    tokens: float | None = 0
    if thread_id:
        tokens = None
        try:
            if (
                dependencies.usage_ledger is not None
                and dependencies.usage_ledger.is_available
            ):
                now = datetime.now(timezone.utc)
                day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
                usage = await dependencies.usage_ledger.query_usage(
                    from_ts=day_start, to_ts=now, ref_id=str(thread_id)
                )
                tokens = llm_tokens_from_rows(usage.get("by_category") or [])
        except Exception:
            logger.warning(
                "officer spend: usage query failed (non-fatal)", exc_info=True
            )
            tokens = None
    try:
        ceiling_int = max(0, int(ceiling or 0))
    except (TypeError, ValueError):
        ceiling_int = 0
    return {"tokens": tokens, "ceiling": ceiling_int}


def officer_editor_block(
    officer_cfg: dict[str, Any], llm_cfg: dict[str, Any]
) -> dict[str, Any]:
    """The kit fields the card's editor seeds from, one shape for both card
    states (officer_post.md §8): live thread metadata when commissioned, the
    durable row config when vacant. Unset numerics stay null so the editor
    shows the true default provenance instead of a materialized fake."""

    def _num(key: str) -> int | None:
        value = officer_cfg.get(key)
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    return {
        "model": llm_cfg.get("model"),
        "reasoning_level": llm_cfg.get("reasoning_level"),
        "slots": officer_cfg.get("slots") or None,
        "sleep_minutes": {
            "min": _num("sleep_min_minutes") or 5,
            "max": _num("sleep_max_minutes") or 60,
        },
        "sleep_min_minutes": _num("sleep_min_minutes"),
        "sleep_max_minutes": _num("sleep_max_minutes"),
        "daily_token_ceiling": _num("daily_token_ceiling"),
        "max_actions_per_wake": _num("max_actions_per_wake"),
        "max_concurrent_workers": _num("max_concurrent_workers"),
    }


def while_vacant_view(state: Any) -> dict[str, Any]:
    """The row's while-vacant ledger in the card's shape:
    ``{entries: [{at?, job_id?, status?, title?}], dropped: n}``. ``title``
    is derived from the stored ``description`` so the storage shape (shared
    with the wake payloads) stays stable."""
    if not isinstance(state, dict):
        state = {}
    raw = state.get("while_vacant")
    entries = []
    if isinstance(raw, list):
        for entry in raw:
            if not isinstance(entry, dict):
                continue
            view = dict(entry)
            if view.get("title") is None and view.get("description"):
                view["title"] = view["description"]
            entries.append(view)
    try:
        dropped = max(0, int(state.get("while_vacant_dropped") or 0))
    except (TypeError, ValueError):
        dropped = 0
    return {"entries": entries, "dropped": dropped}


def officer_runtime_authorization_view(
    state: Any, *, commissioned: bool
) -> dict[str, Any]:
    """Safe Post projection of the durable runtime-grant incident."""

    if not isinstance(state, dict):
        state = {}
    incident = state.get("runtime_actor_incident")
    if not isinstance(incident, dict):
        return {"status": "authorized" if commissioned else "not_applicable"}
    if incident.get("status") == "open":
        notification = incident.get("notification")
        if not isinstance(notification, dict):
            notification = {}
        return {
            "status": "unavailable",
            "failure_class": str(incident.get("failure_class") or "unavailable"),
            "since": incident.get("first_failed_at"),
            "last_attempted_at": incident.get("last_failed_at"),
            "next_retry_at": incident.get("next_retry_at"),
            "operator_notification": str(notification.get("state") or "pending"),
            "planning_suppressed": True,
        }
    return {
        "status": "authorized" if commissioned else "not_applicable",
        "recovered_at": incident.get("resolved_at"),
        "planning_suppressed": False,
    }


async def get_project_officer_summary(
    request: Request,
    project_id: str,
    *,
    dependencies: OfficerPostViewDependencies,
    user: dict[str, Any],
) -> dict[str, Any]:
    """The project's post at a glance — the cockpit's officer card.

    officer_post.md §4/§8: always returns the post. ``commissioned`` /
    ``held`` / ``kit`` / ``incarnations`` / ``communication_policy`` /
    ``while_vacant`` come from the durable ``project_officers`` row; the
    ``officer`` block is ALWAYS present so the card's editor seeds from one
    place — live thread metadata when commissioned, the row's config when
    vacant (live-only fields null there). Kit utilization is lineage-aware:
    in-flight counts follow every incarnation on the post, not just the
    current thread.
    """
    can_manage = await can_manage_project_officer(
        user, project_id, dependencies=dependencies
    )

    post = await dependencies.store.get_or_create_project_officer(project_id) or {}
    officer = await dependencies.store.get_officer_thread_for_project(project_id)
    conference = await dependencies.find_open_conference_thread(project_id)
    conference_block = (
        {
            "thread_id": str(conference["id"]),
            "status": conference.get("status"),
        }
        if conference
        else None
    )

    # Lineage-aware utilization (officer_post.md §4): jobs dispatched by any
    # incarnation keep occupying their slots across decommission→recommission.
    lineage = await dependencies.store.get_project_officer_lineage(project_id)
    in_flight_by_slot: dict[Any, int] = {}
    if lineage:
        # Through the shared admission helper, not a local copy of its query.
        # This is the third place that count lived; when admission widened to
        # all-non-terminal, a stale copy here would show the Legate a free slot
        # the funnel then refuses with a 409.
        from orchestrator.services.officer_admission import count_in_flight_by_slot

        async with dependencies.store.acquire() as conn:
            in_flight_by_slot = await count_in_flight_by_slot(conn, lineage)

    # Ready depth per pool — the number the officer's queue duty is measured
    # against (officer_backlog_pools.md §6). Computed through the tick's own
    # eligibility path so the card cannot promise dispatches the tick will not
    # make; a KB outage leaves it absent rather than showing a false zero.
    ready_by_pool: dict[str, int] = {}

    def _kit_view(slots: Any) -> dict[str, Any] | None:
        """Roster spec + per-slot in-flight + pool depth, or None on a flat cap."""
        if not isinstance(slots, dict) or not slots:
            return None
        kit: dict[str, Any] = {}
        for name, spec in slots.items():
            entry = dict(spec) if isinstance(spec, dict) else {}
            entry["in_flight"] = int(in_flight_by_slot.get(name) or 0)
            if entry.get("category") and str(name) in ready_by_pool:
                depth = ready_by_pool[str(name)]
                entry["ready_depth"] = depth
                # The floor IS the slot count (§13.2). Rendered, not just
                # enforced: a policy the officer cannot see invites drift.
                entry["below_floor"] = depth < int(entry.get("count") or 0)
            kit[str(name)] = entry
        return kit

    row_officer_cfg = (post.get("config_override") or {}).get("officer") or {}
    if not isinstance(row_officer_cfg, dict):
        row_officer_cfg = {}
    row_llm_cfg = (post.get("config_override") or {}).get("llm") or {}
    if not isinstance(row_llm_cfg, dict):
        row_llm_cfg = {}
    provisioning_preflights = await dependencies.store.list_officer_job_preflights(
        project_id=project_id
    )
    knowledge_materialization = (
        await dependencies.store.list_knowledge_materialization_health(project_id)
    )
    floor_wakes = await dependencies.store.list_officer_floor_wake_outcomes(project_id)
    from orchestrator.services.job_liveness import get_liveness_policy
    from orchestrator.services.officer_backlog import auto_pull_enabled, pools_from_meta
    from orchestrator.services.officer_backlog import (
        ready_depth_by_pool as _ready_depth,
    )

    stale_claim_policy = get_liveness_policy().stale_claim.as_dict()
    post_block: dict[str, Any] = {
        # OC-10: a safe capability derived from the same current membership
        # authority as the mutation endpoints.  It is refreshed with every
        # summary poll; the owner/admin endpoint guards remain authoritative.
        "can_manage": can_manage,
        # OC-03 read surface: commissioned means a LIVE thread holds the post,
        # not that the link column is non-null. ``officer`` comes from the
        # post join with the non-ended filter, so it IS the live-post proof; a
        # stale link (a retire that predates the O3 decommission flow, or a
        # thread ended around the endpoint) must read as vacant — the old
        # bool(thread_id) form returned commissioned:true with an empty
        # officer block, and the card wedged on a state it cannot render.
        "commissioned": officer is not None,
        "held": None,
        "kit": None,
        "communication_policy": post.get("communication_policy") or {},
        "incarnations": post.get("incarnations") or [],
        "while_vacant": while_vacant_view(post.get("state")),
        "runtime_authorization": officer_runtime_authorization_view(
            post.get("state"), commissioned=officer is not None
        ),
        "runtime_lifecycle": {
            "observed_build_sha": None,
            "expected_build_sha": dependencies.persistent_provisioner.expected_build_sha,
            "drift_state": "unknown",
            "recycle_phase": "idle",
            "last_failure": None,
            "automatic_reconciliation_enabled": (
                dependencies.persistent_agent_reconciliation_enabled()
            ),
        },
        # Always present so the card never has to branch on shape. A vacant
        # post has no live counters — only the setting the next incarnation
        # will boot with.
        "backlog": {
            "auto_pull": auto_pull_enabled(row_officer_cfg),
            "auto_pull_control": {
                "enable_available": dependencies.auto_pull_release_enabled(),
                "source": "deployment_policy",
                "reason": (
                    None
                    if dependencies.auto_pull_release_enabled()
                    else "release_gate_closed"
                ),
            },
            "breakers": {},
            "stale_claims": [],
            "stale_claim_policy": stale_claim_policy,
            "worker_spend_ceiling_daily": row_officer_cfg.get(
                "worker_spend_ceiling_daily"
            ),
            "provisioning_preflights": provisioning_preflights,
            "knowledge_materialization": knowledge_materialization,
            "floor_wakes": floor_wakes,
        },
    }

    if not officer:
        # Vacant — including a stale ended-thread link, which now reads as
        # vacant on the ``commissioned`` flag too (OC-03). The editor seeds
        # from the row — the last real kit a decommission or the backfill
        # harvested there. The officer block is present but live-only fields
        # (thread_id, status, hold) are null.
        post_block["kit"] = _kit_view(row_officer_cfg.get("slots"))
        try:
            row_ceiling = int(row_officer_cfg.get("daily_token_ceiling") or 0)
        except (TypeError, ValueError):
            row_ceiling = 0
        return {
            "officer": {
                "thread_id": None,
                "status": None,
                "title": None,
                "created_at": None,
                "hold": None,
                **officer_editor_block(row_officer_cfg, row_llm_cfg),
            },
            "conference": conference_block,
            "spend_today": await officer_spend_today(
                None, row_ceiling, dependencies=dependencies
            ),
            **post_block,
        }

    officer_tid = str(officer["id"])
    officer_meta = thread_officer_meta(officer)
    metadata = officer.get("metadata") or {}
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
        except (json.JSONDecodeError, TypeError):
            metadata = {}
    officer_state = metadata.get("officer_state") or {}
    if not isinstance(officer_state, dict):
        officer_state = {}
    lifecycle_view = persistent_recycle_view(metadata)
    lifecycle_view["expected_build_sha"] = (
        dependencies.persistent_provisioner.expected_build_sha
    )
    lifecycle_view["automatic_reconciliation_enabled"] = (
        dependencies.persistent_agent_reconciliation_enabled()
    )
    if dependencies.persistent_provisioner.is_available:
        pod_status = await dependencies.persistent_provisioner.get_pod_status(
            officer_tid
        )
        if pod_status:
            lifecycle_view["observed_build_sha"] = pod_status.get("build_sha")
            lifecycle_view["drift_state"] = (
                "current"
                if not dependencies.persistent_provisioner.expected_build_sha
                or pod_status.get("build_sha")
                == dependencies.persistent_provisioner.expected_build_sha
                else "drifted"
            )
        elif lifecycle_view.get("recycle_phase") != "idle":
            lifecycle_view["drift_state"] = "missing"
    post_block["runtime_lifecycle"] = lifecycle_view

    timer = await dependencies.store.get_pending_officer_timer(officer_tid)
    async with dependencies.store.acquire() as conn:
        pending_events = await conn.fetchval(
            "SELECT COUNT(*) FROM session_wake_events "
            "WHERE thread_id = $1 AND state = 'pending'",
            UUID(officer_tid),
        )

    today = datetime.now(timezone.utc).date().isoformat()
    try:
        token_ceiling = int(officer_meta.get("daily_token_ceiling") or 0)
    except (TypeError, ValueError):
        token_ceiling = 0

    # Hold is thread-scoped runtime state (officer_post.md §5) — read live.
    raw_hold = officer_meta.get("hold")
    public_hold = (
        {k: v for k, v in raw_hold.items() if not str(k).startswith("_")}
        if isinstance(raw_hold, dict)
        else raw_hold
    )
    post_block["held"] = public_hold or None

    _pools = pools_from_meta(officer_meta)
    if _pools and dependencies.vector_store is not None:
        ready_by_pool = await _ready_depth(
            dependencies.store,
            dependencies.vector_store,
            project_id,
            _pools,
            caller="officer_summary",
        )

    post_block["kit"] = _kit_view(
        officer_meta.get("slots") or row_officer_cfg.get("slots")
    )
    # The backlog policies the tick enforces on his behalf. Breakers and stale
    # claims are already computed and stored by the tick — this only surfaces
    # them, so the card cannot disagree with what actually happened.
    post_block["backlog"] = {
        "auto_pull": auto_pull_enabled(officer_meta),
        "auto_pull_control": {
            "enable_available": dependencies.auto_pull_release_enabled(),
            "source": "deployment_policy",
            "reason": (
                None
                if dependencies.auto_pull_release_enabled()
                else "release_gate_closed"
            ),
        },
        "breakers": {
            pool: entry
            for pool, entry in (officer_state.get("backlog_breakers") or {}).items()
            if isinstance(entry, dict)
        },
        "stale_claims": officer_state.get("backlog_stale_claims") or [],
        "workspace_refusals": officer_state.get("backlog_workspace_refusals") or {},
        "stale_claim_policy": officer_state.get("backlog_stale_claim_policy")
        or stale_claim_policy,
        "worker_spend_ceiling_daily": officer_meta.get("worker_spend_ceiling_daily"),
        "provisioning_preflights": provisioning_preflights,
        "knowledge_materialization": knowledge_materialization,
        "floor_wakes": floor_wakes,
    }

    return {
        **post_block,
        "officer": {
            "thread_id": officer_tid,
            "status": officer.get("status"),
            "title": officer.get("title"),
            "created_at": officer.get("created_at"),
            "hold": public_hold or None,
            # The brain HIS judgment runs on (explicit override only — a null
            # means he's on the resolved session default, which the card
            # renders as exactly that) + the full editor numerics, live from
            # the thread's runtime projection.
            **officer_editor_block(
                officer_meta,
                (metadata.get("config_override") or {}).get("llm") or {},
            ),
        },
        "next_wake_at": (timer or {}).get("fire_at"),
        "pending_events": int(pending_events or 0),
        "token_ceiling": {
            "daily": token_ceiling,
            "deferred_today": officer_state.get("ceiling_notice") == today,
        },
        "spend_today": await officer_spend_today(
            officer_tid, token_ceiling, dependencies=dependencies
        ),
        # The officer's pages and digests are feed rows now — the card reads
        # GET /api/notifications?source_kind=thread&source_id=<officer_tid>.
        "conference": conference_block,
    }


__all__ = [
    "OfficerPostViewDependencies",
    "can_manage_project_officer",
    "get_project_officer_summary",
    "iso_or_none",
    "list_officers",
    "officer_editor_block",
    "officer_runtime_authorization_view",
    "officer_spend_today",
    "roster_officer_view",
    "while_vacant_view",
]
