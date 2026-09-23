"""Officer sitrep — the computed state delta injected at every wake.

The minimal ``[SITREP]`` renderer in ``session_wake._format_officer_wake``
listed the wake *reasons*; this module answers the officer's standing question
— "what changed since I last looked?" — so a wake starts from facts instead of
a fan-out of list/get tool calls (knowledge-base/knowledge/features/centurion.md §4, S3).

Delta, not dashboard
--------------------
The sitrep diffs the project's job set against fingerprints stored on the
officer thread (``metadata.officer_state.sitrep``). The watermark advances
ONLY when the message was actually delivered to a live pod — a durable
transcript write is not a wake, and advancing on it would silently swallow
the delta the officer never read (implementation notes, risk 5). The caller
(``session_wake.drain_pending_event_wakes``) owns that rule: it merges the
returned state patch only on live-inject success.

Fingerprints are snapshot-diffs on status + audit step counts, deliberately
NOT ``jobs.updated_at`` — that column is poisoned by the gc trigger and
wake-state bookkeeping writes (schema comment on ``update_jobs_updated_at``).

Failure containment
-------------------
Every section renders inside its own try/except and degrades to a one-line
note (the ``stale_agent_detector._step`` isolation lesson: one poisoned
query must cost one section, not the wake). The audit DB and the usage
ledger are optional at startup, so their sections degrade cleanly to
"unavailable". A total failure returns ``(None, None)`` and the caller falls
back to the minimal renderer — a wake must never be lost to its formatter.
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional
from uuid import UUID

from shared.content_redaction import sanitize_text

logger = logging.getLogger(__name__)

# Line caps per section. The sitrep is a briefing, not a dump: full detail
# for what changed, one-liners for what didn't, tools for everything else.
_MAX_TRANSITIONS = 12
_MAX_NEW = 8
_MAX_ACTIVE = 8
_MAX_PENDING = 5
_DESC_CHARS = 120
# A Legate note is delivered whole (see _legate_lines); the cap only stops a
# pathological paste from crowding out the rest of the briefing.
_MAX_NOTE_CHARS = 4000
LEGATE_SOURCE = "legate"

# Statuses that count as "in flight" for the active-jobs detail and the
# capacity line. 'paused' is shown in transitions but does not hold a slot.
_ACTIVE_STATUSES = ("created", "processing")

_UNSET = object()


def _as_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except (ValueError, TypeError):
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _truncate(text: str, limit: int) -> str:
    text = " ".join(str(text).split())
    return text if len(text) <= limit else text[: limit - 1] + "…"


def _excerpt(text: Any, limit: int) -> str:
    """A bounded excerpt of worker-reachable text, redacted (audit OC-05).

    Job errors, worker message subjects, event summaries and sudo commands
    are written — or echoed — by a worker, and a failed push echoes its
    credential-bearing remote. Redacted BEFORE the cut: a truncation inside a
    credential leaves a fragment no pattern recognizes. The ``[REDACTED]``
    marker stays visible in the line, so a withheld value never reads as one
    the worker left out.
    """
    return _truncate(sanitize_text(str(text)), limit)


def _ago(ts: Optional[datetime], now: datetime) -> str:
    if ts is None:
        return "unknown"
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    seconds = max(0, int((now - ts).total_seconds()))
    if seconds < 90:
        return f"{seconds}s ago"
    minutes = seconds // 60
    if minutes < 120:
        return f"{minutes}m ago"
    hours = minutes // 60
    if hours < 48:
        return f"{hours}h ago"
    return f"{hours // 24}d ago"


def _parse_iso(value: Any) -> Optional[datetime]:
    if not value:
        return None
    try:
        ts = datetime.fromisoformat(str(value))
        return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


@dataclass(frozen=True)
class ReportingHandles:
    """The three read-only handles the sitrep's optional sections need.

    None of them is required: a missing audit reader, usage ledger or vector
    pool costs its own section and never the wake. That is why the default is
    an all-``None`` instance rather than an error — it is exactly the degraded
    path the sections already handle, and the shape a test process gets for
    free.
    """

    audit_reader: Any = None
    usage_ledger: Any = None
    vector_db: Any = None


_HANDLES = ReportingHandles()


def bind_reporting_handles(handles: ReportingHandles) -> None:
    """Hand this module the application's long-lived reporting handles.

    R1.B07 closed this module's two late ``import orchestrator.main`` lookups.
    The sitrep is rendered inside the officer wake drain, which is reached
    through ``kick_event_drain(db)`` from a dozen call sites that hold nothing
    but the store — so threading three more parameters through every one of
    them would move the coupling rather than remove it. The application binds
    them once at startup instead, the same shape
    ``notification_service.connect()`` already uses, and every entry point
    still accepts explicit overrides that win over whatever is bound.
    """
    global _HANDLES
    _HANDLES = handles


def reporting_handles() -> ReportingHandles:
    """What is currently bound (an all-``None`` instance before startup)."""
    return _HANDLES


def _resolve_handles() -> tuple[Any, Any]:
    """The bound audit reader and usage ledger."""
    return _HANDLES.audit_reader, _HANDLES.usage_ledger


def _resolve_vector_db() -> Any:
    """The bound vector DB pool (KB index home)."""
    return _HANDLES.vector_db


def prior_state(thread: dict[str, Any]) -> dict[str, Any]:
    """The stored sitrep state on an officer thread ({} when none)."""
    metadata = _as_dict(thread.get("metadata"))
    officer_state = _as_dict(metadata.get("officer_state"))
    return _as_dict(officer_state.get("sitrep"))


async def build_wake_message(
    db: Any,
    thread: dict[str, Any],
    rows: list[dict[str, Any]],
    *,
    audit_reader: Any = _UNSET,
    usage_ledger: Any = _UNSET,
    vector_db: Any = _UNSET,
) -> tuple[Optional[str], Optional[dict[str, Any]]]:
    """Render the full sitrep for one coalesced officer wake.

    Returns ``(text, state_patch)``. The patch is the new fingerprint state
    for ``merge_thread_officer_state(thread_id, patch)`` and MUST only be
    written after a successful live delivery. ``(None, None)`` means total
    failure — fall back to the minimal renderer.
    """
    try:
        if audit_reader is _UNSET or usage_ledger is _UNSET:
            resolved_audit, resolved_ledger = _resolve_handles()
            if audit_reader is _UNSET:
                audit_reader = resolved_audit
            if usage_ledger is _UNSET:
                usage_ledger = resolved_ledger
        if vector_db is _UNSET:
            vector_db = _resolve_vector_db()

        now = datetime.now(timezone.utc)
        thread_id = str(thread["id"])
        project_id = thread.get("project_id")
        project_id = str(project_id) if project_id else None
        prev = prior_state(thread)
        prev_prints = _as_dict(prev.get("fingerprints"))
        prev_watermark = _parse_iso(prev.get("watermark"))

        lines: list[str] = [_header(now, prev_watermark)]
        notes, reasons = [], []
        for row in rows:
            is_note = str(row.get("source") or "") == LEGATE_SOURCE
            (notes if is_note else reasons).append(row)
        lines += _legate_lines(notes)
        lines += _reason_lines(reasons)

        jobs_lines, fingerprints = await _jobs_section(
            db, audit_reader, project_id, prev_prints, prev_watermark, now
        )
        lines += jobs_lines
        lines += await _pending_section(db, thread_id, project_id, now)
        lines += await _worker_messages_section(db, project_id, now)
        lines += await _knowledge_section(db, vector_db, project_id)
        signals: dict[str, Any] = {}
        lines += await _capacity_section(
            db, thread, thread_id, vector_db, project_id, now, signals=signals
        )
        lines += await _fleet_section(db)
        lines += await _budget_section(thread, usage_ledger, project_id, now)

        lines.append(
            "Assess, act within your authority, then file a sleep. Use "
            "notify_user if something needs the Legate."
        )
        # Deliberately AFTER the closing instruction, and only when a pool is
        # actually starved. The last line of the wake is the one that gets
        # acted on, and for a long stretch the last thing this officer read
        # every wake was "file a sleep" — so an empty queue rendered as a
        # marker in the middle of the capacity line while the instruction to
        # sleep held the position that carries. Doctrine (persona
        # <backlog_doctrine> 1-2) already says an empty ready queue is waste
        # and that a starved pool is solicited or reported as dry; this puts
        # that duty where it is read, on exactly the wakes it applies to.
        starved = signals.get("below_floor") or []
        if starved:
            lines.append(
                f"{', '.join(starved)} {'is' if len(starved) == 1 else 'are'} "
                "BELOW FLOOR. Replenishing the queue is acting: arm a ticket, "
                "or tell the Legate the well is dry."
            )
        patch = {
            "sitrep": {
                "watermark": now.isoformat(),
                "fingerprints": fingerprints,
            }
        }
        return "\n".join(lines), patch
    except Exception:
        logger.exception("sitrep: build failed — falling back to minimal renderer")
        return None, None


def _header(now: datetime, prev_watermark: Optional[datetime]) -> str:
    stamp = now.strftime("%Y-%m-%d %H:%M UTC")
    if prev_watermark is None:
        return f"[SITREP] {stamp} — first sitrep (no prior watermark)"
    return f"[SITREP] {stamp} — delta since {_ago(prev_watermark, now)}"


def _legate_lines(rows: list[dict[str, Any]]) -> list[str]:
    """Render Legate notes verbatim, at the top, before anything else.

    Deliberately NOT routed through :func:`_reason_lines`: that renderer
    truncates a payload summary to 160 chars, which would silently amputate a
    directive delivered on the queued path. A note is the Legate speaking —
    it arrives whole, and it arrives first.
    """
    if not rows:
        return []
    lines = [
        f"Legate notes ({len(rows)}) — direct from your Legate; "
        "they outrank your standing plan:"
    ]
    for index, row in enumerate(rows, start=1):
        message = str(_as_dict(row.get("payload")).get("message") or "").strip()
        if not message:
            continue
        if len(message) > _MAX_NOTE_CHARS:
            message = (
                message[:_MAX_NOTE_CHARS]
                + f"\n[note truncated at {_MAX_NOTE_CHARS} chars]"
            )
        lines.append(f"[note {index}]")
        lines.append(message)
    return lines


def _reason_lines(rows: list[dict[str, Any]]) -> list[str]:
    lines = [f"Wake reasons ({len(rows)}):"]
    for row in rows:
        payload = _as_dict(row.get("payload"))
        source = str(row.get("source") or "event")
        if source == "timer":
            desc = f"timer: slept ~{payload.get('minutes')} min"
            if payload.get("reason"):
                desc += f" ({_excerpt(payload['reason'], 160)})"
        else:
            desc = f"{source}: {row.get('dedup_key')}"
            detail = payload.get("summary") or payload.get("description") or ""
            if detail:
                desc += f" — {_excerpt(detail, 160)}"
        lines.append(f"- {desc}")
    return lines


def _workspace_line(job: dict[str, Any]) -> str:
    """Coordinate-free workspace contract for Officer supervision text."""

    workspace = job.get("workspace_contract")
    if not isinstance(workspace, dict):
        return ""
    requested = workspace.get("requested_backend") or "default"
    assigned = workspace.get("assigned_backend") or "unknown"
    effective = workspace.get("effective_backend") or "unavailable"
    state = workspace.get("state") or "unknown"
    failure = f", failure={workspace['failure']}" if workspace.get("failure") else ""
    residue = (
        f", stale={workspace['stale_backend']}"
        if workspace.get("stale_backend")
        else ""
    )
    return (
        f" [workspace requested={requested}, assigned={assigned}, "
        f"effective={effective}, state={state}{failure}{residue}]"
    )


async def _jobs_section(
    db: Any,
    audit_reader: Any,
    project_id: Optional[str],
    prev_prints: dict[str, Any],
    prev_watermark: Optional[datetime],
    now: datetime,
) -> tuple[list[str], dict[str, Any]]:
    """Snapshot-diff of the project's jobs + audit-DB progress for active ones.

    Returns the rendered lines and the NEW fingerprints. On failure the
    PREVIOUS fingerprints are returned unchanged, so a transient query
    failure never wipes the baseline (which would replay every job as NEW on
    the next healthy sitrep).
    """
    if not project_id:
        return ["Jobs: no project bound to this officer thread."], dict(prev_prints)
    try:
        jobs = (
            await db.query_jobs(
                scope_project_id=project_id, limit=200, include_total=False
            )
        ).jobs
    except Exception:
        logger.warning("sitrep: jobs query failed", exc_info=True)
        return ["Jobs: (section unavailable — jobs query failed)"], dict(prev_prints)

    try:
        by_id = {str(j["id"]): j for j in jobs}
        active_ids = [
            jid for jid, j in by_id.items() if j.get("status") in _ACTIVE_STATUSES
        ]
        steps_by_job: dict[str, int] = {}
        if audit_reader is not None and active_ids:
            try:
                steps_by_job = await audit_reader.get_audit_counts(active_ids)
            except Exception:
                logger.warning("sitrep: audit step counts failed", exc_info=True)

        changed: list[str] = []
        new: list[str] = []
        from shared.job_outcome import effective_job_status

        for jid, job in by_id.items():
            status = effective_job_status(job)
            desc = _excerpt(job.get("description") or "", _DESC_CHARS)
            prev_entry = _as_dict(prev_prints.get(jid))
            if jid not in prev_prints:
                new.append(f"- NEW {jid[:8]} {status}{_workspace_line(job)} — {desc}")
            elif prev_entry.get("status") != status:
                line = f"- {jid[:8]} {prev_entry.get('status')} → {status} — {desc}"
                error = (job.get("error_message") or "").strip()
                if error and status in (
                    "failed",
                    "cancelled",
                    "blocked_undelivered",
                ):
                    line += f" | error: {_excerpt(error, 160)}"
                elif error and status == "pending_review":
                    # Why it is waiting on review instead of sealed — e.g. a
                    # seal held as delivery-unproven. Officer backlog jobs run
                    # at full autonomy, so without this the officer would see
                    # a bare transition and the claim would sit until the
                    # stale page.
                    line += f" | held: {_excerpt(error, 200)}"
                changed.append(line)

        # E3: the one shared liveness computation — the sitrep can never call
        # a job healthy while get_stuck_jobs calls it stuck (same threshold,
        # same audit/heartbeat authority order, updated_at never consulted).
        liveness_by_id: dict[str, dict[str, Any]] = {}
        if active_ids:
            try:
                from orchestrator.services.job_liveness import compute_jobs_liveness

                liveness_by_id = await compute_jobs_liveness(
                    [by_id[jid] for jid in active_ids[:_MAX_ACTIVE]],
                    audit_reader=audit_reader,
                    db=db,
                    now=now,
                )
            except Exception:
                logger.warning("sitrep: liveness computation failed", exc_info=True)

        active_lines: list[str] = []
        for jid in active_ids[:_MAX_ACTIVE]:
            job = by_id[jid]
            steps = steps_by_job.get(jid)
            prev_steps = _as_dict(prev_prints.get(jid)).get("steps")
            detail = f"- {jid[:8]} {job.get('status')}"
            liveness = liveness_by_id.get(jid)
            if liveness:
                detail += f" [{liveness.get('state')}]"
            detail += _workspace_line(job)
            if steps is not None:
                if isinstance(prev_steps, int):
                    detail += f" — steps {prev_steps}→{steps}"
                    if steps == prev_steps and prev_watermark is not None:
                        detail += " (NO PROGRESS since last sitrep)"
                else:
                    detail += f" — steps {steps}"
            last_write = _parse_iso((liveness or {}).get("last_activity_at"))
            if last_write is not None:
                detail += f", last activity {_ago(last_write, now)}"
            if liveness and liveness.get("state") in (
                "suspected_stuck",
                "unavailable",
            ):
                reasons = liveness.get("reasons") or []
                if reasons:
                    detail += f" ({_truncate('; '.join(reasons), 160)})"
            detail += f" — {_excerpt(job.get('description') or '', _DESC_CHARS)}"
            active_lines.append(detail)

        steady: dict[str, int] = {}
        for jid, job in by_id.items():
            status = effective_job_status(job)
            prev_entry = _as_dict(prev_prints.get(jid))
            if jid in prev_prints and prev_entry.get("status") == status:
                if status not in _ACTIVE_STATUSES:
                    steady[status] = steady.get(status, 0) + 1

        lines: list[str] = [f"Jobs ({len(jobs)} in project):"]
        if changed:
            lines.append("Transitions since last sitrep:")
            lines += changed[:_MAX_TRANSITIONS]
            if len(changed) > _MAX_TRANSITIONS:
                lines.append(f"- (+{len(changed) - _MAX_TRANSITIONS} more — list_jobs)")
        if new:
            lines += new[:_MAX_NEW]
            if len(new) > _MAX_NEW:
                lines.append(f"- (+{len(new) - _MAX_NEW} more new)")
        if active_lines:
            policy_view = next(iter(liveness_by_id.values()), {})
            threshold = policy_view.get("threshold_minutes")
            source = policy_view.get("threshold_source")
            if threshold is not None:
                lines.append(
                    "In flight "
                    f"(stall threshold {threshold}m, {source or 'unknown source'}):"
                )
            else:
                lines.append("In flight:")
            lines += active_lines
            if len(active_ids) > _MAX_ACTIVE:
                lines.append(f"- (+{len(active_ids) - _MAX_ACTIVE} more in flight)")
        if steady:
            summary = ", ".join(f"{n} {s}" for s, n in sorted(steady.items()))
            lines.append(f"Unchanged: {summary}.")
        if not (changed or new or active_lines or steady):
            lines.append("- none")

        fingerprints = {
            jid: {
                "status": effective_job_status(job),
                "steps": steps_by_job.get(jid),
            }
            for jid, job in by_id.items()
        }
        return lines, fingerprints
    except Exception:
        logger.warning("sitrep: jobs diff failed", exc_info=True)
        return ["Jobs: (section unavailable — diff failed)"], dict(prev_prints)


async def _pending_section(
    db: Any, thread_id: str, project_id: Optional[str], now: datetime
) -> list[str]:
    """Decisions waiting on the officer: sudo requests + session permission
    gates. Sudo rows expire in minutes (300s TTL) — surfacing them first is
    the point of the section."""
    lines: list[str] = []
    try:
        if project_id:
            async with db.acquire() as conn:
                sudo_rows = await conn.fetch(
                    """
                    SELECT s.id, s.job_id, s.command, s.request_type, s.expires_at
                      FROM sudo_approval_requests s
                      JOIN jobs j ON j.id = s.job_id
                     WHERE j.project_id = $1
                       AND s.status = 'pending'
                       AND s.expires_at > NOW()
                     ORDER BY s.requested_at
                     LIMIT $2
                    """,
                    UUID(project_id),
                    _MAX_PENDING,
                )
            for row in sudo_rows:
                remaining = int((row["expires_at"] - now).total_seconds())
                lines.append(
                    f"- sudo ({row['request_type']}) job {str(row['job_id'])[:8]}: "
                    f"{_excerpt(row['command'], 100)} — expires in {remaining}s"
                )
    except Exception:
        logger.warning("sitrep: sudo pending query failed", exc_info=True)
        lines.append("- (sudo pending unavailable)")
    try:
        async with db.acquire() as conn:
            perm_rows = await conn.fetch(
                """
                SELECT tool_name, requested_at
                  FROM thread_permission_requests
                 WHERE thread_id = $1 AND status = 'pending'
                   AND expires_at > NOW()
                 ORDER BY requested_at
                 LIMIT $2
                """,
                UUID(thread_id),
                _MAX_PENDING,
            )
        for row in perm_rows:
            lines.append(
                f"- permission gate: {row['tool_name']} "
                f"({_ago(row['requested_at'], now)})"
            )
    except Exception:
        logger.warning("sitrep: permission pending query failed", exc_info=True)
        lines.append("- (permission pending unavailable)")
    if not lines:
        return []
    return ["Pending on you:"] + lines


# Bounded probe so a hung vector pool costs seconds, not the wake.
_KNOWLEDGE_PROBE_TIMEOUT_SECONDS = 2.5


async def _worker_messages_section(
    db: Any, project_id: Optional[str], now: datetime
) -> list[str]:
    """Open worker→human message routes (officer_message_routing.md §5.1).

    The durable inbox: async officer-first messages carry no wake of their
    own — this section IS their delivery, coalesced into the next sitrep.
    Blocking routes appear too (their high-urgency wake may have been
    coalesced into this same turn); age + state let the officer triage.
    """
    if not project_id:
        return []
    try:
        routes = await db.list_open_worker_message_routes(project_id, limit=8)
    except Exception:
        logger.warning("sitrep: worker-message routes query failed", exc_info=True)
        return ["Worker messages: (section unavailable — routes query failed)"]
    if not routes:
        return []
    lines = [f"Worker messages ({len(routes)} open):"]
    for route in routes:
        job_id = str(route.get("job_id") or "")[:8]
        thread_id = str(route.get("thread_id") or "")
        state = str(route.get("state") or "?")
        created = route.get("created_at")
        age = _ago(created, now) if created is not None else "unknown"
        marker = "BLOCKING " if route.get("blocking") else ""
        subject = _excerpt(route.get("subject") or "(no subject)", 80)
        purpose = _as_dict(route.get("policy_snapshot")).get("purpose")
        label = f" [{purpose}]" if purpose else ""
        lines.append(
            f"- {marker}{state} — job {job_id} thread {thread_id}{label}, "
            f"{age}: {subject}"
        )
    lines.append(
        "Act with reply_to_job_message / escalate_job_message; close async "
        "items with acknowledge_job_message."
    )
    return lines


async def _knowledge_section(
    db: Any, vector_db: Any, project_id: Optional[str]
) -> list[str]:
    """Knowledge-plane availability for the wake (officer_knowledge_plane.md
    §3.1, K1).

    A vector/KB outage must not kill the officer, but the degradation must be
    VISIBLE: the wake says `project knowledge unavailable` so the officer
    knows KB reads/writes fail closed and does not reconstruct a shadow
    backlog in memory or thread prose. Healthy probes emit nothing — the
    richer knowledge-health surface (note counts, write/materialization
    provenance) is slice K4. When backlog-derived dispatch (`auto_pull`,
    officer_backlog_pools) ships, it must consult this same probe and fail
    closed during an outage instead of dispatching from a reconstructed
    queue.
    """
    if not project_id:
        return []
    lines: list[str] = []
    try:
        health = await db.list_knowledge_materialization_health(project_id)
        unresolved = [
            row
            for row in health
            if row.get("canonical_state") != "canonical"
            or row.get("projection_state") != "synced"
        ]
        if unresolved:
            counts: dict[str, int] = {}
            for row in unresolved:
                state = f"{row.get('canonical_state')}/{row.get('projection_state')}"
                counts[state] = counts.get(state, 0) + 1
            summary = ", ".join(f"{count} {state}" for state, count in counts.items())
            lines.append(
                "Knowledge sync: "
                f"{summary}; affected tickets remain ineligible until canonical "
                "and projection converge."
            )
    except Exception:
        logger.warning("sitrep: knowledge materialization query failed", exc_info=True)
        lines.append("Knowledge sync: (materialization state unavailable)")

    if vector_db is None:
        # No project (nothing to bind) or a deployment/test process without a
        # vector pool handle — absence of the handle is not evidence of an
        # outage, so stay silent rather than cry wolf on every wake.
        return lines
    try:

        async def _probe() -> None:
            async with vector_db.acquire() as conn:
                # knowledge_index is the KB's pgvector home; an empty project
                # returns no row, which is still a healthy probe.
                await conn.fetchval(
                    "SELECT 1 FROM knowledge_index WHERE project_id = $1 LIMIT 1",
                    UUID(project_id),
                )

        await asyncio.wait_for(_probe(), timeout=_KNOWLEDGE_PROBE_TIMEOUT_SECONDS)
        return lines
    except Exception:
        logger.warning("sitrep: knowledge availability probe failed", exc_info=True)
        return lines + [
            "Knowledge: project knowledge unavailable — KB reads/writes and "
            "backlog-derived dispatch fail closed. Keep supervising jobs and "
            "page if urgent; do NOT reconstruct the backlog from memory or "
            "conversation."
        ]


async def _capacity_section(
    db: Any,
    thread: dict[str, Any],
    thread_id: str,
    vector_db: Any = None,
    project_id: Optional[str] = None,
    now: Optional[datetime] = None,
    *,
    signals: Optional[dict[str, Any]] = None,
) -> list[str]:
    """Capacity/runtime lines, plus optional out-signals for the caller.

    ``signals`` is an out-parameter rather than a second return value so the
    section keeps its list-of-lines contract with every existing caller. The
    only key today is ``below_floor``: the starved pool names, which
    :func:`build` renders as the wake's final line. It is computed here
    because this is where ``ready_by_pool`` is queried — recomputing it in the
    caller would cost a second round-trip to say the same thing.
    """
    try:
        from orchestrator.services.officer_admission import count_in_flight_by_slot
        from orchestrator.services.officer_backlog import (
            pool_status_lines,
            pools_from_meta,
            ready_depth_by_pool,
        )
        from orchestrator.services.officer_slots import capacity_lines

        metadata = _as_dict(thread.get("metadata"))
        officer_meta = _as_dict(
            _as_dict(metadata.get("config_override")).get("officer")
        )
        officer_state = _as_dict(metadata.get("officer_state"))
        # Lineage-aware count (officer_post.md §4): jobs dispatched by a
        # prior incarnation on this post still occupy their slots, so the
        # officer's own capacity line matches what admission will enforce.
        lineage = await db.get_officer_capacity_lineage(thread_id)
        if not lineage:
            lineage = [thread_id]
        # Through the SAME helper admission uses. This query used to be inlined
        # here with an `IN ('created','processing')` predicate; when admission
        # widened to all-non-terminal, that copy would have shown the officer a
        # free slot the funnel then refused with a 409 — the capacity line has
        # to be the number he will actually be held to.
        async with db.acquire() as conn:
            in_flight_by_slot = await count_in_flight_by_slot(conn, lineage)

        ready_by_pool: Optional[dict[str, int]] = None
        pools = pools_from_meta(officer_meta)
        if pools and vector_db is not None and project_id:
            ready_by_pool = await ready_depth_by_pool(
                db,
                vector_db,
                str(project_id),
                pools,
                now=now,
                caller="sitrep",
            )
        if signals is not None:
            from orchestrator.services.officer_slots import below_floor_pools

            signals["below_floor"] = below_floor_pools(officer_meta, ready_by_pool)

        oldest_hours: Optional[float] = None
        try:
            claim = await db.get_oldest_open_officer_claim(lineage)
            created = _parse_iso(claim.get("created_at")) if claim else None
            if created is not None:
                oldest_hours = (
                    (now or datetime.now(timezone.utc)) - created
                ).total_seconds() / 3600.0
        except Exception:
            logger.warning("sitrep: oldest-claim age unavailable", exc_info=True)

        lines = [
            capacity_lines(
                officer_meta,
                in_flight_by_slot,
                ready_by_pool=ready_by_pool,
                oldest_claim_age_hours=oldest_hours,
            )
        ]
        from orchestrator.services.persistent_recycler import persistent_recycle_view

        runtime = persistent_recycle_view(metadata)
        if runtime.get("observed_build_sha") or runtime.get("expected_build_sha"):
            lines.append(
                "Runtime: "
                f"drift={runtime['drift_state']}; "
                f"recycle={runtime['recycle_phase']}; "
                f"observed={runtime.get('observed_build_sha') or 'missing'}; "
                f"expected={runtime.get('expected_build_sha') or 'unpinned'}"
                + (
                    f"; failure={runtime['last_failure']}"
                    if runtime.get("last_failure")
                    else ""
                )
                + "."
            )
        lines += pool_status_lines(officer_meta, officer_state, now)
        if project_id:
            try:
                preflights = await db.list_officer_job_preflights(
                    project_id=str(project_id), officer_thread_id=thread_id
                )
                if preflights:
                    states: dict[str, int] = {}
                    for row in preflights:
                        state = str(
                            _as_dict(row.get("context"))
                            .get("provisioning_preflight", {})
                            .get("state")
                            or "unknown"
                        )
                        states[state] = states.get(state, 0) + 1
                    rendered = ", ".join(
                        f"{count} {state}" for state, count in states.items()
                    )
                    lines.append(
                        "Provisioning: "
                        f"{rendered}; these jobs hold capacity but are not dispatchable."
                    )
            except Exception:
                logger.warning("sitrep: provisioning state unavailable", exc_info=True)
                lines.append("Provisioning: (state unavailable)")
            try:
                wakes = await db.list_officer_floor_wake_outcomes(str(project_id))
                if wakes:
                    latest = wakes[0]
                    state = latest.get("state") or "unknown"
                    pool = latest.get("pool") or "unknown"
                    lines.append(
                        "Floor wake: "
                        f"pool {pool} is {state}; attempted="
                        f"{bool(latest.get('last_attempted_at'))}, queued="
                        f"{bool(latest.get('last_queued_at'))}, delivered="
                        f"{bool(latest.get('delivered_at'))}."
                    )
            except Exception:
                logger.warning("sitrep: floor-wake state unavailable", exc_info=True)
                lines.append("Floor wake: (state unavailable)")
        return lines
    except Exception:
        logger.warning("sitrep: capacity query failed", exc_info=True)
        return ["Capacity: (unavailable)"]


async def _fleet_section(db: Any) -> list[str]:
    try:
        async with db.acquire() as conn:
            rows = await conn.fetch(
                "SELECT status, COUNT(*) AS n FROM agents GROUP BY status"
            )
        if not rows:
            return ["Fleet: no agents registered."]
        parts = ", ".join(f"{r['n']} {r['status']}" for r in rows)
        return [f"Fleet: {parts}."]
    except Exception:
        logger.warning("sitrep: fleet query failed", exc_info=True)
        return ["Fleet: (unavailable)"]


def _budget_visible(thread: dict[str, Any]) -> bool:
    """Ships OFF, flipped per century by the Legate.

    A spend figure with no spend policy attached is an invitation to author
    one. On 2026-08-16 this line read "$40" to an officer who had, one minute
    earlier, been ordered to dispatch two specific jobs; he filed both as
    ``blocked:budget-cap`` against a stop threshold nobody had set, and the
    Legate had to countermand him to get the night's work started. Cost is the
    project owner's lever, not the officer's — he sees it only when the owner
    hands it to him.
    """
    officer_meta = _as_dict(
        _as_dict(_as_dict(thread.get("metadata")).get("config_override")).get("officer")
    )
    return officer_meta.get("show_budget") in (True, "true", "True", 1)


async def _budget_section(
    thread: dict[str, Any],
    usage_ledger: Any,
    project_id: Optional[str],
    now: datetime,
) -> list[str]:
    if usage_ledger is None or not project_id or not _budget_visible(thread):
        return []
    try:
        from orchestrator.services.usage_ledger import llm_tokens_from_rows

        day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        usage = await usage_ledger.query_usage(
            from_ts=day_start, to_ts=now, scope_project_id=project_id
        )
        tokens = llm_tokens_from_rows(usage.get("by_category") or [])
        cost = usage.get("total_cost_usd") or 0.0
        return [f"Budget today (project): ${cost:.2f}, {int(tokens):,} tokens."]
    except Exception:
        logger.warning("sitrep: budget query failed", exc_info=True)
        return ["Budget today: (unavailable)"]
