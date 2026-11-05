"""The Officer's conference: one open embodiment per project, and its hold.

Extracted verbatim from ``orchestrator.main`` (R1.B07 lane O, census group
``S_OFFICER``; centurion.md §2/§4, officer_visibility_streamline.md §3.1).

A conference is the standing Officer's *embodiment* — a normal interactive
session wearing his identity via ``officer.conference``, with
``officer.enabled`` still false on it. The rules that travel with it:

* **One open conference per project** is the single-writer rule (§2):
  :func:`find_open_conference_thread` is what the create path reattaches to
  instead of minting a rival. An authorized retirement is irrevocable, so a
  thread with ``runtime_retirement_authorized_at`` set no longer counts
  whatever its status column says — otherwise a stuck retirement would lock the
  project out of conferences for as long as it stays stuck.
* **The conference thinks with his brain** (:func:`inherit_conference_brain`):
  request-provided ``llm`` keys win, absent ones are filled from the standing
  officer. Never raises — a vacant post means today's behaviour.
* **The hold is uniform** (:func:`hold_officer_for_conference`): the wake-claim
  query skips held threads entirely and the watchdog stands down. The live
  stand-by notice is best effort on top of that, not the mechanism.
* **Concluding is idempotent** (:func:`conclude_conference_if_any`): the brief
  wake dedups on the conference thread id, so the end hook and the watchdog's
  self-heal can both run. Never raises.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Optional

from orchestrator.services import officer_notices
from orchestrator.services.officer_metadata import thread_officer_meta
from orchestrator.services.officer_notices import OfficerNoticeDependencies

logger = logging.getLogger(__name__)


@dataclass
class OfficerConferenceDependencies:
    """Collaborators for one conference transition, resolved per invocation."""

    store: Any
    kick_officer_event_drain: Callable[[Any], None]

    def notice_dependencies(self) -> OfficerNoticeDependencies:
        """Dependencies for the sibling one-way delivery module."""
        return OfficerNoticeDependencies(store=self.store)


CONFERENCE_BRAIN_KEYS = ("model", "reasoning_level")


def inherit_conference_brain(
    config_override: dict, officer: Optional[dict]
) -> list[str]:
    """Fill the conference's ``llm`` gaps from the standing officer's brain.

    A conference is his embodiment, so it thinks with his model and effort
    unless the request says otherwise (officer_visibility_streamline.md §3.1,
    closing conference live-fire F2 — every conference used to boot on the
    platform default). Request-provided keys win; only absent ones are
    filled. Returns the keys inherited, for the log line. Never raises: a
    vacant post or a brainless officer means today's behavior.
    """
    if not officer:
        return []
    metadata = officer.get("metadata") or {}
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
        except (json.JSONDecodeError, TypeError):
            metadata = {}
    brain = (metadata.get("config_override") or {}).get("llm") or {}
    if not isinstance(brain, dict):
        return []
    inherited: list[str] = []
    for key in CONFERENCE_BRAIN_KEYS:
        value = brain.get(key)
        if not isinstance(value, str) or not value.strip():
            continue
        llm = config_override.setdefault("llm", {})
        if llm.get(key):
            continue
        llm[key] = value
        inherited.append(key)
    return inherited


def thread_is_conference(thread: dict) -> bool:
    """True for a conference embodiment (centurion.md §2/S9) — a normal
    interactive session wearing the officer's identity via
    ``officer.conference``; ``officer.enabled`` stays false on it."""
    return thread_officer_meta(thread).get("conference") in (True, "true")


async def find_open_conference_thread(
    project_id: str, *, dependencies: OfficerConferenceDependencies
) -> Optional[dict]:
    """The project's open (non-ended) conference thread, if any.

    One open conference per project is the single-writer rule (§2): the
    create path reattaches to this instead of minting a rival embodiment.

    An authorized retirement is irrevocable — the thread admits no further
    input — so it no longer counts, whatever its status column still says.
    Otherwise a retirement that cannot settle (a stuck runtime) would lock
    the project out of conferences for as long as it stays stuck.
    """
    async with dependencies.store.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT id, status, title, created_at FROM threads
             WHERE project_id = $1
               AND status <> 'ended'
               AND runtime_retirement_authorized_at IS NULL
               AND COALESCE(metadata->'config_override'
                            ->'officer'->>'conference','false') = 'true'
             ORDER BY created_at DESC
             LIMIT 1
            """,
            project_id,
        )
    return dict(row) if row else None


async def hold_officer_for_conference(
    project_id: str,
    conference_thread_id: str,
    *,
    dependencies: OfficerConferenceDependencies,
) -> None:
    """Stamp the background officer's conference hold (centurion.md §4).

    With the external timer the hold is uniform: the wake-claim query skips
    held threads entirely (events AND timer rows stay pending) and the
    watchdog stands down. A live stand-by notice is injected best-effort so
    a mid-turn officer parks politely instead of racing the meeting. No-op
    without an enabled officer — a conference on an officer-less project is
    just a session with the persona.
    """
    try:
        officer = await dependencies.store.get_officer_thread_for_project(project_id)
        if not officer or str(officer["id"]) == str(conference_thread_id):
            return
        officer_tid = str(officer["id"])
        hold_result = await dependencies.store.set_project_officer_hold(
            project_id,
            expected_thread_id=officer_tid,
            hold={
                "kind": "conference",
                "thread_id": str(conference_thread_id),
                "since": datetime.now(timezone.utc).isoformat(),
            },
            route_reason="officer_hold",
        )
        logger.info(
            "officer %s: conference hold stamped (conference %s)",
            officer_tid[:8],
            str(conference_thread_id)[:8],
        )
        await officer_notices.deliver_staged_officer_routes(
            hold_result.get("routes") or [],
            reason="officer_hold",
            dependencies=dependencies.notice_dependencies(),
        )
        officer_thread = await dependencies.store.get_thread(officer_tid)
        if officer_thread is not None:
            await officer_notices.inject_officer_notice(
                officer_thread,
                "[conference started — the Legate is meeting with your "
                "conference embodiment. Standing hold: take no scheduling "
                "actions; your timers and events queue durably and arrive "
                "with the session brief. If your backstop fires before "
                "the brief: sleep.]",
                dependencies=dependencies.notice_dependencies(),
            )
    except Exception:
        logger.exception("conference hold: stamping failed (non-fatal)")


async def conclude_conference_if_any(
    thread: dict, *, dependencies: OfficerConferenceDependencies
) -> None:
    """On a conference thread leaving service: release the officer's hold and
    enqueue the ``conference`` brief wake (centurion.md §4).

    The wake coalesces with everything that queued during the meeting —
    insert-dedup on the conference thread id makes end-hook and watchdog
    self-heal idempotent. The brief is a pointer, not a transcript: direction
    agreed in conference lands in the project stores (charter posture, KB,
    backlog), which the officer re-reads anyway. Never raises.
    """
    try:
        if not thread_is_conference(thread):
            return
        project_id = thread.get("project_id")
        if not project_id:
            return
        conf_tid = str(thread["id"])
        officer = await dependencies.store.get_officer_thread_for_project(
            str(project_id)
        )
        if not officer:
            return
        officer_tid = str(officer["id"])
        hold = thread_officer_meta(officer).get("hold") or {}
        if isinstance(hold, dict) and hold.get("thread_id") in (None, conf_tid):
            await dependencies.store.set_project_officer_hold(
                str(project_id),
                expected_thread_id=officer_tid,
                hold=None,
            )
        await dependencies.store.enqueue_session_wake_event(
            officer_tid,
            source="conference",
            dedup_key=conf_tid,
            payload={
                "conference_thread_id": conf_tid,
                "title": str(thread.get("title") or ""),
                "summary": (
                    "conference concluded — direction agreed there is now in "
                    "force. Re-read the charter posture and any KB/backlog "
                    "notes updated during the meeting before your next "
                    "scheduling decision."
                ),
            },
            project_id=str(project_id),
        )
        dependencies.kick_officer_event_drain(dependencies.store)
        logger.info(
            "conference %s concluded — officer %s hold released, brief wake enqueued",
            conf_tid[:8],
            officer_tid[:8],
        )
    except Exception:
        logger.exception("conference end: brief wake failed (non-fatal)")


__all__ = [
    "CONFERENCE_BRAIN_KEYS",
    "OfficerConferenceDependencies",
    "conclude_conference_if_any",
    "find_open_conference_thread",
    "hold_officer_for_conference",
    "inherit_conference_brain",
    "thread_is_conference",
]
