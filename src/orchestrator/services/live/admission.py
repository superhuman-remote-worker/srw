"""Live-session admission; knowledge-base/knowledge/features/live_session_jobs.md §3, §4.2.

A live job starts inside a live session (a phone call): ``live_session: true``
plus ``execution.session {connector, id}`` naming one of the job's
connectors and the provider's session id. Admission refuses only the
contradictions (§2): a live job without a session, a session without
``live_session``, a session connector that is not among the job's
connectors or is no live connector, an expert with no live voice, and a
session another job already holds.

The stage runs after the job's connectors are resolved. It ensures the
session's row, puts the reserved ``context.live_session`` on the job and
binds the session to the job in the job's own INSERT transaction
(:meth:`LiveAdmission.on_created`). After creation :func:`hand_off` gives the
session to the connector's pod, which accepts it at the engine; a handoff
that fails marks the session failed, and the job, which starts tactical,
sees it ended and handles it as a missed call.
"""

from __future__ import annotations

import asyncio
import copy
import json
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Awaitable, Callable

from fastapi import HTTPException

from orchestrator.services.live.instructions import (
    LiveVoice,
    LiveVoiceError,
    catalog_voice,
    idle_instruction,
    live_voice_of,
    with_call_time,
    with_default_voice,
    with_engine_rules,
)
from orchestrator.services.live.pods import LivePodClient, LivePodError
from orchestrator.services.live.store import LiveSessionStore
from shared.connectors.builtin import spec_for_type
from shared.runtime.live.contract import SessionStatus

logger = logging.getLogger(__name__)

#: The reserved job context key the server writes (callers' copies are
#: stripped at ingress).
CONTEXT_KEY = "live_session"
#: SIP status the pod rejects a session SRW will not take with.
REJECT_STATUS = 480


@dataclass(frozen=True)
class LiveAdmissionDependencies:
    db: Any
    resolve_connector_refs: Callable[..., Awaitable[list[str]]]
    pods: LivePodClient
    #: Who names the session: ``api`` (a REST caller), ``manifest``, or
    #: ``webhook`` (SRW received it; the row exists already).
    session_origin: str = "api"


@dataclass
class LiveAdmission:
    session: dict[str, Any]
    connector: dict[str, Any]
    voice: LiveVoice
    sessions: LiveSessionStore

    @property
    def context_value(self) -> dict[str, Any]:
        return {
            "session_id": str(self.session["id"]),
            "connector_id": str(self.connector["id"]),
            "provider_session_id": self.session["provider_session_id"],
        }

    async def on_created(self, conn: Any, job_id: str) -> None:
        """Binds the session in the job's INSERT transaction: one session,
        one job."""
        if not await self.sessions.bind_job(conn, self.session["id"], job_id):
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "live_session_bound",
                    "message": "This live session is already bound to a job",
                },
            )


def refuse_session_contradictions(command: Any) -> tuple[str, str] | None:
    """``(alias, provider session id)`` of a live job, ``None`` for any
    other job; 422 for a contradiction in the request itself."""
    execution = getattr(command, "execution", None)
    session = getattr(execution, "session", None) if execution else None
    live = bool(getattr(command, "live_session", False))
    if not live and session is None:
        return None
    if live and session is None:
        raise HTTPException(
            422,
            "A live-session job names its session: execution.session {connector, id}",
        )
    if not live:
        raise HTTPException(
            422, "execution.session names a live session: set live_session: true"
        )
    if session.connector not in execution.connectors:
        raise HTTPException(
            422,
            f"execution.session.connector {session.connector!r} is not a key of "
            "execution.connectors",
        )
    return session.connector, session.id


async def resolve_live_voice(
    db: Any,
    *,
    config_name: str | None,
    expert_id: str | None,
    expert_row: dict[str, Any] | None,
    config_override: dict[str, Any] | None,
) -> LiveVoice:
    """The live voice of the job's expert, as dispatch will resolve it."""
    from orchestrator.services.config_resolver import resolve_config
    from shared.runtime.core.loader import canonical_config_name
    from shared.runtime.core.subagent_roster import RosterResolutionError

    row = expert_row
    if row is None and expert_id:
        row = await db.get_expert_by_id(str(expert_id))
    try:
        blob = await asyncio.to_thread(
            resolve_config,
            base_config_name=canonical_config_name(config_name or "worker_base"),
            expert_row=copy.deepcopy(row) if row else None,
            request_override=copy.deepcopy(config_override)
            if config_override
            else None,
            expert_type="worker",
            db_refs={},
        )
    except RosterResolutionError as exc:
        # An authoring error in the merged roster (a `live` block on an entry
        # that is not `driver: live`): its message names the entry and the fix.
        raise LiveVoiceError(
            f"The expert's subagent roster is invalid: {exc}"
        ) from None
    except Exception as exc:
        logger.warning("Live admission: the expert's config did not resolve (%s)", exc)
        raise LiveVoiceError(
            f"The expert's config could not be resolved ({type(exc).__name__})"
        ) from None
    return live_voice_of(blob.get("agent") or {})


async def line_default_voice(db: Any, connector: dict[str, Any]) -> str | None:
    """The voice the line's ``live_voice`` catalog row names, if any.

    Never fails admission: an unreadable row leaves the built-in default."""
    model = (connector.get("config") or {}).get("model")
    if not isinstance(model, str) or not model:
        return None
    try:
        row = await db.resolve_catalog_model(model, capability="live_voice")
    except Exception:
        logger.warning("Live admission: the catalog row of %s was not read", model)
        return None
    return catalog_voice(row)


async def prepare_live_session(
    *,
    command: Any,
    datasource_ids: list[str],
    effective_user_id: str | None,
    project_id: str | None,
    config_name: str | None,
    expert_id: str | None,
    expert_row: dict[str, Any] | None,
    config_override: dict[str, Any] | None,
    dependencies: LiveAdmissionDependencies,
) -> LiveAdmission | None:
    named = refuse_session_contradictions(command)
    if named is None:
        return None
    alias, provider_session_id = named
    from orchestrator.schemas.execution_selection import ExecutionSelection

    one = ExecutionSelection(connectors={alias: command.execution.connectors[alias]})
    ids = await dependencies.resolve_connector_refs(
        one,
        owner_id=str(effective_user_id) if effective_user_id else None,
        project_id=project_id,
    )
    connector_id = ids[0] if ids else None
    if connector_id is None or connector_id not in {str(i) for i in datasource_ids}:
        raise HTTPException(403, "One or more selected connectors are unavailable")
    connector = await dependencies.db.get_datasource(str(connector_id))
    spec = spec_for_type((connector or {}).get("type"))
    if connector is None or spec is None or not spec.live:
        raise HTTPException(
            422,
            f"execution.session.connector {alias!r} is not a live connector "
            "(a phone line)",
        )
    try:
        voice = await resolve_live_voice(
            dependencies.db,
            config_name=config_name,
            expert_id=expert_id,
            expert_row=expert_row,
            config_override=config_override,
        )
    except LiveVoiceError as exc:
        raise HTTPException(422, str(exc)) from None
    if not voice.voice_named:
        voice = with_default_voice(
            voice, await line_default_voice(dependencies.db, connector)
        )
    sessions = LiveSessionStore(dependencies.db)
    async with dependencies.db.acquire() as conn:
        session = await sessions.ensure_session(
            conn,
            connector_id=str(connector_id),
            provider_session_id=provider_session_id,
            origin=dependencies.session_origin,
        )
    if session.get("job_id") is not None:
        raise HTTPException(
            409,
            detail={
                "code": "live_session_bound",
                "message": "This live session is already bound to a job",
            },
        )
    if session["status"] not in (SessionStatus.RINGING, SessionStatus.ACTIVE):
        raise HTTPException(
            409,
            detail={
                "code": "live_session_over",
                "message": f"This live session is {session['status']}",
            },
        )
    return LiveAdmission(
        session=session, connector=connector, voice=voice, sessions=sessions
    )


def handoff_body(
    admission: LiveAdmission, job_id: str, *, now: datetime | None = None
) -> dict[str, Any]:
    """What the line's pod accepts the session with; the instructions end
    with the engine's delegation rules and the call's date and time (``now``,
    the handoff by default)."""
    config = admission.connector.get("config") or {}
    voice = admission.voice
    model = str(config.get("model") or "")
    return {
        "session_id": str(admission.session["id"]),
        "provider_session_id": admission.session["provider_session_id"],
        "job_id": str(job_id),
        "model": model,
        "accept": {
            "instructions": with_call_time(
                with_engine_rules(voice.instructions, model), now
            ),
            "voice": voice.voice,
        },
        "greeting_instruction": voice.greeting_instruction,
        "idle_instruction": idle_instruction(model),
        "texts": {"fallback": voice.fallback},
        "limits": {
            "max_duration_s": int(config.get("max_duration_s") or 900),
            "no_job_fallback_s": int(config.get("no_job_fallback_s") or 8),
        },
        "transfer_target": config.get("transfer_target"),
    }


async def hand_off(
    admission: LiveAdmission,
    job: dict[str, Any],
    pods: LivePodClient,
    *,
    automation_id: str | None = None,
) -> dict[str, Any]:
    """Give the admitted session to the connector's pod. Never raises: a
    failure marks the session failed (the job sees it ended)."""
    sessions = admission.sessions
    session_id = str(admission.session["id"])
    body = handoff_body(admission, str(job["id"]))
    if automation_id is None:
        context = job.get("context")
        if isinstance(context, str):
            # A row read back with get_job: its jsonb context is text. (The
            # row create_job returns has no context at all; the inbound
            # path notes its automation itself.)
            try:
                context = json.loads(context)
            except ValueError:
                context = None
        context = context if isinstance(context, dict) else {}
        automation_id = context.get("automation_id")
    await sessions.note_admission(
        session_id,
        automation_id=automation_id,
        project_id=job.get("project_id"),
        model=body["model"] or None,
        handoff=body,
    )
    try:
        await pods.handoff(str(admission.connector["id"]), body)
    except LivePodError as exc:
        logger.warning(
            "Live session %s not handed to its pod: %s (HTTP %s)",
            session_id,
            exc,
            exc.status,
        )
        _, row = await sessions.transition(
            session_id, SessionStatus.FAILED, error=f"handoff failed: {exc}"
        )
        return row or admission.session
    except Exception as exc:
        # Never raises: the job is created and bound by now, so any other
        # failure to reach the pod also ends the session (its job sees it).
        logger.exception("Live session %s not handed to its pod", session_id)
        _, row = await sessions.transition(
            session_id,
            SessionStatus.FAILED,
            error=f"handoff failed: {type(exc).__name__}",
        )
        return row or admission.session
    _, row = await sessions.transition(session_id, SessionStatus.ACTIVE)
    return row or admission.session
