"""Inbound live sessions through a live automation; live_session_jobs.md §3.1, §4.4.

The provider posts an incoming session (OpenAI's ``live.transport.incoming``)
to the connector's public webhook. It is verified against the connector's
``webhook_secret``, recorded ``ringing`` (a redelivery is a no-op) and
answered at once; a background task then finds the connector's enabled live
automation, admits its job as the automation's owner through the normal job
admission (which binds the session and hands it to the connector's pod), and
records the fire. No automation, a spent daily budget or a refused admission
rejects the session with SIP 480 through the pod.

An external server that receives the provider's webhook itself does not come
here: it creates the live job with ``POST /api/jobs`` (§3.2).
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Mapping
from uuid import UUID, uuid4

from fastapi import HTTPException

from orchestrator.services.application_tasks import drain_task_mapping
from orchestrator.services.live.admission import REJECT_STATUS
from orchestrator.services.live.pods import LivePodClient, LivePodError
from orchestrator.services.live.store import LiveSessionStore
from shared.connectors.builtin import spec_for_type
from shared.runtime.live.contract import SessionStatus
from shared.runtime.live.webhook import (
    WebhookVerificationError,
    parse_incoming_call,
    verify_webhook,
)

logger = logging.getLogger(__name__)

TRIGGER = "live"


class LiveTaskRegistry:
    """Identity-guarded per-app admission tasks (one per session)."""

    def __init__(self) -> None:
        self.owner = uuid4().hex
        self.tasks: dict[str, asyncio.Task] = {}
        self.stopping = False

    def start(self, key: str, coroutine: Any) -> asyncio.Task | None:
        existing = self.tasks.get(key)
        if self.stopping or (existing is not None and not existing.done()):
            coroutine.close()
            return existing
        task = asyncio.create_task(coroutine, name=f"live-{key}")
        self.tasks[key] = task

        def done(completed: asyncio.Task) -> None:
            if self.tasks.get(key) is completed:
                self.tasks.pop(key, None)
            if not completed.cancelled() and completed.exception() is not None:
                logger.error(
                    "Live task %s failed (%s)",
                    key,
                    type(completed.exception()).__name__,
                )

        task.add_done_callback(done)
        return task

    async def drain(self) -> None:
        self.stopping = True
        try:
            await drain_task_mapping(self.tasks)
        finally:
            self.stopping = False


@dataclass(frozen=True)
class LiveInboundDependencies:
    db: Any
    pods: LivePodClient
    tasks: LiveTaskRegistry
    #: Admits a job as its owner (trusted in-process admission, as the job
    #: bench does): ``(owner_id, JobCreate) -> job row``.
    admit_as_owner: Callable[[str, Any], Awaitable[dict[str, Any]]]

    @property
    def sessions(self) -> LiveSessionStore:
        return LiveSessionStore(self.db)


def _detail(exc: HTTPException) -> str:
    detail = exc.detail
    if isinstance(detail, Mapping):
        detail = detail.get("message") or detail.get("code") or str(detail)
    return str(detail)[:500]


async def live_connector(db: Any, connector_id: Any) -> dict[str, Any] | None:
    try:
        UUID(str(connector_id))
    except ValueError:
        return None
    connector = await db.get_datasource(str(connector_id))
    spec = spec_for_type((connector or {}).get("type"))
    if connector is None or spec is None or not spec.live:
        return None
    return connector


async def receive_webhook(
    connector_id: str,
    headers: Mapping[str, str],
    body: bytes,
    dependencies: LiveInboundDependencies,
) -> dict[str, Any]:
    connector = await live_connector(dependencies.db, connector_id)
    if connector is None:
        raise HTTPException(404, "Not found")
    credentials = connector.get("credentials") or {}
    secret = (
        credentials.get("webhook_secret") if isinstance(credentials, Mapping) else None
    )
    if not isinstance(secret, str) or not secret:
        raise HTTPException(401, "Webhook verification failed")
    try:
        payload = verify_webhook(secret, headers, body)
    except WebhookVerificationError:
        raise HTTPException(401, "Webhook verification failed") from None
    webhook_id = next(
        (value for key, value in headers.items() if key.lower() == "webhook-id"), ""
    )
    try:
        incoming = parse_incoming_call(payload, webhook_id)
    except ValueError:
        raise HTTPException(400, "Malformed incoming session") from None
    if incoming is None:
        return {"received": True}
    session = await dependencies.sessions.insert_incoming(
        str(connector["id"]), incoming
    )
    if session is None:
        return {"received": True}
    dependencies.tasks.start(
        f"live-admit-{session['id']}",
        start_inbound(session, connector, dependencies),
    )
    return {"received": True}


def job_from_automation(automation: Mapping[str, Any], provider_session_id: str) -> Any:
    """The live job a live automation fires for one session."""
    from orchestrator.schemas.job_create import JobCreate

    shape = automation.get("job_shape") or {}
    execution = dict(shape.get("execution") or {})
    connectors = dict(execution.get("connectors") or {})
    primary = shape.get("primary_connector")
    config_override = dict(automation.get("config_override") or {})
    if "autonomy" not in config_override and automation.get("autonomy"):
        config_override["autonomy"] = automation["autonomy"]
    expert = (
        str(automation["expert_id"])
        if automation.get("expert_id")
        else (str(automation.get("expert") or "") or None)
    )
    if expert == "worker_base":
        expert = None
    body: dict[str, Any] = {
        "description": automation.get("prompt") or "",
        "expert": expert,
        "config_override": config_override or None,
        "priority": automation.get("priority") or 5,
        "project_id": (
            str(automation["project_id"]) if automation.get("project_id") else None
        ),
        "context": {
            "automation_id": str(automation["id"]),
            "automation_name": automation.get("name") or "",
            "automation_trigger": TRIGGER,
        },
        "execution": {
            "connectors": connectors,
            "session": {"connector": primary, "id": provider_session_id},
        },
        "live_session": True,
    }
    if "workspace" in shape:
        # The tri-state survives: a missing key is the default, null is none.
        body["workspace"] = shape["workspace"]
    return JobCreate(**body)


async def _reject(
    dependencies: LiveInboundDependencies,
    session: Mapping[str, Any],
    connector: Mapping[str, Any],
    *,
    status: SessionStatus,
    reason: str,
) -> None:
    try:
        await dependencies.pods.reject(
            str(connector["id"]),
            str(session["id"]),
            session["provider_session_id"],
            REJECT_STATUS,
        )
    except LivePodError as exc:
        logger.warning(
            "Live session %s not rejected at the pod: %s", session["id"], exc
        )
    if status == SessionStatus.REJECTED:
        await dependencies.sessions.transition(
            session["id"], status, close_reason=reason
        )
    else:
        await dependencies.sessions.transition(session["id"], status, error=reason)


async def start_inbound(
    session: Mapping[str, Any],
    connector: Mapping[str, Any],
    dependencies: LiveInboundDependencies,
) -> None:
    db = dependencies.db
    automation = await db.live_automation_for_connector(str(connector["id"]))
    if automation is None:
        logger.info(
            "Live session %s rejected: no enabled live automation on its connector",
            session["id"],
        )
        await _reject(
            dependencies,
            session,
            connector,
            status=SessionStatus.REJECTED,
            reason="no_automation",
        )
        return
    automation_id = str(automation["id"])
    if not await db.claim_live_automation_fire(automation_id):
        await db.record_live_automation_fire(
            automation_id, job_id=None, status="refused: daily fire limit reached"
        )
        await _reject(
            dependencies,
            session,
            connector,
            status=SessionStatus.REJECTED,
            reason="fire_limit",
        )
        return
    try:
        command = job_from_automation(automation, session["provider_session_id"])
        job = await dependencies.admit_as_owner(str(automation["owner_id"]), command)
    except HTTPException as exc:
        reason = _detail(exc)
        logger.warning(
            "Live session %s: the automation's job was refused (HTTP %s)",
            session["id"],
            exc.status_code,
        )
        await db.record_live_automation_fire(
            automation_id, job_id=None, status=f"refused: {reason}"
        )
        await _reject(
            dependencies,
            session,
            connector,
            status=SessionStatus.FAILED,
            reason=f"admission refused: {reason}",
        )
        return
    except Exception as exc:
        logger.exception("Live session %s: admission failed", session["id"])
        await db.record_live_automation_fire(
            automation_id, job_id=None, status=f"failed: {type(exc).__name__}"
        )
        await _reject(
            dependencies,
            session,
            connector,
            status=SessionStatus.FAILED,
            reason=f"admission failed: {type(exc).__name__}: {str(exc)[:300]}",
        )
        return
    # The created job row carries no context, so the handoff cannot name the
    # automation that fired: the session learns it here.
    await dependencies.sessions.note_admission(
        session["id"], automation_id=automation_id
    )
    await db.record_live_automation_fire(
        automation_id, job_id=str(job["id"]), status=None
    )
