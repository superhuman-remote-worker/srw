"""Officer actions on a worker-message route: reply, escalate, acknowledge.

Extracted verbatim from ``orchestrator.main`` (R1.B07 lane M, census group
``J_messaging``; officer_message_routing.md §4 — M3). All three are internal
routes whose caller must *be* the project's commissioned officer, and the
identity comes only from the verified runtime actor — never from a request
body.

Two fences travel with them and neither is re-derived:

* :func:`require_officer_route_actor` — internal key, then the job, then the
  project, then ``authorize_runtime_actor_request``. A job without a project
  has no chain of command and answers 403.
* :func:`officer_route_for_action` — the open route the action targets, plus
  the **incarnation fence**: a route addressed to a PREVIOUS officer thread is
  never adoptable by the current one (§5.1) — the drain already handed it to
  the user.

Reply delivers through the existing inbound-reply lane rather than a second
copy of it, which is why :class:`OfficerMessageActionDependencies` takes
``route_inbound_reply`` and ``record_route_reply_resolution`` as constructed
ports instead of importing them: the application binds them to
``services.inbound_reply`` with that service's own dependency object.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Awaitable, Callable

from fastapi import HTTPException, Request

from orchestrator.schemas.messaging import (
    OfficerMessageAckRequest,
    OfficerMessageEscalateRequest,
    OfficerMessageReplyRequest,
)


@dataclass
class OfficerMessageActionDependencies:
    """Collaborators for one officer route action, resolved per invocation."""

    store: Any
    notifier: Any
    require_internal: Callable[[Request], Awaitable[None]]
    authorize_runtime_actor_request: Callable[..., Awaitable[Any]]
    route_inbound_reply: Callable[..., Awaitable[tuple[str, int]]]
    record_route_reply_resolution: Callable[..., Awaitable[None]]


async def require_officer_route_actor(
    request: Request, job_id: str, *, dependencies: OfficerMessageActionDependencies
) -> tuple[dict[str, Any], dict[str, Any], int | None]:
    """Derive and authorize the current officer from hidden runtime identity.

    The internal-key gate is *not* here: it is the first statement of each of
    the three route declarations, where ``scripts/check_endpoint_auth.py`` can
    see it. It ran in exactly that position before the extraction too (this
    helper's first statement, reached as the handler's first statement), so the
    call and its order are unchanged — only the declaration site moved.
    """
    job = await dependencies.store.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")
    project_id = str(job["project_id"]) if job.get("project_id") else None
    if not project_id:
        raise HTTPException(
            status_code=403,
            detail="Job has no project — there is no officer chain of command.",
        )
    actor = await dependencies.authorize_runtime_actor_request(
        dependencies.store,
        request,
        action="officer_message",
        project_id=project_id,
    )
    # Downstream route-transition code consumes the historical row-shaped
    # ``{"id": thread_id}`` value.  The id now comes only from the verified
    # runtime actor, never from a public request body.
    officer = {"id": actor.thread_id}
    return job, officer, actor.officer_incarnation


async def officer_route_for_action(
    job_id: str,
    thread_id: str,
    officer_thread_id: str,
    *,
    dependencies: OfficerMessageActionDependencies,
) -> dict[str, Any]:
    """The open route an officer action targets, or a 409 explaining why not.

    The incarnation fence: a route addressed to a PREVIOUS officer thread is
    never adoptable by the current one (§5.1) — the drain already handed it
    to the user.
    """
    route = await dependencies.store.find_message_route_for_thread(
        job_id, thread_id, open_only=True
    )
    if not route:
        raise HTTPException(
            status_code=409,
            detail=(
                "No open worker-message route on this thread (it may already "
                "be resolved or timed out). For plain guidance use "
                "send_message_to_job."
            ),
        )
    if str(route.get("officer_thread_id") or "") != str(officer_thread_id):
        raise HTTPException(
            status_code=409,
            detail=(
                "This route is not addressed to the current officer "
                "incarnation (it predates this commission or is user-direct); "
                "it is not adoptable. For plain guidance use "
                "send_message_to_job."
            ),
        )
    return route


async def officer_reply_to_worker_message(
    request: Request,
    job_id: str,
    thread_id: str,
    body: OfficerMessageReplyRequest,
    *,
    dependencies: OfficerMessageActionDependencies,
) -> dict[str, Any]:
    """Answer a worker message as the commissioned officer. **Internal** —
    requires ``X-Internal-Key``; the caller must BE the project's officer.

    Delivers through the existing reply lane [A-reply] — a blocking route's
    worker resumes exactly once (job-status CAS) — and CAS-records
    ``resolved_by_officer`` on the route. The reply is guidance, never
    authorization: no approval/ready/claim side effects, and the original
    message is never erased.
    """
    job, officer, incarnation = await require_officer_route_actor(
        request, job_id, dependencies=dependencies
    )
    message = (body.message or "").strip()
    if not message:
        raise HTTPException(status_code=400, detail="message must not be empty")
    route = await officer_route_for_action(
        job_id, thread_id, str(officer["id"]), dependencies=dependencies
    )

    try:
        delivery_strategy, sequence = await dependencies.route_inbound_reply(
            job_id=job_id,
            thread_id=thread_id,
            message=f"[Answered by the project officer]\n\n{message}",
            resolver_kind="officer",
            resolver_id=str(officer["id"]),
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e

    # The blocking branch already recorded; this covers async routes (the
    # reply took a queued/guidance lane). CAS — a second record is a no-op.
    await dependencies.record_route_reply_resolution(
        job_id,
        thread_id,
        actor_kind="officer",
        actor_id=str(officer["id"]),
        note="officer replied",
    )
    refreshed = await dependencies.store.get_message_route(str(route["route_id"]))
    return {
        "status": "replied",
        "delivery_strategy": delivery_strategy,
        "sequence": sequence,
        "route_id": str(route["route_id"]),
        "route_state": (refreshed or route).get("state"),
    }


async def officer_escalate_worker_message(
    request: Request,
    job_id: str,
    thread_id: str,
    body: OfficerMessageEscalateRequest,
    *,
    dependencies: OfficerMessageActionDependencies,
) -> dict[str, Any]:
    """Escalate a worker message thread to the user with officer context.
    **Internal** — requires ``X-Internal-Key``; caller must BE the officer.

    Same ``thread_id`` keeps the reply/resume path: the user's answer
    resumes the worker directly. The original worker text and the officer's
    context are delivered clearly delimited (§7).
    """
    job, officer, incarnation = await require_officer_route_actor(
        request, job_id, dependencies=dependencies
    )
    route = await officer_route_for_action(
        job_id, thread_id, str(officer["id"]), dependencies=dependencies
    )
    if route.get("state") not in ("pending_officer", "pending_both"):
        raise HTTPException(
            status_code=409,
            detail=(f"Route is already {route.get('state')} — nothing to escalate."),
        )

    from orchestrator.services import message_routing as routing_svc

    outcome = await routing_svc.escalate_route(
        dependencies.store,
        route,
        reason="officer_escalated",
        actor_kind="officer",
        actor_id=str(officer["id"]),
        officer_thread_id=str(officer["id"]),
        officer_incarnation=incarnation,
        officer_context=body.context,
        expected_states=("pending_officer", "pending_both"),
        notifier=dependencies.notifier,
    )
    if not outcome["escalated"]:
        refreshed = await dependencies.store.get_message_route(str(route["route_id"]))
        state = (refreshed or {}).get("state")
        if state == "escalated_to_user":
            # The SLA reconciler won the CAS a moment earlier — the thread is
            # with the user either way.
            return {
                "status": "escalated",
                "delivered": bool((refreshed or {}).get("user_delivery_at")),
                "route_id": str(route["route_id"]),
                "note": "already escalated (officer SLA expired first)",
            }
        raise HTTPException(
            status_code=409,
            detail=f"Route changed before escalation (now: {state}).",
        )
    return {
        "status": "escalated",
        "delivered": outcome["delivered"],
        "route_id": str(route["route_id"]),
    }


async def officer_acknowledge_worker_message(
    request: Request,
    job_id: str,
    thread_id: str,
    body: OfficerMessageAckRequest,
    *,
    dependencies: OfficerMessageActionDependencies,
) -> dict[str, Any]:
    """Close an ASYNC worker message route without a reply. **Internal** —
    requires ``X-Internal-Key``; caller must BE the officer.

    Refused for blocking routes: a frozen worker needs an answer or an
    escalation, never a silent ack pretending nobody waited.
    """
    job, officer, incarnation = await require_officer_route_actor(
        request, job_id, dependencies=dependencies
    )
    route = await officer_route_for_action(
        job_id, thread_id, str(officer["id"]), dependencies=dependencies
    )
    if route.get("blocking"):
        raise HTTPException(
            status_code=400,
            detail=(
                "This worker is frozen waiting for an answer — use "
                "reply_to_job_message or escalate_job_message. Acknowledge is "
                "for async items only."
            ),
        )
    note = (body.note or "").strip()
    updated = await dependencies.store.transition_message_route(
        str(route["route_id"]),
        to_state="resolved_by_officer",
        expected_states=["pending_officer", "pending_both", "delivery_failed"],
        actor_kind="officer",
        actor_id=str(officer["id"]),
        officer_thread_id=str(officer["id"]),
        officer_incarnation=incarnation,
        note=f"acknowledged{': ' + note if note else ''}",
    )
    if not updated:
        refreshed = await dependencies.store.get_message_route(str(route["route_id"]))
        raise HTTPException(
            status_code=409,
            detail=(
                "Route changed before acknowledge "
                f"(now: {(refreshed or {}).get('state')})."
            ),
        )
    return {
        "status": "acknowledged",
        "route_id": str(route["route_id"]),
        "route_state": updated.get("state"),
    }


__all__ = [
    "OfficerMessageActionDependencies",
    "officer_acknowledge_worker_message",
    "officer_escalate_worker_message",
    "officer_reply_to_worker_message",
    "officer_route_for_action",
    "require_officer_route_actor",
]
