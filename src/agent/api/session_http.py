"""HTTP transport for an attached persistent session.

Owns request parsing, field validation, the internal-key check for durable
event input, the mapping of admission refusals to status codes, and the
acknowledgement shapes of ``/api/input``, ``/api/interrupt`` and
``/api/approve``. The runtime keeps admission, the interrupt state and
permission decisions; it supplies them through :class:`SessionHttpPorts`.

Both the persistent-mode and dual-mode applications register these routes
with :func:`register_session_http_routes`; dual mode adds its pod-state
precheck. The orchestrator forwards ``POST /api/threads/{id}/{input,
interrupt,approve/{approval_id}}`` here, and SSE clients (Cockpit, MCP, curl)
drive a session without a WebSocket.
"""

from __future__ import annotations

import hmac
import json
import logging
import os
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, Sequence

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response

from agent.api.session_contract import (
    ACCEPTED_INPUT_ROLES,
    DurableInputUnavailable,
    ProtectedCloudUnavailable,
    SessionIdentityMismatch,
    SessionOperations,
    SessionRuntimeView,
    TerminationAdmissionClosed,
    accepted_input_payload,
    canonical_session_identity_fingerprint,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class SessionHttpPorts:
    runtime: SessionRuntimeView
    operations: SessionOperations


def stateless_rejection() -> JSONResponse:
    """409 for direct-session verbs on a stateless executor pod."""
    return JSONResponse(
        {
            "error": (
                "stateless executor: this pod serves queued turns from the "
                "run_queue (threads.execution_lane='stateless'); direct "
                "session attach/input is not accepted here"
            )
        },
        status_code=409,
    )


def termination_rejection() -> JSONResponse:
    """Stable, base64/secret-free retry contract for direct injectors."""

    return JSONResponse(
        {
            "error": "runtime_terminating",
            "retryable": True,
            "message": "Persistent runtime is terminating; retry on its replacement.",
        },
        status_code=503,
        headers={"Retry-After": "5"},
    )


def protected_cloud_unavailable_rejection() -> JSONResponse:
    return JSONResponse(
        {
            "error": "protected_cloud_unavailable",
            "retryable": True,
            "message": "Protected cloud is temporarily unavailable; retry when it recovers.",
        },
        status_code=503,
        headers={"Retry-After": "5"},
    )


def session_identity_mismatch_rejection() -> JSONResponse:
    return JSONResponse(
        {"error": "session_identity_mismatch", "retryable": True},
        status_code=409,
    )


async def handle_input(request: Request, ports: SessionHttpPorts) -> JSONResponse:
    """Push user input onto the loop's queue. Body: {content, role?, turn_id?}.

    ``role`` defaults to 'human'. The orchestrator sends ``role='event'`` when
    injecting a system notice (a worker job the session created finished) so the
    persisted row does not render as a user bubble.

    Before persistence, a 503 tells either caller to retry. After the durable
    transaction commits, both human and event inputs receive an accepted 202
    with their exact delivery state. The orchestrator interprets that as
    persisted-not-executed and retains its stable-identity outbox claim; an
    uncorrelated human client must not submit a second input.
    """
    runtime = ports.runtime
    if runtime.stateless_mode():
        return stateless_rejection()
    if runtime.runtime_admission_closed():
        return termination_rejection()
    if runtime.session() is not None and not runtime.protected_cloud_ready():
        return protected_cloud_unavailable_rejection()
    if runtime.session() is None or runtime.input_queue() is None:
        return JSONResponse({"error": "Session not active"}, status_code=503)
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "invalid JSON"}, status_code=400)
    content = body.get("content", "")
    if not isinstance(content, str) or not content:
        return JSONResponse(
            {"error": "content must be a non-empty string"},
            status_code=400,
        )
    role = body.get("role") or "human"
    if role not in ACCEPTED_INPUT_ROLES:
        return JSONResponse(
            {"error": f"role must be one of {sorted(ACCEPTED_INPUT_ROLES)}"},
            status_code=400,
        )
    delivery_id = body.get("delivery_id")
    expected_session_identity_fingerprint = body.get("session_identity_fingerprint")
    if role == "event":
        if delivery_id is None:
            return JSONResponse(
                {"error": "durable event input requires a delivery_id"},
                status_code=400,
            )
        expected_key = os.environ.get("MCP_INTERNAL_KEY", "")
        presented_key = request.headers.get("X-Internal-Key", "")
        if not expected_key or not hmac.compare_digest(expected_key, presented_key):
            # The identity links an outbox row to its one paid turn. It is
            # server-owned authority, not an unauthenticated dedup hint.
            return JSONResponse(
                {"error": "durable event delivery requires internal authority"},
                status_code=403,
            )
        try:
            delivery_id = str(uuid.UUID(str(delivery_id)))
        except (ValueError, TypeError, AttributeError):
            return JSONResponse(
                {"error": "delivery_id must be a UUID"}, status_code=400
            )
    elif delivery_id is not None:
        return JSONResponse(
            {"error": "delivery_id is reserved for durable event input"},
            status_code=400,
        )
    expected_session_identity_fingerprint = canonical_session_identity_fingerprint(
        expected_session_identity_fingerprint
    )
    if (
        expected_session_identity_fingerprint is None
        or runtime.identity_fingerprint() != expected_session_identity_fingerprint
    ):
        return session_identity_mismatch_rejection()
    if not ports.operations.ensure_loop_started("rest_input"):
        if runtime.runtime_admission_closed():
            return termination_rejection()
        return JSONResponse({"error": "Session not ready"}, status_code=503)
    try:
        admission = await ports.operations.accept_input(
            content,
            role=role,
            delivery_id=delivery_id,
            expected_session_identity_fingerprint=expected_session_identity_fingerprint,
        )
    except TerminationAdmissionClosed:
        return termination_rejection()
    except ProtectedCloudUnavailable:
        return protected_cloud_unavailable_rejection()
    except DurableInputUnavailable:
        return JSONResponse(
            {
                "error": "durable_input_unavailable",
                "retryable": True,
                "message": "Durable input admission is temporarily unavailable.",
            },
            status_code=503,
            headers={"Retry-After": "5"},
        )
    except SessionIdentityMismatch:
        return session_identity_mismatch_rejection()
    return JSONResponse(
        {
            **accepted_input_payload(admission),
            "turn_id": runtime.session().turn_count,
            "queue_depth": runtime.input_queue().qsize(),
        },
        status_code=(
            200
            if admission.delivery_state in {"admitted", "settled", "cancelled"}
            else 202
        ),
    )


async def handle_interrupt(
    request: Optional[Request], ports: SessionHttpPorts
) -> JSONResponse:
    """Signal the pinned loop, optionally fenced to a correlated turn.

    Every HTTP call must carry the exact pinned-session fingerprint.  After
    removing that identity field, an otherwise-empty body retains the legacy
    active-turn behavior. New orchestrator forwarding may additionally supply
    ``client_request_id`` and ``target_turn_id``; those calls are rejected
    before any RAM mutation unless the exact transcript turn is active.
    Stateless pods consume the durable inbox instead of this direct route.
    """

    runtime = ports.runtime
    if runtime.stateless_mode():
        return stateless_rejection()
    if runtime.session() is None:
        return JSONResponse({"error": "Session not active"}, status_code=503)

    parsed_body: Dict[str, Any] = {}
    if request is not None:
        raw = await request.body()
        if raw:
            try:
                parsed = json.loads(raw)
            except (TypeError, ValueError):
                return JSONResponse(
                    {"error": "invalid JSON", "error_code": "invalid_request"},
                    status_code=400,
                )
            if not isinstance(parsed, dict):
                return JSONResponse(
                    {
                        "error": "body must be a JSON object",
                        "error_code": "invalid_request",
                    },
                    status_code=400,
                )
            parsed_body = parsed

    expected_session_identity_fingerprint = canonical_session_identity_fingerprint(
        parsed_body.get("session_identity_fingerprint")
    )
    if (
        expected_session_identity_fingerprint is None
        or runtime.identity_fingerprint() != expected_session_identity_fingerprint
    ):
        return session_identity_mismatch_rejection()

    body = dict(parsed_body)
    body.pop("session_identity_fingerprint", None)
    if not body:
        # Legacy clients omitted a turn identity. Scope the exact-runtime
        # request to the concrete active turn observed now; an idle request
        # must never arm the next input.
        target_turn_id = int(runtime.session().turn_count)
        mode = ports.operations.signal_interrupt(target_turn_id)
        if mode is None:
            return JSONResponse(
                {
                    "ack": False,
                    "applied": False,
                    "target_turn_id": target_turn_id,
                    "error": "target turn is no longer active",
                    "error_code": "target_turn_not_active",
                },
                status_code=409,
            )
        logger.info(
            "Interrupt received via legacy REST "
            "(target_turn=%d mode=%s tool_inflight=%s)",
            target_turn_id,
            mode,
            runtime.tool_inflight(),
        )
        return JSONResponse(
            {
                "ack": True,
                "applied": True,
                "target_turn_id": target_turn_id,
                "mode": mode,
            }
        )

    client_request_id = body.get("client_request_id")
    target_turn_id = body.get("target_turn_id")
    if not isinstance(client_request_id, str) or not client_request_id:
        return JSONResponse(
            {
                "error": "client_request_id must be a non-empty string",
                "error_code": "invalid_request",
            },
            status_code=400,
        )
    if (
        isinstance(target_turn_id, bool)
        or not isinstance(target_turn_id, int)
        or target_turn_id < 1
    ):
        return JSONResponse(
            {
                "error": "target_turn_id must be a positive integer",
                "error_code": "invalid_request",
            },
            status_code=400,
        )

    response: Dict[str, Any] = {
        "client_request_id": client_request_id,
        "target_turn_id": target_turn_id,
    }
    request_id = body.get("request_id")
    if request_id is not None:
        if not isinstance(request_id, str) or not request_id:
            return JSONResponse(
                {
                    "error": "request_id must be a non-empty string",
                    "error_code": "invalid_request",
                },
                status_code=400,
            )
        response["request_id"] = request_id

    mode = ports.operations.signal_interrupt(target_turn_id)
    if mode is None:
        return JSONResponse(
            {
                **response,
                "applied": False,
                "error": "target turn is no longer active",
                "error_code": "target_turn_not_active",
            },
            status_code=409,
        )
    logger.info(
        "Correlated interrupt received via REST "
        "(target_turn=%d mode=%s tool_inflight=%s)",
        target_turn_id,
        mode,
        runtime.tool_inflight(),
    )
    return JSONResponse({**response, "ack": True, "applied": True, "mode": mode})


async def handle_approve(request: Request, ports: SessionHttpPorts) -> JSONResponse:
    """Resolve a pending permission gate by UPDATEing the
    thread_permission_requests row. Body: {decision: approve|deny,
    approval_id?}. If approval_id is omitted, the most-recent-pending
    row for this thread is resolved (legacy single-pending-at-a-time
    contract). The DB trigger emits NOTIFY → agent's permission_check
    wakes up."""
    runtime = ports.runtime
    if runtime.stateless_mode():
        return stateless_rejection()
    if runtime.session() is None:
        return JSONResponse({"error": "Session not active"}, status_code=503)
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "invalid JSON"}, status_code=400)
    decision_raw = body.get("decision")
    if decision_raw == "approve":
        decision = "approved"
    elif decision_raw == "deny":
        decision = "denied"
    else:
        return JSONResponse(
            {"error": "decision must be 'approve' or 'deny'"},
            status_code=400,
        )
    approval_id = body.get("approval_id")
    resolved = await ports.operations.resolve_permission(
        decision,
        approval_id=approval_id,
        decided_by="rest_client",
    )
    if resolved is None:
        return JSONResponse(
            {
                "error": "No matching pending request",
                "approval_id": approval_id,
            },
            status_code=404,
        )
    return JSONResponse(
        {
            "accepted": True,
            "decision": decision_raw,
            "approval_id": str(resolved["id"]),
            "tool_call_id": resolved["tool_call_id"],
        }
    )


def register_session_http_routes(
    app: FastAPI,
    ports: SessionHttpPorts,
    *,
    precheck: Optional[Callable[[], Optional[Response]]] = None,
    tags: Optional[Sequence[str]] = None,
) -> None:
    """Declare ``/api/input``, ``/api/interrupt`` and ``/api/approve``.

    ``precheck`` lets a mode refuse before the session is consulted (dual
    mode answers 404 outside its SESSION state). Route and operation names
    are the ones both applications have always published.
    """

    route_tags = list(tags) if tags is not None else None

    @app.post("/api/input", tags=route_tags)
    async def api_input(request: Request):
        refusal = precheck() if precheck is not None else None
        if refusal is not None:
            return refusal
        return await handle_input(request, ports)

    @app.post("/api/interrupt", tags=route_tags)
    async def api_interrupt(request: Request):
        refusal = precheck() if precheck is not None else None
        if refusal is not None:
            return refusal
        return await handle_interrupt(request, ports)

    @app.post("/api/approve", tags=route_tags)
    async def api_approve(request: Request):
        refusal = precheck() if precheck is not None else None
        if refusal is not None:
            return refusal
        return await handle_approve(request, ports)
