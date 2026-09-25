"""WebSocket transport for an attached persistent session.

Owns one client connection end to end: the handshake-to-session identity
recheck, the stateless/termination/readiness refusals, its subscriber queue
and pump, the welcome snapshot, the receive loop and verb dispatch, direct
replies, and cleanup of that connection's subscription, Canvas awareness and
pump. Durable admission, loop start, interrupts, permission decisions,
session commands and the subscriber registry's lifecycle effects stay with
the runtime and are reached through :class:`SessionSocketPorts`.

Headless lifecycle: the first connection may start the persistent loop, and
later connections join its broadcast stream. Closing a socket only
unsubscribes it; the loop keeps running until its own completion handler or
an out-of-band termination stops it, and a disconnect never schedules pod
exit. Direct replies and pings go to this socket only and never enter the
event journal.

Both the persistent-mode and dual-mode applications register the two aliases
(``/ws/chat`` and the Ingress path ``/p/{thread_id}/ws``) with
:func:`register_session_websocket_routes`; dual mode adds its pod-state
precheck.
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect

from agent.api import session_transport
from agent.api._session_auth import SessionAuthBindings, validate_session_token
from agent.api.session_canvas_control import (
    CANVAS_CONTROL_METHODS,
    CanvasControlChannel,
)
from agent.api.session_contract import (
    DurableInputUnavailable,
    ProtectedCloudUnavailable,
    SessionIdentityMismatch,
    SessionOperations,
    SessionRuntimeView,
    TerminationAdmissionClosed,
    accepted_input_payload,
)
from agent.services.workspace_undo import (
    WorkspaceUndoRetryable,
    WorkspaceUndoUnavailable,
)

logger = logging.getLogger(__name__)

# Idle keepalive on the control WS. Must be shorter than the cockpit's
# CONTROL_WS_WATCHDOG_TIMEOUT_MS and any edge/tunnel idle timeout on the WS
# path.
WS_PING_INTERVAL_S: float = 20.0


@dataclass(frozen=True, slots=True)
class SessionConnectionPorts:
    """Runtime-owned registry and bookkeeping a connection participates in.

    ``subscribe`` keeps the runtime's first-subscriber/attention effects;
    ``unsubscribe`` removes exactly one queue and nothing else.
    """

    subscribe: Callable[[str], asyncio.Queue]
    unsubscribe: Callable[[str], None]
    note_connection_arrived: Callable[[], None]
    track_side_task: Callable[[asyncio.Task[Any]], asyncio.Task[Any]]


@dataclass(frozen=True, slots=True)
class SessionWelcomePorts:
    """Authoritative state for the ``session.state`` welcome snapshot."""

    durable_control_modes: Callable[[], Awaitable[tuple[str, str]]]
    pending_permissions: Callable[[], Awaitable[list[dict[str, Any]]]]
    running_tool: Callable[[Any], Optional[dict[str, Any]]]


@dataclass(frozen=True, slots=True)
class SessionSocketCommands:
    """Runtime session commands a client may request over its socket.

    Each command still receives the socket for its direct replies.
    """

    config_update: Callable[..., Awaitable[None]]
    compact: Callable[..., Awaitable[None]]
    archive: Callable[[WebSocket], Awaitable[None]]
    vm_upgrade: Callable[[WebSocket], Awaitable[None]]
    workspace_upgrade: Callable[[WebSocket, str], Awaitable[None]]
    rewind: Callable[[WebSocket, dict[str, Any]], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class SessionSocketPorts:
    runtime: SessionRuntimeView
    operations: SessionOperations
    connection: SessionConnectionPorts
    welcome: SessionWelcomePorts
    commands: SessionSocketCommands
    canvas: CanvasControlChannel


def _terminating_rejection() -> dict[str, Any]:
    return {
        "error": "runtime_terminating",
        "retryable": True,
        "message": "Retry input on the replacement runtime.",
    }


async def serve_session_websocket(ws: WebSocket, ports: SessionSocketPorts) -> None:
    """Serve one authenticated client connection of the attached session."""

    runtime = ports.runtime
    operations = ports.operations
    connection = ports.connection
    commands = ports.commands
    send = session_transport.send_message

    validated_identity = getattr(ws.state, "session_identity_fingerprint", None)
    validated_session = runtime.session()
    await ws.accept()
    if (
        not isinstance(validated_identity, str)
        or validated_session is None
        or runtime.session() is not validated_session
        or runtime.identity_fingerprint() != validated_identity
    ):
        await ws.close(code=4403, reason="session identity changed")
        return

    # Stateless executor (M3): sessions on this pod are driven exclusively by
    # run_queue claims — there is no live WS surface. Mirror the REST 409.
    if runtime.stateless_mode():
        try:
            await ws.send_json(
                {
                    "method": "error",
                    "params": {
                        "message": (
                            "stateless executor: this pod serves queued turns; "
                            "no direct session WebSocket is available"
                        )
                    },
                }
            )
        except Exception:
            pass
        await ws.close(code=4409, reason="stateless executor")
        return

    if runtime.runtime_admission_closed():
        try:
            await ws.send_json(
                {
                    "method": "input.rejected",
                    "params": _terminating_rejection(),
                }
            )
        finally:
            await ws.close(code=4512, reason="runtime terminating")
        return

    # Signal the boot-WS watchdog that a connection arrived. Done before
    # the readiness check so even a failed-to-be-ready connection counts:
    # the user clearly came back, and a different error path applies.
    connection.note_connection_arrived()

    # Readiness gates on the loop primitives, not just the session — the
    # runtime's single definition shared with /ready and /session/status so
    # the probe and the WS gate can't drift.
    if not runtime.session_ready():
        await send(ws, "error", {"message": "Agent not ready"})
        await ws.close(code=4503, reason="Agent not ready")
        return

    # Register this WS as a subscriber on the broadcast hub.
    client_id = uuid.uuid4().hex
    queue = connection.subscribe(client_id)
    pump_task = asyncio.create_task(
        session_transport.run_subscriber_pump(
            ws, queue, ping_interval=WS_PING_INTERVAL_S
        ),
        name=f"subscriber-pump-{client_id[:8]}",
    )

    logger.info(
        f"WebSocket connected: thread={runtime.thread_id()} client={client_id[:8]}"
    )

    def _identity_current() -> bool:
        return bool(
            runtime.session() is validated_session
            and runtime.identity_fingerprint() == validated_identity
        )

    async def _release_connection() -> None:
        connection.unsubscribe(client_id)
        ports.canvas.release(client_id)
        if not pump_task.done():
            pump_task.cancel()
            try:
                await pump_task
            except asyncio.CancelledError:
                pass

    async def _reject_preloop_identity_change() -> None:
        await _release_connection()
        await ws.close(code=4403, reason="session identity changed")

    def _spawn_ws_effect(coro: Any, *, name: str) -> asyncio.Task[Any]:
        return connection.track_side_task(asyncio.create_task(coro, name=name))

    # Send current session state so this client can sync. Direct send —
    # this is the welcome frame, only the connecting client cares.
    #
    # running_tool: if the loop is blocked in a tool call right now, tell this
    # (re)attaching client which command is running so it can render a "running
    # command" card instead of a blank "Connecting…". Incremental history may
    # already carry the AIMessage + tool_call, but it cannot say that the call
    # is still running — this welcome frame is the authoritative snapshot.
    #
    # pending_permissions: same idea for supervised gates that are still
    # waiting on an answer. The durable row survives, but REST history does
    # not carry it, so without this a reload (or a dropped live stream) leaves
    # the approval card unrenderable and the gate unanswerable — the failure
    # in knowledge-history/done/supervised_parallel_gates_timeout_fabricates_denial.md.
    running_tool = ports.welcome.running_tool(validated_session)
    (
        durable_permission_mode,
        durable_narration_mode,
    ) = await ports.welcome.durable_control_modes()
    if not _identity_current():
        await _reject_preloop_identity_change()
        return
    pending_permissions = await ports.welcome.pending_permissions()
    if not _identity_current():
        await _reject_preloop_identity_change()
        return
    task_manager = validated_session.session_task_manager
    session_tasks = task_manager.to_dict_list() if task_manager is not None else []
    await send(
        ws,
        "session.state",
        {
            "thread_id": runtime.thread_id(),
            "permission_mode": durable_permission_mode,
            "narration_mode": durable_narration_mode,
            "turn_count": validated_session.turn_count,
            # Authoritative join signal for a cold Cockpit reattach. REST can
            # already contain an incrementally persisted prefix of this turn;
            # the client uses (turn_in_flight, turn_count) to keep that prefix
            # and cursor-replayed suffix in one streaming bubble.
            "turn_in_flight": runtime.turn_open(),
            "message_count": len(validated_session.messages),
            "model": validated_session.config.llm.model,
            "temperature": validated_session.config.llm.temperature,
            "running_tool": running_tool,
            "pending_permissions": pending_permissions,
            "tasks": session_tasks,
        },
    )
    if not _identity_current():
        await _reject_preloop_identity_change()
        return

    # Spawn the persistent loop if it isn't already running. Reconnecting
    # to a session whose loop is mid-turn just joins the broadcast — no
    # restart, no replay (replay arrives in chunk 2 via the event log).
    operations.ensure_loop_started("websocket", client_id=client_id)

    # --- WebSocket receive loop ---
    try:
        # The exact current queue lock is also the admission serialization
        # point. Any old-token admission that started before this claim must
        # commit before this snapshot; one that starts after it observes the
        # new token and cannot create old-generation work. Fetch the complete
        # (unbounded) generation once so one steal produces at most one epoch
        # rotation and one terminal boundary per abandoned target.
        while True:
            raw = await ws.receive_text()
            if not _identity_current():
                await ws.close(code=4403, reason="session identity changed")
                break
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                # Plain text → treat as message
                data = {"method": "message", "content": raw}

            method = data.get("method", "message")
            if runtime.retirement_admission_closed():
                # This socket may predate the exact ``ending`` transition.
                # Retire every moment-scoped control surface together: a late
                # approval/config/upgrade must not leak through simply because
                # it is not a user-message verb. The SSE/journal plane lives in
                # Cockpit and is intentionally independent of this socket.
                await send(ws, "input.rejected", _terminating_rejection())
                await ws.close(code=4512, reason="runtime terminating")
                break

            if method == "message":
                content = data.get("content", "")
                if content and runtime.input_queue() is not None:
                    try:
                        admission = await operations.accept_input(
                            content,
                            expected_session_identity_fingerprint=validated_identity,
                        )
                        rejection_error = "runtime_terminating"
                        rejection_message = "Retry input on the replacement runtime."
                    except TerminationAdmissionClosed:
                        admission = None
                        rejection_error = "runtime_terminating"
                        rejection_message = "Retry input on the replacement runtime."
                    except ProtectedCloudUnavailable:
                        admission = None
                        rejection_error = "protected_cloud_unavailable"
                        rejection_message = (
                            "Retry input when the protected cloud mount recovers."
                        )
                    except DurableInputUnavailable:
                        admission = None
                        rejection_error = "durable_input_unavailable"
                        rejection_message = "Retry input when durable storage recovers."
                    except SessionIdentityMismatch:
                        await ws.close(code=4403, reason="session identity changed")
                        break
                    if admission is None:
                        await send(
                            ws,
                            "input.rejected",
                            {
                                "error": rejection_error,
                                "retryable": True,
                                "message": rejection_message,
                            },
                        )
                    elif admission.deferred:
                        await send(
                            ws,
                            "input.accepted",
                            accepted_input_payload(admission),
                        )

            elif method in CANVAS_CONTROL_METHODS:
                await ports.canvas.handle(
                    ws,
                    data,
                    client_id,
                    expected_session_identity_fingerprint=validated_identity,
                )

            elif method == "approve":
                # Phase 3: resolve the most-recent-pending permission
                # request in the DB. Cockpit can pass an explicit
                # approval_id to disambiguate when multiple are
                # pending (rare — agent's loop serializes most flows).
                approval_id = data.get("approval_id")
                _spawn_ws_effect(
                    operations.resolve_permission(
                        "approved",
                        approval_id=approval_id,
                        decided_by="ws_client",
                    ),
                    name="resolve-approve",
                )

            elif method == "deny":
                approval_id = data.get("approval_id")
                _spawn_ws_effect(
                    operations.resolve_permission(
                        "denied",
                        approval_id=approval_id,
                        decided_by="ws_client",
                    ),
                    name="resolve-deny",
                )

            elif method == "interrupt":
                # Legacy WebSocket clients carry no target. Bind the request to
                # the concrete active turn observed now; never leave a bare flag
                # that can interrupt a later input.
                session = runtime.session()
                target_turn_id = int(session.turn_count) if session else 0
                mode = (
                    operations.signal_interrupt(target_turn_id)
                    if target_turn_id > 0
                    else None
                )
                if mode is None:
                    await send(
                        ws,
                        "interrupt.ack",
                        {
                            "applied": False,
                            "target_turn_id": target_turn_id or None,
                            "error_code": "target_turn_not_active",
                        },
                    )
                    logger.info("Idle legacy WebSocket interrupt rejected")
                else:
                    await send(
                        ws,
                        "interrupt.ack",
                        {
                            "applied": True,
                            "target_turn_id": target_turn_id,
                            "mode": mode,
                        },
                    )
                    logger.info(
                        "Interrupt acknowledged (target_turn=%d mode=%s)",
                        target_turn_id,
                        mode,
                    )

            elif method in {"mode.set", "narration.set"}:
                # These verbs are lane-agnostic orchestrator REST controls.
                # Keeping a second live-only mutation path here would let an
                # old client change RAM without the desired scalar, inbox
                # order, durable result receipt, or owner fence.
                await send(
                    ws,
                    "error",
                    {
                        "code": "control_transport_retired",
                        "message": "Use the session control REST endpoint",
                    },
                )

            elif method == "config.update":
                config_override = data.get("config", {})
                # Slice B: a datasource change rides the same frame as a
                # sibling key — the full desired selection (None = unchanged).
                datasource_ids = data.get("datasource_ids")
                if config_override or datasource_ids is not None:
                    _spawn_ws_effect(
                        commands.config_update(
                            ws,
                            config_override,
                            datasource_ids=datasource_ids,
                            request_id=data.get("request_id"),
                        ),
                        name="handle-config-update",
                    )

            elif method == "compact":
                # Manual compaction (/compact command, or the rewind action
                # sheet's "Summarize up to here" with boundary_message_id).
                focus = data.get("focus", "")
                _spawn_ws_effect(
                    commands.compact(
                        ws, focus, boundary_message_id=data.get("boundary_message_id")
                    ),
                    name="handle-compact",
                )

            elif method == "archive":
                # End session (/done command)
                _spawn_ws_effect(commands.archive(ws), name="handle-archive")

            elif method == "upgrade-to-vm":
                # Upgrade workspace from container to VM
                _spawn_ws_effect(commands.vm_upgrade(ws), name="handle-vm-upgrade")

            elif method == "upgrade-to-workspace":
                # Upgrade a lite (virtual) session to a real sandbox container
                target_tier = data.get("target_tier", "sandbox")
                _spawn_ws_effect(
                    commands.workspace_upgrade(ws, target_tier),
                    name="handle-workspace-upgrade",
                )

            elif method == "undo":
                session = runtime.session()
                if session is None:
                    await send(
                        ws,
                        "error",
                        {"message": "Session no longer active"},
                    )
                    continue
                if session.shell_owner_token is not None:
                    await send(
                        ws,
                        "error",
                        {
                            "code": "control_transport_required",
                            "message": "Use the session control REST endpoint",
                        },
                    )
                    continue
                turn_id = data.get("turn_id")
                try:
                    restored = await session.undo_turn(turn_id)
                except WorkspaceUndoUnavailable as exc:
                    await send(
                        ws,
                        "error",
                        {
                            "code": exc.code,
                            "message": str(exc),
                        },
                    )
                except WorkspaceUndoRetryable as exc:
                    await send(
                        ws,
                        "error",
                        {
                            "code": "workspace_undo_retryable",
                            "message": str(exc),
                        },
                    )
                else:
                    await send(
                        ws,
                        "files.restored",
                        {**restored, "turn_id": turn_id},
                    )

            elif method == "rewind":
                if runtime.session() is None:
                    await send(
                        ws,
                        "error",
                        {
                            "message": "Session no longer active",
                            "request_id": data.get("request_id"),
                        },
                    )
                    continue
                _spawn_ws_effect(
                    commands.rewind(ws, data),
                    name="handle-rewind",
                )

            else:
                await send(ws, "error", {"message": f"Unknown method: {method}"})

    except WebSocketDisconnect:
        logger.info(
            f"WebSocket disconnected: thread={runtime.thread_id()} "
            f"client={client_id[:8]} (loop continues)"
        )
    except Exception as e:
        logger.exception(f"WebSocket error: {e}")
    finally:
        # Headless keystone: WS close only unsubscribes. The loop keeps
        # running until its completion handler routes its natural exit, or an
        # out-of-band termination intervenes. We do NOT cancel the loop task
        # here, and we do NOT schedule pod exit.
        await _release_connection()
        logger.info(
            f"WebSocket pump released: thread={runtime.thread_id()} "
            f"client={client_id[:8]}"
        )


WebSocketPrecheck = Callable[[WebSocket], Awaitable[bool]]


def register_session_websocket_routes(
    app: FastAPI,
    *,
    auth: SessionAuthBindings,
    ports: SessionSocketPorts,
    precheck: Optional[WebSocketPrecheck] = None,
) -> None:
    """Declare the two authenticated aliases of the session socket.

    ``/ws/chat`` is the direct path (local dev, cluster-internal callers);
    ``/p/{thread_id}/ws`` is the path the per-session Ingress routes to. The
    ``thread_id`` path parameter is not trusted: the session JWT's ``tid``
    and complete identity fingerprint are checked against the runtime's
    current binding. ``precheck`` runs after authentication and before the
    connection is served; it must accept and close the socket itself when it
    refuses.
    """

    async def _authenticated(ws: WebSocket) -> None:
        if not await validate_session_token(ws, auth):
            return
        if precheck is not None and not await precheck(ws):
            return
        await serve_session_websocket(ws, ports)

    @app.websocket("/ws/chat")
    async def ws_chat(ws: WebSocket):
        await _authenticated(ws)

    @app.websocket("/p/{thread_id}/ws")
    async def ws_session(ws: WebSocket, thread_id: str):
        await _authenticated(ws)
