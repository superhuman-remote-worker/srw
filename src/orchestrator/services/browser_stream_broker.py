"""Shared-browser stream broker helpers and WebSocket relay."""

from __future__ import annotations

import asyncio
import json
import logging
import shlex
import struct
from uuid import UUID

from orchestrator.security.auth import resolve_ws_user
from orchestrator.security.csrf import websocket_origin_allowed
from orchestrator.services import resolve_ssh_key_path
from orchestrator.services.browser_stream_config import (
    BrowserStreamConfigurationError,
    browser_stream_config,
)
from orchestrator.services.canvas_ssh import (
    PINNED_SSH_TRANSPORT_POOL,
    CanvasSSHError,
    GenerationResolver,
    RemoteWorkspaceTarget,
    VMAttestationVerdict,
    attest_vm_workspace_target,
    canvas_workspace_generation,
    require_same_remote_workspace,
    resolve_canvas_workspace_target,
    vm_canvas_route_available,
)
from orchestrator.services.ssh_helpers import orchestrator_can_reach
from orchestrator.services.stateless_workspace_gate import (
    stateless_session_workspace_check,
)

T_HELLO, T_FRAME, T_STATE, T_INPUT, T_CONTROL, T_ERROR = 1, 2, 3, 4, 5, 6
MAX_STREAM_FRAME = 8 * 1024 * 1024
MAX_BROWSER_CLIENT_MESSAGE = 64 * 1024
STREAM_INFO_TIMEOUT_S = 45.0
STREAM_INFO_MAX_OUTPUT = 64 * 1024
# How often an open stream re-checks that its workspace is still the one it
# opened on. A WebSocket outlives the proof that admitted it; the IDE relay
# (ide_proxy_gateway._IDE_WS_LIFECYCLE_RECHECK_S) uses the same interval.
STREAM_WORKSPACE_RECHECK_S = 15.0

logger = logging.getLogger(__name__)


class BrowserStreamUnavailable(Exception):
    def __init__(self, status: int, detail: str) -> None:
        super().__init__(detail)
        self.status = status
        self.detail = detail


def encode_stream_frame(frame_type: int, payload: bytes) -> bytes:
    if len(payload) + 1 > MAX_STREAM_FRAME:
        raise ValueError(f"stream frame too large: {len(payload) + 1}")
    return struct.pack(">IB", len(payload) + 1, frame_type) + payload


async def read_stream_frame(reader) -> tuple[int, bytes]:
    head = await reader.readexactly(4)
    (length,) = struct.unpack(">I", head)
    if not 1 <= length <= MAX_STREAM_FRAME:
        raise ValueError(f"bad stream frame length: {length}")
    body = await reader.readexactly(length)
    return body[0], body[1:]


def _metadata(thread: dict) -> dict:
    value = thread.get("metadata") or {}
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (TypeError, ValueError):
            value = {}
    return value if isinstance(value, dict) else {}


def _active_context(thread: dict) -> dict:
    """Prefer a ready VM, otherwise use the container workspace context."""
    metadata = _metadata(thread)
    vm = metadata.get("vm") or {}
    if isinstance(vm, dict) and vm.get("status") == "ready":
        return vm
    container = metadata.get("workspace_container") or {}
    return container if isinstance(container, dict) else {}


def workspace_ready(thread: dict) -> bool:
    return _active_context(thread).get("status") == "ready"


async def _vm_target_verdict(target: RemoteWorkspaceTarget) -> VMAttestationVerdict:
    """Controller proof for a VM target; a container target has none to give."""

    if target.vm_runtime is None:
        return "current"
    return await attest_vm_workspace_target(target)


async def _require_current_vm(target: RemoteWorkspaceTarget, changed: str) -> None:
    verdict = await _vm_target_verdict(target)
    if verdict == "changed":
        raise BrowserStreamUnavailable(409, changed)
    if verdict != "current":
        raise BrowserStreamUnavailable(503, "workspace runtime could not be verified")


_STATELESS_BROWSER_START_STATUSES = frozenset(
    {"created", "active", "idle", "awaiting_user"}
)


def _stateless_browser_start_allowed(thread: dict) -> bool:
    """Whether a fresh daemon may be started for a stateless thread.

    Pinned behavior is intentionally unchanged. Stateless End publishes its
    closed lifecycle marker before workspace teardown; unresolved claimant
    losses likewise mean an older controller may still have admitted I/O.
    Neither state may race an auto-spawning ``browser-exec stream_info``.
    """

    if thread.get("execution_lane") != "stateless":
        return True
    _, workspace_refusal = stateless_session_workspace_check(thread)
    if workspace_refusal is not None:
        return False
    if str(thread.get("status") or "") not in _STATELESS_BROWSER_START_STATUSES:
        return False
    metadata = _metadata(thread)
    if "_stateless_workspace_retirement_pending" in metadata:
        return False
    if "_stateless_claim_retirement" in metadata:
        return False
    if "_stateless_claim_loss_hold" in metadata:
        return False
    # The loss ledger is unresolved-only: its writer removes the key when the
    # last claimant is settled.  Any present value (including a falsey or
    # malformed one) is therefore authority to hold admission, never evidence
    # that quiescence completed.
    if "_stateless_claim_losses" in metadata:
        return False
    if "protected_cloud" in metadata and metadata["protected_cloud"] is not False:
        return False
    return workspace_ready(thread)


def _stateless_workspace_process_tag(thread: dict) -> str | None:
    """Return the exact runtime tag inherited by a stateless browser daemon.

    ``exec_stream_info`` uses the orchestrator's direct pinned SSH transport,
    rather than ``RemoteBackend.exec_claim_resource``.  SSH does not forward
    arbitrary environment variables, so the command itself must inject the
    same Pod-incarnation tag that the workspace entrypoint and terminal
    retirement scanner use.  Pinned sessions deliberately retain their
    original command shape.
    """

    if thread.get("execution_lane") != "stateless":
        return None
    try:
        thread_id = str(UUID(str(thread.get("id"))))
        runtime_incarnation = str(
            UUID(str(_active_context(thread).get("_runtime_incarnation")))
        )
    except (TypeError, ValueError, AttributeError) as exc:
        raise BrowserStreamUnavailable(
            503, "stateless browser runtime authority is unavailable"
        ) from exc
    return f"v1:session:{thread_id}:{runtime_incarnation}"


async def exec_stream_info(
    thread: dict,
    *,
    initial_baton: str | None = None,
    generation_resolver: GenerationResolver,
) -> dict:
    """Cold-start browser-exec through the pinned SSH transport.

    A VM target is proved by the controller before the command runs and again
    after it returns, so a browser identity minted on a runtime that was
    replaced meanwhile is never handed out. The row checks of the pinned pool
    run in between, as for a container.
    """

    config = browser_stream_config()
    args: dict = {}
    if initial_baton:
        args["initial_baton"] = initial_baton
    encoded_args = json.dumps(args, sort_keys=True, separators=(",", ":"))
    remote = f"browser-exec stream_info --json {shlex.quote(encoded_args)}"
    process_tag = _stateless_workspace_process_tag(thread)
    if process_tag is not None:
        remote = f"env SRW_WORKSPACE_PROCESS_TAG={shlex.quote(process_tag)} {remote}"
    try:
        target = _resolve_target(thread)
        if not _target_routable(target):
            raise BrowserStreamUnavailable(503, "workspace route is unavailable")
        key_path = _resolve_key_path()
        await _require_current_vm(target, "workspace runtime is not attested")
        result = await PINNED_SSH_TRANSPORT_POOL.run_command(
            target=target,
            command=remote,
            key_path=key_path,
            generation_resolver=generation_resolver,
            timeout=STREAM_INFO_TIMEOUT_S,
            max_output_bytes=STREAM_INFO_MAX_OUTPUT,
        )
        await _require_current_vm(
            target, "workspace changed while the browser was starting"
        )
    except BrowserStreamUnavailable:
        raise
    except CanvasSSHError as exc:
        if exc.code == "workspace_command_timeout":
            detail = "browser start timed out"
        elif exc.code == "workspace_generation_changed":
            detail = "workspace changed while the browser was starting"
        else:
            detail = "browser startup transport is unavailable"
        raise BrowserStreamUnavailable(exc.status_code, detail) from exc
    except Exception as exc:
        raise BrowserStreamUnavailable(
            503, "browser startup transport is unavailable"
        ) from exc

    if result.exit_status != 0:
        raise BrowserStreamUnavailable(502, "browser-exec command failed")
    lines = [
        line
        for line in result.stdout.decode(errors="replace").splitlines()
        if line.strip()
    ]
    if not lines:
        raise BrowserStreamUnavailable(502, "browser-exec returned no output")
    try:
        info = json.loads(lines[-1])
    except ValueError as exc:
        raise BrowserStreamUnavailable(
            502, "browser-exec returned malformed output"
        ) from exc
    if not isinstance(info, dict):
        raise BrowserStreamUnavailable(502, "browser-exec returned malformed output")
    if "error" in info:
        raise BrowserStreamUnavailable(502, "browser-exec reported an error")
    try:
        stream_port = int(info["port"])
    except (KeyError, TypeError, ValueError) as exc:
        raise BrowserStreamUnavailable(
            502, "browser-exec returned incomplete stream identity"
        ) from exc
    if (
        not isinstance(info.get("generation"), str)
        or not info["generation"]
        or not isinstance(info.get("token"), str)
        or not info["token"]
        or info.get("baton") not in {"agent", "user"}
        or stream_port != config.stream_port
    ):
        raise BrowserStreamUnavailable(
            502, "browser-exec returned invalid stream identity"
        )
    return info


async def _exec_stream_info_with_lifecycle(
    thread: dict,
    *,
    thread_id: str,
    db,
    generation_resolver: GenerationResolver,
) -> dict:
    """Cold-start with stateless End serialization; pinned stays byte-for-byte.

    The lifecycle advisory lock closes the check-to-spawn race with public End.
    A second read under the same lock catches an End/claim-loss marker published
    while the SSH command was in flight. Existing relays do not hold this lock:
    terminal retirement stops the daemon itself, which closes their channels.
    """

    if thread.get("execution_lane") != "stateless":
        return await exec_stream_info(
            thread,
            generation_resolver=generation_resolver,
        )

    lifecycle_lock = getattr(db, "stateless_session_workspace_ensure_lock", None)
    if not callable(lifecycle_lock):
        raise BrowserStreamUnavailable(
            503, "stateless browser lifecycle authority is unavailable"
        )
    try:
        async with lifecycle_lock(thread_id, wait=True) as cleanup_owner:
            if not cleanup_owner:
                raise BrowserStreamUnavailable(
                    503, "stateless browser lifecycle authority is unavailable"
                )
            current = await db.get_thread(thread_id)
            if (
                not current
                or current.get("execution_lane") != "stateless"
                or str(current.get("user_id") or "") != str(thread.get("user_id") or "")
                or not _stateless_browser_start_allowed(current)
            ):
                raise BrowserStreamUnavailable(
                    409, "stateless browser lifecycle is closed"
                )
            info = await exec_stream_info(
                current,
                generation_resolver=generation_resolver,
            )
            current = await db.get_thread(thread_id)
            if (
                not current
                or current.get("execution_lane") != "stateless"
                or str(current.get("user_id") or "") != str(thread.get("user_id") or "")
                or not _stateless_browser_start_allowed(current)
            ):
                raise BrowserStreamUnavailable(
                    409, "stateless browser lifecycle changed during startup"
                )
            return info
    except BrowserStreamUnavailable:
        raise
    except asyncio.TimeoutError as exc:
        raise BrowserStreamUnavailable(
            503, "stateless browser lifecycle authority timed out"
        ) from exc


def _resolve_target(thread: dict) -> RemoteWorkspaceTarget:
    """Resolve the attested target: the bound container or the ready VM."""

    return resolve_canvas_workspace_target(
        dict(thread),
        canvas_workspace_generation(dict(thread)),
    )


def _target_routable(target: RemoteWorkspaceTarget) -> bool:
    """A container needs a route; a VM needs the same cluster (no tailnet)."""

    if target.vm_runtime is not None:
        return vm_canvas_route_available(target.host, can_reach=orchestrator_can_reach)
    return orchestrator_can_reach(target.host)


def _resolve_reachable_target(thread: dict) -> RemoteWorkspaceTarget:
    """Resolve the current attested workspace route, failing closed."""

    target = _resolve_target(thread)
    if not _target_routable(target):
        raise BrowserStreamUnavailable(503, "workspace route is unavailable")
    return target


async def _stream_target_verdict(
    db, thread_id: str, target: RemoteWorkspaceTarget
) -> VMAttestationVerdict:
    """Whether an open relay still talks to the workspace it opened on.

    The durable row answers first: a VM being suspended (``suspending``), a
    restored VM (new runtime) or a VM that took over from the container all
    show there. For a VM the controller then proves the live runtime, which
    also catches a launcher replaced behind an unchanged row. A container has
    no such proof, and an unreadable row never ends its stream; for a VM an
    inconclusive answer does (the viewer reconnects and is admitted again).
    """

    try:
        current = await db.get_thread(thread_id)
    except Exception:
        return "unknown" if target.vm_runtime is not None else "current"
    if not current:
        return "changed"
    try:
        require_same_remote_workspace(dict(current), target)
    except CanvasSSHError:
        return "changed"
    return await _vm_target_verdict(target)


async def _vm_stream_end_verdict(
    db, thread_id: str, target: RemoteWorkspaceTarget
) -> VMAttestationVerdict | None:
    """Row-only reason a VM relay's channel ended; ``None`` keeps it transient."""

    try:
        current = await db.get_thread(thread_id)
    except Exception:
        return None
    if not current:
        return "changed"
    try:
        require_same_remote_workspace(dict(current), target)
    except CanvasSSHError:
        return "changed"
    return None


async def _get_canvas_record(db, thread_id: str):
    """Load the staged browser identity, failing closed on storage errors."""

    from orchestrator.services.canvas import CanvasService

    try:
        return await CanvasService(db).get(thread_id)
    except Exception:
        logger.warning(
            "Could not load browser Canvas generation for thread %s",
            thread_id,
            exc_info=True,
        )
        return None


def _resolve_key_path() -> str:
    key_path = resolve_ssh_key_path()
    if not key_path:
        raise BrowserStreamUnavailable(503, "workspace SSH key unavailable")
    return key_path


def _open_loopback(**kwargs):
    return PINNED_SSH_TRANSPORT_POOL.open_loopback_connection(**kwargs)


_ACTIVE_VIEWERS: dict[str, int] = {}


def _reserve_viewer(thread_id: str, maximum: int) -> bool:
    """Atomically reserve one slot between event-loop suspension points."""

    current = _ACTIVE_VIEWERS.get(thread_id, 0)
    if current >= maximum:
        return False
    _ACTIVE_VIEWERS[thread_id] = current + 1
    return True


def _release_viewer(thread_id: str) -> None:
    remaining = _ACTIVE_VIEWERS.get(thread_id, 1) - 1
    if remaining <= 0:
        _ACTIVE_VIEWERS.pop(thread_id, None)
    else:
        _ACTIVE_VIEWERS[thread_id] = remaining


async def _close_ws(ws, code: int = 1000, reason: str | None = None) -> None:
    try:
        if reason is None:
            await ws.close(code=code)
        else:
            await ws.close(code=code, reason=reason)
    except Exception:
        pass


async def _reject_ws(ws, code: int, reason: str) -> None:
    """Complete the handshake before sending a contractual close code.

    ASGI servers turn ``websocket.close`` before ``websocket.accept`` into an
    HTTP handshake denial. Real browsers then observe an abnormal ``1006``
    close instead of the application code, so Cockpit cannot distinguish a
    viewer limit, stale generation, or disabled feature from a network flap.
    Accepting and immediately closing exposes no browser payload while making
    the fail-closed reason observable to the admitted client.
    """

    try:
        await ws.accept()
    except Exception:
        # The peer may disappear during admission. ``_close_ws`` is already
        # exception-safe, so let it make the final best-effort close.
        pass
    await _close_ws(ws, code, reason)


async def relay_browser_stream(ws, thread_id: str, *, db) -> None:
    """Relay binary WebSocket frames through a generation-pinned SSH channel.

    WebSocket messages carry ``[1-byte type][payload]``. The workspace TCP
    protocol adds only a four-byte length prefix, which this broker mirrors
    without inspecting browser frame or input payloads.
    """

    # This must be the first admission check so cross-site probes cannot use
    # close codes to distinguish feature, authentication, or thread state.
    if not websocket_origin_allowed(ws.headers):
        await _reject_ws(ws, 4403, "Origin not allowed")
        return

    try:
        config = browser_stream_config()
    except BrowserStreamConfigurationError:
        await _reject_ws(ws, 4404, "Shared browser is not enabled")
        return
    if not config.enabled:
        await _reject_ws(ws, 4404, "Shared browser is not enabled")
        return

    user = await resolve_ws_user(ws, db)
    if not user:
        await _reject_ws(ws, 4401, "Authentication required")
        return
    if not user.get("is_approved"):
        await _reject_ws(ws, 4403, "Account pending approval")
        return

    thread = await db.get_thread(thread_id)
    if not thread or str(thread.get("user_id") or "") != str(user.get("id") or ""):
        await _reject_ws(ws, 4403, "Thread access denied")
        return
    if not workspace_ready(thread):
        await _reject_ws(ws, 4503, "Workspace not ready")
        return
    try:
        # An unattested or unroutable (tailnet) VM fails here, before any
        # browser start, exactly like an unattested container.
        _resolve_reachable_target(thread)
        _resolve_key_path()
    except Exception:
        await _reject_ws(ws, 4503, "Workspace SSH unavailable")
        return

    reserved = False
    if not _reserve_viewer(thread_id, config.max_viewers):
        await _reject_ws(ws, 4429, "Viewer limit reached")
        return
    reserved = True

    accepted = False
    stream_failed = False
    protocol_closed = False
    target: RemoteWorkspaceTarget | None = None
    workspace_verdict: VMAttestationVerdict | None = None
    try:
        try:

            async def generation_resolver() -> dict:
                current = await db.get_thread(thread_id)
                return dict(current) if current else {}

            info = await _exec_stream_info_with_lifecycle(
                thread,
                thread_id=thread_id,
                db=db,
                generation_resolver=generation_resolver,
            )
        except BrowserStreamUnavailable:
            await _reject_ws(ws, 4502, "Browser unreachable")
            return

        # Startup is deliberately outside the accepted WebSocket. Re-admit
        # against fresh authority before exposing browser state or retaining
        # the viewer slot for a live relay.
        try:
            current_config = browser_stream_config()
        except BrowserStreamConfigurationError:
            await _reject_ws(ws, 4404, "Shared browser is not enabled")
            return
        if not current_config.enabled:
            await _reject_ws(ws, 4404, "Shared browser is not enabled")
            return

        current_user = await resolve_ws_user(ws, db)
        if not current_user:
            await _reject_ws(ws, 4401, "Authentication required")
            return
        if not current_user.get("is_approved"):
            await _reject_ws(ws, 4403, "Account pending approval")
            return

        current_thread = await db.get_thread(thread_id)
        if not current_thread or str(current_thread.get("user_id") or "") != str(
            current_user.get("id") or ""
        ):
            await _reject_ws(ws, 4403, "Thread access denied")
            return
        if thread.get("execution_lane") == "stateless" and (
            current_thread.get("execution_lane") != "stateless"
            or not _stateless_browser_start_allowed(current_thread)
        ):
            # The lifecycle lock protects cold-start, but it is intentionally
            # released before authentication and Canvas reads. End, claimant
            # loss, or a class/tier repair can therefore land in that gap.
            # Reapply the complete admission predicate at the final fresh-row
            # linearization point before accepting or opening loopback.
            await _reject_ws(ws, 4409, "Browser generation ended")
            return
        if not workspace_ready(current_thread):
            await _reject_ws(ws, 4503, "Workspace not ready")
            return
        try:
            target = _resolve_reachable_target(current_thread)
            key_path = _resolve_key_path()
        except Exception:
            await _reject_ws(ws, 4503, "Workspace SSH unavailable")
            return

        # The durable Canvas pointer is the browser identity authority. Missing,
        # non-browser, stale, or unreadable state must never silently attach to
        # whatever generation happens to be live in the workspace.
        record = await _get_canvas_record(db, thread_id)
        staged = getattr(
            getattr(record, "source", None),
            "browser_generation",
            None,
        )
        if staged is None or str(staged) != str(info.get("generation")):
            await _reject_ws(ws, 4409, "Browser generation ended")
            return
        if int(info["port"]) != current_config.stream_port:
            await _reject_ws(ws, 4502, "Browser unreachable")
            return
        # A VM is proved by the controller once more at the last point before
        # accepting: a runtime that changed is an ended browser, an
        # inconclusive proof a transient failure the viewer retries.
        admission = await _vm_target_verdict(target)
        if admission == "changed":
            await _reject_ws(ws, 4409, "Browser generation ended")
            return
        if admission != "current":
            await _reject_ws(ws, 4502, "Browser unreachable")
            return

        await ws.accept()
        accepted = True
        async with _open_loopback(
            target=target,
            destination_port=current_config.stream_port,
            key_path=key_path,
            generation_resolver=generation_resolver,
        ) as (reader, writer):
            hello = json.dumps(
                {
                    "token": info["token"],
                    "min_protocol": 1,
                    "max_viewers": current_config.max_viewers,
                },
                separators=(",", ":"),
            ).encode()
            writer.write(encode_stream_frame(T_HELLO, hello))
            await writer.drain()

            async def ws_to_tcp() -> None:
                nonlocal protocol_closed
                while True:
                    message = await ws.receive()
                    if (
                        isinstance(message, dict)
                        and message.get("type") == "websocket.disconnect"
                    ):
                        return
                    data = message.get("bytes") if isinstance(message, dict) else None
                    if (
                        not isinstance(message, dict)
                        or message.get("type") != "websocket.receive"
                        or message.get("text") is not None
                        or not isinstance(data, bytes)
                        or not data
                        or len(data) > MAX_BROWSER_CLIENT_MESSAGE
                        or data[0] not in (T_INPUT, T_CONTROL)
                    ):
                        protocol_closed = True
                        await _close_ws(ws, 4400, "Invalid browser protocol message")
                        return
                    writer.write(encode_stream_frame(data[0], data[1:]))
                    await writer.drain()

            async def tcp_to_ws() -> None:
                while True:
                    frame_type, payload = await read_stream_frame(reader)
                    await ws.send_bytes(bytes([frame_type]) + payload)

            async def touch_activity() -> None:
                while True:
                    try:
                        await db.merge_thread_workspace_context(thread_id, {})
                    except Exception:
                        logger.debug(
                            "Could not mark browser viewer activity for thread %s",
                            thread_id,
                            exc_info=True,
                        )
                    await asyncio.sleep(current_config.activity_interval_seconds)

            async def supervise_workspace() -> None:
                # Ends the relay once its workspace is suspended, restored,
                # replaced (a VM taking over from the container, or the
                # reverse) or, for a VM, can no longer be proved.
                nonlocal workspace_verdict
                while True:
                    await asyncio.sleep(STREAM_WORKSPACE_RECHECK_S)
                    verdict = await _stream_target_verdict(db, thread_id, target)
                    if verdict != "current":
                        workspace_verdict = verdict
                        logger.info(
                            "Shared-browser stream for thread %s closing: workspace %s",
                            thread_id,
                            "changed" if verdict == "changed" else "unverified",
                        )
                        return

            tasks = [
                asyncio.create_task(ws_to_tcp()),
                asyncio.create_task(tcp_to_ws()),
                asyncio.create_task(touch_activity()),
                asyncio.create_task(supervise_workspace()),
            ]
            done: set[asyncio.Task] = set()
            try:
                done, _ = await asyncio.wait(
                    tasks,
                    return_when=asyncio.FIRST_COMPLETED,
                )
            finally:
                for task in tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
            stream_failed = any(
                not task.cancelled() and task.exception() is not None for task in done
            )
    except Exception:
        stream_failed = True
        logger.debug(
            "Shared-browser stream ended for thread %s",
            thread_id,
            exc_info=True,
        )
        if not accepted:
            await _reject_ws(ws, 4502, "Browser unreachable")
    finally:
        if reserved:
            _release_viewer(thread_id)
        if (
            accepted
            and not protocol_closed
            and stream_failed
            and workspace_verdict is None
            and target is not None
            and target.vm_runtime is not None
        ):
            # A VM's channel usually dies because the VM went away (suspend,
            # restore, replacement). Say so when the row already shows it, so
            # the viewer offers a restart instead of reconnecting to nothing.
            # One row read; no controller call on this path.
            workspace_verdict = await _vm_stream_end_verdict(db, thread_id, target)
        if accepted and not protocol_closed:
            if workspace_verdict == "changed":
                await _close_ws(ws, 4409, "Browser generation ended")
            elif workspace_verdict == "unknown" or stream_failed:
                await _close_ws(ws, 4502, "stream ended")
            else:
                await _close_ws(ws)


__all__ = [
    "BrowserStreamUnavailable",
    "MAX_BROWSER_CLIENT_MESSAGE",
    "MAX_STREAM_FRAME",
    "T_CONTROL",
    "T_ERROR",
    "T_FRAME",
    "T_HELLO",
    "T_INPUT",
    "T_STATE",
    "encode_stream_frame",
    "exec_stream_info",
    "read_stream_frame",
    "relay_browser_stream",
    "workspace_ready",
]
