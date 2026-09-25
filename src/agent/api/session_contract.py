"""Contract between the session transport and the persistent runtime.

The HTTP and WebSocket adapters (``session_http``, ``session_websocket``) and
the runtime that owns admission, execution and lifecycle
(``persistent_app``) both import these types, so neither imports the other.
Nothing here reads process state: the runtime supplies call-time providers
and operations through the port objects below, and the transport calls them
at the same points the handlers always read module state.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Optional

# Roles POST /api/input may request. 'human' is the normal path; 'event' is a
# system-injected notice (currently: a worker job this session created reached a
# terminal state — knowledge-base/knowledge/features/session_wake_on_job_completion.md).
#
# An allow-list rather than a passthrough because ordinary /api/input is an
# internal orchestrator effect endpoint rather than a browser-token endpoint.
# Durable events must name the exact runtime fingerprint and also require the
# existing internal transport key. Human requests that carry the new
# fingerprint are fenced identically; the missing-field compatibility branch
# remains only until the orchestrator forwarding cutover is atomic. An
# arbitrary role would let anything that can reach the pod forge 'ai' or
# 'system' rows.
ACCEPTED_INPUT_ROLES = frozenset({"human", "event"})


class WorkspaceNotReady(RuntimeError):
    """The session's workspace container never became ready in time.

    Subclasses RuntimeError so existing ``except RuntimeError`` handlers (e.g.
    the pool-mode /session/attach path) keep catching it, while the lifespan
    startup can catch it specifically to exit cleanly instead of crash-looping.
    """


class ProtectedCloudUnavailable(WorkspaceNotReady):
    """A protected attach cannot prove its lower+overlay delivery contract.

    This is terminal for the current attach.  Retrying a partially described
    protected payload as an ordinary workspace would expose credentials and
    tools before the review boundary exists, so callers must fail closed.
    """


class TerminationAdmissionClosed(RuntimeError):
    """Input reached the runtime after its termination fence closed."""


class DurableInputUnavailable(RuntimeError):
    """A retry-stable event could not establish its durable inbox row."""


class SessionIdentityMismatch(RuntimeError):
    """Input was addressed to a different pinned runtime incarnation."""


@dataclass(frozen=True, slots=True)
class AcceptedInput:
    message_id: str
    delivery_id: str
    delivery_state: str
    claim_generation: int
    enqueued: bool
    duplicate: bool = False
    deferred: bool = False


def accepted_input_payload(admission: AcceptedInput) -> dict[str, Any]:
    """Serialize one durable input acknowledgement across REST and WS.

    Once the transcript+delivery transaction commits, the input belongs to
    the durable inbox.  In particular, ``deferred`` means the successor will
    reclaim it; telling an uncorrelated WebSocket client to retry would mint a
    second delivery identity and could buy a second turn.
    """

    return {
        "accepted": True,
        "message_id": admission.message_id,
        "duplicate": admission.duplicate,
        "deferred": admission.deferred,
        "retryable": False,
        "delivery_id": admission.delivery_id,
        "delivery_state": admission.delivery_state,
    }


def canonical_session_identity_fingerprint(value: Any) -> str | None:
    """Return one exact v1 fingerprint, never a normalised lookalike."""

    if not isinstance(value, str) or not (
        value.startswith("sha256:")
        and len(value) == 71
        and all(char in "0123456789abcdef" for char in value[7:])
    ):
        return None
    return value


@dataclass(frozen=True, slots=True)
class SessionRuntimeView:
    """Read-only providers over the attached runtime, evaluated per call.

    Each provider returns the runtime's value at the moment it is called; a
    binding that captured the first attached session would let a stale
    handshake or request act on its successor.
    """

    stateless_mode: Callable[[], bool]
    session: Callable[[], Any]
    thread_id: Callable[[], Optional[str]]
    identity_fingerprint: Callable[[], Optional[str]]
    runtime_admission_closed: Callable[[], bool]
    retirement_admission_closed: Callable[[], bool]
    protected_cloud_ready: Callable[[], bool]
    session_ready: Callable[[], bool]
    input_queue: Callable[[], Optional[asyncio.Queue]]
    turn_open: Callable[[], bool]
    tool_inflight: Callable[[], bool]


@dataclass(frozen=True, slots=True)
class SessionOperations:
    """Runtime operations the transport invokes; their owners stay put.

    ``accept_input`` persists the transcript row and delivery before any
    queue publication; ``signal_interrupt`` mutates the loop's interrupt state
    only for a still-active target turn; ``resolve_permission`` decides one
    durable permission row.
    """

    ensure_loop_started: Callable[..., bool]
    accept_input: Callable[..., Awaitable[AcceptedInput]]
    signal_interrupt: Callable[[int], Optional[str]]
    resolve_permission: Callable[..., Awaitable[Optional[dict[str, Any]]]]
