"""Pod-side validation of the orchestrator-minted session JWT.

The pod knows two things from its env:
  - SESSION_JWT_SECRET — shared with the orchestrator (K8s Secret)
  - SESSION_BOUND_THREAD_ID — the thread this pod was provisioned for

A valid handshake: token signature OK, audience=agent, not expired, claim
`tid` matches the bound thread, and claim `sif` matches the complete local
thread/generation/agent/attach/Pod identity.  A token minted for G1 therefore
cannot authorize a successor G2 of the same thread.

For dedicated session pods the bound thread comes straight from the env
var. For job-pool pods that get a thread attached later via /session/attach
the env var is empty at pod-creation time (K8s env can't be patched on a
running pod), so we fall back to the currently-attached thread reported by
the runtime's bindings. Same JWT, same signature check — only the expected
`tid` source differs.

The runtime supplies :class:`SessionAuthBindings`; this module never imports
the application that owns the session.

See knowledge-base/knowledge/features/direct_session_websockets.md §Component details.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Callable, Optional

import jwt
from fastapi import WebSocket

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class SessionAuthBindings:
    """Call-time view of the pod's current session binding.

    Both providers are consulted on every handshake, so replacing the attached
    session immediately invalidates tokens minted for its predecessor.
    """

    attached_thread_id: Callable[[], Optional[str]]
    identity_fingerprint: Callable[[], Optional[str]]


def _resolve_bound_tid(bindings: SessionAuthBindings) -> str:
    """Return the thread_id this pod is currently authorized to serve.

    Prefers the env var (dedicated session pods stamped by the orchestrator's
    agent_provisioner). Falls back to the attached session's thread_id for
    idle-pool pods bound at runtime via /session/attach.
    """
    env_tid = os.environ.get("SESSION_BOUND_THREAD_ID", "")
    if env_tid:
        return env_tid
    try:
        return str(bindings.attached_thread_id() or "")
    except Exception:
        return ""


def _resolve_bound_session_identity_fingerprint(
    bindings: SessionAuthBindings,
) -> str | None:
    """Return the exact local identity advertised by this pod's `/ready`."""

    try:
        return bindings.identity_fingerprint()
    except Exception:
        return None


async def validate_session_token(ws: WebSocket, bindings: SessionAuthBindings) -> bool:
    """Validate the WS query param `t`. Closes the WS with an appropriate
    code on failure. Returns True if the connection should proceed."""
    secret = os.environ.get("SESSION_JWT_SECRET", "")
    bound_tid = _resolve_bound_tid(bindings)
    bound_fingerprint = _resolve_bound_session_identity_fingerprint(bindings)
    if not secret or not bound_tid or bound_fingerprint is None:
        # Misconfigured pod — fail closed.
        await ws.accept()
        await ws.close(code=4500, reason="pod missing session auth config")
        return False

    token = ws.query_params.get("t")
    if not token:
        await ws.accept()
        await ws.close(code=4401, reason="missing session token")
        return False

    try:
        claims = jwt.decode(
            token,
            secret,
            algorithms=["HS256"],
            audience="agent",
            leeway=2,
            options={
                "require": ["exp", "iat", "aud", "sub", "tid", "sif"],
                "verify_signature": True,
                "verify_exp": True,
                "verify_iat": True,
                "verify_aud": True,
            },
        )
    except jwt.PyJWTError as e:
        logger.warning("ws_chat: invalid session token: %s", e)
        await ws.accept()
        await ws.close(code=4401, reason="invalid session token")
        return False

    if str(claims.get("tid") or "") != bound_tid:
        logger.warning(
            "ws_chat: token tid %r != bound %r — rejecting",
            claims.get("tid"),
            bound_tid,
        )
        await ws.accept()
        await ws.close(code=4403, reason="session token mismatch")
        return False

    if claims.get("sif") != bound_fingerprint:
        logger.warning("ws_chat: token session identity does not match local binding")
        await ws.accept()
        await ws.close(code=4403, reason="session token identity mismatch")
        return False

    ws.state.session_identity_fingerprint = bound_fingerprint

    return True
