"""Shared session-lifecycle helpers for the session-start paths.

The orchestrator has two paths that move a thread through its startup
phases: the cold path in ``routers/sessions.py::_do_prepare`` (POST
/api/sessions/{tid}/prepare) and the warm-pool fast path inside
``main.py::create_thread`` (``_provision_or_assign``). Both paths must
emit the same ``session.lifecycle`` events so the cockpit's startup card
renders identically regardless of which path bound the agent.

Single source of truth for:
  - ``emit``: broadcasting ``session.lifecycle`` events on the user's
    notification channel
  - ``wait_for_binding``: polling the caller's store until
    ``threads.agent_id`` is set
  - ``wait_for_ready``: polling the agent pod's ``/ready`` endpoint until
    the pod reports session-ready (gated by the agent-side
    ``_session_ready()`` 3-way check)
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx

from orchestrator.services.notification_feed import notification_feed
from orchestrator.services.session_runtime_admission import thread_runtime_is_preparable

logger = logging.getLogger(__name__)


def emit(user_id: str, thread_id: str, state: str, **extra: Any) -> None:
    """Broadcast locally and queue a generation-scoped cross-replica hint.

    This remains synchronous and non-blocking for the startup callers.
    """
    notification_feed.publish_lifecycle(user_id, thread_id, state, **extra)


async def wait_for_binding(thread_id: str, timeout_s: int, *, store: Any) -> bool:
    """Poll the DB until ``threads.agent_id`` is set, or wall-clock timeout.

    *store* is the caller's own database handle. Both session-start paths
    already hold one, so resolving a module singleton out of ``main`` here
    only made this helper untestable and tied it to a single application.
    """
    deadline = asyncio.get_event_loop().time() + timeout_s
    interval = 2
    while asyncio.get_event_loop().time() < deadline:
        thread = await store.get_thread(thread_id)
        if not thread_runtime_is_preparable(thread):
            return False
        if thread and thread.get("agent_id"):
            return True
        await asyncio.sleep(interval)
    return False


async def wait_for_ready(
    pod_ip: str,
    pod_port: int,
    timeout_s: int,
    *,
    require_protected_cloud: bool = False,
    expected_session_identity_fingerprint: str | None = None,
) -> bool:
    """Poll the agent pod's /ready until it returns ready=true, or timeout.

    The agent's ``/ready`` returns ``ready=true`` only once
    ``_session_ready()`` passes its 3-way check (session attached,
    LLM tools wired, loop queue initialized). Mid-attach windows
    correctly return False, so this is the truthful signal for "the
    cockpit's WS will succeed if opened now."
    """
    deadline = asyncio.get_event_loop().time() + timeout_s
    interval = 2
    while asyncio.get_event_loop().time() < deadline:
        if await probe_ready(
            pod_ip,
            pod_port,
            require_protected_cloud=require_protected_cloud,
            expected_session_identity_fingerprint=(
                expected_session_identity_fingerprint
            ),
        ):
            return True
        await asyncio.sleep(interval)
    return False


async def probe_ready(
    pod_ip: str,
    pod_port: int,
    *,
    required_capability: str | None = None,
    require_protected_cloud: bool = False,
    expected_session_identity_fingerprint: str | None = None,
) -> bool:
    """Single-shot probe of the agent pod's /ready endpoint.

    Returns True iff the agent's three-way ``_session_ready()`` check
    passes. Treats connection errors (pod down, Uvicorn not yet
    listening, timeout) as "not ready" — same shape as ``wait_for_ready``'s
    inner check, factored out so ``GET /connection`` can verify the
    binding is actually serveable before minting a token. Without this,
    ``/connection`` returns 200 based on ``agent.status`` alone, the
    cockpit opens the WS during the attach window, Traefik 503s every
    attempt, and the reconnect loop runs out before the pod's
    K8s endpoint converges.
    """
    try:
        async with httpx.AsyncClient(timeout=2) as client:
            resp = await client.get(f"http://{pod_ip}:{pod_port}/ready")
            if resp.status_code != 200:
                return False
            payload = resp.json()
            # Readiness is a security boundary for protected sessions.  Do not
            # let JSON truthiness turn malformed mixed-version payloads such
            # as ``"false"`` or ``1`` into an affirmative readiness signal.
            if payload.get("ready") is not True:
                return False
            capabilities = payload.get("capabilities")
            if not isinstance(capabilities, dict):
                return (
                    required_capability is None
                    and not require_protected_cloud
                    and expected_session_identity_fingerprint is None
                )
            if expected_session_identity_fingerprint is not None and not (
                type(capabilities.get("pinned_session_identity_contract")) is int
                and capabilities["pinned_session_identity_contract"] == 1
                and payload.get("session_identity_fingerprint")
                == expected_session_identity_fingerprint
            ):
                return False
            protected_contract = capabilities.get("protected_cloud_contract")
            if require_protected_cloud and not (
                type(protected_contract) is int
                and protected_contract == 1
                and capabilities.get("protected_cloud_ready") is True
            ):
                return False
            if required_capability is not None:
                return capabilities.get(required_capability) is True
            return True
    except Exception:
        return False
