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
import math
from typing import Any
from uuid import UUID

import httpx

from orchestrator.services.notification_feed import notification_feed
from orchestrator.services.session_runtime_admission import (
    ThreadRuntimeAuthority,
    same_thread_runtime_authority,
    thread_runtime_is_preparable,
)
from orchestrator.services.vm_thread_initial import initial_vm_startup_view

logger = logging.getLogger(__name__)
_INITIAL_VM_COMMUNICATION_ALLOWANCE_S = 120


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
    vm_store: Any | None = None,
    vm_thread_id: str | None = None,
    vm_runtime_generation: str | None = None,
    vm_binding: Any | None = None,
) -> bool:
    """Poll the agent pod's /ready until it returns ready=true, or timeout.

    The agent's ``/ready`` returns ``ready=true`` only once
    ``_session_ready()`` passes its 3-way check (session attached,
    LLM tools wired, loop queue initialized). Mid-attach windows
    correctly return False, so this is the truthful signal for "the
    cockpit's WS will succeed if opened now." VM callers also pass the exact
    captured binding. Only a current initial source may renew the communication
    allowance, and durable admission consumes the original finite budget.
    """
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout_s
    interval = 2
    vm_observer = (
        vm_store is not None
        and vm_thread_id is not None
        and vm_runtime_generation is not None
        and vm_binding is not None
    )
    if (
        any(
            value is not None
            for value in (vm_store, vm_thread_id, vm_runtime_generation, vm_binding)
        )
        and not vm_observer
    ):
        return False
    if vm_observer:
        try:
            if (
                vm_thread_id != vm_binding.thread_id
                or vm_runtime_generation != vm_binding.runtime_generation
                or pod_ip != vm_binding.pod_ip
                or pod_port != vm_binding.pod_port
                or expected_session_identity_fingerprint
                != vm_binding.session_identity_fingerprint
            ):
                return False
        except (AttributeError, ValueError):
            return False
    source_key: tuple[str, str, str] | None = None
    communication_deadline: float | None = None
    admission_deadline: float | None = None
    unavailable = object()

    async def current_vm_thread():
        """Fence each asynchronous observation to the captured actor and pod."""
        try:
            thread = await vm_store.get_thread(vm_thread_id)
            expected = ThreadRuntimeAuthority(vm_thread_id, vm_runtime_generation)
            if (
                not same_thread_runtime_authority(thread, expected)
                or str(thread.get("agent_id")) != vm_binding.agent_id
                or str(thread.get("runtime_attach_token"))
                != vm_binding.runtime_attach_token
            ):
                return None
            current_binding = await vm_store.get_pinned_session_binding(
                vm_thread_id,
                expected_runtime_generation=vm_runtime_generation,
            )
            if (
                current_binding is None
                or current_binding.target_key != vm_binding.target_key
                or current_binding.agent_status
                not in {"booting", "ready", "working", "session"}
            ):
                return None
            # The joined binding read is an await: End may race without G rotation.
            thread = await vm_store.get_thread(vm_thread_id)
            if (
                not same_thread_runtime_authority(thread, expected)
                or str(thread.get("agent_id")) != vm_binding.agent_id
                or str(thread.get("runtime_attach_token"))
                != vm_binding.runtime_attach_token
            ):
                return None
            return thread
        except Exception as exc:
            logger.warning(
                "VM readiness authority unavailable for thread %s (%s)",
                vm_thread_id,
                type(exc).__name__,
            )
            return unavailable

    while True:
        now = loop.time()
        prior_deadline = (
            admission_deadline
            if admission_deadline is not None
            else communication_deadline
            if communication_deadline is not None
            else deadline
        )
        if now >= prior_deadline:
            return False
        phase = None
        if vm_observer:
            thread = await current_vm_thread()
            if thread is unavailable:
                await asyncio.sleep(interval)
                continue
            if thread is None:
                return False
            try:
                view = await initial_vm_startup_view(thread, store=vm_store)
            except Exception as exc:
                # A temporary source-read outage spends the existing allowance.
                logger.warning(
                    "Initial VM readiness source unavailable for thread %s (%s)",
                    vm_thread_id,
                    type(exc).__name__,
                )
                view = None
            current = await current_vm_thread()
            if current is unavailable:
                await asyncio.sleep(interval)
                continue
            if current is None:
                return False
            now = loop.time()
            if now >= prior_deadline:
                return False
            if view is not None:
                if not isinstance(view, dict):
                    return False
                try:
                    key = tuple(
                        str(UUID(view[field]))
                        for field in (
                            "request_id",
                            "provision_generation",
                            "runtime_generation",
                        )
                    )
                    valid = (
                        type(view.get("contract_version")) is int
                        and view["contract_version"] == 1
                        and key[2] == vm_runtime_generation
                        and view.get("phase") in {"resource_wait", "admitted"}
                    )
                    if not valid or (source_key is not None and key != source_key):
                        return False
                except (KeyError, TypeError, ValueError):
                    return False
                source_key = key
                phase = view["phase"]
                if phase == "resource_wait":
                    if admission_deadline is not None:
                        return False
                    communication_deadline = now + _INITIAL_VM_COMMUNICATION_ALLOWANCE_S
                else:
                    elapsed = view.get("admission_elapsed_s")
                    if (
                        type(elapsed) not in (int, float)
                        or not math.isfinite(elapsed)
                        or elapsed < 0
                    ):
                        return False
                    candidate = now + max(0.0, timeout_s - elapsed)
                    admission_deadline = (
                        candidate
                        if admission_deadline is None
                        else min(admission_deadline, candidate)
                    )

        if admission_deadline is not None:
            live_deadline = admission_deadline
        elif communication_deadline is not None:
            live_deadline = communication_deadline
        else:
            live_deadline = deadline
        if now >= live_deadline:
            return False
        # A pre-admission wait is a timing hint only. It cannot confer Ready.
        if phase != "resource_wait" and await probe_ready(
            pod_ip,
            pod_port,
            require_protected_cloud=require_protected_cloud,
            expected_session_identity_fingerprint=(
                expected_session_identity_fingerprint
            ),
        ):
            if vm_observer:
                current = await current_vm_thread()
                if current is unavailable:
                    await asyncio.sleep(interval)
                    continue
                if current is None:
                    return False
            # Legacy and sandbox probes retain their historical in-flight
            # result. A source-attested wait has a strict irreversible bound.
            return source_key is None or loop.time() < live_deadline
        await asyncio.sleep(interval)


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
