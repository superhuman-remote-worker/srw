"""The exact registered process a pinned session mutation may be delivered to.

Extracted verbatim from ``orchestrator.main`` (R1.B06 lane A, census group
``R_ATTACH``). Everything here exists to answer one question — *is this
still the same process?* — and to answer it by **comparison** rather than
reconstruction, which is port contract §P6.

Properties moved unchanged:

* **Two transports, two proofs, never mixed.** A Pod-backed agent
  (``pod_uid`` present) is resolved through PostgreSQL's
  ``get_pinned_session_binding`` and then attested against the live
  namespace/Pod coordinate; a pool process with no ``pod_uid`` is resolved
  through :func:`local_pinned_session_target_matches`, which additionally
  refuses any row that grew a ``pod_uid``. Neither path may fall back to the
  other. An unattested Pod target is refused, never downgraded to the local
  comparison.
* **The process generation is read, not derived.**
  :func:`agent_process_generation` is the single reader of
  ``metadata.dispatch_process_generation`` and is the only thing entitled to
  say what generation a row carries. An absent or blank generation is not a
  wildcard — :func:`prepare_pinned_session_mutation_target` refuses it.
* **The ``/ready`` GET is a rollout fence, not a health check.** It carries
  no credentials and no user input; it exists so a mixed-rollout runtime that
  would ignore the ``_recipient`` envelope never receives the mutation.
  A runtime that does not advertise ``pinned_session_recipient_binding`` is
  refused, and so is one already claiming a *different* thread. Any transport
  or decode failure is a refusal (§P7 — a fence that cannot prove authority
  refuses).
* **Every fact is re-read after every await.**
  :func:`pinned_session_mutation_target_is_current` re-reads the agent row,
  the binding and the Kubernetes attestation, and
  :func:`prepare_pinned_session_mutation_target` ends by calling it — a target
  that was true before the ``/ready`` round trip is not evidence after it.

``attest_pinned_session_mutation_pod`` and
``pinned_session_mutation_target_is_current`` are reached through the
dependency dataclass even from inside this module. That is not indirection
for its own sake: both are patched on ``orchestrator.main`` by the recipient
suite to drive the refusal branches, and resolving them in this module's own
namespace would make those patches green but inert (port contract §P3).
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, NamedTuple

import httpx

from orchestrator.services.session_runtime_admission import thread_runtime_authority
from shared.pinned_session_identity import PinnedSessionBinding


class PinnedSessionMutationTarget(NamedTuple):
    agent: dict[str, Any]
    binding: PinnedSessionBinding | None
    recipient: dict[str, Any]
    process_generation: str
    runtime_generation: str
    attach_token: str


@dataclass(frozen=True)
class PinnedSessionMutationTargetDependencies:
    """Collaborators for one recipient resolution, rebuilt per invocation.

    * ``store`` — main's ``postgres_db``. Rebound wholesale by tests, so it is
      never captured at import.
    * ``agent_provisioner`` / ``persistent_provisioner`` — the two Kubernetes
      attestation authorities. Injected rather than imported because tests
      patch attributes on the main-namespace singletons, and because
      :func:`attest_pinned_session_mutation_pod` must keep choosing between
      them by the *stored* hostname rather than by a rebuilt name.
    * ``attest_pinned_session_mutation_pod`` — main's bridge, injected so a
      patch there steers both the prepare-time and the re-read attestation.
      Signature ``(*, binding) -> Awaitable[bool]``.
    * ``pinned_session_mutation_target_is_current`` — main's bridge, injected
      for the same reason; it is the tail check of
      :func:`prepare_pinned_session_mutation_target`. Signature
      ``(target) -> Awaitable[bool]``.
    """

    store: Any
    agent_provisioner: Any
    persistent_provisioner: Any
    attest_pinned_session_mutation_pod: Callable[..., Awaitable[bool]]
    pinned_session_mutation_target_is_current: Callable[..., Awaitable[bool]]


def agent_process_generation(agent: Mapping[str, Any]) -> str:
    metadata = agent.get("metadata") or {}
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
        except (TypeError, ValueError):
            metadata = {}
    return (
        str(metadata.get("dispatch_process_generation") or "").strip()
        if isinstance(metadata, Mapping)
        else ""
    )


async def attest_pinned_session_mutation_pod(
    *,
    binding: PinnedSessionBinding,
    dependencies: PinnedSessionMutationTargetDependencies,
) -> bool:
    """Prove the exact namespace/Pod coordinate selected by PostgreSQL."""

    if binding.agent_hostname == f"persistent-{binding.thread_id[:12]}":
        return (
            await dependencies.persistent_provisioner.attest_pinned_session_recipient(
                binding.agent_hostname,
                thread_id=binding.thread_id,
                expected_runtime_generation=binding.runtime_generation,
                expected_pod_uid=binding.pod_uid,
                expected_pod_ip=binding.pod_ip,
                namespace=binding.pod_namespace,
            )
        )
    return await dependencies.agent_provisioner.attest_pinned_session_recipient(
        binding.agent_hostname,
        thread_id=binding.thread_id,
        expected_runtime_generation=binding.runtime_generation,
        expected_pod_uid=binding.pod_uid,
        expected_pod_ip=binding.pod_ip,
        authority_kind=binding.pod_authority_kind,
        namespace=binding.pod_namespace,
    )


def local_pinned_session_target_matches(
    *,
    thread: Mapping[str, Any] | None,
    agent: Mapping[str, Any] | None,
    thread_id: str,
    agent_id: str,
    runtime_generation: str,
    attach_token: str,
    process_generation: str,
    pod_ip: str,
    pod_port: int,
) -> bool:
    """Validate the explicit non-Kubernetes pool transport after every await."""

    authority = thread_runtime_authority(thread)
    return bool(
        authority is not None
        and authority.generation == runtime_generation
        and str((thread or {}).get("agent_id") or "") == agent_id
        and str((thread or {}).get("runtime_attach_token") or "") == attach_token
        and agent
        and str(agent.get("id") or "") == agent_id
        and str(agent.get("thread_id") or "") == thread_id
        and str(agent.get("status") or "") == "session"
        and not agent.get("current_job_id")
        and not str(agent.get("pod_uid") or "").strip()
        and str(agent.get("pod_ip") or "") == pod_ip
        and int(agent.get("pod_port") or 8001) == pod_port
        and agent_process_generation(agent) == process_generation
    )


async def prepare_pinned_session_mutation_target(
    *,
    thread_id: str,
    agent_id: str,
    runtime_generation: str,
    attach_token: str,
    dependencies: PinnedSessionMutationTargetDependencies,
    required_capability: str | None = None,
) -> PinnedSessionMutationTarget | None:
    """Resolve one exact registered process before delivering session state."""

    store = dependencies.store
    agent = await store.get_agent(agent_id)
    process_generation = agent_process_generation(agent or {})
    if not agent or not process_generation or not agent.get("pod_ip"):
        return None
    pod_uid = str(agent.get("pod_uid") or "").strip() or None
    binding = None
    if pod_uid is not None:
        binding = await store.get_pinned_session_binding(
            thread_id,
            expected_runtime_generation=runtime_generation,
        )
        if not (
            binding
            and binding.agent_id == agent_id
            and binding.runtime_attach_token == attach_token
            and binding.agent_status == "session"
            and binding.agent_hostname == str(agent.get("hostname") or "")
            and binding.pod_uid == pod_uid
            and binding.pod_ip == str(agent.get("pod_ip") or "")
            and binding.pod_port == int(agent.get("pod_port") or 8001)
            and str(agent.get("thread_id") or "") == thread_id
            and not agent.get("current_job_id")
        ):
            return None
        target_ip = binding.pod_ip
        target_port = binding.pod_port
    else:
        thread = await store.get_thread(thread_id)
        target_ip = str(agent.get("pod_ip") or "")
        target_port = int(agent.get("pod_port") or 8001)
        if not local_pinned_session_target_matches(
            thread=thread,
            agent=agent,
            thread_id=thread_id,
            agent_id=agent_id,
            runtime_generation=runtime_generation,
            attach_token=attach_token,
            process_generation=process_generation,
            pod_ip=target_ip,
            pod_port=target_port,
        ):
            return None

    # This GET carries no credentials or user input. It only prevents a mixed
    # rollout runtime that would ignore the process recipient envelope from
    # receiving the subsequent mutation.
    ready_url = f"http://{target_ip}:{target_port}/ready"
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.get(ready_url)
        ready = response.json()
    except Exception:
        return None
    capabilities = ready.get("capabilities") if isinstance(ready, Mapping) else None
    observed_thread = ready.get("thread_id") if isinstance(ready, Mapping) else None
    if not (
        isinstance(capabilities, Mapping)
        and capabilities.get("pinned_session_recipient_binding") is True
        and (required_capability is None or capabilities.get(required_capability) is True)
        and observed_thread in {None, "", thread_id}
    ):
        return None
    if (
        binding is not None
        and not await dependencies.attest_pinned_session_mutation_pod(binding=binding)
    ):
        return None

    target = PinnedSessionMutationTarget(
        agent=dict(agent),
        binding=binding,
        process_generation=process_generation,
        runtime_generation=runtime_generation,
        attach_token=attach_token,
        recipient={
            "expected_thread_id": thread_id,
            "expected_agent_id": agent_id,
            "expected_pod_uid": pod_uid,
            "expected_process_generation": process_generation,
        },
    )
    return (
        target
        if await dependencies.pinned_session_mutation_target_is_current(target)
        else None
    )


async def pinned_session_mutation_target_is_current(
    target: PinnedSessionMutationTarget,
    *,
    dependencies: PinnedSessionMutationTargetDependencies,
) -> bool:
    """Re-read every DB/Kubernetes fact after an await or HTTP response."""

    store = dependencies.store
    recipient = target.recipient
    thread_id = str(recipient["expected_thread_id"])
    agent_id = str(recipient["expected_agent_id"])
    fresh_agent = await store.get_agent(agent_id)
    if target.binding is None:
        fresh_thread = await store.get_thread(thread_id)
        return local_pinned_session_target_matches(
            thread=fresh_thread,
            agent=fresh_agent,
            thread_id=thread_id,
            agent_id=agent_id,
            runtime_generation=target.runtime_generation,
            attach_token=target.attach_token,
            process_generation=target.process_generation,
            pod_ip=str(target.agent.get("pod_ip") or ""),
            pod_port=int(target.agent.get("pod_port") or 8001),
        )

    fresh_binding = await store.get_pinned_session_binding(
        thread_id,
        expected_runtime_generation=target.binding.runtime_generation,
    )
    if not (
        fresh_binding
        and fresh_binding.target_key == target.binding.target_key
        and fresh_binding.agent_status == "session"
        and fresh_agent
        and str(fresh_agent.get("thread_id") or "") == thread_id
        and str(fresh_agent.get("status") or "") == "session"
        and not fresh_agent.get("current_job_id")
        and agent_process_generation(fresh_agent) == target.process_generation
    ):
        return False
    return await dependencies.attest_pinned_session_mutation_pod(binding=fresh_binding)
