"""Recovering the successor runtime a failed pinned attach left behind.

Extracted verbatim from ``orchestrator.main`` (R1.B06 lane A, census group
``R_ATTACH``). This is deliberately **not** merged into
``session_attach_binding``: the binding module owns an atomic authority
transition that either commits or does not, while this module owns a
restartable, at-least-once actuator sequence whose failure mode is "leave G2
open and try again". Folding them together would let one module's
optimistic-retry habits reach code that must be all-or-nothing.

The ordering here is contract, not implementation detail (port contract §P9).
Attach failure → abort (which rotates G1 to G2 and appends a durable outcome
row) → :func:`prepare_attach_abort_successor_workspace` →
:func:`reconcile_attach_abort_successor`, driven by
:func:`schedule_attach_abort_successor`. Each step re-proves the exact edge it
was handed:

* **The append-only abort outcome is the work item.** Nothing here recaptures
  an arbitrary "current" generation. Every entry point parses the outcome's
  UUIDs strictly, refuses a successor equal to the retired generation, and
  re-checks :func:`current_attach_abort_successor` after every await — the
  thread must still be the exact open, unbound, un-retired, created-status G2
  that the outcome named.
* **``workspace_process_zero_v1`` is stronger than tmux cleanup.** It killed
  every workspace-user process including the entrypoint's code-server, so the
  Ready Pod cannot be reused: that protocol alone owns exact-UID deletion (PVC
  retained), the endpoint CAS, fresh U2 creation and an IDE health proof. The
  three other protocols did not kill workspace residents and pass straight
  through. An unrecognised protocol returns ``None`` — it never guesses.
* **Static Docker workspaces fail closed.** They have no exact restart
  actuator, so the successor refuses rather than advertising a dead
  code-server lease as Ready. A retry that finds a *different* live lease
  never releases or rewrites it.
* **Exactly once per retired edge.** :func:`schedule_attach_abort_successor`
  keys its task on the full ``(thread, retired generation, retired attach
  token, retired agent)`` tuple, so duplicate lost-response retries share one
  task and every stale G1/G3 continuation is a no-op.
* **The advisory lock is released before provisioning.**
  :func:`reconcile_attach_abort_successor` deliberately drops the thread lock
  before calling ``provision_or_assign``, whose own lifecycle lock must never
  nest beneath it; the post-release re-read is the CAS that makes a lost race
  a silent no-op.

The task registry is a field on :class:`SessionAttachRecoveryDependencies`
rather than a module global. ``orchestrator.main`` keeps owning that dict —
this module never creates a registry of its own (port contract §P10) and never
captures one at import (§P1).
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Awaitable, Callable
from uuid import UUID

from orchestrator.services.container_provisioner import (
    WORKSPACE_RUNTIME_INCARNATION_KEY,
)
from orchestrator.services.session_runtime_admission import (
    same_thread_runtime_authority,
    thread_runtime_authority,
)
from orchestrator.services.session_runtime_identity import thread_uses_pinned_execution
from orchestrator.services.stateless_workspace_gate import thread_metadata_object
from orchestrator.services.vm_thread_initial import initial_thread_vm_blocks_runtime
from orchestrator.services.workspace_binding import (
    CANVAS_WORKSPACE_GENERATION_KEY,
    remote_canvas_presentation_available,
)
from orchestrator.services.workspace_lifecycle import EnsureOutcome, WorkspaceOwner
from shared.runtime.core.loader import canonical_config_name

logger = logging.getLogger(__name__)

AttachAbortSuccessorTaskKey = tuple[str, str, str, str]


@dataclass(frozen=True)
class SessionAttachRecoveryDependencies:
    """Collaborators for one abort-successor recovery, per invocation.

    * ``store`` — main's ``postgres_db``.
    * ``container_provisioner`` / ``docker_provisioner`` — the two workspace
      actuators. Injected because tests patch attributes on the main-namespace
      singletons.
    * ``workspace_suspension_service`` — passed straight through to
      ``ensure_session_workspace``; rebound on main by several suites.
    * ``ensure_session_workspace`` — B05's session provisioner entry point,
      reached through main's name because it is patched there.
    * ``thread_project_ids`` — main's ``_thread_project_ids`` (B05 root).
    * ``reconcile_attach_abort_successor`` — main's own bridge, injected so a
      patch there steers the scheduled task body. Signature
      ``(candidate) -> Awaitable[bool]``.
    * ``successor_tasks`` — the in-flight task registry ``orchestrator.main``
      owns, keyed by ``(thread_id, retired_runtime_generation,
      retired_attach_token, retired_agent_id)``. Handed in rather than created
      here, so main stays the single owner of request-path task state.
    """

    store: Any
    container_provisioner: Any
    docker_provisioner: Any
    workspace_suspension_service: Any
    ensure_session_workspace: Callable[..., Awaitable[Any]]
    thread_project_ids: Callable[[str], Awaitable[list[str]]]
    reconcile_attach_abort_successor: Callable[..., Awaitable[bool]]
    provision_or_assign: Callable[..., Awaitable[None]]
    successor_tasks: dict[AttachAbortSuccessorTaskKey, "asyncio.Task[None]"]


def current_attach_abort_successor(
    thread: Mapping[str, Any] | None,
    *,
    thread_id: str,
    successor_generation: str,
) -> bool:
    """Return whether an abort outcome still owns its exact open G2."""

    authority = thread_runtime_authority(thread)
    return bool(
        authority is not None
        and authority.thread_id == thread_id
        and authority.generation == successor_generation
        and thread_uses_pinned_execution(thread)
        and str(thread.get("status") or "") == "created"
        and thread.get("runtime_retirement_token") is None
        and thread.get("agent_id") is None
        and thread.get("runtime_attach_token") is None
        and not initial_thread_vm_blocks_runtime(thread)
    )


async def prepare_attach_abort_successor_workspace(
    candidate: Mapping[str, Any],
    current: Mapping[str, Any],
    *,
    dependencies: SessionAttachRecoveryDependencies,
) -> Mapping[str, Any] | None:
    """Recreate the workspace killed by an exact delivered-attach abort.

    ``workspace_process_zero_v1`` is intentionally stronger than tmux cleanup:
    it kills every workspace-user process, including the entrypoint's
    code-server.  Reusing that Ready Pod would make G2 appear healthy while its
    IDE is dead.  The append-only G1 outcome therefore owns exact UID deletion
    (PVC retained), a G2/endpoint CAS, fresh U2 creation, and an IDE health
    proof.  Other abort protocols did not kill workspace residents and skip
    this actuator.
    """

    store = dependencies.store
    if initial_thread_vm_blocks_runtime(current):
        return None
    container_provisioner = dependencies.container_provisioner
    docker_provisioner = dependencies.docker_provisioner

    protocol = str(candidate.get("quiescence_protocol") or "")
    if protocol in {
        "pre_delivery_no_payload_v1",
        "agent_attach_not_started_v1",
        "agent_runtime_zero_v1",
    }:
        return current
    if protocol != "workspace_process_zero_v1":
        return None

    try:
        thread_id = str(UUID(str(candidate.get("thread_id") or "")))
        retired_generation = str(
            UUID(str(candidate.get("retired_runtime_generation") or ""))
        )
        retired_attach_token = str(
            UUID(str(candidate.get("retired_attach_token") or ""))
        )
        retired_agent_id = str(UUID(str(candidate.get("retired_agent_id") or "")))
        successor_generation = str(
            UUID(str(candidate.get("successor_generation") or ""))
        )
        workspace_generation = str(
            UUID(str(candidate.get("workspace_generation") or ""))
        )
        retired_workspace_runtime = str(
            UUID(str(candidate.get("workspace_runtime_incarnation") or ""))
        )
    except (TypeError, ValueError):
        return None

    if not current_attach_abort_successor(
        current,
        thread_id=thread_id,
        successor_generation=successor_generation,
    ):
        return None
    metadata = thread_metadata_object(current)
    workspace = metadata.get("workspace_container") or {}
    binding = metadata.get("_workspace_binding") or {}
    if not isinstance(workspace, Mapping) or not isinstance(binding, Mapping):
        return None
    if str(workspace.get("provisioner") or "") == "docker":
        if str(binding.get("generation") or "") != workspace_generation:
            return None
        current_lease = str(workspace.get("_docker_workspace_lease_id") or "")
        if current_lease == retired_workspace_runtime:
            if not await docker_provisioner.release_thread_workspace(
                thread_id,
                expected_lease_id=retired_workspace_runtime,
                force_quarantine=True,
            ):
                return None
            if not await store.clear_pinned_attach_abort_docker_workspace_endpoint(
                thread_id,
                retired_runtime_generation=retired_generation,
                retired_attach_token=retired_attach_token,
                retired_agent_id=retired_agent_id,
                successor_generation=successor_generation,
                workspace_generation=workspace_generation,
                docker_lease_id=retired_workspace_runtime,
            ):
                return None
            current = await store.get_thread(thread_id)
            if not current_attach_abort_successor(
                current,
                thread_id=thread_id,
                successor_generation=successor_generation,
            ):
                return None
            return current
        # A retry may observe the exact G2 after provision_or_assign already
        # installed a distinct lease. Never release or rewrite that successor.
        if current_lease and current_lease != retired_workspace_runtime:
            return current if str(workspace.get("status") or "") == "ready" else None
        # The exact terminal-lease CAS already cleared the old mirror; the
        # caller may now provision a fresh Docker lease for G2.
        if not current_lease and str(workspace.get("status") or "") == "deleted":
            return current
        return None
    if (
        str(workspace.get("provisioner") or "") != "k8s"
        or str(binding.get("generation") or "") != workspace_generation
    ):
        # Static Docker workspaces have no exact restart actuator.  Their
        # all-UID-zero receipt remains durable, but the successor fails closed
        # instead of advertising a dead code-server lease as Ready.
        return None

    current_runtime = str(workspace.get(WORKSPACE_RUNTIME_INCARNATION_KEY) or "")
    if current_runtime == retired_workspace_runtime:
        owner = WorkspaceOwner.session(thread_id)
        pod_authority = await container_provisioner.workspace_pod_authority(
            owner,
            expected_runtime_incarnation=retired_workspace_runtime,
        )
        if pod_authority in {"exact_live", "exact_terminal"}:
            deleted = await container_provisioner.delete_workspace(
                owner,
                expected_runtime_incarnation=retired_workspace_runtime,
                captured_teardown_uid=retired_workspace_runtime,
                wait_for_exact_absence=True,
                defer_context_clear=True,
            )
            if not deleted:
                return None
        elif pod_authority != "exact_absent":
            # A same-name replacement or ambiguous control-plane result is not
            # deletion authority over either endpoint.
            return None

        cleared = await store.clear_pinned_attach_abort_workspace_endpoint(
            thread_id,
            retired_runtime_generation=retired_generation,
            retired_attach_token=retired_attach_token,
            retired_agent_id=retired_agent_id,
            successor_generation=successor_generation,
            workspace_generation=workspace_generation,
            workspace_runtime_incarnation=retired_workspace_runtime,
        )
        if not cleared:
            return None
        current = await store.get_thread(thread_id)
        if not current_attach_abort_successor(
            current,
            thread_id=thread_id,
            successor_generation=successor_generation,
        ):
            return None
        metadata = thread_metadata_object(current)
        workspace = metadata.get("workspace_container") or {}
        binding = metadata.get("_workspace_binding") or {}
        if (
            not isinstance(workspace, Mapping)
            or not isinstance(binding, Mapping)
            or str(binding.get("generation") or "") != workspace_generation
        ):
            return None
        current_runtime = str(workspace.get(WORKSPACE_RUNTIME_INCARNATION_KEY) or "")

    if not current_runtime:
        ensured = await dependencies.ensure_session_workspace(
            thread_id,
            db=store,
            provisioner=container_provisioner,
            suspension=dependencies.workspace_suspension_service,
            expected_runtime_generation=successor_generation,
            _pinned_runtime_lock_held=True,
        )
        if ensured is None or ensured.outcome == EnsureOutcome.FAILED:
            return None
        current = await store.get_thread(thread_id)
        if not current_attach_abort_successor(
            current,
            thread_id=thread_id,
            successor_generation=successor_generation,
        ):
            return None
        metadata = thread_metadata_object(current)
        workspace = metadata.get("workspace_container") or {}
        binding = metadata.get("_workspace_binding") or {}
        if not isinstance(workspace, Mapping) or not isinstance(binding, Mapping):
            return None
        current_runtime = str(workspace.get(WORKSPACE_RUNTIME_INCARNATION_KEY) or "")

    # A crash after creation but before provisioning re-enters here with U2
    # already Ready.  Accept only a different, fully attested K8s incarnation
    # on the same durable backing; U1 can never be re-advertised.
    try:
        current_runtime = str(UUID(current_runtime))
    except (TypeError, ValueError):
        return None
    if (
        current_runtime == retired_workspace_runtime
        or str(workspace.get("status") or "") != "ready"
        or str(workspace.get("provisioner") or "") != "k8s"
        or str(binding.get("generation") or "") != workspace_generation
        or str(workspace.get(CANVAS_WORKSPACE_GENERATION_KEY) or "")
        != workspace_generation
        or not remote_canvas_presentation_available(metadata, dict(workspace))
    ):
        return None
    if not await container_provisioner.wait_for_workspace_code_server(
        WorkspaceOwner.session(thread_id),
        expected_runtime_incarnation=current_runtime,
    ):
        return None

    final = await store.get_thread(thread_id)
    if not current_attach_abort_successor(
        final,
        thread_id=thread_id,
        successor_generation=successor_generation,
    ):
        return None
    final_metadata = thread_metadata_object(final)
    final_workspace = final_metadata.get("workspace_container") or {}
    final_binding = final_metadata.get("_workspace_binding") or {}
    if (
        not isinstance(final_workspace, Mapping)
        or not isinstance(final_binding, Mapping)
        or str(final_workspace.get("status") or "") != "ready"
        or str(final_workspace.get("provisioner") or "") != "k8s"
        or str(final_workspace.get(WORKSPACE_RUNTIME_INCARNATION_KEY) or "")
        != current_runtime
        or str(final_workspace.get(CANVAS_WORKSPACE_GENERATION_KEY) or "")
        != workspace_generation
        or str(final_binding.get("generation") or "") != workspace_generation
        or not remote_canvas_presentation_available(
            final_metadata, dict(final_workspace)
        )
    ):
        return None
    return final


async def reconcile_attach_abort_successor(
    candidate: Mapping[str, Any],
    *,
    dependencies: SessionAttachRecoveryDependencies,
) -> bool:
    """Provision only the exact unbound G2 named by a durable abort outcome.

    The append-only outcome is the restart-safe work item.  A request-local
    task gives the common path low latency, while the stale-agent sweep calls
    this same routine until G2 is bound/provisioning or ceases to be current.
    No code is permitted to recapture an arbitrary current generation after a
    failed G1 attach.
    """

    store = dependencies.store
    try:
        thread_id = str(UUID(str(candidate.get("thread_id") or "")))
        retired_generation = str(
            UUID(
                str(
                    candidate.get("retired_runtime_generation")
                    or candidate.get("runtime_generation")
                    or ""
                )
            )
        )
        successor_generation = str(
            UUID(str(candidate.get("successor_generation") or ""))
        )
    except (TypeError, ValueError):
        return False
    if successor_generation == retired_generation:
        return False

    async with store.try_thread_advisory_lock(thread_id) as lock_owner:
        if not lock_owner:
            return False
        current = await store.get_thread(thread_id)
        if not current_attach_abort_successor(
            current,
            thread_id=thread_id,
            successor_generation=successor_generation,
        ):
            return False
        current = await prepare_attach_abort_successor_workspace(
            candidate, current, dependencies=dependencies
        )
        if current is None:
            return False
        authority = thread_runtime_authority(current)
        if authority is None:
            return False
        metadata = thread_metadata_object(current)
        config_override = metadata.get("config_override") or {}
        if not isinstance(config_override, dict):
            return False
        datasource_ids = metadata.get("datasource_ids")
        if not isinstance(datasource_ids, list):
            datasource_ids = None
        current_user_id = str(current.get("user_id") or "system")
        current_config_name = canonical_config_name(
            str(current.get("config_name") or "session_base")
        )

    # Never nest provision_or_assign's lifecycle lock beneath this owner. Its
    # exact G2 authority check is the post-release CAS: if End/Resume/G3 wins
    # here, the delayed G2 provision task becomes a no-op.
    project_ids = await dependencies.thread_project_ids(thread_id)
    latest = await store.get_thread(thread_id)
    if not same_thread_runtime_authority(
        latest, authority
    ) or not current_attach_abort_successor(
        latest,
        thread_id=thread_id,
        successor_generation=successor_generation,
    ):
        return False
    await dependencies.provision_or_assign(
        current_user_id,
        thread_id,
        current_config_name,
        config_override,
        project_ids,
        datasource_ids,
        runtime_generation=successor_generation,
    )
    return True


def schedule_attach_abort_successor(
    thread_id: str,
    *,
    retired_runtime_generation: str,
    retired_attach_token: str,
    retired_agent_id: str,
    dependencies: SessionAttachRecoveryDependencies,
) -> "asyncio.Task[None]":
    """Strongly own provisioning of the exact successor G after attach abort.

    The G1 create/prepare task must stop after the generation rotation, but a
    browser poll does not resubmit prepare and headless callers may never
    reconnect.  The append-only abort outcome names G2; this owner reads that
    exact edge, then provisions only while G2 is still created/open/unbound.
    Duplicate lost-response retries share one task and every stale G1/G3
    continuation is a no-op.
    """

    store = dependencies.store
    successor_tasks = dependencies.successor_tasks

    task_key = (
        thread_id,
        retired_runtime_generation,
        retired_attach_token,
        retired_agent_id,
    )
    existing = successor_tasks.get(task_key)
    if existing is not None and not existing.done():
        return existing

    async def _run() -> None:
        try:
            async with store.acquire() as conn:
                outcome = await conn.fetchrow(
                    "SELECT thread_id, runtime_generation AS "
                    "retired_runtime_generation, runtime_attach_token AS "
                    "retired_attach_token, agent_id AS retired_agent_id, "
                    "successor_generation, quiescence_protocol, "
                    "workspace_generation, workspace_runtime_incarnation FROM "
                    "thread_runtime_attach_abort_outcomes "
                    "WHERE thread_id=$1::uuid AND runtime_generation=$2::uuid "
                    "AND runtime_attach_token=$3::uuid AND agent_id=$4::uuid",
                    thread_id,
                    retired_runtime_generation,
                    retired_attach_token,
                    retired_agent_id,
                )
            if outcome is None:
                return
            await dependencies.reconcile_attach_abort_successor(dict(outcome))
        except Exception:
            logger.exception(
                "Failed to reconcile successor runtime after exact attach "
                "abort (thread=%s retired_generation=%s)",
                thread_id,
                retired_runtime_generation,
            )

    task = asyncio.create_task(
        _run(),
        name=f"attach-abort-successor-{thread_id[:8]}-{retired_runtime_generation[:8]}",
    )
    successor_tasks[task_key] = task

    def _done(finished: "asyncio.Task[None]") -> None:
        if successor_tasks.get(task_key) is finished:
            successor_tasks.pop(task_key, None)

    task.add_done_callback(_done)
    return task
