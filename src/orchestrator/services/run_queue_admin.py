"""Operator verbs over the stateless run queue and completion commands.

R1.B06, root lane. Four admin operations that were inline in
``orchestrator.main``. Two properties travelled with them and are the reason
this is a service rather than four router bodies:

* **Unpark is the only path out of ``parked``.** Neither enqueue nor input
  recording revives a parked unit (stateless_turn_resilience §5.1), so this is
  the single writer of that edge — and it refuses while the claimant has not
  quiesced. A stopped stateless unit answers ``409 awaiting claimant
  quiescence``, and an unreadable metadata blob is treated as *stopped*, not as
  permission: ``stateless_stop_markers`` raising is a refusal, never a pass.
  The thread row is read ``FOR UPDATE`` in the same transaction that unparks.
* **Attest is the only override of claimant-loss debt.** A hold settles
  automatically only on observed exact-terminal proof; ``attest-claimant-gone``
  records a named administrator's assertion (with a durable receipt) for the
  cases Kubernetes can never prove, and refuses while the API shows the exact
  claimant running or still inside its termination grace.
* **The completion-command verbs are disabled-by-default.** With the feature
  off they answer ``404``, not ``403`` — the endpoint does not exist rather
  than existing and refusing, so nothing enumerates a disabled surface.

The flag arrives as a callable (port contract P1). Reading it at import time
would freeze whatever value the application happened to hold when this module
was first imported, which is precisely the bug that rule exists to prevent.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any, Awaitable, Callable
from uuid import UUID

from fastapi import HTTPException

from shared.session_retirement import CLAIM_LOSS_LEDGER_KEY, stateless_stop_markers

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RunQueueAdminDependencies:
    """Collaborators resolved per invocation from the handling application."""

    db: Any
    #: The admin guard; returns the acting administrator's row.
    require_admin: Callable[..., Awaitable[Any]]
    #: B11 feature gate, as a callable — never a captured value.
    completion_commands_enabled: Callable[[], bool]
    #: B08 owns the resolution authority; B06 only calls it.
    get_completion_command_resolution: Callable[[], Any]


def completion_operator_result(result: Any) -> dict[str, Any]:
    """Serialize the bounded dataclass returned by the resolution service."""

    from dataclasses import fields, is_dataclass

    if not is_dataclass(result):
        raise RuntimeError("completion operator service returned an invalid result")
    return {field.name: getattr(result, field.name) for field in fields(result)}


async def read_run_queue_model(
    *, dependencies: RunQueueAdminDependencies
) -> dict[str, Any]:
    """Operator read model for the stateless run_queue.

    ``src/shared/run_queue.list_active`` passthrough: current leases (with
    ``lease_remaining_seconds`` — negative means expired, awaiting the reaper)
    and parked units (the unpark worklist). Diagnostics only; never an input to
    correctness decisions.
    """
    from shared.run_queue import list_active

    async with dependencies.db.acquire() as conn:
        return await list_active(conn)


async def unpark_run_queue_unit(
    unit_id: str, *, dependencies: RunQueueAdminDependencies
) -> dict[str, Any]:
    """parked → queued, attempts reset, runnable now.

    404 when the unit is not currently parked; 409 while a stateless claimant
    has not quiesced.
    """
    from shared.run_queue import unpark_unit

    try:
        UUID(str(unit_id))
    except (ValueError, TypeError):
        raise HTTPException(status_code=404, detail="Unit is not parked") from None
    async with dependencies.db.acquire() as conn:
        async with conn.transaction():
            authority = await conn.fetchrow(
                "SELECT execution_lane, metadata FROM threads "
                "WHERE id = $1::uuid FOR UPDATE",
                unit_id,
            )
            if (
                authority is not None
                and str(authority["execution_lane"] or "") == "stateless"
            ):
                metadata = authority["metadata"]
                if isinstance(metadata, str):
                    try:
                        metadata = json.loads(metadata)
                    except (TypeError, ValueError):
                        metadata = None
                try:
                    markers = stateless_stop_markers(metadata)
                except RuntimeError:
                    markers = frozenset({"malformed"})
                if markers:
                    detail = "Unit is awaiting claimant quiescence"
                    if CLAIM_LOSS_LEDGER_KEY in markers:
                        # Unpark never clears claimant-loss debt; point the
                        # operator at the one audited verb that can.
                        detail += (
                            "; once the claimant process is confirmed gone, "
                            f"POST /api/admin/run-queue/{unit_id}"
                            "/attest-claimant-gone"
                        )
                    raise HTTPException(status_code=409, detail=detail)
            ok = await unpark_unit(conn, unit_id=unit_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Unit is not parked")
    logger.info("run_queue unpark: unit=%s", unit_id)
    return {"unit_id": unit_id, "state": "queued"}


def _metadata_object(value: Any) -> Any:
    if value is None:
        return {}
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (TypeError, ValueError):
            return None
    return value


async def attest_claimant_gone(
    unit_id: str,
    *,
    pod: str,
    pod_uid: str,
    reason: str,
    admin: Any,
    dependencies: RunQueueAdminDependencies,
) -> dict[str, Any]:
    """Settle claimant-loss debt whose exact claimant a human confirmed gone.

    The reaper settles a hold only on the exact Pod UID observed with every
    container terminated. A 404 (force delete, node GC, a pre-finalizer Pod)
    or a lost node's frozen statuses can never prove that, by design, so this
    audited verb is the override. It never contradicts a live observation:
    409 while the exact Pod runs or is still inside its termination grace.
    It ACKs every debt naming the exact ``(pod, pod_uid)`` with
    ``quiesced_by="operator:<admin id>"`` and a durable receipt, and — when
    that Pod is still retained by the executor finalizer and no other claim
    names it — releases the finalizer as well.
    """
    from datetime import datetime, timezone

    from orchestrator.services.agent_provisioner import (
        STATELESS_EXECUTOR_PROCESS_ZERO_FINALIZER,
        agent_provisioner,
    )
    from orchestrator.services.run_queue_reaper import unreferenced_executor_uids
    from shared.session_retirement import (
        CLAIM_LOSS_HOLD_KEY,
        acknowledge_session_claim_quiesced,
        unresolved_claim_losses,
    )

    not_found = HTTPException(
        status_code=404, detail="No unresolved claimant-loss debt names that pod"
    )
    pod_name = str(pod or "").strip()
    uid = str(pod_uid or "").strip()
    try:
        UUID(str(unit_id))
        UUID(uid)
    except (ValueError, TypeError):
        raise not_found from None
    async with dependencies.db.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT execution_lane, metadata FROM threads WHERE id = $1::uuid",
            unit_id,
        )
    if row is None or str(row["execution_lane"] or "") != "stateless" or not pod_name:
        raise not_found
    try:
        losses = unresolved_claim_losses(_metadata_object(row["metadata"]))
    except RuntimeError:
        raise HTTPException(
            status_code=409,
            detail="Claimant-loss ledger is malformed; it cannot be attested",
        ) from None
    tokens = sorted(
        token
        for token, authority in losses.items()
        if authority.pod == pod_name and authority.pod_uid == uid
    )
    if not tokens:
        raise not_found

    kubernetes_authority = await agent_provisioner.agent_pod_authority(
        pod_name, expected_pod_uid=uid
    )
    if kubernetes_authority == "exact_live":
        raise HTTPException(
            status_code=409,
            detail="Claimant pod is running in the Kubernetes API; "
            "it cannot be attested gone",
        )
    observed = await agent_provisioner.read_stateless_executor_pod(
        pod_name, expected_pod_uid=uid
    )
    # Kubernetes stamps deletionTimestamp at the END of the graceful window.
    deadline = getattr(getattr(observed, "metadata", None), "deletion_timestamp", None)
    if isinstance(deadline, datetime) and deadline.tzinfo is None:
        deadline = deadline.replace(tzinfo=timezone.utc)
    if (
        kubernetes_authority != "exact_terminal"
        and isinstance(deadline, datetime)
        and deadline > datetime.now(timezone.utc)
    ):
        raise HTTPException(
            status_code=409,
            detail="Claimant pod is inside its termination grace until "
            f"{deadline.isoformat()}; the kubelet may still report it terminal",
        )

    actor = f"operator:{admin['id']}"
    settled: list[int] = []
    for token in tokens:
        if await acknowledge_session_claim_quiesced(
            dependencies.db,
            thread_id=unit_id,
            previous_lease_token=token,
            leased_by=pod_name,
            pod_uid=uid,
            quiesced_by=actor,
            receipt={
                "evidence": "operator_attestation",
                "kubernetes_authority": kubernetes_authority,
                "reason": reason,
            },
        ):
            settled.append(token)
    if not settled:
        raise HTTPException(
            status_code=409,
            detail="Claimant-loss debt changed concurrently; re-read and retry",
        )
    logger.warning(
        "run_queue claimant attested gone: unit=%s pod=%s uid=%s tokens=%s "
        "kubernetes=%s by=%s reason=%r",
        unit_id,
        pod_name,
        uid,
        settled,
        kubernetes_authority,
        actor,
        reason,
    )

    async with dependencies.db.acquire() as conn:
        after = _metadata_object(
            await conn.fetchval(
                "SELECT metadata FROM threads WHERE id = $1::uuid", unit_id
            )
        )
    hold_released = isinstance(after, dict) and not (
        {CLAIM_LOSS_LEDGER_KEY, CLAIM_LOSS_HOLD_KEY} & set(after)
    )

    finalizer_released = False
    retained = await agent_provisioner.read_stateless_executor_pod(
        pod_name, expected_pod_uid=uid
    )
    retained_metadata = getattr(retained, "metadata", None)
    if (
        retained is not None
        and getattr(retained_metadata, "deletion_timestamp", None) is not None
        and STATELESS_EXECUTOR_PROCESS_ZERO_FINALIZER
        in (getattr(retained_metadata, "finalizers", None) or [])
    ):
        async with dependencies.db.acquire() as conn:
            unreferenced = uid in await unreferenced_executor_uids(
                conn, {uid: pod_name}
            )
        if unreferenced:
            # The human attestation is the process-zero proof here.
            finalizer_released = (
                await agent_provisioner.release_stateless_executor_finalizer_exact(
                    retained, require_process_zero=False
                )
            )
    return {
        "unit_id": unit_id,
        "pod": pod_name,
        "pod_uid": uid,
        "settled_lease_tokens": settled,
        "kubernetes_authority": kubernetes_authority,
        "hold_released": hold_released,
        "finalizer_released": finalizer_released,
    }


async def unpark_completion_command(
    command_id: str, *, admin: Any, dependencies: RunQueueAdminDependencies
) -> dict[str, Any]:
    """Rearm one exact parked completion command and its pending effects."""

    if not dependencies.completion_commands_enabled():
        raise HTTPException(status_code=404, detail="Completion commands are disabled")
    from orchestrator.services.completion_command_resolution import (
        CompletionResolutionConflict,
        CompletionResolutionNotFound,
    )

    try:
        command_uuid = UUID(str(command_id))
    except (TypeError, ValueError):
        raise HTTPException(
            status_code=404, detail="Completion command not found"
        ) from None
    try:
        result = await dependencies.get_completion_command_resolution().unpark(
            command_uuid,
            actor=str(admin["id"]),
        )
    except CompletionResolutionNotFound as exc:
        raise HTTPException(
            status_code=404, detail="Completion command not found"
        ) from exc
    except CompletionResolutionConflict as exc:
        raise HTTPException(status_code=409, detail=exc.reason) from exc
    return completion_operator_result(result)


async def force_resolve_completion_command(
    command_id: str,
    *,
    expected_state: str,
    terminal_status: str,
    reason: str,
    admin: Any,
    dependencies: RunQueueAdminDependencies,
) -> dict[str, Any]:
    """Abandon a quiescent tail and write an operator-selected terminal state."""

    if not dependencies.completion_commands_enabled():
        raise HTTPException(status_code=404, detail="Completion commands are disabled")
    from orchestrator.services.completion_command_resolution import (
        CompletionResolutionConflict,
        CompletionResolutionNotFound,
    )

    try:
        command_uuid = UUID(str(command_id))
    except (TypeError, ValueError):
        raise HTTPException(
            status_code=404, detail="Completion command not found"
        ) from None
    try:
        result = await dependencies.get_completion_command_resolution().force_resolve(
            command_uuid,
            expected_state=expected_state,
            terminal_status=terminal_status,
            actor=str(admin["id"]),
            reason=reason,
        )
    except CompletionResolutionNotFound as exc:
        raise HTTPException(
            status_code=404, detail="Completion command not found"
        ) from exc
    except CompletionResolutionConflict as exc:
        raise HTTPException(status_code=409, detail=exc.reason) from exc

    # The durable jobs/command/effect transaction is authoritative. Checkpoint
    # pruning is the same non-fatal hygiene used by ordinary terminal writes.
    try:
        await dependencies.db.delete_checkpoint_thread(result.job_id)
    except Exception:
        logger.warning(
            "completion force-resolve checkpoint prune failed for job %s",
            result.job_id,
            exc_info=True,
        )
    return completion_operator_result(result)
