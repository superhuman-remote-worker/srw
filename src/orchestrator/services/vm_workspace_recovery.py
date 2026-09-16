"""Bounded, claim-fenced reconciliation of held VM workspaces."""

from __future__ import annotations

import asyncio
import logging
import random
from collections.abc import Callable, Mapping
from typing import Any
from uuid import UUID

from orchestrator.services.vm_workspace_recovery_store import (
    RecoveryClaim,
    VMWorkspaceRecoveryStore,
)
from shared.workspace_recovery import WorkspaceRecoveryCode

logger = logging.getLogger(__name__)

def recovery_retry_delay(
    attempt: int,
    *,
    remaining_seconds: float | None = None,
    jitter: Callable[[], float] | None = None,
) -> float:
    """Return 10/20/40/60-second exponential delay with bounded jitter."""

    base = min(60.0, 10.0 * (2 ** max(0, attempt - 1)))
    factor = (jitter or (lambda: random.uniform(0.8, 1.2)))()
    delay = min(60.0, max(0.0, base * factor))
    if remaining_seconds is not None:
        delay = min(delay, max(0.0, remaining_seconds))
    return delay


def _same_identifier(left: object, right: object) -> bool:
    return left is not None and right is not None and str(left) == str(right)


def _successor(observation: Mapping[str, Any]) -> Mapping[str, Any]:
    value = observation.get("successor")
    return value if isinstance(value, Mapping) else {}


def _attestation_key(observation: Mapping[str, Any]) -> tuple[str, ...]:
    successor = _successor(observation)
    return tuple(
        str(value or "")
        for value in (
            observation.get("owner_kind"),
            observation.get("owner_id"),
            observation.get("provision_generation"),
            observation.get("vm_uid"),
            observation.get("root_pvc_uid"),
            successor.get("vmi_uid"),
            successor.get("launcher_uid"),
            successor.get("node_uid"),
            successor.get("pod_ip"),
            successor.get("ssh_registration_id"),
        )
    )


class VMWorkspaceRecoveryService:
    """Run external recovery probes outside short durable claim transactions."""

    def __init__(
        self,
        store: VMWorkspaceRecoveryStore | Any,
        observer: Any,
        *,
        automatic_enabled: bool = True,
        probe_timeout_seconds: float = 10.0,
        claim_ttl_seconds: float = 30.0,
        claim_poll_seconds: float = 0.25,
        scan_interval_seconds: float = 3.0,
        jitter: Callable[[], float] | None = None,
    ) -> None:
        self.store = store
        self.observer = observer
        self.automatic_enabled = automatic_enabled
        self.probe_timeout_seconds = max(0.1, probe_timeout_seconds)
        self.claim_ttl_seconds = max(self.probe_timeout_seconds + 1, claim_ttl_seconds)
        self.claim_poll_seconds = max(0.001, claim_poll_seconds)
        self.scan_interval_seconds = max(0.01, scan_interval_seconds)
        self.jitter = jitter

    async def run(self, shutdown_event: asyncio.Event) -> None:
        if not self.automatic_enabled:
            paused = await self.store.pause_automatic_disabled()
            if paused:
                logger.warning(
                    "Paused %d VM workspace recoveries because automation is disabled",
                    paused,
                )
            await shutdown_event.wait()
            return
        logger.info("VM workspace recovery reconciler started")
        while not shutdown_event.is_set():
            try:
                operation_ids = await self.store.list_due_operation_ids(limit=32)
                results = await asyncio.gather(
                    *(self.reconcile_once(operation_id) for operation_id in operation_ids),
                    return_exceptions=True,
                )
                for operation_id, result in zip(operation_ids, results):
                    if isinstance(result, BaseException):
                        logger.error(
                            "VM workspace recovery reconciliation failed for %s: %r",
                            operation_id,
                            result,
                        )
            except Exception:
                logger.exception("VM workspace recovery scan failed")
            try:
                await asyncio.wait_for(
                    shutdown_event.wait(), timeout=self.scan_interval_seconds
                )
            except asyncio.TimeoutError:
                pass
        logger.info("VM workspace recovery reconciler stopped")

    async def _observe(self, claim: RecoveryClaim) -> Mapping[str, Any] | None:
        observe = getattr(self.observer, "observe_workspace_recovery", None)
        if not callable(observe):
            raise RuntimeError("controller recovery observation is unavailable")
        probe = asyncio.create_task(observe(claim.captured_identity))
        deadline = min(self.probe_timeout_seconds, claim.remaining_seconds)
        loop = asyncio.get_running_loop()
        stop_at = loop.time() + max(0.0, deadline)
        try:
            while True:
                remaining = stop_at - loop.time()
                if remaining <= 0:
                    probe.cancel()
                    await asyncio.gather(probe, return_exceptions=True)
                    raise TimeoutError("workspace recovery observation timed out")
                done, _ = await asyncio.wait(
                    {probe},
                    timeout=min(self.claim_poll_seconds, remaining),
                )
                if probe in done:
                    value = probe.result()
                    if not isinstance(value, Mapping):
                        raise RuntimeError(
                            "controller recovery observation is not an object"
                        )
                    return value
                if not await self.store.claim_is_current(claim):
                    probe.cancel()
                    await asyncio.gather(probe, return_exceptions=True)
                    return None
        except asyncio.CancelledError:
            probe.cancel()
            await asyncio.gather(probe, return_exceptions=True)
            raise

    def _delay(self, claim: RecoveryClaim) -> float:
        return recovery_retry_delay(
            claim.attempt,
            remaining_seconds=claim.remaining_seconds,
            jitter=self.jitter,
        )

    async def _defer(
        self,
        claim: RecoveryClaim,
        *,
        phase: str,
        observation: Mapping[str, Any] | None = None,
        diagnostic: Mapping[str, Any] | None = None,
    ) -> None:
        await self.store.defer_claim(
            operation_id=claim.operation_id,
            version=claim.version,
            claim_token=claim.claim_token,
            phase=phase,
            observation=observation,
            diagnostic=diagnostic,
            next_check_seconds=self._delay(claim),
        )

    async def _pause(
        self,
        claim: RecoveryClaim,
        *,
        code: WorkspaceRecoveryCode,
        reason: str,
        observation: Mapping[str, Any] | None = None,
    ) -> None:
        await self.store.pause_for_attention(
            operation_id=claim.operation_id,
            version=claim.version,
            claim_token=claim.claim_token,
            code=code,
            diagnostic={
                "reason": reason,
                **({"observation": dict(observation)} if observation else {}),
            },
        )

    @staticmethod
    def _identity_matches(
        claim: RecoveryClaim, observation: Mapping[str, Any]
    ) -> bool:
        captured = claim.captured_identity
        return all(
            _same_identifier(observation.get(key), captured.get(key))
            for key in (
                "owner_kind",
                "owner_id",
                "provision_generation",
                "vm_uid",
                "root_pvc_uid",
            )
        )

    async def reconcile_once(self, operation_id: UUID) -> None:
        claim = await self.store.claim_due(
            operation_id, ttl_seconds=self.claim_ttl_seconds
        )
        if claim is None:
            return
        try:
            observation = await self._observe(claim)
        except Exception as exc:
            await self._defer(
                claim,
                phase="waiting_runtime",
                diagnostic={
                    "reason": "controller_observation_failed",
                    "detail": str(exc)[:500],
                },
            )
            return
        if observation is None:
            # A lost durable claim deliberately produces no follow-up write.
            return
        preconditions: Mapping[str, Any] = {}
        read_preconditions = getattr(self.store, "recovery_preconditions", None)
        if callable(read_preconditions):
            try:
                value = await read_preconditions(claim)
            except Exception as exc:
                await self._defer(
                    claim,
                    phase="reconciling_outcome",
                    observation=observation,
                    diagnostic={
                        "reason": "recovery_precondition_read_failed",
                        "detail": str(exc)[:500],
                    },
                )
                return
            if value is None:
                return
            preconditions = value
        if not self._identity_matches(claim, observation):
            await self._pause(
                claim,
                code=WorkspaceRecoveryCode.IDENTITY_CONFLICT,
                reason="captured_workspace_identity_changed",
                observation=observation,
            )
            return
        if observation.get("ambiguous") is True:
            await self._pause(
                claim,
                code=WorkspaceRecoveryCode.IDENTITY_CONFLICT,
                reason="controller_observation_ambiguous",
                observation=observation,
            )
            return
        continuation = preconditions.get(
            "continuation", observation.get("continuation")
        )
        if continuation not in {"safe", "not_started"}:
            await self._pause(
                claim,
                code=WorkspaceRecoveryCode.TOOL_OUTCOME_UNKNOWN,
                reason="checkpoint_or_tool_outcome_unknown",
                observation=observation,
            )
            return
        remote_operations = preconditions.get(
            "remote_operations", observation.get("remote_operations")
        )
        if remote_operations != "settled":
            await self._defer(
                claim,
                phase="reconciling_outcome",
                observation=observation,
                diagnostic={"reason": "remote_operations_unresolved"},
            )
            return
        if observation.get("prior_runtime") not in {"stopped", "same_runtime"}:
            if observation.get("prior_runtime") == "running":
                await self._defer(
                    claim,
                    phase="verifying_stop",
                    observation=observation,
                    diagnostic={"reason": "prior_runtime_still_running"},
                )
            else:
                await self._pause(
                    claim,
                    code=WorkspaceRecoveryCode.PRIOR_RUNTIME_UNFENCED,
                    reason="prior_runtime_stop_evidence_unknown",
                    observation=observation,
                )
            return
        successor = _successor(observation)
        if (
            observation.get("ready") is not True
            or observation.get("authenticated") is not True
            or not successor.get("vmi_uid")
            or not successor.get("launcher_uid")
            or not successor.get("pod_ip")
        ):
            await self._defer(
                claim,
                phase="waiting_runtime",
                observation=observation,
                diagnostic={"reason": "successor_not_authenticated_ready"},
            )
            return
        replacing = not _same_identifier(
            successor.get("launcher_uid"),
            claim.captured_identity.get("prior_launcher_uid"),
        )
        if replacing and not observation.get("stop_receipt_digest"):
            await self._pause(
                claim,
                code=WorkspaceRecoveryCode.PRIOR_RUNTIME_UNFENCED,
                reason="replacement_missing_stop_receipt",
                observation=observation,
            )
            return
        staged = await self.store.stage_observation(
            operation_id=claim.operation_id,
            version=claim.version,
            claim_token=claim.claim_token,
            phase="attesting",
            observation=observation,
        )
        if staged is None:
            return
        try:
            final_observation = await self._observe(staged)
        except Exception as exc:
            await self._defer(
                staged,
                phase="attesting",
                diagnostic={
                    "reason": "final_re_attestation_failed",
                    "detail": str(exc)[:500],
                },
            )
            return
        if final_observation is None:
            return
        if (
            not self._identity_matches(staged, final_observation)
            or _attestation_key(final_observation) != _attestation_key(observation)
            or final_observation.get("ready") is not True
            or final_observation.get("authenticated") is not True
        ):
            await self._pause(
                staged,
                code=WorkspaceRecoveryCode.IDENTITY_CONFLICT,
                reason="final_re_attestation_changed",
                observation=final_observation,
            )
            return
        await self.store.release_recovered(
            operation_id=staged.operation_id,
            version=staged.version,
            claim_token=staged.claim_token,
            initial_observation=observation,
            final_observation=final_observation,
            resume_receipt={
                "kind": "vm_workspace_recovery",
                "claim_token": staged.claim_token,
                "successor": dict(_successor(final_observation)),
                "stop_receipt_digest": final_observation.get(
                    "stop_receipt_digest"
                ),
            },
        )


__all__ = [
    "VMWorkspaceRecoveryService",
    "recovery_retry_delay",
]
