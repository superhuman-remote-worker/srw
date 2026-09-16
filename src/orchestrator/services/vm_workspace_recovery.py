"""Bounded, claim-fenced reconciliation of held VM workspaces."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import random
from collections.abc import Callable, Mapping
from typing import Any
from uuid import UUID

from orchestrator.services.vm_workspace_recovery_store import (
    RecoveryClaim,
    RetentionPinCommand,
    VMWorkspaceRecoveryStore,
)
from orchestrator.services.vm_workspace_recovery_telemetry import (
    VMWorkspaceRecoveryTelemetry,
    workspace_recovery_telemetry,
)
from shared.workspace_recovery import WorkspaceRecoveryCode

logger = logging.getLogger(__name__)


_REQUIRED_SUCCESSOR_AUTHORITY = (
    "vmi_uid",
    "launcher_uid",
    "node_uid",
    "pod_ip",
    "ssh_registration_id",
    "guest_boot_id",
    "guest_machine_id",
    "interface_mac",
)


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


def _stable_guest_network_key(successor: Mapping[str, Any]) -> str:
    network = successor.get("guest_network")
    if not isinstance(network, Mapping):
        return ""
    stable = {
        str(key): value
        for key, value in network.items()
        if key not in {"challenge", "registration_id"}
    }
    return json.dumps(stable, sort_keys=True, separators=(",", ":"), default=str)


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
            observation.get("prior_runtime"),
            observation.get("stop_receipt_digest"),
            successor.get("vmi_uid"),
            successor.get("launcher_uid"),
            successor.get("node_uid"),
            successor.get("pod_ip"),
            successor.get("guest_boot_id"),
            successor.get("guest_machine_id"),
            _stable_guest_network_key(successor),
        )
    )


def _valid_uuid(value: object) -> bool:
    try:
        UUID(str(value))
    except (TypeError, ValueError, AttributeError):
        return False
    return True


def _valid_machine_id(value: object) -> bool:
    if (
        not isinstance(value, str)
        or len(value) != 32
        or value != value.lower()
        or value == "0" * 32
        or any(character not in "0123456789abcdef" for character in value)
    ):
        return False
    return True


def _observation_digest(observation: Mapping[str, Any] | None) -> str | None:
    if observation is None:
        return None
    encoded = json.dumps(
        dict(observation), sort_keys=True, separators=(",", ":"), default=str
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


class VMWorkspaceRecoveryService:
    """Run external recovery probes outside short durable claim transactions."""

    def __init__(
        self,
        store: VMWorkspaceRecoveryStore | Any,
        observer: Any,
        *,
        automatic_enabled: bool = True,
        replacement_enabled: bool = True,
        probe_timeout_seconds: float = 10.0,
        claim_ttl_seconds: float = 30.0,
        permit_ttl_seconds: float = 30.0,
        max_global_probes: int = 4,
        max_probes_per_node: int = 1,
        deadline_seconds: float = 900.0,
        claim_poll_seconds: float = 0.25,
        scan_interval_seconds: float = 3.0,
        jitter: Callable[[], float] | None = None,
        telemetry: VMWorkspaceRecoveryTelemetry | Any | None = None,
        allow_observation_past_deadline_for_acceptance: bool = False,
    ) -> None:
        self.store = store
        self.observer = observer
        self.automatic_enabled = automatic_enabled
        self.replacement_enabled = replacement_enabled
        self.probe_timeout_seconds = max(0.1, probe_timeout_seconds)
        self.claim_ttl_seconds = max(self.probe_timeout_seconds + 1, claim_ttl_seconds)
        self.permit_ttl_seconds = max(
            self.probe_timeout_seconds + 1, permit_ttl_seconds
        )
        self.max_global_probes = max(1, int(max_global_probes))
        if int(max_probes_per_node) != 1:
            raise ValueError("recovery protocol v1 supports one probe per node")
        self.max_probes_per_node = int(max_probes_per_node)
        self.deadline_seconds = max(0.1, float(deadline_seconds))
        self.claim_poll_seconds = max(0.001, claim_poll_seconds)
        self.scan_interval_seconds = max(0.01, scan_interval_seconds)
        self.jitter = jitter
        self.telemetry = telemetry or workspace_recovery_telemetry
        self.allow_observation_past_deadline_for_acceptance = bool(
            allow_observation_past_deadline_for_acceptance
        )

    @classmethod
    def from_settings(
        cls,
        store: VMWorkspaceRecoveryStore | Any,
        observer: Any,
        *,
        settings: Any,
        telemetry: VMWorkspaceRecoveryTelemetry | Any | None = None,
        **kwargs: Any,
    ) -> "VMWorkspaceRecoveryService":
        """Build the reconciler from one already-validated rollout envelope."""

        return cls(
            store,
            observer,
            automatic_enabled=settings.enabled,
            replacement_enabled=settings.replacement_enabled,
            probe_timeout_seconds=settings.external_call_timeout_seconds,
            claim_ttl_seconds=settings.claim_ttl_seconds,
            permit_ttl_seconds=settings.permit_ttl_seconds,
            max_global_probes=settings.max_global_probes,
            max_probes_per_node=settings.max_probes_per_node,
            deadline_seconds=settings.deadline_seconds,
            telemetry=telemetry,
            **kwargs,
        )

    def _emit(
        self,
        *,
        event: str,
        result: str,
        phase: str,
        claim: RecoveryClaim | None = None,
        command: RetentionPinCommand | None = None,
        code: WorkspaceRecoveryCode | str = "none",
        state: str = "recovering_workspace",
        reason: str | None = None,
        observation: Mapping[str, Any] | None = None,
    ) -> None:
        """Best-effort observability must never acquire recovery authority."""

        identity = claim.captured_identity if claim is not None else {}
        age_seconds = (
            min(
                self.deadline_seconds,
                max(0.0, self.deadline_seconds - claim.remaining_seconds),
            )
            if claim is not None
            else None
        )
        try:
            self.telemetry.emit(
                event=event,
                state=state,
                phase=phase,
                code=code,
                result=result,
                age_seconds=age_seconds,
                reason=reason,
                observation_digest=_observation_digest(observation),
                operation_id=(
                    claim.operation_id
                    if claim is not None
                    else command.recovery_id
                    if command is not None
                    else None
                ),
                job_id=(
                    identity.get("owner_id")
                    if identity.get("owner_kind") == "job"
                    else command.owner_id
                    if command is not None and command.owner_kind == "job"
                    else None
                ),
                vm_uid=identity.get("vm_uid"),
                pvc_uid=(
                    identity.get("root_pvc_uid")
                    if claim is not None
                    else command.pvc_uid
                    if command is not None
                    else None
                ),
            )
        except Exception:
            logger.exception("VM workspace recovery telemetry emission failed")

    async def run(self, shutdown_event: asyncio.Event) -> None:
        if not self.automatic_enabled:
            paused = await self.store.pause_automatic_disabled()
            if paused:
                logger.warning(
                    "Paused %d VM workspace recoveries because automation is disabled",
                    paused,
                )
                self._emit(
                    event="pause",
                    state="paused_attention",
                    phase="paused_attention",
                    code="none",
                    result="accepted",
                    reason="automatic_recovery_disabled",
                )
            # Hold protection remains active while automation is disabled.
            # Continue reconciling durable controller pin activation/release,
            # but never observe or replace a runtime.
            while not shutdown_event.is_set():
                try:
                    await self._reconcile_retention_pins()
                except Exception:
                    logger.exception("VM workspace recovery pin sync failed")
                try:
                    await asyncio.wait_for(
                        shutdown_event.wait(), timeout=self.scan_interval_seconds
                    )
                except asyncio.TimeoutError:
                    pass
            return
        logger.info("VM workspace recovery reconciler started")
        while not shutdown_event.is_set():
            try:
                await self._reconcile_retention_pins()
                operation_ids = await self.store.list_due_operation_ids(limit=32)
                results = await asyncio.gather(
                    *(
                        self.reconcile_once(operation_id)
                        for operation_id in operation_ids
                    ),
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
        deadline = self.probe_timeout_seconds
        if not self.allow_observation_past_deadline_for_acceptance:
            deadline = min(deadline, claim.remaining_seconds)
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
                    self._emit(
                        event="probe",
                        result="lost_claim",
                        phase="observing",
                        claim=claim,
                        reason="durable_claim_lost",
                    )
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

    async def _sync_retention_pin(self, command: RetentionPinCommand) -> bool:
        reconcile = getattr(self.observer, "reconcile_workspace_recovery_pin", None)
        if not callable(reconcile):
            await self.store.defer_retention_pin_command(
                command, error="controller recovery pin transport is unavailable"
            )
            self._emit(
                event="pin_sync",
                result="deferred",
                phase="recovering",
                command=command,
                reason="controller_pin_unavailable",
            )
            return False
        try:
            result = await asyncio.wait_for(
                reconcile(command), timeout=self.probe_timeout_seconds
            )
            if not isinstance(
                result, Mapping
            ) or not await self.store.acknowledge_retention_pin(command, result):
                raise RuntimeError("controller recovery pin acknowledgement changed")
            self._emit(
                event="pin_sync",
                result="accepted",
                phase="recovering"
                if command.desired_state == "active"
                else "recovered",
                state=(
                    "recovering_workspace"
                    if command.desired_state == "active"
                    else "recovered"
                ),
                command=command,
            )
            return True
        except Exception as exc:
            await self.store.defer_retention_pin_command(command, error=str(exc))
            self._emit(
                event="pin_sync",
                result="failed",
                phase="recovering",
                command=command,
                reason="controller_pin_sync_failed",
            )
            return False

    async def _reconcile_retention_pins(self) -> None:
        commands = await self.store.list_retention_pin_commands(limit=32)
        if commands:
            await asyncio.gather(
                *(self._sync_retention_pin(command) for command in commands)
            )

    async def _require_retention_pin(self, claim: RecoveryClaim) -> bool:
        command = await self.store.retention_pin_command(claim)
        if command is None:
            return False
        if not await self.store.retention_pin_is_acknowledged(
            claim
        ) and not await self._sync_retention_pin(command):
            return False
        return bool(await self.store.retention_pin_is_acknowledged(claim))

    async def _attach_stop_receipt(
        self, claim: RecoveryClaim, observation: Mapping[str, Any]
    ) -> Mapping[str, Any] | None:
        """Persist positive controller evidence and consume only exact receipts."""

        result = dict(observation)
        # The controller response is not itself a receipt. Only a digest
        # returned by the append-only store boundary is trusted downstream.
        result.pop("stop_receipt_digest", None)
        evidence = result.get("stop_evidence")
        digest = None
        if isinstance(evidence, Mapping):
            digest = await self.store.accept_stop_evidence(claim, evidence)
            if digest is None:
                await self._pause(
                    claim,
                    code=WorkspaceRecoveryCode.PRIOR_RUNTIME_UNFENCED,
                    reason="controller_stop_evidence_rejected",
                    observation=observation,
                )
                return None
        if digest is None:
            digest = await self.store.trusted_stop_receipt(claim)
        if isinstance(digest, str) and digest:
            result["prior_runtime"] = "stopped"
            result["stop_receipt_digest"] = digest
        return result

    async def _defer(
        self,
        claim: RecoveryClaim,
        *,
        phase: str,
        observation: Mapping[str, Any] | None = None,
        diagnostic: Mapping[str, Any] | None = None,
    ) -> None:
        changed = await self.store.defer_claim(
            operation_id=claim.operation_id,
            version=claim.version,
            claim_token=claim.claim_token,
            phase=phase,
            observation=observation,
            diagnostic=diagnostic,
            next_check_seconds=self._delay(claim),
        )
        if changed:
            reason = diagnostic.get("reason") if diagnostic else None
            self._emit(
                event="probe",
                result="deferred",
                phase=phase,
                claim=claim,
                reason=str(reason) if reason is not None else None,
                observation=observation,
            )

    async def _pause(
        self,
        claim: RecoveryClaim,
        *,
        code: WorkspaceRecoveryCode,
        reason: str,
        observation: Mapping[str, Any] | None = None,
    ) -> None:
        changed = await self.store.pause_for_attention(
            operation_id=claim.operation_id,
            version=claim.version,
            claim_token=claim.claim_token,
            code=code,
            diagnostic={
                "reason": reason,
                **({"observation": dict(observation)} if observation else {}),
            },
        )
        if changed:
            self._emit(
                event="pause",
                state="paused_attention",
                phase="paused_attention",
                code=code,
                result="accepted",
                claim=claim,
                reason=reason,
                observation=observation,
            )

    async def _read_preconditions(
        self, claim: RecoveryClaim, observation: Mapping[str, Any]
    ) -> Mapping[str, Any] | None:
        """Attach database-owned continuation evidence to an external observation."""

        read_preconditions = getattr(self.store, "recovery_preconditions", None)
        if not callable(read_preconditions):
            return dict(observation)
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
            return None
        if value is None:
            return None
        return {
            **dict(observation),
            "continuation": value.get("continuation", observation.get("continuation")),
            "remote_operations": value.get(
                "remote_operations", observation.get("remote_operations")
            ),
        }

    async def _reject_unsafe_observation(
        self, claim: RecoveryClaim, observation: Mapping[str, Any]
    ) -> bool:
        """Fail closed on every predicate required by the release transaction."""

        if (
            not self._identity_matches(claim, observation)
            or observation.get("ambiguous") is not False
        ):
            await self._pause(
                claim,
                code=WorkspaceRecoveryCode.IDENTITY_CONFLICT,
                reason="captured_workspace_identity_changed_or_ambiguous",
                observation=observation,
            )
            return True
        continuation = observation.get("continuation")
        if continuation not in {"safe", "not_started"}:
            await self._pause(
                claim,
                code=WorkspaceRecoveryCode.TOOL_OUTCOME_UNKNOWN,
                reason="checkpoint_or_tool_outcome_unknown",
                observation=observation,
            )
            return True
        if observation.get("remote_operations") != "settled":
            await self._defer(
                claim,
                phase="reconciling_outcome",
                observation=observation,
                diagnostic={"reason": "remote_operations_unresolved"},
            )
            return True
        prior_runtime = observation.get("prior_runtime")
        if prior_runtime not in {"stopped", "same_runtime"}:
            if prior_runtime == "running":
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
            return True
        successor = _successor(observation)
        missing_authority = [
            key
            for key in _REQUIRED_SUCCESSOR_AUTHORITY
            if not isinstance(successor.get(key), str)
            or not str(successor.get(key)).strip()
        ]
        if (
            observation.get("ready") is not True
            or observation.get("authenticated") is not True
            or missing_authority
        ):
            await self._defer(
                claim,
                phase="waiting_runtime",
                observation=observation,
                diagnostic={
                    "reason": "successor_not_authenticated_ready",
                    "missing_authority": missing_authority,
                },
            )
            return True
        if not _valid_uuid(successor["vmi_uid"]) or not _valid_uuid(
            successor["launcher_uid"]
        ):
            await self._pause(
                claim,
                code=WorkspaceRecoveryCode.IDENTITY_CONFLICT,
                reason="successor_authority_malformed",
                observation=observation,
            )
            return True
        if (
            not _valid_uuid(successor["guest_boot_id"])
            or not _valid_machine_id(successor["guest_machine_id"])
            or not isinstance(successor.get("guest_network"), Mapping)
        ):
            await self._defer(
                claim,
                phase="waiting_runtime",
                observation=observation,
                diagnostic={"reason": "successor_guest_identity_malformed"},
            )
            return True
        replacing = not _same_identifier(
            successor.get("launcher_uid"),
            claim.captured_identity.get("prior_launcher_uid"),
        )
        if replacing and not self.replacement_enabled:
            await self._pause(
                claim,
                code=WorkspaceRecoveryCode.PRIOR_RUNTIME_UNFENCED,
                reason="automatic_replacement_recovery_disabled",
                observation=observation,
            )
            return True
        if replacing and (
            prior_runtime != "stopped"
            or not isinstance(observation.get("stop_receipt_digest"), str)
            or not str(observation.get("stop_receipt_digest")).strip()
        ):
            await self._pause(
                claim,
                code=WorkspaceRecoveryCode.PRIOR_RUNTIME_UNFENCED,
                reason="replacement_missing_exact_stop_evidence",
                observation=observation,
            )
            return True
        return False

    @staticmethod
    def _identity_matches(claim: RecoveryClaim, observation: Mapping[str, Any]) -> bool:
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
            operation_id,
            ttl_seconds=self.claim_ttl_seconds,
            permit_ttl_seconds=self.permit_ttl_seconds,
            max_global_probes=self.max_global_probes,
            max_probes_per_node=self.max_probes_per_node,
        )
        if claim is None:
            return
        self._emit(
            event="probe",
            result="accepted",
            phase="observing",
            claim=claim,
            reason="durable_claim_acquired",
        )
        try:
            pin_acknowledged = await self._require_retention_pin(claim)
        except Exception as exc:
            pin_acknowledged = False
            pin_error = str(exc)[:500]
        else:
            pin_error = "controller pin is not acknowledged"
        if not pin_acknowledged:
            await self._defer(
                claim,
                phase="observing",
                diagnostic={
                    "reason": "controller_retention_pin_unacknowledged",
                    "detail": pin_error,
                },
            )
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
        observation = await self._attach_stop_receipt(claim, observation)
        if observation is None:
            return
        attached = await self._read_preconditions(claim, observation)
        if attached is None:
            return
        observation = attached
        if await self._reject_unsafe_observation(claim, observation):
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
        staged = await self.store.renew_claim(
            staged,
            ttl_seconds=self.claim_ttl_seconds,
            permit_ttl_seconds=self.permit_ttl_seconds,
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
        final_observation = await self._attach_stop_receipt(staged, final_observation)
        if final_observation is None:
            return
        attached_final = await self._read_preconditions(staged, final_observation)
        if attached_final is None:
            return
        final_observation = attached_final
        if await self._reject_unsafe_observation(staged, final_observation):
            return
        if _attestation_key(final_observation) != _attestation_key(observation):
            await self._pause(
                staged,
                code=WorkspaceRecoveryCode.IDENTITY_CONFLICT,
                reason="final_re_attestation_changed",
                observation=final_observation,
            )
            return
        released = await self.store.release_recovered(
            operation_id=staged.operation_id,
            version=staged.version,
            claim_token=staged.claim_token,
            initial_observation=observation,
            final_observation=final_observation,
            resume_receipt={
                "kind": "vm_workspace_recovery",
                "claim_token": staged.claim_token,
                "successor": dict(_successor(final_observation)),
                "stop_receipt_digest": final_observation.get("stop_receipt_digest"),
            },
        )
        if released:
            self._emit(
                event="release",
                state="recovered",
                phase="recovered",
                code=WorkspaceRecoveryCode.REPLACEMENT_OBSERVED,
                result="succeeded",
                claim=staged,
                reason="final_attestation_accepted",
                observation=final_observation,
            )
            # Release is durable in PostgreSQL first. A controller failure keeps
            # the Lease active and the background sync retries the safe leak.
            await self._reconcile_retention_pins()


__all__ = [
    "VMWorkspaceRecoveryService",
    "recovery_retry_delay",
]
