"""Application-owned durable completion services and control admission.

The jobs row and :class:`CompletionControl` remain the one serialization
point shared by completion reports and human controls.  This module only
composes the existing durable services; it does not duplicate their policy.
The runtime is constructed once per FastAPI application and its collaborators
are resolved lazily so the default-off route remains independent of command
storage and application startup keeps its existing late-binding behavior.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
import hashlib
import logging
from typing import Any

from fastapi import HTTPException


@dataclass(frozen=True, slots=True)
class CompletionAlertDependencies:
    """Officer-notification ports for durable completion incidents."""

    store: Any
    notify_all_officers: Callable[..., Awaitable[Any]]
    kick_officer_event_drain: Callable[[Any], None]


class CompletionAlerts:
    """Translate service incidents into stable officer notification records."""

    def __init__(self, dependencies: CompletionAlertDependencies) -> None:
        self.dependencies = dependencies

    async def sweep(self, message: str) -> None:
        digest = hashlib.sha256(message.encode("utf-8")).hexdigest()[:32]
        await self.dependencies.notify_all_officers(
            self.dependencies.store,
            source="completion_sweep",
            dedup_key=f"completion_sweep:{digest}",
            payload={"summary": message[:1000]},
        )
        self.dependencies.kick_officer_event_drain(self.dependencies.store)

    async def resolution(self, incident: Any) -> None:
        await self.dependencies.notify_all_officers(
            self.dependencies.store,
            source="completion_resolution",
            dedup_key=str(incident.dedup_key),
            payload={
                "kind": str(incident.kind)[:128],
                "command_id": str(incident.command_id),
                "job_id": str(incident.job_id),
                "actor": str(incident.actor)[:128],
                "reason": str(incident.reason)[:1000],
                "terminal_status": incident.terminal_status,
            },
        )
        self.dependencies.kick_officer_event_drain(self.dependencies.store)

    async def monitor(self, alert: Any) -> None:
        await self.dependencies.notify_all_officers(
            self.dependencies.store,
            source="completion_monitor",
            dedup_key=str(alert.dedup_key),
            payload={
                "kind": str(alert.kind),
                "summary": str(alert.message)[:1000],
                "command_id": alert.command_id,
                "job_id": alert.job_id,
                "command_state": alert.command_state,
                "age_seconds": alert.age_seconds,
                "unit_id": alert.unit_id,
                "queue_state": alert.queue_state,
                "runnable_at": (
                    alert.runnable_at.isoformat() if alert.runnable_at else None
                ),
            },
        )
        self.dependencies.kick_officer_event_drain(self.dependencies.store)


@dataclass(frozen=True, slots=True)
class CompletionRuntimeDependencies:
    """Narrow application ports used to construct durable completion owners."""

    store: Any
    workflow: Callable[[Any], Awaitable[dict[str, Any]]]
    commands_enabled: Callable[[], bool]
    status_reorder_enabled: Callable[[], bool]
    sweep_alert: Callable[[str], Awaitable[None]]
    resolution_alert: Callable[[Any], Awaitable[None]]
    monitor_alert: Callable[[Any], Awaitable[None]]
    max_queued_session_age_seconds: Callable[[], float]
    logger: logging.Logger


class CompletionRuntime:
    """One application's lazily composed durable completion services."""

    def __init__(self, dependencies: CompletionRuntimeDependencies) -> None:
        self.dependencies = dependencies
        self._finalizer: Any | None = None
        self._sweep_router: Any | None = None
        self._control: Any | None = None
        self._command_resolution: Any | None = None
        self._monitor: Any | None = None

    def finalizer(self) -> Any:
        """Build the finalizer only after command mode asks for it."""

        if self._finalizer is None:
            from orchestrator.services.completion_finalizer import CompletionFinalizer

            self._finalizer = CompletionFinalizer(
                self.dependencies.store,
                workflow=self.dependencies.workflow,
                preclaim=(
                    self.command_resolution().preclaim_command
                    if self.dependencies.status_reorder_enabled()
                    else None
                ),
            )
        return self._finalizer

    def command_resolution(self) -> Any:
        """Build the non-executing safety/operator command service lazily."""

        if self._command_resolution is None:
            from orchestrator.services.completion_command_resolution import (
                CompletionCommandResolution,
            )

            self._command_resolution = CompletionCommandResolution(
                self.dependencies.store,
                alert=self.dependencies.resolution_alert,
            )
        return self._command_resolution

    def monitor(self) -> Any:
        """Build monitoring independently of the finalizer drain loop."""

        if self._monitor is None:
            from orchestrator.services.completion_monitor import CompletionMonitor

            self._monitor = CompletionMonitor(
                self.dependencies.store,
                self.dependencies.monitor_alert,
                completion_commands_enabled=self.dependencies.commands_enabled(),
                max_queued_session_age_seconds=(
                    self.dependencies.max_queued_session_age_seconds()
                ),
            )
        return self._monitor

    def sweep_router(self) -> Any:
        """Build the class-1 sweep router only when a caller needs it."""

        if self._sweep_router is None:
            from orchestrator.services.completion_sweep_router import (
                CompletionSweepRouter,
            )

            self._sweep_router = CompletionSweepRouter(
                self.dependencies.store,
                self.finalizer(),
                alert=self.dependencies.sweep_alert,
                safety_net=(
                    self.command_resolution()
                    if self.dependencies.status_reorder_enabled()
                    else None
                ),
            )
        return self._sweep_router

    def control(self) -> Any:
        """Return the sole command-aware completion-control authority."""

        if self._control is None:
            from orchestrator.services.completion_control import CompletionControl

            self._control = CompletionControl(
                self.dependencies.store,
                self.sweep_router(),
            )
        return self._control

    def reset(self) -> None:
        """Release cached service objects during application shutdown/tests."""

        self._finalizer = None
        self._sweep_router = None
        self._control = None
        self._command_resolution = None
        self._monitor = None


class CompletionControlBoundary:
    """HTTP/application adapter over one :class:`CompletionControl` instance.

    Feature-flag checks happen before importing or constructing Gate-3 owners.
    The disabled path consequently retains its historical call signatures and
    never names completion-command storage.
    """

    def __init__(self, runtime: CompletionRuntime) -> None:
        self.runtime = runtime

    @property
    def _enabled(self) -> bool:
        return self.runtime.dependencies.commands_enabled()

    async def guard(self, job_id: str, *, source: str) -> None:
        if not self._enabled:
            return
        decision = await self.runtime.control().guard_job(job_id, source=source)
        if decision.blocked:
            raise HTTPException(status_code=409, detail="completion finalizing")

    async def claim(
        self, job: Mapping[str, Any], *, source: str,
        terminal_review_source: Mapping[str, Any] | None = None,
    ) -> Any:
        if not self._enabled:
            return None
        from orchestrator.services.completion_control import (
            CompletionControlClaimConflict,
        )

        try:
            return await self.runtime.control().claim_job(
                str(job["id"]),
                source=source,
                expected_status=str(job.get("status") or ""),
                expected_lane=str(job.get("execution_lane") or "pinned"),
                **(
                    {"terminal_review_source": terminal_review_source}
                    if terminal_review_source is not None else {}
                ),
            )
        except CompletionControlClaimConflict as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    async def claim_pause(
        self,
        job_id: str,
        *,
        source: str,
        expected_agent_id: str | None,
        operator_hold: bool = False,
        paused_by: str | None = None,
    ) -> Any:
        if not self._enabled:
            return None
        from orchestrator.services.completion_control import (
            CompletionControlClaimConflict,
        )

        try:
            return await self.runtime.control().claim_pause_job(
                job_id,
                source=source,
                expected_agent_id=expected_agent_id,
                operator_hold=operator_hold,
                paused_by=paused_by,
            )
        except CompletionControlClaimConflict as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    async def abort(self, claim: Any) -> None:
        """Best-effort exact-marker release after validation/external failure."""

        if claim is None:
            return
        try:
            await self.runtime.control().abort_claim(claim)
        except Exception:
            self.runtime.dependencies.logger.exception(
                "Failed to release completion control claim %s for job %s",
                getattr(claim, "claim_id", "unknown"),
                getattr(claim, "job_id", "unknown"),
            )

    def finish(self, claim: Any) -> Any:
        """Return the exact-claim mutation context owned by the authority."""

        return self.runtime.control().finish_claim(claim)

    def finish_claim(self, claim: Any) -> Any:
        """Expose the authority's established name to migrated consumers."""

        return self.finish(claim)

    def resume_guard_kwargs(
        self,
        command_id: str | None = None,
        owner: str | None = None,
        control_claim: Any | None = None,
    ) -> dict[str, Any]:
        if not self._enabled:
            return {}
        values: dict[str, Any] = {"completion_commands_enabled": True}
        if command_id is not None or owner is not None:
            values.update(
                completion_owner_command_id=command_id,
                completion_owner=owner,
            )
        if control_claim is not None:
            values["completion_control_claim_id"] = str(control_claim.claim_id)
        return values

    def dispatch_guard_kwargs(self) -> dict[str, bool]:
        return {"completion_commands_enabled": True} if self._enabled else {}

    def active_claim(self, job: Mapping[str, Any] | None) -> bool:
        if not self._enabled or not job:
            return False
        from orchestrator.services.completion_control import (
            completion_control_claim_active,
        )

        return completion_control_claim_active(job.get("context"))

    @staticmethod
    def claim_detail(job: Mapping[str, Any] | None) -> str:
        from orchestrator.services.completion_control import (
            completion_control_claim_detail,
        )

        return completion_control_claim_detail(job.get("context") if job else None)


__all__ = [
    "CompletionAlertDependencies",
    "CompletionAlerts",
    "CompletionControlBoundary",
    "CompletionRuntime",
    "CompletionRuntimeDependencies",
]
