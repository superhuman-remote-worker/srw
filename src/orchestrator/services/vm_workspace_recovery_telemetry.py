"""Aggregate metrics and redacted structured audits for VM recovery."""

from __future__ import annotations

import hashlib
import logging
import re
from typing import Any

try:
    from opentelemetry import metrics

    _HAS_OTEL = True
except Exception:  # pragma: no cover — defensive for images without OTel API
    metrics = None  # type: ignore[assignment]
    _HAS_OTEL = False


class _NullInstrument:
    """No-op counter/histogram used when the OTel API is not installed."""

    def add(self, *_args: Any, **_kwargs: Any) -> None:
        return None

    def record(self, *_args: Any, **_kwargs: Any) -> None:
        return None


class _NullMeter:
    """No-op meter factory used when the OTel API is not installed."""

    def create_counter(self, *_args: Any, **_kwargs: Any) -> _NullInstrument:
        return _NullInstrument()

    def create_histogram(self, *_args: Any, **_kwargs: Any) -> _NullInstrument:
        return _NullInstrument()

from shared.workspace_recovery import WorkspaceRecoveryCode


logger = logging.getLogger(__name__)

_EVENTS = {
    "hold",
    "queue_fenced",
    "probe",
    "pause",
    "retry",
    "cancel",
    "cleanup_blocked",
    "release",
    "pin_sync",
}
_STATES = {"recovering_workspace", "paused_attention", "recovered", "cancelled"}
_PHASES = {
    "recovering",
    "observing",
    "waiting_runtime",
    "verifying_stop",
    "attesting",
    "reconciling_outcome",
    "paused_attention",
    "recovered",
    "cancelled",
    "superseded",
}
_CODES = {"none", *(code.value for code in WorkspaceRecoveryCode)}
_RESULTS = {
    "accepted",
    "blocked",
    "deferred",
    "rejected",
    "lost_claim",
    "deadline",
    "succeeded",
    "failed",
}
_CLEANUP_BLOCKERS = {
    "active_recovery_hold",
    "active_retention_pin",
    "cleanup_authority_unavailable",
    "controller_pin_unavailable",
    "identity_changed",
    "unknown_pvc_identity",
    "workspace_cleanup_already_admitted",
    "workspace_recovery_unresolved",
}
_REASONS = {
    "admission_requires_attention",
    "automatic_recovery_disabled",
    "automatic_replacement_recovery_disabled",
    "captured_workspace_identity_changed_or_ambiguous",
    "checkpoint_or_tool_outcome_unknown",
    "controller_observation_failed",
    "controller_pin_sync_failed",
    "controller_pin_unavailable",
    "controller_retention_pin_unacknowledged",
    "controller_stop_evidence_rejected",
    "durable_claim_acquired",
    "durable_claim_lost",
    "final_attestation_accepted",
    "final_re_attestation_changed",
    "final_re_attestation_failed",
    "force_end",
    "operator_retry_accepted",
    "prior_runtime_still_running",
    "prior_runtime_stop_evidence_unknown",
    "queue_lease_token_advanced",
    "recovery_participant_cancelled",
    "recovery_precondition_read_failed",
    "remote_operations_unresolved",
    "replacement_missing_exact_stop_evidence",
    "successor_authenticated_ready",
    "successor_authority_malformed",
    "successor_guest_identity_malformed",
    "successor_not_authenticated_ready",
    "workspace_cleanup_already_admitted",
    "workspace_hold_committed",
    "workspace_recovery_unresolved",
}
_DIGEST = re.compile(r"^sha256:([a-f0-9]{64})$")


def _bucket(value: object, allowed: set[str]) -> str:
    normalized = str(value or "").strip().lower()
    return normalized if normalized in allowed else "other"


def _reason(value: object) -> str:
    normalized = str(value or "").strip().lower()
    return normalized if normalized in _REASONS else "redacted"


def _digest_preview(value: object) -> str | None:
    match = _DIGEST.fullmatch(str(value or "").strip().lower())
    return f"sha256:{match.group(1)[:12]}…" if match else None


def _reference(value: object) -> str | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    return "sha256:" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:12] + "…"


class VMWorkspaceRecoveryTelemetry:
    """Emit bounded OTel dimensions and identifier-free recovery audit logs."""

    def __init__(self, *, meter: Any | None = None) -> None:
        if meter is None:
            if _HAS_OTEL and metrics is not None:
                meter = metrics.get_meter(
                    "orchestrator.services.vm_workspace_recovery", "1"
                )
            else:
                meter = _NullMeter()
        source = meter
        self._events = source.create_counter(
            "srw.vm_workspace_recovery.events",
            unit="{event}",
            description="VM workspace recovery transitions by bounded outcome",
        )
        self._age = source.create_histogram(
            "srw.vm_workspace_recovery.age",
            unit="s",
            description="Age of a recovery when a transition is observed",
        )

    def emit(
        self,
        *,
        event: str,
        state: str,
        phase: str,
        code: WorkspaceRecoveryCode | str,
        result: str,
        age_seconds: float | None = None,
        reason: str | None = None,
        cleanup_blocker: str | None = None,
        observation_digest: str | None = None,
        accepted_lease_token: int | None = None,
        hold_lease_token: int | None = None,
        operation_id: object | None = None,
        job_id: object | None = None,
        vm_uid: object | None = None,
        pvc_uid: object | None = None,
    ) -> None:
        """Record one transition without high-cardinality metric dimensions.

        Identifiers are accepted only so callers do not need a second logging
        path. Audits contain short one-way references; raw identifiers and the
        full controller observation digest are never emitted.
        """

        code_value = code.value if isinstance(code, WorkspaceRecoveryCode) else code
        attributes = {
            "event": _bucket(event, _EVENTS),
            "phase": _bucket(phase, _PHASES),
            "code": _bucket(code_value, _CODES),
            "result": _bucket(result, _RESULTS),
        }
        self._events.add(1, attributes=attributes)

        safe_age: float | None = None
        if age_seconds is not None:
            try:
                candidate = float(age_seconds)
            except (TypeError, ValueError):
                candidate = -1
            if candidate >= 0:
                safe_age = round(candidate, 3)
                self._age.record(candidate, attributes=attributes)

        audit: dict[str, Any] = {
            "audit_event": "vm_workspace_recovery",
            "recovery_event": attributes["event"],
            "recovery_state": _bucket(state, _STATES),
            "recovery_phase": attributes["phase"],
            "recovery_code": attributes["code"],
            "recovery_result": attributes["result"],
        }
        if safe_age is not None:
            audit["recovery_age_seconds"] = safe_age
        if reason is not None:
            audit["recovery_reason"] = _reason(reason)
        if cleanup_blocker is not None:
            audit["cleanup_blocker"] = _bucket(cleanup_blocker, _CLEANUP_BLOCKERS)
            if audit["cleanup_blocker"] == "other":
                audit["cleanup_blocker"] = "redacted"
        digest = _digest_preview(observation_digest)
        if digest is not None:
            audit["controller_observation_digest"] = digest
        if (
            isinstance(accepted_lease_token, int)
            and accepted_lease_token > 0
            and isinstance(hold_lease_token, int)
            and hold_lease_token > accepted_lease_token
        ):
            audit["queue_token_transition"] = {
                "from": accepted_lease_token,
                "to": hold_lease_token,
            }
        for key, value in (
            ("operation_ref", operation_id),
            ("job_ref", job_id),
            ("vm_ref", vm_uid),
            ("pvc_ref", pvc_uid),
        ):
            reference = _reference(value)
            if reference is not None:
                audit[key] = reference
        logger.info("VM workspace recovery transition", extra=audit)


workspace_recovery_telemetry = VMWorkspaceRecoveryTelemetry()
