"""Low-cardinality metrics and redacted audits for workspace recovery."""

from __future__ import annotations

import json
import logging

from orchestrator.logging_config import JsonLogFormatter
from orchestrator.services.vm_workspace_recovery_telemetry import (
    VMWorkspaceRecoveryTelemetry,
)
from shared.workspace_recovery import WorkspaceRecoveryCode


class _Instrument:
    def __init__(self) -> None:
        self.calls: list[tuple[float, dict[str, str]]] = []

    def add(self, value: float, attributes: dict[str, str]) -> None:
        self.calls.append((value, attributes))

    def record(self, value: float, attributes: dict[str, str]) -> None:
        self.calls.append((value, attributes))


class _Meter:
    def __init__(self) -> None:
        self.counter = _Instrument()
        self.age = _Instrument()

    def create_counter(self, *_args, **_kwargs) -> _Instrument:
        return self.counter

    def create_histogram(self, *_args, **_kwargs) -> _Instrument:
        return self.age


def test_metrics_use_only_aggregate_event_phase_code_and_result_labels() -> None:
    meter = _Meter()
    telemetry = VMWorkspaceRecoveryTelemetry(meter=meter)

    telemetry.emit(
        event="probe",
        state="recovering_workspace",
        phase="attesting",
        code=WorkspaceRecoveryCode.REPLACEMENT_OBSERVED,
        result="accepted",
        age_seconds=41.25,
        reason="successor_authenticated_ready",
        observation_digest="sha256:" + "a" * 64,
        operation_id="7c365609-a92a-40be-9198-2738f691735b",
        job_id="120c27d2-0f91-458a-aad3-b490aa2036e2",
        vm_uid="e507d6e4-476a-4993-8a4a-ae1249c73727",
        pvc_uid="af392012-4ef4-4f98-a9ed-30256c1b0645",
    )

    assert meter.counter.calls == [
        (
            1,
            {
                "event": "probe",
                "phase": "attesting",
                "code": "workspace_replacement_observed",
                "result": "accepted",
            },
        )
    ]
    assert meter.age.calls == [(41.25, meter.counter.calls[0][1])]
    assert (
        not {
            "operation_id",
            "job_id",
            "vm_uid",
            "pvc_uid",
            "reason",
            "observation_digest",
        }
        & meter.counter.calls[0][1].keys()
    )


def test_structured_audit_redacts_identifiers_and_controller_digest(caplog) -> None:
    telemetry = VMWorkspaceRecoveryTelemetry(meter=_Meter())
    raw = {
        "operation_id": "7c365609-a92a-40be-9198-2738f691735b",
        "job_id": "120c27d2-0f91-458a-aad3-b490aa2036e2",
        "vm_uid": "e507d6e4-476a-4993-8a4a-ae1249c73727",
        "pvc_uid": "af392012-4ef4-4f98-a9ed-30256c1b0645",
        "observation_digest": "sha256:" + "b" * 64,
    }

    with caplog.at_level(
        logging.INFO,
        logger="orchestrator.services.vm_workspace_recovery_telemetry",
    ):
        telemetry.emit(
            event="cleanup_blocked",
            state="recovering_workspace",
            phase="verifying_stop",
            code=WorkspaceRecoveryCode.PRIOR_RUNTIME_UNFENCED,
            result="blocked",
            age_seconds=73.9,
            reason="controller_pin_unavailable",
            cleanup_blocker="active_recovery_hold",
            accepted_lease_token=27,
            hold_lease_token=28,
            **raw,
        )

    record = caplog.records[-1]
    payload = json.loads(JsonLogFormatter().format(record))
    encoded = json.dumps(payload)

    assert payload["audit_event"] == "vm_workspace_recovery"
    assert payload["recovery_event"] == "cleanup_blocked"
    assert payload["recovery_state"] == "recovering_workspace"
    assert payload["recovery_age_seconds"] == 73.9
    assert payload["recovery_reason"] == "controller_pin_unavailable"
    assert payload["cleanup_blocker"] == "active_recovery_hold"
    assert payload["queue_token_transition"] == {"from": 27, "to": 28}
    assert payload["controller_observation_digest"] == "sha256:bbbbbbbbbbbb…"
    for value in raw.values():
        assert value not in encoded


def test_unbounded_audit_values_collapse_to_safe_aggregate_buckets(caplog) -> None:
    meter = _Meter()
    telemetry = VMWorkspaceRecoveryTelemetry(meter=meter)

    with caplog.at_level(logging.INFO):
        telemetry.emit(
            event="unexpected-7c365609-a92a-40be-9198-2738f691735b",
            state="unknown 120c27d2-0f91-458a-aad3-b490aa2036e2",
            phase="custom-phase-per-operation",
            code="custom-code-per-job",
            result="custom-result-per-pvc",
            age_seconds=-1,
            reason="password=hunter2",
            cleanup_blocker="job/120c27d2-0f91-458a-aad3-b490aa2036e2",
            observation_digest="not-a-sha256",
        )

    assert meter.counter.calls[0][1] == {
        "event": "other",
        "phase": "other",
        "code": "other",
        "result": "other",
    }
    payload = json.loads(JsonLogFormatter().format(caplog.records[-1]))
    assert payload["recovery_state"] == "other"
    assert payload["recovery_reason"] == "redacted"
    assert payload["cleanup_blocker"] == "redacted"
    assert "controller_observation_digest" not in payload
    assert "hunter2" not in json.dumps(payload)


def test_unknown_but_token_shaped_reason_is_redacted(caplog) -> None:
    telemetry = VMWorkspaceRecoveryTelemetry(meter=_Meter())

    with caplog.at_level(logging.INFO):
        telemetry.emit(
            event="pause",
            state="paused_attention",
            phase="paused_attention",
            code=WorkspaceRecoveryCode.IDENTITY_CONFLICT,
            result="accepted",
            reason="user_secret_token",
        )

    payload = json.loads(JsonLogFormatter().format(caplog.records[-1]))
    assert payload["recovery_reason"] == "redacted"
