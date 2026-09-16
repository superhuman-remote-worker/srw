"""Stdlib-only wire contracts for durable VM workspace recovery."""

from __future__ import annotations

import os
from dataclasses import dataclass
from enum import Enum
from typing import Literal, Mapping, Any
from uuid import UUID


class WorkspaceRecoveryCode(str, Enum):
    RUNTIME_NOT_READY = "workspace_runtime_not_ready"
    TRANSPORT_UNAVAILABLE = "workspace_transport_unavailable"
    REPLACEMENT_OBSERVED = "workspace_replacement_observed"
    IDENTITY_CONFLICT = "workspace_identity_conflict"
    PRIOR_RUNTIME_UNFENCED = "prior_runtime_unfenced"
    SHARED_WRITERS_UNFENCED = "shared_workspace_writers_unfenced"
    TOOL_OUTCOME_UNKNOWN = "tool_outcome_unknown"
    CHECKPOINT_UNAVAILABLE = "checkpoint_unavailable"
    DEADLINE_EXCEEDED = "workspace_recovery_deadline_exceeded"


RecoveryAction = Literal["hold_committed", "paused_attention", "recovered"]


@dataclass(frozen=True, slots=True)
class WorkspaceRecoveryDisposition:
    code: WorkspaceRecoveryCode
    action: RecoveryAction
    operation_id: UUID
    accepted_lease_token: int
    hold_lease_token: int

    def __post_init__(self) -> None:
        if self.accepted_lease_token <= 0:
            raise ValueError("accepted_lease_token must be positive")
        if self.hold_lease_token <= self.accepted_lease_token:
            raise ValueError("hold_lease_token must advance the accepted token")

    @classmethod
    def hold_committed(
        cls,
        *,
        operation_id: UUID,
        accepted_lease_token: int,
        hold_lease_token: int,
        code: WorkspaceRecoveryCode,
    ) -> "WorkspaceRecoveryDisposition":
        return cls(
            code=code,
            action="hold_committed",
            operation_id=operation_id,
            accepted_lease_token=accepted_lease_token,
            hold_lease_token=hold_lease_token,
        )

    @classmethod
    def paused_attention(
        cls,
        *,
        operation_id: UUID,
        accepted_lease_token: int,
        hold_lease_token: int,
        code: WorkspaceRecoveryCode,
    ) -> "WorkspaceRecoveryDisposition":
        return cls(
            code=code,
            action="paused_attention",
            operation_id=operation_id,
            accepted_lease_token=accepted_lease_token,
            hold_lease_token=hold_lease_token,
        )

    @classmethod
    def recovered(
        cls,
        *,
        operation_id: UUID,
        accepted_lease_token: int,
        hold_lease_token: int,
        code: WorkspaceRecoveryCode = WorkspaceRecoveryCode.REPLACEMENT_OBSERVED,
    ) -> "WorkspaceRecoveryDisposition":
        return cls(
            code=code,
            action="recovered",
            operation_id=operation_id,
            accepted_lease_token=accepted_lease_token,
            hold_lease_token=hold_lease_token,
        )

    def as_error_detail(self) -> dict[str, Any]:
        return {
            "detail": "VM workspace is temporarily unavailable",
            "code": self.code.value,
            "recovery": {
                "version": 1,
                "action": self.action,
                "operation_id": str(self.operation_id),
                "accepted_lease_token": self.accepted_lease_token,
                "hold_lease_token": self.hold_lease_token,
            },
        }


@dataclass(frozen=True, slots=True)
class RecoveryAttemptDisposition:
    job_id: UUID
    lease_token: int
    bundle_authorized: bool
    authority_digest: str | None
    disposition: Mapping[str, Any] | None
    recovery_id: UUID | None
    refunded: bool

    @property
    def requires_recovery_hold(self) -> bool:
        if self.recovery_id is not None:
            return True
        return bool(
            self.disposition
            and self.disposition.get("action") in {"hold_committed", "paused_attention"}
        )


def workspace_recovery_enabled() -> bool:
    return os.getenv("VM_WORKSPACE_RECOVERY_ENABLED", "false").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


__all__ = [
    "RecoveryAttemptDisposition",
    "WorkspaceRecoveryCode",
    "WorkspaceRecoveryDisposition",
    "workspace_recovery_enabled",
]
