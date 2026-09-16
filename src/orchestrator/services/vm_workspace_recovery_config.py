"""Validated rollout settings for durable VM workspace recovery."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass


def _boolean(environ: Mapping[str, str], key: str, default: bool) -> bool:
    raw = environ.get(key)
    if raw is None:
        return default
    value = raw.strip().lower()
    if value == "true":
        return True
    if value == "false":
        return False
    raise ValueError(f"{key} must be true or false")


def _integer(environ: Mapping[str, str], key: str, default: int) -> int:
    raw = environ.get(key)
    if raw is None:
        return default
    try:
        return int(raw.strip())
    except (AttributeError, ValueError) as exc:
        raise ValueError(f"{key} must be an integer") from exc


@dataclass(frozen=True, slots=True)
class VMWorkspaceRecoverySettings:
    """One process-wide view of the recovery protocol's rollout envelope."""

    enabled: bool = False
    replacement_enabled: bool = False
    deadline_seconds: int = 900
    max_global_probes: int = 4
    max_probes_per_node: int = 1
    claim_ttl_seconds: int = 30
    permit_ttl_seconds: int = 30
    external_call_timeout_seconds: int = 10

    def __post_init__(self) -> None:
        if self.replacement_enabled and not self.enabled:
            raise ValueError(
                "VM_WORKSPACE_REPLACEMENT_RECOVERY_ENABLED=true requires "
                "VM_WORKSPACE_RECOVERY_ENABLED=true"
            )
        # Migration 0250 makes this relationship an immutable row constraint.
        # Reject unsupported tuning instead of accepting a value PostgreSQL
        # cannot honor and silently running with a different safety budget.
        if self.deadline_seconds != 900:
            raise ValueError(
                "VM_WORKSPACE_RECOVERY_DEADLINE_SECONDS protocol v1 requires exactly 900"
            )
        if not 1 <= self.max_global_probes <= 64:
            raise ValueError(
                "VM_WORKSPACE_RECOVERY_MAX_GLOBAL_PROBES must be between 1 and 64"
            )
        # The durable probe-slot schema has one unique live node key. A future
        # protocol can add indexed node slots before widening this setting.
        if self.max_probes_per_node != 1:
            raise ValueError(
                "VM_WORKSPACE_RECOVERY_MAX_PROBES_PER_NODE protocol v1 requires exactly 1"
            )
        if self.external_call_timeout_seconds <= 0:
            raise ValueError(
                "VM_WORKSPACE_RECOVERY_EXTERNAL_CALL_TIMEOUT_SECONDS must be positive"
            )
        for key, value in (
            ("VM_WORKSPACE_RECOVERY_CLAIM_TTL_SECONDS", self.claim_ttl_seconds),
            ("VM_WORKSPACE_RECOVERY_PERMIT_TTL_SECONDS", self.permit_ttl_seconds),
        ):
            if value <= self.external_call_timeout_seconds:
                raise ValueError(
                    f"{key} must exceed "
                    "VM_WORKSPACE_RECOVERY_EXTERNAL_CALL_TIMEOUT_SECONDS"
                )
            if value > self.deadline_seconds:
                raise ValueError(
                    f"{key} must not exceed VM_WORKSPACE_RECOVERY_DEADLINE_SECONDS"
                )

    @classmethod
    def from_env(
        cls, environ: Mapping[str, str] | None = None
    ) -> "VMWorkspaceRecoverySettings":
        source = os.environ if environ is None else environ
        return cls(
            enabled=_boolean(source, "VM_WORKSPACE_RECOVERY_ENABLED", False),
            replacement_enabled=_boolean(
                source, "VM_WORKSPACE_REPLACEMENT_RECOVERY_ENABLED", False
            ),
            deadline_seconds=_integer(
                source, "VM_WORKSPACE_RECOVERY_DEADLINE_SECONDS", 900
            ),
            max_global_probes=_integer(
                source, "VM_WORKSPACE_RECOVERY_MAX_GLOBAL_PROBES", 4
            ),
            max_probes_per_node=_integer(
                source, "VM_WORKSPACE_RECOVERY_MAX_PROBES_PER_NODE", 1
            ),
            claim_ttl_seconds=_integer(
                source, "VM_WORKSPACE_RECOVERY_CLAIM_TTL_SECONDS", 30
            ),
            permit_ttl_seconds=_integer(
                source, "VM_WORKSPACE_RECOVERY_PERMIT_TTL_SECONDS", 30
            ),
            external_call_timeout_seconds=_integer(
                source, "VM_WORKSPACE_RECOVERY_EXTERNAL_CALL_TIMEOUT_SECONDS", 10
            ),
        )
