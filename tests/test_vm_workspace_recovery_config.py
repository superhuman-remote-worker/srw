"""Runtime configuration for the VM workspace recovery rollout boundary."""

from __future__ import annotations

import pytest

from orchestrator.services.vm_workspace_recovery_config import (
    VMWorkspaceRecoverySettings,
)
from orchestrator.services.vm_workspace_recovery import VMWorkspaceRecoveryService


ENV_KEYS = (
    "VM_WORKSPACE_RECOVERY_ENABLED",
    "VM_WORKSPACE_REPLACEMENT_RECOVERY_ENABLED",
    "VM_WORKSPACE_RECOVERY_DEADLINE_SECONDS",
    "VM_WORKSPACE_RECOVERY_MAX_GLOBAL_PROBES",
    "VM_WORKSPACE_RECOVERY_MAX_PROBES_PER_NODE",
    "VM_WORKSPACE_RECOVERY_CLAIM_TTL_SECONDS",
    "VM_WORKSPACE_RECOVERY_PERMIT_TTL_SECONDS",
    "VM_WORKSPACE_RECOVERY_EXTERNAL_CALL_TIMEOUT_SECONDS",
)


def _clear(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in ENV_KEYS:
        monkeypatch.delenv(key, raising=False)


def test_workspace_recovery_runtime_defaults_are_dark_and_bounded(monkeypatch) -> None:
    _clear(monkeypatch)

    assert VMWorkspaceRecoverySettings.from_env() == VMWorkspaceRecoverySettings(
        enabled=False,
        replacement_enabled=False,
        deadline_seconds=900,
        max_global_probes=4,
        max_probes_per_node=1,
        claim_ttl_seconds=30,
        permit_ttl_seconds=30,
        external_call_timeout_seconds=10,
    )


def test_workspace_recovery_runtime_reads_the_rendered_contract(monkeypatch) -> None:
    values = {
        "VM_WORKSPACE_RECOVERY_ENABLED": "true",
        "VM_WORKSPACE_REPLACEMENT_RECOVERY_ENABLED": "true",
        "VM_WORKSPACE_RECOVERY_DEADLINE_SECONDS": "900",
        "VM_WORKSPACE_RECOVERY_MAX_GLOBAL_PROBES": "3",
        "VM_WORKSPACE_RECOVERY_MAX_PROBES_PER_NODE": "1",
        "VM_WORKSPACE_RECOVERY_CLAIM_TTL_SECONDS": "45",
        "VM_WORKSPACE_RECOVERY_PERMIT_TTL_SECONDS": "40",
        "VM_WORKSPACE_RECOVERY_EXTERNAL_CALL_TIMEOUT_SECONDS": "12",
    }
    for key, value in values.items():
        monkeypatch.setenv(key, value)

    settings = VMWorkspaceRecoverySettings.from_env()

    assert settings.enabled is True
    assert settings.replacement_enabled is True
    assert settings.max_global_probes == 3
    assert settings.claim_ttl_seconds == 45
    assert settings.permit_ttl_seconds == 40
    assert settings.external_call_timeout_seconds == 12


@pytest.mark.asyncio
async def test_validated_settings_reach_the_durable_claim_and_permit_boundary() -> None:
    class Store:
        def __init__(self) -> None:
            self.claim_kwargs: dict[str, object] | None = None

        async def claim_due(self, _operation_id, **kwargs):
            self.claim_kwargs = kwargs
            return None

    store = Store()
    settings = VMWorkspaceRecoverySettings(
        enabled=True,
        replacement_enabled=True,
        max_global_probes=3,
        max_probes_per_node=1,
        claim_ttl_seconds=45,
        permit_ttl_seconds=40,
        external_call_timeout_seconds=12,
    )
    service = VMWorkspaceRecoveryService.from_settings(
        store,
        object(),
        settings=settings,
    )

    await service.reconcile_once("00000000-0000-4000-8000-000000000601")

    assert service.automatic_enabled is True
    assert service.replacement_enabled is True
    assert service.probe_timeout_seconds == 12
    assert store.claim_kwargs == {
        "ttl_seconds": 45,
        "permit_ttl_seconds": 40,
        "max_global_probes": 3,
        "max_probes_per_node": 1,
    }


@pytest.mark.parametrize(
    ("key", "value", "message"),
    (
        ("VM_WORKSPACE_RECOVERY_ENABLED", "yes", "must be true or false"),
        (
            "VM_WORKSPACE_REPLACEMENT_RECOVERY_ENABLED",
            "true",
            "requires VM_WORKSPACE_RECOVERY_ENABLED=true",
        ),
        (
            "VM_WORKSPACE_RECOVERY_DEADLINE_SECONDS",
            "901",
            "protocol v1 requires exactly 900",
        ),
        (
            "VM_WORKSPACE_RECOVERY_MAX_PROBES_PER_NODE",
            "2",
            "protocol v1 requires exactly 1",
        ),
        (
            "VM_WORKSPACE_RECOVERY_CLAIM_TTL_SECONDS",
            "10",
            "must exceed VM_WORKSPACE_RECOVERY_EXTERNAL_CALL_TIMEOUT_SECONDS",
        ),
        (
            "VM_WORKSPACE_RECOVERY_PERMIT_TTL_SECONDS",
            "10",
            "must exceed VM_WORKSPACE_RECOVERY_EXTERNAL_CALL_TIMEOUT_SECONDS",
        ),
    ),
)
def test_workspace_recovery_runtime_rejects_unsupported_or_unsafe_values(
    monkeypatch, key: str, value: str, message: str
) -> None:
    _clear(monkeypatch)
    monkeypatch.setenv(key, value)

    with pytest.raises(ValueError, match=message):
        VMWorkspaceRecoverySettings.from_env()
