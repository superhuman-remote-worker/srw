"""Contracts for the in-image real VM recovery fault scenario."""

from __future__ import annotations

import pytest

from orchestrator.operator_cli.vm_workspace_recovery_acceptance import (
    CONFIRMATION,
    LiveScenario,
    PROTOCOL_VERSION,
    REQUIRED_SCENARIOS,
    require_execution_guard,
)


def test_acceptance_command_carries_the_complete_live_scenario_matrix() -> None:
    assert PROTOCOL_VERSION == 1
    assert REQUIRED_SCENARIOS == (
        "response_loss",
        "leader_overlap",
        "slow_boot",
        "deadline",
        "missing_stop_evidence",
        "forced_deletion",
        "replacement",
    )


def test_acceptance_command_requires_chart_gate_and_exact_confirmation() -> None:
    with pytest.raises(RuntimeError, match="chart gate"):
        require_execution_guard({}, confirmation=CONFIRMATION, protocol_version=1)
    with pytest.raises(RuntimeError, match="confirmation"):
        require_execution_guard(
            {"VM_WORKSPACE_RECOVERY_ACCEPTANCE_GATE_ENABLED": "true"},
            confirmation="wrong",
            protocol_version=1,
        )
    with pytest.raises(RuntimeError, match="protocol"):
        require_execution_guard(
            {"VM_WORKSPACE_RECOVERY_ACCEPTANCE_GATE_ENABLED": "true"},
            confirmation=CONFIRMATION,
            protocol_version=2,
        )

    require_execution_guard(
        {"VM_WORKSPACE_RECOVERY_ACCEPTANCE_GATE_ENABLED": "true"},
        confirmation=CONFIRMATION,
        protocol_version=1,
    )


@pytest.mark.asyncio
async def test_acceptance_vm_patch_uses_supported_custom_objects_arguments() -> None:
    calls: list[dict[str, object]] = []

    class CustomObjects:
        def patch_namespaced_custom_object(
            self, *, group, version, namespace, plural, name, body
        ):
            calls.append(
                {
                    "group": group,
                    "version": version,
                    "namespace": namespace,
                    "plural": plural,
                    "name": name,
                    "body": body,
                }
            )

    scenario = object.__new__(LiveScenario)
    scenario.namespace = "agent-vms"
    scenario._custom = CustomObjects()

    await scenario._set_vm_run_strategy(
        "00000000-0000-4000-8000-000000000611", "Halted"
    )

    assert calls == [
        {
            "group": "kubevirt.io",
            "version": "v1",
            "namespace": "agent-vms",
            "plural": "virtualmachines",
            "name": "agent-vm-00000000-0000-4000-8000-000000000611",
            "body": {"spec": {"runStrategy": "Halted"}},
        }
    ]


@pytest.mark.asyncio
async def test_acceptance_fixture_uses_production_job_creation_boundary() -> None:
    executed: list[tuple[str, tuple[object, ...]]] = []
    create_kwargs: list[dict[str, object]] = []

    class Transaction:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

    class Connection:
        def transaction(self):
            return Transaction()

        async def execute(self, query, *values):
            executed.append((query, values))

    class Acquire:
        async def __aenter__(self):
            return Connection()

        async def __aexit__(self, *_args):
            return None

    class Database:
        async def create_job(self, **kwargs):
            create_kwargs.append(kwargs)
            return {"id": kwargs["job_id"]}

        def acquire(self):
            return Acquire()

    scenario = object.__new__(LiveScenario)
    scenario.db = Database()
    scenario.run_id = "fixture-boundary"
    scenario.job_id = None

    job_id, lease_token = await scenario._create_job()

    assert create_kwargs == [
        {
            "description": "[vm-recovery-gate:fixture-boundary] retained disk fixture",
            "context": {"vm_workspace_recovery_acceptance_gate": "fixture-boundary"},
            "origin": "lifecycle",
            "status": "processing",
            "execution_lane": "stateless",
            "job_id": job_id,
        }
    ]
    assert lease_token == 27
    assert scenario.job_id == job_id
    assert ["run_queue" in query for query, _values in executed] == [True, False]
    assert ["worker_batch_attempts" in query for query, _values in executed] == [
        False,
        True,
    ]
    assert all("INSERT INTO jobs" not in query for query, _values in executed)
