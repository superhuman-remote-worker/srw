"""The provisioning readers fall back to an upgrade's template (Slice A2b)."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from orchestrator.services.sandbox_workspace_settings import resolve_sandbox_settings
from orchestrator.services.vm_workspace_config import vm_provisioning_options

JOB_ID = "4b0f3f7e-8d7c-4a53-9d7e-6f1d2f0c9a11"


@pytest.mark.asyncio
async def test_a_session_vm_upgrade_uses_its_template_options():
    thread = {
        "id": JOB_ID,
        "metadata": {
            "vm": {
                "upgrade_config": {
                    "image": "r.example/vm:2",
                    "cpu_cores": 4,
                    "memory": "8Gi",
                }
            }
        },
    }
    options = await vm_provisioning_options(SimpleNamespace(), "Session", thread)
    assert options == {"vm_image": "r.example/vm:2", "cpu_cores": 4, "memory": "8Gi"}


@pytest.mark.asyncio
async def test_a_job_vm_upgrade_uses_its_template_options():
    job = {"id": JOB_ID, "context": {"vm": {"upgrade_config": {"disk_size": "40Gi"}}}}
    assert await vm_provisioning_options(SimpleNamespace(), "Job", job) == {
        "disk_size": "40Gi"
    }


@pytest.mark.asyncio
async def test_a_snapshot_vm_keeps_its_own_options(monkeypatch):
    monkeypatch.setattr(
        "orchestrator.services.vm_workspace_config.read_execution",
        AsyncMock(return_value={"resolved": {}}),
    )
    monkeypatch.setattr(
        "orchestrator.services.vm_workspace_config.srw_snapshot_config",
        lambda _snapshot: ({}, {"workspace": {"vm": {"image": "r.example/own:1"}}}),
    )
    thread = {
        "id": JOB_ID,
        "execution_harness_adapter": "srw/v1",
        "metadata": {"vm": {"upgrade_config": {"image": "r.example/vm:2"}}},
    }
    options = await vm_provisioning_options(SimpleNamespace(), "Session", thread)
    assert options == {"vm_image": "r.example/own:1"}


@pytest.mark.asyncio
async def test_a_job_container_upgrade_uses_its_template_settings(monkeypatch):
    monkeypatch.setattr(
        "orchestrator.services.sandbox_workspace_settings.read_execution",
        AsyncMock(return_value=None),
    )
    store = SimpleNamespace(
        get_job=AsyncMock(
            return_value={
                "id": JOB_ID,
                "context": {
                    "workspace_container": {
                        "upgrade_config": {
                            "image": "r.example/site:1",
                            "cpu": 1,
                            "sources": {"tier": "upgrade"},
                        }
                    }
                },
            }
        )
    )
    settings = await resolve_sandbox_settings(store, "job", JOB_ID)
    assert (settings.image, settings.cpu) == ("r.example/site:1", 1)


@pytest.mark.asyncio
async def test_a_job_admitted_from_a_container_template_keeps_its_settings(
    monkeypatch,
):
    monkeypatch.setattr(
        "orchestrator.services.sandbox_workspace_settings.read_execution",
        AsyncMock(return_value={"harness_adapter": "srw/v1"}),
    )
    monkeypatch.setattr(
        "orchestrator.services.sandbox_workspace_settings.srw_snapshot_config",
        lambda _snapshot: (
            {},
            {"workspace": {"sandbox": {"image": "r.example/admitted:1"}}},
        ),
    )
    store = SimpleNamespace(get_job=AsyncMock())
    settings = await resolve_sandbox_settings(store, "job", JOB_ID)
    assert settings.image == "r.example/admitted:1"
    store.get_job.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_session_never_reads_a_job_upgrade(monkeypatch):
    monkeypatch.setattr(
        "orchestrator.services.sandbox_workspace_settings.read_execution",
        AsyncMock(return_value=None),
    )
    store = SimpleNamespace(get_job=AsyncMock())
    settings = await resolve_sandbox_settings(store, "session", JOB_ID)
    assert settings.is_empty()
    store.get_job.assert_not_awaited()
