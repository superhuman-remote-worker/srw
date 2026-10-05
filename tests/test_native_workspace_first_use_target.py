"""Only a positively identified no-watchdog class can skip native notice."""

import pytest
from types import SimpleNamespace
from uuid import uuid4

from orchestrator.services.native_workspace_first_use import describe_native_target
from shared.pinned_session_identity import PinnedSessionBinding


@pytest.mark.asyncio
async def test_stateless_and_officer_descriptors_are_explicit_no_watchdog_classes():
    async def impossible_target(**_kwargs):
        raise AssertionError("no pinned recipient lookup for a no-watchdog class")

    stateless = {"execution_lane": "stateless", "status": "active"}
    officer = {
        "execution_lane": "pinned",
        "status": "active",
        "metadata": {"config_override": {"officer": {"enabled": True}}},
    }
    unknown = {"execution_lane": "pinned", "status": "active", "metadata": {}}
    assert await describe_native_target(stateless, prepare=impossible_target) == (
        {"execution_lane": "stateless", "no_boot_watchdog": True},
        None,
    )
    assert await describe_native_target(officer, prepare=impossible_target) == (
        {"execution_lane": "pinned", "no_boot_watchdog": "officer"},
        None,
    )
    assert await describe_native_target(unknown, prepare=lambda **_: None) is None


@pytest.mark.asyncio
async def test_uuid_typed_db_row_describes_current_pinned_vm_recipient():
    thread_id, generation, agent_id, attach, pod, process = [uuid4() for _ in range(6)]
    binding = PinnedSessionBinding(
        thread_id=str(thread_id),
        runtime_generation=str(generation),
        agent_id=str(agent_id),
        runtime_attach_token=str(attach),
        agent_hostname="persistent-" + str(thread_id)[:12],
        pod_namespace="default",
        pod_uid=str(pod),
        pod_ip="10.0.0.2",
        pod_port=8001,
        agent_status="session",
    )
    calls = []

    async def prepare(**kwargs):
        calls.append(kwargs)
        return SimpleNamespace(binding=binding, process_generation=str(process))

    thread = {
        "id": thread_id,
        "agent_id": agent_id,
        "runtime_generation": generation,
        "runtime_attach_token": attach,
        "execution_lane": "pinned",
        "status": "active",
        "metadata": {"vm": {"status": "ready"}},
    }
    result = await describe_native_target(
        thread,
        prepare=prepare,
        backend="vm",
        vm_binding="a" * 64,
    )
    assert result is not None
    assert result[0]["native_recipient"]["thread_id"] == str(thread_id)
    assert result[0]["native_recipient"]["workspace_digest"] == "sha256:" + "a" * 64
    assert calls[0]["required_capability"] == "native_workspace_first_use1"
