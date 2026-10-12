"""The VM source can extend only the enclosing readiness observation."""

import asyncio
import time
from dataclasses import replace
from unittest.mock import AsyncMock

import pytest

from orchestrator.services import session_lifecycle
from shared.pinned_session_identity import PinnedSessionBinding


THREAD = "11111111-1111-4111-8111-111111111111"
GENERATION = "22222222-2222-4222-8222-222222222222"
AGENT = "33333333-3333-4333-8333-333333333333"
ATTACH = "44444444-4444-4444-8444-444444444444"
REQUEST = "66666666-6666-4666-8666-666666666666"
PROVISION = "77777777-7777-4777-8777-777777777777"


class Store:
    def __init__(self):
        self.thread = {
            "id": THREAD,
            "execution_lane": "pinned",
            "status": "created",
            "runtime_generation": GENERATION,
            "agent_id": AGENT,
            "runtime_attach_token": ATTACH,
            "runtime_retirement_token": None,
            "metadata": {},
        }
        self.binding = PinnedSessionBinding(
            thread_id=THREAD,
            runtime_generation=GENERATION,
            agent_id=AGENT,
            runtime_attach_token=ATTACH,
            agent_hostname="srw-agent-test",
            pod_namespace="srw",
            pod_uid="55555555-5555-4555-8555-555555555555",
            pod_ip="10.0.0.5",
            pod_port=8001,
            agent_status="session",
        )

    async def get_thread(self, _thread_id):
        return self.thread

    async def get_pinned_session_binding(self, _thread_id, **_kwargs):
        return self.binding


@pytest.fixture
def clock(monkeypatch):
    elapsed = {"now": 0.0}
    monkeypatch.setattr(time, "monotonic", lambda: elapsed["now"])
    original_sleep = asyncio.sleep

    async def advance(_seconds):
        elapsed["now"] += 30.0
        await original_sleep(0)

    monkeypatch.setattr(session_lifecycle.asyncio, "sleep", advance)
    return elapsed


def _source(phase="resource_wait", *, elapsed=None):
    view = {
        "contract_version": 1,
        "phase": phase,
        "request_id": REQUEST,
        "provision_generation": PROVISION,
        "runtime_generation": GENERATION,
    }
    if elapsed is not None:
        view["admission_elapsed_s"] = elapsed
    return view


async def _observe(store, timeout=960):
    return await session_lifecycle.wait_for_ready(
        "10.0.0.5",
        8001,
        timeout,
        expected_session_identity_fingerprint=(
            store.binding.session_identity_fingerprint
        ),
        vm_store=store,
        vm_thread_id=THREAD,
        vm_runtime_generation=GENERATION,
        vm_binding=store.binding,
    )


@pytest.mark.asyncio
async def test_lost_wait_signal_spends_120_seconds_without_ready(monkeypatch, clock):
    store = Store()

    async def view(*_args, **_kwargs):
        return _source() if clock["now"] == 0 else None

    monkeypatch.setattr(session_lifecycle, "initial_vm_startup_view", view)
    monkeypatch.setattr(session_lifecycle, "probe_ready", AsyncMock(return_value=False))
    assert await _observe(store) is False
    assert clock["now"] == 120


@pytest.mark.asyncio
async def test_late_wait_cannot_reopen_spent_communications_allowance(
    monkeypatch, clock
):
    store = Store()
    views = []

    async def view(*_args, **_kwargs):
        views.append(clock["now"])
        return _source() if clock["now"] in {0, 120} else None

    monkeypatch.setattr(session_lifecycle, "initial_vm_startup_view", view)
    monkeypatch.setattr(session_lifecycle, "probe_ready", AsyncMock(return_value=False))
    assert await _observe(store) is False
    assert clock["now"] == 120
    assert 120 not in views


@pytest.mark.asyncio
async def test_inflight_source_reply_after_allowance_expiry_cannot_renew(
    monkeypatch, clock
):
    store = Store()

    async def view(*_args, **_kwargs):
        if clock["now"] == 30:
            clock["now"] = 121
        return _source() if clock["now"] <= 121 else None

    monkeypatch.setattr(session_lifecycle, "initial_vm_startup_view", view)
    monkeypatch.setattr(session_lifecycle, "probe_ready", AsyncMock(return_value=False))
    assert await _observe(store) is False
    assert clock["now"] == 121


@pytest.mark.asyncio
async def test_transient_authority_read_outage_spends_allowance(monkeypatch, clock):
    store = Store()
    original_get_thread = store.get_thread

    async def get_thread(thread_id):
        if clock["now"] == 30:
            raise ConnectionError("temporary database outage")
        return await original_get_thread(thread_id)

    store.get_thread = get_thread

    async def view(*_args, **_kwargs):
        return _source() if clock["now"] == 0 else None

    monkeypatch.setattr(session_lifecycle, "initial_vm_startup_view", view)
    monkeypatch.setattr(session_lifecycle, "probe_ready", AsyncMock(return_value=False))
    assert await _observe(store) is False
    assert clock["now"] == 120


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["agent", "generation", "binding", "end"])
async def test_wait_stops_on_changed_authority(monkeypatch, clock, change):
    store = Store()
    monkeypatch.setattr(
        session_lifecycle,
        "initial_vm_startup_view",
        AsyncMock(return_value=_source()),
    )
    monkeypatch.setattr(session_lifecycle, "probe_ready", AsyncMock(return_value=True))
    original_sleep = session_lifecycle.asyncio.sleep

    async def mutate(seconds):
        await original_sleep(seconds)
        if clock["now"] == 30:
            if change == "agent":
                store.thread["agent_id"] = "88888888-8888-4888-8888-888888888888"
            elif change == "generation":
                store.thread["runtime_generation"] = (
                    "88888888-8888-4888-8888-888888888888"
                )
            elif change == "binding":
                store.binding = replace(store.binding, pod_ip="10.0.0.99")
            else:
                store.thread["runtime_retirement_token"] = (
                    "88888888-8888-4888-8888-888888888888"
                )

    monkeypatch.setattr(session_lifecycle.asyncio, "sleep", mutate)
    assert await _observe(store) is False
    assert clock["now"] == 30


@pytest.mark.asyncio
async def test_expired_durable_admission_cannot_publish_ready(monkeypatch, clock):
    store = Store()

    async def view(*_args, **_kwargs):
        return _source() if clock["now"] == 0 else _source("admitted", elapsed=961)

    monkeypatch.setattr(session_lifecycle, "initial_vm_startup_view", view)
    monkeypatch.setattr(session_lifecycle, "probe_ready", AsyncMock(return_value=True))
    assert await _observe(store) is False
    assert clock["now"] == 30


@pytest.mark.asyncio
async def test_later_admission_elapsed_cannot_extend_first_deadline(monkeypatch, clock):
    store = Store()

    async def view(*_args, **_kwargs):
        if clock["now"] == 0:
            return _source("admitted", elapsed=30)
        return _source("admitted", elapsed=0)

    async def probe(*_args, **_kwargs):
        return clock["now"] >= 960

    monkeypatch.setattr(session_lifecycle, "initial_vm_startup_view", view)
    monkeypatch.setattr(session_lifecycle, "probe_ready", probe)
    assert await _observe(store) is False
    assert clock["now"] == 930


@pytest.mark.asyncio
async def test_changed_initial_source_cannot_extend_wait(monkeypatch, clock):
    store = Store()

    async def view(*_args, **_kwargs):
        if clock["now"] == 0:
            return _source()
        return {
            **_source(),
            "request_id": "88888888-8888-4888-8888-888888888888",
        }

    monkeypatch.setattr(session_lifecycle, "initial_vm_startup_view", view)
    monkeypatch.setattr(session_lifecycle, "probe_ready", AsyncMock(return_value=False))
    assert await _observe(store) is False
    assert clock["now"] == 30


@pytest.mark.asyncio
async def test_probe_finishing_after_admission_deadline_cannot_return_ready(
    monkeypatch, clock
):
    store = Store()
    monkeypatch.setattr(
        session_lifecycle,
        "initial_vm_startup_view",
        AsyncMock(return_value=_source("admitted", elapsed=950)),
    )

    async def late_ready(*_args, **_kwargs):
        clock["now"] = 20
        return True

    monkeypatch.setattr(session_lifecycle, "probe_ready", late_ready)
    assert await _observe(store) is False


@pytest.mark.asyncio
async def test_vm_observer_requires_the_captured_pinned_ready_fingerprint(
    monkeypatch, clock
):
    store = Store()
    monkeypatch.setattr(
        session_lifecycle,
        "initial_vm_startup_view",
        AsyncMock(return_value=_source("admitted", elapsed=0)),
    )
    monkeypatch.setattr(session_lifecycle, "probe_ready", AsyncMock(return_value=True))
    assert (
        await session_lifecycle.wait_for_ready(
            store.binding.pod_ip,
            store.binding.pod_port,
            960,
            vm_store=store,
            vm_thread_id=THREAD,
            vm_runtime_generation=GENERATION,
            vm_binding=store.binding,
        )
        is False
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["binding", "end"])
async def test_ready_probe_cannot_publish_after_awaited_authority_change(
    monkeypatch, clock, change
):
    store = Store()
    monkeypatch.setattr(
        session_lifecycle,
        "initial_vm_startup_view",
        AsyncMock(return_value=_source("admitted", elapsed=0)),
    )

    async def ready_then_change(*_args, **_kwargs):
        if change == "binding":
            store.binding = replace(store.binding, pod_uid="rotated-pod")
        else:
            store.thread["runtime_retirement_token"] = (
                "88888888-8888-4888-8888-888888888888"
            )
        return True

    monkeypatch.setattr(session_lifecycle, "probe_ready", ready_then_change)
    assert await _observe(store) is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "metadata",
    [
        {},
        {"vm": {"rootdisk": "kept"}},
        {"vm": {"idle_wake_operation_id": "wake"}},
        {"vm": {"status": "provisioning"}},
    ],
    ids=["legacy", "retained", "wake", "unsupported-vm"],
)
async def test_unsupported_vm_sources_remain_finite(monkeypatch, clock, metadata):
    store = Store()
    store.thread["metadata"] = metadata
    monkeypatch.setattr(session_lifecycle, "probe_ready", AsyncMock(return_value=False))
    assert await _observe(store) is False
    assert clock["now"] == 960


@pytest.mark.asyncio
async def test_sandbox_readiness_remains_finite(monkeypatch, clock):
    monkeypatch.setattr(session_lifecycle, "probe_ready", AsyncMock(return_value=False))
    assert await session_lifecycle.wait_for_ready("10.0.0.5", 8001, 180) is False
    assert clock["now"] == 180


@pytest.mark.asyncio
@pytest.mark.parametrize("vm_context", [False, True], ids=["sandbox", "legacy-vm"])
async def test_inflight_legacy_ready_probe_keeps_finite_result(
    monkeypatch, clock, vm_context
):
    store = Store()

    async def ready(*_args, **_kwargs):
        clock["now"] = 181
        return True

    monkeypatch.setattr(session_lifecycle, "probe_ready", ready)
    if vm_context:
        assert await _observe(store, timeout=180) is True
    else:
        assert await session_lifecycle.wait_for_ready("10.0.0.5", 8001, 180) is True
