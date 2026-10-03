"""An exact attachment-End retries without waiting for unrelated full scans."""

import asyncio
from types import SimpleNamespace

import pytest

from orchestrator.application import controls, sessions
from orchestrator import main
from orchestrator.services import stale_agent_detector as detector
from orchestrator.services.session_attach_binding import (
    acknowledge_retiring_failed_attach,
)
from orchestrator.services.vm_provisioner import VMTeardownResult
from tests import test_vm_end_actuator_handoff_real_postgres as fixtures


pg_dsn = fixtures.pg_dsn
_schema_applied = fixtures._schema_applied
_base_db = fixtures._base_db
db = fixtures.db


class RetryClock:
    """Model only this owner's shutdown waits; never cancel remote cleanup."""

    def __init__(self, stopped, *, stop_at=60):
        self.stopped = stopped
        self.stop_at = stop_at
        self.elapsed = 0.0
        self.delays = []

    def __getattr__(self, name):
        return getattr(asyncio, name)

    def get_running_loop(self):
        loop = asyncio.get_running_loop()
        return SimpleNamespace(time=lambda: loop.time() + self.elapsed)

    async def wait_for(self, coro, *, timeout):
        if coro.cr_code is not asyncio.Event.wait.__code__:
            return await asyncio.wait_for(coro, timeout=timeout)
        coro.close()
        if self.stopped.is_set():
            return True
        self.delays.append(timeout)
        self.elapsed += timeout
        if self.elapsed >= self.stop_at:
            self.stopped.set()
            return True
        raise asyncio.TimeoutError


@pytest.mark.asyncio
@pytest.mark.parametrize("permanent", [False, True])
async def test_pre_setup_end_retries_remote_proof_within_five_seconds(
    db, monkeypatch, permanent
):
    ids, retirement, _, events, k8s, vm = await fixtures.scenario(
        db, monkeypatch, permanent=permanent, actor_status="booting"
    )
    proof = dict(
        expected_runtime_generation=ids["generation"],
        expected_attach_token=ids["attach_token"],
        expected_agent_pod_uid=ids["pod_uid"],
        local_quiescence_protocol="agent_attach_not_started_v1",
        workspace_generation=None,
        workspace_runtime_incarnation=None,
        dependencies=sessions.session_attach_binding_dependencies(
            main.app.state.resources
        ),
    )
    assert not await acknowledge_retiring_failed_attach(
        ids["agent"], ids["thread"], **proof
    )
    stopped = asyncio.Event()
    clock = RetryClock(stopped)
    monkeypatch.setattr(detector, "asyncio", clock)
    attempts = []
    stopped_uids = []
    stop_pod = k8s.delete_namespaced_pod

    def record_exact_stop(*args, **kwargs):
        stopped_uids.append(kwargs["body"]["preconditions"]["uid"])
        return stop_pod(*args, **kwargs)

    monkeypatch.setattr(k8s, "delete_namespaced_pod", record_exact_stop)
    release = vm.release_vm_captured

    async def prove_remote_zero(thread_id, identity, **kwargs):
        current = await db.get_thread(thread_id)
        assert str(current["runtime_generation"]) == ids["generation"]
        assert str(current["runtime_retirement_token"]) == retirement["token"]
        assert str(current["agent_id"]) == ids["agent"]
        assert str(current["runtime_attach_token"]) == ids["attach_token"]
        assert current["runtime_retirement_local_quiescence"] is None
        attempts.append(clock.elapsed)
        if len(attempts) == 1:
            # A delete request/pending remote observation is not process zero.
            assert current["status"] == "active"
            return VMTeardownResult("retry_pending", False)
        result = await release(thread_id, identity, **kwargs)
        stopped.set()
        return result

    monkeypatch.setattr(vm, "release_vm_captured", prove_remote_zero)
    full_scans = []
    gc = db.gc_offline_agents

    async def record_full_scan(**kwargs):
        full_scans.append(clock.elapsed)
        return await gc(**kwargs)

    monkeypatch.setattr(db, "gc_offline_agents", record_full_scan)
    await asyncio.wait_for(
        detector.stale_agent_detector(
            stopped,
            dependencies=controls.stale_agent_detector_dependencies(
                main.app.state.resources
            ),
        ),
        timeout=15,
    )
    assert len(attempts) == 2, "exact remote cleanup must retry before the 60s scan"
    assert 0 < attempts[1] - attempts[0] <= 5
    assert full_scans == [0], "unrelated reconciliation keeps its 60s cadence"
    assert events[-1] == "vm-stop"
    assert events[:-1] and set(events[:-1]) == {"pod-stop"}
    assert stopped_uids and set(stopped_uids) == {ids["pod_uid"]}
    current = await db.get_thread(ids["thread"])
    assert current is None if permanent else current["status"] == "ended"
    assert await db.list_retryable_pinned_retirements() == []
    assert await acknowledge_retiring_failed_attach(
        ids["agent"], ids["thread"], **proof
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("actor_status", ["booting", "ready", "session"])
async def test_short_retry_poll_never_releases_an_unmarked_live_actor(
    db, monkeypatch, actor_status
):
    ids, retirement, _, events, _, _ = await fixtures.scenario(
        db, monkeypatch, actor_status=actor_status
    )
    assert await db.list_retryable_pinned_retirements() == []
    stopped = asyncio.Event()
    clock = RetryClock(stopped, stop_at=10)
    monkeypatch.setattr(detector, "asyncio", clock)
    await asyncio.wait_for(
        detector.stale_agent_detector(
            stopped,
            dependencies=controls.stale_agent_detector_dependencies(
                main.app.state.resources
            ),
        ),
        timeout=15,
    )
    current = await db.get_thread(ids["thread"])
    assert str(current["runtime_retirement_token"]) == retirement["token"]
    assert str(current["agent_id"]) == ids["agent"]
    assert current["runtime_retirement_local_quiescence"] is None
    assert current["runtime_retirement_actuator_request"] is None
    assert current["status"] == "active"
    assert events == []


@pytest.mark.asyncio
async def test_short_retry_keeps_ambiguous_remote_writers_pending(db, monkeypatch):
    ids, retirement, request, events, _, vm = await fixtures.scenario(db, monkeypatch)
    assert await db.request_pinned_thread_retirement_actuator(ids["thread"], **request)
    stopped = asyncio.Event()
    clock = RetryClock(stopped, stop_at=10)
    monkeypatch.setattr(detector, "asyncio", clock)

    async def ambiguous_remote(*args, **kwargs):
        return VMTeardownResult("process_zero_unproven", False)

    monkeypatch.setattr(vm, "release_vm_captured", ambiguous_remote)
    await asyncio.wait_for(
        detector.stale_agent_detector(
            stopped,
            dependencies=controls.stale_agent_detector_dependencies(
                main.app.state.resources
            ),
        ),
        timeout=15,
    )
    current = await db.get_thread(ids["thread"])
    assert str(current["runtime_retirement_token"]) == retirement["token"]
    assert str(current["runtime_generation"]) == ids["generation"]
    assert str(current["agent_id"]) == ids["agent"]
    assert current["runtime_retirement_local_quiescence"] is None
    assert current["runtime_retirement_stage_receipt"] is None
    assert current["runtime_retirement_actuator_request"] is not None
    assert current["status"] == "active"
    # Even proven death of the separate agent Pod cannot settle remote writers.
    assert events and set(events) == {"pod-stop"}
