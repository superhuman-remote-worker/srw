"""Terminal Job VM cleanup survives a slow caller and an orchestrator restart."""

import asyncio
from contextlib import asynccontextmanager
import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from orchestrator.services.job_mutation_controls import (
    JobControlOperations, _terminal_vm_needs_release,
)
from orchestrator.services import thread_retirement


JOB_ID = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
VM = {"status": "ready", "provision_generation": "11111111-2222-4333-8444-555555555555"}


def controls(*, store, archive=None):
    return JobControlOperations(SimpleNamespace(
        store=store,
        logger=logging.getLogger(__name__),
        completion_commands_enabled=lambda: True,
        completion_control=SimpleNamespace(
            dispatch_guard_kwargs=lambda: {}, active_claim=lambda _job: False,
        ),
        manifest_cancel=AsyncMock(return_value=False),
        archive_and_cleanup_workspace=archive or AsyncMock(),
        handle_scholar_completion=AsyncMock(),
        maybe_wake_session=AsyncMock(),
        kick_session_wake_drain=MagicMock(),
        trigger_dispatch=MagicMock(),
        resolve_job_notifications=AsyncMock(),
    ))


def test_inherited_child_cannot_select_parent_vm_for_terminal_cleanup():
    child = {"parent_job_id": "bbbbbbbb-cccc-4ddd-8eee-ffffffffffff",
             "context": {"vm": VM, "inherits_parent_workspace": True}}
    assert _terminal_vm_needs_release(child) is False


@pytest.mark.asyncio
async def test_cancelled_stateless_vm_returns_durable_pending_without_snapshot_wait():
    job = {"id": JOB_ID, "status": "processing", "execution_lane": "stateless",
           "assigned_agent_id": None, "context": {"vm": VM}}
    store = SimpleNamespace(cancel_stateless_job=AsyncMock(return_value=(True, True)))
    archive = AsyncMock(side_effect=AssertionError("slow snapshot entered request"))
    operation = controls(store=store, archive=archive)
    operation.cascade_cancel_to_children = AsyncMock(return_value=True)
    operation.wait_for_stateless_cancel_settle = AsyncMock(
        side_effect=AssertionError("cleanup wait entered request"))

    result = await operation.cancel(JOB_ID, job=job)

    assert result == {"status": "cancelled", "cleanup_pending": True}
    store.cancel_stateless_job.assert_awaited_once()
    archive.assert_not_awaited()


@pytest.mark.asyncio
async def test_pinned_vm_cancel_signals_with_bounded_wait_before_pending_response():
    agent_id = "bbbbbbbb-cccc-4ddd-8eee-ffffffffffff"
    job = {"id": JOB_ID, "status": "processing", "execution_lane": "pinned",
           "assigned_agent_id": agent_id, "context": {"vm": VM}}
    store = SimpleNamespace(linearize_pinned_cancel=AsyncMock(return_value=True))
    archive = AsyncMock(side_effect=AssertionError("slow snapshot entered request"))
    operation = controls(store=store, archive=archive)
    operation._signal = AsyncMock(return_value=(True, 200))

    assert await operation.cancel(JOB_ID, job=job) == {
        "status": "cancelled", "cleanup_pending": True,
    }
    operation._signal.assert_awaited_once_with(
        JOB_ID, agent_id, "cancel", reason="Cancelled via cockpit",
        timeout_seconds=5,
    )
    archive.assert_not_awaited()


@pytest.mark.asyncio
async def test_reconcile_replays_terminal_vm_after_caller_disappears():
    job = {"id": JOB_ID, "status": "cancelled", "execution_lane": "stateless",
           "context": {"vm": VM, "_stateless_cancel_cleanup_pending": True}}

    class Store:
        def __init__(self):
            self.row = job

        async def list_terminal_vm_cleanup_jobs(self, *, limit, after_id):
            assert limit == 4 and after_id is None
            return [self.row]

        async def get_job(self, job_id):
            assert job_id == JOB_ID
            return self.row

        async def get_descendant_jobs(self, job_id, *, include_cancelled):
            assert job_id == JOB_ID and include_cancelled is True
            return []

    operation = controls(store=Store())
    operation.wait_for_stateless_cancel_settle = AsyncMock(return_value=True)

    assert await operation.reconcile_terminal_vm_cleanups(limit=4) == 1
    operation.wait_for_stateless_cancel_settle.assert_awaited_once_with(
        JOB_ID, timeout_seconds=0,
    )


@pytest.mark.asyncio
async def test_reconcile_ordinary_terminal_vm_uses_single_owner_and_strict_archive():
    job = {"id": JOB_ID, "status": "completed", "execution_lane": "pinned",
           "context": {"vm": VM, "_job_terminal_vm_cleanup": {
               "version": 1, "source": "approve",
               "provision_generation": VM["provision_generation"],
           }}}

    @asynccontextmanager
    async def lock(job_id):
        assert job_id == JOB_ID
        yield True

    store = SimpleNamespace(
        list_terminal_vm_cleanup_jobs=AsyncMock(return_value=[job]),
        get_job=AsyncMock(return_value=job),
        stateless_cancel_cleanup_lock=lock,
        complete_terminal_vm_cleanup_marker=AsyncMock(side_effect=[False, True]),
    )
    archive = AsyncMock()
    operation = controls(store=store, archive=archive)

    assert await operation.reconcile_terminal_vm_cleanups(limit=4) == 1
    archive.assert_awaited_once_with(JOB_ID)
    assert store.complete_terminal_vm_cleanup_marker.await_count == 2
    store.complete_terminal_vm_cleanup_marker.assert_awaited_with(
        JOB_ID, expected_generation=VM["provision_generation"],
    )


@pytest.mark.asyncio
async def test_reconcile_rejects_marker_for_another_vm_generation():
    job = {"id": JOB_ID, "status": "completed", "execution_lane": "pinned",
           "context": {"vm": VM, "_job_terminal_vm_cleanup": {
               "version": 1, "source": "approve",
               "provision_generation": "99999999-2222-4333-8444-555555555555",
           }}}
    @asynccontextmanager
    async def lock(_job_id):
        yield True

    store = SimpleNamespace(
        list_terminal_vm_cleanup_jobs=AsyncMock(return_value=[job]),
        get_job=AsyncMock(return_value=job),
        stateless_cancel_cleanup_lock=lock,
    )
    archive = AsyncMock()
    operation = controls(store=store, archive=archive)

    assert await operation.reconcile_terminal_vm_cleanups(limit=4) == 0
    archive.assert_not_awaited()


@pytest.mark.asyncio
async def test_completed_exact_parent_clears_marker_without_restarting_teardown():
    marker = {"version": 1, "source": "approve",
              "provision_generation": VM["provision_generation"],
              "admission_id": "77777777-8888-4999-8aaa-bbbbbbbbbbbb"}
    job = {"id": JOB_ID, "status": "completed", "execution_lane": "pinned",
           "context": {"vm": {**VM, "status": "deleting"},
                       "_job_terminal_vm_cleanup": marker}}

    @asynccontextmanager
    async def lock(_job_id):
        yield True

    store = SimpleNamespace(
        list_terminal_vm_cleanup_jobs=AsyncMock(return_value=[job]),
        get_job=AsyncMock(return_value=job),
        stateless_cancel_cleanup_lock=lock,
        complete_terminal_vm_cleanup_marker=AsyncMock(return_value=True),
    )
    archive = AsyncMock(side_effect=AssertionError("completed parent restarted"))
    operation = controls(store=store, archive=archive)

    assert await operation.reconcile_terminal_vm_cleanups() == 1
    archive.assert_not_awaited()
    store.complete_terminal_vm_cleanup_marker.assert_awaited_once_with(
        JOB_ID, expected_generation=VM["provision_generation"],
    )


@pytest.mark.asyncio
async def test_hung_descendant_is_bounded_and_does_not_hide_next_candidate(monkeypatch):
    from orchestrator.services import job_mutation_controls as module

    monkeypatch.setattr(module, "TERMINAL_VM_CLEANUP_TIMEOUT_SECONDS", .02,
                        raising=False)
    second_id = "bbbbbbbb-cccc-4ddd-8eee-ffffffffffff"
    marker = {"version": 1, "source": "cancel",
              "provision_generation": VM["provision_generation"]}
    rows = {
        job_id: {"id": job_id, "status": "cancelled", "execution_lane": "pinned",
                 "context": {"vm": VM, "_job_terminal_vm_cleanup": marker}}
        for job_id in (JOB_ID, second_id)
    }
    cancelled = asyncio.Event()

    @asynccontextmanager
    async def lock(_job_id):
        yield True

    completion_calls = {}

    async def complete(job_id, *, expected_generation):
        assert expected_generation == VM["provision_generation"]
        completion_calls[job_id] = completion_calls.get(job_id, 0) + 1
        return completion_calls[job_id] > 1

    store = SimpleNamespace(
        list_terminal_vm_cleanup_jobs=AsyncMock(return_value=list(rows.values())),
        get_job=AsyncMock(side_effect=lambda job_id: rows[job_id]),
        stateless_cancel_cleanup_lock=lock,
        delete_checkpoint_thread=AsyncMock(),
        complete_terminal_vm_cleanup_marker=complete,
    )
    operation = controls(store=store)

    async def cascade(job_id):
        if job_id == JOB_ID:
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()
        return True

    operation.cascade_cancel_to_children = cascade
    assert await asyncio.wait_for(
        operation.reconcile_terminal_vm_cleanups(limit=2), .3,
    ) == 1
    assert cancelled.is_set()
    assert rows[JOB_ID]["context"]["_job_terminal_vm_cleanup"] == marker
    operation.dependencies.archive_and_cleanup_workspace.assert_awaited_once_with(
        second_id,
    )


@pytest.mark.asyncio
async def test_slow_snapshot_does_not_block_another_terminal_vm_candidate():
    second_id = "bbbbbbbb-cccc-4ddd-8eee-ffffffffffff"
    marker = {"version": 1, "source": "approve",
              "provision_generation": VM["provision_generation"]}
    rows = {
        job_id: {"id": job_id, "status": "completed", "execution_lane": "pinned",
                 "context": {"vm": VM, "_job_terminal_vm_cleanup": marker}}
        for job_id in (JOB_ID, second_id)
    }
    slow_started = asyncio.Event()
    slow_release = asyncio.Event()
    fast_done = asyncio.Event()

    @asynccontextmanager
    async def lock(_job_id):
        yield True

    async def archive(job_id):
        if job_id == JOB_ID:
            slow_started.set()
            await slow_release.wait()
        else:
            fast_done.set()

    store = SimpleNamespace(
        list_terminal_vm_cleanup_jobs=AsyncMock(return_value=list(rows.values())),
        get_job=AsyncMock(side_effect=lambda job_id: rows[job_id]),
        stateless_cancel_cleanup_lock=lock,
        complete_terminal_vm_cleanup_marker=AsyncMock(side_effect=[False, False, True, True]),
    )
    operation = controls(store=store, archive=archive)
    task = asyncio.create_task(operation.reconcile_terminal_vm_cleanups(limit=2))
    try:
        await asyncio.wait_for(slow_started.wait(), 1)
        await asyncio.wait_for(fast_done.wait(), 1)
        assert not task.done()
    finally:
        slow_release.set()
    assert await asyncio.wait_for(task, 1) == 2


@pytest.mark.asyncio
async def test_idle_sweep_does_not_wait_for_slow_terminal_cleanup(monkeypatch):
    import orchestrator.main as orch_main

    shutdown = asyncio.Event()
    started = asyncio.Event()
    cancelled = asyncio.Event()

    @asynccontextmanager
    async def acquire():
        yield SimpleNamespace(fetchval=AsyncMock(return_value=False))

    async def reconcile_session_workspaces(**_kwargs):
        async def finish_cycle():
            await started.wait()
            shutdown.set()
        asyncio.create_task(finish_cycle())

    async def slow_reconcile(*, limit):
        assert limit == 4
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    monkeypatch.setattr(orch_main, "postgres_db", SimpleNamespace(acquire=acquire))
    monkeypatch.setattr(orch_main, "reconcile_session_workspaces",
                        reconcile_session_workspaces)
    monkeypatch.setattr(orch_main, "_job_mutation_operations", lambda: SimpleNamespace(
        reconcile_terminal_vm_cleanups=slow_reconcile,
    ))

    await asyncio.wait_for(orch_main.workspace_idle_sweeper(shutdown), 1)
    assert started.is_set() and cancelled.is_set()


@pytest.mark.asyncio
async def test_terminal_vm_archive_keeps_reserved_storage_and_attests_charge(monkeypatch):
    vm = {**VM, "workspace_storage": {"uid": "reserved-storage"}}
    job = {"id": JOB_ID, "status": "completed", "context": {"vm": vm}}

    class Connection:
        async def fetchval(self, *_args):
            return False

    @asynccontextmanager
    async def acquire():
        yield Connection()

    store = SimpleNamespace(get_job=AsyncMock(return_value=job), acquire=acquire)
    identity = SimpleNamespace(provision_generation=VM["provision_generation"],
                               rootdisk_pvc_uid="22222222-3333-4444-8555-666666666666")
    provisioner = SimpleNamespace(
        lifecycle_available=True,
        capture_vm_teardown_identity=AsyncMock(return_value=identity),
        _storage_context=AsyncMock(return_value={"uid": "reserved-storage"}),
        release_vm_captured=AsyncMock(return_value=SimpleNamespace(disposition="completed")),
    )
    permit = SimpleNamespace(allowed=True)
    acquire_permit = AsyncMock(return_value=permit)
    complete = AsyncMock()
    monkeypatch.setattr(thread_retirement, "acquire_vm_cleanup_permit", acquire_permit)
    monkeypatch.setattr(thread_retirement, "complete_vm_cleanup_permit", complete)
    monkeypatch.setattr(thread_retirement, "completed_cleanup_outcome", lambda _: None)
    monkeypatch.setattr(thread_retirement, "vm_cleanup_kwargs", lambda _: {})
    from orchestrator.services.vm_workspace_policy import vm_needs_release

    actions = await thread_retirement.archive_and_cleanup_workspace(
        JOB_ID, dependencies=SimpleNamespace(
            store=store, container_provisioner=None, vm_provisioner=provisioner,
            recovery_store=SimpleNamespace(), docker_provisioner=None,
            get_container_context=lambda _: {}, get_vm_context=lambda _: vm,
            vm_needs_release=vm_needs_release,
        ),
    )

    assert actions == ["vm released"]
    assert acquire_permit.await_args.kwargs["purge_disk"] is False
    assert provisioner.release_vm_captured.await_args.kwargs["purge_disk"] is False
    assert complete.await_args.kwargs["provisioner"] is provisioner
