"""Background claim priority, independent shutdown, and lease-loss behavior."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from agent.api import cloud_push_task, turn_executor as te
from tests.test_turn_executor import make_claim


@pytest.fixture
def executor(monkeypatch):
    monkeypatch.setattr(
        te.StatelessTurnExecutor, "_db", property(lambda _self: object())
    )
    result = te.StatelessTurnExecutor(
        pod_name="bg-pod",
        pod_uid="bg-uid",
        bg_task_enabled=True,
        worker_enabled=True,
        audit_writer=None,
    )
    result._detach_cached_session = AsyncMock()
    result._fetch_bundle = AsyncMock(return_value={"unit_kind": "bg_task"})
    return result


@pytest.mark.asyncio
@pytest.mark.parametrize("available", ["session", "worker", "background"])
async def test_background_claim_is_third(executor, monkeypatch, available):
    claim = make_claim()
    session_poll = AsyncMock(return_value=claim if available == "session" else None)
    worker_poll = AsyncMock(
        return_value=SimpleNamespace(unit=claim) if available == "worker" else None
    )
    bg_poll = AsyncMock(return_value=claim)
    monkeypatch.setattr(te, "claim_unit", session_poll)
    monkeypatch.setattr(te, "claim_worker_batch", worker_poll)
    monkeypatch.setattr(te, "claim_bg_task", bg_poll)

    async def serve(_claim):
        executor.request_stop()

    session = AsyncMock(side_effect=serve)
    worker = AsyncMock(side_effect=serve)
    background = AsyncMock(side_effect=serve)
    executor._serve_claim = session
    executor._serve_worker_claim = worker
    executor._serve_bg_task_claim = background
    await executor.run()
    assert session.await_count == (available == "session")
    assert worker.await_count == (available == "worker")
    assert background.await_count == (available == "background")
    assert bg_poll.await_count == (available == "background")


@pytest.mark.asyncio
async def test_disabled_executor_never_polls_background_tasks(executor, monkeypatch):
    executor._bg_task_enabled = False
    executor._worker_enabled = False
    monkeypatch.setattr(te, "claim_unit", AsyncMock(return_value=None))
    poll = AsyncMock()
    monkeypatch.setattr(te, "claim_bg_task", poll)
    executor._expire_warm_session = AsyncMock()

    async def stop(_delay):
        executor.request_stop()

    executor._sleep_interruptible = stop
    await executor.run()
    poll.assert_not_awaited()


@pytest.mark.asyncio
async def test_background_shutdown_releases_only_its_task(executor, monkeypatch):
    started = asyncio.Event()
    quiesced = asyncio.Event()
    events = []

    async def push(*_args, **_kwargs):
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            quiesced.set()
            events.append("quiesced")

    async def fail(*_args, **_kwargs):
        assert quiesced.is_set()
        events.append("released")

    monkeypatch.setattr(cloud_push_task, "run_adopted_cloud_push", push)
    monkeypatch.setattr(te, "fail_bg_task", AsyncMock(side_effect=fail))
    complete = AsyncMock()
    monkeypatch.setattr(te, "complete_bg_task", complete)
    executor._abort_turn_politely = Mock()
    claim = replace(make_claim(), unit_kind="bg_task")
    executor._task = asyncio.create_task(executor._serve_bg_task_claim(claim))
    await started.wait()
    await executor.stop(timeout=0)
    assert events == ["quiesced", "released"]
    assert executor._bg_claim is None
    executor._abort_turn_politely.assert_not_called()
    complete.assert_not_awaited()


@pytest.mark.asyncio
async def test_background_heartbeat_loss_cancels_writer(executor, monkeypatch):
    stopped = asyncio.Event()

    async def push(*_args, **_kwargs):
        try:
            await asyncio.Event().wait()
        finally:
            stopped.set()

    monkeypatch.setattr(cloud_push_task, "run_adopted_cloud_push", push)
    monkeypatch.setattr(te, "HEARTBEAT_INTERVAL_SECONDS", 0.001)
    monkeypatch.setattr(te, "heartbeat_unit", AsyncMock(return_value=None))
    monkeypatch.setattr(te, "fail_bg_task", AsyncMock(return_value=None))
    complete = AsyncMock()
    monkeypatch.setattr(te, "complete_bg_task", complete)
    await asyncio.wait_for(
        executor._serve_bg_task_claim(replace(make_claim(), unit_kind="bg_task")),
        timeout=1,
    )
    assert stopped.is_set()
    complete.assert_not_awaited()


@pytest.mark.asyncio
async def test_deferred_bundle_does_no_cloud_work(executor, monkeypatch):
    executor._fetch_bundle.return_value = {"deferred": True}
    push = AsyncMock()
    monkeypatch.setattr(cloud_push_task, "run_adopted_cloud_push", push)
    failure = AsyncMock()
    monkeypatch.setattr(te, "fail_bg_task", failure)
    claim = replace(make_claim(), unit_kind="bg_task")
    await executor._serve_bg_task_claim(claim)
    push.assert_not_awaited()
    assert failure.await_args.kwargs["deferred"] is True


@pytest.mark.asyncio
async def test_cloud_only_handler_resumes_files_and_checkpoints_before_ack(
    tmp_path, monkeypatch
):
    from agent.api import persistent_app as pa
    from shared import cloud_sync_generations as generations
    from tests.cloud_sync.test_push_effects import (
        _sync,
        _coordinator,
        _requirement,
        _sha,
        THREAD,
        WORKSPACE,
    )

    sync, workspace, remote = _sync(tmp_path)
    (workspace / "a.txt").write_bytes(b"alpha")
    (workspace / "b.txt").write_bytes(b"bravo")
    (remote / "a.txt").write_bytes(b"alpha")
    requirement = replace(
        _requirement({}),
        push_owner_token=9,
        push_progress={
            "planned": 2,
            "files": {
                "a.txt": {
                    "state": "uploaded",
                    "sha256": _sha(b"alpha"),
                    "size": 5,
                    "remote_etag": _sha(b"alpha"),
                }
            },
        },
    )

    @asynccontextmanager
    async def acquire():
        yield object()

    db = SimpleNamespace(acquire=acquire)
    monkeypatch.setattr(
        cloud_push_task,
        "adopt_bg_push",
        AsyncMock(return_value={"mount-a": requirement}),
    )
    monkeypatch.setattr(
        cloud_push_task, "bg_push_lease_is_current", AsyncMock(return_value=True)
    )
    monkeypatch.setattr(
        cloud_push_task, "build_push_backend", lambda *_args, **_kwargs: sync._backend
    )
    monkeypatch.setattr(
        pa, "_build_sync_coordinator", lambda **_kwargs: _coordinator(sync)
    )
    monkeypatch.setattr(pa, "_assert_cloud_generation_owner", AsyncMock())
    record = AsyncMock(return_value=True)
    monkeypatch.setattr(generations, "record_push_progress", record)

    async def ack(*_args):
        assert record.await_count == 1
        assert list(record.await_args.kwargs["files"]) == ["b.txt"]
        assert (remote / "b.txt").read_bytes() == b"bravo"

    acknowledge = AsyncMock(side_effect=ack)
    monkeypatch.setattr(pa, "_ack_cloud_generation", acknowledge)
    attach = AsyncMock(
        side_effect=AssertionError("background work cannot attach a session")
    )
    monkeypatch.setattr(pa._session_attach, "attach", attach)
    abandon = AsyncMock()
    monkeypatch.setattr(cloud_push_task, "abandon_bg_push", abandon)
    await cloud_push_task.run_adopted_cloud_push(
        db,
        replace(make_claim(), unit_kind="bg_task"),
        {
            "unit_kind": "bg_task",
            "task_kind": "cloud_push",
            "thread_id": THREAD,
            "workspace": {"backend": "virtual", "workspace_generation": WORKSPACE},
            "cloud_sync": {},
        },
        pod_name="bg-pod",
        pod_uid="bg-uid",
    )
    acknowledge.assert_awaited_once()
    attach.assert_not_awaited()
    abandon.assert_not_awaited()
    assert [
        path for path, *_ in sync.conditional_writes if not path.startswith(".srw/")
    ] == ["b.txt"]
