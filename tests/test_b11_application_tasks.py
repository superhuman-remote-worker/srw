"""R1.B11: the per-lifecycle background task set.

``run_when_leader`` itself is covered by the leader-election suites; here the
set is checked for what it owns: start-once, the lifecycle's own shutdown
event reaching gated loops, and the shutdown await order.
"""

from __future__ import annotations

import asyncio
import gc
import warnings

import pytest

from orchestrator.services import application_tasks, leader_election


@pytest.mark.asyncio
async def test_start_runs_the_coroutine_once_per_key():
    event = asyncio.Event()
    tasks = application_tasks.ApplicationTaskSet(event)
    ran: list[str] = []

    async def body(label: str) -> None:
        ran.append(label)

    task = tasks.start("alpha", body("alpha"), name="alpha-task")
    duplicate = body("again")
    with pytest.raises(RuntimeError, match="already started"):
        tasks.start("alpha", duplicate)
    await task
    assert ran == ["alpha"]
    assert task.get_name() == "alpha-task"
    assert tasks.keys == ("alpha",)
    # The refused coroutine was closed, not leaked un-awaited.
    with warnings.catch_warnings():
        warnings.simplefilter("error", RuntimeWarning)
        gc.collect()


@pytest.mark.asyncio
async def test_leader_gated_start_hands_the_lifecycle_event_to_run_when_leader(
    monkeypatch,
):
    seen: list[tuple[object, asyncio.Event]] = []

    async def fake_run_when_leader(make_coro, shutdown_event):
        seen.append((make_coro, shutdown_event))

    monkeypatch.setattr(leader_election, "run_when_leader", fake_run_when_leader)
    event = asyncio.Event()
    tasks = application_tasks.ApplicationTaskSet(event)

    async def loop(_stop):
        return None

    await tasks.start_leader_gated("gated", loop, name="gated-task")
    assert seen == [(loop, event)]


@pytest.mark.asyncio
async def test_a_gated_loop_does_not_run_without_leadership():
    leader_election.is_leader.clear()
    event = asyncio.Event()
    tasks = application_tasks.ApplicationTaskSet(event)
    ran = asyncio.Event()

    async def loop(_stop):
        ran.set()

    tasks.start_leader_gated("gated", loop)
    await asyncio.sleep(0.05)
    assert not ran.is_set()
    await asyncio.wait_for(tasks.stop(["gated"]), timeout=5)
    assert not ran.is_set()


@pytest.mark.asyncio
async def test_stop_sets_the_event_and_awaits_every_started_task():
    event = asyncio.Event()
    tasks = application_tasks.ApplicationTaskSet(event)
    finished: list[str] = []

    async def until_stopped(label: str, delay: float) -> None:
        await event.wait()
        await asyncio.sleep(delay)
        finished.append(label)

    tasks.start("a", until_stopped("a", 0.03))
    tasks.start("b", until_stopped("b", 0.0))
    tasks.start("c", until_stopped("c", 0.01))

    started = list(tasks._tasks.values())
    # "missing" was never started (a feature-gated task): it is skipped. The
    # exact await order is pinned by the lifespan characterization, where
    # stub tasks make it observable.
    await tasks.stop(["c", "missing", "a"])
    assert event.is_set()
    assert set(finished) == {"a", "b", "c"}
    assert all(task.done() for task in started)


@pytest.mark.asyncio
async def test_an_unnamed_started_task_is_still_awaited_after_the_named_ones():
    event = asyncio.Event()
    tasks = application_tasks.ApplicationTaskSet(event)
    order: list[str] = []

    async def mark(label: str) -> None:
        await event.wait()
        order.append(label)

    tasks.start("named", mark("named"))
    tasks.start("forgotten", mark("forgotten"))
    await tasks.stop(["named"])
    assert sorted(order) == ["forgotten", "named"]


@pytest.mark.asyncio
async def test_a_failed_task_is_returned_after_every_task_was_awaited(caplog):
    event = asyncio.Event()
    tasks = application_tasks.ApplicationTaskSet(event)
    finished: list[str] = []

    async def broken(label: str) -> None:
        raise ValueError(f"bad interval {label}")

    async def fine() -> None:
        await event.wait()
        await asyncio.sleep(0.01)
        finished.append("fine")

    tasks.start("broken", broken("one"))
    tasks.start("also_broken", broken("two"))
    tasks.start("fine", fine())
    await asyncio.sleep(0)
    caplog.set_level("ERROR")
    failure = await tasks.stop(["broken", "also_broken", "fine"])
    assert isinstance(failure, ValueError) and str(failure) == "bad interval one"
    assert finished == ["fine"]
    assert caplog.text.count("ended with an error; shutdown continues") == 2


@pytest.mark.asyncio
async def test_stop_never_absorbs_cancellation():
    event = asyncio.Event()
    tasks = application_tasks.ApplicationTaskSet(event)
    tasks.start("forever", asyncio.Event().wait())
    stopper = asyncio.create_task(tasks.stop(["forever"]))
    await asyncio.sleep(0.01)
    stopper.cancel()
    with pytest.raises(asyncio.CancelledError):
        await stopper
    tasks._tasks["forever"].cancel()
