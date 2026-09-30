"""Termination task/authority characterization; adapter only changes on extraction."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from agent.api import persistent_app as pa


def runtime():
    return pa


def name(value):
    return value


@pytest.fixture
def termination(monkeypatch):
    owner = runtime()
    monkeypatch.setattr(owner, name("_termination_task"), None)
    monkeypatch.setattr(owner, name("_terminating"), False)
    monkeypatch.setattr(pa, "_session", SimpleNamespace())
    monkeypatch.setattr(pa, "_loop_task", None)
    monkeypatch.setattr(pa._session_identity, "_thread_id", "captured-thread")
    monkeypatch.setattr(pa._session_identity, "_runtime_contract", False)
    monkeypatch.setenv("SESSION_BOUND_THREAD_ID", "")
    return owner


@pytest.mark.asyncio
async def test_independent_termination_callers_join_one_exact_task(
    termination, monkeypatch
):
    entered, release = asyncio.Event(), asyncio.Event()

    async def cleanup(*args, **kwargs):
        entered.set()
        await release.wait()
        return "settled"

    body = AsyncMock(side_effect=cleanup)
    monkeypatch.setattr(termination, name("_terminate_session_inner"), body)
    first = asyncio.create_task(getattr(termination, name("_terminate_session"))("End"))
    await entered.wait()
    exact = getattr(termination, name("_termination_task"))
    second = asyncio.create_task(
        getattr(termination, name("_terminate_session"))("detach")
    )
    await asyncio.sleep(0)
    assert getattr(termination, name("_termination_task")) is exact
    assert not second.done()
    release.set()
    assert await first == await second == "settled"
    body.assert_awaited_once()
    assert getattr(termination, name("_termination_task")) is None
    assert not getattr(termination, name("_terminating"))


@pytest.mark.asyncio
async def test_cancelled_caller_keeps_the_exact_termination_owner(
    termination, monkeypatch
):
    entered, release = asyncio.Event(), asyncio.Event()

    async def cleanup(*args, **kwargs):
        entered.set()
        await release.wait()
        return "settled"

    body = AsyncMock(side_effect=cleanup)
    monkeypatch.setattr(termination, name("_terminate_session_inner"), body)
    caller = asyncio.create_task(
        getattr(termination, name("_terminate_session"))("End")
    )
    await entered.wait()
    exact = getattr(termination, name("_termination_task"))
    caller.cancel()
    with pytest.raises(asyncio.CancelledError):
        await caller
    assert not exact.done()
    waiter = asyncio.create_task(
        getattr(termination, name("_terminate_session"))("shutdown")
    )
    release.set()
    assert await waiter == "settled"
    body.assert_awaited_once()


@pytest.mark.asyncio
async def test_loop_reentry_does_not_join_the_owner_joining_it(
    termination, monkeypatch
):
    reenter = asyncio.Event()

    async def loop():
        await reenter.wait()
        return await getattr(termination, name("_terminate_session"))("loop_complete")

    loop_task = asyncio.create_task(loop())
    monkeypatch.setattr(pa, "_loop_task", loop_task)

    async def cleanup(*args, **kwargs):
        reenter.set()
        assert await asyncio.wait_for(loop_task, 1) is None
        return "settled"

    body = AsyncMock(side_effect=cleanup)
    monkeypatch.setattr(termination, name("_terminate_session_inner"), body)
    assert (
        await asyncio.wait_for(
            getattr(termination, name("_terminate_session"))("drain"), 2
        )
        == "settled"
    )
    body.assert_awaited_once()


@pytest.mark.asyncio
async def test_begin_refusal_precedes_destructive_effects(termination, monkeypatch):
    from agent.api.session_contract import EventJournalUnavailable

    monkeypatch.setenv("STATELESS_EXECUTOR", "0")
    begin = AsyncMock(return_value=False)
    monkeypatch.setattr(termination, name("_begin_exact_session_retirement"), begin)
    cleanup = AsyncMock()
    monkeypatch.setattr(pa, "_session", SimpleNamespace(cleanup=cleanup))
    with pytest.raises(EventJournalUnavailable):
        await getattr(termination, name("_terminate_session_inner"))("End")
    begin.assert_awaited_once()
    cleanup.assert_not_awaited()
    assert pa._session is not None


def test_retirement_mirror_refuses_a_successor(termination, monkeypatch):
    exact = pa._session_identity.retirement_identity()
    monkeypatch.setattr(termination, name("_retirement_admission_identity"), exact)
    assert getattr(termination, name("_retirement_admission_closed"))()
    monkeypatch.setattr(pa._session_identity, "_thread_id", "successor")
    assert not getattr(termination, name("_retirement_admission_closed"))()
