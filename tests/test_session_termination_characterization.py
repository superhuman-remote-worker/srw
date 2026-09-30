"""Termination task/authority characterization; adapter only changes on extraction."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from agent.api import persistent_app as pa


_NAMES = {
    "_reset_retirement_admission_mirror": "reset_retirement_admission_mirror",
    "_track_session_side_task": "track_session_side_task",
    "_quiesce_session_side_tasks": "quiesce_session_side_tasks",
    "_terminal_retirement_disposition": "terminal_retirement_disposition",
    "_termination_admission_closed": "termination_admission_closed",
    "_retirement_admission_closed": "retirement_admission_closed",
    "activate_termination_admission_fence": "activate_termination_admission_fence",
    "_termination_quiescent": "termination_quiescent",
    "_wait_for_termination_quiescence": "wait_for_termination_quiescence",
    "_handle_heartbeat_intents": "handle_heartbeat_intents",
    "_session_parked": "session_parked",
    "_drain_suspend_session": "drain_suspend_session",
    "_start_pending_exact_drain_suspend_retry": "start_pending_exact_drain_suspend_retry",
    "_retry_pending_exact_drain_suspend": "retry_pending_exact_drain_suspend",
    "_deregister_before_exit": "deregister_before_exit",
    "_schedule_exit": "schedule_exit",
    "_boot_ws_watchdog": "boot_ws_watchdog",
    "_thread_status_watchdog": "thread_status_watchdog",
    "_start_watchdogs": "start_watchdogs",
    "_stop_watchdogs": "stop_watchdogs",
    "_stop_and_join_watchdogs": "stop_and_join_watchdogs",
    "_signal_ws_connected": "signal_ws_connected",
    "_exit_workspace_not_ready": "exit_workspace_not_ready",
    "_exit_grant_denied": "exit_grant_denied",
    "_exit_memory_unavailable": "exit_memory_unavailable",
    "_exit_duplicate_provision": "exit_duplicate_provision",
    "_exit_session_ended": "exit_session_ended",
    "_terminate_session": "terminate",
    "_dedicated_pod_owes_exit": "dedicated_pod_owes_exit",
    "_request_vm_retirement_actuator": "request_vm_retirement_actuator",
    "_settle_exact_retirement_after_quiescence": "settle_exact_retirement_after_quiescence",
    "_terminate_session_inner": "_terminate_inner",
    "_detach_session": "detach_session",
    "_loop_completion_handler": "loop_completion_handler",
    "_reconcile_retirement_begin_or_reopen_controls": "reconcile_retirement_begin_or_reopen_controls",
    "_begin_exact_session_retirement": "begin_retirement",
    "_runtime_admission_closed": "runtime_admission_closed",
    "_retirement_admission_identity": "retirement_admission_identity",
    "_retirement_admission_disposition": "retirement_admission_disposition",
    "_retirement_admission_token": "retirement_admission_token",
    "_retirement_admission_permanent": "retirement_admission_permanent",
    "_pending_drain_suspend": "pending_drain_suspend",
    "_pending_drain_suspend_retry_task": "pending_drain_suspend_retry_task",
    "_pending_exit_task": "pending_exit_task",
    "_drain_intent_handled": "drain_intent_handled",
    "_drain_deferred_logged": "drain_deferred_logged",
    "_termination_admission_fenced": "termination_admission_fenced",
    "_termination_fence_reason": "termination_fence_reason",
    "_ws_connected_event": "ws_connected_event",
    "_watchdog_tasks": "watchdog_tasks",
    "_terminating": "terminating",
    "_termination_task": "termination_task",
    "_sessions_served": "sessions_served",
    "_max_sessions_per_process": "max_sessions_per_process",
    "_session_side_tasks": "session_side_tasks",
    "_TERMINATION_SENTINEL_PATH": "termination_sentinel_path",
    "_TERMINATION_QUEUE_SENTINEL": "termination_queue_sentinel",
    "_session_boot_ws_timeout_s": "session_boot_ws_timeout_s",
    "_thread_status_poll_s": "thread_status_poll_s",
}


_NAMES = {'_reset_retirement_admission_mirror': 'reset_retirement_admission_mirror', '_track_session_side_task': 'track_session_side_task', '_quiesce_session_side_tasks': 'quiesce_session_side_tasks', '_terminal_retirement_disposition': 'terminal_retirement_disposition', '_termination_admission_closed': 'termination_admission_closed', '_retirement_admission_closed': 'retirement_admission_closed', 'activate_termination_admission_fence': 'activate_termination_admission_fence', '_termination_quiescent': 'termination_quiescent', '_wait_for_termination_quiescence': 'wait_for_termination_quiescence', '_handle_heartbeat_intents': 'handle_heartbeat_intents', '_session_parked': 'session_parked', '_drain_suspend_session': 'drain_suspend_session', '_start_pending_exact_drain_suspend_retry': 'start_pending_exact_drain_suspend_retry', '_retry_pending_exact_drain_suspend': 'retry_pending_exact_drain_suspend', '_deregister_before_exit': 'deregister_before_exit', '_schedule_exit': 'schedule_exit', '_boot_ws_watchdog': 'boot_ws_watchdog', '_thread_status_watchdog': 'thread_status_watchdog', '_start_watchdogs': 'start_watchdogs', '_stop_watchdogs': 'stop_watchdogs', '_stop_and_join_watchdogs': 'stop_and_join_watchdogs', '_signal_ws_connected': 'signal_ws_connected', '_exit_workspace_not_ready': 'exit_workspace_not_ready', '_exit_grant_denied': 'exit_grant_denied', '_exit_memory_unavailable': 'exit_memory_unavailable', '_exit_duplicate_provision': 'exit_duplicate_provision', '_exit_session_ended': 'exit_session_ended', '_terminate_session': 'terminate', '_dedicated_pod_owes_exit': 'dedicated_pod_owes_exit', '_request_vm_retirement_actuator': 'request_vm_retirement_actuator', '_settle_exact_retirement_after_quiescence': 'settle_exact_retirement_after_quiescence', '_terminate_session_inner': '_terminate_inner', '_detach_session': 'detach_session', '_loop_completion_handler': 'loop_completion_handler', '_reconcile_retirement_begin_or_reopen_controls': 'reconcile_retirement_begin_or_reopen_controls', '_begin_exact_session_retirement': 'begin_retirement', '_runtime_admission_closed': 'runtime_admission_closed', '_retirement_admission_identity': 'retirement_admission_identity', '_retirement_admission_disposition': 'retirement_admission_disposition', '_retirement_admission_token': 'retirement_admission_token', '_retirement_admission_permanent': 'retirement_admission_permanent', '_pending_drain_suspend': 'pending_drain_suspend', '_pending_drain_suspend_retry_task': 'pending_drain_suspend_retry_task', '_pending_exit_task': 'pending_exit_task', '_drain_intent_handled': 'drain_intent_handled', '_drain_deferred_logged': 'drain_deferred_logged', '_termination_admission_fenced': 'termination_admission_fenced', '_termination_fence_reason': 'termination_fence_reason', '_ws_connected_event': 'ws_connected_event', '_watchdog_tasks': 'watchdog_tasks', '_terminating': 'terminating', '_termination_task': 'termination_task', '_sessions_served': 'sessions_served', '_max_sessions_per_process': 'max_sessions_per_process', '_session_side_tasks': 'session_side_tasks', '_TERMINATION_SENTINEL_PATH': 'termination_sentinel_path', '_TERMINATION_QUEUE_SENTINEL': 'termination_queue_sentinel', '_session_boot_ws_timeout_s': 'session_boot_ws_timeout_s', '_thread_status_poll_s': 'thread_status_poll_s'}

def runtime():
    return pa._session_termination


def name(value):
    return _NAMES[value]


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
