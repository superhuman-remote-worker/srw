"""Actual-source ownership and independent termination instances."""

import ast
import asyncio
import dataclasses
import logging
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from agent.api import persistent_app, session_termination
from agent.api.session_termination import (
    SessionTerminationCoordinator,
    SessionTerminationPorts,
)


def owner(*, session=None, officer=None, stateless=False):
    identity = SimpleNamespace(
        thread_id="captured",
        session_generation="generation",
        attach_token="attach",
        agent_id="agent",
        pod_uid="pod",
        runtime_contract=False,
        retirement_identity=lambda: ("captured", None, None),
    )
    identity.snapshot = lambda: identity
    values = {
        field.name: lambda: None
        for field in dataclasses.fields(SessionTerminationPorts)
    }
    values.update(
        session=lambda: session,
        identity=lambda: identity,
        orchestrator_client=lambda: SimpleNamespace(dispatch_process_generation="process"),
        officer_config=lambda: officer,
        stateless_mode=lambda: stateless,
        loop_task=lambda: None,
        session_type=SimpleNamespace,
        idle_timeout_error=TimeoutError,
    )
    return SessionTerminationCoordinator(
        SessionTerminationPorts(**values),
        logger=logging.getLogger(__name__),
        termination_queue_sentinel=object(),
    )


def test_factory_composes_one_termination_owner_and_keeps_no_state_copy():
    tree = ast.parse(Path(persistent_app.__file__).read_text())
    instances = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and getattr(node.func, "id", None) == "SessionTerminationCoordinator"
    ]
    assert len(instances) == 1
    retired = {
        "_termination_task",
        "_terminating",
        "_retirement_admission_identity",
        "_retirement_admission_token",
        "_retirement_admission_disposition",
        "_retirement_admission_permanent",
        "_pending_drain_suspend",
        "_watchdog_tasks",
        "_session_side_tasks",
        "_pending_exit_task",
    }
    assert (
        not {node.id for node in ast.walk(tree) if isinstance(node, ast.Name)} & retired
    )
    assert not {
        node.name
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    } & {
        "_terminate_session",
        "_terminate_session_inner",
        "_begin_exact_session_retirement",
        "_drain_suspend_session",
    }


def test_owner_has_no_module_globals_or_dynamic_runtime_lookup():
    tree = ast.parse(Path(session_termination.__file__).read_text())
    for node in ast.walk(tree):
        assert not isinstance(node, ast.Global)
        if isinstance(node, ast.Call):
            assert getattr(node.func, "id", "") != "__import__"
            assert getattr(node.func, "attr", "") != "import_module"
        if isinstance(node, ast.Attribute):
            assert not (
                getattr(node.value, "id", "") == "sys" and node.attr == "modules"
            )


@pytest.mark.asyncio
async def test_independent_owners_share_no_task_mirror_or_side_task_set():
    first, second = owner(session=object()), owner(session=object())
    assert first.session_side_tasks is not second.session_side_tasks
    assert first.watchdog_tasks is not second.watchdog_tasks
    first.retirement_admission_identity = ("captured", None, None)
    assert first.retirement_admission_closed()
    assert not second.retirement_admission_closed()
    entered, release = asyncio.Event(), asyncio.Event()

    async def cleanup(*args, **kwargs):
        entered.set()
        await release.wait()
        return "settled"

    first._terminate_inner = AsyncMock(side_effect=cleanup)
    task = asyncio.create_task(first.terminate("End"))
    await entered.wait()
    assert first.termination_task is not None
    assert second.termination_task is None
    release.set()
    assert await task == "settled"
    assert first.termination_task is None


@pytest.mark.asyncio
async def test_side_task_cancellation_finally_is_joined_before_return():
    runtime = owner()
    entered, finalized = asyncio.Event(), asyncio.Event()

    async def side_task():
        try:
            entered.set()
            await asyncio.Event().wait()
        finally:
            await asyncio.sleep(0)
            finalized.set()

    task = runtime.track_session_side_task(asyncio.create_task(side_task()))
    await entered.wait()
    await runtime.quiesce_session_side_tasks()
    assert finalized.is_set()
    assert task.done()
    assert not runtime.session_side_tasks


@pytest.mark.asyncio
async def test_native_first_use_keeps_same_life_ready_without_ws_or_input():
    """A native channel is first use for the existing boot watchdog."""
    runtime = owner(session=object())
    runtime.session_boot_ws_timeout_s = 0.01
    terminated = []

    async def terminate(reason):
        terminated.append(reason)

    runtime.terminate = terminate
    async def wait_forever(_poll):
        await asyncio.Event().wait()

    runtime.thread_status_watchdog = wait_forever
    runtime.start_watchdogs()
    assert runtime.note_native_first_use(("captured", "generation", "agent", "attach", "pod", "process")) == "accepted"
    runtime.start_watchdogs()  # A same-life restart must retain the accepted use.
    await asyncio.sleep(0.03)
    assert terminated == []
    await runtime.stop_and_join_watchdogs()


def test_native_notice_does_not_latch_no_watchdog_officer_or_stateless_life():
    life = ("captured", "generation", "agent", "attach", "pod", "process")
    for runtime in (owner(session=object(), officer=object()),
                    owner(session=object(), stateless=True)):
        runtime.ws_connected_event = asyncio.Event()
        assert runtime.note_native_first_use(life) is None
        assert not runtime.ws_connected_event.is_set()
