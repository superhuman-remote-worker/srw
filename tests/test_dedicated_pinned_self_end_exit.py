"""A dedicated pinned Pod exits after it settles its own End.

Agent-initiated End (idle timeout, the socket ``archive`` verb, the loop's own
completion or crash) settles the exact pinned retirement from inside the Pod.
The orchestrator's settlement deliberately leaves the Pod alone ("the process
exits itself"): the Pod's own HTTP request is still waiting for the response,
and historical claimant retirement only ever acts on a terminal Pod. A
dedicated Pod (``--thread-id`` / ``SESSION_BOUND_THREAD_ID``) can serve no
other thread, so once its bound thread has settled it must exit. Pool and dual
Pods (no bound thread) must not: the pool owns their lifecycle. An unproven
settlement must never exit either — the fenced process is the only local
retry owner — and neither may an End handed to the VM retirement actuator,
which is still pending.

See knowledge-base/knowledge/issues/agent_initiated_pinned_end_wedges_permanent_delete.md.
"""

from __future__ import annotations

from agent.api import session_termination
import asyncio
import contextlib
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from agent.api import persistent_app as pa
from agent.persistent_graph import IdleTimeoutError

THREAD = "57513614-b6b5-4b32-96e0-5bfecd740fcf"
OTHER_THREAD = "11111111-1111-4111-8111-111111111111"
GENERATION = "88888888-8888-4888-8888-888888888888"
ATTACH = "99999999-9999-4999-8999-999999999999"
SELF_END_REASONS = ["archive", "idle_timeout", "loop_complete", "loop_crash"]
WATCHDOG_REASONS = [
    "boot_ws_timeout",
    "thread_ended_oob",
    "thread_retirement_authorized",
]


@contextlib.contextmanager
def _attached_runtime(*, inner, bound_thread: str | None, exit_fn=None):
    """One attached pinned runtime whose inner teardown is ``inner``."""

    with contextlib.ExitStack() as stack:
        stack.enter_context(
            patch.dict("os.environ", {"SESSION_BOUND_THREAD_ID": bound_thread or ""})
        )
        stack.enter_context(patch.object(pa, "_session", MagicMock()))
        stack.enter_context(patch.object(pa._session_identity, "_thread_id", THREAD))
        stack.enter_context(
            patch.object(pa._session_identity, "_session_generation", GENERATION)
        )
        stack.enter_context(patch.object(pa._session_identity, "_attach_token", ATTACH))
        stack.enter_context(
            patch.object(pa._session_termination, "termination_task", None)
        )
        stack.enter_context(patch.object(pa._session_termination, "terminating", False))
        stack.enter_context(patch.object(pa, "_stateless_mode", return_value=False))
        stack.enter_context(
            patch.object(pa._session_termination, "_terminate_inner", inner)
        )
        stack.enter_context(
            patch.object(
                session_termination, "_EXACT_RETIREMENT_SETTLEMENT_RETRY_DELAYS", (0.0,)
            )
        )
        yield stack.enter_context(
            patch.object(pa._session_termination, "schedule_exit", side_effect=exit_fn)
        )


async def _terminate(reason: str, *, bound_thread: str | None, **kwargs):
    inner = AsyncMock(return_value=None)
    with _attached_runtime(inner=inner, bound_thread=bound_thread) as exit_fn:
        await pa._session_termination.terminate(reason, **kwargs)
    inner.assert_awaited_once()
    assert inner.await_args.args == (reason,)
    return exit_fn


@pytest.mark.asyncio
@pytest.mark.parametrize("reason", SELF_END_REASONS)
async def test_dedicated_pod_exits_after_its_own_settled_end(reason):
    exit_fn = await _terminate(reason, bound_thread=THREAD)

    exit_fn.assert_called_once_with(delay=1.0)


@pytest.mark.asyncio
async def test_exit_is_scheduled_only_after_settlement_returns():
    order: list[str] = []

    async def settle(*_args, **_kwargs):
        order.append("settled")

    with _attached_runtime(
        inner=AsyncMock(side_effect=settle),
        bound_thread=THREAD,
        exit_fn=lambda **_: order.append("exit"),
    ):
        await pa._session_termination.terminate("archive")

    assert order == ["settled", "exit"]


@pytest.mark.asyncio
@pytest.mark.parametrize("reason", SELF_END_REASONS)
async def test_pool_or_dual_attached_session_does_not_exit(reason):
    """No bound thread: the pool owns this Pod; End never exits it here."""

    exit_fn = await _terminate(reason, bound_thread=None)

    exit_fn.assert_not_called()


@pytest.mark.asyncio
async def test_pod_bound_to_another_thread_does_not_exit():
    exit_fn = await _terminate("archive", bound_thread=OTHER_THREAD)

    exit_fn.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("reason", SELF_END_REASONS)
async def test_unproven_settlement_never_schedules_exit(reason):
    inner = AsyncMock(side_effect=pa.EventJournalUnavailable("not settled"))

    with _attached_runtime(inner=inner, bound_thread=THREAD) as exit_fn:
        with pytest.raises(pa.EventJournalUnavailable):
            await pa._session_termination.terminate(reason)

    exit_fn.assert_not_called()


@pytest.mark.asyncio
async def test_unmarked_termination_does_not_exit():
    """Drain-suspend and stateless drops keep status authority elsewhere."""

    exit_fn = await _terminate("idle_timeout", bound_thread=THREAD, mark_thread=False)

    exit_fn.assert_not_called()


@pytest.mark.asyncio
async def test_stateless_executor_never_exits_on_a_session_end():
    """A stateless executor serves many claims; its teardown never exits it."""

    inner = AsyncMock(return_value=None)
    with _attached_runtime(inner=inner, bound_thread=THREAD) as exit_fn:
        with patch.object(pa, "_stateless_mode", return_value=True):
            await pa._session_termination.terminate("idle_timeout")

    exit_fn.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("reason", ["shutdown", "rest_detach", "legacy", "drain"])
async def test_orchestrator_or_process_driven_teardown_keeps_its_own_exit_owner(
    reason,
):
    exit_fn = await _terminate(reason, bound_thread=THREAD)

    exit_fn.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("bound_thread", [THREAD, None])
@pytest.mark.parametrize("reason", WATCHDOG_REASONS)
async def test_watchdog_reasons_keep_exiting_exactly_once(reason, bound_thread):
    exit_fn = await _terminate(reason, bound_thread=bound_thread)

    exit_fn.assert_called_once_with(delay=1.0)


@pytest.mark.asyncio
@pytest.mark.parametrize("bound_thread", [THREAD, None])
@pytest.mark.parametrize("reason", SELF_END_REASONS + WATCHDOG_REASONS)
async def test_pending_vm_actuator_handoff_never_exits(reason, bound_thread):
    """A VM End handed to the retirement actuator has not settled yet.

    Neither the dedicated self-End exit nor a watchdog's exit may run: the
    Pod stays until the actuator settles the End it was handed.
    """

    inner = AsyncMock(return_value="actuator_requested")
    with _attached_runtime(inner=inner, bound_thread=bound_thread) as exit_fn:
        result = await pa._session_termination.terminate(reason)

    assert result == "actuator_requested"
    inner.assert_awaited_once()
    exit_fn.assert_not_called()


@pytest.mark.asyncio
async def test_idle_timeout_completion_handler_exits_dedicated_pod():
    """The real idle path: loop raises IdleTimeoutError → archive → End."""

    async def idle_loop():
        raise IdleTimeoutError("idle")

    loop_task = asyncio.create_task(idle_loop())
    inner = AsyncMock(return_value=None)
    with _attached_runtime(inner=inner, bound_thread=THREAD) as exit_fn:
        with patch.object(pa, "_handle_idle_archive", new=AsyncMock()) as archive:
            await pa._session_termination.loop_completion_handler(loop_task)

    archive.assert_awaited_once()
    assert inner.await_args.args == ("idle_timeout",)
    exit_fn.assert_called_once_with(delay=1.0)
