"""A drift-drain intent must suspend an adopted session, not dead-letter.

Regression guard for the stale-Centurion-pod incident of 2026-07-30: the
lifecycle reconciler set ``intents.should_drain`` on a dual-mode pool pod
hosting an adopted officer session, and dual_app's handler — which had only
worker semantics — logged "will freeze at next phase boundary" and did
nothing. Sessions have no phase boundaries, so the pod survived every image
roll and served a full day on a stale build.

The session leg (June's clean drain-suspend, built in persistent_app) is
delegated, not re-implemented — see
knowledge-base/knowledge/issues/dual_app_persistent_app_redundancy.md for why that distinction
is load-bearing. These tests drive ``_handle_heartbeat_intents``, the
callback actually registered with the heartbeat loop.
"""

from unittest.mock import AsyncMock, patch

import pytest

import agent.api.dual_app as dual_app
import agent.api.persistent_app as pa


def _drain_response():
    return {
        "status": "ok",
        "intents": {"should_drain": True, "drain_reason": "stale_image"},
    }


@pytest.fixture(autouse=True)
def _restore_globals():
    """Snapshot/restore the module globals these tests mutate, then set a
    clean SESSION baseline (the incident shape: adopted session, no job)."""
    dual_names = (
        "_pod_state",
        "_current_job_id",
        "_drain_intent_received",
        "_drain_intent_handled",
        "_drain_deferred_logged",
    )
    saved_dual = {name: getattr(dual_app, name) for name in dual_names}
    saved_session = pa._session

    dual_app._pod_state = dual_app.PodState.SESSION
    dual_app._current_job_id = None
    dual_app._drain_intent_received = False
    dual_app._drain_intent_handled = False
    dual_app._drain_deferred_logged = False
    pa._session = object()  # attached; individual tests override
    yield
    for name, val in saved_dual.items():
        setattr(dual_app, name, val)
    pa._session = saved_session


class TestParkedSessionSuspends:
    @pytest.mark.asyncio
    async def test_parked_session_drain_suspends(self):
        suspend = AsyncMock()
        with (
            patch.object(pa._session_termination, "session_parked", return_value=True),
            patch.object(pa._session_termination, "drain_suspend_session", suspend),
        ):
            await dual_app._handle_heartbeat_intents(_drain_response())
        suspend.assert_awaited_once()
        assert dual_app._drain_intent_handled
        assert dual_app._drain_intent_received

    @pytest.mark.asyncio
    async def test_handled_once_no_reentry(self):
        """A retried heartbeat after the suspend must not suspend twice."""
        suspend = AsyncMock()
        with (
            patch.object(pa._session_termination, "session_parked", return_value=True),
            patch.object(pa._session_termination, "drain_suspend_session", suspend),
        ):
            await dual_app._handle_heartbeat_intents(_drain_response())
            await dual_app._handle_heartbeat_intents(_drain_response())
        suspend.assert_awaited_once()


class TestDefersWhileNotParked:
    @pytest.mark.asyncio
    async def test_turn_in_flight_defers_then_lands(self):
        """The incident's dead-letter, corrected: deferred heartbeats keep
        re-checking, and the first parked tick suspends."""
        suspend = AsyncMock()
        parked = False
        with (
            patch.object(
                pa._session_termination, "session_parked", side_effect=lambda: parked
            ),
            patch.object(pa._session_termination, "drain_suspend_session", suspend),
        ):
            await dual_app._handle_heartbeat_intents(_drain_response())
            await dual_app._handle_heartbeat_intents(_drain_response())
            suspend.assert_not_awaited()
            assert not dual_app._drain_intent_handled
            assert dual_app._drain_intent_received

            parked = True
            await dual_app._handle_heartbeat_intents(_drain_response())
        suspend.assert_awaited_once()
        assert dual_app._drain_intent_handled

    @pytest.mark.asyncio
    async def test_attach_window_defers(self):
        """/session/attach flips the pod state before the backgrounded setup
        creates pa._session — a drain landing in that window must not act."""
        pa._session = None
        suspend = AsyncMock()
        with patch.object(pa._session_termination, "drain_suspend_session", suspend):
            await dual_app._handle_heartbeat_intents(_drain_response())
        suspend.assert_not_awaited()
        assert not dual_app._drain_intent_handled

    @pytest.mark.asyncio
    async def test_deferral_logs_once(self):
        with (
            patch.object(pa._session_termination, "session_parked", return_value=False),
            patch.object(dual_app, "logger") as log,
        ):
            await dual_app._handle_heartbeat_intents(_drain_response())
            await dual_app._handle_heartbeat_intents(_drain_response())
        drain_logs = [c for c in log.info.call_args_list if "Drain intent" in c.args[0]]
        assert len(drain_logs) == 1


class TestOtherPodStatesUntouched:
    @pytest.mark.asyncio
    async def test_busy_worker_still_defers_to_phase_boundary(self):
        dual_app._pod_state = dual_app.PodState.WORKING
        dual_app._current_job_id = "job-under-test"
        suspend = AsyncMock()
        with patch.object(pa._session_termination, "drain_suspend_session", suspend):
            await dual_app._handle_heartbeat_intents(_drain_response())
        suspend.assert_not_awaited()
        assert dual_app._drain_intent_received
        assert dual_app._drain_intent_handled

    @pytest.mark.asyncio
    async def test_idle_worker_still_exits(self):
        dual_app._pod_state = dual_app.PodState.IDLE
        with patch("agent.api.dual_app.os._exit") as fake_exit:
            await dual_app._handle_heartbeat_intents(_drain_response())
        fake_exit.assert_called_once_with(0)

    @pytest.mark.asyncio
    async def test_no_intent_is_a_no_op(self):
        suspend = AsyncMock()
        with patch.object(pa._session_termination, "drain_suspend_session", suspend):
            await dual_app._handle_heartbeat_intents({"status": "ok"})
            await dual_app._handle_heartbeat_intents({"status": "ok", "intents": {}})
        suspend.assert_not_awaited()
        assert not dual_app._drain_intent_received
