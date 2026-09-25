"""Attach-vs-cloud-provisioning gate on session resume.

``POST /api/persistent/threads/{id}/resume`` schedules session-folder
provisioning fire-and-forget so the endpoint stays fast, then binds an agent.
The agent reads its cloud config within ~150ms of attach and NEVER re-reads —
so an attach that beats provisioning leaves the session with
``workspace_sync = None`` for its whole life. Provisioning needs several WebDAV
round-trips (~5s measured) against ~90ms of DB work for the attach, so the
attach won every time.

``_register_late_cloud_setup`` / ``_await_late_cloud_setup`` are the fix: the
two attach paths (``resume_thread._reprovision`` and the sessions router's
``_do_prepare``) await the in-flight task instead of racing it.

knowledge-history/done/session_resume_cloud_sync_race_late_provision.md
"""

from tests import _b09_control_seams as control_seams

import asyncio

import pytest

import orchestrator.main
from orchestrator.application import controls as controls_composition


@pytest.fixture(autouse=True)
def _clear_registry():
    """The registry is module-level; don't leak tasks between tests."""
    orchestrator.main.app.state.resources.late_cloud_setup_tasks.clear()
    yield
    orchestrator.main.app.state.resources.late_cloud_setup_tasks.clear()


class TestAwaitLateCloudSetup:
    @pytest.mark.asyncio
    async def test_noop_when_nothing_registered(self):
        """Nothing in flight for this thread (already done, never needed, or
        scheduled on the other HA replica) — the attach must not stall."""
        await asyncio.wait_for(
            control_seams.await_late_cloud_setup("t-unknown"), timeout=1
        )

    @pytest.mark.asyncio
    async def test_attach_observes_the_handle_the_task_persists(self):
        """The contract that matters: after the gate returns, the state the
        provisioning task writes is visible. This is what the agent's
        ``GET /api/agents/threads/{id}/workspace`` read depends on."""
        persisted: dict[str, str] = {}

        async def _provision() -> None:
            await asyncio.sleep(0.05)  # stands in for the WebDAV round-trips
            persisted["nc_session_folder"] = "sessions/t1"

        control_seams.register_late_cloud_setup("t1", asyncio.create_task(_provision()))

        # The attach path reads only after the gate.
        await control_seams.await_late_cloud_setup("t1")
        assert persisted == {"nc_session_folder": "sessions/t1"}

    @pytest.mark.asyncio
    async def test_without_the_gate_the_attach_would_read_nothing(self):
        """Guard against the gate silently becoming a no-op: the same task,
        read WITHOUT awaiting it, still exposes the pre-fix empty state."""
        persisted: dict[str, str] = {}

        async def _provision() -> None:
            await asyncio.sleep(0.05)
            persisted["nc_session_folder"] = "sessions/t1"

        control_seams.register_late_cloud_setup("t1", asyncio.create_task(_provision()))

        await asyncio.sleep(0)  # yield once, as a fast attach would
        assert persisted == {}

    @pytest.mark.asyncio
    async def test_failed_provisioning_does_not_break_the_attach(self):
        """``_late_cloud_setup`` already swallows its own errors, but a task
        that raises must never propagate into the attach path — a broken cloud
        degrades sync, it does not block the session from starting."""

        async def _provision() -> None:
            raise RuntimeError("webdav exploded")

        control_seams.register_late_cloud_setup("t1", asyncio.create_task(_provision()))

        await asyncio.wait_for(control_seams.await_late_cloud_setup("t1"), timeout=1)

    @pytest.mark.asyncio
    async def test_wedged_provisioning_times_out_instead_of_hanging_resume(
        self, monkeypatch
    ):
        """A cloud that never answers must delay the resume, not hang it. On
        timeout we fall through to the pre-fix behaviour (attach, possibly
        degraded) rather than stranding the user on a spinner."""
        monkeypatch.setattr(
            controls_composition, "LATE_CLOUD_SETUP_ATTACH_TIMEOUT_S", 0.05
        )

        task = asyncio.create_task(asyncio.sleep(30))
        control_seams.register_late_cloud_setup("t1", task)
        try:
            await asyncio.wait_for(
                control_seams.await_late_cloud_setup("t1"), timeout=2
            )
        finally:
            task.cancel()

    @pytest.mark.asyncio
    async def test_timeout_does_not_cancel_the_provisioning_task(self, monkeypatch):
        """``asyncio.shield`` is load-bearing: a waiter giving up must leave
        provisioning running, so the handle still lands for the NEXT resume.
        A bare ``wait_for(task)`` would cancel it and keep the thread broken
        forever."""
        monkeypatch.setattr(
            controls_composition, "LATE_CLOUD_SETUP_ATTACH_TIMEOUT_S", 0.02
        )
        persisted: dict[str, str] = {}

        async def _provision() -> None:
            await asyncio.sleep(0.1)
            persisted["nc_session_folder"] = "sessions/t1"

        task = asyncio.create_task(_provision())
        control_seams.register_late_cloud_setup("t1", task)

        await control_seams.await_late_cloud_setup("t1")  # gives up early
        assert persisted == {}
        assert not task.cancelled()

        await task
        assert persisted == {"nc_session_folder": "sessions/t1"}


class TestRegistrySlotDiscipline:
    @pytest.mark.asyncio
    async def test_slot_is_released_when_the_task_finishes(self):
        """Otherwise the registry grows one entry per resumed thread, forever."""

        async def _provision() -> None:
            return None

        task = asyncio.create_task(_provision())
        control_seams.register_late_cloud_setup("t1", task)
        assert "t1" in orchestrator.main.app.state.resources.late_cloud_setup_tasks

        await task
        await asyncio.sleep(0)  # let the done-callback run
        assert "t1" not in orchestrator.main.app.state.resources.late_cloud_setup_tasks

    @pytest.mark.asyncio
    async def test_stale_callback_does_not_clobber_a_newer_registration(self):
        """Same guard as ``_schedule_protected_engage``: a second resume
        landing right behind the first must keep ITS task awaitable, not have
        the slot cleared out from under it by the finished one."""
        first_done = asyncio.Event()

        async def _first() -> None:
            await first_done.wait()

        async def _second() -> None:
            await asyncio.sleep(0.05)

        first = asyncio.create_task(_first())
        control_seams.register_late_cloud_setup("t1", first)

        second = asyncio.create_task(_second())
        control_seams.register_late_cloud_setup("t1", second)

        first_done.set()
        await first
        await asyncio.sleep(0)  # first's done-callback fires here

        assert (
            orchestrator.main.app.state.resources.late_cloud_setup_tasks.get("t1")
            is second
        )
        await second
