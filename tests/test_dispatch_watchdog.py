"""The dispatcher stall watchdog: when it speaks, when it restarts the
replica, and when it stays quiet."""

from __future__ import annotations

import asyncio
import dataclasses
import logging
from types import SimpleNamespace

import pytest

from orchestrator.services import dispatch_watchdog
from orchestrator.services import job_dispatcher as scheduler
from tests import test_dispatcher_preflight_ownership as ownership
from orchestrator.services.dispatch_watchdog import (
    DispatchStall,
    dispatch_stall,
    dispatch_stall_watchdog,
    overdue_preflight,
)

INTERVAL = 30.0


def _state(*, started=None, finished=None, active=0, pending=0):
    return SimpleNamespace(
        loop_pass_started_at=started,
        loop_pass_finished_at=finished,
        active={str(i): ("owner", True) for i in range(active)},
        pending={str(i): object() for i in range(pending)},
    )


class TestDispatchStall:
    def test_on_time_between_passes(self):
        state = _state(started=100.0, finished=101.0)

        assert (
            dispatch_stall(state, now=125.0, watching_since=0.0, interval=INTERVAL)
            is None
        )

    def test_the_gap_between_passes_is_not_a_stall(self):
        state = _state(started=100.0, finished=101.0)

        assert (
            dispatch_stall(state, now=131.0, watching_since=0.0, interval=INTERVAL)
            is None
        )

    def test_no_pass_since_the_last_one_finished(self):
        state = _state(started=100.0, finished=101.0, active=2, pending=7)

        stall = dispatch_stall(state, now=341.0, watching_since=0.0, interval=INTERVAL)

        assert stall == DispatchStall(
            seconds=210.0, in_pass=False, active_preflights=2, pending_preflights=7
        )
        assert "no scheduling pass has finished for 210 s" in stall.describe()
        assert "2 workspace preparation(s) running" in stall.describe()
        assert "7 job(s) waiting" in stall.describe()

    def test_a_pass_that_does_not_come_back(self):
        """The whole running time counts: a pass has no interval to wait out."""
        state = _state(started=200.0, finished=101.0)

        stall = dispatch_stall(state, now=260.0, watching_since=0.0, interval=INTERVAL)

        assert stall is not None and stall.in_pass is True
        assert stall.seconds == 60.0
        assert "a scheduling pass has been running for 60 s" in stall.describe()

    def test_a_first_pass_that_never_finished(self):
        state = _state(started=50.0, finished=None)

        stall = dispatch_stall(state, now=95.0, watching_since=40.0, interval=INTERVAL)

        assert stall is not None and stall.in_pass is True
        assert stall.seconds == 45.0

    def test_a_loop_that_never_made_a_pass(self):
        """Counted from when the watch began."""
        state = _state()

        assert (
            dispatch_stall(state, now=60.0, watching_since=40.0, interval=INTERVAL)
            is None
        )
        stall = dispatch_stall(state, now=140.0, watching_since=40.0, interval=INTERVAL)
        assert stall is not None and stall.in_pass is False
        assert stall.seconds == 70.0

    def test_a_state_without_the_timestamps_reads_as_no_pass_yet(self):
        stall = dispatch_stall(
            object(), now=100.0, watching_since=0.0, interval=INTERVAL
        )

        assert stall is not None
        assert stall.active_preflights == 0 and stall.pending_preflights == 0


async def _watch(
    state,
    times,
    *,
    alarm=180.0,
    restart=0.0,
    may_schedule=lambda _state: True,
    on_tick=None,
):
    """Run the watchdog over ``times``: the first reading is when the watch
    began, each later one is one look at the dispatcher. Returns how often it
    asked for the process to end."""
    shutdown = asyncio.Event()
    readings = iter(times)
    ended: list[bool] = []
    looks = {"n": 0}

    def clock() -> float:
        try:
            value = next(readings)
        except StopIteration:
            shutdown.set()
            return times[-1]
        if on_tick is not None and looks["n"]:
            on_tick(looks["n"], value)
        looks["n"] += 1
        return value

    def terminate() -> None:
        # The process is ending: its shutdown follows.
        ended.append(True)
        shutdown.set()

    await asyncio.wait_for(
        dispatch_stall_watchdog(
            shutdown,
            state=state,
            may_schedule=may_schedule,
            interval=INTERVAL,
            alarm_seconds=alarm,
            restart_seconds=restart,
            terminate=terminate,
            check_interval=0,
            clock=clock,
        ),
        timeout=5,
    )
    return len(ended)


def _errors(caplog) -> list[str]:
    return [r.getMessage() for r in caplog.records if r.levelno == logging.ERROR]


class TestWatchdog:
    @pytest.mark.asyncio
    async def test_quiet_while_the_dispatcher_is_on_time(self, caplog):
        state = _state(started=0.0, finished=1.0)

        def keep_passing(_look, now):
            state.loop_pass_started_at = now - 2.0
            state.loop_pass_finished_at = now - 1.0

        with caplog.at_level(logging.WARNING, logger=dispatch_watchdog.__name__):
            ended = await _watch(
                state, [0.0, 15.0, 30.0, 45.0, 600.0], on_tick=keep_passing
            )

        assert ended == 0
        assert caplog.records == []

    @pytest.mark.asyncio
    async def test_one_error_when_the_stall_reaches_the_alarm(self, caplog):
        state = _state(started=0.0, finished=1.0, active=2, pending=5)

        with caplog.at_level(logging.WARNING, logger=dispatch_watchdog.__name__):
            ended = await _watch(state, [0.0, 100.0, 215.0, 230.0, 245.0])

        assert ended == 0
        (line,) = _errors(caplog)
        assert line.startswith("Dispatcher stalled: no scheduling pass has finished")
        assert "2 workspace preparation(s) running" in line

    @pytest.mark.asyncio
    async def test_the_error_repeats_every_five_minutes_not_every_look(self, caplog):
        state = _state(started=0.0, finished=1.0)

        with caplog.at_level(logging.WARNING, logger=dispatch_watchdog.__name__):
            await _watch(state, [0.0, 215.0, 230.0, 400.0, 520.0, 535.0])

        assert len(_errors(caplog)) == 2

    @pytest.mark.asyncio
    async def test_it_says_when_the_dispatcher_is_back(self, caplog):
        state = _state(started=0.0, finished=1.0)

        def recover(look, now):
            if look >= 2:
                state.loop_pass_started_at = now - 2.0
                state.loop_pass_finished_at = now - 1.0

        with caplog.at_level(logging.WARNING, logger=dispatch_watchdog.__name__):
            await _watch(state, [0.0, 400.0, 415.0, 430.0], on_tick=recover)

        assert len(_errors(caplog)) == 1
        warnings = [
            r.getMessage() for r in caplog.records if r.levelno == logging.WARNING
        ]
        assert warnings == ["Dispatcher is making passes again after a stall of 369 s"]

    @pytest.mark.asyncio
    async def test_no_restart_unless_it_is_asked_for(self):
        state = _state(started=0.0, finished=1.0)

        assert await _watch(state, [0.0, 10_000.0, 20_000.0]) == 0

    @pytest.mark.asyncio
    async def test_restart_once_the_stall_reaches_its_threshold(self, caplog):
        state = _state(started=50.0, finished=1.0)  # a pass that hangs

        with caplog.at_level(logging.WARNING, logger=dispatch_watchdog.__name__):
            ended = await _watch(
                state, [0.0, 250.0, 500.0, 660.0, 9_999.0], restart=600.0
            )

        assert ended == 1
        critical = [r for r in caplog.records if r.levelno == logging.CRITICAL]
        assert len(critical) == 1
        assert "Ending this replica" in critical[0].getMessage()
        # The alarm came first.
        assert len(_errors(caplog)) >= 1

    @pytest.mark.asyncio
    async def test_restart_alone_without_the_alarm(self, caplog):
        state = _state(started=50.0, finished=1.0)

        with caplog.at_level(logging.WARNING, logger=dispatch_watchdog.__name__):
            ended = await _watch(state, [0.0, 700.0], alarm=0.0, restart=600.0)

        assert ended == 1
        assert _errors(caplog) == []

    @pytest.mark.asyncio
    async def test_a_replica_that_may_not_schedule_is_not_stalled(self, caplog):
        """Not the leader, paused or closing: no pass is expected, and the
        watch starts over when it may schedule again."""
        state = _state(started=0.0, finished=1.0)
        allowed = {"now": False}

        def gate(look, _now):
            allowed["now"] = look >= 3

        with caplog.at_level(logging.WARNING, logger=dispatch_watchdog.__name__):
            ended = await _watch(
                state,
                [0.0, 1_000.0, 2_000.0, 2_015.0, 2_030.0],
                restart=600.0,
                may_schedule=lambda _state: allowed["now"],
                on_tick=gate,
            )

        assert ended == 0
        assert caplog.records == []

    @pytest.mark.asyncio
    async def test_off_waits_for_shutdown_instead_of_returning(self):
        """A leader-gated loop that returns is started again every second."""
        shutdown = asyncio.Event()
        task = asyncio.create_task(
            dispatch_stall_watchdog(
                shutdown,
                state=_state(),
                may_schedule=lambda _state: True,
                interval=INTERVAL,
                alarm_seconds=0.0,
                restart_seconds=0.0,
            )
        )
        await asyncio.sleep(0.01)
        assert not task.done()
        shutdown.set()
        await asyncio.wait_for(task, timeout=1)


class TestSettings:
    def _settings(self):
        from orchestrator.application.settings import DeploymentSettings

        return DeploymentSettings.from_environment()

    def test_defaults(self, monkeypatch):
        for name in (
            "DISPATCH_STALL_ALARM_SECONDS",
            "DISPATCH_STALL_RESTART_SECONDS",
            "DISPATCH_PREFLIGHT_TIMEOUT_SECONDS",
        ):
            monkeypatch.delenv(name, raising=False)

        settings = self._settings()

        assert settings.dispatch_stall_alarm_seconds == 180.0
        assert settings.dispatch_stall_restart_seconds == 0.0  # never
        assert settings.dispatch_preflight_timeout_seconds == 1800.0

    def test_values(self, monkeypatch):
        monkeypatch.setenv("DISPATCH_STALL_ALARM_SECONDS", "90")
        monkeypatch.setenv("DISPATCH_STALL_RESTART_SECONDS", "600")
        monkeypatch.setenv("DISPATCH_PREFLIGHT_TIMEOUT_SECONDS", "0")

        settings = self._settings()

        assert settings.dispatch_stall_alarm_seconds == 90.0
        assert settings.dispatch_stall_restart_seconds == 600.0
        assert settings.dispatch_preflight_timeout_seconds == 0.0  # no limit

    @pytest.mark.parametrize("raw", ["soon", "-5"])
    def test_nonsense_is_not_a_restart_threshold(self, monkeypatch, raw):
        monkeypatch.setenv("DISPATCH_STALL_RESTART_SECONDS", raw)

        assert self._settings().dispatch_stall_restart_seconds == 0.0


# =============================================================================
# The dispatcher's side: pass timestamps and the preflight time limit
# =============================================================================


class TestTheLoopKeepsItsPassTimes:
    @pytest.mark.asyncio
    async def test_a_finished_pass_is_on_time(self, monkeypatch):
        from orchestrator.services import leader_election

        leader = asyncio.Event()
        leader.set()
        monkeypatch.setattr(leader_election, "is_leader", leader)
        deps = ownership.dependencies(ownership.SnapshotStore(pinned=[]), None)
        stop = asyncio.Event()
        loop = asyncio.create_task(
            scheduler.auto_assign_dispatcher(stop, dependencies=deps)
        )
        try:
            await ownership.until(lambda: deps.state.loop_pass_finished_at is not None)
            state = deps.state
            assert state.loop_pass_started_at <= state.loop_pass_finished_at
            assert (
                dispatch_stall(
                    state,
                    now=state.loop_pass_finished_at + 5.0,
                    watching_since=0.0,
                    interval=scheduler.DISPATCH_INTERVAL_SECONDS,
                )
                is None
            )
        finally:
            stop.set()
            loop.cancel()
            await asyncio.gather(loop, return_exceptions=True)
            await deps.state.drain()

    @pytest.mark.asyncio
    async def test_a_pass_that_hangs_shows_as_a_running_pass(self, monkeypatch):
        from orchestrator.services import leader_election

        leader = asyncio.Event()
        leader.set()
        monkeypatch.setattr(leader_election, "is_leader", leader)
        entered = asyncio.Event()

        async def hang(**_kwargs):
            entered.set()
            await asyncio.Event().wait()

        monkeypatch.setattr(scheduler, "dispatch_pending_jobs", hang)
        deps = ownership.dependencies(ownership.SnapshotStore(pinned=[]), None)
        stop = asyncio.Event()
        loop = asyncio.create_task(
            scheduler.auto_assign_dispatcher(stop, dependencies=deps)
        )
        try:
            await asyncio.wait_for(entered.wait(), 2)
            state = deps.state
            assert state.loop_pass_started_at is not None
            assert state.loop_pass_finished_at is None
            stall = dispatch_stall(
                state,
                now=state.loop_pass_started_at + 240.0,
                watching_since=0.0,
                interval=scheduler.DISPATCH_INTERVAL_SECONDS,
            )
            assert stall is not None and stall.in_pass is True
            assert stall.seconds == 240.0
            # The dispatcher's own "may I schedule" is what the watchdog asks.
            assert scheduler.may_schedule(state) is True
            leader.clear()
            assert scheduler.may_schedule(state) is False
        finally:
            stop.set()
            loop.cancel()
            await asyncio.gather(loop, return_exceptions=True)
            await deps.state.drain()


class TestThePreflightTimeLimit:
    @pytest.mark.asyncio
    async def test_a_preflight_that_never_returns_frees_its_slot(self, caplog):
        """Two workspace creations that hang used to hold both creation slots
        for good; every later job waited behind them."""
        jobs = [ownership.sandbox(f"job-{i}") for i in range(3)]
        store = ownership.SnapshotStore(pinned=jobs)
        started: list[str] = []
        cancelled: list[str] = []

        async def create(owner, **_kwargs):
            started.append(owner.id)
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancelled.append(owner.id)
                raise

        deps = dataclasses.replace(
            ownership.dependencies(store, create), preflight_timeout_seconds=0.05
        )
        try:
            with caplog.at_level(logging.ERROR, logger=scheduler.__name__):
                await scheduler.dispatch_pending_jobs(dependencies=deps)
                await ownership.until(lambda: len(started) == 2)
                first_two = sorted(started)
                # Both creation slots are taken; the third job waits.
                assert scheduler.MUTATION_PREFLIGHT_LIMIT == 2
                await ownership.until(lambda: len(started) == 3)

            # The third job only got a slot because the limit cancelled the
            # first two.
            assert sorted(cancelled[:2]) == first_two
            assert started[2] not in first_two
            lines = [
                r.getMessage()
                for r in caplog.records
                if "did not return within" in r.getMessage()
            ]
            assert len(lines) >= 2
            assert all("cancelled it and freed its slot" in line for line in lines)
        finally:
            await deps.state.drain()

    @pytest.mark.asyncio
    async def test_a_timeout_inside_the_preflight_is_not_the_limit(self, caplog):
        """A lock wait that gives up raises the same exception type. It stays
        an ordinary preflight failure, with its traceback."""
        store = ownership.SnapshotStore(pinned=[ownership.sandbox("locked")])

        async def create(_owner, **_kwargs):
            raise TimeoutError("workspace mutation lock")

        deps = dataclasses.replace(
            ownership.dependencies(store, create), preflight_timeout_seconds=30.0
        )
        try:
            with caplog.at_level(logging.ERROR, logger=scheduler.__name__):
                await scheduler.dispatch_pending_jobs(dependencies=deps)
                await ownership.flush(deps.state)

            messages = [r.getMessage() for r in caplog.records]
            assert any("preflight failed for job locked" in m for m in messages)
            assert not any("did not return within" in m for m in messages)
            assert not deps.state.active
        finally:
            await deps.state.drain()

    @pytest.mark.asyncio
    async def test_zero_means_no_limit(self):
        store = ownership.SnapshotStore(pinned=[ownership.sandbox("slow")])
        entered = asyncio.Event()

        async def create(_owner, **_kwargs):
            entered.set()
            await asyncio.Event().wait()

        deps = dataclasses.replace(
            ownership.dependencies(store, create), preflight_timeout_seconds=0.0
        )
        try:
            await scheduler.dispatch_pending_jobs(dependencies=deps)
            await asyncio.wait_for(entered.wait(), 2)
            await asyncio.sleep(0.1)
            assert "slow" in deps.state.active
        finally:
            await deps.state.drain()

    def test_the_default_limit_is_half_an_hour(self):
        deps = ownership.dependencies(ownership.SnapshotStore(pinned=[]), None)

        assert deps.preflight_timeout_seconds == scheduler.PREFLIGHT_TIMEOUT_SECONDS
        assert scheduler.PREFLIGHT_TIMEOUT_SECONDS == 1800.0


class TestAfterAskingForTheRestart:
    @pytest.mark.asyncio
    async def test_it_waits_for_shutdown_and_asks_only_once(self):
        """Seen on k3d: the loop returned after asking, and the leader gate
        started it again in the process that was shutting down."""
        state = _state(started=0.0, finished=None)
        shutdown = asyncio.Event()
        asked: list[bool] = []
        task = asyncio.create_task(
            dispatch_stall_watchdog(
                shutdown,
                state=state,
                may_schedule=lambda _state: True,
                interval=INTERVAL,
                alarm_seconds=20.0,
                restart_seconds=60.0,
                terminate=lambda: asked.append(True),
                check_interval=0,
                clock=lambda: 1_000.0,
            )
        )
        await ownership.until(lambda: bool(asked))
        await asyncio.sleep(0.02)

        assert asked == [True]
        assert not task.done()
        shutdown.set()
        await asyncio.wait_for(task, timeout=1)


# =============================================================================
# A preflight that ignores its cancel
# =============================================================================


class TestOverduePreflight:
    def _state(self, **started):
        state = _state(active=len(started), pending=4)
        state.active_since = dict(started)
        return state

    def test_none_without_a_time_limit(self):
        state = self._state(job=0.0)

        assert overdue_preflight(state, now=99_999.0, preflight_timeout=0.0) is None

    def test_none_while_inside_the_limit(self):
        state = self._state(job=100.0)

        assert overdue_preflight(state, now=700.0, preflight_timeout=600.0) is None

    def test_the_one_that_is_longest_overdue(self):
        state = self._state(young=900.0, old=100.0, older=40.0)

        stall = overdue_preflight(state, now=1_000.0, preflight_timeout=600.0)

        assert stall is not None
        assert stall.overdue_job_id == "older"
        assert stall.seconds == 360.0
        assert stall.active_preflights == 3 and stall.pending_preflights == 4
        assert stall.describe().startswith(
            "the workspace preparation for job older was cancelled at its time "
            "limit and has not returned for 360 s"
        )

    def test_a_state_without_start_times(self):
        assert overdue_preflight(object(), now=1.0, preflight_timeout=1.0) is None

    @pytest.mark.asyncio
    async def test_the_watchdog_alarms_and_restarts_on_it(self, caplog):
        """Passes keep running, so only the preflight says something is
        wrong: it holds a creation slot although it was cancelled."""
        state = _state(started=0.0, finished=1.0, active=1)
        state.active_since = {"hung": 0.0}

        def keep_passing(_look, now):
            state.loop_pass_started_at = now - 2.0
            state.loop_pass_finished_at = now - 1.0

        shutdown = asyncio.Event()
        readings = iter([0.0, 500.0, 800.0, 815.0, 1_250.0, 9_999.0])
        ended: list[bool] = []
        looks = {"n": 0}

        def clock() -> float:
            try:
                value = next(readings)
            except StopIteration:
                shutdown.set()
                return 9_999.0
            if looks["n"]:
                keep_passing(looks["n"], value)
            looks["n"] += 1
            return value

        def terminate() -> None:
            ended.append(True)
            shutdown.set()

        with caplog.at_level(logging.WARNING, logger=dispatch_watchdog.__name__):
            await asyncio.wait_for(
                dispatch_stall_watchdog(
                    shutdown,
                    state=state,
                    may_schedule=lambda _state: True,
                    interval=INTERVAL,
                    alarm_seconds=180.0,
                    restart_seconds=600.0,
                    preflight_timeout=600.0,
                    terminate=terminate,
                    check_interval=0,
                    clock=clock,
                ),
                timeout=5,
            )

        # 500: inside the limit. 800: 200 s overdue, the alarm. 815: no
        # second line yet. 1250: 650 s overdue, the alarm again (five minutes
        # on) and the restart.
        assert ended == [True]
        errors = _errors(caplog)
        assert len(errors) == 2
        assert all("workspace preparation for job hung" in line for line in errors)
        assert "has not returned for 200 s" in errors[0]


class TestTheDispatcherRecordsPreflightStarts:
    @pytest.mark.asyncio
    async def test_start_times_follow_the_active_preflights(self):
        store = ownership.SnapshotStore(pinned=[ownership.sandbox("slow")])
        entered, release = asyncio.Event(), asyncio.Event()

        async def create(_owner, **_kwargs):
            entered.set()
            await release.wait()
            return False

        deps = ownership.dependencies(store, create)
        try:
            await scheduler.dispatch_pending_jobs(dependencies=deps)
            await asyncio.wait_for(entered.wait(), 2)
            assert set(deps.state.active_since) == {"slow"} == set(deps.state.active)
            release.set()
            await ownership.until(lambda: "slow" not in deps.state.active)
            assert deps.state.active_since == {}
        finally:
            await deps.state.drain()
        assert deps.state.active_since == {}

    @pytest.mark.asyncio
    async def test_a_preflight_that_ignores_its_cancel_stays_overdue(self):
        """A blocking call that never returns cannot be cancelled. The limit
        fires, the preflight stays, and the watchdog can see for how long."""
        store = ownership.SnapshotStore(pinned=[ownership.sandbox("hung")])
        cancelled, release = asyncio.Event(), asyncio.Event()

        async def create(_owner, **_kwargs):
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancelled.set()
                # What a joined SDK thread does to its caller: it keeps it.
                await asyncio.shield(release.wait())
                raise

        deps = dataclasses.replace(
            ownership.dependencies(store, create), preflight_timeout_seconds=0.05
        )
        try:
            await scheduler.dispatch_pending_jobs(dependencies=deps)
            await asyncio.wait_for(cancelled.wait(), 2)
            await asyncio.sleep(0.02)

            assert "hung" in deps.state.active
            started = deps.state.active_since["hung"]
            stall = overdue_preflight(
                deps.state, now=started + 400.0, preflight_timeout=0.05
            )
            assert stall is not None and stall.overdue_job_id == "hung"
            assert stall.seconds == pytest.approx(399.95)
        finally:
            release.set()
            await deps.state.drain()
