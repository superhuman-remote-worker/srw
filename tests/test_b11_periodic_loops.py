"""R1.B11: the periodic loop bodies main.py hosted, now in their domain owners.

Each moved loop takes its collaborators as keyword parameters. The unit cases
drive one or two ticks through a stand-in for the owner module's ``asyncio``
name (:class:`_Cadence`), which records every ``wait_for`` timeout, checks the
loop waits on *its* shutdown event, and ends the loop the way production does
(the shutdown event fires during the cadence wait and the loop ``break``s).
Module singletons are replaced by :class:`_Forbidden` so a loop that fell back
to one instead of the injected collaborator fails loudly.

The real-PostgreSQL cases run the three retention loops against
``schema_current.sql``: the thread_events prune must keep every event that is
still the only durable receipt for a pending control or interrupt request and
delete only by the two age windows; the security/ssh prunes must delete only
past the resolved retention.
"""

from __future__ import annotations

import asyncio
import json
import logging
from contextlib import asynccontextmanager
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, call
from uuid import UUID, uuid4

import asyncpg
import pytest
import pytest_asyncio
from testcontainers.postgres import PostgresContainer

import orchestrator.services.agent_provisioner as agent_provisioner_module
import orchestrator.services.ide_profile_store as ide_profile_store_module
import orchestrator.services.ide_session as ide_session_module
import orchestrator.services.ide_settings as ide_settings_module
import orchestrator.services.imap_poller as imap_poller_module
import orchestrator.services.lifecycle.reconciler as lifecycle_reconciler_module
import orchestrator.services.retention_sweepers as retention_module
import orchestrator.services.ro_reader_reconciler as ro_reader_module
import orchestrator.services.session_provisioner as session_provisioner_module
import orchestrator.services.snapshot_service as snapshot_service_module
import orchestrator.services.sudo_gate as sudo_gate_module
from orchestrator.database.postgres import PostgresDB

SCHEMA_FILE = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "orchestrator"
    / "database"
    / "schema_current.sql"
)


class _Cadence:
    """Stands in for a loop module's ``asyncio`` name.

    Every ``wait_for`` is recorded with its timeout and must wait on the loop's
    own shutdown event. The first ``ticks - 1`` waits report the cadence
    timeout (the loop ticks again); the last one sets the shutdown event and
    returns the real wait, so the loop leaves through its ``break``. Every
    other attribute resolves to the real :mod:`asyncio`.
    """

    def __init__(self, shutdown: asyncio.Event, ticks: int):
        self.shutdown = shutdown
        self.ticks = ticks
        self.timeouts: list[float] = []

    def __getattr__(self, name):
        return getattr(asyncio, name)

    async def wait_for(self, awaitable, timeout):
        assert awaitable.cr_frame.f_locals.get("self") is self.shutdown, (
            "the cadence wait must be on the loop's shutdown event"
        )
        self.timeouts.append(timeout)
        if len(self.timeouts) < self.ticks:
            awaitable.close()
            raise asyncio.TimeoutError
        self.shutdown.set()
        return await awaitable


class _Forbidden:
    """A module singleton a moved loop must never reach."""

    def __init__(self, name: str):
        self._name = name

    def __getattr__(self, attr):
        raise AssertionError(
            f"moved loop fell back to the module singleton {self._name}.{attr}"
        )


async def _drive(monkeypatch, module, loop, *, ticks: int, **kwargs) -> _Cadence:
    shutdown = asyncio.Event()
    cadence = _Cadence(shutdown, ticks)
    monkeypatch.setattr(module, "asyncio", cadence)
    await asyncio.wait_for(loop(shutdown, **kwargs), timeout=5)
    assert shutdown.is_set()
    return cadence


def _records(caplog, module) -> list[tuple[str, str]]:
    return [
        (record.levelname, record.getMessage())
        for record in caplog.records
        if record.name == module.__name__
    ]


async def _assert_parks_until_shutdown(loop_factory, *untouched) -> None:
    """The loop must stay parked on ``shutdown_event.wait()`` and return on set."""

    shutdown = asyncio.Event()
    waiting = asyncio.Event()
    real_wait = shutdown.wait

    async def observed_wait():
        waiting.set()
        return await real_wait()

    shutdown.wait = AsyncMock(side_effect=observed_wait)  # type: ignore[method-assign]
    task = asyncio.create_task(loop_factory(shutdown))
    try:
        await asyncio.wait_for(waiting.wait(), timeout=1)
        await asyncio.sleep(0)
        assert not task.done(), "the disabled loop must park, not return"
        for collaborator in untouched:
            assert collaborator.mock_calls == []
        shutdown.set()
        await asyncio.wait_for(task, timeout=1)
        shutdown.wait.assert_awaited_once()
        for collaborator in untouched:
            assert collaborator.mock_calls == []
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)


# =============================================================================
# agent_pool_reconciler -> services/agent_provisioner.py
# =============================================================================


def _provisioner(*, available: bool = True) -> MagicMock:
    provisioner = MagicMock()
    provisioner.is_available = available
    provisioner.ensure_warm_pool = AsyncMock()
    provisioner.reap_pods = AsyncMock()
    provisioner.scale_down_idle = AsyncMock()
    return provisioner


class TestAgentPoolReconciler:
    @pytest.mark.asyncio
    async def test_ticks_the_injected_provisioner_in_order(self, monkeypatch, caplog):
        caplog.set_level(logging.INFO)
        monkeypatch.setattr(
            agent_provisioner_module,
            "agent_provisioner",
            _Forbidden("agent_provisioner"),
        )
        provisioner = _provisioner()

        cadence = await _drive(
            monkeypatch,
            agent_provisioner_module,
            agent_provisioner_module.agent_pool_reconciler,
            ticks=2,
            provisioner=provisioner,
        )

        assert cadence.timeouts == [60.0, 60.0]
        assert [name for name, *_ in provisioner.mock_calls] == [
            "ensure_warm_pool",
            "reap_pods",
            "scale_down_idle",
        ] * 2
        assert _records(caplog, agent_provisioner_module) == [
            ("INFO", "Agent pool reconciler started"),
            ("INFO", "Agent pool reconciler stopped"),
        ]

    @pytest.mark.asyncio
    async def test_unavailable_provisioner_is_not_driven(self, monkeypatch):
        provisioner = _provisioner(available=False)

        cadence = await _drive(
            monkeypatch,
            agent_provisioner_module,
            agent_provisioner_module.agent_pool_reconciler,
            ticks=1,
            provisioner=provisioner,
        )

        assert cadence.timeouts == [60.0]
        provisioner.ensure_warm_pool.assert_not_awaited()
        provisioner.reap_pods.assert_not_awaited()
        provisioner.scale_down_idle.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_tick_failure_is_logged_at_error_and_the_loop_continues(
        self, monkeypatch, caplog
    ):
        caplog.set_level(logging.INFO)
        provisioner = _provisioner()
        provisioner.ensure_warm_pool.side_effect = [RuntimeError("k8s down"), None]

        cadence = await _drive(
            monkeypatch,
            agent_provisioner_module,
            agent_provisioner_module.agent_pool_reconciler,
            ticks=2,
            provisioner=provisioner,
        )

        assert cadence.timeouts == [60.0, 60.0]
        assert provisioner.ensure_warm_pool.await_count == 2
        # The failing tick aborted before reaping; the next tick ran fully.
        assert provisioner.reap_pods.await_count == 1
        assert provisioner.scale_down_idle.await_count == 1
        assert (
            "ERROR",
            "Error in agent pool reconciler: k8s down",
        ) in _records(caplog, agent_provisioner_module)


# =============================================================================
# lifecycle_reconciler_loop -> services/lifecycle/reconciler.py
# =============================================================================


class TestLifecycleReconcilerLoop:
    @pytest.mark.asyncio
    async def test_tick_failure_is_logged_with_traceback_and_the_loop_continues(
        self, monkeypatch, caplog
    ):
        caplog.set_level(logging.INFO)
        reconciler = MagicMock()
        reconciler.tick = AsyncMock(side_effect=[RuntimeError("drain failed"), None])

        cadence = await _drive(
            monkeypatch,
            lifecycle_reconciler_module,
            lifecycle_reconciler_module.lifecycle_reconciler_loop,
            ticks=2,
            reconciler=reconciler,
        )

        assert cadence.timeouts == [60.0, 60.0]
        assert reconciler.tick.await_count == 2
        assert _records(caplog, lifecycle_reconciler_module) == [
            ("INFO", "Lifecycle reconciler loop started"),
            ("ERROR", "Lifecycle reconciler tick failed"),
            ("INFO", "Lifecycle reconciler loop stopped"),
        ]
        failed = next(
            r
            for r in caplog.records
            if r.getMessage() == "Lifecycle reconciler tick failed"
        )
        assert failed.exc_info is not None
        assert str(failed.exc_info[1]) == "drain failed"


# =============================================================================
# sudo_expiration_sweeper -> services/sudo_gate.py
# =============================================================================


class TestSudoExpirationSweeper:
    @pytest.mark.asyncio
    async def test_both_steps_run_every_tick_and_fail_in_isolation(
        self, monkeypatch, caplog
    ):
        caplog.set_level(logging.INFO)
        monkeypatch.setattr(sudo_gate_module, "sudo_gate", _Forbidden("sudo_gate"))
        gate = MagicMock()
        gate.sweep_expired = AsyncMock(side_effect=[RuntimeError("nats down"), 0])
        fail_expired = AsyncMock(side_effect=[0, RuntimeError("db down")])

        cadence = await _drive(
            monkeypatch,
            sudo_gate_module,
            sudo_gate_module.sudo_expiration_sweeper,
            ticks=2,
            gate=gate,
            fail_expired_vm_upgrade_jobs=fail_expired,
        )

        assert cadence.timeouts == [15.0, 15.0]
        assert gate.sweep_expired.await_count == 2
        # A gate failure does not skip the vm_upgrade step (tick 1) and a
        # vm_upgrade failure does not stop the loop (tick 2).
        assert fail_expired.await_count == 2
        assert fail_expired.await_args_list == [call(), call()]
        assert _records(caplog, sudo_gate_module) == [
            ("INFO", "Sudo expiration sweeper started"),
            ("ERROR", "Error in sudo expiration sweeper: nats down"),
            ("ERROR", "Error failing expired vm_upgrade jobs: db down"),
            ("INFO", "Sudo expiration sweeper stopped"),
        ]


# =============================================================================
# ide_session_ttl_sweeper -> services/ide_session.py
# =============================================================================


class TestIdeSessionTtlSweeper:
    @pytest.mark.asyncio
    async def test_expires_through_the_injected_service(self, monkeypatch, caplog):
        caplog.set_level(logging.INFO)
        monkeypatch.setattr(
            ide_session_module,
            "ide_session_service",
            _Forbidden("ide_session_service"),
        )
        sessions = MagicMock()
        sessions.check_ttl_all = AsyncMock(side_effect=[RuntimeError("db down"), 3])

        cadence = await _drive(
            monkeypatch,
            ide_session_module,
            ide_session_module.ide_session_ttl_sweeper,
            ticks=2,
            ide_sessions=sessions,
        )

        assert cadence.timeouts == [60.0, 60.0]
        assert sessions.check_ttl_all.await_count == 2
        assert _records(caplog, ide_session_module) == [
            ("INFO", "IDE session TTL sweeper started"),
            ("ERROR", "Error in IDE session TTL sweeper: db down"),
            ("INFO", "IDE session sweeper: expired 3 sessions"),
            ("INFO", "IDE session TTL sweeper stopped"),
        ]


# =============================================================================
# ro_reader_reconciler_loop -> services/ro_reader_reconciler.py
# =============================================================================


class TestRoReaderReconcilerLoop:
    @pytest.mark.asyncio
    async def test_router_provider_is_read_on_every_tick(self, monkeypatch, caplog):
        caplog.set_level(logging.INFO)
        store = MagicMock()
        first_router, second_router = object(), object()
        router = MagicMock(side_effect=[first_router, second_router])
        reconcile = AsyncMock(side_effect=[RuntimeError("nextcloud down"), 0])
        monkeypatch.setattr(ro_reader_module, "reconcile_orphaned_ro_mounts", reconcile)

        cadence = await _drive(
            monkeypatch,
            ro_reader_module,
            ro_reader_module.ro_reader_reconciler_loop,
            ticks=2,
            store=store,
            router=router,
        )

        assert cadence.timeouts == [900.0, 900.0]
        # main re-read its main_cloud_router global each tick; the provider
        # keeps that late binding (a router swap lands on the next tick).
        assert reconcile.await_args_list == [
            call(postgres_db=store, router=first_router),
            call(postgres_db=store, router=second_router),
        ]
        assert router.call_count == 2
        assert _records(caplog, ro_reader_module) == [
            ("INFO", "RO reader reconciler started"),
            ("ERROR", "Error in RO reader reconciler: nextcloud down"),
            ("INFO", "RO reader reconciler stopped"),
        ]


# =============================================================================
# workspace_idle_sweeper -> services/session_provisioner.py
# =============================================================================


class TestWorkspaceIdleSweeper:
    @pytest.mark.asyncio
    async def test_reconciles_with_the_injected_collaborators(
        self, monkeypatch, caplog
    ):
        caplog.set_level(logging.INFO)
        store, provisioner, suspension = MagicMock(), MagicMock(), MagicMock()
        reconcile = AsyncMock(side_effect=[RuntimeError("k8s down"), 1])
        monkeypatch.setattr(
            session_provisioner_module, "reconcile_session_workspaces", reconcile
        )

        cadence = await _drive(
            monkeypatch,
            session_provisioner_module,
            session_provisioner_module.workspace_idle_sweeper,
            ticks=2,
            store=store,
            provisioner=provisioner,
            suspension=suspension,
        )

        assert cadence.timeouts == [60.0, 60.0]
        assert (
            reconcile.await_args_list
            == [call(db=store, provisioner=provisioner, suspension=suspension)] * 2
        )
        assert _records(caplog, session_provisioner_module) == [
            ("INFO", "Workspace idle sweeper started"),
            ("ERROR", "Error in session workspace reconcile: k8s down"),
            ("INFO", "Workspace idle sweeper stopped"),
        ]
        store.acquire.assert_not_called()

    @pytest.mark.asyncio
    async def test_drives_the_vm_idle_service_once_migration_0270_exists(
        self, monkeypatch, caplog
    ):
        caplog.set_level(logging.INFO)
        conn = _FakeConn([True])
        store = _store_with_conn(conn)
        reconcile = AsyncMock(return_value=0)
        monkeypatch.setattr(
            session_provisioner_module, "reconcile_session_workspaces", reconcile
        )
        vm_idle = MagicMock()
        vm_idle.reconcile_once = AsyncMock(side_effect=[RuntimeError("held"), 0])
        factory = MagicMock(return_value=vm_idle)

        cadence = await _drive(
            monkeypatch,
            session_provisioner_module,
            session_provisioner_module.workspace_idle_sweeper,
            ticks=2,
            store=store,
            provisioner=MagicMock(),
            suspension=MagicMock(),
            vm_idle_service_factory=factory,
        )

        assert cadence.timeouts == [60.0, 60.0]
        conn.fetchval.assert_awaited_once_with(
            "SELECT to_regclass('public.vm_idle_operations') IS NOT NULL"
        )
        # Built once at loop start, driven every tick; a failed pass is held,
        # not fatal, and the session reconcile still runs each tick.
        factory.assert_called_once_with()
        assert vm_idle.reconcile_once.await_args_list == [call(limit=16)] * 2
        assert reconcile.await_count == 2
        assert [
            level
            for level, message in _records(caplog, session_provisioner_module)
            if message == "VM idle reconcile held"
        ] == ["ERROR"]

    @pytest.mark.asyncio
    async def test_skips_the_vm_idle_service_before_migration_0270(self, monkeypatch):
        store = _store_with_conn(_FakeConn([False]))
        monkeypatch.setattr(
            session_provisioner_module,
            "reconcile_session_workspaces",
            AsyncMock(return_value=0),
        )
        factory = MagicMock()

        await _drive(
            monkeypatch,
            session_provisioner_module,
            session_provisioner_module.workspace_idle_sweeper,
            ticks=1,
            store=store,
            provisioner=MagicMock(),
            suspension=MagicMock(),
            vm_idle_service_factory=factory,
        )

        factory.assert_not_called()


# =============================================================================
# code_server_settings_sweeper -> services/ide_settings.py
# =============================================================================


class _IdeHarness:
    """Patches the ide_settings helpers the sweeper resolves at call time."""

    def __init__(self, monkeypatch, *, snapshot_available: bool):
        self.k8s_row = {"user_id": "user-1", "context": {"kind": "k8s"}}
        self.vm_row = {"user_id": "user-2", "context": {"kind": "vm"}}
        self.db = MagicMock()
        self.db.list_active_ide_workspaces = AsyncMock(
            return_value=[self.k8s_row, self.vm_row]
        )
        self.container_provisioner = MagicMock()
        self.vm_provisioner = MagicMock()
        self.snapshots = MagicMock()
        self.snapshots.is_available = snapshot_available
        self.store = MagicMock(name="settings_store")
        self.classifier = MagicMock(name="classifier")
        self.profile = MagicMock(name="profile_store")
        self.store_factory = MagicMock(return_value=self.store)
        self.classifier_factory = MagicMock(return_value=self.classifier)
        self.profile_factory = MagicMock(return_value=self.profile)
        self.evict = AsyncMock(side_effect=lambda rows, provisioner, db: list(rows))
        self.reconcile_settings = AsyncMock(return_value=2)
        self.reconcile_extensions = AsyncMock(return_value=1)
        self.capture = AsyncMock()
        self.reconcile_vm = AsyncMock(return_value=0)
        patches = {
            "IdeSettingsStore": self.store_factory,
            "OpenVsxClassifier": self.classifier_factory,
            "evict_dead_workspaces": self.evict,
            "reconcile_ide_settings": self.reconcile_settings,
            "reconcile_extensions": self.reconcile_extensions,
            "capture_ide_profile": self.capture,
            "reconcile_vm_ide_workspace": self.reconcile_vm,
            "is_vm_capture_context": lambda context: context["kind"] == "vm",
            "resolve_ssh_target": lambda context: ("ws.svc.cluster.local", 2222),
        }
        for name, value in patches.items():
            monkeypatch.setattr(ide_settings_module, name, value)
        monkeypatch.setattr(
            ide_profile_store_module, "IdeProfileStore", self.profile_factory
        )

    def kwargs(self) -> dict:
        return {
            "db": self.db,
            "container_provisioner": self.container_provisioner,
            "snapshot_service": self.snapshots,
            "vm_provisioner": self.vm_provisioner,
        }


class TestCodeServerSettingsSweeper:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("value", ["false", "0", "off"])
    async def test_disabled_sweeper_parks_on_the_shutdown_event(
        self, monkeypatch, caplog, value
    ):
        caplog.set_level(logging.INFO)
        monkeypatch.setenv("IDE_SETTINGS_SYNC_ENABLED", value)
        harness = _IdeHarness(monkeypatch, snapshot_available=True)

        await _assert_parks_until_shutdown(
            lambda shutdown: ide_settings_module.code_server_settings_sweeper(
                shutdown, **harness.kwargs()
            ),
            harness.db,
            harness.container_provisioner,
            harness.snapshots,
            harness.vm_provisioner,
            harness.store_factory,
            harness.classifier_factory,
        )
        assert _records(caplog, ide_settings_module) == [
            (
                "INFO",
                "Code-server settings sweeper disabled (IDE_SETTINGS_SYNC_ENABLED)",
            )
        ]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("value", [None, "1", "TRUE", "yes"])
    async def test_one_tick_uses_every_injected_collaborator(
        self, monkeypatch, caplog, value
    ):
        caplog.set_level(logging.INFO)
        if value is None:
            monkeypatch.delenv("IDE_SETTINGS_SYNC_ENABLED", raising=False)
        else:
            monkeypatch.setenv("IDE_SETTINGS_SYNC_ENABLED", value)
        monkeypatch.delenv("IDE_SETTINGS_SYNC_INTERVAL_S", raising=False)
        h = _IdeHarness(monkeypatch, snapshot_available=True)

        cadence = await _drive(
            monkeypatch,
            ide_settings_module,
            ide_settings_module.code_server_settings_sweeper,
            ticks=1,
            **h.kwargs(),
        )

        assert cadence.timeouts == [600.0]
        h.store_factory.assert_called_once_with(h.db)
        h.classifier_factory.assert_called_once_with()
        h.db.list_active_ide_workspaces.assert_awaited_once_with()
        h.evict.assert_awaited_once_with([h.k8s_row], h.container_provisioner, h.db)
        h.reconcile_settings.assert_awaited_once_with(
            h.store, [h.k8s_row], ide_settings_module.pull_ide_config
        )
        h.reconcile_extensions.assert_awaited_once_with(
            h.store,
            [h.k8s_row],
            ide_settings_module.list_ide_extensions,
            h.classifier,
        )
        h.profile_factory.assert_called_once_with(h.snapshots._s3, h.snapshots._bucket)
        h.capture.assert_awaited_once_with(
            h.store, "user-1", "ws.svc.cluster.local", 2222, h.profile
        )
        h.reconcile_vm.assert_awaited_once_with(
            store=h.store,
            workspace=h.vm_row,
            db=h.db,
            vm_provisioner=h.vm_provisioner,
            classifier=h.classifier,
            profile_store=h.profile,
        )
        assert _records(caplog, ide_settings_module) == [
            ("INFO", "Code-server settings sweeper started (interval=600s)"),
            (
                "INFO",
                "IDE settings sweeper: 1 workspace(s), 0 dialed via stable service DNS",
            ),
            ("INFO", "IDE settings sweeper: synced 2 file(s)"),
            ("INFO", "IDE settings sweeper: synced 1 extension(s)"),
            ("INFO", "Code-server settings sweeper stopped"),
        ]

    @pytest.mark.asyncio
    async def test_without_snapshots_vm_capture_runs_without_a_profile_store(
        self, monkeypatch
    ):
        monkeypatch.delenv("IDE_SETTINGS_SYNC_ENABLED", raising=False)
        h = _IdeHarness(monkeypatch, snapshot_available=False)

        await _drive(
            monkeypatch,
            ide_settings_module,
            ide_settings_module.code_server_settings_sweeper,
            ticks=1,
            **h.kwargs(),
        )

        h.profile_factory.assert_not_called()
        h.capture.assert_not_awaited()
        h.reconcile_vm.assert_awaited_once_with(
            store=h.store,
            workspace=h.vm_row,
            db=h.db,
            vm_provisioner=h.vm_provisioner,
            classifier=h.classifier,
        )

    @pytest.mark.asyncio
    async def test_tick_failure_is_logged_and_the_classifier_cache_persists(
        self, monkeypatch, caplog
    ):
        caplog.set_level(logging.INFO)
        monkeypatch.delenv("IDE_SETTINGS_SYNC_ENABLED", raising=False)
        monkeypatch.setenv("IDE_SETTINGS_SYNC_INTERVAL_S", "42")
        h = _IdeHarness(monkeypatch, snapshot_available=False)
        h.db.list_active_ide_workspaces.side_effect = [RuntimeError("db down"), []]

        cadence = await _drive(
            monkeypatch,
            ide_settings_module,
            ide_settings_module.code_server_settings_sweeper,
            ticks=2,
            **h.kwargs(),
        )

        assert cadence.timeouts == [42.0, 42.0]
        assert h.db.list_active_ide_workspaces.await_count == 2
        h.store_factory.assert_called_once_with(h.db)
        h.classifier_factory.assert_called_once_with()
        assert _records(caplog, ide_settings_module) == [
            ("INFO", "Code-server settings sweeper started (interval=42s)"),
            ("ERROR", "Error in code-server settings sweeper: db down"),
            ("INFO", "Code-server settings sweeper stopped"),
        ]


# =============================================================================
# snapshot_gc_sweeper -> services/snapshot_service.py
# =============================================================================


class TestSnapshotGcSweeper:
    @pytest.mark.asyncio
    async def test_runs_gc_on_the_injected_service_daily(self, monkeypatch, caplog):
        caplog.set_level(logging.INFO)
        monkeypatch.setattr(
            snapshot_service_module,
            "snapshot_service",
            _Forbidden("snapshot_service"),
        )
        snapshots = MagicMock()
        snapshots.is_available = True
        stats = {"soft_deleted": 1, "purged": 0}
        snapshots.run_gc = AsyncMock(side_effect=[RuntimeError("s3 down"), stats])

        cadence = await _drive(
            monkeypatch,
            snapshot_service_module,
            snapshot_service_module.snapshot_gc_sweeper,
            ticks=2,
            snapshots=snapshots,
        )

        assert cadence.timeouts == [86400, 86400]
        assert snapshots.run_gc.await_count == 2
        assert _records(caplog, snapshot_service_module) == [
            ("INFO", "Snapshot GC sweeper started"),
            ("ERROR", "Error in snapshot GC sweeper: s3 down"),
            ("INFO", f"Snapshot GC: {stats}"),
            ("INFO", "Snapshot GC sweeper stopped"),
        ]

    @pytest.mark.asyncio
    async def test_unavailable_service_is_not_collected(self, monkeypatch):
        snapshots = MagicMock()
        snapshots.is_available = False
        snapshots.run_gc = AsyncMock()

        await _drive(
            monkeypatch,
            snapshot_service_module,
            snapshot_service_module.snapshot_gc_sweeper,
            ticks=1,
            snapshots=snapshots,
        )

        snapshots.run_gc.assert_not_awaited()


# =============================================================================
# imap_poll_loop -> services/imap_poller.py
# =============================================================================


class TestImapPollLoop:
    @pytest.mark.asyncio
    async def test_unconfigured_poller_parks_on_the_shutdown_event(
        self, monkeypatch, caplog
    ):
        caplog.set_level(logging.INFO)
        monkeypatch.setattr(
            imap_poller_module, "imap_poller", _Forbidden("imap_poller")
        )
        poller = MagicMock()
        poller.is_available = False
        poller.poll_once = AsyncMock()

        await _assert_parks_until_shutdown(
            lambda shutdown: imap_poller_module.imap_poll_loop(shutdown, poller=poller),
            poller.poll_once,
        )
        assert _records(caplog, imap_poller_module) == [
            ("INFO", "IMAP poller not started (not configured)")
        ]

    @pytest.mark.asyncio
    async def test_polls_the_injected_poller_on_its_interval(self, monkeypatch, caplog):
        caplog.set_level(logging.INFO)
        monkeypatch.setattr(
            imap_poller_module, "imap_poller", _Forbidden("imap_poller")
        )
        poller = MagicMock()
        poller.is_available = True
        poller.poll_interval = 7
        poller.poll_once = AsyncMock(side_effect=[RuntimeError("imap down"), 2])

        cadence = await _drive(
            monkeypatch,
            imap_poller_module,
            imap_poller_module.imap_poll_loop,
            ticks=2,
            poller=poller,
        )

        assert cadence.timeouts == [7, 7]
        assert poller.poll_once.await_count == 2
        assert _records(caplog, imap_poller_module) == [
            ("INFO", "IMAP poller started (interval=7s)"),
            ("ERROR", "IMAP poller error: imap down"),
            ("INFO", "IMAP poller: processed 2 email reply(ies)"),
            ("INFO", "IMAP poller stopped"),
        ]


# =============================================================================
# Every simple loop: a shutdown already signalled means no tick at all
# =============================================================================


_PRESET_CASES = [
    (
        agent_provisioner_module,
        "agent_pool_reconciler",
        {"provisioner": _Forbidden("provisioner")},
    ),
    (
        lifecycle_reconciler_module,
        "lifecycle_reconciler_loop",
        {"reconciler": _Forbidden("reconciler")},
    ),
    (
        sudo_gate_module,
        "sudo_expiration_sweeper",
        {
            "gate": _Forbidden("gate"),
            "fail_expired_vm_upgrade_jobs": _Forbidden("fail_expired"),
        },
    ),
    (
        ide_session_module,
        "ide_session_ttl_sweeper",
        {"ide_sessions": _Forbidden("ide_sessions")},
    ),
    (
        ro_reader_module,
        "ro_reader_reconciler_loop",
        {"store": _Forbidden("store"), "router": _Forbidden("router")},
    ),
    (
        session_provisioner_module,
        "workspace_idle_sweeper",
        {
            "store": _Forbidden("store"),
            "provisioner": _Forbidden("provisioner"),
            "suspension": _Forbidden("suspension"),
        },
    ),
    (
        snapshot_service_module,
        "snapshot_gc_sweeper",
        {"snapshots": _Forbidden("snapshots")},
    ),
    (
        retention_module,
        "thread_events_prune_sweeper",
        {"store": _Forbidden("store")},
    ),
    (
        retention_module,
        "security_events_prune_sweeper",
        {"store": _Forbidden("store")},
    ),
    (
        retention_module,
        "ssh_attachments_prune_sweeper",
        {"store": _Forbidden("store")},
    ),
]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("module", "name", "kwargs"),
    _PRESET_CASES,
    ids=[name for _, name, _ in _PRESET_CASES],
)
async def test_signalled_shutdown_skips_the_tick(module, name, kwargs):
    shutdown = asyncio.Event()
    shutdown.set()
    await asyncio.wait_for(getattr(module, name)(shutdown, **kwargs), timeout=1)


# =============================================================================
# Retention loops -> services/retention_sweepers.py (unit)
# =============================================================================


class _FakeConn:
    def __init__(self, results):
        self.fetchval = AsyncMock(side_effect=results)


def _store_with_conn(conn) -> MagicMock:
    store = MagicMock()

    @asynccontextmanager
    async def acquire():
        yield conn

    store.acquire = MagicMock(side_effect=acquire)
    return store


class TestRetentionSweepersUnit:
    @pytest.mark.asyncio
    async def test_thread_events_runs_ended_then_active_prune(
        self, monkeypatch, caplog
    ):
        caplog.set_level(logging.INFO)
        monkeypatch.delenv("THREAD_EVENTS_PRUNE_INTERVAL_S", raising=False)
        conn = _FakeConn([2, 3])
        store = _store_with_conn(conn)

        cadence = await _drive(
            monkeypatch,
            retention_module,
            retention_module.thread_events_prune_sweeper,
            ticks=1,
            store=store,
        )

        assert cadence.timeouts == [300.0]
        ended_sql, active_sql = (c.args[0] for c in conn.fetchval.await_args_list)
        assert "SELECT id FROM threads WHERE status = 'ended'" in ended_sql
        assert "interval '24 hours'" in ended_sql
        assert "SELECT id FROM threads WHERE status <> 'ended'" in active_sql
        assert "interval '7 days'" in active_sql
        assert _records(caplog, retention_module) == [
            ("INFO", "Thread-events prune sweeper started (interval=300s)"),
            ("INFO", "thread_events prune: ended=2 active=3"),
            ("INFO", "Thread-events prune sweeper stopped"),
        ]

    @pytest.mark.asyncio
    async def test_thread_events_error_is_a_warning_and_the_loop_continues(
        self, monkeypatch, caplog
    ):
        caplog.set_level(logging.INFO)
        monkeypatch.setenv("THREAD_EVENTS_PRUNE_INTERVAL_S", "12")
        conn = _FakeConn([RuntimeError("db down"), None, None])
        store = _store_with_conn(conn)

        cadence = await _drive(
            monkeypatch,
            retention_module,
            retention_module.thread_events_prune_sweeper,
            ticks=2,
            store=store,
        )

        assert cadence.timeouts == [12.0, 12.0]
        assert store.acquire.call_count == 2
        # Nothing deleted on tick 2 (None counts) -> no count line.
        assert _records(caplog, retention_module) == [
            ("INFO", "Thread-events prune sweeper started (interval=12s)"),
            ("WARNING", "thread_events prune error (non-fatal): db down"),
            ("INFO", "Thread-events prune sweeper stopped"),
        ]

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("name", "method", "interval_env", "retention_env", "label", "table"),
        [
            (
                "security_events_prune_sweeper",
                "prune_security_events",
                "SECURITY_EVENTS_PRUNE_INTERVAL_S",
                "SECURITY_EVENTS_RETENTION_DAYS",
                "Security-events",
                "security_events",
            ),
            (
                "ssh_attachments_prune_sweeper",
                "prune_ssh_attachments",
                "SSH_ATTACHMENTS_PRUNE_INTERVAL_S",
                "SSH_ATTACHMENTS_RETENTION_DAYS",
                "SSH-attachments",
                "ssh_attachments",
            ),
        ],
    )
    @pytest.mark.parametrize(
        ("interval", "retention", "expected_interval", "expected_retention"),
        [(None, None, 3600, 90), ("120", "14", 120, 14)],
        ids=["defaults", "env"],
    )
    async def test_age_prune_uses_resolved_retention_and_cadence(
        self,
        monkeypatch,
        caplog,
        name,
        method,
        interval_env,
        retention_env,
        label,
        table,
        interval,
        retention,
        expected_interval,
        expected_retention,
    ):
        caplog.set_level(logging.INFO)
        for env, value in ((interval_env, interval), (retention_env, retention)):
            if value is None:
                monkeypatch.delenv(env, raising=False)
            else:
                monkeypatch.setenv(env, value)
        store = MagicMock()
        prune = AsyncMock(side_effect=[RuntimeError("db down"), 4])
        setattr(store, method, prune)

        cadence = await _drive(
            monkeypatch,
            retention_module,
            getattr(retention_module, name),
            ticks=2,
            store=store,
        )

        assert cadence.timeouts == [float(expected_interval)] * 2
        assert prune.await_args_list == [call(expected_retention)] * 2
        assert _records(caplog, retention_module) == [
            (
                "INFO",
                f"{label} prune sweeper started "
                f"(interval={expected_interval}s, retention={expected_retention}d)",
            ),
            ("WARNING", f"{table} prune error (non-fatal): db down"),
            ("INFO", f"{table} prune: deleted=4"),
            ("INFO", f"{label} prune sweeper stopped"),
        ]


# =============================================================================
# Retention loops against real PostgreSQL
# =============================================================================


@pytest.fixture(scope="module")
def pg_dsn():
    try:
        container = PostgresContainer("postgres:15")
        container.start()
    except Exception as exc:
        pytest.skip(f"local PostgreSQL container unavailable: {exc}")
    try:
        yield container.get_connection_url().replace(
            "postgresql+psycopg2", "postgresql"
        )
    finally:
        container.stop()


@pytest_asyncio.fixture(scope="module")
async def _schema_applied(pg_dsn):
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(SCHEMA_FILE.read_text())
    finally:
        await conn.close()


@pytest_asyncio.fixture
async def db(pg_dsn, _schema_applied):
    store = PostgresDB(connection_string=pg_dsn, min_connections=1, max_connections=4)
    await store.connect()
    async with store.acquire() as conn:
        await conn.execute(
            "TRUNCATE thread_events, thread_control_requests, "
            "thread_interrupt_requests, security_events, ssh_attachments, "
            "threads, users CASCADE"
        )
    try:
        yield store
    finally:
        await store.close()


class _TickSignal:
    """Delegates to the real store and flags when one prune tick finished."""

    def __init__(self, store: PostgresDB, method: str):
        self._store = store
        self._method = method
        self.calls = 0
        self.done = asyncio.Event()

    def __getattr__(self, name):
        attr = getattr(self._store, name)
        if name != self._method:
            return attr
        if name == "acquire":

            @asynccontextmanager
            async def acquire():
                self.calls += 1
                try:
                    async with attr() as conn:
                        yield conn
                finally:
                    self.done.set()

            return acquire

        async def prune(*args, **kwargs):
            self.calls += 1
            try:
                return await attr(*args, **kwargs)
            finally:
                self.done.set()

        return prune


async def _run_one_real_tick(loop, signal: _TickSignal) -> None:
    """Run the loop on the real event loop: one tick, then the real cadence wait
    (hours long) must return as soon as shutdown is signalled."""

    shutdown = asyncio.Event()
    task = asyncio.create_task(loop(shutdown, store=signal))
    try:
        await asyncio.wait_for(signal.done.wait(), timeout=30)
        shutdown.set()
        await asyncio.wait_for(task, timeout=5)
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    assert signal.calls == 1


async def _thread(conn, *, status: str) -> UUID:
    user_id, thread_id = uuid4(), uuid4()
    await conn.execute(
        "INSERT INTO users (id, display_name, email) VALUES ($1, 'b11', $2)",
        user_id,
        f"{user_id}@example.test",
    )
    await conn.execute(
        "INSERT INTO threads (id, user_id, status, execution_lane, config_name, "
        "metadata) VALUES ($1, $2, $3, 'stateless', 'session_base', $4::jsonb)",
        thread_id,
        user_id,
        status,
        json.dumps({"config_override": {"workspace": {"backend": "virtual"}}}),
    )
    return thread_id


class _Journal:
    """Seeds one thread's events, each tagged by label, at a given age."""

    def __init__(self, conn, thread_id: UUID):
        self.conn = conn
        self.thread_id = thread_id
        self.seq = 0
        self.ids: dict[int, str] = {}

    async def event(
        self,
        label: str,
        age: str,
        *,
        control_request_id: UUID | None = None,
        interrupt_request_id: UUID | None = None,
    ) -> None:
        self.seq += 1
        event_id = await self.conn.fetchval(
            "INSERT INTO thread_events (thread_id, epoch, seq, kind, payload, "
            "created_at, control_request_id, interrupt_request_id) "
            "VALUES ($1, 0, $2, 'test', $3::jsonb, now() - $4::text::interval, $5, $6) "
            "RETURNING id",
            self.thread_id,
            self.seq,
            json.dumps({"label": label}),
            age,
            control_request_id,
            interrupt_request_id,
        )
        self.ids[event_id] = label

    async def control(self, *, pending: bool) -> UUID:
        self.seq += 1
        if pending:
            return await self.conn.fetchval(
                "INSERT INTO thread_control_requests (thread_id, request_seq, "
                "client_request_id, verb, requested_by) "
                "VALUES ($1, $2, $3, 'mode.set', 'b11') RETURNING id",
                self.thread_id,
                self.seq,
                uuid4(),
            )
        return await self.conn.fetchval(
            "INSERT INTO thread_control_requests (thread_id, request_seq, "
            "client_request_id, verb, requested_by, outcome, result, applied_at, "
            "applied_lease_token, journal_epoch, journal_seq, acknowledged_at) "
            "VALUES ($1, $2, $3, 'mode.set', 'b11', 'applied', '{}'::jsonb, now(), "
            "1, 0, $2, now()) RETURNING id",
            self.thread_id,
            self.seq,
            uuid4(),
        )

    async def interrupt(self, *, outcome: str | None, result: dict | None = None):
        self.seq += 1
        columns = (
            "thread_id, client_request_id, target_turn_id, accepted_lease_token, "
            "accepted_leased_by, requested_by"
        )
        if outcome is None:
            return await self.conn.fetchval(
                f"INSERT INTO thread_interrupt_requests ({columns}) "
                "VALUES ($1, $2, 2, 1, 'pod-a', 'b11') RETURNING id",
                self.thread_id,
                uuid4(),
            )
        if outcome == "applied":
            return await self.conn.fetchval(
                f"INSERT INTO thread_interrupt_requests ({columns}, outcome, result, "
                "applied_mode, applied_at, applied_lease_token, journal_epoch, "
                "journal_seq, acknowledged_at) VALUES ($1, $2, 2, 1, 'pod-a', "
                "'b11', 'applied', $3::jsonb, 'hard', now(), 1, 0, $4, now()) "
                "RETURNING id",
                self.thread_id,
                uuid4(),
                json.dumps(result or {}),
                self.seq,
            )
        return await self.conn.fetchval(
            f"INSERT INTO thread_interrupt_requests ({columns}, outcome, result, "
            "applied_at, applied_lease_token, journal_epoch, journal_seq, "
            "acknowledged_at, error_code) VALUES ($1, $2, 2, 1, 'pod-a', 'b11', "
            "'rejected', '{}'::jsonb, now(), 1, 0, $3, now(), 'turn_ended') "
            "RETURNING id",
            self.thread_id,
            uuid4(),
            self.seq,
        )

    async def seed(self, *, stale: str, fresh: str) -> None:
        """Every receipt kind, each on an event past the thread's window."""

        await self.event("plain_stale", stale)
        await self.event("plain_fresh", fresh)
        await self.event(
            "control_pending",
            stale,
            control_request_id=await self.control(pending=True),
        )
        await self.event(
            "control_applied",
            stale,
            control_request_id=await self.control(pending=False),
        )
        await self.event(
            "interrupt_pending",
            stale,
            interrupt_request_id=await self.interrupt(outcome=None),
        )
        await self.event(
            "interrupt_applied_unconsumed",
            stale,
            interrupt_request_id=await self.interrupt(
                outcome="applied", result={"mode": "hard"}
            ),
        )
        await self.event(
            "interrupt_applied_consumed",
            stale,
            interrupt_request_id=await self.interrupt(
                outcome="applied", result={"consumed_input_seq": 4}
            ),
        )
        await self.event(
            "interrupt_rejected",
            stale,
            interrupt_request_id=await self.interrupt(outcome="rejected"),
        )


_KEPT = {
    "plain_fresh",
    "control_pending",
    "interrupt_pending",
    "interrupt_applied_unconsumed",
}


class TestRetentionSweepersRealPostgres:
    @pytest.mark.asyncio
    async def test_thread_events_prune_keeps_pending_receipts_and_ages_out_by_window(
        self, db, monkeypatch, caplog
    ):
        caplog.set_level(logging.INFO)
        monkeypatch.delenv("THREAD_EVENTS_PRUNE_INTERVAL_S", raising=False)
        async with db.acquire() as conn:
            ended = _Journal(conn, await _thread(conn, status="ended"))
            # Ended threads age out after 24h: 25h is stale, 23h is fresh.
            await ended.seed(stale="25 hours", fresh="23 hours")
            active = _Journal(conn, await _thread(conn, status="active"))
            # Live threads age out after 7 days: 6 days (well past the ended
            # window) must survive.
            await active.seed(stale="8 days", fresh="6 days")

        signal = _TickSignal(db, "acquire")
        await _run_one_real_tick(retention_module.thread_events_prune_sweeper, signal)

        async with db.acquire() as conn:
            remaining = {
                row["id"] for row in await conn.fetch("SELECT id FROM thread_events")
            }
            requests = (
                await conn.fetchval("SELECT count(*) FROM thread_control_requests"),
                await conn.fetchval("SELECT count(*) FROM thread_interrupt_requests"),
            )
        for journal in (ended, active):
            kept = {
                journal.ids[event_id] for event_id in remaining & journal.ids.keys()
            }
            assert kept == _KEPT, journal.thread_id
        # The prune only touches the journal, never the request rows.
        assert requests == (4, 8)
        assert (
            "INFO",
            "thread_events prune: ended=4 active=4",
        ) in _records(caplog, retention_module)
        assert _records(caplog, retention_module)[-1] == (
            "INFO",
            "Thread-events prune sweeper stopped",
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("retention", "expected_kept"),
        [(None, {"89d", "10d"}), ("30", {"10d"})],
        ids=["default-90d", "env-30d"],
    )
    async def test_security_events_prune_deletes_only_past_retention(
        self, db, monkeypatch, caplog, retention, expected_kept
    ):
        caplog.set_level(logging.INFO)
        if retention is None:
            monkeypatch.delenv("SECURITY_EVENTS_RETENTION_DAYS", raising=False)
        else:
            monkeypatch.setenv("SECURITY_EVENTS_RETENTION_DAYS", retention)
        async with db.acquire() as conn:
            for label in ("91d", "89d", "10d"):
                await conn.execute(
                    "INSERT INTO security_events (created_at, event_type, "
                    "resource_type, detail) "
                    "VALUES (now() - $1::text::interval, 'forbidden', 'job', $2)",
                    label.replace("d", " days"),
                    label,
                )

        signal = _TickSignal(db, "prune_security_events")
        await _run_one_real_tick(retention_module.security_events_prune_sweeper, signal)

        async with db.acquire() as conn:
            kept = {
                row["detail"]
                for row in await conn.fetch("SELECT detail FROM security_events")
            }
        assert kept == expected_kept
        assert (
            "INFO",
            f"security_events prune: deleted={3 - len(expected_kept)}",
        ) in _records(caplog, retention_module)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("retention", "expected_kept"),
        [(None, {"89d", "10d"}), ("30", {"10d"})],
        ids=["default-90d", "env-30d"],
    )
    async def test_ssh_attachments_prune_deletes_only_past_retention(
        self, db, monkeypatch, caplog, retention, expected_kept
    ):
        caplog.set_level(logging.INFO)
        if retention is None:
            monkeypatch.delenv("SSH_ATTACHMENTS_RETENTION_DAYS", raising=False)
        else:
            monkeypatch.setenv("SSH_ATTACHMENTS_RETENTION_DAYS", retention)
        async with db.acquire() as conn:
            for label in ("91d", "89d", "10d"):
                await conn.execute(
                    "INSERT INTO ssh_attachments (handle, attached_at) "
                    "VALUES ($1, now() - $2::text::interval)",
                    label,
                    label.replace("d", " days"),
                )

        signal = _TickSignal(db, "prune_ssh_attachments")
        await _run_one_real_tick(retention_module.ssh_attachments_prune_sweeper, signal)

        async with db.acquire() as conn:
            kept = {
                row["handle"]
                for row in await conn.fetch("SELECT handle FROM ssh_attachments")
            }
        assert kept == expected_kept
        assert (
            "INFO",
            f"ssh_attachments prune: deleted={3 - len(expected_kept)}",
        ) in _records(caplog, retention_module)
