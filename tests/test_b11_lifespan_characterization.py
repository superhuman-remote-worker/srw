"""R1.B11 characterization of the application lifespan's task ownership.

The lifespan starts ~54 background tasks and stops them on shutdown. Nothing
here runs a task body: ``asyncio.create_task`` is recorded while the lifespan
is entered, and every recorded task is a stub that notes when shutdown awaits
it. Each task is identified *behaviourally* — by the coroutine it would run
(the loop a ``run_when_leader`` wrapper would start, for gated ones) and its
task name — never by the module that defines it, so the same expectations hold
before and after the bodies move to their owners.

Every external collaborator the lifespan touches is replaced on
``orchestrator.main`` with a fake; ``KUBECONFIG`` points nowhere, so no path
can reach a real cluster or database.
"""

from __future__ import annotations

import asyncio
import contextlib
import functools
import os
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

os.environ.setdefault("VECTOR_DB_URL", "postgresql://test@localhost/test")

import orchestrator.main as main  # noqa: E402


# --------------------------------------------------------------------------- #
# Recording stubs
# --------------------------------------------------------------------------- #


class _StubTask:
    """Stands in for one background task; shutdown's ``await`` is recorded."""

    def __init__(self, recorder: "_Recorder", label: str, *, fail: bool) -> None:
        self._recorder = recorder
        self.label = label
        self._fail = fail
        self._cancelled = False
        self._callbacks: list[Any] = []

    def __await__(self):
        self._recorder.awaited.append(self.label)
        self._recorder.events.append(f"await:{self.label}")
        if self._fail:
            raise RuntimeError(f"{self.label} ended with an error")
        if False:  # pragma: no cover - makes this a generator
            yield
        return None

    # Minimal Task surface a supervisor may use.
    def done(self) -> bool:
        return self.label in self._recorder.awaited or self._cancelled

    def cancel(self, *_args: Any) -> bool:
        self._cancelled = True
        self._recorder.cancelled.append(self.label)
        self._recorder.events.append(f"cancel:{self.label}")
        return True

    def cancelled(self) -> bool:
        return self._cancelled

    def exception(self) -> BaseException | None:
        return RuntimeError(self.label) if self._fail else None

    def result(self) -> None:
        return None

    def add_done_callback(self, callback: Any, **_kwargs: Any) -> None:
        self._callbacks.append(callback)

    def remove_done_callback(self, callback: Any) -> int:
        return 0

    def get_name(self) -> str:
        return self.label


class _Recorder:
    def __init__(self) -> None:
        self.created: list[dict[str, Any]] = []
        self.awaited: list[str] = []
        self.cancelled: list[str] = []
        self.events: list[str] = []
        self.fail_labels: set[str] = set()

    def create_task(self, coro: Any, *, name: str | None = None, **_kw: Any):
        label, leader, target, arguments = _identify(coro)
        coro.close()
        entry = {
            "label": label,
            "leader": leader,
            "name": name,
            "target": target,
            "arguments": arguments,
        }
        self.created.append(entry)
        self.events.append(f"create:{label}")
        return _StubTask(self, label, fail=label in self.fail_labels)


def _identify(coro: Any) -> tuple[str, bool, Any, dict[str, Any]]:
    """Name the loop a created coroutine runs, and whether it is leader-gated."""

    qualname = getattr(coro, "__qualname__", repr(coro))
    frame_locals = dict(getattr(getattr(coro, "cr_frame", None), "f_locals", {}))
    if qualname == "run_when_leader":
        make = frame_locals["make_coro"]
        inner = make(asyncio.Event())
        inner_name = inner.__qualname__
        inner_locals = dict(inner.cr_frame.f_locals)
        inner.close()
        return inner_name, True, make, inner_locals
    return qualname, False, None, frame_locals


# --------------------------------------------------------------------------- #
# Fake collaborators
# --------------------------------------------------------------------------- #


def _fake(name: str, log: list[str], *, async_methods=(), sync_methods=(), **attrs):
    fake = MagicMock(name=name)
    for method in async_methods:

        async def _call(*_a, _method=method, **_k):
            log.append(f"{name}.{_method}")
            return None

        setattr(fake, method, _call)
    for method in sync_methods:

        def _scall(*_a, _method=method, **_k):
            log.append(f"{name}.{_method}")
            return None

        setattr(fake, method, _scall)
    for key, value in attrs.items():
        setattr(fake, key, value)
    return fake


class _FakeStore:
    """The application database, as far as startup and shutdown touch it."""

    def __init__(self, log: list[str], name: str = "postgres_db") -> None:
        self._log = log
        self._name = name
        self.pool = object()
        self._pool = None  # run_as_leader never acquires
        self.manifests_ready = False
        self.manifest_runtime_image = None
        self.manifest_skills_provider = None
        self.closed = False

    async def connect(self) -> None:
        self._log.append(f"{self._name}.connect")

    async def apply_migrations(self) -> None:
        self._log.append(f"{self._name}.apply_migrations")

    async def disconnect(self) -> None:
        self.closed = True
        self._log.append(f"{self._name}.disconnect")

    async def backfill_encrypt_datasource_credentials(self):
        return {"encrypted": 0, "skipped": 0, "errors": 0}

    async def backfill_strip_thread_config_secrets(self):
        return {"stripped": 0, "skipped": 0, "errors": 0}

    async def get_system_setting(self, *_a, **_k):
        return None

    async def delete_system_setting(self, *_a, **_k):
        return None

    def resolve_catalog_model(self, *_a, **_k):
        return None

    def __getattr__(self, item: str):
        # Startup hands some bound store methods to loops as references; a
        # task body never runs here, so they only need to exist.
        if item.startswith("__"):
            raise AttributeError(item)
        method = AsyncMock(name=f"{self._name}.{item}")
        object.__setattr__(self, item, method)
        return method


@contextlib.contextmanager
def _lifespan_environment(monkeypatch, recorder: _Recorder, *, env=None):
    log = recorder.events
    for key in (
        "LLM_BASE_URL",
        "COLLABORA_ENABLED",
        "MCP_DEV_TOKEN",
        "VM_MODE",
        "VM_WORKSPACE_RECOVERY_ACCEPTANCE_GATE_ENABLED",
        "STATELESS_DELETION_COST_RECONCILER_ENABLED",
        "STATELESS_CLOUD_PUSH_RECOVERY_ENABLED",
    ):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("KUBECONFIG", "/dev/null")
    for key, value in (env or {}).items():
        monkeypatch.setenv(key, value)

    store = _FakeStore(log)
    vector = _FakeStore(log, "vector_db")
    monkeypatch.setattr(main, "postgres_db", store)
    monkeypatch.setattr(main, "vector_db", vector)
    monkeypatch.setattr(main, "audit_db", None)
    monkeypatch.setattr(
        main,
        "audit_store",
        _fake("audit_store", log, async_methods=("connect", "disconnect")),
    )
    monkeypatch.setattr(main, "audit_reader", main.audit_store)
    for singleton, async_methods, sync_methods, attrs in (
        (
            "gitea_client",
            ("ensure_initialized", "ensure_oidc_configured", "close"),
            (),
            {},
        ),
        ("keycloak_groups", ("ensure_initialized",), (), {}),
        (
            "nats_bridge",
            ("connect", "disconnect"),
            (),
            {"lifecycle_identity_authenticated": False},
        ),
        ("snapshot_service", ("connect",), (), {"is_available": False}),
        (
            "vm_provisioner",
            ("disconnect",),
            ("connect",),
            {"mode": "off", "is_available": False},
        ),
        (
            "container_provisioner",
            (),
            ("connect",),
            {"is_available": False, "in_cluster": False},
        ),
        (
            "docker_provisioner",
            (),
            ("connect",),
            {"is_available": False, "workspace_hosts": []},
        ),
        ("ide_session_service", (), ("connect",), {}),
        ("persistent_provisioner", (), ("connect",), {"is_available": False}),
        (
            "agent_provisioner",
            (),
            ("connect",),
            {"is_available": False, "_k8s_available": False},
        ),
        ("workspace_suspension_service", (), ("connect",), {}),
        ("ide_proxy_service", (), ("connect",), {}),
        ("notification_service", (), ("connect",), {}),
        ("imap_poller", (), ("connect",), {"is_available": False}),
        ("sudo_gate", (), ("connect",), {}),
    ):
        monkeypatch.setattr(
            main,
            singleton,
            _fake(
                singleton,
                log,
                async_methods=async_methods,
                sync_methods=sync_methods,
                **attrs,
            ),
        )
    main.agent_provisioner.list_pods = AsyncMock(return_value=[])

    # Startup steps that would otherwise reach real services or schemas.
    import orchestrator.services.manifest_experts as manifest_experts
    import orchestrator.services.manifest_projects as manifest_projects

    monkeypatch.setattr(manifest_experts, "migrate_stored_experts", AsyncMock())
    monkeypatch.setattr(manifest_experts, "seed_bundled_expert_manifests", AsyncMock())
    monkeypatch.setattr(manifest_experts, "installed_srw_image", lambda: None)
    monkeypatch.setattr(manifest_projects, "migrate_projects", AsyncMock())
    monkeypatch.setattr(
        main,
        "seed_managed_default_experts",
        AsyncMock(return_value={"worker": None, "session": None}),
    )
    monkeypatch.setattr(
        main.readiness_service, "try_auto_pin_required_defaults", AsyncMock()
    )
    # The capability probe is bound wherever the startup step that calls it
    # lives (the lifespan at the base, the metering bootstrap afterwards).
    import orchestrator.services.infrastructure_metering.bootstrap as metering

    for owner in (main, metering):
        monkeypatch.setattr(
            owner,
            "probe_schema_capabilities",
            AsyncMock(return_value=_NoCapabilities()),
            raising=False,
        )
    monkeypatch.setattr(main, "initialize_main_cloud_instance_authority", AsyncMock())
    monkeypatch.setattr(main, "preload_retained_main_cloud_instances", AsyncMock())
    import orchestrator.seed.llm_config as llm_config

    monkeypatch.setattr(
        llm_config, "ensure_tavily_search_endpoint", AsyncMock(return_value=False)
    )
    monkeypatch.setattr(
        llm_config, "ensure_elevenlabs_tts_endpoint", AsyncMock(return_value=False)
    )

    real_drain = main.kb_datasource_tasks.drain

    async def _drain():
        log.append("kb_datasource_tasks.drain")
        await real_drain()

    monkeypatch.setattr(main.kb_datasource_tasks, "drain", _drain)

    from shared.runtime.core import model_registry

    real_register = model_registry.register_catalog_lookup

    def _register(value):
        log.append(
            "register_catalog_lookup(None)"
            if value is None
            else "register_catalog_lookup(store)"
        )
        return real_register(value)

    monkeypatch.setattr(model_registry, "register_catalog_lookup", _register)
    monkeypatch.setattr(asyncio, "create_task", recorder.create_task)
    try:
        yield SimpleNamespace(store=store, vector=vector, log=log)
    finally:
        monkeypatch.undo()


class _NoCapabilities:
    """Every metering schema capability absent: the tier stays dark."""

    def __getattr__(self, item: str):
        if item == "diagnostics":
            return lambda: {}
        if item.startswith("storage_identity_key"):
            return None
        return False


# --------------------------------------------------------------------------- #
# Expected behaviour at the base (default gates of an unconfigured process)
# --------------------------------------------------------------------------- #

# (label, leader-gated, task name) in creation order.
DEFAULT_CREATION = [
    ("run_as_leader", False, None),
    ("run_datasource_project_reconciler", False, None),
    ("stale_agent_detector", True, None),
    ("cleanup_expired_tokens", False, None),
    ("cleanup_expired_sessions", False, None),
    ("auto_assign_dispatcher", True, None),
    ("VMWorkspaceRecoveryService.run", True, "vm-workspace-recovery"),
    ("sudo_expiration_sweeper", False, None),
    ("thread_events_prune_sweeper", False, None),
    ("run_queue_reaper_loop", False, None),
    ("stateless_pod_deletion_cost_loop", False, "stateless-pod-deletion-cost"),
    ("SessionMemoryEffectDrain.run_drain", False, "session-memory-effect-drain"),
    ("CompletionMonitor.run", False, "completion-monitor"),
    ("security_events_prune_sweeper", False, None),
    ("ssh_attachments_prune_sweeper", False, None),
    ("run_retention_sweeper", False, None),
    ("thread_permission_notify_sweeper", True, None),
    ("attention_sleep_sweeper", True, None),
    ("officer_watchdog", True, None),
    ("message_route_reconciler_loop", True, None),
    ("officer_backlog_tick_loop", True, None),
    ("ide_session_ttl_sweeper", True, None),
    ("workspace_idle_sweeper", False, None),
    ("code_server_settings_sweeper", True, None),
    ("snapshot_gc_sweeper", False, None),
    ("pinned_agent_create_intent_reconciler", True, None),
    ("pinned_k8s_create_fence_gc_sweeper", True, None),
    ("imap_poll_loop", True, None),
    ("notification_steps_loop", True, None),
    ("delegation_timeout_sweeper", True, None),
    ("llm_outage_redispatch_sweeper", True, None),
    ("infra_transient_redispatch_sweeper", True, None),
    ("agent_pool_reconciler", True, None),
    ("ro_reader_reconciler_loop", True, None),
    ("cron_dispatcher_loop", False, None),
    ("project_loop_sweeper_loop", False, None),
    ("stale_verification_sweeper_loop", False, None),
    ("session_wake_sweeper_loop", False, None),
    ("kb_reindex_sweeper_loop", True, None),
    ("llm_pricing_sync_loop", False, None),
    ("cloud_pricing_sync_loop", False, None),
    ("workspace_metering_loop", True, None),
    ("llm_usage_poll_loop", False, None),
    ("usage_rollup_loop", True, None),
    ("lifecycle_reconciler_loop", True, None),
    ("run_listen_loop", False, None),
]

DEFAULT_SHUTDOWN = [
    "run_as_leader",
    "run_datasource_project_reconciler",
    "stale_agent_detector",
    "cleanup_expired_tokens",
    "cleanup_expired_sessions",
    "auto_assign_dispatcher",
    "VMWorkspaceRecoveryService.run",
    "sudo_expiration_sweeper",
    "thread_events_prune_sweeper",
    "run_queue_reaper_loop",
    "stateless_pod_deletion_cost_loop",
    "SessionMemoryEffectDrain.run_drain",
    "CompletionMonitor.run",
    "security_events_prune_sweeper",
    "ssh_attachments_prune_sweeper",
    "run_retention_sweeper",
    "thread_permission_notify_sweeper",
    "attention_sleep_sweeper",
    "officer_watchdog",
    "message_route_reconciler_loop",
    "officer_backlog_tick_loop",
    "ide_session_ttl_sweeper",
    "workspace_idle_sweeper",
    "code_server_settings_sweeper",
    "snapshot_gc_sweeper",
    "pinned_agent_create_intent_reconciler",
    "pinned_k8s_create_fence_gc_sweeper",
    "imap_poll_loop",
    "notification_steps_loop",
    "delegation_timeout_sweeper",
    "llm_outage_redispatch_sweeper",
    "infra_transient_redispatch_sweeper",
    "agent_pool_reconciler",
    "ro_reader_reconciler_loop",
    "lifecycle_reconciler_loop",
    "run_listen_loop",
    "cron_dispatcher_loop",
    "project_loop_sweeper_loop",
    "stale_verification_sweeper_loop",
    "session_wake_sweeper_loop",
    "kb_reindex_sweeper_loop",
    "llm_pricing_sync_loop",
    "cloud_pricing_sync_loop",
    "workspace_metering_loop",
    "llm_usage_poll_loop",
    "usage_rollup_loop",
]

# Closure after every task is awaited, in order.
SHUTDOWN_CLOSURE = [
    "kb_datasource_tasks.drain",
    "nats_bridge.disconnect",
    "vm_provisioner.disconnect",
    "gitea_client.close",
    "register_catalog_lookup(None)",
    "vector_db.disconnect",
    "audit_store.disconnect",
    "postgres_db.disconnect",
]


def _creation(recorder: _Recorder) -> list[tuple[str, bool, str | None]]:
    return [(e["label"], e["leader"], e["name"]) for e in recorder.created]


def _closure(recorder: _Recorder) -> list[str]:
    return [event for event in recorder.events if event in SHUTDOWN_CLOSURE]


def _run_lifespan(monkeypatch, recorder, *, env=None, inside=None):
    async def _go():
        with _lifespan_environment(monkeypatch, recorder, env=env) as world:
            async with main.lifespan(main.app):
                if inside is not None:
                    await inside(world)
            return world

    return _go()


# --------------------------------------------------------------------------- #
# Characterization
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_default_lifespan_starts_each_task_once_with_its_gate(monkeypatch):
    recorder = _Recorder()
    await _run_lifespan(monkeypatch, recorder)

    assert _creation(recorder) == DEFAULT_CREATION
    labels = [label for label, *_ in DEFAULT_CREATION]
    assert len(labels) == len(set(labels))


@pytest.mark.asyncio
async def test_shutdown_awaits_every_started_task_in_the_recorded_order(monkeypatch):
    recorder = _Recorder()
    await _run_lifespan(monkeypatch, recorder)

    assert recorder.awaited == DEFAULT_SHUTDOWN
    assert sorted(recorder.awaited) == sorted(e["label"] for e in recorder.created)
    # Every task is awaited before the registry drains and anything closes.
    last_create = max(
        i for i, e in enumerate(recorder.events) if e.startswith("create:")
    )
    first_close = recorder.events.index("kb_datasource_tasks.drain")
    assert last_create < first_close
    assert _closure(recorder) == SHUTDOWN_CLOSURE


@pytest.mark.asyncio
async def test_optional_tasks_follow_their_gates(monkeypatch):
    recorder = _Recorder()
    await _run_lifespan(
        monkeypatch,
        recorder,
        env={
            "VM_MODE": "same-cluster",
            "VM_WORKSPACE_RECOVERY_ACCEPTANCE_GATE_ENABLED": "true",
            "STATELESS_DELETION_COST_RECONCILER_ENABLED": "false",
            "STATELESS_CLOUD_PUSH_RECOVERY_ENABLED": "true",
        },
    )
    created = {e["label"]: e for e in recorder.created}
    assert created["vm_readiness_prober"]["leader"] is True
    assert created["VMCreationRetryService.run"]["name"] == "vm-creation-retry"
    assert "VMWorkspaceRecoveryService.run" not in created
    assert "stateless_pod_deletion_cost_loop" not in created
    assert created["CompletionFinalizer.run_drain"]["name"] == (
        "completion-finalizer-drain"
    )
    assert "CompletionSweepRouter.run" not in created
    assert sorted(recorder.awaited) == sorted(created)


@pytest.mark.asyncio
async def test_completion_commands_start_the_sweep_router(monkeypatch):
    recorder = _Recorder()
    monkeypatch.setattr(main, "COMPLETION_COMMANDS_ENABLED", True)
    await _run_lifespan(monkeypatch, recorder)
    created = {e["label"]: e for e in recorder.created}
    assert created["CompletionFinalizer.run_drain"]["name"] == (
        "completion-finalizer-drain"
    )
    assert created["CompletionSweepRouter.run"]["name"] == "completion-sweep-router"
    order = recorder.awaited
    assert (
        order.index("CompletionFinalizer.run_drain")
        < order.index("CompletionSweepRouter.run")
        < order.index("CompletionMonitor.run")
    )


@pytest.mark.asyncio
async def test_two_consecutive_lifecycles_are_isolated(monkeypatch):
    first = _Recorder()
    await _run_lifespan(monkeypatch, first)
    first_event = main._shutdown_event
    second = _Recorder()
    await _run_lifespan(monkeypatch, second)

    assert _creation(first) == _creation(second) == DEFAULT_CREATION
    assert first.awaited == second.awaited == DEFAULT_SHUTDOWN
    assert main._shutdown_event is not first_event
    assert first_event.is_set()


@pytest.mark.asyncio
async def test_preflight_refusal_happens_before_any_resource_is_acquired(
    monkeypatch,
):
    recorder = _Recorder()
    monkeypatch.setattr(main, "COMPLETION_COMMANDS_ENABLED", False)
    monkeypatch.setattr(main, "COMPLETION_STATUS_REORDER_ENABLED", True)
    monkeypatch.setattr(main.sys, "exit", MagicMock(side_effect=SystemExit(1)))
    with pytest.raises(SystemExit):
        await _run_lifespan(monkeypatch, recorder)
    assert recorder.created == []
    assert "postgres_db.connect" not in recorder.events


@pytest.mark.asyncio
async def test_leader_task_uses_the_application_store(monkeypatch):
    recorder = _Recorder()
    world = await _run_lifespan(monkeypatch, recorder)
    leader = recorder.created[0]
    assert leader["label"] == "run_as_leader"
    assert leader["arguments"]["db"] is world.store


# --------------------------------------------------------------------------- #
# Defects characterized at the base (strict xfail; corrected separately)
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason=(
        "R1.B11 characterization: one task that ended with an error aborts the "
        "rest of shutdown, so later tasks are never awaited and no pool closes"
    ),
)
async def test_shutdown_still_stops_every_task_and_closes_pools_after_a_failure(
    monkeypatch,
):
    recorder = _Recorder()
    recorder.fail_labels = {"stale_agent_detector"}
    with contextlib.suppress(RuntimeError):
        await _run_lifespan(monkeypatch, recorder)
    assert recorder.awaited == DEFAULT_SHUTDOWN
    assert _closure(recorder) == SHUTDOWN_CLOSURE


@pytest.mark.asyncio
@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason=(
        "R1.B11 characterization: a startup failure after tasks exist leaves "
        "them running and the pools open"
    ),
)
async def test_startup_failure_stops_started_tasks_and_releases_pools(monkeypatch):
    recorder = _Recorder()

    class _Boom(RuntimeError):
        pass

    def _explode(*_a, **_k):
        raise _Boom("lifecycle reconciler construction failed")

    monkeypatch.setattr(main, "InstanceLifecycleReconciler", _explode)
    with pytest.raises(_Boom):
        await _run_lifespan(monkeypatch, recorder)
    started = [e["label"] for e in recorder.created]
    assert started, "the failure must come after tasks were started"
    stopped = set(recorder.awaited) | set(recorder.cancelled)
    assert stopped == set(started)
    assert "postgres_db.disconnect" in recorder.events
    assert "vector_db.disconnect" in recorder.events


@pytest.mark.asyncio
@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason=(
        "R1.B11 characterization: a dispatch triggered during the application's "
        "life is never awaited or cancelled before the pools close"
    ),
)
async def test_triggered_dispatch_is_stopped_before_the_pools_close(monkeypatch):
    from orchestrator.services import leader_election

    recorder = _Recorder()
    monkeypatch.setattr(main, "AUTO_ASSIGN_ENABLED", True)
    monkeypatch.setattr(leader_election.is_leader, "is_set", lambda: True)

    async def _inside(_world):
        main._trigger_dispatch()

    await _run_lifespan(monkeypatch, recorder, inside=_inside)
    triggered = [
        e["label"] for e in recorder.created if "dispatch_pending_jobs" in e["label"]
    ]
    assert triggered, "the trigger must have scheduled a dispatch"
    stopped = set(recorder.awaited) | set(recorder.cancelled)
    assert set(triggered) <= stopped
    closing = recorder.events.index("postgres_db.disconnect")
    for label in triggered:
        stop_index = (
            max(
                i
                for i, event in enumerate(recorder.events)
                if event in {f"await:{label}", f"cancel:{label}"}
            )
            if any(
                event in {f"await:{label}", f"cancel:{label}"}
                for event in recorder.events
            )
            else len(recorder.events)
        )
        assert stop_index < closing


def test_identify_names_the_gated_loop_not_the_wrapper():
    from orchestrator.services.leader_election import run_when_leader

    async def some_loop(_event):
        return None

    event = asyncio.Event()
    wrapped = run_when_leader(functools.partial(some_loop), event)
    label, leader, _target, _args = _identify(wrapped)
    wrapped.close()
    assert (label, leader) == (
        "test_identify_names_the_gated_loop_not_the_wrapper.<locals>.some_loop",
        True,
    )
