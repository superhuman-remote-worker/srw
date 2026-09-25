"""R1.B11 characterization of the application lifespan's task ownership.

The lifespan starts ~54 background tasks and stops them on shutdown. Nothing
here runs a task body: ``asyncio.create_task`` is recorded while the lifespan
is entered, and every recorded task is a stub that notes when shutdown awaits
it. Each task is identified *behaviourally* — by the coroutine it would run
(the loop a ``run_when_leader`` wrapper would start, for gated ones) and its
task name — never by the module that defines it, so the same expectations hold
before and after the bodies move to their owners.

Each run enters ``orchestrator.application.lifecycle.lifespan`` over a fresh
application of its own (``_application``: the resources ``create_app()``
builds, without the process-wide router and auth bindings, which the lifespan
never reads). Its stores and clients are replaced with fakes on that
application's resources; every process-wide collaborator the lifespan touches
is replaced with a fake on its owning module (the singletons
``tests/conftest.py`` snapshots). ``KUBECONFIG`` points nowhere, so no path
can reach a real cluster or database.
"""

from __future__ import annotations

import asyncio
import contextlib
import functools
import importlib
import inspect
import os
import sys
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import FastAPI

os.environ.setdefault("VECTOR_DB_URL", "postgresql://test@localhost/test")

from orchestrator.application import (
    build_application_resources,
    lifecycle,
)
from orchestrator.application import (
    controls as controls_composition,
)
from orchestrator.application import (
    jobs as jobs_composition,
)
from orchestrator.application.resources import ApplicationResources  # noqa: E402
from orchestrator.services import (
    default_experts as default_experts_module,  # noqa: E402
)
from orchestrator.services import job_dispatcher as job_dispatcher_module  # noqa: E402
from orchestrator.services import lifecycle as instance_lifecycle_module
from orchestrator.services import readiness as readiness_module  # noqa: E402
from orchestrator.services.cloud import (  # noqa: E402
    instance_registry as instance_registry_module,
)

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


def _application() -> FastAPI:
    """A fresh application with the resources ``create_app()`` builds.

    Only the lifespan runs here, and it reads nothing but
    ``app.state.resources``; the routers and the process-wide bindings
    ``create_app()`` also installs (auth provisioning backends, the VM
    authority routers) are left alone so no other test sees this application.
    """

    app = FastAPI(lifespan=lifecycle.lifespan)
    app.state.resources = build_application_resources()
    return app


# (owner module, attribute) of every process-wide collaborator the lifespan
# reads, with the fake's recorded async/sync methods and plain attributes.
_OWNER_FAKES = (
    (
        "orchestrator.services.nats_bridge",
        "nats_bridge",
        ("connect", "disconnect"),
        (),
        {"lifecycle_identity_authenticated": False},
    ),
    (
        "orchestrator.services.snapshot_service",
        "snapshot_service",
        ("connect",),
        (),
        {"is_available": False},
    ),
    (
        "orchestrator.services.vm_provisioner",
        "vm_provisioner",
        ("disconnect",),
        ("connect",),
        {"mode": "off", "is_available": False},
    ),
    (
        "orchestrator.services.container_provisioner",
        "container_provisioner",
        (),
        ("connect",),
        {"is_available": False, "in_cluster": False},
    ),
    (
        "orchestrator.services.docker_provisioner",
        "docker_provisioner",
        (),
        ("connect",),
        {"is_available": False, "workspace_hosts": []},
    ),
    ("orchestrator.services.ide_session", "ide_session_service", (), ("connect",), {}),
    (
        "orchestrator.services.persistent_provisioner",
        "persistent_provisioner",
        (),
        ("connect",),
        {"is_available": False},
    ),
    (
        "orchestrator.services.agent_provisioner",
        "agent_provisioner",
        (),
        ("connect",),
        {"is_available": False, "_k8s_available": False},
    ),
    (
        "orchestrator.services.workspace_suspension",
        "workspace_suspension_service",
        (),
        ("connect",),
        {},
    ),
    ("orchestrator.services.ide_proxy", "ide_proxy_service", (), ("connect",), {}),
    (
        "orchestrator.services.notification_service",
        "notification_service",
        (),
        ("connect",),
        {},
    ),
    (
        "orchestrator.services.imap_poller",
        "imap_poller",
        (),
        ("connect",),
        {"is_available": False},
    ),
    ("orchestrator.services.sudo_gate", "sudo_gate", (), ("connect",), {}),
)


def _owner(attribute: str) -> Any:
    """The module that owns the process-wide collaborator ``attribute``."""

    for module_name, name, *_ in _OWNER_FAKES:
        if name == attribute:
            return importlib.import_module(module_name)
    raise KeyError(attribute)


@contextlib.contextmanager
def _lifespan_environment(
    monkeypatch, recorder: _Recorder, *, env=None, app=None, settings=None
):
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

    app = app if app is not None else _application()
    resources: ApplicationResources = app.state.resources
    for key, value in (settings or {}).items():
        monkeypatch.setattr(resources.settings, key, value)

    store = _FakeStore(log)
    vector = _FakeStore(log, "vector_db")
    monkeypatch.setattr(resources, "postgres_db", store)
    monkeypatch.setattr(resources, "vector_db", vector)
    monkeypatch.setattr(resources, "audit_db", None)
    monkeypatch.setattr(
        resources,
        "audit_store",
        _fake("audit_store", log, async_methods=("connect", "disconnect")),
    )
    monkeypatch.setattr(resources, "audit_reader", resources.audit_store)
    monkeypatch.setattr(
        resources,
        "gitea_client",
        _fake(
            "gitea_client",
            log,
            async_methods=("ensure_initialized", "ensure_oidc_configured", "close"),
        ),
    )
    monkeypatch.setattr(
        resources,
        "keycloak_groups",
        _fake("keycloak_groups", log, async_methods=("ensure_initialized",)),
    )
    for module_name, singleton, async_methods, sync_methods, attrs in _OWNER_FAKES:
        monkeypatch.setattr(
            importlib.import_module(module_name),
            singleton,
            _fake(
                singleton,
                log,
                async_methods=async_methods,
                sync_methods=sync_methods,
                **attrs,
            ),
        )
    _owner("agent_provisioner").agent_provisioner.list_pods = AsyncMock(return_value=[])

    # Startup steps that would otherwise reach real services or schemas.
    import orchestrator.services.manifest_experts as manifest_experts
    import orchestrator.services.manifest_projects as manifest_projects

    monkeypatch.setattr(manifest_experts, "migrate_stored_experts", AsyncMock())
    monkeypatch.setattr(manifest_experts, "seed_bundled_expert_manifests", AsyncMock())
    monkeypatch.setattr(manifest_experts, "installed_srw_image", lambda: None)
    monkeypatch.setattr(manifest_projects, "migrate_projects", AsyncMock())
    monkeypatch.setattr(
        default_experts_module,
        "seed_managed_default_experts",
        AsyncMock(return_value={"worker": None, "session": None}),
    )
    monkeypatch.setattr(readiness_module, "try_auto_pin_required_defaults", AsyncMock())
    # The capability probe is bound where the metering bootstrap that calls it
    # lives (R1.B11 moved the startup step to its domain).
    import orchestrator.services.infrastructure_metering.bootstrap as metering

    monkeypatch.setattr(
        metering,
        "probe_schema_capabilities",
        AsyncMock(return_value=_NoCapabilities()),
    )
    monkeypatch.setattr(
        instance_registry_module,
        "initialize_main_cloud_instance_authority",
        AsyncMock(),
    )
    monkeypatch.setattr(
        instance_registry_module, "preload_retained_main_cloud_instances", AsyncMock()
    )
    import orchestrator.seed.llm_config as llm_config

    monkeypatch.setattr(
        llm_config, "ensure_tavily_search_endpoint", AsyncMock(return_value=False)
    )
    monkeypatch.setattr(
        llm_config, "ensure_elevenlabs_tts_endpoint", AsyncMock(return_value=False)
    )

    real_drain = resources.kb_datasource_tasks.drain

    async def _drain():
        log.append("kb_datasource_tasks.drain")
        await real_drain()

    monkeypatch.setattr(resources.kb_datasource_tasks, "drain", _drain)

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
        yield SimpleNamespace(
            app=app, resources=resources, store=store, vector=vector, log=log
        )
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


def _run_lifespan(
    monkeypatch, recorder, *, env=None, inside=None, app=None, settings=None
):
    """Enter ``lifecycle.lifespan`` once, over ``app`` or a fresh application.

    ``settings`` replaces deployment gates on that application's own
    ``DeploymentSettings`` for the run.
    """

    async def _go():
        with _lifespan_environment(
            monkeypatch, recorder, env=env, app=app, settings=settings
        ) as world:
            async with lifecycle.lifespan(world.app):
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
    await _run_lifespan(
        monkeypatch, recorder, settings={"completion_commands_enabled": True}
    )
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
    world = await _run_lifespan(monkeypatch, first)
    first_event = world.resources.shutdown_event
    second = _Recorder()
    # The same application, started a second time.
    await _run_lifespan(monkeypatch, second, app=world.app)

    assert _creation(first) == _creation(second) == DEFAULT_CREATION
    assert first.awaited == second.awaited == DEFAULT_SHUTDOWN
    assert world.resources.shutdown_event is not first_event
    assert first_event.is_set()


@pytest.mark.asyncio
async def test_preflight_refusal_happens_before_any_resource_is_acquired(
    monkeypatch,
):
    recorder = _Recorder()
    monkeypatch.setattr(sys, "exit", MagicMock(side_effect=SystemExit(1)))
    with pytest.raises(SystemExit):
        await _run_lifespan(
            monkeypatch,
            recorder,
            settings={
                "completion_commands_enabled": False,
                "completion_status_reorder_enabled": True,
            },
        )
    assert recorder.created == []
    assert "postgres_db.connect" not in recorder.events
    # Nothing was acquired, so nothing is unwound either.
    assert "postgres_db.disconnect" not in recorder.events


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
async def test_shutdown_still_stops_every_task_and_closes_pools_after_a_failure(
    monkeypatch,
):
    recorder = _Recorder()
    recorder.fail_labels = {"stale_agent_detector"}
    # The failure still surfaces from shutdown, after everything else closed.
    with pytest.raises(RuntimeError, match="stale_agent_detector ended with an error"):
        await _run_lifespan(monkeypatch, recorder)
    assert recorder.awaited == DEFAULT_SHUTDOWN
    assert _closure(recorder) == SHUTDOWN_CLOSURE


@pytest.mark.asyncio
async def test_startup_failure_stops_started_tasks_and_releases_pools(monkeypatch):
    recorder = _Recorder()

    class _Boom(RuntimeError):
        pass

    def _explode(*_a, **_k):
        raise _Boom("lifecycle reconciler construction failed")

    monkeypatch.setattr(
        instance_lifecycle_module, "InstanceLifecycleReconciler", _explode
    )
    with pytest.raises(_Boom):
        await _run_lifespan(monkeypatch, recorder)
    started = [e["label"] for e in recorder.created]
    assert started, "the failure must come after tasks were started"
    stopped = set(recorder.awaited) | set(recorder.cancelled)
    assert stopped == set(started)
    assert "postgres_db.disconnect" in recorder.events
    assert "vector_db.disconnect" in recorder.events


@pytest.mark.asyncio
async def test_triggered_dispatch_is_stopped_before_the_pools_close(monkeypatch):
    from orchestrator.services import leader_election

    recorder = _Recorder()
    monkeypatch.setattr(leader_election.is_leader, "is_set", lambda: True)

    async def _inside(world):
        job_dispatcher_module.trigger_dispatch(
            dependencies=jobs_composition.job_dispatch_dependencies(world.resources)
        )

    await _run_lifespan(
        monkeypatch,
        recorder,
        inside=_inside,
        settings={"auto_assign_enabled": True},
    )
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


# --------------------------------------------------------------------------- #
# Bindings: the moved bodies receive this application's own collaborators
# --------------------------------------------------------------------------- #


def _binding(operation: Any) -> tuple[Any, Any, Any]:
    """What a ``resources.bound`` callable binds: (operation, factory, resources)."""

    closure = inspect.getclosurevars(operation).nonlocals
    return closure["operation"], closure["dependencies"], closure["resources"]


def _keywords(entry: dict[str, Any]) -> dict[str, Any]:
    target = entry["target"]
    if isinstance(target, functools.partial):
        return dict(target.keywords)
    return {
        key: value
        for key, value in entry["arguments"].items()
        if key not in {"shutdown_event", "stop", "ev", "se", "shutdown"}
    }


@pytest.mark.asyncio
async def test_moved_loops_receive_the_application_collaborators(monkeypatch):
    recorder = _Recorder()
    names = (
        "agent_provisioner",
        "persistent_provisioner",
        "container_provisioner",
        "ide_session_service",
        "imap_poller",
        "snapshot_service",
        "workspace_suspension_service",
        "main_cloud_router",
        "sudo_gate",
    )
    during: dict[str, Any] = {}

    async def _snapshot(world):
        # The environment restores the real singletons on exit; compare with
        # the collaborators that were live while the lifespan ran. The main
        # cloud router is the application's own; the rest are process-wide
        # singletons read from their owning modules.
        during.update(
            {
                name: (
                    world.resources.main_cloud_router
                    if name == "main_cloud_router"
                    else getattr(_owner(name), name)
                )
                for name in names
            }
        )

    world = await _run_lifespan(monkeypatch, recorder, inside=_snapshot)
    resources = world.resources
    live = SimpleNamespace(**during)
    created = {entry["label"]: entry for entry in recorder.created}

    dispatch = _keywords(created["auto_assign_dispatcher"])["dependencies"]
    assert dispatch.store is world.store
    assert dispatch.state is resources.job_dispatch_state
    assert dispatch.agent_provisioner is live.agent_provisioner
    # The pause-pending set preemption fills is the one delivery discards from.
    assert (
        controls_composition.job_delivery_operations(
            resources
        ).dependencies.pause_pending_job_ids
        is resources.job_dispatch_state.pause_pending_job_ids
    )

    detector = _keywords(created["stale_agent_detector"])["dependencies"]
    assert detector.store is world.store
    # The detector's dispatch trigger is this application's: the dispatcher's
    # own ``trigger_dispatch`` bound to the dispatch dependencies it builds
    # from these resources.
    assert _binding(detector.trigger_dispatch) == (
        job_dispatcher_module.trigger_dispatch,
        jobs_composition.job_dispatch_dependencies,
        resources,
    )
    # The retirement operations are providers recomposed per call from this
    # application's resources.
    for provider, composition in (
        (
            detector.pinned_retirement_operations,
            controls_composition.pinned_retirement_operations,
        ),
        (
            detector.thread_retirement_operations,
            controls_composition.thread_retirement_operations,
        ),
    ):
        assert isinstance(provider, functools.partial)
        assert provider.func is composition
        assert len(provider.args) == 1 and provider.args[0] is resources
        assert not provider.keywords

    for label in (
        "pinned_agent_create_intent_reconciler",
        "pinned_k8s_create_fence_gc_sweeper",
    ):
        dependencies = _keywords(created[label])["dependencies"]
        assert dependencies.store is world.store
        assert dependencies.persistent_provisioner is live.persistent_provisioner

    for label in (
        "thread_events_prune_sweeper",
        "security_events_prune_sweeper",
        "ssh_attachments_prune_sweeper",
    ):
        assert _keywords(created[label])["store"] is world.store

    assert _keywords(created["agent_pool_reconciler"])["provisioner"] is (
        live.agent_provisioner
    )
    assert _keywords(created["ide_session_ttl_sweeper"])["ide_sessions"] is (
        live.ide_session_service
    )
    assert _keywords(created["imap_poll_loop"])["poller"] is live.imap_poller
    assert _keywords(created["snapshot_gc_sweeper"])["snapshots"] is (
        live.snapshot_service
    )
    ide_settings = _keywords(created["code_server_settings_sweeper"])
    assert ide_settings["db"] is world.store
    assert ide_settings["container_provisioner"] is live.container_provisioner
    workspace_idle = _keywords(created["workspace_idle_sweeper"])
    assert workspace_idle["store"] is world.store
    assert workspace_idle["suspension"] is live.workspace_suspension_service
    ro_reader = _keywords(created["ro_reader_reconciler_loop"])
    assert ro_reader["store"] is world.store
    assert ro_reader["router"]() is live.main_cloud_router
    sudo = _keywords(created["sudo_expiration_sweeper"])
    assert sudo["gate"] is live.sudo_gate


@pytest.mark.asyncio
async def test_a_failed_first_connect_unwinds_without_masking_the_error(monkeypatch):
    recorder = _Recorder()

    async def _refused():
        raise ConnectionRefusedError("app database unreachable")

    async def _inside(_world):  # pragma: no cover - startup never completes
        raise AssertionError("startup must not complete")

    with _lifespan_environment(monkeypatch, recorder) as world:
        world.store.connect = _refused
        with pytest.raises(ConnectionRefusedError, match="unreachable"):
            async with lifecycle.lifespan(world.app):
                await _inside(world)
    assert recorder.created == []
    # The ordinary closure ran; each close was a no-op on an unopened client.
    assert _closure(recorder) == SHUTDOWN_CLOSURE


@pytest.mark.asyncio
async def test_a_failing_cleanup_does_not_replace_the_startup_error(monkeypatch):
    recorder = _Recorder()

    class _Boom(RuntimeError):
        pass

    def _explode(*_a, **_k):
        raise _Boom("lifecycle reconciler construction failed")

    monkeypatch.setattr(
        instance_lifecycle_module, "InstanceLifecycleReconciler", _explode
    )
    recorder.fail_labels = {"stale_agent_detector"}
    with pytest.raises(_Boom):
        await _run_lifespan(monkeypatch, recorder)
    assert "postgres_db.disconnect" in recorder.events
