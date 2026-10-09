"""The connector registry keeps the runtime order of every entry point.

Recording materializers stand in for the real ones; each test drives a real
entry point (the worker's job setup, session attach, the live update and the
backend swap) and checks the order the materializers were called in. The
orders are the ones the agent kept before materializers existed
(connector_drivers.md lane 1 §3.4).
"""

from __future__ import annotations

import asyncio
import inspect
import threading
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import agent.connectors.registry as registry_module
from agent.connectors import RuntimeContext, deliveries_from_payload
from agent.connectors.registry import (
    BACKEND_SWAP_ORDER,
    LIVE_ORDER,
    RELEASE_ORDER,
    SESSION_HARNESS_ORDER,
    SESSION_WORKSPACE_ORDER,
    WORKER_ORDER,
    ConnectorRegistry,
)
from shared.connectors.contract import DELIVERY_FORMS

#: One connector of every agent-side delivery form, in payload order.
PAYLOAD = [
    {"type": "generic", "name": "Env", "credentials": {"env_vars": {"A": "1"}}},
    {"type": "postgresql", "name": "PG", "connection_url": "postgresql://db/x"},
    {"type": "mcp", "name": "Docs", "credentials": {"transport": "http"}},
    {"type": "repository", "name": "Repo", "connection_url": "https://h/o/r.git"},
    {"type": "kubeconfig", "name": "Kube", "credentials": {"files": []}},
    {"type": "ssh_key", "name": "Key", "credentials": {"files": []}},
    {"type": "kb", "name": "KB", "datasource_id": "kb-1"},
    {
        "type": "lease_probe",
        "name": "Lease",
        "datasource_id": "00000000-0000-4000-8000-0000000000c3",
        "credentials": {
            "lease": {
                "id": "00000000-0000-4000-8000-0000000000c2",
                "connector_id": "00000000-0000-4000-8000-0000000000c3",
                "token": "scl_test",
            }
        },
    },
]
AGENT_FORMS = (
    "env_file",
    "lease_token",
    "credential_file",
    "checkout",
    "ssh_identity",
    "managed_connection",
    "mcp_client",
    "knowledge_index",
)


class _Recorder:
    def __init__(self, form: str, log: list):
        self.form = form
        self.log = log

    def _names(self, deliveries):
        return [delivery.entry["name"] for delivery in deliveries]

    def materialize(self, deliveries, rt):
        self.log.append((self.form, "materialize", self._names(deliveries)))

    def replace(self, old, new, rt):
        self.log.append((self.form, "replace", self._names(new)))

    def on_backend_swap(self, deliveries, backend):
        self.log.append((self.form, "on_backend_swap", self._names(deliveries)))

    def release(self, rt):
        self.log.append((self.form, "release", []))

    def facts(self, deliveries, rt):
        return []


class _ReadyRecorder(_Recorder):
    async def ready(self, rt):
        self.log.append((self.form, "ready", []))


class _KnowledgeRecorder(_Recorder):
    def bindings(self, deliveries, *, project_ids, runtime_actor):
        self.log.append((self.form, "bindings", self._names(deliveries)))
        return []


class _StagedRecorder(_Recorder):
    """The checkout's live change: begun, staged, then swapped in (recorded
    as its ``replace`` at the moment it takes effect)."""

    def begin_replace(self, old, new, rt):
        pass

    def stage_replace(self, old, new, rt, cancel=None):
        return lambda: self.replace(old, new, rt)


def _recording_registry(log: list) -> ConnectorRegistry:
    kinds = {
        "mcp_client": _ReadyRecorder,
        "knowledge_index": _KnowledgeRecorder,
        "checkout": _StagedRecorder,
    }
    return ConnectorRegistry(
        kinds.get(form, _Recorder)(form, log) for form in AGENT_FORMS
    )


@pytest.fixture
def log(monkeypatch):
    calls: list = []
    monkeypatch.setattr(registry_module, "_REGISTRY", _recording_registry(calls))
    return calls


def _materialized(log):
    return [form for form, step, _ in log if step == "materialize"]


# =============================================================================
# The orders themselves
# =============================================================================


def test_every_agent_form_has_a_materializer():
    registry = ConnectorRegistry.default()
    assert set(registry.by_form) == set(AGENT_FORMS)
    # The generic-hosting forms are delivered into a pod, never by the agent.
    assert set(DELIVERY_FORMS) - set(AGENT_FORMS) == {"pod_env", "pod_file"}


def test_the_orders_are_the_runtime_orders():
    assert WORKER_ORDER == (
        "env_file",
        "lease_token",
        "managed_connection",
        "mcp_client",
        "checkout",
        "credential_file",
    )
    assert SESSION_HARNESS_ORDER == ("managed_connection", "mcp_client")
    # Credential files reach the session workspace too (slice D1d), after
    # the checkouts as on a worker; lease tokens (C2) before them.
    assert SESSION_WORKSPACE_ORDER == (
        "env_file",
        "lease_token",
        "checkout",
        "credential_file",
    )
    assert LIVE_ORDER == (
        "env_file",
        "lease_token",
        "ssh_identity",
        "knowledge_index",
        "managed_connection",
        "mcp_client",
        "checkout",
        "credential_file",
    )
    assert BACKEND_SWAP_ORDER == (
        "env_file",
        "lease_token",
        "checkout",
        "credential_file",
    )
    # What lives in the workspace lives as long as it does.
    assert RELEASE_ORDER == ("managed_connection",)


def test_a_form_has_one_materializer():
    with pytest.raises(ValueError, match="two materializers"):
        ConnectorRegistry([_Recorder("env_file", []), _Recorder("env_file", [])])


@pytest.mark.asyncio
async def test_each_materializer_sees_only_its_routed_deliveries(log):
    await registry_module.connector_registry().setup_worker(
        deliveries_from_payload(PAYLOAD), RuntimeContext(execution="worker")
    )
    routed = {form: names for form, step, names in log if step == "materialize"}
    assert routed == {
        "env_file": ["Env"],
        "lease_token": ["Lease"],
        "managed_connection": ["PG"],
        "mcp_client": ["Docs"],
        "checkout": ["Repo"],
        "credential_file": ["Kube"],
    }


# =============================================================================
# Entry point 1: worker setup (UniversalAgent._setup_job_tools)
# =============================================================================


@pytest.mark.asyncio
async def test_worker_setup_runs_the_worker_order(log):
    from agent.agent import UniversalAgent

    class Stop(Exception):
        pass

    agent = object.__new__(UniversalAgent)
    agent._current_job_id = "00000000-0000-0000-0000-0000000000a1"
    agent._job_metadata = {"datasources": PAYLOAD}
    agent._workspace_manager = MagicMock()
    agent._datasource_connections = {}
    agent._datasource_clients = {}
    agent.config = SimpleNamespace(display_name="Expert")

    def readme(*args, **kwargs):
        log.append(("readme", "inject", []))
        raise Stop

    with (
        patch(
            "agent.tools.registry.register_mcp_tools",
            side_effect=lambda manager: log.append(("mcp_tools", "register", [])),
        ),
        patch("agent.core.datasource_setup.inject_workspace_facts", readme),
        pytest.raises(Stop),
    ):
        await UniversalAgent._setup_job_tools(agent)

    assert [(form, step) for form, step, _ in log] == [
        ("env_file", "materialize"),
        ("lease_token", "materialize"),
        ("managed_connection", "materialize"),
        ("mcp_client", "materialize"),
        ("mcp_client", "ready"),
        ("checkout", "materialize"),
        ("credential_file", "materialize"),
        ("mcp_tools", "register"),
        ("readme", "inject"),
    ]


@pytest.mark.parametrize(
    ("entry", "expected"),
    [
        ("setup_worker", ["checkout"]),
        (
            "attach_workspace",
            ["env_file", "lease_token", "checkout", "credential_file"],
        ),
    ],
)
@pytest.mark.asyncio
async def test_setting_up_an_execution_clones_off_the_event_loop(
    log, monkeypatch, entry, expected
):
    """A first clone through the git swap driver waits minutes for a
    starting pod (C3), so the checkout runs in a worker thread wherever an
    execution is set up."""
    offloaded = []
    real_to_thread = asyncio.to_thread

    async def to_thread(func, *args, **kwargs):
        offloaded.append(getattr(func, "__self__", None).form)
        return await real_to_thread(func, *args, **kwargs)

    monkeypatch.setattr(registry_module.asyncio, "to_thread", to_thread)
    await getattr(registry_module.connector_registry(), entry)(
        deliveries_from_payload(PAYLOAD), RuntimeContext(execution="worker")
    )
    assert offloaded == expected


@pytest.mark.parametrize("entry", ["setup_worker", "attach_workspace"])
@pytest.mark.asyncio
async def test_a_waiting_checkout_leaves_the_event_loop_running(entry):
    """The C0 stateless gate's regression: a token repository whose driver
    never started held the loop for the whole wait (210 s), so the worker's
    lease heartbeat never ran and the unit was parked. Here the checkout
    waits for a callback the loop runs: inline, it waits out its timeout."""
    loop = asyncio.get_running_loop()
    loop_ran = threading.Event()

    class WaitingCheckout(_Recorder):
        def materialize(self, deliveries, rt):
            loop.call_soon_threadsafe(loop_ran.set)
            self.log.append((self.form, "materialize", loop_ran.wait(timeout=5)))

    calls: list = []
    registry = ConnectorRegistry(
        (WaitingCheckout if form == "checkout" else _Recorder)(form, calls)
        for form in AGENT_FORMS
    )
    await getattr(registry, entry)(
        deliveries_from_payload(PAYLOAD), RuntimeContext(execution="worker")
    )
    assert ("checkout", "materialize", True) in calls


def test_worker_release_closes_the_connections_only(log):
    """The workspace's files outlive the shell the release follows."""
    from agent.agent import UniversalAgent

    agent = object.__new__(UniversalAgent)
    agent._knowledge_graph = None
    agent._datasource_connections = {"postgresql": MagicMock()}
    agent._datasource_clients = {}

    UniversalAgent._close_datasource_connections(agent)

    assert [(form, step) for form, step, _ in log] == [
        ("managed_connection", "release"),
    ]
    assert agent._datasource_connections == {}


def test_workspace_init_loads_identities_after_the_managed_repository():
    """C1's position: identities load with the workspace, before any clone."""
    from agent.agent import UniversalAgent
    from agent.api.persistent_session import PersistentSession

    for source, offload in (
        (inspect.getsource(UniversalAgent._setup_job_workspace), ""),
        (inspect.getsource(PersistentSession._setup_workspace), "offload=True"),
    ):
        managed = source.index("materialize_managed_repository_credentials(")
        identities = source.index("initialize_workspace(")
        assert managed < identities
        assert offload in source[identities : identities + 80]


@pytest.mark.asyncio
async def test_workspace_init_runs_only_the_identity_form(log):
    await registry_module.connector_registry().initialize_workspace(
        RuntimeContext(execution="session"), offload=True
    )
    assert log == [("ssh_identity", "materialize", [])]


# =============================================================================
# Entry point 2: session attach, harness then workspace (session_attach)
# =============================================================================


@pytest.mark.asyncio
async def test_session_attach_runs_the_harness_phase_then_the_workspace_phase(
    log,
):
    import agent.api.persistent_app as mod
    from agent.api import session_workspace

    class FakeSession:
        def __init__(self, *args, **kwargs):
            self.cloud_mount_manager = None
            self.cloud_mount_error = None
            self.overlay_mount_manager = None
            self.workspace_manager = SimpleNamespace(
                path=Path("/workspace"), backend=MagicMock()
            )
            self.workspace_ssh_identity_status = {}
            self.config = SimpleNamespace(display_name="A")
            self.workspace_sync = None
            self.postgres_conn = None
            self.tool_context = None
            log.append(("session", "construct", []))

        async def setup(self, **kwargs):
            log.append(("workspace", "initialize", []))

        async def recover_subagents(self):
            return None

    workspace_override = {
        "remote": {"host": "10.42.0.10"},
        "datasources": [dict(entry) for entry in PAYLOAD],
    }
    from shared.runtime.core.loader import load_agent_config_from_dict

    fake_agent = SimpleNamespace(
        # A real config: the connectors' tool categories ride config_override.
        config=load_agent_config_from_dict({"agent_id": "a", "display_name": "A"}),
        _tactical_llm=None,
        _llm=object(),
        _auxiliary_llm=object(),
        postgres_conn=None,
        vector_conn=None,
    )
    fake_orchestrator = SimpleNamespace(
        get_thread_workspace=AsyncMock(return_value=workspace_override)
    )

    mod._session = None
    mod._session_identity._thread_id = None
    with (
        patch.object(mod, "_agent", fake_agent),
        patch.object(mod, "_orchestrator_client", fake_orchestrator),
        patch.object(mod, "PersistentSession", FakeSession),
        patch.object(
            session_workspace,
            "poll_workspace_ready",
            new=AsyncMock(return_value=workspace_override),
        ),
        patch.object(mod, "_build_sync_coordinator"),
        patch.object(mod, "_restore_session_messages", new=AsyncMock()),
        patch.object(mod, "_update_thread_status", new=AsyncMock()),
        patch.object(mod._session_termination, "start_watchdogs"),
        patch(
            "agent.core.datasource_setup.inject_workspace_facts",
            side_effect=lambda *a, **k: log.append(("readme", "inject", [])),
        ),
    ):
        try:
            await mod._session_attach.attach("thread-1")
        finally:
            mod._session = None
            mod._session_identity._thread_id = None

    assert [(form, step) for form, step, _ in log] == [
        ("managed_connection", "materialize"),
        ("mcp_client", "materialize"),
        ("mcp_client", "ready"),
        ("knowledge_index", "bindings"),
        ("session", "construct"),
        ("workspace", "initialize"),
        ("env_file", "materialize"),
        ("lease_token", "materialize"),
        ("checkout", "materialize"),
        ("credential_file", "materialize"),
        ("readme", "inject"),
    ]


# =============================================================================
# Entry point 3: live update (PersistentSession.resetup_datasources)
# =============================================================================


def _live_session(datasource_configs):
    from agent.api.persistent_session import PersistentSession
    from shared.runtime.core.loader import ToolsConfig

    config = MagicMock()
    config.tools = ToolsConfig()
    config.extra = {}
    session = PersistentSession(
        thread_id="00000000-0000-0000-0000-0000000000b1",
        config=config,
        datasources={},
        _datasource_clients={},
        datasource_configs=datasource_configs,
    )
    session.tool_context = SimpleNamespace(datasources=session.datasources)
    session.workspace_manager = MagicMock()
    session.workspace_manager.source_repos = {}
    session.resetup_tools_for_backend = MagicMock()
    return session


@pytest.mark.asyncio
async def test_live_update_swaps_the_harness_before_the_checkouts(log):
    session = _live_session([dict(PAYLOAD[0])])
    original = registry_module.ConnectorRegistry.replace_live

    async def replace_live(self, old, new, rt, *, on_harness_replaced):
        def hook(connections, clients):
            log.append(("harness", "swap", []))
            on_harness_replaced(connections, clients)

        await original(self, old, new, rt, on_harness_replaced=hook)

    with (
        patch.object(registry_module.ConnectorRegistry, "replace_live", replace_live),
        patch(
            "agent.core.datasource_setup.inject_workspace_facts",
            side_effect=lambda *a, **k: log.append(("readme", "inject", [])),
        ),
        patch("agent.tools.registry.register_mcp_tools"),
    ):
        await session.resetup_datasources([dict(entry) for entry in PAYLOAD])

    assert [(form, step) for form, step, _ in log] == [
        ("env_file", "replace"),
        ("lease_token", "replace"),
        ("ssh_identity", "replace"),
        ("knowledge_index", "replace"),
        ("managed_connection", "materialize"),
        ("mcp_client", "materialize"),
        ("mcp_client", "ready"),
        ("harness", "swap"),
        ("checkout", "replace"),
        ("credential_file", "replace"),
        ("readme", "inject"),
    ]
    session.resetup_tools_for_backend.assert_called_once()


@pytest.mark.asyncio
async def test_live_update_offloads_the_workspace_round_trips(log, monkeypatch):
    """The workspace round trips run in worker threads: the environment
    and identity steps as before, the checkouts' staging and the credential
    files; the checkouts' swap-in runs on the loop."""
    offloaded = []
    real_to_thread = asyncio.to_thread
    loop_thread = threading.get_ident()

    async def to_thread(func, *args, **kwargs):
        offloaded.append(getattr(func, "__self__", None).form)
        return await real_to_thread(func, *args, **kwargs)

    swapped_in_on = []
    original = _StagedRecorder.replace

    def replace(self, old, new, rt):
        swapped_in_on.append(threading.get_ident())
        original(self, old, new, rt)

    monkeypatch.setattr(registry_module.asyncio, "to_thread", to_thread)
    monkeypatch.setattr(_StagedRecorder, "replace", replace)
    await registry_module.connector_registry().replace_live(
        [],
        deliveries_from_payload(PAYLOAD),
        RuntimeContext(execution="session"),
        on_harness_replaced=lambda connections, clients: None,
    )
    assert offloaded == [
        "env_file",
        "lease_token",
        "ssh_identity",
        "checkout",
        "credential_file",
    ]
    assert swapped_in_on == [loop_thread]


@pytest.mark.asyncio
async def test_a_waiting_live_checkout_leaves_the_event_loop_running():
    """live_connector_add_clones_on_the_event_loop: a repository added to a
    running session clones (its git swap driver's pod may still be
    starting) while the loop runs on."""
    loop = asyncio.get_running_loop()
    loop_ran = threading.Event()

    class WaitingCheckout(_Recorder):
        def begin_replace(self, old, new, rt):
            pass

        def stage_replace(self, old, new, rt, cancel=None):
            loop.call_soon_threadsafe(loop_ran.set)
            waited = loop_ran.wait(timeout=5)
            return lambda: self.log.append((self.form, "replace", waited))

    calls: list = []
    registry = ConnectorRegistry(
        (WaitingCheckout if form == "checkout" else _Recorder)(form, calls)
        for form in AGENT_FORMS
    )
    await registry.replace_live(
        [],
        deliveries_from_payload(PAYLOAD),
        RuntimeContext(execution="session"),
        on_harness_replaced=lambda connections, clients: None,
    )
    assert ("checkout", "replace", True) in calls


@pytest.mark.asyncio
async def test_a_live_update_can_be_stopped_by_the_sessions_cleanup(monkeypatch):
    """The change runs with a cancel event the session holds while it runs
    (cleanup sets it; see test_persistent_session) and forgets after."""
    session = _live_session([])
    seen: list = []

    async def replace_live(self, old, new, rt, *, on_harness_replaced):
        seen.append((rt.cancel, session._live_change_cancel))

    monkeypatch.setattr(registry_module.ConnectorRegistry, "replace_live", replace_live)
    with patch("agent.core.datasource_setup.inject_workspace_facts"):
        await session.resetup_datasources([dict(PAYLOAD[3])])
    [(cancel, held)] = seen
    assert isinstance(cancel, threading.Event) and held is cancel
    assert not cancel.is_set() and session._live_change_cancel is None


@pytest.mark.asyncio
async def test_a_cancelled_live_update_sets_its_cancel_event():
    """Session end quiesces the update's task; the thread it awaits cannot
    be cancelled, so the registry sets the change's cancel event."""
    started = threading.Event()
    stopped: list[bool] = []

    class SlowCheckout(_StagedRecorder):
        def stage_replace(self, old, new, rt, cancel=None):
            started.set()
            stopped.append(cancel.wait(5))
            return lambda: None

    calls: list = []
    registry = ConnectorRegistry(
        (SlowCheckout if form == "checkout" else _Recorder)(form, calls)
        for form in AGENT_FORMS
    )
    rt = RuntimeContext(execution="session", cancel=threading.Event())
    task = asyncio.create_task(
        registry.replace_live(
            [],
            deliveries_from_payload(PAYLOAD),
            rt,
            on_harness_replaced=lambda connections, clients: None,
        )
    )
    assert await asyncio.to_thread(started.wait, 5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    for _ in range(100):
        if stopped:
            break
        await asyncio.sleep(0.01)
    assert stopped == [True] and rt.cancel.is_set()


@pytest.mark.asyncio
async def test_live_updates_apply_one_at_a_time(monkeypatch):
    """A second change waits for the first (whose checkout may clone for
    minutes off the loop), then diffs against what the first applied."""
    session = _live_session([])
    first_started = asyncio.Event()
    release_first = asyncio.Event()
    seen: list[tuple[list, list]] = []

    async def replace_live(self, old, new, rt, *, on_harness_replaced):
        seen.append(([d.name for d in old], [d.name for d in new]))
        if len(seen) == 1:
            first_started.set()
            await release_first.wait()

    monkeypatch.setattr(registry_module.ConnectorRegistry, "replace_live", replace_live)
    repo = dict(PAYLOAD[3])
    with patch("agent.core.datasource_setup.inject_workspace_facts"):
        first = asyncio.create_task(session.resetup_datasources([repo]))
        await first_started.wait()
        second = asyncio.create_task(session.resetup_datasources([]))
        await asyncio.sleep(0.05)
        assert len(seen) == 1  # the second waits
        release_first.set()
        await asyncio.gather(first, second)
    assert seen == [([], ["Repo"]), (["Repo"], [])]
    assert session.datasource_configs == []


# =============================================================================
# Entry point 4: backend swap (PersistentSession.swap_backend)
# =============================================================================


def test_backend_swap_delivers_the_workspace_forms_before_the_old_one_retires(
    log,
):
    session = _live_session([dict(entry) for entry in PAYLOAD])
    old_backend = MagicMock()
    old_backend.retire.side_effect = lambda: log.append(("backend", "retire", []))
    session.workspace_manager = MagicMock(backend=old_backend)
    new_backend = MagicMock()
    new_backend.is_connected.return_value = True

    with patch.object(session, "_setup_shell_manager"):
        session.swap_backend(new_backend)

    assert [(form, step) for form, step, _ in log] == [
        ("env_file", "on_backend_swap"),
        ("lease_token", "on_backend_swap"),
        # The git swap wiring of checkouts (C3) follows the workspace too.
        ("checkout", "on_backend_swap"),
        ("credential_file", "on_backend_swap"),
        ("backend", "retire"),
    ]
    assert log[0][2] == ["Env"]
    assert log[1][2] == ["Lease"]
    assert log[2][2] == ["Repo"]
    assert log[3][2] == ["Kube"]
