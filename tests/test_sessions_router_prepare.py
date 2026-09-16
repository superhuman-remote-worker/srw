"""Tests for POST /api/sessions/{tid}/prepare and GET /api/sessions/{tid}/connection."""

from datetime import datetime, timezone
import json
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import ValidationError
from shared.pinned_session_identity import PinnedSessionBinding


CONNECTION_THREAD_ID = "11111111-1111-4111-8111-111111111111"
CONNECTION_GENERATION = "22222222-2222-4222-8222-222222222222"
CONNECTION_AGENT_ID = "33333333-3333-4333-8333-333333333333"
CONNECTION_ATTACH_TOKEN = "44444444-4444-4444-8444-444444444444"
CONNECTION_POD_UID = "55555555-5555-4555-8555-555555555555"


def _connection_thread(
    *,
    lane: str = "pinned",
    agent_id: str | None = CONNECTION_AGENT_ID,
    user_id: str = "u1",
) -> dict:
    return {
        "id": CONNECTION_THREAD_ID,
        "execution_lane": lane,
        "status": "created",
        "user_id": user_id,
        "agent_id": agent_id,
        "runtime_generation": CONNECTION_GENERATION,
        "runtime_attach_token": (
            CONNECTION_ATTACH_TOKEN if agent_id is not None else None
        ),
        "runtime_retirement_token": None,
        "metadata": {},
    }


def _connection_agent(
    *,
    status: str = "ready",
    pod_ip: str | None = "10.0.0.5",
) -> dict:
    return {
        "id": CONNECTION_AGENT_ID,
        "thread_id": CONNECTION_THREAD_ID,
        "pod_ip": pod_ip,
        "pod_port": 8001,
        "status": status,
        "hostname": "srw-agent-x",
        "pod_uid": CONNECTION_POD_UID,
    }


def _connection_binding(
    *,
    status: str = "ready",
    pod_ip: str = "10.0.0.5",
    pod_port: int = 8001,
    hostname: str = "srw-agent-x",
    pod_uid: str = CONNECTION_POD_UID,
    agent_id: str = CONNECTION_AGENT_ID,
    attach_token: str = CONNECTION_ATTACH_TOKEN,
) -> PinnedSessionBinding:
    return PinnedSessionBinding(
        thread_id=CONNECTION_THREAD_ID,
        runtime_generation=CONNECTION_GENERATION,
        agent_id=agent_id,
        runtime_attach_token=attach_token,
        agent_hostname=hostname,
        pod_namespace="srw",
        pod_uid=pod_uid,
        pod_ip=pod_ip,
        pod_port=pod_port,
        agent_status=status,
    )


def _sequence_then_repeat(*rows):
    """AsyncMock side effect whose final authoritative row remains current."""

    iterator = iter(rows)
    last = rows[-1]

    def _next(*_args, **_kwargs):
        nonlocal last
        try:
            last = next(iterator)
        except StopIteration:
            pass
        return last

    return _next


def _install_fake_auth(monkeypatch, user_id: str = "u1") -> None:
    """Replace require_approved_user on the sessions module with a fake.

    The handlers call `require_approved_user(request, db)` inline rather than
    via Depends, so app.dependency_overrides won't intercept it — we patch the
    name on the importing module instead.
    """
    from orchestrator.routers import sessions as sessions_mod

    async def _fake(request, db):
        return {"id": user_id, "is_approved": True}

    monkeypatch.setattr(sessions_mod, "require_approved_user", _fake, raising=True)

    # Every connection path joins protected-reader readiness before lane or
    # transport discovery. ``_Ports`` defaults that port to immediately-ready;
    # individual protected tests override it on their own holder.


class _Ports:
    """Mutable stand-ins for the sessions router's explicit ports.

    R1.B06 replaced this router's late ``from orchestrator.main import ...``
    with a constructed ``SessionsDependencies``. Tests reassign a collaborator
    after construction, so ``dependencies`` forwards through this holder at
    call time rather than capturing the callables when it is built.
    """

    def __init__(self, store=None):
        self.store = store if store is not None else AsyncMock()
        self.agent_provisioner = MagicMock()
        self.container_provisioner = MagicMock()
        self.workspace_suspension_service = MagicMock()
        self.session_router = MagicMock()
        self.session_tokens = MagicMock()
        self.ensure_session_workspace = AsyncMock()
        # Attach gate: lets in-flight cloud session-folder provisioning land
        # before an agent binds (knowledge-base/knowledge/issues/
        # session_resume_cloud_sync_race_late_provision.md).
        self.await_late_cloud_setup = AsyncMock(return_value=None)
        self.await_protected_cloud_runtime_ready = AsyncMock(return_value=True)
        self.session_grant_violations = AsyncMock(return_value=[])
        self.session_endpoint_violations = AsyncMock(return_value=[])
        self.find_idle_persistent_agent = AsyncMock(return_value=None)
        self.send_session_attach = AsyncMock(return_value=True)

    @property
    def dependencies(self):
        from orchestrator.routers.sessions import SessionsDependencies

        holder = self
        return SessionsDependencies(
            store=holder.store,
            agent_provisioner=holder.agent_provisioner,
            container_provisioner=holder.container_provisioner,
            workspace_suspension_service=holder.workspace_suspension_service,
            session_router=holder.session_router,
            session_tokens=holder.session_tokens,
            ensure_session_workspace=(
                lambda *a, **k: holder.ensure_session_workspace(*a, **k)
            ),
            await_late_cloud_setup=(
                lambda *a, **k: holder.await_late_cloud_setup(*a, **k)
            ),
            await_protected_cloud_runtime_ready=(
                lambda *a, **k: holder.await_protected_cloud_runtime_ready(*a, **k)
            ),
            session_grant_violations=(
                lambda *a, **k: holder.session_grant_violations(*a, **k)
            ),
            session_endpoint_violations=(
                lambda *a, **k: holder.session_endpoint_violations(*a, **k)
            ),
            find_idle_persistent_agent=(
                lambda *a, **k: holder.find_idle_persistent_agent(*a, **k)
            ),
            send_session_attach=(lambda *a, **k: holder.send_session_attach(*a, **k)),
        )


def _fake_main(store=None) -> _Ports:
    """Build the router's ports without standing up the composition root."""
    return _Ports(store)


@pytest.fixture
def app(monkeypatch):
    """Minimal FastAPI app with the sessions router mounted."""
    from orchestrator.routers.sessions import router as sessions_router

    app = FastAPI()

    _install_fake_auth(monkeypatch)

    def _capture_background_task(coro):
        coro.close()
        return MagicMock()

    monkeypatch.setattr(
        "orchestrator.routers.sessions._schedule_prepare_task",
        _capture_background_task,
        raising=True,
    )

    fake_db = AsyncMock()
    fake_db.get_thread = AsyncMock(
        return_value={
            "id": "11111111-1111-4111-8111-111111111111",
            "execution_lane": "pinned",
            "status": "created",
            "user_id": "u1",
            "agent_id": None,
            "config_name": "persistent_defaults",
            "runtime_generation": "22222222-2222-4222-8222-222222222222",
            "runtime_retirement_token": None,
            "metadata": {},
        }
    )
    # advisory lock context manager
    lock_cm = AsyncMock()
    lock_cm.__aenter__.return_value = None
    lock_cm.__aexit__.return_value = False
    fake_db.thread_advisory_lock = MagicMock(return_value=lock_cm)

    # The router uses `from main import postgres_db` — patch the symbol on the
    # router module so the late import resolves to our fake.
    fake_main = _fake_main(fake_db)

    app.state.sessions_dependencies_factory = lambda: fake_main.dependencies
    app.include_router(sessions_router)
    return app, fake_db


def test_prepare_returns_202_immediately(app):
    """POST /prepare returns 202 with state=provisioning before any work."""
    fastapi_app, fake_db = app
    client = TestClient(fastapi_app)

    resp = client.post("/api/sessions/t1/prepare", json={})
    assert resp.status_code == 202
    assert resp.json() == {"state": "provisioning"}


def test_prepare_returns_404_for_missing_thread(app):
    """POST /prepare returns 404 if thread doesn't exist."""
    fastapi_app, fake_db = app
    fake_db.get_thread.return_value = None
    client = TestClient(fastapi_app)

    resp = client.post("/api/sessions/missing/prepare", json={})
    assert resp.status_code == 404


def test_prepare_returns_403_when_thread_owned_by_other_user(app):
    """POST /prepare returns 403 if caller is not the thread owner."""
    fastapi_app, fake_db = app
    fake_db.get_thread.return_value = {"id": "t1", "user_id": "OTHER", "agent_id": None}
    client = TestClient(fastapi_app)

    resp = client.post("/api/sessions/t1/prepare", json={})
    assert resp.status_code == 403


@pytest.mark.parametrize(
    "override",
    [
        {"workspace": {"backend": "vm"}},
        {"workspace": {"backend": "virtual"}},
        {"workspace": {"backend": "none"}},
        {"workspace": {"tier": "vm"}},
        {"agent": {"workspace": {"backend": "virtual"}}},
        {"officer": {"enabled": True}},
        {"agent": {"officer": {"enabled": True}}},
    ],
)
def test_protected_prepare_rejects_topology_override_before_scheduling(
    app, monkeypatch, override
):
    from orchestrator.routers import sessions as sessions_mod

    fastapi_app, fake_db = app
    thread = dict(fake_db.get_thread.return_value)
    thread["metadata"] = {
        "protected_cloud": True,
        "config_override": {"workspace": {"backend": "sandbox"}},
    }
    fake_db.get_thread.return_value = thread
    schedule = MagicMock()
    monkeypatch.setattr(sessions_mod, "_schedule_prepare_task", schedule)

    response = TestClient(fastapi_app).post(
        "/api/sessions/t1/prepare", json={"config_override": override}
    )

    assert response.status_code == 422
    assert response.json()["detail"]["code"] in {
        "protected_cloud_unsupported_workspace",
        "protected_cloud_unsupported_session_class",
    }
    schedule.assert_not_called()


def test_malformed_protected_prepare_refuses_before_scheduling(app, monkeypatch):
    from orchestrator.routers import sessions as sessions_mod

    fastapi_app, fake_db = app
    thread = dict(fake_db.get_thread.return_value)
    thread["metadata"] = {"protected_cloud": "true"}
    fake_db.get_thread.return_value = thread
    schedule = MagicMock()
    monkeypatch.setattr(sessions_mod, "_schedule_prepare_task", schedule)

    response = TestClient(fastapi_app).post(
        "/api/sessions/t1/prepare",
        json={"config_override": {"workspace": {"backend": "sandbox"}}},
    )

    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "protected_cloud_malformed"
    schedule.assert_not_called()


def test_ordinary_prepare_keeps_existing_workspace_and_officer_compatibility(
    app, monkeypatch
):
    from orchestrator.routers import sessions as sessions_mod

    fastapi_app, fake_db = app
    schedule = MagicMock(side_effect=lambda coro: coro.close())
    monkeypatch.setattr(sessions_mod, "_schedule_prepare_task", schedule)

    response = TestClient(fastapi_app).post(
        "/api/sessions/t1/prepare",
        json={
            "config_override": {
                "workspace": {"backend": "vm"},
                "officer": {"enabled": True},
            }
        },
    )

    assert response.status_code == 202
    schedule.assert_called_once()


@pytest.mark.parametrize("execution_lane", ["stateless", "future-lane"])
def test_prepare_refuses_every_non_pinned_lane(app, execution_lane):
    """Only the explicit pinned lane may enter pod provisioning."""
    fastapi_app, fake_db = app
    fake_db.get_thread.return_value = _connection_thread(
        lane=execution_lane,
        agent_id=None,
    )
    client = TestClient(fastapi_app)

    resp = client.post("/api/sessions/t1/prepare", json={})

    assert resp.status_code == 409
    assert "pinned provisioning" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_do_prepare_refuses_stateless_lane_before_provisioning(monkeypatch):
    """The background entry re-checks the lane in case it changed after POST."""

    from orchestrator.routers import sessions as sessions_mod

    db = AsyncMock()
    db.get_thread.return_value = _connection_thread(
        lane="stateless",
        agent_id=None,
    )
    lock_cm = AsyncMock()
    lock_cm.__aenter__.return_value = None
    lock_cm.__aexit__.return_value = False
    db.thread_advisory_lock = MagicMock(return_value=lock_cm)
    fake_main = _fake_main(db)
    provision = AsyncMock()
    monkeypatch.setattr(
        sessions_mod, "_provision_agent_for_thread", provision, raising=True
    )
    emitted: list[tuple[str, dict]] = []
    monkeypatch.setattr(
        sessions_mod,
        "lifecycle_emit",
        lambda _uid, _tid, state, **extra: emitted.append((state, extra)),
        raising=True,
    )

    await sessions_mod._do_prepare(
        thread_id=CONNECTION_THREAD_ID,
        user_id="u1",
        config_name="session_base",
        config_override=None,
        runtime_authority=sessions_mod.ThreadRuntimeAuthority(
            thread_id=CONNECTION_THREAD_ID,
            generation=CONNECTION_GENERATION,
        ),
        dependencies=fake_main.dependencies,
    )

    assert [state for state, _ in emitted] == ["provisioning", "failed"]
    assert "pinned provisioning" in emitted[-1][1]["reason"]
    provision.assert_not_awaited()


@pytest.mark.asyncio
async def test_provision_helper_refuses_stateless_lane(monkeypatch):
    """Direct callers cannot bypass the public and background gates."""
    from orchestrator.routers import sessions as sessions_mod

    db = AsyncMock()
    db.get_thread.return_value = {
        "id": "t1",
        "execution_lane": "stateless",
        "status": "created",
        "user_id": "u1",
    }
    fake_main = _fake_main(db)

    with pytest.raises(RuntimeError, match="pinned provisioning"):
        await sessions_mod._provision_agent_for_thread(
            thread_id="t1",
            config_name="session_base",
            config_override=None,
            dependencies=fake_main.dependencies,
        )


@pytest.mark.asyncio
async def test_provision_helper_suppresses_pod_fallback_after_lane_transition(
    monkeypatch,
):
    """A failed warm reservation must not use its stale pinned snapshot."""

    from orchestrator.routers import sessions as sessions_mod

    db = AsyncMock()
    db.get_thread = AsyncMock(
        side_effect=_sequence_then_repeat(
            {
                "id": "t1",
                "execution_lane": "pinned",
                "status": "created",
                "agent_id": None,
            },
            {
                "id": "t1",
                "execution_lane": "pinned",
                "status": "created",
                "agent_id": None,
            },
            {
                "id": "t1",
                "execution_lane": "stateless",
                "status": "created",
                "agent_id": None,
            },
        )
    )
    fake_main = _fake_main(db)

    fake_main.find_idle_persistent_agent = AsyncMock(
        return_value={
            "id": "a1",
            "pod_ip": "10.0.0.1",
            "pod_port": 8001,
        }
    )
    fake_main.send_session_attach = AsyncMock(return_value=False)
    fake_main.agent_provisioner.provision_agent = AsyncMock()

    with pytest.raises(RuntimeError, match="pinned provisioning"):
        await sessions_mod._provision_agent_for_thread(
            thread_id="t1",
            config_name="session_base",
            config_override=None,
            dependencies=fake_main.dependencies,
        )

    fake_main.agent_provisioner.provision_agent.assert_not_awaited()


@pytest.mark.parametrize("metadata", [{}, "{}", '{"protected_cloud": false}'])
@pytest.mark.asyncio
async def test_do_prepare_emits_phases_for_warm_thread(monkeypatch, metadata):
    """When the thread already has an agent_id and the pod is ready,
    _do_prepare skips the actual provision work, runs the readiness probe,
    calls session_router.ensure_route, and emits provisioning → booting →
    ready. "provisioning" is emitted up-front (before lock acquisition) so
    the cockpit's progress UI surfaces the phase even when /resume's
    sibling reprovision wins the race to bind the agent_id."""
    from orchestrator.routers import sessions as sessions_mod

    db = AsyncMock()
    # Already bound and ready.
    db.get_thread.return_value = {**_connection_thread(), "metadata": metadata}
    binding = _connection_binding(
        hostname="srw-agent-s-deadbeef",
        pod_uid="k8s-uid-1",
    )
    db.get_pinned_session_binding.return_value = binding
    lock_cm = AsyncMock()
    lock_cm.__aenter__.return_value = None
    lock_cm.__aexit__.return_value = False
    db.thread_advisory_lock = MagicMock(return_value=lock_cm)
    fake_main = _fake_main(db)

    # Mock the readiness probe to immediately return ready=true.
    observed_ready: dict = {}

    async def _ready_ok(pod_ip, pod_port, timeout_s, **kwargs):
        observed_ready.update(kwargs)
        return True

    monkeypatch.setattr(sessions_mod, "wait_for_ready", _ready_ok, raising=True)

    # Mock session_router on main.

    fake_main.session_router = AsyncMock()
    fake_main.session_router.ensure_route = AsyncMock(return_value="/p/t1")

    # Capture lifecycle emits at the call site inside sessions.py — patching
    # the bound `lifecycle_emit` name avoids the dual-module-path problem
    # (orchestrator.services.session_lifecycle vs services.session_lifecycle
    # under pytest's two-rooted sys.path).
    emit_calls: list[dict] = []

    def _capture_emit(user_id, thread_id, state, **extra):
        emit_calls.append(
            {"user_id": user_id, "thread_id": thread_id, "state": state, **extra}
        )

    monkeypatch.setattr(sessions_mod, "lifecycle_emit", _capture_emit, raising=True)

    await sessions_mod._do_prepare(
        thread_id=CONNECTION_THREAD_ID,
        user_id="u1",
        config_name="persistent_defaults",
        config_override=None,
        runtime_authority=sessions_mod.ThreadRuntimeAuthority(
            thread_id=CONNECTION_THREAD_ID,
            generation=CONNECTION_GENERATION,
        ),
        dependencies=fake_main.dependencies,
    )

    # Expected phase sequence: provisioning (up-front) → booting → ready.
    # Agent was already bound, but "provisioning" is still emitted so the
    # cockpit's resume card surfaces the phase regardless of who won the race.
    states = [c["state"] for c in emit_calls]
    assert states == ["provisioning", "booting", "ready"]

    # ensure_route called with correct pod info.
    fake_main.session_router.ensure_route.assert_called_once_with(
        thread_id=CONNECTION_THREAD_ID,
        pod_name="srw-agent-s-deadbeef",
        pod_uid="k8s-uid-1",
        runtime_generation=CONNECTION_GENERATION,
    )
    assert (
        observed_ready["expected_session_identity_fingerprint"]
        == binding.session_identity_fingerprint
    )
    assert observed_ready["require_protected_cloud"] is False


@pytest.mark.parametrize("mutation_phase", ["post_ready", "post_route"])
@pytest.mark.parametrize(
    "changed_binding",
    [
        _connection_binding(hostname="srw-agent-successor"),
        _connection_binding(pod_uid="successor-pod-uid"),
        _connection_binding(pod_ip="10.0.0.99"),
        _connection_binding(pod_port=9001),
        _connection_binding(agent_id="66666666-6666-4666-8666-666666666666"),
        _connection_binding(attach_token="77777777-7777-4777-8777-777777777777"),
        _connection_binding(status="offline"),
    ],
    ids=[
        "hostname",
        "pod_uid",
        "pod_ip",
        "pod_port",
        "agent_id",
        "attach",
        "offline_status",
    ],
)
@pytest.mark.asyncio
async def test_do_prepare_never_publishes_ready_for_a_changed_binding(
    monkeypatch,
    mutation_phase,
    changed_binding,
):
    """Readiness and routing for A cannot publish B as session-ready."""

    from orchestrator.routers import sessions as sessions_mod

    original = _connection_binding()
    db = AsyncMock()
    db.get_thread.return_value = _connection_thread()
    db.get_pinned_session_binding.side_effect = (
        [original, changed_binding]
        if mutation_phase == "post_ready"
        else [original, original, changed_binding]
    )
    lock_cm = AsyncMock()
    lock_cm.__aenter__.return_value = None
    lock_cm.__aexit__.return_value = False
    db.thread_advisory_lock = MagicMock(return_value=lock_cm)
    fake_main = _fake_main(db)
    monkeypatch.setattr(
        sessions_mod,
        "wait_for_ready",
        AsyncMock(return_value=True),
        raising=True,
    )

    fake_main.session_router = MagicMock()
    fake_main.session_router.ensure_route = AsyncMock(return_value="/p/t1")
    fake_main.session_router.teardown_route = AsyncMock(return_value=True)
    emitted: list[str] = []
    monkeypatch.setattr(
        sessions_mod,
        "lifecycle_emit",
        lambda _uid, _tid, state, **_extra: emitted.append(state),
        raising=True,
    )

    await sessions_mod._do_prepare(
        thread_id=CONNECTION_THREAD_ID,
        user_id="u1",
        config_name="persistent_defaults",
        config_override=None,
        runtime_authority=sessions_mod.ThreadRuntimeAuthority(
            thread_id=CONNECTION_THREAD_ID,
            generation=CONNECTION_GENERATION,
        ),
        dependencies=fake_main.dependencies,
    )

    assert emitted == ["provisioning", "booting"]
    if mutation_phase == "post_ready":
        fake_main.session_router.ensure_route.assert_not_awaited()
        fake_main.session_router.teardown_route.assert_not_awaited()
    else:
        fake_main.session_router.ensure_route.assert_awaited_once()
        fake_main.session_router.teardown_route.assert_awaited_once_with(
            CONNECTION_THREAD_ID,
            expected_namespace="srw",
            expected_runtime_generation=CONNECTION_GENERATION,
            expected_owner_uid=CONNECTION_POD_UID,
        )


@pytest.mark.parametrize("failure_phase", ["ensure_route", "final_binding_read"])
@pytest.mark.asyncio
async def test_do_prepare_cleans_partial_route_on_every_exception(
    monkeypatch,
    failure_phase,
):
    """Service-only creation or a failed final reread cannot strand a route."""

    from orchestrator.routers import sessions as sessions_mod

    original = _connection_binding()
    db = AsyncMock()
    db.get_thread.return_value = _connection_thread()
    db.get_pinned_session_binding.side_effect = (
        [original, original, RuntimeError("DB reread failed")]
        if failure_phase == "final_binding_read"
        else [original, original]
    )
    lock_cm = AsyncMock()
    lock_cm.__aenter__.return_value = None
    lock_cm.__aexit__.return_value = False
    db.thread_advisory_lock = MagicMock(return_value=lock_cm)
    fake_main = _fake_main(db)
    monkeypatch.setattr(
        sessions_mod,
        "wait_for_ready",
        AsyncMock(return_value=True),
        raising=True,
    )

    fake_main.session_router = MagicMock()
    fake_main.session_router.ensure_route = AsyncMock(
        side_effect=(
            RuntimeError("Ingress create failed")
            if failure_phase == "ensure_route"
            else None
        )
    )
    fake_main.session_router.teardown_route = AsyncMock(return_value=True)
    emitted: list[dict] = []
    monkeypatch.setattr(
        sessions_mod,
        "lifecycle_emit",
        lambda _uid, _tid, state, **extra: emitted.append({"state": state, **extra}),
        raising=True,
    )

    await sessions_mod._do_prepare(
        thread_id=CONNECTION_THREAD_ID,
        user_id="u1",
        config_name="persistent_defaults",
        config_override=None,
        runtime_authority=sessions_mod.ThreadRuntimeAuthority(
            thread_id=CONNECTION_THREAD_ID,
            generation=CONNECTION_GENERATION,
        ),
        dependencies=fake_main.dependencies,
    )

    assert [event["state"] for event in emitted] == [
        "provisioning",
        "booting",
        "failed",
    ]
    fake_main.session_router.teardown_route.assert_awaited_once_with(
        CONNECTION_THREAD_ID,
        expected_namespace="srw",
        expected_runtime_generation=CONNECTION_GENERATION,
        expected_owner_uid=CONNECTION_POD_UID,
    )


@pytest.mark.asyncio
async def test_do_prepare_reports_failed_when_exact_route_cleanup_is_incomplete(
    monkeypatch,
):
    from orchestrator.routers import sessions as sessions_mod

    original = _connection_binding()
    db = AsyncMock()
    db.get_thread.return_value = _connection_thread()
    db.get_pinned_session_binding.side_effect = [original, original]
    lock_cm = AsyncMock()
    lock_cm.__aenter__.return_value = None
    lock_cm.__aexit__.return_value = False
    db.thread_advisory_lock = MagicMock(return_value=lock_cm)
    fake_main = _fake_main(db)
    monkeypatch.setattr(
        sessions_mod, "wait_for_ready", AsyncMock(return_value=True), raising=True
    )
    fake_main.session_router = MagicMock()
    fake_main.session_router.ensure_route = AsyncMock(
        side_effect=RuntimeError("Ingress create failed")
    )
    fake_main.session_router.teardown_route = AsyncMock(return_value=False)
    emitted: list[dict] = []
    monkeypatch.setattr(
        sessions_mod,
        "lifecycle_emit",
        lambda _uid, _tid, state, **extra: emitted.append({"state": state, **extra}),
        raising=True,
    )

    await sessions_mod._do_prepare(
        thread_id=CONNECTION_THREAD_ID,
        user_id="u1",
        config_name="persistent_defaults",
        config_override=None,
        runtime_authority=sessions_mod.ThreadRuntimeAuthority(
            thread_id=CONNECTION_THREAD_ID,
            generation=CONNECTION_GENERATION,
        ),
        dependencies=fake_main.dependencies,
    )

    assert emitted[-1]["state"] == "failed"
    assert emitted[-1]["reason"] == "incomplete session route could not be removed"
    assert emitted[-1]["session_runtime_generation"] == CONNECTION_GENERATION


@pytest.mark.asyncio
async def test_do_prepare_emits_failed_when_pod_not_ready(monkeypatch):
    """If readiness probe times out, _do_prepare emits failed and does not
    call session_router.ensure_route."""
    from orchestrator.routers import sessions as sessions_mod

    db = AsyncMock()
    db.get_thread.return_value = _connection_thread()
    db.get_pinned_session_binding.return_value = _connection_binding()
    lock_cm = AsyncMock()
    lock_cm.__aenter__.return_value = None
    lock_cm.__aexit__.return_value = False
    db.thread_advisory_lock = MagicMock(return_value=lock_cm)
    fake_main = _fake_main(db)

    # Readiness times out.
    async def _ready_timeout(pod_ip, pod_port, timeout_s, **_kwargs):
        return False

    monkeypatch.setattr(sessions_mod, "wait_for_ready", _ready_timeout, raising=True)

    fake_main.session_router = AsyncMock()
    fake_main.session_router.ensure_route = AsyncMock()

    emit_calls: list[dict] = []

    def _capture_emit(user_id, thread_id, state, **extra):
        emit_calls.append(
            {"user_id": user_id, "thread_id": thread_id, "state": state, **extra}
        )

    monkeypatch.setattr(sessions_mod, "lifecycle_emit", _capture_emit, raising=True)

    await sessions_mod._do_prepare(
        thread_id=CONNECTION_THREAD_ID,
        user_id="u1",
        config_name="persistent_defaults",
        config_override=None,
        runtime_authority=sessions_mod.ThreadRuntimeAuthority(
            thread_id=CONNECTION_THREAD_ID,
            generation=CONNECTION_GENERATION,
        ),
        dependencies=fake_main.dependencies,
    )

    states = [c["state"] for c in emit_calls]
    assert "failed" in states
    fake_main.session_router.ensure_route.assert_not_called()


@pytest.mark.asyncio
async def test_do_prepare_waits_when_agent_pod_marker_in_flight(monkeypatch):
    """If create-thread already created a session pod, prepare should wait
    for that pod to register instead of provisioning a duplicate."""
    from orchestrator.routers import sessions as sessions_mod

    marker = {
        "status": "created",
        "pod_name": "srw-agent-s-existing",
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    db = AsyncMock()
    db.get_thread = AsyncMock(
        side_effect=_sequence_then_repeat(
            {
                **_connection_thread(agent_id=None),
                "metadata": {"agent_pod": marker},
            },
            _connection_thread(),
        )
    )
    db.get_pinned_session_binding.return_value = _connection_binding(
        hostname="srw-agent-s-existing",
        pod_uid="k8s-uid-1",
    )
    lock_cm = AsyncMock()
    lock_cm.__aenter__.return_value = None
    lock_cm.__aexit__.return_value = False
    db.thread_advisory_lock = MagicMock(return_value=lock_cm)
    fake_main = _fake_main(db)

    provision = AsyncMock()
    monkeypatch.setattr(
        sessions_mod, "_provision_agent_for_thread", provision, raising=True
    )

    async def _bound(*args, **kwargs):
        return True

    async def _ready_ok(pod_ip, pod_port, timeout_s, **_kwargs):
        return True

    monkeypatch.setattr(sessions_mod, "wait_for_binding", _bound, raising=True)
    monkeypatch.setattr(sessions_mod, "wait_for_ready", _ready_ok, raising=True)

    fake_main.session_router = AsyncMock()
    fake_main.session_router.ensure_route = AsyncMock(return_value="/p/t1")
    fake_main.ensure_session_workspace = AsyncMock(return_value=None)
    fake_main.session_grant_violations = AsyncMock(return_value=[])
    fake_main.session_endpoint_violations = AsyncMock(return_value=[])

    emit_calls: list[dict] = []

    def _capture_emit(user_id, thread_id, state, **extra):
        emit_calls.append(
            {"user_id": user_id, "thread_id": thread_id, "state": state, **extra}
        )

    monkeypatch.setattr(sessions_mod, "lifecycle_emit", _capture_emit, raising=True)

    await sessions_mod._do_prepare(
        thread_id=CONNECTION_THREAD_ID,
        user_id="u1",
        config_name="persistent_defaults",
        config_override=None,
        runtime_authority=sessions_mod.ThreadRuntimeAuthority(
            thread_id=CONNECTION_THREAD_ID,
            generation=CONNECTION_GENERATION,
        ),
        dependencies=fake_main.dependencies,
    )

    provision.assert_not_awaited()
    states = [c["state"] for c in emit_calls]
    assert states == ["provisioning", "booting", "ready"]
    fake_main.session_router.ensure_route.assert_called_once_with(
        thread_id=CONNECTION_THREAD_ID,
        pod_name="srw-agent-s-existing",
        pod_uid="k8s-uid-1",
        runtime_generation=CONNECTION_GENERATION,
    )


@pytest.mark.asyncio
async def test_do_prepare_reconciles_workspace_on_cold_start(monkeypatch):
    """An unbound thread (cold start / reopen) kicks off ensure_session_workspace
    so a 'ready'-but-dead workspace is recreated (drift) instead of the fresh
    agent dialing a corpse. Warm reconnects (agent already bound) skip it."""
    from orchestrator.routers import sessions as sessions_mod

    db = AsyncMock()
    db.get_thread = AsyncMock(
        side_effect=_sequence_then_repeat(
            _connection_thread(agent_id=None),
            _connection_thread(agent_id=None),
            _connection_thread(agent_id=None),
            _connection_thread(agent_id=None),
            _connection_thread(agent_id=None),
            _connection_thread(),
        )
    )
    db.get_pinned_session_binding.return_value = _connection_binding(
        hostname="srw-agent-s-new",
        pod_uid="uid-1",
    )
    lock_cm = AsyncMock()
    lock_cm.__aenter__.return_value = None
    lock_cm.__aexit__.return_value = False
    db.thread_advisory_lock = MagicMock(return_value=lock_cm)
    fake_main = _fake_main(db)

    monkeypatch.setattr(
        sessions_mod, "_provision_agent_for_thread", AsyncMock(), raising=True
    )

    async def _bound(*a, **k):
        return True

    async def _ready_ok(pod_ip, pod_port, timeout_s, **_kwargs):
        return True

    monkeypatch.setattr(sessions_mod, "wait_for_binding", _bound, raising=True)
    monkeypatch.setattr(sessions_mod, "wait_for_ready", _ready_ok, raising=True)

    fake_main.session_router = AsyncMock()
    fake_main.session_router.ensure_route = AsyncMock(return_value="/p/t1")
    fake_main.ensure_session_workspace = AsyncMock(return_value=None)
    fake_main.session_grant_violations = AsyncMock(return_value=[])
    fake_main.session_endpoint_violations = AsyncMock(return_value=[])

    monkeypatch.setattr(
        sessions_mod, "lifecycle_emit", lambda *a, **k: None, raising=True
    )

    await sessions_mod._do_prepare(
        thread_id=CONNECTION_THREAD_ID,
        user_id="u1",
        config_name="persistent_defaults",
        config_override=None,
        runtime_authority=sessions_mod.ThreadRuntimeAuthority(
            thread_id=CONNECTION_THREAD_ID,
            generation=CONNECTION_GENERATION,
        ),
        dependencies=fake_main.dependencies,
    )

    fake_main.ensure_session_workspace.assert_called_once()
    assert fake_main.ensure_session_workspace.call_args.args[0] == CONNECTION_THREAD_ID


@pytest.mark.asyncio
async def test_do_prepare_waits_for_cloud_folder_before_binding_an_agent(monkeypatch):
    """/prepare is the second attach path (POST /resume's _reprovision is the
    first). The agent reads its cloud config within ~150ms of attach and never
    re-reads, so binding ahead of in-flight session-folder provisioning leaves
    the session with workspace_sync=None for its whole life. Ordering — gate
    first, THEN provision — is the contract.
    knowledge-history/done/session_resume_cloud_sync_race_late_provision.md
    """
    from orchestrator.routers import sessions as sessions_mod

    calls: list[str] = []

    db = AsyncMock()
    db.get_thread = AsyncMock(
        side_effect=_sequence_then_repeat(
            _connection_thread(agent_id=None),
            _connection_thread(agent_id=None),
            _connection_thread(agent_id=None),
            _connection_thread(agent_id=None),
            _connection_thread(agent_id=None),
            _connection_thread(),
        )
    )
    db.get_pinned_session_binding.return_value = _connection_binding(
        hostname="srw-agent-s-new",
        pod_uid="uid-1",
    )
    lock_cm = AsyncMock()

    async def _enter_lock():
        calls.append("lock_enter")

    lock_cm.__aenter__.side_effect = _enter_lock
    lock_cm.__aexit__.return_value = False
    db.thread_advisory_lock = MagicMock(return_value=lock_cm)
    fake_main = _fake_main(db)

    async def _provision(**kwargs):
        calls.append("provision")

    monkeypatch.setattr(
        sessions_mod, "_provision_agent_for_thread", _provision, raising=True
    )

    async def _bound(*a, **k):
        return True

    async def _ready_ok(pod_ip, pod_port, timeout_s, **_kwargs):
        return True

    monkeypatch.setattr(sessions_mod, "wait_for_binding", _bound, raising=True)
    monkeypatch.setattr(sessions_mod, "wait_for_ready", _ready_ok, raising=True)

    async def _gate(thread_id):
        calls.append("cloud_gate")

    fake_main.await_late_cloud_setup = _gate
    fake_main.session_router = AsyncMock()
    fake_main.session_router.ensure_route = AsyncMock(return_value="/p/t1")
    fake_main.ensure_session_workspace = AsyncMock(return_value=None)
    fake_main.session_grant_violations = AsyncMock(return_value=[])
    fake_main.session_endpoint_violations = AsyncMock(return_value=[])

    monkeypatch.setattr(
        sessions_mod, "lifecycle_emit", lambda *a, **k: None, raising=True
    )

    await sessions_mod._do_prepare(
        thread_id=CONNECTION_THREAD_ID,
        user_id="u1",
        config_name="persistent_defaults",
        config_override=None,
        runtime_authority=sessions_mod.ThreadRuntimeAuthority(
            thread_id=CONNECTION_THREAD_ID,
            generation=CONNECTION_GENERATION,
        ),
        dependencies=fake_main.dependencies,
    )

    # Gate first, and OUTSIDE the advisory lock — holding it across the wait
    # would stall the fresh pod's own POST /api/agents/register, which takes
    # that same lock.
    assert calls == ["cloud_gate", "lock_enter", "provision"]


@pytest.mark.asyncio
async def test_do_prepare_grant_denied_fails_fast_without_provisioning(monkeypatch):
    """An unbound session whose resolved config exceeds the user's capability
    grants fails fast: emit provisioning→failed with the violation, and kick off
    NEITHER workspace reconciliation NOR agent provisioning. Prevents the doomed
    pod that 403s at the workspace endpoint and the cockpit's ~5m40s timeout.
    knowledge-base/knowledge/issues/session_permission_mode_grant_denied_ready_timeout.md
    """
    from orchestrator.routers import sessions as sessions_mod

    db = AsyncMock()
    db.get_thread.return_value = _connection_thread(agent_id=None)
    lock_cm = AsyncMock()
    lock_cm.__aenter__.return_value = None
    lock_cm.__aexit__.return_value = False
    db.thread_advisory_lock = MagicMock(return_value=lock_cm)
    fake_main = _fake_main(db)

    provision = AsyncMock()
    monkeypatch.setattr(
        sessions_mod, "_provision_agent_for_thread", provision, raising=True
    )

    fake_main.session_grant_violations = AsyncMock(
        return_value=["permission_mode: 'autonomous' exceeds the ceiling"]
    )
    # No stub for the detail formatter: the router imports the real one from
    # ``grant_enforcement`` now, so the assertion below checks the shipped text.
    fake_main.ensure_session_workspace = AsyncMock(return_value=None)

    emit_calls: list[dict] = []

    def _capture_emit(user_id, thread_id, state, **extra):
        emit_calls.append(
            {"user_id": user_id, "thread_id": thread_id, "state": state, **extra}
        )

    monkeypatch.setattr(sessions_mod, "lifecycle_emit", _capture_emit, raising=True)

    await sessions_mod._do_prepare(
        thread_id=CONNECTION_THREAD_ID,
        user_id="u1",
        config_name="persistent_defaults",
        config_override=None,
        runtime_authority=sessions_mod.ThreadRuntimeAuthority(
            thread_id=CONNECTION_THREAD_ID,
            generation=CONNECTION_GENERATION,
        ),
        dependencies=fake_main.dependencies,
    )

    states = [c["state"] for c in emit_calls]
    assert states == ["provisioning", "failed"], states
    failed = next(c for c in emit_calls if c["state"] == "failed")
    assert "capability grants" in failed["reason"]
    provision.assert_not_awaited()
    fake_main.ensure_session_workspace.assert_not_called()


# --------------------------------------------------------------------------- #
# GET /api/sessions/{tid}/connection
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "metadata,expected_status",
    [
        ({}, 200),
        ({"protected_cloud": False}, 200),
        ({"protected_cloud": True}, 425),
        ({"protected_cloud": "false"}, 425),
        ({"protected_cloud": None}, 425),
        ([], 425),
        (None, 425),
    ],
)
def test_connection_probes_persisted_metadata_with_the_correct_cloud_requirement(
    monkeypatch, metadata, expected_status
):
    """An ordinary pinned /ready is sufficient only for ordinary stored rows."""

    import httpx
    from orchestrator.routers import sessions as sessions_mod
    from orchestrator.services import session_lifecycle
    from orchestrator.services.session_tokens import SessionTokenService

    _install_fake_auth(monkeypatch)
    db = AsyncMock()
    # asyncpg returns JSONB as text; PostgresDB.get_thread returns dict(row).
    db.get_thread.return_value = {
        **_connection_thread(),
        "metadata": json.dumps(metadata),
    }
    binding = _connection_binding()
    db.get_pinned_session_binding.return_value = binding
    fake_main = _fake_main(db)

    def ready_response(request):
        assert request.url.path == "/ready"
        return httpx.Response(
            200,
            json={
                "ready": True,
                "thread_id": CONNECTION_THREAD_ID,
                "session_identity_fingerprint": binding.session_identity_fingerprint,
                "capabilities": {
                    "pinned_session_identity_contract": 1,
                    "protected_cloud_contract": None,
                    "protected_cloud_ready": False,
                },
            },
        )

    real_client = httpx.AsyncClient
    transport = httpx.MockTransport(ready_response)
    monkeypatch.setattr(
        session_lifecycle.httpx,
        "AsyncClient",
        lambda **kwargs: real_client(transport=transport, **kwargs),
    )
    monkeypatch.setattr(sessions_mod, "probe_ready", session_lifecycle.probe_ready)
    fake_main.session_router.ensure_route = AsyncMock(return_value="/p/t1")
    fake_main.session_tokens = SessionTokenService(
        secret="test-secret-do-not-use", ttl_seconds=60
    )

    app = FastAPI()
    app.state.sessions_dependencies_factory = lambda: fake_main.dependencies
    app.include_router(sessions_mod.router)
    response = TestClient(app).get(f"/api/sessions/{CONNECTION_THREAD_ID}/connection")

    assert response.status_code == expected_status
    if expected_status == 200:
        assert response.json()["state"] == "ready"
        assert response.json()["session_runtime_generation"] == CONNECTION_GENERATION
        claims = fake_main.session_tokens.validate(response.json()["token"])
        assert claims["sif"] == binding.session_identity_fingerprint
        fake_main.session_router.ensure_route.assert_awaited_once()
    else:
        assert response.json()["detail"] == "session not ready"
        fake_main.session_router.ensure_route.assert_not_awaited()


def test_connection_returns_ws_url_and_token_when_ready(monkeypatch):
    """GET /connection returns 200 with ws_url + token when bound + ready."""
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from orchestrator.routers.sessions import router as sessions_router
    from orchestrator.services.session_tokens import SessionTokenService

    app = FastAPI()
    _install_fake_auth(monkeypatch)

    fake_db = AsyncMock()
    fake_db.get_thread.return_value = _connection_thread()
    fake_db.get_pinned_session_binding.return_value = _connection_binding()
    from orchestrator.routers import sessions as sessions_mod

    fake_main = _fake_main(fake_db)

    # /connection probes the agent's /ready before minting; stub it to
    # always pass so we exercise the 200 path.
    observed_probe = {}

    async def _probe_ok(pod_ip, pod_port, **kwargs):
        observed_probe.update(kwargs)
        return True

    monkeypatch.setattr(sessions_mod, "probe_ready", _probe_ok, raising=True)

    # Inject a real SessionTokenService and a fake session_router.
    test_tokens = SessionTokenService(secret="test-secret-do-not-use", ttl_seconds=60)

    fake_main.session_tokens = test_tokens
    fake_main.session_router = MagicMock()
    fake_main.session_router.ensure_route = AsyncMock(return_value="/p/t1")
    monkeypatch.setenv("SESSION_INGRESS_HOST", "api.test.example")

    app.state.sessions_dependencies_factory = lambda: fake_main.dependencies
    app.include_router(sessions_router)
    client = TestClient(app)
    resp = client.get(f"/api/sessions/{CONNECTION_THREAD_ID}/connection")
    assert resp.status_code == 200
    body = resp.json()
    assert body["state"] == "ready"
    assert body["control_socket"] == "websocket"
    assert body["ws_url"].startswith(
        f"wss://api.test.example/p/{CONNECTION_THREAD_ID}/ws?t="
    )
    assert isinstance(body["token"], str) and body["token"]
    assert isinstance(body["expires_at"], int)
    assert body["pinned_runtime_generation_contract"] == 1
    assert body["session_runtime_generation"] == CONNECTION_GENERATION
    assert "execution_lane" not in body
    assert body["controls"]["config.update"] == "websocket"
    assert body["controls"]["mode.set"] == "rest"
    assert observed_probe["expected_session_identity_fingerprint"].startswith("sha256:")
    claims = test_tokens.validate(body["token"])
    assert claims["sif"] == observed_probe["expected_session_identity_fingerprint"]
    # ensure_route is called idempotently on every /connection.
    fake_main.session_router.ensure_route.assert_called_once_with(
        thread_id=CONNECTION_THREAD_ID,
        pod_name="srw-agent-x",
        pod_uid=CONNECTION_POD_UID,
        runtime_generation=CONNECTION_GENERATION,
    )
    assert fake_db.get_pinned_session_binding.await_count == 3
    fake_db.get_pinned_session_binding.assert_awaited_with(
        CONNECTION_THREAD_ID,
        expected_runtime_generation=CONNECTION_GENERATION,
    )


@pytest.mark.parametrize("mutation_phase", ["post_probe", "post_route"])
@pytest.mark.parametrize(
    "changed_binding",
    [
        _connection_binding(hostname="srw-agent-successor"),
        _connection_binding(pod_uid="successor-pod-uid"),
        _connection_binding(pod_ip="10.0.0.99"),
        _connection_binding(pod_port=9001),
        _connection_binding(agent_id="66666666-6666-4666-8666-666666666666"),
        _connection_binding(attach_token="77777777-7777-4777-8777-777777777777"),
    ],
    ids=["hostname", "pod_uid", "pod_ip", "pod_port", "agent_id", "attach"],
)
def test_connection_refuses_any_changed_physical_binding_after_await(
    monkeypatch,
    mutation_phase,
    changed_binding,
):
    """A ready response/route for A cannot mint a token after DB names B."""

    from orchestrator.routers import sessions as sessions_mod

    fastapi_app = FastAPI()
    _install_fake_auth(monkeypatch)
    original = _connection_binding()
    fake_db = AsyncMock()
    fake_db.get_thread.return_value = _connection_thread()
    fake_db.get_pinned_session_binding.side_effect = (
        [original, changed_binding]
        if mutation_phase == "post_probe"
        else [original, original, changed_binding]
    )
    fake_main = _fake_main(fake_db)
    monkeypatch.setattr(
        sessions_mod,
        "probe_ready",
        AsyncMock(return_value=True),
        raising=True,
    )

    fake_main.session_tokens = MagicMock()
    fake_main.session_router = MagicMock()
    fake_main.session_router.ensure_route = AsyncMock(return_value="/p/t1")
    fake_main.session_router.teardown_route = AsyncMock(return_value=True)
    fastapi_app.state.sessions_dependencies_factory = lambda: fake_main.dependencies
    fastapi_app.include_router(sessions_mod.router)

    response = TestClient(fastapi_app).get(
        f"/api/sessions/{CONNECTION_THREAD_ID}/connection"
    )

    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "session_binding_invalid"
    fake_main.session_tokens.mint.assert_not_called()
    if mutation_phase == "post_probe":
        fake_main.session_router.ensure_route.assert_not_awaited()
        fake_main.session_router.teardown_route.assert_not_awaited()
    else:
        fake_main.session_router.ensure_route.assert_awaited_once()
        fake_main.session_router.teardown_route.assert_awaited_once_with(
            CONNECTION_THREAD_ID,
            expected_namespace="srw",
            expected_runtime_generation=CONNECTION_GENERATION,
            expected_owner_uid=CONNECTION_POD_UID,
        )


@pytest.mark.parametrize(
    "failure_phase", ["ensure_route", "final_binding_read", "token_mint"]
)
def test_connection_cleans_partial_route_on_every_exception(
    monkeypatch,
    failure_phase,
):
    """No exception after route mutation may leave the captured route behind."""

    from orchestrator.routers import sessions as sessions_mod

    fastapi_app = FastAPI()
    _install_fake_auth(monkeypatch)
    original = _connection_binding()
    fake_db = AsyncMock()
    fake_db.get_thread.return_value = _connection_thread()
    if failure_phase == "final_binding_read":
        fake_db.get_pinned_session_binding.side_effect = [
            original,
            original,
            RuntimeError("DB reread failed"),
        ]
    elif failure_phase == "token_mint":
        fake_db.get_pinned_session_binding.side_effect = [
            original,
            original,
            original,
        ]
    else:
        fake_db.get_pinned_session_binding.side_effect = [original, original]
    fake_main = _fake_main(fake_db)
    monkeypatch.setattr(
        sessions_mod, "probe_ready", AsyncMock(return_value=True), raising=True
    )
    fake_main.session_tokens = MagicMock()
    if failure_phase == "token_mint":
        fake_main.session_tokens.mint.side_effect = RuntimeError("token mint failed")
    fake_main.session_router = MagicMock()
    fake_main.session_router.ensure_route = AsyncMock(
        side_effect=(
            RuntimeError("Ingress create failed")
            if failure_phase == "ensure_route"
            else None
        )
    )
    fake_main.session_router.teardown_route = AsyncMock(return_value=True)
    fastapi_app.state.sessions_dependencies_factory = lambda: fake_main.dependencies
    fastapi_app.include_router(sessions_mod.router)

    response = TestClient(fastapi_app, raise_server_exceptions=False).get(
        f"/api/sessions/{CONNECTION_THREAD_ID}/connection"
    )

    assert response.status_code == 500
    if failure_phase == "token_mint":
        fake_main.session_tokens.mint.assert_called_once()
    else:
        fake_main.session_tokens.mint.assert_not_called()
    fake_main.session_router.teardown_route.assert_awaited_once_with(
        CONNECTION_THREAD_ID,
        expected_namespace="srw",
        expected_runtime_generation=CONNECTION_GENERATION,
        expected_owner_uid=CONNECTION_POD_UID,
    )


@pytest.mark.parametrize(
    ("cleanup_failure", "message"),
    [
        (False, "incomplete session route could not be removed"),
        (RuntimeError("Kubernetes cleanup failed"), "Kubernetes cleanup failed"),
    ],
    ids=["false", "raises"],
)
def test_connection_fails_when_exact_partial_route_cleanup_is_incomplete(
    monkeypatch,
    cleanup_failure,
    message,
):
    from orchestrator.routers import sessions as sessions_mod

    fastapi_app = FastAPI()
    _install_fake_auth(monkeypatch)
    original = _connection_binding()
    fake_db = AsyncMock()
    fake_db.get_thread.return_value = _connection_thread()
    fake_db.get_pinned_session_binding.side_effect = [original, original]
    fake_main = _fake_main(fake_db)
    monkeypatch.setattr(
        sessions_mod, "probe_ready", AsyncMock(return_value=True), raising=True
    )
    fake_main.session_tokens = MagicMock()
    fake_main.session_router = MagicMock()
    fake_main.session_router.ensure_route = AsyncMock(
        side_effect=RuntimeError("Ingress create failed")
    )
    if isinstance(cleanup_failure, BaseException):
        fake_main.session_router.teardown_route = AsyncMock(side_effect=cleanup_failure)
    else:
        fake_main.session_router.teardown_route = AsyncMock(
            return_value=cleanup_failure
        )
    fastapi_app.state.sessions_dependencies_factory = lambda: fake_main.dependencies
    fastapi_app.include_router(sessions_mod.router)

    with pytest.raises(RuntimeError, match=message):
        TestClient(fastapi_app).get(f"/api/sessions/{CONNECTION_THREAD_ID}/connection")
    fake_main.session_tokens.mint.assert_not_called()


@pytest.mark.parametrize("mutation_phase", ["post_probe", "post_route"])
@pytest.mark.parametrize(
    ("current_status", "expected_status"),
    [("offline", 425), ("booting", 409)],
)
def test_connection_regates_agent_status_after_readiness(
    monkeypatch,
    mutation_phase,
    current_status,
    expected_status,
):
    """Mutable heartbeat state is not in target_key, but remains a live gate."""

    from orchestrator.routers import sessions as sessions_mod

    fastapi_app = FastAPI()
    _install_fake_auth(monkeypatch)
    fake_db = AsyncMock()
    fake_db.get_thread.return_value = _connection_thread()
    original = _connection_binding(status="ready")
    changed = _connection_binding(status=current_status)
    fake_db.get_pinned_session_binding.side_effect = (
        [original, changed]
        if mutation_phase == "post_probe"
        else [original, original, changed]
    )
    fake_main = _fake_main(fake_db)
    monkeypatch.setattr(
        sessions_mod,
        "probe_ready",
        AsyncMock(return_value=True),
        raising=True,
    )
    fake_main.session_tokens = MagicMock()
    fake_main.session_router = MagicMock()
    fake_main.session_router.ensure_route = AsyncMock(return_value="/p/t1")
    fake_main.session_router.teardown_route = AsyncMock(return_value=True)
    fastapi_app.state.sessions_dependencies_factory = lambda: fake_main.dependencies
    fastapi_app.include_router(sessions_mod.router)

    response = TestClient(fastapi_app).get(
        f"/api/sessions/{CONNECTION_THREAD_ID}/connection"
    )

    assert response.status_code == expected_status
    if mutation_phase == "post_probe":
        fake_main.session_router.ensure_route.assert_not_awaited()
        fake_main.session_router.teardown_route.assert_not_awaited()
    else:
        fake_main.session_router.ensure_route.assert_awaited_once()
        fake_main.session_router.teardown_route.assert_awaited_once_with(
            CONNECTION_THREAD_ID,
            expected_namespace="srw",
            expected_runtime_generation=CONNECTION_GENERATION,
            expected_owner_uid=CONNECTION_POD_UID,
        )
    fake_main.session_tokens.mint.assert_not_called()


def test_connection_reports_stateless_ready_without_a_socket(monkeypatch):
    """A queue-served thread is admission-ready but has no control socket."""
    from orchestrator.routers import sessions as sessions_mod

    fastapi_app = FastAPI()
    _install_fake_auth(monkeypatch)
    fake_db = AsyncMock()
    fake_db.get_thread.return_value = _connection_thread(
        lane="stateless", agent_id=None
    )
    fake_main = _fake_main(fake_db)
    fastapi_app.state.sessions_dependencies_factory = lambda: fake_main.dependencies
    fastapi_app.include_router(sessions_mod.router)

    resp = TestClient(fastapi_app).get(
        f"/api/sessions/{CONNECTION_THREAD_ID}/connection"
    )

    assert resp.status_code == 200
    assert resp.json() == {
        "state": "ready",
        "control_socket": "none",
        "ws_url": None,
        "token": None,
        "expires_at": None,
        "pinned_runtime_generation_contract": 1,
        "session_runtime_generation": CONNECTION_GENERATION,
        # Declared per verb, never inferred from the lane: config edits ride
        # the owner PATCH, the scalars + undo ride the control inbox, and the
        # verbs with no stateless transport are ABSENT (not mapped to a
        # socket that will never open).
        "controls": {
            "config.update": "rest",
            "workspace.undo": "rest",
            "mode.set": "rest",
            "narration.set": "rest",
        },
        "control_options": {},
        "conversation_revision": 0,
        # The run-queue lifecycle block rides the connection payload so a
        # reload can re-derive a queued or parked turn.  This fake serves no
        # connection, so the read degrades to None -- which is exactly the
        # contract: a queue read that fails never withholds readiness.  The
        # block's own shape and both degrade paths are covered by
        # test_thread_queue_endpoints.py::test_connection_queue_block_reads_and_degrades.
        "queue": None,
    }
    fake_db.get_pinned_session_binding.assert_not_awaited()


def test_connection_advertises_idle_conversation_rewind_when_enabled(monkeypatch):
    from orchestrator.routers import sessions as sessions_mod

    class _Conn:
        async def fetchval(self, *_args, **_kwargs):
            return False

        async def fetchrow(self, *_args, **_kwargs):
            return None

    class _Acquire:
        async def __aenter__(self):
            return _Conn()

        async def __aexit__(self, *_args):
            return False

    fastapi_app = FastAPI()
    _install_fake_auth(monkeypatch)
    monkeypatch.setenv("SESSION_REWIND_IDLE_CONVERSATION_ENABLED", "true")
    fake_db = AsyncMock()
    fake_db.get_thread.return_value = {
        **_connection_thread(lane="stateless", agent_id=None),
        "kind": "session",
        "parent_job_id": None,
        "parent_thread_id": None,
        "conversation_revision": 4,
    }
    fake_db.acquire = lambda: _Acquire()
    fake_main = _fake_main(fake_db)
    fastapi_app.state.sessions_dependencies_factory = lambda: fake_main.dependencies
    fastapi_app.include_router(sessions_mod.router)

    response = TestClient(fastapi_app).get(
        f"/api/sessions/{CONNECTION_THREAD_ID}/connection"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["controls"]["rewind"] == "rest"
    assert payload["control_options"]["rewind"] == {
        "version": 1,
        "modes": ["conversation"],
        "requires_idle": True,
    }
    assert payload["conversation_revision"] == 4


def test_stateless_controls_never_advertise_a_socket_verb():
    """The socket-only verbs must not leak into the stateless declaration —
    a Cockpit that trusted such an entry would queue into the void again."""
    from orchestrator.routers.sessions import PINNED_CONTROLS, STATELESS_CONTROLS

    assert "websocket" not in STATELESS_CONTROLS.values()
    for verb in ("compact", "archive", "rewind", "upgrade-to-workspace"):
        assert verb not in STATELESS_CONTROLS
        assert PINNED_CONTROLS[verb] == "websocket"
    # Both lanes agree on the durable scalars.
    for verb in ("mode.set", "narration.set"):
        assert PINNED_CONTROLS[verb] == STATELESS_CONTROLS[verb] == "rest"


def test_connection_models_require_their_transport_discriminator_and_socket_shape():
    from orchestrator.routers.sessions import (
        PinnedConnectionResponse,
        StatelessConnectionResponse,
    )

    for model in (PinnedConnectionResponse, StatelessConnectionResponse):
        with pytest.raises(ValidationError):
            model.model_validate({})


def test_connection_openapi_is_a_required_discriminated_union():
    from orchestrator.routers.sessions import router as sessions_router

    fastapi_app = FastAPI()
    fastapi_app.include_router(sessions_router)
    openapi = fastapi_app.openapi()
    response_schema = openapi["paths"]["/api/sessions/{thread_id}/connection"]["get"][
        "responses"
    ]["200"]["content"]["application/json"]["schema"]

    assert len(response_schema["oneOf"]) == 2
    assert response_schema["discriminator"] == {
        "propertyName": "control_socket",
        "mapping": {
            "websocket": "#/components/schemas/PinnedConnectionResponse",
            "none": "#/components/schemas/StatelessConnectionResponse",
        },
    }
    stateless_required = set(
        openapi["components"]["schemas"]["StatelessConnectionResponse"]["required"]
    )
    assert stateless_required == {
        "state",
        "control_socket",
        "ws_url",
        "token",
        "expires_at",
        "session_runtime_generation",
    }


@pytest.mark.parametrize(
    ("execution_lane", "agent_id"),
    [("future-lane", None), ("stateless", "agent-should-not-be-bound")],
)
def test_connection_fails_closed_for_unsafe_lane_rows(
    monkeypatch, execution_lane, agent_id
):
    from orchestrator.routers import sessions as sessions_mod

    fastapi_app = FastAPI()
    _install_fake_auth(monkeypatch)
    fake_db = AsyncMock()
    fake_db.get_thread.return_value = _connection_thread(
        lane=execution_lane,
        agent_id=(CONNECTION_AGENT_ID if agent_id else None),
    )
    fake_main = _fake_main(fake_db)
    fastapi_app.state.sessions_dependencies_factory = lambda: fake_main.dependencies
    fastapi_app.include_router(sessions_mod.router)

    resp = TestClient(fastapi_app).get(
        f"/api/sessions/{CONNECTION_THREAD_ID}/connection"
    )

    assert resp.status_code == 409
    fake_db.get_pinned_session_binding.assert_not_awaited()


def test_connection_returns_425_when_thread_unbound(monkeypatch):
    """If thread.agent_id is None, return 425 (cockpit must POST /prepare)."""
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from orchestrator.routers.sessions import router as sessions_router

    app = FastAPI()
    _install_fake_auth(monkeypatch)

    fake_db = AsyncMock()
    fake_db.get_thread.return_value = _connection_thread(agent_id=None)

    fake_main = _fake_main(fake_db)

    app.state.sessions_dependencies_factory = lambda: fake_main.dependencies
    app.include_router(sessions_router)
    client = TestClient(app)
    resp = client.get(f"/api/sessions/{CONNECTION_THREAD_ID}/connection")
    assert resp.status_code == 425


def test_connection_returns_404_for_missing_thread(monkeypatch):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from orchestrator.routers.sessions import router as sessions_router

    app = FastAPI()
    _install_fake_auth(monkeypatch)
    fake_db = AsyncMock()
    fake_db.get_thread.return_value = None

    fake_main = _fake_main(fake_db)

    app.state.sessions_dependencies_factory = lambda: fake_main.dependencies
    app.include_router(sessions_router)
    client = TestClient(app)
    resp = client.get("/api/sessions/missing/connection")
    assert resp.status_code == 404


def test_connection_returns_403_when_other_user(monkeypatch):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from orchestrator.routers.sessions import router as sessions_router

    app = FastAPI()
    _install_fake_auth(monkeypatch)
    fake_db = AsyncMock()
    fake_db.get_thread.return_value = _connection_thread(user_id="OTHER")

    fake_main = _fake_main(fake_db)

    app.state.sessions_dependencies_factory = lambda: fake_main.dependencies
    app.include_router(sessions_router)
    client = TestClient(app)
    resp = client.get(f"/api/sessions/{CONNECTION_THREAD_ID}/connection")
    assert resp.status_code == 403


def test_connection_returns_409_when_agent_not_ready(monkeypatch):
    """If the bound agent has no pod_ip or wrong status, return 409."""
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from orchestrator.routers.sessions import router as sessions_router

    app = FastAPI()
    _install_fake_auth(monkeypatch)
    fake_db = AsyncMock()
    fake_db.get_thread.return_value = _connection_thread()
    fake_db.get_pinned_session_binding.return_value = None

    fake_main = _fake_main(fake_db)

    app.state.sessions_dependencies_factory = lambda: fake_main.dependencies
    app.include_router(sessions_router)
    client = TestClient(app)
    resp = client.get(f"/api/sessions/{CONNECTION_THREAD_ID}/connection")
    assert resp.status_code == 409


def test_connection_waits_for_durable_recovery_when_agent_offline(monkeypatch):
    """Offline is a liveness hint; the foreground reader never clears A."""
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from orchestrator.routers.sessions import router as sessions_router

    app = FastAPI()
    _install_fake_auth(monkeypatch)
    fake_db = AsyncMock()
    fake_db.get_thread.return_value = _connection_thread()
    fake_db.get_pinned_session_binding.return_value = _connection_binding(
        status="offline"
    )

    fake_main = _fake_main(fake_db)

    app.state.sessions_dependencies_factory = lambda: fake_main.dependencies
    app.include_router(sessions_router)
    client = TestClient(app)
    resp = client.get(f"/api/sessions/{CONNECTION_THREAD_ID}/connection")
    assert resp.status_code == 425
    fake_db.clear_stale_thread_agent_if_matches.assert_not_awaited()
    fake_db.get_pinned_session_binding.assert_awaited_once_with(
        CONNECTION_THREAD_ID,
        expected_runtime_generation=CONNECTION_GENERATION,
    )


def test_connection_returns_typed_terminal_refusal_when_agent_row_missing(monkeypatch):
    """A missing reciprocal row cannot be repaired by pointer clearing."""
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from orchestrator.routers.sessions import router as sessions_router

    app = FastAPI()
    _install_fake_auth(monkeypatch)
    fake_db = AsyncMock()
    fake_db.get_thread.return_value = _connection_thread()
    fake_db.get_pinned_session_binding.return_value = None

    fake_main = _fake_main(fake_db)

    app.state.sessions_dependencies_factory = lambda: fake_main.dependencies
    app.include_router(sessions_router)
    client = TestClient(app)
    resp = client.get(f"/api/sessions/{CONNECTION_THREAD_ID}/connection")
    assert resp.status_code == 409
    assert resp.json()["detail"]["code"] == "session_binding_invalid"
    fake_db.clear_stale_thread_agent_if_matches.assert_not_awaited()


def test_connection_keeps_409_and_no_unbind_when_agent_booting(monkeypatch):
    """REGRESSION GUARD: a 'booting' agent (normal cold start) must stay 409 so
    the cockpit keeps polling — it must NOT be unbound as if it were dead."""
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from orchestrator.routers.sessions import router as sessions_router

    app = FastAPI()
    _install_fake_auth(monkeypatch)
    fake_db = AsyncMock()
    fake_db.get_thread.return_value = _connection_thread()
    fake_db.get_pinned_session_binding.return_value = _connection_binding(
        status="booting"
    )

    fake_main = _fake_main(fake_db)

    app.state.sessions_dependencies_factory = lambda: fake_main.dependencies
    app.include_router(sessions_router)
    client = TestClient(app)
    resp = client.get(f"/api/sessions/{CONNECTION_THREAD_ID}/connection")
    assert resp.status_code == 409
    fake_db.clear_stale_thread_agent_if_matches.assert_not_awaited()


def test_connection_returns_425_when_pod_not_session_ready(monkeypatch):
    """If the agent is bound but its /ready endpoint reports not ready
    (Uvicorn still in lifespan / _attach_session not finished), return
    425 so the cockpit's _pollConnectionUntilReady waits instead of
    opening a WS that would 503 at Traefik."""
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from orchestrator.routers.sessions import router as sessions_router

    app = FastAPI()
    _install_fake_auth(monkeypatch)
    fake_db = AsyncMock()
    fake_db.get_thread.return_value = _connection_thread()
    fake_db.get_pinned_session_binding.return_value = _connection_binding()
    from orchestrator.routers import sessions as sessions_mod

    fake_main = _fake_main(fake_db)

    async def _probe_not_ready(pod_ip, pod_port, **_kwargs):
        return False

    monkeypatch.setattr(sessions_mod, "probe_ready", _probe_not_ready, raising=True)

    app.state.sessions_dependencies_factory = lambda: fake_main.dependencies
    app.include_router(sessions_router)
    client = TestClient(app)
    resp = client.get(f"/api/sessions/{CONNECTION_THREAD_ID}/connection")
    assert resp.status_code == 425
    assert fake_db.get_pinned_session_binding.await_count == 1
