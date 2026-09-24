"""Exact recipient/fingerprint gates for pinned session effects."""

from tests import _b09_control_seams as control_seams

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from shared.pinned_session_identity import PinnedSessionBinding
from orchestrator.services import session_attach_payload
from orchestrator.application import sessions as sessions_composition
from orchestrator.services import agent_provisioner as agent_provisioner_module
from orchestrator.services import (
    managed_repository_authority as managed_repository_authority_module,
)
from orchestrator.services import (
    pinned_session_mutation_target as pinned_session_mutation_target_module,
)
from orchestrator.services import (
    protected_cloud_engage as protected_cloud_engage_module,
)
from orchestrator.services import (
    session_attach_binding as session_attach_binding_module,
)
import httpx

THREAD_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
AGENT_ID = "11111111-1111-4111-8111-111111111111"
GENERATION = "22222222-2222-4222-8222-222222222222"
ATTACH_TOKEN = "33333333-3333-4333-8333-333333333333"
FINGERPRINT = "sha256:" + ("a" * 64)
PROCESS_GENERATION = "process-a"


def _binding(*, pod_authority_kind: str = "provisioned") -> PinnedSessionBinding:
    return PinnedSessionBinding(
        thread_id=THREAD_ID,
        runtime_generation=GENERATION,
        agent_id=AGENT_ID,
        runtime_attach_token=ATTACH_TOKEN,
        agent_hostname="agent-a",
        pod_namespace="srw",
        pod_uid="pod-a",
        pod_ip="10.42.0.17",
        pod_port=8001,
        agent_status="session",
        pod_authority_kind=pod_authority_kind,
    )


def _route(app, path):
    return next(
        route for route in app.routes if route.path == path and "POST" in route.methods
    )


def _recipient(*, process_generation=PROCESS_GENERATION, pod_uid="pod-a"):
    return {
        "expected_thread_id": THREAD_ID,
        "expected_agent_id": AGENT_ID,
        "expected_pod_uid": pod_uid,
        "expected_process_generation": process_generation,
    }


@pytest.mark.asyncio
async def test_orchestrator_attests_receipt_backed_warm_pool_shape(monkeypatch):
    from orchestrator import main

    binding = _binding(pod_authority_kind="warm_pool")
    attest = AsyncMock(return_value=True)
    monkeypatch.setattr(
        agent_provisioner_module.agent_provisioner,
        "attest_pinned_session_recipient",
        attest,
    )

    assert await pinned_session_mutation_target_module.attest_pinned_session_mutation_pod(
        binding=binding,
        dependencies=sessions_composition.pinned_session_mutation_target_dependencies(
            main.app.state.resources
        ),
    )
    attest.assert_awaited_once_with(
        binding.agent_hostname,
        thread_id=THREAD_ID,
        expected_runtime_generation=GENERATION,
        expected_pod_uid="pod-a",
        expected_pod_ip="10.42.0.17",
        authority_kind="warm_pool",
        namespace="srw",
    )


@pytest.mark.asyncio
async def test_status_wrong_fingerprint_returns_no_turn_state():
    import agent.api.persistent_app as mod

    app = mod.create_persistent_app("config", THREAD_ID)
    route = _route(app, "/session/status")
    with (
        patch.object(
            mod,
            "_current_pinned_session_identity_fingerprint",
            return_value=FINGERPRINT,
        ),
        patch.object(mod, "_turn_in_flight") as turn_state,
    ):
        response = await route.endpoint(
            {"session_identity_fingerprint": "sha256:" + ("b" * 64)}
        )

    assert response.status_code == 409
    turn_state.assert_not_called()


@pytest.mark.asyncio
async def test_status_exact_fingerprint_is_recipient_verified():
    import json

    import agent.api.persistent_app as mod

    app = mod.create_persistent_app("config", THREAD_ID)
    route = _route(app, "/session/status")
    with (
        patch.object(
            mod,
            "_current_pinned_session_identity_fingerprint",
            return_value=FINGERPRINT,
        ),
        patch.object(mod, "_session_ready", return_value=True),
        patch.object(mod, "_turn_in_flight", return_value=False),
        patch.object(mod, "_thread_id", THREAD_ID),
    ):
        response = await route.endpoint({"session_identity_fingerprint": FINGERPRINT})

    assert response.status_code == 200
    payload = json.loads(response.body)
    assert payload["recipient_verified"] is True
    assert payload["thread_id"] == THREAD_ID
    assert payload["turn_in_flight"] is False


@pytest.mark.asyncio
async def test_detach_wrong_fingerprint_has_zero_effect():
    import agent.api.persistent_app as mod

    app = mod.create_persistent_app("config", THREAD_ID)
    route = _route(app, "/session/detach")
    terminate = AsyncMock()
    with (
        patch.object(
            mod,
            "_current_pinned_session_identity_fingerprint",
            return_value=FINGERPRINT,
        ),
        patch.object(mod, "_terminate_session", terminate),
    ):
        response = await route.endpoint(
            {"session_identity_fingerprint": "sha256:" + ("b" * 64)}
        )

    assert response.status_code == 409
    terminate.assert_not_awaited()


@pytest.mark.asyncio
async def test_orchestrator_detach_sends_exact_bound_fingerprint(monkeypatch):
    from orchestrator import main

    binding = _binding()
    db = SimpleNamespace(
        get_thread=AsyncMock(
            return_value={
                "id": THREAD_ID,
                "execution_lane": "pinned",
                "status": "active",
                "runtime_generation": GENERATION,
            }
        ),
        get_pinned_session_binding=AsyncMock(return_value=binding),
    )
    observed = {}

    class _Response:
        status_code = 200

    class _Client:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def post(self, url, *, json):
            observed.update({"url": url, "json": json})
            return _Response()

    monkeypatch.setattr(main.app.state.resources, "postgres_db", db)
    monkeypatch.setattr(httpx, "AsyncClient", _Client)

    assert await control_seams.detach_agent_session(THREAD_ID, timeout=1) is True
    assert observed == {
        "url": "http://10.42.0.17:8001/session/detach",
        "json": {"session_identity_fingerprint": binding.session_identity_fingerprint},
    }


@pytest.mark.asyncio
async def test_orchestrator_detach_never_dials_without_exact_binding(monkeypatch):
    from orchestrator import main

    db = SimpleNamespace(
        get_thread=AsyncMock(
            return_value={
                "id": THREAD_ID,
                "execution_lane": "pinned",
                "status": "active",
                "runtime_generation": GENERATION,
            }
        ),
        get_pinned_session_binding=AsyncMock(return_value=None),
    )
    client = AsyncMock()
    monkeypatch.setattr(main.app.state.resources, "postgres_db", db)
    monkeypatch.setattr(httpx, "AsyncClient", client)

    assert await control_seams.detach_agent_session(THREAD_ID, timeout=1) is False
    client.assert_not_called()


@pytest.mark.asyncio
async def test_parked_pinned_loop_reclaims_durable_input(monkeypatch):
    import agent.api.persistent_app as mod

    queue: asyncio.Queue = asyncio.Queue()

    async def reclaim():
        if queue.empty():
            queue.put_nowait(
                {"content": "durable successor wake", "delivery_id": "delivery-a"}
            )
        return {("delivery-a", 1)}

    monkeypatch.setattr(mod, "_reclaim_pending_pinned_inputs", reclaim)
    monkeypatch.setattr(mod, "_runtime_admission_closed", lambda: False)
    monkeypatch.setattr(mod, "_stateless_mode", lambda: False)

    item = await mod._wait_for_persistent_input(queue, timeout=0.1)

    assert item["delivery_id"] == "delivery-a"
    assert item["content"] == "durable successor wake"


@pytest.mark.asyncio
async def test_persistent_attach_wrong_process_refuses_before_pool_claim(monkeypatch):
    import agent.api.persistent_app as mod

    client = SimpleNamespace(
        agent_id=AGENT_ID,
        dispatch_process_generation=PROCESS_GENERATION,
    )
    adopt = MagicMock()
    monkeypatch.setenv("POD_UID", "pod-a")
    monkeypatch.setattr(mod, "_orchestrator_client", client)
    monkeypatch.setattr(mod, "_session", None)
    monkeypatch.setattr(mod, "_pool_attach_claim", None)
    monkeypatch.setattr(mod, "_pool_attach_runtime_generation", None)
    monkeypatch.setattr(mod, "_pool_attach_token", None)
    monkeypatch.setattr(mod, "_pool_attach_task", None)
    monkeypatch.setattr(mod, "_adopt_attached_runtime_identity", adopt)

    response = await mod._admit_pool_session_attach(
        {
            "thread_id": THREAD_ID,
            "pinned_runtime_generation_contract": 1,
            "session_runtime_generation": GENERATION,
            "session_runtime_attach_token": ATTACH_TOKEN,
            "_recipient": _recipient(process_generation="foreign-process"),
        }
    )

    assert response.status_code == 503
    assert json.loads(response.body)["error"] == "recipient_authority_mismatch"
    assert mod._pool_attach_claim is None
    assert mod._pool_attach_task is None
    adopt.assert_not_called()


@pytest.mark.asyncio
async def test_dual_attach_wrong_process_refuses_before_state_flip(monkeypatch):
    import agent.api.dual_app as mod

    app = mod.create_dual_app("config")
    route = _route(app, "/session/attach")
    client = SimpleNamespace(
        agent_id=AGENT_ID,
        dispatch_process_generation=PROCESS_GENERATION,
    )
    monkeypatch.setenv("POD_UID", "pod-a")
    monkeypatch.setattr(mod, "_orchestrator_client", client)
    monkeypatch.setattr(mod, "_pod_state", mod.PodState.IDLE)
    monkeypatch.setattr(mod, "_session_attach_claim", None)
    monkeypatch.setattr(mod, "_session_attach_task", None)

    response = await route.endpoint(
        {
            "thread_id": THREAD_ID,
            "pinned_runtime_generation_contract": 1,
            "session_runtime_generation": GENERATION,
            "session_runtime_attach_token": ATTACH_TOKEN,
            "_recipient": _recipient(process_generation="foreign-process"),
        }
    )

    assert response.status_code == 503
    assert json.loads(response.body)["error"] == "recipient_authority_mismatch"
    assert mod._pod_state == mod.PodState.IDLE
    assert mod._session_attach_claim is None
    assert mod._session_attach_task is None


@pytest.mark.asyncio
async def test_ready_advertises_session_recipient_only_with_process_epoch(monkeypatch):
    import agent.api.persistent_app as mod

    app = mod.create_persistent_app("config", None)
    route = next(
        route
        for route in app.routes
        if route.path == "/ready" and "GET" in route.methods
    )
    client = SimpleNamespace(agent_id=AGENT_ID, dispatch_process_generation=None)
    monkeypatch.setattr(mod, "_orchestrator_client", client)
    monkeypatch.setattr(mod, "_session_ready", lambda: False)

    without_epoch = json.loads((await route.endpoint()).body)
    assert "pinned_session_recipient_binding" not in without_epoch["capabilities"]

    client.dispatch_process_generation = PROCESS_GENERATION
    with_epoch = json.loads((await route.endpoint()).body)
    assert with_epoch["capabilities"]["pinned_session_recipient_binding"] is True


@pytest.mark.asyncio
async def test_dual_ready_advertises_session_recipient_only_with_process_epoch(
    monkeypatch,
):
    import agent.api.dual_app as mod

    app = mod.create_dual_app("config")
    route = next(
        route
        for route in app.routes
        if route.path == "/ready" and "GET" in route.methods
    )
    agent = MagicMock()
    agent.get_status.return_value = {
        "initialized": True,
        "connections": {"postgres": True},
    }
    client = SimpleNamespace(agent_id=AGENT_ID, dispatch_process_generation=None)
    monkeypatch.setattr(mod, "_agent", agent)
    monkeypatch.setattr(mod, "_orchestrator_client", client)
    monkeypatch.setattr(mod, "_pod_state", mod.PodState.IDLE)
    monkeypatch.setattr(mod, "_shutdown_requested", False)

    without_epoch = (await route.endpoint()).model_dump()
    assert "pinned_session_recipient_binding" not in without_epoch["capabilities"]

    client.dispatch_process_generation = PROCESS_GENERATION
    with_epoch = (await route.endpoint()).model_dump()
    assert with_epoch["capabilities"]["pinned_session_recipient_binding"] is True


@pytest.mark.asyncio
async def test_same_ip_successor_fails_pre_delivery_process_recheck(monkeypatch):
    from orchestrator import main

    binding = _binding()
    original = {
        "id": AGENT_ID,
        "thread_id": THREAD_ID,
        "status": "session",
        "current_job_id": None,
        "hostname": binding.agent_hostname,
        "pod_uid": binding.pod_uid,
        "pod_ip": binding.pod_ip,
        "pod_port": binding.pod_port,
        "metadata": {"dispatch_process_generation": PROCESS_GENERATION},
    }
    replacement = {
        **original,
        "pod_uid": "pod-successor",
        "metadata": {"dispatch_process_generation": "process-successor"},
    }
    db = SimpleNamespace(
        get_agent=AsyncMock(side_effect=[original, replacement]),
        get_pinned_session_binding=AsyncMock(return_value=binding),
    )

    class _Response:
        status_code = 200

        @staticmethod
        def json():
            return {
                "ready": True,
                "thread_id": None,
                "capabilities": {"pinned_session_recipient_binding": True},
            }

    class _Client:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def get(self, url):
            assert url == "http://10.42.0.17:8001/ready"
            return _Response()

    attest = AsyncMock(return_value=True)
    monkeypatch.setattr(main.app.state.resources, "postgres_db", db)
    monkeypatch.setattr(httpx, "AsyncClient", _Client)
    monkeypatch.setattr(
        pinned_session_mutation_target_module,
        "attest_pinned_session_mutation_pod",
        attest,
    )

    target = await pinned_session_mutation_target_module.prepare_pinned_session_mutation_target(
        thread_id=THREAD_ID,
        agent_id=AGENT_ID,
        runtime_generation=GENERATION,
        attach_token=ATTACH_TOKEN,
        dependencies=sessions_composition.pinned_session_mutation_target_dependencies(
            main.app.state.resources
        ),
    )

    assert target is None
    assert attest.await_count == 1


@pytest.mark.asyncio
async def test_attach_wrapper_sends_server_recipient_and_postchecks(monkeypatch):
    from orchestrator import main

    thread = {
        "id": THREAD_ID,
        "execution_lane": "pinned",
        "status": "created",
        "runtime_generation": GENERATION,
        "runtime_retirement_token": None,
        "runtime_attach_token": ATTACH_TOKEN,
        "agent_id": AGENT_ID,
    }
    agent = {
        "id": AGENT_ID,
        "pod_ip": "10.42.0.17",
        "pod_port": 8001,
    }
    target = pinned_session_mutation_target_module.PinnedSessionMutationTarget(
        agent=agent,
        binding=_binding(),
        recipient=_recipient(),
        process_generation=PROCESS_GENERATION,
        runtime_generation=GENERATION,
        attach_token=ATTACH_TOKEN,
    )
    db = SimpleNamespace(get_thread=AsyncMock(return_value=thread))
    observed = {}

    class _Response:
        status_code = 200

    class _Client:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def post(self, url, *, json):
            observed.update({"url": url, "json": json})
            return _Response()

    monkeypatch.setattr(main.app.state.resources, "postgres_db", db)
    monkeypatch.setattr(
        protected_cloud_engage_module,
        "_await_protected_cloud_runtime_ready",
        AsyncMock(return_value=True),
    )
    monkeypatch.setattr(
        managed_repository_authority_module,
        "prepare_thread_repository_authority",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        session_attach_binding_module,
        "reserve_session_attach_binding",
        AsyncMock(return_value=ATTACH_TOKEN),
    )
    monkeypatch.setattr(
        session_attach_payload,
        "assemble_session_attach_payload",
        AsyncMock(return_value={"session_runtime_generation": GENERATION}),
    )
    monkeypatch.setattr(
        pinned_session_mutation_target_module,
        "prepare_pinned_session_mutation_target",
        AsyncMock(return_value=target),
    )
    current = AsyncMock(return_value=True)
    monkeypatch.setattr(
        pinned_session_mutation_target_module,
        "pinned_session_mutation_target_is_current",
        current,
    )
    monkeypatch.setattr(httpx, "AsyncClient", _Client)

    accepted = await session_attach_binding_module.send_session_attach_locked(
        agent,
        THREAD_ID,
        dependencies=sessions_composition.session_attach_binding_dependencies(
            main.app.state.resources
        ),
    )

    assert accepted is True
    assert observed == {
        "url": "http://10.42.0.17:8001/session/attach",
        "json": {
            "session_runtime_generation": GENERATION,
            "session_runtime_attach_token": ATTACH_TOKEN,
            "_recipient": _recipient(),
        },
    }
    # The post-check is the owner's, bound by the composition to this
    # application's store.
    current.assert_awaited_once()
    assert current.await_args.args == (target,)
    assert set(current.await_args.kwargs) == {"dependencies"}
    assert current.await_args.kwargs["dependencies"].store is db
