"""Dual warm-pool attach aborts retain exact authority until proof is accepted."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import agent.api.dual_app as dual_app
import agent.api.persistent_app as persistent_app
from agent.api import session_attach, session_termination


GENERATION = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
ATTACH_TOKEN = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
WORKSPACE_GENERATION = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
WORKSPACE_RUNTIME = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
POD_UID = "pod-uid-a"
PROCESS_GENERATION = "process-a"


def _attach_endpoint():
    return next(
        route.endpoint
        for route in dual_app.create_dual_app().routes
        if getattr(route, "path", "") == "/session/attach"
    )


def _ready_endpoint():
    return next(
        route.endpoint
        for route in dual_app.create_dual_app().routes
        if getattr(route, "path", "") == "/ready"
    )


def _request(*, workspace: bool = False) -> dict:
    payload = {
        "thread_id": "thread-a",
        "pinned_runtime_generation_contract": 1,
        "session_runtime_generation": GENERATION,
        "session_runtime_attach_token": ATTACH_TOKEN,
        "_recipient": {
            "expected_thread_id": "thread-a",
            "expected_agent_id": "agent-a",
            "expected_pod_uid": POD_UID,
            "expected_process_generation": PROCESS_GENERATION,
        },
    }
    if workspace:
        payload.update(
            {
                "workspace_generation": WORKSPACE_GENERATION,
                "workspace_runtime_incarnation": WORKSPACE_RUNTIME,
            }
        )
    return payload


def _receipt(*, workspace: bool = False) -> dict:
    return {
        "thread_id": "thread-a",
        "session_runtime_generation": GENERATION,
        "session_runtime_attach_token": ATTACH_TOKEN,
        "agent_pod_uid": POD_UID,
        "local_runtime_quiesced": True,
        "local_quiescence_protocol": (
            "workspace_process_zero_v1" if workspace else "agent_runtime_zero_v1"
        ),
        "workspace_generation": WORKSPACE_GENERATION if workspace else None,
        "workspace_runtime_incarnation": WORKSPACE_RUNTIME if workspace else None,
    }


@pytest.fixture(autouse=True)
def _restore_globals(monkeypatch):
    saved = {
        "pod_state": dual_app._pod_state,
        "claim": dual_app._session_attach_claim,
        "task": dual_app._session_attach_task,
        "agent": dual_app._agent,
        "client": dual_app._orchestrator_client,
        "thread": persistent_app._session_identity.thread_id,
        "receipt": persistent_app._session_attach._release_receipt,
        "pa_client": persistent_app._orchestrator_client,
    }
    monkeypatch.setenv("POD_UID", POD_UID)
    dual_app._pod_state = dual_app.PodState.IDLE
    dual_app._session_attach_claim = None
    dual_app._session_attach_task = None
    dual_app._agent = MagicMock()
    dual_app._orchestrator_client = None
    persistent_app._session_identity._thread_id = None
    persistent_app._session_attach._release_receipt = None
    yield
    task = dual_app._session_attach_task
    if task is not None and not isinstance(task, MagicMock) and not task.done():
        task.cancel()
    dual_app._pod_state = saved["pod_state"]
    dual_app._session_attach_claim = saved["claim"]
    dual_app._session_attach_task = saved["task"]
    dual_app._agent = saved["agent"]
    dual_app._orchestrator_client = saved["client"]
    persistent_app._session_identity._thread_id = saved["thread"]
    persistent_app._session_attach._release_receipt = saved["receipt"]
    persistent_app._orchestrator_client = saved["pa_client"]


def _client(*, bound: bool = True, release=True) -> MagicMock:
    client = MagicMock()
    client.agent_id = "agent-a"
    client.dispatch_process_generation = PROCESS_GENERATION
    client.adopt_session_runtime_identity.return_value = True
    client.bind_pod_runtime_actor = AsyncMock(return_value=bound)
    client.release_thread_agent = AsyncMock(
        side_effect=release if callable(release) else None,
        return_value=release if not callable(release) else False,
    )
    client.clear_session_runtime_identity = MagicMock(return_value=True)
    return client


@pytest.mark.asyncio
async def test_dual_ready_reports_exact_non_secret_session_identity(monkeypatch):
    from shared.pinned_session_identity import (
        pinned_session_ready_identity_fingerprint,
    )

    thread_id = "10000000-0000-4000-8000-000000000001"
    agent_id = "30000000-0000-4000-8000-000000000003"
    dual_app._pod_state = dual_app.PodState.SESSION
    dual_app._agent.get_status.return_value = {
        "initialized": True,
        "connections": {"postgres": True},
    }
    monkeypatch.setattr(persistent_app._session_identity, "_thread_id", thread_id)
    monkeypatch.setattr(
        persistent_app._session_identity, "_session_generation", GENERATION
    )
    monkeypatch.setattr(persistent_app._session_identity, "_attach_token", ATTACH_TOKEN)
    monkeypatch.setattr(
        persistent_app, "_orchestrator_client", MagicMock(agent_id=agent_id)
    )
    monkeypatch.setattr(persistent_app, "_session_ready", lambda: True)
    monkeypatch.setattr(persistent_app, "_session", None)

    response = await _ready_endpoint()()
    payload = response.model_dump()

    assert payload["session_identity_fingerprint"] == (
        pinned_session_ready_identity_fingerprint(
            thread_id=thread_id,
            runtime_generation=GENERATION,
            agent_id=agent_id,
            runtime_attach_token=ATTACH_TOKEN,
            pod_uid=POD_UID,
        )
    )
    assert ATTACH_TOKEN not in response.model_dump_json()
    assert payload["capabilities"]["pinned_session_identity_contract"] == 1


async def _attach_failure(error: BaseException, *, workspace: bool = False) -> None:
    assert dual_app._session_attach_claim is not None
    # The one-way proof boundary must already have flipped before production
    # can enter even the first line of PersistentSession attach.
    assert dual_app._session_attach_claim["setup_started"] is True
    assert persistent_app._session_attach.retain_release_receipt(
        _receipt(workspace=workspace)
    )
    raise error


def _failing_attach(error: BaseException, *, workspace: bool = False):
    async def _fail(**_kwargs):
        await _attach_failure(error, workspace=workspace)

    return _fail


@pytest.mark.asyncio
async def test_actor_refusal_uses_only_monotonic_pre_setup_proof():
    client = _client(bound=False, release=True)
    dual_app._orchestrator_client = client

    with patch.object(persistent_app._session_attach, "attach", AsyncMock()) as setup:
        response = await _attach_endpoint()(_request(workspace=True))
        assert response.status_code == 409
        task = dual_app._session_attach_task
        assert task is not None
        await task

    setup.assert_not_awaited()
    client.release_thread_agent.assert_awaited_once_with(
        "thread-a",
        session_runtime_generation=GENERATION,
        session_runtime_attach_token=ATTACH_TOKEN,
        agent_pod_uid=POD_UID,
        local_runtime_quiesced=True,
        local_quiescence_protocol="agent_attach_not_started_v1",
        workspace_generation=WORKSPACE_GENERATION,
        workspace_runtime_incarnation=WORKSPACE_RUNTIME,
    )
    assert dual_app._session_attach_claim is None
    assert dual_app._pod_state is dual_app.PodState.IDLE


@pytest.mark.asyncio
async def test_setup_forwards_canonical_workspace_identity_to_shared_attach():
    client = _client()
    dual_app._orchestrator_client = client

    with patch.object(persistent_app._session_attach, "attach", AsyncMock()) as setup:
        response = await _attach_endpoint()(_request(workspace=True))
        assert response.status_code == 200
        await dual_app._session_attach_task

    setup.assert_awaited_once()
    kwargs = setup.await_args.kwargs
    assert kwargs["workspace_generation"] == WORKSPACE_GENERATION
    assert kwargs["workspace_runtime_incarnation"] == WORKSPACE_RUNTIME


@pytest.mark.asyncio
async def test_pre_setup_release_replays_same_proof_until_already_detached(monkeypatch):
    retry_sleep = AsyncMock()
    monkeypatch.setattr(session_attach.asyncio, "sleep", retry_sleep)
    outcomes = iter((False, False, True))
    client = _client(bound=False, release=lambda *_a, **_kw: next(outcomes))
    dual_app._orchestrator_client = client

    with patch.object(
        session_termination, "_EXACT_RETIREMENT_SETTLEMENT_RETRY_DELAYS", (0.0,)
    ):
        response = await _attach_endpoint()(_request(workspace=True))
        assert response.status_code == 409
        await dual_app._session_attach_task

    assert client.release_thread_agent.await_count == 3
    assert [call.args[0] for call in retry_sleep.await_args_list] == [0.0, 0.0]
    assert (
        len({str(call.kwargs) for call in client.release_thread_agent.await_args_list})
        == 1
    )
    assert dual_app._pod_state is dual_app.PodState.IDLE


@pytest.mark.asyncio
async def test_setup_started_failure_cannot_emit_pre_setup_protocol():
    client = _client(release=True)
    dual_app._orchestrator_client = client

    with patch.object(
        persistent_app._session_attach,
        "attach",
        side_effect=_failing_attach(
            RuntimeError("failed before manager construction"), workspace=True
        ),
    ):
        response = await _attach_endpoint()(_request(workspace=True))
        assert response.status_code == 200
        await dual_app._session_attach_task

    release = client.release_thread_agent.await_args
    assert release.kwargs["local_quiescence_protocol"] == "workspace_process_zero_v1"
    assert release.kwargs["local_quiescence_protocol"] != "agent_attach_not_started_v1"
    assert dual_app._pod_state is dual_app.PodState.IDLE


@pytest.mark.asyncio
async def test_task_creation_failure_releases_and_scrubs_bound_actor(monkeypatch):
    client = _client(release=True)
    dual_app._orchestrator_client = client

    def fail_task_creation(_coro, **_kwargs):
        raise RuntimeError("event loop refused task")

    monkeypatch.setattr(dual_app.asyncio, "create_task", fail_task_creation)
    with patch.object(persistent_app._session_attach, "attach", AsyncMock()) as setup:
        response = await _attach_endpoint()(_request(workspace=True))

    assert response.status_code == 500
    setup.assert_not_awaited()
    assert (
        client.release_thread_agent.await_args.kwargs["local_quiescence_protocol"]
        == "agent_attach_not_started_v1"
    )
    client.clear_runtime_actor.assert_called_once_with()
    assert dual_app._session_attach_claim is None
    assert dual_app._pod_state is dual_app.PodState.IDLE


@pytest.mark.asyncio
async def test_unconfirmed_partial_setup_release_stays_session_and_nonready():
    release_seen = asyncio.Event()

    async def refuse_release(*_args, **_kwargs):
        release_seen.set()
        return False

    client = _client(release=refuse_release)
    dual_app._orchestrator_client = client
    with (
        patch.object(
            persistent_app._session_attach,
            "attach",
            side_effect=_failing_attach(RuntimeError("overlay refused")),
        ),
        patch.object(
            session_termination,
            "_EXACT_RETIREMENT_SETTLEMENT_RETRY_DELAYS",
            (0.0, 0.01),
        ),
    ):
        response = await _attach_endpoint()(_request())
        assert response.status_code == 200
        task = dual_app._session_attach_task
        await release_seen.wait()
        assert dual_app._session_attach_claim["setup_started"] is True
        assert dual_app._pod_state is dual_app.PodState.SESSION
        assert persistent_app._session_ready() is False
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    assert persistent_app._session_attach._release_receipt == _receipt()
    assert dual_app._pod_state is dual_app.PodState.SESSION


@pytest.mark.asyncio
async def test_cancelled_setup_uses_process_zero_before_propagating():
    client = _client(release=True)
    dual_app._orchestrator_client = client
    with patch.object(
        persistent_app._session_attach,
        "attach",
        side_effect=_failing_attach(asyncio.CancelledError()),
    ):
        response = await _attach_endpoint()(_request())
        assert response.status_code == 200
        with pytest.raises(asyncio.CancelledError):
            await dual_app._session_attach_task

    assert (
        client.release_thread_agent.await_args.kwargs["local_quiescence_protocol"]
        == "agent_runtime_zero_v1"
    )
    assert dual_app._pod_state is dual_app.PodState.IDLE


@pytest.mark.asyncio
async def test_delayed_actor_refusal_never_releases_successor_claim():
    bind_entered = asyncio.Event()
    release_bind = asyncio.Event()
    client = _client()

    async def delayed_refusal(_thread_id):
        bind_entered.set()
        await release_bind.wait()
        return False

    client.bind_pod_runtime_actor = AsyncMock(side_effect=delayed_refusal)
    dual_app._orchestrator_client = client
    endpoint_task = asyncio.create_task(_attach_endpoint()(_request()))
    await bind_entered.wait()
    successor = {
        **(dual_app._session_attach_claim or {}),
        "session_runtime_generation": "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee",
    }
    dual_app._session_attach_claim = successor
    release_bind.set()
    response = await endpoint_task

    assert response.status_code == 409
    client.release_thread_agent.assert_not_awaited()
    client.clear_runtime_actor.assert_not_called()
    assert dual_app._session_attach_claim is successor
    assert dual_app._pod_state is dual_app.PodState.SESSION
