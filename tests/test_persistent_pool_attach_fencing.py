"""Warm persistent-pool admission stays fenced until exact DB release."""

from __future__ import annotations

import asyncio
from contextlib import ExitStack
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import agent.api.persistent_app as app


GENERATION = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
ATTACH_TOKEN = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
WORKSPACE_GENERATION = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
WORKSPACE_INCARNATION = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
OTHER_WORKSPACE_GENERATION = "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"


def _release_receipt(thread_id="thread-a", generation=GENERATION, token=ATTACH_TOKEN):
    return {
        "thread_id": thread_id,
        "session_runtime_generation": generation,
        "session_runtime_attach_token": token,
        "agent_pod_uid": "pod-uid-a",
        "local_runtime_quiesced": True,
        "local_quiescence_protocol": "agent_runtime_zero_v1",
        "workspace_generation": None,
        "workspace_runtime_incarnation": None,
    }


def _request(thread_id="thread-a"):
    return {
        "thread_id": thread_id,
        "pinned_runtime_generation_contract": 1,
        "session_runtime_generation": GENERATION,
        "session_runtime_attach_token": ATTACH_TOKEN,
    }


def _partial_cleanup_context(*, setup_started: bool):
    return {
        "thread_id": "thread-a",
        "setup_started": setup_started,
        "workspace_tier": "sandbox",
        "workspace_generation": WORKSPACE_GENERATION,
        "workspace_runtime_incarnation": WORKSPACE_INCARNATION,
        "workspace_ssh_host_key_fingerprint": "SHA256:exact-host",
        "remote": {
            "host": "workspace.internal",
            "port": 22,
            "username": "agent-host",
            "key_path": "/run/secrets/workspace-key",
        },
        "datasources": {},
        "datasource_clients": {},
    }


def _partial_cleanup_patchers(context):
    return [
        patch.object(app, "_session", None),
        patch.object(app._session_identity, "_thread_id", "thread-a"),
        patch.object(app._session_identity, "_session_generation", GENERATION),
        patch.object(app._session_identity, "_attach_token", ATTACH_TOKEN),
        patch.object(app._session_identity, "_runtime_contract", True),
        patch.object(app, "_failed_attach_workspace_cleanup_context", context),
        patch.object(app, "_failed_attach_release_receipt", None),
        patch.object(app, "_event_writer", None),
        patch.object(app, "_stop_thread_interrupt_watcher", new=AsyncMock()),
        patch.object(app, "_stop_thread_control_watcher", new=AsyncMock()),
        patch.object(app, "_stop_and_join_watchdogs", new=AsyncMock()),
        patch.object(app, "_quiesce_session_side_tasks", new=AsyncMock()),
        patch.object(app._session_identity, "clear", return_value=True),
        patch.object(app, "_clear_attached_runtime_actor"),
        patch.object(app, "_apply_session_embedding_env"),
        patch("agent.tools.registry.register_mcp_tools"),
        patch.dict(app.os.environ, {"POD_UID": "pod-uid-a"}),
    ]


@pytest.fixture(autouse=True)
def _restore_pool_globals():
    # Real attach tests adopt lifecycle identity before session construction.
    saved = (
        app._session,
        app._session_identity.thread_id,
        app._pool_attach_claim,
        app._pool_attach_runtime_generation,
        app._pool_attach_token,
        app._pool_attach_task,
        app._failed_attach_release_receipt,
        app._failed_attach_workspace_cleanup_context,
        app._orchestrator_client,
        app._heartbeat_task,
        app._session_identity.session_generation,
        app._session_identity.attach_token,
        app._session_identity.runtime_contract,
        app._session_identity.status_contract,
        app._retirement_admission_identity,
        app._retirement_admission_disposition,
        app._retirement_admission_token,
        app._retirement_admission_permanent,
        app._runtime_authorization_admission_open,
        app._session_identity.attach_generation,
    )
    app._session = None
    app._session_identity._thread_id = None
    app._pool_attach_claim = None
    app._pool_attach_runtime_generation = None
    app._pool_attach_token = None
    app._pool_attach_task = None
    app._failed_attach_release_receipt = None
    app._failed_attach_workspace_cleanup_context = None
    app._heartbeat_task = None
    yield
    (
        app._session,
        app._session_identity._thread_id,
        app._pool_attach_claim,
        app._pool_attach_runtime_generation,
        app._pool_attach_token,
        app._pool_attach_task,
        app._failed_attach_release_receipt,
        app._failed_attach_workspace_cleanup_context,
        app._orchestrator_client,
        app._heartbeat_task,
        app._session_identity._session_generation,
        app._session_identity._attach_token,
        app._session_identity._runtime_contract,
        app._session_identity._status_contract,
        app._retirement_admission_identity,
        app._retirement_admission_disposition,
        app._retirement_admission_token,
        app._retirement_admission_permanent,
        app._runtime_authorization_admission_open,
        app._session_identity._attach_generation,
    ) = saved


def test_pool_claim_is_non_ready_before_session_construction():
    assert app._pool_heartbeat_status() == "ready"
    app._pool_attach_claim = "thread-a"
    app._pool_attach_runtime_generation = GENERATION
    app._pool_attach_token = ATTACH_TOKEN
    app._failed_attach_release_receipt = _release_receipt()
    assert app._session is None
    assert app._pool_heartbeat_status() == "session"


@pytest.mark.asyncio
async def test_real_attach_boundary_forwards_workspace_identity_pair():
    inner = AsyncMock()
    with patch.object(app, "_attach_session_inner", inner):
        await app._attach_session(
            thread_id="thread-a",
            workspace_generation=WORKSPACE_GENERATION,
            workspace_runtime_incarnation=WORKSPACE_INCARNATION,
        )

    kwargs = inner.await_args.kwargs
    assert kwargs["workspace_generation"] == WORKSPACE_GENERATION
    assert kwargs["workspace_runtime_incarnation"] == WORKSPACE_INCARNATION


@pytest.mark.parametrize(
    ("workspace_generation", "workspace_runtime_incarnation"),
    (
        (WORKSPACE_GENERATION, None),
        (None, WORKSPACE_INCARNATION),
        ("not-a-uuid", WORKSPACE_INCARNATION),
    ),
)
def test_attach_workspace_identity_requires_canonical_pair(
    workspace_generation, workspace_runtime_incarnation
):
    with pytest.raises(app.WorkspaceNotReady, match="malformed or incomplete"):
        app._canonical_attach_workspace_identity(
            workspace_generation,
            workspace_runtime_incarnation,
        )


def test_null_workspace_pair_allows_attach_to_poll_later_physical_identity():
    expected = app._canonical_attach_workspace_identity(None, None)

    app._assert_attach_workspace_tier(expected, is_lite_session=False)
    app._assert_attach_workspace_payload(
        expected,
        {
            "workspace_generation": WORKSPACE_GENERATION,
            "workspace_runtime_incarnation": WORKSPACE_INCARNATION,
        },
    )


@pytest.mark.asyncio
async def test_pre_setup_attach_abort_uses_only_monotonic_not_started_proof():
    context = _partial_cleanup_context(setup_started=False)
    sandbox_zero = AsyncMock()
    patchers = _partial_cleanup_patchers(context) + [
        patch.object(app, "_strict_cleanup_partial_sandbox_workspace", sandbox_zero)
    ]
    with ExitStack() as stack:
        for patcher in patchers:
            stack.enter_context(patcher)
        receipt = await app._cleanup_failed_event_journal_attach("thread-a")

    assert receipt == {
        "thread_id": "thread-a",
        "session_runtime_generation": GENERATION,
        "session_runtime_attach_token": ATTACH_TOKEN,
        "agent_pod_uid": "pod-uid-a",
        "local_runtime_quiesced": True,
        "local_quiescence_protocol": "agent_attach_not_started_v1",
        "workspace_generation": WORKSPACE_GENERATION,
        "workspace_runtime_incarnation": WORKSPACE_INCARNATION,
    }
    sandbox_zero.assert_not_awaited()


@pytest.mark.asyncio
async def test_partial_sandbox_setup_requires_actual_workspace_process_zero():
    context = _partial_cleanup_context(setup_started=True)
    local_zero = AsyncMock()
    sandbox_zero = AsyncMock(return_value="workspace_process_zero_v1")
    patchers = _partial_cleanup_patchers(context) + [
        patch.object(app, "_strict_cleanup_partial_attach_local_resources", local_zero),
        patch.object(app, "_strict_cleanup_partial_sandbox_workspace", sandbox_zero),
    ]
    with ExitStack() as stack:
        for patcher in patchers:
            stack.enter_context(patcher)
        receipt = await app._cleanup_failed_event_journal_attach("thread-a")

    local_zero.assert_awaited_once_with(context)
    sandbox_zero.assert_awaited_once_with(context)
    assert receipt["local_quiescence_protocol"] == "workspace_process_zero_v1"
    assert receipt["workspace_generation"] == WORKSPACE_GENERATION
    assert receipt["workspace_runtime_incarnation"] == WORKSPACE_INCARNATION


@pytest.mark.asyncio
async def test_partial_sandbox_zero_uses_attested_remote_identity():
    context = _partial_cleanup_context(setup_started=True)
    backend = MagicMock()
    backend.connect = MagicMock()
    backend.protected_workspace_zero_cleanup_strict = MagicMock(
        return_value="workspace_process_zero_v1"
    )
    backend.disconnect = MagicMock()

    with patch(
        "shared.runtime.core.backends.remote.RemoteBackend", return_value=backend
    ) as cls:
        assert (
            await app._strict_cleanup_partial_sandbox_workspace(context)
            == "workspace_process_zero_v1"
        )

    cls.assert_called_once_with(
        host="workspace.internal",
        port=22,
        username="agent-host",
        key_path="/run/secrets/workspace-key",
        workspace_path="/home/agent-host/workspace",
        job_id="thread-a",
        connect_timeout=30,
        max_retries=5,
        retry_timeouts_as_booting=False,
        sudo_action="freeze",
        workspace_generation=WORKSPACE_GENERATION,
        runtime_incarnation=WORKSPACE_INCARNATION,
        expected_host_key_fingerprint="SHA256:exact-host",
        workspace_tier="sandbox",
    )
    backend.connect.assert_called_once()
    backend.protected_workspace_zero_cleanup_strict.assert_called_once()
    backend.disconnect.assert_called_once()


@pytest.mark.asyncio
async def test_real_attach_crosses_one_way_setup_boundary_before_constructor():
    """Constructor failure cannot downgrade a delivered attach to pre-setup.

    This exercises the production attach ladder through both workspace reads.
    The one-way latch must flip before ``PersistentSession`` construction, so
    deleting or moving that assignment cannot leave the helper-only proof
    tests green.
    """

    from shared.runtime.core.loader import AgentConfig

    workspace = {
        "status": "ready",
        "backend": "sandbox",
        "protected_cloud": False,
        "remote": {
            "host": "workspace.internal",
            "port": 22,
            "username": "agent-host",
            "key_path": "/run/secrets/workspace-key",
        },
        "workspace_generation": WORKSPACE_GENERATION,
        "workspace_runtime_incarnation": WORKSPACE_INCARNATION,
        "workspace_ssh_host_key_fingerprint": "SHA256:exact-host",
        "pinned_runtime_generation_contract": 1,
        "session_runtime_generation": GENERATION,
        "session_runtime_attach_token": ATTACH_TOKEN,
    }
    client = SimpleNamespace(
        get_thread_workspace=AsyncMock(return_value=workspace),
        session_runtime_generation=GENERATION,
        session_runtime_attach_token=ATTACH_TOKEN,
        pinned_runtime_generation_contract=True,
        runtime_actor=None,
        adopt_session_runtime_identity=MagicMock(return_value=True),
        clear_session_runtime_identity=MagicMock(return_value=True),
    )
    agent = SimpleNamespace(
        config=AgentConfig(agent_id="pool", display_name="Pool"),
        _tactical_llm=None,
        _llm=object(),
        _auxiliary_llm=object(),
        postgres_conn=None,
        vector_conn=None,
    )
    constructor_failure = RuntimeError("constructor refused")

    with (
        patch.object(app, "_agent", agent),
        patch.object(app, "_orchestrator_client", client),
        patch.object(app, "_session", None),
        patch.object(app._session_identity, "_thread_id", None),
        patch.object(app, "_event_writer", None),
        patch.object(app, "_failed_attach_workspace_cleanup_context", None),
        patch.object(app, "_poll_workspace_ready", AsyncMock(return_value=workspace)),
        patch.object(app, "PersistentSession", side_effect=constructor_failure),
        patch.object(app, "_apply_session_embedding_env"),
        patch("agent.tools.registry.register_mcp_tools"),
    ):
        with pytest.raises(RuntimeError, match="constructor refused"):
            await app._attach_session_inner(
                thread_id="thread-a",
                pinned_runtime_generation_contract=1,
                session_runtime_generation=GENERATION,
                session_runtime_attach_token=ATTACH_TOKEN,
                workspace_generation=WORKSPACE_GENERATION,
                workspace_runtime_incarnation=WORKSPACE_INCARNATION,
            )

        context = app._failed_attach_workspace_cleanup_context
        assert isinstance(context, dict)
        assert context["setup_started"] is True
        assert context["workspace_tier"] == "sandbox"
        assert context["workspace_generation"] == WORKSPACE_GENERATION
        assert context["workspace_runtime_incarnation"] == WORKSPACE_INCARNATION


@pytest.mark.asyncio
async def test_real_attach_rejects_workspace_identity_drift_before_constructor():
    """Expected claim identity is authority, but never cleanup proof by itself."""

    workspace = {
        "status": "ready",
        "backend": "sandbox",
        "protected_cloud": False,
        "remote": {
            "host": "workspace.internal",
            "port": 22,
            "username": "agent-host",
            "key_path": "/run/secrets/workspace-key",
        },
        "workspace_generation": OTHER_WORKSPACE_GENERATION,
        "workspace_runtime_incarnation": WORKSPACE_INCARNATION,
        "workspace_ssh_host_key_fingerprint": "SHA256:exact-host",
        "pinned_runtime_generation_contract": 1,
        "session_runtime_generation": GENERATION,
        "session_runtime_attach_token": ATTACH_TOKEN,
    }
    client = SimpleNamespace(
        get_thread_workspace=AsyncMock(return_value=workspace),
        session_runtime_generation=GENERATION,
        session_runtime_attach_token=ATTACH_TOKEN,
        pinned_runtime_generation_contract=True,
        runtime_actor=None,
        adopt_session_runtime_identity=MagicMock(return_value=True),
        clear_session_runtime_identity=MagicMock(return_value=True),
    )
    constructor = MagicMock()

    with (
        patch.object(app, "_orchestrator_client", client),
        patch.object(app, "_session", None),
        patch.object(app._session_identity, "_thread_id", None),
        patch.object(app, "_event_writer", None),
        patch.object(app, "_failed_attach_workspace_cleanup_context", None),
        patch.object(app._session_identity, "_session_generation", None),
        patch.object(app._session_identity, "_attach_token", None),
        patch.object(app._session_identity, "_runtime_contract", False),
        patch.object(app, "_retirement_admission_identity", None),
        patch.object(app, "_retirement_admission_disposition", None),
        patch.object(app, "_retirement_admission_token", None),
        patch.object(app, "_retirement_admission_permanent", None),
        patch.object(app, "PersistentSession", constructor),
    ):
        with pytest.raises(app.WorkspaceNotReady, match="identity changed"):
            await app._attach_session_inner(
                thread_id="thread-a",
                pinned_runtime_generation_contract=1,
                session_runtime_generation=GENERATION,
                session_runtime_attach_token=ATTACH_TOKEN,
                workspace_generation=WORKSPACE_GENERATION,
                workspace_runtime_incarnation=WORKSPACE_INCARNATION,
            )

        constructor.assert_not_called()
        context = app._failed_attach_workspace_cleanup_context
        assert isinstance(context, dict)
        assert context["setup_started"] is False
        assert context["workspace_generation"] is None
        assert context["workspace_runtime_incarnation"] is None


@pytest.mark.asyncio
async def test_partial_cleanup_failure_keeps_exact_retry_owner_until_proven():
    context = _partial_cleanup_context(setup_started=True)
    receipt = _release_receipt()
    cleanup = AsyncMock(
        side_effect=[app.EventJournalUnavailable("writer remains"), receipt]
    )
    with (
        patch.object(app, "_session", None),
        patch.object(app._session_identity, "_thread_id", "thread-a"),
        patch.object(app._session_identity, "_session_generation", GENERATION),
        patch.object(app._session_identity, "_attach_token", ATTACH_TOKEN),
        patch.object(app._session_identity, "_runtime_contract", True),
        patch.object(app, "_failed_attach_workspace_cleanup_context", context),
        patch.object(app, "_cleanup_failed_event_journal_attach", cleanup),
        patch.object(app, "_EXACT_RETIREMENT_SETTLEMENT_RETRY_DELAYS", (0.0,)),
    ):
        assert await app._cleanup_failed_attach_until_proven("thread-a") == receipt

    assert cleanup.await_count == 2


@pytest.mark.asyncio
async def test_unconfirmed_failure_retains_claim_and_non_ready_fence():
    client = MagicMock()
    client.release_thread_agent = AsyncMock(return_value=False)
    app._orchestrator_client = client
    app._pool_attach_claim = "thread-a"
    app._pool_attach_runtime_generation = GENERATION
    app._pool_attach_token = ATTACH_TOKEN
    app._failed_attach_release_receipt = _release_receipt()

    with (
        patch.object(
            app,
            "_attach_session",
            AsyncMock(side_effect=RuntimeError("overlay refused")),
        ),
        patch.object(app, "_EXACT_RETIREMENT_SETTLEMENT_RETRY_DELAYS", (0.0, 0.01)),
    ):
        task = asyncio.create_task(
            app._run_pool_attach_transaction("thread-a", {}, GENERATION, ATTACH_TOKEN)
        )
        app._pool_attach_task = task
        while client.release_thread_agent.await_count < 1:
            await asyncio.sleep(0)
        assert app._pool_attach_claim == "thread-a"
        assert app._pool_heartbeat_status() == "session"
        task.cancel()
        await task

    client.release_thread_agent.assert_awaited_with(
        "thread-a",
        session_runtime_generation=GENERATION,
        session_runtime_attach_token=ATTACH_TOKEN,
        agent_pod_uid="pod-uid-a",
        local_runtime_quiesced=True,
        local_quiescence_protocol="agent_runtime_zero_v1",
        workspace_generation=None,
        workspace_runtime_incarnation=None,
    )
    assert app._pool_attach_claim == "thread-a"
    assert app._pool_attach_task is task
    assert app._pool_heartbeat_status() == "session"


@pytest.mark.asyncio
async def test_confirmed_failure_release_reopens_pool_once():
    client = MagicMock()
    client.release_thread_agent = AsyncMock(return_value=True)
    app._orchestrator_client = client
    app._pool_attach_claim = "thread-a"
    app._pool_attach_runtime_generation = GENERATION
    app._pool_attach_token = ATTACH_TOKEN
    app._failed_attach_release_receipt = _release_receipt()

    with patch.object(
        app, "_attach_session", AsyncMock(side_effect=RuntimeError("lower refused"))
    ):
        task = asyncio.create_task(
            app._run_pool_attach_transaction("thread-a", {}, GENERATION, ATTACH_TOKEN)
        )
        app._pool_attach_task = task
        await task

    assert app._pool_attach_claim is None
    assert app._pool_attach_task is None
    assert app._pool_heartbeat_status() == "ready"


@pytest.mark.asyncio
async def test_lost_release_responses_replay_identical_proof_until_confirmed():
    """A committed abort is learned only from released/already-detached.

    Transport loss is represented by ``False`` at the client boundary.  The
    pool must replay the immutable G/attach/process-zero receipt, stay
    nonclaimable between attempts, and reopen only after the server proves the
    old generation was released (including its durable ``already_detached``
    replay outcome).
    """

    observations: list[tuple[str | None, dict]] = []
    client = MagicMock()

    async def release(*args, **kwargs):
        observations.append((app._pool_attach_claim, dict(kwargs)))
        return len(observations) >= 3

    client.release_thread_agent = AsyncMock(side_effect=release)
    app._orchestrator_client = client
    app._pool_attach_claim = "thread-a"
    app._pool_attach_runtime_generation = GENERATION
    app._pool_attach_token = ATTACH_TOKEN
    app._failed_attach_release_receipt = _release_receipt()

    with (
        patch.object(
            app,
            "_attach_session",
            AsyncMock(side_effect=RuntimeError("delivered attach failed")),
        ),
        patch.object(app, "_EXACT_RETIREMENT_SETTLEMENT_RETRY_DELAYS", (0.0,)),
    ):
        task = asyncio.create_task(
            app._run_pool_attach_transaction("thread-a", {}, GENERATION, ATTACH_TOKEN)
        )
        app._pool_attach_task = task
        await task

    assert len(observations) == 3
    assert all(claim == "thread-a" for claim, _ in observations)
    assert observations[0][1] == observations[1][1] == observations[2][1]
    assert observations[0][1] == {
        "session_runtime_generation": GENERATION,
        "session_runtime_attach_token": ATTACH_TOKEN,
        "agent_pod_uid": "pod-uid-a",
        "local_runtime_quiesced": True,
        "local_quiescence_protocol": "agent_runtime_zero_v1",
        "workspace_generation": None,
        "workspace_runtime_incarnation": None,
    }
    assert app._failed_attach_release_receipt is None
    assert app._pool_attach_claim is None
    assert app._pool_attach_task is None
    assert app._pool_heartbeat_status() == "ready"


@pytest.mark.asyncio
async def test_late_generation_one_cleanup_cannot_clear_generation_two_claim():
    generation_two = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
    token_two = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
    client = MagicMock()

    async def release_and_rebind(*_args, **_kwargs):
        app._pool_attach_claim = "thread-a"
        app._pool_attach_runtime_generation = generation_two
        app._pool_attach_token = token_two
        return True

    client.release_thread_agent = AsyncMock(side_effect=release_and_rebind)
    app._orchestrator_client = client
    app._pool_attach_claim = "thread-a"
    app._pool_attach_runtime_generation = GENERATION
    app._pool_attach_token = ATTACH_TOKEN
    app._failed_attach_release_receipt = _release_receipt()

    with patch.object(
        app,
        "_attach_session",
        AsyncMock(side_effect=RuntimeError("generation one attach failed")),
    ):
        task = asyncio.create_task(
            app._run_pool_attach_transaction("thread-a", {}, GENERATION, ATTACH_TOKEN)
        )
        app._pool_attach_task = task
        await task

    assert app._pool_attach_claim == "thread-a"
    assert app._pool_attach_runtime_generation == generation_two
    assert app._pool_attach_token == token_two
    assert app._pool_heartbeat_status() == "session"


@pytest.mark.asyncio
async def test_admission_claims_before_blocking_setup_and_rejects_second_attach():
    entered = asyncio.Event()
    blocker = asyncio.Event()
    client = MagicMock()
    client.release_thread_agent = AsyncMock(return_value=True)
    app._orchestrator_client = client

    async def blocked_attach(**_kwargs):
        entered.set()
        await blocker.wait()

    with patch.object(app, "_attach_session", side_effect=blocked_attach):
        first = await app._admit_pool_session_attach(_request())
        assert first.status_code == 200
        assert app._pool_attach_claim == "thread-a"
        assert app._pool_heartbeat_status() == "session"
        await entered.wait()

        second = await app._admit_pool_session_attach(_request("thread-b"))
        assert second.status_code == 409

        task = app._pool_attach_task
        assert task is not None
        app._failed_attach_release_receipt = _release_receipt()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    assert app._pool_attach_claim is None
    client.release_thread_agent.assert_awaited_once_with(
        "thread-a",
        session_runtime_generation=GENERATION,
        session_runtime_attach_token=ATTACH_TOKEN,
        agent_pod_uid="pod-uid-a",
        local_runtime_quiesced=True,
        local_quiescence_protocol="agent_runtime_zero_v1",
        workspace_generation=None,
        workspace_runtime_incarnation=None,
    )


@pytest.mark.asyncio
async def test_stale_release_receipt_never_clears_current_claim():
    client = MagicMock()
    client.release_thread_agent = AsyncMock(return_value=True)
    app._orchestrator_client = client
    app._pool_attach_claim = "thread-a"
    app._pool_attach_runtime_generation = GENERATION
    app._pool_attach_token = ATTACH_TOKEN
    app._failed_attach_release_receipt = _release_receipt(
        generation="cccccccc-cccc-4ccc-8ccc-cccccccccccc"
    )

    with patch.object(
        app, "_attach_session", AsyncMock(side_effect=RuntimeError("failed"))
    ):
        task = asyncio.create_task(
            app._run_pool_attach_transaction("thread-a", {}, GENERATION, ATTACH_TOKEN)
        )
        app._pool_attach_task = task
        await task

    client.release_thread_agent.assert_not_awaited()
    assert app._pool_attach_claim == "thread-a"
    assert app._pool_heartbeat_status() == "session"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "exit_helper_name",
    (
        "_exit_workspace_not_ready",
        "_exit_grant_denied",
        "_exit_memory_unavailable",
    ),
)
async def test_dedicated_attach_failure_releases_receipt_before_exit(
    exit_helper_name, monkeypatch
):
    order: list[str] = []
    client = MagicMock()

    async def release(*_args, **_kwargs):
        order.append("release")
        return True

    async def deregister():
        order.append("deregister")

    async def close():
        order.append("close")

    client.release_thread_agent = AsyncMock(side_effect=release)
    client.deregister = AsyncMock(side_effect=deregister)
    client.close = AsyncMock(side_effect=close)
    client.stop_heartbeat = MagicMock()
    app._orchestrator_client = client
    app._failed_attach_release_receipt = _release_receipt()

    def exit_process(code):
        order.append(f"exit:{code}")
        raise SystemExit(code)

    monkeypatch.setattr(app.os, "_exit", exit_process)
    with pytest.raises(SystemExit):
        await getattr(app, exit_helper_name)("thread-a", RuntimeError("attach failed"))

    assert order == ["release", "deregister", "close", "exit:0"]
    assert app._failed_attach_release_receipt is None


@pytest.mark.asyncio
async def test_dedicated_exit_stays_nonready_while_release_is_unconfirmed(monkeypatch):
    release_seen = asyncio.Event()
    client = MagicMock()

    async def unresolved(*_args, **_kwargs):
        release_seen.set()
        return False

    client.release_thread_agent = AsyncMock(side_effect=unresolved)
    client.deregister = AsyncMock()
    client.close = AsyncMock()
    app._orchestrator_client = client
    app._failed_attach_release_receipt = _release_receipt()
    exit_process = MagicMock()
    monkeypatch.setattr(app.os, "_exit", exit_process)

    with patch.object(app, "_EXACT_RETIREMENT_SETTLEMENT_RETRY_DELAYS", (0.0, 0.01)):
        task = asyncio.create_task(
            app._exit_workspace_not_ready("thread-a", RuntimeError("attach failed"))
        )
        await release_seen.wait()
        assert app._pool_heartbeat_status() == "session"
        client.deregister.assert_not_awaited()
        exit_process.assert_not_called()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    assert app._failed_attach_release_receipt == _release_receipt()
    assert app._pool_heartbeat_status() == "session"
