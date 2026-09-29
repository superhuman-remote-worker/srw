"""P0 Officer runtime-grant liveness and no-spend unit contracts."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
from langchain_core.messages import AIMessage

from agent.api.orchestrator_client import OrchestratorClient
from agent.persistent_graph import PersistentLoopCallbacks, run_persistent_loop
from shared.runtime_actor import (
    RUNTIME_ACTOR_MAINTENANCE_PHASE_HEADER,
    RUNTIME_ACTOR_MAINTENANCE_PHASE_PRE_TURN,
    RUNTIME_ACTOR_REFRESH_HEADER,
    RuntimeActorContext,
)


PROJECT_ID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
THREAD_ID = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
USER_ID = "cccccccc-cccc-cccc-cccc-cccccccccccc"


def _actor(*, kind: str = "officer", expires_in: timedelta) -> RuntimeActorContext:
    now = datetime.now(timezone.utc)
    return RuntimeActorContext(
        caller_kind=kind,
        project_id=PROJECT_ID,
        project_role="owner" if kind == "officer" else None,
        thread_id=THREAD_ID if kind == "officer" else None,
        officer_incarnation=0 if kind == "officer" else None,
        user_id=USER_ID,
        access_credential="sra_" + "A" * 43,
        refresh_credential="srr_" + "B" * 43,
        access_expires_at=now + timedelta(minutes=5),
        refresh_expires_at=now + expires_in,
    )


def _client(actor: RuntimeActorContext) -> tuple[OrchestratorClient, AsyncMock]:
    client = OrchestratorClient(
        orchestrator_url="http://orchestrator.test",
        pod_ip="127.0.0.1",
        pod_port=8002,
        hostname="officer-test",
        config_name="persistent_defaults",
        pid=123,
    )
    client.runtime_actor = actor
    http = AsyncMock()
    client._client = http
    return client, http


def _pre_turn_headers(refresh_token: str) -> dict[str, str]:
    return {
        RUNTIME_ACTOR_REFRESH_HEADER: refresh_token,
        RUNTIME_ACTOR_MAINTENANCE_PHASE_HEADER: (
            RUNTIME_ACTOR_MAINTENANCE_PHASE_PRE_TURN
        ),
    }


@pytest.mark.asyncio
async def test_heartbeat_maintenance_renews_without_a_privileged_tool_call():
    actor = _actor(expires_in=timedelta(hours=5))
    client, http = _client(actor)
    next_expiry = datetime.now(timezone.utc) + timedelta(hours=24)
    http.post.return_value = MagicMock(
        status_code=200,
        json=lambda: {
            "runtime_actor": {
                **actor.audit_payload(),
                "access_credential": "sra_" + "C" * 43,
                "refresh_credential": "srr_" + "D" * 43,
                "access_expires_at": (
                    datetime.now(timezone.utc) + timedelta(minutes=5)
                ).isoformat(),
                "refresh_expires_at": next_expiry.isoformat(),
            }
        },
    )

    maintained, _ = await client.maintain_runtime_actor(force=False)

    assert maintained
    assert client.runtime_actor is actor
    assert actor.refresh_credential == "srr_" + "D" * 43
    assert actor.refresh_expires_at == next_expiry
    assert http.post.await_args.kwargs["headers"] == {
        RUNTIME_ACTOR_REFRESH_HEADER: "srr_" + "B" * 43
    }


@pytest.mark.asyncio
async def test_maintenance_is_a_local_noop_outside_the_renewal_window():
    client, http = _client(_actor(expires_in=timedelta(hours=8)))

    maintained, _ = await client.maintain_runtime_actor(force=False)

    assert maintained
    http.post.assert_not_awaited()


@pytest.mark.asyncio
async def test_successful_officer_heartbeat_runs_credential_maintenance():
    client, _http = _client(_actor(expires_in=timedelta(hours=5)))
    client.agent_id = "agent-1"
    maintain = AsyncMock(return_value=(True, "renewed"))
    client.maintain_runtime_actor = maintain

    async def _heartbeat(*_args, **_kwargs):
        client.stop_heartbeat()
        return {"intents": {}}

    client.heartbeat = AsyncMock(side_effect=_heartbeat)
    await client.run_heartbeat_loop(
        get_status=lambda: "session",
        get_job_id=lambda: None,
        get_metrics=lambda: None,
    )

    maintain.assert_awaited_once_with(force=False)


@pytest.mark.asyncio
async def test_worker_grants_do_not_gain_officer_renewal_semantics():
    client, http = _client(_actor(kind="worker", expires_in=timedelta(seconds=-1)))

    maintained, _ = await client.maintain_runtime_actor(force=True)

    assert maintained
    http.post.assert_not_awaited()


@pytest.mark.asyncio
async def test_maintenance_failure_is_bounded_and_does_not_persist_details(
    monkeypatch, caplog
):
    monkeypatch.setenv("RUNTIME_ACTOR_RETRY_BASE_SECONDS", "60")
    actor = _actor(expires_in=timedelta(seconds=-1))
    client, http = _client(actor)
    http.post.return_value = MagicMock(
        status_code=403,
        json=lambda: {
            "detail": {
                "code": "expired_credential",
                "private": actor.refresh_credential,
            }
        },
    )

    first, first_reason = await client.maintain_runtime_actor(force=True)
    second, second_reason = await client.maintain_runtime_actor(force=True)

    assert not first and not second
    assert first_reason == "RuntimeError"
    assert second_reason == "Officer runtime authorization retry is backed off"
    assert actor.refresh_credential not in first_reason
    assert actor.refresh_credential not in caplog.text
    assert http.post.await_count == 1


@pytest.mark.asyncio
async def test_two_lost_refresh_responses_retry_with_the_still_known_bearer():
    actor = _actor(expires_in=timedelta(seconds=-1))
    original = actor.refresh_credential
    client, http = _client(actor)
    rotated = "srr_" + "R" * 43
    delivered = MagicMock(
        status_code=200,
        json=lambda: {
            "runtime_actor": {
                **actor.audit_payload(),
                "access_credential": "sra_" + "S" * 43,
                "refresh_credential": rotated,
                "access_expires_at": (
                    datetime.now(timezone.utc) + timedelta(minutes=5)
                ).isoformat(),
                "refresh_expires_at": (
                    datetime.now(timezone.utc) + timedelta(hours=24)
                ).isoformat(),
            }
        },
    )
    http.post.side_effect = [
        httpx.ReadTimeout("committed response lost"),
        httpx.ReadTimeout("second committed response lost"),
        delivered,
    ]

    for _ in range(2):
        maintained, _reason = await client.maintain_runtime_actor(force=True)
        assert not maintained
        assert actor.refresh_credential == original
        # Advance only the deterministic local retry gate; the server-side
        # contract is covered against real PostgreSQL below.
        client._runtime_actor_retry_at = 0.0
    maintained, _reason = await client.maintain_runtime_actor(force=True)

    assert maintained
    assert actor.refresh_credential == rotated
    assert http.post.await_count == 3
    assert all(
        call.kwargs["headers"] == _pre_turn_headers(original)
        for call in http.post.await_args_list
    )


@pytest.mark.asyncio
async def test_verification_loss_response_keeps_predecessor_for_redelivery():
    actor = _actor(expires_in=timedelta(seconds=-1))
    original = actor.refresh_credential
    client, http = _client(actor)
    rotated = "srr_" + "V" * 43
    http.post.side_effect = [
        MagicMock(
            status_code=503,
            json=lambda: {
                "detail": {
                    "code": "runtime_maintenance_unavailable",
                    "retryable": True,
                }
            },
        ),
        MagicMock(
            status_code=200,
            json=lambda: {
                "runtime_actor": {
                    **actor.audit_payload(),
                    "access_credential": "sra_" + "W" * 43,
                    "refresh_credential": rotated,
                    "access_expires_at": (
                        datetime.now(timezone.utc) + timedelta(minutes=5)
                    ).isoformat(),
                    "refresh_expires_at": (
                        datetime.now(timezone.utc) + timedelta(hours=24)
                    ).isoformat(),
                }
            },
        ),
    ]

    maintained, reason = await client.maintain_runtime_actor(force=True)
    assert not maintained and reason == "RuntimeError"
    assert actor.refresh_credential == original
    client._runtime_actor_retry_at = 0.0

    maintained, _ = await client.maintain_runtime_actor(force=True)
    assert maintained
    assert actor.refresh_credential == rotated
    assert all(
        call.kwargs["headers"] == _pre_turn_headers(original)
        for call in http.post.await_args_list
    )


def _config() -> MagicMock:
    config = MagicMock()
    config.llm.timeout = 30
    config.memory.enabled = False
    config.memory.observer_interval = 5
    config.context_management.max_summary_length = 10_000
    config.officer.enabled = True
    return config


def _context_manager() -> MagicMock:
    manager = MagicMock()
    manager.ensure_within_limits = AsyncMock(
        side_effect=lambda messages, *_args, **_kwargs: messages
    )
    return manager


def _callbacks(
    inputs: list[str], gate: AsyncMock, *, on_error: AsyncMock
) -> PersistentLoopCallbacks:
    queue = iter(inputs)

    async def _input():
        try:
            return next(queue)
        except StopIteration:
            raise asyncio.CancelledError from None

    return PersistentLoopCallbacks(
        get_user_input=_input,
        on_token=AsyncMock(),
        on_thinking=AsyncMock(),
        on_tool_start=AsyncMock(),
        on_tool_result=AsyncMock(),
        permission_check=AsyncMock(return_value=True),
        on_turn_start=AsyncMock(),
        on_turn_complete=AsyncMock(),
        on_error=on_error,
        check_interrupt=MagicMock(return_value=False),
        persist_message=AsyncMock(),
        on_turn_settled=AsyncMock(),
        before_turn_authorization=gate,
    )


@pytest.mark.asyncio
async def test_failed_runtime_gate_records_turn_without_provider_spend():
    provider_calls = 0

    async def _astream(*_args, **_kwargs):
        nonlocal provider_calls
        provider_calls += 1
        yield AIMessage(content="must not run")

    llm = MagicMock(reasoning=None)
    llm.astream = _astream
    gate = AsyncMock(return_value=(False, "RuntimeError"))
    on_error = AsyncMock()
    callbacks = _callbacks(["scheduled wake"], gate, on_error=on_error)

    await run_persistent_loop(
        llm_with_tools=llm,
        tools=[],
        context_manager=_context_manager(),
        config=_config(),
        system_prompt="system",
        callbacks=callbacks,
        messages=[],
    )

    assert provider_calls == 0
    assert "no model request was made" in on_error.await_args.args[0]
    callbacks.persist_message.assert_awaited_once()
    callbacks.on_turn_complete.assert_not_awaited()
    callbacks.on_turn_settled.assert_awaited_once_with(1)


@pytest.mark.asyncio
async def test_no_spend_callback_survives_model_tool_hot_swap_for_direct_and_queue():
    """The callback belongs to the loop, not either rebuilt LLM instance."""

    provider_calls = 0

    async def _astream(*_args, **_kwargs):
        nonlocal provider_calls
        provider_calls += 1
        yield AIMessage(content="must not run")

    boot_llm = MagicMock(reasoning=None)
    boot_llm.astream = _astream
    rebuilt_llm = MagicMock(reasoning=None)
    rebuilt_llm.astream = _astream
    rebuilt_tool = MagicMock()
    rebuilt_tool.name = "list_jobs"
    gate = AsyncMock(return_value=(False, "maintenance unavailable"))
    callbacks = _callbacks(
        [
            "direct input",
            {"id": "queued-id", "role": "system", "content": "queued wake"},
        ],
        gate,
        on_error=AsyncMock(),
    )
    context_manager = _context_manager()
    get_current_tools = MagicMock(return_value=(rebuilt_llm, [rebuilt_tool]))

    await run_persistent_loop(
        llm_with_tools=boot_llm,
        tools=[],
        context_manager=context_manager,
        config=_config(),
        system_prompt="system",
        callbacks=callbacks,
        messages=[],
        get_current_tools=get_current_tools,
    )

    assert get_current_tools.call_count == 2
    assert gate.await_count == 2
    assert callbacks.persist_message.await_count == 2
    assert callbacks.on_turn_settled.await_count == 2
    assert context_manager.ensure_within_limits.await_count == 0
    assert provider_calls == 0


@pytest.mark.asyncio
async def test_no_spend_callback_survives_real_live_config_rebuild(monkeypatch):
    """Exercise persistent_app's shipped config-update rebuild, not a fake swap."""

    from agent.api import persistent_app
    from shared.runtime.core.loader import load_agent_config

    provider_calls = 0

    async def _astream(*_args, **_kwargs):
        nonlocal provider_calls
        provider_calls += 1
        yield AIMessage(content="must not run")

    boot_llm = MagicMock(reasoning=None)
    boot_llm.astream = _astream
    rebuilt_llm = MagicMock(reasoning=None)
    rebuilt_llm.astream = _astream
    config = load_agent_config("config/persistent_defaults.yaml")
    config.officer.enabled = True
    session = SimpleNamespace(
        config=config,
        _llm=boot_llm,
        tools=[],
        messages=[],
        permission_mode="auto_accept",
        narration_mode="auto",
        auxiliary_llm=None,
        memory_service=None,
        memory_extraction_prompt=None,
        _bind_tools=MagicMock(),
        refresh_context_limits=MagicMock(),
    )
    client = SimpleNamespace(
        update_thread_config=AsyncMock(
            side_effect=lambda _thread_id, config, **_: config
        ),
        maintain_runtime_actor=AsyncMock(),
    )
    monkeypatch.setattr(persistent_app, "_session", session)
    monkeypatch.setattr(persistent_app._session_identity, "_thread_id", THREAD_ID)
    monkeypatch.setattr(persistent_app, "_orchestrator_client", client)
    monkeypatch.setattr(persistent_app, "_broadcast", MagicMock())
    monkeypatch.setattr(persistent_app, "_runtime_authorization_admission_open", True)
    monkeypatch.setattr(
        "shared.runtime.core.loader.create_llm", lambda *_args: rebuilt_llm
    )

    gate_calls = 0

    async def _failed_maintenance_with_live_rebuild(*, force: bool):
        nonlocal gate_calls
        assert force is True
        gate_calls += 1
        if gate_calls == 1:
            # Rebuild while this already-running loop owns the callback. The
            # next queued input must pick up the replacement model while
            # retaining the same pre-turn authorization fence.
            await persistent_app._handle_config_update(
                MagicMock(),
                {"llm": {"temperature": 0.2}},
                request_id="rebuild-proof",
            )
        return False, "verification maintenance failed"

    client.maintain_runtime_actor.side_effect = _failed_maintenance_with_live_rebuild
    callbacks = _callbacks(
        [
            "direct durable wake",
            {"id": "queued-id", "role": "system", "content": "queued wake"},
        ],
        persistent_app._loop_before_turn_authorization,
        on_error=AsyncMock(),
    )
    context_manager = _context_manager()
    current_tools = MagicMock(side_effect=lambda: (session._llm, session.tools))
    await run_persistent_loop(
        llm_with_tools=boot_llm,
        tools=[],
        context_manager=context_manager,
        config=session.config,
        system_prompt="system",
        callbacks=callbacks,
        messages=[],
        get_current_tools=current_tools,
    )

    assert gate_calls == 2
    assert client.maintain_runtime_actor.await_count == 2
    assert all(
        call.kwargs == {"force": True}
        for call in client.maintain_runtime_actor.await_args_list
    )
    assert current_tools.call_count == 2
    assert callbacks.persist_message.await_count == 2
    assert context_manager.ensure_within_limits.await_count == 0
    assert session._llm is rebuilt_llm
    assert persistent_app._runtime_authorization_admission_open is False
    assert provider_calls == 0


@pytest.mark.asyncio
async def test_persistent_officer_gate_uses_the_maintenance_channel(monkeypatch):
    from agent.api import persistent_app

    client = MagicMock()
    client.maintain_runtime_actor = AsyncMock(return_value=(True, "recovered"))
    monkeypatch.setattr(persistent_app, "_officer_cfg", lambda: object())
    monkeypatch.setattr(persistent_app, "_orchestrator_client", client)

    assert await persistent_app._loop_before_turn_authorization() == (
        True,
        "recovered",
    )
    client.maintain_runtime_actor.assert_awaited_once_with(force=True)


def test_attach_shares_one_rotatable_actor_with_the_maintenance_client(monkeypatch):
    from agent.api import persistent_app

    actor = _actor(expires_in=timedelta(hours=5))
    client, _http = _client(_actor(expires_in=timedelta(hours=5)))
    monkeypatch.setattr(persistent_app, "_orchestrator_client", client)

    attached = persistent_app._runtime_actor_context_for_attach(actor.to_payload())

    assert attached is client.runtime_actor
    assert attached is not actor
    next_refresh = datetime.now(timezone.utc) + timedelta(hours=24)
    assert client.runtime_actor.apply_refreshed_payload(
        {
            **client.runtime_actor.audit_payload(),
            "access_credential": "sra_" + "E" * 43,
            "refresh_credential": "srr_" + "F" * 43,
            "access_expires_at": (
                datetime.now(timezone.utc) + timedelta(minutes=5)
            ).isoformat(),
            "refresh_expires_at": next_refresh.isoformat(),
        }
    )
    assert attached.refresh_credential == "srr_" + "F" * 43

    persistent_app._clear_attached_runtime_actor()
    assert client.runtime_actor is None


@pytest.mark.asyncio
async def test_later_recovery_allows_exactly_the_later_turn_to_spend():
    provider_calls = 0

    async def _astream(*_args, **_kwargs):
        nonlocal provider_calls
        provider_calls += 1
        yield AIMessage(content="authorized")

    llm = MagicMock(reasoning=None)
    llm.astream = _astream
    gate = AsyncMock(side_effect=[(False, "RuntimeError"), (True, "recovered")])
    callbacks = _callbacks(["first wake", "retry wake"], gate, on_error=AsyncMock())

    await run_persistent_loop(
        llm_with_tools=llm,
        tools=[],
        context_manager=_context_manager(),
        config=_config(),
        system_prompt="system",
        callbacks=callbacks,
        messages=[],
    )

    assert provider_calls == 1
    assert gate.await_count == 2
    assert callbacks.on_turn_complete.await_count == 1


@pytest.mark.asyncio
async def test_watchdog_pages_only_the_claimed_project_incident(monkeypatch):
    from orchestrator.services import officer_watchdog
    from orchestrator.services.officer_watchdog import OfficerWatchdogDependencies
    from orchestrator.services.runtime_actor import OfficerRuntimeMaintenance

    outcome = OfficerRuntimeMaintenance(
        authorized=False,
        state="failed",
        project_id=PROJECT_ID,
        thread_id=THREAD_ID,
        officer_incarnation=3,
        failure_code="refresh_expired",
        notification_due=True,
        notification_claim_id="claim-safe-no-secret",
        incident_changed=True,
    )
    maintain = AsyncMock(return_value=outcome)
    page = AsyncMock(return_value=True)
    settle = AsyncMock(return_value=True)
    # The watchdog imports both runtime-actor helpers directly, so a rebind on
    # ``orchestrator.main`` would no longer reach the code under test.
    monkeypatch.setattr(officer_watchdog, "maintain_current_officer_runtime", maintain)
    monkeypatch.setattr(
        officer_watchdog, "settle_officer_runtime_incident_notification", settle
    )
    store = MagicMock(name="postgres_db")
    dependencies = OfficerWatchdogDependencies(
        store=store,
        persistent_provisioner=MagicMock(),
        persistent_thread_recycler=lambda: None,
        kick_officer_event_drain=MagicMock(),
        dispatch_officer_page=page,
        conclude_conference_if_any=AsyncMock(),
        officer_runtime_verification_enabled=lambda: False,
        persistent_agent_reconciliation_enabled=lambda: False,
    )

    result = await officer_watchdog.maintain_officer_runtime_authorization(
        {"id": THREAD_ID, "project_id": PROJECT_ID}, dependencies=dependencies
    )

    assert result is outcome
    page.assert_awaited_once()
    settle.assert_awaited_once_with(
        store,
        project_id=PROJECT_ID,
        thread_id=THREAD_ID,
        officer_incarnation=3,
        notification_claim_id="claim-safe-no-secret",
        delivered=True,
        failure_class=None,
    )


def test_officer_summary_projects_only_safe_incident_fields():
    from orchestrator.services.officer_post_views import (
        officer_runtime_authorization_view,
    )

    view = officer_runtime_authorization_view(
        {
            "runtime_actor_incident": {
                "status": "open",
                "failure_class": "refresh_expired",
                "first_failed_at": "2026-08-20T00:00:00+00:00",
                "last_failed_at": "2026-08-20T00:01:00+00:00",
                "next_retry_at": "2026-08-20T00:02:00+00:00",
                "refresh_token_hash": "must-never-surface",
                "notification": {"state": "delivered", "claim_id": "private"},
            }
        },
        commissioned=True,
    )

    assert view == {
        "status": "unavailable",
        "failure_class": "refresh_expired",
        "since": "2026-08-20T00:00:00+00:00",
        "last_attempted_at": "2026-08-20T00:01:00+00:00",
        "next_retry_at": "2026-08-20T00:02:00+00:00",
        "operator_notification": "delivered",
        "planning_suppressed": True,
    }
