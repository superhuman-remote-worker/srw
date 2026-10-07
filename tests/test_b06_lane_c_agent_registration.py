"""Wire-level tests for the extracted agent registration / heartbeat routes.

R1.B06 lane C, census group ``S_REG``. Every test drives the router mounted on a
bare ``FastAPI()``, so the assertions are on the HTTP contract — status code and
error body — not on a Python return value.

What is under test is the identity discipline (port contract §P6/§P7): the
persistent bind fence refuses *before* the hostname upsert, every refusal keeps
its exact ``code``, and each runtime-actor bootstrap refusal writes its
``runtime_actor_denied`` security event.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from orchestrator.routers import agent_registration as router_mod
from orchestrator.security.access import require_internal as real_require_internal
from orchestrator.services.agent_registration import AgentRegistrationDependencies
from orchestrator.services.runtime_actor import RuntimeActorCredentialError

THREAD_ID = "11111111-1111-4111-8111-111111111111"
GENERATION = "22222222-2222-4222-8222-222222222222"
AGENT_ID = "33333333-3333-4333-8333-333333333333"
OTHER_AGENT_ID = "44444444-4444-4444-8444-444444444444"
ATTACH_TOKEN = "55555555-5555-4555-8555-555555555555"
JOB_ID = "66666666-6666-4666-8666-666666666666"


def _thread(**over):
    row = {
        "id": THREAD_ID,
        "execution_lane": "pinned",
        "status": "created",
        "runtime_generation": GENERATION,
        "runtime_retirement_token": None,
        "runtime_retirement_authorized_at": None,
        "agent_id": None,
        "runtime_attach_token": ATTACH_TOKEN,
        "metadata": {},
    }
    row.update(over)
    return row


def _registration(**over):
    body = {
        "config_name": "assistant",
        "pod_ip": "10.0.0.9",
        "hostname": "srw-agent-abc",
        "pod_port": 8001,
        "agent_mode": "worker",
    }
    body.update(over)
    return body


def _register_result(agent_id=AGENT_ID):
    return {
        "agent_id": agent_id,
        "heartbeat_interval_seconds": 60,
        "dispatch_process_generation": GENERATION,
    }


def _store(**over):
    db = MagicMock(name="postgres_db")
    db.register_agent = AsyncMock(return_value=_register_result())
    db.get_thread = AsyncMock(return_value=_thread())
    db.get_agent = AsyncMock(return_value=None)
    db.fetchrow = AsyncMock(return_value=None)
    db.publish_pinned_agent_pod_provision_intent = AsyncMock(return_value=True)
    db.heartbeat = AsyncMock(return_value={"intents": {}})
    db.get_job = AsyncMock(return_value=None)
    db.merge_workspace_container_context = AsyncMock()
    db.list_agents = AsyncMock(return_value=[])
    db.delete_agent = AsyncMock(return_value=True)

    @asynccontextmanager
    async def _lock(_thread_id):
        yield None

    db.thread_advisory_lock = _lock
    for key, value in over.items():
        setattr(db, key, value)
    return db


def _bind_aware_store():
    """A store whose thread row reflects the bind, like the real one does.

    The final read in ``register_agent`` refuses unless the row now names the
    freshly registered agent and its attach token, so a fixture that always
    returns the pre-bind row would make every happy path look like a lost race.
    """
    bound = {"value": False}
    store = _store()

    async def _get_thread(_thread_id):
        return _thread(agent_id=AGENT_ID) if bound["value"] else _thread()

    async def _bind(*_args, **_kwargs):
        bound["value"] = True
        return ATTACH_TOKEN

    store.get_thread = AsyncMock(side_effect=_get_thread)
    return store, AsyncMock(side_effect=_bind)


def _deps(store=None, **over) -> AgentRegistrationDependencies:
    base = dict(
        store=store if store is not None else _store(),
        gitea_client=MagicMock(),
        logger=MagicMock(),
        require_internal=AsyncMock(return_value=None),
        require_admin=AsyncMock(return_value={"id": "admin"}),
        is_internal_call=MagicMock(return_value=False),
        log_security_event=AsyncMock(),
        completion_commands_enabled=lambda: False,
        require_pinned_status_identity=lambda: False,
        thread_uses_pinned_execution=lambda t: bool(
            t and t.get("execution_lane") == "pinned"
        ),
        thread_accepts_runtime=lambda t: bool(t),
        protected_cloud_delivery_state=AsyncMock(return_value=("ready", None)),
        bind_registered_persistent_agent=AsyncMock(return_value=ATTACH_TOKEN),
        slide_thread_grant_on_liveness=AsyncMock(),
        trigger_dispatch=MagicMock(),
    )
    base.update(over)
    return AgentRegistrationDependencies(**base)


def _client(dependencies) -> TestClient:
    app = FastAPI()
    app.state.agent_registration_dependencies_factory = lambda: dependencies
    app.include_router(router_mod.router)
    return TestClient(app, raise_server_exceptions=False)


# --- transport guard ---------------------------------------------------------


@pytest.mark.parametrize(
    "method,path,payload",
    [
        ("post", "/api/agents/register", _registration()),
        ("post", f"/api/agents/{AGENT_ID}/heartbeat", {"status": "ready"}),
        (
            "post",
            f"/api/agents/{AGENT_ID}/runtime-actor/session",
            {"thread_id": THREAD_ID},
        ),
    ],
)
def test_internal_routes_fail_closed_without_a_key(method, path, payload):
    """The gate moved from the handler body to the router; it still runs first."""
    store = _store()
    deps = _deps(store=store, require_internal=real_require_internal)
    resp = getattr(_client(deps), method)(path, json=payload)
    assert resp.status_code == 401
    assert resp.json() == {"detail": "Invalid internal key"}
    store.register_agent.assert_not_awaited()
    store.heartbeat.assert_not_awaited()
    store.get_thread.assert_not_awaited()


# --- worker registration -----------------------------------------------------


def test_worker_registration_never_takes_the_thread_lane_path():
    store = _store()
    deps = _deps(store=store)
    resp = _client(deps).post("/api/agents/register", json=_registration())
    assert resp.status_code == 200
    assert resp.json()["agent_id"] == AGENT_ID
    assert resp.json()["session_runtime_generation"] is None
    assert resp.json()["session_runtime_attach_token"] is None
    store.get_thread.assert_not_awaited()
    deps.bind_registered_persistent_agent.assert_not_awaited()


def test_registration_may_not_self_assert_verified_provenance():
    deps = _deps()
    resp = _client(deps).post(
        "/api/agents/register",
        json=_registration(product_provenance={"provenance_status": "verified"}),
    )
    assert resp.status_code == 422
    deps.store.register_agent.assert_not_awaited()


def test_completion_flag_is_read_per_request_not_at_import():
    """Port contract §P1: the flag reaches the service as a callable."""
    flag = {"value": False}
    deps = _deps(completion_commands_enabled=lambda: flag["value"])
    client = _client(deps)
    client.post("/api/agents/register", json=_registration())
    assert (
        deps.store.register_agent.await_args.kwargs["completion_commands_enabled"]
        is False
    )
    flag["value"] = True
    client.post("/api/agents/register", json=_registration())
    assert (
        deps.store.register_agent.await_args.kwargs["completion_commands_enabled"]
        is True
    )


# --- persistent registration: the bind-boundary fence ------------------------


def _persistent(**over):
    return _registration(agent_mode="persistent", thread_id=THREAD_ID, **over)


def test_non_pinned_lane_is_refused_before_any_agent_upsert():
    store = _store(get_thread=AsyncMock(return_value=_thread(execution_lane="queue")))
    deps = _deps(store=store)
    resp = _client(deps).post("/api/agents/register", json=_persistent())
    assert resp.status_code == 409
    assert resp.json() == {
        "detail": "thread execution lane does not accept persistent agents"
    }
    store.register_agent.assert_not_awaited()


def test_missing_runtime_generation_makes_the_row_unauthoritative():
    store = _store(
        get_thread=AsyncMock(return_value=_thread(runtime_generation=None)),
    )
    deps = _deps(store=store)
    resp = _client(deps).post("/api/agents/register", json=_persistent())
    assert resp.status_code == 409
    store.register_agent.assert_not_awaited()


def test_presented_generation_mismatch_is_its_own_code():
    store = _store()
    deps = _deps(store=store)
    resp = _client(deps).post(
        "/api/agents/register",
        json=_persistent(session_runtime_generation=OTHER_AGENT_ID),
    )
    assert resp.status_code == 409
    assert resp.json() == {"detail": {"code": "pinned_runtime_generation_mismatch"}}
    store.register_agent.assert_not_awaited()


def test_identity_enforcement_requires_a_presented_generation():
    store = _store()
    deps = _deps(store=store, require_pinned_status_identity=lambda: True)
    resp = _client(deps).post("/api/agents/register", json=_persistent())
    assert resp.status_code == 409
    assert resp.json() == {"detail": {"code": "pinned_runtime_generation_required"}}
    store.register_agent.assert_not_awaited()


def test_malformed_protected_cloud_marker_is_refused():
    store = _store(
        get_thread=AsyncMock(
            return_value=_thread(metadata={"protected_cloud": "maybe"})
        )
    )
    deps = _deps(store=store)
    resp = _client(deps).post("/api/agents/register", json=_persistent())
    assert resp.status_code == 409
    assert resp.json() == {"detail": {"code": "protected_cloud_malformed"}}
    store.register_agent.assert_not_awaited()


def test_protected_runtime_demands_its_exact_generation():
    store = _store(
        get_thread=AsyncMock(return_value=_thread(metadata={"protected_cloud": True}))
    )
    deps = _deps(store=store)
    resp = _client(deps).post("/api/agents/register", json=_persistent())
    assert resp.status_code == 409
    detail = resp.json()["detail"]
    assert detail["code"] == "pinned_runtime_generation_required"
    assert "exact session generation" in detail["message"]
    store.register_agent.assert_not_awaited()


def test_protected_runtime_not_ready_reports_its_state_and_reason():
    store = _store(
        get_thread=AsyncMock(return_value=_thread(metadata={"protected_cloud": True}))
    )
    deps = _deps(
        store=store,
        protected_cloud_delivery_state=AsyncMock(return_value=("pending", "no_folder")),
    )
    resp = _client(deps).post(
        "/api/agents/register",
        json=_persistent(session_runtime_generation=GENERATION),
    )
    assert resp.status_code == 409
    assert resp.json() == {
        "detail": {
            "code": "protected_cloud_not_ready",
            "state": "pending",
            "reason": "no_folder",
        }
    }
    store.register_agent.assert_not_awaited()


def test_a_different_live_owner_is_refused_before_the_upsert():
    """The double-provisioning fence: hostname is not an ownership credential."""
    store = _store(
        get_thread=AsyncMock(return_value=_thread(agent_id=OTHER_AGENT_ID)),
        get_agent=AsyncMock(
            return_value={"id": OTHER_AGENT_ID, "hostname": "other", "status": "ready"}
        ),
    )
    deps = _deps(store=store)
    resp = _client(deps).post("/api/agents/register", json=_persistent())
    assert resp.status_code == 409
    assert resp.json() == {"detail": "thread already bound to another live agent"}
    store.register_agent.assert_not_awaited()
    deps.bind_registered_persistent_agent.assert_not_awaited()


def test_a_thread_naming_a_missing_agent_is_inconsistent_not_permissive():
    store = _store(
        get_thread=AsyncMock(return_value=_thread(agent_id=OTHER_AGENT_ID)),
        get_agent=AsyncMock(return_value=None),
    )
    deps = _deps(store=store)
    resp = _client(deps).post("/api/agents/register", json=_persistent())
    assert resp.status_code == 409
    assert resp.json() == {"detail": "thread agent ownership is inconsistent"}
    store.register_agent.assert_not_awaited()


def test_same_hostname_restart_targets_the_exact_authorized_row():
    store = _store(
        get_thread=AsyncMock(return_value=_thread(agent_id=AGENT_ID)),
        get_agent=AsyncMock(
            return_value={
                "id": AGENT_ID,
                "hostname": "srw-agent-abc",
                "status": "ready",
            }
        ),
    )
    deps = _deps(store=store)
    resp = _client(deps).post("/api/agents/register", json=_persistent())
    assert resp.status_code == 200
    kwargs = store.register_agent.await_args.kwargs
    assert kwargs["expected_agent_id"] == AGENT_ID
    assert kwargs["insert_only"] is False
    # An exact restart is the only case that carries the thread on the upsert.
    assert kwargs["thread_id"] == THREAD_ID


def test_a_fresh_bind_inserts_unbound_so_the_pair_is_published_once():
    """§P8: publishing ``agents.thread_id`` first is an unfenceable authority."""
    store, bind = _bind_aware_store()
    deps = _deps(store=store, bind_registered_persistent_agent=bind)
    resp = _client(deps).post("/api/agents/register", json=_persistent())
    assert resp.status_code == 200
    kwargs = store.register_agent.await_args.kwargs
    assert kwargs["thread_id"] is None
    assert kwargs["insert_only"] is True
    assert kwargs["expected_agent_id"] is None
    deps.bind_registered_persistent_agent.assert_awaited_once_with(
        THREAD_ID, AGENT_ID, None, GENERATION
    )
    assert resp.json()["session_runtime_attach_token"] == ATTACH_TOKEN
    assert resp.json()["session_runtime_generation"] == GENERATION


def test_a_planned_provision_intent_must_name_the_presenting_pod():
    store = _store(
        fetchrow=AsyncMock(
            return_value={
                "attempt_id": OTHER_AGENT_ID,
                "pod_name": "srw-agent-somebody-else",
                "namespace": "srw",
            }
        )
    )
    deps = _deps(store=store)
    resp = _client(deps).post(
        "/api/agents/register", json=_persistent(pod_uid="pod-uid-1")
    )
    assert resp.status_code == 409
    assert resp.json() == {"detail": {"code": "agent_pod_provision_intent_mismatch"}}
    store.register_agent.assert_not_awaited()


def test_a_lost_final_bind_is_a_409_not_a_registered_agent():
    store = _store()
    deps = _deps(
        store=store, bind_registered_persistent_agent=AsyncMock(return_value=None)
    )
    resp = _client(deps).post("/api/agents/register", json=_persistent())
    assert resp.status_code == 409
    assert resp.json() == {
        "detail": "thread execution lane changed before agent binding"
    }


def test_the_final_read_refuses_a_row_that_moved_during_registration():
    store = _store()
    store.get_thread = AsyncMock(
        side_effect=[
            _thread(),  # entry
            _thread(),  # after repository authority
            _thread(),  # after bootstrap validation
            _thread(agent_id=OTHER_AGENT_ID),  # final read: someone else won
        ]
    )
    deps = _deps(store=store)
    resp = _client(deps).post("/api/agents/register", json=_persistent())
    assert resp.status_code == 409
    assert resp.json() == {
        "detail": "Pinned runtime changed before registration completed"
    }


def test_a_malformed_bootstrap_denies_and_audits(monkeypatch):
    monkeypatch.setattr(
        "orchestrator.services.agent_registration.request_bootstrap_token",
        MagicMock(
            side_effect=RuntimeActorCredentialError("duplicate_bootstrap", "duplicated")
        ),
    )
    store = _store()
    deps = _deps(store=store)
    resp = _client(deps).post("/api/agents/register", json=_persistent())
    assert resp.status_code == 403
    assert resp.json() == {
        "detail": "Runtime actor bootstrap is malformed or duplicated."
    }
    store.register_agent.assert_not_awaited()
    audit = deps.log_security_event.await_args.kwargs
    assert audit["event_type"] == "runtime_actor_denied"
    assert audit["resource_type"] == "runtime_actor_bootstrap"
    assert audit["resource_id"] == THREAD_ID


def test_an_invalid_bootstrap_denies_before_the_upsert(monkeypatch):
    monkeypatch.setattr(
        "orchestrator.services.agent_registration.request_bootstrap_token",
        MagicMock(return_value="bootstrap-token"),
    )
    monkeypatch.setattr(
        "orchestrator.services.agent_registration.validate_thread_runtime_actor_bootstrap",
        AsyncMock(side_effect=RuntimeActorCredentialError("expired", "expired")),
    )
    store = _store()
    deps = _deps(store=store)
    resp = _client(deps).post("/api/agents/register", json=_persistent())
    assert resp.status_code == 403
    assert resp.json() == {"detail": "Runtime actor bootstrap is invalid or expired."}
    store.register_agent.assert_not_awaited()


def test_a_proven_bootstrap_mints_an_actor_into_the_response(monkeypatch):
    monkeypatch.setattr(
        "orchestrator.services.agent_registration.request_bootstrap_token",
        MagicMock(return_value="bootstrap-token"),
    )
    monkeypatch.setattr(
        "orchestrator.services.agent_registration.validate_thread_runtime_actor_bootstrap",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        "orchestrator.services.agent_registration.mint_thread_runtime_actor",
        AsyncMock(return_value=SimpleNamespace(to_payload=lambda: {"actor_id": "a-1"})),
    )
    store, bind = _bind_aware_store()
    deps = _deps(store=store, bind_registered_persistent_agent=bind)
    resp = _client(deps).post("/api/agents/register", json=_persistent())
    assert resp.status_code == 200
    assert resp.json()["runtime_actor"] == {"actor_id": "a-1"}


def test_repository_authority_failure_is_a_503_and_writes_nothing(monkeypatch):
    from orchestrator.services.managed_repository_authority import (
        ManagedRepositoryAuthorityError,
    )

    monkeypatch.setattr(
        "orchestrator.services.agent_registration.prepare_thread_repository_authority",
        AsyncMock(side_effect=ManagedRepositoryAuthorityError("gitea_down")),
    )
    store = _store()
    deps = _deps(store=store)
    resp = _client(deps).post("/api/agents/register", json=_persistent())
    assert resp.status_code == 503
    assert resp.json() == {"detail": "Workspace repository authority is unavailable"}
    store.register_agent.assert_not_awaited()


# --- pod runtime actor -------------------------------------------------------


def test_pod_runtime_actor_requires_a_bootstrap_beyond_the_internal_key(monkeypatch):
    monkeypatch.setattr(
        "orchestrator.services.agent_registration.request_bootstrap_token",
        MagicMock(return_value=None),
    )
    deps = _deps()
    resp = _client(deps).post(
        f"/api/agents/{AGENT_ID}/runtime-actor/session",
        json={"thread_id": THREAD_ID},
    )
    assert resp.status_code == 403
    assert resp.json() == {"detail": "Runtime actor pod bootstrap is required."}


def test_pod_runtime_actor_denies_an_unbound_bootstrap_and_audits(monkeypatch):
    monkeypatch.setattr(
        "orchestrator.services.agent_registration.request_bootstrap_token",
        MagicMock(return_value="bootstrap-token"),
    )
    monkeypatch.setattr(
        "orchestrator.services.agent_registration.exchange_runtime_actor_pod_bootstrap",
        AsyncMock(side_effect=RuntimeActorCredentialError("not_bound", "not bound")),
    )
    deps = _deps()
    resp = _client(deps).post(
        f"/api/agents/{AGENT_ID}/runtime-actor/session",
        json={"thread_id": THREAD_ID},
    )
    assert resp.status_code == 403
    assert resp.json() == {
        "detail": "Runtime actor pod bootstrap is invalid or not bound."
    }
    audit = deps.log_security_event.await_args.kwargs
    assert audit["resource_type"] == "runtime_actor_pod_bootstrap"


def test_pod_runtime_actor_reads_the_binding_from_the_agent_row(monkeypatch):
    exchange = AsyncMock(
        return_value=SimpleNamespace(to_payload=lambda: {"actor_id": "pool-1"})
    )
    monkeypatch.setattr(
        "orchestrator.services.agent_registration.request_bootstrap_token",
        MagicMock(return_value="bootstrap-token"),
    )
    monkeypatch.setattr(
        "orchestrator.services.agent_registration.exchange_runtime_actor_pod_bootstrap",
        exchange,
    )
    deps = _deps()
    resp = _client(deps).post(
        f"/api/agents/{AGENT_ID}/runtime-actor/session",
        json={"thread_id": THREAD_ID},
    )
    assert resp.status_code == 200
    assert resp.json() == {"runtime_actor": {"actor_id": "pool-1"}}
    assert exchange.await_args.kwargs["agent_id"] == AGENT_ID


# --- heartbeat ---------------------------------------------------------------


def test_unknown_agent_heartbeat_is_404():
    store = _store(heartbeat=AsyncMock(return_value=None))
    deps = _deps(store=store)
    resp = _client(deps).post(
        f"/api/agents/{AGENT_ID}/heartbeat", json={"status": "ready"}
    )
    assert resp.status_code == 404
    assert resp.json() == {"detail": f"Agent '{AGENT_ID}' not found"}


def test_authority_refused_heartbeat_keeps_its_exact_code():
    store = _store(heartbeat=AsyncMock(return_value={"authority_refused": True}))
    deps = _deps(store=store)
    resp = _client(deps).post(
        f"/api/agents/{AGENT_ID}/heartbeat", json={"status": "ready"}
    )
    assert resp.status_code == 409
    detail = resp.json()["detail"]
    assert detail["code"] == "pinned_runtime_identity_mismatch"
    assert "does not own the current pinned session runtime" in detail["message"]


def test_heartbeat_rejects_a_status_outside_the_pattern():
    deps = _deps()
    resp = _client(deps).post(
        f"/api/agents/{AGENT_ID}/heartbeat", json={"status": "vibing"}
    )
    assert resp.status_code == 422
    deps.store.heartbeat.assert_not_awaited()


def test_heartbeat_returns_the_backstop_contract():
    store = _store(
        heartbeat=AsyncMock(return_value={"intents": {"drain": True}}),
        get_job=AsyncMock(
            return_value={
                "status": "failed",
                "context": {
                    "pending_guidance": [{"id": "g1"}],
                    "queued_replies": [{"id": "r1"}],
                },
            }
        ),
    )
    deps = _deps(store=store)
    resp = _client(deps).post(
        f"/api/agents/{AGENT_ID}/heartbeat",
        json={"status": "working", "current_job_id": JOB_ID},
    )
    assert resp.status_code == 200
    assert resp.json() == {
        "status": "ok",
        "intents": {"drain": True},
        "job_status": "failed",
        "pending_guidance": [{"id": "g1"}],
        "queued_replies": [{"id": "r1"}],
    }


def test_heartbeat_reads_a_string_encoded_context():
    store = _store(
        get_job=AsyncMock(
            return_value={
                "status": "processing",
                "context": '{"pending_guidance": [{"id": "g1"}]}',
            }
        )
    )
    deps = _deps(store=store)
    resp = _client(deps).post(
        f"/api/agents/{AGENT_ID}/heartbeat",
        json={"status": "working", "current_job_id": JOB_ID},
    )
    assert resp.json()["pending_guidance"] == [{"id": "g1"}]
    assert resp.json()["queued_replies"] == []


def test_a_job_read_failure_degrades_to_no_information_not_a_prune():
    """``None`` means "keep your inbox"; ``[]`` means "prune it"."""
    store = _store(get_job=AsyncMock(side_effect=RuntimeError("db down")))
    deps = _deps(store=store)
    resp = _client(deps).post(
        f"/api/agents/{AGENT_ID}/heartbeat",
        json={"status": "working", "current_job_id": JOB_ID},
    )
    assert resp.status_code == 200
    assert resp.json()["job_status"] is None
    assert resp.json()["pending_guidance"] is None
    assert resp.json()["queued_replies"] is None


def test_heartbeat_triggers_dispatch_only_on_a_transition_into_ready():
    store = _store(
        heartbeat=AsyncMock(
            return_value={
                "intents": {},
                "previous_status": "working",
                "effective_status": "ready",
            }
        )
    )
    deps = _deps(store=store)
    assert (
        _client(deps)
        .post(f"/api/agents/{AGENT_ID}/heartbeat", json={"status": "ready"})
        .status_code
        == 200
    )
    deps.trigger_dispatch.assert_called_once()


def test_a_draining_agent_reporting_ready_does_not_dispatch():
    store = _store(
        heartbeat=AsyncMock(
            return_value={
                "intents": {},
                "previous_status": "working",
                "effective_status": "draining",
            }
        )
    )
    deps = _deps(store=store)
    _client(deps).post(f"/api/agents/{AGENT_ID}/heartbeat", json={"status": "ready"})
    deps.trigger_dispatch.assert_not_called()


def test_a_failed_grant_slide_never_fails_the_heartbeat():
    store = _store(
        heartbeat=AsyncMock(return_value={"intents": {}, "thread_id": THREAD_ID})
    )
    deps = _deps(
        store=store,
        slide_thread_grant_on_liveness=AsyncMock(side_effect=RuntimeError("grant db")),
    )
    resp = _client(deps).post(
        f"/api/agents/{AGENT_ID}/heartbeat", json={"status": "ready"}
    )
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"
    deps.logger.warning.assert_called()


def test_a_failed_activity_merge_never_fails_the_heartbeat():
    store = _store(
        merge_workspace_container_context=AsyncMock(side_effect=RuntimeError("boom"))
    )
    deps = _deps(store=store)
    resp = _client(deps).post(
        f"/api/agents/{AGENT_ID}/heartbeat",
        json={"status": "working", "current_job_id": JOB_ID},
    )
    assert resp.status_code == 200


@pytest.mark.parametrize("lanes", [frozenset({"stateless", "pinned"}), frozenset()])
def test_a_pinned_session_heartbeat_carries_the_fanout_switch(lanes):
    """parallel_subagents.md §14.2 P5: the heartbeat is the only response a
    running pinned session receives, so it carries the same two keys as the
    pinned attach body, the switch read now for the pinned lane."""
    store = _store(
        heartbeat=AsyncMock(
            return_value={
                "intents": {},
                "thread_id": THREAD_ID,
                "execution_lane": "pinned",
            }
        )
    )
    seen: list[str] = []

    def _switch(lane):
        seen.append(lane)
        return lane in lanes

    deps = _deps(store=store, session_subagent_fanout=_switch)
    resp = _client(deps).post(
        f"/api/agents/{AGENT_ID}/heartbeat", json={"status": "session"}
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["session_subagent_batch_settle_contract"] == 1
    assert body["session_subagent_fanout"] is ("pinned" in lanes)
    assert seen == ["pinned"]


@pytest.mark.parametrize(
    "beat",
    [
        # A stateless thread learns its switch at every claim, never here.
        {"intents": {}, "thread_id": THREAD_ID, "execution_lane": "stateless"},
        # An unknown or missing lane never inherits the pinned switch.
        {"intents": {}, "thread_id": THREAD_ID, "execution_lane": None},
        # A pool or worker agent with no bound thread.
        {"intents": {}, "thread_id": None, "execution_lane": None},
        # An older store result without the lane.
        {"intents": {}, "thread_id": THREAD_ID},
    ],
)
def test_a_heartbeat_without_a_pinned_thread_carries_no_advertisement(beat):
    store = _store(heartbeat=AsyncMock(return_value=beat))
    switch = MagicMock(return_value=True)
    deps = _deps(store=store, session_subagent_fanout=switch)
    resp = _client(deps).post(
        f"/api/agents/{AGENT_ID}/heartbeat", json={"status": "ready"}
    )
    assert resp.status_code == 200
    body = resp.json()
    assert "session_subagent_fanout" not in body
    assert "session_subagent_batch_settle_contract" not in body
    switch.assert_not_called()


def test_a_failed_switch_read_never_fails_the_heartbeat():
    """Absent keys mean "no change" to the agent, so a failed read omits
    them instead of failing the beat or reporting the lane off."""
    store = _store(
        heartbeat=AsyncMock(
            return_value={
                "intents": {},
                "thread_id": THREAD_ID,
                "execution_lane": "pinned",
            }
        )
    )
    deps = _deps(
        store=store, session_subagent_fanout=MagicMock(side_effect=RuntimeError("x"))
    )
    resp = _client(deps).post(
        f"/api/agents/{AGENT_ID}/heartbeat", json={"status": "session"}
    )
    assert resp.status_code == 200
    assert "session_subagent_fanout" not in resp.json()
    deps.logger.warning.assert_called()


def test_graph_progress_folds_into_metrics():
    store = _store()
    deps = _deps(store=store)
    _client(deps).post(
        f"/api/agents/{AGENT_ID}/heartbeat",
        json={
            "status": "ready",
            "graph_progress": 7,
            "metrics": {"aux": {"degraded": True}},
        },
    )
    kwargs = store.heartbeat.await_args.kwargs
    assert kwargs["metrics"]["graph_progress"] == 7
    assert kwargs["aux_degraded"] is True


# --- admin reads and deregistration ------------------------------------------


def test_list_agents_is_admin_only():
    deps = _deps(
        require_admin=AsyncMock(
            side_effect=HTTPException(status_code=403, detail="Admin required")
        )
    )
    resp = _client(deps).get("/api/agents")
    assert resp.status_code == 403
    deps.store.list_agents.assert_not_awaited()


def test_list_agents_enforces_its_limit_bounds():
    deps = _deps()
    assert _client(deps).get("/api/agents?limit=501").status_code == 422
    assert _client(deps).get("/api/agents?limit=0").status_code == 422
    deps.store.list_agents.assert_not_awaited()


def test_list_agents_passes_the_status_filter_through():
    store = _store(list_agents=AsyncMock(return_value=[{"id": AGENT_ID}]))
    deps = _deps(store=store)
    resp = _client(deps).get("/api/agents?status=ready&limit=5")
    assert resp.status_code == 200
    assert resp.json() == [{"id": AGENT_ID}]
    store.list_agents.assert_awaited_once_with(status="ready", limit=5)


def test_get_agent_missing_is_404():
    deps = _deps()
    resp = _client(deps).get(f"/api/agents/{AGENT_ID}")
    assert resp.status_code == 404
    assert resp.json() == {"detail": f"Agent '{AGENT_ID}' not found"}


def test_system_info_refuses_an_offline_agent():
    store = _store(
        get_agent=AsyncMock(return_value={"status": "offline", "pod_ip": "10.0.0.9"})
    )
    deps = _deps(store=store)
    resp = _client(deps).get(f"/api/agents/{AGENT_ID}/system-info")
    assert resp.status_code == 400
    assert resp.json() == {"detail": "Agent is offline"}


def test_system_info_refuses_an_agent_with_no_pod_ip():
    store = _store(
        get_agent=AsyncMock(return_value={"status": "ready", "pod_ip": None})
    )
    deps = _deps(store=store)
    resp = _client(deps).get(f"/api/agents/{AGENT_ID}/system-info")
    assert resp.status_code == 400
    assert resp.json() == {"detail": "Agent has no pod IP configured"}


def test_system_info_proxies_and_maps_a_transport_error_to_502(monkeypatch):
    import httpx

    class _Boom:
        def __init__(self, *a, **k):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def get(self, url):
            raise httpx.RequestError("connection refused")

    monkeypatch.setattr(httpx, "AsyncClient", _Boom)
    store = _store(
        get_agent=AsyncMock(
            return_value={"status": "ready", "pod_ip": "10.0.0.9", "pod_port": 8001}
        )
    )
    deps = _deps(store=store)
    resp = _client(deps).get(f"/api/agents/{AGENT_ID}/system-info")
    assert resp.status_code == 502
    assert "Failed to connect to agent" in resp.json()["detail"]


def test_delete_agent_accepts_the_internal_key_without_admin():
    store = _store()
    deps = _deps(
        store=store,
        is_internal_call=MagicMock(return_value=True),
        require_admin=AsyncMock(
            side_effect=AssertionError("admin gate must not run for internal callers")
        ),
    )
    resp = _client(deps).delete(f"/api/agents/{AGENT_ID}")
    assert resp.status_code == 200
    assert resp.json() == {"status": "deleted"}


def test_delete_agent_requires_admin_for_everyone_else():
    deps = _deps(
        require_admin=AsyncMock(
            side_effect=HTTPException(status_code=403, detail="Admin required")
        )
    )
    resp = _client(deps).delete(f"/api/agents/{AGENT_ID}")
    assert resp.status_code == 403
    deps.store.delete_agent.assert_not_awaited()


def test_delete_agent_missing_is_404():
    store = _store(delete_agent=AsyncMock(return_value=False))
    deps = _deps(store=store)
    resp = _client(deps).delete(f"/api/agents/{AGENT_ID}")
    assert resp.status_code == 404
    assert resp.json() == {"detail": f"Agent '{AGENT_ID}' not found"}
