"""Wire-level tests for the extracted agent thread / subagent-child routes.

R1.B06 lane C, census group ``S_CHILD``. Thirteen routes, driven through a
router mounted on a bare ``FastAPI()``.

Three things are asserted that only a wire test can see: the **declaration
order** that keeps ``…/subagents/live`` and ``…/subagents/by-call`` from being
parsed as thread ids, the refusal **status codes and bodies** each database
exception maps to, and the fact that ``agent_create_thread`` refuses a hostile
``config_name`` *before* any insert.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from orchestrator.routers import agent_child_threads as router_mod
from orchestrator.security.access import require_internal as real_require_internal
from orchestrator.services.agent_child_threads import AgentChildThreadDependencies
from shared.persistent_input_delivery import InputDeliveryConflict
from shared.session_subagent_authority import SessionParentAuthorityRefused
from shared.subagent_parent_authority import ParentExecutionAuthorityRefused

JOB_ID = "11111111-1111-4111-8111-111111111111"
THREAD_ID = "22222222-2222-4222-8222-222222222222"
CHILD_ID = "33333333-3333-4333-8333-333333333333"
AGENT_ID = "44444444-4444-4444-8444-444444444444"
GENERATION = "55555555-5555-4555-8555-555555555555"
ATTACH_TOKEN = "66666666-6666-4666-8666-666666666666"
DELIVERY_ID = "77777777-7777-4777-8777-777777777777"


def _job_authority():
    return {
        "version": 1,
        "execution_lane": "stateless",
        "parent_job_id": JOB_ID,
        "worker_lease_token": 3,
    }


def _session_authority():
    return {
        "version": 1,
        "execution_lane": "pinned",
        "parent_thread_id": THREAD_ID,
        "agent_id": AGENT_ID,
        "pod_uid": "pod-uid-1",
        "session_runtime_generation": GENERATION,
        "runtime_attach_token": ATTACH_TOKEN,
    }


def _child_row(**over):
    row = {
        "id": CHILD_ID,
        "parent_job_id": JOB_ID,
        "subagent_handle": "scholar-ab12",
        "subagent_type": "scholar",
        "subagent_status": "running",
        "runtime_generation": GENERATION,
        "status": "active",
        "metadata": {"subagent": {"brief_description": "read the docs"}},
    }
    row.update(over)
    return row


def _store(**over):
    db = MagicMock(name="postgres_db")
    db.create_subagent_thread = AsyncMock(return_value={"thread_id": CHILD_ID})
    db.list_live_subagent_threads = AsyncMock(return_value=[_child_row()])
    db.get_subagent_thread = AsyncMock(return_value=_child_row())
    db.reopen_subagent_thread = AsyncMock(return_value={"result": "reopened"})
    db.terminalize_subagent_thread_and_enqueue = AsyncMock(
        return_value={"result": "applied"}
    )
    db.create_session_subagent_thread = AsyncMock(return_value={"thread_id": CHILD_ID})
    db.list_live_session_subagent_threads = AsyncMock(return_value=[_child_row()])
    db.get_session_subagent_thread = AsyncMock(return_value=_child_row())
    db.get_session_subagent_thread_by_call = AsyncMock(return_value=_child_row())
    db.reopen_session_subagent_thread = AsyncMock(return_value={"result": "reopened"})
    db.terminalize_session_subagent_thread = AsyncMock(
        return_value={"result": "applied"}
    )
    db.save_thread_message = AsyncMock(return_value="msg-1")
    db.create_thread = AsyncMock(return_value=THREAD_ID)
    db.get_application_expert_default = AsyncMock(return_value=None)
    db.bind_thread_managed_repository = AsyncMock(return_value=True)
    for key, value in over.items():
        setattr(db, key, value)
    return db


def _deps(store=None, **over) -> AgentChildThreadDependencies:
    gitea = MagicMock()
    gitea.is_initialized = False
    gitea.is_configured = False
    provisioner = MagicMock()
    provisioner.is_available = False
    provisioner.in_cluster = False
    base = dict(
        store=store if store is not None else _store(),
        gitea_client=gitea,
        container_provisioner=provisioner,
        logger=MagicMock(),
        require_internal=AsyncMock(return_value=None),
        is_experts_db_enabled=lambda: False,
        resolve_config=MagicMock(),
        prefetch_roster_refs=AsyncMock(return_value={}),
        resolve_session_account_defaults=AsyncMock(return_value={}),
        backend_from_override=MagicMock(return_value=None),
    )
    base.update(over)
    return AgentChildThreadDependencies(**base)


def _client(dependencies) -> TestClient:
    app = FastAPI()
    app.state.agent_child_threads_dependencies_factory = lambda: dependencies
    app.include_router(router_mod.router)
    return TestClient(app, raise_server_exceptions=False)


# --- transport guard ---------------------------------------------------------


@pytest.mark.parametrize(
    "path,payload",
    [
        ("/api/agents/threads", {}),
        (
            f"/api/agents/jobs/{JOB_ID}/subagents/live",
            {"parent_authority": _job_authority()},
        ),
        (
            f"/api/agents/threads/{THREAD_ID}/subagents/live",
            {"parent_authority": _session_authority()},
        ),
        (f"/api/agents/threads/{THREAD_ID}/messages", {"role": "ai"}),
    ],
)
def test_every_child_route_fails_closed_without_an_internal_key(path, payload):
    """The gate moved from the handler body to the router; it still runs first."""
    store = _store()
    deps = _deps(store=store, require_internal=real_require_internal)
    resp = _client(deps).post(path, json=payload)
    assert resp.status_code == 401
    assert resp.json() == {"detail": "Invalid internal key"}
    store.create_thread.assert_not_awaited()
    store.save_thread_message.assert_not_awaited()
    store.list_live_subagent_threads.assert_not_awaited()
    store.list_live_session_subagent_threads.assert_not_awaited()


def test_the_internal_guard_runs_after_body_validation():
    """Pre-existing shape, preserved: a malformed body answers 422, not 401.

    The guard is the first statement *inside* the handler, so FastAPI's own
    request parsing runs first. Noted here so a future change to the ordering
    is a deliberate one rather than an accident.
    """
    deps = _deps(require_internal=real_require_internal)
    resp = _client(deps).post(f"/api/agents/jobs/{JOB_ID}/subagents/live", json={})
    assert resp.status_code == 422


def test_stateless_legacy_message_writer_refusal_is_a_409():
    store = _store(
        save_thread_message=AsyncMock(
            side_effect=RuntimeError(
                "legacy message writer is unavailable for stateless threads"
            )
        )
    )
    response = _client(_deps(store=store)).post(
        f"/api/agents/threads/{THREAD_ID}/messages",
        json={"role": "human", "content": "stale writer"},
    )

    assert response.status_code == 409
    assert response.json() == {
        "detail": {
            "code": "stateless_legacy_writer_refused",
            "message": "legacy message writer is unavailable for stateless threads",
        }
    }


# --- declaration order -------------------------------------------------------


def test_live_and_by_call_are_not_parsed_as_child_thread_ids():
    """Declaration order is the only thing keeping these from being ids."""
    store = _store()
    deps = _deps(store=store)
    client = _client(deps)

    assert (
        client.post(
            f"/api/agents/jobs/{JOB_ID}/subagents/live",
            json={"parent_authority": _job_authority()},
        ).status_code
        == 200
    )
    store.list_live_subagent_threads.assert_awaited_once()
    store.get_subagent_thread.assert_not_awaited()

    assert (
        client.post(
            f"/api/agents/threads/{THREAD_ID}/subagents/live",
            json={"parent_authority": _session_authority()},
        ).status_code
        == 200
    )
    store.list_live_session_subagent_threads.assert_awaited_once()

    assert (
        client.post(
            f"/api/agents/threads/{THREAD_ID}/subagents/by-call",
            json={
                "parent_authority": _session_authority(),
                "parent_tool_call_id": "call-1",
            },
        ).status_code
        == 200
    )
    store.get_session_subagent_thread_by_call.assert_awaited_once()
    store.get_session_subagent_thread.assert_not_awaited()


def test_a_non_uuid_child_id_is_a_422_not_a_lookup():
    deps = _deps()
    resp = _client(deps).post(
        f"/api/agents/jobs/{JOB_ID}/subagents/not-a-uuid",
        json={"parent_authority": _job_authority()},
    )
    assert resp.status_code == 422
    deps.store.get_subagent_thread.assert_not_awaited()


# --- worker children ---------------------------------------------------------


def test_worker_child_create_derives_the_row_from_the_job_in_the_path():
    store = _store()
    deps = _deps(store=store)
    resp = _client(deps).post(
        f"/api/agents/jobs/{JOB_ID}/subagents",
        json={
            "parent_authority": _job_authority(),
            "handle": "scholar-ab12",
            "subagent_type": "scholar",
            "subagent_id": CHILD_ID,
        },
    )
    assert resp.status_code == 200
    assert resp.json() == {"thread_id": CHILD_ID, "status": "created"}
    kwargs = store.create_subagent_thread.await_args.kwargs
    assert kwargs["parent_job_id"] == JOB_ID
    assert kwargs["thread_id"] == CHILD_ID


def test_worker_child_create_forbids_a_non_null_parent_thread_id():
    """The compatibility field is NULL-only; a job child belongs to the job."""
    deps = _deps()
    resp = _client(deps).post(
        f"/api/agents/jobs/{JOB_ID}/subagents",
        json={
            "parent_authority": _job_authority(),
            "handle": "scholar-ab12",
            "subagent_type": "scholar",
            "parent_thread_id": THREAD_ID,
        },
    )
    assert resp.status_code == 422
    deps.store.create_subagent_thread.assert_not_awaited()


def test_stale_parent_authority_is_a_409_carrying_the_refusal_detail():
    store = _store(
        create_subagent_thread=AsyncMock(
            side_effect=ParentExecutionAuthorityRefused("lease_superseded")
        )
    )
    deps = _deps(store=store)
    resp = _client(deps).post(
        f"/api/agents/jobs/{JOB_ID}/subagents",
        json={
            "parent_authority": _job_authority(),
            "handle": "h-0001",
            "subagent_type": "scholar",
        },
    )
    assert resp.status_code == 409
    assert resp.json() == {
        "detail": {
            "code": "parent_execution_authority_refused",
            "reason": "lease_superseded",
        }
    }


def test_a_value_error_from_the_store_is_a_400():
    store = _store(
        create_subagent_thread=AsyncMock(side_effect=ValueError("handle collides"))
    )
    deps = _deps(store=store)
    resp = _client(deps).post(
        f"/api/agents/jobs/{JOB_ID}/subagents",
        json={
            "parent_authority": _job_authority(),
            "handle": "h-0001",
            "subagent_type": "scholar",
        },
    )
    assert resp.status_code == 400
    assert resp.json() == {"detail": "handle collides"}


def test_a_missing_job_is_a_404():
    store = _store(create_subagent_thread=AsyncMock(return_value=None))
    deps = _deps(store=store)
    resp = _client(deps).post(
        f"/api/agents/jobs/{JOB_ID}/subagents",
        json={
            "parent_authority": _job_authority(),
            "handle": "h-0001",
            "subagent_type": "scholar",
        },
    )
    assert resp.status_code == 404
    assert resp.json() == {"detail": f"Job '{JOB_ID}' not found"}


def test_a_refused_reopen_returns_the_whole_result_as_a_409():
    store = _store(
        reopen_subagent_thread=AsyncMock(
            return_value={"result": "refused", "reason": "parent_completed"}
        )
    )
    deps = _deps(store=store)
    resp = _client(deps).post(
        f"/api/agents/jobs/{JOB_ID}/subagents/{CHILD_ID}/reopen",
        json={"runtime_generation": GENERATION, "parent_authority": _job_authority()},
    )
    assert resp.status_code == 409
    assert resp.json() == {
        "detail": {"result": "refused", "reason": "parent_completed"}
    }


@pytest.mark.parametrize("outcome", ["applied", "idempotent"])
def test_worker_terminalize_accepts_both_success_outcomes(outcome):
    store = _store(
        terminalize_subagent_thread_and_enqueue=AsyncMock(
            return_value={"result": outcome}
        )
    )
    deps = _deps(store=store)
    resp = _client(deps).post(
        f"/api/agents/jobs/{JOB_ID}/subagents/{CHILD_ID}/terminal",
        json={
            "runtime_generation": GENERATION,
            "parent_authority": _job_authority(),
            "delivery_id": DELIVERY_ID,
            "message": "done",
            "timestamp": "2026-09-09T10:00:00Z",
            "subagent_status": "completed",
        },
    )
    assert resp.status_code == 200
    assert resp.json() == {"result": outcome}


def test_worker_terminalize_refuses_an_empty_report():
    deps = _deps()
    resp = _client(deps).post(
        f"/api/agents/jobs/{JOB_ID}/subagents/{CHILD_ID}/terminal",
        json={
            "runtime_generation": GENERATION,
            "parent_authority": _job_authority(),
            "delivery_id": DELIVERY_ID,
            "message": "",
            "timestamp": "2026-09-09T10:00:00Z",
            "subagent_status": "completed",
        },
    )
    assert resp.status_code == 422
    deps.store.terminalize_subagent_thread_and_enqueue.assert_not_awaited()


def test_live_list_projects_each_row_through_the_shared_projection():
    store = _store()
    deps = _deps(store=store)
    resp = _client(deps).post(
        f"/api/agents/jobs/{JOB_ID}/subagents/live",
        json={"parent_authority": _job_authority()},
    )
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["job_id"] == JOB_ID
    assert payload["count"] == 1
    assert payload["subagents"][0]["handle"] == "scholar-ab12"


# --- session children --------------------------------------------------------


def test_session_child_create_forbids_an_unknown_field():
    """``extra="forbid"``: a rolling agent build fails loudly, not silently."""
    deps = _deps()
    resp = _client(deps).post(
        f"/api/agents/threads/{THREAD_ID}/subagents",
        json={
            "parent_authority": _session_authority(),
            "handle": "h-0001",
            "subagent_type": "scholar",
            "invented_field": True,
        },
    )
    assert resp.status_code == 422
    deps.store.create_session_subagent_thread.assert_not_awaited()


def test_session_authority_reaches_the_store_as_json_not_a_model():
    store = _store()
    deps = _deps(store=store)
    resp = _client(deps).post(
        f"/api/agents/threads/{THREAD_ID}/subagents",
        json={
            "parent_authority": _session_authority(),
            "handle": "h-0001",
            "subagent_type": "scholar",
        },
    )
    assert resp.status_code == 200
    wired = store.create_session_subagent_thread.await_args.kwargs["parent_authority"]
    assert isinstance(wired, dict)
    assert wired["execution_lane"] == "pinned"
    assert wired["parent_thread_id"] == THREAD_ID


def test_a_pinned_session_authority_missing_its_attach_token_is_refused():
    """Half a pinned identity is not a pinned identity."""
    authority = _session_authority()
    authority.pop("runtime_attach_token")
    deps = _deps()
    resp = _client(deps).post(
        f"/api/agents/threads/{THREAD_ID}/subagents",
        json={
            "parent_authority": authority,
            "handle": "h-0001",
            "subagent_type": "scholar",
        },
    )
    assert resp.status_code == 422
    deps.store.create_session_subagent_thread.assert_not_awaited()


def test_stale_session_authority_is_a_409_with_its_own_code():
    store = _store(
        get_session_subagent_thread=AsyncMock(
            side_effect=SessionParentAuthorityRefused("generation_rotated")
        )
    )
    deps = _deps(store=store)
    resp = _client(deps).post(
        f"/api/agents/threads/{THREAD_ID}/subagents/{CHILD_ID}",
        json={"parent_authority": _session_authority()},
    )
    assert resp.status_code == 409
    assert resp.json() == {
        "detail": {
            "code": "session_parent_authority_refused",
            "reason": "generation_rotated",
        }
    }


def test_a_missing_session_child_is_a_404():
    store = _store(get_session_subagent_thread=AsyncMock(return_value=None))
    deps = _deps(store=store)
    resp = _client(deps).post(
        f"/api/agents/threads/{THREAD_ID}/subagents/{CHILD_ID}",
        json={"parent_authority": _session_authority()},
    )
    assert resp.status_code == 404
    assert resp.json() == {"detail": "Subagent thread not found"}


def test_a_delivery_conflict_keeps_its_distinct_code():
    store = _store(
        terminalize_session_subagent_thread=AsyncMock(
            side_effect=InputDeliveryConflict("a different delivery already landed")
        )
    )
    deps = _deps(store=store)
    resp = _client(deps).post(
        f"/api/agents/threads/{THREAD_ID}/subagents/{CHILD_ID}/terminal",
        json={
            "parent_authority": _session_authority(),
            "runtime_generation": GENERATION,
            "subagent_status": "completed",
            "delivery_id": DELIVERY_ID,
        },
    )
    assert resp.status_code == 409
    detail = resp.json()["detail"]
    assert detail["code"] == "subagent_delivery_conflict"
    assert "already landed" in detail["message"]


@pytest.mark.parametrize("outcome", ["applied", "idempotent", "already_delivered"])
def test_session_terminalize_accepts_all_three_success_outcomes(outcome):
    store = _store(
        terminalize_session_subagent_thread=AsyncMock(return_value={"result": outcome})
    )
    deps = _deps(store=store)
    resp = _client(deps).post(
        f"/api/agents/threads/{THREAD_ID}/subagents/{CHILD_ID}/terminal",
        json={
            "parent_authority": _session_authority(),
            "runtime_generation": GENERATION,
            "subagent_status": "completed",
        },
    )
    assert resp.status_code == 200
    assert resp.json() == {"result": outcome}


def test_session_terminalize_forwards_the_foreground_recovery_flag():
    store = _store()
    deps = _deps(store=store)
    _client(deps).post(
        f"/api/agents/threads/{THREAD_ID}/subagents/{CHILD_ID}/terminal",
        json={
            "parent_authority": _session_authority(),
            "runtime_generation": GENERATION,
            "subagent_status": "failed",
            "foreground_orphan_recovery": True,
        },
    )
    kwargs = store.terminalize_session_subagent_thread.await_args.kwargs
    assert kwargs["foreground_orphan_recovery"] is True
    assert kwargs["delivery_id"] is None


def test_a_missing_parent_session_is_a_404():
    store = _store(create_session_subagent_thread=AsyncMock(return_value=None))
    deps = _deps(store=store)
    resp = _client(deps).post(
        f"/api/agents/threads/{THREAD_ID}/subagents",
        json={
            "parent_authority": _session_authority(),
            "handle": "h-0001",
            "subagent_type": "scholar",
        },
    )
    assert resp.status_code == 404
    assert resp.json() == {"detail": "Parent session not found"}


# --- session creation and message append -------------------------------------


def test_a_hostile_config_name_is_refused_before_any_insert():
    store = _store()
    deps = _deps(store=store)
    resp = _client(deps).post(
        "/api/agents/threads", json={"config_name": "a; rm -rf /"}
    )
    assert resp.status_code == 422
    store.create_thread.assert_not_awaited()


def test_a_missing_application_session_expert_is_a_503():
    store = _store(get_application_expert_default=AsyncMock(return_value=None))
    deps = _deps(store=store, is_experts_db_enabled=lambda: True)
    resp = _client(deps).post("/api/agents/threads", json={})
    assert resp.status_code == 503
    assert resp.json() == {
        "detail": "No application session expert default is configured"
    }
    store.create_thread.assert_not_awaited()


def test_agent_thread_creation_is_owner_less_and_datasource_empty():
    store = _store()
    deps = _deps(
        store=store,
        resolve_config=MagicMock(
            side_effect=lambda **kw: kw["capture"].update(
                {"merged_fragment": {"interactive": {"narration_mode": "verbose"}}}
            )
        ),
    )
    resp = _client(deps).post("/api/agents/threads", json={"title": "Local Session"})
    assert resp.status_code == 200
    assert resp.json() == {"thread_id": THREAD_ID, "status": "created"}
    kwargs = store.create_thread.await_args.kwargs
    assert kwargs["user_id"] is None
    assert kwargs["datasource_ids"] == []
    assert kwargs["narration_mode"] == "verbose"
    provenance = kwargs["datasource_selection_provenance"]
    assert provenance["origin"] == "system_empty"
    assert provenance["creation_path"] == "internal_agent_thread"


def test_save_message_passes_every_component_column_through():
    store = _store()
    deps = _deps(store=store)
    resp = _client(deps).post(
        f"/api/agents/threads/{THREAD_ID}/messages",
        json={
            "role": "ai",
            "content": "hello",
            "thinking": "hmm",
            "tool_call_id": "call-1",
            "provider": "anthropic",
            "response_metadata": {"model": "opus"},
        },
    )
    assert resp.status_code == 200
    assert resp.json() == {"message_id": "msg-1", "status": "saved"}
    kwargs = store.save_thread_message.await_args.kwargs
    assert kwargs["thread_id"] == THREAD_ID
    assert kwargs["thinking"] == "hmm"
    assert kwargs["tool_call_id"] == "call-1"
    assert kwargs["response_metadata"] == {"model": "opus"}


def test_save_message_maps_a_store_failure_to_500():
    store = _store(save_thread_message=AsyncMock(side_effect=RuntimeError("db down")))
    deps = _deps(store=store)
    resp = _client(deps).post(
        f"/api/agents/threads/{THREAD_ID}/messages", json={"role": "ai"}
    )
    assert resp.status_code == 500
    assert resp.json() == {"detail": "db down"}
