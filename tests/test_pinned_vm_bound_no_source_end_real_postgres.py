"""Real DB guard for End of a bound VM Session before its first VM request."""

import json
from uuid import uuid4

import pytest

from tests.test_pinned_vm_initial_binding_real_postgres import (
    _bind_protected_agent,
    _initial_vm,
    _poll,
    db as _db,
)
from tests.test_vm_resource_thread_source_real_postgres import (
    _base_db,  # noqa: F401
    _schema_applied,  # noqa: F401
    pg_dsn,  # noqa: F401
    thread_schema,  # noqa: F401
)


db = _db


async def _bound_source_free(db, monkeypatch):
    thread_id, policy, override, dependencies = await _initial_vm(
        db, monkeypatch, native=True
    )
    current = await _bind_protected_agent(db, thread_id)
    agent = await db.fetchrow(
        "SELECT hostname,pod_uid FROM agents WHERE id=$1 AND thread_id=$2",
        current["agent_id"],
        thread_id,
    )
    assert agent is not None
    assert not await db.fetchval(
        "SELECT EXISTS(SELECT 1 FROM vm_creation_retries WHERE thread_id=$1)",
        thread_id,
    )
    metadata = current["metadata"]
    if isinstance(metadata, str):
        metadata = json.loads(metadata)
    assert "vm" not in metadata
    return current, agent, policy, override, dependencies


async def _begin_authorized(db, current, *, permanent):
    retirement = await db.begin_pinned_thread_retirement(
        str(current["id"]),
        permanent=permanent,
        settle_status="ended",
        expected_runtime_generation=str(current["runtime_generation"]),
        expected_agent_id=(
            str(current["agent_id"]) if current["agent_id"] is not None else None
        ),
        expected_attach_token=(
            str(current["runtime_attach_token"])
            if current["runtime_attach_token"] is not None
            else None
        ),
    )
    assert retirement["state"] == "pending", {
        "state": retirement["state"],
        "reason": retirement.get("reason"),
    }
    assert await db.authorize_pinned_thread_retirement(
        str(current["id"]),
        token=retirement["token"],
        generation=retirement["generation"],
        settle_status="ended",
    )
    return retirement


async def _source_free_receipt(db, current, agent, retirement):
    # The production writer, not test JSON, must grant this receipt. The
    # service regression supplies the exact external Pod proof before calling it.
    return await db.acknowledge_pinned_thread_local_quiescence(
        str(current["id"]),
        expected_runtime_generation=retirement["generation"],
        expected_retirement_token=retirement["token"],
        expected_agent_id=str(current["agent_id"]),
        expected_attach_token=str(current["runtime_attach_token"]),
        expected_settle_status="ended",
        expected_quiescence_protocol="agent_runtime_zero_v1",
        expected_workspace_generation=None,
        expected_workspace_runtime_incarnation=None,
        quiescence_actor="orchestrator",
        expected_agent_pod_uid=agent["pod_uid"],
        require_zero_admission=True,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ["direct_permanent", "soft_then_permanent"])
async def test_bound_source_free_current_actor_zero_settles_and_preserves_history(
    db, monkeypatch, path
):
    current, agent, _policy, _override, _dependencies = await _bound_source_free(
        db, monkeypatch
    )
    first = await _begin_authorized(db, current, permanent=path == "direct_permanent")
    initial_token = str(first["token"])
    assert first["context"]["entry_status"] == "created"
    assert first["context"]["workspace_backend"] == "vm"
    assert first["context"]["agent_id"] == str(current["agent_id"])
    assert first["context"]["vm"] is None
    assert first["context"]["vm_creation_source"] is None

    # Behavioral RED on the current code: this exact bound, source-free VM
    # actor is incorrectly forced through workspace_actuator_zero_v1.
    receipt = await _source_free_receipt(db, current, agent, first)
    assert receipt is not None
    assert receipt["quiescence_protocol"] == "agent_runtime_zero_v1"
    assert receipt["agent_pod_name"] == agent["hostname"]
    assert receipt["agent_pod_uid"] == agent["pod_uid"]
    assert receipt.get("vm_creation_request_id") is None
    assert receipt.get("vm_creation_provision_generation") is None
    assert await db.fetchval(
        "SELECT public.pinned_vm_bound_no_vm_source($1,$2,$3)",
        current["id"],
        current["runtime_generation"],
        first["token"],
    )

    if path == "soft_then_permanent":
        assert await db.settle_pinned_thread_retirement(
            str(current["id"]),
            token=first["token"],
            generation=first["generation"],
            final_status="ended",
        )
        ended = await db.get_thread(str(current["id"]))
        assert ended["status"] == "ended"
        # The normal End funnel completes the warm protection release after
        # soft settlement. This test drives DB seams directly and models the
        # exact Pod's externally proven absence with the production CAS.
        protection_id = first["context"]["agent_pod"]["warm_binding_protection"]
        assert await db.complete_pinned_warm_binding_release(
            protection_id,
            release_outcome="exact_absent_v1",
            agent_present=False,
        )
        assert (
            await db.fetchval(
                "SELECT status FROM thread_agent_warm_binding_protections "
                "WHERE protection_id=$1::uuid",
                protection_id,
            )
            == "released"
        )
        first = await _begin_authorized(db, ended, permanent=True)
        assert first["context"]["entry_status"] == "ended"

    assert await db.clear_pinned_retirement_physical_runtime_endpoint(
        str(current["id"]),
        runtime_generation=first["generation"],
        retirement_token=first["token"],
        completed_quiescence_protocol="agent_runtime_zero_v1",
        expected_stopped_agent_pod_name=agent["hostname"],
        expected_stopped_agent_pod_uid=agent["pod_uid"],
    )
    assert await db.pinned_retirement_external_cleanup_complete(
        str(current["id"]),
        runtime_generation=first["generation"],
        retirement_token=first["token"],
    )
    await db.delete_thread(
        str(current["id"]),
        expected_runtime_retirement_token=first["token"],
        expected_runtime_generation=first["generation"],
    )
    assert await db.get_thread(str(current["id"])) is None
    history = await db.fetch(
        "SELECT retirement_token::text AS retirement_token, outcome::text AS outcome "
        "FROM thread_runtime_retirement_outcomes WHERE thread_id=$1",
        current["id"],
    )
    expected = {(str(first["token"]), "deleted")}
    if path == "soft_then_permanent":
        expected.add((initial_token, "settled"))
    assert {(row["retirement_token"], row["outcome"]) for row in history} == expected


@pytest.mark.asyncio
async def test_bound_no_source_predicate_refuses_real_creation_source_waiter_and_owner(
    db, monkeypatch
):
    current, _agent, policy, _override, dependencies = await _bound_source_free(
        db, monkeypatch
    )
    assert (await _poll(db, dependencies.vm_provisioner, current))[
        "vm_status"
    ] == "provisioning"
    source = await db.fetchrow(
        "SELECT * FROM vm_creation_retries WHERE thread_id=$1", current["id"]
    )
    assert source is not None
    assert (await policy.admit(request_id=str(source["request_id"])))[
        "action"
    ] == "admitted"
    assert await db.fetchval(
        "SELECT EXISTS(SELECT 1 FROM vm_resource_waiters "
        "WHERE owner_kind='thread' AND thread_id=$1)",
        current["id"],
    )
    assert await db.fetchval(
        "SELECT EXISTS(SELECT 1 FROM vm_thread_creation_owners WHERE thread_id=$1)",
        current["id"],
    )
    retirement = await _begin_authorized(db, current, permanent=True)
    assert not await db.fetchval(
        "SELECT public.pinned_vm_bound_no_vm_source($1,$2,$3)",
        current["id"],
        current["runtime_generation"],
        retirement["token"],
    )


@pytest.mark.asyncio
async def test_bound_no_source_predicate_refuses_execution_workspace_binding(
    db, monkeypatch
):
    current, _agent, _policy, _override, _dependencies = await _bound_source_free(
        db, monkeypatch
    )
    execution = await db.fetchrow(
        "SELECT id FROM srw_execution_specs WHERE work_kind='Session' AND work_id=$1",
        current["id"],
    )
    owner = await db.fetchval("SELECT user_id FROM threads WHERE id=$1", current["id"])
    instance = uuid4()
    await db.execute(
        "INSERT INTO srw_workspace_instances"
        "(id,owner_id,recipe,revision,pvc_name,pvc_uid,generation,execution_id,backend_state) "
        "VALUES($1,$2,$3::jsonb,'fixture','fixture-pvc',$4,2,$5,'{}')",
        instance,
        owner,
        json.dumps({"backend": "vm", "retention": "Retain"}),
        str(uuid4()),
        execution["id"],
    )
    await db.execute(
        "INSERT INTO srw_execution_workspace_bindings(execution_id,instance_id) "
        "VALUES($1,$2)",
        execution["id"],
        instance,
    )
    retirement = await _begin_authorized(db, current, permanent=True)
    assert not await db.fetchval(
        "SELECT public.pinned_vm_bound_no_vm_source($1,$2,$3)",
        current["id"],
        current["runtime_generation"],
        retirement["token"],
    )


@pytest.mark.asyncio
async def test_bound_no_source_refuses_durable_control_admission(db, monkeypatch):
    current, agent, _policy, _override, _dependencies = await _bound_source_free(
        db, monkeypatch
    )
    # A terminal control request still proves this exact generation was used.
    # It is a blocking negative row, never a hand-crafted granting receipt.
    await db.execute(
        "INSERT INTO thread_control_requests "
        "(thread_id,request_seq,client_request_id,verb,requested_by,"
        "runtime_generation,accepted_agent_id,outcome,result,applied_at,"
        "applied_agent_id,journal_epoch,journal_seq,acknowledged_at) "
        "VALUES ($1,1,$2,'stop','owner',$3,$4,'rejected','{}'::jsonb,"
        "now(),$4,1,1,now())",
        current["id"],
        uuid4(),
        current["runtime_generation"],
        current["agent_id"],
    )
    retirement = await _begin_authorized(db, current, permanent=True)
    assert not await db.fetchval(
        "SELECT public.pinned_vm_bound_no_vm_source($1,$2,$3)",
        current["id"],
        current["runtime_generation"],
        retirement["token"],
    )
    assert await _source_free_receipt(db, current, agent, retirement) is None
