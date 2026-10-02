"""The internal startup hint is read from the current native Session source."""

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from orchestrator.services import vm_thread_initial
from orchestrator.services.stateless_workspace_gate import thread_metadata_object
from tests.test_pinned_vm_initial_binding_real_postgres import (
    _base_db,  # noqa: F401
    _bind_cold_agent,
    _initial_vm,
    _poll,
    _schema_applied,  # noqa: F401
    db as pg_store,  # noqa: F401
    pg_dsn,  # noqa: F401
    thread_schema,  # noqa: F401
)


@pytest.mark.asyncio
async def test_coordinator_polls_the_exact_postgres_startup_generation(
    pg_store, monkeypatch
):
    from agent.api.session_attach import SessionAttachCoordinator
    from agent.api.session_workspace import poll_workspace_ready
    from tests.test_session_attach_runtime import _identity, _ports

    current, source, _policy = await _waiting_source(pg_store, monkeypatch)
    view = await vm_thread_initial.initial_vm_startup_view(current, store=pg_store)
    assert view is not None
    generation = str(current["runtime_generation"])
    identity, _ = _identity()
    identity.bind_thread(str(current["id"]))
    identity.adopt(
        generation, str(current["runtime_attach_token"]), contract_advertised=True
    )
    wait = {
        "vm_status": "waiting_capacity",
        "session_runtime_generation": generation,
        "vm_startup": view,
    }
    ready = {
        **wait,
        "vm_status": "ready",
        "vm_ssh_host": "192.0.2.10",
        "vm_startup": {**view, "phase": "admitted", "admission_elapsed_s": 1},
    }
    client = SimpleNamespace(get_thread_workspace=AsyncMock(side_effect=[wait, ready]))
    coordinator = SessionAttachCoordinator(
        _ports(
            identity,
            orchestrator_client=lambda: client,
            poll_workspace_ready=poll_workspace_ready,
        )
    )
    result = await coordinator._poll_workspace(
        str(current["id"]), require_vm=True, poll_interval=0
    )
    assert result is not None
    assert result["session_runtime_generation"] == generation
    assert client.get_thread_workspace.await_count == 2
    assert (
        await pg_store.fetchval(
            "SELECT count(*) FROM vm_creation_effects WHERE request_id=$1",
            source["request_id"],
        )
        == 0
    )


async def _waiting_source(store, monkeypatch):
    thread_id, policy, _override, dependencies = await _initial_vm(
        store, monkeypatch, native=True
    )
    thread = await _bind_cold_agent(store, thread_id)
    await _poll(store, dependencies.vm_provisioner, thread)
    source = await store.fetchrow(
        "SELECT * FROM vm_creation_retries WHERE thread_id=$1", thread_id
    )
    assert source is not None
    # The initial request starts queued; resource admission records this
    # reason only after it has evaluated the still-waiting waiter.
    await store.execute(
        "UPDATE vm_creation_retries SET reason='resource_wait' WHERE request_id=$1",
        source["request_id"],
    )
    current = await store.get_thread(str(thread_id))
    return current, source, policy


@pytest.mark.asyncio
async def test_current_initial_session_wait_has_only_typed_read_only_hint(
    pg_store,  # noqa: F811
    monkeypatch,
):
    current, source, _policy = await _waiting_source(pg_store, monkeypatch)
    before = await pg_store.fetchrow(
        "SELECT state,reason,revision,boot_counted FROM vm_creation_retries WHERE request_id=$1",
        source["request_id"],
    )
    view = await vm_thread_initial.initial_vm_startup_view(current, store=pg_store)
    assert view == {
        "contract_version": 1,
        "phase": "resource_wait",
        "request_id": str(source["request_id"]),
        "provision_generation": str(source["provision_generation"]),
        "runtime_generation": str(current["runtime_generation"]),
    }
    assert (
        vm_thread_initial.initial_vm_wait_payload(current, startup_view=view)[
            "vm_startup"
        ]
        == view
    )
    assert not any(
        key in view for key in ("token", "credentials", "grant", "ssh_host", "capacity")
    )
    assert (
        await pg_store.fetchrow(
            "SELECT state,reason,revision,boot_counted FROM vm_creation_retries WHERE request_id=$1",
            source["request_id"],
        )
        == before
    )
    assert (
        await pg_store.fetchval(
            "SELECT count(*) FROM vm_resource_reservations WHERE request_id=$1",
            source["request_id"],
        )
        == 0
    )
    assert (
        await pg_store.fetchval(
            "SELECT count(*) FROM vm_creation_effects WHERE request_id=$1",
            source["request_id"],
        )
        == 0
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation",
    [
        "runtime",
        "agent",
        "attach",
        "owner",
        "project",
        "marker",
        "request",
        "provision",
        "retirement",
        "idle_intent",
        "nonfit",
        "cancelled",
        "attention",
        "settled",
        "missing_waiter",
        "observed_vm",
        "observed_pvc",
        "effect",
        "boot_counted",
    ],
)
async def test_wait_hint_refuses_other_actor_or_nonwaiting_source(
    pg_store,  # noqa: F811
    monkeypatch,
    mutation,
):
    current, source, _policy = await _waiting_source(pg_store, monkeypatch)
    current = dict(current)
    request_id = source["request_id"]
    if mutation in {"runtime", "agent", "attach", "owner", "project"}:
        current[
            {
                "runtime": "runtime_generation",
                "agent": "agent_id",
                "attach": "runtime_attach_token",
                "owner": "user_id",
                "project": "project_id",
            }[mutation]
        ] = uuid4()
    elif mutation in {"marker", "request", "provision"}:
        metadata = deepcopy(thread_metadata_object(current))
        vm = metadata["vm"]
        if mutation == "marker":
            vm["initial_runtime"]["agent_id"] = str(uuid4())
        else:
            vm[
                "creation_request_id"
                if mutation == "request"
                else "provision_generation"
            ] = str(uuid4())
        current["metadata"] = metadata
    elif mutation == "retirement":
        current["runtime_retirement_token"] = uuid4()
    elif mutation == "idle_intent":
        current["pinned_idle_terminal_intent_at"] = "2026-10-01T00:00:00Z"
    elif mutation == "nonfit":
        await pg_store.execute(
            "UPDATE vm_resource_waiters SET state='nonfit' WHERE request_id=$1",
            request_id,
        )
    elif mutation == "missing_waiter":
        await pg_store.execute(
            "DELETE FROM vm_resource_waiters WHERE request_id=$1", request_id
        )
    elif mutation in {"cancelled", "attention", "settled"}:
        if mutation == "attention":
            await pg_store.execute(
                "UPDATE vm_creation_retries SET state='reconciling',claim_token=$2,"
                "claim_expires_at=clock_timestamp()+interval '60 seconds' WHERE request_id=$1",
                request_id,
                uuid4(),
            )
            await pg_store.execute(
                "UPDATE vm_creation_retries SET state='attention',claim_token=NULL,"
                "claim_expires_at=NULL WHERE request_id=$1",
                request_id,
            )
        else:
            await pg_store.execute(
                "UPDATE vm_creation_retries SET state='cancel_requested' WHERE request_id=$1",
                request_id,
            )
            if mutation == "settled":
                await pg_store.execute(
                    "UPDATE vm_creation_retries SET state='settled',"
                    "resolved_at=clock_timestamp() WHERE request_id=$1",
                    request_id,
                )
    elif mutation in {"observed_vm", "observed_pvc", "boot_counted"}:
        field = {
            "observed_vm": "observed_vm_uid",
            "observed_pvc": "observed_pvc_uid",
            "boot_counted": "boot_counted",
        }[mutation]
        await pg_store.execute(
            f"UPDATE vm_creation_retries SET {field}=$2 WHERE request_id=$1",
            request_id,
            True if mutation == "boot_counted" else uuid4(),
        )
    elif mutation == "effect":
        await pg_store.execute(
            "INSERT INTO vm_creation_effects "
            "(effect_nonce,request_id,effect_number,effect_kind,carrier_uid,"
            "carrier_namespace,carrier_intent) "
            "VALUES($1,$2,1,'vm',$3,'test','{}'::jsonb)",
            uuid4(),
            request_id,
            uuid4(),
        )
    source_before = await pg_store.fetchval(
        "SELECT to_jsonb(r) FROM vm_creation_retries r WHERE request_id=$1",
        request_id,
    )
    waiter_before = await pg_store.fetchval(
        "SELECT to_jsonb(w) FROM vm_resource_waiters w WHERE request_id=$1",
        request_id,
    )
    effects_before = await pg_store.fetchval(
        "SELECT count(*) FROM vm_creation_effects WHERE request_id=$1",
        request_id,
    )
    reservations_before = await pg_store.fetchval(
        "SELECT count(*) FROM vm_resource_reservations WHERE request_id=$1",
        request_id,
    )
    assert (
        await vm_thread_initial.initial_vm_startup_view(current, store=pg_store) is None
    )
    assert (
        await pg_store.fetchval(
            "SELECT to_jsonb(r) FROM vm_creation_retries r WHERE request_id=$1",
            request_id,
        )
        == source_before
    )
    assert (
        await pg_store.fetchval(
            "SELECT to_jsonb(w) FROM vm_resource_waiters w WHERE request_id=$1",
            request_id,
        )
        == waiter_before
    )
    assert (
        await pg_store.fetchval(
            "SELECT count(*) FROM vm_creation_effects WHERE request_id=$1",
            request_id,
        )
        == effects_before
    )
    assert (
        await pg_store.fetchval(
            "SELECT count(*) FROM vm_resource_reservations WHERE request_id=$1",
            request_id,
        )
        == reservations_before
    )


@pytest.mark.asyncio
async def test_admitted_hint_uses_durable_reservation_age(pg_store, monkeypatch):  # noqa: F811
    current, source, policy = await _waiting_source(pg_store, monkeypatch)
    admitted = await policy.admit(request_id=str(source["request_id"]))
    assert admitted["action"] == "admitted"
    view = await vm_thread_initial.initial_vm_startup_view(current, store=pg_store)
    assert view["phase"] == "admitted"
    assert view["request_id"] == str(source["request_id"])
    assert 0 <= view["admission_elapsed_s"] < 30
    assert (
        await pg_store.fetchval(
            "SELECT count(*) FROM vm_creation_effects WHERE request_id=$1",
            source["request_id"],
        )
        == 0
    )


@pytest.mark.asyncio
async def test_wait_hint_rejects_corrupt_immutable_waiter_digest_without_writing(
    pg_store,  # noqa: F811
    monkeypatch,
):
    current, source, _policy = await _waiting_source(pg_store, monkeypatch)
    request_id = source["request_id"]
    before = await pg_store.fetchrow(
        "SELECT request_digest,state,revision FROM vm_resource_waiters WHERE request_id=$1",
        request_id,
    )
    with pytest.raises(_RollbackFixture):
        async with pg_store.acquire() as conn, conn.transaction():
            # Simulate an impossible durable mismatch inside a rolled-back
            # transaction; the production query must still check both digests.
            await conn.execute("SET LOCAL session_replication_role = replica")
            await conn.execute(
                "UPDATE vm_resource_waiters SET request_digest=$2 WHERE request_id=$1",
                request_id,
                "sha256:" + "f" * 64,
            )
            assert (
                await vm_thread_initial.initial_vm_startup_view(current, store=conn)
                is None
            )
            raise _RollbackFixture
    assert (
        await pg_store.fetchrow(
            "SELECT request_digest,state,revision FROM vm_resource_waiters WHERE request_id=$1",
            request_id,
        )
        == before
    )


class _RollbackFixture(Exception):
    pass


@pytest.mark.asyncio
async def test_released_reservation_history_cannot_return_to_wait(
    pg_store,  # noqa: F811
    monkeypatch,
):
    current, source, policy = await _waiting_source(pg_store, monkeypatch)
    request_id = source["request_id"]
    assert (await policy.admit(request_id=str(request_id)))["action"] == "admitted"
    before = await pg_store.fetchrow(
        "SELECT state,reason,revision FROM vm_creation_retries WHERE request_id=$1",
        request_id,
    )
    assert before["reason"] == "resource_wait"
    with pytest.raises(_RollbackFixture):
        async with pg_store.acquire() as conn, conn.transaction():
            await conn.execute("SET LOCAL session_replication_role = replica")
            await conn.execute(
                "UPDATE vm_resource_reservations SET state='released',"
                "released_at=clock_timestamp(),release_evidence='{}'::jsonb "
                "WHERE request_id=$1",
                request_id,
            )
            await conn.execute(
                "UPDATE vm_resource_waiters SET state='waiting' WHERE request_id=$1",
                request_id,
            )
            assert (
                await vm_thread_initial.initial_vm_startup_view(current, store=conn)
                is None
            )
            raise _RollbackFixture
    assert (
        await pg_store.fetchrow(
            "SELECT state,reason,revision FROM vm_creation_retries WHERE request_id=$1",
            request_id,
        )
        == before
    )
