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
from tests.test_session_attach_shutdown_ordering import (
    runtime as shutdown_runtime,  # noqa: F401
)


@pytest.mark.asyncio
async def test_coordinator_polls_the_exact_postgres_startup_generation(
    pg_store,  # noqa: F811
    monkeypatch,
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


async def _nonquota_waiting_source(store, monkeypatch):
    import httpx
    from orchestrator.services.vm_creation_retry_store import VMCreationRetryStore
    from orchestrator.services.vm_creation_transport import replay_vm_creation
    from shared.vm_lifecycle_auth import AUTH_FIELD, sign_payload, verify_payload
    from tests.test_vm_resource_configuration import whole_launcher_configuration

    thread_id, _policy, _override, dependencies = await _initial_vm(
        store, monkeypatch, native=True
    )
    current = await _bind_cold_agent(store, thread_id)
    configuration = whole_launcher_configuration()
    configuration.update(version=1, namespace="workers", storage_class="local")
    configuration.pop("resource_admission")
    monkeypatch.delenv("VM_RESOURCE_ADMISSION_CONFIG")

    async def resolve(_client, request, *, secret):
        return {"request": request, "controller_configuration": configuration}

    monkeypatch.setattr(
        "orchestrator.services.vm_creation_transport.resolve_vm_creation_configuration",
        resolve,
    )
    await _poll(store, dependencies.vm_provisioner, current)
    retry = VMCreationRetryStore(store)
    claim = (await retry.claim_due(limit=1))[0]
    secret = b"initial-binding-test"

    async def capacity_response(path, *, json, timeout):
        assert path == "/vm-creation/create"
        assert verify_payload(
            json, direction="request", operation="creation_retry_create", secret=secret
        )
        result = sign_payload(
            {
                "status": "creation_pending",
                "reason": "capacity_wait",
                "job_id": str(thread_id),
                "provision_generation": str(claim["provision_generation"]),
            },
            direction="response",
            operation="creation_retry_create",
            secret=secret,
            correlation_id=json[AUTH_FIELD]["request_id"],
        )
        return httpx.Response(
            200, json=result, request=httpx.Request("POST", "http://controller" + path)
        )

    observation = await replay_vm_creation(
        SimpleNamespace(post=AsyncMock(side_effect=capacity_response)),
        claim,
        secret=secret,
    )
    assert observation == {"outcome": "capacity_wait", "reason": "capacity_wait"}
    assert await retry.apply_observation(
        request_id=str(claim["request_id"]),
        claim_token=str(claim["claim_token"]),
        expected_revision=claim["revision"],
        observation=observation,
    )
    source = await store.fetchrow(
        "SELECT * FROM vm_creation_retries WHERE request_id=$1", claim["request_id"]
    )
    assert source["reason"] == "controller_count_wait"
    assert await store.merge_thread_vm_context_if_provision_generation(
        str(thread_id),
        str(source["provision_generation"]),
        {"status": "waiting_capacity"},
    )
    return await store.get_thread(str(thread_id)), source


@pytest.mark.asyncio
async def test_nonquota_authenticated_capacity_wait_outlasts_900_for_exact_life(
    pg_store,  # noqa: F811
    monkeypatch,
):
    from agent.api.session_attach import SessionAttachCoordinator
    from agent.api.session_workspace import poll_workspace_ready
    from tests.test_session_attach_runtime import _identity, _ports
    import time

    current, source = await _nonquota_waiting_source(pg_store, monkeypatch)
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
    identity, _ = _identity()
    identity.bind_thread(str(current["id"]))
    identity.adopt(
        str(current["runtime_generation"]),
        str(current["runtime_attach_token"]),
        contract_advertised=True,
    )
    clock = {"now": 0}

    async def payload(*_args, **_kwargs):
        result = {
            "vm_status": "waiting_capacity",
            "vm_startup": view,
            "session_runtime_generation": str(current["runtime_generation"]),
        }
        if clock["now"] >= 1860:
            result.update(
                vm_status="ready",
                vm_ssh_host="192.0.2.10",
                vm_startup={**view, "phase": "admitted", "admission_elapsed_s": 0},
            )
        return result

    async def sleep(seconds):
        clock["now"] += seconds

    client = SimpleNamespace(get_thread_workspace=AsyncMock(side_effect=payload))
    coordinator = SessionAttachCoordinator(
        _ports(
            identity,
            orchestrator_client=lambda: client,
            poll_workspace_ready=poll_workspace_ready,
        )
    )
    with monkeypatch.context() as timing:
        timing.setattr(time, "monotonic", lambda: clock["now"])
        timing.setattr("agent.api.session_workspace.asyncio.sleep", sleep)
        result = await coordinator._poll_workspace(
            str(current["id"]), require_vm=True, poll_interval=30
        )
    assert result is not None and result["session_runtime_generation"] == str(
        current["runtime_generation"]
    )
    assert clock["now"] == 1860
    assert (
        await pg_store.fetchrow(
            "SELECT state,reason,revision,boot_counted FROM vm_creation_retries WHERE request_id=$1",
            source["request_id"],
        )
        == before
    )
    for table in (
        "vm_resource_waiters",
        "vm_resource_reservations",
        "vm_creation_effects",
    ):
        assert (
            await pg_store.fetchval(
                f"SELECT count(*) FROM {table} WHERE request_id=$1",
                source["request_id"],
            )
            == 0
        )
    assert source["observed_vm_uid"] is None and source["observed_pvc_uid"] is None


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
        "unknown_wait",
        "quota_wait",
        "cancelled",
        "physical_vm",
    ],
)
async def test_nonquota_capacity_hint_refuses_inexact_or_unproven_source(
    pg_store,  # noqa: F811
    monkeypatch,
    mutation,
):
    current, source = await _nonquota_waiting_source(pg_store, monkeypatch)
    current = dict(current)
    if mutation in {"runtime", "agent", "attach", "owner", "project"}:
        key = {
            "runtime": "runtime_generation",
            "agent": "agent_id",
            "attach": "runtime_attach_token",
            "owner": "user_id",
            "project": "project_id",
        }[mutation]
        current[key] = uuid4()
    elif mutation in {"marker", "physical_vm"}:
        metadata = deepcopy(thread_metadata_object(current))
        if mutation == "marker":
            metadata["vm"]["initial_runtime"]["agent_id"] = str(uuid4())
        else:
            metadata["vm"]["vm_uid"] = str(uuid4())
        current["metadata"] = metadata
    elif mutation in {"unknown_wait", "quota_wait"}:
        await pg_store.execute(
            "UPDATE vm_creation_retries SET reason=$2 WHERE request_id=$1",
            source["request_id"],
            "capacity_wait" if mutation == "unknown_wait" else "resource_wait",
        )
    else:
        await pg_store.execute(
            "UPDATE vm_creation_retries SET state='cancel_requested' WHERE request_id=$1",
            source["request_id"],
        )
    assert (
        await vm_thread_initial.initial_vm_startup_view(current, store=pg_store) is None
    )


@pytest.mark.asyncio
async def test_nonquota_readiness_clock_starts_at_durable_controller_admission(
    pg_store,  # noqa: F811
    monkeypatch,
):
    from orchestrator.services.vm_creation_retry_store import VMCreationRetryStore

    current, source = await _nonquota_waiting_source(pg_store, monkeypatch)
    await pg_store.execute(
        "UPDATE vm_creation_retries SET next_probe_at=clock_timestamp() WHERE request_id=$1",
        source["request_id"],
    )
    retry = VMCreationRetryStore(pg_store)
    claim = (await retry.claim_due(limit=1))[0]
    admission = await retry.authorize_controller(
        request_id=str(source["request_id"]),
        claim_token=str(claim["claim_token"]),
        observed={
            "job_id": str(current["id"]),
            "provision_generation": str(source["provision_generation"]),
            "request_digest": source["request_digest"],
            "controller_configuration_digest": source[
                "controller_configuration_digest"
            ],
            "expected_pvc_uid": None,
        },
    )
    assert admission["allowed"] is True
    for _ in range(2):
        view = await vm_thread_initial.initial_vm_startup_view(current, store=pg_store)
        assert view is not None and view["phase"] == "admitted"
        assert 0 <= view["admission_elapsed_s"] < 30
    assert (
        await pg_store.fetchval(
            "SELECT count(*) FROM vm_creation_effects WHERE request_id=$1",
            source["request_id"],
        )
        == 0
    )


@pytest.mark.asyncio
async def test_bound_postgres_life_is_reserved_before_session_construction(
    pg_store,  # noqa: F811
    monkeypatch,
):
    import asyncio
    from agent.api.session_attach import SessionAttachCoordinator
    from tests.test_session_attach_runtime import _identity, _ports

    current, source, _policy = await _waiting_source(pg_store, monkeypatch)
    identity, _ = _identity()
    identity.bind_thread(str(current["id"]))
    identity.adopt(
        str(current["runtime_generation"]),
        str(current["runtime_attach_token"]),
        contract_advertised=True,
    )
    coordinator = SessionAttachCoordinator(_ports(identity))
    coordinator.attach = AsyncMock()
    try:
        assert coordinator.pool_heartbeat_status() == "session"
        admission = await coordinator.admit_pool_attach(str(current["id"]), {})
        assert admission.status_code == 409
        coordinator.attach.assert_not_awaited()
        assert coordinator.pool_task is None
        assert (
            await pg_store.fetchval(
                "SELECT count(*) FROM vm_creation_effects WHERE request_id=$1",
                source["request_id"],
            )
            == 0
        )
    finally:
        if coordinator.pool_task is not None:
            await asyncio.gather(coordinator.pool_task, return_exceptions=True)


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
@pytest.mark.parametrize("dedicated", [True, False], ids=["dedicated", "pool"])
async def test_shutdown_cannot_retire_a_postgres_life_while_attach_owns_cleanup(
    pg_store,  # noqa: F811
    monkeypatch,
    shutdown_runtime,  # noqa: F811
    dedicated,
):
    from agent.api import persistent_app as pa
    from tests.test_session_attach_shutdown_ordering import (
        test_lifespan_never_terminates_over_attach_cleanup,
    )

    current, source = await _nonquota_waiting_source(pg_store, monkeypatch)

    async def retire(*args, **kwargs):
        return await pg_store.begin_pinned_thread_retirement(
            str(current["id"]),
            permanent=False,
            expected_runtime_generation=str(current["runtime_generation"]),
            expected_agent_id=str(current["agent_id"]),
            expected_attach_token=str(current["runtime_attach_token"]),
            settle_status="ended",
        )

    pa._session_termination.terminate.side_effect = retire
    await test_lifespan_never_terminates_over_attach_cleanup(
        monkeypatch, shutdown_runtime, dedicated, True, thread=str(current["id"])
    )
    after = await pg_store.get_thread(str(current["id"]))
    assert after["runtime_generation"] == current["runtime_generation"]
    assert after["runtime_retirement_token"] is None
    assert after["agent_id"] == current["agent_id"]
    assert after["runtime_attach_token"] == current["runtime_attach_token"]
    assert (
        await pg_store.fetchval(
            "SELECT state FROM vm_creation_retries WHERE request_id=$1",
            source["request_id"],
        )
        == source["state"]
    )


@pytest.mark.asyncio
async def test_nonquota_admission_age_cannot_restart_on_read(
    pg_store,  # noqa: F811
    monkeypatch,
):
    from orchestrator.services.vm_creation_retry_store import VMCreationRetryStore

    current, source = await _nonquota_waiting_source(pg_store, monkeypatch)
    await pg_store.execute(
        "UPDATE vm_creation_retries SET next_probe_at=clock_timestamp() WHERE request_id=$1",
        source["request_id"],
    )
    retry = VMCreationRetryStore(pg_store)
    claim = (await retry.claim_due(limit=1))[0]
    admission = await retry.authorize_controller(
        request_id=str(source["request_id"]),
        claim_token=str(claim["claim_token"]),
        observed={
            "job_id": str(current["id"]),
            "provision_generation": str(source["provision_generation"]),
            "request_digest": source["request_digest"],
            "controller_configuration_digest": source[
                "controller_configuration_digest"
            ],
            "expected_pvc_uid": None,
        },
    )
    assert admission["allowed"] is True
    with pytest.raises(_RollbackFixture):
        async with pg_store.acquire() as conn, conn.transaction():
            # Age only this disposable fixture inside a rolled-back transaction.
            # The readiness view must read its persisted admission, not poll time.
            await conn.execute(
                "UPDATE vm_workspace_cleanup_admissions SET admitted_at=clock_timestamp()-interval '1900 seconds' "
                "WHERE id=(SELECT creation_admission_id FROM vm_creation_retries WHERE request_id=$1)",
                source["request_id"],
            )
            for _ in range(2):
                view = await vm_thread_initial.initial_vm_startup_view(
                    current, store=conn
                )
                assert view is not None and view["phase"] == "admitted"
                assert 1900 <= view["admission_elapsed_s"] < 1930
            raise _RollbackFixture


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "column", ["request_digest", "controller_configuration_digest"]
)
async def test_nonquota_wait_refuses_corrupt_frozen_digest_without_writing(
    pg_store,  # noqa: F811
    monkeypatch,
    column,
):
    current, source = await _nonquota_waiting_source(pg_store, monkeypatch)
    with pytest.raises(_RollbackFixture):
        async with pg_store.acquire() as conn, conn.transaction():
            await conn.execute("SET LOCAL session_replication_role = replica")
            await conn.execute(
                f"UPDATE vm_creation_retries SET {column}=$2 WHERE request_id=$1",
                source["request_id"],
                "sha256:" + "f" * 64,
            )
            assert (
                await vm_thread_initial.initial_vm_startup_view(current, store=conn)
                is None
            )
            raise _RollbackFixture
    after = await pg_store.fetchrow(
        "SELECT state,reason,revision,request_digest,controller_configuration_digest "
        "FROM vm_creation_retries WHERE request_id=$1",
        source["request_id"],
    )
    assert all(after[key] == source[key] for key in after.keys())


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
