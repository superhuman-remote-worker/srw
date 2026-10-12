"""Positive failed-initial End must terminalize its exact unadmitted waiter."""

import asyncio
from uuid import uuid4

import pytest

from tests.test_vm_source_actor_audit_real_postgres import (
    actor_schema,  # noqa: F401
    audit_schema,  # noqa: F401
    _base_db,  # noqa: F401
    _schema_applied,  # noqa: F401
    cleanup_lineage_schema,  # noqa: F401
    db as _db,
    pg_dsn,  # noqa: F401
    thread_schema,  # noqa: F401
)
from tests.test_pinned_vm_initial_binding_real_postgres import (
    _initial_vm,
    _bind_protected_agent,
    _poll,
)
from tests.test_pinned_vm_failed_initial_end_real_postgres import (
    _begin,
    _authorize,
    _current_zero_arguments,
    _abort_and_rebind_same_pod,
    _wait_for_owner_waiters,
)
from orchestrator.services.vm_creation_retry_store import (
    VMCreationRetryConflict,
    VMCreationRetryStore,
)
from orchestrator.services.vm_resource_reservation_store import (
    VMResourceReservationStore,
)
from shared.vm_resource_admission import ResourceAdmissionError


db = _db


@pytest.mark.asyncio
@pytest.mark.parametrize("permanent", [False, True])
async def test_normal_unadmitted_initial_vm_end_settles_source_and_waiter(
    db,
    monkeypatch,
    permanent,  # noqa: F811
):
    thread_id, policy, override, dependencies = await _initial_vm(
        db, monkeypatch, native=True
    )
    current = await _bind_protected_agent(db, thread_id)
    await _poll(db, dependencies.vm_provisioner, current)
    source = await db.fetchrow(
        "SELECT * FROM vm_creation_retries WHERE thread_id=$1", thread_id
    )
    waiter = await db.fetchrow(
        "SELECT * FROM vm_resource_waiters WHERE request_id=$1", source["request_id"]
    )
    assert waiter["state"] == "waiting"
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_resource_reservations WHERE request_id=$1",
            source["request_id"],
        )
        == 0
    )
    retirement = await _begin(db, current, permanent=permanent)
    assert retirement["state"] == "pending", retirement
    await _authorize(db, current, retirement)
    assert (
        await VMCreationRetryStore(db).settle_never_issued(
            request_id=str(source["request_id"])
        )
    )["settled"]
    assert await db.acknowledge_pinned_thread_local_quiescence(
        str(thread_id), **await _current_zero_arguments(db, current, retirement)
    )
    if permanent:
        agent = await db.get_agent(str(current["agent_id"]))
        assert await db.clear_pinned_retirement_physical_runtime_endpoint(
            str(thread_id),
            runtime_generation=retirement["generation"],
            retirement_token=retirement["token"],
            completed_quiescence_protocol="agent_runtime_zero_v1",
            expected_stopped_agent_pod_name=agent["hostname"],
            expected_stopped_agent_pod_uid=agent["pod_uid"],
        )
        await db.delete_thread(
            str(thread_id),
            expected_runtime_generation=retirement["generation"],
            expected_runtime_retirement_token=retirement["token"],
        )
        assert await db.get_thread(str(thread_id)) is None
    else:
        assert await db.settle_pinned_thread_retirement(
            str(thread_id),
            token=retirement["token"],
            generation=retirement["generation"],
            final_status="ended",
        )
        assert (await db.get_thread(str(thread_id)))["status"] == "ended"
    terminal = await db.fetchrow(
        "SELECT * FROM vm_resource_waiters WHERE request_id=$1", source["request_id"]
    )
    assert terminal["state"] == "cancelled"
    assert terminal["reason"] == "creation_cancelled"
    for field in (
        "request_id",
        "thread_id",
        "provision_generation",
        "request_digest",
        "enqueued_at",
        "bypasses",
        "protected_order",
    ):
        assert terminal[field] == waiter[field]
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_resource_reservations WHERE request_id=$1",
            source["request_id"],
        )
        == 0
    )


async def _waiting(db, monkeypatch):
    thread_id, policy, _, dependencies = await _initial_vm(db, monkeypatch, native=True)
    current = await _bind_protected_agent(db, thread_id)
    await _poll(db, dependencies.vm_provisioner, current)
    source = await db.fetchrow(
        "SELECT * FROM vm_creation_retries WHERE thread_id=$1", thread_id
    )
    return current, source, policy


async def _cancel(db, current):
    retirement = await _begin(db, current, permanent=False)
    assert retirement["state"] == "pending", retirement
    await _authorize(db, current, retirement)
    return retirement


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["waiting", "nonfit", "parked"])
async def test_unadmitted_source_settlement_preserves_fairness_and_replays_purely(
    db, monkeypatch, state
):
    current, source, _ = await _waiting(db, monkeypatch)
    await db.execute(
        "UPDATE vm_resource_waiters SET state=$2,bypasses=2,protected_order=7,revision=revision+1 WHERE request_id=$1",
        source["request_id"],
        state,
    )
    retirement = await _cancel(db, current)
    before = dict(
        await db.fetchrow(
            "SELECT * FROM vm_resource_waiters WHERE request_id=$1",
            source["request_id"],
        )
    )
    retries = VMCreationRetryStore(db)
    assert (await retries.settle_never_issued(request_id=str(source["request_id"])))[
        "settled"
    ]
    after = dict(
        await db.fetchrow(
            "SELECT * FROM vm_resource_waiters WHERE request_id=$1",
            source["request_id"],
        )
    )
    assert after == {
        **before,
        "state": "cancelled",
        "reason": "creation_cancelled",
        "revision": before["revision"] + 1,
    }
    assert (await retries.settle_never_issued(request_id=str(source["request_id"])))[
        "settled"
    ]
    assert (
        dict(
            await db.fetchrow(
                "SELECT * FROM vm_resource_waiters WHERE request_id=$1",
                source["request_id"],
            )
        )
        == after
    )
    assert await db.acknowledge_pinned_thread_local_quiescence(
        str(current["id"]), **await _current_zero_arguments(db, current, retirement)
    )
    assert await db.settle_pinned_thread_retirement(
        str(current["id"]),
        token=retirement["token"],
        generation=retirement["generation"],
        final_status="ended",
    )


@pytest.mark.asyncio
async def test_waiter_cancellation_rolls_back_with_failed_source_settlement(
    db, monkeypatch
):
    current, source, _ = await _waiting(db, monkeypatch)
    await _cancel(db, current)
    before = dict(
        await db.fetchrow(
            "SELECT * FROM vm_resource_waiters WHERE request_id=$1",
            source["request_id"],
        )
    )
    source_before = dict(
        await db.fetchrow(
            "SELECT * FROM vm_creation_retries WHERE request_id=$1",
            source["request_id"],
        )
    )
    thread_before = await db.get_thread(str(current["id"]))
    actual = VMResourceReservationStore.release_never_issued_on_conn

    async def interrupt(self, conn, **kwargs):
        assert await actual(self, conn, **kwargs) is False
        assert (
            await conn.fetchval(
                "SELECT state FROM vm_resource_waiters WHERE request_id=$1",
                source["request_id"],
            )
            == "cancelled"
        )
        raise RuntimeError("injected loss before source settlement commit")

    monkeypatch.setattr(
        VMResourceReservationStore, "release_never_issued_on_conn", interrupt
    )
    with pytest.raises(RuntimeError, match="injected loss"):
        await VMCreationRetryStore(db).settle_never_issued(
            request_id=str(source["request_id"])
        )
    assert (
        dict(
            await db.fetchrow(
                "SELECT * FROM vm_resource_waiters WHERE request_id=$1",
                source["request_id"],
            )
        )
        == before
    )
    assert (
        dict(
            await db.fetchrow(
                "SELECT * FROM vm_creation_retries WHERE request_id=$1",
                source["request_id"],
            )
        )
        == source_before
    )
    assert await db.get_thread(str(current["id"])) == thread_before


@pytest.mark.asyncio
@pytest.mark.parametrize("first", ["admit", "end"])
async def test_admission_and_unadmitted_source_end_serialize_in_both_orders(
    db, monkeypatch, first
):
    current, source, policy = await _waiting(db, monkeypatch)

    async def end():
        await _cancel(db, current)
        return await VMCreationRetryStore(db).settle_never_issued(
            request_id=str(source["request_id"])
        )

    async def admit():
        return await policy.admit(request_id=str(source["request_id"]))

    async with db.acquire() as lock, lock.transaction():
        await lock.fetchval(
            "SELECT id FROM threads WHERE id=$1 FOR UPDATE", current["id"]
        )
        first_task = asyncio.create_task(admit() if first == "admit" else end())
        await _wait_for_owner_waiters(db, 1)
        second_task = asyncio.create_task(end() if first == "admit" else admit())
        await _wait_for_owner_waiters(db, 2)
    one, two = await asyncio.gather(first_task, second_task)
    admitted, ended = (one, two) if first == "admit" else (two, one)
    assert ended["settled"]
    assert admitted["action"] == ("admitted" if first == "admit" else "unavailable")
    assert await db.fetchval(
        "SELECT state FROM vm_resource_waiters WHERE request_id=$1",
        source["request_id"],
    ) == ("released" if first == "admit" else "cancelled")
    reservations = await db.fetch(
        "SELECT * FROM vm_resource_reservations WHERE request_id=$1",
        source["request_id"],
    )
    assert len(reservations) == (1 if first == "admit" else 0)
    assert all(row["state"] == "released" for row in reservations)
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_creation_effects WHERE request_id=$1",
            source["request_id"],
        )
        == 0
    )


@pytest.mark.asyncio
async def test_unadmitted_source_after_protected_abort_and_actor_gc_ends_without_maintenance(
    db, monkeypatch
):
    current, source, _ = await _waiting(db, monkeypatch)
    await _abort_and_rebind_same_pod(db, current, rebind=False)
    assert await db.delete_agent(str(current["agent_id"]))
    detached = await db.get_thread(str(current["id"]))
    retirement = await _begin(db, detached, permanent=True)
    assert retirement["state"] == "pending", retirement
    await _authorize(db, detached, retirement)
    assert (
        await VMCreationRetryStore(db).settle_never_issued(
            request_id=str(source["request_id"])
        )
    )["settled"]
    assert await db.clear_pinned_retirement_physical_runtime_endpoint(
        str(current["id"]),
        runtime_generation=retirement["generation"],
        retirement_token=retirement["token"],
    )
    await db.delete_thread(
        str(current["id"]),
        expected_runtime_generation=retirement["generation"],
        expected_runtime_retirement_token=retirement["token"],
    )
    assert await db.get_thread(str(current["id"])) is None
    assert (
        await db.fetchval(
            "SELECT state FROM vm_resource_waiters WHERE request_id=$1",
            source["request_id"],
        )
        == "cancelled"
    )
    assert (
        await db.fetchval(
            "SELECT thread_agent_id FROM vm_creation_retries WHERE request_id=$1",
            source["request_id"],
        )
        == source["thread_agent_id"]
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "damage",
    [
        "admitted",
        "waiter_digest",
        "authorization",
        "generation",
        "carrier",
        "observed_pvc",
        "admission",
        "captured_source",
        "issued_effect",
        "rejected_effect",
    ],
)
async def test_unadmitted_waiter_revalidates_current_authority_and_no_debt(
    db, monkeypatch, damage
):
    current, source, _ = await _waiting(db, monkeypatch)
    await _cancel(db, current)
    before = dict(
        await db.fetchrow(
            "SELECT * FROM vm_resource_waiters WHERE request_id=$1",
            source["request_id"],
        )
    )
    actual = VMResourceReservationStore.release_never_issued_on_conn

    async def corrupt_then_release(self, conn, **kwargs):
        # Test-only impossible-history fixtures after the outer authority read.
        # Restore ordinary triggers before invoking the actual resource writer.
        await conn.execute("SET LOCAL session_replication_role=replica")
        if damage == "admitted":
            await conn.execute(
                "UPDATE vm_resource_waiters SET state='admitted' WHERE request_id=$1",
                source["request_id"],
            )
        elif damage == "waiter_digest":
            await conn.execute(
                "UPDATE vm_resource_waiters SET request_digest=$2 WHERE request_id=$1",
                source["request_id"],
                "sha256:" + "f" * 64,
            )
        elif damage == "authorization":
            await conn.execute(
                "UPDATE threads SET runtime_retirement_authorized_at=NULL WHERE id=$1",
                current["id"],
            )
        elif damage == "generation":
            await conn.execute(
                "UPDATE threads SET runtime_generation=$2 WHERE id=$1",
                current["id"],
                uuid4(),
            )
        elif damage == "carrier":
            await conn.execute(
                "UPDATE vm_creation_retries SET creation_carrier_uid=$2,creation_carrier_namespace='workers' WHERE request_id=$1",
                source["request_id"],
                uuid4(),
            )
        elif damage == "observed_pvc":
            await conn.execute(
                "UPDATE vm_creation_retries SET observed_pvc_uid=$2 WHERE request_id=$1",
                source["request_id"],
                uuid4(),
            )
        elif damage == "admission":
            await conn.execute(
                "UPDATE vm_creation_retries SET creation_admission_id=$2 WHERE request_id=$1",
                source["request_id"],
                uuid4(),
            )
        elif damage == "captured_source":
            await conn.execute(
                "UPDATE threads SET runtime_retirement_context=jsonb_set(runtime_retirement_context,'{vm_creation_source,request_id}',to_jsonb($2::text)) WHERE id=$1",
                current["id"],
                str(uuid4()),
            )
        elif damage in {"issued_effect", "rejected_effect"}:
            await conn.execute(
                "INSERT INTO vm_creation_effects(effect_nonce,request_id,effect_number,effect_kind,carrier_uid,carrier_namespace,carrier_intent) VALUES($1,$2,1,'rootdisk',$3,'workers','{}')",
                uuid4(),
                source["request_id"],
                uuid4(),
            )
            if damage == "rejected_effect":
                await conn.execute(
                    "UPDATE vm_creation_effects SET state='rejected',evidence='{\"outcome\":\"rejected\"}',resolved_at=now() WHERE request_id=$1",
                    source["request_id"],
                )
        await conn.execute("SET LOCAL session_replication_role=origin")
        return await actual(self, conn, **kwargs)

    monkeypatch.setattr(
        VMResourceReservationStore, "release_never_issued_on_conn", corrupt_then_release
    )
    with pytest.raises(ResourceAdmissionError):
        await VMCreationRetryStore(db).settle_never_issued(
            request_id=str(source["request_id"])
        )
    assert (
        dict(
            await db.fetchrow(
                "SELECT * FROM vm_resource_waiters WHERE request_id=$1",
                source["request_id"],
            )
        )
        == before
    )
    assert (
        await db.fetchval(
            "SELECT state FROM vm_creation_retries WHERE request_id=$1",
            source["request_id"],
        )
        == "cancel_requested"
    )
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_resource_reservations WHERE request_id=$1",
            source["request_id"],
        )
        == 0
    )


@pytest.mark.asyncio
async def test_wrong_end_token_cannot_authorize_unadmitted_source_settlement(
    db, monkeypatch
):
    current, source, _ = await _waiting(db, monkeypatch)
    retirement = await _begin(db, current, permanent=False)
    assert retirement["state"] == "pending", retirement
    assert not await db.authorize_pinned_thread_retirement(
        str(current["id"]),
        token=str(uuid4()),
        generation=retirement["generation"],
        settle_status="ended",
    )
    with pytest.raises(VMCreationRetryConflict):
        await VMCreationRetryStore(db).settle_never_issued(
            request_id=str(source["request_id"])
        )
    assert (
        await db.fetchval(
            "SELECT state FROM vm_resource_waiters WHERE request_id=$1",
            source["request_id"],
        )
        == "waiting"
    )
    await _authorize(db, current, retirement)
    assert (
        await VMCreationRetryStore(db).settle_never_issued(
            request_id=str(source["request_id"])
        )
    )["settled"]


@pytest.mark.asyncio
async def test_job_without_reservation_keeps_existing_waiter_behavior(db):
    from tests.test_vm_resource_whole_store_real_postgres import environment, waiter

    policy, inventory, _, _ = await environment(db)
    source = await waiter(db, policy, inventory)
    assert await db.linearize_pinned_cancel(
        str(source["job_id"]), expected_status="paused"
    )
    before = dict(
        await db.fetchrow(
            "SELECT * FROM vm_resource_waiters WHERE request_id=$1",
            source["request_id"],
        )
    )
    assert (
        await VMCreationRetryStore(db).settle_never_issued(
            request_id=str(source["request_id"])
        )
    )["settled"]
    assert (
        dict(
            await db.fetchrow(
                "SELECT * FROM vm_resource_waiters WHERE request_id=$1",
                source["request_id"],
            )
        )
        == before
    )


@pytest.mark.asyncio
async def test_already_cancelled_waiter_is_a_pure_source_settlement_replay(
    db, monkeypatch
):
    from orchestrator.services.vm_resource_waiter_maintenance import (
        VMResourceWaiterMaintenance,
    )

    current, source, policy = await _waiting(db, monkeypatch)
    await _cancel(db, current)
    assert (
        await VMResourceWaiterMaintenance(policy).maintain(
            request_id=str(source["request_id"])
        )
    )["action"] == "cancelled"
    before = dict(
        await db.fetchrow(
            "SELECT * FROM vm_resource_waiters WHERE request_id=$1",
            source["request_id"],
        )
    )
    assert (
        await VMCreationRetryStore(db).settle_never_issued(
            request_id=str(source["request_id"])
        )
    )["settled"]
    assert (
        dict(
            await db.fetchrow(
                "SELECT * FROM vm_resource_waiters WHERE request_id=$1",
                source["request_id"],
            )
        )
        == before
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("configured_source", ["golden", "prepared"])
async def test_never_authorized_configured_source_needs_no_pin_disposition(
    db, monkeypatch, configured_source
):
    from orchestrator.services import vm_creation_transport
    from orchestrator.services.vm_creation_request import build_vm_creation_request
    from orchestrator.services.vm_provisioner import VMProvisioner
    from shared.vm_creation_issuance import canonical_configuration_digest
    from shared.vm_creation_retry import canonical_request_digest
    from shared.workspace_preparation import preparation_request

    thread_id, _, override, dependencies = await _initial_vm(
        db, monkeypatch, native=True
    )
    current = await _bind_protected_agent(db, thread_id)
    resolve = vm_creation_transport.resolve_vm_creation_configuration

    async def configured(*args, **kwargs):
        result = await resolve(*args, **kwargs)
        result["controller_configuration"]["golden_enabled"] = True
        return result

    monkeypatch.setattr(
        vm_creation_transport, "resolve_vm_creation_configuration", configured
    )
    if configured_source == "golden":
        # Authenticated delivery uses an actually frozen golden-enabled config.
        await _poll(db, dependencies.vm_provisioner, current)
    else:
        # Real source CAS with a valid Session preparation request; this case
        # proves never-authorized source cancellation, not artifact preparation.
        context = VMProvisioner._fresh_provision_ctx()
        context["status"] = "provisioning"
        preparation = preparation_request(
            {"image": override["workspace"]["vm"]["image"], "prepare": []},
            scope_kind="Account",
            scope_uid=current["user_id"],
            allocation_id=thread_id,
            owner_kind="session",
            runtime_generation=current["runtime_generation"],
        )
        request = build_vm_creation_request(
            job_id=str(thread_id),
            entity_type="thread",
            agent_config="session_base",
            vm_image=override["workspace"]["vm"]["image"],
            cpu_cores=8,
            memory="16Gi",
            description="never-authorized prepared source",
            network_tier="restricted",
            provision_generation=context["provision_generation"],
            preparation=preparation,
        )
        resolved = await configured(None, request, secret=b"initial-binding-test")
        config = resolved["controller_configuration"]
        assert await db.begin_pinned_thread_vm_provisioning(
            str(thread_id),
            expected_runtime_generation=str(current["runtime_generation"]),
            expected_agent_id=str(current["agent_id"]),
            expected_attach_token=str(current["runtime_attach_token"]),
            expected_vm_context=None,
            provision_context=context,
            creation_source={
                "request_id": str(uuid4()),
                "request": request,
                "request_digest": canonical_request_digest(request),
                "controller_configuration": config,
                "controller_configuration_digest": canonical_configuration_digest(
                    config
                ),
            },
        )
    source = await db.fetchrow(
        "SELECT * FROM vm_creation_retries WHERE thread_id=$1", thread_id
    )
    assert source["creation_admission_id"] is None
    assert source["creation_carrier_uid"] is None
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_creation_effects WHERE request_id=$1",
            source["request_id"],
        )
        == 0
    )
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_resource_reservations WHERE request_id=$1",
            source["request_id"],
        )
        == 0
    )
    retirement = await _cancel(db, current)
    assert (
        await VMCreationRetryStore(db).settle_never_issued(
            request_id=str(source["request_id"])
        )
    )["settled"]
    assert (
        await db.fetchval(
            "SELECT state FROM vm_resource_waiters WHERE request_id=$1",
            source["request_id"],
        )
        == "cancelled"
    )
    assert await db.acknowledge_pinned_thread_local_quiescence(
        str(thread_id), **await _current_zero_arguments(db, current, retirement)
    )
    assert await db.settle_pinned_thread_retirement(
        str(thread_id),
        token=retirement["token"],
        generation=retirement["generation"],
        final_status="ended",
    )
