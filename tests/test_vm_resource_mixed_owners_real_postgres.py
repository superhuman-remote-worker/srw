"""Jobs and genuine pinned threads compete in one durable resource ledger."""

import asyncio
from uuid import uuid4

import pytest

from orchestrator.services.vm_creation_request import build_vm_creation_request
from orchestrator.services.vm_provisioner import VMProvisioner
from orchestrator.services.vm_resource_capacity import vm_capacity_snapshot
from shared.vm_creation_issuance import canonical_configuration_digest
from shared.vm_creation_retry import canonical_request_digest
from shared.vm_launcher_profile import predict_launcher
from tests.test_vm_resource_capacity_real_postgres import LaterClock
from tests.test_vm_resource_configuration import whole_launcher_configuration
from tests.test_vm_resource_thread_source_real_postgres import (
    db as _mixed_db,
    thread_schema,  # noqa: F401
    _base_db,  # noqa: F401
    _schema_applied,  # noqa: F401
    pg_dsn,  # noqa: F401
    _thread,
)
from tests.test_vm_resource_whole_store_real_postgres import environment, waiter


db = _mixed_db


async def thread_waiter(db, store, inventory, *, user_id=None):
    owner, thread_id = await _thread(
        db, lane="pinned", status="created", user_id=user_id
    )
    runtime = await db.fetchval(
        "SELECT runtime_generation FROM threads WHERE id=$1", thread_id
    )
    request_id, generation = uuid4(), uuid4()
    config = whole_launcher_configuration()
    config.update(namespace="workers", storage_class="local")
    resource = config["resource_admission"]
    resource.update(
        cluster_id=inventory.cluster_id, policy_digest=inventory.policy_digest
    )
    resource["template_profile"].update(
        storage_class="local",
        guest_vcpus=8,
        guest_memory_bytes=16 * 1024**3,
    )
    resource["launcher_prediction"]["vector"] = predict_launcher(
        store.launcher_profile,
        guest_vcpus=8,
        guest_memory_bytes=16 * 1024**3,
    ).to_six_dict()
    resource["host_mapping"]["vector"] = store.cost.cost(8, "16Gi").to_six_dict()
    request = build_vm_creation_request(
        job_id=str(thread_id),
        entity_type="thread",
        agent_config="worker_base",
        vm_image="pinned:image",
        cpu_cores=8,
        memory="16Gi",
        description="mixed source",
        network_tier="restricted",
        provision_generation=str(generation),
    )
    context = VMProvisioner._fresh_provision_ctx()
    context.update(status="provisioning", provision_generation=str(generation))
    assert await db.begin_pinned_thread_vm_provisioning(
        str(thread_id),
        expected_runtime_generation=str(runtime),
        expected_agent_id=None,
        expected_attach_token=None,
        expected_vm_context=None,
        provision_context=context,
        creation_source={
            "request_id": str(request_id),
            "request": request,
            "request_digest": canonical_request_digest(request),
            "controller_configuration": config,
            "controller_configuration_digest": canonical_configuration_digest(config),
        },
    )
    source = await db.fetchrow(
        "SELECT * FROM vm_creation_retries WHERE request_id=$1", request_id
    )
    assert source["job_id"] is None and source["thread_id"] == thread_id
    assert source["thread_owner_user_id"] == owner
    return source


@pytest.mark.asyncio
@pytest.mark.parametrize("same_owner", [False, True])
@pytest.mark.parametrize("thread_first", [False, True])
async def test_mixed_sources_cannot_double_spend_last_budget(
    db, same_owner, thread_first
):
    store, inventory, _, demand = await environment(
        db,
        installation_count=2 if same_owner else 1,
        owner_count=1,
    )
    job = await waiter(db, store, inventory, user_id=uuid4())
    owner = await db.fetchval("SELECT user_id FROM jobs WHERE id=$1", job["job_id"])
    thread = await thread_waiter(
        db, store, inventory, user_id=owner if same_owner else None
    )
    requests = [thread, job] if thread_first else [job, thread]
    results = await asyncio.gather(
        *(store.admit(request_id=str(row["request_id"])) for row in requests)
    )
    assert sum(result["action"] == "admitted" for result in results) == 1
    winner_index = next(
        i for i, result in enumerate(results) if result["action"] == "admitted"
    )
    winner, loser = requests[winner_index], requests[1 - winner_index]
    losing_result = results[1 - winner_index]
    assert losing_result["action"] in {"wait", "nominate"}
    if losing_result["action"] == "nominate":
        assert losing_result["request_id"] == str(winner["request_id"])
    assert (
        await store.admit(request_id=str(winner["request_id"])) == results[winner_index]
    )
    assert await store.admit(request_id=str(loser["request_id"])) == {
        "action": "wait",
        "reason": "owner_budget" if same_owner else "installation_budget",
    }
    held = await db.fetch(
        "SELECT r.request_id,w.owner_kind,w.job_id,w.thread_id FROM vm_resource_reservations r "
        "JOIN vm_resource_waiters w USING(request_id) WHERE r.cluster_id=$1 AND r.state<>'released'",
        inventory.cluster_id,
    )
    assert len(held) == 1 and held[0]["request_id"] == winner["request_id"]
    assert (held[0]["job_id"] is None) == (held[0]["owner_kind"] == "thread")
    capacity = await vm_capacity_snapshot(db)
    cluster = next(
        c for c in capacity["clusters"] if c["cluster_id"] == inventory.cluster_id
    )
    assert cluster["available"] is True
    assert cluster["held"]["unbound"] == demand.to_six_dict()
    assert cluster["waiting"]["count"] == 1


@pytest.mark.asyncio
async def test_mixed_owner_charges_share_admin_snapshot_and_survive_stale_inventory(db):
    store, inventory, _, demand = await environment(db, installation_count=2)
    job = await waiter(db, store, inventory, user_id=uuid4())
    thread = await thread_waiter(db, store, inventory)
    requests = [job, thread]
    await asyncio.gather(
        *(store.admit(request_id=str(row["request_id"])) for row in requests)
    )
    # Fair selection may defer a concurrent caller; exact retry converges once.
    for row in requests:
        assert (await store.admit(request_id=str(row["request_id"])))[
            "action"
        ] == "admitted"
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_resource_reservations WHERE cluster_id=$1",
            inventory.cluster_id,
        )
        == 2
    )
    expected = (demand + demand).to_six_dict()
    for database, fresh in ((db, True), (LaterClock(db), False)):
        capacity = await vm_capacity_snapshot(database)
        cluster = next(
            c for c in capacity["clusters"] if c["cluster_id"] == inventory.cluster_id
        )
        assert cluster["available"] is fresh
        assert cluster["held"]["unbound"] == cluster["held"]["total"] == expected
        assert cluster["waiting"]["count"] == 0
        if fresh:
            assert cluster["totals"]["unbound"] == expected
            assert cluster["totals"]["external"] == dict.fromkeys(expected, 0)
        else:
            assert cluster["reason"] == "inventory_stale"
            assert cluster["totals"] is None
