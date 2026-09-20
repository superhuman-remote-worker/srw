"""Exact resource-policy installation and drain transitions on PostgreSQL."""

import asyncio
from dataclasses import replace
import json
from uuid import uuid4

import pytest
import pytest_asyncio

from shared.vm_resource_admission import ResourceAdmissionError
from tests.test_vm_creation_retry_real_postgres import (
    _schema_applied,  # noqa: F401
    pg_dsn,  # noqa: F401
    postgres_db_fixture as _postgres_db_fixture,
)
from tests.test_vm_resource_policy import enforcement_policy

postgres_db_fixture = _postgres_db_fixture


@pytest_asyncio.fixture
async def db(postgres_db_fixture):
    await postgres_db_fixture.execute(
        "DELETE FROM vm_resource_admission_policy WHERE cluster_id LIKE 'd3i-%'"
    )
    yield postgres_db_fixture
    await postgres_db_fixture.execute(
        "DELETE FROM vm_resource_admission_policy WHERE cluster_id LIKE 'd3i-%'"
    )


def snapshot(*, cluster_id=None, namespace="workers"):
    from shared.vm_resource_policy import validate_enforcement_resource_policy

    document = enforcement_policy()
    document["namespace"] = namespace
    document["policy"]["stableClusterId"] = cluster_id or f"d3i-{uuid4()}"
    return validate_enforcement_resource_policy(document)


def store(db, policy):
    from orchestrator.services.vm_resource_policy_lifecycle_store import (
        VMResourcePolicyLifecycleStore,
    )

    return VMResourcePolicyLifecycleStore(db, snapshot=policy)


async def wait_for_policy_waiters(db, count):
    for _ in range(250):
        waiting = await db.fetchval(
            "SELECT count(*) FROM pg_stat_activity "
            "WHERE wait_event_type='Lock' "
            "AND query LIKE 'SELECT * FROM vm_resource_admission_policy%FOR UPDATE%'"
        )
        if waiting >= count:
            return
        await asyncio.sleep(0.02)
    pytest.fail(f"only {waiting} lifecycle calls reached the policy lock")


@pytest.mark.asyncio
async def test_concurrent_exact_shadow_install_has_one_row_and_one_receipt(db):
    policy = snapshot()
    lifecycle = store(db, policy)

    first, second = await asyncio.gather(
        lifecycle.ensure_shadow(), lifecycle.ensure_shadow()
    )

    assert first == second
    assert first.cluster_id == policy.inventory.cluster_id
    assert first.namespace == "workers"
    assert first.policy_digest == policy.policy_digest
    assert first.revision == 1
    assert first.mode == "shadow"
    row = await db.fetchrow(
        "SELECT *,document::text AS document_text FROM vm_resource_admission_policy WHERE cluster_id=$1",
        policy.inventory.cluster_id,
    )
    assert row["namespace"] == "workers"
    assert row["policy_digest"] == policy.policy_digest
    assert json.loads(row["document_text"]) == json.loads(policy.canonical_document)
    assert row["revision"] == 1
    assert row["mode"] == "shadow"
    assert await db.fetchval(
        "SELECT count(*) FROM vm_resource_admission_policy WHERE cluster_id=$1",
        policy.inventory.cluster_id,
    ) == 1


@pytest.mark.asyncio
async def test_competing_shadow_documents_install_one_without_replacement(db):
    cluster_id = f"d3i-{uuid4()}"
    first = snapshot(cluster_id=cluster_id)
    document = enforcement_policy()
    document["policy"]["stableClusterId"] = cluster_id
    document["policy"]["fairness"]["maxBypasses"] = 3
    from shared.vm_resource_policy import validate_enforcement_resource_policy

    second = validate_enforcement_resource_policy(document)
    results = await asyncio.gather(
        store(db, first).ensure_shadow(),
        store(db, second).ensure_shadow(),
        return_exceptions=True,
    )

    receipts = [item for item in results if not isinstance(item, Exception)]
    errors = [item for item in results if isinstance(item, Exception)]
    assert len(receipts) == 1
    assert len(errors) == 1
    assert isinstance(errors[0], ResourceAdmissionError)
    assert str(errors[0]) == "resource_policy_changed"
    row = await db.fetchrow(
        "SELECT policy_digest,document::text AS document_text,revision,mode "
        "FROM vm_resource_admission_policy WHERE cluster_id=$1",
        cluster_id,
    )
    assert row["policy_digest"] == receipts[0].policy_digest
    winning = first if first.policy_digest == receipts[0].policy_digest else second
    assert json.loads(row["document_text"]) == json.loads(winning.canonical_document)
    assert (row["revision"], row["mode"]) == (1, "shadow")


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["document", "namespace", "mode"])
async def test_shadow_install_never_replaces_an_existing_row(db, change):
    policy = snapshot()
    receipt = await store(db, policy).ensure_shadow()
    other = snapshot(cluster_id=policy.inventory.cluster_id)
    if change == "document":
        from shared.vm_resource_policy import validate_enforcement_resource_policy

        document = enforcement_policy()
        document["policy"]["stableClusterId"] = policy.inventory.cluster_id
        document["policy"]["fairness"]["maxBypasses"] = 3
        other = validate_enforcement_resource_policy(document)
    elif change == "namespace":
        other = snapshot(cluster_id=policy.inventory.cluster_id, namespace="other")
    elif change == "mode":
        await db.execute(
            "UPDATE vm_resource_admission_policy SET mode='off' WHERE cluster_id=$1",
            policy.inventory.cluster_id,
        )

    with pytest.raises(ResourceAdmissionError, match="resource_policy_changed"):
        await store(db, other).ensure_shadow()

    row = await db.fetchrow(
        "SELECT namespace,policy_digest,revision,mode FROM vm_resource_admission_policy WHERE cluster_id=$1",
        policy.inventory.cluster_id,
    )
    assert row["namespace"] == receipt.namespace
    assert row["policy_digest"] == receipt.policy_digest
    assert row["revision"] == receipt.revision
    assert row["mode"] == ("off" if change == "mode" else "shadow")


@pytest.mark.asyncio
async def test_begin_drain_preserves_sequence_and_cursor_and_requires_new_receipt(db):
    policy = snapshot()
    lifecycle = store(db, policy)
    shadow = await lifecycle.ensure_shadow()
    maintenance_id = uuid4()
    await db.execute(
        "UPDATE vm_resource_admission_policy SET admission_sequence=7,maintenance_enqueued_at=clock_timestamp(),maintenance_request_id=$2 WHERE cluster_id=$1",
        policy.inventory.cluster_id,
        maintenance_id,
    )

    drained = await lifecycle.begin_drain(expected=shadow)
    assert drained == replace(shadow, revision=2, mode="drain")
    assert await lifecycle.begin_drain(expected=drained) == drained
    with pytest.raises(ResourceAdmissionError, match="resource_policy_changed"):
        await lifecycle.begin_drain(expected=shadow)
    with pytest.raises(ResourceAdmissionError, match="resource_policy_changed"):
        await lifecycle.ensure_shadow()

    row = await db.fetchrow(
        "SELECT revision,mode,admission_sequence,maintenance_request_id FROM vm_resource_admission_policy WHERE cluster_id=$1",
        policy.inventory.cluster_id,
    )
    assert dict(row) == {
        "revision": 2,
        "mode": "drain",
        "admission_sequence": 7,
        "maintenance_request_id": maintenance_id,
    }


@pytest.mark.asyncio
async def test_begin_drain_accepts_exact_preexisting_enforce_receipt(db):
    from orchestrator.services.vm_resource_policy_lifecycle_store import (
        ResourcePolicyReceipt,
    )
    from orchestrator.services.vm_resource_reservation_store import (
        VMResourceReservationStore,
    )
    from orchestrator.services.vm_resource_inventory_store import (
        VMResourceInventoryStore,
    )

    policy = snapshot()
    lifecycle = store(db, policy)
    shadow = await lifecycle.ensure_shadow()
    await db.execute(
        "UPDATE vm_resource_admission_policy SET mode='enforce',revision=2 WHERE cluster_id=$1",
        policy.inventory.cluster_id,
    )
    enforce = ResourcePolicyReceipt(
        shadow.cluster_id,
        shadow.namespace,
        shadow.policy_digest,
        2,
        "enforce",
    )

    drained = await lifecycle.begin_drain(expected=enforce)
    assert drained == replace(
        enforce, revision=3, mode="drain"
    )
    settings = policy.inventory
    inventory = VMResourceInventoryStore(
        db,
        cluster_id=settings.cluster_id,
        namespace=settings.namespace,
        policy_digest=settings.policy_digest,
        label_keys=settings.label_keys,
        max_items=settings.max_items,
        max_bytes=settings.max_bytes,
        stale_after_seconds=settings.stale_after_seconds,
        history_limit=settings.history_limit,
    )
    reservations = VMResourceReservationStore(
        db,
        inventory=inventory,
        policy_document=json.loads(policy.canonical_document),
        policy_revision=drained.revision,
    )
    async with db.acquire() as conn, conn.transaction():
        with pytest.raises(ResourceAdmissionError, match="resource_policy_changed"):
            await reservations._lock_policy(conn)


@pytest.mark.asyncio
async def test_begin_drain_refuses_absent_off_and_fabricated_receipts(db):
    from orchestrator.services.vm_resource_policy_lifecycle_store import (
        ResourcePolicyReceipt,
    )

    policy = snapshot()
    lifecycle = store(db, policy)
    absent = ResourcePolicyReceipt(
        policy.inventory.cluster_id,
        policy.inventory.namespace,
        policy.policy_digest,
        1,
        "shadow",
    )
    with pytest.raises(ResourceAdmissionError, match="resource_policy_changed"):
        await lifecycle.begin_drain(expected=absent)

    shadow = await lifecycle.ensure_shadow()
    for invalid in (
        replace(shadow, cluster_id="d3i-other"),
        replace(shadow, namespace="other"),
        replace(shadow, policy_digest="sha256:" + "a" * 64),
        replace(shadow, revision=True),
        {},
    ):
        with pytest.raises(ResourceAdmissionError, match="resource_policy_changed"):
            await lifecycle.begin_drain(expected=invalid)

    await db.execute(
        "UPDATE vm_resource_admission_policy SET mode='off' WHERE cluster_id=$1",
        policy.inventory.cluster_id,
    )
    with pytest.raises(ResourceAdmissionError, match="resource_policy_changed"):
        await lifecycle.begin_drain(expected=replace(shadow, mode="off"))


@pytest.mark.asyncio
async def test_concurrent_begin_drain_increments_once_and_rejects_stale_caller(db):
    policy = snapshot()
    lifecycle = store(db, policy)
    shadow = await lifecycle.ensure_shadow()
    results = await asyncio.gather(
        lifecycle.begin_drain(expected=shadow),
        lifecycle.begin_drain(expected=shadow),
        return_exceptions=True,
    )

    receipts = [item for item in results if not isinstance(item, Exception)]
    errors = [item for item in results if isinstance(item, Exception)]
    assert receipts == [replace(shadow, revision=2, mode="drain")]
    assert len(errors) == 1
    assert isinstance(errors[0], ResourceAdmissionError)
    assert str(errors[0]) == "resource_policy_changed"
    assert await db.fetchval(
        "SELECT revision FROM vm_resource_admission_policy WHERE cluster_id=$1",
        policy.inventory.cluster_id,
    ) == 2


@pytest.mark.asyncio
async def test_shadow_replay_racing_begin_drain_never_reactivates_shadow(db):
    policy = snapshot()
    lifecycle = store(db, policy)
    shadow = await lifecycle.ensure_shadow()

    replay, drained = await asyncio.gather(
        lifecycle.ensure_shadow(),
        lifecycle.begin_drain(expected=shadow),
        return_exceptions=True,
    )

    assert drained == replace(shadow, revision=2, mode="drain")
    assert replay == shadow or (
        isinstance(replay, ResourceAdmissionError)
        and str(replay) == "resource_policy_changed"
    )
    assert dict(
        await db.fetchrow(
            "SELECT revision,mode FROM vm_resource_admission_policy WHERE cluster_id=$1",
            policy.inventory.cluster_id,
        )
    ) == {"revision": 2, "mode": "drain"}


@pytest.mark.asyncio
async def test_shadow_replay_and_drain_refuse_state_changed_during_policy_wait(db):
    policy = snapshot()
    lifecycle = store(db, policy)
    shadow = await lifecycle.ensure_shadow()
    maintenance_id = uuid4()
    await db.execute(
        "UPDATE vm_resource_admission_policy SET admission_sequence=11,"
        "maintenance_enqueued_at=clock_timestamp(),maintenance_request_id=$2 "
        "WHERE cluster_id=$1",
        policy.inventory.cluster_id,
        maintenance_id,
    )

    async with db.acquire() as blocker, blocker.transaction():
        await blocker.fetchrow(
            "SELECT * FROM vm_resource_admission_policy WHERE cluster_id=$1 FOR UPDATE",
            policy.inventory.cluster_id,
        )
        replay = asyncio.create_task(lifecycle.ensure_shadow())
        drain = asyncio.create_task(lifecycle.begin_drain(expected=shadow))
        await wait_for_policy_waiters(db, 2)
        await blocker.execute(
            "UPDATE vm_resource_admission_policy SET mode='drain',revision=revision+1 "
            "WHERE cluster_id=$1",
            policy.inventory.cluster_id,
        )

    results = await asyncio.wait_for(
        asyncio.gather(replay, drain, return_exceptions=True), 5
    )
    assert len(results) == 2
    assert all(isinstance(item, ResourceAdmissionError) for item in results)
    assert [str(item) for item in results] == [
        "resource_policy_changed",
        "resource_policy_changed",
    ]
    row = await db.fetchrow(
        "SELECT revision,mode,admission_sequence,maintenance_request_id "
        "FROM vm_resource_admission_policy WHERE cluster_id=$1",
        policy.inventory.cluster_id,
    )
    assert dict(row) == {
        "revision": 2,
        "mode": "drain",
        "admission_sequence": 11,
        "maintenance_request_id": maintenance_id,
    }
