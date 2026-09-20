import asyncio
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest

from shared.vm_resource_inventory import (
    INVENTORY_KINDS,
    InventoryError,
    canonical_snapshot,
    snapshot_digest,
)
from orchestrator.services.vm_resource_inventory_store import VMResourceInventoryStore
from tests.test_vm_resource_inventory_contract import snapshot
from tests.test_vm_creation_retry_real_postgres import (
    db as _db_fixture,
    postgres_db_fixture,
    pg_dsn,
    _schema_applied,  # noqa: F401
)

db = _db_fixture


def setup(db, *, history_limit=3, stale_after_seconds=60):
    value = snapshot()
    value["cluster_id"] = "test-" + str(uuid4())
    now = datetime.now(timezone.utc) - timedelta(seconds=5)
    value["started_at"] = value["finished_at"] = now.isoformat()
    store = VMResourceInventoryStore(
        db,
        cluster_id=value["cluster_id"],
        namespace=value["namespace"],
        policy_digest=value["policy_digest"],
        label_keys=value["label_keys"],
        max_items=100,
        max_bytes=100000,
        stale_after_seconds=stale_after_seconds,
        history_limit=history_limit,
    )
    return store, value


async def publish(store, value):
    value = canonical_snapshot(value, max_items=100, max_bytes=100000)
    return await store.publish(snapshot=value, digest=snapshot_digest(value))


def successor(value, *, complete=True):
    value = deepcopy(value)
    value["snapshot_id"] = str(uuid4())
    value["sequence"] += 1
    newer = datetime.fromisoformat(value["started_at"]) + timedelta(milliseconds=1)
    value["started_at"] = value["finished_at"] = newer.isoformat()
    if not complete:
        value.update(complete=False, reason="collection_failed", resource_versions={})
        value.update({kind: [] for kind in INVENTORY_KINDS})
    return value


@pytest.mark.asyncio
async def test_newer_incomplete_invalidates_complete_and_replay_never_refreshes(db):
    store, value = setup(db)
    first = await publish(store, value)
    assert (await store.current())["available"]
    failed = successor(value, complete=False)
    await publish(store, failed)
    current = await store.current()
    assert not current["available"] and current["reason"] == "inventory_incomplete"
    assert current["snapshot"]["snapshot_id"] == failed["snapshot_id"]
    replay = await publish(store, value)
    assert replay["received_at"] == first["received_at"] and replay["current"] is False
    assert (await store.current())["snapshot"]["snapshot_id"] == failed["snapshot_id"]


@pytest.mark.asyncio
async def test_concurrent_publish_cannot_move_current_backward(db):
    store, old = setup(db)
    new = successor(old, complete=False)
    results = await asyncio.gather(
        publish(store, old), publish(store, new), return_exceptions=True
    )
    assert any(isinstance(result, dict) for result in results)
    current = await store.current()
    assert current["snapshot"]["snapshot_id"] == new["snapshot_id"]
    assert not current["available"]


@pytest.mark.asyncio
async def test_equal_time_conflict_invalidates_without_uuid_tiebreak_or_replay_repair(
    db,
):
    store, first = setup(db)
    await publish(store, first)
    conflicting = successor(first, complete=False)
    conflicting["started_at"] = conflicting["finished_at"] = first["started_at"]
    with pytest.raises(InventoryError, match="observation_conflict"):
        await publish(store, conflicting)
    assert (await store.current())["reason"] == "inventory_observation_conflict"
    await publish(store, first)
    assert not (await store.current())["available"]
    await publish(store, successor(first))
    assert (await store.current())["available"]


@pytest.mark.asyncio
async def test_new_receipt_does_not_make_old_collection_fresh(db):
    store, value = setup(db)
    value["started_at"] = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
    await publish(store, value)
    assert (await store.current())["reason"] == "inventory_stale"


@pytest.mark.asyncio
async def test_lock_wait_samples_database_time_before_freshness(db):
    store, first = setup(db, stale_after_seconds=1)
    await publish(store, first)
    value = successor(first)
    value["started_at"] = value["finished_at"] = datetime.now(timezone.utc).isoformat()
    async with db.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                "SELECT 1 FROM vm_resource_inventory_heads WHERE cluster_id=$1 FOR UPDATE",
                first["cluster_id"],
            )
            task = asyncio.create_task(publish(store, value))
            await asyncio.sleep(1.1)
            assert not task.done()
    await task
    assert (await store.current())["reason"] == "inventory_stale"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [
        ("cluster_id", "other"),
        ("namespace", "other"),
        ("policy_digest", "sha256:" + "b" * 64),
        ("label_keys", ["zone"]),
    ],
)
async def test_wrong_configured_scope_is_not_inventory_authority(db, field, value):
    store, original = setup(db)
    await publish(store, original)
    invalid = successor(original)
    invalid[field] = value
    with pytest.raises(InventoryError):
        await publish(store, invalid)
    assert (await store.current())["snapshot"]["snapshot_id"] == original["snapshot_id"]


@pytest.mark.asyncio
async def test_id_reuse_digest_mismatch_and_future_clock_are_refused(db):
    store, first = setup(db)
    await publish(store, first)
    changed = deepcopy(first)
    changed["sequence"] += 1
    with pytest.raises(InventoryError, match="snapshot_conflict"):
        await publish(store, changed)
    with pytest.raises(InventoryError, match="digest"):
        await store.publish(snapshot=first, digest="sha256:" + "b" * 64)
    future = successor(first)
    future["started_at"] = future["finished_at"] = (
        datetime.now(timezone.utc) + timedelta(days=1)
    ).isoformat()
    with pytest.raises(InventoryError, match="future"):
        await publish(store, future)


@pytest.mark.asyncio
async def test_pruned_snapshot_cannot_reset_high_water_or_reappear_as_fresh(db):
    store, first = setup(db, history_limit=2)
    value = first
    for _ in range(4):
        await publish(store, value)
        previous, value = value, successor(value)
    count = await db.fetchval(
        "SELECT count(*) FROM vm_resource_inventory_snapshots WHERE cluster_id=$1",
        first["cluster_id"],
    )
    assert count == 2
    with pytest.raises(InventoryError, match="observation_old"):
        await publish(store, first)
    assert (await store.current())["snapshot"]["snapshot_id"] == previous["snapshot_id"]
