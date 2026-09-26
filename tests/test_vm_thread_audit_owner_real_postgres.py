"""Permanent failed-initial VM End preserves immutable source audit identity."""

from pathlib import Path
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4
import asyncio

import asyncpg
import pytest
import pytest_asyncio

from tests.test_pinned_vm_failed_initial_end_real_postgres import (
    _authorize,
    _base_db,  # noqa: F401
    _begin,
    _current_zero_arguments,
    _failed_source,
    _partial_after_aborts,
    _wait_for_owner_waiters,
    _schema_applied,  # noqa: F401
    cleanup_lineage_schema,  # noqa: F401
    pg_dsn,  # noqa: F401
    thread_schema,  # noqa: F401
    setup,  # noqa: F401
)
from orchestrator.services.vm_creation_retry_store import (
    VMCreationRetryConflict,
    VMCreationRetryStore,
)


@pytest_asyncio.fixture(scope="module")
async def audit_schema(pg_dsn, cleanup_lineage_schema):  # noqa: F811
    conn = await asyncpg.connect(pg_dsn)
    try:
        if not await conn.fetchval(
            "SELECT to_regclass('public.vm_thread_creation_owners')"
        ):
            migrations = (
                Path(__file__).resolve().parents[1]
                / "src/orchestrator/database/migrations/app"
            )
            for name in (
                "0289_vm_thread_creation_audit_owners.sql",
                "0290_validate_vm_thread_creation_audit_owners.sql",
            ):
                await conn.execute((migrations / name).read_text())
    finally:
        await conn.close()


@pytest_asyncio.fixture
async def db(audit_schema, _base_db):  # noqa: F811
    yield _base_db


async def _ready_to_delete(db, monkeypatch, *, soft_first, source_order="bound"):
    current, original = await _failed_source(db, monkeypatch, source_order, 2)
    retirement = await _begin(db, current, permanent=not soft_first)
    assert retirement["state"] == "pending", retirement
    await _authorize(db, current, retirement)
    assert (
        await VMCreationRetryStore(db).settle_never_issued(
            request_id=str(original["request_id"])
        )
    )["settled"]
    arguments = await _current_zero_arguments(db, current, retirement)
    assert await db.acknowledge_pinned_thread_local_quiescence(
        str(current["id"]), **arguments
    )
    agent = await db.fetchrow("SELECT * FROM agents WHERE id=$1", current["agent_id"])
    if soft_first:
        assert await db.settle_pinned_thread_retirement(
            str(current["id"]),
            token=retirement["token"],
            generation=retirement["generation"],
            final_status="ended",
        )
        # Model only the exact stopped-Pod/finalizer receipt boundary; complete
        # the real warm-release ledger before admitting permanent End.
        assert await db.complete_pinned_warm_binding_release(
            retirement["context"]["agent_pod"]["warm_binding_protection"],
            release_outcome="exact_absent_v1",
            agent_present=False,
        )
        current = await db.get_thread(str(current["id"]))
        assert current["status"] == "ended"
        assert current["runtime_retirement_context"] is None
        assert current["runtime_retirement_local_quiescence"] is None
        prior = retirement
        retirement = await _begin(db, current, permanent=True)
        assert retirement["state"] == "pending", retirement
        assert retirement["token"] != prior["token"]
        assert retirement["generation"] == prior["generation"]
        assert retirement["context"].get("vm_creation_source") is None
        await _authorize(db, current, retirement)
        assert await db.clear_pinned_retirement_physical_runtime_endpoint(
            str(current["id"]),
            runtime_generation=retirement["generation"],
            retirement_token=retirement["token"],
        )
    else:
        assert await db.clear_pinned_retirement_physical_runtime_endpoint(
            str(current["id"]),
            runtime_generation=retirement["generation"],
            retirement_token=retirement["token"],
            completed_quiescence_protocol="agent_runtime_zero_v1",
            expected_stopped_agent_pod_name=agent["hostname"],
            expected_stopped_agent_pod_uid=agent["pod_uid"],
        )
    assert await db.pinned_retirement_external_cleanup_complete(
        str(current["id"]),
        runtime_generation=retirement["generation"],
        retirement_token=retirement["token"],
    )
    return current, original, retirement


@pytest.mark.asyncio
@pytest.mark.parametrize("soft_first", [False, True])
@pytest.mark.parametrize("source_order", ["prebind", "bound"])
async def test_permanent_failed_initial_end_keeps_source_and_charge_history(
    db,
    monkeypatch,
    soft_first,
    source_order,
):
    current, original, retirement = await _ready_to_delete(
        db,
        monkeypatch,
        soft_first=soft_first,
        source_order=source_order,
    )
    source = await db.fetchrow(
        "SELECT * FROM vm_creation_retries WHERE request_id=$1",
        original["request_id"],
    )
    waiter = await db.fetchrow(
        "SELECT * FROM vm_resource_waiters WHERE request_id=$1",
        original["request_id"],
    )
    charges = await db.fetch(
        "SELECT * FROM vm_resource_reservations WHERE request_id=$1 ORDER BY id",
        original["request_id"],
    )
    assert source["state"] == "settled" and waiter["state"] == "released"
    assert charges and all(row["state"] == "released" for row in charges)
    await db.delete_thread(
        str(current["id"]),
        expected_runtime_generation=retirement["generation"],
        expected_runtime_retirement_token=retirement["token"],
    )
    assert await db.get_thread(str(current["id"])) is None
    assert (
        await db.fetchrow(
            "SELECT * FROM vm_creation_retries WHERE request_id=$1",
            original["request_id"],
        )
        == source
    )
    assert (
        await db.fetchrow(
            "SELECT * FROM vm_resource_waiters WHERE request_id=$1",
            original["request_id"],
        )
        == waiter
    )
    assert (
        await db.fetch(
            "SELECT * FROM vm_resource_reservations WHERE request_id=$1 ORDER BY id",
            original["request_id"],
        )
        == charges
    )
    assert (
        await db.fetchval(
            "SELECT count(*) FROM thread_runtime_retirement_outcomes "
            "WHERE thread_id=$1 AND runtime_generation=$2 AND retirement_token=$3 "
            "AND outcome='deleted' AND permanent",
            current["id"],
            current["runtime_generation"],
            retirement["token"],
        )
        == 1
    )
    owner = await db.fetchrow(
        "SELECT * FROM vm_thread_creation_owners WHERE thread_id=$1", current["id"]
    )
    assert owner["live_thread_id"] is None
    assert str(owner["deleted_retirement_token"]) == retirement["token"]
    assert owner["deleted_runtime_generation"] == current["runtime_generation"]
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_thread_creation_settlements WHERE request_id=$1",
            original["request_id"],
        )
        == 1
    )
    # Lost DELETE reply and controller restart cannot recreate the owner or a
    # charge. No original source/carrier identity changes for replay.
    await db.delete_thread(
        str(current["id"]),
        expected_runtime_generation=retirement["generation"],
        expected_runtime_retirement_token=retirement["token"],
    )
    assert await VMCreationRetryStore(db).claim_due(limit=1) == []
    async with db.acquire() as conn, conn.transaction():
        with pytest.raises(VMCreationRetryConflict):
            await VMCreationRetryStore(db)._thread_scope(conn, source)


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ["clear", "relink", "delete", "fake_tombstone"])
async def test_live_audit_owner_cannot_be_manually_retired(db, monkeypatch, mutation):
    current, original, retirement = await _ready_to_delete(
        db, monkeypatch, soft_first=True
    )
    with pytest.raises(asyncpg.CheckViolationError):
        if mutation == "clear":
            await db.execute(
                "UPDATE vm_thread_creation_owners SET live_thread_id=NULL WHERE thread_id=$1",
                current["id"],
            )
        elif mutation == "relink":
            await db.execute(
                "UPDATE vm_thread_creation_owners SET live_thread_id=$2 WHERE thread_id=$1",
                current["id"],
                uuid4(),
            )
        elif mutation == "delete":
            await db.execute(
                "DELETE FROM vm_thread_creation_owners WHERE thread_id=$1",
                current["id"],
            )
        else:
            await db.execute(
                "UPDATE vm_thread_creation_owners SET deleted_at=now(),deleted_runtime_generation=$2,deleted_retirement_token=$3,deletion_receipt='{}' WHERE thread_id=$1",
                current["id"],
                current["runtime_generation"],
                retirement["token"],
            )
    assert await db.get_thread(str(current["id"])) is not None
    owner = await db.fetchrow(
        "SELECT * FROM vm_thread_creation_owners WHERE thread_id=$1", current["id"]
    )
    assert owner["live_thread_id"] == current["id"] and owner["deleted_at"] is None


@pytest.mark.asyncio
async def test_owner_tombstone_cannot_commit_without_actual_thread_delete(
    db, monkeypatch
):
    current, original, retirement = await _ready_to_delete(
        db, monkeypatch, soft_first=True
    )
    # Simulate a writer which has the exact normal End authority and publishes
    # the real deleted outcome, but omits DELETE. Deferred validation must
    # roll the entire transaction back, not trust trigger nesting depth.
    with pytest.raises(asyncpg.CheckViolationError, match="must commit together"):
        async with db.acquire() as conn, conn.transaction():
            await conn.execute(
                "SELECT id FROM threads WHERE id=$1 FOR UPDATE", current["id"]
            )
            await conn.execute(
                "INSERT INTO thread_runtime_retirement_outcomes "
                "(thread_id,runtime_generation,retirement_token,disposition,permanent,outcome) "
                "VALUES($1,$2,$3,'ended',true,'deleted')",
                current["id"],
                current["runtime_generation"],
                retirement["token"],
            )
            await conn.execute(
                "UPDATE vm_thread_creation_owners o SET deleted_at=now(),"
                "deleted_runtime_generation=t.runtime_generation,deleted_retirement_token=t.runtime_retirement_token,"
                "deletion_receipt=public.vm_thread_creation_delete_evidence(t) FROM threads t "
                "WHERE t.id=$1 AND o.thread_id=t.id",
                current["id"],
            )
    assert await db.get_thread(str(current["id"])) is not None
    assert (
        await db.fetchval(
            "SELECT deleted_at FROM vm_thread_creation_owners WHERE thread_id=$1",
            current["id"],
        )
        is None
    )
    assert (
        await db.fetchval(
            "SELECT count(*) FROM thread_runtime_retirement_outcomes WHERE thread_id=$1 AND outcome='deleted'",
            current["id"],
        )
        == 0
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation",
    ["missing_receipt", "wrong_generation", "changed_packet", "charged_again"],
)
async def test_historical_source_requires_unchanged_positive_settlement(
    db, monkeypatch, mutation
):
    current, original, retirement = await _ready_to_delete(
        db, monkeypatch, soft_first=True
    )
    # Explicit corruption/missing-history fixture. No production writer can
    # rewrite the receipt or resurrect the released reservation.
    async with db.acquire() as conn, conn.transaction():
        await conn.execute("SET LOCAL session_replication_role='replica'")
        if mutation == "missing_receipt":
            await conn.execute(
                "DELETE FROM vm_thread_creation_settlements WHERE request_id=$1",
                original["request_id"],
            )
        elif mutation == "wrong_generation":
            await conn.execute(
                "UPDATE vm_thread_creation_settlements SET runtime_generation=$2 WHERE request_id=$1",
                original["request_id"],
                uuid4(),
            )
        elif mutation == "changed_packet":
            await conn.execute(
                "UPDATE vm_thread_creation_settlements SET terminal_evidence='{}' WHERE request_id=$1",
                original["request_id"],
            )
        else:
            await conn.execute(
                "UPDATE vm_resource_reservations SET state='reserved',released_at=NULL,release_evidence=NULL WHERE request_id=$1",
                original["request_id"],
            )
    with pytest.raises(
        asyncpg.CheckViolationError,
        match="lacks settled failed-initial source evidence",
    ):
        await db.delete_thread(
            str(current["id"]),
            expected_runtime_generation=retirement["generation"],
            expected_runtime_retirement_token=retirement["token"],
        )
    assert await db.get_thread(str(current["id"])) is not None
    assert (
        await db.fetchval(
            "SELECT live_thread_id FROM vm_thread_creation_owners WHERE thread_id=$1",
            current["id"],
        )
        == current["id"]
    )


@pytest.mark.asyncio
async def test_settlement_and_deleted_owner_are_append_only(db, monkeypatch):
    current, original, retirement = await _ready_to_delete(
        db, monkeypatch, soft_first=True
    )
    for statement in (
        "UPDATE vm_thread_creation_settlements SET terminal_evidence='{}' WHERE request_id=$1",
        "DELETE FROM vm_thread_creation_settlements WHERE request_id=$1",
    ):
        with pytest.raises(asyncpg.CheckViolationError, match="append-only"):
            await db.execute(statement, original["request_id"])
    await db.delete_thread(
        str(current["id"]),
        expected_runtime_generation=retirement["generation"],
        expected_runtime_retirement_token=retirement["token"],
    )
    for statement in (
        "UPDATE vm_thread_creation_owners SET live_thread_id=thread_id WHERE thread_id=$1",
        "UPDATE vm_thread_creation_owners SET deletion_receipt='{}' WHERE thread_id=$1",
        "UPDATE vm_thread_creation_owners SET deleted_at=NULL WHERE thread_id=$1",
        "DELETE FROM vm_thread_creation_owners WHERE thread_id=$1",
    ):
        with pytest.raises(asyncpg.CheckViolationError):
            await db.execute(statement, current["id"])


@pytest.mark.asyncio
async def test_migration_backfills_live_identity_without_inventing_old_settlement(
    pg_dsn,  # noqa: F811
    tmp_path,
    monkeypatch,
):
    from orchestrator.database.migrate import discover, run_migrations
    from orchestrator.database.postgres import PostgresDB

    name = "vm_audit_upgrade_" + uuid4().hex[:12]
    admin = await asyncpg.connect(pg_dsn)
    await admin.execute(f'CREATE DATABASE "{name}"')
    parts = urlsplit(pg_dsn)
    dsn = urlunsplit(
        (parts.scheme, parts.netloc, "/" + name, parts.query, parts.fragment)
    )
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=3)
    store = None
    migrations = (
        Path(__file__).resolve().parents[1] / "src/orchestrator/database/migrations/app"
    )
    previous = tmp_path / "through_0288"
    previous.mkdir()
    try:
        for path in discover(migrations):
            if path.name <= "0288_vm_initial_creation_cleanup_lineage.sql":
                (previous / path.name).write_bytes(path.read_bytes())
        await run_migrations(pool, previous)
        store = PostgresDB(connection_string=dsn, min_connections=1, max_connections=4)
        await store.connect()
        current, original, retirement = await _ready_to_delete(
            store, monkeypatch, soft_first=True
        )
        source = await store.fetchrow(
            "SELECT * FROM vm_creation_retries WHERE request_id=$1",
            original["request_id"],
        )
        waiter = await store.fetchrow(
            "SELECT * FROM vm_resource_waiters WHERE request_id=$1",
            original["request_id"],
        )
        assert (
            await store.fetchval(
                "SELECT to_regclass('public.vm_thread_creation_owners')"
            )
            is None
        )
        await run_migrations(pool, migrations)
        assert (
            await store.fetchval(
                "SELECT live_thread_id FROM vm_thread_creation_owners WHERE thread_id=$1",
                current["id"],
            )
            == current["id"]
        )
        assert (
            await store.fetchval("SELECT count(*) FROM vm_thread_creation_settlements")
            == 0
        )
        assert (
            await store.fetchrow(
                "SELECT * FROM vm_creation_retries WHERE request_id=$1",
                original["request_id"],
            )
            == source
        )
        assert (
            await store.fetchrow(
                "SELECT * FROM vm_resource_waiters WHERE request_id=$1",
                original["request_id"],
            )
            == waiter
        )
        assert (
            await store.fetchval(
                "SELECT count(*) FROM pg_constraint WHERE conname IN "
                "('vm_creation_retries_audit_owner_fkey','vm_resource_waiters_audit_owner_fkey') "
                "AND convalidated AND confrelid='public.vm_thread_creation_owners'::regclass"
            )
            == 2
        )
        # An old soft End did not capture source settlement. Its ordinary
        # current permanent authority cannot invent that missing history.
        with pytest.raises(
            asyncpg.CheckViolationError,
            match="lacks settled failed-initial source evidence",
        ):
            await store.delete_thread(
                str(current["id"]),
                expected_runtime_generation=retirement["generation"],
                expected_runtime_retirement_token=retirement["token"],
            )
        assert await store.get_thread(str(current["id"])) is not None
    finally:
        if store is not None:
            await store.close()
        await pool.close()
        await admin.execute(f'DROP DATABASE "{name}"')
        await admin.close()


async def _finish_disposed_source(db, thread_id, *, soft_first):
    current = await db.get_thread(str(thread_id))
    retirement = await _begin(db, current, permanent=not soft_first)
    assert retirement["state"] == "pending", retirement
    agent = None
    if current["agent_id"] is not None:
        arguments = await _current_zero_arguments(db, current, retirement)
        assert await db.acknowledge_pinned_thread_local_quiescence(
            str(thread_id), **arguments
        )
        agent = await db.fetchrow(
            "SELECT * FROM agents WHERE id=$1", current["agent_id"]
        )
    if soft_first:
        assert await db.settle_pinned_thread_retirement(
            str(thread_id),
            token=retirement["token"],
            generation=retirement["generation"],
            final_status="ended",
        )
        if agent:
            assert await db.complete_pinned_warm_binding_release(
                retirement["context"]["agent_pod"]["warm_binding_protection"],
                release_outcome="exact_absent_v1",
                agent_present=False,
            )
        current = await db.get_thread(str(thread_id))
        retirement = await _begin(db, current, permanent=True)
        assert retirement["state"] == "pending", retirement
        await _authorize(db, current, retirement)
        agent = None
    assert await db.clear_pinned_retirement_physical_runtime_endpoint(
        str(thread_id),
        runtime_generation=retirement["generation"],
        retirement_token=retirement["token"],
        **(
            dict(
                completed_quiescence_protocol="agent_runtime_zero_v1",
                expected_stopped_agent_pod_name=agent["hostname"],
                expected_stopped_agent_pod_uid=agent["pod_uid"],
            )
            if agent
            else {}
        ),
    )
    await db.delete_thread(
        str(thread_id),
        expected_runtime_generation=retirement["generation"],
        expected_runtime_retirement_token=retirement["token"],
    )
    assert await db.get_thread(str(thread_id)) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("last_effect", ["rootdisk", "cloud_init"])
@pytest.mark.parametrize("soft_first", [False, True])
async def test_partial_disposition_receipt_survives_permanent_end(
    db,
    setup,  # noqa: F811
    monkeypatch,
    last_effect,
    soft_first,
):
    from tests import test_vm_resource_thread_disposition_real_postgres as helpers
    from shared.vm_creation_disposition import disposition_identity
    from vm_controller.creation_disposition import CreationDisposer

    async def prepare(db, monkeypatch, last_effect):
        return await _partial_after_aborts(
            db, monkeypatch, last_effect, permanent=not soft_first
        )

    monkeypatch.setattr(helpers, "partial_thread", prepare)
    (
        ctrl,
        api,
        store,
        row,
        thread_id,
        admitted,
        _,
        carrier,
    ) = await helpers.controller_runtime(db, setup, monkeypatch, last_effect)
    assert (
        await store.freeze_disposition(request_id=row["request_id"], carrier=carrier)
    )["frozen"]
    assert (
        await db.fetchval(
            "SELECT public.vm_thread_creation_terminal_evidence(r) FROM vm_creation_retries r WHERE request_id=$1",
            row["request_id"],
        )
        is None
    )
    api.lost_deletes.add("DataVolume")
    for _ in range(8):
        result = await CreationDisposer(ctrl).run(disposition_identity(row))
        if result["status"] == "creation_disposed":
            break
    assert result["status"] == "creation_disposed", result
    source = await db.fetchrow(
        "SELECT * FROM vm_creation_retries WHERE request_id=$1", row["request_id"]
    )
    effects = await db.fetch(
        "SELECT * FROM vm_creation_effects WHERE request_id=$1 ORDER BY effect_nonce",
        row["request_id"],
    )
    assert source["reason"] == "creation_disposed" and effects
    await _finish_disposed_source(db, thread_id, soft_first=soft_first)
    assert (
        await db.fetchrow(
            "SELECT * FROM vm_creation_retries WHERE request_id=$1", row["request_id"]
        )
        == source
    )
    assert (
        await db.fetch(
            "SELECT * FROM vm_creation_effects WHERE request_id=$1 ORDER BY effect_nonce",
            row["request_id"],
        )
        == effects
    )
    assert (
        await db.fetchval(
            "SELECT state FROM vm_resource_reservations WHERE id=$1",
            admitted["reservation_id"],
        )
        == "released"
    )


@pytest.mark.asyncio
async def test_source_pin_receipt_is_required_before_audit_owner_deletion(
    db,
    setup,  # noqa: F811
    monkeypatch,
):
    from tests import test_vm_resource_thread_disposition_real_postgres as helpers
    from shared.vm_creation_disposition import disposition_identity
    from vm_controller.creation_disposition import CreationDisposer
    from vm_controller.creation_sources import pins

    (
        ctrl,
        api,
        store,
        row,
        admitted,
        source_name,
    ) = await helpers.missing_carrier_runtime(db, setup, monkeypatch)
    assert (
        row["effects"] == []
        and pins(api.read("DataVolume", source_name))[row["request_id"]]["state"]
        == "active"
    )
    assert (await store.settle_never_issued(request_id=row["request_id"]))[
        "settled"
    ] is False
    assert (
        await db.fetchval(
            "SELECT public.vm_thread_creation_terminal_evidence(r) FROM vm_creation_retries r WHERE request_id=$1",
            row["request_id"],
        )
        is None
    )
    api.lost.add("DataVolume")
    for _ in range(8):
        result = await CreationDisposer(ctrl).run(disposition_identity(row))
        if result["status"] == "creation_disposed":
            break
    assert result["status"] == "creation_disposed", result
    assert (
        pins(api.read("DataVolume", source_name))[row["request_id"]]["state"]
        == "disposed"
    )
    source = await db.fetchrow(
        "SELECT * FROM vm_creation_retries WHERE request_id=$1", row["request_id"]
    )
    await _finish_disposed_source(db, source["thread_id"], soft_first=False)
    assert (
        await db.fetchrow(
            "SELECT * FROM vm_creation_retries WHERE request_id=$1", row["request_id"]
        )
        == source
    )
    assert (
        await db.fetchval(
            "SELECT state FROM vm_resource_reservations WHERE id=$1",
            admitted["reservation_id"],
        )
        == "released"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("first", ["delete", "admit"])
async def test_late_admission_and_permanent_delete_serialize_without_new_charge(
    db, monkeypatch, first
):
    from tests import test_pinned_vm_failed_initial_end_real_postgres as helpers

    original_initial = helpers._initial_vm
    policies = []

    async def capture_policy(*args, **kwargs):
        values = await original_initial(*args, **kwargs)
        policies.append(values[1])
        return values

    monkeypatch.setattr(helpers, "_initial_vm", capture_policy)
    current, original, retirement = await _ready_to_delete(
        db, monkeypatch, soft_first=True
    )
    charges = await db.fetch(
        "SELECT * FROM vm_resource_reservations WHERE request_id=$1 ORDER BY id",
        original["request_id"],
    )

    async def delete():
        return await db.delete_thread(
            str(current["id"]),
            expected_runtime_generation=retirement["generation"],
            expected_runtime_retirement_token=retirement["token"],
        )

    async def admit():
        return await policies[0].admit(request_id=str(original["request_id"]))

    async with db.acquire() as locked, locked.transaction():
        await locked.fetchval(
            "SELECT id FROM threads WHERE id=$1 FOR UPDATE", current["id"]
        )
        first_task = asyncio.create_task(delete() if first == "delete" else admit())
        await _wait_for_owner_waiters(db, 1)
        second_task = asyncio.create_task(admit() if first == "delete" else delete())
        await _wait_for_owner_waiters(db, 2)
    first_result, second_result = await asyncio.gather(first_task, second_task)
    admitted = second_result if first == "delete" else first_result
    assert admitted["action"] == "unavailable", admitted
    assert await db.get_thread(str(current["id"])) is None
    assert (
        await db.fetch(
            "SELECT * FROM vm_resource_reservations WHERE request_id=$1 ORDER BY id",
            original["request_id"],
        )
        == charges
    )
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_creation_retries WHERE thread_id=$1", current["id"]
        )
        == 1
    )


@pytest.mark.asyncio
async def test_ready_source_is_outside_failed_initial_audit_deletion(db, monkeypatch):
    from tests.test_vm_resource_thread_source_real_postgres import _ready_charged_thread
    from orchestrator.services.vm_provisioning_phases import VMProvisioningPhaseStore

    prepared = await _ready_charged_thread(db, monkeypatch)
    assert await VMProvisioningPhaseStore(db).publish_thread_ready(
        str(prepared["thread_id"]),
        str(prepared["generation"]),
        prepared["registration"],
        prepared["vm_uid"],
        prepared["updates"],
    )
    assert (
        await db.fetchval(
            "SELECT public.vm_thread_creation_terminal_evidence(r) FROM vm_creation_retries r WHERE thread_id=$1",
            prepared["thread_id"],
        )
        is None
    )
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_thread_creation_settlements WHERE thread_id=$1",
            prepared["thread_id"],
        )
        == 0
    )
    assert (
        await db.fetchval(
            "SELECT live_thread_id FROM vm_thread_creation_owners WHERE thread_id=$1",
            prepared["thread_id"],
        )
        == prepared["thread_id"]
    )
    assert (
        await db.fetchval(
            "SELECT state FROM vm_resource_reservations WHERE id=$1",
            prepared["admitted"]["reservation_id"],
        )
        == "active"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("delete_first", [False, True])
async def test_retired_owner_cannot_receive_a_new_reservation(
    db, monkeypatch, delete_first
):
    current, original, retirement = await _ready_to_delete(
        db, monkeypatch, soft_first=True
    )
    if delete_first:
        await db.delete_thread(
            str(current["id"]),
            expected_runtime_generation=retirement["generation"],
            expected_runtime_retirement_token=retirement["token"],
        )
    with pytest.raises(asyncpg.CheckViolationError):
        await db.execute(
            "INSERT INTO vm_resource_reservations SELECT (jsonb_populate_record("
            "NULL::vm_resource_reservations,to_jsonb(v)||jsonb_build_object("
            "'id',$2::uuid,'revision',v.revision+1,'state','reserved','released_at',NULL,'release_evidence',NULL))).* "
            "FROM vm_resource_reservations v WHERE request_id=$1 LIMIT 1",
            original["request_id"],
            uuid4(),
        )


@pytest.mark.asyncio
async def test_deleted_owner_ledger_and_original_thread_id_cannot_be_reused(
    db, monkeypatch
):
    current, original, retirement = await _ready_to_delete(
        db, monkeypatch, soft_first=True
    )
    await db.delete_thread(
        str(current["id"]),
        expected_runtime_generation=retirement["generation"],
        expected_runtime_retirement_token=retirement["token"],
    )
    for statement in (
        "UPDATE vm_creation_retries SET revision=revision+1 WHERE request_id=$1",
        "DELETE FROM vm_creation_retries WHERE request_id=$1",
        "UPDATE vm_resource_waiters SET bypasses=bypasses+1 WHERE request_id=$1",
        "DELETE FROM vm_resource_waiters WHERE request_id=$1",
        "UPDATE vm_resource_reservations SET revision=revision+1 WHERE request_id=$1",
        "DELETE FROM vm_resource_reservations WHERE request_id=$1",
    ):
        with pytest.raises(
            asyncpg.CheckViolationError, match="retired VM audit evidence is immutable"
        ):
            await db.execute(statement, original["request_id"])
    with pytest.raises(asyncpg.CheckViolationError):
        await db.execute("INSERT INTO threads(id) VALUES($1)", current["id"])


@pytest.mark.asyncio
async def test_bound_source_agent_audit_fk_still_blocks_normal_agent_gc(
    db, monkeypatch
):
    current, original, retirement = await _ready_to_delete(
        db, monkeypatch, soft_first=True
    )
    await db.delete_thread(
        str(current["id"]),
        expected_runtime_generation=retirement["generation"],
        expected_runtime_retirement_token=retirement["token"],
    )
    agent = await db.fetchrow(
        "SELECT * FROM agents WHERE id=$1", original["thread_agent_id"]
    )
    assert (
        agent["status"] == "offline"
        and agent["thread_id"] is None
        and agent["current_job_id"] is None
    )
    assert (
        await db.fetchval(
            "SELECT count(*) FROM threads WHERE agent_id=$1 OR control_admission_agent_id=$1",
            agent["id"],
        )
        == 0
    )
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_resource_reservations WHERE request_id=$1 AND state<>'released'",
            original["request_id"],
        )
        == 0
    )
    # Separate actor-provenance ownership remains unchanged by this package.
    # Both normal deregistration and exact offline cleanup reach the existing
    # restrictive FK, despite the original thread and charge being settled.
    with pytest.raises(
        asyncpg.ForeignKeyViolationError,
        match="vm_creation_retries_thread_agent_id_fkey",
    ):
        await db.delete_agent(str(agent["id"]))
    with pytest.raises(
        asyncpg.ForeignKeyViolationError,
        match="vm_creation_retries_thread_agent_id_fkey",
    ):
        await db.delete_exact_offline_unbound_agent(
            str(agent["id"]),
            expected_hostname=agent["hostname"],
            expected_pod_uid=agent["pod_uid"],
        )
    unrelated = uuid4()
    await db.execute(
        "INSERT INTO agents(id,config_name,hostname,status,last_heartbeat) VALUES($1,'worker_base','owned-gc-control','offline',now()-interval '25 hours')",
        unrelated,
    )
    await db.execute(
        "UPDATE agents SET last_heartbeat=now()-interval '25 hours' WHERE id=$1",
        agent["id"],
    )
    with pytest.raises(
        asyncpg.ForeignKeyViolationError,
        match="vm_creation_retries_thread_agent_id_fkey",
    ):
        await db.gc_offline_agents(retention_hours=24)
    # One historical source rolls back the entire ordinary GC batch, including
    # another otherwise eligible agent owned only by this test.
    assert (
        await db.fetchval(
            "SELECT count(*) FROM agents WHERE id=ANY($1::uuid[])",
            [agent["id"], unrelated],
        )
        == 2
    )
