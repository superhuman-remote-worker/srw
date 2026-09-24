"""Connection-bound VM access admission through the actual idle tables."""
# ruff: noqa: F401, F811 -- imported pytest fixture and its parameter name

from __future__ import annotations

import asyncio
from uuid import UUID, uuid4
from types import SimpleNamespace
from unittest.mock import AsyncMock
from dataclasses import replace

import pytest

from tests.test_vm_idle_lifecycle_real_postgres import (
    _schema_applied,  # noqa: F401
    db,
    pg_dsn,  # noqa: F401
    postgres_db_fixture,  # noqa: F401
    profiled_idle_image_policy,  # noqa: F401
    seed_wait,
)


@pytest.mark.asyncio
async def test_warm_access_is_exact_connection_hold_and_poll_never_renews(
    db, monkeypatch
):
    from orchestrator.services.vm_idle_access import VMIdleAccessStore
    from orchestrator.services.vm_idle_lifecycle import VMIdleLifecycleStore

    for key, value in {
        "WORKSPACE_IDLE_RELEASE_ENABLED": "true",
        "VM_CREATION_RETRY_ENABLED": "true",
        "VM_REMOTE_OPERATION_PROTOCOL_ENABLED": "true",
        "VM_MODE": "same-cluster",
        "VM_PERSISTENT_ROOTDISK": "true",
    }.items():
        monkeypatch.setenv(key, value)
    await db.execute("TRUNCATE vm_idle_access_leases, vm_idle_operations CASCADE")
    owner, episode, identity = await seed_wait(db)
    access = VMIdleAccessStore(db)
    first = await access.acquire(
        owner_kind="job",
        owner_id=str(owner),
        kind="ide",
        claimant=f"tab:{uuid4()}",
    )
    second = await access.acquire(
        owner_kind="job",
        owner_id=str(owner),
        kind="ide",
        claimant=f"tab:{uuid4()}",
    )
    assert first and second and first["id"] != second["id"]
    assert first["provision_generation"] == UUID(identity["generation"])
    assert first["vm_uid"] == UUID(identity["vm_uid"])
    before = await db.fetchval(
        "SELECT expires_at FROM vm_idle_access_leases WHERE id=$1",
        first["id"],
    )
    assert await access.inspect(
        str(first["id"]),
        owner_kind="job",
        owner_id=str(owner),
        kind="ide",
        claimant=first["claimed_by"],
    )
    assert (
        await db.fetchval(
            "SELECT expires_at FROM vm_idle_access_leases WHERE id=$1",
            first["id"],
        )
        == before
    )
    assert (
        await VMIdleLifecycleStore(db).admit_release(
            str(owner),
            episode_id=episode.episode_id,
            revision=episode.revision,
            identity=identity,
        )
        is None
    )
    assert await access.close(
        str(first["id"]),
        owner_kind="job",
        owner_id=str(owner),
        kind="ide",
        claimant=first["claimed_by"],
    )
    assert await access.inspect(
        str(second["id"]),
        owner_kind="job",
        owner_id=str(owner),
        kind="ide",
        claimant=second["claimed_by"],
    )
    await db.execute(
        "UPDATE vm_idle_access_leases SET acquired_at=clock_timestamp()-interval '3 minutes',"
        "expires_at=clock_timestamp()-interval '1 minute' WHERE id=$1",
        second["id"],
    )
    assert not await access.renew(
        str(second["id"]),
        owner_kind="job",
        owner_id=str(owner),
        kind="ide",
        claimant=second["claimed_by"],
    )
    fresh = await access.acquire(
        owner_kind="job",
        owner_id=str(owner),
        kind="ide",
        claimant=f"tab:{uuid4()}",
    )
    assert fresh and fresh["id"] != second["id"]


@pytest.mark.asyncio
async def test_signed_gateway_connection_replay_never_extends_or_recreates_lease(db):
    from orchestrator.services.vm_idle_access import VMIdleAccessStore

    await db.execute("TRUNCATE vm_idle_access_leases, vm_idle_operations CASCADE")
    owner, _, _ = await seed_wait(db)
    access = VMIdleAccessStore(db)
    user_id = str(uuid4())
    args = dict(
        owner_kind="job",
        owner_id=str(owner),
        kind="ssh",
        user_id=user_id,
        connection_id="a" * 32,
    )
    first = await access.request(**args)
    assert first
    replay = await access.request(**args)
    assert replay and replay["id"] == first["id"]
    assert replay["expires_at"] == first["expires_at"]
    assert await access.close(
        str(first["id"]),
        owner_kind="job",
        owner_id=str(owner),
        kind="ssh",
        claimant=first["claimed_by"],
    )
    assert await access.request(**args) is None
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_idle_access_leases WHERE claimed_by=$1",
            first["claimed_by"],
        )
        == 1
    )
    next_connection = await access.request(**{**args, "connection_id": "b" * 32})
    assert next_connection and next_connection["id"] != first["id"]


@pytest.mark.asyncio
async def test_one_hour_cap_requires_new_explicit_connection_admission(db):
    from orchestrator.services.vm_idle_access import VMIdleAccessStore

    await db.execute("TRUNCATE vm_idle_access_leases, vm_idle_operations CASCADE")
    owner, _, _ = await seed_wait(db)
    access = VMIdleAccessStore(db)
    user = str(uuid4())
    first = await access.request(
        owner_kind="job",
        owner_id=str(owner),
        kind="ide",
        user_id=user,
    )
    assert first
    await db.execute(
        "UPDATE vm_idle_access_leases SET "
        "acquired_at=clock_timestamp()-interval '61 minutes',"
        "expires_at=clock_timestamp()-interval '1 minute',"
        "max_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1",
        first["id"],
    )
    assert not await access.renew(
        str(first["id"]),
        owner_kind="job",
        owner_id=str(owner),
        kind="ide",
        claimant=first["claimed_by"],
    )
    fresh = await access.request(
        owner_kind="job",
        owner_id=str(owner),
        kind="ide",
        user_id=user,
    )
    assert fresh and fresh["id"] != first["id"]
    assert (
        await access.inspect_for_user(
            str(first["id"]),
            owner_kind="job",
            owner_id=str(owner),
            kind="ide",
            user_id=user,
        )
        is None
    )


@pytest.mark.asyncio
async def test_pinned_cancel_conflicts_with_live_ide_writer_then_wins_after_close(db):
    from orchestrator.services.vm_idle_access import VMIdleAccessStore

    await db.execute("TRUNCATE vm_idle_access_leases, vm_idle_operations CASCADE")
    owner, _, _ = await seed_wait(db)
    await db.execute("UPDATE jobs SET execution_lane='pinned' WHERE id=$1", owner)
    access = VMIdleAccessStore(db)
    user = str(uuid4())
    tab = await access.acquire(
        owner_kind="job",
        owner_id=str(owner),
        kind="ide",
        claimant=f"{user}:tab",
    )
    assert tab
    writer = await access.begin_ide_operation(
        str(tab["id"]),
        owner_kind="job",
        owner_id=str(owner),
        user_id=user,
    )
    assert writer
    assert await access.close_for_user(
        str(tab["id"]),
        owner_kind="job",
        owner_id=str(owner),
        kind="ide",
        user_id=user,
    )
    assert not await db.linearize_pinned_cancel(
        str(owner),
        expected_status="waiting_for_reply",
    )
    assert await access.close(
        str(writer["id"]),
        owner_kind="job",
        owner_id=str(owner),
        kind="ide",
        claimant=writer["claimed_by"],
    )
    assert await db.linearize_pinned_cancel(
        str(owner),
        expected_status="waiting_for_reply",
    )
    assert (
        await access.acquire(
            owner_kind="job",
            owner_id=str(owner),
            kind="ide",
            claimant=f"{user}:new",
        )
        is None
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ["general", "pinned", "stateless_queue"])
async def test_cancel_waiting_on_writer_owner_lock_sees_committed_lease(db, path):
    """An admission that wins the Job lock must beat a waiting End/cancel."""
    await db.execute("TRUNCATE vm_idle_access_leases, vm_idle_operations CASCADE")
    owner, _, identity = await seed_wait(db)
    await db.execute(
        "UPDATE jobs SET execution_lane=$2 WHERE id=$1",
        owner,
        "pinned" if path == "pinned" else "stateless",
    )
    writer_id = uuid4()
    async with db.acquire() as conn, conn.transaction():
        # Same queue -> Job order and insert shape as begin_ide_operation.
        await conn.fetchrow(
            "SELECT unit_id FROM run_queue WHERE unit_id=$1 FOR UPDATE", owner
        )
        await conn.fetchrow("SELECT id FROM jobs WHERE id=$1 FOR UPDATE", owner)
        cancellation = (
            db.linearize_pinned_cancel(str(owner), expected_status="waiting_for_reply")
            if path == "pinned"
            else db.cancel_stateless_job(str(owner))
            if path == "stateless_queue"
            else db.cancel_job(str(owner))
        )
        cancel = asyncio.create_task(cancellation)
        await asyncio.sleep(0.05)
        assert not cancel.done()
        await conn.execute(
            "INSERT INTO vm_idle_access_leases "
            "(id,owner_kind,owner_id,provision_generation,vm_uid,kind,claimed_by,"
            "expires_at,max_expires_at) VALUES "
            "($1,'job',$2,$3,$4,'ide',$5,"
            "clock_timestamp()+interval '2 minutes',"
            "clock_timestamp()+interval '1 hour')",
            writer_id,
            owner,
            UUID(identity["generation"]),
            UUID(identity["vm_uid"]),
            f"{uuid4()}:operation:{uuid4()}",
        )
    assert await asyncio.wait_for(cancel, timeout=2) == (
        (False, False) if path == "stateless_queue" else False
    )
    assert await db.fetchval("SELECT status::text FROM jobs WHERE id=$1", owner) == (
        "waiting_for_reply"
    )


@pytest.mark.asyncio
async def test_ide_writer_survives_tab_close_and_blocks_ready_recycle_and_cleanup(db):
    from orchestrator.services.vm_idle_access import VMIdleAccessStore
    from orchestrator.services.vm_workspace_recovery_store import (
        VMWorkspaceRecoveryStore,
        acquire_vm_cleanup_permit,
    )

    await db.execute("TRUNCATE vm_idle_access_leases, vm_idle_operations CASCADE")
    owner, _, identity = await seed_wait(db)
    access = VMIdleAccessStore(db)
    user = str(uuid4())
    tab = await access.acquire(
        owner_kind="job", owner_id=str(owner), kind="ide", claimant=f"{user}:tab"
    )
    assert tab
    writer = await access.begin_ide_operation(
        str(tab["id"]),
        owner_kind="job",
        owner_id=str(owner),
        user_id=user,
    )
    assert writer and writer["id"] != tab["id"]
    assert not await access.close_for_user(
        str(writer["id"]),
        owner_kind="job",
        owner_id=str(owner),
        kind="ide",
        user_id=user,
    )
    assert await access.close_for_user(
        str(tab["id"]), owner_kind="job", owner_id=str(owner), kind="ide", user_id=user
    )
    assert await access.inspect(
        str(writer["id"]),
        owner_kind="job",
        owner_id=str(owner),
        kind="ide",
        claimant=writer["claimed_by"],
    )

    await db.execute("UPDATE jobs SET status='paused' WHERE id=$1", owner)
    assert not await db.begin_ready_vm_retirement_if_quiescent(
        str(owner),
        provision_generation=identity["generation"],
        vm_uid=identity["vm_uid"],
        pvc_uid=identity["pvc_uid"],
    )
    cleanup = await acquire_vm_cleanup_permit(
        VMWorkspaceRecoveryStore(db),
        owner_kind="job",
        owner_id=str(owner),
        identity=SimpleNamespace(
            provision_generation=identity["generation"],
            vm_uid=identity["vm_uid"],
            rootdisk_pvc_uid=identity["pvc_uid"],
        ),
        source="public_vm_delete",
        purge_disk=True,
    )
    assert not cleanup.allowed and cleanup.reason == "active_workspace_access"
    assert await access.close(
        str(writer["id"]),
        owner_kind="job",
        owner_id=str(owner),
        kind="ide",
        claimant=writer["claimed_by"],
    )
    assert await db.begin_ready_vm_retirement_if_quiescent(
        str(owner),
        provision_generation=identity["generation"],
        vm_uid=identity["vm_uid"],
        pvc_uid=identity["pvc_uid"],
    )
    assert (
        await access.begin_ide_operation(
            str(tab["id"]),
            owner_kind="job",
            owner_id=str(owner),
            user_id=user,
        )
        is None
    )


@pytest.mark.asyncio
async def test_ide_operation_expiry_cancels_transport_and_closes_only_its_row(db):
    import asyncio
    from orchestrator.services.vm_idle_access import VMIdleAccessLost, VMIdleAccessStore

    await db.execute("TRUNCATE vm_idle_access_leases, vm_idle_operations CASCADE")
    owner, _, _ = await seed_wait(db)
    access = VMIdleAccessStore(db)
    user = str(uuid4())
    tab = await access.acquire(
        owner_kind="job", owner_id=str(owner), kind="ide", claimant=f"{user}:tab"
    )
    assert tab
    entered = asyncio.Event()

    async def holding_upload():
        async with access.ide_operation(
            str(tab["id"]),
            owner_kind="job",
            owner_id=str(owner),
            user_id=user,
            heartbeat_seconds=0.02,
        ):
            entered.set()
            await asyncio.sleep(10)

    task = asyncio.create_task(holding_upload())
    await asyncio.wait_for(entered.wait(), timeout=2)
    writer_id = await db.fetchval(
        "SELECT id FROM vm_idle_access_leases WHERE owner_id=$1 AND claimed_by LIKE $2",
        owner,
        f"{user}:operation:%",
    )
    assert writer_id
    await db.execute(
        "UPDATE vm_idle_access_leases SET "
        "acquired_at=clock_timestamp()-interval '3 minutes', "
        "expires_at=clock_timestamp()-interval '1 second' "
        "WHERE id=$1",
        writer_id,
    )
    with pytest.raises(VMIdleAccessLost):
        await asyncio.wait_for(task, timeout=2)
    assert await db.fetchval(
        "SELECT closed_at IS NOT NULL FROM vm_idle_access_leases WHERE id=$1",
        writer_id,
    )
    assert await access.inspect(
        str(tab["id"]),
        owner_kind="job",
        owner_id=str(owner),
        kind="ide",
        claimant=tab["claimed_by"],
    )


@pytest.mark.asyncio
async def test_connected_ide_socket_renews_old_tab_and_close_does_not_kill_writer(db):
    from orchestrator.services.vm_idle_access import VMIdleAccessStore

    await db.execute("TRUNCATE vm_idle_access_leases, vm_idle_operations CASCADE")
    owner, _, _ = await seed_wait(db)
    access = VMIdleAccessStore(db)
    user = str(uuid4())
    tab = await access.acquire(
        owner_kind="job", owner_id=str(owner), kind="ide", claimant=f"{user}:tab"
    )
    # The tab has already been connected for longer than its initial two-minute
    # sliding TTL. A live socket, rather than an HTTP poll, must keep it usable.
    await db.execute(
        "UPDATE vm_idle_access_leases SET acquired_at=clock_timestamp()-interval '3 minutes', "
        "expires_at=clock_timestamp()+interval '5 seconds' WHERE id=$1", tab["id"],
    )
    entered = asyncio.Event()
    async def socket():
        async with access.ide_operation(
            str(tab["id"]), owner_kind="job", owner_id=str(owner),
            user_id=user, heartbeat_seconds=0.02,
        ):
            entered.set()
            await asyncio.sleep(10)

    task = asyncio.create_task(socket())
    await asyncio.wait_for(entered.wait(), 2)
    try:
        await asyncio.sleep(0.15)
        assert not task.done()
        assert await db.fetchval(
            "SELECT expires_at>clock_timestamp()+interval '1 minute' "
            "FROM vm_idle_access_leases WHERE id=$1", tab["id"],
        )
        second_writer = await access.begin_ide_operation(
            str(tab["id"]), owner_kind="job", owner_id=str(owner), user_id=user,
        )
        assert second_writer is not None
        assert await access.close(
            str(second_writer["id"]), owner_kind="job", owner_id=str(owner),
            kind="ide", claimant=second_writer["claimed_by"],
        )
        writer_row = await db.fetchrow(
            "SELECT id,claimed_by FROM vm_idle_access_leases WHERE owner_id=$1 "
            "AND claimed_by LIKE $2 AND closed_at IS NULL ORDER BY acquired_at LIMIT 1",
            owner, f"{user}:operation:%",
        )
        writer = writer_row["id"]
        other_tab = await access.acquire(
            owner_kind="job", owner_id=str(owner), kind="ide",
            claimant=f"{user}:other-tab",
        )
        assert other_tab is not None
        assert not await access.renew_tab_for_operation(
            str(other_tab["id"]), operation_id=str(writer),
            operation_claimant=writer_row["claimed_by"],
            tab_claimant=other_tab["claimed_by"],
            owner_kind="job", owner_id=str(owner),
        )
        assert await access.close_for_user(
            str(tab["id"]), owner_kind="job", owner_id=str(owner),
            kind="ide", user_id=user,
        )
        closed_expiry = await db.fetchval(
            "SELECT expires_at FROM vm_idle_access_leases WHERE id=$1", tab["id"],
        )
        await asyncio.sleep(0.08)
        assert not task.done()
        assert await db.fetchval(
            "SELECT closed_at IS NULL FROM vm_idle_access_leases WHERE id=$1", writer,
        )
        assert await access.begin_ide_operation(
            str(tab["id"]), owner_kind="job", owner_id=str(owner), user_id=user,
        ) is None
        assert await db.fetchval(
            "SELECT closed_at IS NOT NULL FROM vm_idle_access_leases WHERE id=$1",
            tab["id"],
        )
        assert await db.fetchval(
            "SELECT expires_at FROM vm_idle_access_leases WHERE id=$1", tab["id"],
        ) == closed_expiry
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    assert await db.fetchval(
        "SELECT closed_at IS NOT NULL FROM vm_idle_access_leases WHERE id=$1", writer,
    )


@pytest.mark.asyncio
async def test_job_ide_start_runtime_swap_refuses_active_url_after_transport(db):
    from orchestrator.routers.ide import IdeDependencies, start_ide_session
    from orchestrator.services.vm_ide_transport import VMIDETransport
    from tests.test_vm_ide_transport import _Pool, _proof

    await db.execute("TRUNCATE vm_idle_access_leases, vm_idle_operations CASCADE")
    owner, _, identity = await seed_wait(db)
    user = {"id": uuid4(), "is_approved": True}
    job = await db.get_job(str(owner))
    entered, resume = asyncio.Event(), asyncio.Event()
    admitted = replace(
        _proof(), workspace_generation=identity["generation"], vm_uid=identity["vm_uid"],
    )
    successor = replace(admitted, vm_uid=str(uuid4()))
    async def swap(*args, **kwargs):
        entered.set()
        await resume.wait()
        return successor
    connection = SimpleNamespace(run=AsyncMock(), open_connection=AsyncMock())
    transport = VMIDETransport(
        SimpleNamespace(attest_workspace_runtime=AsyncMock(side_effect=swap)),
        pool=_Pool(connection), key_path="/private/key",
    )
    deps = IdeDependencies(
        store=db, ide_sessions=object(), ide_proxy=SimpleNamespace(_vm_provisioner=object()),
        vm_ide_transport=transport,
        require_job_access=AsyncMock(return_value=(user, job)),
    )
    task = asyncio.create_task(start_ide_session(SimpleNamespace(), str(owner), dependencies=deps))
    await asyncio.wait_for(entered.wait(), 2)
    await db.execute(
        "UPDATE jobs SET context=jsonb_set(context,'{vm,vm_uid}',to_jsonb($2::text)) "
        "WHERE id=$1", owner, str(uuid4()),
    )
    resume.set()
    response = await asyncio.wait_for(task, 2)
    assert response == {"status": "unavailable", "code_server_url": None,
                        "code": "ide_runtime_changed"}
    assert await db.fetchval(
        "SELECT count(*) FROM vm_idle_access_leases WHERE owner_id=$1 AND closed_at IS NOT NULL",
        owner,
    ) == 1
    connection.run.assert_not_awaited()


@pytest.mark.asyncio
async def test_expired_pending_claim_cannot_be_revived_by_repeat_wake(db, monkeypatch):
    from orchestrator.services.vm_idle_lifecycle import VMIdleLifecycleStore

    for key, value in {
        "WORKSPACE_IDLE_RELEASE_ENABLED": "true",
        "VM_CREATION_RETRY_ENABLED": "true",
        "VM_REMOTE_OPERATION_PROTOCOL_ENABLED": "true",
        "VM_MODE": "same-cluster",
        "VM_PERSISTENT_ROOTDISK": "true",
    }.items():
        monkeypatch.setenv(key, value)
    await db.execute("TRUNCATE vm_idle_access_leases, vm_idle_operations CASCADE")
    owner, episode, identity = await seed_wait(db)
    idle = VMIdleLifecycleStore(db)
    operation = await idle.admit_release(
        str(owner),
        episode_id=episode.episode_id,
        revision=episode.revision,
        identity=identity,
    )
    assert operation
    claimant = f"tab:{uuid4()}"
    wake = await idle.request_wake(
        str(owner),
        execution_requested=False,
        access_kind="ide",
        access_claimant=claimant,
    )
    assert wake
    original = await db.fetchval(
        "SELECT id FROM vm_idle_access_leases WHERE owner_id=$1 AND claimed_by=$2",
        owner,
        claimant,
    )
    await db.execute(
        "UPDATE vm_idle_access_leases SET acquired_at=clock_timestamp()-interval '3 minutes',"
        "expires_at=clock_timestamp()-interval '1 minute' WHERE id=$1",
        original,
    )
    assert await idle.request_wake(
        str(owner),
        execution_requested=False,
        access_kind="ide",
        access_claimant=claimant,
    )
    assert await db.fetchval(
        "SELECT expires_at<clock_timestamp() FROM vm_idle_access_leases WHERE id=$1",
        original,
    )
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_idle_access_leases WHERE owner_id=$1 AND claimed_by=$2",
            owner,
            claimant,
        )
        == 2
    )


@pytest.mark.asyncio
async def test_new_access_request_joins_one_wake_and_never_executes_job(
    db, monkeypatch
):
    from orchestrator.services.vm_idle_access import VMIdleAccessStore
    from orchestrator.services.vm_idle_lifecycle import VMIdleLifecycleStore

    for key, value in {
        "WORKSPACE_IDLE_RELEASE_ENABLED": "true",
        "VM_CREATION_RETRY_ENABLED": "true",
        "VM_REMOTE_OPERATION_PROTOCOL_ENABLED": "true",
        "VM_MODE": "same-cluster",
        "VM_PERSISTENT_ROOTDISK": "true",
    }.items():
        monkeypatch.setenv(key, value)
    await db.execute("TRUNCATE vm_idle_access_leases, vm_idle_operations CASCADE")
    owner, episode, identity = await seed_wait(db)
    idle = VMIdleLifecycleStore(db)
    operation = await idle.admit_release(
        str(owner),
        episode_id=episode.episode_id,
        revision=episode.revision,
        identity=identity,
    )
    assert operation
    access = VMIdleAccessStore(db)
    a, b = await __import__("asyncio").gather(
        access.request(
            owner_kind="job", owner_id=str(owner), kind="ide", user_id=str(uuid4())
        ),
        access.request(
            owner_kind="job", owner_id=str(owner), kind="ide", user_id=str(uuid4())
        ),
    )
    assert a and b and a["id"] != b["id"]
    assert a["wake_id"] == b["wake_id"]
    assert (
        await db.fetchval(
            "SELECT wake_execution_requested FROM vm_idle_operations WHERE id=$1",
            operation["id"],
        )
        is False
    )
    assert (
        await db.fetchval(
            "SELECT state FROM run_queue WHERE unit_id=$1",
            owner,
        )
        == "done"
    )
    assert (
        await access.inspect_for_user(
            str(a["id"]),
            owner_kind="job",
            owner_id=str(owner),
            kind="ide",
            user_id=a["claimed_by"].split(":", 1)[0],
        )
        is None
    )


@pytest.mark.asyncio
async def test_current_identity_change_refuses_stale_lease(db):
    from orchestrator.services.vm_idle_access import VMIdleAccessStore

    await db.execute("TRUNCATE vm_idle_access_leases, vm_idle_operations CASCADE")
    owner, _, _ = await seed_wait(db)
    access = VMIdleAccessStore(db)
    claimant = f"{uuid4()}:{uuid4()}"
    lease = await access.acquire(
        owner_kind="job", owner_id=str(owner), kind="ide", claimant=claimant
    )
    assert lease
    await db.execute(
        "UPDATE jobs SET context=jsonb_set(context,'{vm,vm_uid}',$2::jsonb) "
        "WHERE id=$1",
        owner,
        '"' + str(uuid4()) + '"',
    )
    assert (
        await access.inspect(
            str(lease["id"]),
            owner_kind="job",
            owner_id=str(owner),
            kind="ide",
            claimant=claimant,
        )
        is None
    )
    assert not await access.renew(
        str(lease["id"]),
        owner_kind="job",
        owner_id=str(owner),
        kind="ide",
        claimant=claimant,
    )


@pytest.mark.asyncio
async def test_job_ide_actual_entrypoints_do_not_renew_on_status_poll(db):
    from orchestrator.routers.ide import (
        IdeDependencies,
        get_ide_session,
        start_ide_session,
        stop_ide_session,
    )

    await db.execute("TRUNCATE vm_idle_access_leases, vm_idle_operations CASCADE")
    owner, _, _ = await seed_wait(db)
    user = {"id": uuid4(), "is_approved": True}
    job = await db.get_job(str(owner))
    def admitted_proof(*args, **kwargs):
        return SimpleNamespace(
            workspace_generation=str(kwargs["expected_generation"]),
            vm_uid=str(kwargs["expected_vm_uid"]),
        )
    transport = SimpleNamespace(
        start_and_probe=AsyncMock(side_effect=admitted_proof),
        probe=AsyncMock(side_effect=admitted_proof),
    )
    sessions = SimpleNamespace(
        start_session=AsyncMock(),
        get_session_status=AsyncMock(),
        stop_session=AsyncMock(),
    )
    deps = IdeDependencies(
        store=db,
        ide_sessions=sessions,
        ide_proxy=SimpleNamespace(_vm_provisioner=object()),
        vm_ide_transport=transport,
        require_job_access=AsyncMock(return_value=(user, job)),
    )
    response = await start_ide_session(SimpleNamespace(), str(owner), dependencies=deps)
    assert response["status"] == "active"
    lease_id = UUID(response["access_lease_id"])
    before = await db.fetchval(
        "SELECT expires_at FROM vm_idle_access_leases WHERE id=$1",
        lease_id,
    )
    for _ in range(2):
        status = await get_ide_session(SimpleNamespace(), str(owner), dependencies=deps)
        assert status["code_server_url"] is None
    assert (
        await db.fetchval(
            "SELECT expires_at FROM vm_idle_access_leases WHERE id=$1",
            lease_id,
        )
        == before
    )
    sessions.get_session_status.assert_not_awaited()
    status = await get_ide_session(
        SimpleNamespace(),
        str(owner),
        lease_id=str(lease_id),
        dependencies=deps,
    )
    assert status["status"] == "active"
    assert str(lease_id) in status["code_server_url"]
    assert transport.start_and_probe.await_args.kwargs["expected_vm_uid"]
    assert transport.start_and_probe.await_args.kwargs["expected_generation"]
    assert transport.probe.await_args.kwargs["expected_vm_uid"]
    assert transport.probe.await_args.kwargs["expected_generation"]
    transport.probe.side_effect = lambda *args, **kwargs: SimpleNamespace(
        workspace_generation=str(kwargs["expected_generation"]),
        vm_uid=str(uuid4()),
    )
    wrong_proof = await get_ide_session(
        SimpleNamespace(), str(owner), lease_id=str(lease_id), dependencies=deps,
    )
    assert wrong_proof["status"] == "unavailable"
    assert wrong_proof["code"] == "ide_runtime_changed"
    assert (
        await db.fetchval(
            "SELECT expires_at FROM vm_idle_access_leases WHERE id=$1",
            lease_id,
        )
        == before
    )
    async def switch_during_probe(*args, **kwargs):
        await db.execute(
            "UPDATE jobs SET context=jsonb_set(context,'{vm,vm_uid}',to_jsonb($2::text)) "
            "WHERE id=$1", owner, str(uuid4()),
        )
        return admitted_proof(*args, **kwargs)
    transport.probe.side_effect = switch_during_probe
    stale = await get_ide_session(
        SimpleNamespace(), str(owner), lease_id=str(lease_id), dependencies=deps,
    )
    assert stale["status"] == "unavailable"
    assert stale["code"] == "ide_runtime_changed"
    await stop_ide_session(
        SimpleNamespace(), str(owner), lease_id=str(lease_id), dependencies=deps
    )
    assert await db.fetchval(
        "SELECT closed_at IS NOT NULL FROM vm_idle_access_leases WHERE id=$1",
        lease_id,
    )
    sessions.stop_session.assert_not_awaited()


@pytest.mark.asyncio
async def test_vm_proxy_actual_route_requires_current_caller_lease(db):
    from fastapi import HTTPException
    from orchestrator.routers.ide import IdeDependencies, ide_proxy_http
    from orchestrator.services.ide_proxy import IdeProxyTarget
    from orchestrator.services.vm_ide_transport import VMIDEHTTPResponse
    from orchestrator.services.vm_idle_access import VMIdleAccessStore

    await db.execute("TRUNCATE vm_idle_access_leases, vm_idle_operations CASCADE")
    owner, _, identity = await seed_wait(db)
    user = {"id": uuid4(), "is_approved": True}
    lease = await VMIdleAccessStore(db).request(
        owner_kind="job",
        owner_id=str(owner),
        kind="ide",
        user_id=str(user["id"]),
    )
    target = IdeProxyTarget(
        entity_id=str(owner),
        owner_kind="job",
        backend="vm",
        scope="vm",
        host="10.42.0.91",
        port=22,
        identity=(
            identity["generation"],
            identity["vm_uid"],
            identity["vmi_uid"],
            identity["launcher_uid"],
            identity["pvc_uid"],
            "SHA256:" + "A" * 43,
        ),
    )
    transport = SimpleNamespace(
        request_http=AsyncMock(
            return_value=VMIDEHTTPResponse(
                200,
                (("content-type", "text/plain"),),
                b"guest",
            )
        )
    )
    deps = IdeDependencies(
        store=db,
        ide_sessions=object(),
        ide_proxy=SimpleNamespace(
            resolve_target=AsyncMock(return_value=target), evict=lambda *_: None
        ),
        vm_ide_transport=transport,
        require_approved_user=AsyncMock(return_value=user),
        user_can_access_ide_entity=AsyncMock(return_value=True),
    )
    request = SimpleNamespace(
        headers={"accept": "text/plain"},
        method="GET",
        url=SimpleNamespace(query=""),
        client=SimpleNamespace(host="127.0.0.1"),
    )
    response = await ide_proxy_http(
        request,
        str(owner),
        path=f"_vm/{lease['id']}/workspace",
        dependencies=deps,
    )
    assert response.body == b"guest"
    assert transport.request_http.await_args.kwargs["url"].endswith("/workspace")
    with pytest.raises(HTTPException) as exc:
        await ide_proxy_http(request, str(owner), path="workspace", dependencies=deps)
    assert exc.value.status_code == 409
    other = {"id": uuid4(), "is_approved": True}
    other_deps = replace(deps, require_approved_user=AsyncMock(return_value=other))
    with pytest.raises(HTTPException) as exc:
        await ide_proxy_http(
            request,
            str(owner),
            path=f"_vm/{lease['id']}/workspace",
            dependencies=other_deps,
        )
    assert exc.value.status_code == 409
