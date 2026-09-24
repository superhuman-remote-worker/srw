"""A recovery gate reset must not forge a stateless dispatch transition."""

import json
from uuid import UUID
from unittest.mock import AsyncMock

import pytest

from orchestrator.operator_cli.vm_workspace_recovery_acceptance import (
    AcceptanceFailure,
    LiveScenario,
)
from shared.worker_queue import _CAS_JOB_SQL
from tests.test_vm_recovery_gate_stop_store_real_postgres import seeded
from tests.test_vm_workspace_recovery_real_postgres import (  # noqa: F401
    app_pg as _app_pg,
    pg_dsn,
    _schema_applied,
    insert_recovery,
)

app_pg = _app_pg


@pytest.mark.asyncio
async def test_resolve_then_reset_uses_fresh_workspace_claim_authority(app_pg):
    doc = await seeded(app_pg)
    job_id, operation_id = UUID(doc["job_id"]), UUID(doc["operation_id"])
    scenario = object.__new__(LiveScenario)
    scenario.db, scenario.run_id = app_pg, doc["run_id"]
    async with app_pg.acquire() as conn:
        leased_until = await conn.fetchval(
            "INSERT INTO run_queue(unit_id,unit_kind,state,lease_token,leased_by,leased_until) "
            "VALUES($1,'worker_batch','leased',27,$2,clock_timestamp()+interval '5 minutes') "
            "RETURNING leased_until",
            job_id,
            f"vm-recovery-gate:{doc['run_id']}",
        )
        assert (
            await conn.fetchval(
                _CAS_JOB_SQL,
                job_id,
                "processing",
                f"vm-recovery-gate:{doc['run_id']}",
                27,
                leased_until,
            )
            == job_id
        )
        await conn.execute(
            "UPDATE jobs SET status='paused',freeze_data='{}'::jsonb WHERE id=$1",
            job_id,
        )
        await conn.execute(
            "UPDATE run_queue SET state='parked',lease_token=28,leased_by=NULL,"
            "leased_until=NULL,park_reason='workspace_recovery' WHERE unit_id=$1",
            job_id,
        )
        await conn.execute(
            "UPDATE vm_workspace_recoveries SET phase='paused_attention' WHERE id=$1",
            operation_id,
        )
    await scenario._resolve_fixture_recovery(operation_id, job_id)
    assert (
        await app_pg.fetchval("SELECT status FROM jobs WHERE id=$1", job_id) == "paused"
    )
    assert (
        await app_pg.fetchval("SELECT freeze_data FROM jobs WHERE id=$1", job_id)
        is None
    )
    token = await scenario._reset_lease(job_id)
    assert token == 29
    async with app_pg.acquire() as conn:
        job = await conn.fetchrow("SELECT status,context FROM jobs WHERE id=$1", job_id)
        queue = await conn.fetchrow("SELECT * FROM run_queue WHERE unit_id=$1", job_id)
        marker = json.loads(job["context"])["_workspace_dispatch_authority"]
        assert job["status"] == "processing"
        assert marker["queue_lease_token"] == queue["lease_token"] == token
        assert (
            marker["worker_pod"]
            == queue["leased_by"]
            == f"vm-recovery-gate:{doc['run_id']}"
        )
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM worker_batch_attempts WHERE job_id=$1 AND lease_token=$2",
                job_id,
                token,
            )
            == 1
        )
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM vm_workspace_recovery_jobs WHERE job_id=$1 AND resolved_at IS NULL",
                job_id,
            )
            == 0
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "refusal",
    [
        "foreign_run",
        "open_recovery",
        "operator_pause",
        "terminal",
        "live_worker",
        "foreign_park",
    ],
)
async def test_fixture_claim_refusal_rolls_back_queue_reset(app_pg, refusal):
    doc = await seeded(app_pg)
    job_id = UUID(doc["job_id"])
    scenario = object.__new__(LiveScenario)
    scenario.db, scenario.run_id = app_pg, doc["run_id"]
    async with app_pg.acquire() as conn:
        await conn.execute(
            "INSERT INTO run_queue(unit_id,unit_kind,state,lease_token) VALUES($1,'worker_batch','parked',28)",
            job_id,
        )
        if refusal != "open_recovery":
            await conn.execute(
                "UPDATE vm_workspace_recovery_jobs SET resolved_at=clock_timestamp(),participation='cancelled' WHERE job_id=$1",
                job_id,
            )
        if refusal == "foreign_run":
            scenario.run_id = "another-run"
        elif refusal == "operator_pause":
            await conn.execute(
                "UPDATE jobs SET status='paused',context=context||'{\"_operator_pause_hold\":{}}'::jsonb WHERE id=$1",
                job_id,
            )
        elif refusal == "terminal":
            await conn.execute("UPDATE jobs SET status='cancelled' WHERE id=$1", job_id)
        elif refusal == "live_worker":
            await conn.execute(
                "UPDATE run_queue SET state='leased',leased_by='another-worker',"
                "leased_until=clock_timestamp()+interval '5 minutes' WHERE unit_id=$1",
                job_id,
            )
        elif refusal == "foreign_park":
            await conn.execute(
                "UPDATE run_queue SET park_reason='operator_pause' WHERE unit_id=$1",
                job_id,
            )
        before = dict(
            await conn.fetchrow("SELECT * FROM run_queue WHERE unit_id=$1", job_id)
        )
    with pytest.raises(AcceptanceFailure, match="current claim authority"):
        await scenario._reset_lease(job_id)
    assert (
        dict(await app_pg.fetchrow("SELECT * FROM run_queue WHERE unit_id=$1", job_id))
        == before
    )
    assert (
        await app_pg.fetchval(
            "SELECT count(*) FROM worker_batch_attempts WHERE job_id=$1", job_id
        )
        == 0
    )


@pytest.mark.asyncio
async def test_cleanup_preserves_completed_recovery_evidence(app_pg):
    doc, scenario, _ = await _pinned_fixture(app_pg)
    operation_id = UUID(doc["operation_id"])
    async with app_pg.acquire() as conn:
        await conn.execute(
            "UPDATE vm_workspace_recoveries SET phase='recovered',resolved_at=clock_timestamp() WHERE id=$1",
            operation_id,
        )
        await conn.execute(
            "UPDATE vm_workspace_recovery_jobs SET participation='released',resolved_at=clock_timestamp() WHERE recovery_id=$1",
            operation_id,
        )
        await conn.execute(
            "UPDATE vm_workspace_recovery_retention_pins SET released_at=clock_timestamp(),"
            "controller_release_requested_at=clock_timestamp(),"
            "controller_released_at=clock_timestamp() WHERE recovery_id=$1",
            operation_id,
        )
        before = dict(
            await conn.fetchrow(
                "SELECT * FROM vm_workspace_recoveries WHERE id=$1", operation_id
            )
        )
    await scenario.cleanup()
    scenario._purge_fixture.assert_awaited_once_with(UUID(doc["job_id"]))
    assert (
        dict(
            await app_pg.fetchrow(
                "SELECT * FROM vm_workspace_recoveries WHERE id=$1", operation_id
            )
        )
        == before
    )


async def _pinned_fixture(app_pg):
    doc = await seeded(app_pg, source_run=True)
    job_id, recovery_id = UUID(doc["job_id"]), UUID(doc["operation_id"])
    pin_uid = str(UUID("ec84fd66-961a-450b-8b33-110c262faf8b"))
    async with app_pg.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET context=context||$2::jsonb WHERE id=$1",
            job_id,
            json.dumps(
                {
                    "vm": {
                        "provision_generation": doc["generation"],
                        "rootdisk_pvc_uid": doc["pvc_uid"],
                    }
                }
            ),
        )
        await conn.execute(
            "INSERT INTO vm_workspace_recovery_retention_pins "
            "(recovery_id,pvc_uid,provision_generation,controller_pinned_at,"
            "controller_pin_uid,controller_pin_resource_version) "
            "VALUES($1,$2,$3,clock_timestamp(),$4,'1')",
            recovery_id,
            UUID(doc["pvc_uid"]),
            UUID(doc["generation"]),
            pin_uid,
        )
    scenario = object.__new__(LiveScenario)
    scenario.db, scenario.run_id = app_pg, doc["run_id"]
    scenario.namespace = doc["namespace"]
    scenario.settings = type("Settings", (), {"external_call_timeout_seconds": 1})()
    scenario._cleanup_stop_retention = AsyncMock()
    scenario._purge_fixture = AsyncMock()
    return doc, scenario, pin_uid


@pytest.mark.asyncio
async def test_cleanup_releases_only_exact_fixture_controller_pin_before_purge(app_pg):
    doc, scenario, pin_uid = await _pinned_fixture(app_pg)
    foreign_recovery = await insert_recovery(app_pg, owner_id=UUID(int=932))
    async with app_pg.acquire() as conn:
        foreign = await conn.fetchrow(
            "SELECT root_pvc_uid,provision_generation FROM vm_workspace_recoveries WHERE id=$1",
            foreign_recovery,
        )
        await conn.execute(
            "INSERT INTO vm_workspace_recovery_retention_pins "
            "(recovery_id,pvc_uid,provision_generation,controller_pinned_at,"
            "controller_pin_uid,controller_pin_resource_version) "
            "VALUES($1,$2,$3,clock_timestamp(),$4,'2')",
            foreign_recovery,
            foreign["root_pvc_uid"],
            foreign["provision_generation"],
            str(UUID(int=933)),
        )

    async def acknowledge(command):
        assert command.owner_id == UUID(doc["job_id"])
        assert command.recovery_id == UUID(doc["operation_id"])
        assert command.desired_state == "released"
        assert command.controller_pin_uid == pin_uid
        assert scenario._purge_fixture.await_count == 0
        return {
            "state": "released",
            "recovery_id": str(command.recovery_id),
            "pvc_uid": str(command.pvc_uid),
            "provision_generation": str(command.provision_generation),
            "pin_uid": pin_uid,
            "resource_version": "1",
        }

    scenario.provisioner = type(
        "Observer", (), {"reconcile_workspace_recovery_pin": staticmethod(acknowledge)}
    )()
    await scenario.cleanup()
    scenario._purge_fixture.assert_awaited_once_with(UUID(doc["job_id"]))
    own = await app_pg.fetchrow(
        "SELECT released_at,controller_released_at FROM vm_workspace_recovery_retention_pins WHERE recovery_id=$1",
        UUID(doc["operation_id"]),
    )
    assert own["released_at"] is not None and own["controller_released_at"] is not None
    foreign = await app_pg.fetchrow(
        "SELECT released_at,controller_released_at FROM vm_workspace_recovery_retention_pins WHERE recovery_id=$1",
        foreign_recovery,
    )
    assert foreign["released_at"] is None and foreign["controller_released_at"] is None


@pytest.mark.asyncio
@pytest.mark.parametrize("reply", ["lost", "wrong_uid"])
async def test_cleanup_holds_purge_without_exact_controller_pin_ack(app_pg, reply):
    doc, scenario, pin_uid = await _pinned_fixture(app_pg)

    async def unacknowledged(command):
        if reply == "lost":
            raise TimeoutError("response lost")
        return {
            "state": "released",
            "recovery_id": str(command.recovery_id),
            "pvc_uid": str(command.pvc_uid),
            "provision_generation": str(command.provision_generation),
            "pin_uid": str(UUID(int=934)),
            "resource_version": "1",
        }

    scenario.provisioner = type(
        "Observer",
        (),
        {"reconcile_workspace_recovery_pin": staticmethod(unacknowledged)},
    )()
    with pytest.raises(AcceptanceFailure, match="controller retention pin"):
        await scenario.cleanup()
    scenario._purge_fixture.assert_not_awaited()
    pin = await app_pg.fetchrow(
        "SELECT released_at,controller_released_at,controller_pin_uid FROM vm_workspace_recovery_retention_pins WHERE recovery_id=$1",
        UUID(doc["operation_id"]),
    )
    assert pin["released_at"] is not None
    assert pin["controller_released_at"] is None
    assert pin["controller_pin_uid"] == pin_uid


@pytest.mark.asyncio
async def test_cleanup_replays_pending_controller_pin_release_then_purges(app_pg):
    doc, scenario, pin_uid = await _pinned_fixture(app_pg)
    seen = 0

    async def lose_then_ack(command):
        nonlocal seen
        seen += 1
        if seen == 1:
            raise TimeoutError("response lost")
        return {
            "state": "released",
            "recovery_id": str(command.recovery_id),
            "pvc_uid": str(command.pvc_uid),
            "provision_generation": str(command.provision_generation),
            "pin_uid": pin_uid,
            "resource_version": "1",
        }

    scenario.provisioner = type(
        "Observer",
        (),
        {"reconcile_workspace_recovery_pin": staticmethod(lose_then_ack)},
    )()
    with pytest.raises(AcceptanceFailure, match="controller retention pin"):
        await scenario.cleanup()
    scenario._purge_fixture.assert_not_awaited()
    await scenario.cleanup()
    assert seen == 2
    scenario._purge_fixture.assert_awaited_once_with(UUID(doc["job_id"]))


@pytest.mark.asyncio
async def test_cleanup_refuses_foreign_source_pin_without_releasing_any_pin(app_pg):
    doc, scenario, _ = await _pinned_fixture(app_pg)
    async with app_pg.acquire() as conn:
        await conn.execute(
            "UPDATE vm_workspace_recoveries SET phase='recovered',"
            "resolved_at=clock_timestamp() WHERE id=$1",
            UUID(doc["operation_id"]),
        )
        await conn.execute(
            "UPDATE vm_workspace_recovery_jobs SET participation='released',"
            "resolved_at=clock_timestamp() WHERE recovery_id=$1",
            UUID(doc["operation_id"]),
        )
        await conn.execute(
            "UPDATE vm_workspace_recovery_retention_pins SET "
            "released_at=clock_timestamp() WHERE recovery_id=$1",
            UUID(doc["operation_id"]),
        )
    foreign_recovery = await insert_recovery(
        app_pg,
        owner_id=UUID(doc["job_id"]),
        original_cause={"gate": "another-run"},
    )
    async with app_pg.acquire() as conn:
        foreign = await conn.fetchrow(
            "SELECT root_pvc_uid,provision_generation FROM vm_workspace_recoveries WHERE id=$1",
            foreign_recovery,
        )
        await conn.execute(
            "INSERT INTO vm_workspace_recovery_jobs(recovery_id,job_id,"
            "prior_queue_state,prior_job_status) VALUES($1,$2,'non_worker','processing')",
            foreign_recovery,
            UUID(doc["job_id"]),
        )
        await conn.execute(
            "INSERT INTO vm_workspace_recovery_retention_pins "
            "(recovery_id,pvc_uid,provision_generation,controller_pinned_at,"
            "controller_pin_uid,controller_pin_resource_version) "
            "VALUES($1,$2,$3,clock_timestamp(),$4,'1')",
            foreign_recovery,
            foreign["root_pvc_uid"],
            foreign["provision_generation"],
            str(UUID(int=935)),
        )
    with pytest.raises(
        AcceptanceFailure, match="fixture controller retention pin changed"
    ):
        await scenario.cleanup()
    scenario._purge_fixture.assert_not_awaited()
    async with app_pg.acquire() as conn:
        rows = await conn.fetch(
            "SELECT r.id,r.phase,p.released_at FROM vm_workspace_recoveries r "
            "JOIN vm_workspace_recovery_retention_pins p ON p.recovery_id=r.id "
            "WHERE r.id=ANY($1::uuid[]) ORDER BY r.id",
            [UUID(doc["operation_id"]), foreign_recovery],
        )
    assert len(rows) == 2
    by_id = {row["id"]: row for row in rows}
    assert by_id[UUID(doc["operation_id"])]["phase"] == "recovered"
    assert by_id[UUID(doc["operation_id"])]["released_at"] is not None
    assert by_id[foreign_recovery]["phase"] == "recovering"
    assert by_id[foreign_recovery]["released_at"] is None


@pytest.mark.asyncio
async def test_cleanup_refuses_foreign_unpinned_participant_before_any_write(app_pg):
    doc, scenario, pin_uid = await _pinned_fixture(app_pg)

    async def acknowledge(command):
        return {
            "state": "released",
            "recovery_id": str(command.recovery_id),
            "pvc_uid": str(command.pvc_uid),
            "provision_generation": str(command.provision_generation),
            "pin_uid": pin_uid,
            "resource_version": "1",
        }

    scenario.provisioner = type(
        "Observer", (), {"reconcile_workspace_recovery_pin": staticmethod(acknowledge)}
    )()
    async with app_pg.acquire() as conn:
        await conn.execute(
            "UPDATE vm_workspace_recoveries SET phase='recovered',"
            "resolved_at=clock_timestamp() WHERE id=$1",
            UUID(doc["operation_id"]),
        )
        await conn.execute(
            "UPDATE vm_workspace_recovery_jobs SET participation='released',"
            "resolved_at=clock_timestamp() WHERE recovery_id=$1",
            UUID(doc["operation_id"]),
        )
    foreign_recovery = await insert_recovery(
        app_pg,
        owner_id=UUID(doc["job_id"]),
        original_cause={"gate": "another-run"},
    )
    async with app_pg.acquire() as conn:
        await conn.execute(
            "INSERT INTO vm_workspace_recovery_jobs(recovery_id,job_id,"
            "prior_queue_state,prior_job_status) VALUES($1,$2,'non_worker','processing')",
            foreign_recovery,
            UUID(doc["job_id"]),
        )
    with pytest.raises(
        AcceptanceFailure, match="fixture controller retention pin changed"
    ):
        await scenario.cleanup()
    scenario._purge_fixture.assert_not_awaited()
    assert (
        await app_pg.fetchval(
            "SELECT phase FROM vm_workspace_recoveries WHERE id=$1", foreign_recovery
        )
        == "recovering"
    )
    assert (
        await app_pg.fetchval(
            "SELECT released_at FROM vm_workspace_recovery_retention_pins WHERE recovery_id=$1",
            UUID(doc["operation_id"]),
        )
        is None
    )


@pytest.mark.asyncio
async def test_cleanup_refuses_changed_fixture_pvc_before_release(app_pg):
    doc, scenario, _ = await _pinned_fixture(app_pg)
    async with app_pg.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET context=jsonb_set(context,$2::text[],$3::jsonb) "
            "WHERE id=$1",
            UUID(doc["job_id"]),
            ["vm", "rootdisk_pvc_uid"],
            json.dumps(str(UUID(int=936))),
        )
    with pytest.raises(
        AcceptanceFailure, match="fixture controller retention pin changed"
    ):
        await scenario.cleanup()
    scenario._purge_fixture.assert_not_awaited()
    assert (
        await app_pg.fetchval(
            "SELECT released_at FROM vm_workspace_recovery_retention_pins WHERE recovery_id=$1",
            UUID(doc["operation_id"]),
        )
        is None
    )


@pytest.mark.asyncio
async def test_cleanup_refuses_missing_fixture_pin_before_any_recovery_write(app_pg):
    doc, scenario, _ = await _pinned_fixture(app_pg)
    async with app_pg.acquire() as conn:
        await conn.execute(
            "DELETE FROM vm_workspace_recovery_retention_pins WHERE recovery_id=$1",
            UUID(doc["operation_id"]),
        )
    with pytest.raises(
        AcceptanceFailure, match="fixture controller retention pin changed"
    ):
        await scenario.cleanup()
    scenario._purge_fixture.assert_not_awaited()
    assert (
        await app_pg.fetchval(
            "SELECT phase FROM vm_workspace_recoveries WHERE id=$1",
            UUID(doc["operation_id"]),
        )
        == "recovering"
    )
    assert (
        await app_pg.fetchval(
            "SELECT participation FROM vm_workspace_recovery_jobs WHERE recovery_id=$1",
            UUID(doc["operation_id"]),
        )
        == "held"
    )


@pytest.mark.asyncio
async def test_cleanup_preserves_recovered_operation_while_releasing_attention_pin(
    app_pg,
):
    doc, scenario, _ = await _pinned_fixture(app_pg)
    recovered_id = UUID(doc["operation_id"])
    attention_uid = str(UUID(int=937))
    async with app_pg.acquire() as conn:
        await conn.execute(
            "UPDATE vm_workspace_recoveries SET phase='recovered',"
            "resolved_at=clock_timestamp() WHERE id=$1",
            recovered_id,
        )
        await conn.execute(
            "UPDATE vm_workspace_recovery_jobs SET participation='released',"
            "resolved_at=clock_timestamp() WHERE recovery_id=$1",
            recovered_id,
        )
        await conn.execute(
            "UPDATE vm_workspace_recovery_retention_pins SET "
            "released_at=clock_timestamp(),"
            "controller_release_requested_at=clock_timestamp(),"
            "controller_released_at=clock_timestamp() WHERE recovery_id=$1",
            recovered_id,
        )
        before_recovery = dict(
            await conn.fetchrow(
                "SELECT * FROM vm_workspace_recoveries WHERE id=$1",
                recovered_id,
            )
        )
        before_participant = dict(
            await conn.fetchrow(
                "SELECT * FROM vm_workspace_recovery_jobs WHERE recovery_id=$1",
                recovered_id,
            )
        )
        before_pin = dict(
            await conn.fetchrow(
                "SELECT * FROM vm_workspace_recovery_retention_pins WHERE recovery_id=$1",
                recovered_id,
            )
        )
        started = await conn.fetchval("SELECT clock_timestamp()")
        attention_id = await conn.fetchval(
            "INSERT INTO vm_workspace_recoveries "
            "(owner_kind,owner_id,workspace_contract_digest,provision_generation,"
            "cluster_name,namespace,vm_uid,prior_vmi_uid,prior_launcher_uid,"
            "root_pvc_uid,phase,first_observed_at,deadline_at,next_check_at,"
            "reason_code,original_cause) "
            "SELECT owner_kind,owner_id,workspace_contract_digest,provision_generation,"
            "cluster_name,namespace,vm_uid,prior_vmi_uid,prior_launcher_uid,"
            "root_pvc_uid,'paused_attention',$2::timestamptz,"
            "$2::timestamptz+interval '15 minutes',$2::timestamptz,"
            "'prior_runtime_unfenced',original_cause "
            "FROM vm_workspace_recoveries WHERE id=$1 RETURNING id",
            recovered_id,
            started,
        )
        await conn.execute(
            "INSERT INTO vm_workspace_recovery_jobs(recovery_id,job_id,"
            "prior_queue_state,prior_job_status) VALUES($1,$2,'non_worker','paused')",
            attention_id,
            UUID(doc["job_id"]),
        )
        await conn.execute(
            "INSERT INTO vm_workspace_recovery_retention_pins "
            "(recovery_id,pvc_uid,provision_generation,controller_pinned_at,"
            "controller_pin_uid,controller_pin_resource_version) "
            "VALUES($1,$2,$3,clock_timestamp(),$4,'2')",
            attention_id,
            UUID(doc["pvc_uid"]),
            UUID(doc["generation"]),
            attention_uid,
        )

    async def acknowledge(command):
        assert command.recovery_id == attention_id
        return {
            "state": "released",
            "recovery_id": str(command.recovery_id),
            "pvc_uid": str(command.pvc_uid),
            "provision_generation": str(command.provision_generation),
            "pin_uid": attention_uid,
            "resource_version": "2",
        }

    scenario.provisioner = type(
        "Observer", (), {"reconcile_workspace_recovery_pin": staticmethod(acknowledge)}
    )()
    await scenario.cleanup()
    scenario._purge_fixture.assert_awaited_once_with(UUID(doc["job_id"]))
    async with app_pg.acquire() as conn:
        assert (
            dict(
                await conn.fetchrow(
                    "SELECT * FROM vm_workspace_recoveries WHERE id=$1",
                    recovered_id,
                )
            )
            == before_recovery
        )
        assert (
            dict(
                await conn.fetchrow(
                    "SELECT * FROM vm_workspace_recovery_jobs WHERE recovery_id=$1",
                    recovered_id,
                )
            )
            == before_participant
        )
        assert (
            dict(
                await conn.fetchrow(
                    "SELECT * FROM vm_workspace_recovery_retention_pins WHERE recovery_id=$1",
                    recovered_id,
                )
            )
            == before_pin
        )
        attention = await conn.fetchrow(
            "SELECT r.phase,r.resolved_at,p.released_at,p.controller_released_at "
            "FROM vm_workspace_recoveries r JOIN vm_workspace_recovery_retention_pins p "
            "ON p.recovery_id=r.id WHERE r.id=$1",
            attention_id,
        )
    assert attention["phase"] == "cancelled"
    assert all(
        attention[field] is not None
        for field in ("resolved_at", "released_at", "controller_released_at")
    )
