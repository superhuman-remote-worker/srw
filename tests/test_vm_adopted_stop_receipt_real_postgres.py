"""Adopted never-Ready VM cleanup SQL parity on the full migration chain.

External authenticated inventory/stop attestations are simulated; these tests
exercise real pre-Ready binding, permit completion, stop receipt, and charge SQL.
"""

from __future__ import annotations
from copy import deepcopy
from datetime import datetime, timezone
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID, uuid4
import asyncpg
import pytest
import pytest_asyncio
from orchestrator.database.migrate import run_migrations
from orchestrator.services.vm_creation_retry_store import VMCreationRetryStore
from orchestrator.services.vm_pre_ssh_stop_store import VMPreSSHStopStore
from orchestrator.services.vm_provisioner import VMTeardownIdentity
from orchestrator.services.vm_provisioning_phases import VMProvisioningPhaseStore
from orchestrator.services.vm_workspace_recovery_store import (
    VMWorkspaceRecoveryStore,
    acquire_vm_cleanup_permit,
    complete_vm_cleanup_permit,
)
from tests.test_vm_pre_ssh_stop_real_postgres import terminal_proof
from tests.test_vm_provisioning_phases import running
from tests.test_vm_resource_inventory_real_postgres import publish
from tests.test_vm_resource_whole_store_real_postgres import (
    _base_db,  # noqa: F401
    _db_fixture,  # noqa: F401
    pg_dsn,  # noqa: F401
    postgres_db_fixture,  # noqa: F401
    whole_schema,  # noqa: F401
    environment,
    waiter,
)


@pytest_asyncio.fixture(scope="module")
async def _schema_applied(pg_dsn):  # noqa: F811
    pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=4)
    try:
        await run_migrations(
            pool,
            Path(__file__).resolve().parents[1]
            / "src/orchestrator/database/migrations/app",
        )
        yield
    finally:
        await pool.close()


@pytest_asyncio.fixture
async def db(whole_schema, _db_fixture):  # noqa: F811
    yield _db_fixture


async def seeded_stop(db, *, pod_shape="absent"):
    policy, inventory, original, demand = await environment(db, installation_count=2)
    retry = await waiter(db, policy, inventory, lane="stateless", user_id=uuid4())
    admitted = await policy.admit(request_id=str(retry["request_id"]))
    assert admitted["action"] == "admitted"
    claim = (await VMCreationRetryStore(db).claim_due(limit=1))[0]
    job_id, generation = str(claim["job_id"]), str(claim["provision_generation"])
    vm_uid, vmi_uid, launcher_uid, pvc_uid = (str(uuid4()) for _ in range(4))
    await VMCreationRetryStore(db).authorize_controller(
        request_id=str(retry["request_id"]),
        claim_token=str(claim["claim_token"]),
        observed={
            "job_id": job_id,
            "provision_generation": generation,
            "request_digest": claim["request_digest"],
            "controller_configuration_digest": claim["controller_configuration_digest"],
            "expected_pvc_uid": None,
        },
    )
    await db.execute(
        "UPDATE vm_creation_retries SET state='succeeded',reason='creation_adopted',"
        "boot_counted=TRUE,revision=revision+1,observed_vm_uid=$2,"
        "observed_pvc_uid=$3,resolved_at=clock_timestamp(),"
        "claim_token=NULL,claim_expires_at=NULL WHERE request_id=$1",
        retry["request_id"],
        UUID(vm_uid),
        UUID(pvc_uid),
    )
    context = json.loads(
        await db.fetchval("SELECT context FROM jobs WHERE id=$1", UUID(job_id))
    )
    context["vm"].update(
        status="created",
        provision_generation=generation,
        vm_uid=vm_uid,
        vmi_uid=vmi_uid,
        rootdisk_pvc_uid=pvc_uid,
        creation_request_id=str(retry["request_id"]),
    )
    context["_vm_creation_pending"] = str(retry["request_id"])
    if pod_shape == "null":
        context["vm"]["active_pod_uid"] = None
    await db.execute(
        "UPDATE jobs SET context=$2::jsonb WHERE id=$1",
        UUID(job_id),
        json.dumps(context),
    )
    # Signed runtime inventory + actual phase observation capture physical IDs
    # before SSH/Ready without manufacturing an owner Pod binding.
    sample = deepcopy(original)
    sample["vms"] = [
        {
            "uid": vm_uid,
            "name": "agent-vm-" + job_id,
            "owner_kind": "job",
            "owner_id": job_id,
            "provision_generation": generation,
            "deleting": False,
        }
    ]
    sample["vmis"] = [
        {
            "uid": vmi_uid,
            "name": "agent-vm-" + job_id,
            "vm_uid": vm_uid,
            "node_uid": admitted["node_uid"],
            "node_name": admitted["node_name"],
            "phase": "Running",
            "deleting": False,
        }
    ]
    sample["pods"] = [
        {
            "uid": launcher_uid,
            "namespace": "workers",
            "name": "virt-launcher-owned",
            "node_uid": admitted["node_uid"],
            "node_name": admitted["node_name"],
            "terminal": False,
            "deleting": False,
            "requests": demand.to_six_dict(),
            "vmi_uid": vmi_uid,
            "reservation_id": admitted["reservation_id"],
            "provision_generation": generation,
        }
    ]
    sample["snapshot_id"] = str(uuid4())
    sample["sequence"] += 1
    sample["started_at"] = sample["finished_at"] = datetime.now(
        timezone.utc
    ).isoformat()
    await publish(inventory, sample)
    phases = VMProvisioningPhaseStore(db)
    token = await phases.capture(job_id, generation)
    observed = running(
        owner_id=job_id,
        namespace="workers",
        provision_generation=generation,
        vm_uid=vm_uid,
        vmi_uid=vmi_uid,
        rootdisk_pvc_uid=pvc_uid,
    )
    assert (
        await phases.apply_status(
            token,
            {
                "provision_generation": generation,
                "vm_uid": vm_uid,
                "vmi_uid": vmi_uid,
                "rootdisk_pvc_uid": pvc_uid,
                "namespace": "workers",
                "vm_name": "agent-vm-" + job_id,
                "provisioning": observed,
            },
        )
        == "observed"
    )
    owner = json.loads(
        await db.fetchval("SELECT context FROM jobs WHERE id=$1", UUID(job_id))
    )
    assert owner["vm"].get("active_pod_uid") == context["vm"].get("active_pod_uid")
    assert owner["vm"].get("ssh_verified_at") is None
    assert owner["_vm_creation_pending"] == str(retry["request_id"])
    charge = await db.fetchrow(
        "SELECT state,vm_uid,vmi_uid,launcher_uid FROM vm_resource_reservations WHERE id=$1",
        UUID(admitted["reservation_id"]),
    )
    assert charge["state"] == "active"
    assert tuple(str(charge[k]) for k in ("vm_uid", "vmi_uid", "launcher_uid")) == (
        vm_uid,
        vmi_uid,
        launcher_uid,
    )
    await db.execute(
        "UPDATE vm_workspace_cleanup_admissions SET completed_at=clock_timestamp(),"
        "outcome='adopted' WHERE id=(SELECT creation_admission_id "
        "FROM vm_creation_retries WHERE request_id=$1)",
        retry["request_id"],
    )
    permit = await acquire_vm_cleanup_permit(
        VMWorkspaceRecoveryStore(db),
        owner_kind="job",
        owner_id=job_id,
        identity=VMTeardownIdentity(generation, vm_uid, pvc_uid),
        source="dispatcher_vm_recycle",
        purge_disk=False,
    )
    assert permit.allowed and permit.parent_cleanup is not None
    await db.execute(
        "UPDATE jobs SET context=jsonb_set(context,'{vm,status}',"
        "'\"retiring_process_zero\"') WHERE id=$1",
        UUID(job_id),
    )
    frozen = {
        "kind": "vm_pre_ssh_stop_candidate_v1",
        "job_id": job_id,
        "provision_generation": generation,
        "namespace": inventory.namespace,
        "vm_name": "agent-vm-" + job_id,
        "vm_uid": vm_uid,
        "vmi_uid": vmi_uid,
        "launcher_name": "virt-launcher-owned",
        "launcher_uid": launcher_uid,
        "pvc_uid": pvc_uid,
        "node_name": admitted["node_name"],
        "node_uid": admitted["node_uid"],
        "vm_resource_version": "42",
        "vm_generation": 7,
        "launcher_resource_version": "43",
        "containers": [
            {
                "kind": "regular",
                "name": "compute",
                "container_id": "containerd://compute",
            },
            {
                "kind": "init",
                "name": "guest-console-log",
                "container_id": "containerd://console",
            },
        ],
    }
    return {
        "job_id": job_id,
        "generation": generation,
        "permit": permit.parent_cleanup,
        "cleanup_permit": permit,
        "frozen": frozen,
        "reservation_id": admitted["reservation_id"],
        "store": VMPreSSHStopStore(db),
        "request_id": retry["request_id"],
    }


def absence(state):
    f = state["frozen"]
    return {
        "version": 1,
        "kind": "vm_cleanup_physical_stop",
        "job_id": state["job_id"],
        "provision_generation": state["generation"],
        **{k: f[k] for k in ("vm_uid", "vmi_uid", "launcher_uid", "pvc_uid")},
        "vm_absent": True,
        "vmi_absent": True,
        "launcher_absent": True,
        "same_generation_replacement": False,
        "pvc_disposition": "retained",
        "controller_authenticated": True,
    }


async def positive_stop(state):
    intent = await state["store"].admit_intent(
        state["job_id"], state["generation"], state["permit"], state["frozen"]
    )
    return await state["store"].commit_positive_proof(
        state["job_id"],
        state["generation"],
        state["permit"],
        terminal_proof(state["frozen"], intent["frozen_digest"]),
    )


async def durable_state(db, state):
    return {
        "cleanup": dict(
            await db.fetchrow(
                "SELECT * FROM vm_workspace_cleanup_admissions WHERE id=$1",
                state["cleanup_permit"].admission_id,
            )
        ),
        "charge": dict(
            await db.fetchrow(
                "SELECT * FROM vm_resource_reservations WHERE id=$1",
                UUID(state["reservation_id"]),
            )
        ),
        "waiter": dict(
            await db.fetchrow(
                "SELECT * FROM vm_resource_waiters WHERE request_id=$1",
                state["request_id"],
            )
        ),
        "receipts": [
            dict(r)
            for r in await db.fetch(
                "SELECT * FROM vm_resource_cleanup_stop_receipts WHERE cleanup_admission_id=$1",
                state["cleanup_permit"].admission_id,
            )
        ],
        "proofs": await db.fetchval(
            "SELECT count(*) FROM vm_pre_ssh_stop_proofs WHERE job_id=$1",
            UUID(state["job_id"]),
        ),
        "zeros": await db.fetchval(
            "SELECT count(*) FROM managed_repository_process_zero_receipts WHERE owner_id=$1",
            UUID(state["job_id"]),
        ),
        "context": await db.fetchval(
            "SELECT context FROM jobs WHERE id=$1", UUID(state["job_id"])
        ),
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("pod_shape", ["absent", "null"])
async def test_ordinary_completion_releases_genuine_pre_ready_charge_atomically(
    db, pod_shape
):
    state = await seeded_stop(db, pod_shape=pod_shape)
    await positive_stop(state)
    before = await durable_state(db, state)
    provisioner = SimpleNamespace(
        attest_vm_cleanup_stop=AsyncMock(return_value=absence(state))
    )
    try:
        await complete_vm_cleanup_permit(
            VMWorkspaceRecoveryStore(db),
            state["cleanup_permit"],
            outcome="completed",
            provisioner=provisioner,
        )
    except asyncpg.CheckViolationError:
        # RED must reproduce the live SQL literal and roll back the *whole*
        # ordinary completion, while preserving its existing positive proof.
        assert await durable_state(db, state) == before
        raise
    after = await durable_state(db, state)
    assert after["cleanup"]["outcome"] == "completed"
    assert after["cleanup"]["completed_at"] is not None
    assert after["charge"]["state"] == after["waiter"]["state"] == "released"
    assert len(after["receipts"]) == after["proofs"] == after["zeros"] == 1
    assert after["context"] == before["context"]
    assert (
        await db.fetchval(
            "SELECT ready_at FROM vm_creation_retries WHERE request_id=$1",
            state["request_id"],
        )
        is None
    )
    provisioner.attest_vm_cleanup_stop.assert_awaited_once()
    await complete_vm_cleanup_permit(
        VMWorkspaceRecoveryStore(db),
        state["cleanup_permit"],
        outcome="completed",
        provisioner=provisioner,
    )
    assert await durable_state(db, state) == after
    provisioner.attest_vm_cleanup_stop.assert_awaited_once()


async def insert_stop(conn, state, proof=None, changes=None):
    f = state["frozen"]
    row = {
        "cleanup_admission_id": state["cleanup_permit"].admission_id,
        "reservation_id": UUID(state["reservation_id"]),
        "request_id": state["request_id"],
        "job_id": UUID(state["job_id"]),
        "provision_generation": UUID(state["generation"]),
        **{k: UUID(f[k]) for k in ("vm_uid", "vmi_uid", "launcher_uid", "pvc_uid")},
        "intent_digest": state["permit"]["intent_digest"],
    }
    row.update(changes or {})
    await conn.execute(
        "INSERT INTO vm_resource_cleanup_stop_receipts "
        "(cleanup_admission_id,reservation_id,request_id,job_id,provision_generation,"
        "vm_uid,vmi_uid,launcher_uid,pvc_uid,intent_digest,stop_evidence) "
        "VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb)",
        *row.values(),
        json.dumps(proof or absence(state)),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case",
    [
        "owner_pod",
        "ready",
        "reason",
        "null_reason",
        "successor",
        "job_id",
        "generation",
        "vm_uid",
        "vmi_uid",
        "launcher_uid",
        "pvc_uid",
        "request_id",
        "reservation_id",
        "intent_digest",
        "owner_vm",
        "owner_vmi",
        "owner_pvc",
        "owner_generation",
        "parent_open",
        "missing_zero",
        "vm_present",
        "vmi_present",
        "launcher_present",
        "unauthenticated",
        "replacement",
        "wrong_disposition",
        "wrong_kind",
    ],
)
async def test_direct_sql_cannot_expand_nullable_pod_exception(db, case):
    state = await seeded_stop(db)
    await positive_stop(state)
    before = await durable_state(db, state)
    proof, changes = absence(state), {}
    async with db.acquire() as conn:
        tx = conn.transaction()
        await tx.start()
        try:
            if case != "parent_open":
                await conn.execute(
                    "UPDATE vm_workspace_cleanup_admissions SET completed_at=clock_timestamp(),outcome='completed' WHERE id=$1",
                    state["cleanup_permit"].admission_id,
                )
            if case == "owner_pod" or case.startswith("owner_"):
                field = {
                    "owner_pod": "active_pod_uid",
                    "owner_vm": "vm_uid",
                    "owner_vmi": "vmi_uid",
                    "owner_pvc": "rootdisk_pvc_uid",
                    "owner_generation": "provision_generation",
                }[case]
                await conn.execute(
                    "UPDATE jobs SET context=jsonb_set(context,ARRAY['vm',$2],to_jsonb($3::text)) WHERE id=$1",
                    UUID(state["job_id"]),
                    field,
                    str(uuid4()),
                )
            elif case in {
                "ready",
                "reason",
                "null_reason",
                "successor",
                "missing_zero",
            }:
                # Model hostile/stale durable boundary rows without invoking
                # unrelated setup guards. Only the tested receipt INSERT runs
                # with production triggers enabled; all setup rolls back.
                await conn.execute("SET LOCAL session_replication_role=replica")
                if case == "ready":
                    await conn.execute(
                        "UPDATE vm_creation_retries SET ready_at=clock_timestamp() WHERE request_id=$1",
                        state["request_id"],
                    )
                elif case in {"reason", "null_reason"}:
                    await conn.execute(
                        "UPDATE vm_creation_retries SET reason=$2 WHERE request_id=$1",
                        state["request_id"],
                        "other" if case == "reason" else None,
                    )
                elif case == "missing_zero":
                    await conn.execute(
                        "DELETE FROM managed_repository_process_zero_receipts WHERE owner_id=$1",
                        UUID(state["job_id"]),
                    )
                else:
                    successor_vmi, successor_pod = uuid4(), uuid4()
                    await conn.execute(
                        "INSERT INTO vm_resource_recovery_successors "
                        "(recovery_id,reservation_id,ordinal,owner_id,provision_generation,vm_uid,root_pvc_uid,prior_vmi_uid,prior_launcher_uid,successor_vmi_uid,successor_launcher_uid,stop_receipt_digest,final_attestation_digest) "
                        "VALUES($1,$2,1,$3,$4,$5,$6,$7,$8,$9,$10,$11,$11)",
                        uuid4(),
                        UUID(state["reservation_id"]),
                        UUID(state["job_id"]),
                        UUID(state["generation"]),
                        UUID(state["frozen"]["vm_uid"]),
                        UUID(state["frozen"]["pvc_uid"]),
                        UUID(state["frozen"]["vmi_uid"]),
                        UUID(state["frozen"]["launcher_uid"]),
                        successor_vmi,
                        successor_pod,
                        "sha256:" + "f" * 64,
                    )
                    await conn.execute(
                        "UPDATE jobs SET context=jsonb_set(context,'{vm,vmi_uid}',to_jsonb($2::text)) WHERE id=$1",
                        UUID(state["job_id"]),
                        str(successor_vmi),
                    )
                    changes.update(vmi_uid=successor_vmi, launcher_uid=successor_pod)
                    proof.update(
                        vmi_uid=str(successor_vmi), launcher_uid=str(successor_pod)
                    )
                await conn.execute("SET LOCAL session_replication_role=origin")
            elif case in {
                "job_id",
                "vm_uid",
                "vmi_uid",
                "launcher_uid",
                "pvc_uid",
                "request_id",
                "reservation_id",
                "generation",
            }:
                key = "provision_generation" if case == "generation" else case
                changes[key] = uuid4()
                if key in proof:
                    proof[key] = str(changes[key])
            elif case == "intent_digest":
                changes["intent_digest"] = "sha256:" + "b" * 64
            elif case != "parent_open":
                key, value = {
                    "vm_present": ("vm_absent", False),
                    "vmi_present": ("vmi_absent", False),
                    "launcher_present": ("launcher_absent", False),
                    "unauthenticated": ("controller_authenticated", False),
                    "replacement": ("same_generation_replacement", True),
                    "wrong_disposition": ("pvc_disposition", "unknown"),
                    "wrong_kind": ("kind", "logical_delete"),
                }[case]
                proof[key] = value
            message = (
                "retired Job VM owner cannot acquire obligations"
                if case == "job_id"
                else "VM resource cleanup stop proof changed"
            )
            with pytest.raises(asyncpg.CheckViolationError, match=message):
                await insert_stop(conn, state, proof, changes)
        finally:
            await tx.rollback()
    assert await durable_state(db, state) == before


@pytest.mark.asyncio
async def test_release_failure_rolls_back_parent_receipt_charge_and_waiter(db):
    state = await seeded_stop(db)
    await positive_stop(state)
    before = await durable_state(db, state)
    provisioner = SimpleNamespace(
        attest_vm_cleanup_stop=AsyncMock(return_value=absence(state))
    )
    await db.execute(
        "CREATE FUNCTION public.test_adopted_release_refusal() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.state='released' THEN RAISE EXCEPTION 'injected adopted release refusal' USING ERRCODE='23514'; END IF; RETURN NEW; END $$"
    )
    await db.execute(
        "CREATE TRIGGER z_test_adopted_release_refusal BEFORE UPDATE ON vm_resource_reservations FOR EACH ROW EXECUTE FUNCTION public.test_adopted_release_refusal()"
    )
    try:
        with pytest.raises(
            asyncpg.CheckViolationError, match="injected adopted release refusal"
        ):
            await complete_vm_cleanup_permit(
                VMWorkspaceRecoveryStore(db),
                state["cleanup_permit"],
                outcome="completed",
                provisioner=provisioner,
            )
    finally:
        await db.execute(
            "DROP TRIGGER z_test_adopted_release_refusal ON vm_resource_reservations"
        )
        await db.execute("DROP FUNCTION public.test_adopted_release_refusal()")
    assert await durable_state(db, state) == before
    await complete_vm_cleanup_permit(
        VMWorkspaceRecoveryStore(db),
        state["cleanup_permit"],
        outcome="completed",
        provisioner=provisioner,
    )
    assert (await durable_state(db, state))["charge"]["state"] == "released"


@pytest.mark.asyncio
async def test_changed_binding_during_external_probe_cannot_commit(db):
    from shared.vm_resource_admission import ResourceAdmissionError

    state = await seeded_stop(db)
    await positive_stop(state)

    async def changed(_candidate):
        await db.execute(
            "UPDATE jobs SET context=jsonb_set(context,'{vm,active_pod_uid}',to_jsonb($2::text)) WHERE id=$1",
            UUID(state["job_id"]),
            str(uuid4()),
        )
        return absence(state)

    provisioner = SimpleNamespace(attest_vm_cleanup_stop=AsyncMock(side_effect=changed))
    with pytest.raises(
        ResourceAdmissionError, match="resource_cleanup_successor_changed"
    ):
        await complete_vm_cleanup_permit(
            VMWorkspaceRecoveryStore(db),
            state["cleanup_permit"],
            outcome="completed",
            provisioner=provisioner,
        )
    after = await durable_state(db, state)
    assert after["cleanup"]["completed_at"] is None
    assert after["charge"]["state"] == "teardown"
    assert after["waiter"]["state"] == "admitted"
    assert after["receipts"] == []
    assert after["proofs"] == after["zeros"] == 1


@pytest.mark.asyncio
async def test_forward_0336_upgrade_releases_existing_held_charge_without_rewriting_history(
    pg_dsn,  # noqa: F811
    tmp_path,
):
    from urllib.parse import urlsplit, urlunsplit
    from orchestrator.database.migrate import discover
    from orchestrator.database.postgres import PostgresDB

    migrations = (
        Path(__file__).resolve().parents[1] / "src/orchestrator/database/migrations/app"
    )
    prior = tmp_path / "through-0335"
    prior.mkdir()
    for path in discover(migrations):
        if path.name.startswith("0336_"):
            continue
        (prior / path.name).write_bytes(path.read_bytes())
    name = "adopted_stop_upgrade_" + uuid4().hex[:12]
    admin = await asyncpg.connect(pg_dsn)
    await admin.execute(f'CREATE DATABASE "{name}"')
    parts = urlsplit(pg_dsn)
    dsn = urlunsplit(parts._replace(path="/" + name))
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=4)
    db = PostgresDB(connection_string=dsn, min_connections=1, max_connections=4)
    try:
        await run_migrations(pool, prior)
        await db.connect()
        old_ledger = [
            dict(r)
            for r in await db.fetch("SELECT * FROM schema_migrations ORDER BY filename")
        ]
        state = await seeded_stop(db)
        await positive_stop(state)
        held = await durable_state(db, state)
        provisioner = SimpleNamespace(
            attest_vm_cleanup_stop=AsyncMock(return_value=absence(state))
        )
        with pytest.raises(
            asyncpg.CheckViolationError, match="VM resource cleanup stop proof changed"
        ):
            await complete_vm_cleanup_permit(
                VMWorkspaceRecoveryStore(db),
                state["cleanup_permit"],
                outcome="completed",
                provisioner=provisioner,
            )
        assert await durable_state(db, state) == held
        await run_migrations(pool, migrations)
        assert [
            dict(r)
            for r in await db.fetch(
                "SELECT * FROM schema_migrations WHERE filename NOT LIKE '0336_%' ORDER BY filename"
            )
        ] == old_ledger
        await complete_vm_cleanup_permit(
            VMWorkspaceRecoveryStore(db),
            state["cleanup_permit"],
            outcome="completed",
            provisioner=provisioner,
        )
        released = await durable_state(db, state)
        assert released["charge"]["state"] == released["waiter"]["state"] == "released"
        assert released["cleanup"]["outcome"] == "completed"
        assert len(released["receipts"]) == released["proofs"] == released["zeros"] == 1
        assert released["context"] == held["context"]
        ledger = [
            dict(r)
            for r in await db.fetch("SELECT * FROM schema_migrations ORDER BY filename")
        ]
        await run_migrations(pool, migrations)
        assert [
            dict(r)
            for r in await db.fetch("SELECT * FROM schema_migrations ORDER BY filename")
        ] == ledger
        await complete_vm_cleanup_permit(
            VMWorkspaceRecoveryStore(db),
            state["cleanup_permit"],
            outcome="completed",
            provisioner=provisioner,
        )
        assert await durable_state(db, state) == released
        assert provisioner.attest_vm_cleanup_stop.await_count == 2
    finally:
        await db.close()
        await pool.close()
        await admin.execute(f'DROP DATABASE "{name}"')
        await admin.close()
