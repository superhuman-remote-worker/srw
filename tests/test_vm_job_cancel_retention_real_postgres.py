"""Cancelled never-ready Jobs retain exact storage under immutable authority."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from copy import deepcopy
from datetime import datetime, timezone
import json
from pathlib import Path
from uuid import UUID, uuid4

import asyncpg
import pytest
import pytest_asyncio
from testcontainers.postgres import PostgresContainer

from orchestrator.services.vm_provisioner import VMTeardownIdentity
from orchestrator.services.vm_workspace_recovery_store import (
    VMWorkspaceRecoveryStore,
    acquire_vm_cleanup_permit,
)
from tests.test_vm_pre_ssh_stop_real_postgres import (
    _base_db,  # noqa: F401
    _db_fixture,  # noqa: F401
    _schema_applied,  # noqa: F401
    db as _pre_ssh_db,  # noqa: F401
    pre_ssh_schema,  # noqa: F401
    postgres_db_fixture,  # noqa: F401
    seeded_stop,
    terminal_proof,
    whole_schema,  # noqa: F401
)


@pytest.fixture(scope="module")
def pg_dsn():
    # Required authority qualification must fail rather than report a green skip.
    with PostgresContainer("postgres:15") as container:
        yield container.get_connection_url().replace(
            "postgresql+psycopg2", "postgresql"
        )


@pytest_asyncio.fixture(scope="module")
async def retention_schema(pg_dsn, pre_ssh_schema):  # noqa: F811
    migration = Path(__file__).resolve().parents[1] / (
        "src/orchestrator/database/migrations/app/0338_vm_job_cancel_retention.sql"
    )
    conn = await asyncpg.connect(pg_dsn)
    try:
        if migration.exists() and not await conn.fetchval(
            "SELECT to_regclass('public.vm_job_cancel_retention_authorities') IS NOT NULL"
        ):
            await conn.execute(migration.read_text())
        yield
    finally:
        await conn.close()


@pytest_asyncio.fixture
async def db(retention_schema, _pre_ssh_db):  # noqa: F811
    yield _pre_ssh_db


@pytest.fixture(autouse=True)
def enabled(monkeypatch):
    monkeypatch.setenv("VM_JOB_CANCEL_RETENTION_ENABLED", "true")


async def cancelled(db, *, old=True, binding="null", retiring=True, bound=True):
    state = await seeded_stop(
        db,
        cleanup_source="job_terminal_vm_release" if old else None,
        purge_disk=True,
        retiring=retiring,
        bound=bound,
    )
    owner = UUID(state["job_id"])
    context = json.loads(
        await db.fetchval("SELECT context FROM jobs WHERE id=$1", owner)
    )
    context["_stateless_cancel_cleanup_pending"] = True
    context.pop("_job_terminal_vm_cleanup", None)
    context["vm"].pop("active_pod_uid", None)
    if binding == "absent":
        context["vm"].pop("workspace_storage", None)
    else:
        context["vm"]["workspace_storage"] = None if binding == "null" else binding
    await db.execute(
        "UPDATE jobs SET status='cancelled',assigned_agent_id=NULL,context=$2::jsonb WHERE id=$1",
        owner,
        json.dumps(context),
    )
    await db.execute(
        "UPDATE run_queue SET state='done',leased_by=NULL,leased_until=NULL WHERE unit_id=$1",
        owner,
    )
    state["identity"] = VMTeardownIdentity(
        state["generation"], state["frozen"]["vm_uid"], state["frozen"]["pvc_uid"]
    )
    state["recovery"] = VMWorkspaceRecoveryStore(db)
    return state


async def acquire(state):
    from orchestrator.services.vm_job_cancel_retention import acquire_cancel_retention

    return await acquire_cancel_retention(
        state["recovery"], job_id=state["job_id"], identity=state["identity"]
    )


async def authority_rows(db, state):
    owner = UUID(state["job_id"])
    return await db.fetch(
        "SELECT * FROM vm_workspace_cleanup_admissions WHERE owner_kind='job' "
        "AND owner_id=$1 ORDER BY id",
        owner,
    )


async def publish_cancelled_launcher(state, *, invalid=None):
    """The guest starts after Cancel, independently of provisioning updates."""
    from tests.test_vm_resource_inventory_real_postgres import publish

    frozen = state["frozen"]
    sample = deepcopy(state["snapshot"])
    sample.update(snapshot_id=str(uuid4()), sequence=sample["sequence"] + 1)
    sample["started_at"] = sample["finished_at"] = datetime.now(
        timezone.utc
    ).isoformat()
    sample["vms"] = [
        {
            "uid": frozen["vm_uid"],
            "name": "agent-vm-" + state["job_id"],
            "owner_kind": "job",
            "owner_id": state["job_id"],
            "provision_generation": state["generation"],
            "deleting": False,
        }
    ]
    sample["vmis"] = [
        {
            "uid": frozen["vmi_uid"],
            "name": "agent-vm-" + state["job_id"],
            "vm_uid": frozen["vm_uid"],
            "node_uid": frozen["node_uid"],
            "node_name": frozen["node_name"],
            "phase": "Running",
            "deleting": False,
        }
    ]
    sample["pods"] = [
        {
            "uid": frozen["launcher_uid"],
            "namespace": frozen["namespace"],
            "name": frozen["launcher_name"],
            "node_uid": frozen["node_uid"],
            "node_name": frozen["node_name"],
            "terminal": False,
            "deleting": False,
            "requests": state["demand"].to_six_dict(),
            "vmi_uid": frozen["vmi_uid"],
            "reservation_id": state["reservation_id"],
            "provision_generation": state["generation"],
        }
    ]
    if invalid == "owner":
        sample["vms"][0]["owner_id"] = str(uuid4())
    elif invalid == "generation":
        sample["vms"][0]["provision_generation"] = str(uuid4())
    elif invalid == "reservation":
        sample["pods"][0]["reservation_id"] = str(uuid4())
    elif invalid == "duplicate_vmi":
        sample["vmis"].append(
            {**sample["vmis"][0], "uid": str(uuid4()), "name": "other-vmi"}
        )
    elif invalid == "duplicate_launcher":
        sample["pods"].append(
            {**sample["pods"][0], "uid": str(uuid4()), "name": "other-launcher"}
        )
    elif invalid == "pending":
        sample["vmis"][0]["phase"] = "Pending"
    elif invalid == "deleting":
        sample["pods"][0]["deleting"] = True
    elif invalid == "oversized":
        sample["pods"][0]["requests"]["cpu_millicores"] += 1
    await publish(state["inventory"], sample)
    return sample


@pytest.mark.asyncio
@pytest.mark.parametrize("oversized", [False, True])
@pytest.mark.parametrize("vm_status", ["created", "ssh_pending", "ssh_unreachable"])
async def test_cancel_before_guest_boot_binds_occupancy_for_retaining_cleanup(
    db, oversized, vm_status
):
    # Missing terminal observation leaves a real late-booted VM running forever.
    state = await cancelled(db, old=False, retiring=False, bound=False)
    owner = UUID(state["job_id"])
    await db.execute(
        "UPDATE jobs SET context=jsonb_set(context,'{vm,status}',to_jsonb($2::text)) "
        "WHERE id=$1",
        owner,
        vm_status,
    )
    before = json.loads(
        await db.fetchval("SELECT context FROM jobs WHERE id=$1", owner)
    )
    assert not (await acquire(state)).allowed  # No runtime has been observed yet.
    await publish_cancelled_launcher(state, invalid="oversized" if oversized else None)
    permit = await acquire(state)
    assert permit.allowed, permit.reason
    assert permit.parent_cleanup["intent"]["purge_disk"] is False
    charge = await db.fetchrow(
        "SELECT * FROM vm_resource_reservations WHERE id=$1",
        UUID(state["reservation_id"]),
    )
    # Retention admission moves the still-counted occupancy into teardown.
    assert charge["state"] == "teardown"
    assert charge["released_at"] is None
    assert charge["observed_cpu_millicores"] == state["demand"].cpu_millicores + int(
        oversized
    )
    for key in ("vm_uid", "vmi_uid", "launcher_uid"):
        assert str(charge[key]) == state["frozen"][key]
    after = await db.fetchrow("SELECT status,context FROM jobs WHERE id=$1", owner)
    context = json.loads(after["context"])
    assert after["status"] == "cancelled"
    assert context == {
        **before,
        "vm": {**before["vm"], "vmi_uid": state["frozen"]["vmi_uid"]},
    }
    assert (
        await db.fetchval(
            "SELECT ready_at FROM vm_creation_retries WHERE job_id=$1", owner
        )
        is None
    )
    assert (
        await db.fetchval("SELECT state FROM run_queue WHERE unit_id=$1", owner)
        == "done"
    )
    assert await db.fetchval("SELECT count(*) FROM vm_pre_ssh_stop_intents") == 0
    assert await acquire(state) == permit


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "invalid",
    [
        "owner",
        "generation",
        "reservation",
        "duplicate_vmi",
        "duplicate_launcher",
        "pending",
        "deleting",
    ],
)
async def test_cancelled_late_boot_requires_exact_singular_inventory(db, invalid):
    state = await cancelled(db, old=False, retiring=False, bound=False)
    await publish_cancelled_launcher(state, invalid=invalid)
    before = await authority_rows(db, state)
    assert not (await acquire(state)).allowed
    assert await authority_rows(db, state) == before
    charge = await db.fetchrow(
        "SELECT state,vm_uid,vmi_uid,launcher_uid FROM vm_resource_reservations WHERE id=$1",
        UUID(state["reservation_id"]),
    )
    assert dict(charge) == {
        "state": "reserved",
        "vm_uid": None,
        "vmi_uid": None,
        "launcher_uid": None,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "invalid",
    [
        "disabled",
        "unauthenticated",
        "known_vmi_changed",
        "prior_ssh",
        "prior_launcher",
        "request_changed",
        "partial_binding",
        "inventory_conflict",
        "inventory_incomplete",
    ],
)
async def test_cancelled_late_boot_never_overrides_prior_authority(
    db, monkeypatch, invalid
):
    state = await cancelled(db, old=False, retiring=False, bound=False)
    sample = await publish_cancelled_launcher(state)
    owner = UUID(state["job_id"])
    if invalid == "disabled":
        monkeypatch.setenv("VM_JOB_CANCEL_RETENTION_ENABLED", "false")
    elif invalid in {"inventory_conflict", "inventory_incomplete"}:
        if invalid == "inventory_conflict":
            await db.execute(
                "UPDATE vm_resource_inventory_heads SET observation_conflict=true"
            )
        else:
            from tests.test_vm_resource_inventory_real_postgres import (
                publish,
                successor,
            )

            incomplete = successor(sample, complete=False)
            incomplete["installed_profile"] = None
            await publish(state["inventory"], incomplete)
    elif invalid == "partial_binding":
        await db.execute(
            "UPDATE vm_resource_reservations SET vm_uid=$2 WHERE id=$1",
            UUID(state["reservation_id"]),
            UUID(state["frozen"]["vm_uid"]),
        )
    else:
        context = json.loads(
            await db.fetchval("SELECT context FROM jobs WHERE id=$1", owner)
        )
        if invalid == "request_changed":
            context["_vm_creation_pending"] = str(uuid4())
        else:
            field, value = {
                "unauthenticated": ("identity_authenticated", False),
                "known_vmi_changed": ("vmi_uid", str(uuid4())),
                "prior_ssh": ("ssh_verified_at", "2026-10-09T20:00:00Z"),
                "prior_launcher": ("active_pod_uid", str(uuid4())),
            }[invalid]
            context["vm"][field] = value
        await db.execute(
            "UPDATE jobs SET context=$2::jsonb WHERE id=$1", owner, json.dumps(context)
        )
    before = dict(
        await db.fetchrow(
            "SELECT * FROM vm_resource_reservations WHERE id=$1",
            UUID(state["reservation_id"]),
        )
    )
    context_before = await db.fetchval("SELECT context FROM jobs WHERE id=$1", owner)
    assert not (await acquire(state)).allowed
    assert (
        dict(
            await db.fetchrow(
                "SELECT * FROM vm_resource_reservations WHERE id=$1",
                UUID(state["reservation_id"]),
            )
        )
        == before
    )
    assert (
        await db.fetchval("SELECT context FROM jobs WHERE id=$1", owner)
        == context_before
    )
    assert (
        await db.fetchval("SELECT count(*) FROM vm_job_cancel_retention_authorities")
        == 0
    )


@pytest.mark.asyncio
async def test_fresh_cancel_retention_admits_false_parent(db):
    state = await cancelled(db, old=False)
    permit = await acquire(state)
    assert permit.allowed
    assert permit.parent_cleanup["intent"]["purge_disk"] is False
    assert permit.parent_cleanup["intent"]["source"] == "job_terminal_vm_release"
    row = await db.fetchrow(
        "SELECT * FROM vm_job_cancel_retention_authorities WHERE cleanup_admission_id=$1",
        permit.admission_id,
    )
    assert row["superseded_admission_id"] is None
    assert row["pvc_uid"] == UUID(state["frozen"]["pvc_uid"])
    assert await acquire(state) == permit
    assert not await db.fetchval(
        "SELECT context ? '_job_terminal_vm_cleanup' FROM jobs WHERE id=$1",
        UUID(state["job_id"]),
    )


@pytest.mark.asyncio
async def test_reachable_ssh_policy1_retention_uses_typed_positive_stop(
    db, monkeypatch
):
    from unittest.mock import AsyncMock

    from orchestrator.services import vm_provisioner as module
    from orchestrator.services.vm_provisioner import (
        VMProvisioner,
        VMTeardownIdentity,
        VMTeardownResult,
        _VMTeardownProbe,
    )

    state = await cancelled(db, old=False)
    permit = await acquire(state)
    assert permit.allowed
    monkeypatch.setenv("VM_MODE", "same-cluster")
    provisioner = VMProvisioner()
    provisioner._db = db
    provisioner._snapshot_service = None
    identity = VMTeardownIdentity(
        state["generation"],
        state["frozen"]["vm_uid"],
        state["frozen"]["pvc_uid"],
        ssh_host="100.64.1.9",
        ssh_port=22,
        ssh_host_key_fingerprint="SHA256:retained-test-key",
    )
    provisioner._probe_vm_teardown_identity = AsyncMock(
        return_value=_VMTeardownProbe(
            "present",
            VMTeardownIdentity(
                state["generation"],
                state["frozen"]["vm_uid"],
                state["frozen"]["pvc_uid"],
                credential_runtime_started=True,
            ),
            rootdisk_identity_known=True,
        )
    )
    retire = AsyncMock(return_value=True)
    monkeypatch.setattr(module, "retire_managed_repository_processes", retire)
    qualification = preflight(state)

    async def controller_stop(payload):
        if payload["action"] == "inspect":
            return {
                "status": "candidate",
                "frozen": state["frozen"],
                "retention_preflight": qualification,
                "_identity_authenticated": True,
            }
        if payload["action"] == "stop":
            return {
                "status": "positive_terminal_proof",
                "terminal_evidence": terminal_proof(
                    state["frozen"], payload["frozen_digest"]
                ),
                "_identity_authenticated": True,
            }
        assert payload["action"] == "release"
        return {"status": "finalizer_released", "_identity_authenticated": True}

    provisioner._request_pre_ssh_stop = AsyncMock(side_effect=controller_stop)

    async def delete_after_proof(*_args, **_kwargs):
        assert (
            await db.fetchval(
                "SELECT count(*) FROM vm_pre_ssh_stop_proofs WHERE job_id=$1",
                UUID(state["job_id"]),
            )
            == 1
        )
        return VMTeardownResult("completed", True)

    provisioner.delete_vm_captured = AsyncMock(side_effect=delete_after_proof)
    result = await provisioner.release_vm_captured(
        state["job_id"],
        identity,
        purge_disk=False,
        capture_snapshot=False,
        parent_cleanup=permit.parent_cleanup,
    )
    assert result == VMTeardownResult("completed", True)
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_pre_ssh_stop_intents WHERE job_id=$1",
            UUID(state["job_id"]),
        )
        == 1
    )
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_pre_ssh_stop_proofs WHERE job_id=$1",
            UUID(state["job_id"]),
        )
        == 1
    )
    retire.assert_not_awaited()


def _policy1_release_fixture(db, state, monkeypatch):
    from unittest.mock import AsyncMock

    from orchestrator.services import vm_provisioner as module
    from orchestrator.services.vm_provisioner import (
        VMProvisioner,
        VMTeardownIdentity,
        _VMTeardownProbe,
    )

    monkeypatch.setenv("VM_MODE", "same-cluster")
    provisioner = VMProvisioner()
    provisioner._db = db
    provisioner._snapshot_service = None
    identity = VMTeardownIdentity(
        state["generation"],
        state["frozen"]["vm_uid"],
        state["frozen"]["pvc_uid"],
        ssh_host="100.64.1.9",
        ssh_port=22,
        ssh_host_key_fingerprint="SHA256:retained-test-key",
    )
    provisioner._probe_vm_teardown_identity = AsyncMock(
        return_value=_VMTeardownProbe(
            "present",
            VMTeardownIdentity(
                state["generation"],
                state["frozen"]["vm_uid"],
                state["frozen"]["pvc_uid"],
                credential_runtime_started=True,
            ),
            rootdisk_identity_known=True,
        )
    )
    retire = AsyncMock(return_value=True)
    monkeypatch.setattr(module, "retire_managed_repository_processes", retire)
    return provisioner, identity, retire


@pytest.mark.asyncio
async def test_policy1_pending_positive_proof_replays_without_generic_zero(
    db, monkeypatch
):
    from unittest.mock import AsyncMock

    from orchestrator.services.vm_provisioner import VMTeardownResult

    state = await cancelled(db, old=False)
    permit = await acquire(state)
    provisioner, identity, retire = _policy1_release_fixture(db, state, monkeypatch)
    qualification = preflight(state)
    stopped = False
    actions = []

    async def controller_stop(payload):
        nonlocal stopped
        actions.append(payload["action"])
        if payload["action"] == "inspect":
            return {
                "status": "candidate",
                "frozen": state["frozen"],
                "retention_preflight": qualification,
                "_identity_authenticated": True,
            }
        if payload["action"] == "stop":
            return (
                {
                    "status": "positive_terminal_proof",
                    "terminal_evidence": terminal_proof(
                        state["frozen"], payload["frozen_digest"]
                    ),
                    "_identity_authenticated": True,
                }
                if stopped
                else {
                    "status": "pending_terminal_proof",
                    "_identity_authenticated": True,
                }
            )
        assert payload["action"] == "release"
        return {"status": "finalizer_released", "_identity_authenticated": True}

    provisioner._request_pre_ssh_stop = AsyncMock(side_effect=controller_stop)
    provisioner.delete_vm_captured = AsyncMock(
        return_value=VMTeardownResult("completed", True)
    )
    kwargs = {
        "purge_disk": False,
        "capture_snapshot": False,
        "parent_cleanup": permit.parent_cleanup,
    }
    first = await provisioner.release_vm_captured(state["job_id"], identity, **kwargs)
    assert first == VMTeardownResult("process_zero_unproven", False)
    assert actions == ["inspect", "stop"]
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_pre_ssh_stop_intents WHERE job_id=$1",
            UUID(state["job_id"]),
        )
        == 1
    )
    assert (
        await db.fetchval(
            "SELECT count(*) FROM managed_repository_process_zero_receipts "
            "WHERE owner_kind='job' AND owner_id=$1 AND scope='vm'",
            UUID(state["job_id"]),
        )
        == 0
    )
    provisioner.delete_vm_captured.assert_not_awaited()
    stopped = True
    second = await provisioner.release_vm_captured(state["job_id"], identity, **kwargs)
    assert second == VMTeardownResult("completed", True)
    assert actions == ["inspect", "stop", "stop", "release"]
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_pre_ssh_stop_proofs WHERE job_id=$1",
            UUID(state["job_id"]),
        )
        == 1
    )
    assert (
        await db.fetchval(
            "SELECT count(*) FROM managed_repository_process_zero_receipts "
            "WHERE owner_kind='job' AND owner_id=$1 AND scope='vm'",
            UUID(state["job_id"]),
        )
        == 1
    )
    retire.assert_not_awaited()


@pytest.mark.asyncio
async def test_policy1_preexisting_generic_zero_without_intent_stays_held(
    db, monkeypatch
):
    from unittest.mock import AsyncMock

    from orchestrator.services.vm_provisioner import VMTeardownResult

    state = await cancelled(db, old=False)
    permit = await acquire(state)
    await db.execute(
        "INSERT INTO managed_repository_process_zero_receipts "
        "(owner_kind,owner_id,scope,provisioner,runtime_incarnation) "
        "VALUES('job',$1,'vm','vm',$2)",
        UUID(state["job_id"]),
        state["generation"],
    )
    provisioner, identity, retire = _policy1_release_fixture(db, state, monkeypatch)
    provisioner._request_pre_ssh_stop = AsyncMock()
    provisioner.delete_vm_captured = AsyncMock()
    result = await provisioner.release_vm_captured(
        state["job_id"],
        identity,
        purge_disk=False,
        capture_snapshot=False,
        parent_cleanup=permit.parent_cleanup,
    )
    assert result == VMTeardownResult("process_zero_unproven", False)
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_pre_ssh_stop_intents WHERE job_id=$1",
            UUID(state["job_id"]),
        )
        == 0
    )
    provisioner._request_pre_ssh_stop.assert_not_awaited()
    provisioner.delete_vm_captured.assert_not_awaited()
    retire.assert_not_awaited()


@pytest.mark.asyncio
async def test_nonretained_generic_zero_keeps_existing_delete_path(db, monkeypatch):
    from unittest.mock import AsyncMock

    from orchestrator.services.vm_provisioner import VMTeardownResult

    state = await cancelled(db, old=False)
    await db.execute(
        "INSERT INTO managed_repository_process_zero_receipts "
        "(owner_kind,owner_id,scope,provisioner,runtime_incarnation) "
        "VALUES('job',$1,'vm','vm',$2)",
        UUID(state["job_id"]),
        state["generation"],
    )
    provisioner, identity, retire = _policy1_release_fixture(db, state, monkeypatch)
    provisioner._request_pre_ssh_stop = AsyncMock()
    provisioner.delete_vm_captured = AsyncMock(
        return_value=VMTeardownResult("completed", True)
    )
    result = await provisioner.release_vm_captured(
        state["job_id"],
        identity,
        purge_disk=False,
        capture_snapshot=False,
    )
    assert result == VMTeardownResult("completed", True)
    provisioner._request_pre_ssh_stop.assert_not_awaited()
    provisioner.delete_vm_captured.assert_awaited_once()
    retire.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["missing_parent", "wrong_source", "wrong_admission"])
async def test_policy1_unproven_parent_holds_before_any_effect(db, monkeypatch, fault):
    from unittest.mock import AsyncMock

    from orchestrator.services.vm_provisioner import VMTeardownResult

    state = await cancelled(db, old=False)
    permit = await acquire(state)
    provisioner, identity, retire = _policy1_release_fixture(db, state, monkeypatch)
    parent = deepcopy(permit.parent_cleanup)
    if fault == "missing_parent":
        parent = None
    elif fault == "wrong_source":
        parent["intent"]["source"] = "different_source"
    else:
        parent["admission_id"] = str(uuid4())
    provisioner._request_pre_ssh_stop = AsyncMock()
    provisioner.delete_vm_captured = AsyncMock()
    result = await provisioner.release_vm_captured(
        state["job_id"],
        identity,
        purge_disk=False,
        capture_snapshot=False,
        parent_cleanup=parent,
    )
    assert result == VMTeardownResult("retention_preflight_unproven", False)
    provisioner._request_pre_ssh_stop.assert_not_awaited()
    provisioner.delete_vm_captured.assert_not_awaited()
    retire.assert_not_awaited()


@pytest.mark.asyncio
async def test_policy1_selector_distinguishes_absent_and_exact_authority(db):
    from orchestrator.services.vm_job_cancel_retention import (
        current_policy1_retention_parent,
    )

    state = await cancelled(db, old=False)
    args = dict(
        job_id=state["job_id"],
        generation=state["generation"],
        vm_uid=state["frozen"]["vm_uid"],
        pvc_uid=state["frozen"]["pvc_uid"],
    )
    assert await current_policy1_retention_parent(db, None, **args) == "other"
    permit = await acquire(state)
    assert (
        await current_policy1_retention_parent(db, permit.parent_cleanup, **args)
        == "open"
    )
    assert (
        await current_policy1_retention_parent(
            db, permit.parent_cleanup, **{**args, "vm_uid": str(uuid4())}
        )
        is None
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("parent_committed", [False, True])
async def test_policy1_stop_holds_inside_outer_transaction(
    db, monkeypatch, parent_committed
):
    from unittest.mock import AsyncMock

    from orchestrator.services.vm_provisioner import VMTeardownResult

    state = await cancelled(db, old=False)
    permit = await acquire(state) if parent_committed else None
    provisioner, identity, retire = _policy1_release_fixture(db, state, monkeypatch)
    provisioner._request_pre_ssh_stop = AsyncMock()
    provisioner.delete_vm_captured = AsyncMock()
    with pytest.raises(RuntimeError, match="roll back"):
        async with db.transaction_scope():
            if permit is None:
                permit = await acquire(state)
            assert permit.allowed
            result = await provisioner.release_vm_captured(
                state["job_id"],
                identity,
                purge_disk=False,
                capture_snapshot=False,
                parent_cleanup=permit.parent_cleanup,
            )
            assert result == VMTeardownResult("retention_preflight_unproven", False)
            raise RuntimeError("roll back")
    provisioner._request_pre_ssh_stop.assert_not_awaited()
    provisioner.delete_vm_captured.assert_not_awaited()
    retire.assert_not_awaited()
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_pre_ssh_stop_intents WHERE job_id=$1",
            UUID(state["job_id"]),
        )
        == 0
    )


@pytest.mark.asyncio
async def test_existing_purge_parent_gets_linked_retention_successor(db):
    state = await cancelled(db)
    old = dict(
        await db.fetchrow(
            "SELECT * FROM vm_workspace_cleanup_admissions WHERE id=$1",
            state["cleanup_permit"].admission_id,
        )
    )
    permit = await acquire(state)
    assert permit.allowed and permit.admission_id != old["id"]
    assert permit.parent_cleanup["intent"]["purge_disk"] is False
    assert permit.parent_cleanup["request_id"] != str(old["request_id"])
    current = dict(
        await db.fetchrow(
            "SELECT * FROM vm_workspace_cleanup_admissions WHERE id=$1", old["id"]
        )
    )
    assert current.pop("outcome") == "superseded_by_retention"
    assert current.pop("completed_at") is not None
    old.pop("outcome")
    old.pop("completed_at")
    assert current == old
    assert (
        await db.fetchval(
            "SELECT superseded_admission_id FROM vm_job_cancel_retention_authorities "
            "WHERE cleanup_admission_id=$1",
            permit.admission_id,
        )
        == old["id"]
    )
    assert await acquire(state) == permit


@pytest.mark.asyncio
@pytest.mark.parametrize("binding", ["null", "absent"])
async def test_retention_scope_null_and_missing_binding_are_unbound(db, binding):
    state = await cancelled(db, binding=binding)
    assert (await acquire(state)).allowed


@pytest.mark.asyncio
@pytest.mark.parametrize("binding", ["", [], {}, {"uid": str(uuid4())}])
async def test_malformed_or_bound_cancel_holds_without_legacy_fallback(db, binding):
    state = await cancelled(db, binding=binding)
    before = await authority_rows(db, state)
    permit = await acquire(state)
    assert permit is not None and not permit.allowed
    assert await authority_rows(db, state) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("old", [False, True])
async def test_disabled_retention_admission_preserves_held_purge_parent(
    db, monkeypatch, old
):
    state = await cancelled(db, old=old)
    before = await authority_rows(db, state)
    monkeypatch.delenv("VM_JOB_CANCEL_RETENTION_ENABLED", raising=False)
    permit = await acquire(state)
    assert permit is not None and not permit.allowed
    assert await authority_rows(db, state) == before
    assert await db.fetchval("SELECT count(*) FROM vm_pre_ssh_stop_intents") == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("value", ["false", "malformed"])
async def test_disabled_gate_replays_existing_retention_without_legacy_fallback(
    db,
    monkeypatch,
    value,
):
    state = await cancelled(db)
    permit = await acquire(state)
    monkeypatch.setenv("VM_JOB_CANCEL_RETENTION_ENABLED", value)
    assert await acquire(state) == permit


async def child(db, state, *, completed=False, direct=False):
    parent = state["cleanup_permit"]
    kwargs = dict(
        owner_kind="job",
        owner_id=UUID(state["job_id"]),
        pvc_uid=UUID(state["frozen"]["pvc_uid"]),
        request_id=uuid4(),
        source="controller_rootdisk_delete",
        intent_digest="sha256:" + "b" * 64,
    )
    if direct:
        await db.execute(
            "INSERT INTO vm_workspace_cleanup_admissions "
            "(id,owner_kind,owner_id,pvc_uid,request_id,source,intent_digest,parent_admission_id) "
            "VALUES($1,$2,$3,$4,$5,$6,$7,$8)",
            uuid4(),
            *kwargs.values(),
            parent.admission_id,
        )
        return
    permit = await state["recovery"].acquire_cleanup_permit(
        **kwargs,
        parent_cleanup=parent.parent_cleanup,
        parent_provision_generation=state["generation"],
        expected_vm_uid=state["frozen"]["vm_uid"],
    )
    if completed and permit.allowed:
        await state["recovery"].complete_cleanup_permit(
            permit.admission_id, outcome="completed"
        )
    return permit


@pytest.mark.asyncio
@pytest.mark.parametrize("completed", [False, True])
async def test_any_controller_child_refuses_retention_transfer(db, completed):
    state = await cancelled(db)
    assert (await child(db, state, completed=completed)).allowed
    before = await authority_rows(db, state)
    permit = await acquire(state)
    assert permit is not None and not permit.allowed
    assert await authority_rows(db, state) == before


@pytest.mark.asyncio
async def test_retention_fences_superseded_parent_and_direct_old_controller_child(db):
    state = await cancelled(db)
    assert (await acquire(state)).allowed
    with pytest.raises(asyncpg.CheckViolationError):
        await child(db, state, direct=True)
    permit = await acquire_vm_cleanup_permit(
        state["recovery"],
        owner_kind="job",
        owner_id=state["job_id"],
        identity=state["identity"],
        source="job_terminal_vm_release",
        purge_disk=True,
    )
    assert not permit.allowed
    assert not (await child(db, state)).allowed


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "source", ["lifecycle_vm_reap", "controller_rootdisk_gc", "public_vm_delete"]
)
async def test_generic_public_delete_source_cannot_bypass_retention(db, source):
    state = await cancelled(db)
    assert (await acquire(state)).allowed
    permit = await state["recovery"].acquire_cleanup_permit(
        owner_kind="job",
        owner_id=UUID(state["job_id"]),
        pvc_uid=UUID(state["frozen"]["pvc_uid"]),
        request_id=uuid4(),
        source=source,
        intent_digest="sha256:" + "c" * 64,
    )
    assert not permit.allowed


@pytest.mark.asyncio
async def test_retention_link_and_original_intent_are_immutable(db):
    state = await cancelled(db)
    permit = await acquire(state)
    for query, identifier in [
        (
            "UPDATE vm_job_cancel_retention_authorities SET pvc_uid=gen_random_uuid() "
            "WHERE cleanup_admission_id=$1",
            permit.admission_id,
        ),
        (
            "DELETE FROM vm_job_cancel_retention_authorities WHERE cleanup_admission_id=$1",
            permit.admission_id,
        ),
        (
            "UPDATE vm_workspace_cleanup_admissions SET intent_digest='changed' WHERE id=$1",
            state["cleanup_permit"].admission_id,
        ),
        (
            "UPDATE vm_workspace_cleanup_admissions SET outcome='completed' WHERE id=$1",
            state["cleanup_permit"].admission_id,
        ),
    ]:
        with pytest.raises(asyncpg.CheckViolationError):
            await db.execute(query, identifier)


@pytest.mark.asyncio
async def test_generic_retention_completion_requires_exact_receipts(db):
    state = await cancelled(db)
    permit = await acquire(state)
    with pytest.raises(asyncpg.CheckViolationError):
        await state["recovery"].complete_cleanup_permit(
            permit.admission_id, outcome="completed"
        )
    assert (
        await db.fetchval(
            "SELECT completed_at FROM vm_workspace_cleanup_admissions WHERE id=$1",
            permit.admission_id,
        )
        is None
    )


def preflight(state):
    frozen = state["frozen"]
    return {
        "version": 1,
        "kind": "vm_cancel_retention_preflight_v1",
        "stop_policy": "cancel_retention_v1",
        "frozen": frozen,
        "namespace": frozen["namespace"],
        "owner_id": state["job_id"],
        "pvc_name": "rootdisk-" + state["job_id"],
        "pvc_uid": frozen["pvc_uid"],
        "dv_uid": str(uuid4()),
        "ownership": "standalone_dv",
        "deleting": False,
        "consumer_scope": "exact_frozen_runtime_only",
    }


async def insert_intent(db, state, permit, qualification):
    # Exercise the SQL admission contract before Task 3 wires the extra argument
    # through the existing application/Controller exchange.
    return await db.fetchval(
        "INSERT INTO vm_pre_ssh_stop_intents "
        "(cleanup_admission_id,job_id,provision_generation,creation_request_id,"
        "reservation_id,reservation_revision,vm_uid,vmi_uid,launcher_uid,pvc_uid,node_uid,"
        "cleanup_intent_digest,frozen,frozen_digest,retention_preflight) "
        "SELECT cleanup_admission_id,job_id,provision_generation,creation_request_id,"
        "reservation_id,reservation_revision,vm_uid,vmi_uid,launcher_uid,pvc_uid,node_uid,"
        "intent_digest,$2::jsonb,'sha256:'||encode(sha256(convert_to($2::jsonb::text,'UTF8')),'hex'),"
        "$3::jsonb FROM vm_job_cancel_retention_authorities WHERE cleanup_admission_id=$1 "
        "RETURNING frozen_digest",
        permit.admission_id,
        json.dumps(state["frozen"]),
        json.dumps(qualification),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", [None, "missing", "foreign"])
async def test_python_stop_intent_requires_and_replays_retention_preflight(db, fault):
    from orchestrator.services.vm_pre_ssh_stop_store import VMPreSSHStopConflict

    state = await cancelled(db)
    permit = await acquire(state)
    qualification = preflight(state)
    if fault == "foreign":
        qualification["pvc_uid"] = str(uuid4())
    if fault is not None:
        with pytest.raises(VMPreSSHStopConflict):
            await state["store"].admit_intent(
                state["job_id"],
                state["generation"],
                permit.parent_cleanup,
                state["frozen"],
                retention_preflight=None if fault == "missing" else qualification,
            )
        assert await db.fetchval("SELECT count(*) FROM vm_pre_ssh_stop_intents") == 0
    else:
        intent = await state["store"].admit_intent(
            state["job_id"],
            state["generation"],
            permit.parent_cleanup,
            state["frozen"],
            retention_preflight=qualification,
        )
        assert intent["retention_preflight"] == qualification
        assert (
            await state["store"].current_intent(
                state["job_id"],
                state["generation"],
                permit.parent_cleanup,
            )
            == intent
        )


async def positive_retention(db):
    state = await cancelled(db)
    permit = await acquire(state)
    state["retention"] = permit
    state["preflight"] = qualification = preflight(state)
    digest = await insert_intent(db, state, permit, qualification)
    await state["store"].commit_positive_proof(
        state["job_id"],
        state["generation"],
        permit.parent_cleanup,
        terminal_proof(state["frozen"], digest),
    )
    frozen = state["frozen"]
    state["evidence"] = {
        "version": 1,
        "kind": "vm_cleanup_physical_stop",
        "job_id": state["job_id"],
        "provision_generation": state["generation"],
        "vm_uid": frozen["vm_uid"],
        "vmi_uid": frozen["vmi_uid"],
        "launcher_uid": frozen["launcher_uid"],
        "pvc_uid": frozen["pvc_uid"],
        "vm_absent": True,
        "vmi_absent": True,
        "launcher_absent": True,
        "same_generation_replacement": False,
        "pvc_disposition": "retained",
        "controller_authenticated": True,
        "retained_rootdisk": {
            "version": 1,
            "kind": "vm_retained_rootdisk_v1",
            "namespace": frozen["namespace"],
            "owner_kind": "job",
            "owner_id": state["job_id"],
            "pvc_name": qualification["pvc_name"],
            "pvc_uid": frozen["pvc_uid"],
            "dv_uid": qualification["dv_uid"],
            "ownership": "standalone_dv",
            "deleting": False,
            "no_consumers": True,
        },
    }
    return state


async def settle_sql(db, state, *, evidence=None, release=True):
    # Production SQL constraints are the subject, so stage the same transactional
    # order as release_cleanup_compute_on_conn; no physical client is mocked.
    evidence = state["evidence"] if evidence is None else evidence
    async with db.acquire() as conn, conn.transaction():
        admission = state["retention"].admission_id
        await conn.execute(
            "UPDATE vm_workspace_cleanup_admissions SET completed_at=clock_timestamp(),"
            "outcome='completed' WHERE id=$1",
            admission,
        )
        await conn.execute(
            "INSERT INTO vm_resource_cleanup_stop_receipts "
            "(cleanup_admission_id,reservation_id,request_id,job_id,provision_generation,"
            "vm_uid,vmi_uid,launcher_uid,pvc_uid,intent_digest,stop_evidence) "
            "SELECT cleanup_admission_id,reservation_id,creation_request_id,job_id,"
            "provision_generation,vm_uid,vmi_uid,launcher_uid,pvc_uid,intent_digest,$2::jsonb "
            "FROM vm_job_cancel_retention_authorities WHERE cleanup_admission_id=$1",
            admission,
            json.dumps(evidence),
        )
        if release:
            await conn.execute(
                "UPDATE vm_resource_reservations v SET state='released',"
                "released_at=clock_timestamp(),release_evidence=jsonb_build_object("
                "'kind','exact_cleanup_compute_absent','cleanup_admission_id',s.cleanup_admission_id,"
                "'job_id',s.job_id,'provision_generation',s.provision_generation,'vm_uid',s.vm_uid,"
                "'vmi_uid',s.vmi_uid,'launcher_uid',s.launcher_uid,'pvc_uid',s.pvc_uid,"
                "'stop_evidence_digest','sha256:'||encode(sha256(convert_to(s.stop_evidence::text,'UTF8')),'hex')) "
                "FROM vm_resource_cleanup_stop_receipts s WHERE s.cleanup_admission_id=$1 AND v.id=s.reservation_id",
                admission,
            )

            await conn.execute(
                "UPDATE vm_resource_waiters SET state='released',revision=revision+1 "
                "WHERE request_id=(SELECT creation_request_id FROM vm_job_cancel_retention_authorities "
                "WHERE cleanup_admission_id=$1)",
                admission,
            )


@pytest.mark.asyncio
async def test_retention_compute_settlement_keeps_full_wire_proof_and_replays_once(db):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock
    from orchestrator.services.vm_workspace_recovery_store import (
        complete_vm_cleanup_permit,
        prepare_vm_cleanup_resource,
    )

    state = await positive_retention(db)
    permit = state["retention"]
    candidate = await prepare_vm_cleanup_resource(state["recovery"], permit)
    assert candidate["retention_preflight"] == state["preflight"]
    provisioner = SimpleNamespace(
        attest_vm_cleanup_stop=AsyncMock(return_value=state["evidence"])
    )
    await complete_vm_cleanup_permit(
        state["recovery"], permit, outcome="completed", provisioner=provisioner
    )
    stored = await db.fetchval(
        "SELECT stop_evidence FROM vm_resource_cleanup_stop_receipts WHERE cleanup_admission_id=$1",
        permit.admission_id,
    )
    assert json.loads(stored) == state["evidence"]
    assert await db.fetchval(
        "SELECT public.vm_job_cancel_retention_settled($1)", permit.admission_id
    )
    await complete_vm_cleanup_permit(
        state["recovery"],
        await acquire(state),
        outcome="completed",
        provisioner=provisioner,
    )
    provisioner.attest_vm_cleanup_stop.assert_awaited_once()


@pytest.mark.asyncio
async def test_markerless_retention_retry_precedes_checkpoint_prune(db):
    state = await cancelled(db)
    permit = await acquire(state)
    assert permit.allowed
    assert await db.quiesce_cancelled_stateless_vm_parent(state["job_id"])
    assert not await db.complete_stateless_cancel_cleanup(state["job_id"])
    context = json.loads(
        await db.fetchval("SELECT context FROM jobs WHERE id=$1", UUID(state["job_id"]))
    )
    assert context["_stateless_cancel_cleanup_pending"] is True
    assert "_job_terminal_vm_cleanup" not in context


@pytest.mark.asyncio
async def test_delete_keeps_strict_prune_for_legacy_open_true_parent(db, monkeypatch):
    from unittest.mock import AsyncMock

    state = await cancelled(db)
    assert await db.quiesce_cancelled_stateless_vm_parent(state["job_id"])
    assert not await db.quiesce_cancelled_stateless_vm_parent(
        state["job_id"], retention_only=True
    )
    prune = AsyncMock()
    monkeypatch.setattr(db, "delete_checkpoint_thread", prune)
    assert await db.prepare_stateless_job_for_delete(state["job_id"])
    prune.assert_awaited_once_with(state["job_id"], strict=True)


@pytest.mark.asyncio
async def test_markerless_settled_retention_clears_pending_after_restart(
    db, monkeypatch
):
    state = await positive_retention(db)
    await settle_sql(db, state)
    monkeypatch.setenv("VM_JOB_CANCEL_RETENTION_ENABLED", "false")
    assert await db.quiesce_cancelled_stateless_vm_parent(state["job_id"])
    assert await db.complete_stateless_cancel_cleanup(state["job_id"])
    context = json.loads(
        await db.fetchval("SELECT context FROM jobs WHERE id=$1", UUID(state["job_id"]))
    )
    assert "_stateless_cancel_cleanup_pending" not in context
    assert context["vm"]["compute_released"] is True
    assert context["vm"]["disk_kept"] is True
    assert context["vm"]["rootdisk_pvc_uid"] == state["frozen"]["pvc_uid"]


@pytest.mark.asyncio
@pytest.mark.parametrize("already_settled", [False, True])
async def test_actual_cancel_settle_keeps_disk_and_survives_settlement_response_loss(
    db, monkeypatch, already_settled
):
    import logging
    from functools import partial
    from types import SimpleNamespace
    from unittest.mock import AsyncMock
    from orchestrator.services import thread_retirement
    from orchestrator.services.job_mutation_controls import JobControlOperations
    from orchestrator.services.vm_workspace_policy import vm_needs_release

    state = await positive_retention(db)
    if already_settled:
        await settle_sql(db, state)
    monkeypatch.setenv("VM_JOB_CANCEL_RETENTION_ENABLED", "false")
    provisioner = SimpleNamespace(
        lifecycle_available=True,
        capture_vm_teardown_identity=AsyncMock(return_value=state["identity"]),
        release_vm_captured=AsyncMock(
            return_value=SimpleNamespace(disposition="completed")
        ),
        delete_vm_captured=AsyncMock(
            side_effect=AssertionError("Cancel must not purge")
        ),
        attest_vm_cleanup_stop=AsyncMock(return_value=state["evidence"]),
    )
    archive_dependencies = SimpleNamespace(
        store=db,
        vm_provisioner=provisioner,
        recovery_store=state["recovery"],
        container_provisioner=None,
        docker_provisioner=None,
        get_container_context=lambda _: {},
        get_vm_context=lambda job: json.loads(job["context"])["vm"],
        vm_needs_release=vm_needs_release,
    )
    control = JobControlOperations(
        SimpleNamespace(
            store=db,
            logger=logging.getLogger(__name__),
            archive_and_cleanup_workspace=partial(
                thread_retirement.archive_and_cleanup_workspace,
                dependencies=archive_dependencies,
            ),
        )
    )
    assert await control.wait_for_stateless_cancel_settle(
        state["job_id"], timeout_seconds=0
    )
    assert await control.wait_for_stateless_cancel_settle(
        state["job_id"], timeout_seconds=0
    )
    assert provisioner.release_vm_captured.await_count == int(not already_settled)
    provisioner.delete_vm_captured.assert_not_awaited()
    assert (
        await db.fetchval("SELECT count(*) FROM vm_job_retained_disk_purge_authorities")
        == 0
    )
    assert await db.fetchval(
        "SELECT public.vm_job_cancel_retention_settled($1)",
        state["retention"].admission_id,
    )
    context = json.loads(
        await db.fetchval("SELECT context FROM jobs WHERE id=$1", UUID(state["job_id"]))
    )
    assert context["vm"]["disk_kept"] is True


@pytest.mark.asyncio
async def test_durably_ready_source_requires_distinct_ready_authority(db):
    state = await cancelled(db, old=False, retiring=False)
    await db.execute(
        "UPDATE jobs SET context=jsonb_set(context,'{vm}',context->'vm'||$2::jsonb) WHERE id=$1",
        UUID(state["job_id"]),
        json.dumps(
            {"status": "ready", "active_pod_uid": state["frozen"]["launcher_uid"]}
        ),
    )
    await db.execute(
        "UPDATE vm_creation_retries SET ready_at=clock_timestamp() WHERE job_id=$1",
        UUID(state["job_id"]),
    )
    before = await authority_rows(db, state)
    # Explicit additions stop at 0338; schema_current may already include0340.
    # Either way, no Ready proof means no fallback to ordinary True cleanup.
    permit = await acquire(state)
    assert permit.allowed is False
    installed = await db.fetchval(
        "SELECT to_regprocedure('public.vm_job_initial_ready_retention_candidate(uuid,uuid,uuid,uuid,uuid,uuid,boolean)') IS NOT NULL"
    )
    assert permit.reason == (
        "initial_ready_retention_unproven"
        if installed
        else "cancel_retention_schema_unavailable"
    )
    assert await authority_rows(db, state) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("enabled_gate", [False, True])
async def test_markerless_cancel_archive_never_falls_back_to_purge(
    db, monkeypatch, enabled_gate
):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock
    from orchestrator.services import thread_retirement
    from orchestrator.services.vm_workspace_policy import vm_needs_release

    state = await cancelled(db)
    monkeypatch.setenv("VM_JOB_CANCEL_RETENTION_ENABLED", str(enabled_gate).lower())
    generic = AsyncMock(side_effect=AssertionError("retention fell through to purge"))
    monkeypatch.setattr(thread_retirement, "acquire_vm_cleanup_permit", generic)
    monkeypatch.setattr(thread_retirement, "complete_vm_cleanup_permit", AsyncMock())
    provisioner = SimpleNamespace(
        lifecycle_available=True,
        capture_vm_teardown_identity=AsyncMock(return_value=state["identity"]),
        release_vm_captured=AsyncMock(
            return_value=SimpleNamespace(disposition="completed")
        ),
    )
    context = json.loads(
        await db.fetchval("SELECT context FROM jobs WHERE id=$1", UUID(state["job_id"]))
    )
    dependencies = SimpleNamespace(
        store=db,
        vm_provisioner=provisioner,
        recovery_store=state["recovery"],
        container_provisioner=None,
        docker_provisioner=None,
        get_container_context=lambda _: {},
        get_vm_context=lambda _: context["vm"],
        vm_needs_release=vm_needs_release,
    )
    if enabled_gate:
        await thread_retirement.archive_and_cleanup_workspace(
            state["job_id"], dependencies=dependencies
        )
        assert provisioner.release_vm_captured.await_args.kwargs["purge_disk"] is False
        assert (
            provisioner.release_vm_captured.await_args.kwargs["capture_snapshot"]
            is False
        )
    else:
        with pytest.raises(RuntimeError, match="retention"):
            await thread_retirement.archive_and_cleanup_workspace(
                state["job_id"], dependencies=dependencies
            )
        provisioner.release_vm_captured.assert_not_awaited()
    generic.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("already_settled", [False, True])
@pytest.mark.parametrize("lost_reply", [False, True])
async def test_permanent_job_delete_reuses_retention_then_typed_purge(
    db, already_settled, lost_reply
):
    import logging
    from functools import partial
    from types import SimpleNamespace
    from unittest.mock import AsyncMock
    from orchestrator.services import thread_retirement
    from orchestrator.services.job_mutation_controls import JobControlOperations
    from orchestrator.services.vm_workspace_policy import vm_needs_release

    state = await positive_retention(db)
    if already_settled:
        await settle_sql(db, state)
        assert await db.complete_stateless_cancel_cleanup(state["job_id"])
    effects = []
    purge_parents = []

    async def release(_job_id, _identity, **kwargs):
        assert kwargs["purge_disk"] is False
        effects.append("compute")
        return SimpleNamespace(disposition="completed")

    async def purge(_job_id, _identity, **kwargs):
        assert kwargs["purge_disk"] is True
        context = json.loads(
            await db.fetchval(
                "SELECT context FROM jobs WHERE id=$1", UUID(state["job_id"])
            )
        )
        assert context["_stateless_delete_pending"] is True
        purge_parents.append(kwargs["parent_cleanup"]["admission_id"])
        if len(purge_parents) == 1:
            effects.append("disk")
            if lost_reply:
                return SimpleNamespace(disposition="retry_pending")
        return SimpleNamespace(disposition="completed")

    async def attest(candidate):
        if not candidate["purge_disk"]:
            return state["evidence"]
        return {
            **{
                key: value
                for key, value in state["evidence"].items()
                if key != "retained_rootdisk"
            },
            "pvc_disposition": "purged",
            "controller_scope": candidate["controller_scope"],
        }

    provisioner = SimpleNamespace(
        lifecycle_available=True,
        capture_vm_teardown_identity=AsyncMock(return_value=state["identity"]),
        release_vm_captured=release,
        delete_vm_captured=purge,
        attest_vm_cleanup_stop=attest,
    )
    archive_dependencies = SimpleNamespace(
        store=db,
        vm_provisioner=provisioner,
        recovery_store=state["recovery"],
        container_provisioner=None,
        docker_provisioner=None,
        get_container_context=lambda _: {},
        get_vm_context=lambda job: json.loads(job["context"])["vm"],
        vm_needs_release=vm_needs_release,
    )

    @asynccontextmanager
    async def vector_connection():
        yield SimpleNamespace(execute=AsyncMock())

    control = JobControlOperations(
        SimpleNamespace(
            store=db,
            logger=logging.getLogger(__name__),
            archive_and_cleanup_workspace=partial(
                thread_retirement.archive_and_cleanup_workspace,
                dependencies=archive_dependencies,
            ),
            snapshot_service=SimpleNamespace(is_available=False),
            vector_db=SimpleNamespace(acquire=vector_connection),
            resolve_job_notifications=AsyncMock(),
        )
    )
    job = await db.get_job(state["job_id"])
    if lost_reply:
        from fastapi import HTTPException

        with pytest.raises(HTTPException) as lost:
            await control.delete(
                state["job_id"], caller={"id": job["user_id"]}, job=job
            )
        assert lost.value.status_code == 503
        assert await db.get_job(state["job_id"]) is not None
        job = await db.get_job(state["job_id"])
    result = await control.delete(
        state["job_id"], caller={"id": job["user_id"]}, job=job
    )
    assert result["status"] == "deleted"
    assert effects == (["disk"] if already_settled else ["compute", "disk"])
    assert len(set(purge_parents)) == 1
    assert await db.get_job(state["job_id"]) is None
    assert (
        await db.fetchval("SELECT count(*) FROM vm_job_retained_disk_purge_receipts")
        == 1
    )
    assert await db.fetchval(
        "SELECT deletion_receipt IS NOT NULL FROM vm_job_creation_owners WHERE job_id=$1",
        UUID(state["job_id"]),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    [
        "missing",
        "foreign_owner",
        "foreign_pvc",
        "foreign_namespace",
        "malformed_dv",
        "unsupported_policy",
        "unknown_field",
        "false_consumer_claim",
    ],
)
async def test_missing_or_foreign_preflight_cannot_insert_retention_stop_intent(
    db, change
):
    state = await cancelled(db)
    permit = await acquire(state)
    qualification = preflight(state)
    if change == "missing":
        qualification = None
    elif change == "foreign_owner":
        qualification["owner_id"] = str(uuid4())
    elif change == "foreign_pvc":
        qualification["pvc_uid"] = str(uuid4())
    elif change == "foreign_namespace":
        qualification["namespace"] = "foreign"
    elif change == "malformed_dv":
        qualification["dv_uid"] = "not-a-uid"
    elif change == "unsupported_policy":
        qualification["stop_policy"] = "legacy"
    elif change == "unknown_field":
        qualification["unqualified"] = True
    else:
        qualification["no_consumers"] = True
    with pytest.raises(asyncpg.CheckViolationError):
        await insert_intent(db, state, permit, qualification)
    assert await db.fetchval("SELECT count(*) FROM vm_pre_ssh_stop_intents") == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    [
        "missing",
        "unknown_field",
        "foreign_dv",
        "foreign_pvc",
        "foreign_owner",
        "foreign_namespace",
        "deleting",
        "consumers",
        "string_boolean",
        "unreleased",
    ],
)
async def test_malformed_retained_settlement_rolls_back_completion_and_release(
    db, change
):
    state = await positive_retention(db)
    evidence = deepcopy(state["evidence"])
    retained = evidence["retained_rootdisk"]
    if change == "missing":
        evidence.pop("retained_rootdisk")
    elif change == "unknown_field":
        retained["unqualified"] = True
    elif change.startswith("foreign_"):
        field = {
            "foreign_dv": "dv_uid",
            "foreign_pvc": "pvc_uid",
            "foreign_owner": "owner_id",
            "foreign_namespace": "namespace",
        }[change]
        retained[field] = "foreign" if field == "namespace" else str(uuid4())
    elif change == "deleting":
        retained["deleting"] = True
    elif change == "consumers":
        retained["no_consumers"] = False
    elif change == "string_boolean":
        retained["no_consumers"] = "true"
    with pytest.raises(asyncpg.CheckViolationError):
        await settle_sql(db, state, evidence=evidence, release=change != "unreleased")
    assert (
        await db.fetchval(
            "SELECT completed_at FROM vm_workspace_cleanup_admissions WHERE id=$1",
            state["retention"].admission_id,
        )
        is None
    )
    assert (
        await db.fetchval(
            "SELECT state FROM vm_resource_reservations WHERE id=$1",
            UUID(state["reservation_id"]),
        )
        == "teardown"
    )


@pytest.mark.asyncio
async def test_completed_replay_uses_released_charge_without_new_admission(
    db, monkeypatch
):
    state = await positive_retention(db)
    await settle_sql(db, state)
    monkeypatch.setenv("VM_JOB_CANCEL_RETENTION_ENABLED", "false")
    permit = await acquire(state)
    assert permit.allowed and permit.completed_outcome == "completed"
    assert permit.admission_id == state["retention"].admission_id
    from orchestrator.services.vm_workspace_recovery_store import (
        completed_cleanup_outcome,
    )

    assert completed_cleanup_outcome(permit) == "completed"
    from orchestrator.services.vm_job_cancel_retention import (
        current_policy1_retention_parent,
    )

    assert (
        await current_policy1_retention_parent(
            db,
            permit.parent_cleanup,
            job_id=state["job_id"],
            generation=state["generation"],
            vm_uid=state["frozen"]["vm_uid"],
            pvc_uid=state["frozen"]["pvc_uid"],
        )
        == "settled"
    )
    assert (
        await db.fetchval("SELECT count(*) FROM vm_job_cancel_retention_authorities")
        == 1
    )
    assert (
        await db.fetchval(
            "SELECT public.vm_job_cancel_retention_settled($1)",
            permit.admission_id,
        )
        is True
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("physical_runtime_present", [False, True])
async def test_settled_policy1_release_replays_only_when_compute_is_absent(
    db, monkeypatch, physical_runtime_present
):
    from unittest.mock import AsyncMock

    from orchestrator.services.vm_provisioner import (
        VMProvisioner,
        VMTeardownIdentity,
        VMTeardownResult,
        _VMTeardownProbe,
    )

    state = await positive_retention(db)
    await settle_sql(db, state)
    permit = await acquire(state)
    assert permit.completed_outcome == "completed"
    monkeypatch.setenv("VM_MODE", "same-cluster")
    provisioner = VMProvisioner()
    provisioner._db = db
    provisioner._snapshot_service = None
    observed = VMTeardownIdentity(
        state["generation"],
        state["frozen"]["vm_uid"] if physical_runtime_present else None,
        state["frozen"]["pvc_uid"],
    )
    provisioner._probe_vm_teardown_identity = AsyncMock(
        return_value=_VMTeardownProbe(
            "present" if physical_runtime_present else "absent",
            observed,
            rootdisk_identity_known=True,
            runtime_absence_known=not physical_runtime_present,
            vmi_absent=not physical_runtime_present,
            launcher_absent=not physical_runtime_present,
        )
    )
    provisioner._request_pre_ssh_stop = AsyncMock()
    provisioner.delete_vm_captured = AsyncMock()
    result = await provisioner.release_vm_captured(
        state["job_id"],
        state["identity"],
        purge_disk=False,
        capture_snapshot=False,
        parent_cleanup=permit.parent_cleanup,
    )
    assert result == (
        VMTeardownResult("retention_preflight_unproven", False)
        if physical_runtime_present
        else VMTeardownResult("completed", True)
    )
    provisioner._request_pre_ssh_stop.assert_not_awaited()
    provisioner.delete_vm_captured.assert_not_awaited()


@pytest.mark.asyncio
async def test_typed_delete_bootstraps_then_commits_complete_chain(db, monkeypatch):
    from orchestrator.services.vm_job_retained_disk_purge import (
        acquire_job_retained_disk_purge,
    )

    state = await positive_retention(db)
    await settle_sql(db, state)
    monkeypatch.setenv("VM_JOB_CANCEL_RETENTION_ENABLED", "false")
    purge = await acquire_job_retained_disk_purge(
        state["recovery"],
        job_id=state["job_id"],
        identity=state["identity"],
    )
    assert purge.allowed and purge.parent_cleanup["intent"]["purge_disk"] is True
    assert (
        await db.fetchval(
            "SELECT public.validate_vm_job_retained_disk_purge($1,false)",
            purge.admission_id,
        )
        is True
    )
    assert (
        await db.fetchval(
            "SELECT old_cleanup_admission_id FROM vm_job_retained_disk_purge_predecessors "
            "WHERE cleanup_admission_id=$1",
            purge.admission_id,
        )
        == state["retention"].admission_id
    )
    # Committed typed parent can authorize its exact physical child.
    state["cleanup_permit"] = purge
    assert (await child(db, state)).allowed
    assert not await db.fetchval(
        "SELECT public.vm_job_cancel_retention_discharged($1)",
        state["retention"].admission_id,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("release_savepoint", [False, True])
async def test_typed_purge_savepoint_cannot_issue_child_before_outer_commit(
    db, release_savepoint
):
    from orchestrator.services.vm_job_retained_disk_purge import (
        acquire_job_retained_disk_purge,
    )

    state = await positive_retention(db)
    await settle_sql(db, state)
    before = await authority_rows(db, state)
    async with db.acquire() as conn:

        class BorrowedConnection:
            @asynccontextmanager
            async def acquire(self):
                yield conn

        outer = conn.transaction()
        await outer.start()
        try:
            savepoint = conn.transaction()
            await savepoint.start()
            store = VMWorkspaceRecoveryStore(BorrowedConnection())
            purge = await acquire_job_retained_disk_purge(
                store, job_id=state["job_id"], identity=state["identity"]
            )
            assert purge.allowed
            if release_savepoint:
                await savepoint.commit()
            assert await conn.fetchval(
                "SELECT a.admitted_xact_id=pg_current_xact_id() "
                "AND p.admitted_xact_id=pg_current_xact_id() "
                "FROM vm_job_retained_disk_purge_authorities a "
                "JOIN vm_job_retained_disk_purge_predecessors p "
                "USING(cleanup_admission_id) WHERE a.cleanup_admission_id=$1",
                purge.admission_id,
            )
            state["cleanup_permit"] = purge
            with pytest.raises(asyncpg.CheckViolationError):
                await child(conn, state, direct=True)
        finally:
            await outer.rollback()
    assert await authority_rows(db, state) == before
    assert not await db.fetchval(
        "SELECT EXISTS(SELECT 1 FROM vm_job_retained_disk_purge_authorities)"
    )


@pytest.mark.asyncio
async def test_purge_bootstrap_without_authority_or_link_rolls_back(db):
    from orchestrator.services.vm_workspace_recovery_store import (
        vm_cleanup_request_identity,
    )

    state = await positive_retention(db)
    await settle_sql(db, state)
    owner, pvc, request, digest, _ = vm_cleanup_request_identity(
        owner_kind="job",
        owner_id=state["job_id"],
        identity=state["identity"],
        source="public_vm_delete",
        purge_disk=True,
    )
    with pytest.raises(asyncpg.CheckViolationError):
        await db.execute(
            "INSERT INTO vm_workspace_cleanup_admissions "
            "(id,owner_kind,owner_id,pvc_uid,request_id,source,intent_digest) "
            "VALUES($1,'job',$2,$3,$4,'public_vm_delete',$5)",
            uuid4(),
            owner,
            pvc,
            request,
            digest,
        )
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_workspace_cleanup_admissions WHERE request_id=$1",
            request,
        )
        == 0
    )


@pytest.mark.asyncio
async def test_retention_transfer_and_controller_child_have_one_winner(db):
    state = await cancelled(db)
    retain, deleting = await asyncio.gather(acquire(state), child(db, state))
    assert retain.allowed != deleting.allowed
    assert await db.fetchval(
        "SELECT count(*) FROM vm_job_cancel_retention_authorities"
    ) == int(retain.allowed)


@pytest.mark.asyncio
async def test_retention_fences_other_owner_using_same_pvc_after_compute_release(db):
    state = await positive_retention(db)
    await settle_sql(db, state)
    other = uuid4()
    await db.execute(
        "INSERT INTO jobs(id,description,status) VALUES($1,'foreign','cancelled')",
        other,
    )
    permit = await state["recovery"].acquire_cleanup_permit(
        owner_kind="job",
        owner_id=other,
        pvc_uid=UUID(state["frozen"]["pvc_uid"]),
        source="public_vm_delete",
        request_id=uuid4(),
        intent_digest="sha256:" + "a" * 64,
    )
    assert not permit.allowed


@pytest.mark.asyncio
async def test_historical_settlement_cannot_finish_cancel_for_a_successor(db):
    from orchestrator.services.vm_job_cancel_retention import (
        retention_settlement_is_current_on_conn,
    )

    state = await positive_retention(db)
    await settle_sql(db, state)
    owner = UUID(state["job_id"])
    context = json.loads(
        await db.fetchval("SELECT context FROM jobs WHERE id=$1", owner)
    )
    context["last_vm"] = context["vm"]
    context["vm"] = {"provision_generation": str(uuid4()), "status": "failed"}
    await db.execute(
        "UPDATE jobs SET status='paused',context=$2::jsonb WHERE id=$1",
        owner,
        json.dumps(context),
    )
    async with db.acquire() as conn:
        assert not await retention_settlement_is_current_on_conn(
            conn,
            job_id=owner,
            generation=UUID(state["generation"]),
            admission_id=state["retention"].admission_id,
        )
    assert not (await acquire(state)).allowed


@pytest.mark.asyncio
async def test_transfer_rollback_leaves_original_purge_pending(db, monkeypatch):
    from orchestrator.services import vm_job_cancel_retention as retention

    state = await cancelled(db)
    before = await authority_rows(db, state)

    async def changed_resource(*args, **kwargs):
        raise RuntimeError("resource policy changed before prepare")

    monkeypatch.setattr(retention, "prepare_vm_cleanup_resource", changed_resource)
    with pytest.raises(RuntimeError, match="resource policy changed"):
        await acquire(state)
    assert await authority_rows(db, state) == before
    assert (
        await db.fetchval("SELECT count(*) FROM vm_job_cancel_retention_authorities")
        == 0
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("winner", ["retention", "child"])
async def test_retention_and_old_controller_serialize_before_commit(
    db, monkeypatch, winner
):
    from orchestrator.services.vm_creation_retry_store import VMCreationRetryStore

    state = await cancelled(db)
    locked, release = asyncio.Event(), asyncio.Event()
    if winner == "retention":
        original = VMCreationRetryStore._scope

        async def paused_scope(*args, **kwargs):
            result = await original(*args, **kwargs)
            locked.set()
            await release.wait()
            return result

        monkeypatch.setattr(VMCreationRetryStore, "_scope", paused_scope)
        first = asyncio.create_task(acquire(state))
        await asyncio.wait_for(locked.wait(), 5)
        second = asyncio.create_task(child(db, state))
        release.set()
        retained, deleting = await asyncio.wait_for(asyncio.gather(first, second), 5)
        assert retained.allowed and not deleting.allowed
    else:

        async def old_controller():
            async with db.acquire() as conn, conn.transaction():
                await child(conn, state, direct=True)
                locked.set()
                await release.wait()

        first = asyncio.create_task(old_controller())
        await asyncio.wait_for(locked.wait(), 5)
        second = asyncio.create_task(acquire(state))
        release.set()
        _, retained = await asyncio.wait_for(asyncio.gather(first, second), 5)
        assert not retained.allowed


@pytest.mark.asyncio
async def test_disabled_gate_still_refuses_gc_and_generic_delete_after_settlement(
    db, monkeypatch
):
    state = await positive_retention(db)
    await settle_sql(db, state)
    monkeypatch.setenv("VM_JOB_CANCEL_RETENTION_ENABLED", "false")
    for source in ("controller_rootdisk_gc", "lifecycle_vm_reap", "public_vm_delete"):
        result = await state["recovery"].acquire_cleanup_permit(
            owner_kind="job",
            owner_id=UUID(state["job_id"]),
            pvc_uid=UUID(state["frozen"]["pvc_uid"]),
            request_id=uuid4(),
            source=source,
            intent_digest="sha256:" + "d" * 64,
        )
        assert not result.allowed


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault", ["authority_missing", "chain_missing", "provisional_child"]
)
async def test_typed_purge_bootstrap_cannot_commit_or_issue_an_incomplete_chain(
    db, fault
):
    from orchestrator.services.vm_job_cancel_retention import JobRetainedPurgeBootstrap
    from orchestrator.services.vm_workspace_recovery_store import (
        vm_cleanup_request_identity,
    )

    state = await positive_retention(db)
    await settle_sql(db, state)
    before = await authority_rows(db, state)
    a = await db.fetchrow(
        "SELECT * FROM vm_job_cancel_retention_authorities WHERE cleanup_admission_id=$1",
        state["retention"].admission_id,
    )
    owner, pvc, request, digest, _ = vm_cleanup_request_identity(
        owner_kind="job",
        owner_id=state["job_id"],
        identity=state["identity"],
        source="public_vm_delete",
        purge_disk=True,
    )
    bootstrap = JobRetainedPurgeBootstrap(
        job_id=owner,
        pvc_uid=pvc,
        final_request_id=a["creation_request_id"],
        provision_generation=a["provision_generation"],
        cleanup_request_id=request,
        intent_digest=digest,
        retention_admission_ids=frozenset({a["cleanup_admission_id"]}),
    )
    with pytest.raises(asyncpg.CheckViolationError):
        async with db.acquire() as conn, conn.transaction():
            purge = await state["recovery"].acquire_cleanup_permit_on_conn(
                conn,
                owner_kind="job",
                owner_id=owner,
                pvc_uid=pvc,
                request_id=request,
                source="public_vm_delete",
                intent_digest=digest,
                _retained_purge_bootstrap=bootstrap,
            )
            assert purge.allowed
            if fault != "authority_missing":
                await conn.execute(
                    "INSERT INTO vm_job_retained_disk_purge_authorities "
                    "(cleanup_admission_id,job_id,final_request_id,provision_generation,"
                    "vm_uid,vmi_uid,launcher_uid,pvc_uid,binding_kind,cleanup_request_id,intent_digest,"
                    "admitted_xact_id) "
                    "SELECT $1,job_id,creation_request_id,provision_generation,vm_uid,vmi_uid,"
                    "launcher_uid,pvc_uid,'unbound',$2,$3,'1'::xid8 "
                    "FROM vm_job_cancel_retention_authorities "
                    "WHERE cleanup_admission_id=$4",
                    purge.admission_id,
                    request,
                    digest,
                    a["cleanup_admission_id"],
                )
            if fault == "provisional_child":
                await conn.execute(
                    "INSERT INTO vm_job_retained_disk_purge_predecessors "
                    "(cleanup_admission_id,source_request_id,old_cleanup_admission_id,"
                    "reservation_id,reservation_revision,stop_evidence_digest,admitted_xact_id) "
                    "SELECT $1,s.request_id,s.cleanup_admission_id,s.reservation_id,v.revision,"
                    "'sha256:'||encode(sha256(convert_to(s.stop_evidence::text,'UTF8')),'hex'),"
                    "'1'::xid8 "
                    "FROM vm_resource_cleanup_stop_receipts s JOIN vm_resource_reservations v "
                    "ON v.id=s.reservation_id WHERE s.cleanup_admission_id=$2",
                    purge.admission_id,
                    a["cleanup_admission_id"],
                )
                assert await conn.fetchval(
                    "SELECT public.validate_vm_job_retained_disk_purge($1,false)",
                    purge.admission_id,
                )
                # Neither row can forge a prior transaction's authority.
                assert await conn.fetchval(
                    "SELECT a.admitted_xact_id=pg_current_xact_id() "
                    "AND p.admitted_xact_id=pg_current_xact_id() "
                    "FROM vm_job_retained_disk_purge_authorities a "
                    "JOIN vm_job_retained_disk_purge_predecessors p "
                    "USING(cleanup_admission_id) WHERE a.cleanup_admission_id=$1",
                    purge.admission_id,
                )
                # Even a locally complete chain must commit before an effect child.
                state["cleanup_permit"] = purge
                await child(conn, state, direct=True)
    assert await authority_rows(db, state) == before
    assert (
        await db.fetchval("SELECT count(*) FROM vm_job_retained_disk_purge_authorities")
        == 0
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["retention_id", "pvc", "source_request"])
async def test_wrong_retention_or_pvc_cannot_bootstrap_purge(db, fault):
    from dataclasses import replace
    from orchestrator.services.vm_job_cancel_retention import JobRetainedPurgeBootstrap
    from orchestrator.services.vm_workspace_recovery_store import (
        vm_cleanup_request_identity,
    )

    state = await positive_retention(db)
    await settle_sql(db, state)
    a = await db.fetchrow(
        "SELECT * FROM vm_job_cancel_retention_authorities WHERE cleanup_admission_id=$1",
        state["retention"].admission_id,
    )
    owner, pvc, request, digest, _ = vm_cleanup_request_identity(
        owner_kind="job",
        owner_id=state["job_id"],
        identity=state["identity"],
        source="public_vm_delete",
        purge_disk=True,
    )
    bootstrap = JobRetainedPurgeBootstrap(
        job_id=owner,
        pvc_uid=pvc,
        final_request_id=a["creation_request_id"],
        provision_generation=a["provision_generation"],
        cleanup_request_id=request,
        intent_digest=digest,
        retention_admission_ids=frozenset({a["cleanup_admission_id"]}),
    )
    if fault == "retention_id":
        bootstrap = replace(bootstrap, retention_admission_ids=frozenset({uuid4()}))
    elif fault == "pvc":
        bootstrap = replace(bootstrap, pvc_uid=uuid4())
    else:
        bootstrap = replace(bootstrap, final_request_id=uuid4())
    async with db.acquire() as conn, conn.transaction():
        permit = await state["recovery"].acquire_cleanup_permit_on_conn(
            conn,
            owner_kind="job",
            owner_id=owner,
            pvc_uid=pvc,
            request_id=request,
            source="public_vm_delete",
            intent_digest=digest,
            _retained_purge_bootstrap=bootstrap,
        )
        assert not permit.allowed
    assert (
        await db.fetchval("SELECT count(*) FROM vm_job_retained_disk_purge_authorities")
        == 0
    )


@pytest.mark.asyncio
async def test_new_authority_cannot_attach_to_an_already_settled_parent(db):
    state = await positive_retention(db)
    await settle_sql(db, state)
    with pytest.raises(asyncpg.CheckViolationError):
        await db.fetchval(
            "SELECT public.validate_vm_job_cancel_retention($1,true)",
            state["retention"].admission_id,
        )


@pytest.mark.asyncio
async def test_fresh_created_vm_cancel_admits_retention_before_runtime_retirement(db):
    state = await cancelled(db, old=False, retiring=False)
    permit = await acquire(state)
    assert permit.allowed and permit.parent_cleanup["intent"]["purge_disk"] is False
    assert (
        await db.fetchval(
            "SELECT context->'vm'->>'status' FROM jobs WHERE id=$1",
            UUID(state["job_id"]),
        )
        == "created"
    )
    assert (
        await db.fetchval(
            "SELECT state FROM vm_resource_reservations WHERE id=$1",
            UUID(state["reservation_id"]),
        )
        == "teardown"
    )


@pytest.mark.asyncio
async def test_present_terminal_marker_rebinds_only_to_its_retaining_successor(db):
    state = await cancelled(db)
    marker = {
        "version": 1,
        "provision_generation": state["generation"],
        "admission_id": str(state["cleanup_permit"].admission_id),
        "reason": "terminal_cleanup",
    }
    await db.execute(
        "UPDATE jobs SET context=jsonb_set(context,'{_job_terminal_vm_cleanup}',$2::jsonb) WHERE id=$1",
        UUID(state["job_id"]),
        json.dumps(marker),
    )
    permit = await acquire(state)
    assert permit.allowed
    actual = json.loads(
        await db.fetchval(
            "SELECT context->'_job_terminal_vm_cleanup' FROM jobs WHERE id=$1",
            UUID(state["job_id"]),
        )
    )
    assert actual == {**marker, "admission_id": str(permit.admission_id)}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault",
    [
        "ready",
        "unauthenticated",
        "wrong_generation",
        "marker_null",
        "marker_foreign",
        "marker_missing_id",
        "inherited",
        "leased_queue",
    ],
)
async def test_changed_or_uncertain_source_preserves_the_original_parent(db, fault):
    state = await cancelled(db)
    owner = UUID(state["job_id"])
    context = json.loads(
        await db.fetchval("SELECT context FROM jobs WHERE id=$1", owner)
    )
    if fault == "ready":
        await db.execute(
            "UPDATE vm_creation_retries SET ready_at=clock_timestamp() WHERE job_id=$1",
            owner,
        )
    elif fault == "unauthenticated":
        context["vm"]["identity_authenticated"] = False
    elif fault == "wrong_generation":
        state["identity"] = VMTeardownIdentity(
            str(uuid4()),
            state["identity"].vm_uid,
            state["identity"].rootdisk_pvc_uid,
        )
    elif fault == "marker_null":
        context["_job_terminal_vm_cleanup"] = None
    elif fault in {"marker_foreign", "marker_missing_id"}:
        context["_job_terminal_vm_cleanup"] = {
            "version": 1,
            "provision_generation": state["generation"],
        }
        if fault == "marker_foreign":
            context["_job_terminal_vm_cleanup"]["admission_id"] = str(uuid4())
    elif fault == "inherited":
        context["inherits_parent_workspace"] = True
    else:
        await db.execute(
            "UPDATE run_queue SET state='leased',lease_token=lease_token+1,"
            "leased_by='test-old-worker',leased_until=clock_timestamp()+interval '1 minute' WHERE unit_id=$1",
            owner,
        )
    await db.execute(
        "UPDATE jobs SET context=$2::jsonb WHERE id=$1", owner, json.dumps(context)
    )
    before = await authority_rows(db, state)
    permit = await acquire(state)
    assert permit is not None and not permit.allowed
    assert await authority_rows(db, state) == before


@pytest.mark.asyncio
async def test_retention_fence_precedes_completed_generic_cleanup_replay(db):
    state = await cancelled(db)
    request, admission = uuid4(), uuid4()
    owner, pvc = UUID(state["job_id"]), UUID(state["frozen"]["pvc_uid"])
    digest = "sha256:" + "e" * 64
    await db.execute(
        "INSERT INTO vm_workspace_cleanup_admissions "
        "(id,owner_kind,owner_id,pvc_uid,request_id,source,intent_digest,completed_at,outcome) "
        "VALUES($1,'job',$2,$3,$4,'controller_rootdisk_gc',$5,clock_timestamp(),'completed')",
        admission,
        owner,
        pvc,
        request,
        digest,
    )
    assert (await acquire(state)).allowed
    replay = await state["recovery"].acquire_cleanup_permit(
        owner_kind="job",
        owner_id=owner,
        pvc_uid=pvc,
        request_id=request,
        source="controller_rootdisk_gc",
        intent_digest=digest,
    )
    assert not replay.allowed
    assert replay.completed_outcome is None


@pytest.mark.asyncio
async def test_fresh_retention_and_standalone_gc_admission_have_one_winner(db):
    state = await cancelled(db, old=False, retiring=False)
    retained, gc = await asyncio.gather(
        acquire(state),
        state["recovery"].acquire_cleanup_permit(
            owner_kind="job",
            owner_id=UUID(state["job_id"]),
            pvc_uid=UUID(state["frozen"]["pvc_uid"]),
            request_id=uuid4(),
            source="controller_rootdisk_gc",
            intent_digest="sha256:" + "f" * 64,
        ),
    )
    assert retained.allowed != gc.allowed
