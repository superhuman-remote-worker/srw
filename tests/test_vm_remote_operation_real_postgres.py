"""Real-PostgreSQL fences for exact VM remote-operation leases."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from uuid import UUID, uuid4

import asyncpg
import pytest
import pytest_asyncio
from testcontainers.postgres import PostgresContainer

from orchestrator.database.postgres import PostgresDB
from shared.workspace_contract import workspace_contract_authority_identity


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_FILE = ROOT / "src/orchestrator/database/schema_current.sql"
MIGRATION = ROOT / (
    "src/orchestrator/database/migrations/app/0199_vm_remote_operation_leases.sql"
)
FINGERPRINT = "SHA256:" + "A" * 43


@pytest.fixture(scope="module")
def pg_dsn():
    with PostgresContainer("postgres:15") as postgres:
        yield postgres.get_connection_url().replace("postgresql+psycopg2", "postgresql")


@pytest_asyncio.fixture(scope="module")
async def _schema_applied(pg_dsn):
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(SCHEMA_FILE.read_text())
        present = await conn.fetchval(
            "SELECT to_regclass('public.vm_remote_operation_leases') IS NOT NULL"
        )
        if not present:
            await conn.execute(MIGRATION.read_text())
    finally:
        await conn.close()


@pytest_asyncio.fixture
async def db(pg_dsn, _schema_applied):
    store = PostgresDB(
        connection_string=pg_dsn,
        min_connections=1,
        max_connections=6,
    )
    await store.connect()
    async with store.acquire() as conn:
        await conn.execute(
            "TRUNCATE vm_remote_operation_leases, jobs, threads, users CASCADE"
        )
        # schema_current.sql is intentionally schema-only, while migration
        # 0199 seeds this singleton. Mirror that seed for snapshot-based tests.
        await conn.execute(
            "INSERT INTO vm_remote_operation_protocol_gate ("
            "singleton, protocol_version, activated_at, activated_by) "
            "VALUES (TRUE, 1, NULL, NULL) ON CONFLICT (singleton) DO NOTHING"
        )
        await conn.execute(
            "ALTER TABLE vm_remote_operation_protocol_gate DISABLE TRIGGER "
            "trg_vm_remote_operation_protocol_forward_only; "
            "UPDATE vm_remote_operation_protocol_gate SET "
            "activated_at = NULL, activated_by = NULL WHERE singleton = TRUE; "
            "ALTER TABLE vm_remote_operation_protocol_gate ENABLE TRIGGER "
            "trg_vm_remote_operation_protocol_forward_only"
        )
    try:
        yield store
    finally:
        await store.close()


def _vm_identity() -> tuple[dict, dict[str, str]]:
    ids = {
        "generation": str(uuid4()),
        "vm_uid": str(uuid4()),
        "launcher_uid": str(uuid4()),
    }
    return (
        {
            "status": "ready",
            "provision_generation": ids["generation"],
            "identity_authenticated": True,
            "identity_provision_generation": ids["generation"],
            "vm_uid": ids["vm_uid"],
            "active_pod_uid": ids["launcher_uid"],
            "ssh_host": "10.42.0.91",
            "ssh_port": 22,
            "ssh_host_key_fingerprint": FINGERPRINT,
            "ssh_registration_id": "registration-a",
        },
        ids,
    )


async def _seed_owner(db: PostgresDB, owner_kind: str):
    user_id = uuid4()
    owner_id = uuid4()
    vm, ids = _vm_identity()
    config_override = {"workspace": {"backend": "vm", "size": "medium"}}
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO users (id, display_name, email) VALUES ($1, 'vm-owner', $2)",
            user_id,
            f"{user_id}@example.test",
        )
        if owner_kind == "job":
            await conn.execute(
                "INSERT INTO jobs (id, description, status, user_id, context, "
                "config_override) VALUES ($1, 'vm job', 'processing', $2, "
                "$3::jsonb, $4::jsonb)",
                owner_id,
                user_id,
                json.dumps({"vm": vm}),
                json.dumps(config_override),
            )
        else:
            await conn.execute(
                "INSERT INTO threads (id, user_id, status, execution_lane, metadata) "
                # Keep this trigger-focused fixture outside the separate pinned
                # retirement protocol. VM lease behavior is independent of lane.
                "VALUES ($1, $2, 'active', 'stateless', $3::jsonb)",
                owner_id,
                user_id,
                json.dumps({"vm": vm, "config_override": config_override}),
            )
    return str(owner_id), vm, ids


async def _claim(
    db: PostgresDB,
    owner_kind: str,
    owner_id: str,
    vm: dict,
    *,
    operation_kind: str | None = None,
    claimant: str | None = None,
):
    assert await db.activate_vm_remote_operation_protocol(
        protocol_version=1,
        activated_by="real-postgres-test",
    )
    row = (
        await db.get_job(owner_id)
        if owner_kind == "job"
        else await db.get_thread(owner_id)
    )
    assert row is not None
    state = row["context" if owner_kind == "job" else "metadata"]
    if isinstance(state, str):
        state = json.loads(state)
    config_override = (
        row.get("config_override")
        if owner_kind == "job"
        else state.get("config_override")
    )
    identity = workspace_contract_authority_identity(
        {"context": state, "config_override": config_override}
    )
    assert identity is not None
    workspace_tier, workspace_contract_digest = identity
    row = await db.claim_vm_remote_operation(
        owner_id,
        protocol_version=1,
        owner_kind=owner_kind,
        operation_kind=operation_kind
        or ("thread_upload" if owner_kind == "thread" else "ide_settings"),
        workspace_tier=workspace_tier,
        workspace_contract_digest=workspace_contract_digest,
        workspace_generation=vm["provision_generation"],
        vm_uid=vm["vm_uid"],
        launcher_pod_uid=vm["active_pod_uid"],
        ssh_host=vm["ssh_host"],
        ssh_port=vm["ssh_port"],
        ssh_host_key_fingerprint=vm["ssh_host_key_fingerprint"],
        claimant=claimant or f"test:{owner_kind}",
        lease_seconds=60,
    )
    assert row is not None
    return row


@pytest.mark.asyncio
async def test_different_operation_kinds_share_one_owner_lease(db):
    owner_id, vm, _ids = await _seed_owner(db, "thread")
    upload = await _claim(
        db,
        "thread",
        owner_id,
        vm,
        operation_kind="thread_upload",
        claimant="test:upload",
    )

    workspace_tier, workspace_contract_digest = _contract_identity(vm)
    delete = await db.claim_vm_remote_operation(
        owner_id,
        protocol_version=1,
        owner_kind="thread",
        operation_kind="thread_delete",
        workspace_tier=workspace_tier,
        workspace_contract_digest=workspace_contract_digest,
        workspace_generation=vm["provision_generation"],
        vm_uid=vm["vm_uid"],
        launcher_pod_uid=vm["active_pod_uid"],
        ssh_host=vm["ssh_host"],
        ssh_port=vm["ssh_port"],
        ssh_host_key_fingerprint=vm["ssh_host_key_fingerprint"],
        claimant="test:delete",
        lease_seconds=60,
    )

    assert delete is None
    assert await db.settle_vm_remote_operation(
        str(upload["id"]),
        claim_token=int(upload["claim_token"]),
        claimant="test:upload",
        result_kind="failed",
    )
    assert (
        await db.claim_vm_remote_operation(
            owner_id,
            protocol_version=1,
            owner_kind="thread",
            operation_kind="thread_delete",
            workspace_tier=workspace_tier,
            workspace_contract_digest=workspace_contract_digest,
            workspace_generation=vm["provision_generation"],
            vm_uid=vm["vm_uid"],
            launcher_pod_uid=vm["active_pod_uid"],
            ssh_host=vm["ssh_host"],
            ssh_port=vm["ssh_port"],
            ssh_host_key_fingerprint=vm["ssh_host_key_fingerprint"],
            claimant="test:delete",
            lease_seconds=60,
        )
        is not None
    )


@pytest.mark.asyncio
async def test_profile_pointer_cas_races_component_merge_without_lost_pointer(db):
    owner_id, vm, _ids = await _seed_owner(db, "job")
    lease = await _claim(
        db,
        "job",
        owner_id,
        vm,
        operation_kind="ide_profile",
        claimant="test:profile",
    )
    async with db.acquire() as conn:
        user_id = await conn.fetchval(
            "SELECT user_id FROM jobs WHERE id=$1::uuid", UUID(owner_id)
        )
    pointer = {
        "version": 1,
        "key": f"ide-profiles/{user_id}/globalStorage/{'a' * 64}.tar.zst",
        "sha256": "a" * 64,
        "size": 123,
    }

    published, merged = await asyncio.gather(
        db.cas_user_ide_profile_pointer(
            str(user_id),
            pointer_name="globalStorage",
            expected_pointer=None,
            pointer=pointer,
            vm_operation_id=str(lease["id"]),
            vm_claim_token=int(lease["claim_token"]),
            vm_claimant="test:profile",
            vm_owner_kind="job",
            vm_owner_id=owner_id,
        ),
        db.merge_user_ide_component(
            str(user_id),
            component="extensions",
            patch={"sig": "b" * 64},
        ),
    )
    assert published and merged
    settings = await db.get_user_settings(str(user_id))
    assert settings["ide"]["profile_pointers"]["globalStorage"] == pointer
    assert settings["ide"]["extensions"]["sig"] == "b" * 64


def _contract_identity(vm: dict) -> tuple[str, str]:
    config_override = {"workspace": {"backend": "vm", "size": "medium"}}
    identity = workspace_contract_authority_identity(
        {"context": {"vm": vm}, "config_override": config_override}
    )
    assert identity is not None
    return identity


@pytest.mark.asyncio
async def test_snapshot_capture_kind_claims_and_settles_exact_identity(db):
    owner_id, vm, _ids = await _seed_owner(db, "job")
    lease = await _claim(
        db,
        "job",
        owner_id,
        vm,
        operation_kind="snapshot_capture",
        claimant="test:snapshot",
    )
    workspace_tier, workspace_contract_digest = _contract_identity(vm)

    assert await db.settle_vm_remote_operation(
        str(lease["id"]),
        claim_token=int(lease["claim_token"]),
        claimant="test:snapshot",
        result_kind="succeeded",
        owner_kind="job",
        owner_id=owner_id,
        operation_kind="snapshot_capture",
        workspace_tier=workspace_tier,
        workspace_contract_digest=workspace_contract_digest,
        workspace_generation=vm["provision_generation"],
        vm_uid=vm["vm_uid"],
        launcher_pod_uid=vm["active_pod_uid"],
        ssh_host=vm["ssh_host"],
        ssh_port=vm["ssh_port"],
        ssh_host_key_fingerprint=vm["ssh_host_key_fingerprint"],
    )


@pytest.mark.asyncio
async def test_protocol_is_default_dark_then_monotonically_activates(db):
    owner_id, vm, _ids = await _seed_owner(db, "thread")
    workspace_tier, workspace_contract_digest = _contract_identity(vm)
    kwargs = {
        "protocol_version": 1,
        "owner_kind": "thread",
        "operation_kind": "thread_upload",
        "workspace_tier": workspace_tier,
        "workspace_contract_digest": workspace_contract_digest,
        "workspace_generation": vm["provision_generation"],
        "vm_uid": vm["vm_uid"],
        "launcher_pod_uid": vm["active_pod_uid"],
        "ssh_host": vm["ssh_host"],
        "ssh_port": vm["ssh_port"],
        "ssh_host_key_fingerprint": vm["ssh_host_key_fingerprint"],
        "claimant": "test:protocol",
        "lease_seconds": 60,
    }

    assert await db.claim_vm_remote_operation(owner_id, **kwargs) is None
    assert not await db.activate_vm_remote_operation_protocol(
        protocol_version=2,
        activated_by="old-or-future-replica",
    )
    assert await db.activate_vm_remote_operation_protocol(
        protocol_version=1,
        activated_by="converged-v1",
    )
    # Idempotent replay cannot replace the first activation audit actor.
    assert await db.activate_vm_remote_operation_protocol(
        protocol_version=1,
        activated_by="retrying-v1",
    )
    claimed = await db.claim_vm_remote_operation(owner_id, **kwargs)
    assert claimed is not None
    assert claimed["protocol_version"] == 1
    async with db.acquire() as conn:
        gate = await conn.fetchrow(
            "SELECT protocol_version, activated_at, activated_by FROM "
            "vm_remote_operation_protocol_gate WHERE singleton = TRUE"
        )
    assert gate["protocol_version"] == 1
    assert gate["activated_at"] is not None
    assert gate["activated_by"] == "converged-v1"
    async with db.acquire() as conn:
        with pytest.raises(asyncpg.ObjectInUseError):
            await conn.execute(
                "UPDATE vm_remote_operation_protocol_gate SET "
                "activated_at = NULL, activated_by = NULL WHERE singleton = TRUE"
            )
        with pytest.raises(asyncpg.ObjectInUseError):
            await conn.execute("TRUNCATE vm_remote_operation_protocol_gate")


def _success_identity(owner_kind: str, owner_id: str, vm: dict) -> dict:
    workspace_tier, workspace_contract_digest = _contract_identity(vm)
    return {
        "owner_kind": owner_kind,
        "owner_id": owner_id,
        "workspace_tier": workspace_tier,
        "workspace_contract_digest": workspace_contract_digest,
        "workspace_generation": vm["provision_generation"],
        "vm_uid": vm["vm_uid"],
        "launcher_pod_uid": vm["active_pod_uid"],
        "ssh_host": vm["ssh_host"],
        "ssh_port": vm["ssh_port"],
        "ssh_host_key_fingerprint": vm["ssh_host_key_fingerprint"],
        "operation_kind": (
            "thread_upload" if owner_kind == "thread" else "ide_settings"
        ),
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("owner_kind", ["job", "thread"])
async def test_ordinary_owner_updates_do_not_reference_absent_columns(db, owner_kind):
    owner_id, _vm, _ids = await _seed_owner(db, owner_kind)
    async with db.acquire() as conn:
        if owner_kind == "job":
            result = await conn.execute(
                "UPDATE jobs SET context = context || '{\"diagnostic\":true}'::jsonb "
                "WHERE id = $1",
                UUID(owner_id),
            )
        else:
            result = await conn.execute(
                "UPDATE threads SET metadata = metadata || "
                "'{\"diagnostic\":true}'::jsonb WHERE id = $1",
                UUID(owner_id),
            )
    assert result == "UPDATE 1"


@pytest.mark.asyncio
@pytest.mark.parametrize("owner_kind", ["job", "thread"])
async def test_active_lease_blocks_vm_rebind_then_settlement_reopens(db, owner_kind):
    owner_id, vm, _ids = await _seed_owner(db, owner_kind)
    lease = await _claim(db, owner_kind, owner_id, vm)
    table = "jobs" if owner_kind == "job" else "threads"
    column = "context" if owner_kind == "job" else "metadata"
    async with db.acquire() as conn:
        with pytest.raises(asyncpg.ObjectInUseError):
            await conn.execute(
                f"UPDATE {table} SET {column} = jsonb_set({column}, "
                "'{vm,ssh_host}', '\"10.42.0.92\"'::jsonb) WHERE id = $1",
                UUID(owner_id),
            )
    assert await db.settle_vm_remote_operation(
        str(lease["id"]),
        claim_token=int(lease["claim_token"]),
        claimant=f"test:{owner_kind}",
        result_kind="succeeded",
        **_success_identity(owner_kind, owner_id, vm),
    )
    async with db.acquire() as conn:
        result = await conn.execute(
            f"UPDATE {table} SET {column} = jsonb_set({column}, "
            "'{vm,ssh_host}', '\"10.42.0.92\"'::jsonb) WHERE id = $1",
            UUID(owner_id),
        )
    assert result == "UPDATE 1"


@pytest.mark.asyncio
@pytest.mark.parametrize("owner_kind", ["job", "thread"])
async def test_active_lease_blocks_owner_delete_then_settlement_reopens(db, owner_kind):
    owner_id, vm, _ids = await _seed_owner(db, owner_kind)
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO managed_repository_process_zero_receipts ("
            "owner_kind, owner_id, scope, provisioner, runtime_incarnation) "
            "VALUES ($1, $2, 'vm', 'vm', $3)",
            owner_kind,
            UUID(owner_id),
            vm["provision_generation"],
        )
    lease = await _claim(db, owner_kind, owner_id, vm)
    table = "jobs" if owner_kind == "job" else "threads"
    cleanup_trigger = f"trg_{table}_c_require_workspace_cleanup_before_delete"

    async with db.acquire() as conn:
        # Isolate the 0199 veto from 0197's independent terminal workspace
        # cleanup contract when that sibling migration is present. This test
        # also runs against the schema snapshot plus 0199 alone.
        cleanup_trigger_present = await conn.fetchval(
            "SELECT EXISTS (SELECT 1 FROM pg_trigger WHERE tgrelid=$1::regclass "
            "AND tgname=$2 AND NOT tgisinternal)",
            table,
            cleanup_trigger,
        )
        if cleanup_trigger_present:
            await conn.execute(f"ALTER TABLE {table} DISABLE TRIGGER {cleanup_trigger}")
        try:
            with pytest.raises(asyncpg.ObjectInUseError):
                await conn.execute(
                    f"DELETE FROM {table} WHERE id = $1",
                    UUID(owner_id),
                )

            assert await db.settle_vm_remote_operation(
                str(lease["id"]),
                claim_token=int(lease["claim_token"]),
                claimant=f"test:{owner_kind}",
                result_kind="succeeded",
                **_success_identity(owner_kind, owner_id, vm),
            )
            result = await conn.execute(
                f"DELETE FROM {table} WHERE id = $1",
                UUID(owner_id),
            )
        finally:
            if cleanup_trigger_present:
                await conn.execute(
                    f"ALTER TABLE {table} ENABLE TRIGGER {cleanup_trigger}"
                )
    assert result == "DELETE 1"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("owner_kind", "terminal_status"),
    [("job", "completed"), ("thread", "ended")],
)
async def test_active_lease_blocks_status_only_completion_or_end(
    db, owner_kind, terminal_status
):
    owner_id, vm, _ids = await _seed_owner(db, owner_kind)
    lease = await _claim(db, owner_kind, owner_id, vm)
    table = "jobs" if owner_kind == "job" else "threads"

    async with db.acquire() as conn:
        with pytest.raises(asyncpg.ObjectInUseError):
            await conn.execute(
                f"UPDATE {table} SET status = $2 WHERE id = $1",
                UUID(owner_id),
                terminal_status,
            )

    assert await db.settle_vm_remote_operation(
        str(lease["id"]),
        claim_token=int(lease["claim_token"]),
        claimant=f"test:{owner_kind}",
        result_kind="succeeded",
        **_success_identity(owner_kind, owner_id, vm),
    )
    async with db.acquire() as conn:
        assert (
            await conn.execute(
                f"UPDATE {table} SET status = $2 WHERE id = $1",
                UUID(owner_id),
                terminal_status,
            )
            == "UPDATE 1"
        )


@pytest.mark.asyncio
async def test_terminal_owner_cannot_renew_even_if_fault_injected_lease_is_live(db):
    owner_id, vm, _ids = await _seed_owner(db, "thread")
    lease = await _claim(db, "thread", owner_id, vm)
    async with db.acquire() as conn:
        # Expiry makes the status transition legal; the second UPDATE models
        # an old/faulty writer resurrecting only the lease row afterward.
        await conn.execute(
            "UPDATE vm_remote_operation_leases SET "
            "claimed_at = now() - interval '2 seconds', "
            "lease_expires_at = now() - interval '1 second' WHERE id = $1",
            lease["id"],
        )
        await conn.execute(
            "UPDATE threads SET status = 'ended' WHERE id = $1",
            UUID(owner_id),
        )
        await conn.execute(
            "UPDATE vm_remote_operation_leases SET claimed_at = now(), "
            "lease_expires_at = now() + interval '60 seconds' WHERE id = $1",
            lease["id"],
        )

    assert (
        await db.renew_vm_remote_operation(
            str(lease["id"]),
            claim_token=int(lease["claim_token"]),
            claimant="test:thread",
            lease_seconds=60,
        )
        is None
    )


@pytest.mark.asyncio
async def test_stale_claimant_cannot_renew_after_database_time_expiry(db):
    owner_id, vm, _ids = await _seed_owner(db, "thread")
    lease = await _claim(db, "thread", owner_id, vm)
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE vm_remote_operation_leases "
            "SET claimed_at = now() - interval '2 seconds', "
            "lease_expires_at = now() - interval '1 second' WHERE id = $1",
            lease["id"],
        )
    assert (
        await db.renew_vm_remote_operation(
            str(lease["id"]),
            claim_token=int(lease["claim_token"]),
            claimant="test:thread",
            lease_seconds=60,
        )
        is None
    )


@pytest.mark.asyncio
async def test_expired_claimant_cannot_settle_success_without_successor(db):
    owner_id, vm, _ids = await _seed_owner(db, "thread")
    lease = await _claim(db, "thread", owner_id, vm)
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE vm_remote_operation_leases "
            "SET claimed_at = now() - interval '2 seconds', "
            "lease_expires_at = now() - interval '1 second' WHERE id = $1",
            lease["id"],
        )

    assert not await db.settle_vm_remote_operation(
        str(lease["id"]),
        claim_token=int(lease["claim_token"]),
        claimant="test:thread",
        result_kind="succeeded",
        **_success_identity("thread", owner_id, vm),
    )
    async with db.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT settled_at, result_kind FROM vm_remote_operation_leases "
            "WHERE id = $1",
            lease["id"],
        )
    assert row["settled_at"] is None
    assert row["result_kind"] is None


@pytest.mark.asyncio
async def test_reclaimed_predecessor_cannot_settle_or_report_success(db):
    owner_id, vm, _ids = await _seed_owner(db, "thread")
    predecessor = await _claim(db, "thread", owner_id, vm)
    workspace_tier, workspace_contract_digest = _contract_identity(vm)
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE vm_remote_operation_leases "
            "SET claimed_at = now() - interval '2 seconds', "
            "lease_expires_at = now() - interval '1 second' WHERE id = $1",
            predecessor["id"],
        )
    successor = await db.claim_vm_remote_operation(
        owner_id,
        protocol_version=1,
        owner_kind="thread",
        operation_kind="thread_upload",
        workspace_tier=workspace_tier,
        workspace_contract_digest=workspace_contract_digest,
        workspace_generation=vm["provision_generation"],
        vm_uid=vm["vm_uid"],
        launcher_pod_uid=vm["active_pod_uid"],
        ssh_host=vm["ssh_host"],
        ssh_port=vm["ssh_port"],
        ssh_host_key_fingerprint=vm["ssh_host_key_fingerprint"],
        claimant="test:successor",
        lease_seconds=60,
    )
    assert successor is not None
    assert successor["id"] == predecessor["id"]
    assert successor["claim_token"] != predecessor["claim_token"]

    assert not await db.settle_vm_remote_operation(
        str(predecessor["id"]),
        claim_token=int(predecessor["claim_token"]),
        claimant="test:thread",
        result_kind="succeeded",
        **_success_identity("thread", owner_id, vm),
    )
    assert await db.settle_vm_remote_operation(
        str(successor["id"]),
        claim_token=int(successor["claim_token"]),
        claimant="test:successor",
        result_kind="succeeded",
        **_success_identity("thread", owner_id, vm),
    )


async def _set_workspace_backend(
    db: PostgresDB, owner_kind: str, owner_id: str, backend: str
) -> None:
    table = "jobs" if owner_kind == "job" else "threads"
    if owner_kind == "job":
        expression = (
            "config_override = jsonb_set(config_override, "
            "'{workspace,backend}', to_jsonb($2::text), true)"
        )
    else:
        expression = (
            "metadata = jsonb_set(metadata, "
            "'{config_override,workspace,backend}', to_jsonb($2::text), true)"
        )
    async with db.acquire() as conn:
        await conn.execute(
            f"UPDATE {table} SET {expression} WHERE id = $1",
            UUID(owner_id),
            backend,
        )


async def _set_workspace_size(
    db: PostgresDB, owner_kind: str, owner_id: str, size: str
) -> None:
    table = "jobs" if owner_kind == "job" else "threads"
    if owner_kind == "job":
        expression = (
            "config_override = jsonb_set(config_override, "
            "'{workspace,size}', to_jsonb($2::text), true)"
        )
    else:
        expression = (
            "metadata = jsonb_set(metadata, "
            "'{config_override,workspace,size}', to_jsonb($2::text), true)"
        )
    async with db.acquire() as conn:
        await conn.execute(
            f"UPDATE {table} SET {expression} WHERE id = $1",
            UUID(owner_id),
            size,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("owner_kind", ["job", "thread"])
async def test_active_lease_blocks_selected_tier_transition_with_stale_vm(
    db, owner_kind
):
    owner_id, vm, _ids = await _seed_owner(db, owner_kind)
    await _claim(db, owner_kind, owner_id, vm)

    with pytest.raises(asyncpg.ObjectInUseError):
        await _set_workspace_backend(db, owner_kind, owner_id, "sandbox")


@pytest.mark.asyncio
@pytest.mark.parametrize("owner_kind", ["job", "thread"])
async def test_active_lease_blocks_stamped_contract_transition(db, owner_kind):
    owner_id, vm, _ids = await _seed_owner(db, owner_kind)
    table = "jobs" if owner_kind == "job" else "threads"
    state_column = "context" if owner_kind == "job" else "metadata"
    vm_contract = {
        "version": 1,
        "requested_backend": "vm",
        "assigned_backend": "vm",
        "assignment_source": "real-pg-test",
    }
    async with db.acquire() as conn:
        await conn.execute(
            f"UPDATE {table} SET {state_column} = jsonb_set({state_column}, "
            "'{_workspace_contract}', $2::jsonb, true) WHERE id = $1",
            UUID(owner_id),
            json.dumps(vm_contract),
        )
    await _claim(db, owner_kind, owner_id, vm)
    sandbox_contract = dict(vm_contract, assigned_backend="sandbox")

    async with db.acquire() as conn:
        with pytest.raises(asyncpg.ObjectInUseError):
            if owner_kind == "job":
                await conn.execute(
                    "UPDATE jobs SET context = jsonb_set(context, "
                    "'{_workspace_contract}', $2::jsonb, true), "
                    "config_override = jsonb_set(config_override, "
                    "'{workspace,backend}', '\"sandbox\"'::jsonb, true) "
                    "WHERE id = $1",
                    UUID(owner_id),
                    json.dumps(sandbox_contract),
                )
            else:
                await conn.execute(
                    "UPDATE threads SET metadata = jsonb_set(jsonb_set(metadata, "
                    "'{_workspace_contract}', $2::jsonb, true), "
                    "'{config_override,workspace,backend}', "
                    "'\"sandbox\"'::jsonb, true) WHERE id = $1",
                    UUID(owner_id),
                    json.dumps(sandbox_contract),
                )


@pytest.mark.asyncio
@pytest.mark.parametrize("owner_kind", ["job", "thread"])
async def test_expired_lease_cannot_renew_or_settle_after_vm_to_sandbox_transition(
    db, owner_kind
):
    owner_id, vm, _ids = await _seed_owner(db, owner_kind)
    lease = await _claim(db, owner_kind, owner_id, vm)
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE vm_remote_operation_leases SET "
            "claimed_at = now() - interval '2 seconds', "
            "lease_expires_at = now() - interval '1 second' WHERE id = $1",
            lease["id"],
        )
    await _set_workspace_backend(db, owner_kind, owner_id, "sandbox")
    async with db.acquire() as conn:
        # Fault injection isolates the owner-contract proof from the ordinary
        # expiry predicate. Stale vm residue still exists on the owner row.
        await conn.execute(
            "UPDATE vm_remote_operation_leases SET claimed_at = now(), "
            "lease_expires_at = now() + interval '60 seconds' WHERE id = $1",
            lease["id"],
        )

    assert (
        await db.renew_vm_remote_operation(
            str(lease["id"]),
            claim_token=int(lease["claim_token"]),
            claimant=f"test:{owner_kind}",
            lease_seconds=60,
        )
        is None
    )
    assert not await db.settle_vm_remote_operation(
        str(lease["id"]),
        claim_token=int(lease["claim_token"]),
        claimant=f"test:{owner_kind}",
        result_kind="succeeded",
        **_success_identity(owner_kind, owner_id, vm),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("owner_kind", ["job", "thread"])
async def test_workspace_config_digest_drift_invalidates_expired_receipt(
    db, owner_kind
):
    owner_id, vm, _ids = await _seed_owner(db, owner_kind)
    lease = await _claim(db, owner_kind, owner_id, vm)
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE vm_remote_operation_leases SET "
            "claimed_at = now() - interval '2 seconds', "
            "lease_expires_at = now() - interval '1 second' WHERE id = $1",
            lease["id"],
        )
    await _set_workspace_size(db, owner_kind, owner_id, "large")
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE vm_remote_operation_leases SET claimed_at = now(), "
            "lease_expires_at = now() + interval '60 seconds' WHERE id = $1",
            lease["id"],
        )

    assert (
        await db.renew_vm_remote_operation(
            str(lease["id"]),
            claim_token=int(lease["claim_token"]),
            claimant=f"test:{owner_kind}",
            lease_seconds=60,
        )
        is None
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("owner_kind", ["job", "thread"])
async def test_exact_reclaim_races_workspace_tier_transition_atomically(db, owner_kind):
    owner_id, vm, _ids = await _seed_owner(db, owner_kind)
    predecessor = await _claim(db, owner_kind, owner_id, vm)
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE vm_remote_operation_leases SET "
            "claimed_at = now() - interval '2 seconds', "
            "lease_expires_at = now() - interval '1 second' WHERE id = $1",
            predecessor["id"],
        )

    async def reclaim():
        return await db.claim_vm_remote_operation(
            owner_id,
            protocol_version=1,
            owner_kind=owner_kind,
            operation_kind=(
                "thread_upload" if owner_kind == "thread" else "ide_settings"
            ),
            workspace_tier=str(predecessor["workspace_tier"]),
            workspace_contract_digest=str(predecessor["workspace_contract_digest"]),
            workspace_generation=vm["provision_generation"],
            vm_uid=vm["vm_uid"],
            launcher_pod_uid=vm["active_pod_uid"],
            ssh_host=vm["ssh_host"],
            ssh_port=vm["ssh_port"],
            ssh_host_key_fingerprint=vm["ssh_host_key_fingerprint"],
            claimant=f"test:{owner_kind}:reclaim",
            lease_seconds=60,
        )

    async def transition() -> bool:
        try:
            await _set_workspace_backend(db, owner_kind, owner_id, "sandbox")
            return True
        except asyncpg.ObjectInUseError:
            return False

    reclaimed, transitioned = await asyncio.gather(reclaim(), transition())
    assert (reclaimed is not None) != transitioned

    row = (
        await db.get_job(owner_id)
        if owner_kind == "job"
        else await db.get_thread(owner_id)
    )
    assert row is not None
    state = row["context" if owner_kind == "job" else "metadata"]
    if isinstance(state, str):
        state = json.loads(state)
    config = (
        row.get("config_override") if owner_kind == "job" else state["config_override"]
    )
    if isinstance(config, str):
        config = json.loads(config)
    assert config["workspace"]["backend"] == ("sandbox" if transitioned else "vm")


@pytest.mark.asyncio
async def test_canvas_vm_save_is_one_owner_exclusive_operation(db, monkeypatch):
    """A Canvas save into a VM holds the upload's exact lease for its duration.

    It claims before the save's thread FOR SHARE lock, so neither the claim nor
    the settlement (each a thread FOR UPDATE) waits on the save, and an upload
    for the same owner is refused while it runs.
    """

    from types import SimpleNamespace

    from orchestrator.services import canvas_files
    from orchestrator.services import vm_provisioner as vm_provisioner_module

    monkeypatch.setenv("VM_REMOTE_OPERATION_PROTOCOL_ENABLED", "true")
    monkeypatch.setenv("VM_MODE", "same-cluster")
    owner_id, vm, _ids = await _seed_owner(db, "thread")
    # Same-cluster readiness as vm_readiness publishes it.
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE threads SET metadata = jsonb_set(metadata, "
            "'{vm,ssh_ready_source}', '\"provisioner_probe\"') WHERE id=$1",
            UUID(owner_id),
        )
    assert await db.activate_vm_remote_operation_protocol(
        protocol_version=1,
        activated_by="real-postgres-test",
    )

    class Provisioner:
        async def attest_workspace_runtime(self, owner, *, entity_type):
            assert (owner, entity_type) == (owner_id, "thread")
            return SimpleNamespace(
                workspace_generation=vm["provision_generation"],
                runtime_incarnation=vm["active_pod_uid"],
                launcher_pod_uid=vm["active_pod_uid"],
                vm_uid=vm["vm_uid"],
                host=vm["ssh_host"],
                port=vm["ssh_port"],
                ssh_host_key_fingerprint=vm["ssh_host_key_fingerprint"],
            )

    monkeypatch.setattr(vm_provisioner_module, "vm_provisioner", Provisioner())
    thread = await db.get_thread(owner_id)

    async with canvas_files.vm_canvas_write_authority(thread, store=db):
        identity = canvas_files._VM_CANVAS_WRITE_AUTHORITY.get()
        assert identity.workspace_generation == vm["provision_generation"]
        # The save's own row lock, as CanvasService.edit_file takes it.
        async with db.acquire() as conn, conn.transaction():
            await conn.fetchrow(
                "SELECT id FROM threads WHERE id=$1 FOR SHARE", UUID(owner_id)
            )
        # An upload for the same owner cannot start meanwhile.
        refused = await db.claim_vm_remote_operation(
            owner_id,
            protocol_version=1,
            owner_kind="thread",
            operation_kind="thread_upload",
            workspace_tier="vm",
            workspace_contract_digest=identity.workspace_contract_digest,
            workspace_generation=vm["provision_generation"],
            vm_uid=vm["vm_uid"],
            launcher_pod_uid=vm["active_pod_uid"],
            ssh_host=vm["ssh_host"],
            ssh_port=vm["ssh_port"],
            ssh_host_key_fingerprint=vm["ssh_host_key_fingerprint"],
            claimant="test:upload",
            lease_seconds=60,
        )
        assert refused is None

    async with db.acquire() as conn:
        rows = await conn.fetch(
            "SELECT operation_kind, result_kind, settled_at FROM "
            "vm_remote_operation_leases WHERE owner_id=$1",
            UUID(owner_id),
        )
    assert [(r["operation_kind"], r["result_kind"]) for r in rows] == [
        ("thread_upload", "succeeded")
    ]
    assert rows[0]["settled_at"] is not None
    assert canvas_files._VM_CANVAS_WRITE_AUTHORITY.get() is None
