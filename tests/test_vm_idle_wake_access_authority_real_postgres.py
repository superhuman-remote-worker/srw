"""Wake access intent must compose with native creation authority."""

import asyncio
from uuid import uuid4

import pytest

from orchestrator.services.vm_creation_retry_store import VMCreationRetryStore
from orchestrator.services.vm_idle_access import VMIdleAccessStore
from tests.test_vm_idle_admission_handoff_real_postgres import (
    db as _runtime_db,
    runtime_schema,  # noqa: F401
    _db_fixture,  # noqa: F401
    whole_schema,  # noqa: F401
    _base_db,  # noqa: F401
    _schema_applied,  # noqa: F401
    postgres_db_fixture,  # noqa: F401
    pg_dsn,  # noqa: F401
    application_service,
    resolve_wake,
    suspended_charged_job,
)

db = _runtime_db


async def waiting_access_wake(db, monkeypatch):
    policy, retry, operation, wake, identity = await suspended_charged_job(
        db, monkeypatch
    )
    access = VMIdleAccessStore(db)
    leases = await asyncio.gather(
        *(
            access.request(
                owner_kind="job",
                owner_id=str(retry["job_id"]),
                kind="ide",
                user_id=str(uuid4()),
                connection_id=str(uuid4()),
            )
            for _ in range(2)
        )
    )
    assert all(leases) and all(x["wake_id"] == wake["wake_id"] for x in leases)
    assert await application_service(db, monkeypatch).reconcile_once() == 1
    source = await resolve_wake(db, monkeypatch, policy, wake, identity)
    admission = await policy.admit(request_id=str(source["request_id"]))
    assert admission["action"] == "admitted"
    store = VMCreationRetryStore(db)
    claim = (await store.claim_due(limit=1))[0]
    arguments = dict(
        request_id=str(source["request_id"]),
        claim_token=str(claim["claim_token"]),
        observed={
            "job_id": str(retry["job_id"]),
            "provision_generation": str(wake["wake_generation"]),
            "request_digest": claim["request_digest"],
            "controller_configuration_digest": claim["controller_configuration_digest"],
            "expected_pvc_uid": identity["pvc_uid"],
        },
    )
    return policy, retry, operation, wake, identity, leases, source, store, arguments


@pytest.mark.asyncio
async def test_native_wake_authorization_accepts_its_live_requesting_access(
    db, monkeypatch
):
    _, retry, _, _, _, leases, source, store, arguments = await waiting_access_wake(
        db, monkeypatch
    )
    before = await db.fetchrow(
        "SELECT status,freeze_data FROM jobs WHERE id=$1", retry["job_id"]
    )
    grant = await store.authorize_controller(**arguments)
    assert grant["allowed"], grant
    assert dict(
        await db.fetchrow(
            "SELECT status,freeze_data FROM jobs WHERE id=$1", retry["job_id"]
        )
    ) == dict(before)
    assert before["status"] == "waiting_for_reply"
    assert (
        await db.fetchval(
            "SELECT state FROM run_queue WHERE unit_id=$1", retry["job_id"]
        )
        == "done"
    )
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_creation_retries WHERE job_id=$1", retry["job_id"]
        )
        == 2
    )
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_resource_reservations WHERE request_id=$1",
            source["request_id"],
        )
        == 1
    )
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_idle_access_leases WHERE owner_id=$1 AND closed_at IS NULL AND expires_at>clock_timestamp()",
            retry["job_id"],
        )
        == 2
    )
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_creation_effects WHERE request_id=$1",
            source["request_id"],
        )
        == 0
    )
    assert all(lease["closed_at"] is None for lease in leases)


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["null_wake", "wake", "generation", "vm"])
async def test_any_additional_unrelated_live_lease_still_blocks(
    db, monkeypatch, changed
):
    _, retry, _, _, _, leases, source, store, args = await waiting_access_wake(
        db, monkeypatch
    )
    lease = dict(leases[0])
    lease["wake_id"] = (
        None
        if changed == "null_wake"
        else uuid4()
        if changed == "wake"
        else lease["wake_id"]
    )
    lease["provision_generation"] = (
        uuid4() if changed == "generation" else lease["provision_generation"]
    )
    lease["vm_uid"] = uuid4() if changed == "vm" else lease["vm_uid"]
    await db.execute(
        "INSERT INTO vm_idle_access_leases(owner_kind,owner_id,kind,claimed_by,wake_id,provision_generation,vm_uid,expires_at,max_expires_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)",
        lease["owner_kind"],
        lease["owner_id"],
        lease["kind"],
        "unrelated",
        lease["wake_id"],
        lease["provision_generation"],
        lease["vm_uid"],
        lease["expires_at"],
        lease["max_expires_at"],
    )
    result = await store.authorize_controller(**args)
    assert result == {"allowed": False, "reason": "active_workspace_access"}
    assert (
        await db.fetchval(
            "SELECT creation_admission_id FROM vm_creation_retries WHERE request_id=$1",
            source["request_id"],
        )
        is None
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changed",
    [
        "preflight_request",
        "nonwake",
        "preflight_digest",
        "predecessor",
        "cleanup_id",
        "rootdisk",
        "wake_ready",
        "stop",
        "zero",
        "closed",
        "current_generation",
        "claim_expired",
        "deadline",
        "cancelled",
    ],
)
async def test_changed_native_authority_never_exempts_live_access(
    db, monkeypatch, changed
):
    import json
    from asyncpg import CheckViolationError

    _, retry, operation, _, _, _, source, store, args = await waiting_access_wake(
        db, monkeypatch
    )
    if changed in {
        "preflight_request",
        "nonwake",
        "preflight_digest",
        "predecessor",
        "cleanup_id",
        "current_generation",
    }:
        context = json.loads(
            await db.fetchval("SELECT context FROM jobs WHERE id=$1", retry["job_id"])
        )
        prior = context["vm"]["creation_preflight"]
        if changed == "preflight_request":
            prior["request_id"] = str(uuid4())
        elif changed == "nonwake":
            context["vm"].pop("idle_wake_operation_id")
        elif changed == "preflight_digest":
            prior["request_digest"] = "sha256:" + "0" * 64
        elif changed == "predecessor":
            prior["predecessor_evidence"]["vm_uid"] = str(uuid4())
        elif changed == "cleanup_id":
            prior["predecessor_cleanup_admission_id"] = str(uuid4())
        else:
            context["vm"]["provision_generation"] = str(uuid4())
        if changed == "current_generation":
            with pytest.raises(
                CheckViolationError, match="VM process-zero authority is required"
            ):
                await db.execute(
                    "UPDATE jobs SET context=$2::jsonb WHERE id=$1",
                    retry["job_id"],
                    json.dumps(context),
                )
            assert await db.fetchval(
                "SELECT context->'vm'->>'provision_generation' FROM jobs WHERE id=$1",
                retry["job_id"],
            ) == str(source["provision_generation"])
            return
        await db.execute(
            "UPDATE jobs SET context=$2::jsonb WHERE id=$1",
            retry["job_id"],
            json.dumps(context),
        )
    elif changed == "rootdisk":
        with pytest.raises(CheckViolationError, match="source identity is immutable"):
            await db.execute(
                "UPDATE vm_idle_operations SET retained_kind='snapshot' WHERE id=$1",
                operation["id"],
            )
        assert (
            await db.fetchval(
                "SELECT retained_kind FROM vm_idle_operations WHERE id=$1",
                operation["id"],
            )
            == "rootdisk"
        )
        return
    elif changed == "wake_ready":
        await db.execute(
            "UPDATE vm_idle_operations SET wake_ready_at=clock_timestamp() WHERE id=$1",
            operation["id"],
        )
    elif changed == "stop":
        await db.execute(
            "UPDATE vm_idle_operations SET stop_evidence=jsonb_set(stop_evidence,'{controller_authenticated}','false') WHERE id=$1",
            operation["id"],
        )
    elif changed == "zero":
        await db.execute("DELETE FROM managed_repository_process_zero_receipts")
    elif changed == "closed":
        await db.execute(
            "UPDATE vm_idle_operations SET phase='superseded',closed_at=clock_timestamp() WHERE id=$1",
            operation["id"],
        )
    elif changed == "claim_expired":
        await db.execute(
            "UPDATE vm_creation_retries SET claim_expires_at=clock_timestamp()-interval '1 second' WHERE request_id=$1",
            source["request_id"],
        )
    elif changed == "deadline":
        await db.execute(
            "UPDATE srw_execution_specs SET created_at=created_at-interval '2 hours' WHERE work_id=$1",
            retry["job_id"],
        )
    else:
        # Normal Cancel remains fenced by live access even for an exact wake.
        assert not await db.cancel_job(str(retry["job_id"]))
        assert (
            await db.fetchval("SELECT status FROM jobs WHERE id=$1", retry["job_id"])
            == "waiting_for_reply"
        )
        assert (
            await db.fetchval(
                "SELECT creation_admission_id FROM vm_creation_retries WHERE request_id=$1",
                source["request_id"],
            )
            is None
        )
        return
    result = await store.authorize_controller(**args)
    assert not result["allowed"], result
    assert (
        await db.fetchval(
            "SELECT creation_admission_id FROM vm_creation_retries WHERE request_id=$1",
            source["request_id"],
        )
        is None
    )
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_creation_effects WHERE request_id=$1",
            source["request_id"],
        )
        == 0
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["source", "request", "intent"])
async def test_generic_or_mismatched_permit_cannot_claim_wake_exception(
    db, monkeypatch, changed
):
    from uuid import NAMESPACE_URL, uuid5
    from orchestrator.services.vm_creation_retry_store import _creation_intent
    from orchestrator.services.vm_workspace_recovery_store import (
        cleanup_intent_digest,
        VMWorkspaceRecoveryStore,
    )

    _, retry, _, _, identity, _, source, _, _ = await waiting_access_wake(
        db, monkeypatch
    )
    result = await VMWorkspaceRecoveryStore(db).acquire_cleanup_permit(
        owner_kind="job",
        owner_id=retry["job_id"],
        pvc_uid=source["expected_pvc_uid"],
        request_id=uuid4()
        if changed == "request"
        else uuid5(NAMESPACE_URL, "vm-create:" + str(source["request_id"])),
        source="public_vm_delete" if changed == "source" else "controller_vm_create",
        intent_digest="sha256:" + "1" * 64
        if changed == "intent"
        else cleanup_intent_digest(_creation_intent(source)),
    )
    assert not result.allowed and result.reason == "active_workspace_access"


@pytest.mark.asyncio
async def test_concurrent_authorization_replays_one_permit_without_closing_access(
    db, monkeypatch
):
    _, retry, _, _, _, leases, source, store, args = await waiting_access_wake(
        db, monkeypatch
    )
    results = await asyncio.gather(
        *(store.authorize_controller(**args) for _ in range(2))
    )
    allowed = [result for result in results if result["allowed"]]
    assert allowed
    assert all(
        result["allowed"] or result["reason"] == "workspace_cleanup_already_admitted"
        for result in results
    )
    replay = await store.authorize_controller(**args)
    assert replay["allowed"] and all(
        result["admission_id"] == replay["admission_id"] for result in allowed
    )
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_workspace_cleanup_admissions WHERE completed_at IS NULL AND owner_id=$1",
            retry["job_id"],
        )
        == 1
    )
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_idle_access_leases WHERE id=ANY($1::uuid[]) AND closed_at IS NULL AND expires_at>clock_timestamp()",
            [x["id"] for x in leases],
        )
        == 2
    )


@pytest.mark.asyncio
async def test_owner_lock_revalidates_access_lineage_after_wait(db, monkeypatch):
    _, retry, _, _, _, leases, _, store, args = await waiting_access_wake(
        db, monkeypatch
    )
    async with db.acquire() as conn, conn.transaction():
        await conn.fetchrow(
            "SELECT id FROM jobs WHERE id=$1 FOR UPDATE", retry["job_id"]
        )
        pending = asyncio.create_task(store.authorize_controller(**args))
        await asyncio.sleep(0.05)
        assert not pending.done()
        await conn.execute(
            "UPDATE vm_idle_access_leases SET wake_id=$2 WHERE id=$1",
            leases[0]["id"],
            uuid4(),
        )
    result = await asyncio.wait_for(pending, 3)
    assert result == {"allowed": False, "reason": "active_workspace_access"}
