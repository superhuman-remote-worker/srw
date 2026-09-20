"""Fixed cancellation effects share the original owner/PVC cleanup authority."""

import json
from uuid import UUID, uuid4

import pytest

from orchestrator.services.vm_creation_disposition_store import (
    VMCreationDispositionStore,
)
from orchestrator.services.vm_creation_retry_store import VMCreationRetryConflict
from tests.test_vm_creation_disposition_real_postgres import cancelled_disk, freeze
from tests.test_vm_creation_effects_real_postgres import (
    db as _db_fixture,
    postgres_db_fixture,  # noqa: F401
    pg_dsn,  # noqa: F401
    _schema_applied,  # noqa: F401
    observed_creation,
)

db = _db_fixture


async def partial(db, monkeypatch, *, secret=False):
    if secret:
        store, row, carrier, _ = await observed_creation(
            db, monkeypatch, stop_after="cloud_init"
        )
        await db.linearize_pinned_cancel(str(row["job_id"]), expected_status="paused")
    else:
        store, row, carrier, _ = await cancelled_disk(db, monkeypatch)
    disposition = (await freeze(store, row, carrier))["disposition"]
    return VMCreationDispositionStore(store), row, carrier, disposition


@pytest.mark.asyncio
async def test_exact_root_child_is_single_flight_and_does_not_block_parent(
    db, monkeypatch
):
    service, row, carrier, disposition = await partial(db, monkeypatch)
    kwargs = dict(request_id=str(row["request_id"]), carrier=carrier, stage="rootdisk")
    grant = await service.authorize(**kwargs)
    assert grant["operation"] == "purge_rootdisk"
    assert grant["resource"] == disposition["objects"]["rootdisk"]
    assert await service.authorize(**kwargs) == grant
    assert (await service.retries.inspect(request_id=str(row["request_id"])))[
        "state"
    ] == "cancel_requested"
    assert await freeze(service.retries, row, carrier) == {
        "frozen": True,
        "disposition": disposition,
    }
    async with db.acquire() as conn:
        child = await conn.fetchrow(
            "SELECT * FROM vm_workspace_cleanup_admissions WHERE id=$1",
            UUID(grant["cleanup"]["admission_id"]),
        )
        assert str(child["parent_admission_id"]) == disposition["admission_id"]
        assert child["completed_at"] is None
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM vm_workspace_cleanup_admissions WHERE parent_admission_id=$1",
                child["parent_admission_id"],
            )
            == 1
        )


@pytest.mark.asyncio
async def test_root_progress_requires_exact_completed_sql_child(db, monkeypatch):
    service, row, carrier, _ = await partial(db, monkeypatch)
    kwargs = dict(request_id=str(row["request_id"]), carrier=carrier, stage="rootdisk")
    grant = await service.authorize(**kwargs)
    evidence = grant["completion"]
    with pytest.raises(
        VMCreationRetryConflict, match="creation_disposition_incomplete"
    ):
        await service.record(**kwargs, evidence=evidence)
    cleanup = grant["cleanup"]
    assert await service.retries.cleanup.complete_cleanup_permit(
        UUID(cleanup["admission_id"]),
        outcome="deleted",
        request_id=UUID(cleanup["request_id"]),
        intent_digest=cleanup["intent_digest"],
    )
    wrong = {**evidence, "admission_id": str(uuid4())}
    with pytest.raises(
        VMCreationRetryConflict, match="creation_disposition_evidence_changed"
    ):
        await service.record(**kwargs, evidence=wrong)
    result = await service.record(**kwargs, evidence=evidence)
    assert result == {"recorded": True, "stage": "rootdisk", "evidence": evidence}
    assert await service.record(**kwargs, evidence=evidence) == result
    async with db.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT state,cancellation_progress FROM vm_creation_retries WHERE request_id=$1",
            row["request_id"],
        )
        assert row["state"] == "cancel_requested"
        assert json.loads(row["cancellation_progress"]) == {"rootdisk": evidence}


@pytest.mark.asyncio
async def test_typed_secret_progress_cannot_be_replaced_by_boolean_or_other_uid(
    db, monkeypatch
):
    service, row, carrier, disposition = await partial(db, monkeypatch, secret=True)
    kwargs = dict(
        request_id=str(row["request_id"]), carrier=carrier, stage="cloud_init"
    )
    grant = await service.authorize(**kwargs)
    assert grant["operation"] == "delete_secret"
    assert grant["resource"] == disposition["objects"]["cloud_init"]
    for evidence in ({"done": True}, {**grant["completion"], "uid": str(uuid4())}):
        with pytest.raises(
            VMCreationRetryConflict, match="creation_disposition_evidence_changed"
        ):
            await service.record(**kwargs, evidence=evidence)
    assert (await service.record(**kwargs, evidence=grant["completion"]))[
        "recorded"
    ] is True


@pytest.mark.asyncio
async def test_no_secret_effect_is_not_a_secret_delete_grant(db, monkeypatch):
    service, row, carrier, _ = await partial(db, monkeypatch)
    with pytest.raises(
        VMCreationRetryConflict, match="creation_disposition_stage_unavailable"
    ):
        await service.authorize(
            request_id=str(row["request_id"]), carrier=carrier, stage="cloud_init"
        )


@pytest.mark.asyncio
async def test_forged_child_parent_or_digest_cannot_borrow_disposition(db, monkeypatch):
    service, row, carrier, _ = await partial(db, monkeypatch)
    grant = await service.authorize(
        request_id=str(row["request_id"]), carrier=carrier, stage="rootdisk"
    )
    cleanup = grant["cleanup"]
    base = dict(
        owner_kind="job",
        owner_id=row["job_id"],
        pvc_uid=UUID(grant["resource"]["pvc_uid"]),
        request_id=UUID(cleanup["request_id"]),
        source=cleanup["source"],
        intent_digest=cleanup["intent_digest"],
        parent_cleanup=cleanup["parent_cleanup"],
        parent_provision_generation=str(row["provision_generation"]),
    )
    for altered in (
        {"intent_digest": "sha256:" + "0" * 64},
        {"parent_cleanup": {**base["parent_cleanup"], "disposition_id": str(uuid4())}},
        {"request_id": uuid4()},
    ):
        permit = await service.retries.cleanup.acquire_cleanup_permit(
            **{**base, **altered}
        )
        assert permit.allowed is False
        assert permit.reason == "parent_cleanup_identity_changed"


@pytest.mark.asyncio
async def test_unrelated_child_still_blocks_parent_scope(db, monkeypatch):
    service, row, carrier, disposition = await partial(db, monkeypatch)
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO vm_workspace_cleanup_admissions(id,owner_kind,owner_id,pvc_uid,source,request_id,intent_digest,parent_admission_id) VALUES($1,'job',$2,$3,'controller_rootdisk_delete',$4,'unrelated',$5)",
            uuid4(),
            row["job_id"],
            UUID(disposition["objects"]["rootdisk"]["pvc_uid"]),
            uuid4(),
            UUID(disposition["admission_id"]),
        )
    with pytest.raises(
        VMCreationRetryConflict, match="workspace_cleanup_already_admitted"
    ):
        await service.authorize(
            request_id=str(row["request_id"]), carrier=carrier, stage="rootdisk"
        )


@pytest.mark.asyncio
async def test_two_replicas_share_one_child_and_replay_completed_authority(
    db, monkeypatch
):
    import asyncio

    service, row, carrier, _ = await partial(db, monkeypatch)
    kwargs = dict(request_id=str(row["request_id"]), carrier=carrier, stage="rootdisk")
    first, second = await asyncio.gather(
        service.authorize(**kwargs), service.authorize(**kwargs)
    )
    assert first == second
    cleanup = first["cleanup"]
    assert await service.retries.cleanup.complete_cleanup_permit(
        UUID(cleanup["admission_id"]),
        outcome="deleted",
        request_id=UUID(cleanup["request_id"]),
        intent_digest=cleanup["intent_digest"],
    )
    assert await service.authorize(**kwargs) == first
    replay = await service.retries.cleanup.acquire_cleanup_permit(
        owner_kind="job",
        owner_id=row["job_id"],
        pvc_uid=UUID(first["resource"]["pvc_uid"]),
        request_id=UUID(cleanup["request_id"]),
        source=cleanup["source"],
        intent_digest=cleanup["intent_digest"],
        parent_cleanup=cleanup["parent_cleanup"],
        parent_provision_generation=str(row["provision_generation"]),
        revalidate_completed=True,
    )
    assert replay.allowed and replay.completed_outcome == "deleted"
    left, right = await asyncio.gather(
        service.record(**kwargs, evidence=first["completion"]),
        service.record(**kwargs, evidence=first["completion"]),
    )
    assert left == right


@pytest.mark.asyncio
async def test_completed_child_with_other_outcome_does_not_prove_purge(db, monkeypatch):
    service, row, carrier, _ = await partial(db, monkeypatch)
    kwargs = dict(request_id=str(row["request_id"]), carrier=carrier, stage="rootdisk")
    grant = await service.authorize(**kwargs)
    cleanup = grant["cleanup"]
    assert await service.retries.cleanup.complete_cleanup_permit(
        UUID(cleanup["admission_id"]),
        outcome="retained",
        request_id=UUID(cleanup["request_id"]),
        intent_digest=cleanup["intent_digest"],
    )
    with pytest.raises(
        VMCreationRetryConflict, match="creation_disposition_incomplete"
    ):
        await service.authorize(**kwargs)
    with pytest.raises(
        VMCreationRetryConflict, match="creation_disposition_incomplete"
    ):
        await service.record(**kwargs, evidence=grant["completion"])


@pytest.mark.asyncio
async def test_owner_change_during_job_lock_wait_cannot_grant_cleanup(db, monkeypatch):
    import asyncio

    service, row, carrier, disposition = await partial(db, monkeypatch)
    async with db.acquire() as conn:
        async with conn.transaction():
            await conn.fetchrow(
                "SELECT id FROM jobs WHERE id=$1 FOR UPDATE", row["job_id"]
            )
            grant = asyncio.create_task(
                service.authorize(
                    request_id=str(row["request_id"]), carrier=carrier, stage="rootdisk"
                )
            )
            await asyncio.sleep(0.15)
            assert not grant.done()
            await conn.execute(
                "UPDATE jobs SET context=jsonb_set(context,'{inherits_parent_workspace}',to_jsonb($2::text)) WHERE id=$1",
                row["job_id"],
                "unknown",
            )
        with pytest.raises(VMCreationRetryConflict, match="workspace_owner_changed"):
            await asyncio.wait_for(grant, timeout=5)
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM vm_workspace_cleanup_admissions WHERE parent_admission_id=$1",
                UUID(disposition["admission_id"]),
            )
            == 0
        )


@pytest.mark.asyncio
async def test_untyped_progress_cannot_be_promoted_by_recording_exact_evidence(
    db, monkeypatch
):
    service, row, carrier, _ = await partial(db, monkeypatch, secret=True)
    kwargs = dict(
        request_id=str(row["request_id"]), carrier=carrier, stage="cloud_init"
    )
    grant = await service.authorize(**kwargs)
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE vm_creation_retries SET cancellation_progress=$2::jsonb WHERE request_id=$1",
            row["request_id"],
            json.dumps({"cloud_init": {"done": True}}),
        )
    with pytest.raises(
        VMCreationRetryConflict, match="creation_disposition_evidence_changed"
    ):
        await service.record(**kwargs, evidence=grant["completion"])


@pytest.mark.asyncio
async def test_parent_row_and_supplied_hash_agreement_is_not_creation_authority(
    db, monkeypatch
):
    service, row, carrier, _ = await partial(db, monkeypatch)
    grant = await service.authorize(
        request_id=str(row["request_id"]), carrier=carrier, stage="rootdisk"
    )
    cleanup = grant["cleanup"]
    digest = "sha256:" + "d" * 64
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE vm_workspace_cleanup_admissions SET intent_digest=$2 WHERE id=$1",
            UUID(cleanup["parent_cleanup"]["admission_id"]),
            digest,
        )
    permit = await service.retries.cleanup.acquire_cleanup_permit(
        owner_kind="job",
        owner_id=row["job_id"],
        pvc_uid=UUID(grant["resource"]["pvc_uid"]),
        request_id=UUID(cleanup["request_id"]),
        source=cleanup["source"],
        intent_digest=cleanup["intent_digest"],
        parent_cleanup={**cleanup["parent_cleanup"], "intent_digest": digest},
        parent_provision_generation=str(row["provision_generation"]),
    )
    assert permit.allowed is False
    assert permit.reason == "parent_cleanup_identity_changed"


@pytest.mark.asyncio
async def test_dedicated_disposition_source_cannot_acquire_without_creation_parent(db):
    from tests.test_vm_creation_retry_real_postgres import admitted_job
    from orchestrator.services.vm_creation_retry_store import VMCreationRetryStore

    job, _, _ = await admitted_job(db)
    permit = await VMCreationRetryStore(db).cleanup.acquire_cleanup_permit(
        owner_kind="job",
        owner_id=job,
        pvc_uid=uuid4(),
        request_id=uuid4(),
        source="controller_creation_rootdisk_delete",
        intent_digest="sha256:" + "0" * 64,
    )
    assert permit.allowed is False
    assert permit.reason == "parent_cleanup_identity_changed"
    async with db.acquire() as conn:
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM vm_workspace_cleanup_admissions WHERE owner_id=$1",
                job,
            )
            == 0
        )
