"""Fresh attachment effects compose actual workspace-instance row authority."""

import asyncio
import json
from uuid import UUID, uuid4

import pytest

from tests.test_vm_creation_attachment_real_postgres import (
    attachment_reserved,
    attach_carrier,
    db as _db_fixture,
    postgres_db_fixture,  # noqa: F401
    pg_dsn,  # noqa: F401
    _schema_applied,  # noqa: F401
)
from orchestrator.services.vm_creation_retry_store import VMCreationRetryConflict

db = _db_fixture


async def seed_instance(db, row, request):
    binding = request["workspace_storage"]
    async with db.acquire() as conn:
        owner = await conn.fetchval(
            "INSERT INTO users(display_name,is_approved) VALUES('Attachment',TRUE) RETURNING id"
        )
        await conn.execute(
            "INSERT INTO srw_workspace_instances(id,owner_id,recipe,revision,pvc_name,pvc_uid,generation,execution_id,backend_state) VALUES($1,$2,$3::jsonb,'retained',$4,$5,$6,$7,$8::jsonb)",
            UUID(binding["uid"]),
            owner,
            json.dumps({"backend": "vm", "retention": "Retain"}),
            "srw-ws-" + UUID(binding["uid"]).hex,
            binding["pvc_uid"],
            binding["generation"],
            row["execution_id"],
            json.dumps({
                "storage": binding,
                **({"network_profile": request["network_profile"]}
                   if "network_profile" in request else {}),
            }),
        )
        await conn.execute(
            "INSERT INTO srw_execution_workspace_bindings(execution_id,instance_id) VALUES($1,$2)",
            row["execution_id"],
            UUID(binding["uid"]),
        )


async def admitted(db, monkeypatch):
    store, row, claim, carrier, request = await attachment_reserved(db, monkeypatch)
    await seed_instance(db, row, request)
    return store, row, claim, attach_carrier(carrier, request), request


@pytest.mark.asyncio
async def test_exact_instance_attachment_has_one_grant_across_replicas(db, monkeypatch):
    store, row, claim, carrier, _ = await admitted(db, monkeypatch)

    async def begin():
        return await store.begin_effect(
            request_id=str(row["request_id"]),
            claim_token=str(claim["claim_token"]),
            carrier=carrier,
        )

    results = await asyncio.gather(begin(), begin())
    assert sum(result["actuation_allowed"] for result in results) == 1
    assert any(result.get("disposition") == "observe_only" for result in results)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change", ["generation", "deleting", "owner", "pvc", "execution", "recipe"]
)
async def test_changed_instance_refuses_before_effect_grant(db, monkeypatch, change):
    store, row, claim, carrier, request = await admitted(db, monkeypatch)
    async with db.acquire() as conn:
        field, value = {
            "generation": ("generation", 2),
            "deleting": ("status", "Deleting"),
            "pvc": ("pvc_uid", str(uuid4())),
            "execution": ("execution_id", None),
            "owner": (
                "backend_state",
                json.dumps(
                    {
                        "storage": {
                            **request["workspace_storage"],
                            "owner_id": str(uuid4()),
                        }
                    }
                ),
            ),
            "recipe": ("recipe", json.dumps({"backend": "vm", "retention": "Delete"})),
        }[change]
        await conn.execute(
            f"UPDATE srw_workspace_instances SET {field}=$2 WHERE id=$1",
            UUID(request["workspace_storage"]["uid"]),
            value,
        )
    with pytest.raises(VMCreationRetryConflict, match="creation_attachment"):
        await store.begin_effect(
            request_id=str(row["request_id"]),
            claim_token=str(claim["claim_token"]),
            carrier=carrier,
        )
    async with db.acquire() as conn:
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM vm_creation_effects WHERE request_id=$1",
                row["request_id"],
            )
            == 0
        )


@pytest.mark.asyncio
async def test_instance_lock_wait_rechecks_claim_deadline(db, monkeypatch):
    store, row, claim, carrier, request = await admitted(db, monkeypatch)
    async with db.acquire() as blocker:
        async with blocker.transaction():
            await blocker.execute(
                "SELECT id FROM srw_workspace_instances WHERE id=$1 FOR UPDATE",
                UUID(request["workspace_storage"]["uid"]),
            )
            async with db.acquire() as conn:
                await conn.execute(
                    "UPDATE vm_creation_retries SET claim_expires_at=clock_timestamp()+interval '0.15 seconds' WHERE request_id=$1",
                    row["request_id"],
                )
            task = asyncio.create_task(
                store.begin_effect(
                    request_id=str(row["request_id"]),
                    claim_token=str(claim["claim_token"]),
                    carrier=carrier,
                )
            )
            await asyncio.sleep(0.3)
        with pytest.raises(VMCreationRetryConflict, match="retry_claim_changed"):
            await task


@pytest.mark.asyncio
async def test_captured_instance_pvc_completes_original_storage_binding(
    db, monkeypatch
):
    from orchestrator.services.vm_creation_attachment_store import (
        attachment_instance_on_conn,
    )

    store, row, _, _, request = await admitted(db, monkeypatch)
    pvc = str(uuid4())
    # record_created stores the captured PVC in the instance column; backend
    # storage remains the original nullable first-create binding.
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE srw_workspace_instances SET pvc_uid=$2,status='Attached' WHERE id=$1",
            UUID(request["workspace_storage"]["uid"]),
            pvc,
        )
        current = dict(
            await conn.fetchrow(
                "SELECT * FROM vm_creation_retries WHERE request_id=$1",
                row["request_id"],
            )
        )
        current["canonical_request"] = {
            **request,
            "workspace_storage": {**request["workspace_storage"], "pvc_uid": pvc},
        }
        current["controller_configuration"] = json.loads(
            current["controller_configuration"]
        )
        current["expected_pvc_uid"] = UUID(pvc)
        async with conn.transaction():
            instance = await attachment_instance_on_conn(conn, current)
        assert instance["pvc_uid"] == pvc
