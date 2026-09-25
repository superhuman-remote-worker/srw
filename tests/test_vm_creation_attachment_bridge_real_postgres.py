"""Retained attachment effects through the controller and actual SQL authority."""

import json
from uuid import UUID, uuid4

import pytest

from tests.test_vm_creation_actuation import poll_until_terminal

from tests.test_vm_creation_attachment_actuation import (
    setup as _setup_fixture,
    attached as _attached_fixture,
)
from tests.test_vm_creation_attachment_authority_real_postgres import seed_instance
from tests.test_vm_creation_retry_real_postgres import (
    db as _db_fixture,
    postgres_db_fixture,  # noqa: F401
    pg_dsn,  # noqa: F401
    _schema_applied,  # noqa: F401
    admit,
)
from orchestrator.services.vm_creation_request import capture_vm_creation_request
from orchestrator.services.vm_creation_retry_store import VMCreationRetryStore
from shared.worker_queue import enqueue_worker_batch
from shared.vm_workspace_storage import storage_name

setup = _setup_fixture
attached = _attached_fixture
db = _db_fixture


async def bridge(db, attached):
    ctrl, api, fake, payload = attached
    job, generation = UUID(payload["job_id"]), UUID(payload["provision_generation"])
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO jobs(id,description,status,execution_lane,context) VALUES($1,'create','paused','stateless',$2::jsonb)",
            job,
            json.dumps(
                {"vm": {"provision_generation": str(generation), "status": "failed"}}
            ),
        )
        await conn.execute(
            "INSERT INTO srw_execution_specs(id,work_kind,work_id,document,resolved,revision,harness_adapter) VALUES($1,'Job',$2,'{}',$3::jsonb,'revision-1','srw/v1')",
            uuid4(),
            job,
            json.dumps({"spec": {"timeoutSeconds": 3600}}),
        )
        await enqueue_worker_batch(conn, job_id=job)
        await conn.execute(
            "UPDATE run_queue SET attempts_since_completion=3 WHERE unit_id=$1", job
        )
    snapshot = await capture_vm_creation_request(
        db,
        job_id=str(job),
        generation=str(generation),
        request=fake.row["request"],
        controller_configuration=fake.row["controller_configuration"],
        controller_configuration_digest=fake.row["controller_configuration_digest"],
    )
    row = await admit(
        db,
        job,
        generation,
        {
            "origin": "initial",
            "expected_status": "paused",
            "expected_pvc_uid": None,
            "request_digest": snapshot["request_digest"],
            "controller_configuration_digest": snapshot[
                "controller_configuration_digest"
            ],
        },
    )
    await seed_instance(db, row, fake.row["request"])
    store = VMCreationRetryStore(db)
    claim = (await store.claim_due(limit=1))[0]
    payload["creation_retry"].update(
        request_id=str(row["request_id"]), claim_token=str(claim["claim_token"])
    )

    async def authority(path, body, *, operation):
        method = path.rsplit("/", 1)[1].replace("-", "_")
        assert operation == "creation_retry_" + method
        return await getattr(
            store, "authorize_controller" if method == "authorize" else method
        )(**body)

    ctrl._workspace_cleanup_authority_request = authority
    return store, row


@pytest.mark.asyncio
@pytest.mark.parametrize("cancelled", [False, True])
async def test_attachment_real_authority_adopts_lost_vm_reply(db, attached, cancelled):
    ctrl, api, _, payload = attached
    store, row = await bridge(db, attached)
    api.lost.add("VirtualMachine")
    for _ in range(4):
        assert (await ctrl._do_create_serialized(payload))["status"] == "creation_pending"
        if "VirtualMachine" in api.writes:
            break
    assert api.writes == ["Lease", "Lease", "DataVolume", "Secret", "VirtualMachine"]
    if cancelled:
        async with db.acquire() as conn:
            await conn.execute(
                "UPDATE jobs SET status='cancelled' WHERE id=$1", row["job_id"]
            )
            await conn.execute(
                "UPDATE srw_workspace_instances SET status='Deleting' WHERE id=$1",
                UUID(payload["workspace_storage"]["uid"]),
            )
    result = await poll_until_terminal(ctrl._do_create_serialized, payload)
    assert result["status"] == "created"
    assert api.writes.count("VirtualMachine") == 1
    observed = await store.inspect(request_id=str(row["request_id"]))
    assert observed["state"] == ("settled" if cancelled else "succeeded")
    from vm_controller.creation_attachment import require_legacy_attachment_idle

    await require_legacy_attachment_idle(
        ctrl, api.read("Lease", storage_name(payload["workspace_storage"]))
    )
    async with db.acquire() as conn:
        instance = await conn.fetchrow(
            "SELECT * FROM srw_workspace_instances WHERE id=$1",
            UUID(payload["workspace_storage"]["uid"]),
        )
        context = json.loads(
            await conn.fetchval("SELECT context FROM jobs WHERE id=$1", row["job_id"])
        )
        assert instance["status"] == ("Deleting" if cancelled else "Attached")
        assert instance["pvc_uid"] == result["rootdisk_pvc_uid"]
        assert context["vm"]["workspace_storage"]["pvc_uid"] == instance["pvc_uid"]
        assert context["_vm_creation_pending"] == str(row["request_id"])
        queue = await conn.fetchrow(
            "SELECT state,attempts_since_completion FROM run_queue WHERE unit_id=$1",
            row["job_id"],
        )
        assert dict(queue) == {"state": "done", "attempts_since_completion": 3}


@pytest.mark.asyncio
async def test_cancel_after_lost_attachment_reply_preserves_hold_without_disk(
    db, attached
):
    ctrl, api, _, payload = attached
    store, row = await bridge(db, attached)
    original = api.create
    name = storage_name(payload["workspace_storage"])

    def create(body):
        result = original(body)
        if body["kind"] == "Lease" and body["metadata"]["name"] == name:
            raise TimeoutError("attachment accepted, reply lost")
        return result

    api.create = create
    assert (await ctrl._do_create_serialized(payload))["status"] == "creation_pending"
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET status='cancelled' WHERE id=$1", row["job_id"]
        )
    assert (await ctrl._do_create_serialized(payload))["status"] == "creation_attention"
    observed = await store.inspect(request_id=str(row["request_id"]))
    assert (
        len(observed["effects"]) == 1 and observed["effects"][0]["state"] == "observed"
    )
    assert api.writes == ["Lease", "Lease"]
    from vm_controller.creation_attachment import require_legacy_attachment_idle

    with pytest.raises(RuntimeError, match="held"):
        await require_legacy_attachment_idle(ctrl, api.read("Lease", name))
