"""Controller fault injection connected to real PostgreSQL effect authority."""

import json
from uuid import UUID, uuid4

import pytest

from tests.test_vm_creation_actuation import (
    poll_until_terminal,
    setup as _actuation_fixture,
)
from tests.test_vm_creation_actuation import profiled_setup as _profiled_fixture
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

db = _db_fixture
setup = _actuation_fixture
profiled_setup = _profiled_fixture


@pytest.mark.asyncio
async def test_profiled_first_create_uses_stored_configuration_with_real_authority(
    db, profiled_setup
):
    ctrl, api, source, payload = profiled_setup
    job, generation = UUID(payload["job_id"]), UUID(payload["provision_generation"])
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO jobs(id,description,status,execution_lane,context) VALUES($1,'profiled create','paused','stateless',$2::jsonb)",
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
        request=source.row["request"],
        controller_configuration=source.row["controller_configuration"],
        controller_configuration_digest=source.row["controller_configuration_digest"],
    )
    retry = await admit(
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
    store = VMCreationRetryStore(db)
    claim = (await store.claim_due(limit=1))[0]
    payload["creation_retry"].update(
        request_id=str(retry["request_id"]), claim_token=str(claim["claim_token"])
    )

    async def authority(path, body, *, operation):
        method = path.rsplit("/", 1)[1].replace("-", "_")
        assert operation == "creation_retry_" + method
        return await getattr(
            store, "authorize_controller" if method == "authorize" else method
        )(**body)

    ctrl._workspace_cleanup_authority_request = authority
    result = await poll_until_terminal(ctrl._do_create_serialized, payload)

    assert result["status"] == "created", (api.writes, result)
    assert api.writes == ["Lease", "DataVolume", "Secret", "VirtualMachine"]
    inspected = await store.inspect(request_id=str(retry["request_id"]))
    assert (
        inspected["controller_configuration"] == source.row["controller_configuration"]
    )
    assert (
        inspected["controller_configuration_digest"]
        == snapshot["controller_configuration_digest"]
    )
    assert [effect["state"] for effect in inspected["effects"]] == ["observed"] * 3
    async with db.acquire() as conn:
        queue = await conn.fetchrow(
            "SELECT state,attempts_since_completion FROM run_queue WHERE unit_id=$1",
            job,
        )
        assert dict(queue) == {"state": "done", "attempts_since_completion": 3}
        context = json.loads(
            await conn.fetchval("SELECT context FROM jobs WHERE id=$1", job)
        )
        assert context["_vm_creation_pending"] == str(retry["request_id"])
        assert context["vm"]["vm_uid"] == result["vm_uid"]
        assert context["vm"]["provision_attempts"] == 1
        assert (
            await conn.fetchval(
                "SELECT state FROM vm_creation_retries WHERE request_id=$1",
                retry["request_id"],
            )
            == "succeeded"
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("cancelled", [False, True])
async def test_real_authority_lost_vm_reply_adoption_retains_attempts_and_hold(
    db, setup, cancelled
):
    ctrl, api, fake, payload = setup
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
    api.lost.add("VirtualMachine")
    assert (await ctrl._do_create_serialized(payload))["status"] == "creation_pending"
    assert api.writes == ["Lease", "DataVolume"]
    assert (await ctrl._do_create_serialized(payload))["status"] == "creation_pending"
    assert api.writes == ["Lease", "DataVolume", "Secret"]
    pending = await ctrl._do_create_serialized(payload)
    assert pending["status"] == "creation_pending"
    assert api.writes.count("VirtualMachine") == 1
    if cancelled:
        async with db.acquire() as conn:
            await conn.execute("UPDATE jobs SET status='cancelled' WHERE id=$1", job)
    result = await ctrl._do_create_serialized(payload)
    assert result["status"] == "created"
    assert api.writes.count("VirtualMachine") == 1
    async with db.acquire() as conn:
        queue = await conn.fetchrow(
            "SELECT state,attempts_since_completion FROM run_queue WHERE unit_id=$1",
            job,
        )
        assert dict(queue) == {"state": "done", "attempts_since_completion": 3}
        context = json.loads(
            await conn.fetchval("SELECT context FROM jobs WHERE id=$1", job)
        )
        assert context["_vm_creation_pending"] == str(row["request_id"])
        assert context["vm"]["vm_uid"] == result["vm_uid"]
        assert context["vm"]["provision_attempts"] == 1
        assert await conn.fetchval(
            "SELECT state FROM vm_creation_retries WHERE request_id=$1",
            row["request_id"],
        ) == ("settled" if cancelled else "succeeded")
