"""Controller fault injection connected to real PostgreSQL effect authority."""

import json
from uuid import UUID, uuid4

import pytest

from tests.test_vm_creation_prepared_actuation import (
    setup as _actuation_fixture,
    prepared as _prepared_fixture,
)
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
prepared = _prepared_fixture


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "cancelled,forged",
    [(False, None), (True, None), (False, "scope"), (False, "receipt")],
)
async def test_prepared_real_authority_lost_vm_reply_and_completed_clone(
    db, prepared, cancelled, forged
):
    ctrl, api, fake, payload, service = prepared
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
        if method == "begin_effect" and forged:
            from shared.vm_creation_issuance import (
                verify_creation_carrier,
                seal_creation_carrier,
            )
            from vm_controller.creation_actuation import CreationActuator

            secret = CreationActuator(ctrl).secret
            carrier = body["carrier"]
            values = verify_creation_carrier(carrier, secret=secret)
            source = values["rootdisk_source"]
            if forged == "scope":
                source["artifact"]["request"]["scope"]["uid"] = str(uuid4())
            else:
                source["receipt"]["buildUid"] = str(uuid4())
            body["carrier"] = seal_creation_carrier(
                values,
                namespace=carrier["metadata"]["namespace"],
                uid=carrier["metadata"]["uid"],
                resource_version=carrier["metadata"]["resourceVersion"],
                secret=secret,
            )
        return await getattr(
            store, "authorize_controller" if method == "authorize" else method
        )(**body)

    ctrl._workspace_cleanup_authority_request = authority
    initial = await ctrl._do_create_serialized(payload)
    assert initial["reason"] == "preparation_wait"
    service.store.finish(next(iter(service.store.pods)))
    api.lost.add("VirtualMachine")
    for _ in range(5):
        pending = await ctrl._do_create_serialized(payload)
        if (forged and pending.get("reason") != "preparation_wait") or (
            not forged and "VirtualMachine" in api.writes
        ):
            break
    assert pending["status"] == "creation_pending"
    if forged:
        async with db.acquire() as conn:
            assert (
                await conn.fetchval(
                    "SELECT count(*) FROM vm_creation_effects WHERE request_id=$1",
                    row["request_id"],
                )
                == 0
            )
        assert api.writes.count("VirtualMachine") == 0
        assert api.writes.count("DataVolume") == 0
        assert api.writes.count("Secret") == 0
        return
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
        assert context["vm"]["preparation"] == result["preparation"]
        assert context["vm"]["preparation_request"] == payload["preparation"]
        assert await conn.fetchval(
            "SELECT state FROM vm_creation_retries WHERE request_id=$1",
            row["request_id"],
        ) == ("settled" if cancelled else "succeeded")

    observed = await store.inspect(request_id=str(row["request_id"]))
    sources = [
        effect["carrier_intent"]["rootdisk_source"] for effect in observed["effects"]
    ]
    assert len(sources) == 3 and all(source == sources[0] for source in sources)
    assert sources[0]["kind"] == "prepared"
    from vm_controller.creation_sources import source_manager, pins
    from vm_controller.workspace_preparation import allocation_name, creation_held

    allocation = await service.store.get(allocation_name(payload["preparation"]))
    assert creation_held(allocation)
    root = "agent-vm-" + str(job) + "-rootdisk"
    api.objects["DataVolume", root]["status"] = {"phase": "Succeeded"}
    api.objects["PersistentVolumeClaim", root]["status"] = {"phase": "Bound"}
    await source_manager(ctrl, observed).release_completed(observed)
    allocation = await service.store.get(allocation.name)
    assert allocation.state["phase"] == "Allocated"
    assert not creation_held(allocation)
    assert (
        pins(api.read("DataVolume", sources[0]["name"]))[str(row["request_id"])][
            "state"
        ]
        == "released"
    )
    assert api.writes.count("DataVolume") == 1
    assert len(service.store.created_pods) == 1
