"""Source cancellation freezes exact intent without completing the parent."""

from copy import deepcopy
from uuid import UUID, uuid4

import pytest

from orchestrator.services.vm_creation_disposition_store import (
    VMCreationDispositionStore,
)
from orchestrator.services.vm_creation_retry_store import VMCreationRetryConflict
from tests.test_vm_creation_effects_real_postgres import (
    db as _db_fixture,
    postgres_db_fixture,  # noqa: F401
    pg_dsn,  # noqa: F401
    _schema_applied,  # noqa: F401
    reserved,
)
from vm_controller import controller as settings

db = _db_fixture


async def frozen_golden(db, monkeypatch, *, cancel=True):
    monkeypatch.setattr(settings, "VM_GOLDEN_IMAGE_ENABLED", True)
    store, row, _, carrier = await reserved(db, monkeypatch)
    public = await store.inspect(request_id=str(row["request_id"]))
    image = public["request"]["vm_image"]
    name = settings._golden_name(image)
    dv_uid = str(uuid4())
    source = {
        "kind": "golden",
        "image": image,
        "namespace": settings.VM_NAMESPACE,
        "name": name,
        "dv_uid": dv_uid,
        "pvc_uid": str(uuid4()),
        "pvc_owner_dv_uid": dv_uid,
        "image_ref": image,
        "registry_source": {"registry": {"url": "docker://" + image}},
        "storage": settings.VMController._golden_dv_manifest(None, name, image)["spec"][
            "storage"
        ],
        "pvc_volume_mode": "Filesystem",
    }
    if cancel:
        await db.linearize_pinned_cancel(str(row["job_id"]), expected_status="paused")
    service = VMCreationDispositionStore(store)
    disposition = (
        (await service.freeze(request_id=public["request_id"], carrier=carrier))[
            "disposition"
        ]
        if cancel
        else None
    )
    return service, public, carrier, disposition, source


@pytest.mark.asyncio
async def test_source_intent_is_durable_before_cas_and_never_parent_completion(
    db, monkeypatch
):
    service, row, carrier, disposition, source = await frozen_golden(db, monkeypatch)
    kwargs = dict(request_id=row["request_id"], carrier=carrier, stage="source")
    grant = await service.authorize(**kwargs, source=source)
    plan = grant["plan"]
    assert grant["operation"] == "dispose_source"
    assert plan["kind"] == "source_disposition_planned"
    assert plan["source"] == source
    assert plan["target"]["kind"] == "rootdisk_never_issued"
    assert plan["tombstone"]["state"] == "disposed"
    # A crash now has made no external CAS. The exact intent remains replayable,
    # but neither a source-progress key nor the grant completes the parent.
    assert await service.authorize(**kwargs) == grant
    current = await service.retries.inspect(request_id=row["request_id"])
    assert current["cancellation_progress"] == {"source": plan}
    assert current["state"] == "cancel_requested"
    async with db.acquire() as conn:
        assert await conn.fetchval(
            "SELECT completed_at IS NULL FROM vm_workspace_cleanup_admissions WHERE id=$1",
            UUID(disposition["admission_id"]),
        )
    with pytest.raises(VMCreationRetryConflict):
        await service.record(**kwargs, evidence={"done": True})


@pytest.mark.asyncio
async def test_frozen_source_identity_cannot_be_reselected_after_lost_grant(
    db, monkeypatch
):
    service, row, carrier, _, source = await frozen_golden(db, monkeypatch)
    kwargs = dict(request_id=row["request_id"], carrier=carrier, stage="source")
    grant = await service.authorize(**kwargs, source=source)
    changed = deepcopy(source)
    changed["dv_uid"] = changed["pvc_owner_dv_uid"] = str(uuid4())
    with pytest.raises(VMCreationRetryConflict):
        await service.authorize(**kwargs, source=changed)
    assert await service.authorize(**kwargs, source=source) == grant


@pytest.mark.asyncio
async def test_unknown_source_requires_exact_semantic_resolution(db, monkeypatch):
    service, row, carrier, _, source = await frozen_golden(db, monkeypatch)
    kwargs = dict(request_id=row["request_id"], carrier=carrier, stage="source")
    for value in (
        None,
        {"kind": "registry", "image": row["request"]["vm_image"]},
        {**source, "name": "another-source"},
    ):
        with pytest.raises(VMCreationRetryConflict):
            await service.authorize(**kwargs, source=value)
    assert (await service.retries.inspect(request_id=row["request_id"]))[
        "cancellation_progress"
    ] == {}


@pytest.mark.asyncio
@pytest.mark.parametrize("lost_reply", [False, True])
async def test_crash_after_intent_replays_source_cas_even_without_pin(
    db, monkeypatch, setup, lost_reply
):
    from vm_controller.creation_disposition_sources import DispositionSources
    from vm_controller.creation_sources import pins
    from vm_controller.creation_actuation import CreationActuator

    service, row, carrier, disposition, source = await frozen_golden(db, monkeypatch)
    ctrl, api = source_runtime(setup, service, row, source, monkeypatch)
    await service.authorize(
        request_id=row["request_id"], carrier=carrier, stage="source", source=source
    )
    original = ctrl.k8s_client.replace_namespaced_custom_object
    lost = False

    def replace(**kwargs):
        nonlocal lost
        result = original(**kwargs)
        if lost_reply and not lost:
            lost = True
            raise TimeoutError("accepted source CAS reply lost")
        return result

    ctrl.k8s_client.replace_namespaced_custom_object = replace
    current = await service.retries.inspect(request_id=row["request_id"])
    disposer = DispositionSources(CreationActuator(ctrl), current, carrier, disposition)
    await disposer.run()
    await disposer.run()
    assert len(api.replacements) == 1
    assert (
        pins(api.read("DataVolume", source["name"]))[row["request_id"]]
        == current["cancellation_progress"]["source"]["tombstone"]
    )
    assert (await service.retries.inspect(request_id=row["request_id"]))[
        "state"
    ] == "cancel_requested"


def source_runtime(setup, service, row, source, monkeypatch):
    from tests.test_vm_creation_effects_real_postgres import SECRET
    from kubernetes.client.exceptions import ApiException

    ctrl, api, _, _ = setup
    monkeypatch.setattr(settings, "LIFECYCLE_HMAC_SECRET", SECRET)
    name = source["name"]
    dv = ctrl._golden_dv_manifest(name, source["image"])
    dv["metadata"].update(uid=source["dv_uid"], resourceVersion="1")
    dv["status"] = {"phase": "Succeeded"}
    api.objects["DataVolume", name] = dv
    api.objects["PersistentVolumeClaim", name] = {
        "metadata": {
            "name": name,
            "namespace": settings.VM_NAMESPACE,
            "uid": source["pvc_uid"],
            "ownerReferences": [
                {"kind": "DataVolume", "uid": source["dv_uid"], "controller": True}
            ],
        },
        "spec": {"volumeMode": "Filesystem"},
        "status": {"phase": "Bound"},
    }
    api.replacements = []

    def replace(**kwargs):
        old = api.read("DataVolume", kwargs["name"])
        body = deepcopy(kwargs["body"])
        if any(
            body["metadata"][key] != old["metadata"][key]
            for key in ("uid", "resourceVersion")
        ):
            raise ApiException(status=409)
        body["metadata"]["resourceVersion"] = str(
            int(old["metadata"]["resourceVersion"]) + 1
        )
        api.objects["DataVolume", kwargs["name"]] = body
        api.replacements.append(deepcopy(body))
        return body

    ctrl.k8s_client.replace_namespaced_custom_object = replace
    ctrl.k8s_client.list_namespaced_custom_object = lambda **_: {
        "metadata": {"resourceVersion": "1"},
        "items": [],
    }
    ctrl.core_api.list_namespaced_pod = lambda **_: {
        "metadata": {"resourceVersion": "1"},
        "items": [],
    }

    async def authority(path, body, *, operation):
        method = path.rsplit("/", 1)[-1].replace("-", "_")
        return await getattr(service.retries, method)(**body)

    ctrl._workspace_cleanup_authority_request = authority
    return ctrl, api


from tests.test_vm_creation_actuation import setup as _setup_fixture  # noqa: E402

setup = _setup_fixture


@pytest.mark.asyncio
async def test_cancel_source_cas_fences_publisher_after_its_fresh_sql_read(
    db, monkeypatch, setup
):
    import asyncio
    from kubernetes.client.exceptions import ApiException
    from vm_controller.creation_disposition_sources import DispositionSources
    from vm_controller.creation_sources import GoldenSources, pins
    from vm_controller.creation_actuation import CreationActuator

    service, row, carrier, _, source = await frozen_golden(
        db, monkeypatch, cancel=False
    )
    ctrl, api = source_runtime(setup, service, row, source, monkeypatch)
    publisher = GoldenSources(ctrl)
    checked, resume = asyncio.Event(), asyncio.Event()
    original = publisher.reader.authority

    async def paused_inspect(*args, **kwargs):
        fresh = await original(*args, **kwargs)
        assert fresh["state"] == "reconciling"
        checked.set()
        await resume.wait()
        return fresh

    publisher.reader.authority = paused_inspect
    task = asyncio.create_task(
        publisher.hold(row, source, api.read("DataVolume", source["name"]))
    )
    try:
        await asyncio.wait_for(checked.wait(), 5)
        await db.linearize_pinned_cancel(row["job_id"], expected_status="paused")
        disposition = (
            await service.freeze(request_id=row["request_id"], carrier=carrier)
        )["disposition"]
        current = await service.retries.inspect(request_id=row["request_id"])
        await DispositionSources(
            CreationActuator(ctrl), current, carrier, disposition
        ).run()
    finally:
        resume.set()
    with pytest.raises(ApiException) as exc:
        await task
    assert exc.value.status == 409
    assert len(api.replacements) == 1
    assert (
        pins(api.read("DataVolume", source["name"]))[row["request_id"]]["state"]
        == "disposed"
    )


@pytest.mark.asyncio
async def test_purged_clone_source_requires_completed_exact_child_before_intent(
    db, monkeypatch, setup
):
    from tests.test_vm_creation_effects_real_postgres import disk_observation, SECRET
    from shared.vm_creation_issuance import (
        verify_creation_carrier,
        seal_creation_carrier,
    )
    from vm_controller.creation_disposition_sources import DispositionSources
    from vm_controller.creation_actuation import CreationActuator
    from vm_controller.creation_sources import pins

    service, row, carrier, _, source = await frozen_golden(
        db, monkeypatch, cancel=False
    )
    values = verify_creation_carrier(carrier, secret=SECRET)
    values.update(version=2, rootdisk_source=source)
    carrier = seal_creation_carrier(
        values,
        namespace=carrier["metadata"]["namespace"],
        uid=carrier["metadata"]["uid"],
        resource_version="3",
        secret=SECRET,
    )
    async with db.acquire() as conn:
        token = await conn.fetchval(
            "SELECT claim_token FROM vm_creation_retries WHERE request_id=$1",
            UUID(row["request_id"]),
        )
    await service.retries.begin_effect(
        request_id=row["request_id"], claim_token=str(token), carrier=carrier
    )
    observed = disk_observation(carrier)
    observed["object"]["spec"] = {
        "source": {"pvc": {"name": source["name"], "namespace": source["namespace"]}},
        "storage": {"volumeMode": "Filesystem"},
    }
    await service.retries.observe_effect(
        request_id=row["request_id"], carrier=carrier, observation=observed
    )
    assert await db.linearize_pinned_cancel(row["job_id"], expected_status="paused")
    disposition = (await service.freeze(request_id=row["request_id"], carrier=carrier))[
        "disposition"
    ]
    kwargs = dict(request_id=row["request_id"], carrier=carrier)
    root = await service.authorize(**kwargs, stage="rootdisk")
    with pytest.raises(VMCreationRetryConflict):
        await service.authorize(**kwargs, stage="source")
    cleanup = root["cleanup"]
    assert await service.retries.cleanup.complete_cleanup_permit(
        UUID(cleanup["admission_id"]),
        request_id=UUID(cleanup["request_id"]),
        intent_digest=cleanup["intent_digest"],
        outcome="deleted",
    )
    await service.record(**kwargs, stage="rootdisk", evidence=root["completion"])
    plan = (await service.authorize(**kwargs, stage="source"))["plan"]
    assert plan["target"] == root["completion"]
    ctrl, api = source_runtime(setup, service, row, source, monkeypatch)
    current = await service.retries.inspect(request_id=row["request_id"])
    await DispositionSources(
        CreationActuator(ctrl), current, carrier, disposition
    ).run()
    assert (
        pins(api.read("DataVolume", source["name"]))[row["request_id"]]
        == plan["tombstone"]
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["source_uid", "source_pvc_uid", "unexpected_target"])
async def test_changed_source_or_unexpected_target_preserves_hold(
    db, monkeypatch, setup, fault
):
    from vm_controller.creation_disposition_sources import DispositionSources
    from vm_controller.creation_actuation import CreationActuator

    service, row, carrier, disposition, source = await frozen_golden(db, monkeypatch)
    ctrl, api = source_runtime(setup, service, row, source, monkeypatch)
    await service.authorize(
        request_id=row["request_id"], carrier=carrier, stage="source", source=source
    )
    if fault == "unexpected_target":
        api.objects["DataVolume", "agent-vm-" + row["job_id"] + "-rootdisk"] = {
            "metadata": {"uid": str(uuid4())}
        }
    else:
        kind = "DataVolume" if fault == "source_uid" else "PersistentVolumeClaim"
        api.objects[kind, source["name"]]["metadata"]["uid"] = str(uuid4())
    current = await service.retries.inspect(request_id=row["request_id"])
    with pytest.raises(ValueError):
        await DispositionSources(
            CreationActuator(ctrl), current, carrier, disposition
        ).run()
    assert api.replacements == []
    assert (await service.retries.inspect(request_id=row["request_id"]))[
        "state"
    ] == "cancel_requested"


@pytest.mark.asyncio
async def test_all_progress_keys_including_planned_source_cannot_complete_parent(
    db, monkeypatch
):
    import asyncpg

    service, row, carrier, _, source = await frozen_golden(db, monkeypatch)
    await service.authorize(
        request_id=row["request_id"], carrier=carrier, stage="source", source=source
    )
    async with service.db.acquire() as conn:
        await conn.execute(
            'UPDATE vm_creation_retries SET cancellation_progress=cancellation_progress || \'{"cloud_init":{},"rootdisk":{},"workspace_attachment":{}}\'::jsonb WHERE request_id=$1',
            UUID(row["request_id"]),
        )
        with pytest.raises(
            asyncpg.CheckViolationError, match="disposition has not completed"
        ):
            await conn.execute(
                "UPDATE vm_creation_retries SET state='settled' WHERE request_id=$1",
                UUID(row["request_id"]),
            )


@pytest.mark.asyncio
async def test_disposed_tombstones_compact_without_reopening_old_publication(
    db, monkeypatch, setup
):
    import json
    from vm_controller.creation_disposition_sources import DispositionSources
    from vm_controller.creation_actuation import CreationActuator
    from vm_controller.creation_sources import (
        GoldenSources,
        PINS,
        pins,
        RELEASED_PIN_LIMIT,
    )
    from kubernetes.client.exceptions import ApiException

    service, row, carrier, disposition, source = await frozen_golden(db, monkeypatch)
    plan = (
        await service.authorize(
            request_id=row["request_id"], carrier=carrier, stage="source", source=source
        )
    )["plan"]
    ctrl, api = source_runtime(setup, service, row, source, monkeypatch)
    old = {}
    for _ in range(80):
        request_id, job, generation, did = (str(uuid4()) for _ in range(4))
        tombstone = deepcopy(plan["tombstone"])
        tombstone.update(
            job_id=job,
            provision_generation=generation,
            disposition_id=did,
            rootdisk_name="agent-vm-" + job + "-rootdisk",
        )
        tombstone["target"]["name"] = tombstone["rootdisk_name"]
        old[request_id] = tombstone
    api.objects["DataVolume", source["name"]]["metadata"].setdefault("annotations", {})[
        PINS
    ] = json.dumps(old)
    stale = api.read("DataVolume", source["name"])
    current = await service.retries.inspect(request_id=row["request_id"])
    await DispositionSources(
        CreationActuator(ctrl), current, carrier, disposition
    ).run()
    remaining = pins(api.read("DataVolume", source["name"]))
    assert len(remaining) == RELEASED_PIN_LIMIT
    assert remaining[row["request_id"]] == plan["tombstone"]
    with pytest.raises(ApiException):
        await GoldenSources(ctrl).replace(stale)
    # Simulate eventual compaction of this request's tombstone by later requests.
    del remaining[row["request_id"]]
    api.objects["DataVolume", source["name"]]["metadata"]["annotations"][PINS] = (
        json.dumps(remaining)
    )
    with pytest.raises(ValueError, match="no longer current"):
        await GoldenSources(ctrl).hold(
            current, source, api.read("DataVolume", source["name"])
        )
    assert len(api.replacements) == 1
