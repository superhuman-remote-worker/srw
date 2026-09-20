"""Inherited attachment authority binds the original owner and immediate handoff."""

import json
from uuid import UUID, uuid4

import pytest

from tests.test_vm_creation_preflight_real_postgres import (
    db as _db_fixture,
    postgres_db_fixture,  # noqa: F401
    pg_dsn,  # noqa: F401
    _schema_applied,  # noqa: F401
    initial_job,
    candidate,
)
from tests.test_vm_creation_attachment_authority_real_postgres import seed_instance
from tests.test_vm_creation_configuration import controller
from tests.test_vm_creation_attachment_actuation import (
    setup as _setup_fixture,
    attached as _attached_fixture,
)
from shared.vm_workspace_storage import storage_name, storage_labels
from orchestrator.services.vm_creation_retry_store import VMCreationRetryStore

from orchestrator.services.vm_creation_preflight import VMCreationPreflightStore
from orchestrator.services.vm_creation_retry_store import VMCreationRetryConflict
from orchestrator.services.vm_workspace_recovery_store import cleanup_intent_digest
from vm_controller.creation_configuration import resolve_creation_configuration


setup = _setup_fixture
attached = _attached_fixture
db = _db_fixture


async def inherited(db, *, length=2):
    jobs = [await initial_job(db) for _ in range(length)]
    binding = {
        "uid": str(uuid4()),
        "generation": length,
        "pvc_uid": str(uuid4()),
        "owner_kind": "job",
        "owner_id": str(jobs[0]),
    }
    request, fresh = candidate(jobs[-1], workspace_storage=binding)
    async with db.acquire() as conn:
        execution = await conn.fetchval(
            "SELECT id FROM srw_execution_specs WHERE work_id=$1", jobs[-1]
        )
    await seed_instance(db, {"execution_id": execution}, request)
    history = []
    async with db.acquire() as conn:
        for number, job in enumerate(jobs[:-1], 1):
            generation, vm_uid = str(uuid4()), str(uuid4())
            vm = {
                "status": "deleted",
                "provision_generation": generation,
                "identity_provision_generation": generation,
                "identity_authenticated": True,
                "vm_uid": vm_uid,
                "rootdisk_pvc_uid": binding["pvc_uid"],
                "workspace_storage": {**binding, "generation": number},
            }
            await conn.execute(
                "UPDATE jobs SET status='completed',context=$2::jsonb WHERE id=$1",
                job,
                json.dumps({"vm": vm}),
            )
            previous_execution = await conn.fetchval(
                "SELECT id FROM srw_execution_specs WHERE work_id=$1", job
            )
            await conn.execute(
                "INSERT INTO srw_execution_workspace_bindings(execution_id,instance_id) VALUES($1,$2)",
                previous_execution,
                UUID(binding["uid"]),
            )
            receipt = await conn.fetchval(
                "INSERT INTO managed_repository_process_zero_receipts(owner_kind,owner_id,scope,provisioner,runtime_incarnation) VALUES('job',$1,'vm','vm',$2) RETURNING id",
                job,
                generation,
            )
            vm_intent = {
                "owner_kind": "job",
                "owner_id": str(job),
                "provision_generation": generation,
                "vm_uid": vm_uid,
                "pvc_uid": binding["pvc_uid"],
                "purge_disk": False,
                "resource": "vm_workspace",
                "source": "lifecycle_vm_reap",
            }
            detach_intent = {
                "owner_kind": "job",
                "owner_id": str(job),
                "generation": generation,
                "pvc_uid": binding["pvc_uid"],
                "resource": "retained_workspace_binding",
                "source": "retained_workspace_detach",
            }
            ids = []
            for intent, outcome in (
                (vm_intent, "completed"),
                (detach_intent, "retained_workspace_detached"),
            ):
                ids.append(
                    await conn.fetchval(
                        "INSERT INTO vm_workspace_cleanup_admissions(id,owner_kind,owner_id,pvc_uid,source,request_id,intent_digest,completed_at,outcome) VALUES($7,'job',$1,$2,$3,$4,$5,clock_timestamp(),$6) RETURNING id",
                        job,
                        UUID(binding["pvc_uid"]),
                        intent["source"],
                        uuid4(),
                        cleanup_intent_digest(intent),
                        outcome,
                        uuid4(),
                    )
                )
            history.append(
                {
                    "job": job,
                    "execution": previous_execution,
                    "vm": vm,
                    "receipt": receipt,
                    "cleanup": ids[0],
                    "detach": ids[1],
                }
            )
    return jobs, request, fresh, history


@pytest.mark.asyncio
@pytest.mark.parametrize("length", [2, 3])
async def test_inherited_preflight_freezes_exact_previous_job_without_fake_last_vm(
    db, length
):
    jobs, request, fresh, history = await inherited(db, length=length)
    store = VMCreationPreflightStore(db)
    first = await store.begin(
        job_id=str(jobs[-1]), request=request, fresh_context=fresh
    )
    proof = first["predecessor_evidence"]
    assert proof["storage_owner_id"] == str(jobs[0])
    assert proof["previous_job_id"] == str(jobs[-2])
    assert proof["current_job_id"] == str(jobs[-1])
    assert proof["receipt_id"] == str(history[-1]["receipt"])
    assert proof["detach_admission_id"] == str(history[-1]["detach"])
    assert first["predecessor_cleanup_admission_id"] == str(history[-1]["cleanup"])
    claim = (await store.claim_due(limit=1))[0]
    resolved = resolve_creation_configuration(controller(), claim["request"])
    resolved["creation_retry_protocol"] = 1
    row = await store.complete_resolution(claim, resolved)
    assert row["predecessor_evidence"] == proof
    async with db.acquire() as conn:
        context = json.loads(
            await conn.fetchval("SELECT context FROM jobs WHERE id=$1", jobs[-1])
        )
        assert "last_vm" not in context


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    [
        "receipt",
        "vm_cleanup",
        "detach",
        "detach_digest",
        "gap",
        "owner",
        "nonterminal",
        "ambiguous",
        "malformed_context",
    ],
)
async def test_inherited_handoff_missing_or_conflicting_proof_refuses(db, change):
    jobs, request, fresh, history = await inherited(db, length=3)
    previous = history[-1]
    async with db.acquire() as conn:
        if change == "receipt":
            await conn.execute(
                "DELETE FROM managed_repository_process_zero_receipts WHERE id=$1",
                previous["receipt"],
            )
        elif change in {"vm_cleanup", "detach"}:
            await conn.execute(
                "UPDATE vm_workspace_cleanup_admissions SET outcome='other' WHERE id=$1",
                previous["cleanup" if change == "vm_cleanup" else "detach"],
            )
        elif change == "detach_digest":
            await conn.execute(
                "UPDATE vm_workspace_cleanup_admissions SET intent_digest=$2 WHERE id=$1",
                previous["detach"],
                cleanup_intent_digest({"generation": str(uuid4())}),
            )
        elif change in {"gap", "owner"}:
            vm = previous["vm"]
            vm["workspace_storage"]["generation" if change == "gap" else "owner_id"] = (
                1 if change == "gap" else str(uuid4())
            )
            await conn.execute(
                "UPDATE jobs SET context=$2::jsonb WHERE id=$1",
                previous["job"],
                json.dumps({"vm": vm}),
            )
        elif change == "malformed_context":
            await conn.execute(
                "UPDATE jobs SET context='[]'::jsonb WHERE id=$1", previous["job"]
            )
        elif change == "nonterminal":
            await conn.execute(
                "UPDATE jobs SET status='paused' WHERE id=$1", previous["job"]
            )
        else:
            # Two durable historical bindings both claim the immediate generation.
            vm = {
                **history[0]["vm"],
                "workspace_storage": previous["vm"]["workspace_storage"],
            }
            await conn.execute(
                "UPDATE jobs SET context=$2::jsonb WHERE id=$1",
                jobs[0],
                json.dumps({"vm": vm}),
            )
    with pytest.raises(VMCreationRetryConflict):
        await VMCreationPreflightStore(db).begin(
            job_id=str(jobs[-1]), request=request, fresh_context=fresh
        )


async def controller_bridge(db, attached, *, length=3):
    ctrl, api, _, _ = attached
    jobs, request, fresh, history = await inherited(db, length=length)
    original_render = ctrl.render_template

    def render(request, key):
        result = original_render(request, key)
        result["metadata"]["name"] = "agent-vm-" + request["job_id"]
        result["spec"]["template"]["spec"]["volumes"][1]["cloudInitNoCloud"][
            "secretRef"
        ]["name"] = "agent-vm-" + request["job_id"] + "-cloudinit"
        return result

    ctrl.render_template = render
    preflight = VMCreationPreflightStore(db)
    await preflight.begin(job_id=str(jobs[-1]), request=request, fresh_context=fresh)
    claim = (await preflight.claim_due(limit=1))[0]
    resolved = resolve_creation_configuration(ctrl, claim["request"])
    resolved["creation_retry_protocol"] = 1
    row = await preflight.complete_resolution(claim, resolved)
    store = VMCreationRetryStore(db)
    claim = (await store.claim_due(limit=1))[0]
    payload = {
        **resolved["request"],
        "creation_retry": {
            "version": 1,
            "request_id": str(row["request_id"]),
            "claim_token": str(claim["claim_token"]),
            "request_digest": row["request_digest"],
            "controller_configuration_digest": row["controller_configuration_digest"],
        },
    }
    binding = request["workspace_storage"]
    name, namespace = (
        storage_name(binding),
        resolved["controller_configuration"]["namespace"],
    )
    api.create(
        {
            "apiVersion": "cdi.kubevirt.io/v1beta1",
            "kind": "DataVolume",
            "metadata": {
                "name": name,
                "namespace": namespace,
                "labels": {
                    **storage_labels({**binding, "generation": 1}, str(jobs[0])),
                    "srw.io/owner-kind": "job",
                    "srw.io/owner-id": str(jobs[0]),
                },
            },
            "spec": {"source": {"registry": {"url": "docker://original:image"}}},
        }
    )
    api.objects["PersistentVolumeClaim", name]["metadata"]["uid"] = binding["pvc_uid"]
    api.create(
        {
            "apiVersion": "coordination.k8s.io/v1",
            "kind": "Lease",
            "metadata": {
                "name": name,
                "namespace": namespace,
                "labels": storage_labels(
                    {**binding, "generation": length - 1}, str(jobs[-2])
                ),
                "annotations": {"srw.io/detached": "true"},
            },
            "spec": {},
        }
    )
    api.writes.clear()

    async def authority(path, body, *, operation):
        method = path.rsplit("/", 1)[1].replace("-", "_")
        assert operation == "creation_retry_" + method
        return await getattr(
            store, "authorize_controller" if method == "authorize" else method
        )(**body)

    ctrl._workspace_cleanup_authority_request = authority
    return jobs, history, store, row, payload


@pytest.mark.asyncio
@pytest.mark.parametrize("length", [2, 3])
async def test_inherited_controller_keeps_original_disk_and_advances_exact_previous_lease(
    db, attached, length
):
    ctrl, api, _, _ = attached
    jobs, history, store, row, payload = await controller_bridge(
        db, attached, length=length
    )
    name = storage_name(payload["workspace_storage"])
    original_disk = api.read("DataVolume", name)
    original_lease = api.read("Lease", name)
    result = await ctrl._do_create_serialized(payload)
    assert result["status"] == "created", result
    assert api.read("DataVolume", name) == original_disk
    assert (
        api.read("Lease", name)["metadata"]["uid"] == original_lease["metadata"]["uid"]
    )
    assert "DataVolume" not in api.writes
    assert api.read("VirtualMachine", "agent-vm-" + str(jobs[-1]))["metadata"][
        "labels"
    ]["srw.io/owner-id"] == str(jobs[-1])
    assert (await store.inspect(request_id=str(row["request_id"])))[
        "state"
    ] == "succeeded"


@pytest.mark.asyncio
@pytest.mark.parametrize("lost", ["attachment", "vm"])
async def test_inherited_lost_reply_preserves_disk_and_exact_late_adoption(
    db, attached, lost
):
    ctrl, api, _, _ = attached
    jobs, _, store, row, payload = await controller_bridge(db, attached)
    name = storage_name(payload["workspace_storage"])
    if lost == "attachment":
        original = api.replace

        def replace(body):
            value = original(body)
            if body["metadata"]["name"] == name:
                raise TimeoutError("attachment CAS committed, response lost")
            return value

        api.replace = replace
    else:
        api.lost.add("VirtualMachine")
    assert (await ctrl._do_create_serialized(payload))["status"] == "creation_pending"
    if lost == "vm":
        async with db.acquire() as conn:
            await conn.execute(
                "UPDATE jobs SET status='cancelled' WHERE id=$1", jobs[-1]
            )
            await conn.execute(
                "UPDATE srw_workspace_instances SET status='Deleting' WHERE id=$1",
                UUID(payload["workspace_storage"]["uid"]),
            )
    assert (await ctrl._do_create_serialized(payload))["status"] == "created"
    assert "DataVolume" not in api.writes
    assert api.writes.count("VirtualMachine") == 1
    result = await store.inspect(request_id=str(row["request_id"]))
    assert result["state"] == ("settled" if lost == "vm" else "succeeded")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    ["lease_job", "old_cancel", "new_cancel", "receipt", "detach", "owner_cleanup"],
)
async def test_inherited_fresh_effect_rechecks_complete_handoff(db, attached, change):
    ctrl, api, _, _ = attached
    jobs, history, store, row, payload = await controller_bridge(db, attached)
    name = storage_name(payload["workspace_storage"])
    if change == "lease_job":
        api.objects["Lease", name]["metadata"]["labels"][
            "srw.io/workspace-execution"
        ] = str(jobs[0])
    else:
        async with db.acquire() as conn:
            if change == "old_cancel":
                await conn.execute(
                    "UPDATE jobs SET status='cancelled',context=context || '{\"_stateless_cancel_cleanup_pending\":true}'::jsonb WHERE id=$1",
                    jobs[-2],
                )
            elif change == "new_cancel":
                await conn.execute(
                    "UPDATE jobs SET status='cancelled' WHERE id=$1", jobs[-1]
                )
            elif change == "receipt":
                await conn.execute(
                    "DELETE FROM managed_repository_process_zero_receipts WHERE id=$1",
                    history[-1]["receipt"],
                )
            elif change == "detach":
                await conn.execute(
                    "UPDATE vm_workspace_cleanup_admissions SET outcome='other' WHERE id=$1",
                    history[-1]["detach"],
                )
            else:
                await conn.execute(
                    "INSERT INTO vm_workspace_cleanup_admissions(id,owner_kind,owner_id,pvc_uid,source,request_id,intent_digest) VALUES($1,'job',$2,NULL,'lifecycle_vm_reap',$3,'unrelated-disk')",
                    uuid4(),
                    jobs[0],
                    uuid4(),
                )
    original = api.read("Lease", name)
    result = await ctrl._do_create_serialized(payload)
    assert result["status"] != "created"
    assert api.read("Lease", name) == original
    assert not any(
        kind in api.writes for kind in ("DataVolume", "Secret", "VirtualMachine")
    )
    assert not (await store.inspect(request_id=str(row["request_id"])))["effects"]


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["generation", "deleting"])
async def test_inherited_instance_lock_wait_refuses_catalog_rebind(
    db, attached, change
):
    import asyncio

    ctrl, api, _, _ = attached
    _, _, store, row, payload = await controller_bridge(db, attached)
    started = asyncio.Event()
    authority = ctrl._workspace_cleanup_authority_request

    async def intercepted(path, body, *, operation):
        if path.endswith("begin-effect"):
            started.set()
        return await authority(path, body, operation=operation)

    ctrl._workspace_cleanup_authority_request = intercepted
    uid = UUID(payload["workspace_storage"]["uid"])
    async with db.acquire() as blocker:
        async with blocker.transaction():
            await blocker.execute(
                "SELECT id FROM srw_workspace_instances WHERE id=$1 FOR UPDATE", uid
            )
            task = asyncio.create_task(ctrl._do_create_serialized(payload))
            await asyncio.wait_for(started.wait(), 5)
            if change == "generation":
                await blocker.execute(
                    "UPDATE srw_workspace_instances SET generation=generation+1 WHERE id=$1",
                    uid,
                )
            else:
                await blocker.execute(
                    "UPDATE srw_workspace_instances SET status='Deleting' WHERE id=$1",
                    uid,
                )
        result = await asyncio.wait_for(task, 5)
    assert result["status"] != "created"
    assert not (await store.inspect(request_id=str(row["request_id"])))["effects"]
    assert not any(
        kind in api.writes for kind in ("DataVolume", "Secret", "VirtualMachine")
    )


@pytest.mark.asyncio
async def test_inherited_replacement_cannot_borrow_previous_jobs_cleanup(db):
    jobs, request, fresh, _ = await inherited(db)
    old = {
        "status": "deleted",
        "provision_generation": str(uuid4()),
        "rootdisk_pvc_uid": request["workspace_storage"]["pvc_uid"],
        "workspace_storage": request["workspace_storage"],
    }
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET context=$2::jsonb WHERE id=$1",
            jobs[-1],
            json.dumps({"last_vm": old}),
        )
    with pytest.raises(VMCreationRetryConflict):
        await VMCreationPreflightStore(db).begin(
            job_id=str(jobs[-1]), request=request, fresh_context=fresh
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("which", ["current", "previous"])
async def test_inherited_cancellation_wins_job_row_wait_before_grant(
    db, attached, which
):
    import asyncio

    ctrl, api, _, _ = attached
    jobs, _, store, row, payload = await controller_bridge(db, attached)
    ready, proceed = asyncio.Event(), asyncio.Event()
    authority = ctrl._workspace_cleanup_authority_request

    async def intercepted(path, body, *, operation):
        if path.endswith("begin-effect"):
            ready.set()
            await proceed.wait()
        return await authority(path, body, operation=operation)

    ctrl._workspace_cleanup_authority_request = intercepted
    task = asyncio.create_task(ctrl._do_create_serialized(payload))
    await asyncio.wait_for(ready.wait(), 5)
    target = jobs[-1] if which == "current" else jobs[-2]
    async with db.acquire() as blocker:
        async with blocker.transaction():
            await blocker.execute("SELECT id FROM jobs WHERE id=$1 FOR UPDATE", target)
            proceed.set()
            async with db.acquire() as observer:
                for _ in range(100):
                    if await observer.fetchval(
                        "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE 'SELECT id FROM jobs WHERE id=ANY%')"
                    ):
                        break
                    await asyncio.sleep(0.01)
                else:
                    pytest.fail("creation did not wait for the locked Job")
            await blocker.execute(
                "UPDATE jobs SET status='cancelled',context=context || '{\"_stateless_cancel_cleanup_pending\":true}'::jsonb WHERE id=$1",
                target,
            )
    assert (await asyncio.wait_for(task, 5))["status"] != "created"
    assert not (await store.inspect(request_id=str(row["request_id"])))["effects"]
    assert not any(
        kind in api.writes for kind in ("DataVolume", "Secret", "VirtualMachine")
    )


@pytest.mark.asyncio
async def test_old_handoff_receipt_cannot_authorize_a_replaced_previous_runtime(db):
    jobs, request, fresh, history = await inherited(db)
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET context=$2::jsonb WHERE id=$1",
            jobs[-2],
            json.dumps(
                {
                    "last_vm": history[-1]["vm"],
                    "vm": {"status": "deleted", "provision_generation": str(uuid4())},
                }
            ),
        )
    with pytest.raises(VMCreationRetryConflict):
        await VMCreationPreflightStore(db).begin(
            job_id=str(jobs[-1]), request=request, fresh_context=fresh
        )


@pytest.mark.asyncio
async def test_historical_nonselected_job_cannot_race_predecessor_uniqueness(db):
    import asyncio

    jobs, request, _, history = await inherited(db, length=4)
    store = VMCreationRetryStore(db)

    async def duplicate_candidate():
        async with db.acquire() as writer:
            await writer.execute(
                "UPDATE jobs SET context=$2::jsonb WHERE id=$1",
                jobs[1],
                json.dumps({"vm": history[-1]["vm"]}),
            )

    async with db.acquire() as conn:
        async with conn.transaction():
            await store._scope(
                conn, jobs[-1], UUID(request["workspace_storage"]["pvc_uid"])
            )
            task = asyncio.create_task(duplicate_candidate())
            async with db.acquire() as observer:
                for _ in range(100):
                    if await observer.fetchval(
                        "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE 'UPDATE jobs SET context=$2%')"
                    ):
                        break
                    await asyncio.sleep(0.01)
                else:
                    pytest.fail("historical candidate mutation was not fenced")
            assert not task.done()
        await asyncio.wait_for(task, 5)


@pytest.mark.asyncio
async def test_independent_child_vm_keeps_legacy_parent_cleanup_scope(db):
    parent, child = await initial_job(db), await initial_job(db)
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET parent_job_id=$2 WHERE id=$1", child, parent
        )
        await conn.execute(
            "INSERT INTO vm_workspace_cleanup_admissions(id,owner_kind,owner_id,pvc_uid,source,request_id,intent_digest) VALUES($1,'job',$2,NULL,'lifecycle_vm_reap',$3,'different-workspace')",
            uuid4(),
            parent,
            uuid4(),
        )
    request, fresh = candidate(child)
    value = await VMCreationPreflightStore(db).begin(
        job_id=str(child), request=request, fresh_context=fresh
    )
    assert value["expected_pvc_uid"] is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change", ["authentication", "vm_uid", "rootdisk_pvc_uid", "binding"]
)
async def test_historical_last_vm_cannot_override_current_identity(db, change):
    jobs, request, fresh, history = await inherited(db)
    old = history[-1]["vm"]
    current = dict(old)
    current.pop("workspace_storage")
    if change == "authentication":
        current["identity_authenticated"] = False
    elif change != "binding":
        current[change] = str(uuid4())
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET context=$2::jsonb WHERE id=$1",
            jobs[-2],
            json.dumps({"vm": current, "last_vm": old}),
        )
    with pytest.raises(VMCreationRetryConflict):
        await VMCreationPreflightStore(db).begin(
            job_id=str(jobs[-1]), request=request, fresh_context=fresh
        )


@pytest.mark.asyncio
async def test_fresh_handoff_grant_cannot_use_contradictory_last_vm(db, attached):
    ctrl, api, _, _ = attached
    jobs, history, store, row, payload = await controller_bridge(db, attached)
    old = history[-1]["vm"]
    current = {**old, "vm_uid": str(uuid4())}
    current.pop("workspace_storage")
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET context=$2::jsonb WHERE id=$1",
            jobs[-2],
            json.dumps({"vm": current, "last_vm": old}),
        )
    assert (await ctrl._do_create_serialized(payload))["status"] != "created"
    assert not (await store.inspect(request_id=str(row["request_id"])))["effects"]
    assert not any(
        kind in api.writes for kind in ("DataVolume", "Secret", "VirtualMachine")
    )


@pytest.mark.asyncio
async def test_current_retirement_binding_requires_canonical_generation_type(db):
    from copy import deepcopy

    jobs, request, fresh, history = await inherited(db)
    old = history[-1]["vm"]
    current = deepcopy(old)
    current["workspace_storage"]["generation"] = True
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET context=$2::jsonb WHERE id=$1",
            jobs[-2],
            json.dumps({"vm": current, "last_vm": old}),
        )
    with pytest.raises(VMCreationRetryConflict):
        await VMCreationPreflightStore(db).begin(
            job_id=str(jobs[-1]), request=request, fresh_context=fresh
        )
