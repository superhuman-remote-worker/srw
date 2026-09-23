"""Disposable gate metadata uses exact current Job/recovery and real SQL receipt."""

import json
import hashlib
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import pytest
from tests.test_vm_workspace_recovery_real_postgres import (  # noqa: F401
    app_pg as _app_pg,
    pg_dsn,
    _schema_applied,
    insert_recovery,
)
from tests.test_vm_recovery_gate_stop_control import objects

app_pg = _app_pg


def cleanup_digest(job_id, generation, dv_uid, pvc_uid):
    identity = {
        "source": "controller_rootdisk_delete",
        "owner_kind": "job",
        "owner_id": job_id,
        "dv_uid": dv_uid,
        "pvc_uid": pvc_uid,
        "provision_generation": generation,
    }
    return (
        "sha256:"
        + hashlib.sha256(
            json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
    )


async def seeded(pool, *, offset=timedelta(0)):
    doc, _ = objects()
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO users(id,display_name) VALUES($1,$2)",
            UUID(doc["user_id"]),
            f"VM recovery gate {doc['run_id']}",
        )
        await conn.execute(
            "INSERT INTO jobs(id,user_id,description,status,execution_lane,context) VALUES($1,$2,$3,'processing','stateless',$4::jsonb)",
            UUID(doc["job_id"]),
            UUID(doc["user_id"]),
            f"[vm-recovery-gate:{doc['run_id']}] retained disk fixture",
            json.dumps({"vm_workspace_recovery_acceptance_gate": doc["run_id"]}),
        )
    rid = await insert_recovery(
        pool, owner_id=UUID(doc["job_id"]), first_observed_offset=offset
    )
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM vm_workspace_recoveries WHERE id=$1", rid
        )
        doc.update(
            operation_id=str(rid),
            namespace=row["namespace"],
            generation=str(row["provision_generation"]),
            vm_uid=str(row["vm_uid"]),
            vmi_uid=str(row["prior_vmi_uid"]),
            pod_uid=str(row["prior_launcher_uid"]),
            pvc_uid=str(row["root_pvc_uid"]),
        )
        await conn.execute(
            "INSERT INTO vm_workspace_recovery_jobs(recovery_id,job_id,prior_queue_state,prior_job_status) VALUES($1,$2,'non_worker','processing')",
            rid,
            UUID(doc["job_id"]),
        )
    return doc


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", [None, "owner", "run", "operation", "identity"])
async def test_durable_control_guards_identity_and_reconstructs(app_pg, drift):
    from orchestrator.operator_cli.vm_recovery_gate_stop_control import (
        GateStopStore,
        GateStopError,
    )

    doc = await seeded(app_pg)
    store = GateStopStore(app_pg, doc["run_id"])
    if drift == "owner":
        doc["user_id"] = str(uuid4())
    elif drift == "run":
        doc["run_id"] = "foreign"
    elif drift == "operation":
        doc["operation_id"] = str(uuid4())
    elif drift == "identity":
        doc["vmi_uid"] = str(uuid4())
    if drift:
        with pytest.raises(GateStopError):
            await store.save(doc)
    else:
        await store.save(doc)
        assert await store.load() == [doc]
        changed = {**doc, "pod_uid": str(uuid4())}
        with pytest.raises(GateStopError):
            await store.save(changed)
        assert await store.load() == [doc]


@pytest.mark.asyncio
async def test_positive_release_requires_real_store_receipt_and_cancel_refuses(app_pg):
    from orchestrator.operator_cli.vm_recovery_gate_stop_control import (
        GateStopStore,
        GateStopError,
    )
    from orchestrator.services.vm_workspace_recovery_store import (
        VMWorkspaceRecoveryStore,
    )

    doc = await seeded(app_pg)
    store = GateStopStore(app_pg, doc["run_id"])
    await store.save(doc)
    with pytest.raises(GateStopError):
        async with store.authorized(doc, receipt=True):
            pass
    production = VMWorkspaceRecoveryStore(app_pg, worker_id="gate-stop-test")
    claim = await production.claim_due(UUID(doc["operation_id"]))
    assert claim
    now = datetime.now(timezone.utc).isoformat()
    evidence = dict(
        protocol_version=1,
        vm_uid=doc["vm_uid"],
        vmi_uid=doc["vmi_uid"],
        launcher_uid=doc["pod_uid"],
        root_pvc_uid=doc["pvc_uid"],
        container_id="containerd://gate-compute",
        controller_identity="controller/test",
        observed_at=now,
        declared_containers={"regular": ["compute"], "init": []},
        pod_terminal={"phase": "Failed", "restart_policy": "Never"},
        containers=[
            dict(
                name="compute",
                kind="regular",
                container_id="containerd://gate-compute",
                terminated_container_id="containerd://gate-compute",
                restart_count=0,
                state="terminated",
                last_state=None,
                finished_at=now,
                reason="Error",
            )
        ],
    )
    assert (
        await production.accept_stop_evidence(
            claim, {**evidence, "vmi_uid": str(uuid4())}
        )
        is None
    )
    with pytest.raises(GateStopError):
        async with store.authorized(doc, receipt=True):
            pass
    assert await production.accept_stop_evidence(claim, evidence)
    async with store.authorized(doc, receipt=True):
        pass
    await store.cancel(doc)
    with pytest.raises(GateStopError):
        async with store.authorized(doc, receipt=True):
            pass


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["cancel", "deadline"])
async def test_authority_reads_current_clock_and_status_after_job_lock(app_pg, change):
    import asyncio
    from orchestrator.operator_cli.vm_recovery_gate_stop_control import (
        GateStopStore,
        GateStopError,
    )

    doc = await seeded(
        app_pg,
        offset=timedelta(minutes=-15, seconds=11)
        if change == "deadline"
        else timedelta(0),
    )
    store = GateStopStore(app_pg, doc["run_id"])
    await store.save(doc)
    entered = False

    async def attempt():
        nonlocal entered
        async with store.authorized(doc):
            entered = True

    async with app_pg.acquire() as conn:
        tx = conn.transaction()
        await tx.start()
        await conn.fetchrow(
            "SELECT id FROM jobs WHERE id=$1 FOR UPDATE", UUID(doc["job_id"])
        )
        task = asyncio.create_task(attempt())
        await asyncio.sleep(0.05)
        assert not task.done()
        if change == "cancel":
            await conn.execute(
                "UPDATE jobs SET status='cancelled' WHERE id=$1", UUID(doc["job_id"])
            )
        else:
            await asyncio.sleep(1.2)
        await tx.commit()
    with pytest.raises(GateStopError):
        await task
    assert not entered


@pytest.mark.asyncio
async def test_cleanup_selection_requires_exact_run_context_not_description_prefix(
    app_pg,
):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock
    from orchestrator.operator_cli.vm_workspace_recovery_acceptance import LiveScenario

    doc = await seeded(app_pg)
    other = uuid4()
    async with app_pg.acquire() as conn:
        await conn.execute(
            "INSERT INTO jobs(id,user_id,description,status,execution_lane,context) VALUES($1,$2,$3,'processing','stateless',$4::jsonb)",
            other,
            UUID(doc["user_id"]),
            f"[vm-recovery-gate:{doc['run_id']}] retained disk fixture",
            json.dumps({"vm_workspace_recovery_acceptance_gate": "another-run"}),
        )
    scenario = object.__new__(LiveScenario)
    scenario.db = app_pg
    scenario.run_id = doc["run_id"]
    scenario._core = SimpleNamespace(api_client=object())
    scenario.provisioner = SimpleNamespace(delete_vm=AsyncMock())
    scenario._purge_fixture = AsyncMock()
    await scenario.cleanup()
    assert [call.args[0] for call in scenario._purge_fixture.await_args_list] == [
        UUID(doc["job_id"])
    ]
    async with app_pg.acquire() as conn:
        assert (
            await conn.fetchval("SELECT status FROM jobs WHERE id=$1", other)
            == "processing"
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "outcome", ["false", "exception", "remaining", "recreated", "deleted"]
)
async def test_purge_checks_real_absence_and_retains_exact_snapshot(app_pg, outcome):
    from copy import deepcopy
    from orchestrator.operator_cli.vm_recovery_gate_stop_control import (
        GateFixturePurge,
        GateStopError,
    )
    from tests.test_vm_recovery_gate_stop_control import objects

    doc = await seeded(app_pg)
    _, fixture = objects()
    vm = fixture["vm"]
    vm["metadata"].update(
        name="agent-vm-" + doc["job_id"], namespace=doc["namespace"], uid=doc["vm_uid"]
    )
    vm["metadata"]["labels"]["srw.io/owner-id"] = doc["job_id"]
    live = {"vm": vm, "vmi": None, "dv": None, "pvc": None}

    class Kube:
        async def fixture_objects(self, namespace, job):
            return deepcopy(live)

    calls = []

    async def delete(job, *, purge_disk):
        calls.append(job)
        if outcome == "false":
            return False
        if outcome == "exception":
            raise RuntimeError("private remote payload")
        if outcome == "deleted":
            live["vm"] = None
        if outcome == "recreated":
            live["vm"]["metadata"]["uid"] = str(uuid4())
        return True

    purge = GateFixturePurge(
        app_pg,
        Kube(),
        delete,
        doc["run_id"],
        doc["namespace"],
        anchor={"generation": doc["generation"], "vm": doc["vm_uid"], "pvc": None},
        timeout=0.01,
        interval=0,
    )
    if outcome == "deleted":
        await purge.run(UUID(doc["job_id"]))
    else:
        with pytest.raises(GateStopError) as caught:
            await purge.run(UUID(doc["job_id"]))
        assert "private" not in str(caught.value)
    assert calls == [doc["job_id"]]
    if outcome == "recreated":
        with pytest.raises(GateStopError):
            await purge.run(UUID(doc["job_id"]))
        assert calls == [doc["job_id"]]  # Restart must not recapture the new UID.


@pytest.mark.asyncio
@pytest.mark.parametrize("remaining", ["vmi", "dv", "pvc"])
async def test_purge_requires_every_child_absent_and_resumes_from_snapshot(
    app_pg, remaining
):
    from copy import deepcopy
    from orchestrator.operator_cli.vm_recovery_gate_stop_control import (
        GateFixturePurge,
        GateStopError,
    )
    from tests.test_vm_recovery_gate_stop_control import objects

    doc = await seeded(app_pg)
    _, base = objects()
    root = "agent-vm-" + doc["job_id"] + "-rootdisk"
    vm_name = "agent-vm-" + doc["job_id"]
    live = {
        "vm": base["vm"],
        "vmi": base["vmi"],
        "dv": deepcopy(base["vm"]),
        "pvc": deepcopy(base["vmi"]),
    }
    for kind, obj in live.items():
        obj["metadata"].update(
            name=root if kind in ("dv", "pvc") else vm_name,
            namespace=doc["namespace"],
            uid=str(uuid4()),
        )
        obj["metadata"]["labels"]["srw.io/owner-id"] = doc["job_id"]
    for kind, parent, api, parent_kind in [
        ("vmi", "vm", "kubevirt.io/v1", "VirtualMachine"),
        ("pvc", "dv", "cdi.kubevirt.io/v1beta1", "DataVolume"),
    ]:
        live[kind]["metadata"]["ownerReferences"] = [
            dict(
                controller=True,
                apiVersion=api,
                kind=parent_kind,
                name=live[parent]["metadata"]["name"],
                uid=live[parent]["metadata"]["uid"],
            )
        ]

    class Kube:
        async def fixture_objects(self, namespace, job):
            return deepcopy(live)

    calls = []
    anchor = {
        "generation": doc["generation"],
        "vm": live["vm"]["metadata"]["uid"],
        "pvc": live["pvc"]["metadata"]["uid"],
    }
    captured_dv_uid = live["dv"]["metadata"]["uid"]

    async def delete(job, *, purge_disk):
        calls.append(job)
        for kind in live:
            if kind != remaining:
                live[kind] = None
        return True

    first = GateFixturePurge(
        app_pg,
        Kube(),
        delete,
        doc["run_id"],
        doc["namespace"],
        anchor=anchor,
        timeout=0.01,
        interval=0,
    )
    with pytest.raises(GateStopError, match="still_present"):
        await first.run(UUID(doc["job_id"]))
    live[remaining] = None
    # Even when all Kubernetes objects are now absent, a restart must not
    # silently replace the durable captured VM identity with a newer one.
    with pytest.raises(GateStopError, match="purge_object_recreated"):
        await GateFixturePurge(
            app_pg,
            Kube(),
            delete,
            doc["run_id"],
            doc["namespace"],
            anchor={**anchor, "vm": str(uuid4())},
            timeout=0.01,
            interval=0,
        ).run(UUID(doc["job_id"]))
    assert len(calls) == 1
    async with app_pg.acquire() as conn:
        await conn.execute(
            "INSERT INTO vm_workspace_cleanup_admissions"
            "(id,owner_kind,owner_id,pvc_uid,source,request_id,intent_digest,"
            "completed_at,outcome) VALUES($1,'job',$2,$3,'controller_rootdisk_delete',"
            "$4,$5,clock_timestamp(),'deleted')",
            uuid4(),
            UUID(doc["job_id"]),
            UUID(anchor["pvc"]),
            uuid4(),
            cleanup_digest(
                doc["job_id"], doc["generation"], captured_dv_uid, anchor["pvc"]
            ),
        )
    await GateFixturePurge(
        app_pg,
        Kube(),
        delete,
        doc["run_id"],
        doc["namespace"],
        anchor=anchor,
        timeout=0.01,
        interval=0,
    ).run(UUID(doc["job_id"]))
    assert len(calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "receipt",
    ["missing", "pending", "wrong_pvc", "wrong_source", "stale_digest", "completed"],
)
@pytest.mark.parametrize("capture_available", [False, True])
async def test_absent_purged_rootdisk_requires_matching_completed_admission(
    app_pg, receipt, capture_available
):
    from orchestrator.operator_cli.vm_recovery_gate_stop_control import (
        GateFixturePurge,
        GateStopError,
    )

    doc = await seeded(app_pg)
    dv_uid = str(uuid4())
    snapshot = {
        "version": 1,
        "generation": doc["generation"],
        "run_id": doc["run_id"],
        "job_id": doc["job_id"],
        "user_id": doc["user_id"],
        "namespace": doc["namespace"],
        "uids": {"vm": doc["vm_uid"], "vmi": None, "dv": dv_uid, "pvc": doc["pvc_uid"]},
    }
    async with app_pg.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET context=jsonb_set(context,"
            "'{vm_workspace_recovery_gate_purge}',$2::jsonb) WHERE id=$1",
            UUID(doc["job_id"]),
            json.dumps(snapshot),
        )
    if receipt != "missing":
        async with app_pg.acquire() as conn:
            await conn.execute(
                "INSERT INTO vm_workspace_cleanup_admissions"
                "(id,owner_kind,owner_id,pvc_uid,source,request_id,intent_digest,"
                "completed_at,outcome) VALUES($1,'job',$2,$3,$4,$5,$6,"
                "CASE WHEN $7::bool THEN clock_timestamp() ELSE NULL END,$8)",
                uuid4(),
                UUID(doc["job_id"]),
                uuid4() if receipt == "wrong_pvc" else UUID(doc["pvc_uid"]),
                "controller_vm_create"
                if receipt == "wrong_source"
                else "controller_rootdisk_delete",
                uuid4(),
                "sha256:stale"
                if receipt == "stale_digest"
                else cleanup_digest(
                    doc["job_id"], doc["generation"], dv_uid, doc["pvc_uid"]
                ),
                receipt != "pending",
                None
                if receipt == "pending"
                else ("adopted" if receipt == "wrong_source" else "deleted"),
            )

    class Kube:
        async def fixture_objects(self, namespace, job):
            return dict(vm=None, vmi=None, dv=None, pvc=None)

    async def unexpected_delete(job, *, purge_disk):
        raise AssertionError("absent resources cannot authorize another delete")

    purge = GateFixturePurge(
        app_pg,
        Kube(),
        unexpected_delete,
        doc["run_id"],
        doc["namespace"],
        anchor={
            "generation": doc["generation"],
            "vm": doc["vm_uid"],
            "pvc": doc["pvc_uid"],
        }
        if capture_available
        else None,
        timeout=0.01,
        interval=0,
    )
    if receipt == "completed":
        await purge.run(UUID(doc["job_id"]))
    else:
        with pytest.raises(GateStopError, match="purge_cleanup"):
            await purge.run(UUID(doc["job_id"]))


@pytest.mark.asyncio
async def test_absent_objects_without_capture_or_prior_snapshot_do_not_prove_purge(
    app_pg,
):
    from orchestrator.operator_cli.vm_recovery_gate_stop_control import (
        GateFixturePurge,
        GateStopError,
    )

    doc = await seeded(app_pg)

    class Kube:
        async def fixture_objects(self, namespace, job):
            return dict(vm=None, vmi=None, dv=None, pvc=None)

    async def unexpected_delete(job, *, purge_disk):
        raise AssertionError("no new delete without a capture")

    with pytest.raises(GateStopError, match="purge_identity_unproven"):
        await GateFixturePurge(
            app_pg,
            Kube(),
            unexpected_delete,
            doc["run_id"],
            doc["namespace"],
            anchor=None,
        ).run(UUID(doc["job_id"]))


@pytest.mark.asyncio
@pytest.mark.parametrize("completed", [False, True])
@pytest.mark.parametrize("capture_available", [False, True])
async def test_cli_purge_reports_success_only_after_exact_cleanup_receipt(
    app_pg, monkeypatch, completed, capture_available
):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from orchestrator.operator_cli import vm_recovery_gate_stop_control as gate
    from orchestrator.operator_cli.vm_workspace_recovery_acceptance import LiveScenario
    from orchestrator.services.vm_provisioner import VMTeardownIdentity

    doc = await seeded(app_pg)
    dv_uid = str(uuid4())
    snapshot = {
        "version": 1,
        "generation": doc["generation"],
        "run_id": doc["run_id"],
        "job_id": doc["job_id"],
        "user_id": doc["user_id"],
        "namespace": doc["namespace"],
        "uids": {"vm": doc["vm_uid"], "vmi": None, "dv": dv_uid, "pvc": doc["pvc_uid"]},
    }
    async with app_pg.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET context=jsonb_set(context,"
            "'{vm_workspace_recovery_gate_purge}',$2::jsonb) WHERE id=$1",
            UUID(doc["job_id"]),
            json.dumps(snapshot),
        )
    if completed:
        async with app_pg.acquire() as conn:
            await conn.execute(
                "INSERT INTO vm_workspace_cleanup_admissions"
                "(id,owner_kind,owner_id,pvc_uid,source,request_id,intent_digest,"
                "completed_at,outcome) VALUES($1,'job',$2,$3,'controller_rootdisk_delete',"
                "$4,$5,clock_timestamp(),'deleted')",
                uuid4(),
                UUID(doc["job_id"]),
                UUID(doc["pvc_uid"]),
                uuid4(),
                cleanup_digest(
                    doc["job_id"], doc["generation"], dv_uid, doc["pvc_uid"]
                ),
            )

    class Kube:
        async def fixture_objects(self, namespace, job):
            return dict(vm=None, vmi=None, dv=None, pvc=None)

    monkeypatch.setattr(gate, "KubernetesStopObjects", lambda core: Kube())
    release = AsyncMock(side_effect=AssertionError("no new delete from absence"))
    scenario = object.__new__(LiveScenario)
    scenario.db = app_pg
    scenario.run_id = doc["run_id"]
    scenario.namespace = doc["namespace"]
    scenario._core = object()
    scenario.provisioner = SimpleNamespace(
        capture_vm_teardown_identity=AsyncMock(
            return_value=VMTeardownIdentity(
                doc["generation"], doc["vm_uid"], doc["pvc_uid"]
            )
            if capture_available
            else None,
            side_effect=None
            if capture_available
            else RuntimeError("capture unavailable"),
        ),
        release_vm_captured=release,
    )
    if completed:
        await scenario._purge_fixture(UUID(doc["job_id"]))
    else:
        with pytest.raises(gate.GateStopError, match="purge_cleanup_receipt_missing"):
            await scenario._purge_fixture(UUID(doc["job_id"]))
    release.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["vm", "pvc"])
@pytest.mark.parametrize("missing", [False, True])
async def test_first_purge_snapshot_binds_absent_captured_objects_to_stop_document(
    app_pg, field, missing
):
    from copy import deepcopy
    from unittest.mock import AsyncMock

    from orchestrator.operator_cli.vm_recovery_gate_stop_control import (
        GateFixturePurge,
        GateStopError,
        GateStopStore,
    )

    doc = await seeded(app_pg)
    await GateStopStore(app_pg, doc["run_id"]).save(doc)
    _, base = objects()
    dv = deepcopy(base["vm"])
    dv["metadata"].update(
        name="agent-vm-" + doc["job_id"] + "-rootdisk",
        namespace=doc["namespace"],
        uid=str(uuid4()),
    )
    dv["metadata"]["labels"]["srw.io/owner-id"] = doc["job_id"]
    live = {"vm": None, "vmi": None, "dv": dv, "pvc": None}
    kube = type("Kube", (), {"fixture_objects": AsyncMock(return_value=live)})()
    delete = AsyncMock(return_value=True)
    anchor = {
        "generation": doc["generation"],
        "vm": doc["vm_uid"],
        "pvc": doc["pvc_uid"],
        field: None if missing else str(uuid4()),
    }
    with pytest.raises(GateStopError, match="purge_object_recreated"):
        await GateFixturePurge(
            app_pg,
            kube,
            delete,
            doc["run_id"],
            doc["namespace"],
            anchor=anchor,
            timeout=0.01,
            interval=0,
        ).run(UUID(doc["job_id"]))
    delete.assert_not_awaited()
    async with app_pg.acquire() as conn:
        assert not await conn.fetchval(
            "SELECT context ? $2 FROM jobs WHERE id=$1",
            UUID(doc["job_id"]),
            GateFixturePurge.KEY,
        )
