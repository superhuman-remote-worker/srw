"""Charged VM local drain -> physical End -> retained Resume/permanent End."""

import json
from dataclasses import replace
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest
import httpx
from fastapi import FastAPI

from orchestrator import main
from orchestrator.application import controls
from orchestrator.services import agent_provisioner as agent_module
from orchestrator.services import stale_agent_detector as detector
from orchestrator.services import vm_provisioner as vm_module
from orchestrator.services import thread_workspace_delivery as delivery
from agent.api import session_workspace
from agent.api.orchestrator_client import OrchestratorClient
from orchestrator.routers.agent_thread_status import router
from orchestrator.security import access
from orchestrator.services.agent_provisioner import AgentProvisioner
from orchestrator.services.session_provisioner import ensure_session_workspace
from orchestrator.services.session_router import SessionRouterService
from orchestrator.services.vm_provisioning_phases import VMProvisioningPhaseStore
from orchestrator.services.workspace_suspension import WorkspaceSuspensionService
from tests import test_vm_resource_thread_source_real_postgres as sources
from tests.test_persistent_recycler_real_postgres import StatefulPinnedK8sApi, _K8sError
from tests.test_pinned_vm_initial_binding_real_postgres import _bind_protected_agent
from tests.test_pinned_vm_workspace_delivery import (
    FINGERPRINT,
    vm_delivery as _vm_delivery,
)
from tests.test_vm_end_actuator_handoff_real_postgres import (
    _base_db,  # noqa: F401
    _base_schema,  # noqa: F401
    _schema_applied,  # noqa: F401
    db as _db,
    pg_dsn as _pg_dsn,
)
from tests.test_vm_thread_retained_disk_purge_real_postgres import (
    RetainedDisk,
    predecessor_snapshot,
)


db = _db
pg_dsn = _pg_dsn
vm_delivery = _vm_delivery


async def charged_bound_ready(db, monkeypatch):
    """Keep the existing signed creation fixture, with an actor at initial CAS.

    The ordinary helper deliberately creates an actorless source. Binding only
    after Ready cannot qualify a successful charged End: 0295 matches source
    actor/attach to Begin. This input adapter delegates every transition to the
    real store and signs physical observations for that actually captured actor.
    """

    class BoundSource:
        bound = None

        def __getattr__(self, name):
            return getattr(db, name)

        async def begin_pinned_thread_vm_provisioning(self, thread_id, **kwargs):
            self.bound = await _bind_protected_agent(db, thread_id)
            kwargs.update(
                expected_agent_id=str(self.bound["agent_id"]),
                expected_attach_token=str(self.bound["runtime_attach_token"]),
            )
            return await db.begin_pinned_thread_vm_provisioning(thread_id, **kwargs)

    source = BoundSource()
    seal = sources.seal_creation_carrier

    def actor_carrier(values, **kwargs):
        return seal(
            {
                **values,
                "thread_agent_id": str(source.bound["agent_id"]),
                "thread_attach_token": str(source.bound["runtime_attach_token"]),
            },
            **kwargs,
        )

    with monkeypatch.context() as creation:
        creation.setattr(sources, "seal_creation_carrier", actor_carrier)
        case = await sources._ready_charged_thread(source, creation)
    case["updates"]["ssh_host_key_fingerprint"] = FINGERPRINT
    assert await VMProvisioningPhaseStore(db).publish_thread_ready(
        str(case["thread_id"]),
        str(case["generation"]),
        case["registration"],
        case["vm_uid"],
        case["updates"],
    )
    current = await db.get_thread(str(case["thread_id"]))
    metadata = json.loads(current["metadata"])
    metadata["config_override"] = {
        "workspace": {"backend": "vm"},
        "officer": {"enabled": False},
    }
    await db.execute(
        "UPDATE threads SET metadata=$2::jsonb WHERE id=$1",
        case["thread_id"],
        json.dumps(metadata),
    )
    case["pvc_uid"] = metadata["vm"]["rootdisk_pvc_uid"]
    return case, current


async def end_by_handoff(db, monkeypatch, vm_delivery):
    case, current = await charged_bound_ready(db, monkeypatch)
    thread_id = str(case["thread_id"])
    # Exercise the real attestation and delivery boundary before Begin closes
    # access. Only the signed controller observation is a physical-boundary fake.
    vm_delivery.provisioner._db = db
    vm_delivery.observed.update(
        provision_generation=str(case["generation"]),
        vm_uid=case["vm_uid"],
        vmi_uid=case["vmi_uid"],
        active_pod_uid=case["launcher_uid"],
        rootdisk_pvc_uid=case["pvc_uid"],
        pod_ip=case["updates"]["pod_ip"],
    )
    payload = await delivery.agent_get_thread_workspace_locked(
        thread_id,
        presented_agent_id=str(current["agent_id"]),
        presented_runtime_generation=str(current["runtime_generation"]),
        presented_attach_token=str(current["runtime_attach_token"]),
        dependencies=replace(vm_delivery.dependencies, store=db),
    )
    workspace = await session_workspace.poll_workspace_ready(
        NS(get_thread_workspace=AsyncMock(return_value=payload)),
        thread_id,
        timeout=1,
        require_vm=True,
        session_runtime_generation=None,
    )
    assert len({case["vm_uid"], case["vmi_uid"], case["launcher_uid"]}) == 3
    assert workspace["workspace_runtime_incarnation"] == case["launcher_uid"]
    process = str(uuid4())
    await db.execute(
        "UPDATE agents SET metadata=jsonb_build_object('dispatch_process_generation',$2::text) WHERE id=$1",
        current["agent_id"],
        process,
    )
    retirement = await db.begin_pinned_thread_retirement(thread_id, permanent=False)
    assert retirement["state"] == "pending", retirement
    case["retirement"] = retirement
    assert await db.authorize_pinned_thread_retirement(
        thread_id,
        token=retirement["token"],
        generation=retirement["generation"],
        settle_status="ended",
    )
    pod = retirement["context"]["agent_pod"]
    assert (
        len({case["vm_uid"], case["vmi_uid"], case["launcher_uid"], pod["pod_uid"]})
        == 4
    )
    request = dict(
        agent_id=str(current["agent_id"]),
        pod_uid=pod["pod_uid"],
        process_generation=process,
        runtime_generation=retirement["generation"],
        runtime_attach_token=str(current["runtime_attach_token"]),
        retirement_token=retirement["token"],
        disposition="ended",
        permanent=False,
        workspace_generation=workspace["workspace_generation"],
        workspace_runtime_incarnation=workspace["workspace_runtime_incarnation"],
    )
    assert await db.list_retryable_pinned_retirements() == []
    api = FastAPI()
    api.include_router(router)
    api.state.agent_thread_status_dependencies_factory = lambda: NS(
        db=db, require_internal=access.require_internal
    )
    monkeypatch.setattr(access, "_INTERNAL_KEY", "delivery-handoff-test-key")
    client = OrchestratorClient(
        orchestrator_url="http://test",
        pod_ip="192.0.2.1",
        pod_port=8001,
        hostname="test",
        config_name="creator",
        pid=123,
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=api),
        base_url="http://test",
        headers={"X-Internal-Key": "delivery-handoff-test-key"},
    ) as http:
        client._client = http
        accepted = await client.request_thread_retirement_actuator(
            thread_id,
            pinned_agent_id=request["agent_id"],
            pod_uid=pod["pod_uid"],
            process_generation=process,
            session_runtime_generation=request["runtime_generation"],
            session_runtime_attach_token=request["runtime_attach_token"],
            session_runtime_retirement_token=retirement["token"],
            retirement_disposition="ended",
            retirement_permanent=False,
            workspace_generation=workspace["workspace_generation"],
            workspace_runtime_incarnation=workspace["workspace_runtime_incarnation"],
        )
    assert accepted is not None
    assert accepted["status"] == "actuator_requested"
    assert (
        accepted["actuator_request"]["workspace_runtime_incarnation"]
        == case["launcher_uid"]
    )
    native_ack = db.acknowledge_pinned_thread_local_quiescence
    zero_receipts = []

    async def observe_zero(*args, **kwargs):
        receipt = await native_ack(*args, **kwargs)
        assert receipt is not None
        zero_receipts.append(receipt)
        return receipt

    monkeypatch.setattr(db, "acknowledge_pinned_thread_local_quiescence", observe_zero)
    candidate = (await db.list_retryable_pinned_retirements())[0]
    assert (
        candidate["nominated_before_grace"] and candidate["agent_status"] != "offline"
    )
    events, k8s = [], StatefulPinnedK8sApi()
    key = (pod["namespace"], pod["pod_name"])
    k8s.install_old_pod(namespace=key[0], name=key[1], uid=pod["pod_uid"], labels={})
    k8s.pods[key].spec = NS(
        containers=[NS(name="agent")],
        init_containers=[],
        ephemeral_containers=[],
        volumes=[],
    )
    delete = k8s.delete_namespaced_pod

    def stop_pod(*args, **kwargs):
        events.append("pod-stop")
        result = delete(*args, **kwargs)
        k8s.mark_terminal(*key)
        k8s.pods[key].status.container_statuses[0].name = "agent"
        return result

    k8s.delete_namespaced_pod = stop_pod
    agent = AgentProvisioner()
    agent._k8s_available, agent._core_api = True, k8s
    physical = RetainedDisk(db, case)
    release = physical.release_vm_captured

    async def stop_vm(*args, **kwargs):
        assert key not in k8s.pods
        pending = await db.get_thread(thread_id)
        assert pending["runtime_retirement_actuator_request"] is not None
        assert pending["runtime_retirement_local_quiescence"] is None
        assert pending["runtime_retirement_stage_receipt"] is None
        assert pending["runtime_retirement_external_cleanup"] is None
        assert not await db.resume_thread(thread_id)
        events.append("vm-stop")
        return await release(*args, **kwargs)

    physical.release_vm_captured = stop_vm
    core, networking = MagicMock(), MagicMock()
    core.read_namespaced_service.side_effect = _K8sError(404)
    networking.read_namespaced_ingress.side_effect = _K8sError(404)
    monkeypatch.setattr(agent_module, "agent_provisioner", agent)
    monkeypatch.setattr(vm_module, "vm_provisioner", physical)
    monkeypatch.setattr(main.app.state.resources, "postgres_db", db)
    monkeypatch.setattr(
        main.app.state.resources,
        "session_router",
        SessionRouterService(
            namespace=key[0],
            ingress_host="test.invalid",
            core_api=core,
            networking_api=networking,
        ),
    )
    dependencies = controls.stale_agent_detector_dependencies(main.app.state.resources)
    assert await detector.retry_pending_pinned_retirement(
        candidate, dependencies=dependencies
    )
    assert events == ["pod-stop", "vm-stop"]
    assert len(zero_receipts) == 1
    assert zero_receipts[0]["workspace_generation"] == str(case["generation"])
    assert zero_receipts[0]["workspace_runtime_incarnation"] == case["vm_uid"]
    assert zero_receipts[0]["quiescence_protocol"] == "workspace_actuator_zero_v1"
    monkeypatch.setattr(db, "acknowledge_pinned_thread_local_quiescence", native_ack)
    physical.release_vm_captured = release
    assert physical.stopped and not physical.purged
    charge = await db.fetchrow(
        "SELECT * FROM vm_resource_reservations WHERE id=$1",
        UUID(case["admitted"]["reservation_id"]),
    )
    assert charge["state"] == "released"
    assert (
        json.loads(charge["release_evidence"])["kind"] == "exact_cleanup_compute_absent"
    )
    ended = await db.get_thread(thread_id)
    assert (
        ended["status"] == "ended"
        and ended["runtime_retirement_actuator_request"] is None
    )
    assert json.loads(ended["metadata"])["vm"]["rootdisk_pvc_uid"] == case["pvc_uid"]
    archived = await db.fetchval(
        "SELECT actuator_request FROM thread_runtime_retirement_outcomes WHERE thread_id=$1",
        case["thread_id"],
    )
    assert json.loads(archived) == accepted["actuator_request"]
    assert await db.list_retryable_pinned_retirements() == []
    return case, physical, request, candidate, dependencies


async def resume_source(db, monkeypatch, case):
    thread_id = str(case["thread_id"])
    assert await db.resume_thread(thread_id)
    current = await _bind_protected_agent(db, thread_id)
    configuration = json.loads(
        await db.fetchval(
            "SELECT controller_configuration FROM vm_creation_retries WHERE request_id=$1",
            case["request_id"],
        )
    )
    policy = await db.fetchval(
        "SELECT document FROM vm_resource_admission_policy WHERE cluster_id=$1",
        configuration["resource_admission"]["cluster_id"],
    )
    monkeypatch.setenv("VM_MODE", "same-cluster")
    monkeypatch.setenv("VM_CREATION_RETRY_ENABLED", "true")
    monkeypatch.setenv("VM_RESOURCE_ADMISSION_CONFIG", policy)
    monkeypatch.setenv("VM_NETWORK_PROFILE_ENABLED", "false")

    async def resolve(_client, request, *, secret):
        return {"request": request, "controller_configuration": configuration}

    monkeypatch.setattr(
        "orchestrator.services.vm_creation_transport.resolve_vm_creation_configuration",
        resolve,
    )
    provisioner = vm_module.VMProvisioner()
    provisioner._db, provisioner._controller_url = db, "http://controller.test"
    provisioner._http_client, provisioner._lifecycle_hmac_secret = (
        object(),
        b"combined-session-test",
    )
    suspension = WorkspaceSuspensionService()
    suspension.connect(db, NS(is_available=True), None, vm_provisioner=provisioner)
    await ensure_session_workspace(
        thread_id,
        db=db,
        provisioner=None,
        suspension=suspension,
        expected_runtime_generation=str(current["runtime_generation"]),
    )
    source = await db.fetchrow(
        "SELECT * FROM vm_creation_retries WHERE thread_id=$1 AND request_id<>$2",
        case["thread_id"],
        case["request_id"],
    )
    assert source and source["thread_retained_resume_id"] is not None
    assert source["thread_runtime_generation"] == current["runtime_generation"]
    assert source["thread_agent_id"] == current["agent_id"]
    assert source["thread_attach_token"] == current["runtime_attach_token"]
    assert source["expected_pvc_uid"] == UUID(case["pvc_uid"])
    return current


@pytest.mark.asyncio
@pytest.mark.parametrize("followup", ["resume", "permanent"])
async def test_charged_handoff_composes_with_retained_followup(
    db, monkeypatch, followup, vm_delivery
):
    case, physical, request, candidate, dependencies = await end_by_handoff(
        db, monkeypatch, vm_delivery
    )
    thread_id = str(case["thread_id"])
    history = await predecessor_snapshot(db, case)
    if followup == "resume":
        successor = await resume_source(db, monkeypatch, case)
        assert str(successor["runtime_generation"]) != request["runtime_generation"]
        assert str(successor["agent_id"]) != request["agent_id"]
        before = dict(await db.get_thread(thread_id))
        effects = list(physical.effects)
        assert (
            await db.request_pinned_thread_retirement_actuator(thread_id, **request)
        )["status"] == "settled_or_superseded"
        assert not await detector.retry_pending_pinned_retirement(
            candidate, dependencies=dependencies
        )
        assert dict(await db.get_thread(thread_id)) == before
        assert physical.effects == effects and not physical.purged
    else:
        current = await db.get_thread(thread_id)
        result = await controls.thread_retirement_operations(
            main.app.state.resources
        ).end_thread_flow(
            thread_id,
            current,
            permanent=True,
            force=True,
        )
        assert result == {"status": "deleted"}
        assert await db.get_thread(thread_id) is None
        assert physical.purged and len(physical.effects) == 2
        audit = json.loads(
            await db.fetchval(
                "SELECT deletion_receipt FROM vm_thread_creation_owners WHERE thread_id=$1",
                case["thread_id"],
            )
        )
        assert audit["kind"] == "retained_vm_disk_purge"
        assert (
            audit["compute_cleanup_admission_id"] == physical.effects[0]["admission_id"]
        )
        assert audit["disk_cleanup_admission_id"] == physical.effects[1]["admission_id"]
        assert (
            await db.request_pinned_thread_retirement_actuator(thread_id, **request)
        )["status"] == "settled_or_superseded"
    after = await predecessor_snapshot(db, case)
    for table, rows in history.items():
        assert all(row in after[table] for row in rows), table


@pytest.mark.asyncio
async def test_populated_0297_retained_source_survives_0298(
    pg_dsn, monkeypatch, tmp_path
):
    from pathlib import Path
    from urllib.parse import urlsplit, urlunsplit

    import asyncpg
    from orchestrator.database.postgres import PostgresDB
    from orchestrator.database.migrate import run_migrations
    from orchestrator.services.vm_creation_retry_store import VMCreationRetryStore
    from tests.test_vm_session_retained_resume_real_postgres import retained_resume

    root = Path(__file__).resolve().parents[1]
    database = "followups_upgrade_" + uuid4().hex
    admin = await asyncpg.connect(pg_dsn)
    store = pool = None
    await admin.execute(f'CREATE DATABASE "{database}"')
    dsn = urlunsplit(urlsplit(pg_dsn)._replace(path=f"/{database}"))
    try:
        migrations = root / "src/orchestrator/database/migrations/app"
        stage = tmp_path / "migrations"
        stage.mkdir()
        for path in migrations.glob("*.sql"):
            if path.name.split("_", 1)[0] <= "0297":
                (stage / path.name).write_bytes(path.read_bytes())
        pool = await asyncpg.create_pool(dsn, min_size=1, max_size=3)
        await run_migrations(pool, stage)
        checksums = await pool.fetch(
            "SELECT filename,checksum FROM schema_migrations ORDER BY filename"
        )
        store = PostgresDB(connection_string=dsn, min_connections=1, max_connections=8)
        await store.connect()
        case, _, current, suspension = await retained_resume(
            store,
            monkeypatch,
            ready=True,
            marker=False,
            bind=True,
        )
        assert await store.fetchval(
            "SELECT to_regprocedure('public.valid_vm_thread_nonquota_cleanup_source("
            "public.vm_resource_thread_cleanup_authorities,public.vm_creation_retries)') IS NULL"
        )
        assert (
            await store.fetchval(
                "SELECT controller_configuration->>'version' FROM vm_creation_retries "
                "WHERE thread_id=$1",
                case["thread_id"],
            )
            == "3"
        )
        from orchestrator.services import vm_thread_retained_resume as retained

        native_operation = retained.operation_on_conn

        class QuotaConnection:
            def __init__(self, conn):
                self.conn = conn

            def __getattr__(self, name):
                return getattr(self.conn, name)

            async def fetchval(self, query, *args):
                assert "valid_vm_thread_nonquota_cleanup_source" not in query, (
                    "Quota Resume must not require the newer non-quota validator"
                )
                return await self.conn.fetchval(query, *args)

        async def quota_operation(conn, operation_id, thread_id):
            value = await native_operation(
                QuotaConnection(conn), operation_id, thread_id
            )
            assert value is None or value["nonquota"] is False
            return value

        monkeypatch.setattr(retained, "operation_on_conn", quota_operation)
        await ensure_session_workspace(
            str(case["thread_id"]),
            db=store,
            provisioner=None,
            suspension=suspension,
            expected_runtime_generation=str(current["runtime_generation"]),
        )
        source = await store.fetchrow(
            "SELECT * FROM vm_creation_retries WHERE thread_retained_resume_id IS NOT NULL"
        )
        assert source and source["expected_pvc_uid"] == UUID(case["pvc_uid"])
        before = dict(await store.get_thread(str(case["thread_id"])))
        tables = (
            "vm_thread_retained_resumes",
            "vm_creation_retries",
            "vm_resource_reservations",
            "vm_resource_thread_cleanup_authorities",
            "vm_resource_thread_cleanup_stops",
            "vm_workspace_cleanup_admissions",
            "thread_runtime_retirement_outcomes",
        )

        async def history():
            return {
                table: await store.fetch(
                    f"SELECT (to_jsonb(r)-'actuator_request')::text AS row FROM {table} r ORDER BY row"
                )
                for table in tables
            }

        prior = await history()
        assert len(prior["vm_thread_retained_resumes"]) == 1
        migration = migrations / "0298_pinned_vm_retirement_actuator_request.sql"
        (stage / migration.name).write_bytes(migration.read_bytes())
        await run_migrations(pool, stage)
        await run_migrations(pool, stage)
        assert (
            await pool.fetch(
                "SELECT filename,checksum FROM schema_migrations WHERE filename=ANY($1::text[]) ORDER BY filename",
                [row["filename"] for row in checksums],
            )
            == checksums
        )
        assert await pool.fetchval(
            "SELECT success FROM schema_migrations WHERE filename=$1", migration.name
        )
        after = dict(await store.get_thread(str(case["thread_id"])))
        assert after.pop("runtime_retirement_actuator_request") is None
        assert after == before and await history() == prior
        assert await store.fetchval(
            "SELECT bool_and(actuator_request IS NULL) FROM thread_runtime_retirement_outcomes"
        )
        inspected = await VMCreationRetryStore(store).inspect(
            request_id=str(source["request_id"])
        )
        assert inspected["request_id"] == str(source["request_id"])
    finally:
        if store is not None:
            await store.close()
        if pool is not None:
            await pool.close()
        await admin.execute(f'DROP DATABASE "{database}"')
        await admin.close()
