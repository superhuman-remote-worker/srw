"""Replacing an inherited VM requires this Job's own exact retirement authority."""

import json
import asyncio
import hashlib
import secrets
import time
from uuid import UUID, uuid4
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest
from fastapi import FastAPI
from kubernetes.client.exceptions import ApiException

from tests.test_vm_creation_actuation import poll_until_terminal
from tests.test_vm_creation_inherited_attachment_real_postgres import (
    db as _db_fixture,
    postgres_db_fixture,  # noqa: F401
    pg_dsn,  # noqa: F401
    _schema_applied,  # noqa: F401
    setup as _setup_fixture,
    attached as _attached_fixture,
    controller_bridge,
)
from orchestrator.services.vm_creation_preflight import VMCreationPreflightStore
from orchestrator.services.vm_creation_retry_store import VMCreationRetryConflict
from orchestrator.services.vm_workspace_recovery_store import cleanup_intent_digest
from orchestrator.services.vm_provisioner import VMProvisioner
from vm_controller.creation_configuration import resolve_creation_configuration
from shared.vm_workspace_storage import storage_name


async def ordinary_lineage_owner(db, monkeypatch):
    """Use the same snapshot builder as ordinary Job creation."""
    from tests import test_vm_creation_inherited_attachment_real_postgres as lineage_fixture

    owner = uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO users(id,display_name,is_approved,is_admin) "
            "VALUES($1,'A1 ordinary fixture',true,false)", owner,
        )
        await conn.execute(
            "INSERT INTO capability_grants(scope_kind,scope_id,key,value_json) "
            "VALUES('user',$1,'vm_workspace','true'::jsonb)", owner,
        )

    async def ordinary_job(store, *, timeout=3600, lane="stateless", context=None):
        created = await store.create_job(
            description="A1 ordinary retained fixture", origin="lifecycle",
            status="created", execution_lane=lane, user_id=str(owner),
            context=context or {},
            config_override={"workspace": {"backend": "vm", "vm": {
                "image": "image-original", "cpu_cores": 2,
                "memory": "2Gi", "disk_size": "12Gi",
            }}},
            requested_workspace_backend="vm",
        )
        return UUID(str(created["id"]))

    monkeypatch.setattr(lineage_fixture, "initial_job", ordinary_job)
    return owner


def resume_route_app(db, operations):
    from orchestrator.routers.job_controls import JobControlRouteDependencies, router
    from orchestrator.security.access import require_internal_or_job_access

    app = FastAPI()
    app.include_router(router)
    app.state.job_control_dependencies_factory = lambda: JobControlRouteDependencies(
        operations=operations, store=db,
        require_internal_or_job_access=require_internal_or_job_access,
        require_job_access=AsyncMock(), require_admin=AsyncMock(),
        require_approved_user=AsyncMock(), require_sudo_request_authority=AsyncMock(),
        user_can_access_job_or_thread=AsyncMock(), mcp_scope_project_id=lambda _: None,
    )
    return app


async def owner_token(db, owner):
    token = "srw_" + secrets.token_urlsafe(32)
    await db.create_mcp_token(
        user_id=str(owner), name="a1-route-test",
        token_hash=hashlib.sha256(token.encode()).hexdigest(),
        token_prefix=token[:12], scope="user",
    )
    return token


@pytest.mark.asyncio
async def test_quota_rejected_retained_vm_effect_survives_ordinary_resume(
    db, attached, monkeypatch, tmp_path,
):
    """The public Resume preserves one rejected replacement, disk and deadline."""
    from tests.test_job_control_operations import _operations

    owner = await ordinary_lineage_owner(db, monkeypatch)

    ctrl, api, _, _ = attached
    store, first, (request, fresh, old, _, _) = await replacing(db, attached)
    _, row, payload = await admit_replacement(db, ctrl, store, request, fresh)
    assert str(row["expected_pvc_uid"]) == old["rootdisk_pvc_uid"]
    quota_active = True
    create = api.create

    def quota_create(body):
        if quota_active and body["kind"] == "VirtualMachine":
            denied = ApiException(status=403)
            denied.body = json.dumps({
                "apiVersion": "v1", "kind": "Status", "status": "Failure",
                "code": 403, "reason": "Forbidden",
            })
            raise denied
        return create(body)

    api.create = quota_create
    for _ in range(5):
        result = await ctrl._do_create_serialized(payload)
        current = await store.inspect(request_id=str(row["request_id"]))
        if current["effects"] and current["effects"][-1]["carrier_intent"]["effect_kind"] == "vm":
            break
    assert result["status"] == "creation_pending"
    observed = await store.inspect(request_id=str(row["request_id"]))
    assert observed["effects"][-1]["carrier_intent"]["effect_kind"] == "vm"
    assert observed["effects"][-1]["state"] == "rejected"
    assert observed["expected_pvc_uid"] == old["rootdisk_pvc_uid"]
    immutable = (
        "request_id", "canonical_request", "request_digest",
        "controller_configuration", "controller_configuration_digest",
        "expected_pvc_uid", "provision_generation", "admission_deadline",
    )
    async with db.acquire() as conn:
        before = dict(await conn.fetchrow(
            "SELECT " + ",".join(immutable) + " FROM vm_creation_retries "
            "WHERE request_id=$1", row["request_id"],
        ))

    monkeypatch.setenv("VM_CREATION_RETRY_ENABLED", "true")
    operations = _operations(tmp_path, store=db)
    app = resume_route_app(db, operations)
    token = await owner_token(db, owner)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://a1.test") as client:
        resumed_response = await client.post(
            f"/api/jobs/{request['job_id']}/resume",
            headers={"Authorization": f"Bearer {token}"}, json={},
        )
    assert resumed_response.status_code == 200, resumed_response.text
    resumed = resumed_response.json()
    assert resumed["vm_creation_retry_request_id"] == str(row["request_id"])
    while_held = await store.inspect(request_id=str(row["request_id"]))
    async with db.acquire() as conn:
        frozen = dict(await conn.fetchrow(
            "SELECT " + ",".join(immutable) + " FROM vm_creation_retries "
            "WHERE request_id=$1", row["request_id"],
        ))
    assert frozen == before
    assert while_held["effects"] == observed["effects"]
    assert quota_active

    quota_active = False
    created = await poll_until_terminal(ctrl._do_create_serialized, payload, limit=5)
    assert created["status"] == "created"
    after = await store.inspect(request_id=str(row["request_id"]))
    assert [
        e["state"] for e in after["effects"]
        if e["carrier_intent"]["effect_kind"] == "vm"
    ] == [
        "rejected", "observed",
    ]
    assert created["rootdisk_pvc_uid"] == old["rootdisk_pvc_uid"]


@pytest.mark.asyncio
async def test_a1_resume_http_route_requires_actual_bearer_owner(db):
    """A gate token must enter the public Resume route as its non-admin owner."""
    from orchestrator.routers.job_controls import JobControlRouteDependencies, router
    from orchestrator.security.access import require_internal_or_job_access
    from tests.test_vm_creation_preflight_real_postgres import initial_job

    job_id, owner, outsider = await initial_job(db), uuid4(), uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO users(id,display_name,is_approved,is_admin) "
            "VALUES($1,'A1 owner',true,false),($2,'A1 outsider',true,false)",
            owner, outsider,
        )
        await conn.execute("UPDATE jobs SET user_id=$2 WHERE id=$1", job_id, owner)

    async def token_for(user_id):
        token = "srw_" + secrets.token_urlsafe(32)
        await db.create_mcp_token(
            user_id=str(user_id), name="a1-route-test",
            token_hash=hashlib.sha256(token.encode()).hexdigest(),
            token_prefix=token[:12], scope="user",
        )
        return token

    resume = AsyncMock(return_value={"status": "creation_pending", "vm_creation_retry_request_id": str(uuid4())})
    app = FastAPI()
    app.include_router(router)
    app.state.job_control_dependencies_factory = lambda: JobControlRouteDependencies(
        operations=SimpleNamespace(resume_job=resume), store=db,
        require_internal_or_job_access=require_internal_or_job_access,
        require_job_access=AsyncMock(), require_admin=AsyncMock(),
        require_approved_user=AsyncMock(), require_sudo_request_authority=AsyncMock(),
        user_can_access_job_or_thread=AsyncMock(), mcp_scope_project_id=lambda _: None,
    )
    owner_token, outsider_token = await token_for(owner), await token_for(outsider)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://a1.test") as client:
        denied = await client.post(
            f"/api/jobs/{job_id}/resume", headers={"Authorization": f"Bearer {outsider_token}"}, json={},
        )
        assert denied.status_code == 403
        resume.assert_not_awaited()
        accepted = await client.post(
            f"/api/jobs/{job_id}/resume", headers={"Authorization": f"Bearer {owner_token}"}, json={},
        )
    assert accepted.status_code == 200
    assert accepted.json()["status"] == "creation_pending"
    assert str(resume.await_args.kwargs["user"]["id"]) == str(owner)
    assert resume.await_args.kwargs["req"].headers.get("x-internal-key") is None


@pytest.mark.asyncio
async def test_a1_open_cleanup_refuses_actual_owner_resume(
    db, attached, monkeypatch, tmp_path,
):
    """The predecessor cleanup hold survives the public Resume entrypoint."""
    from tests.test_job_control_operations import _operations

    owner = await ordinary_lineage_owner(db, monkeypatch)
    ctrl, _, _, _ = attached
    jobs, _, _, _, payload = await controller_bridge(db, attached)
    job_id = jobs[-1]
    assert (await poll_until_terminal(ctrl._do_create_serialized, payload, limit=5))["status"] == "created"
    async with db.acquire() as conn:
        context = json.loads(await conn.fetchval("SELECT context FROM jobs WHERE id=$1", job_id))
        context["vm"]["retirement_cleanup_pending"] = True
        context["vm"]["status"] = "deleting"
        await conn.execute(
            "UPDATE jobs SET status='paused',context=$2::jsonb WHERE id=$1",
            job_id, json.dumps(context),
        )
    monkeypatch.setenv("VM_CREATION_RETRY_ENABLED", "true")
    app = resume_route_app(db, _operations(tmp_path, store=db))
    token = await owner_token(db, owner)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://a1.test") as client:
        response = await client.post(
            f"/api/jobs/{job_id}/resume",
            headers={"Authorization": f"Bearer {token}"}, json={},
        )
    assert response.status_code == 409
    async with db.acquire() as conn:
        assert await conn.fetchval("SELECT status FROM jobs WHERE id=$1", job_id) == "paused"


@pytest.mark.asyncio
async def test_a1_admitted_cleanup_blocks_http_resume_then_settles(
    db, attached, monkeypatch, tmp_path,
):
    from tests.test_job_control_operations import _operations
    from orchestrator.services.vm_provisioning_cleanup import recycle_provisioning_vm
    from orchestrator.services.vm_workspace_recovery_store import VMWorkspaceRecoveryStore
    from orchestrator.services.vm_provisioner import VMTeardownIdentity, VMTeardownResult

    owner = await ordinary_lineage_owner(db, monkeypatch)
    ctrl, _, _, _ = attached
    jobs, _, _, _, payload = await controller_bridge(db, attached)
    job_id = jobs[-1]
    assert (await poll_until_terminal(ctrl._do_create_serialized, payload, limit=5))["status"] == "created"
    async with db.acquire() as conn:
        context = json.loads(await conn.fetchval("SELECT context FROM jobs WHERE id=$1", job_id))
        context["vm"]["status"] = "ready"
        context.pop("_vm_creation_pending", None)
        await conn.execute("UPDATE jobs SET status='paused',context=$2::jsonb WHERE id=$1", job_id, json.dumps(context))
    vm = context["vm"]
    entered, release = asyncio.Event(), asyncio.Event()

    class PausedPhysicalStop:
        async def capture_vm_teardown_identity(self, owner_id, *, entity_type):
            assert owner_id == str(job_id) and entity_type == "job"
            return VMTeardownIdentity(
                provision_generation=vm["provision_generation"],
                vm_uid=vm["vm_uid"], rootdisk_pvc_uid=vm["rootdisk_pvc_uid"],
            )

        async def release_vm_captured(self, owner_id, identity, **kwargs):
            assert owner_id == str(job_id)
            assert identity.vm_uid == vm["vm_uid"]
            assert kwargs["purge_disk"] is False
            entered.set()
            await release.wait()
            assert await db.record_managed_repository_workspace_process_zero(
                owner_id, owner_kind="job", scope="vm", provisioner="vm",
                runtime_incarnation=identity.provision_generation,
            )
            return VMTeardownResult("completed", True)

    task = asyncio.create_task(recycle_provisioning_vm(
        str(job_id), vm, db=db, provisioner=PausedPhysicalStop(),
        recovery_store=VMWorkspaceRecoveryStore(db), now=time.time(), phase_timeout=False,
    ))
    await asyncio.wait_for(entered.wait(), timeout=10)
    async with db.acquire() as conn:
        assert await conn.fetchval(
            "SELECT count(*) FROM vm_workspace_cleanup_admissions "
            "WHERE owner_id=$1 AND source='dispatcher_vm_recycle' AND completed_at IS NULL",
            job_id,
        ) == 1
    monkeypatch.setenv("VM_CREATION_RETRY_ENABLED", "true")
    token = await owner_token(db, owner)
    app = resume_route_app(db, _operations(tmp_path, store=db))
    async with db.acquire() as conn:
        before_queue = dict(await conn.fetchrow(
            "SELECT state,lease_token,leased_by FROM run_queue WHERE unit_id=$1", job_id,
        ))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://a1.test") as client:
        blocked = await client.post(f"/api/jobs/{job_id}/resume", headers={"Authorization": f"Bearer {token}"}, json={})
        assert blocked.status_code == 409
        async with db.acquire() as conn:
            after_queue = dict(await conn.fetchrow(
                "SELECT state,lease_token,leased_by FROM run_queue WHERE unit_id=$1", job_id,
            ))
        assert after_queue == before_queue
        release.set()
        assert await asyncio.wait_for(task, timeout=10) == "completed"
        allowed = await client.post(f"/api/jobs/{job_id}/resume", headers={"Authorization": f"Bearer {token}"}, json={})
    assert allowed.status_code == 200, allowed.text


@pytest.mark.asyncio
async def test_a1_ready_recycler_loses_to_prior_queued_resume(
    db, attached, monkeypatch, tmp_path,
):
    from tests.test_job_control_operations import _operations
    from orchestrator.services.vm_provisioning_cleanup import recycle_provisioning_vm
    from orchestrator.services.vm_workspace_recovery_store import VMWorkspaceRecoveryStore
    from orchestrator.services.vm_provisioner import VMTeardownIdentity

    owner = await ordinary_lineage_owner(db, monkeypatch)
    ctrl, _, _, _ = attached
    jobs, _, _, _, payload = await controller_bridge(db, attached)
    job_id = jobs[-1]
    assert (await poll_until_terminal(ctrl._do_create_serialized, payload, limit=5))["status"] == "created"
    async with db.acquire() as conn:
        context = json.loads(await conn.fetchval("SELECT context FROM jobs WHERE id=$1", job_id))
        context["vm"]["status"] = "ready"
        context.pop("_vm_creation_pending", None)
        await conn.execute("UPDATE jobs SET status='paused',context=$2::jsonb WHERE id=$1", job_id, json.dumps(context))
    vm = context["vm"]
    monkeypatch.setenv("VM_CREATION_RETRY_ENABLED", "true")
    token = await owner_token(db, owner)
    app = resume_route_app(db, _operations(tmp_path, store=db))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://a1.test") as client:
        accepted = await client.post(f"/api/jobs/{job_id}/resume", headers={"Authorization": f"Bearer {token}"}, json={})
    assert accepted.status_code == 200
    async with db.acquire() as conn:
        assert await conn.fetchval("SELECT state FROM run_queue WHERE unit_id=$1", job_id) == "queued"

    class NoPhysicalStop:
        async def capture_vm_teardown_identity(self, owner_id, *, entity_type):
            return VMTeardownIdentity(vm["provision_generation"], vm["vm_uid"], vm["rootdisk_pvc_uid"])

        async def release_vm_captured(self, *_args, **_kwargs):
            raise AssertionError("queued worker must fence predecessor retirement")

    outcome = await recycle_provisioning_vm(
        str(job_id), vm, db=db, provisioner=NoPhysicalStop(),
        recovery_store=VMWorkspaceRecoveryStore(db), now=time.time(), phase_timeout=False,
    )
    assert outcome == "authority_changed"
    async with db.acquire() as conn:
        context = json.loads(await conn.fetchval("SELECT context FROM jobs WHERE id=$1", job_id))
        assert context["vm"].get("retirement_cleanup_pending") is not True
        assert not await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM vm_workspace_cleanup_admissions "
            "WHERE owner_id=$1 AND source='dispatcher_vm_recycle')", job_id,
        )

setup, attached, db = _setup_fixture, _attached_fixture, _db_fixture


async def retire(db, attached, payload):
    ctrl, api, _, _ = attached
    job = UUID(payload["job_id"])
    async with db.acquire() as conn:
        context = json.loads(
            await conn.fetchval("SELECT context FROM jobs WHERE id=$1", job)
        )
        old = context["vm"]
        old["status"] = "deleted"
        receipt = await conn.fetchval(
            "INSERT INTO managed_repository_process_zero_receipts(owner_kind,owner_id,scope,provisioner,runtime_incarnation) VALUES('job',$1,'vm','vm',$2) RETURNING id",
            job,
            old["provision_generation"],
        )
        await conn.execute(
            "UPDATE jobs SET status='paused',context=$2::jsonb WHERE id=$1",
            job,
            json.dumps(context),
        )
        intent = {
            "owner_kind": "job",
            "owner_id": str(job),
            "provision_generation": old["provision_generation"],
            "vm_uid": old["vm_uid"],
            "pvc_uid": old["rootdisk_pvc_uid"],
            "purge_disk": False,
            "resource": "vm_workspace",
            "source": "lifecycle_vm_reap",
        }
        cleanup = uuid4()
        await conn.execute(
            "INSERT INTO vm_workspace_cleanup_admissions(id,owner_kind,owner_id,pvc_uid,source,request_id,intent_digest,completed_at,outcome) VALUES($1,'job',$2,$3,'lifecycle_vm_reap',$4,$5,clock_timestamp(),'completed')",
            cleanup,
            job,
            UUID(old["rootdisk_pvc_uid"]),
            uuid4(),
            cleanup_intent_digest(intent),
        )
    api.objects.pop(("VirtualMachine", "agent-vm-" + str(job)))
    api.objects.pop(("Secret", "agent-vm-" + str(job) + "-cloudinit"))
    fresh = VMProvisioner._fresh_provision_ctx()
    request = {key: value for key, value in payload.items() if key != "creation_retry"}
    request["provision_generation"] = fresh["provision_generation"]
    api.writes.clear()
    return request, fresh, old, receipt, cleanup


async def replacing(db, attached):
    ctrl, _, _, _ = attached
    jobs, _, store, first, payload = await controller_bridge(db, attached)
    assert (await poll_until_terminal(ctrl._do_create_serialized, payload, limit=5))["status"] == "created"
    return store, first, await retire(db, attached, payload)


async def admit_replacement(db, ctrl, store, request, fresh):
    preflight = VMCreationPreflightStore(db)
    value = await preflight.begin(
        job_id=request["job_id"], request=request, fresh_context=fresh
    )
    claim = (await preflight.claim_due(limit=1))[0]
    resolved = resolve_creation_configuration(ctrl, claim["request"])
    resolved["creation_retry_protocol"] = 1
    row = await preflight.complete_resolution(claim, resolved)
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
    return value, row, payload


@pytest.mark.asyncio
async def test_inherited_replacement_claims_same_lease_with_own_cleanup_proof(
    db, attached
):
    ctrl, api, _, _ = attached
    store, first, (request, fresh, old, receipt, cleanup) = await replacing(
        db, attached
    )
    name = storage_name(request["workspace_storage"])
    original_disk, original_lease = (
        api.read("DataVolume", name),
        api.read("Lease", name),
    )
    value, row, payload = await admit_replacement(db, ctrl, store, request, fresh)
    proof = value["predecessor_evidence"]
    assert proof["kind"] == "retained_attachment_replacement"
    assert proof["handoff"] == first["predecessor_evidence"]
    assert proof["retired_request_id"] == str(first["request_id"])
    assert proof["retirement"]["receipt_id"] == str(receipt)
    assert row["predecessor_cleanup_admission_id"] == cleanup
    assert row["admission_deadline"] == first["admission_deadline"]
    result = await poll_until_terminal(ctrl._do_create_serialized, payload, limit=5)
    assert result["status"] == "created", result
    assert result["vm_uid"] != old["vm_uid"]
    current = await store.inspect(request_id=str(row["request_id"]))
    attachment = current["effects"][0]
    assert attachment["carrier_intent"]["workspace_attachment"]["action"] == "claim"
    assert attachment["evidence"]["uid"] == original_lease["metadata"]["uid"]
    assert api.read("DataVolume", name) == original_disk
    assert "DataVolume" not in api.writes and api.writes.count("VirtualMachine") == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    [
        "receipt",
        "wrong_receipt",
        "cleanup",
        "purge",
        "vm_uid",
        "authentication",
        "binding",
        "last_vm_override",
    ],
)
async def test_inherited_replacement_requires_own_exact_retirement(
    db, attached, change
):
    _, _, _, _ = attached
    _, _, (request, fresh, old, receipt, cleanup) = await replacing(db, attached)
    async with db.acquire() as conn:
        if change == "receipt":
            await conn.execute(
                "DELETE FROM managed_repository_process_zero_receipts WHERE id=$1",
                receipt,
            )
        elif change == "wrong_receipt":
            await conn.execute(
                "UPDATE managed_repository_process_zero_receipts SET runtime_incarnation=$2 WHERE id=$1",
                receipt,
                str(uuid4()),
            )
        elif change == "cleanup":
            await conn.execute(
                "UPDATE vm_workspace_cleanup_admissions SET outcome='other' WHERE id=$1",
                cleanup,
            )
        elif change == "purge":
            await conn.execute(
                "UPDATE vm_workspace_cleanup_admissions SET intent_digest=$2 WHERE id=$1",
                cleanup,
                cleanup_intent_digest(
                    {
                        "owner_kind": "job",
                        "owner_id": request["job_id"],
                        "provision_generation": old["provision_generation"],
                        "vm_uid": old["vm_uid"],
                        "pvc_uid": old["rootdisk_pvc_uid"],
                        "purge_disk": True,
                        "resource": "vm_workspace",
                        "source": "lifecycle_vm_reap",
                    }
                ),
            )
        else:
            changed = {**old}
            if change == "authentication":
                changed["identity_authenticated"] = False
            elif change == "binding":
                changed.pop("workspace_storage")
            else:
                changed["vm_uid"] = str(uuid4())
            context = json.loads(
                await conn.fetchval(
                    "SELECT context FROM jobs WHERE id=$1", UUID(request["job_id"])
                )
            )
            context["vm"] = changed
            if change == "last_vm_override":
                context["last_vm"] = old
            await conn.execute(
                "UPDATE jobs SET context=$2::jsonb WHERE id=$1",
                UUID(request["job_id"]),
                json.dumps(context),
            )
    with pytest.raises(VMCreationRetryConflict):
        await VMCreationPreflightStore(db).begin(
            job_id=request["job_id"], request=request, fresh_context=fresh
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("lost", ["grant", "attachment", "vm", "cancelled_vm"])
async def test_replacement_lost_reply_never_duplicates_effect(db, attached, lost):
    ctrl, api, _, _ = attached
    store, _, (request, fresh, _, _, _) = await replacing(db, attached)
    _, row, payload = await admit_replacement(db, ctrl, store, request, fresh)
    name = storage_name(request["workspace_storage"])
    if lost == "grant":
        authority = ctrl._workspace_cleanup_authority_request
        dropped = False

        async def lose_grant(path, body, *, operation):
            nonlocal dropped
            result = await authority(path, body, operation=operation)
            if operation == "creation_retry_begin_effect" and not dropped:
                dropped = True
                raise TimeoutError("grant committed, reply lost")
            return result

        ctrl._workspace_cleanup_authority_request = lose_grant
    elif lost == "attachment":
        replace = api.replace

        def lose_attachment(body):
            result = replace(body)
            if body["metadata"]["name"] == name:
                raise TimeoutError("attachment committed, reply lost")
            return result

        api.replace = lose_attachment
    else:
        api.lost.add("VirtualMachine")
    assert (await ctrl._do_create_serialized(payload))["status"] == "creation_pending"
    if lost in {"vm", "cancelled_vm"}:
        for _ in range(3):
            assert (await ctrl._do_create_serialized(payload))["status"] == "creation_pending"
            if "VirtualMachine" in api.writes:
                break
        assert api.writes.count("VirtualMachine") == 1
    if lost == "cancelled_vm":
        async with db.acquire() as conn:
            await conn.execute(
                "UPDATE jobs SET status='cancelled' WHERE id=$1", row["job_id"]
            )
            await conn.execute(
                "UPDATE srw_workspace_instances SET status='Deleting' WHERE id=$1",
                UUID(request["workspace_storage"]["uid"]),
            )
    result = (
        await ctrl._do_create_serialized(payload)
        if lost == "grant"
        else await poll_until_terminal(ctrl._do_create_serialized, payload, limit=5)
    )
    observed = await store.inspect(request_id=str(row["request_id"]))
    if lost == "grant":
        # The committed grant is ambiguous. No controller may repeat it merely
        # because the old Lease still exists without the new request marker.
        assert result["status"] == "creation_attention"
        assert len(observed["effects"]) == 1
        assert observed["effects"][0]["state"] == "issued"
        assert api.writes.count("VirtualMachine") == 0
        assert (
            api.read("Lease", name)["metadata"]["resourceVersion"]
            == row["predecessor_evidence"]["attachment"]["resource_version"]
        )
    else:
        assert result["status"] == "created", result
        assert observed["state"] == (
            "settled" if lost == "cancelled_vm" else "succeeded"
        )
        assert len(observed["effects"]) == 4
        assert api.writes.count("VirtualMachine") == 1
    assert "DataVolume" not in api.writes


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change", ["receipt", "cleanup", "last_vm", "lease_uid", "lease_version", "cancel"]
)
async def test_replacement_rechecks_own_authority_at_fresh_effect(db, attached, change):
    ctrl, api, _, _ = attached
    store, _, (request, fresh, old, receipt, cleanup) = await replacing(db, attached)
    _, row, payload = await admit_replacement(db, ctrl, store, request, fresh)
    async with db.acquire() as conn:
        if change == "receipt":
            await conn.execute(
                "DELETE FROM managed_repository_process_zero_receipts WHERE id=$1",
                receipt,
            )
        elif change == "cleanup":
            await conn.execute(
                "UPDATE vm_workspace_cleanup_admissions SET outcome='other' WHERE id=$1",
                cleanup,
            )
        elif change == "last_vm":
            old["vm_uid"] = str(uuid4())
            await conn.execute(
                "UPDATE jobs SET context=jsonb_set(context,'{last_vm}',$2::jsonb) WHERE id=$1",
                row["job_id"],
                json.dumps(old),
            )
        elif change == "cancel":
            await conn.execute(
                "UPDATE jobs SET status='cancelled' WHERE id=$1", row["job_id"]
            )
        else:
            lease = api.objects[("Lease", storage_name(request["workspace_storage"]))]
            lease["metadata"]["uid" if change == "lease_uid" else "resourceVersion"] = (
                str(uuid4()) if change == "lease_uid" else "9999"
            )
    result = await ctrl._do_create_serialized(payload)
    assert result["status"] != "created", result
    observed = await store.inspect(request_id=str(row["request_id"]))
    assert observed["effects"] == []
    assert (
        "VirtualMachine" not in api.writes
        and "Secret" not in api.writes
        and "DataVolume" not in api.writes
    )


@pytest.mark.asyncio
async def test_second_replacement_preserves_flat_handoff_and_uses_latest_own_retirement(
    db, attached
):
    ctrl, _, _, _ = attached
    store, first, (request, fresh, _, _, _) = await replacing(db, attached)
    _, second, payload = await admit_replacement(db, ctrl, store, request, fresh)
    assert (await poll_until_terminal(ctrl._do_create_serialized, payload, limit=5))["status"] == "created"
    request, fresh, _, receipt, cleanup = await retire(db, attached, payload)
    proof, third, payload = await admit_replacement(db, ctrl, store, request, fresh)
    assert proof["predecessor_evidence"]["handoff"] == first["predecessor_evidence"]
    assert proof["predecessor_evidence"]["retired_request_id"] == str(
        second["request_id"]
    )
    assert proof["predecessor_evidence"]["retirement"]["receipt_id"] == str(receipt)
    assert third["predecessor_cleanup_admission_id"] == cleanup
    assert (await poll_until_terminal(ctrl._do_create_serialized, payload, limit=5))["status"] == "created"


@pytest.mark.asyncio
async def test_replacement_rechecks_own_cleanup_between_effects(db, attached):
    ctrl, api, _, _ = attached
    store, _, (request, fresh, _, _, cleanup) = await replacing(db, attached)
    _, row, payload = await admit_replacement(db, ctrl, store, request, fresh)
    authority = ctrl._workspace_cleanup_authority_request

    revoked = False

    async def revoke_after_attachment(path, body, *, operation):
        nonlocal revoked
        result = await authority(path, body, operation=operation)
        if operation == "creation_retry_observe_effect" and not revoked:
            revoked = True
            async with db.acquire() as conn:
                await conn.execute(
                    "UPDATE vm_workspace_cleanup_admissions SET outcome='other' WHERE id=$1",
                    cleanup,
                )
        return result

    ctrl._workspace_cleanup_authority_request = revoke_after_attachment
    assert (await ctrl._do_create_serialized(payload))["status"] != "created"
    assert revoked
    current = await store.inspect(request_id=str(row["request_id"]))
    assert len(current["effects"]) == 1
    assert current["effects"][0]["state"] == "observed"
    assert (
        "VirtualMachine" not in api.writes
        and "Secret" not in api.writes
        and "DataVolume" not in api.writes
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["revision", "generation", "deadline"])
async def test_replacement_keeps_prior_execution_binding_without_context_copy(
    db, attached, change
):
    _, first, (request, fresh, _, _, _) = await replacing(db, attached)
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET context=context #- '{vm,creation_preflight}' WHERE id=$1",
            UUID(request["job_id"]),
        )
        if change == "revision":
            await conn.execute(
                "UPDATE srw_execution_specs SET revision='changed' WHERE id=$1",
                first["execution_id"],
            )
        elif change == "generation":
            await conn.execute(
                "UPDATE srw_execution_specs SET generation=generation+1 WHERE id=$1",
                first["execution_id"],
            )
        else:
            await conn.execute(
                "UPDATE srw_execution_specs SET resolved=jsonb_set(resolved,'{spec,timeoutSeconds}','7200'::jsonb) WHERE id=$1",
                first["execution_id"],
            )
    with pytest.raises(VMCreationRetryConflict):
        await VMCreationPreflightStore(db).begin(
            job_id=request["job_id"], request=request, fresh_context=fresh
        )


@pytest.mark.asyncio
async def test_replacement_uses_unchanged_ledger_without_context_copy(db, attached):
    ctrl, _, _, _ = attached
    store, first, (request, fresh, _, _, _) = await replacing(db, attached)
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET context=context #- '{vm,creation_preflight}' WHERE id=$1",
            UUID(request["job_id"]),
        )
    _, row, payload = await admit_replacement(db, ctrl, store, request, fresh)
    assert row["admission_deadline"] == first["admission_deadline"]
    assert (await poll_until_terminal(ctrl._do_create_serialized, payload, limit=5))["status"] == "created"


@pytest.mark.asyncio
async def test_a1_stale_workspace_resume_cannot_shed_new_ready_retirement(
    db, attached, monkeypatch, tmp_path,
):
    """Old route classification must not erase a later admitted cleanup marker."""
    from tests.test_job_control_operations import _operations
    from orchestrator.services.vm_provisioning_cleanup import recycle_provisioning_vm
    from orchestrator.services.vm_workspace_recovery_store import VMWorkspaceRecoveryStore
    from orchestrator.services.vm_provisioner import VMTeardownIdentity, VMTeardownResult

    owner = await ordinary_lineage_owner(db, monkeypatch)
    ctrl, _, _, _ = attached
    jobs, _, _, _, payload = await controller_bridge(db, attached)
    job_id = jobs[-1]
    assert (await poll_until_terminal(ctrl._do_create_serialized, payload, limit=5))["status"] == "created"
    async with db.acquire() as conn:
        context = json.loads(await conn.fetchval("SELECT context FROM jobs WHERE id=$1", job_id))
        context["vm"]["status"] = "provisioning"
        context.pop("_vm_creation_pending", None)
        await conn.execute(
            "UPDATE jobs SET status='paused',context=$2::jsonb WHERE id=$1",
            job_id, json.dumps(context),
        )
        before_queue = dict(await conn.fetchrow(
            "SELECT state,lease_token,leased_by FROM run_queue WHERE unit_id=$1", job_id,
        ))
    old_vm = context["vm"]
    classification_reached, allow_old_classification = asyncio.Event(), asyncio.Event()
    physical_stop_reached, allow_physical_stop = asyncio.Event(), asyncio.Event()
    operations = _operations(tmp_path, store=db)

    async def pause_before_old_classification(job):
        classification_reached.set()
        await allow_old_classification.wait()
        return "ready", job, None

    operations.dependencies.prepare_job_workspace_runtime.side_effect = pause_before_old_classification
    operations.dependencies.resume_missing_workspace.side_effect = lambda job: (
        "vm" if (json.loads(job["context"]) if isinstance(job["context"], str)
                 else job["context"])["vm"]["status"] == "provisioning" else None
    )

    class PausedPhysicalStop:
        async def capture_vm_teardown_identity(self, owner_id, *, entity_type):
            assert owner_id == str(job_id) and entity_type == "job"
            return VMTeardownIdentity(
                old_vm["provision_generation"], old_vm["vm_uid"],
                old_vm["rootdisk_pvc_uid"],
            )

        async def release_vm_captured(self, owner_id, identity, **kwargs):
            assert owner_id == str(job_id)
            assert identity.vm_uid == old_vm["vm_uid"]
            assert kwargs["purge_disk"] is False
            physical_stop_reached.set()
            await allow_physical_stop.wait()
            assert await db.record_managed_repository_workspace_process_zero(
                owner_id, owner_kind="job", scope="vm", provisioner="vm",
                runtime_incarnation=identity.provision_generation,
            )
            return VMTeardownResult("completed", True)

    monkeypatch.setenv("VM_CREATION_RETRY_ENABLED", "true")
    token = await owner_token(db, owner)
    app = resume_route_app(db, operations)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://a1.test") as client:
        resume_task = asyncio.create_task(client.post(
            f"/api/jobs/{job_id}/resume",
            headers={"Authorization": f"Bearer {token}"}, json={},
        ))
        await asyncio.wait_for(classification_reached.wait(), timeout=10)
        async with db.acquire() as conn:
            current = json.loads(await conn.fetchval("SELECT context FROM jobs WHERE id=$1", job_id))
            current["vm"]["status"] = "ready"
            await conn.execute("UPDATE jobs SET context=$2::jsonb WHERE id=$1", job_id, json.dumps(current))
        cleanup_task = asyncio.create_task(recycle_provisioning_vm(
            str(job_id), current["vm"], db=db, provisioner=PausedPhysicalStop(),
            recovery_store=VMWorkspaceRecoveryStore(db), now=time.time(),
            phase_timeout=False,
        ))
        await asyncio.wait_for(physical_stop_reached.wait(), timeout=10)
        async with db.acquire() as conn:
            assert await conn.fetchval(
                "SELECT count(*) FROM vm_workspace_cleanup_admissions "
                "WHERE owner_id=$1 AND source='dispatcher_vm_recycle' AND completed_at IS NULL",
                job_id,
            ) == 1
        allow_old_classification.set()
        refused = await asyncio.wait_for(resume_task, timeout=10)
        assert refused.status_code == 409
        async with db.acquire() as conn:
            after = json.loads(await conn.fetchval("SELECT context FROM jobs WHERE id=$1", job_id))
            after_queue = dict(await conn.fetchrow(
                "SELECT state,lease_token,leased_by FROM run_queue WHERE unit_id=$1", job_id,
            ))
        assert after["vm"]["retirement_cleanup_pending"] is True
        assert "last_vm" not in after
        assert after_queue == before_queue
        allow_physical_stop.set()
        assert await asyncio.wait_for(cleanup_task, timeout=10) == "completed"
