"""Initial VM requests must capture the protected session actor they will use."""

import asyncio
import json
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest
from fastapi import HTTPException

from orchestrator.services.session_attach_binding import (
    bind_registered_persistent_agent,
)

from orchestrator.services.thread_admission import provision_thread_workspace
from orchestrator.services.thread_workspace_delivery import (
    ThreadWorkspaceDeliveryDependencies,
    agent_get_thread_workspace_locked,
)
from orchestrator.services.workspace_tier_policy import thread_workspace_backend
from orchestrator.services.vm_creation_retry_store import (
    VMCreationRetryConflict,
    VMCreationRetryStore,
)
from orchestrator.services.vm_provisioner import VMProvisioner
from shared.vm_launcher_profile import predict_launcher
from tests.test_vm_resource_configuration import whole_launcher_configuration
from tests.test_vm_resource_thread_source_real_postgres import (
    _base_db,  # noqa: F401
    _schema_applied,  # noqa: F401
    _thread,
    db as _db,
    pg_dsn,  # noqa: F401
    thread_schema,  # noqa: F401
)
from tests.test_vm_resource_whole_store_real_postgres import environment

db = _db


def test_bound_workspace_poll_composes_the_live_vm_provisioner(monkeypatch):
    from orchestrator.application.preparation import (
        thread_workspace_delivery_dependencies,
    )
    from orchestrator.services import vm_provisioner

    live = object()
    monkeypatch.setattr(vm_provisioner, "vm_provisioner", live)
    dependencies = thread_workspace_delivery_dependencies(
        SimpleNamespace(
            postgres_db=object(), main_cloud_router=object(), gitea_client=object()
        )
    )
    assert dependencies.vm_provisioner is live


async def _initial_vm(db, monkeypatch, *, native=False):
    policy, inventory, _, _ = await environment(db)
    override = {
        "workspace": {
            "backend": "vm",
            "vm": {
                "image": "registry.example/session@sha256:" + "a" * 64,
                "cpu_cores": 8,
                "memory": "16Gi",
            },
        }
    }
    _, thread_id = await _thread(
        db,
        lane="pinned",
        status="created",
        metadata={"config_override": override},
    )
    if native:
        owner = await db.fetchval("SELECT user_id FROM threads WHERE id=$1", thread_id)
        await db.execute(
            "UPDATE users SET is_admin=true,is_approved=true WHERE id=$1", owner
        )
        db.manifest_runtime_image = "test.invalid/srw:installed"
        thread_id = UUID(
            await db.create_thread(
                user_id=str(owner),
                initial_metadata={"config_override": override},
            )
        )
    configuration = whole_launcher_configuration()
    configuration.update(namespace="workers", storage_class="local")
    resource = configuration["resource_admission"]
    resource["cluster_id"] = inventory.cluster_id
    resource["policy_digest"] = inventory.policy_digest
    resource["template_profile"].update(
        storage_class="local",
        guest_vcpus=8,
        guest_memory_bytes=16 * 1024**3,
    )
    resource["launcher_prediction"]["vector"] = predict_launcher(
        policy.launcher_profile,
        guest_vcpus=8,
        guest_memory_bytes=16 * 1024**3,
    ).to_six_dict()
    resource["host_mapping"]["vector"] = policy.cost.cost(8, "16Gi").to_six_dict()
    monkeypatch.setenv("VM_MODE", "same-cluster")
    monkeypatch.setenv("VM_CREATION_RETRY_ENABLED", "true")
    monkeypatch.setenv(
        "VM_RESOURCE_ADMISSION_CONFIG", json.dumps(policy.policy_document)
    )
    monkeypatch.setenv("VM_NETWORK_PROFILE_ENABLED", "false")

    async def resolve(_client, request, *, secret):
        assert secret == b"initial-binding-test"
        return {"request": request, "controller_configuration": configuration}

    monkeypatch.setattr(
        "orchestrator.services.vm_creation_transport.resolve_vm_creation_configuration",
        resolve,
    )
    provisioner = VMProvisioner()
    provisioner._db = db
    provisioner._controller_url = "http://controller.test"
    provisioner._http_client = object()
    provisioner._lifecycle_hmac_secret = b"initial-binding-test"
    return (
        thread_id,
        policy,
        override,
        SimpleNamespace(store=db, vm_provisioner=provisioner),
    )


async def _bind_protected_agent(db, thread_id):
    """Keep all protected binding DB transitions real; model only Pod evidence."""
    thread = await db.get_thread(str(thread_id))
    agent_id, pod_uid, protection_id, attach_token, effect_token = (
        uuid4() for _ in range(5)
    )
    pod_name = "srw-agent-j-" + str(agent_id)[:8]
    await db.execute(
        "INSERT INTO agents (id,config_name,hostname,pod_ip,pod_uid,status,"
        "agent_mode,last_heartbeat) VALUES "
        "($1,'worker_base',$2,'127.0.0.1',$3,'ready','dual',now())",
        agent_id,
        pod_name,
        str(pod_uid),
    )
    planned = await db.plan_pinned_warm_binding_protection(
        str(thread_id),
        expected_runtime_generation=str(thread["runtime_generation"]),
        runtime_attach_token=str(attach_token),
        agent_id=str(agent_id),
        protection_id=str(protection_id),
        source="attach",
        provisioner="agent",
        namespace="agents-a",
        pod_name=pod_name,
        pod_uid=str(pod_uid),
        discovered_resource_version="10",
    )
    assert planned and planned["owned"]
    assert await db.claim_pinned_warm_binding_effect(
        str(protection_id),
        effect_token=str(effect_token),
    )
    assert await db.publish_pinned_warm_binding_protection(
        str(protection_id),
        effect_token=str(effect_token),
        expected_pod_uid=str(pod_uid),
        protection_resource_version="11",
        evidence_protocol="exact_live_finalizer_v1",
    )
    assert await db.bind_pinned_warm_agent(str(protection_id))
    return await db.get_thread(str(thread_id))


async def _bind_cold_agent(db, thread_id):
    thread = await db.get_thread(str(thread_id))
    generation = str(thread["runtime_generation"])
    attempt, agent_id, pod_uid = (uuid4() for _ in range(3))
    pod_name = "persistent-thread-" + str(thread_id)[:12]
    assert await db.reserve_pinned_agent_pod_provision_intent(
        str(thread_id),
        expected_runtime_generation=generation,
        attempt_id=str(attempt),
        pod_name=pod_name,
        provisioner="persistent",
        namespace="agents-a",
    )
    assert await db.publish_pinned_agent_pod_provision_intent(
        str(thread_id),
        expected_runtime_generation=generation,
        attempt_id=str(attempt),
        pod_name=pod_name,
        pod_uid=str(pod_uid),
        namespace="agents-a",
    )
    await db.execute(
        "INSERT INTO agents (id,config_name,hostname,pod_ip,pod_uid,status,"
        "agent_mode,last_heartbeat) VALUES "
        "($1,'session_base',$2,'127.0.0.1',$3,'ready','persistent',now())",
        agent_id,
        pod_name,
        str(pod_uid),
    )
    assert await bind_registered_persistent_agent(
        str(thread_id),
        str(agent_id),
        None,
        generation,
        dependencies=SimpleNamespace(store=db),
    )
    return await db.get_thread(str(thread_id))


@pytest.mark.asyncio
async def test_initial_vm_admission_does_not_capture_an_unbound_actor(db, monkeypatch):
    thread_id, _policy, override, dependencies = await _initial_vm(db, monkeypatch)
    tasks = []
    create_task = asyncio.create_task

    def capture(coroutine, **kwargs):
        task = create_task(coroutine, **kwargs)
        tasks.append(task)
        return task

    monkeypatch.setattr(asyncio, "create_task", capture)
    await provision_thread_workspace(
        SimpleNamespace(
            config_override=override,
            lite_session=False,
            vm_session=True,
            config_name="session_base",
        ),
        str(thread_id),
        dependencies=dependencies,
    )
    if tasks:
        await asyncio.gather(*tasks)
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_creation_retries WHERE thread_id=$1",
            thread_id,
        )
        == 0
    ), "initial VM source must wait for the protected agent/attach identity"


@pytest.mark.asyncio
async def test_prebinding_request_is_stale_after_real_protected_warm_bind(
    db, monkeypatch
):
    """Reproduction control: the source fence is correct and must stay strict."""
    thread_id, policy, override, dependencies = await _initial_vm(db, monkeypatch)
    before = await db.get_thread(str(thread_id))
    assert await dependencies.vm_provisioner.create_thread_vm(
        str(thread_id),
        vm_image=override["workspace"]["vm"]["image"],
        expected_runtime_generation=str(before["runtime_generation"]),
    )
    source = await db.fetchrow(
        "SELECT * FROM vm_creation_retries WHERE thread_id=$1",
        thread_id,
    )
    assert source is not None and source["thread_agent_id"] is None
    assert (await policy.admit(request_id=str(source["request_id"])))[
        "action"
    ] == "admitted"
    current = await _bind_protected_agent(db, thread_id)
    assert current["runtime_generation"] == source["thread_runtime_generation"]
    assert (
        current["agent_id"] is not None and current["runtime_attach_token"] is not None
    )
    retry = VMCreationRetryStore(db)
    async with db.acquire() as conn, conn.transaction():
        with pytest.raises(VMCreationRetryConflict, match="thread_changed"):
            await retry._thread_scope(conn, source)
    assert (
        await db.fetchval(
            "SELECT state FROM vm_resource_reservations WHERE request_id=$1",
            source["request_id"],
        )
        == "reserved"
    )
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_creation_effects WHERE request_id=$1",
            source["request_id"],
        )
        == 0
    )


def _delivery(db, provisioner):
    return ThreadWorkspaceDeliveryDependencies(
        store=db,
        vm_provisioner=provisioner,
        cloud_router=SimpleNamespace(active=SimpleNamespace(is_initialized=False)),
        gitea_client=SimpleNamespace(),
        container_provisioner=None,
        GrantDenied=RuntimeError,
        LiteWorkspaceConfigError=ValueError,
        backend_from_override=lambda co: co["workspace"]["backend"],
        build_agent_cloud_mount=AsyncMock(return_value=None),
        build_agent_cloud_sync=lambda *a, **kw: None,
        build_protected_cloud_mount=lambda *a, **kw: None,
        cloud_workspace_driver=lambda: "sync",
        grant_violations_detail=lambda values: values,
        inject_lite_workspace_config=lambda co, **kw: co,
        inject_thread_dispatch_credentials=AsyncMock(side_effect=lambda co, **kw: co),
        protected_cloud_delivery_state=AsyncMock(return_value=("ready", None)),
        protected_workspace_wait_payload=lambda **kw: kw,
        require_pinned_status_identity=lambda: True,
        resolve_session_config=AsyncMock(return_value=None),
        resolve_thread_datasources=AsyncMock(return_value=[]),
        resolve_thread_repositories=AsyncMock(return_value=[]),
        revalidate_thread_project_ids=AsyncMock(return_value=[]),
        ro_mount_matches_protected_selection=lambda *a, **kw: True,
        schedule_stateless_workspace_ensure=lambda *a, **kw: None,
        thread_accepts_runtime=lambda row: row is not None
        and row["status"] != "ended"
        and row["runtime_retirement_token"] is None,
        thread_project_ids=AsyncMock(return_value=[]),
        thread_workspace_backend=thread_workspace_backend,
        virtual_workspace_rclone_spec=lambda: None,
        vm_workspaces_on_pod_network=lambda: True,
    )


async def _poll(db, provisioner, thread):
    async with db.thread_datasource_lock(str(thread["id"])):
        return await agent_get_thread_workspace_locked(
            str(thread["id"]),
            presented_agent_id=str(thread["agent_id"]),
            presented_runtime_generation=str(thread["runtime_generation"]),
            presented_attach_token=str(thread["runtime_attach_token"]),
            dependencies=_delivery(db, provisioner),
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("binding", ["warm", "cold"])
async def test_first_bound_workspace_poll_installs_current_frozen_vm_source(
    db, monkeypatch, binding
):
    thread_id, policy, override, dependencies = await _initial_vm(
        db, monkeypatch, native=True
    )
    current = await (_bind_protected_agent if binding == "warm" else _bind_cold_agent)(
        db, thread_id
    )
    # The mutable compatibility projection must not replace admitted image or
    # sizing. The production execution snapshot remains unchanged.
    await db.execute(
        "UPDATE threads SET metadata=jsonb_set(metadata,'{config_override,workspace,vm}',"
        '\'{"image":"changed.invalid/latest","cpu_cores":99,"memory":"99Gi"}\') WHERE id=$1',
        thread_id,
    )
    payload = await _poll(db, dependencies.vm_provisioner, current)
    assert payload["vm_status"] == "provisioning"
    assert payload["status"] == "creating"
    assert payload["session_runtime_generation"] == str(current["runtime_generation"])
    assert not any(
        payload.get(key)
        for key in (
            "vm_ssh_host",
            "ssh_key_path",
            "cloud_mount",
            "resolved_config",
            "datasources",
        )
    )
    source = await db.fetchrow(
        "SELECT * FROM vm_creation_retries WHERE thread_id=$1", thread_id
    )
    assert source["thread_runtime_generation"] == current["runtime_generation"]
    assert source["thread_agent_id"] == current["agent_id"]
    assert source["thread_attach_token"] == current["runtime_attach_token"]
    request = json.loads(source["canonical_request"])
    assert request["vm_image"] == override["workspace"]["vm"]["image"]
    assert request["cpu_cores"] == 8 and request["memory"] == "16Gi"
    assert (await policy.admit(request_id=str(source["request_id"])))[
        "action"
    ] == "admitted"
    retry = VMCreationRetryStore(db)
    claim = (await retry.claim_due(limit=1))[0]
    authorized = await retry.authorize_controller(
        request_id=str(source["request_id"]),
        claim_token=str(claim["claim_token"]),
        observed={
            "job_id": str(thread_id),
            "provision_generation": str(source["provision_generation"]),
            "request_digest": source["request_digest"],
            "controller_configuration_digest": source[
                "controller_configuration_digest"
            ],
            "expected_pvc_uid": None,
        },
    )
    assert authorized["allowed"] is True


@pytest.mark.asyncio
async def test_unissued_bound_initial_source_can_enter_normal_end(db, monkeypatch):
    thread_id, policy, _override, dependencies = await _initial_vm(
        db, monkeypatch, native=True
    )
    current = await _bind_protected_agent(db, thread_id)
    await _poll(db, dependencies.vm_provisioner, current)
    source = await db.fetchrow(
        "SELECT * FROM vm_creation_retries WHERE thread_id=$1", thread_id
    )
    assert (await policy.admit(request_id=str(source["request_id"])))[
        "action"
    ] == "admitted"
    retirement = await db.begin_pinned_thread_retirement(
        str(thread_id),
        permanent=True,
        expected_runtime_generation=str(current["runtime_generation"]),
        expected_agent_id=str(current["agent_id"]),
        expected_attach_token=str(current["runtime_attach_token"]),
    )
    assert retirement["state"] == "pending", retirement
    assert retirement["context"]["vm_creation_source"]["request_id"] == str(
        source["request_id"]
    )
    assert await db.authorize_pinned_thread_retirement(
        str(thread_id),
        token=retirement["token"],
        generation=retirement["generation"],
        settle_status="ended",
    )
    result = await VMCreationRetryStore(db).settle_never_issued(
        request_id=str(source["request_id"])
    )
    assert result == {"settled": True, "disposition": "never_issued"}
    with pytest.raises(HTTPException) as refused:
        await _poll(db, dependencies.vm_provisioner, current)
    assert refused.value.status_code == 409
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_creation_retries WHERE thread_id=$1", thread_id
        )
        == 1
    )


@pytest.mark.asyncio
async def test_historical_prebind_source_end_captures_cleanup_only_authority(
    db, monkeypatch
):
    thread_id, policy, override, dependencies = await _initial_vm(db, monkeypatch)
    old = await db.get_thread(str(thread_id))
    assert await dependencies.vm_provisioner.create_thread_vm(
        str(thread_id),
        vm_image=override["workspace"]["vm"]["image"],
        expected_runtime_generation=str(old["runtime_generation"]),
    )
    source = await db.fetchrow(
        "SELECT * FROM vm_creation_retries WHERE thread_id=$1", thread_id
    )
    assert (await policy.admit(request_id=str(source["request_id"])))[
        "action"
    ] == "admitted"
    current = await _bind_protected_agent(db, thread_id)
    async with db.acquire() as conn, conn.transaction():
        with pytest.raises(VMCreationRetryConflict, match="thread_changed"):
            await VMCreationRetryStore(db)._thread_scope(conn, source)
    result = await db.begin_pinned_thread_retirement(
        str(thread_id),
        permanent=True,
        expected_runtime_generation=str(current["runtime_generation"]),
        expected_agent_id=str(current["agent_id"]),
        expected_attach_token=str(current["runtime_attach_token"]),
    )
    assert result["state"] == "pending", result
    captured = result["context"]["vm_creation_source"]
    assert captured["cleanup_protocol"] == "initial_attach_abort_v1"
    assert captured["request_id"] == str(source["request_id"])
    assert captured["thread_runtime_generation"] == str(
        source["thread_runtime_generation"]
    )
    assert (
        captured["thread_agent_id"] is None and captured["thread_attach_token"] is None
    )
    assert result["context"]["agent_id"] == str(current["agent_id"])
    assert (
        await db.fetchrow(
            "SELECT * FROM vm_creation_retries WHERE request_id=$1",
            source["request_id"],
        )
        == source
    )
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_creation_effects WHERE request_id=$1",
            source["request_id"],
        )
        == 0
    )
    assert (
        await db.fetchval(
            "SELECT state FROM vm_resource_reservations WHERE request_id=$1",
            source["request_id"],
        )
        == "reserved"
    )


@pytest.mark.asyncio
async def test_repeated_and_restarted_polls_keep_one_source_and_frozen_selection(
    db, monkeypatch
):
    thread_id, _policy, override, dependencies = await _initial_vm(
        db, monkeypatch, native=True
    )
    current = await _bind_protected_agent(db, thread_id)
    first, duplicate = await asyncio.gather(
        _poll(db, dependencies.vm_provisioner, current),
        _poll(db, dependencies.vm_provisioner, current),
    )
    assert first == duplicate
    before = await db.fetchrow(
        "SELECT * FROM vm_creation_retries WHERE thread_id=$1", thread_id
    )
    rebuilt = VMProvisioner()
    rebuilt._db = db
    # A reconstructed caller needs no old in-process task or controller client
    # to observe its already durable, exact request.
    assert await _poll(db, rebuilt, current) == first
    rows = await db.fetch(
        "SELECT * FROM vm_creation_retries WHERE thread_id=$1", thread_id
    )
    assert len(rows) == 1 and dict(rows[0]) == dict(before)
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_resource_waiters WHERE request_id=$1",
            before["request_id"],
        )
        == 1
    )
    assert await db.merge_thread_vm_context_if_provision_generation(
        str(thread_id),
        str(before["provision_generation"]),
        {"status": "starting"},
    )
    assert (await _poll(db, rebuilt, current))["vm_status"] == "starting"
    vm = json.loads((await db.get_thread(str(thread_id)))["metadata"])["vm"]
    assert vm["initial_runtime"] == {
        key: str(current[key])
        for key in ("runtime_generation", "agent_id", "runtime_attach_token")
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field", ["agent_id", "runtime_generation", "runtime_attach_token"]
)
async def test_stale_workspace_poll_cannot_issue_initial_vm(db, monkeypatch, field):
    thread_id, _policy, _override, dependencies = await _initial_vm(
        db, monkeypatch, native=True
    )
    current = await _bind_protected_agent(db, thread_id)
    current[field] = uuid4()
    with pytest.raises(HTTPException) as refused:
        await _poll(db, dependencies.vm_provisioner, current)
    assert refused.value.status_code == 409
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_creation_retries WHERE thread_id=$1", thread_id
        )
        == 0
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("old_vm", ["malformed"])
async def test_existing_malformed_or_retained_vm_is_not_initial_poll_authority(
    db, monkeypatch, old_vm
):
    thread_id, _policy, _override, dependencies = await _initial_vm(
        db, monkeypatch, native=True
    )
    current = await _bind_protected_agent(db, thread_id)
    await db.execute(
        "UPDATE threads SET metadata=jsonb_set(metadata,'{vm}',$2::jsonb) WHERE id=$1",
        thread_id,
        json.dumps(old_vm),
    )
    with pytest.raises(HTTPException) as refused:
        await _poll(db, dependencies.vm_provisioner, current)
    assert refused.value.status_code in {409, 503}
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_creation_retries WHERE thread_id=$1", thread_id
        )
        == 0
    )


@pytest.mark.asyncio
async def test_vm_operator_revocation_precedes_initial_poll_effect(db, monkeypatch):
    thread_id, _policy, _override, dependencies = await _initial_vm(
        db, monkeypatch, native=True
    )
    current = await _bind_protected_agent(db, thread_id)
    await db.execute(
        "INSERT INTO system_settings(key,value) VALUES('vm_workspaces','{\"enabled\":false}') "
        "ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value",
    )
    try:
        with pytest.raises(HTTPException) as refused:
            await _poll(db, dependencies.vm_provisioner, current)
        assert refused.value.status_code == 403
        assert (
            await db.fetchval(
                "SELECT count(*) FROM vm_creation_retries WHERE thread_id=$1", thread_id
            )
            == 0
        )
    finally:
        await db.execute("DELETE FROM system_settings WHERE key='vm_workspaces'")


@pytest.mark.asyncio
async def test_poll_does_not_rewrite_a_historical_source_captured_before_binding(
    db, monkeypatch
):
    thread_id, _policy, override, dependencies = await _initial_vm(
        db, monkeypatch, native=True
    )
    old = await db.get_thread(str(thread_id))
    assert await dependencies.vm_provisioner.create_thread_vm(
        str(thread_id),
        vm_image=override["workspace"]["vm"]["image"],
        expected_runtime_generation=str(old["runtime_generation"]),
    )
    source = await db.fetchrow(
        "SELECT * FROM vm_creation_retries WHERE thread_id=$1", thread_id
    )
    current = await _bind_protected_agent(db, thread_id)
    await _poll(db, dependencies.vm_provisioner, current)
    # A forged current marker cannot turn the old NULL-actor source into
    # fresh authority. The source itself remains immutable and incompatible.
    await db.execute(
        "UPDATE threads SET metadata=jsonb_set(metadata,'{vm,initial_runtime}',$2::jsonb) WHERE id=$1",
        thread_id,
        json.dumps(
            {
                key: str(current[key])
                for key in (
                    "runtime_generation",
                    "agent_id",
                    "runtime_attach_token",
                )
            }
        ),
    )
    with pytest.raises(HTTPException) as refused:
        await _poll(db, dependencies.vm_provisioner, current)
    assert refused.value.status_code == 409
    assert dict(
        await db.fetchrow(
            "SELECT * FROM vm_creation_retries WHERE thread_id=$1", thread_id
        )
    ) == dict(source)


@pytest.mark.asyncio
async def test_cleared_projection_does_not_reopen_prior_creation_source(
    db, monkeypatch
):
    thread_id, _policy, _override, dependencies = await _initial_vm(
        db, monkeypatch, native=True
    )
    current = await _bind_protected_agent(db, thread_id)
    await _poll(db, dependencies.vm_provisioner, current)
    source = await db.fetchrow(
        "SELECT * FROM vm_creation_retries WHERE thread_id=$1", thread_id
    )
    # Seed a historical projection gap, explicitly bypassing the independent
    # process-zero trigger that correctly prohibits this during normal use.
    # Only refusal to reopen this row is under test, not a lifecycle clear.
    async with db.acquire() as conn, conn.transaction():
        await conn.execute("SET LOCAL session_replication_role=replica")
        await conn.execute(
            "UPDATE threads SET metadata=metadata-'vm' WHERE id=$1", thread_id
        )
    with pytest.raises(HTTPException) as refused:
        await _poll(db, dependencies.vm_provisioner, current)
    assert refused.value.status_code in {409, 503}
    assert dict(
        await db.fetchrow(
            "SELECT * FROM vm_creation_retries WHERE thread_id=$1", thread_id
        )
    ) == dict(source)


@pytest.mark.asyncio
async def test_end_during_controller_resolution_prevents_source_admission(
    db, monkeypatch
):
    thread_id, _policy, _override, dependencies = await _initial_vm(
        db, monkeypatch, native=True
    )
    current = await _bind_protected_agent(db, thread_id)
    from orchestrator.services import vm_creation_transport

    original = vm_creation_transport.resolve_vm_creation_configuration

    async def resolve_then_end(*args, **kwargs):
        result = await original(*args, **kwargs)
        retirement = await db.begin_pinned_thread_retirement(
            str(thread_id),
            permanent=True,
            expected_runtime_generation=str(current["runtime_generation"]),
            expected_agent_id=str(current["agent_id"]),
            expected_attach_token=str(current["runtime_attach_token"]),
        )
        assert retirement["state"] == "pending"
        return result

    monkeypatch.setattr(
        vm_creation_transport, "resolve_vm_creation_configuration", resolve_then_end
    )
    with pytest.raises(HTTPException) as refused:
        await _poll(db, dependencies.vm_provisioner, current)
    assert refused.value.status_code == 409
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_creation_retries WHERE thread_id=$1", thread_id
        )
        == 0
    )


@pytest.mark.asyncio
async def test_current_config_grant_denial_prevents_source_admission(db, monkeypatch):
    thread_id, _policy, _override, dependencies = await _initial_vm(
        db, monkeypatch, native=True
    )
    current = await _bind_protected_agent(db, thread_id)
    delivery = _delivery(db, dependencies.vm_provisioner)

    async def denied(_thread, _metadata, *, status):
        status["state"] = "error"

    delivery = replace(delivery, resolve_session_config=denied)
    async with db.thread_datasource_lock(str(thread_id)):
        with pytest.raises(HTTPException) as refused:
            await agent_get_thread_workspace_locked(
                str(thread_id),
                presented_agent_id=str(current["agent_id"]),
                presented_runtime_generation=str(current["runtime_generation"]),
                presented_attach_token=str(current["runtime_attach_token"]),
                dependencies=delivery,
            )
    assert refused.value.status_code == 403
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_creation_retries WHERE thread_id=$1", thread_id
        )
        == 0
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("old_status", ["suspended", "restoring", "waiting_capacity"])
async def test_created_resume_retained_vm_uses_existing_pending_delivery(
    db, monkeypatch, old_status
):
    thread_id, _policy, _override, dependencies = await _initial_vm(
        db, monkeypatch, native=True
    )
    current = await _bind_protected_agent(db, thread_id)
    predecessor_generation = str(uuid4())
    # Model the existing Resume projection after binding, not a fresh create:
    # predecessor PVC identity is retained and any initial marker names old G.
    old_vm = {
        "status": old_status,
        "rootdisk": "kept",
        "rootdisk_pvc_uid": str(uuid4()),
        "provision_generation": str(uuid4()),
        "initial_runtime": {"runtime_generation": predecessor_generation},
    }
    if old_status == "suspended":
        old_vm.pop("initial_runtime")
    elif old_status == "waiting_capacity":
        old_vm.pop("rootdisk")
        old_vm["idle_wake_operation_id"] = str(uuid4())
        old_vm["initial_runtime"] = {
            key: str(current[key])
            for key in ("runtime_generation", "agent_id", "runtime_attach_token")
        }
    await db.execute(
        "UPDATE threads SET metadata=jsonb_set(metadata,'{vm}',$2::jsonb) WHERE id=$1",
        thread_id,
        json.dumps(old_vm),
    )
    payload = await _poll(db, dependencies.vm_provisioner, current)
    assert payload["vm_status"] == old_status
    assert "resolved_config" in payload  # existing delivery, not initial shim
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_creation_retries WHERE thread_id=$1", thread_id
        )
        == 0
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "request_id",
    [None, str(uuid4()), "malformed"],
    ids=["absent", "different", "malformed"],
)
async def test_marked_initial_vm_requires_its_exact_durable_source(
    db, monkeypatch, request_id
):
    thread_id, _policy, _override, dependencies = await _initial_vm(
        db, monkeypatch, native=True
    )
    current = await _bind_protected_agent(db, thread_id)
    incomplete = {
        "status": "provisioning",
        "provision_generation": str(uuid4()),
        "initial_runtime": {
            key: str(current[key])
            for key in (
                "runtime_generation",
                "agent_id",
                "runtime_attach_token",
            )
        },
    }
    if request_id is not None:
        incomplete["creation_request_id"] = request_id
    await db.execute(
        "UPDATE threads SET metadata=jsonb_set(metadata,'{vm}',$2::jsonb) WHERE id=$1",
        thread_id,
        json.dumps(incomplete),
    )
    with pytest.raises(HTTPException) as refused:
        await _poll(db, dependencies.vm_provisioner, current)
    assert refused.value.status_code == 409
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_creation_retries WHERE thread_id=$1", thread_id
        )
        == 0
    )


@pytest.mark.asyncio
async def test_legacy_initial_vm_poll_preserves_its_bound_marker(db, monkeypatch):
    thread_id, _policy, override, dependencies = await _initial_vm(
        db, monkeypatch, native=True
    )
    monkeypatch.delenv("VM_RESOURCE_ADMISSION_CONFIG")
    monkeypatch.setenv("VM_CREATION_RETRY_ENABLED", "false")
    current = await _bind_protected_agent(db, thread_id)
    monkeypatch.setattr(
        dependencies.vm_provisioner, "_create_http", AsyncMock(return_value=True)
    )
    await _poll(db, dependencies.vm_provisioner, current)
    vm = json.loads((await db.get_thread(str(thread_id)))["metadata"])["vm"]
    marker = vm["initial_runtime"]
    assert "creation_request_id" not in vm
    assert await db.merge_thread_vm_context_if_provision_generation(
        str(thread_id),
        vm["provision_generation"],
        {"status": "waiting_capacity"},
    )
    vm = json.loads((await db.get_thread(str(thread_id)))["metadata"])["vm"]
    assert await dependencies.vm_provisioner.create_thread_vm(
        str(thread_id),
        vm_image=override["workspace"]["vm"]["image"],
        expected_runtime_generation=str(current["runtime_generation"]),
        expected_agent_id=str(current["agent_id"]),
        expected_attach_token=str(current["runtime_attach_token"]),
        expected_vm_context=vm,
        poll=True,
    )
    after = json.loads((await db.get_thread(str(thread_id)))["metadata"])["vm"]
    assert after["initial_runtime"] == marker
    assert (await _poll(db, dependencies.vm_provisioner, current))[
        "status"
    ] == "creating"
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_creation_retries WHERE thread_id=$1", thread_id
        )
        == 0
    )


@pytest.mark.asyncio
async def test_thread_vm_retry_admits_durable_source_without_resource_enforcement(
    db, monkeypatch
):
    """The retry lifecycle is needed even when whole-launcher quotas are off."""
    thread_id, _, override, dependencies = await _initial_vm(
        db, monkeypatch, native=True
    )
    current = await _bind_cold_agent(db, thread_id)
    configuration = whole_launcher_configuration()
    configuration.update(version=1, namespace="workers", storage_class="local")
    configuration.pop("resource_admission")
    monkeypatch.delenv("VM_RESOURCE_ADMISSION_CONFIG")
    calls = []

    async def resolve(_client, request, *, secret):
        calls.append(request)
        return {"request": request, "controller_configuration": configuration}

    monkeypatch.setattr(
        "orchestrator.services.vm_creation_transport.resolve_vm_creation_configuration",
        resolve,
    )
    from unittest.mock import AsyncMock

    dependencies.vm_provisioner._create_http = AsyncMock(
        return_value={"status": "waiting_capacity"}
    )
    assert await dependencies.vm_provisioner.create_thread_vm(
        str(thread_id),
        vm_image=override["workspace"]["vm"]["image"],
        expected_runtime_generation=str(current["runtime_generation"]),
        expected_agent_id=str(current["agent_id"]),
        expected_attach_token=str(current["runtime_attach_token"]),
    )
    source = await db.fetchrow(
        "SELECT * FROM vm_creation_retries WHERE thread_id=$1", thread_id
    )
    assert source is not None, (
        "waiting before creation must retain immutable request authority for End"
    )
    assert source["thread_agent_id"] == current["agent_id"]
    assert source["thread_runtime_generation"] == current["runtime_generation"]
    assert source["thread_attach_token"] == current["runtime_attach_token"]
    metadata = (await db.get_thread(str(thread_id)))["metadata"]
    metadata = json.loads(metadata) if isinstance(metadata, str) else metadata
    assert metadata["vm"]["creation_request_id"] == str(source["request_id"])
    assert metadata["vm"].get("vm_uid") is None
    dependencies.vm_provisioner._create_http.assert_not_awaited()
    assert len(calls) == 1
    assert await db.merge_thread_vm_context_if_provision_generation(
        str(thread_id),
        str(source["provision_generation"]),
        {"status": "waiting_capacity"},
    )
    retirement = await db.begin_pinned_thread_retirement(
        str(thread_id), permanent=False
    )
    assert retirement["state"] == "pending"
    assert retirement["context"]["vm"] is None
    assert retirement["context"]["vm_creation_source"]["request_id"] == str(
        source["request_id"]
    )
    assert await db.authorize_pinned_thread_retirement(
        str(thread_id),
        token=retirement["token"],
        generation=retirement["generation"],
        settle_status="ended",
    )
    settled = await VMCreationRetryStore(db).settle_never_issued(
        request_id=str(source["request_id"])
    )
    assert settled == {"settled": True, "disposition": "never_issued"}
    assert await VMCreationRetryStore(db).claim_due(limit=10) == []
    assert len(calls) == 1
