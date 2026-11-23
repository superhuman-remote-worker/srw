"""A VM Session that slept once can still be Ended, asleep or awake.

An idle wake creates the successor VM on the kept rootdisk with no agent bound
yet (``origin='initial'``, ``expected_pvc_uid`` = the kept disk,
``thread_wake_operation_id`` set). The End cleanup guard used to know only
initial sources with a fresh disk and the agent bound at creation, so every
End after a wake raised ``VM thread cleanup source identity changed``.

The wake's successor is created here through the real creation path (frozen
request, creation retry, controller actuator, adoption) on the same disk the
sleep kept; only the controller's Kubernetes API and the agent Pod are fakes.
"""

import json
import logging
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import asyncpg
import pytest
from kubernetes.client.exceptions import ApiException

from orchestrator.schemas.agent_thread_status import AgentThreadStatusRequest
from orchestrator.services.agent_thread_status import update_thread_status
from orchestrator.services.pinned_retirement import PinnedRetirementOperations
from orchestrator.services.session_router import SessionRouterService
from orchestrator.services.vm_creation_retry_store import VMCreationRetryStore
from orchestrator.services.vm_idle_lifecycle import VMIdleLifecycleStore
from orchestrator.services.vm_provisioner import VMProvisioner, VMTeardownIdentity
from orchestrator.services.vm_provisioning_phases import VMProvisioningPhaseStore
from orchestrator.services.vm_thread_network import document
from orchestrator.services import vm_workspace_recovery_store as recovery_module
from orchestrator.services.vm_workspace_recovery_store import (
    VMWorkspaceRecoveryStore,
    acquire_pinned_thread_retirement_cleanup_permit,
)
from orchestrator.services.thread_resume import resume_thread
from orchestrator.services.vm_thread_retained_resume import ensure_retained_thread_vm
from tests.test_pinned_vm_initial_binding_real_postgres import _bind_cold_agent
from tests.test_vm_creation_actuation import SECRET
from tests.test_vm_idle_pinned_session_real_postgres import bind_fresh_agent
from tests.test_vm_resource_thread_cleanup_real_postgres import PhysicalStop
from tests.test_vm_resource_thread_source_real_postgres import _ready_charged_thread
from tests.test_vm_thread_adopted_without_quotas_delete_real_postgres import (
    _base_db,  # noqa: F401
    _schema_applied,  # noqa: F401
    adopted_source,
    cleaned_retirement,
    db,  # noqa: F401
    pg_dsn,  # noqa: F401
    setup,  # noqa: F401
    thread_schema,  # noqa: F401
)
from vm_controller.creation_actuation import CreationActuator
from vm_controller.creation_configuration import resolve_creation_configuration


async def publish_ready(store, thread_id, source):
    """The readiness prober's exact Ready publication for one adopted source."""
    pending = {
        "status": "ssh_pending",
        "vmi_uid": str(uuid4()),
        "active_pod_uid": str(uuid4()),
        "ssh_registration_id": uuid4().hex,
        "ssh_host": "10.42.0.42",
        "pod_ip": "10.42.0.42",
        "ssh_port": 22,
        "ssh_ready_source": "provisioner_probe",
        "ssh_verified_at": datetime.now(timezone.utc).isoformat(),
    }
    generation = str(source["provision_generation"])
    assert await store.merge_thread_vm_context_if_provision_generation(
        str(thread_id), generation, pending, require_status_not_ready=True
    )
    vm = document((await store.get_thread(str(thread_id)))["metadata"])["vm"]
    assert await VMProvisioningPhaseStore(store).publish_thread_ready(
        str(thread_id),
        generation,
        pending["ssh_registration_id"],
        vm["vm_uid"],
        {**pending, "status": "ready"},
    )
    return document((await store.get_thread(str(thread_id)))["metadata"])["vm"]


def identity_of(vm):
    return {
        "generation": vm["provision_generation"],
        "vm_uid": vm["vm_uid"],
        "vmi_uid": vm["vmi_uid"],
        "launcher_uid": vm["active_pod_uid"],
        "pvc_uid": vm["rootdisk_pvc_uid"],
    }


async def sleep_session(store, monkeypatch, thread_id):
    """Natural pause, 15 minutes later the pinned idle release on the kept disk."""
    monkeypatch.setenv("VM_MODE", "same-cluster")
    monkeypatch.setenv("WORKSPACE_IDLE_RELEASE_ENABLED", "true")
    monkeypatch.setenv("VM_REMOTE_OPERATION_PROTOCOL_ENABLED", "true")
    monkeypatch.setenv("VM_PERSISTENT_ROOTDISK", "true")
    thread = await store.get_thread(str(thread_id))
    vm = document(thread["metadata"])["vm"]
    identity = identity_of(vm)
    agent = await store.fetchrow(
        "SELECT pod_uid FROM agents WHERE id=$1", thread["agent_id"]
    )
    process = uuid4()
    await store.execute(
        "UPDATE agents SET metadata=jsonb_build_object("
        "'dispatch_process_generation',$2::text) WHERE id=$1",
        thread["agent_id"],
        str(process),
    )
    # A turn runs, then ends with nobody watching: the natural pause.
    for status in ("active", "awaiting_user"):
        await update_thread_status(
            str(thread_id),
            AgentThreadStatusRequest(
                status=status,
                agent_id=thread["agent_id"],
                pod_uid=agent["pod_uid"],
                process_generation=str(process),
                session_runtime_generation=thread["runtime_generation"],
                session_runtime_attach_token=thread["runtime_attach_token"],
            ),
            dependencies=SimpleNamespace(
                db=store, thread_accepts_runtime=lambda _row: True
            ),
        )
    row = await store.fetchrow(
        "SELECT workspace_idle_revision,workspace_idle_episode FROM threads WHERE id=$1",
        thread_id,
    )
    episode = json.loads(row["workspace_idle_episode"])
    episode["episode_id"] = str(uuid4())
    episode["entered_at"] = (
        datetime.now(timezone.utc) - timedelta(minutes=16)
    ).isoformat()
    revision = row["workspace_idle_revision"] + 1
    await store.execute(
        "UPDATE threads SET workspace_idle_revision=$2,"
        "workspace_idle_episode=$3::jsonb WHERE id=$1",
        thread_id,
        revision,
        json.dumps(episode),
    )
    idle = VMIdleLifecycleStore(store)
    operation = await idle.admit_thread_release(
        str(thread_id),
        episode_id=episode["episode_id"],
        revision=revision,
        identity=identity,
        turn_quiescent=True,
    )
    assert operation is not None
    context = json.loads(
        await store.fetchval(
            "SELECT runtime_retirement_context FROM threads WHERE id=$1", thread_id
        )
    )
    assert await store.acknowledge_pinned_thread_local_quiescence(
        str(thread_id),
        expected_runtime_generation=str(operation["thread_runtime_generation"]),
        expected_retirement_token=str(operation["thread_retirement_token"]),
        expected_agent_id=context["agent_id"],
        expected_attach_token=context["runtime_attach_token"],
        expected_settle_status="suspended",
        expected_quiescence_protocol="workspace_actuator_zero_v1",
        expected_workspace_generation=identity["generation"],
        expected_workspace_runtime_incarnation=identity["vm_uid"],
    )
    assert await store.settle_pinned_thread_retirement(
        str(thread_id),
        token=str(operation["thread_retirement_token"]),
        generation=str(operation["thread_runtime_generation"]),
        final_status="suspended",
    )
    await store.execute(
        "UPDATE threads SET metadata=jsonb_set(metadata,'{vm}',"
        "metadata->'vm' || $2::jsonb) WHERE id=$1",
        thread_id,
        json.dumps(
            {"status": "suspending", "_suspend_remote_io_closed": str(operation["id"])}
        ),
    )
    await store.execute(
        "INSERT INTO managed_repository_process_zero_receipts "
        "(owner_kind,owner_id,scope,provisioner,runtime_incarnation) "
        "VALUES('thread',$1,'vm','vm',$2)",
        thread_id,
        identity["generation"],
    )
    assert await idle.record_thread_agent_stop(
        str(operation["id"]),
        evidence={
            "version": 1,
            "pod": json.loads(operation["thread_agent_pod_identity"]),
            "disposition": "exact_absent",
            "retirement_token": str(operation["thread_retirement_token"]),
            "controller_authenticated": True,
        },
    )
    assert await idle.complete_release(
        str(operation["id"]),
        evidence={
            "version": 1,
            "kind": "vm_idle_physical_stop",
            "operation_id": str(operation["id"]),
            **identity,
            "vm_absent": True,
            "vmi_absent": True,
            "launcher_absent": True,
            "retained_pvc": True,
            "controller_authenticated": True,
            "same_generation_replacement": False,
        },
    )
    asleep = await store.get_thread(str(thread_id))
    assert asleep["status"] == "suspended" and asleep["agent_id"] is None
    return operation


def controller_provisioner(store, controller_setup, monkeypatch):
    """The orchestrator's creation client against the controller fake."""
    controller = controller_setup[0]

    async def resolve(_client, request, *, secret):
        assert secret == SECRET
        return resolve_creation_configuration(controller, request)

    monkeypatch.setattr(
        "orchestrator.services.vm_creation_transport.resolve_vm_creation_configuration",
        resolve,
    )
    monkeypatch.setenv("VM_MODE", "same-cluster")
    monkeypatch.setenv("VM_CREATION_RETRY_ENABLED", "true")
    monkeypatch.setenv("VM_NETWORK_PROFILE_ENABLED", "false")
    provisioner = VMProvisioner()
    provisioner._db = store
    provisioner._controller_url = "http://controller.test"
    provisioner._http_client = object()
    provisioner._lifecycle_hmac_secret = SECRET
    return provisioner


def stop_compute_keep_disk(controller_setup):
    """The stopped VM: compute gone, DataVolume and PVC kept."""
    api = controller_setup[1]
    for key in list(api.objects):
        if key[0] not in {"DataVolume", "PersistentVolumeClaim", "Lease"}:
            del api.objects[key]


async def actuate(store, controller_setup, request_id):
    """The controller's creation actuator for one claimed source, to adoption."""
    controller = controller_setup[0]
    source = await store.fetchrow(
        "SELECT * FROM vm_creation_retries WHERE request_id=$1", request_id
    )
    retry = VMCreationRetryStore(store)
    claim = (await retry.claim_due(limit=1))[0]
    assert claim["request_id"] == request_id

    async def authority(path, body, *, operation):
        method = path.rsplit("/", 1)[1].replace("-", "_")
        assert operation == "creation_retry_" + method
        return await getattr(
            retry, "authorize_controller" if method == "authorize" else method
        )(**body)

    controller._workspace_cleanup_authority_request = authority
    payload = {
        **json.loads(source["canonical_request"]),
        "creation_retry": {
            "version": 1,
            "request_id": str(source["request_id"]),
            "claim_token": str(claim["claim_token"]),
            "request_digest": source["request_digest"],
            "controller_configuration_digest": source[
                "controller_configuration_digest"
            ],
        },
    }
    for _ in range(8):
        result = await CreationActuator(controller)._run(payload)
        if result["status"] == "created":
            break
    assert result["status"] == "created", result
    adopted = dict(
        await store.fetchrow(
            "SELECT * FROM vm_creation_retries WHERE request_id=$1", request_id
        )
    )
    assert adopted["state"] == "succeeded" and adopted["reason"] == "creation_adopted"
    return adopted


async def wake_session(
    store, controller_setup, monkeypatch, thread_id, original, *, execution
):
    """The idle wake's own successor VM on the kept disk, then exact Ready."""
    stop_compute_keep_disk(controller_setup)
    idle = VMIdleLifecycleStore(store)
    waking = await idle.request_thread_wake(
        str(thread_id), execution_requested=execution
    )
    assert waking is not None and waking["wake_generation"] is not None
    owner = await store.get_thread(str(thread_id))
    provisioner = controller_provisioner(store, controller_setup, monkeypatch)
    request = json.loads(original["canonical_request"])
    assert await provisioner.create_thread_vm(
        str(thread_id),
        vm_image=request["vm_image"],
        cpu_cores=request["cpu_cores"],
        memory=request["memory"],
        wake_operation_id=str(waking["id"]),
        expected_runtime_generation=str(owner["runtime_generation"]),
        expected_agent_id=None,
        expected_attach_token=None,
        expected_vm_context=document(owner["metadata"])["vm"],
    )
    source = await actuate(store, controller_setup, waking["wake_request_id"])
    # The k3d shape: an adopted initial source on the kept disk, bound to the
    # asleep runtime generation with no agent, named by its wake operation.
    assert source["origin"] == "initial"
    assert source["thread_wake_operation_id"] == waking["id"]
    assert source["thread_agent_id"] is None
    assert source["thread_attach_token"] is None
    assert source["thread_runtime_generation"] == owner["runtime_generation"]
    assert (
        source["expected_pvc_uid"]
        == original["observed_pvc_uid"]
        == source["observed_pvc_uid"]
    )
    vm = await publish_ready(store, thread_id, source)
    assert await idle.mark_thread_wake_ready(
        str(waking["id"]),
        generation=vm["provision_generation"],
        vm_uid=vm["vm_uid"],
        vmi_uid=vm["vmi_uid"],
        launcher_uid=vm["active_pod_uid"],
        pvc_uid=vm["rootdisk_pvc_uid"],
    )
    return waking, source


async def end_after_wake(store, controller_setup, monkeypatch, *, mode):
    """A Session that slept once, then woke in one of three ways.

    ``asleep``: an access-only wake (Open browser, SSH); the VM is up and the
    Session stays asleep with no agent (k3d ``b6786b23``). ``awake``: the
    execution wake of opening the Session; a fresh agent (k3d ``5f12b748``).
    ``resumed``: an access-only wake, then Resume; the access continuation
    prepares the fresh agent on the already woken VM.
    """
    current, original = await adopted_source(store, controller_setup, monkeypatch)
    thread_id = current["id"]
    await publish_ready(store, thread_id, original)
    slept = await sleep_session(store, monkeypatch, thread_id)
    waking, source = await wake_session(
        store,
        controller_setup,
        monkeypatch,
        thread_id,
        original,
        execution=mode == "awake",
    )
    idle = VMIdleLifecycleStore(store)
    if mode == "awake":
        # The execution wake opens the fresh runtime only after exact Ready.
        assert not await idle.finish_thread_wake(str(waking["id"]))
        assert (
            await store.fetchval("SELECT status FROM threads WHERE id=$1", thread_id)
            == "created"
        )
        await bind_fresh_agent(store, thread_id)
    assert await idle.finish_thread_wake(str(waking["id"]))
    if mode == "resumed":
        asleep = await store.get_thread(str(thread_id))
        resumed = await resume_thread(
            str(thread_id),
            {"id": str(asleep["user_id"])},
            asleep,
            dependencies=MagicMock(store=store),
        )
        assert resumed == {"status": "resuming", "wake_id": str(waking["wake_id"])}
        assert await idle.get_pending_access_continuation(str(thread_id))
        await bind_fresh_agent(store, thread_id)
        assert await idle.complete_access_continuation(str(waking["id"]))
    woken = await store.get_thread(str(thread_id))
    assert (woken["agent_id"] is None) is (mode == "asleep")
    assert woken["runtime_generation"] == source["thread_runtime_generation"]
    return woken, slept, waking, source, original


def teardown_identity(vm):
    return VMTeardownIdentity(
        provision_generation=vm["provision_generation"],
        vm_uid=vm["vm_uid"],
        rootdisk_pvc_uid=vm["rootdisk_pvc_uid"],
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["asleep", "awake", "resumed"])
async def test_end_after_idle_wake_completes_its_open_admission(
    db,  # noqa: F811 - imported PostgreSQL fixture
    setup,  # noqa: F811 - imported controller fixture
    monkeypatch,
    mode,
):
    woken, _, _, source, _ = await end_after_wake(db, setup, monkeypatch, mode=mode)
    thread_id = str(woken["id"])
    vm = document(woken["metadata"])["vm"]
    begun = await db.begin_pinned_thread_retirement(thread_id, permanent=False)
    assert begun["state"] == "pending"
    assert await db.authorize_pinned_thread_retirement(
        thread_id,
        token=begun["token"],
        generation=begun["generation"],
        settle_status="ended",
    )
    # An End before the fix: its admission commits, then the guard refused
    # the authority. The admission stays open with no authority row (k3d).
    real_prepare = recovery_module.prepare_vm_cleanup_resource

    async def refused_once(*args, **kwargs):
        monkeypatch.setattr(
            recovery_module, "prepare_vm_cleanup_resource", real_prepare
        )
        raise asyncpg.CheckViolationError("VM thread cleanup source identity changed")

    monkeypatch.setattr(recovery_module, "prepare_vm_cleanup_resource", refused_once)
    with pytest.raises(asyncpg.CheckViolationError):
        await acquire_pinned_thread_retirement_cleanup_permit(
            VMWorkspaceRecoveryStore(db),
            thread_id=thread_id,
            identity=teardown_identity(vm),
            purge_disk=False,
        )
    leftover = await db.fetchrow(
        "SELECT * FROM vm_workspace_cleanup_admissions "
        "WHERE owner_id=$1 AND source='pinned_thread_retirement'",
        woken["id"],
    )
    assert leftover["completed_at"] is None
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_resource_thread_cleanup_authorities "
            "WHERE thread_id=$1",
            woken["id"],
        )
        == 0
    )
    # The retry of End (Cockpit, or the stale-agent detector every 5 s)
    # reuses and completes that admission; End keeps the disk.
    retirement = await cleaned_retirement(db, woken, permanent=False)
    admissions = await db.fetch(
        "SELECT id,completed_at,outcome FROM vm_workspace_cleanup_admissions "
        "WHERE owner_id=$1 AND source='pinned_thread_retirement'",
        woken["id"],
    )
    assert [(row["id"], row["outcome"]) for row in admissions] == [
        (leftover["id"], "completed")
    ]
    authority = await db.fetchrow(
        "SELECT * FROM vm_resource_thread_cleanup_authorities "
        "WHERE cleanup_admission_id=$1",
        leftover["id"],
    )
    assert authority["request_id"] == source["request_id"]
    assert authority["agent_id"] == woken["agent_id"]
    assert authority["purge_disk"] is False
    assert await db.settle_pinned_thread_retirement(
        thread_id,
        token=retirement["token"],
        generation=retirement["generation"],
        final_status="ended",
    )
    ended = await db.get_thread(thread_id)
    assert ended["status"] == "ended"
    assert document(ended["metadata"])["vm"]["status"] == "deleted"
    if mode == "awake":
        await resume_on_kept_disk_then_delete(db, setup, monkeypatch, ended, source)
    else:
        await delete_ended(db, ended)


async def delete_ended(store, ended):
    """Delete after End: the retained-disk purge of the wake's kept disk."""
    permanent = await cleaned_retirement(store, ended, permanent=True)
    purge = await store.fetchrow(
        "SELECT * FROM vm_workspace_cleanup_admissions WHERE owner_id=$1 "
        "AND source='pinned_thread_retained_disk_purge'",
        ended["id"],
    )
    assert purge is not None and purge["outcome"] == "completed"
    await store.delete_thread(
        str(ended["id"]),
        expected_runtime_generation=permanent["generation"],
        expected_runtime_retirement_token=permanent["token"],
    )
    assert await store.get_thread(str(ended["id"])) is None


async def resume_on_kept_disk_then_delete(
    store, controller_setup, monkeypatch, ended, wake_source
):
    """Resume after End: the retained Resume's VM on the disk the wake kept."""
    stop_compute_keep_disk(controller_setup)
    assert await store.resume_thread(str(ended["id"]))
    operation = await store.fetchrow(
        "SELECT * FROM vm_thread_retained_resumes WHERE thread_id=$1", ended["id"]
    )
    assert operation is not None
    current = await _bind_cold_agent(
        store, ended["id"], pod_name="srw-agent-s-" + uuid4().hex[:8]
    )
    provisioner = controller_provisioner(store, controller_setup, monkeypatch)
    assert await ensure_retained_thread_vm(
        current, store=store, provisioner=provisioner
    )
    resumed = await actuate(store, controller_setup, operation["request_id"])
    assert resumed["origin"] == "resume"
    assert resumed["thread_retained_resume_id"] == operation["id"]
    assert (
        resumed["expected_pvc_uid"]
        == wake_source["observed_pvc_uid"]
        == resumed["observed_pvc_uid"]
    )
    permanent = await cleaned_retirement(
        store, await store.get_thread(str(ended["id"])), permanent=True
    )
    await store.delete_thread(
        str(ended["id"]),
        expected_runtime_generation=permanent["generation"],
        expected_runtime_retirement_token=permanent["token"],
    )
    assert await store.get_thread(str(ended["id"])) is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("mode", "defect"),
    [
        ("awake", "wake_request"),
        ("awake", "ready_identity"),
        ("awake", "open_operation"),
        ("awake", "slept_pod"),
        ("asleep", "runtime_generation"),
        ("asleep", "wake_pvc"),
    ],
)
async def test_end_after_idle_wake_refuses_inexact_wake_evidence(
    db,  # noqa: F811
    setup,  # noqa: F811
    monkeypatch,
    mode,
    defect,
):
    """A source that is not a proven wake is still refused (damaged history)."""
    woken, _, waking, source, _ = await end_after_wake(
        db, setup, monkeypatch, mode=mode
    )
    statement = {
        "wake_request": (
            "UPDATE vm_idle_operations SET wake_request_id=gen_random_uuid() "
            "WHERE id=$1"
        ),
        "ready_identity": (
            "UPDATE vm_idle_operations SET thread_wake_ready_identity="
            "jsonb_set(thread_wake_ready_identity,'{vm_uid}',"
            "to_jsonb(gen_random_uuid()::text)) WHERE id=$1"
        ),
        "open_operation": (
            "UPDATE vm_idle_operations SET phase='waking',closed_at=NULL WHERE id=$1"
        ),
        # The captured agent would be the Pod the sleep stopped.
        "slept_pod": (
            "UPDATE vm_idle_operations o SET thread_agent_pod_identity="
            "t.metadata->'agent_pod' FROM threads t "
            "WHERE o.id=$1 AND t.id=o.owner_id"
        ),
        "runtime_generation": (
            "UPDATE vm_creation_retries SET thread_runtime_generation="
            "gen_random_uuid() WHERE thread_wake_operation_id=$1"
        ),
        "wake_pvc": (
            "UPDATE vm_idle_operations SET pvc_uid=gen_random_uuid() WHERE id=$1"
        ),
    }[defect]
    async with db.acquire() as conn, conn.transaction():
        await conn.execute("SET LOCAL session_replication_role='replica'")
        assert await conn.execute(statement, waking["id"]) == "UPDATE 1"
    with pytest.raises(asyncpg.CheckViolationError, match="source identity changed"):
        await cleaned_retirement(db, woken, permanent=False)
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_resource_thread_cleanup_authorities "
            "WHERE request_id=$1",
            source["request_id"],
        )
        == 0
    )


async def charged_wake_source(store, monkeypatch, *, exact):
    """A quota (v3) Session whose current VM is a wake's, with a fresh agent.

    The v3 wake is not driven end to end here (charged sleep and resource
    admitted wake have their own suites); the history is rewritten so the
    charged, Ready, agentless source carries a wake's lineage: its closed
    Ready ``pinned_thread`` operation and the kept disk. Only the guard's
    decision on that lineage is under test; every charge check is real.
    """
    case = await _ready_charged_thread(store, monkeypatch)
    thread_id = case["thread_id"]
    assert await VMProvisioningPhaseStore(store).publish_thread_ready(
        str(thread_id),
        str(case["generation"]),
        case["registration"],
        case["vm_uid"],
        case["updates"],
    )
    source = await store.fetchrow(
        "SELECT * FROM vm_creation_retries WHERE request_id=$1", case["request_id"]
    )
    assert source["thread_agent_id"] is None
    assert json.loads(source["controller_configuration"])["version"] == 3
    operation_id = uuid4()
    ready_identity = {
        "generation": str(source["provision_generation"]),
        "vm_uid": str(source["observed_vm_uid"]),
        "vmi_uid": case["vmi_uid"],
        "launcher_uid": case["launcher_uid"],
        "pvc_uid": str(source["observed_pvc_uid"]),
    }
    async with store.acquire() as conn, conn.transaction():
        await conn.execute("SET LOCAL session_replication_role='replica'")
        await conn.execute(
            "INSERT INTO vm_idle_operations (id,owner_kind,owner_id,phase,episode_id,"
            "episode_revision,provision_generation,vm_uid,vmi_uid,launcher_uid,pvc_uid,"
            "retained_kind,release_kind,wake_id,wake_generation,wake_request_id,"
            "wake_execution_requested,wake_ready_at,closed_at,thread_runtime_generation,"
            "thread_retirement_token,thread_agent_pod_identity,thread_wake_ready_identity) "
            "VALUES ($1,'thread',$2,'ready',gen_random_uuid(),1,gen_random_uuid(),"
            "gen_random_uuid(),gen_random_uuid(),gen_random_uuid(),$3,'rootdisk',"
            "'pinned_thread',gen_random_uuid(),$4,$5,true,clock_timestamp(),"
            "clock_timestamp(),gen_random_uuid(),gen_random_uuid(),$6::jsonb,$7::jsonb)",
            operation_id,
            thread_id,
            source["observed_pvc_uid"] if exact else uuid4(),
            source["provision_generation"],
            source["request_id"],
            json.dumps({"pod_uid": str(uuid4()), "pod_name": "slept-agent"}),
            json.dumps(ready_identity),
        )
        await conn.execute(
            "UPDATE vm_creation_retries SET thread_wake_operation_id=$2,"
            "expected_pvc_uid=observed_pvc_uid WHERE request_id=$1",
            source["request_id"],
            operation_id,
        )
    # The wake's fresh agent, in the generation the wake's VM was created in.
    await _bind_cold_agent(store, thread_id)
    await store.execute(
        "UPDATE threads SET metadata=jsonb_set(metadata,'{config_override}',"
        '\'{"workspace":{"backend":"vm"}}\') WHERE id=$1',
        thread_id,
    )
    retirement = await store.begin_pinned_thread_retirement(
        str(thread_id), permanent=False
    )
    assert retirement["state"] == "pending", retirement
    assert retirement["context"]["agent_id"] is not None
    assert await store.authorize_pinned_thread_retirement(
        str(thread_id),
        token=retirement["token"],
        generation=retirement["generation"],
        settle_status="ended",
    )
    case["retirement"] = retirement
    case["pvc_uid"] = retirement["context"]["vm"]["rootdisk_pvc_uid"]
    return case


@pytest.mark.asyncio
@pytest.mark.parametrize("exact", [True, False], ids=["exact", "other-disk"])
async def test_quota_end_after_idle_wake_releases_its_charge(
    db,  # noqa: F811
    monkeypatch,
    exact,
):
    case = await charged_wake_source(db, monkeypatch, exact=exact)
    physical = PhysicalStop(db, case)
    # The same exact external fakes as cleaned_retirement; the VM side is
    # the quota suite's physical stop, which checks the charge stays open.
    agent = SimpleNamespace(
        is_available=True,
        delete_agent_pod_exact=AsyncMock(return_value=True),
        agent_pod_authority=AsyncMock(return_value="exact_absent"),
        retire_historical_claimant_pod_exact=AsyncMock(return_value=True),
    )
    core, networking = MagicMock(), MagicMock()
    core.read_namespaced_service.side_effect = ApiException(status=404)
    networking.read_namespaced_ingress.side_effect = ApiException(status=404)
    operations = PinnedRetirementOperations(
        SimpleNamespace(
            store=db,
            vm_provisioner=physical,
            agent_provisioner=agent,
            session_router=SessionRouterService(
                namespace="agents-a",
                ingress_host="unused.test",
                core_api=core,
                networking_api=networking,
            ),
            recovery_store=VMWorkspaceRecoveryStore(db),
            logger=logging.getLogger(__name__),
        )
    )
    if not exact:
        # The first VM cleanup admission (the captured agent's process-zero
        # recovery) already meets the guard.
        with pytest.raises(
            asyncpg.CheckViolationError, match="source identity changed"
        ):
            await operations.recover_captured_process_zero(case["retirement"])
        return
    assert await operations.recover_captured_process_zero(case["retirement"])
    await operations.cleanup_pinned_thread_retirement(
        case["retirement"], cleanup_agent_pod=True
    )
    assert physical.stopped and physical.purged is False
    charge = await db.fetchrow(
        "SELECT * FROM vm_resource_reservations WHERE id=$1",
        UUID(case["admitted"]["reservation_id"]),
    )
    assert charge["state"] == "released"
    assert await db.settle_pinned_thread_retirement(
        str(case["thread_id"]),
        token=case["retirement"]["token"],
        generation=case["retirement"]["generation"],
    )
