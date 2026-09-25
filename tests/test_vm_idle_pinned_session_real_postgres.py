"""Authenticated pinned VM session idle source and retirement/wake authority."""

import json
import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import asyncpg
import pytest
import pytest_asyncio
from starlette.requests import Request

from orchestrator.schemas.agent_thread_status import AgentThreadStatusRequest
from orchestrator.schemas.thread_transport import ThreadInputRequest
from orchestrator.routers import sessions as sessions_routes
from orchestrator.routers.thread_transport import thread_input
from orchestrator.services.agent_thread_status import update_thread_status
from orchestrator.services.vm_idle_lifecycle import (
    VMIdleLifecycleService, VMIdleLifecycleStore,
)
from orchestrator.services.vm_provisioner import VMProvisioner
from orchestrator.services.session_runtime_admission import (
    same_thread_runtime_authority, thread_runtime_authority,
)
from orchestrator.services.thread_retirement import end_thread_flow
from orchestrator.services.thread_resume import resume_thread
from shared.persistent_input_delivery import (
    InputDeliveryAuthorityLost, persist_input_delivery,
    transition_input_delivery,
)
from tests.test_b10_session_queries_real_postgres import (
    db as _db_fixture,  # noqa: F401
    pg_dsn,  # noqa: F401
    _schema_applied,  # noqa: F401
    _thread,
)

MIGRATION = (
    Path(__file__).resolve().parents[1]
    / "src/orchestrator/database/migrations/app/0277_vm_idle_pinned_session.sql"
)


@pytest_asyncio.fixture(scope="module")
async def session_schema(pg_dsn, _schema_applied):  # noqa: F811
    conn = await asyncpg.connect(pg_dsn)
    try:
        if not await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM information_schema.columns "
            "WHERE table_name='vm_idle_operations' "
            "AND column_name='thread_retirement_token')"
        ):
            await conn.execute(MIGRATION.read_text())
    finally:
        await conn.close()


@pytest_asyncio.fixture
async def db(session_schema, _db_fixture):  # noqa: F811
    yield _db_fixture


async def ready_pinned_thread(db, monkeypatch):
    monkeypatch.setenv("VM_MODE", "same-cluster")
    monkeypatch.setenv("WORKSPACE_IDLE_RELEASE_ENABLED", "true")
    monkeypatch.setenv("VM_REMOTE_OPERATION_PROTOCOL_ENABLED", "true")
    monkeypatch.setenv("VM_PERSISTENT_ROOTDISK", "true")
    generation, vm_uid, vmi_uid, launcher_uid, pvc_uid = (uuid4() for _ in range(5))
    agent_id, pod_uid, process_generation, attach = (uuid4() for _ in range(4))
    metadata = {
        "config_override": {"workspace": {"backend": "vm"}},
        "vm": {
            "status": "ready", "provision_generation": str(generation),
            "identity_provision_generation": str(generation),
            "identity_authenticated": True, "vm_uid": str(vm_uid),
            "vmi_uid": str(vmi_uid), "active_pod_uid": str(launcher_uid),
            "rootdisk_pvc_uid": str(pvc_uid),
            "ssh_host": "10.42.0.92", "ssh_port": 22,
            "ssh_ready_source": "provisioner_probe",
            "ssh_host_key_fingerprint": "SHA256:" + "A" * 43,
        },
    }
    _, thread_id = await _thread(
        db, lane="pinned", status="active", metadata=metadata,
    )
    runtime_generation = await db.fetchval(
        "SELECT runtime_generation FROM threads WHERE id=$1", thread_id,
    )
    pod_name = "pinned-pause-" + str(uuid4())
    attempt = str(uuid4())
    assert await db.reserve_pinned_agent_pod_provision_intent(
        str(thread_id), expected_runtime_generation=str(runtime_generation),
        attempt_id=attempt, pod_name=pod_name, provisioner="agent",
        namespace="test",
    )
    assert await db.publish_pinned_agent_pod_provision_intent(
        str(thread_id), expected_runtime_generation=str(runtime_generation),
        attempt_id=attempt, pod_name=pod_name, pod_uid=str(pod_uid),
        namespace="test",
    )
    async with db.acquire() as conn, conn.transaction():
        await conn.execute(
            "INSERT INTO agents(id,config_name,hostname,status,pod_uid,metadata) "
            "VALUES($1,'session_base',$4,'session',$2,$3::jsonb)",
            agent_id, str(pod_uid),
            json.dumps({"dispatch_process_generation": str(process_generation)}),
            pod_name,
        )
        binding = await conn.fetchrow(
            "UPDATE threads SET agent_id=$2,runtime_attach_token=$3 "
            "WHERE id=$1 RETURNING runtime_generation",
            thread_id, agent_id, attach,
        )
        await conn.execute(
            "UPDATE agents SET thread_id=$2 WHERE id=$1", agent_id, thread_id,
        )
    body = AgentThreadStatusRequest(
        status="awaiting_user", agent_id=agent_id, pod_uid=pod_uid,
        process_generation=str(process_generation),
        session_runtime_generation=binding["runtime_generation"],
        session_runtime_attach_token=attach,
    )
    return thread_id, body, {
        "generation": str(generation), "vm_uid": str(vm_uid),
        "vmi_uid": str(vmi_uid), "launcher_uid": str(launcher_uid),
        "pvc_uid": str(pvc_uid),
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("permanent", [False, True])
async def test_pinned_vm_begin_captures_repo_metadata_without_legacy_alias(
    db, monkeypatch, permanent,
):
    thread_id, _, identity = await ready_pinned_thread(db, monkeypatch)
    repo_metadata = {
        "repo_name": "srw",
        "git_remote_url": "https://example.invalid/srw.git",
    }
    async with db.acquire() as conn:
        # A historical bound VM can carry the legacy managed-repo projection.
        # Re-enable its attachment trigger before exercising Begin.
        await conn.execute(
            "ALTER TABLE threads DISABLE TRIGGER "
            "trg_managed_thread_repository_url_authority"
        )
        try:
            await conn.execute(
                "UPDATE threads SET metadata=jsonb_set(metadata, "
                "'{workspace_container}', $2::jsonb) WHERE id=$1",
                thread_id, json.dumps(repo_metadata),
            )
        finally:
            await conn.execute(
                "ALTER TABLE threads ENABLE TRIGGER "
                "trg_managed_thread_repository_url_authority"
            )

    first = await db.begin_pinned_thread_retirement(
        str(thread_id), permanent=permanent,
    )
    assert first["state"] == "pending"
    assert first["reused"] is False
    context = first["context"]
    assert context["workspace_backend"] == "vm"
    assert context["workspace_container"] == repo_metadata
    assert context["workspace_binding"] is None
    assert context["vm"]["vm_uid"] == identity["vm_uid"]
    assert "_runtime_incarnation" not in context["vm"]

    duplicate = await db.begin_pinned_thread_retirement(
        str(thread_id), permanent=permanent,
    )
    assert duplicate["state"] == "pending"
    assert duplicate["reused"] is True
    assert duplicate["token"] == first["token"]


async def bind_fresh_agent(db, thread_id):
    runtime_generation = await db.fetchval(
        "SELECT runtime_generation FROM threads WHERE id=$1", thread_id,
    )
    agent_id, pod_uid, attach, process = (uuid4() for _ in range(4))
    pod_name = "pinned-woken-" + str(uuid4())
    attempt = str(uuid4())
    assert await db.reserve_pinned_agent_pod_provision_intent(
        str(thread_id), expected_runtime_generation=str(runtime_generation),
        attempt_id=attempt, pod_name=pod_name, provisioner="agent",
        namespace="test",
    )
    assert await db.publish_pinned_agent_pod_provision_intent(
        str(thread_id), expected_runtime_generation=str(runtime_generation),
        attempt_id=attempt, pod_name=pod_name, pod_uid=str(pod_uid),
        namespace="test",
    )
    async with db.acquire() as conn, conn.transaction():
        await conn.execute(
            "INSERT INTO agents(id,config_name,hostname,status,pod_uid,metadata) "
            "VALUES($1,'session_base',$2,'session',$3,$4::jsonb)",
            agent_id, pod_name, str(pod_uid),
            json.dumps({"dispatch_process_generation": str(process)}),
        )
        await conn.execute(
            "UPDATE threads SET agent_id=$2,runtime_attach_token=$3 "
            "WHERE id=$1", thread_id, agent_id, attach,
        )
        await conn.execute(
            "UPDATE agents SET thread_id=$2 WHERE id=$1", agent_id, thread_id,
        )
    return agent_id, pod_uid, attach, process


@pytest.mark.asyncio
async def test_authenticated_natural_pause_has_one_database_clock_episode(
    db, monkeypatch,
):
    thread_id, body, identity = await ready_pinned_thread(db, monkeypatch)
    from orchestrator.services.vm_remote_operation import _identity_from_row

    selected = _identity_from_row(
        dict(await db.fetchrow("SELECT * FROM threads WHERE id=$1", thread_id)),
        owner_kind="thread", owner_id=str(thread_id), operation_kind="idle_policy",
    )
    assert selected.vm_uid == identity["vm_uid"]
    dependencies = SimpleNamespace(
        db=db, thread_accepts_runtime=lambda _row: True,
    )
    assert await update_thread_status(
        str(thread_id), body, dependencies=dependencies,
    ) == {"status": "awaiting_user"}
    first = await db.fetchrow(
        "SELECT workspace_idle_revision,workspace_idle_episode "
        "FROM threads WHERE id=$1", thread_id,
    )
    assert first["workspace_idle_revision"] == 1
    episode = json.loads(first["workspace_idle_episode"])
    assert episode["wait_kind"] == "natural_pause"
    assert episode["runtime_identity"] == {
        "owner_kind": "thread", "owner_id": str(thread_id),
        "backend": "vm", "runtime_generation": identity["generation"],
        "runtime_uid": identity["vm_uid"],
    }
    assert await update_thread_status(
        str(thread_id), body, dependencies=dependencies,
    ) == {"status": "awaiting_user"}
    assert await db.fetchval(
        "SELECT workspace_idle_episode FROM threads WHERE id=$1", thread_id,
    ) == first["workspace_idle_episode"]
    await update_thread_status(
        str(thread_id), body.model_copy(update={"status": "active"}),
        dependencies=dependencies,
    )
    assert await db.fetchval(
        "SELECT workspace_idle_episode FROM threads WHERE id=$1", thread_id,
    ) == first["workspace_idle_episode"]


@pytest.mark.asyncio
async def test_due_pinned_pause_installs_one_exact_retirement_and_operation(
    db, monkeypatch,
):
    thread_id, body, identity = await ready_pinned_thread(db, monkeypatch)
    await update_thread_status(
        str(thread_id), body,
        dependencies=SimpleNamespace(
            db=db, thread_accepts_runtime=lambda _row: True,
        ),
    )
    source = await db.fetchrow(
        "SELECT workspace_idle_revision,workspace_idle_episode "
        "FROM threads WHERE id=$1", thread_id,
    )
    episode = json.loads(source["workspace_idle_episode"])
    episode["episode_id"] = str(uuid4())
    episode["entered_at"] = (
        datetime.now(timezone.utc) - timedelta(minutes=16)
    ).isoformat()
    await db.execute(
        "UPDATE threads SET workspace_idle_revision=$2,"
        "workspace_idle_episode=$3::jsonb WHERE id=$1",
        thread_id, source["workspace_idle_revision"] + 1,
        json.dumps(episode),
    )
    operation = await VMIdleLifecycleStore(db).admit_thread_release(
        str(thread_id), episode_id=episode["episode_id"], revision=2,
        identity=identity, turn_quiescent=True,
    )
    assert operation is not None
    assert operation["release_kind"] == "pinned_thread"
    assert operation["thread_runtime_generation"] == body.session_runtime_generation
    owner = await db.fetchrow(
        "SELECT runtime_retirement_token,runtime_retirement_authorized_at "
        "FROM threads WHERE id=$1", thread_id,
    )
    assert owner["runtime_retirement_token"] == operation["thread_retirement_token"]
    assert owner["runtime_retirement_authorized_at"] is not None
    again = await VMIdleLifecycleStore(db).admit_thread_release(
        str(thread_id), episode_id=episode["episode_id"], revision=2,
        identity=identity, turn_quiescent=True,
    )
    assert again is not None and again["id"] == operation["id"]
    for mutation in (
        "thread_retirement_token=$2",
        "thread_runtime_generation=$2",
        "owner_id=$2",
    ):
        with pytest.raises(asyncpg.CheckViolationError):
            await db.execute(
                f"UPDATE vm_idle_operations SET {mutation} WHERE id=$1",
                operation["id"], uuid4(),
            )
    with pytest.raises(asyncpg.CheckViolationError):
        await db.execute(
            "DELETE FROM vm_idle_operations WHERE id=$1", operation["id"],
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("permission_status", ["approved", "denied", "pending"])
async def test_resolved_permission_history_does_not_pin_later_pause(
    db, monkeypatch, permission_status,
):
    thread_id, body, identity = await ready_pinned_thread(db, monkeypatch)
    await db.execute(
        "INSERT INTO thread_permission_requests "
        "(thread_id,tool_call_id,tool_name,status,decided_at) "
        "VALUES($1,$2,'run_command',$3,CASE WHEN $3='pending' "
        "THEN NULL ELSE clock_timestamp() END)",
        thread_id, str(uuid4()), permission_status,
    )
    await update_thread_status(
        str(thread_id), body,
        dependencies=SimpleNamespace(
            db=db, thread_accepts_runtime=lambda _row: True,
        ),
    )
    source = await db.fetchrow(
        "SELECT workspace_idle_revision,workspace_idle_episode "
        "FROM threads WHERE id=$1", thread_id,
    )
    episode = json.loads(source["workspace_idle_episode"])
    episode["episode_id"] = str(uuid4())
    episode["entered_at"] = (
        datetime.now(timezone.utc) - timedelta(minutes=16)
    ).isoformat()
    await db.execute(
        "UPDATE threads SET workspace_idle_revision=workspace_idle_revision+1,"
        "workspace_idle_episode=$2::jsonb WHERE id=$1",
        thread_id, json.dumps(episode),
    )
    operation = await VMIdleLifecycleStore(db).admit_thread_release(
        str(thread_id), episode_id=episode["episode_id"],
        revision=source["workspace_idle_revision"] + 1, identity=identity,
        turn_quiescent=True,
    )
    assert (operation is not None) is (permission_status != "pending")


@pytest.mark.asyncio
@pytest.mark.parametrize("wake_first", [False, True])
async def test_permanent_end_and_wake_serialize_on_retained_thread(
    db, monkeypatch, wake_first,
):
    thread_id, body, identity = await ready_pinned_thread(db, monkeypatch)
    await update_thread_status(
        str(thread_id), body,
        dependencies=SimpleNamespace(
            db=db, thread_accepts_runtime=lambda _row: True,
        ),
    )
    source = await db.fetchrow(
        "SELECT workspace_idle_episode FROM threads WHERE id=$1", thread_id,
    )
    episode = json.loads(source["workspace_idle_episode"])
    episode["episode_id"] = str(uuid4())
    episode["entered_at"] = (
        datetime.now(timezone.utc) - timedelta(minutes=16)
    ).isoformat()
    await db.execute(
        "UPDATE threads SET workspace_idle_revision=2,"
        "workspace_idle_episode=$2::jsonb WHERE id=$1",
        thread_id, json.dumps(episode),
    )
    idle = VMIdleLifecycleStore(db)
    operation = await idle.admit_thread_release(
        str(thread_id), episode_id=episode["episode_id"], revision=2,
        identity=identity, turn_quiescent=True,
    )
    assert operation is not None
    if wake_first:
        assert await idle.request_thread_wake(
            str(thread_id), execution_requested=True,
        ) is not None
    outcome = await end_thread_flow(
        str(thread_id), await db.get_thread(str(thread_id)),
        permanent=True, force=False,
        dependencies=MagicMock(store=db),
    )
    assert outcome["status"] == "ending"
    assert outcome["retirement_permanent"] is True
    assert await db.fetchval(
        "SELECT thread_terminal_intent_at IS NOT NULL "
        "FROM vm_idle_operations WHERE id=$1", operation["id"],
    )
    assert await idle.request_thread_wake(
        str(thread_id), execution_requested=True,
    ) is None


@pytest.mark.asyncio
async def test_input_admission_and_idle_nomination_have_one_thread_lock_winner(
    db, monkeypatch,
):
    thread_id, body, identity = await ready_pinned_thread(db, monkeypatch)
    await update_thread_status(
        str(thread_id), body,
        dependencies=SimpleNamespace(
            db=db, thread_accepts_runtime=lambda _row: True,
        ),
    )
    source = await db.fetchrow(
        "SELECT workspace_idle_episode FROM threads WHERE id=$1", thread_id,
    )
    episode = json.loads(source["workspace_idle_episode"])
    episode["episode_id"] = str(uuid4())
    episode["entered_at"] = (
        datetime.now(timezone.utc) - timedelta(minutes=16)
    ).isoformat()
    await db.execute(
        "UPDATE threads SET workspace_idle_revision=2,"
        "workspace_idle_episode=$2::jsonb WHERE id=$1",
        thread_id, json.dumps(episode),
    )

    async def input_writer():
        try:
            async with db.acquire() as conn, conn.transaction():
                return await persist_input_delivery(
                    conn, thread_id=thread_id, delivery_id=uuid4(),
                    role="user", content="continue", source="user",
                    turn_number=None, agent_id=body.agent_id,
                    pod_uid=str(body.pod_uid),
                    runtime_generation=body.process_generation,
                    session_runtime_generation=body.session_runtime_generation,
                    runtime_attach_token=body.session_runtime_attach_token,
                )
        except InputDeliveryAuthorityLost:
            return None

    operation, delivery = await asyncio.wait_for(asyncio.gather(
        VMIdleLifecycleStore(db).admit_thread_release(
            str(thread_id), episode_id=episode["episode_id"], revision=2,
            identity=identity, turn_quiescent=True,
        ),
        input_writer(),
    ), timeout=10)
    assert (operation is not None) is not (delivery is not None)
    assert await db.fetchval(
        "SELECT count(*) FROM vm_idle_operations WHERE owner_kind='thread' "
        "AND owner_id=$1", thread_id,
    ) == int(operation is not None)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "second_execution", [
        False, True, "terminal", "terminal_before_release_wake",
        "terminal_before_create",
        "terminal_pre_ready",
    ],
)
async def test_two_connections_join_one_thread_wake_and_preserve_pause_clock(
    db, monkeypatch, second_execution,
):
    thread_id, body, identity = await ready_pinned_thread(db, monkeypatch)
    await update_thread_status(
        str(thread_id), body,
        dependencies=SimpleNamespace(
            db=db, thread_accepts_runtime=lambda _row: True,
        ),
    )
    source = await db.fetchrow(
        "SELECT workspace_idle_revision,workspace_idle_episode "
        "FROM threads WHERE id=$1", thread_id,
    )
    original = json.loads(source["workspace_idle_episode"])
    episode = dict(original)
    episode["episode_id"] = str(uuid4())
    episode["entered_at"] = (
        datetime.now(timezone.utc) - timedelta(minutes=16)
    ).isoformat()
    await db.execute(
        "UPDATE threads SET workspace_idle_revision=2,"
        "workspace_idle_episode=$2::jsonb WHERE id=$1",
        thread_id, json.dumps(episode),
    )
    operation = await VMIdleLifecycleStore(db).admit_thread_release(
        str(thread_id), episode_id=episode["episode_id"], revision=2,
        identity=identity, turn_quiescent=True,
    )
    assert operation is not None
    if second_execution == "terminal_before_release_wake":
        assert await VMIdleLifecycleStore(db).request_thread_wake(
            str(thread_id), execution_requested=True,
        ) is not None
        assert await db.reserve_pinned_thread_idle_terminal_end(
            str(thread_id),
        ) == "waiting_for_release"
    context = await db.fetchval(
        "SELECT runtime_retirement_context FROM threads WHERE id=$1", thread_id,
    )
    context = json.loads(context)
    assert await db.acknowledge_pinned_thread_local_quiescence(
        str(thread_id),
        expected_runtime_generation=str(operation["thread_runtime_generation"]),
        expected_retirement_token=str(operation["thread_retirement_token"]),
        expected_agent_id=context["agent_id"],
        expected_attach_token=context["runtime_attach_token"],
        expected_settle_status="suspended",
        expected_quiescence_protocol="workspace_actuator_zero_v1",
        expected_workspace_generation=identity["generation"],
        expected_workspace_runtime_incarnation=identity["vm_uid"],
    ) is not None
    assert await db.settle_pinned_thread_retirement(
        str(thread_id), token=str(operation["thread_retirement_token"]),
        generation=str(operation["thread_runtime_generation"]),
        final_status="suspended",
    )
    # The controller evidence is a fixture; complete_release still verifies
    # the durable retirement outcome, process zero and exact source tuple.
    await db.execute(
        "UPDATE threads SET metadata=jsonb_set(metadata,'{vm}',"
        "metadata->'vm' || $2::jsonb) WHERE id=$1",
        thread_id, json.dumps({
            "status": "suspending", "_suspend_remote_io_closed": str(operation["id"]),
        }),
    )
    await db.execute(
        "INSERT INTO managed_repository_process_zero_receipts "
        "(owner_kind,owner_id,scope,provisioner,runtime_incarnation) "
        "VALUES('thread',$1,'vm','vm',$2)",
        thread_id, identity["generation"],
    )
    idle = VMIdleLifecycleStore(db)
    evidence = {
        "version": 1, "kind": "vm_idle_physical_stop",
        "operation_id": str(operation["id"]),
        "generation": identity["generation"],
        "vm_uid": identity["vm_uid"], "vmi_uid": identity["vmi_uid"],
        "launcher_uid": identity["launcher_uid"],
        "pvc_uid": identity["pvc_uid"],
        "vm_absent": True, "vmi_absent": True, "launcher_absent": True,
        "retained_pvc": True, "controller_authenticated": True,
        "same_generation_replacement": False,
    }
    assert not await idle.complete_release(
        str(operation["id"]), evidence={**evidence, "pvc_uid": str(uuid4())},
    )
    assert not await idle.complete_release(str(operation["id"]), evidence=evidence)
    pod = json.loads(operation["thread_agent_pod_identity"])
    pod_absent = {
        "version": 1, "pod": pod, "disposition": "exact_absent",
        "retirement_token": str(operation["thread_retirement_token"]),
        "controller_authenticated": True,
    }
    assert not await idle.record_thread_agent_stop(
        str(operation["id"]),
        evidence={**pod_absent, "pod": {**pod, "pod_uid": str(uuid4())}},
    )
    assert await idle.record_thread_agent_stop(
        str(operation["id"]), evidence=pod_absent,
    )
    assert await idle.complete_release(str(operation["id"]), evidence=evidence)
    assert await idle.complete_release(str(operation["id"]), evidence=evidence)
    assert not await idle.complete_release(
        str(operation["id"]),
        evidence={**evidence, "launcher_uid": str(uuid4())},
    )
    assert await db.fetchval(
        "SELECT phase FROM vm_idle_operations WHERE id=$1", operation["id"],
    ) == "suspended"
    if second_execution == "terminal_before_release_wake":
        assert await db.reserve_pinned_thread_idle_terminal_end(
            str(thread_id),
        ) == "ready_for_destructive_retirement"
        assert await idle.request_thread_wake(
            str(thread_id), execution_requested=True,
        ) is None
        return
    if second_execution == "terminal":
        assert await db.reserve_pinned_thread_idle_terminal_end(
            str(thread_id),
        ) == "ready_for_destructive_retirement"
        assert await idle.request_thread_wake(
            str(thread_id), execution_requested=True,
        ) is None
        assert not await idle.close_thread_terminal_after_delete(
            str(operation["id"]),
        )
        monkeypatch.setenv("WORKSPACE_IDLE_RELEASE_ENABLED", "false")
        assert str(operation["id"]) in {
            str(row["id"]) for row in await idle.pending_operations(limit=16)
        }
        return
    if second_execution == "terminal_before_create":
        waking = await idle.request_thread_wake(
            str(thread_id), execution_requested=True,
        )
        assert waking is not None
        assert await db.reserve_pinned_thread_idle_terminal_end(
            str(thread_id),
        ) == "ready_for_destructive_retirement"
        owner = await db.get_thread(str(thread_id))
        old_vm = json.loads(owner["metadata"])["vm"]
        proposed = VMProvisioner._fresh_provision_ctx()
        proposed.update(
            status="provisioning",
            provision_generation=str(waking["wake_generation"]),
            idle_wake_operation_id=str(waking["id"]),
            idle_wake_request_id=str(waking["wake_request_id"]),
            idle_predecessor_pvc_uid=identity["pvc_uid"],
        )
        assert not await db.begin_pinned_thread_vm_provisioning(
            str(thread_id),
            expected_runtime_generation=str(owner["runtime_generation"]),
            expected_agent_id=None, expected_attach_token=None,
            expected_vm_context=old_vm, provision_context=proposed,
            wake_operation_id=str(waking["id"]),
        )
        return
    if second_execution == "terminal_pre_ready":
        waking = await idle.request_thread_wake(
            str(thread_id), execution_requested=True,
        )
        assert waking is not None
        predecessor = json.loads((await db.get_thread(str(thread_id)))["metadata"])["vm"]
        proposed = VMProvisioner._fresh_provision_ctx()
        proposed.update(
            status="provisioning",
            provision_generation=str(waking["wake_generation"]),
            idle_wake_operation_id=str(waking["id"]),
            idle_wake_request_id=str(waking["wake_request_id"]),
            idle_predecessor_pvc_uid=identity["pvc_uid"],
        )
        owner = await db.get_thread(str(thread_id))
        prepare_authority = thread_runtime_authority(owner)
        assert prepare_authority is not None
        assert await db.begin_pinned_thread_vm_provisioning(
            str(thread_id),
            expected_runtime_generation=str(owner["runtime_generation"]),
            expected_agent_id=None, expected_attach_token=None,
            expected_vm_context=predecessor, provision_context=proposed,
            wake_operation_id=str(waking["id"]),
        )
        issued_attempt = str(uuid4())
        issued_pod = "issued-end-agent-" + str(uuid4())
        assert await db.reserve_pinned_agent_pod_provision_intent(
            str(thread_id),
            expected_runtime_generation=prepare_authority.generation,
            attempt_id=issued_attempt, pod_name=issued_pod,
            provisioner="agent", namespace="test",
        )
        assert await db.reserve_pinned_thread_idle_terminal_end(
            str(thread_id),
        ) == "waiting_for_release"
        after_end = await db.get_thread(str(thread_id))
        assert after_end["pinned_idle_terminal_intent_at"] is not None
        assert not same_thread_runtime_authority(after_end, prepare_authority)
        assert await db.reserve_pinned_agent_pod_provision_intent(
            str(thread_id),
            expected_runtime_generation=prepare_authority.generation,
            attempt_id=str(uuid4()), pod_name="stale-end-agent-" + str(uuid4()),
            provisioner="agent", namespace="test",
        ) is None
        assert not await db.publish_pinned_agent_pod_provision_intent(
            str(thread_id),
            expected_runtime_generation=prepare_authority.generation,
            attempt_id=issued_attempt, pod_name=issued_pod,
            pod_uid=str(uuid4()), namespace="test",
        )
        with pytest.raises(asyncpg.CheckViolationError):
            await db.execute(
                "UPDATE threads SET agent_id=$2 WHERE id=$1",
                thread_id, uuid4(),
            )
        with pytest.raises(asyncpg.CheckViolationError):
            await db.execute(
                "UPDATE vm_idle_operations SET wake_generation=$2 "
                "WHERE id=$1", waking["id"], uuid4(),
            )
        with pytest.raises(asyncpg.CheckViolationError):
            await db.execute(
                "UPDATE vm_idle_operations SET phase='ready' WHERE id=$1",
                waking["id"],
            )
        assert await idle.request_thread_wake(
            str(thread_id), execution_requested=True,
        ) is None
        retired = SimpleNamespace(end_thread_flow=AsyncMock(return_value={
            "status": "ending",
        }))
        provisioner = SimpleNamespace(attest_workspace_runtime=AsyncMock())
        service = VMIdleLifecycleService(
            db, provisioner, None, thread_retirement=retired,
        )
        source_op = await idle.get_operation(str(waking["id"]))
        assert not await service._finish_thread_terminal(
            source_op, current=lambda: True,
        )
        retired.end_thread_flow.assert_not_awaited()
        successor_uid, successor_vmi, successor_launcher = (
            uuid4() for _ in range(3)
        )
        assert await db.merge_thread_vm_context_if_provision_generation(
            str(thread_id), str(waking["wake_generation"]), {
                "status": "ready", "identity_authenticated": True,
                "identity_provision_generation": str(waking["wake_generation"]),
                "vm_uid": str(successor_uid), "vmi_uid": str(successor_vmi),
                "active_pod_uid": str(successor_launcher),
                "rootdisk_pvc_uid": identity["pvc_uid"],
            },
        )
        assert not await idle.mark_thread_wake_ready(
            str(waking["id"]), generation=str(waking["wake_generation"]),
            vm_uid=str(successor_uid), vmi_uid=str(successor_vmi),
            launcher_uid=str(successor_launcher), pvc_uid=identity["pvc_uid"],
        )
        provisioner.attest_workspace_runtime.return_value = SimpleNamespace(
            workspace_generation=str(waking["wake_generation"]),
            vm_uid=str(successor_uid), vmi_uid=str(successor_vmi),
            launcher_pod_uid=str(successor_launcher),
            rootdisk_pvc_uid=identity["pvc_uid"],
        )
        assert await db.reserve_pinned_thread_idle_terminal_end(
            str(thread_id),
        ) == "ready_for_destructive_retirement"
        assert not await service._finish_thread_terminal(
            source_op, current=lambda: True,
        )
        retired.end_thread_flow.assert_awaited_once()
        assert await idle.get_open_for_thread(str(thread_id)) is not None
        return
    first, second = await asyncio.gather(
        idle.request_thread_wake(str(thread_id), execution_requested=False),
        idle.request_thread_wake(
            str(thread_id), execution_requested=second_execution,
        ),
    )
    assert first is not None and second is not None
    assert first["wake_id"] == second["wake_id"]
    assert first["wake_generation"] == second["wake_generation"]
    assert first["wake_request_id"] == second["wake_request_id"]
    latest = await idle.get_open_for_thread(str(thread_id))
    assert latest["wake_execution_requested"] is second_execution
    assert await db.fetchval(
        "SELECT count(*) FROM vm_idle_operations WHERE owner_kind='thread' "
        "AND owner_id=$1 AND closed_at IS NULL", thread_id,
    ) == 1
    retired = await db.get_thread(str(thread_id))
    old_vm = json.loads(retired["metadata"])["vm"]
    proposed = VMProvisioner._fresh_provision_ctx()
    proposed.update(
        status="provisioning",
        provision_generation=str(latest["wake_generation"]),
        idle_wake_operation_id=str(latest["id"]),
        idle_wake_request_id=str(latest["wake_request_id"]),
        idle_predecessor_pvc_uid=identity["pvc_uid"],
    )
    authority = dict(
        expected_runtime_generation=str(retired["runtime_generation"]),
        expected_agent_id=None, expected_attach_token=None,
        expected_vm_context=old_vm, provision_context=proposed,
    )
    assert not await db.begin_pinned_thread_vm_provisioning(
        str(thread_id), **authority,
    )
    assert await db.begin_pinned_thread_vm_provisioning(
        str(thread_id), **authority, wake_operation_id=str(latest["id"]),
    )
    assert not await db.begin_pinned_thread_vm_provisioning(
        str(thread_id), **authority, wake_operation_id=str(latest["id"]),
    )
    successor_vm = json.loads((await db.get_thread(str(thread_id)))["metadata"])["vm"]
    assert successor_vm["provision_generation"] == str(latest["wake_generation"])
    assert successor_vm["idle_wake_request_id"] == str(latest["wake_request_id"])
    successor_uid, successor_vmi, successor_launcher = (uuid4() for _ in range(3))
    assert await db.merge_thread_vm_context_if_provision_generation(
        str(thread_id), str(latest["wake_generation"]), {
            "status": "ready", "identity_authenticated": True,
            "identity_provision_generation": str(latest["wake_generation"]),
            "vm_uid": str(successor_uid), "vmi_uid": str(successor_vmi),
            "active_pod_uid": str(successor_launcher),
            "rootdisk_pvc_uid": identity["pvc_uid"],
            "ssh_ready_source": "provisioner_probe",
            "ssh_host": "10.42.0.93", "ssh_port": 22,
            "ssh_host_key_fingerprint": "SHA256:" + "A" * 43,
        },
    )
    assert await idle.mark_thread_wake_ready(
        str(latest["id"]), generation=str(latest["wake_generation"]),
        vm_uid=str(successor_uid), vmi_uid=str(successor_vmi),
        launcher_uid=str(successor_launcher), pvc_uid=identity["pvc_uid"],
    )
    proof = await db.fetchval(
        "SELECT thread_wake_ready_identity FROM vm_idle_operations WHERE id=$1",
        latest["id"],
    )
    assert json.loads(proof) == {
        "generation": str(latest["wake_generation"]),
        "vm_uid": str(successor_uid),
        "vmi_uid": str(successor_vmi),
        "launcher_uid": str(successor_launcher),
        "pvc_uid": identity["pvc_uid"],
    }
    with pytest.raises(asyncpg.CheckViolationError):
        await db.execute(
            "UPDATE vm_idle_operations SET thread_wake_ready_identity=NULL "
            "WHERE id=$1", latest["id"],
        )
    await db.execute(
        "UPDATE threads SET metadata=jsonb_set(metadata,"
        "'{vm,active_pod_uid}',to_jsonb($2::text)) WHERE id=$1",
        thread_id, str(uuid4()),
    )
    assert not await idle.finish_thread_wake(str(latest["id"]))
    await db.execute(
        "UPDATE threads SET metadata=jsonb_set(metadata,"
        "'{vm,active_pod_uid}',to_jsonb($2::text)) WHERE id=$1",
        thread_id, str(successor_launcher),
    )
    if second_execution:
        attempts = 0
        fresh_binding = None

        async def existing_prepare_path(
            owner_id, user_id, config_name, config_override,
            runtime_authority, *, dependencies,
        ):
            nonlocal attempts, fresh_binding
            attempts += 1
            assert owner_id == str(thread_id)
            assert user_id
            assert config_name == "session_base"
            assert config_override is None
            assert runtime_authority.generation == str(
                (await db.get_thread(str(thread_id)))["runtime_generation"]
            )
            assert await db.fetchval(
                "SELECT status FROM threads WHERE id=$1", thread_id,
            ) == "created"
            if attempts == 1:
                return False  # denied/unavailable preparation remains replayable
            fresh_binding = await bind_fresh_agent(db, thread_id)
            return True

        monkeypatch.setattr(
            sessions_routes, "_do_prepare", existing_prepare_path,
        )

        async def prepare_after_ready(owner_id, operation_id):
            return await sessions_routes.prepare_woken_pinned_session(
                owner_id, operation_id,
                dependencies=SimpleNamespace(store=db),
            )

        service = VMIdleLifecycleService(
            db, None, None, thread_prepare=prepare_after_ready,
        )
        claimed = await idle.get_operation(str(latest["id"]))
        assert not await service._wake_thread(claimed, current=lambda: True)
        assert await db.fetchval(
            "SELECT status FROM threads WHERE id=$1", thread_id,
        ) == "created"
        assert await idle.get_open_for_thread(str(thread_id)) is not None
        assert await idle.request_thread_wake(
            str(thread_id), execution_requested=True,
        ) is not None
        assert await service._wake_thread(claimed, current=lambda: True)
        assert attempts == 2
    else:
        assert await idle.finish_thread_wake(str(latest["id"]))
    current = await db.fetchrow(
        "SELECT status,workspace_idle_revision,workspace_idle_episode "
        "FROM threads WHERE id=$1", thread_id,
    )
    assert current["status"] == ("created" if second_execution else "suspended")
    assert current["workspace_idle_revision"] == 3
    rebound = json.loads(current["workspace_idle_episode"])
    assert rebound["episode_id"] == episode["episode_id"]
    assert rebound["entered_at"] == episode["entered_at"]
    assert rebound["runtime_identity"]["runtime_generation"] == str(
        latest["wake_generation"]
    )
    if not second_execution:
        first_access = await idle.request_thread_wake(
            str(thread_id), execution_requested=False,
            access_kind="ssh", access_claimant="reader-a",
        )
        second_access = await idle.request_thread_wake(
            str(thread_id), execution_requested=False,
            access_kind="ssh", access_claimant="reader-a",
        )
        assert first_access is not None and second_access is not None
        assert await db.fetchval(
            "SELECT count(*) FROM vm_idle_access_leases WHERE "
            "owner_kind='thread' AND owner_id=$1 AND kind='ssh' "
            "AND claimed_by='reader-a' AND closed_at IS NULL",
            thread_id,
        ) == 1
        resumed = await resume_thread(
            str(thread_id), {"id": str((await db.get_thread(str(thread_id)))["user_id"])},
            await db.get_thread(str(thread_id)),
            dependencies=MagicMock(store=db),
        )
        assert resumed == {
            "status": "resuming", "wake_id": str(latest["wake_id"]),
        }
        assert await db.fetchval(
            "SELECT status FROM threads WHERE id=$1", thread_id,
        ) == "created"
        assert await idle.get_open_for_thread(str(thread_id)) is None
        assert await idle.get_pending_access_continuation(str(thread_id))
        assert await resume_thread(
            str(thread_id), {"id": str((await db.get_thread(str(thread_id)))["user_id"])},
            await db.get_thread(str(thread_id)),
            dependencies=MagicMock(store=db),
        ) == resumed
        queued_before = await db.fetchval(
            "SELECT count(*) FROM thread_input_deliveries WHERE thread_id=$1",
            thread_id,
        )
        input_wait = await thread_input(
            str(thread_id), ThreadInputRequest(content="continue"),
            Request({"type": "http"}),
            dependencies=SimpleNamespace(
                store=db,
                require_approved_user=AsyncMock(return_value={
                    "id": str((await db.get_thread(str(thread_id)))["user_id"]),
                }),
            ),
        )
        assert input_wait.status_code == 202
        assert json.loads(input_wait.body)["wake_id"] == str(latest["wake_id"])
        assert await db.fetchval(
            "SELECT count(*) FROM thread_input_deliveries WHERE thread_id=$1",
            thread_id,
        ) == queued_before

        access_attempts = 0
        access_binding = None

        async def prepare_access_successor(
            owner_id, user_id, config_name, config_override,
            runtime_authority, *, dependencies,
        ):
            nonlocal access_attempts, access_binding
            access_attempts += 1
            assert owner_id == str(thread_id)
            assert runtime_authority.generation == str(
                (await db.get_thread(str(thread_id)))["runtime_generation"]
            )
            if access_attempts == 1:
                return False
            if access_binding is None:
                access_binding = await bind_fresh_agent(db, thread_id)
            return True

        monkeypatch.setattr(
            sessions_routes, "_do_prepare", prepare_access_successor,
        )

        async def prepare_access(owner_id, operation_id):
            return await sessions_routes.prepare_woken_pinned_session(
                owner_id, operation_id,
                dependencies=SimpleNamespace(store=db),
            )

        access_service = VMIdleLifecycleService(
            db, None, None, thread_prepare=prepare_access,
        )
        access_service.nominate = AsyncMock(return_value=0)
        assert await access_service.reconcile_once() == 0
        assert await idle.get_pending_access_continuation(str(thread_id))
        complete = access_service.store.complete_access_continuation
        completion_attempts = 0

        async def lose_first_completion(operation_id):
            nonlocal completion_attempts
            completion_attempts += 1
            if completion_attempts == 1:
                return False  # callback succeeded; receipt write response lost
            return await complete(operation_id)

        access_service.store.complete_access_continuation = lose_first_completion
        assert await access_service.reconcile_once() == 0
        assert await idle.get_pending_access_continuation(str(thread_id))
        assert await access_service.reconcile_once() == 1
        assert await idle.get_pending_access_continuation(str(thread_id)) is None
        assert access_attempts == 3
        assert await db.fetchval(
            "SELECT phase FROM vm_idle_operations WHERE id=$1", latest["id"],
        ) == "ready"
        assert json.loads((await db.get_thread(str(thread_id)))["metadata"])[
            "vm"
        ]["provision_generation"] == str(latest["wake_generation"])
        with pytest.raises(asyncpg.CheckViolationError):
            await db.execute(
                "UPDATE vm_idle_thread_access_continuations "
                "SET requested_at=clock_timestamp() WHERE operation_id=$1",
                latest["id"],
            )
        with pytest.raises(asyncpg.CheckViolationError):
            await db.execute(
                "DELETE FROM vm_idle_thread_access_continuations "
                "WHERE operation_id=$1", latest["id"],
            )
    if second_execution is True:
        agent_id, pod_uid, attach, process = fresh_binding
        runtime_generation = str(
            (await db.get_thread(str(thread_id)))["runtime_generation"]
        )
        async with db.acquire() as conn, conn.transaction():
            delivery_id = uuid4()
            delivery = await persist_input_delivery(
                conn, thread_id=thread_id, delivery_id=delivery_id,
                role="user", content="continue", source="user",
                turn_number=None, agent_id=agent_id, pod_uid=str(pod_uid),
                runtime_generation=str(process),
                session_runtime_generation=runtime_generation,
                runtime_attach_token=attach,
            )
            assert await transition_input_delivery(
                conn, delivery_id=delivery_id, agent_id=agent_id,
                pod_uid=str(pod_uid), runtime_generation=str(process),
                session_runtime_generation=runtime_generation,
                runtime_attach_token=attach,
                claim_generation=delivery["claim_generation"],
                transition="admitted", turn_number=1,
            )
            assert await transition_input_delivery(
                conn, delivery_id=delivery_id, agent_id=agent_id,
                pod_uid=str(pod_uid), runtime_generation=str(process),
                session_runtime_generation=runtime_generation,
                runtime_attach_token=attach,
                claim_generation=delivery["claim_generation"],
                transition="settled", turn_number=1,
            )
        assert await db.fetchval(
            "SELECT workspace_idle_episode FROM threads WHERE id=$1",
            thread_id,
        ) is None
        new_body = AgentThreadStatusRequest(
            status="active", agent_id=agent_id, pod_uid=pod_uid,
            process_generation=str(process),
            session_runtime_generation=runtime_generation,
            session_runtime_attach_token=attach,
        )
        from orchestrator.services.vm_remote_operation import _identity_from_row

        _identity_from_row(
            await db.get_thread(str(thread_id)), owner_kind="thread",
            owner_id=str(thread_id), operation_kind="idle_policy",
        )
        source_dependencies = SimpleNamespace(
            db=db, thread_accepts_runtime=lambda _row: True,
        )
        await update_thread_status(
            str(thread_id), new_body, dependencies=source_dependencies,
        )
        await update_thread_status(
            str(thread_id), new_body.model_copy(update={
                "status": "awaiting_user",
            }), dependencies=source_dependencies,
        )
        second_source = await db.fetchrow(
            "SELECT workspace_idle_revision,workspace_idle_episode "
            "FROM threads WHERE id=$1", thread_id,
        )
        second_episode = json.loads(second_source["workspace_idle_episode"])
        assert second_episode["episode_id"] != episode["episode_id"]
        from orchestrator.services import vm_idle_lifecycle

        evaluate_at_source_time = vm_idle_lifecycle.evaluate_idle

        def after_second_wait(*args, now, **kwargs):
            return evaluate_at_source_time(
                *args, now=now + timedelta(minutes=16), **kwargs,
            )

        monkeypatch.setattr(
            vm_idle_lifecycle, "evaluate_idle", after_second_wait,
        )
        second_identity = {
            "generation": str(latest["wake_generation"]),
            "vm_uid": str(successor_uid), "vmi_uid": str(successor_vmi),
            "launcher_uid": str(successor_launcher),
            "pvc_uid": identity["pvc_uid"],
        }
        second = await idle.admit_thread_release(
            str(thread_id), episode_id=second_episode["episode_id"],
            revision=second_source["workspace_idle_revision"],
            identity=second_identity, turn_quiescent=True,
        )
        assert second is not None and second["id"] != latest["id"]
        second_context = json.loads(await db.fetchval(
            "SELECT runtime_retirement_context FROM threads WHERE id=$1",
            thread_id,
        ))
        assert await db.acknowledge_pinned_thread_local_quiescence(
            str(thread_id),
            expected_runtime_generation=str(second["thread_runtime_generation"]),
            expected_retirement_token=str(second["thread_retirement_token"]),
            expected_agent_id=second_context["agent_id"],
            expected_attach_token=second_context["runtime_attach_token"],
            expected_settle_status="suspended",
            expected_quiescence_protocol="workspace_actuator_zero_v1",
            expected_workspace_generation=second_identity["generation"],
            expected_workspace_runtime_incarnation=second_identity["vm_uid"],
        ) is not None
        assert await db.settle_pinned_thread_retirement(
            str(thread_id), token=str(second["thread_retirement_token"]),
            generation=str(second["thread_runtime_generation"]),
            final_status="suspended",
        )
        await db.execute(
            "UPDATE threads SET metadata=jsonb_set(metadata,'{vm}',"
            "metadata->'vm' || $2::jsonb) WHERE id=$1", thread_id,
            json.dumps({
                "status": "suspending",
                "_suspend_remote_io_closed": str(second["id"]),
            }),
        )
        await db.execute(
            "INSERT INTO managed_repository_process_zero_receipts "
            "(owner_kind,owner_id,scope,provisioner,runtime_incarnation) "
            "VALUES('thread',$1,'vm','vm',$2)",
            thread_id, second_identity["generation"],
        )
        second_pod = json.loads(second["thread_agent_pod_identity"])
        assert await idle.record_thread_agent_stop(
            str(second["id"]), evidence={
                "version": 1, "pod": second_pod,
                "disposition": "exact_absent",
                "retirement_token": str(second["thread_retirement_token"]),
                "controller_authenticated": True,
            },
        )
        assert await idle.complete_release(str(second["id"]), evidence={
            **evidence, "operation_id": str(second["id"]),
            "generation": second_identity["generation"],
            "vm_uid": second_identity["vm_uid"],
            "vmi_uid": second_identity["vmi_uid"],
            "launcher_uid": second_identity["launcher_uid"],
        })
        second_wake = await idle.request_thread_wake(
            str(thread_id), execution_requested=False,
        )
        assert second_wake is not None
        assert second_wake["wake_generation"] != latest["wake_generation"]
        assert await db.fetchval(
            "SELECT status FROM threads WHERE id=$1", thread_id,
        ) == "suspended"
        second_owner = await db.get_thread(str(thread_id))
        second_old_vm = json.loads(second_owner["metadata"])["vm"]
        next_vm = VMProvisioner._fresh_provision_ctx()
        next_vm.update(
            status="provisioning",
            provision_generation=str(second_wake["wake_generation"]),
            idle_wake_operation_id=str(second_wake["id"]),
            idle_wake_request_id=str(second_wake["wake_request_id"]),
            idle_predecessor_pvc_uid=identity["pvc_uid"],
        )
        assert await db.begin_pinned_thread_vm_provisioning(
            str(thread_id),
            expected_runtime_generation=str(second_owner["runtime_generation"]),
            expected_agent_id=None, expected_attach_token=None,
            expected_vm_context=second_old_vm, provision_context=next_vm,
            wake_operation_id=str(second_wake["id"]),
        )
        third_vm, third_vmi, third_launcher = (uuid4() for _ in range(3))
        assert await db.merge_thread_vm_context_if_provision_generation(
            str(thread_id), str(second_wake["wake_generation"]), {
                "status": "ready", "identity_authenticated": True,
                "identity_provision_generation": str(second_wake["wake_generation"]),
                "vm_uid": str(third_vm), "vmi_uid": str(third_vmi),
                "active_pod_uid": str(third_launcher),
                "rootdisk_pvc_uid": identity["pvc_uid"],
                "ssh_host": "10.42.0.94", "ssh_port": 22,
                "ssh_ready_source": "provisioner_probe",
                "ssh_host_key_fingerprint": "SHA256:" + "A" * 43,
            },
        )
        assert await idle.mark_thread_wake_ready(
            str(second_wake["id"]),
            generation=str(second_wake["wake_generation"]),
            vm_uid=str(third_vm), vmi_uid=str(third_vmi),
            launcher_uid=str(third_launcher), pvc_uid=identity["pvc_uid"],
        )
        assert await idle.finish_thread_wake(str(second_wake["id"]))
        final = await db.fetchrow(
            "SELECT status,workspace_idle_episode FROM threads WHERE id=$1",
            thread_id,
        )
        assert final["status"] == "suspended"
        final_episode = json.loads(final["workspace_idle_episode"])
        assert final_episode["episode_id"] == second_episode["episode_id"]
        assert final_episode["entered_at"] == second_episode["entered_at"]
        assert final_episode["runtime_identity"]["runtime_generation"] == str(
            second_wake["wake_generation"]
        )
        assert await db.fetchval(
            "SELECT count(*) FROM vm_idle_operations WHERE "
            "owner_kind='thread' AND owner_id=$1 AND phase='ready'",
            thread_id,
        ) == 2


@pytest.mark.asyncio
async def test_pinned_thread_ide_explicit_access_and_read_only_poll(db, monkeypatch):
    from orchestrator.routers.thread_files import (
        ThreadFilesDependencies, get_thread_ide_status,
        start_thread_ide_session, stop_thread_ide_session,
    )

    await db.execute("TRUNCATE vm_idle_access_leases, vm_idle_operations CASCADE")
    thread_id, _, _ = await ready_pinned_thread(db, monkeypatch)
    user = {"id": uuid4(), "is_approved": True}
    thread = await db.get_thread(str(thread_id))
    def admitted_proof(*args, **kwargs):
        return SimpleNamespace(
            workspace_generation=str(kwargs["expected_generation"]),
            vm_uid=str(kwargs["expected_vm_uid"]),
        )
    transport = SimpleNamespace(start_and_probe=AsyncMock(side_effect=admitted_proof),
                                probe=AsyncMock(side_effect=admitted_proof))
    deps = ThreadFilesDependencies(
        store=db, container_provisioner=object(), vm_provisioner=object(),
        thread_workspace_backend=lambda *_: "vm",
        require_stateless_workspace=lambda *_: "vm",
        require_thread_owner=AsyncMock(return_value=(user, thread)),
        vm_ide_transport=transport,
    )
    response = await start_thread_ide_session(str(thread_id), SimpleNamespace(),
                                              dependencies=deps)
    assert response["status"] == "active"
    lease_id = response["access_lease_id"]
    before = await db.fetchval(
        "SELECT expires_at FROM vm_idle_access_leases WHERE id=$1", __import__("uuid").UUID(lease_id),
    )
    status = await get_thread_ide_status(str(thread_id), SimpleNamespace(),
                                         dependencies=deps)
    assert status["code_server_url"] is None
    assert await db.fetchval(
        "SELECT expires_at FROM vm_idle_access_leases WHERE id=$1", __import__("uuid").UUID(lease_id),
    ) == before
    active = await get_thread_ide_status(str(thread_id), SimpleNamespace(),
                                         lease_id=lease_id, dependencies=deps)
    assert active["status"] == "active" and lease_id in active["code_server_url"]
    assert transport.start_and_probe.await_args.kwargs["expected_vm_uid"]
    assert transport.start_and_probe.await_args.kwargs["expected_generation"]
    assert transport.probe.await_args.kwargs["expected_vm_uid"]
    assert transport.probe.await_args.kwargs["expected_generation"]
    transport.probe.side_effect = lambda *args, **kwargs: SimpleNamespace(
        workspace_generation=str(kwargs["expected_generation"]),
        vm_uid=str(uuid4()),
    )
    wrong_proof = await get_thread_ide_status(
        str(thread_id), SimpleNamespace(), lease_id=lease_id, dependencies=deps,
    )
    assert wrong_proof["status"] == "unavailable"
    assert wrong_proof["code"] == "ide_runtime_changed"
    async def switch_during_probe(*args, **kwargs):
        await db.execute(
            "UPDATE threads SET metadata=jsonb_set(metadata,'{vm,vm_uid}',to_jsonb($2::text)) "
            "WHERE id=$1", thread_id, str(uuid4()),
        )
        return admitted_proof(*args, **kwargs)
    transport.probe.side_effect = switch_during_probe
    stale = await get_thread_ide_status(
        str(thread_id), SimpleNamespace(), lease_id=lease_id, dependencies=deps,
    )
    assert stale["status"] == "unavailable"
    assert stale["code"] == "ide_runtime_changed"
    await stop_thread_ide_session(str(thread_id), SimpleNamespace(), lease_id=lease_id,
                                  dependencies=deps)
    assert await db.fetchval(
        "SELECT closed_at IS NOT NULL FROM vm_idle_access_leases WHERE id=$1",
        __import__("uuid").UUID(lease_id),
    )


@pytest.mark.asyncio
async def test_pinned_thread_ide_start_runtime_swap_refuses_active_url(db, monkeypatch):
    from orchestrator.routers.thread_files import (
        ThreadFilesDependencies, start_thread_ide_session,
    )
    from orchestrator.services.vm_ide_transport import VMIDETransport
    from tests.test_vm_ide_transport import _Pool, _proof
    from dataclasses import replace

    await db.execute("TRUNCATE vm_idle_access_leases, vm_idle_operations CASCADE")
    thread_id, _, identity = await ready_pinned_thread(db, monkeypatch)
    user = {"id": uuid4(), "is_approved": True}
    thread = await db.get_thread(str(thread_id))
    entered, resume = asyncio.Event(), asyncio.Event()
    admitted = replace(
        _proof(), workspace_generation=identity["generation"], vm_uid=identity["vm_uid"],
    )
    successor = replace(admitted, vm_uid=str(uuid4()))
    async def swap(*args, **kwargs):
        entered.set()
        await resume.wait()
        return successor
    connection = SimpleNamespace(run=AsyncMock(), open_connection=AsyncMock())
    transport = VMIDETransport(
        SimpleNamespace(attest_workspace_runtime=AsyncMock(side_effect=swap)),
        pool=_Pool(connection), key_path="/private/key",
    )
    deps = ThreadFilesDependencies(
        store=db, container_provisioner=object(), vm_provisioner=object(),
        thread_workspace_backend=lambda *_: "vm",
        require_stateless_workspace=lambda *_: "vm",
        require_thread_owner=AsyncMock(return_value=(user, thread)),
        vm_ide_transport=transport,
    )
    task = asyncio.create_task(
        start_thread_ide_session(str(thread_id), SimpleNamespace(), dependencies=deps)
    )
    await asyncio.wait_for(entered.wait(), 2)
    await db.execute(
        "UPDATE threads SET metadata=jsonb_set(metadata,'{vm,vm_uid}',to_jsonb($2::text)) "
        "WHERE id=$1", thread_id, str(uuid4()),
    )
    resume.set()
    response = await asyncio.wait_for(task, 2)
    assert response == {"status": "unavailable", "code_server_url": None,
                        "code": "ide_runtime_changed"}
    assert await db.fetchval(
        "SELECT count(*) FROM vm_idle_access_leases WHERE owner_id=$1 AND closed_at IS NOT NULL",
        thread_id,
    ) == 1
    connection.run.assert_not_awaited()
