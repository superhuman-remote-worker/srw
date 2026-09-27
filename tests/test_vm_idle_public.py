"""The owner sees workspace compute state without runtime authority fields."""
# ruff: noqa: F811 -- imported pytest fixture and its parameter name

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest

from orchestrator.services.vm_idle_public import (
    project_vm_idle_state,
    read_vm_idle_states,
)
from shared.workspace_idle_policy import IdleEpisode, RuntimeIdentity, episode_document
from tests.test_vm_idle_lifecycle_real_postgres import (
    _schema_applied,  # noqa: F401
    db,  # noqa: F401
    pg_dsn,  # noqa: F401
    postgres_db_fixture,  # noqa: F401
    profiled_idle_image_policy,  # noqa: F401
    seed_wait,
)


def _owner() -> dict:
    owner, generation, vm_uid = uuid4(), uuid4(), uuid4()
    entered = datetime.now(timezone.utc) - timedelta(minutes=1)
    episode = IdleEpisode(
        str(uuid4()),
        1,
        "human_message",
        str(uuid4()),
        entered,
        None,
        0,
        RuntimeIdentity("job", str(owner), "vm", str(generation), str(vm_uid)),
    )
    return {
        "id": owner,
        "owner_kind": "job",
        "status": "waiting_for_reply",
        "execution_lane": "stateless",
        "workspace_idle_episode": episode_document(episode),
        "workspace_idle_revision": 1,
        "idle_episode_id": episode.episode_id,
        "idle_phase": None,
        "idle_reason": None,
        "idle_retry_after": None,
        "context": {
            "vm": {
                "status": "ready",
                "identity_authenticated": True,
                "identity_provision_generation": str(generation),
                "provision_generation": str(generation),
                "vm_uid": str(vm_uid),
                "vmi_uid": str(uuid4()),
                "active_pod_uid": str(uuid4()),
                "rootdisk_pvc_uid": str(uuid4()),
                "ssh_host": "secret-host",
                "ssh_host_key_fingerprint": "SHA256:secret-key",
            }
        },
    }


def test_owner_projection_covers_warm_release_wake_and_safe_holds():
    row = _owner()
    warm = project_vm_idle_state(row)
    assert warm["state"] == "warm" and warm["idle_expires_at"]
    for phase in ("releasing", "release_held", "suspended", "waking", "wake_held"):
        row["idle_phase"] = phase
        row["idle_reason"] = "private-controller-error"
        projected = project_vm_idle_state(row)
        assert projected["state"] == phase
        assert "private-controller-error" not in str(projected)
        assert "secret-host" not in str(projected)
        if phase.endswith("held"):
            assert projected["reason_code"] == "workspace_attention"
    row["idle_phase"] = "ready"
    row["workspace_idle_episode"] = None
    assert project_vm_idle_state(row) == {"state": "ready"}
    row["context"]["vm"]["identity_authenticated"] = False
    assert project_vm_idle_state(row) == {
        "state": "release_held",
        "reason_code": "identity_unverified",
    }


@pytest.mark.parametrize(
    "status", ["pending", "provisioning", "created", "ssh_pending"]
)
@pytest.mark.parametrize("owner_kind", ["job", "thread"])
def test_initial_vm_provisioning_has_no_idle_lifecycle(status, owner_kind):
    row = _owner()
    row["status"] = "created"
    row["context"]["vm"]["status"] = status
    row["workspace_idle_episode"] = None
    row["workspace_idle_revision"] = 0
    row["idle_phase"] = None
    row["idle_episode_id"] = None
    if owner_kind == "thread":
        row["owner_kind"] = "thread"
        row["execution_lane"] = "pinned"
        row["metadata"] = row.pop("context")

    assert project_vm_idle_state(row) is None


@pytest.mark.parametrize("status", ["provisioning", "created", "ssh_pending"])
def test_nonready_vm_with_existing_idle_operation_keeps_its_lifecycle(status):
    row = _owner()
    row["context"]["vm"]["status"] = status
    row["idle_phase"] = "wake_held"
    row["idle_reason"] = "resource_reservation_held"

    assert project_vm_idle_state(row) == {
        "state": "wake_held",
        "reason_code": "resource_reservation_held",
    }


@pytest.mark.parametrize("status", ["provisioning", "created", "ssh_pending"])
def test_nonready_vm_with_prior_idle_history_stays_held(status):
    row = _owner()
    row["context"]["vm"] = {"status": status}
    row["workspace_idle_episode"] = None
    row["workspace_idle_revision"] = 2
    row["idle_phase"] = None

    assert project_vm_idle_state(row) == {
        "state": "release_held",
        "reason_code": "identity_unverified",
    }


@pytest.mark.parametrize(
    "status", ["pending", "provisioning", "created", "ssh_pending"]
)
@pytest.mark.parametrize("state", ["queued", "resolving", "reconciling", "succeeded"])
def test_current_thread_creation_supersedes_only_empty_idle_history(status, state):
    row = _thread_owner()
    request_id = str(uuid4())
    row.update(workspace_idle_episode=None, workspace_idle_revision=2, idle_phase=None)
    row["metadata"]["vm"] = {"status": status, "creation_request_id": request_id}
    creation = {
        "request_id": request_id,
        "state": state,
        "stage": "readiness" if state == "succeeded" else "creation",
    }
    assert project_vm_idle_state(row, thread_creation=creation) is None


@pytest.mark.parametrize(
    "fault",
    [
        "attention",
        "cancel_requested",
        "settled",
        "wrong_request",
        "missing_request",
        "configuration",
        "failed_vm",
        "malformed_episode",
        "idle_hold",
        "job",
        "retirement",
    ],
)
def test_creation_progress_cannot_hide_other_idle_holds(fault):
    row = _thread_owner()
    request_id = str(uuid4())
    row.update(workspace_idle_revision=2, idle_phase=None)
    episode = row["workspace_idle_episode"]
    row["workspace_idle_episode"] = None
    row["metadata"]["vm"] = {"status": "ssh_pending", "creation_request_id": request_id}
    creation = {"request_id": request_id, "state": "reconciling", "stage": "creation"}
    if fault in {"attention", "cancel_requested", "settled"}:
        creation["state"] = fault
    elif fault == "wrong_request":
        creation["request_id"] = str(uuid4())
    elif fault == "missing_request":
        del row["metadata"]["vm"]["creation_request_id"]
        del creation["request_id"]
    elif fault == "configuration":
        creation["stage"] = "configuration"
    elif fault == "failed_vm":
        row["metadata"]["vm"]["status"] = "failed"
    elif fault == "malformed_episode":
        row["workspace_idle_episode"] = {}
    elif fault == "idle_hold":
        row.update(
            workspace_idle_episode=episode,
            idle_phase="wake_held",
            idle_reason="resource_reservation_held",
        )
    elif fault == "job":
        row.update(owner_kind="job", context=row.pop("metadata"))
    elif fault == "retirement":
        row["runtime_retirement_token"] = str(uuid4())
    expected = (
        {"state": "wake_held", "reason_code": "resource_reservation_held"}
        if fault == "idle_hold"
        else {"state": "release_held", "reason_code": "identity_unverified"}
    )
    assert project_vm_idle_state(row, thread_creation=creation) == expected


@pytest.mark.parametrize("raw_episode", [{}, "{malformed json"])
def test_nonready_vm_with_nonnull_idle_episode_stays_held(raw_episode):
    row = _owner()
    row["status"] = "created"
    row["context"]["vm"] = {"status": "provisioning"}
    row["workspace_idle_episode"] = raw_episode
    row["workspace_idle_revision"] = 0
    row["idle_phase"] = None
    row["idle_episode_id"] = None

    assert project_vm_idle_state(row) == {
        "state": "release_held",
        "reason_code": "identity_unverified",
    }


@pytest.mark.asyncio
async def test_real_postgres_public_read_tracks_releasing_without_leaking_identity(
    db, monkeypatch
):
    from orchestrator.services.vm_idle_lifecycle import VMIdleLifecycleStore

    for key, value in {
        "WORKSPACE_IDLE_RELEASE_ENABLED": "true",
        "VM_CREATION_RETRY_ENABLED": "true",
        "VM_REMOTE_OPERATION_PROTOCOL_ENABLED": "true",
        "VM_MODE": "same-cluster",
        "VM_PERSISTENT_ROOTDISK": "true",
    }.items():
        monkeypatch.setenv(key, value)
    await db.execute("TRUNCATE vm_idle_access_leases, vm_idle_operations CASCADE")
    owner, episode, identity = await seed_wait(db)
    states = await read_vm_idle_states(db, owner_kind="job", owner_ids=[str(owner)])
    assert states[str(owner)]["state"] == "warm"
    operation = await VMIdleLifecycleStore(db).admit_release(
        str(owner),
        episode_id=episode.episode_id,
        revision=episode.revision,
        identity=identity,
    )
    assert operation
    states = await read_vm_idle_states(db, owner_kind="job", owner_ids=[str(owner)])
    assert states[str(owner)] == {"state": "releasing"}
    assert identity["vm_uid"] not in str(states)


def _thread_owner() -> dict:
    row = _owner()
    row.update(owner_kind="thread", status="active", execution_lane="pinned")
    row["metadata"] = row.pop("context")
    row["workspace_idle_episode"]["runtime_identity"]["owner_kind"] = "thread"
    return row


@pytest.mark.parametrize("permanent", [False, True])
@pytest.mark.parametrize("phase", [None, "releasing", "suspended", "waking"])
def test_authorized_terminal_retirement_hides_idle_projection(permanent, phase):
    row = _thread_owner()
    row.update(
        runtime_retirement_token=str(uuid4()),
        runtime_retirement_authorized_at=datetime.now(timezone.utc),
        runtime_retirement_context=json.dumps({"settle_status": "ended"}),
        runtime_retirement_permanent=permanent,
        idle_phase=phase,
    )
    assert project_vm_idle_state(row) is None
    assert row["status"] == "active"


@pytest.mark.parametrize(
    "retirement",
    [
        {
            "runtime_retirement_token": "preflight",
            "runtime_retirement_context": {"settle_status": "ended"},
        },
        {"runtime_retirement_token": None, "runtime_retirement_authorized_at": None},
        {
            "runtime_retirement_token": "suspend",
            "runtime_retirement_authorized_at": "now",
            "runtime_retirement_context": {"settle_status": "suspended"},
        },
    ],
)
@pytest.mark.parametrize("phase", ["releasing", "suspended", "waking", None])
def test_preflight_aborted_and_suspended_retirement_preserve_idle(retirement, phase):
    row = _thread_owner()
    row.update(retirement, idle_phase=phase)
    projected = project_vm_idle_state(row)
    assert projected["state"] == (phase or "warm")


def test_settled_thread_end_has_no_idle_projection():
    row = _thread_owner()
    row["status"] = "ended"
    assert project_vm_idle_state(row) is None


@pytest.mark.asyncio
async def test_real_postgres_thread_read_hides_only_authorized_terminal_retirement(db):
    row = _thread_owner()
    await db.execute(
        "INSERT INTO threads(id,status,execution_lane,metadata) VALUES($1,'active','pinned',$2::jsonb)",
        row["id"],
        json.dumps(row["metadata"]),
    )

    async def read():
        return await read_vm_idle_states(
            db, owner_kind="thread", owner_ids=[str(row["id"])]
        )

    assert (await read())[str(row["id"])] == {"state": "ready"}
    await db.execute(
        "UPDATE threads SET runtime_retirement_token=$2, "
        "runtime_retirement_permanent=false,runtime_retirement_started_at=now(), "
        "runtime_retirement_context=$3::jsonb WHERE id=$1",
        row["id"],
        uuid4(),
        json.dumps({"settle_status": "ended"}),
    )
    assert (await read())[str(row["id"])] == {"state": "ready"}
    await db.execute(
        "UPDATE threads SET runtime_retirement_authorized_at=now() WHERE id=$1",
        row["id"],
    )
    assert await read() == {}
    assert (
        await db.fetchval("SELECT status FROM threads WHERE id=$1", row["id"])
        == "active"
    )
