"""The owner sees workspace compute state without runtime authority fields."""
# ruff: noqa: F811 -- imported pytest fixture and its parameter name

from __future__ import annotations

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


@pytest.mark.parametrize("status", ["pending", "provisioning"])
def test_initial_vm_provisioning_has_no_idle_lifecycle(status):
    row = _owner()
    row["status"] = "created"
    row["context"]["vm"]["status"] = status
    row["workspace_idle_episode"] = None
    row["workspace_idle_revision"] = 0
    row["idle_phase"] = None
    row["idle_episode_id"] = None

    assert project_vm_idle_state(row) is None


def test_nonready_vm_with_existing_idle_operation_keeps_its_lifecycle():
    row = _owner()
    row["context"]["vm"]["status"] = "provisioning"
    row["idle_phase"] = "wake_held"
    row["idle_reason"] = "resource_reservation_held"

    assert project_vm_idle_state(row) == {
        "state": "wake_held",
        "reason_code": "resource_reservation_held",
    }


def test_nonready_vm_with_prior_idle_history_stays_held():
    row = _owner()
    row["context"]["vm"] = {"status": "provisioning"}
    row["workspace_idle_episode"] = None
    row["workspace_idle_revision"] = 2
    row["idle_phase"] = None

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
