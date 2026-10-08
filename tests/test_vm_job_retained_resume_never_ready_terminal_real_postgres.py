"""A never-ready adopted C keeps B's PVC through pre-SSH positive stop."""

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from orchestrator.services.vm_job_cancel_retention import acquire_cancel_retention
from orchestrator.services.vm_job_retained_resume import complete_retained_cancel
from orchestrator.services.vm_pre_ssh_stop_store import VMPreSSHStopStore
from orchestrator.services.vm_workspace_recovery_store import (
    VMWorkspaceRecoveryStore,
    complete_vm_cleanup_permit,
    prepare_vm_cleanup_resource,
)
from shared.vm_cancel_retention import retained_rootdisk_from_preflight
from tests.test_vm_pre_ssh_stop_real_postgres import terminal_proof
from tests.test_vm_job_retained_resume_real_postgres import (
    _base_db,  # noqa: F401
    _db_fixture,  # noqa: F401
    _schema_applied,  # noqa: F401
    _pre_ssh_db,  # noqa: F401
    _retention_db,  # noqa: F401
    db as _resume_db,
    enabled,  # noqa: F401
    pg_dsn,  # noqa: F401
    postgres_db_fixture,  # noqa: F401
    pre_ssh_schema,  # noqa: F401
    resume_schema,  # noqa: F401
    retention_schema,  # noqa: F401
    whole_schema,  # noqa: F401
    adopted_resume,
)

db = _resume_db


@pytest.mark.asyncio
async def test_never_ready_c_positive_stop_settles_shared_keep_and_terminal(db):
    state = await adopted_resume(db, ready=False)
    identity = state["identity"]
    recovery = VMWorkspaceRecoveryStore(db)
    await db.execute(
        "UPDATE jobs SET context=jsonb_set(context,'{vm,status}',"
        "'\"retiring_process_zero\"'::jsonb) WHERE id=$1",
        UUID(state["job_id"]),
    )
    permit = await acquire_cancel_retention(
        recovery,
        job_id=state["job_id"],
        identity=identity,
    )
    assert permit.allowed and permit.parent_cleanup["intent"]["purge_disk"] is False
    assert (
        await db.fetchval(
            "SELECT policy_version FROM vm_job_cancel_retention_authorities "
            "WHERE cleanup_admission_id=$1",
            permit.admission_id,
        )
        == 2
    )
    candidate = await prepare_vm_cleanup_resource(recovery, permit)
    assert candidate is not None
    charge = await db.fetchrow(
        "SELECT * FROM vm_resource_reservations WHERE request_id=$1",
        state["retry"]["request_id"],
    )
    assert charge["state"] == "teardown"
    namespace = state["retry"]["controller_configuration"]["namespace"]
    frozen = {
        "kind": "vm_pre_ssh_stop_candidate_v1",
        "job_id": state["job_id"],
        "provision_generation": identity.provision_generation,
        "namespace": namespace,
        "vm_name": f"agent-vm-{state['job_id']}",
        "vm_uid": identity.vm_uid,
        "vmi_uid": str(charge["vmi_uid"]),
        "launcher_name": "virt-launcher-owned",
        "launcher_uid": str(charge["launcher_uid"]),
        "pvc_uid": identity.rootdisk_pvc_uid,
        "node_name": charge["node_name"],
        "node_uid": str(charge["node_uid"]),
        "vm_resource_version": "42",
        "vm_generation": 7,
        "launcher_resource_version": "43",
        "containers": [
            {
                "kind": "regular",
                "name": "compute",
                "container_id": "containerd://compute",
            },
            {
                "kind": "init",
                "name": "guest-console-log",
                "container_id": "containerd://console",
            },
        ],
    }
    preflight = {
        "version": 1,
        "kind": "vm_cancel_retention_preflight_v1",
        "stop_policy": "cancel_retention_v1",
        "frozen": frozen,
        "namespace": namespace,
        "owner_id": state["job_id"],
        "pvc_name": f"agent-vm-{state['job_id']}-rootdisk",
        "pvc_uid": identity.rootdisk_pvc_uid,
        "dv_uid": state["preflight"]["dv_uid"],
        "ownership": "standalone_dv",
        "deleting": False,
        "consumer_scope": "exact_frozen_runtime_only",
    }
    stop = VMPreSSHStopStore(db)
    intent = await stop.admit_intent(
        state["job_id"],
        identity.provision_generation,
        permit.parent_cleanup,
        frozen,
        retention_preflight=preflight,
    )
    assert intent["retention_preflight"] == preflight
    assert await stop.commit_positive_proof(
        state["job_id"],
        identity.provision_generation,
        permit.parent_cleanup,
        terminal_proof(frozen, intent["frozen_digest"]),
    )
    evidence = {
        "version": 1,
        "kind": "vm_cleanup_physical_stop",
        **{
            key: candidate[key]
            for key in (
                "job_id",
                "provision_generation",
                "vm_uid",
                "vmi_uid",
                "launcher_uid",
                "pvc_uid",
            )
        },
        "vm_absent": True,
        "vmi_absent": True,
        "launcher_absent": True,
        "same_generation_replacement": False,
        "pvc_disposition": "retained",
        "controller_authenticated": True,
        "retained_rootdisk": retained_rootdisk_from_preflight(preflight),
    }
    provisioner = SimpleNamespace(
        attest_vm_cleanup_stop=AsyncMock(return_value=evidence)
    )
    await complete_vm_cleanup_permit(
        recovery,
        permit,
        outcome="completed",
        provisioner=provisioner,
    )
    assert await complete_retained_cancel(db, state["job_id"], clear_pending=False)
    async with db.acquire() as conn:
        terminal = await conn.fetchrow(
            "SELECT terminal_kind,physical_cleanup_admission_id FROM "
            "vm_job_retained_resume_terminals WHERE resume_id=$1",
            state["resume"]["id"],
        )
        assert terminal["terminal_kind"] == "kept_compute"
        assert terminal["physical_cleanup_admission_id"] == permit.admission_id
        assert await conn.fetchval(
            "SELECT public.vm_job_cancel_retention_settled($1)",
            permit.admission_id,
        )
        assert not await conn.fetchval(
            "SELECT public.vm_job_cancel_retention_discharged($1)",
            state["retention"].admission_id,
        )
        assert (
            await conn.fetchval(
                "SELECT state FROM vm_resource_reservations WHERE request_id=$1",
                state["retry"]["request_id"],
            )
            == "released"
        )
        assert json.loads(
            await conn.fetchval(
                "SELECT context FROM jobs WHERE id=$1",
                UUID(state["job_id"]),
            )
        )["_stateless_cancel_cleanup_pending"]
