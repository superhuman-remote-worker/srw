"""Public Delete purges the final disk across two physical keeps and a logical tail."""

import json
import logging
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from orchestrator.services.job_mutation_controls import JobControlOperations
from orchestrator.services.thread_retirement import archive_and_cleanup_workspace
from orchestrator.services.vm_job_retained_resume import settle_retained_no_compute
from orchestrator.services.vm_provisioner import (
    VMProvisioner,
    VMTeardownIdentity,
    _VMTeardownProbe,
)
from orchestrator.services.vm_workspace_policy import vm_needs_release
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
    admit_resume,
    settled_ready_keep,
)

db = _resume_db


@pytest.mark.asyncio
@pytest.mark.parametrize("tail", ["source_absent", "never_issued"])
async def test_public_delete_purges_two_keeps_with_immutable_logical_tail(
    db, tail, monkeypatch
):
    state = await settled_ready_keep(db)
    owner = UUID(state["job_id"])
    first_keep = state["retention"].admission_id
    second_keep = state["keep"].admission_id
    physical_c = state["identity"]
    c_operation = state["resume"]
    c_terminal = await db.fetchrow(
        "SELECT * FROM vm_job_retained_resume_terminals WHERE resume_id=$1",
        c_operation["id"],
    )
    assert c_terminal["terminal_kind"] == "kept_compute"
    assert c_terminal["physical_cleanup_admission_id"] == second_keep
    assert await db.prepare_stateless_job_for_workspace_resume(
        state["job_id"],
        "vm",
        expected_status="cancelled",
        owner_resume_user_id=c_operation["requested_by"],
    )
    d_operation = await db.fetchrow(
        "SELECT * FROM vm_job_retained_resumes WHERE predecessor_terminal_id=$1",
        c_terminal["id"],
    )
    assert d_operation["physical_cleanup_admission_id"] == second_keep
    assert d_operation["root_retention_admission_id"] == first_keep
    assert d_operation["pvc_uid"] == c_operation["pvc_uid"]
    state["resume"] = d_operation
    if tail == "never_issued":
        await admit_resume(db, state)
        assert (await db.cancel_stateless_job(state["job_id"]))[0]
        assert await settle_retained_no_compute(db, state["job_id"])
        assert await db.complete_stateless_cancel_cleanup(state["job_id"])

    async def discharged(admission_id):
        return await db.fetchval(
            "SELECT public.vm_job_cancel_retention_discharged($1)", admission_id
        )

    assert not await discharged(first_keep)
    assert not await discharged(second_keep)
    observed = {}

    monkeypatch.setenv("VM_MODE", "same-cluster")
    provisioner = VMProvisioner()
    provisioner._db = db
    provisioner._controller_url = "http://controller.invalid"
    provisioner.capture_vm_teardown_identity = AsyncMock(
        side_effect=AssertionError("logical D has no VM")
    )

    async def probe(job_id, generation, **kwargs):
        assert job_id == state["job_id"]
        assert generation == physical_c.provision_generation
        # The logical projection stays D/absent throughout physical C deletion.
        assert (
            await provisioner._current_provision_generation("job", job_id) != generation
        )
        assert not await discharged(first_keep)
        assert not await discharged(second_keep)
        if "controller_scope" in kwargs:
            assert observed.get("physical_delete") is True
            value = await db.fetchval(
                "SELECT public.vm_job_retained_disk_purge_candidate(cleanup_admission_id) "
                "FROM vm_job_retained_disk_purge_authorities WHERE job_id=$1",
                owner,
            )
            observed["candidate"] = json.loads(value)
        return _VMTeardownProbe(
            disposition="absent",
            identity=VMTeardownIdentity(
                generation,
                None,
                None
                if observed.get("physical_delete")
                else physical_c.rootdisk_pvc_uid,
            ),
            rootdisk_identity_known=True,
            runtime_absence_known=True,
            vmi_absent=True,
            launcher_absent=True,
        )

    async def delete_http(job_id, **kwargs):
        assert job_id == state["job_id"]
        assert kwargs["purge_disk"] is True
        assert kwargs["provision_generation"] == physical_c.provision_generation
        assert kwargs["expected_vm_uid"] == physical_c.vm_uid
        assert kwargs["expected_rootdisk_pvc_uid"] == physical_c.rootdisk_pvc_uid
        assert kwargs["parent_cleanup"]["intent"]["source"] == "public_vm_delete"
        assert not await discharged(first_keep)
        assert not await discharged(second_keep)
        observed["physical_delete"] = True
        return True

    # Only external authenticated probe/delete transport is simulated. The real
    # generation, physical process-zero, parent and final attestation gates run.
    provisioner._probe_vm_teardown_identity = AsyncMock(side_effect=probe)
    provisioner._delete_http = AsyncMock(side_effect=delete_http)
    archive_dependencies = SimpleNamespace(
        store=db,
        recovery_store=state["recovery"],
        vm_provisioner=provisioner,
        container_provisioner=None,
        docker_provisioner=None,
        get_container_context=lambda _: {},
        get_vm_context=lambda job: (
            json.loads(job["context"])
            if isinstance(job["context"], str)
            else job["context"]
        ).get("vm", {}),
        vm_needs_release=vm_needs_release,
    )

    @asynccontextmanager
    async def vector_connection():
        yield SimpleNamespace(execute=AsyncMock())

    controls = JobControlOperations(
        SimpleNamespace(
            store=db,
            logger=logging.getLogger(__name__),
            archive_and_cleanup_workspace=lambda job_id: archive_and_cleanup_workspace(
                job_id, dependencies=archive_dependencies
            ),
            snapshot_service=SimpleNamespace(is_available=False),
            vector_db=SimpleNamespace(acquire=vector_connection),
            resolve_job_notifications=AsyncMock(),
        )
    )
    result = await controls.delete(
        state["job_id"],
        caller={"id": str(d_operation["requested_by"])},
        job=await db.get_job(state["job_id"]),
    )
    assert result["status"] == "deleted"
    provisioner.capture_vm_teardown_identity.assert_not_awaited()
    provisioner._delete_http.assert_awaited_once()
    assert provisioner._probe_vm_teardown_identity.await_count == 3
    assert await db.fetchrow("SELECT id FROM jobs WHERE id=$1", owner) is None
    assert await discharged(first_keep)
    assert await discharged(second_keep)

    candidate = observed["candidate"]
    purge = await db.fetchrow(
        "SELECT a.cleanup_admission_id,a.retained_terminal_id,r.chain_digest "
        "FROM vm_job_retained_disk_purge_authorities a "
        "JOIN vm_job_retained_disk_purge_receipts r USING(cleanup_admission_id) "
        "WHERE a.job_id=$1",
        owner,
    )
    assert purge["retained_terminal_id"] is not None
    assert purge["chain_digest"] == candidate["chain_digest"]
    predecessor_ids = await db.fetch(
        "SELECT old_cleanup_admission_id FROM vm_job_retained_disk_purge_predecessors "
        "WHERE cleanup_admission_id=$1",
        purge["cleanup_admission_id"],
    )
    assert {row["old_cleanup_admission_id"] for row in predecessor_ids} == {
        first_keep,
        second_keep,
    }
    assert (
        await db.fetchval(
            "SELECT public.vm_job_retained_disk_purge_chain_digest($1)",
            purge["cleanup_admission_id"],
        )
        == purge["chain_digest"]
    )
    historical_tail = json.loads(
        await db.fetchval(
            "SELECT public.vm_job_retained_purge_tail($1,false)",
            purge["cleanup_admission_id"],
        )
    )
    assert len(historical_tail) == 2
    assert {entry["resume_id"] for entry in historical_tail} == {
        str(c_operation["id"]),
        str(d_operation["id"]),
    }
    assert [entry["terminal_id"] for entry in historical_tail] == [
        str(purge["retained_terminal_id"]),
        str(c_terminal["id"]),
    ]
    d_terminal = await db.fetchrow(
        "SELECT * FROM vm_job_retained_resume_terminals WHERE resume_id=$1",
        d_operation["id"],
    )
    assert d_terminal["terminal_kind"] == tail
    assert d_terminal["physical_cleanup_admission_id"] == second_keep
    assert historical_tail[0]["evidence"] == json.loads(d_terminal["evidence"])
    assert historical_tail[1]["evidence"] == json.loads(c_terminal["evidence"])
