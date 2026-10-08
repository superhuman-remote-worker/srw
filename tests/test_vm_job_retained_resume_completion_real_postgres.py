"""S36 terminalizes an adopted retained C only after its shared physical keep."""

import json
import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from orchestrator.services.completion_effects import (
    CompletionEffectDependencies,
    run_completion_workspace_teardown,
)
from orchestrator.services.vm_provisioner import VMTeardownResult
from orchestrator.services.vm_workspace_recovery_store import VMWorkspaceRecoveryStore
from shared.vm_cancel_retention import retained_rootdisk_from_preflight
from tests.test_vm_job_retained_resume_completion_protocol import Runner
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
    authorized_completed_resume,
    ready_witness,
)

db = _resume_db


@pytest.mark.asyncio
async def test_completed_s36_retains_b_disk_and_releases_only_c_compute(db):
    state = await authorized_completed_resume(db)
    runner = Runner()
    runner.command_id = str(state["completion_command"])
    physical = {}

    async def qualify(candidate):
        proof = ready_witness(candidate)
        physical["proof"] = proof
        return proof

    async def stop(job_id, identity, **kwargs):
        assert job_id == state["job_id"] and identity == state["identity"]
        assert kwargs["purge_disk"] is False
        assert kwargs["capture_snapshot"] is False
        assert kwargs["parent_cleanup"]["retention_preflight"] == physical["proof"]
        await db.execute(
            "UPDATE jobs SET context=jsonb_set(context,'{vm,status}',"
            "'\"retiring_process_zero\"'::jsonb) WHERE id=$1",
            UUID(job_id),
        )
        await db.execute(
            "INSERT INTO managed_repository_process_zero_receipts"
            "(owner_kind,owner_id,scope,provisioner,runtime_incarnation) "
            "VALUES('job',$1,'vm','vm',$2)",
            UUID(job_id),
            identity.provision_generation,
        )
        return VMTeardownResult("completed", True)

    async def attest(candidate):
        physical["candidate"] = candidate
        return {
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
            "retained_rootdisk": retained_rootdisk_from_preflight(physical["proof"]),
        }

    provisioner = SimpleNamespace(
        capture_vm_teardown_identity=AsyncMock(return_value=state["identity"]),
        qualify_retained_ready_stop=AsyncMock(side_effect=qualify),
        release_vm_captured=AsyncMock(side_effect=stop),
        attest_vm_cleanup_stop=AsyncMock(side_effect=attest),
    )
    archive = AsyncMock(side_effect=AssertionError("legacy purge must not run"))
    dependencies = CompletionEffectDependencies(
        store=db,
        container_provisioner=None,
        vm_provisioner=provisioner,
        get_container_context=lambda _row: {},
        get_vm_context=lambda row: (
            json.loads(row["context"])
            if isinstance(row["context"], str)
            else row["context"]
        ).get("vm", {}),
        archive_and_cleanup_workspace=archive,
        s36_exact_absence_timeout_seconds=lambda: 1.0,
        logger=logging.getLogger(__name__),
        recovery_store=VMWorkspaceRecoveryStore(db),
    )

    result = await run_completion_workspace_teardown(
        state["job_id"], runner, dependencies=dependencies
    )
    replay = await run_completion_workspace_teardown(
        state["job_id"], runner, dependencies=dependencies
    )

    assert result == {"actions": ["vm released"], "teardown_disposition": "completed"}
    assert replay == result
    assert physical["candidate"]["retention_preflight"] == physical["proof"]
    archive.assert_not_awaited()
    provisioner.release_vm_captured.assert_awaited_once()
    provisioner.qualify_retained_ready_stop.assert_awaited_once()
    provisioner.attest_vm_cleanup_stop.assert_awaited_once()
    async with db.acquire() as conn:
        terminal = await conn.fetchrow(
            "SELECT terminal_kind,physical_cleanup_admission_id FROM "
            "vm_job_retained_resume_terminals WHERE resume_id=$1",
            state["resume"]["id"],
        )
        assert terminal["terminal_kind"] == "kept_compute"
        assert await conn.fetchval(
            "SELECT public.vm_job_cancel_retention_settled($1)",
            terminal["physical_cleanup_admission_id"],
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
