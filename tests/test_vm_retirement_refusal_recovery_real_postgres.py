"""Immutable historical refusal plus normal retirement retry, using real SQL."""

import ast
import hashlib
from pathlib import Path
from dataclasses import replace
from unittest.mock import AsyncMock
from uuid import uuid4

import asyncio
import asyncpg

import pytest

from orchestrator.services.vm_provisioner import (
    VMProvisioner,
    VMTeardownIdentity,
    _VMTeardownProbe,
)
from tests.test_vm_retirement_teardown_order_real_postgres import (
    _base_db,  # noqa: F401
    _schema_applied,  # noqa: F401
    admitted_teardown,
    db as _db,
    pg_dsn,  # noqa: F401
    setup as _setup,
    thread_schema,  # noqa: F401
)
from tests.test_vm_thread_adopted_without_quotas_delete_real_postgres import (
    cleaned_retirement,
)
from orchestrator.services.vm_thread_cleanup_refusal_recovery import (
    recover_completed_thread_vm_refusal,
)
from orchestrator.services.vm_workspace_recovery_store import (
    completed_cleanup_outcome,
    acquire_pinned_thread_retirement_cleanup_permit,
)

db, setup = _db, _setup
FAILING_REVISION = "3f77c14835b2465cf139acd0ef8c6c9bec6ee00b"


def original_classifier():
    """Execute the exact old production function, not an invented refusal row."""
    source = (
        Path(__file__).with_name("fixtures") / "vm_cleanup_classifier_3f77c1483.py.txt"
    ).read_text()
    assert (
        hashlib.sha256(source.encode()).hexdigest()
        == "648fc607e8e2494a34e31528e907d44b09d21da73e60b68843d4e97f591d1913"
    )
    method = ast.parse(source).body[0]
    method.decorator_list = []
    namespace = {
        "VMTeardownIdentity": VMTeardownIdentity,
        "_VMTeardownProbe": _VMTeardownProbe,
    }
    exec(
        compile(
            ast.Module(body=[method], type_ignores=[]),
            "historical-production-classifier",
            "exec",
        ),
        namespace,
    )
    return namespace[method.name]


async def historical_refusal(db, setup, monkeypatch, *, settle_child=True):
    with monkeypatch.context() as old:
        old.setattr(
            VMProvisioner,
            "_classify_captured_probe",
            staticmethod(original_classifier()),
        )
        case = await admitted_teardown(db, setup, monkeypatch, disk_first=True)
    (
        current,
        source,
        retirement,
        identity,
        store,
        permit,
        provisioner,
        physical,
        child,
        result,
    ) = case
    assert result.disposition == "identity_superseded"
    original = dict(
        await db.fetchrow(
            "SELECT * FROM vm_workspace_cleanup_admissions WHERE id=$1",
            permit.admission_id,
        )
    )
    assert (
        original["outcome"] == "identity_superseded"
        and original["completed_at"] is not None
    )
    physical.update(vm=False, disk=False)
    if settle_child:
        assert await store.complete_cleanup_permit(
            child.admission_id, outcome="deleted"
        )
    permit = await acquire_pinned_thread_retirement_cleanup_permit(
        store,
        thread_id=current["id"],
        identity=identity,
        purge_disk=True,
    )
    assert completed_cleanup_outcome(permit) == "identity_superseded"
    case = (*case[:5], permit, *case[6:])
    return case, original


@pytest.mark.asyncio
async def test_recorded_refusal_recovers_once_without_rewriting_original_receipt(
    db, setup, monkeypatch
):
    case, original = await historical_refusal(db, setup, monkeypatch)
    current, source, _, _, _, permit, *_ = case
    retirement = await cleaned_retirement(db, current, permanent=True)
    # Normal repeated retirement must reuse the same authority, no arbitrary
    # request IDs, fabricated physical identity or mutable old completion.
    await cleaned_retirement(db, current, permanent=True)
    assert (
        dict(
            await db.fetchrow(
                "SELECT * FROM vm_workspace_cleanup_admissions WHERE id=$1",
                permit.admission_id,
            )
        )
        == original
    )

    receipt = await db.fetchrow(
        "SELECT * FROM vm_thread_cleanup_refusal_recoveries WHERE refused_admission_id=$1",
        permit.admission_id,
    )
    assert (
        receipt is not None and str(receipt["retirement_token"]) == retirement["token"]
    )
    successor = await db.fetchrow(
        "SELECT * FROM vm_workspace_cleanup_admissions WHERE id=$1",
        receipt["successor_admission_id"],
    )
    assert (
        successor["outcome"] == "completed"
        and successor["intent_digest"] == original["intent_digest"]
    )
    assert (
        successor["id"] != original["id"]
        and successor["request_id"] != original["request_id"]
    )
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_thread_cleanup_refusal_recoveries WHERE refused_admission_id=$1",
            permit.admission_id,
        )
        == 1
    )
    await db.delete_thread(
        str(current["id"]),
        expected_runtime_generation=retirement["generation"],
        expected_runtime_retirement_token=retirement["token"],
    )
    assert await db.get_thread(str(current["id"])) is None
    assert (
        dict(
            await db.fetchrow(
                "SELECT * FROM vm_creation_retries WHERE request_id=$1",
                source["request_id"],
            )
        )
        == source
    )
    assert (
        dict(
            await db.fetchrow(
                "SELECT * FROM vm_workspace_cleanup_admissions WHERE id=$1",
                permit.admission_id,
            )
        )
        == original
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault",
    [
        "disk_replacement",
        "vm_replacement",
        "writers",
        "unknown_runtime",
        "unknown_disk",
        "unreadable",
        "generation",
        "missing_zero",
        "retirement_token",
        "runtime_generation",
        "proof_owner",
        "proof_identity",
        "proof_authentication",
        "proof_absence",
        "proof_disk",
        "missing_child",
    ],
)
async def test_refusal_recovery_requires_exact_current_and_complete_proof(
    db, setup, monkeypatch, fault
):
    case, original = await historical_refusal(
        db, setup, monkeypatch, settle_child=fault != "missing_child"
    )
    current, _, retirement, identity, store, permit, provisioner, _, child, _ = case
    probe = _VMTeardownProbe(
        "absent",
        replace(identity, vm_uid=None, rootdisk_pvc_uid=None),
        rootdisk_identity_known=True,
        runtime_absence_known=True,
        vmi_absent=True,
        launcher_absent=True,
    )
    if fault == "disk_replacement":
        probe = replace(
            probe, identity=replace(probe.identity, rootdisk_pvc_uid=str(uuid4()))
        )
    elif fault == "vm_replacement":
        probe = replace(
            probe,
            disposition="present",
            identity=replace(identity, vm_uid=str(uuid4())),
        )
    elif fault in {"writers", "unknown_runtime"}:
        probe = replace(probe, runtime_absence_known=False, launcher_absent=False)
    elif fault == "unknown_disk":
        probe = replace(probe, rootdisk_identity_known=False)
    elif fault == "unreadable":
        probe = replace(probe, disposition="unknown", identity=None)
    elif fault == "generation":
        monkeypatch.setattr(
            provisioner,
            "_current_provision_generation",
            AsyncMock(return_value=str(uuid4())),
        )
    elif fault == "missing_zero":
        monkeypatch.setattr(
            db,
            "managed_repository_workspace_process_zero_is_current",
            AsyncMock(return_value=False),
        )
    elif fault in {"retirement_token", "runtime_generation"}:
        retirement = {
            **retirement,
            "token" if fault == "retirement_token" else "generation": str(uuid4()),
        }
    provisioner._probe_vm_teardown_identity = AsyncMock(return_value=probe)
    if fault.startswith("proof_"):
        candidate = {
            "owner_kind": "thread",
            "owner_id": str(current["id"]),
            "provision_generation": identity.provision_generation,
            "vm_uid": identity.vm_uid,
            "pvc_uid": identity.rootdisk_pvc_uid,
            "vmi_uid": retirement["context"]["vm"].get("vmi_uid"),
            "launcher_uid": retirement["context"]["vm"].get("active_pod_uid"),
            "purge_disk": True,
        }
        proof = await provisioner.attest_vm_cleanup_stop(candidate)
        assert proof is not None
        key, value = {
            "proof_owner": ("owner_id", str(uuid4())),
            "proof_identity": ("vm_uid", str(uuid4())),
            "proof_authentication": ("controller_authenticated", False),
            "proof_absence": ("launcher_absent", False),
            "proof_disk": ("pvc_disposition", "retained"),
        }[fault]
        proof[key] = value
        monkeypatch.setattr(
            provisioner, "attest_vm_cleanup_stop", AsyncMock(return_value=proof)
        )
    if fault in {"retirement_token", "runtime_generation"} or fault.startswith(
        "proof_"
    ):
        with pytest.raises(
            asyncpg.CheckViolationError, match="recovery authority unproven"
        ):
            await recover_completed_thread_vm_refusal(
                store, provisioner, permit, retirement
            )
    else:
        result = await recover_completed_thread_vm_refusal(
            store, provisioner, permit, retirement
        )
        assert completed_cleanup_outcome(result) == "identity_superseded"
    assert (
        dict(
            await db.fetchrow(
                "SELECT * FROM vm_workspace_cleanup_admissions WHERE id=$1",
                permit.admission_id,
            )
        )
        == original
    )
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_thread_cleanup_refusal_recoveries WHERE refused_admission_id=$1",
            permit.admission_id,
        )
        == 0
    )
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_workspace_cleanup_admissions WHERE owner_id=$1 AND source='pinned_thread_retirement'",
            current["id"],
        )
        == 1
    )


@pytest.mark.asyncio
async def test_concurrent_recovery_is_one_immutable_receipt_and_replay_revalidates(
    db, setup, monkeypatch
):
    case, original = await historical_refusal(db, setup, monkeypatch)
    current, _, retirement, identity, store, permit, provisioner, *_ = case
    results = await asyncio.gather(
        *[
            recover_completed_thread_vm_refusal(store, provisioner, permit, retirement)
            for _ in range(2)
        ]
    )
    assert all(completed_cleanup_outcome(v) == "completed" for v in results)
    assert results[0].admission_id == results[1].admission_id
    assert results[0].request_id == results[1].request_id
    for table, where, identifier in (
        ("vm_workspace_cleanup_admissions", "id", permit.admission_id),
        ("vm_workspace_cleanup_admissions", "id", results[0].admission_id),
        (
            "vm_thread_cleanup_refusal_recoveries",
            "refused_admission_id",
            permit.admission_id,
        ),
    ):
        with pytest.raises(asyncpg.CheckViolationError, match="immutable"):
            await db.execute(f"DELETE FROM {table} WHERE {where}=$1", identifier)
    with pytest.raises(asyncpg.CheckViolationError, match="immutable"):
        await db.execute(
            "UPDATE vm_workspace_cleanup_admissions SET outcome='completed' WHERE id=$1",
            permit.admission_id,
        )
    assert (
        dict(
            await db.fetchrow(
                "SELECT * FROM vm_workspace_cleanup_admissions WHERE id=$1",
                permit.admission_id,
            )
        )
        == original
    )
    # A receipt never substitutes for a fresh proof about a possible replacement.
    monkeypatch.setattr(
        provisioner, "attest_vm_cleanup_stop", AsyncMock(return_value=None)
    )
    result = await recover_completed_thread_vm_refusal(
        store, provisioner, permit, retirement
    )
    assert completed_cleanup_outcome(result) == "identity_superseded"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault",
    [
        "owner_generation",
        "owner_token",
        "source_actor",
        "source_generation",
        "current_vm",
        "current_disk",
        "current_request",
        "child_outcome",
        "child_parent",
        "missing_zero",
        "receipt_authentication",
        "receipt_writer",
    ],
)
async def test_durable_recovery_authority_refuses_contradictions(
    db, setup, monkeypatch, fault
):
    case, _ = await historical_refusal(db, setup, monkeypatch)
    current, source, retirement, _, store, permit, provisioner, _, child, _ = case
    result = await recover_completed_thread_vm_refusal(
        store, provisioner, permit, retirement
    )
    assert completed_cleanup_outcome(result) == "completed"
    # Normal production operations establish the whole history first. Only
    # rollback-only corruption in this disposable database bypasses triggers to
    # independently test the final predicate and its targeted mutations.
    async with db.acquire() as conn:
        tx = conn.transaction()
        await tx.start()
        try:
            await conn.execute("SET LOCAL session_replication_role='replica'")
            if fault in {"owner_generation", "owner_token"}:
                key = (
                    "runtime_generation"
                    if fault == "owner_generation"
                    else "runtime_retirement_token"
                )
                await conn.execute(
                    f"UPDATE threads SET {key}=$2 WHERE id=$1", current["id"], uuid4()
                )
            elif fault in {"source_actor", "source_generation"}:
                key = (
                    "thread_agent_id"
                    if fault == "source_actor"
                    else "provision_generation"
                )
                await conn.execute(
                    f"UPDATE vm_creation_retries SET {key}=$2 WHERE request_id=$1",
                    source["request_id"],
                    uuid4(),
                )
            elif fault in {"current_vm", "current_disk", "current_request"}:
                key = {
                    "current_vm": "vm_uid",
                    "current_disk": "rootdisk_pvc_uid",
                    "current_request": "creation_request_id",
                }[fault]
                await conn.execute(
                    "UPDATE threads SET metadata=jsonb_set(metadata,ARRAY['vm',$2],to_jsonb($3::text)) WHERE id=$1",
                    current["id"],
                    key,
                    str(uuid4()),
                )
            elif fault == "child_outcome":
                await conn.execute(
                    "UPDATE vm_workspace_cleanup_admissions SET outcome='recreated' WHERE id=$1",
                    child.admission_id,
                )
            elif fault == "child_parent":
                await conn.execute(
                    "UPDATE vm_workspace_cleanup_admissions SET parent_admission_id=NULL WHERE id=$1",
                    child.admission_id,
                )
            elif fault == "missing_zero":
                await conn.execute(
                    "DELETE FROM managed_repository_process_zero_receipts WHERE owner_kind='thread' AND owner_id=$1",
                    current["id"],
                )
            else:
                key = (
                    "controller_authenticated"
                    if fault == "receipt_authentication"
                    else "launcher_absent"
                )
                await conn.execute(
                    "UPDATE vm_thread_cleanup_refusal_recoveries SET physical_stop=jsonb_set(physical_stop,ARRAY[$2],'false') WHERE refused_admission_id=$1",
                    permit.admission_id,
                    key,
                )
            with pytest.raises(
                asyncpg.CheckViolationError, match="recovery authority unproven"
            ):
                await conn.fetchval(
                    "SELECT public.validate_vm_thread_cleanup_refusal_recovery(r) FROM vm_thread_cleanup_refusal_recoveries r WHERE refused_admission_id=$1",
                    permit.admission_id,
                )
        finally:
            await tx.rollback()
