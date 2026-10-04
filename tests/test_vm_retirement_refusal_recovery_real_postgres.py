"""Immutable historical refusal plus normal retirement retry, using real SQL."""

import ast
import subprocess
from pathlib import Path

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

db, setup = _db, _setup
FAILING_REVISION = "3f77c14835b2465cf139acd0ef8c6c9bec6ee00b"


def original_classifier():
    """Execute the exact old production function, not an invented refusal row."""
    source = subprocess.check_output(
        [
            "git",
            "show",
            FAILING_REVISION + ":src/orchestrator/services/vm_provisioner.py",
        ],
        cwd=Path(__file__).resolve().parents[1],
        text=True,
    )
    owner = next(
        v
        for v in ast.parse(source).body
        if isinstance(v, ast.ClassDef) and v.name == "VMProvisioner"
    )
    method = next(
        v
        for v in owner.body
        if isinstance(v, ast.FunctionDef) and v.name == "_classify_captured_probe"
    )
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


async def historical_refusal(db, setup, monkeypatch):
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
    assert await store.complete_cleanup_permit(child.admission_id, outcome="deleted")
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
