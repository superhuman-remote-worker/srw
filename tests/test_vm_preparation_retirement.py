"""Prepared work that never received a disk can retire without inventing a VM."""

import asyncio
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, Mock, call
from uuid import uuid4

import asyncpg
import pytest

from orchestrator.services.vm_provisioner import (
    VMProvisioner,
    VMTeardownIdentity,
    _VMTeardownProbe,
)
from tests.test_non_pinned_workspace_lifecycle_real_postgres import (
    _schema_applied,  # noqa: F401
    db as _postgres_db_fixture,
    pg_dsn,  # noqa: F401
)
from tests.test_vm_preparation_lifecycle import complete, engine, request
from vm_controller.workspace_preparation import allocation_name

db = _postgres_db_fixture


@pytest.mark.parametrize("workloads_fail", [False, True])
def test_prepared_gate_revokes_active_tokens_after_workload_cleanup(workloads_fail):
    path = (
        Path(__file__).resolve().parents[1]
        / "scripts/workspace-preparation-srw-k3d-gate.py"
    )
    spec = importlib.util.spec_from_file_location("preparation_gate_test", path)
    gate = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(gate)
    smoke = gate.PreparedSmoke.__new__(gate.PreparedSmoke)
    smoke.prefix, smoke.token = "cutover-123456abcdef-", "fixture-only-token"
    active_id = str(uuid4())
    revoked = {
        "name": smoke.prefix + "mcp",
        "id": str(uuid4()),
        "revoked_at": "2026-09-13T00:00:00Z",
    }
    active = {"name": smoke.prefix + "mcp", "id": active_id, "revoked_at": None}
    other = {"name": "different-owner", "id": str(uuid4()), "revoked_at": None}
    smoke.gate = SimpleNamespace(
        login=Mock(),
        request=Mock(
            side_effect=[
                [revoked, active, other],
                {"status": "revoked"},
                [revoked, {**active, "revoked_at": "2026-09-13T00:01:00Z"}, other],
            ]
        ),
    )
    smoke.evidence = {"cleanup": {}}
    smoke._cleanup_owned = Mock(
        side_effect=gate.GateFailure("fixture cleanup failed")
        if workloads_fail
        else None
    )
    if workloads_fail:
        with pytest.raises(gate.GateFailure, match="fixture cleanup failed"):
            smoke.cleanup()
    else:
        smoke.cleanup()
    assert smoke.gate.request.call_args_list == [
        call("GET", "/api/mcp-tokens"),
        call("DELETE", "/api/mcp-tokens/" + active_id),
        call("GET", "/api/mcp-tokens"),
    ]
    assert smoke.token is None
    assert smoke.evidence["cleanup"]["mcpTokenRevoked"] is True


@pytest.mark.asyncio
async def test_cancel_receipt_closes_preparation_before_any_workspace_source():
    service, value = engine(), request()
    await service.prepare(value)
    while not (result := await service.cancel_with_receipt(value))["cancelled"]:
        await service.reconcile()
    assert result["workspaceNeverIssued"] is True
    assert (await service.prepare(value))[0] is None
    assert (await service.cancel_with_receipt(value))["workspaceNeverIssued"] is True


@pytest.mark.asyncio
async def test_cancel_receipt_never_reclassifies_a_delivered_source():
    service, value = engine(), request()
    await complete(service, value)
    result = await service.cancel_with_receipt(value)
    assert result["cancelled"] is True
    assert result["workspaceNeverIssued"] is False
    assert (await service.cancel_with_receipt(value))["workspaceNeverIssued"] is False


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["failed", "ready", "missing", "replaced", "unproven"])
async def test_legacy_cancellation_uses_only_matching_terminal_builder_evidence(case):
    service, value = engine(), request()
    await complete(service, value, code=0 if case == "ready" else 17)
    allocation = service.store.data[allocation_name(value)]
    allocation.state.pop("workspace_source_issued")
    artifact = service.store.data[allocation.state["artifact"]]
    if case == "missing":
        del service.store.data[artifact.name]
    elif case == "replaced":
        allocation.state["artifact_uid"] = str(uuid4())
    elif case == "unproven":
        artifact.state.pop("receipt")
    result = await service.cancel_with_receipt(value)
    assert result["cancelled"] is True
    assert result["workspaceNeverIssued"] is (case == "failed")
    # Evidence survives artifact garbage collection only after it was proven.
    service.store.data.pop(artifact.name, None)
    assert (await service.cancel_with_receipt(value))["workspaceNeverIssued"] is (
        case == "failed"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("remaining", [None, "vm", "vmi", "launcher", "unknown"])
async def test_controller_serializes_cancellation_with_vm_creation_and_absence(
    remaining,
):
    from kubernetes.client.exceptions import ApiException
    from vm_controller.controller import (
        KUBEVIRT_PLURAL,
        KUBEVIRT_VMI_PLURAL,
        VMController,
    )

    value = request()
    controller = VMController.__new__(VMController)
    service = SimpleNamespace(
        cancel_with_receipt=AsyncMock(
            return_value={"cancelled": True, "workspaceNeverIssued": True}
        )
    )
    controller._workspace_preparation = lambda: service

    def observed(**kwargs):
        if remaining == "unknown":
            raise ApiException(status=503)
        if (remaining == "vm" and kwargs["plural"] == KUBEVIRT_PLURAL) or (
            remaining == "vmi" and kwargs["plural"] == KUBEVIRT_VMI_PLURAL
        ):
            return {"metadata": {"uid": str(uuid4())}}
        raise ApiException(status=404)

    controller.k8s_client = SimpleNamespace(
        get_namespaced_custom_object=MagicMock(side_effect=observed)
    )
    controller.core_api = SimpleNamespace(
        list_namespaced_pod=MagicMock(
            return_value=SimpleNamespace(
                items=[object()] if remaining == "launcher" else []
            )
        )
    )
    lock = controller._lifecycle_lock_for(value["allocationId"])
    async with lock:
        pending = asyncio.create_task(controller._cancel_preparation(value))
        await asyncio.sleep(0)
        service.cancel_with_receipt.assert_not_awaited()
    if remaining == "unknown":
        with pytest.raises(ApiException) as caught:
            await pending
        assert caught.value.status == 503
    else:
        result = await pending
        assert result["workspaceNeverIssued"] is (remaining is None)


@pytest.mark.asyncio
@pytest.mark.parametrize("owner_kind", ["job", "thread"])
@pytest.mark.parametrize(
    "cancelled,proof",
    [(True, True), (True, False), (True, None), (True, "true"), (False, True)],
)
async def test_unallocated_preparation_retirement_requires_positive_receipt(
    db, monkeypatch, owner_kind, cancelled, proof
):
    monkeypatch.setenv("VM_MODE", "same-cluster")
    owner = uuid4()
    generation = str(uuid4())
    from shared.workspace_preparation import preparation_request

    value = preparation_request(
        {"image": "registry.example/base:latest", "prepare": [{"command": ["false"]}]},
        scope_kind="Account",
        scope_uid=str(uuid4()),
        allocation_id=str(owner),
        owner_kind="job" if owner_kind == "job" else "session",
        runtime_generation=generation if owner_kind == "thread" else None,
    )
    vm = VMProvisioner._fresh_provision_ctx()
    vm.update(
        status="failed", provision_generation=generation, preparation_request=value
    )
    table, column = (
        ("jobs", "context") if owner_kind == "job" else ("threads", "metadata")
    )
    async with db.acquire() as conn:
        if owner_kind == "job":
            await conn.execute(
                "INSERT INTO jobs(id, description, status, context) VALUES($1, 'never allocated preparation', 'failed', $2::jsonb)",
                owner,
                json.dumps({"vm": vm}),
            )
        else:
            await conn.execute(
                "INSERT INTO threads(id, status, metadata) VALUES($1, 'ended', $2::jsonb)",
                owner,
                json.dumps({"vm": vm}),
            )
    provisioner = VMProvisioner()
    provisioner._db = db
    identity = VMTeardownIdentity(generation, None, None)
    # Absence completes only with whole-runtime proof: a VM 404 alone can race
    # KubeVirt VMI and virt-launcher deletion.
    provisioner._probe_vm_teardown_identity = AsyncMock(
        return_value=_VMTeardownProbe(
            "absent",
            identity,
            rootdisk_identity_known=True,
            runtime_absence_known=True,
            vmi_absent=True,
            launcher_absent=True,
        )
    )
    provisioner.preparation_operation = AsyncMock(
        return_value={"cancelled": cancelled, "workspaceNeverIssued": proof}
    )
    provisioner._delete_vm_with_identity = AsyncMock()
    outcome = await provisioner.release_vm_captured(
        str(owner), identity, entity_type=owner_kind
    )
    succeeded = cancelled is True and proof is True
    assert (outcome.disposition == "completed") is succeeded
    assert (
        await db.managed_repository_workspace_process_zero_is_current(
            str(owner),
            owner_kind=owner_kind,
            scope="vm",
            provisioner="vm",
            runtime_incarnation=generation,
        )
        is succeeded
    )
    provisioner._delete_vm_with_identity.assert_not_awaited()
    if succeeded:
        async with db.acquire() as conn:
            state = json.loads(
                await conn.fetchval(f"SELECT {column} FROM {table} WHERE id=$1", owner)
            )
            assert state["vm"]["status"] == "deleted"
            if owner_kind == "thread":
                # VM retirement does not replace the pinned Session's separate
                # permanent owner-retirement protocol.
                with pytest.raises(asyncpg.CheckViolationError, match="pinned thread"):
                    await conn.execute(f"DELETE FROM {table} WHERE id=$1", owner)
            else:
                assert (
                    await conn.execute(f"DELETE FROM {table} WHERE id=$1", owner)
                    == "DELETE 1"
                )
