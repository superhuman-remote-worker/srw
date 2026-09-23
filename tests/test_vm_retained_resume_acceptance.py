"""Boundaries for the disposable retained-disk Resume acceptance adapter."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
import io
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from orchestrator.operator_cli import vm_retained_resume_acceptance as gate


def test_execution_guard_requires_exact_disposable_context(tmp_path: Path) -> None:
    job_id, pvc_uid, cluster_uid = (str(uuid4()) for _ in range(3))
    values = dict(
        run_id="srw-a1-owned-20260923", job_id=job_id,
        expected_owner_id=str(uuid4()), expected_pvc_uid=pvc_uid,
        cluster_uid=cluster_uid, namespace="srw-a1-owned", context="srw-a1-owned",
        confirm=gate.CONFIRMATION, protocol_version=1,
        output=gate.OUTPUT_ROOT / "srw-a1-owned-20260923" / "result.json",
    )
    env = {"VM_RETAINED_RESUME_ACCEPTANCE_GATE_ENABLED": "true"}
    gate.require_execution_guard(values, env)
    for key, invalid in (
        ("run_id", "../other"), ("job_id", str(uuid4())),
        ("namespace", "production"), ("context", "shared"),
        ("protocol_version", 2), ("confirm", "yes"),
        ("output", tmp_path / "result.json"),
    ):
        changed = dict(values, **{key: invalid})
        if key == "job_id":
            changed["job_id"] = "bad-id"
        with pytest.raises(gate.AcceptanceFailure):
            gate.require_execution_guard(changed, env)
    with pytest.raises(gate.AcceptanceFailure):
        gate.require_execution_guard(values, {})


def test_fixture_snapshot_refuses_historical_or_leased_job() -> None:
    job_id, owner, pvc_uid, gen = (str(uuid4()) for _ in range(4))
    now = datetime.now(timezone.utc)
    job = {
        "id": job_id, "user_id": owner, "status": "paused",
        "execution_lane": "stateless", "assigned_agent_id": None,
        "created_at": now, "context": {"vm_retained_resume_acceptance_gate": "srw-a1-owned-20260923", "vm": {
            "status": "ready", "provision_generation": gen,
            "rootdisk_pvc_uid": pvc_uid,
        }},
    }
    user = {"id": owner, "display_name": "A1 retained Resume gate srw-a1-owned-20260923",
            "is_approved": True, "is_admin": False, "can_use_vm": True}
    queue = {"state": "done", "leased_by": None, "lease_token": 0}
    gate.validate_fixture_snapshot(job, user, queue, run_id="srw-a1-owned-20260923",
                                   expected_owner_id=owner,
                                   expected_pvc_uid=pvc_uid, now=now)
    for changed_job, changed_queue in (
        ({**job, "created_at": now - timedelta(days=2)}, queue),
        ({**job, "context": {}}, queue),
        (job, {**queue, "state": "leased", "leased_by": "worker-x"}),
        (job, {**queue, "lease_token": 1}),
    ):
        with pytest.raises(gate.AcceptanceFailure):
            gate.validate_fixture_snapshot(changed_job, user, changed_queue,
                                           run_id="srw-a1-owned-20260923",
                                           expected_owner_id=owner,
                                           expected_pvc_uid=pvc_uid, now=now)


def test_retry_snapshot_exact_immutable_equality_and_retained_pvc() -> None:
    request_id, job_id, gen, pvc_uid = (str(uuid4()) for _ in range(4))
    before = {"request_id": request_id, "job_id": job_id,
              "provision_generation": gen, "expected_pvc_uid": pvc_uid,
              "canonical_request": {"job_id": job_id, "provision_generation": gen},
              "request_digest": "sha256:" + "a" * 64,
              "controller_configuration_digest": "sha256:" + "b" * 64,
              "execution_revision": "sha256:" + "c" * 64,
              "admission_deadline": datetime.now(timezone.utc) + timedelta(minutes=15)}
    after = dict(before, state="succeeded", observed_pvc_uid=pvc_uid,
                 observed_vm_uid=str(uuid4()), ready_at=datetime.now(timezone.utc))
    gate.validate_retained_retry(before, after, job_id=job_id,
                                 expected_pvc_uid=pvc_uid)
    with pytest.raises(gate.AcceptanceFailure):
        gate.validate_retained_retry(before, {**after, "request_digest": "sha256:" + "d" * 64},
                                     job_id=job_id, expected_pvc_uid=pvc_uid)
    with pytest.raises(gate.AcceptanceFailure):
        gate.validate_retained_retry(before, {**after, "observed_pvc_uid": str(uuid4())},
                                     job_id=job_id, expected_pvc_uid=pvc_uid)


def test_worker_evidence_rejects_synthetic_lease_or_missing_pod() -> None:
    job_id = str(uuid4())
    queue = {"unit_id": job_id, "unit_kind": "worker_batch", "state": "done",
             "lease_token": 7, "last_leased_by": "worker-owned", "leased_by": None}
    attempt = {"job_id": job_id, "lease_token": 7, "claimed_attempt": 1,
               "bundle_authorized_at": datetime.now(timezone.utc),
               "authority_digest": "sha256:" + "a" * 64}
    pod = {"metadata": {"name": "worker-owned", "uid": str(uuid4()),
                        "labels": {"srw/class": "agent-stateless",
                                   "app.kubernetes.io/component": "agent-stateless"}},
           "status": {"phase": "Running"}}
    gate.validate_worker_evidence(queue, attempt, pod, job_id=job_id,
                                  run_id="srw-a1-owned-20260923")
    for bad_attempt, bad_pod in (
        ({**attempt, "bundle_authorized_at": None}, pod),
        ({**attempt, "authority_digest": None}, pod),
        (attempt, {**pod, "metadata": {**pod["metadata"], "uid": ""}}),
        (attempt, {**pod, "metadata": {**pod["metadata"], "name": "other"}}),
    ):
        with pytest.raises(gate.AcceptanceFailure):
            gate.validate_worker_evidence(queue, bad_attempt, bad_pod,
                                          job_id=job_id, run_id="srw-a1-owned-20260923")


def test_host_barrier_refuses_missing_or_wrong_ack(monkeypatch, capsys) -> None:
    monkeypatch.setattr(gate.sys, "stdin", io.StringIO("SRW_A1_ACK:ARM\n"))
    gate.host_exchange("ARM", "a" * 64)
    assert capsys.readouterr().out == "SRW_A1_ARM:" + "a" * 64 + "\n"
    monkeypatch.setattr(gate.sys, "stdin", io.StringIO("SRW_A1_ACK:QUOTA_RELEASE\n"))
    with pytest.raises(gate.AcceptanceFailure):
        gate.host_exchange("ARM", "a" * 64)


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [200, 409])
async def test_owner_resume_uses_exact_http_route_and_revokes_token(monkeypatch, status) -> None:
    import httpx

    job_id, owner_id, token_id = (str(uuid4()) for _ in range(3))
    scenario = gate.LiveScenario.__new__(gate.LiveScenario)
    scenario.args = SimpleNamespace(job_id=job_id, expected_owner_id=owner_id,
                                    run_id="srw-a1-owned-20260923")
    scenario.value = AsyncMock(return_value=owner_id)
    scenario.db = SimpleNamespace(
        create_mcp_token=AsyncMock(return_value={"id": token_id}),
        revoke_mcp_token=AsyncMock(return_value=True),
    )
    calls = []

    class Client:
        def __init__(self, **_):
            pass
        async def __aenter__(self):
            return self
        async def __aexit__(self, *_):
            return None
        async def post(self, url, *, headers, json):
            calls.append((url, headers, json))
            return SimpleNamespace(status_code=status, json=lambda: {"vm_creation_retry_request_id": token_id})

    monkeypatch.setattr(httpx, "AsyncClient", Client)
    result = await scenario.owner_resume(expected_status=status)
    assert calls[0][0].endswith(f"/api/jobs/{job_id}/resume")
    assert calls[0][1]["Authorization"].startswith("Bearer srw_")
    assert calls[0][2] == {}
    assert result == ({"vm_creation_retry_request_id": token_id} if status == 200 else {})
    scenario.db.create_mcp_token.assert_awaited_once()
    scenario.db.revoke_mcp_token.assert_awaited_once_with(token_id, owner_id)


@pytest.mark.asyncio
async def test_cleanup_only_requests_owner_delete_and_reports_pending_settlement(monkeypatch) -> None:
    import httpx

    job_id, owner_id, pvc_uid, token_id = (str(uuid4()) for _ in range(4))
    scenario = gate.LiveScenario.__new__(gate.LiveScenario)
    scenario.args = SimpleNamespace(job_id=job_id, expected_owner_id=owner_id,
                                    expected_pvc_uid=pvc_uid,
                                    run_id="srw-a1-owned-20260923")
    rows = [
        {"id": job_id, "user_id": owner_id, "status": "completed",
         "execution_lane": "stateless", "context": {
             "vm_retained_resume_acceptance_gate": scenario.args.run_id,
             "vm": {"rootdisk_pvc_uid": pvc_uid},
         }},
        {"state": "done", "leased_by": None},
        {"id": owner_id, "display_name": "A1 retained Resume gate srw-a1-owned-20260923",
         "is_approved": True, "is_admin": False},
    ]
    scenario.row = AsyncMock(side_effect=rows)
    scenario.value = AsyncMock(return_value=False)
    scenario.db = SimpleNamespace(
        create_mcp_token=AsyncMock(return_value={"id": token_id}),
        revoke_mcp_token=AsyncMock(return_value=True),
    )
    calls = []

    class Client:
        def __init__(self, **_):
            pass
        async def __aenter__(self):
            return self
        async def __aexit__(self, *_):
            return None
        async def delete(self, url, *, headers):
            calls.append((url, headers))
            return SimpleNamespace(status_code=200)

    monkeypatch.setattr(httpx, "AsyncClient", Client)
    result = await scenario.cleanup_only()
    assert calls[0][0].endswith(f"/api/jobs/{job_id}")
    assert calls[0][1]["Authorization"].startswith("Bearer srw_")
    assert result["outcome"] == "cleanup_requested"
    assert result["physical_settlement"] == "pending_production_authority"
    scenario.db.revoke_mcp_token.assert_awaited_once_with(token_id, owner_id)


@pytest.mark.asyncio
async def test_cleanup_only_holds_unresolved_replacement_before_owner_delete() -> None:
    job_id, owner_id, pvc_uid = (str(uuid4()) for _ in range(3))
    scenario = gate.LiveScenario.__new__(gate.LiveScenario)
    scenario.args = SimpleNamespace(job_id=job_id, expected_owner_id=owner_id,
                                    expected_pvc_uid=pvc_uid,
                                    run_id="srw-a1-owned-20260923")
    scenario.row = AsyncMock(side_effect=[
        {"id": job_id, "user_id": owner_id, "status": "paused",
         "execution_lane": "stateless", "context": {
             "vm_retained_resume_acceptance_gate": scenario.args.run_id,
             "vm": {"rootdisk_pvc_uid": pvc_uid},
         }},
        {"state": "done", "leased_by": None},
    ])
    scenario.value = AsyncMock(side_effect=[False, True])
    scenario.db = SimpleNamespace(create_mcp_token=AsyncMock())
    with pytest.raises(gate.AcceptanceFailure, match="unresolved creation retry"):
        await scenario.cleanup_only()
    scenario.db.create_mcp_token.assert_not_awaited()
