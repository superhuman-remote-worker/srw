"""Boundaries for the disposable retained-disk Resume acceptance adapter."""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
import io
import logging
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
@pytest.mark.parametrize("missing", [None, "worker", "sentinel"])
async def test_execute_proves_worker_sentinel_before_terminal_vm_teardown(
    monkeypatch, missing,
) -> None:
    job_id, owner_id, pvc_uid, old_gen, old_vm_uid, new_gen, new_vm_uid, request_id = (
        str(uuid4()) for _ in range(8)
    )
    scenario = gate.LiveScenario.__new__(gate.LiveScenario)
    scenario.args = SimpleNamespace(
        job_id=job_id, expected_owner_id=owner_id, expected_pvc_uid=pvc_uid,
        run_id="srw-a1-owned-20260923", cluster_uid=str(uuid4()), namespace="srw",
    )
    before = {
        "request_id": request_id, "job_id": job_id, "provision_generation": new_gen,
        "origin": "resume", "expected_pvc_uid": pvc_uid,
        "admission_deadline": datetime.now(timezone.utc) + timedelta(minutes=10),
        "canonical_request": {"job_id": job_id}, "request_digest": "sha256:" + "a" * 64,
        "controller_configuration_digest": "sha256:" + "b" * 64,
        "execution_id": str(uuid4()), "execution_revision": "sha256:" + "c" * 64,
        "execution_generation": str(uuid4()),
    }
    after = {**before, "state": "succeeded", "observed_pvc_uid": pvc_uid,
             "observed_vm_uid": new_vm_uid, "ready_at": datetime.now(timezone.utc)}
    storage = {"pvc_uid": pvc_uid, "pv_uid": str(uuid4())}
    scenario.fixture_snapshot = AsyncMock(return_value=(
        {"context": {"vm": {"provision_generation": old_gen}}},
        {"provision_generation": old_gen, "observed_vm_uid": old_vm_uid,
         "request_id": str(uuid4())},
    ))
    scenario.revisions = AsyncMock(return_value={})
    scenario.ready_identity = AsyncMock(side_effect=[
        {"vm_uid": old_vm_uid}, {"vm_uid": new_vm_uid},
    ])
    scenario.pvc_pv_identity = AsyncMock(return_value=storage)
    scenario.sentinel_write = AsyncMock(return_value="a" * 64)
    scenario.retire_predecessor = AsyncMock(return_value={"cleanup_admission_id": str(uuid4())})
    scenario.exclusive_vm_creation = AsyncMock()
    scenario.begin_replacement = AsyncMock(return_value=before)
    scenario.rejected_vm_effect = AsyncMock(return_value=True)
    scenario.owner_resume = AsyncMock(return_value={"vm_creation_retry_request_id": request_id})
    scenario.retry_row = AsyncMock(side_effect=[before, after])
    worker_name = "a1-worker-owned"
    authorized_at = datetime.now(timezone.utc)
    worker = {
        "queue": {"lease_token": 1},
        "attempt": {"authority_digest": "sha256:" + "d" * 64,
                    "bundle_authorized_at": authorized_at},
        "pod_name": worker_name, "pod_uid": str(uuid4()),
    }
    scenario.capture_worker = AsyncMock(return_value=worker)
    state = {"barrier": False, "vm_deleted": False, "reads": 0, "terminal": False}

    async def read_sentinel(*_):
        state["reads"] += 1
        if state["vm_deleted"] or (missing == "sentinel" and state["reads"] == 2):
            raise gate.AcceptanceFailure("pinned sentinel unavailable")

    async def completion_row(query, *_):
        assert state["barrier"] is True
        if "FROM run_queue" in query:
            return {"lease_token": 1, "last_leased_by": worker_name, "state": "done"}
        if "FROM jobs" in query:
            return {"status": "completed", "completed_at": authorized_at}
        if "FROM worker_batch_attempts" in query:
            return {"authority_digest": worker["attempt"]["authority_digest"],
                    "bundle_authorized_at": authorized_at, "refunded_at": None}
        if "FROM job_completion_commands" in query:
            # Exercise the real S36 adapter with its exact purge intent. Its
            # external VM transport is the test's deleted-guest observation.
            from orchestrator.services.completion_effects import (
                CompletionEffectDependencies, run_completion_workspace_teardown,
            )
            from orchestrator.services.vm_provisioner import VMTeardownResult
            from orchestrator.services.vm_workspace_recovery_store import CleanupPermit

            class Runner:
                command_id = str(uuid4())

                async def authorize_workspace_teardown(self):
                    return SimpleNamespace(authorized=True)

                async def capture_intent(self, *_):
                    return {
                        "kind": "vm", "provision_generation": new_gen,
                        "vm_uid": new_vm_uid, "rootdisk_pvc_uid": pvc_uid,
                        "ssh_host_key_fingerprint": "SHA256:" + "A" * 43,
                    }

                async def run(self, *, callback, **_):
                    return await callback()

            async def release_vm(*_, **kwargs):
                assert kwargs["purge_disk"] is True
                state["vm_deleted"] = True
                return VMTeardownResult("completed", True)

            async def uncharged_source(query, *args):
                assert "FROM vm_creation_retries" in query
                assert tuple(map(str, args)) == (job_id, new_gen)
                return {"controller_configuration": {"version": 1}}

            async def no_retained_disk_hold(query, *args):
                if query == "SELECT to_regclass('public.vm_idle_operations') IS NOT NULL":
                    return True
                assert "storage_disposition='retention_unknown'" in query
                assert tuple(map(str, args)) == (job_id, new_gen, pvc_uid)
                return None

            @asynccontextmanager
            async def transaction():
                yield

            connection = SimpleNamespace(
                fetchrow=AsyncMock(side_effect=uncharged_source),
                fetchval=AsyncMock(side_effect=no_retained_disk_hold),
                transaction=transaction,
            )

            @asynccontextmanager
            async def acquire():
                yield connection

            cleanup = SimpleNamespace(
                db=SimpleNamespace(acquire=acquire),
                acquire_cleanup_permit=AsyncMock(return_value=CleanupPermit(
                    allowed=True, admission_id=uuid4(),
                )),
                complete_cleanup_permit=AsyncMock(),
            )
            dependencies = CompletionEffectDependencies(
                store=SimpleNamespace(get_job=AsyncMock(return_value={}), acquire=acquire),
                container_provisioner=SimpleNamespace(),
                vm_provisioner=SimpleNamespace(release_vm_captured=release_vm),
                get_container_context=lambda _: {}, get_vm_context=lambda _: {},
                archive_and_cleanup_workspace=AsyncMock(return_value=[]),
                s36_exact_absence_timeout_seconds=lambda: 5,
                logger=logging.getLogger(__name__), recovery_store=cleanup,
            )
            teardown = await run_completion_workspace_teardown(
                job_id, Runner(), dependencies=dependencies,
            )
            assert teardown["teardown_disposition"] == "completed"
            cleanup.complete_cleanup_permit.assert_awaited_once()
            assert connection.fetchrow.await_count == 2
            assert connection.fetchval.await_count == 2
            state["terminal"] = True
            return {"id": str(uuid4()), "state": "done", "outcome": {},
                    "finalized_at": authorized_at, "accepted_lease_token": 1}
        raise AssertionError("unexpected terminal projection")

    async def wait(_, probe, *, seconds):
        value = await probe()
        if not value:
            raise gate.AcceptanceFailure("missing actual worker")
        return value

    scenario.sentinel_verify = AsyncMock(side_effect=read_sentinel)
    scenario.row = AsyncMock(side_effect=completion_row)
    scenario.wait = wait
    if missing == "worker":
        scenario.capture_worker.return_value = None
    stages = []

    def exchange(stage, _):
        stages.append(stage)
        if stage == "PROVIDER_BARRIER":
            state["barrier"] = True

    monkeypatch.setattr(gate, "host_exchange", exchange)
    if missing is None:
        result = await scenario.execute()
        assert result["outcome"] == "passed"
        assert result["assertions"]["pinned_ssh_sentinel_pre_terminal_worker"]
        assert result["assertions"]["normal_terminal_cleanup_allowed"]
        assert state == {"barrier": True, "vm_deleted": True, "reads": 2,
                         "terminal": True}
        assert stages[-2:] == ["PROVIDER_BARRIER", "PROVIDER_VERIFY"]
    else:
        with pytest.raises(gate.AcceptanceFailure):
            await scenario.execute()
        assert "PROVIDER_BARRIER" not in stages
        assert state["terminal"] is False


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
