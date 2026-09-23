from __future__ import annotations

from orchestrator.services import job_dispatcher
from orchestrator.services.workspace_lifecycle import EnsureOutcome, WorkspaceOwner
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from tests import _b09_control_seams as control_seams

from shared.workspace_contract import (
    LEGACY_K8S_RUNTIME_ADOPTION_KEY,
    WORKSPACE_CONTRACT_CONTEXT_KEY,
    WorkspaceContractError,
    normalize_workspace_backend,
    resolve_workspace_contract,
    resolve_workspace_runtime,
    strip_and_stamp_workspace_creation,
    validate_worker_workspace_projection,
    vm_mode_from_env,
    workspace_contract_projection,
)


HISTORICAL_K8S_JOB_RUNTIME = {
    "status": "ready",
    "provisioner": "k8s",
    "pod_ip": "10.42.1.17",
    "port": 30022,
    "host": "workspace-job.internal",
}


@pytest.mark.parametrize(
    ("configured", "expected"),
    [
        (None, "off"),
        (" SAME-CLUSTER ", "same-cluster"),
        ("off", "off"),
        ("invalid", "external"),
    ],
)
def test_vm_mode_from_env_normalizes_without_changing_pure_resolver(
    monkeypatch, configured, expected
):
    if configured is None:
        monkeypatch.delenv("VM_MODE", raising=False)
    else:
        monkeypatch.setenv("VM_MODE", configured)
    assert vm_mode_from_env() == expected


def _runtime_attestation(
    *,
    runtime_incarnation: str | None = None,
    host: str = "workspace-job.internal",
    pod_ip: str = "10.42.1.17",
):
    from orchestrator.services.container_provisioner import WorkspaceRuntimeAttestation

    backing = str(uuid4())
    return WorkspaceRuntimeAttestation(
        backing_id=f"k8s-pvc:test:{backing}",
        workspace_generation=backing,
        runtime_incarnation=runtime_incarnation or str(uuid4()),
        ssh_host_key_fingerprint="SHA256:" + ("a" * 43),
        host=host,
        pod_ip=pod_ip,
        port=30022,
    )


def _adopted_k8s_runtime(attestation, *, status: str = "ready") -> dict:
    return {
        "status": status,
        "provisioner": "k8s",
        "host": attestation.host,
        "pod_ip": attestation.pod_ip,
        "port": attestation.port,
        "_runtime_incarnation": attestation.runtime_incarnation,
        LEGACY_K8S_RUNTIME_ADOPTION_KEY: {
            "version": 1,
            "runtime_incarnation": attestation.runtime_incarnation,
            "workspace_generation": attestation.workspace_generation,
            "ssh_host_key_fingerprint": attestation.ssh_host_key_fingerprint,
        },
    }


class _LegacyAdoptionDB:
    """The owner rows plus the exact slice of the 0198 ledger adoption uses.

    Adoption has no authorised transition into the new protocol without a
    durable `adopt` generation, so a double that omitted the ledger would let
    the bridge look like it still writes the owner row on its own.
    """

    def __init__(self, *jobs: dict) -> None:
        self.jobs = {str(job["id"]): deepcopy(job) for job in jobs}
        self.adoption_calls: list[dict] = []
        self.before_cas = None
        self.reservations: dict[str, dict] = {}
        self.reservation_calls: list[dict] = []
        self._next_generation = 1

    async def get_job(self, job_id: str):
        job = self.jobs.get(str(job_id))
        return deepcopy(job) if job is not None else None

    def _key(self, owner_id, owner_kind, scope):
        return (str(owner_id), owner_kind, scope)

    async def reserve_managed_repository_workspace_creation(
        self,
        owner_id: str,
        *,
        owner_kind: str,
        scope: str,
        claimant: str,
        operation_kind: str = "create",
        desired_manifest_digest: str,
        lease_seconds: int = 1800,
    ):
        self.reservation_calls.append(
            {
                "owner_id": str(owner_id),
                "owner_kind": owner_kind,
                "scope": scope,
                "operation_kind": operation_kind,
                "digest": desired_manifest_digest,
            }
        )
        job = self.jobs.get(str(owner_id))
        if job is None:
            return None
        workspace = (job.get("context") or {}).get("workspace_container") or {}
        # The production reserve refuses `adopt` over anything but the exact
        # historical shape; reproduce that here so a test can never adopt a row
        # that already carries authority.
        if operation_kind == "adopt" and not (
            scope == "workspace_container"
            and workspace
            and workspace.get("provisioner") == "k8s"
            and str(workspace.get("status") or "") == "ready"
            and "_runtime_incarnation" not in workspace
            and "_creation_reservation_id" not in workspace
            and "_creation_claim_token" not in workspace
        ):
            return None
        key = self._key(owner_id, owner_kind, scope)
        existing = self.reservations.get(key)
        if existing is not None and existing.get("settled_at") is None:
            if (
                existing["claimed_by"] != claimant
                or existing["operation_kind"] != operation_kind
                or existing["desired_manifest_digest"] != desired_manifest_digest
            ):
                return None
            return deepcopy(existing)
        record = {
            "id": f"reservation-{self._next_generation}",
            "reservation_generation": self._next_generation,
            "claim_token": self._next_generation,
            "claimed_by": claimant,
            "operation_kind": operation_kind,
            "desired_manifest_digest": desired_manifest_digest,
            "phase": "reserved",
            "runtime_incarnation": None,
            "settled_at": None,
            "external_mutation_started_at": None,
        }
        self._next_generation += 1
        self.reservations[key] = record
        return deepcopy(record)

    async def authorize_managed_repository_workspace_creation_runtime(
        self,
        owner_id: str,
        *,
        owner_kind: str,
        scope: str,
        reservation_generation: int,
        claimant: str,
        claim_token: int,
        runtime_incarnation: str,
    ) -> bool:
        record = self.reservations.get(self._key(owner_id, owner_kind, scope))
        if record is None or record["settled_at"] is not None:
            return False
        if (
            record["reservation_generation"] != reservation_generation
            or record["claim_token"] != claim_token
            or record["claimed_by"] != claimant
        ):
            return False
        if record["runtime_incarnation"] not in (None, runtime_incarnation):
            return False
        record["runtime_incarnation"] = runtime_incarnation
        record["phase"] = "runtime_bound"
        return True

    async def settle_managed_repository_workspace_creation_reservation(
        self,
        owner_id: str,
        *,
        owner_kind: str,
        scope: str,
        reservation_generation: int,
        claimant: str,
        claim_token: int,
        runtime_incarnation: str | None,
    ) -> bool:
        record = self.reservations.get(self._key(owner_id, owner_kind, scope))
        if record is None or record["settled_at"] is not None:
            return False
        if record["runtime_incarnation"] != runtime_incarnation:
            return False
        record["settled_at"] = "settled"
        record["phase"] = "settled"
        record["result_kind"] = "settled"
        return True

    async def abort_managed_repository_workspace_creation_reservation(
        self,
        owner_id: str,
        *,
        owner_kind: str,
        scope: str,
        reservation_generation: int,
        claimant: str,
        claim_token: int,
    ) -> bool:
        record = self.reservations.get(self._key(owner_id, owner_kind, scope))
        if record is None or record["settled_at"] is not None:
            return False
        job = self.jobs.get(str(owner_id))
        workspace = ((job or {}).get("context") or {}).get("workspace_container") or {}
        if workspace.get("_creation_reservation_id") == record["id"]:
            return False
        record["settled_at"] = "aborted"
        record["phase"] = "aborted"
        record["result_kind"] = "aborted"
        return True

    async def adopt_legacy_k8s_job_workspace_runtime(
        self, job_id: str, **kwargs
    ) -> bool:
        self.adoption_calls.append(deepcopy(kwargs))
        if self.before_cas is not None:
            self.before_cas(self.jobs[str(job_id)])
        current = self.jobs.get(str(job_id))
        if current is None:
            return False
        context = current.get("context") or {}
        config = current.get("config_override") or {}
        snapshot_matches = (
            str(current.get("status") or "") == kwargs["expected_status"]
            and current.get("execution_lane") == kwargs["expected_execution_lane"]
            and (
                str(current["parent_job_id"]) if current.get("parent_job_id") else None
            )
            == kwargs["expected_parent_job_id"]
            and context.get(WORKSPACE_CONTRACT_CONTEXT_KEY)
            == kwargs["expected_contract"]
            and context.get("workspace_backend") == kwargs["expected_legacy_backend"]
            and config.get("workspace") == kwargs["expected_workspace_config"]
            and context.get("workspace_container") == kwargs["expected_workspace"]
        )
        if not snapshot_matches:
            return False
        current["context"] = {
            **context,
            "workspace_container": deepcopy(kwargs["adopted_workspace"]),
        }
        return True


def _stamped_job(
    backend: str,
    *,
    requested: str | None = None,
    vm: dict | None = None,
    container: dict | None = None,
) -> dict:
    context: dict = {
        WORKSPACE_CONTRACT_CONTEXT_KEY: {
            "version": 1,
            "requested_backend": requested,
            "assigned_backend": backend,
            "assignment_source": "test",
        }
    }
    if vm is not None:
        context["vm"] = vm
    if container is not None:
        context["workspace_container"] = container
    return {
        "config_override": {"workspace": {"backend": backend}},
        "context": context,
    }


READY_VM = {
    "status": "ready",
    "ssh_host": "private-vm.internal",
    "ssh_port": 22,
    "provision_generation": "11111111-1111-4111-8111-111111111111",
}
READY_SANDBOX = {
    "status": "ready",
    "host": "workspace.private.svc",
    "port": 30022,
    "_runtime_incarnation": "22222222-2222-4222-8222-222222222222",
}


@pytest.mark.parametrize(
    ("value", "expected"),
    [("container", "sandbox"), ("remote", "vm"), (" VM ", "vm")],
)
def test_legacy_backend_aliases_normalize_once(value: str, expected: str) -> None:
    assert normalize_workspace_backend(value) == expected


def test_common_creation_boundary_strips_forged_runtime_authority() -> None:
    forged_context = {
        WORKSPACE_CONTRACT_CONTEXT_KEY: {
            "version": 99,
            "assigned_backend": "sandbox",
        },
        "workspace_runtime": {"effective_backend": "sandbox"},
        "workspace_backend": "sandbox",
        "vm": READY_VM,
        "workspace_container": READY_SANDBOX,
        "ordinary": "kept",
    }
    context, config, contract = strip_and_stamp_workspace_creation(
        forged_context,
        {
            "workspace": {
                "backend": "remote",
                "remote": {
                    "host": "caller.invalid",
                    "key_path": "/caller/key",
                },
            }
        },
        requested_backend="remote",
        assignment_source="request",
    )

    assert context == {
        "ordinary": "kept",
        WORKSPACE_CONTRACT_CONTEXT_KEY: {
            "version": 1,
            "requested_backend": "vm",
            "assigned_backend": "vm",
            "assignment_source": "request",
        },
    }
    assert config["workspace"] == {"backend": "vm"}
    assert contract.requested_backend == contract.assigned_backend == "vm"


def test_http_job_model_strips_workspace_authority_at_parse_boundary() -> None:
    from orchestrator.main import JobCreate

    body = JobCreate(
        description="caller cannot attest its own workspace",
        context={
            "_workspace_contract": {"assigned_backend": "sandbox"},
            "workspace_runtime": {"effective_backend": "sandbox"},
            "workspace_backend": "sandbox",
            "workspace_container": READY_SANDBOX,
            "vm": READY_VM,
            "ordinary": "kept",
        },
    )

    assert body.context == {"ordinary": "kept"}


def test_vm_assignment_does_not_accept_ready_sandbox() -> None:
    decision = resolve_workspace_runtime(
        _stamped_job("vm", requested="vm", container=READY_SANDBOX)
    )
    assert decision.state == "mismatch"
    assert decision.effective_backend is None
    assert decision.stale_backend == "sandbox"


def test_vm_assignment_uses_vm_and_reports_stale_sandbox() -> None:
    decision = resolve_workspace_runtime(
        _stamped_job("vm", requested="vm", vm=READY_VM, container=READY_SANDBOX)
    )
    assert decision.ready
    assert decision.effective_backend == "vm"
    assert decision.selected_context_key == "vm"
    assert decision.stale_backend == "sandbox"


def test_same_cluster_vm_requires_provisioner_probe_attestation() -> None:
    job = _stamped_job("vm", requested="vm", vm=READY_VM)
    unattested = resolve_workspace_runtime(job, vm_mode="same-cluster")
    assert unattested.state == "invalid"
    assert unattested.reason == "vm_runtime_unattested"

    job["context"]["vm"] = {
        **READY_VM,
        "ssh_ready_source": "provisioner_probe",
    }
    assert resolve_workspace_runtime(job, vm_mode="same-cluster").ready


def test_sandbox_assignment_ignores_stale_vm() -> None:
    decision = resolve_workspace_runtime(
        _stamped_job("sandbox", vm=READY_VM, container=READY_SANDBOX)
    )
    assert decision.ready
    assert decision.effective_backend == "sandbox"
    assert decision.selected_context_key == "workspace_container"
    assert decision.stale_backend == "vm"


def test_matching_provisioning_failure_is_truthful() -> None:
    decision = resolve_workspace_runtime(
        _stamped_job(
            "vm",
            requested="vm",
            vm={"status": "failed", "error": "private backend details"},
            container=READY_SANDBOX,
        )
    )
    assert decision.state == "failed"
    assert decision.reason == "vm_provisioning_failed"
    assert "private" not in str(decision.safe_projection())


def test_legacy_remote_and_container_config_values_derive_canonically() -> None:
    vm_job = {
        "config_override": {"workspace": {"backend": "remote"}},
        "context": {
            "vm": {
                **READY_VM,
                "provision_generation": str(uuid4()),
            }
        },
    }
    sandbox_job = {
        "config_override": {"workspace": {"backend": "container"}},
        "context": {
            "workspace_container": {
                **READY_SANDBOX,
                "_runtime_incarnation": str(uuid4()),
            }
        },
    }

    assert resolve_workspace_contract(vm_job).assigned_backend == "vm"
    assert resolve_workspace_runtime(vm_job).effective_backend == "vm"
    assert resolve_workspace_contract(sandbox_job).assigned_backend == "sandbox"
    assert resolve_workspace_runtime(sandbox_job).effective_backend == "sandbox"


def test_legacy_ready_runtime_without_provenance_fails_closed() -> None:
    unproven_vm = {
        key: value for key, value in READY_VM.items() if key != "provision_generation"
    }
    with pytest.raises(WorkspaceContractError) as exc:
        resolve_workspace_contract(
            {
                "config_override": {"workspace": {"backend": "vm"}},
                "context": {"vm": unproven_vm},
            }
        )
    assert exc.value.code == "legacy_workspace_ambiguous"


def test_stamped_ready_runtime_without_provenance_fails_closed() -> None:
    unproven_container = {
        key: value
        for key, value in READY_SANDBOX.items()
        if key != "_runtime_incarnation"
    }
    decision = resolve_workspace_runtime(
        _stamped_job("sandbox", container=unproven_container)
    )

    assert decision.state == "invalid"
    assert decision.reason == "sandbox_runtime_unattested"


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["created", "paused"])
async def test_exact_previous_release_k8s_job_runtime_is_live_attested_and_adopted(
    status: str,
) -> None:
    from orchestrator.services.job_workspace_adoption import (
        LegacyK8sAdoptionOutcome,
        ensure_legacy_k8s_job_runtime_authority,
    )

    job = {
        "id": str(uuid4()),
        "status": status,
        "execution_lane": "pinned",
        "config_override": {"workspace": {"backend": "container"}},
        # Exact immediately-previous-release shape: no contract, marker or UID.
        "context": {"workspace_container": dict(HISTORICAL_K8S_JOB_RUNTIME)},
    }
    db = _LegacyAdoptionDB(job)
    attestation = _runtime_attestation()
    provisioner = AsyncMock()
    provisioner.attest_workspace_runtime.return_value = attestation

    result = await ensure_legacy_k8s_job_runtime_authority(db, provisioner, job)

    assert result.outcome is LegacyK8sAdoptionOutcome.ADOPTED
    assert result.owner.kind == "job"
    assert result.owner.id == job["id"]
    assert provisioner.attest_workspace_runtime.await_count == 3
    stored = await db.get_job(job["id"])
    runtime = stored["context"]["workspace_container"]
    assert runtime["_runtime_incarnation"] == attestation.runtime_incarnation
    assert runtime[LEGACY_K8S_RUNTIME_ADOPTION_KEY]["workspace_generation"] == (
        attestation.workspace_generation
    )
    assert resolve_workspace_runtime(stored).ready


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["completed", "cancelled"])
async def test_terminal_legacy_k8s_runtime_is_never_adopted(status: str) -> None:
    from orchestrator.services.job_workspace_adoption import (
        LegacyK8sAdoptionOutcome,
        ensure_legacy_k8s_job_runtime_authority,
    )

    job = {
        "id": str(uuid4()),
        "status": status,
        "execution_lane": "pinned",
        "config_override": {"workspace": {"backend": "sandbox"}},
        "context": {"workspace_container": dict(HISTORICAL_K8S_JOB_RUNTIME)},
    }
    db = _LegacyAdoptionDB(job)
    provisioner = AsyncMock()

    result = await ensure_legacy_k8s_job_runtime_authority(db, provisioner, job)

    assert result.outcome is LegacyK8sAdoptionOutcome.NOT_NEEDED
    provisioner.attest_workspace_runtime.assert_not_awaited()
    assert db.adoption_calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["unavailable", "replaced"])
async def test_legacy_k8s_adoption_refuses_missing_or_replaced_pod(
    failure: str,
) -> None:
    from orchestrator.services.job_workspace_adoption import (
        LegacyK8sAdoptionOutcome,
        ensure_legacy_k8s_job_runtime_authority,
    )

    job = {
        "id": str(uuid4()),
        "status": "created",
        "execution_lane": "pinned",
        "config_override": {"workspace": {"backend": "sandbox"}},
        "context": {"workspace_container": dict(HISTORICAL_K8S_JOB_RUNTIME)},
    }
    db = _LegacyAdoptionDB(job)
    provisioner = AsyncMock()
    if failure == "unavailable":
        provisioner.attest_workspace_runtime.side_effect = RuntimeError(
            "Pod unavailable"
        )
    else:
        provisioner.attest_workspace_runtime.side_effect = [
            _runtime_attestation(),
            _runtime_attestation(),
        ]

    result = await ensure_legacy_k8s_job_runtime_authority(db, provisioner, job)

    assert result.outcome is LegacyK8sAdoptionOutcome.RETRY
    assert db.adoption_calls == []
    stored = await db.get_job(job["id"])
    assert "_runtime_incarnation" not in stored["context"]["workspace_container"]


@pytest.mark.asyncio
async def test_pod_replacement_between_attestation_and_persistence_is_reverted() -> (
    None
):
    from orchestrator.services.job_workspace_adoption import (
        LegacyK8sAdoptionOutcome,
        ensure_legacy_k8s_job_runtime_authority,
    )

    job = {
        "id": str(uuid4()),
        "status": "created",
        "execution_lane": "pinned",
        "config_override": {"workspace": {"backend": "sandbox"}},
        "context": {"workspace_container": dict(HISTORICAL_K8S_JOB_RUNTIME)},
    }
    db = _LegacyAdoptionDB(job)
    predecessor = _runtime_attestation()
    provisioner = AsyncMock()
    # The first two reads bracket the tentative stamp; the post-CAS read sees
    # the same-name replacement before any caller is told it adopted.
    provisioner.attest_workspace_runtime.side_effect = [
        predecessor,
        predecessor,
        _runtime_attestation(),
    ]

    result = await ensure_legacy_k8s_job_runtime_authority(db, provisioner, job)

    assert result.outcome is LegacyK8sAdoptionOutcome.RETRY
    assert result.reason == "kubernetes_runtime_changed_after_persistence"
    assert len(db.adoption_calls) == 2
    stored = await db.get_job(job["id"])
    assert stored["context"]["workspace_container"] == HISTORICAL_K8S_JOB_RUNTIME


@pytest.mark.asyncio
async def test_old_callback_can_adopt_runtime_for_new_stamped_sandbox_contract() -> (
    None
):
    from orchestrator.services.job_workspace_adoption import (
        LegacyK8sAdoptionOutcome,
        ensure_legacy_k8s_job_runtime_authority,
    )

    job = {
        "id": str(uuid4()),
        "status": "created",
        "execution_lane": "pinned",
        **_stamped_job(
            "sandbox", requested="sandbox", container=HISTORICAL_K8S_JOB_RUNTIME
        ),
    }
    db = _LegacyAdoptionDB(job)
    provisioner = AsyncMock()
    provisioner.attest_workspace_runtime.return_value = _runtime_attestation()

    result = await ensure_legacy_k8s_job_runtime_authority(db, provisioner, job)

    assert result.outcome is LegacyK8sAdoptionOutcome.ADOPTED
    assert resolve_workspace_runtime(result.authority_job).ready


@pytest.mark.asyncio
async def test_stamped_sandbox_adoption_ignores_opposite_tier_diagnostic_residue() -> (
    None
):
    from orchestrator.services.job_workspace_adoption import (
        LegacyK8sAdoptionOutcome,
        ensure_legacy_k8s_job_runtime_authority,
    )

    job = {
        "id": str(uuid4()),
        "status": "created",
        "execution_lane": "pinned",
        **_stamped_job(
            "sandbox",
            requested="sandbox",
            container=HISTORICAL_K8S_JOB_RUNTIME,
            vm=READY_VM,
        ),
    }
    db = _LegacyAdoptionDB(job)
    provisioner = AsyncMock()
    provisioner.attest_workspace_runtime.return_value = _runtime_attestation()

    result = await ensure_legacy_k8s_job_runtime_authority(db, provisioner, job)

    assert result.outcome is LegacyK8sAdoptionOutcome.ADOPTED
    decision = resolve_workspace_runtime(result.authority_job)
    assert decision.ready
    assert decision.effective_backend == "sandbox"
    assert decision.stale_backend == "vm"


@pytest.mark.asyncio
async def test_unstamped_both_tier_legacy_row_remains_ambiguous() -> None:
    from orchestrator.services.job_workspace_adoption import (
        LegacyK8sAdoptionOutcome,
        ensure_legacy_k8s_job_runtime_authority,
    )

    job = {
        "id": str(uuid4()),
        "status": "created",
        "execution_lane": "pinned",
        "config_override": {"workspace": {"backend": "sandbox"}},
        "context": {
            "workspace_container": dict(HISTORICAL_K8S_JOB_RUNTIME),
            "vm": dict(READY_VM),
        },
    }
    db = _LegacyAdoptionDB(job)
    provisioner = AsyncMock()

    result = await ensure_legacy_k8s_job_runtime_authority(db, provisioner, job)

    assert result.outcome is LegacyK8sAdoptionOutcome.NOT_NEEDED
    assert result.reason == "authority_ambiguous"
    provisioner.attest_workspace_runtime.assert_not_awaited()
    assert db.adoption_calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "workspace_update",
    [
        {"status": "deleted"},
        {"status": "failed"},
        {"status": "creating"},
        {"status": "pending"},
        {"status": "ready", "host": None, "pod_ip": None},
    ],
    ids=["deleted", "failed", "creating", "pending", "missing-endpoint"],
)
async def test_nonready_adoption_marker_defers_to_workspace_lifecycle(
    workspace_update: dict,
) -> None:
    from orchestrator.services.job_workspace_adoption import (
        LegacyK8sAdoptionOutcome,
        ensure_legacy_k8s_job_runtime_authority,
    )

    attestation = _runtime_attestation()
    workspace = _adopted_k8s_runtime(attestation)
    workspace.update(workspace_update)
    job = {
        "id": str(uuid4()),
        "status": "paused",
        "execution_lane": "pinned",
        **_stamped_job("sandbox", container=workspace),
    }
    db = _LegacyAdoptionDB(job)
    provisioner = AsyncMock()

    result = await ensure_legacy_k8s_job_runtime_authority(db, provisioner, job)

    assert result.outcome is LegacyK8sAdoptionOutcome.NOT_NEEDED
    provisioner.attest_workspace_runtime.assert_not_awaited()
    assert db.adoption_calls == []


@pytest.mark.asyncio
async def test_completion_recovery_reprovisions_and_retires_the_stale_marker(
    monkeypatch,
) -> None:
    from orchestrator import main as orch_main
    from orchestrator.services.job_workspace_adoption import (
        LegacyK8sAdoptionOutcome,
        ensure_legacy_k8s_job_runtime_authority,
    )

    job = {
        "id": str(uuid4()),
        "status": "created",
        "execution_lane": "stateless",
        **_stamped_job("sandbox", container=HISTORICAL_K8S_JOB_RUNTIME),
    }
    db = _LegacyAdoptionDB(job)
    predecessor = _runtime_attestation()
    provisioner = AsyncMock()
    provisioner.attest_workspace_runtime.return_value = predecessor
    first = await ensure_legacy_k8s_job_runtime_authority(db, provisioner, job)
    assert first.outcome is LegacyK8sAdoptionOutcome.ADOPTED

    # A workspace_unavailable completion parks the job, and lifecycle deletion
    # preserves diagnostic marker history until ordinary provisioning replaces
    # the Pod. The marker must not turn this state into an adoption wait.
    current = db.jobs[job["id"]]
    current["status"] = "paused"
    current["context"]["workspace_container"]["status"] = "deleted"
    deleted_job = await db.get_job(job["id"])
    assert orch_main._job_needs_sandbox(deleted_job)
    assert control_seams.resume_missing_workspace(deleted_job) == "sandbox"

    # Model the provisioner's replacement callback. JSONB merge legitimately
    # leaves the predecessor marker beside the new server-written Pod UID; live
    # attestation refreshes both as one exact snapshot CAS before resume.
    replacement = _runtime_attestation(
        host="workspace-replacement.internal", pod_ip="10.42.2.19"
    )

    async def provision_replacement(owner, **kwargs):
        assert owner == WorkspaceOwner.job(job["id"])
        assert kwargs["current_status"] == "deleted"
        db.jobs[job["id"]]["context"]["workspace_container"].update(
            {
                "status": "ready",
                "provisioner": "k8s",
                "host": replacement.host,
                "pod_ip": replacement.pod_ip,
                "port": replacement.port,
                "_runtime_incarnation": replacement.runtime_incarnation,
            }
        )
        return SimpleNamespace(
            outcome=EnsureOutcome.PENDING,
            status="creating",
        )

    db.get_admittable_stateless_jobs = AsyncMock(
        side_effect=lambda **_kwargs: [deepcopy(db.jobs[job["id"]])]
    )
    db.admit_stateless_worker_job = AsyncMock(return_value=(True, "inserted"))
    db.update_job_status = AsyncMock()
    monkeypatch.setattr(orch_main, "postgres_db", db)
    monkeypatch.setattr(orch_main, "AUTO_ASSIGN_ENABLED", False)
    monkeypatch.setattr(orch_main, "STATELESS_WORKER_ENABLED", True)
    monkeypatch.setattr(orch_main.container_provisioner, "_k8s_available", True)
    monkeypatch.setattr(orch_main.container_provisioner, "_in_cluster", True)
    live_attestation = AsyncMock(return_value=replacement)
    monkeypatch.setattr(
        orch_main.container_provisioner,
        "attest_workspace_runtime",
        live_attestation,
    )
    ensure_workspace = AsyncMock(side_effect=provision_replacement)
    monkeypatch.setattr(job_dispatcher, "ensure_workspace", ensure_workspace)

    # First real dispatcher pass sees the deleted lifecycle state and starts
    # ordinary provisioning without trying to attest the vanished predecessor.
    await job_dispatcher.dispatch_pending_jobs(
        dependencies=orch_main._job_dispatch_dependencies()
    )
    ensure_workspace.assert_awaited_once()
    live_attestation.assert_not_awaited()
    db.admit_stateless_worker_job.assert_not_awaited()

    # The replacement callback left the predecessor marker beside its new UID.
    # That successor was created by this server under a durable reservation, so
    # it is not an adopted runtime: the next dispatcher pass retires the stale
    # marker by exact snapshot CAS and admits the resumed job exactly once.
    # Refreshing the marker instead would be a second adoption of a Pod nobody
    # adopted, and leaving it would make every later pre-network check
    # re-attest the vanished predecessor and refuse delivery for good.
    await job_dispatcher.dispatch_pending_jobs(
        dependencies=orch_main._job_dispatch_dependencies()
    )

    resumed = await db.get_job(job["id"])
    runtime = resumed["context"]["workspace_container"]
    assert runtime["_runtime_incarnation"] == replacement.runtime_incarnation
    assert LEGACY_K8S_RUNTIME_ADOPTION_KEY not in runtime
    assert resolve_workspace_runtime(resumed).ready
    assert control_seams.resume_missing_workspace(resumed) is None
    db.admit_stateless_worker_job.assert_awaited_once()
    db.update_job_status.assert_not_awaited()


@pytest.mark.asyncio
async def test_ready_adopted_runtime_control_plane_failure_remains_retryable() -> None:
    from orchestrator.services.job_workspace_adoption import (
        LegacyK8sAdoptionOutcome,
        ensure_legacy_k8s_job_runtime_authority,
    )

    attestation = _runtime_attestation()
    job = {
        "id": str(uuid4()),
        "status": "created",
        "execution_lane": "pinned",
        **_stamped_job("sandbox", container=_adopted_k8s_runtime(attestation)),
    }
    db = _LegacyAdoptionDB(job)
    provisioner = AsyncMock()
    provisioner.attest_workspace_runtime.side_effect = RuntimeError(
        "temporary Kubernetes API outage"
    )

    result = await ensure_legacy_k8s_job_runtime_authority(db, provisioner, job)

    assert result.outcome is LegacyK8sAdoptionOutcome.RETRY
    assert result.reason == "kubernetes_attestation_unavailable"
    assert db.adoption_calls == []


@pytest.mark.asyncio
async def test_inherited_legacy_runtime_is_attested_as_parent_owner() -> None:
    from orchestrator.services.job_workspace_adoption import (
        LegacyK8sAdoptionOutcome,
        ensure_legacy_k8s_job_runtime_authority,
    )

    parent = {
        "id": str(uuid4()),
        "status": "waiting",
        "execution_lane": "pinned",
        "config_override": {"workspace": {"backend": "sandbox"}},
        "context": {"workspace_container": dict(HISTORICAL_K8S_JOB_RUNTIME)},
    }
    child = {
        "id": str(uuid4()),
        "parent_job_id": parent["id"],
        "status": "created",
        "execution_lane": "pinned",
        "config_override": {"workspace": {"backend": "sandbox"}},
        "context": {
            "inherits_parent_workspace": True,
            "workspace_container": dict(HISTORICAL_K8S_JOB_RUNTIME),
        },
    }
    db = _LegacyAdoptionDB(parent, child)
    provisioner = AsyncMock()
    provisioner.attest_workspace_runtime.return_value = _runtime_attestation()

    result = await ensure_legacy_k8s_job_runtime_authority(db, provisioner, child)

    assert result.outcome is LegacyK8sAdoptionOutcome.ADOPTED
    assert result.owner.id == parent["id"]
    assert result.authority_job["id"] == parent["id"]
    assert all(
        call.args[0].id == parent["id"]
        for call in provisioner.attest_workspace_runtime.await_args_list
    )
    assert (
        "_runtime_incarnation"
        not in (await db.get_job(child["id"]))["context"]["workspace_container"]
    )


@pytest.mark.asyncio
async def test_concurrent_tier_transition_wins_legacy_adoption_cas() -> None:
    from orchestrator.services.job_workspace_adoption import (
        LegacyK8sAdoptionOutcome,
        ensure_legacy_k8s_job_runtime_authority,
    )

    job = {
        "id": str(uuid4()),
        "status": "created",
        "execution_lane": "pinned",
        "config_override": {"workspace": {"backend": "sandbox"}},
        "context": {"workspace_container": dict(HISTORICAL_K8S_JOB_RUNTIME)},
    }
    db = _LegacyAdoptionDB(job)

    def transition(current: dict) -> None:
        current["config_override"]["workspace"] = {"backend": "vm"}
        current["context"] = {
            WORKSPACE_CONTRACT_CONTEXT_KEY: {
                "version": 1,
                "requested_backend": "vm",
                "assigned_backend": "vm",
                "assignment_source": "operator",
            },
            "vm": {**READY_VM, "provision_generation": str(uuid4())},
        }

    db.before_cas = transition
    provisioner = AsyncMock()
    provisioner.attest_workspace_runtime.return_value = _runtime_attestation()

    result = await ensure_legacy_k8s_job_runtime_authority(db, provisioner, job)

    assert result.outcome is LegacyK8sAdoptionOutcome.NOT_NEEDED
    assert result.reason == "workspace_snapshot_changed"
    assert resolve_workspace_runtime(result.authority_job).effective_backend == "vm"
    assert LEGACY_K8S_RUNTIME_ADOPTION_KEY not in repr(result.authority_job)


@pytest.mark.asyncio
async def test_adopted_runtime_is_live_revalidated_at_network_boundary() -> None:
    from orchestrator.services.job_workspace_adoption import (
        ensure_legacy_k8s_job_runtime_authority,
        verify_adopted_k8s_runtime_before_delivery,
    )

    job = {
        "id": str(uuid4()),
        "status": "created",
        "execution_lane": "pinned",
        "config_override": {"workspace": {"backend": "sandbox"}},
        "context": {"workspace_container": dict(HISTORICAL_K8S_JOB_RUNTIME)},
    }
    db = _LegacyAdoptionDB(job)
    original = _runtime_attestation()
    provisioner = AsyncMock()
    provisioner.attest_workspace_runtime.return_value = original
    adopted = await ensure_legacy_k8s_job_runtime_authority(db, provisioner, job)

    assert await verify_adopted_k8s_runtime_before_delivery(
        db, provisioner, adopted.authority_job
    )
    provisioner.attest_workspace_runtime.return_value = _runtime_attestation()
    assert not await verify_adopted_k8s_runtime_before_delivery(
        db, provisioner, adopted.authority_job
    )


def test_observed_legacy_vm_request_sandbox_config_shape_fails_closed() -> None:
    decision = resolve_workspace_runtime(
        {
            "config_override": {"workspace": {"backend": "sandbox"}},
            "context": {
                "workspace_backend": "vm",
                "workspace_container": {
                    **READY_SANDBOX,
                    "_runtime_incarnation": str(uuid4()),
                },
            },
        }
    )
    assert decision.state == "invalid"
    assert decision.reason == "legacy_workspace_ambiguous"


def test_safe_projection_never_contains_transport_coordinates() -> None:
    projection = workspace_contract_projection(
        _stamped_job("vm", requested="vm", vm=READY_VM, container=READY_SANDBOX)
    )
    rendered = repr(projection)
    assert projection["effective_backend"] == "vm"
    assert "private-vm" not in rendered
    assert "workspace.private" not in rendered
    assert "ssh_host" not in rendered


@pytest.mark.parametrize("replace_endpoint", [False, True], ids=["fresh", "resume"])
def test_fresh_and_resume_select_only_the_assigned_runtime(
    replace_endpoint: bool,
) -> None:
    vm_job = _stamped_job("vm", requested="vm", vm=READY_VM, container=READY_SANDBOX)
    config, decision = control_seams.inject_matching_workspace_config(
        vm_job,
        {"workspace": {"backend": "vm"}},
        replace_endpoint=replace_endpoint,
    )
    assert decision.effective_backend == "vm"
    assert config["workspace"]["backend"] == "vm"
    assert config["workspace"]["remote"]["host"] == READY_VM["ssh_host"]
    assert READY_SANDBOX["host"] not in repr(config)

    sandbox_job = _stamped_job("sandbox", vm=READY_VM, container=READY_SANDBOX)
    config, decision = control_seams.inject_matching_workspace_config(
        sandbox_job,
        {"workspace": {"backend": "sandbox"}},
        replace_endpoint=replace_endpoint,
    )
    assert decision.effective_backend == "sandbox"
    assert config["workspace"]["backend"] == "sandbox"
    assert config["workspace"]["remote"]["host"] == READY_SANDBOX["host"]
    assert READY_VM["ssh_host"] not in repr(config)


@pytest.mark.parametrize("replace_endpoint", [False, True], ids=["fresh", "resume"])
def test_fresh_and_resume_refuse_opposite_only_readiness(
    replace_endpoint: bool,
) -> None:
    job = _stamped_job("vm", requested="vm", container=READY_SANDBOX)
    config, decision = control_seams.inject_matching_workspace_config(
        job,
        {"workspace": {"backend": "vm"}},
        replace_endpoint=replace_endpoint,
    )
    assert decision.state == "mismatch"
    assert decision.effective_backend is None
    assert config == {"workspace": {"backend": "vm"}}


@pytest.mark.asyncio
async def test_pre_delivery_recheck_ignores_only_opposite_tier_residue(
    monkeypatch,
) -> None:
    from orchestrator import main

    original = {
        "id": str(uuid4()),
        **_stamped_job("vm", requested="vm", vm=READY_VM),
    }
    refreshed = {
        **original,
        "context": {
            **original["context"],
            "workspace_container": READY_SANDBOX,
        },
    }
    monkeypatch.setattr(main.postgres_db, "get_job", AsyncMock(return_value=refreshed))

    assert await main._workspace_runtime_unchanged_before_delivery(original)


@pytest.mark.asyncio
async def test_pre_delivery_recheck_refuses_matching_runtime_change(
    monkeypatch,
) -> None:
    from orchestrator import main

    original = {
        "id": str(uuid4()),
        **_stamped_job("vm", requested="vm", vm=READY_VM),
    }
    refreshed = {
        **original,
        "context": {
            **original["context"],
            "vm": {**READY_VM, "ssh_host": "replacement.internal"},
        },
    }
    monkeypatch.setattr(main.postgres_db, "get_job", AsyncMock(return_value=refreshed))

    assert not await main._workspace_runtime_unchanged_before_delivery(original)


def test_worker_recipient_accepts_exact_server_tier_projection() -> None:
    validate_worker_workspace_projection(
        config_override={
            "workspace": {
                "backend": "vm",
                "remote": {"host": "not-returned-in-errors"},
            }
        },
        resolved_config=None,
        workspace_runtime={
            "requested_backend": "vm",
            "assigned_backend": "vm",
            "effective_backend": "vm",
            "state": "ready",
        },
    )


def test_job_start_workspace_authority_survives_producer_consumer_round_trip() -> None:
    from orchestrator.main import JobStartRequest as ProducerJobStartRequest
    from agent.api.models import JobStartRequest as ConsumerJobStartRequest

    runtime = {
        "requested_backend": None,
        "assigned_backend": "sandbox",
        "effective_backend": "sandbox",
        "state": "ready",
    }
    producer = ProducerJobStartRequest(
        job_id=str(uuid4()),
        description="exercise the real dispatch wire contract",
        config_override={
            "workspace": {
                "backend": "sandbox",
                "remote": {"host": "workspace.internal"},
            }
        },
        workspace_runtime=runtime,
    )

    payload = producer.model_dump(exclude_none=True)
    assert payload["workspace_runtime"] == runtime
    consumer = ConsumerJobStartRequest.model_validate(payload)
    validate_worker_workspace_projection(
        config_override=consumer.config_override,
        resolved_config=consumer.resolved_config,
        workspace_runtime=consumer.workspace_runtime,
    )


@pytest.mark.parametrize(
    ("runtime", "code"),
    [
        (None, "workspace_runtime_authority_missing"),
        (
            {
                "assigned_backend": "vm",
                "effective_backend": "sandbox",
                "state": "ready",
            },
            "workspace_runtime_authority_mismatch",
        ),
        (
            {
                "assigned_backend": "vm",
                "effective_backend": "vm",
                "state": "ready",
            },
            "workspace_runtime_config_mismatch",
        ),
    ],
)
def test_worker_recipient_refuses_missing_or_cross_tier_authority(
    runtime, code
) -> None:
    with pytest.raises(WorkspaceContractError) as raised:
        validate_worker_workspace_projection(
            config_override={
                "workspace": {
                    "backend": "sandbox",
                    "remote": {"host": "private-coordinate"},
                }
            },
            resolved_config=None,
            workspace_runtime=runtime,
        )
    assert raised.value.code == code
    assert "private-coordinate" not in raised.value.detail


def test_worker_recipient_reads_resolved_config_agent_tier() -> None:
    validate_worker_workspace_projection(
        config_override=None,
        resolved_config={
            "agent": {
                "workspace": {
                    "backend": "container",
                    "remote": {"host": "sandbox.internal"},
                }
            }
        },
        workspace_runtime={
            "assigned_backend": "sandbox",
            "effective_backend": "sandbox",
            "state": "ready",
        },
    )
