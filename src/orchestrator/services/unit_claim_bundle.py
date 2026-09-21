"""The claim bundle one leased stateless unit needs to run its turn.

R1.B06. The stateless turn executor calls this straight after ``claim_unit``.
What it hands back is assembled by the *same* B05 operations the pinned lane
uses — ``session_attach_payload.assemble_session_attach_payload``,
``job_workspace_authority``'s attestations, ``job_start_bundle`` — under the
same fail-closed rules, which is the property that keeps the two lanes from
drifting apart.

Two things survived the extraction deliberately and must keep surviving it:

* **The live-lease proof is one statement.** ``(unit_id, lease_token)`` must
  match ``state='leased'`` with the exact current token, and the same SELECT
  reads the watermarks. Credentials reach only the executor that currently
  holds the unit; a zombie with a stale token gets the same generic 403 as a
  guess, so there is no enumeration oracle. Nothing here may split that read.
* **Attestation is taken twice.** Credential and datasource assembly can block
  on connector locks and external stores for longer than the queue lease, so
  the workspace authority is re-attested immediately before the response
  crosses the credential boundary. The initial and confirmed attestations are
  both required and both compared.

Transport stays in ``routers/unit_claim.py``: ``require_internal`` is the
router's, not this module's.
"""

from __future__ import annotations

import copy
import hashlib
import json
import logging
import os
import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable
from uuid import UUID, NAMESPACE_URL, uuid5

from fastapi import HTTPException
from orchestrator.schemas.agent_runtime import WorkspaceRecoveryReport
from shared.workspace_recovery import (
    WorkspaceRecoveryCode,
    WorkspaceRecoveryDisposition,
    workspace_recovery_enabled,
)

from orchestrator.security.access import vm_workspaces_on_pod_network
from orchestrator.services import (
    dispatch_credentials,
    job_start_bundle,
    job_workspace_authority,
    session_attach_payload,
)
from orchestrator.services.job_workspace_runtime import (
    get_container_context,
    get_vm_context,
    inject_container_workspace_config,
    inject_vm_workspace_config,
    stateless_worker_workspace_owner,
)
from orchestrator.services.session_class_policy import require_stateless_workspace
from orchestrator.services.stateless_workspace_gate import (
    stateless_session_workspace_check,
)
from orchestrator.services.vm_provisioner import vm_provisioner
from shared.runtime.core.loader import canonical_config_name
from shared.session_retirement import STATELESS_STOP_KEYS, stateless_stop_markers
from shared.workspace_contract import (
    WorkspaceContractError,
    resolve_workspace_contract,
    resolve_workspace_runtime,
    workspace_runtime_authority_digest,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class UnitClaimBundleDependencies:
    """Everything the bundle needs from the application, per invocation.

    The four ``*_dependencies`` fields are **factories**, not bound operations.
    That is what lets this module call the B05 services directly and lets
    ``main`` retire the thin wrappers it used to keep for them: the wrapper
    existed only to marry an operation to its dependency object, and that
    marriage now happens here, at the call.
    """

    db: Any
    #: The router's transport guard, resolved from the handling app.
    require_internal: Callable[..., Awaitable[Any]]
    #: B06 lane A — the pinned sender, reused for the stateless attach.
    send_session_attach: Callable[..., Awaitable[Any]]
    #: B06 lane B — whether the thread carries a knowledge scope.
    thread_has_knowledge_scope: Callable[..., Awaitable[bool]]
    #: B05 — the thread's authorized project ids.
    thread_project_ids: Callable[..., Awaitable[Any]]
    #: Unassigned in the census; injected rather than imported.
    resolve_background_push_workspace: Callable[..., Any]
    session_attach_payload_dependencies: Callable[[], Any]
    job_workspace_authority_dependencies: Callable[[], Any]
    job_start_bundle_dependencies: Callable[[], Any]
    dispatch_credential_dependencies: Callable[[], Any]
    recovery_store: Any = None
    #: Read-only pooled-executor attestation (K8s Pod UID + pool membership).
    #: Injected so routes/services stay testable without a cluster. ``None``
    #: is unknown authority (503), never success.
    attest_stateless_claimant: Callable[[str, str], Awaitable[None]] | None = None


class _WorkspaceRecoveryRefusal(HTTPException):
    def __init__(
        self,
        detail: str,
        code: WorkspaceRecoveryCode = WorkspaceRecoveryCode.RUNTIME_NOT_READY,
    ):
        super().__init__(409, detail)
        self.code = code


def _digest(value: Any) -> str:
    return (
        "sha256:"
        + hashlib.sha256(
            json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
    )


async def get_workspace_recovery_disposition(
    *,
    unit_id: str,
    lease_token: int,
    dependencies: UnitClaimBundleDependencies,
) -> WorkspaceRecoveryDisposition | None:
    if dependencies.recovery_store is None:
        return None
    async with dependencies.db.acquire() as conn:
        attempt = await dependencies.recovery_store.get_attempt_disposition(
            conn,
            job_id=UUID(unit_id),
            lease_token=lease_token,
        )
        receipt = attempt.disposition if attempt else None
        if not receipt:
            # Legacy attempts can be absent; this reads committed receipts
            # only and makes no inference about execution or refund rights.
            row = await conn.fetchrow(
                "SELECT accepted_result AS receipt FROM vm_workspace_recovery_requests "
                "WHERE scope_kind='job' AND scope_id=$1 "
                "AND accepted_result->>'accepted_lease_token'=$2 "
                "UNION ALL SELECT outcome->'disposition' AS receipt "
                "FROM vm_workspace_recovery_jobs WHERE job_id=$1 "
                "AND accepted_lease_token=$3 AND outcome ? 'disposition' LIMIT 1",
                UUID(unit_id),
                str(lease_token),
                lease_token,
            )
            receipt = row["receipt"] if row else None
            if isinstance(receipt, str):
                receipt = json.loads(receipt)
    if not receipt:
        return None
    return WorkspaceRecoveryDisposition(
        code=WorkspaceRecoveryCode(receipt["code"]),
        action=receipt["action"],
        operation_id=UUID(receipt["operation_id"]),
        accepted_lease_token=receipt["accepted_lease_token"],
        hold_lease_token=receipt["hold_lease_token"],
    )


async def _validate_worker_lease(
    conn: Any, *, unit_id: str, lease_token: int, pod_name: str, pod_uid: str
) -> None:
    """Exact database lease proof for a pooled worker claimant.

    Checks a valid UID shape, the exact leased queue token, and the matching
    ``leased_by`` pod name in one short read. This is the database half of
    worker admission; the Kubernetes executor-Pod observation is separate
    (``_attest_worker_claimant``) and always happens outside write-lock
    transactions. No ``agents`` registration is required: pooled executors
    intentionally never create one.
    """
    try:
        UUID(str(pod_uid))
    except (ValueError, TypeError, AttributeError):
        raise HTTPException(403, "Lease validation failed") from None
    row = await conn.fetchrow(
        "SELECT state, lease_token, leased_by FROM run_queue WHERE unit_id=$1::uuid",
        unit_id,
    )
    if not (
        row
        and row["state"] == "leased"
        and row["lease_token"] == lease_token
        and pod_name
        and pod_uid
        and row["leased_by"] == pod_name
    ):
        raise HTTPException(403, "Lease validation failed")


async def _attest_worker_claimant(
    *, pod_name: str, pod_uid: str, dependencies: UnitClaimBundleDependencies
) -> None:
    """Read-only executor-Pod attestation for a pooled worker claimant.

    The slow Kubernetes read stays outside database transactions; callers
    re-read the exact lease under the existing short transactional boundary
    after it returns. ``None`` (unwired collaborator) is unknown authority,
    never success.
    """
    attestor = getattr(dependencies, "attest_stateless_claimant", None)
    if attestor is None:
        raise HTTPException(503, "Claimant authority unavailable")
    await attestor(str(pod_name or ""), str(pod_uid or ""))


# Retained alias for suites that still resolve the pre-fix validator name.
# It now performs only the database lease half; attestation is separate.
async def _validate_worker_identity(
    conn: Any, *, unit_id: str, lease_token: int, pod_name: str, pod_uid: str
) -> None:
    await _validate_worker_lease(
        conn,
        unit_id=unit_id,
        lease_token=lease_token,
        pod_name=pod_name,
        pod_uid=pod_uid,
    )


async def report_workspace_recovery(
    *,
    unit_id: str,
    report: WorkspaceRecoveryReport,
    dependencies: UnitClaimBundleDependencies,
) -> WorkspaceRecoveryDisposition:
    store = dependencies.recovery_store
    if store is None:
        raise HTTPException(409, "Workspace recovery is unavailable")
    intent_digest = _digest({"unit_id": unit_id, **report.model_dump(mode="json")})
    # The accepted request is checked before current authority: admission
    # rotates the token and the response can be lost after that commit.
    # Matching accepted replay is idempotent and must not demand a current
    # lease or a live Pod; conflicting intent stays refused.
    async with dependencies.db.acquire() as conn:
        try:
            prior = await store._accepted_request(
                conn,
                job_id=UUID(unit_id),
                request_id=report.request_id,
                intent_digest=intent_digest,
            )
        except RuntimeError as exc:
            raise HTTPException(409, "Recovery request identity conflict") from exc
        if prior is not None:
            return prior
        if not workspace_recovery_enabled():
            raise HTTPException(409, "Workspace recovery is unavailable")
        await _validate_worker_lease(
            conn,
            unit_id=unit_id,
            lease_token=report.lease_token,
            pod_name=report.pod_name,
            pod_uid=str(report.pod_uid),
        )
    # Slow executor-Pod observation outside the database connection. The
    # locked lease CAS inside ``admit_hold`` rechecks exact currency before
    # any hold commits, so a lease stolen during this read cannot admit.
    await _attest_worker_claimant(
        pod_name=report.pod_name,
        pod_uid=str(report.pod_uid),
        dependencies=dependencies,
    )
    job = await dependencies.db.get_job(unit_id)
    try:
        contract = resolve_workspace_contract(job) if job else None
    except WorkspaceContractError:
        contract = None
    if not (
        job
        and job.get("execution_lane") == "stateless"
        and contract
        and contract.assigned_backend == "vm"
        and vm_workspaces_on_pod_network()
    ):
        raise HTTPException(409, "Workspace recovery is unavailable")
    owner = stateless_worker_workspace_owner(job)
    owner_job = job if owner.id == unit_id else await dependencies.db.get_job(owner.id)
    vm = get_vm_context(owner_job or {})

    def optional_uuid(key: str) -> UUID | None:
        try:
            return UUID(str(vm.get(key)))
        except (ValueError, TypeError):
            return None

    cluster = os.getenv("VM_CLUSTER_NAME", "local").strip()
    if not cluster:
        raise HTTPException(409, "Workspace recovery cluster is unavailable")
    namespace = vm.get("namespace")
    if not isinstance(namespace, str) or not namespace.strip():
        namespace = None
    try:
        return await store.admit_hold(
            job_id=UUID(unit_id),
            accepted_lease_token=report.lease_token,
            owner_kind=owner.kind,
            owner_id=UUID(owner.id),
            workspace_contract_digest=_digest(contract.to_context()),
            provision_generation=optional_uuid("provision_generation"),
            cluster_name=cluster,
            namespace=namespace,
            vm_uid=optional_uuid("vm_uid"),
            prior_vmi_uid=optional_uuid("vmi_uid"),
            prior_launcher_uid=optional_uuid("active_pod_uid"),
            root_pvc_uid=optional_uuid("rootdisk_pvc_uid"),
            code=WorkspaceRecoveryCode(report.code),
            request_id=report.request_id,
            actor_kind="worker",
            actor_id=f"{report.pod_name}/{report.pod_uid}",
            intent_digest=intent_digest,
            original_cause={"code": report.code},
        )
    except RuntimeError as exc:
        # Admission does its own locked lease CAS. Never claim acceptance if
        # that transaction lost authority or refused the request identity.
        raise HTTPException(409, "Workspace recovery admission refused") from exc


async def _attest_recoverable_vm(owner: Any, *, dependencies: Any) -> Any:
    try:
        return await job_workspace_authority.attest_stateless_worker_vm_workspace(
            owner, dependencies=dependencies
        )
    except job_workspace_authority.RecoverableWorkspaceAuthorityRefusal as exc:
        raise _WorkspaceRecoveryRefusal(exc.detail, exc.recovery_code) from exc


async def claim_bundle_for_unit(
    unit_id: str,
    *,
    lease_token: int,
    pod_name: str,
    pod_uid: str,
    dependencies: UnitClaimBundleDependencies,
) -> dict[str, Any]:
    try:
        return await _assemble_claim_bundle(
            unit_id,
            lease_token=lease_token,
            pod_name=pod_name,
            pod_uid=pod_uid,
            dependencies=dependencies,
        )
    except _WorkspaceRecoveryRefusal as exc:
        if not workspace_recovery_enabled() or dependencies.recovery_store is None:
            raise
        report = WorkspaceRecoveryReport(
            lease_token=lease_token,
            pod_name=pod_name,
            pod_uid=pod_uid,
            code=exc.code.value,
            request_id=uuid5(
                NAMESPACE_URL, f"workspace-bundle:{unit_id}:{lease_token}"
            ),
        )
        receipt = await report_workspace_recovery(
            unit_id=unit_id, report=report, dependencies=dependencies
        )
        raise HTTPException(409, receipt.as_error_detail()) from None


async def _assemble_claim_bundle(
    unit_id: str,
    *,
    lease_token: int,
    pod_name: str,
    pod_uid: str,
    dependencies: UnitClaimBundleDependencies,
) -> dict[str, Any]:
    """Claim bundle for a leased stateless unit — internal (agent executor).

    The stateless turn executor calls this right after ``claim_unit`` to get
    everything a turn needs: the queue watermarks (skip-if-answered) and the
    full session-attach payload (config resolution, credentials in-flight,
    reauthorized datasources) — assembled by the SAME
    ``session_attach_payload.assemble_session_attach_payload`` the pinned-lane sender uses, under
    the same fail-closed rules.

    Auth is two-layer (stateless_agents.md §5.6): the ``X-Internal-Key``
    transport guard every agent→orchestrator call carries, PLUS proof of a
    LIVE lease — (unit_id, lease_token) must match ``state='leased'`` with
    the exact current token, checked server-side in one SELECT that also
    reads the watermarks. Credentials therefore flow only to the executor
    that currently holds the unit; a zombie with a stale token gets the same
    generic 403 as a guess (no enumeration oracle).

    Errors: 401 bad internal key; 403 token mismatch / not leased (single
    generic detail); 404 unit row absent; 409 not a session unit, thread not
    on the stateless lane, or attach assembly refused (generic reason).
    """
    from shared.run_queue import (
        LANE_STATELESS,
        UNIT_KIND_BG_TASK,
        UNIT_KIND_SESSION_TURN,
        UNIT_KIND_WORKER_BATCH,
    )

    try:
        UUID(str(unit_id))
    except (ValueError, TypeError):
        raise HTTPException(status_code=404, detail="Unknown unit") from None

    _t_start = time.perf_counter()
    async with dependencies.db.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT queue.unit_kind, queue.state, queue.lease_token, "
            "queue.leased_by, "
            "queue.input_seq, queue.consumed_seq, thread.status AS thread_status, "
            "thread.execution_lane AS thread_lane, thread.metadata AS thread_metadata "
            "FROM run_queue AS queue LEFT JOIN threads AS thread "
            "ON thread.id = queue.unit_id WHERE queue.unit_id = $1::uuid",
            unit_id,
        )
    if row is None:
        raise HTTPException(status_code=404, detail="Unknown unit")
    if (
        row["unit_kind"] == UNIT_KIND_WORKER_BATCH
        and dependencies.recovery_store is not None
    ):
        prior = await get_workspace_recovery_disposition(
            unit_id=unit_id,
            lease_token=lease_token,
            dependencies=dependencies,
        )
        if prior is not None:
            raise HTTPException(409, prior.as_error_detail())
    if row["state"] != "leased" or int(row["lease_token"]) != int(lease_token):
        # ONE generic detail for both cases — stale token and not-leased are
        # deliberately indistinguishable to the caller.
        raise HTTPException(status_code=403, detail="Lease validation failed")
    if row["unit_kind"] == UNIT_KIND_BG_TASK:
        from orchestrator.services.cloud_push_recovery import (
            CloudPushBundleRefused,
            build_cloud_push_bundle,
        )

        try:
            return await build_cloud_push_bundle(
                dependencies.db,
                unit_id=unit_id,
                lease_token=lease_token,
                pod_name=pod_name,
                pod_uid=pod_uid,
                resolve_workspace=dependencies.resolve_background_push_workspace,
            )
        except CloudPushBundleRefused:
            raise HTTPException(
                status_code=403, detail="Lease validation failed"
            ) from None
    if row["unit_kind"] == UNIT_KIND_WORKER_BATCH:
        # Initial worker admission before expensive credential assembly.
        # Unconditional with respect to VM_WORKSPACE_RECOVERY_ENABLED: lease
        # plus pooled-executor attestation admits the bundle in both flag
        # states. The slow Pod observation stays outside any write lock; the
        # final authorization below re-observes both claimant and lease.
        async with dependencies.db.acquire() as conn:
            await _validate_worker_lease(
                conn,
                unit_id=unit_id,
                lease_token=lease_token,
                pod_name=pod_name,
                pod_uid=pod_uid,
            )
        await _attest_worker_claimant(
            pod_name=pod_name, pod_uid=pod_uid, dependencies=dependencies
        )
        job = await dependencies.db.get_job(unit_id)
        if not job or job.get("execution_lane") != LANE_STATELESS:
            raise HTTPException(
                status_code=409, detail="Job is not on the stateless lane"
            )
        (
            workspace_action,
            job,
            _workspace_reason,
        ) = await job_workspace_authority.prepare_job_workspace_runtime(
            job,
            dependencies=dependencies.job_workspace_authority_dependencies(),
        )
        if workspace_action != "proceed":
            try:
                vm_contract = resolve_workspace_contract(job)
            except WorkspaceContractError:
                vm_contract = None
            if (
                vm_contract
                and vm_contract.assigned_backend == "vm"
                and vm_workspaces_on_pod_network()
            ):
                raise _WorkspaceRecoveryRefusal("Job workspace authority is not ready")
            raise HTTPException(409, "Job workspace authority is not ready")
        workspace_decision = resolve_workspace_runtime(job, vm_mode=vm_provisioner.mode)
        assigned_backend = (
            workspace_decision.contract.assigned_backend
            if workspace_decision.contract is not None
            else None
        )
        if (
            workspace_decision.contract is None
            or assigned_backend not in {"sandbox", "vm"}
            or (assigned_backend == "vm" and not vm_workspaces_on_pod_network())
        ):
            raise HTTPException(
                status_code=409,
                detail="Job workspace contract is not stateless-compatible",
            )
        initial_runtime_digest = workspace_runtime_authority_digest(
            job, vm_mode=vm_provisioner.mode
        )
        if initial_runtime_digest is None:
            if assigned_backend == "vm":
                raise _WorkspaceRecoveryRefusal("Job workspace authority is not ready")
            raise HTTPException(
                status_code=409,
                detail="Job workspace authority is not ready",
            )

        # Inheriting scholar/critic/delegation jobs deliberately keep only a
        # snapshot of their parent's workspace in their own row. Resolve the
        # parent's live endpoint through the same helper as pinned dispatch so
        # a recreated shared pod cannot send this claimant to a stale address.
        (
            inherit_action,
            _,
        ) = await job_workspace_authority.resolve_subjob_inherited_workspace(
            job, dependencies=dependencies.job_workspace_authority_dependencies()
        )
        if inherit_action != "proceed":
            if assigned_backend == "vm":
                raise _WorkspaceRecoveryRefusal(
                    "Stateless worker parent workspace is not ready"
                )
            raise HTTPException(
                status_code=409,
                detail="Stateless worker parent workspace is not ready",
            )

        # Job context is only a lifecycle hint. Bind this claim to the exact
        # live Kubernetes objects and SSH host key, using the parent owner for
        # children that share its workspace. The attested endpoint replaces
        # any stale copied/persisted host in this in-memory bundle only.
        workspace_owner = stateless_worker_workspace_owner(job)
        attested_job = dict(job)
        raw_context = job.get("context") or {}
        if isinstance(raw_context, str):
            try:
                raw_context = json.loads(raw_context)
            except (json.JSONDecodeError, TypeError):
                raw_context = {}
        if not isinstance(raw_context, dict):
            raw_context = {}
        attested_context = copy.deepcopy(raw_context)

        raw_override = job.get("config_override")
        if isinstance(raw_override, str):
            try:
                raw_override = json.loads(raw_override)
            except (json.JSONDecodeError, TypeError):
                raw_override = None
        exact_override = (
            copy.deepcopy(raw_override) if isinstance(raw_override, dict) else None
        )

        if assigned_backend == "vm":
            vm_ctx = get_vm_context(job)
            if not (
                vm_ctx.get("status") == "ready"
                and vm_ctx.get("ssh_ready_source") == "provisioner_probe"
                and vm_ctx.get("identity_authenticated") is True
                and vm_ctx.get("identity_provision_generation")
                == vm_ctx.get("provision_generation")
                and bool(vm_ctx.get("active_pod_uid"))
                and bool(vm_ctx.get("ssh_host") or vm_ctx.get("pod_ip"))
                and bool(vm_ctx.get("ssh_host_key_fingerprint"))
            ):
                raise _WorkspaceRecoveryRefusal(
                    "Stateless worker VM workspace is not Kubernetes-ready"
                )
            initial_attestation = await _attest_recoverable_vm(
                workspace_owner,
                dependencies=dependencies.job_workspace_authority_dependencies(),
            )
            exact_vm_ctx = copy.deepcopy(vm_ctx)
            exact_vm_ctx.update(
                {
                    "status": "ready",
                    "ssh_host": initial_attestation.host,
                    "pod_ip": initial_attestation.pod_ip,
                    "ssh_port": initial_attestation.port,
                    "provision_generation": (initial_attestation.workspace_generation),
                    "active_pod_uid": initial_attestation.runtime_incarnation,
                    "ssh_host_key_fingerprint": (
                        initial_attestation.ssh_host_key_fingerprint
                    ),
                }
            )
            attested_context["vm"] = exact_vm_ctx
            attested_job["config_override"] = inject_vm_workspace_config(
                exact_override,
                exact_vm_ctx,
                replace_endpoint=True,
            )
        else:
            container_ctx = get_container_context(job)
            if not (
                container_ctx.get("status") == "ready"
                and container_ctx.get("provisioner") == "k8s"
                and bool(container_ctx.get("host") or container_ctx.get("pod_ip"))
            ):
                raise HTTPException(
                    status_code=409,
                    detail="Stateless worker workspace is not Kubernetes-ready",
                )
            initial_attestation = (
                await job_workspace_authority.attest_stateless_worker_workspace(
                    workspace_owner,
                    dependencies=dependencies.job_workspace_authority_dependencies(),
                )
            )
            exact_container_ctx = copy.deepcopy(container_ctx)
            exact_container_ctx.update(
                {
                    "status": "ready",
                    "provisioner": "k8s",
                    "host": initial_attestation.host,
                    "pod_ip": initial_attestation.pod_ip,
                    "port": initial_attestation.port,
                    "_runtime_incarnation": (initial_attestation.runtime_incarnation),
                }
            )
            attested_context["workspace_container"] = exact_container_ctx
            attested_job["config_override"] = inject_container_workspace_config(
                exact_override,
                exact_container_ctx,
                replace_endpoint=True,
            )
        attested_job["context"] = attested_context

        job_start = await job_start_bundle.build_job_start_request(
            attested_job,
            persist_dispatch_state=False,
            dependencies=dependencies.job_start_bundle_dependencies(),
        )
        if job_start is None:
            raise HTTPException(status_code=409, detail="Job bundle assembly refused")
        job_start = job_start.model_copy(
            update={
                "workspace_generation": (initial_attestation.workspace_generation),
                "workspace_runtime_incarnation": (
                    initial_attestation.runtime_incarnation
                ),
                "workspace_ssh_host_key_fingerprint": (
                    initial_attestation.ssh_host_key_fingerprint
                ),
                "workspace_owner_kind": workspace_owner.kind,
                "workspace_owner_id": workspace_owner.id,
            }
        )

        # Credential/config assembly above can take seconds, and the final
        # claimant observation below is itself a slow external wait. Run that
        # observation FIRST so the workspace/job/lease/repository revalidation
        # after it is fresh: an authority change landing mid-claimant-wait
        # must not ride through on checks that already ran. The residual
        # cross-system window (claimant replaced during the revalidation reads
        # themselves) is unavoidable — Kubernetes and Postgres are not one
        # atomic transaction — and is documented, not hidden; the short
        # transactional lease recheck before authorization narrows it to
        # milliseconds for the lease half.
        await _attest_worker_claimant(
            pod_name=pod_name, pod_uid=pod_uid, dependencies=dependencies
        )
        # Repeat the full control-plane + host-key attestation so a
        # Pod/PVC/Service replacement during assembly or the claimant wait
        # never crosses the response boundary under stale workspace authority.
        if assigned_backend == "vm":
            confirmed_attestation = await _attest_recoverable_vm(
                workspace_owner,
                dependencies=dependencies.job_workspace_authority_dependencies(),
            )
        else:
            confirmed_attestation = (
                await job_workspace_authority.attest_stateless_worker_workspace(
                    workspace_owner,
                    dependencies=dependencies.job_workspace_authority_dependencies(),
                )
            )
        if confirmed_attestation != initial_attestation:
            logger.warning(
                "Stateless worker workspace authority changed during bundle "
                "assembly for job %s",
                unit_id,
            )
            if assigned_backend == "vm":
                raise _WorkspaceRecoveryRefusal(
                    "Stateless worker workspace authority unavailable",
                    WorkspaceRecoveryCode.REPLACEMENT_OBSERVED,
                )
            raise HTTPException(
                status_code=409,
                detail="Stateless worker workspace authority unavailable",
            )

        # The Kubernetes objects are only half of the authority. Re-read the
        # job's assigned tier after slow credential/config assembly so a
        # concurrent control-plane transition cannot ship a sandbox bundle
        # under an obsolete contract. Opposite-tier diagnostic residue is not
        # part of this comparison and cannot perturb a valid sandbox claim.
        final_job = await dependencies.db.get_job(unit_id)
        final_action = "fail"
        if final_job is not None:
            (
                final_action,
                final_job,
                _final_reason,
            ) = await job_workspace_authority.prepare_job_workspace_runtime(
                final_job,
                dependencies=dependencies.job_workspace_authority_dependencies(),
            )
        try:
            final_contract = (
                resolve_workspace_contract(final_job) if final_job else None
            )
        except WorkspaceContractError:
            final_contract = None
        if (
            final_job is None
            or final_action != "proceed"
            or final_job.get("execution_lane") != LANE_STATELESS
            or final_contract != workspace_decision.contract
            or final_contract.assigned_backend != assigned_backend
            or workspace_runtime_authority_digest(
                final_job, vm_mode=vm_provisioner.mode
            )
            != initial_runtime_digest
        ):
            raise HTTPException(
                status_code=409,
                detail="Job workspace contract changed during bundle assembly",
            )

        # Recheck the exact lease after both slow operations so a stolen zombie
        # never receives either credentials or workspace authority.
        async with dependencies.db.acquire() as conn:
            lease_still_current = bool(
                await conn.fetchval(
                    "SELECT EXISTS (SELECT 1 FROM run_queue "
                    "WHERE unit_id = $1::uuid "
                    "AND unit_kind = 'worker_batch' "
                    "AND state = 'leased' AND lease_token = $2::bigint)",
                    unit_id,
                    lease_token,
                )
            )
        if not lease_still_current:
            raise HTTPException(status_code=403, detail="Lease validation failed")
        if not await dependencies.db.managed_repository_authorities_are_current(
            job_start.managed_repository_credentials
        ):
            raise HTTPException(
                status_code=409,
                detail="Job repository authority changed during bundle assembly",
            )

        context = job.get("context") or {}
        if isinstance(context, str):
            try:
                context = json.loads(context)
            except (TypeError, ValueError):
                context = {}
        batch_context = context.get("worker_batch") or {}
        if not isinstance(batch_context, dict):
            batch_context = {}

        def _positive_float(value: Any, fallback: float) -> float:
            try:
                parsed = float(value)
            except (TypeError, ValueError):
                return fallback
            return parsed if parsed > 0 else fallback

        min_wall_seconds = _positive_float(
            os.environ.get("WORKER_BATCH_MIN_WALL_SECONDS"), 300.0
        )
        target_wall_seconds = _positive_float(
            batch_context.get(
                "target_wall_seconds",
                context.get("worker_batch_target_wall_seconds", 300.0),
            ),
            300.0,
        )
        target_wall_seconds = max(target_wall_seconds, min_wall_seconds)
        iteration_cap_raw = batch_context.get(
            "iteration_cap", context.get("worker_batch_iteration_cap")
        )
        try:
            iteration_cap = int(iteration_cap_raw)
        except (TypeError, ValueError):
            iteration_cap = None
        if iteration_cap is not None and iteration_cap <= 0:
            iteration_cap = None

        # Execution evidence is independent of admission policy: a later flag
        # change must never turn an issued bundle into pre-bundle refund debt.
        # Authorization accounting stays unconditional with respect to
        # VM_WORKSPACE_RECOVERY_ENABLED.
        if dependencies.recovery_store is None:
            raise HTTPException(503, "Worker bundle authorization is unavailable")
        # The fresh claimant observation already ran above, before the final
        # workspace/job/lease/repository revalidation. Recheck the exact lease
        # once more under the short transactional boundary below before any
        # authorization writes: a successful observation before slow work must
        # not authorize a later stolen lease, and the transactional recheck
        # keeps that window to milliseconds. The Kubernetes/DB observations
        # are not one atomic transaction.
        async with dependencies.db.acquire() as conn:
            async with conn.transaction():
                await _validate_worker_lease(
                    conn,
                    unit_id=unit_id,
                    lease_token=lease_token,
                    pod_name=pod_name,
                    pod_uid=pod_uid,
                )
                authorized = await dependencies.recovery_store.record_bundle_authorized(
                    conn,
                    job_id=UUID(unit_id),
                    lease_token=lease_token,
                    authority_digest=initial_runtime_digest,
                )
                if not authorized:
                    raise HTTPException(403, "Lease validation failed")
        return {
            "unit_id": unit_id,
            "job_id": unit_id,
            "unit_kind": UNIT_KIND_WORKER_BATCH,
            "execution_lane": LANE_STATELESS,
            "job": job_start.model_dump(exclude_none=True),
            "batch": {
                "target_wall_seconds": target_wall_seconds,
                "iteration_cap": iteration_cap,
                "min_wall_seconds": min_wall_seconds,
            },
        }

    if row["unit_kind"] != UNIT_KIND_SESSION_TURN:
        raise HTTPException(status_code=409, detail="Unit kind carries no attach")
    claimant_pod = str(pod_name or "").strip()
    claimant_uid = str(pod_uid or "").strip()
    if (
        not claimant_pod
        or not claimant_uid
        or str(row["leased_by"] or "") != claimant_pod
    ):
        raise HTTPException(status_code=403, detail="Lease validation failed")
    initial_metadata = row["thread_metadata"]
    if isinstance(initial_metadata, str):
        try:
            initial_metadata = json.loads(initial_metadata)
        except (TypeError, ValueError):
            initial_metadata = None
    try:
        initial_stop_markers = stateless_stop_markers(initial_metadata)
    except RuntimeError:
        initial_stop_markers = STATELESS_STOP_KEYS
    if (
        str(row["thread_lane"] or "") != LANE_STATELESS
        or str(row["thread_status"] or "") not in {"created", "active", "awaiting_user"}
        or bool(initial_stop_markers)
    ):
        raise HTTPException(status_code=403, detail="Lease validation failed")
    _t_lease = time.perf_counter()

    # unit_id == thread_id for session_turn units.
    thread = await dependencies.db.get_thread(unit_id)
    if not thread or thread.get("execution_lane") != LANE_STATELESS:
        raise HTTPException(
            status_code=409, detail="Thread is not on the stateless lane"
        )

    # Defense at the credential/attach boundary for already-queued legacy
    # rows and direct DB/operator mistakes.  Public input/control admission
    # performs the same check before writing, but correctness cannot depend on
    # every producer having done so.
    require_stateless_workspace(thread)

    # Derive the assembly inputs exactly the way the resume dispatcher does
    # (resume_thread._reprovision): the stored override from metadata (secrets
    # stripped at rest) + in-flight credential re-injection. Secrets travel in
    # this response only — never persisted to the thread row (§5.6).
    md = thread.get("metadata") or {}
    if isinstance(md, str):
        try:
            md = json.loads(md)
        except (json.JSONDecodeError, TypeError):
            md = {}
    co = (md.get("config_override") or {}) if isinstance(md, dict) else {}
    pids = await dependencies.thread_project_ids(unit_id)
    include_kb_profile = await dependencies.thread_has_knowledge_scope(
        project_ids=pids,
        datasource_ids=(md.get("datasource_ids") if isinstance(md, dict) else None),
    )
    co = await dispatch_credentials.inject_thread_dispatch_credentials(
        co,
        user_id=str(thread["user_id"]) if thread.get("user_id") else None,
        project_id=str(thread["project_id"]) if thread.get("project_id") else None,
        include_kb_profile=include_kb_profile,
        dependencies=dependencies.dispatch_credential_dependencies(),
    )
    config_name = canonical_config_name(thread.get("config_name") or "session_base")
    _t_creds = time.perf_counter()

    # Same serialization the pinned sender takes (_send_session_attach): the
    # assembly must not race a live connector-selection update.
    async with dependencies.db.thread_datasource_lock(unit_id):
        attach = await session_attach_payload.assemble_session_attach_payload(
            unit_id,
            config_override=co,
            config_name=config_name,
            dependencies=dependencies.session_attach_payload_dependencies(),
        )
    if attach is None:
        # Generic by design — refusal reasons live in the server log only.
        raise HTTPException(status_code=409, detail="Attach assembly refused")

    # Credential/datasource assembly can block on connector locks and external
    # stores for longer than the queue lease.  Recheck immediately before the
    # response crosses the credential boundary, exactly like worker bundles:
    # token N may have been reaped/stolen while the payload was being built.
    from shared.session_retirement import (
        ClaimantAuthority,
        active_claim_authority,
        unresolved_claim_losses,
    )

    lease_still_current = False
    async with dependencies.db.acquire() as conn:
        async with conn.transaction():
            final_thread = await conn.fetchrow(
                "SELECT status::text AS status, execution_lane, metadata "
                "FROM threads WHERE id = $1::uuid FOR UPDATE",
                unit_id,
            )
            if final_thread is not None:
                _final_backend, final_class_or_workspace_refusal = (
                    stateless_session_workspace_check(final_thread)
                )
                final_metadata = final_thread["metadata"]
                if isinstance(final_metadata, str):
                    try:
                        final_metadata = json.loads(final_metadata)
                    except (TypeError, ValueError):
                        final_metadata = None
                try:
                    losses = unresolved_claim_losses(final_metadata)
                    active_claim = active_claim_authority(final_metadata)
                    final_stop_markers = stateless_stop_markers(final_metadata)
                except RuntimeError:
                    losses = {0: ClaimantAuthority("invalid", "invalid")}
                    active_claim = None
                    final_stop_markers = STATELESS_STOP_KEYS
                final_queue = await conn.fetchrow(
                    "SELECT state, lease_token, leased_by FROM run_queue "
                    "WHERE unit_id = $1::uuid AND unit_kind = 'session_turn' "
                    "FOR UPDATE",
                    unit_id,
                )
                expected_authority = ClaimantAuthority(claimant_pod, claimant_uid)
                active_compatible = active_claim is None or (
                    int(active_claim[0]) < int(lease_token)
                    or (
                        int(active_claim[0]) == int(lease_token)
                        and active_claim[1] == expected_authority
                    )
                )
                lease_still_current = bool(
                    str(final_thread["execution_lane"] or "") == LANE_STATELESS
                    and str(final_thread["status"] or "")
                    in {"created", "active", "awaiting_user"}
                    and final_class_or_workspace_refusal is None
                    and isinstance(final_metadata, dict)
                    and not final_stop_markers
                    and not losses
                    and final_metadata.get("protected_cloud") in (None, False)
                    and active_compatible
                    and final_queue is not None
                    and str(final_queue["state"] or "") == "leased"
                    and int(final_queue["lease_token"] or 0) == int(lease_token)
                    and str(final_queue["leased_by"] or "") == claimant_pod
                )
                if lease_still_current:
                    stamped = await conn.fetchval(
                        """
                        UPDATE threads
                        SET metadata = jsonb_set(
                            COALESCE(metadata, '{}'::jsonb),
                            '{_stateless_active_claim}',
                            jsonb_build_object(
                                'lease_token', $2::bigint,
                                'pod', $3::text,
                                'pod_uid', $4::text,
                                'credential_bound_at', to_jsonb(now())
                            ),
                            true
                        )
                        WHERE id = $1::uuid
                          AND execution_lane = 'stateless'
                          AND status IN
                              ('created', 'active', 'awaiting_user')
                          AND NOT (COALESCE(metadata, '{}'::jsonb)
                                   ? '_stateless_workspace_retirement_pending')
                          AND NOT (COALESCE(metadata, '{}'::jsonb)
                                   ? '_stateless_claim_retirement')
                          AND NOT (COALESCE(metadata, '{}'::jsonb)
                                   ? '_stateless_claim_losses')
                          AND NOT (COALESCE(metadata, '{}'::jsonb)
                                   ? '_stateless_claim_loss_hold')
                          AND COALESCE(metadata->'protected_cloud', 'false'::jsonb)
                              = 'false'::jsonb
                        RETURNING id
                        """,
                        unit_id,
                        int(lease_token),
                        claimant_pod,
                        claimant_uid,
                    )
                    lease_still_current = bool(stamped)
    if not lease_still_current:
        raise HTTPException(status_code=403, detail="Lease validation failed")
    _t_end = time.perf_counter()
    logger.info(
        "claim-bundle timing: unit=%s lease=%.3fs creds=%.3fs assemble=%.3fs "
        "total=%.3fs",
        unit_id,
        _t_lease - _t_start,
        _t_creds - _t_lease,
        _t_end - _t_creds,
        _t_end - _t_start,
    )

    return {
        "unit_id": unit_id,
        "thread_id": unit_id,
        "unit_kind": UNIT_KIND_SESSION_TURN,
        "execution_lane": LANE_STATELESS,
        "watermarks": {
            "input_seq": row["input_seq"],
            "consumed_seq": row["consumed_seq"],
        },
        "attach": attach,
    }
