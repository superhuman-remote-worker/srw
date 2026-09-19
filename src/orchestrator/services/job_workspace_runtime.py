"""Job workspace context readers, tier predicates and endpoint injection.

Extracted verbatim from ``orchestrator.main`` (R1.B05 lane J, census group
``R_WORKSPACE``). Everything here answers one of three questions about a *job*
row: what live runtime does its context hold, which tier does the server-owned
contract assign it, and what SSH endpoint should the dispatched bundle carry.

``shared.workspace_contract`` stays the authority for the tier contract and the
runtime decision — every predicate below asks it rather than re-deriving a tier
from ``config_override``. ``services.job_workspace_adoption`` stays the
authority for legacy Kubernetes adoption, and
``services.job_workspace_authority`` owns attestation and the pre-delivery
recheck.

The VM provisioner mode arrives as a callable (``vm_mode``) rather than a value,
matching ``services.job_projection``: the application rebinds/patches the
provisioner singleton, and a captured mode would freeze the tier decision at
factory time.
"""

from __future__ import annotations

import copy
import json
import os
from dataclasses import dataclass
from typing import Any, Callable, Literal, Mapping, Optional, Protocol

from fastapi import HTTPException

from orchestrator.services.workspace_lifecycle import WorkspaceOwner
from shared.backend_kinds import LITE_BACKENDS
from shared.workspace_contract import (
    WorkspaceContractError,
    resolve_workspace_contract,
    resolve_workspace_runtime,
)


ExecutionLane = Literal["pinned", "stateless"]

# Job context key holding each remote tier's live workspace, by tier name.
WORKSPACE_CONTEXT_KEYS = {"vm": "vm", "sandbox": "workspace_container"}


class JobWorkspaceRuntimeStore(Protocol):
    async def update_job_status(
        self, job_id: str, *, status: str, error_message: str | None = ...
    ) -> Any: ...


class WorkspaceProvisionerCapabilities(Protocol):
    @property
    def is_available(self) -> bool: ...

    @property
    def in_cluster(self) -> bool: ...


@dataclass(frozen=True)
class JobWorkspaceRuntimeDependencies:
    """Per-invocation collaborators for job workspace tier resolution."""

    store: JobWorkspaceRuntimeStore
    vm_mode: Callable[[], Any]
    workspace_provisioner: WorkspaceProvisionerCapabilities
    vm_workspaces_on_pod_network: Callable[[], bool]
    stateless_worker_enabled: Callable[[], bool]
    backend_from_override: Callable[[Any], Optional[str]]


# =============================================================================
# Job context readers (pure)
# =============================================================================


def get_vm_context(job: dict) -> dict:
    """Extract the vm sub-dict from job context."""
    ctx = job.get("context") or {}
    if isinstance(ctx, str):
        try:
            ctx = json.loads(ctx)
        except (json.JSONDecodeError, TypeError):
            ctx = {}
    return ctx.get("vm", {})


def get_container_context(job: dict) -> dict:
    """Extract the workspace_container sub-dict from job context."""
    ctx = job.get("context") or {}
    if isinstance(ctx, str):
        try:
            ctx = json.loads(ctx)
        except (json.JSONDecodeError, TypeError):
            ctx = {}
    return ctx.get("workspace_container", {})


def get_infra_transient_context(job: dict) -> dict:
    """Extract the infra_transient sub-dict from job context.

    The attempt counter lives in ``context``, NOT in ``freeze_data``: the
    sweeper clears ``freeze_data`` to make the job dispatchable again, so a
    counter kept there would reset to zero on every retry and the give-up
    ceiling would never be reached. Mirrors ``context.llm_outage``.
    """
    ctx = job.get("context") or {}
    if isinstance(ctx, str):
        try:
            ctx = json.loads(ctx)
        except (json.JSONDecodeError, TypeError):
            ctx = {}
    value = ctx.get("infra_transient")
    return value if isinstance(value, dict) else {}


def stateless_worker_workspace_owner(job: dict) -> WorkspaceOwner:
    """Resolve whose Kubernetes workspace a stateless worker must attest."""

    ctx = job.get("context") or {}
    if isinstance(ctx, str):
        try:
            ctx = json.loads(ctx)
        except (json.JSONDecodeError, TypeError):
            ctx = {}
    parent_id = job.get("parent_job_id")
    if parent_id and ctx.get("inherits_parent_workspace"):
        return WorkspaceOwner.job(str(parent_id))
    return WorkspaceOwner.job(str(job["id"]))


def scholar_provision_parent_id(job: dict) -> str | None:
    """Parent id a provisioning-scholar subjob should provision the shared,
    parent-owned workspace under, or None for a normal job.

    A scholar spawned before its parent had any workspace carries
    ``context.provisions_parent_workspace = <parentId>`` (stamped by
    ``_spawn_scholar_subjob``). It provisions the ONE pod ``workspace-<parentId>``
    under the parent's identity and rides it, rather than self-provisioning a
    throwaway pod (Phase 1,
    knowledge-base/knowledge/issues/scholar_selfprovisioned_workspace_misclassified_as_inherited.md).
    """
    ctx = job.get("context") or {}
    if isinstance(ctx, str):
        try:
            ctx = json.loads(ctx)
        except (json.JSONDecodeError, ValueError):
            return None
    pid = ctx.get("provisions_parent_workspace")
    return str(pid) if pid else None


# =============================================================================
# Tier predicates
# =============================================================================


def job_needs_vm(job: dict) -> bool:
    """Whether the authoritative contract assigns the VM tier."""

    try:
        return resolve_workspace_contract(job).assigned_backend == "vm"
    except WorkspaceContractError:
        # Malformed/ambiguous jobs are refused by the bundle resolver. Never
        # guess a tier here merely because one runtime happens to be ready.
        return False


def job_needs_sandbox(
    job: dict, *, dependencies: JobWorkspaceRuntimeDependencies
) -> bool:
    """Check if a job needs a sandbox workspace container.

    Returns True if:
    - config_override.workspace.backend == "sandbox" (or legacy "container"), OR
    - backend is not explicitly set to "vm" AND a workspace
      provisioner is available (k8s ContainerProvisioner OR DockerProvisioner).

    Returns False if the job already has a ready VM or container inherited
    from a parent job (worktree sharing — no new container needed).
    """
    try:
        contract = resolve_workspace_contract(job)
    except WorkspaceContractError:
        return False
    if contract.assigned_backend != "sandbox":
        return False
    decision = resolve_workspace_runtime(job, vm_mode=dependencies.vm_mode())
    # An opposite-tier VM must never suppress sandbox provisioning.
    return decision.effective_backend != "sandbox"


def scholar_should_provision_parent_container(
    config_override: Any, *, dependencies: JobWorkspaceRuntimeDependencies
) -> bool:
    """True if a scholar spawned with no parent workspace should provision the
    parent's SHARED container workspace (Phase 1) rather than self-provision.

    Backend gate only: container/sandbox (or unset → default sandbox) qualifies;
    VM/remote and lite (virtual/none) parents keep today's behavior. The dispatch
    seam additionally enforces a k8s in-cluster provisioner before acting on the
    marker, so a non-k8s deployment falls through to the self-provision path.
    """
    backend = dependencies.backend_from_override(config_override)
    if backend in ("vm", "remote"):
        return False
    if backend in LITE_BACKENDS:
        return False
    return True


def resolve_requested_job_execution_lane(
    requested_lane: ExecutionLane | None,
    *,
    default_stateless: bool,
    needs_vm: bool,
    needs_sandbox: bool,
    dependencies: JobWorkspaceRuntimeDependencies,
) -> ExecutionLane | None:
    """Apply the default-off, pod-network-workspace worker admission gate.

    ``None`` is preserved unless a capable omitted root opts into defaulting,
    so Postgres can still distinguish authoritative child-lane inheritance.
    """
    container_provisioner = dependencies.workspace_provisioner
    same_cluster_vm = needs_vm and dependencies.vm_workspaces_on_pod_network()
    if needs_vm and not same_cluster_vm:
        # External VMs still require the registered agent's mesh sidecar. The
        # shared executor Deployment deliberately has no tailnet identity.
        return "pinned"
    if requested_lane is None and default_stateless:
        if dependencies.stateless_worker_enabled() and (
            same_cluster_vm
            or (
                container_provisioner.is_available
                and container_provisioner.in_cluster
                and needs_sandbox
            )
        ):
            return "stateless"
        return None
    if requested_lane != "stateless":
        return requested_lane
    if not dependencies.stateless_worker_enabled():
        raise HTTPException(
            status_code=409, detail="Stateless worker admission is disabled"
        )
    if not same_cluster_vm and not (
        container_provisioner.is_available and container_provisioner.in_cluster
    ):
        raise HTTPException(
            status_code=503,
            detail=(
                "Stateless workers require an in-cluster Kubernetes "
                "workspace provisioner"
            ),
        )
    if not (needs_sandbox or same_cluster_vm):
        raise HTTPException(
            status_code=422,
            detail=(
                "Stateless workers currently require a Kubernetes sandbox or "
                "same-cluster VM workspace"
            ),
        )
    return "stateless"


def resume_missing_workspace(
    job: dict, *, dependencies: JobWorkspaceRuntimeDependencies
) -> Optional[str]:
    """Which remote workspace a resume would ship without an address, if any.

    Returns ``'vm'`` / ``'sandbox'`` when the job's backend needs a remote
    workspace but its context holds no live one, else ``None``.

    ``_resume_job_on_agent`` injects ``workspace.remote`` only when the context
    says the workspace is ready, and neither of its two injection blocks has an
    else. So resuming a job whose workspace never came up — or was reaped —
    silently skips injection while config_override still names the backend, and
    the agent dies at ``init_workspace`` with "no workspace.remote config was
    provided" (src/agent.py:1896). Job 4435994d hit exactly this.

    Nothing in the resume path provisions; only the dispatcher does. So callers
    must hand such a job to the dispatcher rather than push it at an agent.
    This is the resume-side sibling of the dispatch backstop in
    ``_dispatch_job_to_agent``, which *fails* the job instead — correct there,
    because the dispatcher was supposed to have resolved the workspace already,
    but wrong here: an explicit Resume means "re-provision it".

    Uses the same ``job_needs_vm`` / ``job_needs_sandbox`` predicates the
    dispatcher uses to decide what to provision, so resume and dispatch agree
    on what a job needs. Keep the readiness conditions in step with the two
    injection blocks in ``_resume_job_on_agent``.
    """

    try:
        contract = resolve_workspace_contract(job)
    except WorkspaceContractError:
        # No tier can be shed safely when authority itself is ambiguous. The
        # shared bundle resolver will refuse it rather than choosing one.
        return None
    if contract.assigned_backend in LITE_BACKENDS:
        return None
    decision = resolve_workspace_runtime(job, vm_mode=dependencies.vm_mode())
    return None if decision.ready else contract.assigned_backend


# =============================================================================
# Workspace endpoint injection
# =============================================================================


def container_ssh_key_path(container_ctx: dict) -> str:
    """Resolve the private-key path shipped to a managed sandbox worker.

    The key itself is never persisted in job state. The path must therefore be
    reconstructed on every start and resume, using the same rules in both
    paths. Docker and Kubernetes mount the shared key at different defaults.
    """
    override = os.environ.get("SSH_KEY_PATH", "").strip()
    if override:
        return override
    if container_ctx.get("provisioner") == "docker":
        return "/run/secrets/ssh/id_ed25519"
    return "/run/secrets/vm-ssh-key"


def inject_container_workspace_config(
    config_override: dict | None,
    container_ctx: dict,
    *,
    replace_endpoint: bool = False,
) -> dict:
    """Inject a complete managed-sandbox SSH configuration.

    ``replace_endpoint`` is used on resume because a recreated pod may have a
    new address. Managed username/key fields are always refreshed because they
    are in-flight deployment configuration and are deliberately not written
    back to ``jobs.config_override``.
    """
    container_host = container_ctx.get("host") or container_ctx.get("pod_ip")
    if container_ctx.get("status") != "ready" or not container_host:
        return config_override or {}

    config_override = config_override or {}
    workspace = config_override.setdefault("workspace", {})
    workspace["backend"] = "sandbox"
    remote = workspace.setdefault("remote", {})

    if replace_endpoint or not remote.get("host"):
        remote["host"] = container_host
    if replace_endpoint or not remote.get("port"):
        remote["port"] = container_ctx.get("port", 22)
    # These are properties of the orchestrator-managed sandbox image and agent
    # deployment, not user/job settings. Always refresh them so a stale
    # persisted remote block cannot point a resumed worker at an obsolete mount.
    remote["username"] = "agent-host"
    remote["key_path"] = container_ssh_key_path(container_ctx)
    if not remote.get("workspace_path"):
        remote["workspace_path"] = "/home/agent-host/workspace"

    # Managed sandboxes freeze on sudo so an operator can approve a VM upgrade.
    config_override.setdefault("shell", {}).setdefault("sudo_action", "freeze")
    return config_override


def inject_vm_workspace_config(
    config_override: dict | None,
    vm_ctx: dict,
    *,
    replace_endpoint: bool = False,
) -> dict:
    """Inject only the authoritative VM endpoint into a worker config."""

    if vm_ctx.get("status") != "ready" or not vm_ctx.get("ssh_host"):
        return config_override or {}
    config_override = config_override or {}
    workspace = config_override.setdefault("workspace", {})
    workspace["backend"] = "vm"
    remote = workspace.setdefault("remote", {})
    if replace_endpoint or not remote.get("host"):
        remote["host"] = vm_ctx["ssh_host"]
    if replace_endpoint or not remote.get("port"):
        remote["port"] = vm_ctx.get("ssh_port", 22)
    remote.setdefault("username", "agent-host")
    remote.setdefault("key_path", "/run/secrets/vm-ssh-key")
    remote.setdefault("workspace_path", "/home/agent-host/workspace")
    remote.setdefault(
        "connect_timeout", int(os.environ.get("VM_REMOTE_CONNECT_TIMEOUT_S", "10"))
    )
    remote.setdefault(
        "max_retries", int(os.environ.get("VM_REMOTE_CONNECT_MAX_RETRIES", "6"))
    )
    remote.setdefault("retry_timeouts_as_booting", True)
    config_override.setdefault("shell", {})["sudo_action"] = "allow"
    return config_override


def inject_matching_workspace_config(
    job: Mapping[str, Any],
    config_override: dict | None,
    *,
    replace_endpoint: bool = False,
    dependencies: JobWorkspaceRuntimeDependencies,
) -> tuple[dict, Any]:
    """Apply exactly one runtime selected by the server-owned tier contract."""

    decision = resolve_workspace_runtime(job, vm_mode=dependencies.vm_mode())
    config = copy.deepcopy(config_override or {})
    if not decision.ready:
        return config, decision
    if decision.effective_backend == "vm":
        config = inject_vm_workspace_config(
            config,
            get_vm_context(dict(job)),
            replace_endpoint=replace_endpoint,
        )
    elif decision.effective_backend == "sandbox":
        config = inject_container_workspace_config(
            config,
            get_container_context(dict(job)),
            replace_endpoint=replace_endpoint,
        )
    return config, decision


def apply_sticky_sudo_denial(job: dict, config_override: dict | None) -> dict | None:
    """Flip the agent's sudo gate to a reasoned block when the job carries a
    sudo/VM-upgrade denial (``context.sudo_denial``, written by
    ``_resume_job_without_vm_internal``).

    Without this, the agent resuming from its checkpoint replays the gated
    command, hits ``sudo_action="freeze"`` again, and re-freezes into a brand
    new approval loop the operator just declined. Applies to non-VM backends
    only — a VM owns its own sudo gate, and reaching one means the upgrade was
    approved after all.

    Returns the (possibly newly created) config_override.
    """
    ctx = job.get("context") or {}
    if isinstance(ctx, str):
        try:
            ctx = json.loads(ctx)
        except (json.JSONDecodeError, TypeError):
            ctx = {}
    denial = ctx.get("sudo_denial")
    if not isinstance(denial, dict):
        return config_override
    if ((config_override or {}).get("workspace") or {}).get("backend") == "vm":
        return config_override
    config_override = config_override or {}
    shell_cfg = config_override.setdefault("shell", {})
    shell_cfg["sudo_action"] = "block"
    decided_by = denial.get("decided_by") or "the operator"
    reason = denial.get("reason") or ""
    shell_cfg["sudo_block_message"] = (
        "Command blocked: sudo was "
        + ("denied" if denial.get("denied", True) else "waived (resume without VM)")
        + f" for this job by {decided_by}"
        + (f" — {reason}" if reason else "")
        + ". Do not re-attempt sudo; use a rootless alternative or record the "
        "limitation in your results."
    )
    return config_override


async def fail_vm_parked_job(
    job_id: str, vm_error: str, *, dependencies: JobWorkspaceRuntimeDependencies
) -> None:
    """Fail a job whose VM provisioning parked terminally.

    A parked VM context alone leaves the job in 'created' with nothing
    scheduled to ever change its state — an invisible wedge that stalls any
    loop whose current_job never turns terminal. Failing the job makes the
    park visible (cockpit/API) and lets loop failure handling advance. See
    knowledge-base/knowledge/issues/vm_ssh_readiness_probe_unroutable_from_orchestrator.md.
    """
    await dependencies.store.update_job_status(
        job_id,
        status="failed",
        error_message=(
            f"VM provisioning failed: {vm_error}. "
            "Use Resume to retry through guarded workspace cleanup. "
            "If retirement proof is unavailable, operator recovery is required."
        ),
    )
