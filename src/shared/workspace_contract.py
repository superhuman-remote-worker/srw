"""Server-owned workspace-tier contracts and runtime observations.

The persisted contract answers *what tier this job is allowed to run on*.
Provisioner context answers *what tier is currently available*.  Keeping those
questions separate prevents a ready sandbox from satisfying a VM assignment (or
vice versa) and gives every dispatch surface one order-independent decision.

This module is deliberately pure.  Kubernetes/VM endpoint attestation remains
with the provisioners; the contract resolver consumes only their durable,
server-written summaries and never returns transport coordinates.
"""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from typing import Any, Mapping
from uuid import UUID

from shared.workspace_defaults import default_backend

WORKSPACE_CONTRACT_CONTEXT_KEY = "_workspace_contract"
WORKSPACE_DISPATCH_AUTHORITY_CONTEXT_KEY = "_workspace_dispatch_authority"
WORKSPACE_RUNTIME_CONTEXT_KEY = "workspace_runtime"
LEGACY_K8S_RUNTIME_ADOPTION_KEY = "_legacy_k8s_runtime_adoption"
WORKSPACE_CONTRACT_VERSION = 1

CANONICAL_WORKSPACE_BACKENDS = frozenset({"sandbox", "vm", "virtual", "none"})
REMOTE_WORKSPACE_BACKENDS = frozenset({"sandbox", "vm"})
_ALIASES = {"container": "sandbox", "remote": "vm"}
_VM_MODES = frozenset({"off", "same-cluster", "external"})


def vm_mode_from_env() -> str:
    """Return the normalized deployment VM mode for impure API projections.

    Runtime resolution remains pure: callers that project persisted rows read
    the process configuration here and pass the result explicitly.
    """

    mode = os.getenv("VM_MODE")
    if mode is None:
        return "off"
    normalized = mode.strip().lower()
    return normalized if normalized in _VM_MODES else "external"


def stateless_worker_backend_admissible(backend: object, *, vm_mode: str) -> bool:
    """One truth for which workspace tiers the stateless worker lane serves.

    Used by BOTH admission twins — the claim-side queue eligibility check and
    the agent's workspace-creation guard — so they can never drift apart
    again (the enqueue CAS admitting VM while the claim CAS rejected it was
    an unbounded livelock). Sandbox is always admissible; VM only on the
    pod network (``vm_mode == "same-cluster"``); lite tiers never, on this
    lane. Aliases normalize; anything unrecognized is inadmissible.
    """

    if not isinstance(backend, str):
        return False
    canonical = _ALIASES.get(backend.strip().lower(), backend.strip().lower())
    if canonical == "sandbox":
        return True
    return canonical == "vm" and vm_mode == "same-cluster"


class WorkspaceContractError(ValueError):
    """The authoritative workspace contract is malformed or contradictory."""

    def __init__(self, code: str, detail: str):
        super().__init__(detail)
        self.code = code
        self.detail = detail


@dataclass(frozen=True, slots=True)
class WorkspaceContract:
    requested_backend: str | None
    assigned_backend: str
    assignment_source: str
    version: int = WORKSPACE_CONTRACT_VERSION
    compatibility: bool = False

    def to_context(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "requested_backend": self.requested_backend,
            "assigned_backend": self.assigned_backend,
            "assignment_source": self.assignment_source,
        }


@dataclass(frozen=True, slots=True)
class WorkspaceRuntimeDecision:
    contract: WorkspaceContract | None
    effective_backend: str | None
    state: str
    reason: str | None = None
    selected_context_key: str | None = None
    stale_backend: str | None = None

    @property
    def ready(self) -> bool:
        return self.state == "ready"

    def safe_projection(self) -> dict[str, Any]:
        contract = self.contract
        return {
            "requested_backend": (
                contract.requested_backend if contract is not None else None
            ),
            "assigned_backend": (
                contract.assigned_backend if contract is not None else None
            ),
            "effective_backend": self.effective_backend,
            "assignment_source": (
                contract.assignment_source if contract is not None else None
            ),
            "state": self.state,
            "failure": self.reason,
            "stale_backend": self.stale_backend,
            "compatibility_derived": bool(contract and contract.compatibility),
        }


def _object(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (TypeError, ValueError):
            return {}
        return dict(parsed) if isinstance(parsed, Mapping) else {}
    return {}


def normalize_workspace_backend(
    value: Any,
    *,
    default: str | None = None,
    field: str = "workspace.backend",
) -> str | None:
    """Normalize the two legacy aliases and reject every unknown tier."""

    if value is None:
        return default
    if not isinstance(value, str) or not value.strip():
        raise WorkspaceContractError(
            "invalid_workspace_backend", f"{field} must name a workspace tier"
        )
    normalized = _ALIASES.get(value.strip().lower(), value.strip().lower())
    if normalized not in CANONICAL_WORKSPACE_BACKENDS:
        raise WorkspaceContractError(
            "invalid_workspace_backend",
            f"{field} must be one of {sorted(CANONICAL_WORKSPACE_BACKENDS)}",
        )
    return normalized


def configured_workspace_backend(config_override: Any) -> str | None:
    config = _object(config_override)
    workspace = _object(config.get("workspace"))
    if "backend" not in workspace:
        return None
    return normalize_workspace_backend(workspace.get("backend"))


def canonicalize_workspace_config(
    config_override: Any, *, assigned_backend: str
) -> dict[str, Any]:
    """Return a copy whose backend is the canonical authoritative assignment."""

    config = _object(config_override)
    workspace = _object(config.get("workspace"))
    # Remote endpoints are effective-runtime authority. They are injected
    # from the selected provisioner immediately before dispatch and can never
    # be supplied by a job creator, even an internal/model-authored caller.
    workspace.pop("remote", None)
    workspace["backend"] = normalize_workspace_backend(assigned_backend)
    config["workspace"] = workspace
    return config


def build_workspace_contract(
    config_override: Any,
    *,
    requested_backend: Any = None,
    assignment_source: str | None = None,
) -> WorkspaceContract:
    """Build the one creation-time contract from trusted server inputs."""

    assigned = configured_workspace_backend(config_override) or default_backend(
        "worker"
    )
    requested = normalize_workspace_backend(
        requested_backend, field="requested workspace.backend"
    )
    source = str(assignment_source or "resolved_config").strip() or "resolved_config"
    return WorkspaceContract(
        requested_backend=requested,
        assigned_backend=assigned,
        assignment_source=source,
    )


def strip_and_stamp_workspace_creation(
    context: Any,
    config_override: Any,
    *,
    requested_backend: Any = None,
    assignment_source: str | None = None,
) -> tuple[dict[str, Any], dict[str, Any], WorkspaceContract]:
    """Strip runtime authority and stamp the canonical creation-time contract."""

    clean_context = _object(context)
    clean_context.pop(WORKSPACE_CONTRACT_CONTEXT_KEY, None)
    clean_context.pop(WORKSPACE_DISPATCH_AUTHORITY_CONTEXT_KEY, None)
    clean_context.pop(WORKSPACE_RUNTIME_CONTEXT_KEY, None)
    # Historical callers used this unnamespaced hint.  It was never runtime
    # attestation and must not survive a creation boundary.
    clean_context.pop("workspace_backend", None)
    clean_context.pop("vm", None)
    clean_context.pop("workspace_container", None)

    contract = build_workspace_contract(
        config_override,
        requested_backend=requested_backend,
        assignment_source=assignment_source,
    )
    clean_config = canonicalize_workspace_config(
        config_override, assigned_backend=contract.assigned_backend
    )
    clean_context[WORKSPACE_CONTRACT_CONTEXT_KEY] = contract.to_context()
    return clean_context, clean_config, contract


def _uuid(value: Any) -> str | None:
    try:
        parsed = UUID(str(value))
    except (TypeError, ValueError):
        return None
    return str(parsed)


def _runtime_is_authoritative(
    backend: str, runtime: Mapping[str, Any], *, vm_mode: str = "external"
) -> bool:
    """Recognize only provisioner-written runtime incarnation evidence.

    The contract stamp establishes the assigned tier; it does not authenticate
    an endpoint stored beside it.  New and compatibility rows therefore use
    the same provenance test before a ready runtime can become effective.
    """

    if backend == "vm":
        # KubeVirt/NATS generations are server-minted.  The development Docker
        # VM pool has no generation, but only its server-side allocator writes
        # the provisioner marker.
        generation_is_authoritative = bool(
            _uuid(runtime.get("provision_generation"))
            or runtime.get("provisioner") == "docker"
        )
        if vm_mode == "same-cluster":
            return generation_is_authoritative and (
                runtime.get("ssh_ready_source") == "provisioner_probe"
            )
        return generation_is_authoritative
    if backend == "sandbox":
        provisioner = runtime.get("provisioner")
        if provisioner == "docker":
            return bool(
                _uuid(runtime.get("_docker_workspace_lease_id"))
                and runtime.get("_docker_workspace_attested") is True
            )
        # Managed Kubernetes contexts carry the immutable Pod UID.  Older
        # unlabelled/UID-less snapshots are intentionally not execution proof.
        return bool(_uuid(runtime.get("_runtime_incarnation")))
    return False


def resolve_workspace_contract(job: Mapping[str, Any]) -> WorkspaceContract:
    """Resolve a stamped contract, or derive a conservative legacy contract."""

    context = _object(job.get("context"))
    raw = context.get(WORKSPACE_CONTRACT_CONTEXT_KEY)
    configured = configured_workspace_backend(job.get("config_override"))
    if isinstance(raw, Mapping):
        if raw.get("version") != WORKSPACE_CONTRACT_VERSION:
            raise WorkspaceContractError(
                "workspace_contract_version",
                "Workspace contract version is unsupported",
            )
        assigned = normalize_workspace_backend(
            raw.get("assigned_backend"), field="assigned workspace backend"
        )
        requested = normalize_workspace_backend(
            raw.get("requested_backend"), field="requested workspace backend"
        )
        source = raw.get("assignment_source")
        if not isinstance(source, str) or not source.strip():
            raise WorkspaceContractError(
                "workspace_contract_invalid", "Workspace assignment source is missing"
            )
        if configured is not None and configured != assigned:
            raise WorkspaceContractError(
                "workspace_contract_config_mismatch",
                "Persisted workspace assignment disagrees with job configuration",
            )
        return WorkspaceContract(requested, assigned, source.strip())

    # Compatibility is deliberately based on persisted configuration, never on
    # context.workspace_backend or whichever runtime happens to be ready.
    assigned = configured or "sandbox"
    vm = _object(context.get("vm"))
    container = _object(context.get("workspace_container"))
    legacy_hint = None
    if "workspace_backend" in context:
        legacy_hint = normalize_workspace_backend(
            context.get("workspace_backend"), field="legacy workspace backend"
        )
        if configured is not None and legacy_hint != configured:
            raise WorkspaceContractError(
                "legacy_workspace_ambiguous",
                "Legacy workspace request disagrees with job configuration",
            )
        if configured is None:
            matching_runtime = vm if legacy_hint == "vm" else container
            if legacy_hint in REMOTE_WORKSPACE_BACKENDS and not (
                _runtime_is_authoritative(legacy_hint, matching_runtime)
            ):
                raise WorkspaceContractError(
                    "legacy_workspace_ambiguous",
                    "Legacy workspace request has no authoritative provenance",
                )
            assigned = legacy_hint
    if configured is None and vm.get("requested") is True:
        if _runtime_is_authoritative("vm", vm):
            assigned = "vm"
        else:
            raise WorkspaceContractError(
                "legacy_workspace_ambiguous",
                "Legacy VM request has no authoritative provenance",
            )
    elif configured is not None and vm.get("requested") is True and assigned != "vm":
        raise WorkspaceContractError(
            "legacy_workspace_ambiguous",
            "Legacy VM request disagrees with job configuration",
        )
    if configured is None and vm and container:
        raise WorkspaceContractError(
            "legacy_workspace_ambiguous",
            "Legacy job has conflicting workspace histories",
        )
    matching_runtime = vm if assigned == "vm" else container
    matching_ready = bool(
        matching_runtime.get("status") == "ready"
        and (
            matching_runtime.get("ssh_host")
            if assigned == "vm"
            else matching_runtime.get("host") or matching_runtime.get("pod_ip")
        )
    )
    if (
        assigned in REMOTE_WORKSPACE_BACKENDS
        and matching_ready
        and not _runtime_is_authoritative(assigned, matching_runtime)
    ):
        raise WorkspaceContractError(
            "legacy_workspace_ambiguous",
            "Legacy ready workspace has no authoritative provenance",
        )
    return WorkspaceContract(
        requested_backend=assigned,
        assigned_backend=assigned,
        assignment_source="legacy_compatibility",
        compatibility=True,
    )


def resolve_workspace_runtime(
    job: Mapping[str, Any], *, vm_mode: str = "external"
) -> WorkspaceRuntimeDecision:
    """Return the only tier that may enter a worker bundle for ``job``."""

    try:
        contract = resolve_workspace_contract(job)
    except WorkspaceContractError as exc:
        return WorkspaceRuntimeDecision(None, None, "invalid", exc.code)

    assigned = contract.assigned_backend
    if assigned in {"virtual", "none"}:
        return WorkspaceRuntimeDecision(contract, assigned, "ready")

    context = _object(job.get("context"))
    vm = _object(context.get("vm"))
    container = _object(context.get("workspace_container"))
    vm_endpoint_ready = bool(vm.get("status") == "ready" and vm.get("ssh_host"))
    container_endpoint_ready = bool(
        container.get("status") == "ready"
        and (container.get("host") or container.get("pod_ip"))
    )
    vm_ready = vm_endpoint_ready and _runtime_is_authoritative(
        "vm", vm, vm_mode=vm_mode
    )
    container_ready = container_endpoint_ready and _runtime_is_authoritative(
        "sandbox", container
    )

    matching_ready = vm_ready if assigned == "vm" else container_ready
    matching_endpoint_ready = (
        vm_endpoint_ready if assigned == "vm" else container_endpoint_ready
    )
    opposite_ready = container_ready if assigned == "vm" else vm_ready
    matching = vm if assigned == "vm" else container
    opposite = "sandbox" if assigned == "vm" else "vm"
    key = "vm" if assigned == "vm" else "workspace_container"

    if matching_ready:
        return WorkspaceRuntimeDecision(
            contract,
            assigned,
            "ready",
            selected_context_key=key,
            stale_backend=opposite if opposite_ready else None,
        )
    if matching_endpoint_ready:
        return WorkspaceRuntimeDecision(
            contract,
            None,
            "invalid",
            f"{assigned}_runtime_unattested",
            stale_backend=opposite if opposite_ready else None,
        )
    if matching.get("status") in {"failed", "delete_failed", "query_failed"}:
        return WorkspaceRuntimeDecision(
            contract,
            None,
            "failed",
            f"{assigned}_provisioning_failed",
            stale_backend=opposite if opposite_ready else None,
        )
    if opposite_ready:
        return WorkspaceRuntimeDecision(
            contract,
            None,
            "mismatch",
            f"{opposite}_ready_for_{assigned}_assignment",
            stale_backend=opposite,
        )
    return WorkspaceRuntimeDecision(
        contract,
        None,
        "waiting",
        f"{assigned}_runtime_not_ready",
    )


def normalized_workspace_backend_sql(config_expr: str = "config_override") -> str:
    """SQL that canonicalizes a job's configured backend the way 0175 does.

    The trigger normalizes ``container``/``remote`` before comparing, so every
    statement it fences has to normalize identically or fail the comparison.
    """

    coalesced = f"lower(COALESCE({config_expr}->'workspace'->>'backend', 'sandbox'))"
    return (
        f"CASE {coalesced} WHEN 'container' THEN 'sandbox' "
        f"WHEN 'remote' THEN 'vm' ELSE {coalesced} END"
    )


def pinned_dispatch_authority_jsonb_sql(
    *,
    agent_expr: str,
    lease_expr: str,
    context_expr: str = "context",
    config_expr: str = "config_override",
) -> str:
    """SQL that builds the pinned dispatch-authority marker for a jobs UPDATE.

    Migration 0175 fences every write that lands a job on the claimed pinned
    shape (processing, with an assigned agent) on this marker, and matches it
    field-by-field against the row the same statement writes.  Both the
    dispatcher's claim CAS and the in-process resume of a parked agent have to
    emit it, so the fragment lives here rather than being retyped per call
    site — a marker that drifts from the trigger fails closed at runtime, not
    in review.

    ``agent_expr`` and ``lease_expr`` are SQL expressions for the agent and
    lease values the SAME statement assigns; passing anything else produces a
    marker the trigger rejects.
    """

    normalized_backend = normalized_workspace_backend_sql(config_expr)
    return f"""jsonb_build_object(
    'version', 1,
    'dispatch_kind', 'pinned',
    'contract_version', CASE
        WHEN {context_expr} ? '{WORKSPACE_CONTRACT_CONTEXT_KEY}' THEN 1 ELSE 0
    END,
    'assigned_backend', COALESCE(
        {context_expr}->'{WORKSPACE_CONTRACT_CONTEXT_KEY}'->>'assigned_backend',
        {normalized_backend}
    ),
    'agent_id', ({agent_expr})::text,
    'lease_expires_at', to_jsonb({lease_expr})
)"""


def workspace_contract_projection(
    job: Mapping[str, Any], *, vm_mode: str = "external"
) -> dict[str, Any]:
    """Safe, coordinate-free API/formatter projection."""

    return resolve_workspace_runtime(job, vm_mode=vm_mode).safe_projection()


def workspace_runtime_authority_digest(
    job: Mapping[str, Any], *, vm_mode: str = "external"
) -> str | None:
    """Hash the exact runtime authority a worker bundle would receive.

    Dispatch uses this immediately before network delivery to prove that the
    selected endpoint/generation did not change while credentials and the rest
    of the bundle were assembled.  The digest is intentionally one-way: it is
    safe to compare or log as a correlation value, while transport coordinates
    remain server-only.
    """

    decision = resolve_workspace_runtime(job, vm_mode=vm_mode)
    if not decision.ready or decision.contract is None:
        return None
    material: dict[str, Any] = {
        "contract": decision.contract.to_context(),
        "effective_backend": decision.effective_backend,
    }
    context = _object(job.get("context"))
    if decision.effective_backend == "vm":
        runtime = _object(context.get("vm"))
        material["runtime"] = {
            key: runtime.get(key)
            for key in (
                "status",
                "provisioner",
                "provision_generation",
                "vm_name",
                "ssh_host",
                "ssh_port",
            )
        }
    elif decision.effective_backend == "sandbox":
        runtime = _object(context.get("workspace_container"))
        material["runtime"] = {
            key: runtime.get(key)
            for key in (
                "status",
                "provisioner",
                "_runtime_incarnation",
                "_docker_workspace_lease_id",
                LEGACY_K8S_RUNTIME_ADOPTION_KEY,
                "pod_name",
                "namespace",
                "host",
                "pod_ip",
                "port",
            )
        }
    encoded = json.dumps(
        material, sort_keys=True, separators=(",", ":"), default=str
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def workspace_contract_authority_identity(
    job: Mapping[str, Any],
    *,
    vm_mode: str = "external",
    allow_vm_suspending: bool = False,
) -> tuple[str, str] | None:
    """Return the selected tier plus a coordinate-free contract digest.

    Durable remote-effect receipts bind the selected workspace contract as
    well as an endpoint. Runtime coordinates are excluded because each receipt
    binds them separately.
    """

    decision = resolve_workspace_runtime(job, vm_mode=vm_mode)
    if not decision.ready and allow_vm_suspending:
        # Suspension first reserves the lifecycle by changing only this state
        # marker.  Exact read/capture effects still need the same canonical
        # contract proof while the endpoint remains alive.  Re-run the pure
        # resolver against a copy with that one server-owned phase projected
        # as ready; endpoint identity is validated independently by the VM
        # lease and controller attestation.
        context = _object(job.get("context"))
        vm = _object(context.get("vm"))
        if vm.get("status") == "suspending":
            vm["status"] = "ready"
            context["vm"] = vm
            projected = dict(job)
            projected["context"] = context
            decision = resolve_workspace_runtime(projected, vm_mode=vm_mode)
    if not decision.ready or decision.contract is None:
        return None
    config = _object(job.get("config_override"))
    workspace = _object(config.get("workspace"))
    workspace.pop("remote", None)
    workspace["backend"] = decision.contract.assigned_backend
    material = {
        "version": 1,
        "contract": decision.contract.to_context(),
        "effective_backend": decision.effective_backend,
        "workspace_config": workspace,
    }
    encoded = json.dumps(
        material, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode()
    return decision.effective_backend or "", hashlib.sha256(encoded).hexdigest()


def validate_worker_workspace_projection(
    *,
    config_override: Any,
    resolved_config: Any,
    workspace_runtime: Any,
) -> None:
    """Refuse a worker bundle whose declared/effective tier is inconsistent.

    The orchestrator owns the projection; this recipient-side check is the
    rolling-upgrade backstop. A pre-contract orchestrator cannot hand a new
    worker a bundle at all, and a malformed bundle cannot describe one tier
    while configuring another. Only tier names are inspected here—transport
    coordinates remain opaque and are never included in an error.
    """

    projection = _object(workspace_runtime)
    if not projection:
        raise WorkspaceContractError(
            "workspace_runtime_authority_missing",
            "Worker bundle is missing workspace runtime authority",
        )
    assigned = normalize_workspace_backend(
        projection.get("assigned_backend"), field="assigned workspace backend"
    )
    effective = normalize_workspace_backend(
        projection.get("effective_backend"), field="effective workspace backend"
    )
    if projection.get("state") != "ready" or assigned != effective:
        raise WorkspaceContractError(
            "workspace_runtime_authority_mismatch",
            "Worker bundle workspace authority is not ready and consistent",
        )

    resolved = _object(resolved_config)
    execution_config = (
        _object(resolved.get("agent")) if resolved else _object(config_override)
    )
    configured = configured_workspace_backend(execution_config) or "sandbox"
    if configured != assigned:
        raise WorkspaceContractError(
            "workspace_runtime_config_mismatch",
            "Worker bundle workspace configuration disagrees with its authority",
        )
    if assigned in REMOTE_WORKSPACE_BACKENDS:
        workspace = _object(execution_config.get("workspace"))
        remote = _object(workspace.get("remote"))
        if not isinstance(remote.get("host"), str) or not remote["host"].strip():
            raise WorkspaceContractError(
                "workspace_runtime_endpoint_missing",
                "Worker bundle has no endpoint for its assigned workspace tier",
            )


__all__ = [
    "CANONICAL_WORKSPACE_BACKENDS",
    "LEGACY_K8S_RUNTIME_ADOPTION_KEY",
    "REMOTE_WORKSPACE_BACKENDS",
    "WORKSPACE_CONTRACT_CONTEXT_KEY",
    "WORKSPACE_CONTRACT_VERSION",
    "WORKSPACE_DISPATCH_AUTHORITY_CONTEXT_KEY",
    "WORKSPACE_RUNTIME_CONTEXT_KEY",
    "WorkspaceContract",
    "WorkspaceContractError",
    "WorkspaceRuntimeDecision",
    "build_workspace_contract",
    "canonicalize_workspace_config",
    "configured_workspace_backend",
    "normalize_workspace_backend",
    "normalized_workspace_backend_sql",
    "pinned_dispatch_authority_jsonb_sql",
    "resolve_workspace_contract",
    "resolve_workspace_runtime",
    "strip_and_stamp_workspace_creation",
    "workspace_contract_projection",
    "workspace_contract_authority_identity",
    "workspace_runtime_authority_digest",
    "validate_worker_workspace_projection",
]
