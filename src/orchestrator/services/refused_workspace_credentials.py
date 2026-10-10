"""What a refused resume does with the workspace it keeps (decision 34).

A start refusal under completion commands fails the job through the
completion ledger. A refused resume keeps the paused job's workspace (owner
decision, 2026-10-10), as the provisioner keeps a refused job's pod with
completion commands off; a refused fresh start tears down what was
provisioned for it.

When the refusal revokes access (:data:`REVOCATION_REASONS`), the kept
workspace must not go on holding the credentials SRW delivered into it
(workspace credentials are the asset SRW protects). Once the refusal is
admitted, the claim owner scrubs them through the endpoint and host key the
claim attested: it ends the job's own tmux session, as a terminal teardown
retires a job's shell (:func:`retire_job_shells`), so no shell keeps the
credentials its environment exported, then runs
:func:`managed_repository_process_retirement.scrub_workspace_credentials`
over the pinned SSH transport the terminal owners use. That command's
docstring lists exactly what it removes; :data:`SCRUB_REMAINS` is what it
leaves.

It is best-effort: the outcome is recorded on the job
(``context.start_refusal_credential_scrub``) and never blocks the failure,
which the finalizer writes. Running it only after admission matters: the
admission proves the claim still owned the job, and from then on nothing
claims it again (a pinned job is held by its pending command, a stateless
job's unit is closed), so no successor's freshly delivered credentials can be
removed by mistake.

When nothing can reach the workspace (no attested endpoint: the static
Docker pool, a VM whose host key no provisioner probe authenticated, a VM
outside same-cluster mode), the workspace is torn down instead, as a refused
fresh start's is, and the record says why. Two cases keep the workspace
without a scrub: a lite tier (``virtual`` or ``none``) has no shell
workspace that SRW credentials could reach, and a child that shares its
parent's workspace holds only the parent's material, which is not this
refusal's to remove.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import json
import logging
from typing import Any, Mapping

from orchestrator.services import managed_repository_process_retirement
from orchestrator.services import resolve_ssh_key_path
from orchestrator.services.job_workspace_runtime import (
    scholar_provision_parent_id,
    stateless_worker_workspace_owner,
)
from shared.backend_kinds import LITE_BACKENDS
from shared.workspace_contract import (
    WorkspaceContractError,
    resolve_workspace_contract,
    vm_mode_from_env,
)

logger = logging.getLogger(__name__)

#: Refusals that revoke access to something SRW delivered into the workspace:
#: a connector no longer authorized, a denied grant, and a connector bind or
#: provider mint that failed for good (both refuse as ``BindTimeRefused``).
REVOCATION_REASONS = frozenset(
    {"connector_unavailable", "grant_denied", "connector_bind_refused"}
)

#: Where the scrub's outcome is recorded on the job.
SCRUB_CONTEXT_KEY = "start_refusal_credential_scrub"

#: What a scrub leaves: lines SRW added to the user's own files, which name
#: nothing once the scrub ran. Recorded with every scrub.
SCRUB_REMAINS = (
    "~/.gitconfig: include.path = ~/.srw-credentials/git/config",
    "~/.ssh/config: Include ~/.ssh/srw-managed/config.d/*.conf",
)


@dataclass(frozen=True, slots=True)
class ScrubTarget:
    """A workspace SSH endpoint, the host key the claim attested for it, and
    what the job's shell retirement is fenced by."""

    host: str
    port: int
    host_key_fingerprint: str
    workspace_generation: str | None = None
    runtime_incarnation: str | None = None
    #: The stateless claim's lease token; ``None`` for a pinned shell.
    shell_owner_token: int | None = None


@dataclass(frozen=True, slots=True)
class Unreachable:
    """Why nothing can reach the workspace to scrub it."""

    why: str


@dataclass(frozen=True, slots=True)
class RefusedResume:
    """What a refused resume does with the workspace.

    ``keep_workspace`` is the report's ``resume`` flag (the finalizer keeps
    the workspace instead of tearing it down). ``scrub`` is the endpoint to
    scrub after admission; ``skipped`` names why a revocation keeps the
    workspace without a scrub; ``unreachable`` why a revocation's workspace
    is torn down instead.
    """

    keep_workspace: bool
    scrub: ScrubTarget | None = None
    skipped: str | None = None
    unreachable: str | None = None


def attested_scrub_target(
    attestation: Any, *, shell_owner_token: int | None = None
) -> ScrubTarget | Unreachable:
    """The scrub target of a control-plane attestation, if it is complete."""

    if attestation is None:
        return Unreachable("no attested workspace endpoint")
    host = getattr(attestation, "host", None) or getattr(attestation, "pod_ip", None)
    port = getattr(attestation, "port", None)
    fingerprint = getattr(attestation, "ssh_host_key_fingerprint", None)
    if not host or not isinstance(port, int) or isinstance(port, bool):
        return Unreachable("the attestation names no endpoint")
    if not isinstance(fingerprint, str) or not fingerprint.startswith("SHA256:"):
        return Unreachable("the attestation names no host key")
    return ScrubTarget(
        host=str(host),
        port=port,
        host_key_fingerprint=fingerprint,
        workspace_generation=getattr(attestation, "workspace_generation", None),
        runtime_incarnation=getattr(attestation, "runtime_incarnation", None),
        shell_owner_token=shell_owner_token,
    )


def _context(job: Mapping[str, Any]) -> dict[str, Any]:
    context = job.get("context") or {}
    if isinstance(context, str):
        try:
            context = json.loads(context)
        except (TypeError, ValueError):
            return {}
    return dict(context) if isinstance(context, Mapping) else {}


def vm_scrub_target(job: Mapping[str, Any]) -> ScrubTarget | Unreachable:
    """A pinned VM workspace's endpoint and the host key its provisioner
    probe authenticated for the current generation, in same-cluster mode;
    otherwise why not. A job without a VM has no attested endpoint here."""

    vm = _context(job).get("vm")
    if not isinstance(vm, Mapping) or not vm:
        return Unreachable("no attested workspace endpoint")
    mode = vm_mode_from_env()
    if mode != "same-cluster":
        return Unreachable(f"VM mode {mode}")
    if vm.get("status") != "ready":
        return Unreachable("the VM is not ready")
    if not (
        vm.get("ssh_ready_source") == "provisioner_probe"
        and vm.get("identity_authenticated") is True
        and vm.get("identity_provision_generation") == vm.get("provision_generation")
    ):
        return Unreachable("no provisioner probe authenticated the VM's host key")
    try:
        port = int(vm.get("ssh_port") or 22)
    except (TypeError, ValueError):
        return Unreachable("the VM names no endpoint")
    host = vm.get("ssh_host") or vm.get("pod_ip")
    fingerprint = vm.get("ssh_host_key_fingerprint")
    if not host:
        return Unreachable("the VM names no endpoint")
    if not isinstance(fingerprint, str) or not fingerprint.startswith("SHA256:"):
        return Unreachable("the VM names no host key")
    return ScrubTarget(host=str(host), port=port, host_key_fingerprint=fingerprint)


def _lite_tier(job: Mapping[str, Any]) -> bool:
    try:
        contract = resolve_workspace_contract(dict(job))
    except (WorkspaceContractError, TypeError, ValueError):
        return False
    return contract is not None and contract.assigned_backend in LITE_BACKENDS


def _shares_parent_workspace(job: Mapping[str, Any]) -> bool:
    job_dict = dict(job)
    return (
        stateless_worker_workspace_owner(job_dict).id != str(job["id"])
        or scholar_provision_parent_id(job_dict) is not None
    )


def plan_refused_resume(
    reason: str,
    *,
    job: Mapping[str, Any],
    target: ScrubTarget | Unreachable,
) -> RefusedResume:
    """Decide what a refused resume does with the job's workspace."""

    if reason not in REVOCATION_REASONS:
        return RefusedResume(keep_workspace=True)
    if _lite_tier(job):
        return RefusedResume(keep_workspace=True, skipped="no_shell_workspace")
    if _shares_parent_workspace(job):
        return RefusedResume(keep_workspace=True, skipped="shared_workspace")
    if isinstance(target, Unreachable):
        # Workspace credentials first: what nothing can scrub goes whole, as
        # a refused fresh start's workspace does.
        return RefusedResume(keep_workspace=False, unreachable=target.why)
    return RefusedResume(keep_workspace=True, scrub=target)


async def retire_job_shells(job_id: str, target: ScrubTarget) -> bool:
    """End the job's own tmux session, as a terminal teardown does.

    Rebuilds the job's remote backend from the attested target and runs its
    terminal shell retirement: :meth:`RemoteBackend.shell_cleanup_strict` for
    a pinned shell, :meth:`RemoteBackend.shell_cleanup` under the stateless
    claim's lease token, which both require the workspace's acknowledgement.
    Only this job's session is touched (its owner id is checked), and the
    work item's credential files go with it. ``False`` when not acknowledged.
    """

    from shared.runtime.core.backends.remote import RemoteBackend

    backend = RemoteBackend(
        host=target.host,
        port=target.port,
        username="agent-host",
        key_path=resolve_ssh_key_path(),
        workspace_path=managed_repository_process_retirement.WORKSPACE_HOME
        + "/workspace",
        job_id=str(job_id),
        workspace_generation=target.workspace_generation,
        runtime_incarnation=target.runtime_incarnation,
        expected_host_key_fingerprint=target.host_key_fingerprint,
        require_host_key_fingerprint=True,
    )
    try:
        if target.shell_owner_token is None:
            await asyncio.to_thread(backend.shell_cleanup_strict)
        else:
            backend.set_shell_owner_token(target.shell_owner_token)
            await asyncio.to_thread(backend.shell_cleanup)
        return True
    except Exception:
        logger.warning(
            "Job %s's shell retirement was not acknowledged", job_id, exc_info=True
        )
        return False
    finally:
        try:
            await asyncio.to_thread(backend.retire)
        except Exception:
            pass


async def _scrub(job_id: str, target: ScrubTarget) -> tuple[bool, bool]:
    try:
        shells = await retire_job_shells(job_id, target)
    except Exception:
        logger.warning("Job %s's shell retirement failed", job_id, exc_info=True)
        shells = False
    try:
        files = await managed_repository_process_retirement.scrub_workspace_credentials(
            host=target.host,
            port=target.port,
            host_key_fingerprint=target.host_key_fingerprint,
        )
    except Exception:
        logger.warning(
            "Credential scrub of job %s's kept workspace failed",
            job_id,
            exc_info=True,
        )
        files = False
    return shells, files


async def scrub_refused_workspace(
    store: Any,
    job_id: str,
    *,
    reason: str,
    plan: RefusedResume,
) -> None:
    """Scrub a kept workspace after an admitted refusal, or record why not.

    Best-effort: a failed scrub is logged and recorded, never raised.
    """

    record: dict[str, Any] = {"refusal": reason}
    if plan.unreachable is not None:
        record.update(outcome="torn_down", reason=f"unreachable: {plan.unreachable}")
    elif plan.skipped is not None:
        record.update(outcome="skipped", reason=plan.skipped)
    elif plan.scrub is not None:
        shells, files = await _scrub(job_id, plan.scrub)
        record.update(
            outcome="scrubbed" if shells and files else "failed",
            shells="retired" if shells else "not_retired",
            files="scrubbed" if files else "failed",
            remains=list(SCRUB_REMAINS),
        )
        if not (shells and files):
            logger.warning(
                "Job %s keeps its workspace after a %s refusal, but its "
                "credentials were not fully scrubbed (shells=%s, files=%s)",
                job_id,
                reason,
                record["shells"],
                record["files"],
            )
    else:
        return
    try:
        await store.merge_job_context(job_id, {SCRUB_CONTEXT_KEY: record})
    except Exception:
        logger.warning(
            "Could not record the credential scrub of job %s: %s",
            job_id,
            record,
            exc_info=True,
        )


__all__ = [
    "REVOCATION_REASONS",
    "RefusedResume",
    "SCRUB_CONTEXT_KEY",
    "SCRUB_REMAINS",
    "ScrubTarget",
    "Unreachable",
    "attested_scrub_target",
    "plan_refused_resume",
    "retire_job_shells",
    "scrub_refused_workspace",
    "vm_scrub_target",
]
