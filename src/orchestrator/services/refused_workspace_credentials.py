"""What a refused resume does with the workspace it keeps (decision 34).

A start refusal under completion commands fails the job through the
completion ledger. A refused resume keeps the paused job's workspace (owner
decision, 2026-10-10), as the provisioner keeps a refused job's pod with
completion commands off; a refused fresh start tears down what was
provisioned for it.

When the refusal revokes access (:data:`REVOCATION_REASONS`), the kept
workspace must not go on holding the credentials SRW delivered for it, so
they are scrubbed from it: the claim owner, once the refusal is admitted,
runs :func:`managed_repository_process_retirement.scrub_workspace_credentials`
over the pinned SSH transport the terminal owners already use, against the
endpoint and host key the claim attested. It is best-effort: the outcome is
recorded on the job (``context.start_refusal_credential_scrub``) and never
blocks the failure, which the finalizer writes.

Running it only after admission matters: the admission proves the claim
still owned the job, and from then on nothing claims it again (a pinned job
is held by its pending command, a stateless job's unit is closed), so no
successor's freshly delivered credentials can be removed by mistake.

Two cases keep the workspace without a scrub: a lite tier (``virtual`` or
``none``) has no shell workspace that SRW credentials could reach, and a
child that shares its parent's workspace holds only the parent's material,
which is not this refusal's to remove. A revocation whose workspace no
attested endpoint reaches (the static Docker pool) is torn down instead.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
import logging
from typing import Any, Mapping

from orchestrator.services import managed_repository_process_retirement
from orchestrator.services.job_workspace_runtime import (
    scholar_provision_parent_id,
    stateless_worker_workspace_owner,
)
from shared.backend_kinds import LITE_BACKENDS
from shared.workspace_contract import WorkspaceContractError, resolve_workspace_contract

logger = logging.getLogger(__name__)

#: Refusals that revoke access to something SRW delivered into the workspace:
#: a connector no longer authorized, a denied grant, and a connector bind or
#: provider mint that failed for good (both refuse as ``BindTimeRefused``).
REVOCATION_REASONS = frozenset(
    {"connector_unavailable", "grant_denied", "connector_bind_refused"}
)

#: Where the scrub's outcome is recorded on the job.
SCRUB_CONTEXT_KEY = "start_refusal_credential_scrub"


@dataclass(frozen=True, slots=True)
class ScrubTarget:
    """A workspace SSH endpoint and the host key the claim attested for it."""

    host: str
    port: int
    host_key_fingerprint: str


@dataclass(frozen=True, slots=True)
class RefusedResume:
    """What a refused resume does with the workspace.

    ``keep_workspace`` is the report's ``resume`` flag (the finalizer keeps
    the workspace instead of tearing it down); ``scrub`` is the endpoint to
    scrub after admission; ``skipped`` names why a revocation keeps the
    workspace without a scrub.
    """

    keep_workspace: bool
    scrub: ScrubTarget | None = None
    skipped: str | None = None


def attested_scrub_target(attestation: Any) -> ScrubTarget | None:
    """The scrub target of a control-plane attestation, if it is complete."""

    if attestation is None:
        return None
    host = getattr(attestation, "host", None) or getattr(attestation, "pod_ip", None)
    port = getattr(attestation, "port", None)
    fingerprint = getattr(attestation, "ssh_host_key_fingerprint", None)
    if not host or not isinstance(port, int) or isinstance(port, bool):
        return None
    if not isinstance(fingerprint, str) or not fingerprint.startswith("SHA256:"):
        return None
    return ScrubTarget(str(host), port, fingerprint)


def _context(job: Mapping[str, Any]) -> dict[str, Any]:
    context = job.get("context") or {}
    if isinstance(context, str):
        try:
            context = json.loads(context)
        except (TypeError, ValueError):
            return {}
    return dict(context) if isinstance(context, Mapping) else {}


def vm_scrub_target(job: Mapping[str, Any]) -> ScrubTarget | None:
    """A VM workspace's endpoint and the host key its provisioner probe
    authenticated for the current generation; ``None`` short of that."""

    vm = _context(job).get("vm")
    if not isinstance(vm, Mapping):
        return None
    if not (
        vm.get("status") == "ready"
        and vm.get("ssh_ready_source") == "provisioner_probe"
        and vm.get("identity_authenticated") is True
        and vm.get("identity_provision_generation") == vm.get("provision_generation")
    ):
        return None
    try:
        port = int(vm.get("ssh_port") or 22)
    except (TypeError, ValueError):
        return None
    host = vm.get("ssh_host") or vm.get("pod_ip")
    fingerprint = vm.get("ssh_host_key_fingerprint")
    if not host or not isinstance(fingerprint, str):
        return None
    if not fingerprint.startswith("SHA256:"):
        return None
    return ScrubTarget(str(host), port, fingerprint)


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
    target: ScrubTarget | None,
) -> RefusedResume:
    """Decide what a refused resume does with the job's workspace."""

    if reason not in REVOCATION_REASONS:
        return RefusedResume(keep_workspace=True)
    if _lite_tier(job):
        return RefusedResume(keep_workspace=True, skipped="no_shell_workspace")
    if _shares_parent_workspace(job):
        return RefusedResume(keep_workspace=True, skipped="shared_workspace")
    if target is None:
        # Nothing reaches the workspace to scrub it: it goes, credentials and
        # all, as a refused fresh start's does.
        return RefusedResume(keep_workspace=False)
    return RefusedResume(keep_workspace=True, scrub=target)


async def scrub_refused_workspace(
    store: Any,
    job_id: str,
    *,
    reason: str,
    plan: RefusedResume,
) -> None:
    """Scrub (or record why not) a kept workspace after an admitted refusal.

    Best-effort: a failed scrub is logged and recorded, never raised.
    """

    if plan.scrub is None and plan.skipped is None:
        return
    record: dict[str, Any] = {"reason": reason}
    if plan.scrub is None:
        record.update(outcome="skipped", detail=plan.skipped)
    else:
        try:
            scrubbed = (
                await managed_repository_process_retirement.scrub_workspace_credentials(
                    host=plan.scrub.host,
                    port=plan.scrub.port,
                    host_key_fingerprint=plan.scrub.host_key_fingerprint,
                )
            )
        except Exception:
            logger.warning(
                "Credential scrub of job %s's kept workspace failed",
                job_id,
                exc_info=True,
            )
            scrubbed = False
        record["outcome"] = "scrubbed" if scrubbed else "failed"
        if not scrubbed:
            logger.warning(
                "Job %s keeps its workspace after a %s refusal, but its "
                "credentials could not be scrubbed",
                job_id,
                reason,
            )
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
    "ScrubTarget",
    "attested_scrub_target",
    "plan_refused_resume",
    "scrub_refused_workspace",
    "vm_scrub_target",
]
