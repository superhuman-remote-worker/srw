"""Resolve and freshly attest the exact pinned process for job mutation."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
import json
import logging
from typing import Any, NamedTuple

from shared.pinned_session_identity import PinnedJobRecipient


class PinnedJobMutationTarget(NamedTuple):
    agent: dict[str, Any]
    recipient: PinnedJobRecipient
    #: The ``capabilities`` the pod's ``/ready`` declared: what its harness
    #: image implements (``jobs``, ``pause``, ``model_protocols``, ...).
    capabilities: dict[str, Any] = {}


# A freshly pinned recipient can still be mid-attestation when a mutation
# arrives; bound the retry so a wedged pod cannot hold the request open.
FRESH_PINNED_RECIPIENT_ATTESTATION_ATTEMPTS = 8
FRESH_PINNED_RECIPIENT_ATTESTATION_DELAY_S = 0.25


@dataclass(frozen=True, slots=True)
class PinnedJobMutationTargetDependencies:
    store: Any
    agent_provisioner: Any
    logger: logging.Logger
    http_client_factory: Callable[..., Any]
    sleep: Callable[[float], Awaitable[None]]


async def prepare_pinned_job_mutation_target(
    *,
    agent_id: str,
    job_id: str,
    require_idle: bool,
    dependencies: PinnedJobMutationTargetDependencies,
) -> PinnedJobMutationTarget | None:
    """Resolve and freshly attest the exact process for pinned job control."""

    fresh = await dependencies.store.get_agent(agent_id)
    if not fresh or not fresh.get("pod_ip"):
        dependencies.logger.warning(
            "Pinned recipient unavailable for job %s (agent=%s)", job_id, agent_id
        )
        return None

    status = str(fresh.get("status") or "")
    current_job_id = str(fresh.get("current_job_id") or "") or None
    is_fresh_accept = status == "ready" and current_job_id is None
    if require_idle:
        is_exact_retry = status == "working" and current_job_id == job_id
        if not (is_fresh_accept or is_exact_retry):
            dependencies.logger.warning(
                "Pinned recipient cannot accept/replay job %s "
                "(agent=%s status=%s current=%s)",
                job_id,
                agent_id,
                status,
                current_job_id,
            )
            return None
    elif status != "working" or current_job_id != job_id:
        dependencies.logger.warning(
            "Pinned recipient no longer owns job %s (agent=%s status=%s current=%s)",
            job_id,
            agent_id,
            status,
            current_job_id,
        )
        return None

    metadata = fresh.get("metadata") or {}
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
        except (TypeError, ValueError):
            metadata = {}
    process_generation = (
        str(metadata.get("dispatch_process_generation") or "").strip()
        if isinstance(metadata, Mapping)
        else ""
    )
    if not process_generation:
        dependencies.logger.warning(
            "Pinned recipient lacks process generation for job %s (agent=%s)",
            job_id,
            agent_id,
        )
        return None

    pod_uid = str(fresh.get("pod_uid") or "").strip() or None
    ready_url = f"http://{fresh['pod_ip']}:{fresh['pod_port']}/ready"
    try:
        async with dependencies.http_client_factory(timeout=5.0) as client:
            ready_response = await client.get(ready_url)
        ready_payload = (
            ready_response.json() if ready_response.status_code == 200 else {}
        )
    except Exception as exc:
        dependencies.logger.info(
            "Pinned recipient capability probe failed for agent %s (%s)",
            agent_id,
            type(exc).__name__,
        )
        return None
    if not (
        isinstance(ready_payload, Mapping)
        and ready_payload.get("ready") is True
        and isinstance(ready_payload.get("capabilities"), Mapping)
        and ready_payload["capabilities"].get("pinned_recipient_binding") is True
    ):
        dependencies.logger.warning(
            "Pinned recipient binding capability unavailable for agent %s", agent_id
        )
        return None

    if pod_uid is not None:
        # A newly started agent can answer its own /ready probe one API-cache
        # beat before Kubernetes publishes containerStatuses[*].ready=true.
        attempts = (
            FRESH_PINNED_RECIPIENT_ATTESTATION_ATTEMPTS
            if require_idle and is_fresh_accept
            else 1
        )
        attested = False
        for attempt in range(attempts):
            attested = await dependencies.agent_provisioner.attest_pinned_job_recipient(
                str(fresh.get("hostname") or ""),
                expected_pod_uid=pod_uid,
                expected_pod_ip=str(fresh["pod_ip"]),
            )
            if attested:
                if attempt:
                    dependencies.logger.info(
                        "Pinned recipient Pod became attestable for job %s "
                        "after %s retries (agent=%s)",
                        job_id,
                        attempt,
                        agent_id,
                    )
                break
            if attempt + 1 < attempts:
                await dependencies.sleep(FRESH_PINNED_RECIPIENT_ATTESTATION_DELAY_S)
        if not attested:
            dependencies.logger.warning(
                "Pinned recipient Pod attestation failed for job %s (agent=%s)",
                job_id,
                agent_id,
            )
            return None

    return PinnedJobMutationTarget(
        agent=fresh,
        recipient=PinnedJobRecipient(
            expected_agent_id=agent_id,
            expected_pod_uid=pod_uid,
            expected_process_generation=process_generation,
            expected_job_id=job_id,
        ),
        capabilities=dict(ready_payload["capabilities"]),
    )


__all__ = [
    "FRESH_PINNED_RECIPIENT_ATTESTATION_ATTEMPTS",
    "FRESH_PINNED_RECIPIENT_ATTESTATION_DELAY_S",
    "PinnedJobMutationTarget",
    "PinnedJobMutationTargetDependencies",
    "prepare_pinned_job_mutation_target",
]
