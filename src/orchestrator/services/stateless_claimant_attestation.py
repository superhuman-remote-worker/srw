"""Read-only Kubernetes attestation for pooled stateless claimants.

The stateless executor pool (``helm/templates/agent/stateless-deployment.yaml``)
deliberately never creates an ``agents`` registration row
(``src/agent/__main__.py::run_stateless_server``). The bundle therefore cannot
require ``agents.hostname/pod_uid``. Instead it combines the existing exact
database lease proof (``unit_kind=worker_batch``, ``leased``, exact token,
matching ``leased_by``) with a read-only observation of the claimed executor
Pod:

* look the claimed Pod name up in the server-owned executor namespace,
* compare its immutable Kubernetes UID with the caller-supplied ``pod_uid``,
* require membership in the intended stateless pool via the chart's actual
  class/instance metadata,
* require a non-terminal, non-deleting runtime phase.

Trust boundary: Kubernetes object matching verifies the claimed runtime
identity; it does not by itself cryptographically authenticate the HTTP
sender. The existing internal transport guard (``require_internal``) and the
exact current lease proof remain required. This repair does not implement
per-Pod caller credentials and does not change the shared-internal-key model.

Readiness handling: ``Ready`` (and the ``agent`` container's ``ready`` flag)
is observability only for this class — no Service selects it — so a valid
busy executor must not be rejected because it is busy. Attestation therefore
requires ``phase=Running`` with no ``deletionTimestamp`` and ignores the
``Ready`` condition entirely.

Namespace ownership: the chart-managed pool runs in the release namespace,
which the chart publishes as ``WORKSPACE_NAMESPACE``
(``helm/templates/configmap.yaml``). This module reads that server-owned
value explicitly — the same source as ``stateless_capacity.namespace()`` —
and never searches caller-supplied or arbitrary namespaces. ``AGENT_NAMESPACE``
(preferred by ``AgentProvisioner`` for dynamically provisioned pods) is
deliberately not consulted: the pooled Deployment is not provisioner-managed.

Pool identity: expected ``app.kubernetes.io/name`` and
``app.kubernetes.io/instance`` come from the server-owned ``AGENT_LABEL_NAME``
/ ``AGENT_LABEL_INSTANCE`` env (rendered by the orchestrator Deployment from
``srw.name`` / ``Release.Name``). A missing expected value fails closed as
unknown authority; it never silently disables the check. The fixed component
markers are ``app.kubernetes.io/component=agent-stateless`` and
``srw/class=agent-stateless`` (see the rendered Deployment pod template).

Outcomes:

* ``403 Lease validation failed`` — permanent identity refusal: malformed
  UID, absent/replaced/mismatched Pod, terminal/deleting phase, wrong pool
  labels, wrong namespace/release. No bundle, no fresh recovery hold, no
  tenant/config/credential data in the diagnostic.
* ``503 Claimant authority unavailable`` — unknown authority: unavailable
  Kubernetes client, read timeout/transport error, or missing server pool
  configuration. Bounded, never success, never disguised as a
  workspace-recovery (409) event. The worker executor releases with backoff
  and retries either status identically (``ClaimBundleError`` → driver-error
  release path).

The slow Kubernetes read always happens outside database write-lock
transactions. Callers re-read the exact queue lease under the existing short
transactional boundary after attestation returns, so a lease stolen (or a Pod
replaced) during the external read cannot authorize stale authority. The two
observations are not one atomic transaction; that boundary is documented,
not hidden.
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Any, Callable
from uuid import UUID

from fastapi import HTTPException

from orchestrator.services.pinned_k8s_effect import run_bounded_k8s_call

logger = logging.getLogger(__name__)

STATELESS_CLASS_VALUE = "agent-stateless"
STATELESS_COMPONENT_VALUE = "agent-stateless"
K8S_READ_REQUEST_TIMEOUT = (5.0, 15.0)


def executor_namespace() -> str:
    """Server-owned namespace the chart-managed stateless pool runs in."""
    return os.environ.get("WORKSPACE_NAMESPACE", "superhuman-remote-worker")


def pool_identity() -> tuple[str, str]:
    """Server-owned expected pool identity (chart name, release instance)."""
    return (
        os.environ.get("AGENT_LABEL_NAME", "").strip(),
        os.environ.get("AGENT_LABEL_INSTANCE", "").strip(),
    )


def _is_valid_uid(value: str) -> bool:
    try:
        UUID(value)
    except (ValueError, TypeError, AttributeError):
        return False
    return True


def _pod_labels(pod: Any) -> dict[str, str]:
    return dict(getattr(getattr(pod, "metadata", None), "labels", None) or {})


def claimant_pool_mismatch_reason(
    pod: Any, *, expected_name: str, expected_instance: str
) -> str | None:
    """Return a stable mismatch key, or ``None`` when the pool matches."""
    labels = _pod_labels(pod)
    if labels.get("srw/class") != STATELESS_CLASS_VALUE:
        return "pool_class"
    if labels.get("app.kubernetes.io/component") != STATELESS_COMPONENT_VALUE:
        return "pool_component"
    if labels.get("app.kubernetes.io/name") != expected_name:
        return "pool_name"
    if labels.get("app.kubernetes.io/instance") != expected_instance:
        return "pool_instance"
    return None


def claimant_lifecycle_refused(pod: Any) -> bool:
    """Whether the observed Pod cannot currently execute a batch claim."""
    metadata = getattr(pod, "metadata", None)
    status = getattr(pod, "status", None)
    if getattr(metadata, "deletion_timestamp", None) is not None:
        return True
    return str(getattr(status, "phase", "") or "") != "Running"


async def attest_stateless_executor_pod(
    pod_name: str,
    pod_uid: str,
    *,
    core_api: Any,
    namespace: str,
    expected_name: str,
    expected_instance: str,
) -> None:
    """Observe one exact pooled executor Pod, or raise 403/503.

    Raises:
        HTTPException(403, "Lease validation failed"): permanent identity
            refusal — never a bundle, never a fresh recovery hold.
        HTTPException(503, "Claimant authority unavailable"): unknown
            authority — Kubernetes unavailable/timed out or server pool
            configuration missing. Bounded, retryable, never success.
    """
    name = str(pod_name or "").strip()
    uid = str(pod_uid or "").strip()
    namespace = str(namespace or "").strip()
    expected_name = str(expected_name or "").strip()
    expected_instance = str(expected_instance or "").strip()
    if not name or not uid or not _is_valid_uid(uid):
        raise HTTPException(403, "Lease validation failed")
    if not namespace or not expected_name or not expected_instance:
        logger.warning("stateless claimant attestation misconfigured: no authority")
        raise HTTPException(503, "Claimant authority unavailable")
    if core_api is None:
        logger.info("stateless claimant attestation unavailable: no kubernetes client")
        raise HTTPException(503, "Claimant authority unavailable")
    try:
        pod = await run_bounded_k8s_call(
            core_api.read_namespaced_pod,
            name=name,
            namespace=namespace,
            request_timeout=K8S_READ_REQUEST_TIMEOUT,
        )
    except Exception as exc:
        if getattr(exc, "status", None) == 404:
            raise HTTPException(403, "Lease validation failed") from None
        logger.info("stateless claimant attestation read failed: %s", exc)
        raise HTTPException(503, "Claimant authority unavailable") from None
    actual_uid = str(getattr(getattr(pod, "metadata", None), "uid", "") or "")
    if not actual_uid or actual_uid != uid:
        raise HTTPException(403, "Lease validation failed")
    if claimant_lifecycle_refused(pod):
        raise HTTPException(403, "Lease validation failed")
    mismatch = claimant_pool_mismatch_reason(
        pod, expected_name=expected_name, expected_instance=expected_instance
    )
    if mismatch is not None:
        raise HTTPException(403, "Lease validation failed")


def build_claimant_attestor(
    *,
    core_api_factory: Callable[[], Any | None] | None = None,
    namespace_factory: Callable[[], str] | None = None,
    identity_factory: Callable[[], tuple[str, str]] | None = None,
) -> Callable[[str, str], Any]:
    """Build the injected ``attest_stateless_claimant`` collaborator.

    Factories (not captured values) so process-startup configuration changes
    and test ``monkeypatch.setenv`` remain visible per claim.
    """

    async def _attest(pod_name: str, pod_uid: str) -> None:
        from orchestrator.services.stateless_capacity import load_core_api

        factory = core_api_factory or load_core_api
        try:
            core_api = await asyncio.to_thread(factory)
        except Exception as exc:
            logger.info("stateless claimant attestation client failed: %s", exc)
            raise HTTPException(503, "Claimant authority unavailable") from None
        namespace = (namespace_factory or executor_namespace)()
        expected_name, expected_instance = (identity_factory or pool_identity)()
        await attest_stateless_executor_pod(
            pod_name,
            pod_uid,
            core_api=core_api,
            namespace=namespace,
            expected_name=expected_name,
            expected_instance=expected_instance,
        )

    return _attest


__all__ = [
    "K8S_READ_REQUEST_TIMEOUT",
    "STATELESS_CLASS_VALUE",
    "STATELESS_COMPONENT_VALUE",
    "attest_stateless_executor_pod",
    "build_claimant_attestor",
    "claimant_lifecycle_refused",
    "claimant_pool_mismatch_reason",
    "executor_namespace",
    "pool_identity",
]
