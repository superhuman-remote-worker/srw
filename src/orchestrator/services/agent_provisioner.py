"""Unified Agent Provisioner — On-demand pod lifecycle for jobs and sessions.

Creates ephemeral K8s Pods for dual-mode agents. Each pod handles exactly one
task (job or session), then exits (restartPolicy: Never). Replaces both the
static agent Deployment and the PersistentProvisioner.

Lifecycle:
    provision_agent()        — pending job or new session → create agent pod
    delete_agent_pod()       — explicit cleanup by pod name
    delete_agent_pod_by_thread() — cleanup by thread_id label
    reap_pods()              — periodic GC (completed / stale / unstartable)
    ensure_warm_pool()       — maintain MIN_AGENTS idle pods for responsiveness

Docker Compose mode is unaffected — agents use the static container pool.
"""

import asyncio
import logging
import os
import shlex
from datetime import datetime, timezone
from typing import Any, Optional
from uuid import UUID, uuid4

from shared.runtime.core.loader import canonical_config_name

from orchestrator.services.agent_pod_entrypoint import (
    agent_exec_command,
    validate_config_name,
)
from orchestrator.services.runtime_actor import (
    issue_runtime_actor_bootstrap,
    issue_runtime_actor_pod_bootstrap,
)
from orchestrator.services.pinned_k8s_effect import (
    PINNED_AUTHORITY_FINALIZER,
    PINNED_AUTHORITY_PROTECTION_PROTOCOL,
    discover_exact_pinned_pod_authority,
    fence_unmodified_planned_pod_authority,
    finalizer_release_patch,
    legacy_pinned_namespace_candidates,
    observe_planned_pinned_pod_authority,
    pod_containers_are_terminal,
    protect_planned_pinned_pod_authority,
    protect_legacy_pinned_agent_authority as protect_legacy_pinned_objects,
    release_planned_pinned_pod_authority,
    retire_terminal_claimant_pod,
    run_bounded_k8s_call,
    run_bounded_k8s_mutation,
)
from orchestrator.services.session_runtime_admission import (
    ThreadRuntimeAuthority,
    same_thread_runtime_authority,
    thread_runtime_authority,
)

logger = logging.getLogger(__name__)

# Must exceed the executor's 120s shutdown/abort budget plus local backend and
# durable claimant-ACK drain. A shorter Kubernetes grace can SIGKILL the only
# actor capable of proving SFTP quiescence and leave the loss ledger absorbing.
STATELESS_CLAIMANT_EVICTION_GRACE_SECONDS = int(
    os.environ.get("STATELESS_CLAIMANT_EVICTION_GRACE_SECONDS", "180")
)

# Retention boundary for pooled stateless executor Pods, stamped by the chart
# (helm/templates/agent/stateless-deployment.yaml). Claimant-loss debt settles
# only on an observed exact UID with every container terminated; without this
# finalizer the kubelet removes the object ~0.7 s after the containers stop and
# the debt becomes unsettleable (a 404 is never process-zero proof). The
# run_queue reaper releases it once the Pod is process-zero and no durable
# claimant authority still names the UID.
STATELESS_EXECUTOR_PROCESS_ZERO_FINALIZER = (
    "lifecycle.srw.dev/stateless-executor-process-zero"
)
STATELESS_EXECUTOR_LABEL_SELECTOR = (
    "srw/class=agent-stateless,app.kubernetes.io/component=agent-stateless"
)
STATELESS_EXECUTOR_READ_TIMEOUT = (5.0, 15.0)
# The sanctioned human assertion that a node is powered off (non-graceful
# node shutdown). PodGC force-deletes its Pods without any kubelet report.
NODE_OUT_OF_SERVICE_TAINT = "node.kubernetes.io/out-of-service"


class AgentPodObservationError(RuntimeError):
    """The Kubernetes API could not answer (anything but an exact 404)."""


def _executor_statuses(status: Any, field: str) -> list[Any] | None:
    raw = getattr(status, field, None)
    if raw is None:
        return []
    return list(raw) if isinstance(raw, (list, tuple)) else None


def _executor_status_started(container: Any) -> bool:
    state = getattr(container, "state", None)
    last = getattr(container, "last_state", None)
    return bool(
        getattr(state, "running", None) is not None
        or getattr(state, "terminated", None) is not None
        or getattr(last, "running", None) is not None
        or getattr(last, "terminated", None) is not None
        or getattr(container, "container_id", None)
        or getattr(container, "started", None) is True
        or getattr(container, "ready", None) is True
        or int(getattr(container, "restart_count", 0) or 0) != 0
    )


def _executor_containers_quiescent(pod: Any) -> bool:
    """Every regular and init container terminated; no ephemeral one running.

    ``pinned_k8s_effect.pod_containers_are_terminal`` also demands terminated
    ephemeral statuses, but the kubelet's terminal rewrite never touches them:
    a ``kubectl debug`` container whose image never pulled stays ``waiting``
    forever and would retain the Pod indefinitely. An ephemeral container with
    no start evidence has no process, so it is accepted; declared regular and
    init containers must all report terminated.
    """

    spec = getattr(pod, "spec", None)
    status = getattr(pod, "status", None)
    for spec_field, status_field, unstarted_ok in (
        ("containers", "container_statuses", False),
        ("init_containers", "init_container_statuses", False),
        ("ephemeral_containers", "ephemeral_container_statuses", True),
    ):
        statuses = _executor_statuses(status, status_field)
        if statuses is None or (spec_field == "containers" and not statuses):
            return False
        declared = {str(c.name) for c in getattr(spec, spec_field, None) or []}
        observed = {str(getattr(s, "name", "")) for s in statuses}
        if not unstarted_ok and not declared.issubset(observed):
            return False
        for container in statuses:
            state = getattr(container, "state", None)
            if getattr(state, "terminated", None) is not None:
                continue
            if unstarted_ok and not _executor_status_started(container):
                continue
            return False
    return True


def _deleting_executor_never_scheduled(pod: Any) -> bool:
    """A deleting Pod without ``nodeName`` never ran any process.

    Mirrors the workspace rule (container_provisioner.
    ``_deleting_pod_was_never_scheduled``; duplicated rather than imported to
    keep this module free of a provisioner-to-provisioner import): the binding
    subresource refuses a Pod that is being deleted, so no kubelet can ever
    start its containers. The phase is deliberately not consulted: PodGC's
    unscheduled-terminating sweep flips such a Pod to ``Failed`` before its
    force delete, and a phase gate would then retain it forever. Any
    contradictory container status is ambiguous.
    """

    spec = getattr(pod, "spec", None)
    status = getattr(pod, "status", None)
    if getattr(spec, "node_name", None):
        return False
    for field in (
        "container_statuses",
        "init_container_statuses",
        "ephemeral_container_statuses",
    ):
        statuses = _executor_statuses(status, field)
        if statuses is None or any(_executor_status_started(s) for s in statuses):
            return False
    return True


def _deleting_executor_never_initialized(pod: Any) -> bool:
    """A Pod killed inside ``wait-for-orchestrator`` never ran the executor.

    The kubelet's terminal status rewrite marks only initialized Pods' waiting
    containers terminated; a Pod deleted during init keeps its regular
    containers ``waiting: PodInitializing`` forever, so the strict all-terminal
    rule would retain it indefinitely. Require the kubelet's own terminal
    phase, every init container terminated with at least one failure (a
    successful init would have initialized the Pod and moved the containers
    past ``PodInitializing``), and no start evidence on any regular container.
    """

    spec = getattr(pod, "spec", None)
    status = getattr(pod, "status", None)
    if getattr(status, "phase", None) not in {"Failed", "Succeeded"}:
        return False
    # PodGC writes a terminal phase for Pods on lost/out-of-service nodes
    # without any kubelet having stopped them; their statuses are frozen.
    if any(
        getattr(condition, "type", None) == "DisruptionTarget"
        and getattr(condition, "reason", None) == "DeletionByPodGC"
        for condition in getattr(status, "conditions", None) or []
    ):
        return False
    init_statuses = _executor_statuses(status, "init_container_statuses")
    statuses = _executor_statuses(status, "container_statuses")
    ephemeral = _executor_statuses(status, "ephemeral_container_statuses")
    if not init_statuses or not statuses or ephemeral is None:
        return False
    for declared_field, observed in (
        ("init_containers", init_statuses),
        ("containers", statuses),
    ):
        declared = {str(c.name) for c in getattr(spec, declared_field, None) or []}
        if not declared.issubset({str(getattr(s, "name", "")) for s in observed}):
            return False
    init_terminal = [
        getattr(getattr(s, "state", None), "terminated", None) for s in init_statuses
    ]
    if any(item is None for item in init_terminal) or all(
        int(getattr(item, "exit_code", 0) or 0) == 0 for item in init_terminal
    ):
        return False
    for container in statuses:
        state = getattr(container, "state", None)
        if getattr(state, "terminated", None) is not None:
            continue
        waiting = getattr(state, "waiting", None)
        if getattr(
            waiting, "reason", None
        ) != "PodInitializing" or _executor_status_started(container):
            return False
    return all(
        getattr(getattr(s, "state", None), "terminated", None) is not None
        for s in ephemeral
    )


def stateless_executor_process_zero(pod: Any) -> bool:
    """Whether one deleting executor Pod can never run a process again.

    The finalizer-release predicate: every declared container observed
    terminated (the exact-terminal proof the claimant-loss reconciler settles
    on), or the Pod provably never started its executor. Running or frozen
    statuses on a lost node are never container evidence; the reaper's
    separate node-lost rule (Node absent or out-of-service) and an operator
    attestation cover that case.
    """

    if getattr(getattr(pod, "metadata", None), "deletion_timestamp", None) is None:
        return False
    return (
        _executor_containers_quiescent(pod)
        or _deleting_executor_never_scheduled(pod)
        or _deleting_executor_never_initialized(pod)
    )


def classify_agent_pod_authority(pod: Any, expected_pod_uid: str) -> str:
    """Classify one read of a claimant Pod name against the expected UID.

    ``exact_terminal`` requires the exact UID with every container observed
    terminated; ``replacement`` is a same-name successor; deletion grace and
    phase Unknown remain ``unknown`` because the old process may still run.
    """

    actual_uid = str(getattr(getattr(pod, "metadata", None), "uid", "") or "")
    if not actual_uid:
        return "unknown"
    if actual_uid != str(expected_pod_uid):
        return "replacement"
    phase = str(getattr(getattr(pod, "status", None), "phase", "") or "")
    statuses = getattr(pod.status, "container_statuses", None) or []
    all_containers_terminated = bool(statuses) and all(
        getattr(getattr(status, "state", None), "terminated", None) is not None
        for status in statuses
    )
    # A graceful deletion may stamp deletionTimestamp before the final
    # terminal phase update.  Exact UID plus every container's terminated
    # state is the process-level proof we need; deletionTimestamp alone is
    # deliberately not proof.
    if all_containers_terminated:
        return "exact_terminal"
    if getattr(getattr(pod, "metadata", None), "deletion_timestamp", None):
        return "unknown"
    if phase in {"Running", "Pending"}:
        return "exact_live"
    # A terminal phase with missing/partial container status is still
    # ambiguous: the API object is authoritative only when every claimant
    # container has an observed terminal state.
    return "unknown"


def _env_flag(name: str, default: bool) -> bool:
    """Parse a boolean env var exactly the way ContainerProvisioner does.

    Duplicated rather than imported to keep this module free of a
    provisioner-to-provisioner import, but the parsing MUST stay identical:
    session-agent PVCs are gated on the *same* ``WORKSPACE_PVC_ENABLED`` flag
    as workspace PVCs, and a value that read as "on" for one and "off" for the
    other would give the cluster a silent, half-applied durability setting.
    """
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() not in {"0", "false", "no", "off"}


def _normalize_config_name(config_name: str, purpose: str) -> str:
    """Guard against an expert UUID leaking into ``config_name``.

    ``config_name`` must name a bundled config that resolves to an on-disk
    ``<name>.yaml``. The cockpit's expert picker puts the expert UUID here
    instead, and ``--config <uuid>`` crashes agent startup (no such file). A
    bound expert is applied via the thread's ``config_override`` (sessions) or
    ``AGENT_EXPERT_ID`` (jobs), so a UUID in this slot is always wrong — boot
    the purpose's base config instead. See
    knowledge-history/done/global_expert_management.md.

    This is also the provisioner boundary for the value: ``config_name`` is
    the one caller-controlled word in the pod's ``sh -c`` entrypoint, so it is
    validated here — before any Kubernetes call or pod spec — and the
    entrypoint itself is built from a quoted argv (``agent_exec_command``).
    Security audit 2026-08-27, finding #3."""
    if not config_name:
        return config_name
    config_name = validate_config_name(config_name)
    try:
        UUID(str(config_name))
    except (ValueError, TypeError, AttributeError):
        return canonical_config_name(config_name)
    base = "worker_base" if purpose == "job" else "session_base"
    logger.warning(
        "agent config_name %s is a UUID (expert id in the config slot); "
        "booting base %s instead — expert applies via config_override.",
        config_name,
        base,
    )
    return base


# Pending-phase container waiting reasons we treat as unrecoverable past grace.
# All reflect bad configuration or missing dependencies that the kubelet will
# retry forever without making progress — deleting the pod lets the scaler
# recreate it with the current config.
_TERMINAL_WAITING_REASONS: frozenset[str] = frozenset(
    {
        "CreateContainerConfigError",
        "CreateContainerError",
        "ImagePullBackOff",
        "ErrImagePull",
        "InvalidImageName",
        "RunContainerError",
    }
)


class AgentProvisioner:
    """Provisions agent pods on demand via Kubernetes API.

    Follows the same singleton + ``connect(db)`` pattern as
    ContainerProvisioner and PersistentProvisioner.
    """

    def __init__(self) -> None:
        self._db: Optional[Any] = None
        self._core_api: Optional[Any] = None
        self._k8s_available: bool = False
        self._in_cluster: bool = False
        self._namespace: str = os.environ.get(
            "AGENT_NAMESPACE",
            os.environ.get("WORKSPACE_NAMESPACE", "superhuman-remote-worker"),
        )
        self._agent_image: str = os.environ.get(
            "AGENT_IMAGE",
            os.environ.get(
                "PERSISTENT_AGENT_IMAGE",
                "ghcr.io/superhuman-remote-worker/srw-agent:latest",
            ),
        )
        self._configmap_name: str = os.environ.get("AGENT_CONFIGMAP", "srw-config")
        self._secret_name: str = os.environ.get("AGENT_SECRET", "srw")
        self._ssh_secret_name: str = os.environ.get(
            "WORKSPACE_SSH_SECRET", "vm-ssh-key"
        )
        # Durable /workspace for SESSION agent pods. Gated on the SAME flag,
        # size and storage class as workspace PVCs (ContainerProvisioner) so a
        # cluster has one storage switch, not two. Off (the default) → the
        # emptyDir behavior this file has always had. Job agent pods are never
        # PVC-backed: they are stateless dispatch runners whose durable state
        # lives in the workspace pod, the job repo and Postgres. See the volume
        # list in _build_pod_manifest for the session rationale.
        self._pvc_enabled: bool = _env_flag("WORKSPACE_PVC_ENABLED", False)
        self._pvc_size: str = os.environ.get("WORKSPACE_PVC_SIZE", "10Gi")
        self._storage_class: str = os.environ.get(
            "WORKSPACE_STORAGE_CLASS", "longhorn-ephemeral"
        )
        self._max_agents: int = int(os.environ.get("MAX_AGENTS", "10"))
        self._min_agents: int = int(os.environ.get("MIN_AGENTS", "0"))
        self._agent_buffer: int = int(os.environ.get("AGENT_BUFFER", "0"))
        self._reserved_session_slots: int = int(
            os.environ.get("RESERVED_SESSION_SLOTS", "0")
        )
        self._reserved_job_slots: int = int(os.environ.get("RESERVED_JOB_SLOTS", "0"))
        self._label_selector: str = "srw/managed-by=agent-provisioner"
        # Standard Helm chart labels for chart-managed NetworkPolicies.
        # Without these, the database NetworkPolicies (which match on
        # app.kubernetes.io/{name,instance,component}=agent) reject ingress
        # from dynamically-provisioned agent pods. Injected by the chart's
        # orchestrator Deployment; defaults match the homelab values.
        self._chart_label_name: str = os.environ.get("AGENT_LABEL_NAME", "").strip()
        self._chart_label_instance: str = os.environ.get(
            "AGENT_LABEL_INSTANCE", ""
        ).strip()
        self._tailscale_enabled: bool = os.environ.get(
            "AGENT_TAILSCALE_ENABLED", "false"
        ).strip().lower() in ("true", "1", "yes")
        self._headscale_url: str = os.environ.get("HEADSCALE_URL", "").strip()
        # Sustained-dark window for the tailscale sidecar liveness probe.
        # After this long without a Running backend, the kubelet kills the
        # sidecar and the tunnel_dark reaper recycles the pod. The self-heal
        # loop recovers transient loss well inside this window.
        self._tailscale_dark_timeout: int = int(
            os.environ.get("AGENT_TAILSCALE_DARK_TIMEOUT_SECONDS", "600")
        )
        # host/port for the agent's `wait-for-orchestrator` init container,
        # derived from the chart-injected ORCHESTRATOR_URL (default tracks
        # the dev release name).
        from urllib.parse import urlparse

        _orch = urlparse(
            os.environ.get("ORCHESTRATOR_URL", "http://srw-orchestrator:8085")
        )
        self._orchestrator_host: str = _orch.hostname or "srw-orchestrator"
        self._orchestrator_port: int = _orch.port or 8085

    # =========================================================================
    # Properties
    # =========================================================================

    @property
    def is_available(self) -> bool:
        """Whether K8s provisioning is available."""
        return self._k8s_available

    @property
    def in_cluster(self) -> bool:
        """True if connected via in-cluster config (running inside K8s)."""
        return self._in_cluster

    @property
    def max_agents(self) -> int:
        return self._max_agents

    @property
    def min_agents(self) -> int:
        return self._min_agents

    # =========================================================================
    # Initialization
    # =========================================================================

    def connect(self, db: Any) -> None:
        """Initialize provisioner with database connection."""
        self._db = db
        self._init_k8s()

        if self._k8s_available:
            logger.info(
                "AgentProvisioner ready (namespace=%s, image=%s, "
                "max=%d, min=%d, buffer=%d, "
                "reserved_session=%d, reserved_job=%d)",
                self._namespace,
                self._agent_image,
                self._max_agents,
                self._min_agents,
                self._agent_buffer,
                self._reserved_session_slots,
                self._reserved_job_slots,
            )
        else:
            logger.info(
                "AgentProvisioner: K8s not available — "
                "agents must be started manually or via Docker pool"
            )

    def _init_k8s(self) -> None:
        """Try to initialize K8s client."""
        try:
            from kubernetes import client as k8s_client
            from kubernetes import config as k8s_config

            in_cluster = False
            try:
                k8s_config.load_incluster_config()
                in_cluster = True
            except k8s_config.ConfigException:
                try:
                    k8s_config.load_kube_config()
                except k8s_config.ConfigException:
                    logger.info("K8s not available for agent provisioning")
                    return

            self._core_api = k8s_client.CoreV1Api()
            self._k8s_available = True
            self._in_cluster = in_cluster
        except ImportError:
            logger.info(
                "kubernetes package not installed — agent provisioning disabled"
            )

    # =========================================================================
    # Pod lifecycle
    # =========================================================================

    async def _session_runtime_authority(
        self, thread_id: str | None
    ) -> ThreadRuntimeAuthority | None:
        if not thread_id or self._db is None:
            return None
        try:
            return thread_runtime_authority(await self._db.get_thread(thread_id))
        except Exception:
            logger.exception(
                "Session runtime generation read failed before provisioning: %s",
                thread_id,
            )
            return None

    async def _same_session_runtime_authority(
        self, expected: ThreadRuntimeAuthority | None
    ) -> bool:
        if expected is None or self._db is None:
            return False
        try:
            return same_thread_runtime_authority(
                await self._db.get_thread(expected.thread_id), expected
            )
        except Exception:
            logger.exception(
                "Session runtime generation recheck failed: %s", expected.thread_id
            )
            return False

    async def provision_agent(
        self,
        purpose: str,
        thread_id: Optional[str] = None,
        config_name: str = "worker_base",
        expert_id: Optional[str] = None,
        cpu_request: str = "250m",
        memory_request: str = "512Mi",
        cpu_limit: str = "1000m",
        memory_limit: str = "2Gi",
        expected_runtime_generation: str | None = None,
    ) -> Optional[str]:
        """Create an on-demand agent pod.

        Args:
            purpose: ``"job"`` or ``"session"``.
            thread_id: Thread UUID (required for session purpose).
            config_name: Agent config to use.
            cpu_request/memory_request: Resource requests.
            cpu_limit/memory_limit: Resource limits.

        Returns:
            Pod name if created, None if at capacity or on error.

        Raises:
            InvalidConfigNameError: ``config_name`` is not a bundled-config
                selector. Raised before any Kubernetes or database call.
        """
        # Boundary check first: a bad name is not worth a K8s round-trip, and
        # the manifest builder must never see one.
        config_name = _normalize_config_name(config_name, purpose)
        if not self._k8s_available:
            logger.info(
                "K8s not available — start agent manually: "
                "python -m agent --port 8001 --loop"
            )
            return None
        if purpose == "session" and expected_runtime_generation is not None:
            session_authority = ThreadRuntimeAuthority(
                thread_id=str(thread_id or ""),
                generation=str(expected_runtime_generation),
            )
            if not await self._same_session_runtime_authority(session_authority):
                return None
        else:
            session_authority = (
                await self._session_runtime_authority(thread_id)
                if purpose == "session"
                else None
            )
        if purpose == "session" and session_authority is None:
            logger.info(
                "Refusing agent pod provision for non-preparable session %s",
                thread_id,
            )
            return None

        # Capacity check with reservation-aware eviction
        counts = await self.active_counts_by_purpose()
        if purpose == "session" and not await self._same_session_runtime_authority(
            session_authority
        ):
            return None
        total = counts["total"]
        if total >= self._max_agents:
            # At capacity — try to evict an idle agent of the OTHER purpose
            # if this purpose has reserved slots configured.
            evicted = await self._try_evict_for_reservation(purpose, counts)
            if purpose == "session" and not await self._same_session_runtime_authority(
                session_authority
            ):
                return None
            if not evicted:
                logger.warning(
                    "Agent pool at capacity (%d/%d) — cannot provision new %s agent",
                    total,
                    self._max_agents,
                    purpose,
                )
                return None
            # One slot freed via eviction — fall through to create pod

        # Reservation check: ensure this purpose doesn't starve the other
        if purpose == "job" and self._reserved_session_slots > 0:
            job_ceiling = self._max_agents - self._reserved_session_slots
            if counts["job"] >= job_ceiling:
                logger.warning(
                    "Job agents at reservation ceiling (%d/%d, "
                    "%d slots reserved for sessions)",
                    counts["job"],
                    job_ceiling,
                    self._reserved_session_slots,
                )
                return None
        elif purpose == "session" and self._reserved_job_slots > 0:
            session_ceiling = self._max_agents - self._reserved_job_slots
            if counts["session"] >= session_ceiling:
                logger.warning(
                    "Session agents at reservation ceiling (%d/%d, "
                    "%d slots reserved for jobs)",
                    counts["session"],
                    session_ceiling,
                    self._reserved_job_slots,
                )
                return None

        short_id = uuid4().hex[:8]
        pod_name = f"srw-agent-{purpose[0]}-{short_id}"
        provision_attempt: str | None = None
        intent_namespace = self._namespace

        # Session agents get a PVC-backed /workspace; job agents keep emptyDir.
        # The pod name is random per provision (srw-agent-s-<8hex>), so the
        # volume identity has to come from the *thread* instead — that is what
        # makes a recycled agent pod (drift drain, crash, node loss, version
        # upgrade) reattach the same data rather than boot onto empty scratch.
        #
        # The name is deliberately NOT `pvc-persistent-<id>`: that belongs to
        # the legacy PersistentProvisioner pod path. Sharing one RWO claim
        # between both paths would wedge whichever pod attached second if the
        # two ever coexist for a single thread.
        pvc_name: Optional[str] = None
        if self._pvc_enabled and purpose == "session" and thread_id:
            pvc_name = f"pvc-agent-s-{thread_id[:12]}"

        if purpose == "session" and thread_id:
            # Persist both the chosen Pod name and the deterministic PVC claim
            # before either Kubernetes effect. A timeout can otherwise leave
            # a bootstrap-bearing Pod or user-data volume with no UID owner.
            candidate_attempt = str(uuid4())
            intent = await self._db.reserve_pinned_agent_pod_provision_intent(
                thread_id,
                expected_runtime_generation=session_authority.generation,
                attempt_id=candidate_attempt,
                pod_name=pod_name,
                provisioner="agent",
                namespace=self._namespace,
                protection_protocol=PINNED_AUTHORITY_PROTECTION_PROTOCOL,
                pvc_name=pvc_name,
            )
            if not isinstance(intent, dict):
                return None
            pod_name = str(intent.get("pod_name") or "")
            provision_attempt = str(intent.get("attempt_id") or "")
            intent_namespace = str(intent.get("namespace") or "")
            if not pod_name or not provision_attempt or not intent_namespace:
                return None
            workspace_claim = intent.get("workspace_claim")
            if pvc_name:
                if not isinstance(workspace_claim, dict):
                    return None
                claim_id = str(workspace_claim.get("claim_id") or "")
                claim_attempt = str(workspace_claim.get("create_attempt") or "")
                claim_uid = await self._ensure_pinned_agent_pvc(
                    pvc_name,
                    thread_id=thread_id,
                    runtime_generation=str(
                        workspace_claim.get("created_runtime_generation") or ""
                    ),
                    claim_id=claim_id,
                    create_attempt=claim_attempt,
                    expected_pvc_uid=(
                        str(workspace_claim.get("pvc_uid") or "") or None
                    ),
                    namespace=str(workspace_claim.get("namespace") or ""),
                )
                if (
                    not claim_uid
                    or not await self._db.publish_pinned_agent_workspace_claim(
                        thread_id,
                        expected_runtime_generation=session_authority.generation,
                        claim_id=claim_id,
                        pvc_name=pvc_name,
                        pvc_uid=claim_uid,
                        namespace=str(workspace_claim.get("namespace") or ""),
                    )
                ):
                    logger.error(
                        "Session agent PVC %s lacks exact durable authority for %s",
                        pvc_name,
                        thread_id,
                    )
                    return None

        runtime_actor_bootstrap: Optional[str] = None
        runtime_actor_pod_bootstrap: Optional[str] = None
        if purpose == "session" and thread_id:
            try:
                runtime_actor_bootstrap = await issue_runtime_actor_bootstrap(
                    self._db, thread_id
                )
                if not await self._same_session_runtime_authority(session_authority):
                    return None
            except Exception:
                logger.exception(
                    "Could not issue runtime actor bootstrap for session %s; "
                    "refusing to provision an identity-less pod",
                    thread_id,
                )
                return None
        else:
            # A job pod is also a future warm-pool session host: when it goes
            # idle, provision_or_assign may hand it a thread instead of paying
            # for a dedicated pod. That thread does not exist yet, so the pod
            # gets a thread-less bootstrap it can bind at attach time. Without
            # it the pod would attach with no actor identity and every machine
            # tag / charter write on that session would be denied — silently,
            # several tool calls later.
            try:
                runtime_actor_pod_bootstrap = await issue_runtime_actor_pod_bootstrap(
                    self._db
                )
            except Exception:
                logger.exception(
                    "Could not issue pod runtime actor bootstrap; refusing to "
                    "provision an identity-less pool pod"
                )
                return None

        manifest = self._build_pod_manifest(
            pod_name=pod_name,
            purpose=purpose,
            thread_id=thread_id,
            config_name=config_name,
            expert_id=expert_id,
            cpu_request=cpu_request,
            memory_request=memory_request,
            cpu_limit=cpu_limit,
            memory_limit=memory_limit,
            pvc_name=pvc_name,
            runtime_actor_bootstrap=runtime_actor_bootstrap,
            runtime_actor_pod_bootstrap=runtime_actor_pod_bootstrap,
            session_runtime_generation=(
                session_authority.generation if session_authority else None
            ),
            provision_attempt=provision_attempt,
            namespace=intent_namespace,
        )

        if purpose == "session" and not await self._same_session_runtime_authority(
            session_authority
        ):
            return None

        try:
            created_pod = await run_bounded_k8s_mutation(
                self._core_api.create_namespaced_pod,
                namespace=intent_namespace,
                body=manifest,
            )
            created_uid = str(
                getattr(getattr(created_pod, "metadata", None), "uid", "") or ""
            )
            if not created_uid:
                try:
                    observed = await asyncio.to_thread(
                        self._core_api.read_namespaced_pod,
                        name=pod_name,
                        namespace=intent_namespace,
                    )
                    observed_labels = dict(
                        getattr(getattr(observed, "metadata", None), "labels", None)
                        or {}
                    )
                    if (
                        observed_labels.get("srw.io/runtime-generation")
                        == (session_authority.generation if session_authority else None)
                        and observed_labels.get("srw.io/thread-id") == str(thread_id)
                        and observed_labels.get("srw.io/provision-attempt")
                        == provision_attempt
                    ):
                        created_uid = str(
                            getattr(getattr(observed, "metadata", None), "uid", "")
                            or ""
                        )
                except Exception:
                    logger.warning(
                        "Could not resolve exact created agent Pod identity: %s",
                        pod_name,
                    )
            if purpose == "session" and not await self._same_session_runtime_authority(
                session_authority
            ):
                logger.info(
                    "Session %s ended during pod creation; deleting orphan %s",
                    thread_id,
                    pod_name,
                )
                if created_uid:
                    await self.delete_agent_pod_exact(
                        pod_name,
                        expected_pod_uid=created_uid,
                        namespace=intent_namespace,
                    )
                return None
            logger.info(
                "Agent pod created: %s (purpose=%s, config=%s)",
                pod_name,
                purpose,
                config_name,
            )

            # For sessions, store pod info in thread metadata
            if purpose == "session" and thread_id:
                if not await self._db.publish_pinned_agent_pod_provision_intent(
                    thread_id,
                    expected_runtime_generation=session_authority.generation,
                    attempt_id=provision_attempt,
                    pod_name=pod_name,
                    pod_uid=created_uid,
                    namespace=intent_namespace,
                ):
                    if created_uid:
                        await self.delete_agent_pod_exact(
                            pod_name,
                            expected_pod_uid=created_uid,
                            namespace=intent_namespace,
                        )
                    return None
                now_iso = datetime.now(timezone.utc).isoformat()
                published = await self._set_thread_context(
                    thread_id,
                    {
                        "status": "created",
                        "pod_name": pod_name,
                        "namespace": intent_namespace,
                        "pod_uid": created_uid or None,
                        "provision_attempt": provision_attempt,
                        "runtime_generation": session_authority.generation,
                        "created_at": now_iso,
                        "updated_at": now_iso,
                    },
                    expected_runtime_generation=session_authority.generation,
                )
                if (
                    not created_uid
                    or not published
                    or not await self._same_session_runtime_authority(session_authority)
                ):
                    if created_uid:
                        await self.delete_agent_pod_exact(
                            pod_name,
                            expected_pod_uid=created_uid,
                            namespace=intent_namespace,
                        )
                    return None

            return pod_name
        except Exception as e:
            if hasattr(e, "status") and e.status == 409:
                try:
                    incumbent = await asyncio.to_thread(
                        self._core_api.read_namespaced_pod,
                        name=pod_name,
                        namespace=intent_namespace,
                    )
                except Exception:
                    return None
                labels = dict(
                    getattr(getattr(incumbent, "metadata", None), "labels", None) or {}
                )
                exact = purpose != "session" or (
                    session_authority is not None
                    and labels.get("srw.io/runtime-generation")
                    == session_authority.generation
                    and labels.get("srw.io/thread-id") == str(thread_id)
                    and labels.get("srw.io/provision-attempt") == provision_attempt
                    and await self._same_session_runtime_authority(session_authority)
                )
                if exact:
                    incumbent_uid = str(
                        getattr(getattr(incumbent, "metadata", None), "uid", "") or ""
                    )
                    if purpose == "session" and (
                        not incumbent_uid
                        or not await self._db.publish_pinned_agent_pod_provision_intent(
                            thread_id,
                            expected_runtime_generation=session_authority.generation,
                            attempt_id=provision_attempt,
                            pod_name=pod_name,
                            pod_uid=incumbent_uid,
                            namespace=intent_namespace,
                        )
                    ):
                        return None
                    logger.info("Exact agent pod already exists: %s", pod_name)
                    return pod_name
                logger.warning("Conflicting agent pod exists: %s", pod_name)
                return None

            logger.error("Failed to create agent pod %s: %s", pod_name, e)
            if purpose == "session" and thread_id:
                # A non-409 exception can be an accepted-then-timeout. Leave
                # the durable intent planned; a retry/leader/End observes the
                # exact attempt-labelled name and either promotes its UID or
                # deletes it. Never publish a partial ``agent_pod`` failure.
                try:
                    incumbent = await asyncio.to_thread(
                        self._core_api.read_namespaced_pod,
                        name=pod_name,
                        namespace=intent_namespace,
                    )
                    labels = dict(
                        getattr(getattr(incumbent, "metadata", None), "labels", None)
                        or {}
                    )
                    incumbent_uid = str(
                        getattr(getattr(incumbent, "metadata", None), "uid", "") or ""
                    )
                    exact = bool(
                        incumbent_uid
                        and labels.get("srw.io/thread-id") == str(thread_id)
                        and labels.get("srw.io/runtime-generation")
                        == session_authority.generation
                        and labels.get("srw.io/provision-attempt") == provision_attempt
                    )
                    if (
                        exact
                        and await self._same_session_runtime_authority(
                            session_authority
                        )
                        and await self._db.publish_pinned_agent_pod_provision_intent(
                            thread_id,
                            expected_runtime_generation=session_authority.generation,
                            attempt_id=provision_attempt,
                            pod_name=pod_name,
                            pod_uid=incumbent_uid,
                            namespace=intent_namespace,
                        )
                    ):
                        return pod_name
                except Exception:
                    logger.info(
                        "Agent Pod create outcome remains owned by intent %s",
                        provision_attempt,
                    )
            return None

    async def delete_agent_pod(
        self, pod_name: str, *, expected_pod_uid: str | None = None
    ) -> bool:
        """Delete one exact agent Pod, never whichever object owns a name."""

        expected_uid = str(expected_pod_uid or "").strip()
        if not self._k8s_available or not expected_uid:
            logger.warning("Refusing name-only agent Pod deletion for %s", pod_name)
            return False
        return await self.delete_agent_pod_exact(
            pod_name,
            expected_pod_uid=expected_uid,
            namespace=self._namespace,
        )

    async def agent_pod_live(self, pod_name: str) -> Optional[bool]:
        """Return exact pod liveness for terminal claimant reconciliation.

        ``False`` is authoritative only for 404 or a terminal pod whose
        containers are all terminated. API ambiguity returns ``None`` and
        callers must remain fenced. Deletion grace and phase Unknown are not
        quiescence proof: a claimant process may still be running.
        A same-name Running/Pending pod stays live/ambiguous; its mere name is
        never treated as proof that the old claimant process has disappeared.
        """

        if not self._k8s_available or not str(pod_name).strip():
            return None
        try:
            pod = await asyncio.to_thread(
                self._core_api.read_namespaced_pod,
                name=str(pod_name),
                namespace=self._namespace,
            )
        except Exception as exc:
            if getattr(exc, "status", None) == 404:
                return False
            logger.debug("Agent pod liveness probe failed for %s: %s", pod_name, exc)
            return None
        if getattr(getattr(pod, "metadata", None), "deletion_timestamp", None):
            return None
        phase = str(getattr(getattr(pod, "status", None), "phase", "") or "")
        if phase in {"Running", "Pending"}:
            return True
        if phase in {"Failed", "Succeeded"}:
            statuses = getattr(pod.status, "container_statuses", None) or []
            if statuses and all(
                getattr(getattr(status, "state", None), "terminated", None) is not None
                for status in statuses
            ):
                return False
        return None

    async def agent_pod_authority(
        self,
        pod_name: str,
        *,
        expected_pod_uid: str,
        namespace: str | None = None,
    ) -> str:
        """Classify one immutable claimant Pod identity.

        The name is only a lookup key.  ``exact_absent`` is returned solely
        for an API 404 or a demonstrably terminal Pod with the expected UID;
        a same-name replacement is a distinct result so callers never delete
        or otherwise act on the successor object.  Deletion grace and phase
        Unknown remain ambiguous because the old process may still run.
        """

        name = str(pod_name or "").strip()
        expected_uid = str(expected_pod_uid or "").strip()
        if not self._k8s_available or not name or not expected_uid:
            return "unknown"
        try:
            state, _ = await self.observe_agent_pod_exact(
                name, expected_pod_uid=expected_uid, namespace=namespace
            )
        except AgentPodObservationError as exc:
            logger.debug("Agent pod authority probe failed for %s: %s", name, exc)
            return "unknown"
        return state

    async def observe_agent_pod_exact(
        self,
        pod_name: str,
        *,
        expected_pod_uid: str,
        namespace: str | None = None,
    ) -> tuple[str, Any | None]:
        """One bounded raw read: ``(classification, pod object or None)``.

        Only an exact 404 is answered (``exact_absent``); a timeout, 403 or
        any other failure raises ``AgentPodObservationError`` so a caller
        that must fail closed (the operator attestation) can tell "the API
        says gone" from "the API did not answer". No label filtering: the
        classification binds the UID alone.
        """

        name = str(pod_name or "").strip()
        expected_uid = str(expected_pod_uid or "").strip()
        if not self._k8s_available or not name or not expected_uid:
            raise AgentPodObservationError("Kubernetes client unavailable")
        try:
            pod = await run_bounded_k8s_call(
                self._core_api.read_namespaced_pod,
                name=name,
                namespace=namespace or self._namespace,
                request_timeout=STATELESS_EXECUTOR_READ_TIMEOUT,
            )
        except Exception as exc:
            if getattr(exc, "status", None) == 404:
                return "exact_absent", None
            raise AgentPodObservationError(str(exc)) from exc
        return classify_agent_pod_authority(pod, expected_uid), pod

    async def attest_pinned_job_recipient(
        self,
        pod_name: str,
        *,
        expected_pod_uid: str,
        expected_pod_ip: str,
    ) -> bool:
        """Freshly attest one exact, ready job-agent Pod before mutation I/O."""

        name = str(pod_name or "").strip()
        expected_uid = str(expected_pod_uid or "").strip()
        expected_ip = str(expected_pod_ip or "").strip()
        if not self._k8s_available or not name or not expected_uid or not expected_ip:
            return False
        try:
            pod = await run_bounded_k8s_call(
                self._core_api.read_namespaced_pod,
                name=name,
                namespace=self._namespace,
            )
        except Exception as exc:
            logger.debug("Pinned recipient Pod probe failed for %s: %s", name, exc)
            return False

        metadata = getattr(pod, "metadata", None)
        status = getattr(pod, "status", None)
        labels = dict(getattr(metadata, "labels", None) or {})
        if (
            str(getattr(metadata, "name", "") or "") != name
            or str(getattr(metadata, "uid", "") or "") != expected_uid
            or getattr(metadata, "deletion_timestamp", None) is not None
            or str(getattr(status, "phase", "") or "") != "Running"
            or str(getattr(status, "pod_ip", "") or "") != expected_ip
            or labels.get("srw/component") != "agent"
            or labels.get("srw/managed-by") != "agent-provisioner"
            or labels.get("srw/purpose") != "job"
        ):
            return False

        return any(
            str(getattr(item, "name", "") or "") == "agent"
            and getattr(item, "ready", None) is True
            for item in getattr(status, "container_statuses", None) or []
        )

    async def attest_pinned_session_recipient(
        self,
        pod_name: str,
        *,
        thread_id: str,
        expected_runtime_generation: str,
        expected_pod_uid: str,
        expected_pod_ip: str,
        authority_kind: str = "provisioned",
        namespace: str | None = None,
    ) -> bool:
        """Freshly attest one exact, ready pinned-session Pod before I/O.

        Provisioned session Pods carry the thread/generation labels from their
        creation intent. A warm-pool Pod starts with job labels; route
        publication may replace those with the exact thread/generation session
        identity. Its authority in either shape comes from the bound protection
        receipt already selected by PostgreSQL plus the finalizer checked here.
        """

        name = str(pod_name or "").strip()
        tid = str(thread_id or "").strip()
        generation = str(expected_runtime_generation or "").strip()
        expected_uid = str(expected_pod_uid or "").strip()
        expected_ip = str(expected_pod_ip or "").strip()
        captured_namespace = str(namespace or self._namespace).strip()
        kind = str(authority_kind or "").strip()
        if not all(
            (
                self._k8s_available,
                name,
                tid,
                generation,
                expected_uid,
                expected_ip,
                captured_namespace,
            )
        ) or kind not in {"provisioned", "warm_pool"}:
            return False
        try:
            pod = await run_bounded_k8s_call(
                self._core_api.read_namespaced_pod,
                name=name,
                namespace=captured_namespace,
            )
        except Exception as exc:
            logger.debug("Pinned session recipient probe failed for %s: %s", name, exc)
            return False

        metadata = getattr(pod, "metadata", None)
        status = getattr(pod, "status", None)
        labels = dict(getattr(metadata, "labels", None) or {})
        finalizers = {
            str(value) for value in getattr(metadata, "finalizers", None) or []
        }
        provisioned_identity = bool(
            labels.get("srw/purpose") == "session"
            and labels.get("srw.io/thread-id") == tid
            and labels.get("srw.io/runtime-generation") == generation
        )
        warm_identity = bool(
            PINNED_AUTHORITY_FINALIZER in finalizers
            and (labels.get("srw/purpose") == "job" or provisioned_identity)
        )
        if (
            str(getattr(metadata, "name", "") or "") != name
            or str(getattr(metadata, "namespace", "") or captured_namespace)
            != captured_namespace
            or str(getattr(metadata, "uid", "") or "") != expected_uid
            or getattr(metadata, "deletion_timestamp", None) is not None
            or str(getattr(status, "phase", "") or "") != "Running"
            or str(getattr(status, "pod_ip", "") or "") != expected_ip
            or labels.get("srw/component") != "agent"
            or labels.get("srw/managed-by") != "agent-provisioner"
            or (kind == "provisioned" and not provisioned_identity)
            or (kind == "warm_pool" and not warm_identity)
        ):
            return False

        return any(
            str(getattr(item, "name", "") or "") == "agent"
            and getattr(item, "ready", None) is True
            for item in getattr(status, "container_statuses", None) or []
        )

    async def agent_pod_provision_intent_authority(
        self,
        pod_name: str,
        *,
        expected_thread_id: str,
        expected_runtime_generation: str,
        expected_attempt_id: str,
        namespace: str,
    ) -> dict[str, str | None]:
        """Observe one pre-effect Pod name through its immutable labels."""

        name = str(pod_name or "").strip()
        if not self._k8s_available or not name or not namespace:
            return {"state": "unknown", "pod_uid": None}
        try:
            pod = await asyncio.to_thread(
                self._core_api.read_namespaced_pod,
                name=name,
                namespace=namespace,
            )
        except Exception as exc:
            if getattr(exc, "status", None) == 404:
                return {"state": "exact_absent", "pod_uid": None}
            return {"state": "unknown", "pod_uid": None}
        metadata = getattr(pod, "metadata", None)
        labels = dict(getattr(metadata, "labels", None) or {})
        pod_uid = str(getattr(metadata, "uid", "") or "")
        if not pod_uid:
            return {"state": "unknown", "pod_uid": None}
        exact = bool(
            labels.get("srw/managed-by") == "agent-provisioner"
            and labels.get("srw/purpose") == "session"
            and labels.get("srw.io/thread-id") == str(expected_thread_id)
            and labels.get("srw.io/runtime-generation")
            == str(expected_runtime_generation)
            and labels.get("srw.io/provision-attempt") == str(expected_attempt_id)
        )
        is_fence = labels.get("srw.io/provision-fence") == "true"
        return {
            "state": (
                "exact_fence"
                if exact and is_fence
                else "exact_present"
                if exact
                else "replacement"
            ),
            "pod_uid": pod_uid if exact else None,
        }

    async def protect_legacy_pinned_agent_authority(
        self, authority: dict[str, Any]
    ) -> dict[str, Any] | None:
        """Grandfather one exact pre-0200 session Pod/PVC tuple."""

        if not self._k8s_available or self._core_api is None:
            return None
        thread_id = str(authority.get("thread_id") or "")
        generation = str(authority.get("runtime_generation") or "")
        attempt_id = str(authority.get("attempt_id") or "")
        pod_name = str(authority.get("pod_name") or "")
        if not all((thread_id, generation, attempt_id, pod_name)):
            return None
        claim = authority.get("workspace_claim")
        if claim is not None and not isinstance(claim, dict):
            return None
        pvc_labels = None
        pvc_name = None
        expected_pvc_uid = None
        if claim is not None:
            claim_id = str(claim.get("claim_id") or "")
            claim_attempt = str(claim.get("create_attempt") or "")
            pvc_name = str(claim.get("pvc_name") or "")
            if not all((claim_id, claim_attempt, pvc_name)):
                return None
            expected_pvc_uid = str(claim.get("pvc_uid") or "") or None
            pvc_labels = {
                "srw.io/thread-id": thread_id,
                "srw.io/runtime-generation": generation,
                "srw.io/workspace-claim": claim_id,
                "srw.io/provision-attempt": claim_attempt,
                "srw.io/claim-provisioner": "agent",
            }
        return await protect_legacy_pinned_objects(
            self._core_api,
            namespaces=legacy_pinned_namespace_candidates(self._namespace),
            pod_name=pod_name,
            expected_pod_uid=str(authority.get("pod_uid") or "") or None,
            pod_labels={
                "srw/managed-by": "agent-provisioner",
                "srw/purpose": "session",
                "srw.io/thread-id": thread_id,
                "srw.io/runtime-generation": generation,
                "srw.io/provision-attempt": attempt_id,
            },
            pvc_name=pvc_name,
            expected_pvc_uid=expected_pvc_uid,
            pvc_labels=pvc_labels,
        )

    @staticmethod
    def _warm_binding_labels(_authority: dict[str, Any]) -> dict[str, str]:
        return {
            "srw/managed-by": "agent-provisioner",
            "srw/purpose": "job",
        }

    @staticmethod
    def _routed_warm_binding_labels(authority: dict[str, Any]) -> dict[str, str]:
        """Return the exact post-route identity of a protected warm Pod."""

        return {
            "srw/managed-by": "agent-provisioner",
            "srw/purpose": "session",
            "srw.io/thread-id": str(authority.get("thread_id") or ""),
            "srw.io/runtime-generation": str(authority.get("runtime_generation") or ""),
        }

    async def discover_pinned_warm_agent_authority(
        self, authority: dict[str, Any]
    ) -> dict[str, Any] | None:
        """Read one exact pool Pod before Postgres owns its finalizer effect."""

        if not self._k8s_available or self._core_api is None:
            return None
        pod_name = str(authority.get("pod_name") or "")
        pod_uid = str(authority.get("pod_uid") or "")
        if not pod_name or not pod_uid:
            return None
        return await discover_exact_pinned_pod_authority(
            self._core_api,
            namespaces=legacy_pinned_namespace_candidates(self._namespace),
            pod_name=pod_name,
            expected_pod_uid=pod_uid,
            expected_labels=self._warm_binding_labels(authority),
        )

    async def protect_planned_pinned_warm_agent_authority(
        self, authority: dict[str, Any]
    ) -> dict[str, Any] | None:
        """Install protection only after the exact warm plan is durable."""

        if (
            not self._k8s_available
            or self._core_api is None
            or str(authority.get("status") or "") != "protecting"
            or not str(authority.get("effect_token") or "")
        ):
            return None
        return await protect_planned_pinned_pod_authority(
            self._core_api,
            namespace=str(authority.get("namespace") or ""),
            pod_name=str(authority.get("pod_name") or ""),
            expected_pod_uid=str(authority.get("pod_uid") or ""),
            expected_discovered_resource_version=str(
                authority.get("discovered_resource_version") or ""
            ),
            protection_id=str(authority.get("protection_id") or ""),
            effect_token=str(authority.get("effect_token") or ""),
            expected_labels=self._warm_binding_labels(authority),
            allow_current_resource_version=(
                str(authority.get("source") or "") == "legacy_binding"
            ),
        )

    async def fence_expired_pinned_warm_agent_authority(
        self, authority: dict[str, Any]
    ) -> dict[str, Any]:
        """Fence one expired attach effect before returning its Pod to pool."""

        if not self._k8s_available or self._core_api is None:
            return {"state": "unknown"}
        return await fence_unmodified_planned_pod_authority(
            self._core_api,
            namespace=str(authority.get("namespace") or ""),
            pod_name=str(authority.get("pod_name") or ""),
            expected_pod_uid=str(authority.get("pod_uid") or ""),
            protection_id=str(authority.get("protection_id") or ""),
            effect_token=str(authority.get("effect_token") or ""),
            expected_labels=self._warm_binding_labels(authority),
        )

    async def observe_planned_pinned_warm_agent_authority(
        self, authority: dict[str, Any]
    ) -> dict[str, Any]:
        if not self._k8s_available or self._core_api is None:
            return {"state": "unknown", "finalizer_present": None}
        return await observe_planned_pinned_pod_authority(
            self._core_api,
            namespace=str(authority.get("namespace") or ""),
            pod_name=str(authority.get("pod_name") or ""),
            expected_pod_uid=str(authority.get("pod_uid") or ""),
            expected_labels=self._warm_binding_labels(authority),
        )

    async def release_planned_pinned_warm_agent_authority(
        self, authority: dict[str, Any]
    ) -> dict[str, Any] | None:
        if not self._k8s_available or self._core_api is None:
            return None
        released = await release_planned_pinned_pod_authority(
            self._core_api,
            namespace=str(authority.get("namespace") or ""),
            pod_name=str(authority.get("pod_name") or ""),
            expected_pod_uid=str(authority.get("pod_uid") or ""),
            expected_labels=self._warm_binding_labels(authority),
        )
        if released is not None:
            return released
        # Session route publication is UID/RV fenced and adds the exact thread
        # and generation before changing purpose=job to purpose=session.  That
        # is the only alternate label shape this captured warm authority owns.
        routed_labels = self._routed_warm_binding_labels(authority)
        if not all(routed_labels.values()):
            return None
        return await release_planned_pinned_pod_authority(
            self._core_api,
            namespace=str(authority.get("namespace") or ""),
            pod_name=str(authority.get("pod_name") or ""),
            expected_pod_uid=str(authority.get("pod_uid") or ""),
            expected_labels=routed_labels,
        )

    async def fence_agent_pod_provision_intent(
        self,
        pod_name: str,
        *,
        expected_thread_id: str,
        expected_runtime_generation: str,
        expected_attempt_id: str,
        namespace: str,
    ) -> dict[str, str | None]:
        """Causally close an ambiguous create with the same Kubernetes name.

        A linearizable GET returning 404 can run before an already-dispatched
        create commits.  A second CREATE for the same name cannot: either the
        credential-bearing request won and this call observes its UID, or this
        secret-free, unschedulable fence Pod wins and every delayed request is
        forced to ``AlreadyExists``.  The database revokes the attempt before
        callers enter this method.
        """

        name = str(pod_name or "").strip()
        if not self._k8s_available or not name or not namespace:
            return {"state": "unknown", "pod_uid": None}
        labels = {
            "app": "srw-agent",
            "srw/managed-by": "agent-provisioner",
            "srw/purpose": "session",
            "srw.io/thread-id": str(expected_thread_id),
            "srw.io/runtime-generation": str(expected_runtime_generation),
            "srw.io/provision-attempt": str(expected_attempt_id),
            "srw.io/provision-fence": "true",
        }
        manifest = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": name,
                "namespace": namespace,
                "labels": labels,
                "finalizers": [PINNED_AUTHORITY_FINALIZER],
            },
            "spec": {
                "automountServiceAccountToken": False,
                "restartPolicy": "Never",
                # No scheduler in the deployment owns this name. Even if a
                # cluster adds one accidentally, the container has no SRW
                # credentials and exits immediately.
                "schedulerName": "srw-retirement-fence",
                "containers": [
                    {
                        "name": "fence",
                        "image": self._agent_image,
                        "command": ["/bin/sh", "-c", "exit 0"],
                    }
                ],
            },
        }
        incumbent = None
        try:
            incumbent = await run_bounded_k8s_mutation(
                self._core_api.create_namespaced_pod,
                namespace=namespace,
                body=manifest,
            )
        except Exception as exc:
            if getattr(exc, "status", None) != 409:
                # A lost fence response may still have committed. Observe an
                # exact object, but never turn a concurrent 404 into proof.
                try:
                    incumbent = await asyncio.to_thread(
                        self._core_api.read_namespaced_pod,
                        name=name,
                        namespace=namespace,
                    )
                except Exception:
                    return {"state": "unknown", "pod_uid": None}
            else:
                try:
                    incumbent = await asyncio.to_thread(
                        self._core_api.read_namespaced_pod,
                        name=name,
                        namespace=namespace,
                    )
                except Exception:
                    return {"state": "unknown", "pod_uid": None}
        metadata = getattr(incumbent, "metadata", None)
        actual_labels = dict(getattr(metadata, "labels", None) or {})
        pod_uid = str(getattr(metadata, "uid", "") or "")
        exact = bool(
            pod_uid
            and actual_labels.get("srw/managed-by") == "agent-provisioner"
            and actual_labels.get("srw/purpose") == "session"
            and actual_labels.get("srw.io/thread-id") == str(expected_thread_id)
            and actual_labels.get("srw.io/runtime-generation")
            == str(expected_runtime_generation)
            and actual_labels.get("srw.io/provision-attempt")
            == str(expected_attempt_id)
        )
        is_fence = actual_labels.get("srw.io/provision-fence") == "true"
        return {
            "state": (
                "exact_fence"
                if exact and is_fence
                else "exact_original"
                if exact
                else "replacement"
            ),
            "pod_uid": pod_uid if exact else None,
        }

    async def agent_workspace_claim_authority(
        self,
        pvc_name: str,
        *,
        expected_thread_id: str,
        expected_runtime_generation: str,
        expected_claim_id: str,
        expected_create_attempt: str,
        namespace: str,
        expected_pvc_uid: str | None = None,
    ) -> dict[str, str | None]:
        """Classify one durable agent-workspace PVC by immutable labels/UID."""

        name = str(pvc_name or "").strip()
        if not self._k8s_available or not name or not namespace:
            return {"state": "unknown", "pvc_uid": None}
        try:
            claim = await asyncio.to_thread(
                self._core_api.read_namespaced_persistent_volume_claim,
                name=name,
                namespace=namespace,
            )
        except Exception as exc:
            if getattr(exc, "status", None) == 404:
                return {"state": "exact_absent", "pvc_uid": None}
            return {"state": "unknown", "pvc_uid": None}
        metadata = getattr(claim, "metadata", None)
        labels = dict(getattr(metadata, "labels", None) or {})
        pvc_uid = str(getattr(metadata, "uid", "") or "")
        exact = bool(
            pvc_uid
            and labels.get("srw.io/thread-id") == str(expected_thread_id)
            and labels.get("srw.io/runtime-generation")
            == str(expected_runtime_generation)
            and labels.get("srw.io/workspace-claim") == str(expected_claim_id)
            and labels.get("srw.io/provision-attempt") == str(expected_create_attempt)
            and labels.get("srw.io/claim-provisioner") == "agent"
        )
        if expected_pvc_uid and pvc_uid != str(expected_pvc_uid):
            exact = False
        is_fence = labels.get("srw.io/workspace-claim-fence") == "true"
        return {
            "state": (
                "exact_fence"
                if exact and is_fence
                else "exact_present"
                if exact
                else "replacement"
            ),
            "pvc_uid": pvc_uid if exact else None,
        }

    async def ensure_agent_workspace_claim(
        self,
        pvc_name: str,
        *,
        expected_thread_id: str,
        expected_runtime_generation: str,
        expected_claim_id: str,
        expected_create_attempt: str,
        namespace: str,
        expected_pvc_uid: str | None = None,
    ) -> str | None:
        """Create/re-attest the exact retained PVC during a soft retirement."""

        return await self._ensure_pinned_agent_pvc(
            pvc_name,
            thread_id=expected_thread_id,
            runtime_generation=expected_runtime_generation,
            claim_id=expected_claim_id,
            create_attempt=expected_create_attempt,
            expected_pvc_uid=expected_pvc_uid,
            namespace=namespace,
        )

    async def fence_agent_workspace_claim(
        self,
        pvc_name: str,
        *,
        expected_thread_id: str,
        expected_runtime_generation: str,
        expected_claim_id: str,
        expected_create_attempt: str,
        namespace: str,
    ) -> dict[str, str | None]:
        """Acquire a PVC name with an inert, non-provisioning tombstone."""

        name = str(pvc_name or "").strip()
        if not self._k8s_available or not name or not namespace:
            return {"state": "unknown", "pvc_uid": None}
        labels = {
            "app": "srw-agent",
            "srw/component": "agent-workspace-pvc",
            "srw.io/component": "agent-workspace",
            "srw/thread-id": str(expected_thread_id),
            "srw.io/thread-id": str(expected_thread_id),
            "srw.io/runtime-generation": str(expected_runtime_generation),
            "srw.io/workspace-claim": str(expected_claim_id),
            "srw.io/provision-attempt": str(expected_create_attempt),
            "srw.io/claim-provisioner": "agent",
            "srw.io/workspace-claim-fence": "true",
        }
        manifest = {
            "apiVersion": "v1",
            "kind": "PersistentVolumeClaim",
            "metadata": {
                "name": name,
                "namespace": namespace,
                "labels": labels,
                "finalizers": [PINNED_AUTHORITY_FINALIZER],
            },
            "spec": {
                "accessModes": ["ReadWriteOnce"],
                # Empty storageClass prevents dynamic provisioning. The object
                # occupies the API key but can never carry thread data.
                "storageClassName": "",
                "resources": {"requests": {"storage": "1Mi"}},
            },
        }
        incumbent = None
        try:
            incumbent = await run_bounded_k8s_mutation(
                self._core_api.create_namespaced_persistent_volume_claim,
                namespace=namespace,
                body=manifest,
            )
        except Exception:
            try:
                incumbent = await asyncio.to_thread(
                    self._core_api.read_namespaced_persistent_volume_claim,
                    name=name,
                    namespace=namespace,
                )
            except Exception:
                return {"state": "unknown", "pvc_uid": None}
        metadata = getattr(incumbent, "metadata", None)
        actual_labels = dict(getattr(metadata, "labels", None) or {})
        pvc_uid = str(getattr(metadata, "uid", "") or "")
        exact = bool(
            pvc_uid
            and actual_labels.get("srw.io/thread-id") == str(expected_thread_id)
            and actual_labels.get("srw.io/runtime-generation")
            == str(expected_runtime_generation)
            and actual_labels.get("srw.io/workspace-claim") == str(expected_claim_id)
            and actual_labels.get("srw.io/provision-attempt")
            == str(expected_create_attempt)
            and actual_labels.get("srw.io/claim-provisioner") == "agent"
        )
        is_fence = actual_labels.get("srw.io/workspace-claim-fence") == "true"
        return {
            "state": (
                "exact_fence"
                if exact and is_fence
                else "exact_original"
                if exact
                else "replacement"
            ),
            "pvc_uid": pvc_uid if exact else None,
        }

    async def delete_agent_workspace_claim_exact(
        self,
        pvc_name: str,
        *,
        expected_pvc_uid: str,
        namespace: str,
    ) -> bool:
        """Delete only one immutable PVC UID; never a same-name successor."""

        name = str(pvc_name or "").strip()
        uid = str(expected_pvc_uid or "").strip()
        if not self._k8s_available or not name or not uid or not namespace:
            return False
        try:
            await run_bounded_k8s_mutation(
                self._core_api.delete_namespaced_persistent_volume_claim,
                name=name,
                namespace=namespace,
                body={"preconditions": {"uid": uid}},
            )
            return True
        except Exception as exc:
            return getattr(exc, "status", None) == 404

    async def release_agent_workspace_claim_finalizer_exact(
        self,
        pvc_name: str,
        *,
        expected_pvc_uid: str,
        namespace: str,
    ) -> bool:
        """Release only SRW's finalizer from one exact PVC UID."""

        try:
            claim = await run_bounded_k8s_call(
                self._core_api.read_namespaced_persistent_volume_claim,
                name=pvc_name,
                namespace=namespace,
            )
        except Exception as exc:
            return getattr(exc, "status", None) == 404
        metadata = getattr(claim, "metadata", None)
        uid = str(getattr(metadata, "uid", "") or "")
        resource_version = str(getattr(metadata, "resource_version", "") or "")
        finalizers = [
            str(value) for value in getattr(metadata, "finalizers", None) or []
        ]
        if uid != str(expected_pvc_uid) or not resource_version:
            return False
        patch = finalizer_release_patch(
            uid=uid, resource_version=resource_version, finalizers=finalizers
        )
        if patch is None:
            return True
        try:
            await run_bounded_k8s_mutation(
                self._core_api.patch_namespaced_persistent_volume_claim,
                name=pvc_name,
                namespace=namespace,
                body=patch,
            )
            return True
        except Exception as exc:
            return getattr(exc, "status", None) == 404

    async def delete_agent_pod_exact(
        self,
        pod_name: str,
        *,
        expected_pod_uid: str,
        namespace: str | None = None,
    ) -> bool:
        """Request deletion of only the exact credential-bearing claimant."""

        name = str(pod_name or "").strip()
        expected_uid = str(expected_pod_uid or "").strip()
        if not self._k8s_available or not name or not expected_uid:
            return False
        captured_namespace = namespace or self._namespace
        try:
            await run_bounded_k8s_mutation(
                self._core_api.delete_namespaced_pod,
                name=name,
                namespace=captured_namespace,
                grace_period_seconds=STATELESS_CLAIMANT_EVICTION_GRACE_SECONDS,
                body={"preconditions": {"uid": expected_uid}},
            )
            logger.info(
                "Exact claimant pod deletion requested: %s uid=%s", name, expected_uid
            )
            return True
        except Exception as exc:
            if getattr(exc, "status", None) == 404:
                return True
            # HTTP 409 means the precondition protected a same-name successor.
            if getattr(exc, "status", None) == 409:
                logger.info("Exact claimant pod already replaced: %s", name)
                return True
            logger.warning("Exact claimant pod delete failed for %s: %s", name, exc)
            return False

    async def release_agent_pod_finalizer_exact(
        self,
        pod_name: str,
        *,
        expected_pod_uid: str,
        namespace: str,
        terminal_required: bool = True,
    ) -> bool:
        """Release only SRW's finalizer from one exact Pod UID."""

        try:
            pod = await run_bounded_k8s_call(
                self._core_api.read_namespaced_pod,
                name=pod_name,
                namespace=namespace,
            )
        except Exception as exc:
            return getattr(exc, "status", None) == 404
        metadata = getattr(pod, "metadata", None)
        uid = str(getattr(metadata, "uid", "") or "")
        resource_version = str(getattr(metadata, "resource_version", "") or "")
        finalizers = [
            str(value) for value in getattr(metadata, "finalizers", None) or []
        ]
        if uid != str(expected_pod_uid) or not resource_version:
            return False
        if terminal_required and not pod_containers_are_terminal(pod):
            return False
        patch = finalizer_release_patch(
            uid=uid, resource_version=resource_version, finalizers=finalizers
        )
        if patch is None:
            return True
        try:
            await run_bounded_k8s_mutation(
                self._core_api.patch_namespaced_pod,
                name=pod_name,
                namespace=namespace,
                body=patch,
            )
            return True
        except Exception as exc:
            return getattr(exc, "status", None) == 404

    @staticmethod
    def _stateless_executor_release() -> str:
        """The Helm release whose executors this orchestrator may release.

        Server-owned chart env (``AGENT_LABEL_INSTANCE`` = ``.Release.Name``),
        never the Pod. Release names are unique per namespace, so a second
        release sharing it can never have its executors released against this
        orchestrator's database. The chart *name* is deliberately not part of
        the scope: a chart rename would otherwise strand every executor the
        previous chart created behind a selector nobody matches.
        """

        from orchestrator.services.stateless_claimant_attestation import (
            pool_identity,
        )

        return pool_identity()[1]

    @classmethod
    def _stateless_executor_identity(cls, pod: Any) -> tuple[str, str, str] | None:
        """``(name, uid, resourceVersion)`` of this release's executor Pod."""

        from orchestrator.services.stateless_claimant_attestation import (
            STATELESS_CLASS_VALUE,
            STATELESS_COMPONENT_VALUE,
        )

        release = cls._stateless_executor_release()
        metadata = getattr(pod, "metadata", None)
        labels = dict(getattr(metadata, "labels", None) or {})
        name = str(getattr(metadata, "name", "") or "")
        uid = str(getattr(metadata, "uid", "") or "")
        resource_version = str(getattr(metadata, "resource_version", "") or "")
        if not (release and name and uid and resource_version):
            return None
        if (
            labels.get("srw/class") != STATELESS_CLASS_VALUE
            or labels.get("app.kubernetes.io/component") != STATELESS_COMPONENT_VALUE
            or labels.get("app.kubernetes.io/instance") != release
        ):
            return None
        return name, uid, resource_version

    def _warn_retention_blind_once(self, reason: str) -> None:
        warned = self.__dict__.setdefault("_retention_blind_warned", set())
        if reason in warned:
            return
        warned.add(reason)
        logger.warning(
            "Stateless executor retention degraded (%s): Pods retained by %s "
            "may need a manual release",
            reason,
            STATELESS_EXECUTOR_PROCESS_ZERO_FINALIZER,
        )

    async def list_retained_stateless_executor_pods(self) -> list[Any] | None:
        """Deleting pool executor Pods still held by the process-zero finalizer.

        ``None`` means unknown (no client, no pool identity, API failure) and
        callers release nothing; an empty list means nothing is retained.
        """

        release = self._stateless_executor_release()
        if not self._k8s_available:
            self._warn_retention_blind_once("no Kubernetes client")
            return None
        if not release:
            self._warn_retention_blind_once("AGENT_LABEL_INSTANCE is unset")
            return None
        selector = ",".join(
            (
                STATELESS_EXECUTOR_LABEL_SELECTOR,
                f"app.kubernetes.io/instance={release}",
            )
        )
        try:
            # Runs on the reaper's serial loop: a slow API must not delay steals.
            listed = await run_bounded_k8s_call(
                self._core_api.list_namespaced_pod,
                namespace=self._namespace,
                label_selector=selector,
                request_timeout=STATELESS_EXECUTOR_READ_TIMEOUT,
            )
        except Exception as exc:
            logger.warning("Stateless executor retention list failed: %s", exc)
            return None
        retained = []
        for pod in getattr(listed, "items", None) or []:
            metadata = getattr(pod, "metadata", None)
            finalizers = getattr(metadata, "finalizers", None) or []
            if (
                getattr(metadata, "deletion_timestamp", None) is not None
                and STATELESS_EXECUTOR_PROCESS_ZERO_FINALIZER in finalizers
                and self._stateless_executor_identity(pod) is not None
            ):
                retained.append(pod)
        return retained

    async def read_stateless_executor_pod(
        self, pod_name: str, *, expected_pod_uid: str
    ) -> Any | None:
        """Read one exact pool executor Pod; ``None`` for absent/other/unknown."""

        name = str(pod_name or "").strip()
        uid = str(expected_pod_uid or "").strip()
        if not self._k8s_available or not name or not uid:
            return None
        try:
            pod = await run_bounded_k8s_call(
                self._core_api.read_namespaced_pod,
                name=name,
                namespace=self._namespace,
                request_timeout=STATELESS_EXECUTOR_READ_TIMEOUT,
            )
        except Exception:
            return None
        identity = self._stateless_executor_identity(pod)
        if identity is None or identity[1] != uid:
            return None
        return pod

    async def stateless_executor_node_lost(self, node_name: str) -> bool:
        """Whether a human or the cloud has declared a Pod's node gone.

        ``True`` only for an exact Node 404 (PodGC's orphan sweep acts on
        the same fact) or the ``node.kubernetes.io/out-of-service`` taint (the
        sanctioned non-graceful-shutdown assertion). A kubelet on such a node
        will never report terminal container statuses, so without this the
        executor would stay Terminating forever. Any read failure is
        ``False``: an unanswered question is never evidence.
        """

        name = str(node_name or "").strip()
        if not self._k8s_available or not name:
            return False
        try:
            node = await run_bounded_k8s_call(
                self._core_api.read_node,
                name=name,
                request_timeout=STATELESS_EXECUTOR_READ_TIMEOUT,
            )
        except Exception as exc:
            status = getattr(exc, "status", None)
            if status == 404:
                return True
            if status == 403:
                self._warn_retention_blind_once(
                    "Node reads are forbidden, so executors on lost nodes are "
                    "never released; grant the chart's orchestrator "
                    "node-observer ClusterRole"
                )
            return False
        taints = getattr(getattr(node, "spec", None), "taints", None) or []
        return any(
            getattr(taint, "key", None) == NODE_OUT_OF_SERVICE_TAINT for taint in taints
        )

    async def release_stateless_executor_finalizer_exact(
        self, pod: Any, *, require_process_zero: bool = True
    ) -> bool:
        """Remove only the process-zero finalizer from one observed executor.

        The JSON Patch tests the UID, the observed resourceVersion and the
        exact finalizer list, so the release applies to precisely the object
        state the caller evaluated; any intervening write is a retryable 422.
        A live (non-deleting) executor is never unprotected.
        ``require_process_zero=False`` is reserved for an audited operator
        attestation that the claimant process is gone.
        """

        if not self._k8s_available:
            return False
        identity = self._stateless_executor_identity(pod)
        metadata = getattr(pod, "metadata", None)
        if identity is None or getattr(metadata, "deletion_timestamp", None) is None:
            return False
        if require_process_zero and not stateless_executor_process_zero(pod):
            return False
        name, uid, resource_version = identity
        patch = finalizer_release_patch(
            uid=uid,
            resource_version=resource_version,
            finalizers=[
                str(value) for value in getattr(metadata, "finalizers", None) or []
            ],
            finalizer=STATELESS_EXECUTOR_PROCESS_ZERO_FINALIZER,
        )
        if patch is None:
            return False
        try:
            await run_bounded_k8s_mutation(
                self._core_api.patch_namespaced_pod,
                name=name,
                namespace=self._namespace,
                body=patch,
            )
        except Exception as exc:
            if getattr(exc, "status", None) == 404:
                return True
            logger.info(
                "Stateless executor finalizer release deferred for %s uid=%s: %s",
                name,
                uid,
                exc,
            )
            return False
        logger.info(
            "Stateless executor process-zero finalizer released: pod=%s uid=%s",
            name,
            uid,
        )
        return True

    async def retire_historical_claimant_pod_exact(self, **identity: Any) -> bool:
        """Release an exact terminal Pod under historical settlement authority."""

        if not self._k8s_available:
            return False
        return await retire_terminal_claimant_pod(self._core_api, **identity)

    async def _archive_pod_logs(self, pod_name: str) -> None:
        """Refuse name-addressed log reads during an exact-UID teardown.

        Kubernetes log subresources have no UID precondition.  A GET followed
        by a log request can therefore cross a same-name Pod replacement and
        expose the successor's output.  Exact teardown intentionally gives up
        this best-effort archive rather than reading through a mutable name.
        """

        del pod_name

    async def _stamp_log_archive_keys(self, pod, keys: list) -> None:
        """Append archive keys to the jobs/threads this pod served."""
        if not self._db:
            return
        import json

        keys_json = json.dumps(keys)
        pod_name = pod.metadata.name
        thread_id = (pod.metadata.labels or {}).get("srw.io/thread-id")
        async with self._db.acquire() as conn:
            if thread_id:
                await conn.execute(
                    """
                    UPDATE threads
                    SET metadata = jsonb_set(
                            COALESCE(metadata, '{}'),
                            '{log_archive_keys}',
                            COALESCE(metadata->'log_archive_keys', '[]'::jsonb)
                                || $2::jsonb)
                    WHERE id = $1
                    """,
                    thread_id,
                    keys_json,
                )
            # Worker pods carry no job label (one pod serves many jobs
            # sequentially) — resolve through the agent registration while it
            # still exists. Stamping now rather than resolving at read time:
            # jobs.assigned_agent_id is ON DELETE SET NULL, so the job→pod
            # link dies with the agent row.
            await conn.execute(
                """
                UPDATE jobs
                SET context = jsonb_set(
                        COALESCE(context, '{}'),
                        '{log_archive_keys}',
                        COALESCE(context->'log_archive_keys', '[]'::jsonb)
                            || $2::jsonb)
                WHERE assigned_agent_id IN (
                    SELECT id FROM agents WHERE hostname = $1
                )
                """,
                pod_name,
                keys_json,
            )

    async def delete_agent_pod_by_thread(
        self,
        thread_id: str,
        *,
        expected_pod_name: str | None = None,
        expected_pod_uid: str | None = None,
    ) -> bool:
        """Compatibility wrapper requiring one captured immutable Pod identity."""

        del thread_id
        if not expected_pod_name or not expected_pod_uid:
            logger.warning("Refusing label-only agent Pod deletion")
            return False
        return await self.delete_agent_pod(
            expected_pod_name, expected_pod_uid=expected_pod_uid
        )

    async def get_pod_status(self, thread_id: str) -> Optional[dict]:
        """Query pod status by thread_id label (for session pods).

        Returns:
            Status dict with pod_name, phase, pod_ip, ready; or None.
        """
        if not self._k8s_available:
            return None

        try:
            pods = await asyncio.to_thread(
                self._core_api.list_namespaced_pod,
                namespace=self._namespace,
                label_selector=f"srw/thread-id={thread_id[:12]}",
            )
            if not pods.items:
                return None

            pod = pods.items[0]
            ready = False
            if pod.status.container_statuses:
                ready = all(cs.ready for cs in pod.status.container_statuses)

            return {
                "thread_id": thread_id,
                "pod_name": pod.metadata.name,
                "phase": pod.status.phase,
                "pod_ip": pod.status.pod_ip,
                "ready": ready,
            }
        except Exception as e:
            logger.error("Failed to query agent pod for thread %s: %s", thread_id, e)
            return None

    # =========================================================================
    # Pool management
    # =========================================================================

    async def active_count(self) -> int:
        """Count managed agent pods that are NOT in Succeeded/Failed phase."""
        counts = await self.active_counts_by_purpose()
        return counts["total"]

    async def active_counts_by_purpose(self) -> dict[str, int]:
        """Count managed agent pods by purpose (job/session).

        Returns:
            Dict with keys ``job``, ``session``, ``total``.
        """
        result = {"job": 0, "session": 0, "total": 0}
        if not self._k8s_available:
            return result

        try:
            pods = await asyncio.to_thread(
                self._core_api.list_namespaced_pod,
                namespace=self._namespace,
                label_selector=self._label_selector,
            )
            for pod in pods.items:
                if pod.status.phase in ("Succeeded", "Failed"):
                    continue
                # With restartPolicy: Never and a tailscale sidecar, a crashed
                # agent container leaves the pod in phase=Running indefinitely
                # (the sidecar is still up). Skip these so they don't pin the
                # MAX_AGENTS ceiling — the reaper will delete them.
                if self._has_dead_agent_container(pod):
                    continue
                # A live agent with a kubelet-killed tailscale sidecar is a
                # tunnel_dark zombie awaiting reap — don't pin the ceiling.
                if self._has_dead_tunnel_sidecar(pod):
                    continue
                purpose = (pod.metadata.labels or {}).get("srw/purpose", "job")
                if purpose in result:
                    result[purpose] += 1
                result["total"] += 1
            return result
        except Exception as e:
            logger.error("Failed to count active agent pods: %s", e)
            return result

    @staticmethod
    def _has_dead_agent_container(pod) -> bool:
        """True if the primary "agent" container has terminated.

        Catches both crashes (non-zero exit) and clean exits that didn't
        propagate to pod phase because a sidecar is still running.
        """
        for cs in getattr(pod.status, "container_statuses", None) or []:
            if cs.name != "agent":
                continue
            state = getattr(cs, "state", None)
            if state and getattr(state, "terminated", None) is not None:
                return True
        return False

    @staticmethod
    def _has_dead_tunnel_sidecar(pod) -> bool:
        """True if the "tailscale" sidecar container has terminated.

        The kubelet kills the sidecar via its liveness probe after a sustained
        dark window; with restartPolicy=Never it stays terminated. The agent
        container may still be Running, so this is checked independently of
        _has_dead_agent_container.
        """
        for cs in getattr(pod.status, "container_statuses", None) or []:
            if cs.name != "tailscale":
                continue
            state = getattr(cs, "state", None)
            if state and getattr(state, "terminated", None) is not None:
                return True
        return False

    async def _count_idle_agents(self) -> int:
        """Count agents registered as ready (idle, waiting for work) in the DB."""
        if not self._db:
            return 0
        try:
            async with self._db.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT COUNT(*) AS cnt FROM agents "
                    "WHERE status = 'ready' "
                    "AND COALESCE(agent_mode, 'worker') IN ('worker', 'dual')"
                )
                return row["cnt"] if row else 0
        except Exception as e:
            logger.error("Failed to count idle agents: %s", e)
            return 0

    async def ensure_warm_pool(self) -> int:
        """Maintain agent pool floor and buffer.

        Two complementary mechanisms:
        - **MIN_AGENTS**: absolute floor — never fewer than this many pods.
        - **AGENT_BUFFER**: headroom — try to keep this many idle agents
          ready for instant dispatch, scaling up to MAX_AGENTS.

        Returns:
            Number of pods created.
        """
        if not self._k8s_available:
            return 0
        if self._min_agents <= 0 and self._agent_buffer <= 0:
            return 0

        current = await self.active_count()
        idle = await self._count_idle_agents()

        # Target: whichever is higher — the absolute floor or the buffer need
        needed_for_floor = max(0, self._min_agents - current)
        needed_for_buffer = max(0, self._agent_buffer - idle)
        needed = max(needed_for_floor, needed_for_buffer)

        # Don't exceed MAX_AGENTS
        needed = min(needed, self._max_agents - current)

        if needed <= 0:
            return 0

        created = 0
        for _ in range(needed):
            pod_name = await self.provision_agent(purpose="job")
            if pod_name:
                created += 1
            else:
                break  # At capacity or error

        if created > 0:
            logger.info(
                "Warm pool: created %d agent pod(s) "
                "(active=%d, idle=%d, min=%d, buffer=%d)",
                created,
                current + created,
                idle,
                self._min_agents,
                self._agent_buffer,
            )
        return created

    async def reap_pods(
        self,
        offline_threshold_minutes: int = 10,
        unstartable_grace_seconds: int = 300,
        crashed_grace_seconds: int = 60,
        tunnel_dark_grace_seconds: int = 60,
    ) -> dict[str, int]:
        """Single-pass GC over managed agent pods.

        Lists the pod set once and dispatches each pod to one of four
        policies with different SLOs:

          - ``completed``: phase in {Succeeded, Failed} → delete immediately.
          - ``crashed``: phase == Running but the ``agent`` container has
            terminated (any exit code) for at least ``crashed_grace_seconds``.
            Catches sidecar-pinned pods that never propagate to phase=Failed,
            including agents that crash before their first heartbeat.
          - ``tunnel_dark``: phase == Running but the ``tailscale`` sidecar
            terminated (kubelet killed it after a sustained dark window) for at
            least ``tunnel_dark_grace_seconds``. The agent may still be up, but
            with no tunnel it cannot reach its workspace — recycle it.
          - ``stale``: phase == Running but the agent's heartbeat has been
            offline in the DB for ``offline_threshold_minutes``.
          - ``unstartable``: phase == Pending with a terminal
            ``state.waiting.reason`` (e.g. CreateContainerConfigError,
            ImagePullBackOff) older than ``unstartable_grace_seconds``.

        Returns a per-category count dict.
        """
        stats = {
            "completed": 0,
            "crashed": 0,
            "tunnel_dark": 0,
            "stale": 0,
            "drained": 0,
            "unstartable": 0,
        }
        if not self._k8s_available:
            return stats

        try:
            pods = await asyncio.to_thread(
                self._core_api.list_namespaced_pod,
                namespace=self._namespace,
                label_selector=self._label_selector,
            )
        except Exception:
            logger.exception("Failed to list agent pods for reaping")
            return stats

        offline_hostnames = await self._fetch_offline_hostnames(
            offline_threshold_minutes
        )
        draining_hostnames = await self._fetch_draining_hostnames()

        for pod in pods.items:
            if self._is_completed(pod):
                category = "completed"
            elif self._is_crashed(pod, crashed_grace_seconds):
                category = "crashed"
            elif self._is_tunnel_dark(pod, tunnel_dark_grace_seconds):
                category = "tunnel_dark"
            elif self._is_stale_running(pod, offline_hostnames):
                category = "stale"
            elif self._is_drained_running(pod, draining_hostnames):
                category = "drained"
            elif self._is_unstartable(pod, unstartable_grace_seconds):
                category = "unstartable"
            else:
                continue
            # Capture agent stderr before delete so unexpected exits don't
            # vanish with the pod. Diagnostic for the
            # persistent_session_permission_check_race incident — see
            # knowledge-base/knowledge/issues/persistent_session_permission_check_race.md.
            await self._capture_agent_logs_before_reap(pod, category)
            pod_uid = str(getattr(pod.metadata, "uid", "") or "")
            if await self.delete_agent_pod(pod.metadata.name, expected_pod_uid=pod_uid):
                stats[category] += 1

        if sum(stats.values()) > 0:
            logger.info("Reaped agent pod(s): %s", stats)
        return stats

    async def _capture_agent_logs_before_reap(self, pod, category: str) -> None:
        """Log the agent container's tail before reap deletes the pod.

        Diagnostic for the persistent_session_permission_check_race incident:
        when an agent exits unexpectedly (or is reaped while a thread is bound),
        the pod is deleted by ``delete_agent_pod`` and its stderr is gone. We
        emit the last 500 lines to the orchestrator's stderr at WARNING so they
        survive in the cluster's log aggregation.

        Always exception-safe: a capture failure must never block the reap.
        """
        pod_name = pod.metadata.name
        # tunnel_dark reaps the pod for a dead *sidecar*; capture that
        # container's logs, else the agent's.
        target = "tailscale" if category == "tunnel_dark" else "agent"
        exit_code: Any = None
        reason: Any = None
        signal: Any = None
        for cs in getattr(pod.status, "container_statuses", None) or []:
            if cs.name != target:
                continue
            terminated = getattr(getattr(cs, "state", None), "terminated", None)
            if terminated is None:
                # Container may have restarted (e.g. OOMKilled then restarted):
                # the kill reason lives in last_state.terminated, not state.
                terminated = getattr(
                    getattr(cs, "last_state", None), "terminated", None
                )
            if terminated is not None:
                exit_code = getattr(terminated, "exit_code", None)
                # reason distinguishes OOMKilled (kernel, memory) from Error
                # (e.g. liveness-probe SIGKILL on a frozen loop) — the one
                # signal that tells us which failure mode a 137 exit actually
                # was. Without it, exit_code=137 alone is ambiguous.
                reason = getattr(terminated, "reason", None)
                signal = getattr(terminated, "signal", None)
            break

        try:
            log_tail = await asyncio.to_thread(
                self._core_api.read_namespaced_pod_log,
                name=pod_name,
                namespace=self._namespace,
                container=target,
                tail_lines=500,
                timestamps=True,
            )
        except Exception as e:
            logger.warning(
                "Reap log capture: failed to fetch logs for pod=%s "
                "(category=%s, exit_code=%s, reason=%s, signal=%s): %s",
                pod_name,
                category,
                exit_code,
                reason,
                signal,
                e,
            )
            return

        logger.warning(
            "Reap log capture: pod=%s category=%s phase=%s exit_code=%s "
            "reason=%s signal=%s "
            "logs_below_marker_BEGIN\n%s\nlogs_below_marker_END pod=%s",
            pod_name,
            category,
            pod.status.phase,
            exit_code,
            reason,
            signal,
            log_tail or "(empty)",
            pod_name,
        )

    @staticmethod
    def _is_completed(pod) -> bool:
        return pod.status.phase in ("Succeeded", "Failed")

    @staticmethod
    def _is_crashed(pod, grace_seconds: int) -> bool:
        """Pod is Running but the agent container terminated past the grace.

        Brief grace lets in-flight DB writes (final heartbeat, audit) land
        before we delete the pod, which preserves debuggability without
        meaningfully delaying capacity reclaim.
        """
        if pod.status.phase != "Running":
            return False
        for cs in getattr(pod.status, "container_statuses", None) or []:
            if cs.name != "agent":
                continue
            state = getattr(cs, "state", None)
            terminated = getattr(state, "terminated", None) if state else None
            if terminated is None:
                return False
            finished_at = getattr(terminated, "finished_at", None)
            if finished_at is None:
                # Terminated but no timestamp yet — be conservative, wait.
                return False
            age = (datetime.now(timezone.utc) - finished_at).total_seconds()
            return age >= grace_seconds
        return False

    @staticmethod
    def _is_tunnel_dark(pod, grace_seconds: int) -> bool:
        """Pod is Running but the tailscale sidecar terminated past the grace.

        Brief grace mirrors _is_crashed (let final writes land). The long
        hysteresis already happened in the kubelet liveness probe.
        """
        if pod.status.phase != "Running":
            return False
        for cs in getattr(pod.status, "container_statuses", None) or []:
            if cs.name != "tailscale":
                continue
            state = getattr(cs, "state", None)
            terminated = getattr(state, "terminated", None) if state else None
            if terminated is None:
                return False
            finished_at = getattr(terminated, "finished_at", None)
            if finished_at is None:
                return False
            age = (datetime.now(timezone.utc) - finished_at).total_seconds()
            return age >= grace_seconds
        return False

    @staticmethod
    def _is_stale_running(pod, offline_hostnames: set[str]) -> bool:
        return pod.status.phase == "Running" and pod.metadata.name in offline_hostnames

    @staticmethod
    def _is_drained_running(pod, draining_hostnames: set[str]) -> bool:
        """Pod is Running and the orchestrator marked the agent as draining.

        Closes the actuation gap in `_drain_stale_image_agents`: that path
        flips the DB row to ``draining`` but never deletes the pod, so the
        agent kept heartbeating until something else terminated it. Now
        ``draining`` triggers a force delete on the next reconciler tick.
        """
        return pod.status.phase == "Running" and pod.metadata.name in draining_hostnames

    @staticmethod
    def _is_unstartable(pod, grace_seconds: int) -> bool:
        if pod.status.phase != "Pending":
            return False
        created = pod.metadata.creation_timestamp
        if created is None:
            return False
        age = (datetime.now(timezone.utc) - created).total_seconds()
        if age < grace_seconds:
            return False
        for attr in ("container_statuses", "init_container_statuses"):
            for cs in getattr(pod.status, attr, None) or []:
                state = getattr(cs, "state", None)
                waiting = getattr(state, "waiting", None) if state else None
                reason = getattr(waiting, "reason", None) if waiting else None
                if reason in _TERMINAL_WAITING_REASONS:
                    return True
        return False

    async def _fetch_offline_hostnames(self, threshold_minutes: int) -> set[str]:
        """Return hostnames of agents whose heartbeat has been stale past threshold."""
        if not self._db:
            return set()
        try:
            async with self._db.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT hostname FROM agents
                    WHERE status = 'offline'
                      AND hostname IS NOT NULL
                      AND last_heartbeat < NOW() - make_interval(mins => $1)
                    """,
                    threshold_minutes,
                )
            return {r["hostname"] for r in rows}
        except Exception:
            logger.exception("Failed to query offline agents for reaping")
            return set()

    async def _fetch_draining_hostnames(self) -> set[str]:
        """Return hostnames of agents the orchestrator has marked draining."""
        if not self._db:
            return set()
        try:
            async with self._db.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT hostname FROM agents
                    WHERE status = 'draining'
                      AND hostname IS NOT NULL
                    """
                )
            return {r["hostname"] for r in rows}
        except Exception:
            logger.exception("Failed to query draining agents for reaping")
            return set()

    async def scale_down_idle(self, max_terminate: int = 2) -> int:
        """Terminate excess idle agent pods above the MIN_AGENTS floor
        while leaving AGENT_BUFFER idle pods alone.

        Runs each reconciler cycle but only removes up to *max_terminate* pods
        per invocation for gradual scale-down (avoids thundering herd).

        Must mirror ensure_warm_pool()'s targets: that loop keeps
        AGENT_BUFFER idle pods around; terminating idle pods on the
        active-vs-min count alone made the two loops fight — warm pool
        created one pod every cycle, scale-down deleted it the next
        (one pod/minute churn for hours on dev,
        knowledge-base/knowledge/issues/session_silent_failure_audit.md #12).

        Returns:
            Number of pods terminated.
        """
        if not self._k8s_available or not self._db:
            return 0
        if self._min_agents <= 0:
            return 0  # No floor configured — nothing to scale down to

        active = await self.active_count()
        if active <= self._min_agents:
            return 0

        # Find truly idle agents (no job, no thread, not session-bound)
        try:
            async with self._db.acquire() as conn:
                idle_rows = await conn.fetch(
                    """
                    SELECT id, hostname, pod_uid FROM agents
                    WHERE status = 'ready'
                      AND current_job_id IS NULL
                      AND thread_id IS NULL
                      AND hostname IS NOT NULL
                    ORDER BY last_heartbeat ASC
                    LIMIT $1
                    """,
                    max_terminate + 5,  # fetch a few extra for filtering
                )
        except Exception:
            logger.exception("Failed to query idle agents for scale-down")
            return 0

        if not idle_rows:
            return 0

        excess_active = active - self._min_agents
        # Idle pods up to the buffer are warm-pool inventory, not excess —
        # ensure_warm_pool would immediately recreate them.
        idle_count = await self._count_idle_agents()
        excess_idle = idle_count - self._agent_buffer
        to_terminate = min(excess_active, excess_idle, len(idle_rows), max_terminate)
        if to_terminate <= 0:
            return 0
        terminated = 0

        for row in idle_rows[:to_terminate]:
            hostname = row["hostname"]
            agent_id = str(row["id"])
            pod_uid = str(row.get("pod_uid") or "")
            if await self.delete_agent_pod(hostname, expected_pod_uid=pod_uid):
                # Mark agent offline so it doesn't get re-counted as idle
                try:
                    async with self._db.acquire() as conn:
                        await conn.execute(
                            "UPDATE agents SET status = 'offline' WHERE id = $1",
                            agent_id,
                        )
                except Exception:
                    pass  # Heartbeat timeout will handle it
                terminated += 1

        if terminated:
            logger.info(
                "Scale-down: terminated %d idle agent pod(s) (active=%d, min=%d)",
                terminated,
                active - terminated,
                self._min_agents,
            )
        return terminated

    async def _try_evict_for_reservation(self, purpose: str, counts: dict) -> bool:
        """Evict an idle agent of the OTHER purpose to honour reserved slots.

        Called when the pool is at capacity. If the requesting *purpose* has
        reserved slots configured, finds an idle agent pod of the opposing type
        and deletes it to free one slot.

        Returns:
            True if a pod was successfully evicted.
        """
        # Determine which reservation applies
        if purpose == "session" and self._reserved_session_slots > 0:
            other_purpose = "job"
        elif purpose == "job" and self._reserved_job_slots > 0:
            other_purpose = "session"
        else:
            return False  # No reservation configured for this purpose

        if not self._db:
            return False

        try:
            # Find idle agents (no job, no thread) whose hostname matches a
            # Running pod of the OTHER purpose.
            async with self._db.acquire() as conn:
                idle_rows = await conn.fetch(
                    """
                    SELECT id, hostname, pod_uid FROM agents
                    WHERE status = 'ready'
                      AND current_job_id IS NULL
                      AND thread_id IS NULL
                      AND hostname IS NOT NULL
                    ORDER BY last_heartbeat ASC
                    """,
                )
            if not idle_rows:
                return False

            idle_hostnames = {r["hostname"]: r for r in idle_rows}

            # List pods of the other purpose
            pods = await asyncio.to_thread(
                self._core_api.list_namespaced_pod,
                namespace=self._namespace,
                label_selector=(f"{self._label_selector},srw/purpose={other_purpose}"),
            )
            for pod in pods.items:
                if pod.status.phase in ("Succeeded", "Failed"):
                    continue
                if pod.metadata.name in idle_hostnames:
                    agent_id = str(idle_hostnames[pod.metadata.name]["id"])
                    row_uid = str(
                        idle_hostnames[pod.metadata.name].get("pod_uid") or ""
                    )
                    pod_uid = str(getattr(pod.metadata, "uid", "") or "")
                    if row_uid != pod_uid:
                        logger.info(
                            "Eviction skipped for replaced agent Pod %s",
                            pod.metadata.name,
                        )
                        continue
                    if await self.delete_agent_pod(
                        pod.metadata.name, expected_pod_uid=pod_uid
                    ):
                        # Mark evicted agent offline
                        try:
                            async with self._db.acquire() as conn:
                                await conn.execute(
                                    "UPDATE agents SET status = 'offline' "
                                    "WHERE id = $1",
                                    agent_id,
                                )
                        except Exception:
                            pass
                        logger.info(
                            "Evicted idle %s agent %s to free slot for %s",
                            other_purpose,
                            pod.metadata.name,
                            purpose,
                        )
                        return True
        except Exception:
            logger.exception("Failed to evict agent for reservation")

        return False

    # =========================================================================
    # Pod manifest
    # =========================================================================

    def _build_pod_manifest(
        self,
        pod_name: str,
        purpose: str,
        thread_id: Optional[str],
        config_name: str,
        cpu_request: str,
        memory_request: str,
        cpu_limit: str,
        memory_limit: str,
        expert_id: Optional[str] = None,
        pvc_name: Optional[str] = None,
        runtime_actor_bootstrap: Optional[str] = None,
        runtime_actor_pod_bootstrap: Optional[str] = None,
        session_runtime_generation: Optional[str] = None,
        provision_attempt: Optional[str] = None,
        namespace: Optional[str] = None,
    ) -> dict:
        """Build the Kubernetes Pod manifest for an agent.

        Uses ``envFrom`` to inject all keys from the shared ConfigMap and
        Secret, avoiding duplication of the 60+ env vars.

        ``pvc_name`` (session agents only, when ``WORKSPACE_PVC_ENABLED`` is
        set) backs ``/workspace`` with that claim; ``None`` keeps the emptyDir
        this file has always used.
        """
        # The sink re-checks what the boundary already checked: whichever
        # path reaches this builder, a name that is not a bundled-config
        # selector never becomes a pod spec.
        validate_config_name(config_name)

        # Build the agent argv based on purpose. It is shell-quoted as a
        # list, never string-formatted — see agent_pod_entrypoint.
        agent_argv: list[str] = ["python", "agent.py"]
        if purpose == "session" and thread_id:
            agent_argv += ["--mode", "persistent", "--thread-id", str(thread_id)]
        agent_argv += ["--config", config_name, "--port", "8001", "--host", "0.0.0.0"]

        # Labels
        labels = {
            "app": "srw-agent",
            "srw/component": "agent",
            "srw/managed-by": "agent-provisioner",
            "srw/purpose": purpose,
        }
        # Standard chart labels — required for the chart's database
        # NetworkPolicies to allow ingress from these dynamic pods. The
        # chart's Helm-rendered "agent" component selectors expect:
        #   app.kubernetes.io/name      = <chart name>      (e.g. srw-dev)
        #   app.kubernetes.io/instance  = <release name>    (e.g. ...-deployment)
        #   app.kubernetes.io/component = agent
        if self._chart_label_name:
            labels["app.kubernetes.io/name"] = self._chart_label_name
        if self._chart_label_instance:
            labels["app.kubernetes.io/instance"] = self._chart_label_instance
        if self._chart_label_name or self._chart_label_instance:
            labels["app.kubernetes.io/component"] = "agent"
        if thread_id:
            labels["srw/thread-id"] = thread_id[:12]
        if session_runtime_generation:
            labels["srw.io/runtime-generation"] = session_runtime_generation
            # Full-value label consumed by the per-session Service selector
            # built by the session router (knowledge-base/knowledge/features/direct_session_websockets.md).
            # The legacy `srw/thread-id` label above is kept for backwards-compat
            # with the lifecycle reconciler, which still selects on the truncated
            # form. K8s label values cap at 63 chars; a UUID fits.
            labels["srw.io/thread-id"] = thread_id
        if provision_attempt:
            labels["srw.io/provision-attempt"] = provision_attempt
        # Build SHA label — lets the lifecycle reconciler enumerate stale
        # pods by selector without joining to the agents table. Set when
        # the image tag follows the `:sha-XXXXXXX` convention; absent for
        # `:latest` or semver-style tags (local dev — drift detection
        # is a no-op there anyway).
        if ":sha-" in self._agent_image:
            labels["srw/build-sha"] = self._agent_image.rsplit(":sha-", 1)[-1]

        containers: list[dict] = [
            {
                "name": "agent",
                "image": self._agent_image,
                "imagePullPolicy": "IfNotPresent",
                # The shell is needed to keep the command construction shared
                # with older manifests, but it must not remain PID 1.  Without
                # ``exec`` kubelet's SIGTERM stops at ``sh`` and the Python
                # drain handler never runs; the pod survives until SIGKILL at
                # the end of its termination grace period.  Every argv word is
                # shell-quoted, so the shell only ever sees the one command.
                "command": agent_exec_command(agent_argv),
                "ports": [{"containerPort": 8001}],
                # Inject all env from shared ConfigMap + Secret
                "envFrom": [
                    {"configMapRef": {"name": self._configmap_name}},
                    {"secretRef": {"name": self._secret_name}},
                ],
                # Pod-specific overrides
                "env": [
                    {"name": "AGENT_CONFIG", "value": config_name},
                    {"name": "AGENT_PORT", "value": "8001"},
                    {
                        "name": "MCP_INTERNAL_KEY",
                        "valueFrom": {
                            "secretKeyRef": {
                                "name": self._secret_name,
                                "key": "MCP_INTERNAL_KEY",
                                "optional": True,
                            }
                        },
                    },
                    *(
                        [{"name": "AGENT_EXPERT_ID", "value": expert_id}]
                        if expert_id
                        else []
                    ),
                    # Downward-API injection: the K8s-assigned pod UID. The
                    # agent reports this back at /api/agents/register so the
                    # session router can stamp ownerReferences on per-session
                    # Service/Ingress resources for K8s GC.
                    # (knowledge-base/knowledge/features/direct_session_websockets.md)
                    {
                        "name": "POD_UID",
                        "valueFrom": {
                            "fieldRef": {"fieldPath": "metadata.uid"},
                        },
                    },
                    # Session handshake authentication. The orchestrator mints
                    # an HS256 JWT carrying `tid=<thread_id>`; the pod's
                    # validator (src/api/_session_auth.py) verifies the
                    # signature with SESSION_JWT_SECRET and checks the `tid`
                    # claim against SESSION_BOUND_THREAD_ID. Both must be set
                    # for the direct-WS flow; missing them closes every
                    # handshake with code 4500. Worker pods get empty values
                    # (the WS endpoints aren't reachable for them anyway).
                    {
                        "name": "SESSION_BOUND_THREAD_ID",
                        "value": thread_id or "",
                    },
                    {
                        "name": "SESSION_RUNTIME_GENERATION",
                        "value": session_runtime_generation or "",
                    },
                    {
                        "name": "SRW_RUNTIME_ACTOR_BOOTSTRAP",
                        "value": runtime_actor_bootstrap or "",
                    },
                    # Thread-less twin of the above, carried by pool-eligible
                    # pods and exchanged at /session/attach once the pod knows
                    # which thread it is serving.
                    {
                        "name": "SRW_RUNTIME_ACTOR_POD_BOOTSTRAP",
                        "value": runtime_actor_pod_bootstrap or "",
                    },
                    {
                        "name": "SESSION_JWT_SECRET",
                        "valueFrom": {
                            "secretKeyRef": {
                                "name": os.environ.get(
                                    "SESSION_JWT_SECRET_NAME",
                                    "srw-session-jwt",
                                ),
                                "key": os.environ.get(
                                    "SESSION_JWT_SECRET_KEY",
                                    "jwt-secret",
                                ),
                                "optional": True,
                            },
                        },
                    },
                ],
                "securityContext": {
                    "runAsNonRoot": True,
                    "runAsUser": 999,
                    "runAsGroup": 999,
                    "allowPrivilegeEscalation": False,
                    "readOnlyRootFilesystem": True,
                    "capabilities": {"drop": ["ALL"]},
                },
                "volumeMounts": [
                    {"name": "workspace", "mountPath": "/workspace"},
                    {
                        "name": "vm-ssh-key",
                        "mountPath": "/run/secrets/vm-ssh-key",
                        "subPath": "ssh-privatekey",
                        "readOnly": True,
                    },
                    {"name": "tmp", "mountPath": "/tmp"},
                    {"name": "run", "mountPath": "/run"},
                    {"name": "home-srw", "mountPath": "/home/srw"},
                ],
                "livenessProbe": {
                    "httpGet": {"path": "/health", "port": 8001},
                    "initialDelaySeconds": 60,
                    "periodSeconds": 30,
                    # /health is served on the agent's asyncio loop, so a GC
                    # pause or brief sync work blows the kubelet's default 1s
                    # timeout; with the default 3-failure budget that SIGKILLs a
                    # healthy-but-busy pod (observed on srw-agent-j-a7d8f8e0:
                    # "/health context deadline exceeded"). Give transient
                    # stalls room — 5s timeout x 5 failures = ~150s before kill.
                    # A genuinely dead pod is still caught by heartbeat/offline
                    # detection (3 min) and the reaper.
                    "timeoutSeconds": 5,
                    "failureThreshold": 5,
                },
                "readinessProbe": {
                    "httpGet": {"path": "/ready", "port": 8001},
                    # startupProbe already gates readiness checks until the
                    # HTTP server is alive. A second 30s delay let the agent
                    # register ready while Kubernetes still reported the
                    # container unready; dispatch then claimed the job before
                    # exact Pod attestation could succeed and waited for lease
                    # recovery. Probe /ready immediately after startup clears.
                    "initialDelaySeconds": 0,
                    "periodSeconds": 10,
                    # Same event-loop tax as the liveness probe above: the 1s
                    # default trips on a transient stall and drops the pod from
                    # the Service endpoints (a WS blip for the user). Also seen
                    # on srw-agent-j-a7d8f8e0. failureThreshold stays at the
                    # default 3 so a genuinely not-ready pod still flips fast.
                    "timeoutSeconds": 5,
                },
                "startupProbe": {
                    "httpGet": {"path": "/health", "port": 8001},
                    # Poll quickly so the Kubernetes Ready bit can catch up
                    # with the agent's own ready heartbeat before dispatch.
                    # Keep the original 100s total startup allowance.
                    "failureThreshold": 100,
                    "periodSeconds": 1,
                },
                "resources": {
                    "requests": {
                        "memory": memory_request,
                        "cpu": cpu_request,
                    },
                    "limits": {
                        "memory": memory_limit,
                        "cpu": cpu_limit,
                    },
                },
            },
        ]

        volumes: list[dict] = [
            # /workspace in the AGENT pod. For `sandbox`-tier sessions and for
            # jobs this really is scratch: the authoritative workspace is the
            # separate ws-thread-* / workspace pod the agent reaches over SSH,
            # and nothing here is the source of truth.
            #
            # We persist it for sessions anyway, because that framing stopped
            # being complete: `backend: none` / lite sessions have no workspace
            # pod at all, so agent-local state written under /workspace is the
            # only copy and it died with every pod recycle. Rather than make
            # durability depend on a tier the pod can't see at manifest-build
            # time, PVC-back /workspace for all sessions — for sandbox sessions
            # the claim is cheap insurance over scratch; for lite sessions it is
            # the fix. Job agents stay emptyDir (pvc_name is None for them).
            {"name": "workspace", "persistentVolumeClaim": {"claimName": pvc_name}}
            if pvc_name
            else {"name": "workspace", "emptyDir": {"sizeLimit": "10Gi"}},
            {
                "name": "vm-ssh-key",
                "secret": {
                    "secretName": self._ssh_secret_name,
                    "defaultMode": 0o444,
                },
            },
            {
                "name": "tmp",
                "emptyDir": {"medium": "Memory", "sizeLimit": "256Mi"},
            },
            {
                "name": "run",
                "emptyDir": {"medium": "Memory", "sizeLimit": "16Mi"},
            },
            {"name": "home-srw", "emptyDir": {"sizeLimit": "512Mi"}},
        ]

        if self._tailscale_enabled and self._headscale_url:
            tailscale_args = (
                "mkdir -p /dev/net; "
                "[ -c /dev/net/tun ] || mknod /dev/net/tun c 10 200; "
                "tailscaled --state=mem: --tun=tailscale0 "
                "--no-logs-no-support & "
                "TSPID=$!; "
                "for i in $(seq 1 30); do "
                "[ -S /var/run/tailscale/tailscaled.sock ] && break; "
                "sleep 1; done; "
                "while true; do "
                "if tailscale up "
                '--auth-key="${TS_AUTHKEY}" '
                f'--login-server="{self._headscale_url}" '
                '--hostname="${POD_NAME}" '
                "--accept-dns=false "
                "--timeout=60s 2>&1; then "
                'echo "Tailscale authenticated"; break; fi; '
                'echo "Auth retry in 15s..."; sleep 15; done; '
                # Supervision loop: re-up if the node loses its registration
                # (headscale #2006 lastSeen false-GC, control-plane restart,
                # network/DERP blip). `tailscale up` is idempotent when the
                # backend is already Running. Permanent loss is handled by the
                # liveness probe + the tunnel_dark reaper, not here. The grep
                # tolerates the space MarshalIndent puts after the JSON colon.
                'while kill -0 "$TSPID" 2>/dev/null; do '
                "if ! tailscale status --json --peers=false 2>/dev/null | "
                'grep -qE \'"BackendState":[[:space:]]*"Running"\'; then '
                "tailscale up "
                '--auth-key="${TS_AUTHKEY}" '
                f'--login-server="{self._headscale_url}" '
                '--hostname="${POD_NAME}" '
                "--accept-dns=false --timeout=60s || true; "
                "fi; sleep 30; done"
            )
            # ceil(dark_timeout / 30s); >=1. The kubelet measures the sustained
            # dark window via failureThreshold, so the reaper needs no timing.
            dark_failures = max(1, (self._tailscale_dark_timeout + 29) // 30)
            containers.append(
                {
                    "name": "tailscale",
                    "image": "ghcr.io/tailscale/tailscale:v1.82.5",
                    "securityContext": {
                        "capabilities": {"add": ["NET_ADMIN", "NET_RAW"]},
                    },
                    "command": ["/bin/sh", "-c"],
                    "args": [tailscale_args],
                    "env": [
                        {
                            "name": "TS_AUTHKEY",
                            "valueFrom": {
                                "secretKeyRef": {
                                    "name": self._secret_name,
                                    "key": "TAILSCALE_AUTH_KEY",
                                    "optional": True,
                                }
                            },
                        },
                        {
                            "name": "POD_NAME",
                            "valueFrom": {"fieldRef": {"fieldPath": "metadata.name"}},
                        },
                    ],
                    "volumeMounts": [
                        {
                            "name": "tailscale-state",
                            "mountPath": "/var/lib/tailscale",
                        }
                    ],
                    # status --peers=false skips the (huge, on a big tailnet)
                    # peer list so the check stays well under timeoutSeconds;
                    # plain `tailscale status --json` exceeds the default 1s on a
                    # large tailnet and the kubelet kills a healthy sidecar.
                    "livenessProbe": {
                        "exec": {
                            "command": [
                                "/bin/sh",
                                "-c",
                                "tailscale status --json --peers=false 2>/dev/null | "
                                'grep -qE \'"BackendState":[[:space:]]*"Running"\'',
                            ]
                        },
                        "initialDelaySeconds": 120,
                        "periodSeconds": 30,
                        "timeoutSeconds": 5,
                        "failureThreshold": dark_failures,
                    },
                    "resources": {
                        "requests": {"memory": "64Mi", "cpu": "50m"},
                        "limits": {"memory": "128Mi", "cpu": "200m"},
                    },
                }
            )
            volumes.append(
                {"name": "tailscale-state", "emptyDir": {"sizeLimit": "16Mi"}}
            )

        pod_metadata = {
            "name": pod_name,
            "namespace": namespace or self._namespace,
            "labels": labels,
        }
        if provision_attempt:
            pod_metadata["finalizers"] = [PINNED_AUTHORITY_FINALIZER]

        return {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": pod_metadata,
            "spec": {
                "restartPolicy": "Never",
                "terminationGracePeriodSeconds": 180,
                "securityContext": {
                    "seccompProfile": {"type": "RuntimeDefault"},
                },
                # Wait for orchestrator before starting agent. The host:port
                # comes from ORCHESTRATOR_URL (chart-injected, defaults to
                # `http://srw-orchestrator:8085`) so a non-default
                # fullnameOverride doesn't desync agent init from the actual
                # orchestrator Service name.
                "initContainers": [
                    {
                        "name": "wait-for-orchestrator",
                        "image": "busybox:1.36",
                        "command": [
                            "sh",
                            "-c",
                            "until nc -z "
                            f"{shlex.quote(str(self._orchestrator_host))} "
                            f"{shlex.quote(str(self._orchestrator_port))}; "
                            "do sleep 2; done",
                        ],
                    }
                ],
                "containers": containers,
                "volumes": volumes,
            },
        }

    # =========================================================================
    # Internal helpers
    # =========================================================================

    async def _create_pvc(
        self, pvc_name: str, size: str = "10Gi", labels: Optional[dict] = None
    ) -> Optional[str]:
        """Create a PVC for a session agent's ``/workspace``. Idempotent.

        Returns ``"created"`` for a new volume, ``"reused"`` if the claim
        already existed (409 — i.e. the reattach path taken by every pod
        recycle), or ``None`` on failure. Callers must treat ``None`` as fatal
        for the provision: see provision_agent().
        """
        if not self._k8s_available:
            return None

        pvc_labels = {
            "app": "srw-agent",
            "srw/component": "agent-workspace-pvc",
            # The label the lifecycle reaper selects on
            # (lifecycle/workspace_manager.py::_LABEL_SELECTOR). Without it a
            # claim is invisible to GC and leaks storage forever once its
            # thread is gone — the bug PersistentProvisioner._create_pvc has,
            # which is why this does not simply reuse that helper.
            "srw.io/component": "agent-workspace",
        }
        if labels:
            pvc_labels.update(labels)

        pvc_manifest = {
            "apiVersion": "v1",
            "kind": "PersistentVolumeClaim",
            "metadata": {
                "name": pvc_name,
                "namespace": self._namespace,
                "labels": pvc_labels,
            },
            "spec": {
                "accessModes": ["ReadWriteOnce"],
                "storageClassName": self._storage_class,
                "resources": {"requests": {"storage": size}},
            },
        }

        try:
            await asyncio.to_thread(
                self._core_api.create_namespaced_persistent_volume_claim,
                namespace=self._namespace,
                body=pvc_manifest,
            )
            logger.info(
                "Session agent PVC created: %s (storageClass=%s, size=%s)",
                pvc_name,
                self._storage_class,
                size,
            )
            return "created"
        except Exception as e:
            if hasattr(e, "status") and e.status == 409:
                logger.debug("Session agent PVC already exists: %s", pvc_name)
                return "reused"
            # A 403 is (almost) always the capacity guard rather than RBAC: the
            # orchestrator SA may create PVCs, so the Forbidden it actually
            # hits is a ResourceQuota "exceeded quota" rejection. Say so
            # distinctly, so an operator can tell "cluster full" from broken
            # infra. Either way the caller still fails closed.
            if hasattr(e, "status") and e.status == 403:
                logger.error(
                    "Session agent PVC %s rejected (403) — most likely the "
                    "namespace ResourceQuota is exhausted; raise "
                    "workspace.resourceQuota.maxStorage/maxCount or wait for "
                    "PVCs to be reclaimed: %s",
                    pvc_name,
                    getattr(e, "body", e),
                )
                return None
            logger.error("Failed to create session agent PVC %s: %s", pvc_name, e)
            return None

    async def _ensure_pinned_agent_pvc(
        self,
        pvc_name: str,
        *,
        thread_id: str,
        runtime_generation: str,
        claim_id: str,
        create_attempt: str,
        expected_pvc_uid: str | None,
        namespace: str,
    ) -> str | None:
        """Create/observe only the exact durable PVC claim and return its UID."""

        if not self._k8s_available or not all(
            (
                pvc_name,
                thread_id,
                runtime_generation,
                claim_id,
                create_attempt,
                namespace,
            )
        ):
            return None
        labels = {
            "app": "srw-agent",
            "srw/component": "agent-workspace-pvc",
            "srw.io/component": "agent-workspace",
            "srw/thread-id": str(thread_id),
            "srw.io/thread-id": str(thread_id),
            "srw.io/runtime-generation": str(runtime_generation),
            "srw.io/workspace-claim": str(claim_id),
            "srw.io/provision-attempt": str(create_attempt),
            "srw.io/claim-provisioner": "agent",
        }
        manifest = {
            "apiVersion": "v1",
            "kind": "PersistentVolumeClaim",
            "metadata": {
                "name": pvc_name,
                "namespace": namespace,
                "labels": labels,
                "finalizers": [PINNED_AUTHORITY_FINALIZER],
            },
            "spec": {
                "accessModes": ["ReadWriteOnce"],
                "storageClassName": self._storage_class,
                "resources": {"requests": {"storage": self._pvc_size}},
            },
        }

        async def _read_exact() -> str | None:
            try:
                claim = await asyncio.to_thread(
                    self._core_api.read_namespaced_persistent_volume_claim,
                    name=pvc_name,
                    namespace=namespace,
                )
            except Exception:
                return None
            metadata = getattr(claim, "metadata", None)
            actual_labels = dict(getattr(metadata, "labels", None) or {})
            actual_uid = str(getattr(metadata, "uid", "") or "")
            if not (
                actual_uid
                and actual_labels.get("srw.io/thread-id") == str(thread_id)
                and actual_labels.get("srw.io/runtime-generation")
                == str(runtime_generation)
                and actual_labels.get("srw.io/workspace-claim") == str(claim_id)
                and actual_labels.get("srw.io/provision-attempt") == str(create_attempt)
                and actual_labels.get("srw.io/claim-provisioner") == "agent"
            ):
                return None
            if expected_pvc_uid and actual_uid != str(expected_pvc_uid):
                return None
            return actual_uid

        if expected_pvc_uid:
            return await _read_exact()
        try:
            created = await run_bounded_k8s_mutation(
                self._core_api.create_namespaced_persistent_volume_claim,
                namespace=namespace,
                body=manifest,
            )
            created_uid = str(
                getattr(getattr(created, "metadata", None), "uid", "") or ""
            )
            return created_uid or await _read_exact()
        except Exception:
            # 409 and accepted-then-timeout share the same exact observation.
            # A 404 remains ambiguous and leaves the DB intent planned.
            return await _read_exact()

    async def _delete_pvc(self, pvc_name: str) -> bool:
        """Delete a newly-created session PVC after lifecycle refusal."""

        if not self._k8s_available:
            return False
        try:
            await asyncio.to_thread(
                self._core_api.delete_namespaced_persistent_volume_claim,
                name=pvc_name,
                namespace=self._namespace,
            )
            return True
        except Exception as exc:
            if getattr(exc, "status", None) == 404:
                return True
            logger.warning("Failed to delete refused session PVC %s: %s", pvc_name, exc)
            return False

    async def _set_thread_context(
        self,
        thread_id: str,
        updates: dict,
        *,
        expected_runtime_generation: str | None = None,
    ) -> bool:
        """Store agent pod status in thread metadata under ``agent_pod``."""
        if not self._db:
            return False

        try:
            import json

            async with self._db.acquire() as conn:
                result = await conn.execute(
                    """
                    UPDATE threads
                    SET metadata      = jsonb_set(
                            COALESCE(metadata, '{}'),
                            '{agent_pod}',
                            COALESCE(metadata->'agent_pod', '{}'::jsonb)
                                || $2::jsonb
                                        ),
                        last_activity = CURRENT_TIMESTAMP
                    WHERE id = $1
                      AND status IN ('created','active','awaiting_user','suspended')
                      AND runtime_retirement_token IS NULL
                      AND ($3::uuid IS NULL OR runtime_generation = $3::uuid)
                    """,
                    thread_id,
                    json.dumps(updates),
                    expected_runtime_generation,
                )
            return result == "UPDATE 1"
        except Exception:
            logger.exception(
                "Failed to update agent pod context for thread %s", thread_id
            )
            return False


# Module-level singleton
agent_provisioner = AgentProvisioner()
