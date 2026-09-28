"""Persistent Agent Provisioner — On-demand pod lifecycle for interactive sessions.

Creates ephemeral K8s Pods per persistent thread. Pods run the same agent
image as worker agents but with ``--mode persistent --thread-id <uuid>``.

Lifecycle:
    create_agent_pod()   — user creates session → orchestrator provisions pod
    delete_agent_pod()   — idle timeout / session end → pod deleted (workspace
                           snapshot handled by WorkspaceSuspensionService)
    get_pod_status()     — check if pod is running for a thread

For local development, persistent agents are started manually via:
    python -m agent --mode persistent --thread-id <uuid> --config session_base
"""

import asyncio
import logging
import os
import shlex
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import StrEnum
from typing import Any, Dict, Optional

from shared.runtime.core.loader import canonical_config_name

from orchestrator.services.agent_pod_entrypoint import (
    agent_exec_command,
    validate_config_name,
)
from orchestrator.services.runtime_actor import issue_runtime_actor_bootstrap
from orchestrator.services.pinned_k8s_effect import (
    PINNED_AUTHORITY_FINALIZER,
    PINNED_AUTHORITY_PROTECTION_PROTOCOL,
    discover_exact_pinned_pod_authority,
    fence_unmodified_planned_pod_authority,
    finalizer_release_patch,
    pod_containers_are_terminal,
    legacy_pinned_namespace_candidates,
    observe_planned_pinned_pod_authority,
    protect_planned_pinned_pod_authority,
    protect_legacy_pinned_agent_authority as protect_legacy_pinned_objects,
    release_planned_pinned_pod_authority,
    run_bounded_k8s_call,
    run_bounded_k8s_mutation,
)
from orchestrator.services.session_runtime_admission import (
    ThreadRuntimeAuthority,
    same_thread_runtime_authority,
    thread_runtime_authority,
)

logger = logging.getLogger(__name__)


class PersistentPodCreateStatus(StrEnum):
    """Truthful outcomes for the deterministic persistent-pod name."""

    CREATED = "created"
    ALREADY_CURRENT = "already_current"
    TERMINATING = "terminating"
    CONFLICTING = "conflicting"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class PersistentPodCreateResult:
    """Result of one create attempt without collapsing a 409 into success."""

    status: PersistentPodCreateStatus
    pod_name: str
    pod_uid: str | None = None
    build_sha: str | None = None
    failure_class: str | None = None

    @property
    def usable(self) -> bool:
        return self.status in {
            PersistentPodCreateStatus.CREATED,
            PersistentPodCreateStatus.ALREADY_CURRENT,
        }


def _normalize_config_name(config_name: str) -> str:
    """A UUID in ``config_name`` means the cockpit put the expert id in the
    wrong slot — it has no on-disk ``<uuid>.yaml`` and ``--config <uuid>``
    crashes startup. Sessions apply the bound expert via ``config_override``,
    so fall back to the session base. See
    knowledge-history/done/global_expert_management.md.

    This is also the provisioner boundary for the value: ``config_name`` is
    the one caller-controlled word in the pod's ``sh -c`` entrypoint, so it is
    validated here — before any Kubernetes call or pod spec — and the
    entrypoint itself is built from a quoted argv (``agent_exec_command``).
    Security audit 2026-08-27, finding #3."""
    if not config_name:
        return canonical_config_name(config_name)
    config_name = validate_config_name(config_name)
    try:
        uuid.UUID(str(config_name))
    except (ValueError, TypeError, AttributeError):
        return config_name
    logger.warning(
        "session config_name %s is a UUID (expert id in the config slot); "
        "booting session_base — expert applies via config_override.",
        config_name,
    )
    return "session_base"


class PersistentProvisioner:
    """Provisions persistent agent pods on demand via Kubernetes API.

    Follows the ContainerProvisioner pattern: direct K8s, graceful
    degradation when K8s is not available.
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
            "PERSISTENT_AGENT_IMAGE",
            "ghcr.io/superhuman-remote-worker/srw-agent:latest",
        )
        self._agent_image_pull_policy: str = os.environ.get(
            "PERSISTENT_AGENT_IMAGE_PULL_POLICY", "Always"
        ).strip()
        if self._agent_image_pull_policy not in {"Always", "IfNotPresent", "Never"}:
            raise ValueError(
                "PERSISTENT_AGENT_IMAGE_PULL_POLICY must be one of "
                "Always, IfNotPresent, or Never"
            )
        # Chart labels — without these the database NetworkPolicies (which
        # match app.kubernetes.io/{name,instance} + component=agent) REJECT
        # ingress from these pods: the officer respawn crash-looped on
        # asyncpg ECONNREFUSED to srw-postgres until they were added
        # (2026-07-30). Injected by the chart's orchestrator Deployment,
        # same mechanism as agent_provisioner.
        self._chart_label_name: str = os.environ.get("AGENT_LABEL_NAME", "").strip()
        self._chart_label_instance: str = os.environ.get(
            "AGENT_LABEL_INSTANCE", ""
        ).strip()
        self._configmap_name: str = os.environ.get("AGENT_CONFIGMAP", "srw-config")
        self._secret_name: str = os.environ.get("AGENT_SECRET", "srw")
        self._ssh_secret_name: str = os.environ.get(
            "WORKSPACE_SSH_SECRET", "vm-ssh-key"
        )
        self._storage_class: str = os.environ.get(
            "WORKSPACE_STORAGE_CLASS", "longhorn-ephemeral"
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

    @property
    def is_available(self) -> bool:
        """Whether K8s provisioning is available."""
        return self._k8s_available

    @property
    def in_cluster(self) -> bool:
        """True if connected via in-cluster config (running inside K8s)."""
        return self._in_cluster

    @property
    def mode(self) -> Optional[str]:
        """Current provisioning mode."""
        if self._k8s_available:
            return "k8s"
        return None

    @property
    def image_ref(self) -> str:
        """Exact server-configured image used for a new persistent pod."""

        return self._agent_image

    def connect(self, db: Any) -> None:
        """Initialize provisioner with database connection.

        Args:
            db: PostgresDB instance for thread/agent tracking.
        """
        self._db = db
        self._init_k8s()

        if self._k8s_available:
            logger.info(
                "PersistentProvisioner ready (namespace=%s, image=%s)",
                self._namespace,
                self._agent_image,
            )
        else:
            logger.info(
                "PersistentProvisioner: not available "
                "(persistent agents must be started manually)"
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
                    logger.info(
                        "K8s not available — persistent agents must be started manually"
                    )
                    return

            self._core_api = k8s_client.CoreV1Api()
            self._k8s_available = True
            self._in_cluster = in_cluster
        except ImportError:
            logger.info(
                "kubernetes package not installed — persistent agents must "
                "be started manually"
            )

    # =========================================================================
    # Pod lifecycle
    # =========================================================================

    async def _runtime_authority(self, thread_id: str) -> ThreadRuntimeAuthority | None:
        if self._db is None:
            return None
        try:
            return thread_runtime_authority(await self._db.get_thread(thread_id))
        except Exception:
            logger.exception(
                "Persistent provision authority read failed: %s", thread_id
            )
            return None

    async def _runtime_authority_matches(
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
                "Persistent provision authority recheck failed: %s",
                expected.thread_id,
            )
            return False

    async def create_agent_pod(
        self,
        thread_id: str,
        config_name: str = "session_base",
        expert_id: str | None = None,
        cpu_request: str = "250m",
        memory_request: str = "512Mi",
        cpu_limit: str = "1000m",
        memory_limit: str = "2Gi",
        lifecycle_generation: str | None = None,
        target_image_ref: str | None = None,
        expected_runtime_generation: str | None = None,
        namespace: str | None = None,
    ) -> PersistentPodCreateResult:
        """Create a K8s pod running a persistent agent for *thread_id*.

        Args:
            thread_id: Thread UUID to bind the agent to.
            config_name: Agent config to use (e.g. ``session_base``).
            cpu_request: CPU request.
            memory_request: Memory request.
            cpu_limit: CPU limit.
            memory_limit: Memory limit.

        Returns a typed result. In particular, an existing terminating pod is
        never reported as successful, and a live 409 is accepted only when
        its server-owned labels describe this exact thread/build/generation.
        """
        config_name = _normalize_config_name(config_name)
        image_ref = target_image_ref or self._agent_image
        target_build_sha = self._build_sha(image_ref)
        if not self._k8s_available:
            logger.info(
                "K8s not available — start agent manually: "
                "python -m agent --mode persistent --thread-id %s "
                "--config %s",
                thread_id,
                config_name,
            )
            return PersistentPodCreateResult(
                PersistentPodCreateStatus.FAILED,
                f"persistent-{thread_id[:12]}",
                failure_class="kubernetes_unavailable",
            )
        runtime_authority = await self._runtime_authority(thread_id)
        if runtime_authority is None or (
            expected_runtime_generation is not None
            and runtime_authority.generation != expected_runtime_generation
        ):
            return PersistentPodCreateResult(
                PersistentPodCreateStatus.FAILED,
                f"persistent-{thread_id[:12]}",
                failure_class="session_not_preparable",
            )

        pod_name = f"persistent-{thread_id[:12]}"
        pvc_name = f"pvc-persistent-{thread_id[:12]}"
        candidate_attempt = str(uuid.uuid4())
        intent = await self._db.reserve_pinned_agent_pod_provision_intent(
            thread_id,
            expected_runtime_generation=runtime_authority.generation,
            attempt_id=candidate_attempt,
            pod_name=pod_name,
            provisioner="persistent",
            namespace=namespace or self._namespace,
            protection_protocol=PINNED_AUTHORITY_PROTECTION_PROTOCOL,
            pvc_name=pvc_name,
        )
        provision_attempt = (
            str(intent.get("attempt_id") or "") if isinstance(intent, dict) else ""
        )
        intent_namespace = (
            str(intent.get("namespace") or "") if isinstance(intent, dict) else ""
        )

        # Avoid minting an unused bootstrap for the ordinary idempotent case.
        # Kubernetes create remains the final concurrency CAS; a race after
        # this read is classified again from the incumbent on 409.
        try:
            incumbent = await self._read_pod(pod_name, namespace=intent_namespace)
        except Exception as exc:
            return PersistentPodCreateResult(
                PersistentPodCreateStatus.FAILED,
                pod_name,
                failure_class=f"observation_{type(exc).__name__}"[:128],
            )
        if not await self._runtime_authority_matches(runtime_authority):
            return PersistentPodCreateResult(
                PersistentPodCreateStatus.FAILED,
                pod_name,
                failure_class="session_not_preparable",
            )
        if incumbent is not None:
            classified = self._classify_incumbent(
                incumbent,
                thread_id=thread_id,
                pod_name=pod_name,
                lifecycle_generation=lifecycle_generation,
                session_runtime_generation=runtime_authority.generation,
                expected_build_sha=target_build_sha,
                provision_attempt=provision_attempt or None,
            )
            if (
                provision_attempt
                and classified.status is PersistentPodCreateStatus.ALREADY_CURRENT
            ):
                if (
                    not classified.pod_uid
                    or not await self._db.publish_pinned_agent_pod_provision_intent(
                        thread_id,
                        expected_runtime_generation=runtime_authority.generation,
                        attempt_id=provision_attempt,
                        pod_name=pod_name,
                        pod_uid=classified.pod_uid,
                        namespace=intent_namespace,
                    )
                ):
                    return PersistentPodCreateResult(
                        PersistentPodCreateStatus.FAILED,
                        pod_name,
                        failure_class="provision_intent_publication_refused",
                    )
            return classified
        if not provision_attempt:
            return PersistentPodCreateResult(
                PersistentPodCreateStatus.FAILED,
                pod_name,
                failure_class="provision_intent_refused",
            )

        workspace_claim = intent.get("workspace_claim")
        if not isinstance(workspace_claim, dict):
            return PersistentPodCreateResult(
                PersistentPodCreateStatus.FAILED,
                pod_name,
                failure_class="workspace_claim_refused",
            )
        claim_id = str(workspace_claim.get("claim_id") or "")
        claim_uid = await self._ensure_pinned_agent_pvc(
            pvc_name,
            thread_id=thread_id,
            runtime_generation=str(
                workspace_claim.get("created_runtime_generation") or ""
            ),
            claim_id=claim_id,
            create_attempt=str(workspace_claim.get("create_attempt") or ""),
            expected_pvc_uid=str(workspace_claim.get("pvc_uid") or "") or None,
            namespace=str(workspace_claim.get("namespace") or ""),
        )
        if not await self._runtime_authority_matches(runtime_authority):
            # The deterministic PVC is the resumable session workspace. A
            # stale generation must never delete it by name.
            return PersistentPodCreateResult(
                PersistentPodCreateStatus.FAILED,
                pod_name,
                failure_class="session_not_preparable",
            )
        if not claim_uid or not await self._db.publish_pinned_agent_workspace_claim(
            thread_id,
            expected_runtime_generation=runtime_authority.generation,
            claim_id=claim_id,
            pvc_name=pvc_name,
            pvc_uid=claim_uid,
            namespace=str(workspace_claim.get("namespace") or ""),
        ):
            now_iso = datetime.now(timezone.utc).isoformat()
            await self._set_thread_context(
                thread_id,
                {
                    "status": "failed",
                    "error": "PVC creation failed",
                    "updated_at": now_iso,
                },
                expected_runtime_generation=runtime_authority.generation,
            )
            return PersistentPodCreateResult(
                PersistentPodCreateStatus.FAILED,
                pod_name,
                failure_class="pvc_creation_failed",
            )

        try:
            runtime_actor_bootstrap = await issue_runtime_actor_bootstrap(
                self._db, thread_id
            )
            if not await self._runtime_authority_matches(runtime_authority):
                return PersistentPodCreateResult(
                    PersistentPodCreateStatus.FAILED,
                    pod_name,
                    failure_class="session_not_preparable",
                )
        except Exception:
            logger.exception(
                "Could not issue runtime actor bootstrap for session %s; "
                "refusing to provision an identity-less pod",
                thread_id,
            )
            return PersistentPodCreateResult(
                PersistentPodCreateStatus.FAILED,
                pod_name,
                failure_class="runtime_bootstrap_failed",
            )

        manifest = self._build_agent_pod_manifest(
            pod_name=pod_name,
            thread_id=thread_id,
            config_name=config_name,
            expert_id=expert_id,
            cpu_request=cpu_request,
            memory_request=memory_request,
            cpu_limit=cpu_limit,
            memory_limit=memory_limit,
            pvc_name=pvc_name,
            runtime_actor_bootstrap=runtime_actor_bootstrap,
            lifecycle_generation=lifecycle_generation,
            session_runtime_generation=runtime_authority.generation,
            provision_attempt=provision_attempt,
            image_ref=image_ref,
            namespace=intent_namespace,
        )
        if not await self._runtime_authority_matches(runtime_authority):
            return PersistentPodCreateResult(
                PersistentPodCreateStatus.FAILED,
                pod_name,
                failure_class="session_not_preparable",
            )

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
                observed = await self._read_pod(pod_name, namespace=intent_namespace)
                observed_labels = dict(
                    getattr(getattr(observed, "metadata", None), "labels", None) or {}
                )
                if (
                    observed_labels.get("srw.io/runtime-generation")
                    == runtime_authority.generation
                    and observed_labels.get("srw/thread-id") == thread_id
                    and observed_labels.get("srw.io/provision-attempt")
                    == provision_attempt
                ):
                    created_uid = str(
                        getattr(getattr(observed, "metadata", None), "uid", "") or ""
                    )
            if not await self._runtime_authority_matches(runtime_authority):
                if created_uid:
                    await self.delete_agent_pod_exact(
                        thread_id,
                        expected_pod_uid=created_uid,
                        namespace=intent_namespace,
                    )
                return PersistentPodCreateResult(
                    PersistentPodCreateStatus.FAILED,
                    pod_name,
                    failure_class="session_ended_during_create",
                )
            if not created_uid:
                # A nonempty agent_pod marker is process authority. Never
                # publish a name-only intermediate shape: the 0185 lifecycle
                # fence requires the immutable Kubernetes UID in the first
                # publication so a same-name replacement cannot be adopted.
                return PersistentPodCreateResult(
                    PersistentPodCreateStatus.FAILED,
                    pod_name,
                    failure_class="pod_uid_unavailable",
                )
            logger.info("Agent pod created: %s (thread %s)", pod_name, thread_id)
            if not await self._db.publish_pinned_agent_pod_provision_intent(
                thread_id,
                expected_runtime_generation=runtime_authority.generation,
                attempt_id=provision_attempt,
                pod_name=pod_name,
                pod_uid=created_uid,
                namespace=intent_namespace,
            ):
                await self.delete_agent_pod_exact(
                    thread_id,
                    expected_pod_uid=created_uid,
                    namespace=intent_namespace,
                )
                return PersistentPodCreateResult(
                    PersistentPodCreateStatus.FAILED,
                    pod_name,
                    failure_class="provision_intent_publication_refused",
                )
            now_iso = datetime.now(timezone.utc).isoformat()
            published = await self._set_thread_context(
                thread_id,
                {
                    "status": "created",
                    "pod_name": pod_name,
                    "pod_uid": created_uid,
                    "provision_attempt": provision_attempt,
                    "namespace": intent_namespace,
                    "expected_build_sha": target_build_sha,
                    "runtime_generation": runtime_authority.generation,
                    "created_at": now_iso,
                    "updated_at": now_iso,
                },
                expected_runtime_generation=runtime_authority.generation,
            )
            if not published or not await self._runtime_authority_matches(
                runtime_authority
            ):
                if created_uid:
                    await self.delete_agent_pod_exact(
                        thread_id,
                        expected_pod_uid=created_uid,
                        namespace=intent_namespace,
                    )
                return PersistentPodCreateResult(
                    PersistentPodCreateStatus.FAILED,
                    pod_name,
                    failure_class="runtime_generation_changed_before_publish",
                )

            # Wait for pod to become ready
            pod_ip = await self._wait_for_ready(
                pod_name, timeout=120, namespace=intent_namespace
            )
            if not await self._runtime_authority_matches(runtime_authority):
                if created_uid:
                    await self.delete_agent_pod_exact(
                        thread_id,
                        expected_pod_uid=created_uid,
                        namespace=intent_namespace,
                    )
                return PersistentPodCreateResult(
                    PersistentPodCreateStatus.FAILED,
                    pod_name,
                    failure_class="session_ended_during_ready_wait",
                )
            if pod_ip:
                published = await self._set_thread_context(
                    thread_id,
                    {
                        "status": "ready",
                        "pod_ip": pod_ip,
                        "updated_at": datetime.now(timezone.utc).isoformat(),
                    },
                    expected_runtime_generation=runtime_authority.generation,
                )
                logger.info(
                    "Agent pod ready: %s @ %s (thread %s)",
                    pod_name,
                    pod_ip,
                    thread_id,
                )
            else:
                logger.warning(
                    "Agent pod not ready within timeout: %s (thread %s)",
                    pod_name,
                    thread_id,
                )
                published = await self._set_thread_context(
                    thread_id,
                    {
                        "status": "creating",
                        "updated_at": datetime.now(timezone.utc).isoformat(),
                    },
                    expected_runtime_generation=runtime_authority.generation,
                )

            if not published or not await self._runtime_authority_matches(
                runtime_authority
            ):
                await self.delete_agent_pod_exact(
                    thread_id,
                    expected_pod_uid=created_uid,
                    namespace=intent_namespace,
                )
                return PersistentPodCreateResult(
                    PersistentPodCreateStatus.FAILED,
                    pod_name,
                    failure_class="runtime_generation_changed_after_ready",
                )

            pod_uid = created_uid
            published = await self._set_thread_context(
                thread_id,
                {
                    "pod_uid": pod_uid,
                    "observed_build_sha": target_build_sha,
                    "lifecycle_generation": lifecycle_generation,
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                },
                expected_runtime_generation=runtime_authority.generation,
            )
            if not published or not await self._runtime_authority_matches(
                runtime_authority
            ):
                await self.delete_agent_pod_exact(
                    thread_id,
                    expected_pod_uid=created_uid,
                    namespace=intent_namespace,
                )
                return PersistentPodCreateResult(
                    PersistentPodCreateStatus.FAILED,
                    pod_name,
                    failure_class="runtime_generation_changed_at_final_publish",
                )
            return PersistentPodCreateResult(
                PersistentPodCreateStatus.CREATED,
                pod_name,
                pod_uid=pod_uid,
                build_sha=target_build_sha,
            )
        except Exception as e:
            if hasattr(e, "status") and e.status == 409:
                try:
                    incumbent = await self._read_pod(
                        pod_name, namespace=intent_namespace
                    )
                except Exception as exc:
                    return PersistentPodCreateResult(
                        PersistentPodCreateStatus.FAILED,
                        pod_name,
                        failure_class=f"observation_{type(exc).__name__}"[:128],
                    )
                if incumbent is not None:
                    classified = self._classify_incumbent(
                        incumbent,
                        thread_id=thread_id,
                        pod_name=pod_name,
                        lifecycle_generation=lifecycle_generation,
                        session_runtime_generation=runtime_authority.generation,
                        expected_build_sha=target_build_sha,
                        provision_attempt=provision_attempt,
                    )
                    if (
                        classified.status is PersistentPodCreateStatus.ALREADY_CURRENT
                        and classified.pod_uid
                        and await self._db.publish_pinned_agent_pod_provision_intent(
                            thread_id,
                            expected_runtime_generation=runtime_authority.generation,
                            attempt_id=provision_attempt,
                            pod_name=pod_name,
                            pod_uid=classified.pod_uid,
                            namespace=intent_namespace,
                        )
                    ):
                        return classified
                    return PersistentPodCreateResult(
                        PersistentPodCreateStatus.FAILED,
                        pod_name,
                        failure_class="provision_intent_publication_refused",
                    )
                logger.info(
                    "Persistent pod create conflicted but incumbent vanished: %s",
                    pod_name,
                )
                return PersistentPodCreateResult(
                    PersistentPodCreateStatus.CONFLICTING,
                    pod_name,
                    failure_class="conflict_without_incumbent",
                )

            logger.error(
                "Failed to create agent pod for thread %s (%s)",
                thread_id,
                type(e).__name__,
            )
            # Non-409 may be accepted-then-timeout. Re-observe the durable
            # attempt-labelled name and promote its immutable UID when exact;
            # otherwise leave the intent planned for restart/End recovery.
            try:
                incumbent = await self._read_pod(pod_name, namespace=intent_namespace)
                if incumbent is not None:
                    classified = self._classify_incumbent(
                        incumbent,
                        thread_id=thread_id,
                        pod_name=pod_name,
                        lifecycle_generation=lifecycle_generation,
                        session_runtime_generation=runtime_authority.generation,
                        expected_build_sha=target_build_sha,
                        provision_attempt=provision_attempt,
                    )
                    if (
                        classified.status is PersistentPodCreateStatus.ALREADY_CURRENT
                        and classified.pod_uid
                        and await self._runtime_authority_matches(runtime_authority)
                        and await self._db.publish_pinned_agent_pod_provision_intent(
                            thread_id,
                            expected_runtime_generation=runtime_authority.generation,
                            attempt_id=provision_attempt,
                            pod_name=pod_name,
                            pod_uid=classified.pod_uid,
                            namespace=intent_namespace,
                        )
                    ):
                        return classified
            except Exception:
                logger.info(
                    "Persistent Pod create outcome remains owned by intent %s",
                    provision_attempt,
                )
            return PersistentPodCreateResult(
                PersistentPodCreateStatus.FAILED,
                pod_name,
                failure_class=type(e).__name__[:128],
            )

    async def delete_agent_pod(self, thread_id: str) -> bool:
        """Delete the agent pod for a persistent session.

        Args:
            thread_id: Thread UUID.

        Returns:
            True if deleted (or already gone), False on error.
        """
        if not self._k8s_available:
            return False
        pod_name = f"persistent-{thread_id[:12]}"

        try:
            await asyncio.to_thread(
                self._core_api.delete_namespaced_pod,
                name=pod_name,
                namespace=self._namespace,
                grace_period_seconds=30,
            )
            logger.info("Agent pod deleted: %s (thread %s)", pod_name, thread_id)
            await self._set_thread_context(
                thread_id,
                {
                    "status": "deleted",
                    "deleted_at": datetime.now(timezone.utc).isoformat(),
                },
            )
            return True
        except Exception as e:
            if hasattr(e, "status") and e.status == 404:
                logger.debug(
                    "Agent pod already deleted: %s (thread %s)",
                    pod_name,
                    thread_id,
                )
                await self._set_thread_context(
                    thread_id,
                    {
                        "status": "deleted",
                        "deleted_at": datetime.now(timezone.utc).isoformat(),
                    },
                )
                return True
            logger.error("Failed to delete agent pod for thread %s: %s", thread_id, e)
            return False

    async def delete_agent_pod_exact(
        self,
        thread_id: str,
        *,
        expected_pod_uid: str,
        namespace: str | None = None,
    ) -> bool:
        """Delete only the exact old pod object, never a same-name successor."""

        if not self._k8s_available or not str(expected_pod_uid).strip():
            return False
        pod_name = f"persistent-{thread_id[:12]}"
        captured_namespace = namespace or self._namespace
        try:
            await run_bounded_k8s_mutation(
                self._core_api.delete_namespaced_pod,
                name=pod_name,
                namespace=captured_namespace,
                grace_period_seconds=30,
                body={"preconditions": {"uid": str(expected_pod_uid)}},
            )
            return True
        except Exception as exc:
            if getattr(exc, "status", None) in {404, 409}:
                # 409 is the UID precondition protecting a replacement.
                return True
            logger.warning("Exact persistent pod deletion failed for %s", pod_name)
            return False

    async def release_agent_pod_finalizer_exact(
        self,
        thread_id: str,
        *,
        expected_pod_uid: str,
        namespace: str,
        terminal_required: bool = True,
    ) -> bool:
        """Release only SRW's finalizer from one exact persistent Pod UID."""

        return await self._release_pod_finalizer_exact(
            f"persistent-{thread_id[:12]}",
            expected_pod_uid=expected_pod_uid,
            namespace=namespace,
            terminal_required=terminal_required,
        )

    async def delete_agent_pvc(self, thread_id: str) -> bool:
        """Delete the PVC for an agent pod (final cleanup only).

        Called on thread end/deletion — NOT during suspension.
        """
        pvc_name = f"pvc-persistent-{thread_id[:12]}"
        return await self._delete_pvc(pvc_name)

    async def get_pod_status(
        self, thread_id: str, *, namespace: str | None = None
    ) -> Optional[Dict[str, Any]]:
        """Query pod status for a thread.

        Args:
            thread_id: Thread UUID.

        Returns:
            Status dict with pod_name, phase, pod_ip, ready; or None.
        """
        if not self._k8s_available:
            return None

        pod_name = f"persistent-{thread_id[:12]}"
        captured_namespace = namespace or self._namespace

        try:
            pod = await asyncio.to_thread(
                self._core_api.read_namespaced_pod,
                name=pod_name,
                namespace=captured_namespace,
            )

            ready = False
            if pod.status.container_statuses:
                ready = all(cs.ready for cs in pod.status.container_statuses)

            return {
                "thread_id": thread_id,
                "pod_name": pod_name,
                "pod_uid": str(getattr(pod.metadata, "uid", "") or "") or None,
                "phase": pod.status.phase,
                "pod_ip": pod.status.pod_ip,
                "ready": ready,
                "terminating": bool(getattr(pod.metadata, "deletion_timestamp", None)),
                "build_sha": (pod.metadata.labels or {}).get("srw/build-sha"),
                "labels": dict(pod.metadata.labels or {}),
            }
        except Exception as e:
            if hasattr(e, "status") and e.status == 404:
                return None
            logger.error("Failed to query agent pod for thread %s: %s", thread_id, e)
            return None

    async def attest_pinned_session_recipient(
        self,
        pod_name: str,
        *,
        thread_id: str,
        expected_runtime_generation: str,
        expected_pod_uid: str,
        expected_pod_ip: str,
        namespace: str,
    ) -> bool:
        """Freshly attest one exact dedicated session Pod before mutation I/O."""

        name = str(pod_name or "").strip()
        tid = str(thread_id or "").strip()
        generation = str(expected_runtime_generation or "").strip()
        expected_uid = str(expected_pod_uid or "").strip()
        expected_ip = str(expected_pod_ip or "").strip()
        captured_namespace = str(namespace or "").strip()
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
        ):
            return False
        try:
            pod = await self._read_pod(name, namespace=captured_namespace)
        except Exception:
            return False
        if pod is None:
            return False

        metadata = getattr(pod, "metadata", None)
        status = getattr(pod, "status", None)
        labels = dict(getattr(metadata, "labels", None) or {})
        if (
            str(getattr(metadata, "name", "") or "") != name
            or str(getattr(metadata, "namespace", "") or captured_namespace)
            != captured_namespace
            or str(getattr(metadata, "uid", "") or "") != expected_uid
            or getattr(metadata, "deletion_timestamp", None) is not None
            or str(getattr(status, "phase", "") or "") != "Running"
            or str(getattr(status, "pod_ip", "") or "") != expected_ip
            or labels.get("srw/component") != "persistent-agent"
            or labels.get("srw/thread-id") != tid
            or labels.get("srw.io/runtime-generation") != generation
        ):
            return False
        statuses = getattr(status, "container_statuses", None) or []
        return bool(statuses) and all(
            getattr(item, "ready", None) is True for item in statuses
        )

    async def agent_pod_authority(
        self,
        pod_name: str,
        *,
        expected_pod_uid: str,
        namespace: str | None = None,
    ) -> str:
        """Classify one exact deterministic persistent Pod identity."""

        name = str(pod_name or "").strip()
        expected_uid = str(expected_pod_uid or "").strip()
        if not self._k8s_available or not name or not expected_uid:
            return "unknown"
        try:
            pod = await self._read_pod(name, namespace=namespace)
        except Exception:
            return "unknown"
        if pod is None:
            return "exact_absent"
        actual_uid = str(getattr(getattr(pod, "metadata", None), "uid", "") or "")
        if not actual_uid:
            return "unknown"
        if actual_uid != expected_uid:
            return "replacement"
        phase = str(getattr(getattr(pod, "status", None), "phase", "") or "")
        statuses = (
            getattr(getattr(pod, "status", None), "container_statuses", None) or []
        )
        if statuses and all(
            getattr(getattr(status, "state", None), "terminated", None) is not None
            for status in statuses
        ):
            return "exact_terminal"
        if getattr(getattr(pod, "metadata", None), "deletion_timestamp", None):
            return "unknown"
        return "exact_live" if phase in {"Running", "Pending"} else "unknown"

    async def agent_pod_provision_intent_authority(
        self,
        pod_name: str,
        *,
        expected_thread_id: str,
        expected_runtime_generation: str,
        expected_attempt_id: str,
        namespace: str,
    ) -> dict[str, str | None]:
        """Observe one deterministic name through the pre-effect attempt."""

        name = str(pod_name or "").strip()
        if not self._k8s_available or not name or not namespace:
            return {"state": "unknown", "pod_uid": None}
        try:
            pod = await self._read_pod(name, namespace=namespace)
        except Exception:
            return {"state": "unknown", "pod_uid": None}
        if pod is None:
            return {"state": "exact_absent", "pod_uid": None}
        metadata = getattr(pod, "metadata", None)
        labels = dict(getattr(metadata, "labels", None) or {})
        pod_uid = str(getattr(metadata, "uid", "") or "")
        if not pod_uid:
            return {"state": "unknown", "pod_uid": None}
        exact = bool(
            labels.get("srw/component") == "persistent-agent"
            and labels.get("srw/thread-id") == str(expected_thread_id)
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
        """Grandfather one exact pre-0200 persistent Pod/PVC tuple."""

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
                "srw.io/claim-provisioner": "persistent",
            }
        return await protect_legacy_pinned_objects(
            self._core_api,
            namespaces=legacy_pinned_namespace_candidates(self._namespace),
            pod_name=pod_name,
            expected_pod_uid=str(authority.get("pod_uid") or "") or None,
            pod_labels={
                "srw/component": "persistent-agent",
                "srw/thread-id": thread_id,
                "srw.io/runtime-generation": generation,
                "srw.io/provision-attempt": attempt_id,
            },
            pvc_name=pvc_name,
            expected_pvc_uid=expected_pvc_uid,
            pvc_labels=pvc_labels,
        )

    @staticmethod
    def _warm_binding_labels(authority: dict[str, Any]) -> dict[str, str]:
        return {
            "srw/component": "persistent-agent",
            "srw/thread-id": str(authority.get("thread_id") or ""),
        }

    async def discover_pinned_warm_agent_authority(
        self, authority: dict[str, Any]
    ) -> dict[str, Any] | None:
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
        self, authority: dict[str, Any], *, terminal_only: bool = False
    ) -> dict[str, Any] | None:
        if not self._k8s_available or self._core_api is None:
            return None
        return await release_planned_pinned_pod_authority(
            self._core_api,
            namespace=str(authority.get("namespace") or ""),
            pod_name=str(authority.get("pod_name") or ""),
            expected_pod_uid=str(authority.get("pod_uid") or ""),
            expected_labels=self._warm_binding_labels(authority),
            terminal_only=terminal_only,
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
        """Acquire the deterministic Pod name with a credential-free fence."""

        name = str(pod_name or "").strip()
        if not self._k8s_available or not name or not namespace:
            return {"state": "unknown", "pod_uid": None}
        labels = {
            "app": "srw-persistent-agent",
            "srw/component": "persistent-agent",
            "srw/thread-id": str(expected_thread_id),
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
            try:
                incumbent = await self._read_pod(name, namespace=namespace)
            except Exception:
                return {"state": "unknown", "pod_uid": None}
            if incumbent is None:
                return {"state": "unknown", "pod_uid": None}
            if getattr(exc, "status", None) not in {None, 409}:
                # Reading an exact committed fence after a lost response is
                # sufficient; a 404 is deliberately not.
                pass
        metadata = getattr(incumbent, "metadata", None)
        actual_labels = dict(getattr(metadata, "labels", None) or {})
        pod_uid = str(getattr(metadata, "uid", "") or "")
        exact = bool(
            pod_uid
            and actual_labels.get("srw/component") == "persistent-agent"
            and actual_labels.get("srw/thread-id") == str(expected_thread_id)
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
        """Classify one persistent-agent PVC by immutable labels and UID."""

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
            and labels.get("srw.io/claim-provisioner") == "persistent"
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
        """Create/re-attest the exact retained PVC during soft retirement."""

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
            "app": "srw-persistent-agent",
            "srw/component": "agent-workspace-pvc",
            "srw.io/component": "agent-workspace",
            "srw/thread-id": str(expected_thread_id),
            "srw.io/thread-id": str(expected_thread_id),
            "srw.io/runtime-generation": str(expected_runtime_generation),
            "srw.io/workspace-claim": str(expected_claim_id),
            "srw.io/provision-attempt": str(expected_create_attempt),
            "srw.io/claim-provisioner": "persistent",
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
            and actual_labels.get("srw.io/claim-provisioner") == "persistent"
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

    # =========================================================================
    # Internal helpers
    # =========================================================================

    @property
    def expected_build_sha(self) -> str | None:
        return self._build_sha(self._agent_image)

    @staticmethod
    def _build_sha(image_ref: str) -> str | None:
        if ":sha-" not in image_ref:
            return None
        return image_ref.rsplit(":sha-", 1)[-1]

    async def _read_pod(
        self, pod_name: str, *, namespace: str | None = None
    ) -> Any | None:
        try:
            return await run_bounded_k8s_call(
                self._core_api.read_namespaced_pod,
                name=pod_name,
                namespace=namespace or self._namespace,
            )
        except Exception as exc:
            if getattr(exc, "status", None) == 404:
                return None
            logger.warning("Persistent pod observation failed for %s", pod_name)
            raise

    async def _release_pod_finalizer_exact(
        self,
        pod_name: str,
        *,
        expected_pod_uid: str,
        namespace: str,
        terminal_required: bool,
    ) -> bool:
        try:
            pod = await self._read_pod(pod_name, namespace=namespace)
        except Exception:
            return False
        if pod is None:
            return True
        metadata = getattr(pod, "metadata", None)
        uid = str(getattr(metadata, "uid", "") or "")
        resource_version = str(getattr(metadata, "resource_version", "") or "")
        finalizers = [
            str(value) for value in getattr(metadata, "finalizers", None) or []
        ]
        if uid != str(expected_pod_uid) or not resource_version:
            return False
        if terminal_required:
            phase = str(getattr(getattr(pod, "status", None), "phase", "") or "")
            if phase not in {"Failed", "Succeeded"} or not pod_containers_are_terminal(
                pod
            ):
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

    def _classify_incumbent(
        self,
        pod: Any,
        *,
        thread_id: str,
        pod_name: str,
        lifecycle_generation: str | None,
        session_runtime_generation: Optional[str] = None,
        expected_build_sha: str | None,
        provision_attempt: str | None = None,
    ) -> PersistentPodCreateResult:
        metadata = getattr(pod, "metadata", None)
        labels = dict(getattr(metadata, "labels", None) or {})
        pod_uid = str(getattr(metadata, "uid", "") or "") or None
        build_sha = labels.get("srw/build-sha")
        if getattr(metadata, "deletion_timestamp", None):
            return PersistentPodCreateResult(
                PersistentPodCreateStatus.TERMINATING,
                pod_name,
                pod_uid=pod_uid,
                build_sha=build_sha,
            )
        exact = (
            labels.get("srw/component") == "persistent-agent"
            and labels.get("srw/thread-id") == thread_id
            and labels.get("srw.io/runtime-generation") == session_runtime_generation
            and (
                provision_attempt is None
                or labels.get("srw.io/provision-attempt") == provision_attempt
            )
            and (expected_build_sha is None or build_sha == expected_build_sha)
            and (
                lifecycle_generation is None
                or labels.get("srw/recycle-generation") == lifecycle_generation
            )
        )
        return PersistentPodCreateResult(
            (
                PersistentPodCreateStatus.ALREADY_CURRENT
                if exact
                else PersistentPodCreateStatus.CONFLICTING
            ),
            pod_name,
            pod_uid=pod_uid,
            build_sha=build_sha,
            failure_class=None if exact else "incumbent_authority_mismatch",
        )

    async def _create_pvc(
        self, pvc_name: str, size: str = "10Gi", labels: Optional[dict] = None
    ) -> Optional[str]:
        """Create a PVC, distinguishing a new claim from an existing one."""
        if not self._k8s_available:
            return None

        pvc_labels = {
            "app": "srw-persistent-agent",
            "srw/component": "agent-workspace-pvc",
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
                "PVC created: %s (storageClass=%s)", pvc_name, self._storage_class
            )
            return "created"
        except Exception as e:
            if hasattr(e, "status") and e.status == 409:
                logger.debug("PVC already exists: %s", pvc_name)
                return "reused"
            logger.error("Failed to create PVC %s: %s", pvc_name, e)
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
        """Create/observe one UID-fenced retained persistent-agent claim."""

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
            "app": "srw-persistent-agent",
            "srw/component": "agent-workspace-pvc",
            "srw.io/component": "agent-workspace",
            "srw/thread-id": str(thread_id),
            "srw.io/thread-id": str(thread_id),
            "srw.io/runtime-generation": str(runtime_generation),
            "srw.io/workspace-claim": str(claim_id),
            "srw.io/provision-attempt": str(create_attempt),
            "srw.io/claim-provisioner": "persistent",
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
                "resources": {"requests": {"storage": "10Gi"}},
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
                and actual_labels.get("srw.io/claim-provisioner") == "persistent"
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
            return await _read_exact()

    async def _delete_pvc(self, pvc_name: str) -> bool:
        """Delete a PVC. Idempotent — 404 treated as success."""
        if not self._k8s_available:
            return False

        try:
            await asyncio.to_thread(
                self._core_api.delete_namespaced_persistent_volume_claim,
                name=pvc_name,
                namespace=self._namespace,
            )
            logger.info("PVC deleted: %s", pvc_name)
            return True
        except Exception as e:
            if hasattr(e, "status") and e.status == 404:
                logger.debug("PVC already deleted: %s", pvc_name)
                return True
            logger.error("Failed to delete PVC %s: %s", pvc_name, e)
            return False

    def _build_agent_pod_manifest(
        self,
        pod_name: str,
        thread_id: str,
        config_name: str,
        cpu_request: str,
        memory_request: str,
        cpu_limit: str,
        memory_limit: str,
        pvc_name: Optional[str] = None,
        expert_id: Optional[str] = None,
        runtime_actor_bootstrap: Optional[str] = None,
        lifecycle_generation: Optional[str] = None,
        session_runtime_generation: Optional[str] = None,
        provision_attempt: Optional[str] = None,
        image_ref: Optional[str] = None,
        namespace: Optional[str] = None,
    ) -> dict:
        """Build the Kubernetes Pod manifest for a persistent agent.

        Uses ``envFrom`` to inject all keys from the shared ConfigMap and
        Secret, avoiding duplication of the 60+ env vars from the static
        Deployment.  Pod-specific overrides (AGENT_CONFIG, AGENT_PORT) are
        set via ``env``.
        """
        # The sink re-checks what the boundary already checked: whichever
        # path reaches this builder, a name that is not a bundled-config
        # selector never becomes a pod spec.
        validate_config_name(config_name)
        # Shell-quoted as a list, never string-formatted — see
        # agent_pod_entrypoint.
        agent_argv = [
            "python",
            "agent.py",
            "--mode",
            "persistent",
            "--thread-id",
            str(thread_id),
            "--config",
            config_name,
            "--port",
            "8001",
            "--host",
            "0.0.0.0",
        ]
        labels = {
            "app": "srw-persistent-agent",
            "srw/thread-id": thread_id,
            "srw/component": "persistent-agent",
        }
        # NetworkPolicy admission (see __init__): the Helm-rendered DB
        # policies select component=agent specifically — "persistent-agent"
        # does not match them.
        if self._chart_label_name:
            labels["app.kubernetes.io/name"] = self._chart_label_name
        if self._chart_label_instance:
            labels["app.kubernetes.io/instance"] = self._chart_label_instance
        if self._chart_label_name or self._chart_label_instance:
            labels["app.kubernetes.io/component"] = "agent"
        # Build SHA — lets the lifecycle reconciler enumerate stale pods by
        # selector, same convention as agent_provisioner.
        selected_image = image_ref or self._agent_image
        build_sha = self._build_sha(selected_image)
        if build_sha:
            labels["srw/build-sha"] = build_sha
        if lifecycle_generation:
            labels["srw/recycle-generation"] = str(lifecycle_generation)
        if session_runtime_generation:
            labels["srw.io/runtime-generation"] = session_runtime_generation
        if provision_attempt:
            labels["srw.io/provision-attempt"] = provision_attempt
        return {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": pod_name,
                "namespace": namespace or self._namespace,
                "labels": labels,
                "finalizers": [PINNED_AUTHORITY_FINALIZER],
            },
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
                "containers": [
                    {
                        "name": "agent",
                        "image": selected_image,
                        "imagePullPolicy": self._agent_image_pull_policy,
                        # ``exec`` so python, not ``sh``, is PID 1 and the
                        # kubelet's SIGTERM reaches the drain handler; every
                        # argv word is shell-quoted, so the shell only ever
                        # sees the one command.
                        "command": agent_exec_command(agent_argv),
                        # Kubernetes exposes deletionTimestamp outside the
                        # container before the process can observe it.  preStop
                        # creates a pod-local sentinel first, closing input and
                        # provider admission synchronously inside the runtime,
                        # then holds the grace window while the current tool
                        # batch settles.  Abrupt node loss may skip hooks and is
                        # handled by the persistent transcript/LF-5 recovery
                        # path instead.
                        "lifecycle": {
                            "preStop": {
                                "exec": {
                                    "command": [
                                        "sh",
                                        "-c",
                                        ": > /tmp/srw-persistent-terminating; "
                                        "exec python -m src.api.persistent_termination",
                                    ]
                                }
                            }
                        },
                        "ports": [{"containerPort": 8001}],
                        # Inject all env from shared ConfigMap + Secret
                        "envFrom": [
                            {
                                "configMapRef": {
                                    "name": self._configmap_name,
                                },
                            },
                            {
                                "secretRef": {
                                    "name": self._secret_name,
                                },
                            },
                        ],
                        # Pod-specific overrides
                        "env": [
                            {"name": "AGENT_CONFIG", "value": config_name},
                            {"name": "AGENT_PORT", "value": "8001"},
                            {
                                "name": "POD_UID",
                                "valueFrom": {
                                    "fieldRef": {
                                        "apiVersion": "v1",
                                        "fieldPath": "metadata.uid",
                                    }
                                },
                            },
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
                            {
                                "name": "SRW_RUNTIME_ACTOR_BOOTSTRAP",
                                "value": runtime_actor_bootstrap or "",
                            },
                            {
                                "name": "SESSION_RUNTIME_GENERATION",
                                "value": session_runtime_generation or "",
                            },
                        ]
                        + (
                            [{"name": "AGENT_EXPERT_ID", "value": expert_id}]
                            if expert_id
                            else []
                        ),
                        "securityContext": {
                            "runAsNonRoot": True,
                            "runAsUser": 999,
                            "runAsGroup": 999,
                            "allowPrivilegeEscalation": False,
                            "readOnlyRootFilesystem": True,
                            "capabilities": {"drop": ["ALL"]},
                        },
                        "volumeMounts": [
                            {
                                "name": "workspace",
                                "mountPath": "/workspace",
                            },
                            {
                                "name": "vm-ssh-key",
                                "mountPath": "/run/secrets/vm-ssh-key",
                                "subPath": "ssh-privatekey",
                                "readOnly": True,
                            },
                            {"name": "tmp", "mountPath": "/tmp"},
                            {"name": "run", "mountPath": "/run"},
                            {
                                "name": "home-srw",
                                "mountPath": "/home/srw",
                            },
                        ],
                        # timeoutSeconds raised from the 1s default: token
                        # counting and restore paths can block the event loop
                        # for seconds at a time, and a 1s probe deadline
                        # SIGKILLed a healthy officer pod mid-turn (exit 137,
                        # k3d smoke). 5s tolerates legitimate loop stalls while
                        # still catching a truly wedged process.
                        "livenessProbe": {
                            "httpGet": {
                                "path": "/health",
                                "port": 8001,
                            },
                            "initialDelaySeconds": 60,
                            "periodSeconds": 30,
                            "timeoutSeconds": 5,
                            "failureThreshold": 4,
                        },
                        # /health, NOT /ready: /ready reports "free to accept
                        # a session" (503 while one is attached), which left
                        # dedicated pods 0/1-Ready while demonstrably serving
                        # turns (k3d smoke, open item 8). Dedicated pods are
                        # addressed by pod IP, never through a Service
                        # selector, so readiness here is operator signal —
                        # and the honest signal is process health.
                        "readinessProbe": {
                            "httpGet": {
                                "path": "/health",
                                "port": 8001,
                            },
                            "initialDelaySeconds": 30,
                            "periodSeconds": 10,
                            "timeoutSeconds": 5,
                        },
                        "startupProbe": {
                            "httpGet": {
                                "path": "/health",
                                "port": 8001,
                            },
                            "failureThreshold": 10,
                            "periodSeconds": 10,
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
                    }
                ],
                "volumes": [
                    {
                        "name": "workspace",
                        "persistentVolumeClaim": {"claimName": pvc_name},
                    }
                    if pvc_name
                    else {
                        "name": "workspace",
                        "emptyDir": {"sizeLimit": "10Gi"},
                    },
                    {
                        "name": "vm-ssh-key",
                        "secret": {
                            "secretName": self._ssh_secret_name,
                            "defaultMode": 0o444,
                        },
                    },
                    {
                        "name": "tmp",
                        "emptyDir": {
                            "medium": "Memory",
                            "sizeLimit": "256Mi",
                        },
                    },
                    {
                        "name": "run",
                        "emptyDir": {
                            "medium": "Memory",
                            "sizeLimit": "16Mi",
                        },
                    },
                    {
                        "name": "home-srw",
                        "emptyDir": {"sizeLimit": "512Mi"},
                    },
                ],
            },
        }

    async def _wait_for_ready(
        self, pod_name: str, timeout: int = 120, *, namespace: str | None = None
    ) -> Optional[str]:
        """Poll until the agent pod is Running and has an IP.

        Returns:
            Pod IP if ready, None if timeout.
        """
        deadline = asyncio.get_event_loop().time() + timeout

        while asyncio.get_event_loop().time() < deadline:
            try:
                pod = await asyncio.to_thread(
                    self._core_api.read_namespaced_pod,
                    name=pod_name,
                    namespace=namespace or self._namespace,
                )
                if pod.status.phase == "Running" and pod.status.pod_ip:
                    if pod.status.container_statuses and all(
                        cs.ready for cs in pod.status.container_statuses
                    ):
                        return pod.status.pod_ip
            except Exception:
                pass

            await asyncio.sleep(2)

        return None

    async def _set_thread_context(
        self,
        thread_id: str,
        updates: dict,
        *,
        expected_runtime_generation: str | None = None,
    ) -> bool:
        """Store agent pod status in thread metadata.

        Uses the existing ``merge_thread_workspace_context`` with an
        ``agent_pod`` wrapper key so it doesn't collide with workspace
        container context.
        """
        if not self._db:
            return False

        try:
            # We store under metadata.agent_pod by wrapping the merge.
            # The existing merge_thread_workspace_context merges into
            # metadata.workspace_container — we need a custom approach.
            # For simplicity, do a direct JSONB merge on metadata.agent_pod.
            import json

            async with self._db.acquire() as conn:
                result = await conn.execute(
                    """
                    UPDATE threads
                    SET metadata      = jsonb_set(
                            COALESCE(metadata, '{}'),
                            '{agent_pod}',
                            COALESCE(metadata->'agent_pod', '{}'::jsonb) || $2::jsonb
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
persistent_provisioner = PersistentProvisioner()
