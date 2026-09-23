"""Shared pinned-thread retirement authority and exact external actuators.

This module owns the immutable retirement-context validators, process-zero
recovery, agent/workspace claim reconciliation, and physical cleanup sequence.
Application construction supplies every stateful collaborator explicitly;
leader scheduling and HTTP policy remain owned by the root application.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
import json
import logging
import re
from typing import Any
from uuid import UUID

from orchestrator.services.cloud.protected_reader_authority import (
    ProtectedNextcloudReaderGrantPlan,
)
from orchestrator.services.cloud.ro_engage import revoke_ro_mount_attempt
from orchestrator.services.container_provisioner import (
    WORKSPACE_RUNTIME_INCARNATION_KEY,
)
from orchestrator.services.pinned_agent_authority import (
    reconcile_legacy_pinned_agent_authority,
    release_pinned_warm_binding_protection,
)
from orchestrator.services.workspace_lifecycle import WorkspaceOwner
from orchestrator.services.vm_workspace_recovery_store import (
    acquire_vm_cleanup_permit,
    vm_cleanup_kwargs,
    completed_cleanup_outcome,
    complete_vm_cleanup_permit,
)


@dataclass(frozen=True, slots=True)
class PinnedRetirementDependencies:
    """Exact stores and external actuators used by pinned retirement."""

    store: Any
    agent_provisioner: Any
    persistent_provisioner: Any
    container_provisioner: Any
    docker_provisioner: Any
    vm_provisioner: Any
    recovery_store: Any
    session_router: Any
    resolve_protected_reader_backend: Callable[[Any], Awaitable[Any]]
    resolve_ssh_key_path: Callable[[], Any]
    logger: logging.Logger


@dataclass(frozen=True, slots=True)
class PinnedRetirementOperations:
    """Application-owned pinned retirement operation set."""

    dependencies: PinnedRetirementDependencies

    async def _admit_vm_cleanup(
        self, thread_id: str, identity: Any, *, purge_disk: bool
    ) -> Any | None:
        permit = await acquire_vm_cleanup_permit(
            self.dependencies.recovery_store,
            owner_kind="thread",
            owner_id=thread_id,
            identity=identity,
            source="pinned_thread_retirement",
            purge_disk=purge_disk,
        )
        return permit if permit.allowed else None

    async def _complete_vm_cleanup(self, permit: Any, outcome: str) -> None:
        if outcome in {"completed", "identity_superseded"}:
            await complete_vm_cleanup_permit(
                self.dependencies.recovery_store,
                permit,
                outcome=outcome,
            )

    async def begin_pinned_thread_retirement(
        self, thread_id: str, **kwargs: Any
    ) -> dict[str, Any]:
        """Adopt exact legacy Kubernetes authority before freezing retirement."""

        try:
            legacy = await reconcile_legacy_pinned_agent_authority(
                self.dependencies.store,
                agent_provisioner=self.dependencies.agent_provisioner,
                persistent_provisioner=self.dependencies.persistent_provisioner,
                thread_id=thread_id,
                limit=2,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            self.dependencies.logger.exception(
                "Pinned legacy Kubernetes authority adoption failed before End: %s",
                thread_id,
            )
            return {
                "state": "malformed",
                "reason": "agent_k8s_authority_adoption_failed",
            }
        if not legacy.complete:
            return {
                "state": "malformed",
                "reason": "agent_k8s_authority_adoption_unresolved",
            }
        return await self.dependencies.store.begin_pinned_thread_retirement(
            thread_id, **kwargs
        )

    async def _pinned_retirement_is_current(
        self, retirement: Mapping[str, Any]
    ) -> bool:
        """Recheck one durable pinned cleanup token without reopening admission."""

        current = await self.dependencies.store.get_thread(
            str(retirement.get("context", {}).get("thread_id") or "")
        )
        return bool(
            current
            and str(current.get("runtime_generation") or "")
            == str(retirement.get("generation") or "")
            and str(current.get("runtime_retirement_token") or "")
            == str(retirement.get("token") or "")
            and bool(current.get("runtime_retirement_permanent"))
            == bool(retirement.get("permanent"))
        )

    def _retirement_context_runtime_exposed(
        self,
        retirement: Mapping[str, Any],
    ) -> bool:
        """Whether this generation ever delivered a process authority.

        New Begin rows carry the database-monotonic exposure bit.  The identity
        fallback is deliberately conservative for an already-pending marker
        written during a rolling upgrade.
        """

        context = retirement.get("context")
        context = {} if context is None else context
        if not isinstance(context, Mapping):
            return True
        return bool(
            context.get("runtime_authority_exposed") is True
            or context.get("agent_id")
            or context.get("runtime_attach_token")
            or context.get("agent") not in (None, {})
            or context.get("agent_pod") not in (None, {})
            or context.get("agent_pod_provision_intent") not in (None, {})
        )

    def _retirement_json_field_is_nonnull(
        self, value: Mapping[str, Any], key: str
    ) -> bool:
        return key in value and value[key] is not None

    def _retirement_json_status_is_absent(self, value: Mapping[str, Any]) -> bool:
        if "status" not in value or value["status"] is None:
            return True
        return isinstance(value["status"], str) and value["status"] in {"", "deleted"}

    def _never_delivered_protected_reader_shape(
        self,
        retirement: Mapping[str, Any],
        thread: Mapping[str, Any],
        row: Mapping[str, Any] | None,
        *,
        require_current_revoked: bool,
    ) -> bool:
        """Prove a protected grant existed but no runtime could receive it.

        ``cloud_ro_mounts.status='active'`` records backend probe completion, not
        credential delivery.  A retirement that captured no actor, workspace,
        binding, VM, or staged overlay may revoke that exact reader first and then
        publish a zero-stage receipt.  This predicate is shared by the pre-stage
        actuator and receipt replay so a later/replacement row cannot be relabelled
        as the captured source.
        """

        context = retirement.get("context")
        context = {} if context is None else context
        if not isinstance(context, Mapping):
            return False
        raw_metadata = thread.get("metadata")
        raw_metadata = {} if raw_metadata is None else raw_metadata
        if isinstance(raw_metadata, str):
            try:
                raw_metadata = json.loads(raw_metadata)
            except (TypeError, ValueError):
                return False
        if not isinstance(raw_metadata, Mapping):
            return False

        def _object_or_empty(value: Any) -> Mapping[str, Any] | None:
            if value is None:
                return {}
            return value if isinstance(value, Mapping) else None

        captured_workspace = _object_or_empty(context.get("workspace_container"))
        captured_binding = _object_or_empty(context.get("workspace_binding"))
        captured_vm = _object_or_empty(context.get("vm"))
        current_workspace = _object_or_empty(raw_metadata.get("workspace_container"))
        current_binding = _object_or_empty(raw_metadata.get("_workspace_binding"))
        current_vm = _object_or_empty(raw_metadata.get("vm"))
        if any(
            value is None
            for value in (
                captured_workspace,
                captured_binding,
                captured_vm,
                current_workspace,
                current_binding,
                current_vm,
            )
        ):
            return False
        assert captured_workspace is not None
        assert captured_binding is not None
        assert captured_vm is not None
        assert current_workspace is not None
        assert current_binding is not None
        assert current_vm is not None

        def _empty_actor(value: Any) -> bool:
            return value is None or isinstance(value, Mapping) and not value

        workspace_fields = (
            WORKSPACE_RUNTIME_INCARNATION_KEY,
            "_docker_workspace_lease_id",
            "pod_ip",
            "pod_name",
            "host",
            "port",
            "ide_host",
            "ide_port",
            "_canvas_workspace_generation",
        )
        vm_fields = (
            "provision_generation",
            "identity_provision_generation",
            "vm_uid",
            "_runtime_incarnation",
            "rootdisk_pvc_uid",
            "ssh_host",
            "ssh_port",
            "_canvas_workspace_generation",
        )
        captured_agent_pod = context.get("agent_pod")
        captured_provision_intent = context.get("agent_pod_provision_intent")
        pre_registration_process_zero = bool(
            context.get("runtime_authority_exposed") is True
            and thread.get("runtime_authority_exposed") is True
            and context.get("agent_id") is None
            and context.get("runtime_attach_token") is None
            and _empty_actor(context.get("agent"))
            and isinstance(captured_agent_pod, Mapping)
            and captured_agent_pod
            and _empty_actor(raw_metadata.get("agent_pod"))
            and self._retirement_has_exact_local_quiescence(retirement, thread)
        )
        pre_provision_process_zero = bool(
            context.get("runtime_authority_exposed") is True
            and thread.get("runtime_authority_exposed") is True
            and context.get("agent_id") is None
            and context.get("runtime_attach_token") is None
            and _empty_actor(context.get("agent"))
            and _empty_actor(context.get("agent_pod"))
            and isinstance(captured_provision_intent, Mapping)
            and captured_provision_intent
            and _empty_actor(raw_metadata.get("agent_pod"))
            and self._retirement_has_exact_local_quiescence(retirement, thread)
        )
        process_authority_zero = bool(
            context.get("runtime_authority_exposed") is False
            and thread.get("runtime_authority_exposed") is False
            and _empty_actor(context.get("agent"))
            and _empty_actor(context.get("agent_pod"))
            or pre_registration_process_zero
            or pre_provision_process_zero
        )
        no_runtime = bool(
            str(thread.get("runtime_generation") or "")
            == str(retirement.get("generation") or "")
            and str(thread.get("runtime_retirement_token") or "")
            == str(retirement.get("token") or "")
            and context.get("protected_cloud") is True
            and raw_metadata.get("protected_cloud") is True
            and process_authority_zero
            and context.get("agent_id") is None
            and context.get("control_admission_agent_id") is None
            and context.get("runtime_attach_token") is None
            and _empty_actor(raw_metadata.get("agent_pod"))
            and thread.get("agent_id") is None
            and thread.get("control_admission_agent_id") is None
            and thread.get("runtime_attach_token") is None
            and captured_workspace == current_workspace
            and captured_binding == current_binding == {}
            and captured_vm == current_vm
            and self._retirement_json_status_is_absent(captured_workspace)
            and not any(
                self._retirement_json_field_is_nonnull(captured_workspace, field)
                for field in workspace_fields
            )
            and self._retirement_json_status_is_absent(captured_vm)
            and not any(
                self._retirement_json_field_is_nonnull(captured_vm, field)
                for field in vm_fields
            )
        )
        if not no_runtime:
            return False

        captured_ro = context.get("protected_ro")
        if captured_ro is None:
            return row is None
        if not isinstance(captured_ro, Mapping):
            return False
        try:
            UUID(str(captured_ro["id"]))
            UUID(str(captured_ro["runtime_generation"]))
            staged_epoch = int(captured_ro["staged_epoch"])
        except (KeyError, TypeError, ValueError):
            return False
        captured_plan = ProtectedNextcloudReaderGrantPlan.from_ro_mount_row(captured_ro)
        if captured_plan is None:
            return False
        captured_status = str(captured_ro.get("status") or "")
        captured_baseline = captured_ro.get("etag_baseline")
        if not (
            str(captured_ro.get("runtime_generation") or "")
            == str(retirement.get("generation") or "")
            and captured_ro.get("backend") == "nextcloud"
            and str(captured_ro.get("user_id") or "")
            == str(context.get("user_id") or "")
            and str(captured_ro.get("thread_id") or "")
            == str(context.get("thread_id") or "")
            and captured_status in {"engaging", "active", "revoking", "revoked"}
            and (
                isinstance(captured_baseline, Mapping)
                if captured_status == "active"
                else captured_baseline is None or isinstance(captured_baseline, Mapping)
            )
            and staged_epoch == 0
            and captured_ro.get("staged_summary") is None
        ):
            return False
        if row is None:
            return True
        try:
            current_epoch = int(row["staged_epoch"])
        except (KeyError, TypeError, ValueError):
            return False
        current_plan = ProtectedNextcloudReaderGrantPlan.from_ro_mount_row(row)
        expected_current_statuses = (
            {"revoked"}
            if require_current_revoked
            else {
                captured_status,
                "revoking",
                "revoked",
            }
        )
        return bool(
            str(row.get("id") or "") == str(captured_ro.get("id") or "")
            and current_plan == captured_plan
            and str(row.get("user_id") or "") == str(captured_ro.get("user_id") or "")
            and str(row.get("runtime_generation") or "")
            == str(captured_ro.get("runtime_generation") or "")
            and str(row.get("engage_attempt") or "")
            == str(captured_ro.get("engage_attempt") or "")
            and str(row.get("status") or "") in expected_current_statuses
            and row.get("etag_baseline") == captured_baseline
            and current_epoch == 0
            and row.get("staged_summary") is None
        )

    async def _revoke_never_delivered_protected_reader(
        self,
        retirement: Mapping[str, Any],
        thread: Mapping[str, Any],
        row: Mapping[str, Any] | None,
    ) -> bool:
        """Exact-revoke the pre-agent reader before mandatory soft staging."""

        context = retirement.get("context") or {}
        captured_ro = (
            context.get("protected_ro") if isinstance(context, Mapping) else None
        )
        if not isinstance(captured_ro, Mapping) or not (
            self._never_delivered_protected_reader_shape(
                retirement,
                thread,
                row,
                require_current_revoked=False,
            )
        ):
            return False
        plan = ProtectedNextcloudReaderGrantPlan.from_ro_mount_row(captured_ro)
        if plan is None:
            return False
        backend = await self.dependencies.resolve_protected_reader_backend(plan)
        current_status = str((row or {}).get("status") or "")
        revoked = current_status == "revoked"
        if not revoked:
            revoked = await revoke_ro_mount_attempt(
                backend=backend,
                postgres_db=self.dependencies.store,
                row_id=str(captured_ro.get("id") or ""),
                thread_id=str(context.get("thread_id") or ""),
                runtime_generation=str(captured_ro.get("runtime_generation") or ""),
                plan=plan,
            )
        current = await self.dependencies.store.get_ro_mount_by_thread(
            str(context.get("thread_id") or "")
        )
        if (
            not revoked
            and current is not None
            and str(current.get("status") or "") != "revoked"
        ):
            return False
        fresh_thread = await self.dependencies.store.get_thread(
            str(context.get("thread_id") or "")
        )
        return bool(
            fresh_thread
            and self._never_delivered_protected_reader_shape(
                retirement,
                fresh_thread,
                current,
                require_current_revoked=True,
            )
        )

    def _retirement_has_exact_local_quiescence(
        self, retirement: Mapping[str, Any], thread: Mapping[str, Any]
    ) -> bool:
        """Validate the append-only local cleanup receipt for this exact life."""

        context = retirement.get("context")
        context = {} if context is None else context
        receipt = thread.get("runtime_retirement_local_quiescence")
        if isinstance(receipt, str):
            try:
                receipt = json.loads(receipt)
            except (TypeError, ValueError):
                return False
        if not isinstance(context, Mapping) or not isinstance(receipt, Mapping):
            return False
        workspace = context.get("workspace_container")
        binding = context.get("workspace_binding")
        vm = context.get("vm")
        agent = context.get("agent")
        agent_pod = context.get("agent_pod")
        provision_intent = context.get("agent_pod_provision_intent")
        workspace_provision_intent = context.get("workspace_provision_intent")
        workspace_claim = context.get("agent_workspace_claim")
        protected_ro = context.get("protected_ro")
        workspace = {} if workspace is None else workspace
        binding = {} if binding is None else binding
        vm = {} if vm is None else vm
        agent = {} if agent is None else agent
        agent_pod = {} if agent_pod is None else agent_pod
        provision_intent = {} if provision_intent is None else provision_intent
        workspace_provision_intent = (
            {} if workspace_provision_intent is None else workspace_provision_intent
        )
        workspace_claim = {} if workspace_claim is None else workspace_claim
        protected_ro = {} if protected_ro is None else protected_ro
        if not all(
            isinstance(value, Mapping)
            for value in (
                workspace,
                binding,
                vm,
                agent,
                agent_pod,
                provision_intent,
                workspace_provision_intent,
                workspace_claim,
                protected_ro,
            )
        ):
            return False
        if (
            agent
            and (not agent.get("hostname") or not agent.get("pod_uid"))
            or agent_pod
            and (not agent_pod.get("pod_name") or not agent_pod.get("pod_uid"))
            or provision_intent
            and (
                not provision_intent.get("attempt_id")
                or not provision_intent.get("pod_name")
                or provision_intent.get("provisioner") not in {"agent", "persistent"}
                or provision_intent.get("status") != "planned"
            )
            or workspace_claim
            and (
                not workspace_claim.get("claim_id")
                or not workspace_claim.get("pvc_name")
                or workspace_claim.get("provisioner") not in {"agent", "persistent"}
                or workspace_claim.get("status") not in {"planned", "ready"}
                or (workspace_claim.get("status") == "ready")
                != bool(str(workspace_claim.get("pvc_uid") or ""))
            )
        ):
            return False
        backend = str(context.get("workspace_backend") or "")
        sandbox_generation = binding.get("generation")
        sandbox_runtime = (
            workspace.get("_docker_workspace_lease_id")
            if workspace.get("provisioner") == "docker"
            else workspace.get(WORKSPACE_RUNTIME_INCARNATION_KEY)
        )
        sandbox_physical_evidence = bool(
            binding
            or self._retirement_json_field_is_nonnull(
                workspace, WORKSPACE_RUNTIME_INCARNATION_KEY
            )
            or not self._retirement_json_status_is_absent(workspace)
            or any(
                self._retirement_json_field_is_nonnull(workspace, field)
                for field in (
                    "pod_ip",
                    "pod_name",
                    "host",
                    "port",
                    "ide_host",
                    "ide_port",
                    "_canvas_workspace_generation",
                )
            )
        )
        pre_provision_intent_zero = bool(
            provision_intent
            and not agent
            and not agent_pod
            and context.get("agent_id") is None
            and context.get("runtime_attach_token") is None
        )
        workspace_create_pending = bool(workspace_provision_intent)
        if pre_provision_intent_zero or workspace_create_pending:
            expected_protocol = "agent_runtime_zero_v1"
        elif backend == "sandbox":
            if (
                bool(retirement.get("permanent"))
                and sandbox_physical_evidence
                and str(receipt.get("quiescence_actor") or "") == "orchestrator"
                and str(receipt.get("quiescence_protocol") or "")
                == "sandbox_actuator_zero_v1"
            ):
                expected_protocol = "sandbox_actuator_zero_v1"
            elif sandbox_generation and sandbox_runtime:
                expected_protocol = "workspace_process_zero_v1"
            elif not sandbox_generation and not sandbox_runtime:
                expected_protocol = "agent_runtime_zero_v1"
            else:
                return False
        elif backend in {"virtual", "none"}:
            expected_protocol = "agent_runtime_zero_v1"
        elif backend in {"vm", "remote"}:
            expected_protocol = "workspace_actuator_zero_v1"
        else:
            expected_protocol = None
        expected_workspace_generation = (
            None
            if pre_provision_intent_zero or workspace_create_pending
            else sandbox_generation
            if backend == "sandbox"
            else vm.get("provision_generation")
            if backend in {"vm", "remote"}
            else None
        )
        expected_workspace_runtime = (
            None
            if pre_provision_intent_zero or workspace_create_pending
            else sandbox_runtime
            if backend == "sandbox"
            else vm.get("vm_uid")
            if backend in {"vm", "remote"}
            else None
        )
        pre_registration_pod_zero = bool(
            agent_pod
            and not agent
            and context.get("agent_id") is None
            and context.get("runtime_attach_token") is None
        )
        return bool(
            int(receipt.get("version") or 0) == 1
            and str(receipt.get("runtime_generation") or "")
            == str(retirement.get("generation") or "")
            and str(receipt.get("retirement_token") or "")
            == str(retirement.get("token") or "")
            and str(receipt.get("agent_id") or "") == str(context.get("agent_id") or "")
            and str(receipt.get("runtime_attach_token") or "")
            == str(context.get("runtime_attach_token") or "")
            and str(receipt.get("settle_status") or "")
            == str(context.get("settle_status") or "")
            and expected_protocol is not None
            and str(receipt.get("quiescence_protocol") or "") == expected_protocol
            and str(receipt.get("quiescence_actor") or "") in {"agent", "orchestrator"}
            and str(receipt.get("workspace_generation") or "")
            == str(expected_workspace_generation or "")
            and str(receipt.get("workspace_runtime_incarnation") or "")
            == str(expected_workspace_runtime or "")
            and (
                not pre_registration_pod_zero
                or (
                    str(receipt.get("quiescence_actor") or "") == "orchestrator"
                    and str(receipt.get("quiescence_protocol") or "")
                    == expected_protocol
                    and str(receipt.get("agent_pod_name") or "")
                    == str(agent_pod.get("pod_name") or "")
                    and str(receipt.get("agent_pod_uid") or "")
                    == str(agent_pod.get("pod_uid") or "")
                )
            )
            and (
                not pre_provision_intent_zero
                or (
                    str(receipt.get("quiescence_actor") or "") == "orchestrator"
                    and str(receipt.get("quiescence_protocol") or "")
                    == "agent_runtime_zero_v1"
                    and str(receipt.get("agent_pod_provision_attempt") or "")
                    == str(provision_intent.get("attempt_id") or "")
                    and str(receipt.get("agent_pod_name") or "")
                    == str(provision_intent.get("pod_name") or "")
                    and bool(str(receipt.get("agent_pod_uid") or ""))
                    and receipt.get("agent_pod_fence_protocol")
                    == "k8s_name_tombstone_v1"
                )
            )
        )

    async def _wait_for_captured_agent_pod_retired(
        self,
        pod_name: str,
        pod_uid: str,
        *,
        namespace: str,
        allowed: set[str],
        timeout_s: float = 60.0,
    ) -> str | None:
        """Wait until one exact Pod process is absent/terminal, never by name."""

        deadline = asyncio.get_running_loop().time() + timeout_s
        while True:
            authority = await self.dependencies.agent_provisioner.agent_pod_authority(
                pod_name, expected_pod_uid=pod_uid, namespace=namespace
            )
            if authority in allowed:
                return authority
            if (
                authority != "exact_live"
                and asyncio.get_running_loop().time() >= deadline
            ):
                return None
            if asyncio.get_running_loop().time() >= deadline:
                return None
            await asyncio.sleep(0.25)

    def _captured_retirement_agent_pods(
        self,
        retirement: Mapping[str, Any],
    ) -> set[tuple[str, str, str, str]]:
        """Return immutable agent Pod name/UID pairs from one Begin context."""

        context = retirement.get("context")
        context = {} if context is None else context
        if not isinstance(context, Mapping):
            return set()
        captured: set[tuple[str, str, str, str]] = set()
        agent_pod = context.get("agent_pod") or {}
        pod_namespace = (
            str(agent_pod.get("namespace") or "")
            if isinstance(agent_pod, Mapping)
            else ""
        )
        protection_protocol = (
            str(agent_pod.get("protection_protocol") or "")
            if isinstance(agent_pod, Mapping)
            else ""
        )
        agent = context.get("agent") or {}
        if isinstance(agent, Mapping) and agent:
            captured.add(
                (
                    str(agent.get("hostname") or ""),
                    str(agent.get("pod_uid") or ""),
                    pod_namespace,
                    protection_protocol,
                )
            )
        if isinstance(agent_pod, Mapping) and agent_pod:
            captured.add(
                (
                    str(agent_pod.get("pod_name") or ""),
                    str(agent_pod.get("pod_uid") or ""),
                    pod_namespace,
                    protection_protocol,
                )
            )
        return captured

    async def _stop_captured_retirement_agent(
        self,
        retirement: Mapping[str, Any],
    ) -> None:
        """Delete/wait only the exact captured actor process, never by name."""

        for (
            pod_name,
            pod_uid,
            namespace,
            protection_protocol,
        ) in self._captured_retirement_agent_pods(retirement):
            if not pod_name and not pod_uid:
                continue
            if not (
                pod_name
                and pod_uid
                and namespace
                and protection_protocol == "finalizer_v1"
                and self.dependencies.agent_provisioner.is_available
            ):
                raise RuntimeError("captured agent Pod identity is incomplete")
            if not await self.dependencies.agent_provisioner.delete_agent_pod_exact(
                pod_name,
                expected_pod_uid=pod_uid,
                namespace=namespace,
            ):
                raise RuntimeError("exact agent Pod deletion is retryable")
            terminal = await self._wait_for_captured_agent_pod_retired(
                pod_name,
                pod_uid,
                namespace=namespace,
                allowed={"exact_terminal", "exact_absent"},
            )
            if terminal not in {"exact_terminal", "exact_absent"}:
                raise RuntimeError("exact agent Pod termination is retryable")
            # A prior attempt may have released the exact terminal Pod's finalizer
            # before it could append the durable local-quiescence receipt.
            if (
                terminal == "exact_terminal"
                and not await self.dependencies.agent_provisioner.release_agent_pod_finalizer_exact(
                    pod_name,
                    expected_pod_uid=pod_uid,
                    namespace=namespace,
                    terminal_required=True,
                )
            ):
                raise RuntimeError("exact agent Pod finalizer release is retryable")
            absent = await self._wait_for_captured_agent_pod_retired(
                pod_name,
                pod_uid,
                namespace=namespace,
                allowed={"exact_absent", "replacement"},
            )
            if absent not in {"exact_absent", "replacement"}:
                raise RuntimeError("exact agent Pod final deletion is retryable")

    def _pre_registration_agent_pod_zero_candidate(
        self, retirement: Mapping[str, Any], thread: Mapping[str, Any]
    ) -> tuple[str, str] | None:
        """Return the sole captured Pod for a created life never registered.

        The retirement token closes the persistent registration/bind boundary.
        This shape has no session identity capable of acknowledging cleanup, so an
        exact Pod UID stop is the only truthful process-zero actuator.  A bound,
        active, incomplete, replaced, or already-cleared marker is never adopted.
        """

        context = retirement.get("context")
        context = {} if context is None else context
        if not isinstance(context, Mapping):
            return None
        raw_metadata = thread.get("metadata")
        raw_metadata = {} if raw_metadata is None else raw_metadata
        if isinstance(raw_metadata, str):
            try:
                raw_metadata = json.loads(raw_metadata)
            except (TypeError, ValueError):
                return None
        if not isinstance(raw_metadata, Mapping):
            return None
        captured_agent = context.get("agent")
        captured_pod = context.get("agent_pod")
        current_pod = raw_metadata.get("agent_pod")
        if not (
            str(thread.get("runtime_generation") or "")
            == str(retirement.get("generation") or "")
            and str(thread.get("runtime_retirement_token") or "")
            == str(retirement.get("token") or "")
            and str(thread.get("status") or "") == "created"
            and str(context.get("entry_status") or "") == "created"
            and context.get("runtime_authority_exposed") is True
            and context.get("agent_id") is None
            and context.get("control_admission_agent_id") is None
            and context.get("runtime_attach_token") is None
            and (
                captured_agent is None
                or isinstance(captured_agent, Mapping)
                and not captured_agent
            )
            and isinstance(captured_pod, Mapping)
            and captured_pod
            and current_pod == captured_pod
            and thread.get("agent_id") is None
            and thread.get("control_admission_agent_id") is None
            and thread.get("runtime_attach_token") is None
        ):
            return None
        pod_name = str(captured_pod.get("pod_name") or "")
        pod_uid = str(captured_pod.get("pod_uid") or "")
        return (pod_name, pod_uid) if pod_name and pod_uid else None

    async def _recover_pre_registration_agent_pod_zero(
        self, retirement: Mapping[str, Any], thread: Mapping[str, Any]
    ) -> bool:
        """Exact-stop and receipt one pre-registration claimant Pod."""

        pod = self._pre_registration_agent_pod_zero_candidate(retirement, thread)
        if pod is None:
            return False
        context = retirement.get("context") or {}
        thread_id = str(context.get("thread_id") or "")
        # Sanctioned registration takes the same advisory lifecycle lock as End;
        # this pre-effect check therefore cannot race its unbound-agent insert.
        # The post-effect receipt CAS repeats it against direct/legacy writers. A
        # registration that lost publication can leave one exact offline row
        # behind after its Pod is gone; retain that identity and delete it only
        # through the atomic offline+unbound predicate after exact Pod-zero.
        matching_agents = await self.dependencies.store.fetch(
            "SELECT id::text,hostname,pod_uid::text,status,thread_id::text,"
            "current_job_id::text FROM agents "
            "WHERE thread_id=$1::uuid OR hostname=$2 OR pod_uid=$3 ORDER BY id",
            thread_id,
            pod[0],
            pod[1],
        )
        if len(matching_agents) > 1:
            return False
        orphan_agent_id: str | None = None
        if matching_agents:
            matching = matching_agents[0]
            if not (
                str(matching.get("hostname") or "") == pod[0]
                and str(matching.get("pod_uid") or "") == pod[1]
                and str(matching.get("status") or "") in {"offline", "failed"}
                and matching.get("thread_id") is None
                and matching.get("current_job_id") is None
            ):
                return False
            orphan_agent_id = str(matching.get("id") or "")
            if not orphan_agent_id:
                return False
        try:
            await self._stop_captured_retirement_agent(retirement)
        except Exception:
            self.dependencies.logger.exception(
                "Pre-registration agent Pod stop remains retryable for thread %s",
                thread_id,
            )
            return False
        if (
            orphan_agent_id
            and not await self.dependencies.store.delete_exact_offline_unbound_agent(
                orphan_agent_id,
                expected_hostname=pod[0],
                expected_pod_uid=pod[1],
            )
        ):
            return False
        workspace = context.get("workspace_container")
        binding = context.get("workspace_binding")
        workspace = {} if workspace is None else workspace
        binding = {} if binding is None else binding
        if not isinstance(workspace, Mapping) or not isinstance(binding, Mapping):
            return False
        workspace_generation = str(binding.get("generation") or "")
        workspace_runtime = str(workspace.get(WORKSPACE_RUNTIME_INCARNATION_KEY) or "")
        physical_workspace_evidence = bool(
            binding
            or str(workspace.get("status") or "") not in {"", "deleted"}
            or any(
                workspace.get(field) is not None
                for field in (
                    WORKSPACE_RUNTIME_INCARNATION_KEY,
                    "pod_ip",
                    "pod_name",
                    "host",
                    "port",
                    "ide_host",
                    "ide_port",
                    "_canvas_workspace_generation",
                )
            )
        )
        if physical_workspace_evidence and not (
            workspace_generation and workspace_runtime
        ):
            return False
        if physical_workspace_evidence:
            if not (
                retirement.get("permanent")
                and context.get("workspace_backend") == "sandbox"
                and workspace.get("provisioner") == "k8s"
                and workspace.get("status")
                in {"ready", "suspending", "suspended", "deleted"}
                and binding.get("kind") == "remote"
                and str(binding.get("backing_id") or "").startswith(
                    ("k8s-pvc:", "k8s-pod:")
                )
                and str(workspace.get("_canvas_workspace_generation") or "")
                == workspace_generation
                and not context.get("workspace_provision_intent")
                and self.dependencies.container_provisioner.is_available
            ):
                return False
            workspace_authority = (
                await self.dependencies.container_provisioner.workspace_pod_authority(
                    WorkspaceOwner.session(thread_id),
                    expected_runtime_incarnation=workspace_runtime,
                )
            )
            if workspace_authority in {"exact_live", "exact_terminal"}:
                deleted = (
                    await self.dependencies.container_provisioner.delete_workspace(
                        WorkspaceOwner.session(thread_id),
                        expected_runtime_incarnation=workspace_runtime,
                        wait_for_exact_absence=True,
                        exact_absence_timeout_seconds=120.0,
                        defer_context_clear=True,
                    )
                )
                if not deleted:
                    return False
            elif workspace_authority != "exact_absent":
                return False
        receipt = await self.dependencies.store.acknowledge_pinned_thread_pre_registration_pod_zero(
            thread_id,
            expected_runtime_generation=str(retirement.get("generation") or ""),
            expected_retirement_token=str(retirement.get("token") or ""),
            expected_pod_name=pod[0],
            expected_pod_uid=pod[1],
            expected_workspace_generation=workspace_generation or None,
            expected_workspace_runtime_incarnation=workspace_runtime or None,
        )
        return receipt is not None

    def _agent_pod_provision_intent_zero_candidate(
        self, retirement: Mapping[str, Any], thread: Mapping[str, Any]
    ) -> dict[str, str] | None:
        """Return one captured pre-effect Pod attempt with no session owner."""

        context = retirement.get("context")
        context = {} if context is None else context
        if not isinstance(context, Mapping):
            return None
        intent = context.get("agent_pod_provision_intent")
        if not isinstance(intent, Mapping) or not intent:
            return None
        metadata = thread.get("metadata")
        metadata = {} if metadata is None else metadata
        if isinstance(metadata, str):
            try:
                metadata = json.loads(metadata)
            except (TypeError, ValueError):
                return None
        if not isinstance(metadata, Mapping):
            return None
        attempt_id = str(intent.get("attempt_id") or "")
        pod_name = str(intent.get("pod_name") or "")
        provisioner = str(intent.get("provisioner") or "")
        namespace = str(intent.get("namespace") or "")
        protection_protocol = str(intent.get("protection_protocol") or "")
        try:
            UUID(attempt_id)
        except (TypeError, ValueError):
            return None
        if not (
            provisioner in {"agent", "persistent"}
            and pod_name
            and namespace
            and protection_protocol == "finalizer_v1"
            and str(intent.get("status") or "") == "planned"
            and str(intent.get("thread_id") or "")
            == str(context.get("thread_id") or "")
            and str(intent.get("runtime_generation") or "")
            == str(retirement.get("generation") or "")
            and str(thread.get("runtime_generation") or "")
            == str(retirement.get("generation") or "")
            and str(thread.get("runtime_retirement_token") or "")
            == str(retirement.get("token") or "")
            and context.get("runtime_authority_exposed") is True
            and context.get("agent_id") is None
            and context.get("control_admission_agent_id") is None
            and context.get("runtime_attach_token") is None
            and context.get("agent") in (None, {})
            and context.get("agent_pod") in (None, {})
            and thread.get("agent_id") is None
            and thread.get("control_admission_agent_id") is None
            and thread.get("runtime_attach_token") is None
            and metadata.get("agent_pod") in (None, {})
        ):
            return None
        return {
            "attempt_id": attempt_id,
            "pod_name": pod_name,
            "provisioner": provisioner,
            "namespace": namespace,
            "protection_protocol": protection_protocol,
        }

    async def _recover_agent_pod_provision_intent_zero(
        self, retirement: Mapping[str, Any], thread: Mapping[str, Any]
    ) -> bool:
        """Resolve an accepted-or-absent create attempt before settlement."""

        intent = self._agent_pod_provision_intent_zero_candidate(retirement, thread)
        if intent is None:
            return False
        context = retirement.get("context") or {}
        thread_id = str(context.get("thread_id") or "")
        provider = (
            self.dependencies.persistent_provisioner
            if intent["provisioner"] == "persistent"
            else self.dependencies.agent_provisioner
        )
        if not provider.is_available:
            return False
        if await self.dependencies.store.fetchval(
            "SELECT EXISTS (SELECT 1 FROM agents WHERE thread_id=$1::uuid OR hostname=$2)",
            thread_id,
            intent["pod_name"],
        ):
            return False
        generation = str(retirement.get("generation") or "")
        token = str(retirement.get("token") or "")
        if not await self.dependencies.store.revoke_pinned_agent_pod_provision_intent(
            thread_id,
            expected_runtime_generation=generation,
            expected_retirement_token=token,
            expected_attempt_id=intent["attempt_id"],
            expected_pod_name=intent["pod_name"],
        ):
            return False

        intent_row = await self.dependencies.store.fetchrow(
            "SELECT status,pod_uid FROM thread_agent_pod_provision_intents "
            "WHERE attempt_id=$1::uuid AND thread_id=$2::uuid",
            intent["attempt_id"],
            thread_id,
        )
        if intent_row is None:
            return False
        intent_status = str(intent_row["status"] or "")
        fence_uid = str(intent_row["pod_uid"] or "")
        if intent_status == "fenced":
            observed = await provider.agent_pod_provision_intent_authority(
                intent["pod_name"],
                expected_thread_id=thread_id,
                expected_runtime_generation=generation,
                expected_attempt_id=intent["attempt_id"],
                namespace=intent["namespace"],
            )
            if not (
                isinstance(observed, Mapping)
                and str(observed.get("state") or "") == "exact_fence"
                and str(observed.get("pod_uid") or "") == fence_uid
            ):
                return False
        elif intent_status == "revoking":
            # A same-name CREATE is the causal barrier. A GET 404 alone could run
            # before the original credential-bearing request commits. If an
            # original manifest already owns the name, delete that exact UID and
            # retry until the *secret-free* fence itself wins. The retained fence
            # stays live through settlement/deletion so no delayed create CAS can
            # observe the key absent.
            deadline = asyncio.get_running_loop().time() + 60.0
            while True:
                fenced = await provider.fence_agent_pod_provision_intent(
                    intent["pod_name"],
                    expected_thread_id=thread_id,
                    expected_runtime_generation=generation,
                    expected_attempt_id=intent["attempt_id"],
                    namespace=intent["namespace"],
                )
                if not isinstance(fenced, Mapping):
                    return False
                fence_state = str(fenced.get("state") or "")
                candidate_uid = str(fenced.get("pod_uid") or "")
                if fence_state == "exact_fence":
                    fence_uid = candidate_uid
                    break
                if fence_state != "exact_original" or not candidate_uid:
                    return False
                deleted = (
                    await self.dependencies.persistent_provisioner.delete_agent_pod_exact(
                        thread_id,
                        expected_pod_uid=candidate_uid,
                        namespace=intent["namespace"],
                    )
                    if intent["provisioner"] == "persistent"
                    else await self.dependencies.agent_provisioner.delete_agent_pod_exact(
                        intent["pod_name"],
                        expected_pod_uid=candidate_uid,
                        namespace=intent["namespace"],
                    )
                )
                if not deleted:
                    return False
                released = (
                    await self.dependencies.persistent_provisioner.release_agent_pod_finalizer_exact(
                        thread_id,
                        expected_pod_uid=candidate_uid,
                        namespace=intent["namespace"],
                        terminal_required=True,
                    )
                    if intent["provisioner"] == "persistent"
                    else await self.dependencies.agent_provisioner.release_agent_pod_finalizer_exact(
                        intent["pod_name"],
                        expected_pod_uid=candidate_uid,
                        namespace=intent["namespace"],
                        terminal_required=True,
                    )
                )
                if not released:
                    return False
                while True:
                    authority = await provider.agent_pod_authority(
                        intent["pod_name"],
                        expected_pod_uid=candidate_uid,
                        namespace=intent["namespace"],
                    )
                    if authority == "exact_absent":
                        break
                    if authority == "replacement":
                        # The next loop classifies whether another delayed exact
                        # create or a foreign successor won the name.
                        break
                    if asyncio.get_running_loop().time() >= deadline:
                        return False
                    await asyncio.sleep(0.2)
                if asyncio.get_running_loop().time() >= deadline:
                    return False
            if (
                not fence_uid
                or not await self.dependencies.store.fence_pinned_agent_pod_provision_intent(
                    thread_id,
                    expected_runtime_generation=generation,
                    expected_retirement_token=token,
                    expected_attempt_id=intent["attempt_id"],
                    expected_pod_name=intent["pod_name"],
                    fence_pod_uid=fence_uid,
                )
            ):
                return False
            intent_status = "fenced"
        if intent_status != "fenced" or not fence_uid:
            return False

        receipt = await self.dependencies.store.acknowledge_pinned_agent_pod_provision_intent_zero(
            thread_id,
            expected_runtime_generation=generation,
            expected_retirement_token=token,
            expected_attempt_id=intent["attempt_id"],
            expected_pod_name=intent["pod_name"],
            observed_pod_uid=fence_uid,
        )
        return receipt is not None

    def _captured_agent_workspace_claim(
        self,
        retirement: Mapping[str, Any],
    ) -> dict[str, str] | None:
        """Validate the immutable PVC claim frozen by retirement Begin."""

        context = retirement.get("context")
        context = {} if context is None else context
        if not isinstance(context, Mapping):
            return None
        raw = context.get("agent_workspace_claim")
        if raw in (None, {}):
            return None
        if not isinstance(raw, Mapping):
            raise RuntimeError("captured agent workspace claim is malformed")
        values = {
            "claim_id": str(raw.get("claim_id") or ""),
            "thread_id": str(raw.get("thread_id") or ""),
            "created_runtime_generation": str(
                raw.get("created_runtime_generation") or ""
            ),
            "create_attempt": str(raw.get("create_attempt") or ""),
            "provisioner": str(raw.get("provisioner") or ""),
            "pvc_name": str(raw.get("pvc_name") or ""),
            "status": str(raw.get("status") or ""),
            "pvc_uid": str(raw.get("pvc_uid") or ""),
            "namespace": str(raw.get("namespace") or ""),
            "protection_protocol": str(raw.get("protection_protocol") or ""),
        }
        try:
            UUID(values["claim_id"])
            UUID(values["thread_id"])
            UUID(values["created_runtime_generation"])
            UUID(values["create_attempt"])
        except (TypeError, ValueError):
            raise RuntimeError(
                "captured agent workspace claim UUID is malformed"
            ) from None
        if not (
            values["thread_id"] == str(context.get("thread_id") or "")
            and values["provisioner"] in {"agent", "persistent"}
            and values["pvc_name"]
            and values["namespace"]
            and values["protection_protocol"] == "finalizer_v1"
            and values["status"] in {"planned", "ready"}
            and (values["status"] == "ready") == bool(values["pvc_uid"])
        ):
            raise RuntimeError("captured agent workspace claim authority is incomplete")
        return values

    async def _reconcile_agent_workspace_claim_for_retirement(
        self,
        retirement: Mapping[str, Any],
    ) -> None:
        """Retain a soft PVC or causally fence a permanently retired PVC name."""

        claim = self._captured_agent_workspace_claim(retirement)
        if claim is None:
            return
        provider = (
            self.dependencies.persistent_provisioner
            if claim["provisioner"] == "persistent"
            else self.dependencies.agent_provisioner
        )
        if not provider.is_available:
            raise RuntimeError("captured agent workspace provisioner is unavailable")
        context = retirement.get("context") or {}
        thread_id = str(context.get("thread_id") or "")
        generation = str(retirement.get("generation") or "")
        token = str(retirement.get("token") or "")
        if not bool(retirement.get("permanent")):
            retained_uid = await provider.ensure_agent_workspace_claim(
                claim["pvc_name"],
                expected_thread_id=thread_id,
                expected_runtime_generation=claim["created_runtime_generation"],
                expected_claim_id=claim["claim_id"],
                expected_create_attempt=claim["create_attempt"],
                namespace=claim["namespace"],
                expected_pvc_uid=claim["pvc_uid"] or None,
            )
            if (
                not retained_uid
                or not await self.dependencies.store.publish_pinned_agent_workspace_claim(
                    thread_id,
                    expected_runtime_generation=generation,
                    expected_retirement_token=token,
                    claim_id=claim["claim_id"],
                    pvc_name=claim["pvc_name"],
                    pvc_uid=retained_uid,
                    namespace=claim["namespace"],
                )
            ):
                raise RuntimeError("exact agent workspace retention is retryable")
            return

        from orchestrator.services.historical_agent_pod_cleanup import (
            retire_historical_claimant_pods,
        )

        async def assert_current_claim_retirement() -> None:
            current = await self.dependencies.store.get_thread(thread_id)
            if not (
                current
                and str(current.get("runtime_generation") or "") == generation
                and str(current.get("runtime_retirement_token") or "") == token
                and current.get("runtime_retirement_permanent") is True
                and current.get("runtime_retirement_authorized_at") is not None
            ):
                raise RuntimeError("historical claimant retirement authority changed")
            if not (
                not self._retirement_context_runtime_exposed(retirement)
                or self._retirement_has_exact_local_quiescence(retirement, current)
                or await self.dependencies.store.pinned_thread_has_prior_soft_settlement(
                    thread_id,
                    runtime_generation=generation,
                    retirement_token=token,
                )
            ):
                raise RuntimeError(
                    "historical claimant cleanup requires local quiescence"
                )

        await retire_historical_claimant_pods(
            self.dependencies.store,
            claim=claim,
            current_pod=context.get("agent_pod") or {},
            assert_current=assert_current_claim_retirement,
            agent_provisioner=self.dependencies.agent_provisioner,
        )

        if not await self.dependencies.store.revoke_pinned_agent_workspace_claim(
            thread_id,
            expected_runtime_generation=generation,
            expected_retirement_token=token,
            expected_claim_id=claim["claim_id"],
            expected_pvc_name=claim["pvc_name"],
        ):
            raise RuntimeError("agent workspace claim revocation is retryable")
        claim_row = await self.dependencies.store.fetchrow(
            "SELECT status,pvc_uid FROM thread_agent_workspace_claims "
            "WHERE claim_id=$1::uuid AND thread_id=$2::uuid",
            claim["claim_id"],
            thread_id,
        )
        if claim_row is None:
            raise RuntimeError("agent workspace claim disappeared")
        claim_status = str(claim_row["status"] or "")
        fence_uid = str(claim_row["pvc_uid"] or "")
        if claim_status == "reclaimed":
            # The post-horizon GC reconciler already deleted the exact fence UID
            # and durably closed this immutable claim. This can happen when an
            # earlier retirement attempt finished name fencing but failed in a
            # later obligation. The row-locked revoke call above revalidated the
            # captured claim tuple, so terminal replay has nothing left to actuate.
            return
        if claim_status == "fenced":
            observed = await provider.agent_workspace_claim_authority(
                claim["pvc_name"],
                expected_thread_id=thread_id,
                expected_runtime_generation=claim["created_runtime_generation"],
                expected_claim_id=claim["claim_id"],
                expected_create_attempt=claim["create_attempt"],
                namespace=claim["namespace"],
                expected_pvc_uid=fence_uid,
            )
            if not (
                isinstance(observed, Mapping)
                and str(observed.get("state") or "") == "exact_fence"
                and str(observed.get("pvc_uid") or "") == fence_uid
            ):
                raise RuntimeError("agent workspace fence reattestation failed")
            return
        if claim_status != "revoking":
            raise RuntimeError("agent workspace claim transition is malformed")

        deadline = asyncio.get_running_loop().time() + 120.0
        while True:
            fenced = await provider.fence_agent_workspace_claim(
                claim["pvc_name"],
                expected_thread_id=thread_id,
                expected_runtime_generation=claim["created_runtime_generation"],
                expected_claim_id=claim["claim_id"],
                expected_create_attempt=claim["create_attempt"],
                namespace=claim["namespace"],
            )
            if not isinstance(fenced, Mapping):
                raise RuntimeError("agent workspace fence observation is malformed")
            state = str(fenced.get("state") or "")
            candidate_uid = str(fenced.get("pvc_uid") or "")
            if state == "exact_fence" and candidate_uid:
                fence_uid = candidate_uid
                break
            if state != "exact_original" or not candidate_uid:
                raise RuntimeError("agent workspace name has foreign authority")
            if not await provider.delete_agent_workspace_claim_exact(
                claim["pvc_name"],
                expected_pvc_uid=candidate_uid,
                namespace=claim["namespace"],
            ):
                raise RuntimeError("exact agent workspace deletion is retryable")
            if not await provider.release_agent_workspace_claim_finalizer_exact(
                claim["pvc_name"],
                expected_pvc_uid=candidate_uid,
                namespace=claim["namespace"],
            ):
                raise RuntimeError("agent workspace finalizer release is retryable")
            if asyncio.get_running_loop().time() >= deadline:
                raise RuntimeError("agent workspace fence acquisition timed out")
            await asyncio.sleep(0.25)
        if not await self.dependencies.store.fence_pinned_agent_workspace_claim(
            thread_id,
            expected_runtime_generation=generation,
            expected_retirement_token=token,
            expected_claim_id=claim["claim_id"],
            expected_pvc_name=claim["pvc_name"],
            fence_pvc_uid=fence_uid,
        ):
            raise RuntimeError("agent workspace fence publication is retryable")

    def _captured_workspace_provision_intent(
        self,
        retirement: Mapping[str, Any],
    ) -> dict[str, Any] | None:
        """Validate the immutable multi-resource K8s create attempt from Begin."""

        context = retirement.get("context")
        context = {} if context is None else context
        if not isinstance(context, Mapping):
            raise RuntimeError("captured retirement context is malformed")
        raw = context.get("workspace_provision_intent")
        if raw in (None, {}):
            return None
        if not isinstance(raw, Mapping):
            raise RuntimeError("captured workspace provision intent is malformed")
        intent = dict(raw)
        try:
            attempt_id = str(UUID(str(intent.get("attempt_id") or "")))
            thread_id = str(UUID(str(intent.get("thread_id") or "")))
            generation = str(UUID(str(intent.get("runtime_generation") or "")))
        except (TypeError, ValueError):
            raise RuntimeError(
                "captured workspace provision intent UUID is malformed"
            ) from None
        if not (
            attempt_id == str(intent.get("attempt_id") or "")
            and thread_id == str(context.get("thread_id") or "")
            and generation == str(retirement.get("generation") or "")
            and str(intent.get("namespace") or "")
            and str(intent.get("pod_name") or "")
            and str(intent.get("network_tier") or "")
            and re.fullmatch(
                r"[0-9a-f]{64}", str(intent.get("manifest_fingerprint") or "")
            )
            and intent.get("status") in {"planned", "fenced", "retired"}
            and isinstance(intent.get("previous_binding"), Mapping)
            and (intent.get("created_agent_id") is None)
            == (intent.get("created_attach_token") is None)
            and (intent.get("pvc_name") is None) == (intent.get("service_name") is None)
        ):
            raise RuntimeError("captured workspace provision intent is incomplete")
        return intent

    async def _reconcile_workspace_provision_intent_for_retirement(
        self,
        retirement: Mapping[str, Any],
    ) -> bool:
        """Causally fence every name submitted by one pinned workspace create."""

        captured = self._captured_workspace_provision_intent(retirement)
        if captured is None:
            return False
        if not self.dependencies.container_provisioner.is_available:
            raise RuntimeError("captured workspace provisioner is unavailable")
        context = retirement.get("context") or {}
        thread_id = str(context.get("thread_id") or "")
        generation = str(retirement.get("generation") or "")
        token = str(retirement.get("token") or "")
        permanent = bool(retirement.get("permanent"))
        current = await self.dependencies.store.revoke_pinned_thread_workspace_provision_intent(
            thread_id,
            expected_runtime_generation=generation,
            expected_retirement_token=token,
            expected_attempt_id=str(captured.get("attempt_id") or ""),
        )
        if not isinstance(current, Mapping):
            raise RuntimeError("workspace provision intent revocation is retryable")
        fences = await self.dependencies.container_provisioner.fence_pinned_workspace_provision_intent(
            current,
            permanent=permanent,
        )
        if not isinstance(fences, Mapping):
            raise RuntimeError("workspace provision name fencing is retryable")

        previous_binding = captured.get("previous_binding") or {}
        if permanent and isinstance(previous_binding, Mapping) and previous_binding:
            if previous_binding.get("kind") == "virtual":
                from orchestrator.services.thread_uploads import (
                    purge_attested_pinned_virtual_workspace,
                )

                current_thread = await self.dependencies.store.get_thread(thread_id)
                if (
                    current_thread is None
                    or not await purge_attested_pinned_virtual_workspace(
                        current_thread,
                        expected_runtime_generation=generation,
                        expected_retirement_token=token,
                    )
                ):
                    raise RuntimeError(
                        "exact prior virtual backing cleanup is retryable"
                    )

        if not await self.dependencies.store.fence_pinned_thread_workspace_provision_intent(
            thread_id,
            expected_runtime_generation=generation,
            expected_retirement_token=token,
            expected_attempt_id=str(captured.get("attempt_id") or ""),
            fence_pod_uid=str(fences.get("fence_pod_uid") or ""),
            fence_pvc_uid=str(fences.get("fence_pvc_uid") or "") or None,
            fence_configmap_uid=(str(fences.get("fence_configmap_uid") or "") or None),
            fence_service_uid=(str(fences.get("fence_service_uid") or "") or None),
            permanent=permanent,
        ):
            raise RuntimeError("workspace provision fence publication is retryable")
        return True

    def _captured_virtual_binding_agent_zero_only(
        self,
        context: Mapping[str, Any],
        workspace: Mapping[str, Any],
        binding: Mapping[str, Any],
    ) -> bool:
        """Return whether the captured virtual backing owns no process runtime."""

        if str(context.get("workspace_backend") or "") != "virtual":
            return False
        raw_generation = str(binding.get("generation") or "")
        try:
            canonical_generation = str(UUID(raw_generation))
        except (TypeError, ValueError):
            return False
        return bool(
            not workspace
            and context.get("vm") in (None, {})
            and context.get("workspace_provision_intent") in (None, {})
            and set(binding)
            == {
                "generation",
                "kind",
                "backing_id",
                "ssh_host_key_fingerprint",
            }
            and binding.get("kind") == "virtual"
            and re.fullmatch(
                r"rclone:[0-9a-f]{64}",
                str(binding.get("backing_id") or ""),
            )
            is not None
            and canonical_generation == raw_generation
            and binding.get("ssh_host_key_fingerprint") is None
        )

    def _captured_lite_backend_agent_zero_only(
        self,
        context: Mapping[str, Any],
        workspace: Mapping[str, Any],
        binding: Mapping[str, Any],
    ) -> bool:
        """Return whether the captured lite tier owns nothing but its agent Pod.

        Officers and their conferences run here: ``workspace.backend`` is
        ``none``, so there is no workspace container, binding, VM, or provision
        intent — the agent runtime is the only process, and its exact Pod stop is
        the whole zero proof (``agent_runtime_zero_v1``, which the receipt trigger
        has accepted for this backend all along).
        """

        return bool(
            str(context.get("workspace_backend") or "") == "none"
            and not workspace
            and not binding
            and context.get("vm") in (None, {})
            and context.get("workspace_provision_intent") in (None, {})
        )

    def _captured_vm_recovery_identity(
        self,
        context: Mapping[str, Any],
        *,
        permanent: bool,
    ):
        """Return the exact captured VM identity usable by crash recovery.

        VM teardown is an orchestrator actuator, not an inference from an ended
        thread.  Require the same reciprocal generation/UID shape captured at
        retirement Begin before stopping the agent Pod or issuing an external
        effect.
        """
        from orchestrator.services.vm_provisioner import VMTeardownIdentity

        if str(context.get("workspace_backend") or "") not in {"vm", "remote"}:
            return None
        vm = context.get("vm")
        workspace = context.get("workspace_container")
        binding = context.get("workspace_binding")
        provision_intent = context.get("workspace_provision_intent")
        if (
            not isinstance(vm, Mapping)
            or workspace not in (None, {})
            or binding not in (None, {})
            or provision_intent not in (None, {})
        ):
            return None
        generation = str(vm.get("provision_generation") or "")
        identity_generation = str(vm.get("identity_provision_generation") or "")
        vm_uid = str(vm.get("vm_uid") or "")
        runtime_incarnation = str(vm.get("_runtime_incarnation") or "")
        rootdisk_uid = str(vm.get("rootdisk_pvc_uid") or "")
        try:
            UUID(generation)
        except (TypeError, ValueError):
            return None
        if (
            identity_generation != generation
            or vm.get("identity_authenticated") is not True
            or not vm_uid
            or runtime_incarnation != vm_uid
            or (permanent and not rootdisk_uid)
        ):
            return None
        return VMTeardownIdentity(
            provision_generation=generation,
            vm_uid=vm_uid,
            rootdisk_pvc_uid=rootdisk_uid or None,
            ssh_host=vm.get("ssh_host"),
            ssh_port=vm.get("ssh_port"),
            ssh_host_key_fingerprint=vm.get("ssh_host_key_fingerprint"),
            credential_runtime_started=vm.get("credential_runtime_started"),
        )

    async def _recover_captured_sandbox_process_zero(
        self,
        retirement: Mapping[str, Any],
    ) -> bool:
        """Mint a crash-recovery zero proof for one exact runtime incarnation.

        The agent process is stopped and proven absent *before* the workspace is
        contacted, so cached SSH credentials cannot reconnect behind the zero
        proof.  The thread advisory lock is deliberately not held while waiting
        for the agent Pod: a graceful SIGTERM path may be concurrently finishing
        its own ACK/settlement through that lock.  Once the Pod is absent, the
        immutable retirement token serializes the exact remote UID-zero command
        with staging and settlement.
        """

        context = retirement.get("context")
        context = {} if context is None else context
        if not isinstance(context, Mapping):
            return False
        if not self._retirement_context_runtime_exposed(retirement):
            return True

        thread_id = str(context.get("thread_id") or "")
        permanent = bool(retirement.get("permanent"))
        current = await self.dependencies.store.get_thread(thread_id)
        if current and self._agent_pod_provision_intent_zero_candidate(
            retirement, current
        ):
            async with self.dependencies.store.try_thread_advisory_lock(
                thread_id
            ) as lock_owner:
                if not lock_owner:
                    return False
                current = await self.dependencies.store.get_thread(thread_id)
                return bool(
                    current
                    and await self._recover_agent_pod_provision_intent_zero(
                        retirement, current
                    )
                )

        captured_workspace = context.get("workspace_container")
        captured_binding = context.get("workspace_binding")
        captured_workspace = {} if captured_workspace is None else captured_workspace
        captured_binding = {} if captured_binding is None else captured_binding
        if not isinstance(captured_workspace, Mapping) or not isinstance(
            captured_binding, Mapping
        ):
            return False
        workspace_backend = str(context.get("workspace_backend") or "sandbox")
        virtual_binding_agent_zero_only = (
            self._captured_virtual_binding_agent_zero_only(
                context, captured_workspace, captured_binding
            )
        )
        lite_agent_zero_only = self._captured_lite_backend_agent_zero_only(
            context, captured_workspace, captured_binding
        )
        captured_vm_identity = self._captured_vm_recovery_identity(
            context, permanent=permanent
        )
        if (
            workspace_backend != "sandbox"
            and not virtual_binding_agent_zero_only
            and not lite_agent_zero_only
            and captured_vm_identity is None
        ):
            # Unknown backends and incomplete VM authority remain fail-closed.
            self.dependencies.logger.warning(
                "Pinned retirement recovery has no process-zero actuator or complete "
                "captured authority for backend %r on thread %s; the durable marker "
                "stays pending",
                workspace_backend,
                thread_id,
            )
            return False
        if (
            captured_vm_identity is not None
            and not self.dependencies.vm_provisioner.lifecycle_available
        ):
            self.dependencies.logger.warning(
                "Pinned retirement recovery has no process-zero actuator or complete "
                "captured authority for backend %r on thread %s; the durable marker "
                "stays pending",
                workspace_backend,
                thread_id,
            )
            return False
        captured_pods = self._captured_retirement_agent_pods(retirement)
        if not captured_pods or any(
            not name
            or not uid
            or not namespace
            or protection_protocol != "finalizer_v1"
            for name, uid, namespace, protection_protocol in captured_pods
        ):
            return False
        if current and self._pre_registration_agent_pod_zero_candidate(
            retirement, current
        ):
            # Unlike a registered session, this Pod has no caller that could need
            # the lifecycle lock to publish its own ACK. Serialize against its
            # still-in-flight registration before exact UID deletion.
            async with self.dependencies.store.try_thread_advisory_lock(
                thread_id
            ) as lock_owner:
                if not lock_owner:
                    return False
                current = await self.dependencies.store.get_thread(thread_id)
                return bool(
                    current
                    and await self._recover_pre_registration_agent_pod_zero(
                        retirement, current
                    )
                )

        captured_docker_lease = str(
            captured_workspace.get("_docker_workspace_lease_id") or ""
        )
        if captured_workspace.get("provisioner") == "docker":
            try:
                UUID(captured_docker_lease)
            except (TypeError, ValueError):
                return False
            # Allocation is fenced before the old agent disappears or the remote
            # strict-zero command runs. A delayed allocator can therefore never
            # hand this host/lease to another thread during crash recovery.
            if not await self.dependencies.docker_provisioner.fence_thread_workspace_lease(
                str(context.get("thread_id") or ""),
                expected_lease_id=captured_docker_lease,
            ):
                return False

        # Never hold the DB lifecycle mutex while waiting for a Pod whose graceful
        # shutdown may need that same mutex to append its own exact proof.
        try:
            await self._stop_captured_retirement_agent(retirement)
        except Exception:
            self.dependencies.logger.exception(
                "Pinned retirement agent stop remains retryable for thread %s",
                context.get("thread_id"),
            )
            return False

        generation = str(retirement.get("generation") or "")
        token = str(retirement.get("token") or "")
        if not thread_id or not generation or not token:
            return False

        async with self.dependencies.store.try_thread_advisory_lock(
            thread_id
        ) as lock_owner:
            if not lock_owner:
                return False
            current = await self.dependencies.store.get_thread(thread_id)
            if current is None:
                return True
            if str(current.get("runtime_retirement_token") or "") != token:
                # A graceful exact-agent settlement may have won while the Pod
                # stop was converging.  Never act on whichever runtime is current.
                return str(
                    current.get("runtime_generation") or ""
                ) != generation or str(current.get("status") or "") in {
                    "ended",
                    "suspended",
                }
            if str(current.get("runtime_generation") or "") != generation:
                return False
            if self._retirement_has_exact_local_quiescence(retirement, current):
                return True

            workspace = captured_workspace
            binding = captured_binding
            if not isinstance(workspace, Mapping) or not isinstance(binding, Mapping):
                return False
            if captured_vm_identity is not None:
                cleanup = await self._admit_vm_cleanup(
                    thread_id,
                    captured_vm_identity,
                    purge_disk=permanent,
                )
                if cleanup is None:
                    return False
                disposition = completed_cleanup_outcome(cleanup)
                if disposition is None:
                    vm_result = (
                        await self.dependencies.vm_provisioner.release_vm_captured(
                            thread_id,
                            captured_vm_identity,
                            ssh_host=captured_vm_identity.ssh_host,
                            ssh_port=captured_vm_identity.ssh_port,
                            purge_disk=permanent,
                            entity_type="thread",
                            capture_snapshot=False,
                            **vm_cleanup_kwargs(cleanup),
                        )
                    )
                    disposition = vm_result.disposition
                    await self._complete_vm_cleanup(cleanup, disposition)
                if disposition != "completed":
                    self.dependencies.logger.warning(
                        "Pinned retirement VM process-zero remains retryable for "
                        "thread %s: %s",
                        thread_id,
                        disposition,
                    )
                    return False
                receipt = await self.dependencies.store.acknowledge_pinned_thread_local_quiescence(
                    thread_id,
                    expected_runtime_generation=generation,
                    expected_retirement_token=token,
                    expected_agent_id=str(context.get("agent_id") or ""),
                    expected_attach_token=str(
                        context.get("runtime_attach_token") or ""
                    ),
                    expected_settle_status=str(context.get("settle_status") or ""),
                    expected_quiescence_protocol="workspace_actuator_zero_v1",
                    expected_workspace_generation=(
                        captured_vm_identity.provision_generation
                    ),
                    expected_workspace_runtime_incarnation=captured_vm_identity.vm_uid,
                    quiescence_actor="orchestrator",
                )
                return receipt is not None
            if not workspace and (not binding or virtual_binding_agent_zero_only):
                receipt = await self.dependencies.store.acknowledge_pinned_thread_local_quiescence(
                    thread_id,
                    expected_runtime_generation=generation,
                    expected_retirement_token=token,
                    expected_agent_id=str(context.get("agent_id") or ""),
                    expected_attach_token=str(
                        context.get("runtime_attach_token") or ""
                    ),
                    expected_settle_status=str(context.get("settle_status") or ""),
                    expected_quiescence_protocol="agent_runtime_zero_v1",
                    expected_workspace_generation=None,
                    expected_workspace_runtime_incarnation=None,
                    quiescence_actor="orchestrator",
                    expected_agent_pod_uid=next(iter(captured_pods))[1],
                    require_zero_admission=True,
                )
                if receipt is None and (
                    virtual_binding_agent_zero_only or lite_agent_zero_only
                ):
                    # A used life (inputs were admitted) cannot be zero-admission.
                    # Its settled-work receipt is the DB's separate contract.
                    receipt = await self.dependencies.store.acknowledge_settled_virtual_actor_exit(
                        thread_id,
                        runtime_generation=generation,
                        retirement_token=token,
                        agent_id=str(context.get("agent_id") or ""),
                        attach_token=str(context.get("runtime_attach_token") or ""),
                        stopped_pod_uid=next(iter(captured_pods))[1],
                    )
                if receipt is None:
                    # The exact Pod is already stopped; only the settled-work
                    # contract stands between this life and its receipt.
                    self.dependencies.logger.warning(
                        "Pinned retirement receipt refused for thread %s (backend %r): "
                        "the exact agent Pod is stopped, but the DB found unfinished "
                        "or unprovable work for this life",
                        thread_id,
                        workspace_backend,
                    )
                    return False
                return True
            workspace_generation = str(binding.get("generation") or "")
            runtime_incarnation = str(
                (
                    workspace.get("_docker_workspace_lease_id")
                    if workspace.get("provisioner") == "docker"
                    else workspace.get(WORKSPACE_RUNTIME_INCARNATION_KEY)
                )
                or ""
            )
            fingerprint = str(binding.get("ssh_host_key_fingerprint") or "")
            host = str(workspace.get("pod_ip") or workspace.get("host") or "")
            workspace_status = str(workspace.get("status") or "")
            raw_port = workspace.get("port", 30022)
            try:
                UUID(thread_id)
                UUID(generation)
                UUID(token)
                UUID(str(context.get("agent_id") or ""))
                UUID(str(context.get("runtime_attach_token") or ""))
                UUID(workspace_generation)
                UUID(runtime_incarnation)
                port = int(raw_port)
            except (TypeError, ValueError):
                return False
            if (
                not host
                or not 1 <= port <= 65535
                or not fingerprint.startswith("SHA256:")
                or binding.get("kind") != "remote"
                or str(workspace.get("_canvas_workspace_generation") or "")
                != workspace_generation
            ):
                return False

            # A permanent retirement token prevents a same-name successor from
            # being admitted.  After the exact captured agent Pod is stopped, a
            # Kubernetes 404 (or an exact UID whose containers are all terminal)
            # is therefore a stronger process-zero proof than SSH: there is no
            # workspace process namespace left to contact.  Receipt that actuator
            # proof before the ordinary retirement cleanup removes the residual
            # PVC/Service.  Replacement and ambiguous observations stay refused.
            if permanent and workspace_status in {
                "ready",
                "suspending",
                "suspended",
                "deleted",
            }:
                workspace_authority = await self.dependencies.container_provisioner.workspace_pod_authority(
                    WorkspaceOwner.session(thread_id),
                    expected_runtime_incarnation=runtime_incarnation,
                )
                if workspace_authority in {"exact_absent", "exact_terminal"}:
                    receipt = await self.dependencies.store.acknowledge_pinned_thread_local_quiescence(
                        thread_id,
                        expected_runtime_generation=generation,
                        expected_retirement_token=token,
                        expected_agent_id=str(context.get("agent_id") or ""),
                        expected_attach_token=str(
                            context.get("runtime_attach_token") or ""
                        ),
                        expected_settle_status=str(context.get("settle_status") or ""),
                        expected_quiescence_protocol="sandbox_actuator_zero_v1",
                        expected_workspace_generation=workspace_generation,
                        expected_workspace_runtime_incarnation=runtime_incarnation,
                        quiescence_actor="orchestrator",
                    )
                    return receipt is not None

            # SSH is a live-runtime actuator. A captured suspension/deletion state
            # may use exact Pod absence above, but may never dial its stale endpoint
            # when the Kubernetes observation is live, replaced, or ambiguous.
            if workspace_status != "ready":
                return False

            from shared.runtime.core.backends.remote import RemoteBackend

            key_path = self.dependencies.resolve_ssh_key_path()
            if not key_path:
                return False
            backend = RemoteBackend(
                host=host,
                port=port,
                username="agent-host",
                key_path=key_path,
                workspace_path="/home/agent-host/workspace",
                job_id=thread_id,
                workspace_generation=workspace_generation,
                runtime_incarnation=runtime_incarnation,
                expected_host_key_fingerprint=fingerprint,
                workspace_owner_kind="session",
                workspace_owner_id=thread_id,
                workspace_tier="sandbox",
                sudo_action="freeze",
            )
            try:
                protocol = await asyncio.to_thread(
                    backend.protected_workspace_zero_cleanup_strict
                )
            except Exception:
                self.dependencies.logger.exception(
                    "Pinned retirement workspace zero remains retryable for thread %s",
                    thread_id,
                )
                return False
            finally:
                await asyncio.to_thread(backend.disconnect)
            if protocol != "workspace_process_zero_v1":
                return False
            current = await self.dependencies.store.get_thread(thread_id)
            if not current or not await self._pinned_retirement_is_current(retirement):
                return False
            receipt = await self.dependencies.store.acknowledge_pinned_thread_local_quiescence(
                thread_id,
                expected_runtime_generation=generation,
                expected_retirement_token=token,
                expected_agent_id=str(context.get("agent_id") or ""),
                expected_attach_token=str(context.get("runtime_attach_token") or ""),
                expected_settle_status=str(context.get("settle_status") or ""),
                expected_quiescence_protocol=protocol,
                expected_workspace_generation=workspace_generation,
                expected_workspace_runtime_incarnation=runtime_incarnation,
                quiescence_actor="orchestrator",
            )
            return receipt is not None

    async def _complete_retiring_soft_warm_binding_release(
        self,
        retirement: Mapping[str, Any],
    ) -> bool:
        """Finish the durable finalizer release started by soft settlement."""

        if bool(retirement.get("permanent")):
            return True
        context = retirement.get("context")
        context = {} if context is None else context
        marker = context.get("agent_pod") if isinstance(context, Mapping) else None
        protection_id = (
            str(marker.get("warm_binding_protection") or "")
            if isinstance(marker, Mapping)
            else ""
        )
        if not protection_id:
            return True
        return await release_pinned_warm_binding_protection(
            self.dependencies.store,
            protection_id=protection_id,
            agent_provisioner=self.dependencies.agent_provisioner,
            persistent_provisioner=self.dependencies.persistent_provisioner,
        )

    async def _cleanup_pinned_thread_retirement(
        self,
        retirement: Mapping[str, Any],
        *,
        cleanup_agent_pod: bool = True,
        stop_agent_before_workspace: bool = False,
        defer_agent_workspace_claim_until_caller_exit: bool = False,
    ) -> None:
        """Actuate only identities captured by ``begin_pinned_thread_retirement``.

        Every ambiguous failure raises and deliberately leaves the durable token
        pending. Resume and all runtime/credential admission stay closed until an
        exact retry completes. Deterministic names are lookup keys only; every K8s
        deletion carries a captured UID and soft End never deletes a PVC.
        """

        context = retirement.get("context")
        context = {} if context is None else context
        if not isinstance(context, Mapping):
            raise RuntimeError("pinned retirement context is malformed")
        thread_id = str(context.get("thread_id") or "")
        generation = str(retirement.get("generation") or "")
        if not thread_id or str(context.get("generation") or "") != generation:
            raise RuntimeError("pinned retirement context authority is malformed")
        permanent = bool(retirement.get("permanent"))

        # Wait out/prohibit every protected-engage producer before touching its
        # stable remote grant key. The scheduler uses this same cross-replica lock.
        if not await self._pinned_retirement_is_current(retirement):
            raise RuntimeError("pinned retirement authority changed")
        if (
            permanent
            and await self.dependencies.store.pinned_retirement_external_cleanup_complete(
                thread_id,
                runtime_generation=generation,
                retirement_token=str(retirement.get("token") or ""),
            )
        ):
            # The prior attempt crossed the append-once external-cleanup CAS only
            # after route teardown, exact resource actuators, reader revocation and
            # any required captured-agent stop.  A lost response/restart therefore
            # replays from this receipt without probing deterministic resource names
            # or adopting a successor endpoint.
            return

        agent = context.get("agent")
        agent = {} if agent is None else agent
        if not isinstance(agent, Mapping):
            raise RuntimeError("captured agent identity is malformed")
        agent_pod = context.get("agent_pod")
        agent_pod = {} if agent_pod is None else agent_pod
        if not isinstance(agent_pod, Mapping):
            raise RuntimeError("captured agent Pod identity is malformed")

        # Never address a captured Pod by IP here. Pod IPs are reusable, and an
        # old cleanup cannot prove that the process currently listening there is
        # the captured Pod/runtime. Agent-initiated End already performs its own
        # graceful memory/git flush; owner/offline retirement deliberately relies
        # on the exact UID deletion below instead of risking a successor detach.

        # Remove only the route owned by this Pod UID + runtime generation.
        route = context.get("route")
        route = {} if route is None else route
        if not isinstance(route, Mapping):
            raise RuntimeError("captured route identity is malformed")
        route_owner_uid = str((route or {}).get("owner_pod_uid") or "")
        if route_owner_uid:
            route_namespace = str(
                (route or {}).get("namespace")
                or (agent_pod or {}).get("namespace")
                or ""
            ).strip()
            if not route_namespace:
                raise RuntimeError("captured route namespace authority is malformed")
            if not await self.dependencies.session_router.teardown_route(
                thread_id,
                expected_namespace=route_namespace,
                expected_runtime_generation=generation,
                expected_owner_uid=route_owner_uid,
            ):
                raise RuntimeError("exact session route cleanup is retryable")

        if stop_agent_before_workspace:
            if not cleanup_agent_pod or permanent:
                raise RuntimeError("idle Pod stop authority is malformed")
            await self._stop_captured_retirement_agent(retirement)

        workspace_provision_intent_zero = (
            await self._reconcile_workspace_provision_intent_for_retirement(retirement)
        )

        # Workspace teardown is separately identity-fenced. PVC deletion occurs
        # only for permanent End; soft End preserves the backing for Resume.
        ws = context.get("workspace_container")
        ws = {} if ws is None else ws
        if not isinstance(ws, Mapping):
            raise RuntimeError("captured workspace identity is malformed")
        backend = str(context.get("workspace_backend") or "sandbox")
        vm = context.get("vm")
        vm = {} if vm is None else vm
        if not isinstance(vm, Mapping):
            raise RuntimeError("captured VM identity is malformed")
        vm_identity_present = any(
            self._retirement_json_field_is_nonnull(vm, field)
            for field in (
                "provision_generation",
                "identity_provision_generation",
                "vm_uid",
                "_runtime_incarnation",
                "rootdisk_pvc_uid",
                "ssh_host",
                "ssh_port",
                "_canvas_workspace_generation",
            )
        ) or not self._retirement_json_status_is_absent(vm)
        binding = context.get("workspace_binding")
        binding = {} if binding is None else binding
        if not isinstance(binding, Mapping):
            raise RuntimeError("captured workspace binding is malformed")
        retained_soft_workspace = context.get("retained_soft_workspace")
        retained_soft_workspace = (
            {} if retained_soft_workspace is None else retained_soft_workspace
        )
        if not isinstance(retained_soft_workspace, Mapping):
            raise RuntimeError("captured retained workspace authority is malformed")
        virtual_binding_present = bool(binding and binding.get("kind") == "virtual")
        sandbox_identity_present = bool(
            self._retirement_json_field_is_nonnull(
                ws, WORKSPACE_RUNTIME_INCARNATION_KEY
            )
            or (binding and not virtual_binding_present)
            or not self._retirement_json_status_is_absent(ws)
            or any(
                self._retirement_json_field_is_nonnull(ws, field)
                for field in (
                    "pod_ip",
                    "pod_name",
                    "host",
                    "port",
                    "ide_host",
                    "ide_port",
                    "_canvas_workspace_generation",
                )
            )
        )
        if backend not in {"sandbox", "virtual", "none", "vm", "remote"}:
            raise RuntimeError("captured workspace backend is unsupported")
        if workspace_provision_intent_zero and vm_identity_present:
            raise RuntimeError("workspace create intent also contains VM authority")
        if (
            not workspace_provision_intent_zero
            and backend in {"vm", "remote"}
            and sandbox_identity_present
        ):
            raise RuntimeError("captured VM retirement also contains sandbox authority")
        if (
            not workspace_provision_intent_zero
            and backend == "sandbox"
            and vm_identity_present
        ):
            raise RuntimeError("captured sandbox retirement also contains VM authority")
        if (
            not workspace_provision_intent_zero
            and backend == "virtual"
            and (
                sandbox_identity_present
                or vm_identity_present
                or (binding and not virtual_binding_present)
            )
        ):
            raise RuntimeError(
                "captured virtual retirement contains physical authority"
            )
        if (
            not workspace_provision_intent_zero
            and backend == "none"
            and (sandbox_identity_present or vm_identity_present or binding)
        ):
            raise RuntimeError("captured lite retirement contains physical authority")
        completed_quiescence_protocol: str | None = None
        completed_external_cleanup_protocol: str | None = None
        if workspace_provision_intent_zero:
            completed_external_cleanup_protocol = "workspace_provision_fence_v1"
        elif backend in {"vm", "remote"} and vm_identity_present:
            from orchestrator.services.vm_provisioner import VMTeardownIdentity

            provision_generation = str(vm.get("provision_generation") or "")
            vm_uid = str(vm.get("vm_uid") or "")
            rootdisk_uid = str(vm.get("rootdisk_pvc_uid") or "")
            if (
                not self.dependencies.vm_provisioner.lifecycle_available
                or not provision_generation
                or not vm_uid
                or (permanent and not rootdisk_uid)
            ):
                raise RuntimeError("exact VM cleanup authority is incomplete")
            vm_identity = VMTeardownIdentity(
                provision_generation=provision_generation,
                vm_uid=vm_uid,
                rootdisk_pvc_uid=rootdisk_uid or None,
                ssh_host=vm.get("ssh_host"),
                ssh_port=vm.get("ssh_port"),
                ssh_host_key_fingerprint=vm.get("ssh_host_key_fingerprint"),
                credential_runtime_started=vm.get("credential_runtime_started"),
            )
            cleanup = await self._admit_vm_cleanup(
                thread_id,
                vm_identity,
                purge_disk=permanent,
            )
            if cleanup is None:
                raise RuntimeError("exact VM cleanup held for workspace recovery")
            disposition = completed_cleanup_outcome(cleanup)
            if disposition is None:
                vm_result = await self.dependencies.vm_provisioner.release_vm_captured(
                    thread_id,
                    vm_identity,
                    ssh_host=vm.get("ssh_host"),
                    ssh_port=vm.get("ssh_port"),
                    purge_disk=permanent,
                    entity_type="thread",
                    capture_snapshot=False,
                    **vm_cleanup_kwargs(cleanup),
                )
                disposition = vm_result.disposition
                await self._complete_vm_cleanup(cleanup, disposition)
            if disposition != "completed":
                raise RuntimeError("exact VM cleanup is retryable")
            completed_quiescence_protocol = "workspace_actuator_zero_v1"
            completed_external_cleanup_protocol = "workspace_actuator_zero_v1"
        elif backend in {"vm", "remote"}:
            pass
        elif backend == "sandbox" and ws.get("provisioner") == "docker":
            lease_id = str(ws.get("_docker_workspace_lease_id") or "")
            if (
                not lease_id
                or not await self.dependencies.docker_provisioner.release_thread_workspace(
                    thread_id,
                    expected_lease_id=lease_id,
                    force_quarantine=True,
                )
            ):
                raise RuntimeError("exact Docker workspace cleanup is retryable")
            # Quarantine/release fences allocation of this exact lease, but does
            # not prove its remote same-UID processes stopped. Exposed Docker lives
            # need a pre-existing host-key/lease-bound workspace_process_zero_v1
            # receipt from the live agent (or the dedicated recovery actuator).
            completed_external_cleanup_protocol = "sandbox_actuator_zero_v1"
        elif backend == "sandbox" and sandbox_identity_present:
            if not self.dependencies.container_provisioner.is_available:
                raise RuntimeError("Kubernetes workspace cleanup is unavailable")
            captured_runtime = str(ws.get(WORKSPACE_RUNTIME_INCARNATION_KEY) or "")
            workspace_identity = await self.dependencies.container_provisioner.capture_workspace_teardown_identity(
                WorkspaceOwner.session(thread_id),
                expected_runtime_incarnation=captured_runtime or None,
            )
            captured_generation = str(binding.get("generation") or "")
            captured_backing = str(binding.get("backing_id") or "")
            retained_pvc_uid = str(retained_soft_workspace.get("pvc_uid") or "")
            retained_authority = bool(retained_soft_workspace)
            if (
                not captured_generation
                or binding.get("kind") != "remote"
                or not (
                    captured_backing.startswith("k8s-pvc:")
                    or captured_backing.startswith("k8s-pod:")
                )
            ):
                raise RuntimeError("exact Kubernetes cleanup authority is incomplete")
            try:
                UUID(captured_generation)
                backing_resource_uid = str(UUID(captured_backing.rsplit(":", 1)[-1]))
                if captured_runtime:
                    UUID(captured_runtime)
                if retained_authority:
                    UUID(str(retained_soft_workspace.get("attempt_id") or ""))
                    UUID(retained_pvc_uid)
            except (TypeError, ValueError):
                raise RuntimeError(
                    "exact Kubernetes cleanup authority is malformed"
                ) from None
            if retained_authority:
                expected_owner = WorkspaceOwner.session(thread_id)
                expected_pvc_name = f"pvc-{expected_owner.pod_name}"
                if (
                    not permanent
                    or context.get("entry_status") != "ended"
                    or retained_soft_workspace.get("version") != 1
                    or str(retained_soft_workspace.get("runtime_generation") or "")
                    != generation
                    or str(retained_soft_workspace.get("workspace_generation") or "")
                    != captured_generation
                    or str(retained_soft_workspace.get("namespace") or "")
                    != str(ws.get("namespace") or "")
                    or str(retained_soft_workspace.get("pod_name") or "")
                    != expected_owner.pod_name
                    or str(ws.get("pod_name") or "") != expected_owner.pod_name
                    or str(retained_soft_workspace.get("pvc_name") or "")
                    != expected_pvc_name
                    or retained_pvc_uid != backing_resource_uid
                    or captured_backing
                    != (
                        f"k8s-pvc:{retained_soft_workspace.get('namespace')}:"
                        f"{retained_pvc_uid}"
                    )
                    or ws.get("status") != "deleted"
                    or ws.get("provisioner") != "k8s"
                    or ws.get(WORKSPACE_RUNTIME_INCARNATION_KEY) is not None
                    or ws.get("pod_ip") is not None
                    or ws.get("ide_host") is not None
                    or ws.get("ide_port") is not None
                    or str(ws.get("_canvas_workspace_generation") or "")
                    != captured_generation
                    or captured_runtime
                    or workspace_identity.pod_uid is not None
                    or workspace_identity.service_uid is not None
                    or workspace_identity.pvc_uid not in (None, retained_pvc_uid)
                ):
                    raise RuntimeError(
                        "retained Kubernetes workspace authority changed before deletion"
                    )
            elif not captured_runtime:
                raise RuntimeError("exact Kubernetes cleanup authority is incomplete")
            if (
                workspace_identity.pod_uid is not None
                and workspace_identity.pod_uid != captured_runtime
            ):
                raise RuntimeError("workspace Pod identity changed before retirement")
            if captured_backing.startswith("k8s-pod:"):
                if backing_resource_uid != captured_runtime:
                    raise RuntimeError(
                        "workspace Pod backing authority is inconsistent"
                    )
                if workspace_identity.pvc_uid is not None:
                    # ``k8s-pod`` is the immutable emptyDir shape. A PVC that
                    # appeared after Begin belongs to a replacement/provisioning
                    # attempt and must never be adopted by deterministic name.
                    raise RuntimeError(
                        "workspace PVC appeared outside captured retirement authority"
                    )
            elif (
                workspace_identity.pvc_uid is not None
                and workspace_identity.pvc_uid != backing_resource_uid
            ):
                # A present same-name PVC must still be the exact captured UID.
                # Absence is an idempotent replay after an earlier exact release
                # deleted the volume but a later retirement obligation remained
                # retryable; pinned release rechecks Pod absence and
                # never adopts a deterministic-name successor.
                raise RuntimeError("workspace PVC identity changed before retirement")
            released = await self.dependencies.container_provisioner.release_workspace(
                WorkspaceOwner.session(thread_id),
                reclaim_volume=permanent,
                capture_snapshot=True,
                strict=True,
                teardown_identity=workspace_identity,
                pinned_retirement=retirement,
            )
            if not released:
                raise RuntimeError("exact Kubernetes workspace cleanup is retryable")
            completed_quiescence_protocol = "sandbox_actuator_zero_v1"
            completed_external_cleanup_protocol = "sandbox_actuator_zero_v1"
        elif backend == "virtual" and virtual_binding_present and permanent:
            from orchestrator.services.thread_uploads import (
                purge_attested_pinned_virtual_workspace,
            )

            current = await self.dependencies.store.get_thread(thread_id)
            if current is None or not await purge_attested_pinned_virtual_workspace(
                current,
                expected_runtime_generation=generation,
                expected_retirement_token=str(retirement.get("token") or ""),
            ):
                raise RuntimeError("exact virtual backing cleanup is retryable")
            completed_external_cleanup_protocol = "virtual_backing_zero_v1"

        # Reader cleanup uses the captured row+attempt; never re-read a row by
        # thread and accidentally revoke a successor grant.
        protected_ro = context.get("protected_ro")
        protected_ro = {} if protected_ro is None else protected_ro
        if not isinstance(protected_ro, Mapping):
            raise RuntimeError("captured protected reader identity is malformed")
        if protected_ro.get("status") in {
            "engaging",
            "active",
            "revoking",
        }:
            row_id = str(protected_ro.get("id") or "")
            ro_generation = str(protected_ro.get("runtime_generation") or "")
            plan = ProtectedNextcloudReaderGrantPlan.from_ro_mount_row(protected_ro)
            if not row_id or not ro_generation or plan is None:
                raise RuntimeError("protected reader cleanup lacks exact identity")
            backend_client = await self.dependencies.resolve_protected_reader_backend(
                plan
            )
            if not await revoke_ro_mount_attempt(
                backend=backend_client,
                postgres_db=self.dependencies.store,
                row_id=row_id,
                thread_id=thread_id,
                runtime_generation=ro_generation,
                plan=plan,
            ):
                current_ro = await self.dependencies.store.get_ro_mount_by_thread(
                    thread_id
                )
                if not current_ro or str(current_ro.get("status") or "") in {
                    "engaging",
                    "active",
                    "revoking",
                }:
                    raise RuntimeError("protected reader row cleanup is retryable")

        # Agent Pods last: workspace snapshots/detach may still need the process.
        # The exact self-settlement route is itself awaiting this HTTP response;
        # synchronously deleting/waiting for that caller creates a lifecycle
        # cycle.  Its append-only local-quiescence receipt proves writers are
        # stopped, so settlement may answer first and the process exits itself.
        stopped_agent_pod_name: str | None = None
        stopped_agent_pod_uid: str | None = None
        if cleanup_agent_pod:
            captured_agent_pods = self._captured_retirement_agent_pods(retirement)
            if len(captured_agent_pods) > 1:
                raise RuntimeError("captured retirement agent identities disagree")
            if not stop_agent_before_workspace:
                await self._stop_captured_retirement_agent(retirement)
            if captured_agent_pods:
                (
                    stopped_agent_pod_name,
                    stopped_agent_pod_uid,
                    _stopped_agent_pod_namespace,
                    _stopped_agent_pod_protection_protocol,
                ) = next(iter(captured_agent_pods))
                if completed_quiescence_protocol is None:
                    completed_quiescence_protocol = "agent_runtime_zero_v1"

        if defer_agent_workspace_claim_until_caller_exit:
            # A permanent final ACK is sent by the same agent process whose Pod
            # mounts this claim. Its append-only local-quiescence receipt proves
            # the runtime stopped all writers, but Kubernetes cannot remove the
            # PVC until this HTTP response lets that caller exit. Leave the claim
            # completely untouched; an owner/reconciler retry exact-stops the
            # captured Pod first and then resumes the ordinary fenced cleanup.
            if (
                not permanent
                or cleanup_agent_pod
                or self._captured_agent_workspace_claim(retirement) is None
            ):
                raise RuntimeError("agent workspace cleanup handoff is malformed")
            return

        # A bootstrap Pod can have a distinct persistent PVC whose create was
        # already sent before registration. Soft settlement retains/re-attests the
        # exact UID. Permanent settlement deletes only exact claimants and leaves
        # a same-name inert PVC tombstone until the API-request horizon expires.
        # This runs after agent stop so a mounted claim is never torn down first.
        await self._reconcile_agent_workspace_claim_for_retirement(retirement)

        if permanent and not (
            await self.dependencies.store.clear_pinned_retirement_physical_runtime_endpoint(
                thread_id,
                runtime_generation=generation,
                retirement_token=str(retirement.get("token") or ""),
                completed_quiescence_protocol=completed_quiescence_protocol,
                completed_external_cleanup_protocol=(
                    completed_external_cleanup_protocol
                ),
                expected_stopped_agent_pod_name=stopped_agent_pod_name,
                expected_stopped_agent_pod_uid=stopped_agent_pod_uid,
            )
        ):
            raise RuntimeError(
                "permanent retirement physical endpoint cleanup CAS is retryable"
            )

        if not await self._pinned_retirement_is_current(retirement):
            raise RuntimeError("pinned retirement authority changed before settlement")

    pinned_retirement_is_current = _pinned_retirement_is_current
    retirement_context_runtime_exposed = _retirement_context_runtime_exposed
    never_delivered_protected_reader_shape = _never_delivered_protected_reader_shape
    revoke_never_delivered_protected_reader = _revoke_never_delivered_protected_reader
    retirement_has_exact_local_quiescence = _retirement_has_exact_local_quiescence
    wait_for_captured_agent_pod_retired = _wait_for_captured_agent_pod_retired
    captured_retirement_agent_pods = _captured_retirement_agent_pods
    stop_captured_retirement_agent = _stop_captured_retirement_agent
    pre_registration_agent_pod_zero_candidate = (
        _pre_registration_agent_pod_zero_candidate
    )
    recover_pre_registration_agent_pod_zero = _recover_pre_registration_agent_pod_zero
    agent_pod_provision_intent_zero_candidate = (
        _agent_pod_provision_intent_zero_candidate
    )
    recover_agent_pod_provision_intent_zero = _recover_agent_pod_provision_intent_zero
    captured_virtual_binding_agent_zero_only = _captured_virtual_binding_agent_zero_only
    captured_agent_workspace_claim = _captured_agent_workspace_claim
    reconcile_agent_workspace_claim_for_retirement = (
        _reconcile_agent_workspace_claim_for_retirement
    )
    recover_captured_process_zero = _recover_captured_sandbox_process_zero
    complete_retiring_soft_warm_binding_release = (
        _complete_retiring_soft_warm_binding_release
    )
    cleanup_pinned_thread_retirement = _cleanup_pinned_thread_retirement


__all__ = [
    "PinnedRetirementDependencies",
    "PinnedRetirementOperations",
]
