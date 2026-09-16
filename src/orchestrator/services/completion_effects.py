"""Completion effect-journal and workspace-teardown adapters."""

from __future__ import annotations

from collections.abc import Callable, Coroutine, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
import logging
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

from orchestrator.services.completion_effect_policy import COMPLETION_EFFECT_INDEX
from orchestrator.services.container_provisioner import WorkspaceTeardownIdentity
from orchestrator.services.workspace_lifecycle import WorkspaceOwner


@dataclass(frozen=True, slots=True)
class CompletionEffectDependencies:
    """Workspace authorities used only by the S36 external effect adapter."""

    store: Any
    container_provisioner: Any
    vm_provisioner: Any
    get_container_context: Callable[[Mapping[str, Any]], Mapping[str, Any]]
    get_vm_context: Callable[[Mapping[str, Any]], Mapping[str, Any]]
    archive_and_cleanup_workspace: Callable[[str], Coroutine[Any, Any, list[str]]]
    s36_exact_absence_timeout_seconds: Callable[[], float]
    logger: logging.Logger
    recovery_store: Any


def completion_effect_dedup_key(
    effect_runner: Any,
    effect_name: str,
    job_id: str,
) -> str:
    """Return the stable notification key for one journalled effect attempt."""

    command_id = (
        getattr(effect_runner, "command_id", None)
        if effect_runner is not None
        else None
    )
    if command_id:
        return f"{effect_name}:{command_id}"
    return f"{effect_name}:{job_id}:{uuid4()}"


async def run_completion_effect(
    effect_runner: Any | None,
    name: str,
    group: str,
    callback: Callable[[], Coroutine[Any, Any, Any]],
    *,
    retry_on_error: bool = False,
    error_output: Callable[[BaseException], Any] | None = None,
    retry_if: Callable[[Any], bool] | None = None,
    supersede_if: Callable[[Any], bool] | None = None,
    depends_on_groups: tuple[str, ...] = (),
    transactional: bool = False,
    effect_timeout_seconds: float | None = None,
    command_lease_seconds: float | None = None,
) -> Any:
    """Run a legacy callback directly or through the durable effect journal."""

    if effect_runner is None:
        return await callback()
    if (name, group) not in COMPLETION_EFFECT_INDEX:
        raise RuntimeError(f"unregistered completion effect {group}/{name}")
    run_effect = (
        getattr(effect_runner, "run_transactional", effect_runner.run)
        if transactional
        else effect_runner.run
    )
    return await run_effect(
        name=name,
        group=group,
        callback=callback,
        retry_on_error=retry_on_error,
        error_output=error_output,
        retry_if=retry_if,
        supersede_if=supersede_if,
        depends_on_groups=depends_on_groups,
        effect_timeout_seconds=effect_timeout_seconds,
        command_lease_seconds=command_lease_seconds,
    )


async def run_completion_workspace_teardown(
    job_id: str,
    effect_runner: Any | None,
    *,
    dependencies: CompletionEffectDependencies,
) -> dict[str, Any]:
    """Run S36 under its durable report-order/admission authorization.

    The authorization transaction cannot span external archive/delete I/O. It
    therefore installs a pending-effect marker under the jobs-row lock before
    any backend is touched. A higher report that acquired the same lock first
    makes this S36 a durable handoff with no external calls.

    Every command-backed backend uses this same journal and authorization.
    Kubernetes and authenticated KubeVirt resources retain immutable teardown
    identities (including both resources after a workspace-to-VM upgrade).
    Docker and the default-off route keep their historical cleanup call.
    """
    postgres_db = dependencies.store
    container_provisioner = dependencies.container_provisioner
    vm_provisioner = dependencies.vm_provisioner
    _get_container_context = dependencies.get_container_context
    _get_vm_context = dependencies.get_vm_context
    _archive_and_cleanup_workspace = dependencies.archive_and_cleanup_workspace
    _COMPLETION_S36_EXACT_ABSENCE_TIMEOUT_SECONDS = (
        dependencies.s36_exact_absence_timeout_seconds()
    )
    logger = dependencies.logger
    recovery_store = dependencies.recovery_store

    async def _archive_and_teardown_workspace() -> dict[str, Any]:
        cleanup_admission: Any | None = None
        try:
            cleanup_request_id = UUID(str(effect_runner.command_id))
        except (AttributeError, TypeError, ValueError):
            cleanup_request_id = uuid5(NAMESPACE_URL, f"completion-cleanup:{job_id}")

        async def _admit_destructive_cleanup(pvc_uid: Any) -> None:
            nonlocal cleanup_admission
            try:
                parsed_pvc_uid = UUID(str(pvc_uid)) if pvc_uid is not None else None
            except (TypeError, ValueError, AttributeError):
                parsed_pvc_uid = None
            permit = await recovery_store.acquire_cleanup_permit(
                owner_kind="job",
                owner_id=UUID(job_id),
                pvc_uid=parsed_pvc_uid,
                request_id=cleanup_request_id,
                source="completion_workspace_teardown",
            )
            if not permit.allowed:
                raise RuntimeError(
                    "workspace teardown held for unresolved workspace recovery"
                )
            cleanup_admission = permit

        async def _complete_destructive_cleanup(outcome: str) -> None:
            admission_id = getattr(cleanup_admission, "admission_id", None)
            if admission_id is not None:
                await recovery_store.complete_cleanup_permit(
                    admission_id, outcome=outcome
                )

        async def _release_captured_vm(intent: Mapping[str, Any]) -> Any:
            from orchestrator.services.vm_provisioner import VMTeardownIdentity

            generation = intent.get("provision_generation")
            vm_uid = intent.get("vm_uid")
            rootdisk_uid = intent.get("rootdisk_pvc_uid")
            ssh_host = intent.get("ssh_host")
            ssh_port = intent.get("ssh_port")
            ssh_host_key_fingerprint = intent.get("ssh_host_key_fingerprint")
            if not isinstance(generation, str) or str(UUID(generation)) != generation:
                raise RuntimeError(
                    "VM teardown intent has invalid provision generation"
                )
            for label, value in (
                ("VM UID", vm_uid),
                ("rootdisk PVC UID", rootdisk_uid),
            ):
                if value is not None and (
                    not isinstance(value, str)
                    or not value
                    or value != value.strip()
                    or len(value) > 256
                    or any(character.isspace() for character in value)
                ):
                    raise RuntimeError(f"VM teardown intent has invalid {label}")
            if ssh_host is not None and (
                not isinstance(ssh_host, str) or not ssh_host or len(ssh_host) > 512
            ):
                raise RuntimeError("VM teardown intent has invalid SSH host")
            if ssh_port is not None and (
                isinstance(ssh_port, bool)
                or not isinstance(ssh_port, int)
                or not 1 <= ssh_port <= 65535
            ):
                raise RuntimeError("VM teardown intent has invalid SSH port")
            if (
                not isinstance(ssh_host_key_fingerprint, str)
                or not ssh_host_key_fingerprint.startswith("SHA256:")
                or any(character.isspace() for character in ssh_host_key_fingerprint)
            ):
                raise RuntimeError("VM teardown intent has invalid SSH host key")
            await _admit_destructive_cleanup(rootdisk_uid)
            return await vm_provisioner.release_vm_captured(
                job_id,
                VMTeardownIdentity(
                    provision_generation=generation,
                    vm_uid=vm_uid,
                    rootdisk_pvc_uid=rootdisk_uid,
                    ssh_host=ssh_host,
                    ssh_port=ssh_port,
                    ssh_host_key_fingerprint=ssh_host_key_fingerprint,
                ),
                ssh_host=ssh_host,
                ssh_port=ssh_port,
            )

        async def _capture_kubernetes_teardown_detail() -> dict[str, Any]:
            captured = await container_provisioner.capture_terminal_workspace_identity(
                WorkspaceOwner.job(job_id)
            )
            return {
                "pod_uid": captured.pod_uid,
                "pvc_uid": captured.pvc_uid,
                "service_uid": captured.service_uid,
                "pod_ip": captured.pod_ip,
                "ssh_host_key_fingerprint": captured.ssh_host_key_fingerprint,
                "ssh_port": captured.ssh_port,
                "snapshot_generation": effect_runner.command_id,
                "snapshot_created_at": datetime.now(timezone.utc).isoformat(),
            }

        async def _release_captured_kubernetes(
            intent: Mapping[str, Any],
        ) -> str:
            pod_uid = intent.get("pod_uid")
            pvc_uid = intent.get("pvc_uid")
            service_uid = intent.get("service_uid")
            pod_ip = intent.get("pod_ip")
            host_key = intent.get("ssh_host_key_fingerprint")
            ssh_port = intent.get("ssh_port")
            snapshot_generation = intent.get("snapshot_generation")
            snapshot_created_at = intent.get("snapshot_created_at")
            if not isinstance(pod_uid, str) or not pod_uid:
                raise RuntimeError("workspace teardown intent has invalid Pod UID")
            if pvc_uid is not None and (not isinstance(pvc_uid, str) or not pvc_uid):
                raise RuntimeError("workspace teardown intent has invalid PVC UID")
            if service_uid is not None and (
                not isinstance(service_uid, str) or not service_uid
            ):
                raise RuntimeError("workspace teardown intent has invalid Service UID")
            if not isinstance(pod_ip, str) or not pod_ip:
                raise RuntimeError("workspace teardown intent has invalid Pod IP")
            if not isinstance(host_key, str) or not host_key:
                raise RuntimeError("workspace teardown intent has invalid SSH host key")
            if isinstance(ssh_port, bool) or not isinstance(ssh_port, int):
                raise RuntimeError("workspace teardown intent has invalid SSH port")
            if (
                snapshot_generation != effect_runner.command_id
                or not isinstance(snapshot_created_at, str)
                or not snapshot_created_at
            ):
                raise RuntimeError(
                    "workspace teardown intent has invalid snapshot identity"
                )
            teardown_identity = WorkspaceTeardownIdentity(
                pod_uid=pod_uid,
                pvc_uid=pvc_uid,
                service_uid=service_uid,
                pod_ip=pod_ip,
                ssh_host_key_fingerprint=host_key,
                ssh_port=ssh_port,
            )
            await _admit_destructive_cleanup(pvc_uid)
            released = await container_provisioner.release_workspace(
                WorkspaceOwner.job(job_id),
                teardown_identity=teardown_identity,
                require_snapshot=True,
                expected_runtime_incarnation=pod_uid,
                expected_host_key_fingerprint=host_key,
                strict_terminal_snapshot=True,
                terminal_snapshot_generation=snapshot_generation,
                terminal_snapshot_created_at=snapshot_created_at,
                strict=True,
                exact_absence_timeout_seconds=(
                    _COMPLETION_S36_EXACT_ABSENCE_TIMEOUT_SECONDS
                ),
            )
            if released:
                return "completed"
            return await container_provisioner.classify_workspace_teardown_identity(
                WorkspaceOwner.job(job_id),
                teardown_identity,
            )

        try:
            if effect_runner is not None:
                authorization = await effect_runner.authorize_workspace_teardown()
                if not authorization.authorized:
                    if authorization.superseded:
                        return {
                            "actions": [],
                            "error": (
                                "jobs status changed before workspace teardown "
                                "authorization"
                            ),
                            "teardown_disposition": "world_state_superseded",
                            "observed_status": authorization.observed_status,
                            "expected_status": authorization.expected_status,
                        }
                    if authorization.operator_hold:
                        return {
                            "actions": [],
                            "error": (
                                "workspace teardown authorization marker conflicts "
                                "with current jobs status"
                            ),
                            "teardown_disposition": "operator_hold",
                            "observed_status": authorization.observed_status,
                            "expected_status": authorization.expected_status,
                        }
                    return {
                        "actions": [],
                        "teardown_disposition": "deferred",
                        "higher_report_seq": authorization.higher_report_seq,
                    }

            use_uid_fenced_kubernetes_teardown = False
            use_identity_fenced_vm_teardown = False
            teardown_intent: dict[str, Any] | None = None
            if effect_runner is not None:
                teardown_intent = await effect_runner.capture_intent(
                    "workspace_archive_teardown"
                )
                intent_kind = (
                    teardown_intent.get("kind")
                    if isinstance(teardown_intent, Mapping)
                    else None
                )
                use_uid_fenced_kubernetes_teardown = bool(
                    intent_kind in {"kubernetes", "vm_and_kubernetes"}
                )
                use_identity_fenced_vm_teardown = bool(
                    intent_kind in {"vm", "vm_and_kubernetes"}
                )
                teardown_job = await postgres_db.get_job(job_id)
                if (
                    not use_uid_fenced_kubernetes_teardown
                    and not use_identity_fenced_vm_teardown
                    and teardown_job is not None
                ):
                    workspace_context = _get_container_context(teardown_job)
                    vm_context = _get_vm_context(teardown_job)
                    workspace_is_active = bool(workspace_context) and (
                        workspace_context.get("status")
                        not in ("deleted", "deleting", "released", None)
                    )
                    vm_is_active = bool(vm_context) and (
                        vm_context.get("status") not in ("deleted", "deleting")
                    )
                    legacy_backend_is_active = bool(
                        (
                            workspace_is_active
                            and workspace_context.get("provisioner") == "docker"
                        )
                        or (vm_is_active and vm_context.get("provisioner") == "docker")
                    )
                    use_uid_fenced_kubernetes_teardown = (
                        workspace_is_active
                        and workspace_context.get("provisioner") != "docker"
                        and not legacy_backend_is_active
                    )
                    use_identity_fenced_vm_teardown = (
                        vm_is_active
                        and vm_context.get("provisioner") != "docker"
                        and not legacy_backend_is_active
                    )

                    kubernetes_detail = None
                    vm_detail = None
                    if use_uid_fenced_kubernetes_teardown:
                        kubernetes_detail = await _capture_kubernetes_teardown_detail()
                    if use_identity_fenced_vm_teardown:
                        captured_vm = await vm_provisioner.capture_vm_teardown_identity(
                            job_id
                        )
                        vm_detail = {
                            "provision_generation": (captured_vm.provision_generation),
                            "vm_uid": captured_vm.vm_uid,
                            "rootdisk_pvc_uid": captured_vm.rootdisk_pvc_uid,
                            "ssh_host": captured_vm.ssh_host,
                            "ssh_port": captured_vm.ssh_port,
                            "ssh_host_key_fingerprint": (
                                captured_vm.ssh_host_key_fingerprint
                            ),
                        }
                    if kubernetes_detail is not None and vm_detail is not None:
                        intent_detail = {
                            "kind": "vm_and_kubernetes",
                            "vm": vm_detail,
                            "kubernetes": kubernetes_detail,
                        }
                    elif vm_detail is not None:
                        intent_detail = {"kind": "vm", **vm_detail}
                    elif kubernetes_detail is not None:
                        intent_detail = {"kind": "kubernetes", **kubernetes_detail}
                    else:
                        intent_detail = None
                    if intent_detail is not None:
                        teardown_intent = await effect_runner.capture_intent(
                            "workspace_archive_teardown",
                            intent_detail,
                        )

            cleanup_actions: list[str] = []
            teardown_dispositions: list[str] = []
            retry_reasons: list[str] = []
            if use_identity_fenced_vm_teardown:
                try:
                    if teardown_intent is None:
                        raise RuntimeError("VM teardown intent is missing identity")
                    vm_intent = (
                        teardown_intent.get("vm")
                        if teardown_intent.get("kind") == "vm_and_kubernetes"
                        else teardown_intent
                    )
                    if not isinstance(vm_intent, Mapping):
                        raise RuntimeError("VM teardown intent is missing identity")
                    outcome = await _release_captured_vm(vm_intent)
                    teardown_dispositions.append(outcome.disposition)
                    if outcome.disposition == "completed":
                        cleanup_actions.append("vm released")
                    elif outcome.disposition != "identity_superseded":
                        retry_reasons.append(
                            "captured VM teardown remains " + outcome.disposition
                        )
                except Exception as exc:
                    retry_reasons.append(f"captured VM teardown failed: {exc}")

            if use_uid_fenced_kubernetes_teardown:
                try:
                    if teardown_intent is None:
                        raise RuntimeError(
                            "workspace teardown intent is missing Kubernetes identity"
                        )
                    kubernetes_intent = (
                        teardown_intent.get("kubernetes")
                        if teardown_intent.get("kind") == "vm_and_kubernetes"
                        else teardown_intent
                    )
                    if not isinstance(kubernetes_intent, Mapping):
                        raise RuntimeError(
                            "workspace teardown intent is missing Kubernetes identity"
                        )
                    kubernetes_disposition = await _release_captured_kubernetes(
                        kubernetes_intent
                    )
                    teardown_dispositions.append(kubernetes_disposition)
                    if kubernetes_disposition == "completed":
                        cleanup_actions.append("k8s workspace released")
                    elif kubernetes_disposition != "identity_superseded":
                        retry_reasons.append(
                            "captured Kubernetes teardown remains "
                            + kubernetes_disposition
                        )
                except Exception as exc:
                    retry_reasons.append(f"captured Kubernetes teardown failed: {exc}")

            # A composite must give each captured side one independent chance
            # to converge.  Unknown beats superseded so the exact old
            # counterpart remains recoverable; once both sides are terminal,
            # any proven replacement terminal-supersedes only S36.
            if retry_reasons:
                raise RuntimeError("; ".join(retry_reasons))
            if "identity_superseded" in teardown_dispositions:
                await _complete_destructive_cleanup("identity_superseded")
                return {
                    "actions": cleanup_actions,
                    "teardown_disposition": "identity_superseded",
                }

            if not (
                use_identity_fenced_vm_teardown or use_uid_fenced_kubernetes_teardown
            ):
                await _admit_destructive_cleanup(None)
                cleanup_actions = await _archive_and_cleanup_workspace(job_id)
        except Exception as exc:
            logger.warning(
                "Workspace cleanup failed for job %s (non-blocking): %s",
                job_id,
                exc,
            )
            return {
                "actions": [f"workspace cleanup failed: {exc}"],
                "error": str(exc),
                "teardown_disposition": "retry_pending",
            }
        await _complete_destructive_cleanup("completed")
        return {
            "actions": list(cleanup_actions),
            "teardown_disposition": "completed",
        }

    output = await run_completion_effect(
        effect_runner,
        "workspace_archive_teardown",
        "workspace_teardown",
        _archive_and_teardown_workspace,
        retry_if=lambda output: bool(output.get("error")),
        supersede_if=lambda output: (
            output.get("teardown_disposition") == "identity_superseded"
        ),
        effect_timeout_seconds=890.0,
        command_lease_seconds=900.0,
    )
    if output.get("teardown_disposition") == "world_state_superseded":
        # The retryable output above deliberately keeps S36 pending. Raising
        # after the runner has persisted it lets the finalizer supersede the
        # whole command without ever treating teardown as complete.
        from orchestrator.services.completion_finalizer import (
            CompletionDispositionSuperseded,
        )

        raise CompletionDispositionSuperseded(
            observed_status=str(output.get("observed_status") or "unknown"),
            expected_statuses=(str(output.get("expected_status") or ""),),
            reason="workspace_teardown_status_superseded",
        )
    return output


__all__ = [
    "CompletionEffectDependencies",
    "completion_effect_dedup_key",
    "run_completion_effect",
    "run_completion_workspace_teardown",
]
