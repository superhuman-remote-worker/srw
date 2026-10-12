"""Describe the exact pinned native target and its workspace attestation."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Awaitable, Callable
from typing import Any
from uuid import UUID

from orchestrator.services.canvas_ssh import (
    CanvasSSHError,
    bound_workspace_generation,
    remote_target_is_vm_backed,
    resolve_remote_workspace_target,
)
from orchestrator.services.ssh_access import thread_metadata_object


def container_workspace_digest(thread: dict[str, Any]) -> str | None:
    """Hash the immutable provisioner binding and the endpoint it selects."""

    try:
        if remote_target_is_vm_backed(thread):
            return None
        target = resolve_remote_workspace_target(
            thread, bound_workspace_generation(thread)
        )
        binding = thread_metadata_object(thread).get("_workspace_binding")
        if not isinstance(binding, dict) or not isinstance(
            binding.get("backing_id"), str
        ):
            return None
        material = json.dumps(
            {"binding": binding, "endpoint": target.pool_key},
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("ascii")
        return "sha256:" + hashlib.sha256(material).hexdigest()
    except (CanvasSSHError, TypeError, ValueError, UnicodeError):
        return None


async def describe_native_target(
    thread: dict[str, Any],
    *,
    prepare: Callable[..., Awaitable[Any]],
    backend: str = "container",
    vm_binding: str | None = None,
) -> tuple[dict[str, Any], Any | None] | None:
    """Return a descriptor only for an admitted pinned life or known exemption."""

    if thread.get("status") in {"ended", "suspended"}:
        return None
    lane = thread.get("execution_lane")
    if lane == "stateless":
        return {"execution_lane": "stateless", "no_boot_watchdog": True}, None
    if lane != "pinned" or thread.get("runtime_retirement_token") is not None:
        return None
    metadata = thread_metadata_object(thread)
    config = metadata.get("config_override")
    if isinstance(config, dict):
        officer = config.get("officer")
        if isinstance(officer, dict) and officer.get("enabled") is True:
            return {"execution_lane": "pinned", "no_boot_watchdog": "officer"}, None
    try:
        thread_id, agent_id, generation, attach_token = (
            str(UUID(str(thread.get(name))))
            for name in ("id", "agent_id", "runtime_generation", "runtime_attach_token")
        )
    except (TypeError, ValueError, AttributeError):
        return None
    if backend == "container":
        digest = container_workspace_digest(thread)
    elif (
        backend == "vm"
        and remote_target_is_vm_backed(thread)
        and isinstance(vm_binding, str)
        and len(vm_binding) == 64
    ):
        try:
            int(vm_binding, 16)
        except ValueError:
            return None
        digest = "sha256:" + vm_binding
    else:
        return None
    if digest is None:
        return None
    target = await prepare(
        thread_id=thread_id,
        agent_id=agent_id,
        runtime_generation=generation,
        attach_token=attach_token,
        required_capability="native_workspace_first_use1",
    )
    binding = getattr(target, "binding", None)
    if (
        target is None
        or binding is None
        or binding.thread_id != thread_id
        or binding.agent_id != agent_id
        or binding.runtime_generation != generation
        or binding.runtime_attach_token != attach_token
        or target.process_generation == ""
    ):
        return None
    return {
        "execution_lane": "pinned",
        "native_first_use_contract": 1,
        "native_recipient": {
            "thread_id": thread_id,
            "runtime_generation": generation,
            "agent_id": agent_id,
            "pod_uid": binding.pod_uid,
            "process_generation": target.process_generation,
            "session_identity_fingerprint": binding.session_identity_fingerprint,
            "workspace_digest": digest,
        },
    }, target
