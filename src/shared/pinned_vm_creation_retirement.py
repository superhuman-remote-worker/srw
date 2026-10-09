"""Receipt shape for initial VM End; database settlement is separate authority."""

from collections.abc import Mapping
from uuid import UUID


def initial_vm_creation_retirement_source(context):
    """Recognize the captured source shape, never infer that it has settled.

    Database receipt publication and final End independently require positive
    source/disposition settlement under the current retirement token.
    """
    if not isinstance(context, Mapping) or (
        context.get("workspace_backend") != "vm"
        or context.get("vm") is not None
        or context.get("workspace_binding") not in (None, {})
    ):
        return None
    workspace = context.get("workspace_container")
    if workspace is not None and (
        not isinstance(workspace, Mapping)
        or set(workspace) - {"repo_name", "git_remote_url"}
    ):
        return None
    source = context.get("vm_creation_source")
    if not isinstance(source, Mapping):
        return None
    try:
        for key in ("request_id", "provision_generation", "thread_runtime_generation"):
            if str(UUID(source[key])) != source[key]:
                return None
    except (KeyError, TypeError, ValueError, AttributeError):
        return None
    captured = source.get("captured_vm")
    if not isinstance(captured, Mapping) or (
        captured.get("creation_request_id") != source["request_id"]
        or captured.get("provision_generation") != source["provision_generation"]
        or captured.get("vm_uid") is not None
        or captured.get("rootdisk") == "kept"
        or captured.get("idle_wake_operation_id") is not None
        or captured.get("idle_predecessor_pvc_uid") is not None
    ):
        return None
    return source


def pre_registration_vm_agent_zero_source(context):
    """Recognize a never-issued VM life with only its published Agent Pod.

    A captured Agent workspace claim is the Pod's retained PVC, not VM compute
    authority. The database independently joins that claim to its durable row
    and published Pod intent before accepting a local-zero receipt.
    """

    if not isinstance(context, Mapping) or (
        context.get("workspace_backend") != "vm"
        or context.get("entry_status") != "created"
        or context.get("runtime_authority_exposed") is not True
        or context.get("vm") is not None
        or context.get("vm_creation_source") is not None
        or context.get("agent_id") is not None
        or context.get("control_admission_agent_id") is not None
        or context.get("runtime_attach_token") is not None
        or context.get("agent") not in (None, {})
        or context.get("agent_pod_provision_intent") not in (None, {})
        or context.get("workspace_binding") not in (None, {})
        or context.get("workspace_provision_intent") not in (None, {})
    ):
        return False
    workspace = context.get("workspace_container")
    pod = context.get("agent_pod")
    if (
        (
            workspace is not None
            and (
                not isinstance(workspace, Mapping)
                or set(workspace) - {"repo_name", "git_remote_url"}
            )
        )
        or not isinstance(pod, Mapping)
        or not pod.get("pod_name")
        or not pod.get("pod_uid")
    ):
        return False
    claim = context.get("agent_workspace_claim")
    if claim in (None, {}):
        return True
    if not isinstance(claim, Mapping):
        return False
    try:
        for field in (
            "claim_id",
            "thread_id",
            "created_runtime_generation",
            "create_attempt",
        ):
            if str(UUID(str(claim[field]))) != claim[field]:
                return False
    except (KeyError, TypeError, ValueError, AttributeError):
        return False
    return bool(
        claim.get("status") == "ready"
        and claim.get("provisioner") in {"agent", "persistent"}
        and claim.get("thread_id") == context.get("thread_id")
        and claim.get("namespace") == pod.get("namespace")
        and pod.get("runtime_generation") == context.get("generation")
        and claim.get("protection_protocol")
        == pod.get("protection_protocol")
        == "finalizer_v1"
        and bool(claim.get("pvc_name"))
        and bool(claim.get("pvc_uid"))
    )
