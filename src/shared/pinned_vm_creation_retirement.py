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
