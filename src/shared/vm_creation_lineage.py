"""Accounting identity for an exact workspace request; never a handoff grant."""

from shared.vm_workspace_storage import storage_binding


def disk_owner(request):
    binding = request.get("workspace_storage")
    if binding is None:
        return request["job_id"]
    binding = storage_binding(binding)
    if binding["owner_kind"] != "job":
        raise ValueError("Unsupported creation storage owner")
    if binding["owner_id"] != request["job_id"] and (
        binding["generation"] < 2 or binding["pvc_uid"] is None
    ):
        raise ValueError("Inherited creation requires exact retained storage")
    return binding["owner_id"]
