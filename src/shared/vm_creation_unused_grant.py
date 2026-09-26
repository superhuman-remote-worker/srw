"""Reasons and evidence for a creation grant the winning issuer never used."""

RETRYABLE_NOT_ATTEMPTED_REASON = "resource_inventory_unavailable"
NOT_ATTEMPTED_REASONS = frozenset({
    RETRYABLE_NOT_ATTEMPTED_REASON, "resource_node_changed",
    "creation_carrier_changed", "creation_observed_object_missing",
    "creation_observed_object_changed", "retained_disk_changed",
    "workspace_recovery_held", "workspace_attachment_unproven",
    "creation_existing_vm_unproven", "creation_rootdisk_source_unproven",
})


def not_attempted_evidence(reason: str) -> dict[str, str]:
    if type(reason) is not str or reason not in NOT_ATTEMPTED_REASONS:
        raise ValueError("Invalid unused-grant refusal")
    return {"outcome": "not_attempted", "reason": reason}
