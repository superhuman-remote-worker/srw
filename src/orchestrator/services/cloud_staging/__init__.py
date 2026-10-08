"""Protected cloud Slice C — staging pipeline (stage → S3 → review → apply)."""

from __future__ import annotations


def select_protected_mount(mount_rows: list[dict]) -> dict | None:
    """The one definition of which of a thread's mounts is the protected one:
    the first non-default project mount that has a cloud handle, on a
    provider that offers the protected level (its adapter's declaration;
    Nextcloud today).

    Default-project rows expose the owner's personal cloud home and are outside
    protected-mode v1's project-folder-scale safety contract. A session may
    attach both a default and a regular project, so enforce that exclusion here
    as well as in Cockpit's checkbox gate instead of relying on row order.
    Mirrors (and replaces) the inline pick in
    ``main._engage_protected_cloud_for_thread``.
    """
    # Late: the Nextcloud adapter imports cloud_staging.source_identity, which
    # runs this package's __init__ while orchestrator.services.cloud loads.
    from orchestrator.services.cloud import PROTECTED_PROJECT_FOLDER, provider_offers

    for row in mount_rows or []:
        if (
            row.get("mount_kind") != "project_default"
            and provider_offers(row.get("backend_id"), PROTECTED_PROJECT_FOLDER)
            and row.get("cloud_handle")
        ):
            return row
    return None
