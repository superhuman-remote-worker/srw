"""HTTP adapters for the admin "Main cloud" page and its operator operations.

The main cloud is configured by Helm only (main_cloud_as_connectors.md,
slice 2). ``GET /api/admin/main-cloud`` reports the provider, its installation,
its health, where the configuration comes from and the provider support
matrix; nothing here changes the configuration.

The removed connection-form API answers **410 Gone** rather than 404: a
cached cockpit or a script still calling it learns that the endpoint was
retired on purpose and where the configuration lives now. The one-shot
instance-authority backfill and thread-mount transport repair stay as
operator operations until their work is done.

Every route awaits the admin gate first — before any body is inspected — so a
non-admin learns nothing, not even that an endpoint was retired.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request

from orchestrator.services import main_cloud_settings

router = APIRouter()

#: The 410 body of the retired connection-form API.
RETIRED_DETAIL = (
    "The main cloud is configured by Helm only; this endpoint was removed. "
    "GET /api/admin/main-cloud shows the active configuration."
)


@dataclass(frozen=True)
class MainCloudSettingsRouteDependencies:
    """Per-app collaborators plus main's composed ``_require_admin`` gate.

    ``require_admin`` has no import default on purpose: it is a main-local
    composition of ``security.access.require_admin`` with the store, the
    approved-user resolver and the security-event audit already bound.
    """

    operations: main_cloud_settings.MainCloudSettingsDependencies
    require_admin: Callable[[Request], Awaitable[dict[str, Any]]]


def get_main_cloud_settings_dependencies(
    request: Request,
) -> MainCloudSettingsRouteDependencies:
    """Resolve collaborators only from the application handling this request."""
    return request.app.state.main_cloud_settings_dependencies_factory()


@router.get("/api/admin/main-cloud")
async def get_main_cloud_page(
    request: Request,
    *,
    dependencies: MainCloudSettingsRouteDependencies = Depends(
        get_main_cloud_settings_dependencies
    ),
) -> dict[str, Any]:
    """The read-only Main cloud page: provider, installation, health, the
    configuration's source and the provider support matrix. Admin-only; holds
    no secret."""
    await dependencies.require_admin(request)
    return await main_cloud_settings.get_main_cloud_page(
        dependencies=dependencies.operations
    )


async def _retired(
    request: Request, dependencies: MainCloudSettingsRouteDependencies
) -> None:
    await dependencies.require_admin(request)
    raise HTTPException(status_code=410, detail=RETIRED_DETAIL)


@router.get("/api/admin/system-settings/main_cloud")
async def get_main_cloud_settings(
    request: Request,
    *,
    dependencies: MainCloudSettingsRouteDependencies = Depends(
        get_main_cloud_settings_dependencies
    ),
) -> None:
    """Retired: the connection form's read (410)."""
    await _retired(request, dependencies)


@router.put("/api/admin/system-settings/main_cloud")
async def put_main_cloud_settings(
    request: Request,
    *,
    dependencies: MainCloudSettingsRouteDependencies = Depends(
        get_main_cloud_settings_dependencies
    ),
) -> None:
    """Retired: the connection form's save (410)."""
    await _retired(request, dependencies)


@router.delete("/api/admin/system-settings/main_cloud")
async def delete_main_cloud_settings(
    request: Request,
    *,
    dependencies: MainCloudSettingsRouteDependencies = Depends(
        get_main_cloud_settings_dependencies
    ),
) -> None:
    """Retired: the connection form's reset to env (410). Startup applies Helm."""
    await _retired(request, dependencies)


@router.post("/api/admin/system-settings/main_cloud/test")
async def test_main_cloud_settings(
    request: Request,
    *,
    dependencies: MainCloudSettingsRouteDependencies = Depends(
        get_main_cloud_settings_dependencies
    ),
) -> None:
    """Retired: the connection form's dry run (410)."""
    await _retired(request, dependencies)


@router.post("/api/admin/system-settings/main_cloud/reload")
async def reload_main_cloud_settings(
    request: Request,
    *,
    dependencies: MainCloudSettingsRouteDependencies = Depends(
        get_main_cloud_settings_dependencies
    ),
) -> None:
    """Retired: the forced local re-attestation (410). A secret rotation
    reaches the orchestrator with its restart."""
    await _retired(request, dependencies)


@router.post("/api/admin/system-settings/main_cloud/backfill-instance-authority")
async def backfill_main_cloud_instance_authority(
    request: Request,
    apply: bool = False,
    *,
    dependencies: MainCloudSettingsRouteDependencies = Depends(
        get_main_cloud_settings_dependencies
    ),
) -> dict[str, Any]:
    """Stamp pre-0186 rows with the installation they actually live on.

    Admin-only. **Dry run by default** — pass ``?apply=true`` to write.

    0186 added ``main_cloud_backend_instance_id`` nullable with no backfill, so
    rows stamped before it name a provider but no installation. The router
    fails closed on that shape, which is correct for effects: those projects
    cannot grant membership, share, or have their folder deleted, because we
    cannot say *which* installation to act on.

    This is deliberately NOT a SQL migration. The design
    (knowledge-base/knowledge/features/protected_session_lifecycle_and_mount_readiness.md)
    requires "an operator-attested single-installation mapping plus a verified
    remote proof", and a migration running inside psql at startup can obtain
    neither. Stamping without that proof would launder a guess into recorded
    authority — silently, permanently, and unquestioned by everything
    downstream. A loud refusal is recoverable; a wrong instance UUID is not.

    So the safety argument here is:

    1. the active instance is **re-attested against the live installation**
       before anything is read, so its proof reflects reality now;
    2. every provider named by an unstamped row must resolve to exactly one
       *installation* in the registry. Two registry rows for one provider are
       not automatically ambiguous — they are commonly two routing snapshots
       of the same installation, which share an
       ``installation_proof_sha256``. Two distinct **proofs** are the genuine
       ambiguity, and abort;
    3. a provider we cannot re-attest right now (not the active backend) is
       never stamped, because nothing proves where its rows live.
    """
    await dependencies.require_admin(request)
    return await main_cloud_settings.backfill_main_cloud_instance_authority(
        apply=apply, dependencies=dependencies.operations
    )


@router.post("/api/admin/system-settings/main_cloud/repair-thread-mounts")
async def repair_thread_mount_transport(
    request: Request,
    apply: bool = False,
    *,
    dependencies: MainCloudSettingsRouteDependencies = Depends(
        get_main_cloud_settings_dependencies
    ),
) -> dict[str, Any]:
    """Re-derive the transport of partial project mount rows.

    Admin-only. **Dry run by default** — pass ``?apply=true`` to write.

    The other half of the instance-authority backfill: stamping a project
    does not touch the ``thread_mounts`` rows minted while it was unstamped,
    and one such row (no installation, no WebDAV URL) makes workspace
    delivery discard every mount the thread has and fall back to the legacy
    session folder. Each row is rebuilt through the same builder thread
    create uses, against the project's stamped installation, and written in
    place only when every transport column resolved. Unstamped projects and
    unresolvable installations are skipped and reported, never guessed.
    """
    await dependencies.require_admin(request)
    return await main_cloud_settings.repair_thread_mount_transport(
        apply=apply, dependencies=dependencies.operations
    )
