"""Importing an MCP Registry ``server.json`` as a managed MCP driver (D6).

The mapping is ``shared.connectors.server_json``; this registers its result
like any image (``connector_driver_registrations.register_driver``): in the
caller's Account, a Project or the Catalog, under the same authority, names
and trust rules, with the ``server.json`` kept as the registration's source.
The image is resolved in its registry, and when it carries the MCP
Registry's ownership label (``io.modelcontextprotocol.server.name``) the
label must name the same server.

A registered managed MCP driver is listed with its spec; connectors of it
bind once service-plane hosting serves registrations (this release binds
registered bind-time drivers only).
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from fastapi import HTTPException

from orchestrator.services.connector_driver_registrations import (
    DriverTrustPolicy,
    Registration,
    ResolveImage,
    register_driver,
)
from shared.connectors.images import ImageReference
from shared.connectors.server_json import (
    SERVER_NAME_LABEL,
    ServerJsonError,
    spec_from_server_json,
)


async def import_server_json(
    db: Any,
    user: Mapping[str, Any],
    *,
    server: Mapping[str, Any],
    scope: Mapping[str, Any] | None,
    package: int | None,
    policy: DriverTrustPolicy,
    resolve_image: ResolveImage,
    request: Any = None,
) -> Registration:
    """Register one ``oci`` package of ``server`` as a managed MCP driver."""
    try:
        spec_json, reference = spec_from_server_json(server, package=package)
    except ServerJsonError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from None
    return await register_driver(
        db,
        user,
        scope=scope,
        image_reference=reference,
        policy=policy,
        resolve_image=_owned(resolve_image, str(server.get("name"))),
        spec_json=spec_json,
        spec_source="server_json",
        source_document=dict(server),
        description=(
            str(server["description"])[:2000]
            if isinstance(server.get("description"), str)
            else None
        ),
        request=request,
    )


def _owned(resolve: ResolveImage, server_name: str) -> ResolveImage:
    """``resolve``, refusing an image whose ownership label names another
    server (an image without the label is accepted: SRW does not verify
    foreign images)."""

    async def checked(lookup: str) -> Any:
        resolved = await resolve(lookup)
        labelled = (getattr(resolved, "labels", None) or {}).get(SERVER_NAME_LABEL)
        if labelled is not None and labelled != server_name:
            raise HTTPException(
                status_code=422,
                detail=(
                    f"The image {ImageReference.parse(lookup)} says it is the MCP "
                    f"server {labelled!r}, not {server_name!r}"
                ),
            )
        return resolved

    return checked


__all__ = ["import_server_json"]
