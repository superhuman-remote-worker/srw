"""``srw.cloud-folder/v1``: one folder of the main cloud, mounted into a session.

Only SRW creates a cloud folder connector, for a folder the main cloud
provisions (``user_creatable`` is off: the API refuses a create). Its config
names the folder and never a credential: the folder's kind and project, the
provider and its installation, and the provider's opaque folder handle. The
credential is the main cloud's, resolved when the folder is mounted. A
project's folder carries the managed key ``project-cloud:<project>``; no
other marker counts. A user's own root (folder kind ``user_root``) carries
``user-cloud-root:<user>`` and is mounted with its user's own credential,
which SRW stores on the row when the user connects it
(``orchestrator.services.cloud_user_root``); the API never sets one.

The connector never reaches the agent as a connector: no payload entry, no
tools, no credentials (:meth:`CloudFolderDriver.runtime_allowed` keeps it
out of ``forwarded_datasources``). Its delivery is the mount, which the
workspace's mount plan builds from :meth:`CloudFolderDriver.mount_binding`.

The spec's levels are the union of what any provider enforces. A connector
is offered the ones its provider's support matrix declares for the folder's
kind on the workspace tier (``orchestrator.services.cloud.capabilities``),
so a level the provider cannot enforce there is never bound, and a
read-only binding without a reader grant is refused (the strict access
rule, ``shared.connectors.contract.strict_access_resolution``). The reviewed
level (``protected``) also needs the protected review lane: never for an
officer's session, nor where the installation's switch is off.

Design: knowledge-base/knowledge/features/main_cloud_as_connectors.md,
"Slice 3 plan (2026-10-10)".
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from fastapi import HTTPException

from orchestrator.services.connector_drivers.base import (
    AccessContext,
    BindContext,
    ConnectorDraft,
    DatasourceDriver,
    DeploymentGates,
    NormalizedConnector,
    ValidationContext,
    bound_access,
)
from shared.connectors.builtin import CLOUD_FOLDER_KINDS, CLOUD_FOLDER_SPEC
from shared.connectors.platform import USER_CLOUD_ROOT, native_user, platform_owned

#: The managed key kind of a project's own cloud folder connector.
PROJECT_CLOUD = "project-cloud"
#: The level that needs the protected review lane.
_REVIEWED = "protected"

_KEYS_IN_ORDER = tuple(CLOUD_FOLDER_SPEC.config_schema["properties"])
_KEYS = frozenset(_KEYS_IN_ORDER)
_REQUIRED = tuple(CLOUD_FOLDER_SPEC.config_schema["required"])
#: A personal root's connection, in its config once its user connects it
#: (main cloud slice 4): never the credential, which is the row's own.
USER_ROOT_KEYS = frozenset({"account_id", "grant", "connect_failure"})
#: Why a connector mounts nothing, as closed codes: its access level is
#: refused here (the strict rule; ``detail`` says why), or its row is not a
#: folder SRW created (a config that does not name one, or no managed key).
UNAVAILABLE_REASONS: tuple[str, ...] = ("access_unavailable", "config_invalid")


def normalize_cloud_folder_config(config: Any) -> dict[str, Any]:
    """The config a cloud folder connector stores, or ``ValueError``.

    The keys the spec declares and no others; ``project_id`` and
    ``folder_handle`` are required for a project folder (a personal root
    names its folder once its user connected it, and only a personal root
    has the connection's keys, :data:`USER_ROOT_KEYS`); ids are UUIDs, kept
    in their canonical spelling.
    """
    if not isinstance(config, Mapping):
        raise ValueError("A cloud folder connector's config must be an object")
    unknown = sorted(set(config) - _KEYS)
    if unknown:
        raise ValueError(f"Unknown cloud folder config field(s): {', '.join(unknown)}")
    missing = [key for key in _REQUIRED if config.get(key) in (None, "")]
    if config.get("folder_kind") == "project":
        missing += [
            key for key in ("project_id", "folder_handle") if not config.get(key)
        ]
    if missing:
        raise ValueError(
            f"A cloud folder connector needs {', '.join(sorted(set(missing)))}"
        )
    if config["folder_kind"] not in CLOUD_FOLDER_KINDS:
        raise ValueError(
            f"Cloud folder folder_kind must be one of {', '.join(CLOUD_FOLDER_KINDS)}"
        )
    root_only = sorted(key for key in USER_ROOT_KEYS if key in config)
    if root_only and config["folder_kind"] != "user_root":
        raise ValueError(
            f"Only a personal root has {', '.join(root_only)} in its config"
        )
    out: dict[str, Any] = {"folder_kind": config["folder_kind"]}
    for key in ("project_id", "backend_instance_id"):
        value = config.get(key)
        if value in (None, ""):
            continue
        try:
            out[key] = str(UUID(str(value)))
        except ValueError:
            raise ValueError(f"Cloud folder {key} must be a UUID") from None
    for key in ("backend_id", "folder_handle", "account_id"):
        if key not in config or (key != "backend_id" and config[key] in (None, "")):
            continue
        value = config[key]
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"Cloud folder {key} must be a non-empty string")
        out[key] = value
    for key in ("grant", "connect_failure"):
        if config.get(key) is None:
            continue
        if not isinstance(config[key], Mapping):
            raise ValueError(f"Cloud folder {key} must be an object")
        out[key] = dict(config[key])
    # In the spec's key order, so a stored config reads the same each time.
    return {key: out[key] for key in _KEYS_IN_ORDER if key in out}


@dataclass(frozen=True)
class CloudFolderMount:
    """One cloud folder connector, ready for the workspace's mount plan.

    ``connector_id`` is the mount's id, stable across plans. ``name`` is the
    workspace name the folder is mounted under, at ``cloud/<name>``
    (:attr:`target_path`); a plan that mounts two connectors of one name
    tells them apart. ``access`` is the level the binding resolved
    (``read_only``, ``protected`` or ``read_write``); the credential that
    makes it true is the plan's to resolve, from the main cloud.
    """

    connector_id: str
    name: str
    access: str
    folder_kind: str
    project_id: str | None
    backend_id: str
    backend_instance_id: str
    folder_handle: str

    @property
    def target_path(self) -> str:
        return f"cloud/{self.name}"

    def mount_row(self) -> dict[str, Any]:
        """The connector in the shape of a ``thread_mounts`` row, which the
        existing rclone row builder reads (``agent_cloud_mounts``)."""
        return {
            "id": self.connector_id,
            "source_kind": "cloud_folder",
            "source_ref": self.connector_id,
            "mount_kind": self.folder_kind,
            "target_path": self.target_path,
            "backend_id": self.backend_id,
            "backend_instance_id": self.backend_instance_id,
            "cloud_handle": self.folder_handle,
            "access": self.access,
        }


@dataclass(frozen=True)
class CloudFolderUnavailable:
    """A cloud folder connector that mounts nothing, and why.

    ``reason`` is one of :data:`UNAVAILABLE_REASONS`; ``detail`` is a line a
    person can read. Nothing takes the folder's place.
    """

    connector_id: str
    reason: str
    detail: str


class CloudFolderDriver(DatasourceDriver):
    def __init__(self) -> None:
        super().__init__(CLOUD_FOLDER_SPEC)

    async def validate(
        self,
        draft: ConnectorDraft,
        *,
        existing: Mapping[str, Any] | None,
        ctx: ValidationContext,
    ) -> NormalizedConnector:
        """What SRW stores for a folder it provisions, and the few edits a
        stored one takes (its name and description).

        No credential and no connection URL: the main cloud holds both. A
        create here is SRW's own (``validate_platform_connector``); the API
        refuses one before it reaches a driver (``user_creatable``).
        """
        if draft.credentials:
            raise HTTPException(
                status_code=400,
                detail=(
                    "A cloud folder connector holds no credentials; SRW uses "
                    "the main cloud's when it mounts the folder"
                ),
            )
        if draft.connection_url:
            raise HTTPException(
                status_code=400,
                detail="A cloud folder connector has no connection URL",
            )
        if draft.is_global:
            raise HTTPException(
                status_code=400,
                detail="Cloud folder connectors cannot be published (is_global)",
            )
        if existing is None:
            return NormalizedConnector(None, self._config(draft.config), {})
        if draft.config is not None and self._config(draft.config) != (
            self._config(existing.get("config"))
        ):
            raise HTTPException(
                status_code=400,
                detail="SRW sets which folder a cloud folder connector names",
            )
        return NormalizedConnector(None, None, None)

    @staticmethod
    def _config(config: Any) -> dict[str, Any]:
        try:
            return normalize_cloud_folder_config(config)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    def runtime_allowed(self, row: Mapping[str, Any], gates: DeploymentGates) -> bool:
        """Never: the folder is mounted into the workspace, and the agent
        gets neither an entry, nor tools, nor the main cloud's credential."""
        return False

    def bind(
        self, row: Mapping[str, Any], credentials: Any, *, ctx: BindContext
    ) -> dict[str, Any] | None:
        return None

    def effective_access(self, row: Mapping[str, Any]) -> str | None:
        """The level ``row`` binds at on some tier, or ``None`` if refused."""
        return bound_access(self, row).level

    def offered_access(
        self, row: Mapping[str, Any], ctx: AccessContext
    ) -> tuple[str, ...]:
        """The levels the row's provider declares it offers for the folder's
        kind on ``ctx``'s workspace tier (any tier for ``None``), without the
        reviewed level where the protected lane is not open to ``ctx``; none
        for a row that names no folder or no provider SRW knows."""
        from orchestrator.services.cloud import provider_capabilities

        try:
            config = normalize_cloud_folder_config(row.get("config"))
        except ValueError:
            return ()
        declared = provider_capabilities(config["backend_id"])
        if declared is None:
            return ()
        reviewed = ctx.protected_mode_enabled and not ctx.officer
        return tuple(
            level
            for level in self.spec.ranked_access_ids()
            if (reviewed or level != _REVIEWED)
            and declared.offers(
                self.type_id,
                config["folder_kind"],
                level,
                workspace_backend=ctx.workspace_backend,
            )
        )

    def mount_binding(
        self,
        row: Mapping[str, Any],
        *,
        ctx: AccessContext,
        requested: str | None = None,
        ceiling: str | None = None,
    ) -> CloudFolderMount | CloudFolderUnavailable:
        """What the mount plan mounts for one attached connector row, or why
        it mounts nothing.

        ``requested`` is the level the session resolved for the connector
        when it was created (``threads.metadata.datasource_access``): it
        binds as stored or not at all (``access_unavailable``). Without one
        the connector binds at its project link's default
        (``project_access``), else its highest offered level, a default
        lowered to what is allowed here but never raised, and read-only when
        its project link or its creator's tag says so. ``ceiling`` is the
        highest level the session's owner may have (a viewer of the
        folder's project: ``read_only``). Only a row SRW created names a
        folder the main cloud's credential may be used for: one with a
        managed key, and for a project's folder the key of the project its
        config names.
        """
        from orchestrator.services.thread_mount_rows import slugify_mount_name

        connector_id = str(row.get("id") or "")
        key = platform_owned(row)
        if not key:
            return CloudFolderUnavailable(
                connector_id,
                "config_invalid",
                "This cloud folder connector was not created by SRW",
            )
        try:
            config = normalize_cloud_folder_config(row.get("config"))
        except ValueError as exc:
            return CloudFolderUnavailable(connector_id, "config_invalid", str(exc))
        if (
            config["folder_kind"] == "project"
            and key != f"{PROJECT_CLOUD}:{config['project_id']}"
        ):
            return CloudFolderUnavailable(
                connector_id,
                "config_invalid",
                "This cloud folder connector is not its project's own folder",
            )
        if config["folder_kind"] == "user_root" and (
            key.partition(":")[0] != USER_CLOUD_ROOT or native_user(row) is None
        ):
            return CloudFolderUnavailable(
                connector_id,
                "config_invalid",
                "This cloud folder connector is not a user's own root",
            )
        access = bound_access(
            self,
            row,
            ctx=ctx,
            requested=requested,
            requested_origin="materialized",
            ceiling=ceiling,
        )
        if access.level is None:
            return CloudFolderUnavailable(
                connector_id,
                "access_unavailable",
                access.reason or "No access level is offered for this folder here",
            )
        return CloudFolderMount(
            connector_id=connector_id,
            name=slugify_mount_name(str(row.get("name") or "")),
            access=access.level,
            folder_kind=config["folder_kind"],
            project_id=config.get("project_id"),
            backend_id=config["backend_id"],
            backend_instance_id=config["backend_instance_id"],
            # A personal root names its folder once its user connected it;
            # its binder decides what an unconnected one mounts.
            folder_handle=config.get("folder_handle") or "",
        )
