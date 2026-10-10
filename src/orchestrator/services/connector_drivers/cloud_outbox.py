"""``srw.cloud-outbox/v1``: a user's outbox in the main cloud.

One connector per user (managed key ``cloud-outbox:<user>``), and per
execution one folder SRW creates in the main cloud's service account and
shares with the execution's owner. A session mounts its folder at
``cloud/outbox``; a job's is a copy of its deliverables SRW makes when the
job completes. The connector names no folder and holds nothing (its config
is empty): the folder is the execution's, recorded on it
(``main_cloud_session_handle`` for a session), and the credential is the
main cloud's, resolved when the folder is mounted.

Only SRW creates an outbox connector (``user_creatable`` is off, and
:meth:`CloudOutboxDriver.validate` refuses a create outside SRW's own
context). It never reaches the agent as a connector: no payload entry, no
tools, no credentials (:meth:`CloudOutboxDriver.runtime_allowed`). A
session's delivery is the mount, which the workspace's mount plan builds
from :meth:`CloudOutboxDriver.mount_binding`.

Which tiers it is offered on depends on the execution
(:meth:`CloudOutboxDriver.offered_access`). A session's folder is mounted
with the service account's credential, so it is offered only on the tiers
the provider's support matrix declares for the outbox (not a root VM). A
job's outbox is a copy SRW makes from the job's repository: nothing of the
main cloud reaches the workspace, so the tier narrows nothing there.

Design: knowledge-base/knowledge/features/main_cloud_as_connectors.md,
"Overnight run: slices 3 to 8" (session outbox).
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

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
from orchestrator.services.connector_drivers.platform import PLATFORM_ENVIRONMENT
from shared.connectors.builtin import CLOUD_OUTBOX_SPEC
from shared.connectors.platform import CLOUD_OUTBOX, managed_kind, native_user

#: The outbox's one level.
_LEVEL = "read_write"
#: The workspace name a session mounts its outbox under, at ``cloud/outbox``.
WORKSPACE_NAME = "outbox"
#: Why an outbox mounts nothing, as closed codes: its level is refused on
#: this tier (``detail`` says why); the row is not a user's outbox SRW
#: created; it is another user's; the session has no folder (SRW has not
#: created it, or the session was started without the outbox); or no main
#: cloud serves the session.
UNAVAILABLE_REASONS: tuple[str, ...] = (
    "access_unavailable",
    "config_invalid",
    "not_eligible",
    "no_session_folder",
    "backend_unavailable",
)


@dataclass(frozen=True)
class CloudOutboxMount:
    """A session's outbox, ready for the workspace's mount plan.

    ``connector_id`` is the mount's id, stable across plans. ``folder_handle``
    is the session's folder (the thread's ``main_cloud_session_handle``),
    ``shared`` whether SRW has shared it with the session's owner yet.
    """

    connector_id: str
    access: str
    backend_id: str
    backend_instance_id: str | None
    folder_handle: str
    shared: bool
    name: str = WORKSPACE_NAME

    @property
    def target_path(self) -> str:
        return f"cloud/{self.name}"

    def mount_row(self) -> dict[str, Any]:
        """The outbox in the shape of a ``thread_mounts`` row, as the cloud
        folder connector gives its own (``CloudFolderMount.mount_row``)."""
        return {
            "id": self.connector_id,
            "source_kind": "cloud_outbox",
            "source_ref": self.connector_id,
            "mount_kind": "cloud_outbox",
            "target_path": self.target_path,
            "backend_id": self.backend_id,
            "backend_instance_id": self.backend_instance_id,
            "cloud_handle": self.folder_handle,
            "access": self.access,
        }


@dataclass(frozen=True)
class CloudOutboxUnavailable:
    """An outbox that mounts nothing, and why.

    ``reason`` is one of :data:`UNAVAILABLE_REASONS`; ``detail`` is a line a
    person can read. Nothing takes the folder's place.
    """

    connector_id: str
    reason: str
    detail: str


def is_outbox_row(row: Mapping[str, Any] | None) -> bool:
    """Whether ``row`` is a user's outbox SRW keeps (its managed key's kind)."""
    return managed_kind(row) == CLOUD_OUTBOX


class CloudOutboxDriver(DatasourceDriver):
    def __init__(self) -> None:
        super().__init__(CLOUD_OUTBOX_SPEC)

    async def validate(
        self,
        draft: ConnectorDraft,
        *,
        existing: Mapping[str, Any] | None,
        ctx: ValidationContext,
    ) -> NormalizedConnector:
        """What SRW stores for a user's outbox: nothing but its name.

        A create is SRW's alone (``validate_platform_connector``, for a
        user it provisions); the API refuses one before it reaches a driver
        (``user_creatable``), and this refuses any other context too. A
        stored one takes a name or description edit only.
        """
        if existing is None and ctx.environment is not PLATFORM_ENVIRONMENT:
            raise HTTPException(
                status_code=400,
                detail="SRW creates a user's session outbox itself",
            )
        if draft.credentials:
            raise HTTPException(
                status_code=400,
                detail=(
                    "A session outbox holds no credentials; SRW uses the main "
                    "cloud's when it creates and mounts the folder"
                ),
            )
        if draft.connection_url:
            raise HTTPException(
                status_code=400, detail="A session outbox has no connection URL"
            )
        if draft.is_global:
            raise HTTPException(
                status_code=400,
                detail="Session outboxes cannot be published (is_global)",
            )
        if draft.config:
            raise HTTPException(
                status_code=400,
                detail=(
                    "A session outbox has no config: SRW creates its folder for "
                    "each session"
                ),
            )
        if existing is None:
            return NormalizedConnector(None, {}, {})
        return NormalizedConnector(None, None, None)

    def runtime_allowed(self, row: Mapping[str, Any], gates: DeploymentGates) -> bool:
        """Never: the folder is mounted into the workspace, and the agent
        gets neither an entry, nor tools, nor the main cloud's credential."""
        return False

    def bind(
        self, row: Mapping[str, Any], credentials: Any, *, ctx: BindContext
    ) -> dict[str, Any] | None:
        return None

    def effective_access(self, row: Mapping[str, Any]) -> str | None:
        """The level ``row`` binds at somewhere, or ``None`` if refused."""
        return bound_access(self, row).level

    def offered_access(
        self, row: Mapping[str, Any], ctx: AccessContext
    ) -> tuple[str, ...]:
        """The outbox's level where the provider offers it for ``ctx``.

        The provider is ``ctx.main_cloud_backend`` (any shipped provider for
        ``None``). A session's binding needs the tier the provider declares
        for mounting the outbox (``ctx.workspace_backend``; any for
        ``None``). A job's is SRW's copy at completion, made from the job's
        repository, so its tier narrows nothing: the provider only has to
        offer the outbox at all. A row with a config is no outbox SRW made.
        """
        from orchestrator.services.cloud import REGISTRY, provider_capabilities

        if row.get("config"):
            return ()
        if ctx.main_cloud_backend is None:
            declarations = [
                getattr(adapter, "capabilities", None) for adapter in REGISTRY.values()
            ]
        else:
            declarations = [provider_capabilities(ctx.main_cloud_backend)]
        tier = None if ctx.execution == "job" else ctx.workspace_backend
        offered = any(
            declared is not None
            and declared.offers(self.type_id, None, _LEVEL, workspace_backend=tier)
            for declared in declarations
        )
        return (_LEVEL,) if offered else ()

    def mount_binding(
        self,
        row: Mapping[str, Any],
        *,
        ctx: AccessContext,
        owner_id: str | None,
        session_folder: str | None,
        backend_instance_id: str | None = None,
        shared: bool = False,
    ) -> CloudOutboxMount | CloudOutboxUnavailable:
        """What a session's mount plan mounts for its attached outbox, or
        why it mounts nothing.

        ``owner_id`` is the session's owner, whose outbox alone it mounts.
        ``ctx.main_cloud_backend`` and ``backend_instance_id`` are the main
        cloud stamped on the session, ``session_folder`` the folder SRW
        created for it (``None``: none yet) and ``shared`` whether SRW
        shared that folder with the owner. The level comes before the
        folder: a tier the outbox is not offered on says so, even though
        SRW then never creates a folder for it.
        """
        connector_id = str(row.get("id") or "")
        if not is_outbox_row(row) or row.get("config"):
            return CloudOutboxUnavailable(
                connector_id,
                "config_invalid",
                "This connector is not a session outbox SRW created",
            )
        user = native_user(row)
        if user is None or owner_id is None or user != str(owner_id):
            return CloudOutboxUnavailable(
                connector_id,
                "not_eligible",
                "This session outbox belongs to another user",
            )
        if ctx.main_cloud_backend is None:
            return CloudOutboxUnavailable(
                connector_id,
                "backend_unavailable",
                "No main cloud serves this session",
            )
        access = bound_access(self, row, ctx=ctx)
        if access.level is None:
            return CloudOutboxUnavailable(
                connector_id,
                "access_unavailable",
                access.reason
                or "The main cloud does not offer a session outbox on this tier",
            )
        if not session_folder:
            return CloudOutboxUnavailable(
                connector_id,
                "no_session_folder",
                "SRW has not created this session's outbox folder",
            )
        return CloudOutboxMount(
            connector_id=connector_id,
            access=access.level,
            backend_id=ctx.main_cloud_backend,
            backend_instance_id=backend_instance_id,
            folder_handle=session_folder,
            shared=shared,
        )


__all__ = [
    "UNAVAILABLE_REASONS",
    "WORKSPACE_NAME",
    "CloudOutboxDriver",
    "CloudOutboxMount",
    "CloudOutboxUnavailable",
    "is_outbox_row",
]
