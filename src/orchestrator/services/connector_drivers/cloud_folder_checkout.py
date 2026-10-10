"""``srw.cloud-folder-checkout/v1``: a project's cloud folder, copied into a job.

Today's "Mode A" as a connector (main_cloud_as_connectors.md, slice 6.3):
one connector per project (managed key ``project-cloud-checkout:<project>``),
jobs only. SRW copies the project's folder into the job's repository at
``projects/<slug>/`` before the job is dispatched
(``job_cloud_baseline.seed_project_folder_baseline``); the agent works on the
copy like any file in its repository. The level says what happens to the
copy's changes, which only SRW ever applies:

* ``read_only``: never. Nothing writes the cloud folder;
* ``reviewed_write_back``: at completion the changes become a diff the job's
  owner reviews (``capture_checkout_diff``), and only an accepted diff is
  written back (``job_diff_review``).

Only SRW creates a checkout connector (``user_creatable`` is off). Its config
names the project and nothing else: the folder is the project row's, and the
credential that copies it is the main cloud's. It never reaches the agent as a
connector (:meth:`CloudFolderCheckoutDriver.runtime_allowed`); what the job is
told is ``context.cloud_connectors`` (``shared.job_cloud_connectors``).

Which levels a connector is offered is its provider's support matrix
(``orchestrator.services.cloud.capabilities``): the provider is the
execution's (``AccessContext.main_cloud_backend``, the project's main cloud),
since the row names none.

Design: knowledge-base/knowledge/features/main_cloud_as_connectors.md,
"Overnight run: slices 3 to 8" (Mode A becomes ``cloud_folder_checkout``).
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
from orchestrator.services.connector_drivers.platform import PLATFORM_ENVIRONMENT
from shared.connectors.builtin import CLOUD_FOLDER_CHECKOUT_SPEC
from shared.connectors.platform import (
    PROJECT_CLOUD_CHECKOUT,
    managed_key,
    platform_owned,
)

#: The level whose changes SRW never applies, and the one whose accepted
#: diff it writes back.
READ_ONLY = "read_only"
REVIEWED_WRITE_BACK = "reviewed_write_back"
#: The matrix's folder kind: a checkout is always a project's folder.
_FOLDER_KIND = "project"
#: Why a connector copies nothing, as closed codes: its level is refused
#: here (``detail`` says why), or its row is not the job's project's own
#: checkout SRW created.
UNAVAILABLE_REASONS: tuple[str, ...] = ("access_unavailable", "config_invalid")


def checkout_key(project_id: Any) -> str:
    """The managed key of ``project_id``'s own checkout connector."""
    return managed_key(PROJECT_CLOUD_CHECKOUT, project_id)


def normalize_checkout_config(config: Any) -> dict[str, Any]:
    """The config a checkout connector stores (``{project_id}``), or
    ``ValueError``."""
    if not isinstance(config, Mapping):
        raise ValueError("A project folder copy connector's config must be an object")
    unknown = sorted(set(config) - {"project_id"})
    if unknown:
        raise ValueError(
            f"Unknown project folder copy config field(s): {', '.join(unknown)}"
        )
    try:
        return {"project_id": str(UUID(str(config.get("project_id") or "")))}
    except ValueError:
        raise ValueError(
            "A project folder copy connector names its project (project_id)"
        ) from None


@dataclass(frozen=True)
class CloudFolderCheckout:
    """One attached checkout connector, as a job's admission records it
    (``context.cloud_checkout``): which connector, and its level."""

    connector_id: str
    project_id: str
    access: str

    def context(self) -> dict[str, Any]:
        return {"datasource_id": self.connector_id, "access": self.access}


@dataclass(frozen=True)
class CloudFolderCheckoutUnavailable:
    """A checkout connector that copies nothing, and why. ``reason`` is one
    of :data:`UNAVAILABLE_REASONS`; nothing takes the copy's place."""

    connector_id: str
    reason: str
    detail: str


class CloudFolderCheckoutDriver(DatasourceDriver):
    def __init__(self) -> None:
        super().__init__(CLOUD_FOLDER_CHECKOUT_SPEC)

    async def validate(
        self,
        draft: ConnectorDraft,
        *,
        existing: Mapping[str, Any] | None,
        ctx: ValidationContext,
    ) -> NormalizedConnector:
        """What SRW stores for a project's checkout: the project, and the
        name or description edits a stored one takes.

        A create is SRW's alone (``validate_platform_connector``, when it
        provisions a project's cloud folder); the API refuses one before it
        reaches a driver (``user_creatable``), and this refuses any other
        context too.
        """
        if existing is None and ctx.environment is not PLATFORM_ENVIRONMENT:
            raise HTTPException(
                status_code=400,
                detail="SRW creates a project's folder copy connector itself",
            )
        if draft.credentials:
            raise HTTPException(
                status_code=400,
                detail=(
                    "A project folder copy holds no credentials; SRW uses the "
                    "main cloud's when it copies the folder"
                ),
            )
        if draft.connection_url:
            raise HTTPException(
                status_code=400,
                detail="A project folder copy has no connection URL",
            )
        if draft.is_global:
            raise HTTPException(
                status_code=400,
                detail="Project folder copies cannot be published (is_global)",
            )
        if existing is None:
            return NormalizedConnector(None, self._config(draft.config), {})
        if draft.config is not None and self._config(draft.config) != (
            self._config(existing.get("config"))
        ):
            raise HTTPException(
                status_code=400,
                detail="SRW sets which project a folder copy connector names",
            )
        return NormalizedConnector(None, None, None)

    @staticmethod
    def _config(config: Any) -> dict[str, Any]:
        try:
            return normalize_checkout_config(config)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    def runtime_allowed(self, row: Mapping[str, Any], gates: DeploymentGates) -> bool:
        """Never: the copy is files in the job's repository, and the agent
        gets neither an entry, nor tools, nor the main cloud's credential."""
        return False

    def bind(
        self, row: Mapping[str, Any], credentials: Any, *, ctx: BindContext
    ) -> dict[str, Any] | None:
        return None

    def effective_access(self, row: Mapping[str, Any]) -> str | None:
        """The level ``row`` binds at somewhere, or ``None`` if refused."""
        return bound_access(self, row, ctx=AccessContext(execution="job")).level

    def offered_access(
        self, row: Mapping[str, Any], ctx: AccessContext
    ) -> tuple[str, ...]:
        """The levels the provider declares for a project folder's checkout
        on ``ctx``'s tier (any tier for ``None``), lowest rank first.

        The provider is ``ctx.main_cloud_backend``; ``None`` offers what any
        shipped provider offers. A session is never offered one (the spec's
        ``executions``), nor is a row that names no project.
        """
        from orchestrator.services.cloud import REGISTRY, provider_capabilities

        if ctx.execution == "session":
            return ()
        try:
            normalize_checkout_config(row.get("config"))
        except ValueError:
            return ()
        if ctx.main_cloud_backend is None:
            declarations = [
                getattr(adapter, "capabilities", None) for adapter in REGISTRY.values()
            ]
        else:
            declarations = [provider_capabilities(ctx.main_cloud_backend)]
        return tuple(
            level
            for level in self.spec.ranked_access_ids()
            if any(
                declared is not None
                and declared.offers(
                    self.type_id,
                    _FOLDER_KIND,
                    level,
                    workspace_backend=ctx.workspace_backend,
                )
                for declared in declarations
            )
        )

    def checkout_binding(
        self,
        row: Mapping[str, Any],
        *,
        project_id: str,
        ctx: AccessContext,
        requested: str | None = None,
    ) -> CloudFolderCheckout | CloudFolderCheckoutUnavailable:
        """The level a job of ``project_id`` binds its attached checkout row
        at, or why it gets no copy.

        Only the job's own project's checkout copies anything: a row with
        its managed key, naming that project. ``requested`` is a level the
        job asks for; without one the row binds at its highest offered
        level, and read-only when its project link or its creator's tag
        says so (the strict rule: a read-only binding never gets the
        reviewed level).
        """
        connector_id = str(row.get("id") or "")
        try:
            config = normalize_checkout_config(row.get("config"))
        except ValueError as exc:
            return CloudFolderCheckoutUnavailable(
                connector_id, "config_invalid", str(exc)
            )
        try:
            own = checkout_key(project_id)
        except ValueError:
            own = None
        if (
            platform_owned(row) != own
            or own is None
            or config["project_id"] != str(UUID(str(project_id)))
        ):
            return CloudFolderCheckoutUnavailable(
                connector_id,
                "config_invalid",
                "This project folder copy connector is not the job's project's own",
            )
        access = bound_access(self, row, ctx=ctx, requested=requested)
        if access.level is None:
            return CloudFolderCheckoutUnavailable(
                connector_id,
                "access_unavailable",
                access.reason or "No level of the project folder copy is offered here",
            )
        return CloudFolderCheckout(
            connector_id=connector_id,
            project_id=config["project_id"],
            access=access.level,
        )


__all__ = [
    "READ_ONLY",
    "REVIEWED_WRITE_BACK",
    "UNAVAILABLE_REASONS",
    "CloudFolderCheckout",
    "CloudFolderCheckoutDriver",
    "CloudFolderCheckoutUnavailable",
    "checkout_key",
    "normalize_checkout_config",
]
