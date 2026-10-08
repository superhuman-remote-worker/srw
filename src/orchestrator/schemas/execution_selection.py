"""``execution.connectors``: the Connectors a creation request selects by ref.

The shape is the manifest standard's ``ConnectorMap``
(``src/shared/manifests/schema.json``): an alias per entry, each one
``{"ref": …}``. A ref names a Connector resource by ``name`` in a ``scope``
(omitted: the execution's own scope, its Project when it has one, else the
caller's Account; ``me`` names the caller's Account), or by ``uid``, the
convention ``instanceRef`` uses. A datasource's Connector has the datasource's
id as its uid, so a ``uid`` ref and a ``datasource_ids`` entry name the same
connector.

Structure is validated here. ``orchestrator.services.connector_refs`` turns
the refs into datasource ids at admission, and the ordinary connector policy
authorizes them exactly as it authorizes ``datasource_ids``. An inline
Connector has no datasource row and no sharing policy, so it is refused until
a driver can bind an inline definition.
"""

from __future__ import annotations

from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, model_validator

#: The manifest ``Name`` (an alias, a resource name, a scope name).
ResourceName = Annotated[
    str,
    StringConstraints(pattern=r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$", max_length=63),
]

INLINE_CONNECTOR_REFUSED = (
    "An inline connector cannot be attached to a job or session yet: it has no "
    "connector row or sharing policy. Create the connector, then reference it."
)


class ConnectorScope(BaseModel):
    """A manifest scope. Live Account and Project scopes are named by UUID;
    ``me`` (or ``personal``) is the caller's Account."""

    model_config = ConfigDict(extra="forbid")

    kind: Literal["Account", "Project", "Catalog"]
    name: ResourceName


class ConnectorRef(BaseModel):
    """A Connector resource by ``name`` (and ``scope``) or by ``uid``.

    The connector is bound live, as ``datasource_ids`` bind it, so a ref
    never pins a revision.
    """

    model_config = ConfigDict(extra="forbid")

    name: ResourceName | None = Field(
        None, description="The Connector resource's metadata.name."
    )
    scope: ConnectorScope | None = Field(
        None,
        description=(
            "Where the named Connector lives. Omit for the execution's scope: "
            "its Project, else the caller's Account."
        ),
    )
    uid: UUID | None = Field(
        None,
        description="The Connector resource's uid, which is its connector id.",
    )

    @model_validator(mode="after")
    def _one_identity(self) -> "ConnectorRef":
        if (self.name is None) == (self.uid is None):
            raise ValueError("A connector ref names exactly one of name or uid.")
        if self.uid is not None and self.scope is not None:
            raise ValueError("A uid ref has no scope; the uid is the identity.")
        return self


class ConnectorSelection(BaseModel):
    """One ``ConnectorMap`` entry: ``{"ref": …}``."""

    model_config = ConfigDict(extra="forbid")

    ref: ConnectorRef | None = None
    inline: dict[str, Any] | None = Field(
        None, description="Refused: create the connector and reference it."
    )

    @model_validator(mode="after")
    def _ref_only(self) -> "ConnectorSelection":
        if self.inline is not None:
            raise ValueError(INLINE_CONNECTOR_REFUSED)
        if self.ref is None:
            raise ValueError("A connector selection needs a ref.")
        return self


class ExecutionSelection(BaseModel):
    """The ``execution`` block of a job or session request.

    Only ``connectors`` is accepted here for now; the Expert and the
    workspace are the request's own ``expert`` and ``workspace`` fields.
    """

    model_config = ConfigDict(extra="forbid")

    connectors: dict[ResourceName, ConnectorSelection] = Field(
        ...,
        description=(
            "Connectors to attach, by alias: each a ref to a Connector "
            "resource. Mutually exclusive with datasource_ids and "
            "use_datasource_defaults. An empty map attaches none, like "
            "datasource_ids: []."
        ),
    )


__all__ = [
    "INLINE_CONNECTOR_REFUSED",
    "ConnectorRef",
    "ConnectorScope",
    "ConnectorSelection",
    "ExecutionSelection",
]
