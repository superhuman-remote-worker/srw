"""Canonical inventory of connector types shipped by SRW.

The public product calls these resources connectors; ``datasource`` remains
the internal API and database term. Keep this module descriptive and static:
runtime availability, user grants, and attachment state belong to the later
capability resolver rather than this build-level inventory.

The inventory is derived from the built-in driver specs in
``shared.connectors``, the one place a connector type is declared; this
module keeps its historical names for existing importers.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from shared.connectors.builtin import DATASOURCE_SPECS, PLATFORM_SPECS, tool_map_entry
from shared.connectors.contract import DriverSpec

DatasourceRuntimeKind = Literal[
    "generic_env",
    "repository",
    "knowledge",
    "managed_tools",
    "email_tools",
    "mcp_tools",
    "credential_file",
    "ssh_identity",
    "cloud_mount",
]


@dataclass(frozen=True, slots=True)
class DatasourceTypeDefinition:
    """One supported internal datasource type and its guide coverage decision."""

    type_id: str
    title: str
    guide_topic: str
    runtime_kind: DatasourceRuntimeKind


_RUNTIME_KIND_BY_FORM: dict[str, DatasourceRuntimeKind] = {
    "env_file": "generic_env",
    "checkout": "repository",
    "knowledge_index": "knowledge",
    "managed_connection": "managed_tools",
    "mcp_client": "mcp_tools",
    "credential_file": "credential_file",
    "ssh_identity": "ssh_identity",
    "cloud_mount": "cloud_mount",
}


def _runtime_kind(spec: DriverSpec) -> DatasourceRuntimeKind:
    kind = _RUNTIME_KIND_BY_FORM[spec.delivery_forms[0]]
    # Tier-keyed managed tools (email) were catalogued as their own kind.
    if kind == "managed_tools" and "tiers" in tool_map_entry(spec):
        return "email_tools"
    return kind


def _definitions(specs: tuple[DriverSpec, ...]) -> tuple[DatasourceTypeDefinition, ...]:
    return tuple(
        DatasourceTypeDefinition(
            spec.legacy_type, spec.title, spec.guide_topic or "", _runtime_kind(spec)
        )
        for spec in specs
        if spec.legacy_type
    )


#: The types a user creates a connector of: the connector catalogue.
DATASOURCE_TYPE_CATALOG: tuple[DatasourceTypeDefinition, ...] = _definitions(
    DATASOURCE_SPECS
)
#: The types only SRW creates, for what it provisions (a project's cloud
#: folder, a ``cloud_mount`` the workspace delivers). No create form, API
#: type list or agent sees them as a connector type.
PLATFORM_TYPE_CATALOG: tuple[DatasourceTypeDefinition, ...] = _definitions(
    PLATFORM_SPECS
)

DATASOURCE_TYPE_IDS: tuple[str, ...] = tuple(
    definition.type_id for definition in DATASOURCE_TYPE_CATALOG
)
DATASOURCE_TYPES: frozenset[str] = frozenset(DATASOURCE_TYPE_IDS)

if len(DATASOURCE_TYPE_IDS) != len(DATASOURCE_TYPES):
    raise RuntimeError("Duplicate type_id in DATASOURCE_TYPE_CATALOG")
if DATASOURCE_TYPES & {definition.type_id for definition in PLATFORM_TYPE_CATALOG}:
    raise RuntimeError(
        "A type is in both DATASOURCE_TYPE_CATALOG and PLATFORM_TYPE_CATALOG"
    )


__all__ = [
    "DATASOURCE_TYPE_CATALOG",
    "DATASOURCE_TYPE_IDS",
    "DATASOURCE_TYPES",
    "PLATFORM_TYPE_CATALOG",
    "DatasourceRuntimeKind",
    "DatasourceTypeDefinition",
]
