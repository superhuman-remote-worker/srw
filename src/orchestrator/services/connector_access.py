"""Which access level an execution binds each of its connectors at.

Main cloud slice 5. A connector whose driver has strict levels (a cloud
folder: ``read_only``, ``protected``, ``read_write``) is bound at a level the
execution chooses within what its driver offers it on the execution's
workspace tier (``SupportsOfferedAccess``), below a ceiling the owner's role
sets (a project viewer's folder: ``read_only``,
``datasource_policy.access_ceiling_for``), with the project link's default
(``project_datasources.access``) for one it does not choose. The rule is the
contract's (``shared.connectors.contract.resolve_access``): a chosen level
binds as chosen or is refused, a default is only ever lowered.

:func:`access_choice` is what the picker shows for one connector: every
level with whether it may be chosen here and why not, and the level it gets
unchosen.

Design: knowledge-base/knowledge/features/main_cloud_as_connectors.md,
"Access levels", "What people see" and "Overnight run: slices 3 to 8".
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from orchestrator.services.connector_drivers.base import (
    DatasourceDriver,
    SupportsOfferedAccess,
    bound_access,
)
from orchestrator.services.connector_drivers.registry import (
    ConnectorDriverRegistry,
    builtin_connector_drivers,
)
from shared.connectors.builtin import spec_for_row
from shared.connectors.contract import (
    AccessContext,
    bound_read_only,
    resolve_access,
    strict_access,
)
from shared.connectors.platform import native_cloud_project

#: The reviewed level of a cloud folder, which makes a session protected.
PROTECTED = "protected"


class ConnectorAccessError(ValueError):
    """A level an execution asked for cannot be bound; ``detail`` is the
    API's 422 body (``code``, ``message``, and the connector and level)."""

    def __init__(self, detail: dict[str, Any]) -> None:
        super().__init__(detail.get("message") or detail.get("code"))
        self.detail = detail


@dataclass(frozen=True)
class SelectionAccess:
    """The level of every strict connector of one selection.

    ``levels`` maps each such connector to the level it binds at (``None``:
    a default no level was allowed for; it mounts nothing and says why).
    ``origins`` is the audit record per connector (``access``, ``origin``
    and, when lowered or refused, ``reason``). ``protected`` is the one
    connector bound at ``protected``, or ``None``.
    """

    levels: dict[str, str | None] = field(default_factory=dict)
    origins: dict[str, dict[str, Any]] = field(default_factory=dict)
    protected: str | None = None


def _registry(store: Any) -> ConnectorDriverRegistry:
    registry = getattr(store, "connector_drivers", None)
    if isinstance(registry, ConnectorDriverRegistry):
        return registry
    return builtin_connector_drivers()


async def strict_selection_rows(
    store: Any, datasource_ids: list[str], project_ids: list[str]
) -> list[dict[str, Any]]:
    """The selection's strict connectors as the thread resolver reads them
    (``project_read_only``, ``project_access``), with their managed key."""
    if not datasource_ids:
        return []
    rows = await store.resolve_datasources_for_thread(
        datasource_ids=datasource_ids, project_ids=project_ids
    )
    strict: list[dict[str, Any]] = []
    for row in rows:
        if not strict_spec_row(row):
            continue
        if "managed_key" not in row:
            full = await store.get_datasource(str(row["id"]))
            if full is None:
                continue
            row = {
                **full,
                "project_read_only": row.get("project_read_only"),
                "project_access": row.get("project_access"),
            }
        strict.append(dict(row))
    return strict


async def resolve_selection_access(
    store: Any,
    *,
    owner_id: str | None,
    datasource_ids: list[str],
    project_ids: list[str],
    requested: Mapping[str, str],
    ctx: AccessContext,
    keep: Mapping[str, str | None] | None = None,
    rows: list[dict[str, Any]] | None = None,
    keep_origins: Mapping[str, Any] | None = None,
) -> SelectionAccess:
    """Resolve the level of every strict connector of an authorized
    selection, or raise :class:`ConnectorAccessError`.

    ``requested`` maps connector ids to the level the execution asks for;
    each must be a strict connector of the selection
    (``connector_access_not_selected``) and is bound as asked or refused
    (``connector_access_unavailable``, with the rule's closed reason).
    ``keep`` maps connectors to a level resolved before (a live edit): kept
    as it is, not resolved again, with its origin record from
    ``keep_origins`` (the session's ``datasource_selection.access``) when it
    has one, so a kept default stays a default an attach resolves again,
    and a kept choice a choice (``materialized`` when it is not known). Any
    other connector gets its default
    (``resolve_access``): the project link's, lowered within what ``ctx``
    offers and the owner's ceiling (a viewer of a cloud folder's project),
    or no level at all. More than one connector at ``protected`` is refused
    (``protected_cloud_single_folder``). ``rows`` are :func:`strict_selection_rows` when the caller read them.
    """
    from orchestrator.services.datasource_policy import access_ceiling_for

    if not datasource_ids:
        if requested:
            raise _not_selected(next(iter(requested)))
        return SelectionAccess()
    if rows is None:
        rows = await strict_selection_rows(store, datasource_ids, project_ids)
    by_id = {str(row["id"]): row for row in rows}
    for datasource_id in requested:
        if datasource_id not in by_id:
            raise _not_selected(datasource_id)
    cloud_projects = sorted(
        {project for row in rows if (project := native_cloud_project(row)) is not None}
    )
    roles: dict[str, str] = (
        await store.get_user_project_roles(owner_id, cloud_projects)
        if owner_id is not None and cloud_projects
        else {}
    )
    registry = _registry(store)
    result = SelectionAccess()
    for datasource_id in datasource_ids:
        row = by_id.get(datasource_id)
        if row is None or datasource_id in result.levels:
            continue
        if (
            keep is not None
            and datasource_id in keep
            and datasource_id not in requested
        ):
            result.levels[datasource_id] = keep[datasource_id]
            previous = (keep_origins or {}).get(datasource_id)
            result.origins[datasource_id] = (
                dict(previous)
                if isinstance(previous, Mapping)
                and previous.get("access") == keep[datasource_id]
                and previous.get("origin")
                else {"access": keep[datasource_id], "origin": "materialized"}
            )
            continue
        driver = registry.for_type(row.get("type"))
        if driver is None:
            continue
        ceiling = access_ceiling_for(row, roles.get(native_cloud_project(row) or ""))
        level = requested.get(datasource_id)
        resolved = bound_access(driver, row, ctx=ctx, requested=level, ceiling=ceiling)
        if level is not None and resolved.refused:
            raise ConnectorAccessError(
                {
                    "code": "connector_access_unavailable",
                    "message": resolved.reason
                    or f"The access level {level!r} is not available here",
                    "datasource_id": datasource_id,
                    "access": level,
                    "reason": resolved.code,
                }
            )
        result.levels[datasource_id] = resolved.level
        record: dict[str, Any] = {"access": resolved.level, "origin": resolved.origin}
        if resolved.reason:
            record["reason"] = resolved.reason
        if resolved.code:
            record["code"] = resolved.code
        result.origins[datasource_id] = record
    protected = [cid for cid, level in result.levels.items() if level == PROTECTED]
    if len(protected) > 1:
        raise ConnectorAccessError(
            {
                "code": "protected_cloud_single_folder",
                "message": (
                    "A session can have one protected cloud folder; "
                    f"{len(protected)} are set to protected"
                ),
                "datasource_ids": protected,
            }
        )
    return SelectionAccess(
        levels=result.levels,
        origins=result.origins,
        protected=protected[0] if protected else None,
    )


def _not_selected(datasource_id: str) -> ConnectorAccessError:
    return ConnectorAccessError(
        {
            "code": "connector_access_not_selected",
            "message": (
                "datasource_access names a connector that is not in the "
                "session's selection or has no access levels"
            ),
            "datasource_id": datasource_id,
        }
    )


def strict_spec_row(row: Mapping[str, Any]) -> bool:
    """Whether ``row``'s driver binds it at a strict level an execution may
    choose (``strict_access``)."""
    spec = spec_for_row(row)
    return spec is not None and strict_access(spec)


def access_choice(
    row: Mapping[str, Any],
    driver: DatasourceDriver | None,
    ctx: AccessContext,
    *,
    ceiling: str | None = None,
) -> dict[str, Any] | None:
    """The levels an execution in ``ctx`` may choose for ``row``, or
    ``None`` for a connector whose driver has no strict levels.

    ``levels`` lists every level of the driver, lowest first, each with
    ``offered`` (it may be chosen here) and, when not, ``reason`` (a closed
    code of ``ACCESS_REFUSALS``: ``level_not_offered`` off this tier or
    provider, ``above_ceiling`` above the owner's ceiling or a read-only
    link) and ``detail``. ``default`` is the level an execution that names
    none gets, ``default_origin`` where it came from (``link_default``,
    ``driver_default``, ``clamped``...) and ``default_reason`` why it was
    lowered or refused; ``default`` ``None`` mounts nothing. ``ceiling`` is
    the owner's ceiling, ``None`` for none.
    """
    spec = driver.spec if driver is not None else spec_for_row(row)
    if spec is None or not strict_access(spec):
        return None
    offered = (
        driver.offered_access(row, ctx)
        if isinstance(driver, SupportsOfferedAccess)
        else None
    )
    entry = {**row, "project_read_only": bound_read_only(row)}
    link_default = row.get("project_access")

    def resolved(level: str | None):
        return resolve_access(
            entry,
            spec,
            offered=offered,
            requested=level,
            link_default=str(link_default) if link_default else None,
            ceiling=ceiling,
        )

    levels = []
    for level in sorted(spec.access_levels, key=lambda lv: lv.rank):
        chosen = resolved(level.id)
        levels.append(
            {
                "id": level.id,
                "rank": level.rank,
                "writes": level.writes,
                "enforced_by": level.enforced_by,
                "offered": not chosen.refused,
                "reason": chosen.code,
                "detail": chosen.reason,
            }
        )
    default = resolved(None)
    return {
        "levels": levels,
        "default": default.level,
        "default_origin": default.origin,
        "default_reason": default.reason,
        "ceiling": ceiling,
    }


__all__ = [
    "PROTECTED",
    "ConnectorAccessError",
    "SelectionAccess",
    "access_choice",
    "resolve_selection_access",
    "strict_selection_rows",
    "strict_spec_row",
]
