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
from typing import Any

from orchestrator.services.connector_drivers.base import (
    DatasourceDriver,
    SupportsOfferedAccess,
)
from shared.connectors.builtin import spec_for_row
from shared.connectors.contract import (
    AccessContext,
    bound_read_only,
    resolve_access,
    strict_access,
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


__all__ = ["access_choice", "strict_spec_row"]
