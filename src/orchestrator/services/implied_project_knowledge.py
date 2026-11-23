"""A project's knowledge base for work that was never offered its connector.

The rule on the agent: a knowledge base is bound only when its connector is
delivered (``agent.services.knowledge.bindings.build_knowledge_bindings``). A
job or session that names its connectors and leaves the project's own
knowledge base out therefore runs without it.

That rule presumes the connector could have been chosen. Two kinds of work
never had the choice, because a project's knowledge base has had a connector
row only since the knowledge-base repository split, and some projects still
have none:

- work in a project that has no knowledge-base connector at all;
- work created before its project's knowledge-base connector existed, whose
  stored selection therefore cannot contain it.

Before the rule, every such job and session had its project's knowledge base.
It keeps it: at delivery, the project's knowledge base is added to the
connector entries the agent receives, marked ``implied``. Nothing is stored;
the stored selection stays what the owner chose. Work created once the
connector exists is never touched here, so there an explicit selection without
it means no knowledge base
(knowledge-base/knowledge/issues/project_knowledge_base_is_bound_without_its_connector.md).
"""

from __future__ import annotations

import inspect
import logging
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping, Sequence

from shared.connectors.platform import project_kb_key
from shared.native_kb import NATIVE_PROJECT_CONFIG_KEY, native_kb_project_id

logger = logging.getLogger(__name__)

#: Entry key naming why a knowledge base the selection does not hold is
#: delivered anyway.
IMPLIED_KEY = "implied"
NO_CONNECTOR = "project_has_no_knowledge_connector"
PREDATES_CONNECTOR = "work_predates_knowledge_connector"

_DEFAULT_NAME = "Project Knowledge"
_DEFAULT_ROOT = "knowledge"


def _aware(value: Any) -> datetime | None:
    if not isinstance(value, datetime):
        return None
    return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


async def _connector_of(store: Any, project_id: str) -> Any:
    """The project's own knowledge-base connector row, ``None`` when it has
    none, or something that is neither (a store that cannot say).

    Found by its managed key (``project-kb:<project>``), which every such row
    carries: creation writes it and startup stamps older rows.
    """
    getter = getattr(store, "get_datasource_by_managed_key", None)
    if not inspect.iscoroutinefunction(getter):
        return NotImplemented
    try:
        key = project_kb_key(project_id)
    except (TypeError, ValueError):
        return NotImplemented  # not a project id: nothing to decide
    return await getter(key)


def _reason(row: Any, created_at: Any) -> str | None:
    """Why ``row``'s knowledge base is implied for work created at
    ``created_at``, or ``None`` when the selection decides."""
    if row is None:
        return NO_CONNECTOR
    if not isinstance(row, Mapping):
        return None  # the store could not say: the selection decides
    work_at, row_at = _aware(created_at), _aware(row.get("created_at"))
    if work_at is not None and row_at is not None and work_at < row_at:
        return PREDATES_CONNECTOR
    return None


def _entry(project_id: str, row: Any, reason: str) -> dict[str, Any]:
    """The entry the knowledge-base driver would deliver for the project's own
    row (``KnowledgeBaseDriver.bind``), built without one."""
    known = row if isinstance(row, Mapping) else {}
    config = known.get("config")
    root = config.get("root_path") if isinstance(config, Mapping) else None
    return {
        "type": "kb",
        "name": str(known.get("name") or _DEFAULT_NAME),
        "description": known.get("description"),
        "connection_url": None,
        "credentials": {},
        "project_read_only": True,
        "datasource_id": str(known["id"]) if known.get("id") else None,
        "config": {
            "root_path": str(root or _DEFAULT_ROOT),
            NATIVE_PROJECT_CONFIG_KEY: project_id,
        },
        IMPLIED_KEY: reason,
    }


async def with_implied_project_knowledge(
    datasources: Sequence[dict[str, Any]] | None,
    *,
    store: Any,
    project_ids: Iterable[Any] | None,
    created_at: Any,
) -> list[dict[str, Any]] | None:
    """``datasources`` plus the knowledge base of each of ``project_ids`` that
    the work was never offered as a connector.

    ``created_at`` is when the job or session was created. A project whose
    knowledge base is already among the entries is left alone, as is every
    project whose connector existed by then. Returns the input unchanged
    (the same object) when nothing is implied.
    """
    projects = [str(pid) for pid in (project_ids or []) if pid]
    if not projects:
        return datasources  # type: ignore[return-value]

    entries = list(datasources or [])
    delivered = {native_kb_project_id(entry) for entry in entries}
    implied: list[dict[str, Any]] = []
    for project_id in dict.fromkeys(projects):
        if project_id in delivered:
            continue
        row = await _connector_of(store, project_id)
        if row is NotImplemented:
            continue
        reason = _reason(row, created_at)
        if reason is None:
            continue
        implied.append(_entry(project_id, row, reason))
        logger.debug(
            "Project %s knowledge base delivered without its connector (%s)",
            project_id,
            reason,
        )
    if not implied:
        return datasources  # type: ignore[return-value]
    return entries + implied


__all__ = [
    "IMPLIED_KEY",
    "NO_CONNECTOR",
    "PREDATES_CONNECTOR",
    "with_implied_project_knowledge",
]
