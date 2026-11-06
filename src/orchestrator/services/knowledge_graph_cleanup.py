"""Remove a deleted project's knowledge from the Neo4j graph.

Neo4j sits outside PostgreSQL, so deleting a project row cascades nothing into
it. Before this module existed nothing removed the graph side at all: an
agent-written note, its tags and its keywords outlived their project forever
(dev, 11-06: 82 notes of 12 deleted projects).

Two paths remove it now. The project delete does it right after the project
row is gone, so a delete that is refused leaves the graph alone. A
leader-gated sweep removes whatever that could not: a delete while Neo4j was
unreachable, a connector note projected after the delete, and every project
deleted before this cleanup existed.

The graph is optional. Without one, both paths do nothing.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from typing import Any

logger = logging.getLogger(__name__)

#: How often the leader sweeps. Leftovers are only clutter: reads are scoped
#: to a project, and a deleted project's id never comes back.
SWEEP_SECONDS = 6 * 60 * 60


async def delete_project_graph_knowledge(project_id: str, *, graph: Any) -> int | None:
    """Delete a project's graph knowledge; return the node count, or None.

    None means nothing was deleted: there is no graph, or Neo4j failed. A
    failure is logged and left to the sweep. It never fails the delete, whose
    project row is already gone.
    """
    kg = graph.get()
    if kg is None:
        return None
    try:
        return await asyncio.to_thread(kg.delete_project_knowledge, project_id)
    except Exception as exc:
        logger.warning(
            "Project %s deleted, but its knowledge graph nodes were not (%s); "
            "the orphan sweep removes them",
            project_id,
            type(exc).__name__,
        )
        return None


async def sweep_orphaned_graph_knowledge(*, store: Any, graph: Any) -> int:
    """Delete the graph knowledge of every project that no longer exists.

    Returns how many projects were swept.
    """
    kg = graph.get()
    if kg is None:
        return 0
    # The graph is read before the projects table. A project row exists
    # before any node carries its id, so an id missing from the later read
    # belongs to a deleted project, never to one created in between.
    graph_ids = await asyncio.to_thread(kg.knowledge_project_ids)
    candidates: dict[str, uuid.UUID] = {}
    for raw in graph_ids:
        try:
            candidates[raw] = uuid.UUID(raw)
        except ValueError:
            continue  # not a project id, so nothing this sweep can judge
    if not candidates:
        return 0
    async with store.acquire() as conn:
        # Every installation has at least one project (each user's default
        # one). An empty table means the database is not the one the graph
        # belongs to (a restore in progress, a wrong DSN), and sweeping
        # against it would delete everything.
        if not await conn.fetchval("SELECT EXISTS (SELECT 1 FROM projects)"):
            logger.warning(
                "Knowledge graph orphan sweep skipped: the projects table is empty"
            )
            return 0
        rows = await conn.fetch(
            "SELECT id FROM projects WHERE id = ANY($1::uuid[])",
            list(candidates.values()),
        )
    # Compared as UUIDs, whatever type the driver returns: a mismatch here
    # would make every project look deleted.
    live = {uuid.UUID(str(row["id"])) for row in rows}
    swept = 0
    for raw, project_uuid in candidates.items():
        if project_uuid in live:
            continue
        count = await asyncio.to_thread(kg.delete_project_knowledge, raw)
        logger.info(
            "Removed %d knowledge graph nodes of deleted project %s", count, raw
        )
        swept += 1
    return swept


async def orphaned_graph_knowledge_sweep_loop(
    shutdown_event: asyncio.Event,
    *,
    store: Any,
    graph: Any,
    interval_seconds: float = SWEEP_SECONDS,
) -> None:
    """Leader-gated loop: a sweep when leadership starts, then every interval.

    A failed pass is logged and the next one runs on time. Returns only on
    shutdown, because ``run_when_leader`` re-creates a loop that returns.
    """
    while not shutdown_event.is_set():
        try:
            await sweep_orphaned_graph_knowledge(store=store, graph=graph)
        except Exception as exc:
            logger.warning(
                "Knowledge graph orphan sweep failed (%s); next pass in %.0fs",
                type(exc).__name__,
                interval_seconds,
            )
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=interval_seconds)
            break
        except asyncio.TimeoutError:
            pass
