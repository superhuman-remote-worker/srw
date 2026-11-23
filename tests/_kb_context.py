"""A mock tool context whose knowledge bases are attached.

The knowledge tools read and write only the knowledge bases bound to the
runtime (``ToolContext.knowledge_bindings``); a project id alone names none
(``build_knowledge_bindings``). Tests that drive the tools through a
``MagicMock`` context describe their scope the short way, with ``project_id``
and ``project_ids``. This context turns that into the bindings a runtime with
those projects' knowledge-base connectors attached would hold, and follows
later assignments (``ctx.project_ids = []`` detaches them all).
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Iterable, List, Optional
from unittest.mock import AsyncMock, MagicMock

from agent.services.knowledge.bindings import (
    NATIVE_PROJECT_CONFIG_KEY,
    KnowledgeBinding,
)


def project_kb_connector(project_id: Any, **overrides: Any) -> dict:
    """A project's own knowledge-base connector, as a runtime receives it.

    ``build_knowledge_bindings`` binds a project's knowledge base only when
    this row is among the delivered connectors.
    """
    row = {
        "type": "kb",
        "datasource_id": str(uuid.uuid4()),
        "name": "Project Knowledge",
        "config": {
            "root_path": "knowledge",
            NATIVE_PROJECT_CONFIG_KEY: str(project_id),
        },
    }
    row.update(overrides)
    return row


def attached_project_bindings(
    project_id: Any, project_ids: Optional[Iterable[Any]]
) -> List[KnowledgeBinding]:
    """The bindings of a runtime whose projects' own knowledge bases are all
    attached: one native binding per project, the primary one writable.

    Without a primary ``project_id`` nothing is writable, as in a runtime
    whose own project's knowledge base is not attached.
    """
    if project_ids is None:
        ids = [project_id] if project_id else []
    else:
        ids = list(project_ids)
    bindings: List[KnowledgeBinding] = []
    for index, raw_id in enumerate(ids):
        try:
            kb_id = uuid.UUID(str(raw_id))
        except (TypeError, ValueError):
            continue
        primary = index == 0
        bindings.append(
            KnowledgeBinding(
                kb_id=kb_id,
                alias="project" if primary else f"project-{kb_id.hex[:8]}",
                name="Project Knowledge"
                if primary
                else f"Project Knowledge {kb_id.hex[:8]}",
                kind="native",
                writable=primary and bool(project_id),
                root_path="knowledge",
            )
        )
    return bindings


class KbMockContext(MagicMock):
    """``MagicMock`` tool context with its projects' knowledge bases attached.

    ``knowledge_bindings`` is derived from ``project_id`` / ``project_ids`` on
    every read unless a test assigns its own list.
    """

    @property
    def knowledge_bindings(self) -> List[KnowledgeBinding]:
        explicit = self.__dict__.get("_explicit_knowledge_bindings")
        if explicit is not None:
            return explicit
        return attached_project_bindings(
            self.__dict__.get("project_id"), self.__dict__.get("project_ids")
        )

    @knowledge_bindings.setter
    def knowledge_bindings(self, value: Any) -> None:
        self.__dict__["_explicit_knowledge_bindings"] = value


def connector_has_always_existed(store: Any = None) -> AsyncMock:
    """A store's ``get_datasource_by_managed_key`` for delivery tests whose
    subject is not the knowledge base.

    The orchestrator asks it for a project's knowledge-base connector
    wherever it builds the connector entries for an agent
    (``implied_project_knowledge``). This answer, a connector older than any
    job or session, adds nothing to those entries: what the test selected is
    what is delivered. Any other managed key goes to ``store``'s own method,
    as before.
    """
    original = getattr(store, "get_datasource_by_managed_key", None)

    async def lookup(managed_key: str) -> Any:
        if str(managed_key).startswith("project-kb:"):
            return {
                "id": "00000000-0000-4000-8000-0000000000cb",
                "name": "Project Knowledge",
                "description": None,
                "config": {"root_path": "knowledge"},
                "managed_key": managed_key,
                "created_at": datetime(2000, 1, 1, tzinfo=timezone.utc),
            }
        if original is None:
            return None
        return await original(managed_key)

    return AsyncMock(side_effect=lookup)
