"""``knowledge_index``: OKF knowledge bases SRW indexes centrally.

The agent receives an index id, never the repository or its credentials, and
binds it read-only next to the project's native knowledge base. A project's
own KB connector is a management surface over that native KB: it collapses
into the native binding and is not listed in the README. A live change to
the KB selection applies at the next attach (the driver's ``live_attach`` is
off): its bindings are wired into memory and KB machinery the tool context
holds a copy of.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Sequence
from typing import Any

from agent.connectors.base import Delivery, FactsLines, RuntimeContext
from shared.native_kb import native_kb_project_id

logger = logging.getLogger(__name__)


def _key(delivery: Delivery) -> str:
    return f"{delivery.entry.get('type')}:{delivery.entry.get('name')}"


class KnowledgeIndexMaterializer:
    form = "knowledge_index"

    def materialize(self, deliveries: Sequence[Delivery], rt: RuntimeContext) -> None:
        """Nothing to open: the bindings are built with the tool context."""

    def bindings(
        self,
        deliveries: Sequence[Delivery],
        *,
        project_ids: Iterable[str],
        runtime_actor: Any,
    ) -> list[Any]:
        from agent.services.knowledge.bindings import build_knowledge_bindings

        return build_knowledge_bindings(
            project_ids=project_ids,
            datasources=[delivery.entry for delivery in deliveries],
            runtime_actor=runtime_actor,
        )

    def replace(
        self,
        old: Sequence[Delivery],
        new: Sequence[Delivery],
        rt: RuntimeContext,
    ) -> None:
        if {_key(delivery) for delivery in old} != {_key(delivery) for delivery in new}:
            logger.warning(
                "kb-type datasource selection changed live — knowledge "
                "bindings apply on the next attach, not mid-session"
            )
            rt.deferred.add(self.form)

    def facts(
        self, deliveries: Sequence[Delivery], rt: RuntimeContext
    ) -> list[FactsLines]:
        out: list[FactsLines] = []
        for delivery in deliveries:
            ds = delivery.entry
            # A project's own KB is bound as the writable native KB; listing
            # it here would advertise it as "read-only".
            if native_kb_project_id(ds):
                continue
            name = ds.get("name", "Unnamed")
            root = str((ds.get("config") or {}).get("root_path") or "")
            root_hint = f", root `{root}`" if root else ""
            out.append(
                FactsLines(
                    "OKF Knowledge Bases",
                    delivery.index,
                    [
                        f"- **{name}** (centrally indexed, read-only{root_hint}) — "
                        "use `kb_search`, `kb_grep`, `kb_list`, and `kb_read`"
                    ],
                )
            )
        return out
