"""Knowledge retrieval for context injection: which knowledge bases a
conversation reads, and the notes it gets from its bound ones.

The notes reach the model as an appended knowledge context entry
(append-only context injection; ``agent.core.context_injection``).
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence

from agent.services.knowledge.bindings import KnowledgeBinding
from shared.runtime.knowledge.chunker import embedding_version_for_service

logger = logging.getLogger(__name__)


@dataclass
class KnowledgeInjectionSelection:
    """Bound records and provenance ready for one transient injection block."""

    notes: List[Any] = field(default_factory=list)
    bindings: List[KnowledgeBinding] = field(default_factory=list)
    external_watermarks: Dict[str, Optional[str]] = field(default_factory=dict)
    counts_by_binding: Dict[str, int] = field(default_factory=dict)


def selected_knowledge_bindings(context: Any) -> List[KnowledgeBinding]:
    """Return concrete bindings without treating permissive mocks as scopes."""
    value = getattr(context, "knowledge_bindings", None) if context else None
    if not isinstance(value, (list, tuple)):
        return []
    return [binding for binding in value if isinstance(binding, KnowledgeBinding)]


async def _bounded(coro: Any, timeout: Optional[float]) -> Any:
    if timeout is None:
        return await coro
    return await asyncio.wait_for(coro, timeout=timeout)


def _record_kb_id(record: Any, fallback: uuid.UUID) -> uuid.UUID:
    raw = getattr(record, "kb_id", None) or getattr(record, "project_id", None)
    try:
        kb_id = raw if isinstance(raw, uuid.UUID) else uuid.UUID(str(raw))
    except (TypeError, ValueError, AttributeError):
        kb_id = fallback
    if getattr(record, "kb_id", None) is None:
        try:
            record.kb_id = kb_id
        except Exception:
            pass
    return kb_id


def _dedupe_records(
    records: Sequence[Any], fallback: uuid.UUID, seen: set[tuple[str, str]]
) -> List[Any]:
    unique: List[Any] = []
    for record in records:
        kb_id = _record_kb_id(record, fallback)
        key = (str(kb_id), str(getattr(record, "note_id", "")))
        if key in seen:
            continue
        seen.add(key)
        unique.append(record)
    return unique


def _round_robin(groups: Sequence[Sequence[Any]]) -> List[Any]:
    """Fairly merge binding result sets without reordering within any set."""
    merged: List[Any] = []
    offset = 0
    while any(offset < len(group) for group in groups):
        for group in groups:
            if offset < len(group):
                merged.append(group[offset])
        offset += 1
    return merged


async def retrieve_bound_knowledge(
    knowledge_store: Any,
    bindings: Sequence[KnowledgeBinding],
    query: str,
    *,
    limit: int = 5,
    timeout: Optional[float] = None,
) -> KnowledgeInjectionSelection:
    """Retrieve a protected native/external mix for passive injection.

    Native KBs share one RRF query and retain up to three protected slots.
    External KBs are queried independently so one broken datasource cannot
    discard successful results from the others. External result sets are
    round-robin merged, preserving each set's own RRF order.
    """
    scoped = [binding for binding in bindings if isinstance(binding, KnowledgeBinding)]
    if not knowledge_store or not scoped or limit <= 0:
        return KnowledgeInjectionSelection(bindings=scoped)

    try:
        current_version = embedding_version_for_service(
            knowledge_store.embedding_service
        )
    except Exception:
        current_version = None

    native = [binding for binding in scoped if binding.is_native]
    external = [binding for binding in scoped if not binding.is_native]

    async def search_native() -> List[Any]:
        if not native:
            return []
        try:
            return list(
                await _bounded(
                    knowledge_store.search_chunks(
                        kb_ids=[binding.kb_id for binding in native],
                        query=query,
                        embedding_version=current_version,
                        match_count=limit,
                    ),
                    timeout,
                )
            )
        except Exception as exc:
            logger.warning(
                "Native knowledge injection retrieval failed (non-fatal): %s: %s",
                type(exc).__name__,
                exc,
            )
            return []

    async def search_external(
        binding: KnowledgeBinding,
    ) -> tuple[List[Any], Optional[str]]:
        async def fetch_watermark() -> Optional[str]:
            try:
                watermark = await _bounded(
                    knowledge_store.get_watermark(binding.kb_id), timeout
                )
                commit = getattr(watermark, "indexed_commit", None)
                status = getattr(watermark, "status", None) or "ready"
                source_head = getattr(watermark, "source_head", None)
                if status != "ready":
                    last_clean = (
                        f"last clean @ {commit}" if commit else "no clean commit"
                    )
                    attempted = f", source @ {source_head}" if source_head else ""
                    return f"{status} — {last_clean}{attempted}"
                return commit
            except Exception as exc:
                logger.debug(
                    "Knowledge watermark lookup skipped for %s: %s",
                    binding.alias,
                    exc,
                )
                return None

        search_result, watermark = await asyncio.gather(
            _search_one_external(binding), fetch_watermark()
        )
        return search_result, watermark or binding.indexed_commit

    async def _search_one_external(binding: KnowledgeBinding) -> List[Any]:
        try:
            return list(
                await _bounded(
                    knowledge_store.search_chunks(
                        kb_ids=[binding.kb_id],
                        query=query,
                        embedding_version=current_version,
                        match_count=limit,
                    ),
                    timeout,
                )
            )
        except Exception as exc:
            logger.warning(
                "Knowledge injection retrieval failed for [%s] (non-fatal): %s: %s",
                binding.alias,
                type(exc).__name__,
                exc,
            )
            return []

    native_records, external_payloads = await asyncio.gather(
        search_native(),
        asyncio.gather(*(search_external(binding) for binding in external)),
    )

    seen: set[tuple[str, str]] = set()
    native_fallback = native[0].kb_id if native else scoped[0].kb_id
    native_unique = _dedupe_records(native_records, native_fallback, seen)

    external_groups: List[List[Any]] = []
    watermark_by_id: Dict[uuid.UUID, Optional[str]] = {}
    # Track externals whose watermark reports a non-ready state so a still-
    # indexing KB is disclosed even when it contributed zero selected records.
    # fetch_watermark() renders non-ready as "<status> — ..."; a bare commit
    # SHA never contains " — ".
    unready_external_ids: set[uuid.UUID] = set()
    for binding, (records, watermark) in zip(external, external_payloads):
        external_groups.append(_dedupe_records(records, binding.kb_id, seen))
        watermark_by_id[binding.kb_id] = watermark
        if isinstance(watermark, str) and " — " in watermark:
            unready_external_ids.add(binding.kb_id)
    external_unique = _round_robin(external_groups)

    if native:
        native_take = min(3, len(native_unique), limit)
        external_take = min(2, len(external_unique), max(0, limit - native_take))
        selected_native = native_unique[:native_take]
        selected_external = external_unique[:external_take]
        remaining = limit - len(selected_native) - len(selected_external)
        if remaining and native_take < 3:
            extra = external_unique[external_take : external_take + remaining]
            selected_external.extend(extra)
            remaining -= len(extra)
        if remaining:
            selected_native.extend(native_unique[native_take : native_take + remaining])
        selected = selected_native + selected_external
    else:
        selected = external_unique[:limit]

    binding_by_id = {binding.kb_id: binding for binding in scoped}
    counts: Dict[str, int] = {}
    selected_external_ids: set[uuid.UUID] = set()
    for record in selected:
        kb_id = _record_kb_id(record, scoped[0].kb_id)
        binding = binding_by_id.get(kb_id)
        if binding is None:
            continue
        counts[binding.alias] = counts.get(binding.alias, 0) + 1
        if not binding.is_native:
            selected_external_ids.add(binding.kb_id)

    external_watermarks = {
        binding.alias: watermark_by_id.get(binding.kb_id)
        for binding in external
        if binding.kb_id in selected_external_ids
        or binding.kb_id in unready_external_ids
    }
    return KnowledgeInjectionSelection(
        notes=selected,
        bindings=scoped,
        external_watermarks=external_watermarks,
        counts_by_binding=counts,
    )
