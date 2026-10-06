"""The append-on-change planner of append-only context injection.

Design: knowledge-base/knowledge/features/append_only_context_injection.md
(D1, D2, D4, D5, D8, D11, D21, D22, D29, D30) and the WP2 spec
(knowledge-base/knowledge/plans/append_only_context_injection_wp2_spec.md
§C, §G).

In ``append_only`` mode harness context (memory, knowledge, guidance, ...)
is not rebuilt at the request tail on every call. It is appended to the
history once, as typed entries (``shared.runtime.core.context_entries``),
and appended again only when it changes. :func:`plan_context_entries` decides
what to append: it reads what the history already holds
(:func:`scan_presence`, the entries' own metadata, D3) and compares it with
what the sources hold now. Four modes:

- **item** (memory, knowledge, guidance): an item whose ``(kind, key)`` is
  absent is new; present with another hash it is changed and rendered with
  the "(updated; ...)" marker (D5). One entry per kind (D2); memory takes at
  most ``max_memories`` per entry and the rest drip in on later requests
  (D29). A memory the model fetched itself with ``memory_search`` is present
  too, by its handle (D25, D30), so it is not pushed after the fetch.
- **state** (charter, citation, subagents): the section's latest hash is
  compared with the hash of the current rendering. A cleared state appends
  the cleared rendering once (O6); an absent section with an empty state
  appends nothing.
- **once per conversation** (memory_summary, D35): appended while its
  section is absent, i.e. at conversation start and again after a
  compaction removed it, never because its body changed: the summary is
  static for the conversation.
- **once per turn** (turn_boundary): one entry per session turn (D22).

Presence is read from the history *after* compaction, so whatever
compaction evicted is absent and becomes eligible again (D4, D21).

Pure: no I/O, no clock, no counters. The same history and sources always
give byte-identical entries, and an entry's text carries no per-turn data
(D11). The worker execute node (``graph.py``) and the session loop
(``persistent_graph.py``) call it in ``append_only`` mode.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import (
    Any,
    Callable,
    Collection,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
)

from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, ToolMessage

from shared.runtime.core.context_entries import (
    INJECTION_KINDS,
    MEMORY_SUMMARY_KIND,
    digest,
    entry_meta,
    is_append_only,
    is_context_entry,
    knowledge_item_key,
    make_context_entry,
    memory_handle,
    memory_list_items,
)
from shared.tool_catalog.names import MEMORY_SEARCH_TOOL_NAME

logger = logging.getLogger(__name__)


# --- Items and presence -------------------------------------------------------


@dataclass(frozen=True)
class Item:
    """One presence-tracked item: what the history records about it.

    ``key`` identifies the item across versions and ``hash`` its content
    (spec §A.3: importance, TTL and scores never go in). ``record`` is the
    source object the renderer needs; ``handle`` is the model-visible short
    name of a memory (D30), None for other kinds.
    """

    key: str
    hash: str
    record: Any = None
    handle: Optional[str] = None


@dataclass
class Presence:
    """What the typed entries in a history hold.

    ``items`` maps ``(kind, key)`` to the hash of the newest entry that
    listed the item; ``sections`` maps a section (the kind, or
    ``turn_boundary:<turn>``) to the hash of its newest entry.
    ``memory_handles`` maps a memory's display handle (D30) to the hash of
    the newest copy the model has seen, pushed in an entry or pulled through
    ``memory_search`` (D25): a pulled result names its memories only by
    handle, so handles are what push and pull have in common.
    """

    items: Dict[Tuple[str, str], str] = field(default_factory=dict)
    sections: Dict[str, str] = field(default_factory=dict)
    memory_handles: Dict[str, str] = field(default_factory=dict)

    def item_hash(self, kind: str, item: "Item") -> Optional[str]:
        """The hash the history holds for ``item``, None when it is absent.

        A memory with a handle is looked up by handle first, so a memory the
        model fetched with ``memory_search`` counts as present for the push.
        """
        if kind == "memory" and item.handle is not None:
            pulled = self.memory_handles.get(item.handle)
            if pulled is not None:
                return pulled
        return self.items.get((kind, item.key))


def _pulled_memory_result(msg: BaseMessage, search_call_ids: Collection[str]) -> bool:
    """Whether ``msg`` is the result of a ``memory_search`` call.

    Worker tool results carry the tool's name; session results are stored
    with only the call id, so the id is matched against the calls the
    assistant made earlier in the same history.
    """
    if not isinstance(msg, ToolMessage):
        return False
    if getattr(msg, "name", None) == MEMORY_SEARCH_TOOL_NAME:
        return True
    return str(getattr(msg, "tool_call_id", "") or "") in search_call_ids


def scan_presence(messages: Iterable[BaseMessage]) -> Presence:
    """Read the presence of every entry in ``messages`` (D3).

    One pass over typed entries and ``memory_search`` results (D25): a
    memory the model fetched is present by its handle and the hash of the
    content it was shown, so the push does not repeat it. A later entry or
    result overrides an earlier one for the same item or section, so a
    changed item counts with its newest hash. Compaction removes both kinds
    from the history, which makes their memories eligible again (D4).
    """
    presence = Presence()
    search_call_ids: set[str] = set()
    for msg in messages:
        if isinstance(msg, AIMessage):
            for call in getattr(msg, "tool_calls", None) or ():
                if call.get("name") == MEMORY_SEARCH_TOOL_NAME and call.get("id"):
                    search_call_ids.add(str(call["id"]))
            continue
        if _pulled_memory_result(msg, search_call_ids):
            for handle, item_hash in memory_list_items(msg.content):
                presence.memory_handles[handle] = item_hash
            continue
        meta = entry_meta(msg)
        if meta is None:
            continue
        kind = meta["kind"]
        for item in meta.get("items") or ():
            if not isinstance(item, Mapping):
                continue
            key, item_hash = item.get("key"), item.get("hash")
            if key is None or item_hash is None:
                continue
            presence.items[(kind, str(key))] = str(item_hash)
            handle = item.get("handle")
            if kind == "memory" and handle:
                presence.memory_handles[str(handle)] = str(item_hash)
        section = meta.get("section") or kind
        entry_hash = meta.get("hash")
        if entry_hash is not None:
            presence.sections[str(section)] = str(entry_hash)
    return presence


def memory_item(record: Any) -> Item:
    """The presence item of a memory row: key = row id, hash = content."""
    content = str(getattr(record, "content", "") or "")
    record_id = getattr(record, "id", None)
    if record_id is None:
        # Not from the store (no row id): content identity, and no handle,
        # since a handle must map back to a row (D30).
        return Item(key="sha:" + digest(content), hash=digest(content), record=record)
    return Item(
        key=str(record_id),
        hash=digest(content),
        record=record,
        handle=memory_handle(record_id),
    )


def knowledge_item(record: Any) -> Item:
    """The presence item of a knowledge note: key = KB + note, hash = title + content."""
    title = str(getattr(record, "title", "") or "")
    content = str(getattr(record, "content", "") or "")
    return Item(
        key=knowledge_item_key(record),
        hash=digest(f"{title}\n{content}"),
        record=record,
    )


def guidance_text(entry: Mapping[str, Any]) -> str:
    """The text of a guidance entry as the renderer shows it."""
    return str(entry.get("text", "")).strip()


def guidance_item(entry: Mapping[str, Any]) -> Item:
    """The presence item of a guidance entry: key = its id, else its text hash."""
    text = guidance_text(entry)
    entry_id = entry.get("id")
    key = str(entry_id) if entry_id else "sha:" + digest(text)
    return Item(key=key, hash=digest(text), record=entry)


# --- Sources and plan ---------------------------------------------------------


def max_memories_per_entry(config: Any) -> int:
    """``memory.max_memories_per_entry`` (D29); 5 when the config has none.

    A non-int (a ``MagicMock`` config in a test) reads as the default.
    """
    value = getattr(getattr(config, "memory", None), "max_memories_per_entry", None)
    if isinstance(value, int) and not isinstance(value, bool) and value > 0:
        return value
    return 5


@dataclass
class ContextSources:
    """What the harness would inject now, as the planner's input.

    Item kinds come as records; state kinds as rendered text, with ``None``
    meaning "no such source" (leave the section alone) and an empty value
    meaning "the state is empty" (append the cleared rendering if the
    history still shows an older state).

    - ``charter``: the rendered charter block ("" when there is none; a
      charter has no cleared rendering).
    - ``memory_summary``: the up-front memory summary (D35), loaded once per
      conversation (``agent.core.memory_summary``); "" when there is none
      or the history already holds it.
    - ``memory_records``: memory rows in rank order (``InjectionBlock.records``
      of kind memory, or the legacy ``recall_store.retrieve`` list).
    - ``knowledge_records``, ``knowledge_bindings``, ``external_watermarks``:
      knowledge notes in rank order plus what ``assemble_knowledge_block``
      needs to label bound KBs.
    - ``failed_citations``: the failed citations; None when there is no
      citation engine (or the lookup failed).
    - ``guidance``: pending supervisor guidance entries; ``delivered_guidance_ids``
      the ids already delivered (spec §C, D21).
    - ``subagents``: the rendered active-subagents block; None when there is
      no subagent runtime, "" when none is running.
    - ``turn_boundary``: the rendered App Guide turn boundary ("" when off);
      ``turn`` the session turn id (None on workers).
    """

    charter: str = ""
    memory_summary: str = ""
    memory_records: Sequence[Any] = ()
    knowledge_records: Sequence[Any] = ()
    knowledge_bindings: Optional[Sequence[Any]] = None
    external_watermarks: Optional[Mapping[str, Optional[str]]] = None
    failed_citations: Optional[Sequence[Any]] = None
    guidance: Sequence[Mapping[str, Any]] = ()
    delivered_guidance_ids: Collection[str] = ()
    subagents: Optional[str] = None
    turn_boundary: str = ""
    turn: Optional[int] = None


@dataclass
class Planned:
    """The plan of one request build.

    ``entries`` go after the carrier in this order (``INJECTION_KINDS``).
    ``guidance_ids`` are the ids of the guidance entries appended now; the
    caller adds them to ``delivered_guidance_ids``. ``memory_appended`` counts
    the memories rendered into the entry; ``memory_present`` the retrieved
    memories the history already held unchanged.
    """

    entries: List[HumanMessage] = field(default_factory=list)
    guidance_ids: List[str] = field(default_factory=list)
    memory_appended: int = 0
    memory_present: int = 0


def format_active_subagents_none(model: Optional[str] = None) -> str:
    """The cleared rendering of the subagents section: none is active (O6)."""
    from shared.runtime.services.guardrails import format_nudge

    return format_nudge("active_subagents_none", model=model)


def _select_changed(
    kind: str,
    items: Iterable[Item],
    presence: Presence,
    cap: Optional[int] = None,
) -> Tuple[List[Tuple[Item, bool]], int]:
    """Item mode: the new or changed items in rank order, at most ``cap``.

    Returns ``[(item, updated), ...]`` and the number of items the history
    already holds with the same hash. A key repeated in ``items`` counts once.
    """
    selected: List[Tuple[Item, bool]] = []
    present = 0
    seen: set[str] = set()
    for item in items:
        if item.key in seen:
            continue
        seen.add(item.key)
        prior = presence.item_hash(kind, item)
        if prior == item.hash:
            present += 1
            continue
        if cap is not None and len(selected) >= cap:
            continue
        selected.append((item, prior is not None))
    return selected, present


def _item_meta(item: Item) -> Dict[str, Any]:
    return {"key": item.key, "hash": item.hash, "handle": item.handle}


def _plan_memory(
    sources: ContextSources,
    presence: Presence,
    planned: Planned,
    *,
    model: Optional[str],
    max_memories: int,
) -> Optional[HumanMessage]:
    from shared.runtime.services.recall_store import RecallStore

    items = [memory_item(r) for r in sources.memory_records if r is not None]
    selected, present = _select_changed("memory", items, presence, cap=max_memories)
    planned.memory_present = present
    if not selected:
        return None
    body = RecallStore.render_memory_entry(
        [(item.record, item.handle, updated) for item, updated in selected],
        model=model,
    ).strip()
    if not body:
        return None
    entry = make_context_entry(
        "memory",
        body,
        section="memory",
        items=[_item_meta(item) for item, _ in selected],
        turn=sources.turn,
    )
    planned.memory_appended = len(selected)
    return entry


def _plan_knowledge(
    sources: ContextSources,
    presence: Presence,
    *,
    model: Optional[str],
) -> Optional[HumanMessage]:
    from shared.runtime.services.knowledge_store import KnowledgeStore

    items = [knowledge_item(r) for r in sources.knowledge_records if r is not None]
    selected, _ = _select_changed("knowledge", items, presence)
    if not selected:
        return None
    body = KnowledgeStore.assemble_knowledge_block(
        [item.record for item, _ in selected],
        model=model,
        bindings=list(sources.knowledge_bindings or []) or None,
        external_watermarks=dict(sources.external_watermarks or {}) or None,
        updated_keys={item.key for item, updated in selected if updated},
    ).strip()
    if not body:
        return None
    return make_context_entry(
        "knowledge",
        body,
        section="knowledge",
        items=[_item_meta(item) for item, _ in selected],
        turn=sources.turn,
    )


def _plan_guidance(
    sources: ContextSources,
    presence: Presence,
    planned: Planned,
) -> Optional[HumanMessage]:
    """Guidance: pending minus delivered ids minus keys already present (§C)."""
    from agent.core.guidance_injection import format_supervisor_guidance

    delivered = {str(v) for v in sources.delivered_guidance_ids if v is not None}
    new: List[Item] = []
    seen: set[str] = set()
    for entry in sources.guidance:
        if not isinstance(entry, Mapping) or not guidance_text(entry):
            continue
        entry_id = entry.get("id")
        if entry_id and str(entry_id) in delivered:
            continue
        item = guidance_item(entry)
        if item.key in seen or ("guidance", item.key) in presence.items:
            continue
        seen.add(item.key)
        new.append(item)
    if not new:
        return None
    body = format_supervisor_guidance(
        [dict(item.record) for item in new], repeat_notice=False
    ).strip()
    if not body:
        return None
    entry = make_context_entry(
        "guidance",
        body,
        section="guidance",
        items=[_item_meta(item) for item in new],
        turn=sources.turn,
    )
    planned.guidance_ids = [
        str(item.record["id"]) for item in new if item.record.get("id")
    ]
    return entry


def _plan_state(
    kind: str,
    body: str,
    presence: Presence,
    *,
    has_state: bool,
    turn: Optional[int],
) -> Optional[HumanMessage]:
    """State mode: append when the section is absent or its hash differs.

    ``body`` is the full rendering, or the cleared one when ``has_state`` is
    False; an absent section with an empty state appends nothing.
    """
    body = body.strip()
    if not body:
        return None
    state_hash = digest(body)
    prior = presence.sections.get(kind)
    if prior is None and not has_state:
        return None
    if prior == state_hash:
        return None
    return make_context_entry(
        kind, body, section=kind, state_hash=state_hash, turn=turn
    )


def _plan_charter(
    sources: ContextSources, presence: Presence
) -> Optional[HumanMessage]:
    charter = sources.charter or ""
    return _plan_state(
        "charter", charter, presence, has_state=bool(charter.strip()), turn=sources.turn
    )


def _plan_memory_summary(
    sources: ContextSources, presence: Presence
) -> Optional[HumanMessage]:
    """Once per conversation (D35): append the summary while it is absent.

    Append-if-absent, never on change: the summary is computed once at
    conversation start and stays as it was (D11). A body that differs from
    the one in the history (memory grew, or a fresh process recomputed it)
    appends nothing; only a compaction that removed the entry makes it
    absent, and then the current body goes in again (D21).
    """
    body = (sources.memory_summary or "").strip()
    if not body or MEMORY_SUMMARY_KIND in presence.sections:
        return None
    return make_context_entry(
        MEMORY_SUMMARY_KIND,
        body,
        section=MEMORY_SUMMARY_KIND,
        state_hash=digest(body),
        turn=sources.turn,
    )


def _plan_citation(
    sources: ContextSources,
    presence: Presence,
    *,
    model: Optional[str],
) -> Optional[HumanMessage]:
    if sources.failed_citations is None:
        return None
    from agent.core.citation_feedback_injection import (
        format_citation_feedback_resolved,
        format_failed_citations,
    )

    failed = list(sources.failed_citations)
    body = (
        format_failed_citations(failed)
        if failed
        else format_citation_feedback_resolved(model)
    )
    return _plan_state(
        "citation", body, presence, has_state=bool(failed), turn=sources.turn
    )


def _plan_subagents(
    sources: ContextSources,
    presence: Presence,
    *,
    model: Optional[str],
) -> Optional[HumanMessage]:
    if sources.subagents is None:
        return None
    block = sources.subagents.strip()
    body = block or format_active_subagents_none(model)
    return _plan_state(
        "subagents", body, presence, has_state=bool(block), turn=sources.turn
    )


def _plan_turn_boundary(
    sources: ContextSources, presence: Presence
) -> Optional[HumanMessage]:
    """Once per turn: append the boundary when this turn's section is absent."""
    body = (sources.turn_boundary or "").strip()
    if not body:
        return None
    if sources.turn is None:
        logger.warning(
            "Turn boundary planned without a turn id; it is skipped (it is "
            "once per session turn)"
        )
        return None
    section = f"turn_boundary:{sources.turn}"
    if section in presence.sections:
        return None
    return make_context_entry("turn_boundary", body, section=section, turn=sources.turn)


def _ends_in_open_tool_call(messages: Sequence[BaseMessage]) -> bool:
    """Whether the carrier the entries would follow is an open tool call.

    The carrier is the last non-entry message. An AIMessage with tool calls
    there has no results yet, and the fold drops anything that sits between
    a call and its results: entries appended now would count as present in
    the history but never reach the provider.
    """
    for msg in reversed(messages):
        if is_context_entry(msg):
            continue
        return isinstance(msg, AIMessage) and bool(getattr(msg, "tool_calls", None))
    return False


def plan_context_entries(
    messages: Sequence[BaseMessage],
    sources: ContextSources,
    *,
    model: Optional[str],
    max_memories: int,
) -> Planned:
    """The entries to append to ``messages`` for this request build.

    ``messages`` is the history after compaction; ``sources`` what the
    harness holds now; ``model`` resolves the family's nudge texts;
    ``max_memories`` is ``memory.max_memories_per_entry`` (D29). The result
    lists the entries in ``INJECTION_KINDS`` order. A kind whose renderer
    fails is skipped with a warning, so a broken source never fails the
    request. Nothing is planned while the history ends in a tool call
    without results: the fold would drop the entries (an entry never sits
    between a call and its results), so they would be recorded as present
    without ever being sent. The next build, after the results, plans them.
    """
    if _ends_in_open_tool_call(messages):
        logger.warning(
            "No context entries planned: the history ends in a tool call "
            "without results, and entries after it would never be sent"
        )
        return Planned()
    presence = scan_presence(messages)
    planned = Planned()
    planners: Dict[str, Callable[[], Optional[HumanMessage]]] = {
        "charter": lambda: _plan_charter(sources, presence),
        "memory_summary": lambda: _plan_memory_summary(sources, presence),
        "memory": lambda: _plan_memory(
            sources, presence, planned, model=model, max_memories=max_memories
        ),
        "knowledge": lambda: _plan_knowledge(sources, presence, model=model),
        "citation": lambda: _plan_citation(sources, presence, model=model),
        "guidance": lambda: _plan_guidance(sources, presence, planned),
        "subagents": lambda: _plan_subagents(sources, presence, model=model),
        "turn_boundary": lambda: _plan_turn_boundary(sources, presence),
    }
    for kind in INJECTION_KINDS:
        try:
            entry = planners[kind]()
        except Exception as exc:
            logger.warning(
                "Planning the %s context entry failed (non-fatal, skipped): %s: %s",
                kind,
                type(exc).__name__,
                exc,
            )
            continue
        if entry is not None:
            planned.entries.append(entry)
    return planned


__all__ = [
    "ContextSources",
    "Item",
    "Planned",
    "Presence",
    "format_active_subagents_none",
    "guidance_item",
    "guidance_text",
    "is_append_only",
    "knowledge_item",
    "max_memories_per_entry",
    "memory_item",
    "plan_context_entries",
    "scan_presence",
]
