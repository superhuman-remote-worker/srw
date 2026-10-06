"""The append-on-change planner (append-only context injection, WP2 spec §C).

``plan_context_entries`` reads presence from the history's own entries and
appends only what is new or changed: item mode for memory, knowledge and
guidance; state mode for charter, citation and subagents (with cleared
renderings, O6); once per conversation for the memory summary (D35,
append-if-absent); once per turn for the App Guide boundary. The planner is
pure and not wired yet, so these tests drive it directly and feed each plan
back into the history, the way the wiring will.
"""

from __future__ import annotations

import re
import uuid
from types import SimpleNamespace
from typing import List

import pytest
from langchain_core.messages import (
    AIMessage,
    BaseMessage,
    HumanMessage,
    SystemMessage,
    ToolMessage,
)

import agent.core.context_injection as planner_module
from agent.core.context_injection import (
    ContextSources,
    Item,
    Planned,
    Presence,
    format_active_subagents_none,
    guidance_item,
    knowledge_item,
    memory_item,
    plan_context_entries,
    scan_presence,
)
from agent.core.citation_feedback_injection import (
    format_citation_feedback_resolved,
    format_failed_citations,
)
from agent.core.memory_injection import create_memory_injection_messages
from shared.runtime.core.context_entries import (
    INJECTION_KINDS,
    PERSIST_ROLE_CONTEXT,
    SRW_INJECTION_KEY,
    UPDATED_ITEM_MARKER,
    digest,
    entry_body,
    entry_meta,
    fold_context_entries,
    is_append_only,
    is_context_entry,
    knowledge_item_key,
    make_context_entry,
    memory_handle,
)
from shared.runtime.core.message_markers import PERSIST_ROLE_KEY
from shared.runtime.services.knowledge_store import KnowledgeRecord
from shared.runtime.services.recall_store import MemoryRecord

KB = uuid.UUID(int=0xB0)
OTHER_KB = uuid.UUID(int=0xB1)
CHARTER = "[CHARTER — standing orders]\n\n# Charter\n\nShip weekly."
SUBAGENTS = (
    "<active_subagents>\n- sa-1 (scholar): running\n"
    "Reports push automatically as evidence; do not poll.\n</active_subagents>"
)
BOUNDARY = "<managed_product_guide_turn_boundary>\nReturn to the request.\n</managed_product_guide_turn_boundary>"
SUMMARY = (
    "Project memory overview (harness context, not a user message): 3 memories "
    "from earlier jobs and sessions.\nBy type: 2 factual, 1 procedural.\n"
    "Frequent topics: deploy, release.\nCall memory_search to look up the rest."
)


def mem(n: int, content: str | None = None, **kwargs) -> MemoryRecord:
    kwargs.setdefault("importance", 0.5)
    return MemoryRecord(
        id=uuid.UUID(int=n), content=content or f"Memory fact {n}.", **kwargs
    )


def note(note_id: str, content: str, title: str = "", kb=KB) -> KnowledgeRecord:
    return KnowledgeRecord(
        note_id=note_id,
        kb_id=kb,
        project_id=kb,
        title=title,
        note_type="learning",
        content=content,
    )


def citation(cid: str, claim: str = "The sky is green.") -> SimpleNamespace:
    return SimpleNamespace(
        id=cid,
        claim=claim,
        verification_notes="quote not found",
        similarity_score=None,
    )


def guidance(gid, text: str) -> dict:
    return {"id": gid, "text": text, "source": "officer", "created_at": "2026-10-05"}


def base_history() -> List[BaseMessage]:
    return [SystemMessage(content="system"), HumanMessage(content="Do the task.")]


def plan(messages, sources, *, max_memories: int = 5) -> Planned:
    return plan_context_entries(
        messages, sources, model=None, max_memories=max_memories
    )


def kinds(planned: Planned) -> List[str]:
    return [entry_meta(entry)["kind"] for entry in planned.entries]


def only(planned: Planned, kind: str) -> HumanMessage:
    matches = [e for e in planned.entries if entry_meta(e)["kind"] == kind]
    assert len(matches) == 1, kinds(planned)
    return matches[0]


# --- Presence -----------------------------------------------------------------


class TestScanPresence:
    def test_history_without_entries_has_no_presence(self):
        mem_ai, mem_tool = create_memory_injection_messages("[1]\nlegacy memory")
        presence = scan_presence(base_history() + [mem_ai, mem_tool])
        assert presence == Presence()

    def test_reads_items_and_sections_of_entries(self):
        entry = make_context_entry(
            "memory",
            "body",
            section="memory",
            items=[{"key": "k1", "hash": "h1", "handle": "m:abcdef"}],
        )
        presence = scan_presence(base_history() + [entry])
        assert presence.items == {("memory", "k1"): "h1"}
        assert presence.sections == {"memory": digest("body")}

    def test_a_later_entry_overrides_the_earlier_hash(self):
        first = make_context_entry(
            "memory", "v1", section="memory", items=[{"key": "k", "hash": "h1"}]
        )
        second = make_context_entry(
            "memory", "v2", section="memory", items=[{"key": "k", "hash": "h2"}]
        )
        presence = scan_presence([first, HumanMessage(content="x"), second])
        assert presence.items[("memory", "k")] == "h2"
        assert presence.sections["memory"] == digest("v2")

    def test_turn_boundary_sections_are_per_turn(self):
        t1 = make_context_entry("turn_boundary", "b", section="turn_boundary:1")
        t2 = make_context_entry("turn_boundary", "b", section="turn_boundary:2")
        presence = scan_presence([t1, t2])
        assert set(presence.sections) == {"turn_boundary:1", "turn_boundary:2"}

    def test_malformed_items_are_skipped(self):
        entry = make_context_entry("knowledge", "body", section="knowledge")
        entry.additional_kwargs[SRW_INJECTION_KEY]["items"] = [
            "junk",
            {"key": "only-key"},
            {"key": "ok", "hash": "h"},
        ]
        presence = scan_presence([entry])
        assert presence.items == {("knowledge", "ok"): "h"}


class TestItemIdentity:
    def test_memory_item_keys_by_row_id_and_hashes_content(self):
        record = mem(7, "Prefers ruff.", importance=0.9, remaining_turns=3)
        item = memory_item(record)
        assert item == Item(
            key=str(record.id),
            hash=digest("Prefers ruff."),
            record=record,
            handle=memory_handle(record.id),
        )

    def test_memory_hash_ignores_importance_and_ttl(self):
        a = memory_item(mem(1, "same", importance=0.1, remaining_turns=9))
        b = memory_item(mem(1, "same", importance=0.9, remaining_turns=0))
        assert (a.key, a.hash) == (b.key, b.hash)

    def test_memory_without_row_id_keys_by_content_and_has_no_handle(self):
        item = memory_item(MemoryRecord(content="loose"))
        assert item.key == "sha:" + digest("loose")
        assert item.handle is None

    def test_knowledge_item_keys_by_kb_and_note(self):
        record = note("n1", "body", title="Title")
        item = knowledge_item(record)
        assert item.key == f"kb:{KB}:n1" == knowledge_item_key(record)
        assert item.hash == digest("Title\nbody")

    def test_guidance_item_keys_by_id_else_text_hash(self):
        assert guidance_item(guidance("g1", "Go")).key == "g1"
        assert guidance_item(guidance(None, " Go ")).key == "sha:" + digest("Go")
        assert guidance_item(guidance("g1", "Go")).hash == digest("Go")


# --- Item mode: memory ----------------------------------------------------------


class TestMemoryEntries:
    def test_new_memories_become_one_entry(self):
        records = [mem(1), mem(2)]
        planned = plan(base_history(), ContextSources(memory_records=records))

        entry = only(planned, "memory")
        meta = entry_meta(entry)
        assert meta["section"] == "memory"
        assert meta["items"] == [
            {
                "key": str(r.id),
                "hash": digest(r.content),
                "handle": memory_handle(r.id),
            }
            for r in records
        ]
        assert planned.memory_appended == 2
        assert planned.memory_present == 0

    def test_present_memories_are_not_appended_again(self):
        history = base_history()
        records = [mem(1), mem(2)]
        history += plan(history, ContextSources(memory_records=records)).entries

        planned = plan(history, ContextSources(memory_records=records))
        assert planned.entries == []
        assert planned.memory_appended == 0
        assert planned.memory_present == 2

    def test_only_the_new_memory_is_appended(self):
        history = base_history()
        history += plan(history, ContextSources(memory_records=[mem(1)])).entries

        planned = plan(history, ContextSources(memory_records=[mem(1), mem(2)]))
        entry = only(planned, "memory")
        assert [i["key"] for i in entry_meta(entry)["items"]] == [str(mem(2).id)]
        assert "Memory fact 2." in entry.content
        assert "Memory fact 1." not in entry.content
        assert (planned.memory_appended, planned.memory_present) == (1, 1)

    def test_a_changed_memory_is_appended_with_the_updated_marker(self):
        history = base_history()
        history += plan(history, ContextSources(memory_records=[mem(1, "v1")])).entries

        planned = plan(history, ContextSources(memory_records=[mem(1, "v2")]))
        entry = only(planned, "memory")
        assert entry_meta(entry)["items"][0]["hash"] == digest("v2")
        assert UPDATED_ITEM_MARKER in entry.content
        assert "v2" in entry.content

        # The newer version is what the history holds from now on.
        history += planned.entries
        assert (
            plan(history, ContextSources(memory_records=[mem(1, "v2")])).entries == []
        )

    def test_a_new_memory_has_no_updated_marker(self):
        planned = plan(base_history(), ContextSources(memory_records=[mem(1)]))
        assert UPDATED_ITEM_MARKER not in only(planned, "memory").content

    def test_importance_change_alone_is_not_a_change(self):
        history = base_history()
        history += plan(
            history, ContextSources(memory_records=[mem(1, "same", importance=0.2)])
        ).entries
        planned = plan(
            history, ContextSources(memory_records=[mem(1, "same", importance=0.9)])
        )
        assert planned.entries == []

    def test_cap_and_drip_feed_across_successive_plans(self):
        records = [mem(n) for n in range(1, 13)]
        history = base_history()
        appended, present = [], []
        for _ in range(4):
            planned = plan(history, ContextSources(memory_records=records))
            appended.append(planned.memory_appended)
            present.append(planned.memory_present)
            history += planned.entries
            history.append(ToolMessage(content="tool output", tool_call_id="c"))

        assert appended == [5, 5, 2, 0]
        assert present == [0, 5, 10, 12]
        entries = [m for m in history if is_context_entry(m)]
        keys = [i["key"] for e in entries for i in entry_meta(e)["items"]]
        # Rank order, each memory exactly once.
        assert keys == [str(r.id) for r in records]

    def test_cap_follows_max_memories(self):
        records = [mem(n) for n in range(1, 6)]
        planned = plan(
            base_history(), ContextSources(memory_records=records), max_memories=2
        )
        assert planned.memory_appended == 2
        assert len(entry_meta(only(planned, "memory"))["items"]) == 2

    def test_a_duplicate_record_counts_once(self):
        planned = plan(base_history(), ContextSources(memory_records=[mem(1), mem(1)]))
        assert planned.memory_appended == 1

    def test_handles_are_shown_and_ids_and_hashes_are_not(self):
        records = [mem(1), mem(2)]
        entry = only(
            plan(base_history(), ContextSources(memory_records=records)), "memory"
        )
        text = entry.content
        meta = entry_meta(entry)

        assert re.findall(r"\[(m:[0-9a-f]{6})\]", text) == [
            memory_handle(r.id) for r in records
        ]
        for record in records:
            assert str(record.id) not in text
            assert record.id.hex not in text
        for item in meta["items"]:
            assert item["hash"] not in text
        assert meta["hash"] not in text

    def test_rendering_ignores_the_ttl(self):
        pinned = plan(
            base_history(),
            ContextSources(memory_records=[mem(1, "same", remaining_turns=9)]),
        )
        unpinned = plan(
            base_history(),
            ContextSources(memory_records=[mem(1, "same", remaining_turns=None)]),
        )
        assert pinned.entries[0].content == unpinned.entries[0].content
        assert "turns left" not in pinned.entries[0].content


# --- Item mode: knowledge -------------------------------------------------------


class TestKnowledgeEntries:
    def test_new_notes_become_one_entry(self):
        notes = [note("n1", "one"), note("n2", "two")]
        entry = only(
            plan(base_history(), ContextSources(knowledge_records=notes)), "knowledge"
        )
        meta = entry_meta(entry)
        assert [i["key"] for i in meta["items"]] == [f"kb:{KB}:n1", f"kb:{KB}:n2"]
        assert all(i["handle"] is None for i in meta["items"])
        assert "one" in entry.content and "two" in entry.content

    def test_present_notes_are_skipped_and_changed_ones_marked(self):
        history = base_history()
        history += plan(
            history,
            ContextSources(knowledge_records=[note("n1", "one"), note("n2", "two")]),
        ).entries

        planned = plan(
            history,
            ContextSources(
                knowledge_records=[
                    note("n1", "one"),
                    note("n2", "two v2"),
                    note("n3", "3"),
                ]
            ),
        )
        entry = only(planned, "knowledge")
        assert [i["key"] for i in entry_meta(entry)["items"]] == [
            f"kb:{KB}:n2",
            f"kb:{KB}:n3",
        ]
        lines = entry_body(entry).splitlines()
        n2_line = next(line for line in lines if line.startswith("[1]"))
        n3_line = next(line for line in lines if line.startswith("[2]"))
        assert UPDATED_ITEM_MARKER in n2_line
        assert UPDATED_ITEM_MARKER not in n3_line

    def test_a_title_change_is_a_change(self):
        history = base_history()
        history += plan(
            history, ContextSources(knowledge_records=[note("n1", "body", title="Old")])
        ).entries
        planned = plan(
            history, ContextSources(knowledge_records=[note("n1", "body", title="New")])
        )
        assert UPDATED_ITEM_MARKER in only(planned, "knowledge").content

    def test_the_same_note_id_in_two_kbs_is_two_items(self):
        planned = plan(
            base_history(),
            ContextSources(
                knowledge_records=[note("n1", "a", kb=KB), note("n1", "a", kb=OTHER_KB)]
            ),
        )
        assert len(entry_meta(only(planned, "knowledge"))["items"]) == 2

    def test_bindings_label_bound_notes(self):
        binding = SimpleNamespace(kb_id=KB, alias="docs")
        planned = plan(
            base_history(),
            ContextSources(
                knowledge_records=[note("n1", "body", title="T")],
                knowledge_bindings=[binding],
                external_watermarks={"ext": "abc123"},
            ),
        )
        text = only(planned, "knowledge").content
        assert "[docs] docs:n1 — T" in text
        assert "External snapshots (as of): [ext] abc123" in text


# --- State mode -------------------------------------------------------------------


class TestCitationState:
    def test_no_engine_plans_nothing(self):
        assert plan(base_history(), ContextSources(failed_citations=None)).entries == []

    def test_absent_and_empty_plans_nothing(self):
        assert plan(base_history(), ContextSources(failed_citations=[])).entries == []

    def test_change_then_clear_then_quiet(self):
        history = base_history()

        first = plan(history, ContextSources(failed_citations=[citation("c1")]))
        entry = only(first, "citation")
        assert entry_body(entry) == format_failed_citations([citation("c1")])
        assert entry_meta(entry)["hash"] == digest(entry_body(entry))
        history += first.entries

        # Same failed set: nothing.
        assert (
            plan(history, ContextSources(failed_citations=[citation("c1")])).entries
            == []
        )

        # Changed set: the new full rendering.
        changed = plan(
            history, ContextSources(failed_citations=[citation("c1"), citation("c2")])
        )
        assert "[c2]" in only(changed, "citation").content
        history += changed.entries

        # Cleared: the cleared rendering, once.
        cleared = plan(history, ContextSources(failed_citations=[]))
        assert (
            entry_body(only(cleared, "citation")) == format_citation_feedback_resolved()
        )
        history += cleared.entries
        assert plan(history, ContextSources(failed_citations=[])).entries == []

        # Failing again: the full rendering again.
        again = plan(history, ContextSources(failed_citations=[citation("c3")]))
        assert "[c3]" in only(again, "citation").content


class TestSubagentState:
    def test_no_runtime_plans_nothing(self):
        assert plan(base_history(), ContextSources(subagents=None)).entries == []

    def test_absent_and_none_running_plans_nothing(self):
        assert plan(base_history(), ContextSources(subagents="")).entries == []

    def test_change_clear_and_quiet(self):
        history = base_history()
        first = plan(history, ContextSources(subagents=SUBAGENTS))
        assert entry_body(only(first, "subagents")) == SUBAGENTS
        history += first.entries

        assert plan(history, ContextSources(subagents=SUBAGENTS)).entries == []

        moved = SUBAGENTS.replace("running", "delivery_pending")
        changed = plan(history, ContextSources(subagents=moved))
        assert "delivery_pending" in only(changed, "subagents").content
        history += changed.entries

        cleared = plan(history, ContextSources(subagents=""))
        assert entry_body(only(cleared, "subagents")) == format_active_subagents_none()
        history += cleared.entries
        assert plan(history, ContextSources(subagents="")).entries == []


class TestCharterState:
    def test_once_then_again_on_change(self):
        history = base_history()
        first = plan(history, ContextSources(charter=CHARTER))
        assert entry_body(only(first, "charter")) == CHARTER
        history += first.entries

        assert plan(history, ContextSources(charter=CHARTER)).entries == []

        amended = CHARTER + "\nAlso: ship docs."
        changed = plan(history, ContextSources(charter=amended))
        assert entry_body(only(changed, "charter")) == amended

    def test_a_missing_charter_appends_no_cleared_rendering(self):
        history = base_history()
        history += plan(history, ContextSources(charter=CHARTER)).entries
        assert plan(history, ContextSources(charter="")).entries == []

    def test_again_after_compaction_drops_the_entry(self):
        history = base_history()
        history += plan(history, ContextSources(charter=CHARTER)).entries
        history += [AIMessage(content="done"), HumanMessage(content="next")]

        compacted = _compact(history)
        assert (
            entry_body(
                only(plan(compacted, ContextSources(charter=CHARTER)), "charter")
            )
            == CHARTER
        )


# --- Once per conversation (D35) -------------------------------------------------


class TestMemorySummary:
    """The up-front memory summary: append-if-absent, never on change."""

    def test_appended_once_then_quiet(self):
        history = base_history()
        first = plan(history, ContextSources(memory_summary=SUMMARY))
        entry = only(first, "memory_summary")
        assert entry_body(entry) == SUMMARY
        meta = entry_meta(entry)
        assert meta["section"] == "memory_summary"
        assert meta["items"] == []
        assert meta["hash"] == digest(SUMMARY)
        history += first.entries

        for _ in range(3):
            history += [
                AIMessage(
                    content="", tool_calls=[{"name": "t", "args": {}, "id": "c1"}]
                ),
                ToolMessage(content="out", tool_call_id="c1"),
            ]
            assert plan(history, ContextSources(memory_summary=SUMMARY)).entries == []

    def test_a_changed_body_is_never_appended(self):
        """Counts that moved mid-conversation (or a fresh process that
        recomputed them) do not re-send it: the summary is static (D35)."""
        history = base_history()
        history += plan(history, ContextSources(memory_summary=SUMMARY)).entries
        grown = SUMMARY.replace("3 memories", "4 memories")
        assert plan(history, ContextSources(memory_summary=grown)).entries == []

    def test_again_after_compaction_drops_the_entry(self):
        history = base_history()
        history += plan(history, ContextSources(memory_summary=SUMMARY)).entries
        history += [AIMessage(content="done"), HumanMessage(content="next")]

        compacted = _compact(history)
        again = only(
            plan(compacted, ContextSources(memory_summary=SUMMARY)), "memory_summary"
        )
        assert entry_body(again) == SUMMARY

    def test_nothing_to_summarize_plans_nothing(self):
        assert plan(base_history(), ContextSources(memory_summary="")).entries == []
        assert plan(base_history(), ContextSources(memory_summary="  \n")).entries == []

    def test_after_the_charter_and_before_the_memories(self):
        planned = plan(
            base_history(),
            ContextSources(
                charter=CHARTER, memory_summary=SUMMARY, memory_records=[mem(1)]
            ),
        )
        assert kinds(planned) == ["charter", "memory_summary", "memory"]

    def test_a_session_entry_carries_the_turn(self):
        entry = only(
            plan(base_history(), ContextSources(memory_summary=SUMMARY, turn=2)),
            "memory_summary",
        )
        assert entry_meta(entry)["turn"] == 2


# --- Once per turn ----------------------------------------------------------------


class TestTurnBoundary:
    def test_once_per_turn(self):
        history = base_history()
        first = plan(history, ContextSources(turn_boundary=BOUNDARY, turn=1))
        entry = only(first, "turn_boundary")
        assert entry_meta(entry)["section"] == "turn_boundary:1"
        assert entry_meta(entry)["turn"] == 1
        history += first.entries

        # Later requests of the same turn: nothing.
        history += [
            AIMessage(content="", tool_calls=[{"name": "t", "args": {}, "id": "c1"}]),
            ToolMessage(content="out", tool_call_id="c1"),
        ]
        assert (
            plan(history, ContextSources(turn_boundary=BOUNDARY, turn=1)).entries == []
        )

        # The next turn gets its own.
        history += [AIMessage(content="answer"), HumanMessage(content="follow-up")]
        second = plan(history, ContextSources(turn_boundary=BOUNDARY, turn=2))
        assert entry_meta(only(second, "turn_boundary"))["section"] == "turn_boundary:2"

    def test_again_after_a_mid_turn_compaction(self):
        history = base_history()
        history += plan(history, ContextSources(turn_boundary=BOUNDARY, turn=3)).entries
        compacted = _compact(history)
        planned = plan(compacted, ContextSources(turn_boundary=BOUNDARY, turn=3))
        assert kinds(planned) == ["turn_boundary"]

    def test_off_or_without_a_turn_id_plans_nothing(self):
        assert (
            plan(base_history(), ContextSources(turn_boundary="", turn=1)).entries == []
        )
        assert (
            plan(base_history(), ContextSources(turn_boundary=BOUNDARY)).entries == []
        )


# --- Guidance -----------------------------------------------------------------------


class TestGuidance:
    def test_new_guidance_is_one_entry_without_the_repeat_notice(self):
        planned = plan(
            base_history(),
            ContextSources(
                guidance=[
                    guidance("g1", "Focus on tests."),
                    guidance("g2", "Skip docs."),
                ]
            ),
        )
        entry = only(planned, "guidance")
        assert "Focus on tests." in entry.content and "Skip docs." in entry.content
        assert "may repeat" not in entry.content
        assert [i["key"] for i in entry_meta(entry)["items"]] == ["g1", "g2"]
        assert planned.guidance_ids == ["g1", "g2"]

    def test_delivered_ids_are_filtered(self):
        planned = plan(
            base_history(),
            ContextSources(
                guidance=[guidance("g1", "old"), guidance("g2", "new")],
                delivered_guidance_ids=["g1"],
            ),
        )
        assert [i["key"] for i in entry_meta(only(planned, "guidance"))["items"]] == [
            "g2"
        ]
        assert planned.guidance_ids == ["g2"]

    def test_all_delivered_plans_nothing(self):
        planned = plan(
            base_history(),
            ContextSources(
                guidance=[guidance("g1", "x")], delivered_guidance_ids={"g1"}
            ),
        )
        assert planned.entries == [] and planned.guidance_ids == []

    def test_present_keys_are_filtered(self):
        # The pinned lane: the inbox still holds the entry until the ack
        # lands, and nothing recorded it as delivered yet.
        history = base_history()
        history += plan(history, ContextSources(guidance=[guidance("g1", "x")])).entries
        planned = plan(history, ContextSources(guidance=[guidance("g1", "x")]))
        assert planned.entries == [] and planned.guidance_ids == []

    def test_id_less_guidance_is_keyed_by_text_and_not_reported_as_an_id(self):
        planned = plan(
            base_history(), ContextSources(guidance=[guidance(None, "No id.")])
        )
        entry = only(planned, "guidance")
        assert entry_meta(entry)["items"][0]["key"] == "sha:" + digest("No id.")
        assert planned.guidance_ids == []

    def test_empty_text_is_skipped(self):
        planned = plan(base_history(), ContextSources(guidance=[guidance("g1", "  ")]))
        assert planned.entries == []


# --- Whole plans ----------------------------------------------------------------------


def _all_sources(turn: int = 4) -> ContextSources:
    return ContextSources(
        charter=CHARTER,
        memory_summary=SUMMARY,
        memory_records=[mem(1), mem(2)],
        knowledge_records=[note("n1", "kb body")],
        failed_citations=[citation("c1")],
        guidance=[guidance("g1", "Steer.")],
        subagents=SUBAGENTS,
        turn_boundary=BOUNDARY,
        turn=turn,
    )


def _compact(history: List[BaseMessage]) -> List[BaseMessage]:
    """Simulate compaction: the summary replaces everything but the system prompt."""
    return [history[0], HumanMessage(content="[summary] earlier work")]


class TestWholePlan:
    def test_entries_follow_injection_kinds_order(self):
        planned = plan(base_history(), _all_sources())
        assert kinds(planned) == list(INJECTION_KINDS)

    def test_every_entry_carries_the_schema(self):
        for entry in plan(base_history(), _all_sources(turn=4)).entries:
            meta = entry_meta(entry)
            assert isinstance(entry, HumanMessage)
            assert entry.additional_kwargs[PERSIST_ROLE_KEY] == PERSIST_ROLE_CONTEXT
            assert meta["v"] == 1
            assert meta["visible"] is False
            assert meta["turn"] == 4
            assert re.fullmatch(r"[0-9a-f]{16}", meta["hash"])
            assert entry.content.startswith(f'<srw_context kind="{meta["kind"]}">\n')
            assert entry.content.endswith("\n</srw_context>")
            if meta["kind"] in ("memory", "knowledge", "guidance"):
                assert meta["section"] == meta["kind"]
                assert meta["items"]
                for item in meta["items"]:
                    assert set(item) == {"key", "hash", "handle"}
                    assert re.fullmatch(r"[0-9a-f]{16}", item["hash"])
            else:
                assert meta["items"] == []
                assert meta["hash"] == digest(entry_body(entry))

    def test_workers_have_no_turn(self):
        sources = _all_sources()
        sources.turn = None
        sources.turn_boundary = ""
        for entry in plan(base_history(), sources).entries:
            assert entry_meta(entry)["turn"] is None

    def test_deterministic(self):
        first = plan(base_history(), _all_sources())
        second = plan(base_history(), _all_sources())
        assert [e.content for e in first.entries] == [e.content for e in second.entries]
        assert [e.additional_kwargs for e in first.entries] == [
            e.additional_kwargs for e in second.entries
        ]

    def test_the_plan_is_quiet_once_appended(self):
        history = base_history()
        history += plan(history, _all_sources()).entries
        sources = _all_sources()
        sources.delivered_guidance_ids = ["g1"]
        quiet = plan(history, sources)
        assert quiet.entries == []
        assert quiet.memory_present == 2

    def test_re_eligible_after_compaction(self):
        history = base_history()
        history += plan(history, _all_sources()).entries
        history += [AIMessage(content="answer"), HumanMessage(content="next")]

        compacted = _compact(history)
        sources = _all_sources()
        # Guidance was delivered (and the summarizer keeps it, O1): it does
        # not come back. Everything else the sources still hold does.
        sources.delivered_guidance_ids = ["g1"]
        replanned = plan(compacted, sources)
        assert kinds(replanned) == [k for k in INJECTION_KINDS if k != "guidance"]
        assert replanned.memory_appended == 2
        assert UPDATED_ITEM_MARKER not in "".join(e.content for e in replanned.entries)

    def test_planned_entries_fold_into_the_tool_result_carrier(self):
        history = base_history() + [
            AIMessage(content="", tool_calls=[{"name": "t", "args": {}, "id": "c1"}]),
            ToolMessage(content="tool output", tool_call_id="c1"),
        ]
        planned = plan(
            history, ContextSources(memory_records=[mem(1)], charter=CHARTER)
        )
        folded = fold_context_entries(history + planned.entries)
        assert len(folded) == len(history)
        assert folded[-1].content == "\n\n".join(
            ["tool output"] + [e.content for e in planned.entries]
        )

    @pytest.mark.parametrize("trailing_entries", [0, 2], ids=["bare", "after-entries"])
    def test_nothing_is_planned_after_an_open_tool_call(self, trailing_entries):
        """An AIMessage with calls but no results is no carrier.

        The fold drops anything between a call and its results, so entries
        planned there would be recorded as present (and persisted) without
        ever reaching the provider. Entries already stored after the open
        call do not make it a carrier either.
        """
        history = base_history() + [
            AIMessage(content="", tool_calls=[{"name": "t", "args": {}, "id": "c1"}])
        ]
        history += [
            make_context_entry("memory", f"stale {n}", section="memory")
            for n in range(trailing_entries)
        ]

        planned = plan(history, _all_sources())

        assert planned.entries == []
        assert planned.guidance_ids == []
        assert planned.memory_appended == 0

    def test_planning_resumes_once_the_results_are_in(self):
        history = base_history() + [
            AIMessage(content="", tool_calls=[{"name": "t", "args": {}, "id": "c1"}])
        ]
        assert plan(history, _all_sources()).entries == []

        history.append(ToolMessage(content="out", tool_call_id="c1"))
        planned = plan(history, _all_sources())

        assert kinds(planned) == list(INJECTION_KINDS)
        folded = fold_context_entries(history + planned.entries)
        assert folded[-1].content.startswith("out\n\n<srw_context")

    def test_a_text_answer_is_still_a_carrier(self):
        """Only an open call blocks planning; after a text-only answer the
        entries stand alone (the fold's degenerate case)."""
        history = base_history() + [AIMessage(content="done")]
        assert kinds(plan(history, _all_sources())) == list(INJECTION_KINDS)

    def test_a_failing_renderer_skips_only_its_kind(self, monkeypatch):
        from shared.runtime.services.recall_store import RecallStore

        def boom(*_args, **_kwargs):
            raise RuntimeError("renderer broke")

        monkeypatch.setattr(RecallStore, "render_memory_entry", boom)
        planned = plan(base_history(), _all_sources())
        assert "memory" not in kinds(planned)
        assert "charter" in kinds(planned) and "knowledge" in kinds(planned)
        assert planned.memory_appended == 0

    def test_is_append_only_is_re_exported(self):
        assert planner_module.is_append_only is is_append_only
        config = SimpleNamespace(
            context_management=SimpleNamespace(injection_mode="append_only")
        )
        assert planner_module.is_append_only(config)


@pytest.mark.parametrize("kind", ["memory", "knowledge", "guidance"])
def test_item_kinds_never_list_an_item_twice_in_one_entry(kind):
    sources = {
        "memory": ContextSources(memory_records=[mem(1), mem(1), mem(2)]),
        "knowledge": ContextSources(
            knowledge_records=[note("n1", "a"), note("n1", "a"), note("n2", "b")]
        ),
        "guidance": ContextSources(
            guidance=[guidance("g1", "a"), guidance("g1", "a"), guidance("g2", "b")]
        ),
    }[kind]
    keys = [
        i["key"] for i in entry_meta(only(plan(base_history(), sources), kind))["items"]
    ]
    assert len(keys) == len(set(keys)) == 2
