"""Static entry renderers and their configuration (WP2 spec §C, §G; D11, D29).

An appended entry stays in the history unchanged, so its text carries no
per-turn data: the memory line lost its ``pinned, N turns left`` clause
(the one legacy-visible change of sub-step 2.3, O12). The renderers gained
the handle label, the "(updated; ...)" marker and the cleared renderings;
the defaults keep the legacy bytes.
"""

from __future__ import annotations

import uuid
from pathlib import Path

import pytest
import yaml

from agent.core.citation_feedback_injection import format_citation_feedback_resolved
from agent.core.context_injection import format_active_subagents_none
from agent.core.guidance_injection import format_supervisor_guidance
from shared.runtime.core.context_entries import (
    UPDATED_ITEM_MARKER,
    knowledge_item_key,
    memory_handle,
)
from shared.runtime.core.loader import (
    MemoryConfig,
    load_agent_config,
    load_agent_config_from_dict,
)
from shared.runtime.services.guardrails import KNOWN_NUDGES, format_nudge
from shared.runtime.services.knowledge_store import KnowledgeRecord, KnowledgeStore
from shared.runtime.services.recall_store import MemoryRecord, RecallStore

_REPO = Path(__file__).resolve().parents[1]
_KB = uuid.UUID(int=0xB0)


def _memory(**overrides) -> MemoryRecord:
    fields = {
        "id": uuid.UUID(int=1),
        "content": "User prefers ruff with line length 88.",
        "memory_type": "preference",
        "importance": 0.8,
        "source_phase": 2,
    }
    fields.update(overrides)
    return MemoryRecord(**fields)


class TestFormatMemory:
    @pytest.mark.parametrize("remaining_turns", [None, 0, 1, 3, 10])
    def test_text_is_static_across_the_ttl(self, remaining_turns):
        rendered = RecallStore.format_memory(
            _memory(remaining_turns=remaining_turns), 1
        )
        assert rendered == (
            "[1] (importance: 0.8, phase 2, preference)\n"
            "User prefers ruff with line length 88."
        )

    def test_handle_labels_the_memory(self):
        record = _memory()
        handle = memory_handle(record.id)
        rendered = RecallStore.format_memory(record, 4, handle=handle)
        assert rendered.startswith(f"[{handle}] (importance: 0.8")
        assert "[4]" not in rendered

    def test_updated_adds_the_marker(self):
        rendered = RecallStore.format_memory(_memory(), 1, updated=True)
        assert rendered.splitlines()[0] == (
            f"[1] (importance: 0.8, phase 2, preference) {UPDATED_ITEM_MARKER}"
        )
        assert UPDATED_ITEM_MARKER == "(updated; replaces the earlier version above)"


class TestRenderMemoryEntry:
    def test_header_then_one_block_per_memory(self):
        first = _memory()
        second = _memory(id=uuid.UUID(int=2), content="Second.", memory_type="factual")
        body = RecallStore.render_memory_entry(
            [
                (first, memory_handle(first.id), False),
                (second, memory_handle(second.id), True),
            ]
        )
        assert body == "\n\n".join(
            [
                format_nudge("memory_entry_header"),
                RecallStore.format_memory(first, 1, handle=memory_handle(first.id)),
                RecallStore.format_memory(
                    second, 2, handle=memory_handle(second.id), updated=True
                ),
            ]
        )

    def test_no_pinned_split_and_no_token_footer(self):
        body = RecallStore.render_memory_entry(
            [(_memory(remaining_turns=5, token_count=40), None, False)]
        )
        assert "Pinned" not in body
        assert "Retrieved" not in body
        assert "End Memories" not in body
        assert "tokens" not in body

    def test_empty_renders_nothing(self):
        assert RecallStore.render_memory_entry([]) == ""


def _note(**overrides) -> KnowledgeRecord:
    fields = {
        "note_id": "deploy",
        "kb_id": _KB,
        "project_id": _KB,
        "title": "Deploy",
        "note_type": "decision",
        "confidence": "high",
        "tags": ["deploy"],
        "content": "Use helm upgrade, never kubectl patch.",
    }
    fields.update(overrides)
    return KnowledgeRecord(**fields)


class TestKnowledgeRenderers:
    def test_format_note_default_is_unchanged(self):
        assert KnowledgeStore.format_note(_note(), 1) == (
            "[1] (decision, high confidence) Tags: deploy\n"
            "Use helm upgrade, never kubectl patch."
        )

    def test_format_note_updated_marker(self):
        rendered = KnowledgeStore.format_note(_note(), 1, updated=True)
        assert rendered.splitlines()[0] == (
            f"[1] (decision, high confidence) Tags: deploy {UPDATED_ITEM_MARKER}"
        )

    def test_updated_keys_mark_only_the_matching_note(self):
        changed = _note()
        fresh = _note(note_id="auth", title="Auth", content="Keycloak needs openid.")
        block = KnowledgeStore.assemble_knowledge_block(
            [changed, fresh], updated_keys={knowledge_item_key(changed)}
        )
        marked = [line for line in block.splitlines() if UPDATED_ITEM_MARKER in line]
        assert marked == [
            f"[1] (decision, high confidence) Tags: deploy {UPDATED_ITEM_MARKER}"
        ]

    def test_without_updated_keys_the_block_is_the_legacy_block(self):
        notes = [_note(), _note(note_id="auth", content="x")]
        assert KnowledgeStore.assemble_knowledge_block(
            notes, updated_keys=set()
        ) == KnowledgeStore.assemble_knowledge_block(notes)


class TestGuidanceRenderer:
    ENTRIES = [{"id": "g1", "text": "Focus.", "source": "officer", "created_at": "t0"}]

    def test_default_keeps_the_legacy_repeat_notice(self):
        rendered = format_supervisor_guidance(self.ENTRIES)
        assert rendered == (
            "[SUPERVISOR GUIDANCE] Mid-run guidance from your supervisor. "
            "Your current plan and todos remain in force — fold this guidance "
            "into the work in progress instead of re-planning. It may repeat "
            "for a turn or two until delivery is confirmed; act on it once."
            "\n\n- Focus. (officer, t0)"
        )
        assert format_supervisor_guidance(self.ENTRIES, repeat_notice=True) == rendered

    def test_append_only_drops_the_repeat_notice(self):
        rendered = format_supervisor_guidance(self.ENTRIES, repeat_notice=False)
        assert "may repeat" not in rendered
        assert "act on it once" not in rendered
        assert rendered == (
            "[SUPERVISOR GUIDANCE] Mid-run guidance from your supervisor. "
            "Your current plan and todos remain in force — fold this guidance "
            "into the work in progress instead of re-planning."
            "\n\n- Focus. (officer, t0)"
        )

    def test_nothing_to_render_is_empty_either_way(self):
        assert format_supervisor_guidance([], repeat_notice=False) == ""


class TestClearedRenderings:
    @pytest.mark.parametrize(
        "key",
        ["memory_entry_header", "citation_feedback_resolved", "active_subagents_none"],
    )
    def test_nudges_are_registered_without_placeholders(self, key):
        assert KNOWN_NUDGES[key] == set()
        text = format_nudge(key)
        assert text and "{" not in text

    def test_citation_cleared_rendering(self):
        assert format_citation_feedback_resolved() == format_nudge(
            "citation_feedback_resolved"
        )

    def test_subagents_cleared_rendering(self):
        assert format_active_subagents_none() == format_nudge("active_subagents_none")

    @pytest.mark.parametrize(
        "key",
        ["memory_entry_header", "citation_feedback_resolved", "active_subagents_none"],
    )
    def test_no_per_turn_data(self, key):
        text = format_nudge(key)
        for token in ("{", "turns left", "tokens", "TTL"):
            assert token not in text


def _minimal(**overrides):
    return {"agent_id": "test_agent", "display_name": "Test Agent", **overrides}


class TestMaxMemoriesPerEntry:
    def test_dataclass_default(self):
        assert MemoryConfig().max_memories_per_entry == 5

    def test_dict_loader_default(self):
        config = load_agent_config_from_dict(_minimal())
        assert config.memory.max_memories_per_entry == 5

    def test_dict_loader_override(self):
        config = load_agent_config_from_dict(
            _minimal(memory={"max_memories_per_entry": 3})
        )
        assert config.memory.max_memories_per_entry == 3

    @pytest.mark.parametrize("value", [0, -1, "many", True, None])
    def test_rejects_non_positive_or_non_integer(self, value):
        with pytest.raises(ValueError, match="max_memories_per_entry"):
            load_agent_config_from_dict(
                _minimal(memory={"max_memories_per_entry": value})
            )

    @pytest.mark.parametrize("role_base", ["worker_base", "session_base"])
    def test_bundled_role_bases(self, role_base):
        config = load_agent_config(str(_REPO / "config" / f"{role_base}.yaml"))
        assert config.memory.max_memories_per_entry == 5

    def test_file_loader_override(self, tmp_path):
        leaf = tmp_path / "leaf.yaml"
        leaf.write_text(
            yaml.safe_dump(
                {
                    "$extends": "worker_base",
                    "agent_id": "cap_test",
                    "display_name": "Cap test",
                    "memory": {"max_memories_per_entry": 2},
                }
            )
        )
        config = load_agent_config(str(leaf))
        assert config.memory.max_memories_per_entry == 2
        # The rest of the section still comes from the chain.
        assert config.memory.budget_tokens == 4000

    def test_the_shared_root_states_the_key(self):
        root = yaml.safe_load((_REPO / "config" / "expert_base.yaml").read_text())
        assert root["memory"]["max_memories_per_entry"] == 5

    def test_survives_an_asdict_round_trip(self):
        # Dispatch paths re-parse a live config through dataclasses.asdict().
        import dataclasses

        config = load_agent_config_from_dict(
            _minimal(memory={"max_memories_per_entry": 4})
        )
        again = load_agent_config_from_dict(
            _minimal(memory=dataclasses.asdict(config.memory))
        )
        assert again.memory.max_memories_per_entry == 4
