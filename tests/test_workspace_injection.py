"""Tests for workspace injection — synthetic tool messages for transient injection.

Tests create_todos_human_message, create_instruction_tool_messages,
and is_workspace_injection_message.
"""

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage

from shared.runtime.core.message_markers import (
    INSTRUCTION_PATH_KEY,
    PERSIST_ROLE_KEY,
    PHASE_KEY,
    PROTECTED_KEY,
    is_protected_message,
    protected_identity,
    protected_path,
    protected_phase_key,
)
from shared.runtime.core.workspace_injection import (
    INSTRUCTION_TOOL_CALL_ID_PREFIX,
    PHASE_INSTRUCTION_CONTENT_PREFIX,
    TODOS_INJECTION_CONTENT_PREFIX,
    content_hash_id,
    create_phase_instruction_message,
    create_todos_human_message,
    create_instruction_tool_messages,
    is_workspace_injection_message,
)


# =============================================================================
# create_todos_human_message
# =============================================================================


class TestCreateTodosHumanMessage:
    """Tests for todo list injection as transient HumanMessage."""

    def test_returns_human_message(self):
        msg = create_todos_human_message("- [ ] Task 1")
        assert isinstance(msg, HumanMessage)

    def test_content_has_prefix(self):
        msg = create_todos_human_message("- [ ] Task 1")
        assert msg.content.startswith(TODOS_INJECTION_CONTENT_PREFIX)

    def test_content_has_closing_tag(self):
        msg = create_todos_human_message("todos here")
        assert msg.content.endswith("</active_tasks>")

    def test_contains_todo_content(self):
        msg = create_todos_human_message("- [x] Done\n- [ ] Pending")
        assert "- [x] Done" in msg.content
        assert "- [ ] Pending" in msg.content


# =============================================================================
# create_instruction_tool_messages
# =============================================================================


class TestCreateInstructionToolMessages:
    """Tests for instruction file injection as fake tool call."""

    def test_returns_ai_and_tool_message_pair(self):
        ai_msg, tool_msg = create_instruction_tool_messages("guide.md", "# Guide")
        assert isinstance(ai_msg, AIMessage)
        assert isinstance(tool_msg, ToolMessage)

    def test_tool_call_id_has_instruction_prefix(self):
        ai_msg, tool_msg = create_instruction_tool_messages("file.md", "content")
        tc_id = ai_msg.tool_calls[0]["id"]
        assert tc_id.startswith(INSTRUCTION_TOOL_CALL_ID_PREFIX)
        assert tool_msg.tool_call_id == tc_id

    def test_tool_call_uses_correct_path(self):
        ai_msg, _ = create_instruction_tool_messages("instructions/todo_guide.md", "x")
        tc = ai_msg.tool_calls[0]
        assert tc["args"]["path"] == "instructions/todo_guide.md"

    def test_tool_message_contains_instruction_content(self):
        _, tool_msg = create_instruction_tool_messages("f.md", "instruction body")
        assert tool_msg.content == "instruction body"


# =============================================================================
# is_workspace_injection_message
# =============================================================================


class TestIsWorkspaceInjectionMessage:
    """Tests for detecting transient injection messages."""

    def test_detects_instruction_tool_message(self):
        _, tool_msg = create_instruction_tool_messages("f.md", "x")
        assert is_workspace_injection_message(tool_msg) is True

    def test_detects_instruction_ai_message(self):
        ai_msg, _ = create_instruction_tool_messages("f.md", "x")
        assert is_workspace_injection_message(ai_msg) is True

    def test_detects_todos_human_message(self):
        msg = create_todos_human_message("todos")
        assert is_workspace_injection_message(msg) is True

    def test_regular_human_message_not_injection(self):
        msg = HumanMessage(content="Hello, help me with this task.")
        assert is_workspace_injection_message(msg) is False

    def test_regular_ai_message_not_injection(self):
        msg = AIMessage(content="Sure, I'll help you.")
        assert is_workspace_injection_message(msg) is False

    def test_regular_tool_message_not_injection(self):
        msg = ToolMessage(content="file contents", tool_call_id="call_abc123")
        assert is_workspace_injection_message(msg) is False

    def test_system_message_not_injection(self):
        msg = SystemMessage(content="You are an agent.")
        assert is_workspace_injection_message(msg) is False

    def test_ai_with_regular_tool_calls_not_injection(self):
        msg = AIMessage(
            content="",
            tool_calls=[{"name": "read_file", "args": {}, "id": "call_regular"}],
        )
        assert is_workspace_injection_message(msg) is False


# =============================================================================
# content_hash_id — deterministic injection tool_call_ids
# =============================================================================


class TestContentHashId:
    """Injection tool_call_ids must be deterministic (prompt-cache hygiene).

    Identical injected content must produce byte-identical payloads so
    provider prefix caches can match; uuid4-based ids made every request
    unique even when nothing changed.
    """

    def test_same_content_same_id(self):
        assert content_hash_id("abc") == content_hash_id("abc")

    def test_different_content_different_id(self):
        assert content_hash_id("abc") != content_hash_id("abd")

    def test_length_and_charset(self):
        h = content_hash_id("anything")
        assert len(h) == 8
        assert all(c in "0123456789abcdef" for c in h)

    def test_instruction_pair_deterministic(self):
        ai1, tool1 = create_instruction_tool_messages("guide.md", "content")
        ai2, tool2 = create_instruction_tool_messages("guide.md", "content")
        assert ai1.tool_calls[0]["id"] == ai2.tool_calls[0]["id"]
        assert tool1.tool_call_id == tool2.tool_call_id

    def test_instruction_pair_varies_by_path(self):
        _, tool1 = create_instruction_tool_messages("a.md", "content")
        _, tool2 = create_instruction_tool_messages("b.md", "content")
        assert tool1.tool_call_id != tool2.tool_call_id

    def test_memory_pair_deterministic(self):
        from tests._legacy_injection_rows import create_memory_injection_messages

        _, tool1 = create_memory_injection_messages("memories")
        _, tool2 = create_memory_injection_messages("memories")
        assert tool1.tool_call_id == tool2.tool_call_id

    def test_knowledge_pair_deterministic(self):
        from tests._legacy_injection_rows import create_knowledge_injection_messages

        _, tool1 = create_knowledge_injection_messages("notes")
        _, tool2 = create_knowledge_injection_messages("notes")
        assert tool1.tool_call_id == tool2.tool_call_id

    def test_citation_pair_deterministic(self):
        from tests._legacy_injection_rows import (
            create_citation_feedback_injection_messages,
        )

        _, tool1 = create_citation_feedback_injection_messages("failed")
        _, tool2 = create_citation_feedback_injection_messages("failed")
        assert tool1.tool_call_id == tool2.tool_call_id


# =============================================================================
# create_phase_instruction_message — persistent, protected phase block
# =============================================================================


class TestCreatePhaseInstructionMessage:
    """The phase_start delivery shape (U2 WP1): a HumanMessage carrying the
    protected markers, persisted in state — not a transient injection."""

    PATH = "skills/research-guide/SKILL.md"

    def _block(self, content="You are entering a tactical phase."):
        return create_phase_instruction_message(
            self.PATH, content, "tactical", "2:tactical"
        )

    def test_is_human_message_with_role_event(self):
        msg = self._block()
        assert isinstance(msg, HumanMessage)
        assert msg.additional_kwargs[PERSIST_ROLE_KEY] == "event"

    def test_marker_contract(self):
        msg = self._block()
        assert msg.additional_kwargs == {
            PERSIST_ROLE_KEY: "event",
            PROTECTED_KEY: True,
            PHASE_KEY: "2:tactical",
            INSTRUCTION_PATH_KEY: self.PATH,
        }
        assert is_protected_message(msg) is True
        assert protected_phase_key(msg) == "2:tactical"
        assert protected_path(msg) == self.PATH
        assert protected_identity(msg) == ("2:tactical", self.PATH)

    def test_content_names_phase_path_and_body(self):
        msg = self._block("BODY TEXT")
        assert msg.content.startswith(f"{PHASE_INSTRUCTION_CONTENT_PREFIX}tactical]")
        assert self.PATH in msg.content
        assert msg.content.endswith("BODY TEXT")
        assert "whole phase" in msg.content

    def test_deterministic(self):
        a, b = self._block(), self._block()
        assert a.content == b.content
        assert a.additional_kwargs == b.additional_kwargs
        # No id: the add_messages reducer assigns one on append, and a
        # RemoveMessage for an id that never reached state would raise.
        assert a.id is None

    def test_not_a_transient_injection(self):
        # It is history, not tail: summarisation must not drop it as transient
        # (that path filters without RemoveMessage markers and would orphan it).
        assert is_workspace_injection_message(self._block()) is False

    def test_plain_messages_are_not_protected(self):
        assert is_protected_message(HumanMessage(content="hi")) is False
        assert is_protected_message(AIMessage(content="x")) is False
        assert protected_phase_key(HumanMessage(content="hi")) is None
        assert protected_path(HumanMessage(content="hi")) is None
