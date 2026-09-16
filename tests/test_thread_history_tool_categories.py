"""The thread-history endpoint stamps each replayed tool call with its category.

The live SSE `tool.started` frame carries `category` (graph.py's
`_get_tool_category`), but `thread_messages.tool_calls` never stored it. Without
the stamp the cockpit's folded-chip summary buckets every replayed call as
"other", so the same turn reads "19x citations - 12x searches" while streaming
and "38x steps" after a reload.

See knowledge-base/knowledge/features/session_turn_rendering.md "Phase 2 - the live edge".
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.main import _stamp_tool_categories, get_thread_messages_history


class _AsyncContext:
    def __init__(self, value=None):
        self.value = value

    async def __aenter__(self):
        return self.value

    async def __aexit__(self, *_args):
        return False


def _patched(db):
    """Patch the endpoint's module-level deps: auth + the db singleton."""
    owner = AsyncMock(return_value=({"id": "u1"}, {"id": "t1", "user_id": "u1"}))
    conn = MagicMock()
    conn.transaction.return_value = _AsyncContext()
    conn.fetchrow = AsyncMock(
        return_value={"events_epoch": 2, "conversation_revision": 3}
    )
    db.acquire.return_value = _AsyncContext(conn)
    return (
        patch("orchestrator.main.require_thread_owner", owner),
        patch("orchestrator.main.postgres_db", db),
    )


def _msg(*tool_calls):
    return {"role": "ai", "content": None, "tool_calls": list(tool_calls) or None}


class TestStampToolCategories:
    """The pure helper. Asserts against the REAL registry, not a mock — the
    whole point is that history agrees with what the live path emits, so a
    mocked category would test nothing."""

    def test_stamps_the_registry_category_onto_each_call(self):
        messages = [_msg({"name": "cite_web", "args": {}, "id": "1"})]
        _stamp_tool_categories(messages)
        assert messages[0]["tool_calls"][0]["category"] == "citation"

    def test_categories_match_the_live_paths_source(self):
        # These are the buckets the chip renders; if the registry moves, this
        # test moves with it rather than silently drifting from the SSE frame.
        from agent.tools.registry import TOOL_REGISTRY

        messages = [
            _msg(
                {"name": "web_search", "args": {}, "id": "1"},
                {"name": "run_command", "args": {}, "id": "2"},
                {"name": "read_file", "args": {}, "id": "3"},
            )
        ]
        _stamp_tool_categories(messages)
        for tc in messages[0]["tool_calls"]:
            assert tc["category"] == TOOL_REGISTRY[tc["name"]]["category"]

    def test_leaves_unknown_tools_uncategorised(self):
        # Renamed/removed tool, or a row from another deployment. No category is
        # the honest answer — the cockpit buckets it as "other".
        messages = [_msg({"name": "no_such_tool_xyz", "args": {}, "id": "1"})]
        _stamp_tool_categories(messages)
        assert "category" not in messages[0]["tool_calls"][0]

    def test_tolerates_a_call_with_no_name(self):
        messages = [_msg({"args": {}, "id": "1"})]
        _stamp_tool_categories(messages)  # must not raise
        assert "category" not in messages[0]["tool_calls"][0]

    def test_tolerates_messages_without_tool_calls(self):
        # role='human'/'tool' rows carry tool_calls=None.
        messages = [
            {"role": "human", "content": "hi", "tool_calls": None},
            {"role": "ai"},
        ]
        _stamp_tool_categories(messages)  # must not raise
        assert messages[0]["tool_calls"] is None

    def test_mutates_in_place_and_returns_none(self):
        messages = [_msg({"name": "cite_web", "args": {}, "id": "1"})]
        assert _stamp_tool_categories(messages) is None
        assert messages[0]["tool_calls"][0]["category"] == "citation"


@pytest.mark.asyncio
class TestEndpointStamps:
    async def test_full_load_stamps_categories(self):
        db = MagicMock()
        db.get_thread_messages_history = AsyncMock(
            return_value=[_msg({"name": "cite_web", "args": {}, "id": "1"})]
        )
        p1, p2 = _patched(db)
        with p1, p2:
            out = await get_thread_messages_history("t1", MagicMock())
        assert out["messages"][0]["tool_calls"][0]["category"] == "citation"

    async def test_cursor_window_stamps_too(self):
        # The ?before/?after path returns from a different db call — it must not
        # be the one place the annotation is missing.
        db = MagicMock()
        db.get_thread_messages_page = AsyncMock(
            return_value=([_msg({"name": "web_search", "args": {}, "id": "1"})], False)
        )
        p1, p2 = _patched(db)
        with p1, p2:
            out = await get_thread_messages_history(
                "t1", MagicMock(), before="2026-07-15T00:00:00Z"
            )
        assert out["messages"][0]["tool_calls"][0]["category"] == "research"

    async def test_paged_read_stamps_too(self):
        db = MagicMock()
        db.get_thread_messages_history = AsyncMock(
            return_value=[_msg({"name": "run_command", "args": {}, "id": "1"})]
        )
        db.get_thread_message_count = AsyncMock(return_value=1)
        p1, p2 = _patched(db)
        with p1, p2:
            out = await get_thread_messages_history("t1", MagicMock(), limit=10)
        assert out["messages"][0]["tool_calls"][0]["category"] == "shell"
