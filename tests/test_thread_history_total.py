"""HF-7 (c) — the thread-history endpoint skips the per-open COUNT(*).

`total` is unread by the cockpit (persistent-chat.service.ts uses only
`.messages`) and the MCP tool, so the endpoint no longer issues a
get_thread_message_count on the hot paths:

* full load (thread open, no limit)  -> total = len(messages), no COUNT
* cursor window (?before / ?after)   -> total = len(messages), no COUNT
* explicit limit/offset paged read   -> COUNT retained (a paginating client
                                        may want the true total)
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.main import get_thread_messages_history


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


@pytest.mark.asyncio
async def test_full_load_skips_count():
    db = MagicMock()
    db.get_thread_messages_history = AsyncMock(return_value=[{"id": "a"}, {"id": "b"}])
    db.get_thread_message_count = AsyncMock(return_value=999)
    p_owner, p_db = _patched(db)
    with p_owner, p_db:
        result = await get_thread_messages_history("t1", MagicMock())
    # total comes free from the full transcript, not a COUNT(*).
    assert result["total"] == 2
    assert result["events_epoch"] == 2
    assert result["conversation_revision"] == 3
    assert db.get_thread_message_count.call_count == 0, "no per-open COUNT on full load"


@pytest.mark.asyncio
async def test_cursor_window_skips_count():
    db = MagicMock()
    db.get_thread_messages_page = AsyncMock(return_value=([{"id": "a"}], False))
    db.get_thread_message_count = AsyncMock(return_value=999)
    p_owner, p_db = _patched(db)
    with p_owner, p_db:
        result = await get_thread_messages_history(
            "t1", MagicMock(), after="2026-07-03T00:00:00Z"
        )
    assert result["total"] == 1
    assert result["has_more"] is False
    assert db.get_thread_message_count.call_count == 0, "no COUNT on a cursor window"


@pytest.mark.asyncio
async def test_explicit_paged_read_keeps_true_count():
    db = MagicMock()
    db.get_thread_messages_history = AsyncMock(return_value=[{"id": "a"}, {"id": "b"}])
    db.get_thread_message_count = AsyncMock(return_value=57)
    p_owner, p_db = _patched(db)
    with p_owner, p_db:
        result = await get_thread_messages_history("t1", MagicMock(), limit=2)
    # A paginating client asked for a window -> the true total is still served.
    assert result["total"] == 57
    assert result["has_more"] is True  # full page implies more
    assert db.get_thread_message_count.call_count == 1
