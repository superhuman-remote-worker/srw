"""HF-7 (c) — the thread-history endpoint skips the per-open COUNT(*).

`total` is unread by the cockpit (persistent-chat.service.ts uses only
`.messages`) and the MCP tool, so the endpoint no longer issues a
get_thread_message_count on the hot paths:

* full load (thread open, no limit)  -> total = len(messages), no COUNT
* cursor window (?before / ?after)   -> total = len(messages), no COUNT
* explicit limit/offset paged read   -> COUNT retained (a paginating client
                                        may want the true total)
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from orchestrator.routers import thread_history
from orchestrator.routers.thread_history import get_thread_messages_history


class _AsyncContext:
    def __init__(self, value=None):
        self.value = value

    async def __aenter__(self):
        return self.value

    async def __aexit__(self, *_args):
        return False


def _dependencies(db):
    """The router's application collaborators: the owner gate and the store."""
    owner = AsyncMock(return_value=({"id": "u1"}, {"id": "t1", "user_id": "u1"}))
    conn = MagicMock()
    conn.transaction.return_value = _AsyncContext()
    conn.fetchrow = AsyncMock(
        return_value={"events_epoch": 2, "conversation_revision": 3}
    )
    db.acquire.return_value = _AsyncContext(conn)
    return thread_history.ThreadHistoryDependencies(
        store=db,
        vector_db=None,  # the history read never touches the vector store
        require_thread_owner=owner,
    )


@pytest.mark.asyncio
async def test_full_load_skips_count():
    db = MagicMock()
    db.get_thread_messages_history = AsyncMock(return_value=[{"id": "a"}, {"id": "b"}])
    db.get_thread_message_count = AsyncMock(return_value=999)
    result = await get_thread_messages_history(
        "t1", MagicMock(), dependencies=_dependencies(db)
    )
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
    result = await get_thread_messages_history(
        "t1",
        MagicMock(),
        after="2026-07-03T00:00:00Z",
        dependencies=_dependencies(db),
    )
    assert result["total"] == 1
    assert result["has_more"] is False
    assert db.get_thread_message_count.call_count == 0, "no COUNT on a cursor window"


@pytest.mark.asyncio
async def test_explicit_paged_read_keeps_true_count():
    db = MagicMock()
    db.get_thread_messages_history = AsyncMock(return_value=[{"id": "a"}, {"id": "b"}])
    db.get_thread_message_count = AsyncMock(return_value=57)
    result = await get_thread_messages_history(
        "t1", MagicMock(), limit=2, dependencies=_dependencies(db)
    )
    # A paginating client asked for a window -> the true total is still served.
    assert result["total"] == 57
    assert result["has_more"] is True  # full page implies more
    assert db.get_thread_message_count.call_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "cursor",
    [
        {"before": "not-a-timestamp"},
        {"after": "2026-13-45T00:00:00Z"},
        {"before": "2026-07-03T00:00:00Z", "after": "2026-07-03T00:00:00Z"},
    ],
)
async def test_a_bad_cursor_is_a_400_before_any_read(cursor):
    """Unparseable or conflicting cursors are refused before the snapshot read."""
    from fastapi import HTTPException

    db = MagicMock()
    with pytest.raises(HTTPException) as exc:
        await get_thread_messages_history(
            "t1", MagicMock(), dependencies=_dependencies(db), **cursor
        )
    assert exc.value.status_code == 400
    db.acquire.assert_not_called()
