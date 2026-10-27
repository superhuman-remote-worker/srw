"""F7 on a real PostgreSQL: a restored image turn sends the live bytes.

Design: knowledge-base/knowledge/features/append_only_context_injection.md
§3.6 (F7). Runs the agent's real upsert and history SQL against
``schema_current.sql`` (the fixtures of test_context_rows_pg.py); no
migration is involved, ``provider_raw`` and ``tool_calls`` are existing jsonb
columns.

- A tool's image row with a context entry, after a call whose arguments
  jsonb reorders, comes back byte-identical on the Chat Completions wire, and
  its ``content`` still matches the text the cockpit and the rewind sweep
  recognise it by.
- The serializer keeps a call's argument text exactly when Postgres hands
  its keys back in another order (the unit tests emulate that order).
"""

from __future__ import annotations

import json
from typing import Any, Dict
from uuid import UUID, uuid4

import pytest
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

from agent.api.persistent_app import _db_rows_to_lc_messages
from agent.core.thread_messages import (
    ARGUMENTS_KEY,
    _serialize_message_row,
    restored_tool_call_args,
)
from tests.test_context_rows_pg import (  # noqa: F401  (pytest fixtures)
    _schema_applied,
    _thread,
    db,
    pg_dsn,
)
from tests.test_session_restore_byte_exact import (
    MARKER,
    _call,
    _image_row,
    _memory_entry,
    _wire,
)

# The anchored pattern the cockpit hides these rows by, and that migration
# 0211 and the rewind sweep (src/shared/thread_rewind.py) match.
IMAGE_ROW_PATTERN = r"^Image content from tool call [^[:space:]]+:[[:space:]]*$"


async def _save(store, thread_id: str, message, turn: int = 1) -> None:
    await store.save_thread_message(
        thread_id=thread_id, **_serialize_message_row(message, turn)
    )


@pytest.mark.asyncio
async def test_an_image_turn_comes_back_byte_identical(db):  # noqa: F811 (fixture)
    thread_id = await _thread(db)
    image = _image_row(turn=1)
    live = [
        HumanMessage(content="Fix the screenshot's colour.", id=str(uuid4())),
        _call(
            {
                "path": "app.css",
                "old_string": "red",
                "new_string": "blue",
                "options": {"strict": True, "all": False},
            },
            turn=1,
        ),
        ToolMessage(
            content="ok\n[image attached: image/png]",
            tool_call_id="call_1",
            id=str(uuid4()),
        ),
        image,
        _memory_entry(turn=1),
        AIMessage(content="The square is blue now.", id=str(uuid4())),
    ]
    for message in live:
        await _save(db, thread_id, message)
    # The turn-end reconcile writes the same rows again.
    await db.save_thread_messages(
        thread_id, [_serialize_message_row(message, 1) for message in live]
    )

    rows = await db.get_thread_messages_history(
        thread_id, order_by_seq=True, include_provider_raw=True
    )
    restored = _db_rows_to_lc_messages(rows)

    assert _wire(restored) == _wire(live)
    assert restored[3].content == image.content
    async with db.acquire() as conn:
        content, hidden = await conn.fetchrow(
            "SELECT content, content ~ $2 FROM thread_messages WHERE id = $1",
            UUID(image.id),
            IMAGE_ROW_PATTERN,
        )
    assert (content, hidden) == (MARKER, True)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("args", "postgres_reorders"),
    [
        ({"path": "a.md", "why": "look"}, True),
        ({"path": "a.md", "content": "text"}, False),
        # "é" is two bytes: it sorts with the two-letter keys, after them.
        ({"zz": 1, "é": 2, "b": 3, "ab": 4}, True),
        ({"todos": [{"status": "open", "content": "x", "id": 1}]}, True),
        ({"path": "a.md", "options": {"bb": 1, "a": 2}}, True),
    ],
    ids=["shorter-key-last", "in-order", "byte-length", "nested-list", "nested-object"],
)
async def test_argument_text_is_kept_exactly_when_postgres_reorders(
    db,  # noqa: F811 (fixture)
    args,
    postgres_reorders,
):
    thread_id = await _thread(db)
    call = _call(args, turn=1)
    await _save(db, thread_id, call)

    async with db.acquire() as conn:
        stored = await conn.fetchval(
            "SELECT tool_calls FROM thread_messages WHERE id = $1", UUID(call.id)
        )
    (entry,) = json.loads(stored)
    assert (json.dumps(entry["args"]) != json.dumps(args)) == postgres_reorders

    assert (ARGUMENTS_KEY in entry) == postgres_reorders
    restored: Dict[str, Any] = restored_tool_call_args(entry)
    assert json.dumps(restored) == json.dumps(args)
