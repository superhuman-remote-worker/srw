from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import pytest

from agent.database.postgres_db import PostgresDB


@pytest.mark.asyncio
async def test_agent_db_end_thread_closes_control_admission():
    """The direct-DB fallback is unavailable to pinned runtimes."""
    db = PostgresDB.__new__(PostgresDB)
    conn = AsyncMock()

    @asynccontextmanager
    async def acquire():
        yield conn

    db.acquire = acquire
    await db.end_thread("t1")

    sql = " ".join(conn.execute.await_args.args[0].split())
    assert "status = 'ended'" in sql
    assert "control_admission_agent_id = NULL" in sql
    assert "execution_lane <> 'pinned'" in sql


@pytest.mark.asyncio
async def test_agent_db_terminal_status_closes_control_admission():
    db = PostgresDB.__new__(PostgresDB)
    conn = AsyncMock()

    @asynccontextmanager
    async def acquire():
        yield conn

    db.acquire = acquire
    await db.update_thread_status("t1", "suspended")

    sql = " ".join(conn.execute.await_args.args[0].split())
    assert "WHEN $2 IN ('ended', 'suspended') THEN NULL" in sql
    assert "execution_lane = 'pinned'" in sql
    assert "$2 IN ('ending', 'ended', 'suspended')" in sql


@pytest.mark.asyncio
async def test_history_projects_only_resume_fields():
    """HF-7 thread-read diet: the resume reader returns exactly what the resume
    consumers use — id/role/content/tool_calls/tool_call_id/turn_number, plus
    the delivery's admitted_turn_number that orders the conversation — and does
    NOT fetch the resume-unused component columns (thinking/reasoning/
    tool_results/provider*/response_metadata/additional_kwargs/metrics/
    created_at). Those are never read on resume; the rebuilt AIMessage doesn't
    carry them. Stable message IDs are retained for exact subagent-call
    correlation across recovery."""
    db = PostgresDB.__new__(PostgresDB)  # bypass __init__/connection
    db.fetch = AsyncMock(
        return_value=[
            {
                "id": "00000000-0000-0000-0000-000000000001",
                "role": "tool",
                "content": "result",
                "tool_calls": None,
                "tool_call_id": "call_1",
                "turn_number": 2,
            }
        ]
    )
    rows = await db.get_thread_messages_history("t1")
    assert rows[0] == {
        "id": "00000000-0000-0000-0000-000000000001",
        "role": "tool",
        "content": "result",
        "tool_calls": None,
        "tool_call_id": "call_1",
        "turn_number": 2,
        "admitted_turn_number": None,
    }
    # The tool-result link survives (so _db_rows_to_lc_messages need not fall
    # back to positional pairing).
    assert rows[0]["tool_call_id"] == "call_1"
    # The SELECT projection must not re-introduce the resume-unused over-fetch.
    # (created_at legitimately stays in the ORDER BY, so scope the check to the
    # projection clause between SELECT and FROM.)
    sql = " ".join(db.fetch.call_args[0][0].split())
    projection = sql.split("FROM")[0]
    for dropped in (
        "reasoning",
        "tool_results",
        "provider_raw",
        "response_metadata",
        "additional_kwargs",
        "thinking",
        "metrics",
        "created_at",
    ):
        assert dropped not in projection, f"resume reader must not fetch {dropped}"


@pytest.mark.asyncio
async def test_history_excludes_summary_marker_rows():
    """Display-only role='summary' compaction markers must never re-enter the
    agent's resumed LLM context (they'd double-count its own summary)."""
    db = PostgresDB.__new__(PostgresDB)
    db.fetch = AsyncMock(return_value=[])
    await db.get_thread_messages_history("t1")
    sql = " ".join(db.fetch.call_args[0][0].split())
    assert "role NOT IN ('summary', 'error')" in sql


@pytest.mark.asyncio
async def test_history_excludes_cancelled_and_other_unadmitted_delivery_rows():
    """Cockpit keeps the transcript row; the agent restore must not model it."""
    db = PostgresDB.__new__(PostgresDB)
    db.fetch = AsyncMock(return_value=[])

    await db.get_thread_messages_history("t1")

    sql = " ".join(db.fetch.call_args[0][0].split())
    assert "thread_input_deliveries" in sql
    for state in ("persisted", "owned", "queued", "deferred", "cancelled"):
        assert f"'{state}'" in sql


@pytest.mark.asyncio
async def test_history_supports_since_turn_for_checkpoint_resume():
    """Resume-from-checkpoint loads only the tail (turn > B), skipping rows
    the persisted summary already covers. Without ``since_turn`` the query is
    unchanged."""
    db = PostgresDB.__new__(PostgresDB)
    db.fetch = AsyncMock(return_value=[])

    await db.get_thread_messages_history("t1", since_turn=5)
    sql = " ".join(db.fetch.call_args[0][0].split())
    assert "turn_number >" in sql, "since_turn must add a turn_number filter"
    assert "role NOT IN ('summary', 'error')" in sql, (
        "since_turn must preserve summary exclusion"
    )
    assert "ORDER BY turn_number ASC" in sql, "ordering must be preserved"
    # The boundary value is bound as a parameter, not interpolated.
    args = db.fetch.call_args[0]
    assert 5 in args[1:], f"boundary 5 must be a bound param, got args={args}"

    db.fetch = AsyncMock(return_value=[])
    await db.get_thread_messages_history("t1")
    sql_no_since = " ".join(db.fetch.call_args[0][0].split())
    assert "turn_number >" not in sql_no_since, (
        "default call (since_turn=None) must NOT filter by turn_number — "
        "back-compat with existing callers"
    )


@pytest.mark.asyncio
async def test_get_latest_compaction_checkpoint_parses_metrics():
    """Returns the latest summary row's content + boundary_turn from metrics."""
    db = PostgresDB.__new__(PostgresDB)
    db.fetchrow = AsyncMock(
        return_value={
            "content": "summary of prior work",
            # metrics may arrive as JSON string from asyncpg if codec isn't registered;
            # the method must handle both dict and str/bytes.
            "metrics": '{"before": 100, "after": 11, "trigger": "auto", "boundary_turn": 7}',
            "turn_number": 8,
        }
    )

    result = await db.get_latest_compaction_checkpoint("t1")

    assert result is not None
    assert result["summary"] == "summary of prior work"
    assert result["boundary_turn"] == 7
    assert result["turn_number"] == 8

    # SQL must select only summary rows, ordered newest-first.
    sql = " ".join(db.fetchrow.call_args[0][0].split())
    assert "role = 'summary'" in sql
    assert "ORDER BY turn_number DESC" in sql
    assert "LIMIT 1" in sql


@pytest.mark.asyncio
async def test_get_latest_compaction_checkpoint_returns_none_when_absent():
    """No summary row → None → restore falls back to Path B (full load)."""
    db = PostgresDB.__new__(PostgresDB)
    db.fetchrow = AsyncMock(return_value=None)
    assert await db.get_latest_compaction_checkpoint("t1") is None


@pytest.mark.asyncio
async def test_get_latest_compaction_checkpoint_handles_missing_boundary_turn():
    """Back-compat: rows written before this feature lack ``boundary_turn``."""
    db = PostgresDB.__new__(PostgresDB)
    db.fetchrow = AsyncMock(
        return_value={
            "content": "old phase-3 summary",
            "metrics": {"before": 50, "after": 8, "trigger": "manual"},
            "turn_number": 3,
        }
    )

    result = await db.get_latest_compaction_checkpoint("t1")
    assert result is not None
    assert result["boundary_turn"] is None, (
        "absent boundary_turn must surface as None so restore falls back to Path B"
    )


# ---------------------------------------------------------------------------
# Phase 3: seq-cursor resume (seq_gt) + boundary resolution
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_history_seq_gt_filters_and_orders_by_seq():
    """Resume on the seq cursor: seq > S, ordered by seq ASC (insertion order)."""
    db = PostgresDB.__new__(PostgresDB)
    db.fetch = AsyncMock(return_value=[])
    await db.get_thread_messages_history("t1", limit=None, seq_gt=42)
    sql = " ".join(db.fetch.call_args[0][0].split())
    assert "seq >" in sql, "seq_gt must add a seq filter"
    assert "ORDER BY seq ASC" in sql, "seq cursor must order by seq, not turn"
    assert "role NOT IN ('summary', 'error')" in sql, "summary exclusion preserved"
    assert 42 in db.fetch.call_args[0][1:], "boundary seq must be a bound param"


@pytest.mark.asyncio
async def test_history_seq_gt_follows_the_conversation_order_of_the_boundary():
    """Input typed during a tool call has a lower seq than the rest of its turn
    and belongs to the next one, so a summary that covered that turn never saw
    it. The cursor keeps every row written after the boundary row and adds
    every row of a later turn than the boundary row's."""
    db = PostgresDB.__new__(PostgresDB)
    db.fetch = AsyncMock(return_value=[])
    await db.get_thread_messages_history("t1", limit=None, seq_gt=42)
    sql = " ".join(db.fetch.call_args[0][0].split())
    row_turn = "COALESCE(delivery.admitted_turn_number, message.turn_number)"
    assert "boundary_message.seq = $2" in sql
    assert f"AND (seq > $2 OR {row_turn} > boundary.turn)" in sql


@pytest.mark.asyncio
async def test_history_without_seq_gt_keeps_turn_ordering():
    """Back-compat: no seq_gt → original turn/created_at ordering, no seq filter."""
    db = PostgresDB.__new__(PostgresDB)
    db.fetch = AsyncMock(return_value=[])
    await db.get_thread_messages_history("t1")
    sql = " ".join(db.fetch.call_args[0][0].split())
    assert "seq >" not in sql
    assert "ORDER BY turn_number ASC" in sql


@pytest.mark.asyncio
async def test_get_seq_for_message_id_coerces_and_returns_seq():
    import uuid

    from agent.database.postgres_db import _THREAD_MSG_ID_NS

    db = PostgresDB.__new__(PostgresDB)
    db.fetchrow = AsyncMock(return_value={"seq": 99})
    seq = await db.get_seq_for_message_id("t1", "chatcmpl-xyz")
    assert seq == 99
    sql = " ".join(db.fetchrow.call_args[0][0].split())
    assert "SELECT seq FROM thread_messages" in sql
    # The provider id is coerced to its UUID row id for the lookup.
    assert db.fetchrow.call_args[0][2] == str(
        uuid.uuid5(_THREAD_MSG_ID_NS, "chatcmpl-xyz")
    )


@pytest.mark.asyncio
async def test_get_seq_for_message_id_returns_none_when_absent():
    """Unpersisted boundary (transient injection / fresh resume id) → None."""
    db = PostgresDB.__new__(PostgresDB)
    db.fetchrow = AsyncMock(return_value=None)
    assert await db.get_seq_for_message_id("t1", "m_x") is None


@pytest.mark.asyncio
async def test_checkpoint_surfaces_boundary_seq():
    db = PostgresDB.__new__(PostgresDB)
    db.fetchrow = AsyncMock(
        return_value={
            "content": "summary",
            "metrics": '{"boundary_turn": 7, "boundary_seq": 780}',
            "turn_number": 8,
        }
    )
    result = await db.get_latest_compaction_checkpoint("t1")
    assert result["boundary_seq"] == 780
    assert result["boundary_turn"] == 7


# ---------------------------------------------------------------------------
# Phase 4: resume floor (newest_first)
# ---------------------------------------------------------------------------


def _full_row(mid, turn):
    """A thread_messages row carrying every DB column (the reader now projects
    only a subset; the extra keys are ignored, and are kept here to prove the
    reader tolerates a full row)."""
    return {
        "id": mid,
        "role": "ai",
        "content": f"c{mid}",
        "tool_calls": None,
        "turn_number": turn,
        "metrics": None,
        "tool_call_id": None,
        "thinking": None,
        "reasoning": None,
        "tool_results": None,
        "provider": None,
        "provider_raw": None,
        "additional_kwargs": None,
        "response_metadata": None,
        "created_at": None,
    }


@pytest.mark.asyncio
async def test_newest_first_selects_seq_desc_and_returns_chronological():
    """The resume floor takes the NEWEST N (seq DESC + LIMIT) but hands them
    back oldest→newest so the restored context reads chronologically."""
    db = PostgresDB.__new__(PostgresDB)
    # The DB returns rows newest-first under ORDER BY seq DESC.
    db.fetch = AsyncMock(
        return_value=[_full_row("n3", 3), _full_row("n2", 2), _full_row("n1", 1)]
    )
    rows = await db.get_thread_messages_history("t1", limit=1000, newest_first=True)
    sql = " ".join(db.fetch.call_args[0][0].split())
    assert "ORDER BY seq DESC" in sql, "newest_first must select by seq DESC"
    assert "LIMIT" in sql, "the floor must cap the row count"
    # Identify rows by a projected field (id is no longer returned): _full_row
    # sets content=f"c{mid}", so chronological order is c-n1, c-n2, c-n3.
    assert [r["content"] for r in rows] == ["cn1", "cn2", "cn3"], (
        "result must be chronological"
    )


@pytest.mark.asyncio
async def test_newest_first_composes_with_seq_cursor():
    """The floor and the boundary_seq cursor combine: newest N of seq > S."""
    db = PostgresDB.__new__(PostgresDB)
    db.fetch = AsyncMock(return_value=[])
    await db.get_thread_messages_history("t1", limit=500, seq_gt=42, newest_first=True)
    sql = " ".join(db.fetch.call_args[0][0].split())
    assert "seq >" in sql and "ORDER BY seq DESC" in sql
    args = db.fetch.call_args[0]
    assert 42 in args[1:] and 500 in args[1:]
