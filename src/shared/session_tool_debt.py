"""Unanswered tool calls in a Session transcript ("tool debt").

An assistant row whose tool_calls carry an id with no later role='tool'
row of the same turn is a call whose outcome the transcript cannot prove.
The stateless executor refuses to start a turn over it (it parks the unit);
the owner's Continue / Stop decision answers each such call and settles the
interrupted turn (knowledge-base/knowledge/issues/
session_unknown_tool_result_needs_admin.md). Both read the rows defined here.
"""

from __future__ import annotations

from typing import Any

# Consumption is an input watermark, not proof that an external command
# finished. A successor must inspect all live debt before restore can repair
# an unanswered call out of model context. Delegation-only batches retain
# their existing child-ledger recovery; mixed batches remain ambiguous.
SESSION_TOOL_DEBT_SQL = """
SELECT EXISTS (
    SELECT 1 FROM thread_messages AS malformed_row
     WHERE malformed_row.thread_id = $1::uuid
       AND malformed_row.role = 'ai'
       AND malformed_row.rewound_at IS NULL
       AND malformed_row.tool_calls IS NOT NULL
       AND jsonb_typeof(malformed_row.tool_calls) IS DISTINCT FROM 'array'
) OR EXISTS (
    -- A result identifies a call by turn/id, not an occurrence. Duplicate
    -- same-turn IDs cannot prove which physical effect that result settled.
    SELECT 1
      FROM thread_messages AS duplicate_row
     CROSS JOIN LATERAL jsonb_array_elements(
         CASE WHEN jsonb_typeof(duplicate_row.tool_calls) = 'array'
              THEN duplicate_row.tool_calls ELSE '[]'::jsonb END
     ) AS duplicate_call
     WHERE duplicate_row.thread_id = $1::uuid
       AND duplicate_row.role = 'ai'
       AND duplicate_row.rewound_at IS NULL
     GROUP BY duplicate_row.turn_number, duplicate_call ->> 'id'
    HAVING COUNT(*) > 1 AND BOOL_OR(EXISTS (
        SELECT 1 FROM jsonb_array_elements(
            CASE WHEN jsonb_typeof(duplicate_row.tool_calls) = 'array'
                 THEN duplicate_row.tool_calls ELSE '[]'::jsonb END
        ) AS sibling
        WHERE sibling ->> 'name' IS DISTINCT FROM 'delegate_agent'
    ))
) OR EXISTS (
    SELECT 1
      FROM thread_messages AS call_row
     CROSS JOIN LATERAL jsonb_array_elements(
         CASE WHEN jsonb_typeof(call_row.tool_calls) = 'array'
              THEN call_row.tool_calls ELSE '[]'::jsonb END
     ) AS tool_call
     WHERE call_row.thread_id = $1::uuid
       AND call_row.role = 'ai'
       AND call_row.rewound_at IS NULL
       AND (
           jsonb_typeof(tool_call -> 'id') IS DISTINCT FROM 'string'
           OR BTRIM(COALESCE(tool_call ->> 'id', '')) = ''
           OR NOT EXISTS (
               SELECT 1 FROM thread_messages AS result_row
                WHERE result_row.thread_id = call_row.thread_id
                  AND result_row.role = 'tool'
                  AND result_row.rewound_at IS NULL
                  AND result_row.seq > call_row.seq
                  AND result_row.turn_number IS NOT DISTINCT FROM call_row.turn_number
                  AND result_row.tool_call_id = tool_call ->> 'id'
           )
       )
       AND EXISTS (
           SELECT 1 FROM jsonb_array_elements(
               CASE WHEN jsonb_typeof(call_row.tool_calls) = 'array'
                    THEN call_row.tool_calls ELSE '[]'::jsonb END
           ) AS sibling
            WHERE sibling ->> 'name' IS DISTINCT FROM 'delegate_agent'
               OR jsonb_typeof(sibling -> 'id') IS DISTINCT FROM 'string'
               OR BTRIM(COALESCE(sibling ->> 'id', '')) = ''
       )
)
"""

# Every call that keeps SESSION_TOOL_DEBT_SQL's last branch true, one row each:
# a well-formed id with no result row in its turn, in a batch that is not
# delegation-only. Malformed or duplicate ids are not listed; they stay debt
# after every listed call is answered, which callers treat as ambiguous.
UNANSWERED_TOOL_CALLS_SQL = """
SELECT call_row.seq AS call_seq,
       call_row.turn_number,
       tool_call ->> 'id' AS call_id,
       tool_call ->> 'name' AS name,
       (tool_call -> 'args')::text AS args
  FROM thread_messages AS call_row
 CROSS JOIN LATERAL jsonb_array_elements(
     CASE WHEN jsonb_typeof(call_row.tool_calls) = 'array'
          THEN call_row.tool_calls ELSE '[]'::jsonb END
 ) WITH ORDINALITY AS calls(tool_call, position)
 WHERE call_row.thread_id = $1::uuid
   AND call_row.role = 'ai'
   AND call_row.rewound_at IS NULL
   AND jsonb_typeof(tool_call -> 'id') = 'string'
   AND BTRIM(tool_call ->> 'id') <> ''
   AND NOT EXISTS (
       SELECT 1 FROM thread_messages AS result_row
        WHERE result_row.thread_id = call_row.thread_id
          AND result_row.role = 'tool'
          AND result_row.rewound_at IS NULL
          AND result_row.seq > call_row.seq
          AND result_row.turn_number IS NOT DISTINCT FROM call_row.turn_number
          AND result_row.tool_call_id = tool_call ->> 'id'
   )
   AND EXISTS (
       SELECT 1 FROM jsonb_array_elements(
           CASE WHEN jsonb_typeof(call_row.tool_calls) = 'array'
                THEN call_row.tool_calls ELSE '[]'::jsonb END
       ) AS sibling
        WHERE sibling ->> 'name' IS DISTINCT FROM 'delegate_agent'
           OR jsonb_typeof(sibling -> 'id') IS DISTINCT FROM 'string'
           OR BTRIM(COALESCE(sibling ->> 'id', '')) = ''
   )
 ORDER BY call_row.seq, calls.position
"""


async def has_tool_debt(conn: Any, thread_id: str) -> bool:
    """Whether the thread's live transcript holds any tool debt."""
    value = await conn.fetchval("SELECT (" + SESSION_TOOL_DEBT_SQL + ")", thread_id)
    if type(value) is not bool:
        raise RuntimeError("Session tool-debt classification is unproven")
    return value


async def unanswered_tool_calls(conn: Any, thread_id: str) -> list[dict[str, Any]]:
    """The listable unanswered calls, oldest first."""
    rows = await conn.fetch(UNANSWERED_TOOL_CALLS_SQL, thread_id)
    return [dict(row) for row in rows]


__all__ = [
    "SESSION_TOOL_DEBT_SQL",
    "UNANSWERED_TOOL_CALLS_SQL",
    "has_tool_debt",
    "unanswered_tool_calls",
]
