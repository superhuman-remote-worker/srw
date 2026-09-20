"""Framework-free SQL predicates shared by rewind preview and application."""

from __future__ import annotations


# A target must be an ordinary user prompt. Durable server deliveries carry a
# separate ledger row; old image carriers have an anchored legacy marker.
LIVE_USER_TARGET_SQL = """
SELECT message.id, message.seq, message.content, message.turn_number
FROM thread_messages AS message
WHERE message.thread_id = $1::uuid
  AND message.id = $2::uuid
  AND message.rewound_at IS NULL
  AND message.role = 'human'
  AND NOT (
      message.tool_calls IS NULL
      AND message.tool_call_id IS NULL
      AND message.content ~ '^Image content from tool call [^[:space:]]+:[[:space:]]*$'
  )
  AND NOT EXISTS (
      SELECT 1
      FROM thread_input_deliveries AS delivery
      WHERE delivery.thread_id = message.thread_id
        AND delivery.message_id = message.id
  )
"""


SESSION_MEMORY_REWIND_GUARD_SQL = """
SELECT effect.producer_id
FROM completion_effects AS effect
JOIN thread_messages AS boundary
  ON boundary.thread_id = effect.scope_id
 AND boundary.turn_execution_id = effect.producer_id
WHERE effect.producer_kind = 'session_turn'
  AND effect.effect_name = 'final_memory_extraction'
  AND effect.scope_id = $1::uuid
  AND effect.state = 'pending'
  AND boundary.seq >= $2::bigint
ORDER BY boundary.seq, effect.producer_id
"""


# Kept in one place so preview and the locked mutation cannot drift.
LIVE_SESSION_CHILD_EXISTS_SQL = """
SELECT EXISTS (
    SELECT 1
    FROM threads AS child
    WHERE child.kind = 'subagent'
      AND child.parent_job_id IS NULL
      AND child.parent_thread_id = $1::uuid
      AND (
          (child.status IN ('created', 'active')
           AND child.subagent_status IN ('queued', 'running'))
          OR (
              child.status = 'ended'
              AND child.subagent_outcome IS DISTINCT FROM 'cancelled:parent_retired'
              AND child.parent_tool_call_id IS NOT NULL
              AND COALESCE(
                  child.metadata->'subagent'->>'run_in_background', 'false'
              ) = 'false'
              AND (child.metadata->>'subagent_foreground_recovery_generation')
                    IS DISTINCT FROM child.runtime_generation::text
              AND NOT EXISTS (
                  SELECT 1 FROM thread_messages AS parent_result
                  WHERE parent_result.thread_id = $1::uuid
                    AND parent_result.role = 'tool'
                    AND parent_result.tool_call_id = child.parent_tool_call_id
                    AND parent_result.rewound_at IS NULL
              )
              AND EXISTS (
                  SELECT 1
                  FROM thread_messages AS parent_call
                  CROSS JOIN LATERAL jsonb_array_elements(
                      CASE WHEN jsonb_typeof(parent_call.tool_calls) = 'array'
                           THEN parent_call.tool_calls ELSE '[]'::jsonb END
                  ) AS tool_call
                  WHERE parent_call.thread_id = $1::uuid
                    AND parent_call.role = 'ai'
                    AND parent_call.rewound_at IS NULL
                    AND tool_call->>'id' = child.parent_tool_call_id
              )
          )
      )
)
"""


__all__ = [
    "LIVE_SESSION_CHILD_EXISTS_SQL",
    "LIVE_USER_TARGET_SQL",
    "SESSION_MEMORY_REWIND_GUARD_SQL",
]
