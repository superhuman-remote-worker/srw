-- P4 arm A, step 2 of 2 (vector database): of the memories step 1 saw
-- injected, how many came from the same conversation while their source was
-- still in context -- what arm B (memory.retrieval.same_conversation:
-- skip_in_context) would have skipped. The test:
-- knowledge-base/knowledge/tests/aoci_same_conversation_memory_reinjection_test.md
--
--   psql "$VECTOR_DB_URL" -X -q -v step1=p4_step1.json \
--     -f bench/queries/p4_same_conversation_step2_vector.sql
--
-- Read-only (SELECT only, no temp table), so it runs on a replica. Step 1's
-- document is read with a psql backquote (`cat`) and passed as one literal.
-- It touches only the memories rows of the conversations step 1 found, and
-- never their embeddings.
--
-- A handle is resolved within its own conversation: the newest memory of the
-- same job_id (sessions store the thread id there) created by the request
-- whose handle (first 6 hex of sha256 of the id text) matches. No match means
-- the memory came from another job or session of the project (or the row is
-- gone): not same-conversation. Superseded rows count, the injection happened.
--
-- "Source in context" is the take-in's rule
-- (agent/services/memory/same_conversation.py source_in_context):
--   end := source_turn_end, else source_turn_start
--   no turn stamp      -> in context until the first compaction
--                         (compacted_through = 0)
--   end > turn         -> an earlier run of the conversation: not in context
--   otherwise          -> in context when end > compacted_through
--
-- Result 1: per lane, injected memories, same-conversation ones, and how many
-- of those had their source in context (all positions, then exact ones only).
-- Result 2: the same per conversation, with step 1's memory_inject audit
-- cross-check (workers only).

\set ON_ERROR_STOP on
\if :{?step1}
\else
\warn 'Pass -v step1=<path of the JSON step 1 wrote>'
\quit
\endif
\set p4 `cat :'step1'`

BEGIN READ ONLY;

WITH injections AS MATERIALIZED (
    SELECT i.*
    FROM jsonb_to_recordset((:'p4'::jsonb)->'injections') AS i(
        job_id uuid,
        lane text,
        request_id bigint,
        request_ts timestamptz,
        turn int,
        compacted_through int,
        position_exact boolean,
        handle text
    )
),
conversation_memories AS MATERIALIZED (
    SELECT
        m.id,
        m.job_id,
        m.created_at,
        m.source,
        coalesce(m.source_turn_end, m.source_turn_start) AS source_turn,
        'm:' || left(encode(sha256(convert_to(m.id::text, 'UTF8')), 'hex'), 6)
            AS handle
    FROM memories AS m
    WHERE m.job_id IN (SELECT DISTINCT job_id FROM injections)
),
resolved AS (
    SELECT
        i.*,
        m.id AS memory_id,
        m.source,
        m.source_turn,
        CASE
            WHEN m.id IS NULL THEN NULL
            WHEN m.source_turn IS NULL THEN i.compacted_through <= 0
            WHEN m.source_turn > i.turn THEN false
            ELSE m.source_turn > i.compacted_through
        END AS source_in_context
    FROM injections AS i
    LEFT JOIN LATERAL (
        SELECT cm.*
        FROM conversation_memories AS cm
        WHERE cm.job_id = i.job_id
          AND cm.handle = i.handle
          AND cm.created_at <= i.request_ts
        ORDER BY cm.created_at DESC
        LIMIT 1
    ) AS m ON true
)
SELECT
    CASE WHEN GROUPING(lane) = 1 THEN 'ALL' ELSE lane END AS lane,
    count(*) AS injected,
    count(memory_id) AS same_conversation,
    count(*) FILTER (WHERE source_in_context) AS source_in_context,
    count(*) FILTER (WHERE source_in_context AND source_turn IS NULL)
        AS of_which_unstamped,
    round(100.0 * count(*) FILTER (WHERE source_in_context)
          / nullif(count(*), 0), 1) AS pct_of_injected,
    count(*) FILTER (WHERE position_exact) AS injected_exact,
    count(*) FILTER (WHERE position_exact AND source_in_context)
        AS source_in_context_exact,
    round(100.0 * count(*) FILTER (WHERE position_exact AND source_in_context)
          / nullif(count(*) FILTER (WHERE position_exact), 0), 1)
        AS pct_of_injected_exact
FROM resolved
GROUP BY ROLLUP (lane)
ORDER BY GROUPING(lane), lane;

WITH injections AS MATERIALIZED (
    SELECT i.*
    FROM jsonb_to_recordset((:'p4'::jsonb)->'injections') AS i(
        job_id uuid,
        lane text,
        request_ts timestamptz,
        turn int,
        compacted_through int,
        handle text
    )
),
conversation_memories AS MATERIALIZED (
    SELECT
        m.id,
        m.job_id,
        m.created_at,
        coalesce(m.source_turn_end, m.source_turn_start) AS source_turn,
        'm:' || left(encode(sha256(convert_to(m.id::text, 'UTF8')), 'hex'), 6)
            AS handle
    FROM memories AS m
    WHERE m.job_id IN (SELECT DISTINCT job_id FROM injections)
),
resolved AS (
    SELECT
        i.job_id,
        m.id AS memory_id,
        CASE
            WHEN m.id IS NULL THEN NULL
            WHEN m.source_turn IS NULL THEN i.compacted_through <= 0
            WHEN m.source_turn > i.turn THEN false
            ELSE m.source_turn > i.compacted_through
        END AS source_in_context,
        i.compacted_through > 0 AS after_compaction
    FROM injections AS i
    LEFT JOIN LATERAL (
        SELECT cm.*
        FROM conversation_memories AS cm
        WHERE cm.job_id = i.job_id
          AND cm.handle = i.handle
          AND cm.created_at <= i.request_ts
        ORDER BY cm.created_at DESC
        LIMIT 1
    ) AS m ON true
),
per_job AS (
    SELECT
        job_id,
        count(*) AS injected,
        count(memory_id) AS same_conversation,
        count(*) FILTER (WHERE source_in_context) AS source_in_context,
        count(*) FILTER (WHERE memory_id IS NOT NULL AND NOT source_in_context)
            AS same_conversation_source_evicted,
        bool_or(after_compaction) AS compacted
    FROM resolved
    GROUP BY job_id
)
SELECT
    j.job_id,
    j.lane,
    coalesce(p.injected, 0) AS injected,
    coalesce(p.same_conversation, 0) AS same_conversation,
    coalesce(p.source_in_context, 0) AS source_in_context,
    coalesce(p.same_conversation_source_evicted, 0) AS evicted_source,
    coalesce(p.compacted, false) AS compacted,
    j.requests,
    j.audit_rows,
    j.audit_appended,
    j.audit_arm_b_skipped
FROM jsonb_to_recordset((:'p4'::jsonb)->'jobs') AS j(
    job_id uuid,
    lane text,
    requests int,
    injected int,
    audit_rows int,
    audit_appended int,
    audit_arm_b_skipped int
)
LEFT JOIN per_job AS p USING (job_id)
ORDER BY coalesce(p.source_in_context, 0) DESC, j.job_id;

COMMIT;
