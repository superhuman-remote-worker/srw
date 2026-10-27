-- P4 arm A, step 1 of 2 (audit database): which memories were injected, when,
-- and where each conversation stood. Step 2 (vector database) resolves the
-- memories and decides whether their source was still in context. The test:
-- knowledge-base/knowledge/tests/aoci_same_conversation_memory_reinjection_test.md
--
--   psql "$AUDIT_DB_URL" -X -q -v from='2026-10-20' -v to='2026-10-21' \
--     [-v job_ids='uuid-1,uuid-2'] [-v out=p4_step1.json] \
--     -f bench/queries/p4_same_conversation_step1_audit.sql
--
-- Read-only (one READ ONLY transaction, SELECT only), so it runs on a replica.
-- The dev audit replica has a 4 GiB limit and was OOM-killed by overlapping
-- scans of llm_requests.request bodies. This script reads each request body in
-- the window exactly once, row by row, in one statement; nothing sorts or
-- de-duplicates bodies. Keep the window to a day or a few, and never run it
-- next to another body scan.
--
-- Writes one JSON document to :out (default p4_step1.json):
--   {"window": {...},
--    "jobs": [{job_id, lane, prior_requests, requests, injected,
--              audit_rows, audit_appended, audit_present, audit_arm_b_skipped}],
--    "injections": [{job_id, lane, request_id, request_ts, turn,
--                    compacted_through, position_exact, handle}]}
--
-- How it reads the archived main requests (llm_requests, call_type 'main',
-- the folded request each provider got):
--
-- * A memory entry is the <srw_context kind="memory"> block the planner
--   appends; each memory in it is labelled with its display handle [m:xxxxxx]
--   (first 6 hex of sha256 of the memory id text, context_entries.memory_handle).
--   memory_search results carry handles too, outside such a block, and are not
--   counted: they are pulled, not injected.
-- * Entries stay in the history, folded into the message before them. The
--   entries a request appended come after its last assistant message (the
--   previous response), folded into the tail carrier or standing alone. So a
--   memory is injected at a request when its handle is in a memory entry
--   after the request's last assistant message: a first push, or a push again
--   after a compaction evicted the earlier one.
-- * turn / compacted_through mirror where the take-in places the conversation
--   (agent/services/memory/same_conversation.py):
--   - worker: turn = main requests before this one (each answered main
--     request is one turn_count); compacted_through = turn minus the
--     assistant messages in the request (legacy *_inject_ pairs excluded).
--   - session (agent_type 'persistent'): turn = iteration (the session turn);
--     compacted_through = the turn of the first request that carried the
--     current compaction summary, minus one (the checkpoint's boundary_turn);
--     0 while no summary is in the history.
--   position_exact is false for a session whose summary first shows on its
--   first in-window request although it started earlier: that compaction may
--   have happened before the window, so its boundary may be too late.
-- * The jobs list joins the memory_inject audit rows (workers only) as a
--   cross-check: audit_appended should match injected. Under arm B
--   audit_arm_b_skipped counts what the take-in skipped.

\set ON_ERROR_STOP on
\if :{?from}
\else
\warn 'Pass -v from=<timestamp> -v to=<timestamp> (a window of a day or a few)'
\quit
\endif
\if :{?to}
\else
\warn 'Pass -v to=<timestamp>'
\quit
\endif
\if :{?job_ids}
\else
\set job_ids ''
\endif
\if :{?out}
\else
\set out p4_step1.json
\endif

BEGIN READ ONLY;

\pset tuples_only on
\pset format unaligned
\o :out

WITH window_requests AS MATERIALIZED (
    -- The one pass over the bodies: per request, its assistant messages and
    -- the position of the last one, the digest of its compaction summary,
    -- and the memory handles of each message holding a memory entry.
    SELECT
        r.id,
        r.job_id,
        r."timestamp" AS ts,
        r.iteration,
        CASE WHEN r.agent_type = 'persistent' THEN 'session' ELSE 'worker' END
            AS lane,
        body.responses,
        body.last_response,
        body.summary_digest,
        body.entry_handles
    FROM llm_requests AS r
    CROSS JOIN LATERAL (
        SELECT
            count(*) FILTER (WHERE msg.is_response) AS responses,
            max(msg.idx) FILTER (WHERE msg.is_response) AS last_response,
            max(md5(split_part(msg.value->>'content', E'\n\n<srw_context ', 1)))
                FILTER (
                    WHERE msg.value->'additional_kwargs'->>'srw_compaction_summary'
                          = 'true'
                       OR (msg.value->>'role' = 'system'
                           AND msg.value->>'content' LIKE '[Summary of prior work]%')
                ) AS summary_digest,
            jsonb_agg(jsonb_build_object('i', msg.idx, 'h', entry.handles))
                FILTER (WHERE entry.handles <> '') AS entry_handles
        FROM (
            SELECT
                e.value,
                e.idx,
                e.value->>'role' = 'assistant'
                    AND coalesce(e.value->'tool_calls'->0->>'id', '')
                        NOT LIKE '%\_inject\_%' AS is_response
            FROM jsonb_array_elements(r.request->'messages')
                WITH ORDINALITY AS e(value, idx)
        ) AS msg
        CROSS JOIN LATERAL (
            SELECT coalesce(string_agg(DISTINCT handle[1], ','), '') AS handles
            FROM regexp_matches(
                    msg.value->>'content',
                    '<srw_context kind="memory">(.*?)</srw_context>',
                    'g'
                 ) AS block,
                 regexp_matches(block[1], '(?:^|\n)\[(m:[0-9a-f]{6})\]', 'g')
                    AS handle
        ) AS entry
    ) AS body
    WHERE r.call_type = 'main'
      AND r."timestamp" >= :'from'::timestamptz
      AND r."timestamp" < :'to'::timestamptz
      AND (:'job_ids' = ''
           OR r.job_id = ANY (string_to_array(:'job_ids', ',')::uuid[]))
),
prior AS (
    -- Main requests before the window: index on (job_id, timestamp), no body.
    SELECT r.job_id, count(*) AS n
    FROM llm_requests AS r
    WHERE r.call_type = 'main'
      AND r."timestamp" < :'from'::timestamptz
      AND r.job_id IN (SELECT DISTINCT job_id FROM window_requests)
    GROUP BY r.job_id
),
ordered AS (
    SELECT
        w.*,
        coalesce(p.n, 0) AS prior_requests,
        row_number() OVER (PARTITION BY w.job_id ORDER BY w.ts, w.id) AS in_window
    FROM window_requests AS w
    LEFT JOIN prior AS p USING (job_id)
),
summary_start AS (
    -- First in-window request carrying each summary of a conversation.
    SELECT job_id, summary_digest, min(in_window) AS first_in_window,
           min(iteration) AS first_turn
    FROM ordered
    WHERE summary_digest IS NOT NULL
    GROUP BY job_id, summary_digest
),
injected AS (
    SELECT
        o.id,
        o.job_id,
        o.ts,
        o.lane,
        o.prior_requests,
        CASE o.lane
            WHEN 'worker' THEN o.prior_requests + o.in_window - 1
            ELSE o.iteration
        END AS turn,
        CASE o.lane
            WHEN 'worker' THEN greatest(
                o.prior_requests + o.in_window - 1 - o.responses, 0)
            ELSE greatest(coalesce(s.first_turn - 1, 0), 0)
        END AS compacted_through,
        NOT coalesce(
            o.lane = 'session' AND s.first_in_window = 1 AND o.prior_requests > 0,
            false
        ) AS position_exact,
        ARRAY(
            SELECT DISTINCT h
            FROM jsonb_array_elements(coalesce(o.entry_handles, '[]')) AS e,
                 unnest(string_to_array(e->>'h', ',')) AS h
            WHERE (e->>'i')::bigint > coalesce(o.last_response, 0)
            ORDER BY h
        ) AS new_handles
    FROM ordered AS o
    LEFT JOIN summary_start AS s USING (job_id, summary_digest)
),
audit AS (
    SELECT
        a.job_id,
        count(*) AS audit_rows,
        sum((a.payload->>'appended')::int) AS audit_appended,
        sum((a.payload->>'present')::int) AS audit_present,
        sum((a.payload->'stats'->'extra'->'same_conversation'->>'skipped')::int)
            AS audit_arm_b_skipped
    FROM agent_audit AS a
    WHERE a.step_type = 'memory_inject'
      AND a.event_phase = 'pre'
      AND a."timestamp" >= :'from'::timestamptz
      AND a."timestamp" < :'to'::timestamptz
      AND a.job_id IN (SELECT DISTINCT job_id FROM window_requests)
    GROUP BY a.job_id
),
jobs AS (
    SELECT
        i.job_id,
        min(i.lane) AS lane,
        min(i.prior_requests) AS prior_requests,
        count(*) AS requests,
        sum(cardinality(i.new_handles)) AS injected
    FROM injected AS i
    GROUP BY i.job_id
)
SELECT jsonb_build_object(
    'window', jsonb_build_object(
        'from', :'from'::timestamptz,
        'to', :'to'::timestamptz,
        'job_ids', nullif(:'job_ids', '')
    ),
    'jobs', coalesce((
        SELECT jsonb_agg(
            to_jsonb(j) || jsonb_build_object(
                'audit_rows', coalesce(a.audit_rows, 0),
                'audit_appended', a.audit_appended,
                'audit_present', a.audit_present,
                'audit_arm_b_skipped', a.audit_arm_b_skipped
            ) ORDER BY j.job_id)
        FROM jobs AS j
        LEFT JOIN audit AS a USING (job_id)
    ), '[]'::jsonb),
    'injections', coalesce((
        SELECT jsonb_agg(jsonb_build_object(
            'job_id', i.job_id,
            'lane', i.lane,
            'request_id', i.id,
            'request_ts', i.ts,
            'turn', i.turn,
            'compacted_through', i.compacted_through,
            'position_exact', i.position_exact,
            'handle', h
        ) ORDER BY i.job_id, i.ts, i.id, h)
        FROM injected AS i, unnest(i.new_handles) AS h
    ), '[]'::jsonb)
);

\o
\pset format aligned
\pset tuples_only off
COMMIT;
\warn 'Wrote' :out
