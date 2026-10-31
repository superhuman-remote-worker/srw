-- migration:     0027_memory_search_includes_pinned_rows.sql
-- description:   The three memory search functions stop skipping rows with
--                remaining_turns > 0. Those rows were the TTL "pinned" tier:
--                every new memory was pinned for 10 turns, and only the pinned
--                tier (RecallStore.get_ttl_active) could return it until a
--                per-turn decrement brought it to 0. The append-only memory
--                design retires that tier (vault append_only_context_injection,
--                D12/D36), so a pinned row must be found by search like any other
--                row. Without this, memory_search never returns a fresh memory,
--                and a row pinned in a project that stopped retrieving stays
--                hidden for good (30% of dev's live memories on 2026-10-18).
--
--                SHAPE OF THE CHANGE. The bodies are 0017's text with the
--                `remaining_turns` predicates deleted, nothing else: signatures,
--                defaults, return types, SET clauses and the dynamic EXECUTE are
--                unchanged, so callers are untouched. The remaining_turns column
--                and its partial indexes stay; nothing reads them any more.
--
--                REVERTING. Re-run the three memory functions from 0017.
-- depends-on:    0026_job_vector_retirement.sql
-- expected:      Instant. CREATE OR REPLACE FUNCTION only; no table is read or
--                rewritten.
-- locks:         brief lock on the three pg_proc catalog rows.
-- transactional: YES.

CREATE OR REPLACE FUNCTION public.memory_hybrid_search(query_text text, query_embedding vector, job_id_param uuid, match_count integer DEFAULT 10, dense_weight double precision DEFAULT 0.6, sparse_weight double precision DEFAULT 0.3, recency_weight double precision DEFAULT 0.1, rrf_k integer DEFAULT 50, importance_floor double precision DEFAULT 0.0)
 RETURNS SETOF memories
 LANGUAGE plpgsql
 SET "hnsw.iterative_scan" TO 'relaxed_order'
AS $function$
BEGIN
RETURN QUERY EXECUTE $q$
WITH dense AS (
    SELECT id, ROW_NUMBER() OVER (ORDER BY best_dist) AS rank_ix
    FROM (
        SELECT id, MIN(dist) AS best_dist
        FROM (
            -- Content embedding matches
            (SELECT id, subvector(embedding, 1, 4000)::halfvec(4000) <=> subvector($2, 1, 4000)::halfvec(4000) AS dist
            FROM memories WHERE job_id = $3 AND embedding IS NOT NULL AND importance >= $9
                AND valid_to IS NULL
            ORDER BY dist LIMIT $4 * 3)

            UNION ALL

            -- Trigger phrase embedding matches → parent memory_id
            (SELECT rm.memory_id AS id, subvector(rm.embedding, 1, 4000)::halfvec(4000) <=> subvector($2, 1, 4000)::halfvec(4000) AS dist
            FROM memory_retrieval_messages rm
            INNER JOIN memories m ON rm.memory_id = m.id
            WHERE m.job_id = $3 AND rm.embedding IS NOT NULL AND m.importance >= $9
                AND m.valid_to IS NULL
            ORDER BY dist LIMIT $4 * 5)
        ) all_matches
        GROUP BY id
    ) best_per_memory
    LIMIT $4 * 2
),
sparse AS (
    SELECT id, ROW_NUMBER() OVER (ORDER BY ts_rank_cd(sparse_keywords, websearch_to_tsquery('english', $1)) DESC) AS rank_ix
    FROM memories WHERE job_id = $3 AND sparse_keywords @@ websearch_to_tsquery('english', $1) AND importance >= $9
        AND valid_to IS NULL
    ORDER BY rank_ix LIMIT $4 * 2
),
recent AS (
    SELECT id, ROW_NUMBER() OVER (ORDER BY created_at DESC) AS rank_ix
    FROM memories WHERE job_id = $3 AND importance >= $9
        AND valid_to IS NULL
    ORDER BY rank_ix LIMIT $4
)
SELECT memories.* FROM (
    SELECT COALESCE(d.id, s.id, r.id) AS mid,
        COALESCE(1.0 / ($8 + d.rank_ix), 0.0) * $5 +
        COALESCE(1.0 / ($8 + s.rank_ix), 0.0) * $6 +
        COALESCE(1.0 / ($8 + r.rank_ix), 0.0) * $7 AS rrf_score
    FROM dense d
    FULL OUTER JOIN sparse s ON d.id = s.id
    FULL OUTER JOIN recent r ON COALESCE(d.id, s.id) = r.id
) ranked
JOIN memories ON ranked.mid = memories.id
ORDER BY ranked.rrf_score DESC
LIMIT $4
$q$ USING query_text, query_embedding, job_id_param, match_count, dense_weight, sparse_weight, recency_weight, rrf_k, importance_floor;
END;
$function$;


CREATE OR REPLACE FUNCTION public.memory_project_hybrid_search(query_text text, query_embedding vector, project_id_param uuid, match_count integer DEFAULT 10, dense_weight double precision DEFAULT 0.6, sparse_weight double precision DEFAULT 0.3, recency_weight double precision DEFAULT 0.1, rrf_k integer DEFAULT 50, importance_floor double precision DEFAULT 0.0)
 RETURNS SETOF memories
 LANGUAGE plpgsql
 SET "hnsw.iterative_scan" TO 'relaxed_order'
AS $function$
BEGIN
RETURN QUERY EXECUTE $q$
WITH dense AS (
    SELECT id, ROW_NUMBER() OVER (ORDER BY best_dist) AS rank_ix
    FROM (
        SELECT id, MIN(dist) AS best_dist
        FROM (
            -- Content embedding matches
            (SELECT id, subvector(embedding, 1, 4000)::halfvec(4000) <=> subvector($2, 1, 4000)::halfvec(4000) AS dist
            FROM memories WHERE project_id = $3 AND embedding IS NOT NULL AND importance >= $9
                AND valid_to IS NULL
            ORDER BY dist LIMIT $4 * 3)

            UNION ALL

            -- Trigger phrase embedding matches → parent memory_id
            (SELECT rm.memory_id AS id, subvector(rm.embedding, 1, 4000)::halfvec(4000) <=> subvector($2, 1, 4000)::halfvec(4000) AS dist
            FROM memory_retrieval_messages rm
            INNER JOIN memories m ON rm.memory_id = m.id
            WHERE m.project_id = $3 AND rm.embedding IS NOT NULL AND m.importance >= $9
                AND m.valid_to IS NULL
            ORDER BY dist LIMIT $4 * 5)
        ) all_matches
        GROUP BY id
    ) best_per_memory
    LIMIT $4 * 2
),
sparse AS (
    SELECT id, ROW_NUMBER() OVER (ORDER BY ts_rank_cd(sparse_keywords, websearch_to_tsquery('english', $1)) DESC) AS rank_ix
    FROM memories WHERE project_id = $3 AND sparse_keywords @@ websearch_to_tsquery('english', $1) AND importance >= $9
        AND valid_to IS NULL
    ORDER BY rank_ix LIMIT $4 * 2
),
recent AS (
    SELECT id, ROW_NUMBER() OVER (ORDER BY created_at DESC) AS rank_ix
    FROM memories WHERE project_id = $3 AND importance >= $9
        AND valid_to IS NULL
    ORDER BY rank_ix LIMIT $4
)
SELECT memories.* FROM (
    SELECT COALESCE(d.id, s.id, r.id) AS mid,
        COALESCE(1.0 / ($8 + d.rank_ix), 0.0) * $5 +
        COALESCE(1.0 / ($8 + s.rank_ix), 0.0) * $6 +
        COALESCE(1.0 / ($8 + r.rank_ix), 0.0) * $7 AS rrf_score
    FROM dense d
    FULL OUTER JOIN sparse s ON d.id = s.id
    FULL OUTER JOIN recent r ON COALESCE(d.id, s.id) = r.id
) ranked
                           JOIN memories ON ranked.mid = memories.id
ORDER BY ranked.rrf_score DESC LIMIT $4
$q$ USING query_text, query_embedding, project_id_param, match_count, dense_weight, sparse_weight, recency_weight, rrf_k, importance_floor;
END;
$function$;


CREATE OR REPLACE FUNCTION public.memory_multi_project_hybrid_search(query_text text, query_embedding vector, project_ids_param uuid[], match_count integer DEFAULT 10, dense_weight double precision DEFAULT 0.6, sparse_weight double precision DEFAULT 0.3, recency_weight double precision DEFAULT 0.1, rrf_k integer DEFAULT 50, importance_floor double precision DEFAULT 0.0)
 RETURNS SETOF memories
 LANGUAGE plpgsql
 SET "hnsw.iterative_scan" TO 'relaxed_order'
AS $function$
BEGIN
RETURN QUERY EXECUTE $q$
WITH dense AS (
    SELECT id, ROW_NUMBER() OVER (ORDER BY best_dist) AS rank_ix
    FROM (
        SELECT id, MIN(dist) AS best_dist
        FROM (
            -- Content embedding matches
            (SELECT id, subvector(embedding, 1, 4000)::halfvec(4000) <=> subvector($2, 1, 4000)::halfvec(4000) AS dist
            FROM memories WHERE project_id = ANY($3) AND embedding IS NOT NULL AND importance >= $9
                AND valid_to IS NULL
            ORDER BY dist LIMIT $4 * 3)

            UNION ALL

            -- Trigger phrase embedding matches → parent memory_id
            (SELECT rm.memory_id AS id, subvector(rm.embedding, 1, 4000)::halfvec(4000) <=> subvector($2, 1, 4000)::halfvec(4000) AS dist
            FROM memory_retrieval_messages rm
            INNER JOIN memories m ON rm.memory_id = m.id
            WHERE m.project_id = ANY($3) AND rm.embedding IS NOT NULL AND m.importance >= $9
                AND m.valid_to IS NULL
            ORDER BY dist LIMIT $4 * 5)
        ) all_matches
        GROUP BY id
    ) best_per_memory
    LIMIT $4 * 2
),
sparse AS (
    SELECT id, ROW_NUMBER() OVER (ORDER BY ts_rank_cd(sparse_keywords, websearch_to_tsquery('english', $1)) DESC) AS rank_ix
    FROM memories WHERE project_id = ANY($3) AND sparse_keywords @@ websearch_to_tsquery('english', $1) AND importance >= $9
        AND valid_to IS NULL
    ORDER BY rank_ix LIMIT $4 * 2
),
recent AS (
    SELECT id, ROW_NUMBER() OVER (ORDER BY created_at DESC) AS rank_ix
    FROM memories WHERE project_id = ANY($3) AND importance >= $9
        AND valid_to IS NULL
    ORDER BY rank_ix LIMIT $4
)
SELECT memories.*
FROM (SELECT COALESCE(d.id, s.id, r.id)                                AS mid,
             COALESCE(1.0 / ($8 + d.rank_ix), 0.0) * $5 +
             COALESCE(1.0 / ($8 + s.rank_ix), 0.0) * $6 +
             COALESCE(1.0 / ($8 + r.rank_ix), 0.0) * $7 AS rrf_score
      FROM dense d
               FULL OUTER JOIN sparse s ON d.id = s.id
               FULL OUTER JOIN recent r ON COALESCE(d.id, s.id) = r.id) ranked
JOIN memories ON ranked.mid = memories.id
ORDER BY ranked.rrf_score DESC
LIMIT $4
$q$ USING query_text, query_embedding, project_ids_param, match_count, dense_weight, sparse_weight, recency_weight, rrf_k, importance_floor;
END;
$function$;
