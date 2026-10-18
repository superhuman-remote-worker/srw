-- =============================================================================
-- GENERATED FILE — DO NOT EDIT BY HAND.
--
-- Canonical current schema for the 'vector' database, produced by replaying every
-- migration under src/orchestrator/database/migrations/vector/ from zero into a
-- throwaway container and dumping the result.
--
-- Source of truth : src/orchestrator/database/migrations/vector/*.sql
-- Regenerate      : scripts/schema-snapshot.sh vector
-- CI enforces that this file matches a fresh regeneration (db-migrations.yml).
-- The frozen src/orchestrator/database/{schema,vector_schema}.sql snapshots are a
-- separate, historical concern; THIS file tracks the live migration chain.
--
-- (No runtime-only objects for this family.)
-- ==============================================================================

--
-- PostgreSQL database dump
--



SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: pg_trgm; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public;


--
-- Name: EXTENSION pg_trgm; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION pg_trgm IS 'text similarity measurement and index searching based on trigrams';


--
-- Name: uuid-ossp; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS "uuid-ossp" WITH SCHEMA public;


--
-- Name: EXTENSION "uuid-ossp"; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION "uuid-ossp" IS 'generate universally unique identifiers (UUIDs)';


--
-- Name: vector; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public;


--
-- Name: EXTENSION vector; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION vector IS 'vector data type and ivfflat and hnsw access methods';


--
-- Name: confidence_level; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.confidence_level AS ENUM (
    'high',
    'medium',
    'low'
);


--
-- Name: extraction_method; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.extraction_method AS ENUM (
    'direct_quote',
    'paraphrase',
    'inference',
    'aggregation',
    'negative'
);


--
-- Name: source_type; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.source_type AS ENUM (
    'document',
    'website',
    'database',
    'custom'
);


--
-- Name: verification_status; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.verification_status AS ENUM (
    'pending',
    'verified',
    'failed',
    'unverified'
);


SET default_table_access_method = heap;

--
-- Name: knowledge_index; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.knowledge_index (
    id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    note_id character varying(100) NOT NULL,
    project_id uuid NOT NULL,
    title text NOT NULL,
    note_type character varying(50) NOT NULL,
    status character varying(50) DEFAULT 'active'::character varying,
    confidence character varying(20),
    tags text[] DEFAULT '{}'::text[],
    keywords text[] DEFAULT '{}'::text[],
    job_id uuid,
    phase integer,
    content text NOT NULL,
    retrieval_messages text[] DEFAULT '{}'::text[],
    embedding public.vector(4096),
    search_doc tsvector,
    created_at timestamp with time zone,
    modified_at timestamp with time zone,
    indexed_at timestamp with time zone DEFAULT now(),
    content_hash character varying(64),
    remaining_cycles integer,
    last_verified_cycle integer,
    kb_id uuid,
    path text,
    blob_sha character varying(64),
    superseded_by character varying(100),
    invalidated_at timestamp with time zone,
    embedding_version text,
    priority smallint DEFAULT 1 NOT NULL,
    ready_at timestamp with time zone,
    CONSTRAINT knowledge_index_priority_valid CHECK (((priority >= 0) AND (priority <= 2))),
    CONSTRAINT valid_note_status CHECK (((status)::text = ANY ((ARRAY['active'::character varying, 'resolved'::character varying, 'superseded'::character varying, 'archived'::character varying])::text[]))),
    CONSTRAINT valid_note_type CHECK (((note_type)::text = ANY ((ARRAY['goal'::character varying, 'plan'::character varying, 'decision'::character varying, 'learning'::character varying, 'code'::character varying, 'source'::character varying, 'question'::character varying, 'state'::character varying, 'retrospective'::character varying, 'datasource'::character varying, 'feature'::character varying, 'issue'::character varying, 'idea'::character varying, 'charter'::character varying, 'report'::character varying])::text[])))
);


--
-- Name: COLUMN knowledge_index.priority; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.knowledge_index.priority IS 'Backlog rank: 0=high, 1=normal, 2=low. A display label only — no code path may gate or reorder work on it.';


--
-- Name: COLUMN knowledge_index.ready_at; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.knowledge_index.ready_at IS 'When this ticket was last authorized for dispatch (officer stamped `ready`). NULL means unauthorized: a ticket carrying the `ready` tag with no ready_at fails CLOSED and is not dispatchable, which is the deliberate outcome after a vault rebuild that lost the value. Compared against the newest claiming job''s created_at to implement one-shot claims.';


--
-- Name: knowledge_chunk_hybrid_search(text, public.vector, uuid[], text, integer, double precision, double precision, double precision, integer); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.knowledge_chunk_hybrid_search(query_text text, query_embedding public.vector, kb_ids_param uuid[], version_param text DEFAULT NULL::text, match_count integer DEFAULT 15, dense_weight double precision DEFAULT 0.6, sparse_weight double precision DEFAULT 0.3, recency_weight double precision DEFAULT 0.1, rrf_k integer DEFAULT 60) RETURNS SETOF public.knowledge_index
    LANGUAGE plpgsql
    SET "hnsw.iterative_scan" TO 'relaxed_order'
    AS $_$
BEGIN
RETURN QUERY EXECUTE $q$
-- Dense arm: top chunks by vector distance (HNSW), collapsed to the best-ranked
-- chunk per note. Over-fetch $5 * 4 chunks so a note whose best chunk
-- ranks behind several other notes' chunks still survives the collapse.
WITH dense AS (
    SELECT mid, MIN(rank_ix) AS rank_ix FROM (
        SELECT c.note_row AS mid,
               ROW_NUMBER() OVER (
                   ORDER BY subvector(c.embedding, 1, 4000)::halfvec(4000)
                            <=> subvector($2, 1, 4000)::halfvec(4000)
               ) AS rank_ix
        FROM knowledge_chunks c
        JOIN knowledge_index ki ON ki.id = c.note_row
        WHERE c.kb_id = ANY($3)
          AND ki.status = 'active'
          AND c.embedding IS NOT NULL
          AND ($4 IS NULL OR c.embedding_version = $4)
        ORDER BY subvector(c.embedding, 1, 4000)::halfvec(4000)
                 <=> subvector($2, 1, 4000)::halfvec(4000)
        LIMIT $5 * 4
    ) ranked_chunks
    GROUP BY mid
),
-- Sparse arm: chunk-granular tsvector match, same best-chunk-per-note collapse.
sparse AS (
    SELECT mid, MIN(rank_ix) AS rank_ix FROM (
        SELECT c.note_row AS mid,
               ROW_NUMBER() OVER (
                   ORDER BY ts_rank_cd(c.search_doc, websearch_to_tsquery('english', $1)) DESC
               ) AS rank_ix
        FROM knowledge_chunks c
        JOIN knowledge_index ki ON ki.id = c.note_row
        WHERE c.kb_id = ANY($3)
          AND ki.status = 'active'
          AND ($4 IS NULL OR c.embedding_version = $4)
          AND c.search_doc @@ websearch_to_tsquery('english', $1)
        ORDER BY ts_rank_cd(c.search_doc, websearch_to_tsquery('english', $1)) DESC
        LIMIT $5 * 4
    ) ranked_chunks
    GROUP BY mid
),
-- Recency arm: note-level freshness, restricted to notes that actually carry
-- chunks of the current pipeline version (keeps ghosts out — see header).
recent AS (
    SELECT ki.id AS mid, ROW_NUMBER() OVER (ORDER BY ki.modified_at DESC) AS rank_ix
    FROM knowledge_index ki
    WHERE ki.kb_id = ANY($3) AND ki.status = 'active'
      AND EXISTS (
          SELECT 1 FROM knowledge_chunks c
          WHERE c.note_row = ki.id
            AND ($4 IS NULL OR c.embedding_version = $4)
      )
    ORDER BY rank_ix LIMIT $5
)
SELECT ki.* FROM (
    SELECT COALESCE(d.mid, s.mid, r.mid) AS mid,
        COALESCE(1.0 / ($9 + d.rank_ix), 0.0) * $6 +
        COALESCE(1.0 / ($9 + s.rank_ix), 0.0) * $7 +
        COALESCE(1.0 / ($9 + r.rank_ix), 0.0) * $8 AS rrf_score
    FROM dense d
             FULL OUTER JOIN sparse s ON d.mid = s.mid
             FULL OUTER JOIN recent r ON COALESCE(d.mid, s.mid) = r.mid) ranked
                     JOIN knowledge_index ki ON ranked.mid = ki.id
ORDER BY ranked.rrf_score DESC LIMIT $5
$q$ USING query_text, query_embedding, kb_ids_param, version_param, match_count, dense_weight, sparse_weight, recency_weight, rrf_k;
END;
$_$;


--
-- Name: knowledge_chunk_multi_angle_search(text, public.vector, uuid[], text, integer, double precision, double precision, double precision, text[], double precision, text[], double precision, integer); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.knowledge_chunk_multi_angle_search(query_text text, query_embedding public.vector, kb_ids_param uuid[], version_param text DEFAULT NULL::text, match_count integer DEFAULT 15, dense_weight double precision DEFAULT 0.6, sparse_weight double precision DEFAULT 0.3, recency_weight double precision DEFAULT 0.1, exact_terms text[] DEFAULT '{}'::text[], exact_weight double precision DEFAULT 0.6, tag_terms text[] DEFAULT '{}'::text[], tag_weight double precision DEFAULT 0.2, rrf_k integer DEFAULT 60) RETURNS TABLE(note_row uuid, rrf_score double precision, arms text[])
    LANGUAGE plpgsql
    SET "hnsw.iterative_scan" TO 'relaxed_order'
    AS $_$
BEGIN
RETURN QUERY EXECUTE $q$
-- Dense arm: copy of knowledge_chunk_hybrid_search's dense arm, empty when
-- $2 IS NULL. Over-fetch $5 * 4 chunks so a note whose best chunk ranks behind
-- several other notes' chunks still survives the per-note collapse.
WITH dense AS (
    SELECT mid, MIN(rank_ix) AS rank_ix FROM (
        SELECT c.note_row AS mid,
               ROW_NUMBER() OVER (
                   ORDER BY subvector(c.embedding, 1, 4000)::halfvec(4000)
                            <=> subvector($2, 1, 4000)::halfvec(4000)
               ) AS rank_ix
        FROM knowledge_chunks c
        JOIN knowledge_index ki ON ki.id = c.note_row
        WHERE $2 IS NOT NULL
          AND c.kb_id = ANY($3)
          AND ki.status = 'active'
          AND c.embedding IS NOT NULL
          AND ($4 IS NULL OR c.embedding_version = $4)
        ORDER BY subvector(c.embedding, 1, 4000)::halfvec(4000)
                 <=> subvector($2, 1, 4000)::halfvec(4000)
        LIMIT $5 * 4
    ) ranked_chunks
    GROUP BY mid
),
-- Sparse arm: copy of the existing chunk-granular tsvector arm, empty when
-- $1 = ''.
sparse AS (
    SELECT mid, MIN(rank_ix) AS rank_ix FROM (
        SELECT c.note_row AS mid,
               ROW_NUMBER() OVER (
                   ORDER BY ts_rank_cd(c.search_doc, websearch_to_tsquery('english', $1)) DESC
               ) AS rank_ix
        FROM knowledge_chunks c
        JOIN knowledge_index ki ON ki.id = c.note_row
        WHERE $1 <> ''
          AND c.kb_id = ANY($3)
          AND ki.status = 'active'
          AND ($4 IS NULL OR c.embedding_version = $4)
          AND c.search_doc @@ websearch_to_tsquery('english', $1)
        ORDER BY ts_rank_cd(c.search_doc, websearch_to_tsquery('english', $1)) DESC
        LIMIT $5 * 4
    ) ranked_chunks
    GROUP BY mid
),
-- Recency arm: note-level freshness, restricted to notes that actually carry
-- chunks of the current pipeline version (keeps ghosts out). Two deliberate
-- differences from the old function, both explained in the header: NULLS LAST,
-- and the "there is a semantic query to freshen" gate.
recent AS (
    SELECT ki.id AS mid,
           ROW_NUMBER() OVER (ORDER BY ki.modified_at DESC NULLS LAST) AS rank_ix
    FROM knowledge_index ki
    WHERE ($1 <> '' OR $2 IS NOT NULL)
      AND ki.kb_id = ANY($3) AND ki.status = 'active'
      AND EXISTS (
          SELECT 1 FROM knowledge_chunks c
          WHERE c.note_row = ki.id
            AND ($4 IS NULL OR c.embedding_version = $4)
      )
    ORDER BY rank_ix LIMIT $5
),
-- Exact arm: one row per note containing ANY term (case-insensitive substring),
-- ranked by the best trigram similarity across terms. Uses
-- idx_knowledge_content_trgm (0024).
exact AS (
    SELECT x.id AS mid, ROW_NUMBER() OVER (ORDER BY x.best DESC) AS rank_ix
    FROM (
        SELECT ki.id,
               MAX(GREATEST(similarity(ki.content, t.term),
                            similarity(ki.title, t.term))) AS best
        FROM knowledge_index ki, unnest($9) AS t(term)
        WHERE cardinality($9) > 0
          AND ki.kb_id = ANY($3) AND ki.status = 'active'
          AND ki.path IS NOT NULL
          AND (ki.content ILIKE '%' || t.term || '%'
               OR ki.title ILIKE '%' || t.term || '%')
        GROUP BY ki.id
    ) x
    ORDER BY rank_ix LIMIT $5 * 4
),
-- Tag arm: a BOOST, not a filter — notes carrying any requested tag, by recency.
tagged AS (
    SELECT ki.id AS mid,
           ROW_NUMBER() OVER (ORDER BY ki.modified_at DESC NULLS LAST) AS rank_ix
    FROM knowledge_index ki
    WHERE cardinality($11) > 0
      AND ki.kb_id = ANY($3) AND ki.status = 'active'
      AND ki.path IS NOT NULL AND ki.tags && $11
    ORDER BY rank_ix LIMIT $5 * 4
),
fused AS (
    SELECT COALESCE(d.mid, s.mid, r.mid, e.mid, g.mid) AS mid,
           COALESCE(1.0 / ($13 + d.rank_ix), 0.0) * $6 +
           COALESCE(1.0 / ($13 + s.rank_ix), 0.0) * $7 +
           COALESCE(1.0 / ($13 + r.rank_ix), 0.0) * $8 +
           COALESCE(1.0 / ($13 + e.rank_ix), 0.0) * $10 +
           COALESCE(1.0 / ($13 + g.rank_ix), 0.0) * $12 AS score,
           ARRAY_REMOVE(ARRAY[
               CASE WHEN d.mid IS NOT NULL THEN 'dense'::text END,
               CASE WHEN s.mid IS NOT NULL THEN 'sparse'::text END,
               CASE WHEN r.mid IS NOT NULL THEN 'recency'::text END,
               CASE WHEN e.mid IS NOT NULL THEN 'exact'::text END,
               CASE WHEN g.mid IS NOT NULL THEN 'tag'::text END], NULL) AS arms
    FROM dense d
             FULL OUTER JOIN sparse s ON d.mid = s.mid
             FULL OUTER JOIN recent r ON COALESCE(d.mid, s.mid) = r.mid
             FULL OUTER JOIN exact  e ON COALESCE(d.mid, s.mid, r.mid) = e.mid
             FULL OUTER JOIN tagged g ON COALESCE(d.mid, s.mid, r.mid, e.mid) = g.mid
)
SELECT mid, score, arms FROM fused ORDER BY score DESC LIMIT $5
$q$ USING query_text, query_embedding, kb_ids_param, version_param, match_count,
          dense_weight, sparse_weight, recency_weight, exact_terms, exact_weight,
          tag_terms, tag_weight, rrf_k;
END;
$_$;


--
-- Name: knowledge_hybrid_search(text, public.vector, uuid, integer, double precision, double precision, double precision, integer); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.knowledge_hybrid_search(query_text text, query_embedding public.vector, project_id_param uuid, match_count integer DEFAULT 10, dense_weight double precision DEFAULT 0.6, sparse_weight double precision DEFAULT 0.3, recency_weight double precision DEFAULT 0.1, rrf_k integer DEFAULT 50) RETURNS SETOF public.knowledge_index
    LANGUAGE plpgsql
    SET "hnsw.iterative_scan" TO 'relaxed_order'
    AS $_$
BEGIN
RETURN QUERY EXECUTE $q$
WITH dense AS (
    SELECT id, ROW_NUMBER() OVER (ORDER BY subvector(embedding, 1, 4000)::halfvec(4000) <=> subvector($2, 1, 4000)::halfvec(4000)) AS rank_ix
    FROM knowledge_index
    WHERE project_id = $3 AND status = 'active' AND embedding IS NOT NULL
    ORDER BY rank_ix LIMIT $4 * 2
),
sparse AS (
    SELECT id, ROW_NUMBER() OVER (ORDER BY ts_rank_cd(search_doc, websearch_to_tsquery('english', $1)) DESC) AS rank_ix
    FROM knowledge_index
    WHERE project_id = $3 AND status = 'active'
      AND search_doc @@ websearch_to_tsquery('english', $1)
    ORDER BY rank_ix LIMIT $4 * 2
),
recent AS (
    SELECT id, ROW_NUMBER() OVER (ORDER BY modified_at DESC) AS rank_ix
    FROM knowledge_index
    WHERE project_id = $3 AND status = 'active'
    ORDER BY rank_ix LIMIT $4
)
SELECT ki.* FROM (
    SELECT COALESCE(d.id, s.id, r.id) AS mid,
        COALESCE(1.0 / ($8 + d.rank_ix), 0.0) * $5 +
        COALESCE(1.0 / ($8 + s.rank_ix), 0.0) * $6 +
        COALESCE(1.0 / ($8 + r.rank_ix), 0.0) * $7 AS rrf_score
    FROM dense d
             FULL OUTER JOIN sparse s ON d.id = s.id
             FULL OUTER JOIN recent r ON COALESCE(d.id, s.id) = r.id) ranked
                     JOIN knowledge_index ki ON ranked.mid = ki.id
ORDER BY ranked.rrf_score DESC LIMIT $4
$q$ USING query_text, query_embedding, project_id_param, match_count, dense_weight, sparse_weight, recency_weight, rrf_k;
END;
$_$;


--
-- Name: knowledge_index_capture_revision(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.knowledge_index_capture_revision() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    v_action TEXT := 'delete';
    v_replaced_by UUID := NULL;        -- a deleted version was replaced by nothing
BEGIN
    IF TG_OP = 'UPDATE' THEN
        v_action := 'update';
        v_replaced_by := NEW.job_id;
    END IF;
    INSERT INTO knowledge_note_revisions (
        project_id, note_id, title, note_type, status, confidence,
        tags, keywords, job_id, phase, content, created_at, modified_at,
        action, replaced_by_job_id
    ) VALUES (
        OLD.project_id, OLD.note_id, OLD.title, OLD.note_type, OLD.status,
        OLD.confidence, OLD.tags, OLD.keywords, OLD.job_id, OLD.phase,
        OLD.content, OLD.created_at, OLD.modified_at,
        v_action, v_replaced_by
    );
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END;
$$;


--
-- Name: knowledge_multi_project_hybrid_search(text, public.vector, uuid[], integer, double precision, double precision, double precision, integer); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.knowledge_multi_project_hybrid_search(query_text text, query_embedding public.vector, project_ids_param uuid[], match_count integer DEFAULT 10, dense_weight double precision DEFAULT 0.6, sparse_weight double precision DEFAULT 0.3, recency_weight double precision DEFAULT 0.1, rrf_k integer DEFAULT 50) RETURNS SETOF public.knowledge_index
    LANGUAGE plpgsql
    SET "hnsw.iterative_scan" TO 'relaxed_order'
    AS $_$
BEGIN
RETURN QUERY EXECUTE $q$
WITH dense AS (
    SELECT id, ROW_NUMBER() OVER (ORDER BY subvector(embedding, 1, 4000)::halfvec(4000) <=> subvector($2, 1, 4000)::halfvec(4000)) AS rank_ix
    FROM knowledge_index
    WHERE project_id = ANY($3) AND status = 'active' AND embedding IS NOT NULL
    ORDER BY rank_ix LIMIT $4 * 2
),
sparse AS (
    SELECT id, ROW_NUMBER() OVER (ORDER BY ts_rank_cd(search_doc, websearch_to_tsquery('english', $1)) DESC) AS rank_ix
    FROM knowledge_index
    WHERE project_id = ANY($3) AND status = 'active'
      AND search_doc @@ websearch_to_tsquery('english', $1)
    ORDER BY rank_ix LIMIT $4 * 2
),
recent AS (
    SELECT id, ROW_NUMBER() OVER (ORDER BY modified_at DESC) AS rank_ix
    FROM knowledge_index
    WHERE project_id = ANY($3) AND status = 'active'
    ORDER BY rank_ix LIMIT $4
)
SELECT ki.*
FROM (SELECT COALESCE(d.id, s.id, r.id)                           AS mid,
             COALESCE(1.0 / ($8 + d.rank_ix), 0.0) * $5 +
        COALESCE(1.0 / ($8 + s.rank_ix), 0.0) * $6 +
        COALESCE(1.0 / ($8 + r.rank_ix), 0.0) * $7 AS rrf_score
    FROM dense d
    FULL OUTER JOIN sparse s ON d.id = s.id
    FULL OUTER JOIN recent r ON COALESCE(d.id, s.id) = r.id
) ranked
JOIN knowledge_index ki ON ranked.mid = ki.id
ORDER BY ranked.rrf_score DESC
LIMIT $4
$q$ USING query_text, query_embedding, project_ids_param, match_count, dense_weight, sparse_weight, recency_weight, rrf_k;
END;
$_$;


--
-- Name: memories; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.memories (
    id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    job_id uuid NOT NULL,
    agent_id character varying(100),
    content text NOT NULL,
    summary character varying(500),
    memory_type character varying(50) DEFAULT 'factual'::character varying,
    source character varying(50) DEFAULT 'observer'::character varying,
    keywords text[] DEFAULT '{}'::text[],
    embedding public.vector(4096),
    sparse_keywords tsvector,
    importance double precision DEFAULT 0.5,
    source_turn_start integer,
    source_turn_end integer,
    source_phase integer,
    token_count integer DEFAULT 0,
    access_count integer DEFAULT 0,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    last_accessed timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    project_id uuid,
    remaining_turns integer,
    valid_from timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    valid_to timestamp with time zone,
    superseded_at timestamp with time zone,
    superseded_by uuid,
    CONSTRAINT valid_memory_source CHECK (((source)::text = ANY ((ARRAY['observer'::character varying, 'todo'::character varying, 'compaction'::character varying, 'phase_archive'::character varying, 'tool_error'::character varying])::text[]))),
    CONSTRAINT valid_memory_type CHECK (((memory_type)::text = ANY ((ARRAY['factual'::character varying, 'procedural'::character varying, 'error_solution'::character varying, 'vocabulary'::character varying, 'relational'::character varying])::text[])))
);


--
-- Name: memory_hybrid_search(text, public.vector, uuid, integer, double precision, double precision, double precision, integer, double precision); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.memory_hybrid_search(query_text text, query_embedding public.vector, job_id_param uuid, match_count integer DEFAULT 10, dense_weight double precision DEFAULT 0.6, sparse_weight double precision DEFAULT 0.3, recency_weight double precision DEFAULT 0.1, rrf_k integer DEFAULT 50, importance_floor double precision DEFAULT 0.0) RETURNS SETOF public.memories
    LANGUAGE plpgsql
    SET "hnsw.iterative_scan" TO 'relaxed_order'
    AS $_$
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
$_$;


--
-- Name: memory_multi_project_hybrid_search(text, public.vector, uuid[], integer, double precision, double precision, double precision, integer, double precision); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.memory_multi_project_hybrid_search(query_text text, query_embedding public.vector, project_ids_param uuid[], match_count integer DEFAULT 10, dense_weight double precision DEFAULT 0.6, sparse_weight double precision DEFAULT 0.3, recency_weight double precision DEFAULT 0.1, rrf_k integer DEFAULT 50, importance_floor double precision DEFAULT 0.0) RETURNS SETOF public.memories
    LANGUAGE plpgsql
    SET "hnsw.iterative_scan" TO 'relaxed_order'
    AS $_$
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
$_$;


--
-- Name: memory_project_hybrid_search(text, public.vector, uuid, integer, double precision, double precision, double precision, integer, double precision); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.memory_project_hybrid_search(query_text text, query_embedding public.vector, project_id_param uuid, match_count integer DEFAULT 10, dense_weight double precision DEFAULT 0.6, sparse_weight double precision DEFAULT 0.3, recency_weight double precision DEFAULT 0.1, rrf_k integer DEFAULT 50, importance_floor double precision DEFAULT 0.0) RETURNS SETOF public.memories
    LANGUAGE plpgsql
    SET "hnsw.iterative_scan" TO 'relaxed_order'
    AS $_$
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
$_$;


--
-- Name: protect_job_vector_scope_retirement(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.protect_job_vector_scope_retirement() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'Vector scope fences cannot be removed'
            USING ERRCODE='23514', CONSTRAINT='job_vector_scope_retirement_immutable';
    END IF;
    IF NEW.job_id IS DISTINCT FROM OLD.job_id
       OR (OLD.retired_at IS NOT NULL AND NEW.retired_at IS DISTINCT FROM OLD.retired_at) THEN
        RAISE EXCEPTION 'Vector scope retirement cannot be changed'
            USING ERRCODE='23514', CONSTRAINT='job_vector_scope_retirement_immutable';
    END IF;
    RETURN NEW;
END;
$$;


--
-- Name: require_active_job_vector_scope(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.require_active_job_vector_scope() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    retired TIMESTAMPTZ;
BEGIN
    INSERT INTO public.job_vector_scopes (job_id) VALUES (NEW.job_id)
        ON CONFLICT (job_id) DO NOTHING;
    -- FOR SHARE conflicts with the retirement's non-key UPDATE. Hold it until
    -- the producer transaction commits, including any retrieval-message rows.
    SELECT retired_at INTO retired FROM public.job_vector_scopes
     WHERE job_id=NEW.job_id FOR SHARE;
    IF retired IS NOT NULL THEN
        RAISE EXCEPTION 'The job vector scope is permanently retired'
            USING ERRCODE='23514', CONSTRAINT='job_vector_scope_retired';
    END IF;
    RETURN NEW;
END;
$$;


--
-- Name: retire_job_vector_scope(uuid); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.retire_job_vector_scope(requested_job uuid) RETURNS void
    LANGUAGE plpgsql
    AS $$
BEGIN
    INSERT INTO public.job_vector_scopes (job_id) VALUES (requested_job)
        ON CONFLICT (job_id) DO NOTHING;
    PERFORM 1 FROM public.job_vector_scopes WHERE job_id=requested_job FOR UPDATE;
    UPDATE public.job_vector_scopes SET retired_at=clock_timestamp()
     WHERE job_id=requested_job AND retired_at IS NULL;

    -- One vector transaction commits the fence and the whole existing API
    -- cleanup set. Retrieval messages cascade. Shared sources, project/session
    -- memories, knowledge and audit retention remain outside this job scope.
    DELETE FROM public.memories WHERE job_id=requested_job;
    DELETE FROM public.citations WHERE job_id=requested_job;
    DELETE FROM public.source_annotations WHERE job_id=requested_job;
    DELETE FROM public.source_tags WHERE job_id=requested_job;
    DELETE FROM public.source_embeddings WHERE job_id=requested_job;
    DELETE FROM public.job_sources WHERE job_id=requested_job;
END;
$$;


--
-- Name: citations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.citations (
    id integer NOT NULL,
    job_id uuid NOT NULL,
    claim text NOT NULL,
    verbatim_quote text,
    quote_context text NOT NULL,
    quote_language text,
    relevance_reasoning text,
    confidence public.confidence_level DEFAULT 'high'::public.confidence_level,
    extraction_method public.extraction_method DEFAULT 'direct_quote'::public.extraction_method,
    source_id integer NOT NULL,
    locator jsonb NOT NULL,
    verification_status public.verification_status DEFAULT 'pending'::public.verification_status,
    verification_notes text,
    similarity_score real,
    matched_location jsonb,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    created_by text
);


--
-- Name: citations_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.citations_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: citations_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.citations_id_seq OWNED BY public.citations.id;


--
-- Name: job_sources; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.job_sources (
    job_id uuid NOT NULL,
    source_id integer NOT NULL,
    added_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: job_vector_scopes; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.job_vector_scopes (
    job_id uuid NOT NULL,
    retired_at timestamp with time zone
);


--
-- Name: TABLE job_vector_scopes; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE public.job_vector_scopes IS 'Destination write fence for the existing job_id scope. Session UUIDs also use this column and remain active: only permanent job deletion retires a scope. No cross-database FK or expiry; late writers must see the tombstone.';


--
-- Name: kb_index_watermark; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.kb_index_watermark (
    kb_id uuid NOT NULL,
    repo_name text,
    branch text,
    indexed_commit character varying(64),
    pipeline_version text,
    updated_at timestamp with time zone DEFAULT now(),
    source_head character varying(64),
    status text DEFAULT 'ready'::text NOT NULL,
    last_attempt_at timestamp with time zone,
    last_success_at timestamp with time zone,
    last_error text,
    notes_done integer,
    notes_total integer,
    error_fingerprint text,
    error_streak integer DEFAULT 0 NOT NULL,
    wedged_since timestamp with time zone,
    advisory text,
    CONSTRAINT kb_index_watermark_status_valid CHECK ((status = ANY (ARRAY['pending'::text, 'indexing'::text, 'ready'::text, 'partial'::text, 'failed'::text])))
);


--
-- Name: knowledge_chunks; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.knowledge_chunks (
    id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    note_row uuid NOT NULL,
    kb_id uuid NOT NULL,
    chunk_ix integer NOT NULL,
    heading_path text,
    content text NOT NULL,
    embedding public.vector(4096),
    search_doc tsvector,
    embedding_version text,
    created_at timestamp with time zone DEFAULT now()
);


--
-- Name: knowledge_links; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.knowledge_links (
    id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    source_note_row uuid NOT NULL,
    kb_id uuid NOT NULL,
    source_id character varying(100) NOT NULL,
    target_id character varying(100) NOT NULL,
    rel_type text DEFAULT 'references'::text NOT NULL,
    created_at timestamp with time zone DEFAULT now()
);


--
-- Name: knowledge_note_revisions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.knowledge_note_revisions (
    id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    project_id uuid NOT NULL,
    note_id character varying(100) NOT NULL,
    title text NOT NULL,
    note_type character varying(50) NOT NULL,
    status character varying(50),
    confidence character varying(20),
    tags text[] DEFAULT '{}'::text[],
    keywords text[] DEFAULT '{}'::text[],
    job_id uuid,
    phase integer,
    content text NOT NULL,
    created_at timestamp with time zone,
    modified_at timestamp with time zone,
    action character varying(10) NOT NULL,
    replaced_by_job_id uuid,
    changed_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT valid_revision_action CHECK (((action)::text = ANY ((ARRAY['update'::character varying, 'delete'::character varying])::text[])))
);


--
-- Name: memory_retrieval_messages; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.memory_retrieval_messages (
    id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    memory_id uuid NOT NULL,
    message text NOT NULL,
    embedding public.vector(4096),
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: project_loop_ttl_effects; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.project_loop_ttl_effects (
    loop_id uuid NOT NULL,
    total_jobs_run integer NOT NULL,
    completed_member_id uuid NOT NULL,
    project_id uuid NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT project_loop_ttl_effects_total_jobs_run_check CHECK ((total_jobs_run >= 0))
);


--
-- Name: TABLE project_loop_ttl_effects; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE public.project_loop_ttl_effects IS 'Immutable project-loop turn identities whose knowledge_index cycle TTL decrement committed. A key collision with different project/member identity is corruption and callers fail closed.';


--
-- Name: schema_migrations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.schema_migrations (
    filename text NOT NULL,
    checksum text NOT NULL,
    applied_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    applied_by text DEFAULT CURRENT_USER NOT NULL,
    execution_ms integer NOT NULL,
    success boolean DEFAULT true NOT NULL,
    error text
);


--
-- Name: session_memory_effect_executions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.session_memory_effect_executions (
    producer_id uuid NOT NULL,
    effect_name text NOT NULL,
    thread_id uuid NOT NULL,
    input_message_id uuid NOT NULL,
    turn_number integer NOT NULL,
    boundary_seq bigint NOT NULL,
    end_seq bigint NOT NULL,
    memory_scope_kind text NOT NULL,
    memory_scope_id uuid NOT NULL,
    state text DEFAULT 'writing'::text NOT NULL,
    extracted_count integer,
    stored_count integer,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    completed_at timestamp with time zone,
    CONSTRAINT session_memory_effect_name CHECK ((effect_name = 'final_memory_extraction'::text)),
    CONSTRAINT session_memory_effect_scope CHECK (((memory_scope_kind = ANY (ARRAY['thread'::text, 'project'::text])) AND ((memory_scope_kind <> 'thread'::text) OR (memory_scope_id = thread_id)))),
    CONSTRAINT session_memory_effect_seq_window CHECK (((boundary_seq > 0) AND (end_seq >= boundary_seq))),
    CONSTRAINT session_memory_effect_state CHECK ((state = ANY (ARRAY['writing'::text, 'done'::text]))),
    CONSTRAINT session_memory_effect_terminal_shape CHECK ((((state = 'writing'::text) AND (extracted_count IS NULL) AND (stored_count IS NULL) AND (completed_at IS NULL)) OR ((state = 'done'::text) AND (extracted_count >= 0) AND (stored_count >= 0) AND (stored_count <= extracted_count) AND (completed_at IS NOT NULL)))),
    CONSTRAINT session_memory_effect_turn_positive CHECK ((turn_number > 0))
);


--
-- Name: TABLE session_memory_effect_executions; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE public.session_memory_effect_executions IS 'Immutable destination receipts for session_turn final-memory effects. The writing row is inserted, all memory mutations run, and the row becomes done in one vector transaction. Rows are not time-pruned: a delayed app-DB receipt replay must never become a new vector mutation.';


--
-- Name: COLUMN session_memory_effect_executions.producer_id; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.session_memory_effect_executions.producer_id IS 'turn_execution_id minted by the fenced app-DB final-persist transaction.';


--
-- Name: COLUMN session_memory_effect_executions.memory_scope_id; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.session_memory_effect_executions.memory_scope_id IS 'Immutable thread or project destination captured with the accepted turn.';


--
-- Name: source_annotations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.source_annotations (
    id integer NOT NULL,
    source_id integer NOT NULL,
    job_id uuid NOT NULL,
    annotation_type text DEFAULT 'note'::text NOT NULL,
    content text NOT NULL,
    page_reference text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    created_by text
);


--
-- Name: source_annotations_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.source_annotations_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: source_annotations_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.source_annotations_id_seq OWNED BY public.source_annotations.id;


--
-- Name: source_embeddings; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.source_embeddings (
    id integer NOT NULL,
    source_id integer NOT NULL,
    job_id uuid NOT NULL,
    chunk_index integer DEFAULT 0 NOT NULL,
    chunk_text text NOT NULL,
    embedding public.vector(4096),
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: source_embeddings_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.source_embeddings_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: source_embeddings_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.source_embeddings_id_seq OWNED BY public.source_embeddings.id;


--
-- Name: source_tags; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.source_tags (
    id integer NOT NULL,
    source_id integer NOT NULL,
    job_id uuid NOT NULL,
    tag text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: source_tags_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.source_tags_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: source_tags_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.source_tags_id_seq OWNED BY public.source_tags.id;


--
-- Name: sources; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.sources (
    id integer NOT NULL,
    type public.source_type NOT NULL,
    identifier text NOT NULL,
    name text NOT NULL,
    version text,
    content text NOT NULL,
    content_hash text,
    metadata jsonb,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: sources_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.sources_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: sources_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.sources_id_seq OWNED BY public.sources.id;


--
-- Name: citations id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.citations ALTER COLUMN id SET DEFAULT nextval('public.citations_id_seq'::regclass);


--
-- Name: source_annotations id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.source_annotations ALTER COLUMN id SET DEFAULT nextval('public.source_annotations_id_seq'::regclass);


--
-- Name: source_embeddings id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.source_embeddings ALTER COLUMN id SET DEFAULT nextval('public.source_embeddings_id_seq'::regclass);


--
-- Name: source_tags id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.source_tags ALTER COLUMN id SET DEFAULT nextval('public.source_tags_id_seq'::regclass);


--
-- Name: sources id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.sources ALTER COLUMN id SET DEFAULT nextval('public.sources_id_seq'::regclass);


--
-- Name: citations citations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.citations
    ADD CONSTRAINT citations_pkey PRIMARY KEY (id);


--
-- Name: job_sources job_sources_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.job_sources
    ADD CONSTRAINT job_sources_pkey PRIMARY KEY (job_id, source_id);


--
-- Name: job_vector_scopes job_vector_scopes_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.job_vector_scopes
    ADD CONSTRAINT job_vector_scopes_pkey PRIMARY KEY (job_id);


--
-- Name: kb_index_watermark kb_index_watermark_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.kb_index_watermark
    ADD CONSTRAINT kb_index_watermark_pkey PRIMARY KEY (kb_id);


--
-- Name: knowledge_chunks knowledge_chunks_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.knowledge_chunks
    ADD CONSTRAINT knowledge_chunks_pkey PRIMARY KEY (id);


--
-- Name: knowledge_index knowledge_index_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.knowledge_index
    ADD CONSTRAINT knowledge_index_pkey PRIMARY KEY (id);


--
-- Name: knowledge_links knowledge_links_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.knowledge_links
    ADD CONSTRAINT knowledge_links_pkey PRIMARY KEY (id);


--
-- Name: knowledge_note_revisions knowledge_note_revisions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.knowledge_note_revisions
    ADD CONSTRAINT knowledge_note_revisions_pkey PRIMARY KEY (id);


--
-- Name: memories memories_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.memories
    ADD CONSTRAINT memories_pkey PRIMARY KEY (id);


--
-- Name: memory_retrieval_messages memory_retrieval_messages_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.memory_retrieval_messages
    ADD CONSTRAINT memory_retrieval_messages_pkey PRIMARY KEY (id);


--
-- Name: project_loop_ttl_effects project_loop_ttl_effects_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.project_loop_ttl_effects
    ADD CONSTRAINT project_loop_ttl_effects_pkey PRIMARY KEY (loop_id, total_jobs_run);


--
-- Name: schema_migrations schema_migrations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.schema_migrations
    ADD CONSTRAINT schema_migrations_pkey PRIMARY KEY (filename);


--
-- Name: session_memory_effect_executions session_memory_effect_executions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.session_memory_effect_executions
    ADD CONSTRAINT session_memory_effect_executions_pkey PRIMARY KEY (producer_id, effect_name);


--
-- Name: source_annotations source_annotations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.source_annotations
    ADD CONSTRAINT source_annotations_pkey PRIMARY KEY (id);


--
-- Name: source_embeddings source_embeddings_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.source_embeddings
    ADD CONSTRAINT source_embeddings_pkey PRIMARY KEY (id);


--
-- Name: source_embeddings source_embeddings_source_id_job_id_chunk_index_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.source_embeddings
    ADD CONSTRAINT source_embeddings_source_id_job_id_chunk_index_key UNIQUE (source_id, job_id, chunk_index);


--
-- Name: source_tags source_tags_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.source_tags
    ADD CONSTRAINT source_tags_pkey PRIMARY KEY (id);


--
-- Name: source_tags source_tags_source_id_job_id_tag_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.source_tags
    ADD CONSTRAINT source_tags_source_id_job_id_tag_key UNIQUE (source_id, job_id, tag);


--
-- Name: sources sources_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.sources
    ADD CONSTRAINT sources_pkey PRIMARY KEY (id);


--
-- Name: knowledge_chunks uq_knowledge_chunk; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.knowledge_chunks
    ADD CONSTRAINT uq_knowledge_chunk UNIQUE (note_row, chunk_ix);


--
-- Name: knowledge_index uq_knowledge_project_note; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.knowledge_index
    ADD CONSTRAINT uq_knowledge_project_note UNIQUE (project_id, note_id);


--
-- Name: sources uq_sources_content_hash; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.sources
    ADD CONSTRAINT uq_sources_content_hash UNIQUE (content_hash);


--
-- Name: idx_annotations_content_fts; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_annotations_content_fts ON public.source_annotations USING gin (to_tsvector('simple'::regconfig, content));


--
-- Name: idx_annotations_job; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_annotations_job ON public.source_annotations USING btree (job_id);


--
-- Name: idx_annotations_source; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_annotations_source ON public.source_annotations USING btree (source_id);


--
-- Name: idx_annotations_type; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_annotations_type ON public.source_annotations USING btree (annotation_type);


--
-- Name: idx_citations_claim_fts; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_citations_claim_fts ON public.citations USING gin (to_tsvector('english'::regconfig, claim));


--
-- Name: idx_citations_created_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_citations_created_at ON public.citations USING btree (created_at);


--
-- Name: idx_citations_created_by; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_citations_created_by ON public.citations USING btree (created_by);


--
-- Name: idx_citations_job_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_citations_job_id ON public.citations USING btree (job_id);


--
-- Name: idx_citations_locator; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_citations_locator ON public.citations USING gin (locator);


--
-- Name: idx_citations_source_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_citations_source_id ON public.citations USING btree (source_id);


--
-- Name: idx_citations_verification_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_citations_verification_status ON public.citations USING btree (verification_status);


--
-- Name: idx_knowledge_backlog; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_knowledge_backlog ON public.knowledge_index USING btree (project_id, priority, created_at) WHERE (((status)::text = 'active'::text) AND ((note_type)::text = ANY ((ARRAY['feature'::character varying, 'issue'::character varying, 'idea'::character varying])::text[])));


--
-- Name: idx_knowledge_backlog_page; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_knowledge_backlog_page ON public.knowledge_index USING btree (project_id, priority, created_at, note_id) WHERE (((status)::text = 'active'::text) AND ((note_type)::text = ANY ((ARRAY['feature'::character varying, 'issue'::character varying, 'idea'::character varying])::text[])));


--
-- Name: idx_knowledge_chunks_embedding; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_knowledge_chunks_embedding ON public.knowledge_chunks USING hnsw (((public.subvector(embedding, 1, 4000))::public.halfvec(4000)) public.halfvec_cosine_ops) WITH (m='16', ef_construction='64');


--
-- Name: idx_knowledge_chunks_kb_version; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_knowledge_chunks_kb_version ON public.knowledge_chunks USING btree (kb_id, embedding_version);


--
-- Name: idx_knowledge_chunks_note; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_knowledge_chunks_note ON public.knowledge_chunks USING btree (note_row);


--
-- Name: idx_knowledge_chunks_search; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_knowledge_chunks_search ON public.knowledge_chunks USING gin (search_doc);


--
-- Name: idx_knowledge_content_trgm; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_knowledge_content_trgm ON public.knowledge_index USING gin (content public.gin_trgm_ops);


--
-- Name: idx_knowledge_embedding_halfvec; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_knowledge_embedding_halfvec ON public.knowledge_index USING hnsw (((public.subvector(embedding, 1, 4000))::public.halfvec(4000)) public.halfvec_cosine_ops) WITH (m='16', ef_construction='256');


--
-- Name: idx_knowledge_index_stale; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_knowledge_index_stale ON public.knowledge_index USING btree (project_id) WHERE ((remaining_cycles <= 0) AND ((status)::text = 'active'::text));


--
-- Name: idx_knowledge_kb; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_knowledge_kb ON public.knowledge_index USING btree (kb_id);


--
-- Name: idx_knowledge_links_note_row; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_knowledge_links_note_row ON public.knowledge_links USING btree (source_note_row);


--
-- Name: idx_knowledge_links_source; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_knowledge_links_source ON public.knowledge_links USING btree (kb_id, source_id);


--
-- Name: idx_knowledge_links_target; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_knowledge_links_target ON public.knowledge_links USING btree (kb_id, target_id);


--
-- Name: idx_knowledge_note_revisions_note; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_knowledge_note_revisions_note ON public.knowledge_note_revisions USING btree (project_id, note_id, changed_at DESC);


--
-- Name: idx_knowledge_project; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_knowledge_project ON public.knowledge_index USING btree (project_id);


--
-- Name: idx_knowledge_project_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_knowledge_project_status ON public.knowledge_index USING btree (project_id, status);


--
-- Name: idx_knowledge_project_type; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_knowledge_project_type ON public.knowledge_index USING btree (project_id, note_type);


--
-- Name: idx_knowledge_search; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_knowledge_search ON public.knowledge_index USING gin (search_doc);


--
-- Name: idx_knowledge_tags; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_knowledge_tags ON public.knowledge_index USING gin (tags);


--
-- Name: idx_memories_embedding_halfvec; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_memories_embedding_halfvec ON public.memories USING hnsw (((public.subvector(embedding, 1, 4000))::public.halfvec(4000)) public.halfvec_cosine_ops) WITH (m='16', ef_construction='256');


--
-- Name: idx_memories_job; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_memories_job ON public.memories USING btree (job_id);


--
-- Name: idx_memories_job_accessed; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_memories_job_accessed ON public.memories USING btree (job_id, last_accessed DESC);


--
-- Name: idx_memories_job_importance; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_memories_job_importance ON public.memories USING btree (job_id, importance DESC);


--
-- Name: idx_memories_job_type; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_memories_job_type ON public.memories USING btree (job_id, memory_type);


--
-- Name: idx_memories_job_valid; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_memories_job_valid ON public.memories USING btree (job_id) WHERE (valid_to IS NULL);


--
-- Name: idx_memories_keywords; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_memories_keywords ON public.memories USING gin (keywords);


--
-- Name: idx_memories_project; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_memories_project ON public.memories USING btree (project_id);


--
-- Name: idx_memories_project_importance; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_memories_project_importance ON public.memories USING btree (project_id, importance DESC);


--
-- Name: idx_memories_project_ttl_active; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_memories_project_ttl_active ON public.memories USING btree (project_id, remaining_turns) WHERE (remaining_turns > 0);


--
-- Name: idx_memories_project_valid; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_memories_project_valid ON public.memories USING btree (project_id) WHERE (valid_to IS NULL);


--
-- Name: idx_memories_sparse; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_memories_sparse ON public.memories USING gin (sparse_keywords);


--
-- Name: idx_memories_ttl_active; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_memories_ttl_active ON public.memories USING btree (job_id, remaining_turns) WHERE (remaining_turns > 0);


--
-- Name: idx_memory_retrieval_messages_embedding_halfvec; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_memory_retrieval_messages_embedding_halfvec ON public.memory_retrieval_messages USING hnsw (((public.subvector(embedding, 1, 4000))::public.halfvec(4000)) public.halfvec_cosine_ops) WITH (m='16', ef_construction='256');


--
-- Name: idx_mrm_memory; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_mrm_memory ON public.memory_retrieval_messages USING btree (memory_id);


--
-- Name: idx_source_embeddings_job; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_source_embeddings_job ON public.source_embeddings USING btree (job_id);


--
-- Name: idx_source_embeddings_source; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_source_embeddings_source ON public.source_embeddings USING btree (source_id);


--
-- Name: idx_sources_content_fts; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_sources_content_fts ON public.sources USING gin (to_tsvector('simple'::regconfig, content));


--
-- Name: idx_sources_content_hash; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_sources_content_hash ON public.sources USING btree (content_hash);


--
-- Name: idx_sources_identifier; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_sources_identifier ON public.sources USING btree (identifier);


--
-- Name: idx_sources_metadata; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_sources_metadata ON public.sources USING gin (metadata);


--
-- Name: idx_sources_name; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_sources_name ON public.sources USING btree (name);


--
-- Name: idx_sources_type; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_sources_type ON public.sources USING btree (type);


--
-- Name: idx_tags_job; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_tags_job ON public.source_tags USING btree (job_id);


--
-- Name: idx_tags_tag; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_tags_tag ON public.source_tags USING btree (tag);


--
-- Name: schema_migrations_dirty_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX schema_migrations_dirty_idx ON public.schema_migrations USING btree (filename) WHERE (success = false);


--
-- Name: uq_knowledge_kb_path; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX uq_knowledge_kb_path ON public.knowledge_index USING btree (kb_id, path) WHERE ((kb_id IS NOT NULL) AND (path IS NOT NULL));


--
-- Name: source_annotations annotations_job_scope; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER annotations_job_scope BEFORE INSERT OR UPDATE OF job_id ON public.source_annotations FOR EACH ROW EXECUTE FUNCTION public.require_active_job_vector_scope();


--
-- Name: citations citations_job_scope; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER citations_job_scope BEFORE INSERT OR UPDATE OF job_id ON public.citations FOR EACH ROW EXECUTE FUNCTION public.require_active_job_vector_scope();


--
-- Name: source_embeddings embeddings_job_scope; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER embeddings_job_scope BEFORE INSERT OR UPDATE OF job_id ON public.source_embeddings FOR EACH ROW EXECUTE FUNCTION public.require_active_job_vector_scope();


--
-- Name: job_vector_scopes job_vector_scope_retirement_immutable; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER job_vector_scope_retirement_immutable BEFORE DELETE OR UPDATE ON public.job_vector_scopes FOR EACH ROW EXECUTE FUNCTION public.protect_job_vector_scope_retirement();


--
-- Name: memories memories_job_scope; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER memories_job_scope BEFORE INSERT OR UPDATE OF job_id ON public.memories FOR EACH ROW EXECUTE FUNCTION public.require_active_job_vector_scope();


--
-- Name: job_sources sources_job_scope; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER sources_job_scope BEFORE INSERT OR UPDATE OF job_id ON public.job_sources FOR EACH ROW EXECUTE FUNCTION public.require_active_job_vector_scope();


--
-- Name: source_tags tags_job_scope; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER tags_job_scope BEFORE INSERT OR UPDATE OF job_id ON public.source_tags FOR EACH ROW EXECUTE FUNCTION public.require_active_job_vector_scope();


--
-- Name: knowledge_index trg_knowledge_index_revision_delete; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER trg_knowledge_index_revision_delete BEFORE DELETE ON public.knowledge_index FOR EACH ROW EXECUTE FUNCTION public.knowledge_index_capture_revision();


--
-- Name: knowledge_index trg_knowledge_index_revision_update; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER trg_knowledge_index_revision_update BEFORE UPDATE ON public.knowledge_index FOR EACH ROW WHEN (((old.content IS DISTINCT FROM new.content) OR (old.title IS DISTINCT FROM new.title))) EXECUTE FUNCTION public.knowledge_index_capture_revision();


--
-- Name: citations citations_source_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.citations
    ADD CONSTRAINT citations_source_id_fkey FOREIGN KEY (source_id) REFERENCES public.sources(id);


--
-- Name: memories fk_memories_superseded_by; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.memories
    ADD CONSTRAINT fk_memories_superseded_by FOREIGN KEY (superseded_by) REFERENCES public.memories(id) ON DELETE SET NULL;


--
-- Name: job_sources job_sources_source_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.job_sources
    ADD CONSTRAINT job_sources_source_id_fkey FOREIGN KEY (source_id) REFERENCES public.sources(id);


--
-- Name: knowledge_chunks knowledge_chunks_note_row_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.knowledge_chunks
    ADD CONSTRAINT knowledge_chunks_note_row_fkey FOREIGN KEY (note_row) REFERENCES public.knowledge_index(id) ON DELETE CASCADE;


--
-- Name: knowledge_links knowledge_links_source_note_row_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.knowledge_links
    ADD CONSTRAINT knowledge_links_source_note_row_fkey FOREIGN KEY (source_note_row) REFERENCES public.knowledge_index(id) ON DELETE CASCADE;


--
-- Name: memory_retrieval_messages memory_retrieval_messages_memory_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.memory_retrieval_messages
    ADD CONSTRAINT memory_retrieval_messages_memory_id_fkey FOREIGN KEY (memory_id) REFERENCES public.memories(id) ON DELETE CASCADE;


--
-- Name: source_annotations source_annotations_source_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.source_annotations
    ADD CONSTRAINT source_annotations_source_id_fkey FOREIGN KEY (source_id) REFERENCES public.sources(id) ON DELETE CASCADE;


--
-- Name: source_embeddings source_embeddings_source_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.source_embeddings
    ADD CONSTRAINT source_embeddings_source_id_fkey FOREIGN KEY (source_id) REFERENCES public.sources(id) ON DELETE CASCADE;


--
-- Name: source_tags source_tags_source_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.source_tags
    ADD CONSTRAINT source_tags_source_id_fkey FOREIGN KEY (source_id) REFERENCES public.sources(id) ON DELETE CASCADE;


--
-- PostgreSQL database dump complete
--
