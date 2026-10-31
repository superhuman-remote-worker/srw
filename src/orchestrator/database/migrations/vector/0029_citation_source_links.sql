-- migration:     0029_citation_source_links.sql
-- description:   Source names and anchors per work unit, and source retention
--                (citation engine overhaul WP5). Content stays deduplicated
--                on sources (UNIQUE content_hash); the name, identifier and
--                metadata a job or session registered it under move to its
--                job_sources link, so a later job citing the same bytes no
--                longer shows the first registrant's file name or loses its
--                own cloud anchor. NULL link fields fall back to the source
--                row (links written before this). Deleting a session removes
--                its citation layer (never its memories), and a source that
--                nothing links or cites any more is removed with its last
--                link.
-- depends-on:    0028_citation_reason_and_withdrawal.sql
-- expected:      Instant ALTER; no rewrite, no backfill. Function replace.
-- locks:         Brief ACCESS EXCLUSIVE on job_sources for the ALTER.
-- transactional: yes

BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '5min';

ALTER TABLE public.job_sources
    ADD COLUMN IF NOT EXISTS name       TEXT,
    ADD COLUMN IF NOT EXISTS identifier TEXT,
    ADD COLUMN IF NOT EXISTS metadata   JSONB;

-- Remove the given sources when no link and no citation refers to them.
-- Annotations, tags and embeddings cascade (they are job-scoped and go with
-- their link anyway). Returns how many sources were removed.
CREATE FUNCTION public.remove_unlinked_sources(candidate_ids INTEGER[])
RETURNS INTEGER LANGUAGE plpgsql AS $$
DECLARE
    removed INTEGER;
BEGIN
    DELETE FROM public.sources s
     WHERE s.id = ANY(candidate_ids)
       AND NOT EXISTS (SELECT 1 FROM public.job_sources js WHERE js.source_id = s.id)
       AND NOT EXISTS (SELECT 1 FROM public.citations c WHERE c.source_id = s.id);
    GET DIAGNOSTICS removed = ROW_COUNT;
    RETURN removed;
END;
$$;

-- A permanently deleted session's citation layer. Its scope stays active and
-- its memories stay: sessions share memories with their project, unlike a
-- job's scope, which retire_job_vector_scope fences and empties.
CREATE FUNCTION public.retire_session_citations(requested_thread UUID)
RETURNS VOID LANGUAGE plpgsql AS $$
DECLARE
    linked INTEGER[];
BEGIN
    linked := ARRAY(
        SELECT source_id FROM public.job_sources WHERE job_id = requested_thread
        UNION
        SELECT source_id FROM public.citations WHERE job_id = requested_thread
    );
    DELETE FROM public.citations WHERE job_id = requested_thread;
    DELETE FROM public.source_annotations WHERE job_id = requested_thread;
    DELETE FROM public.source_tags WHERE job_id = requested_thread;
    DELETE FROM public.source_embeddings WHERE job_id = requested_thread;
    DELETE FROM public.job_sources WHERE job_id = requested_thread;
    PERFORM public.remove_unlinked_sources(linked);
END;
$$;

-- Unchanged from 0026 except the last statement: the job's sources that
-- nothing else links or cites are removed with it.
CREATE OR REPLACE FUNCTION public.retire_job_vector_scope(requested_job UUID)
RETURNS VOID LANGUAGE plpgsql AS $$
DECLARE
    linked INTEGER[];
BEGIN
    INSERT INTO public.job_vector_scopes (job_id) VALUES (requested_job)
        ON CONFLICT (job_id) DO NOTHING;
    PERFORM 1 FROM public.job_vector_scopes WHERE job_id=requested_job FOR UPDATE;
    UPDATE public.job_vector_scopes SET retired_at=clock_timestamp()
     WHERE job_id=requested_job AND retired_at IS NULL;

    linked := ARRAY(
        SELECT source_id FROM public.job_sources WHERE job_id = requested_job
        UNION
        SELECT source_id FROM public.citations WHERE job_id = requested_job
    );

    -- One vector transaction commits the fence and the whole existing API
    -- cleanup set. Retrieval messages cascade. Shared sources, project/session
    -- memories, knowledge and audit retention remain outside this job scope.
    DELETE FROM public.memories WHERE job_id=requested_job;
    DELETE FROM public.citations WHERE job_id=requested_job;
    DELETE FROM public.source_annotations WHERE job_id=requested_job;
    DELETE FROM public.source_tags WHERE job_id=requested_job;
    DELETE FROM public.source_embeddings WHERE job_id=requested_job;
    DELETE FROM public.job_sources WHERE job_id=requested_job;
    PERFORM public.remove_unlinked_sources(linked);
END;
$$;

COMMIT;
