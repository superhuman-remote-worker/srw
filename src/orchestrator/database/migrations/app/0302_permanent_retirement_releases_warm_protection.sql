-- A permanent retirement settles the warm-pool protection it captured.
--
-- A warm-pool Pod bound at attach is protected by an exact `bound` row in
-- thread_agent_warm_binding_protections (0200). Soft settlement moves that
-- row to `releasing` while the thread row still exists (its agent cleared),
-- then the finalizer release settles it. A permanent retirement of a live
-- warm-bound life stopped the exact Pod and deleted the thread, but the row
-- stayed `bound` for good: `bound` only leaves through `releasing`, and the
-- reciprocity check accepted `releasing` only against a surviving thread row,
-- while the thread-delete authority requires that row to still name the
-- agent until the DELETE. No ordering could satisfy both.
--
-- Accept `releasing` for an absent thread only on the append-only proof the
-- permanent retirement writes in the same transaction: the exact
-- thread/generation/agent/attach-token outcome with permanent, ended,
-- deleted. The actor must still be detached and draining. Every other branch
-- is unchanged, and so are the transition matrix, the release receipts and
-- the `released` checks.
CREATE OR REPLACE FUNCTION public.validate_thread_agent_warm_binding_protection()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    thread_row public.threads%ROWTYPE;
    agent_row public.agents%ROWTYPE;
    marker jsonb;
BEGIN
    SELECT * INTO thread_row FROM public.threads WHERE id = NEW.thread_id;
    SELECT * INTO agent_row FROM public.agents WHERE id = NEW.agent_id;
    marker := COALESCE(thread_row.metadata->'agent_pod', '{}'::jsonb);

    IF NEW.status IN ('planned', 'protecting', 'protected')
       AND NEW.source = 'attach' THEN
        IF thread_row.id IS NULL
           OR thread_row.runtime_generation IS DISTINCT FROM NEW.runtime_generation
           OR thread_row.runtime_retirement_token IS NOT NULL
           OR thread_row.agent_id IS NOT NULL
           OR thread_row.runtime_attach_token IS NOT NULL
           OR agent_row.id IS NULL
           OR agent_row.thread_id IS NOT NULL
           OR agent_row.current_job_id IS NOT NULL
           OR agent_row.status::text <> 'draining' THEN
            RAISE EXCEPTION 'warm attach plan is not reciprocal'
                USING ERRCODE = '23514',
                      CONSTRAINT = 'thread_agent_warm_binding_reciprocity';
        END IF;
    ELSIF NEW.status IN ('planned', 'protecting', 'protected')
          AND NEW.source = 'legacy_binding' THEN
        IF thread_row.id IS NULL
           OR thread_row.runtime_generation IS DISTINCT FROM NEW.runtime_generation
           OR thread_row.runtime_retirement_token IS NOT NULL
           OR thread_row.agent_id IS DISTINCT FROM NEW.agent_id
           OR thread_row.runtime_attach_token
                IS DISTINCT FROM NEW.runtime_attach_token
           OR agent_row.id IS NULL
           OR agent_row.thread_id IS DISTINCT FROM NEW.thread_id
           OR agent_row.status::text <> 'session' THEN
            RAISE EXCEPTION 'legacy warm binding plan is not reciprocal'
                USING ERRCODE = '23514',
                      CONSTRAINT = 'thread_agent_warm_binding_reciprocity';
        END IF;
    ELSIF NEW.status = 'bound' THEN
        IF thread_row.id IS NULL
           OR thread_row.runtime_generation IS DISTINCT FROM NEW.runtime_generation
           OR thread_row.agent_id IS DISTINCT FROM NEW.agent_id
           OR thread_row.runtime_attach_token
                IS DISTINCT FROM NEW.runtime_attach_token
           OR agent_row.id IS NULL
           OR agent_row.thread_id IS DISTINCT FROM NEW.thread_id
           OR agent_row.status::text <> 'session'
           OR marker->>'warm_binding_protection'
                IS DISTINCT FROM NEW.protection_id::text
           OR marker->>'pod_name' IS DISTINCT FROM NEW.pod_name
           OR marker->>'pod_uid' IS DISTINCT FROM NEW.pod_uid
           OR marker->>'runtime_generation'
                IS DISTINCT FROM NEW.runtime_generation::text
           OR marker->>'namespace' IS DISTINCT FROM NEW.namespace
           OR marker->>'protection_protocol' <> 'finalizer_v1' THEN
            RAISE EXCEPTION 'warm binding publication is not reciprocal'
                USING ERRCODE = '23514',
                      CONSTRAINT = 'thread_agent_warm_binding_reciprocity';
        END IF;
    ELSIF NEW.status = 'releasing' THEN
        -- Soft settlement keeps the thread row and clears its agent. A
        -- permanent retirement deletes the row in the same transaction and
        -- leaves its append-only outcome for exactly this life instead.
        IF agent_row.id IS NULL
           OR agent_row.thread_id IS NOT NULL
           OR agent_row.status::text <> 'draining'
           OR (
                thread_row.id IS NOT NULL
                AND thread_row.agent_id IS NOT DISTINCT FROM NEW.agent_id
           )
           OR (
                thread_row.id IS NULL
                AND NOT EXISTS (
                    SELECT 1
                      FROM public.thread_runtime_retirement_outcomes outcome
                     WHERE outcome.thread_id = NEW.thread_id
                       AND outcome.runtime_generation = NEW.runtime_generation
                       AND outcome.agent_id = NEW.agent_id
                       AND outcome.runtime_attach_token
                           = NEW.runtime_attach_token
                       AND outcome.permanent
                       AND outcome.disposition = 'ended'
                       AND outcome.outcome = 'deleted'
                )
           ) THEN
            RAISE EXCEPTION 'warm binding release is not fenced'
                USING ERRCODE = '23514',
                      CONSTRAINT = 'thread_agent_warm_binding_reciprocity';
        END IF;
    ELSIF NEW.status IN ('released', 'aborted') THEN
        IF thread_row.id IS NOT NULL
           AND thread_row.agent_id IS NOT DISTINCT FROM NEW.agent_id THEN
            RAISE EXCEPTION 'released warm Pod remains thread authority'
                USING ERRCODE = '23514',
                      CONSTRAINT = 'thread_agent_warm_binding_reciprocity';
        END IF;
        IF agent_row.id IS NOT NULL AND (
            agent_row.thread_id IS NOT NULL
            OR agent_row.status::text NOT IN ('ready', 'offline')
        ) THEN
            RAISE EXCEPTION 'released warm Pod remains reserved'
                USING ERRCODE = '23514',
                      CONSTRAINT = 'thread_agent_warm_binding_reciprocity';
        END IF;
    END IF;
    RETURN NULL;
END;
$$;
