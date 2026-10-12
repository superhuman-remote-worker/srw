-- Repeated confirmed pre-setup releases preserve a successful creation source.
-- This deletion-only path proves actor lineage, never VM or disk process zero.
-- Keep 0320/0321's direct-successor readiness proof unchanged and retain every
-- authenticated capture, retirement, exact purge and all-source debt guard.
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

CREATE FUNCTION public.vm_thread_creation_pre_setup_abort_path_evidence(
    owner_row public.threads, source_row public.vm_creation_retries
) RETURNS jsonb LANGUAGE plpgsql STABLE AS $$
DECLARE
    cursor_generation uuid := source_row.thread_runtime_generation;
    visited uuid[] := ARRAY[]::uuid[];
    edge public.thread_runtime_attach_abort_outcomes%ROWTYPE;
    edge_count integer;
    hops integer := 0;
    previous_release timestamptz;
    path jsonb := '[]'::jsonb;
BEGIN
    IF source_row.owner_kind IS DISTINCT FROM 'thread'
       OR source_row.thread_id IS DISTINCT FROM owner_row.id
       OR source_row.state IS DISTINCT FROM 'succeeded'
       OR source_row.reason IS DISTINCT FROM 'creation_adopted'
       OR source_row.thread_runtime_generation IS NULL
       OR owner_row.runtime_generation IS NULL
       OR owner_row.runtime_retirement_started_at IS NULL
       OR source_row.thread_runtime_generation IS NOT DISTINCT FROM owner_row.runtime_generation
       OR source_row.thread_agent_id IS NULL OR source_row.thread_attach_token IS NULL THEN
        RETURN NULL;
    END IF;
    SELECT creation.completed_at INTO previous_release
      FROM public.vm_workspace_cleanup_admissions creation
     WHERE creation.id=source_row.creation_admission_id
       AND creation.owner_kind='thread' AND creation.owner_id=owner_row.id
       AND creation.source='controller_vm_create' AND creation.outcome='adopted'
       AND creation.completed_at IS NOT NULL;
    IF NOT FOUND THEN RETURN NULL; END IF;
    LOOP
        IF hops>=4096 OR cursor_generation=ANY(visited) THEN RETURN NULL; END IF;
        visited := array_append(visited,cursor_generation);
        SELECT count(*) INTO edge_count FROM (
            SELECT 1 FROM public.thread_runtime_attach_abort_outcomes abort
             WHERE abort.thread_id=owner_row.id AND abort.runtime_generation=cursor_generation
             LIMIT 2
        ) candidates;
        IF edge_count<>1 THEN RETURN NULL; END IF;
        SELECT * INTO edge FROM public.thread_runtime_attach_abort_outcomes
         WHERE thread_id=owner_row.id AND runtime_generation=cursor_generation;
        IF edge.agent_id IS NULL OR edge.runtime_attach_token IS NULL
           OR NULLIF(btrim(edge.agent_pod_uid),'') IS NULL
           OR edge.successor_generation IS NULL OR edge.successor_generation=ANY(visited)
           OR edge.release_kind IS DISTINCT FROM 'process_zero'
           OR edge.quiescence_protocol IS DISTINCT FROM 'agent_attach_not_started_v1'
           OR edge.released_at IS NULL OR edge.released_at<previous_release
           OR edge.released_at>owner_row.runtime_retirement_started_at
           OR (hops=0 AND (
               edge.agent_id IS DISTINCT FROM source_row.thread_agent_id
               OR edge.runtime_attach_token IS DISTINCT FROM source_row.thread_attach_token))
           OR ((edge.workspace_generation IS NULL AND edge.workspace_runtime_incarnation IS NULL)
               OR (edge.workspace_generation=source_row.provision_generation
                   AND edge.workspace_runtime_incarnation::text=source_row.observed_vm_uid::text)) IS NOT TRUE
           OR NOT EXISTS (SELECT 1 FROM public.thread_agent_pod_provision_intents intent
               WHERE intent.thread_id=edge.thread_id AND intent.runtime_generation=edge.runtime_generation
                 AND intent.pod_uid=edge.agent_pod_uid AND intent.status='published'
                 AND intent.provisioner IN ('agent','persistent')
                 AND intent.protection_protocol='finalizer_v1'
                 AND intent.resolved_at IS NOT NULL AND intent.resolved_at<=edge.released_at) THEN
            RETURN NULL;
        END IF;
        path := path || jsonb_build_array(to_jsonb(edge));
        previous_release := edge.released_at;
        cursor_generation := edge.successor_generation;
        hops := hops+1;
        IF cursor_generation=owner_row.runtime_generation THEN RETURN path; END IF;
    END LOOP;
END;
$$;

DO $migration$
DECLARE
    definition text;
    old_guard text := 'public.vm_thread_creation_pre_setup_abort_evidence(owner_row,source) IS NULL';
    new_guard text := 'public.vm_thread_creation_pre_setup_abort_path_evidence(owner_row,source) IS NULL';
    old_evidence text := $old$        'pre_setup_abort',public.vm_thread_creation_pre_setup_abort_evidence(owner_row,source),$old$;
    new_evidence text := $new$        'pre_setup_abort',public.vm_thread_creation_pre_setup_abort_evidence(owner_row,source),
        'pre_setup_abort_path',public.vm_thread_creation_pre_setup_abort_path_evidence(owner_row,source),$new$;
BEGIN
    definition := pg_get_functiondef('public.vm_thread_creation_nonquota_delete_evidence(public.threads)'::regprocedure);
    IF (length(definition)-length(replace(definition,old_guard,'')))/length(old_guard) <> 2
       OR (length(definition)-length(replace(definition,old_evidence,'')))/length(old_evidence) <> 1 THEN
        RAISE EXCEPTION '0322 requires the exact 0320 non-quota source/actor guards and evidence';
    END IF;
    EXECUTE replace(replace(definition,old_guard,new_guard),old_evidence,new_evidence);
END;
$migration$;
COMMIT;
