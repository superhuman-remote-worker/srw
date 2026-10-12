-- A confirmed pre-setup actor release rotates G without rewriting a successful
-- VM creation source. Admit only its exact immutable abort edge. This proves
-- source lineage, not VM process zero: 0319's independent physical identity,
-- retirement, completed purge and zero-debt predicates remain required.
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

CREATE FUNCTION public.vm_thread_creation_pre_setup_abort_evidence(
    owner_row public.threads, source_row public.vm_creation_retries
) RETURNS jsonb LANGUAGE plpgsql STABLE AS $$
DECLARE evidence jsonb;
BEGIN
    IF source_row.owner_kind IS DISTINCT FROM 'thread'
       OR source_row.thread_id IS DISTINCT FROM owner_row.id
       OR source_row.state IS DISTINCT FROM 'succeeded'
       OR source_row.reason IS DISTINCT FROM 'creation_adopted'
       OR source_row.thread_runtime_generation IS NULL
       OR source_row.thread_runtime_generation IS NOT DISTINCT FROM owner_row.runtime_generation
       OR source_row.thread_agent_id IS NULL OR source_row.thread_attach_token IS NULL THEN
        RETURN NULL;
    END IF;
    SELECT to_jsonb(abort) INTO evidence
      FROM public.thread_runtime_attach_abort_outcomes abort
      JOIN public.vm_workspace_cleanup_admissions creation
        ON creation.id=source_row.creation_admission_id
       AND creation.owner_kind='thread' AND creation.owner_id=owner_row.id
       AND creation.source='controller_vm_create' AND creation.outcome='adopted'
       AND creation.completed_at IS NOT NULL AND creation.completed_at<=abort.released_at
     WHERE abort.thread_id=owner_row.id
       AND abort.runtime_generation=source_row.thread_runtime_generation
       AND abort.runtime_attach_token=source_row.thread_attach_token
       AND abort.agent_id=source_row.thread_agent_id
       AND NULLIF(abort.agent_pod_uid,'') IS NOT NULL
       AND abort.successor_generation=owner_row.runtime_generation
       AND abort.release_kind='process_zero'
       AND abort.quiescence_protocol='agent_attach_not_started_v1'
       AND ((abort.workspace_generation IS NULL AND abort.workspace_runtime_incarnation IS NULL)
            OR (abort.workspace_generation=source_row.provision_generation
                AND abort.workspace_runtime_incarnation::text=source_row.observed_vm_uid::text));
    RETURN evidence;
END;
$$;

DO $migration$
DECLARE
    definition text;
    old_generation text := $old$       OR source.thread_runtime_generation IS DISTINCT FROM owner_row.runtime_generation$old$;
    new_generation text := $new$       OR (source.thread_runtime_generation IS DISTINCT FROM owner_row.runtime_generation
           AND public.vm_thread_creation_pre_setup_abort_evidence(owner_row,source) IS NULL)$new$;
    old_actor text := $old$       ROW(owner_row.agent_id,owner_row.runtime_attach_token) AND NOT ($old$;
    new_actor text := $new$       ROW(owner_row.agent_id,owner_row.runtime_attach_token)
       AND public.vm_thread_creation_pre_setup_abort_evidence(owner_row,source) IS NULL AND NOT ($new$;
    old_evidence text := $old$        'source_evidence',public.vm_thread_retained_source_snapshot(source),$old$;
    new_evidence text := $new$        'source_evidence',public.vm_thread_retained_source_snapshot(source),
        'pre_setup_abort',public.vm_thread_creation_pre_setup_abort_evidence(owner_row,source),$new$;
BEGIN
    definition := pg_get_functiondef('public.vm_thread_creation_nonquota_delete_evidence(public.threads)'::regprocedure);
    IF (length(definition)-length(replace(definition,old_generation,'')))/length(old_generation) <> 1
       OR (length(definition)-length(replace(definition,old_actor,'')))/length(old_actor) <> 1
       OR (length(definition)-length(replace(definition,old_evidence,'')))/length(old_evidence) <> 1 THEN
        RAISE EXCEPTION '0320 requires the exact 0319 generation, actor and source evidence predicates';
    END IF;
    EXECUTE replace(replace(replace(definition,old_generation,new_generation),old_actor,new_actor),
                    old_evidence,new_evidence);
END;
$migration$;
COMMIT;
