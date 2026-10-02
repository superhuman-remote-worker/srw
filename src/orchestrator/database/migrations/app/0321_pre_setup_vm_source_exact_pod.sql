-- Anchor a pre-setup source rotation to the original published Pod life.
-- An abort links source actor/G/T to successor G; only its exact immutable
-- provision intent proves which Pod UID owned that source. VM zero remains
-- independently required by the permanent retirement and purge protocol.
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';
DO $migration$
DECLARE
    definition text;
    old_guard text := $old$       AND NULLIF(abort.agent_pod_uid,'') IS NOT NULL$old$;
    new_guard text := $new$       AND NULLIF(abort.agent_pod_uid,'') IS NOT NULL
       AND EXISTS (SELECT 1 FROM public.thread_agent_pod_provision_intents intent
           WHERE intent.thread_id=abort.thread_id
             AND intent.runtime_generation=abort.runtime_generation
             AND intent.pod_uid=abort.agent_pod_uid
             AND intent.status='published'
             AND intent.provisioner IN ('agent','persistent')
             AND intent.protection_protocol='finalizer_v1'
             AND intent.resolved_at IS NOT NULL AND intent.resolved_at<=abort.released_at)$new$;
BEGIN
    definition := pg_get_functiondef('public.vm_thread_creation_pre_setup_abort_evidence(public.threads,public.vm_creation_retries)'::regprocedure);
    IF (length(definition)-length(replace(definition,old_guard,'')))/length(old_guard) <> 1 THEN
        RAISE EXCEPTION '0321 requires the exact 0320 abort Pod guard';
    END IF;
    EXECUTE replace(definition,old_guard,new_guard);
END;
$migration$;
COMMIT;
