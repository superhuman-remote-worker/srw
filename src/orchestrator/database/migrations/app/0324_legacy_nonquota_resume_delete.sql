-- Deletion-only compatibility for a v1 Resume accepted before retained
-- operations were recorded. The current life must be unexposed and unbound.
-- A settled old End and its exact completed keep/process-zero prove the old
-- source; 0319's unchanged current purge/external/debt guards prove deletion.
-- This does not mint a missing Resume edge or authorize attach/release/Ready.
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

CREATE FUNCTION public.vm_thread_creation_legacy_resume_delete_lineage(
    owner_row public.threads, source public.vm_creation_retries
) RETURNS jsonb LANGUAGE plpgsql STABLE AS $$
DECLARE
    context jsonb := owner_row.runtime_retirement_context;
    soft public.thread_runtime_retirement_outcomes%ROWTYPE;
    keep public.vm_workspace_cleanup_admissions%ROWTYPE;
    zero public.managed_repository_process_zero_receipts%ROWTYPE;
    digest text;
BEGIN
    IF NOT public.valid_vm_thread_nonquota_creation(source)
       OR source.thread_id IS DISTINCT FROM owner_row.id
       OR source.origin IS DISTINCT FROM 'initial' OR source.expected_pvc_uid IS NOT NULL
       OR source.thread_wake_operation_id IS NOT NULL OR source.thread_retained_resume_id IS NOT NULL
       OR source.state IS DISTINCT FROM 'succeeded' OR source.reason IS DISTINCT FROM 'creation_adopted'
       OR source.boot_counted IS DISTINCT FROM true
       OR source.thread_runtime_generation IS NULL OR source.thread_agent_id IS NULL OR source.thread_attach_token IS NULL
       OR source.observed_vm_uid IS NULL OR source.observed_pvc_uid IS NULL
       OR source.thread_runtime_generation IS NOT DISTINCT FROM owner_row.runtime_generation
       OR owner_row.execution_lane IS DISTINCT FROM 'pinned' OR owner_row.status IS DISTINCT FROM 'created'
       OR owner_row.runtime_authority_exposed IS DISTINCT FROM false
       OR owner_row.agent_id IS NOT NULL OR owner_row.runtime_attach_token IS NOT NULL
       OR owner_row.control_admission_agent_id IS NOT NULL
       OR owner_row.runtime_retirement_started_at IS NULL
       OR owner_row.runtime_retirement_token IS NULL OR owner_row.runtime_retirement_authorized_at IS NULL
       OR owner_row.runtime_retirement_permanent IS DISTINCT FROM true
       OR context->>'thread_id' IS DISTINCT FROM owner_row.id::text
       OR context->>'generation' IS DISTINCT FROM owner_row.runtime_generation::text
       OR context->>'entry_status' IS DISTINCT FROM 'created'
       OR context->>'settle_status' IS DISTINCT FROM 'ended'
       OR context->'runtime_authority_exposed' IS DISTINCT FROM 'false'::jsonb
       OR context->'agent_id' IS DISTINCT FROM 'null'::jsonb
       OR context->'runtime_attach_token' IS DISTINCT FROM 'null'::jsonb
       OR context->'control_admission_agent_id' IS DISTINCT FROM 'null'::jsonb
       OR COALESCE(context->'agent_pod','null'::jsonb) NOT IN ('null'::jsonb,'{}'::jsonb)
       OR COALESCE(context->'workspace_binding','null'::jsonb) NOT IN ('null'::jsonb,'{}'::jsonb)
       OR COALESCE(context->'initial_vm_creation','null'::jsonb)<>'null'::jsonb
       OR EXISTS (SELECT 1 FROM public.agents a WHERE a.thread_id=owner_row.id)
       OR EXISTS (SELECT 1 FROM public.vm_thread_retained_resumes op WHERE op.thread_id=owner_row.id)
       OR (SELECT count(*) FROM public.vm_creation_retries r WHERE r.owner_kind='thread' AND r.thread_id=owner_row.id)<>1
       OR NOT EXISTS (SELECT 1 FROM public.vm_workspace_cleanup_admissions c
           WHERE c.id=source.creation_admission_id AND c.owner_kind='thread' AND c.owner_id=owner_row.id
             AND c.source='controller_vm_create' AND c.completed_at IS NOT NULL AND c.outcome='adopted') THEN
        RETURN NULL;
    END IF;
    SELECT * INTO soft FROM public.thread_runtime_retirement_outcomes o
     WHERE o.thread_id=owner_row.id AND o.runtime_generation=source.thread_runtime_generation
       AND o.agent_id=source.thread_agent_id AND o.runtime_attach_token=source.thread_attach_token
       AND NOT o.permanent AND o.outcome='settled' AND o.disposition='ended'
       AND o.settled_at<owner_row.runtime_retirement_started_at
     ORDER BY o.settled_at DESC LIMIT 1;
    IF NOT FOUND THEN RETURN NULL; END IF;
    digest := 'sha256:'||encode(sha256(convert_to(format(
        '{"owner_id":"%s","owner_kind":"thread","provision_generation":"%s","purge_disk":false,"pvc_uid":"%s","resource":"vm_workspace","source":"pinned_thread_retirement","vm_uid":"%s"}',
        owner_row.id,source.provision_generation,source.observed_pvc_uid,source.observed_vm_uid
    ),'UTF8')),'hex');
    SELECT * INTO keep FROM public.vm_workspace_cleanup_admissions c
     WHERE c.owner_kind='thread' AND c.owner_id=owner_row.id AND c.pvc_uid=source.observed_pvc_uid
       AND c.source='pinned_thread_retirement' AND c.intent_digest=digest
       AND c.parent_admission_id IS NULL AND c.completed_at IS NOT NULL AND c.outcome='completed'
       AND c.completed_at<=soft.settled_at AND source.resolved_at<=c.admitted_at
     ORDER BY c.completed_at DESC LIMIT 1;
    IF NOT FOUND THEN RETURN NULL; END IF;
    SELECT * INTO zero FROM public.managed_repository_process_zero_receipts p
     WHERE p.owner_kind='thread' AND p.owner_id=owner_row.id AND p.scope='vm' AND p.provisioner='vm'
       AND p.runtime_incarnation=source.provision_generation::text AND p.observed_at<=soft.settled_at;
    IF NOT FOUND THEN RETURN NULL; END IF;
    RETURN jsonb_build_object('kind','legacy_nonquota_unexposed_resume_delete',
        'soft_end',to_jsonb(soft),'keep_admission',to_jsonb(keep),'process_zero',to_jsonb(zero));
END;
$$;

-- Retain all existing common capture, permanent retirement, exact purge,
-- external-cleanup and all-source/effect/quota/admission guards verbatim.
DO $migration$
DECLARE
    definition text;
    generation_guard text := $old$       OR (source.thread_runtime_generation IS DISTINCT FROM owner_row.runtime_generation
           AND public.vm_thread_creation_pre_setup_abort_path_evidence(owner_row,source) IS NULL)$old$;
    actor_guard text := $old$       AND public.vm_thread_creation_pre_setup_abort_path_evidence(owner_row,source) IS NULL AND NOT ($old$;
    evidence_clause text := $old$        'pre_setup_abort_path',public.vm_thread_creation_pre_setup_abort_path_evidence(owner_row,source),$old$;
BEGIN
    definition := pg_get_functiondef('public.vm_thread_creation_nonquota_delete_evidence(public.threads)'::regprocedure);
    IF (length(definition)-length(replace(definition,generation_guard,'')))/length(generation_guard)<>1
       OR (length(definition)-length(replace(definition,actor_guard,'')))/length(actor_guard)<>1
       OR (length(definition)-length(replace(definition,evidence_clause,'')))/length(evidence_clause)<>1 THEN
        RAISE EXCEPTION '0324 requires exact installed nonquota generation, actor and evidence clauses';
    END IF;
    definition := replace(definition,generation_guard,
        replace(generation_guard,' IS NULL)',' IS NULL AND public.vm_thread_creation_legacy_resume_delete_lineage(owner_row,source) IS NULL)'));
    definition := replace(definition,actor_guard,
        replace(actor_guard,' IS NULL AND NOT (',' IS NULL AND public.vm_thread_creation_legacy_resume_delete_lineage(owner_row,source) IS NULL AND NOT ('));
    definition := replace(definition,evidence_clause,evidence_clause||E'\n'||
        $new$        'legacy_resume_delete',public.vm_thread_creation_legacy_resume_delete_lineage(owner_row,source),$new$);
    EXECUTE definition;
END;
$migration$;
COMMIT;
