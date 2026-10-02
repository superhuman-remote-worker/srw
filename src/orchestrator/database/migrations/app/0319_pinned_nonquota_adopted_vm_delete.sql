-- Preserve the existing failed-initial, quota-backed compute and retained-disk
-- proofs. Successful v1 creation has no quota ledger: require its own captured
-- physical identity and exact completed purge instead of inventing a reservation.
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

CREATE FUNCTION public.vm_thread_creation_nonquota_delete_evidence(owner_row public.threads)
RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE
    context jsonb := owner_row.runtime_retirement_context;
    vm jsonb := context->'vm';
    source public.vm_creation_retries%ROWTYPE;
    purge public.vm_workspace_cleanup_admissions%ROWTYPE;
    request_id uuid;
    digest text;
BEGIN
    IF owner_row.execution_lane IS DISTINCT FROM 'pinned'
       OR owner_row.runtime_retirement_token IS NULL
       OR owner_row.runtime_retirement_authorized_at IS NULL
       OR owner_row.runtime_retirement_permanent IS DISTINCT FROM true
       OR context->>'thread_id' IS DISTINCT FROM owner_row.id::text
       OR context->>'generation' IS DISTINCT FROM owner_row.runtime_generation::text
       OR context->>'settle_status' IS DISTINCT FROM 'ended'
       OR context->>'workspace_backend' IS DISTINCT FROM 'vm'
       OR jsonb_typeof(vm) IS DISTINCT FROM 'object'
       OR vm->'identity_authenticated' IS DISTINCT FROM 'true'::jsonb
       OR vm->>'identity_provision_generation' IS DISTINCT FROM vm->>'provision_generation'
       OR NULLIF(vm->>'vm_uid','') IS NULL
       OR NULLIF(vm->>'rootdisk_pvc_uid','') IS NULL
       OR owner_row.runtime_retirement_external_cleanup IS NULL
       OR owner_row.runtime_retirement_external_cleanup IS DISTINCT FROM
           public.pinned_retirement_external_cleanup_expected(context,
               owner_row.runtime_generation,owner_row.runtime_retirement_token)
       OR NOT public.pinned_retirement_external_resources_absent(owner_row.id,owner_row.metadata)
       OR NOT EXISTS (SELECT 1 FROM public.thread_runtime_retirement_outcomes o
           WHERE o.thread_id=owner_row.id AND o.runtime_generation=owner_row.runtime_generation
             AND o.retirement_token=owner_row.runtime_retirement_token
             AND o.permanent AND o.disposition='ended' AND o.outcome='deleted'
             AND o.agent_id IS NOT DISTINCT FROM owner_row.agent_id
             AND o.runtime_attach_token IS NOT DISTINCT FROM owner_row.runtime_attach_token) THEN
        RETURN NULL;
    END IF;

    PERFORM 1 FROM public.vm_thread_creation_owners
        WHERE thread_id=owner_row.id AND live_thread_id=owner_row.id FOR UPDATE;
    IF NOT FOUND THEN RETURN NULL; END IF;
    -- Lock every source before child ledgers, as live creation does. A retired
    -- audit owner must never hide debt belonging to another request/generation.
    PERFORM r.request_id FROM public.vm_creation_retries r
        WHERE r.owner_kind='thread' AND r.thread_id=owner_row.id
        ORDER BY r.request_id FOR UPDATE;
    FOR request_id IN SELECT r.request_id FROM public.vm_creation_retries r
        WHERE r.owner_kind='thread' AND r.thread_id=owner_row.id ORDER BY r.request_id
    LOOP
        PERFORM public.lock_vm_thread_creation_terminal_ledger(owner_row.id,request_id);
    END LOOP;
    SELECT * INTO source FROM public.vm_creation_retries r
        WHERE r.owner_kind='thread' AND r.thread_id=owner_row.id
          AND r.provision_generation::text=vm->>'provision_generation';
    IF NOT FOUND
       OR source.state IS DISTINCT FROM 'succeeded'
       OR source.reason IS DISTINCT FROM 'creation_adopted'
       OR source.controller_configuration->>'version' IS DISTINCT FROM '1'
       OR COALESCE(source.controller_configuration->'resource_admission','null'::jsonb)<>'null'::jsonb
       OR source.observed_vm_uid::text IS DISTINCT FROM vm->>'vm_uid'
       OR source.observed_pvc_uid::text IS DISTINCT FROM vm->>'rootdisk_pvc_uid'
       OR source.thread_runtime_generation IS DISTINCT FROM owner_row.runtime_generation
       OR EXISTS (SELECT 1 FROM public.vm_resource_reservations r WHERE r.request_id=source.request_id)
       OR NOT EXISTS (SELECT 1 FROM public.vm_workspace_cleanup_admissions c
           WHERE c.id=source.creation_admission_id AND c.owner_kind='thread'
             AND c.owner_id=owner_row.id AND c.source='controller_vm_create'
             AND c.completed_at IS NOT NULL AND c.outcome='adopted') THEN
        RETURN NULL;
    END IF;
    IF ROW(source.thread_agent_id,source.thread_attach_token) IS DISTINCT FROM
       ROW(owner_row.agent_id,owner_row.runtime_attach_token) AND NOT (
        owner_row.status='ended' AND owner_row.agent_id IS NULL
        AND owner_row.runtime_attach_token IS NULL AND owner_row.control_admission_agent_id IS NULL
        AND NOT EXISTS (SELECT 1 FROM public.agents a WHERE a.thread_id=owner_row.id)
        AND EXISTS (SELECT 1 FROM public.thread_runtime_retirement_outcomes o
            WHERE o.thread_id=owner_row.id AND o.runtime_generation=owner_row.runtime_generation
              AND o.disposition='ended' AND o.outcome='settled' AND NOT o.permanent
              AND o.agent_id IS NOT DISTINCT FROM source.thread_agent_id
              AND o.runtime_attach_token IS NOT DISTINCT FROM source.thread_attach_token)
    ) THEN RETURN NULL; END IF;

    digest := 'sha256:' || encode(sha256(convert_to(format(
        '{"owner_id":"%s","owner_kind":"thread","provision_generation":"%s","purge_disk":true,"pvc_uid":"%s","resource":"vm_workspace","source":"pinned_thread_retirement","vm_uid":"%s"}',
        owner_row.id,source.provision_generation,source.observed_pvc_uid,source.observed_vm_uid
    ),'UTF8')),'hex');
    SELECT * INTO purge FROM public.vm_workspace_cleanup_admissions c
        WHERE c.owner_kind='thread' AND c.owner_id=owner_row.id
          AND c.pvc_uid=source.observed_pvc_uid AND c.source='pinned_thread_retirement'
          AND c.intent_digest=digest AND c.completed_at IS NOT NULL AND c.outcome='completed';
    IF NOT FOUND
       OR EXISTS (SELECT 1 FROM public.vm_creation_retries r
           WHERE r.owner_kind='thread' AND r.thread_id=owner_row.id AND r.state<>'succeeded'
             AND NOT EXISTS (SELECT 1 FROM public.vm_thread_retained_resume_terminals t
                 WHERE t.operation_id=r.thread_retained_resume_id AND t.source_request_id=r.request_id
                   AND t.source_terminal_evidence=public.vm_thread_retained_source_snapshot(r)))
       OR EXISTS (SELECT 1 FROM public.vm_creation_effects e
           JOIN public.vm_creation_retries r USING(request_id)
           WHERE r.owner_kind='thread' AND r.thread_id=owner_row.id AND e.state='issued')
       OR EXISTS (SELECT 1 FROM public.vm_resource_reservations v
           JOIN public.vm_creation_retries r USING(request_id)
           WHERE r.owner_kind='thread' AND r.thread_id=owner_row.id AND v.state<>'released')
       OR EXISTS (SELECT 1 FROM public.vm_resource_waiters w
           WHERE w.owner_kind='thread' AND w.thread_id=owner_row.id AND w.state NOT IN ('released','cancelled'))
       OR EXISTS (SELECT 1 FROM public.vm_workspace_cleanup_admissions c
           WHERE c.owner_kind='thread' AND c.owner_id=owner_row.id AND c.completed_at IS NULL) THEN
        RETURN NULL;
    END IF;
    RETURN jsonb_build_object('version',4,'kind','nonquota_adopted_vm_cleanup',
        'request_id',source.request_id,'cleanup_admission_id',purge.id,
        'runtime_generation',owner_row.runtime_generation,'retirement_token',owner_row.runtime_retirement_token,
        'source_evidence',public.vm_thread_retained_source_snapshot(source),
        'purge_admission',to_jsonb(purge),
        'local_quiescence',owner_row.runtime_retirement_local_quiescence,
        'external_cleanup',owner_row.runtime_retirement_external_cleanup);
END;
$$;

ALTER FUNCTION public.vm_thread_creation_delete_evidence(public.threads)
RENAME TO vm_thread_creation_quota_delete_evidence;
CREATE FUNCTION public.vm_thread_creation_delete_evidence(owner_row public.threads)
RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE evidence jsonb;
BEGIN
    evidence := public.vm_thread_creation_quota_delete_evidence(owner_row);
    IF evidence IS NOT NULL THEN RETURN evidence; END IF;
    RETURN public.vm_thread_creation_nonquota_delete_evidence(owner_row);
END;
$$;
COMMIT;
