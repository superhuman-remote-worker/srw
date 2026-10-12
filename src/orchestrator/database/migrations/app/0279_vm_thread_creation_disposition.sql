-- migration: 0279_vm_thread_creation_disposition.sql
-- description: Bind partial thread creation cleanup and release to captured End.
-- depends-on: 0278_vm_resource_thread_runtime.sql
-- transactional: yes
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

-- Preserve the 0269 Job protocol while accepting a retired, real thread
-- source. The thread's frozen End tuple and original creation admission are
-- checked again when evidence is recorded, the parent closes, and End exits.
CREATE FUNCTION public.valid_vm_creation_thread_disposition_identity(
    retry public.vm_creation_retries, require_captured boolean
) RETURNS boolean LANGUAGE sql STABLE AS $$
    SELECT (retry.owner_kind='thread' AND retry.job_id IS NULL
       AND retry.thread_id IS NOT NULL
       AND retry.canonical_request->>'entity_type'='thread'
       AND retry.canonical_request->>'job_id'=retry.thread_id::text
       AND retry.canonical_request->>'provision_generation'=retry.provision_generation::text
       AND COALESCE(retry.canonical_request->'workspace_storage','null'::jsonb)='null'::jsonb
       AND retry.cancellation_disposition->>'owner_kind'='thread'
       AND retry.cancellation_disposition->>'thread_id'=retry.thread_id::text
       AND retry.cancellation_disposition->>'job_id'=retry.thread_id::text
       AND retry.cancellation_disposition->>'thread_runtime_generation'=retry.thread_runtime_generation::text
       AND retry.cancellation_disposition->>'thread_agent_id' IS NOT DISTINCT FROM retry.thread_agent_id::text
       AND retry.cancellation_disposition->>'thread_attach_token' IS NOT DISTINCT FROM retry.thread_attach_token::text
       AND retry.cancellation_disposition->>'thread_wake_operation_id' IS NOT DISTINCT FROM retry.thread_wake_operation_id::text
       AND retry.cancellation_disposition->>'disk_policy'='purge_new_thread_disk'
       AND retry.cancellation_disposition->'workspace_storage'='null'::jsonb
       AND retry.cancellation_disposition->'workspace_instance_id'='null'::jsonb
       AND EXISTS (SELECT 1 FROM public.threads t
           WHERE t.id=retry.thread_id AND t.execution_lane='pinned'
             AND t.runtime_generation=retry.thread_runtime_generation
             AND t.runtime_retirement_token IS NOT NULL
             AND t.runtime_retirement_authorized_at IS NOT NULL
             AND t.runtime_retirement_context->>'settle_status'='ended'
             AND retry.cancellation_disposition->>'retirement_token'=t.runtime_retirement_token::text
             AND t.runtime_retirement_context->'vm_creation_source'->>'request_id'=retry.request_id::text
             AND t.runtime_retirement_context->'vm_creation_source'->>'provision_generation'=retry.provision_generation::text
             AND t.runtime_retirement_context->'vm_creation_source'->>'thread_runtime_generation'=retry.thread_runtime_generation::text
             AND t.runtime_retirement_context->'vm_creation_source'->>'request_digest'=retry.request_digest
             AND t.runtime_retirement_context->'vm_creation_source'->>'controller_configuration_digest'=retry.controller_configuration_digest
             AND t.runtime_retirement_context->'vm_creation_source'->>'thread_agent_id' IS NOT DISTINCT FROM retry.thread_agent_id::text
             AND t.runtime_retirement_context->'vm_creation_source'->>'thread_attach_token' IS NOT DISTINCT FROM retry.thread_attach_token::text
             AND t.runtime_retirement_context->'vm_creation_source'->'captured_vm'->>'creation_request_id'=retry.request_id::text
             AND t.runtime_retirement_context->'vm_creation_source'->'captured_vm'->>'provision_generation'=retry.provision_generation::text
             AND COALESCE(t.runtime_retirement_context->'vm_creation_source'->'captured_vm'->'vm_uid','null'::jsonb)='null'::jsonb
             AND (COALESCE(t.runtime_retirement_context->'vm_creation_source'->'captured_vm'->'rootdisk_pvc_uid','null'::jsonb)='null'::jsonb
                  OR t.runtime_retirement_context->'vm_creation_source'->'captured_vm'->>'rootdisk_pvc_uid'=retry.observed_pvc_uid::text)
             AND (CASE WHEN require_captured THEN
                 t.metadata->'vm'=t.runtime_retirement_context->'vm_creation_source'->'captured_vm'
                 ELSE t.metadata->'vm'=t.runtime_retirement_context->'vm_creation_source'->'captured_vm'
                      OR NOT t.metadata ? 'vm' END))) IS TRUE;
$$;

CREATE OR REPLACE FUNCTION public.valid_vm_creation_source_authority(retry public.vm_creation_retries)
RETURNS boolean LANGUAGE plpgsql STABLE AS $$
DECLARE
    disposition jsonb := retry.cancellation_disposition;
    plan jsonb := retry.cancellation_progress->'source';
    completion jsonb := retry.cancellation_completion->'source';
    source jsonb := retry.cancellation_progress->'source'->'source';
    frozen_source jsonb := retry.cancellation_disposition->'source';
    root jsonb := retry.cancellation_disposition->'objects'->'rootdisk';
    root_completion jsonb := retry.cancellation_completion->'rootdisk';
    target jsonb := retry.cancellation_progress->'source'->'target';
    resolution text := retry.cancellation_disposition->>'source_resolution';
    expected_name text;
BEGIN
    IF NOT (public.valid_vm_creation_source_completion(plan,completion)
        AND resolution IN ('required','unknown','not_required')
        AND plan->>'request_id'=retry.request_id::text
        AND plan->>'job_id'=COALESCE(retry.job_id,retry.thread_id)::text
        AND plan->>'provision_generation'=retry.provision_generation::text
        AND plan->>'request_digest'=retry.request_digest
        AND plan->>'controller_configuration_digest'=retry.controller_configuration_digest
        AND plan->'disposition_id'=disposition->'disposition_id'
        AND (frozen_source='null'::jsonb OR source=frozen_source)
        AND (resolution<>'required' OR
            (frozen_source<>'null'::jsonb AND frozen_source->>'kind' IN ('golden','prepared')))
        AND (resolution<>'unknown' OR
            (frozen_source='null'::jsonb AND source<>'null'::jsonb))
        AND (resolution<>'not_required' OR
            (source=frozen_source AND (frozen_source='null'::jsonb OR
                frozen_source->>'kind' IN ('registry','retained'))))
    ) IS TRUE THEN RETURN false; END IF;
    -- An unresolved source may have published a hold before the first create
    -- effect. Only the frozen no-source decision can prove no disposition is
    -- needed; a subsequently supplied source must still prove its own result.
    IF completion->>'outcome'='not_required' AND NOT (
        resolution='not_required' OR
        (resolution='required' AND source=frozen_source
            AND source->>'kind'='prepared' AND source->>'mode'='retained')
    ) IS TRUE THEN RETURN false; END IF;
    IF resolution='unknown' AND NOT (
        (COALESCE(retry.canonical_request->'preparation','null'::jsonb)='null'::jsonb
            AND source->>'kind'='golden') OR
        (COALESCE(retry.canonical_request->'preparation','null'::jsonb)<>'null'::jsonb
            AND source->>'kind' IN ('prepared','preparation_never_delivered'))
    ) IS TRUE THEN RETURN false; END IF;
    IF source='null'::jsonb OR source->>'kind' IN ('registry','retained') OR
       (source->>'kind'='prepared' AND source->>'mode'='retained') THEN
        RETURN (target='null'::jsonb AND plan->'tombstone'='null'::jsonb) IS TRUE;
    END IF;
    IF root IS NULL THEN
        expected_name := CASE WHEN retry.canonical_request->'workspace_storage'<>'null'::jsonb
            THEN 'srw-ws-' || replace(retry.canonical_request->'workspace_storage'->>'uid','-','')
            ELSE 'agent-vm-' || COALESCE(retry.job_id,retry.thread_id)::text || '-rootdisk' END;
        RETURN (target=jsonb_build_object('kind','rootdisk_never_issued',
                'name',expected_name,'namespace',disposition->>'namespace')
            AND retry.expected_pvc_uid IS NULL AND retry.observed_pvc_uid IS NULL
            AND NOT EXISTS(SELECT 1 FROM jsonb_array_elements(disposition->'effects') effect
                WHERE effect->>'effect_kind'='rootdisk' AND effect->>'state'<>'rejected')) IS TRUE;
    END IF;
    IF disposition->>'disk_policy'='retain' THEN
        RETURN (target=jsonb_build_object('kind','rootdisk_completed',
                'name',root->>'name','namespace',root->>'namespace',
                'uid',root->>'uid','pvc_uid',root->>'pvc_uid')) IS TRUE;
    END IF;
    RETURN (root_completion->>'kind'='rootdisk_purged'
        AND target=root_completion) IS TRUE;
END;
$$;

CREATE OR REPLACE FUNCTION public.valid_vm_creation_resource_completion(retry public.vm_creation_retries, stage text, evidence jsonb)
RETURNS boolean LANGUAGE plpgsql STABLE AS $$
DECLARE resource jsonb; expected_name text; keys text[];
BEGIN
    IF NOT (stage IN ('cloud_init','rootdisk') AND jsonb_typeof(evidence)='object'
        AND evidence->'version'='1'::jsonb
        AND evidence->'disposition_id'=retry.cancellation_disposition->'disposition_id'
        AND evidence=retry.cancellation_progress->stage) IS TRUE THEN RETURN false; END IF;
    resource := retry.cancellation_disposition->'objects'->stage;
    IF resource IS NULL THEN
        expected_name := CASE WHEN stage='rootdisk' AND retry.canonical_request->'workspace_storage'<>'null'::jsonb
            THEN 'srw-ws-' || replace(retry.canonical_request->'workspace_storage'->>'uid','-','')
            ELSE 'agent-vm-' || COALESCE(retry.job_id,retry.thread_id)::text || CASE WHEN stage='rootdisk' THEN '-rootdisk' ELSE '-cloudinit' END END;
        keys := ARRAY['version','kind','disposition_id','name','namespace'];
        RETURN (evidence ?& keys AND evidence-keys='{}'::jsonb
            AND evidence->>'kind'=CASE WHEN stage='rootdisk' THEN 'rootdisk_never_issued' ELSE 'secret_never_issued' END
            AND evidence->>'name'=expected_name
            AND evidence->'namespace'=retry.cancellation_disposition->'namespace'
            AND (stage<>'rootdisk' OR (retry.expected_pvc_uid IS NULL AND retry.observed_pvc_uid IS NULL))
            AND NOT EXISTS(SELECT 1 FROM public.vm_creation_effects e
                WHERE e.request_id=retry.request_id AND e.effect_kind=stage AND e.state<>'rejected')) IS TRUE;
    END IF;
    IF NOT (evidence->'name'=resource->'name' AND evidence->'namespace'=resource->'namespace'
        AND evidence->'uid'=resource->'uid') IS TRUE THEN RETURN false; END IF;
    IF stage='cloud_init' THEN
        keys := ARRAY['version','kind','disposition_id','name','namespace','uid'];
        RETURN (evidence ?& keys AND evidence-keys='{}'::jsonb AND evidence->>'kind'='secret_absent') IS TRUE;
    END IF;
    IF retry.cancellation_disposition->>'disk_policy'='retain' THEN
        keys := ARRAY['version','kind','disposition_id','name','namespace','uid','pvc_uid'];
        RETURN (evidence ?& keys AND evidence-keys='{}'::jsonb
            AND evidence->>'kind'='rootdisk_retained' AND evidence->'pvc_uid'=resource->'pvc_uid'
            AND evidence->>'pvc_uid'=COALESCE(retry.expected_pvc_uid,retry.observed_pvc_uid)::text) IS TRUE;
    END IF;
    keys := ARRAY['version','kind','disposition_id','name','namespace','uid','pvc_uid','admission_id','request_id','intent_digest'];
    RETURN (evidence ?& keys AND evidence-keys='{}'::jsonb
        AND evidence->>'kind'='rootdisk_purged' AND evidence->'pvc_uid'=resource->'pvc_uid'
        AND retry.cancellation_disposition->>'disk_policy'=CASE WHEN retry.owner_kind='thread' THEN 'purge_new_thread_disk' ELSE 'purge_new_job_disk' END
        AND EXISTS(SELECT 1 FROM public.vm_workspace_cleanup_admissions a
            WHERE a.id::text=evidence->>'admission_id' AND a.parent_admission_id=retry.creation_admission_id
              AND a.request_id::text=evidence->>'request_id' AND a.intent_digest=evidence->>'intent_digest'
              AND a.owner_kind=retry.owner_kind AND a.owner_id=COALESCE(retry.job_id,retry.thread_id) AND a.pvc_uid::text=evidence->>'pvc_uid'
              AND a.source='controller_creation_rootdisk_delete' AND a.completed_at IS NOT NULL AND a.outcome='deleted')) IS TRUE;
END;
$$;

CREATE OR REPLACE FUNCTION public.valid_vm_creation_attachment_completion(retry public.vm_creation_retries, evidence jsonb)
RETURNS boolean LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE plan jsonb; binding jsonb; prior jsonb; lease jsonb; observed jsonb; labels jsonb; ann jsonb;
BEGIN
    plan := retry.cancellation_progress->'workspace_attachment';
    binding := COALESCE(retry.canonical_request->'workspace_storage','null'::jsonb);
    IF NOT (jsonb_typeof(plan)='object'
        AND plan ?& ARRAY['version','kind','disposition_id','request_id','job_id','provision_generation','binding','name','namespace','outcome','prior']
        AND plan-ARRAY['version','kind','disposition_id','request_id','job_id','provision_generation','binding','name','namespace','outcome','prior']='{}'::jsonb
        AND plan->'version'='1'::jsonb AND plan->>'kind'='attachment_disposition_planned'
        AND plan->'disposition_id'=retry.cancellation_disposition->'disposition_id'
        AND plan->>'request_id'=retry.request_id::text AND plan->>'job_id'=COALESCE(retry.job_id,retry.thread_id)::text
        AND plan->>'provision_generation'=retry.provision_generation::text
        AND plan->'namespace'=retry.cancellation_disposition->'namespace'
        AND plan->'binding'=binding
        AND jsonb_typeof(evidence)='object' AND evidence ?& ARRAY['version','kind','plan','outcome','lease']
        AND evidence-ARRAY['version','kind','plan','outcome','lease']='{}'::jsonb
        AND evidence->'version'='1'::jsonb AND evidence->>'kind'='attachment_disposition_completed'
        AND evidence->'plan'=plan AND evidence->'outcome'=plan->'outcome'
        AND retry.cancellation_completion ?& ARRAY['cloud_init','rootdisk','source']) IS TRUE THEN RETURN false; END IF;
    IF binding='null'::jsonb THEN
        RETURN (plan->>'outcome'='not_applicable' AND plan->'prior'='null'::jsonb
            AND plan->'name'='null'::jsonb AND evidence->'lease'='null'::jsonb) IS TRUE;
    END IF;
    IF NOT (plan->>'name'='srw-ws-' || replace(binding->>'uid','-','') AND (
        (plan->>'outcome'='released' AND binding->'pvc_uid'='null'::jsonb
            AND retry.cancellation_completion->'rootdisk'->>'kind'='rootdisk_never_issued') OR
        (plan->>'outcome'='detached' AND retry.cancellation_completion->'rootdisk'->>'kind'='rootdisk_retained')
    )) IS TRUE THEN RETURN false; END IF;
    labels := jsonb_build_object('srw.io/workspace-instance',binding->'uid',
        'srw.io/workspace-generation',binding->>'generation','srw.io/workspace-execution',COALESCE(retry.job_id,retry.thread_id)::text);
    prior := plan->'prior'; lease := evidence->'lease';
    observed := retry.cancellation_disposition->'objects'->'workspace_attach';
    IF prior='null'::jsonb THEN
        IF NOT (observed IS NULL AND binding->'generation'='1'::jsonb
            AND plan->>'outcome'='released') IS TRUE THEN RETURN false; END IF;
    ELSE
        IF NOT (jsonb_typeof(prior)='object' AND prior ?& ARRAY['uid','resource_version','labels','annotations']
            AND prior-ARRAY['uid','resource_version','labels','annotations']='{}'::jsonb
            AND prior->'uid'=observed->'uid' AND prior->'resource_version'=observed->'resource_version'
            AND prior->'labels'=labels AND prior->'uid'=lease->'uid'
            AND prior->'resource_version'<>lease->'resource_version') IS TRUE THEN RETURN false; END IF;
    END IF;
    ann := lease->'annotations';
    RETURN (jsonb_typeof(lease)='object' AND lease ?& ARRAY['uid','resource_version','labels','annotations']
        AND lease-ARRAY['uid','resource_version','labels','annotations']='{}'::jsonb
        AND jsonb_typeof(lease->'uid')='string' AND lease->>'uid' ~ '^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
        AND jsonb_typeof(lease->'resource_version')='string' AND length(lease->>'resource_version') BETWEEN 1 AND 256
        AND lease->'labels'=labels AND jsonb_typeof(ann)='object'
        AND ann ?& ARRAY['srw.io/vm-create-request-id','srw.io/vm-create-effect-nonce','srw.io/provision-generation','srw.io/detached','srw.io/released','srw.io/vm-create-disposition']
        AND ann-ARRAY['srw.io/vm-create-request-id','srw.io/vm-create-effect-nonce','srw.io/provision-generation','srw.io/detached','srw.io/released','srw.io/vm-create-disposition']='{}'::jsonb
        AND ann->>'srw.io/vm-create-request-id'=retry.request_id::text
        AND ann->>'srw.io/provision-generation'=retry.provision_generation::text
        AND ann->'srw.io/vm-create-effect-nonce'=COALESCE(prior->'annotations'->'srw.io/vm-create-effect-nonce','null'::jsonb)
        AND ann->>'srw.io/detached'=CASE WHEN plan->>'outcome'='detached' THEN 'true' ELSE 'false' END
        AND ann->>'srw.io/released'=CASE WHEN plan->>'outcome'='released' THEN 'true' ELSE 'false' END
        AND (ann->>'srw.io/vm-create-disposition')::jsonb=plan) IS TRUE;
END;
$$;

CREATE OR REPLACE FUNCTION public.valid_vm_creation_disposition_evidence(retry public.vm_creation_retries)
RETURNS boolean LANGUAGE plpgsql STABLE AS $$
DECLARE stage text;
BEGIN
    IF NOT (
        retry.cancellation_disposition IS NOT NULL
        AND retry.state IN ('cancel_requested','settled')
        AND retry.cancellation_disposition->>'request_id'=retry.request_id::text
        AND retry.cancellation_disposition->>'admission_id'=retry.creation_admission_id::text
        AND retry.cancellation_disposition->>'job_id'=COALESCE(retry.job_id,retry.thread_id)::text
        AND retry.cancellation_disposition->>'provision_generation'=retry.provision_generation::text
        AND retry.observed_vm_uid IS NULL
        AND (retry.owner_kind='job' OR (retry.owner_kind='thread'
             AND public.valid_vm_creation_thread_disposition_identity(retry,false)))
        AND retry.boot_counted=false
        AND jsonb_typeof(retry.cancellation_completion)='object'
        AND retry.cancellation_completion ?& ARRAY['source','cloud_init','rootdisk','workspace_attachment']
        AND retry.cancellation_completion-ARRAY['source','cloud_init','rootdisk','workspace_attachment']='{}'::jsonb
        AND NOT EXISTS(SELECT 1 FROM public.vm_creation_effects e
            WHERE e.request_id=retry.request_id
              AND (e.state='issued' OR (e.effect_kind='vm' AND e.state<>'rejected')))
        AND public.valid_vm_creation_source_authority(retry)
        AND public.valid_vm_creation_attachment_completion(
            retry,retry.cancellation_completion->'workspace_attachment')
    ) IS TRUE THEN RETURN false; END IF;
    FOREACH stage IN ARRAY ARRAY['cloud_init','rootdisk'] LOOP
        IF NOT public.valid_vm_creation_resource_completion(
            retry,stage,retry.cancellation_completion->stage) THEN RETURN false; END IF;
    END LOOP;
    RETURN true;
END;
$$;

CREATE OR REPLACE FUNCTION public.valid_vm_creation_disposition_parent_identity(
    retry public.vm_creation_retries, parent public.vm_workspace_cleanup_admissions)
RETURNS boolean LANGUAGE sql STABLE AS $$
    SELECT (parent.id=retry.creation_admission_id
        AND parent.owner_kind=retry.owner_kind AND parent.owner_id=COALESCE(retry.job_id,retry.thread_id)
        AND parent.source='controller_vm_create' AND parent.parent_admission_id IS NULL
        AND parent.pvc_uid IS NOT DISTINCT FROM COALESCE(retry.expected_pvc_uid,retry.observed_pvc_uid)
        AND parent.request_id=uuid_generate_v5(uuid_ns_url(),'vm-create:' || retry.request_id::text)
    ) IS TRUE;
$$;

CREATE OR REPLACE FUNCTION public.guard_vm_creation_disposition() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='INSERT' THEN
        IF NEW.cancellation_disposition IS NOT NULL OR NEW.cancellation_progress<>'{}'::jsonb THEN
            RAISE EXCEPTION 'Creation disposition requires locked cancellation' USING ERRCODE='23514';
        END IF;
        RETURN NEW;
    END IF;
    IF OLD.cancellation_disposition IS NOT NULL AND
       NEW.cancellation_disposition IS DISTINCT FROM OLD.cancellation_disposition THEN
        RAISE EXCEPTION 'Creation cancellation disposition is immutable' USING ERRCODE='23514';
    END IF;
    IF NEW.cancellation_disposition IS NOT NULL AND OLD.cancellation_disposition IS NULL THEN
        IF OLD.state<>'cancel_requested' OR NEW.state<>'cancel_requested' OR
           NEW.creation_admission_id IS NULL OR NEW.creation_carrier_uid IS NULL OR
           (NEW.owner_kind='thread' AND NOT public.valid_vm_creation_thread_disposition_identity(NEW,true)) OR
           EXISTS(SELECT 1 FROM public.vm_creation_effects WHERE request_id=NEW.request_id
                  AND (state='issued' OR (effect_kind='vm' AND state<>'rejected'))) THEN
            RAISE EXCEPTION 'Creation disposition requires no possible VM issuance' USING ERRCODE='23514';
        END IF;
    END IF;
    IF NEW.cancellation_disposition IS NOT NULL AND NEW.state<>'cancel_requested' AND
       NOT (OLD.state='cancel_requested' AND NEW.state='settled'
            AND NEW.reason='creation_disposed' AND NEW.resolved_at IS NOT NULL
            AND NEW.claim_token IS NULL AND NEW.claim_expires_at IS NULL
            AND public.valid_vm_creation_disposition_evidence(NEW)) THEN
        RAISE EXCEPTION 'Creation disposition has not completed' USING ERRCODE='23514';
    END IF;
    IF EXISTS(SELECT 1 FROM jsonb_each(OLD.cancellation_progress) AS p
              WHERE NEW.cancellation_progress->p.key IS DISTINCT FROM p.value) OR
       (NEW.cancellation_progress - ARRAY['cloud_init','rootdisk','workspace_attachment','source'])<>'{}'::jsonb THEN
        RAISE EXCEPTION 'Creation cancellation progress is monotonic' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION public.guard_vm_creation_disposition_terminal() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE retry public.vm_creation_retries%ROWTYPE;
        parent public.vm_workspace_cleanup_admissions%ROWTYPE;
        instance public.srw_workspace_instances%ROWTYPE;
BEGIN
    SELECT * INTO retry FROM public.vm_creation_retries WHERE request_id=NEW.request_id;
    IF retry.cancellation_disposition IS NULL OR retry.state<>'settled'
       OR retry.reason<>'creation_disposed' THEN RETURN NULL; END IF;
    SELECT * INTO parent FROM public.vm_workspace_cleanup_admissions
        WHERE id=retry.creation_admission_id;
    IF NOT (public.valid_vm_creation_disposition_evidence(retry)
        AND parent.completed_at IS NOT NULL AND parent.outcome='creation_disposed'
        AND public.valid_vm_creation_disposition_parent_identity(retry,parent)) IS TRUE THEN
        RAISE EXCEPTION 'Creation disposition terminal parent is incomplete' USING ERRCODE='23514';
    END IF;
    IF retry.cancellation_disposition->>'workspace_instance_id' IS NOT NULL THEN
        SELECT * INTO instance FROM public.srw_workspace_instances
            WHERE id=(retry.cancellation_disposition->>'workspace_instance_id')::uuid;
        IF NOT (FOUND AND instance.execution_id IS NULL
            AND instance.status=CASE
                WHEN retry.cancellation_completion->'workspace_attachment'->>'outcome'='detached'
                THEN 'Detached' ELSE 'Released' END
            AND instance.pvc_uid IS NOT DISTINCT FROM retry.cancellation_completion->'rootdisk'->>'pvc_uid'
            AND instance.backend_state->'retained_creation_disposition'->>'request_id'=retry.request_id::text
            AND instance.backend_state->'retained_creation_disposition'->'attachment'=retry.cancellation_completion->'workspace_attachment'
            AND instance.backend_state->'retained_creation_disposition'->'rootdisk'=retry.cancellation_completion->'rootdisk') IS TRUE THEN
            RAISE EXCEPTION 'Creation disposition terminal instance is incomplete' USING ERRCODE='23514';
        END IF;
    END IF;
    IF retry.owner_kind='thread' AND NOT EXISTS (
        SELECT 1 FROM public.threads t WHERE t.id=retry.thread_id
          AND NOT t.metadata ? 'vm'
          AND public.valid_vm_creation_thread_disposition_identity(retry,false)
          AND NOT EXISTS (SELECT 1 FROM public.vm_resource_reservations v
              WHERE v.request_id=retry.request_id AND v.state<>'released')
    ) THEN RAISE EXCEPTION 'Thread creation disposition remains held' USING ERRCODE='23514'; END IF;
    RETURN NULL;
END;
$$;

CREATE OR REPLACE FUNCTION public.guard_vm_resource_release_v2() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    source RECORD;
    idle RECORD;
    successor RECORD;
    owner_context JSONB;
    proof JSONB;
    current_vmi UUID;
    current_launcher UUID;
    cleanup_stop RECORD;
    thread_row RECORD;
BEGIN
    IF TG_OP<>'UPDATE' OR NEW.resource_version<>2 OR NEW.state<>'released'
       OR OLD.state='released' THEN
        RETURN NEW;
    END IF;
    SELECT w.owner_kind,w.job_id,w.thread_id,w.provision_generation,retry.state AS retry_state,
           retry.observed_vm_uid,retry.observed_pvc_uid,
           retry.cancellation_disposition,retry.creation_admission_id,
           retry.canonical_request,retry.controller_configuration
      INTO source
      FROM public.vm_resource_waiters w
      JOIN public.vm_creation_retries retry ON retry.request_id=w.request_id
     WHERE w.request_id=NEW.request_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'VM resource release source missing' USING ERRCODE='23514';
    END IF;
    proof := NEW.release_evidence;
    IF source.owner_kind='thread' THEN
        IF proof->>'kind'='never_vm_issued' THEN
            IF OLD.state<>'reserved' OR OLD.vm_uid IS NOT NULL
               OR source.retry_state<>'cancel_requested'
               OR proof->>'request_id' IS DISTINCT FROM NEW.request_id::text
               OR proof->>'thread_id' IS DISTINCT FROM source.thread_id::text
               OR proof->>'provision_generation' IS DISTINCT FROM source.provision_generation::text
               OR NOT (
                   (source.controller_configuration->>'golden_enabled' IS NOT DISTINCT FROM 'false'
                    AND source.canonical_request->'preparation' IS NULL
                    AND NOT EXISTS(SELECT 1 FROM public.vm_creation_effects e
                        WHERE e.request_id=NEW.request_id AND e.state<>'rejected'))
                   OR (proof->>'disposition_id' IS NOT NULL
                       AND proof->>'disposition_id' IS NOT DISTINCT FROM
                           source.cancellation_disposition->>'disposition_id'
                       AND proof->>'creation_admission_id' IS NOT DISTINCT FROM
                           source.creation_admission_id::text
                       AND EXISTS(SELECT 1 FROM public.vm_creation_retries r
                           WHERE r.request_id=NEW.request_id
                             AND public.valid_vm_creation_disposition_evidence(r))
                       AND EXISTS(SELECT 1 FROM public.vm_workspace_cleanup_admissions a
                           WHERE a.id=source.creation_admission_id
                             AND a.owner_kind='thread' AND a.owner_id=source.thread_id
                             AND a.completed_at IS NOT NULL AND a.outcome='creation_disposed')
                       AND NOT EXISTS(SELECT 1 FROM public.vm_creation_effects e
                           WHERE e.request_id=NEW.request_id
                             AND e.effect_kind='vm' AND e.state<>'rejected'))
               ) THEN
                RAISE EXCEPTION 'VM thread never-issued release unproven' USING ERRCODE='23514';
            END IF;
            RETURN NEW;
        END IF;
        IF proof->>'kind'<>'exact_compute_absent' OR OLD.state<>'teardown' THEN
            RAISE EXCEPTION 'VM thread physical release unproven' USING ERRCODE='23514';
        END IF;
        SELECT * INTO idle FROM public.vm_idle_operations
         WHERE id=(proof->>'operation_id')::uuid;
        SELECT id,status,metadata,runtime_generation,agent_id,runtime_attach_token
          INTO thread_row FROM public.threads WHERE id=source.thread_id;
        SELECT successor_vmi_uid,successor_launcher_uid INTO successor
          FROM public.vm_resource_recovery_successors
         WHERE reservation_id=NEW.id ORDER BY ordinal DESC LIMIT 1;
        current_vmi := COALESCE(successor.successor_vmi_uid,OLD.vmi_uid);
        current_launcher := COALESCE(successor.successor_launcher_uid,OLD.launcher_uid);
        owner_context := thread_row.metadata->'vm';
        IF idle.id IS NULL OR thread_row.id IS NULL
           OR thread_row.status<>'suspended'
           OR thread_row.runtime_generation=(SELECT thread_runtime_generation
                  FROM public.vm_creation_retries WHERE request_id=NEW.request_id)
           OR thread_row.agent_id IS NOT NULL OR thread_row.runtime_attach_token IS NOT NULL
           OR idle.owner_kind<>'thread' OR idle.release_kind<>'pinned_thread'
           OR idle.owner_id<>source.thread_id
           OR idle.provision_generation<>source.provision_generation
           OR idle.vm_uid IS DISTINCT FROM OLD.vm_uid
           OR idle.vm_uid IS DISTINCT FROM source.observed_vm_uid
           OR idle.pvc_uid IS DISTINCT FROM source.observed_pvc_uid
           OR idle.vmi_uid IS DISTINCT FROM current_vmi
           OR idle.launcher_uid IS DISTINCT FROM current_launcher
           OR idle.phase<>'suspended' OR idle.stop_verified_at IS NULL
           OR idle.thread_agent_stop_verified_at IS NULL
           OR idle.thread_agent_stop_evidence->>'version' IS DISTINCT FROM '1'
           OR idle.thread_agent_stop_evidence->>'retirement_token'
                  IS DISTINCT FROM idle.thread_retirement_token::text
           OR idle.thread_agent_stop_evidence->>'controller_authenticated'
                  IS DISTINCT FROM 'true'
           OR idle.thread_agent_stop_evidence->'pod'
                  IS DISTINCT FROM idle.thread_agent_pod_identity
           OR idle.stop_evidence->>'version' IS DISTINCT FROM '1'
           OR idle.stop_evidence->>'kind' IS DISTINCT FROM 'vm_idle_physical_stop'
           OR owner_context->>'status' IS DISTINCT FROM 'suspended'
           OR owner_context->>'rootdisk' IS DISTINCT FROM 'kept'
           OR owner_context->>'_suspend_remote_io_closed' IS DISTINCT FROM idle.id::text
           OR owner_context->>'provision_generation' IS DISTINCT FROM idle.provision_generation::text
           OR owner_context->>'vm_uid' IS DISTINCT FROM idle.vm_uid::text
           OR owner_context->>'rootdisk_pvc_uid' IS DISTINCT FROM idle.pvc_uid::text
           OR proof->>'thread_id' IS DISTINCT FROM source.thread_id::text
           OR proof->>'provision_generation' IS DISTINCT FROM source.provision_generation::text
           OR proof->>'vm_uid' IS DISTINCT FROM idle.vm_uid::text
           OR proof->>'vmi_uid' IS DISTINCT FROM idle.vmi_uid::text
           OR proof->>'launcher_uid' IS DISTINCT FROM idle.launcher_uid::text
           OR proof->>'pvc_uid' IS DISTINCT FROM idle.pvc_uid::text
           OR proof->>'stop_evidence_digest' IS DISTINCT FROM
              ('sha256:' || encode(sha256(convert_to(idle.stop_evidence::text,'UTF8')),'hex'))
           OR idle.stop_evidence->>'operation_id' IS DISTINCT FROM idle.id::text
           OR idle.stop_evidence->>'generation' IS DISTINCT FROM idle.provision_generation::text
           OR idle.stop_evidence->>'vm_uid' IS DISTINCT FROM idle.vm_uid::text
           OR idle.stop_evidence->>'vmi_uid' IS DISTINCT FROM idle.vmi_uid::text
           OR idle.stop_evidence->>'launcher_uid' IS DISTINCT FROM idle.launcher_uid::text
           OR idle.stop_evidence->>'pvc_uid' IS DISTINCT FROM idle.pvc_uid::text
           OR idle.stop_evidence->>'vm_absent' IS DISTINCT FROM 'true'
           OR idle.stop_evidence->>'vmi_absent' IS DISTINCT FROM 'true'
           OR idle.stop_evidence->>'launcher_absent' IS DISTINCT FROM 'true'
           OR idle.stop_evidence->>'retained_pvc' IS DISTINCT FROM 'true'
           OR idle.stop_evidence->>'controller_authenticated' IS DISTINCT FROM 'true'
           OR idle.stop_evidence->>'same_generation_replacement' IS DISTINCT FROM 'false'
           OR NOT EXISTS(
               SELECT 1 FROM public.managed_repository_process_zero_receipts p
                WHERE p.owner_kind='thread' AND p.owner_id=source.thread_id
                  AND p.scope='vm' AND p.provisioner='vm'
                  AND p.runtime_incarnation=source.provision_generation::text
           ) THEN
            RAISE EXCEPTION 'VM thread physical release proof changed' USING ERRCODE='23514';
        END IF;
        RETURN NEW;
    END IF;
    IF proof->>'kind'='never_vm_issued' THEN
        IF OLD.state<>'reserved' OR OLD.vm_uid IS NOT NULL
           OR source.retry_state<>'cancel_requested'
           OR proof->>'request_id' IS DISTINCT FROM NEW.request_id::text
           OR proof->>'job_id' IS DISTINCT FROM source.job_id::text
           OR proof->>'provision_generation' IS DISTINCT FROM source.provision_generation::text
           OR (
               EXISTS (SELECT 1 FROM public.vm_creation_effects e
                       WHERE e.request_id=NEW.request_id)
               AND NOT (
                   proof->>'disposition_id' IS NOT DISTINCT FROM
                       source.cancellation_disposition->>'disposition_id'
                   AND proof->>'disposition_id' IS NOT NULL
                   AND proof->>'creation_admission_id' IS NOT DISTINCT FROM
                       source.creation_admission_id::text
                   AND EXISTS (
                       SELECT 1 FROM public.vm_creation_retries r
                        WHERE r.request_id=NEW.request_id
                          AND public.valid_vm_creation_disposition_evidence(r)
                   )
                   AND EXISTS (
                       SELECT 1 FROM public.vm_workspace_cleanup_admissions c
                        WHERE c.id=source.creation_admission_id
                          AND c.completed_at IS NOT NULL
                          AND c.outcome='creation_disposed'
                   )
                   AND NOT EXISTS (
                       SELECT 1 FROM public.vm_creation_effects e
                        WHERE e.request_id=NEW.request_id
                          AND e.effect_kind='vm' AND e.state<>'rejected'
                   )
               )
           ) THEN
            RAISE EXCEPTION 'VM resource never-issued release unproven' USING ERRCODE='23514';
        END IF;
        RETURN NEW;
    END IF;
    IF proof->>'kind'='exact_cleanup_compute_absent' THEN
        SELECT * INTO cleanup_stop FROM public.vm_resource_cleanup_stop_receipts
         WHERE cleanup_admission_id=(proof->>'cleanup_admission_id')::uuid;
        IF OLD.state<>'teardown' OR cleanup_stop.cleanup_admission_id IS NULL
           OR cleanup_stop.reservation_id<>NEW.id
           OR cleanup_stop.request_id<>NEW.request_id
           OR cleanup_stop.job_id<>source.job_id
           OR cleanup_stop.provision_generation<>source.provision_generation
           OR cleanup_stop.vm_uid IS DISTINCT FROM OLD.vm_uid
           OR proof->>'job_id' IS DISTINCT FROM cleanup_stop.job_id::text
           OR proof->>'provision_generation' IS DISTINCT FROM cleanup_stop.provision_generation::text
           OR proof->>'vm_uid' IS DISTINCT FROM cleanup_stop.vm_uid::text
           OR proof->>'vmi_uid' IS DISTINCT FROM cleanup_stop.vmi_uid::text
           OR proof->>'launcher_uid' IS DISTINCT FROM cleanup_stop.launcher_uid::text
           OR proof->>'pvc_uid' IS DISTINCT FROM cleanup_stop.pvc_uid::text
           OR proof->>'stop_evidence_digest' IS DISTINCT FROM
              ('sha256:' || encode(sha256(convert_to(cleanup_stop.stop_evidence::text,'UTF8')),'hex')) THEN
            RAISE EXCEPTION 'VM resource cleanup release proof changed' USING ERRCODE='23514';
        END IF;
        RETURN NEW;
    END IF;
    IF proof->>'kind'<>'exact_compute_absent' OR OLD.state<>'teardown' THEN
        RAISE EXCEPTION 'VM resource physical release unproven' USING ERRCODE='23514';
    END IF;
    SELECT * INTO idle FROM public.vm_idle_operations
     WHERE id=(proof->>'operation_id')::uuid;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'VM resource idle release missing' USING ERRCODE='23514';
    END IF;
    SELECT successor_vmi_uid,successor_launcher_uid INTO successor
      FROM public.vm_resource_recovery_successors
     WHERE reservation_id=NEW.id ORDER BY ordinal DESC LIMIT 1;
    current_vmi := COALESCE(successor.successor_vmi_uid, OLD.vmi_uid);
    current_launcher := COALESCE(successor.successor_launcher_uid, OLD.launcher_uid);
    SELECT context->'vm' INTO owner_context FROM public.jobs WHERE id=source.job_id;
    IF idle.owner_kind<>'job' OR idle.owner_id<>source.job_id
       OR idle.provision_generation<>source.provision_generation
       OR idle.vm_uid IS DISTINCT FROM OLD.vm_uid
       OR idle.vm_uid IS DISTINCT FROM source.observed_vm_uid
       OR idle.pvc_uid IS DISTINCT FROM source.observed_pvc_uid
       OR idle.vmi_uid IS DISTINCT FROM current_vmi
       OR idle.launcher_uid IS DISTINCT FROM current_launcher
       OR idle.phase<>'suspended' OR idle.stop_verified_at IS NULL
       OR idle.stop_evidence->>'version' IS DISTINCT FROM '1'
       OR idle.stop_evidence->>'kind' IS DISTINCT FROM 'vm_idle_physical_stop'
       OR owner_context->>'status' IS DISTINCT FROM 'suspended'
       OR owner_context->>'_suspend_remote_io_closed' IS DISTINCT FROM idle.id::text
       OR owner_context->>'provision_generation' IS DISTINCT FROM idle.provision_generation::text
       OR owner_context->>'vm_uid' IS DISTINCT FROM idle.vm_uid::text
       OR owner_context->>'rootdisk_pvc_uid' IS DISTINCT FROM idle.pvc_uid::text
       OR proof->>'job_id' IS DISTINCT FROM source.job_id::text
       OR proof->>'provision_generation' IS DISTINCT FROM source.provision_generation::text
       OR proof->>'vm_uid' IS DISTINCT FROM idle.vm_uid::text
       OR proof->>'vmi_uid' IS DISTINCT FROM idle.vmi_uid::text
       OR proof->>'launcher_uid' IS DISTINCT FROM idle.launcher_uid::text
       OR proof->>'pvc_uid' IS DISTINCT FROM idle.pvc_uid::text
       OR proof->>'stop_evidence_digest' IS DISTINCT FROM
          ('sha256:' || encode(sha256(convert_to(idle.stop_evidence::text,'UTF8')),'hex'))
       OR idle.stop_evidence->>'operation_id' IS DISTINCT FROM idle.id::text
       OR idle.stop_evidence->>'generation' IS DISTINCT FROM idle.provision_generation::text
       OR idle.stop_evidence->>'vm_uid' IS DISTINCT FROM idle.vm_uid::text
       OR idle.stop_evidence->>'vmi_uid' IS DISTINCT FROM idle.vmi_uid::text
       OR idle.stop_evidence->>'launcher_uid' IS DISTINCT FROM idle.launcher_uid::text
       OR idle.stop_evidence->>'pvc_uid' IS DISTINCT FROM idle.pvc_uid::text
       OR idle.stop_evidence->>'vm_absent' IS DISTINCT FROM 'true'
       OR idle.stop_evidence->>'vmi_absent' IS DISTINCT FROM 'true'
       OR idle.stop_evidence->>'launcher_absent' IS DISTINCT FROM 'true'
       OR idle.stop_evidence->>'retained_pvc' IS DISTINCT FROM 'true'
       OR idle.stop_evidence->>'controller_authenticated' IS DISTINCT FROM 'true'
       OR idle.stop_evidence->>'same_generation_replacement' IS DISTINCT FROM 'false'
       OR NOT EXISTS(
           SELECT 1 FROM public.managed_repository_process_zero_receipts p
            WHERE p.owner_kind='job' AND p.owner_id=source.job_id
              AND p.scope='vm' AND p.provisioner='vm'
              AND p.runtime_incarnation=source.provision_generation::text
       ) THEN
        RAISE EXCEPTION 'VM resource physical release proof changed' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$$;

-- The historical function name remains for the 0278 process-zero and delete
-- callers. Its second branch now also accepts exact completed disposition.
CREATE OR REPLACE FUNCTION public.thread_vm_creation_never_issued_source(
    requested_thread uuid, requested_generation text
)
RETURNS boolean LANGUAGE sql STABLE AS $$
    SELECT EXISTS (
        SELECT 1 FROM public.threads t
        JOIN public.vm_creation_retries r
          ON r.request_id = (t.runtime_retirement_context
                             ->'vm_creation_source'->>'request_id')::uuid
        LEFT JOIN public.vm_workspace_cleanup_admissions a
          ON a.id = r.creation_admission_id
        WHERE t.id = requested_thread
          AND t.execution_lane = 'pinned'
          AND t.runtime_retirement_token IS NOT NULL
          AND t.runtime_retirement_authorized_at IS NOT NULL
          AND t.runtime_retirement_context->'vm_creation_source'->>'request_id'
              ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          AND t.runtime_retirement_context->'vm_creation_source'
              ->>'thread_runtime_generation' = t.runtime_generation::text
          AND t.runtime_retirement_context->'vm_creation_source'
              ->>'provision_generation' = requested_generation
          AND r.owner_kind = 'thread' AND r.thread_id = t.id
          AND r.job_id IS NULL
          AND r.thread_runtime_generation = t.runtime_generation
          AND r.provision_generation::text = requested_generation
          AND r.request_digest = t.runtime_retirement_context
              ->'vm_creation_source'->>'request_digest'
          AND r.controller_configuration_digest = t.runtime_retirement_context
              ->'vm_creation_source'->>'controller_configuration_digest'
          AND r.state = 'settled' AND (
              (r.reason = 'creation_never_issued'
               AND r.observed_vm_uid IS NULL AND r.observed_pvc_uid IS NULL
               AND (a.id IS NULL OR (a.completed_at IS NOT NULL
                     AND a.outcome = 'never_issued' AND a.owner_kind = 'thread'
                     AND a.owner_id = t.id))
               AND NOT EXISTS (SELECT 1 FROM public.vm_creation_effects e
                   WHERE e.request_id = r.request_id
                     AND e.state IN ('issued','observed')))
              OR (r.reason = 'creation_disposed'
                  AND r.observed_vm_uid IS NULL
                  AND a.completed_at IS NOT NULL
                  AND a.outcome = 'creation_disposed'
                  AND a.owner_kind = 'thread' AND a.owner_id = t.id
                  AND public.valid_vm_creation_disposition_evidence(r))
          )
          AND NOT EXISTS (
              SELECT 1 FROM public.vm_resource_reservations v
               WHERE v.request_id = r.request_id AND v.state <> 'released'
          )
    );
$$;
COMMIT;
