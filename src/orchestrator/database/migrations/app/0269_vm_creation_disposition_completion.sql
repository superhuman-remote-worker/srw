-- migration: 0269_vm_creation_disposition_completion.sql
-- description: Persist actual creation disposition evidence separately from plans.
-- depends-on: 0268_workspace_idle_terminal_exit.sql
-- transactional: yes
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE public.vm_creation_retries
    ADD COLUMN cancellation_completion jsonb NOT NULL DEFAULT '{}';

CREATE FUNCTION public.valid_vm_creation_source_readback(source jsonb, tombstone jsonb, observation jsonb, outcome text)
RETURNS boolean LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE field text; fact jsonb; source_gone boolean;
BEGIN
    IF NOT (jsonb_typeof(observation)='object'
        AND observation ?& ARRAY['dv','pvc','pin']
        AND observation-ARRAY['dv','pvc','pin']='{}'::jsonb) IS TRUE THEN RETURN false; END IF;
    FOREACH field IN ARRAY ARRAY['dv','pvc'] LOOP
        fact := observation->field;
        IF fact<>'null'::jsonb AND NOT (
            jsonb_typeof(fact)='object' AND fact ?& ARRAY['uid','resource_version']
            AND fact-ARRAY['uid','resource_version']='{}'::jsonb
            AND jsonb_typeof(fact->'uid')='string'
            AND fact->>'uid' ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
            AND jsonb_typeof(fact->'resource_version')='string'
            AND length(fact->>'resource_version') BETWEEN 1 AND 256
        ) IS TRUE THEN RETURN false; END IF;
    END LOOP;
    source_gone := observation->'dv'='null'::jsonb OR observation->'dv'->'uid'<>source->'dv_uid';
    RETURN (CASE WHEN source_gone THEN
        outcome='source_identity_gone' AND observation->'pin'='null'::jsonb
        ELSE outcome='pin_disposed' AND observation->'pin'=tombstone
    END) IS TRUE;
END;
$$;

CREATE FUNCTION public.valid_vm_creation_source_completion(plan jsonb, evidence jsonb)
RETURNS boolean LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
    source jsonb;
    observation jsonb;
    allocation jsonb;
    receipt jsonb;
BEGIN
    IF NOT (jsonb_typeof(plan)='object' AND plan->'version'='1'::jsonb
        AND plan->>'kind'='source_disposition_planned'
        AND jsonb_typeof(evidence)='object' AND evidence->'version'='1'::jsonb
        AND evidence->>'kind'='source_disposition_completed' AND evidence->'plan'=plan
        AND evidence ?& ARRAY['version','kind','outcome','plan','source_observation','allocation']
        AND evidence - ARRAY['version','kind','outcome','plan','source_observation','allocation']='{}'::jsonb
        AND octet_length(evidence::text)<=1048576) IS TRUE THEN RETURN false; END IF;
    source := plan->'source';
    observation := evidence->'source_observation';
    allocation := evidence->'allocation';
    IF plan->'tombstone'<>'null'::jsonb THEN
        IF NOT (source->>'kind' IN ('golden','prepared')
            AND plan->'tombstone'->>'state'='disposed'
            AND plan->'tombstone'->'dv_uid'=source->'dv_uid'
            AND plan->'tombstone'->'pvc_uid'=source->'pvc_uid'
            AND plan->'tombstone'->'target'=plan->'target'
            AND public.valid_vm_creation_source_readback(source,plan->'tombstone',observation,evidence->>'outcome')) IS TRUE THEN RETURN false; END IF;
    ELSIF source->>'kind'='preparation_never_delivered' THEN
        IF NOT (evidence->>'outcome'='allocation_never_delivered' AND observation='null'::jsonb) IS TRUE THEN RETURN false; END IF;
    ELSIF NOT (evidence->>'outcome'='not_required' AND observation='null'::jsonb
        AND plan->'target'='null'::jsonb AND
        (source='null'::jsonb OR source->>'kind' IN ('registry','retained')
         OR (source->>'kind'='prepared' AND source->>'mode'='retained'))) IS TRUE THEN
        RETURN false;
    END IF;
    IF source->>'kind'='preparation_never_delivered' OR
       (source->>'kind'='prepared' AND source->>'mode'='clone') THEN
        IF NOT (jsonb_typeof(allocation)='object'
            AND allocation ?& ARRAY['name','uid','resource_version','request','phase','creation_binding','workspace_source_issued','receipt']
            AND allocation-ARRAY['name','uid','resource_version','request','phase','creation_binding','workspace_source_issued','receipt']='{}'::jsonb
            AND allocation->'name'=source->'allocation'->'name'
            AND allocation->'uid'=source->'allocation'->'uid'
            AND allocation->'request'=source->'allocation'->'request'
            AND allocation->>'phase'='Cancelled'
            AND jsonb_typeof(allocation->'resource_version')='string'
            AND length(allocation->>'resource_version') BETWEEN 1 AND 256) IS TRUE THEN RETURN false; END IF;
        receipt := allocation->'receipt';
        IF NOT (jsonb_typeof(receipt)='object' AND receipt->'version'='1'::jsonb AND receipt->'plan'=plan) IS TRUE THEN RETURN false; END IF;
        IF source->>'kind'='preparation_never_delivered' THEN
            IF NOT (allocation->'workspace_source_issued'='false'::jsonb
                AND allocation->'creation_binding'=COALESCE(source->'allocation'->'state'->'creation_binding','null'::jsonb)
                AND receipt->>'kind'='prepared_source_never_delivered'
                AND receipt-ARRAY['version','kind','plan']='{}'::jsonb) IS TRUE THEN RETURN false; END IF;
        ELSE
            IF NOT (allocation->'workspace_source_issued'='true'::jsonb
                AND allocation->'creation_binding'->'request_id'=plan->'request_id'
                AND allocation->'creation_binding'->'provision_generation'=plan->'provision_generation'
                AND allocation->'creation_binding'->'request_digest'=plan->'request_digest') IS TRUE THEN RETURN false; END IF;
            IF receipt->>'kind'='prepared_source_disposed' THEN
                IF NOT (receipt-ARRAY['version','kind','plan','source_resource_version']='{}'::jsonb
                    AND jsonb_typeof(receipt->'source_resource_version')='string'
                    AND length(receipt->>'source_resource_version')>0) IS TRUE THEN RETURN false; END IF;
            ELSIF receipt->>'kind'='prepared_source_identity_gone' THEN
                IF NOT (receipt-ARRAY['version','kind','plan','source_observation']='{}'::jsonb
                    AND public.valid_vm_creation_source_readback(source,plan->'tombstone',
                        receipt->'source_observation','source_identity_gone')) IS TRUE THEN RETURN false; END IF;
            ELSE RETURN false;
            END IF;
        END IF;
    ELSIF allocation<>'null'::jsonb THEN RETURN false;
    END IF;
    RETURN true;
END;
$$;

CREATE FUNCTION public.valid_vm_creation_source_authority(retry public.vm_creation_retries)
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
        AND plan->>'job_id'=retry.job_id::text
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
            ELSE 'agent-vm-' || retry.job_id::text || '-rootdisk' END;
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

CREATE FUNCTION public.valid_vm_creation_resource_completion(retry public.vm_creation_retries, stage text, evidence jsonb)
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
            ELSE 'agent-vm-' || retry.job_id::text || CASE WHEN stage='rootdisk' THEN '-rootdisk' ELSE '-cloudinit' END END;
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
        AND retry.cancellation_disposition->>'disk_policy'='purge_new_job_disk'
        AND EXISTS(SELECT 1 FROM public.vm_workspace_cleanup_admissions a
            WHERE a.id::text=evidence->>'admission_id' AND a.parent_admission_id=retry.creation_admission_id
              AND a.request_id::text=evidence->>'request_id' AND a.intent_digest=evidence->>'intent_digest'
              AND a.owner_kind='job' AND a.owner_id=retry.job_id AND a.pvc_uid::text=evidence->>'pvc_uid'
              AND a.source='controller_creation_rootdisk_delete' AND a.completed_at IS NOT NULL AND a.outcome='deleted')) IS TRUE;
END;
$$;

CREATE FUNCTION public.valid_vm_creation_attachment_completion(retry public.vm_creation_retries, evidence jsonb)
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
        AND plan->>'request_id'=retry.request_id::text AND plan->>'job_id'=retry.job_id::text
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
        'srw.io/workspace-generation',binding->>'generation','srw.io/workspace-execution',retry.job_id::text);
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

CREATE FUNCTION public.valid_vm_creation_disposition_evidence(retry public.vm_creation_retries)
RETURNS boolean LANGUAGE plpgsql STABLE AS $$
DECLARE stage text;
BEGIN
    IF NOT (
        retry.cancellation_disposition IS NOT NULL
        AND retry.state IN ('cancel_requested','settled')
        AND retry.cancellation_disposition->>'request_id'=retry.request_id::text
        AND retry.cancellation_disposition->>'admission_id'=retry.creation_admission_id::text
        AND retry.cancellation_disposition->>'job_id'=retry.job_id::text
        AND retry.cancellation_disposition->>'provision_generation'=retry.provision_generation::text
        AND retry.observed_vm_uid IS NULL
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

CREATE FUNCTION public.guard_vm_creation_completion() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE evidence jsonb; stage text;
BEGIN
    IF TG_OP='INSERT' AND NEW.cancellation_completion<>'{}'::jsonb THEN
        RAISE EXCEPTION 'Creation completion requires locked disposition' USING ERRCODE='23514';
    END IF;
    IF NOT (jsonb_typeof(NEW.cancellation_completion)='object'
        AND NEW.cancellation_completion-ARRAY['source','cloud_init','rootdisk','workspace_attachment']='{}'::jsonb) IS TRUE THEN
        RAISE EXCEPTION 'Creation completion evidence is invalid' USING ERRCODE='23514';
    END IF;
    IF TG_OP='UPDATE' AND EXISTS(SELECT 1 FROM jsonb_each(OLD.cancellation_completion) p
        WHERE NEW.cancellation_completion->p.key IS DISTINCT FROM p.value) THEN
        RAISE EXCEPTION 'Creation completion evidence is immutable' USING ERRCODE='23514';
    END IF;
    IF NEW.cancellation_completion ? 'source' THEN
        evidence := NEW.cancellation_completion->'source';
        IF NEW.cancellation_disposition IS NULL OR NOT (
            public.valid_vm_creation_source_authority(NEW)
        ) IS TRUE THEN
            RAISE EXCEPTION 'Creation source completion is unproven' USING ERRCODE='23514';
        END IF;
    END IF;
    FOREACH stage IN ARRAY ARRAY['cloud_init','rootdisk'] LOOP
        IF NEW.cancellation_completion ? stage AND NOT
            public.valid_vm_creation_resource_completion(NEW,stage,NEW.cancellation_completion->stage) THEN
            RAISE EXCEPTION 'Creation resource completion is unproven' USING ERRCODE='23514';
        END IF;
    END LOOP;
    IF NEW.cancellation_completion ? 'workspace_attachment' AND NOT
        public.valid_vm_creation_attachment_completion(NEW,NEW.cancellation_completion->'workspace_attachment') THEN
        RAISE EXCEPTION 'Creation attachment completion is unproven' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER vm_creation_completion BEFORE INSERT OR UPDATE ON public.vm_creation_retries
FOR EACH ROW EXECUTE FUNCTION public.guard_vm_creation_completion();

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

CREATE FUNCTION public.valid_vm_creation_disposition_parent_identity(
    retry public.vm_creation_retries, parent public.vm_workspace_cleanup_admissions)
RETURNS boolean LANGUAGE sql STABLE AS $$
    SELECT (parent.id=retry.creation_admission_id
        AND parent.owner_kind='job' AND parent.owner_id=retry.job_id
        AND parent.source='controller_vm_create' AND parent.parent_admission_id IS NULL
        AND parent.pvc_uid IS NOT DISTINCT FROM COALESCE(retry.expected_pvc_uid,retry.observed_pvc_uid)
        AND parent.request_id=uuid_generate_v5(uuid_ns_url(),'vm-create:' || retry.request_id::text)
    ) IS TRUE;
$$;

CREATE FUNCTION public.guard_vm_creation_disposition_parent() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE retry public.vm_creation_retries%ROWTYPE;
BEGIN
    SELECT * INTO retry FROM public.vm_creation_retries r
        WHERE r.creation_admission_id=NEW.id AND r.cancellation_disposition IS NOT NULL;
    IF FOUND AND (retry.state='settled' OR NEW.completed_at IS NOT NULL) AND NOT (
        retry.state='settled' AND retry.reason='creation_disposed'
        AND NEW.completed_at IS NOT NULL
        AND NEW.outcome='creation_disposed'
        AND public.valid_vm_creation_disposition_parent_identity(retry,NEW)
        AND public.valid_vm_creation_disposition_evidence(retry)
    ) IS TRUE THEN
        RAISE EXCEPTION 'Creation disposition parent remains held' USING ERRCODE='23514';
    END IF;
    RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER vm_creation_disposition_parent AFTER UPDATE ON public.vm_workspace_cleanup_admissions
DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.guard_vm_creation_disposition_parent();

CREATE FUNCTION public.guard_vm_creation_disposition_instance() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE current_instance public.srw_workspace_instances%ROWTYPE;
        retry public.vm_creation_retries%ROWTYPE;
        completion jsonb;
        receipt jsonb;
BEGIN
    SELECT * INTO current_instance FROM public.srw_workspace_instances WHERE id=NEW.id;
    SELECT * INTO retry FROM public.vm_creation_retries r
        WHERE r.cancellation_disposition->>'workspace_instance_id'=NEW.id::text;
    IF ROW(current_instance.status,current_instance.execution_id,current_instance.generation,
           current_instance.pvc_name,current_instance.pvc_uid,current_instance.backend_state->'storage')
       IS DISTINCT FROM ROW(OLD.status,OLD.execution_id,OLD.generation,OLD.pvc_name,OLD.pvc_uid,OLD.backend_state->'storage')
       AND FOUND AND retry.state='cancel_requested' THEN
        RAISE EXCEPTION 'Creation disposition instance remains held' USING ERRCODE='23514';
    END IF;
    IF FOUND AND retry.state='settled' AND retry.reason='creation_disposed' THEN
        completion := retry.cancellation_completion;
        receipt := current_instance.backend_state->'retained_creation_disposition';
        IF NOT (
            public.valid_vm_creation_disposition_evidence(retry)
            AND current_instance.execution_id IS NULL
            AND current_instance.generation=(retry.canonical_request->'workspace_storage'->>'generation')::bigint
            AND current_instance.pvc_uid IS NOT DISTINCT FROM completion->'rootdisk'->>'pvc_uid'
            AND current_instance.status=CASE WHEN completion->'workspace_attachment'->>'outcome'='detached'
                THEN 'Detached' ELSE 'Released' END
            AND receipt->>'request_id'=retry.request_id::text
            AND receipt->>'disposition_id'=retry.cancellation_disposition->>'disposition_id'
            AND receipt->>'provision_generation'=retry.provision_generation::text
            AND receipt->>'execution_id'=retry.execution_id::text
            AND receipt->'attachment'=completion->'workspace_attachment'
            AND receipt->'rootdisk'=completion->'rootdisk'
        ) IS TRUE THEN
            RAISE EXCEPTION 'Creation disposition instance is incomplete' USING ERRCODE='23514';
        END IF;
    END IF;
    RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER vm_creation_disposition_instance AFTER UPDATE ON public.srw_workspace_instances
DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.guard_vm_creation_disposition_instance();

CREATE FUNCTION public.guard_vm_creation_disposition_terminal() RETURNS trigger LANGUAGE plpgsql AS $$
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
    RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER vm_creation_disposition_terminal AFTER UPDATE ON public.vm_creation_retries
DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.guard_vm_creation_disposition_terminal();
COMMIT;
