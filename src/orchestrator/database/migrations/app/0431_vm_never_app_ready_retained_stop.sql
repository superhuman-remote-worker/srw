-- migration: 0431_vm_never_app_ready_retained_stop.sql
-- description: Immutable policy-1 held stop with exact older process-zero linkage.
-- depends-on: 0430_connector_minted_credentials.sql
-- transactional: yes
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE public.vm_pre_ssh_stop_intents ADD COLUMN prior_zero_receipt_id uuid;
ALTER TABLE public.vm_pre_ssh_stop_intents ADD COLUMN held_stop_xact_id xid8;
ALTER TABLE public.vm_pre_ssh_stop_proofs ADD COLUMN held_stop_xact_id xid8;
ALTER TABLE public.vm_pre_ssh_stop_intents ADD CONSTRAINT vm_pre_ssh_stop_prior_zero_fk
    FOREIGN KEY (prior_zero_receipt_id) REFERENCES public.managed_repository_process_zero_receipts(id) NOT VALID;

CREATE FUNCTION public.vm_never_app_ready_stop_authorized(i public.vm_pre_ssh_stop_intents)
RETURNS boolean LANGUAGE plpgsql AS $body$
DECLARE a public.vm_job_cancel_retention_authorities%ROWTYPE;
        f jsonb := i.frozen;
        receipt_id uuid;
        item jsonb;
BEGIN
    SELECT * INTO a FROM public.vm_job_cancel_retention_authorities
     WHERE cleanup_admission_id=i.cleanup_admission_id;
    IF a.cleanup_admission_id IS NULL OR a.policy_version IS DISTINCT FROM 1
       OR a.admitted_xact_id=pg_current_xact_id() OR a.job_retained_resume_id IS NOT NULL
       OR i.job_id IS DISTINCT FROM a.job_id OR i.provision_generation IS DISTINCT FROM a.provision_generation
       OR i.creation_request_id IS DISTINCT FROM a.creation_request_id
       OR i.reservation_id IS DISTINCT FROM a.reservation_id OR i.reservation_revision IS DISTINCT FROM a.reservation_revision
       OR i.vm_uid IS DISTINCT FROM a.vm_uid OR i.vmi_uid IS DISTINCT FROM a.vmi_uid
       OR i.launcher_uid IS DISTINCT FROM a.launcher_uid OR i.pvc_uid IS DISTINCT FROM a.pvc_uid
       OR i.node_uid IS DISTINCT FROM a.node_uid OR i.cleanup_intent_digest IS DISTINCT FROM a.intent_digest
       OR f->>'kind' IS DISTINCT FROM 'vm_job_never_app_ready_retained_stop_candidate_v1'
       OR (SELECT array_agg(k ORDER BY k COLLATE "C") FROM jsonb_object_keys(f) k) IS DISTINCT FROM ARRAY['cleanup_admission_id','cleanup_intent_digest','cleanup_request_id','containers','job_id','kind','kube_vm_ready_at_inspection','launcher_name','launcher_resource_version','launcher_uid','namespace','node_name','node_uid','provision_generation','pvc_uid','vm_generation','vm_name','vm_resource_version','vm_uid','vmi_uid']
       OR jsonb_typeof(f->'kube_vm_ready_at_inspection') IS DISTINCT FROM 'boolean'
       OR f->>'cleanup_admission_id' IS DISTINCT FROM a.cleanup_admission_id::text
       OR f->>'cleanup_request_id' IS DISTINCT FROM a.cleanup_request_id::text
       OR f->>'cleanup_intent_digest' IS DISTINCT FROM a.intent_digest
       OR f->>'namespace' IS DISTINCT FROM a.namespace
       OR f->>'vm_name' IS DISTINCT FROM 'agent-vm-'||a.job_id
       OR jsonb_typeof(f->'containers') IS DISTINCT FROM 'array'
       OR jsonb_array_length(f->'containers')=0
       OR NOT EXISTS(SELECT 1 FROM jsonb_array_elements(f->'containers') c WHERE c->>'kind'='regular' AND c->>'name'='compute')
       OR (SELECT count(*)<>count(DISTINCT c->>'name') FROM jsonb_array_elements(f->'containers') c) THEN
        RETURN false;
    END IF;
    SELECT z.id INTO receipt_id FROM public.managed_repository_process_zero_receipts z
     WHERE z.owner_kind='job' AND z.owner_id=i.job_id AND z.scope='vm' AND z.provisioner='vm'
       AND z.runtime_incarnation=i.provision_generation::text;
    IF i.prior_zero_receipt_id IS DISTINCT FROM receipt_id THEN RETURN false; END IF;
    FOREACH item IN ARRAY ARRAY[f->'launcher_name',f->'node_name',f->'vm_resource_version',f->'launcher_resource_version'] LOOP
        IF jsonb_typeof(item) IS DISTINCT FROM 'string' OR item#>>'{}' !~ '^[^[:space:]]+$' THEN RETURN false; END IF;
    END LOOP;
    FOR item IN SELECT value FROM jsonb_array_elements(f->'containers') LOOP
        IF (SELECT array_agg(k ORDER BY k COLLATE "C") FROM jsonb_object_keys(item) k) IS DISTINCT FROM ARRAY['container_id','kind','name']
           OR item->>'kind' NOT IN ('regular','init')
           OR jsonb_typeof(item->'name') IS DISTINCT FROM 'string' OR item->>'name' !~ '^[^[:space:]]+$'
           OR jsonb_typeof(item->'container_id') IS DISTINCT FROM 'string' OR item->>'container_id' !~ '^[^[:space:]]+$' THEN RETURN false; END IF;
    END LOOP;
    RETURN public.validate_vm_job_cancel_retention(i.cleanup_admission_id,false);
END;
$body$;

CREATE OR REPLACE FUNCTION public.guard_vm_pre_ssh_stop_intent() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    admission public.vm_workspace_cleanup_admissions%ROWTYPE;
    retry public.vm_creation_retries%ROWTYPE;
    reservation public.vm_resource_reservations%ROWTYPE;
    owner_vm jsonb;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'VM pre-SSH stop intent is append-only' USING ERRCODE='23514';
    END IF;
    IF NEW.frozen->>'kind' IN ('vm_initial_ready_positive_stop_candidate_v1','vm_job_never_app_ready_retained_stop_candidate_v1') THEN
    IF NEW.frozen->>'kind'='vm_initial_ready_positive_stop_candidate_v1' THEN
        NEW.initial_ready_xact_id := pg_current_xact_id();
    ELSE
        NEW.held_stop_xact_id := pg_current_xact_id();
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended('workspace-recovery:job:'||NEW.job_id,0));
    PERFORM pg_advisory_xact_lock(hashtextextended('workspace-recovery-pvc:'||(SELECT pvc_uid::text FROM public.vm_workspace_cleanup_admissions WHERE id=NEW.cleanup_admission_id),0));
    PERFORM 1 FROM public.run_queue WHERE unit_id=NEW.job_id FOR UPDATE;
    END IF;
    SELECT context->'vm' INTO owner_vm FROM public.jobs
     WHERE id=NEW.job_id FOR UPDATE;
    SELECT * INTO admission FROM public.vm_workspace_cleanup_admissions
     WHERE id=NEW.cleanup_admission_id FOR UPDATE;
    SELECT * INTO retry FROM public.vm_creation_retries
     WHERE request_id=NEW.creation_request_id FOR UPDATE;
    SELECT * INTO reservation FROM public.vm_resource_reservations
     WHERE id=NEW.reservation_id FOR UPDATE;
    IF admission.id IS NULL OR admission.owner_kind IS DISTINCT FROM 'job'
       OR admission.owner_id IS DISTINCT FROM NEW.job_id
       OR admission.completed_at IS NOT NULL
       OR admission.pvc_uid IS DISTINCT FROM NEW.pvc_uid
       OR admission.intent_digest IS DISTINCT FROM NEW.cleanup_intent_digest
       OR retry.request_id IS NULL OR retry.job_id IS DISTINCT FROM NEW.job_id
       OR retry.provision_generation IS DISTINCT FROM NEW.provision_generation
       OR retry.state IS DISTINCT FROM 'succeeded'
       OR retry.observed_vm_uid IS DISTINCT FROM NEW.vm_uid
       OR retry.observed_pvc_uid IS DISTINCT FROM NEW.pvc_uid
       OR (CASE WHEN NEW.frozen->>'kind'='vm_initial_ready_positive_stop_candidate_v1'
           THEN retry.ready_at IS NULL OR NOT public.vm_initial_ready_stop_authorized(NEW)
           WHEN NEW.frozen->>'kind'='vm_job_never_app_ready_retained_stop_candidate_v1'
           THEN retry.ready_at IS NOT NULL OR NOT public.vm_never_app_ready_stop_authorized(NEW)
           ELSE retry.ready_at IS NOT NULL OR NEW.frozen->>'kind' IS DISTINCT FROM 'vm_pre_ssh_stop_candidate_v1' END)
       OR reservation.id IS NULL OR reservation.request_id IS DISTINCT FROM retry.request_id
       OR reservation.revision IS DISTINCT FROM NEW.reservation_revision
       OR reservation.state IS DISTINCT FROM 'teardown'
       OR reservation.vm_uid IS DISTINCT FROM NEW.vm_uid
       OR reservation.vmi_uid IS DISTINCT FROM NEW.vmi_uid
       OR reservation.launcher_uid IS DISTINCT FROM NEW.launcher_uid
       OR reservation.node_uid IS DISTINCT FROM NEW.node_uid
       OR owner_vm->>'provision_generation' IS DISTINCT FROM NEW.provision_generation::text
       OR owner_vm->>'vm_uid' IS DISTINCT FROM NEW.vm_uid::text
       OR owner_vm->>'rootdisk_pvc_uid' IS DISTINCT FROM NEW.pvc_uid::text
       OR owner_vm->>'status' IS DISTINCT FROM 'retiring_process_zero'
       OR (NEW.frozen->>'kind'<>'vm_job_never_app_ready_retained_stop_candidate_v1'
           AND NEW.prior_zero_receipt_id IS NOT NULL)
       OR (NEW.frozen->>'kind'<>'vm_job_never_app_ready_retained_stop_candidate_v1' AND EXISTS (
           SELECT 1 FROM public.managed_repository_process_zero_receipts z
           WHERE z.owner_kind='job' AND z.owner_id=NEW.job_id
             AND z.scope='vm' AND z.provisioner='vm'
             AND z.runtime_incarnation=NEW.provision_generation::text))
       OR EXISTS (SELECT 1 FROM public.vm_resource_recovery_successors s
                   WHERE s.reservation_id=reservation.id)
       OR NEW.frozen->>'job_id' IS DISTINCT FROM NEW.job_id::text
       OR NEW.frozen->>'provision_generation' IS DISTINCT FROM NEW.provision_generation::text
       OR NEW.frozen->>'vm_uid' IS DISTINCT FROM NEW.vm_uid::text
       OR NEW.frozen->>'vmi_uid' IS DISTINCT FROM NEW.vmi_uid::text
       OR NEW.frozen->>'launcher_uid' IS DISTINCT FROM NEW.launcher_uid::text
       OR NEW.frozen->>'pvc_uid' IS DISTINCT FROM NEW.pvc_uid::text
       OR NEW.frozen->>'node_uid' IS DISTINCT FROM NEW.node_uid::text
       OR jsonb_typeof(NEW.frozen->'vm_generation') IS DISTINCT FROM 'number'
       OR NEW.frozen->>'vm_generation' !~ '^[1-9][0-9]*$'
       OR NEW.frozen_digest IS DISTINCT FROM
          'sha256:'||encode(sha256(convert_to(NEW.frozen::text,'UTF8')),'hex') THEN
        RAISE EXCEPTION 'VM pre-SSH stop intent lacks exact retirement authority'
            USING ERRCODE='23514', CONSTRAINT='vm_pre_ssh_stop_intent_authority';
    END IF;
    RETURN NEW;
END;
$$;


CREATE OR REPLACE FUNCTION public.guard_vm_pre_ssh_stop_proof() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    intent public.vm_pre_ssh_stop_intents%ROWTYPE;
    admission public.vm_workspace_cleanup_admissions%ROWTYPE;
    retry public.vm_creation_retries%ROWTYPE;
    reservation public.vm_resource_reservations%ROWTYPE;
    owner_vm jsonb;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'VM pre-SSH stop proof is append-only' USING ERRCODE='23514';
    END IF;
    IF EXISTS(SELECT 1 FROM public.vm_pre_ssh_stop_intents i WHERE i.cleanup_admission_id=NEW.cleanup_admission_id AND i.frozen->>'kind' IN ('vm_initial_ready_positive_stop_candidate_v1','vm_job_never_app_ready_retained_stop_candidate_v1')) THEN
    PERFORM pg_advisory_xact_lock(hashtextextended('workspace-recovery:job:'||NEW.job_id,0));
    PERFORM pg_advisory_xact_lock(hashtextextended('workspace-recovery-pvc:'||(SELECT pvc_uid::text FROM public.vm_workspace_cleanup_admissions WHERE id=NEW.cleanup_admission_id),0));
    PERFORM 1 FROM public.run_queue WHERE unit_id=NEW.job_id FOR UPDATE;
    END IF;
    SELECT context->'vm' INTO owner_vm FROM public.jobs
     WHERE id=NEW.job_id FOR UPDATE;
    SELECT * INTO admission FROM public.vm_workspace_cleanup_admissions
     WHERE id=NEW.cleanup_admission_id FOR UPDATE;
    -- The intent is immutable. Read it before taking the same retry/charge
    -- lock order as the application store; no intent row lock is needed.
    SELECT * INTO intent FROM public.vm_pre_ssh_stop_intents
     WHERE cleanup_admission_id=NEW.cleanup_admission_id;
    SELECT * INTO retry FROM public.vm_creation_retries
     WHERE request_id=intent.creation_request_id FOR UPDATE;
    SELECT * INTO reservation FROM public.vm_resource_reservations
     WHERE id=intent.reservation_id FOR UPDATE;
    IF intent.frozen->>'kind'='vm_initial_ready_positive_stop_candidate_v1' THEN
        IF intent.initial_ready_xact_id IS NULL OR intent.initial_ready_xact_id=pg_current_xact_id() THEN
            RAISE EXCEPTION 'Initial Ready proof requires committed stop intent' USING ERRCODE='23514';
        END IF;
        NEW.initial_ready_xact_id := pg_current_xact_id();
    ELSIF intent.frozen->>'kind'='vm_job_never_app_ready_retained_stop_candidate_v1' THEN
        IF intent.held_stop_xact_id IS NULL OR intent.held_stop_xact_id=pg_current_xact_id() THEN
            RAISE EXCEPTION 'Held stop proof requires committed stop intent' USING ERRCODE='23514';
        END IF;
        NEW.held_stop_xact_id := pg_current_xact_id();
    END IF;
    IF intent.cleanup_admission_id IS NULL
       OR intent.job_id IS DISTINCT FROM NEW.job_id
       OR intent.provision_generation IS DISTINCT FROM NEW.provision_generation
       OR intent.frozen_digest IS DISTINCT FROM NEW.frozen_digest
       OR admission.id IS NULL OR admission.completed_at IS NOT NULL
       OR admission.owner_kind IS DISTINCT FROM 'job'
       OR admission.owner_id IS DISTINCT FROM intent.job_id
       OR admission.pvc_uid IS DISTINCT FROM intent.pvc_uid
       OR admission.intent_digest IS DISTINCT FROM intent.cleanup_intent_digest
       OR retry.request_id IS NULL
       OR retry.job_id IS DISTINCT FROM intent.job_id
       OR retry.provision_generation IS DISTINCT FROM intent.provision_generation
       OR retry.state IS DISTINCT FROM 'succeeded'
       OR (CASE WHEN intent.frozen->>'kind'='vm_initial_ready_positive_stop_candidate_v1'
           THEN retry.ready_at IS NULL OR NOT public.vm_initial_ready_stop_authorized(intent)
               OR NOT public.valid_vm_initial_ready_positive_stop(intent.frozen,NEW.terminal_evidence,NEW.frozen_digest)
           WHEN intent.frozen->>'kind'='vm_job_never_app_ready_retained_stop_candidate_v1'
           THEN retry.ready_at IS NOT NULL OR NOT public.vm_never_app_ready_stop_authorized(intent)
               OR NOT public.valid_vm_initial_ready_positive_stop(intent.frozen,
                   jsonb_set(NEW.terminal_evidence,'{kind}','"vm_initial_ready_positive_stop_v1"'::jsonb),NEW.frozen_digest)
           ELSE retry.ready_at IS NOT NULL OR intent.frozen->>'kind' IS DISTINCT FROM 'vm_pre_ssh_stop_candidate_v1' END)
       OR retry.observed_vm_uid IS DISTINCT FROM intent.vm_uid
       OR retry.observed_pvc_uid IS DISTINCT FROM intent.pvc_uid
       OR reservation.id IS NULL OR reservation.state IS DISTINCT FROM 'teardown'
       OR reservation.request_id IS DISTINCT FROM intent.creation_request_id
       OR reservation.revision IS DISTINCT FROM intent.reservation_revision
       OR reservation.vm_uid IS DISTINCT FROM intent.vm_uid
       OR reservation.vmi_uid IS DISTINCT FROM intent.vmi_uid
       OR reservation.launcher_uid IS DISTINCT FROM intent.launcher_uid
       OR reservation.node_uid IS DISTINCT FROM intent.node_uid
       OR owner_vm->>'status' IS DISTINCT FROM 'retiring_process_zero'
       OR owner_vm->>'provision_generation' IS DISTINCT FROM NEW.provision_generation::text
       OR NEW.terminal_evidence->>'kind' IS DISTINCT FROM (CASE WHEN intent.frozen->>'kind'='vm_initial_ready_positive_stop_candidate_v1'
           THEN 'vm_initial_ready_positive_stop_v1'
           WHEN intent.frozen->>'kind'='vm_job_never_app_ready_retained_stop_candidate_v1'
           THEN 'vm_job_never_app_ready_retained_positive_stop_v1'
           ELSE 'vm_pre_ssh_positive_stop_v1' END)
       OR NEW.terminal_evidence->>'frozen_digest' IS DISTINCT FROM NEW.frozen_digest
       OR NEW.terminal_evidence->>'vm_run_strategy' IS DISTINCT FROM 'Halted'
       OR NEW.terminal_evidence->>'vm_generation' IS DISTINCT FROM
          ((intent.frozen->>'vm_generation')::bigint+1)::text
       OR NEW.terminal_evidence->>'node_ready' IS DISTINCT FROM 'true'
       OR NEW.terminal_evidence->>'same_generation_replacement' IS DISTINCT FROM 'false'
       OR NEW.terminal_evidence->>'pod_finalizer' IS DISTINCT FROM 'srw.io/vm-pre-ssh-positive-stop'
       OR NEW.terminal_evidence->>'pod_intent_digest' IS DISTINCT FROM NEW.frozen_digest
       OR NEW.terminal_evidence->>'vm_uid' IS DISTINCT FROM intent.vm_uid::text
       OR NEW.terminal_evidence->>'launcher_uid' IS DISTINCT FROM intent.launcher_uid::text
       OR NEW.terminal_evidence->>'node_uid' IS DISTINCT FROM intent.node_uid::text
       OR NEW.terminal_evidence->>'controller_authenticated' IS DISTINCT FROM 'true'
       OR NEW.evidence_digest IS DISTINCT FROM
          'sha256:'||encode(sha256(convert_to(NEW.terminal_evidence::text,'UTF8')),'hex') THEN
        RAISE EXCEPTION 'VM pre-SSH stop proof lacks exact current authority'
            USING ERRCODE='23514', CONSTRAINT='vm_pre_ssh_stop_proof_authority';
    END IF;
    RETURN NEW;
END;
$$;



-- Preserve the original settled predicate; add an exact committed typed-proof fence.
CREATE OR REPLACE FUNCTION public.vm_job_cancel_retention_settled(parent_id uuid)
RETURNS boolean LANGUAGE plpgsql AS $body$
DECLARE a public.vm_job_cancel_retention_authorities%ROWTYPE;
        c public.vm_workspace_cleanup_admissions%ROWTYPE;
        i public.vm_pre_ssh_stop_intents%ROWTYPE;
        s public.vm_resource_cleanup_stop_receipts%ROWTYPE;
        v public.vm_resource_reservations%ROWTYPE;
        retained jsonb;
        expected jsonb;
        digest text;
BEGIN
    SELECT * INTO a FROM public.vm_job_cancel_retention_authorities WHERE cleanup_admission_id=parent_id;
    IF a.cleanup_admission_id IS NULL THEN RETURN false; END IF;
    PERFORM public.validate_vm_job_cancel_retention(parent_id,false);
    SELECT * INTO c FROM public.vm_workspace_cleanup_admissions WHERE id=parent_id;
    SELECT * INTO i FROM public.vm_pre_ssh_stop_intents WHERE cleanup_admission_id=parent_id;
    SELECT * INTO s FROM public.vm_resource_cleanup_stop_receipts WHERE cleanup_admission_id=parent_id;
    SELECT * INTO v FROM public.vm_resource_reservations WHERE id=a.reservation_id;
    retained := jsonb_build_object('version',1,'kind','vm_retained_rootdisk_v1',
        'namespace',a.namespace,'owner_kind','job','owner_id',a.job_id,
        'pvc_name',i.retention_preflight->>'pvc_name','pvc_uid',a.pvc_uid,
        'dv_uid',i.retention_preflight->>'dv_uid','ownership','standalone_dv',
        'deleting',false,'no_consumers',true);
    expected := jsonb_build_object('version',1,'kind','vm_cleanup_physical_stop',
        'job_id',a.job_id,'provision_generation',a.provision_generation,'vm_uid',a.vm_uid,
        'vmi_uid',a.vmi_uid,'launcher_uid',a.launcher_uid,'pvc_uid',a.pvc_uid,
        'vm_absent',true,'vmi_absent',true,'launcher_absent',true,'same_generation_replacement',false,
        'pvc_disposition','retained','controller_authenticated',true,'retained_rootdisk',retained);
    digest := 'sha256:'||encode(sha256(convert_to(expected::text,'UTF8')),'hex');
    RETURN c.completed_at IS NOT NULL AND c.outcome='completed'
       AND i.cleanup_admission_id IS NOT NULL AND i.retention_preflight IS NOT NULL
       AND s.cleanup_admission_id IS NOT NULL AND s.stop_evidence=expected
       AND s.reservation_id=a.reservation_id AND s.request_id=a.creation_request_id
       AND s.intent_digest=a.intent_digest AND s.job_id=a.job_id
       AND s.provision_generation=a.provision_generation
       AND s.vm_uid=a.vm_uid AND s.vmi_uid=a.vmi_uid AND s.launcher_uid=a.launcher_uid AND s.pvc_uid=a.pvc_uid
       AND v.state='released' AND v.request_id=a.creation_request_id AND v.revision=a.reservation_revision
       AND v.release_evidence=jsonb_build_object('kind','exact_cleanup_compute_absent',
           'cleanup_admission_id',parent_id,'job_id',a.job_id,'provision_generation',a.provision_generation,
           'vm_uid',a.vm_uid,'vmi_uid',a.vmi_uid,'launcher_uid',a.launcher_uid,
           'pvc_uid',a.pvc_uid,'stop_evidence_digest',digest)
       AND EXISTS (SELECT 1 FROM public.vm_pre_ssh_stop_proofs p WHERE p.cleanup_admission_id=parent_id
           AND p.job_id=a.job_id AND p.provision_generation=a.provision_generation AND p.frozen_digest=i.frozen_digest)
       AND EXISTS (SELECT 1 FROM public.managed_repository_process_zero_receipts z
           WHERE z.owner_kind='job' AND z.owner_id=a.job_id AND z.scope='vm' AND z.provisioner='vm'
           AND z.runtime_incarnation=a.provision_generation::text)
       AND (i.frozen->>'kind' IS DISTINCT FROM 'vm_job_never_app_ready_retained_stop_candidate_v1'
           OR (i.held_stop_xact_id IS NOT NULL
               AND EXISTS (SELECT 1 FROM public.vm_pre_ssh_stop_proofs p
                   WHERE p.cleanup_admission_id=parent_id
                     AND p.held_stop_xact_id IS NOT NULL
                     AND p.held_stop_xact_id<>i.held_stop_xact_id
                     AND p.frozen_digest=i.frozen_digest
                     AND p.terminal_evidence->>'kind'='vm_job_never_app_ready_retained_positive_stop_v1')
               AND EXISTS (SELECT 1 FROM public.managed_repository_process_zero_receipts z
                   WHERE z.owner_kind='job' AND z.owner_id=a.job_id AND z.scope='vm'
                     AND z.provisioner='vm' AND z.runtime_incarnation=a.provision_generation::text
                     AND (i.prior_zero_receipt_id IS NULL OR z.id=i.prior_zero_receipt_id))));
END;
$body$;

-- A later completed typed purge discharges storage, without modifying history.

COMMIT;
