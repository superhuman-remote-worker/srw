-- migration: 0340_vm_job_ready_cancel_retention.sql
-- description: Initial Ready Job Cancel retains exact custody and positive stop authority.
-- depends-on: 0339_vm_job_retained_resume.sql
-- transactional: yes
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE public.vm_job_cancel_retention_authorities
    DROP CONSTRAINT vm_job_cancel_retention_policy,
    ADD CONSTRAINT vm_job_cancel_retention_policy CHECK (
        (policy_version=1 AND job_retained_resume_id IS NULL AND ready_retention_preflight IS NULL)
        OR (policy_version=2 AND job_retained_resume_id IS NOT NULL)
        OR (policy_version=3 AND job_retained_resume_id IS NULL AND ready_retention_preflight IS NOT NULL)
    ) NOT VALID;
CREATE OR REPLACE FUNCTION public.vm_job_initial_ready_retention_candidate(
    owner uuid, generation uuid, expected_vm uuid, pvc uuid, old_parent uuid,
    retaining_parent uuid DEFAULT NULL, existing_stop boolean DEFAULT false
) RETURNS jsonb LANGUAGE plpgsql AS $body$
DECLARE j public.jobs%ROWTYPE;
        r public.vm_creation_retries%ROWTYPE;
        v public.vm_resource_reservations%ROWTYPE;
        q public.run_queue%ROWTYPE;
        vm jsonb;
BEGIN
    SELECT * INTO q FROM public.run_queue WHERE unit_id=owner FOR UPDATE;
    SELECT * INTO j FROM public.jobs WHERE id=owner FOR UPDATE;
    SELECT * INTO r FROM public.vm_creation_retries
     WHERE owner_kind='job' AND job_id=owner AND provision_generation=generation FOR UPDATE;
    SELECT * INTO v FROM public.vm_resource_reservations WHERE request_id=r.request_id FOR UPDATE;
    vm := j.context->'vm';
    IF j.id IS NULL OR j.status IS DISTINCT FROM 'cancelled'
       OR j.context->'_stateless_cancel_cleanup_pending' IS DISTINCT FROM 'true'::jsonb
       OR j.execution_lane IS DISTINCT FROM 'stateless'
       OR j.assigned_agent_id IS NOT NULL OR j.parent_job_id IS NOT NULL
       OR COALESCE(j.context->'inherits_parent_workspace','false'::jsonb) IS DISTINCT FROM 'false'::jsonb
       OR jsonb_typeof(vm) IS DISTINCT FROM 'object'
       OR vm->>'provision_generation' IS DISTINCT FROM generation::text
       OR vm->>'vm_uid' IS DISTINCT FROM expected_vm::text
       OR vm->>'rootdisk_pvc_uid' IS DISTINCT FROM pvc::text
       OR vm->>'status' IS NULL OR vm->>'status' NOT IN ('created','ssh_pending','ssh_unreachable','ready','retiring_process_zero')
       OR vm->'identity_authenticated' IS DISTINCT FROM 'true'::jsonb
       OR vm->>'identity_provision_generation' IS DISTINCT FROM generation::text
       OR COALESCE(vm->'workspace_storage','null'::jsonb)<>'null'::jsonb
       OR COALESCE(r.canonical_request->'workspace_storage','null'::jsonb)<>'null'::jsonb
       OR q.unit_id IS NULL OR q.unit_kind IS DISTINCT FROM 'worker_batch'
       OR q.state IS DISTINCT FROM 'done' OR q.leased_by IS NOT NULL OR q.leased_until IS NOT NULL
       OR r.request_id IS NULL OR r.state IS DISTINCT FROM 'succeeded'
       OR vm->>'creation_request_id' IS DISTINCT FROM r.request_id::text
       OR r.reason IS DISTINCT FROM 'creation_adopted' OR r.resolved_at IS NULL
       OR r.claim_token IS NOT NULL OR r.claim_expires_at IS NOT NULL
       OR r.ready_at IS NULL OR r.job_retained_resume_id IS NOT NULL
       OR (old_parent IS NOT NULL AND NOT EXISTS (
           SELECT 1 FROM public.vm_workspace_cleanup_admissions old
           WHERE old.id=old_parent AND r.ready_at<=old.admitted_at))
       OR j.context ? '_vm_job_retained_resume'
       OR r.observed_vm_uid IS DISTINCT FROM expected_vm OR r.observed_pvc_uid IS DISTINCT FROM pvc
       OR r.controller_configuration->'version' IS DISTINCT FROM '3'::jsonb
       OR r.controller_configuration->'persistent_rootdisk' IS DISTINCT FROM 'true'::jsonb
       OR r.controller_configuration->'headscale_enabled' IS DISTINCT FROM 'false'::jsonb
       OR v.id IS NULL OR v.resource_version IS DISTINCT FROM 2
       OR v.state NOT IN ('reserved','active','warm','teardown')
       OR v.vm_uid IS DISTINCT FROM expected_vm OR v.vmi_uid IS NULL OR v.launcher_uid IS NULL
       OR vm->>'vmi_uid' IS DISTINCT FROM v.vmi_uid::text
       OR (vm->>'active_pod_uid' IS NOT NULL AND vm->>'active_pod_uid'<>v.launcher_uid::text)
       OR r.controller_configuration->'resource_admission'->>'cluster_id' IS DISTINCT FROM v.cluster_id
       OR COALESCE(r.controller_configuration->>'namespace','') !~ '^[a-z0-9]([-a-z0-9]*[a-z0-9])?$'
       OR length(r.controller_configuration->>'namespace')>63
       OR EXISTS (SELECT 1 FROM public.srw_execution_specs e
           JOIN public.srw_execution_workspace_bindings b ON b.execution_id=e.id
           WHERE e.work_kind='Job' AND e.work_id=owner)
       OR EXISTS (SELECT 1 FROM public.srw_workspace_instances w WHERE w.pvc_uid=pvc::text)
       OR EXISTS (SELECT 1 FROM public.vm_resource_recovery_successors s WHERE s.reservation_id=v.id)
       OR EXISTS (SELECT 1 FROM public.vm_creation_retries later WHERE later.owner_kind='job'
           AND later.job_id=owner AND (later.created_at,later.request_id)>(r.created_at,r.request_id))
       OR EXISTS (SELECT 1 FROM public.vm_creation_effects e WHERE e.request_id=r.request_id AND e.state='issued')
       OR EXISTS (SELECT 1 FROM public.agents a WHERE a.current_job_id=owner AND a.status NOT IN ('offline','failed','completed'))
       OR EXISTS (SELECT 1 FROM public.jobs other WHERE other.id<>owner AND
           (other.context->'vm'->>'inherited_from_job_id'=owner::text
            OR other.context->'vm'->>'rootdisk_pvc_uid'=pvc::text))
       OR EXISTS (SELECT 1 FROM public.vm_idle_access_leases l WHERE l.owner_kind='job' AND l.owner_id=owner
           AND l.closed_at IS NULL AND l.expires_at>clock_timestamp())
       OR EXISTS (SELECT 1 FROM public.vm_remote_operation_leases l WHERE l.owner_kind='job' AND l.owner_id=owner
           AND l.settled_at IS NULL AND l.lease_expires_at>clock_timestamp())
       OR EXISTS (SELECT 1 FROM public.vm_idle_operations o WHERE o.owner_kind='job' AND o.owner_id=owner AND o.closed_at IS NULL)
       OR EXISTS (SELECT 1 FROM public.vm_workspace_recoveries h WHERE h.resolved_at IS NULL
           AND ((h.owner_kind='job' AND h.owner_id=owner) OR h.root_pvc_uid=pvc))
       OR EXISTS (SELECT 1 FROM public.vm_workspace_recovery_retention_pins p WHERE p.pvc_uid=pvc AND p.released_at IS NULL)
       OR EXISTS (SELECT 1 FROM public.vm_workspace_recovery_jobs h WHERE h.job_id=owner AND h.resolved_at IS NULL)
       OR EXISTS (SELECT 1 FROM public.vm_workspace_cleanup_admissions c
           WHERE ((c.owner_kind='job' AND c.owner_id=owner) OR c.pvc_uid=pvc)
           AND c.completed_at IS NULL AND c.id IS DISTINCT FROM old_parent
           AND c.id IS DISTINCT FROM retaining_parent)
       OR EXISTS (SELECT 1 FROM public.vm_workspace_cleanup_admissions c WHERE
           c.parent_admission_id=old_parent OR c.parent_admission_id=retaining_parent)
       OR EXISTS (SELECT 1 FROM public.vm_pre_ssh_stop_intents i WHERE i.job_id=owner
           AND i.provision_generation=generation
           AND (NOT existing_stop OR i.cleanup_admission_id IS DISTINCT FROM retaining_parent))
       OR (NOT existing_stop AND EXISTS (SELECT 1 FROM public.managed_repository_process_zero_receipts z
           WHERE z.owner_kind='job' AND z.owner_id=owner AND z.scope='vm'
           AND z.provisioner='vm' AND z.runtime_incarnation=generation::text))
       OR (j.context ? '_job_terminal_vm_cleanup' AND (
           j.context->'_job_terminal_vm_cleanup'->>'version'='1'
           AND j.context->'_job_terminal_vm_cleanup'->>'provision_generation'=generation::text
           AND j.context->'_job_terminal_vm_cleanup'->>'admission_id'
               IN (old_parent::text,retaining_parent::text)) IS DISTINCT FROM true) THEN
        RETURN NULL;
    END IF;
    RETURN jsonb_build_object('creation_request_id',r.request_id,'reservation_id',v.id,
        'reservation_revision',v.revision,'vmi_uid',v.vmi_uid,'launcher_uid',v.launcher_uid,
        'node_uid',v.node_uid,'namespace',r.controller_configuration->>'namespace','cluster_id',v.cluster_id);
END;
$body$;

CREATE OR REPLACE FUNCTION public.validate_vm_job_cancel_retention(parent_id uuid, fresh boolean DEFAULT false)
RETURNS boolean LANGUAGE plpgsql AS $body$
DECLARE a public.vm_job_cancel_retention_authorities%ROWTYPE;
        c public.vm_workspace_cleanup_admissions%ROWTYPE;
        old public.vm_workspace_cleanup_admissions%ROWTYPE;
        expected jsonb;
        digest text;
        old_digest text;
        original_request uuid;
        expected_request uuid;
        candidate jsonb;
BEGIN
    SELECT * INTO a FROM public.vm_job_cancel_retention_authorities WHERE cleanup_admission_id=parent_id;
    IF a.cleanup_admission_id IS NULL THEN RETURN false; END IF;
    SELECT * INTO c FROM public.vm_workspace_cleanup_admissions WHERE id=parent_id FOR SHARE;
    expected := jsonb_build_object('owner_id',a.job_id,'owner_kind','job',
        'provision_generation',a.provision_generation,'purge_disk',false,'pvc_uid',a.pvc_uid,
        'resource','vm_workspace','source','job_terminal_vm_release','vm_uid',a.vm_uid);
    digest := 'sha256:'||encode(sha256(convert_to(format(
        '{"owner_id":"%s","owner_kind":"job","provision_generation":"%s","purge_disk":false,"pvc_uid":"%s","resource":"vm_workspace","source":"job_terminal_vm_release","vm_uid":"%s"}',
        a.job_id,a.provision_generation,a.pvc_uid,a.vm_uid),'UTF8')),'hex');
    old_digest := 'sha256:'||encode(sha256(convert_to(format(
        '{"owner_id":"%s","owner_kind":"job","provision_generation":"%s","purge_disk":true,"pvc_uid":"%s","resource":"vm_workspace","source":"job_terminal_vm_release","vm_uid":"%s"}',
        a.job_id,a.provision_generation,a.pvc_uid,a.vm_uid),'UTF8')),'hex');
    original_request := public.uuid_generate_v5(public.uuid_ns_url(),
        'vm-workspace-cleanup:job_terminal_vm_release:job:'||a.job_id||':'||
        a.provision_generation||':'||a.vm_uid||':'||a.pvc_uid);
    expected_request := CASE WHEN a.superseded_admission_id IS NULL THEN original_request
        ELSE public.uuid_generate_v5(public.uuid_ns_url(),
            'vm-job-cancel-retain-v1:'||a.superseded_admission_id||':'||digest) END;
    IF c.id IS NULL OR c.owner_kind IS DISTINCT FROM 'job' OR c.owner_id IS DISTINCT FROM a.job_id
       OR c.pvc_uid IS DISTINCT FROM a.pvc_uid OR c.source IS DISTINCT FROM 'job_terminal_vm_release'
       OR c.parent_admission_id IS NOT NULL OR c.request_id IS DISTINCT FROM expected_request
       OR a.cleanup_request_id IS DISTINCT FROM expected_request
       OR c.intent_digest IS DISTINCT FROM digest OR a.intent_digest IS DISTINCT FROM digest
       OR a.retaining_intent IS DISTINCT FROM expected
       OR (fresh AND c.completed_at IS NOT NULL)
       OR (c.completed_at IS NOT NULL AND c.outcome IS DISTINCT FROM 'completed') THEN
        RAISE EXCEPTION 'Cancel retention immutable identity changed' USING ERRCODE='23514';
    END IF;
    IF a.superseded_admission_id IS NOT NULL THEN
        SELECT * INTO old FROM public.vm_workspace_cleanup_admissions WHERE id=a.superseded_admission_id FOR SHARE;
        IF old.id IS NULL OR old.owner_kind IS DISTINCT FROM 'job' OR old.owner_id IS DISTINCT FROM a.job_id
           OR old.pvc_uid IS DISTINCT FROM a.pvc_uid OR old.source IS DISTINCT FROM c.source
           OR old.parent_admission_id IS NOT NULL OR old.request_id IS DISTINCT FROM original_request
           OR a.superseded_request_id IS DISTINCT FROM original_request
           OR old.intent_digest IS DISTINCT FROM old_digest OR a.superseded_intent_digest IS DISTINCT FROM old_digest
           OR old.completed_at IS NULL OR old.outcome IS DISTINCT FROM 'superseded_by_retention'
           OR EXISTS (SELECT 1 FROM public.vm_workspace_cleanup_admissions child WHERE child.parent_admission_id=old.id) THEN
            RAISE EXCEPTION 'Cancel retention supersession lacks unissued exact parent' USING ERRCODE='23514';
        END IF;
    END IF;
    IF c.completed_at IS NULL THEN
        IF a.policy_version=3 THEN
            candidate := public.vm_job_initial_ready_retention_candidate(
                a.job_id,a.provision_generation,a.vm_uid,a.pvc_uid,a.superseded_admission_id,a.cleanup_admission_id,NOT fresh);
        ELSIF a.policy_version=2 THEN
            candidate := public.vm_job_retained_stop_candidate(
                a.job_id,a.provision_generation,a.vm_uid,a.pvc_uid,a.superseded_admission_id,a.cleanup_admission_id,NOT fresh);
        ELSE
            candidate := public.vm_job_cancel_retention_candidate(
                a.job_id,a.provision_generation,a.vm_uid,a.pvc_uid,a.superseded_admission_id,a.cleanup_admission_id,NOT fresh);
        END IF;
        IF candidate IS DISTINCT FROM jsonb_build_object('creation_request_id',a.creation_request_id,
            'reservation_id',a.reservation_id,'reservation_revision',a.reservation_revision,
            'vmi_uid',a.vmi_uid,'launcher_uid',a.launcher_uid,'node_uid',a.node_uid,
            'namespace',a.namespace,'cluster_id',a.cluster_id) THEN
            RAISE EXCEPTION 'Cancel retention current authority changed' USING ERRCODE='23514';
        END IF;
    END IF;
    RETURN true;
END;
$body$;

CREATE OR REPLACE FUNCTION public.vm_job_retained_ready_candidate(parent public.vm_job_cancel_retention_authorities)
RETURNS jsonb LANGUAGE sql IMMUTABLE AS $body$
    SELECT (jsonb_build_object('version',1,'kind',CASE WHEN parent.policy_version=3
        THEN 'vm_job_initial_ready_stop_candidate_v1' ELSE 'vm_job_retained_ready_stop_candidate_v1' END,
        'owner_kind','job','job_id',parent.job_id,'namespace',parent.namespace,'cluster_id',parent.cluster_id,
        'continuation_id',parent.job_retained_resume_id,'request_id',parent.creation_request_id,
        'provision_generation',parent.provision_generation,'reservation_id',parent.reservation_id,
        'reservation_revision',parent.reservation_revision,'vm_uid',parent.vm_uid,'vmi_uid',parent.vmi_uid,
        'launcher_uid',parent.launcher_uid,'node_uid',parent.node_uid,'pvc_uid',parent.pvc_uid,
        'cleanup_request_id',parent.cleanup_request_id,'cleanup_intent_digest',parent.intent_digest)
        - CASE WHEN parent.policy_version=3 THEN ARRAY['continuation_id'] ELSE ARRAY[]::text[] END);
$body$;

CREATE OR REPLACE FUNCTION public.guard_vm_job_retained_stop_authority() RETURNS trigger LANGUAGE plpgsql AS $body$
DECLARE r public.vm_creation_retries%ROWTYPE;
        op public.vm_job_retained_resumes%ROWTYPE;
        p jsonb := NEW.ready_retention_preflight;
BEGIN
    NEW.admitted_xact_id := pg_current_xact_id();
    IF NEW.policy_version=1 THEN RETURN NEW; END IF;
    SELECT * INTO r FROM public.vm_creation_retries WHERE request_id=NEW.creation_request_id;
    IF NEW.policy_version=3 THEN
        IF NEW.job_retained_resume_id IS NOT NULL OR r.job_retained_resume_id IS NOT NULL
           OR r.ready_at IS NULL OR r.request_id IS NULL
           OR r.job_id IS DISTINCT FROM NEW.job_id
           OR r.provision_generation IS DISTINCT FROM NEW.provision_generation
           OR r.observed_vm_uid IS DISTINCT FROM NEW.vm_uid
           OR r.observed_pvc_uid IS DISTINCT FROM NEW.pvc_uid
           OR p IS NULL OR p->>'dv_uid' IS NULL
           OR p->>'dv_uid' !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
           OR p->>'pvc_name' IS DISTINCT FROM 'agent-vm-'||NEW.job_id||'-rootdisk'
           OR p IS DISTINCT FROM jsonb_build_object('version',1,'kind','vm_job_initial_ready_preflight_v1',
               'stop_policy','initial_ready_cancel_v1','frozen',public.vm_job_retained_ready_candidate(NEW),
               'namespace',NEW.namespace,'owner_id',NEW.job_id,'pvc_name',p->>'pvc_name','pvc_uid',NEW.pvc_uid,
               'dv_uid',p->>'dv_uid','ownership','standalone_dv','deleting',false,
               'consumer_scope','exact_frozen_runtime_only') THEN
            RAISE EXCEPTION 'Initial Ready retention requires exact immutable preflight' USING ERRCODE='23514';
        END IF;
        RETURN NEW;
    END IF;
    SELECT * INTO op FROM public.vm_job_retained_resumes WHERE id=NEW.job_retained_resume_id;
    IF op.id IS NULL OR r.job_retained_resume_id IS DISTINCT FROM op.id
       OR r.request_id IS DISTINCT FROM op.request_id OR r.job_id IS DISTINCT FROM NEW.job_id
       OR op.provision_generation IS DISTINCT FROM NEW.provision_generation
       OR op.pvc_uid IS DISTINCT FROM NEW.pvc_uid
       OR (r.ready_at IS NULL AND p IS NOT NULL)
       OR (r.ready_at IS NOT NULL AND (
           p IS NULL OR p->>'dv_uid' IS NULL
           OR p->>'dv_uid' !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
           OR p->>'pvc_name' IS NULL OR length(p->>'pvc_name') NOT BETWEEN 1 AND 253
           OR p->>'pvc_name' !~ '^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$'
           OR p IS DISTINCT FROM jsonb_build_object('version',1,'kind','vm_job_retained_ready_preflight_v1',
               'stop_policy','retained_ready_continuation_v1','frozen',public.vm_job_retained_ready_candidate(NEW),
               'namespace',NEW.namespace,'owner_id',NEW.job_id,'pvc_name',p->>'pvc_name','pvc_uid',NEW.pvc_uid,
               'dv_uid',p->>'dv_uid','ownership','standalone_dv','deleting',false,
               'consumer_scope','exact_frozen_runtime_only'))) THEN
        RAISE EXCEPTION 'Retained continuation requires exact immutable stop preflight' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$body$;

CREATE OR REPLACE FUNCTION public.guard_vm_job_retained_resume() RETURNS trigger
LANGUAGE plpgsql AS $body$
DECLARE j public.jobs%ROWTYPE;
        a public.vm_job_cancel_retention_authorities%ROWTYPE;
        root public.vm_job_cancel_retention_authorities%ROWTYPE;
        r public.vm_creation_retries%ROWTYPE;
        q public.run_queue%ROWTYPE;
        previous public.vm_job_retained_resumes%ROWTYPE;
        terminal public.vm_job_retained_resume_terminals%ROWTYPE;
BEGIN
    IF TG_OP<>'INSERT' THEN
        RAISE EXCEPTION 'Retained Job Resume is append-only' USING ERRCODE='23514';
    END IF;
    NEW.admitted_xact_id := pg_current_xact_id();
    PERFORM pg_advisory_xact_lock(hashtextextended('workspace-recovery:job:'||NEW.job_id,0));
    PERFORM pg_advisory_xact_lock(hashtextextended('workspace-recovery-pvc:'||NEW.pvc_uid,0));
    SELECT * INTO q FROM public.run_queue WHERE unit_id=NEW.job_id FOR UPDATE;
    SELECT * INTO j FROM public.jobs WHERE id=NEW.job_id FOR UPDATE;
    SELECT * INTO a FROM public.vm_job_cancel_retention_authorities WHERE cleanup_admission_id=NEW.physical_cleanup_admission_id;
    SELECT * INTO root FROM public.vm_job_cancel_retention_authorities WHERE cleanup_admission_id=NEW.root_retention_admission_id;
    SELECT * INTO r FROM public.vm_creation_retries WHERE request_id=a.creation_request_id FOR SHARE;
    IF j.id IS NULL OR j.user_id IS DISTINCT FROM NEW.requested_by
       OR j.status NOT IN ('cancelled','failed','paused') OR j.execution_lane IS DISTINCT FROM 'stateless'
       OR j.assigned_agent_id IS NOT NULL OR j.parent_job_id IS NOT NULL
       OR COALESCE(j.context->'inherits_parent_workspace','false'::jsonb)<>'false'::jsonb
       OR j.context ?| ARRAY['_stateless_cancel_cleanup_pending','_stateless_delete_pending','_completion_control_claim','_worker_execution_hold']
       OR a.cleanup_admission_id IS NULL OR a.job_id IS DISTINCT FROM NEW.job_id
       OR a.pvc_uid IS DISTINCT FROM NEW.pvc_uid OR root.job_id IS DISTINCT FROM NEW.job_id
       OR root.pvc_uid IS DISTINCT FROM NEW.pvc_uid
       OR public.vm_job_cancel_retention_discharged(root.cleanup_admission_id)
       OR NOT public.vm_job_cancel_retention_settled(a.cleanup_admission_id)
       OR NEW.predecessor_request_id IS DISTINCT FROM a.creation_request_id
       OR NEW.predecessor_generation IS DISTINCT FROM a.provision_generation
       OR NEW.source_revision IS DISTINCT FROM r.revision
       OR r.request_id IS NULL OR r.state IS DISTINCT FROM 'succeeded'
       OR COALESCE(r.canonical_request->'workspace_storage','null'::jsonb)<>'null'::jsonb
       OR q.unit_id IS NULL OR q.unit_kind IS DISTINCT FROM 'worker_batch' OR q.state IS DISTINCT FROM 'done'
       OR q.leased_by IS NOT NULL OR q.leased_until IS NOT NULL
       OR NEW.retained_vm->>'status' IS DISTINCT FROM 'deleted'
       OR NEW.retained_vm->'identity_authenticated' IS DISTINCT FROM 'true'::jsonb
       OR NEW.retained_vm->>'provision_generation' IS DISTINCT FROM a.provision_generation::text
       OR NEW.retained_vm->>'identity_provision_generation' IS DISTINCT FROM a.provision_generation::text
       OR NEW.retained_vm->>'vm_uid' IS DISTINCT FROM a.vm_uid::text
       OR NEW.retained_vm->>'rootdisk_pvc_uid' IS DISTINCT FROM a.pvc_uid::text
       OR COALESCE(NEW.retained_vm->'workspace_storage','null'::jsonb)<>'null'::jsonb
       OR EXISTS (SELECT 1 FROM public.vm_creation_retries source WHERE source.request_id=NEW.request_id OR source.provision_generation=NEW.provision_generation)
       OR EXISTS (SELECT 1 FROM public.vm_workspace_cleanup_admissions c WHERE c.completed_at IS NULL
           AND ((c.owner_kind='job' AND c.owner_id=NEW.job_id) OR c.pvc_uid=NEW.pvc_uid))
       OR EXISTS (SELECT 1 FROM public.vm_workspace_recoveries recovery WHERE recovery.resolved_at IS NULL
           AND ((recovery.owner_kind='job' AND recovery.owner_id=NEW.job_id) OR recovery.root_pvc_uid=NEW.pvc_uid))
       OR EXISTS (SELECT 1 FROM public.vm_workspace_recovery_retention_pins pin WHERE pin.pvc_uid=NEW.pvc_uid AND pin.released_at IS NULL)
       OR EXISTS (SELECT 1 FROM public.vm_idle_access_leases lease WHERE lease.owner_kind='job' AND lease.owner_id=NEW.job_id
           AND lease.closed_at IS NULL AND lease.expires_at>clock_timestamp())
       OR EXISTS (SELECT 1 FROM public.vm_idle_operations idle WHERE idle.owner_kind='job' AND idle.owner_id=NEW.job_id AND idle.closed_at IS NULL)
       OR EXISTS (SELECT 1 FROM public.vm_resource_reservations charge JOIN public.vm_creation_retries source USING(request_id)
           WHERE source.owner_kind='job' AND source.job_id=NEW.job_id AND charge.state<>'released')
       OR EXISTS (SELECT 1 FROM public.vm_creation_effects effect JOIN public.vm_creation_retries source USING(request_id)
           WHERE source.owner_kind='job' AND source.job_id=NEW.job_id AND effect.state='issued') THEN
        RAISE EXCEPTION 'Retained Job Resume authority is unproven' USING ERRCODE='23514';
    END IF;
    IF NEW.predecessor_terminal_id IS NULL THEN
        IF NEW.physical_cleanup_admission_id IS DISTINCT FROM NEW.root_retention_admission_id
           OR NEW.retained_vm IS DISTINCT FROM j.context->'vm'
           OR j.context ? '_vm_job_retained_resume'
           OR EXISTS (SELECT 1 FROM public.vm_creation_retries later WHERE later.owner_kind='job' AND later.job_id=NEW.job_id
               AND (later.created_at,later.request_id)>(r.created_at,r.request_id)) THEN
            RAISE EXCEPTION 'Retained Job Resume predecessor changed' USING ERRCODE='23514';
        END IF;
    ELSE
        SELECT * INTO terminal FROM public.vm_job_retained_resume_terminals WHERE id=NEW.predecessor_terminal_id;
        SELECT * INTO previous FROM public.vm_job_retained_resumes WHERE id=terminal.resume_id;
        IF terminal.id IS NULL OR previous.job_id IS DISTINCT FROM NEW.job_id
           OR previous.root_retention_admission_id IS DISTINCT FROM NEW.root_retention_admission_id
           OR previous.pvc_uid IS DISTINCT FROM NEW.pvc_uid
           OR terminal.physical_cleanup_admission_id IS DISTINCT FROM NEW.physical_cleanup_admission_id
           OR j.context->>'_vm_job_retained_resume' IS DISTINCT FROM previous.id::text
           OR (terminal.terminal_kind<>'kept_compute' AND NEW.retained_vm IS DISTINCT FROM previous.retained_vm) THEN
            RAISE EXCEPTION 'Retained Job Resume terminal changed' USING ERRCODE='23514';
        END IF;
    END IF;
    RETURN NEW;
END;
$body$;

CREATE FUNCTION public.check_vm_job_initial_ready_stop_parent() RETURNS trigger
LANGUAGE plpgsql AS $body$
BEGIN
    IF NEW.owner_kind='job' AND NEW.source='job_terminal_vm_release'
       AND NEW.parent_admission_id IS NULL AND EXISTS (
           SELECT 1 FROM public.jobs j JOIN public.vm_creation_retries r
             ON r.owner_kind='job' AND r.job_id=j.id
            AND r.provision_generation::text=j.context->'vm'->>'provision_generation'
           WHERE j.id=NEW.owner_id AND j.status='cancelled'
             AND j.context->'_stateless_cancel_cleanup_pending'='true'::jsonb
             AND r.ready_at IS NOT NULL AND r.job_retained_resume_id IS NULL
             AND NOT j.context ? '_vm_job_retained_resume'
             AND r.observed_pvc_uid=NEW.pvc_uid)
       AND NOT EXISTS(SELECT 1 FROM public.vm_job_cancel_retention_authorities a
           WHERE (a.cleanup_admission_id=NEW.id OR a.superseded_admission_id=NEW.id)
             AND a.policy_version=3) THEN
        RAISE EXCEPTION 'Initial Ready stop bootstrap is incomplete' USING ERRCODE='23514';
    END IF;
    RETURN NULL;
END;
$body$;
CREATE CONSTRAINT TRIGGER vm_job_initial_ready_stop_parent_commit
AFTER INSERT ON public.vm_workspace_cleanup_admissions DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION public.check_vm_job_initial_ready_stop_parent();


ALTER TABLE public.vm_pre_ssh_stop_intents ADD COLUMN initial_ready_xact_id xid8;
ALTER TABLE public.vm_pre_ssh_stop_proofs ADD COLUMN initial_ready_xact_id xid8;

CREATE FUNCTION public.vm_initial_ready_stop_authorized(i public.vm_pre_ssh_stop_intents)
RETURNS boolean LANGUAGE plpgsql AS $body$
DECLARE a public.vm_job_cancel_retention_authorities%ROWTYPE;
        f jsonb := i.frozen;
        item jsonb;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended('workspace-recovery:job:'||i.job_id,0));
    PERFORM pg_advisory_xact_lock(hashtextextended('workspace-recovery-pvc:'||i.pvc_uid,0));
    PERFORM 1 FROM public.run_queue WHERE unit_id=i.job_id FOR UPDATE;
    PERFORM 1 FROM public.jobs WHERE id=i.job_id FOR UPDATE;
    PERFORM 1 FROM public.vm_workspace_cleanup_admissions WHERE id=i.cleanup_admission_id FOR UPDATE;
    SELECT * INTO a FROM public.vm_job_cancel_retention_authorities
     WHERE cleanup_admission_id=i.cleanup_admission_id;
    IF a.policy_version IS DISTINCT FROM 3 OR a.admitted_xact_id=pg_current_xact_id()
       OR a.job_retained_resume_id IS NOT NULL OR i.retention_preflight IS DISTINCT FROM a.ready_retention_preflight
       OR i.job_id IS DISTINCT FROM a.job_id OR i.provision_generation IS DISTINCT FROM a.provision_generation
       OR i.creation_request_id IS DISTINCT FROM a.creation_request_id
       OR i.reservation_id IS DISTINCT FROM a.reservation_id OR i.reservation_revision IS DISTINCT FROM a.reservation_revision
       OR i.vm_uid IS DISTINCT FROM a.vm_uid OR i.vmi_uid IS DISTINCT FROM a.vmi_uid
       OR i.launcher_uid IS DISTINCT FROM a.launcher_uid OR i.pvc_uid IS DISTINCT FROM a.pvc_uid
       OR i.node_uid IS DISTINCT FROM a.node_uid OR i.cleanup_intent_digest IS DISTINCT FROM a.intent_digest
       OR f->>'kind' IS DISTINCT FROM 'vm_initial_ready_positive_stop_candidate_v1'
       OR (SELECT array_agg(k ORDER BY k COLLATE "C") FROM jsonb_object_keys(f) k) IS DISTINCT FROM ARRAY['cleanup_admission_id','cleanup_intent_digest','cleanup_request_id','containers','job_id','kind','launcher_name','launcher_resource_version','launcher_uid','namespace','node_name','node_uid','provision_generation','pvc_uid','vm_generation','vm_name','vm_resource_version','vm_uid','vmi_uid']
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
    FOREACH item IN ARRAY ARRAY[f->'launcher_name',f->'node_name',f->'vm_resource_version',f->'launcher_resource_version'] LOOP
        IF jsonb_typeof(item) IS DISTINCT FROM 'string' OR item#>>'{}' !~ '^[^[:space:]]+$' THEN RETURN false; END IF;
    END LOOP;
    FOR item IN SELECT value FROM jsonb_array_elements(f->'containers') LOOP
        IF (SELECT array_agg(k ORDER BY k COLLATE "C") FROM jsonb_object_keys(item) k) IS DISTINCT FROM ARRAY['container_id','kind','name']
           OR item->>'kind' NOT IN ('regular','init')
           OR jsonb_typeof(item->'name') IS DISTINCT FROM 'string' OR item->>'name' !~ '^[^[:space:]]+$'
           OR jsonb_typeof(item->'container_id') IS DISTINCT FROM 'string' OR item->>'container_id' !~ '^[^[:space:]]+$' THEN
            RETURN false;
        END IF;
    END LOOP;
    RETURN public.validate_vm_job_cancel_retention(i.cleanup_admission_id,false);
END;
$body$;

CREATE FUNCTION public.valid_vm_initial_ready_positive_stop(f jsonb, p jsonb, digest text)
RETURNS boolean LANGUAGE plpgsql IMMUTABLE AS $body$
DECLARE item jsonb;
        expected jsonb;
BEGIN
    IF jsonb_typeof(p) IS DISTINCT FROM 'object'
       OR (SELECT array_agg(k ORDER BY k COLLATE "C") FROM jsonb_object_keys(p) k) IS DISTINCT FROM
          ARRAY['containers','controller_authenticated','frozen_digest','kind','launcher_uid','node_ready','node_uid','pod_finalizer','pod_intent_digest','pod_terminal','same_generation_replacement','vm_generation','vm_run_strategy','vm_uid','vmi_disposition','vmi_uid']
       OR p->>'kind' IS DISTINCT FROM 'vm_initial_ready_positive_stop_v1'
       OR p->>'frozen_digest' IS DISTINCT FROM digest OR p->>'pod_intent_digest' IS DISTINCT FROM digest
       OR p->>'pod_finalizer' IS DISTINCT FROM 'srw.io/vm-pre-ssh-positive-stop'
       OR p->>'vm_run_strategy' IS DISTINCT FROM 'Halted'
       OR p->'vm_generation' IS DISTINCT FROM to_jsonb((f->>'vm_generation')::bigint+1)
       OR p->'node_ready' IS DISTINCT FROM 'true'::jsonb OR p->'same_generation_replacement' IS DISTINCT FROM 'false'::jsonb
       OR p->'controller_authenticated' IS DISTINCT FROM 'true'::jsonb
       OR p->>'vmi_disposition' IS NULL OR p->>'vmi_disposition' NOT IN ('absent','terminal')
       OR p->'vm_uid' IS DISTINCT FROM f->'vm_uid' OR p->'vmi_uid' IS DISTINCT FROM f->'vmi_uid'
       OR p->'launcher_uid' IS DISTINCT FROM f->'launcher_uid' OR p->'node_uid' IS DISTINCT FROM f->'node_uid'
       OR p->'pod_terminal' NOT IN ('{"phase":"Failed","restart_policy":"Never"}'::jsonb,'{"phase":"Succeeded","restart_policy":"Never"}'::jsonb)
       OR p->'pod_terminal' IS NULL OR jsonb_typeof(p->'containers') IS DISTINCT FROM 'array'
       OR jsonb_array_length(p->'containers')<>jsonb_array_length(f->'containers')
       OR (SELECT count(*)<>count(DISTINCT c->>'name') FROM jsonb_array_elements(p->'containers') c) THEN
        RETURN false;
    END IF;
    FOR item IN SELECT value FROM jsonb_array_elements(p->'containers') LOOP
        SELECT c INTO expected FROM jsonb_array_elements(f->'containers') c WHERE c->'kind'=item->'kind' AND c->'name'=item->'name';
        IF expected IS NULL OR (SELECT array_agg(k ORDER BY k COLLATE "C") FROM jsonb_object_keys(item) k) IS DISTINCT FROM
           ARRAY['container_id','finished_at','kind','last_state','name','reason','restart_count','state','terminated_container_id']
           OR item->'container_id' IS DISTINCT FROM expected->'container_id'
           OR item->'terminated_container_id' IS DISTINCT FROM expected->'container_id'
           OR item->'restart_count' IS DISTINCT FROM '0'::jsonb OR item->>'state' IS DISTINCT FROM 'terminated'
           OR item->'last_state' IS DISTINCT FROM 'null'::jsonb
           OR jsonb_typeof(item->'reason') IS DISTINCT FROM 'string' OR item->>'reason' !~ '^[^[:space:]]+$'
           OR item->>'reason'='ContainerStatusUnknown'
           OR jsonb_typeof(item->'finished_at') IS DISTINCT FROM 'string'
           OR item->>'finished_at' !~ '(Z|[+-][0-9]{2}:[0-9]{2})$' THEN RETURN false; END IF;
        PERFORM (item->>'finished_at')::timestamptz;
    END LOOP;
    RETURN true;
EXCEPTION WHEN invalid_datetime_format OR datetime_field_overflow OR invalid_text_representation THEN RETURN false;
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
    IF NEW.frozen->>'kind'='vm_initial_ready_positive_stop_candidate_v1' THEN
    NEW.initial_ready_xact_id := pg_current_xact_id();
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
       OR EXISTS (
           SELECT 1 FROM public.managed_repository_process_zero_receipts z
           WHERE z.owner_kind='job' AND z.owner_id=NEW.job_id
             AND z.scope='vm' AND z.provisioner='vm'
             AND z.runtime_incarnation=NEW.provision_generation::text)
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
    IF EXISTS(SELECT 1 FROM public.vm_pre_ssh_stop_intents i WHERE i.cleanup_admission_id=NEW.cleanup_admission_id AND i.frozen->>'kind'='vm_initial_ready_positive_stop_candidate_v1') THEN
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
           THEN 'vm_initial_ready_positive_stop_v1' ELSE 'vm_pre_ssh_positive_stop_v1' END)
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

CREATE OR REPLACE FUNCTION public.guard_vm_job_cancel_retention_preflight() RETURNS trigger
LANGUAGE plpgsql AS $body$
DECLARE a public.vm_job_cancel_retention_authorities%ROWTYPE;
        p jsonb := NEW.retention_preflight;
BEGIN
    SELECT * INTO a FROM public.vm_job_cancel_retention_authorities WHERE cleanup_admission_id=NEW.cleanup_admission_id;
    IF a.cleanup_admission_id IS NULL THEN RETURN NEW; END IF;
    IF a.policy_version=3 THEN
        IF NOT public.vm_initial_ready_stop_authorized(NEW) THEN
            RAISE EXCEPTION 'Initial Ready stop requires committed exact custody authority' USING ERRCODE='23514';
        END IF;
        RETURN NEW;
    END IF;
    IF p IS NULL OR p->>'dv_uid' IS NULL OR p->>'dv_uid' !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
       OR p->>'pvc_name' IS NULL OR length(p->>'pvc_name') NOT BETWEEN 1 AND 253
       OR p->>'pvc_name' !~ '^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$'
       OR p IS DISTINCT FROM jsonb_build_object('version',1,'kind','vm_cancel_retention_preflight_v1',
           'stop_policy','cancel_retention_v1','frozen',NEW.frozen,'namespace',a.namespace,
           'owner_id',a.job_id,'pvc_name',p->>'pvc_name','pvc_uid',a.pvc_uid,'dv_uid',p->>'dv_uid',
           'ownership','standalone_dv','deleting',false,'consumer_scope','exact_frozen_runtime_only')
       OR NEW.job_id IS DISTINCT FROM a.job_id OR NEW.provision_generation IS DISTINCT FROM a.provision_generation
       OR NEW.creation_request_id IS DISTINCT FROM a.creation_request_id
       OR NEW.reservation_id IS DISTINCT FROM a.reservation_id OR NEW.reservation_revision IS DISTINCT FROM a.reservation_revision
       OR NEW.vm_uid IS DISTINCT FROM a.vm_uid OR NEW.vmi_uid IS DISTINCT FROM a.vmi_uid
       OR NEW.launcher_uid IS DISTINCT FROM a.launcher_uid OR NEW.pvc_uid IS DISTINCT FROM a.pvc_uid
       OR NEW.node_uid IS DISTINCT FROM a.node_uid OR NEW.cleanup_intent_digest IS DISTINCT FROM a.intent_digest
       OR NEW.frozen->>'namespace' IS DISTINCT FROM a.namespace THEN
        RAISE EXCEPTION 'Cancel retention requires exact pre-stop ownership proof' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$body$;

COMMIT;
