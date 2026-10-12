-- Pinned Ready Job Cancel retains its exact standalone disk until typed owner Delete.


ALTER TABLE public.vm_job_cancel_retention_authorities
    DROP CONSTRAINT IF EXISTS vm_job_cancel_retention_policy,
    ADD CONSTRAINT vm_job_cancel_retention_policy CHECK (
       (policy_version=1 AND job_retained_resume_id IS NULL AND ready_retention_preflight IS NULL)
       OR (policy_version=2 AND job_retained_resume_id IS NOT NULL)
       OR (policy_version IN (3,4) AND job_retained_resume_id IS NULL
           AND ready_retention_preflight IS NOT NULL)) NOT VALID;

CREATE OR REPLACE FUNCTION public.vm_job_pinned_ready_retention_candidate(owner uuid, generation uuid, expected_vm uuid, pvc uuid, old_parent uuid, retaining_parent uuid DEFAULT NULL::uuid, existing_stop boolean DEFAULT false) RETURNS jsonb
    LANGUAGE plpgsql
    AS $_$
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
       OR j.context->'_job_terminal_vm_cleanup'->>'retention_policy' IS DISTINCT FROM '4'
       OR j.context->'_job_terminal_vm_cleanup'->>'source' IS DISTINCT FROM 'cancel'
       OR j.context->'_job_terminal_vm_cleanup'->>'version' IS DISTINCT FROM '1'
       OR j.execution_lane IS DISTINCT FROM 'pinned'
       OR j.assigned_agent_id IS NOT NULL OR j.parent_job_id IS NOT NULL
       OR COALESCE(j.context->'inherits_parent_workspace','false'::jsonb) IS DISTINCT FROM 'false'::jsonb
       OR jsonb_typeof(vm) IS DISTINCT FROM 'object'
       OR vm->>'provision_generation' IS DISTINCT FROM generation::text
       OR vm->>'vm_uid' IS DISTINCT FROM expected_vm::text
       OR vm->>'rootdisk_pvc_uid' IS DISTINCT FROM pvc::text
       OR vm->>'status' IS NULL OR (vm->>'status' NOT IN ('created','ssh_pending','ssh_unreachable','ready','retiring_process_zero')
           AND NOT (vm->>'status'='deleted' AND v.state='teardown'
               AND public.vm_job_deleted_retention_replay_allowed(4,owner,generation,
                   expected_vm,pvc,retaining_parent,existing_stop,r.request_id,
                   v.id,v.revision,v.vmi_uid,v.launcher_uid,v.node_uid)))
       OR vm->'identity_authenticated' IS DISTINCT FROM 'true'::jsonb
       OR vm->>'identity_provision_generation' IS DISTINCT FROM generation::text
       OR COALESCE(vm->'workspace_storage','null'::jsonb)<>'null'::jsonb
       OR COALESCE(r.canonical_request->'workspace_storage','null'::jsonb)<>'null'::jsonb
       OR (q.unit_id IS NOT NULL AND (q.state IS DISTINCT FROM 'done'
           OR q.leased_by IS NOT NULL OR q.leased_until IS NOT NULL))
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
       OR j.context->'_job_terminal_vm_cleanup'->>'provision_generation' IS DISTINCT FROM generation::text
       OR (j.context->'_job_terminal_vm_cleanup'->>'admission_id' IS NOT NULL
           AND j.context->'_job_terminal_vm_cleanup'->>'admission_id' IS DISTINCT FROM old_parent::text
           AND j.context->'_job_terminal_vm_cleanup'->>'admission_id' IS DISTINCT FROM retaining_parent::text) THEN
        RETURN NULL;
    END IF;
    RETURN jsonb_build_object('creation_request_id',r.request_id,'reservation_id',v.id,
        'reservation_revision',v.revision,'vmi_uid',v.vmi_uid,'launcher_uid',v.launcher_uid,
        'node_uid',v.node_uid,'namespace',r.controller_configuration->>'namespace','cluster_id',v.cluster_id);
END;
$_$;

-- Pending Cancel is a disk fence before the signed preflight and authority exist.
CREATE FUNCTION public.vm_job_pinned_cancel_retention_pending(kind text, owner uuid, pvc uuid)
RETURNS boolean LANGUAGE sql STABLE AS $body$
    SELECT EXISTS(SELECT 1 FROM public.jobs j
        WHERE j.execution_lane='pinned' AND j.status='cancelled'
          AND j.context->'_job_terminal_vm_cleanup'->>'retention_policy'='4'
          AND ((kind='job' AND j.id=owner)
               OR (pvc IS NOT NULL AND j.context->'vm'->>'rootdisk_pvc_uid'=pvc::text)));
$body$;

CREATE OR REPLACE FUNCTION public.vm_job_cancel_retention_cleanup_allowed(
    kind text, owner uuid, pvc uuid, cleanup_source text,
    cleanup_request uuid, digest text, parent_id uuid, provisional boolean DEFAULT false)
RETURNS boolean LANGUAGE plpgsql AS $body$
DECLARE j public.jobs%ROWTYPE;
        expected_digest text;
        original_request uuid;
BEGIN
    IF public.vm_job_pinned_cancel_retention_pending(kind,owner,pvc) THEN
        SELECT * INTO j FROM public.jobs WHERE id=owner;
        IF kind='job' AND j.execution_lane='pinned' AND j.status='cancelled'
           AND j.context->'_job_terminal_vm_cleanup'->>'retention_policy'='4'
           AND pvc::text=j.context->'vm'->>'rootdisk_pvc_uid'
           AND parent_id IS NULL AND cleanup_source='job_terminal_vm_release' THEN
            expected_digest := 'sha256:'||encode(sha256(convert_to(format(
                '{"owner_id":"%s","owner_kind":"job","provision_generation":"%s","purge_disk":false,"pvc_uid":"%s","resource":"vm_workspace","source":"job_terminal_vm_release","vm_uid":"%s"}',
                owner,j.context->'vm'->>'provision_generation',pvc,
                j.context->'vm'->>'vm_uid'),'UTF8')),'hex');
            original_request := public.uuid_generate_v5(public.uuid_ns_url(),
                'vm-workspace-cleanup:job_terminal_vm_release:job:'||owner||':'||
                (j.context->'vm'->>'provision_generation')||':'||
                (j.context->'vm'->>'vm_uid')||':'||pvc);
            IF digest=expected_digest AND (cleanup_request=original_request
                OR EXISTS(SELECT 1 FROM public.vm_workspace_cleanup_admissions old
                    WHERE old.owner_kind='job' AND old.owner_id=owner
                      AND old.pvc_uid=pvc AND old.source='job_terminal_vm_release'
                      AND old.outcome='superseded_by_retention'
                      AND cleanup_request=public.uuid_generate_v5(public.uuid_ns_url(),
                          'vm-job-cancel-retain-v1:'||old.id||':'||digest))) THEN
                -- Admission is provisional until the deferred parent check
                -- sees the exact policy-4 authority in this transaction.
                RETURN true;
            END IF;
            -- The exact old purge parent can be superseded in this transaction;
            -- the deferred check requires its matching immutable successor.
            IF provisional AND cleanup_request=original_request
               AND EXISTS(SELECT 1 FROM public.vm_workspace_cleanup_admissions old
                   WHERE old.owner_kind='job' AND old.owner_id=owner
                     AND old.pvc_uid=pvc AND old.request_id=cleanup_request
                     AND old.intent_digest=digest
                     AND old.outcome='superseded_by_retention'
                     AND NOT EXISTS(SELECT 1 FROM public.vm_workspace_cleanup_admissions child
                         WHERE child.parent_admission_id=old.id)) THEN RETURN true; END IF;
        END IF;
        IF kind='job' AND cleanup_source='public_vm_delete' AND parent_id IS NULL
           AND EXISTS(SELECT 1 FROM public.vm_job_cancel_retention_authorities a
               WHERE a.job_id=owner AND a.pvc_uid=pvc
                 AND public.vm_job_cancel_retention_settled(a.cleanup_admission_id)) THEN
            RETURN public.vm_job_cancel_retention_cleanup_allowed_v1(
                kind,owner,pvc,cleanup_source,cleanup_request,digest,parent_id,provisional);
        END IF;
        RETURN false;
    END IF;
    IF kind='job' AND cleanup_source='terminal_checkpoint_prune'
       AND EXISTS(SELECT 1 FROM public.vm_job_retained_resumes WHERE job_id=owner)
       AND NOT public.vm_job_retained_terminal_is_current(owner) THEN RETURN false; END IF;
    IF kind='job' AND cleanup_source='job_terminal_vm_release' AND parent_id IS NULL
       AND public.vm_job_retained_stop_allowed(owner,pvc,cleanup_request,digest) THEN RETURN true; END IF;
    IF kind='job' AND cleanup_source='controller_vm_create' AND parent_id IS NULL
       AND public.vm_job_retained_creation_allowed(owner,pvc,cleanup_request,digest) THEN RETURN true; END IF;
    RETURN public.vm_job_cancel_retention_cleanup_allowed_v1(
        kind,owner,pvc,cleanup_source,cleanup_request,digest,parent_id,provisional);
END;
$body$;

CREATE OR REPLACE FUNCTION public.validate_vm_job_cancel_retention(parent_id uuid, fresh boolean DEFAULT false) RETURNS boolean
    LANGUAGE plpgsql
    AS $$
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
        IF a.policy_version=4 THEN
            candidate := public.vm_job_pinned_ready_retention_candidate(
                a.job_id,a.provision_generation,a.vm_uid,a.pvc_uid,a.superseded_admission_id,a.cleanup_admission_id,NOT fresh);
        ELSIF a.policy_version=3 THEN
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
$$;

CREATE OR REPLACE FUNCTION public.vm_job_retained_ready_candidate(parent public.vm_job_cancel_retention_authorities) RETURNS jsonb
    LANGUAGE sql IMMUTABLE
    AS $$
    SELECT (jsonb_build_object('version',1,'kind',CASE WHEN parent.policy_version IN (3,4)
        THEN 'vm_job_initial_ready_stop_candidate_v1' ELSE 'vm_job_retained_ready_stop_candidate_v1' END,
        'owner_kind','job','job_id',parent.job_id,'namespace',parent.namespace,'cluster_id',parent.cluster_id,
        'continuation_id',parent.job_retained_resume_id,'request_id',parent.creation_request_id,
        'provision_generation',parent.provision_generation,'reservation_id',parent.reservation_id,
        'reservation_revision',parent.reservation_revision,'vm_uid',parent.vm_uid,'vmi_uid',parent.vmi_uid,
        'launcher_uid',parent.launcher_uid,'node_uid',parent.node_uid,'pvc_uid',parent.pvc_uid,
        'cleanup_request_id',parent.cleanup_request_id,'cleanup_intent_digest',parent.intent_digest)
        - CASE WHEN parent.policy_version IN (3,4) THEN ARRAY['continuation_id'] ELSE ARRAY[]::text[] END);
$$;

CREATE OR REPLACE FUNCTION public.guard_vm_job_retained_stop_authority() RETURNS trigger
    LANGUAGE plpgsql
    AS $_$
DECLARE r public.vm_creation_retries%ROWTYPE;
        op public.vm_job_retained_resumes%ROWTYPE;
        p jsonb := NEW.ready_retention_preflight;
BEGIN
    NEW.admitted_xact_id := pg_current_xact_id();
    IF NEW.policy_version=1 THEN RETURN NEW; END IF;
    SELECT * INTO r FROM public.vm_creation_retries WHERE request_id=NEW.creation_request_id;
    IF NEW.policy_version IN (3,4) THEN
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
$_$;

CREATE OR REPLACE FUNCTION public.guard_vm_job_cancel_retention_preflight() RETURNS trigger
    LANGUAGE plpgsql
    AS $_$
DECLARE a public.vm_job_cancel_retention_authorities%ROWTYPE;
        p jsonb := NEW.retention_preflight;
BEGIN
    SELECT * INTO a FROM public.vm_job_cancel_retention_authorities WHERE cleanup_admission_id=NEW.cleanup_admission_id;
    IF a.cleanup_admission_id IS NULL THEN RETURN NEW; END IF;
    IF a.policy_version IN (3,4) THEN
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
$_$;

CREATE OR REPLACE FUNCTION public.vm_initial_ready_stop_authorized(i public.vm_pre_ssh_stop_intents) RETURNS boolean
    LANGUAGE plpgsql
    AS $_$
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
    IF (a.policy_version IS NULL OR a.policy_version NOT IN (3,4)) OR a.admitted_xact_id=pg_current_xact_id()
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
$_$;


CREATE OR REPLACE FUNCTION public.guard_vm_job_cancel_retention_cleanup() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE row public.vm_workspace_cleanup_admissions%ROWTYPE;
        protected boolean;
BEGIN
    row := CASE WHEN TG_OP='DELETE' THEN OLD ELSE NEW END;
    PERFORM pg_advisory_xact_lock(hashtextextended('workspace-recovery:'||row.owner_kind||':'||row.owner_id,0));
    IF row.pvc_uid IS NOT NULL THEN
        PERFORM pg_advisory_xact_lock(hashtextextended('workspace-recovery-pvc:'||row.pvc_uid,0));
    END IF;
    SELECT EXISTS(SELECT 1 FROM public.vm_job_cancel_retention_authorities
        WHERE cleanup_admission_id=row.id OR superseded_admission_id=row.id) INTO protected;
    IF TG_OP='DELETE' THEN
        IF protected THEN RAISE EXCEPTION 'Cancel retention parent is immutable' USING ERRCODE='23514'; END IF;
        RETURN OLD;
    END IF;
    IF TG_OP='UPDATE' AND (protected OR OLD.outcome='superseded_by_retention'
        OR NEW.outcome='superseded_by_retention') AND (
        (to_jsonb(NEW)-'completed_at'-'outcome') IS DISTINCT FROM (to_jsonb(OLD)-'completed_at'-'outcome')
        OR (OLD.completed_at IS NOT NULL AND NEW IS DISTINCT FROM OLD)) THEN
        RAISE EXCEPTION 'Cancel retention parent identity is immutable' USING ERRCODE='23514';
    END IF;
    IF NOT public.vm_job_cancel_retention_cleanup_allowed(
        row.owner_kind,row.owner_id,row.pvc_uid,row.source,row.request_id,row.intent_digest,row.parent_admission_id,
        (TG_OP='INSERT' AND row.source IN ('public_vm_delete','job_terminal_vm_release') AND row.parent_admission_id IS NULL)
        OR (TG_OP='UPDATE' AND row.outcome='superseded_by_retention')) THEN
        -- Exact retaining/superseded row completion is checked at commit below.
        IF NOT (TG_OP='UPDATE' AND protected AND row.parent_admission_id IS NULL) THEN
            RAISE EXCEPTION 'Cancelled Job disk is protected by retention' USING ERRCODE='23514';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

CREATE FUNCTION public.guard_vm_job_pinned_cancel_retention() RETURNS trigger
LANGUAGE plpgsql AS $body$
DECLARE pending boolean;
        held boolean;
        settled boolean;
BEGIN
    pending := COALESCE(OLD.context->'_job_terminal_vm_cleanup'->>'retention_policy'='4',false);
    SELECT EXISTS(SELECT 1 FROM public.vm_job_cancel_retention_authorities a
        WHERE a.job_id=OLD.id AND a.policy_version=4
          AND NOT public.vm_job_cancel_retention_discharged(a.cleanup_admission_id))
      INTO held;
    IF TG_OP='DELETE' THEN
        IF pending OR held THEN
            RAISE EXCEPTION 'Pinned Ready rootdisk requires completed typed Delete before Job deletion'
                USING ERRCODE='23514';
        END IF;
        RETURN OLD;
    END IF;
    IF NOT pending AND NOT held THEN RETURN NEW; END IF;
    IF NEW.execution_lane IS DISTINCT FROM 'pinned'
       OR NEW.status IS DISTINCT FROM 'cancelled'
       OR NEW.assigned_agent_id IS NOT NULL
       OR NEW.user_id IS DISTINCT FROM OLD.user_id
       OR NEW.project_id IS DISTINCT FROM OLD.project_id
       OR NEW.parent_job_id IS DISTINCT FROM OLD.parent_job_id
       OR NEW.context->'vm'->>'provision_generation' IS DISTINCT FROM OLD.context->'vm'->>'provision_generation'
       OR NEW.context->'vm'->>'creation_request_id' IS DISTINCT FROM OLD.context->'vm'->>'creation_request_id'
       OR NEW.context->'vm'->>'vm_uid' IS DISTINCT FROM OLD.context->'vm'->>'vm_uid'
       OR NEW.context->'vm'->>'vmi_uid' IS DISTINCT FROM OLD.context->'vm'->>'vmi_uid'
       OR NEW.context->'vm'->>'rootdisk_pvc_uid' IS DISTINCT FROM OLD.context->'vm'->>'rootdisk_pvc_uid'
       OR NEW.context->'vm'->>'identity_authenticated' IS DISTINCT FROM OLD.context->'vm'->>'identity_authenticated'
       OR NEW.context->'vm'->>'identity_provision_generation' IS DISTINCT FROM OLD.context->'vm'->>'identity_provision_generation'
       OR NEW.context ? '_vm_job_retained_resume'
       OR NEW.context ? 'worker_resume_id' THEN
        RAISE EXCEPTION 'Pinned Ready retention owner or runtime identity changed'
            USING ERRCODE='23514';
    END IF;
    IF pending THEN
        IF NEW.context ? '_job_terminal_vm_cleanup' THEN
            IF ((NEW.context->'_job_terminal_vm_cleanup')-'admission_id')
                 IS DISTINCT FROM ((OLD.context->'_job_terminal_vm_cleanup')-'admission_id')
               OR (NEW.context->'_job_terminal_vm_cleanup'->>'admission_id' IS NOT NULL
                   AND NOT EXISTS(SELECT 1 FROM public.vm_job_cancel_retention_authorities a
                       WHERE a.job_id=OLD.id AND a.policy_version=4
                         AND a.cleanup_admission_id::text=
                             NEW.context->'_job_terminal_vm_cleanup'->>'admission_id')) THEN
                RAISE EXCEPTION 'Pinned Ready Cancel decision is immutable'
                    USING ERRCODE='23514';
            END IF;
        ELSE
            SELECT bool_and(public.vm_job_cancel_retention_settled(a.cleanup_admission_id))
              INTO settled FROM public.vm_job_cancel_retention_authorities a
             WHERE a.job_id=OLD.id AND a.policy_version=4;
            IF settled IS DISTINCT FROM true
               OR NEW.context->'vm'->>'status' IS DISTINCT FROM 'deleted'
               OR NEW.context->'vm'->'compute_released' IS DISTINCT FROM 'true'::jsonb
               OR NEW.context->'vm'->'disk_kept' IS DISTINCT FROM 'true'::jsonb THEN
                RAISE EXCEPTION 'Pinned Ready marker removal lacks retained stop'
                    USING ERRCODE='23514';
            END IF;
        END IF;
    END IF;
    RETURN NEW;
END;
$body$;

CREATE TRIGGER a_vm_job_pinned_cancel_retention_guard
BEFORE UPDATE OR DELETE ON public.jobs FOR EACH ROW
EXECUTE FUNCTION public.guard_vm_job_pinned_cancel_retention();

CREATE OR REPLACE FUNCTION public.check_vm_job_initial_ready_stop_parent() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF NEW.owner_kind='job' AND NEW.source='job_terminal_vm_release'
       AND NEW.parent_admission_id IS NULL AND EXISTS (
           SELECT 1 FROM public.jobs j JOIN public.vm_creation_retries r
             ON r.owner_kind='job' AND r.job_id=j.id
            AND r.provision_generation::text=j.context->'vm'->>'provision_generation'
           WHERE j.id=NEW.owner_id AND j.status='cancelled'
             AND ((j.execution_lane='stateless'
                 AND j.context->'_stateless_cancel_cleanup_pending'='true'::jsonb)
               OR (j.execution_lane='pinned'
                 AND j.context->'_job_terminal_vm_cleanup'->>'retention_policy'='4'))
             AND r.ready_at IS NOT NULL AND r.job_retained_resume_id IS NULL
             AND NOT j.context ? '_vm_job_retained_resume'
             AND r.observed_pvc_uid=NEW.pvc_uid)
       AND NOT EXISTS(SELECT 1 FROM public.vm_job_cancel_retention_authorities a
           WHERE (a.cleanup_admission_id=NEW.id OR a.superseded_admission_id=NEW.id)
             AND a.policy_version=CASE
                 WHEN EXISTS(SELECT 1 FROM public.jobs j
                     WHERE j.id=NEW.owner_id AND j.execution_lane='pinned'
                       AND j.context->'_job_terminal_vm_cleanup'->>'retention_policy'='4')
                 THEN 4 ELSE 3 END) THEN
        RAISE EXCEPTION 'Initial Ready stop bootstrap is incomplete' USING ERRCODE='23514';
    END IF;
    RETURN NULL;
END;
$$;

CREATE OR REPLACE FUNCTION public.check_vm_job_cancel_retention_transaction() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE parent_id uuid;
        c public.vm_workspace_cleanup_admissions%ROWTYPE;
        a public.vm_job_cancel_retention_authorities%ROWTYPE;
BEGIN
    IF TG_TABLE_NAME='vm_job_cancel_retention_authorities' THEN
        PERFORM public.validate_vm_job_cancel_retention(NEW.cleanup_admission_id,true);
        RETURN NEW;
    END IF;
    SELECT * INTO c FROM public.vm_workspace_cleanup_admissions WHERE id=NEW.id;
    SELECT * INTO a FROM public.vm_job_cancel_retention_authorities
     WHERE cleanup_admission_id=c.id OR superseded_admission_id=c.id;
    IF c.source='job_terminal_vm_release' AND c.parent_admission_id IS NULL
       AND public.vm_job_pinned_cancel_retention_pending(c.owner_kind,c.owner_id,c.pvc_uid)
       AND c.outcome IS DISTINCT FROM 'superseded_by_retention'
       AND NOT EXISTS(SELECT 1 FROM public.vm_job_cancel_retention_authorities exact
           WHERE exact.cleanup_admission_id=c.id AND exact.policy_version=4) THEN
        RAISE EXCEPTION 'Pinned Ready retention bootstrap is incomplete' USING ERRCODE='23514';
    END IF;
    IF c.outcome='superseded_by_retention' AND
       (a.cleanup_admission_id IS NULL OR a.superseded_admission_id IS DISTINCT FROM c.id) THEN
        RAISE EXCEPTION 'Superseded purge has no immutable retention successor' USING ERRCODE='23514';
    END IF;
    IF a.cleanup_admission_id IS NOT NULL THEN
        PERFORM public.validate_vm_job_cancel_retention(a.cleanup_admission_id,false);
        IF c.id=a.cleanup_admission_id AND c.completed_at IS NOT NULL
           AND public.vm_job_cancel_retention_settled(c.id) IS DISTINCT FROM true THEN
            RAISE EXCEPTION 'Cancel retention completion lacks exact retained stop and released charge' USING ERRCODE='23514';
        END IF;
    END IF;
    IF c.source='public_vm_delete' AND c.parent_admission_id IS NULL THEN
        IF NOT public.vm_job_cancel_retention_cleanup_allowed(
            c.owner_kind,c.owner_id,c.pvc_uid,c.source,c.request_id,c.intent_digest,NULL,false) THEN
            RAISE EXCEPTION 'Retained disk purge bootstrap is incomplete' USING ERRCODE='23514';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;
