-- migration: 0338_vm_job_cancel_retention.sql
-- description: Immutable Cancel retention and serialized, separately typed disk purge.
-- depends-on: 0337_stateless_terminal_snapshot_ack.sql
-- transactional: yes
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

CREATE TABLE public.vm_job_cancel_retention_authorities (
    cleanup_admission_id uuid PRIMARY KEY REFERENCES public.vm_workspace_cleanup_admissions(id),
    superseded_admission_id uuid UNIQUE REFERENCES public.vm_workspace_cleanup_admissions(id),
    job_id uuid NOT NULL REFERENCES public.vm_job_creation_owners(job_id),
    creation_request_id uuid NOT NULL UNIQUE REFERENCES public.vm_creation_retries(request_id),
    provision_generation uuid NOT NULL,
    reservation_id uuid NOT NULL UNIQUE REFERENCES public.vm_resource_reservations(id),
    reservation_revision bigint NOT NULL CHECK (reservation_revision>0),
    vm_uid uuid NOT NULL,
    vmi_uid uuid NOT NULL,
    launcher_uid uuid NOT NULL,
    pvc_uid uuid NOT NULL,
    node_uid uuid NOT NULL,
    namespace text NOT NULL CHECK (namespace ~ '^[a-z0-9]([-a-z0-9]*[a-z0-9])?$' AND length(namespace)<=63),
    cluster_id text NOT NULL CHECK (length(cluster_id)>0),
    cleanup_request_id uuid NOT NULL UNIQUE,
    intent_digest text NOT NULL CHECK (intent_digest ~ '^sha256:[0-9a-f]{64}$'),
    retaining_intent jsonb NOT NULL CHECK (jsonb_typeof(retaining_intent)='object'),
    superseded_request_id uuid,
    superseded_intent_digest text,
    policy_version integer NOT NULL DEFAULT 1 CHECK (policy_version=1),
    admitted_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (job_id,provision_generation),
    CHECK ((superseded_admission_id IS NULL AND superseded_request_id IS NULL
            AND superseded_intent_digest IS NULL)
        OR (superseded_admission_id IS NOT NULL AND superseded_request_id IS NOT NULL
            AND superseded_intent_digest IS NOT NULL
            AND superseded_intent_digest ~ '^sha256:[0-9a-f]{64}$')),
    CHECK (cleanup_admission_id IS DISTINCT FROM superseded_admission_id)
);
CREATE INDEX vm_job_cancel_retention_owner ON public.vm_job_cancel_retention_authorities(job_id);
CREATE INDEX vm_job_cancel_retention_pvc ON public.vm_job_cancel_retention_authorities(pvc_uid);
CREATE TRIGGER vm_job_cancel_retention_immutable
BEFORE UPDATE OR DELETE ON public.vm_job_cancel_retention_authorities
FOR EACH ROW EXECUTE FUNCTION public.guard_vm_job_retained_disk_purge_row();

ALTER TABLE public.vm_pre_ssh_stop_intents ADD COLUMN retention_preflight jsonb;

-- xmin can identify a subtransaction, including a RELEASED savepoint whose
-- outer transaction has not committed. Capture the top-level xid8 instead;
-- existing rows receive this migration transaction's stamp. The existing
-- append-only triggers protect both stamps after insertion.
ALTER TABLE public.vm_job_retained_disk_purge_authorities
    ADD COLUMN admitted_xact_id xid8 NOT NULL DEFAULT pg_current_xact_id();
ALTER TABLE public.vm_job_retained_disk_purge_predecessors
    ADD COLUMN admitted_xact_id xid8 NOT NULL DEFAULT pg_current_xact_id();
CREATE FUNCTION public.stamp_vm_job_retained_purge_transaction() RETURNS trigger
LANGUAGE plpgsql AS $body$
BEGIN
    -- Always overwrite caller input, including an explicitly supplied old ID.
    NEW.admitted_xact_id := pg_current_xact_id();
    RETURN NEW;
END;
$body$;
CREATE TRIGGER vm_job_retained_purge_authority_transaction
BEFORE INSERT ON public.vm_job_retained_disk_purge_authorities
FOR EACH ROW EXECUTE FUNCTION public.stamp_vm_job_retained_purge_transaction();
CREATE TRIGGER vm_job_retained_purge_predecessor_transaction
BEFORE INSERT ON public.vm_job_retained_disk_purge_predecessors
FOR EACH ROW EXECUTE FUNCTION public.stamp_vm_job_retained_purge_transaction();

-- Called while holding owner/PVC and queue/Job locks. Read every current writer
-- boundary; a missing or malformed optional binding is never treated as bound
-- authority, while SQL NULL and JSON null both mean absent.
CREATE FUNCTION public.vm_job_cancel_retention_candidate(
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
       OR j.execution_lane IS DISTINCT FROM 'stateless'
       OR j.assigned_agent_id IS NOT NULL OR j.parent_job_id IS NOT NULL
       OR COALESCE(j.context->'inherits_parent_workspace','false'::jsonb) IS DISTINCT FROM 'false'::jsonb
       OR j.context->'_stateless_cancel_cleanup_pending' IS DISTINCT FROM 'true'::jsonb
       OR jsonb_typeof(vm) IS DISTINCT FROM 'object'
       OR vm->>'provision_generation' IS DISTINCT FROM generation::text
       OR vm->>'vm_uid' IS DISTINCT FROM expected_vm::text
       OR vm->>'rootdisk_pvc_uid' IS DISTINCT FROM pvc::text
       OR vm->>'status' IS NULL OR vm->>'status' NOT IN ('created','retiring_process_zero')
       OR vm->'identity_authenticated' IS DISTINCT FROM 'true'::jsonb
       OR vm->>'identity_provision_generation' IS DISTINCT FROM generation::text
       OR COALESCE(vm->'workspace_storage','null'::jsonb)<>'null'::jsonb
       OR COALESCE(r.canonical_request->'workspace_storage','null'::jsonb)<>'null'::jsonb
       OR q.unit_id IS NULL OR q.unit_kind IS DISTINCT FROM 'worker_batch'
       OR q.state IS DISTINCT FROM 'done' OR q.leased_by IS NOT NULL OR q.leased_until IS NOT NULL
       OR r.request_id IS NULL OR r.state IS DISTINCT FROM 'succeeded'
       OR r.reason IS DISTINCT FROM 'creation_adopted' OR r.resolved_at IS NULL
       OR r.ready_at IS NOT NULL OR r.claim_token IS NOT NULL OR r.claim_expires_at IS NOT NULL
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

-- Immutable identity checks apply to active and settled authority; the held
-- candidate predicate is deliberately separate from settled replay.
CREATE FUNCTION public.validate_vm_job_cancel_retention(parent_id uuid, fresh boolean DEFAULT false)
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
        candidate := public.vm_job_cancel_retention_candidate(
            a.job_id,a.provision_generation,a.vm_uid,a.pvc_uid,a.superseded_admission_id,a.cleanup_admission_id,NOT fresh);
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

-- The existing immutable intent guard still runs; new retaining intents also
-- need authenticated ownership qualification before the Halted effect.
CREATE FUNCTION public.guard_vm_job_cancel_retention_preflight() RETURNS trigger
LANGUAGE plpgsql AS $body$
DECLARE a public.vm_job_cancel_retention_authorities%ROWTYPE;
        p jsonb := NEW.retention_preflight;
BEGIN
    SELECT * INTO a FROM public.vm_job_cancel_retention_authorities WHERE cleanup_admission_id=NEW.cleanup_admission_id;
    IF a.cleanup_admission_id IS NULL THEN RETURN NEW; END IF;
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
CREATE TRIGGER vm_job_cancel_retention_preflight
BEFORE INSERT ON public.vm_pre_ssh_stop_intents
FOR EACH ROW EXECUTE FUNCTION public.guard_vm_job_cancel_retention_preflight();

CREATE FUNCTION public.vm_job_cancel_retention_settled(parent_id uuid)
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
           AND z.runtime_incarnation=a.provision_generation::text);
END;
$body$;

-- A later completed typed purge discharges storage, without modifying history.
CREATE FUNCTION public.vm_job_cancel_retention_discharged(retention_id uuid)
RETURNS boolean LANGUAGE sql STABLE AS $body$
    SELECT EXISTS (
        SELECT 1 FROM public.vm_job_cancel_retention_authorities a
        JOIN public.vm_job_retained_disk_purge_predecessors link ON link.old_cleanup_admission_id=a.cleanup_admission_id
        JOIN public.vm_job_retained_disk_purge_authorities d ON d.cleanup_admission_id=link.cleanup_admission_id
        JOIN public.vm_job_retained_disk_purge_receipts receipt ON receipt.cleanup_admission_id=d.cleanup_admission_id
        JOIN public.vm_workspace_cleanup_admissions c ON c.id=d.cleanup_admission_id
        WHERE a.cleanup_admission_id=retention_id AND d.job_id=a.job_id AND d.pvc_uid=a.pvc_uid
          AND c.completed_at IS NOT NULL AND c.outcome='completed'
          AND receipt.chain_digest=public.vm_job_retained_disk_purge_chain_digest(c.id)
    );
$body$;

CREATE FUNCTION public.vm_job_cancel_retention_cleanup_allowed(
    kind text, owner uuid, pvc uuid, cleanup_source text, cleanup_request uuid,
    digest text, parent_id uuid, provisional boolean DEFAULT false
) RETURNS boolean LANGUAGE plpgsql AS $body$
DECLARE a public.vm_job_cancel_retention_authorities%ROWTYPE;
        purge_id uuid;
BEGIN
    FOR a IN SELECT * FROM public.vm_job_cancel_retention_authorities r
        WHERE ((kind='job' AND r.job_id=owner) OR r.pvc_uid=pvc)
          AND NOT public.vm_job_cancel_retention_discharged(r.cleanup_admission_id)
    LOOP
        IF kind='job' AND owner=a.job_id AND pvc=a.pvc_uid AND parent_id IS NULL
           AND cleanup_source='job_terminal_vm_release' AND cleanup_request=a.cleanup_request_id
           AND digest=a.intent_digest THEN CONTINUE; END IF;
        IF kind='job' AND owner=a.job_id AND pvc IS NULL AND parent_id IS NULL
           AND cleanup_source='terminal_checkpoint_prune'
           AND digest='sha256:'||encode(sha256(convert_to(format(
               '{"mode":"delete_thread","resource":"checkpoint_thread","thread_id":"%s"}',owner),'UTF8')),'hex')
           AND public.vm_job_cancel_retention_settled(a.cleanup_admission_id) THEN CONTINUE; END IF;
        IF kind IS DISTINCT FROM 'job' OR owner IS DISTINCT FROM a.job_id OR pvc IS DISTINCT FROM a.pvc_uid THEN
            RETURN false;
        END IF;
        IF cleanup_source='public_vm_delete' AND parent_id IS NULL THEN
            SELECT id INTO purge_id FROM public.vm_workspace_cleanup_admissions
             WHERE owner_kind=kind AND owner_id=owner AND request_id=cleanup_request AND intent_digest=digest;
            -- Insert ordering only: the deferred trigger requires full typed
            -- authority before commit. Python additionally requires the private bootstrap.
            IF provisional THEN CONTINUE; END IF;
        ELSIF cleanup_source='controller_rootdisk_delete' AND parent_id IS NOT NULL THEN
            purge_id := parent_id;
        ELSE RETURN false;
        END IF;
        IF purge_id IS NULL OR NOT EXISTS (
            SELECT 1 FROM public.vm_job_retained_disk_purge_authorities d
            JOIN public.vm_job_retained_disk_purge_predecessors p USING (cleanup_admission_id)
            WHERE d.cleanup_admission_id=purge_id AND d.job_id=a.job_id AND d.pvc_uid=a.pvc_uid
              AND p.old_cleanup_admission_id=a.cleanup_admission_id
              -- MVCC makes a visible row from another top-level transaction
              -- committed. Same-transaction chains never issue physical work,
              -- even after one or more nested savepoints have been released.
              AND (cleanup_source<>'controller_rootdisk_delete'
                   OR (d.admitted_xact_id<>pg_current_xact_id()
                       AND p.admitted_xact_id<>pg_current_xact_id()))
        ) THEN RETURN false; END IF;
        PERFORM public.validate_vm_job_retained_disk_purge(purge_id,false);
    END LOOP;
    RETURN true;
END;
$body$;

-- Serialize direct/old controller insertions with transfer, even without new
-- application code. Completed children still prohibit a transfer.
CREATE FUNCTION public.guard_vm_job_cancel_retention_cleanup() RETURNS trigger
LANGUAGE plpgsql AS $body$
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
        TG_OP='INSERT' AND row.source='public_vm_delete' AND row.parent_admission_id IS NULL) THEN
        -- Exact retaining/superseded row completion is checked at commit below.
        IF NOT (TG_OP='UPDATE' AND protected AND row.parent_admission_id IS NULL) THEN
            RAISE EXCEPTION 'Cancelled Job disk is protected by retention' USING ERRCODE='23514';
        END IF;
    END IF;
    RETURN NEW;
END;
$body$;
CREATE TRIGGER a_vm_job_cancel_retention_cleanup_guard
BEFORE INSERT OR UPDATE OR DELETE ON public.vm_workspace_cleanup_admissions
FOR EACH ROW EXECUTE FUNCTION public.guard_vm_job_cancel_retention_cleanup();

CREATE FUNCTION public.check_vm_job_cancel_retention_transaction() RETURNS trigger
LANGUAGE plpgsql AS $body$
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
$body$;
CREATE CONSTRAINT TRIGGER vm_job_cancel_retention_authority_transaction
AFTER INSERT ON public.vm_job_cancel_retention_authorities
DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.check_vm_job_cancel_retention_transaction();
CREATE CONSTRAINT TRIGGER vm_job_cancel_retention_parent_transaction
AFTER INSERT OR UPDATE ON public.vm_workspace_cleanup_admissions
DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.check_vm_job_cancel_retention_transaction();
COMMIT;
