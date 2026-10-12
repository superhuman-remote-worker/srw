-- migration: 0339_vm_job_retained_resume.sql
-- description: Explicit same-PVC Job Resume continues immutable retained disk custody.
-- depends-on: 0338_vm_job_cancel_retention.sql
-- transactional: yes
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

CREATE TABLE public.vm_job_retained_resumes (
    id uuid PRIMARY KEY,
    job_id uuid NOT NULL REFERENCES public.vm_job_creation_owners(job_id),
    root_retention_admission_id uuid NOT NULL REFERENCES public.vm_job_cancel_retention_authorities(cleanup_admission_id),
    physical_cleanup_admission_id uuid NOT NULL REFERENCES public.vm_job_cancel_retention_authorities(cleanup_admission_id),
    predecessor_terminal_id uuid UNIQUE,
    predecessor_request_id uuid NOT NULL REFERENCES public.vm_creation_retries(request_id),
    predecessor_generation uuid NOT NULL,
    pvc_uid uuid NOT NULL,
    request_id uuid NOT NULL UNIQUE,
    provision_generation uuid NOT NULL UNIQUE,
    explicit_resume_id uuid NOT NULL UNIQUE,
    requested_by uuid NOT NULL,
    source_revision bigint NOT NULL,
    retained_vm jsonb NOT NULL CHECK (jsonb_typeof(retained_vm)='object'),
    admitted_xact_id xid8 NOT NULL DEFAULT pg_current_xact_id(),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (provision_generation<>predecessor_generation),
    CHECK (request_id<>predecessor_request_id)
);
CREATE UNIQUE INDEX vm_job_retained_first_resume
    ON public.vm_job_retained_resumes(root_retention_admission_id)
    WHERE predecessor_terminal_id IS NULL;
CREATE INDEX vm_job_retained_resume_owner ON public.vm_job_retained_resumes(job_id);

CREATE TABLE public.vm_job_retained_resume_terminals (
    id uuid PRIMARY KEY,
    resume_id uuid NOT NULL UNIQUE REFERENCES public.vm_job_retained_resumes(id),
    allocated_request_id uuid NOT NULL UNIQUE,
    source_request_id uuid REFERENCES public.vm_creation_retries(request_id),
    terminal_kind text NOT NULL CHECK (terminal_kind IN ('source_absent','never_issued','creation_disposed','kept_compute')),
    physical_cleanup_admission_id uuid NOT NULL REFERENCES public.vm_job_cancel_retention_authorities(cleanup_admission_id),
    evidence jsonb NOT NULL CHECK (jsonb_typeof(evidence)='object'),
    admitted_xact_id xid8 NOT NULL DEFAULT pg_current_xact_id(),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK ((terminal_kind='source_absent')=(source_request_id IS NULL))
);
ALTER TABLE public.vm_job_retained_resumes
    ADD CONSTRAINT vm_job_retained_resume_terminal FOREIGN KEY (predecessor_terminal_id)
    REFERENCES public.vm_job_retained_resume_terminals(id);

CREATE FUNCTION public.guard_vm_job_retained_resume() RETURNS trigger
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
       OR j.context ?| ARRAY['_stateless_cancel_cleanup_pending','_stateless_delete_pending','_completion_control_claim']
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
CREATE TRIGGER guard_vm_job_retained_resume
BEFORE INSERT OR UPDATE OR DELETE ON public.vm_job_retained_resumes
FOR EACH ROW EXECUTE FUNCTION public.guard_vm_job_retained_resume();

CREATE FUNCTION public.check_vm_job_retained_resume_owner() RETURNS trigger
LANGUAGE plpgsql AS $body$
DECLARE j public.jobs%ROWTYPE;
BEGIN
    SELECT * INTO j FROM public.jobs WHERE id=NEW.job_id;
    IF j.context->>'_vm_job_retained_resume' IS DISTINCT FROM NEW.id::text
       OR j.context->>'worker_resume_id' IS DISTINCT FROM NEW.explicit_resume_id::text
       OR j.status NOT IN ('created','paused') OR j.assigned_agent_id IS NOT NULL THEN
        RAISE EXCEPTION 'Retained Job Resume owner CAS is incomplete' USING ERRCODE='23514';
    END IF;
    RETURN NULL;
END;
$body$;
CREATE CONSTRAINT TRIGGER vm_job_retained_resume_owner_commit
AFTER INSERT ON public.vm_job_retained_resumes DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION public.check_vm_job_retained_resume_owner();

CREATE FUNCTION public.vm_job_retained_source_absent_evidence(operation_id uuid)
RETURNS jsonb LANGUAGE plpgsql AS $body$
DECLARE op public.vm_job_retained_resumes%ROWTYPE;
        j public.jobs%ROWTYPE;
        q public.run_queue%ROWTYPE;
BEGIN
    SELECT * INTO op FROM public.vm_job_retained_resumes WHERE id=operation_id;
    SELECT * INTO j FROM public.jobs WHERE id=op.job_id;
    SELECT * INTO q FROM public.run_queue WHERE unit_id=op.job_id;
    IF op.id IS NULL OR j.id IS NULL OR j.status IS DISTINCT FROM 'cancelled'
       OR j.execution_lane IS DISTINCT FROM 'stateless' OR j.assigned_agent_id IS NOT NULL
       OR j.parent_job_id IS NOT NULL
       OR j.context->>'_vm_job_retained_resume' IS DISTINCT FROM op.id::text
       OR (j.context ? '_stateless_cancel_cleanup_pending'
           AND j.context->'_stateless_cancel_cleanup_pending' IS DISTINCT FROM 'true'::jsonb)
       OR (COALESCE(j.context->'vm','null'::jsonb)<>'null'::jsonb AND (
           jsonb_typeof(j.context->'vm') IS DISTINCT FROM 'object'
           OR j.context->'vm'->>'provision_generation' IS DISTINCT FROM op.provision_generation::text
           OR j.context->'vm'->'identity_authenticated'='true'::jsonb
           OR j.context->'vm'->>'vm_uid' IS NOT NULL OR j.context->'vm'->>'vmi_uid' IS NOT NULL
           OR j.context->'vm'->>'active_pod_uid' IS NOT NULL))
       OR q.unit_kind IS DISTINCT FROM 'worker_batch' OR q.state IS DISTINCT FROM 'done'
       OR q.leased_by IS NOT NULL OR q.leased_until IS NOT NULL
       OR NOT public.vm_job_cancel_retention_settled(op.physical_cleanup_admission_id)
       OR public.vm_job_cancel_retention_discharged(op.root_retention_admission_id)
       OR EXISTS (SELECT 1 FROM public.vm_creation_retries r WHERE r.request_id=op.request_id
           OR (r.owner_kind='job' AND r.job_id=op.job_id AND r.provision_generation=op.provision_generation))
       OR EXISTS (SELECT 1 FROM public.vm_resource_waiters w WHERE w.request_id=op.request_id)
       OR EXISTS (SELECT 1 FROM public.vm_resource_reservations r WHERE r.request_id=op.request_id)
       OR EXISTS (SELECT 1 FROM public.vm_creation_effects e WHERE e.request_id=op.request_id)
       OR EXISTS (SELECT 1 FROM public.vm_workspace_cleanup_admissions c WHERE c.owner_kind='job' AND c.owner_id=op.job_id
           AND (c.completed_at IS NULL OR c.request_id=public.uuid_generate_v5(public.uuid_ns_url(),'vm-create:'||op.request_id))
           AND NOT (c.source='terminal_checkpoint_prune' AND c.pvc_uid IS NULL AND c.parent_admission_id IS NULL
               AND c.intent_digest='sha256:'||encode(sha256(convert_to(format(
                   '{"mode":"delete_thread","resource":"checkpoint_thread","thread_id":"%s"}',op.job_id),'UTF8')),'hex')))
       OR EXISTS (SELECT 1 FROM public.vm_job_retained_resumes later WHERE later.job_id=op.job_id
           AND (later.created_at,later.id)>(op.created_at,op.id)) THEN RETURN NULL; END IF;
    RETURN jsonb_build_object('version',1,'kind','vm_job_retained_resume_source_absent',
        'resume_id',op.id,'job_id',op.job_id,'allocated_request_id',op.request_id,
        'provision_generation',op.provision_generation,'pvc_uid',op.pvc_uid,
        'physical_cleanup_admission_id',op.physical_cleanup_admission_id);
END;
$body$;

CREATE FUNCTION public.guard_vm_job_retained_resume_terminal() RETURNS trigger
LANGUAGE plpgsql AS $body$
DECLARE op public.vm_job_retained_resumes%ROWTYPE;
        proof jsonb;
BEGIN
    IF TG_OP<>'INSERT' THEN
        RAISE EXCEPTION 'Retained Job terminal is append-only' USING ERRCODE='23514';
    END IF;
    NEW.admitted_xact_id := pg_current_xact_id();
    SELECT * INTO op FROM public.vm_job_retained_resumes WHERE id=NEW.resume_id;
    PERFORM pg_advisory_xact_lock(hashtextextended('workspace-recovery:job:'||op.job_id,0));
    PERFORM pg_advisory_xact_lock(hashtextextended('workspace-recovery-pvc:'||op.pvc_uid,0));
    PERFORM 1 FROM public.run_queue WHERE unit_id=op.job_id FOR UPDATE;
    PERFORM 1 FROM public.jobs WHERE id=op.job_id FOR UPDATE;
    IF NEW.terminal_kind='source_absent' THEN
        proof := public.vm_job_retained_source_absent_evidence(op.id);
    ELSIF NEW.terminal_kind='kept_compute' THEN
        proof := public.vm_job_retained_kept_evidence(op.id);
    ELSE
        proof := public.vm_job_retained_noeffect_evidence(op.id);
        IF proof->>'terminal_kind' IS DISTINCT FROM NEW.terminal_kind THEN proof := NULL; END IF;
    END IF;
    IF op.id IS NULL OR proof IS NULL OR NEW.evidence IS DISTINCT FROM proof
       OR NEW.allocated_request_id IS DISTINCT FROM op.request_id
       OR NEW.physical_cleanup_admission_id::text IS DISTINCT FROM proof->>'physical_cleanup_admission_id'
       OR (NEW.terminal_kind<>'source_absent' AND NEW.source_request_id IS DISTINCT FROM op.request_id)
       OR public.vm_job_cancel_retention_discharged(op.root_retention_admission_id)
       OR NOT public.vm_job_retained_terminal_authorized(op.job_id) THEN
        RAISE EXCEPTION 'Retained Job terminal source is unproven' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$body$;
CREATE TRIGGER guard_vm_job_retained_resume_terminal
BEFORE INSERT OR UPDATE OR DELETE ON public.vm_job_retained_resume_terminals
FOR EACH ROW EXECUTE FUNCTION public.guard_vm_job_retained_resume_terminal();

ALTER TABLE public.vm_creation_retries
    ADD COLUMN job_retained_resume_id uuid,
    ADD CONSTRAINT vm_creation_retained_resume_fkey FOREIGN KEY (job_retained_resume_id)
        REFERENCES public.vm_job_retained_resumes(id) NOT VALID,
    ADD COLUMN job_retained_resume_admitted_xact_id xid8;

CREATE FUNCTION public.guard_vm_job_retained_resume_source() RETURNS trigger
LANGUAGE plpgsql AS $body$
DECLARE op public.vm_job_retained_resumes%ROWTYPE;
        predecessor public.vm_creation_retries%ROWTYPE;
        j public.jobs%ROWTYPE;
        protected boolean;
BEGIN
    IF TG_OP='UPDATE' THEN
        IF ROW(NEW.job_retained_resume_id,NEW.job_retained_resume_admitted_xact_id)
           IS DISTINCT FROM ROW(OLD.job_retained_resume_id,OLD.job_retained_resume_admitted_xact_id) THEN
            RAISE EXCEPTION 'Retained Job source link is immutable' USING ERRCODE='23514';
        END IF;
        RETURN NEW;
    END IF;
    IF NEW.owner_kind='job' THEN
        PERFORM pg_advisory_xact_lock(hashtextextended('workspace-recovery:job:'||NEW.job_id,0));
    END IF;
    IF NEW.expected_pvc_uid IS NOT NULL THEN
        PERFORM pg_advisory_xact_lock(hashtextextended('workspace-recovery-pvc:'||NEW.expected_pvc_uid,0));
    END IF;
    SELECT EXISTS(SELECT 1 FROM public.vm_job_cancel_retention_authorities a
        WHERE ((NEW.owner_kind='job' AND a.job_id=NEW.job_id) OR a.pvc_uid=NEW.expected_pvc_uid)
          AND NOT public.vm_job_cancel_retention_discharged(a.cleanup_admission_id)) INTO protected;
    IF NEW.job_retained_resume_id IS NULL AND NOT protected THEN RETURN NEW; END IF;
    SELECT * INTO op FROM public.vm_job_retained_resumes WHERE id=NEW.job_retained_resume_id;
    SELECT * INTO j FROM public.jobs WHERE id=NEW.job_id;
    SELECT * INTO predecessor FROM public.vm_creation_retries WHERE request_id=op.predecessor_request_id;
    IF NEW.owner_kind IS DISTINCT FROM 'job' OR NEW.origin IS DISTINCT FROM 'resume'
       OR op.id IS NULL OR op.job_id IS DISTINCT FROM NEW.job_id
       OR op.request_id IS DISTINCT FROM NEW.request_id OR op.provision_generation IS DISTINCT FROM NEW.provision_generation
       OR op.pvc_uid IS DISTINCT FROM NEW.expected_pvc_uid
       OR op.physical_cleanup_admission_id IS DISTINCT FROM NEW.predecessor_cleanup_admission_id
       OR j.id IS NULL OR j.context->>'_vm_job_retained_resume' IS DISTINCT FROM op.id::text
       OR j.context->'vm'->>'provision_generation' IS DISTINCT FROM op.provision_generation::text
       OR j.context->>'_vm_creation_pending' IS DISTINCT FROM op.request_id::text
       OR j.status NOT IN ('created','paused','failed') OR j.assigned_agent_id IS NOT NULL
       OR j.context ?| ARRAY['_stateless_cancel_cleanup_pending','_stateless_delete_pending','_completion_control_claim']
       OR NEW.canonical_request-'provision_generation' IS DISTINCT FROM predecessor.canonical_request-'provision_generation'
       OR COALESCE(NEW.canonical_request->'workspace_storage','null'::jsonb)<>'null'::jsonb
       OR EXISTS(SELECT 1 FROM public.vm_job_retained_resume_terminals WHERE resume_id=op.id)
       OR NOT public.vm_job_cancel_retention_settled(op.physical_cleanup_admission_id)
       OR public.vm_job_cancel_retention_discharged(op.root_retention_admission_id) THEN
        RAISE EXCEPTION 'Retained Job creation requires its live explicit Resume' USING ERRCODE='23514';
    END IF;
    NEW.job_retained_resume_admitted_xact_id := pg_current_xact_id();
    RETURN NEW;
END;
$body$;
CREATE TRIGGER a_vm_job_retained_resume_source
BEFORE INSERT OR UPDATE ON public.vm_creation_retries
FOR EACH ROW EXECUTE FUNCTION public.guard_vm_job_retained_resume_source();

CREATE FUNCTION public.vm_job_retained_creation_digest(source public.vm_creation_retries)
RETURNS text LANGUAGE sql IMMUTABLE AS $body$
    SELECT 'sha256:'||encode(sha256(convert_to(format(
        '{"controller_configuration_digest":"%s","expected_pvc_uid":"%s","job_id":"%s","provision_generation":"%s","request_digest":"%s","request_id":"%s","source":"controller_vm_create"}',
        source.controller_configuration_digest,source.expected_pvc_uid,source.job_id,
        source.provision_generation,source.request_digest,source.request_id),'UTF8')),'hex');
$body$;

CREATE FUNCTION public.vm_job_retained_creation_allowed(owner uuid,pvc uuid,request uuid,digest text)
RETURNS boolean LANGUAGE sql STABLE AS $body$
    SELECT EXISTS(SELECT 1 FROM public.vm_creation_retries r
        JOIN public.vm_job_retained_resumes op ON op.id=r.job_retained_resume_id
        JOIN public.jobs j ON j.id=op.job_id
        WHERE r.owner_kind='job' AND r.job_id=owner AND op.job_id=owner AND r.expected_pvc_uid=pvc AND op.pvc_uid=pvc
          AND r.request_id=op.request_id AND r.provision_generation=op.provision_generation
          AND r.origin='resume' AND r.job_retained_resume_admitted_xact_id<>pg_current_xact_id()
          AND op.admitted_xact_id<>pg_current_xact_id()
          AND request=public.uuid_generate_v5(public.uuid_ns_url(),'vm-create:'||r.request_id)
          AND digest=public.vm_job_retained_creation_digest(r)
          AND j.context->>'_vm_job_retained_resume'=op.id::text
          AND j.context->'vm'->>'provision_generation'=op.provision_generation::text
          AND NOT EXISTS(SELECT 1 FROM public.vm_job_retained_resume_terminals terminal WHERE terminal.resume_id=op.id)
          AND public.vm_job_cancel_retention_settled(op.physical_cleanup_admission_id)
          AND NOT public.vm_job_cancel_retention_discharged(op.root_retention_admission_id)
          AND NOT EXISTS(SELECT 1 FROM public.vm_job_cancel_retention_authorities a
              WHERE ((a.job_id=owner) OR a.pvc_uid=pvc)
                AND NOT public.vm_job_cancel_retention_discharged(a.cleanup_admission_id)
                AND (a.job_id<>owner OR a.pvc_uid<>pvc OR (
                    a.cleanup_admission_id<>op.root_retention_admission_id AND NOT EXISTS(
                        SELECT 1 FROM public.vm_job_retained_resumes ancestor
                        WHERE ancestor.root_retention_admission_id=op.root_retention_admission_id
                          AND ancestor.request_id=a.creation_request_id))))
          AND ((r.state='reconciling' AND r.claim_token IS NOT NULL AND r.claim_expires_at>clock_timestamp()
                AND j.status IN ('created','paused','failed') AND j.assigned_agent_id IS NULL
                AND NOT j.context ?| ARRAY['_stateless_cancel_cleanup_pending','_stateless_delete_pending','_completion_control_claim'])
              OR EXISTS(SELECT 1 FROM public.vm_workspace_cleanup_admissions c
                  WHERE c.id=r.creation_admission_id AND c.owner_kind='job' AND c.owner_id=owner
                    AND c.pvc_uid=pvc AND c.source='controller_vm_create'
                    AND c.request_id=request AND c.intent_digest=digest)));
$body$;

ALTER FUNCTION public.vm_job_cancel_retention_cleanup_allowed(text,uuid,uuid,text,uuid,text,uuid,boolean)
    RENAME TO vm_job_cancel_retention_cleanup_allowed_v1;

CREATE FUNCTION public.vm_job_cancel_retention_cleanup_allowed(
    kind text,owner uuid,pvc uuid,cleanup_source text,cleanup_request uuid,
    digest text,parent_id uuid,provisional boolean DEFAULT false
) RETURNS boolean LANGUAGE plpgsql AS $body$
BEGIN
    IF kind='job' AND cleanup_source='terminal_checkpoint_prune'
       AND EXISTS(SELECT 1 FROM public.vm_job_retained_resumes WHERE job_id=owner)
       AND NOT public.vm_job_retained_terminal_is_current(owner) THEN RETURN false; END IF;
    IF kind='job' AND cleanup_source='job_terminal_vm_release' AND parent_id IS NULL
       AND public.vm_job_retained_stop_allowed(owner,pvc,cleanup_request,digest) THEN RETURN true; END IF;
    IF kind='job' AND cleanup_source='controller_vm_create' AND parent_id IS NULL
       AND public.vm_job_retained_creation_allowed(owner,pvc,cleanup_request,digest) THEN RETURN true; END IF;
    RETURN public.vm_job_cancel_retention_cleanup_allowed_v1(kind,owner,pvc,cleanup_source,cleanup_request,digest,parent_id,provisional);
END;
$body$;

ALTER TABLE public.vm_workspace_cleanup_admissions ADD COLUMN retained_resume_admitted_xact_id xid8;
CREATE FUNCTION public.stamp_vm_job_retained_parent() RETURNS trigger LANGUAGE plpgsql AS $body$
BEGIN
    IF TG_OP='UPDATE' THEN
        IF NEW.retained_resume_admitted_xact_id IS DISTINCT FROM OLD.retained_resume_admitted_xact_id THEN
            RAISE EXCEPTION 'Retained Resume parent stamp is immutable' USING ERRCODE='23514';
        END IF;
    ELSE
        NEW.retained_resume_admitted_xact_id := NULL;
        IF NEW.owner_kind='job' AND NEW.source IN ('controller_vm_create','job_terminal_vm_release')
           AND EXISTS(SELECT 1 FROM public.vm_job_retained_resumes op WHERE op.job_id=NEW.owner_id
               AND (NEW.source='job_terminal_vm_release' OR NEW.request_id=public.uuid_generate_v5(public.uuid_ns_url(),'vm-create:'||op.request_id))) THEN
            NEW.retained_resume_admitted_xact_id := pg_current_xact_id();
        END IF;
    END IF;
    RETURN NEW;
END;
$body$;
CREATE TRIGGER vm_job_retained_parent_stamp BEFORE INSERT OR UPDATE ON public.vm_workspace_cleanup_admissions
FOR EACH ROW EXECUTE FUNCTION public.stamp_vm_job_retained_parent();

CREATE FUNCTION public.check_vm_job_retained_creation_parent() RETURNS trigger LANGUAGE plpgsql AS $body$
BEGIN
    IF NEW.retained_resume_admitted_xact_id IS NOT NULL AND NEW.source='controller_vm_create'
       AND NOT EXISTS(SELECT 1 FROM public.vm_creation_retries r
           JOIN public.vm_job_retained_resumes op ON op.id=r.job_retained_resume_id
           WHERE r.creation_admission_id=NEW.id AND r.owner_kind='job' AND r.job_id=NEW.owner_id
             AND op.job_id=NEW.owner_id AND r.request_id=op.request_id
             AND r.provision_generation=op.provision_generation AND r.expected_pvc_uid=NEW.pvc_uid
             AND NEW.request_id=public.uuid_generate_v5(public.uuid_ns_url(),'vm-create:'||r.request_id)
             AND NEW.intent_digest=public.vm_job_retained_creation_digest(r)) THEN
        RAISE EXCEPTION 'Retained creation parent source link is incomplete' USING ERRCODE='23514';
    END IF;
    RETURN NULL;
END;
$body$;
CREATE CONSTRAINT TRIGGER vm_job_retained_creation_parent_commit
AFTER INSERT ON public.vm_workspace_cleanup_admissions DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION public.check_vm_job_retained_creation_parent();

CREATE FUNCTION public.guard_vm_job_retained_create_effect() RETURNS trigger LANGUAGE plpgsql AS $body$
DECLARE r public.vm_creation_retries%ROWTYPE;
        parent public.vm_workspace_cleanup_admissions%ROWTYPE;
BEGIN
    SELECT * INTO r FROM public.vm_creation_retries WHERE request_id=NEW.request_id;
    IF r.job_retained_resume_id IS NULL THEN RETURN NEW; END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended('workspace-recovery:job:'||r.job_id,0));
    PERFORM pg_advisory_xact_lock(hashtextextended('workspace-recovery-pvc:'||r.expected_pvc_uid,0));
    PERFORM 1 FROM public.run_queue WHERE unit_id=r.job_id FOR UPDATE;
    PERFORM 1 FROM public.jobs WHERE id=r.job_id FOR UPDATE;
    SELECT * INTO r FROM public.vm_creation_retries WHERE request_id=NEW.request_id FOR UPDATE;
    SELECT * INTO parent FROM public.vm_workspace_cleanup_admissions WHERE id=r.creation_admission_id;
    IF NOT EXISTS(SELECT 1 FROM public.jobs j WHERE j.id=r.job_id
        AND j.status IN ('created','paused','failed') AND j.assigned_agent_id IS NULL
        AND NOT j.context ?| ARRAY['_stateless_cancel_cleanup_pending','_stateless_delete_pending','_completion_control_claim']) THEN
        RAISE EXCEPTION 'Retained Resume effect requires committed live authority' USING ERRCODE='23514';
    END IF;
    IF r.state IS DISTINCT FROM 'reconciling' OR r.claim_token IS NULL OR r.claim_expires_at<=clock_timestamp()
       OR parent.id IS NULL OR parent.completed_at IS NOT NULL
       OR parent.retained_resume_admitted_xact_id IS NULL OR parent.retained_resume_admitted_xact_id=pg_current_xact_id()
       OR NOT public.vm_job_retained_creation_allowed(r.job_id,r.expected_pvc_uid,parent.request_id,parent.intent_digest) THEN
        RAISE EXCEPTION 'Retained Resume effect requires committed live authority' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$body$;
CREATE TRIGGER a_vm_job_retained_create_effect BEFORE INSERT ON public.vm_creation_effects
FOR EACH ROW EXECUTE FUNCTION public.guard_vm_job_retained_create_effect();

-- Readiness may publish these two SSH-wait states before a neverReady Cancel.
-- Every source/identity/charge/lease/child predicate from 0338 stays exact.
CREATE OR REPLACE FUNCTION public.vm_job_cancel_retention_candidate(
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
       OR vm->>'status' IS NULL OR vm->>'status' NOT IN ('created','ssh_pending','ssh_unreachable','retiring_process_zero')
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
-- Existing rows all receive NULL in the new unique link; atomic policy admission
-- needs this uniqueness in the same short, lock-timeout-bounded migration.
ALTER TABLE public.vm_job_cancel_retention_authorities
    DROP CONSTRAINT vm_job_cancel_retention_authorities_policy_version_check,
    -- squawk-ignore disallowed-unique-constraint
    ADD COLUMN job_retained_resume_id uuid UNIQUE,
    ADD CONSTRAINT vm_cancel_retained_resume_fkey FOREIGN KEY (job_retained_resume_id)
        REFERENCES public.vm_job_retained_resumes(id) NOT VALID,
    ADD COLUMN ready_retention_preflight jsonb,
    ADD COLUMN admitted_xact_id xid8 NOT NULL DEFAULT pg_current_xact_id(),
    ADD CONSTRAINT vm_job_cancel_retention_policy CHECK (
        (policy_version=1 AND job_retained_resume_id IS NULL AND ready_retention_preflight IS NULL)
        OR (policy_version=2 AND job_retained_resume_id IS NOT NULL)) NOT VALID;

CREATE FUNCTION public.vm_job_retained_terminal_authorized(owner uuid)
RETURNS boolean LANGUAGE sql STABLE AS $body$
    SELECT EXISTS(SELECT 1 FROM public.jobs j WHERE j.id=owner AND (
        (j.status='cancelled' AND j.context->'_stateless_cancel_cleanup_pending'='true'::jsonb)
        OR (j.status IN ('cancelled','failed','completed') AND j.context->'_stateless_delete_pending'='true'::jsonb)
        OR EXISTS(SELECT 1 FROM public.job_completion_commands command
            JOIN public.completion_effects effect ON effect.producer_id=command.id
            LEFT JOIN public.completion_effects disposition ON disposition.producer_kind='job_completion'
                AND disposition.producer_id=command.id AND disposition.effect_name='main_status_write' AND disposition.state='done'
            LEFT JOIN public.completion_effects entry ON entry.producer_kind='job_completion'
                AND entry.producer_id=command.id AND entry.effect_name='late_callback_guard' AND entry.state='done'
            WHERE command.job_id=j.id AND command.report_seq=j.completion_seq_hwm
              AND command.state='finalizing' AND command.lease_expires_at>clock_timestamp()
              AND command.deadline_at>clock_timestamp() AND effect.producer_kind='job_completion'
              AND effect.effect_name='workspace_archive_teardown' AND effect.state='pending'
              AND effect.complete_by>clock_timestamp()
              AND effect.detail->'teardown_authorization'->'active'='true'::jsonb
              AND effect.detail->'teardown_authorization'->>'report_seq'=command.report_seq::text
              AND j.status IN ('completed','failed','cancelled')
              AND COALESCE(NULLIF(disposition.detail#>>'{output,new_status}',''),
                  CASE WHEN entry.detail#>>'{output,matched}'='true' THEN entry.detail#>>'{output,entry_status}' END)=j.status::text)));
$body$;

CREATE FUNCTION public.vm_job_retained_stop_candidate(
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
    IF j.id IS NULL OR NOT public.vm_job_retained_terminal_authorized(owner)
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
       OR r.reason IS DISTINCT FROM 'creation_adopted' OR r.resolved_at IS NULL
       OR r.claim_token IS NOT NULL OR r.claim_expires_at IS NOT NULL
       OR NOT EXISTS(SELECT 1 FROM public.vm_job_retained_resumes op
           WHERE op.id=r.job_retained_resume_id AND op.request_id=r.request_id
             AND op.job_id=owner AND op.provision_generation=generation AND op.pvc_uid=pvc
             AND j.context->>'_vm_job_retained_resume'=op.id::text
             AND op.admitted_xact_id<>pg_current_xact_id()
             AND r.job_retained_resume_admitted_xact_id<>pg_current_xact_id()
             AND public.vm_job_cancel_retention_settled(op.physical_cleanup_admission_id)
             AND NOT public.vm_job_cancel_retention_discharged(op.root_retention_admission_id)
             AND NOT EXISTS(SELECT 1 FROM public.vm_job_retained_resume_terminals t WHERE t.resume_id=op.id))
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
        IF a.policy_version=2 THEN
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

CREATE FUNCTION public.vm_job_retained_ready_candidate(parent public.vm_job_cancel_retention_authorities)
RETURNS jsonb LANGUAGE sql IMMUTABLE AS $body$
    SELECT jsonb_build_object('version',1,'kind','vm_job_retained_ready_stop_candidate_v1',
        'owner_kind','job','job_id',parent.job_id,'namespace',parent.namespace,'cluster_id',parent.cluster_id,
        'continuation_id',parent.job_retained_resume_id,'request_id',parent.creation_request_id,
        'provision_generation',parent.provision_generation,'reservation_id',parent.reservation_id,
        'reservation_revision',parent.reservation_revision,'vm_uid',parent.vm_uid,'vmi_uid',parent.vmi_uid,
        'launcher_uid',parent.launcher_uid,'node_uid',parent.node_uid,'pvc_uid',parent.pvc_uid,
        'cleanup_request_id',parent.cleanup_request_id,'cleanup_intent_digest',parent.intent_digest);
$body$;

CREATE FUNCTION public.guard_vm_job_retained_stop_authority() RETURNS trigger LANGUAGE plpgsql AS $body$
DECLARE r public.vm_creation_retries%ROWTYPE;
        op public.vm_job_retained_resumes%ROWTYPE;
        p jsonb := NEW.ready_retention_preflight;
BEGIN
    NEW.admitted_xact_id := pg_current_xact_id();
    IF NEW.policy_version=1 THEN RETURN NEW; END IF;
    SELECT * INTO r FROM public.vm_creation_retries WHERE request_id=NEW.creation_request_id;
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
CREATE TRIGGER vm_job_retained_stop_authority BEFORE INSERT ON public.vm_job_cancel_retention_authorities
FOR EACH ROW EXECUTE FUNCTION public.guard_vm_job_retained_stop_authority();

CREATE FUNCTION public.vm_job_retained_stop_allowed(owner uuid,pvc uuid,request uuid,digest text)
RETURNS boolean LANGUAGE plpgsql AS $body$
DECLARE op public.vm_job_retained_resumes%ROWTYPE;
        r public.vm_creation_retries%ROWTYPE;
        a public.vm_job_cancel_retention_authorities%ROWTYPE;
        candidate jsonb;
        expected_digest text;
BEGIN
    SELECT source.* INTO r FROM public.vm_creation_retries source JOIN public.vm_job_retained_resumes link
      ON source.job_retained_resume_id=link.id JOIN public.jobs j ON j.id=link.job_id
      WHERE source.owner_kind='job' AND source.job_id=owner AND link.pvc_uid=pvc
        AND j.context->>'_vm_job_retained_resume'=link.id::text;
    IF r.request_id IS NULL THEN RETURN false; END IF;
    SELECT * INTO op FROM public.vm_job_retained_resumes WHERE id=r.job_retained_resume_id;
    SELECT * INTO a FROM public.vm_job_cancel_retention_authorities WHERE job_retained_resume_id=op.id;
    IF a.cleanup_admission_id IS NOT NULL THEN
        IF a.cleanup_request_id IS DISTINCT FROM request OR a.intent_digest IS DISTINCT FROM digest THEN RETURN false; END IF;
        PERFORM public.validate_vm_job_cancel_retention(a.cleanup_admission_id,false);
        RETURN true;
    END IF;
    expected_digest := 'sha256:'||encode(sha256(convert_to(format(
        '{"owner_id":"%s","owner_kind":"job","provision_generation":"%s","purge_disk":false,"pvc_uid":"%s","resource":"vm_workspace","source":"job_terminal_vm_release","vm_uid":"%s"}',
        owner,r.provision_generation,pvc,r.observed_vm_uid),'UTF8')),'hex');
    IF digest IS DISTINCT FROM expected_digest OR request IS DISTINCT FROM public.uuid_generate_v5(public.uuid_ns_url(),
        'vm-workspace-cleanup:job_terminal_vm_release:job:'||owner||':'||r.provision_generation||':'||r.observed_vm_uid||':'||pvc) THEN RETURN false; END IF;
    candidate := public.vm_job_retained_stop_candidate(owner,r.provision_generation,r.observed_vm_uid,pvc,NULL);
    RETURN candidate IS NOT NULL;
END;
$body$;

CREATE FUNCTION public.check_vm_job_retained_stop_parent() RETURNS trigger LANGUAGE plpgsql AS $body$
BEGIN
    IF NEW.owner_kind='job' AND NEW.source='job_terminal_vm_release' AND NEW.parent_admission_id IS NULL
       AND EXISTS(SELECT 1 FROM public.vm_job_retained_resumes op JOIN public.vm_creation_retries r
           ON r.job_retained_resume_id=op.id WHERE op.job_id=NEW.owner_id AND op.pvc_uid=NEW.pvc_uid
             AND NEW.request_id=public.uuid_generate_v5(public.uuid_ns_url(),
               'vm-workspace-cleanup:job_terminal_vm_release:job:'||op.job_id||':'||op.provision_generation||':'||r.observed_vm_uid||':'||op.pvc_uid))
       AND NOT EXISTS(SELECT 1 FROM public.vm_job_cancel_retention_authorities a
           WHERE a.cleanup_admission_id=NEW.id AND a.policy_version=2) THEN
        RAISE EXCEPTION 'Retained continuation stop bootstrap is incomplete' USING ERRCODE='23514';
    END IF;
    RETURN NULL;
END;
$body$;
CREATE CONSTRAINT TRIGGER vm_job_retained_stop_parent_commit AFTER INSERT ON public.vm_workspace_cleanup_admissions
DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.check_vm_job_retained_stop_parent();
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
        preflight jsonb;
BEGIN
    SELECT * INTO a FROM public.vm_job_cancel_retention_authorities WHERE cleanup_admission_id=parent_id;
    IF a.cleanup_admission_id IS NULL THEN RETURN false; END IF;
    PERFORM public.validate_vm_job_cancel_retention(parent_id,false);
    SELECT * INTO c FROM public.vm_workspace_cleanup_admissions WHERE id=parent_id;
    SELECT * INTO i FROM public.vm_pre_ssh_stop_intents WHERE cleanup_admission_id=parent_id;
    SELECT * INTO s FROM public.vm_resource_cleanup_stop_receipts WHERE cleanup_admission_id=parent_id;
    SELECT * INTO v FROM public.vm_resource_reservations WHERE id=a.reservation_id;
    preflight := COALESCE(a.ready_retention_preflight,i.retention_preflight);
    retained := jsonb_build_object('version',1,'kind','vm_retained_rootdisk_v1',
        'namespace',a.namespace,'owner_kind','job','owner_id',a.job_id,
        'pvc_name',preflight->>'pvc_name','pvc_uid',a.pvc_uid,
        'dv_uid',preflight->>'dv_uid','ownership','standalone_dv',
        'deleting',false,'no_consumers',true);
    expected := jsonb_build_object('version',1,'kind','vm_cleanup_physical_stop',
        'job_id',a.job_id,'provision_generation',a.provision_generation,'vm_uid',a.vm_uid,
        'vmi_uid',a.vmi_uid,'launcher_uid',a.launcher_uid,'pvc_uid',a.pvc_uid,
        'vm_absent',true,'vmi_absent',true,'launcher_absent',true,'same_generation_replacement',false,
        'pvc_disposition','retained','controller_authenticated',true,'retained_rootdisk',retained);
    digest := 'sha256:'||encode(sha256(convert_to(expected::text,'UTF8')),'hex');
    RETURN c.completed_at IS NOT NULL AND c.outcome='completed'
       AND preflight IS NOT NULL AND (a.ready_retention_preflight IS NOT NULL OR i.cleanup_admission_id IS NOT NULL)
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
       AND (a.ready_retention_preflight IS NOT NULL OR EXISTS (SELECT 1 FROM public.vm_pre_ssh_stop_proofs p WHERE p.cleanup_admission_id=parent_id
           AND p.job_id=a.job_id AND p.provision_generation=a.provision_generation AND p.frozen_digest=i.frozen_digest))
       AND EXISTS (SELECT 1 FROM public.managed_repository_process_zero_receipts z
           WHERE z.owner_kind='job' AND z.owner_id=a.job_id AND z.scope='vm' AND z.provisioner='vm'
           AND z.runtime_incarnation=a.provision_generation::text);
END;
$body$;

CREATE FUNCTION public.vm_job_retained_kept_evidence(operation_id uuid)
RETURNS jsonb LANGUAGE plpgsql AS $body$
DECLARE op public.vm_job_retained_resumes%ROWTYPE;
        a public.vm_job_cancel_retention_authorities%ROWTYPE;
        j public.jobs%ROWTYPE;
BEGIN
    SELECT * INTO op FROM public.vm_job_retained_resumes WHERE id=operation_id;
    SELECT * INTO a FROM public.vm_job_cancel_retention_authorities WHERE job_retained_resume_id=operation_id;
    SELECT * INTO j FROM public.jobs WHERE id=op.job_id;
    IF op.id IS NULL OR a.cleanup_admission_id IS NULL OR a.policy_version<>2
       OR j.status IS NULL OR j.status NOT IN ('cancelled','completed','failed') OR j.execution_lane IS DISTINCT FROM 'stateless'
       OR j.assigned_agent_id IS NOT NULL OR j.parent_job_id IS NOT NULL
       OR j.context->>'_vm_job_retained_resume' IS DISTINCT FROM op.id::text
       OR j.context->'vm'->>'provision_generation' IS DISTINCT FROM op.provision_generation::text
       OR j.context->'vm'->>'vm_uid' IS DISTINCT FROM a.vm_uid::text
       OR j.context->'vm'->>'rootdisk_pvc_uid' IS DISTINCT FROM a.pvc_uid::text
       OR j.context->'vm'->>'status' IS DISTINCT FROM 'deleted'
       OR (j.context ? '_stateless_cancel_cleanup_pending'
           AND j.context->'_stateless_cancel_cleanup_pending' IS DISTINCT FROM 'true'::jsonb)
       OR NOT public.vm_job_cancel_retention_settled(a.cleanup_admission_id)
       OR public.vm_job_cancel_retention_discharged(op.root_retention_admission_id)
       OR NOT EXISTS(SELECT 1 FROM public.run_queue q WHERE q.unit_id=op.job_id
           AND q.unit_kind='worker_batch' AND q.state='done' AND q.leased_by IS NULL AND q.leased_until IS NULL)
       OR EXISTS(SELECT 1 FROM public.vm_creation_retries later WHERE later.owner_kind='job' AND later.job_id=op.job_id
           AND later.request_id<>op.request_id AND later.created_at>=op.created_at)
       OR EXISTS(SELECT 1 FROM public.vm_resource_reservations v JOIN public.vm_creation_retries r USING(request_id)
           WHERE r.owner_kind='job' AND r.job_id=op.job_id AND v.state<>'released')
       OR EXISTS(SELECT 1 FROM public.vm_creation_effects e JOIN public.vm_creation_retries r USING(request_id)
           WHERE r.owner_kind='job' AND r.job_id=op.job_id AND e.state='issued') THEN RETURN NULL; END IF;
    RETURN jsonb_build_object('version',1,'kind','vm_job_retained_resume_kept_compute',
        'resume_id',op.id,'job_id',op.job_id,'allocated_request_id',op.request_id,
        'source_request_id',op.request_id,'provision_generation',op.provision_generation,
        'pvc_uid',op.pvc_uid,'physical_cleanup_admission_id',a.cleanup_admission_id);
END;
$body$;

CREATE FUNCTION public.vm_job_retained_noeffect_evidence(operation_id uuid)
RETURNS jsonb LANGUAGE plpgsql AS $body$
DECLARE op public.vm_job_retained_resumes%ROWTYPE;
        r public.vm_creation_retries%ROWTYPE;
        j public.jobs%ROWTYPE;
BEGIN
    SELECT * INTO op FROM public.vm_job_retained_resumes WHERE id=operation_id;
    SELECT * INTO r FROM public.vm_creation_retries WHERE request_id=op.request_id;
    SELECT * INTO j FROM public.jobs WHERE id=op.job_id;
    IF op.id IS NULL OR r.job_retained_resume_id IS DISTINCT FROM op.id
       OR r.owner_kind IS DISTINCT FROM 'job' OR r.job_id IS DISTINCT FROM op.job_id
       OR r.provision_generation IS DISTINCT FROM op.provision_generation OR r.expected_pvc_uid IS DISTINCT FROM op.pvc_uid
       OR r.state IS DISTINCT FROM 'settled' OR r.resolved_at IS NULL
       OR r.reason IS NULL OR r.reason NOT IN ('creation_never_issued','creation_disposed')
       OR r.ready_at IS NOT NULL OR r.observed_vm_uid IS NOT NULL OR r.boot_counted
       OR r.claim_token IS NOT NULL OR r.claim_expires_at IS NOT NULL
       OR j.status IS DISTINCT FROM 'cancelled' OR j.execution_lane IS DISTINCT FROM 'stateless'
       OR j.assigned_agent_id IS NOT NULL OR j.parent_job_id IS NOT NULL
       OR j.context->>'_vm_job_retained_resume' IS DISTINCT FROM op.id::text
       OR j.context->'vm'->>'provision_generation' IS DISTINCT FROM op.provision_generation::text
       OR j.context->'vm'->'identity_authenticated'='true'::jsonb OR j.context->'vm'->>'vm_uid' IS NOT NULL
       OR (j.context ? '_stateless_cancel_cleanup_pending'
           AND j.context->'_stateless_cancel_cleanup_pending' IS DISTINCT FROM 'true'::jsonb)
       OR NOT public.vm_job_cancel_retention_settled(op.physical_cleanup_admission_id)
       OR public.vm_job_cancel_retention_discharged(op.root_retention_admission_id)
       OR NOT EXISTS(SELECT 1 FROM public.run_queue q WHERE q.unit_id=op.job_id
           AND q.unit_kind='worker_batch' AND q.state='done' AND q.leased_by IS NULL AND q.leased_until IS NULL)
       OR NOT EXISTS(SELECT 1 FROM public.vm_resource_waiters w WHERE w.request_id=r.request_id AND w.state IN ('released','cancelled'))
       OR EXISTS(SELECT 1 FROM public.vm_creation_retries later WHERE later.owner_kind='job' AND later.job_id=op.job_id
           AND later.request_id<>op.request_id AND later.created_at>=op.created_at)
       OR EXISTS(SELECT 1 FROM public.vm_resource_reservations v JOIN public.vm_creation_retries source USING(request_id)
           WHERE source.owner_kind='job' AND source.job_id=op.job_id AND v.state<>'released')
       OR EXISTS(SELECT 1 FROM public.vm_creation_effects e JOIN public.vm_creation_retries source USING(request_id)
           WHERE source.owner_kind='job' AND source.job_id=op.job_id AND e.state='issued')
       OR EXISTS(SELECT 1 FROM public.vm_creation_effects e WHERE e.request_id=r.request_id AND e.effect_kind='vm' AND e.state<>'rejected')
       OR (r.creation_admission_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM public.vm_workspace_cleanup_admissions c
           WHERE c.id=r.creation_admission_id AND c.completed_at IS NOT NULL AND c.outcome=
             CASE r.reason WHEN 'creation_never_issued' THEN 'never_issued' ELSE 'creation_disposed' END))
       OR (r.reason='creation_never_issued' AND (r.observed_pvc_uid IS NOT NULL OR r.cancellation_disposition IS NOT NULL
           OR EXISTS(SELECT 1 FROM public.vm_creation_effects e WHERE e.request_id=r.request_id AND e.state<>'rejected')))
       OR (r.reason='creation_disposed' AND (r.cancellation_disposition->>'disk_policy' IS DISTINCT FROM 'retain'
           OR NOT public.valid_vm_creation_disposition_evidence(r))) THEN RETURN NULL; END IF;
    RETURN jsonb_build_object('version',1,'kind','vm_job_retained_resume_noeffect',
        'terminal_kind',CASE r.reason WHEN 'creation_never_issued' THEN 'never_issued' ELSE 'creation_disposed' END,
        'resume_id',op.id,'job_id',op.job_id,'allocated_request_id',op.request_id,'source_request_id',op.request_id,
        'provision_generation',op.provision_generation,'pvc_uid',op.pvc_uid,
        'physical_cleanup_admission_id',op.physical_cleanup_admission_id,
        'native_source',public.vm_thread_retained_source_snapshot(r));
END;
$body$;

CREATE FUNCTION public.vm_job_retained_terminal_is_current(owner uuid)
RETURNS boolean LANGUAGE sql STABLE AS $body$
    SELECT EXISTS(SELECT 1 FROM public.vm_job_retained_resumes op
        JOIN public.vm_job_retained_resume_terminals t ON t.resume_id=op.id
        JOIN public.jobs j ON j.id=op.job_id
        WHERE op.job_id=owner AND j.context->>'_vm_job_retained_resume'=op.id::text
          AND t.evidence=CASE t.terminal_kind
              WHEN 'source_absent' THEN public.vm_job_retained_source_absent_evidence(op.id)
              WHEN 'kept_compute' THEN public.vm_job_retained_kept_evidence(op.id)
              ELSE public.vm_job_retained_noeffect_evidence(op.id) END);
$body$;

-- Logical nonissuance is already a separate allowed projection in Q1. Extend
-- only that predicate for an exact typed terminal; never insert a VM zero row.
ALTER FUNCTION public.job_vm_creation_never_issued_terminal_source(uuid,text)
RENAME TO job_vm_unprotected_creation_never_issued_terminal_source;
CREATE FUNCTION public.job_vm_creation_never_issued_terminal_source(requested_job uuid,requested_generation text)
RETURNS boolean LANGUAGE sql STABLE AS $body$
    SELECT public.job_vm_unprotected_creation_never_issued_terminal_source(requested_job,requested_generation)
      OR EXISTS(SELECT 1 FROM public.vm_job_retained_resumes op
          JOIN public.vm_job_retained_resume_terminals t ON t.resume_id=op.id
          WHERE op.job_id=requested_job AND op.provision_generation::text=requested_generation
            AND t.terminal_kind IN ('source_absent','never_issued','creation_disposed')
            AND t.evidence=CASE WHEN t.terminal_kind='source_absent'
                THEN public.vm_job_retained_source_absent_evidence(op.id)
                ELSE public.vm_job_retained_noeffect_evidence(op.id) END);
$body$;
CREATE FUNCTION public.vm_job_retained_inherited_rootdisk(retry public.vm_creation_retries)
RETURNS jsonb LANGUAGE plpgsql STABLE AS $body$
DECLARE op public.vm_job_retained_resumes%ROWTYPE;
        a public.vm_job_cancel_retention_authorities%ROWTYPE;
        p jsonb;
BEGIN
    IF retry.owner_kind IS DISTINCT FROM 'job' OR retry.job_retained_resume_id IS NULL
       OR retry.expected_pvc_uid IS NULL OR retry.observed_vm_uid IS NOT NULL OR retry.boot_counted
       OR retry.ready_at IS NOT NULL OR retry.state NOT IN ('cancel_requested','settled')
       OR (retry.observed_pvc_uid IS NOT NULL AND retry.observed_pvc_uid<>retry.expected_pvc_uid)
       OR COALESCE(retry.canonical_request->'workspace_storage','null'::jsonb)<>'null'::jsonb
       OR EXISTS(SELECT 1 FROM public.vm_creation_effects e WHERE e.request_id=retry.request_id
           AND (e.state='issued' OR (e.effect_kind IN ('vm','rootdisk') AND e.state<>'rejected'))) THEN RETURN NULL; END IF;
    SELECT * INTO op FROM public.vm_job_retained_resumes WHERE id=retry.job_retained_resume_id;
    SELECT * INTO a FROM public.vm_job_cancel_retention_authorities WHERE cleanup_admission_id=op.physical_cleanup_admission_id;
    IF op.id IS NULL OR op.job_id IS DISTINCT FROM retry.job_id OR op.request_id IS DISTINCT FROM retry.request_id
       OR op.provision_generation IS DISTINCT FROM retry.provision_generation OR op.pvc_uid IS DISTINCT FROM retry.expected_pvc_uid
       OR op.admitted_xact_id=pg_current_xact_id() OR retry.job_retained_resume_admitted_xact_id=pg_current_xact_id()
       OR a.job_id IS DISTINCT FROM op.job_id OR a.pvc_uid IS DISTINCT FROM op.pvc_uid
       OR a.namespace IS DISTINCT FROM retry.controller_configuration->>'namespace'
       OR a.cluster_id IS DISTINCT FROM retry.controller_configuration->'resource_admission'->>'cluster_id'
       OR NOT public.vm_job_cancel_retention_settled(a.cleanup_admission_id) THEN RETURN NULL; END IF;
    IF retry.state='cancel_requested' THEN
        IF public.vm_job_cancel_retention_discharged(op.root_retention_admission_id)
           OR EXISTS(SELECT 1 FROM public.vm_job_retained_resume_terminals t WHERE t.resume_id=op.id)
           OR NOT EXISTS(SELECT 1 FROM public.jobs j JOIN public.run_queue q ON q.unit_id=j.id
               WHERE j.id=op.job_id AND j.status='cancelled' AND j.assigned_agent_id IS NULL
                 AND j.context->>'_vm_job_retained_resume'=op.id::text
                 AND j.context->'vm'->>'provision_generation'=op.provision_generation::text
                 AND q.state='done' AND q.leased_by IS NULL AND q.leased_until IS NULL)
           OR EXISTS(SELECT 1 FROM public.vm_creation_retries later WHERE later.owner_kind='job' AND later.job_id=op.job_id
               AND (later.created_at,later.request_id)>(retry.created_at,retry.request_id)) THEN RETURN NULL; END IF;
    ELSIF retry.reason IS DISTINCT FROM 'creation_disposed' OR retry.resolved_at IS NULL THEN RETURN NULL;
    END IF;
    p := COALESCE(a.ready_retention_preflight,(SELECT i.retention_preflight FROM public.vm_pre_ssh_stop_intents i
        WHERE i.cleanup_admission_id=a.cleanup_admission_id));
    IF p IS NULL OR p->>'dv_uid' IS NULL OR p->>'pvc_name' IS NULL THEN RETURN NULL; END IF;
    RETURN jsonb_build_object('name',p->>'pvc_name','namespace',a.namespace,'uid',p->>'dv_uid','pvc_uid',a.pvc_uid);
END;
$body$;

ALTER FUNCTION public.valid_vm_creation_resource_completion(public.vm_creation_retries,text,jsonb)
RENAME TO valid_vm_creation_resource_without_job_inheritance;
CREATE FUNCTION public.valid_vm_creation_resource_completion(retry public.vm_creation_retries,stage text,evidence jsonb)
RETURNS boolean LANGUAGE plpgsql STABLE AS $body$
DECLARE resource jsonb;
BEGIN
    IF stage='rootdisk' AND retry.job_retained_resume_id IS NOT NULL
       AND retry.cancellation_disposition->'objects'->'rootdisk' IS NULL
       AND retry.cancellation_disposition->>'disk_policy'='retain' THEN
        resource := public.vm_job_retained_inherited_rootdisk(retry);
        RETURN COALESCE(resource IS NOT NULL AND evidence=retry.cancellation_progress->stage
          AND evidence=jsonb_build_object('version',1,'disposition_id',retry.cancellation_disposition->>'disposition_id',
              'kind','rootdisk_retained')||resource,false);
    END IF;
    RETURN public.valid_vm_creation_resource_without_job_inheritance(retry,stage,evidence);
END;
$body$;
-- A terminal continuation is a separate immutable logical fact. Historical
-- validation never projects its physical ancestor into the current Job.
CREATE FUNCTION public.vm_job_retained_terminal_valid(terminal_id uuid)
RETURNS boolean LANGUAGE plpgsql STABLE AS $body$
DECLARE t public.vm_job_retained_resume_terminals%ROWTYPE;
        op public.vm_job_retained_resumes%ROWTYPE;
        r public.vm_creation_retries%ROWTYPE;
        a public.vm_job_cancel_retention_authorities%ROWTYPE;
        expected jsonb;
BEGIN
    SELECT * INTO t FROM public.vm_job_retained_resume_terminals WHERE id=terminal_id;
    SELECT * INTO op FROM public.vm_job_retained_resumes WHERE id=t.resume_id;
    SELECT * INTO a FROM public.vm_job_cancel_retention_authorities WHERE cleanup_admission_id=t.physical_cleanup_admission_id;
    IF t.id IS NULL OR op.id IS NULL OR t.allocated_request_id<>op.request_id
       OR a.job_id IS DISTINCT FROM op.job_id OR a.pvc_uid IS DISTINCT FROM op.pvc_uid
       OR NOT public.vm_job_cancel_retention_settled(a.cleanup_admission_id) THEN RETURN false; END IF;
    expected := jsonb_build_object('version',1,'resume_id',op.id,'job_id',op.job_id,
        'allocated_request_id',op.request_id,'provision_generation',op.provision_generation,
        'pvc_uid',op.pvc_uid,'physical_cleanup_admission_id',a.cleanup_admission_id);
    IF t.terminal_kind='source_absent' THEN
        IF t.source_request_id IS NOT NULL OR a.cleanup_admission_id<>op.physical_cleanup_admission_id
           OR EXISTS(SELECT 1 FROM public.vm_creation_retries source WHERE source.request_id=op.request_id
               OR (source.owner_kind='job' AND source.job_id=op.job_id AND source.provision_generation=op.provision_generation))
           OR EXISTS(SELECT 1 FROM public.vm_resource_waiters w WHERE w.request_id=op.request_id)
           OR EXISTS(SELECT 1 FROM public.vm_resource_reservations v WHERE v.request_id=op.request_id)
           OR EXISTS(SELECT 1 FROM public.vm_creation_effects e WHERE e.request_id=op.request_id)
           OR EXISTS(SELECT 1 FROM public.vm_workspace_cleanup_admissions c WHERE c.owner_kind='job' AND c.owner_id=op.job_id
               AND c.request_id=public.uuid_generate_v5(public.uuid_ns_url(),'vm-create:'||op.request_id)) THEN RETURN false; END IF;
        expected := expected||jsonb_build_object('kind','vm_job_retained_resume_source_absent');
    ELSE
        SELECT * INTO r FROM public.vm_creation_retries WHERE request_id=op.request_id;
        IF t.source_request_id IS DISTINCT FROM op.request_id OR r.job_retained_resume_id IS DISTINCT FROM op.id
           OR r.job_id IS DISTINCT FROM op.job_id OR r.owner_kind IS DISTINCT FROM 'job'
           OR r.provision_generation IS DISTINCT FROM op.provision_generation OR r.expected_pvc_uid IS DISTINCT FROM op.pvc_uid
           OR r.resolved_at IS NULL OR r.claim_token IS NOT NULL OR r.claim_expires_at IS NOT NULL
           OR EXISTS(SELECT 1 FROM public.vm_resource_reservations v WHERE v.request_id=r.request_id AND v.state<>'released')
           OR EXISTS(SELECT 1 FROM public.vm_resource_waiters w WHERE w.request_id=r.request_id AND w.state NOT IN ('released','cancelled'))
           OR EXISTS(SELECT 1 FROM public.vm_creation_effects e WHERE e.request_id=r.request_id AND e.state='issued') THEN RETURN false; END IF;
        expected := expected||jsonb_build_object('source_request_id',op.request_id);
        IF t.terminal_kind='kept_compute' THEN
            IF a.job_retained_resume_id IS DISTINCT FROM op.id OR a.policy_version<>2
               OR a.creation_request_id<>r.request_id OR r.state<>'succeeded'
               OR r.observed_vm_uid IS DISTINCT FROM a.vm_uid OR r.observed_pvc_uid IS DISTINCT FROM a.pvc_uid THEN RETURN false; END IF;
            expected := expected||jsonb_build_object('kind','vm_job_retained_resume_kept_compute');
        ELSE
            IF t.terminal_kind NOT IN ('never_issued','creation_disposed') OR r.state<>'settled'
               OR r.reason IS DISTINCT FROM (CASE t.terminal_kind WHEN 'never_issued' THEN 'creation_never_issued' ELSE 'creation_disposed' END)
               OR a.cleanup_admission_id<>op.physical_cleanup_admission_id
               OR r.observed_vm_uid IS NOT NULL OR r.ready_at IS NOT NULL OR r.boot_counted
               OR EXISTS(SELECT 1 FROM public.vm_creation_effects e WHERE e.request_id=r.request_id AND e.effect_kind='vm' AND e.state<>'rejected')
               OR (t.terminal_kind='creation_disposed' AND NOT public.valid_vm_creation_disposition_evidence(r)) THEN RETURN false; END IF;
            expected := expected||jsonb_build_object('kind','vm_job_retained_resume_noeffect',
                'terminal_kind',t.terminal_kind,'native_source',public.vm_thread_retained_source_snapshot(r));
        END IF;
    END IF;
    RETURN t.evidence=expected;
END;
$body$;
CREATE OR REPLACE FUNCTION public.vm_job_retained_terminal_is_current(owner uuid)
RETURNS boolean LANGUAGE sql STABLE AS $body$
    SELECT EXISTS(SELECT 1 FROM public.vm_job_retained_resumes op
        JOIN public.vm_job_retained_resume_terminals t ON t.resume_id=op.id
        JOIN public.jobs j ON j.id=op.job_id JOIN public.run_queue q ON q.unit_id=j.id
        JOIN public.vm_job_cancel_retention_authorities a ON a.cleanup_admission_id=t.physical_cleanup_admission_id
        WHERE op.job_id=owner AND j.context->>'_vm_job_retained_resume'=op.id::text
          AND j.status IN ('cancelled','completed','failed') AND j.execution_lane='stateless'
          AND j.assigned_agent_id IS NULL AND j.parent_job_id IS NULL
          AND q.unit_kind='worker_batch' AND q.state='done' AND q.leased_by IS NULL AND q.leased_until IS NULL
          AND NOT EXISTS(SELECT 1 FROM public.vm_job_retained_resumes later WHERE later.job_id=owner
              AND (later.created_at,later.id)>(op.created_at,op.id))
          AND (CASE WHEN t.terminal_kind='kept_compute' THEN
              j.context->'vm'->>'provision_generation'=op.provision_generation::text
              AND j.context->'vm'->>'vm_uid'=a.vm_uid::text
              AND j.context->'vm'->>'rootdisk_pvc_uid'=a.pvc_uid::text
              AND j.context->'vm'->>'status'='deleted'
            ELSE COALESCE(j.context->'vm','null'::jsonb)='null'::jsonb OR (
              j.context->'vm'->>'provision_generation'=op.provision_generation::text
              AND j.context->'vm'->>'vm_uid' IS NULL
              AND COALESCE(j.context->'vm'->'identity_authenticated','false'::jsonb)<>'true'::jsonb) END)
          AND public.vm_job_retained_terminal_valid(t.id));
$body$;
CREATE OR REPLACE FUNCTION public.job_vm_creation_never_issued_terminal_source(requested_job uuid,requested_generation text)
RETURNS boolean LANGUAGE sql STABLE AS $body$
    SELECT public.job_vm_unprotected_creation_never_issued_terminal_source(requested_job,requested_generation)
      OR EXISTS(SELECT 1 FROM public.vm_job_retained_resumes op
          JOIN public.vm_job_retained_resume_terminals t ON t.resume_id=op.id
          WHERE op.job_id=requested_job AND op.provision_generation::text=requested_generation
            AND t.terminal_kind IN ('source_absent','never_issued','creation_disposed')
            AND public.vm_job_retained_terminal_valid(t.id));
$body$;
ALTER TABLE public.vm_job_retained_disk_purge_authorities
    ADD COLUMN retained_terminal_id uuid,
    ADD CONSTRAINT vm_retained_purge_terminal_fkey FOREIGN KEY (retained_terminal_id)
        REFERENCES public.vm_job_retained_resume_terminals(id) NOT VALID;
-- The live validator requires the current owner. Historical receipt digests
-- use only immutable terminal/link material and survive logical Job deletion.
CREATE FUNCTION public.vm_job_retained_purge_tail(parent_id uuid, require_current boolean DEFAULT true)
RETURNS jsonb LANGUAGE plpgsql STABLE AS $body$
DECLARE d public.vm_job_retained_disk_purge_authorities%ROWTYPE;
        t public.vm_job_retained_resume_terminals%ROWTYPE;
        op public.vm_job_retained_resumes%ROWTYPE;
        head public.vm_job_retained_resumes%ROWTYPE;
        a public.vm_job_cancel_retention_authorities%ROWTYPE;
        next_id uuid;
        visited uuid[] := '{}';
        tail jsonb := '[]';
BEGIN
    SELECT * INTO d FROM public.vm_job_retained_disk_purge_authorities WHERE cleanup_admission_id=parent_id;
    IF d.retained_terminal_id IS NULL THEN RETURN NULL; END IF;
    SELECT source.* INTO head FROM public.vm_job_retained_resumes source
        JOIN public.vm_job_retained_resume_terminals edge ON edge.resume_id=source.id WHERE edge.id=d.retained_terminal_id;
    IF head.job_id IS DISTINCT FROM d.job_id OR head.pvc_uid IS DISTINCT FROM d.pvc_uid
       OR (require_current AND (NOT public.vm_job_retained_terminal_is_current(d.job_id)
       OR NOT EXISTS(SELECT 1 FROM public.jobs j WHERE j.id=d.job_id
           AND j.context->>'_vm_job_retained_resume'=head.id::text
           AND j.context->'_stateless_delete_pending'='true'::jsonb))) THEN RETURN NULL; END IF;
    next_id := d.retained_terminal_id;
    WHILE next_id IS NOT NULL LOOP
        IF next_id=ANY(visited) THEN RETURN NULL; END IF;
        visited := array_append(visited,next_id);
        SELECT * INTO t FROM public.vm_job_retained_resume_terminals WHERE id=next_id;
        SELECT * INTO op FROM public.vm_job_retained_resumes WHERE id=t.resume_id;
        IF t.id IS NULL OR op.id IS NULL OR op.job_id<>d.job_id OR op.pvc_uid<>d.pvc_uid
           OR op.root_retention_admission_id<>head.root_retention_admission_id
           OR (require_current AND (op.admitted_xact_id=pg_current_xact_id() OR t.admitted_xact_id=pg_current_xact_id()
           OR NOT public.vm_job_retained_terminal_valid(t.id))) THEN RETURN NULL; END IF;
        IF next_id=d.retained_terminal_id THEN
            SELECT * INTO a FROM public.vm_job_cancel_retention_authorities WHERE cleanup_admission_id=t.physical_cleanup_admission_id;
            IF a.creation_request_id IS DISTINCT FROM d.final_request_id OR a.vm_uid IS DISTINCT FROM d.vm_uid
               OR a.provision_generation IS DISTINCT FROM d.provision_generation THEN RETURN NULL; END IF;
        END IF;
        tail := tail||jsonb_build_array(jsonb_build_object('resume_id',op.id,'terminal_id',t.id,
            'request_id',op.request_id,'provision_generation',op.provision_generation,
            'predecessor_terminal_id',op.predecessor_terminal_id,'evidence',t.evidence));
        next_id := op.predecessor_terminal_id;
    END LOOP;
    IF op.physical_cleanup_admission_id<>head.root_retention_admission_id
       OR EXISTS(SELECT 1 FROM public.vm_job_retained_resumes foreign_op
           WHERE foreign_op.job_id=d.job_id AND NOT EXISTS(SELECT 1 FROM public.vm_job_retained_resume_terminals edge
               WHERE edge.resume_id=foreign_op.id AND edge.id=ANY(visited))) THEN RETURN NULL; END IF;
    RETURN tail;
END;
$body$;
ALTER FUNCTION public.vm_job_retained_disk_purge_chain_digest(uuid) RENAME TO vm_job_retained_disk_physical_chain_digest;
CREATE FUNCTION public.vm_job_retained_disk_purge_chain_digest(parent_id uuid)
RETURNS text LANGUAGE sql STABLE AS $body$
    SELECT CASE WHEN d.retained_terminal_id IS NULL THEN public.vm_job_retained_disk_physical_chain_digest(parent_id)
        ELSE 'sha256:'||encode(sha256(convert_to(jsonb_build_object('version',2,
            'physical_chain_digest',public.vm_job_retained_disk_physical_chain_digest(parent_id),
            'terminal_tail',public.vm_job_retained_purge_tail(parent_id,false))::text,'UTF8')),'hex') END
    FROM public.vm_job_retained_disk_purge_authorities d WHERE d.cleanup_admission_id=parent_id;
$body$;
CREATE OR REPLACE FUNCTION public.validate_vm_job_retained_disk_purge(
    parent_id uuid, require_receipt boolean DEFAULT false
) RETURNS boolean LANGUAGE plpgsql AS $body$
DECLARE
    d public.vm_job_retained_disk_purge_authorities%ROWTYPE;
    parent public.vm_workspace_cleanup_admissions%ROWTYPE;
    owner_row public.jobs%ROWTYPE;
    retry public.vm_creation_retries%ROWTYPE;
    old_retry public.vm_creation_retries%ROWTYPE;
    old_parent public.vm_workspace_cleanup_admissions%ROWTYPE;
    old_stop public.vm_resource_cleanup_stop_receipts%ROWTYPE;
    charge public.vm_resource_reservations%ROWTYPE;
    final_charge public.vm_resource_reservations%ROWTYPE;
    link public.vm_job_retained_disk_purge_predecessors%ROWTYPE;
    receipt public.vm_job_retained_disk_purge_receipts%ROWTYPE;
    instance public.srw_workspace_instances%ROWTYPE;
    binding jsonb;
    expected_digest text;
    expected_request uuid;
    link_count bigint := 0;
    retry_count bigint := 0;
    previous_generation bigint := 0;
    previous_execution_id uuid;
    previous_execution_revision text;
    previous_execution_generation bigint;
    scope_namespace text;
    scope_cluster_id text;
    terminal_tail jsonb;
BEGIN
    -- The public path already holds owner/PVC advisory locks. These row locks
    -- also make a direct/generic completion trigger re-read the actual current
    -- owner, parent and Released workspace rather than trusting a prior API
    -- response or a stale candidate projection.
    SELECT * INTO d FROM public.vm_job_retained_disk_purge_authorities
     WHERE cleanup_admission_id=parent_id FOR SHARE;
    SELECT * INTO owner_row FROM public.jobs WHERE id=d.job_id FOR UPDATE;
    PERFORM 1 FROM public.vm_job_creation_owners
     WHERE job_id=d.job_id FOR SHARE;
    SELECT * INTO parent FROM public.vm_workspace_cleanup_admissions
     WHERE id=parent_id FOR SHARE;
    SELECT * INTO retry FROM public.vm_creation_retries
     WHERE request_id=d.final_request_id FOR SHARE;
    SELECT * INTO final_charge FROM public.vm_resource_reservations
     WHERE request_id=d.final_request_id FOR SHARE;
    terminal_tail := public.vm_job_retained_purge_tail(parent_id);
    scope_namespace := retry.controller_configuration->>'namespace';
    scope_cluster_id := final_charge.cluster_id;
    expected_request := public.uuid_generate_v5(public.uuid_ns_url(),
        'vm-workspace-cleanup:public_vm_delete:job:'||d.job_id::text||':'||
        d.provision_generation::text||':'||d.vm_uid::text||':'||d.pvc_uid::text);
    expected_digest := 'sha256:'||encode(sha256(convert_to(format(
        '{"owner_id":"%s","owner_kind":"job","provision_generation":"%s",' ||
        '"purge_disk":true,"pvc_uid":"%s","resource":"vm_workspace",' ||
        '"source":"public_vm_delete","vm_uid":"%s"}',
        d.job_id,d.provision_generation,d.pvc_uid,d.vm_uid),'UTF8')),'hex');
    IF d.cleanup_admission_id IS NULL OR parent.id IS NULL
       OR parent.owner_kind IS DISTINCT FROM 'job'
       OR parent.owner_id IS DISTINCT FROM d.job_id
       OR parent.source IS DISTINCT FROM 'public_vm_delete'
       OR parent.parent_admission_id IS NOT NULL
       OR parent.pvc_uid IS DISTINCT FROM d.pvc_uid
       OR parent.request_id IS DISTINCT FROM expected_request
       OR d.cleanup_request_id IS DISTINCT FROM expected_request
       OR parent.intent_digest IS DISTINCT FROM expected_digest
       OR d.intent_digest IS DISTINCT FROM expected_digest
       OR (parent.completed_at IS NOT NULL AND parent.outcome IS DISTINCT FROM 'completed')
       OR owner_row.id IS NULL OR owner_row.parent_job_id IS NOT NULL
       OR owner_row.status NOT IN ('completed','failed','cancelled')
       OR NOT EXISTS (SELECT 1 FROM public.vm_job_creation_owners o
           WHERE o.job_id=d.job_id AND o.live_job_id=d.job_id AND o.deleted_at IS NULL)
       OR retry.request_id IS NULL OR retry.owner_kind IS DISTINCT FROM 'job'
       OR retry.job_id IS DISTINCT FROM d.job_id
       OR retry.provision_generation IS DISTINCT FROM d.provision_generation
       OR retry.state IS DISTINCT FROM 'succeeded' OR retry.resolved_at IS NULL
       OR retry.observed_vm_uid IS DISTINCT FROM d.vm_uid
       OR retry.observed_pvc_uid IS DISTINCT FROM d.pvc_uid
       OR retry.creation_admission_id IS NULL
       OR final_charge.id IS NULL OR final_charge.state IS DISTINCT FROM 'released'
       OR scope_namespace IS NULL OR length(scope_namespace) NOT BETWEEN 1 AND 63
       OR scope_namespace !~ '^[a-z0-9]([-a-z0-9]*[a-z0-9])?$'
       OR scope_cluster_id IS NULL OR length(scope_cluster_id)=0
       OR retry.controller_configuration->'resource_admission'->>'cluster_id'
           IS DISTINCT FROM scope_cluster_id
       OR (d.retained_terminal_id IS NULL AND EXISTS(SELECT 1 FROM public.vm_job_retained_resumes typed WHERE typed.job_id=d.job_id))
       OR (d.retained_terminal_id IS NOT NULL AND terminal_tail IS NULL)
       OR (d.retained_terminal_id IS NULL AND (false
       OR owner_row.context->'vm'->>'provision_generation' IS DISTINCT FROM d.provision_generation::text
       OR owner_row.context->'vm'->>'vm_uid' IS DISTINCT FROM d.vm_uid::text
       OR owner_row.context->'vm'->>'rootdisk_pvc_uid' IS DISTINCT FROM d.pvc_uid::text
       OR EXISTS (SELECT 1 FROM public.vm_creation_retries later
           WHERE later.owner_kind='job' AND later.job_id=d.job_id
             AND (later.created_at,later.request_id)>(retry.created_at,retry.request_id))
       ))
       OR EXISTS (SELECT 1 FROM public.vm_resource_reservations held
           JOIN public.vm_creation_retries source USING (request_id)
           WHERE source.owner_kind='job' AND source.job_id=d.job_id
             AND held.state<>'released')
       OR EXISTS (SELECT 1 FROM public.vm_creation_effects effect
           JOIN public.vm_creation_retries source USING (request_id)
           WHERE source.owner_kind='job' AND source.job_id=d.job_id
             AND effect.state='issued')
       OR EXISTS (SELECT 1 FROM public.vm_resource_waiters waiter
           WHERE waiter.owner_kind='job' AND waiter.job_id=d.job_id
             AND waiter.state NOT IN ('released','cancelled'))
       OR EXISTS (SELECT 1 FROM public.vm_workspace_recoveries recovery
           WHERE recovery.resolved_at IS NULL AND
             ((recovery.owner_kind='job' AND recovery.owner_id=d.job_id)
              OR recovery.root_pvc_uid=d.pvc_uid))
       OR EXISTS (SELECT 1 FROM public.vm_workspace_recovery_retention_pins pin
           WHERE pin.pvc_uid=d.pvc_uid AND pin.released_at IS NULL)
       OR EXISTS (SELECT 1 FROM public.vm_idle_access_leases lease
           WHERE lease.owner_kind='job' AND lease.owner_id=d.job_id
             AND lease.closed_at IS NULL AND lease.expires_at>clock_timestamp())
       OR EXISTS (SELECT 1 FROM public.vm_workspace_cleanup_admissions other
           WHERE other.owner_kind='job' AND other.owner_id=d.job_id
             AND other.completed_at IS NULL AND other.id<>parent_id
             AND other.source<>'terminal_checkpoint_prune'
             AND NOT (other.source='controller_rootdisk_delete'
               AND other.parent_admission_id=parent_id
               AND other.pvc_uid=d.pvc_uid
               AND NOT require_receipt
               AND other.request_id IS NOT NULL
               AND other.intent_digest ~ '^sha256:[0-9a-f]{64}$')) THEN
        RAISE EXCEPTION 'Job retained disk purge current authority changed'
            USING ERRCODE='23514',CONSTRAINT='vm_job_retained_purge_authority';
    END IF;
    IF d.binding_kind='bound' THEN
        binding := retry.canonical_request->'workspace_storage';
        IF jsonb_typeof(binding) IS DISTINCT FROM 'object'
           OR jsonb_typeof(binding->'generation') IS DISTINCT FROM 'number'
           OR (binding->>'generation') !~ '^[1-9][0-9]*$' THEN
            RAISE EXCEPTION 'Job retained disk workspace generation unproven'
                USING ERRCODE='23514',CONSTRAINT='vm_job_retained_purge_binding';
        END IF;
        SELECT * INTO instance FROM public.srw_workspace_instances
         WHERE id=d.workspace_instance_id FOR UPDATE;
        IF jsonb_typeof(binding) IS DISTINCT FROM 'object'
           OR binding->>'uid' IS DISTINCT FROM d.workspace_instance_id::text
           OR binding->>'generation' IS DISTINCT FROM d.workspace_generation::text
           OR (d.workspace_generation>1 AND
               binding->>'pvc_uid' IS DISTINCT FROM d.pvc_uid::text)
           OR (binding->>'pvc_uid' IS NOT NULL AND
               binding->>'pvc_uid' IS DISTINCT FROM d.pvc_uid::text)
           OR binding->>'owner_kind' IS DISTINCT FROM 'job'
           OR binding->>'owner_id' IS DISTINCT FROM d.job_id::text
           OR instance.id IS NULL OR instance.generation IS DISTINCT FROM d.workspace_generation
           OR instance.pvc_uid IS DISTINCT FROM d.pvc_uid::text
           OR instance.status IS DISTINCT FROM 'Released'
           OR instance.execution_id IS NOT NULL OR instance.pod_uid IS NOT NULL
           OR NOT EXISTS (SELECT 1 FROM public.srw_execution_workspace_bindings b
                WHERE b.execution_id=retry.execution_id
                  AND b.instance_id=d.workspace_instance_id)
           OR EXISTS (SELECT 1 FROM public.srw_workspace_instances other
                WHERE other.id<>d.workspace_instance_id AND other.pvc_uid=d.pvc_uid::text)
           OR EXISTS (SELECT 1 FROM public.srw_workspace_instances other
                WHERE other.id=d.workspace_instance_id AND other.status<>'Released') THEN
            RAISE EXCEPTION 'Job retained disk workspace release unproven'
                USING ERRCODE='23514',CONSTRAINT='vm_job_retained_purge_binding';
        END IF;
    ELSE
        IF d.binding_kind IS DISTINCT FROM 'unbound'
           OR d.workspace_instance_id IS NOT NULL OR d.workspace_generation IS NOT NULL
           OR COALESCE(retry.canonical_request->'workspace_storage','null'::jsonb)<>'null'::jsonb
           OR EXISTS (SELECT 1 FROM public.srw_workspace_instances foreign_workspace
                WHERE foreign_workspace.pvc_uid=d.pvc_uid::text)
           OR EXISTS (SELECT 1 FROM public.srw_execution_workspace_bindings b
                WHERE b.execution_id=retry.execution_id)
           OR EXISTS (SELECT 1 FROM public.vm_workspace_recovery_jobs r
                WHERE r.job_id=d.job_id AND r.resolved_at IS NULL) THEN
            RAISE EXCEPTION 'Job retained rootdisk has another owner'
                USING ERRCODE='23514',CONSTRAINT='vm_job_retained_purge_unbound';
        END IF;
    END IF;

    FOR link IN SELECT p.* FROM public.vm_job_retained_disk_purge_predecessors p
        JOIN public.vm_creation_retries source ON source.request_id=p.source_request_id
        WHERE p.cleanup_admission_id=parent_id
        ORDER BY source.created_at,source.request_id
        FOR SHARE OF p
    LOOP
        link_count := link_count+1;
        SELECT * INTO old_retry FROM public.vm_creation_retries
         WHERE request_id=link.source_request_id FOR SHARE;
        SELECT * INTO old_parent FROM public.vm_workspace_cleanup_admissions
         WHERE id=link.old_cleanup_admission_id FOR SHARE;
        SELECT * INTO old_stop FROM public.vm_resource_cleanup_stop_receipts
         WHERE cleanup_admission_id=link.old_cleanup_admission_id FOR SHARE;
        SELECT * INTO charge FROM public.vm_resource_reservations
         WHERE id=link.reservation_id FOR SHARE;
        IF old_retry.request_id IS NULL OR old_retry.owner_kind IS DISTINCT FROM 'job'
           OR old_retry.job_id IS DISTINCT FROM d.job_id
           OR old_retry.state IS DISTINCT FROM 'succeeded'
           OR old_retry.resolved_at IS NULL
           OR old_retry.observed_pvc_uid IS DISTINCT FROM d.pvc_uid
           OR old_retry.observed_vm_uid IS DISTINCT FROM old_stop.vm_uid
           OR old_retry.creation_admission_id IS NULL
           OR old_stop.cleanup_admission_id IS NULL
           OR old_stop.request_id IS DISTINCT FROM old_retry.request_id
           OR old_stop.job_id IS DISTINCT FROM d.job_id
           OR old_stop.provision_generation IS DISTINCT FROM old_retry.provision_generation
           OR old_stop.pvc_uid IS DISTINCT FROM d.pvc_uid
           OR old_stop.reservation_id IS DISTINCT FROM link.reservation_id
           OR old_stop.intent_digest IS DISTINCT FROM old_parent.intent_digest
           OR old_stop.stop_evidence->>'pvc_disposition' IS DISTINCT FROM 'retained'
           OR old_stop.stop_evidence->>'vm_absent' IS DISTINCT FROM 'true'
           OR old_stop.stop_evidence->>'vmi_absent' IS DISTINCT FROM 'true'
           OR old_stop.stop_evidence->>'launcher_absent' IS DISTINCT FROM 'true'
           OR old_stop.stop_evidence->>'controller_authenticated' IS DISTINCT FROM 'true'
           OR old_stop.stop_evidence->>'same_generation_replacement' IS DISTINCT FROM 'false'
           OR link.stop_evidence_digest IS DISTINCT FROM
               'sha256:'||encode(sha256(convert_to(old_stop.stop_evidence::text,'UTF8')),'hex')
           OR old_parent.id IS NULL OR old_parent.owner_kind IS DISTINCT FROM 'job'
           OR old_parent.owner_id IS DISTINCT FROM d.job_id
           OR old_parent.pvc_uid IS DISTINCT FROM d.pvc_uid
           OR old_parent.parent_admission_id IS NOT NULL
           OR old_parent.completed_at IS NULL OR old_parent.outcome IS DISTINCT FROM 'completed'
           OR old_parent.source NOT IN (
               'dispatcher_vm_recycle','lifecycle_vm_reap',
               'job_terminal_vm_release','public_vm_delete')
           OR charge.id IS NULL OR charge.request_id IS DISTINCT FROM old_retry.request_id
           OR charge.revision IS DISTINCT FROM link.reservation_revision
           OR charge.state IS DISTINCT FROM 'released'
           OR charge.cluster_id IS DISTINCT FROM scope_cluster_id
           OR old_retry.controller_configuration->>'namespace' IS DISTINCT FROM scope_namespace
           OR old_retry.controller_configuration->'resource_admission'->>'cluster_id'
               IS DISTINCT FROM scope_cluster_id
           OR charge.vm_uid IS DISTINCT FROM old_stop.vm_uid
           OR charge.release_evidence->>'kind' IS DISTINCT FROM 'exact_cleanup_compute_absent'
           OR charge.release_evidence->>'cleanup_admission_id' IS DISTINCT FROM old_parent.id::text
           OR charge.release_evidence->>'stop_evidence_digest' IS DISTINCT FROM link.stop_evidence_digest
           OR NOT EXISTS (SELECT 1 FROM public.managed_repository_process_zero_receipts z
               WHERE z.owner_kind='job' AND z.owner_id=d.job_id
                 AND z.scope='vm' AND z.provisioner='vm'
                 AND z.runtime_incarnation=old_retry.provision_generation::text)
           OR EXISTS (SELECT 1 FROM public.vm_pre_ssh_stop_intents i
               WHERE i.job_id=d.job_id AND i.provision_generation=old_retry.provision_generation
                 AND (i.cleanup_admission_id<>old_parent.id OR
                 NOT EXISTS (SELECT 1 FROM public.vm_pre_ssh_stop_proofs proof
                     WHERE proof.cleanup_admission_id=i.cleanup_admission_id
                       AND proof.job_id=d.job_id
                       AND proof.provision_generation=old_retry.provision_generation
                       AND proof.terminal_evidence->>'vm_uid'=old_stop.vm_uid::text
                       AND proof.terminal_evidence->>'launcher_uid'=old_stop.launcher_uid::text)))
           OR EXISTS (SELECT 1 FROM public.vm_resource_recovery_successors successor
               WHERE successor.reservation_id=charge.id
                 AND (successor.owner_id<>d.job_id
                      OR successor.provision_generation<>old_retry.provision_generation
                      OR successor.vm_uid<>old_stop.vm_uid
                      OR successor.root_pvc_uid<>d.pvc_uid)) THEN
            RAISE EXCEPTION 'Job retained disk predecessor changed'
                USING ERRCODE='23514',CONSTRAINT='vm_job_retained_purge_predecessor';
        END IF;
        IF d.binding_kind='bound' THEN
            binding := old_retry.canonical_request->'workspace_storage';
            IF jsonb_typeof(binding) IS DISTINCT FROM 'object'
               OR jsonb_typeof(binding->'generation') IS DISTINCT FROM 'number'
               OR (binding->>'generation') !~ '^[1-9][0-9]*$' THEN
                RAISE EXCEPTION 'Job retained disk predecessor generation unproven'
                    USING ERRCODE='23514',CONSTRAINT='vm_job_retained_purge_predecessor_binding';
            END IF;
            IF jsonb_typeof(binding) IS DISTINCT FROM 'object'
               OR binding->>'uid' IS DISTINCT FROM d.workspace_instance_id::text
               OR binding->>'owner_id' IS DISTINCT FROM d.job_id::text
               OR binding->>'owner_kind' IS DISTINCT FROM 'job'
               OR (binding->>'pvc_uid' IS NOT NULL AND
                   binding->>'pvc_uid' IS DISTINCT FROM d.pvc_uid::text)
               OR ((binding->>'generation')::bigint>1 AND
                   binding->>'pvc_uid' IS DISTINCT FROM d.pvc_uid::text)
               OR (binding->>'generation')::bigint<previous_generation
               OR (binding->>'generation')::bigint>d.workspace_generation
               OR ((binding->>'generation')::bigint=previous_generation
                   AND (old_retry.execution_id IS DISTINCT FROM previous_execution_id
                     OR old_retry.execution_revision IS DISTINCT FROM previous_execution_revision
                     OR old_retry.execution_generation IS DISTINCT FROM previous_execution_generation))
               OR NOT EXISTS (SELECT 1 FROM public.srw_execution_workspace_bindings b
                    WHERE b.execution_id=old_retry.execution_id
                      AND b.instance_id=d.workspace_instance_id) THEN
                RAISE EXCEPTION 'Job retained disk predecessor binding changed'
                    USING ERRCODE='23514',CONSTRAINT='vm_job_retained_purge_predecessor_binding';
            END IF;
            previous_generation := (binding->>'generation')::bigint;
            previous_execution_id := old_retry.execution_id;
            previous_execution_revision := old_retry.execution_revision;
            previous_execution_generation := old_retry.execution_generation;
        ELSE
            IF COALESCE(old_retry.canonical_request->'workspace_storage','null'::jsonb)<>'null'::jsonb THEN
                RAISE EXCEPTION 'Job retained rootdisk predecessor changed'
                    USING ERRCODE='23514';
            END IF;
        END IF;
    END LOOP;
    IF NOT EXISTS (SELECT 1 FROM public.vm_job_retained_disk_purge_predecessors final_link
        JOIN public.vm_resource_cleanup_stop_receipts final_stop
          ON final_stop.cleanup_admission_id=final_link.old_cleanup_admission_id
        WHERE final_link.cleanup_admission_id=parent_id
          AND final_link.source_request_id=d.final_request_id
          AND final_stop.vmi_uid=d.vmi_uid
          AND final_stop.launcher_uid=d.launcher_uid) THEN
        RAISE EXCEPTION 'Job retained disk final runtime identity changed'
            USING ERRCODE='23514',CONSTRAINT='vm_job_retained_purge_predecessor';
    END IF;
    SELECT count(*) INTO retry_count FROM public.vm_creation_retries r
     WHERE r.owner_kind='job' AND r.job_id=d.job_id;
    IF link_count=0 OR retry_count<link_count
       OR NOT EXISTS (SELECT 1 FROM public.vm_job_retained_disk_purge_predecessors p
           WHERE p.cleanup_admission_id=parent_id AND p.source_request_id=d.final_request_id)
       OR EXISTS (SELECT 1 FROM public.vm_resource_cleanup_stop_receipts s
           WHERE s.job_id=d.job_id AND s.stop_evidence->>'pvc_disposition'='retained'
             AND NOT EXISTS (SELECT 1 FROM public.vm_job_retained_disk_purge_predecessors p
                 WHERE p.cleanup_admission_id=parent_id AND p.source_request_id=s.request_id)) THEN
        RAISE EXCEPTION 'Job retained disk predecessor chain incomplete'
            USING ERRCODE='23514',CONSTRAINT='vm_job_retained_purge_chain';
    END IF;
    -- An earlier retry outside this same-PVC retained chain still needs its
    -- own already-completed ordinary purged or never-issued terminal packet.
    -- The 0326 logical predecessor guard continues to reject physical A then
    -- never-issued B; this branch does not fabricate an attestor for B.
    FOR old_retry IN SELECT r.* FROM public.vm_creation_retries r
      WHERE r.owner_kind='job' AND r.job_id=d.job_id
        AND NOT EXISTS (SELECT 1 FROM public.vm_job_retained_disk_purge_predecessors p
            WHERE p.cleanup_admission_id=parent_id AND p.source_request_id=r.request_id)
      ORDER BY r.created_at,r.request_id
    LOOP
        IF terminal_tail IS NOT NULL AND EXISTS(SELECT 1 FROM jsonb_array_elements(terminal_tail) item
            JOIN public.vm_job_retained_resume_terminals t ON t.id::text=item->>'terminal_id'
            WHERE t.source_request_id=old_retry.request_id AND t.terminal_kind IN ('never_issued','creation_disposed')) THEN
            CONTINUE;
        END IF;
        IF (old_retry.created_at,old_retry.request_id)>=(retry.created_at,retry.request_id)
           OR COALESCE(public.vm_job_terminal_prior_packet_evidence(old_retry.request_id)->>'kind','')
              NOT IN ('never_issued','physical_stop') THEN
            RAISE EXCEPTION 'Job retained disk has unclassified retry history'
                USING ERRCODE='23514',CONSTRAINT='vm_job_retained_purge_chain';
        END IF;
    END LOOP;
    SELECT * INTO receipt FROM public.vm_job_retained_disk_purge_receipts
     WHERE cleanup_admission_id=parent_id;
    IF require_receipt AND receipt.cleanup_admission_id IS NULL THEN
        RAISE EXCEPTION 'Job retained disk purge lacks signed receipt'
            USING ERRCODE='23514',CONSTRAINT='vm_job_retained_purge_receipt';
    END IF;
    IF receipt.cleanup_admission_id IS NOT NULL AND (
       receipt.chain_digest IS DISTINCT FROM public.vm_job_retained_disk_purge_chain_digest(parent_id)
       OR receipt.purge_evidence IS DISTINCT FROM (jsonb_build_object(
           'version',1,'kind','vm_cleanup_physical_stop','job_id',d.job_id,
           'provision_generation',d.provision_generation,'vm_uid',d.vm_uid,
           'vmi_uid',d.vmi_uid,'launcher_uid',d.launcher_uid,'pvc_uid',d.pvc_uid,
           'vm_absent',true,'vmi_absent',true,'launcher_absent',true,
           'same_generation_replacement',false,'controller_authenticated',true,
           'pvc_disposition','purged',
           'controller_scope',jsonb_build_object(
               'version',1,'namespace',scope_namespace,'cluster_id',scope_cluster_id)) ||
           CASE WHEN d.binding_kind='bound'
               THEN jsonb_build_object('captured_workspace_storage',
                   jsonb_set(retry.canonical_request->'workspace_storage',
                       '{pvc_uid}',to_jsonb(d.pvc_uid::text),true))
               ELSE '{}'::jsonb END)) THEN
        RAISE EXCEPTION 'Job retained disk purge receipt changed'
            USING ERRCODE='23514',CONSTRAINT='vm_job_retained_purge_receipt';
    END IF;
    RETURN true;
END;
$body$;

ALTER TABLE public.vm_job_creation_terminal_packets
    DROP CONSTRAINT vm_job_creation_terminal_packets_terminal_kind_check,
    ADD CONSTRAINT vm_job_creation_terminal_packets_terminal_kind_check
        CHECK (terminal_kind IN ('never_issued','physical_stop','retained_late_purge','retained_noeffect')) NOT VALID;
ALTER FUNCTION public.vm_job_terminal_packet_evidence(uuid) RENAME TO vm_job_terminal_packet_without_retained_noeffect;
CREATE FUNCTION public.vm_job_terminal_packet_evidence(source_request uuid)
RETURNS jsonb LANGUAGE plpgsql AS $body$
DECLARE r public.vm_creation_retries%ROWTYPE;
        t public.vm_job_retained_resume_terminals%ROWTYPE;
        d public.vm_job_retained_disk_purge_authorities%ROWTYPE;
        tail jsonb;
BEGIN
    SELECT * INTO r FROM public.vm_creation_retries WHERE request_id=source_request;
    SELECT * INTO t FROM public.vm_job_retained_resume_terminals WHERE source_request_id=source_request
        AND terminal_kind IN ('never_issued','creation_disposed');
    IF t.id IS NULL THEN RETURN public.vm_job_terminal_packet_without_retained_noeffect(source_request); END IF;
    SELECT authority.* INTO d FROM public.vm_job_retained_disk_purge_authorities authority
        JOIN public.vm_workspace_cleanup_admissions c ON c.id=authority.cleanup_admission_id
        WHERE authority.job_id=r.job_id AND authority.pvc_uid=r.expected_pvc_uid
          AND authority.retained_terminal_id IS NOT NULL AND c.completed_at IS NOT NULL AND c.outcome='completed';
    IF d.cleanup_admission_id IS NULL THEN RETURN NULL; END IF;
    tail := public.vm_job_retained_purge_tail(d.cleanup_admission_id);
    IF tail IS NULL OR NOT EXISTS(SELECT 1 FROM jsonb_array_elements(tail) item WHERE item->>'terminal_id'=t.id::text) THEN RETURN NULL; END IF;
    PERFORM public.validate_vm_job_retained_disk_purge(d.cleanup_admission_id,true);
    RETURN jsonb_build_object('kind','retained_noeffect','job_id',r.job_id,'request_id',r.request_id,
        'provision_generation',r.provision_generation,'cleanup_admission_id',d.cleanup_admission_id,
        'terminal_id',t.id,'terminal_evidence',t.evidence,'chain_digest',public.vm_job_retained_disk_purge_chain_digest(d.cleanup_admission_id));
END;
$body$;
COMMIT;
