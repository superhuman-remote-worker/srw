-- migration: 0297_vm_thread_retained_resume.sql
-- description: Ordinary retained Session Resume and its terminal disk lineage.
-- depends-on: 0296_vm_thread_retained_disk_purge.sql
-- transactional: yes
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

CREATE TABLE public.vm_thread_retained_resumes (
    id uuid PRIMARY KEY,
    thread_id uuid NOT NULL REFERENCES public.vm_thread_creation_owners(thread_id),
    runtime_generation uuid NOT NULL,
    predecessor_runtime_generation uuid NOT NULL,
    predecessor_retirement_token uuid NOT NULL,
    compute_cleanup_admission_id uuid NOT NULL REFERENCES public.vm_resource_thread_cleanup_authorities(cleanup_admission_id),
    predecessor_terminal_id uuid,
    source_revision bigint NOT NULL,
    retained_vm jsonb NOT NULL,
    request_id uuid NOT NULL UNIQUE,
    provision_generation uuid NOT NULL UNIQUE,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(thread_id,runtime_generation)
);
ALTER TABLE public.vm_creation_retries
    ADD COLUMN thread_retained_resume_id uuid,
    ADD CONSTRAINT vm_creation_retained_resume_operation_fkey FOREIGN KEY(thread_retained_resume_id)
        REFERENCES public.vm_thread_retained_resumes(id) NOT VALID,
    ADD CONSTRAINT vm_creation_retained_resume_owner CHECK (
        thread_retained_resume_id IS NULL OR
        (owner_kind='thread' AND origin='resume' AND thread_wake_operation_id IS NULL AND expected_pvc_uid IS NOT NULL)) NOT VALID;
-- Existing sources have NULL in the new column. The guard fixes each operation
-- to its unique request_id, so its source needs no second uniqueness index.

CREATE TABLE public.vm_thread_retained_resume_terminals (
    id uuid PRIMARY KEY,
    operation_id uuid NOT NULL REFERENCES public.vm_thread_retained_resumes(id),
    runtime_generation uuid NOT NULL,
    retirement_token uuid NOT NULL,
    compute_cleanup_admission_id uuid NOT NULL REFERENCES public.vm_resource_thread_cleanup_authorities(cleanup_admission_id),
    source_request_id uuid REFERENCES public.vm_creation_retries(request_id),
    source_terminal_evidence jsonb,
    retained_vm jsonb NOT NULL,
    retirement_context jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(operation_id,runtime_generation,retirement_token)
);
ALTER TABLE public.vm_thread_retained_resumes ADD CONSTRAINT vm_thread_retained_resume_terminal_fkey
    FOREIGN KEY(predecessor_terminal_id) REFERENCES public.vm_thread_retained_resume_terminals(id);

-- Validators are entered only after owner/PVC advisory locks and the owner row.
-- Immutable predecessor history is checked independently of the current runtime.

CREATE FUNCTION public.validate_vm_thread_retained_compute(cleanup_id uuid)
RETURNS boolean LANGUAGE plpgsql AS $$
DECLARE
    a public.vm_resource_thread_cleanup_authorities%ROWTYPE;
    s public.vm_resource_thread_cleanup_stops%ROWTYPE;
    source public.vm_creation_retries%ROWTYPE;
    charge public.vm_resource_reservations%ROWTYPE;
    old_cleanup public.vm_workspace_cleanup_admissions%ROWTYPE;
    soft public.thread_runtime_retirement_outcomes%ROWTYPE;
    expected_stop jsonb;
    old_digest text;
BEGIN
    SELECT * INTO a FROM public.vm_resource_thread_cleanup_authorities WHERE cleanup_admission_id=cleanup_id;
    SELECT * INTO source FROM public.vm_creation_retries WHERE request_id=a.request_id FOR SHARE;
    SELECT * INTO old_cleanup FROM public.vm_workspace_cleanup_admissions WHERE id=a.cleanup_admission_id FOR SHARE;
    SELECT * INTO charge FROM public.vm_resource_reservations WHERE id=a.reservation_id FOR SHARE;
    SELECT * INTO s FROM public.vm_resource_thread_cleanup_stops WHERE cleanup_admission_id=a.cleanup_admission_id;
    SELECT * INTO soft FROM public.thread_runtime_retirement_outcomes
        WHERE thread_id=a.thread_id AND runtime_generation=a.runtime_generation AND retirement_token=a.retirement_token;
    expected_stop := jsonb_build_object(
        'version',1,'kind','vm_cleanup_physical_stop','owner_kind','thread','owner_id',a.thread_id,
        'provision_generation',a.provision_generation,'vm_uid',a.vm_uid,'vmi_uid',a.vmi_uid,
        'launcher_uid',a.launcher_uid,'pvc_uid',a.pvc_uid,
        'vm_absent',true,'vmi_absent',true,'launcher_absent',true,
        'same_generation_replacement',false,'controller_authenticated',true,'pvc_disposition','retained');
    -- These keys/values are fixed ASCII UUIDs and literals, sorted exactly as
    -- cleanup_intent_digest's compact JSON. jsonb::text is NOT its encoding.
    old_digest := 'sha256:' || encode(sha256(convert_to(format(
        '{"owner_id":"%s","owner_kind":"thread","provision_generation":"%s","purge_disk":false,"pvc_uid":"%s","resource":"vm_workspace","source":"pinned_thread_retirement","vm_uid":"%s"}',
        a.thread_id,a.provision_generation,a.pvc_uid,a.vm_uid),'UTF8')),'hex');
    IF a.cleanup_admission_id IS NULL OR a.purge_disk IS DISTINCT FROM false
       OR s.cleanup_admission_id IS NULL OR s.stop_evidence IS DISTINCT FROM expected_stop
       OR old_cleanup.id IS NULL OR old_cleanup.owner_kind IS DISTINCT FROM 'thread'
       OR old_cleanup.owner_id IS DISTINCT FROM a.thread_id OR old_cleanup.pvc_uid IS DISTINCT FROM a.pvc_uid
       OR old_cleanup.parent_admission_id IS NOT NULL
       OR old_cleanup.source IS DISTINCT FROM 'pinned_thread_retirement'
       OR old_cleanup.request_id IS DISTINCT FROM a.cleanup_request_id
       OR old_cleanup.intent_digest IS DISTINCT FROM old_digest OR a.intent_digest IS DISTINCT FROM old_digest
       OR old_cleanup.completed_at IS NULL OR old_cleanup.outcome IS DISTINCT FROM 'completed'
       OR soft.thread_id IS NULL OR soft.permanent IS DISTINCT FROM false
       OR soft.outcome IS DISTINCT FROM 'settled' OR soft.disposition IS DISTINCT FROM 'ended'
       OR soft.agent_id IS DISTINCT FROM a.agent_id OR soft.runtime_attach_token IS DISTINCT FROM a.attach_token
       OR old_cleanup.completed_at>soft.settled_at
       OR source.resolved_at IS NULL OR source.resolved_at>old_cleanup.admitted_at
       OR NOT EXISTS (SELECT 1 FROM public.managed_repository_process_zero_receipts p
           WHERE p.id=s.process_zero_receipt_id AND p.owner_kind='thread' AND p.owner_id=a.thread_id
             AND p.scope='vm' AND p.provisioner='vm' AND p.runtime_incarnation=a.provision_generation::text
             AND p.observed_at<=soft.settled_at)
       OR charge.id IS NULL OR charge.state IS DISTINCT FROM 'released'
       OR charge.resource_version IS DISTINCT FROM 2 OR charge.request_id IS DISTINCT FROM a.request_id
       OR charge.revision IS DISTINCT FROM a.reservation_revision
       OR charge.released_at IS NULL OR charge.released_at<s.accepted_at
       OR charge.release_evidence IS DISTINCT FROM jsonb_build_object(
           'kind','exact_cleanup_compute_absent','owner_kind','thread','thread_id',a.thread_id,
           'cleanup_admission_id',a.cleanup_admission_id,'reservation_revision',a.reservation_revision,
           'stop_evidence_digest','sha256:'||encode(sha256(convert_to(s.stop_evidence::text,'UTF8')),'hex'))
       OR charge.vmi_uid IS DISTINCT FROM a.vmi_uid OR charge.launcher_uid IS DISTINCT FROM a.launcher_uid
       OR (charge.vm_uid IS NOT NULL AND charge.vm_uid IS DISTINCT FROM a.vm_uid)
       OR (a.vmi_uid IS NULL AND (source.ready_at IS NOT NULL OR charge.vm_uid IS NOT NULL OR a.launcher_uid IS NOT NULL))
       OR (a.vmi_uid IS NOT NULL AND (source.ready_at IS NULL OR charge.vm_uid IS DISTINCT FROM a.vm_uid OR a.launcher_uid IS NULL))
       OR EXISTS (SELECT 1 FROM public.vm_resource_reservations r WHERE r.request_id=a.request_id AND r.revision>a.reservation_revision)
       OR EXISTS (SELECT 1 FROM public.vm_resource_recovery_successors r WHERE r.reservation_id=a.reservation_id
           AND (r.successor_vmi_uid IS DISTINCT FROM a.vmi_uid OR r.successor_launcher_uid IS DISTINCT FROM a.launcher_uid))
       OR NOT EXISTS (SELECT 1 FROM public.vm_resource_waiters w WHERE w.request_id=a.request_id
           AND w.owner_kind='thread' AND w.thread_id=a.thread_id AND w.provision_generation=a.provision_generation AND w.state='released') THEN
        RAISE EXCEPTION 'VM retained disk predecessor unproven' USING ERRCODE='23514';
    END IF;
    IF source.request_id IS NULL OR source.owner_kind IS DISTINCT FROM 'thread'
       OR source.thread_id IS DISTINCT FROM a.thread_id OR (source.state IS DISTINCT FROM 'succeeded' AND NOT COALESCE(public.valid_vm_thread_retained_handoff(source),false))
       OR source.thread_wake_operation_id IS NOT NULL
       OR NOT (source.origin='initial' AND source.expected_pvc_uid IS NULL
           OR source.origin='resume' AND EXISTS (
               SELECT 1 FROM public.vm_thread_retained_resumes op
               JOIN public.vm_resource_thread_cleanup_authorities prior ON prior.cleanup_admission_id=op.compute_cleanup_admission_id
               WHERE op.id=source.thread_retained_resume_id AND op.thread_id=a.thread_id
                 AND op.runtime_generation=source.thread_runtime_generation
                 AND op.request_id=source.request_id AND op.provision_generation=source.provision_generation
                 AND prior.pvc_uid=source.expected_pvc_uid AND prior.pvc_uid=a.pvc_uid))
       OR source.thread_runtime_generation IS DISTINCT FROM a.runtime_generation
       OR source.thread_agent_id IS DISTINCT FROM a.agent_id OR source.thread_attach_token IS DISTINCT FROM a.attach_token
       OR source.provision_generation IS DISTINCT FROM a.provision_generation
       OR source.observed_vm_uid IS DISTINCT FROM a.vm_uid OR source.observed_pvc_uid IS DISTINCT FROM a.pvc_uid
       OR source.controller_configuration->>'version' IS DISTINCT FROM '3' THEN
        RAISE EXCEPTION 'VM retained compute source unproven' USING ERRCODE='23514';
    END IF;
    RETURN true;
END;
$$;

CREATE FUNCTION public.guard_vm_thread_retained_resume() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE owner_row public.threads%ROWTYPE;
        a public.vm_resource_thread_cleanup_authorities%ROWTYPE;
        source public.vm_creation_retries%ROWTYPE;
        soft public.thread_runtime_retirement_outcomes%ROWTYPE;
        terminal public.vm_thread_retained_resume_terminals%ROWTYPE;
BEGIN
    IF TG_OP<>'INSERT' THEN
        RAISE EXCEPTION 'VM retained Resume operation is append-only' USING ERRCODE='23514';
    END IF;
    SELECT * INTO owner_row FROM public.threads WHERE id=NEW.thread_id FOR SHARE;
    SELECT * INTO a FROM public.vm_resource_thread_cleanup_authorities WHERE cleanup_admission_id=NEW.compute_cleanup_admission_id;
    SELECT * INTO source FROM public.vm_creation_retries WHERE request_id=a.request_id FOR SHARE;
    PERFORM public.validate_vm_thread_retained_compute(a.cleanup_admission_id);
    SELECT * INTO soft FROM public.thread_runtime_retirement_outcomes
        WHERE thread_id=NEW.thread_id AND runtime_generation=NEW.predecessor_runtime_generation
          AND retirement_token=NEW.predecessor_retirement_token;
    IF NEW.predecessor_terminal_id IS NOT NULL THEN
        SELECT * INTO terminal FROM public.vm_thread_retained_resume_terminals WHERE id=NEW.predecessor_terminal_id;
        IF terminal.id IS NULL OR terminal.runtime_generation IS DISTINCT FROM NEW.predecessor_runtime_generation
           OR terminal.retirement_token IS DISTINCT FROM NEW.predecessor_retirement_token
           OR terminal.compute_cleanup_admission_id IS DISTINCT FROM a.cleanup_admission_id
           OR NOT EXISTS (SELECT 1 FROM public.vm_thread_retained_resumes previous
               WHERE previous.id=terminal.operation_id AND previous.thread_id=NEW.thread_id) THEN
            RAISE EXCEPTION 'VM retained Resume terminal predecessor changed' USING ERRCODE='23514';
        END IF;
    ELSIF NEW.predecessor_runtime_generation IS DISTINCT FROM a.runtime_generation
       OR NEW.predecessor_retirement_token IS DISTINCT FROM a.retirement_token THEN
        RAISE EXCEPTION 'VM retained Resume predecessor changed' USING ERRCODE='23514';
    END IF;
    IF owner_row.id IS NULL OR owner_row.execution_lane IS DISTINCT FROM 'pinned'
       OR owner_row.status IS DISTINCT FROM 'created' OR owner_row.runtime_retirement_token IS NOT NULL
       OR owner_row.runtime_generation IS DISTINCT FROM NEW.runtime_generation
       OR owner_row.runtime_generation=NEW.predecessor_runtime_generation
       OR owner_row.agent_id IS NOT NULL OR owner_row.runtime_attach_token IS NOT NULL
       OR soft.thread_id IS NULL OR soft.permanent OR soft.outcome IS DISTINCT FROM 'settled'
       OR soft.disposition IS DISTINCT FROM 'ended' OR soft.settled_at>NEW.created_at
       OR a.thread_id IS DISTINCT FROM NEW.thread_id OR source.revision IS DISTINCT FROM NEW.source_revision
       OR NEW.retained_vm IS DISTINCT FROM owner_row.metadata->'vm'
       OR NEW.retained_vm->>'creation_request_id' IS DISTINCT FROM a.request_id::text
       OR NEW.retained_vm->>'provision_generation' IS DISTINCT FROM a.provision_generation::text
       OR NEW.retained_vm->>'identity_provision_generation' IS DISTINCT FROM a.provision_generation::text
       OR NEW.retained_vm->'identity_authenticated' IS DISTINCT FROM 'true'::jsonb
       OR NEW.retained_vm->>'vm_uid' IS DISTINCT FROM a.vm_uid::text
       OR NEW.retained_vm->>'rootdisk_pvc_uid' IS DISTINCT FROM a.pvc_uid::text
       OR NEW.retained_vm->>'status' IS DISTINCT FROM 'deleted'
       OR EXISTS (SELECT 1 FROM public.vm_workspace_cleanup_admissions c
           WHERE ((c.owner_kind='thread' AND c.owner_id=NEW.thread_id) OR c.pvc_uid=a.pvc_uid) AND c.completed_at IS NULL)
       OR EXISTS (SELECT 1 FROM public.vm_workspace_recoveries r
           WHERE r.owner_kind='thread' AND r.owner_id=NEW.thread_id AND r.resolved_at IS NULL)
       OR EXISTS (SELECT 1 FROM public.vm_workspace_recovery_retention_pins pin WHERE pin.pvc_uid=a.pvc_uid AND pin.released_at IS NULL)
       OR EXISTS (SELECT 1 FROM public.vm_idle_access_leases l WHERE l.owner_kind='thread' AND l.owner_id=NEW.thread_id
           AND l.closed_at IS NULL AND l.expires_at>clock_timestamp())
       OR EXISTS (SELECT 1 FROM public.vm_idle_operations idle WHERE idle.owner_kind='thread' AND idle.owner_id=NEW.thread_id AND idle.closed_at IS NULL)
       OR EXISTS (SELECT 1 FROM public.vm_creation_retries r WHERE r.request_id=NEW.request_id OR r.provision_generation=NEW.provision_generation) THEN
        RAISE EXCEPTION 'VM retained Resume authority unproven' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER guard_vm_thread_retained_resume
BEFORE INSERT OR UPDATE OR DELETE ON public.vm_thread_retained_resumes
FOR EACH ROW EXECUTE FUNCTION public.guard_vm_thread_retained_resume();

CREATE FUNCTION public.valid_vm_thread_retained_resume_source(source public.vm_creation_retries)
RETURNS boolean LANGUAGE sql AS $$
    SELECT source.owner_kind='thread' AND source.origin='resume'
       AND source.thread_wake_operation_id IS NULL AND source.thread_agent_id IS NOT NULL AND source.thread_attach_token IS NOT NULL
       AND EXISTS (SELECT 1 FROM public.vm_thread_retained_resumes op
           JOIN public.vm_resource_thread_cleanup_authorities a ON a.cleanup_admission_id=op.compute_cleanup_admission_id
           WHERE op.id=source.thread_retained_resume_id AND op.thread_id=source.thread_id
             AND op.runtime_generation=source.thread_runtime_generation
             AND op.request_id=source.request_id AND op.provision_generation=source.provision_generation
             AND a.pvc_uid=source.expected_pvc_uid);
$$;

CREATE OR REPLACE FUNCTION public.guard_vm_creation_thread_source() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    current_thread public.threads%ROWTYPE;
    current_vm jsonb;
    current_wake public.vm_idle_operations%ROWTYPE;
BEGIN
    IF TG_OP='UPDATE' THEN
        IF ROW(NEW.owner_kind,NEW.thread_id,NEW.thread_runtime_generation,
               NEW.thread_agent_id,NEW.thread_attach_token,
               NEW.thread_wake_operation_id,NEW.thread_retained_resume_id,NEW.thread_owner_user_id,
               NEW.thread_owner_project_id)
           IS DISTINCT FROM
           ROW(OLD.owner_kind,OLD.thread_id,OLD.thread_runtime_generation,
               OLD.thread_agent_id,OLD.thread_attach_token,
               OLD.thread_wake_operation_id,OLD.thread_retained_resume_id,OLD.thread_owner_user_id,
               OLD.thread_owner_project_id) THEN
            RAISE EXCEPTION 'VM creation source identity is immutable' USING ERRCODE='23514';
        END IF;
        RETURN NEW;
    END IF;
    IF NEW.owner_kind='job' THEN
        RETURN NEW;
    END IF;
    IF jsonb_typeof(NEW.canonical_request) IS DISTINCT FROM 'object'
       OR NEW.canonical_request->>'entity_type' IS DISTINCT FROM 'thread'
       OR NEW.canonical_request->>'job_id' IS DISTINCT FROM NEW.thread_id::text
       OR NEW.canonical_request->>'provision_generation' IS DISTINCT FROM NEW.provision_generation::text
       OR jsonb_typeof(NEW.controller_configuration) IS DISTINCT FROM 'object'
       OR NEW.controller_configuration->'version' IS DISTINCT FROM '3'::jsonb THEN
        RAISE EXCEPTION 'VM thread creation request identity mismatch' USING ERRCODE='23514';
    END IF;
    SELECT * INTO current_thread FROM public.threads
      WHERE id=NEW.thread_id FOR SHARE;
    current_vm := current_thread.metadata->'vm';
    IF NOT FOUND OR current_thread.execution_lane IS DISTINCT FROM 'pinned'
       OR current_thread.runtime_generation IS DISTINCT FROM NEW.thread_runtime_generation
       OR current_thread.runtime_retirement_token IS NOT NULL
       OR current_thread.pinned_idle_terminal_intent_at IS NOT NULL
       OR current_thread.agent_id IS DISTINCT FROM NEW.thread_agent_id
       OR current_thread.runtime_attach_token IS DISTINCT FROM NEW.thread_attach_token
       OR jsonb_typeof(current_vm) IS DISTINCT FROM 'object'
       OR current_vm->>'provision_generation' IS DISTINCT FROM NEW.provision_generation::text
       OR current_vm->>'creation_request_id' IS DISTINCT FROM NEW.request_id::text
       OR current_vm->>'status' IS DISTINCT FROM 'provisioning' THEN
        RAISE EXCEPTION 'VM thread creation owner changed' USING ERRCODE='23514';
    END IF;
    IF NEW.thread_agent_id IS NULL THEN
        IF EXISTS(SELECT 1 FROM public.agents WHERE thread_id=NEW.thread_id) THEN
            RAISE EXCEPTION 'VM thread creation agent mismatch' USING ERRCODE='23514';
        END IF;
    ELSE
        -- The actor UUID is immutable historical provenance. Replace the old
        -- FK's live-row key lock with exact reciprocal tuple protection under
        -- the already-held thread lock, before accepting a new source.
        PERFORM 1 FROM public.agents
            WHERE id=NEW.thread_agent_id AND thread_id=NEW.thread_id FOR SHARE;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'VM thread creation agent mismatch' USING ERRCODE='23514';
        END IF;
    END IF;
    IF NEW.thread_retained_resume_id IS NOT NULL THEN
        IF NOT public.valid_vm_thread_retained_resume_source(NEW) THEN
            RAISE EXCEPTION 'VM retained Resume source changed' USING ERRCODE='23514';
        END IF;
    ELSIF NEW.thread_wake_operation_id IS NOT NULL THEN
        SELECT * INTO current_wake FROM public.vm_idle_operations
          WHERE id=NEW.thread_wake_operation_id FOR SHARE;
        IF NOT FOUND OR current_wake.owner_kind IS DISTINCT FROM 'thread'
           OR current_wake.owner_id IS DISTINCT FROM NEW.thread_id
           OR current_wake.release_kind IS DISTINCT FROM 'pinned_thread'
           OR current_wake.phase IS NULL
           OR current_wake.phase NOT IN ('waking','wake_held')
           OR current_wake.closed_at IS NOT NULL
           OR current_wake.stop_verified_at IS NULL
           OR current_wake.thread_terminal_intent_at IS NOT NULL
           OR current_wake.wake_request_id IS DISTINCT FROM NEW.request_id
           OR current_wake.wake_generation IS DISTINCT FROM NEW.provision_generation
           OR current_wake.pvc_uid IS DISTINCT FROM NEW.expected_pvc_uid THEN
            RAISE EXCEPTION 'VM thread wake source changed' USING ERRCODE='23514';
        END IF;
    ELSIF NEW.expected_pvc_uid IS NOT NULL OR EXISTS (
        SELECT 1 FROM public.vm_resource_thread_cleanup_authorities a
        JOIN public.vm_resource_thread_cleanup_stops s USING(cleanup_admission_id)
        WHERE a.thread_id=NEW.thread_id AND NOT a.purge_disk
          AND a.runtime_generation<>NEW.thread_runtime_generation) THEN
        RAISE EXCEPTION 'VM thread retained disk requires Resume or wake source' USING ERRCODE='23514';
    END IF;
    NEW.thread_owner_user_id := current_thread.user_id;
    NEW.thread_owner_project_id := current_thread.project_id;
    RETURN NEW;
END;
$$;


CREATE FUNCTION public.vm_thread_retained_source_snapshot(retry public.vm_creation_retries)
RETURNS jsonb LANGUAGE sql STABLE AS $$
    SELECT jsonb_build_object('version',1,'kind','retained_vm_creation_terminal',
        'source',to_jsonb(retry)-ARRAY['canonical_request','controller_configuration','revision',
            'claim_token','claim_expires_at','next_probe_at','backoff_attempt','transport_outage_started_at','updated_at'],
        'effects',COALESCE((SELECT jsonb_agg(to_jsonb(e) ORDER BY e.effect_nonce)
            FROM public.vm_creation_effects e WHERE e.request_id=retry.request_id),'[]'::jsonb),
        'reservations',COALESCE((SELECT jsonb_agg(to_jsonb(r) ORDER BY r.id)
            FROM public.vm_resource_reservations r WHERE r.request_id=retry.request_id),'[]'::jsonb),
        'waiter',(SELECT to_jsonb(w) FROM public.vm_resource_waiters w WHERE w.request_id=retry.request_id));
$$;

CREATE FUNCTION public.vm_thread_retained_source_terminal_evidence(retry public.vm_creation_retries)
RETURNS jsonb LANGUAGE plpgsql STABLE AS $$
BEGIN
    IF NOT public.valid_vm_thread_retained_resume_source(retry)
       OR retry.state IS DISTINCT FROM 'settled' OR retry.resolved_at IS NULL
       OR retry.reason IS NULL OR retry.reason NOT IN ('creation_never_issued','creation_disposed','retained_creation_handoff')
       OR EXISTS (SELECT 1 FROM public.vm_creation_effects e WHERE e.request_id=retry.request_id AND e.state='issued')
       OR EXISTS (SELECT 1 FROM public.vm_resource_reservations r WHERE r.request_id=retry.request_id AND r.state<>'released')
       OR NOT EXISTS (SELECT 1 FROM public.vm_resource_waiters w WHERE w.request_id=retry.request_id AND w.state IN ('released','cancelled'))
       OR (retry.creation_admission_id IS NOT NULL AND NOT EXISTS (
           SELECT 1 FROM public.vm_workspace_cleanup_admissions a WHERE a.id=retry.creation_admission_id
             AND a.completed_at IS NOT NULL AND a.outcome=CASE retry.reason WHEN 'creation_never_issued' THEN 'never_issued' WHEN 'creation_disposed' THEN 'creation_disposed' ELSE 'adopted' END)) THEN
        RETURN NULL;
    END IF;
    IF retry.reason='retained_creation_handoff' THEN
        IF NOT EXISTS (SELECT 1 FROM public.vm_resource_thread_cleanup_authorities a
            JOIN public.vm_resource_thread_cleanup_stops s USING(cleanup_admission_id)
            JOIN public.vm_workspace_cleanup_admissions c ON c.id=a.cleanup_admission_id
            WHERE a.request_id=retry.request_id AND c.completed_at IS NOT NULL AND c.outcome='completed') THEN
            RETURN NULL;
        END IF;
    ELSIF retry.observed_vm_uid IS NOT NULL OR retry.boot_counted OR EXISTS (
        SELECT 1 FROM public.vm_creation_effects e WHERE e.request_id=retry.request_id AND e.effect_kind='vm' AND e.state<>'rejected') THEN
        RETURN NULL;
    ELSIF retry.reason='creation_never_issued' AND (
        retry.observed_pvc_uid IS NOT NULL OR retry.cancellation_disposition IS NOT NULL
        OR EXISTS (SELECT 1 FROM public.vm_creation_effects e WHERE e.request_id=retry.request_id AND e.state<>'rejected')) THEN
        RETURN NULL;
    ELSIF retry.reason='creation_disposed' AND (
        retry.cancellation_disposition->>'disk_policy' IS DISTINCT FROM 'retain'
        OR NOT public.valid_vm_creation_disposition_evidence(retry)) THEN
        RETURN NULL;
    END IF;
    RETURN public.vm_thread_retained_source_snapshot(retry);
END;
$$;

CREATE FUNCTION public.valid_vm_thread_retained_runtime(op public.vm_thread_retained_resumes, owner_row public.threads)
RETURNS boolean LANGUAGE plpgsql STABLE AS $$
DECLARE cursor_generation uuid := op.runtime_generation;
        visited uuid[] := ARRAY[]::uuid[];
        edge public.thread_runtime_attach_abort_outcomes%ROWTYPE;
        source public.vm_creation_retries%ROWTYPE;
        edge_count integer;
        hops integer := 0;
BEGIN
    IF op.id IS NULL OR op.thread_id IS DISTINCT FROM owner_row.id OR owner_row.execution_lane IS DISTINCT FROM 'pinned' THEN
        RETURN false;
    END IF;
    IF cursor_generation=owner_row.runtime_generation THEN RETURN true; END IF;
    IF owner_row.status IS DISTINCT FROM 'created' THEN RETURN false; END IF;
    SELECT * INTO source FROM public.vm_creation_retries WHERE request_id=op.request_id;
    LOOP
        IF hops>=4096 OR cursor_generation=ANY(visited) THEN RETURN false; END IF;
        visited := array_append(visited,cursor_generation);
        SELECT count(*) INTO edge_count FROM (SELECT 1 FROM public.thread_runtime_attach_abort_outcomes o
            WHERE o.thread_id=op.thread_id AND o.runtime_generation=cursor_generation LIMIT 2) candidates;
        IF edge_count<>1 THEN RETURN false; END IF;
        SELECT * INTO edge FROM public.thread_runtime_attach_abort_outcomes
            WHERE thread_id=op.thread_id AND runtime_generation=cursor_generation;
        IF edge.release_kind IS DISTINCT FROM 'process_zero'
           OR edge.quiescence_protocol IS DISTINCT FROM 'agent_attach_not_started_v1'
           OR edge.workspace_generation IS NOT NULL OR edge.workspace_runtime_incarnation IS NOT NULL
           OR edge.agent_id IS NULL OR edge.runtime_attach_token IS NULL
           OR NULLIF(btrim(edge.agent_pod_uid),'') IS NULL
           OR (hops=0 AND source.request_id IS NOT NULL AND (
               edge.agent_id IS DISTINCT FROM source.thread_agent_id OR edge.runtime_attach_token IS DISTINCT FROM source.thread_attach_token)) THEN
            RETURN false;
        END IF;
        cursor_generation := edge.successor_generation;
        hops := hops+1;
        IF cursor_generation=owner_row.runtime_generation THEN RETURN true; END IF;
    END LOOP;
END;
$$;
ALTER FUNCTION public.thread_vm_creation_cleanup_lineage(public.threads,public.vm_creation_retries,boolean)
RENAME TO thread_vm_initial_creation_cleanup_lineage;
CREATE FUNCTION public.thread_vm_creation_cleanup_lineage(owner_row public.threads, retry public.vm_creation_retries, require_initial boolean DEFAULT false)
RETURNS text LANGUAGE plpgsql STABLE AS $$
DECLARE prior text;
BEGIN
    prior := public.thread_vm_initial_creation_cleanup_lineage(owner_row,retry,require_initial);
    IF prior IS NOT NULL THEN RETURN prior; END IF;
    IF NOT require_initial AND public.valid_vm_thread_retained_resume_source(retry)
       AND retry.observed_vm_uid IS NULL AND NOT retry.boot_counted
       AND EXISTS (SELECT 1 FROM public.vm_thread_retained_resumes op
           WHERE op.id=retry.thread_retained_resume_id AND op.runtime_generation<>owner_row.runtime_generation
             AND public.valid_vm_thread_retained_runtime(op,owner_row)) THEN
        RETURN 'retained_attach_abort_v1';
    END IF;
    RETURN NULL;
END;
$$;

CREATE FUNCTION public.guard_vm_thread_retained_resume_terminal() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE op public.vm_thread_retained_resumes%ROWTYPE;
        owner_row public.threads%ROWTYPE;
        source public.vm_creation_retries%ROWTYPE;
        a public.vm_resource_thread_cleanup_authorities%ROWTYPE;
        evidence jsonb;
BEGIN
    IF TG_OP<>'INSERT' THEN
        RAISE EXCEPTION 'VM retained terminal is append-only' USING ERRCODE='23514';
    END IF;
    SELECT * INTO op FROM public.vm_thread_retained_resumes WHERE id=NEW.operation_id;
    SELECT * INTO owner_row FROM public.threads WHERE id=op.thread_id FOR SHARE;
    SELECT * INTO source FROM public.vm_creation_retries WHERE request_id=op.request_id FOR SHARE;
    SELECT * INTO a FROM public.vm_resource_thread_cleanup_authorities WHERE cleanup_admission_id=NEW.compute_cleanup_admission_id;
    IF op.id IS NULL OR owner_row.id IS NULL OR owner_row.runtime_generation IS DISTINCT FROM NEW.runtime_generation
       OR NOT public.valid_vm_thread_retained_runtime(op,owner_row)
       OR owner_row.runtime_retirement_token IS DISTINCT FROM NEW.retirement_token
       OR owner_row.runtime_retirement_authorized_at IS NULL
       OR owner_row.runtime_retirement_context IS DISTINCT FROM NEW.retirement_context
       OR a.thread_id IS DISTINCT FROM op.thread_id
       OR NOT EXISTS (SELECT 1 FROM public.thread_runtime_retirement_outcomes o
           WHERE o.thread_id=op.thread_id AND o.runtime_generation=NEW.runtime_generation
             AND o.retirement_token=NEW.retirement_token AND o.disposition='ended'
             AND o.outcome=CASE WHEN o.permanent THEN 'deleted' ELSE 'settled' END
             AND o.agent_id IS NOT DISTINCT FROM owner_row.agent_id
             AND o.runtime_attach_token IS NOT DISTINCT FROM owner_row.runtime_attach_token) THEN
        RAISE EXCEPTION 'VM retained terminal retirement unproven' USING ERRCODE='23514';
    END IF;
    IF NEW.retained_vm->>'creation_request_id' IS DISTINCT FROM a.request_id::text
       OR NEW.retained_vm->>'provision_generation' IS DISTINCT FROM a.provision_generation::text
       OR NEW.retained_vm->>'identity_provision_generation' IS DISTINCT FROM a.provision_generation::text
       OR NEW.retained_vm->'identity_authenticated' IS DISTINCT FROM 'true'::jsonb
       OR NEW.retained_vm->>'vm_uid' IS DISTINCT FROM a.vm_uid::text
       OR NEW.retained_vm->>'rootdisk_pvc_uid' IS DISTINCT FROM a.pvc_uid::text
       OR NEW.retained_vm->>'status' IS DISTINCT FROM 'deleted'
       OR (source.observed_vm_uid IS NULL AND NEW.retained_vm IS DISTINCT FROM op.retained_vm) THEN
        RAISE EXCEPTION 'VM retained terminal backing unproven' USING ERRCODE='23514';
    END IF;
    IF source.request_id IS NULL THEN
        IF NEW.source_request_id IS NOT NULL OR NEW.source_terminal_evidence IS NOT NULL
           OR NEW.compute_cleanup_admission_id IS DISTINCT FROM op.compute_cleanup_admission_id THEN
            RAISE EXCEPTION 'VM retained terminal absent source changed' USING ERRCODE='23514';
        END IF;
    ELSE
        evidence := public.vm_thread_retained_source_terminal_evidence(source);
        IF NEW.source_request_id IS DISTINCT FROM source.request_id OR evidence IS NULL
           OR NEW.source_terminal_evidence IS DISTINCT FROM evidence
           OR (source.observed_vm_uid IS NULL AND NEW.compute_cleanup_admission_id IS DISTINCT FROM op.compute_cleanup_admission_id)
           OR (source.observed_vm_uid IS NOT NULL AND a.request_id IS DISTINCT FROM source.request_id) THEN
            RAISE EXCEPTION 'VM retained terminal source unproven' USING ERRCODE='23514';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER guard_vm_thread_retained_resume_terminal
BEFORE INSERT OR UPDATE OR DELETE ON public.vm_thread_retained_resume_terminals
FOR EACH ROW EXECUTE FUNCTION public.guard_vm_thread_retained_resume_terminal();

CREATE FUNCTION public.capture_vm_thread_retained_resume_terminal() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE op public.vm_thread_retained_resumes%ROWTYPE;
        source public.vm_creation_retries%ROWTYPE;
        cleanup_id uuid;
        context jsonb;
        retained_vm jsonb;
BEGIN
    IF NEW.disposition<>'ended' THEN RETURN NEW; END IF;
    SELECT candidate.* INTO op FROM public.vm_thread_retained_resumes candidate JOIN public.threads t ON t.id=candidate.thread_id
        WHERE candidate.thread_id=NEW.thread_id AND public.valid_vm_thread_retained_runtime(candidate,t);
    IF op.id IS NULL THEN RETURN NEW; END IF;
    SELECT * INTO source FROM public.vm_creation_retries WHERE request_id=op.request_id;
    IF source.state='succeeded' THEN RETURN NEW; END IF;
    SELECT runtime_retirement_context INTO context FROM public.threads WHERE id=NEW.thread_id;
    cleanup_id := op.compute_cleanup_admission_id;
    retained_vm := op.retained_vm;
    IF source.observed_vm_uid IS NOT NULL THEN
        SELECT cleanup_admission_id INTO cleanup_id FROM public.vm_resource_thread_cleanup_authorities
            WHERE request_id=source.request_id AND runtime_generation=NEW.runtime_generation AND retirement_token=NEW.retirement_token;
        retained_vm := jsonb_build_object('status','deleted','creation_request_id',source.request_id,
            'provision_generation',source.provision_generation,'identity_provision_generation',source.provision_generation,
            'identity_authenticated',true,'vm_uid',source.observed_vm_uid,'rootdisk_pvc_uid',source.observed_pvc_uid);
    END IF;
    INSERT INTO public.vm_thread_retained_resume_terminals
        (id,operation_id,runtime_generation,retirement_token,compute_cleanup_admission_id,
         source_request_id,source_terminal_evidence,retained_vm,retirement_context)
        VALUES(public.uuid_generate_v5(op.id,'terminal:'||NEW.retirement_token),op.id,
            NEW.runtime_generation,NEW.retirement_token,cleanup_id,source.request_id,
            CASE WHEN source.request_id IS NOT NULL THEN public.vm_thread_retained_source_terminal_evidence(source) END,retained_vm,context);
    RETURN NEW;
END;
$$;
CREATE TRIGGER thread_runtime_retirement_capture_retained_resume
AFTER INSERT ON public.thread_runtime_retirement_outcomes
FOR EACH ROW EXECUTE FUNCTION public.capture_vm_thread_retained_resume_terminal();
-- Restore the backing in the existing final owner transition. A separate owner
-- UPDATE would leave a deferred reciprocal-agent event with the old binding.
CREATE FUNCTION public.project_vm_thread_retained_terminal() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE backing jsonb;
BEGIN
    IF OLD.runtime_retirement_token IS NOT NULL AND NOT OLD.runtime_retirement_permanent
       AND NEW.runtime_retirement_token IS NULL AND NEW.status='ended'
       AND NEW.runtime_generation=OLD.runtime_generation THEN
        SELECT terminal.retained_vm INTO backing FROM public.vm_thread_retained_resume_terminals terminal
            JOIN public.vm_thread_retained_resumes op ON op.id=terminal.operation_id
            WHERE op.thread_id=OLD.id AND terminal.runtime_generation=OLD.runtime_generation
              AND terminal.retirement_token=OLD.runtime_retirement_token;
        IF backing IS NOT NULL THEN
            NEW.metadata := jsonb_set(COALESCE(NEW.metadata,'{}'::jsonb),'{vm}',backing);
        END IF;
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER a_vm_thread_retained_terminal_projection
BEFORE UPDATE ON public.threads FOR EACH ROW EXECUTE FUNCTION public.project_vm_thread_retained_terminal();

-- A current cancelled Resume may purge its old disk only after positively
-- discharging its own source. The old released charge grants no new effect.
CREATE FUNCTION public.valid_vm_thread_retained_early_end(owner_row public.threads, op public.vm_thread_retained_resumes)
RETURNS boolean LANGUAGE sql STABLE AS $$
    SELECT (op.id IS NOT NULL AND owner_row.id=op.thread_id
        AND owner_row.execution_lane='pinned' AND public.valid_vm_thread_retained_runtime(op,owner_row)
        AND owner_row.runtime_retirement_token IS NOT NULL
        AND owner_row.runtime_retirement_authorized_at IS NOT NULL
        AND owner_row.runtime_retirement_permanent
        AND owner_row.runtime_retirement_context->>'generation'=owner_row.runtime_generation::text
        AND owner_row.runtime_retirement_context->>'thread_id'=op.thread_id::text
        AND owner_row.runtime_retirement_context->>'settle_status'='ended'
        AND owner_row.runtime_retirement_context->>'agent_id' IS NOT DISTINCT FROM owner_row.agent_id::text
        AND owner_row.runtime_retirement_context->>'runtime_attach_token' IS NOT DISTINCT FROM owner_row.runtime_attach_token::text
        AND ((NOT EXISTS (SELECT 1 FROM public.vm_creation_retries r WHERE r.request_id=op.request_id)
              AND NOT EXISTS (SELECT 1 FROM public.vm_resource_waiters w WHERE w.request_id=op.request_id)
              AND NOT EXISTS (SELECT 1 FROM public.vm_resource_reservations r WHERE r.request_id=op.request_id)
              AND NOT EXISTS (SELECT 1 FROM public.vm_creation_effects e WHERE e.request_id=op.request_id)
              AND owner_row.runtime_retirement_context->'vm'=op.retained_vm)
             OR EXISTS (SELECT 1 FROM public.vm_creation_retries r
                 WHERE r.request_id=op.request_id AND public.valid_vm_thread_retained_resume_source(r)
                   AND r.observed_vm_uid IS NULL AND NOT r.boot_counted
                   AND public.valid_thread_vm_creation_retirement_source(r,false)
                   AND public.vm_thread_retained_source_terminal_evidence(r) IS NOT NULL))) IS TRUE;
$$;
CREATE OR REPLACE FUNCTION public.validate_vm_thread_retained_disk_purge(
    d public.vm_thread_retained_disk_purge_authorities, after_endpoint_cleanup boolean DEFAULT false)
RETURNS boolean LANGUAGE plpgsql AS $$
DECLARE
    a public.vm_resource_thread_cleanup_authorities%ROWTYPE;
    s public.vm_resource_thread_cleanup_stops%ROWTYPE;
    owner_row public.threads%ROWTYPE;
    source public.vm_creation_retries%ROWTYPE;
    charge public.vm_resource_reservations%ROWTYPE;
    old_cleanup public.vm_workspace_cleanup_admissions%ROWTYPE;
    cleanup public.vm_workspace_cleanup_admissions%ROWTYPE;
    soft public.thread_runtime_retirement_outcomes%ROWTYPE;
    vm jsonb;
    captured jsonb;
    terminal public.vm_thread_retained_resume_terminals%ROWTYPE;
    early_op public.vm_thread_retained_resumes%ROWTYPE;
    early_end boolean := false;
    expected_stop jsonb;
    old_digest text;
    new_digest text;
BEGIN
    SELECT * INTO a FROM public.vm_resource_thread_cleanup_authorities
        WHERE cleanup_admission_id=d.compute_cleanup_admission_id;
    SELECT * INTO owner_row FROM public.threads WHERE id=a.thread_id FOR UPDATE;
    SELECT * INTO source FROM public.vm_creation_retries WHERE request_id=a.request_id FOR UPDATE;
    PERFORM id FROM public.vm_workspace_cleanup_admissions
        WHERE id IN (a.cleanup_admission_id,d.cleanup_admission_id) ORDER BY id FOR UPDATE;
    SELECT * INTO old_cleanup FROM public.vm_workspace_cleanup_admissions WHERE id=a.cleanup_admission_id;
    SELECT * INTO cleanup FROM public.vm_workspace_cleanup_admissions WHERE id=d.cleanup_admission_id;
    SELECT * INTO charge FROM public.vm_resource_reservations WHERE id=a.reservation_id FOR UPDATE;
    SELECT * INTO s FROM public.vm_resource_thread_cleanup_stops WHERE cleanup_admission_id=a.cleanup_admission_id;
    SELECT * INTO soft FROM public.thread_runtime_retirement_outcomes
        WHERE thread_id=a.thread_id AND runtime_generation=a.runtime_generation AND retirement_token=a.retirement_token;
    expected_stop := jsonb_build_object(
        'version',1,'kind','vm_cleanup_physical_stop','owner_kind','thread','owner_id',a.thread_id,
        'provision_generation',a.provision_generation,'vm_uid',a.vm_uid,'vmi_uid',a.vmi_uid,
        'launcher_uid',a.launcher_uid,'pvc_uid',a.pvc_uid,
        'vm_absent',true,'vmi_absent',true,'launcher_absent',true,
        'same_generation_replacement',false,'controller_authenticated',true,'pvc_disposition','retained');
    -- These keys/values are fixed ASCII UUIDs and literals, sorted exactly as
    -- cleanup_intent_digest's compact JSON. jsonb::text is NOT its encoding.
    old_digest := 'sha256:' || encode(sha256(convert_to(format(
        '{"owner_id":"%s","owner_kind":"thread","provision_generation":"%s","purge_disk":false,"pvc_uid":"%s","resource":"vm_workspace","source":"pinned_thread_retirement","vm_uid":"%s"}',
        a.thread_id,a.provision_generation,a.pvc_uid,a.vm_uid),'UTF8')),'hex');
    new_digest := 'sha256:' || encode(sha256(convert_to(format(
        '{"compute_cleanup_admission_id":"%s","owner_id":"%s","owner_kind":"thread","provision_generation":"%s","purge_disk":true,"pvc_uid":"%s","resource":"vm_workspace","retirement_token":"%s","runtime_generation":"%s","source":"pinned_thread_retained_disk_purge","vm_uid":"%s"}',
        a.cleanup_admission_id,a.thread_id,a.provision_generation,a.pvc_uid,d.retirement_token,d.runtime_generation,a.vm_uid),'UTF8')),'hex');
    IF a.cleanup_admission_id IS NULL OR a.purge_disk IS DISTINCT FROM false
       OR s.cleanup_admission_id IS NULL OR s.stop_evidence IS DISTINCT FROM expected_stop
       OR old_cleanup.id IS NULL OR old_cleanup.owner_kind IS DISTINCT FROM 'thread'
       OR old_cleanup.owner_id IS DISTINCT FROM a.thread_id OR old_cleanup.pvc_uid IS DISTINCT FROM a.pvc_uid
       OR old_cleanup.parent_admission_id IS NOT NULL
       OR old_cleanup.source IS DISTINCT FROM 'pinned_thread_retirement'
       OR old_cleanup.request_id IS DISTINCT FROM a.cleanup_request_id
       OR old_cleanup.intent_digest IS DISTINCT FROM old_digest OR a.intent_digest IS DISTINCT FROM old_digest
       OR old_cleanup.completed_at IS NULL OR old_cleanup.outcome IS DISTINCT FROM 'completed'
       OR soft.thread_id IS NULL OR soft.permanent IS DISTINCT FROM false
       OR soft.outcome IS DISTINCT FROM 'settled' OR soft.disposition IS DISTINCT FROM 'ended'
       OR soft.agent_id IS DISTINCT FROM a.agent_id OR soft.runtime_attach_token IS DISTINCT FROM a.attach_token
       OR old_cleanup.completed_at>soft.settled_at
       OR source.resolved_at IS NULL OR source.resolved_at>old_cleanup.admitted_at
       OR NOT EXISTS (SELECT 1 FROM public.managed_repository_process_zero_receipts p
           WHERE p.id=s.process_zero_receipt_id AND p.owner_kind='thread' AND p.owner_id=a.thread_id
             AND p.scope='vm' AND p.provisioner='vm' AND p.runtime_incarnation=a.provision_generation::text
             AND p.observed_at<=soft.settled_at)
       OR charge.id IS NULL OR charge.state IS DISTINCT FROM 'released'
       OR charge.resource_version IS DISTINCT FROM 2 OR charge.request_id IS DISTINCT FROM a.request_id
       OR charge.revision IS DISTINCT FROM a.reservation_revision
       OR charge.released_at IS NULL OR charge.released_at<s.accepted_at
       OR charge.release_evidence IS DISTINCT FROM jsonb_build_object(
           'kind','exact_cleanup_compute_absent','owner_kind','thread','thread_id',a.thread_id,
           'cleanup_admission_id',a.cleanup_admission_id,'reservation_revision',a.reservation_revision,
           'stop_evidence_digest','sha256:'||encode(sha256(convert_to(s.stop_evidence::text,'UTF8')),'hex'))
       OR charge.vmi_uid IS DISTINCT FROM a.vmi_uid OR charge.launcher_uid IS DISTINCT FROM a.launcher_uid
       OR (charge.vm_uid IS NOT NULL AND charge.vm_uid IS DISTINCT FROM a.vm_uid)
       OR (a.vmi_uid IS NULL AND (source.ready_at IS NOT NULL OR charge.vm_uid IS NOT NULL OR a.launcher_uid IS NOT NULL))
       OR (a.vmi_uid IS NOT NULL AND (source.ready_at IS NULL OR charge.vm_uid IS DISTINCT FROM a.vm_uid OR a.launcher_uid IS NULL))
       OR EXISTS (SELECT 1 FROM public.vm_resource_reservations r WHERE r.request_id=a.request_id AND r.revision>a.reservation_revision)
       OR EXISTS (SELECT 1 FROM public.vm_resource_recovery_successors r WHERE r.reservation_id=a.reservation_id
           AND (r.successor_vmi_uid IS DISTINCT FROM a.vmi_uid OR r.successor_launcher_uid IS DISTINCT FROM a.launcher_uid))
       OR NOT EXISTS (SELECT 1 FROM public.vm_resource_waiters w WHERE w.request_id=a.request_id
           AND w.owner_kind='thread' AND w.thread_id=a.thread_id AND w.provision_generation=a.provision_generation AND w.state='released') THEN
        RAISE EXCEPTION 'VM retained disk predecessor unproven' USING ERRCODE='23514';
    END IF;
    SELECT terminal_row.* INTO terminal FROM public.vm_thread_retained_resume_terminals terminal_row
        JOIN public.vm_thread_retained_resumes op ON op.id=terminal_row.operation_id
        WHERE op.thread_id=a.thread_id AND terminal_row.runtime_generation=d.runtime_generation
          AND terminal_row.compute_cleanup_admission_id=a.cleanup_admission_id
          AND EXISTS (SELECT 1 FROM public.thread_runtime_retirement_outcomes o
              WHERE o.thread_id=a.thread_id AND o.runtime_generation=terminal_row.runtime_generation
                AND o.retirement_token=terminal_row.retirement_token AND NOT o.permanent AND o.outcome='settled'
                AND o.disposition='ended' AND o.settled_at=owner_row.ended_at);
    IF terminal.id IS NOT NULL THEN
        SELECT * INTO soft FROM public.thread_runtime_retirement_outcomes
            WHERE thread_id=a.thread_id AND runtime_generation=terminal.runtime_generation AND retirement_token=terminal.retirement_token;
    END IF;
    SELECT * INTO early_op FROM public.vm_thread_retained_resumes
        WHERE thread_id=a.thread_id AND public.valid_vm_thread_retained_runtime(vm_thread_retained_resumes,owner_row)
          AND compute_cleanup_admission_id=a.cleanup_admission_id;
    early_end := COALESCE(public.valid_vm_thread_retained_early_end(owner_row,early_op),false);
    IF owner_row.id IS NULL OR owner_row.execution_lane IS DISTINCT FROM 'pinned'
       OR (NOT early_end AND (owner_row.status IS DISTINCT FROM 'ended' OR owner_row.ended_at IS DISTINCT FROM soft.settled_at))
       OR owner_row.runtime_generation IS DISTINCT FROM d.runtime_generation
       OR (NOT early_end AND terminal.id IS NULL AND d.runtime_generation IS DISTINCT FROM a.runtime_generation)
       OR source.request_id IS NULL OR source.owner_kind IS DISTINCT FROM 'thread'
       OR source.thread_id IS DISTINCT FROM a.thread_id OR (source.state IS DISTINCT FROM 'succeeded' AND NOT COALESCE(public.valid_vm_thread_retained_handoff(source),false))
       OR NOT (source.origin='initial' OR public.valid_vm_thread_retained_resume_source(source)) OR source.thread_wake_operation_id IS NOT NULL
       OR source.thread_runtime_generation IS DISTINCT FROM a.runtime_generation
       OR source.thread_agent_id IS DISTINCT FROM a.agent_id OR source.thread_attach_token IS DISTINCT FROM a.attach_token
       OR source.provision_generation IS DISTINCT FROM a.provision_generation
       OR source.observed_vm_uid IS DISTINCT FROM a.vm_uid OR source.observed_pvc_uid IS DISTINCT FROM a.pvc_uid
       OR source.controller_configuration->>'version' IS DISTINCT FROM '3'
       OR source.revision IS DISTINCT FROM d.source_revision
       OR owner_row.runtime_retirement_token IS DISTINCT FROM d.retirement_token
       OR d.retirement_token=a.retirement_token OR owner_row.runtime_retirement_permanent IS DISTINCT FROM true
       OR owner_row.runtime_retirement_authorized_at IS NULL
       OR (NOT early_end AND (owner_row.agent_id IS NOT NULL OR owner_row.runtime_attach_token IS NOT NULL
       OR owner_row.control_admission_agent_id IS NOT NULL
       OR EXISTS (SELECT 1 FROM public.agents agent WHERE agent.thread_id=a.thread_id)))
       OR owner_row.runtime_retirement_context IS DISTINCT FROM d.retirement_context
       OR d.retirement_context->>'thread_id' IS DISTINCT FROM a.thread_id::text
       OR d.retirement_context->>'generation' IS DISTINCT FROM d.runtime_generation::text
       OR (NOT early_end AND d.retirement_context->>'entry_status' IS DISTINCT FROM 'ended')
       OR d.retirement_context->>'settle_status' IS DISTINCT FROM 'ended'
       OR (NOT early_end AND (d.retirement_context->'agent_id' IS DISTINCT FROM 'null'::jsonb
       OR d.retirement_context->'runtime_attach_token' IS DISTINCT FROM 'null'::jsonb
       OR d.retirement_context->'control_admission_agent_id' IS DISTINCT FROM 'null'::jsonb))
       OR cleanup.id IS NULL OR cleanup.owner_kind IS DISTINCT FROM 'thread'
       OR cleanup.owner_id IS DISTINCT FROM a.thread_id OR cleanup.pvc_uid IS DISTINCT FROM a.pvc_uid
       OR cleanup.parent_admission_id IS NOT NULL
       OR cleanup.source IS DISTINCT FROM 'pinned_thread_retained_disk_purge'
       OR cleanup.request_id IS DISTINCT FROM d.cleanup_request_id
       OR cleanup.intent_digest IS DISTINCT FROM new_digest OR d.intent_digest IS DISTINCT FROM new_digest
       OR (cleanup.completed_at IS NOT NULL AND (cleanup.outcome IS DISTINCT FROM 'completed'
           OR NOT EXISTS (SELECT 1 FROM public.vm_thread_retained_disk_purge_receipts p WHERE p.cleanup_admission_id=d.cleanup_admission_id))) THEN
        RAISE EXCEPTION 'VM retained disk current authority changed' USING ERRCODE='23514';
    END IF;
    captured := CASE WHEN early_end THEN early_op.retained_vm ELSE d.retirement_context->'vm' END;
    vm := owner_row.metadata->'vm';
    IF after_endpoint_cleanup AND NOT (owner_row.metadata ? 'vm')
       AND owner_row.runtime_retirement_external_cleanup IS NOT NULL
       AND owner_row.runtime_retirement_external_cleanup =
           public.pinned_retirement_external_cleanup_expected(d.retirement_context,d.runtime_generation,d.retirement_token)
       AND EXISTS (SELECT 1 FROM public.vm_thread_retained_disk_purge_receipts p WHERE p.cleanup_admission_id=d.cleanup_admission_id) THEN
        vm := captured;
    END IF;
    -- A never-Ready charge stays unbound even if metadata recorded an observed
    -- VMI/launcher before Ready. As in 0295, only an actual reservation binding
    -- constrains those fields; its physical proof still requires whole-runtime absence.
    IF vm->>'creation_request_id' IS DISTINCT FROM a.request_id::text
       OR vm->>'provision_generation' IS DISTINCT FROM a.provision_generation::text
       OR vm->>'identity_provision_generation' IS DISTINCT FROM a.provision_generation::text
       OR vm->'identity_authenticated' IS DISTINCT FROM 'true'::jsonb
       OR vm->>'vm_uid' IS DISTINCT FROM a.vm_uid::text OR vm->>'rootdisk_pvc_uid' IS DISTINCT FROM a.pvc_uid::text
       OR vm->>'status' IS DISTINCT FROM 'deleted'
       OR (vm->>'_runtime_incarnation' IS NOT NULL AND vm->>'_runtime_incarnation'<>a.vm_uid::text)
       OR (a.vmi_uid IS NOT NULL AND (vm->>'vmi_uid' IS DISTINCT FROM a.vmi_uid::text OR vm->>'active_pod_uid' IS DISTINCT FROM a.launcher_uid::text))
       OR captured->>'creation_request_id' IS DISTINCT FROM a.request_id::text
       OR captured->>'provision_generation' IS DISTINCT FROM a.provision_generation::text
       OR captured->>'identity_provision_generation' IS DISTINCT FROM a.provision_generation::text
       OR captured->'identity_authenticated' IS DISTINCT FROM 'true'::jsonb
       OR captured->>'vm_uid' IS DISTINCT FROM a.vm_uid::text OR captured->>'rootdisk_pvc_uid' IS DISTINCT FROM a.pvc_uid::text
       OR captured->>'status' IS DISTINCT FROM 'deleted'
       OR (captured->>'_runtime_incarnation' IS NOT NULL AND captured->>'_runtime_incarnation'<>a.vm_uid::text)
       OR (a.vmi_uid IS NOT NULL AND (captured->>'vmi_uid' IS DISTINCT FROM a.vmi_uid::text OR captured->>'active_pod_uid' IS DISTINCT FROM a.launcher_uid::text))
       OR EXISTS (SELECT 1 FROM public.vm_idle_access_leases l WHERE l.owner_kind='thread' AND l.owner_id=a.thread_id
           AND l.closed_at IS NULL AND l.expires_at>clock_timestamp())
       OR EXISTS (SELECT 1 FROM public.vm_workspace_recoveries r
           LEFT JOIN public.vm_workspace_recovery_retention_pins pin ON pin.recovery_id=r.id AND pin.released_at IS NULL
           WHERE r.resolved_at IS NULL AND ((r.owner_kind='thread' AND r.owner_id=a.thread_id) OR pin.pvc_uid=a.pvc_uid))
       OR EXISTS (SELECT 1 FROM public.vm_workspace_recovery_retention_pins pin WHERE pin.pvc_uid=a.pvc_uid AND pin.released_at IS NULL)
       OR EXISTS (SELECT 1 FROM public.vm_workspace_cleanup_admissions c
           WHERE ((c.owner_kind='thread' AND c.owner_id=a.thread_id) OR c.pvc_uid=a.pvc_uid)
             AND c.completed_at IS NULL AND c.id<>d.cleanup_admission_id AND c.parent_admission_id IS DISTINCT FROM d.cleanup_admission_id) THEN
        RAISE EXCEPTION 'VM retained disk backing or access changed' USING ERRCODE='23514';
    END IF;
    RETURN true;
END;
$$;


ALTER FUNCTION public.thread_vm_creation_never_issued_source(uuid,text)
RENAME TO thread_vm_initial_creation_never_issued_source;
CREATE FUNCTION public.thread_vm_creation_never_issued_source(requested_thread uuid, requested_generation text)
RETURNS boolean LANGUAGE sql STABLE AS $$
    SELECT public.thread_vm_initial_creation_never_issued_source(requested_thread,requested_generation)
       OR EXISTS (SELECT 1 FROM public.threads t JOIN public.vm_creation_retries r ON r.thread_id=t.id
           WHERE t.id=requested_thread AND r.provision_generation::text=requested_generation
             AND public.valid_vm_thread_retained_resume_source(r)
             AND public.valid_thread_vm_creation_retirement_source(r,false)
             AND public.vm_thread_retained_source_terminal_evidence(r) IS NOT NULL);
$$;
ALTER FUNCTION public.pinned_vm_creation_agent_zero_source(uuid,uuid,uuid)
RENAME TO pinned_vm_initial_creation_agent_zero_source;
CREATE FUNCTION public.pinned_vm_creation_agent_zero_source(requested_thread uuid, requested_runtime uuid, requested_token uuid)
RETURNS boolean LANGUAGE sql STABLE AS $$
    SELECT public.pinned_vm_initial_creation_agent_zero_source(requested_thread,requested_runtime,requested_token)
       OR EXISTS (SELECT 1 FROM public.threads t JOIN public.vm_creation_retries r ON r.thread_id=t.id
           WHERE t.id=requested_thread AND t.runtime_generation=requested_runtime AND t.runtime_retirement_token=requested_token
             AND public.valid_vm_thread_retained_resume_source(r)
             AND public.valid_thread_vm_creation_retirement_source(r,false)
             AND t.runtime_retirement_context->>'workspace_backend'='vm'
             AND t.runtime_retirement_context->'vm'='null'::jsonb
             AND COALESCE(t.runtime_retirement_context->'workspace_binding','null'::jsonb) IN ('null'::jsonb,'{}'::jsonb)
             AND (COALESCE(t.runtime_retirement_context->'workspace_container','null'::jsonb)='null'::jsonb
                  OR (jsonb_typeof(t.runtime_retirement_context->'workspace_container')='object'
                      AND (t.runtime_retirement_context->'workspace_container')-'repo_name'-'git_remote_url'='{}'::jsonb))
             AND public.vm_thread_retained_source_terminal_evidence(r) IS NOT NULL);
$$;
CREATE OR REPLACE FUNCTION public.valid_thread_vm_creation_retirement_source(retry public.vm_creation_retries, require_captured boolean) RETURNS boolean
    LANGUAGE sql STABLE
    AS $$
    SELECT EXISTS (SELECT 1 FROM public.threads t
        WHERE t.id=retry.thread_id AND t.execution_lane='pinned'
          AND t.runtime_retirement_token IS NOT NULL
          AND t.runtime_retirement_authorized_at IS NOT NULL
          AND t.runtime_retirement_context->>'settle_status'='ended'
          AND t.runtime_retirement_context->>'generation'=t.runtime_generation::text
          AND t.runtime_retirement_context->>'agent_id' IS NOT DISTINCT FROM t.agent_id::text
          AND t.runtime_retirement_context->>'runtime_attach_token' IS NOT DISTINCT FROM t.runtime_attach_token::text
          AND public.thread_vm_creation_cleanup_lineage(t,retry)=COALESCE(
              t.runtime_retirement_context->'vm_creation_source'->>'cleanup_protocol','exact')
          AND t.runtime_retirement_context->'vm_creation_source'->>'request_id'=retry.request_id::text
          AND t.runtime_retirement_context->'vm_creation_source'->>'provision_generation'=retry.provision_generation::text
          AND t.runtime_retirement_context->'vm_creation_source'->>'thread_runtime_generation'=retry.thread_runtime_generation::text
          AND t.runtime_retirement_context->'vm_creation_source'->>'thread_agent_id' IS NOT DISTINCT FROM retry.thread_agent_id::text
          AND t.runtime_retirement_context->'vm_creation_source'->>'thread_attach_token' IS NOT DISTINCT FROM retry.thread_attach_token::text
          AND t.runtime_retirement_context->'vm_creation_source'->>'request_digest'=retry.request_digest
          AND t.runtime_retirement_context->'vm_creation_source'->>'controller_configuration_digest'=retry.controller_configuration_digest
          AND t.runtime_retirement_context->'vm_creation_source'->'captured_vm'->>'creation_request_id'=retry.request_id::text
          AND t.runtime_retirement_context->'vm_creation_source'->'captured_vm'->>'provision_generation'=retry.provision_generation::text
          AND COALESCE(t.runtime_retirement_context->'vm_creation_source'->'captured_vm'->'vm_uid','null'::jsonb)='null'::jsonb
          AND (COALESCE(t.runtime_retirement_context->'vm_creation_source'->'captured_vm'->'rootdisk_pvc_uid','null'::jsonb)='null'::jsonb
               OR t.runtime_retirement_context->'vm_creation_source'->'captured_vm'->>'rootdisk_pvc_uid'=retry.observed_pvc_uid::text)
          AND (t.metadata->'vm'=t.runtime_retirement_context->'vm_creation_source'->'captured_vm'
               OR (NOT require_captured AND retry.state='settled' AND (
                   NOT t.metadata ? 'vm' OR EXISTS (
                       SELECT 1 FROM public.vm_thread_retained_resumes op
                       WHERE op.id=retry.thread_retained_resume_id AND op.retained_vm=t.metadata->'vm'
                         AND public.valid_vm_thread_retained_resume_source(retry)
                         AND retry.observed_vm_uid IS NULL AND NOT retry.boot_counted
                         AND retry.reason IN ('creation_never_issued','creation_disposed')) OR (
                       public.valid_vm_thread_retained_resume_source(retry)
                       AND retry.reason='retained_creation_handoff'
                       AND t.metadata->'vm'->>'creation_request_id'=retry.request_id::text
                       AND t.metadata->'vm'->>'provision_generation'=retry.provision_generation::text
                       AND (t.metadata->'vm'->>'vm_uid' IS NULL OR t.metadata->'vm'->>'vm_uid'=retry.observed_vm_uid::text)
                       AND (t.metadata->'vm'->>'rootdisk_pvc_uid' IS NULL OR t.metadata->'vm'->>'rootdisk_pvc_uid'=retry.observed_pvc_uid::text)
                       AND EXISTS (SELECT 1 FROM public.vm_resource_thread_cleanup_authorities a
                           WHERE a.request_id=retry.request_id AND a.runtime_generation=t.runtime_generation
                             AND a.retirement_token=t.runtime_retirement_token)) OR EXISTS (
                       SELECT 1 FROM public.vm_thread_retained_resume_terminals terminal
                       WHERE terminal.operation_id=retry.thread_retained_resume_id
                         AND terminal.runtime_generation=t.runtime_generation AND terminal.retirement_token=t.runtime_retirement_token
                         AND terminal.source_request_id=retry.request_id AND terminal.retained_vm=t.metadata->'vm')))));
$$;
CREATE OR REPLACE FUNCTION public.capture_vm_thread_creation_settlement() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE owner_row public.threads%ROWTYPE;
        retry public.vm_creation_retries%ROWTYPE;
BEGIN
    SELECT * INTO owner_row FROM public.threads WHERE id=NEW.thread_id FOR UPDATE;
    IF NEW.disposition='ended' AND owner_row.runtime_retirement_context->'vm_creation_source'
       NOT IN ('null'::jsonb,'{}'::jsonb)
       AND NOT EXISTS (SELECT 1 FROM public.vm_creation_retries retained WHERE retained.request_id::text=owner_row.runtime_retirement_context->'vm_creation_source'->>'request_id' AND retained.thread_retained_resume_id IS NOT NULL)
       AND public.pinned_vm_creation_agent_zero_source(NEW.thread_id,NEW.runtime_generation,NEW.retirement_token) THEN
        PERFORM 1 FROM public.vm_thread_creation_owners WHERE thread_id=NEW.thread_id FOR UPDATE;
        SELECT * INTO retry FROM public.vm_creation_retries
            WHERE request_id::text=owner_row.runtime_retirement_context->'vm_creation_source'->>'request_id'
              AND owner_kind='thread' AND thread_id=NEW.thread_id FOR UPDATE;
        PERFORM public.lock_vm_thread_creation_terminal_ledger(NEW.thread_id,retry.request_id);
        INSERT INTO public.vm_thread_creation_settlements(
            request_id,thread_id,runtime_generation,retirement_token,terminal_evidence,local_quiescence)
        VALUES (retry.request_id,NEW.thread_id,NEW.runtime_generation,NEW.retirement_token,
            public.vm_thread_creation_terminal_evidence(retry),owner_row.runtime_retirement_local_quiescence);
    END IF;
    RETURN NEW;
END;
$$;

CREATE FUNCTION public.valid_vm_thread_retained_handoff(source public.vm_creation_retries)
RETURNS boolean LANGUAGE sql STABLE AS $$
    SELECT public.valid_vm_thread_retained_resume_source(source)
       AND source.state='settled' AND source.reason='retained_creation_handoff'
       AND source.boot_counted AND source.ready_at IS NULL AND source.resolved_at IS NOT NULL
       AND source.observed_vm_uid IS NOT NULL AND source.observed_pvc_uid=source.expected_pvc_uid
       AND source.cancellation_disposition IS NULL
       AND EXISTS (SELECT 1 FROM public.vm_workspace_cleanup_admissions c
           WHERE c.id=source.creation_admission_id AND c.owner_kind='thread' AND c.owner_id=source.thread_id
             AND c.source='controller_vm_create' AND c.pvc_uid=source.expected_pvc_uid
             AND c.completed_at IS NOT NULL AND c.outcome='adopted')
       AND (SELECT count(*)=3 AND count(DISTINCT e.effect_kind)=3
                   AND bool_and(e.state='observed' AND e.effect_kind IN ('rootdisk','cloud_init','vm')
                       AND e.carrier_uid=source.creation_carrier_uid AND e.carrier_namespace=source.creation_carrier_namespace)
            FROM public.vm_creation_effects e WHERE e.request_id=source.request_id AND e.state<>'rejected')
       AND EXISTS (SELECT 1 FROM public.vm_creation_effects vm
           JOIN public.vm_creation_effects root ON root.request_id=vm.request_id AND root.effect_kind='rootdisk' AND root.state='observed'
           JOIN public.vm_creation_effects secret ON secret.request_id=vm.request_id AND secret.effect_kind='cloud_init' AND secret.state='observed'
           WHERE vm.request_id=source.request_id AND vm.effect_kind='vm' AND vm.state='observed'
             AND vm.evidence->>'uid'=source.observed_vm_uid::text
             AND vm.evidence->>'pvc_uid'=source.observed_pvc_uid::text
             AND root.evidence->>'pvc_uid'=source.observed_pvc_uid::text
             AND vm.evidence->>'cloud_init_uid'=secret.evidence->>'uid');
$$;
CREATE OR REPLACE FUNCTION public.validate_vm_thread_cleanup_authority(a public.vm_resource_thread_cleanup_authorities, allow_released boolean DEFAULT false)
RETURNS boolean LANGUAGE plpgsql AS $$
DECLARE
    owner_row public.threads%ROWTYPE;
    source public.vm_creation_retries%ROWTYPE;
    charge public.vm_resource_reservations%ROWTYPE;
    cleanup public.vm_workspace_cleanup_admissions%ROWTYPE;
    outcome public.thread_runtime_retirement_outcomes%ROWTYPE;
    vm jsonb;
    expected_digest text;
    terminal_handoff boolean;
BEGIN
    SELECT * INTO owner_row FROM public.threads WHERE id=a.thread_id FOR UPDATE;
    SELECT * INTO source FROM public.vm_creation_retries WHERE request_id=a.request_id FOR UPDATE;
    SELECT * INTO cleanup FROM public.vm_workspace_cleanup_admissions WHERE id=a.cleanup_admission_id FOR UPDATE;
    SELECT * INTO charge FROM public.vm_resource_reservations WHERE id=a.reservation_id FOR UPDATE;
    terminal_handoff := COALESCE(public.valid_vm_thread_retained_handoff(source),false);
    vm := owner_row.metadata->'vm';
    IF terminal_handoff THEN
        IF NOT public.valid_thread_vm_creation_retirement_source(source,false)
           OR a.retirement_context IS DISTINCT FROM owner_row.runtime_retirement_context THEN
            RAISE EXCEPTION 'VM retained handoff retirement changed' USING ERRCODE='23514';
        END IF;
        -- Identity comes from the sealed observed source, never a rewritten Begin.
        vm := jsonb_build_object('creation_request_id',source.request_id,
            'provision_generation',source.provision_generation,'identity_provision_generation',source.provision_generation,
            'identity_authenticated',true,'vm_uid',source.observed_vm_uid,'rootdisk_pvc_uid',source.observed_pvc_uid);
    END IF;
    -- Permanent endpoint cleanup deliberately removes metadata.vm after this
    -- authority's purge and debit. Its existing exact external receipt then
    -- authorizes reading the immutable Begin snapshot for final deletion.
    IF allow_released AND a.purge_disk AND NOT (owner_row.metadata ? 'vm')
       AND owner_row.runtime_retirement_external_cleanup IS NOT NULL
       AND owner_row.runtime_retirement_external_cleanup =
           public.pinned_retirement_external_cleanup_expected(owner_row.runtime_retirement_context,
               owner_row.runtime_generation,owner_row.runtime_retirement_token) THEN
        vm := a.retirement_context->'vm';
    END IF;
    expected_digest := 'sha256:' || encode(sha256(convert_to(format(
        '{"owner_id":"%s","owner_kind":"thread","provision_generation":"%s","purge_disk":%s,"pvc_uid":"%s","resource":"vm_workspace","source":"pinned_thread_retirement","vm_uid":"%s"}',
        a.thread_id,a.provision_generation,a.purge_disk::text,a.pvc_uid,a.vm_uid), 'UTF8')),'hex');
    IF owner_row.id IS NULL OR owner_row.execution_lane IS DISTINCT FROM 'pinned'
       OR owner_row.runtime_generation IS DISTINCT FROM a.runtime_generation
       OR source.request_id IS NULL OR source.owner_kind IS DISTINCT FROM 'thread'
       OR source.thread_id IS DISTINCT FROM a.thread_id OR (source.state IS DISTINCT FROM 'succeeded' AND NOT terminal_handoff)
       OR source.thread_runtime_generation IS DISTINCT FROM a.runtime_generation
       OR source.thread_agent_id IS DISTINCT FROM a.agent_id
       OR source.thread_attach_token IS DISTINCT FROM a.attach_token
       OR source.provision_generation IS DISTINCT FROM a.provision_generation
       OR source.observed_vm_uid IS DISTINCT FROM a.vm_uid
       OR source.observed_pvc_uid IS DISTINCT FROM a.pvc_uid
       OR source.controller_configuration->>'version' IS DISTINCT FROM '3'
       OR charge.id IS NULL OR charge.request_id IS DISTINCT FROM a.request_id
       OR charge.revision IS DISTINCT FROM a.reservation_revision OR charge.resource_version<>2
       OR (charge.state NOT IN ('reserved','active','warm','teardown')
           AND NOT (allow_released AND charge.state='released'))
       OR charge.vmi_uid IS DISTINCT FROM a.vmi_uid
       OR charge.launcher_uid IS DISTINCT FROM a.launcher_uid
       OR (charge.vm_uid IS NOT NULL AND charge.vm_uid IS DISTINCT FROM a.vm_uid)
       OR (a.vmi_uid IS NULL AND (source.ready_at IS NOT NULL OR charge.vm_uid IS NOT NULL))
       OR (a.vmi_uid IS NOT NULL AND (source.ready_at IS NULL OR charge.vm_uid IS DISTINCT FROM a.vm_uid))
       OR EXISTS (SELECT 1 FROM public.vm_resource_reservations newer
           WHERE newer.request_id=a.request_id AND newer.revision>a.reservation_revision)
       OR EXISTS (SELECT 1 FROM public.vm_resource_recovery_successors successor
           WHERE successor.reservation_id=a.reservation_id
             AND (successor.successor_vmi_uid IS DISTINCT FROM a.vmi_uid
                  OR successor.successor_launcher_uid IS DISTINCT FROM a.launcher_uid))
       OR NOT EXISTS (SELECT 1 FROM public.vm_resource_waiters w
           WHERE w.request_id=a.request_id AND w.owner_kind='thread' AND w.thread_id=a.thread_id
             AND w.provision_generation=a.provision_generation
             AND (w.state='admitted' OR (allow_released AND w.state='released')))
       OR cleanup.id IS NULL OR cleanup.owner_kind IS DISTINCT FROM 'thread'
       OR cleanup.owner_id IS DISTINCT FROM a.thread_id OR cleanup.pvc_uid IS DISTINCT FROM a.pvc_uid
       OR cleanup.source IS DISTINCT FROM 'pinned_thread_retirement'
       OR cleanup.request_id IS DISTINCT FROM a.cleanup_request_id
       OR cleanup.intent_digest IS DISTINCT FROM a.intent_digest OR a.intent_digest<>expected_digest
       OR (cleanup.completed_at IS NOT NULL AND cleanup.outcome IS DISTINCT FROM 'completed')
       OR vm->>'creation_request_id' IS DISTINCT FROM a.request_id::text
       OR vm->>'provision_generation' IS DISTINCT FROM a.provision_generation::text
       OR vm->>'identity_provision_generation' IS DISTINCT FROM a.provision_generation::text
       OR vm->'identity_authenticated' IS DISTINCT FROM 'true'::jsonb
       OR vm->>'vm_uid' IS DISTINCT FROM a.vm_uid::text
       OR vm->>'rootdisk_pvc_uid' IS DISTINCT FROM a.pvc_uid::text
       OR (vm->>'_runtime_incarnation' IS NOT NULL AND vm->>'_runtime_incarnation'<>a.vm_uid::text)
       OR (a.vmi_uid IS NOT NULL AND (
           vm->>'vmi_uid' IS DISTINCT FROM a.vmi_uid::text
           OR vm->>'active_pod_uid' IS DISTINCT FROM a.launcher_uid::text)) THEN
        RAISE EXCEPTION 'VM thread cleanup source identity changed' USING ERRCODE='23514';
    END IF;
    IF owner_row.runtime_retirement_token IS NOT NULL THEN
        IF owner_row.runtime_retirement_token IS DISTINCT FROM a.retirement_token
           OR owner_row.runtime_retirement_authorized_at IS NULL
           OR owner_row.runtime_retirement_permanent IS DISTINCT FROM a.purge_disk
           OR owner_row.agent_id IS DISTINCT FROM a.agent_id
           OR owner_row.runtime_attach_token IS DISTINCT FROM a.attach_token
           OR a.retirement_context IS NULL
           OR a.retirement_context IS DISTINCT FROM owner_row.runtime_retirement_context
           OR a.retirement_context->>'thread_id' IS DISTINCT FROM a.thread_id::text
           OR a.retirement_context->>'generation' IS DISTINCT FROM a.runtime_generation::text
           OR a.retirement_context->>'settle_status' IS DISTINCT FROM 'ended'
           OR (NOT terminal_handoff AND (
               a.retirement_context->'vm'->>'provision_generation' IS DISTINCT FROM a.provision_generation::text
               OR a.retirement_context->'vm'->>'vm_uid' IS DISTINCT FROM a.vm_uid::text
               OR a.retirement_context->'vm'->>'rootdisk_pvc_uid' IS DISTINCT FROM a.pvc_uid::text)) THEN
            RAISE EXCEPTION 'VM thread cleanup retirement changed' USING ERRCODE='23514';
        END IF;
    ELSE
        SELECT * INTO outcome FROM public.thread_runtime_retirement_outcomes
         WHERE thread_id=a.thread_id AND runtime_generation=a.runtime_generation
           AND retirement_token=a.retirement_token;
        IF owner_row.status IS DISTINCT FROM 'ended' OR owner_row.agent_id IS NOT NULL
           OR owner_row.runtime_attach_token IS NOT NULL OR a.purge_disk
           OR outcome.thread_id IS NULL OR outcome.disposition IS DISTINCT FROM 'ended'
           OR outcome.permanent OR outcome.outcome IS DISTINCT FROM 'settled'
           OR outcome.agent_id IS DISTINCT FROM a.agent_id
           OR outcome.runtime_attach_token IS DISTINCT FROM a.attach_token
           OR owner_row.ended_at IS DISTINCT FROM outcome.settled_at
           OR vm->>'status' IS DISTINCT FROM 'deleted'
           OR cleanup.completed_at IS NULL OR cleanup.completed_at>outcome.settled_at
           OR source.resolved_at IS NULL OR source.resolved_at>cleanup.admitted_at
           OR NOT EXISTS (SELECT 1 FROM public.managed_repository_process_zero_receipts p
               WHERE p.owner_kind='thread' AND p.owner_id=a.thread_id AND p.scope='vm'
                 AND p.provisioner='vm' AND p.runtime_incarnation=a.provision_generation::text
                 AND p.observed_at<=outcome.settled_at) THEN
            RAISE EXCEPTION 'VM thread cleanup settled lineage unproven' USING ERRCODE='23514';
        END IF;
    END IF;
    RETURN true;
END;
$$;

CREATE OR REPLACE FUNCTION public.valid_vm_creation_thread_disposition_identity(retry public.vm_creation_retries, require_captured boolean) RETURNS boolean
    LANGUAGE sql STABLE
    AS $$
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
       AND ((retry.disposition_carrier_uid IS NULL
             AND retry.creation_carrier_uid IS NOT NULL
             AND retry.cancellation_disposition->>'carrier_uid'=retry.creation_carrier_uid::text
             AND retry.cancellation_disposition->>'namespace'=retry.creation_carrier_namespace)
            OR (retry.disposition_carrier_uid IS NOT NULL
             AND retry.creation_carrier_uid IS NULL
             AND retry.cancellation_disposition->>'carrier_kind'='thread_creation_cancel'
             AND retry.cancellation_disposition->>'carrier_uid'=retry.disposition_carrier_uid::text
             AND retry.cancellation_disposition->>'namespace'=retry.disposition_carrier_namespace))
       AND ((retry.cancellation_disposition->>'disk_policy'='purge_new_thread_disk'
             AND retry.expected_pvc_uid IS NULL)
            OR (retry.cancellation_disposition->>'disk_policy'='retain'
             AND retry.creation_carrier_uid IS NOT NULL
             AND retry.disposition_carrier_uid IS NULL
             AND retry.expected_pvc_uid IS NOT NULL
             AND retry.observed_pvc_uid=retry.expected_pvc_uid
             AND (retry.thread_wake_operation_id IS NOT NULL OR public.valid_vm_thread_retained_resume_source(retry))
             AND retry.cancellation_disposition->'objects'->'rootdisk'->>'pvc_uid'=retry.expected_pvc_uid::text
             AND retry.cancellation_disposition->'source'=jsonb_build_object(
                 'kind','retained','pvc_uid',retry.expected_pvc_uid::text)
             AND EXISTS (SELECT 1 FROM public.vm_creation_effects root
                 WHERE root.request_id=retry.request_id
                   AND root.effect_kind='rootdisk' AND root.state='observed'
                   AND root.evidence->>'pvc_uid'=retry.expected_pvc_uid::text
                   AND root.evidence->>'uid'=retry.cancellation_disposition->'objects'->'rootdisk'->>'uid'
                   AND root.carrier_intent->>'thread_wake_operation_id' IS NOT DISTINCT FROM retry.thread_wake_operation_id::text
                   AND root.carrier_intent->'rootdisk_source'=retry.cancellation_disposition->'source')
             AND NOT EXISTS (SELECT 1 FROM public.vm_creation_effects e
                 WHERE e.request_id=retry.request_id AND e.effect_kind='workspace_attach'
                   AND e.state<>'rejected')
             AND (public.valid_vm_thread_retained_resume_source(retry) OR EXISTS (SELECT 1 FROM public.vm_idle_operations idle
                 WHERE idle.id=retry.thread_wake_operation_id
                   AND idle.owner_kind='thread' AND idle.owner_id=retry.thread_id
                   AND idle.release_kind='pinned_thread'
                   AND idle.wake_request_id=retry.request_id
                   AND idle.wake_generation=retry.provision_generation
                   AND idle.pvc_uid=retry.expected_pvc_uid
                   AND idle.stop_verified_at IS NOT NULL
                   AND idle.stop_evidence->>'retained_pvc'='true'
                   AND idle.stop_evidence->>'pvc_uid'=retry.expected_pvc_uid::text
                   AND idle.phase IN ('waking','wake_held')
                   AND idle.closed_at IS NULL))))
       AND retry.cancellation_disposition->'workspace_storage'='null'::jsonb
       AND retry.cancellation_disposition->'workspace_instance_id'='null'::jsonb
       AND public.valid_thread_vm_creation_retirement_source(retry,require_captured)
       AND EXISTS (SELECT 1 FROM public.threads t WHERE t.id=retry.thread_id
           AND retry.cancellation_disposition->>'retirement_token'=t.runtime_retirement_token::text)) IS TRUE;
$$;
CREATE OR REPLACE FUNCTION public.vm_thread_creation_delete_evidence(owner_row public.threads)
RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE evidence jsonb;
        d public.vm_thread_retained_disk_purge_authorities%ROWTYPE;
        a public.vm_resource_thread_cleanup_authorities%ROWTYPE;
        proof jsonb;
BEGIN
    evidence := public.vm_thread_creation_compute_delete_evidence(owner_row);
    IF evidence IS NOT NULL THEN RETURN evidence; END IF;
    SELECT disk.* INTO d FROM public.vm_thread_retained_disk_purge_authorities disk
        JOIN public.vm_resource_thread_cleanup_authorities compute ON compute.cleanup_admission_id=disk.compute_cleanup_admission_id
        WHERE compute.thread_id=owner_row.id AND disk.runtime_generation=owner_row.runtime_generation
          AND disk.retirement_token=owner_row.runtime_retirement_token;
    IF NOT FOUND THEN RETURN NULL; END IF;
    SELECT * INTO a FROM public.vm_resource_thread_cleanup_authorities WHERE cleanup_admission_id=d.compute_cleanup_admission_id;
    SELECT purge_evidence INTO proof FROM public.vm_thread_retained_disk_purge_receipts WHERE cleanup_admission_id=d.cleanup_admission_id;
    IF proof IS NULL
       OR NOT EXISTS (SELECT 1 FROM public.vm_workspace_cleanup_admissions c WHERE c.id=d.cleanup_admission_id AND c.completed_at IS NOT NULL AND c.outcome='completed')
       OR NOT EXISTS (SELECT 1 FROM public.thread_runtime_retirement_outcomes o
           WHERE o.thread_id=owner_row.id AND o.runtime_generation=d.runtime_generation AND o.retirement_token=d.retirement_token
             AND o.permanent AND o.outcome='deleted' AND o.disposition='ended'
             AND o.agent_id IS NOT DISTINCT FROM owner_row.agent_id
             AND o.runtime_attach_token IS NOT DISTINCT FROM owner_row.runtime_attach_token)
       OR owner_row.runtime_retirement_external_cleanup IS NULL
       OR owner_row.runtime_retirement_external_cleanup IS DISTINCT FROM
           public.pinned_retirement_external_cleanup_expected(d.retirement_context,d.runtime_generation,d.retirement_token)
       OR NOT public.pinned_retirement_external_resources_absent(owner_row.id,owner_row.metadata)
       OR EXISTS (SELECT 1 FROM public.vm_creation_retries r WHERE r.owner_kind='thread' AND r.thread_id=owner_row.id AND r.state<>'succeeded'
           AND NOT EXISTS (SELECT 1 FROM public.vm_thread_retained_resume_terminals terminal
               WHERE terminal.operation_id=r.thread_retained_resume_id AND terminal.source_request_id=r.request_id
                 AND terminal.source_terminal_evidence=public.vm_thread_retained_source_snapshot(r)))
       OR EXISTS (SELECT 1 FROM public.vm_creation_effects e JOIN public.vm_creation_retries r USING(request_id)
           WHERE r.owner_kind='thread' AND r.thread_id=owner_row.id AND e.state='issued')
       OR EXISTS (SELECT 1 FROM public.vm_resource_reservations v JOIN public.vm_creation_retries r USING(request_id)
           WHERE r.owner_kind='thread' AND r.thread_id=owner_row.id AND v.state<>'released')
       OR EXISTS (SELECT 1 FROM public.vm_resource_waiters w WHERE w.owner_kind='thread' AND w.thread_id=owner_row.id AND w.state NOT IN ('released','cancelled'))
       OR EXISTS (SELECT 1 FROM public.vm_workspace_cleanup_admissions c WHERE c.owner_kind='thread' AND c.owner_id=owner_row.id AND c.completed_at IS NULL) THEN
        RETURN NULL;
    END IF;
    PERFORM public.validate_vm_thread_retained_disk_purge_receipt(d,proof,true);
    RETURN jsonb_build_object('version',3,'kind','retained_vm_disk_purge',
        'request_id',a.request_id,'compute_cleanup_admission_id',a.cleanup_admission_id,
        'disk_cleanup_admission_id',d.cleanup_admission_id,
        'reservation_id',a.reservation_id,'reservation_revision',a.reservation_revision,
        'runtime_generation',d.runtime_generation,'compute_retirement_token',a.retirement_token,
        'retirement_token',d.retirement_token,'local_quiescence',owner_row.runtime_retirement_local_quiescence,
        'external_cleanup',owner_row.runtime_retirement_external_cleanup,'purge_evidence',proof);
END;
$$;
COMMIT;
