-- migration:     0328_vm_job_creation_audit_owners.sql
-- description:   Retain Job VM source identity and exact terminal evidence after Delete.
-- depends-on:    0327_job_vm_repository_never_issued_safe.sql
-- transactional: yes
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

CREATE TABLE public.vm_job_creation_owners (
    job_id uuid PRIMARY KEY,
    live_job_id uuid UNIQUE REFERENCES public.jobs(id) ON DELETE SET NULL,
    deletion_receipt jsonb,
    created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    deleted_at timestamptz,
    CHECK (live_job_id IS NULL OR live_job_id=job_id),
    CHECK ((deleted_at IS NULL)=(deletion_receipt IS NULL))
);

-- The old jobs FK remains active throughout the backfill. Block source writers
-- before the snapshot, then replace only the retry-to-Job FK.
LOCK TABLE public.vm_creation_retries IN SHARE ROW EXCLUSIVE MODE;
INSERT INTO public.vm_job_creation_owners(job_id,live_job_id)
SELECT DISTINCT job_id,job_id FROM public.vm_creation_retries
WHERE owner_kind='job';
ALTER TABLE public.vm_creation_retries
    ADD CONSTRAINT vm_creation_retries_job_audit_owner_fkey FOREIGN KEY (job_id)
        REFERENCES public.vm_job_creation_owners(job_id) NOT VALID,
    DROP CONSTRAINT vm_creation_retries_job_id_fkey;

CREATE TABLE public.vm_job_creation_terminal_packets (
    request_id uuid PRIMARY KEY REFERENCES public.vm_creation_retries(request_id),
    job_id uuid NOT NULL REFERENCES public.vm_job_creation_owners(job_id),
    provision_generation uuid NOT NULL,
    terminal_kind text NOT NULL CHECK (terminal_kind IN ('never_issued','physical_stop')),
    cleanup_admission_id uuid NOT NULL REFERENCES public.vm_workspace_cleanup_admissions(id),
    evidence jsonb NOT NULL CHECK (jsonb_typeof(evidence)='object'),
    captured_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    UNIQUE (job_id,provision_generation)
);

-- One immutable settlement receipt per logical cleanup parent, including an
-- explicit no-repository case. A later normal revoke may change the live key
-- and intent ledgers, but it cannot erase what the settlement accepted.
CREATE TABLE public.vm_job_repository_settlement_receipts (
    cleanup_admission_id uuid PRIMARY KEY
        REFERENCES public.vm_workspace_cleanup_admissions(id),
    request_id uuid NOT NULL UNIQUE REFERENCES public.vm_creation_retries(request_id),
    job_id uuid NOT NULL REFERENCES public.vm_job_creation_owners(job_id),
    provision_generation uuid NOT NULL,
    authority_id uuid,
    creation_intent_id uuid,
    repository_owner text,
    repo_name text,
    project_id uuid,
    forge_key_id bigint,
    key_generation bigint,
    intent_generation bigint,
    clean_repo_url text,
    captured_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    UNIQUE (job_id,provision_generation),
    CHECK (
        (authority_id IS NULL AND creation_intent_id IS NULL
         AND repository_owner IS NULL AND repo_name IS NULL
         AND project_id IS NULL AND forge_key_id IS NULL
         AND key_generation IS NULL AND intent_generation IS NULL
         AND clean_repo_url IS NULL)
        OR
        (authority_id IS NOT NULL AND creation_intent_id IS NOT NULL
         AND repository_owner IS NOT NULL AND repo_name IS NOT NULL
         AND forge_key_id IS NOT NULL AND forge_key_id>0
         AND key_generation IS NOT NULL AND key_generation>0
         AND intent_generation IS NOT NULL AND intent_generation>0
         AND clean_repo_url IS NOT NULL)
    )
);

CREATE FUNCTION public.vm_job_repository_receipt_pair(
    receipt public.vm_job_repository_settlement_receipts
) RETURNS jsonb LANGUAGE sql STABLE AS $body$
    SELECT CASE WHEN receipt.authority_id IS NULL THEN NULL ELSE
        jsonb_build_object(
            'authority_id',receipt.authority_id,
            'creation_intent_id',receipt.creation_intent_id,
            'repository_owner',receipt.repository_owner,
            'repo_name',receipt.repo_name,
            'project_id',receipt.project_id,
            'forge_key_id',receipt.forge_key_id,
            'key_generation',receipt.key_generation,
            'intent_generation',receipt.intent_generation,
            'clean_repo_url',receipt.clean_repo_url)
    END;
$body$;

CREATE FUNCTION public.guard_vm_job_repository_settlement_receipt()
RETURNS trigger LANGUAGE plpgsql AS $guard$
DECLARE parent public.vm_workspace_cleanup_admissions%ROWTYPE;
        source public.vm_creation_retries%ROWTYPE;
BEGIN
    IF TG_OP='UPDATE' OR TG_OP='DELETE' THEN
        RAISE EXCEPTION 'Job VM repository settlement receipt is immutable'
            USING ERRCODE='23514';
    END IF;
    -- Parent-first is the settlement lock order. Final Delete holds a SHARE
    -- lock on the parent, compatible with this reader, before its Job DELETE.
    SELECT * INTO parent FROM public.vm_workspace_cleanup_admissions
        WHERE id=NEW.cleanup_admission_id FOR SHARE;
    PERFORM 1 FROM public.jobs WHERE id=NEW.job_id FOR SHARE;
    SELECT * INTO source FROM public.vm_creation_retries
        WHERE request_id=NEW.request_id;
    IF parent.id IS NULL OR parent.owner_kind<>'job'
       OR parent.owner_id IS DISTINCT FROM NEW.job_id
       OR parent.source<>'job_terminal_vm_release'
       OR parent.parent_admission_id IS NOT NULL OR parent.pvc_uid IS NOT NULL
       OR parent.request_id IS DISTINCT FROM public.uuid_generate_v5(
           public.uuid_ns_url(),
           'vm-workspace-cleanup:job_terminal_vm_release:job:' ||
           NEW.job_id::text || ':' || NEW.provision_generation::text || ':None:')
       OR parent.completed_at IS NOT NULL
       OR source.request_id IS NULL OR source.owner_kind<>'job'
       OR source.job_id IS DISTINCT FROM NEW.job_id
       OR source.provision_generation IS DISTINCT FROM NEW.provision_generation
       OR source.state<>'settled' OR source.resolved_at IS NULL
       OR NOT EXISTS (SELECT 1 FROM public.vm_job_creation_owners o
           WHERE o.job_id=NEW.job_id AND o.live_job_id=NEW.job_id
             AND o.deleted_at IS NULL)
       OR NOT public.job_vm_never_issued_repository_safe(NEW.job_id)
       OR public.vm_job_repository_receipt_pair(NEW)
          IS DISTINCT FROM public.job_vm_repository_pair_evidence(NEW.job_id) THEN
        RAISE EXCEPTION 'Job VM repository settlement receipt lacks exact live source'
            USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$guard$;
CREATE TRIGGER vm_job_repository_settlement_receipt_immutable
BEFORE INSERT OR UPDATE OR DELETE ON public.vm_job_repository_settlement_receipts
FOR EACH ROW EXECUTE FUNCTION public.guard_vm_job_repository_settlement_receipt();

CREATE FUNCTION public.vm_job_execution_chain_evidence(source_execution uuid)
RETURNS jsonb LANGUAGE sql STABLE AS $body$
    SELECT jsonb_build_object(
        'execution_id',source_execution,
        'revision_generations',COALESCE((
            SELECT jsonb_agg(generation ORDER BY generation)
            FROM public.srw_execution_spec_revisions
            WHERE execution_id=source_execution), '[]'::jsonb),
        'attempts',COALESCE((
            SELECT jsonb_agg(attempt ORDER BY attempt)
            FROM public.srw_execution_attempts
            WHERE execution_id=source_execution), '[]'::jsonb),
        'workspace_instance_ids',COALESCE((
            SELECT jsonb_agg(instance_id ORDER BY instance_id)
            FROM public.srw_execution_workspace_bindings
            WHERE execution_id=source_execution), '[]'::jsonb)
    );
$body$;

CREATE FUNCTION public.vm_job_terminal_packet_evidence(source_request uuid)
RETURNS jsonb LANGUAGE plpgsql STABLE AS $body$
DECLARE r public.vm_creation_retries%ROWTYPE;
        a public.vm_workspace_cleanup_admissions%ROWTYPE;
        repository_receipt public.vm_job_repository_settlement_receipts%ROWTYPE;
        stop_row public.vm_resource_cleanup_stop_receipts%ROWTYPE;
        reservation_count bigint;
BEGIN
    SELECT * INTO r FROM public.vm_creation_retries WHERE request_id=source_request;
    IF NOT FOUND OR r.owner_kind<>'job' OR r.job_id IS NULL
       OR r.resolved_at IS NULL THEN
        RETURN NULL;
    END IF;
    IF public.job_vm_creation_never_issued_terminal_source(
        r.job_id,r.provision_generation::text) THEN
        -- 0326 proves the settled retry and exact logical parent. Recheck
        -- 0327's *historical* non-delivery at Delete without requiring the
        -- repository to remain active after its normal revocation.
        IF NOT public.job_vm_never_issued_history_safe(r.job_id) THEN
            RETURN NULL;
        END IF;
        SELECT * INTO a FROM public.vm_workspace_cleanup_admissions
        WHERE owner_kind='job' AND owner_id=r.job_id
          AND source='job_terminal_vm_release' AND pvc_uid IS NULL
          AND completed_at IS NOT NULL AND outcome='completed'
          AND request_id=public.uuid_generate_v5(public.uuid_ns_url(),
            'vm-workspace-cleanup:job_terminal_vm_release:job:' || r.job_id::text ||
            ':' || r.provision_generation::text || ':None:');
        IF NOT FOUND THEN RETURN NULL; END IF;
        SELECT * INTO repository_receipt
        FROM public.vm_job_repository_settlement_receipts
        WHERE cleanup_admission_id=a.id AND request_id=r.request_id
          AND job_id=r.job_id AND provision_generation=r.provision_generation;
        IF NOT FOUND THEN RETURN NULL; END IF;
        RETURN jsonb_build_object(
            'kind','never_issued','job_id',r.job_id,'request_id',r.request_id,
            'provision_generation',r.provision_generation,
            'execution_id',r.execution_id,'execution_revision',r.execution_revision,
            'execution_generation',r.execution_generation,
            'execution_chain',public.vm_job_execution_chain_evidence(r.execution_id),
            'cleanup_admission_id',a.id,'cleanup_intent_digest',a.intent_digest,
            'repository_pair',public.vm_job_repository_receipt_pair(repository_receipt),
            'retry_reason',r.reason,'resolved_at',r.resolved_at);
    END IF;
    IF r.state<>'succeeded' OR r.observed_vm_uid IS NULL
       OR r.observed_pvc_uid IS NULL OR r.creation_admission_id IS NULL
       OR NOT EXISTS (SELECT 1 FROM public.managed_repository_process_zero_receipts p
           WHERE p.owner_kind='job' AND p.owner_id=r.job_id AND p.scope='vm'
             AND p.provisioner='vm'
             AND p.runtime_incarnation=r.provision_generation::text)
       OR EXISTS (SELECT 1 FROM public.vm_creation_effects e
           WHERE e.request_id=r.request_id AND e.state='issued')
       OR EXISTS (SELECT 1 FROM public.vm_resource_waiters w
           WHERE w.request_id=r.request_id AND w.state<>'released')
       OR EXISTS (SELECT 1 FROM public.vm_resource_reservations v
           WHERE v.request_id=r.request_id AND v.state<>'released') THEN
        RETURN NULL;
    END IF;
    -- A completed terminal parent is the typed physical/storage disposition;
    -- identity includes the generation, VM, PVC and purge intent.
    SELECT * INTO a FROM public.vm_workspace_cleanup_admissions
    WHERE owner_kind='job' AND owner_id=r.job_id
      AND source IN ('job_terminal_vm_release','public_vm_delete')
      AND parent_admission_id IS NULL AND pvc_uid=r.observed_pvc_uid
      AND request_id=public.uuid_generate_v5(public.uuid_ns_url(),
          'vm-workspace-cleanup:' || source || ':job:' || r.job_id::text || ':' ||
          r.provision_generation::text || ':' || r.observed_vm_uid::text || ':' ||
          r.observed_pvc_uid::text)
      AND completed_at IS NOT NULL AND outcome='completed'
    ORDER BY completed_at DESC LIMIT 1;
    IF NOT FOUND THEN RETURN NULL; END IF;
    IF a.intent_digest IS DISTINCT FROM 'sha256:' || pg_catalog.encode(
        pg_catalog.sha256(pg_catalog.convert_to(
            '{"owner_id":"' || r.job_id::text ||
            '","owner_kind":"job","provision_generation":"' ||
            r.provision_generation::text ||
            '","purge_disk":true,"pvc_uid":"' || r.observed_pvc_uid::text ||
            '","resource":"vm_workspace","source":"' || a.source ||
            '","vm_uid":"' || r.observed_vm_uid::text || '"}', 'UTF8'
        )), 'hex') THEN
        RETURN NULL;
    END IF;
    SELECT count(*) INTO reservation_count FROM public.vm_resource_reservations v
        WHERE v.request_id=r.request_id;
    IF reservation_count>0 THEN
        IF reservation_count<>1 THEN RETURN NULL; END IF;
        SELECT s.* INTO stop_row FROM public.vm_resource_cleanup_stop_receipts s
        JOIN public.vm_resource_reservations v ON v.id=s.reservation_id
        WHERE v.request_id=r.request_id AND v.state='released'
          AND s.job_id=r.job_id AND s.request_id=r.request_id
          AND s.provision_generation=r.provision_generation
          AND s.vm_uid=r.observed_vm_uid AND s.pvc_uid=r.observed_pvc_uid
          AND s.cleanup_admission_id=a.id AND s.intent_digest=a.intent_digest
          AND s.stop_evidence->'vm_absent'='true'::jsonb
          AND s.stop_evidence->'vmi_absent'='true'::jsonb
          AND s.stop_evidence->'launcher_absent'='true'::jsonb
          AND s.stop_evidence->>'pvc_disposition'='purged';
        IF NOT FOUND THEN RETURN NULL; END IF;
    ELSE
        -- A missing charge is not evidence that physical stop or storage
        -- purge occurred. Older uncharged paths remain live until they have
        -- their own exact typed terminal witness.
        RETURN NULL;
    END IF;
    RETURN jsonb_build_object(
        'kind','physical_stop','job_id',r.job_id,'request_id',r.request_id,
        'provision_generation',r.provision_generation,
        'vm_uid',r.observed_vm_uid,'pvc_uid',r.observed_pvc_uid,
        'execution_id',r.execution_id,'execution_revision',r.execution_revision,
        'execution_generation',r.execution_generation,
        'execution_chain',public.vm_job_execution_chain_evidence(r.execution_id),
        'cleanup_admission_id',a.id,'cleanup_intent_digest',a.intent_digest,
        'reservation_count',reservation_count,
        'effect_nonces',COALESCE((SELECT jsonb_agg(e.effect_nonce ORDER BY e.effect_nonce)
            FROM public.vm_creation_effects e WHERE e.request_id=r.request_id),'[]'::jsonb),
        'reservation_ids',COALESCE((SELECT jsonb_agg(v.id ORDER BY v.id)
            FROM public.vm_resource_reservations v WHERE v.request_id=r.request_id),'[]'::jsonb),
        'stop_reservation_id',stop_row.reservation_id,
        'retry_reason',r.reason,'resolved_at',r.resolved_at);
END;
$body$;

CREATE FUNCTION public.guard_vm_job_terminal_packet()
RETURNS trigger LANGUAGE plpgsql AS $guard$
DECLARE exact jsonb;
BEGIN
    IF TG_OP='UPDATE' OR TG_OP='DELETE' THEN
        RAISE EXCEPTION 'Job VM terminal packet is immutable' USING ERRCODE='23514';
    END IF;
    PERFORM 1 FROM public.jobs WHERE id=NEW.job_id FOR SHARE;
    IF NOT FOUND THEN RAISE EXCEPTION 'Job VM terminal packet needs live Job' USING ERRCODE='23514'; END IF;
    PERFORM 1 FROM public.vm_job_creation_owners
      WHERE job_id=NEW.job_id AND live_job_id=NEW.job_id AND deleted_at IS NULL FOR SHARE;
    IF NOT FOUND THEN RAISE EXCEPTION 'Job VM audit owner is retired' USING ERRCODE='23514'; END IF;
    exact := public.vm_job_terminal_packet_evidence(NEW.request_id);
    IF exact IS NULL OR NEW.evidence IS DISTINCT FROM exact
       OR NEW.job_id::text IS DISTINCT FROM exact->>'job_id'
       OR NEW.provision_generation::text IS DISTINCT FROM exact->>'provision_generation'
       OR NEW.terminal_kind IS DISTINCT FROM exact->>'kind'
       OR NEW.cleanup_admission_id::text IS DISTINCT FROM exact->>'cleanup_admission_id' THEN
        RAISE EXCEPTION 'Job VM terminal packet lacks exact source' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$guard$;
CREATE TRIGGER vm_job_terminal_packet_immutable
BEFORE INSERT OR UPDATE OR DELETE ON public.vm_job_creation_terminal_packets
FOR EACH ROW EXECUTE FUNCTION public.guard_vm_job_terminal_packet();

CREATE FUNCTION public.guard_vm_job_creation_owner()
RETURNS trigger LANGUAGE plpgsql AS $guard$
DECLARE expected_generations jsonb;
        expected_attempts jsonb;
        expected_recoveries jsonb;
BEGIN
    IF TG_OP='DELETE' THEN
        RAISE EXCEPTION 'Job VM audit owner is durable' USING ERRCODE='23514';
    END IF;
    IF TG_OP='INSERT' THEN
        PERFORM 1 FROM public.jobs WHERE id=NEW.job_id FOR SHARE;
        IF NOT FOUND OR NEW.live_job_id IS DISTINCT FROM NEW.job_id
           OR NEW.deleted_at IS NOT NULL OR NEW.deletion_receipt IS NOT NULL THEN
            RAISE EXCEPTION 'Job VM audit owner requires live Job' USING ERRCODE='23514';
        END IF;
        RETURN NEW;
    END IF;
    IF ROW(NEW.job_id,NEW.created_at) IS DISTINCT FROM ROW(OLD.job_id,OLD.created_at)
       OR (OLD.live_job_id IS NULL AND NEW.live_job_id IS NOT NULL)
       OR (NEW.live_job_id IS NOT NULL AND NEW.live_job_id IS DISTINCT FROM OLD.live_job_id)
       OR (OLD.deleted_at IS NOT NULL AND
           ROW(NEW.deleted_at,NEW.deletion_receipt) IS DISTINCT FROM
           ROW(OLD.deleted_at,OLD.deletion_receipt))
       OR (OLD.deleted_at IS NULL AND NEW.deleted_at IS NULL AND
           NEW.deletion_receipt IS DISTINCT FROM OLD.deletion_receipt) THEN
        RAISE EXCEPTION 'Job VM audit owner identity or tombstone changed' USING ERRCODE='23514';
    END IF;
    IF OLD.deleted_at IS NULL AND NEW.deleted_at IS NOT NULL THEN
        SELECT COALESCE(jsonb_agg(evidence ORDER BY request_id),'[]'::jsonb)
          INTO expected_generations FROM public.vm_job_creation_terminal_packets
          WHERE job_id=NEW.job_id;
        SELECT COALESCE(jsonb_agg(to_jsonb(attempt) ORDER BY lease_token),'[]'::jsonb)
          INTO expected_attempts FROM public.worker_batch_attempts attempt
          WHERE job_id=NEW.job_id;
        SELECT COALESCE(jsonb_agg(to_jsonb(recovery) - 'prior_control_reference'
            - 'prior_freeze_reference' ORDER BY recovery_id),'[]'::jsonb)
          INTO expected_recoveries FROM public.vm_workspace_recovery_jobs recovery
          WHERE job_id=NEW.job_id;
        IF NEW.live_job_id IS DISTINCT FROM NEW.job_id
           OR NEW.deletion_receipt->>'job_id' IS DISTINCT FROM NEW.job_id::text
           OR NEW.deletion_receipt->'generations' IS DISTINCT FROM expected_generations
           OR NEW.deletion_receipt->'worker_attempts' IS DISTINCT FROM expected_attempts
           OR NEW.deletion_receipt->'workspace_recoveries' IS DISTINCT FROM expected_recoveries THEN
            RAISE EXCEPTION 'Job VM audit tombstone lacks exact live owner' USING ERRCODE='23514';
        END IF;
    END IF;
    IF OLD.live_job_id IS NOT NULL AND NEW.live_job_id IS NULL
       AND OLD.deleted_at IS NULL THEN
        RAISE EXCEPTION 'Job VM audit owner cannot detach before tombstone' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$guard$;
CREATE TRIGGER vm_job_creation_owner_authority
BEFORE INSERT OR UPDATE OR DELETE ON public.vm_job_creation_owners
FOR EACH ROW EXECUTE FUNCTION public.guard_vm_job_creation_owner();

CREATE FUNCTION public.validate_vm_job_creation_owner_terminal()
RETURNS trigger LANGUAGE plpgsql AS $guard$
DECLARE owner_row public.vm_job_creation_owners%ROWTYPE;
BEGIN
    SELECT * INTO owner_row FROM public.vm_job_creation_owners WHERE job_id=NEW.job_id;
    IF owner_row.live_job_id IS NULL THEN
        IF owner_row.deleted_at IS NULL OR owner_row.deletion_receipt IS NULL
           OR EXISTS (SELECT 1 FROM public.jobs WHERE id=owner_row.job_id)
           OR (SELECT count(*) FROM public.vm_job_creation_terminal_packets p
               WHERE p.job_id=owner_row.job_id)<>
              (SELECT count(*) FROM public.vm_creation_retries r
               WHERE r.owner_kind='job' AND r.job_id=owner_row.job_id) THEN
            RAISE EXCEPTION 'Job VM audit deletion must retain every terminal source'
                USING ERRCODE='23514';
        END IF;
    ELSIF owner_row.deleted_at IS NOT NULL OR owner_row.deletion_receipt IS NOT NULL THEN
        RAISE EXCEPTION 'Job VM audit tombstone and deletion must commit together'
            USING ERRCODE='23514';
    END IF;
    RETURN NULL;
END;
$guard$;
CREATE CONSTRAINT TRIGGER vm_job_creation_owner_terminal
AFTER INSERT OR UPDATE ON public.vm_job_creation_owners
DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
EXECUTE FUNCTION public.validate_vm_job_creation_owner_terminal();

CREATE FUNCTION public.ensure_vm_job_creation_owner()
RETURNS trigger LANGUAGE plpgsql AS $guard$
BEGIN
    IF NEW.owner_kind<>'job' THEN RETURN NEW; END IF;
    PERFORM 1 FROM public.jobs WHERE id=NEW.job_id FOR SHARE;
    IF NOT FOUND THEN RAISE EXCEPTION 'Job VM source needs live Job' USING ERRCODE='23514'; END IF;
    INSERT INTO public.vm_job_creation_owners(job_id,live_job_id)
        VALUES(NEW.job_id,NEW.job_id) ON CONFLICT(job_id) DO NOTHING;
    PERFORM 1 FROM public.vm_job_creation_owners
      WHERE job_id=NEW.job_id AND live_job_id=NEW.job_id AND deleted_at IS NULL FOR SHARE;
    IF NOT FOUND THEN RAISE EXCEPTION 'retired Job VM owner cannot be relinked' USING ERRCODE='23514'; END IF;
    RETURN NEW;
END;
$guard$;
CREATE TRIGGER a_vm_job_creation_owner
BEFORE INSERT ON public.vm_creation_retries
FOR EACH ROW EXECUTE FUNCTION public.ensure_vm_job_creation_owner();

CREATE FUNCTION public.prevent_vm_job_creation_owner_reuse()
RETURNS trigger LANGUAGE plpgsql AS $guard$
BEGIN
    PERFORM 1 FROM public.vm_job_creation_owners WHERE job_id=NEW.id FOR SHARE;
    IF FOUND THEN RAISE EXCEPTION 'Job VM audit owner UUID cannot be reused' USING ERRCODE='23514'; END IF;
    RETURN NEW;
END;
$guard$;
CREATE TRIGGER vm_job_creation_owner_reuse
BEFORE INSERT ON public.jobs FOR EACH ROW
EXECUTE FUNCTION public.prevent_vm_job_creation_owner_reuse();

CREATE FUNCTION public.require_vm_job_audit_before_delete()
RETURNS trigger LANGUAGE plpgsql AS $guard$
BEGIN
    IF EXISTS (SELECT 1 FROM public.vm_job_creation_owners o WHERE o.job_id=OLD.id)
       AND NOT EXISTS (SELECT 1 FROM public.vm_job_creation_owners o
           WHERE o.job_id=OLD.id AND o.live_job_id=OLD.id
             AND o.deleted_at IS NOT NULL AND o.deletion_receipt IS NOT NULL
             AND (SELECT count(*) FROM public.vm_job_creation_terminal_packets p
                  WHERE p.job_id=OLD.id)=
                 (SELECT count(*) FROM public.vm_creation_retries r
                  WHERE r.owner_kind='job' AND r.job_id=OLD.id)) THEN
        RAISE EXCEPTION 'Job VM audit owner lacks exact terminal disposition'
            USING ERRCODE='23514';
    END IF;
    RETURN OLD;
END;
$guard$;
CREATE TRIGGER vm_job_creation_audit_before_delete
BEFORE DELETE ON public.jobs FOR EACH ROW
EXECUTE FUNCTION public.require_vm_job_audit_before_delete();

-- A retained Job source cannot gain obligations or lose audit evidence.
-- UPDATE/DELETE guards deliberately read the owner without locking it: the
-- final deleter locks and rechecks child rows after owner-first acquisition.
CREATE FUNCTION public.guard_vm_job_retained_ledger()
RETURNS trigger LANGUAGE plpgsql AS $guard$
DECLARE candidate jsonb;
        old_candidate jsonb;
        owned_job uuid;
        old_owned_job uuid;
        source_request uuid;
BEGIN
    IF TG_OP='UPDATE' THEN
        old_candidate := to_jsonb(OLD);
        IF TG_TABLE_NAME='vm_workspace_cleanup_admissions' THEN
            IF old_candidate->>'owner_kind'='job' THEN
                old_owned_job := (old_candidate->>'owner_id')::uuid;
            END IF;
        ELSIF TG_TABLE_NAME='vm_resource_cleanup_stop_receipts' THEN
            old_owned_job := (old_candidate->>'job_id')::uuid;
        ELSIF TG_TABLE_NAME='vm_resource_recovery_successors' THEN
            old_owned_job := (old_candidate->>'owner_id')::uuid;
        ELSIF TG_TABLE_NAME='vm_creation_retries'
           OR TG_TABLE_NAME='vm_resource_waiters' THEN
            IF old_candidate->>'owner_kind'='job' THEN
                old_owned_job := (old_candidate->>'job_id')::uuid;
            END IF;
        ELSE
            SELECT r.job_id INTO old_owned_job FROM public.vm_creation_retries r
            WHERE r.request_id=(old_candidate->>'request_id')::uuid
              AND r.owner_kind='job';
        END IF;
        IF NEW IS DISTINCT FROM OLD AND old_owned_job IS NOT NULL
           AND EXISTS (SELECT 1 FROM public.vm_job_creation_owners
               WHERE job_id=old_owned_job AND live_job_id IS NULL) THEN
            RAISE EXCEPTION 'retired Job VM evidence is immutable'
                USING ERRCODE='23514';
        END IF;
    END IF;
    candidate := CASE WHEN TG_OP='DELETE' THEN to_jsonb(OLD) ELSE to_jsonb(NEW) END;
    IF TG_TABLE_NAME='vm_workspace_cleanup_admissions' THEN
        IF candidate->>'owner_kind'='job' THEN owned_job := (candidate->>'owner_id')::uuid; END IF;
    ELSIF TG_TABLE_NAME='vm_resource_cleanup_stop_receipts' THEN
        owned_job := (candidate->>'job_id')::uuid;
    ELSIF TG_TABLE_NAME='vm_resource_recovery_successors' THEN
        owned_job := (candidate->>'owner_id')::uuid;
    ELSIF TG_TABLE_NAME='vm_creation_retries' OR TG_TABLE_NAME='vm_resource_waiters' THEN
        IF candidate->>'owner_kind'='job' THEN owned_job := (candidate->>'job_id')::uuid; END IF;
    ELSE
        source_request := (candidate->>'request_id')::uuid;
        SELECT r.job_id INTO owned_job FROM public.vm_creation_retries r
            WHERE r.request_id=source_request AND r.owner_kind='job';
    END IF;
    IF owned_job IS NULL THEN
        IF TG_OP='DELETE' THEN RETURN OLD; END IF;
        RETURN NEW;
    END IF;
    IF TG_OP='INSERT' THEN
        PERFORM 1 FROM public.jobs WHERE id=owned_job FOR SHARE;
        IF NOT FOUND THEN RAISE EXCEPTION 'retired Job VM owner cannot acquire obligations' USING ERRCODE='23514'; END IF;
        -- The first cleanup permit may precede its VM retry source. Its
        -- INSERT is still serialized by the live Job; the retry creates the
        -- stable audit owner before any terminal capture.
        IF NOT EXISTS (SELECT 1 FROM public.vm_job_creation_owners
            WHERE job_id=owned_job) THEN
            RETURN NEW;
        END IF;
        PERFORM 1 FROM public.vm_job_creation_owners
            WHERE job_id=owned_job AND live_job_id=owned_job AND deleted_at IS NULL FOR SHARE;
        IF NOT FOUND THEN RAISE EXCEPTION 'retired Job VM owner cannot acquire obligations' USING ERRCODE='23514'; END IF;
    ELSIF (TG_OP='DELETE' OR NEW IS DISTINCT FROM OLD)
      AND EXISTS (SELECT 1 FROM public.vm_job_creation_owners
          WHERE job_id=owned_job AND live_job_id IS NULL) THEN
        RAISE EXCEPTION 'retired Job VM evidence is immutable' USING ERRCODE='23514';
    END IF;
    IF TG_OP='DELETE' THEN RETURN OLD; END IF;
    RETURN NEW;
END;
$guard$;
CREATE TRIGGER a_vm_job_retained_ledger
BEFORE INSERT OR UPDATE OR DELETE ON public.vm_creation_effects
FOR EACH ROW EXECUTE FUNCTION public.guard_vm_job_retained_ledger();
CREATE TRIGGER a_vm_job_retained_ledger
BEFORE INSERT OR UPDATE OR DELETE ON public.vm_resource_reservations
FOR EACH ROW EXECUTE FUNCTION public.guard_vm_job_retained_ledger();
CREATE TRIGGER a_vm_job_retained_ledger
BEFORE INSERT OR UPDATE OR DELETE ON public.vm_resource_waiters
FOR EACH ROW EXECUTE FUNCTION public.guard_vm_job_retained_ledger();
CREATE TRIGGER a_vm_job_retained_ledger
BEFORE INSERT OR UPDATE OR DELETE ON public.vm_workspace_cleanup_admissions
FOR EACH ROW EXECUTE FUNCTION public.guard_vm_job_retained_ledger();
CREATE TRIGGER a_vm_job_retained_ledger
BEFORE INSERT OR UPDATE OR DELETE ON public.vm_resource_cleanup_stop_receipts
FOR EACH ROW EXECUTE FUNCTION public.guard_vm_job_retained_ledger();
CREATE TRIGGER a_vm_job_retained_ledger
BEFORE INSERT OR UPDATE OR DELETE ON public.vm_resource_recovery_successors
FOR EACH ROW EXECUTE FUNCTION public.guard_vm_job_retained_ledger();
CREATE TRIGGER a_vm_job_retained_ledger
BEFORE UPDATE OR DELETE ON public.vm_creation_retries
FOR EACH ROW EXECUTE FUNCTION public.guard_vm_job_retained_ledger();

-- The execution ledger is polymorphic. Its Job rows remain source audit after
-- deletion; existing row changes cannot rewrite that history. INSERT writers
-- already take the live Job lock in 0327.
CREATE FUNCTION public.guard_vm_job_retained_execution()
RETURNS trigger LANGUAGE plpgsql AS $guard$
DECLARE old_execution uuid;
        new_execution uuid;
        old_job uuid;
        new_job uuid;
BEGIN
    IF TG_TABLE_NAME='srw_execution_specs' THEN
        IF OLD.work_kind='Job' THEN old_job := OLD.work_id; END IF;
        IF TG_OP='UPDATE' AND NEW.work_kind='Job' THEN new_job := NEW.work_id; END IF;
    ELSIF TG_TABLE_NAME='srw_workspace_instances' THEN
        old_execution := OLD.execution_id;
        IF TG_OP='UPDATE' THEN new_execution := NEW.execution_id; END IF;
        SELECT x.work_id INTO old_job FROM public.srw_execution_workspace_bindings b
          JOIN public.srw_execution_specs x ON x.id=b.execution_id
          WHERE b.instance_id=OLD.id AND x.work_kind='Job' LIMIT 1;
        IF old_job IS NULL AND EXISTS (
            SELECT 1 FROM public.vm_job_creation_owners WHERE job_id=OLD.owner_id
        ) THEN
            old_job := OLD.owner_id;
        END IF;
        IF TG_OP='UPDATE' AND EXISTS (
            SELECT 1 FROM public.vm_job_creation_owners WHERE job_id=NEW.owner_id
        ) THEN
            new_job := NEW.owner_id;
        END IF;
    ELSE
        old_execution := OLD.execution_id;
        IF TG_OP='UPDATE' THEN new_execution := NEW.execution_id; END IF;
    END IF;
    IF old_job IS NULL AND old_execution IS NOT NULL THEN
        SELECT work_id INTO old_job FROM public.srw_execution_specs
          WHERE id=old_execution AND work_kind='Job';
    END IF;
    IF new_execution IS NOT NULL THEN
        SELECT work_id INTO new_job FROM public.srw_execution_specs
          WHERE id=new_execution AND work_kind='Job';
    END IF;
    IF (TG_OP='DELETE' OR NEW IS DISTINCT FROM OLD)
       AND (EXISTS (SELECT 1 FROM public.vm_job_creation_owners
               WHERE job_id=old_job AND live_job_id IS NULL)
            OR EXISTS (SELECT 1 FROM public.vm_job_creation_owners
               WHERE job_id=new_job AND live_job_id IS NULL)) THEN
        RAISE EXCEPTION 'retired Job VM execution evidence is immutable'
            USING ERRCODE='23514';
    END IF;
    IF TG_OP='DELETE' THEN RETURN OLD; END IF;
    RETURN NEW;
END;
$guard$;
CREATE TRIGGER vm_job_retained_execution
BEFORE UPDATE OR DELETE ON public.srw_execution_specs
FOR EACH ROW EXECUTE FUNCTION public.guard_vm_job_retained_execution();
CREATE TRIGGER vm_job_retained_execution
BEFORE UPDATE OR DELETE ON public.srw_execution_spec_revisions
FOR EACH ROW EXECUTE FUNCTION public.guard_vm_job_retained_execution();
CREATE TRIGGER vm_job_retained_execution
BEFORE UPDATE OR DELETE ON public.srw_execution_attempts
FOR EACH ROW EXECUTE FUNCTION public.guard_vm_job_retained_execution();
CREATE TRIGGER vm_job_retained_execution
BEFORE UPDATE OR DELETE ON public.srw_execution_workspace_bindings
FOR EACH ROW EXECUTE FUNCTION public.guard_vm_job_retained_execution();
CREATE TRIGGER vm_job_retained_execution
BEFORE UPDATE OR DELETE ON public.srw_workspace_instances
FOR EACH ROW EXECUTE FUNCTION public.guard_vm_job_retained_execution();

COMMIT;
