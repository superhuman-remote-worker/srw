-- migration: 0289_vm_thread_creation_audit_owners.sql
-- description: Preserve failed-initial VM source audit ownership through exact permanent End.
-- depends-on: 0288_vm_initial_creation_cleanup_lineage.sql
-- transactional: yes
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

CREATE TABLE public.vm_thread_creation_owners (
    thread_id uuid PRIMARY KEY,
    live_thread_id uuid UNIQUE REFERENCES public.threads(id) ON DELETE SET NULL,
    deleted_runtime_generation uuid,
    deleted_retirement_token uuid,
    deletion_receipt jsonb,
    created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    deleted_at timestamptz,
    CHECK (live_thread_id IS NULL OR live_thread_id=thread_id),
    FOREIGN KEY (thread_id,deleted_runtime_generation,deleted_retirement_token)
        REFERENCES public.thread_runtime_retirement_outcomes
            (thread_id,runtime_generation,retirement_token)
);

-- Old live-thread FKs remain in force throughout the bounded backfill.
-- Stop source/waiter writers before taking the backfill snapshot; the later
-- FK catalog locks alone would allow an insertion to miss the backfill.
LOCK TABLE public.vm_creation_retries, public.vm_resource_waiters IN SHARE ROW EXCLUSIVE MODE;
INSERT INTO public.vm_thread_creation_owners(thread_id,live_thread_id)
SELECT thread_id,thread_id FROM (
    SELECT thread_id FROM public.vm_creation_retries WHERE owner_kind='thread'
    UNION
    SELECT thread_id FROM public.vm_resource_waiters WHERE owner_kind='thread'
) owners;

ALTER TABLE public.vm_creation_retries
    ADD CONSTRAINT vm_creation_retries_audit_owner_fkey FOREIGN KEY (thread_id)
        REFERENCES public.vm_thread_creation_owners(thread_id) NOT VALID,
    DROP CONSTRAINT vm_creation_retries_thread_id_fkey;
ALTER TABLE public.vm_resource_waiters
    ADD CONSTRAINT vm_resource_waiters_audit_owner_fkey FOREIGN KEY (thread_id)
        REFERENCES public.vm_thread_creation_owners(thread_id) NOT VALID,
    DROP CONSTRAINT vm_resource_waiters_thread_id_fkey;

CREATE TABLE public.vm_thread_creation_settlements (
    request_id uuid PRIMARY KEY REFERENCES public.vm_creation_retries(request_id),
    thread_id uuid NOT NULL REFERENCES public.vm_thread_creation_owners(thread_id),
    runtime_generation uuid NOT NULL,
    retirement_token uuid NOT NULL,
    terminal_evidence jsonb NOT NULL,
    local_quiescence jsonb,
    settled_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    FOREIGN KEY (thread_id,runtime_generation,retirement_token)
        REFERENCES public.thread_runtime_retirement_outcomes
            (thread_id,runtime_generation,retirement_token)
);

CREATE FUNCTION public.lock_vm_thread_creation_terminal_ledger(owned_thread uuid, source_request uuid)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    PERFORM 1 FROM public.vm_creation_effects WHERE request_id=source_request ORDER BY effect_nonce FOR SHARE;
    PERFORM 1 FROM public.vm_workspace_cleanup_admissions
        WHERE owner_kind='thread' AND owner_id=owned_thread ORDER BY id FOR SHARE;
    PERFORM 1 FROM public.vm_resource_reservations WHERE request_id=source_request ORDER BY id FOR SHARE;
    PERFORM 1 FROM public.vm_resource_waiters WHERE request_id=source_request FOR SHARE;
END;
$$;

-- A stable audit identity cannot become a new effect/admission owner. These
-- INSERT guards run before child triggers which lock the immutable retry.
-- Existing live-owner cleanup admissions remain allowed through disposition.
CREATE FUNCTION public.guard_vm_thread_creation_terminal_ledger()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE candidate jsonb;
        owned_thread uuid;
        source_request uuid;
        retry public.vm_creation_retries%ROWTYPE;
BEGIN
    candidate := CASE WHEN TG_OP='DELETE' THEN to_jsonb(OLD) ELSE to_jsonb(NEW) END;
    IF TG_TABLE_NAME='vm_workspace_cleanup_admissions' THEN
        IF candidate->>'owner_kind'<>'thread' THEN
            IF TG_OP='DELETE' THEN RETURN OLD; END IF;
            RETURN NEW;
        END IF;
        owned_thread := (candidate->>'owner_id')::uuid;
        IF NOT EXISTS (SELECT 1 FROM public.vm_thread_creation_owners WHERE thread_id=owned_thread) THEN
            IF TG_OP='DELETE' THEN RETURN OLD; END IF;
            RETURN NEW;
        END IF;
    ELSE
        source_request := (candidate->>'request_id')::uuid;
        SELECT * INTO retry FROM public.vm_creation_retries WHERE request_id=source_request;
        IF retry.owner_kind IS DISTINCT FROM 'thread' THEN
            IF TG_OP='DELETE' THEN RETURN OLD; END IF;
            RETURN NEW;
        END IF;
        owned_thread := retry.thread_id;
    END IF;
    IF TG_OP='INSERT' THEN
        PERFORM 1 FROM public.threads WHERE id=owned_thread FOR SHARE;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'retired VM audit owner cannot acquire new obligations' USING ERRCODE='23514';
        END IF;
        PERFORM 1 FROM public.vm_thread_creation_owners
            WHERE thread_id=owned_thread AND live_thread_id=owned_thread AND deleted_at IS NULL FOR SHARE;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'retired VM audit owner cannot acquire new obligations' USING ERRCODE='23514';
        END IF;
        IF source_request IS NOT NULL THEN
            SELECT * INTO retry FROM public.vm_creation_retries WHERE request_id=source_request FOR SHARE;
            IF retry.state='settled' THEN
                RAISE EXCEPTION 'settled VM source cannot acquire new obligations' USING ERRCODE='23514';
            END IF;
        END IF;
    ELSIF (TG_OP='DELETE' OR NEW IS DISTINCT FROM OLD)
       AND EXISTS (SELECT 1 FROM public.vm_thread_creation_owners
           WHERE thread_id=owned_thread AND live_thread_id IS NULL) THEN
        RAISE EXCEPTION 'retired VM audit evidence is immutable' USING ERRCODE='23514';
    END IF;
    IF TG_OP='DELETE' THEN RETURN OLD; END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER a_vm_thread_creation_terminal_ledger
BEFORE INSERT OR UPDATE OR DELETE ON public.vm_creation_effects
FOR EACH ROW EXECUTE FUNCTION public.guard_vm_thread_creation_terminal_ledger();
CREATE TRIGGER a_vm_thread_creation_terminal_ledger
BEFORE INSERT OR UPDATE OR DELETE ON public.vm_resource_reservations
FOR EACH ROW EXECUTE FUNCTION public.guard_vm_thread_creation_terminal_ledger();
CREATE TRIGGER a_vm_thread_creation_terminal_ledger
BEFORE INSERT OR UPDATE OR DELETE ON public.vm_resource_waiters
FOR EACH ROW EXECUTE FUNCTION public.guard_vm_thread_creation_terminal_ledger();
CREATE TRIGGER a_vm_thread_creation_terminal_ledger
BEFORE INSERT OR UPDATE OR DELETE ON public.vm_workspace_cleanup_admissions
FOR EACH ROW EXECUTE FUNCTION public.guard_vm_thread_creation_terminal_ledger();
CREATE TRIGGER a_vm_thread_creation_terminal_ledger
BEFORE UPDATE OR DELETE ON public.vm_creation_retries
FOR EACH ROW EXECUTE FUNCTION public.guard_vm_thread_creation_terminal_ledger();

-- This packet does not grant settlement. The insertion guard below first
-- requires 0288's positive live-source proof. Thereafter byte-equal semantic
-- evidence preserves that proof without reopening the old End context.
CREATE FUNCTION public.vm_thread_creation_terminal_evidence(
    retry public.vm_creation_retries
) RETURNS jsonb LANGUAGE plpgsql STABLE AS $$
DECLARE result jsonb;
BEGIN
    IF retry.owner_kind IS DISTINCT FROM 'thread'
       OR retry.origin IS DISTINCT FROM 'initial'
       OR retry.state IS DISTINCT FROM 'settled'
       OR retry.reason IS NULL OR retry.reason NOT IN ('creation_never_issued','creation_disposed')
       OR retry.expected_pvc_uid IS NOT NULL OR retry.thread_wake_operation_id IS NOT NULL
       OR retry.observed_vm_uid IS NOT NULL OR retry.boot_counted
       OR COALESCE(retry.canonical_request->'workspace_storage','null'::jsonb)<>'null'::jsonb
       OR EXISTS (SELECT 1 FROM public.vm_creation_retries r
           WHERE r.owner_kind='thread' AND r.thread_id=retry.thread_id AND r.request_id<>retry.request_id)
       OR EXISTS (SELECT 1 FROM public.vm_creation_effects e WHERE e.request_id=retry.request_id
           AND (e.state='issued' OR (e.effect_kind='vm' AND e.state<>'rejected')))
       OR EXISTS (SELECT 1 FROM public.vm_resource_reservations v
           WHERE v.request_id=retry.request_id AND v.state<>'released')
       OR EXISTS (SELECT 1 FROM public.vm_resource_waiters w
           WHERE w.owner_kind='thread' AND w.thread_id=retry.thread_id
             AND (w.request_id<>retry.request_id OR w.state NOT IN ('released','cancelled')))
       OR EXISTS (SELECT 1 FROM public.vm_workspace_cleanup_admissions a
           WHERE a.owner_kind='thread' AND a.owner_id=retry.thread_id AND a.completed_at IS NULL)
       OR (retry.reason='creation_never_issued' AND (
           retry.observed_pvc_uid IS NOT NULL OR retry.cancellation_disposition IS NOT NULL
           OR EXISTS (SELECT 1 FROM public.vm_creation_effects e
               WHERE e.request_id=retry.request_id AND e.state<>'rejected')
           OR (retry.creation_admission_id IS NOT NULL AND (
               COALESCE(retry.canonical_request->'preparation','null'::jsonb)<>'null'::jsonb
               OR retry.controller_configuration->'golden_enabled' IS DISTINCT FROM 'false'::jsonb)))) THEN
        RETURN NULL;
    END IF;
    SELECT jsonb_build_object(
        'version',1,
        'source',to_jsonb(retry)-ARRAY['canonical_request','controller_configuration',
            'revision','claim_token','claim_expires_at','next_probe_at','backoff_attempt',
            'transport_outage_started_at','updated_at'],
        'effects',COALESCE((SELECT jsonb_agg(to_jsonb(e) ORDER BY e.effect_nonce)
            FROM public.vm_creation_effects e WHERE e.request_id=retry.request_id),'[]'::jsonb),
        'admissions',COALESCE((SELECT jsonb_agg(to_jsonb(a) ORDER BY a.id)
            FROM public.vm_workspace_cleanup_admissions a
            WHERE a.owner_kind='thread' AND a.owner_id=retry.thread_id),'[]'::jsonb),
        'reservations',COALESCE((SELECT jsonb_agg(to_jsonb(v) ORDER BY v.id)
            FROM public.vm_resource_reservations v WHERE v.request_id=retry.request_id),'[]'::jsonb),
        'waiter',(SELECT to_jsonb(w) FROM public.vm_resource_waiters w WHERE w.request_id=retry.request_id)
    ) INTO result;
    RETURN result;
END;
$$;

CREATE FUNCTION public.guard_vm_thread_creation_settlement()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE owner_row public.threads%ROWTYPE;
        retry public.vm_creation_retries%ROWTYPE;
BEGIN
    IF TG_OP<>'INSERT' THEN
        RAISE EXCEPTION 'VM thread source settlement is append-only' USING ERRCODE='23514';
    END IF;
    SELECT * INTO owner_row FROM public.threads WHERE id=NEW.thread_id FOR UPDATE;
    PERFORM 1 FROM public.vm_thread_creation_owners WHERE thread_id=NEW.thread_id FOR UPDATE;
    SELECT * INTO retry FROM public.vm_creation_retries WHERE request_id=NEW.request_id FOR UPDATE;
    PERFORM public.lock_vm_thread_creation_terminal_ledger(NEW.thread_id,NEW.request_id);
    IF owner_row.id IS NULL OR retry.thread_id IS DISTINCT FROM NEW.thread_id
       OR owner_row.runtime_generation IS DISTINCT FROM NEW.runtime_generation
       OR owner_row.runtime_retirement_token IS DISTINCT FROM NEW.retirement_token
       OR NOT public.pinned_vm_creation_agent_zero_source(
            NEW.thread_id,NEW.runtime_generation,NEW.retirement_token)
       OR NOT EXISTS (SELECT 1 FROM public.thread_runtime_retirement_outcomes o
           WHERE o.thread_id=NEW.thread_id AND o.runtime_generation=NEW.runtime_generation
             AND o.retirement_token=NEW.retirement_token AND o.disposition='ended'
             AND ((o.outcome='settled' AND NOT o.permanent) OR (o.outcome='deleted' AND o.permanent)))
       OR NEW.terminal_evidence IS DISTINCT FROM public.vm_thread_creation_terminal_evidence(retry)
       OR NEW.terminal_evidence IS NULL
       OR NEW.local_quiescence IS DISTINCT FROM owner_row.runtime_retirement_local_quiescence THEN
        RAISE EXCEPTION 'VM thread source settlement lacks exact positive End proof' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER vm_thread_creation_settlement_authority
BEFORE INSERT OR UPDATE OR DELETE ON public.vm_thread_creation_settlements
FOR EACH ROW EXECUTE FUNCTION public.guard_vm_thread_creation_settlement();

CREATE FUNCTION public.capture_vm_thread_creation_settlement()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE owner_row public.threads%ROWTYPE;
        retry public.vm_creation_retries%ROWTYPE;
BEGIN
    SELECT * INTO owner_row FROM public.threads WHERE id=NEW.thread_id FOR UPDATE;
    IF NEW.disposition='ended' AND owner_row.runtime_retirement_context->'vm_creation_source'
       NOT IN ('null'::jsonb,'{}'::jsonb)
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
CREATE TRIGGER thread_runtime_retirement_capture_vm_source
AFTER INSERT ON public.thread_runtime_retirement_outcomes
FOR EACH ROW EXECUTE FUNCTION public.capture_vm_thread_creation_settlement();

-- Called only while the live thread is locked, before its logical owner and
-- immutable source. No old source actor/generation or fresh predicate changes.
CREATE FUNCTION public.vm_thread_creation_delete_evidence(owner_row public.threads)
RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE retry public.vm_creation_retries%ROWTYPE;
        settled public.vm_thread_creation_settlements%ROWTYPE;
        terminal jsonb;
        prior_soft boolean;
BEGIN
    PERFORM 1 FROM public.vm_thread_creation_owners
        WHERE thread_id=owner_row.id AND live_thread_id=owner_row.id FOR UPDATE;
    IF NOT FOUND THEN RETURN NULL; END IF;
    SELECT * INTO retry FROM public.vm_creation_retries
        WHERE owner_kind='thread' AND thread_id=owner_row.id ORDER BY request_id LIMIT 1 FOR UPDATE;
    IF NOT FOUND THEN RETURN NULL; END IF;
    SELECT * INTO settled FROM public.vm_thread_creation_settlements WHERE request_id=retry.request_id FOR SHARE;
    IF NOT FOUND THEN RETURN NULL; END IF;
    PERFORM public.lock_vm_thread_creation_terminal_ledger(owner_row.id,retry.request_id);
    terminal := public.vm_thread_creation_terminal_evidence(retry);
    prior_soft := EXISTS (SELECT 1 FROM public.thread_runtime_retirement_outcomes o
        WHERE o.thread_id=settled.thread_id AND o.runtime_generation=settled.runtime_generation
          AND o.retirement_token=settled.retirement_token AND o.disposition='ended'
          AND o.outcome='settled' AND NOT o.permanent);
    IF terminal IS NULL OR settled.terminal_evidence IS DISTINCT FROM terminal
       OR settled.thread_id IS DISTINCT FROM owner_row.id
       OR settled.runtime_generation IS DISTINCT FROM owner_row.runtime_generation
       OR owner_row.execution_lane IS DISTINCT FROM 'pinned'
       OR owner_row.runtime_retirement_token IS NULL
       OR owner_row.runtime_retirement_authorized_at IS NULL
       OR owner_row.runtime_retirement_permanent IS DISTINCT FROM true
       OR owner_row.runtime_retirement_context->>'generation' IS DISTINCT FROM owner_row.runtime_generation::text
       OR owner_row.runtime_retirement_context->>'settle_status' IS DISTINCT FROM 'ended'
       OR NOT EXISTS (SELECT 1 FROM public.thread_runtime_retirement_outcomes o
           WHERE o.thread_id=owner_row.id AND o.runtime_generation=owner_row.runtime_generation
             AND o.retirement_token=owner_row.runtime_retirement_token AND o.permanent
             AND o.outcome='deleted' AND o.disposition='ended'
             AND o.agent_id IS NOT DISTINCT FROM owner_row.agent_id
             AND o.runtime_attach_token IS NOT DISTINCT FROM owner_row.runtime_attach_token)
       OR owner_row.runtime_retirement_external_cleanup IS NULL
       OR owner_row.runtime_retirement_external_cleanup IS DISTINCT FROM
           public.pinned_retirement_external_cleanup_expected(owner_row.runtime_retirement_context,
               owner_row.runtime_generation,owner_row.runtime_retirement_token)
       OR NOT public.pinned_retirement_external_resources_absent(owner_row.id,owner_row.metadata)
       OR (settled.retirement_token IS DISTINCT FROM owner_row.runtime_retirement_token AND NOT (
           prior_soft AND owner_row.status='ended' AND owner_row.agent_id IS NULL
           AND owner_row.runtime_attach_token IS NULL AND owner_row.control_admission_agent_id IS NULL
           AND NOT EXISTS (SELECT 1 FROM public.agents a WHERE a.thread_id=owner_row.id))) THEN
        RETURN NULL;
    END IF;
    RETURN jsonb_build_object('version',1,'request_id',retry.request_id,
        'source_runtime_generation',settled.runtime_generation,
        'source_retirement_token',settled.retirement_token,
        'runtime_generation',owner_row.runtime_generation,
        'retirement_token',owner_row.runtime_retirement_token,
        'local_quiescence',owner_row.runtime_retirement_local_quiescence,
        'external_cleanup',owner_row.runtime_retirement_external_cleanup);
END;
$$;

CREATE FUNCTION public.guard_vm_thread_creation_owner()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE owner_row public.threads%ROWTYPE;
BEGIN
    IF TG_OP='DELETE' THEN
        RAISE EXCEPTION 'VM thread audit owner is permanent' USING ERRCODE='23514';
    END IF;
    IF TG_OP='INSERT' THEN
        SELECT * INTO owner_row FROM public.threads WHERE id=NEW.thread_id FOR SHARE;
        IF NOT FOUND OR NEW.live_thread_id IS DISTINCT FROM NEW.thread_id
           OR NEW.deleted_runtime_generation IS NOT NULL OR NEW.deleted_retirement_token IS NOT NULL
           OR NEW.deletion_receipt IS NOT NULL OR NEW.deleted_at IS NOT NULL THEN
            RAISE EXCEPTION 'VM audit owner requires its exact live thread' USING ERRCODE='23514';
        END IF;
        RETURN NEW;
    END IF;
    IF ROW(NEW.thread_id,NEW.created_at) IS DISTINCT FROM ROW(OLD.thread_id,OLD.created_at)
       OR (OLD.live_thread_id IS NULL AND NEW.live_thread_id IS NOT NULL)
       OR (NEW.live_thread_id IS NOT NULL AND NEW.live_thread_id IS DISTINCT FROM OLD.live_thread_id)
       OR (OLD.deleted_at IS NOT NULL AND ROW(NEW.deleted_runtime_generation,NEW.deleted_retirement_token,
            NEW.deletion_receipt,NEW.deleted_at) IS DISTINCT FROM
            ROW(OLD.deleted_runtime_generation,OLD.deleted_retirement_token,OLD.deletion_receipt,OLD.deleted_at)) THEN
        RAISE EXCEPTION 'VM audit owner identity and tombstone are immutable' USING ERRCODE='23514';
    END IF;
    IF OLD.deleted_at IS NULL AND NEW.deleted_at IS NOT NULL THEN
        SELECT * INTO owner_row FROM public.threads WHERE id=NEW.thread_id FOR UPDATE;
        IF NOT FOUND OR NEW.live_thread_id IS DISTINCT FROM NEW.thread_id
           OR NEW.deleted_runtime_generation IS DISTINCT FROM owner_row.runtime_generation
           OR NEW.deleted_retirement_token IS DISTINCT FROM owner_row.runtime_retirement_token
           OR NEW.deletion_receipt IS NULL
           OR NEW.deletion_receipt IS DISTINCT FROM public.vm_thread_creation_delete_evidence(owner_row) THEN
            RAISE EXCEPTION 'VM audit tombstone lacks exact current permanent End' USING ERRCODE='23514';
        END IF;
    ELSIF OLD.deleted_at IS NULL AND ROW(NEW.deleted_runtime_generation,NEW.deleted_retirement_token,NEW.deletion_receipt)
          IS DISTINCT FROM ROW(OLD.deleted_runtime_generation,OLD.deleted_retirement_token,OLD.deletion_receipt) THEN
        RAISE EXCEPTION 'VM audit tombstone is incomplete' USING ERRCODE='23514';
    END IF;
    IF OLD.live_thread_id IS NOT NULL AND NEW.live_thread_id IS NULL AND (
        OLD.deleted_at IS NULL OR EXISTS (SELECT 1 FROM public.threads WHERE id=NEW.thread_id)) THEN
        RAISE EXCEPTION 'VM audit owner can detach only with exact thread deletion' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER vm_thread_creation_owner_authority
BEFORE INSERT OR UPDATE OR DELETE ON public.vm_thread_creation_owners
FOR EACH ROW EXECUTE FUNCTION public.guard_vm_thread_creation_owner();

CREATE FUNCTION public.validate_vm_thread_creation_owner_terminal()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE owner_row public.vm_thread_creation_owners%ROWTYPE;
BEGIN
    SELECT * INTO owner_row FROM public.vm_thread_creation_owners WHERE thread_id=NEW.thread_id;
    IF (owner_row.live_thread_id IS NOT NULL AND (
            owner_row.deleted_at IS NOT NULL OR owner_row.deleted_runtime_generation IS NOT NULL
            OR owner_row.deleted_retirement_token IS NOT NULL OR owner_row.deletion_receipt IS NOT NULL))
       OR (owner_row.live_thread_id IS NULL AND (
            owner_row.deleted_at IS NULL OR owner_row.deleted_runtime_generation IS NULL
            OR owner_row.deleted_retirement_token IS NULL OR owner_row.deletion_receipt IS NULL
            OR EXISTS (SELECT 1 FROM public.threads WHERE id=owner_row.thread_id)
            OR NOT EXISTS (SELECT 1 FROM public.thread_runtime_retirement_outcomes o
                WHERE o.thread_id=owner_row.thread_id AND o.runtime_generation=owner_row.deleted_runtime_generation
                  AND o.retirement_token=owner_row.deleted_retirement_token
                  AND o.permanent AND o.outcome='deleted' AND o.disposition='ended'))) THEN
        RAISE EXCEPTION 'VM audit owner detach and exact permanent deletion must commit together' USING ERRCODE='23514';
    END IF;
    RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER vm_thread_creation_owner_terminal
AFTER INSERT OR UPDATE ON public.vm_thread_creation_owners
DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
EXECUTE FUNCTION public.validate_vm_thread_creation_owner_terminal();

CREATE FUNCTION public.ensure_vm_thread_creation_owner()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE owner_row public.threads%ROWTYPE;
BEGIN
    IF NEW.owner_kind<>'thread' THEN RETURN NEW; END IF;
    SELECT * INTO owner_row FROM public.threads WHERE id=NEW.thread_id FOR SHARE;
    IF NOT FOUND OR owner_row.runtime_retirement_token IS NOT NULL THEN
        RAISE EXCEPTION 'VM audit source requires a live nonretiring thread' USING ERRCODE='23514';
    END IF;
    INSERT INTO public.vm_thread_creation_owners(thread_id,live_thread_id)
        VALUES (NEW.thread_id,NEW.thread_id) ON CONFLICT(thread_id) DO NOTHING;
    PERFORM 1 FROM public.vm_thread_creation_owners
        WHERE thread_id=NEW.thread_id AND live_thread_id=NEW.thread_id AND deleted_at IS NULL FOR SHARE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'retired VM audit owner cannot be relinked' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER vm_thread_creation_source_audit_owner
BEFORE INSERT ON public.vm_creation_retries
FOR EACH ROW EXECUTE FUNCTION public.ensure_vm_thread_creation_owner();
CREATE TRIGGER vm_thread_creation_waiter_audit_owner
BEFORE INSERT ON public.vm_resource_waiters
FOR EACH ROW EXECUTE FUNCTION public.ensure_vm_thread_creation_owner();

CREATE FUNCTION public.prevent_vm_thread_creation_owner_reuse()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM 1 FROM public.vm_thread_creation_owners WHERE thread_id=NEW.id FOR SHARE;
    IF FOUND THEN
        RAISE EXCEPTION 'VM audit owner thread UUID cannot be reused' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER thread_vm_creation_audit_owner_reuse
BEFORE INSERT ON public.threads
FOR EACH ROW EXECUTE FUNCTION public.prevent_vm_thread_creation_owner_reuse();

CREATE FUNCTION public.retire_vm_thread_creation_owner()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE evidence jsonb;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM public.vm_thread_creation_owners WHERE thread_id=OLD.id) THEN
        RETURN OLD;
    END IF;
    evidence := public.vm_thread_creation_delete_evidence(OLD);
    IF evidence IS NULL THEN
        RAISE EXCEPTION 'VM audit owner deletion lacks settled failed-initial source evidence' USING ERRCODE='23514';
    END IF;
    UPDATE public.vm_thread_creation_owners SET
        deleted_runtime_generation=OLD.runtime_generation,
        deleted_retirement_token=OLD.runtime_retirement_token,
        deletion_receipt=evidence,deleted_at=transaction_timestamp()
    WHERE thread_id=OLD.id AND live_thread_id=OLD.id AND deleted_at IS NULL;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'VM audit owner retirement lost exact identity' USING ERRCODE='23514';
    END IF;
    RETURN OLD;
END;
$$;
CREATE TRIGGER thread_vm_creation_audit_owner_retirement
BEFORE DELETE ON public.threads
FOR EACH ROW EXECUTE FUNCTION public.retire_vm_thread_creation_owner();

COMMIT;
