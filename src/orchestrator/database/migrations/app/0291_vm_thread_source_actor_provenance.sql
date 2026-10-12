-- migration: 0291_vm_thread_source_actor_provenance.sql
-- description: Preserve immutable VM source actor provenance after operational agent cleanup.
-- depends-on: 0290_validate_vm_thread_creation_audit_owners.sql
-- transactional: yes
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

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
               NEW.thread_wake_operation_id,NEW.thread_owner_user_id,
               NEW.thread_owner_project_id)
           IS DISTINCT FROM
           ROW(OLD.owner_kind,OLD.thread_id,OLD.thread_runtime_generation,
               OLD.thread_agent_id,OLD.thread_attach_token,
               OLD.thread_wake_operation_id,OLD.thread_owner_user_id,
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
    IF NEW.thread_wake_operation_id IS NOT NULL THEN
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
    ELSIF NEW.expected_pvc_uid IS NOT NULL THEN
        RAISE EXCEPTION 'VM thread retained disk requires wake source' USING ERRCODE='23514';
    END IF;
    NEW.thread_owner_user_id := current_thread.user_id;
    NEW.thread_owner_project_id := current_thread.project_id;
    RETURN NEW;
END;
$$;

-- Like 0171 Officer grant provenance, this UUID outlives an operational agent.
-- Current binding/deletion authority and all source immutability remain intact.
ALTER TABLE public.vm_creation_retries
    DROP CONSTRAINT vm_creation_retries_thread_agent_id_fkey;
COMMENT ON COLUMN public.vm_creation_retries.thread_agent_id IS
    'Immutable original pinned-thread actor UUID provenance. New sources require '
    'an exact live reciprocal actor locked under the current thread; historical '
    'sources retain this UUID after protected detach and operational agent GC.';
COMMIT;
