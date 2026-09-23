-- migration: 0278_vm_resource_thread_runtime.sql
-- description: Genuine pinned-thread VM creation identity alongside immutable Job retries.
-- depends-on: 0277_vm_idle_pinned_session.sql
-- transactional: yes
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE public.vm_creation_retries
    ADD COLUMN owner_kind text NOT NULL DEFAULT 'job',
    ADD COLUMN thread_id uuid REFERENCES public.threads(id),
    ADD COLUMN thread_runtime_generation uuid,
    ADD COLUMN thread_agent_id uuid REFERENCES public.agents(id),
    ADD COLUMN thread_attach_token uuid,
    ADD COLUMN thread_wake_operation_id uuid REFERENCES public.vm_idle_operations(id),
    ADD COLUMN thread_owner_user_id uuid,
    ADD COLUMN thread_owner_project_id uuid,
    ALTER COLUMN job_id DROP NOT NULL,
    ALTER COLUMN execution_id DROP NOT NULL,
    ALTER COLUMN execution_revision DROP NOT NULL,
    ALTER COLUMN execution_generation DROP NOT NULL,
    ADD CONSTRAINT vm_creation_retry_exact_owner CHECK (
        (owner_kind='job' AND job_id IS NOT NULL AND thread_id IS NULL
         AND thread_runtime_generation IS NULL AND thread_agent_id IS NULL
         AND thread_attach_token IS NULL AND thread_wake_operation_id IS NULL
         AND thread_owner_user_id IS NULL AND thread_owner_project_id IS NULL
         AND execution_id IS NOT NULL AND execution_revision IS NOT NULL
         AND execution_generation IS NOT NULL)
        OR
        (owner_kind='thread' AND job_id IS NULL AND thread_id IS NOT NULL
         AND thread_runtime_generation IS NOT NULL
         AND ((thread_agent_id IS NULL)=(thread_attach_token IS NULL))
         AND execution_id IS NULL AND execution_revision IS NULL
         AND execution_generation IS NULL AND admission_deadline IS NULL
         AND predecessor_cleanup_admission_id IS NULL
         AND controller_configuration IS NOT NULL)
    );

CREATE UNIQUE INDEX vm_creation_thread_generation
    ON public.vm_creation_retries(thread_id,provision_generation)
    WHERE owner_kind='thread';

CREATE FUNCTION public.guard_vm_creation_thread_source() RETURNS trigger
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
    IF NEW.canonical_request->>'entity_type'<>'thread'
       OR NEW.canonical_request->>'job_id'<>NEW.thread_id::text
       OR NEW.canonical_request->>'provision_generation'<>NEW.provision_generation::text
       OR NEW.controller_configuration->'version'<>'3'::jsonb
       OR NEW.request_id IS DISTINCT FROM COALESCE(
           (SELECT wake_request_id FROM public.vm_idle_operations
             WHERE id=NEW.thread_wake_operation_id),NEW.request_id) THEN
        RAISE EXCEPTION 'VM thread creation request identity mismatch' USING ERRCODE='23514';
    END IF;
    SELECT * INTO current_thread FROM public.threads
      WHERE id=NEW.thread_id FOR SHARE;
    current_vm := current_thread.metadata->'vm';
    IF NOT FOUND OR current_thread.execution_lane<>'pinned'
       OR current_thread.runtime_generation<>NEW.thread_runtime_generation
       OR current_thread.runtime_retirement_token IS NOT NULL
       OR current_thread.pinned_idle_terminal_intent_at IS NOT NULL
       OR current_thread.agent_id IS DISTINCT FROM NEW.thread_agent_id
       OR current_thread.runtime_attach_token IS DISTINCT FROM NEW.thread_attach_token
       OR current_vm->>'provision_generation'<>NEW.provision_generation::text
       OR current_vm->>'status'<>'provisioning' THEN
        RAISE EXCEPTION 'VM thread creation owner changed' USING ERRCODE='23514';
    END IF;
    IF NEW.thread_agent_id IS NULL THEN
        IF EXISTS(SELECT 1 FROM public.agents WHERE thread_id=NEW.thread_id) THEN
            RAISE EXCEPTION 'VM thread creation agent mismatch' USING ERRCODE='23514';
        END IF;
    ELSIF NOT EXISTS(SELECT 1 FROM public.agents
        WHERE id=NEW.thread_agent_id AND thread_id=NEW.thread_id) THEN
        RAISE EXCEPTION 'VM thread creation agent mismatch' USING ERRCODE='23514';
    END IF;
    IF NEW.thread_wake_operation_id IS NOT NULL THEN
        SELECT * INTO current_wake FROM public.vm_idle_operations
          WHERE id=NEW.thread_wake_operation_id FOR SHARE;
        IF NOT FOUND OR current_wake.owner_kind<>'thread'
           OR current_wake.owner_id<>NEW.thread_id
           OR current_wake.release_kind<>'pinned_thread'
           OR current_wake.phase NOT IN ('waking','wake_held')
           OR current_wake.closed_at IS NOT NULL
           OR current_wake.stop_verified_at IS NULL
           OR current_wake.thread_terminal_intent_at IS NOT NULL
           OR current_wake.wake_request_id<>NEW.request_id
           OR current_wake.wake_generation<>NEW.provision_generation
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
CREATE TRIGGER guard_vm_creation_thread_source
BEFORE INSERT OR UPDATE ON public.vm_creation_retries
FOR EACH ROW EXECUTE FUNCTION public.guard_vm_creation_thread_source();

ALTER TABLE public.vm_resource_waiters
    ADD COLUMN owner_kind text NOT NULL DEFAULT 'job',
    ADD COLUMN thread_id uuid REFERENCES public.threads(id),
    ALTER COLUMN job_id DROP NOT NULL,
    ADD CONSTRAINT vm_resource_waiter_exact_owner CHECK (
        (owner_kind='job' AND job_id IS NOT NULL AND thread_id IS NULL)
        OR (owner_kind='thread' AND job_id IS NULL AND thread_id IS NOT NULL)
    );

CREATE OR REPLACE FUNCTION public.guard_vm_resource_waiter() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE source public.vm_creation_retries%ROWTYPE;
BEGIN
    IF TG_OP='INSERT' THEN
        SELECT * INTO source FROM public.vm_creation_retries r
          WHERE r.request_id=NEW.request_id;
        IF NOT FOUND OR source.owner_kind<>NEW.owner_kind
           OR source.job_id IS DISTINCT FROM NEW.job_id
           OR source.thread_id IS DISTINCT FROM NEW.thread_id
           OR source.provision_generation<>NEW.provision_generation
           OR source.request_digest<>NEW.request_digest THEN
            RAISE EXCEPTION 'VM resource waiter source identity mismatch' USING ERRCODE='23503';
        END IF;
        IF NEW.owner_kind='thread' AND (
            NEW.owner_key IS DISTINCT FROM CASE
                WHEN source.thread_owner_user_id IS NULL THEN 'system'
                ELSE 'user:'||source.thread_owner_user_id::text END
            OR NEW.project_id IS DISTINCT FROM source.thread_owner_project_id
            OR NEW.priority<>0
        ) THEN
            RAISE EXCEPTION 'VM resource thread waiter owner mismatch' USING ERRCODE='23514';
        END IF;
        IF NEW.state<>'waiting' OR NEW.bypasses<>0
           OR NEW.protected_order IS NOT NULL THEN
            RAISE EXCEPTION 'VM resource waiter must start waiting' USING ERRCODE='23514';
        END IF;
        RETURN NEW;
    END IF;
    IF ROW(NEW.request_id,NEW.owner_kind,NEW.job_id,NEW.thread_id,
           NEW.provision_generation,NEW.cluster_id,NEW.policy_digest,
           NEW.owner_key,NEW.project_id,NEW.priority,NEW.request_digest,
           NEW.guest_vcpus,NEW.guest_memory_bytes,NEW.cpu_millicores,
           NEW.memory_bytes,NEW.kvm_devices,NEW.placement,NEW.enqueued_at)
       IS DISTINCT FROM
       ROW(OLD.request_id,OLD.owner_kind,OLD.job_id,OLD.thread_id,
           OLD.provision_generation,OLD.cluster_id,OLD.policy_digest,
           OLD.owner_key,OLD.project_id,OLD.priority,OLD.request_digest,
           OLD.guest_vcpus,OLD.guest_memory_bytes,OLD.cpu_millicores,
           OLD.memory_bytes,OLD.kvm_devices,OLD.placement,OLD.enqueued_at)
       OR NEW.revision < OLD.revision OR NEW.bypasses < OLD.bypasses
       OR (OLD.protected_order IS NOT NULL
           AND NEW.protected_order IS DISTINCT FROM OLD.protected_order) THEN
        RAISE EXCEPTION 'VM resource waiter identity is immutable' USING ERRCODE='23514';
    END IF;
    IF NEW.state <> OLD.state AND NOT (
        (OLD.state='waiting' AND NEW.state IN ('nonfit','admitted','cancelled','parked')) OR
        (OLD.state='nonfit' AND NEW.state IN ('waiting','cancelled','parked')) OR
        (OLD.state='parked' AND NEW.state IN ('waiting','cancelled')) OR
        (OLD.state='admitted' AND NEW.state='released')
    ) THEN
        RAISE EXCEPTION 'Invalid VM resource waiter transition' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$$;

COMMIT;
