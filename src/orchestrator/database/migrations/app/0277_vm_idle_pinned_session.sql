-- migration: 0277_vm_idle_pinned_session.sql
-- description: Immutable pinned thread idle source and retirement-token link.
-- depends-on: 0276_vm_resource_job_runtime.sql
-- transactional: yes
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

-- The operation owns the immutable End intent. Mirror its timestamp on the
-- thread so an already-running prepare/Pod writer sees the fence on every
-- current-runtime reread, even before permanent retirement can begin.
ALTER TABLE public.threads
    ADD COLUMN pinned_idle_terminal_intent_at timestamptz;

CREATE FUNCTION public.guard_pinned_idle_terminal_thread() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.pinned_idle_terminal_intent_at IS NOT NULL AND
       NEW.pinned_idle_terminal_intent_at
           IS DISTINCT FROM OLD.pinned_idle_terminal_intent_at THEN
        RAISE EXCEPTION 'Pinned idle End intent is immutable' USING ERRCODE='23514';
    END IF;
    IF OLD.pinned_idle_terminal_intent_at IS NULL AND
       NEW.pinned_idle_terminal_intent_at IS NOT NULL AND NOT EXISTS (
           SELECT 1 FROM public.vm_idle_operations o
           WHERE o.owner_kind='thread' AND o.owner_id=NEW.id
             AND o.release_kind='pinned_thread' AND o.closed_at IS NULL
             AND o.thread_terminal_intent_at=NEW.pinned_idle_terminal_intent_at
             AND o.thread_terminal_intent_generation=NEW.runtime_generation
       ) THEN
        RAISE EXCEPTION 'Pinned idle End intent lacks operation' USING ERRCODE='23514';
    END IF;
    IF NEW.pinned_idle_terminal_intent_at IS NOT NULL AND (
        (NEW.agent_id IS NOT NULL AND NEW.agent_id IS DISTINCT FROM OLD.agent_id)
        OR (NEW.runtime_attach_token IS NOT NULL AND
            NEW.runtime_attach_token IS DISTINCT FROM OLD.runtime_attach_token)
        OR (OLD.pinned_idle_terminal_intent_at IS NOT NULL AND
            NEW.status NOT IN (OLD.status,'suspended','ended'))
    ) THEN
        RAISE EXCEPTION 'Pinned idle End fences new execution' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;

CREATE TRIGGER guard_pinned_idle_terminal_thread_update
BEFORE UPDATE ON public.threads FOR EACH ROW
EXECUTE FUNCTION public.guard_pinned_idle_terminal_thread();

ALTER TABLE public.vm_idle_operations
    ADD COLUMN thread_runtime_generation uuid,
    ADD COLUMN thread_retirement_token uuid,
    ADD COLUMN thread_agent_pod_identity jsonb,
    ADD COLUMN thread_agent_stop_evidence jsonb,
    ADD COLUMN thread_agent_stop_verified_at timestamptz,
    ADD COLUMN thread_wake_ready_identity jsonb,
    ADD COLUMN thread_terminal_intent_at timestamptz,
    ADD COLUMN thread_terminal_intent_generation uuid,
    DROP CONSTRAINT vm_idle_operations_release_kind_check,
    ADD CONSTRAINT vm_idle_operations_release_kind_check CHECK
        (release_kind IN ('stateless','pinned_job','pinned_thread')),
    DROP CONSTRAINT vm_idle_pinned_shape,
    ADD CONSTRAINT vm_idle_pinned_shape CHECK (
        (release_kind IN ('stateless','pinned_thread')
            AND pinned_delivery_id IS NULL AND pinned_wait_receipt_id IS NULL
            AND pinned_agent_id IS NULL AND pinned_process_generation IS NULL
            AND pinned_agent_pod_name IS NULL AND pinned_agent_pod_namespace IS NULL
            AND pinned_agent_pod_uid IS NULL
            AND pinned_original_dispatch_marker IS NULL
            AND pinned_lease_observed_at IS NULL AND pinned_lease_expires_at IS NULL
            AND pinned_terminal_observed_at IS NULL
            AND pinned_stop_evidence IS NULL AND pinned_stop_verified_at IS NULL)
        OR (release_kind='pinned_job' AND owner_kind='job'
            AND pinned_delivery_id IS NOT NULL AND pinned_wait_receipt_id IS NOT NULL
            AND pinned_agent_id IS NOT NULL AND pinned_process_generation IS NOT NULL
            AND pinned_agent_pod_name IS NOT NULL AND pinned_agent_pod_namespace IS NOT NULL
            AND pinned_agent_pod_uid IS NOT NULL
            AND pinned_original_dispatch_marker IS NOT NULL
            AND pinned_lease_observed_at IS NOT NULL AND pinned_lease_expires_at IS NOT NULL
            AND (pinned_stop_verified_at IS NULL OR pinned_terminal_observed_at IS NOT NULL)
            AND ((pinned_stop_evidence IS NULL)=(pinned_stop_verified_at IS NULL)))
    ),
    ADD CONSTRAINT vm_idle_thread_source_shape CHECK (
        (release_kind='pinned_thread' AND owner_kind='thread'
            AND retained_kind='rootdisk'
            AND thread_runtime_generation IS NOT NULL
            AND thread_retirement_token IS NOT NULL
            AND thread_agent_pod_identity IS NOT NULL
            AND ((thread_agent_stop_evidence IS NULL)=
                 (thread_agent_stop_verified_at IS NULL))
            AND ((thread_terminal_intent_at IS NULL)=
                 (thread_terminal_intent_generation IS NULL)))
        OR (release_kind<>'pinned_thread'
            AND thread_runtime_generation IS NULL
            AND thread_retirement_token IS NULL
            AND thread_agent_pod_identity IS NULL
            AND thread_agent_stop_evidence IS NULL
            AND thread_agent_stop_verified_at IS NULL
            AND thread_wake_ready_identity IS NULL
            AND thread_terminal_intent_at IS NULL
            AND thread_terminal_intent_generation IS NULL)
    );

CREATE FUNCTION public.guard_vm_idle_pinned_thread() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    source_thread public.threads%ROWTYPE;
    source_context jsonb;
    source_vm jsonb;
BEGIN
    IF TG_OP='DELETE' THEN
        IF OLD.release_kind='pinned_thread' THEN
            RAISE EXCEPTION 'Pinned thread idle operation is retained' USING ERRCODE='23514';
        END IF;
        RETURN OLD;
    END IF;
    IF TG_OP='UPDATE' THEN
        IF OLD.release_kind='pinned_thread' AND (
            ROW(NEW.release_kind,NEW.owner_kind,NEW.owner_id,NEW.episode_id,
                NEW.episode_revision,NEW.provision_generation,NEW.vm_uid,
                NEW.vmi_uid,NEW.launcher_uid,NEW.pvc_uid,NEW.retained_kind,
                NEW.thread_runtime_generation,NEW.thread_retirement_token)
            IS DISTINCT FROM
            ROW(OLD.release_kind,OLD.owner_kind,OLD.owner_id,OLD.episode_id,
                OLD.episode_revision,OLD.provision_generation,OLD.vm_uid,
                OLD.vmi_uid,OLD.launcher_uid,OLD.pvc_uid,OLD.retained_kind,
                OLD.thread_runtime_generation,OLD.thread_retirement_token)
            OR NEW.thread_agent_pod_identity IS DISTINCT FROM OLD.thread_agent_pod_identity
            OR (OLD.stop_verified_at IS NOT NULL AND
                ROW(NEW.stop_evidence,NEW.stop_verified_at)
                IS DISTINCT FROM ROW(OLD.stop_evidence,OLD.stop_verified_at))
            OR (OLD.thread_agent_stop_verified_at IS NOT NULL AND
                ROW(NEW.thread_agent_stop_evidence,NEW.thread_agent_stop_verified_at)
                IS DISTINCT FROM ROW(OLD.thread_agent_stop_evidence,
                                     OLD.thread_agent_stop_verified_at))
            OR (OLD.thread_terminal_intent_at IS NOT NULL AND
                ROW(NEW.thread_terminal_intent_at,
                    NEW.thread_terminal_intent_generation,
                    NEW.wake_requested,NEW.wake_execution_requested,
                    NEW.wake_id,NEW.wake_generation,NEW.wake_request_id,
                    NEW.wake_ready_at,NEW.thread_wake_ready_identity)
                IS DISTINCT FROM ROW(OLD.thread_terminal_intent_at,
                                     OLD.thread_terminal_intent_generation,
                                     OLD.wake_requested,OLD.wake_execution_requested,
                                     OLD.wake_id,OLD.wake_generation,OLD.wake_request_id,
                                     OLD.wake_ready_at,OLD.thread_wake_ready_identity))
            OR (OLD.thread_terminal_intent_at IS NOT NULL
                AND NEW.phase='ready')
            OR (OLD.thread_wake_ready_identity IS NOT NULL AND
                NEW.thread_wake_ready_identity IS DISTINCT FROM OLD.thread_wake_ready_identity)
            OR (OLD.closed_at IS NOT NULL AND
                ROW(NEW.phase,NEW.closed_at) IS DISTINCT FROM
                ROW(OLD.phase,OLD.closed_at))
        ) THEN
            RAISE EXCEPTION 'Pinned thread idle authority is immutable' USING ERRCODE='23514';
        END IF;
        IF OLD.release_kind='pinned_thread'
           AND OLD.thread_terminal_intent_at IS NULL
           AND NEW.thread_terminal_intent_at IS NOT NULL THEN
            SELECT * INTO source_thread FROM public.threads
                WHERE id=NEW.owner_id FOR SHARE;
            source_vm := source_thread.metadata->'vm';
            IF source_thread.id IS NULL
               OR source_thread.runtime_generation
                  IS DISTINCT FROM NEW.thread_terminal_intent_generation
               OR NOT COALESCE((
                   (OLD.phase IN ('releasing','release_held')
                    AND source_thread.status='awaiting_user'
                    AND source_thread.runtime_retirement_token=OLD.thread_retirement_token)
                   OR (OLD.phase IN ('suspended','waking','wake_held')
                       AND source_thread.status IN ('suspended','created')
                       AND OLD.stop_verified_at IS NOT NULL
                       AND OLD.thread_agent_stop_verified_at IS NOT NULL
                       AND source_thread.runtime_retirement_token IS NULL
                       AND (
                           (source_vm->>'provision_generation'=OLD.provision_generation::text
                            AND source_vm->>'rootdisk_pvc_uid'=OLD.pvc_uid::text)
                           OR (OLD.wake_generation IS NOT NULL
                               AND source_vm->>'provision_generation'=OLD.wake_generation::text
                               AND source_vm->>'idle_wake_operation_id'=OLD.id::text
                               AND source_vm->>'idle_predecessor_pvc_uid'=OLD.pvc_uid::text)
                       ))
               ),false)
            THEN
                RAISE EXCEPTION 'Pinned thread terminal join lost wake race' USING ERRCODE='23514';
            END IF;
        END IF;
        IF OLD.release_kind='pinned_thread'
           AND OLD.thread_agent_stop_verified_at IS NULL
           AND NEW.thread_agent_stop_verified_at IS NOT NULL THEN
            SELECT * INTO source_thread FROM public.threads
                WHERE id=NEW.owner_id FOR SHARE;
            IF source_thread.id IS NULL OR source_thread.status<>'suspended'
               OR source_thread.runtime_generation=NEW.thread_runtime_generation
               OR source_thread.agent_id IS NOT NULL
               OR source_thread.runtime_attach_token IS NOT NULL
               OR NOT EXISTS (
                   SELECT 1 FROM public.thread_runtime_retirement_outcomes o
                   WHERE o.thread_id=NEW.owner_id
                     AND o.runtime_generation=NEW.thread_runtime_generation
                     AND o.retirement_token=NEW.thread_retirement_token
                     AND o.disposition='suspended' AND o.permanent=false
                     AND o.outcome='settled'
               )
               OR NEW.thread_agent_stop_evidence IS DISTINCT FROM jsonb_build_object(
                   'version',1,'pod',NEW.thread_agent_pod_identity,
                   'disposition',NEW.thread_agent_stop_evidence->>'disposition',
                   'retirement_token',NEW.thread_retirement_token::text,
                   'controller_authenticated',true)
               OR NEW.thread_agent_stop_evidence->>'disposition' IS NULL
               OR NEW.thread_agent_stop_evidence->>'disposition'
                  NOT IN ('exact_absent','replacement')
            THEN
                RAISE EXCEPTION 'Pinned thread agent stop is unproven' USING ERRCODE='23514';
            END IF;
        END IF;
        IF OLD.release_kind='pinned_thread'
           AND OLD.thread_wake_ready_identity IS NULL
           AND NEW.thread_wake_ready_identity IS NOT NULL THEN
            SELECT * INTO source_thread FROM public.threads
                WHERE id=NEW.owner_id FOR SHARE;
            source_vm := source_thread.metadata->'vm';
            IF NEW.wake_ready_at IS NULL OR NEW.stop_verified_at IS NULL
               OR NEW.wake_generation IS NULL OR NEW.wake_request_id IS NULL
               OR source_thread.status<>'suspended'
               OR source_vm->>'status' IS DISTINCT FROM 'ready'
               OR source_vm->>'identity_authenticated' IS DISTINCT FROM 'true'
               OR source_vm->>'identity_provision_generation'
                  IS DISTINCT FROM NEW.wake_generation::text
               OR source_vm->>'idle_wake_operation_id' IS DISTINCT FROM NEW.id::text
               OR source_vm->>'idle_wake_request_id'
                  IS DISTINCT FROM NEW.wake_request_id::text
               OR source_vm->>'idle_predecessor_pvc_uid'
                  IS DISTINCT FROM NEW.pvc_uid::text
               OR NEW.thread_wake_ready_identity IS DISTINCT FROM jsonb_build_object(
                    'generation',NEW.wake_generation::text,
                    'vm_uid',source_vm->>'vm_uid',
                    'vmi_uid',source_vm->>'vmi_uid',
                    'launcher_uid',source_vm->>'active_pod_uid',
                    'pvc_uid',NEW.pvc_uid::text)
               OR source_vm->>'vm_uid' IS NULL
               OR source_vm->>'vmi_uid' IS NULL
               OR source_vm->>'active_pod_uid' IS NULL
               OR source_vm->>'rootdisk_pvc_uid' IS DISTINCT FROM NEW.pvc_uid::text
            THEN
                RAISE EXCEPTION 'Pinned thread wake identity is unproven' USING ERRCODE='23514';
            END IF;
        END IF;
        RETURN NEW;
    END IF;
    IF NEW.release_kind<>'pinned_thread' THEN
        RETURN NEW;
    END IF;
    SELECT * INTO source_thread FROM public.threads
        WHERE id=NEW.owner_id FOR SHARE;
    source_context := source_thread.runtime_retirement_context;
    source_vm := source_context->'vm';
    IF source_thread.id IS NULL
       OR source_thread.execution_lane<>'pinned'
       OR source_thread.status<>'awaiting_user'
       OR source_thread.runtime_generation IS DISTINCT FROM NEW.thread_runtime_generation
       OR source_thread.runtime_retirement_token IS DISTINCT FROM NEW.thread_retirement_token
       OR source_thread.runtime_retirement_authorized_at IS NULL
       OR source_thread.runtime_retirement_permanent IS DISTINCT FROM false
       OR source_context->>'generation' IS DISTINCT FROM NEW.thread_runtime_generation::text
       OR source_context->>'settle_status' IS DISTINCT FROM 'suspended'
       OR source_context->>'initiator' IS DISTINCT FROM 'system'
       OR source_context->>'agent_id' IS DISTINCT FROM source_thread.agent_id::text
       OR source_context->>'runtime_attach_token'
          IS DISTINCT FROM source_thread.runtime_attach_token::text
       OR source_context->'agent_pod'->>'pod_uid' IS DISTINCT FROM
          source_thread.metadata->'agent_pod'->>'pod_uid'
       OR source_context->'agent_pod' IS DISTINCT FROM NEW.thread_agent_pod_identity
       OR NEW.thread_agent_pod_identity->>'pod_name' IS NULL
       OR NEW.thread_agent_pod_identity->>'pod_uid' IS NULL
       OR NEW.thread_agent_pod_identity->>'namespace' IS NULL
       OR NEW.thread_agent_pod_identity->>'protection_protocol'
          IS DISTINCT FROM 'finalizer_v1'
       OR source_vm->>'provision_generation' IS DISTINCT FROM NEW.provision_generation::text
       OR source_vm->>'vm_uid' IS DISTINCT FROM NEW.vm_uid::text
       OR source_vm->>'vmi_uid' IS DISTINCT FROM NEW.vmi_uid::text
       OR source_vm->>'active_pod_uid' IS DISTINCT FROM NEW.launcher_uid::text
       OR source_vm->>'rootdisk_pvc_uid' IS DISTINCT FROM NEW.pvc_uid::text
       OR source_vm->>'status' IS DISTINCT FROM 'ready'
       OR source_thread.workspace_idle_revision IS DISTINCT FROM NEW.episode_revision
       OR source_thread.workspace_idle_episode->>'episode_id'
          IS DISTINCT FROM NEW.episode_id::text
       OR source_thread.workspace_idle_episode->>'wait_kind'
          IS DISTINCT FROM 'natural_pause'
       OR source_thread.workspace_idle_episode->'runtime_identity'->>'runtime_generation'
          IS DISTINCT FROM NEW.provision_generation::text
       OR source_thread.workspace_idle_episode->'runtime_identity'->>'runtime_uid'
          IS DISTINCT FROM NEW.vm_uid::text
    THEN
        RAISE EXCEPTION 'Pinned thread idle source is unproven' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;

CREATE TRIGGER vm_idle_pinned_thread_guard
    BEFORE INSERT OR UPDATE OR DELETE ON public.vm_idle_operations
    FOR EACH ROW EXECUTE FUNCTION public.guard_vm_idle_pinned_thread();

-- A closed access-only wake may later receive explicit Resume/input. Keep its
-- immutable Ready operation closed; this row is the replayable execution
-- continuation, keyed to that exact successor rather than a second VM create.
CREATE TABLE public.vm_idle_thread_access_continuations (
    operation_id uuid PRIMARY KEY REFERENCES public.vm_idle_operations(id),
    thread_id uuid NOT NULL,
    requested_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    prepared_at timestamptz
);
CREATE UNIQUE INDEX vm_idle_thread_access_one_pending
    ON public.vm_idle_thread_access_continuations(thread_id)
    WHERE prepared_at IS NULL;

CREATE FUNCTION public.guard_vm_idle_thread_access_continuation()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    source_operation public.vm_idle_operations%ROWTYPE;
    source_thread public.threads%ROWTYPE;
    current_agent public.agents%ROWTYPE;
    source_vm jsonb;
    source_pod jsonb;
BEGIN
    IF TG_OP='DELETE' THEN
        RAISE EXCEPTION 'Pinned access execution receipt is retained' USING ERRCODE='23514';
    END IF;
    IF TG_OP='UPDATE' AND (
        NEW.operation_id IS DISTINCT FROM OLD.operation_id OR
        NEW.thread_id IS DISTINCT FROM OLD.thread_id OR
        NEW.requested_at IS DISTINCT FROM OLD.requested_at OR
        OLD.prepared_at IS NOT NULL OR NEW.prepared_at IS NULL
    ) THEN
        RAISE EXCEPTION 'Pinned access execution source is immutable' USING ERRCODE='23514';
    END IF;
    SELECT * INTO source_operation FROM public.vm_idle_operations
        WHERE id=NEW.operation_id FOR SHARE;
    SELECT * INTO source_thread FROM public.threads
        WHERE id=NEW.thread_id FOR SHARE;
    source_vm := source_thread.metadata->'vm';
    IF source_operation.id IS NULL OR source_thread.id IS NULL
       OR source_operation.owner_kind<>'thread'
       OR source_operation.owner_id IS DISTINCT FROM NEW.thread_id
       OR source_operation.release_kind<>'pinned_thread'
       OR source_operation.phase<>'ready' OR source_operation.closed_at IS NULL
       OR source_operation.wake_ready_at IS NULL
       OR source_operation.wake_execution_requested
       OR source_operation.thread_terminal_intent_at IS NOT NULL
       OR source_thread.status<>'created'
       OR source_thread.runtime_retirement_token IS NOT NULL
       OR source_thread.pinned_idle_terminal_intent_at IS NOT NULL
       OR source_vm->>'status' IS DISTINCT FROM 'ready'
       OR source_vm->>'identity_authenticated' IS DISTINCT FROM 'true'
       OR source_vm->>'provision_generation' IS DISTINCT FROM
          source_operation.wake_generation::text
       OR source_vm->>'idle_wake_operation_id' IS DISTINCT FROM
          source_operation.id::text
       OR source_vm->>'rootdisk_pvc_uid' IS DISTINCT FROM
          source_operation.pvc_uid::text
       OR source_operation.thread_wake_ready_identity IS DISTINCT FROM
          jsonb_build_object(
              'generation',source_operation.wake_generation::text,
              'vm_uid',source_vm->>'vm_uid','vmi_uid',source_vm->>'vmi_uid',
              'launcher_uid',source_vm->>'active_pod_uid',
              'pvc_uid',source_operation.pvc_uid::text)
    THEN
        RAISE EXCEPTION 'Pinned access execution successor is unproven' USING ERRCODE='23514';
    END IF;
    IF TG_OP='INSERT' AND (
        NEW.prepared_at IS NOT NULL OR source_thread.agent_id IS NOT NULL OR
        source_thread.runtime_attach_token IS NOT NULL
    ) THEN
        RAISE EXCEPTION 'Pinned access execution was already bound' USING ERRCODE='23514';
    END IF;
    IF TG_OP='UPDATE' THEN
        SELECT * INTO current_agent FROM public.agents
            WHERE id=source_thread.agent_id FOR SHARE;
        source_pod := source_thread.metadata->'agent_pod';
        IF source_thread.runtime_attach_token IS NULL
           OR current_agent.id IS NULL
           OR current_agent.thread_id IS DISTINCT FROM source_thread.id
           OR current_agent.status NOT IN ('ready','working','session')
           OR source_pod->>'pod_name' IS DISTINCT FROM current_agent.hostname
           OR source_pod->>'pod_uid' IS DISTINCT FROM current_agent.pod_uid
           OR source_pod->>'protection_protocol' IS DISTINCT FROM 'finalizer_v1'
           OR source_pod->>'pod_uid' IS NULL
           OR source_pod->>'pod_uid' IS NOT DISTINCT FROM
              source_operation.thread_agent_pod_identity->>'pod_uid'
        THEN
            RAISE EXCEPTION 'Pinned access execution fresh binding is unproven'
                USING ERRCODE='23514';
        END IF;
    END IF;
    RETURN NEW;
END $$;

CREATE TRIGGER vm_idle_thread_access_continuation_guard
BEFORE INSERT OR UPDATE OR DELETE ON public.vm_idle_thread_access_continuations
FOR EACH ROW EXECUTE FUNCTION public.guard_vm_idle_thread_access_continuation();

COMMIT;
