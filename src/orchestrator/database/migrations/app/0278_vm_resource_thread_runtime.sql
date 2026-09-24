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

CREATE OR REPLACE FUNCTION public.guard_vm_resource_release_v2() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    source RECORD;
    idle RECORD;
    successor RECORD;
    owner_context JSONB;
    proof JSONB;
    current_vmi UUID;
    current_launcher UUID;
    cleanup_stop RECORD;
    thread_row RECORD;
BEGIN
    IF TG_OP<>'UPDATE' OR NEW.resource_version<>2 OR NEW.state<>'released'
       OR OLD.state='released' THEN
        RETURN NEW;
    END IF;
    SELECT w.owner_kind,w.job_id,w.thread_id,w.provision_generation,retry.state AS retry_state,
           retry.observed_vm_uid,retry.observed_pvc_uid,
           retry.cancellation_disposition,retry.creation_admission_id,
           retry.canonical_request,retry.controller_configuration
      INTO source
      FROM public.vm_resource_waiters w
      JOIN public.vm_creation_retries retry ON retry.request_id=w.request_id
     WHERE w.request_id=NEW.request_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'VM resource release source missing' USING ERRCODE='23514';
    END IF;
    proof := NEW.release_evidence;
    IF source.owner_kind='thread' THEN
        IF proof->>'kind'='never_vm_issued' THEN
            IF OLD.state<>'reserved' OR OLD.vm_uid IS NOT NULL
               OR source.retry_state<>'cancel_requested'
               OR proof->>'request_id' IS DISTINCT FROM NEW.request_id::text
               OR proof->>'thread_id' IS DISTINCT FROM source.thread_id::text
               OR proof->>'provision_generation' IS DISTINCT FROM source.provision_generation::text
               OR source.controller_configuration->>'golden_enabled' IS DISTINCT FROM 'false'
               OR source.canonical_request->'preparation' IS NOT NULL
               OR EXISTS(SELECT 1 FROM public.vm_creation_effects e
                          WHERE e.request_id=NEW.request_id
                            AND e.state<>'rejected') THEN
                RAISE EXCEPTION 'VM thread never-issued release unproven' USING ERRCODE='23514';
            END IF;
            RETURN NEW;
        END IF;
        IF proof->>'kind'<>'exact_compute_absent' OR OLD.state<>'teardown' THEN
            RAISE EXCEPTION 'VM thread physical release unproven' USING ERRCODE='23514';
        END IF;
        SELECT * INTO idle FROM public.vm_idle_operations
         WHERE id=(proof->>'operation_id')::uuid;
        SELECT id,status,metadata,runtime_generation,agent_id,runtime_attach_token
          INTO thread_row FROM public.threads WHERE id=source.thread_id;
        SELECT successor_vmi_uid,successor_launcher_uid INTO successor
          FROM public.vm_resource_recovery_successors
         WHERE reservation_id=NEW.id ORDER BY ordinal DESC LIMIT 1;
        current_vmi := COALESCE(successor.successor_vmi_uid,OLD.vmi_uid);
        current_launcher := COALESCE(successor.successor_launcher_uid,OLD.launcher_uid);
        owner_context := thread_row.metadata->'vm';
        IF idle.id IS NULL OR thread_row.id IS NULL
           OR thread_row.status<>'suspended'
           OR thread_row.runtime_generation=(SELECT thread_runtime_generation
                  FROM public.vm_creation_retries WHERE request_id=NEW.request_id)
           OR thread_row.agent_id IS NOT NULL OR thread_row.runtime_attach_token IS NOT NULL
           OR idle.owner_kind<>'thread' OR idle.release_kind<>'pinned_thread'
           OR idle.owner_id<>source.thread_id
           OR idle.provision_generation<>source.provision_generation
           OR idle.vm_uid IS DISTINCT FROM OLD.vm_uid
           OR idle.vm_uid IS DISTINCT FROM source.observed_vm_uid
           OR idle.pvc_uid IS DISTINCT FROM source.observed_pvc_uid
           OR idle.vmi_uid IS DISTINCT FROM current_vmi
           OR idle.launcher_uid IS DISTINCT FROM current_launcher
           OR idle.phase<>'suspended' OR idle.stop_verified_at IS NULL
           OR idle.thread_agent_stop_verified_at IS NULL
           OR idle.thread_agent_stop_evidence->>'version' IS DISTINCT FROM '1'
           OR idle.thread_agent_stop_evidence->>'retirement_token'
                  IS DISTINCT FROM idle.thread_retirement_token::text
           OR idle.thread_agent_stop_evidence->>'controller_authenticated'
                  IS DISTINCT FROM 'true'
           OR idle.thread_agent_stop_evidence->'pod'
                  IS DISTINCT FROM idle.thread_agent_pod_identity
           OR idle.stop_evidence->>'version' IS DISTINCT FROM '1'
           OR idle.stop_evidence->>'kind' IS DISTINCT FROM 'vm_idle_physical_stop'
           OR owner_context->>'status' IS DISTINCT FROM 'suspended'
           OR owner_context->>'rootdisk' IS DISTINCT FROM 'kept'
           OR owner_context->>'_suspend_remote_io_closed' IS DISTINCT FROM idle.id::text
           OR owner_context->>'provision_generation' IS DISTINCT FROM idle.provision_generation::text
           OR owner_context->>'vm_uid' IS DISTINCT FROM idle.vm_uid::text
           OR owner_context->>'rootdisk_pvc_uid' IS DISTINCT FROM idle.pvc_uid::text
           OR proof->>'thread_id' IS DISTINCT FROM source.thread_id::text
           OR proof->>'provision_generation' IS DISTINCT FROM source.provision_generation::text
           OR proof->>'vm_uid' IS DISTINCT FROM idle.vm_uid::text
           OR proof->>'vmi_uid' IS DISTINCT FROM idle.vmi_uid::text
           OR proof->>'launcher_uid' IS DISTINCT FROM idle.launcher_uid::text
           OR proof->>'pvc_uid' IS DISTINCT FROM idle.pvc_uid::text
           OR proof->>'stop_evidence_digest' IS DISTINCT FROM
              ('sha256:' || encode(sha256(convert_to(idle.stop_evidence::text,'UTF8')),'hex'))
           OR idle.stop_evidence->>'operation_id' IS DISTINCT FROM idle.id::text
           OR idle.stop_evidence->>'generation' IS DISTINCT FROM idle.provision_generation::text
           OR idle.stop_evidence->>'vm_uid' IS DISTINCT FROM idle.vm_uid::text
           OR idle.stop_evidence->>'vmi_uid' IS DISTINCT FROM idle.vmi_uid::text
           OR idle.stop_evidence->>'launcher_uid' IS DISTINCT FROM idle.launcher_uid::text
           OR idle.stop_evidence->>'pvc_uid' IS DISTINCT FROM idle.pvc_uid::text
           OR idle.stop_evidence->>'vm_absent' IS DISTINCT FROM 'true'
           OR idle.stop_evidence->>'vmi_absent' IS DISTINCT FROM 'true'
           OR idle.stop_evidence->>'launcher_absent' IS DISTINCT FROM 'true'
           OR idle.stop_evidence->>'retained_pvc' IS DISTINCT FROM 'true'
           OR idle.stop_evidence->>'controller_authenticated' IS DISTINCT FROM 'true'
           OR idle.stop_evidence->>'same_generation_replacement' IS DISTINCT FROM 'false'
           OR NOT EXISTS(
               SELECT 1 FROM public.managed_repository_process_zero_receipts p
                WHERE p.owner_kind='thread' AND p.owner_id=source.thread_id
                  AND p.scope='vm' AND p.provisioner='vm'
                  AND p.runtime_incarnation=source.provision_generation::text
           ) THEN
            RAISE EXCEPTION 'VM thread physical release proof changed' USING ERRCODE='23514';
        END IF;
        RETURN NEW;
    END IF;
    IF proof->>'kind'='never_vm_issued' THEN
        IF OLD.state<>'reserved' OR OLD.vm_uid IS NOT NULL
           OR source.retry_state<>'cancel_requested'
           OR proof->>'request_id' IS DISTINCT FROM NEW.request_id::text
           OR proof->>'job_id' IS DISTINCT FROM source.job_id::text
           OR proof->>'provision_generation' IS DISTINCT FROM source.provision_generation::text
           OR (
               EXISTS (SELECT 1 FROM public.vm_creation_effects e
                       WHERE e.request_id=NEW.request_id)
               AND NOT (
                   proof->>'disposition_id' IS NOT DISTINCT FROM
                       source.cancellation_disposition->>'disposition_id'
                   AND proof->>'disposition_id' IS NOT NULL
                   AND proof->>'creation_admission_id' IS NOT DISTINCT FROM
                       source.creation_admission_id::text
                   AND EXISTS (
                       SELECT 1 FROM public.vm_creation_retries r
                        WHERE r.request_id=NEW.request_id
                          AND public.valid_vm_creation_disposition_evidence(r)
                   )
                   AND EXISTS (
                       SELECT 1 FROM public.vm_workspace_cleanup_admissions c
                        WHERE c.id=source.creation_admission_id
                          AND c.completed_at IS NOT NULL
                          AND c.outcome='creation_disposed'
                   )
                   AND NOT EXISTS (
                       SELECT 1 FROM public.vm_creation_effects e
                        WHERE e.request_id=NEW.request_id
                          AND e.effect_kind='vm' AND e.state<>'rejected'
                   )
               )
           ) THEN
            RAISE EXCEPTION 'VM resource never-issued release unproven' USING ERRCODE='23514';
        END IF;
        RETURN NEW;
    END IF;
    IF proof->>'kind'='exact_cleanup_compute_absent' THEN
        SELECT * INTO cleanup_stop FROM public.vm_resource_cleanup_stop_receipts
         WHERE cleanup_admission_id=(proof->>'cleanup_admission_id')::uuid;
        IF OLD.state<>'teardown' OR cleanup_stop.cleanup_admission_id IS NULL
           OR cleanup_stop.reservation_id<>NEW.id
           OR cleanup_stop.request_id<>NEW.request_id
           OR cleanup_stop.job_id<>source.job_id
           OR cleanup_stop.provision_generation<>source.provision_generation
           OR cleanup_stop.vm_uid IS DISTINCT FROM OLD.vm_uid
           OR proof->>'job_id' IS DISTINCT FROM cleanup_stop.job_id::text
           OR proof->>'provision_generation' IS DISTINCT FROM cleanup_stop.provision_generation::text
           OR proof->>'vm_uid' IS DISTINCT FROM cleanup_stop.vm_uid::text
           OR proof->>'vmi_uid' IS DISTINCT FROM cleanup_stop.vmi_uid::text
           OR proof->>'launcher_uid' IS DISTINCT FROM cleanup_stop.launcher_uid::text
           OR proof->>'pvc_uid' IS DISTINCT FROM cleanup_stop.pvc_uid::text
           OR proof->>'stop_evidence_digest' IS DISTINCT FROM
              ('sha256:' || encode(sha256(convert_to(cleanup_stop.stop_evidence::text,'UTF8')),'hex')) THEN
            RAISE EXCEPTION 'VM resource cleanup release proof changed' USING ERRCODE='23514';
        END IF;
        RETURN NEW;
    END IF;
    IF proof->>'kind'<>'exact_compute_absent' OR OLD.state<>'teardown' THEN
        RAISE EXCEPTION 'VM resource physical release unproven' USING ERRCODE='23514';
    END IF;
    SELECT * INTO idle FROM public.vm_idle_operations
     WHERE id=(proof->>'operation_id')::uuid;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'VM resource idle release missing' USING ERRCODE='23514';
    END IF;
    SELECT successor_vmi_uid,successor_launcher_uid INTO successor
      FROM public.vm_resource_recovery_successors
     WHERE reservation_id=NEW.id ORDER BY ordinal DESC LIMIT 1;
    current_vmi := COALESCE(successor.successor_vmi_uid, OLD.vmi_uid);
    current_launcher := COALESCE(successor.successor_launcher_uid, OLD.launcher_uid);
    SELECT context->'vm' INTO owner_context FROM public.jobs WHERE id=source.job_id;
    IF idle.owner_kind<>'job' OR idle.owner_id<>source.job_id
       OR idle.provision_generation<>source.provision_generation
       OR idle.vm_uid IS DISTINCT FROM OLD.vm_uid
       OR idle.vm_uid IS DISTINCT FROM source.observed_vm_uid
       OR idle.pvc_uid IS DISTINCT FROM source.observed_pvc_uid
       OR idle.vmi_uid IS DISTINCT FROM current_vmi
       OR idle.launcher_uid IS DISTINCT FROM current_launcher
       OR idle.phase<>'suspended' OR idle.stop_verified_at IS NULL
       OR idle.stop_evidence->>'version' IS DISTINCT FROM '1'
       OR idle.stop_evidence->>'kind' IS DISTINCT FROM 'vm_idle_physical_stop'
       OR owner_context->>'status' IS DISTINCT FROM 'suspended'
       OR owner_context->>'_suspend_remote_io_closed' IS DISTINCT FROM idle.id::text
       OR owner_context->>'provision_generation' IS DISTINCT FROM idle.provision_generation::text
       OR owner_context->>'vm_uid' IS DISTINCT FROM idle.vm_uid::text
       OR owner_context->>'rootdisk_pvc_uid' IS DISTINCT FROM idle.pvc_uid::text
       OR proof->>'job_id' IS DISTINCT FROM source.job_id::text
       OR proof->>'provision_generation' IS DISTINCT FROM source.provision_generation::text
       OR proof->>'vm_uid' IS DISTINCT FROM idle.vm_uid::text
       OR proof->>'vmi_uid' IS DISTINCT FROM idle.vmi_uid::text
       OR proof->>'launcher_uid' IS DISTINCT FROM idle.launcher_uid::text
       OR proof->>'pvc_uid' IS DISTINCT FROM idle.pvc_uid::text
       OR proof->>'stop_evidence_digest' IS DISTINCT FROM
          ('sha256:' || encode(sha256(convert_to(idle.stop_evidence::text,'UTF8')),'hex'))
       OR idle.stop_evidence->>'operation_id' IS DISTINCT FROM idle.id::text
       OR idle.stop_evidence->>'generation' IS DISTINCT FROM idle.provision_generation::text
       OR idle.stop_evidence->>'vm_uid' IS DISTINCT FROM idle.vm_uid::text
       OR idle.stop_evidence->>'vmi_uid' IS DISTINCT FROM idle.vmi_uid::text
       OR idle.stop_evidence->>'launcher_uid' IS DISTINCT FROM idle.launcher_uid::text
       OR idle.stop_evidence->>'pvc_uid' IS DISTINCT FROM idle.pvc_uid::text
       OR idle.stop_evidence->>'vm_absent' IS DISTINCT FROM 'true'
       OR idle.stop_evidence->>'vmi_absent' IS DISTINCT FROM 'true'
       OR idle.stop_evidence->>'launcher_absent' IS DISTINCT FROM 'true'
       OR idle.stop_evidence->>'retained_pvc' IS DISTINCT FROM 'true'
       OR idle.stop_evidence->>'controller_authenticated' IS DISTINCT FROM 'true'
       OR idle.stop_evidence->>'same_generation_replacement' IS DISTINCT FROM 'false'
       OR NOT EXISTS(
           SELECT 1 FROM public.managed_repository_process_zero_receipts p
            WHERE p.owner_kind='job' AND p.owner_id=source.job_id
              AND p.scope='vm' AND p.provisioner='vm'
              AND p.runtime_incarnation=source.provision_generation::text
       ) THEN
        RAISE EXCEPTION 'VM resource physical release proof changed' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$$;
-- A typed thread create may be retired before any effect grant. Its settled
-- source proves that no VM process could have existed; this is deliberately
-- NOT a synthetic entry in managed_repository_process_zero_receipts.
CREATE FUNCTION public.thread_vm_creation_never_issued_source(
    requested_thread uuid, requested_generation text
)
RETURNS boolean LANGUAGE sql STABLE AS $$
    SELECT EXISTS (
        SELECT 1 FROM public.threads t
        JOIN public.vm_creation_retries r
          ON r.request_id = (t.runtime_retirement_context
                             ->'vm_creation_source'->>'request_id')::uuid
        LEFT JOIN public.vm_workspace_cleanup_admissions a
          ON a.id = r.creation_admission_id
        WHERE t.id = requested_thread
          AND t.execution_lane = 'pinned'
          AND t.runtime_retirement_token IS NOT NULL
          AND t.runtime_retirement_authorized_at IS NOT NULL
          AND t.runtime_retirement_context->'vm_creation_source'->>'request_id'
              ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          AND t.runtime_retirement_context->'vm_creation_source'
              ->>'thread_runtime_generation' = t.runtime_generation::text
          AND t.runtime_retirement_context->'vm_creation_source'
              ->>'provision_generation' = requested_generation
          AND r.owner_kind = 'thread' AND r.thread_id = t.id
          AND r.job_id IS NULL
          AND r.thread_runtime_generation = t.runtime_generation
          AND r.provision_generation::text = requested_generation
          AND r.request_digest = t.runtime_retirement_context
              ->'vm_creation_source'->>'request_digest'
          AND r.controller_configuration_digest = t.runtime_retirement_context
              ->'vm_creation_source'->>'controller_configuration_digest'
          AND r.state = 'settled' AND r.reason = 'creation_never_issued'
          AND r.observed_vm_uid IS NULL AND r.observed_pvc_uid IS NULL
          AND (a.id IS NULL OR (a.completed_at IS NOT NULL
                                AND a.outcome = 'never_issued'
                                AND a.owner_kind = 'thread'
                                AND a.owner_id = t.id))
          AND NOT EXISTS (
              SELECT 1 FROM public.vm_creation_effects e
               WHERE e.request_id = r.request_id
                 AND e.state IN ('issued','observed')
          )
          AND NOT EXISTS (
              SELECT 1 FROM public.vm_resource_reservations v
               WHERE v.request_id = r.request_id AND v.state <> 'released'
          )
    );
$$;

CREATE OR REPLACE FUNCTION public.managed_repository_process_zero_receipt_exists(
    requested_owner_kind text, requested_owner_id uuid, requested_scope text,
    requested_provisioner text, requested_runtime text
)
RETURNS boolean LANGUAGE plpgsql STABLE AS $$
BEGIN
    IF requested_runtime IS NULL
       OR (requested_scope = 'ide_local'
           AND requested_runtime !~ '^[0-9a-f]{64}$')
       OR (requested_scope <> 'ide_local'
           AND requested_runtime !~
           '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$')
    THEN
        RETURN false;
    END IF;
    IF EXISTS (
        SELECT 1 FROM public.managed_repository_process_zero_receipts receipt
         WHERE receipt.owner_kind = requested_owner_kind
           AND receipt.owner_id = requested_owner_id
           AND receipt.scope = requested_scope
           AND receipt.provisioner = requested_provisioner
           AND receipt.runtime_incarnation = requested_runtime
    ) THEN
        RETURN true;
    END IF;
    RETURN requested_owner_kind = 'thread'
       AND requested_scope = 'vm' AND requested_provisioner = 'vm'
       AND public.thread_vm_creation_never_issued_source(
           requested_owner_id, requested_runtime
       );
END;
$$;

CREATE FUNCTION public.enforce_thread_vm_creation_source_end_delete()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.runtime_retirement_context->'vm_creation_source'
       NOT IN ('null'::jsonb, '{}'::jsonb)
       AND (
           OLD.metadata ? 'vm'
           OR NOT public.thread_vm_creation_never_issued_source(
               OLD.id,
               OLD.runtime_retirement_context->'vm_creation_source'
                   ->>'provision_generation'
           )
       ) THEN
        RAISE EXCEPTION 'thread VM creation source remains unresolved'
            USING ERRCODE='23514',
                  CONSTRAINT='thread_vm_creation_source_end_delete';
    END IF;
    RETURN OLD;
END;
$$;
CREATE TRIGGER thread_vm_creation_source_end_delete
BEFORE DELETE ON public.threads
FOR EACH ROW EXECUTE FUNCTION public.enforce_thread_vm_creation_source_end_delete();

COMMIT;
