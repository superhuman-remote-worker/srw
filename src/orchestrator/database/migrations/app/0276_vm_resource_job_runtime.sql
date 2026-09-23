-- migration: 0276_vm_resource_job_runtime.sql
-- description: Append-only exact same-generation recovery successor binding for one charged VM reservation.
-- depends-on: 0275_vm_idle_pinned_job.sql
-- transactional: yes
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

CREATE TABLE public.vm_resource_recovery_successors (
    recovery_id UUID PRIMARY KEY REFERENCES public.vm_workspace_recoveries(id),
    reservation_id UUID NOT NULL REFERENCES public.vm_resource_reservations(id),
    ordinal BIGINT NOT NULL CHECK (ordinal > 0),
    owner_id UUID NOT NULL,
    provision_generation UUID NOT NULL,
    vm_uid UUID NOT NULL,
    root_pvc_uid UUID NOT NULL,
    prior_vmi_uid UUID NOT NULL,
    prior_launcher_uid UUID NOT NULL,
    successor_vmi_uid UUID NOT NULL,
    successor_launcher_uid UUID NOT NULL,
    stop_receipt_digest TEXT NOT NULL CHECK (stop_receipt_digest ~ '^sha256:[0-9a-f]{64}$'),
    final_attestation_digest TEXT NOT NULL CHECK (final_attestation_digest ~ '^sha256:[0-9a-f]{64}$'),
    committed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (reservation_id, ordinal),
    UNIQUE (reservation_id, successor_vmi_uid),
    UNIQUE (reservation_id, successor_launcher_uid),
    CHECK (prior_vmi_uid <> successor_vmi_uid),
    CHECK (prior_launcher_uid <> successor_launcher_uid)
);
CREATE INDEX vm_resource_successor_current
    ON public.vm_resource_recovery_successors(reservation_id, ordinal DESC);

CREATE FUNCTION public.guard_vm_resource_recovery_successor() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    charge RECORD;
    recovery RECORD;
    prior RECORD;
    owner_vm JSONB;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'VM resource successor receipt is append-only' USING ERRCODE='23514';
    END IF;
    -- The parent lock serializes the ordinal and predecessor chain even when
    -- two recovered operations race to report the same VM generation.
    SELECT r.*, w.job_id, w.provision_generation AS waiter_generation,
           retry.observed_vm_uid, retry.observed_pvc_uid
      INTO charge
      FROM public.vm_resource_reservations r
      JOIN public.vm_resource_waiters w ON w.request_id=r.request_id
     JOIN public.vm_creation_retries retry ON retry.request_id=r.request_id
     WHERE r.id=NEW.reservation_id FOR UPDATE OF r;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'VM resource successor reservation missing' USING ERRCODE='23514';
    END IF;
    IF charge.resource_version<>2
       OR charge.state NOT IN ('active','warm')
       OR charge.vm_uid IS DISTINCT FROM NEW.vm_uid
       OR charge.job_id<>NEW.owner_id
       OR charge.waiter_generation<>NEW.provision_generation
       OR charge.observed_vm_uid IS DISTINCT FROM NEW.vm_uid
       OR charge.observed_pvc_uid IS DISTINCT FROM NEW.root_pvc_uid THEN
        RAISE EXCEPTION 'VM resource successor reservation changed' USING ERRCODE='23514';
    END IF;
    SELECT * INTO recovery FROM public.vm_workspace_recoveries
     WHERE id=NEW.recovery_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'VM resource successor recovery missing' USING ERRCODE='23514';
    END IF;
    IF recovery.owner_kind<>'job'
       OR recovery.owner_id<>NEW.owner_id
       OR recovery.provision_generation<>NEW.provision_generation
       OR recovery.vm_uid<>NEW.vm_uid
       OR recovery.root_pvc_uid<>NEW.root_pvc_uid
       OR recovery.prior_vmi_uid IS DISTINCT FROM NEW.prior_vmi_uid
       OR recovery.prior_launcher_uid IS DISTINCT FROM NEW.prior_launcher_uid
       OR recovery.phase<>'recovered' OR recovery.resolved_at IS NULL
       OR NOT EXISTS (
            SELECT 1 FROM public.vm_workspace_recovery_stop_receipts receipt
             WHERE receipt.recovery_id=NEW.recovery_id
               AND receipt.accepted_claim_token<=recovery.claim_token
               AND receipt.vm_uid=NEW.vm_uid
               AND receipt.vmi_uid=NEW.prior_vmi_uid
               AND receipt.launcher_uid=NEW.prior_launcher_uid
               AND receipt.root_pvc_uid=NEW.root_pvc_uid
               AND receipt.evidence_digest=NEW.stop_receipt_digest
       ) THEN
        RAISE EXCEPTION 'VM resource successor recovery proof changed' USING ERRCODE='23514';
    END IF;
    SELECT * INTO prior FROM public.vm_resource_recovery_successors
     WHERE reservation_id=NEW.reservation_id ORDER BY ordinal DESC LIMIT 1;
    IF prior IS NULL THEN
        IF NEW.ordinal<>1
           OR charge.vmi_uid IS DISTINCT FROM NEW.prior_vmi_uid
           OR charge.launcher_uid IS DISTINCT FROM NEW.prior_launcher_uid THEN
            RAISE EXCEPTION 'VM resource successor predecessor changed' USING ERRCODE='23514';
        END IF;
    ELSIF NEW.ordinal<>prior.ordinal+1
          OR prior.successor_vmi_uid<>NEW.prior_vmi_uid
          OR prior.successor_launcher_uid<>NEW.prior_launcher_uid THEN
        RAISE EXCEPTION 'VM resource successor predecessor changed' USING ERRCODE='23514';
    END IF;
    SELECT context->'vm' INTO owner_vm FROM public.jobs WHERE id=NEW.owner_id;
    IF owner_vm IS NULL
       OR owner_vm->>'provision_generation' IS DISTINCT FROM NEW.provision_generation::text
       OR owner_vm->>'vm_uid' IS DISTINCT FROM NEW.vm_uid::text
       OR owner_vm->>'rootdisk_pvc_uid' IS DISTINCT FROM NEW.root_pvc_uid::text
       OR owner_vm->>'vmi_uid' IS DISTINCT FROM NEW.successor_vmi_uid::text
       OR owner_vm->>'active_pod_uid' IS DISTINCT FROM NEW.successor_launcher_uid::text THEN
        RAISE EXCEPTION 'VM resource successor owner projection changed' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER guard_vm_resource_recovery_successor
    BEFORE INSERT OR UPDATE OR DELETE ON public.vm_resource_recovery_successors
    FOR EACH ROW EXECUTE FUNCTION public.guard_vm_resource_recovery_successor();

CREATE FUNCTION public.guard_vm_resource_release_v2() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    source RECORD;
    idle RECORD;
    successor RECORD;
    owner_context JSONB;
    proof JSONB;
    current_vmi UUID;
    current_launcher UUID;
BEGIN
    IF TG_OP<>'UPDATE' OR NEW.resource_version<>2 OR NEW.state<>'released'
       OR OLD.state='released' THEN
        RETURN NEW;
    END IF;
    SELECT w.job_id,w.provision_generation,retry.state AS retry_state,
           retry.observed_vm_uid,retry.observed_pvc_uid,
           retry.cancellation_disposition,retry.creation_admission_id
      INTO source
      FROM public.vm_resource_waiters w
      JOIN public.vm_creation_retries retry ON retry.request_id=w.request_id
     WHERE w.request_id=NEW.request_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'VM resource release source missing' USING ERRCODE='23514';
    END IF;
    proof := NEW.release_evidence;
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
CREATE TRIGGER guard_vm_resource_release_v2
    BEFORE UPDATE ON public.vm_resource_reservations
    FOR EACH ROW EXECUTE FUNCTION public.guard_vm_resource_release_v2();
COMMIT;
