-- migration: 0333_vm_pre_ssh_positive_stop.sql
-- description: Write-once exact VM pre-SSH stop intent and proof before process zero.
-- depends-on: 0332_cancelled_unbound_workspace_retirement.sql
-- transactional: yes
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

CREATE TABLE public.vm_pre_ssh_stop_intents (
    cleanup_admission_id uuid PRIMARY KEY REFERENCES public.vm_workspace_cleanup_admissions(id),
    job_id uuid NOT NULL,
    provision_generation uuid NOT NULL,
    creation_request_id uuid NOT NULL REFERENCES public.vm_creation_retries(request_id),
    reservation_id uuid NOT NULL REFERENCES public.vm_resource_reservations(id),
    reservation_revision bigint NOT NULL CHECK (reservation_revision > 0),
    vm_uid uuid NOT NULL,
    vmi_uid uuid NOT NULL,
    launcher_uid uuid NOT NULL,
    pvc_uid uuid NOT NULL,
    node_uid uuid NOT NULL,
    cleanup_intent_digest text NOT NULL CHECK (cleanup_intent_digest ~ '^sha256:[0-9a-f]{64}$'),
    frozen jsonb NOT NULL CHECK (jsonb_typeof(frozen) = 'object'),
    frozen_digest text NOT NULL CHECK (frozen_digest ~ '^sha256:[0-9a-f]{64}$'),
    admitted_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (job_id, provision_generation),
    UNIQUE (vm_uid, launcher_uid)
);

CREATE TABLE public.vm_pre_ssh_stop_proofs (
    cleanup_admission_id uuid PRIMARY KEY REFERENCES public.vm_pre_ssh_stop_intents(cleanup_admission_id),
    job_id uuid NOT NULL,
    provision_generation uuid NOT NULL,
    frozen_digest text NOT NULL CHECK (frozen_digest ~ '^sha256:[0-9a-f]{64}$'),
    terminal_evidence jsonb NOT NULL CHECK (jsonb_typeof(terminal_evidence) = 'object'),
    evidence_digest text NOT NULL CHECK (evidence_digest ~ '^sha256:[0-9a-f]{64}$'),
    observed_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE FUNCTION public.guard_vm_pre_ssh_stop_intent() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    admission public.vm_workspace_cleanup_admissions%ROWTYPE;
    retry public.vm_creation_retries%ROWTYPE;
    reservation public.vm_resource_reservations%ROWTYPE;
    owner_vm jsonb;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'VM pre-SSH stop intent is append-only' USING ERRCODE='23514';
    END IF;
    SELECT context->'vm' INTO owner_vm FROM public.jobs
     WHERE id=NEW.job_id FOR UPDATE;
    SELECT * INTO admission FROM public.vm_workspace_cleanup_admissions
     WHERE id=NEW.cleanup_admission_id FOR UPDATE;
    SELECT * INTO retry FROM public.vm_creation_retries
     WHERE request_id=NEW.creation_request_id FOR UPDATE;
    SELECT * INTO reservation FROM public.vm_resource_reservations
     WHERE id=NEW.reservation_id FOR UPDATE;
    IF admission.id IS NULL OR admission.owner_kind IS DISTINCT FROM 'job'
       OR admission.owner_id IS DISTINCT FROM NEW.job_id
       OR admission.completed_at IS NOT NULL
       OR admission.pvc_uid IS DISTINCT FROM NEW.pvc_uid
       OR admission.intent_digest IS DISTINCT FROM NEW.cleanup_intent_digest
       OR retry.request_id IS NULL OR retry.job_id IS DISTINCT FROM NEW.job_id
       OR retry.provision_generation IS DISTINCT FROM NEW.provision_generation
       OR retry.state IS DISTINCT FROM 'succeeded'
       OR retry.observed_vm_uid IS DISTINCT FROM NEW.vm_uid
       OR retry.observed_pvc_uid IS DISTINCT FROM NEW.pvc_uid
       OR retry.ready_at IS NOT NULL
       OR reservation.id IS NULL OR reservation.request_id IS DISTINCT FROM retry.request_id
       OR reservation.revision IS DISTINCT FROM NEW.reservation_revision
       OR reservation.state IS DISTINCT FROM 'teardown'
       OR reservation.vm_uid IS DISTINCT FROM NEW.vm_uid
       OR reservation.vmi_uid IS DISTINCT FROM NEW.vmi_uid
       OR reservation.launcher_uid IS DISTINCT FROM NEW.launcher_uid
       OR reservation.node_uid IS DISTINCT FROM NEW.node_uid
       OR owner_vm->>'provision_generation' IS DISTINCT FROM NEW.provision_generation::text
       OR owner_vm->>'vm_uid' IS DISTINCT FROM NEW.vm_uid::text
       OR owner_vm->>'rootdisk_pvc_uid' IS DISTINCT FROM NEW.pvc_uid::text
       OR owner_vm->>'status' IS DISTINCT FROM 'retiring_process_zero'
       OR EXISTS (
           SELECT 1 FROM public.managed_repository_process_zero_receipts z
           WHERE z.owner_kind='job' AND z.owner_id=NEW.job_id
             AND z.scope='vm' AND z.provisioner='vm'
             AND z.runtime_incarnation=NEW.provision_generation::text)
       OR EXISTS (SELECT 1 FROM public.vm_resource_recovery_successors s
                   WHERE s.reservation_id=reservation.id)
       OR NEW.frozen->>'job_id' IS DISTINCT FROM NEW.job_id::text
       OR NEW.frozen->>'provision_generation' IS DISTINCT FROM NEW.provision_generation::text
       OR NEW.frozen->>'vm_uid' IS DISTINCT FROM NEW.vm_uid::text
       OR NEW.frozen->>'vmi_uid' IS DISTINCT FROM NEW.vmi_uid::text
       OR NEW.frozen->>'launcher_uid' IS DISTINCT FROM NEW.launcher_uid::text
       OR NEW.frozen->>'pvc_uid' IS DISTINCT FROM NEW.pvc_uid::text
       OR NEW.frozen->>'node_uid' IS DISTINCT FROM NEW.node_uid::text
       OR jsonb_typeof(NEW.frozen->'vm_generation') IS DISTINCT FROM 'number'
       OR NEW.frozen->>'vm_generation' !~ '^[1-9][0-9]*$'
       OR NEW.frozen_digest IS DISTINCT FROM
          'sha256:'||encode(sha256(convert_to(NEW.frozen::text,'UTF8')),'hex') THEN
        RAISE EXCEPTION 'VM pre-SSH stop intent lacks exact retirement authority'
            USING ERRCODE='23514', CONSTRAINT='vm_pre_ssh_stop_intent_authority';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER vm_pre_ssh_stop_intent_guard
BEFORE INSERT OR UPDATE OR DELETE ON public.vm_pre_ssh_stop_intents
FOR EACH ROW EXECUTE FUNCTION public.guard_vm_pre_ssh_stop_intent();

CREATE FUNCTION public.guard_vm_pre_ssh_stop_proof() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    intent public.vm_pre_ssh_stop_intents%ROWTYPE;
    admission public.vm_workspace_cleanup_admissions%ROWTYPE;
    retry public.vm_creation_retries%ROWTYPE;
    reservation public.vm_resource_reservations%ROWTYPE;
    owner_vm jsonb;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'VM pre-SSH stop proof is append-only' USING ERRCODE='23514';
    END IF;
    SELECT context->'vm' INTO owner_vm FROM public.jobs
     WHERE id=NEW.job_id FOR UPDATE;
    SELECT * INTO admission FROM public.vm_workspace_cleanup_admissions
     WHERE id=NEW.cleanup_admission_id FOR UPDATE;
    -- The intent is immutable. Read it before taking the same retry/charge
    -- lock order as the application store; no intent row lock is needed.
    SELECT * INTO intent FROM public.vm_pre_ssh_stop_intents
     WHERE cleanup_admission_id=NEW.cleanup_admission_id;
    SELECT * INTO retry FROM public.vm_creation_retries
     WHERE request_id=intent.creation_request_id FOR UPDATE;
    SELECT * INTO reservation FROM public.vm_resource_reservations
     WHERE id=intent.reservation_id FOR UPDATE;
    IF intent.cleanup_admission_id IS NULL
       OR intent.job_id IS DISTINCT FROM NEW.job_id
       OR intent.provision_generation IS DISTINCT FROM NEW.provision_generation
       OR intent.frozen_digest IS DISTINCT FROM NEW.frozen_digest
       OR admission.id IS NULL OR admission.completed_at IS NOT NULL
       OR admission.owner_kind IS DISTINCT FROM 'job'
       OR admission.owner_id IS DISTINCT FROM intent.job_id
       OR admission.pvc_uid IS DISTINCT FROM intent.pvc_uid
       OR admission.intent_digest IS DISTINCT FROM intent.cleanup_intent_digest
       OR retry.request_id IS NULL
       OR retry.job_id IS DISTINCT FROM intent.job_id
       OR retry.provision_generation IS DISTINCT FROM intent.provision_generation
       OR retry.state IS DISTINCT FROM 'succeeded'
       OR retry.ready_at IS NOT NULL
       OR retry.observed_vm_uid IS DISTINCT FROM intent.vm_uid
       OR retry.observed_pvc_uid IS DISTINCT FROM intent.pvc_uid
       OR reservation.id IS NULL OR reservation.state IS DISTINCT FROM 'teardown'
       OR reservation.request_id IS DISTINCT FROM intent.creation_request_id
       OR reservation.revision IS DISTINCT FROM intent.reservation_revision
       OR reservation.vm_uid IS DISTINCT FROM intent.vm_uid
       OR reservation.vmi_uid IS DISTINCT FROM intent.vmi_uid
       OR reservation.launcher_uid IS DISTINCT FROM intent.launcher_uid
       OR reservation.node_uid IS DISTINCT FROM intent.node_uid
       OR owner_vm->>'status' IS DISTINCT FROM 'retiring_process_zero'
       OR owner_vm->>'provision_generation' IS DISTINCT FROM NEW.provision_generation::text
       OR NEW.terminal_evidence->>'kind' IS DISTINCT FROM 'vm_pre_ssh_positive_stop_v1'
       OR NEW.terminal_evidence->>'frozen_digest' IS DISTINCT FROM NEW.frozen_digest
       OR NEW.terminal_evidence->>'vm_run_strategy' IS DISTINCT FROM 'Halted'
       OR NEW.terminal_evidence->>'vm_generation' IS DISTINCT FROM
          ((intent.frozen->>'vm_generation')::bigint+1)::text
       OR NEW.terminal_evidence->>'node_ready' IS DISTINCT FROM 'true'
       OR NEW.terminal_evidence->>'same_generation_replacement' IS DISTINCT FROM 'false'
       OR NEW.terminal_evidence->>'pod_finalizer' IS DISTINCT FROM 'srw.io/vm-pre-ssh-positive-stop'
       OR NEW.terminal_evidence->>'pod_intent_digest' IS DISTINCT FROM NEW.frozen_digest
       OR NEW.terminal_evidence->>'vm_uid' IS DISTINCT FROM intent.vm_uid::text
       OR NEW.terminal_evidence->>'launcher_uid' IS DISTINCT FROM intent.launcher_uid::text
       OR NEW.terminal_evidence->>'node_uid' IS DISTINCT FROM intent.node_uid::text
       OR NEW.terminal_evidence->>'controller_authenticated' IS DISTINCT FROM 'true'
       OR NEW.evidence_digest IS DISTINCT FROM
          'sha256:'||encode(sha256(convert_to(NEW.terminal_evidence::text,'UTF8')),'hex') THEN
        RAISE EXCEPTION 'VM pre-SSH stop proof lacks exact current authority'
            USING ERRCODE='23514', CONSTRAINT='vm_pre_ssh_stop_proof_authority';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER vm_pre_ssh_stop_proof_guard
BEFORE INSERT OR UPDATE OR DELETE ON public.vm_pre_ssh_stop_proofs
FOR EACH ROW EXECUTE FUNCTION public.guard_vm_pre_ssh_stop_proof();

CREATE FUNCTION public.guard_vm_pre_ssh_zero_insert() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.owner_kind='job' AND NEW.scope='vm' AND NEW.provisioner='vm'
       AND EXISTS (SELECT 1 FROM public.vm_pre_ssh_stop_intents i
                    WHERE i.job_id=NEW.owner_id
                      AND i.provision_generation::text=NEW.runtime_incarnation)
       AND NOT EXISTS (
           SELECT 1 FROM public.vm_pre_ssh_stop_proofs p
           JOIN public.vm_pre_ssh_stop_intents i USING (cleanup_admission_id)
           WHERE i.job_id=NEW.owner_id
             AND i.provision_generation::text=NEW.runtime_incarnation
             AND p.job_id=i.job_id
             AND p.provision_generation=i.provision_generation
       ) THEN
        RAISE EXCEPTION 'VM pre-SSH process zero requires positive terminal proof'
            USING ERRCODE='23514', CONSTRAINT='vm_pre_ssh_zero_requires_proof';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER b_vm_pre_ssh_zero_insert
BEFORE INSERT ON public.managed_repository_process_zero_receipts
FOR EACH ROW EXECUTE FUNCTION public.guard_vm_pre_ssh_zero_insert();

COMMIT;
