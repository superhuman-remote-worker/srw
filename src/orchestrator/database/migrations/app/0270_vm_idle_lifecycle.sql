-- migration: 0270_vm_idle_lifecycle.sql
-- description: Exact VM idle release/wake operations and bounded access leases.
-- depends-on: 0269_vm_creation_disposition_completion.sql
-- transactional: yes
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

CREATE TABLE public.vm_idle_operations (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    owner_kind text NOT NULL CHECK (owner_kind IN ('job','thread')),
    owner_id uuid NOT NULL,
    phase text NOT NULL CHECK (phase IN
        ('releasing','release_held','suspended','waking','wake_held','ready','superseded')),
    episode_id uuid NOT NULL,
    episode_revision bigint NOT NULL CHECK (episode_revision > 0),
    provision_generation uuid NOT NULL,
    vm_uid uuid NOT NULL,
    vmi_uid uuid NOT NULL,
    launcher_uid uuid NOT NULL,
    pvc_uid uuid NOT NULL,
    retained_kind text NOT NULL CHECK (retained_kind IN ('rootdisk','snapshot')),
    admitted_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    last_progress_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    retry_after timestamptz,
    reason text,
    stop_evidence jsonb,
    stop_verified_at timestamptz,
    wake_id uuid,
    wake_generation uuid,
    wake_request_id uuid,
    wake_attempt integer NOT NULL DEFAULT 0 CHECK (wake_attempt >= 0),
    wake_requested boolean NOT NULL DEFAULT false,
    wake_execution_requested boolean NOT NULL DEFAULT false,
    wake_ready_at timestamptz,
    wake_reservation_ref text,
    claim_token bigint NOT NULL DEFAULT 0,
    claimed_by text,
    claim_expires_at timestamptz,
    closed_at timestamptz,
    CONSTRAINT vm_idle_stop_shape CHECK
        ((stop_evidence IS NULL) = (stop_verified_at IS NULL)),
    CONSTRAINT vm_idle_wake_shape CHECK
        ((wake_id IS NULL AND wake_generation IS NULL AND wake_request_id IS NULL)
         OR (wake_id IS NOT NULL AND wake_generation IS NOT NULL AND wake_request_id IS NOT NULL)),
    CONSTRAINT vm_idle_closed_shape CHECK
        ((closed_at IS NULL) = (phase NOT IN ('ready','superseded'))),
    CONSTRAINT vm_idle_claim_shape CHECK
        ((claimed_by IS NULL) = (claim_expires_at IS NULL))
);
CREATE UNIQUE INDEX vm_idle_one_open_owner ON public.vm_idle_operations(owner_kind,owner_id)
    WHERE closed_at IS NULL;
CREATE INDEX vm_idle_due ON public.vm_idle_operations(phase,retry_after,admitted_at)
    WHERE closed_at IS NULL;

CREATE TABLE public.vm_idle_access_leases (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    owner_kind text NOT NULL CHECK (owner_kind IN ('job','thread')),
    owner_id uuid NOT NULL,
    provision_generation uuid NOT NULL,
    vm_uid uuid NOT NULL,
    wake_id uuid,
    kind text NOT NULL CHECK (kind IN ('ssh','sftp','ide')),
    claimed_by text NOT NULL CHECK (length(claimed_by) BETWEEN 1 AND 256),
    acquired_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    expires_at timestamptz NOT NULL,
    max_expires_at timestamptz NOT NULL,
    closed_at timestamptz,
    CHECK (expires_at > acquired_at AND max_expires_at >= expires_at)
);
CREATE INDEX vm_idle_active_access ON public.vm_idle_access_leases(owner_kind,owner_id,expires_at)
    WHERE closed_at IS NULL;

COMMIT;
