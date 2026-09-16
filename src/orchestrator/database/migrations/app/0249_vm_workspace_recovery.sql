-- migration:     0249_vm_workspace_recovery.sql
-- description:   Durable authority and evidence for bounded VM workspace recovery.
-- depends-on:    0248_retire_unkeyed_credential_provenance.sql
-- transactional: yes

BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';
SET LOCAL idle_in_transaction_session_timeout = '1min';

CREATE TABLE vm_workspace_recoveries (
    id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    protocol_version integer NOT NULL DEFAULT 1 CHECK (protocol_version = 1),
    owner_kind text NOT NULL CHECK (owner_kind IN ('job', 'thread')),
    owner_id uuid NOT NULL,
    workspace_contract_digest text NOT NULL CHECK (workspace_contract_digest <> ''),
    provision_generation uuid NOT NULL,
    cluster_name text NOT NULL CHECK (cluster_name <> ''),
    namespace text NOT NULL CHECK (namespace <> ''),
    vm_uid uuid NOT NULL,
    prior_vmi_uid uuid NOT NULL,
    prior_launcher_uid uuid NOT NULL,
    root_pvc_uid uuid NOT NULL,
    phase text NOT NULL DEFAULT 'recovering'
        CHECK (phase IN ('recovering', 'paused_attention', 'recovered', 'cancelled')),
    first_observed_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    deadline_at timestamptz NOT NULL
        DEFAULT (transaction_timestamp() + interval '15 minutes'),
    next_check_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    last_progress_at timestamptz,
    reason_code text NOT NULL CHECK (reason_code IN (
        'workspace_runtime_not_ready',
        'workspace_transport_unavailable',
        'workspace_replacement_observed',
        'workspace_identity_conflict',
        'prior_runtime_unfenced',
        'tool_outcome_unknown',
        'checkpoint_unavailable',
        'workspace_recovery_deadline_exceeded'
    )),
    original_cause jsonb NOT NULL DEFAULT '{}'::jsonb
        CHECK (jsonb_typeof(original_cause) = 'object'),
    latest_diagnostic jsonb,
    latest_observation jsonb,
    successor_observation jsonb,
    version integer NOT NULL DEFAULT 1 CHECK (version > 0),
    claim_token bigint NOT NULL DEFAULT 0 CHECK (claim_token >= 0),
    claimed_by text,
    claimed_until timestamptz,
    resolved_at timestamptz,
    CHECK (deadline_at = first_observed_at + interval '15 minutes'),
    CHECK ((claimed_by IS NULL AND claimed_until IS NULL)
        OR (claimed_by IS NOT NULL AND claimed_by <> ''
            AND claimed_until IS NOT NULL AND claim_token > 0)),
    CHECK ((resolved_at IS NULL AND phase IN ('recovering', 'paused_attention'))
        OR (resolved_at IS NOT NULL AND phase IN ('recovered', 'cancelled')))
);

CREATE UNIQUE INDEX vm_workspace_recoveries_one_open_owner
ON vm_workspace_recoveries (owner_kind, owner_id)
WHERE resolved_at IS NULL;

CREATE INDEX vm_workspace_recoveries_due
ON vm_workspace_recoveries (next_check_at, deadline_at, id)
WHERE phase = 'recovering' AND resolved_at IS NULL;

CREATE TABLE vm_workspace_recovery_jobs (
    recovery_id uuid NOT NULL REFERENCES vm_workspace_recoveries(id) ON DELETE CASCADE,
    job_id uuid NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    accepted_lease_token bigint CHECK (accepted_lease_token > 0),
    hold_lease_token bigint NOT NULL CHECK (hold_lease_token > 0),
    prior_queue_state text NOT NULL,
    prior_job_status text NOT NULL,
    prior_control_reference jsonb,
    prior_freeze_reference jsonb,
    checkpoint_id text,
    checkpoint_namespace text,
    participation text NOT NULL DEFAULT 'held'
        CHECK (participation IN ('held', 'attention', 'released', 'cancelled')),
    outcome jsonb,
    resume_receipt jsonb,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    resolved_at timestamptz,
    PRIMARY KEY (recovery_id, job_id),
    CHECK (accepted_lease_token IS NULL OR hold_lease_token > accepted_lease_token),
    CHECK ((checkpoint_id IS NULL) = (checkpoint_namespace IS NULL)),
    CHECK ((resolved_at IS NULL AND participation IN ('held', 'attention'))
        OR (resolved_at IS NOT NULL AND participation IN ('released', 'cancelled')))
);

CREATE UNIQUE INDEX vm_workspace_recovery_jobs_one_open_job
ON vm_workspace_recovery_jobs (job_id)
WHERE resolved_at IS NULL;

CREATE TABLE worker_batch_attempts (
    job_id uuid NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    lease_token bigint NOT NULL CHECK (lease_token > 0),
    claimed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    claimed_attempt integer NOT NULL CHECK (claimed_attempt > 0),
    protocol_version integer NOT NULL DEFAULT 1 CHECK (protocol_version = 1),
    bundle_authorized_at timestamptz,
    authority_digest text,
    disposition jsonb,
    recovery_id uuid REFERENCES vm_workspace_recoveries(id),
    refund_reason text,
    refunded_at timestamptz,
    PRIMARY KEY (job_id, lease_token),
    CHECK ((bundle_authorized_at IS NULL AND authority_digest IS NULL)
        OR (bundle_authorized_at IS NOT NULL AND authority_digest IS NOT NULL
            AND authority_digest <> '')),
    CHECK ((refunded_at IS NULL AND refund_reason IS NULL)
        OR (refunded_at IS NOT NULL AND refund_reason IS NOT NULL
            AND refund_reason <> ''))
);

CREATE TABLE vm_workspace_recovery_requests (
    scope_kind text NOT NULL CHECK (scope_kind IN ('job', 'workspace', 'recovery')),
    scope_id uuid NOT NULL,
    request_id uuid NOT NULL,
    actor_kind text NOT NULL CHECK (actor_kind <> ''),
    actor_id text NOT NULL CHECK (actor_id <> ''),
    intent_digest text NOT NULL CHECK (intent_digest <> ''),
    recovery_id uuid NOT NULL REFERENCES vm_workspace_recoveries(id),
    accepted_result jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (scope_kind, scope_id, request_id)
);

CREATE TABLE vm_workspace_recovery_stop_receipts (
    id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    recovery_id uuid NOT NULL REFERENCES vm_workspace_recoveries(id),
    protocol_version integer NOT NULL DEFAULT 1 CHECK (protocol_version = 1),
    accepted_claim_token bigint NOT NULL CHECK (accepted_claim_token > 0),
    vm_uid uuid NOT NULL,
    vmi_uid uuid NOT NULL,
    launcher_uid uuid NOT NULL,
    container_id text NOT NULL CHECK (container_id <> ''),
    root_pvc_uid uuid NOT NULL,
    controller_identity text NOT NULL CHECK (controller_identity <> ''),
    observed_at timestamptz NOT NULL,
    evidence jsonb NOT NULL CHECK (jsonb_typeof(evidence) = 'object'),
    evidence_digest text NOT NULL CHECK (evidence_digest <> ''),
    accepted_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (recovery_id, evidence_digest)
);

CREATE FUNCTION prevent_vm_workspace_recovery_stop_receipt_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION USING
        ERRCODE = '55000',
        MESSAGE = 'VM workspace recovery stop receipts are append-only';
END;
$$;

CREATE TRIGGER vm_workspace_recovery_stop_receipts_append_only
BEFORE UPDATE OR DELETE ON vm_workspace_recovery_stop_receipts
FOR EACH ROW EXECUTE FUNCTION prevent_vm_workspace_recovery_stop_receipt_mutation();

CREATE TABLE vm_workspace_recovery_probe_slots (
    recovery_id uuid PRIMARY KEY REFERENCES vm_workspace_recoveries(id) ON DELETE CASCADE,
    global_slot integer NOT NULL CHECK (global_slot >= 0),
    node_key text NOT NULL CHECK (node_key <> ''),
    claim_token bigint NOT NULL CHECK (claim_token > 0),
    leased_until timestamptz NOT NULL,
    acquired_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (global_slot),
    UNIQUE (node_key)
);

CREATE TABLE vm_workspace_recovery_retention_pins (
    recovery_id uuid NOT NULL REFERENCES vm_workspace_recoveries(id) ON DELETE CASCADE,
    pvc_uid uuid NOT NULL,
    provision_generation uuid NOT NULL,
    pinned_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    released_at timestamptz,
    PRIMARY KEY (recovery_id, pvc_uid)
);

CREATE FUNCTION prevent_vm_workspace_recovery_identity_rebind()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF ROW(
        OLD.protocol_version, OLD.owner_kind, OLD.owner_id,
        OLD.workspace_contract_digest, OLD.provision_generation,
        OLD.cluster_name, OLD.namespace, OLD.vm_uid, OLD.prior_vmi_uid,
        OLD.prior_launcher_uid, OLD.root_pvc_uid, OLD.first_observed_at,
        OLD.deadline_at, OLD.original_cause
    ) IS DISTINCT FROM ROW(
        NEW.protocol_version, NEW.owner_kind, NEW.owner_id,
        NEW.workspace_contract_digest, NEW.provision_generation,
        NEW.cluster_name, NEW.namespace, NEW.vm_uid, NEW.prior_vmi_uid,
        NEW.prior_launcher_uid, NEW.root_pvc_uid, NEW.first_observed_at,
        NEW.deadline_at, NEW.original_cause
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = 'VM workspace recovery identity and deadline are immutable';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER vm_workspace_recoveries_immutable_identity
BEFORE UPDATE ON vm_workspace_recoveries
FOR EACH ROW EXECUTE FUNCTION prevent_vm_workspace_recovery_identity_rebind();

COMMIT;
