-- migration: 0257_vm_creation_retries.sql
-- description: Immutable VM create intent, observer claims and existing cleanup authority.
-- depends-on: 0256_vm_workspace_cleanup_delegation.sql
-- transactional: yes
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

CREATE TABLE vm_creation_retries (
    request_id uuid PRIMARY KEY,
    job_id uuid NOT NULL REFERENCES jobs(id),
    provision_generation uuid NOT NULL,
    origin text NOT NULL CHECK (origin IN ('initial','resume')),
    request_digest text NOT NULL CHECK (request_digest ~ '^sha256:[0-9a-f]{64}$'),
    canonical_request jsonb NOT NULL CHECK (jsonb_typeof(canonical_request)='object'),
    controller_configuration_digest text NOT NULL CHECK (controller_configuration_digest ~ '^sha256:[0-9a-f]{64}$'),
    execution_id uuid NOT NULL REFERENCES srw_execution_specs(id),
    execution_revision text NOT NULL,
    execution_generation bigint NOT NULL,
    admission_deadline timestamptz,
    expected_pvc_uid uuid,
    predecessor_evidence jsonb NOT NULL DEFAULT '{}' CHECK (jsonb_typeof(predecessor_evidence)='object'),
    predecessor_cleanup_admission_id uuid REFERENCES vm_workspace_cleanup_admissions(id),
    creation_admission_id uuid REFERENCES vm_workspace_cleanup_admissions(id),
    state text NOT NULL DEFAULT 'queued' CHECK (state IN ('queued','reconciling','succeeded','attention','cancel_requested','settled')),
    revision bigint NOT NULL DEFAULT 0 CHECK (revision >= 0),
    claim_token uuid,
    claim_expires_at timestamptz,
    next_probe_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    backoff_attempt integer NOT NULL DEFAULT 0 CHECK (backoff_attempt >= 0),
    transport_outage_started_at timestamptz,
    reason text,
    boot_counted boolean NOT NULL DEFAULT false,
    observed_vm_uid uuid,
    observed_pvc_uid uuid,
    ready_at timestamptz,
    resolved_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(job_id,provision_generation),
    CHECK ((claim_token IS NULL) = (claim_expires_at IS NULL)),
    CHECK (claim_token IS NULL OR state IN ('reconciling','cancel_requested')),
    CHECK ((resolved_at IS NOT NULL) = (state IN ('succeeded','settled'))),
    CHECK (state <> 'succeeded' OR (observed_vm_uid IS NOT NULL AND observed_pvc_uid IS NOT NULL AND creation_admission_id IS NOT NULL)),
    CHECK (ready_at IS NULL OR state='succeeded'),
    CHECK (expected_pvc_uid IS NULL OR observed_pvc_uid IS NULL OR expected_pvc_uid=observed_pvc_uid),
    CHECK (predecessor_cleanup_admission_id IS NULL OR expected_pvc_uid IS NOT NULL)
);
CREATE INDEX vm_creation_retries_due ON vm_creation_retries(next_probe_at,request_id)
WHERE state IN ('queued','reconciling','cancel_requested');

CREATE FUNCTION guard_vm_creation_retry_identity() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF ROW(NEW.request_id,NEW.job_id,NEW.provision_generation,NEW.origin,
           NEW.request_digest,NEW.canonical_request,NEW.controller_configuration_digest,
           NEW.execution_id,NEW.execution_revision,NEW.execution_generation,NEW.admission_deadline,
           NEW.expected_pvc_uid,NEW.predecessor_evidence,NEW.predecessor_cleanup_admission_id,NEW.created_at)
       IS DISTINCT FROM
       ROW(OLD.request_id,OLD.job_id,OLD.provision_generation,OLD.origin,
           OLD.request_digest,OLD.canonical_request,OLD.controller_configuration_digest,
           OLD.execution_id,OLD.execution_revision,OLD.execution_generation,OLD.admission_deadline,
           OLD.expected_pvc_uid,OLD.predecessor_evidence,OLD.predecessor_cleanup_admission_id,OLD.created_at)
       OR (OLD.creation_admission_id IS NOT NULL AND NEW.creation_admission_id IS DISTINCT FROM OLD.creation_admission_id)
       OR (OLD.observed_vm_uid IS NOT NULL AND NEW.observed_vm_uid IS DISTINCT FROM OLD.observed_vm_uid)
       OR (OLD.observed_pvc_uid IS NOT NULL AND NEW.observed_pvc_uid IS DISTINCT FROM OLD.observed_pvc_uid)
       OR (OLD.boot_counted AND NOT NEW.boot_counted)
       OR NEW.revision < OLD.revision THEN
        RAISE EXCEPTION 'VM creation retry identity is immutable' USING ERRCODE='23514';
    END IF;
    IF NEW.state <> OLD.state AND NOT (
        (OLD.state='queued' AND NEW.state IN ('reconciling','cancel_requested')) OR
        (OLD.state='reconciling' AND NEW.state IN ('queued','attention','succeeded','cancel_requested')) OR
        (OLD.state='attention' AND NEW.state IN ('queued','cancel_requested')) OR
        (OLD.state='cancel_requested' AND NEW.state='settled')
    ) THEN
        RAISE EXCEPTION 'Invalid VM creation retry transition' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER vm_creation_retry_identity BEFORE UPDATE ON vm_creation_retries
FOR EACH ROW EXECUTE FUNCTION guard_vm_creation_retry_identity();

CREATE FUNCTION request_vm_creation_retry_cancel(p_job_id uuid, p_generation text DEFAULT NULL)
RETURNS boolean LANGUAGE plpgsql AS $$
DECLARE touched integer;
BEGIN
    UPDATE vm_creation_retries SET state='cancel_requested',revision=revision+1,
        claim_token=NULL,claim_expires_at=NULL,next_probe_at=clock_timestamp(),
        reason='job_cancelled',updated_at=clock_timestamp()
    WHERE job_id=p_job_id AND (p_generation IS NULL OR provision_generation::text=p_generation)
      AND state IN ('queued','reconciling','attention');
    GET DIAGNOSTICS touched = ROW_COUNT;
    RETURN touched > 0;
END;
$$;
CREATE FUNCTION cancel_vm_creation_retry_on_job_control() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.status::text IN ('cancelled','completed') OR
       NEW.context ?| ARRAY['_stateless_delete_pending','_stateless_cancel_cleanup_pending'] THEN
        PERFORM request_vm_creation_retry_cancel(NEW.id);
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER vm_creation_retry_job_control AFTER UPDATE OF status,context ON jobs
FOR EACH ROW EXECUTE FUNCTION cancel_vm_creation_retry_on_job_control();
COMMIT;
