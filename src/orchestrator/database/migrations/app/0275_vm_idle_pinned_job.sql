-- migration: 0275_vm_idle_pinned_job.sql
-- description: Exact delivered pinned Job wait and agent-stop authority.
-- depends-on: 0274_vm_resource_whole_launcher.sql
-- transactional: yes
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

-- This row is an intent until the exact agent's response or authenticated
-- route/completion report accepts it. It never grants idle-release by itself.
CREATE TABLE public.pinned_job_deliveries (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id uuid NOT NULL,
    agent_id uuid NOT NULL,
    original_dispatch_marker jsonb NOT NULL,
    marker_digest text NOT NULL CHECK (marker_digest ~ '^sha256:[0-9a-f]{64}$'),
    projection_digest text NOT NULL CHECK (projection_digest ~ '^sha256:[0-9a-f]{64}$'),
    runtime_authority_digest text NOT NULL CHECK (runtime_authority_digest ~ '^sha256:[0-9a-f]{64}$'),
    identity_digest text NOT NULL CHECK (identity_digest ~ '^sha256:[0-9a-f]{64}$'),
    original_lease_expires_at timestamptz NOT NULL,
    intent_lease_expires_at timestamptz NOT NULL,
    process_generation text NOT NULL CHECK (length(process_generation) BETWEEN 1 AND 128),
    pod_name text NOT NULL CHECK (length(pod_name) BETWEEN 1 AND 253),
    pod_namespace text NOT NULL CHECK (length(pod_namespace) BETWEEN 1 AND 63),
    pod_uid text NOT NULL CHECK (length(pod_uid) BETWEEN 1 AND 128),
    provision_generation uuid NOT NULL,
    vm_uid uuid NOT NULL,
    vmi_uid uuid NOT NULL,
    launcher_uid uuid NOT NULL,
    pvc_uid uuid NOT NULL,
    intent_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    accepted_at timestamptz,
    accepted_via text CHECK (accepted_via IN ('post','route','completion')),
    accepted_lease_expires_at timestamptz,
    CONSTRAINT pinned_delivery_accept_shape CHECK (
        (accepted_at IS NULL AND accepted_via IS NULL AND accepted_lease_expires_at IS NULL)
        OR (accepted_at IS NOT NULL AND accepted_via IS NOT NULL
            AND accepted_lease_expires_at IS NOT NULL)
    ),
    CONSTRAINT pinned_delivery_marker_shape CHECK (
        original_dispatch_marker->'version'='1'::jsonb
        AND original_dispatch_marker->>'dispatch_kind'='pinned'
        AND original_dispatch_marker->>'assigned_backend'='vm'
        AND original_dispatch_marker->>'agent_id'=agent_id::text
        AND original_dispatch_marker->>'lease_expires_at' IS NOT NULL
    ),
    UNIQUE(job_id,marker_digest)
);
CREATE INDEX pinned_job_deliveries_job ON public.pinned_job_deliveries(job_id,intent_at DESC);

CREATE TABLE public.pinned_job_wait_receipts (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    delivery_id uuid NOT NULL REFERENCES public.pinned_job_deliveries(id),
    job_id uuid NOT NULL,
    source_kind text NOT NULL CHECK (source_kind IN ('route','completion')),
    source_id uuid NOT NULL,
    lease_expires_at timestamptz NOT NULL,
    observed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(source_kind,source_id),
    UNIQUE(delivery_id,source_kind,source_id)
);
CREATE INDEX pinned_job_wait_receipts_job ON public.pinned_job_wait_receipts(job_id,source_kind,source_id);

CREATE FUNCTION public.guard_pinned_job_delivery() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='DELETE' THEN
        RAISE EXCEPTION 'Pinned delivery history is retained' USING ERRCODE='23514';
    END IF;
    IF ROW(NEW.id,NEW.job_id,NEW.agent_id,NEW.original_dispatch_marker,
           NEW.marker_digest,NEW.projection_digest,NEW.runtime_authority_digest,
           NEW.identity_digest,NEW.original_lease_expires_at,
           NEW.intent_lease_expires_at,NEW.process_generation,NEW.pod_name,
           NEW.pod_namespace,NEW.pod_uid,NEW.provision_generation,NEW.vm_uid,
           NEW.vmi_uid,NEW.launcher_uid,NEW.pvc_uid,NEW.intent_at)
       IS DISTINCT FROM
       ROW(OLD.id,OLD.job_id,OLD.agent_id,OLD.original_dispatch_marker,
           OLD.marker_digest,OLD.projection_digest,OLD.runtime_authority_digest,
           OLD.identity_digest,OLD.original_lease_expires_at,
           OLD.intent_lease_expires_at,OLD.process_generation,OLD.pod_name,
           OLD.pod_namespace,OLD.pod_uid,OLD.provision_generation,OLD.vm_uid,
           OLD.vmi_uid,OLD.launcher_uid,OLD.pvc_uid,OLD.intent_at)
       OR (OLD.accepted_at IS NOT NULL AND
           ROW(NEW.accepted_at,NEW.accepted_via,NEW.accepted_lease_expires_at)
           IS DISTINCT FROM
           ROW(OLD.accepted_at,OLD.accepted_via,OLD.accepted_lease_expires_at)) THEN
        RAISE EXCEPTION 'Pinned delivery identity is immutable' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER pinned_job_delivery_guard
    BEFORE UPDATE OR DELETE ON public.pinned_job_deliveries
    FOR EACH ROW EXECUTE FUNCTION public.guard_pinned_job_delivery();

CREATE FUNCTION public.guard_pinned_job_wait_receipt() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'Pinned wait source is immutable' USING ERRCODE='23514';
END $$;
CREATE TRIGGER pinned_job_wait_receipt_guard
    BEFORE UPDATE OR DELETE ON public.pinned_job_wait_receipts
    FOR EACH ROW EXECUTE FUNCTION public.guard_pinned_job_wait_receipt();

-- 0272 admitted only stateless terminal decisions. Extend the same native
-- finalized-source fence to a pinned completion with a delivered wait receipt.
CREATE OR REPLACE FUNCTION public.guard_vm_idle_terminal_decision() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    source_job public.jobs%ROWTYPE;
BEGIN
    IF TG_OP='DELETE' THEN
        IF OLD.terminal_source_command_id IS NOT NULL THEN
            RAISE EXCEPTION 'Terminal idle retention decision is immutable' USING ERRCODE='23514';
        END IF;
        RETURN OLD;
    END IF;
    IF TG_OP='UPDATE' AND OLD.terminal_source_command_id IS NOT NULL THEN
        IF NEW.terminal_source_command_id IS DISTINCT FROM OLD.terminal_source_command_id
           OR NEW.terminal_decided_at IS DISTINCT FROM OLD.terminal_decided_at
           OR NEW.storage_disposition IS DISTINCT FROM OLD.storage_disposition
           OR NEW.terminal_publication IS DISTINCT FROM OLD.terminal_publication
           OR (OLD.terminal_published_at IS NOT NULL
               AND NEW.terminal_published_at IS DISTINCT FROM OLD.terminal_published_at)
           OR ROW(NEW.owner_kind,NEW.owner_id,NEW.episode_id,NEW.episode_revision,
                  NEW.provision_generation,NEW.vm_uid,NEW.vmi_uid,NEW.launcher_uid,
                  NEW.pvc_uid,NEW.retained_kind)
              IS DISTINCT FROM
              ROW(OLD.owner_kind,OLD.owner_id,OLD.episode_id,OLD.episode_revision,
                  OLD.provision_generation,OLD.vm_uid,OLD.vmi_uid,OLD.launcher_uid,
                  OLD.pvc_uid,OLD.retained_kind) THEN
            RAISE EXCEPTION 'Terminal idle retention identity is immutable' USING ERRCODE='23514';
        END IF;
    ELSIF NEW.terminal_source_command_id IS NOT NULL THEN
        SELECT * INTO source_job FROM public.jobs WHERE id=NEW.owner_id FOR SHARE;
        IF source_job.id IS NULL OR source_job.status::text<>'pending_review'
           OR source_job.execution_lane NOT IN ('stateless','pinned')
           OR source_job.workspace_idle_revision<>NEW.episode_revision
           OR source_job.workspace_idle_episode->>'episode_id'<>NEW.episode_id::text
           OR source_job.workspace_idle_episode->>'wait_kind'<>'human_review'
           OR source_job.workspace_idle_episode->>'wait_key'<>NEW.terminal_source_command_id::text
           OR (source_job.execution_lane='pinned' AND NOT EXISTS (
               SELECT 1 FROM public.pinned_job_wait_receipts r
               JOIN public.pinned_job_deliveries d ON d.id=r.delivery_id
               WHERE r.job_id=NEW.owner_id AND r.source_kind='completion'
                 AND r.source_id=NEW.terminal_source_command_id
                 AND d.job_id=NEW.owner_id AND d.accepted_at IS NOT NULL
                 AND NEW.pinned_wait_receipt_id=r.id
                 AND NEW.pinned_delivery_id=d.id
           ))
           OR NOT EXISTS (
               SELECT 1 FROM public.job_completion_commands c
               JOIN public.completion_effects e
                 ON e.producer_kind='job_completion' AND e.producer_id=c.id
                AND e.scope_id=NEW.owner_id AND e.effect_name='main_status_write'
                AND e.state='done' AND e.completed_at IS NOT NULL
               WHERE c.id=NEW.terminal_source_command_id AND c.job_id=NEW.owner_id
                 AND c.state='done' AND c.finalized_at IS NOT NULL
                 AND c.report_seq=source_job.completion_seq_hwm
                 AND c.payload->'_accepted_idle_wait_source'->>'rootdisk_pvc_uid'
                     =NEW.pvc_uid::text
           ) THEN
            RAISE EXCEPTION 'Terminal idle review source is not finalized' USING ERRCODE='23514';
        END IF;
    END IF;
    RETURN NEW;
END $$;

ALTER TABLE public.vm_idle_operations
    ADD COLUMN release_kind text NOT NULL DEFAULT 'stateless'
        CHECK (release_kind IN ('stateless','pinned_job')),
    ADD COLUMN pinned_delivery_id uuid REFERENCES public.pinned_job_deliveries(id),
    ADD COLUMN pinned_wait_receipt_id uuid REFERENCES public.pinned_job_wait_receipts(id),
    ADD COLUMN pinned_agent_id uuid,
    ADD COLUMN pinned_process_generation text,
    ADD COLUMN pinned_agent_pod_name text,
    ADD COLUMN pinned_agent_pod_namespace text,
    ADD COLUMN pinned_agent_pod_uid text,
    ADD COLUMN pinned_original_dispatch_marker jsonb,
    ADD COLUMN pinned_lease_observed_at timestamptz,
    ADD COLUMN pinned_lease_expires_at timestamptz,
    ADD COLUMN pinned_terminal_observed_at timestamptz,
    ADD COLUMN pinned_stop_evidence jsonb,
    ADD COLUMN pinned_stop_verified_at timestamptz,
    ADD CONSTRAINT vm_idle_pinned_shape CHECK (
        (release_kind='stateless' AND pinned_delivery_id IS NULL
            AND pinned_wait_receipt_id IS NULL AND pinned_agent_id IS NULL
            AND pinned_process_generation IS NULL AND pinned_agent_pod_name IS NULL
            AND pinned_agent_pod_namespace IS NULL AND pinned_agent_pod_uid IS NULL
            AND pinned_original_dispatch_marker IS NULL
            AND pinned_lease_observed_at IS NULL AND pinned_lease_expires_at IS NULL
            AND pinned_terminal_observed_at IS NULL
            AND pinned_stop_evidence IS NULL AND pinned_stop_verified_at IS NULL)
        OR (release_kind='pinned_job' AND owner_kind='job'
            AND pinned_delivery_id IS NOT NULL AND pinned_wait_receipt_id IS NOT NULL
            AND pinned_agent_id IS NOT NULL AND pinned_process_generation IS NOT NULL
            AND pinned_agent_pod_name IS NOT NULL AND pinned_agent_pod_namespace IS NOT NULL
            AND pinned_agent_pod_uid IS NOT NULL AND pinned_original_dispatch_marker IS NOT NULL
            AND pinned_lease_observed_at IS NOT NULL AND pinned_lease_expires_at IS NOT NULL
            AND (pinned_stop_verified_at IS NULL OR pinned_terminal_observed_at IS NOT NULL)
            AND ((pinned_stop_evidence IS NULL)=(pinned_stop_verified_at IS NULL)))
    );

CREATE FUNCTION public.guard_vm_idle_pinned_job() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='DELETE' THEN
        IF OLD.release_kind='pinned_job' THEN
            RAISE EXCEPTION 'Pinned idle operation is retained' USING ERRCODE='23514';
        END IF;
        RETURN OLD;
    END IF;
    IF TG_OP='UPDATE' THEN
        IF ROW(NEW.release_kind,NEW.pinned_delivery_id,NEW.pinned_wait_receipt_id,
               NEW.pinned_agent_id,NEW.pinned_process_generation,
               NEW.pinned_agent_pod_name,NEW.pinned_agent_pod_namespace,
               NEW.pinned_agent_pod_uid,NEW.pinned_original_dispatch_marker,
               NEW.pinned_lease_observed_at,NEW.pinned_lease_expires_at)
           IS DISTINCT FROM
           ROW(OLD.release_kind,OLD.pinned_delivery_id,OLD.pinned_wait_receipt_id,
               OLD.pinned_agent_id,OLD.pinned_process_generation,
               OLD.pinned_agent_pod_name,OLD.pinned_agent_pod_namespace,
               OLD.pinned_agent_pod_uid,OLD.pinned_original_dispatch_marker,
               OLD.pinned_lease_observed_at,OLD.pinned_lease_expires_at)
           OR (OLD.pinned_stop_verified_at IS NOT NULL AND
               ROW(NEW.pinned_stop_evidence,NEW.pinned_stop_verified_at)
               IS DISTINCT FROM ROW(OLD.pinned_stop_evidence,OLD.pinned_stop_verified_at))
           OR (OLD.pinned_terminal_observed_at IS NOT NULL AND
               NEW.pinned_terminal_observed_at IS DISTINCT FROM
               OLD.pinned_terminal_observed_at) THEN
            RAISE EXCEPTION 'Pinned idle authority is immutable' USING ERRCODE='23514';
        END IF;
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER vm_idle_pinned_job_guard
    BEFORE UPDATE OR DELETE ON public.vm_idle_operations
    FOR EACH ROW EXECUTE FUNCTION public.guard_vm_idle_pinned_job();

-- A final claimant's stale ready-agent selection and every legacy resume
-- writer must see the durable open operation, even if a short control claim
-- has expired. The wake close and paused transition commit together.
CREATE FUNCTION public.guard_open_pinned_job_idle() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM public.vm_idle_operations idle
        WHERE idle.owner_kind='job' AND idle.owner_id=NEW.id
          AND idle.release_kind='pinned_job' AND idle.closed_at IS NULL
    ) AND (
        NEW.status::text IN ('processing','paused')
        OR (NEW.assigned_agent_id IS DISTINCT FROM OLD.assigned_agent_id
            AND NEW.status::text NOT IN ('completed','failed','cancelled'))
    ) THEN
        RAISE EXCEPTION 'Pinned Job idle operation owns dispatch'
            USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER open_pinned_job_idle_guard
    BEFORE UPDATE ON public.jobs
    FOR EACH ROW EXECUTE FUNCTION public.guard_open_pinned_job_idle();

COMMIT;
