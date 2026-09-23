-- migration: 0272_vm_idle_terminal_review.sql
-- description: Exact terminal review no-wake decision and retained-rootdisk hold.
-- depends-on: 0271_vm_network_profile.sql
-- transactional: yes
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE public.vm_idle_operations
    ADD COLUMN terminal_source_command_id uuid REFERENCES public.job_completion_commands(id),
    ADD COLUMN terminal_decided_at timestamptz,
    ADD COLUMN storage_disposition text,
    ADD COLUMN terminal_publication jsonb,
    ADD COLUMN terminal_published_at timestamptz,
    ADD COLUMN terminal_publication_retry_after timestamptz,
    ADD CONSTRAINT vm_idle_terminal_decision_shape CHECK (
        (terminal_source_command_id IS NULL AND terminal_decided_at IS NULL
         AND storage_disposition IS NULL)
        OR (terminal_source_command_id IS NOT NULL AND terminal_decided_at IS NOT NULL
            AND storage_disposition='retention_unknown'
            AND owner_kind='job' AND retained_kind='rootdisk'
            AND wake_id IS NULL AND wake_generation IS NULL
            AND wake_request_id IS NULL AND wake_ready_at IS NULL
            AND NOT wake_requested AND NOT wake_execution_requested
            AND phase IN ('releasing','release_held','suspended','superseded'))
    ),
    ADD CONSTRAINT vm_idle_terminal_publication_shape CHECK (
        (terminal_publication IS NULL AND terminal_published_at IS NULL
         AND terminal_source_command_id IS NULL)
        OR (terminal_publication IS NOT NULL
            AND jsonb_typeof(terminal_publication)='object'
            AND terminal_publication->'version'='1'::jsonb
            AND terminal_source_command_id IS NOT NULL)
    ),
    ADD CONSTRAINT vm_idle_terminal_publication_retry_shape CHECK (
        terminal_publication_retry_after IS NULL
        OR (terminal_source_command_id IS NOT NULL
            AND terminal_published_at IS NULL)
    );
CREATE UNIQUE INDEX vm_idle_one_terminal_decision
    ON public.vm_idle_operations(owner_kind,owner_id)
    WHERE terminal_source_command_id IS NOT NULL;
CREATE INDEX vm_idle_retained_rootdisk ON public.vm_idle_operations(owner_id,pvc_uid)
    WHERE storage_disposition='retention_unknown';

CREATE FUNCTION public.guard_vm_idle_terminal_decision() RETURNS trigger LANGUAGE plpgsql AS $$
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
           OR source_job.execution_lane<>'stateless'
           OR source_job.workspace_idle_revision<>NEW.episode_revision
           OR source_job.workspace_idle_episode->>'episode_id'<>NEW.episode_id::text
           OR source_job.workspace_idle_episode->>'wait_kind'<>'human_review'
           OR source_job.workspace_idle_episode->>'wait_key'<>NEW.terminal_source_command_id::text
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
CREATE TRIGGER vm_idle_terminal_decision_guard
    BEFORE INSERT OR UPDATE OR DELETE ON public.vm_idle_operations
    FOR EACH ROW EXECUTE FUNCTION public.guard_vm_idle_terminal_decision();

-- A cleanup admission and terminal review serialize on the same Job row. A
-- permit admitted first makes terminal approval hold; a later permit cannot
-- erase an accepted retention decision, including with idle admission off.
CREATE FUNCTION public.guard_vm_idle_retained_cleanup() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.owner_kind='job' AND NEW.source IN
       ('completion_workspace_teardown','kept_disk') THEN
        PERFORM 1 FROM public.jobs WHERE id=NEW.owner_id FOR SHARE;
        IF EXISTS (
            SELECT 1 FROM public.vm_idle_operations o
            WHERE o.owner_kind='job' AND o.owner_id=NEW.owner_id
              AND (NEW.pvc_uid IS NULL OR o.pvc_uid=NEW.pvc_uid)
              AND o.storage_disposition='retention_unknown'
        ) THEN
            RAISE EXCEPTION 'Terminal idle retained rootdisk is held' USING ERRCODE='23514';
        END IF;
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER vm_idle_retained_cleanup_guard
    BEFORE INSERT OR UPDATE ON public.vm_workspace_cleanup_admissions
    FOR EACH ROW EXECUTE FUNCTION public.guard_vm_idle_retained_cleanup();

COMMIT;
