-- migration: 0273_vm_idle_access_wake_lineage.sql
-- description: Immutable exact access-only rebind proof for unchanged S17 waits.
-- depends-on: 0272_vm_idle_terminal_review.sql
-- transactional: yes
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE public.vm_idle_operations ADD COLUMN access_rebind_proof jsonb;
CREATE INDEX vm_idle_episode_history
    ON public.vm_idle_operations(owner_kind,owner_id,episode_id,episode_revision DESC,id DESC);
CREATE UNIQUE INDEX vm_idle_one_access_rebind_revision
    ON public.vm_idle_operations(owner_id,episode_id,episode_revision)
    WHERE access_rebind_proof IS NOT NULL;
CREATE UNIQUE INDEX vm_idle_access_successor_generation
    ON public.vm_idle_operations(owner_id,episode_id,wake_generation)
    WHERE access_rebind_proof IS NOT NULL;

CREATE FUNCTION public.guard_vm_idle_access_rebind() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    proof jsonb;
    current_job public.jobs%ROWTYPE;
    prior public.vm_idle_operations%ROWTYPE;
    root public.vm_idle_operations%ROWTYPE;
BEGIN
    IF TG_OP='DELETE' THEN
        -- An old unproven row must remain visible: otherwise a later wake
        -- could appear to be the first link of a forged complete history.
        RAISE EXCEPTION 'VM idle operation history is retained' USING ERRCODE='23514';
    END IF;
    IF TG_OP='UPDATE' AND
       ROW(NEW.owner_kind,NEW.owner_id,NEW.episode_id,NEW.episode_revision,
           NEW.provision_generation,NEW.vm_uid,NEW.vmi_uid,NEW.launcher_uid,
           NEW.pvc_uid,NEW.retained_kind)
       IS DISTINCT FROM
       ROW(OLD.owner_kind,OLD.owner_id,OLD.episode_id,OLD.episode_revision,
           OLD.provision_generation,OLD.vm_uid,OLD.vmi_uid,OLD.launcher_uid,
           OLD.pvc_uid,OLD.retained_kind) THEN
        RAISE EXCEPTION 'VM idle operation source identity is immutable' USING ERRCODE='23514';
    END IF;
    IF TG_OP='UPDATE' AND OLD.access_rebind_proof IS NOT NULL THEN
        IF NEW.access_rebind_proof IS DISTINCT FROM OLD.access_rebind_proof
           OR ROW(NEW.owner_kind,NEW.owner_id,NEW.episode_id,NEW.episode_revision,
                  NEW.provision_generation,NEW.vm_uid,NEW.vmi_uid,NEW.launcher_uid,
                  NEW.pvc_uid,NEW.retained_kind,NEW.stop_evidence,NEW.stop_verified_at,
                  NEW.wake_id,NEW.wake_generation,NEW.wake_request_id,NEW.wake_ready_at,
                  NEW.wake_execution_requested)
              IS DISTINCT FROM
              ROW(OLD.owner_kind,OLD.owner_id,OLD.episode_id,OLD.episode_revision,
                  OLD.provision_generation,OLD.vm_uid,OLD.vmi_uid,OLD.launcher_uid,
                  OLD.pvc_uid,OLD.retained_kind,OLD.stop_evidence,OLD.stop_verified_at,
                  OLD.wake_id,OLD.wake_generation,OLD.wake_request_id,OLD.wake_ready_at,
                  OLD.wake_execution_requested) THEN
            RAISE EXCEPTION 'Access wake lineage identity is immutable' USING ERRCODE='23514';
        END IF;
        RETURN NEW;
    END IF;
    proof := NEW.access_rebind_proof;
    IF proof IS NULL THEN
        RETURN NEW;
    END IF;
    IF jsonb_typeof(proof) IS DISTINCT FROM 'object'
       OR proof->'version' IS DISTINCT FROM '1'::jsonb
       OR NEW.owner_kind<>'job' OR NEW.retained_kind<>'rootdisk'
       OR NEW.stop_verified_at IS NULL OR NEW.stop_evidence IS NULL
       OR NEW.wake_ready_at IS NULL OR NEW.wake_id IS NULL
       OR NEW.wake_generation IS NULL OR NEW.wake_request_id IS NULL
       OR NEW.wake_execution_requested OR NEW.terminal_source_command_id IS NOT NULL
       OR proof->>'operation_id' IS DISTINCT FROM NEW.id::text
       OR proof->>'episode_id' IS DISTINCT FROM NEW.episode_id::text
       OR proof->>'from_revision' IS DISTINCT FROM NEW.episode_revision::text
       OR proof->>'to_revision' IS DISTINCT FROM (NEW.episode_revision+1)::text
       OR proof->>'root_operation_id' IS NULL
       OR proof->>'root_revision' IS NULL
       OR proof->>'chain_length' IS NULL
       OR proof->>'wake_id' IS DISTINCT FROM NEW.wake_id::text
       OR (proof->>'stop_verified_at')::timestamptz IS DISTINCT FROM NEW.stop_verified_at
       OR (proof->>'ready_at')::timestamptz IS DISTINCT FROM NEW.wake_ready_at
       OR proof->'predecessor' IS DISTINCT FROM jsonb_build_object(
            'generation',NEW.provision_generation::text,'vm_uid',NEW.vm_uid::text,
            'vmi_uid',NEW.vmi_uid::text,'launcher_uid',NEW.launcher_uid::text,
            'pvc_uid',NEW.pvc_uid::text)
       OR jsonb_typeof(proof->'successor') IS DISTINCT FROM 'object'
       OR proof->'successor' IS DISTINCT FROM jsonb_build_object(
            'generation',proof->'successor'->>'generation',
            'vm_uid',proof->'successor'->>'vm_uid',
            'vmi_uid',proof->'successor'->>'vmi_uid',
            'launcher_uid',proof->'successor'->>'launcher_uid',
            'pvc_uid',proof->'successor'->>'pvc_uid')
       OR proof->'successor'->>'generation' IS DISTINCT FROM NEW.wake_generation::text
       OR proof->'successor'->>'pvc_uid' IS DISTINCT FROM NEW.pvc_uid::text
       OR proof->'successor'->>'vm_uid' IS NULL
       OR proof->'successor'->>'vmi_uid' IS NULL
       OR proof->'successor'->>'launcher_uid' IS NULL
       OR proof->'successor'->>'vm_uid'=NEW.vm_uid::text
       OR proof->'successor'->>'vmi_uid'=NEW.vmi_uid::text
       OR proof->'successor'->>'launcher_uid'=NEW.launcher_uid::text THEN
        RAISE EXCEPTION 'Access wake lineage shape is unproven' USING ERRCODE='23514';
    END IF;
    SELECT * INTO prior FROM public.vm_idle_operations
    WHERE owner_kind='job' AND owner_id=NEW.owner_id
      AND episode_id=NEW.episode_id AND id<>NEW.id
      AND episode_revision<=NEW.episode_revision
    ORDER BY episode_revision DESC,id DESC LIMIT 1;
    IF prior.id IS NULL THEN
        IF proof->>'root_operation_id' IS DISTINCT FROM NEW.id::text
           OR proof->>'root_revision' IS DISTINCT FROM NEW.episode_revision::text
           OR proof->>'chain_length' IS DISTINCT FROM '1' THEN
            RAISE EXCEPTION 'Access wake first link is unproven' USING ERRCODE='23514';
        END IF;
        root := NEW;
    ELSE
        IF prior.episode_revision<>NEW.episode_revision-1
           OR prior.phase<>'ready' OR prior.closed_at IS NULL
           OR prior.access_rebind_proof IS NULL
           OR prior.access_rebind_proof->>'to_revision' IS DISTINCT FROM NEW.episode_revision::text
           OR prior.access_rebind_proof->>'episode_id' IS DISTINCT FROM NEW.episode_id::text
           OR prior.access_rebind_proof->>'wait_key' IS DISTINCT FROM proof->>'wait_key'
           OR prior.access_rebind_proof->>'wait_kind' IS DISTINCT FROM proof->>'wait_kind'
           OR prior.access_rebind_proof->>'entered_at' IS DISTINCT FROM proof->>'entered_at'
           OR prior.access_rebind_proof->'successor' IS DISTINCT FROM proof->'predecessor'
           OR proof->>'root_operation_id' IS DISTINCT FROM prior.access_rebind_proof->>'root_operation_id'
           OR proof->>'root_revision' IS DISTINCT FROM prior.access_rebind_proof->>'root_revision'
           OR (proof->>'chain_length')::bigint
                IS DISTINCT FROM (prior.access_rebind_proof->>'chain_length')::bigint+1 THEN
            RAISE EXCEPTION 'Access wake prior link is unproven' USING ERRCODE='23514';
        END IF;
        SELECT * INTO root FROM public.vm_idle_operations
        WHERE id=(proof->>'root_operation_id')::uuid;
        IF root.id IS NULL OR root.owner_id<>NEW.owner_id OR root.episode_id<>NEW.episode_id
           OR root.access_rebind_proof->>'root_operation_id' IS DISTINCT FROM root.id::text
           OR root.access_rebind_proof->>'chain_length' IS DISTINCT FROM '1'
           OR root.episode_revision::text IS DISTINCT FROM proof->>'root_revision' THEN
            RAISE EXCEPTION 'Access wake root link is unproven' USING ERRCODE='23514';
        END IF;
    END IF;
    IF NEW.wake_generation=root.provision_generation
       OR (proof->>'chain_length')::bigint<>NEW.episode_revision-root.episode_revision+1 THEN
        RAISE EXCEPTION 'Access wake lineage cycle or gap' USING ERRCODE='23514';
    END IF;
    SELECT * INTO current_job FROM public.jobs WHERE id=NEW.owner_id FOR SHARE;
    IF current_job.id IS NULL
       OR current_job.workspace_idle_revision<>NEW.episode_revision+1
       OR current_job.workspace_idle_episode->>'episode_id'<>NEW.episode_id::text
       OR current_job.workspace_idle_episode->>'wait_key' IS DISTINCT FROM proof->>'wait_key'
       OR current_job.workspace_idle_episode->>'wait_kind' IS DISTINCT FROM proof->>'wait_kind'
       OR current_job.workspace_idle_episode->>'entered_at' IS DISTINCT FROM proof->>'entered_at'
       OR current_job.workspace_idle_episode->'runtime_identity'->>'runtime_generation'
            IS DISTINCT FROM proof->'successor'->>'generation'
       OR current_job.workspace_idle_episode->'runtime_identity'->>'runtime_uid'
            IS DISTINCT FROM proof->'successor'->>'vm_uid'
       OR current_job.context->'vm'->>'vm_uid' IS DISTINCT FROM proof->'successor'->>'vm_uid'
       OR current_job.context->'vm'->>'vmi_uid' IS DISTINCT FROM proof->'successor'->>'vmi_uid'
       OR current_job.context->'vm'->>'active_pod_uid' IS DISTINCT FROM proof->'successor'->>'launcher_uid'
       OR current_job.context->'vm'->>'rootdisk_pvc_uid' IS DISTINCT FROM NEW.pvc_uid::text
       OR current_job.context->'vm'->>'idle_wake_operation_id' IS DISTINCT FROM NEW.id::text
       OR current_job.context->'vm'->>'status'<>'ready' THEN
        RAISE EXCEPTION 'Access wake lineage successor is unproven' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER vm_idle_access_rebind_guard
    BEFORE INSERT OR UPDATE OR DELETE ON public.vm_idle_operations
    FOR EACH ROW EXECUTE FUNCTION public.guard_vm_idle_access_rebind();

COMMIT;
