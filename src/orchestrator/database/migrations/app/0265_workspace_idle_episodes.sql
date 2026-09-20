-- migration: 0265_workspace_idle_episodes.sql
-- description: Fenced human-wait metadata only; physical idle release remains disabled.
-- depends-on: 0264_vm_resource_reservations.sql
-- transactional: yes
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

-- Constant defaults use PostgreSQL's metadata-only column addition. Historical
-- owners remain without an episode; never backdate from legacy activity fields.
ALTER TABLE public.jobs
    ADD COLUMN workspace_idle_revision BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN workspace_idle_episode JSONB;
ALTER TABLE public.threads
    ADD COLUMN workspace_idle_revision BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN workspace_idle_episode JSONB;
ALTER TABLE public.jobs ADD CONSTRAINT jobs_workspace_idle_shape CHECK (
    workspace_idle_revision >= 0 AND (workspace_idle_episode IS NULL OR
    (jsonb_typeof(workspace_idle_episode)='object' AND octet_length(workspace_idle_episode::text)<=4096))
) NOT VALID;
ALTER TABLE public.threads ADD CONSTRAINT threads_workspace_idle_shape CHECK (
    workspace_idle_revision >= 0 AND (workspace_idle_episode IS NULL OR
    (jsonb_typeof(workspace_idle_episode)='object' AND octet_length(workspace_idle_episode::text)<=4096))
) NOT VALID;

CREATE FUNCTION public.guard_workspace_idle_episode() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    previous JSONB;
    current_episode JSONB;
    owner_kind TEXT;
BEGIN
    IF TG_OP='INSERT' THEN
        IF NEW.workspace_idle_revision<>0 OR NEW.workspace_idle_episode IS NOT NULL THEN
            RAISE EXCEPTION 'Idle episodes require an authorized owner transition' USING ERRCODE='23514';
        END IF;
        RETURN NEW;
    END IF;
    previous := OLD.workspace_idle_episode;
    current_episode := NEW.workspace_idle_episode;
    IF NEW.workspace_idle_revision=OLD.workspace_idle_revision AND current_episode IS NOT DISTINCT FROM previous THEN
        RETURN NEW;
    END IF;
    IF NEW.workspace_idle_revision<>OLD.workspace_idle_revision+1 THEN
        RAISE EXCEPTION 'Idle episode revision must advance exactly once' USING ERRCODE='23514';
    END IF;
    IF current_episode IS NULL THEN
        RETURN NEW;
    END IF;
    owner_kind := CASE WHEN TG_TABLE_NAME='jobs' THEN 'job' ELSE 'thread' END;
    IF current_episode ? 'revision' OR current_episode->'version' IS DISTINCT FROM '1'::jsonb
        OR current_episode->'runtime_identity'->>'owner_kind' IS DISTINCT FROM owner_kind
        OR current_episode->'runtime_identity'->>'owner_id' IS DISTINCT FROM NEW.id::text THEN
        RAISE EXCEPTION 'Idle episode owner identity changed' USING ERRCODE='23514';
    END IF;
    IF previous IS NOT NULL AND previous->'episode_id'=current_episode->'episode_id' THEN
        IF ROW(previous->'wait_kind',previous->'wait_key',previous->'entered_at') IS DISTINCT FROM
           ROW(current_episode->'wait_kind',current_episode->'wait_key',current_episode->'entered_at')
           OR (current_episode->>'extend_count')::bigint < (previous->>'extend_count')::bigint
           OR (previous->>'override_until' IS NOT NULL AND
               (current_episode->>'override_until' IS NULL OR
                (current_episode->>'override_until')::timestamptz < (previous->>'override_until')::timestamptz)) THEN
            RAISE EXCEPTION 'Idle wait identity and age are immutable' USING ERRCODE='23514';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER guard_workspace_idle_episode BEFORE INSERT OR UPDATE ON public.jobs
    FOR EACH ROW EXECUTE FUNCTION public.guard_workspace_idle_episode();
CREATE TRIGGER guard_workspace_idle_episode BEFORE INSERT OR UPDATE ON public.threads
    FOR EACH ROW EXECUTE FUNCTION public.guard_workspace_idle_episode();
COMMIT;
