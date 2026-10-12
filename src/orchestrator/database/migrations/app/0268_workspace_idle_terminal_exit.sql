-- migration: 0268_workspace_idle_terminal_exit.sql
-- description: Close native idle episodes in every committed terminal Job statement.
-- depends-on: 0267_vm_resource_waiter_parking.sql
-- transactional: yes
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

-- Replace the existing function so correctness never depends on trigger name
-- ordering. Thread semantics and all source/lifecycle authority stay unchanged.
CREATE OR REPLACE FUNCTION public.guard_workspace_idle_episode() RETURNS trigger LANGUAGE plpgsql AS $$
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
    -- Status authorization belongs to the original writer and its other
    -- guards. Normalize only an unchanged prior episode, inside that exact
    -- terminal statement. Never hide an explicit invalid metadata mutation.
    IF TG_TABLE_NAME='jobs' AND NEW.status::text IN ('completed','failed','cancelled') THEN
        IF NEW.workspace_idle_episode IS NOT NULL THEN
            IF OLD.workspace_idle_episode IS NULL
               OR NEW.workspace_idle_episode IS DISTINCT FROM OLD.workspace_idle_episode
               OR NEW.workspace_idle_revision<>OLD.workspace_idle_revision THEN
                RAISE EXCEPTION 'Terminal Jobs cannot publish an idle episode' USING ERRCODE='23514';
            END IF;
            NEW.workspace_idle_episode := NULL;
            NEW.workspace_idle_revision := OLD.workspace_idle_revision+1;
        ELSIF OLD.workspace_idle_episode IS NOT NULL THEN
            IF NEW.workspace_idle_revision<>OLD.workspace_idle_revision+1 THEN
                RAISE EXCEPTION 'Terminal idle exit revision must advance exactly once' USING ERRCODE='23514';
            END IF;
        ELSIF NEW.workspace_idle_revision<>OLD.workspace_idle_revision THEN
            RAISE EXCEPTION 'Terminal replay cannot advance idle revision' USING ERRCODE='23514';
        END IF;
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
COMMIT;
