-- migration:     0300z_restore_upstream_warm_binding_validator.sql
-- description:   Put 0200's warm-binding validator back where R3.2's superseded
--                variant replaced it, so 0301 can extend it as published.
-- depends-on:    0200_pinned_agent_recycle_authority.sql
-- expected:      < 1s. At most one CREATE OR REPLACE of a trigger function.
-- transactional: yes
-- rollout:       Interstitial. A database that applied the local R3.2 migration
--                0287/0302_permanent_retirement_releases_warm_protection.sql
--                holds a validator whose `releasing` branch 0301's text
--                replacement refuses ("warm release reciprocity branch
--                drifted"). This file runs before 0301 there and restores
--                0200's exact body (same function OID, owner, privileges and
--                trigger bindings); 0301 then adds `terminal_release`, which
--                supersedes the R3.2 variant. Everywhere else it is a no-op:
--                before 0301 on a fresh or upstream database, and after 0301
--                on a database that reached it first. Any other body is
--                refused and nothing changes.

BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '15min';

DO $migration$
DECLARE
    body_md5 text;
BEGIN
    SELECT pg_catalog.md5(p.prosrc) INTO body_md5
      FROM pg_catalog.pg_proc p
     WHERE p.oid = 'public.validate_thread_agent_warm_binding_protection()'::regprocedure;
    -- 0200's body, or 0200's body as 0301 extends it: nothing to restore.
    IF body_md5 IN ('495a2a7154951cc575abae52d9f0b4a1', 'd78e782f395b8c4ff6b9311e547df46f') THEN
        RETURN;
    END IF;
    -- Only the R3.2 body (0287/0302_permanent_retirement_releases_warm_protection.sql).
    IF body_md5 IS DISTINCT FROM '978c07929384cd2d5514fad793d07c11' THEN
        RAISE EXCEPTION 'warm binding validator drifted: %', body_md5;
    END IF;
    EXECUTE $restore$
CREATE OR REPLACE FUNCTION public.validate_thread_agent_warm_binding_protection()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    thread_row public.threads%ROWTYPE;
    agent_row public.agents%ROWTYPE;
    marker jsonb;
BEGIN
    SELECT * INTO thread_row FROM public.threads WHERE id = NEW.thread_id;
    SELECT * INTO agent_row FROM public.agents WHERE id = NEW.agent_id;
    marker := COALESCE(thread_row.metadata->'agent_pod', '{}'::jsonb);

    IF NEW.status IN ('planned', 'protecting', 'protected')
       AND NEW.source = 'attach' THEN
        IF thread_row.id IS NULL
           OR thread_row.runtime_generation IS DISTINCT FROM NEW.runtime_generation
           OR thread_row.runtime_retirement_token IS NOT NULL
           OR thread_row.agent_id IS NOT NULL
           OR thread_row.runtime_attach_token IS NOT NULL
           OR agent_row.id IS NULL
           OR agent_row.thread_id IS NOT NULL
           OR agent_row.current_job_id IS NOT NULL
           OR agent_row.status::text <> 'draining' THEN
            RAISE EXCEPTION 'warm attach plan is not reciprocal'
                USING ERRCODE = '23514',
                      CONSTRAINT = 'thread_agent_warm_binding_reciprocity';
        END IF;
    ELSIF NEW.status IN ('planned', 'protecting', 'protected')
          AND NEW.source = 'legacy_binding' THEN
        IF thread_row.id IS NULL
           OR thread_row.runtime_generation IS DISTINCT FROM NEW.runtime_generation
           OR thread_row.runtime_retirement_token IS NOT NULL
           OR thread_row.agent_id IS DISTINCT FROM NEW.agent_id
           OR thread_row.runtime_attach_token
                IS DISTINCT FROM NEW.runtime_attach_token
           OR agent_row.id IS NULL
           OR agent_row.thread_id IS DISTINCT FROM NEW.thread_id
           OR agent_row.status::text <> 'session' THEN
            RAISE EXCEPTION 'legacy warm binding plan is not reciprocal'
                USING ERRCODE = '23514',
                      CONSTRAINT = 'thread_agent_warm_binding_reciprocity';
        END IF;
    ELSIF NEW.status = 'bound' THEN
        IF thread_row.id IS NULL
           OR thread_row.runtime_generation IS DISTINCT FROM NEW.runtime_generation
           OR thread_row.agent_id IS DISTINCT FROM NEW.agent_id
           OR thread_row.runtime_attach_token
                IS DISTINCT FROM NEW.runtime_attach_token
           OR agent_row.id IS NULL
           OR agent_row.thread_id IS DISTINCT FROM NEW.thread_id
           OR agent_row.status::text <> 'session'
           OR marker->>'warm_binding_protection'
                IS DISTINCT FROM NEW.protection_id::text
           OR marker->>'pod_name' IS DISTINCT FROM NEW.pod_name
           OR marker->>'pod_uid' IS DISTINCT FROM NEW.pod_uid
           OR marker->>'runtime_generation'
                IS DISTINCT FROM NEW.runtime_generation::text
           OR marker->>'namespace' IS DISTINCT FROM NEW.namespace
           OR marker->>'protection_protocol' <> 'finalizer_v1' THEN
            RAISE EXCEPTION 'warm binding publication is not reciprocal'
                USING ERRCODE = '23514',
                      CONSTRAINT = 'thread_agent_warm_binding_reciprocity';
        END IF;
    ELSIF NEW.status = 'releasing' THEN
        IF thread_row.id IS NULL
           OR thread_row.agent_id IS NOT DISTINCT FROM NEW.agent_id
           OR agent_row.id IS NULL
           OR agent_row.thread_id IS NOT NULL
           OR agent_row.status::text <> 'draining' THEN
            RAISE EXCEPTION 'warm binding release is not fenced'
                USING ERRCODE = '23514',
                      CONSTRAINT = 'thread_agent_warm_binding_reciprocity';
        END IF;
    ELSIF NEW.status IN ('released', 'aborted') THEN
        IF thread_row.id IS NOT NULL
           AND thread_row.agent_id IS NOT DISTINCT FROM NEW.agent_id THEN
            RAISE EXCEPTION 'released warm Pod remains thread authority'
                USING ERRCODE = '23514',
                      CONSTRAINT = 'thread_agent_warm_binding_reciprocity';
        END IF;
        IF agent_row.id IS NOT NULL AND (
            agent_row.thread_id IS NOT NULL
            OR agent_row.status::text NOT IN ('ready', 'offline')
        ) THEN
            RAISE EXCEPTION 'released warm Pod remains reserved'
                USING ERRCODE = '23514',
                      CONSTRAINT = 'thread_agent_warm_binding_reciprocity';
        END IF;
    END IF;
    RETURN NULL;
END;
$$
$restore$;
END;
$migration$;

COMMIT;
