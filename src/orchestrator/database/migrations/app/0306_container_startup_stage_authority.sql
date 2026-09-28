-- migration:     0306_container_startup_stage_authority.sql
-- description:   Freeze exact container startup stages on current creation receipts.
-- depends-on:    0305_validate_pinned_permanent_warm_release.sql
-- expected:      < 5s. Nullable columns, checks and scoped guards; no backfill.
-- transactional: yes

BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '15min';

ALTER TABLE public.managed_repository_workspace_creation_reservations
    ADD COLUMN startup_protocol_version smallint,
    ADD COLUMN startup_stage text,
    ADD COLUMN startup_state text,
    ADD COLUMN startup_reason_code text,
    ADD COLUMN scheduled_at timestamptz,
    ADD COLUMN startup_first_ready_at timestamptz,
    ADD COLUMN ready_budget_seconds double precision,
    ADD COLUMN pull_budget_seconds double precision,
    ADD COLUMN ssh_budget_seconds double precision,
    ADD COLUMN startup_attention_at timestamptz;

ALTER TABLE public.managed_repository_workspace_creation_reservations
    ADD CONSTRAINT managed_workspace_startup_stage_shape_check CHECK (COALESCE(
        (startup_protocol_version IS NULL
         AND startup_stage IS NULL AND startup_state IS NULL
         AND startup_reason_code IS NULL AND scheduled_at IS NULL
         AND startup_first_ready_at IS NULL AND ready_budget_seconds IS NULL
         AND pull_budget_seconds IS NULL AND ssh_budget_seconds IS NULL
         AND startup_attention_at IS NULL)
        OR
        (startup_protocol_version = 1
         AND owner_kind IN ('job', 'thread')
         AND scope = 'workspace_container' AND operation_kind = 'create'
         AND runtime_incarnation IS NOT NULL AND pod_uid = runtime_incarnation
         AND (
             (startup_stage = 'scheduling'
              AND startup_state IN ('observing', 'waiting_capacity')
              AND ((startup_state = 'observing'
                    AND startup_reason_code IN ('observation_pending', 'scheduling_other'))
                   OR (startup_state = 'waiting_capacity'
                       AND startup_reason_code IN
                           ('scheduler_unschedulable', 'insufficient_capacity')))
              AND scheduled_at IS NULL AND startup_first_ready_at IS NULL
              AND ready_budget_seconds IS NULL AND pull_budget_seconds IS NULL
              AND ssh_budget_seconds IS NULL AND startup_attention_at IS NULL)
             OR
             (startup_stage = 'readiness'
              AND ((startup_state = 'starting' AND startup_reason_code = 'scheduled'
                    AND startup_attention_at IS NULL)
                   OR (startup_state = 'attention'
                       AND startup_reason_code IN
                           ('invalid_image', 'invalid_configuration',
                            'pull_deadline', 'readiness_deadline', 'ssh_deadline')
                       AND startup_attention_at IS NOT NULL))
              AND scheduled_at IS NOT NULL
              AND ready_budget_seconds >= 0.000001
              AND ready_budget_seconds < 31536000
              AND (pull_budget_seconds IS NULL OR
                   (pull_budget_seconds >= 0.000001
                    AND pull_budget_seconds < 31536000))
              AND ssh_budget_seconds >= 0.000001
              AND ssh_budget_seconds < 31536000
              AND (startup_first_ready_at IS NULL OR
                   (startup_first_ready_at >= scheduled_at
                    AND startup_first_ready_at <= scheduled_at +
                        make_interval(secs => GREATEST(
                            ready_budget_seconds, COALESCE(pull_budget_seconds, 0)))))))), FALSE))
    NOT VALID;

CREATE FUNCTION public.enforce_container_startup_stage_receipt()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    owner_runtime jsonb;
    owner_status text;
    owner_generation uuid;
    hard_deadline timestamptz;
    ssh_deadline timestamptz;
BEGIN
    IF TG_OP = 'UPDATE' THEN
        IF OLD.startup_protocol_version = 1 AND (
            NEW.startup_protocol_version IS DISTINCT FROM 1
            OR NEW.scheduled_at IS DISTINCT FROM OLD.scheduled_at
               AND OLD.scheduled_at IS NOT NULL
            OR NEW.startup_first_ready_at IS DISTINCT FROM OLD.startup_first_ready_at
               AND OLD.startup_first_ready_at IS NOT NULL
            OR NEW.ready_budget_seconds IS DISTINCT FROM OLD.ready_budget_seconds
               AND OLD.ready_budget_seconds IS NOT NULL
            OR NEW.pull_budget_seconds IS DISTINCT FROM OLD.pull_budget_seconds
               AND OLD.scheduled_at IS NOT NULL
            OR NEW.ssh_budget_seconds IS DISTINCT FROM OLD.ssh_budget_seconds
               AND OLD.ssh_budget_seconds IS NOT NULL
            OR OLD.startup_state = 'attention' AND
               (NEW.startup_state IS DISTINCT FROM OLD.startup_state
                OR NEW.startup_reason_code IS DISTINCT FROM OLD.startup_reason_code
                OR NEW.startup_attention_at IS DISTINCT FROM OLD.startup_attention_at)
            OR NEW.scope IS DISTINCT FROM OLD.scope
            OR NEW.operation_kind IS DISTINCT FROM OLD.operation_kind
            OR NEW.pod_uid IS DISTINCT FROM OLD.pod_uid
            OR NEW.runtime_incarnation IS DISTINCT FROM OLD.runtime_incarnation
        ) THEN
            RAISE EXCEPTION 'container startup authority is immutable'
                USING ERRCODE = '23514',
                      CONSTRAINT = 'managed_workspace_startup_stage_authority';
        END IF;
    END IF;
    IF NEW.startup_protocol_version IS DISTINCT FROM 1 THEN
        RETURN NEW;
    END IF;
    IF NEW.phase NOT IN ('runtime_bound', 'settled', 'aborted')
       OR (TG_OP = 'INSERT' OR OLD.startup_protocol_version IS DISTINCT FROM 1)
          AND (NEW.phase <> 'runtime_bound' OR NEW.settled_at IS NOT NULL
               OR NEW.cancel_requested_at IS NOT NULL) THEN
        RAISE EXCEPTION 'container startup requires an open bound receipt'
            USING ERRCODE = '23514',
                  CONSTRAINT = 'managed_workspace_startup_stage_authority';
    END IF;
    IF NEW.cancel_requested_at IS NOT NULL THEN
        -- Native Cancel rotates the claim, then updates the owner projection
        -- in its owner-ordered transaction. Its physical cleanup/abort path
        -- must retain the receipt and does not grant a later Ready.
        RETURN NEW;
    END IF;
    IF TG_OP = 'UPDATE' AND OLD.startup_protocol_version = 1
       AND NEW.startup_stage IS NOT DISTINCT FROM OLD.startup_stage
       AND NEW.startup_state IS NOT DISTINCT FROM OLD.startup_state
       AND NEW.startup_reason_code IS NOT DISTINCT FROM OLD.startup_reason_code
       AND NEW.scheduled_at IS NOT DISTINCT FROM OLD.scheduled_at
       AND NEW.startup_first_ready_at IS NOT DISTINCT FROM OLD.startup_first_ready_at
       AND NEW.ready_budget_seconds IS NOT DISTINCT FROM OLD.ready_budget_seconds
       AND NEW.pull_budget_seconds IS NOT DISTINCT FROM OLD.pull_budget_seconds
       AND NEW.ssh_budget_seconds IS NOT DISTINCT FROM OLD.ssh_budget_seconds
       AND NEW.startup_attention_at IS NOT DISTINCT FROM OLD.startup_attention_at
       AND NEW.phase = 'runtime_bound' THEN
        -- An existing same-generation claim rotates the receipt token before
        -- updating the owner's token. Check the final pair at commit instead.
        RETURN NEW;
    END IF;

    IF NEW.owner_kind = 'job' THEN
        SELECT context->'workspace_container', status::text
          INTO owner_runtime, owner_status
          FROM public.jobs WHERE id = NEW.owner_id;
        IF owner_status IN ('completed', 'failed', 'cancelled') THEN
            RAISE EXCEPTION 'container startup owner is terminal'
                USING ERRCODE = '23514',
                      CONSTRAINT = 'managed_workspace_startup_stage_authority';
        END IF;
    ELSE
        SELECT metadata->'workspace_container', status::text, runtime_generation
          INTO owner_runtime, owner_status, owner_generation
          FROM public.threads WHERE id = NEW.owner_id;
        IF owner_status = 'ended'
           OR owner_generation IS DISTINCT FROM NEW.thread_runtime_generation THEN
            RAISE EXCEPTION 'container startup owner generation changed'
                USING ERRCODE = '23514',
                      CONSTRAINT = 'managed_workspace_startup_stage_authority';
        END IF;
    END IF;
    IF owner_runtime IS NULL
       OR owner_runtime->>'_runtime_incarnation' IS DISTINCT FROM NEW.pod_uid::text
       OR owner_runtime->>'_creation_reservation_id' IS DISTINCT FROM NEW.id::text
       OR owner_runtime->>'_creation_claim_token' IS DISTINCT FROM NEW.claim_token::text THEN
        RAISE EXCEPTION 'container startup owner binding changed'
            USING ERRCODE = '23514',
                  CONSTRAINT = 'managed_workspace_startup_stage_authority';
    END IF;
    IF NEW.startup_state = 'attention' AND
       NEW.startup_reason_code IN ('pull_deadline', 'readiness_deadline') THEN
        hard_deadline := NEW.scheduled_at + make_interval(
            secs => GREATEST(NEW.ready_budget_seconds,
                             COALESCE(NEW.pull_budget_seconds, 0)));
        IF clock_timestamp() <= hard_deadline THEN
            RAISE EXCEPTION 'startup deadline has not elapsed'
                USING ERRCODE = '23514',
                      CONSTRAINT = 'managed_workspace_startup_stage_authority';
        END IF;
    ELSIF NEW.startup_state = 'attention'
          AND NEW.startup_reason_code = 'ssh_deadline' THEN
        hard_deadline := NEW.scheduled_at + make_interval(
            secs => GREATEST(NEW.ready_budget_seconds,
                             COALESCE(NEW.pull_budget_seconds, 0)));
        ssh_deadline := LEAST(
            NEW.startup_first_ready_at + make_interval(secs => NEW.ssh_budget_seconds),
            hard_deadline + make_interval(secs => NEW.ssh_budget_seconds));
        IF NEW.startup_first_ready_at IS NULL
           OR clock_timestamp() <= ssh_deadline THEN
            RAISE EXCEPTION 'startup SSH deadline has not elapsed'
                USING ERRCODE = '23514',
                      CONSTRAINT = 'managed_workspace_startup_stage_authority';
        END IF;
    END IF;
    IF NEW.phase = 'settled' AND OLD.phase IS DISTINCT FROM 'settled' THEN
        hard_deadline := NEW.scheduled_at + make_interval(
            secs => GREATEST(NEW.ready_budget_seconds,
                             COALESCE(NEW.pull_budget_seconds, 0)));
        ssh_deadline := LEAST(
            NEW.startup_first_ready_at + make_interval(secs => NEW.ssh_budget_seconds),
            hard_deadline + make_interval(secs => NEW.ssh_budget_seconds));
        IF current_setting('srw.container_startup_ready_receipt', true)
              IS DISTINCT FROM NEW.id::text
           OR NEW.result_kind IS DISTINCT FROM 'settled'
           OR NEW.startup_stage IS DISTINCT FROM 'readiness'
           OR NEW.startup_state IS DISTINCT FROM 'starting'
           OR NEW.startup_first_ready_at IS NULL
           OR clock_timestamp() >= ssh_deadline
           OR owner_runtime->>'status' IS DISTINCT FROM 'ready' THEN
            RAISE EXCEPTION 'container startup cannot settle before authenticated Ready'
                USING ERRCODE = '23514',
                      CONSTRAINT = 'managed_workspace_startup_stage_authority';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_container_startup_stage_receipt
BEFORE INSERT OR UPDATE ON public.managed_repository_workspace_creation_reservations
FOR EACH ROW EXECUTE FUNCTION public.enforce_container_startup_stage_receipt();

CREATE FUNCTION public.validate_container_startup_current_binding()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    current_receipt public.managed_repository_workspace_creation_reservations%ROWTYPE;
    owner_runtime jsonb;
    owner_generation uuid;
BEGIN
    SELECT * INTO current_receipt
      FROM public.managed_repository_workspace_creation_reservations
     WHERE id = NEW.id;
    IF current_receipt.startup_protocol_version IS DISTINCT FROM 1
       OR current_receipt.phase <> 'runtime_bound'
       OR current_receipt.cancel_requested_at IS NOT NULL
       OR current_receipt.settled_at IS NOT NULL THEN
        RETURN NULL;
    END IF;
    IF current_receipt.owner_kind = 'job' THEN
        SELECT context->'workspace_container' INTO owner_runtime
          FROM public.jobs WHERE id = current_receipt.owner_id;
    ELSE
        SELECT metadata->'workspace_container', runtime_generation
          INTO owner_runtime, owner_generation
          FROM public.threads WHERE id = current_receipt.owner_id;
        IF owner_generation IS DISTINCT FROM
           current_receipt.thread_runtime_generation THEN
            RAISE EXCEPTION 'startup thread generation changed at commit'
                USING ERRCODE = '23514',
                      CONSTRAINT = 'managed_workspace_startup_current_binding';
        END IF;
    END IF;
    IF owner_runtime IS NULL
       OR owner_runtime->>'_runtime_incarnation' IS DISTINCT FROM
          current_receipt.pod_uid::text
       OR owner_runtime->>'_creation_reservation_id' IS DISTINCT FROM
          current_receipt.id::text
       OR owner_runtime->>'_creation_claim_token' IS DISTINCT FROM
          current_receipt.claim_token::text THEN
        RAISE EXCEPTION 'container startup owner binding changed at commit'
            USING ERRCODE = '23514',
                  CONSTRAINT = 'managed_workspace_startup_current_binding';
    END IF;
    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER trg_container_startup_current_binding
AFTER INSERT OR UPDATE ON public.managed_repository_workspace_creation_reservations
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION public.validate_container_startup_current_binding();

CREATE FUNCTION public.enforce_container_startup_owner_ready()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    receipt public.managed_repository_workspace_creation_reservations%ROWTYPE;
    projection jsonb;
    prior_status text;
    hard_deadline timestamptz;
BEGIN
    IF TG_TABLE_NAME = 'jobs' THEN
        projection := NEW.context->'workspace_container';
        prior_status := OLD.context #>> '{workspace_container,status}';
        SELECT * INTO receipt
          FROM public.managed_repository_workspace_creation_reservations
         WHERE owner_kind = 'job' AND owner_id = NEW.id
           AND scope = 'workspace_container' AND startup_protocol_version = 1
           AND settled_at IS NULL
         LIMIT 1;
    ELSE
        projection := NEW.metadata->'workspace_container';
        prior_status := OLD.metadata #>> '{workspace_container,status}';
        SELECT * INTO receipt
          FROM public.managed_repository_workspace_creation_reservations
         WHERE owner_kind = 'thread' AND owner_id = NEW.id
           AND scope = 'workspace_container' AND startup_protocol_version = 1
           AND settled_at IS NULL
         LIMIT 1;
    END IF;
    IF receipt.id IS NULL OR projection->>'status' IS DISTINCT FROM 'ready'
       OR prior_status = 'ready' THEN
        RETURN NEW;
    END IF;
    hard_deadline := receipt.scheduled_at + make_interval(
        secs => GREATEST(receipt.ready_budget_seconds,
                         COALESCE(receipt.pull_budget_seconds, 0)));
    IF current_setting('srw.container_startup_ready_receipt', true)
          IS DISTINCT FROM receipt.id::text
       OR receipt.operation_kind <> 'create'
       OR receipt.phase <> 'runtime_bound' OR receipt.cancel_requested_at IS NOT NULL
       OR receipt.expires_at <= clock_timestamp()
       OR receipt.startup_stage IS DISTINCT FROM 'readiness'
       OR receipt.startup_state IS DISTINCT FROM 'starting'
       OR receipt.startup_first_ready_at IS NULL
       OR clock_timestamp() >= LEAST(
           receipt.startup_first_ready_at + make_interval(secs => receipt.ssh_budget_seconds),
           hard_deadline + make_interval(secs => receipt.ssh_budget_seconds))
       OR projection->>'_runtime_incarnation' IS DISTINCT FROM receipt.pod_uid::text
       OR projection->>'_creation_reservation_id' IS DISTINCT FROM receipt.id::text
       OR projection->>'_creation_claim_token' IS DISTINCT FROM receipt.claim_token::text THEN
        RAISE EXCEPTION 'container startup Ready lacks current stage authority'
            USING ERRCODE = '23514',
                  CONSTRAINT = 'managed_workspace_startup_owner_ready';
    END IF;
    IF TG_TABLE_NAME = 'threads' THEN
        IF NEW.runtime_generation IS DISTINCT FROM
           receipt.thread_runtime_generation THEN
            RAISE EXCEPTION 'container startup Ready generation changed'
                USING ERRCODE = '23514',
                      CONSTRAINT = 'managed_workspace_startup_owner_ready';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_jobs_container_startup_ready
BEFORE UPDATE OF context ON public.jobs
FOR EACH ROW EXECUTE FUNCTION public.enforce_container_startup_owner_ready();
CREATE TRIGGER trg_threads_container_startup_ready
BEFORE UPDATE OF metadata ON public.threads
FOR EACH ROW EXECUTE FUNCTION public.enforce_container_startup_owner_ready();

COMMIT;
