-- migration:     0550_container_exited_startup_attention.sql
-- description:   Add Job container_exited startup attention with a frozen exit code.
-- depends-on:    0549_main_cloud_service_credentials.sql
-- expected:      < 5s. One nullable column, a NOT VALID check swap and one small guard trigger; no backfill.
-- transactional: yes

BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '15min';

ALTER TABLE public.managed_repository_workspace_creation_reservations
    ADD COLUMN startup_exit_code integer;

-- The 0309 shape, plus readiness/attention/container_exited. Its exit code is
-- present exactly for that reason, which is never after a first Ready.
ALTER TABLE public.managed_repository_workspace_creation_reservations
    ADD CONSTRAINT managed_workspace_startup_stage_shape_check_v2 CHECK (COALESCE(
        ((startup_protocol_version IS NULL
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
                             'pull_deadline', 'readiness_deadline', 'ssh_deadline',
                             'container_exited')
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
                             ready_budget_seconds, COALESCE(pull_budget_seconds, 0)))))))))
        AND (startup_exit_code IS NULL
             OR startup_reason_code IS NOT DISTINCT FROM 'container_exited')
        AND (startup_reason_code IS DISTINCT FROM 'container_exited'
             OR (startup_first_ready_at IS NULL AND startup_exit_code IS NOT NULL)),
        FALSE))
    NOT VALID;

ALTER TABLE public.managed_repository_workspace_creation_reservations
    DROP CONSTRAINT managed_workspace_startup_stage_shape_check;

-- A separate guard: the 0309 receipt trigger function stays untouched.
-- Attention is already sticky there; this freezes the exit code too.
CREATE FUNCTION public.freeze_container_startup_exit_code()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.startup_exit_code IS NOT NULL
       AND NEW.startup_exit_code IS DISTINCT FROM OLD.startup_exit_code THEN
        RAISE EXCEPTION 'container startup exit code is immutable'
            USING ERRCODE = '23514',
                  CONSTRAINT = 'managed_workspace_startup_stage_authority';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_container_startup_exit_code_frozen
BEFORE UPDATE ON public.managed_repository_workspace_creation_reservations
FOR EACH ROW EXECUTE FUNCTION public.freeze_container_startup_exit_code();

COMMIT;
