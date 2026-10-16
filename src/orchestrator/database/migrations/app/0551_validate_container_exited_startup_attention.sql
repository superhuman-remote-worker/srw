-- migration:     0551_validate_container_exited_startup_attention.sql
-- description:   Validate the container_exited startup shape after expansion.
-- depends-on:    0550_container_exited_startup_attention.sql
-- transactional: yes

BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '15min';

ALTER TABLE public.managed_repository_workspace_creation_reservations
    VALIDATE CONSTRAINT managed_workspace_startup_stage_shape_check_v2;

COMMIT;
