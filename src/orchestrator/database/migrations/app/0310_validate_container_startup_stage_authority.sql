-- migration:     0310_validate_container_startup_stage_authority.sql
-- description:   Validate the nullable container startup protocol shape after expansion.
-- depends-on:    0309_container_startup_stage_authority.sql
-- transactional: yes

BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '15min';

ALTER TABLE public.managed_repository_workspace_creation_reservations
    VALIDATE CONSTRAINT managed_workspace_startup_stage_shape_check;

COMMIT;
