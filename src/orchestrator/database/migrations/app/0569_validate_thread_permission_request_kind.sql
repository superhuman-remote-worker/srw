-- migration:     0569_validate_thread_permission_request_kind.sql
-- description:   Validate the permission request kind check after the expand step.
-- depends-on:    0568_thread_permission_request_kind.sql
-- transactional: yes

BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '15min';

ALTER TABLE public.thread_permission_requests
    VALIDATE CONSTRAINT thread_permission_requests_kind_check;

COMMIT;
