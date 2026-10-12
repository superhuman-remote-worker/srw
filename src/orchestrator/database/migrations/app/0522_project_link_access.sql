-- migration:     0522_project_link_access.sql
-- description:   A project's default access level per connector link (main
--                cloud as connectors, slice 5, "Access levels"). The level id
--                of the connector's driver a project's work binds it at when
--                the work names none: for a cloud folder read_only, protected
--                or read_write. NULL: the driver's own default. Only drivers
--                whose levels say what they let through (strict drivers) take
--                one; their links keep read_only NULL, which the second CHECK
--                holds. Every other driver keeps the read_only boolean. A
--                default only: a session may still choose another offered
--                level, and a default the workspace cannot have is lowered,
--                never raised. Both CHECKs are added NOT VALID and validated
--                by 0523.
-- depends-on:    0520_cloud_folder_grants.sql
-- expected:      < 1s (a nullable column without a default: catalog only).
-- locks:         brief ACCESS EXCLUSIVE on project_datasources.
-- transactional: yes

BEGIN;
SET LOCAL lock_timeout                        = '2s';
SET LOCAL statement_timeout                   = '30s';
SET LOCAL idle_in_transaction_session_timeout = '5min';

ALTER TABLE public.project_datasources ADD COLUMN access TEXT;

ALTER TABLE public.project_datasources
    ADD CONSTRAINT project_datasources_access_check
        CHECK (access IS NULL OR access ~ '^[A-Za-z][A-Za-z0-9_]{0,62}$')
        NOT VALID;

-- A link carries a level or the read-only boolean, never both.
ALTER TABLE public.project_datasources
    ADD CONSTRAINT project_datasources_access_or_read_only_check
        CHECK (access IS NULL OR read_only IS NULL)
        NOT VALID;

COMMENT ON COLUMN public.project_datasources.access IS
    'The project''s default level for this connector: a level id of its '
    'driver (cloud_folder: read_only, protected, read_write). NULL: the '
    'driver''s default. Only for drivers whose levels declare writes; their '
    'links keep read_only NULL. A default only, never a ceiling.';

COMMIT;
