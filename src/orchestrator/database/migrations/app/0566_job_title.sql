-- migration:     0566_job_title.sql
-- description:   A job gets a display title of its own. Until now the task
--                text (jobs.description) was also the job's name in every
--                list, so a name could not be changed without changing what
--                the agent was asked to do. jobs.title is the name a person
--                gave the job afterwards; NULL means "not named", and readers
--                then show the description as before. Nothing writes it at
--                creation: the Cockpit sets it through PATCH /api/jobs/{id}.
-- depends-on:    0565_project_deletion_mark.sql
-- expected:      < 1s (one nullable column, no default, no rewrite).
-- locks:         ACCESS EXCLUSIVE on jobs for the catalog-only ADD COLUMN,
--                brief.
-- transactional: yes
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE public.jobs ADD COLUMN IF NOT EXISTS title text;

COMMENT ON COLUMN public.jobs.title IS
    'Display name a person gave the job after creating it (PATCH '
    '/api/jobs/{id}, at most 200 characters, enforced by the API). NULL means '
    'the job was never named: readers show jobs.description instead. The '
    'agent never sees it; the task text stays in description.';

COMMIT;
