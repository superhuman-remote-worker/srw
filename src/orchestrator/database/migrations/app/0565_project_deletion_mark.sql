-- migration:     0565_project_deletion_mark.sql
-- description:   A project delete is decided once and then finished, even
--                when the request dies or an external service is down.
--                The decision transaction sets projects.deletion_requested_at
--                and writes a project_deletions row. From then on nothing
--                that would make the row delete refuse can land in that
--                project (project_deletion_in_progress, SQLSTATE 23514): new
--                or revived jobs and sessions, executions that use its
--                resources, workspace instances, repositories, live resource
--                writes, and expert pointers to its Experts. A leader-gated
--                sweeper finishes the teardown from the project_deletions
--                row, which outlives the project row and records what is
--                still left outside the database.
-- depends-on:    0564_suspended_thread_preflight_abort_keeps_generation.sql
-- expected:      < 1s (one nullable column, one new table, four functions,
--                fourteen triggers).
-- locks:         ACCESS EXCLUSIVE on projects for the catalog-only ADD COLUMN;
--                SHARE ROW EXCLUSIVE on each fenced table for CREATE TRIGGER.
--                Each is brief.
-- transactional: yes
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE public.projects ADD COLUMN deletion_requested_at timestamptz;

COMMENT ON COLUMN public.projects.deletion_requested_at IS
    'Set by the transaction that decides a project delete (owner or admin, '
    'refusal checks passed). Never cleared: the request or the project '
    'deletion sweeper finishes the delete, from project_deletions. While set, '
    'nothing that would refuse the row delete can land in the project '
    '(project_deletion_in_progress).';

COMMENT ON COLUMN public.projects.status IS
    'Project lifecycle: active|archived, enforced by valid_project_status since '
    '0169. Archiving is an explicit owner action and hides the project from the '
    'default GET /api/projects list (?status= opts it back in). Deletion is not '
    'a status: a decided delete sets deletion_requested_at until the row is '
    'gone (0565). The column remains NULLABLE on purpose — a CHECK passes on '
    'NULL, and the API fails toward showing an unclassifiable row rather than '
    'hiding it.';

CREATE TABLE public.project_deletions (
    -- No foreign key: the row outlives the project it deletes.
    project_id uuid PRIMARY KEY,
    requested_at timestamptz NOT NULL DEFAULT now(),
    requested_by uuid,
    row_deleted_at timestamptz,
    -- What the external steps need once the project row is gone: the main
    -- cloud backend, its installation and the folder handle.
    targets jsonb NOT NULL DEFAULT '{}'::jsonb,
    pending_steps text[] NOT NULL DEFAULT '{}'::text[],
    attempts integer NOT NULL DEFAULT 0,
    last_error text,
    -- A claim pushes this into the future (a lease); a failed pass sets the
    -- backoff. The sweeper takes rows that are due.
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz,
    CONSTRAINT project_deletions_steps_check CHECK (
        pending_steps <@ ARRAY['index', 'keycloak_group', 'cloud_folder']::text[]
    ),
    CONSTRAINT project_deletions_completed_check CHECK (
        completed_at IS NULL
        OR (row_deleted_at IS NOT NULL AND cardinality(pending_steps) = 0)
    )
);

CREATE INDEX project_deletions_due_idx
    ON public.project_deletions (next_attempt_at)
    WHERE completed_at IS NULL;

COMMENT ON TABLE public.project_deletions IS
    'One row per decided project delete. Before row_deleted_at the project row '
    'still exists and the teardown removes its managed repositories, folder '
    'grants and row; after it, pending_steps names the external state still '
    'to remove (search index and watermark, Keycloak group, main-cloud '
    'folder). completed_at closes the row; the sweeper drops closed rows '
    'after 30 days.';

-- The fence. The decision transaction takes the catalog lock, then the
-- project row FOR UPDATE, and only then runs its refusal checks and sets the
-- mark. A writer either commits first (the checks see its work and refuse
-- the delete) or reads the committed mark here and is refused.
--
-- with_lock: FOR SHARE on the project rows, which waits for an open
-- decision. Inserts use it; their writers hold the catalog lock or nothing
-- the decision locks, so the wait cannot deadlock. Updates of existing work
-- read without it: the decision locks the rows it classifies FOR UPDATE, so
-- the writer's row lock already orders the two.
CREATE FUNCTION public.refuse_if_project_deleting(
    project_ids uuid[], with_lock boolean
)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    marked boolean;
BEGIN
    IF project_ids IS NULL OR cardinality(project_ids) = 0 THEN
        RETURN;
    END IF;
    IF with_lock THEN
        -- Lock first, in a statement of its own: a filter on the mark in the
        -- locking query is pushed below the lock and skips a row that an
        -- open decision is marking, without waiting for it.
        PERFORM 1
           FROM public.projects AS p
          WHERE p.id = ANY(project_ids)
          ORDER BY p.id
            FOR SHARE;
    END IF;
    -- A new statement, so a new snapshot: it sees a decision that committed
    -- while the lock above waited.
    SELECT EXISTS (
        SELECT 1 FROM public.projects AS p
         WHERE p.id = ANY(project_ids)
           AND p.deletion_requested_at IS NOT NULL
    ) INTO marked;
    IF marked THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            CONSTRAINT = 'project_deletion_in_progress',
            MESSAGE = 'This project is being deleted',
            HINT = 'A project accepts no new work or changes while it is deleted.';
    END IF;
END;
$$;

-- Rows that carry project_id. TG_ARGV[0] = 'lock' for inserts.
CREATE FUNCTION public.fence_project_deletion()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM public.refuse_if_project_deleting(
        ARRAY[NEW.project_id], TG_ARGV[0] = 'lock'
    );
    RETURN NEW;
END;
$$;

-- An execution names its projects, and the resources it was resolved from
-- may belong to another project (work in B using A's Expert).
CREATE FUNCTION public.fence_project_deletion_execution()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM public.refuse_if_project_deleting(
        ARRAY(
            SELECT DISTINCT ids.project_id FROM (
                SELECT unnest(NEW.project_ids) AS project_id
                UNION
                SELECT r.project_id FROM public.srw_resources AS r
                 WHERE r.project_id IS NOT NULL
                   AND (r.id = NEW.resource_id
                        OR r.id IN (
                            SELECT (d->>'uid')::uuid
                              FROM jsonb_array_elements(
                                       CASE WHEN jsonb_typeof(NEW.dependencies) = 'array'
                                            THEN NEW.dependencies
                                            ELSE '[]'::jsonb END) AS d
                             WHERE d->>'uid' ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'))
            ) AS ids
             WHERE ids.project_id IS NOT NULL
        ),
        true
    );
    RETURN NEW;
END;
$$;

-- A default, automation or another project's default naming an Expert that
-- a project being deleted defines.
CREATE FUNCTION public.fence_project_deletion_expert_pointer()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    pointer_project uuid;
BEGIN
    IF TG_TABLE_NAME = 'project_experts' THEN
        pointer_project := NEW.project_id;
    END IF;
    PERFORM public.refuse_if_project_deleting(
        ARRAY(
            SELECT DISTINCT r.project_id FROM public.srw_resources AS r
             WHERE r.kind = 'Expert'
               AND r.linked_id = NEW.expert_id
               AND r.deleted_at IS NULL
               AND r.project_id IS NOT NULL
               AND r.project_id IS DISTINCT FROM pointer_project
        ),
        true
    );
    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_jobs_project_deletion_insert
BEFORE INSERT ON public.jobs
FOR EACH ROW WHEN (NEW.project_id IS NOT NULL)
EXECUTE FUNCTION public.fence_project_deletion('lock');

-- Moving a job into the project, or reviving a finished one (Resume, assign
-- and VM-creation retry take failed and cancelled jobs back).
CREATE TRIGGER trg_jobs_project_deletion_update
BEFORE UPDATE OF project_id, status ON public.jobs
FOR EACH ROW WHEN (
    NEW.project_id IS NOT NULL
    AND (
        NEW.project_id IS DISTINCT FROM OLD.project_id
        OR (
            OLD.status IN ('completed', 'failed', 'cancelled')
            AND NEW.status NOT IN ('completed', 'failed', 'cancelled')
        )
    )
)
EXECUTE FUNCTION public.fence_project_deletion('read');

CREATE TRIGGER trg_threads_project_deletion_insert
BEFORE INSERT ON public.threads
FOR EACH ROW WHEN (NEW.project_id IS NOT NULL)
EXECUTE FUNCTION public.fence_project_deletion('lock');

CREATE TRIGGER trg_threads_project_deletion_update
BEFORE UPDATE OF project_id, status ON public.threads
FOR EACH ROW WHEN (
    NEW.project_id IS NOT NULL
    AND (
        NEW.project_id IS DISTINCT FROM OLD.project_id
        OR (OLD.status = 'ended' AND NEW.status <> 'ended')
    )
)
EXECUTE FUNCTION public.fence_project_deletion('read');

CREATE TRIGGER trg_execution_specs_project_deletion
BEFORE INSERT OR UPDATE OF project_ids, resource_id, dependencies
ON public.srw_execution_specs
FOR EACH ROW
EXECUTE FUNCTION public.fence_project_deletion_execution();

CREATE TRIGGER trg_workspace_instances_project_deletion_insert
BEFORE INSERT ON public.srw_workspace_instances
FOR EACH ROW WHEN (NEW.project_id IS NOT NULL)
EXECUTE FUNCTION public.fence_project_deletion('lock');

CREATE TRIGGER trg_workspace_instances_project_deletion_update
BEFORE UPDATE OF project_id ON public.srw_workspace_instances
FOR EACH ROW WHEN (
    NEW.project_id IS NOT NULL AND NEW.project_id IS DISTINCT FROM OLD.project_id
)
EXECUTE FUNCTION public.fence_project_deletion('read');

-- A repository added after the teardown listed them would be orphaned in
-- the forge when its row cascades away.
CREATE TRIGGER trg_project_repositories_project_deletion
BEFORE INSERT ON public.project_repositories
FOR EACH ROW
EXECUTE FUNCTION public.fence_project_deletion('lock');

-- Retirement clears project_id and sets deleted_at; only a live row that
-- stays in the project is refused (a manifest apply, a Project resource
-- bump from a REST edit, an Officer edit or a connector refresh).
CREATE TRIGGER trg_resources_project_deletion_insert
BEFORE INSERT ON public.srw_resources
FOR EACH ROW WHEN (NEW.project_id IS NOT NULL AND NEW.deleted_at IS NULL)
EXECUTE FUNCTION public.fence_project_deletion('lock');

CREATE TRIGGER trg_resources_project_deletion_update
BEFORE UPDATE ON public.srw_resources
FOR EACH ROW WHEN (NEW.project_id IS NOT NULL AND NEW.deleted_at IS NULL)
EXECUTE FUNCTION public.fence_project_deletion('read');

CREATE TRIGGER trg_application_expert_defaults_project_deletion
BEFORE INSERT OR UPDATE OF expert_id ON public.application_expert_defaults
FOR EACH ROW WHEN (NEW.expert_id IS NOT NULL)
EXECUTE FUNCTION public.fence_project_deletion_expert_pointer();

CREATE TRIGGER trg_user_expert_defaults_project_deletion
BEFORE INSERT OR UPDATE OF expert_id ON public.user_expert_defaults
FOR EACH ROW WHEN (NEW.expert_id IS NOT NULL)
EXECUTE FUNCTION public.fence_project_deletion_expert_pointer();

CREATE TRIGGER trg_automations_project_deletion
BEFORE INSERT OR UPDATE OF expert_id ON public.automations
FOR EACH ROW WHEN (NEW.expert_id IS NOT NULL)
EXECUTE FUNCTION public.fence_project_deletion_expert_pointer();

CREATE TRIGGER trg_project_experts_project_deletion
BEFORE INSERT OR UPDATE OF expert_id, default_for ON public.project_experts
FOR EACH ROW WHEN (NEW.default_for IS NOT NULL)
EXECUTE FUNCTION public.fence_project_deletion_expert_pointer();

COMMENT ON FUNCTION public.refuse_if_project_deleting(uuid[], boolean) IS
    'Raises project_deletion_in_progress (23514) when any of the projects '
    'carries deletion_requested_at. with_lock reads them FOR SHARE.';

COMMENT ON FUNCTION public.fence_project_deletion() IS
    'Fence of a decided project delete for rows that carry project_id.';

COMMENT ON FUNCTION public.fence_project_deletion_execution() IS
    'Fence of a decided project delete for executions: their projects and '
    'the projects of the resources they were resolved from.';

COMMENT ON FUNCTION public.fence_project_deletion_expert_pointer() IS
    'Fence of a decided project delete for pointers to its Experts from '
    'outside it.';

COMMIT;
