-- migration:     0524_thread_protected_sources.sql
-- description:   A protected session's folder, chosen from its connector
--                (main cloud as connectors, slice 5, "Protected is the
--                connector's level"). One row per protected thread: the
--                project folder bound at the protected level, frozen at
--                admission from the cloud_folder connector's config (the
--                project, its installation, the folder handle verbatim, and
--                projects/<slug> as the target), so a later rename or
--                re-point never changes what a staged review applies to.
--                cloud_ro_mounts.selected_mount_id names this row's id.
--
--                Every protected thread's selected thread_mounts row (the
--                first by target_path that is not project_default, on
--                Nextcloud, with a handle: select_protected_mount) is
--                copied under the SAME id, so every live grant keeps
--                resolving and no cloud_ro_mounts row is written. The 0186
--                selection check reads this table first, with a
--                thread_mounts fallback for one release (an orchestrator
--                that predates it selects from thread_mounts until the
--                startup pass copies the row). A row is immutable.
-- depends-on:    0523_validate_project_link_access.sql
-- expected:      < 1s (one small table; one row per protected thread).
-- locks:         catalog locks; SHARE ROW EXCLUSIVE on threads and the
--                small main_cloud_backend_instances table for the foreign
--                keys; the function replacement takes no table lock.
-- transactional: yes

BEGIN;
SET LOCAL lock_timeout                        = '2s';
SET LOCAL statement_timeout                   = '15min';
SET LOCAL idle_in_transaction_session_timeout = '5min';
SET LOCAL timezone                            = 'UTC';

CREATE TABLE public.thread_protected_sources (
    id                     UUID PRIMARY KEY,
    thread_id              UUID NOT NULL
        REFERENCES public.threads (id) ON DELETE CASCADE,
    -- The cloud_folder connector bound at protected; NULL for a row copied
    -- from thread_mounts (a session protected before connectors).
    datasource_id          UUID,
    mount_kind             TEXT NOT NULL,
    source_kind            TEXT NOT NULL,
    source_ref             UUID NOT NULL,
    target_path            TEXT NOT NULL,
    backend_id             TEXT NOT NULL,
    backend_instance_id    UUID NOT NULL,
    cloud_handle           TEXT NOT NULL,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT thread_protected_sources_one_per_thread UNIQUE (thread_id),
    CONSTRAINT thread_protected_sources_shape_check
        CHECK (mount_kind = 'project' AND source_kind = 'project_folder'
               AND target_path <> '' AND cloud_handle <> ''),
    CONSTRAINT thread_protected_sources_backend_fk
        FOREIGN KEY (backend_instance_id, backend_id)
        REFERENCES public.main_cloud_backend_instances (id, backend_id)
        ON DELETE RESTRICT
);

CREATE FUNCTION public.enforce_thread_protected_source_immutable()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'a protected session''s source is immutable'
        USING ERRCODE = '23514',
              CONSTRAINT = 'thread_protected_sources_immutable';
END;
$$;

CREATE TRIGGER thread_protected_sources_immutable
BEFORE UPDATE ON public.thread_protected_sources
FOR EACH ROW
EXECUTE FUNCTION public.enforce_thread_protected_source_immutable();

-- Carry every protected session's selected row over under the same id.
WITH selected AS (
    SELECT DISTINCT ON (mount.thread_id) mount.*
      FROM public.thread_mounts AS mount
      JOIN public.threads AS thread ON thread.id = mount.thread_id
     WHERE thread.metadata->'protected_cloud' = 'true'::jsonb
       AND mount.mount_kind <> 'project_default'
       AND mount.backend_id = 'nextcloud'
       AND NULLIF(mount.cloud_handle, '') IS NOT NULL
     ORDER BY mount.thread_id, mount.target_path
)
INSERT INTO public.thread_protected_sources (
    id, thread_id, datasource_id, mount_kind, source_kind, source_ref,
    target_path, backend_id, backend_instance_id, cloud_handle, created_at
)
SELECT selected.id, selected.thread_id, NULL, selected.mount_kind,
       selected.source_kind, selected.source_ref, selected.target_path,
       selected.backend_id, selected.backend_instance_id,
       selected.cloud_handle, COALESCE(selected.created_at, now())
  FROM selected
 WHERE selected.mount_kind = 'project'
   AND selected.source_kind = 'project_folder'
   AND selected.source_ref IS NOT NULL
   AND selected.backend_instance_id IS NOT NULL
   AND selected.target_path <> ''
ON CONFLICT (thread_id) DO NOTHING;

-- 0186's function, verbatim but for the selected-source lookup.
CREATE OR REPLACE FUNCTION public.enforce_cloud_ro_mount_authority_shape()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    modern_authority boolean;
    source_handle jsonb;
    source_ref uuid;
    source_target text;
    source_native text;
    source_mountpoint text;
    source_canonical text;
    grant_canonical text;
    selected record;
    selected_handle jsonb;
    validate_selected boolean;
BEGIN
    modern_authority := (
        NEW.backend_instance_id IS NOT NULL
        OR NEW.grant_group_id IS NOT NULL
        OR NEW.grant_handle_sha256 IS NOT NULL
        OR NEW.source_binding IS NOT NULL
        OR NEW.source_binding_sha256 IS NOT NULL
        OR NEW.selected_mount_id IS NOT NULL
    );
    IF NEW.status IN ('engaging', 'active', 'revoking') THEN
        modern_authority := true;
    END IF;
    IF NOT modern_authority THEN
        -- Drained, revoked rows from before this authority version remain
        -- reject-only history. They can neither engage nor receive effects.
        RETURN NEW;
    END IF;
    IF TG_OP = 'INSERT' AND NEW.status <> 'engaging' THEN
        RAISE EXCEPTION 'protected reader must begin as an engaging intent'
            USING ERRCODE = '23514',
                  CONSTRAINT = 'cloud_ro_mounts_authority_shape';
    END IF;
    IF NEW.backend <> 'nextcloud'
       OR NEW.backend_instance_id IS NULL
       OR NEW.runtime_generation IS NULL
       OR NEW.engage_attempt IS NULL
       OR NEW.selected_mount_id IS NULL
       OR NEW.reader_id IS DISTINCT FROM
            'srw-reader-a-' || replace(NEW.engage_attempt::text, '-', '')
       OR NEW.grant_group_id IS DISTINCT FROM
            'srw-rog-a-' || replace(NEW.engage_attempt::text, '-', '')
       OR NEW.auth_kind IS DISTINCT FROM 'basic'
       OR NULLIF(NEW.webdav_url, '') IS NULL
       OR NEW.grant_handle_sha256 !~ '^[0-9a-f]{64}$'
       OR NEW.source_binding_sha256 !~ '^[0-9a-f]{64}$'
       OR jsonb_typeof(NEW.source_binding) IS DISTINCT FROM 'object'
       OR NEW.source_binding->>'version' IS DISTINCT FROM '1'
       OR NEW.source_binding->>'backend' IS DISTINCT FROM 'nextcloud'
       OR NEW.source_binding->>'backend_instance_id'
            IS DISTINCT FROM NEW.backend_instance_id::text
       OR NEW.source_binding->>'mount_kind' IS DISTINCT FROM 'project'
       OR NEW.source_binding->>'source_kind' IS DISTINCT FROM 'project_folder'
       OR jsonb_typeof(NEW.source_binding->'handle') IS DISTINCT FROM 'object'
       OR (
            NEW.status IN ('engaging', 'active', 'revoking')
            AND NULLIF(NEW.credentials, '') IS NULL
       )
       OR (
            NEW.status = 'active'
            AND jsonb_typeof(NEW.etag_baseline) IS DISTINCT FROM 'object'
       ) THEN
        RAISE EXCEPTION 'protected reader authority is malformed'
            USING ERRCODE = '23514',
                  CONSTRAINT = 'cloud_ro_mounts_authority_shape';
    END IF;

    BEGIN
        source_ref := (NEW.source_binding->>'source_ref')::uuid;
    EXCEPTION WHEN OTHERS THEN
        RAISE EXCEPTION 'protected reader source reference is malformed'
            USING ERRCODE = '23514',
                  CONSTRAINT = 'cloud_ro_mounts_authority_shape';
    END;
    source_handle := NEW.source_binding->'handle';
    source_target := NEW.source_binding->>'target_path';
    source_native := source_handle->>'native_id';
    source_mountpoint := source_handle->>'mountpoint';
    IF source_ref = '00000000-0000-0000-0000-000000000000'::uuid
       OR NEW.source_binding->>'source_ref' IS DISTINCT FROM source_ref::text
       OR NULLIF(source_target, '') IS NULL
       OR left(source_target, 1) = '/'
       OR position(E'\\' in source_target) > 0
       OR position('//' in source_target) > 0
       OR EXISTS (
            SELECT 1
              FROM unnest(string_to_array(source_target, '/')) AS segment(value)
             WHERE value IN ('', '.', '..')
       )
       OR COALESCE(source_native, '') !~ '^[1-9][0-9]*$'
       OR NULLIF(source_mountpoint, '') IS NULL
       OR source_mountpoint IN ('.', '..')
       OR position('/' in source_mountpoint) > 0
       OR position(E'\\' in source_mountpoint) > 0
       OR NEW.source_binding IS DISTINCT FROM pg_catalog.jsonb_build_object(
            'version', 1,
            'backend', 'nextcloud',
            'backend_instance_id', NEW.backend_instance_id::text,
            'mount_kind', 'project',
            'source_kind', 'project_folder',
            'source_ref', source_ref::text,
            'target_path', source_target,
            'handle', pg_catalog.jsonb_build_object(
                'native_id', source_native,
                'mountpoint', source_mountpoint
            )
       ) THEN
        RAISE EXCEPTION 'protected reader source binding is not canonical'
            USING ERRCODE = '23514',
                  CONSTRAINT = 'cloud_ro_mounts_authority_shape';
    END IF;

    source_canonical :=
        '{"backend":' || pg_catalog.to_jsonb('nextcloud'::text)::text ||
        ',"backend_instance_id":' ||
            pg_catalog.to_jsonb(NEW.backend_instance_id::text)::text ||
        ',"handle":{"mountpoint":' ||
            pg_catalog.to_jsonb(source_mountpoint)::text ||
        ',"native_id":' || pg_catalog.to_jsonb(source_native)::text || '}' ||
        ',"mount_kind":' || pg_catalog.to_jsonb('project'::text)::text ||
        ',"source_kind":' ||
            pg_catalog.to_jsonb('project_folder'::text)::text ||
        ',"source_ref":' || pg_catalog.to_jsonb(source_ref::text)::text ||
        ',"target_path":' || pg_catalog.to_jsonb(source_target)::text ||
        ',"version":1}';
    IF NEW.source_binding_sha256 IS DISTINCT FROM pg_catalog.encode(
        pg_catalog.sha256(pg_catalog.convert_to(source_canonical, 'UTF8')),
        'hex'
    ) THEN
        RAISE EXCEPTION 'protected reader source digest does not match'
            USING ERRCODE = '23514',
                  CONSTRAINT = 'cloud_ro_mounts_authority_shape';
    END IF;

    grant_canonical :=
        '{"backend":' || pg_catalog.to_jsonb('nextcloud'::text)::text ||
        ',"backend_instance_id":' ||
            pg_catalog.to_jsonb(NEW.backend_instance_id::text)::text ||
        ',"engage_attempt":' ||
            pg_catalog.to_jsonb(NEW.engage_attempt::text)::text ||
        ',"folder_id":' || pg_catalog.to_jsonb(source_native)::text ||
        ',"group_id":' || pg_catalog.to_jsonb(NEW.grant_group_id)::text ||
        ',"mountpoint":' || pg_catalog.to_jsonb(source_mountpoint)::text ||
        ',"reader_id":' || pg_catalog.to_jsonb(NEW.reader_id)::text ||
        ',"source_sha256":' ||
            pg_catalog.to_jsonb(NEW.source_binding_sha256)::text ||
        ',"version":1}';
    IF NEW.grant_handle IS DISTINCT FROM grant_canonical
       OR NEW.grant_handle_sha256 IS DISTINCT FROM pg_catalog.encode(
            pg_catalog.sha256(pg_catalog.convert_to(grant_canonical, 'UTF8')),
            'hex'
       ) THEN
        RAISE EXCEPTION 'protected reader grant handle does not match its attempt'
            USING ERRCODE = '23514',
                  CONSTRAINT = 'cloud_ro_mounts_authority_shape';
    END IF;

    validate_selected := TG_OP = 'INSERT';
    IF TG_OP = 'UPDATE' THEN
        validate_selected := (
            OLD.status = 'revoked'
            OR NEW.selected_mount_id IS DISTINCT FROM OLD.selected_mount_id
        );
    END IF;
    IF NEW.status = 'engaging' AND validate_selected THEN
        -- 0524: the protected folder a session selected is its
        -- thread_protected_sources row.
        SELECT source.mount_kind, source.target_path, source.source_kind,
               source.source_ref, source.backend_id,
               source.backend_instance_id, source.cloud_handle
          INTO selected
          FROM public.thread_protected_sources AS source
         WHERE source.id = NEW.selected_mount_id
           AND source.thread_id = NEW.thread_id;
        IF NOT FOUND THEN
            -- For one release: a session an older orchestrator selected from
            -- its thread_mounts row, before the startup pass copied it.
            SELECT mount.mount_kind, mount.target_path, mount.source_kind,
                   mount.source_ref, mount.backend_id,
                   mount.backend_instance_id, mount.cloud_handle
              INTO selected
              FROM public.thread_mounts AS mount
             WHERE mount.id = NEW.selected_mount_id
               AND mount.thread_id = NEW.thread_id;
        END IF;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'protected reader selected mount no longer exists'
                USING ERRCODE = '23514',
                      CONSTRAINT = 'cloud_ro_mounts_selected_source';
        END IF;
        selected_handle := public.protected_cloud_try_jsonb(selected.cloud_handle);
        IF selected.mount_kind IS DISTINCT FROM 'project'
           OR selected.target_path IS DISTINCT FROM source_target
           OR selected.source_kind IS DISTINCT FROM 'project_folder'
           OR selected.source_ref IS DISTINCT FROM source_ref
           OR selected.backend_id IS DISTINCT FROM 'nextcloud'
           OR selected.backend_instance_id IS DISTINCT FROM NEW.backend_instance_id
           OR selected_handle IS DISTINCT FROM pg_catalog.jsonb_build_object(
                'backend', 'nextcloud',
                'native_id', source_native,
                'vendor_meta', pg_catalog.jsonb_build_object(
                    'mountpoint', source_mountpoint
                )
           ) THEN
            RAISE EXCEPTION 'protected reader selected source changed'
                USING ERRCODE = '23514',
                      CONSTRAINT = 'cloud_ro_mounts_selected_source';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

COMMENT ON TABLE public.thread_protected_sources IS
    'A protected session''s folder: the project folder its cloud_folder '
    'connector binds at protected, frozen at admission. '
    'cloud_ro_mounts.selected_mount_id names its id. Immutable; rows copied '
    'from thread_mounts keep that row''s id and have no datasource_id.';
COMMENT ON COLUMN public.cloud_ro_mounts.selected_mount_id IS
    'thread_protected_sources.id of the protected folder (for one release, '
    'a thread_mounts.id selected by an older orchestrator).';

COMMIT;
