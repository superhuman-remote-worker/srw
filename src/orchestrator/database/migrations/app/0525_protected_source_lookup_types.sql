-- migration:     0525_protected_source_lookup_types.sql
-- description:   0524's selection check reads a protected session's folder
--                from thread_protected_sources (text columns) and, for one
--                release, from thread_mounts (varchar mount_kind and
--                source_kind) into one record. A backend that ran both
--                lookups failed the second with "type of parameter ... does
--                not match that when preparing the plan". Both lookups now
--                cast to one row type. 0524's function otherwise verbatim.
-- depends-on:    0524_thread_protected_sources.sql
-- expected:      < 1s (a function replacement).
-- locks:         none on tables.
-- transactional: yes

BEGIN;
SET LOCAL lock_timeout                        = '2s';
SET LOCAL statement_timeout                   = '15min';
SET LOCAL idle_in_transaction_session_timeout = '5min';
SET LOCAL timezone                            = 'UTC';

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
        -- 0525: both lookups yield one row type (text, not thread_mounts'
        -- varchar), so a backend that runs both keeps one cached plan.
        SELECT source.mount_kind::text, source.target_path::text,
               source.source_kind::text, source.source_ref::uuid,
               source.backend_id::text, source.backend_instance_id::uuid,
               source.cloud_handle::text
          INTO selected
          FROM public.thread_protected_sources AS source
         WHERE source.id = NEW.selected_mount_id
           AND source.thread_id = NEW.thread_id;
        IF NOT FOUND THEN
            -- For one release: a session an older orchestrator selected from
            -- its thread_mounts row, before the startup pass copied it.
            SELECT mount.mount_kind::text, mount.target_path::text,
                   mount.source_kind::text, mount.source_ref::uuid,
                   mount.backend_id::text, mount.backend_instance_id::uuid,
                   mount.cloud_handle::text
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

COMMIT;
