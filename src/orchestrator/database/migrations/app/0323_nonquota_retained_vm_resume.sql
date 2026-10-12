-- Native non-quota End records the same exact physical-stop authority as quota
-- End. NULL reservation fields mean no quota exists, never a synthetic debit.
-- Retained Resume/disk purge retain every common actor, lifecycle, admission,
-- process-zero, controller-stop, source and disk guard. v3 keeps its full debit.
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE public.vm_resource_thread_cleanup_authorities
    ALTER COLUMN reservation_id DROP NOT NULL,
    ALTER COLUMN reservation_revision DROP NOT NULL;
ALTER TABLE public.vm_resource_thread_cleanup_authorities ADD CONSTRAINT
    vm_thread_cleanup_optional_reservation_pair CHECK (
        (reservation_id IS NULL)=(reservation_revision IS NULL));

CREATE FUNCTION public.valid_vm_thread_nonquota_creation(source public.vm_creation_retries)
RETURNS boolean LANGUAGE sql STABLE AS $$
    SELECT (source.owner_kind='thread'
       AND source.controller_configuration->'version'='1'::jsonb
       AND source.controller_configuration->'persistent_rootdisk'='true'::jsonb
       AND COALESCE(source.controller_configuration->'resource_admission','null'::jsonb)='null'::jsonb
       AND COALESCE(source.controller_configuration->'network_profile_policy','null'::jsonb)='null'::jsonb
       AND COALESCE(source.canonical_request->'network_profile','null'::jsonb)='null'::jsonb
       AND source.canonical_request->>'entity_type'='thread'
       AND source.canonical_request->>'job_id'=source.thread_id::text
       AND source.canonical_request->>'provision_generation'=source.provision_generation::text
       AND NOT EXISTS (SELECT 1 FROM public.vm_resource_reservations r WHERE r.request_id=source.request_id)
       AND NOT EXISTS (SELECT 1 FROM public.vm_resource_waiters w WHERE w.request_id=source.request_id)
    ) IS TRUE;
$$;

CREATE FUNCTION public.valid_vm_thread_nonquota_cleanup_source(
    a public.vm_resource_thread_cleanup_authorities, source public.vm_creation_retries
) RETURNS boolean LANGUAGE sql STABLE AS $$
    SELECT (public.valid_vm_thread_nonquota_creation(source)
       AND a.reservation_id IS NULL AND a.reservation_revision IS NULL
       AND source.thread_id=a.thread_id
       AND ((source.thread_runtime_generation=a.runtime_generation
             AND source.thread_agent_id IS NOT DISTINCT FROM a.agent_id
             AND source.thread_attach_token IS NOT DISTINCT FROM a.attach_token)
            OR public.vm_thread_creation_pre_setup_abort_path_evidence(
                json_populate_record(NULL::public.threads,json_build_object(
                    'id',a.thread_id,'runtime_generation',a.runtime_generation,
                    'runtime_retirement_started_at',a.created_at)),source) IS NOT NULL)
       AND source.provision_generation=a.provision_generation
       AND source.observed_vm_uid=a.vm_uid AND source.observed_pvc_uid=a.pvc_uid
       AND a.vmi_uid::text IS NOT DISTINCT FROM a.retirement_context->'vm'->>'vmi_uid'
       AND a.launcher_uid::text IS NOT DISTINCT FROM a.retirement_context->'vm'->>'active_pod_uid'
       AND (source.state='succeeded' AND source.reason='creation_adopted'
            OR public.valid_vm_thread_retained_cleanup_source(a,source))
       AND source.boot_counted
       AND (source.origin='initial' AND source.expected_pvc_uid IS NULL
            OR public.valid_vm_thread_retained_resume_source(source))
       AND NOT EXISTS (SELECT 1 FROM public.vm_creation_effects e
           WHERE e.request_id=source.request_id AND e.state='issued')
       AND EXISTS (SELECT 1 FROM public.vm_workspace_cleanup_admissions c
           WHERE c.id=source.creation_admission_id AND c.owner_kind='thread'
             AND c.owner_id=a.thread_id AND c.source='controller_vm_create'
             AND c.completed_at IS NOT NULL AND c.outcome='adopted')
    ) IS TRUE;
$$;

-- Replace only the quota-specific clauses on the installed definitions; keep
-- all concurrent common guards and the original v3 quota clauses verbatim.
DO $migration$
DECLARE
    signature text;
    definition text;
    first_clause text;
    last_clause text;
    quota_clause text;
    start_at integer;
    end_at integer;
    version_clause text := $old$       OR source.controller_configuration->>'version' IS DISTINCT FROM '3'$old$;
    actor_clause text;
BEGIN
    FOREACH signature IN ARRAY ARRAY[
        'public.validate_vm_thread_cleanup_authority(public.vm_resource_thread_cleanup_authorities,boolean)',
        'public.validate_vm_thread_retained_compute(uuid)',
        'public.validate_vm_thread_retained_disk_purge(public.vm_thread_retained_disk_purge_authorities,boolean)'
    ] LOOP
        definition := pg_get_functiondef(signature::regprocedure);
        first_clause := '       OR charge.id IS NULL';
        IF signature LIKE '%validate_vm_thread_cleanup_authority%' THEN
            last_clause := $last$       OR cleanup.id IS NULL$last$;
            start_at := strpos(definition,first_clause);
            end_at := strpos(definition,last_clause);
            IF start_at=0 OR end_at<=start_at THEN RAISE EXCEPTION '0323 exact quota guard missing: %',signature; END IF;
            quota_clause := substring(definition FROM start_at FOR end_at-start_at);
        ELSE
            last_clause := $last$AND w.state='released')$last$;
            start_at := strpos(definition,first_clause);
            end_at := strpos(definition,last_clause)+length(last_clause);
            IF start_at=0 OR strpos(definition,last_clause)=0 OR end_at<=start_at THEN
                RAISE EXCEPTION '0323 exact retained quota guard missing: %',signature;
            END IF;
            quota_clause := substring(definition FROM start_at FOR end_at-start_at);
        END IF;
        IF (length(definition)-length(replace(definition,quota_clause,'')))/length(quota_clause)<>1
           OR (length(definition)-length(replace(definition,version_clause,'')))/length(version_clause)<>1 THEN
            RAISE EXCEPTION '0323 requires one quota block and one source discriminator: %',signature;
        END IF;
        definition := replace(definition,quota_clause,
            '       OR (NOT public.valid_vm_thread_nonquota_cleanup_source(a,source) AND (' ||
            substring(quota_clause FROM length('       OR ')+1) || '))' || E'\n');
        definition := replace(definition,version_clause,
            version_clause || ' AND NOT public.valid_vm_thread_nonquota_cleanup_source(a,source)');
        IF signature LIKE '%validate_vm_thread_cleanup_authority%' THEN
            actor_clause := E'OR (NOT terminal_handoff AND (\n           source.thread_runtime_generation';
        ELSE
            actor_clause := E'OR (NOT COALESCE(public.valid_vm_thread_retained_cleanup_source(a,source),false) AND (\n           source.thread_runtime_generation';
        END IF;
        IF (length(definition)-length(replace(definition,actor_clause,'')))/length(actor_clause)<>1 THEN
            RAISE EXCEPTION '0323 requires the exact source actor guard: %',signature;
        END IF;
        definition := replace(definition,actor_clause,
            replace(actor_clause,' AND (',' AND NOT public.valid_vm_thread_nonquota_cleanup_source(a,source) AND ('));
        EXECUTE definition;
    END LOOP;
END;
$migration$;

DO $migration$
DECLARE
    definition text;
    old_clause text := $old$       OR NOT EXISTS (SELECT 1 FROM public.vm_resource_reservations r
           WHERE r.id=a.reservation_id AND (r.state='teardown' OR (allow_released AND r.state='released')))$old$;
    new_clause text := $new$       OR (NOT EXISTS (SELECT 1 FROM public.vm_resource_reservations r
           WHERE r.id=a.reservation_id AND (r.state='teardown' OR (allow_released AND r.state='released')))
           AND NOT EXISTS (SELECT 1 FROM public.vm_creation_retries source WHERE source.request_id=a.request_id
               AND public.valid_vm_thread_nonquota_cleanup_source(a,source)))$new$;
BEGIN
    definition := pg_get_functiondef('public.validate_vm_thread_cleanup_stop(public.vm_resource_thread_cleanup_authorities,public.vm_resource_thread_cleanup_stops,boolean)'::regprocedure);
    IF (length(definition)-length(replace(definition,old_clause,'')))/length(old_clause)<>1 THEN
        RAISE EXCEPTION '0323 requires the exact stop reservation guard';
    END IF;
    EXECUTE replace(definition,old_clause,new_clause);
END;
$migration$;

DO $migration$
DECLARE
    definition text;
    old_clause text := $old$       OR NOT EXISTS (SELECT 1 FROM public.vm_resource_waiters w WHERE w.request_id=retry.request_id AND w.state IN ('released','cancelled'))$old$;
    new_clause text := $new$       OR (NOT EXISTS (SELECT 1 FROM public.vm_resource_waiters w WHERE w.request_id=retry.request_id AND w.state IN ('released','cancelled'))
           AND NOT public.valid_vm_thread_nonquota_creation(retry))$new$;
BEGIN
    definition := pg_get_functiondef('public.vm_thread_retained_source_terminal_evidence(public.vm_creation_retries)'::regprocedure);
    IF (length(definition)-length(replace(definition,old_clause,'')))/length(old_clause)<>1 THEN
        RAISE EXCEPTION '0323 requires the exact retained source waiter guard';
    END IF;
    EXECUTE replace(definition,old_clause,new_clause);
END;
$migration$;
COMMIT;
