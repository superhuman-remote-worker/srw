-- Retry-enabled thread creation retains source authority without optional quotas.
-- v3 still owes its installed whole-launcher policy. v1 is persistent, authenticated
-- non-quota hosting with no network-profile or resource payload; all owner, actor,
-- execution, digest, issuance, cancellation and retained-Resume guards stay intact.
DO $migration$
DECLARE
    definition text;
    old_fragment text := $old$       OR NEW.controller_configuration->'version' IS DISTINCT FROM '3'::jsonb THEN$old$;
    new_fragment text := $new$       OR (
           NEW.controller_configuration->'version' IS DISTINCT FROM '3'::jsonb
           AND (
               NEW.controller_configuration->'version' IS DISTINCT FROM '1'::jsonb
               OR NEW.controller_configuration->'persistent_rootdisk' IS DISTINCT FROM 'true'::jsonb
               OR COALESCE(NEW.controller_configuration->'resource_admission','null'::jsonb) IS DISTINCT FROM 'null'::jsonb
               OR COALESCE(NEW.controller_configuration->'network_profile_policy','null'::jsonb) IS DISTINCT FROM 'null'::jsonb
               OR COALESCE(NEW.canonical_request->'network_profile','null'::jsonb) IS DISTINCT FROM 'null'::jsonb
           )
       ) THEN$new$;
BEGIN
    definition := pg_get_functiondef('public.guard_vm_creation_thread_source()'::regprocedure);
    IF (length(definition)-length(replace(definition,old_fragment,'')))/length(old_fragment) <> 1 THEN
        RAISE EXCEPTION '0310 requires one exact thread source configuration discriminator';
    END IF;
    EXECUTE replace(definition,old_fragment,new_fragment);
END;
$migration$;
