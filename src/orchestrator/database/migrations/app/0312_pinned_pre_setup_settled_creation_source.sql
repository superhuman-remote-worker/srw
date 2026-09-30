-- A never-issued request may update the live VM projection during End. Keep
-- its exact nomination valid only when the existing G/T source-settlement
-- predicate proves no issued/unresolved effect can create a late runtime.
DO $migration$
DECLARE
    definition text;
    old_fragment text := $old$           OR owner_row.metadata->'vm' IS DISTINCT FROM source->'captured_vm'$old$;
    new_fragment text := $new$           OR (
               owner_row.metadata->'vm' IS DISTINCT FROM source->'captured_vm'
               AND NOT public.thread_vm_creation_never_issued_source(
                   owner_row.id,source->>'provision_generation')
           )$new$;
BEGIN
    definition := pg_get_functiondef('public.pinned_pre_setup_retirement_request_valid(public.threads,jsonb,boolean)'::regprocedure);
    IF (length(definition)-length(replace(definition,old_fragment,'')))/length(old_fragment) <> 1 THEN
        RAISE EXCEPTION '0312 requires one exact pre-setup VM projection predicate';
    END IF;
    EXECUTE replace(definition,old_fragment,new_fragment);
END;
$migration$;
