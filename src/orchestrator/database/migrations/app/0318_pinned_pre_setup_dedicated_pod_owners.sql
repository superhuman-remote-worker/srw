-- A cold dedicated Pod is issued by AgentProvisioner; PersistentProvisioner
-- also issues dedicated Pods. Neither name grants process-zero or pool release.
-- Keep the exact published attempt, captured G/actor/name/UID/namespace, actor
-- mode and zero-work predicates. Only their existing provisioner enum changes.
DO $migration$
DECLARE
    definition text;
    old_fragment text := $old$           AND i.provisioner='persistent' AND i.namespace=pod->>'namespace'$old$;
    new_fragment text := $new$           AND i.provisioner IN ('agent','persistent') AND i.namespace=pod->>'namespace'$new$;
BEGIN
    definition := pg_get_functiondef('public.pinned_pre_setup_retirement_request_valid(public.threads,jsonb,boolean)'::regprocedure);
    IF (length(definition)-length(replace(definition,old_fragment,'')))/length(old_fragment) <> 1 THEN
        RAISE EXCEPTION '0318 requires one exact dedicated Pod provisioner predicate';
    END IF;
    EXECUTE replace(definition,old_fragment,new_fragment);
END;
$migration$;
