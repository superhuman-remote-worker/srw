-- migration:     0557_vm_end_accepts_disk_purged_by_upgrade_abort.sql
-- description:   A soft VM End accepts the disk its Session's own completed
--                upgrade abort already purged (E4b).
-- depends-on:    0556_vm_end_accepts_upgrade_virtual_binding.sql
-- transactional: yes

BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

-- Where VM resources are accounted, the abort of a failed workspace upgrade
-- releases the VM with its disk and leaves the charge to End (E4). A soft End
-- releases the charge only on a stop with the disk retained, which can't be
-- observed once the abort purged it: the charge stayed held and End never
-- finished. Its proof may say the disk was purged by that abort instead, but
-- only for the exact VM its Session's own abort released and completed.
CREATE FUNCTION public.vm_thread_cleanup_disk_purged_by_upgrade_abort(
    a public.vm_resource_thread_cleanup_authorities)
RETURNS boolean LANGUAGE sql STABLE AS $$
    SELECT NOT a.purge_disk AND EXISTS (
        SELECT 1 FROM public.vm_workspace_cleanup_admissions c
        WHERE c.owner_kind='thread' AND c.owner_id=a.thread_id AND c.pvc_uid=a.pvc_uid
          AND c.source='abort_thread_vm_upgrade' AND c.parent_admission_id IS NULL
          AND c.completed_at IS NOT NULL AND c.outcome='completed'
          -- cleanup_intent_digest's compact JSON, keys sorted, as in 0295.
          AND c.intent_digest='sha256:' || encode(sha256(convert_to(format(
              '{"owner_id":"%s","owner_kind":"thread","provision_generation":"%s","purge_disk":true,"pvc_uid":"%s","resource":"vm_workspace","source":"abort_thread_vm_upgrade","vm_uid":"%s"}',
              a.thread_id,a.provision_generation,a.pvc_uid,a.vm_uid),'UTF8')),'hex'))
$$;

-- Patch the live definition (0295, guarded again by 0323) in its one place.
-- A stop recorded as retained still holds: it was attested at its time.
DO $migration$
DECLARE
    definition text;
    old_clause text := $old$IF s.stop_evidence IS DISTINCT FROM expected
$old$;
    new_clause text := $new$IF (s.stop_evidence IS DISTINCT FROM expected
           AND NOT (public.vm_thread_cleanup_disk_purged_by_upgrade_abort(a)
               AND s.stop_evidence=expected||'{"pvc_disposition":"purged_by_upgrade_abort"}'::jsonb))
$new$;
BEGIN
    definition := pg_get_functiondef('public.validate_vm_thread_cleanup_stop(public.vm_resource_thread_cleanup_authorities,public.vm_resource_thread_cleanup_stops,boolean)'::regprocedure);
    IF (length(definition)-length(replace(definition,old_clause,'')))/length(old_clause)<>1 THEN
        RAISE EXCEPTION '0557 requires the exact stop disk disposition';
    END IF;
    EXECUTE replace(definition,old_clause,new_clause);
END;
$migration$;

-- A permanent delete after such an End still runs the retained-disk purge,
-- which then confirms the disk absent. A retained resume (validate_vm_thread_
-- retained_compute) keeps requiring a retained disk: there is none to resume.
DO $migration$
DECLARE
    definition text;
    old_clause text := $old$OR s.cleanup_admission_id IS NULL OR s.stop_evidence IS DISTINCT FROM expected_stop$old$;
    new_clause text := $new$OR s.cleanup_admission_id IS NULL OR (s.stop_evidence IS DISTINCT FROM expected_stop
           AND NOT (public.vm_thread_cleanup_disk_purged_by_upgrade_abort(a)
               AND s.stop_evidence=expected_stop||'{"pvc_disposition":"purged_by_upgrade_abort"}'::jsonb))$new$;
BEGIN
    definition := pg_get_functiondef('public.validate_vm_thread_retained_disk_purge(public.vm_thread_retained_disk_purge_authorities,boolean)'::regprocedure);
    IF (length(definition)-length(replace(definition,old_clause,'')))/length(old_clause)<>1 THEN
        RAISE EXCEPTION '0557 requires the exact retained disk predecessor stop';
    END IF;
    EXECUTE replace(definition,old_clause,new_clause);
END;
$migration$;

COMMIT;
