-- migration: 0260_vm_creation_ready_release.sql
-- description: Preserve write-once Ready release after exact VM creation adoption.
-- depends-on: 0259_vm_creation_adoption.sql
-- transactional: yes
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';
CREATE FUNCTION guard_vm_creation_ready_release() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF (OLD.ready_at IS NOT NULL AND NEW.ready_at IS DISTINCT FROM OLD.ready_at)
       OR (NEW.ready_at IS NOT NULL AND NOT (
           NEW.state='succeeded' AND NEW.boot_counted
           AND NEW.observed_vm_uid IS NOT NULL AND NEW.observed_pvc_uid IS NOT NULL
       )) THEN
        RAISE EXCEPTION 'VM creation Ready release is immutable and requires adoption' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER vm_creation_ready_release BEFORE UPDATE ON vm_creation_retries
FOR EACH ROW EXECUTE FUNCTION guard_vm_creation_ready_release();
COMMIT;
