-- migration: 0266_vm_creation_cancellation_disposition.sql
-- description: Freeze partial creation cancellation evidence without releasing authority.
-- depends-on: 0265_workspace_idle_episodes.sql
-- transactional: yes
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE public.vm_creation_retries
    ADD COLUMN cancellation_disposition jsonb,
    ADD COLUMN cancellation_progress jsonb NOT NULL DEFAULT '{}';
ALTER TABLE public.vm_creation_retries ADD CONSTRAINT vm_creation_disposition_shape CHECK ((
    (cancellation_disposition IS NULL OR
     (jsonb_typeof(cancellation_disposition)='object' AND
      cancellation_disposition->'version'='1'::jsonb AND
      cancellation_disposition->>'request_id'=request_id::text)) AND
    jsonb_typeof(cancellation_progress)='object' AND
    (cancellation_disposition IS NOT NULL OR cancellation_progress='{}'::jsonb)
) IS TRUE) NOT VALID;

CREATE FUNCTION public.guard_vm_creation_disposition() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='INSERT' THEN
        IF NEW.cancellation_disposition IS NOT NULL OR NEW.cancellation_progress<>'{}'::jsonb THEN
            RAISE EXCEPTION 'Creation disposition requires locked cancellation' USING ERRCODE='23514';
        END IF;
        RETURN NEW;
    END IF;
    IF OLD.cancellation_disposition IS NOT NULL AND
       NEW.cancellation_disposition IS DISTINCT FROM OLD.cancellation_disposition THEN
        RAISE EXCEPTION 'Creation cancellation disposition is immutable' USING ERRCODE='23514';
    END IF;
    IF NEW.cancellation_disposition IS NOT NULL AND OLD.cancellation_disposition IS NULL THEN
        IF OLD.state<>'cancel_requested' OR NEW.state<>'cancel_requested' OR
           NEW.creation_admission_id IS NULL OR NEW.creation_carrier_uid IS NULL OR
           EXISTS(SELECT 1 FROM public.vm_creation_effects WHERE request_id=NEW.request_id
                  AND (state='issued' OR (effect_kind='vm' AND state<>'rejected'))) THEN
            RAISE EXCEPTION 'Creation disposition requires no possible VM issuance' USING ERRCODE='23514';
        END IF;
    END IF;
    -- This checkpoint has no completion writer. Keep the admission held even
    -- when every recorded create was rejected; source pins may predate grants.
    IF NEW.cancellation_disposition IS NOT NULL AND NEW.state<>'cancel_requested' THEN
        RAISE EXCEPTION 'Creation disposition has not completed' USING ERRCODE='23514';
    END IF;
    IF EXISTS(SELECT 1 FROM jsonb_each(OLD.cancellation_progress) AS p
              WHERE NEW.cancellation_progress->p.key IS DISTINCT FROM p.value) OR
       (NEW.cancellation_progress - ARRAY['cloud_init','rootdisk','workspace_attachment','source'])<>'{}'::jsonb THEN
        RAISE EXCEPTION 'Creation cancellation progress is monotonic' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER vm_creation_disposition BEFORE INSERT OR UPDATE ON public.vm_creation_retries
FOR EACH ROW EXECUTE FUNCTION public.guard_vm_creation_disposition();

CREATE FUNCTION public.guard_vm_creation_effect_disposition() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    disposition jsonb;
BEGIN
    SELECT cancellation_disposition INTO disposition FROM public.vm_creation_retries
        WHERE request_id=NEW.request_id FOR UPDATE;
    IF disposition IS NOT NULL THEN
        RAISE EXCEPTION 'Creation disposition forbids further create effects' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER vm_creation_effect_disposition BEFORE INSERT ON public.vm_creation_effects
FOR EACH ROW EXECUTE FUNCTION public.guard_vm_creation_effect_disposition();
COMMIT;
