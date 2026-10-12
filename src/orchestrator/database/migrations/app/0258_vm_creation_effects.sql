-- migration: 0258_vm_creation_effects.sql
-- description: Single-flight VM create effects under the existing cleanup admission.
-- depends-on: 0257_vm_creation_retries.sql
-- transactional: yes
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';
ALTER TABLE vm_creation_retries ADD COLUMN controller_configuration jsonb CHECK (controller_configuration IS NULL OR jsonb_typeof(controller_configuration)='object');
ALTER TABLE vm_creation_retries ADD COLUMN creation_carrier_uid uuid;
ALTER TABLE vm_creation_retries ADD COLUMN creation_carrier_namespace text;
ALTER TABLE vm_creation_retries ADD CONSTRAINT vm_creation_carrier_pair CHECK (
    (creation_carrier_uid IS NULL)=(creation_carrier_namespace IS NULL)
    AND (creation_carrier_namespace IS NULL OR creation_carrier_namespace<>''));
CREATE FUNCTION guard_vm_creation_carrier_identity() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.controller_configuration IS DISTINCT FROM OLD.controller_configuration OR
       (OLD.creation_carrier_uid IS NOT NULL AND
        ROW(NEW.creation_carrier_uid,NEW.creation_carrier_namespace) IS DISTINCT FROM
        ROW(OLD.creation_carrier_uid,OLD.creation_carrier_namespace)) THEN
        RAISE EXCEPTION 'VM creation carrier identity is immutable' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER vm_creation_carrier_identity BEFORE UPDATE ON vm_creation_retries
FOR EACH ROW EXECUTE FUNCTION guard_vm_creation_carrier_identity();
CREATE TABLE vm_creation_effects (
    effect_nonce uuid PRIMARY KEY,
    request_id uuid NOT NULL REFERENCES vm_creation_retries(request_id),
    effect_number integer NOT NULL CHECK (effect_number>0),
    effect_kind text NOT NULL CHECK (effect_kind IN ('rootdisk','cloud_init','vm')),
    carrier_uid uuid NOT NULL,
    carrier_namespace text NOT NULL CHECK (carrier_namespace<>''),
    carrier_intent jsonb NOT NULL CHECK (jsonb_typeof(carrier_intent)='object'),
    state text NOT NULL DEFAULT 'issued' CHECK (state IN ('issued','observed','rejected')),
    evidence jsonb NOT NULL DEFAULT '{}' CHECK (jsonb_typeof(evidence)='object'),
    issued_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    resolved_at timestamptz,
    UNIQUE(request_id,effect_number),
    CHECK ((state='issued')=(resolved_at IS NULL)),
    CHECK ((state='issued')=(evidence='{}'::jsonb))
);
CREATE UNIQUE INDEX vm_creation_effect_one_outstanding ON vm_creation_effects(request_id)
WHERE state='issued';
CREATE FUNCTION guard_vm_creation_effect_identity() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF ROW(NEW.effect_nonce,NEW.request_id,NEW.effect_number,NEW.effect_kind,
           NEW.carrier_uid,NEW.carrier_namespace,NEW.carrier_intent,NEW.issued_at)
       IS DISTINCT FROM
       ROW(OLD.effect_nonce,OLD.request_id,OLD.effect_number,OLD.effect_kind,
           OLD.carrier_uid,OLD.carrier_namespace,OLD.carrier_intent,OLD.issued_at)
       OR (OLD.state<>'issued' AND NEW IS DISTINCT FROM OLD) THEN
        RAISE EXCEPTION 'VM creation effect identity or resolved evidence is immutable' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER vm_creation_effect_identity BEFORE UPDATE ON vm_creation_effects
FOR EACH ROW EXECUTE FUNCTION guard_vm_creation_effect_identity();
COMMIT;
