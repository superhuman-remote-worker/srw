-- migration: 0331_pinned_pvc_create_response_receipts.sql
-- description: Retain an exact PVC CREATE response separately from Ready publication.
-- depends-on: 0330_pinned_partial_creation_abort_retirement.sql
-- transactional: yes
BEGIN;
SET LOCAL lock_timeout='2s';
SET LOCAL statement_timeout='30s';
CREATE TABLE public.thread_workspace_provision_create_receipts (
 attempt_id uuid NOT NULL REFERENCES public.thread_workspace_provision_intents(attempt_id),
 resource text NOT NULL CHECK(resource='pvc'),
 thread_id uuid NOT NULL,
 runtime_generation uuid NOT NULL,
 namespace text NOT NULL,
 resource_name text NOT NULL,
 resource_uid text NOT NULL CHECK(resource_uid ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'),
 protocol text NOT NULL CHECK(protocol='kubernetes_create_response_v1'),
 observed_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
 PRIMARY KEY(attempt_id,resource)
);
CREATE FUNCTION public.enforce_pinned_workspace_create_receipt() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE source public.thread_workspace_provision_intents%ROWTYPE;
BEGIN
 IF TG_OP<>'INSERT' THEN
  RAISE EXCEPTION 'workspace CREATE receipt is immutable' USING ERRCODE='23514',CONSTRAINT='pinned_workspace_create_receipt_authority';
 END IF;
 SELECT * INTO source FROM public.thread_workspace_provision_intents WHERE attempt_id=NEW.attempt_id FOR SHARE;
 IF source.attempt_id IS NULL OR source.thread_id<>NEW.thread_id
   OR source.runtime_generation<>NEW.runtime_generation OR source.namespace<>NEW.namespace
   OR source.pvc_name IS DISTINCT FROM NEW.resource_name
   OR source.status NOT IN ('planned','revoking') OR source.retained_source_attempt_id IS NOT NULL
   OR (source.pvc_uid IS NOT NULL AND source.pvc_uid<>NEW.resource_uid)
   OR NEW.observed_at IS DISTINCT FROM transaction_timestamp() THEN
  RAISE EXCEPTION 'PVC CREATE receipt lacks exact source intent' USING ERRCODE='23514',CONSTRAINT='pinned_workspace_create_receipt_authority';
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER trg_pinned_workspace_create_receipt_authority BEFORE INSERT OR UPDATE OR DELETE
 ON public.thread_workspace_provision_create_receipts FOR EACH ROW
 EXECUTE FUNCTION public.enforce_pinned_workspace_create_receipt();
COMMIT;
