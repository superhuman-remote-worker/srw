-- migration: 0286_pinned_failed_start_retirement.sql
-- description: Exact failed-start pinned workspace stop, retention and successor authority.
-- depends-on: 0285_validate_vm_creation_unused_grant_receipt.sql
-- transactional: yes
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

-- Exact failed-start pinned workspace stop, retention and successor authority.
-- Original creation actors and issued UIDs remain immutable. No Ready binding
-- is introduced by retirement or by retaining an unstarted workspace.
ALTER TABLE public.thread_workspace_provision_intents
    ADD COLUMN cleanup_disposition text,
    ADD COLUMN cleanup_retirement_token uuid,
    ADD COLUMN purge_completed_at timestamptz,
    ADD COLUMN retained_source_attempt_id uuid,
    ADD COLUMN creation_effects_admitted_at timestamptz,
    ADD COLUMN retained_resume_generation uuid,
    ADD COLUMN retained_successor_attempt_id uuid,
    ADD CONSTRAINT thread_workspace_cleanup_disposition CHECK (
        (cleanup_disposition IS NULL AND cleanup_retirement_token IS NULL
         AND purge_completed_at IS NULL)
        OR (cleanup_disposition IN ('retain','purge')
            AND cleanup_retirement_token IS NOT NULL
            AND (cleanup_disposition='purge' OR purge_completed_at IS NULL))
    ) NOT VALID;
ALTER TABLE public.thread_workspace_provision_intents
    DROP CONSTRAINT thread_workspace_provision_intents_check7,
    ADD CONSTRAINT thread_workspace_retained_storage_source CHECK (
        retained_pvc_uid IS NULL OR (pvc_name IS NOT NULL AND
          (retained_binding_generation IS NOT NULL OR retained_source_attempt_id IS NOT NULL))
    ) NOT VALID;
ALTER TABLE public.thread_workspace_provision_intents
    DROP CONSTRAINT thread_workspace_provision_intents_check9,
    ADD CONSTRAINT thread_workspace_provision_intents_check9 CHECK (
        (status IN ('planned','revoking')
         AND fence_pod_uid IS NULL AND fence_pvc_uid IS NULL
         AND fence_configmap_uid IS NULL AND fence_service_uid IS NULL
         AND fenced_at IS NULL AND gc_after IS NULL AND resolved_at IS NULL)
        OR (status='published'
            AND NULLIF(pod_uid,'') IS NOT NULL
            AND (pvc_name IS NULL OR NULLIF(pvc_uid,'') IS NOT NULL)
            AND (seed_configmap_name IS NULL OR NULLIF(seed_configmap_uid,'') IS NOT NULL)
            AND (service_name IS NULL OR NULLIF(service_uid,'') IS NOT NULL)
            AND fence_pod_uid IS NULL AND fence_pvc_uid IS NULL
            AND fence_configmap_uid IS NULL AND fence_service_uid IS NULL
            AND fenced_at IS NULL AND gc_after IS NULL AND resolved_at IS NOT NULL)
        OR (status='fenced'
            AND NULLIF(fence_pod_uid,'') IS NOT NULL
            AND (pvc_name IS NULL OR retained_pvc_uid IS NOT NULL
                 OR (cleanup_disposition IS NOT NULL AND pvc_uid IS NOT NULL)
                 OR NULLIF(fence_pvc_uid,'') IS NOT NULL)
            AND (seed_configmap_name IS NULL OR NULLIF(fence_configmap_uid,'') IS NOT NULL)
            AND (service_name IS NULL OR retained_service_uid IS NOT NULL
                 OR (cleanup_disposition IS NOT NULL AND service_uid IS NOT NULL)
                 OR NULLIF(fence_service_uid,'') IS NOT NULL)
            AND fenced_at IS NOT NULL AND gc_after >= fenced_at + interval '10 minutes'
            AND resolved_at IS NULL)
        OR (status='retired' AND fenced_at IS NOT NULL
            AND gc_after IS NOT NULL AND resolved_at IS NOT NULL)
    ) NOT VALID;

CREATE OR REPLACE FUNCTION public.enforce_thread_workspace_provision_intent() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    thread_row public.threads%ROWTYPE;
    inverse_count integer;
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION
            'workspace provision intent % is append-only', OLD.attempt_id
            USING ERRCODE = '23514',
                  CONSTRAINT = 'thread_workspace_provision_intent_authority';
    END IF;

    IF TG_OP = 'UPDATE' THEN
        -- The companion retention trigger validates these additive fields.
        -- No change to the original immutable protocol is implied.
        IF (to_jsonb(NEW) - ARRAY['cleanup_disposition','cleanup_retirement_token',
             'purge_completed_at','retained_source_attempt_id','creation_effects_admitted_at',
             'retained_resume_generation','retained_successor_attempt_id'])
           = (to_jsonb(OLD) - ARRAY['cleanup_disposition','cleanup_retirement_token',
             'purge_completed_at','retained_source_attempt_id','creation_effects_admitted_at',
             'retained_resume_generation','retained_successor_attempt_id']) THEN
            RETURN NEW;
        END IF;
        IF NEW.attempt_id IS DISTINCT FROM OLD.attempt_id
           OR NEW.thread_id IS DISTINCT FROM OLD.thread_id
           OR NEW.runtime_generation IS DISTINCT FROM OLD.runtime_generation
           OR NEW.created_agent_id IS DISTINCT FROM OLD.created_agent_id
           OR NEW.created_attach_token IS DISTINCT FROM OLD.created_attach_token
           OR NEW.namespace IS DISTINCT FROM OLD.namespace
           OR NEW.pod_name IS DISTINCT FROM OLD.pod_name
           OR NEW.pvc_name IS DISTINCT FROM OLD.pvc_name
           OR NEW.seed_configmap_name IS DISTINCT FROM OLD.seed_configmap_name
           OR NEW.service_name IS DISTINCT FROM OLD.service_name
           OR NEW.network_tier IS DISTINCT FROM OLD.network_tier
           OR NEW.manifest_fingerprint IS DISTINCT FROM OLD.manifest_fingerprint
           OR NEW.previous_binding IS DISTINCT FROM OLD.previous_binding
           OR NEW.retained_binding_generation
                IS DISTINCT FROM OLD.retained_binding_generation
           OR NEW.retained_pvc_uid IS DISTINCT FROM OLD.retained_pvc_uid
           OR NEW.retained_service_uid IS DISTINCT FROM OLD.retained_service_uid
           OR NEW.created_at IS DISTINCT FROM OLD.created_at
           OR NOT (
                (OLD.status = 'planned' AND NEW.status = 'planned'
                    AND (OLD.pod_uid IS NULL
                         OR NEW.pod_uid IS NOT DISTINCT FROM OLD.pod_uid)
                    AND (OLD.pvc_uid IS NULL
                         OR NEW.pvc_uid IS NOT DISTINCT FROM OLD.pvc_uid)
                    AND (OLD.seed_configmap_uid IS NULL
                         OR NEW.seed_configmap_uid
                            IS NOT DISTINCT FROM OLD.seed_configmap_uid)
                    AND (OLD.service_uid IS NULL
                         OR NEW.service_uid IS NOT DISTINCT FROM OLD.service_uid)
                    AND NEW.fence_pod_uid IS NULL
                    AND NEW.fence_pvc_uid IS NULL
                    AND NEW.fence_configmap_uid IS NULL
                    AND NEW.fence_service_uid IS NULL
                    AND NEW.fenced_at IS NULL AND NEW.gc_after IS NULL
                    AND NEW.resolved_at IS NULL)
                OR (OLD.status = 'planned' AND NEW.status = 'published'
                    AND NULLIF(NEW.pod_uid, '') IS NOT NULL
                    AND (NEW.pvc_name IS NULL OR NULLIF(NEW.pvc_uid, '') IS NOT NULL)
                    AND (NEW.seed_configmap_name IS NULL
                         OR NULLIF(NEW.seed_configmap_uid, '') IS NOT NULL)
                    AND (NEW.service_name IS NULL
                         OR NULLIF(NEW.service_uid, '') IS NOT NULL)
                    AND NEW.fence_pod_uid IS NULL
                    AND NEW.fence_pvc_uid IS NULL
                    AND NEW.fence_configmap_uid IS NULL
                    AND NEW.fence_service_uid IS NULL
                    AND NEW.fenced_at IS NULL AND NEW.gc_after IS NULL
                    AND NEW.resolved_at
                        IS NOT DISTINCT FROM transaction_timestamp())
                OR (OLD.status = 'planned' AND NEW.status = 'revoking'
                    AND NEW.pod_uid IS NOT DISTINCT FROM OLD.pod_uid
                    AND NEW.pvc_uid IS NOT DISTINCT FROM OLD.pvc_uid
                    AND NEW.seed_configmap_uid
                        IS NOT DISTINCT FROM OLD.seed_configmap_uid
                    AND NEW.service_uid IS NOT DISTINCT FROM OLD.service_uid
                    AND NEW.fence_pod_uid IS NULL
                    AND NEW.fence_pvc_uid IS NULL
                    AND NEW.fence_configmap_uid IS NULL
                    AND NEW.fence_service_uid IS NULL
                    AND NEW.fenced_at IS NULL AND NEW.gc_after IS NULL
                    AND NEW.resolved_at IS NULL)
                OR (OLD.status = 'revoking' AND NEW.status = 'fenced'
                    AND NEW.pod_uid IS NOT DISTINCT FROM OLD.pod_uid
                    AND NEW.pvc_uid IS NOT DISTINCT FROM OLD.pvc_uid
                    AND NEW.seed_configmap_uid
                        IS NOT DISTINCT FROM OLD.seed_configmap_uid
                    AND NEW.service_uid IS NOT DISTINCT FROM OLD.service_uid
                    AND NULLIF(NEW.fence_pod_uid, '') IS NOT NULL
                    AND (NEW.pvc_name IS NULL OR NEW.retained_pvc_uid IS NOT NULL
                         OR (NEW.cleanup_disposition='retain' AND NEW.pvc_uid IS NOT NULL)
                         OR NULLIF(NEW.fence_pvc_uid, '') IS NOT NULL)
                    AND (NEW.seed_configmap_name IS NULL
                         OR NULLIF(NEW.fence_configmap_uid, '') IS NOT NULL)
                    AND (NEW.service_name IS NULL
                         OR NEW.retained_service_uid IS NOT NULL
                         OR (NEW.cleanup_disposition='retain' AND NEW.service_uid IS NOT NULL)
                         OR NULLIF(NEW.fence_service_uid, '') IS NOT NULL)
                    AND NEW.fenced_at
                        IS NOT DISTINCT FROM transaction_timestamp()
                    AND NEW.gc_after >= NEW.fenced_at + interval '10 minutes'
                    AND NEW.resolved_at IS NULL)
                OR (OLD.status = 'fenced' AND NEW.status = 'fenced'
                    AND NEW.pod_uid IS NOT DISTINCT FROM OLD.pod_uid
                    AND NEW.pvc_uid IS NOT DISTINCT FROM OLD.pvc_uid
                    AND NEW.seed_configmap_uid
                        IS NOT DISTINCT FROM OLD.seed_configmap_uid
                    AND NEW.service_uid IS NOT DISTINCT FROM OLD.service_uid
                    AND (OLD.fence_pod_uid IS NULL
                         OR NEW.fence_pod_uid IS NOT DISTINCT FROM OLD.fence_pod_uid)
                    AND (OLD.fence_pvc_uid IS NULL
                         OR NEW.fence_pvc_uid IS NOT DISTINCT FROM OLD.fence_pvc_uid)
                    AND (OLD.fence_configmap_uid IS NULL
                         OR NEW.fence_configmap_uid
                            IS NOT DISTINCT FROM OLD.fence_configmap_uid)
                    AND (OLD.fence_service_uid IS NULL
                         OR NEW.fence_service_uid
                            IS NOT DISTINCT FROM OLD.fence_service_uid)
                    AND (
                        NEW.fence_pod_uid IS DISTINCT FROM OLD.fence_pod_uid
                        OR NEW.fence_pvc_uid IS DISTINCT FROM OLD.fence_pvc_uid
                        OR NEW.fence_configmap_uid
                            IS DISTINCT FROM OLD.fence_configmap_uid
                        OR NEW.fence_service_uid
                            IS DISTINCT FROM OLD.fence_service_uid
                    )
                    AND NEW.fenced_at
                        IS NOT DISTINCT FROM transaction_timestamp()
                    AND NEW.gc_after >= NEW.fenced_at + interval '10 minutes'
                    AND NEW.resolved_at IS NULL
                    AND EXISTS (
                        SELECT 1 FROM public.threads thread
                         WHERE thread.id = OLD.thread_id
                           AND thread.runtime_generation = OLD.runtime_generation
                           AND thread.runtime_retirement_token IS NOT NULL
                           AND thread.runtime_retirement_permanent = true
                           AND thread.runtime_retirement_authorized_at IS NOT NULL
                           AND thread.runtime_retirement_context
                                ->'workspace_provision_intent'->>'attempt_id'
                               = OLD.attempt_id::text
                    ))
                OR (OLD.status = 'fenced' AND NEW.status = 'retired'
                    AND NEW.pod_uid IS NOT DISTINCT FROM OLD.pod_uid
                    AND NEW.pvc_uid IS NOT DISTINCT FROM OLD.pvc_uid
                    AND NEW.seed_configmap_uid
                        IS NOT DISTINCT FROM OLD.seed_configmap_uid
                    AND NEW.service_uid IS NOT DISTINCT FROM OLD.service_uid
                    AND NEW.fence_pod_uid IS NOT DISTINCT FROM OLD.fence_pod_uid
                    AND NEW.fence_pvc_uid IS NOT DISTINCT FROM OLD.fence_pvc_uid
                    AND NEW.fence_configmap_uid
                        IS NOT DISTINCT FROM OLD.fence_configmap_uid
                    AND NEW.fence_service_uid
                        IS NOT DISTINCT FROM OLD.fence_service_uid
                    AND NEW.fenced_at IS NOT DISTINCT FROM OLD.fenced_at
                    AND NEW.gc_after IS NOT DISTINCT FROM OLD.gc_after
                    AND OLD.gc_after <= transaction_timestamp()
                    AND NEW.resolved_at
                        IS NOT DISTINCT FROM transaction_timestamp())
           ) THEN
            RAISE EXCEPTION
                'workspace provision intent % transition is not exact', OLD.attempt_id
                USING ERRCODE = '23514',
                      CONSTRAINT = 'thread_workspace_provision_intent_authority';
        END IF;
        RETURN NEW;
    END IF;

    IF NEW.status <> 'planned' THEN
        RAISE EXCEPTION
            'workspace provision intent % must begin planned', NEW.attempt_id
            USING ERRCODE = '23514',
                  CONSTRAINT = 'thread_workspace_provision_intent_authority';
    END IF;
    IF NEW.pod_uid IS NOT NULL
       OR NEW.pvc_uid IS NOT NULL
       OR NEW.seed_configmap_uid IS NOT NULL
       OR NEW.service_uid IS NOT NULL THEN
        RAISE EXCEPTION
            'workspace provision intent % cannot pre-publish a resource UID',
            NEW.attempt_id
            USING ERRCODE = '23514',
                  CONSTRAINT = 'thread_workspace_provision_intent_authority';
    END IF;
    SELECT * INTO thread_row
      FROM public.threads
     WHERE id = NEW.thread_id
     FOR KEY SHARE;
    SELECT count(*) INTO inverse_count
      FROM public.agents
     WHERE thread_id = NEW.thread_id;
    IF thread_row.id IS NULL
       OR thread_row.execution_lane <> 'pinned'
       OR thread_row.runtime_generation IS DISTINCT FROM NEW.runtime_generation
       OR thread_row.runtime_retirement_token IS NOT NULL
       OR thread_row.status NOT IN ('created', 'active', 'awaiting_user', 'suspended')
       OR thread_row.agent_id IS DISTINCT FROM NEW.created_agent_id
       OR thread_row.runtime_attach_token IS DISTINCT FROM NEW.created_attach_token
       OR (
            thread_row.control_admission_agent_id IS NOT NULL
            AND thread_row.control_admission_agent_id
                IS DISTINCT FROM NEW.created_agent_id
       )
       OR (NEW.created_agent_id IS NULL AND inverse_count <> 0)
       OR (NEW.created_agent_id IS NOT NULL AND (
            inverse_count <> 1
            OR NOT EXISTS (
                SELECT 1 FROM public.agents agent
                 WHERE agent.id = NEW.created_agent_id
                   AND agent.thread_id = NEW.thread_id
            )
       )) THEN
        RAISE EXCEPTION
            'workspace provision intent % lacks open pinned authority', NEW.attempt_id
            USING ERRCODE = '23514',
                  CONSTRAINT = 'thread_workspace_provision_intent_authority';
    END IF;
    RETURN NEW;
END;
$$;


CREATE OR REPLACE FUNCTION public.pinned_retirement_workspace_provision_intent_retired(subject_thread_id uuid, runtime_generation uuid, captured_intent jsonb, require_all_resource_fences boolean) RETURNS boolean
    LANGUAGE sql STABLE
    AS $_$
    SELECT COALESCE(CASE
        WHEN captured_intent IS NULL
             OR captured_intent IN ('null'::jsonb, '{}'::jsonb) THEN
            NOT EXISTS (
                SELECT 1
                  FROM public.thread_workspace_provision_intents intent
                 WHERE intent.thread_id = subject_thread_id
                   AND intent.status IN ('planned', 'revoking', 'fenced')
            )
        WHEN jsonb_typeof(captured_intent) = 'object'
             AND COALESCE(captured_intent->>'attempt_id', '') ~
                '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
             AND captured_intent->>'thread_id' = subject_thread_id::text
             AND captured_intent->>'runtime_generation' = runtime_generation::text
             AND NULLIF(captured_intent->>'namespace', '') IS NOT NULL
             AND NULLIF(captured_intent->>'pod_name', '') IS NOT NULL
             AND NULLIF(captured_intent->>'network_tier', '') IS NOT NULL
             AND COALESCE(captured_intent->>'manifest_fingerprint', '') ~
                '^[0-9a-f]{64}$'
             AND COALESCE(captured_intent->>'status', '')
                IN ('planned', 'fenced', 'retired')
             AND jsonb_typeof(captured_intent->'previous_binding') = 'object'
             AND (
                 NULLIF(captured_intent->>'created_agent_id', '') IS NULL
             ) = (
                 NULLIF(captured_intent->>'created_attach_token', '') IS NULL
             )
             AND (
                 NULLIF(captured_intent->>'pvc_name', '') IS NULL
             ) = (
                 NULLIF(captured_intent->>'service_name', '') IS NULL
             ) THEN
            EXISTS (
                SELECT 1
                  FROM public.thread_workspace_provision_intents intent
                 WHERE intent.attempt_id::text = captured_intent->>'attempt_id'
                   AND intent.thread_id = subject_thread_id
                   AND intent.runtime_generation = runtime_generation
                   AND intent.status IN ('fenced', 'retired')
                   AND (NOT require_all_resource_fences
                        OR intent.cleanup_disposition IS NULL
                        OR (intent.cleanup_disposition='purge'
                            AND intent.purge_completed_at IS NOT NULL))
                   AND (
                       NOT require_all_resource_fences
                       OR intent.status = 'retired'
                       OR (
                           NULLIF(intent.fence_pod_uid, '') IS NOT NULL
                           AND (
                               intent.pvc_name IS NULL
                               OR NULLIF(intent.fence_pvc_uid, '') IS NOT NULL
                           )
                           AND (
                               intent.seed_configmap_name IS NULL
                               OR NULLIF(intent.fence_configmap_uid, '') IS NOT NULL
                           )
                           AND (
                               intent.service_name IS NULL
                               OR NULLIF(intent.fence_service_uid, '') IS NOT NULL
                           )
                       )
                   )
                   AND COALESCE(intent.created_agent_id::text, '') =
                       COALESCE(captured_intent->>'created_agent_id', '')
                   AND COALESCE(intent.created_attach_token::text, '') =
                       COALESCE(captured_intent->>'created_attach_token', '')
                   AND intent.namespace = captured_intent->>'namespace'
                   AND intent.pod_name = captured_intent->>'pod_name'
                   AND COALESCE(intent.pvc_name, '') =
                       COALESCE(captured_intent->>'pvc_name', '')
                   AND COALESCE(intent.seed_configmap_name, '') =
                       COALESCE(captured_intent->>'seed_configmap_name', '')
                   AND COALESCE(intent.service_name, '') =
                       COALESCE(captured_intent->>'service_name', '')
                   AND intent.network_tier = captured_intent->>'network_tier'
                   AND intent.manifest_fingerprint =
                       captured_intent->>'manifest_fingerprint'
                   AND intent.previous_binding IS NOT DISTINCT FROM
                       captured_intent->'previous_binding'
                   AND COALESCE(intent.retained_binding_generation::text, '') =
                       COALESCE(
                           captured_intent->>'retained_binding_generation', ''
                       )
                   AND COALESCE(intent.retained_pvc_uid, '') =
                       COALESCE(captured_intent->>'retained_pvc_uid', '')
                   AND COALESCE(intent.retained_service_uid, '') =
                       COALESCE(captured_intent->>'retained_service_uid', '')
                   AND COALESCE(intent.pod_uid, '') =
                       COALESCE(captured_intent->>'pod_uid', '')
                   AND COALESCE(intent.pvc_uid, '') =
                       COALESCE(captured_intent->>'pvc_uid', '')
                   AND COALESCE(intent.seed_configmap_uid, '') =
                       COALESCE(captured_intent->>'seed_configmap_uid', '')
                   AND COALESCE(intent.service_uid, '') =
                       COALESCE(captured_intent->>'service_uid', '')
                   AND NOT EXISTS (
                       SELECT 1
                         FROM public.thread_workspace_provision_intents other
                        WHERE other.thread_id = subject_thread_id
                          AND other.attempt_id <> intent.attempt_id
                          AND other.status IN ('planned', 'revoking', 'fenced')
                   )
            )
        ELSE false
    END, false);
$_$;


CREATE TABLE public.thread_workspace_provision_stop_receipts (
    attempt_id uuid PRIMARY KEY,
    thread_id uuid NOT NULL,
    runtime_generation uuid NOT NULL,
    retirement_token uuid NOT NULL,
    namespace text NOT NULL CHECK (namespace <> ''),
    pod_name text NOT NULL CHECK (pod_name <> ''),
    pod_uid text NOT NULL CHECK (pod_uid <> ''),
    observed_at timestamptz NOT NULL DEFAULT transaction_timestamp()
);

CREATE FUNCTION public.enforce_pinned_workspace_provision_stop_receipt()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    t public.threads%ROWTYPE;
    i public.thread_workspace_provision_intents%ROWTYPE;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'workspace provision stop receipt is immutable'
          USING ERRCODE='23514', CONSTRAINT='pinned_workspace_provision_stop_authority';
    END IF;
    SELECT * INTO t FROM public.threads WHERE id=NEW.thread_id FOR UPDATE;
    SELECT * INTO i FROM public.thread_workspace_provision_intents
      WHERE attempt_id=NEW.attempt_id FOR UPDATE;
    IF t.id IS NULL OR i.attempt_id IS NULL
       OR t.execution_lane <> 'pinned'
       OR t.runtime_generation IS DISTINCT FROM NEW.runtime_generation
       OR t.runtime_retirement_token IS DISTINCT FROM NEW.retirement_token
       OR t.runtime_retirement_authorized_at IS NULL
       OR i.thread_id IS DISTINCT FROM NEW.thread_id
       OR i.runtime_generation IS DISTINCT FROM NEW.runtime_generation
       OR i.cleanup_retirement_token IS DISTINCT FROM NEW.retirement_token
       OR i.status <> 'revoking'
       OR i.namespace IS DISTINCT FROM NEW.namespace
       OR i.pod_name IS DISTINCT FROM NEW.pod_name
       OR i.pod_uid IS DISTINCT FROM NEW.pod_uid
       OR t.runtime_retirement_context->'workspace_provision_intent'->>'attempt_id'
           IS DISTINCT FROM NEW.attempt_id::text
       OR t.runtime_retirement_context->'workspace_provision_intent'->>'pod_uid'
           IS DISTINCT FROM NEW.pod_uid
       OR NEW.observed_at IS DISTINCT FROM transaction_timestamp() THEN
        RAISE EXCEPTION 'workspace provision stop lacks exact retirement authority'
          USING ERRCODE='23514', CONSTRAINT='pinned_workspace_provision_stop_authority';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER trg_pinned_workspace_provision_stop_authority
BEFORE INSERT OR UPDATE OR DELETE ON public.thread_workspace_provision_stop_receipts
FOR EACH ROW EXECUTE FUNCTION public.enforce_pinned_workspace_provision_stop_receipt();

CREATE FUNCTION public.enforce_pinned_workspace_retained_creation()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    t public.threads%ROWTYPE;
    source public.thread_workspace_provision_intents%ROWTYPE;
    successor public.thread_workspace_provision_intents%ROWTYPE;
    captured jsonb;
BEGIN
    SELECT * INTO t FROM public.threads WHERE id=NEW.thread_id FOR UPDATE;
    IF TG_OP='INSERT' THEN
        IF NEW.cleanup_disposition IS NOT NULL OR NEW.cleanup_retirement_token IS NOT NULL
           OR NEW.purge_completed_at IS NOT NULL
           OR NEW.creation_effects_admitted_at IS NOT NULL
           OR NEW.retained_resume_generation IS NOT NULL
           OR NEW.retained_successor_attempt_id IS NOT NULL THEN
            RAISE EXCEPTION 'new workspace intent cannot preclaim retirement'
              USING ERRCODE='23514', CONSTRAINT='pinned_workspace_retention_authority';
        END IF;
        IF NEW.retained_source_attempt_id IS NOT NULL THEN
            SELECT * INTO source FROM public.thread_workspace_provision_intents
              WHERE attempt_id=NEW.retained_source_attempt_id FOR UPDATE;
            IF source.attempt_id IS NULL OR source.thread_id IS DISTINCT FROM NEW.thread_id
               OR source.status <> 'retired' OR source.cleanup_disposition <> 'retain'
               OR source.retained_resume_generation IS DISTINCT FROM NEW.runtime_generation
               OR source.retained_successor_attempt_id IS NOT NULL
               OR source.purge_completed_at IS NOT NULL
               OR source.namespace IS DISTINCT FROM NEW.namespace
               OR source.pvc_name IS DISTINCT FROM NEW.pvc_name
               OR source.service_name IS DISTINCT FROM NEW.service_name
               OR COALESCE(source.pvc_uid,source.retained_pvc_uid) IS NULL
               OR COALESCE(source.service_uid,source.retained_service_uid) IS NULL
               OR NEW.retained_pvc_uid IS DISTINCT FROM COALESCE(source.pvc_uid,source.retained_pvc_uid)
               OR NEW.retained_service_uid IS DISTINCT FROM COALESCE(source.service_uid,source.retained_service_uid)
               OR NEW.retained_binding_generation IS DISTINCT FROM source.retained_binding_generation
               OR NEW.previous_binding IS DISTINCT FROM source.previous_binding
               OR COALESCE(NULLIF(t.metadata->'_workspace_binding','null'::jsonb),'{}'::jsonb)
                   IS DISTINCT FROM source.previous_binding
               OR t.metadata->>'_pinned_retained_creation_attempt'
                   IS DISTINCT FROM source.attempt_id::text THEN
                RAISE EXCEPTION 'workspace retained source is not claimable'
                  USING ERRCODE='23514', CONSTRAINT='pinned_workspace_retention_authority';
            END IF;
        END IF;
        RETURN NEW;
    END IF;
    IF NEW.creation_effects_admitted_at IS DISTINCT FROM OLD.creation_effects_admitted_at THEN
        IF OLD.creation_effects_admitted_at IS NOT NULL
           OR NEW.creation_effects_admitted_at IS DISTINCT FROM transaction_timestamp()
           OR OLD.retained_source_attempt_id IS NULL OR OLD.status <> 'planned'
           OR NEW.status <> 'planned' OR t.id IS NULL
           OR t.runtime_generation IS DISTINCT FROM NEW.runtime_generation
           OR t.runtime_retirement_token IS NOT NULL
           OR t.status NOT IN ('created','active','awaiting_user','suspended')
           OR t.metadata->'workspace_container'->>'_workspace_provision_attempt'
               IS DISTINCT FROM NEW.attempt_id::text
           OR t.metadata->'workspace_container'->>'_workspace_provision_generation'
               IS DISTINCT FROM NEW.runtime_generation::text THEN
            RAISE EXCEPTION 'workspace effects admission is not current or is immutable'
              USING ERRCODE='23514', CONSTRAINT='pinned_workspace_retention_authority';
        END IF;
    END IF;
    IF NEW.retained_source_attempt_id IS DISTINCT FROM OLD.retained_source_attempt_id
       OR (OLD.retained_resume_generation IS NOT NULL
           AND NEW.retained_resume_generation IS DISTINCT FROM OLD.retained_resume_generation)
       OR (OLD.retained_successor_attempt_id IS NOT NULL
           AND NEW.retained_successor_attempt_id IS DISTINCT FROM OLD.retained_successor_attempt_id)
       OR (OLD.purge_completed_at IS NOT NULL
           AND NEW.purge_completed_at IS DISTINCT FROM OLD.purge_completed_at) THEN
        RAISE EXCEPTION 'workspace retained lineage is immutable'
          USING ERRCODE='23514', CONSTRAINT='pinned_workspace_retention_authority';
    END IF;
    IF NEW.cleanup_disposition IS DISTINCT FROM OLD.cleanup_disposition
       OR NEW.cleanup_retirement_token IS DISTINCT FROM OLD.cleanup_retirement_token
       OR NEW.purge_completed_at IS DISTINCT FROM OLD.purge_completed_at THEN
        captured := t.runtime_retirement_context->'workspace_provision_intent';
        IF t.id IS NULL OR t.execution_lane <> 'pinned'
           OR t.runtime_generation IS DISTINCT FROM NEW.runtime_generation
           OR t.runtime_retirement_token IS DISTINCT FROM NEW.cleanup_retirement_token
           OR t.runtime_retirement_authorized_at IS NULL
           OR captured->>'attempt_id' IS DISTINCT FROM NEW.attempt_id::text
           OR NEW.retained_resume_generation IS NOT NULL
           OR NEW.retained_successor_attempt_id IS NOT NULL
           OR NEW.cleanup_disposition IS DISTINCT FROM
               (CASE WHEN t.runtime_retirement_permanent THEN 'purge' ELSE 'retain' END)
           OR NOT (
               (OLD.cleanup_disposition IS NULL AND OLD.status IN ('planned','revoking')
                AND NEW.status='revoking' AND NEW.purge_completed_at IS NULL)
               OR (OLD.cleanup_disposition='retain' AND NEW.cleanup_disposition='purge'
                   AND OLD.status IN ('fenced','retired') AND NEW.status=OLD.status
                   AND t.status='ended' AND NEW.purge_completed_at IS NULL)
               OR (OLD.cleanup_disposition='purge' AND NEW.cleanup_disposition='purge'
                   AND NEW.cleanup_retirement_token=OLD.cleanup_retirement_token
                   AND OLD.purge_completed_at IS NULL
                   AND NEW.purge_completed_at=transaction_timestamp()
                   AND NEW.status IN ('fenced','retired'))
           ) THEN
            RAISE EXCEPTION 'workspace cleanup disposition lacks exact retirement'
              USING ERRCODE='23514', CONSTRAINT='pinned_workspace_retention_authority';
        END IF;
    END IF;
    IF NEW.retained_resume_generation IS DISTINCT FROM OLD.retained_resume_generation THEN
        -- This edge is issued only by the AFTER thread Resume trigger below.
        IF pg_trigger_depth() < 2 OR t.id IS NULL OR t.status <> 'created'
           OR t.runtime_retirement_token IS NOT NULL
           OR t.runtime_generation IS DISTINCT FROM NEW.retained_resume_generation
           OR OLD.cleanup_disposition <> 'retain' OR OLD.status NOT IN ('fenced','retired')
           OR OLD.retained_successor_attempt_id IS NOT NULL THEN
            RAISE EXCEPTION 'workspace retained generation requires Resume'
              USING ERRCODE='23514', CONSTRAINT='pinned_workspace_retention_authority';
        END IF;
    END IF;
    IF NEW.retained_successor_attempt_id IS DISTINCT FROM OLD.retained_successor_attempt_id THEN
        SELECT * INTO successor FROM public.thread_workspace_provision_intents
          WHERE attempt_id=NEW.retained_successor_attempt_id FOR SHARE;
        IF successor.attempt_id IS NULL OR successor.thread_id <> NEW.thread_id
           OR successor.retained_source_attempt_id IS DISTINCT FROM NEW.attempt_id
           OR successor.runtime_generation IS DISTINCT FROM NEW.retained_resume_generation
           OR successor.status <> 'planned' OR NEW.cleanup_disposition <> 'retain'
           OR NEW.status <> 'retired' OR t.runtime_retirement_token IS NOT NULL
           OR t.runtime_generation IS DISTINCT FROM successor.runtime_generation THEN
            RAISE EXCEPTION 'workspace retained successor claim is not exact'
              USING ERRCODE='23514', CONSTRAINT='pinned_workspace_retention_authority';
        END IF;
    END IF;
    IF NEW.retained_source_attempt_id IS NOT NULL
       AND (NEW.pod_uid IS DISTINCT FROM OLD.pod_uid
            OR NEW.pvc_uid IS DISTINCT FROM OLD.pvc_uid
            OR NEW.service_uid IS DISTINCT FROM OLD.service_uid
            OR NEW.seed_configmap_uid IS DISTINCT FROM OLD.seed_configmap_uid)
       AND (NEW.creation_effects_admitted_at IS NULL OR t.id IS NULL
            OR t.runtime_generation IS DISTINCT FROM NEW.runtime_generation
            OR t.runtime_retirement_token IS NOT NULL
            OR t.status NOT IN ('created','active','awaiting_user','suspended')
            OR t.metadata->'workspace_container'->>'_workspace_provision_attempt'
                IS DISTINCT FROM NEW.attempt_id::text
            OR t.metadata->'workspace_container'->>'_workspace_provision_generation'
                IS DISTINCT FROM NEW.runtime_generation::text) THEN
        RAISE EXCEPTION 'retained workspace effects lack current admitted authority'
          USING ERRCODE='23514', CONSTRAINT='pinned_workspace_retention_authority';
    END IF;
    IF OLD.status='revoking' AND NEW.status='fenced'
       AND NEW.cleanup_disposition IS NOT NULL AND NEW.pod_uid IS NULL
       AND NOT (NEW.retained_source_attempt_id IS NOT NULL
                AND NEW.creation_effects_admitted_at IS NULL) THEN
        RAISE EXCEPTION 'workspace issued Pod identity is unresolved'
          USING ERRCODE='23514', CONSTRAINT='pinned_workspace_retention_authority';
    END IF;
    IF OLD.status='revoking' AND NEW.status='fenced'
       AND NEW.cleanup_disposition IS NOT NULL AND NEW.pod_uid IS NOT NULL
       AND NOT EXISTS (
           SELECT 1 FROM public.thread_workspace_provision_stop_receipts proof
           WHERE proof.attempt_id=NEW.attempt_id AND proof.thread_id=NEW.thread_id
             AND proof.runtime_generation=NEW.runtime_generation
             AND proof.namespace=NEW.namespace AND proof.pod_name=NEW.pod_name
             AND proof.pod_uid=NEW.pod_uid
       ) THEN
        RAISE EXCEPTION 'workspace original Pod process stop is unproven'
          USING ERRCODE='23514', CONSTRAINT='pinned_workspace_retention_authority';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER trg_thread_workspace_retention_authority
BEFORE INSERT OR UPDATE ON public.thread_workspace_provision_intents
FOR EACH ROW EXECUTE FUNCTION public.enforce_pinned_workspace_retained_creation();

CREATE FUNCTION public.grant_pinned_retained_creation_resume()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    source public.thread_workspace_provision_intents%ROWTYPE;
    successor_id uuid := public.uuid_generate_v4();
    pending jsonb;
BEGIN
    IF OLD.execution_lane <> 'pinned' OR OLD.status <> 'ended' OR NEW.status <> 'created'
       OR OLD.metadata->>'_pinned_retained_creation_attempt' IS NULL THEN
        RETURN NEW;
    END IF;
    SELECT * INTO source FROM public.thread_workspace_provision_intents
      WHERE attempt_id::text=OLD.metadata->>'_pinned_retained_creation_attempt' FOR UPDATE;
    IF source.attempt_id IS NULL OR source.thread_id <> OLD.id
       OR source.runtime_generation IS DISTINCT FROM OLD.runtime_generation
       OR source.cleanup_disposition <> 'retain' OR source.status <> 'retired'
       OR source.retained_resume_generation IS NOT NULL
       OR source.retained_successor_attempt_id IS NOT NULL
       OR COALESCE(source.pvc_uid,source.retained_pvc_uid) IS NULL
       OR COALESCE(source.service_uid,source.retained_service_uid) IS NULL
       OR OLD.runtime_retirement_token IS NOT NULL OR NEW.runtime_retirement_token IS NOT NULL
       OR NEW.runtime_generation=OLD.runtime_generation
       OR NEW.metadata->>'_pinned_retained_creation_attempt'
           IS DISTINCT FROM source.attempt_id::text
       OR COALESCE(NULLIF(OLD.metadata->'_workspace_binding','null'::jsonb),'{}'::jsonb)
           IS DISTINCT FROM source.previous_binding
       OR COALESCE(NULLIF(NEW.metadata->'_workspace_binding','null'::jsonb),'{}'::jsonb)
           IS DISTINCT FROM source.previous_binding
       OR NOT EXISTS (
           SELECT 1 FROM public.thread_runtime_retirement_outcomes outcome
           WHERE outcome.thread_id=OLD.id AND outcome.runtime_generation=OLD.runtime_generation
             AND outcome.retirement_token=source.cleanup_retirement_token
             AND outcome.outcome='settled' AND outcome.disposition='ended'
             AND outcome.permanent=false
       ) THEN
        RAISE EXCEPTION 'workspace retained Resume source is not exact'
          USING ERRCODE='23514', CONSTRAINT='pinned_workspace_retention_authority';
    END IF;
    UPDATE public.thread_workspace_provision_intents
      SET retained_resume_generation=NEW.runtime_generation
      WHERE attempt_id=source.attempt_id;
    INSERT INTO public.thread_workspace_provision_intents (
        attempt_id,thread_id,runtime_generation,namespace,pod_name,pvc_name,
        seed_configmap_name,service_name,network_tier,manifest_fingerprint,
        previous_binding,retained_binding_generation,retained_pvc_uid,
        retained_service_uid,retained_source_attempt_id
    ) VALUES (
        successor_id,NEW.id,NEW.runtime_generation,source.namespace,source.pod_name,
        source.pvc_name,source.seed_configmap_name,source.service_name,source.network_tier,
        source.manifest_fingerprint,source.previous_binding,source.retained_binding_generation,
        COALESCE(source.pvc_uid,source.retained_pvc_uid),
        COALESCE(source.service_uid,source.retained_service_uid),source.attempt_id
    );
    UPDATE public.thread_workspace_provision_intents
      SET retained_successor_attempt_id=successor_id WHERE attempt_id=source.attempt_id;
    pending := COALESCE(NEW.metadata->'workspace_container','{}'::jsonb)
        || jsonb_build_object('status','pending','provisioner','k8s',
            '_workspace_provision_attempt',successor_id::text,
            '_workspace_provision_generation',NEW.runtime_generation::text,
            '_runtime_incarnation',NULL,'_canvas_workspace_generation',NULL,
            'pod_ip',NULL,'pod_name',NULL,'host',NULL,'port',NULL,'ide_host',NULL,'ide_port',NULL);
    UPDATE public.threads SET metadata=jsonb_set(metadata,'{workspace_container}',pending,true)
      WHERE id=NEW.id AND runtime_generation=NEW.runtime_generation;
    RETURN NEW;
END;
$$;
CREATE TRIGGER trg_thread_retained_creation_resume
AFTER UPDATE OF status ON public.threads
FOR EACH ROW EXECUTE FUNCTION public.grant_pinned_retained_creation_resume();

COMMIT;
