-- migration: 0330_pinned_partial_creation_abort_retirement.sql
-- description: Exact abort lineage and inert-name proof for partial workspace retirement.
-- depends-on: 0329_validate_vm_job_creation_audit_owners.sql
-- transactional: yes
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

-- An abort is local process proof only. This path authorizes capturing the old
-- create obligation; external zero still requires the adapter's causal fence.
CREATE FUNCTION public.pinned_workspace_provision_abort_path(subject uuid, current_generation uuid, source_attempt uuid)
RETURNS uuid[] LANGUAGE sql STABLE AS $$
 WITH RECURSIVE path AS (
   SELECT intent.runtime_generation AS generation,
          ARRAY[intent.runtime_generation] AS generations, 0 AS depth
     FROM public.thread_workspace_provision_intents intent
    WHERE intent.thread_id=subject AND intent.attempt_id=source_attempt
      AND intent.created_agent_id IS NOT NULL AND intent.created_attach_token IS NOT NULL
      AND intent.pod_uid IS NULL AND intent.retained_source_attempt_id IS NULL
   UNION ALL
   SELECT abort.successor_generation, path.generations||abort.successor_generation, path.depth+1
     FROM path JOIN public.thread_runtime_attach_abort_outcomes abort
       ON abort.thread_id=subject AND abort.runtime_generation=path.generation
    WHERE path.depth<16 AND abort.successor_generation<>ALL(path.generations)
      AND abort.release_kind='process_zero'
      AND abort.quiescence_protocol='agent_attach_not_started_v1'
      AND abort.workspace_generation IS NULL AND abort.workspace_runtime_incarnation IS NULL
      AND (path.depth>0 OR EXISTS (
         SELECT 1 FROM public.thread_workspace_provision_intents intent
          WHERE intent.attempt_id=source_attempt AND intent.created_agent_id=abort.agent_id
            AND intent.created_attach_token=abort.runtime_attach_token))
 ) SELECT generations FROM path WHERE generation=current_generation AND depth>0;
$$;

-- Shape evaluation stays pure for immutable receipts after deletion. Authority
-- paths below additionally validate the actual abort lineage under the lock.
CREATE FUNCTION public.pinned_workspace_provision_capture_shape_is_current(subject uuid, current_generation uuid, captured jsonb)
RETURNS boolean LANGUAGE sql IMMUTABLE AS $$
 SELECT COALESCE(captured->>'thread_id'=subject::text AND (
  (captured->>'runtime_generation'=current_generation::text
   AND NOT (captured ? 'retirement_runtime_generation') AND NOT (captured ? 'attach_abort_path'))
  OR (captured->>'runtime_generation'<>current_generation::text
   AND captured->>'retirement_runtime_generation'=current_generation::text
   AND captured->>'pod_uid' IS NULL
   AND CASE WHEN jsonb_typeof(captured->'attach_abort_path')='array' THEN
     jsonb_array_length(captured->'attach_abort_path') BETWEEN 2 AND 17
     AND captured->'attach_abort_path'->>0=captured->>'runtime_generation'
     AND captured->'attach_abort_path'->>-1=current_generation::text
     AND NOT EXISTS (SELECT 1 FROM jsonb_array_elements_text(captured->'attach_abort_path') value
       WHERE value !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
   ELSE false END
  )),false);
$$;

CREATE FUNCTION public.pinned_workspace_provision_capture_is_current(subject uuid, current_generation uuid, captured jsonb)
RETURNS boolean LANGUAGE sql STABLE AS $$
 SELECT COALESCE(
   captured->>'thread_id'=subject::text AND (
     (captured->>'runtime_generation'=current_generation::text
       AND NOT (captured ? 'retirement_runtime_generation') AND NOT (captured ? 'attach_abort_path'))
     OR (captured->>'runtime_generation'<>current_generation::text
       AND captured->>'retirement_runtime_generation'=current_generation::text
       AND captured->>'attempt_id' ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
       AND captured->'attach_abort_path'=to_jsonb(public.pinned_workspace_provision_abort_path(
           subject,current_generation,(captured->>'attempt_id')::uuid))
       AND (captured->'attach_abort_path'->>0)=captured->>'runtime_generation'
       AND jsonb_array_length(captured->'attach_abort_path') BETWEEN 2 AND 17
       AND captured->>'pod_uid' IS NULL
       AND EXISTS (SELECT 1 FROM public.threads t WHERE t.id=subject
         AND t.runtime_generation=current_generation AND t.agent_id IS NULL
         AND t.runtime_attach_token IS NULL AND t.control_admission_agent_id IS NULL
         AND t.runtime_retirement_permanent=true)
     )
   ), false);
$$;

CREATE OR REPLACE FUNCTION public.pinned_retirement_external_cleanup_expected(
    retirement_context jsonb,
    runtime_generation uuid,
    retirement_token uuid
)
RETURNS jsonb
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
    workspace jsonb;
    binding jsonb;
    agent_claim jsonb;
    workspace_intent jsonb;
    vm jsonb;
    protected_ro jsonb;
    backend text;
    workspace_evidence boolean := false;
    binding_evidence boolean := false;
    vm_evidence boolean := false;
    ro_live boolean := false;
    workspace_protocol text;
BEGIN
    IF jsonb_typeof(retirement_context) IS DISTINCT FROM 'object'
       OR COALESCE(jsonb_typeof(retirement_context->'workspace_container'), 'null')
          NOT IN ('object', 'null')
       OR COALESCE(jsonb_typeof(retirement_context->'workspace_binding'), 'null')
          NOT IN ('object', 'null')
       OR COALESCE(jsonb_typeof(retirement_context->'agent_workspace_claim'), 'null')
          NOT IN ('object', 'null')
       OR COALESCE(
            jsonb_typeof(retirement_context->'workspace_provision_intent'),
            'null'
          ) NOT IN ('object', 'null')
       OR COALESCE(jsonb_typeof(retirement_context->'vm'), 'null')
          NOT IN ('object', 'null')
       OR COALESCE(jsonb_typeof(retirement_context->'protected_ro'), 'null')
          NOT IN ('object', 'null') THEN
        RETURN NULL;
    END IF;

    workspace := CASE
        WHEN jsonb_typeof(retirement_context->'workspace_container') = 'object'
        THEN retirement_context->'workspace_container'
        ELSE '{}'::jsonb
    END;
    binding := CASE
        WHEN jsonb_typeof(retirement_context->'workspace_binding') = 'object'
        THEN retirement_context->'workspace_binding'
        ELSE '{}'::jsonb
    END;
    agent_claim := CASE
        WHEN jsonb_typeof(retirement_context->'agent_workspace_claim') = 'object'
        THEN retirement_context->'agent_workspace_claim'
        ELSE '{}'::jsonb
    END;
    workspace_intent := CASE
        WHEN jsonb_typeof(retirement_context->'workspace_provision_intent') = 'object'
        THEN retirement_context->'workspace_provision_intent'
        ELSE '{}'::jsonb
    END;
    vm := CASE
        WHEN jsonb_typeof(retirement_context->'vm') = 'object'
        THEN retirement_context->'vm'
        ELSE '{}'::jsonb
    END;
    protected_ro := CASE
        WHEN jsonb_typeof(retirement_context->'protected_ro') = 'object'
        THEN retirement_context->'protected_ro'
        ELSE '{}'::jsonb
    END;
    backend := COALESCE(retirement_context->>'workspace_backend', '');
    workspace_evidence := (
        COALESCE(workspace->>'status', '') NOT IN ('', 'deleted')
        OR workspace->>'_runtime_incarnation' IS NOT NULL
        OR workspace->>'_docker_workspace_lease_id' IS NOT NULL
        OR workspace->>'pod_ip' IS NOT NULL
        OR workspace->>'pod_name' IS NOT NULL
        OR workspace->>'host' IS NOT NULL
        OR workspace->>'port' IS NOT NULL
        OR workspace->>'ide_host' IS NOT NULL
        OR workspace->>'ide_port' IS NOT NULL
        OR workspace->>'_canvas_workspace_generation' IS NOT NULL
    );
    binding_evidence := binding <> '{}'::jsonb;
    vm_evidence := (
        COALESCE(vm->>'status', '') NOT IN ('', 'deleted')
        OR vm->>'provision_generation' IS NOT NULL
        OR vm->>'identity_provision_generation' IS NOT NULL
        OR vm->>'vm_uid' IS NOT NULL
        OR vm->>'_runtime_incarnation' IS NOT NULL
        OR vm->>'rootdisk_pvc_uid' IS NOT NULL
        OR vm->>'ssh_host' IS NOT NULL
        OR vm->>'ssh_port' IS NOT NULL
        OR vm->>'_canvas_workspace_generation' IS NOT NULL
    );
    ro_live := COALESCE(protected_ro->>'status', '') IN (
        'engaging', 'active', 'revoking'
    );

    IF agent_claim <> '{}'::jsonb AND (
        COALESCE(agent_claim->>'claim_id', '') !~
            '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        OR COALESCE(agent_claim->>'thread_id', '') !~
            '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        OR agent_claim->>'thread_id' IS DISTINCT FROM retirement_context->>'thread_id'
        OR COALESCE(agent_claim->>'created_runtime_generation', '') !~
            '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        OR COALESCE(agent_claim->>'create_attempt', '') !~
            '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        OR COALESCE(agent_claim->>'provisioner', '') NOT IN ('agent', 'persistent')
        OR NULLIF(agent_claim->>'pvc_name', '') IS NULL
        OR COALESCE(agent_claim->>'status', '') NOT IN ('planned', 'ready')
        OR (
            (agent_claim->>'status' = 'ready')
            IS DISTINCT FROM (NULLIF(agent_claim->>'pvc_uid', '') IS NOT NULL)
        )
    ) THEN
        RETURN NULL;
    END IF;

    IF workspace_intent <> '{}'::jsonb AND (
        COALESCE(workspace_intent->>'attempt_id', '') !~
            '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        OR workspace_intent->>'thread_id'
            IS DISTINCT FROM retirement_context->>'thread_id'
        OR NOT public.pinned_workspace_provision_capture_shape_is_current(
            (retirement_context->>'thread_id')::uuid,runtime_generation,workspace_intent)
        OR NULLIF(workspace_intent->>'namespace', '') IS NULL
        OR NULLIF(workspace_intent->>'pod_name', '') IS NULL
        OR NULLIF(workspace_intent->>'network_tier', '') IS NULL
        OR COALESCE(workspace_intent->>'manifest_fingerprint', '')
            !~ '^[0-9a-f]{64}$'
        OR COALESCE(workspace_intent->>'status', '')
            NOT IN ('planned', 'fenced', 'retired')
        OR jsonb_typeof(workspace_intent->'previous_binding')
            IS DISTINCT FROM 'object'
        OR workspace_intent->'previous_binding'
            IS DISTINCT FROM binding
        OR (
            NULLIF(workspace_intent->>'created_agent_id', '') IS NULL
        ) IS DISTINCT FROM (
            NULLIF(workspace_intent->>'created_attach_token', '') IS NULL
        )
        OR (
            NULLIF(workspace_intent->>'pvc_name', '') IS NULL
        ) IS DISTINCT FROM (
            NULLIF(workspace_intent->>'service_name', '') IS NULL
        )
    ) THEN
        RETURN NULL;
    END IF;

    IF workspace_intent <> '{}'::jsonb THEN
        IF vm_evidence OR backend NOT IN ('sandbox', 'virtual', 'none') THEN
            RETURN NULL;
        END IF;
        workspace_protocol := 'workspace_provision_fence_v1';
    ELSIF backend = 'sandbox' THEN
        IF vm_evidence OR (binding_evidence AND binding->>'kind' = 'virtual') THEN
            RETURN NULL;
        END IF;
        workspace_protocol := CASE
            WHEN workspace_evidence OR binding_evidence
            THEN 'sandbox_actuator_zero_v1'
            ELSE 'external_none_v1'
        END;
    ELSIF backend IN ('vm', 'remote') THEN
        IF workspace_evidence OR binding_evidence THEN
            RETURN NULL;
        END IF;
        workspace_protocol := CASE
            WHEN vm_evidence THEN 'workspace_actuator_zero_v1'
            ELSE 'external_none_v1'
        END;
    ELSIF backend = 'virtual' THEN
        IF workspace_evidence OR vm_evidence
           OR (binding_evidence AND (
               binding->>'kind' IS DISTINCT FROM 'virtual'
               OR COALESCE(binding->>'backing_id', '')
                    !~ '^rclone:[0-9a-f]{64}$'
           )) THEN
            RETURN NULL;
        END IF;
        workspace_protocol := CASE
            WHEN binding_evidence THEN 'virtual_backing_zero_v1'
            ELSE 'external_none_v1'
        END;
    ELSIF backend = 'none' THEN
        IF workspace_evidence OR binding_evidence OR vm_evidence THEN
            RETURN NULL;
        END IF;
        workspace_protocol := 'external_none_v1';
    ELSE
        RETURN NULL;
    END IF;

    IF ro_live AND (
        NULLIF(protected_ro->>'id', '') IS NULL
        OR NULLIF(protected_ro->>'runtime_generation', '') IS NULL
        OR NULLIF(protected_ro->>'engage_attempt', '') IS NULL
        OR NULLIF(protected_ro->>'grant_handle', '') IS NULL
    ) THEN
        RETURN NULL;
    END IF;

    RETURN jsonb_build_object(
        'version', 1,
        'runtime_generation', runtime_generation::text,
        'retirement_token', retirement_token::text,
        'cleanup_actor', 'orchestrator',
        'workspace_cleanup_protocol', workspace_protocol,
        'agent_workspace_cleanup_protocol', CASE
            WHEN agent_claim <> '{}'::jsonb
            THEN 'k8s_pvc_name_tombstone_v1' ELSE NULL
        END,
        'protected_reader_cleanup_protocol', CASE
            WHEN ro_live THEN 'protected_reader_zero_v1' ELSE NULL
        END,
        'captured_resources', jsonb_build_object(
            'workspace_backend', backend,
            'workspace_container', retirement_context->'workspace_container',
            'workspace_binding', retirement_context->'workspace_binding',
            'agent_workspace_claim', retirement_context->'agent_workspace_claim',
            'workspace_provision_intent',
                retirement_context->'workspace_provision_intent',
            'vm', retirement_context->'vm',
            'protected_ro', retirement_context->'protected_ro'
        )
    );
END;
$$;

-- Migrations and the runtime intentionally use the same owning SRW role. Keep
-- the canonicalizer unavailable to unrelated PUBLIC roles; it is a database
-- belt for stale/unaware writers, not a cryptographic attestation against a
-- malicious process holding the owner credential. The exact orchestrator
-- actuator remains the external-effect trust root.

CREATE TABLE public.thread_workspace_provision_inert_fence_receipts (
 attempt_id uuid PRIMARY KEY REFERENCES public.thread_workspace_provision_intents(attempt_id),
 thread_id uuid NOT NULL,
 source_runtime_generation uuid NOT NULL,
 retirement_runtime_generation uuid NOT NULL,
 retirement_token uuid NOT NULL,
 namespace text NOT NULL,
 pod_name text NOT NULL,
 fence_pod_uid text NOT NULL CHECK (length(fence_pod_uid)>0),
 protocol text NOT NULL CHECK (protocol='inert_pod_name_fence_v1'),
 observed_at timestamptz NOT NULL DEFAULT transaction_timestamp()
);
CREATE FUNCTION public.enforce_pinned_workspace_inert_fence_receipt() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE t public.threads%ROWTYPE; intent public.thread_workspace_provision_intents%ROWTYPE;
BEGIN
 IF TG_OP<>'INSERT' THEN
   RAISE EXCEPTION 'partial workspace inert fence receipt is immutable'
    USING ERRCODE='23514',CONSTRAINT='pinned_workspace_inert_fence_authority';
 END IF;
 SELECT * INTO t FROM public.threads WHERE id=NEW.thread_id FOR UPDATE;
 SELECT * INTO intent FROM public.thread_workspace_provision_intents WHERE attempt_id=NEW.attempt_id FOR SHARE;
 IF t.id IS NULL OR intent.attempt_id IS NULL OR intent.thread_id<>t.id
   OR t.execution_lane<>'pinned' OR t.runtime_generation<>NEW.retirement_runtime_generation
   OR t.runtime_retirement_token IS DISTINCT FROM NEW.retirement_token
   OR t.runtime_retirement_authorized_at IS NULL
   OR intent.cleanup_retirement_token IS DISTINCT FROM NEW.retirement_token
   OR intent.runtime_generation<>NEW.source_runtime_generation
   OR intent.status<>'revoking' OR intent.pod_uid IS NOT NULL
   OR intent.namespace<>NEW.namespace OR intent.pod_name<>NEW.pod_name
   OR NOT public.pinned_workspace_provision_capture_is_current(t.id,t.runtime_generation,
          t.runtime_retirement_context->'workspace_provision_intent')
   OR t.runtime_retirement_context->'workspace_provision_intent'->>'attempt_id'<>NEW.attempt_id::text
   OR NEW.observed_at IS DISTINCT FROM transaction_timestamp() THEN
   RAISE EXCEPTION 'partial workspace fence lacks exact retirement authority'
    USING ERRCODE='23514',CONSTRAINT='pinned_workspace_inert_fence_authority';
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER trg_pinned_workspace_inert_fence_authority BEFORE INSERT OR UPDATE OR DELETE
 ON public.thread_workspace_provision_inert_fence_receipts FOR EACH ROW
 EXECUTE FUNCTION public.enforce_pinned_workspace_inert_fence_receipt();

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
             AND public.pinned_workspace_provision_capture_is_current(subject_thread_id,runtime_generation,captured_intent)
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
                   AND intent.runtime_generation::text = captured_intent->>'runtime_generation'
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



CREATE OR REPLACE FUNCTION public.enforce_pinned_workspace_retained_creation()
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
           OR NOT public.pinned_workspace_provision_capture_is_current(t.id,t.runtime_generation,captured)
           OR captured->>'runtime_generation' IS DISTINCT FROM NEW.runtime_generation::text
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
                AND NEW.creation_effects_admitted_at IS NULL)
       AND NOT EXISTS (SELECT 1 FROM public.thread_workspace_provision_inert_fence_receipts proof
         WHERE proof.attempt_id=NEW.attempt_id AND proof.thread_id=NEW.thread_id
           AND proof.source_runtime_generation=NEW.runtime_generation
           AND proof.retirement_runtime_generation=t.runtime_generation
           AND proof.retirement_token=NEW.cleanup_retirement_token
           AND proof.namespace=NEW.namespace AND proof.pod_name=NEW.pod_name
           AND proof.fence_pod_uid=NEW.fence_pod_uid) THEN
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

COMMIT;
