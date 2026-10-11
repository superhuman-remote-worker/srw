-- migration:     0526_capture_preregistration_retired_agent_pod.sql
-- description:   A soft End that raced a claim-bearing agent Pod's create
--                settles on the orchestrator's exact pre-registration
--                receipt (that Pod stopped by UID, no agent ever bound).
--                capture_retired_pinned_agent_pod returned on agent_id IS
--                NULL, so the outcome named no Pod, and a later permanent
--                End refused "historical claimant lacks immutable actor
--                settlement" forever before fencing the claim. Record that
--                exact Pod and its claim for such a settlement too. A
--                claim-less agentless life stays unrecorded, as before; the
--                agent branches are verbatim.
-- depends-on:    0525_protected_source_lookup_types.sql
-- expected:      < 1s (a function replacement).
-- locks:         none on tables.
-- transactional: yes

BEGIN;
SET LOCAL lock_timeout                        = '2s';
SET LOCAL statement_timeout                   = '15min';
SET LOCAL idle_in_transaction_session_timeout = '5min';
SET LOCAL timezone                            = 'UTC';

CREATE OR REPLACE FUNCTION public.capture_retired_pinned_agent_pod()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    owner_row public.threads%ROWTYPE;
    actor_row public.agents%ROWTYPE;
    intent_row public.thread_agent_pod_provision_intents%ROWTYPE;
    claim_row public.thread_agent_workspace_claims%ROWTYPE;
    marker jsonb;
    captured_agent jsonb;
    receipt jsonb;
BEGIN
    IF NEW.retired_agent_pod IS NOT NULL THEN
        RAISE EXCEPTION 'retired Pod identity is server-owned'
            USING ERRCODE = '23514',
                  CONSTRAINT = 'retired_agent_pod_identity_authority';
    END IF;
    -- The existing insert-authority trigger first validates the exact live
    -- retirement and local-quiescence receipt.
    IF NEW.permanent THEN
        RETURN NEW;
    END IF;
    IF NEW.agent_id IS NULL THEN
        -- A Pod that never registered settles on the orchestrator's exact
        -- pre-registration receipt. A claim-bearing one must be named here:
        -- a later permanent End retires it before fencing the claim and has
        -- no agent to join on. Anything less exact records nothing.
        IF NEW.runtime_attach_token IS NOT NULL THEN
            RETURN NEW;
        END IF;
        SELECT * INTO owner_row FROM public.threads
         WHERE id = NEW.thread_id FOR SHARE;
        marker := owner_row.runtime_retirement_context->'agent_pod';
        receipt := owner_row.runtime_retirement_local_quiescence;
        IF jsonb_typeof(marker) IS DISTINCT FROM 'object'
           OR marker->>'protection_protocol' IS DISTINCT FROM 'finalizer_v1'
           OR jsonb_typeof(
                owner_row.runtime_retirement_context->'agent_workspace_claim'
              ) IS DISTINCT FROM 'object'
           OR owner_row.runtime_retirement_context->'agent_workspace_claim'
              = '{}'::jsonb
           OR owner_row.runtime_retirement_context->>'agent_id' IS NOT NULL
           OR owner_row.runtime_retirement_context->>'runtime_attach_token'
              IS NOT NULL
           OR jsonb_typeof(owner_row.runtime_retirement_context->'agent')
              = 'object'
              AND owner_row.runtime_retirement_context->'agent' <> '{}'::jsonb
           OR jsonb_typeof(receipt) IS DISTINCT FROM 'object'
           OR receipt->>'quiescence_actor' IS DISTINCT FROM 'orchestrator'
           OR receipt->>'agent_id' IS NOT NULL
           OR receipt->>'runtime_generation'
              IS DISTINCT FROM NEW.runtime_generation::text
           OR receipt->>'retirement_token'
              IS DISTINCT FROM owner_row.runtime_retirement_token::text
           OR receipt->>'agent_pod_name' IS DISTINCT FROM marker->>'pod_name'
           OR receipt->>'agent_pod_uid' IS DISTINCT FROM marker->>'pod_uid' THEN
            RETURN NEW;
        END IF;
        SELECT * INTO intent_row FROM public.thread_agent_pod_provision_intents
         WHERE attempt_id::text = marker->>'provision_attempt'
           AND thread_id = NEW.thread_id FOR SHARE;
        SELECT * INTO claim_row FROM public.thread_agent_workspace_claims
         WHERE claim_id = intent_row.workspace_claim_id FOR SHARE;
        IF intent_row.attempt_id IS NULL
           OR claim_row.claim_id IS NULL
           OR intent_row.runtime_generation IS DISTINCT FROM NEW.runtime_generation
           OR intent_row.status IS DISTINCT FROM 'published'
           OR intent_row.pod_name IS DISTINCT FROM marker->>'pod_name'
           OR intent_row.pod_uid IS DISTINCT FROM marker->>'pod_uid'
           OR intent_row.namespace IS DISTINCT FROM marker->>'namespace'
           OR intent_row.protection_protocol IS DISTINCT FROM 'finalizer_v1'
           OR claim_row.thread_id IS DISTINCT FROM NEW.thread_id
           OR claim_row.claim_id::text IS DISTINCT FROM
              owner_row.runtime_retirement_context->'agent_workspace_claim'->>'claim_id'
           OR claim_row.namespace IS DISTINCT FROM intent_row.namespace
           OR claim_row.provisioner IS DISTINCT FROM intent_row.provisioner
           OR claim_row.status IS DISTINCT FROM 'ready'
           OR NULLIF(claim_row.pvc_uid, '') IS NULL
           OR claim_row.protection_protocol IS DISTINCT FROM 'finalizer_v1'
           OR EXISTS (SELECT 1 FROM public.agents
                       WHERE thread_id = NEW.thread_id
                          OR hostname = intent_row.pod_name
                          OR pod_uid = intent_row.pod_uid) THEN
            RETURN NEW;
        END IF;
        NEW.retired_agent_pod := jsonb_build_object(
            'version', 1,
            'pod_name', intent_row.pod_name,
            'pod_uid', intent_row.pod_uid,
            'namespace', intent_row.namespace,
            'provisioner', intent_row.provisioner,
            'provision_attempt', intent_row.attempt_id,
            'protection_protocol', intent_row.protection_protocol,
            'workspace_claim_id', claim_row.claim_id,
            'workspace_create_attempt', claim_row.create_attempt,
            'workspace_created_runtime_generation', claim_row.created_runtime_generation,
            'pvc_name', claim_row.pvc_name,
            'pvc_uid', claim_row.pvc_uid
        );
        RETURN NEW;
    END IF;
    SELECT * INTO owner_row FROM public.threads
     WHERE id = NEW.thread_id FOR SHARE;
    marker := owner_row.runtime_retirement_context->'agent_pod';
    captured_agent := owner_row.runtime_retirement_context->'agent';
    IF marker->>'protection_protocol' IS DISTINCT FROM 'finalizer_v1' THEN
        RETURN NEW;
    END IF;

    IF owner_row.runtime_retirement_context->'agent_workspace_claim' IS NULL
       OR owner_row.runtime_retirement_context->'agent_workspace_claim'
          IN ('null'::jsonb, '{}'::jsonb) THEN
        SELECT * INTO actor_row FROM public.agents
         WHERE id = NEW.agent_id FOR SHARE;
        SELECT * INTO intent_row FROM public.thread_agent_pod_provision_intents
         WHERE attempt_id::text = marker->>'provision_attempt'
           AND thread_id = NEW.thread_id FOR SHARE;
        IF actor_row.id IS NULL OR intent_row.attempt_id IS NULL
           OR marker ? 'warm_binding_protection'
           OR intent_row.workspace_claim_id IS NOT NULL
           OR owner_row.agent_id IS DISTINCT FROM NEW.agent_id
           OR owner_row.runtime_attach_token IS DISTINCT FROM NEW.runtime_attach_token
           OR actor_row.thread_id IS DISTINCT FROM NEW.thread_id
           OR actor_row.hostname IS DISTINCT FROM marker->>'pod_name'
           OR actor_row.pod_uid IS DISTINCT FROM marker->>'pod_uid'
           OR actor_row.hostname IS DISTINCT FROM captured_agent->>'hostname'
           OR actor_row.pod_uid IS DISTINCT FROM captured_agent->>'pod_uid'
           OR intent_row.runtime_generation IS DISTINCT FROM NEW.runtime_generation
           OR intent_row.status IS DISTINCT FROM 'published'
           OR intent_row.pod_name IS DISTINCT FROM actor_row.hostname
           OR intent_row.pod_uid IS DISTINCT FROM actor_row.pod_uid
           OR intent_row.namespace IS DISTINCT FROM marker->>'namespace'
           OR intent_row.protection_protocol IS DISTINCT FROM 'finalizer_v1' THEN
            RETURN NEW;
        END IF;
        NEW.retired_agent_pod := jsonb_build_object(
            'version', 1,
            'pod_name', intent_row.pod_name,
            'pod_uid', intent_row.pod_uid,
            'namespace', intent_row.namespace,
            'provisioner', intent_row.provisioner,
            'provision_attempt', intent_row.attempt_id,
            'protection_protocol', intent_row.protection_protocol
        );
        RETURN NEW;
    END IF;

    SELECT * INTO actor_row FROM public.agents
     WHERE id = NEW.agent_id FOR SHARE;
    SELECT * INTO intent_row FROM public.thread_agent_pod_provision_intents
     WHERE attempt_id::text = marker->>'provision_attempt'
       AND thread_id = NEW.thread_id FOR SHARE;
    SELECT * INTO claim_row FROM public.thread_agent_workspace_claims
     WHERE claim_id = intent_row.workspace_claim_id FOR SHARE;
    IF actor_row.id IS NULL OR intent_row.attempt_id IS NULL
       OR claim_row.claim_id IS NULL
       OR owner_row.agent_id IS DISTINCT FROM NEW.agent_id
       OR owner_row.runtime_attach_token IS DISTINCT FROM NEW.runtime_attach_token
       OR actor_row.thread_id IS DISTINCT FROM NEW.thread_id
       OR actor_row.hostname IS DISTINCT FROM marker->>'pod_name'
       OR actor_row.pod_uid IS DISTINCT FROM marker->>'pod_uid'
       OR actor_row.hostname IS DISTINCT FROM captured_agent->>'hostname'
       OR actor_row.pod_uid IS DISTINCT FROM captured_agent->>'pod_uid'
       OR intent_row.runtime_generation IS DISTINCT FROM NEW.runtime_generation
       OR intent_row.status IS DISTINCT FROM 'published'
       OR intent_row.pod_name IS DISTINCT FROM actor_row.hostname
       OR intent_row.pod_uid IS DISTINCT FROM actor_row.pod_uid
       OR intent_row.namespace IS DISTINCT FROM marker->>'namespace'
       OR intent_row.protection_protocol IS DISTINCT FROM 'finalizer_v1'
       OR claim_row.thread_id IS DISTINCT FROM NEW.thread_id
       OR claim_row.claim_id::text IS DISTINCT FROM
          owner_row.runtime_retirement_context->'agent_workspace_claim'->>'claim_id'
       OR claim_row.namespace IS DISTINCT FROM intent_row.namespace
       OR claim_row.provisioner IS DISTINCT FROM intent_row.provisioner
       OR claim_row.status IS DISTINCT FROM 'ready'
       OR NULLIF(claim_row.pvc_uid, '') IS NULL
       OR claim_row.protection_protocol IS DISTINCT FROM 'finalizer_v1' THEN
        RAISE EXCEPTION 'soft settlement lacks exact actor/Pod/claim identity'
            USING ERRCODE = '23514',
                  CONSTRAINT = 'retired_agent_pod_identity_authority';
    END IF;
    NEW.retired_agent_pod := jsonb_build_object(
        'version', 1,
        'pod_name', intent_row.pod_name,
        'pod_uid', intent_row.pod_uid,
        'namespace', intent_row.namespace,
        'provisioner', intent_row.provisioner,
        'provision_attempt', intent_row.attempt_id,
        'protection_protocol', intent_row.protection_protocol,
        'workspace_claim_id', claim_row.claim_id,
        'workspace_create_attempt', claim_row.create_attempt,
        'workspace_created_runtime_generation', claim_row.created_runtime_generation,
        'pvc_name', claim_row.pvc_name,
        'pvc_uid', claim_row.pvc_uid
    );
    RETURN NEW;
END;
$$;

COMMIT;
