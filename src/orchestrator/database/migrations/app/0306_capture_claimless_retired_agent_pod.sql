-- Soft settlement also captures the exact Pod of a claim-less dedicated life.
-- (Numbered 0286: origin/develop already carries 0284 and 0285.)
--
-- 0224 recorded the retired Pod only for lives that mounted an agent workspace
-- claim. A dedicated Pod without one (workspace.pvcEnabled false) still
-- carries SRW's protection finalizer, so after its process exits nothing could
-- ever release it: the settled outcome named no Pod, and generation alone is
-- not a Pod relation. Record the same server-validated identity, without the
-- claim fields, when every relation is complete. An incomplete claim-less
-- shape (warm pool Pod, legacy marker, changed actor) stays NULL: it grants no
-- new authority and never blocks settlement. The claim-bearing branch is
-- unchanged, including its refusal.
CREATE OR REPLACE FUNCTION public.capture_retired_pinned_agent_pod()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    owner_row public.threads%ROWTYPE;
    actor_row public.agents%ROWTYPE;
    intent_row public.thread_agent_pod_provision_intents%ROWTYPE;
    claim_row public.thread_agent_workspace_claims%ROWTYPE;
    marker jsonb;
    captured_agent jsonb;
BEGIN
    IF NEW.retired_agent_pod IS NOT NULL THEN
        RAISE EXCEPTION 'retired Pod identity is server-owned'
            USING ERRCODE = '23514',
                  CONSTRAINT = 'retired_agent_pod_identity_authority';
    END IF;
    -- The existing insert-authority trigger first validates the exact live
    -- retirement and local-quiescence receipt.
    IF NEW.permanent OR NEW.agent_id IS NULL THEN
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
