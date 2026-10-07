-- migration: 0335_pinned_abrupt_exit_subagents_and_earlier_lives.sql
-- description: The abrupt pinned actor-exit receipt accepts in-process session
--              subagent children and an earlier settled life's leftover input.
-- depends-on: 0317_pinned_virtual_without_backing_abrupt_exit.sql
-- transactional: yes
-- expected: < 5s. Function definitions only; no row reads or writes.
--
-- After a proven SIGKILL, acknowledge_abrupt_pinned_actor_exit (0313, 0317)
-- refused on two durable facts that no longer name a producer:
--
-- 1. Any threads row with parent_thread_id = the session. Session subagents
--    run in-process in the parent's agent Pod and write only under the
--    parent's exact runtime authority, so once every container of that Pod
--    is proven stopped they are stopped too. A pinned virtual or lite
--    session that had ever delegated could therefore never retire after a
--    SIGKILL. The refusal now applies only to a child with a producer of its
--    own (pinned_session_child_may_produce). The receipt leaves a queued or
--    running child row as the crash left it; the retirement settle then ends
--    it cancelled:parent_retired, as it already does for End.
-- 2. A non-terminal pinned delivery owned by another agent/Pod. An abrupt
--    exit leaves its admitted partial turn admitted forever and its stranded
--    input owned by the dead actor until a successor claims it, so a session
--    killed, resumed and killed again could never retire. A leftover of an
--    earlier receipted life is now accepted; see the comment at the check.
--
-- The receipt's shape and every other refusal are unchanged. The settled
-- actor-exit receipt (0227) keeps its stricter settled-work contract; the
-- retirement recovery tries this receipt after it.
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

-- True when a session child could still act without its parent's Pod: a
-- runtime bound or ever exposed to it, its own unfinished input, control,
-- permission or interrupt, a queue unit or completion effect, a workspace,
-- VM or mount of its own, or a child of its own. Terminal rows are history.
CREATE OR REPLACE FUNCTION public.pinned_session_child_may_produce(child_id uuid)
RETURNS boolean LANGUAGE sql STABLE AS $$
    SELECT EXISTS (
               SELECT 1 FROM public.threads child
                WHERE child.id = child_id
                  AND (child.agent_id IS NOT NULL
                       OR child.control_admission_agent_id IS NOT NULL
                       OR child.runtime_attach_token IS NOT NULL
                       OR child.runtime_authority_exposed
                       OR child.runtime_retirement_token IS NOT NULL
                       OR COALESCE(child.metadata->'agent_pod', 'null'::jsonb)
                          NOT IN ('null'::jsonb, '{}'::jsonb)
                       OR COALESCE(child.metadata->'vm', 'null'::jsonb)
                          NOT IN ('null'::jsonb, '{}'::jsonb)
                       OR COALESCE(child.metadata->'_workspace_binding', 'null'::jsonb)
                          NOT IN ('null'::jsonb, '{}'::jsonb)))
        OR EXISTS (SELECT 1 FROM public.agents WHERE thread_id = child_id)
        OR EXISTS (SELECT 1 FROM public.threads WHERE parent_thread_id = child_id)
        OR EXISTS (SELECT 1 FROM public.thread_input_deliveries
                    WHERE thread_id = child_id AND state NOT IN ('settled','cancelled'))
        OR EXISTS (SELECT 1 FROM public.thread_control_requests
                    WHERE thread_id = child_id AND outcome IS NULL)
        OR EXISTS (SELECT 1 FROM public.thread_permission_requests
                    WHERE thread_id = child_id AND status = 'pending')
        OR EXISTS (SELECT 1 FROM public.thread_interrupt_requests
                    WHERE thread_id = child_id AND
                      (outcome IS NULL OR (outcome = 'applied' AND
                       NOT (COALESCE(result, '{}'::jsonb) ? 'consumed_input_seq'))))
        OR EXISTS (SELECT 1 FROM public.run_queue
                    WHERE unit_id = child_id AND (state <> 'done' OR leased_by IS NOT NULL))
        OR EXISTS (SELECT 1 FROM public.completion_effects
                    WHERE (scope_id = child_id OR producer_id = child_id)
                      AND (state <> 'done' OR claimed_by IS NOT NULL))
        OR EXISTS (SELECT 1 FROM public.thread_agent_workspace_claims
                    WHERE thread_id = child_id AND status <> 'reclaimed')
        OR EXISTS (SELECT 1 FROM public.thread_workspace_provision_intents
                    WHERE thread_id = child_id)
        OR EXISTS (SELECT 1 FROM public.managed_repository_workspace_creation_reservations
                    WHERE owner_kind = 'thread' AND owner_id = child_id)
        OR EXISTS (SELECT 1 FROM public.cloud_ro_mounts
                    WHERE thread_id = child_id AND status IN ('engaging','active','revoking'))
$$;

CREATE OR REPLACE FUNCTION public.acknowledge_abrupt_pinned_actor_exit(
    owner_id uuid, generation_id uuid, retirement_id uuid,
    actor_id uuid, attach_id uuid, stopped_pod_uid text
) RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE
    owner_row public.threads%ROWTYPE;
    actor_row public.agents%ROWTYPE;
    context jsonb;
    marker jsonb;
    binding jsonb;
    backend text;
    dedicated_authorized boolean;
    warm_authorized boolean;
    receipt jsonb;
    input_count bigint;
    input_digest text;
    claim jsonb;
    stranded_count bigint;
    partial_count bigint;
BEGIN
    SELECT * INTO owner_row FROM public.threads
     WHERE id = owner_id FOR UPDATE;
    IF NOT FOUND OR owner_row.kind IS DISTINCT FROM 'session'
       OR owner_row.execution_lane IS DISTINCT FROM 'pinned'
       OR owner_row.runtime_generation IS DISTINCT FROM generation_id
       OR owner_row.runtime_retirement_token IS DISTINCT FROM retirement_id
       OR owner_row.runtime_retirement_authorized_at IS NULL
       OR owner_row.agent_id IS DISTINCT FROM actor_id
       OR owner_row.runtime_attach_token IS DISTINCT FROM attach_id
       OR (owner_row.control_admission_agent_id IS NOT NULL
           AND owner_row.control_admission_agent_id IS DISTINCT FROM actor_id)
       OR owner_row.runtime_authority_exposed IS DISTINCT FROM true
       OR actor_id IS NULL OR attach_id IS NULL
       OR NULLIF(stopped_pod_uid, '') IS NULL THEN
        RETURN NULL;
    END IF;
    context := owner_row.runtime_retirement_context;
    marker := context->'agent_pod';
    binding := context->'workspace_binding';
    backend := context->>'workspace_backend';
    claim := context->'agent_workspace_claim';
    IF backend IS DISTINCT FROM 'virtual' AND backend IS DISTINCT FROM 'none' THEN
        RETURN NULL;
    END IF;
    -- Virtual without backing and none both own only their exact agent Pod.
    -- Absence must agree in captured and current authority; a partial or
    -- physical binding is never inferred away from a terminal agent Pod.
    IF COALESCE(binding, 'null'::jsonb) IN ('null'::jsonb, '{}'::jsonb) THEN
        IF COALESCE(owner_row.metadata->'_workspace_binding', 'null'::jsonb)
           NOT IN ('null'::jsonb, '{}'::jsonb) THEN
            RETURN NULL;
        END IF;
    ELSIF backend IS DISTINCT FROM 'virtual'
       OR jsonb_typeof(binding) IS DISTINCT FROM 'object'
       OR binding->>'kind' IS DISTINCT FROM 'virtual'
       OR COALESCE(binding->>'backing_id', '') !~ '^rclone:[0-9a-f]{64}$'
       OR COALESCE(binding->>'generation', '') !~
          '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
       OR binding->'ssh_host_key_fingerprint' IS DISTINCT FROM 'null'::jsonb
       OR binding - ARRAY['generation','kind','backing_id','ssh_host_key_fingerprint']
          IS DISTINCT FROM '{}'::jsonb
       OR owner_row.metadata->'_workspace_binding' IS DISTINCT FROM binding THEN
        RETURN NULL;
    END IF;
    IF context->>'generation' IS DISTINCT FROM generation_id::text
       OR context->>'agent_id' IS DISTINCT FROM actor_id::text
       OR context->>'runtime_attach_token' IS DISTINCT FROM attach_id::text
       OR context->>'settle_status' IS DISTINCT FROM 'ended'
       OR owner_row.metadata->'agent_pod' IS DISTINCT FROM marker
       OR COALESCE(owner_row.metadata->'protected_cloud', 'false'::jsonb)
          IS DISTINCT FROM 'false'::jsonb
       OR EXISTS (
           SELECT 1 FROM unnest(ARRAY[
               'workspace_container','vm','workspace_provision_intent',
               'agent_pod_provision_intent','protected_ro'
           ]) AS field
           WHERE COALESCE(context->field, 'null'::jsonb)
                 NOT IN ('null'::jsonb, '{}'::jsonb)
       )
       OR marker->>'pod_uid' IS DISTINCT FROM stopped_pod_uid
       OR marker->>'protection_protocol' IS DISTINCT FROM 'finalizer_v1'
       OR NULLIF(marker->>'namespace', '') IS NULL
       OR NULLIF(marker->>'pod_name', '') IS NULL
       OR context->'agent'->>'hostname' IS DISTINCT FROM marker->>'pod_name'
       OR context->'agent'->>'pod_uid' IS DISTINCT FROM stopped_pod_uid THEN
        RETURN NULL;
    END IF;
    -- The same two Pod authorities enforce_pinned_warm_binding_publication
    -- accepts at bind time. A dedicated Pod carries its provision attempt; a
    -- warm-pool Pod carries the protection that was bound to this exact
    -- thread, generation, attach token and actor. Exactly one must vouch.
    dedicated_authorized := EXISTS (
        SELECT 1 FROM public.thread_agent_pod_provision_intents intent
         WHERE intent.thread_id = owner_id
           AND intent.runtime_generation = generation_id
           AND intent.attempt_id::text = marker->>'provision_attempt'
           AND intent.pod_name = marker->>'pod_name'
           AND intent.pod_uid = stopped_pod_uid
           AND intent.namespace = marker->>'namespace'
           AND intent.protection_protocol = 'finalizer_v1'
           AND intent.status = 'published'
           AND intent.workspace_claim_id::text IS NOT DISTINCT FROM claim->>'claim_id' 
    );
    warm_authorized := EXISTS (
        SELECT 1 FROM public.thread_agent_warm_binding_protections warm
         WHERE warm.protection_id::text = marker->>'warm_binding_protection'
           AND warm.thread_id = owner_id
           AND warm.runtime_generation = generation_id
           AND warm.runtime_attach_token = attach_id
           AND warm.agent_id = actor_id
           AND warm.status = 'bound'
           AND warm.namespace = marker->>'namespace'
           AND warm.pod_name = marker->>'pod_name'
           AND warm.pod_uid = stopped_pod_uid
    );
    IF NOT (dedicated_authorized OR warm_authorized) THEN
        RETURN NULL;
    END IF;
    SELECT * INTO actor_row FROM public.agents
     WHERE id = actor_id FOR SHARE;
    IF NOT FOUND OR actor_row.thread_id IS DISTINCT FROM owner_id
       OR actor_row.pod_uid IS DISTINCT FROM stopped_pod_uid
       OR actor_row.hostname IS DISTINCT FROM marker->>'pod_name'
       OR actor_row.current_job_id IS NOT NULL
       OR EXISTS (SELECT 1 FROM public.agents other
                   WHERE other.id <> actor_id
                     AND (other.thread_id = owner_id OR other.pod_uid = stopped_pod_uid))
    THEN
        RETURN NULL;
    END IF;

    -- Storage debt is not another process. Accept only the same protected
    -- PVC publication captured at Begin; End still owes exact reclaim/fencing.
    IF COALESCE(claim,'null'::jsonb) <> 'null'::jsonb AND NOT EXISTS (
        SELECT 1 FROM public.thread_agent_workspace_claims c
         WHERE c.thread_id=owner_id AND c.claim_id::text=claim->>'claim_id'
           AND c.created_runtime_generation::text=claim->>'created_runtime_generation'
           AND c.create_attempt::text=claim->>'create_attempt'
           AND c.provisioner=claim->>'provisioner'
           AND c.pvc_name=claim->>'pvc_name' AND c.pvc_uid=claim->>'pvc_uid'
           AND c.namespace=claim->>'namespace'
           AND c.protection_protocol='finalizer_v1' AND c.status='ready'
           AND claim->>'protection_protocol'='finalizer_v1'
           AND claim->>'status'='ready'
    ) THEN RETURN NULL; END IF;

    -- All supported input/child/control admission locks the owner first and
    -- rejects its retirement token. These reads run after that durable fence,
    -- not before a still-open producer can commit an admission.
    -- A non-terminal input claimed by another agent or Pod refuses, except a
    -- leftover of an earlier life of this thread: its owner is another agent
    -- row that was the actor of an earlier generation whose retirement
    -- settled (every outcome row requires that life's own exit receipt) after
    -- the claim. A claim made after that settlement belongs to a later,
    -- unreceipted life and still refuses, as does the same agent on another
    -- Pod and an actor this life replaced without a retirement.
    IF EXISTS (SELECT 1 FROM public.thread_input_deliveries delivery
                WHERE delivery.thread_id = owner_id
                  AND delivery.state NOT IN ('settled','cancelled')
                  AND (delivery.execution_lane IS DISTINCT FROM 'pinned'
                       OR (delivery.owner_agent_id IS NOT NULL
                           AND (delivery.owner_agent_id IS DISTINCT FROM actor_id
                                OR delivery.owner_pod_uid IS DISTINCT FROM stopped_pod_uid)
                           AND NOT (
                               delivery.owner_agent_id IS DISTINCT FROM actor_id
                               AND EXISTS (
                                   SELECT 1
                                     FROM public.thread_runtime_retirement_outcomes prior
                                    WHERE prior.thread_id = owner_id
                                      AND prior.agent_id = delivery.owner_agent_id
                                      AND prior.runtime_generation
                                          IS DISTINCT FROM generation_id
                                      AND prior.settled_at >= delivery.owned_at)))))
       -- In-process session children died with the proven-stopped Pod; their
       -- queued/running rows stay as the crash left them for the settle.
       OR EXISTS (SELECT 1 FROM public.threads child
                   WHERE child.parent_thread_id = owner_id
                     AND public.pinned_session_child_may_produce(child.id))
       OR EXISTS (SELECT 1 FROM public.thread_control_requests
                   WHERE thread_id = owner_id AND runtime_generation = generation_id)
       OR EXISTS (SELECT 1 FROM public.thread_permission_requests
                   WHERE thread_id = owner_id AND status = 'pending')
       OR EXISTS (SELECT 1 FROM public.thread_interrupt_requests
                   WHERE thread_id = owner_id AND
                     (outcome IS NULL OR (outcome = 'applied' AND
                      NOT (COALESCE(result, '{}'::jsonb) ? 'consumed_input_seq'))))
       OR EXISTS (SELECT 1 FROM public.run_queue
                   WHERE unit_id = owner_id AND (state <> 'done' OR leased_by IS NOT NULL))
       OR EXISTS (SELECT 1 FROM public.completion_effects
                   WHERE (scope_id = owner_id OR producer_id = owner_id)
                     AND (state <> 'done' OR claimed_by IS NOT NULL))
       OR EXISTS (SELECT 1 FROM public.thread_agent_workspace_claims
                   WHERE thread_id = owner_id AND status <> 'reclaimed'
                     AND claim_id::text IS DISTINCT FROM claim->>'claim_id')
       OR EXISTS (SELECT 1 FROM public.thread_workspace_provision_intents
                   WHERE thread_id = owner_id)
       OR EXISTS (SELECT 1 FROM public.managed_repository_workspace_creation_reservations
                   WHERE owner_kind = 'thread' AND
                         managed_repository_workspace_creation_reservations.owner_id =
                         acknowledge_abrupt_pinned_actor_exit.owner_id)
       OR EXISTS (SELECT 1 FROM public.cloud_ro_mounts
                   WHERE thread_id = owner_id AND status IN ('engaging','active','revoking'))
    THEN
        RETURN NULL;
    END IF;

    -- No delivery mutation: queued/owned input is reclaimed once by a successor;
    -- admitted partial turns remain immutable receipts and are never replayed.
    -- owner_runtime_generation is a process UUID, not the thread generation.
    -- The current published Pod/actor join above owns these completed inputs
    -- across container process restarts inside that same Kubernetes UID.
    SELECT count(*), md5(string_agg(delivery_id::text || ':' || state, ',' ORDER BY delivery_id)),
           count(*) FILTER (WHERE state IN ('owned','queued')),
           count(*) FILTER (WHERE state='admitted')
      INTO input_count, input_digest, stranded_count, partial_count
      FROM public.thread_input_deliveries
     WHERE thread_id = owner_id AND owner_agent_id = actor_id
       AND owner_pod_uid = stopped_pod_uid
       AND execution_lane = 'pinned';
    IF input_count = 0 THEN
        RETURN NULL;
    END IF;
    receipt := jsonb_build_object(
        'version', 1,
        'runtime_generation', generation_id,
        'retirement_token', retirement_id,
        'agent_id', actor_id,
        'runtime_attach_token', attach_id,
        'settle_status', 'ended',
        'quiescence_protocol', 'agent_runtime_zero_v1',
        'quiescence_actor', 'orchestrator',
        'workspace_generation', NULL,
        'workspace_runtime_incarnation', NULL,
        'recovery_protocol', CASE WHEN backend = 'virtual'
                                  THEN 'abrupt_virtual_actor_exit_v1'
                                  ELSE 'abrupt_lite_actor_exit_v1' END,
        'agent_pod_uid', stopped_pod_uid,
        'captured_input_count', input_count,
        'captured_input_ids_digest', input_digest,
        'stranded_input_count', stranded_count,
        'partial_admission_count', partial_count
    );
    IF owner_row.runtime_retirement_local_quiescence IS NOT NULL THEN
        RETURN CASE WHEN owner_row.runtime_retirement_local_quiescence = receipt
                    THEN receipt ELSE NULL END;
    END IF;
    UPDATE public.threads SET runtime_retirement_local_quiescence = receipt
     WHERE id = owner_id;
    RETURN receipt;
END;
$$;

COMMIT;
