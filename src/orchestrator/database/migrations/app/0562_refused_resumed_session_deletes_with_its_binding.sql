-- migration:     0562_refused_resumed_session_deletes_with_its_binding.sql
-- description:   A Resumed stateless Session whose new workspace Pod the
--                cluster refused can be deleted: once End reclaimed the PVC
--                its earlier runtime left behind, the binding that names that
--                PVC no longer holds the row.
-- depends-on:    0561_refused_session_creation_ends_without_pod.sql
-- expected:      < 1s. Replaces one classifier; no row scan.
-- locks:         Function-catalog lock only.
-- transactional: yes

BEGIN;
SET LOCAL lock_timeout                        = '2s';
SET LOCAL statement_timeout                   = '15min';
SET LOCAL idle_in_transaction_session_timeout = '5min';
SET LOCAL timezone                            = 'UTC';

-- 0561 read a refused creation as metadata only with no workspace binding:
-- a first-generation Session never had one. A Resumed Session keeps the
-- binding to the PVC its earlier runtime used, and its refused create
-- recorded that same PVC. End names it in `unpublished_creation.retained`.
--
-- Read that binding as metadata only once the refused creation settled with
-- no Pod and reclaimed exactly the PVC the binding names (terminal_reclaim),
-- which happens only under a permanent End: then nothing is left behind it,
-- and the Session can't be resumed. A soft End keeps the PVC (preserve), so
-- until then both guards read the binding as before.
CREATE OR REPLACE FUNCTION public.stateless_unpublished_creation_settled_without_pod(
    requested_thread UUID,
    requested_state JSONB,
    require_reclaimed BOOLEAN
)
RETURNS BOOLEAN
LANGUAGE SQL
STABLE
AS $function$
    SELECT COALESCE((
        SELECT jsonb_typeof(requested_state -> 'workspace_container') = 'object'
           AND requested_state #>> '{workspace_container,provisioner}' = 'k8s'
           AND requested_state #>> '{workspace_container,_runtime_incarnation}'
               IS NULL
           AND (
               COALESCE(requested_state -> '_workspace_binding', '{}'::JSONB)
                   = '{}'::JSONB
               OR (
                   r.cancel_resource_policy = 'terminal_reclaim'
                   AND m.marker #>> '{retained,pvc_uid}' = r.pvc_uid::TEXT
                   AND requested_state #>> '{_workspace_binding,backing_id}'
                       = 'k8s-pvc:' || (m.marker ->> 'namespace') || ':'
                         || r.pvc_uid::TEXT
               )
           )
           AND r.id::TEXT = m.marker ->> 'reservation_id'
           AND r.operation_kind = 'create'
           AND r.settled_at IS NOT NULL
           AND r.phase = 'aborted'
           AND r.result_kind = 'aborted'
           AND r.pod_uid IS NULL
           AND r.runtime_incarnation IS NULL
           AND r.cancel_target_disposition = 'deleted'
           AND r.cancel_cleanup_completed_at IS NOT NULL
           AND r.external_effects #>> '{pod,observed_uid}' IS NULL
           AND (
               NOT require_reclaimed
               OR r.cancel_resource_policy = 'terminal_reclaim'
           )
          FROM public.managed_repository_workspace_creation_reservations AS r
         CROSS JOIN LATERAL (
             SELECT COALESCE(
                 requested_state #> ARRAY[
                     '_stateless_claim_retirement', 'unpublished_creation'
                 ],
                 requested_state #> ARRAY[
                     '_stateless_workspace_retirement_settled',
                     'unpublished_creation'
                 ]
             ) AS marker
         ) AS m
         WHERE r.owner_kind = 'thread'
           AND r.owner_id = requested_thread
           AND r.scope = 'workspace_container'
         ORDER BY r.reservation_generation DESC
         LIMIT 1
    ), FALSE);
$function$;

COMMIT;
