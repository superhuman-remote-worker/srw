-- migration:     0558_stateless_lite_upgrade_replaces_end_stamp.sql
-- description:   A resumed lite Session can take the container upgrade (C1):
--                the guards read End's volume_reclaimed stamp as the empty
--                record it describes, for that one transition only.
-- depends-on:    0557_vm_end_accepts_disk_purged_by_upgrade_abort.sql
-- expected:      < 1s. Adds one immutable classifier and patches two trigger
--                functions from their catalog definitions; no row scan.
-- locks:         Function-catalog lock only; installed row triggers remain.
-- transactional: yes

BEGIN;
SET LOCAL lock_timeout                        = '2s';
SET LOCAL statement_timeout                   = '15min';
SET LOCAL idle_in_transaction_session_timeout = '5min';
SET LOCAL timezone                            = 'UTC';

-- End's settlement stamps every lite row it settles with
-- {"volume_reclaimed": false}, and Resume keeps it. A lite workspace has no
-- process, Pod, lease or volume behind that record, but both guards read any
-- non-empty record without a provisioner as authority to retire, so the
-- upgrade of a resumed Session was refused. Recognize only the upgrade's own
-- UPDATE (database/stateless_workspace_upgrade.py): a live stateless row, its
-- runtime generation and retirement token unchanged, a virtual/none
-- declaration becoming sandbox, the old record exactly End's stamp, the new
-- one exactly the upgrade's pending create. Every other check of both guards
-- still runs, as for a lite row that was never Ended.
CREATE OR REPLACE FUNCTION public.stateless_lite_upgrade_replaces_end_stamp(
    old_row JSONB,
    new_row JSONB
)
RETURNS BOOLEAN
LANGUAGE SQL
IMMUTABLE
AS $function$
    SELECT COALESCE(
        old_row->>'execution_lane' = 'stateless'
        AND new_row->>'execution_lane' = 'stateless'
        AND old_row->>'status' IN ('created', 'active', 'awaiting_user')
        AND new_row->>'status' = old_row->>'status'
        AND new_row->>'runtime_generation' = old_row->>'runtime_generation'
        AND old_row->'runtime_retirement_token' = 'null'::JSONB
        AND new_row->'runtime_retirement_token' = 'null'::JSONB
        AND old_row #> '{metadata,workspace_container}'
            = '{"volume_reclaimed": false}'::JSONB
        AND old_row #>> '{metadata,config_override,workspace,backend}'
            IN ('virtual', 'none')
        AND new_row #>> '{metadata,config_override,workspace,backend}'
            = 'sandbox'
        AND jsonb_typeof(new_row #> '{metadata,workspace_container}') = 'object'
        AND (new_row #> '{metadata,workspace_container}') - ARRAY[
                'status',
                'provisioner',
                '_runtime_creation',
                'upgrade_config',
                'upgrade_sources',
                'seed_from'
            ] = '{}'::JSONB
        AND new_row #> '{metadata,workspace_container,status}'
            = '"pending"'::JSONB
        AND new_row #> '{metadata,workspace_container,provisioner}'
            = '"k8s"'::JSONB
        AND new_row #> '{metadata,workspace_container,_runtime_creation}'
            = jsonb_build_object(
                'generation', new_row->'runtime_generation',
                'mode', 'create',
                'attempted', FALSE,
                'replaces_uid', NULL
            )
        AND jsonb_typeof(new_row #> '{metadata,workspace_container,seed_from}')
            = 'object'
        AND COALESCE(old_row #> '{metadata,vm}', 'null'::JSONB)
            IN ('null'::JSONB, '{}'::JSONB)
        AND COALESCE(new_row #> '{metadata,vm}', 'null'::JSONB)
            IN ('null'::JSONB, '{}'::JSONB)
        AND NOT (old_row->'metadata') ?| ARRAY[
            '_stateless_workspace_retirement_pending',
            '_stateless_workspace_retirement_settled',
            '_stateless_claim_retirement',
            '_stateless_claim_losses',
            '_stateless_claim_loss_hold'
        ]
        AND NOT (new_row->'metadata') ?| ARRAY[
            '_stateless_workspace_retirement_pending',
            '_stateless_workspace_retirement_settled',
            '_stateless_claim_retirement',
            '_stateless_claim_losses',
            '_stateless_claim_loss_hold'
        ],
        FALSE
    );
$function$;

DO $migration$
DECLARE
    definition TEXT;
    old_fragment TEXT := $old$    IF source_kind = 'thread'
       AND OLD.status::TEXT = 'ended'
       AND public.stateless_none_workspace_outcome_is_authority_free(old_state)
    THEN
        old_workspace := '{}'::JSONB;
    END IF;$old$;
    new_fragment TEXT := $new$    IF source_kind = 'thread'
       AND OLD.status::TEXT = 'ended'
       AND public.stateless_none_workspace_outcome_is_authority_free(old_state)
    THEN
        old_workspace := '{}'::JSONB;
    END IF;
    IF source_kind = 'thread' AND TG_OP = 'UPDATE'
       AND public.stateless_lite_upgrade_replaces_end_stamp(
           to_jsonb(OLD), to_jsonb(NEW)
       )
    THEN
        old_workspace := '{}'::JSONB;
    END IF;$new$;
BEGIN
    definition := pg_get_functiondef(
        'public.enforce_managed_repository_process_zero_transition()'::regprocedure
    );
    IF strpos(definition, new_fragment) > 0 THEN
        RETURN;
    END IF;
    IF strpos(definition, old_fragment) = 0 THEN
        RAISE EXCEPTION
            'Unexpected enforce_managed_repository_process_zero_transition definition';
    END IF;
    EXECUTE replace(definition, old_fragment, new_fragment);
END;
$migration$;

DO $migration$
DECLARE
    definition TEXT;
    old_fragment TEXT := $old$
    FOREACH scope_name IN ARRAY ARRAY['workspace_container', 'ide'] LOOP$old$;
    new_fragment TEXT := $new$
    -- A resumed lite Session's upgrade replaces End's stamp (0558).
    IF source_kind = 'thread' AND TG_OP = 'UPDATE'
       AND public.stateless_lite_upgrade_replaces_end_stamp(
           to_jsonb(OLD), to_jsonb(NEW)
       )
    THEN
        old_state := jsonb_set(old_state, '{workspace_container}', '{}'::JSONB);
    END IF;

    FOREACH scope_name IN ARRAY ARRAY['workspace_container', 'ide'] LOOP$new$;
BEGIN
    definition := pg_get_functiondef(
        'public.prevent_retired_workspace_runtime_rebinding()'::regprocedure
    );
    IF strpos(definition, new_fragment) > 0 THEN
        RETURN;
    END IF;
    IF (length(definition) - length(replace(definition, old_fragment, '')))
       / length(old_fragment) <> 1 THEN
        RAISE EXCEPTION
            'Unexpected prevent_retired_workspace_runtime_rebinding definition';
    END IF;
    EXECUTE replace(definition, old_fragment, new_fragment);
END;
$migration$;

COMMIT;
