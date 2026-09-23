-- migration: 0271_vm_network_profile.sql
-- description: Closed immutable retained-instance network profile and disposition fence.
-- depends-on: 0270_vm_idle_lifecycle.sql
-- transactional: yes
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

CREATE FUNCTION public.guard_retained_vm_network_profile() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE profile jsonb;
BEGIN
    profile := NEW.backend_state->'network_profile';
    IF profile IS NOT NULL AND profile IS DISTINCT FROM jsonb_build_object(
        'version', 1,
        'kind', 'nocloud-dhcp-by-interface-name',
        'interface', 'enp1s0',
        'network_data_sha256', 'sha256:dcad9787224fe834481f60286fbca4dd66eecc9fe05ec6db2acae0bbf22b2e6a'
    ) THEN
        RAISE EXCEPTION 'Unsupported retained VM network profile' USING ERRCODE='23514';
    END IF;
    IF TG_OP='UPDATE' AND profile IS DISTINCT FROM OLD.backend_state->'network_profile' THEN
        RAISE EXCEPTION 'Retained VM network profile is immutable' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER retained_vm_network_profile BEFORE INSERT OR UPDATE OF backend_state
ON public.srw_workspace_instances FOR EACH ROW
EXECUTE FUNCTION public.guard_retained_vm_network_profile();

CREATE OR REPLACE FUNCTION public.guard_vm_creation_disposition_instance() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE current_instance public.srw_workspace_instances%ROWTYPE;
        retry public.vm_creation_retries%ROWTYPE;
        completion jsonb;
        receipt jsonb;
BEGIN
    SELECT * INTO current_instance FROM public.srw_workspace_instances WHERE id=NEW.id;
    SELECT * INTO retry FROM public.vm_creation_retries r
        WHERE r.cancellation_disposition->>'workspace_instance_id'=NEW.id::text;
    IF ROW(current_instance.status,current_instance.execution_id,current_instance.generation,
           current_instance.pvc_name,current_instance.pvc_uid,current_instance.backend_state->'storage',
           current_instance.backend_state->'network_profile')
       IS DISTINCT FROM ROW(OLD.status,OLD.execution_id,OLD.generation,OLD.pvc_name,OLD.pvc_uid,
                            OLD.backend_state->'storage',OLD.backend_state->'network_profile')
       AND FOUND AND retry.state='cancel_requested' THEN
        RAISE EXCEPTION 'Creation disposition instance remains held' USING ERRCODE='23514';
    END IF;
    IF FOUND AND retry.state='settled' AND retry.reason='creation_disposed' THEN
        completion := retry.cancellation_completion;
        receipt := current_instance.backend_state->'retained_creation_disposition';
        IF NOT (
            public.valid_vm_creation_disposition_evidence(retry)
            AND current_instance.execution_id IS NULL
            AND current_instance.generation=(retry.canonical_request->'workspace_storage'->>'generation')::bigint
            AND current_instance.pvc_uid IS NOT DISTINCT FROM completion->'rootdisk'->>'pvc_uid'
            AND current_instance.status=CASE WHEN completion->'workspace_attachment'->>'outcome'='detached'
                THEN 'Detached' ELSE 'Released' END
            AND receipt->>'request_id'=retry.request_id::text
            AND receipt->>'disposition_id'=retry.cancellation_disposition->>'disposition_id'
            AND receipt->>'provision_generation'=retry.provision_generation::text
            AND receipt->>'execution_id'=retry.execution_id::text
            AND receipt->'attachment'=completion->'workspace_attachment'
            AND receipt->'rootdisk'=completion->'rootdisk'
        ) IS TRUE THEN
            RAISE EXCEPTION 'Creation disposition instance is incomplete' USING ERRCODE='23514';
        END IF;
    END IF;
    RETURN NULL;
END;
$$;
COMMIT;
