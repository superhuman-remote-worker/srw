-- migration:     0527_vm_job_lifecycle_terminal_packet.sql
-- description: Admit exact completed lifecycle Job VM purge as terminal physical stop.
-- depends-on: 0526_capture_preregistration_retired_agent_pod.sql
-- transactional: yes
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

-- Preserve the historical lifecycle request identity. All existing
-- physical stop, charge, effect and canonical-intent guards remain.
CREATE OR REPLACE FUNCTION public.vm_job_terminal_prior_packet_evidence(source_request uuid) RETURNS jsonb
    LANGUAGE plpgsql STABLE
    AS $$
DECLARE r public.vm_creation_retries%ROWTYPE;
        a public.vm_workspace_cleanup_admissions%ROWTYPE;
        repository_receipt public.vm_job_repository_settlement_receipts%ROWTYPE;
        stop_row public.vm_resource_cleanup_stop_receipts%ROWTYPE;
        reservation_count bigint;
BEGIN
    SELECT * INTO r FROM public.vm_creation_retries WHERE request_id=source_request;
    IF NOT FOUND OR r.owner_kind<>'job' OR r.job_id IS NULL
       OR r.resolved_at IS NULL THEN
        RETURN NULL;
    END IF;
    IF public.job_vm_creation_never_issued_terminal_source(
        r.job_id,r.provision_generation::text) THEN
        -- 0326 proves this retry and logical parent. Other generations may
        -- have their own exact physical packet; unattributed delivery history
        -- cannot be assigned to those generations and remains a hold.
        IF NOT public.vm_job_logical_final_history_safe(
            r.job_id,r.provision_generation) THEN
            RETURN NULL;
        END IF;
        SELECT * INTO a FROM public.vm_workspace_cleanup_admissions
        WHERE owner_kind='job' AND owner_id=r.job_id
          AND source='job_terminal_vm_release' AND pvc_uid IS NULL
          AND completed_at IS NOT NULL AND outcome='completed'
          AND request_id=public.uuid_generate_v5(public.uuid_ns_url(),
            'vm-workspace-cleanup:job_terminal_vm_release:job:' || r.job_id::text ||
            ':' || r.provision_generation::text || ':None:');
        IF NOT FOUND THEN RETURN NULL; END IF;
        SELECT * INTO repository_receipt
        FROM public.vm_job_repository_settlement_receipts
        WHERE cleanup_admission_id=a.id AND request_id=r.request_id
          AND job_id=r.job_id AND provision_generation=r.provision_generation;
        IF NOT FOUND THEN RETURN NULL; END IF;
        RETURN jsonb_build_object(
            'kind','never_issued','job_id',r.job_id,'request_id',r.request_id,
            'provision_generation',r.provision_generation,
            'execution_id',r.execution_id,'execution_revision',r.execution_revision,
            'execution_generation',r.execution_generation,
            'execution_chain',public.vm_job_execution_chain_evidence(r.execution_id),
            'cleanup_admission_id',a.id,'cleanup_intent_digest',a.intent_digest,
            'repository_pair',public.vm_job_repository_receipt_pair(repository_receipt),
            'retry_reason',r.reason,'resolved_at',r.resolved_at);
    END IF;
    IF r.state<>'succeeded' OR r.observed_vm_uid IS NULL
       OR r.observed_pvc_uid IS NULL OR r.creation_admission_id IS NULL
       OR NOT EXISTS (SELECT 1 FROM public.managed_repository_process_zero_receipts p
           WHERE p.owner_kind='job' AND p.owner_id=r.job_id AND p.scope='vm'
             AND p.provisioner='vm'
             AND p.runtime_incarnation=r.provision_generation::text)
       OR EXISTS (SELECT 1 FROM public.vm_creation_effects e
           WHERE e.request_id=r.request_id AND e.state='issued')
       OR EXISTS (SELECT 1 FROM public.vm_resource_waiters w
           WHERE w.request_id=r.request_id AND w.state<>'released')
       OR EXISTS (SELECT 1 FROM public.vm_resource_reservations v
           WHERE v.request_id=r.request_id AND v.state<>'released') THEN
        RETURN NULL;
    END IF;
    -- A completed terminal parent is the typed physical/storage disposition;
    -- identity includes the generation, VM, PVC and purge intent.
    SELECT * INTO a FROM public.vm_workspace_cleanup_admissions
    WHERE owner_kind='job' AND owner_id=r.job_id
      AND source IN ('job_terminal_vm_release','public_vm_delete','lifecycle_vm_delete')
      AND parent_admission_id IS NULL AND pvc_uid=r.observed_pvc_uid
      AND request_id=public.uuid_generate_v5(public.uuid_ns_url(),
          (CASE WHEN source='lifecycle_vm_delete'
              THEN 'workspace-cleanup:delete:job:'
              ELSE 'vm-workspace-cleanup:' || source || ':job:' END) ||
          r.job_id::text || ':' || r.provision_generation::text || ':' ||
          r.observed_vm_uid::text || ':' || r.observed_pvc_uid::text)
      AND completed_at IS NOT NULL AND outcome='completed'
    ORDER BY completed_at DESC LIMIT 1;
    IF NOT FOUND THEN RETURN NULL; END IF;
    IF a.intent_digest IS DISTINCT FROM 'sha256:' || pg_catalog.encode(
        pg_catalog.sha256(pg_catalog.convert_to(
            '{"owner_id":"' || r.job_id::text ||
            '","owner_kind":"job","provision_generation":"' ||
            r.provision_generation::text ||
            '","purge_disk":true,"pvc_uid":"' || r.observed_pvc_uid::text ||
            '","resource":"vm_workspace","source":"' || a.source ||
            '","vm_uid":"' || r.observed_vm_uid::text || '"}', 'UTF8'
        )), 'hex') THEN
        RETURN NULL;
    END IF;
    SELECT count(*) INTO reservation_count FROM public.vm_resource_reservations v
        WHERE v.request_id=r.request_id;
    IF reservation_count>0 THEN
        IF reservation_count<>1 THEN RETURN NULL; END IF;
        SELECT s.* INTO stop_row FROM public.vm_resource_cleanup_stop_receipts s
        JOIN public.vm_resource_reservations v ON v.id=s.reservation_id
        WHERE v.request_id=r.request_id AND v.state='released'
          AND s.job_id=r.job_id AND s.request_id=r.request_id
          AND s.provision_generation=r.provision_generation
          AND s.vm_uid=r.observed_vm_uid AND s.pvc_uid=r.observed_pvc_uid
          AND s.cleanup_admission_id=a.id AND s.intent_digest=a.intent_digest
          AND s.stop_evidence->'vm_absent'='true'::jsonb
          AND s.stop_evidence->'vmi_absent'='true'::jsonb
          AND s.stop_evidence->'launcher_absent'='true'::jsonb
          AND s.stop_evidence->>'pvc_disposition'='purged';
        IF NOT FOUND THEN RETURN NULL; END IF;
    ELSE
        -- A missing charge is not evidence that physical stop or storage
        -- purge occurred. Older uncharged paths remain live until they have
        -- their own exact typed terminal witness.
        RETURN NULL;
    END IF;
    RETURN jsonb_build_object(
        'kind','physical_stop','job_id',r.job_id,'request_id',r.request_id,
        'provision_generation',r.provision_generation,
        'vm_uid',r.observed_vm_uid,'pvc_uid',r.observed_pvc_uid,
        'execution_id',r.execution_id,'execution_revision',r.execution_revision,
        'execution_generation',r.execution_generation,
        'execution_chain',public.vm_job_execution_chain_evidence(r.execution_id),
        'cleanup_admission_id',a.id,'cleanup_intent_digest',a.intent_digest,
        'reservation_count',reservation_count,
        'effect_nonces',COALESCE((SELECT jsonb_agg(e.effect_nonce ORDER BY e.effect_nonce)
            FROM public.vm_creation_effects e WHERE e.request_id=r.request_id),'[]'::jsonb),
        'process_zero_receipt_ids',COALESCE((SELECT jsonb_agg(p.id ORDER BY p.id)
            FROM public.managed_repository_process_zero_receipts p
            WHERE p.owner_kind='job' AND p.owner_id=r.job_id
              AND p.scope='vm' AND p.provisioner='vm'
              AND p.runtime_incarnation=r.provision_generation::text),'[]'::jsonb),
        'reservation_ids',COALESCE((SELECT jsonb_agg(v.id ORDER BY v.id)
            FROM public.vm_resource_reservations v WHERE v.request_id=r.request_id),'[]'::jsonb),
        'stop_reservation_id',stop_row.reservation_id,
        'retry_reason',r.reason,'resolved_at',r.resolved_at);
END;
$$;

COMMIT;
