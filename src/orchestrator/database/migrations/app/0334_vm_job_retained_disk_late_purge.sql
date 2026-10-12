-- migration: 0334_vm_job_retained_disk_late_purge.sql
-- description: Exact Job-only purge after immutable retained VM stops.
-- depends-on: 0333_vm_pre_ssh_positive_stop.sql
-- transactional: yes
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

CREATE TABLE public.vm_job_retained_disk_purge_authorities (
    cleanup_admission_id uuid PRIMARY KEY REFERENCES public.vm_workspace_cleanup_admissions(id),
    job_id uuid NOT NULL REFERENCES public.vm_job_creation_owners(job_id),
    final_request_id uuid NOT NULL REFERENCES public.vm_creation_retries(request_id),
    provision_generation uuid NOT NULL,
    vm_uid uuid NOT NULL,
    vmi_uid uuid NOT NULL,
    launcher_uid uuid NOT NULL,
    pvc_uid uuid NOT NULL,
    binding_kind text NOT NULL CHECK (binding_kind IN ('bound','unbound')),
    workspace_instance_id uuid,
    workspace_generation bigint,
    cleanup_request_id uuid NOT NULL,
    intent_digest text NOT NULL CHECK (intent_digest ~ '^sha256:[0-9a-f]{64}$'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (job_id,pvc_uid),
    CHECK ((binding_kind='bound')=(workspace_instance_id IS NOT NULL AND workspace_generation IS NOT NULL)),
    CHECK (workspace_generation IS NULL OR workspace_generation>0)
);

CREATE TABLE public.vm_job_retained_disk_purge_predecessors (
    cleanup_admission_id uuid NOT NULL REFERENCES public.vm_job_retained_disk_purge_authorities(cleanup_admission_id),
    source_request_id uuid NOT NULL UNIQUE REFERENCES public.vm_creation_retries(request_id),
    old_cleanup_admission_id uuid NOT NULL UNIQUE REFERENCES public.vm_workspace_cleanup_admissions(id),
    reservation_id uuid NOT NULL UNIQUE REFERENCES public.vm_resource_reservations(id),
    reservation_revision bigint NOT NULL CHECK (reservation_revision>0),
    stop_evidence_digest text NOT NULL CHECK (stop_evidence_digest ~ '^sha256:[0-9a-f]{64}$'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (cleanup_admission_id,source_request_id)
);

CREATE TABLE public.vm_job_retained_disk_purge_receipts (
    cleanup_admission_id uuid PRIMARY KEY REFERENCES public.vm_job_retained_disk_purge_authorities(cleanup_admission_id),
    purge_evidence jsonb NOT NULL CHECK (jsonb_typeof(purge_evidence)='object'),
    chain_digest text NOT NULL CHECK (chain_digest ~ '^sha256:[0-9a-f]{64}$'),
    accepted_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

-- The digest covers the complete sorted predecessor identities, not just the
-- final generation. A post-probe re-read must reproduce it byte for byte.
CREATE FUNCTION public.vm_job_retained_disk_purge_chain_digest(parent_id uuid)
RETURNS text LANGUAGE sql STABLE AS $body$
    SELECT 'sha256:'||encode(sha256(convert_to(COALESCE((
        SELECT jsonb_agg(jsonb_build_object(
            'request_id',p.source_request_id,
            'cleanup_admission_id',p.old_cleanup_admission_id,
            'reservation_id',p.reservation_id,
            'reservation_revision',p.reservation_revision,
            'stop_evidence_digest',p.stop_evidence_digest)
            ORDER BY p.source_request_id)::text
        FROM public.vm_job_retained_disk_purge_predecessors p
        WHERE p.cleanup_admission_id=parent_id),'[]'),'UTF8')),'hex');
$body$;

CREATE FUNCTION public.guard_vm_job_retained_disk_purge_row()
RETURNS trigger LANGUAGE plpgsql AS $body$
BEGIN
    IF TG_OP<>'INSERT' THEN
        RAISE EXCEPTION 'Job retained disk purge evidence is append-only'
            USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$body$;
CREATE TRIGGER vm_job_retained_purge_authority_immutable
BEFORE UPDATE OR DELETE ON public.vm_job_retained_disk_purge_authorities
FOR EACH ROW EXECUTE FUNCTION public.guard_vm_job_retained_disk_purge_row();
CREATE TRIGGER vm_job_retained_purge_predecessor_immutable
BEFORE UPDATE OR DELETE ON public.vm_job_retained_disk_purge_predecessors
FOR EACH ROW EXECUTE FUNCTION public.guard_vm_job_retained_disk_purge_row();
CREATE TRIGGER vm_job_retained_purge_receipt_immutable
BEFORE UPDATE OR DELETE ON public.vm_job_retained_disk_purge_receipts
FOR EACH ROW EXECUTE FUNCTION public.guard_vm_job_retained_disk_purge_row();

CREATE FUNCTION public.validate_vm_job_retained_disk_purge(
    parent_id uuid, require_receipt boolean DEFAULT false
) RETURNS boolean LANGUAGE plpgsql AS $body$
DECLARE
    d public.vm_job_retained_disk_purge_authorities%ROWTYPE;
    parent public.vm_workspace_cleanup_admissions%ROWTYPE;
    owner_row public.jobs%ROWTYPE;
    retry public.vm_creation_retries%ROWTYPE;
    old_retry public.vm_creation_retries%ROWTYPE;
    old_parent public.vm_workspace_cleanup_admissions%ROWTYPE;
    old_stop public.vm_resource_cleanup_stop_receipts%ROWTYPE;
    charge public.vm_resource_reservations%ROWTYPE;
    final_charge public.vm_resource_reservations%ROWTYPE;
    link public.vm_job_retained_disk_purge_predecessors%ROWTYPE;
    receipt public.vm_job_retained_disk_purge_receipts%ROWTYPE;
    instance public.srw_workspace_instances%ROWTYPE;
    binding jsonb;
    expected_digest text;
    expected_request uuid;
    link_count bigint := 0;
    retry_count bigint := 0;
    previous_generation bigint := 0;
    previous_execution_id uuid;
    previous_execution_revision text;
    previous_execution_generation bigint;
    scope_namespace text;
    scope_cluster_id text;
BEGIN
    -- The public path already holds owner/PVC advisory locks. These row locks
    -- also make a direct/generic completion trigger re-read the actual current
    -- owner, parent and Released workspace rather than trusting a prior API
    -- response or a stale candidate projection.
    SELECT * INTO d FROM public.vm_job_retained_disk_purge_authorities
     WHERE cleanup_admission_id=parent_id FOR SHARE;
    SELECT * INTO owner_row FROM public.jobs WHERE id=d.job_id FOR UPDATE;
    PERFORM 1 FROM public.vm_job_creation_owners
     WHERE job_id=d.job_id FOR SHARE;
    SELECT * INTO parent FROM public.vm_workspace_cleanup_admissions
     WHERE id=parent_id FOR SHARE;
    SELECT * INTO retry FROM public.vm_creation_retries
     WHERE request_id=d.final_request_id FOR SHARE;
    SELECT * INTO final_charge FROM public.vm_resource_reservations
     WHERE request_id=d.final_request_id FOR SHARE;
    scope_namespace := retry.controller_configuration->>'namespace';
    scope_cluster_id := final_charge.cluster_id;
    expected_request := public.uuid_generate_v5(public.uuid_ns_url(),
        'vm-workspace-cleanup:public_vm_delete:job:'||d.job_id::text||':'||
        d.provision_generation::text||':'||d.vm_uid::text||':'||d.pvc_uid::text);
    expected_digest := 'sha256:'||encode(sha256(convert_to(format(
        '{"owner_id":"%s","owner_kind":"job","provision_generation":"%s",' ||
        '"purge_disk":true,"pvc_uid":"%s","resource":"vm_workspace",' ||
        '"source":"public_vm_delete","vm_uid":"%s"}',
        d.job_id,d.provision_generation,d.pvc_uid,d.vm_uid),'UTF8')),'hex');
    IF d.cleanup_admission_id IS NULL OR parent.id IS NULL
       OR parent.owner_kind IS DISTINCT FROM 'job'
       OR parent.owner_id IS DISTINCT FROM d.job_id
       OR parent.source IS DISTINCT FROM 'public_vm_delete'
       OR parent.parent_admission_id IS NOT NULL
       OR parent.pvc_uid IS DISTINCT FROM d.pvc_uid
       OR parent.request_id IS DISTINCT FROM expected_request
       OR d.cleanup_request_id IS DISTINCT FROM expected_request
       OR parent.intent_digest IS DISTINCT FROM expected_digest
       OR d.intent_digest IS DISTINCT FROM expected_digest
       OR (parent.completed_at IS NOT NULL AND parent.outcome IS DISTINCT FROM 'completed')
       OR owner_row.id IS NULL OR owner_row.parent_job_id IS NOT NULL
       OR owner_row.status NOT IN ('completed','failed','cancelled')
       OR NOT EXISTS (SELECT 1 FROM public.vm_job_creation_owners o
           WHERE o.job_id=d.job_id AND o.live_job_id=d.job_id AND o.deleted_at IS NULL)
       OR retry.request_id IS NULL OR retry.owner_kind IS DISTINCT FROM 'job'
       OR retry.job_id IS DISTINCT FROM d.job_id
       OR retry.provision_generation IS DISTINCT FROM d.provision_generation
       OR retry.state IS DISTINCT FROM 'succeeded' OR retry.resolved_at IS NULL
       OR retry.observed_vm_uid IS DISTINCT FROM d.vm_uid
       OR retry.observed_pvc_uid IS DISTINCT FROM d.pvc_uid
       OR retry.creation_admission_id IS NULL
       OR final_charge.id IS NULL OR final_charge.state IS DISTINCT FROM 'released'
       OR scope_namespace IS NULL OR length(scope_namespace) NOT BETWEEN 1 AND 63
       OR scope_namespace !~ '^[a-z0-9]([-a-z0-9]*[a-z0-9])?$'
       OR scope_cluster_id IS NULL OR length(scope_cluster_id)=0
       OR retry.controller_configuration->'resource_admission'->>'cluster_id'
           IS DISTINCT FROM scope_cluster_id
       OR owner_row.context->'vm'->>'provision_generation' IS DISTINCT FROM d.provision_generation::text
       OR owner_row.context->'vm'->>'vm_uid' IS DISTINCT FROM d.vm_uid::text
       OR owner_row.context->'vm'->>'rootdisk_pvc_uid' IS DISTINCT FROM d.pvc_uid::text
       OR EXISTS (SELECT 1 FROM public.vm_creation_retries later
           WHERE later.owner_kind='job' AND later.job_id=d.job_id
             AND (later.created_at,later.request_id)>(retry.created_at,retry.request_id))
       OR EXISTS (SELECT 1 FROM public.vm_resource_reservations held
           JOIN public.vm_creation_retries source USING (request_id)
           WHERE source.owner_kind='job' AND source.job_id=d.job_id
             AND held.state<>'released')
       OR EXISTS (SELECT 1 FROM public.vm_creation_effects effect
           JOIN public.vm_creation_retries source USING (request_id)
           WHERE source.owner_kind='job' AND source.job_id=d.job_id
             AND effect.state='issued')
       OR EXISTS (SELECT 1 FROM public.vm_resource_waiters waiter
           WHERE waiter.owner_kind='job' AND waiter.job_id=d.job_id
             AND waiter.state NOT IN ('released','cancelled'))
       OR EXISTS (SELECT 1 FROM public.vm_workspace_recoveries recovery
           WHERE recovery.resolved_at IS NULL AND
             ((recovery.owner_kind='job' AND recovery.owner_id=d.job_id)
              OR recovery.root_pvc_uid=d.pvc_uid))
       OR EXISTS (SELECT 1 FROM public.vm_workspace_recovery_retention_pins pin
           WHERE pin.pvc_uid=d.pvc_uid AND pin.released_at IS NULL)
       OR EXISTS (SELECT 1 FROM public.vm_idle_access_leases lease
           WHERE lease.owner_kind='job' AND lease.owner_id=d.job_id
             AND lease.closed_at IS NULL AND lease.expires_at>clock_timestamp())
       OR EXISTS (SELECT 1 FROM public.vm_workspace_cleanup_admissions other
           WHERE other.owner_kind='job' AND other.owner_id=d.job_id
             AND other.completed_at IS NULL AND other.id<>parent_id
             AND other.source<>'terminal_checkpoint_prune'
             AND NOT (other.source='controller_rootdisk_delete'
               AND other.parent_admission_id=parent_id
               AND other.pvc_uid=d.pvc_uid
               AND NOT require_receipt
               AND other.request_id IS NOT NULL
               AND other.intent_digest ~ '^sha256:[0-9a-f]{64}$')) THEN
        RAISE EXCEPTION 'Job retained disk purge current authority changed'
            USING ERRCODE='23514',CONSTRAINT='vm_job_retained_purge_authority';
    END IF;
    IF d.binding_kind='bound' THEN
        binding := retry.canonical_request->'workspace_storage';
        IF jsonb_typeof(binding) IS DISTINCT FROM 'object'
           OR jsonb_typeof(binding->'generation') IS DISTINCT FROM 'number'
           OR (binding->>'generation') !~ '^[1-9][0-9]*$' THEN
            RAISE EXCEPTION 'Job retained disk workspace generation unproven'
                USING ERRCODE='23514',CONSTRAINT='vm_job_retained_purge_binding';
        END IF;
        SELECT * INTO instance FROM public.srw_workspace_instances
         WHERE id=d.workspace_instance_id FOR UPDATE;
        IF jsonb_typeof(binding) IS DISTINCT FROM 'object'
           OR binding->>'uid' IS DISTINCT FROM d.workspace_instance_id::text
           OR binding->>'generation' IS DISTINCT FROM d.workspace_generation::text
           OR (d.workspace_generation>1 AND
               binding->>'pvc_uid' IS DISTINCT FROM d.pvc_uid::text)
           OR (binding->>'pvc_uid' IS NOT NULL AND
               binding->>'pvc_uid' IS DISTINCT FROM d.pvc_uid::text)
           OR binding->>'owner_kind' IS DISTINCT FROM 'job'
           OR binding->>'owner_id' IS DISTINCT FROM d.job_id::text
           OR instance.id IS NULL OR instance.generation IS DISTINCT FROM d.workspace_generation
           OR instance.pvc_uid IS DISTINCT FROM d.pvc_uid::text
           OR instance.status IS DISTINCT FROM 'Released'
           OR instance.execution_id IS NOT NULL OR instance.pod_uid IS NOT NULL
           OR NOT EXISTS (SELECT 1 FROM public.srw_execution_workspace_bindings b
                WHERE b.execution_id=retry.execution_id
                  AND b.instance_id=d.workspace_instance_id)
           OR EXISTS (SELECT 1 FROM public.srw_workspace_instances other
                WHERE other.id<>d.workspace_instance_id AND other.pvc_uid=d.pvc_uid::text)
           OR EXISTS (SELECT 1 FROM public.srw_workspace_instances other
                WHERE other.id=d.workspace_instance_id AND other.status<>'Released') THEN
            RAISE EXCEPTION 'Job retained disk workspace release unproven'
                USING ERRCODE='23514',CONSTRAINT='vm_job_retained_purge_binding';
        END IF;
    ELSE
        IF d.binding_kind IS DISTINCT FROM 'unbound'
           OR d.workspace_instance_id IS NOT NULL OR d.workspace_generation IS NOT NULL
           OR COALESCE(retry.canonical_request->'workspace_storage','null'::jsonb)<>'null'::jsonb
           OR EXISTS (SELECT 1 FROM public.srw_workspace_instances foreign_workspace
                WHERE foreign_workspace.pvc_uid=d.pvc_uid::text)
           OR EXISTS (SELECT 1 FROM public.srw_execution_workspace_bindings b
                WHERE b.execution_id=retry.execution_id)
           OR EXISTS (SELECT 1 FROM public.vm_workspace_recovery_jobs r
                WHERE r.job_id=d.job_id AND r.resolved_at IS NULL) THEN
            RAISE EXCEPTION 'Job retained rootdisk has another owner'
                USING ERRCODE='23514',CONSTRAINT='vm_job_retained_purge_unbound';
        END IF;
    END IF;

    FOR link IN SELECT p.* FROM public.vm_job_retained_disk_purge_predecessors p
        JOIN public.vm_creation_retries source ON source.request_id=p.source_request_id
        WHERE p.cleanup_admission_id=parent_id
        ORDER BY source.created_at,source.request_id
        FOR SHARE OF p
    LOOP
        link_count := link_count+1;
        SELECT * INTO old_retry FROM public.vm_creation_retries
         WHERE request_id=link.source_request_id FOR SHARE;
        SELECT * INTO old_parent FROM public.vm_workspace_cleanup_admissions
         WHERE id=link.old_cleanup_admission_id FOR SHARE;
        SELECT * INTO old_stop FROM public.vm_resource_cleanup_stop_receipts
         WHERE cleanup_admission_id=link.old_cleanup_admission_id FOR SHARE;
        SELECT * INTO charge FROM public.vm_resource_reservations
         WHERE id=link.reservation_id FOR SHARE;
        IF old_retry.request_id IS NULL OR old_retry.owner_kind IS DISTINCT FROM 'job'
           OR old_retry.job_id IS DISTINCT FROM d.job_id
           OR old_retry.state IS DISTINCT FROM 'succeeded'
           OR old_retry.resolved_at IS NULL
           OR old_retry.observed_pvc_uid IS DISTINCT FROM d.pvc_uid
           OR old_retry.observed_vm_uid IS DISTINCT FROM old_stop.vm_uid
           OR old_retry.creation_admission_id IS NULL
           OR old_stop.cleanup_admission_id IS NULL
           OR old_stop.request_id IS DISTINCT FROM old_retry.request_id
           OR old_stop.job_id IS DISTINCT FROM d.job_id
           OR old_stop.provision_generation IS DISTINCT FROM old_retry.provision_generation
           OR old_stop.pvc_uid IS DISTINCT FROM d.pvc_uid
           OR old_stop.reservation_id IS DISTINCT FROM link.reservation_id
           OR old_stop.intent_digest IS DISTINCT FROM old_parent.intent_digest
           OR old_stop.stop_evidence->>'pvc_disposition' IS DISTINCT FROM 'retained'
           OR old_stop.stop_evidence->>'vm_absent' IS DISTINCT FROM 'true'
           OR old_stop.stop_evidence->>'vmi_absent' IS DISTINCT FROM 'true'
           OR old_stop.stop_evidence->>'launcher_absent' IS DISTINCT FROM 'true'
           OR old_stop.stop_evidence->>'controller_authenticated' IS DISTINCT FROM 'true'
           OR old_stop.stop_evidence->>'same_generation_replacement' IS DISTINCT FROM 'false'
           OR link.stop_evidence_digest IS DISTINCT FROM
               'sha256:'||encode(sha256(convert_to(old_stop.stop_evidence::text,'UTF8')),'hex')
           OR old_parent.id IS NULL OR old_parent.owner_kind IS DISTINCT FROM 'job'
           OR old_parent.owner_id IS DISTINCT FROM d.job_id
           OR old_parent.pvc_uid IS DISTINCT FROM d.pvc_uid
           OR old_parent.parent_admission_id IS NOT NULL
           OR old_parent.completed_at IS NULL OR old_parent.outcome IS DISTINCT FROM 'completed'
           OR old_parent.source NOT IN (
               'dispatcher_vm_recycle','lifecycle_vm_reap',
               'job_terminal_vm_release','public_vm_delete')
           OR charge.id IS NULL OR charge.request_id IS DISTINCT FROM old_retry.request_id
           OR charge.revision IS DISTINCT FROM link.reservation_revision
           OR charge.state IS DISTINCT FROM 'released'
           OR charge.cluster_id IS DISTINCT FROM scope_cluster_id
           OR old_retry.controller_configuration->>'namespace' IS DISTINCT FROM scope_namespace
           OR old_retry.controller_configuration->'resource_admission'->>'cluster_id'
               IS DISTINCT FROM scope_cluster_id
           OR charge.vm_uid IS DISTINCT FROM old_stop.vm_uid
           OR charge.release_evidence->>'kind' IS DISTINCT FROM 'exact_cleanup_compute_absent'
           OR charge.release_evidence->>'cleanup_admission_id' IS DISTINCT FROM old_parent.id::text
           OR charge.release_evidence->>'stop_evidence_digest' IS DISTINCT FROM link.stop_evidence_digest
           OR NOT EXISTS (SELECT 1 FROM public.managed_repository_process_zero_receipts z
               WHERE z.owner_kind='job' AND z.owner_id=d.job_id
                 AND z.scope='vm' AND z.provisioner='vm'
                 AND z.runtime_incarnation=old_retry.provision_generation::text)
           OR EXISTS (SELECT 1 FROM public.vm_pre_ssh_stop_intents i
               WHERE i.job_id=d.job_id AND i.provision_generation=old_retry.provision_generation
                 AND (i.cleanup_admission_id<>old_parent.id OR
                 NOT EXISTS (SELECT 1 FROM public.vm_pre_ssh_stop_proofs proof
                     WHERE proof.cleanup_admission_id=i.cleanup_admission_id
                       AND proof.job_id=d.job_id
                       AND proof.provision_generation=old_retry.provision_generation
                       AND proof.terminal_evidence->>'vm_uid'=old_stop.vm_uid::text
                       AND proof.terminal_evidence->>'launcher_uid'=old_stop.launcher_uid::text)))
           OR EXISTS (SELECT 1 FROM public.vm_resource_recovery_successors successor
               WHERE successor.reservation_id=charge.id
                 AND (successor.owner_id<>d.job_id
                      OR successor.provision_generation<>old_retry.provision_generation
                      OR successor.vm_uid<>old_stop.vm_uid
                      OR successor.root_pvc_uid<>d.pvc_uid)) THEN
            RAISE EXCEPTION 'Job retained disk predecessor changed'
                USING ERRCODE='23514',CONSTRAINT='vm_job_retained_purge_predecessor';
        END IF;
        IF d.binding_kind='bound' THEN
            binding := old_retry.canonical_request->'workspace_storage';
            IF jsonb_typeof(binding) IS DISTINCT FROM 'object'
               OR jsonb_typeof(binding->'generation') IS DISTINCT FROM 'number'
               OR (binding->>'generation') !~ '^[1-9][0-9]*$' THEN
                RAISE EXCEPTION 'Job retained disk predecessor generation unproven'
                    USING ERRCODE='23514',CONSTRAINT='vm_job_retained_purge_predecessor_binding';
            END IF;
            IF jsonb_typeof(binding) IS DISTINCT FROM 'object'
               OR binding->>'uid' IS DISTINCT FROM d.workspace_instance_id::text
               OR binding->>'owner_id' IS DISTINCT FROM d.job_id::text
               OR binding->>'owner_kind' IS DISTINCT FROM 'job'
               OR (binding->>'pvc_uid' IS NOT NULL AND
                   binding->>'pvc_uid' IS DISTINCT FROM d.pvc_uid::text)
               OR ((binding->>'generation')::bigint>1 AND
                   binding->>'pvc_uid' IS DISTINCT FROM d.pvc_uid::text)
               OR (binding->>'generation')::bigint<previous_generation
               OR (binding->>'generation')::bigint>d.workspace_generation
               OR ((binding->>'generation')::bigint=previous_generation
                   AND (old_retry.execution_id IS DISTINCT FROM previous_execution_id
                     OR old_retry.execution_revision IS DISTINCT FROM previous_execution_revision
                     OR old_retry.execution_generation IS DISTINCT FROM previous_execution_generation))
               OR NOT EXISTS (SELECT 1 FROM public.srw_execution_workspace_bindings b
                    WHERE b.execution_id=old_retry.execution_id
                      AND b.instance_id=d.workspace_instance_id) THEN
                RAISE EXCEPTION 'Job retained disk predecessor binding changed'
                    USING ERRCODE='23514',CONSTRAINT='vm_job_retained_purge_predecessor_binding';
            END IF;
            previous_generation := (binding->>'generation')::bigint;
            previous_execution_id := old_retry.execution_id;
            previous_execution_revision := old_retry.execution_revision;
            previous_execution_generation := old_retry.execution_generation;
        ELSE
            IF COALESCE(old_retry.canonical_request->'workspace_storage','null'::jsonb)<>'null'::jsonb THEN
                RAISE EXCEPTION 'Job retained rootdisk predecessor changed'
                    USING ERRCODE='23514';
            END IF;
        END IF;
    END LOOP;
    IF NOT EXISTS (SELECT 1 FROM public.vm_job_retained_disk_purge_predecessors final_link
        JOIN public.vm_resource_cleanup_stop_receipts final_stop
          ON final_stop.cleanup_admission_id=final_link.old_cleanup_admission_id
        WHERE final_link.cleanup_admission_id=parent_id
          AND final_link.source_request_id=d.final_request_id
          AND final_stop.vmi_uid=d.vmi_uid
          AND final_stop.launcher_uid=d.launcher_uid) THEN
        RAISE EXCEPTION 'Job retained disk final runtime identity changed'
            USING ERRCODE='23514',CONSTRAINT='vm_job_retained_purge_predecessor';
    END IF;
    SELECT count(*) INTO retry_count FROM public.vm_creation_retries r
     WHERE r.owner_kind='job' AND r.job_id=d.job_id;
    IF link_count=0 OR retry_count<link_count
       OR NOT EXISTS (SELECT 1 FROM public.vm_job_retained_disk_purge_predecessors p
           WHERE p.cleanup_admission_id=parent_id AND p.source_request_id=d.final_request_id)
       OR EXISTS (SELECT 1 FROM public.vm_resource_cleanup_stop_receipts s
           WHERE s.job_id=d.job_id AND s.stop_evidence->>'pvc_disposition'='retained'
             AND NOT EXISTS (SELECT 1 FROM public.vm_job_retained_disk_purge_predecessors p
                 WHERE p.cleanup_admission_id=parent_id AND p.source_request_id=s.request_id)) THEN
        RAISE EXCEPTION 'Job retained disk predecessor chain incomplete'
            USING ERRCODE='23514',CONSTRAINT='vm_job_retained_purge_chain';
    END IF;
    -- An earlier retry outside this same-PVC retained chain still needs its
    -- own already-completed ordinary purged or never-issued terminal packet.
    -- The 0326 logical predecessor guard continues to reject physical A then
    -- never-issued B; this branch does not fabricate an attestor for B.
    FOR old_retry IN SELECT r.* FROM public.vm_creation_retries r
      WHERE r.owner_kind='job' AND r.job_id=d.job_id
        AND NOT EXISTS (SELECT 1 FROM public.vm_job_retained_disk_purge_predecessors p
            WHERE p.cleanup_admission_id=parent_id AND p.source_request_id=r.request_id)
      ORDER BY r.created_at,r.request_id
    LOOP
        IF (old_retry.created_at,old_retry.request_id)>=(retry.created_at,retry.request_id)
           OR COALESCE(public.vm_job_terminal_prior_packet_evidence(old_retry.request_id)->>'kind','')
              NOT IN ('never_issued','physical_stop') THEN
            RAISE EXCEPTION 'Job retained disk has unclassified retry history'
                USING ERRCODE='23514',CONSTRAINT='vm_job_retained_purge_chain';
        END IF;
    END LOOP;
    SELECT * INTO receipt FROM public.vm_job_retained_disk_purge_receipts
     WHERE cleanup_admission_id=parent_id;
    IF require_receipt AND receipt.cleanup_admission_id IS NULL THEN
        RAISE EXCEPTION 'Job retained disk purge lacks signed receipt'
            USING ERRCODE='23514',CONSTRAINT='vm_job_retained_purge_receipt';
    END IF;
    IF receipt.cleanup_admission_id IS NOT NULL AND (
       receipt.chain_digest IS DISTINCT FROM public.vm_job_retained_disk_purge_chain_digest(parent_id)
       OR receipt.purge_evidence IS DISTINCT FROM (jsonb_build_object(
           'version',1,'kind','vm_cleanup_physical_stop','job_id',d.job_id,
           'provision_generation',d.provision_generation,'vm_uid',d.vm_uid,
           'vmi_uid',d.vmi_uid,'launcher_uid',d.launcher_uid,'pvc_uid',d.pvc_uid,
           'vm_absent',true,'vmi_absent',true,'launcher_absent',true,
           'same_generation_replacement',false,'controller_authenticated',true,
           'pvc_disposition','purged',
           'controller_scope',jsonb_build_object(
               'version',1,'namespace',scope_namespace,'cluster_id',scope_cluster_id)) ||
           CASE WHEN d.binding_kind='bound'
               THEN jsonb_build_object('captured_workspace_storage',
                   jsonb_set(retry.canonical_request->'workspace_storage',
                       '{pvc_uid}',to_jsonb(d.pvc_uid::text),true))
               ELSE '{}'::jsonb END)) THEN
        RAISE EXCEPTION 'Job retained disk purge receipt changed'
            USING ERRCODE='23514',CONSTRAINT='vm_job_retained_purge_receipt';
    END IF;
    RETURN true;
END;
$body$;

CREATE FUNCTION public.vm_job_retained_disk_purge_candidate(parent_id uuid)
RETURNS jsonb LANGUAGE plpgsql AS $body$
DECLARE d public.vm_job_retained_disk_purge_authorities%ROWTYPE;
        r public.vm_creation_retries%ROWTYPE;
        binding jsonb;
BEGIN
    PERFORM public.validate_vm_job_retained_disk_purge(parent_id,false);
    SELECT * INTO d FROM public.vm_job_retained_disk_purge_authorities
      WHERE cleanup_admission_id=parent_id;
    SELECT * INTO r FROM public.vm_creation_retries
      WHERE request_id=d.final_request_id;
    binding := r.canonical_request->'workspace_storage';
    RETURN jsonb_build_object(
        'owner_kind','job','job_id',d.job_id,
        'provision_generation',d.provision_generation,
        'vm_uid',d.vm_uid,'vmi_uid',d.vmi_uid,'launcher_uid',d.launcher_uid,
        'pvc_uid',d.pvc_uid,'purge_disk',true,'binding_kind',d.binding_kind,
        'controller_scope',jsonb_build_object(
            'version',1,'namespace',r.controller_configuration->>'namespace',
            'cluster_id',r.controller_configuration->'resource_admission'->>'cluster_id'),
        'workspace_storage',CASE WHEN d.binding_kind='bound' THEN binding ELSE NULL END,
        'captured_workspace_storage',CASE WHEN d.binding_kind='bound'
            THEN jsonb_set(binding,'{pvc_uid}',to_jsonb(d.pvc_uid::text),true)
            ELSE NULL END,
        'chain_digest',public.vm_job_retained_disk_purge_chain_digest(parent_id));
END;
$body$;

CREATE FUNCTION public.guard_vm_job_retained_disk_purge_receipt_insert()
RETURNS trigger LANGUAGE plpgsql AS $body$
DECLARE d public.vm_job_retained_disk_purge_authorities%ROWTYPE;
        r public.vm_creation_retries%ROWTYPE;
BEGIN
    PERFORM public.validate_vm_job_retained_disk_purge(NEW.cleanup_admission_id,false);
    SELECT * INTO d FROM public.vm_job_retained_disk_purge_authorities
      WHERE cleanup_admission_id=NEW.cleanup_admission_id;
    SELECT * INTO r FROM public.vm_creation_retries
      WHERE request_id=d.final_request_id;
    IF NEW.chain_digest IS DISTINCT FROM public.vm_job_retained_disk_purge_chain_digest(NEW.cleanup_admission_id)
       OR NEW.purge_evidence IS DISTINCT FROM (jsonb_build_object(
           'version',1,'kind','vm_cleanup_physical_stop','job_id',d.job_id,
           'provision_generation',d.provision_generation,'vm_uid',d.vm_uid,
           'vmi_uid',d.vmi_uid,'launcher_uid',d.launcher_uid,'pvc_uid',d.pvc_uid,
           'vm_absent',true,'vmi_absent',true,'launcher_absent',true,
           'same_generation_replacement',false,'controller_authenticated',true,
           'pvc_disposition','purged',
           'controller_scope',jsonb_build_object(
               'version',1,'namespace',r.controller_configuration->>'namespace',
               'cluster_id',r.controller_configuration->'resource_admission'->>'cluster_id')) ||
           CASE WHEN d.binding_kind='bound'
               THEN jsonb_build_object('captured_workspace_storage',
                   jsonb_set(r.canonical_request->'workspace_storage',
                       '{pvc_uid}',to_jsonb(d.pvc_uid::text),true))
               ELSE '{}'::jsonb END) THEN
        RAISE EXCEPTION 'Job retained disk purge receipt lacks exact authority'
            USING ERRCODE='23514',CONSTRAINT='vm_job_retained_purge_receipt';
    END IF;
    RETURN NEW;
END;
$body$;
CREATE TRIGGER vm_job_retained_purge_receipt_insert
BEFORE INSERT ON public.vm_job_retained_disk_purge_receipts
FOR EACH ROW EXECUTE FUNCTION public.guard_vm_job_retained_disk_purge_receipt_insert();

-- A generic completed parent must not turn a prior retained stop into a
-- fictional purged stop. This fires even when no new authority was inserted.
CREATE FUNCTION public.guard_vm_job_retained_disk_purge_parent()
RETURNS trigger LANGUAGE plpgsql AS $body$
DECLARE retained boolean;
BEGIN
    IF TG_OP='UPDATE' AND (
        (NEW.id,NEW.owner_kind,NEW.owner_id,NEW.pvc_uid,NEW.source,NEW.request_id,
         NEW.intent_digest,NEW.parent_admission_id,NEW.admitted_at) IS DISTINCT FROM
        (OLD.id,OLD.owner_kind,OLD.owner_id,OLD.pvc_uid,OLD.source,OLD.request_id,
         OLD.intent_digest,OLD.parent_admission_id,OLD.admitted_at)
        OR (OLD.completed_at IS NOT NULL AND NEW IS DISTINCT FROM OLD)) THEN
        RAISE EXCEPTION 'Job retained disk purge parent identity is immutable'
            USING ERRCODE='23514';
    END IF;
    IF NEW.owner_kind<>'job' OR NEW.source<>'public_vm_delete'
       OR NEW.pvc_uid IS NULL OR NEW.completed_at IS NULL THEN
        RETURN NEW;
    END IF;
    SELECT EXISTS (
        SELECT 1 FROM public.vm_resource_cleanup_stop_receipts s
        JOIN public.vm_workspace_cleanup_admissions predecessor_parent
          ON predecessor_parent.id=s.cleanup_admission_id
        JOIN public.vm_resource_reservations charge ON charge.id=s.reservation_id
        WHERE s.job_id=NEW.owner_id AND s.pvc_uid=NEW.pvc_uid
          AND predecessor_parent.owner_kind='job'
          AND predecessor_parent.owner_id=NEW.owner_id
          AND predecessor_parent.completed_at IS NOT NULL
          AND predecessor_parent.outcome='completed'
          AND charge.state='released'
          AND s.stop_evidence->>'pvc_disposition'='retained'
    ) INTO retained;
    IF retained THEN
        IF NEW.outcome IS DISTINCT FROM 'completed' THEN
            RAISE EXCEPTION 'Job retained disk purge cannot settle without proof'
                USING ERRCODE='23514';
        END IF;
        PERFORM public.validate_vm_job_retained_disk_purge(NEW.id,true);
    END IF;
    RETURN NEW;
END;
$body$;
CREATE TRIGGER vm_job_retained_purge_parent_insert
BEFORE INSERT ON public.vm_workspace_cleanup_admissions
FOR EACH ROW WHEN (NEW.owner_kind='job' AND NEW.source='public_vm_delete')
EXECUTE FUNCTION public.guard_vm_job_retained_disk_purge_parent();
CREATE TRIGGER vm_job_retained_purge_parent_update
BEFORE UPDATE ON public.vm_workspace_cleanup_admissions
FOR EACH ROW WHEN ((OLD.owner_kind='job' AND OLD.source='public_vm_delete')
              OR (NEW.owner_kind='job' AND NEW.source='public_vm_delete'))
EXECUTE FUNCTION public.guard_vm_job_retained_disk_purge_parent();

ALTER TABLE public.vm_job_creation_terminal_packets
    DROP CONSTRAINT vm_job_creation_terminal_packets_terminal_kind_check,
    ADD CONSTRAINT vm_job_creation_terminal_packets_terminal_kind_check
        CHECK (terminal_kind IN ('never_issued','physical_stop','retained_late_purge'))
        NOT VALID;
-- The replaced validated constraint already proves every preexisting row is
-- one of the two old values; NOT VALID avoids a write-blocking table scan.

-- Keep the original never-issued and ordinary purged physical branch intact.
ALTER FUNCTION public.vm_job_terminal_packet_evidence(uuid)
    RENAME TO vm_job_terminal_prior_packet_evidence;
CREATE FUNCTION public.vm_job_terminal_packet_evidence(source_request uuid)
RETURNS jsonb LANGUAGE plpgsql AS $body$
DECLARE prior jsonb;
        r public.vm_creation_retries%ROWTYPE;
        d public.vm_job_retained_disk_purge_authorities%ROWTYPE;
        p public.vm_job_retained_disk_purge_predecessors%ROWTYPE;
        old_stop public.vm_resource_cleanup_stop_receipts%ROWTYPE;
        parent public.vm_workspace_cleanup_admissions%ROWTYPE;
        charge public.vm_resource_reservations%ROWTYPE;
BEGIN
    prior := public.vm_job_terminal_prior_packet_evidence(source_request);
    IF prior IS NOT NULL THEN RETURN prior; END IF;
    SELECT * INTO r FROM public.vm_creation_retries WHERE request_id=source_request;
    IF r.request_id IS NULL OR r.owner_kind IS DISTINCT FROM 'job'
       OR r.state IS DISTINCT FROM 'succeeded' THEN RETURN NULL; END IF;
    SELECT * INTO p FROM public.vm_job_retained_disk_purge_predecessors
      WHERE source_request_id=source_request;
    SELECT * INTO d FROM public.vm_job_retained_disk_purge_authorities
      WHERE cleanup_admission_id=p.cleanup_admission_id;
    SELECT * INTO parent FROM public.vm_workspace_cleanup_admissions
      WHERE id=d.cleanup_admission_id;
    SELECT * INTO old_stop FROM public.vm_resource_cleanup_stop_receipts
      WHERE cleanup_admission_id=p.old_cleanup_admission_id;
    SELECT * INTO charge FROM public.vm_resource_reservations
      WHERE id=p.reservation_id;
    IF p.source_request_id IS NULL OR d.cleanup_admission_id IS NULL
       OR parent.completed_at IS NULL OR parent.outcome IS DISTINCT FROM 'completed'
       OR d.job_id IS DISTINCT FROM r.job_id
       OR d.pvc_uid IS DISTINCT FROM r.observed_pvc_uid
       OR old_stop.request_id IS DISTINCT FROM r.request_id
       OR old_stop.provision_generation IS DISTINCT FROM r.provision_generation
       OR charge.state IS DISTINCT FROM 'released' THEN RETURN NULL; END IF;
    PERFORM public.validate_vm_job_retained_disk_purge(d.cleanup_admission_id,true);
    RETURN jsonb_build_object(
        'kind','retained_late_purge','job_id',r.job_id,
        'request_id',r.request_id,'provision_generation',r.provision_generation,
        'vm_uid',r.observed_vm_uid,'pvc_uid',r.observed_pvc_uid,
        'execution_id',r.execution_id,'execution_revision',r.execution_revision,
        'execution_generation',r.execution_generation,
        'execution_chain',public.vm_job_execution_chain_evidence(r.execution_id),
        'cleanup_admission_id',d.cleanup_admission_id,
        'cleanup_intent_digest',parent.intent_digest,
        'old_cleanup_admission_id',p.old_cleanup_admission_id,
        'old_stop_evidence_digest',p.stop_evidence_digest,
        'old_reservation_id',p.reservation_id,
        'old_reservation_revision',p.reservation_revision,
        'purge_generation',d.provision_generation,
        'binding_kind',d.binding_kind,
        'workspace_instance_id',d.workspace_instance_id,
        'workspace_generation',d.workspace_generation,
        'chain_digest',public.vm_job_retained_disk_purge_chain_digest(d.cleanup_admission_id),
        'process_zero_receipt_ids',COALESCE((
            SELECT jsonb_agg(z.id ORDER BY z.id)
            FROM public.managed_repository_process_zero_receipts z
            WHERE z.owner_kind='job' AND z.owner_id=r.job_id
              AND z.scope='vm' AND z.provisioner='vm'
              AND z.runtime_incarnation=r.provision_generation::text),'[]'::jsonb),
        'retry_reason',r.reason,'resolved_at',r.resolved_at);
END;
$body$;

COMMIT;
