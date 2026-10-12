-- migration: 0264_vm_resource_reservations.sql
-- description: Durable VM resource request and reservation identities; enforcement remains off.
-- depends-on: 0263_vm_resource_inventory.sql
-- transactional: yes
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

CREATE TABLE public.vm_resource_admission_policy (
    cluster_id TEXT PRIMARY KEY CHECK (length(cluster_id) BETWEEN 1 AND 253),
    namespace TEXT NOT NULL CHECK (length(namespace) BETWEEN 1 AND 63),
    policy_digest TEXT NOT NULL CHECK (policy_digest ~ '^sha256:[0-9a-f]{64}$'),
    document JSONB NOT NULL CHECK (jsonb_typeof(document) = 'object'),
    mode TEXT NOT NULL DEFAULT 'off' CHECK (mode IN ('off','shadow','enforce','drain')),
    revision BIGINT NOT NULL DEFAULT 1 CHECK (revision > 0),
    admission_sequence BIGINT NOT NULL DEFAULT 0 CHECK (admission_sequence >= 0),
    maintenance_enqueued_at TIMESTAMPTZ,
    maintenance_request_id UUID,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK ((maintenance_enqueued_at IS NULL) = (maintenance_request_id IS NULL))
);

CREATE TABLE public.vm_resource_nodes (
    cluster_id TEXT NOT NULL REFERENCES public.vm_resource_admission_policy(cluster_id),
    node_uid UUID NOT NULL,
    node_name TEXT NOT NULL CHECK (length(node_name) BETWEEN 1 AND 253),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (cluster_id,node_uid),
    UNIQUE (cluster_id,node_uid,node_name)
);

CREATE TABLE public.vm_resource_waiters (
    request_id UUID PRIMARY KEY,
    job_id UUID NOT NULL,
    provision_generation UUID NOT NULL,
    cluster_id TEXT NOT NULL REFERENCES public.vm_resource_admission_policy(cluster_id),
    policy_digest TEXT NOT NULL CHECK (policy_digest ~ '^sha256:[0-9a-f]{64}$'),
    owner_key TEXT NOT NULL CHECK (length(owner_key) BETWEEN 1 AND 253),
    project_id UUID,
    priority INTEGER NOT NULL,
    request_digest TEXT NOT NULL CHECK (request_digest ~ '^sha256:[0-9a-f]{64}$'),
    guest_vcpus BIGINT NOT NULL CHECK (guest_vcpus > 0),
    guest_memory_bytes BIGINT NOT NULL CHECK (guest_memory_bytes > 0),
    cpu_millicores BIGINT NOT NULL CHECK (cpu_millicores > 0),
    memory_bytes BIGINT NOT NULL CHECK (memory_bytes > 0),
    kvm_devices BIGINT NOT NULL CHECK (kvm_devices > 0),
    placement JSONB NOT NULL CHECK (jsonb_typeof(placement) = 'object'),
    state TEXT NOT NULL DEFAULT 'waiting' CHECK (state IN ('waiting','nonfit','admitted','cancelled','released')),
    reason TEXT CHECK (length(reason) <= 80),
    bypasses BIGINT NOT NULL DEFAULT 0 CHECK (bypasses >= 0),
    protected_order BIGINT CHECK (protected_order >= 0),
    revision BIGINT NOT NULL DEFAULT 1 CHECK (revision > 0),
    evaluated_snapshot_id UUID,
    enqueued_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (request_id,cluster_id,policy_digest),
    FOREIGN KEY (request_id) REFERENCES public.vm_creation_retries(request_id)
);
CREATE INDEX vm_resource_waiter_heads ON public.vm_resource_waiters(cluster_id,state,owner_key,enqueued_at,request_id);
CREATE INDEX vm_resource_waiter_maintenance ON public.vm_resource_waiters(cluster_id,enqueued_at,request_id);

CREATE TABLE public.vm_resource_owner_fairness (
    cluster_id TEXT NOT NULL REFERENCES public.vm_resource_admission_policy(cluster_id),
    owner_key TEXT NOT NULL CHECK (length(owner_key) BETWEEN 1 AND 253),
    last_admitted_sequence BIGINT NOT NULL DEFAULT 0 CHECK (last_admitted_sequence >= 0),
    PRIMARY KEY (cluster_id,owner_key)
);

CREATE TABLE public.vm_resource_reservations (
    id UUID PRIMARY KEY,
    request_id UUID NOT NULL,
    revision BIGINT NOT NULL CHECK (revision > 0),
    cluster_id TEXT NOT NULL,
    policy_digest TEXT NOT NULL CHECK (policy_digest ~ '^sha256:[0-9a-f]{64}$'),
    node_uid UUID NOT NULL,
    node_name TEXT NOT NULL,
    cpu_millicores BIGINT NOT NULL CHECK (cpu_millicores > 0),
    memory_bytes BIGINT NOT NULL CHECK (memory_bytes > 0),
    kvm_devices BIGINT NOT NULL CHECK (kvm_devices > 0),
    snapshot_id UUID NOT NULL,
    snapshot_digest TEXT NOT NULL CHECK (snapshot_digest ~ '^sha256:[0-9a-f]{64}$'),
    state TEXT NOT NULL DEFAULT 'reserved' CHECK (state IN ('reserved','active','warm','teardown','released')),
    vm_uid UUID,
    vmi_uid UUID,
    launcher_uid UUID,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    released_at TIMESTAMPTZ,
    release_evidence JSONB CHECK (jsonb_typeof(release_evidence) = 'object'),
    UNIQUE (request_id,revision),
    FOREIGN KEY (request_id,cluster_id,policy_digest)
        REFERENCES public.vm_resource_waiters(request_id,cluster_id,policy_digest),
    FOREIGN KEY (cluster_id,node_uid,node_name)
        REFERENCES public.vm_resource_nodes(cluster_id,node_uid,node_name),
    FOREIGN KEY (snapshot_id,cluster_id,policy_digest)
        REFERENCES public.vm_resource_inventory_snapshots(snapshot_id,cluster_id,policy_digest),
    CHECK ((state='released') = (released_at IS NOT NULL AND release_evidence IS NOT NULL)),
    CHECK (state='released' OR (released_at IS NULL AND release_evidence IS NULL)),
    CHECK (state NOT IN ('active','warm') OR (vm_uid IS NOT NULL AND vmi_uid IS NOT NULL AND launcher_uid IS NOT NULL))
);
CREATE UNIQUE INDEX vm_resource_one_held_request ON public.vm_resource_reservations(request_id) WHERE state<>'released';
CREATE UNIQUE INDEX vm_resource_one_held_vm ON public.vm_resource_reservations(cluster_id,vm_uid) WHERE state<>'released' AND vm_uid IS NOT NULL;
CREATE UNIQUE INDEX vm_resource_one_held_vmi ON public.vm_resource_reservations(cluster_id,vmi_uid) WHERE state<>'released' AND vmi_uid IS NOT NULL;
CREATE UNIQUE INDEX vm_resource_one_held_launcher ON public.vm_resource_reservations(cluster_id,launcher_uid) WHERE state<>'released' AND launcher_uid IS NOT NULL;
CREATE INDEX vm_resource_held_node ON public.vm_resource_reservations(cluster_id,node_uid) WHERE state<>'released';
CREATE INDEX vm_resource_snapshot_references ON public.vm_resource_reservations(snapshot_id);

CREATE FUNCTION public.guard_vm_resource_waiter() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='INSERT' THEN
        -- The parent fields are already immutable. Its existing PK/FK retains
        -- identity; this comparison avoids a blocking duplicate UNIQUE index.
        IF NOT EXISTS (SELECT 1 FROM public.vm_creation_retries r
            WHERE r.request_id=NEW.request_id AND r.job_id=NEW.job_id
            AND r.provision_generation=NEW.provision_generation AND r.request_digest=NEW.request_digest) THEN
            RAISE EXCEPTION 'VM resource waiter source identity mismatch' USING ERRCODE='23503';
        END IF;
        IF NEW.state<>'waiting' OR NEW.bypasses<>0 OR NEW.protected_order IS NOT NULL THEN
            RAISE EXCEPTION 'VM resource waiter must start waiting' USING ERRCODE='23514';
        END IF;
        RETURN NEW;
    END IF;
    IF ROW(NEW.request_id,NEW.job_id,NEW.provision_generation,NEW.cluster_id,NEW.policy_digest,
           NEW.owner_key,NEW.project_id,NEW.priority,NEW.request_digest,NEW.guest_vcpus,
           NEW.guest_memory_bytes,NEW.cpu_millicores,NEW.memory_bytes,NEW.kvm_devices,NEW.placement,NEW.enqueued_at)
       IS DISTINCT FROM
       ROW(OLD.request_id,OLD.job_id,OLD.provision_generation,OLD.cluster_id,OLD.policy_digest,
           OLD.owner_key,OLD.project_id,OLD.priority,OLD.request_digest,OLD.guest_vcpus,
           OLD.guest_memory_bytes,OLD.cpu_millicores,OLD.memory_bytes,OLD.kvm_devices,OLD.placement,OLD.enqueued_at)
       OR NEW.revision < OLD.revision OR NEW.bypasses < OLD.bypasses
       OR (OLD.protected_order IS NOT NULL AND NEW.protected_order IS DISTINCT FROM OLD.protected_order) THEN
        RAISE EXCEPTION 'VM resource waiter identity is immutable' USING ERRCODE='23514';
    END IF;
    IF NEW.state <> OLD.state AND NOT (
        (OLD.state='waiting' AND NEW.state IN ('nonfit','admitted','cancelled')) OR
        (OLD.state='nonfit' AND NEW.state IN ('waiting','cancelled')) OR
        (OLD.state='admitted' AND NEW.state='released')
    ) THEN
        RAISE EXCEPTION 'Invalid VM resource waiter transition' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER guard_vm_resource_waiter BEFORE INSERT OR UPDATE ON public.vm_resource_waiters
    FOR EACH ROW EXECUTE FUNCTION public.guard_vm_resource_waiter();

CREATE FUNCTION public.guard_vm_resource_reservation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='DELETE' THEN
        IF OLD.state<>'released' THEN
            RAISE EXCEPTION 'Held VM resource reservation cannot be deleted' USING ERRCODE='23514';
        END IF;
        RETURN OLD;
    END IF;
    IF TG_OP='INSERT' THEN
        -- Scope is covered by the existing composite FK. Digest is immutable
        -- on the retained parent, so no extra parent-table index is necessary.
        IF NOT EXISTS (SELECT 1 FROM public.vm_resource_inventory_snapshots s
            WHERE s.snapshot_id=NEW.snapshot_id AND s.digest=NEW.snapshot_digest) THEN
            RAISE EXCEPTION 'VM resource snapshot identity mismatch' USING ERRCODE='23503';
        END IF;
        IF NOT EXISTS (SELECT 1 FROM public.vm_resource_waiters w
            WHERE w.request_id=NEW.request_id AND w.cpu_millicores=NEW.cpu_millicores
            AND w.memory_bytes=NEW.memory_bytes AND w.kvm_devices=NEW.kvm_devices)
            OR NEW.state<>'reserved' THEN
            RAISE EXCEPTION 'VM resource reservation must preserve initial demand' USING ERRCODE='23514';
        END IF;
        RETURN NEW;
    END IF;
    IF ROW(NEW.id,NEW.request_id,NEW.revision,NEW.cluster_id,NEW.policy_digest,NEW.node_uid,NEW.node_name,
           NEW.cpu_millicores,NEW.memory_bytes,NEW.kvm_devices,NEW.snapshot_id,NEW.snapshot_digest,NEW.created_at)
       IS DISTINCT FROM
       ROW(OLD.id,OLD.request_id,OLD.revision,OLD.cluster_id,OLD.policy_digest,OLD.node_uid,OLD.node_name,
           OLD.cpu_millicores,OLD.memory_bytes,OLD.kvm_devices,OLD.snapshot_id,OLD.snapshot_digest,OLD.created_at)
       OR (OLD.vm_uid IS NOT NULL AND NEW.vm_uid IS DISTINCT FROM OLD.vm_uid)
       OR (OLD.vmi_uid IS NOT NULL AND NEW.vmi_uid IS DISTINCT FROM OLD.vmi_uid)
       OR (OLD.launcher_uid IS NOT NULL AND NEW.launcher_uid IS DISTINCT FROM OLD.launcher_uid)
       OR (OLD.state='released' AND NEW IS DISTINCT FROM OLD) THEN
        RAISE EXCEPTION 'VM resource reservation identity is immutable' USING ERRCODE='23514';
    END IF;
    IF NEW.state <> OLD.state AND NOT (
        (OLD.state='reserved' AND NEW.state IN ('active','teardown','released')) OR
        (OLD.state='active' AND NEW.state IN ('warm','teardown')) OR
        (OLD.state='warm' AND NEW.state IN ('active','teardown')) OR
        (OLD.state='teardown' AND NEW.state='released')
    ) THEN
        RAISE EXCEPTION 'Invalid VM resource reservation transition' USING ERRCODE='23514';
    END IF;
    IF NEW.state='released' AND (
        (NEW.release_evidence->>'kind') IS NULL OR
        NEW.release_evidence->>'kind' NOT IN ('never_vm_issued','exact_compute_absent') OR
        (NEW.release_evidence->>'kind'='never_vm_issued' AND NEW.vm_uid IS NOT NULL) OR
        (OLD.state='reserved' AND NEW.release_evidence->>'kind'<>'never_vm_issued')
    ) THEN
        RAISE EXCEPTION 'VM resource release evidence is required' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER guard_vm_resource_reservation BEFORE INSERT OR UPDATE OR DELETE ON public.vm_resource_reservations
    FOR EACH ROW EXECUTE FUNCTION public.guard_vm_resource_reservation();
COMMIT;
