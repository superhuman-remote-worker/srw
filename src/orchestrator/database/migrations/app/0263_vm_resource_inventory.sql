-- migration: 0263_vm_resource_inventory.sql
-- description: Persist bounded inventory observations without granting VM capacity.
-- depends-on: 0262_srw_execution_deadline_scan.sql
-- transactional: yes
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

CREATE TABLE public.vm_resource_inventory_heads (
    cluster_id TEXT NOT NULL CHECK (length(cluster_id) BETWEEN 1 AND 253),
    policy_digest TEXT NOT NULL CHECK (policy_digest ~ '^sha256:[0-9a-f]{64}$'),
    namespace TEXT NOT NULL CHECK (length(namespace) BETWEEN 1 AND 253),
    label_keys JSONB NOT NULL CHECK (jsonb_typeof(label_keys) = 'array'),
    current_snapshot_id UUID,
    observed_high_water TIMESTAMPTZ,
    observation_conflict BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (cluster_id, policy_digest),
    CHECK ((current_snapshot_id IS NULL) = (observed_high_water IS NULL))
);

CREATE TABLE public.vm_resource_inventory_snapshots (
    snapshot_id UUID PRIMARY KEY,
    cluster_id TEXT NOT NULL,
    policy_digest TEXT NOT NULL,
    controller_id UUID NOT NULL,
    sequence BIGINT NOT NULL CHECK (sequence > 0),
    digest TEXT NOT NULL CHECK (digest ~ '^sha256:[0-9a-f]{64}$'),
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    complete BOOLEAN NOT NULL,
    document JSONB NOT NULL CHECK (jsonb_typeof(document) = 'object'),
    UNIQUE (snapshot_id, cluster_id, policy_digest),
    FOREIGN KEY (cluster_id, policy_digest)
        REFERENCES public.vm_resource_inventory_heads(cluster_id, policy_digest),
    CHECK (started_at <= finished_at AND finished_at <= received_at)
);
ALTER TABLE public.vm_resource_inventory_heads
    ADD CONSTRAINT vm_resource_inventory_current_fkey
    FOREIGN KEY (current_snapshot_id, cluster_id, policy_digest)
    REFERENCES public.vm_resource_inventory_snapshots(snapshot_id, cluster_id, policy_digest);
CREATE INDEX vm_resource_inventory_history_idx
    ON public.vm_resource_inventory_snapshots(cluster_id, policy_digest, started_at DESC);

CREATE FUNCTION public.vm_resource_inventory_immutable() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW IS DISTINCT FROM OLD THEN
        RAISE EXCEPTION 'Resource inventory observations are immutable' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER vm_resource_inventory_immutable
    BEFORE UPDATE ON public.vm_resource_inventory_snapshots
    FOR EACH ROW EXECUTE FUNCTION public.vm_resource_inventory_immutable();
COMMIT;
