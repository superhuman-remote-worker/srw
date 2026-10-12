-- migration: 0274_vm_resource_whole_launcher.sql
-- description: Versioned six-resource waiter/reservation and monotonic observed charge.
-- depends-on: 0273_vm_idle_access_wake_lineage.sql
-- transactional: yes
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

-- The old three-field records keep their exact interpretation. NULL is
-- unknown, never zero, when the six-field accounting protocol is selected.
ALTER TABLE public.vm_resource_waiters
    ADD COLUMN resource_version SMALLINT NOT NULL DEFAULT 1,
    ADD COLUMN ephemeral_storage_bytes BIGINT,
    ADD COLUMN tun_devices BIGINT,
    ADD COLUMN vhost_net_devices BIGINT,
    ADD CONSTRAINT vm_resource_waiter_vector_version CHECK (
        (resource_version=1 AND ephemeral_storage_bytes IS NULL AND tun_devices IS NULL AND vhost_net_devices IS NULL)
        OR (resource_version=2 AND ephemeral_storage_bytes IS NOT NULL AND tun_devices IS NOT NULL
            AND vhost_net_devices IS NOT NULL AND ephemeral_storage_bytes>0 AND tun_devices>0 AND vhost_net_devices>0)
    );

ALTER TABLE public.vm_resource_reservations
    ADD COLUMN resource_version SMALLINT NOT NULL DEFAULT 1,
    ADD COLUMN ephemeral_storage_bytes BIGINT,
    ADD COLUMN tun_devices BIGINT,
    ADD COLUMN vhost_net_devices BIGINT,
    ADD COLUMN observed_cpu_millicores BIGINT,
    ADD COLUMN observed_memory_bytes BIGINT,
    ADD COLUMN observed_ephemeral_storage_bytes BIGINT,
    ADD COLUMN observed_kvm_devices BIGINT,
    ADD COLUMN observed_tun_devices BIGINT,
    ADD COLUMN observed_vhost_net_devices BIGINT,
    ADD CONSTRAINT vm_resource_reservation_vector_version CHECK (
        (resource_version=1 AND ephemeral_storage_bytes IS NULL AND tun_devices IS NULL AND vhost_net_devices IS NULL)
        OR (resource_version=2 AND ephemeral_storage_bytes IS NOT NULL AND tun_devices IS NOT NULL
            AND vhost_net_devices IS NOT NULL AND ephemeral_storage_bytes>0 AND tun_devices>0 AND vhost_net_devices>0)
    ),
    ADD CONSTRAINT vm_resource_reservation_observed_complete CHECK (
        (observed_cpu_millicores IS NULL AND observed_memory_bytes IS NULL AND observed_ephemeral_storage_bytes IS NULL
            AND observed_kvm_devices IS NULL AND observed_tun_devices IS NULL AND observed_vhost_net_devices IS NULL)
        OR (resource_version=2 AND observed_cpu_millicores IS NOT NULL AND observed_memory_bytes IS NOT NULL
            AND observed_ephemeral_storage_bytes IS NOT NULL AND observed_kvm_devices IS NOT NULL
            AND observed_tun_devices IS NOT NULL AND observed_vhost_net_devices IS NOT NULL
            AND observed_cpu_millicores>=0 AND observed_memory_bytes>=0
            AND observed_ephemeral_storage_bytes>=0 AND observed_kvm_devices>=0
            AND observed_tun_devices>=0 AND observed_vhost_net_devices>=0)
    );

-- These additive guards leave 0264's identity, state and release rules intact.
CREATE FUNCTION public.guard_vm_resource_waiter_v2() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='INSERT' THEN
        RETURN NEW;
    END IF;
    IF ROW(NEW.resource_version,NEW.ephemeral_storage_bytes,NEW.tun_devices,NEW.vhost_net_devices)
       IS DISTINCT FROM
       ROW(OLD.resource_version,OLD.ephemeral_storage_bytes,OLD.tun_devices,OLD.vhost_net_devices) THEN
        RAISE EXCEPTION 'VM resource waiter six-field identity is immutable' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER guard_vm_resource_waiter_v2 BEFORE INSERT OR UPDATE ON public.vm_resource_waiters
    FOR EACH ROW EXECUTE FUNCTION public.guard_vm_resource_waiter_v2();

CREATE FUNCTION public.guard_vm_resource_reservation_v2() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='DELETE' THEN
        RETURN OLD;
    END IF;
    IF TG_OP='INSERT' THEN
        IF NOT EXISTS (SELECT 1 FROM public.vm_resource_waiters w
            WHERE w.request_id=NEW.request_id AND w.resource_version=NEW.resource_version
            AND w.ephemeral_storage_bytes IS NOT DISTINCT FROM NEW.ephemeral_storage_bytes
            AND w.tun_devices IS NOT DISTINCT FROM NEW.tun_devices
            AND w.vhost_net_devices IS NOT DISTINCT FROM NEW.vhost_net_devices) THEN
            RAISE EXCEPTION 'VM resource reservation six-field demand changed' USING ERRCODE='23514';
        END IF;
        RETURN NEW;
    END IF;
    IF ROW(NEW.resource_version,NEW.ephemeral_storage_bytes,NEW.tun_devices,NEW.vhost_net_devices)
       IS DISTINCT FROM
       ROW(OLD.resource_version,OLD.ephemeral_storage_bytes,OLD.tun_devices,OLD.vhost_net_devices) THEN
        RAISE EXCEPTION 'VM resource reservation six-field identity is immutable' USING ERRCODE='23514';
    END IF;
    IF OLD.observed_cpu_millicores IS NOT NULL AND (
        NEW.observed_cpu_millicores IS NULL OR NEW.observed_cpu_millicores<OLD.observed_cpu_millicores
        OR NEW.observed_memory_bytes<OLD.observed_memory_bytes
        OR NEW.observed_ephemeral_storage_bytes<OLD.observed_ephemeral_storage_bytes
        OR NEW.observed_kvm_devices<OLD.observed_kvm_devices
        OR NEW.observed_tun_devices<OLD.observed_tun_devices
        OR NEW.observed_vhost_net_devices<OLD.observed_vhost_net_devices
    ) THEN
        RAISE EXCEPTION 'VM observed launcher charge cannot decrease' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER guard_vm_resource_reservation_v2 BEFORE INSERT OR UPDATE OR DELETE ON public.vm_resource_reservations
    FOR EACH ROW EXECUTE FUNCTION public.guard_vm_resource_reservation_v2();
COMMIT;
