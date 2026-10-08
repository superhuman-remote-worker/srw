-- migration:     0360_connector_driver_images.sql
-- description:   Driver image resolutions (connector drivers D5, "Driver
--                versions"). Each bind of a service driver resolves the
--                driver's image reference to a digest; a row per
--                (driver, reference, digest) records when, the image's
--                entrypoint and command (the shim runs them in the pod) and
--                its io.srw.driver.spec label, so a moved tag is checked
--                against the stored connector at bind and a tag reference
--                can reuse its last digest while the registry is
--                unreachable. Holds no secret: references, digests and
--                labels are public image metadata.
-- depends-on:    0352_datasources_managed_key_idx.notx.sql
-- expected:      < 1s (one empty table and its index).
-- locks:         catalog locks only.
-- transactional: yes

BEGIN;
SET LOCAL lock_timeout                        = '2s';
SET LOCAL statement_timeout                   = '15min';
SET LOCAL idle_in_transaction_session_timeout = '5min';
SET LOCAL timezone                            = 'UTC';

CREATE TABLE public.connector_driver_images (
    id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    driver             TEXT NOT NULL,
    reference          TEXT NOT NULL,
    digest             TEXT NOT NULL,
    entrypoint         JSONB NOT NULL DEFAULT '[]'::jsonb,
    cmd                JSONB NOT NULL DEFAULT '[]'::jsonb,
    spec               JSONB,
    spec_hash          TEXT,
    protocol_version   TEXT NOT NULL,
    first_resolved_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT connector_driver_images_key
        UNIQUE (driver, reference, digest),
    CONSTRAINT connector_driver_images_driver_check
        CHECK (driver <> ''),
    CONSTRAINT connector_driver_images_reference_check
        CHECK (reference <> '' AND char_length(reference) <= 512),
    CONSTRAINT connector_driver_images_digest_check
        CHECK (digest ~ '^sha256:[0-9a-f]{64}$'),
    CONSTRAINT connector_driver_images_entrypoint_check
        CHECK (jsonb_typeof(entrypoint) = 'array'),
    CONSTRAINT connector_driver_images_cmd_check
        CHECK (jsonb_typeof(cmd) = 'array'),
    CONSTRAINT connector_driver_images_spec_check
        CHECK (spec IS NULL OR jsonb_typeof(spec) = 'object'),
    CONSTRAINT connector_driver_images_protocol_check
        CHECK (protocol_version ~ '^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$')
);

-- The latest resolution of one reference (the stale fallback) and the
-- image of one digest (the pod launch).
CREATE INDEX idx_connector_driver_images_latest
    ON public.connector_driver_images (driver, reference, resolved_at DESC);
CREATE INDEX idx_connector_driver_images_digest
    ON public.connector_driver_images (driver, digest);

COMMENT ON TABLE public.connector_driver_images IS
    'Connector driver image resolutions: one row per (driver, reference, '
    'digest) with the image entrypoint, command and spec label. Written at '
    'bind; read by the service-pod launch and the moved-tag check.';

COMMIT;
