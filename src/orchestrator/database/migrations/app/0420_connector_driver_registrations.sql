-- migration:     0420_connector_driver_registrations.sql
-- description:   Registered image drivers and their bind-time bindings
--                (connector drivers D6, "Trust and registration", "Driver
--                versions", "Three planes"). Four new tables, none touching
--                an existing one:
--                * connector_driver_registrations: a driver image someone
--                  registered in their Account, a Project or the shared
--                  Catalog, with the spec its io.srw.driver.spec label (or
--                  its spec operation, or an imported server.json) declared
--                  and the digest its reference resolved to then. One name
--                  per scope; srw.* stays SRW's own;
--                * connector_driver_assignments: which registration a
--                  connector runs (a connector pins it by id when it is
--                  created, so a registration added later under the same
--                  name never moves it);
--                * connector_bind_time_bindings: one per workspace-owning
--                  execution and connector of a bind-time image driver. It
--                  records the bind ({reference, digest, resolved_at,
--                  spec_hash, protocol_version}), the delivery the driver
--                  returned and its driver_state (APP_ENCRYPTION_KEY
--                  ciphertexts), or why the bind was refused, and its
--                  revocation. Owners and connectors carry no foreign key:
--                  a revoke still runs after the job, thread or connector
--                  is gone;
--                * connector_driver_operations: one short-lived driver pod
--                  per operation (spec, check, bind, revoke, gc). It holds
--                  the pod's sdi_ identity (SHA-256 only), the outcome the
--                  shim posted (encrypted) and the pod's lifecycle, so the
--                  installation cap counts live pods and a sweep removes
--                  what a restart left behind.
-- depends-on:    0392_drop_connector_service_pod_key_idx.notx.sql
-- expected:      < 1s (four empty tables and their indexes).
-- locks:         catalog locks; SHARE ROW EXCLUSIVE on users, projects and
--                datasources for the foreign keys.
-- transactional: yes

BEGIN;
SET LOCAL lock_timeout                        = '2s';
SET LOCAL statement_timeout                   = '15min';
SET LOCAL idle_in_transaction_session_timeout = '5min';
SET LOCAL timezone                            = 'UTC';

CREATE TABLE public.connector_driver_registrations (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name              TEXT NOT NULL,
    scope_kind        TEXT NOT NULL,
    owner_id          UUID REFERENCES public.users(id) ON DELETE CASCADE,
    project_id        UUID REFERENCES public.projects(id) ON DELETE CASCADE,
    title             TEXT NOT NULL,
    description       TEXT,
    image_reference   TEXT NOT NULL,
    image_digest      TEXT NOT NULL,
    spec              JSONB NOT NULL,
    spec_hash         TEXT NOT NULL,
    spec_source       TEXT NOT NULL,
    protocol_version  TEXT NOT NULL,
    plane             TEXT NOT NULL,
    source_document   JSONB,
    created_by        UUID REFERENCES public.users(id) ON DELETE SET NULL,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT connector_driver_registrations_name_check
        CHECK (name ~ '^[a-z][a-z0-9-]*(\.[a-z][a-z0-9-]*)+/v[1-9][0-9]*$'
               AND name NOT LIKE 'srw.%'),
    CONSTRAINT connector_driver_registrations_scope_check
        CHECK (
            (scope_kind = 'Account' AND owner_id IS NOT NULL AND project_id IS NULL)
            OR (scope_kind = 'Project' AND project_id IS NOT NULL AND owner_id IS NULL)
            OR (scope_kind = 'Catalog' AND owner_id IS NULL AND project_id IS NULL)
        ),
    CONSTRAINT connector_driver_registrations_title_check
        CHECK (title <> '' AND char_length(title) <= 200),
    CONSTRAINT connector_driver_registrations_reference_check
        CHECK (image_reference <> '' AND char_length(image_reference) <= 512),
    CONSTRAINT connector_driver_registrations_digest_check
        CHECK (image_digest ~ '^sha256:[0-9a-f]{64}$'),
    CONSTRAINT connector_driver_registrations_spec_check
        CHECK (jsonb_typeof(spec) = 'object'),
    CONSTRAINT connector_driver_registrations_spec_hash_check
        CHECK (spec_hash ~ '^sha256:[0-9a-f]{64}$'),
    CONSTRAINT connector_driver_registrations_spec_source_check
        CHECK (spec_source IN ('label', 'spec_operation', 'server_json')),
    CONSTRAINT connector_driver_registrations_protocol_check
        CHECK (protocol_version ~ '^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$'),
    CONSTRAINT connector_driver_registrations_plane_check
        CHECK (plane IN ('bind_time', 'service', 'in_pod')),
    CONSTRAINT connector_driver_registrations_source_check
        CHECK (source_document IS NULL OR jsonb_typeof(source_document) = 'object')
);

-- One name per scope.
CREATE UNIQUE INDEX uq_connector_driver_registrations_catalog
    ON public.connector_driver_registrations (name)
    WHERE scope_kind = 'Catalog';
CREATE UNIQUE INDEX uq_connector_driver_registrations_account
    ON public.connector_driver_registrations (owner_id, name)
    WHERE scope_kind = 'Account';
CREATE UNIQUE INDEX uq_connector_driver_registrations_project
    ON public.connector_driver_registrations (project_id, name)
    WHERE scope_kind = 'Project';

COMMENT ON TABLE public.connector_driver_registrations IS
    'Registered connector driver images (D6): one name per Account, Project '
    'or the shared Catalog, the spec the image declared and the digest its '
    'reference resolved to at registration. srw.* names are SRW''s own.';

CREATE TABLE public.connector_driver_assignments (
    connector_id     UUID PRIMARY KEY
                         REFERENCES public.datasources(id) ON DELETE CASCADE,
    registration_id  UUID NOT NULL
                         REFERENCES public.connector_driver_registrations(id)
                         ON DELETE CASCADE,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_connector_driver_assignments_registration
    ON public.connector_driver_assignments (registration_id);

COMMENT ON TABLE public.connector_driver_assignments IS
    'The registration a connector of a registered image driver runs, pinned '
    'by id when the connector is created. The API refuses to delete a '
    'registration a connector uses; a user or project delete cascades, and '
    'the connector then refuses to bind.';

CREATE TABLE public.connector_bind_time_bindings (
    id                         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    owner_kind                 TEXT NOT NULL,
    owner_id                   UUID NOT NULL,
    connector_id               UUID NOT NULL,
    registration_id            UUID
                                   REFERENCES public.connector_driver_registrations(id)
                                   ON DELETE SET NULL,
    driver                     TEXT NOT NULL,
    status                     TEXT NOT NULL DEFAULT 'pending',
    image_reference            TEXT,
    image_digest               TEXT,
    resolved_at                TIMESTAMPTZ,
    spec_hash                  TEXT,
    protocol_version           TEXT,
    access                     TEXT,
    delivery_ciphertext        TEXT,
    driver_state_ciphertext    TEXT,
    error_class                TEXT,
    error_message              TEXT,
    created_at                 TIMESTAMPTZ NOT NULL DEFAULT now(),
    bound_at                   TIMESTAMPTZ,
    failed_at                  TIMESTAMPTZ,
    revoke_requested_at        TIMESTAMPTZ,
    revoke_reason              TEXT,
    revoked_at                 TIMESTAMPTZ,
    revoke_error               TEXT,
    CONSTRAINT connector_bind_time_bindings_owner_check
        CHECK (owner_kind IN ('job', 'thread')),
    CONSTRAINT connector_bind_time_bindings_driver_check
        CHECK (driver <> ''),
    CONSTRAINT connector_bind_time_bindings_status_check
        CHECK (status IN ('pending', 'bound', 'failed', 'revoking', 'revoked')),
    CONSTRAINT connector_bind_time_bindings_digest_check
        CHECK (image_digest IS NULL OR image_digest ~ '^sha256:[0-9a-f]{64}$'),
    CONSTRAINT connector_bind_time_bindings_bound_check
        CHECK (status NOT IN ('bound', 'revoking')
               OR (delivery_ciphertext IS NOT NULL AND image_digest IS NOT NULL
                   AND bound_at IS NOT NULL)),
    CONSTRAINT connector_bind_time_bindings_failed_check
        CHECK ((status = 'failed') = (failed_at IS NOT NULL)
               AND (status <> 'failed' OR error_message IS NOT NULL)),
    CONSTRAINT connector_bind_time_bindings_revoke_check
        CHECK ((status IN ('revoking', 'revoked')) = (revoke_requested_at IS NOT NULL)
               AND (status = 'revoked') = (revoked_at IS NOT NULL))
);

-- One live binding per execution and connector: a second bind of the same
-- pair waits for the first instead of starting another pod.
CREATE UNIQUE INDEX uq_connector_bind_time_bindings_live
    ON public.connector_bind_time_bindings (owner_kind, owner_id, connector_id)
    WHERE status IN ('pending', 'bound');
-- The connector page (each execution's digest) and the moved-tag check (the
-- digest of the connector's newest binding).
CREATE INDEX idx_connector_bind_time_bindings_connector
    ON public.connector_bind_time_bindings (connector_id, created_at DESC);
-- The reconciler's scan: bindings to revoke, and bound ones whose execution
-- may have ended.
CREATE INDEX idx_connector_bind_time_bindings_open
    ON public.connector_bind_time_bindings (status)
    WHERE status IN ('pending', 'bound', 'revoking');

COMMENT ON TABLE public.connector_bind_time_bindings IS
    'Bind-time image driver bindings (D6): what one bind recorded and '
    'delivered (encrypted), and its revocation. Owner and connector are '
    'plain ids so a revoke can run after either is gone.';

CREATE TABLE public.connector_driver_operations (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    token_hash          BYTEA NOT NULL UNIQUE,
    token_last_four     TEXT NOT NULL,
    operation           TEXT NOT NULL,
    registration_id     UUID
                            REFERENCES public.connector_driver_registrations(id)
                            ON DELETE SET NULL,
    connector_id        UUID,
    binding_id          UUID
                            REFERENCES public.connector_bind_time_bindings(id)
                            ON DELETE SET NULL,
    image_reference     TEXT NOT NULL,
    image_digest        TEXT NOT NULL,
    pod_namespace       TEXT NOT NULL,
    pod_name            TEXT NOT NULL,
    status              TEXT NOT NULL DEFAULT 'running',
    deadline_at         TIMESTAMPTZ NOT NULL,
    exit_code           INTEGER,
    outcome_ciphertext  TEXT,
    error               TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at         TIMESTAMPTZ,
    removed_at          TIMESTAMPTZ,
    CONSTRAINT connector_driver_operations_hash_check
        CHECK (octet_length(token_hash) = 32),
    CONSTRAINT connector_driver_operations_last_four_check
        CHECK (char_length(token_last_four) = 4),
    CONSTRAINT connector_driver_operations_operation_check
        CHECK (operation IN ('spec', 'check', 'bind', 'revoke', 'gc')),
    CONSTRAINT connector_driver_operations_digest_check
        CHECK (image_digest ~ '^sha256:[0-9a-f]{64}$'),
    CONSTRAINT connector_driver_operations_status_check
        CHECK (status IN ('running', 'finished', 'failed')),
    CONSTRAINT connector_driver_operations_finished_check
        CHECK ((status = 'running') = (finished_at IS NULL))
);

-- The installation cap counts pods whose objects were not seen gone; the
-- sweep removes them.
CREATE INDEX idx_connector_driver_operations_live
    ON public.connector_driver_operations (created_at)
    WHERE removed_at IS NULL;
CREATE INDEX idx_connector_driver_operations_binding
    ON public.connector_driver_operations (binding_id)
    WHERE binding_id IS NOT NULL;

COMMENT ON TABLE public.connector_driver_operations IS
    'Short-lived connector driver pods (D6), one per operation: the pod''s '
    'sdi_ identity (SHA-256 only), the outcome its shim posted (encrypted) '
    'and its lifecycle.';

COMMIT;
