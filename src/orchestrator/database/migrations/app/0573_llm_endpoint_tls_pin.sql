-- migration:     0573_llm_endpoint_tls_pin.sql
-- description:   llm_endpoints can store one pinned TLS certificate per
--                endpoint (per-endpoint certificate pinning): the leaf PEM,
--                its SHA-256 fingerprint, subject, issuer, expiry, and who
--                pinned it and when. All columns are nullable with no
--                default; an unpinned endpoint is the normal case. The pin
--                belongs to the row, not its address: no write path clears
--                it on a base_url edit. The two CHECKs are NOT VALID so no
--                scan runs under the lock — 0574 validates.
-- depends-on:    0572_validate_image_generation_capability.sql
-- expected:      < 1s. Nullable ADD COLUMN without a default is a catalog
--                change; no table rewrite.
-- locks:         ACCESS EXCLUSIVE on llm_endpoints for the column adds and
--                constraint adds (tens of rows, admin writes only).
-- transactional: yes

BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';
SET LOCAL idle_in_transaction_session_timeout = '1min';

ALTER TABLE public.llm_endpoints
    ADD COLUMN IF NOT EXISTS tls_pin_sha256 TEXT,
    ADD COLUMN IF NOT EXISTS tls_pin_cert TEXT,
    ADD COLUMN IF NOT EXISTS tls_pin_subject TEXT,
    ADD COLUMN IF NOT EXISTS tls_pin_issuer TEXT,
    ADD COLUMN IF NOT EXISTS tls_pin_not_after TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS tls_pinned_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS tls_pinned_by UUID;

ALTER TABLE public.llm_endpoints
    ADD CONSTRAINT llm_endpoints_tls_pin_sha256_format CHECK (
        tls_pin_sha256 IS NULL
        OR tls_pin_sha256 ~ '^([0-9A-F]{2}:){31}[0-9A-F]{2}$'
    ) NOT VALID,
    ADD CONSTRAINT llm_endpoints_tls_pin_pair CHECK (
        (tls_pin_sha256 IS NULL) = (tls_pin_cert IS NULL)
    ) NOT VALID;

COMMENT ON COLUMN public.llm_endpoints.tls_pin_sha256 IS
    'SHA-256 fingerprint (upper-case, colon-separated) of the pinned leaf certificate; NULL = not pinned.';
COMMENT ON COLUMN public.llm_endpoints.tls_pin_cert IS
    'The pinned leaf certificate, PEM. Set and cleared together with tls_pin_sha256.';
COMMENT ON COLUMN public.llm_endpoints.tls_pin_subject IS
    'Subject of the pinned certificate, as the admin saw it when trusting it.';
COMMENT ON COLUMN public.llm_endpoints.tls_pin_issuer IS
    'Issuer of the pinned certificate.';
COMMENT ON COLUMN public.llm_endpoints.tls_pin_not_after IS
    'Expiry of the pinned certificate; drives the expired warning.';
COMMENT ON COLUMN public.llm_endpoints.tls_pinned_at IS
    'When the admin trusted the pinned certificate.';
COMMENT ON COLUMN public.llm_endpoints.tls_pinned_by IS
    'The admin who trusted it. No FK, like security_events.user_id.';

COMMIT;
