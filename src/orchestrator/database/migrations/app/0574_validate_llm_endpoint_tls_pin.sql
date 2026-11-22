-- migration:     0574_validate_llm_endpoint_tls_pin.sql
-- description:   Validate the two llm_endpoints TLS pin CHECK constraints
--                0573 added NOT VALID.
-- depends-on:    0573_llm_endpoint_tls_pin.sql
-- expected:      < 1s. Scans the admin-curated llm_endpoints table (tens of
--                rows); every existing row has all pin columns NULL.
-- locks:         SHARE UPDATE EXCLUSIVE on llm_endpoints; no writes are
--                blocked.
-- transactional: yes

BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';
SET LOCAL idle_in_transaction_session_timeout = '1min';

ALTER TABLE public.llm_endpoints VALIDATE CONSTRAINT llm_endpoints_tls_pin_sha256_format;
ALTER TABLE public.llm_endpoints VALIDATE CONSTRAINT llm_endpoints_tls_pin_pair;

COMMIT;
