-- migration:     0249_stateless_conversation_rewind.sql
-- description:   Add conversation revisions, durable stateless-rewind
--                receipts, and input-delivery revision provenance.
-- depends-on:    0248_retire_unkeyed_credential_provenance.sql
-- transactional: yes
-- expected:      < 5s; additive columns, constraints, and one partial index.

BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';
SET LOCAL idle_in_transaction_session_timeout = '1min';

ALTER TABLE public.threads
    ADD COLUMN conversation_revision bigint NOT NULL DEFAULT 0,
    ADD CONSTRAINT threads_conversation_revision_nonnegative
        CHECK (conversation_revision >= 0);

COMMENT ON COLUMN public.threads.conversation_revision IS
    'Monotonic transcript-view revision. Incremented only by an applied conversation rewind; clients fence delayed human input and cached history against it.';

ALTER TABLE public.thread_input_deliveries
    ADD COLUMN conversation_revision bigint,
    ADD CONSTRAINT thread_input_deliveries_conversation_revision_nonnegative
        CHECK (conversation_revision IS NULL OR conversation_revision >= 0);

COMMENT ON COLUMN public.thread_input_deliveries.conversation_revision IS
    'Conversation revision captured when this durable input identity was first admitted. NULL identifies pre-0249 history.';

ALTER TABLE public.thread_rewinds
    ADD COLUMN client_request_id uuid,
    ADD COLUMN request_payload jsonb,
    ADD COLUMN result_payload jsonb,
    ADD COLUMN runtime_generation uuid,
    ADD COLUMN before_conversation_revision bigint,
    ADD COLUMN after_conversation_revision bigint,
    ADD COLUMN event_epoch integer,
    ADD COLUMN event_seq bigint,
    ADD CONSTRAINT thread_rewinds_stateless_receipt_shape CHECK (
        (
            client_request_id IS NULL
            AND request_payload IS NULL
            AND result_payload IS NULL
            AND runtime_generation IS NULL
            AND before_conversation_revision IS NULL
            AND after_conversation_revision IS NULL
            AND event_epoch IS NULL
            AND event_seq IS NULL
        )
        OR
        (
            client_request_id IS NOT NULL
            AND request_payload IS NOT NULL
            AND jsonb_typeof(request_payload) = 'object'
            AND result_payload IS NOT NULL
            AND jsonb_typeof(result_payload) = 'object'
            AND runtime_generation IS NOT NULL
            AND before_conversation_revision IS NOT NULL
            AND before_conversation_revision >= 0
            AND after_conversation_revision = before_conversation_revision + 1
            AND event_epoch IS NOT NULL
            AND event_epoch >= 0
            AND event_seq IS NOT NULL
            AND event_seq > 0
        )
    );

CREATE UNIQUE INDEX idx_thread_rewinds_client_request
    ON public.thread_rewinds (thread_id, client_request_id)
    WHERE client_request_id IS NOT NULL;

COMMENT ON COLUMN public.thread_rewinds.client_request_id IS
    'Browser-generated idempotency key for synchronous stateless conversation rewind.';
COMMENT ON COLUMN public.thread_rewinds.request_payload IS
    'Canonical immutable POST body used to detect idempotency-key conflicts.';
COMMENT ON COLUMN public.thread_rewinds.result_payload IS
    'Committed replayable result. Written in the same transaction as the transcript effect.';

COMMIT;
