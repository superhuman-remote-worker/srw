-- migration:     0568_thread_permission_request_kind.sql
-- description:   A permission request is a tool approval or an MCP server's question.
-- depends-on:    0567_drop_expert_phase_prompts.sql
-- expected:      < 5s. Three metadata-only column adds (a constant default) and a
--                NOT VALID check; no rewrite, no backfill.
-- transactional: yes

-- An MCP server may ask the user a question while a tool call runs
-- (elicitation; knowledge-base/knowledge/features/mcp_client_2026_07_28_elicitation.md,
-- owner decision D3 of 2026-11-17). The question waits in this table, beside the
-- tool approvals, so every reader that asks "is this Session waiting on a person?"
-- sees it. Readers that treat a row as a tool's own approval filter on
-- kind = 'tool'. The statuses keep their meaning: approved = accept,
-- denied = decline, expired = cancel or no answer.
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '15min';

ALTER TABLE public.thread_permission_requests
    ADD COLUMN IF NOT EXISTS kind TEXT NOT NULL DEFAULT 'tool';

ALTER TABLE public.thread_permission_requests
    ADD COLUMN IF NOT EXISTS request JSONB;

ALTER TABLE public.thread_permission_requests
    ADD COLUMN IF NOT EXISTS response JSONB;

ALTER TABLE public.thread_permission_requests
    ADD CONSTRAINT thread_permission_requests_kind_check
    CHECK (kind IN ('tool', 'question'))
    NOT VALID;

COMMENT ON COLUMN public.thread_permission_requests.kind IS
    'tool: approve a tool call before it runs. question: an MCP server''s '
    'question during a running call (elicitation).';
COMMENT ON COLUMN public.thread_permission_requests.request IS
    'A question: {server, server_slug, datasource_id, tool, mode, message, '
    'schema, url, confirmation}. NULL for a tool approval.';
COMMENT ON COLUMN public.thread_permission_requests.response IS
    'A decided question: {content, always_accept}. The content goes to the '
    'server; it is never shown to the model.';

COMMIT;
