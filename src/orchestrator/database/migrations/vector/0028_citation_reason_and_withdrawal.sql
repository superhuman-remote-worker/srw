-- migration:     0028_citation_reason_and_withdrawal.sql
-- description:   Why a citation is not verified, and the agent's withdrawal of
--                one (citation engine overhaul WP1, D7: flag, never delete).
--                verification_reason is a short code the UI and the agent's
--                feedback read (quote_not_found, not_supported,
--                source_unavailable, not_checked); verification_notes keeps the
--                prose. withdrawn_at / withdrawn_reason mark a citation the
--                agent stopped using; the row and its verdict stay. All
--                nullable; no backfill.
-- depends-on:    0027_memory_search_includes_pinned_rows.sql
-- expected:      Instant ALTER; no rewrite.

ALTER TABLE citations
    ADD COLUMN IF NOT EXISTS verification_reason TEXT,
    ADD COLUMN IF NOT EXISTS withdrawn_at        TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS withdrawn_reason    TEXT;
