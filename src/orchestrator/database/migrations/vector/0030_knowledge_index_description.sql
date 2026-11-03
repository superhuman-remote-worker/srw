-- migration:     0030_knowledge_index_description.sql
-- description:   The note's frontmatter `description` on knowledge_index. The
--                agent re-renders a note's whole file on every kb_update and
--                kb_delete, and until now it read the note back from this row,
--                which had no description: an explicit description given to
--                kb_write was replaced by the body's first sentence on the
--                next update. The reindexer now stores the file's value here.
--                NULL means the file carries none, or the row was indexed
--                before this column existed (a full reindex fills those in).
-- depends-on:    0029_citation_source_links.sql
-- expected:      Instant ALTER; nullable column, no default, no rewrite.
-- locks:         Brief ACCESS EXCLUSIVE on knowledge_index for the ALTER.
-- transactional: yes

BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '5min';

ALTER TABLE public.knowledge_index
    ADD COLUMN IF NOT EXISTS description TEXT;

COMMIT;
