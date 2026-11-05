-- migration:     0563_mcp_oauth_grants.sql
-- description:   One row per OAuth connection of the MCP server (claude.ai,
--                ChatGPT, Claude Code …): the refresh token that renews the
--                connection's access token. The access token stays an
--                ordinary kind='mcp' auth_tokens row (access_token_id), rotated
--                in place on every refresh, so a connection is still one row
--                in the token list and revoking it there ends the connection.
--                The refresh token is stored as a SHA-256 hash and is never an
--                access token: it lives in this table, not in auth_tokens.
--                previous_refresh_hash catches a replayed (already rotated)
--                refresh token, which revokes the grant (RFC 9700 §4.14.2).
--                expires_at is the lifetime the user chose on the consent
--                page and caps every access token minted from the grant.
--
--                Also revokes the auth_tokens rows the bridge used to mint as
--                "refresh tokens" (origin 'oauth:<client>:refresh'): 365-day
--                access tokens no client could refresh with, valid on MCP and
--                REST alike (G6).
-- depends-on:    0010_auth_tokens_consolidation.sql
-- expected:      < 1s (new empty table; the UPDATE touches the few OAuth rows).
-- locks:         catalog locks; ROW EXCLUSIVE on auth_tokens for the UPDATE,
--                SHARE ROW EXCLUSIVE on auth_tokens/users for the foreign keys.
-- transactional: yes

BEGIN;
SET LOCAL lock_timeout                        = '2s';
SET LOCAL statement_timeout                   = '30s';
SET LOCAL idle_in_transaction_session_timeout = '5min';

CREATE TABLE public.mcp_oauth_grants (
    id                    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id               UUID NOT NULL REFERENCES public.users (id) ON DELETE CASCADE,
    access_token_id       UUID NOT NULL
        REFERENCES public.auth_tokens (id) ON DELETE CASCADE,
    oauth_client_id       TEXT NOT NULL,
    refresh_token_hash    TEXT NOT NULL,
    previous_refresh_hash TEXT,
    scope                 TEXT NOT NULL,
    expires_at            TIMESTAMPTZ,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_refreshed_at     TIMESTAMPTZ,
    revoked_at            TIMESTAMPTZ,
    CONSTRAINT mcp_oauth_grants_refresh_hash_key UNIQUE (refresh_token_hash),
    CONSTRAINT mcp_oauth_grants_client_check CHECK (oauth_client_id <> '')
);

CREATE INDEX mcp_oauth_grants_previous_hash_idx
    ON public.mcp_oauth_grants (previous_refresh_hash)
    WHERE previous_refresh_hash IS NOT NULL;

CREATE INDEX mcp_oauth_grants_access_token_idx
    ON public.mcp_oauth_grants (access_token_id);

COMMENT ON TABLE public.mcp_oauth_grants IS
    'OAuth connections of the MCP server: the hashed refresh token that '
    'renews access_token_id (an auth_tokens kind=mcp row rotated in place). '
    'A refresh token is never an access token. Replaying a rotated refresh '
    'token (previous_refresh_hash) revokes the grant.';

-- The bridge's old "refresh tokens": ordinary 365-day access tokens.
UPDATE public.auth_tokens
   SET revoked_at = now()
 WHERE kind = 'mcp'
   AND origin LIKE 'oauth:%:refresh'
   AND revoked_at IS NULL;

COMMIT;
