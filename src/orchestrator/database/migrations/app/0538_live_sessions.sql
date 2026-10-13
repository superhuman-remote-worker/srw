-- migration:     0538_live_sessions.sql
-- description:   Live-session jobs: one row per live session (a phone call
--                today) and its replayable journal. The connector's service
--                pod holds the session; the orchestrator admits it, binds it
--                to one job and keeps this record.
-- depends-on:    0537_validate_live_voice_capability.sql
-- expected:      new empty tables only
-- locks:         new-table DDL; brief key-share locks on referenced tables
-- transactional: yes
-- rollback:      forward-only; the tables are empty until a live connector
--                receives a session.

BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';
SET LOCAL idle_in_transaction_session_timeout = '1min';

CREATE TABLE IF NOT EXISTS live_sessions (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    -- The live connector (phone line) the session arrived on. Deleting the
    -- connector keeps the session's record with its job.
    connector_id uuid REFERENCES datasources(id) ON DELETE SET NULL,
    provider_session_id text NOT NULL,
    webhook_id text UNIQUE,
    family text NOT NULL DEFAULT 'voice' CHECK (family IN ('voice', 'messaging')),
    transport text NOT NULL DEFAULT 'sip',
    direction text NOT NULL DEFAULT 'inbound'
        CHECK (direction IN ('inbound', 'outbound')),
    origin text NOT NULL CHECK (origin IN ('webhook', 'api', 'manifest')),
    status text NOT NULL
        CHECK (status IN ('ringing', 'active', 'ended', 'rejected', 'failed')),
    automation_id uuid REFERENCES automations(id) ON DELETE SET NULL,
    -- One session, one job: admission binds with WHERE job_id IS NULL.
    job_id uuid REFERENCES jobs(id) ON DELETE SET NULL,
    project_id uuid REFERENCES projects(id) ON DELETE SET NULL,
    model text,
    -- What the pod needs to adopt the session after a restart (no secrets).
    handoff jsonb NOT NULL DEFAULT '{}',
    -- from_number, to_number, sip_headers and other transport details.
    metadata jsonb NOT NULL DEFAULT '{}',
    created_at timestamptz NOT NULL DEFAULT now(),
    accepted_at timestamptz,
    attached_at timestamptz,
    ended_at timestamptz,
    close_reason text,
    error text,
    usage_seconds numeric,
    -- The pod identity holding the session, and until when.
    owner_identity_id uuid,
    lease_until timestamptz,
    UNIQUE (connector_id, provider_session_id)
);

COMMENT ON TABLE live_sessions IS
    'A live session (a phone call) a live connector received, and the one job '
    'it is bound to. Status flow ringing -> active -> ended, or ringing -> '
    'rejected | failed; every update is monotonic.';

-- Keep the bigserial cursor contract shared with the agent and the cockpit.
CREATE TABLE IF NOT EXISTS live_session_events (
    -- squawk-ignore prefer-identity
    id bigserial PRIMARY KEY,
    session_id uuid NOT NULL REFERENCES live_sessions(id) ON DELETE CASCADE,
    event_id text,
    kind text NOT NULL CHECK (kind IN (
        'transcript', 'delegation', 'answer', 'instruction', 'thinking',
        'fallback', 'question', 'follow_up', 'transfer', 'hangup', 'dtmf',
        'closed', 'error', 'system'
    )),
    speaker text,
    text text,
    start_ms integer,
    end_ms integer,
    delegation_id text,
    payload jsonb NOT NULL DEFAULT '{}',
    created_at timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE live_session_events IS
    'The journal of a live session: transcript, delegations, the job''s '
    'answers and steering, and the pod''s own appends. event_id dedupes '
    'replays.';

COMMIT;
