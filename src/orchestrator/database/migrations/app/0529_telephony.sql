-- migration: 0529_telephony.sql
-- description: GPT-Live inbound calls, recorder leases and replayable call journal.
-- depends-on: 0528_vm_job_pinned_ready_cancel_retention.sql
-- expected: new empty tables only
-- locks: new-table DDL; brief key-share locks on referenced tables
-- transactional: YES
-- rollback: forward-only; disabling lines stops new call admission.

SET LOCAL lock_timeout = '2s';

CREATE TABLE IF NOT EXISTS telephony_settings (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    name text NOT NULL,
    project_id uuid REFERENCES projects(id) ON DELETE SET NULL,
    openai_project_id text NOT NULL,
    api_base text NOT NULL DEFAULT 'https://api.openai.com/v1',
    ws_base text NOT NULL DEFAULT 'wss://api.openai.com/v1',
    extra_headers jsonb NOT NULL DEFAULT '{}',
    webhook_secret_encrypted text NOT NULL,
    api_key_encrypted text,
    enabled boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS telephony_lines (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    settings_id uuid NOT NULL REFERENCES telephony_settings(id),
    project_id uuid NOT NULL REFERENCES projects(id),
    owner_user_id uuid NOT NULL REFERENCES users(id),
    name text NOT NULL,
    number text,
    is_default boolean NOT NULL DEFAULT false,
    enabled boolean NOT NULL DEFAULT true,
    engine text NOT NULL DEFAULT 'gpt_live' CHECK (engine = 'gpt_live'),
    model text NOT NULL DEFAULT 'gpt-live-1',
    voice text NOT NULL DEFAULT 'marin',
    expert text NOT NULL DEFAULT 'phone-desk',
    persona text NOT NULL DEFAULT '',
    greeting text NOT NULL,
    fact_sheet text NOT NULL DEFAULT '',
    transfer_target text,
    max_duration_s integer NOT NULL DEFAULT 900 CHECK (max_duration_s > 0),
    answer_timeout_s integer NOT NULL DEFAULT 25 CHECK (answer_timeout_s > 0),
    no_job_fallback_s integer NOT NULL DEFAULT 8 CHECK (no_job_fallback_s > 0),
    goodbye text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS calls (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id text UNIQUE NOT NULL,
    webhook_id text UNIQUE,
    settings_id uuid NOT NULL REFERENCES telephony_settings(id),
    line_id uuid REFERENCES telephony_lines(id) ON DELETE SET NULL,
    project_id uuid REFERENCES projects(id) ON DELETE SET NULL,
    job_id uuid REFERENCES jobs(id) ON DELETE SET NULL,
    direction text NOT NULL CHECK (direction IN ('inbound', 'outbound')),
    status text NOT NULL CHECK (status IN ('ringing', 'active', 'ended', 'rejected', 'failed')),
    from_number text,
    to_number text,
    sip_headers jsonb NOT NULL DEFAULT '[]',
    created_at timestamptz NOT NULL DEFAULT now(),
    accepted_at timestamptz,
    attached_at timestamptz,
    ended_at timestamptz,
    close_reason text,
    usage_seconds numeric,
    error text,
    recorder_owner text,
    recorder_lease_until timestamptz,
    metadata jsonb NOT NULL DEFAULT '{}'
);

-- Keep the prescribed bigserial cursor contract shared with the actor and cockpit.
CREATE TABLE IF NOT EXISTS call_events (
    -- squawk-ignore prefer-identity
    id bigserial PRIMARY KEY,
    call_id uuid NOT NULL REFERENCES calls(id) ON DELETE CASCADE,
    event_id text,
    kind text NOT NULL CHECK (kind IN (
        'transcript', 'delegation', 'answer', 'instruction', 'thinking', 'fallback',
        'question', 'follow_up', 'transfer', 'hangup', 'dtmf', 'usage', 'closed', 'error', 'system'
    )),
    speaker text,
    text text,
    start_ms integer,
    end_ms integer,
    delegation_id text,
    payload jsonb NOT NULL DEFAULT '{}',
    created_at timestamptz NOT NULL DEFAULT now()
);
