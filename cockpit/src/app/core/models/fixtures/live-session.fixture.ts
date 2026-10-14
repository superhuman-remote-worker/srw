import { LiveSession, LiveSessionEvent } from '../live-session.model';

/** Synthetic clinic enquiry; no real caller data. */
export function liveSessionFixture(overrides: Partial<LiveSession> = {}): LiveSession {
  return {
    id: 'session-1',
    connector_id: 'connector-1',
    provider_session_id: 'live_demo',
    family: 'gpt-live',
    transport: 'sip',
    origin: 'phone',
    automation_id: 'automation-1',
    project_id: null,
    model: 'gpt-live-1',
    error: null,
    direction: 'inbound',
    status: 'active',
    from_number: '+4917600000000',
    to_number: '+496052000000',
    created_at: '2026-10-12T08:00:00Z',
    accepted_at: '2026-10-12T08:00:05Z',
    attached_at: '2026-10-12T08:00:06Z',
    ended_at: null,
    close_reason: null,
    usage_seconds: 42,
    job_id: 'job-1',
    ...overrides,
  };
}

export function liveSessionEventFixture(id: number, overrides: Partial<LiveSessionEvent> = {}): LiveSessionEvent {
  return {
    id,
    event_id: `event-${id}`,
    kind: 'transcript',
    speaker: 'caller',
    text: 'Haben Sie ein Schwimmbad?',
    start_ms: 1200,
    end_ms: 2300,
    delegation_id: null,
    payload: {},
    created_at: '2026-10-12T08:00:07Z',
    ...overrides,
  };
}
