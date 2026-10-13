import { Call, CallEvent } from '../call.model';

/** Synthetic clinic enquiry; no real caller data. */
export function callFixture(overrides: Partial<Call> = {}): Call {
  return {
    id: 'call-1',
    session_id: 'live_demo',
    line_id: 'line-1',
    line_name: 'Empfang',
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

export function callEventFixture(id: number, overrides: Partial<CallEvent> = {}): CallEvent {
  return {
    id,
    call_id: 'call-1',
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
