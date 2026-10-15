/** Live-session wire contracts. Journal ids are ordered within a session. */
export interface LiveSession {
  id: string;
  connector_id: string;
  provider_session_id: string;
  family: string;
  transport: string;
  direction: 'inbound' | 'outbound';
  origin: string;
  status: 'ringing' | 'active' | 'ended' | 'rejected' | 'failed';
  automation_id: string | null;
  job_id: string;
  project_id: string | null;
  model: string;
  created_at: string;
  accepted_at: string | null;
  attached_at: string | null;
  ended_at: string | null;
  close_reason: string | null;
  error: string | null;
  usage_seconds: number;
  from_number: string | null;
  to_number: string | null;
}

export type LiveSessionEventKind =
  | 'transcript'
  | 'delegation'
  | 'answer'
  | 'instruction'
  | 'thinking'
  | 'fallback'
  | 'question'
  | 'follow_up'
  | 'transfer'
  | 'hangup'
  | 'dtmf'
  | 'closed'
  | 'error'
  | 'system';

export interface LiveSessionEvent {
  id: number;
  event_id: string;
  kind: LiveSessionEventKind;
  speaker: 'caller' | 'assistant' | 'job' | 'recorder' | null;
  text: string;
  start_ms: number | null;
  end_ms: number | null;
  delegation_id: string | null;
  payload: Record<string, unknown>;
  created_at: string;
}

export interface LiveSessionEventsPage {
  events: LiveSessionEvent[];
  next_after_id: number;
}

export function isActiveLiveSession(session: LiveSession): boolean {
  return session.status === 'active' || session.status === 'ringing';
}
