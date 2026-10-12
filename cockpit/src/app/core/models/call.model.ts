/** Phone subagent wire contracts. Event ids are ordered within a call. */
export interface Call {
  id: string;
  session_id: string;
  line_id: string;
  line_name: string;
  direction: 'inbound' | 'outbound';
  status: 'ringing' | 'active' | 'ended' | 'rejected' | 'failed';
  from_number: string;
  to_number: string;
  created_at: string;
  accepted_at: string | null;
  attached_at: string | null;
  ended_at: string | null;
  close_reason:
    | 'remote_hangup'
    | 'close_requested'
    | 'expired'
    | 'content'
    | 'connection_lost'
    | 'recorder_lost'
    | null;
  usage_seconds: number;
  job_id: string;
}

export type CallEventKind =
  | 'transcript'
  | 'delegation'
  | 'question'
  | 'answer'
  | 'fallback'
  | 'follow_up'
  | 'instruction'
  | 'thinking'
  | 'transfer'
  | 'hangup'
  | 'dtmf'
  | 'usage'
  | 'closed'
  | 'error'
  | 'system';

export interface CallEvent {
  id: number;
  call_id: string;
  kind: CallEventKind;
  speaker: 'caller' | 'assistant' | 'job' | 'recorder' | null;
  text: string;
  start_ms: number | null;
  end_ms: number | null;
  delegation_id: string | null;
  payload: Record<string, unknown>;
  created_at: string;
}

export interface CallEventsPage {
  events: CallEvent[];
  next_after_id: number;
}

export function isLiveCall(call: Call): boolean {
  return call.status === 'active' || call.status === 'ringing';
}
