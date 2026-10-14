import { LiveSession, LiveSessionEvent } from '../../core/models/live-session.model';

export interface TranscriptBubble {
  type: 'transcript';
  id: number;
  speaker: LiveSessionEvent['speaker'];
  text: string;
}

export interface BackOfficeCard {
  type: 'backoffice';
  id: number;
  delegationId: string | null;
  request: string;
  state: 'pending' | 'answered' | 'noted' | 'followUp';
  response: string;
}

export interface CallSystemLine {
  type: 'system';
  id: number;
  kind: 'instruction' | 'system' | 'error' | 'transfer' | 'hangup';
  text: string;
}

export type CallTimelineEntry = TranscriptBubble | BackOfficeCard | CallSystemLine;
export interface CallFollowUp {
  id: number;
  text: string;
}

function requestText(event: LiveSessionEvent): string {
  const request = event.payload?.['request'];
  return typeof request === 'string' && request ? request : event.text;
}

/** Rebuild from the ordered journal so replay/live ordering yields the same UI. */
export function projectCallTimeline(events: readonly LiveSessionEvent[]): {
  timeline: CallTimelineEntry[];
  followUps: CallFollowUp[];
} {
  const ordered = [...new Map(events.map((event) => [event.id, event])).values()].sort(
    (a, b) => a.id - b.id,
  );
  const timeline: CallTimelineEntry[] = [];
  const cards = new Map<string, BackOfficeCard>();
  const explicitFollowUps: CallFollowUp[] = [];
  const followUpKeys = new Set<string>();
  let previous: LiveSessionEvent | null = null;

  for (const event of ordered) {
    if (event.kind === 'transcript') {
      const last = timeline.at(-1);
      if (
        previous?.kind === 'transcript' &&
        previous.speaker === event.speaker &&
        last?.type === 'transcript'
      ) {
        // Deltas already carry their own whitespace; inserting spaces breaks
        // partial words such as "Schwimm" + "bad".
        last.text += event.text;
      } else {
        timeline.push({
          type: 'transcript',
          id: event.id,
          speaker: event.speaker,
          text: event.text,
        });
      }
    } else if (['delegation', 'question', 'answer', 'fallback', 'follow_up'].includes(event.kind)) {
      const key = event.delegation_id ?? `event:${event.id}`;
      let card = cards.get(key);
      if (!card) {
        card = {
          type: 'backoffice',
          id: event.id,
          delegationId: event.delegation_id,
          request: '',
          state: 'pending',
          response: '',
        };
        cards.set(key, card);
        timeline.push(card);
      }
      switch (event.kind) {
        case 'delegation':
          if (!card.request) card.request = requestText(event);
          break;
        case 'question':
          card.request = requestText(event) || card.request;
          break;
        case 'answer':
          card.state = 'answered';
          card.response = event.text;
          break;
        case 'fallback':
          card.state = 'noted';
          card.response = event.text;
          break;
        case 'follow_up':
          card.state = 'followUp';
          card.response = requestText(event);
          followUpKeys.add(key);
          explicitFollowUps.push({ id: event.id, text: requestText(event) || card.request });
          break;
      }
    } else if (
      event.text &&
      ['instruction', 'system', 'error', 'transfer', 'hangup'].includes(event.kind)
    ) {
      timeline.push({
        type: 'system',
        id: event.id,
        kind: event.kind as CallSystemLine['kind'],
        text: event.text,
      });
    }
    // Even a hidden journal row breaks consecutive transcript deltas.
    previous = event;
  }
  const unanswered = [...cards.entries()]
    .filter(([key, card]) => card.request && card.state !== 'answered' && !followUpKeys.has(key))
    .map(([, card]) => ({ id: card.id, text: card.request }));
  return { timeline, followUps: [...explicitFollowUps, ...unanswered].sort((a, b) => a.id - b.id) };
}

export function sessionDuration(call: LiveSession, now: number): string | null {
  let seconds: number;
  if (call.status === 'ringing') return null;
  if (call.status === 'active') {
    const start = call.accepted_at ? Date.parse(call.accepted_at) : NaN;
    if (!Number.isFinite(start)) return null;
    seconds = Math.max(0, (now - start) / 1000);
  } else {
    const start = call.accepted_at ? Date.parse(call.accepted_at) : NaN;
    const end = call.ended_at ? Date.parse(call.ended_at) : NaN;
    seconds =
      Number.isFinite(start) && Number.isFinite(end) && end >= start
        ? (end - start) / 1000
        : call.usage_seconds;
  }
  if (!Number.isFinite(seconds) || seconds < 0) return null;
  const total = Math.floor(seconds);
  const minutes = Math.floor(total / 60);
  const remainder = String(total % 60).padStart(2, '0');
  return minutes < 60
    ? `${minutes}:${remainder}`
    : `${Math.floor(minutes / 60)}:${String(minutes % 60).padStart(2, '0')}:${remainder}`;
}
