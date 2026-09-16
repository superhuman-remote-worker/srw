import {
  computed,
  DestroyRef,
  effect,
  inject,
  Injectable,
  NgZone,
  signal,
  untracked,
} from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { HttpClient } from '@angular/common/http';
import { filter, firstValueFrom, map, Observable, Subscription, tap, timeout } from 'rxjs';
import { environment } from '../environment';
import { Project, ThreadCloudDiffSummary, ThreadMount, ThreadStatus } from '../models/api.model';
// Type + pure derivation only — importing them from the review component
// would pull it (and Monaco's loader) back into the eager bundle graph and
// defeat the @defer that keeps the review surface lazy.
import {
  folderLinkMatches,
  ProtectedFolderLink,
  selectProtectedProjectMount,
} from '../../views/job-diff-review/protected-folder-link';
import { FilePreview, ThreadUploadEvent } from '../models/file.model';
import {
  AssistantTurn,
  ConversationState,
  EMPTY_CONVERSATION,
  isAssistantTurn,
  SystemTurn,
  ToolCallEvent,
  Turn,
  UserTurn,
} from '../models/turn.model';
import { TranslocoService } from '@jsverse/transloco';
import { ApiService } from './api.service';
import type { SessionQueueState } from '../models/api.model';
import { ErrorMessageService } from './error-message.service';
import { IndexedDbService } from './indexed-db.service';
import { NotificationService } from './notification.service';
import { classifyResumeError, ConfigDriftItem } from './resume-error';
import { reduce, ReducerAction } from './turn-reducer';
import {
  attachmentDedupeKey,
  classifyUploadFailure,
  composeAgentContent,
  PendingUpload,
  progressWriteDue,
} from './upload-stage';
import { UploadRegistryService } from './upload-registry.service';
import { AppToastService } from '../../ui/toast';
import {
  CanvasControl,
  CanvasPresentationUpdatedControl,
  CanvasSourceUpdatedControl,
  PersistentThreadTransportBridge,
} from './persistent-thread-transport-bridge.service';
import { CanvasService } from './canvas.service';
import { CapabilitiesService } from './capabilities.service';

/**
 * Transport architecture (post WS→SSE migration, 2026-05-13):
 *
 *  • Server → client: GET /api/persistent/threads/{id}/stream — Server-Sent
 *    Events. EventSource handles reconnect natively with the `Last-Event-ID`
 *    header (set automatically by the browser from the latest `id:` line we
 *    received). We additionally persist the cursor `(epoch, seq)` in IndexedDB
 *    so cross-tab-close resume works — when the user reopens the tab, we read
 *    the saved cursor and pass it as the initial `Last-Event-ID` so any events
 *    the agent produced while we were away replay cleanly.
 *
 *  • Client → server: POST /api/persistent/threads/{id}/input (messages) and
 *    POST /api/persistent/threads/{id}/interrupt (interrupt). Canonical REST.
 *
 *  • Control plane: mode + narration assignments use the orchestrator's
 *    durable REST inbox for every session. Socketless workspace undo uses the
 *    same inbox; pinned undo retains its legacy direct WebSocket verb. Durable
 *    results arrive through the journal/SSE after the serving owner applies
 *    them. The agent's existing WebSocket handler remains for the smaller
 *    verbs that have not yet gained a safe durable contract — approve/deny,
 *    the remaining slash commands, config updates and vm-upgrade.
 *
 * `gone_beyond_horizon`: the server emits this single named event when the
 * cursor is outside replay range (epoch mismatch or seq older than retention).
 * Handler: drop the cursor, REST-reload the message history snapshot, reopen
 * the stream without a cursor.
 */

const CONTROL_WS_RECONNECT_DELAYS_MS = [500, 1000, 2000, 4000];
const CONTROL_WS_RECONNECT_MAX_ATTEMPTS = 8;

// Idle archive emits session.ended before its later memory/git/workspace
// drain can finish staging protected-cloud changes.  Keep the SSE/review
// plane alive and make a bounded sequence of summary probes without ever
// restarting /connection, /prepare, or the control WS.  These are intervals,
// not absolute times; the final probe lands a little under nine minutes after
// the terminal frame, covering browser timer freezes and the slow archive
// path while remaining strictly bounded.
const POST_TERMINAL_CLOUD_PROBE_DELAYS_MS = [
  0, 1000, 3000, 7000, 15_000, 30_000, 60_000, 120_000, 300_000,
];

function isCommittedCanvasControl(
  control: CanvasControl,
): control is CanvasSourceUpdatedControl | CanvasPresentationUpdatedControl {
  return (
    control.method === 'canvas.source_updated' || control.method === 'canvas.presentation_updated'
  );
}

/** Session title from a landing draft's first message: first line, collapsed
 *  whitespace, capped at 60 chars (on a word boundary when one is near). */
export function draftTitleFrom(message: string): string {
  const line = message.split('\n')[0].replace(/\s+/g, ' ').trim();
  if (!line) return 'Untitled Session';
  if (line.length <= 60) return line;
  const cut = line.slice(0, 60);
  const lastSpace = cut.lastIndexOf(' ');
  return (lastSpace > 30 ? cut.slice(0, lastSpace) : cut) + '…';
}

// Control-WS liveness watchdog. The agent's subscriber pump sends a
// `ws.ping` frame every ~20s of idle (src/api/persistent_app.py,
// _WS_PING_INTERVAL_S); if the socket claims OPEN but nothing arrived in
// CONTROL_WS_WATCHDOG_TIMEOUT_MS it's half-open (edge/tunnel idle kill — no
// close frame ever reaches us), so force-close it and let the reconnect
// ladder re-fetch a fresh token. Closes the gap left when F4.5 shipped for
// the SSE only (session_silent_failure_audit.md #9).
const CONTROL_WS_WATCHDOG_INTERVAL_MS = 15_000;
const CONTROL_WS_WATCHDOG_TIMEOUT_MS = 45_000;

// SSE liveness watchdog. The orchestrator emits a typed `ping` event every
// ~20s of idle (see `thread_event_stream` in `orchestrator/main.py`); the
// watchdog checks every WATCHDOG_INTERVAL_MS and forces a reopen if no SSE
// event of any kind has arrived in WATCHDOG_TIMEOUT_MS. This catches silent
// TCP drops (Wi-Fi blip, captive portal, laptop sleep) that don't trip
// `EventSource.onerror` until the OS-level keepalive eventually fires
// hours later. See `knowledge-base/knowledge/issues/persistent_chat_silent_disconnect.md`.
const SSE_WATCHDOG_INTERVAL_MS = 5000;
const SSE_WATCHDOG_TIMEOUT_MS = 45000;

// After an interrupt POST we wait for the agent to emit `interrupt.ack` /
// `turn.completed` over SSE to clear the "Stopping…" state. If that frame is
// lost (silently-stalled stream) the button would wedge forever — re-clicks
// early-return. If nothing clears it within this window, force an SSE reopen
// (replay-from-cursor) which re-delivers the durable turn boundary.
const INTERRUPT_ACK_TIMEOUT_MS = 8000;
const INTERRUPT_RESPONSE_TIMEOUT_MS = 15_000;
const INTERRUPT_RETRY_DELAYS_MS = [250, 1000, 2000, 4000];

// A rewind's direct-to-socket ack can legitimately take a while: the backend
// waits up to 60s server-side for an in-flight turn to interrupt before it
// can even start the sweep, then does git work (checkpoint restore). Give it
// noticeably more room than a plain interrupt before assuming the ack was
// lost (e.g. a WS drop/reconnect between send and ack) and un-wedging the
// rewindInFlight-gated UI client-side.
const REWIND_ACK_TIMEOUT_MS = 90_000;

// After a send is authoritatively accepted (POST 200), the reply must begin
// arriving over SSE. No 409 is duplicate proof because input has no client
// idempotency key; every 409 stays visibly queued/stalled. If no SSE *data*
// frame lands within this window the receive path is presumed dead (zombie
// epoch, silently-dropped socket) and we force one reopen so replay-from-cursor
// delivers the turn. One-shot per send; never re-POSTs (the input is already
// accepted server-side and there's no idempotency key).
const SEND_KICKSTART_TIMEOUT_MS = 5000;

/** Attachment chip shown alongside a user message. */
export interface ChatAttachment {
  /**
   * Stable local id (the FilePreview.id it came from). Exists BEFORE the
   * upload does, which `path` does not — so this is what the bubble's
   * @for tracks by. Tracking by `path` gave every pre-upload chip the key
   * `undefined`: duplicate keys (NG0955) with two files, and a full
   * destroy/recreate of every chip node once the real paths arrived.
   */
  id: string;
  name: string;
  size: number;
  mimeType: string;
  /** Workspace-relative path, e.g. "uploads/photo.jpg". Absent until the
   *  upload resolves. */
  path?: string;
}

/**
 * A queued outgoing send. The queue is *user intent*, not transport state:
 * it survives disconnect/reconnect/thread-creation and is the single source of
 * truth for "messages the user asked to send but which haven't been accepted by
 * the server yet". `localId` is the optimistic bubble's id, so a flushed/failed
 * item can find and restyle or roll back its bubble.
 */
export interface OutboxItem {
  /** The optimistic user-bubble's makeLocalId('user') — bubble↔queue link. */
  localId: string;
  /** What gets POSTed. Undefined until the upload stage resolves the file
   *  names that go into the attachment hint; computed in _flushOutbox. */
  content?: string;
  /** The user's typed text only — used to re-dispatch the bubble faithfully
   *  (without the attachment hint) after a history reload. */
  displayContent: string;
  /** Attachment chips to re-render on the bubble. Present from creation for
   *  the local descriptors; each gains `path` as its upload resolves. */
  attachments?: ChatAttachment[];
  /** Files still to upload, holding their File handles. Empty/absent once
   *  every file has resolved. */
  pendingFiles?: PendingUpload[];
  /** The thread this item belongs to. The flush already guards the POST
   *  against a mid-flight thread switch via tidAtPost; the upload stage is a
   *  second, longer await that needs the same guard, and an eagerly-started
   *  upload (Slice 3) could otherwise resolve into a foreign queue. */
  threadId: string;
  /** Flush attempts so far (diagnostic; there is deliberately no auto-retry). */
  attempts: number;
  /** Transcript revision captured when the user committed this send. */
  expectedConversationRevision?: number;
  /** Rewind moved the transcript past this send's captured revision. */
  requiresReview?: boolean;
}

/** Info about a tool call within an assistant message. */
export interface ToolCallInfo {
  id: string;
  tool: string;
  args: Record<string, unknown>;
  result?: string;
  status: 'pending' | 'running' | 'completed' | 'denied' | 'error' | 'expired';
  /** Tool category from the registry (e.g. workspace, git, research). */
  category?: string;
  /**
   * Supervised approval outcome, if this call passed through a permission
   * gate. Persisted on the backend so it survives history reload.
   * Absent for autonomous / auto-accepted calls.
   */
  decision?: 'approved' | 'denied' | 'expired';
}

/**
 * The tool call the agent is currently blocked on, delivered in the
 * session.state welcome frame so a (re)attaching client can render a
 * running-command card even when the in-flight turn isn't in REST history yet
 * (it's persisted only at turn end).
 */
export interface RunningToolInfo {
  id: string;
  tool: string;
  args: Record<string, unknown>;
}

/** Permission request from the agent. */
export interface PermissionRequest {
  /** Tool call id — correlates the eventual decision back to the call. */
  id: string;
  /** Durable DB permission request id, when emitted by the backend. */
  approvalId?: string;
  tool: string;
  args: Record<string, unknown>;
}

/** A task tracked during the session. */
export interface SessionTask {
  id: string;
  description: string;
  status: 'pending' | 'in_progress' | 'completed';
  priority: string;
  notes: string;
  created_at: string;
  completed_at: string | null;
}

type ConnectionState = 'disconnected' | 'connecting' | 'connected' | 'error';
export type PermissionMode = 'supervised' | 'auto_accept' | 'autonomous';
export type NarrationMode = 'silent' | 'verbose' | 'auto';

type DurableControl =
  | { method: 'mode.set'; mode: PermissionMode }
  | { method: 'narration.set'; mode: NarrationMode }
  | { method: 'workspace.undo' };

type DurableScalarControl = Exclude<DurableControl, { method: 'workspace.undo' }>;

function isDurableScalarControl(control: DurableControl): control is DurableScalarControl {
  return control.method !== 'workspace.undo';
}

interface DurableControlOutboxItem {
  threadId: string;
  request: DurableControl & {
    client_request_id: string;
    session_runtime_generation: string;
  };
  attempts: number;
  ordinal: number;
}

interface DurableControlMarker {
  method: DurableControl['method'] | null;
  ordinal: number;
}

interface DurableControlError extends DurableControlMarker {
  message: string;
}

interface PendingInterruptRequest {
  threadId: string;
  clientRequestId: string;
  targetTurnId: number;
  attempts: number;
}

/** Human-readable labels for the closed session tool groups (transcript stamps). */
const TOOL_GROUP_LABELS: Record<string, string> = {
  canvas: 'Canvas',
  orchestrator: 'Fleet Management',
  agent_catalog: 'Experts & Skills',
  workflows: 'Automations & Loops',
};

/** The ack's connector-change summary (names only — Slice B). */
export interface AppliedDatasourceChange {
  added?: string[];
  removed?: string[];
  kb_deferred?: boolean;
}

/** Summarize a config.changed `applied` fragment for the transcript stamp.
 *  Exported for tests. */
export function describeAppliedConfig(
  applied: Record<string, unknown>,
  datasources?: AppliedDatasourceChange,
): string[] {
  const parts: string[] = [];
  const llm = applied['llm'] as Record<string, unknown> | undefined;
  if (llm?.['model']) parts.push(`model → ${llm['model']}`);
  if (llm?.['temperature'] != null) parts.push(`temperature → ${llm['temperature']}`);
  if (llm?.['reasoning_level']) parts.push(`reasoning → ${llm['reasoning_level']}`);
  const tools = applied['tools'] as Record<string, unknown> | undefined;
  for (const [group, value] of Object.entries(tools ?? {})) {
    const label = TOOL_GROUP_LABELS[group] ?? group;
    const enabled = Array.isArray(value) && value.length > 0;
    parts.push(`${label} tools ${enabled ? 'enabled' : 'disabled'}`);
  }
  const interactive = applied['interactive'] as Record<string, unknown> | undefined;
  if (interactive?.['permission_mode']) parts.push(`mode → ${interactive['permission_mode']}`);
  if (interactive?.['narration_mode']) parts.push(`narration → ${interactive['narration_mode']}`);
  for (const name of datasources?.added ?? []) {
    parts.push(`connector "${name}" attached`);
  }
  for (const name of datasources?.removed ?? []) {
    parts.push(`connector "${name}" detached`);
  }
  if (datasources?.kb_deferred) {
    parts.push('knowledge-base change applies on next resume');
  }
  return parts;
}

/**
 * Live token telemetry for the current turn, driven by per-LLM-call
 * `usage.updated` frames. `inputTokens` is the latest call's prompt size —
 * effectively the current context fill — while output/reasoning accumulate
 * across the turn's calls. (knowledge-base/knowledge/features/context_summarization_rework.md S5)
 */
export interface UsageState {
  /** The thread these numbers describe. The service is a root singleton, so
   *  binding the value to its thread — rather than trusting every future
   *  thread-transition path to remember a reset — is what makes a stale panel
   *  structurally unrenderable. Read through `currentUsage`, never `usage`
   *  directly. See
   *  knowledge-history/done/session_usage_panel_leaks_previous_session_counters.md. */
  threadId: string | null;
  turn: number | null;
  inputTokens: number | null;
  outputTokensTurn: number;
  reasoningTokensTurn: number;
  /** True when ``reasoningTokensTurn`` was derived from the captured reasoning
   *  text because the provider reported no reasoning-token count (e.g. gemma
   *  via vLLM). The figure is then a subset of ``outputTokensTurn``. */
  reasoningEstimated: boolean;
  ctxLimitTokens: number | null;
  /** Absolute token count at which auto-compaction fires. The ctx gauge is
   *  anchored on this (not the raw model window), so "danger" means a
   *  compaction is imminent. Null until the agent reports it; the gauge then
   *  falls back to ``ctxLimitTokens``. */
  compactionThresholdTokens: number | null;
}

/**
 * Live state of an in-flight context compaction, driven by the agent's
 * `compaction.started` / `compaction.progress` frames and cleared by
 * `context.compacted` (success) or `compaction.failed`. Frames are journaled
 * server-side, so a reload mid-compaction reconstructs this from SSE replay
 * (possibly from a progress frame alone — all fields nullable-tolerant).
 * See knowledge-base/knowledge/features/context_summarization_rework.md (S3).
 */
export interface CompactionProgressState {
  trigger: string;
  totalTokens: number | null;
  ctxUsedTokens: number | null;
  ctxLimitTokens: number | null;
  ctxUsedPct: number | null;
  auxLimitTokens: number | null;
  nPasses: number;
  /** 0 while planning (no progress frame yet). */
  currentPass: number;
  firstMsg: number | null;
  lastMsg: number | null;
  inTokens: number | null;
  outTokens: number | null;
  attempt: number;
  stage: string;
  /** Client epoch ms when the first compaction frame arrived (elapsed timer). */
  startedAt: number;
}

/** Transport a control verb travels over for the current session, as
 *  DECLARED by `/connection` (`controls`). `unavailable` = the server did not
 *  advertise the verb, so the Cockpit renders it disabled and never queues it.
 *  Dispatching by declaration rather than by inferring a lane from
 *  `control_socket` is what stopped `config.update` from vanishing into an
 *  outbox on queue-served sessions (issue:
 *  live_settings_silently_dropped_on_stateless_sessions). */
export type ControlTransport = 'websocket' | 'rest' | 'unavailable';
type ControlCapabilityMap = Record<string, 'websocket' | 'rest'>;
type ControlOptionsMap = Record<
  string,
  { version: number; modes?: string[]; requires_idle?: boolean }
>;

interface RewindExpected {
  session_runtime_generation: string;
  conversation_revision: number;
  events_epoch: number;
  transcript_tail_seq: string;
  input_seq: string | null;
  consumed_seq: string | null;
}

export interface StatelessRewindPreview {
  message_id: string;
  mode: 'conversation';
  prompt: string;
  eligible: boolean;
  refusal_code: string | null;
  swept_count: number;
  expected: RewindExpected | null;
}

interface StatelessRewindResult {
  state: 'applied';
  rewind_id: string;
  client_request_id: string;
  message_id: string;
  mode: 'conversation';
  prompt: string;
  swept_count: number;
  surviving_turn: number;
  conversation_revision: number;
  events_epoch: number;
  event_seq: string;
  duplicate?: boolean;
}

interface RewindOperationMarker {
  threadId: string;
  clientRequestId: string;
  body: {
    client_request_id: string;
    message_id: string;
    mode: 'conversation';
    expected: RewindExpected;
  };
  draftSnapshot: string;
  draftRevision: number;
  prefillConsumed: boolean;
}

export interface RewindPrefill {
  prompt: string;
  draftSnapshot: string;
  draftRevision: number;
  clientRequestId: string;
}

/** Verbs that have a REST transport on a socketless session under the legacy
 *  (pre-`controls`) contract: the three durable-inbox verbs, plus
 *  `config.update`, whose owner PATCH (Slice C) predates `controls` and whose
 *  connected-gate passes on a session that binds no agent. Only consulted when
 *  an older orchestrator omits `controls`; a declaration always wins. */
const LEGACY_SOCKETLESS_REST_VERBS = new Set([
  'config.update',
  'mode.set',
  'narration.set',
  'workspace.undo',
]);
/** Verbs the legacy contract carried over the direct socket on a pinned
 *  session. Same fallback role as above. */
const LEGACY_SOCKET_VERBS = new Set([
  'config.update',
  'compact',
  'archive',
  'rewind',
  'undo',
  'upgrade-to-workspace',
  'approve',
  'deny',
]);

/** How long a `config.update` sent over the socket may wait for its
 *  `config.changed` ack (or matching error frame) before the pane rolls the
 *  edit back and says so. The agent applies a live update in well under a
 *  second; a swap that compacts first can take longer, hence the slack. */
const CONFIG_UPDATE_ACK_TIMEOUT_MS = 30_000;

/** Outcome of one `updateConfig` request — resolved, never rejected, so a
 *  caller can always settle its own optimistic state. */
export type ConfigUpdateOutcome =
  | {
      ok: true;
      requestId: string;
      transport: 'websocket' | 'rest';
      /** The fragment the server accepted (REST: the redacted persisted
       *  fragment; socket: the echoed `applied` fragment). */
      applied: Record<string, unknown>;
      /** `now` = the running session rebuilt itself; `next_turn` = persisted,
       *  picked up by the next claim (queue-served sessions). */
      effective: 'now' | 'next_turn' | 'next_attach';
    }
  | { ok: false; requestId: string; transport: ControlTransport; message: string };

/** Lane-free control-socket discovery. The server may carry additional
 * execution details, but the Cockpit discriminates only on transport. */
/** Poll cadence for the durable queue block while a send awaits a claim. */
export const QUEUE_POLL_MS = 5_000;

type ConnectionPayload =
  | {
      state: 'ready';
      control_socket: 'websocket';
      ws_url: string;
      token: string;
      expires_at: number;
      pinned_runtime_generation_contract: 1;
      session_runtime_generation: string;
      controls?: ControlCapabilityMap;
      control_options?: ControlOptionsMap;
      conversation_revision?: number;
      queue?: SessionQueueState | null;
    }
  | {
      state: 'ready';
      control_socket: 'none';
      ws_url: null;
      token: null;
      expires_at: null;
      pinned_runtime_generation_contract: 1;
      session_runtime_generation: string;
      controls?: ControlCapabilityMap;
      control_options?: ControlOptionsMap;
      conversation_revision?: number;
      queue?: SessionQueueState | null;
    };

/** Server-aggregated token telemetry riding the durable `session.state`
 *  snapshot. Wire shape mirrors a `usage.updated` frame, except that
 *  output/reasoning are already summed across the turn's calls. */
interface SessionStateUsage {
  turn: number | null;
  input_tokens: number | null;
  output_tokens: number | null;
  reasoning_tokens: number | null;
  reasoning_estimated: boolean;
  ctx_limit_tokens: number | null;
  compaction_threshold_tokens: number | null;
}

/** Durable REST twin of the agent's direct ``session.state`` welcome frame. */
interface SessionStateSnapshot extends Record<string, unknown> {
  thread_id: string;
  permission_mode: PermissionMode;
  narration_mode: NarrationMode;
  turn_count: number;
  turn_in_flight: boolean;
  message_count: number;
  model: string | null;
  temperature: number | null;
  running_tool: RunningToolInfo | null;
  pending_permissions: unknown[];
  /** Present on task-aware servers; omitted by older rolling-deploy peers. */
  tasks?: SessionTask[];
  /** Last known token telemetry, aggregated from the journal at
   *  `event_cursor`. Explicit null = this thread has never reported usage.
   *  Omitted entirely by older rolling-deploy peers. */
  usage?: SessionStateUsage | null;
  event_cursor: { epoch: number; seq: number };
  /** Exclusive journal floor that reconstructs the latest logical turn. */
  replay_cursor: { epoch: number; seq: number };
  conversation_revision?: number;
  snapshot_source: 'durable_journal';
}

/**
 * Persistent agent session client. See file header for transport rationale.
 *
 * All state is exposed as Angular signals for reactive UI updates.
 */
/** A citation created during a persistent session (engine-backed). The marker
 *  the agent emits inline — `[<id>]` — is `id`; the chat resolves it to this. */
export interface ThreadCitation {
  id: number;
  claim: string;
  source_id: number;
  source_name: string;
  source_type: string;
  source_identifier: string | null;
  verification_status: string;
  confidence: string;
  created_at: string;
  /** Cloud-document citation (cite_document w/ snapshot-anchor): can drift-check. */
  has_cloud_anchor?: boolean;
  /** A backed-up original copy exists → "view original" can stream it. */
  has_snapshot?: boolean;
}

/** On-view drift result for a cloud-document citation (Phase 3c /drift). */
export interface CitationDrift {
  citation_id: number;
  live_state: 'unchanged' | 'changed' | 'unreachable' | 'unknown';
  snapshot_available: boolean;
  reason?: string;
}

@Injectable({ providedIn: 'root' })
export class PersistentChatService {
  private readonly http = inject(HttpClient);
  private readonly api = inject(ApiService);
  private readonly cache = inject(IndexedDbService);
  private readonly zone = inject(NgZone);
  private readonly toast = inject(AppToastService);
  private readonly notifications = inject(NotificationService);
  private readonly transloco = inject(TranslocoService);
  private readonly errors = inject(ErrorMessageService);
  private readonly destroyRef = inject(DestroyRef);
  private readonly threadTransport = inject(PersistentThreadTransportBridge);
  private readonly canvas = inject(CanvasService);
  private readonly capabilities = inject(CapabilitiesService);
  /** Uploads started when a file was ATTACHED, before the user committed to
   *  sending it (knowledge-base/knowledge/features/session_attachment_send_flow.md §5.4). Owned
   *  by a root service, not this one: an uncommitted upload has a different
   *  lifetime from the outbox, and must survive the chat page being
   *  destroyed by navigation. */
  private readonly uploads = inject(UploadRegistryService);

  constructor() {
    let datasourceDefaultsAvailable = this.capabilities.datasourceScopeAutoAttachAvailable();
    this.capabilities.datasourceScopeAutoAttachAvailability$
      .pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe((available) => {
        if (available === datasourceDefaultsAvailable) return;
        datasourceDefaultsAvailable = available;
        if (this.isDraftSession()) void this.retryDraftDefaults();
      });
    effect(() => {
      const epochSignal = (
        this.cache as IndexedDbService & {
          threadHistoryEpochChanged?: () => {
            threadId: string;
            eventsEpoch: number;
            conversationRevision: number;
          } | null;
        }
      ).threadHistoryEpochChanged;
      if (typeof epochSignal !== 'function') return;
      const change = epochSignal();
      const threadId = untracked(() => this.threadId());
      if (!change || !threadId || change.threadId !== threadId) return;
      const key = `${change.threadId}:${change.eventsEpoch}:${change.conversationRevision}`;
      if (key === this.observedHistoryEpoch) return;
      this.observedHistoryEpoch = key;
      this.conversationRevision.update((current) =>
        Math.max(current, change.conversationRevision),
      );
      const generation = this.connectGeneration;
      void this.loadHistory(threadId, generation);
    });
    // PersistentChatService remains the sole SSE/WebSocket lifecycle owner.
    // Canvas receives decoded invalidations through this narrow bridge and
    // may send only its typed control vocabulary back through the current
    // thread's existing control socket.
    const detachCanvasControl = this.threadTransport.attachControlSender((threadId, control) =>
      this._sendCanvasControl(threadId, control),
    );
    this.destroyRef.onDestroy(detachCanvasControl);

    // Refresh the thread's engine citations whenever a turn finishes (the
    // agent may have created new ones via cite_web/cite_document), so inline
    // [N] markers resolve live without a reload. See CitationRefDirective.
    effect(() => {
      const waiting = this.isWaitingForInput();
      const tid = untracked(() => this.threadId());
      if (waiting && tid) {
        void this.loadCitations(tid);
      }
    });
    // Single source of truth for the "Starting session" card phase
    // transitions. The orchestrator emits session.lifecycle events on
    // the user's always-on /notifications/events SSE from every
    // binding path (provision_or_assign for the create-thread fast
    // path, _do_prepare for the cold path). Subscribe via the
    // NotificationService signal so the SSE feed isn't opened twice.
    effect(() => {
      const event = this.notifications.lifecycleEvent();
      const tid = this.threadId();
      if (!event || !tid || event.thread_id !== tid) return;
      const eventGeneration = this._canonicalRuntimeGeneration(event.session_runtime_generation);
      const invalidBinding = this.invalidBindingRuntime;
      if (invalidBinding?.threadId === tid) {
        // A binding refusal is scoped to one exact runtime. Same-G and
        // legacy/malformed lifecycle noise cannot reopen it, but a canonical
        // successor generation is durable authority to try the control plane
        // again. Keep the review/SSE plane in place throughout.
        if (!eventGeneration || eventGeneration === invalidBinding.generation) return;
        this.bindingRecoveryRuntime = {
          threadId: tid,
          rejectedGeneration: invalidBinding.generation,
          candidateGeneration: eventGeneration,
        };
        this.invalidBindingRuntime = null;
        this.sessionRuntimeGeneration = null;
        this.controlSocket = 'unknown';
        this.connectionState.set('connecting');
        if (this.error() === this.transloco.translate('errors.sessions.bindingInvalid')) {
          this.error.set(null);
        }
        queueMicrotask(() => {
          if (
            this.threadId() === tid &&
            this.invalidBindingRuntime?.threadId !== tid &&
            this.terminalControlThreadId !== tid
          ) {
            this._ensureControlWs();
          }
        });
      }
      const bindingRecovery = this.bindingRecoveryRuntime;
      if (
        bindingRecovery?.threadId === tid &&
        eventGeneration !== bindingRecovery.candidateGeneration
      ) {
        return;
      }
      if (
        (event.session_runtime_generation !== undefined && !eventGeneration) ||
        (this.retiredRuntimeGeneration !== null &&
          eventGeneration === this.retiredRuntimeGeneration) ||
        (this.sessionRuntimeGeneration !== null &&
          eventGeneration !== this.sessionRuntimeGeneration)
      ) {
        return;
      }
      // A delayed provisioning/booting/ready event from the just-ended
      // runtime cannot reopen its startup card. Only explicit Resume clears
      // this terminal latch.
      if (this.terminalControlThreadId === tid) return;
      // Once the session is actually live, ignore further lifecycle
      // events (a duplicate from a racing /prepare must not regress
      // the UI). isStartingSession also gates rendering on
      // sessionReady, so the card hides naturally.
      if (this.sessionReady()) return;
      // The orchestrator tags VM-backed starts so the card shows the
      // longer "Booting VM" copy and the readiness poll extends its
      // budget — reasserted here so resume (no create body) is covered.
      if (event.backend === 'vm') this.isVmSession.set(true);
      switch (event.state) {
        case 'provisioning':
        case 'booting':
          this.startupPhase.set(event.state);
          break;
        case 'ready':
          // Server says the agent is session-ready; the cockpit
          // is now opening the WS — that's the "establishing
          // connection" phase. session.state arriving on the WS
          // will flip sessionReady=true and hide the card.
          this.startupPhase.set('connecting');
          break;
        case 'failed':
          this.error.set(event.reason || 'session preparation failed');
          break;
      }
    });
    // Live/non-terminal stage publications travel on the app-wide
    // notification SSE. Terminal publications are durably replayed on the
    // thread journal and converge through the matching _handleEvent case.
    effect(() => {
      const event = this.notifications.cloudDiffStagedEvent();
      if (!event || !this._cloudDiffStagedEventApplies(event)) return;
      void this.refreshCloudDiffCount();
    });

    // Mirror the assistant turn's streaming state into the transport
    // bridge: the shared-browser baton automation hands control to the
    // user when the agent's turn completes and back when one starts,
    // without Canvas ever depending on this service.
    effect(() => {
      this.threadTransport.setAgentTurnActive(this.isStreaming());
    });

    // Stamp the start of an awaiting stretch and tick a 1 s clock while it
    // lasts. Keyed off isAwaitingTurn so every path that zeroes
    // pendingTurnCount also ends the stretch; the ticker never runs idle.
    effect(() => {
      const awaiting = this.isAwaitingTurn();
      untracked(() => {
        if (!awaiting) {
          this._stopAwaitingClock();
          return;
        }
        const now = Date.now();
        if (this.awaitingSince() === null) this.awaitingSince.set(now);
        this.awaitingNow.set(now);
        if (this.awaitingTicker === null) {
          this.awaitingTicker = setInterval(() => this.awaitingNow.set(Date.now()), 1000);
        }
      });
    });
    this.destroyRef.onDestroy(() => this._stopAwaitingClock());

    // While a send awaits a claim, poll the durable queue block so a unit
    // that gets parked (or a reload that lost the accept) is rendered from
    // the server's truth rather than process-local state. A parked verdict
    // ends the awaiting stretch and so the poll.
    //
    // The same poll also runs while the "Starting session" card is up on a
    // thread that exists. That card yields on `sessionReady`, which no
    // durable read flips on its own: if `/connection` is still polling behind
    // a not-yet-ready protected-cloud runtime, nothing else would ever tell
    // this tab that the queued first turn already ran to completion, and the
    // card would outlive the answer (see
    // _reconcileReadinessFromDurableState). The poll costs one 5 s REST read
    // per startup and stops the moment readiness lands, from this path or
    // any other.
    effect(() => {
      const awaiting = this.isAwaitingTurn();
      // `isStartingSession` also covers the pre-thread create, where
      // _pollQueueState no-ops for want of a thread id.
      const startingSession = this.isStartingSession();
      untracked(() => {
        if (!awaiting && !startingSession) {
          this._stopQueuePoll();
          return;
        }
        if (this.queuePollTimer === null) {
          this.queuePollTimer = setInterval(() => void this._pollQueueState(), QUEUE_POLL_MS);
        }
      });
    });
    this.destroyRef.onDestroy(() => this._stopQueuePoll());

    // Invariant: "Stopping…" (isInterrupting) only makes sense while a turn
    // is actually streaming. Whenever streaming ends — turn completed, the
    // turn closed on disconnect, or a reconnect re-synced past it — clear
    // the flag and its fallback timer. This is the safety net that stops a
    // lost interrupt.ack/turn.completed frame from wedging the button: any
    // path that drops the active turn also drops "Stopping…".
    effect(() => {
      if (
        !this.isStreaming() &&
        untracked(() => this.isInterrupting() || this.pendingInterruptRequest !== null)
      ) {
        this._clearPendingInterruptRequest();
      }
    });

    // Re-validate the connection whenever the user returns to the tab.
    // The SSE liveness watchdog is a setInterval, which browsers freeze on
    // a backgrounded tab (~5 min) — so a silent drop goes unnoticed until
    // the user comes back. These DOM listeners fire outside Angular's zone,
    // hence zone.run. Registered once on this root singleton and torn down
    // via DestroyRef (matters for test isolation, not prod lifetime).
    if (typeof document !== 'undefined') {
      const revalidate = (force: boolean) => this.zone.run(() => this._revalidateConnection(force));
      const onVisible = () => {
        if (document.visibilityState === 'visible') {
          // A hide longer than the watchdog window means the socket
          // may have died silently while the tab was frozen (it still
          // reports readyState===OPEN) — force an unconditional
          // revalidate in that case; otherwise the cheap check.
          const hiddenFor = this.hiddenAt ? Date.now() - this.hiddenAt : 0;
          this.hiddenAt = 0;
          revalidate(hiddenFor > SSE_WATCHDOG_TIMEOUT_MS);
        } else {
          this.hiddenAt = Date.now();
        }
      };
      // online / focus: cheap re-check (a live OPEN socket needn't reopen).
      const onUnforced = () => revalidate(false);
      // bfcache restore (persisted) and Chromium's page 'resume' both can
      // close sockets or skip events while frozen → always force.
      const onPageShow = (e: PageTransitionEvent) => {
        if (e.persisted) revalidate(true);
      };
      const onResume = () => revalidate(true);
      document.addEventListener('visibilitychange', onVisible);
      window.addEventListener('online', onUnforced);
      window.addEventListener('focus', onUnforced);
      window.addEventListener('pageshow', onPageShow);
      document.addEventListener('resume', onResume);
      this.destroyRef.onDestroy(() => {
        document.removeEventListener('visibilitychange', onVisible);
        window.removeEventListener('online', onUnforced);
        window.removeEventListener('focus', onUnforced);
        window.removeEventListener('pageshow', onPageShow);
        document.removeEventListener('resume', onResume);
      });
    }
  }

  // --- Connection state ---
  readonly connectionState = signal<ConnectionState>('disconnected');
  readonly isConnected = computed(() => this.connectionState() === 'connected');

  // Agent-liveness, distinct from connection-liveness (audit #8). Seconds
  // since the last agent-origin frame while a turn is open; 0 when idle.
  // Updated on the SSE watchdog's 5s tick.
  readonly agentSilenceSeconds = signal<number>(0);
  private agentLastEventAt = 0;

  // Live context-compaction progress (null = no compaction running).
  // Drives the in-chat progress block + status-bar strip.
  readonly compaction = signal<CompactionProgressState | null>(null);

  // Live per-turn token telemetry (usage.updated frames); null until the
  // first main-LLM call reports usage. Raw backing signal — UI reads
  // `currentUsage`, which enforces the thread binding.
  readonly usage = signal<UsageState | null>(null);
  readonly threadId = signal<string | null>(null);
  /**
   * The usage panel's only sanctioned source: telemetry that provably belongs
   * to the thread on screen.
   *
   * Anything else reads as null, so a value left over from a previous session
   * cannot be rendered even if some future thread-transition path forgets to
   * clear it — the leak this closes was precisely a missing reset. A null
   * `threadId` (draft session, or a value from before this field existed)
   * never matches, including against the null `threadId()` of a draft.
   */
  readonly currentUsage = computed<UsageState | null>(() => {
    const u = this.usage();
    if (!u || u.threadId == null) return null;
    return u.threadId === this.threadId() ? u : null;
  });
  /** Engine citations for this session, keyed by citation id (the agent emits
   *  the id as the inline `[N]` marker). Drives inline resolution +
   *  source popover (see CitationRefDirective). Loaded on connect + per turn. */
  readonly citationsByCid = signal<Map<number, ThreadCitation>>(new Map());
  readonly citationsLoaded = signal(false);

  /**
   * True iff a session start is *actively in flight* — POST creating a thread,
   * SSE/WS handshaking, or connected-but-waiting-for-the-agent-ready frame.
   * Gates the "Starting session" card so it never lingers after disconnect()
   * (which nulls `threadStatus`, so a `threadStatus !== 'ended'` check alone
   * would render a fake spinner indefinitely).
   */
  readonly isStartingSession = computed(
    () =>
      !this.sessionReady() &&
      this.threadStatus() !== 'ended' &&
      this.threadStatus() !== 'ending' &&
      (this.isCreating() ||
        this.connectionState() === 'connecting' ||
        this.connectionState() === 'connected'),
  );

  // --- Reconnect surface (kept for back-compat with the resume banner UI).
  // EventSource handles reconnect natively, so these mostly stay quiet —
  // we only bump `reconnectAttempt` while the SSE is in CONNECTING after an
  // earlier OPEN, and only set `reconnectGaveUp` on terminal CLOSED.
  readonly reconnectAttempt = signal<number>(0);
  readonly reconnectGaveUp = signal<boolean>(false);
  readonly reconnectMaxAttempts = -1; // unbounded; browser owns the loop

  // --- Conversation state ---
  //
  // One signal holds the whole conversation as an ordered list of typed
  // Turns (user / assistant / system). Each AssistantTurn carries an
  // ordered list of typed events (thought | text | tool_call). The reducer
  // (./turn-reducer.ts) maps wire-level SSE events to state mutations and
  // is keyed by stable ids so SSE replay is idempotent.
  //
  // See `knowledge-base/knowledge/features/session_turn_rendering.md` for the design.
  readonly conversation = signal<ConversationState>(EMPTY_CONVERSATION);
  readonly turns = computed(() => this.conversation().turns);
  readonly currentStreamingTurn = computed<AssistantTurn | null>(() => {
    const state = this.conversation();
    if (!state.activeAssistantTurnId) return null;
    const t = state.turns.find(
      (turn): turn is AssistantTurn =>
        isAssistantTurn(turn) && turn.id === state.activeAssistantTurnId,
    );
    return t ?? null;
  });
  readonly isStreaming = computed(() => this.conversation().activeAssistantTurnId !== null);
  readonly isInterrupting = signal(false);
  readonly historyLoaded = signal(false);

  // --- Render windowing (display-only) ---
  // The full conversation lives in `turns`; the component renders only the
  // most recent `windowSize` turns so the DOM stays bounded on long threads
  // (a single thread can be 800+ turns). `loadOlderTurns` widens the window on
  // scroll-up; `growWindow` keeps the visible top anchored when new turns
  // stream in below a scrolled-away user; `resetWindow` re-bounds the DOM on
  // (re)load and once the user is back at the bottom.
  private readonly DEFAULT_WINDOW = 50;
  private readonly WINDOW_STEP = 50;
  readonly windowSize = signal(this.DEFAULT_WINDOW);
  readonly visibleTurns = computed(() => {
    const all = this.turns();
    const n = this.windowSize();
    return all.length <= n ? all : all.slice(-n);
  });
  readonly hasOlderTurns = computed(() => this.turns().length > this.windowSize());

  /** Widen the render window toward the start of the conversation. */
  loadOlderTurns(): void {
    this.windowSize.update((n) => Math.min(n + this.WINDOW_STEP, this.turns().length));
  }

  /** Anchor the visible top while turns stream in below a scrolled-away user. */
  growWindow(delta: number): void {
    if (delta > 0) this.windowSize.update((n) => n + delta);
  }

  /** Re-bound the DOM to the most recent window (on (re)load / back at bottom). */
  resetWindow(): void {
    this.windowSize.set(this.DEFAULT_WINDOW);
  }

  // --- Permission state ---
  readonly permissionMode = signal<PermissionMode>('supervised');
  /** Every gate currently awaiting a decision. A parallel tool batch puts
   *  all of its calls here at once so one card can list them —
   *  knowledge-base/knowledge/superpowers/specs/2026-08-01-batch-tool-approval-design.md. */
  readonly pendingPermissions = signal<PermissionRequest[]>([]);
  private readonly permissionResolutionFailures = new Set<string>();

  // --- Running-command snapshot ---
  // Set from the session.state welcome frame on (re)attach when the loop is
  // blocked in a tool call; cleared when that tool completes or the turn ends.
  // Lets the UI show a "running command" card instead of a blank "Connecting…"
  // during a long mid-turn block (the in-flight turn isn't in REST history).
  readonly runningTool = signal<RunningToolInfo | null>(null);

  // --- Narration state ---
  readonly narrationMode = signal<NarrationMode>('auto');

  // --- Workspace tier state (live settings pane) ---
  /** Current workspace tier when known. Initialized by the settings pane
   *  from thread metadata (`config_override.workspace.backend`); flipped by
   *  `workspace_upgrade.complete`. Null = not yet known. */
  readonly workspaceTier = signal<string | null>(null);
  /** In-flight tier upgrade (drives the pane's button/progress state);
   *  `elapsed` updates from workspace_upgrade.progress heartbeats. Only the
   *  vm tier emits those — `_emit_vm_progress` lives inside the vm branch and
   *  `_poll_workspace_ready` takes no progress_cb — so on the sandbox path
   *  (the only tier request_workspace_upgrade asks for) `elapsed` stays
   *  undefined and the spinner is silent. Fine: a sandbox is a container
   *  spawn, not a VM cold-import. */
  readonly workspaceUpgradeInProgress = signal<{ tier: string; elapsed?: number } | null>(null);
  /** A live agent-initiated upgrade offer (workspace_upgrade.needed), or null.
   *  Drives the inline offer card. Live-only: nothing replays it after a
   *  reload, and re-offering then would be wrong anyway — the agent's turn is
   *  long over and the settings pane still has the button. */
  readonly pendingWorkspaceOffer = signal<{ tier: string; reason: string } | null>(null);
  /** True while an accepted upgrade should auto-resume the agent once it
   *  lands. Means "the user hasn't spoken since accepting — resume for them";
   *  any user send clears it, because they've resumed it themselves. Rendered
   *  by the card, so the two accept buttons don't produce identical spinners. */
  readonly continueAfterUpgrade = signal(false);

  // --- Turn tracking ---
  /**
   * The numeric `turn_id` of the in-flight turn, or null when no turn is
   * streaming. Derived from the active turn in `conversation` — synthetic
   * turns (e.g. greetings) have non-numeric ids and resolve to null.
   */
  readonly currentTurnId = computed<number | null>(() => {
    const id = this.conversation().activeAssistantTurnId;
    if (!id) return null;
    const n = Number(id);
    return Number.isFinite(n) ? n : null;
  });
  readonly isWaitingForInput = signal(false);

  // --- Session metadata (loaded from REST on connect) ---
  readonly sessionTitle = signal<string | null>(null);
  readonly modelName = signal<string | null>(null);
  /** True on a commissioned background officer's own thread (metadata.config_override.officer.enabled). */
  readonly isOfficerThread = signal<boolean>(false);
  readonly temperature = signal<number>(0);
  readonly turnCount = signal<number>(0);
  readonly ncSessionFolder = signal<string | null>(null);
  readonly cloudSessionUrl = signal<string | null>(null);
  /** `Thread.ssh_handle` — minted once and static, so it rides the loaded
   *  Thread (below) alongside `cloudSessionUrl`, not the 10-second
   *  `IdeSessionStatus` poll. Feeds the session view's "Connect over SSH"
   *  panel. Null on threads predating migration 0202 or before the thread
   *  has loaded. */
  readonly sshHandle = signal<string | null>(null);

  // --- Protected cloud mode (Slice C, Task 14): status-bar badge + review
  //     drawer for the thread's staged cloud-diff. `protectedCloud` comes
  //     from the loaded Thread's metadata; the count + mount name come from
  //     the diff summary (refreshed on load and debounced after each
  //     turn.completed — see refreshCloudDiffCount / _scheduleCloudDiffRefresh). ---
  private readonly _protectedCloud = signal(false);
  readonly protectedCloud = computed(() => this._protectedCloud());
  readonly cloudChangesCount = signal(0);
  readonly protectedMountName = signal<string | null>(null);
  /** ISO timestamp the current staged diff was captured at (the summary's
   *  `staged_at`) — surfaces staging staleness in the badge tooltip. Null
   *  when nothing is staged (or the summary hasn't loaded yet). */
  readonly cloudStagedAt = signal<string | null>(null);
  readonly cloudDiffPanelOpen = signal(false);
  /** Remote folders attached to the thread, straight off the thread payload.
   *  The endpoint has returned these since cloud_collaboration_model.md
   *  Phase 1; nothing read them until PC-19 needed to name the folder the
   *  staged diff applies to. */
  readonly threadMounts = signal<ThreadMount[]>([]);
  /** A browser link to the protected project folder, or null when it cannot
   *  be derived with certainty. Consumers must still cross-check it against
   *  the diff summary's `protected_mount` (`folderLinkMatches`) before
   *  offering it — see protected-folder-link.ts. */
  readonly protectedFolderLink = signal<ProtectedFolderLink | null>(null);
  /**
   * The project-folder link, cross-checked against the mount the *summary*
   * says is protected. Only this may be offered as navigation.
   *
   * `protectedFolderLink` is a candidate derived from the thread's mount rows,
   * which the frontend cannot fully verify (`cloud_handle` is not in the REST
   * projection). The summary reports the mount the backend actually protected,
   * so an exact match is what turns the candidate into a fact. PC-19 was
   * caused by exactly the missing check: the header offered a legacy
   * `sessions/<id>` handle as if it were the project folder.
   */
  readonly verifiedProjectFolder = computed<ProtectedFolderLink | null>(() => {
    const link = this.protectedFolderLink();
    return folderLinkMatches(link, this.protectedMountName()) ? link : null;
  });
  /**
   * Outcome of the hidden pending-count probe.
   *
   * Needed because "no banner" used to mean both "nothing is staged" and "we
   * never found out". A protected ended session whose first probe failed had
   * no entry point to the review and no way to ask again — the PC-25 dead end
   * reached by a different road.
   */
  readonly cloudDiffProbe = signal<'idle' | 'loading' | 'ready' | 'error'>('idle');
  private cloudDiffRefreshTimer: ReturnType<typeof setTimeout> | null = null;
  private cloudDiffRequestOrdinal = 0;
  private terminalCloudProbeTimer: ReturnType<typeof setTimeout> | null = null;
  private terminalCloudProbeGeneration = 0;
  private terminalCloudProbeThreadId: string | null = null;
  private static readonly CLOUD_DIFF_REFRESH_DEBOUNCE_MS = 2000;

  // --- Lifecycle state from the row (drives the resume card) ---
  readonly threadStatus = signal<ThreadStatus | null>(null);
  readonly endedAt = signal<string | null>(null);
  /** Safe public projection of the immutable retirement outcome. The browser
   *  never receives or stores the server's retirement token/context. */
  readonly retirementDisposition = signal<'ended' | 'suspended' | null>(null);

  // --- Session readiness (agent has finished init and is ready for messages) ---
  readonly sessionReady = signal(false);

  // --- Startup progress phase (sent by orchestrator while waiting for agent) ---
  readonly startupPhase = signal<string | null>(null);

  /**
   * True when the session currently starting is VM-backed. A cold KubeVirt
   * boot runs minutes past a sandbox start, so this drives the longer
   * "Booting VM (this can take a few minutes)" startup copy and extends the
   * `/connection` readiness poll budget. Set from the create body
   * (createAndConnect) and reasserted by a `backend='vm'` lifecycle event
   * (covers resume); cleared on a genuine thread switch.
   */
  readonly isVmSession = signal(false);

  // --- Outbox: sends the user has committed but the server hasn't accepted
  //     yet. Owned by user intent, NOT the transport lifecycle — it survives
  //     disconnect/reconnect/thread-creation so a message typed on the
  //     "Creating thread" card is never swallowed. ---
  readonly outbox = signal<OutboxItem[]>([]);
  /** localIds of queued (not-yet-accepted) sends — drives queued-bubble
   *  styling in the template. */
  readonly outboxIds = computed(() => new Set(this.outbox().map((i) => i.localId)));
  /** One outbox item by its bubble's localId — the template's window into
   *  `pendingFiles` for the queued bubble's upload stage line (Task 5). */
  outboxItem(localId: string): OutboxItem | undefined {
    return this.outbox().find((i) => i.localId === localId);
  }
  /**
   * True when a flush attempt failed and items are still queued — i.e. the
   * queue is *stalled*, not merely waiting for the session to come up.
   * Because the flush has no timed auto-retry (see _flushOutbox), a stalled
   * queue needs either a reconnect or the user; this signal is what puts a
   * retry/discard affordance on the queued bubble instead of leaving it
   * spinning forever.
   */
  readonly outboxStalled = signal(false);
  /**
   * Sends the server has ACCEPTED whose turn hasn't started yet. The agent
   * 200s `/input` after persisting + enqueueing; if its loop is busy (e.g.
   * the previous turn's cloud push is still flushing) nothing reaches the
   * stream until `turn.started` — which used to read as a swallowed
   * message: outbox empty, no active turn, composer back to idle/mic.
   * +1 per accepted POST (never for a 409 conflict), −1 on
   * `turn.started` (clamped), zeroed by terminal frames and teardown.
   * knowledge-base/knowledge/issues/session_turn_end_cloud_push_blocks_queued_input.md
   */
  readonly pendingTurnCount = signal(0);
  /** True while an accepted send waits for its turn to start — drives the
   *  working placeholder, spinner and dots so a queued input is visibly
   *  alive instead of apparently swallowed. */
  /**
   * The thread's durable run_queue block (accept response, /connection, or the
   * awaiting poll), thread-stamped like `usage`: a value from another thread
   * reads as null. `parked` means the input was accepted but nothing can claim
   * it until it is retried — never rendered as "waiting".
   */
  private readonly _queueState = signal<{ threadId: string; queue: SessionQueueState } | null>(null);
  readonly queueState = computed<SessionQueueState | null>(() => {
    const q = this._queueState();
    if (!q || q.threadId == null) return null;
    return q.threadId === this.threadId() ? q.queue : null;
  });
  readonly isParked = computed(() => this.queueState()?.state === 'parked');
  /** A send is accepted and durably queued, but no agent has claimed it and
   *  the unit is not parked — the only state that shows the waiting bubble. */
  readonly isAwaitingTurn = computed(
    () => this.pendingTurnCount() > 0 && !this.isStreaming() && !this.isParked(),
  );
  /**
   * When the current awaiting stretch began (ms epoch); null while not
   * awaiting. Derived from isAwaitingTurn by a constructor effect, so every
   * pendingTurnCount reset (turn.started, terminal frames, teardown, thread
   * switch) ends the stretch without each site knowing. Feeds the queued
   * bubble's "busier than usual" escalation and elapsed counter — never a
   * queue position (deliberate: depth is not user-facing information).
   */
  readonly awaitingSince = signal<number | null>(null);
  private readonly awaitingNow = signal(Date.now());
  private awaitingTicker: ReturnType<typeof setInterval> | null = null;
  private queuePollTimer: ReturnType<typeof setInterval> | null = null;
  /** Milliseconds spent in the current awaiting stretch; 0 when not awaiting.
   *  Advances once a second, and only while awaiting. */
  readonly awaitingElapsedMs = computed(() => {
    const since = this.awaitingSince();
    return since === null ? 0 : Math.max(0, this.awaitingNow() - since);
  });

  private _stopAwaitingClock(): void {
    if (this.awaitingTicker !== null) {
      clearInterval(this.awaitingTicker);
      this.awaitingTicker = null;
    }
    if (this.awaitingSince() !== null) this.awaitingSince.set(null);
  }

  private _applyQueueState(threadId: string, queue: SessionQueueState | null | undefined): void {
    if (!queue || typeof queue !== 'object' || typeof queue.state !== 'string') return;
    this._queueState.set({ threadId, queue });
    this._reconcileReadinessFromDurableState();
  }

  /**
   * How many durable transcript rows this tab has for the open thread,
   * thread-stamped like `usage` / `_queueState` so another thread's count
   * reads as 0. Counts rows the server (or the append-only cache) actually
   * has — never the optimistic bubble a send paints before it is accepted.
   * Feeds `_reconcileReadinessFromDurableState`.
   */
  private readonly _durableMessages = signal<{ threadId: string; count: number } | null>(null);
  private readonly durableMessageCount = computed(() => {
    const seen = this._durableMessages();
    if (!seen) return 0;
    return seen.threadId === this.threadId() ? seen.count : 0;
  });

  private _recordDurableMessages(threadId: string, count: number): void {
    const seen = this._durableMessages();
    // Never let a narrower read (an `?after=` refresh that returned nothing)
    // shrink what a wider one already established for the same thread.
    if (seen?.threadId === threadId && seen.count >= count) return;
    this._durableMessages.set({ threadId, count });
    this._reconcileReadinessFromDurableState();
  }

  private _stopQueuePoll(): void {
    if (this.queuePollTimer !== null) {
      clearInterval(this.queuePollTimer);
      this.queuePollTimer = null;
    }
  }

  private async _pollQueueState(): Promise<void> {
    const tid = this.threadId();
    if (!tid) return;
    let queue: SessionQueueState | null = null;
    try {
      queue = await firstValueFrom(this.api.getThreadQueue(tid));
    } catch {
      // A failed poll is not news: the accept response / /connection already
      // seeded the state, and the next tick retries. Never let a timer throw.
      return;
    }
    if (this.threadId() !== tid) return;
    this._applyQueueState(tid, queue);
  }

  /**
   * Owner retry of a parked unit: POST /queue/retry, then show the normal
   * waiting state (the unit is queued again; a claim lands as turn.started).
   * A refusal (stop markers, claim-loss hold, not parked) is toasted with the
   * server's code and leaves the parked bubble in place.
   */
  async retryParked(): Promise<'ok' | 'refused'> {
    const tid = this.threadId();
    if (!tid) return 'refused';
    const outcome = await firstValueFrom(this.api.retryThreadQueue(tid));
    if (this.threadId() !== tid) return 'refused';
    if (outcome.kind === 'ok') {
      this._applyQueueState(tid, {
        state: outcome.state || 'queued',
        park_reason: null,
        parked_at: null,
        retryable: false,
        attempts: 0,
        pending_input: true,
      });
      this.pendingTurnCount.update((c) => Math.max(c, 1));
      return 'ok';
    }
    this.toast.danger(this.transloco.translate('chat.parked.retryFailed', { code: outcome.code }));
    return 'refused';
  }
  /**
   * Single-flight guard for _flushOutbox — one POST in flight **per thread**,
   * not per tab. `turn_id` is per-thread, so two different threads can never
   * collide on it; the lock only has to stop two flushes racing within one
   * thread.
   *
   * Per-tab was equivalent until the upload moved inside the lock. An upload
   * is not cancelled by a thread switch (Slice 3 owns cancellation), so a
   * tab-wide lock would be held for the whole remainder of an abandoned
   * upload — and the thread the user just opened could not POST at all
   * meanwhile, leaving its sends silently "queued" for an unbounded time.
   *
   * The value is an opaque token, not a boolean: a stale flush's `finally`
   * must only be able to release the lock it actually took.
   */
  private flushTokens = new Map<string, object>();
  // localIds whose POST is currently in flight (skipped by horizon
  // re-dispatch, since accept-time persistence may already have put their row
  // in the reloaded history).
  //
  // SETS, not scalars: per-thread locking means two flushes coexist by design
  // — one suspended in an upload the user navigated away from, another
  // draining the thread they just opened. A scalar marker would let whichever
  // flush finishes first clear the other's, un-refusing a discard whose bytes
  // or POST are still on the wire. Each flush only ever adds and removes its
  // own localId, so the two cannot interfere.
  private postingLocalIds = new Set<string>();
  // localIds whose upload stage is running. Separate set from the POST's:
  // both windows must refuse a discard, but for different reasons — an
  // in-flight POST's fate isn't decided, while dropping a mid-upload item
  // orphans bytes in the workspace that no endpoint can delete. Only the POST
  // set gates the horizon re-dispatch, because only a POST can have put a row
  // in the reloaded history; a mid-upload bubble MUST be re-dispatched or it
  // disappears.
  private uploadingLocalIds = new Set<string>();

  // --- Pending attachments (queued in composer before send) ---
  readonly pendingAttachments = signal<FilePreview[]>([]);

  // --- Last upload error (cleared on next successful send) ---
  readonly attachmentError = signal<string | null>(null);

  // --- Session tasks ---
  readonly tasks = signal<SessionTask[]>([]);

  // --- File undo ---
  readonly undoAvailable = signal(false);

  // --- Rewind (knowledge-base/knowledge/features/session_rewind.md) ---
  /** Prompt text handed back by rewind.ack — the component moves it into
   *  the composer (edit-and-resend) and clears the signal. */
  readonly rewindPrefill = signal<RewindPrefill | null>(null);
  readonly rewindInFlight = signal<boolean>(false);
  readonly rewindPreview = signal<StatelessRewindPreview | null>(null);
  readonly rewindPreviewLoading = signal(false);
  readonly rewindOutcomeUnknown = signal(false);
  readonly conversationRevision = signal(0);

  // --- Cloud sync degraded (initial cloud->workspace seed failed) ---
  /**
   * True when this session's initial cloud->workspace sync failed: the
   * workspace may be missing files from the cloud, and edits will NOT be
   * saved back to the cloud for the session's lifetime. Sticky for the
   * session (reset on each (re)connect). See knowledge-base/knowledge/issues/main_cloud.md
   * Issue 13.
   */
  readonly cloudSyncDegraded = signal(false);
  /** Id of the sticky degraded toast, so a later recovery can dismiss the
   *  exact one it raised rather than clearing every toast on screen. */
  private cloudSyncDegradedToastId: number | null = null;

  // --- Creating state (thread being created via API before connect) ---
  readonly isCreating = signal(false);

  /**
   * True for the whole resume window — POST /resume through connect(). The
   * composer stays open across it: `isStartingSession` deliberately excludes
   * `threadStatus === 'ended'`, so without this the box would disable itself
   * the instant the user sends and re-enable a moment later.
   */
  readonly isResuming = signal(false);

  /**
   * Non-null while a resume is blocked on config drift (a connector,
   * project, or grant the thread depended on has since disappeared). Set
   * from the 428 the server returns from POST /resume; consumed by the
   * drift-acknowledgment dialog (Task 10), which re-calls resumeSession()
   * with the acknowledged ids once the user confirms.
   */
  readonly pendingDrift = signal<ConfigDriftItem[] | null>(null);

  // --- Draft session (instant landing at `/`) ---
  // The composer is open but no thread exists yet; the first send creates
  // the session with a minimal body and the orchestrator resolves the
  // owner's defaults (knowledge-base/knowledge/features/instant_landing_session.md). Distinct
  // from the composer's persisted text "draft" (localStorage).
  readonly isDraftSession = signal(false);
  // Resolve the default project and session admission before enabling Send.
  private draftProjectIds: string[] | null = null;
  /** Stable, reviewable default selection for the landing draft. Null while
   * the default-project/workspace context is unresolved. */
  readonly draftDatasourceIds = signal<string[] | null>(null);
  readonly draftDefaultsLoading = signal(false);
  readonly draftDefaultsError = signal(false);
  readonly draftConnectorsEnabled = signal(true);
  /** Empty means follow the project/account defaults, including workspace recipes. */
  readonly draftWorkspaceBackend = signal('');
  private draftDefaultsGeneration = 0;
  private creatingFromDraft = false;

  // --- Session paused (idle timeout received) ---
  readonly isSessionPaused = signal(false);

  // --- Error ---
  readonly error = signal<string | null>(null);

  private sse: EventSource | null = null;
  private sseWatchdogTimer: ReturnType<typeof setInterval> | null = null;
  // One-shot fallback: force a reconnect if "Stopping…" doesn't clear after
  // an interrupt POST (lost ack frame). Armed in interrupt(), cleared by the
  // isInterrupting invariant effect.
  private interruptFallbackTimer: ReturnType<typeof setTimeout> | null = null;
  /** Exact turn-scoped interrupt admission. Ambiguous HTTP outcomes retry
   * with this same UUID + target; a late acknowledgement can therefore
   * never be applied to whichever turn happens to be active later. */
  private pendingInterruptRequest: PendingInterruptRequest | null = null;
  private interruptAdmissionInFlight: PendingInterruptRequest | null = null;
  private interruptAdmissionSubscription: Subscription | null = null;
  private interruptRetryTimer: ReturnType<typeof setTimeout> | null = null;
  // One-shot fallback: force-clear rewindInFlight if rewind.ack (direct to
  // the initiator's socket only) never arrives. Armed in rewind(), cleared
  // by rewind.ack / rewind.files_restored / a request_id-matching error,
  // and on connect()/disconnect() teardown (see REWIND_ACK_TIMEOUT_MS).
  private rewindAckFallbackTimer: ReturnType<typeof setTimeout> | null = null;
  // request_id of the rewind currently in flight, so the generic 'error'
  // case can clear rewindInFlight only for a matching rewind — an
  // unrelated in-flight error (e.g. a concurrent config.update denial)
  // must not prematurely re-enable the rewind UI.
  private pendingRewindRequestId: string | null = null;
  private pendingRewindDraftSnapshot = '';
  private pendingRewindDraftRevision = 0;
  private historyLoadGeneration = 0;
  private rewindRefresh: Promise<void> | null = null;
  private observedHistoryEpoch = '';
  // One-shot send-liveness kickstart: force a reopen if a send is accepted
  // but no SSE data frame follows (see SEND_KICKSTART_TIMEOUT_MS). Armed in
  // _postInput, cleared in disconnect().
  private sendKickstartTimer: ReturnType<typeof setTimeout> | null = null;
  private sseLastEventAt = 0;
  // Timestamp of the last SSE *data* frame — bumped ONLY in _handleSseFrame.
  // Deliberately distinct from sseLastEventAt (bumped by onopen-reset + pings)
  // and agentLastEventAt (bumped by control-WS frames too): those signals get
  // refreshed with zero real data in exactly the zombie-stream case, so the
  // send-kickstart must key off this clean data-only clock.
  private sseDataLastAt = 0;
  // True for the current explicit SSE open when replay starts from a cursor.
  // The durable snapshot supplies a full-turn replay floor; the legacy
  // pinned fallback can still use a browser cursor and join only its suffix.
  private sseOpenedWithCursor = false;
  // The DB-authoritative REST snapshot was applied for this connection.
  // A socketless /connection response may unblock the composer only after
  // this is true; otherwise a supervised gate could be live but invisible.
  private sessionSnapshotLoaded = false;
  private sessionSnapshotFailed = false;
  // Journal high-water covered by the durable snapshot. Replay starts from
  // its turn-boundary floor, but stateful frames at/below this cursor must
  // not overwrite the newer permission/runtime scalars we just hydrated.
  private sessionSnapshotCursor: { epoch: number; seq: number } | null = null;
  // Same-thread reconnects retain their already-rendered live turn. When the
  // tab's own cursor is newer than the snapshot's turn-boundary floor, the
  // snapshot reopens that retained turn and SSE resumes after the tab's last
  // folded frame instead of replaying/doubling the prefix.
  private snapshotJoinsPreservedTurn = false;
  // True once the durable snapshot has hydrated `usage` for this connect.
  // Only then may replayed `usage.updated` frames at/below the cursor be
  // dropped as already-counted. A rolling-deploy peer that predates the
  // snapshot's `usage` key seeds nothing, and dropping its covered frames
  // would leave the panel blank after a reload until the next LLM call.
  private snapshotSeededUsage = false;
  // Tab-local resume point. IndexedDB is shared by every tab, so consulting
  // it again after the snapshot lets another tab advance us past a state
  // change this tab has not applied. `undefined` means no snapshot supplied
  // a floor and _openSse should best-effort fall back to IndexedDB; `null`
  // deliberately opens with the server's no-cursor behavior.
  private sseReplayCursor: { epoch: number; seq: number } | null | undefined;
  // Single-flight generation for _openSse. Bumped at every open attempt and in
  // disconnect(); an open whose generation is stale after its async cursor
  // fetch bails instead of installing a resurrected EventSource on a
  // closed/superseded session.
  private sseGeneration = 0;
  // Whole-connect generation. Route reuse can start connect(B) while an
  // earlier connect(A) is still awaiting cache/REST. Every async state apply
  // in the cold-connect path is tied to this generation so A can never paint
  // history/metadata or reclaim B's transports after it has been superseded.
  private connectGeneration = 0;
  // Wall-clock time the tab was last hidden (visibilitychange). A hide longer
  // than the watchdog window forces an unconditional revalidate on return — a
  // socket that died while the tab was frozen still reports readyState===OPEN.
  private hiddenAt = 0;

  // --- Streamed-delta coalescing (de-flicker) ---
  // token/thinking deltas buffer here and fold into `conversation` at most
  // once per DELTA_FLUSH_MS via a single signal write, instead of one
  // change-detection pass per token. Any non-delta action flushes first, so
  // wire order is preserved and no buffered delta outlives a turn boundary.
  private deltaQueue: Extract<ReducerAction, { type: 'token' | 'thinking' }>[] = [];
  private deltaFlushTimer: ReturnType<typeof setTimeout> | null = null;
  private static readonly DELTA_FLUSH_MS = 80;
  private controlWs: WebSocket | null = null;
  // Transport capability discovered from /connection. ``none`` is a stable
  // ready state, not a failed WebSocket open; remember it so focus/SSE
  // recovery and user actions cannot restart the reconnect ladder.
  private controlSocket: 'unknown' | 'websocket' | 'none' = 'unknown';
  /** The `/connection` control declaration, stamped with the thread it
   *  describes so a stale map can never answer for the next session
   *  (singleton-state rule: stamp, don't just reset). null = not resolved
   *  yet, or an older orchestrator that omits `controls`. */
  private readonly controlCapabilities = signal<{
    threadId: string;
    controls: ControlCapabilityMap;
    options: ControlOptionsMap;
  } | null>(null);
  /** Socket-sent config updates awaiting their `config.changed` ack (or a
   *  matching error frame), keyed by request_id. Settled by the frame
   *  reducer, by the ack timeout, or by disconnect(). */
  private readonly pendingConfigUpdates = new Map<
    string,
    {
      threadId: string;
      resolve: (outcome: ConfigUpdateOutcome) => void;
      timer: ReturnType<typeof setTimeout>;
    }
  >();
  private controlWsReconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private controlWsReconnectAttempt = 0;
  private controlWsLastMessageAt = 0;
  private controlWsWatchdogTimer: ReturnType<typeof setInterval> | null = null;
  /** Latest committed user edit waiting for a reconnecting control socket. */
  private pendingCanvasSourceUpdate: {
    threadId: string;
    control: CanvasSourceUpdatedControl | CanvasPresentationUpdatedControl;
  } | null = null;
  /**
   * Control frames issued while the control WS wasn't OPEN, tagged with the
   * thread they were issued for. Drained by _installControlWs's onopen.
   *
   * Queued on the service rather than on a socket object on purpose — see
   * _sendControl for why the socket is the wrong place to hang them.
   */
  private controlOutbox: { threadId: string; frame: string }[] = [];
  /** Depth cap for `controlOutbox`. These are user clicks, so the realistic
   *  depth is 1–2; the cap only stops a wedged socket growing it forever. */
  private static readonly CONTROL_OUTBOX_MAX = 32;
  /**
   * REST controls are serialized separately from the WebSocket outbox. A
   * transient/ambiguous failure keeps the head in place and retries the same
   * client_request_id, so a later setting cannot overtake a request that may
   * already have committed server-side.
   */
  private durableControlOutbox: DurableControlOutboxItem[] = [];
  private durableControlInFlight: DurableControlOutboxItem | null = null;
  private durableControlSubscription: Subscription | null = null;
  private durableControlRetryTimer: ReturnType<typeof setTimeout> | null = null;
  private static readonly DURABLE_CONTROL_RETRY_DELAYS_MS = [250, 1000, 2000, 4000];
  private static readonly DURABLE_CONTROL_RESPONSE_TIMEOUT_MS = 15_000;
  private static readonly DURABLE_CONTROL_OUTBOX_MAX = 32;
  private durableControlOrdinal = 0;
  private durableControlAwaitingAck = new Map<string, DurableControlMarker>();
  private durableControlError: DurableControlError | null = null;
  private intentionalClose = false;
  /**
   * Guard against double-opening the control WS while an async
   * /connection (or /prepare → SSE ready → /connection) fetch is in
   * flight. Distinct from `controlWs` (which is null during the fetch
   * window — _ensureControlWs() must not race past).
   */
  private controlWsOpening = false;
  private controlWsOpeningGeneration = 0;
  /** Current thread whose runtime/control plane is retiring or retired.
   *  Its EventSource and protected-review state intentionally remain live. */
  private terminalControlThreadId: string | null = null;
  /** Exact server-issued authority for the current session life. */
  private sessionRuntimeGeneration: string | null = null;
  /** Non-retryable binding refusal for one exact runtime generation. It
   *  fences only the control plane; SSE, history and protected review remain
   *  live. An authoritative Resume or thread switch clears it. */
  private invalidBindingRuntime: { threadId: string; generation: string } | null = null;
  /** A different-G lifecycle edge may wake one exact connection retry, but
   *  is not itself enough to install that generation as control authority. */
  private bindingRecoveryRuntime: {
    threadId: string;
    rejectedGeneration: string;
    candidateGeneration: string;
  } | null = null;
  /** Most recently terminal runtime on this same-thread review plane. */
  private retiredRuntimeGeneration: string | null = null;
  /** Thread on which the exact runtime-generation transport contract has
   *  been observed. Keep this across same-thread reconnect/Resume gaps so a
   *  delayed legacy-shaped lifecycle frame cannot mutate the successor UI. */
  private readonly runtimeGenerationContractThreads = new Set<string>();

  private _canonicalRuntimeGeneration(value: unknown): string | null {
    if (
      typeof value !== 'string' ||
      !/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value)
    ) {
      return null;
    }
    return value.toLowerCase();
  }

  /**
   * Runtime-scoped, non-terminal journal frames (currently suspend) must be
   * tied to the exact installed runtime. Missing generations remain a
   * deliberate rolling-deploy compatibility path only until this thread has
   * advertised the exact contract once. A terminally retired thread never
   * accepts a suspend frame: End is the stronger lifecycle fact.
   */
  private _runtimeSessionFrameApplies(params: Record<string, unknown>): boolean {
    const threadId = this.threadId();
    if (!threadId) return false;
    const rawGeneration = params['session_runtime_generation'];
    const eventGeneration = this._canonicalRuntimeGeneration(rawGeneration);
    if (rawGeneration !== undefined && !eventGeneration) return false;
    const invalidBinding = this.invalidBindingRuntime;
    if (invalidBinding?.threadId === threadId) {
      return eventGeneration === invalidBinding.generation;
    }
    const bindingRecovery = this.bindingRecoveryRuntime;
    if (bindingRecovery?.threadId === threadId) {
      // Observing a canonical successor makes the rejected generation's
      // delayed journal tail superseded even though /connection has not yet
      // installed the candidate. Only the candidate can cancel its retry.
      return eventGeneration === bindingRecovery.candidateGeneration;
    }
    // A settled suspended frame is authoritative for a control plane already
    // retired by soft End. It must match the retired runtime, not be rejected
    // merely because the ending latch is closed.
    if (this.terminalControlThreadId === threadId) {
      if (this.retiredRuntimeGeneration !== null) {
        return eventGeneration === this.retiredRuntimeGeneration;
      }
      if (eventGeneration !== null) return true;
      if (this.runtimeGenerationContractThreads.has(threadId)) {
        return false;
      }
      return rawGeneration === undefined;
    }
    if (this.sessionRuntimeGeneration !== null) {
      return eventGeneration === this.sessionRuntimeGeneration;
    }
    if (this.runtimeGenerationContractThreads.has(threadId)) return false;
    return rawGeneration === undefined;
  }

  private _isSessionEndedError(err: unknown): boolean {
    const failure = err as {
      status?: unknown;
      error?: { detail?: { code?: unknown } };
    };
    return failure?.status === 409 && failure.error?.detail?.code === 'session_ended';
  }

  private _isSessionEndingError(err: unknown): boolean {
    const failure = err as {
      status?: unknown;
      error?: { detail?: { code?: unknown } };
    };
    return failure?.status === 409 && failure.error?.detail?.code === 'session_ending';
  }

  private _sessionTerminalGeneration(err: unknown): string | null {
    const failure = err as {
      status?: unknown;
      error?: {
        detail?: {
          code?: unknown;
          pinned_runtime_generation_contract?: unknown;
          session_runtime_generation?: unknown;
        };
      };
    };
    const detail = failure?.error?.detail;
    if (
      failure?.status !== 409 ||
      !['session_ending', 'session_ended'].includes(String(detail?.code ?? '')) ||
      detail?.pinned_runtime_generation_contract !== 1
    ) {
      return null;
    }
    return this._canonicalRuntimeGeneration(detail.session_runtime_generation);
  }

  private _retireFromSessionRefusal(threadId: string, err: unknown): void {
    const exactGeneration = this._sessionTerminalGeneration(err);
    if (exactGeneration !== null) {
      this.runtimeGenerationContractThreads.add(threadId);
    }
    const recovery = this.bindingRecoveryRuntime;
    if (recovery?.threadId === threadId && exactGeneration === null) {
      // Once a successor candidate exists, a legacy terminal response cannot
      // identify which life ended. Restore the exact refusal and wait for a
      // generation-bound response/event instead of guessing the candidate.
      this._latchSessionBindingInvalid(threadId, recovery.rejectedGeneration);
      return;
    }
    if (this._isSessionEndingError(err)) {
      const detail = (err as { error?: { detail?: { retirement_disposition?: unknown } } })?.error
        ?.detail;
      this._retireEndingControl(threadId, detail?.retirement_disposition, exactGeneration);
    } else {
      this._retireTerminalControl(threadId, exactGeneration);
    }
  }

  private _sessionBindingInvalidGeneration(err: unknown): string | null {
    const failure = err as {
      status?: unknown;
      error?: {
        detail?: {
          code?: unknown;
          pinned_runtime_generation_contract?: unknown;
          session_runtime_generation?: unknown;
        };
      };
    };
    const detail = failure?.error?.detail;
    if (
      failure?.status !== 409 ||
      detail?.code !== 'session_binding_invalid' ||
      detail?.pinned_runtime_generation_contract !== 1
    ) {
      return null;
    }
    return this._canonicalRuntimeGeneration(detail.session_runtime_generation);
  }

  private _latchSessionBindingInvalid(threadId: string, generation: string): void {
    if (this.threadId() !== threadId) return;
    this.bindingRecoveryRuntime = null;
    this.invalidBindingRuntime = { threadId, generation };
    this.runtimeGenerationContractThreads.add(threadId);
    this.sessionRuntimeGeneration = null;
    this._retireMomentControlPlane();
    this.sessionReady.set(false);
    this.startupPhase.set(null);
    this.connectionState.set('error');
    this.error.set(this.transloco.translate('errors.sessions.bindingInvalid'));
  }

  /** Decide whether an exact input refusal still names the runtime that sent
   *  the request. A POST may finish after terminal retirement or after a G2
   *  connection has installed; that delayed G1 response must keep its outbox
   *  item honest without poisoning the successor control plane. */
  private _inputBindingRefusalApplies(
    threadId: string,
    refusedGeneration: string,
    sentGeneration: string | null,
    sentControlEpoch: number,
  ): boolean {
    if (
      this.threadId() !== threadId ||
      this.terminalControlThreadId === threadId ||
      this.bindingRecoveryRuntime?.threadId === threadId
    ) {
      return false;
    }
    const alreadyInvalid = this.invalidBindingRuntime;
    if (alreadyInvalid?.threadId === threadId) {
      return alreadyInvalid.generation === refusedGeneration;
    }
    if (sentGeneration !== null) {
      return (
        refusedGeneration === sentGeneration && this.sessionRuntimeGeneration === sentGeneration
      );
    }
    // Legacy control connections did not expose G to the browser. They may
    // still receive the exact refusal from an upgraded orchestrator, but only
    // while no terminal/reopen epoch or exact successor has superseded the
    // send.
    return (
      this.sessionRuntimeGeneration === null && this.controlWsOpeningGeneration === sentControlEpoch
    );
  }

  private _isTurnInFlightEndError(err: unknown): boolean {
    const failure = err as {
      status?: unknown;
      error?: { detail?: { code?: unknown } };
    };
    return failure?.status === 409 && failure.error?.detail?.code === 'turn_in_flight';
  }

  private _controlPlaneAllowed(threadId: string, openingGeneration?: number): boolean {
    return (
      !this.intentionalClose &&
      this.threadId() === threadId &&
      this.terminalControlThreadId !== threadId &&
      this.invalidBindingRuntime?.threadId !== threadId &&
      (openingGeneration === undefined || openingGeneration === this.controlWsOpeningGeneration)
    );
  }

  private _cancelTerminalCloudDiffProbe(): void {
    this.terminalCloudProbeGeneration++;
    this.terminalCloudProbeThreadId = null;
    if (this.terminalCloudProbeTimer) {
      clearTimeout(this.terminalCloudProbeTimer);
      this.terminalCloudProbeTimer = null;
    }
  }

  private _scheduleTerminalCloudDiffProbe(threadId: string): void {
    this._cancelTerminalCloudDiffProbe();
    if (!this._protectedCloud() || this.threadId() !== threadId) return;
    this.terminalCloudProbeThreadId = threadId;
    const generation = this.terminalCloudProbeGeneration;
    let index = 0;
    const scheduleNext = (): void => {
      if (
        generation !== this.terminalCloudProbeGeneration ||
        this.threadId() !== threadId ||
        this.terminalControlThreadId !== threadId ||
        index >= POST_TERMINAL_CLOUD_PROBE_DELAYS_MS.length
      ) {
        return;
      }
      const delay = POST_TERMINAL_CLOUD_PROBE_DELAYS_MS[index++];
      this.terminalCloudProbeTimer = setTimeout(() => {
        this.terminalCloudProbeTimer = null;
        if (
          generation !== this.terminalCloudProbeGeneration ||
          this.threadId() !== threadId ||
          this.terminalControlThreadId !== threadId
        ) {
          return;
        }
        void this.refreshCloudDiffCount().finally(() => {
          if (
            generation === this.terminalCloudProbeGeneration &&
            this.threadId() === threadId &&
            this.terminalControlThreadId === threadId
          ) {
            scheduleNext();
          }
        });
      }, delay);
    };
    scheduleNext();
  }

  /**
   * A staged-diff event is only a wake-up edge; the summary endpoint remains
   * the authority for every byte/count shown to the owner. Runtime identity
   * still matters because the same thread can be ended and resumed: a late
   * event from G1 must not perturb G2. A cold ended view may not know the
   * retired generation, so it may use its thread-scoped durable event solely
   * to trigger that authoritative read while the terminal latch is closed.
   */
  private _cloudDiffStagedEventApplies(params: {
    thread_id?: unknown;
    session_runtime_generation?: unknown;
  }): boolean {
    const threadId = this.threadId();
    if (!threadId || params['thread_id'] !== threadId) return false;
    const eventGeneration = this._canonicalRuntimeGeneration(params['session_runtime_generation']);
    if (!eventGeneration) return false;
    const invalidBinding = this.invalidBindingRuntime;
    if (invalidBinding?.threadId === threadId) {
      return eventGeneration === invalidBinding.generation;
    }
    const bindingRecovery = this.bindingRecoveryRuntime;
    if (bindingRecovery?.threadId === threadId) {
      // This event is only a summary-refetch edge. Either known side of the
      // handoff may have published the still-reviewable staged receipt; the
      // endpoint decides which durable bytes are current.
      return (
        eventGeneration === bindingRecovery.rejectedGeneration ||
        eventGeneration === bindingRecovery.candidateGeneration
      );
    }
    if (this.sessionRuntimeGeneration !== null) {
      return eventGeneration === this.sessionRuntimeGeneration;
    }
    if (this.terminalControlThreadId === threadId) {
      return (
        this.retiredRuntimeGeneration === null || eventGeneration === this.retiredRuntimeGeneration
      );
    }
    return false;
  }

  /** Close and forget every command channel or intent scoped to one runtime
   *  moment. The transcript/SSE/review plane is deliberately untouched: a
   *  binding refusal can arrive while a durable staged review is still the
   *  owner's only useful surface. */
  private _retireMomentControlPlane(): void {
    this.controlWsOpeningGeneration++;
    this.controlWsOpening = false;
    this.controlSocket = 'unknown';
    if (this.controlWsReconnectTimer) {
      clearTimeout(this.controlWsReconnectTimer);
      this.controlWsReconnectTimer = null;
    }
    this.controlWsReconnectAttempt = 0;
    this._stopControlWsWatchdog();
    const ws = this.controlWs;
    this.controlWs = null;
    if (ws) {
      ws.onopen = null;
      ws.onmessage = null;
      ws.onerror = null;
      ws.onclose = null;
      try {
        ws.close(1000);
      } catch {
        // Already closed.
      }
    }

    this.controlOutbox = [];
    this.pendingCanvasSourceUpdate = null;
    this._clearPendingInterruptRequest();
    this._clearDurableControlOutbox();
    this.pendingPermissions.set([]);
    this.permissionResolutionFailures.clear();
    this.workspaceUpgradeInProgress.set(null);
    this.pendingWorkspaceOffer.set(null);
    this.continueAfterUpgrade.set(false);
    this.pendingRewindRequestId = null;
    this.rewindInFlight.set(false);
    this._clearRewindAckFallback();
  }

  /** Retire only the moment-scoped runtime/control plane for a life that is
   *  ending or settled. The EventSource, transcript, metadata, review panel
   *  and staged receipt deliberately remain intact so late staging stays
   *  visible. `ending` is not resumable and must never fabricate endedAt. */
  private _retireRuntimeControl(
    threadId: string,
    lifecycle: 'ending' | 'ended',
    exactFrameGeneration: string | null = null,
  ): void {
    if (this.threadId() !== threadId) return;
    const invalidBindingGeneration =
      this.invalidBindingRuntime?.threadId === threadId
        ? this.invalidBindingRuntime.generation
        : null;
    const recoveryGeneration =
      this.bindingRecoveryRuntime?.threadId === threadId
        ? this.bindingRecoveryRuntime.candidateGeneration
        : null;
    if (this.invalidBindingRuntime?.threadId === threadId) {
      this.invalidBindingRuntime = null;
      if (this.error() === this.transloco.translate('errors.sessions.bindingInvalid')) {
        this.error.set(null);
      }
    }
    if (this.bindingRecoveryRuntime?.threadId === threadId) {
      this.bindingRecoveryRuntime = null;
    }
    const firstRetirement = this.terminalControlThreadId !== threadId;
    this.terminalControlThreadId = threadId;
    if (exactFrameGeneration) {
      this.retiredRuntimeGeneration = exactFrameGeneration;
    } else if (this.sessionRuntimeGeneration) {
      this.retiredRuntimeGeneration = this.sessionRuntimeGeneration;
    } else if (invalidBindingGeneration) {
      this.retiredRuntimeGeneration = invalidBindingGeneration;
    } else if (recoveryGeneration) {
      this.retiredRuntimeGeneration = recoveryGeneration;
    }
    this.sessionRuntimeGeneration = null;
    this.sessionReady.set(false);
    this.startupPhase.set(null);
    this.connectionState.set('disconnected');
    // Terminal observation is also a turn boundary. Preserve every buffered
    // token before closing the visible turn so owner End, typed REST 409 and
    // REST-observed End converge with the explicit session.ended frame.
    this._flushDeltas();
    this._closeActiveTurnIfAny('turn_interrupted');
    this.isWaitingForInput.set(false);
    this.pendingTurnCount.set(0);
    // All command state belongs to the retired runtime moment. None may
    // flush into the generation created by an explicit Resume.
    this._retireMomentControlPlane();

    if (lifecycle === 'ended') {
      this.threadStatus.set('ended');
      if (!this.endedAt()) this.endedAt.set(new Date().toISOString());
    } else if (!['ended', 'suspended'].includes(this.threadStatus() ?? '')) {
      this.threadStatus.set('ending');
      this.endedAt.set(null);
    }
    if (
      this._protectedCloud() &&
      (firstRetirement || this.terminalCloudProbeThreadId !== threadId)
    ) {
      this._scheduleTerminalCloudDiffProbe(threadId);
    }
  }

  private _retireEndingControl(
    threadId: string,
    disposition: unknown = null,
    exactFrameGeneration: string | null = null,
  ): void {
    if (disposition === 'ended' || disposition === 'suspended') {
      this.retirementDisposition.set(disposition);
    }
    this._retireRuntimeControl(threadId, 'ending', exactFrameGeneration);
  }

  private _retireTerminalControl(
    threadId: string,
    exactFrameGeneration: string | null = null,
  ): void {
    this._retireRuntimeControl(threadId, 'ended', exactFrameGeneration);
  }

  private _settleSuspendedControl(
    threadId: string,
    exactFrameGeneration: string | null = null,
  ): void {
    this.retirementDisposition.set('suspended');
    this._retireRuntimeControl(threadId, 'ending', exactFrameGeneration);
    if (this.threadId() !== threadId || this.threadStatus() === 'ended') return;
    this.threadStatus.set('suspended');
    this.endedAt.set(null);
  }

  private _reopenTerminalControl(threadId: string): void {
    if (this.threadId() !== threadId) return;
    if (this.invalidBindingRuntime?.threadId === threadId) {
      this.invalidBindingRuntime = null;
    }
    if (this.bindingRecoveryRuntime?.threadId === threadId) {
      this.bindingRecoveryRuntime = null;
    }
    this._cancelTerminalCloudDiffProbe();
    if (this.terminalControlThreadId === threadId) {
      this.terminalControlThreadId = null;
      this.controlWsOpeningGeneration++;
      this.controlWsOpening = false;
      this.controlSocket = 'unknown';
      this.sessionRuntimeGeneration = null;
    }
    this.retirementDisposition.set(null);
  }

  /**
   * Connect to a persistent agent session.
   *
   * Cold path (new thread or thread switch): load thread metadata +
   * transcript history from REST, then open SSE (replay-from-cursor when
   * we have one cached) and the control WS.
   *
   * Same-thread fast path: skip the reset + loadHistory, just refresh
   * transports. This preserves the in-flight assistant turn through
   * chat-page re-mounts and other reconnects against the same thread
   * (knowledge-base/knowledge/issues/persistent_chat_lost_assistant_turn_on_mid_turn_reload.md
   * §Approach 1). loadHistory's GET /messages only returns *persisted*
   * rows; during streaming the AI message isn't fully in thread_messages
   * yet, so re-running it mid-turn would reset state and replace the
   * visible streaming turn with just its persisted prefix. Subsequent SSE
   * events arriving without an active turn are no longer dropped — since
   * §Approach 2, `ensurePlaceholderTurn` absorbs them into a synthetic
   * `recovered:` bubble — but that recovery is a fallback, not a reason to
   * blow away the live turn here.
   */
  async connect(
    threadId: string,
    opts: { carryOutbox?: boolean; preserveReviewPlane?: boolean } = {},
  ): Promise<void> {
    const previousThreadId = this.threadId();
    if (previousThreadId !== threadId) {
      this.invalidBindingRuntime = null;
      this.bindingRecoveryRuntime = null;
    }
    const bindingInvalid = this.invalidBindingRuntime?.threadId === threadId;
    const sameThread = previousThreadId === threadId && this.historyLoaded();
    const preservedReplayCursor = sameThread ? this.sseReplayCursor : undefined;
    const preserveReviewPlane = opts.preserveReviewPlane === true && previousThreadId === threadId;
    this.disconnect({ preserveReviewPlane });
    const generation = this.connectGeneration;
    this.isDraftSession.set(false);
    this.connectionState.set(bindingInvalid ? 'error' : 'connecting');
    this.error.set(
      bindingInvalid ? this.transloco.translate('errors.sessions.bindingInvalid') : null,
    );
    this.cloudSyncDegraded.set(false);
    this.cloudSyncDegradedToastId = null;
    if (!sameThread) {
      // Cold path: wipe and refetch.
      this.dispatch({ type: 'reset', threadId });
      this.historyLoaded.set(false);
      this.sessionReady.set(false);
      this.startupPhase.set(null);
      // A genuine thread switch is the ONLY place the outbox is cleared
      // wholesale — unless we're carrying it across a create/reprovision
      // (createAndConnect), where the queued sends belong to *this* thread.
      if (!opts.carryOutbox) this.outbox.set([]);
      // Same reason for anything still sitting in the composer: on a
      // genuine switch the chips must not follow the user (nor may their
      // bytes — see _discardComposerAttachments).
      //
      // Gated on the thread ID CHANGING, not on `!sameThread`. The cold
      // path also runs for the same thread when history has not loaded —
      // a failed connect the user retries — and discarding there would
      // take their staged attachments away, and DELETE the bytes, over a
      // transient reconnect. Losing staged work is worse than the leak
      // this closes. `carryOutbox` (createAndConnect) is excluded too:
      // there the chips belong to the thread being created.
      if (!opts.carryOutbox && previousThreadId !== threadId) {
        this._discardComposerAttachments();
      }
      // Accepted-send accounting is per-thread; a carried count would
      // pin the new thread's composer on "working".
      this.pendingTurnCount.set(0);
      // Genuine thread switch: a resume watermark from the previous
      // thread would suppress a real terminal frame on this one.
      this.resumedFromEpoch = null;
      // Same gate for the VM-session flag: createAndConnect sets it before
      // calling connect(carryOutbox), so only a real switch clears it. A
      // backend='vm' lifecycle event re-asserts it for the resume path.
      if (!opts.carryOutbox) this.isVmSession.set(false);
      this.sessionTitle.set(null);
      this.modelName.set(null);
      this.isOfficerThread.set(false);
      this.temperature.set(0);
      this.turnCount.set(0);
      // Token telemetry is per-thread. `currentUsage` would refuse to render
      // the outgoing thread's numbers anyway; clearing here frees the value
      // and keeps this list an honest inventory of what a switch resets.
      this.usage.set(null);
      this.ncSessionFolder.set(null);
      this.cloudSessionUrl.set(null);
      this.sshHandle.set(null);
      this.tasks.set([]);
      this.undoAvailable.set(false);
      this.rewindInFlight.set(false);
      this.rewindPrefill.set(null);
      this.rewindPreview.set(null);
      this.rewindPreviewLoading.set(false);
      this.rewindOutcomeUnknown.set(false);
      this.conversationRevision.set(0);
      this.pendingRewindRequestId = null;
      this._clearRewindAckFallback();
      this.isSessionPaused.set(false);
      // Config-drift dialog state is per-thread; a stale drift list
      // from the previous thread must not survive a genuine switch.
      this.pendingDrift.set(null);
      this.runningTool.set(null);
      this.citationsByCid.set(new Map());
      this.citationsLoaded.set(false);
      if (!preserveReviewPlane) {
        this._protectedCloud.set(false);
        this.cloudChangesCount.set(0);
        this.protectedMountName.set(null);
        this.cloudStagedAt.set(null);
        this.cloudDiffPanelOpen.set(false);
        this.threadMounts.set([]);
        this.protectedFolderLink.set(null);
        this.cloudDiffProbe.set('idle');
      }

      this.threadId.set(threadId);
      await this.loadHistory(threadId, generation);
      if (!this._isCurrentConnect(threadId, generation)) return;
      // loadHistory wholesale-replaced turns, killing the optimistic
      // bubbles for any carried outbox items. Re-dispatch them now —
      // BEFORE _openSse and _openControlWs, either of which can trigger
      // markSessionReady (and thus a flush) from the first frame /
      // /connection ready. The queued send must be visible + present
      // before any readiness path runs.
      if (opts.carryOutbox) this._redispatchOutboxBubbles();
    }
    await this.loadThreadMeta(threadId, generation);
    if (!this._isCurrentConnect(threadId, generation)) return;
    void this.loadCitations(threadId);

    // Ending/ended sessions have no admissible runtime/control plane, but
    // their durable event and protected-review plane remains live. Hydrate
    // the snapshot, open SSE only, and latch every control continuation.
    // Only an authoritative Resume success may reopen the latch.
    if (this.threadStatus() === 'ending' || this.threadStatus() === 'ended') {
      this.intentionalClose = false;
      if (this.threadStatus() === 'ending') {
        this._retireEndingControl(threadId, this.retirementDisposition());
      } else {
        this._retireTerminalControl(threadId);
      }
      await this._loadSessionState(threadId, generation, preservedReplayCursor, sameThread);
      if (!this._isCurrentConnect(threadId, generation)) return;
      // Snapshot hydration can restore moment-scoped approval/upgrade state;
      // discard it again without restarting the already-scheduled probe.
      if (this.threadStatus() === 'ending') {
        this._retireEndingControl(threadId, this.retirementDisposition());
      } else {
        this._retireTerminalControl(threadId);
      }
      await this._openSse(threadId);
      return;
    }

    this.intentionalClose = false;
    await this._loadSessionState(threadId, generation, preservedReplayCursor, sameThread);
    if (!this._isCurrentConnect(threadId, generation)) return;
    await this._openSse(threadId);
    if (!this._isCurrentConnect(threadId, generation)) return;
    await this._openControlWs(threadId);
  }

  private _isCurrentConnect(threadId: string, generation: number): boolean {
    return this.threadId() === threadId && this.connectGeneration === generation;
  }

  /**
   * Enter the instant-landing draft state: an open composer with no thread.
   * Detaches any still-connected session the same way a thread switch does —
   * the server-side session stays alive and resumable from the sessions
   * list. Nothing is created server-side until the first send
   * (_createFromDraftSession).
   */
  enterDraftSession(): void {
    this.disconnect();
    this.dispatch({ type: 'reset', threadId: null });
    this.threadId.set(null);
    this.usage.set(null);
    this.outbox.set([]);
    // Leaving a thread for the landing draft is a thread transition like
    // any other: the chips (and any eager upload behind them) belong to the
    // thread being left, not to the draft being entered.
    this._discardComposerAttachments();
    this.sessionReady.set(false);
    this.startupPhase.set(null);
    this.sessionTitle.set(null);
    this.error.set(null);
    this.creatingFromDraft = false;
    this.draftConnectorsEnabled.set(true);
    this.draftWorkspaceBackend.set('');
    this.isDraftSession.set(true);
    // Resolve the default project and compatible connector defaults as one
    // fail-closed context. The composer remains usable for drafting, but
    // Send is disabled until the user has seen this stable preselection.
    void this.retryDraftDefaults();
  }

  async retryDraftDefaults(): Promise<void> {
    const generation = ++this.draftDefaultsGeneration;
    this.draftProjectIds = null;
    this.draftDatasourceIds.set(null);
    this.draftDefaultsLoading.set(true);
    this.draftDefaultsError.set(false);
    this.error.set(null);
    try {
      // A raw read, kept deliberately: this context is fail-closed, and the
      // try/catch below is what turns a failure into "defaults unavailable"
      // rather than a believable "no project". `status=active` matches the
      // other create pickers — a draft can only ever target the account
      // default, which is never archivable anyway.
      const projects = await firstValueFrom(
        this.http.get<Project[]>(`${environment.apiUrl}/projects?status=active`),
      );
      if (generation !== this.draftDefaultsGeneration || !this.isDraftSession()) return;
      const defaultProject = projects.find((project) => project.is_default);
      this.draftProjectIds = defaultProject ? [defaultProject.id] : [];
      const body = this.draftCreationContext();
      if (this.capabilities.datasourceScopeAutoAttachAvailable() && this.draftConnectorsEnabled()) {
        body['use_datasource_defaults'] = true;
      } else {
        // An opt-out or unavailable rollout capability is an explicit empty
        // selection, even if the server enables defaults on omission.
        body['datasource_ids'] = [];
      }
      // Admission resolves the workspace before selecting implicit defaults.
      // Keep the reviewed IDs explicit at create so later auto-attach edits
      // cannot add a connector the user did not see here.
      const preview = await firstValueFrom(
        this.http.post<{project_ids: string[]; workspace_backend: string; datasource_ids: string[]}>(
          `${environment.apiUrl}/persistent/threads/preview`, body,
        ),
      );
      if (generation !== this.draftDefaultsGeneration || !this.isDraftSession()) return;
      this.draftProjectIds = preview.project_ids;
      this.draftDatasourceIds.set(preview.datasource_ids);
    } catch (err) {
      if (generation !== this.draftDefaultsGeneration || !this.isDraftSession()) return;
      this.draftDefaultsError.set(true);
      this.error.set(this.errors.translate(err, 'chat.draft.defaultsFailed'));
    } finally {
      if (generation === this.draftDefaultsGeneration) {
        this.draftDefaultsLoading.set(false);
      }
    }
  }

  setDraftConnectorsEnabled(enabled: boolean): void {
    this.draftConnectorsEnabled.set(enabled);
    void this.retryDraftDefaults();
  }

  setDraftWorkspaceBackend(backend: string | null): void {
    if (!['', 'virtual', 'sandbox', 'vm', 'none'].includes(backend ?? '')) return;
    this.draftWorkspaceBackend.set(backend ?? '');
    void this.retryDraftDefaults();
  }

  private draftCreationContext(): Record<string, any> {
    const body: Record<string, any> = {};
    if (this.draftProjectIds?.length) body['project_ids'] = this.draftProjectIds;
    const backend = this.draftWorkspaceBackend();
    if (backend) body['config_override'] = {workspace: {backend}};
    return body;
  }

  /** Retry the retained queue without appending a second copy of its message. */
  async retryDraftSession(): Promise<void> {
    const first = this.outbox()[0];
    if (!this.isDraftSession() || !first || first.threadId) return;
    await this._createFromDraftSession(first.displayContent);
  }

  /**
   * Create the session backing a landing-page draft (first send). Minimal
   * body by design: the orchestrator resolves the owner's saved defaults
   * (model, permission mode, workspace backend — virtual unless configured
   * otherwise). The queued first message rides the outbox into the new
   * thread (createAndConnect carries it) and flushes on session-ready.
   */
  private async _createFromDraftSession(firstMessage: string): Promise<void> {
    if (this.creatingFromDraft) return;
    if (
      this.draftDefaultsLoading() ||
      this.draftDefaultsError() ||
      this.draftDatasourceIds() === null
    )
      return;
    this.creatingFromDraft = true;
    const generation = this.draftDefaultsGeneration;
    this.isDraftSession.set(false);
    const body: Record<string, any> = {...this.draftCreationContext(), title: draftTitleFrom(firstMessage)};
    body['datasource_ids'] = this.draftConnectorsEnabled() ? (this.draftDatasourceIds() ?? []) : [];
    try {
      await this.createAndConnect(body);
    } catch {
      // createAndConnect surfaced the error state and re-showed the
      // queued bubbles; re-enter draft so the next send retries the
      // create with the same outbox.
      if (generation === this.draftDefaultsGeneration && this.threadId() === null) {
        this.isDraftSession.set(true);
      }
    } finally {
      if (generation === this.draftDefaultsGeneration) this.creatingFromDraft = false;
    }
  }

  /**
   * Create a new persistent thread via REST, then connect.
   * Sets isCreating=true immediately so the UI can show a spinner.
   */
  async createAndConnect(body: Record<string, any>): Promise<string> {
    this.disconnect();
    const creationGeneration = this.connectGeneration;
    let createdThreadId: string | null = null;
    // Clear the conversation + threadId synchronously so the "Creating
    // thread …" startup card isn't rendered on top of turns from the
    // session the user just navigated away from. disconnect() intentionally
    // keeps turns visible (the "Disconnect" button is a read-only state),
    // so the create path has to do it explicitly. connect() will reset
    // again with the real thread id once the POST resolves.
    this.dispatch({ type: 'reset', threadId: null });
    this.threadId.set(null);
    this.usage.set(null);
    this.isCreating.set(true);
    this.error.set(null);
    this.connectionState.set('connecting');
    this.startupPhase.set('creating');
    // A VM-backed create pays a cold KubeVirt boot — flag it up front so the
    // startup card shows the "Booting VM" copy and connect()'s readiness
    // poll uses the longer VM budget from the first iteration.
    this.isVmSession.set((body?.['config_override'] as any)?.workspace?.backend === 'vm');
    try {
      const resp = await firstValueFrom(
        this.http.post<{ thread_id: string }>(`${environment.apiUrl}/persistent/threads`, body),
      );
      const threadId = resp.thread_id;
      createdThreadId = threadId;
      if (this.connectGeneration !== creationGeneration || this.threadId() !== null) {
        // The thread was created, but the user navigated elsewhere
        // while the POST was in flight. Return its id to the caller
        // without stealing the newer route's state or transports.
        return threadId;
      }
      this.isCreating.set(false);
      // Carry the outbox: messages typed on the "Creating thread" card
      // belong to this new thread and must survive the connect() reset.
      await this.connect(threadId, { carryOutbox: true });
      return threadId;
    } catch (e) {
      const ownsCurrentView = createdThreadId
        ? this.threadId() === createdThreadId
        : this.connectGeneration === creationGeneration && this.threadId() === null;
      if (ownsCurrentView) {
        this.isCreating.set(false);
        this.connectionState.set('error');
        this.startupPhase.set(null);
        this.error.set(this.errors.translate(e, 'errors.sessions.createFailed'));
        // The reset at the top of createAndConnect wiped the optimistic
        // bubbles; re-show any queued sends on the error screen so the user
        // doesn't have a silently-retained outbox with no visible messages.
        this._redispatchOutboxBubbles();
      }
      throw e;
    }
  }

  /**
   * Rename a session (persistent thread). Persists the new title via PATCH and,
   * if the renamed thread is the one currently open, updates the live title
   * signal so the header reflects it immediately. Callers update their own
   * list state optimistically and revert on the rejected promise.
   */
  async renameThread(threadId: string, title: string): Promise<void> {
    await firstValueFrom(
      this.http.patch(`${environment.apiUrl}/persistent/threads/${threadId}`, { title }),
    );
    if (this.threadId() === threadId) {
      this.sessionTitle.set(title);
    }
  }

  /**
   * Load message history from REST endpoint and rehydrate as Turns.
   *
   * Server returns flat HistoryMessage rows (one per agent iteration); we
   * group consecutive assistant rows on `turn_number` into a single
   * AssistantTurn so the rendered bubble matches the live experience.
   * Thinking content is not persisted in `thread_messages` so historical
   * turns won't have thought events — known gap, see
   * `knowledge-base/knowledge/features/session_turn_rendering.md`.
   */
  /**
   * Fetch the engine citations for this session (job_id == thread_id) and
   * index them by citation id for inline [N] resolution. Best-effort — a
   * failure just leaves markers unresolved (restored to literal text).
   */
  private async loadCitations(threadId: string): Promise<void> {
    try {
      const resp = await firstValueFrom(
        this.http.get<{ citations: ThreadCitation[] }>(
          `${environment.apiUrl}/persistent/threads/${threadId}/citations`,
        ),
      );
      const map = new Map<number, ThreadCitation>();
      for (const c of resp?.citations ?? []) {
        map.set(c.id, c);
      }
      if (this.threadId() === threadId) {
        this.citationsByCid.set(map);
        this.citationsLoaded.set(true);
      }
    } catch {
      // Non-fatal: citations are an enhancement. Mark "loaded" so the
      // renderer stops waiting and leaves any [N] as literal text.
      if (this.threadId() === threadId) {
        this.citationsLoaded.set(true);
      }
    }
  }

  /**
   * On-view drift check for a cloud-document citation (Phase 3c /drift).
   * Goes through the by-citation endpoint, which now authorizes session
   * citations by thread ownership. Best-effort — null on any failure.
   */
  async fetchCitationDrift(citationId: number): Promise<CitationDrift | null> {
    try {
      return await firstValueFrom(
        this.http.get<CitationDrift>(`${environment.apiUrl}/citations/${citationId}/drift`),
      );
    } catch {
      return null;
    }
  }

  /**
   * Fetch the backed-up original bytes of a cited cloud document (Phase 3c
   * /snapshot) as a Blob, so "view original" carries the auth token (a raw
   * window.open to the URL would not). Null if unavailable/unauthorized.
   */
  async fetchCitationSnapshotBlob(citationId: number): Promise<Blob | null> {
    try {
      return await firstValueFrom(
        this.http.get(`${environment.apiUrl}/citations/${citationId}/snapshot`, {
          responseType: 'blob',
        }),
      );
    } catch {
      return null;
    }
  }

  private async loadHistory(threadId: string, generation?: number): Promise<void> {
    const historyGeneration = ++this.historyLoadGeneration;
    try {
      // 1. Cache-first: paint the cached conversation immediately (zero
      //    latency on reopen). Empty when this thread isn't cached yet.
      const cached = await this.cache.getThreadMessages(threadId);
      if (
        !this._isCurrentThreadRequest(threadId, generation) ||
        historyGeneration !== this.historyLoadGeneration
      )
        return;
      if (cached.length) {
        this.dispatch({ type: 'load_history', threadId, turns: historyToTurns(cached) });
        this.resetWindow();
        this.historyLoaded.set(true);
        this._recordDurableMessages(threadId, cached.length);
      }

      // 2. Refresh from the server. With a cache, fetch only what's newer
      //    (?after=<newest cached>, inclusive); otherwise the full thread.
      const cacheWithEpoch = this.cache as IndexedDbService & {
        getThreadCacheEpoch?: (id: string) => Promise<{
          eventsEpoch: number;
          conversationRevision: number;
        } | null>;
        applyThreadHistoryPage?: (
          id: string,
          epoch: number,
          revision: number,
          rows: Array<HistoryMessage & { threadId: string }>,
        ) => Promise<{
          accepted: boolean;
          replaced: boolean;
          messages: Array<HistoryMessage & { threadId: string }>;
        }>;
      };
      const cacheEpoch = cacheWithEpoch.getThreadCacheEpoch
        ? await cacheWithEpoch.getThreadCacheEpoch(threadId)
        : null;
      if (
        !this._isCurrentThreadRequest(threadId, generation) ||
        historyGeneration !== this.historyLoadGeneration
      )
        return;
      // Versioned history is fetched as a complete snapshot. If its epoch
      // advanced, an `after=` request based on the old epoch could return an
      // empty suffix and incorrectly replace the cache with no transcript.
      const newest = !cacheEpoch && cached.length ? cached[cached.length - 1].created_at : null;
      const url = newest
        ? `${environment.apiUrl}/persistent/threads/${threadId}/messages` +
          `?after=${encodeURIComponent(newest)}`
        : `${environment.apiUrl}/persistent/threads/${threadId}/messages`;
      const resp = await firstValueFrom(
        this.http.get<{
          messages: HistoryMessage[];
          total: number;
          events_epoch?: number;
          conversation_revision?: number;
        }>(url),
      );
      if (
        !this._isCurrentThreadRequest(threadId, generation) ||
        historyGeneration !== this.historyLoadGeneration
      )
        return;
      const fetched = resp.messages ?? [];

      const versioned =
        Number.isInteger(resp.events_epoch) && Number.isInteger(resp.conversation_revision);
      if (versioned && cacheWithEpoch.applyThreadHistoryPage) {
        const applied = await cacheWithEpoch.applyThreadHistoryPage(
          threadId,
          resp.events_epoch as number,
          resp.conversation_revision as number,
          fetched.map((m) => ({ ...m, threadId })),
        );
        if (
          !this._isCurrentThreadRequest(threadId, generation) ||
          historyGeneration !== this.historyLoadGeneration
        )
          return;
        this.conversationRevision.update((current) =>
          Math.max(current, resp.conversation_revision as number),
        );
        this.dispatch({
          type: 'load_history',
          threadId,
          turns: historyToTurns(applied.messages),
          preserveLive: !applied.replaced,
        });
        this.resetWindow();
        this._recordDurableMessages(threadId, applied.messages.length);
        this.historyLoaded.set(true);
        this._redispatchOutboxBubbles(true);
        return;
      }

      // Once a versioned floor exists, an older rolling-deploy response may
      // render the already-cached rows but may not merge into or downgrade v2.
      if (cacheEpoch) {
        this.historyLoaded.set(true);
        return;
      }

      // 3. Append to the cache by id (never full-replace — that loses
      //    history). Best-effort: a no-op when IndexedDB is unavailable.
      if (fetched.length) {
        await this.cache.upsertThreadMessages(fetched.map((m) => ({ ...m, threadId })));
      }

      // 4. Render the merged set. Merge in memory (dedup by id) rather than
      //    reading the cache back, so the render is correct even when
      //    IndexedDB is unavailable. Skip the re-render when the cache was
      //    already current (nothing new fetched).
      if (fetched.length || !cached.length) {
        const merged = mergeMessagesById(cached, fetched);
        this.dispatch({ type: 'load_history', threadId, turns: historyToTurns(merged) });
        this.resetWindow();
        this._recordDurableMessages(threadId, merged.length);
      }
      this.historyLoaded.set(true);
    } catch {
      // Network failure is non-fatal — any cached transcript was already
      // painted above; just mark history loaded.
      if (this._isCurrentThreadRequest(threadId, generation)) {
        this.historyLoaded.set(true);
      }
    }
  }

  /** Load thread metadata (title, model, turn count) from REST. */
  private async loadThreadMeta(threadId: string, generation?: number): Promise<void> {
    try {
      const thread = await firstValueFrom(
        this.http.get<any>(`${environment.apiUrl}/persistent/threads/${threadId}`),
      );
      if (!this._isCurrentThreadRequest(threadId, generation)) return;
      const retirementPending = thread.runtime_retirement_pending === true;
      const retirementDisposition =
        thread.retirement_disposition === 'ended' || thread.retirement_disposition === 'suspended'
          ? thread.retirement_disposition
          : null;
      const effectiveStatus: ThreadStatus | null = retirementPending
        ? 'ending'
        : (thread.status as ThreadStatus) || null;
      // A metadata GET started before a terminal SSE/typed REST observation
      // can complete afterward with the old active snapshot. Do not let that
      // stale response hide the Resume card or undo the terminal latch. An
      // explicit Resume clears the latch and advances connectGeneration, so
      // the successor generation can still apply its authoritative active
      // metadata normally.
      if (this.terminalControlThreadId === threadId) {
        const current = this.threadStatus();
        if ((current === 'ended' || current === 'suspended') && effectiveStatus !== current) {
          return;
        }
        if (
          current === 'ending' &&
          effectiveStatus !== 'ending' &&
          effectiveStatus !== 'ended' &&
          effectiveStatus !== 'suspended'
        ) {
          return;
        }
      }
      this.sessionTitle.set(thread.title || null);
      const model = thread.metadata?.config_override?.llm?.model;
      // `config_name` is an expert profile (normally `session_base`),
      // not an LLM model.  Leave the display unknown until the resolved
      // session-state snapshot supplies the effective model.
      this.modelName.set(model || null);
      const officer = thread.metadata?.config_override?.officer;
      this.isOfficerThread.set(officer?.enabled === true || officer?.enabled === 'true');
      const temperature = thread.metadata?.config_override?.llm?.temperature;
      if (temperature != null) {
        this.temperature.set(temperature);
      }
      this.turnCount.set(thread.total_turns || 0);
      this.ncSessionFolder.set(thread.nc_session_folder || null);
      this.cloudSessionUrl.set(thread.cloud_session_url || null);
      this.sshHandle.set(thread.ssh_handle || null);
      this.threadStatus.set(effectiveStatus);
      this.endedAt.set(thread.ended_at || thread.last_activity || null);
      this.retirementDisposition.set(retirementDisposition);
      this.threadMounts.set(Array.isArray(thread.mounts) ? thread.mounts : []);
      this._protectedCloud.set(!!thread.metadata?.protected_cloud);
      if (this._protectedCloud()) {
        void this.refreshCloudDiffCount();
        void this.resolveProtectedFolderLink();
      } else {
        this.protectedFolderLink.set(null);
      }
      if (retirementPending) {
        this._retireEndingControl(threadId, retirementDisposition);
      } else if (thread.status === 'ended') {
        this._retireTerminalControl(threadId);
      }
    } catch {
      // Non-fatal — UI will show fallback values
    }
  }

  /**
   * Hydrate the lane-free durable state before opening SSE. The server
   * returns a snapshot-consistent replay floor just before the latest turn;
   * replay rebuilds that logical turn, closes the REST-history/completion
   * race, then applies frames newer than event_cursor normally.
   */
  private async _loadSessionState(
    threadId: string,
    generation?: number,
    preservedReplayCursor?: { epoch: number; seq: number } | null,
    retainedConversation = false,
  ): Promise<void> {
    this.sessionSnapshotLoaded = false;
    this.sessionSnapshotFailed = false;
    this.sessionSnapshotCursor = null;
    this.snapshotJoinsPreservedTurn = false;
    this.snapshotSeededUsage = false;
    this.sseReplayCursor = undefined;
    try {
      const snapshot = await firstValueFrom(
        this.http.get<SessionStateSnapshot>(
          `${environment.apiUrl}/persistent/threads/${threadId}/state`,
        ),
      );
      if (!this._isCurrentThreadRequest(threadId, generation)) return;
      // A mismatched payload is never allowed to paint the current tab,
      // even if an intermediary returned a cached 200 for another URL.
      if (snapshot.thread_id !== threadId) {
        throw new Error('session-state thread mismatch');
      }
      const eventCursor = snapshot.event_cursor;
      const replayCursor = snapshot.replay_cursor;
      if (
        !eventCursor ||
        !Number.isFinite(eventCursor.epoch) ||
        !Number.isFinite(eventCursor.seq) ||
        !replayCursor ||
        !Number.isFinite(replayCursor.epoch) ||
        !Number.isFinite(replayCursor.seq) ||
        replayCursor.epoch !== eventCursor.epoch ||
        replayCursor.seq > eventCursor.seq
      ) {
        throw new Error('invalid session-state cursor contract');
      }
      const hasPreservedCursor = !!(
        preservedReplayCursor &&
        Number.isFinite(preservedReplayCursor.epoch) &&
        Number.isFinite(preservedReplayCursor.seq)
      );
      const canPreserveCursor = !!(
        hasPreservedCursor &&
        preservedReplayCursor!.epoch === eventCursor.epoch &&
        preservedReplayCursor!.seq >= replayCursor.seq &&
        preservedReplayCursor!.seq <= eventCursor.seq
      );
      if (retainedConversation && !canPreserveCursor) {
        // An epoch change invalidates both the retained live turn and
        // the append-only message cache (rewind is one bump source).
        // A same-epoch cursor older than replay_cursor is unsafe too:
        // another tab may have completed whole turns since this tab's
        // last frame, and latest-turn replay would skip that history.
        // Repaint before replay so neither shape leaves a gap or mixes
        // two session lives.
        this.dispatch({ type: 'reset', threadId });
        this.historyLoaded.set(false);
        try {
          await this.cache.clearThreadMessages(threadId);
        } catch {
          // IndexedDB is optional; the REST full load below remains
          // authoritative even when local cache cleanup fails.
        }
        try {
          await this.cache.deleteThreadCursor(threadId);
        } catch {
          // Same as above: never block recovery on local storage.
        }
        if (!this._isCurrentThreadRequest(threadId, generation)) return;
        await this.loadHistory(threadId, generation);
        if (!this._isCurrentThreadRequest(threadId, generation)) return;
        this._redispatchOutboxBubbles(true);
      }
      // Cold tabs rebuild the latest turn from the server floor. A
      // same-thread reconnect keeps the conversation it already folded,
      // so its own tab-local cursor is safe when it lies within this
      // snapshot and at/after the floor. Shared IndexedDB is never used
      // for this choice: another tab may have observed frames we did not.
      const selectedReplayCursor = canPreserveCursor ? preservedReplayCursor! : replayCursor;
      this.snapshotJoinsPreservedTurn = !!(
        canPreserveCursor && selectedReplayCursor.seq > replayCursor.seq
      );
      this.sseOpenedWithCursor = true;
      this.sseReplayCursor = selectedReplayCursor;
      this.sessionSnapshotCursor = snapshot.event_cursor;
      this._handleEvent({ method: 'session.state', params: snapshot }, false);
      this.sessionSnapshotLoaded = true;
      if (this.error() === 'Session state unavailable') this.error.set(null);
    } catch {
      if (!this._isCurrentThreadRequest(threadId, generation)) return;
      // Pinned sessions can still recover their exact state over the
      // coexistence WebSocket. A socketless session stays unready: making
      // its composer look usable while a gate is unrenderable is unsafe.
      this.sessionSnapshotFailed = true;
      if (this.controlSocket === 'none') this.sessionReady.set(false);
      this.error.set('Session state unavailable');
    }
  }

  private _isCurrentThreadRequest(threadId: string, generation?: number): boolean {
    return (
      this.threadId() === threadId &&
      (generation === undefined || this.connectGeneration === generation)
    );
  }

  /**
   * Refresh the staged protected-cloud diff count + mount name from the
   * summary endpoint (Slice C, Task 14). Called on thread load and
   * (debounced) after each turn.completed while protected — staging runs
   * at turn end, so that's the natural refresh edge. Best-effort: a
   * failed fetch (getThreadCloudDiff already swallows errors to null)
   * just leaves the previous count/mount in place.
   */
  async refreshCloudDiffCount(): Promise<void> {
    const threadId = this.threadId();
    if (!threadId) return;
    const requestOrdinal = ++this.cloudDiffRequestOrdinal;
    this.cloudDiffProbe.set('loading');
    // The tagged read, not the nullable one: a failure has to be
    // distinguishable from "nothing staged", or the banner cannot tell the
    // difference between an empty folder and an unanswered question.
    const outcome = await firstValueFrom(this.api.getThreadCloudDiffOutcome(threadId));
    if (this.threadId() !== threadId || requestOrdinal !== this.cloudDiffRequestOrdinal) return; // stale response after a switch or same-thread superseding read
    if (outcome.kind === 'ok') {
      const summary = outcome.data;
      this.cloudChangesCount.set(cloudCountFromSummary(summary));
      this.protectedMountName.set(summary.protected_mount);
      this.cloudStagedAt.set(summary.staged_at);
      this.cloudDiffProbe.set('ready');
      return;
    }
    // Previous count/mount are deliberately left in place — a transient
    // failure is not evidence that a staged diff went away.
    this.cloudDiffProbe.set('error');
  }

  /**
   * Resolve the protected project folder to a browser URL (PC-19).
   *
   * Best-effort and fail-quiet: any missing piece — no eligible mount, no
   * project record, no `cloud_storage_url` on it — leaves the link null and
   * the "Open project files" action simply absent. Never falls back to the
   * legacy session-folder handle, which is the wrong folder for a protected
   * thread and is what PC-19 is about.
   */
  async resolveProtectedFolderLink(): Promise<void> {
    const threadId = this.threadId();
    const mount = selectProtectedProjectMount(this.threadMounts());
    if (!threadId || !mount?.source_ref) {
      this.protectedFolderLink.set(null);
      return;
    }
    const project = await firstValueFrom(this.api.getProject(mount.source_ref));
    if (this.threadId() !== threadId) return; // stale response after a switch
    const url = project?.cloud_storage_url;
    if (!project || !url) {
      this.protectedFolderLink.set(null);
      return;
    }
    this.protectedFolderLink.set({
      url,
      name: project.name,
      targetPath: mount.target_path,
    });
  }

  /**
   * Cloud-diff review resolved (applied or rejected).
   *
   * The panel is deliberately NOT closed here any more: it now shows the
   * outcome receipt until the user dismisses it, because a four-second toast
   * over a closing drawer is exactly how PC-20's owner ended up unable to
   * tell whether a 34-second apply had landed. The count is re-read from the
   * server rather than assumed, so a partial failure (which leaves staging
   * intact) is reflected honestly instead of being zeroed optimistically.
   */
  onCloudDiffResolved(): void {
    this.cloudChangesCount.set(0);
    void this.refreshCloudDiffCount();
  }

  /** Close the review surface (the user dismissed it). */
  closeCloudReview(): void {
    this.cloudDiffPanelOpen.set(false);
  }

  /** Debounced refreshCloudDiffCount() after a turn.completed frame — see
   *  the call site in _handleEvent for why the 2s window. */
  private _scheduleCloudDiffRefresh(): void {
    if (this.cloudDiffRefreshTimer) clearTimeout(this.cloudDiffRefreshTimer);
    const threadId = this.threadId();
    this.cloudDiffRefreshTimer = setTimeout(() => {
      this.cloudDiffRefreshTimer = null;
      if (threadId && this.threadId() === threadId) void this.refreshCloudDiffCount();
    }, PersistentChatService.CLOUD_DIFF_REFRESH_DEBOUNCE_MS);
  }

  // ── SSE receive path ─────────────────────────────────────────────────

  /**
   * Open the SSE event stream. When IndexedDB has a cached cursor for
   * this thread we can't use the EventSource constructor to pass a custom
   * `Last-Event-ID` header (the API doesn't accept request headers), so
   * we encode the cursor as a query parameter — the server reads either
   * the header or `?last_event_id=...`. After the first event arrives,
   * the browser tracks the cursor itself via the `id:` lines.
   */
  private async _openSse(threadId: string): Promise<void> {
    // Drop any stale watchdog from a prior open before installing a new one.
    this._stopSseWatchdog();
    this.sseOpenedWithCursor = false;

    // Single-flight guard: claim a generation up front. If a newer open (or
    // a disconnect()) supersedes us while we await the cursor, bail before
    // constructing an EventSource — otherwise a slow cursor read could
    // resurrect a stream on a closed or replaced session.
    const generation = ++this.sseGeneration;
    // Same-thread Resume keeps the durable review/SSE plane but advances the
    // control generation. Any metadata request spawned by this stream belongs
    // to the generation that opened it and must not retire the resumed one.
    const connectGeneration = this.connectGeneration;

    let cursor = this.sseReplayCursor;
    if (cursor === undefined) {
      try {
        cursor = await this.cache.getThreadCursor(threadId);
      } catch {
        // IndexedDB is an optimization, never a prerequisite for the
        // live receive path (private mode/quota/corruption can reject).
        cursor = null;
      }
    }
    if (generation !== this.sseGeneration || this.intentionalClose || this.threadId() !== threadId)
      return;
    this.sseReplayCursor = cursor;
    this.sseOpenedWithCursor = cursor != null;

    // ngsw-bypass keeps the Angular service worker out of the SSE path. Its
    // /api/** dataGroup otherwise buffers the stream body (which never ends),
    // stalling EventSource.onopen ~20s. The param's presence alone is enough.
    const params = new URLSearchParams({ 'ngsw-bypass': 'true' });
    if (cursor) params.set('last_event_id', `${cursor.epoch}:${cursor.seq}`);
    const url = `${environment.apiUrl}/persistent/threads/${threadId}/stream?${params.toString()}`;

    // A racing reopen may have installed a stream before we passed the guard
    // above (its generation could equal ours after an intervening bump/undo).
    // Close whatever's there so we never leak a live socket on replace.
    if (this.sse) {
      this.sse.close();
      this.sse = null;
    }

    // withCredentials true so the srw_session cookie rides along on the
    // cross-origin SSE handshake.
    const es = new EventSource(url, { withCredentials: true });
    this.sse = es;

    es.onopen = () => {
      if (this.sse !== es) return; // superseded — ignore late open
      this.zone.run(() => {
        const wasReconnecting = this.connectionState() !== 'connected';
        const terminalReviewOnly = this.terminalControlThreadId === threadId;
        const bindingInvalid = this.invalidBindingRuntime?.threadId === threadId;
        const bindingRecovering = this.bindingRecoveryRuntime?.threadId === threadId;
        const controlUnavailable = terminalReviewOnly || bindingInvalid || bindingRecovering;
        // EventSource liveness is not agent/control readiness. An ended
        // session deliberately retains (and may reopen) this stream for late
        // protected staging, but must not regain the green Connected UI or
        // actions/IDE polling that are gated on isConnected().
        this.connectionState.set(
          bindingInvalid
            ? 'error'
            : terminalReviewOnly
              ? 'disconnected'
              : bindingRecovering
                ? 'connecting'
                : 'connected',
        );
        if (bindingInvalid) {
          this.error.set(this.transloco.translate('errors.sessions.bindingInvalid'));
        } else if (!terminalReviewOnly && !this.sessionSnapshotFailed) {
          this.error.set(null);
        }
        this.reconnectAttempt.set(0);
        this.reconnectGaveUp.set(false);
        this._startSseWatchdog(threadId);
        // On a reconnect (not the initial open), refetch thread meta
        // so any title.updated / status frame that crossed the wire
        // while we were disconnected is reconciled. Without this the
        // header can stay stuck on "Untitled Session" after a backend
        // loop_crash even though the title was generated and persisted.
        if (wasReconnecting && this.threadId() === threadId) {
          void this.loadThreadMeta(threadId, connectGeneration);
          // Slave the control WS to SSE recovery: the WS has no
          // liveness probe of its own, so re-establish it whenever
          // the (monitored) SSE recovers. Idempotent — bails if the
          // WS is already open/connecting.
          if (!controlUnavailable) this._ensureControlWs();
        }
      });
    };

    es.onmessage = (event: MessageEvent) => {
      if (this.sse !== es) return;
      this.sseLastEventAt = Date.now();
      this.zone.run(() => this._handleSseFrame(event));
    };

    // Server idle-heartbeat — no payload to dispatch, just liveness.
    es.addEventListener('ping', () => {
      if (this.sse !== es) return;
      this.sseLastEventAt = Date.now();
    });

    es.addEventListener('gone_beyond_horizon', (event) => {
      if (this.sse !== es) return;
      this.sseLastEventAt = Date.now();
      this.zone.run(() => this._handleGoneBeyondHorizon(event as MessageEvent));
    });

    es.onerror = () => {
      if (this.sse !== es) return;
      this.zone.run(() => {
        if (this.sse !== es) return;
        if (es.readyState === EventSource.CLOSED) {
          // Terminal — auth failure, thread gone, etc. The browser
          // gave up. Don't bury the UI in a generic banner; let
          // the threadStatus refresh below surface "ended" if
          // that's what happened.
          this._stopSseWatchdog();
          this.connectionState.set('error');
          this.reconnectGaveUp.set(true);
          this._refreshStatusAfterDrop(threadId, connectGeneration);
        } else {
          // CONNECTING — the browser is retrying. Show reconnecting.
          const bindingInvalid = this.invalidBindingRuntime?.threadId === threadId;
          this.connectionState.set(bindingInvalid ? 'error' : 'connecting');
          if (bindingInvalid) {
            this.error.set(this.transloco.translate('errors.sessions.bindingInvalid'));
          }
          this.reconnectAttempt.update((n) => n + 1);
        }
      });
    };
  }

  /**
   * Liveness watchdog for the SSE receive path. Browser `EventSource` only
   * fires `onerror` when the TCP socket closes cleanly or the OS-level
   * keepalive trips (Linux default: 7200s), so silent network drops leave
   * the stream in `readyState === OPEN` for hours. The orchestrator emits
   * a typed `ping` event every ~20s of idle; if we haven't seen *any* SSE
   * event (ping or otherwise) for SSE_WATCHDOG_TIMEOUT_MS, the connection
   * is presumed dead and we force a reopen.
   */
  private _startSseWatchdog(threadId: string): void {
    this._stopSseWatchdog();
    this.sseLastEventAt = Date.now();
    // The agent clock measures agent output, not stream health: a stream
    // reopen (mobile background/foreground, a dropped socket) must not make
    // a silent agent look freshly active. Seed it only on the first open.
    if (this.agentLastEventAt <= 0) {
      this.agentLastEventAt = Date.now();
    }
    this.sseWatchdogTimer = setInterval(() => {
      // Piggyback the agent-quiet signal on this 5s tick (audit #8):
      // only meaningful while a turn is open — an idle agent being
      // quiet is expected.
      this.zone.run(() => {
        const turnOpen = this.conversation().activeAssistantTurnId != null;
        this.agentSilenceSeconds.set(
          turnOpen && this.agentLastEventAt > 0
            ? Math.floor((Date.now() - this.agentLastEventAt) / 1000)
            : 0,
        );
      });
      if (!this.sse || this.sse.readyState !== EventSource.OPEN) {
        // CONNECTING/CLOSED is already handled by onerror.
        return;
      }
      if (Date.now() - this.sseLastEventAt <= SSE_WATCHDOG_TIMEOUT_MS) {
        return;
      }
      // Silent drop. Tear down and let the existing reopen path
      // (with replay-from-cursor) take over.
      this._stopSseWatchdog();
      this.zone.run(() => {
        if (this.sse) {
          this.sse.close();
          this.sse = null;
        }
        this.connectionState.set(
          this.invalidBindingRuntime?.threadId === threadId ? 'error' : 'connecting',
        );
        this.reconnectAttempt.update((n) => n + 1);
        void this._openSse(threadId);
      });
    }, SSE_WATCHDOG_INTERVAL_MS);
  }

  private _stopSseWatchdog(): void {
    if (this.sseWatchdogTimer) {
      clearInterval(this.sseWatchdogTimer);
      this.sseWatchdogTimer = null;
    }
  }

  /**
   * Re-validate liveness on tab resume (visibilitychange / online / focus).
   * The SSE is the single liveness authority: if it isn't OPEN or has gone
   * silent past the watchdog timeout, force a reopen (replay-from-cursor);
   * then ensure the control WS — which has no probe of its own — is back.
   * No-op when there's no active, live session.
   */
  private _revalidateConnection(force = false): void {
    if (this.intentionalClose) return;
    const tid = this.threadId();
    if (!tid) return;
    const terminalControl = this.terminalControlThreadId === tid;
    if (this.threadStatus() === 'ended' && !terminalControl) return;
    // `force` (long hidden-tab wake / bfcache restore / page resume) skips
    // the liveness heuristics: a socket that died while frozen still reports
    // readyState===OPEN with a stale-but-recent sseLastEventAt, so the
    // heuristics can't be trusted after a freeze.
    const sseStale =
      force ||
      !this.sse ||
      this.sse.readyState !== EventSource.OPEN ||
      Date.now() - this.sseLastEventAt > SSE_WATCHDOG_TIMEOUT_MS;
    if (sseStale) {
      // Closes + reopens the SSE and sets connectionState='connecting';
      // the reopen's onopen also re-ensures the control WS (Change 2).
      this.reconnectNow();
    } else if (!terminalControl) {
      // SSE healthy but the WS may have silently dropped — re-ensure it.
      this._ensureControlWs();
    }
  }

  private _handleSseFrame(event: MessageEvent): void {
    // event.lastEventId is "<epoch>:<seq>". Save before dispatch so a
    // dispatch error doesn't lose our place — the SSE replay logic
    // tolerates re-receiving the same seq (it'll just be a no-op given
    // the seq > $3 guard server-side).
    this.currentFrameEpoch = null;
    let currentFrameSeq: number | null = null;
    if (event.lastEventId) {
      const tid = this.threadId();
      if (tid) this._saveCursor(tid, event.lastEventId);
      const colon = event.lastEventId.indexOf(':');
      if (colon > 0) {
        const parsed = Number(event.lastEventId.slice(0, colon));
        if (Number.isFinite(parsed)) this.currentFrameEpoch = parsed;
        const parsedSeq = Number(event.lastEventId.slice(colon + 1));
        if (Number.isFinite(parsedSeq)) currentFrameSeq = parsedSeq;
      }
    }

    let frame: { method: string; params?: Record<string, unknown> };
    try {
      frame = JSON.parse(event.data);
    } catch {
      return;
    }
    // Data-only liveness clock for the send-kickstart. Bump here and only
    // here — a real journal frame arrived. `ping`/`onopen`/control-WS frames
    // deliberately don't touch this (they flow even when the receive path is
    // a zombie polling a dead epoch).
    this.sseDataLastAt = Date.now();
    const snapshotCursor = this.sessionSnapshotCursor;
    const coveredBySnapshot = !!(
      snapshotCursor &&
      this.currentFrameEpoch === snapshotCursor.epoch &&
      currentFrameSeq != null &&
      currentFrameSeq <= snapshotCursor.seq
    );
    this._handleEvent(frame, true, coveredBySnapshot);
  }

  /**
   * gone_beyond_horizon: cursor is too stale to replay (epoch mismatch or
   * seq older than retention). REST-reload the transcript snapshot so the
   * user sees completed turns, then re-anchor the cursor to the server tail
   * (reported in the frame) and reopen the stream.
   *
   * Re-anchoring (rather than dropping the cursor) is what prevents the
   * duplicate-turn render: with no cursor the server replays the whole epoch
   * from seq 0, re-delivering every completed turn as a "live" copy that the
   * reducer can't reconcile against the just-loaded history (history turns
   * are keyed by DB id, replayed turns by numeric turn_id), so the turn shows
   * twice split by a spurious "SESSION RESUMED" divider. Anchoring to the
   * tail replays only genuinely newer events. See memory
   * project_session_epoch_duplicate_render.
   */
  private async _handleGoneBeyondHorizon(event: MessageEvent): Promise<void> {
    const tid = this.threadId();
    if (!tid) return;
    const generation = this.connectGeneration;
    // The frame carries the live epoch + its tail seq:
    // {"params":{"epoch":N,"server_seq":M,...}}.
    let epoch: number | null = null;
    let serverSeq: number | null = null;
    try {
      const p = JSON.parse(event.data)?.params;
      epoch = typeof p?.epoch === 'number' ? p.epoch : null;
      serverSeq = typeof p?.server_seq === 'number' ? p.server_seq : null;
    } catch {
      // Malformed frame — fall back to dropping the cursor (replay-from-0).
    }
    if (this.sse) {
      this.sse.close();
      this.sse = null;
    }
    // Clear the (append-only) cache before reloading — mirrors
    // _reloadAfterRewind's ordering. Epoch bumps are rare (a new agent
    // attach, or a rewind tombstoning rows server-side), so a full
    // refetch here is cheap; but once the epoch has moved, the cache's
    // append-only assumption no longer holds — a stale local copy could
    // otherwise survive a naive merge into the freshly loaded history.
    await this.cache.clearThreadMessages(tid);
    await this.cache.deleteThreadCursor(tid);
    if (!this._isCurrentConnect(tid, generation)) return;
    // Reload transcript so visible history doesn't have a silent gap.
    await this.loadHistory(tid, generation);
    if (!this._isCurrentConnect(tid, generation)) return;
    // loadHistory replaced turns → re-dispatch bubbles for still-queued
    // sends. Skip the item whose POST is in flight: accept-time persistence
    // may already have put its row in the reloaded history, so re-adding it
    // would double the bubble (the flush reconciles it when it resolves).
    this._redispatchOutboxBubbles(true);
    // Re-anchor to the server tail so the reopened stream replays only
    // events newer than the history we just loaded; drop the cursor only
    // when the frame lacked a usable tail.
    if (epoch != null && serverSeq != null) {
      await this.cache.setThreadCursor(tid, epoch, serverSeq);
    } else {
      await this.cache.deleteThreadCursor(tid);
    }
    if (!this._isCurrentConnect(tid, generation)) return;
    // An epoch bump means a new agent attached, so any 'ended' status we
    // are still holding is stale by definition — re-read it. Self-heals a
    // view that already applied a replayed terminal frame (loadHistory
    // above restores the transcript but never touches thread meta).
    await this.loadThreadMeta(tid, generation);
    if (!this._isCurrentConnect(tid, generation)) return;
    await this._loadSessionState(tid, generation);
    if (!this._isCurrentConnect(tid, generation)) return;
    await this._openSse(tid);
  }

  /** Full transcript repaint after a rewind: drop the (append-only)
   *  cache + cursor, then reload from the server's filtered history. */
  private async _reloadAfterRewind(): Promise<void> {
    if (this.rewindRefresh) return this.rewindRefresh;
    this.rewindRefresh = this._performRewindRefresh();
    try {
      await this.rewindRefresh;
    } finally {
      this.rewindRefresh = null;
    }
  }

  private async _performRewindRefresh(): Promise<void> {
    const tid = this.threadId();
    if (!tid) return;
    const generation = this.connectGeneration;
    const hadSse = this.sse !== null;
    this.historyLoadGeneration++;
    await this.cache.clearThreadMessages(tid);
    await this.cache.deleteThreadCursor(tid);
    if (!this._isCurrentConnect(tid, generation)) return;
    await this.loadHistory(tid, generation);
    if (!this._isCurrentConnect(tid, generation)) return;
    this._redispatchOutboxBubbles(true);
    await this.loadThreadMeta(tid, generation);
    if (!this._isCurrentConnect(tid, generation)) return;
    await this._loadSessionState(tid, generation);
    if (!this._isCurrentConnect(tid, generation)) return;
    if (this.sse) {
      this.sse.close();
      this.sse = null;
    }
    if (hadSse) await this._openSse(tid);
  }

  /** Epoch of the frame currently being dispatched, parsed from the SSE
   *  `id:` line. Null for frames that carried no usable id. */
  private currentFrameEpoch: number | null = null;

  /**
   * Epoch the thread was on when we last resumed it. A resume reconnects the
   * SSE while the thread is still on its OLD epoch (the agent hasn't attached
   * yet), so the server replays that epoch's remaining tail — which ends in
   * `session.idle_timeout` + `session.ended`. Those describe a session life
   * we have already superseded; applying them pins the ended UI over a live,
   * streaming session, and nothing re-reads status afterwards. Frames at or
   * below this watermark are ignored by the terminal-lifecycle handlers.
   */
  private resumedFromEpoch: number | null = null;

  /**
   * True when the terminal lifecycle frame being dispatched belongs to a
   * session life we have already resumed past.
   *
   * A resume reopens the SSE while the thread is still on its OLD epoch —
   * the agent hasn't attached and bumped it yet — so the server's
   * `WHERE epoch = $2 AND seq > $3` poll streams that epoch's remaining
   * tail, whose last two rows are `session.idle_timeout` + `session.ended`.
   * Applying them re-pins the ended UI (end marker, resume card, "sending
   * resumes the session" placeholder) over a session that is live and
   * streaming, and nothing re-reads thread status afterwards, so it sticks
   * for the rest of the session.
   */
  private _isSupersededLifecycleFrame(): boolean {
    if (this.resumedFromEpoch === null) return false;
    // No id on the frame → can't prove it's stale; apply it. A live
    // terminal frame is the one case we must never swallow.
    if (this.currentFrameEpoch === null) return false;
    return this.currentFrameEpoch <= this.resumedFromEpoch;
  }

  private _saveCursor(threadId: string, lastEventId: string): void {
    // Parse "<epoch>:<seq>"; ignore malformed (keepalives, etc.).
    const colon = lastEventId.indexOf(':');
    if (colon <= 0) return;
    const epoch = Number(lastEventId.slice(0, colon));
    const seq = Number(lastEventId.slice(colon + 1));
    if (!Number.isFinite(epoch) || !Number.isFinite(seq)) return;
    const current = this.sseReplayCursor;
    if (
      current == null ||
      epoch > current.epoch ||
      (epoch === current.epoch && seq > current.seq)
    ) {
      // Advance synchronously for this tab before the best-effort shared
      // persistence write. A focus reconnect must never borrow another
      // tab's later cursor and skip a frame this service has not folded.
      this.sseReplayCursor = { epoch, seq };
    }
    // Fire-and-forget; cursor staleness is recoverable.
    void this.cache.setThreadCursor(threadId, epoch, seq);
  }

  /**
   * After SSE drops to CLOSED, the agent may have flipped this thread to
   * `ended` (idle archive, /done from another client). Re-fetch meta so the
   * UI swaps to the resume card instead of stuck on "connection error".
   */
  private _refreshStatusAfterDrop(threadId: string, generation: number): void {
    setTimeout(async () => {
      if (!this._isCurrentConnect(threadId, generation)) return;
      await this.loadThreadMeta(threadId, generation);
      if (
        this._isCurrentConnect(threadId, generation) &&
        this.terminalControlThreadId === threadId &&
        (!this.sse || this.sse.readyState === EventSource.CLOSED)
      ) {
        this.reconnectNow();
      }
    }, 1500);
  }

  // ── Control WS (slash commands + permission decisions) ───────────────

  /**
   * Resolve the canonical WS URL + token for the session via the
   * orchestrator REST endpoints (Tasks 6 + 7), then open the WebSocket.
   *
   * Two REST steps, joined by the SSE notification feed:
   *
   *   1. GET /api/sessions/{tid}/connection
   *      → 200 (ready): {ws_url, token, expires_at}. Open WS directly.
   *      → 425 (not bound yet): cold start, fall through to step 2.
   *
   *   2. POST /api/sessions/{tid}/prepare → 202 {state: provisioning}
   *      → wait for `session.lifecycle` SSE event with state=ready
   *      → GET /connection again, then open WS.
   *
   * Errors here are swallowed: the WS open is best-effort (the SSE
   * receive path is the primary signal). User-initiated commands that
   * need the WS will reopen via _ensureControlWs() on demand.
   */
  private async _openControlWs(threadId: string): Promise<void> {
    if (!this._controlPlaneAllowed(threadId)) return;
    if (this.controlWsOpening) return;
    const openingGeneration = ++this.controlWsOpeningGeneration;
    this.controlWsOpening = true;
    try {
      const connection = await this._resolveConnection(threadId, openingGeneration);
      if (!this._controlPlaneAllowed(threadId, openingGeneration)) return;
      if (!this._acceptBindingRecoveryConnection(threadId, connection)) return;
      this._applyQueueState(threadId, connection.queue ?? null);
      // GET /connection only returns 200 after the orchestrator has a
      // bound agent and the agent's /ready probe passes. That REST
      // readiness is enough to unblock the composer; the control WS
      // session.state frame remains a useful reconciliation signal, but
      // must not be the only way to clear the startup card.
      const hasControlSocket = this._connectionHasWebSocket(connection);
      if (connection.state === 'ready' && (hasControlSocket || this.sessionSnapshotLoaded)) {
        this.markSessionReady();
      }
      this._installControlTransport(threadId, connection);
    } catch (err) {
      // Resolution failed — leave controlWs null; _ensureControlWs
      // (driven by user clicks) or the reconnect loop will retry.
      const invalidBindingGeneration = this._sessionBindingInvalidGeneration(err);
      if (
        invalidBindingGeneration !== null &&
        openingGeneration === this.controlWsOpeningGeneration &&
        this.threadId() === threadId
      ) {
        this._latchSessionBindingInvalid(threadId, invalidBindingGeneration);
      } else if (
        (this._isSessionEndedError(err) || this._isSessionEndingError(err)) &&
        openingGeneration === this.controlWsOpeningGeneration &&
        this.threadId() === threadId
      ) {
        this._retireFromSessionRefusal(threadId, err);
      } else if (this._controlPlaneAllowed(threadId, openingGeneration)) {
        this._scheduleControlWsReconnect(threadId);
      }
    } finally {
      if (openingGeneration === this.controlWsOpeningGeneration) {
        this.controlWsOpening = false;
      }
    }
  }

  /**
   * Resolve the {ws_url, token} for a thread. On 425 (no binding yet)
   * POST /prepare to kick off provisioning, then poll /connection until
   * 200 — the always-on NotificationService SSE drives the
   * "Starting session" card via lifecycle events in parallel, so this
   * function only owns the token fetch, not the UI phase rendering.
   */
  private async _resolveConnection(
    threadId: string,
    openingGeneration: number,
  ): Promise<ConnectionPayload> {
    try {
      const connection = await this._fetchConnection(threadId);
      if (!this._controlPlaneAllowed(threadId, openingGeneration)) {
        throw new Error('connection cancelled');
      }
      return connection;
    } catch (err: any) {
      if (
        this._isSessionEndedError(err) ||
        this._isSessionEndingError(err) ||
        this._sessionBindingInvalidGeneration(err) !== null
      ) {
        throw err;
      }
      if (!this._controlPlaneAllowed(threadId, openingGeneration)) {
        throw new Error('connection cancelled');
      }
      if (err?.status === 409) {
        // A live pinned binding can exist before its agent passes /ready.
        // That booting response is deliberately a generic (non-terminal)
        // 409, and must use the full sandbox/VM readiness budget rather than
        // the short control-socket reconnect ladder. The binding already
        // exists, so do not POST /prepare or risk a second provision loop.
        return await this._pollConnectionUntilReady(threadId, openingGeneration);
      }
      if (err?.status !== 425) throw err;
      // Not bound yet — kick off /prepare and poll /connection until
      // the orchestrator binds an agent and the agent's /ready flips
      // true. (The cockpit's startup card is rendered by the
      // session.lifecycle effect in the constructor; we just wait
      // here for the token.)
      await firstValueFrom(
        this.http.post<{ state: string }>(`${environment.apiUrl}/sessions/${threadId}/prepare`, {}),
      );
      if (!this._controlPlaneAllowed(threadId, openingGeneration)) {
        throw new Error('connection cancelled');
      }
      return await this._pollConnectionUntilReady(threadId, openingGeneration);
    }
  }

  /**
   * Poll GET /connection until it returns 200. Backoff: 1s, capped at
   * 2s. Aborts if the user navigates away (threadId() changes) or the
   * service is intentionally closed. Bounded by READY_TIMEOUT_MS so a
   * stuck attach surfaces as an error instead of polling forever.
   */
  private async _pollConnectionUntilReady(
    threadId: string,
    openingGeneration: number,
  ): Promise<ConnectionPayload> {
    // Sandbox/lite sessions are ready in seconds; a cold VM boot runs
    // minutes. Re-read isVmSession() every iteration so a `backend='vm'`
    // lifecycle event arriving mid-poll (the resume path, where the create
    // body didn't flag it) still extends the budget.
    const READY_TIMEOUT_MS = 180_000;
    const VM_READY_TIMEOUT_MS = 1_020_000; // > server 960s > agent 900s
    const start = Date.now();
    let interval = 1_000;
    while (Date.now() - start < (this.isVmSession() ? VM_READY_TIMEOUT_MS : READY_TIMEOUT_MS)) {
      if (!this._controlPlaneAllowed(threadId, openingGeneration)) {
        throw new Error('connection cancelled');
      }
      try {
        const connection = await this._fetchConnection(threadId);
        if (!this._controlPlaneAllowed(threadId, openingGeneration)) {
          throw new Error('connection cancelled');
        }
        return connection;
      } catch (err: any) {
        if (
          this._isSessionEndedError(err) ||
          this._isSessionEndingError(err) ||
          this._sessionBindingInvalidGeneration(err) !== null
        ) {
          throw err;
        }
        if (err?.status === 425 || err?.status === 409) {
          await new Promise((r) => setTimeout(r, interval));
          if (!this._controlPlaneAllowed(threadId, openingGeneration)) {
            throw new Error('connection cancelled');
          }
          interval = Math.min(2_000, interval + 250);
          continue;
        }
        throw err;
      }
    }
    throw new Error('session preparation timed out');
  }

  private async _fetchConnection(threadId: string): Promise<ConnectionPayload> {
    return await firstValueFrom(
      this.http.get<ConnectionPayload>(`${environment.apiUrl}/sessions/${threadId}/connection`),
    );
  }

  private _acceptBindingRecoveryConnection(
    threadId: string,
    connection: ConnectionPayload,
  ): boolean {
    const recovery = this.bindingRecoveryRuntime;
    if (recovery?.threadId !== threadId) return true;
    const responseGeneration =
      connection?.pinned_runtime_generation_contract === 1
        ? this._canonicalRuntimeGeneration(connection.session_runtime_generation)
        : null;
    // The lifecycle event is only a wake-up edge. Installation still needs
    // the exact /connection contract, and that response may name a generation
    // newer than the candidate if recovery rotated again while the GET ran.
    if (!responseGeneration || responseGeneration === recovery.rejectedGeneration) {
      this._latchSessionBindingInvalid(threadId, recovery.rejectedGeneration);
      return false;
    }
    this.bindingRecoveryRuntime = null;
    this.connectionState.set(
      this.sse?.readyState === EventSource.OPEN ? 'connected' : 'connecting',
    );
    return true;
  }

  private _installControlTransport(threadId: string, connection: ConnectionPayload): void {
    if (!this._controlPlaneAllowed(threadId)) return;
    const exactRuntimeContract = connection?.pinned_runtime_generation_contract === 1;
    if (exactRuntimeContract) this.runtimeGenerationContractThreads.add(threadId);
    this.sessionRuntimeGeneration = exactRuntimeContract
      ? this._canonicalRuntimeGeneration(connection.session_runtime_generation)
      : null;
    this.controlCapabilities.set(
      connection?.controls && typeof connection.controls === 'object'
        ? {
            threadId,
            controls: { ...connection.controls },
            options:
              connection.control_options && typeof connection.control_options === 'object'
                ? { ...connection.control_options }
                : {},
          }
        : null,
    );
    if (Number.isInteger(connection.conversation_revision)) {
      this.conversationRevision.update((current) =>
        Math.max(current, connection.conversation_revision as number),
      );
    }
    const marker = this._readRewindMarker(threadId);
    if (marker && !marker.prefillConsumed && !this.rewindInFlight()) {
      // Receipt reads remain available even after new admission is disabled.
      void this._lookupRewindReceipt(marker, true);
    }
    if (!this._connectionHasWebSocket(connection)) {
      this.controlSocket = 'none';
      this.controlWsReconnectAttempt = 0;
      if (this.controlWsReconnectTimer) {
        clearTimeout(this.controlWsReconnectTimer);
        this.controlWsReconnectTimer = null;
      }
      this._stopControlWsWatchdog();
      // Anything queued while the transport was still unknown was waiting
      // for a socket this session will never have. Fail it now, loudly —
      // the old behaviour left such frames in the outbox forever.
      this._failQueuedControls(threadId);
      return;
    }
    this.controlSocket = 'websocket';
    this._installControlWs(threadId, connection.ws_url);
  }

  /** The transport a control verb has on the CURRENT session.
   *
   *  Answers from the `/connection` declaration when there is one. Without
   *  it (an orchestrator predating `controls`) the answer is derived from the
   *  legacy contract, which is exactly what such a server implements; and
   *  before `/connection` resolved at all the verb is assumed socket-bound,
   *  so it queues and is either flushed or failed once the transport is
   *  known (see `_installControlTransport`). */
  controlTransport(verb: string): ControlTransport {
    const threadId = this.threadId();
    const declared = this.controlCapabilities();
    if (declared && threadId && declared.threadId === threadId) {
      return declared.controls[verb] ?? 'unavailable';
    }
    if (this.controlSocket === 'none') {
      return LEGACY_SOCKETLESS_REST_VERBS.has(verb) ? 'rest' : 'unavailable';
    }
    // Pinned (or not yet resolved): the scalars ride REST on both lanes;
    // everything else is a socket verb, including `config.update`, whose
    // pinned owner PATCH is refused while an agent is bound.
    if (verb === 'mode.set' || verb === 'narration.set') return 'rest';
    return 'websocket';
  }

  /** Whether the currently declared session supports this rewind mode. */
  rewindModeAvailable(mode: 'both' | 'conversation' | 'code'): boolean {
    const transport = this.controlTransport('rewind');
    if (transport === 'unavailable') return false;
    if (transport === 'websocket') return true;
    const declared = this.controlCapabilities();
    const modes = declared?.options['rewind']?.modes;
    return mode === 'conversation' && (!modes || modes.includes(mode));
  }

  summarizeAvailable(): boolean {
    return this.controlTransport('compact') === 'websocket';
  }

  /** Drop every control frame queued for `threadId` and tell the user. Runs
   *  when `/connection` resolves to a socketless session: the frames were
   *  queued under the pre-resolution assumption of a socket. */
  private _failQueuedControls(threadId: string): void {
    const queued = this.controlOutbox.filter((item) => item.threadId === threadId);
    if (queued.length === 0) return;
    this.controlOutbox = this.controlOutbox.filter((item) => item.threadId !== threadId);
    for (const item of queued) {
      let requestId: string | undefined;
      try {
        requestId = JSON.parse(item.frame)?.request_id;
      } catch {
        requestId = undefined;
      }
      if (requestId) {
        this._settlePendingConfigUpdate(requestId, {
          ok: false,
          requestId,
          transport: 'unavailable',
          message: this.transloco.translate('chat.control.unavailable'),
        });
      }
    }
    this.error.set(this.transloco.translate('chat.control.unavailable'));
  }

  private _settlePendingConfigUpdate(requestId: string, outcome: ConfigUpdateOutcome): boolean {
    const pending = this.pendingConfigUpdates.get(requestId);
    if (!pending) return false;
    clearTimeout(pending.timer);
    this.pendingConfigUpdates.delete(requestId);
    pending.resolve(outcome);
    return true;
  }

  private _settleAllPendingConfigUpdates(message: string): void {
    for (const requestId of Array.from(this.pendingConfigUpdates.keys())) {
      this._settlePendingConfigUpdate(requestId, {
        ok: false,
        requestId,
        transport: 'websocket',
        message,
      });
    }
  }

  private _connectionHasWebSocket(
    connection: ConnectionPayload,
  ): connection is Extract<ConnectionPayload, { control_socket: 'websocket' }> {
    // The static union protects our own callers, not a rolling deploy.
    // During rollout an older orchestrator has no discriminator but still
    // returns a valid pinned ws_url, so accept that legacy shape. An
    // explicit `none` always wins, and null/empty coordinates never reach
    // the browser's WebSocket constructor or reconnect ladder.
    return (
      connection?.control_socket !== 'none' &&
      typeof connection.ws_url === 'string' &&
      connection.ws_url.trim().length > 0
    );
  }

  private _installControlWs(threadId: string, wsUrl: string): void {
    if (!this._controlPlaneAllowed(threadId)) return;
    const ws = new WebSocket(wsUrl);
    this.controlWs = ws;
    this.controlWsLastMessageAt = Date.now();
    this._startControlWsWatchdog(threadId);
    ws.onopen = () => {
      if (this.controlWs !== ws || !this._controlPlaneAllowed(threadId)) return;
      this.controlWsReconnectAttempt = 0;
      this.controlWsLastMessageAt = Date.now();
      // Drain user-issued commands first — they've been waiting on this
      // socket. Must precede the canvas block, which early-returns.
      this._flushControlOutbox(threadId, ws);
      const pending = this.pendingCanvasSourceUpdate;
      if (!pending || pending.threadId !== threadId) return;
      try {
        ws.send(JSON.stringify(pending.control));
        if (this.pendingCanvasSourceUpdate === pending) {
          this.pendingCanvasSourceUpdate = null;
        }
      } catch {
        // Keep the latest committed revision for the next reconnect.
      }
    };
    ws.onclose = (event: CloseEvent) => {
      if (this.controlWs !== ws) return;
      this.controlWs = null;
      this._stopControlWsWatchdog();
      if (!this._controlPlaneAllowed(threadId)) return;
      // 4401 = expired/invalid token (Task 8). The agent is still
      // bound — just need a fresh token. Skip /prepare and re-fetch
      // /connection directly.
      if ((event && (event as CloseEvent).code) === 4401) {
        void this._reopenWithFreshToken(threadId);
        return;
      }
      // Tiny linear backoff — primary connection signal lives on the SSE
      // so we don't need the WS aggressively reconnecting.
      this._scheduleControlWsReconnect(threadId);
    };
    ws.onerror = () => {
      // The close handler will fire; nothing to do here.
    };
    // SSE is the canonical receive path for agent-emitted events. The
    // orchestrator's _broadcast() stamps every persisted event with
    // params._seq = [epoch, seq] before writing it to thread_events,
    // so any frame carrying _seq will be redelivered by SSE — we drop
    // those here to avoid double-dispatch.
    //
    // Frames WITHOUT _seq come from _ws_send() (per-client direct
    // sends, never persisted): orchestrator status frames during WS
    // startup (provisioning/booting/connecting), the agent's
    // session.state welcome frame, and control-plane acks
    // (mode.changed, narration.changed, interrupt.ack, vm_upgrade.*,
    // workspace_upgrade.*).
    // These never reach SSE, so the WS is the only path that delivers
    // them — and session.state is what flips sessionReady on a
    // reconnect to an already-idle loop where the cached SSE cursor
    // sits past the most recent `ready` event.
    ws.onmessage = (event: MessageEvent) => {
      if (this.controlWs !== ws || !this._controlPlaneAllowed(threadId)) return;
      // Any frame proves liveness — including ws.ping, whose only job
      // is feeding this watchdog.
      this.controlWsLastMessageAt = Date.now();
      let frame: { method?: string; params?: Record<string, unknown> };
      try {
        frame = JSON.parse(event.data);
      } catch {
        return;
      }
      if (!frame?.method || frame.method === 'ws.ping') return;
      if (frame.params && (frame.params as Record<string, unknown>)['_seq'] != null) {
        return;
      }
      this.zone.run(() =>
        this._handleEvent(frame as { method: string; params?: Record<string, unknown> }),
      );
    };
  }

  /**
   * Re-fetch /connection (skip /prepare — the agent is still bound) and
   * reopen the control WS with the fresh token. Triggered by a 4401
   * close on the WS.
   */
  private async _reopenWithFreshToken(threadId: string): Promise<void> {
    if (!this._controlPlaneAllowed(threadId)) return;
    if (this.controlWsOpening) return;
    const openingGeneration = ++this.controlWsOpeningGeneration;
    this.controlWsOpening = true;
    try {
      const connection = await this._fetchConnection(threadId);
      if (!this._controlPlaneAllowed(threadId, openingGeneration)) return;
      if (!this._acceptBindingRecoveryConnection(threadId, connection)) return;
      this._applyQueueState(threadId, connection.queue ?? null);
      if (this._connectionHasWebSocket(connection) || this.sessionSnapshotLoaded) {
        this.markSessionReady();
      }
      this._installControlTransport(threadId, connection);
    } catch (err) {
      const invalidBindingGeneration = this._sessionBindingInvalidGeneration(err);
      if (
        invalidBindingGeneration !== null &&
        openingGeneration === this.controlWsOpeningGeneration &&
        this.threadId() === threadId
      ) {
        this._latchSessionBindingInvalid(threadId, invalidBindingGeneration);
      } else if (
        (this._isSessionEndedError(err) || this._isSessionEndingError(err)) &&
        openingGeneration === this.controlWsOpeningGeneration &&
        this.threadId() === threadId
      ) {
        this._retireFromSessionRefusal(threadId, err);
      } else if (this._controlPlaneAllowed(threadId, openingGeneration)) {
        this._scheduleControlWsReconnect(threadId);
      }
    } finally {
      if (openingGeneration === this.controlWsOpeningGeneration) {
        this.controlWsOpening = false;
      }
    }
  }

  /** Force-close a half-open control WS that stopped delivering frames.
   *  close() fires onclose locally even when the peer is unreachable, so
   *  the regular reconnect ladder (with a fresh /connection token) takes
   *  over from there. */
  private _startControlWsWatchdog(threadId: string): void {
    this._stopControlWsWatchdog();
    this.controlWsWatchdogTimer = setInterval(() => {
      if (!this._controlPlaneAllowed(threadId)) {
        this._stopControlWsWatchdog();
        return;
      }
      const ws = this.controlWs;
      if (!ws || ws.readyState !== WebSocket.OPEN) return;
      if (Date.now() - this.controlWsLastMessageAt > CONTROL_WS_WATCHDOG_TIMEOUT_MS) {
        console.warn('[persistent-chat] control WS silent past watchdog — forcing reconnect');
        ws.close(4002, 'heartbeat timeout');
      }
    }, CONTROL_WS_WATCHDOG_INTERVAL_MS);
  }

  private _stopControlWsWatchdog(): void {
    if (this.controlWsWatchdogTimer) {
      clearInterval(this.controlWsWatchdogTimer);
      this.controlWsWatchdogTimer = null;
    }
  }

  private _scheduleControlWsReconnect(threadId: string): void {
    if (!this._controlPlaneAllowed(threadId)) return;
    if (this.controlSocket === 'none') return;
    if (this.controlWsReconnectAttempt >= CONTROL_WS_RECONNECT_MAX_ATTEMPTS) {
      // Give up silently; user actions that need the WS will reopen
      // on demand via _ensureControlWs.
      return;
    }
    const idx = this.controlWsReconnectAttempt;
    const delay =
      CONTROL_WS_RECONNECT_DELAYS_MS[Math.min(idx, CONTROL_WS_RECONNECT_DELAYS_MS.length - 1)];
    this.controlWsReconnectAttempt = idx + 1;
    this.controlWsReconnectTimer = setTimeout(() => {
      this.controlWsReconnectTimer = null;
      if (!this._controlPlaneAllowed(threadId)) return;
      void this._openControlWs(threadId);
    }, delay);
  }

  /** Open a control WS on demand if one isn't already open. Used by
   *  slash-command and permission paths when the user clicks during a
   *  brief reconnect window. */
  private _ensureControlWs(): void {
    const tid = this.threadId();
    if (!tid) return;
    if (!this._controlPlaneAllowed(tid)) return;
    if (this.controlSocket === 'none') return;
    if (this.controlWs?.readyState === WebSocket.OPEN) return;
    if (this.controlWs?.readyState === WebSocket.CONNECTING) return;
    if (this.controlWsOpening) return;
    this.controlWsReconnectAttempt = 0;
    void this._openControlWs(tid);
  }

  /** Admit one of the deliberately small durable-control subset over REST.
   *
   * Scalar assignments are lane-free. The /undo caller enters this path only
   * after /connection declared a socketless transport; pinned undo stays on
   * its legacy direct WebSocket path. The HTTP 202 means only that the
   * orchestrator committed the request. It must not synthesize owner state;
   * the serving owner writes the acknowledgement through its journal
   * allocator and the normal SSE reducer applies it.
   */
  private _sendDurableControl(control: DurableControl): void {
    const threadId = this.threadId();
    if (!threadId || !this._controlPlaneAllowed(threadId)) return;
    const runtimeGeneration = this.sessionRuntimeGeneration;
    if (!runtimeGeneration) {
      this._setDurableControlError(
        { method: control.method, ordinal: ++this.durableControlOrdinal },
        this.transloco.translate('chat.control.admissionFailed'),
      );
      return;
    }
    const ordinal = ++this.durableControlOrdinal;
    // Keep an ambiguous/in-flight head exactly where it is, but collapse
    // later unsubmitted assignments of the same scalar to the user's
    // newest intent. Removing then appending preserves order relative to
    // the other scalar.
    if (isDurableScalarControl(control)) {
      for (let index = this.durableControlOutbox.length - 1; index >= 0; index--) {
        const queued = this.durableControlOutbox[index];
        const ambiguousRetryHead =
          this.durableControlRetryTimer !== null && queued === this.durableControlOutbox[0];
        if (
          queued !== this.durableControlInFlight &&
          !ambiguousRetryHead &&
          queued.threadId === threadId &&
          queued.request.method === control.method
        ) {
          this.durableControlAwaitingAck.delete(queued.request.client_request_id);
          this.durableControlOutbox.splice(index, 1);
          break;
        }
      }
    }
    if (this.durableControlOutbox.length >= PersistentChatService.DURABLE_CONTROL_OUTBOX_MAX) {
      this._setDurableControlError(
        { method: control.method, ordinal },
        this.transloco.translate('chat.control.backpressure'),
      );
      return;
    }
    const item: DurableControlOutboxItem = {
      threadId,
      request: {
        ...control,
        client_request_id: crypto.randomUUID(),
        session_runtime_generation: runtimeGeneration,
      },
      attempts: 0,
      ordinal,
    };
    // Register before the HTTP request starts. The serving owner can
    // journal the result over SSE before the orchestrator's 202 reaches
    // this tab (especially on an idempotent retry).
    this.durableControlAwaitingAck.set(item.request.client_request_id, {
      method: item.request.method,
      ordinal: item.ordinal,
    });
    this.durableControlOutbox.push(item);
    this._flushDurableControlOutbox();
  }

  /** Single-flight FIFO drain. An ambiguous failure leaves the item at the
   * head and therefore also holds every later setting behind it. Reusing the
   * UUID makes a masked commit safe: the orchestrator returns the original
   * admission instead of allocating a second request sequence. */
  private _flushDurableControlOutbox(): void {
    if (this.durableControlInFlight || this.durableControlRetryTimer) return;

    let item = this.durableControlOutbox[0];
    while (item && (this.intentionalClose || item.threadId !== this.threadId())) {
      this.durableControlOutbox.shift();
      item = this.durableControlOutbox[0];
    }
    if (!item) return;

    this.durableControlInFlight = item;
    item.attempts += 1;
    let settledSynchronously = false;
    let response$: Observable<unknown>;
    try {
      response$ = this.http
        .post(`${environment.apiUrl}/persistent/threads/${item.threadId}/controls`, item.request)
        .pipe(timeout({ first: PersistentChatService.DURABLE_CONTROL_RESPONSE_TIMEOUT_MS }));
    } catch (err) {
      settledSynchronously = true;
      this._handleDurableControlFailure(item, err);
      return;
    }

    const subscription = response$.subscribe({
      next: () => {
        settledSynchronously = true;
        this._finishDurableControlAdmission(item);
      },
      error: (err: unknown) => {
        settledSynchronously = true;
        this._handleDurableControlFailure(item, err);
      },
      // HttpClient normally emits one body and completes. Treat a valid
      // empty 202 response as admitted too, without relying on a body.
      complete: () => {
        settledSynchronously = true;
        this._finishDurableControlAdmission(item);
      },
    });
    if (!settledSynchronously && this.durableControlInFlight === item) {
      this.durableControlSubscription = subscription;
    } else {
      subscription.unsubscribe();
    }
  }

  private _finishDurableControlAdmission(item: DurableControlOutboxItem): void {
    if (this.durableControlInFlight !== item) return;
    this.durableControlSubscription?.unsubscribe();
    this.durableControlSubscription = null;
    this.durableControlInFlight = null;
    if (this.durableControlOutbox[0] === item) {
      this.durableControlOutbox.shift();
    }
    // Deliberately ignore the response body. Only the owner's durable
    // journal acknowledgement is authoritative client state.
    this._flushDurableControlOutbox();
  }

  private _handleDurableControlFailure(item: DurableControlOutboxItem, err: unknown): void {
    if (this.durableControlInFlight !== item) return;
    this.durableControlSubscription?.unsubscribe();
    this.durableControlSubscription = null;
    this.durableControlInFlight = null;

    if (
      this._isRetryableDurableControlFailure(err) &&
      !this.intentionalClose &&
      this.threadId() === item.threadId &&
      this.durableControlOutbox[0] === item
    ) {
      const delays = PersistentChatService.DURABLE_CONTROL_RETRY_DELAYS_MS;
      const delay = delays[Math.min(item.attempts - 1, delays.length - 1)];
      console.warn(
        '[persistent-chat] control admission outcome unknown; retrying ' +
          `${item.request.method} with the same request id`,
      );
      this.durableControlRetryTimer = setTimeout(() => {
        this.durableControlRetryTimer = null;
        this._flushDurableControlOutbox();
      }, delay);
      return;
    }

    if (this.durableControlOutbox[0] === item) {
      this.durableControlOutbox.shift();
    }
    this.durableControlAwaitingAck.delete(item.request.client_request_id);
    if (!this.intentionalClose && this.threadId() === item.threadId) {
      const detail = (err as { error?: { detail?: unknown }; message?: unknown })?.error?.detail;
      this._setDurableControlError(
        { method: item.request.method, ordinal: item.ordinal },
        typeof detail === 'string'
          ? this.sanitizeError(detail)
          : this.transloco.translate('chat.control.admissionFailed'),
      );
    }
    this._flushDurableControlOutbox();
  }

  private _isRetryableDurableControlFailure(err: unknown): boolean {
    const status = (err as { status?: unknown })?.status;
    if (typeof status !== 'number') return true;
    return status === 0 || status === 408 || status === 425 || status === 429 || status >= 500;
  }

  private _setDurableControlError(marker: DurableControlMarker, message: string): void {
    this.durableControlError = { ...marker, message };
    this.error.set(message);
  }

  private _takeDurableControlAck(
    params: Record<string, unknown>,
    expectedMethod: DurableControl['method'],
  ): DurableControlMarker | null {
    const requestId = params['client_request_id'];
    if (typeof requestId !== 'string') return null;
    const marker = this.durableControlAwaitingAck.get(requestId);
    if (!marker || marker.method !== expectedMethod) return null;
    this.durableControlAwaitingAck.delete(requestId);
    return marker;
  }

  private _clearDurableControlErrorAfter(marker: DurableControlMarker | null): void {
    const current = this.durableControlError;
    if (
      !marker ||
      !current ||
      marker.method !== current.method ||
      marker.ordinal <= current.ordinal
    ) {
      return;
    }
    if (this.error() === current.message) {
      this.error.set(null);
    }
    this.durableControlError = null;
  }

  private _clearDurableControlOutbox(): void {
    if (this.durableControlRetryTimer) {
      clearTimeout(this.durableControlRetryTimer);
      this.durableControlRetryTimer = null;
    }
    this.durableControlSubscription?.unsubscribe();
    this.durableControlSubscription = null;
    this.durableControlInFlight = null;
    this.durableControlOutbox = [];
    this.durableControlAwaitingAck.clear();
    this.durableControlError = null;
  }

  /** Send a control-plane command over the session socket. If the WS isn't
   *  open, queue the frame and open one; the send goes out as soon as the
   *  connection establishes.
   *
   *  Returns false — and surfaces an error — when the verb has no socket
   *  transport on this session. That branch used to queue the frame for a
   *  socket a queue-served session never opens, which is how every settings
   *  edit on such a session silently vanished. A verb with no transport is
   *  refused here, at the one choke point, so a caller nobody rerouted fails
   *  visibly instead of quietly. */
  private _sendControl(data: Record<string, unknown>): boolean {
    const threadId = this.threadId();
    if (!threadId || !this._controlPlaneAllowed(threadId)) return false;
    const verb = typeof data['method'] === 'string' ? (data['method'] as string) : '';
    if (this.controlTransport(verb) !== 'websocket') {
      this.error.set(this.transloco.translate('chat.control.unavailable'));
      return false;
    }
    const frame = JSON.stringify(data);
    if (this.controlWs?.readyState === WebSocket.OPEN) {
      try {
        this.controlWs.send(frame);
        return true;
      } catch {
        // Socket died between the readyState read and the write. Fall
        // through and queue rather than losing the command.
      }
    }
    // Queue on the service, not on the socket. This used to attach a
    // one-shot 'open' listener to `this.controlWs` — but _ensureControlWs
    // only kicks off _openControlWs, which awaits _resolveConnection before
    // _installControlWs assigns the new socket. So the socket readable here
    // is still the OLD one: CLOSED (never fires 'open' again) or null (the
    // old code returned outright). Either way the frame vanished silently,
    // taking upgrade clicks, permission decisions and slash commands with
    // it. Only a CONNECTING socket ever worked.
    this.controlOutbox.push({ threadId, frame });
    if (this.controlOutbox.length > PersistentChatService.CONTROL_OUTBOX_MAX) {
      this.controlOutbox.shift();
    }
    this._ensureControlWs();
    return true;
  }

  /** Send a destructive control only on the socket that is open right now. */
  private _sendImmediateControl(data: Record<string, unknown>): boolean {
    const threadId = this.threadId();
    if (!threadId || !this._controlPlaneAllowed(threadId)) return false;
    const verb = typeof data['method'] === 'string' ? String(data['method']) : '';
    if (this.controlTransport(verb) !== 'websocket') {
      this.error.set(this.transloco.translate('chat.control.unavailable'));
      return false;
    }
    const ws = this.controlWs;
    if (!ws || ws.readyState !== WebSocket.OPEN) {
      this.error.set(this.transloco.translate('chat.rewind.connectionDown'));
      return false;
    }
    try {
      ws.send(JSON.stringify(data));
      return true;
    } catch {
      this.error.set(this.transloco.translate('chat.rewind.connectionDown'));
      return false;
    }
  }

  /** Drain frames queued for `threadId` over a freshly-opened socket.
   *
   * Frames tagged for any other thread are dropped rather than carried: a
   * control verb is a command about a specific moment ("approve *this*
   * permission"), so replaying one into another thread would act on the
   * wrong target. Anything still unsent (socket died mid-drain) is kept for
   * the next open. */
  private _flushControlOutbox(threadId: string, ws: WebSocket): void {
    if (this.controlOutbox.length === 0) return;
    const queued = this.controlOutbox;
    this.controlOutbox = [];
    for (const item of queued) {
      if (item.threadId !== threadId) continue;
      if (ws.readyState !== WebSocket.OPEN) {
        this.controlOutbox.push(item);
        continue;
      }
      try {
        ws.send(item.frame);
      } catch {
        this.controlOutbox.push(item);
      }
    }
  }

  /**
   * Canvas controls report acceptance to their caller. Unlike the legacy
   * best-effort control verbs, `true` therefore means the frame was written
   * to the currently-open socket, not merely that an async connection attempt
   * was started. A failed attempt still kick-starts the control transport so
   * a deliberate caller retry can succeed.
   */
  private _sendCanvasControl(threadId: string, data: CanvasControl): boolean {
    if (!this._controlPlaneAllowed(threadId)) return false;
    const ws = this.controlWs;
    if (!ws || ws.readyState !== WebSocket.OPEN) {
      if (isCommittedCanvasControl(data)) {
        this._queueCanvasSourceUpdate(threadId, data);
      }
      this._ensureControlWs();
      return false;
    }
    let outgoing = data;
    if (
      isCommittedCanvasControl(data) &&
      this.pendingCanvasSourceUpdate?.threadId === threadId &&
      this.pendingCanvasSourceUpdate.control.presentation_revision > data.presentation_revision
    ) {
      outgoing = this.pendingCanvasSourceUpdate.control;
    }
    try {
      ws.send(JSON.stringify(outgoing));
      if (isCommittedCanvasControl(outgoing)) {
        const pendingRevision = this.pendingCanvasSourceUpdate?.control.presentation_revision ?? -1;
        if (pendingRevision <= outgoing.presentation_revision) {
          this.pendingCanvasSourceUpdate = null;
        }
      }
      return true;
    } catch {
      if (isCommittedCanvasControl(data)) {
        this._queueCanvasSourceUpdate(threadId, data);
      }
      return false;
    }
  }

  private _queueCanvasSourceUpdate(
    threadId: string,
    control: CanvasSourceUpdatedControl | CanvasPresentationUpdatedControl,
  ): void {
    const pending = this.pendingCanvasSourceUpdate;
    if (
      pending?.threadId === threadId &&
      pending.control.presentation_revision > control.presentation_revision
    )
      return;
    this.pendingCanvasSourceUpdate = { threadId, control };
  }

  /** Cancel the current backoff wait and immediately reopen the SSE.
   *  Resets gave-up state. */
  reconnectNow(): void {
    if (this.intentionalClose) return;
    const tid = this.threadId();
    if (!tid) return;

    this.reconnectGaveUp.set(false);
    this.reconnectAttempt.set(0);
    if (this.sse) {
      this.sse.close();
      this.sse = null;
    }
    const bindingInvalid = this.invalidBindingRuntime?.threadId === tid;
    this.connectionState.set(bindingInvalid ? 'error' : 'connecting');
    if (bindingInvalid) {
      this.error.set(this.transloco.translate('errors.sessions.bindingInvalid'));
    }
    if (this.sessionSnapshotFailed) {
      const generation = this.connectGeneration;
      const preservedReplayCursor = this.sseReplayCursor;
      void this._retrySnapshotAndOpenSse(tid, generation, preservedReplayCursor);
    } else {
      void this._openSse(tid);
    }
  }

  private async _retrySnapshotAndOpenSse(
    threadId: string,
    generation: number,
    preservedReplayCursor: { epoch: number; seq: number } | null | undefined,
  ): Promise<void> {
    await this._loadSessionState(threadId, generation, preservedReplayCursor, true);
    if (!this._isCurrentConnect(threadId, generation) || this.intentionalClose) return;
    if (
      this.terminalControlThreadId !== threadId &&
      this.sessionSnapshotLoaded &&
      this.controlSocket === 'none'
    ) {
      this.markSessionReady();
    }
    await this._openSse(threadId);
  }

  /** Disconnect from the session. */
  disconnect(opts: { preserveReviewPlane?: boolean } = {}): void {
    const preserveReviewPlane = opts.preserveReviewPlane === true;
    // Invalidates cache/REST continuations from any in-flight connect even
    // when no transport has been installed yet.
    this.connectGeneration++;
    // Let a newer thread claim control-WS opening immediately; the stale
    // async resolver observes its invalidated generation before install.
    this.controlWsOpeningGeneration++;
    this.controlWsOpening = false;
    this.controlSocket = 'unknown';
    this.sessionRuntimeGeneration = null;
    if (!preserveReviewPlane) this.retiredRuntimeGeneration = null;
    this.sessionSnapshotLoaded = false;
    this.sessionSnapshotFailed = false;
    this.sessionSnapshotCursor = null;
    this.snapshotJoinsPreservedTurn = false;
    this.snapshotSeededUsage = false;
    this.sseReplayCursor = undefined;
    this.intentionalClose = true;
    this.isCreating.set(false);
    // An accepted send may still be running server-side, but with the
    // transport down there is no turn.started to clear it — never leave
    // the composer stuck on "working" after a teardown.
    this.pendingTurnCount.set(0);
    if (this.controlWsReconnectTimer) {
      clearTimeout(this.controlWsReconnectTimer);
      this.controlWsReconnectTimer = null;
    }
    this.controlWsReconnectAttempt = 0;
    this.pendingCanvasSourceUpdate = null;
    this._stopSseWatchdog();
    this._stopControlWsWatchdog();
    this._clearSendKickstart();
    if (this.cloudDiffRefreshTimer) {
      clearTimeout(this.cloudDiffRefreshTimer);
      this.cloudDiffRefreshTimer = null;
    }
    this.cloudDiffRequestOrdinal++;
    if (!preserveReviewPlane) {
      this._cancelTerminalCloudDiffProbe();
      this.terminalControlThreadId = null;
    }
    // Supersede any _openSse still awaiting its cursor read so it can't
    // resurrect a stream after we've torn down.
    this.sseGeneration++;
    if (this.sse) {
      this.sse.close();
      this.sse = null;
    }
    // Unlike the message outbox above, queued control frames DO get dropped
    // here. A control verb is scoped to a moment ("approve this permission",
    // "upgrade now"); replaying one after a thread switch would act on a
    // target that no longer exists. The socket-level reconnect loop
    // (onclose → _scheduleControlWsReconnect) never routes through
    // disconnect(), so an ordinary drop-and-reconnect still delivers.
    this.controlOutbox = [];
    this.controlCapabilities.set(null);
    // A config update still awaiting its socket ack belongs to the thread
    // being left; settle it as not-applied so the pane can roll back rather
    // than wait on a frame that will now never arrive here.
    this._settleAllPendingConfigUpdates(this.transloco.translate('chat.control.applyFailed'));
    // Interrupt retries carry an exact thread + turn target. Never let a
    // pending browser timer cross navigation even though a request that
    // already committed remains safely durable on its original thread.
    this._clearPendingInterruptRequest();
    // REST setting controls are moment-scoped too. Cancel the browser
    // request/timer and drop the queue on navigation; a request that had
    // already committed remains safe because its URL and durable UUID are
    // tied to the old thread and the owner will journal its result there.
    this._clearDurableControlOutbox();
    const controlWs = this.controlWs;
    this.controlWs = null;
    if (controlWs) {
      // Assignment does not cancel a browser callback already queued for
      // dispatch, so installed handlers also verify socket ownership.
      controlWs.onopen = null;
      controlWs.onmessage = null;
      controlWs.onerror = null;
      controlWs.onclose = null;
      try {
        controlWs.close(1000);
      } catch {
        // ignore
      }
    }
    this.reconnectAttempt.set(0);
    this.reconnectGaveUp.set(false);
    this.connectionState.set('disconnected');
    // Fold any buffered streamed deltas (and cancel their timer) before we
    // close the turn — otherwise a stale flush could fire ≤80ms later and
    // resurrect a `recovered:` bubble on a disconnected/thread-switched view.
    this._flushDeltas();
    // If a turn was streaming when we disconnected, mark it interrupted
    // so isStreaming flips to false and the bubble shows it stopped.
    this._closeActiveTurnIfAny('turn_interrupted');
    this.isWaitingForInput.set(false);
    this.sessionReady.set(false);
    this.startupPhase.set(null);
    // NOTE: the outbox is deliberately NOT cleared here. Transport teardown
    // (thread creation calls disconnect() before connect()) must never
    // destroy queued sends — that was the root of the "Creating thread"
    // swallow. Only a genuine thread switch (connect() cold path) clears it.
    this.pendingPermissions.set([]);
    this.permissionResolutionFailures.clear();
    this.compaction.set(null);
    // The workspace-upgrade signals are per-thread and must not bleed across
    // a switch. workspaceUpgradeInProgress especially: disconnect() closes
    // the control WS, and the terminal workspace_upgrade.complete/.failed
    // frames only ever arrive over that socket (_ws_send, not _broadcast),
    // so leaving it set would spin forever against the wrong thread.
    this.workspaceUpgradeInProgress.set(null);
    this.pendingWorkspaceOffer.set(null);
    this.continueAfterUpgrade.set(false);
    this.sessionTitle.set(null);
    this.modelName.set(null);
    this.isOfficerThread.set(false);
    this.temperature.set(0);
    this.turnCount.set(0);
    this.ncSessionFolder.set(null);
    this.cloudSessionUrl.set(null);
    this.sshHandle.set(null);
    this.threadStatus.set(null);
    this.endedAt.set(null);
    this.retirementDisposition.set(null);
    this.tasks.set([]);
    this.undoAvailable.set(false);
    this.rewindInFlight.set(false);
    this.rewindPrefill.set(null);
    this.rewindOutcomeUnknown.set(false);
    this.pendingRewindRequestId = null;
    this._clearRewindAckFallback();
    this.isSessionPaused.set(false);
    this.pendingDrift.set(null);
    if (!preserveReviewPlane) {
      this._protectedCloud.set(false);
      this.cloudChangesCount.set(0);
      this.protectedMountName.set(null);
      this.cloudStagedAt.set(null);
      this.cloudDiffPanelOpen.set(false);
      this.threadMounts.set([]);
      this.protectedFolderLink.set(null);
      this.cloudDiffProbe.set('idle');
    }
    // NOTE: resumedFromEpoch is deliberately NOT cleared here. connect()
    // calls disconnect() first, and a resume sets the watermark *before*
    // connect() — clearing it here would wipe it before the reopened
    // stream replays the very frames it exists to suppress. Cleared on a
    // genuine thread switch instead (connect's cold path).
  }

  /**
   * Resume an ended session and reconnect.
   *
   * /resume is required for ended threads — it flips status from 'ended'
   * → 'created' and clears the stale agent_id, kicking off a background
   * reprovision. From there we just call `connect()`, which routes through
   * `_openControlWs → _resolveConnection`: a `GET /connection` 425 falls
   * through to `_waitForLifecycleReady` driving `POST /prepare` against
   * the lifecycle SSE feed. The advisory lock in the orchestrator
   * serialises /resume's reprovision with /prepare's _do_prepare so we
   * don't double-provision (knowledge-base/knowledge/issues/persistent_thread_double_provisioning_race.md).
   *
   * /resume can also 428 if a connector/project/grant the thread depended
   * on has since disappeared (config drift). That surfaces on
   * `pendingDrift` instead of `connect()`-ing against a still-ended
   * thread; the caller re-invokes this method with `acknowledge` (the
   * drift ids the user confirmed) once they've decided. Only the server's
   * typed ``session_not_ended`` 409 is a benign double-click; protected-cloud
   * and other 409 refusals stay visible and leave the ended review plane in
   * place.
   */
  async resumeSession(acknowledge?: string[]): Promise<void> {
    const threadId = this.threadId();
    if (!threadId) return;
    const generation = this.connectGeneration;
    this.isSessionPaused.set(false);
    this.isResuming.set(true);
    // Capture the epoch we may resume FROM, but do not publish the watermark
    // until POST /resume actually succeeds. A local terminal frame can arrive
    // before backend retirement begins; its session_not_ended response is not
    // evidence that a successor life exists.
    let candidateResumeEpoch: number | null = null;
    // The reopened stream replays that epoch's tail — ending in the
    // idle_timeout/ended pair that put us here — and those frames must not
    // re-pin the ended UI over the live session. Cleared on a genuine
    // thread switch (connect's cold path) and on disconnect().
    const localCursor = this.sseReplayCursor;
    if (localCursor && Number.isFinite(localCursor.epoch)) {
      // This tab has already folded this cursor. Prefer it to IndexedDB:
      // the cache write is deliberately fire-and-forget and may be stale or
      // unavailable, while losing the watermark lets an old terminal frame
      // re-pin the freshly resumed control generation.
      candidateResumeEpoch = localCursor.epoch;
    } else {
      try {
        const cursor = await this.cache.getThreadCursor(threadId);
        if (cursor && Number.isFinite(cursor.epoch)) {
          candidateResumeEpoch = cursor.epoch;
        }
      } catch {
        // No cursor (fresh load/private mode) → nothing to replay past.
      }
    }
    try {
      try {
        await firstValueFrom(
          this.http.post(
            `${environment.apiUrl}/persistent/threads/${threadId}/resume`,
            acknowledge ? { acknowledge } : {},
          ),
        );
        // A late success for a thread the user has already navigated
        // away from must not clear the CURRENT thread's just-shown
        // drift dialog — same currency guard as the catch below.
        if (this._isCurrentConnect(threadId, generation)) this.pendingDrift.set(null);
      } catch (err) {
        // A late failure for a thread the user has already navigated
        // away from must not mutate the shared error/drift signals —
        // they are current-thread-scoped.
        if (!this._isCurrentConnect(threadId, generation)) return;
        const outcome = classifyResumeError(err);
        if (outcome.kind === 'drift') {
          // Surface the dialog and stop: connect() against a still-
          // ended thread would achieve nothing.
          this.pendingDrift.set(outcome.items);
          return;
        }
        if (outcome.kind === 'error') {
          this.error.set(this.errors.translate(err, 'errors.sessions.resumeFailed'));
          return;
        }
        // `session_not_ended` can mean the agent journaled its terminal frame
        // just before orchestrator retirement began. Keep SSE, review, probes,
        // terminal latch and replay semantics unchanged; reconnecting here
        // would revive G1 control and suppress its later staged-diff event.
        if (this.terminalControlThreadId === threadId) {
          this.error.set(this.transloco.translate('errors.sessions.stillEnding'));
          void this.loadThreadMeta(threadId, generation);
          if (this._protectedCloud()) void this.refreshCloudDiffCount();
        } else {
          this.error.set(this.errors.translate(err, 'errors.sessions.resumeFailed'));
        }
        return;
      }
      if (!this._isCurrentConnect(threadId, generation)) return;
      this.resumedFromEpoch = candidateResumeEpoch;
      // This is the sole transition that reopens a terminal control epoch.
      // The old epoch's queued approvals/interrupts/upgrades were discarded
      // at retirement; preserve only the durable review plane until the new
      // summary/meta reads reconcile it.
      this._reopenTerminalControl(threadId);
      await this.connect(threadId, { preserveReviewPlane: true });
    } finally {
      // connect() leaves connectionState connecting/connected, so
      // isStartingSession takes over from here and the composer never
      // sees a gap.
      this.isResuming.set(false);
    }
  }

  /**
   * End the active session: DELETE the thread server-side (soft — status
   * flips to 'ended', workspace is snapshotted, agent pod is released) and
   * retire the local runtime/control plane. The durable SSE + protected
   * review plane stays open because staging can finish after DELETE returns.
   * The thread row + Gitea repo + cloud session folder are kept so the user
   * can /resume later.
   *
   * An untyped DELETE failure is ambiguous: the request may not have reached
   * the server, or End may have committed while its response was lost. Keep
   * all planes intact until SSE/REST authoritatively observes terminal state.
   */
  async endSession(force = false): Promise<void> {
    const threadId = this.threadId();
    if (!threadId) {
      this.disconnect();
      return;
    }
    let outcome: unknown;
    try {
      const qs = force ? '?force=true' : '';
      outcome = await firstValueFrom(
        this.http.delete<{ status?: unknown; retirement_disposition?: unknown }>(
          `${environment.apiUrl}/persistent/threads/${threadId}${qs}`,
        ),
      );
    } catch (err: unknown) {
      if (this._isTurnInFlightEndError(err) && !force) {
        // Mid-turn guard (session_silent_failure_audit.md #11): the
        // orchestrator refuses to tear down a session whose agent is
        // mid-turn unless forced. Declining keeps the session alive.
        const proceed = confirm(this.transloco.translate('sessions.confirmEndMidTurn'));
        if (proceed) {
          await this.endSession(true);
        }
        return;
      }
      throw err;
    }
    if (this.threadId() !== threadId) return;
    this.intentionalClose = false;
    const body = outcome as { status?: unknown; retirement_disposition?: unknown } | null;
    // The owner endpoint is asynchronous for a live pinned runtime: `ending`
    // proves admission is closed, not that cleanup/staging settled. Never
    // manufacture endedAt/Resume from that acknowledgement. A terminal SSE
    // may win before this response; the ending helper deliberately cannot
    // regress an already ended/suspended UI.
    if (body?.status === 'ending') {
      this._retireEndingControl(threadId, body.retirement_disposition);
    } else if (body?.status === 'ended') {
      this.retirementDisposition.set('ended');
      this._retireTerminalControl(threadId);
    } else if (body?.status === 'suspended') {
      this._settleSuspendedControl(threadId);
    }
    // A DELETE can win before the initial EventSource was installed. Keep an
    // SSE-only ending/ended view so late archive/staging events remain discoverable.
    if (!this.sse) void this._openSse(threadId);
  }

  /**
   * Queue files in the composer — and, when the thread can already take them,
   * start uploading immediately (§5.4).
   *
   * Most users attach first and type second, so starting here usually means
   * the bytes are in the workspace before they finish the sentence. When the
   * preconditions don't hold the file simply waits and uploads at flush time:
   * the deferred path is the fallback and is never removed.
   *
   * Duplicates are refused rather than attached. The backend has no upload
   * idempotency, so a second chip for the same file becomes a second file
   * (`report_1.pdf`) that only the delete endpoint can take back.
   */
  addAttachments(previews: FilePreview[]): void {
    if (!previews.length) return;

    // A file with no name has no identity to compare, so it is never a
    // duplicate — better to attach one file twice than to refuse a real one.
    const keyOf = (p: FilePreview) => (p.file?.name ? attachmentDedupeKey(p.file) : null);
    const seen = new Set(
      this.pendingAttachments()
        .map(keyOf)
        .filter((k): k is string => k !== null),
    );
    const accepted: FilePreview[] = [];
    const duplicates: string[] = [];
    for (const preview of previews) {
      const key = keyOf(preview);
      if (key !== null && seen.has(key)) {
        duplicates.push(preview.name);
        continue;
      }
      if (key !== null) seen.add(key);
      accepted.push(preview);
    }

    if (accepted.length > 0) {
      this.pendingAttachments.update((existing) => [...existing, ...accepted]);
    }
    // Cleared-then-set, in that order: a selection that both accepts some
    // files and rejects others must keep the rejection visible. (The
    // component relies on this clearing to report its OWN rejections after
    // calling us — see applyFilePreviews.)
    this.attachmentError.set(
      duplicates.length > 0
        ? this.transloco.translate('chat.upload.duplicateFile', { name: duplicates[0] })
        : null,
    );

    if (!this.canUploadEagerly()) return;
    const threadId = this.threadId();
    if (!threadId) return;
    // One `start` per file, but NOT one request per file on the wire: the
    // registry gates them (spec §5.3 — at most MAX_CONCURRENT_UPLOADS in
    // flight, and same-named files strictly serialized so the second can
    // never truncate the first). Selecting twenty files here opens two
    // requests, not twenty.
    for (const preview of accepted) this.uploads.start(threadId, preview);
  }

  /**
   * Whether an attached file can start uploading before the user sends
   * (§5.4). A thread has to exist to upload into, the session has to be ready
   * (the workspace is provisioned lazily and 409s until it is), and a `none`
   * tier has no workspace at all — that one is a permanent refusal, not a
   * wait.
   */
  private canUploadEagerly(): boolean {
    return (
      this.threadId() !== null &&
      this.sessionReady() &&
      this.workspaceTier() !== 'none' &&
      this.threadStatus() !== 'ended' &&
      this.threadStatus() !== 'ending'
    );
  }

  /** Drop one queued attachment by id, cancelling its upload if one is
   *  running (abort while the bytes are moving; delete what already
   *  landed — UploadRegistryService.cancel). */
  removeAttachment(id: string): void {
    this.pendingAttachments.update((list) => list.filter((p) => p.id !== id));
    this.uploads.cancel(id);
  }

  /**
   * Drop all queued attachments WITHOUT cancelling their uploads.
   *
   * This is the send path's clearing: the chips leave the composer because
   * they have moved onto the outbox item, which adopts their in-flight
   * uploads a moment later. Cancelling here would abort the very transfer the
   * send is about to await. Use `_discardComposerAttachments` for a
   * transition that genuinely abandons them.
   */
  clearAttachments(): void {
    this.pendingAttachments.set([]);
  }

  /**
   * Abandon everything queued in the composer: cancel the uploads, drop the
   * chips, clear the banner.
   *
   * Runs on every thread transition. Before eager upload the chips merely
   * *followed* the user from thread to thread (nothing cleared them); with
   * it, leaving them would put bytes in the wrong workspace and let an
   * upload resolve into a foreign queue.
   */
  private _discardComposerAttachments(): void {
    this.uploads.abortAll();
    this.pendingAttachments.set([]);
    this.attachmentError.set(null);
  }

  /** Send a user message (with slash command parsing).
   *  If the session isn't ready yet, queues the message and sends it
   *  automatically once the agent signals readiness.
   *
   *  When ``pendingAttachments`` is non-empty the files are NOT uploaded
   *  here: they ride the outbox item as ``pendingFiles`` and upload as
   *  stage 0 of the flush (``_uploadStage``), which then appends a hint
   *  listing the server-confirmed filenames to the text the agent receives.
   *  The displayed user message keeps the original text and shows the files
   *  as attachment chips — present from the moment the user hits send, not
   *  from the moment the bytes land.
   */
  async sendMessage(content: string): Promise<boolean> {
    const trimmed = content.trim();
    const queued = this.pendingAttachments();
    if (this.isDraftSession() && (
      this.draftDefaultsLoading() || this.draftDefaultsError() || this.draftDatasourceIds() === null
    )) return false;

    // Soft End has already closed runtime admission but has not yet settled
    // into a resumable lifecycle. Never queue a message that could leak into
    // the retiring generation or flush into its successor.
    if (this.threadStatus() === 'ending') return false;

    // Slash commands and attachments don't mix. This sits ABOVE the
    // bypass, not below it: a *recognized* command (`/compact`) returns
    // inside the bypass, which used to leave the chips sitting in the
    // composer with no message and no error — the strand named in
    // knowledge-base/knowledge/features/session_attachment_send_flow.md §2. Refuse explicitly
    // instead, so no slash path can silently swallow queued files.
    if (trimmed.startsWith('/') && queued.length > 0) {
      this.attachmentError.set(this.transloco.translate('chat.upload.slashCommandWithAttachments'));
      return false;
    }

    // Slash commands bypass attachment logic.
    if (trimmed.startsWith('/')) {
      if (this.handleSlashCommand(trimmed)) return true;
    }

    if (!trimmed && queued.length === 0) return true;

    // An editable Office frame owns its current in-memory document. Flush
    // that turn first so the agent's next read observes the coordinator-
    // committed revision rather than an older workspace snapshot.
    if (!(await this.canvas.prepareOfficeForUserMessage())) return false;

    // Local descriptors for the bubble. These exist BEFORE any byte moves —
    // that is the entire point of this design. `path` fills in per file as
    // the upload stage resolves it.
    const pendingFiles: PendingUpload[] = queued
      .filter((p) => p.file)
      .map((p) => ({
        id: p.id,
        file: p.file,
        name: p.name,
        size: p.size,
        mimeType: p.mimeType,
        loaded: 0,
        total: p.size || null,
        status: 'queued' as const,
      }));
    const attachments: ChatAttachment[] = pendingFiles.map((f) => ({
      id: f.id,
      name: f.name,
      size: f.size,
      mimeType: f.mimeType,
    }));

    // The user spoke, so they've resumed the agent themselves — drop any
    // queued auto-continuation rather than stacking a "continue where you
    // left off" behind whatever they just said. Safe against the
    // continuation's own send: workspace_upgrade.complete clears the flag
    // before calling us.
    this.continueAfterUpgrade.set(false);

    // ── One synchronous commit point ────────────────────────────────────
    // Bubble, queue entry and composer clearing happen together, with no
    // await between them. Previously the upload sat above this block, so
    // the text cleared at t=0 and the chips cleared at t=upload-complete,
    // with no bubble in between. Signal Desktop commits the same way
    // (register the message Pending, then upload inside the send job).
    //
    // Every send goes through the outbox — queued when the session isn't
    // ready yet, flushed immediately when it is. This serializes to one POST
    // in flight per tab (so two rapid sends can't collide on the default
    // turn_id and lose the second behind a 409), and the queue survives
    // reconnects. Bubble rollback (on 404/410) is the flush's job.
    const localId = makeLocalId('user');
    this.dispatch({
      type: 'user_message',
      id: localId,
      content: trimmed,
      attachments: attachments.length > 0 ? attachments : undefined,
      timestamp: Date.now(),
    });
    this.outbox.update((q) => [
      ...q,
      {
        localId,
        displayContent: trimmed,
        attachments: attachments.length > 0 ? attachments : undefined,
        pendingFiles: pendingFiles.length > 0 ? pendingFiles : undefined,
        // '' on the landing draft — no thread exists yet. The flush
        // pins it to the real id before the first upload.
        threadId: this.threadId() ?? '',
        attempts: 0,
        expectedConversationRevision: this.conversationRevision(),
      },
    ]);
    this.clearAttachments();
    this.attachmentError.set(null);
    // ────────────────────────────────────────────────────────────────────
    this.isWaitingForInput.set(false);
    if (this.isDraftSession()) {
      // First send from the landing draft: create the session now. The
      // queued item above flushes via markSessionReady like any early
      // send; no _flushOutbox needed here (session isn't ready yet).
      void this._createFromDraftSession(trimmed);
      return true;
    }
    if (this.threadStatus() === 'ended') {
      // Ended thread: SENDING resumes it. Typing deliberately does not — an
      // agent pod plus a workspace get reserved on resume, and every
      // half-written message would burn that. The queued item above rides
      // the resume exactly like a landing draft's first message rides thread
      // creation.
      void this.resumeSession();
      return true;
    }
    if (this.threadStatus() === 'suspended') {
      // Suspended is live-resumable rather than ended: /resume deliberately
      // accepts only ended rows. Reopen this tab's retired control moment and
      // let the ordinary connection/prepare path wake a fresh runtime. The
      // durable review plane and queued bubble stay intact, and the outbox is
      // flushed exactly once when the successor reports ready.
      const threadId = this.threadId();
      if (threadId) {
        this._reopenTerminalControl(threadId);
        void this.connect(threadId, { preserveReviewPlane: true });
      }
      return true;
    }
    void this._flushOutbox();
    return true;
  }

  /**
   * Drain the outbox FIFO with exactly one POST in flight (single-flight).
   * Only runs while the session is ready; otherwise items wait for the next
   * markSessionReady / sendMessage to retrigger. Deliberately has NO timed
   * auto-retry: the agent persists + enqueues input *before* returning 200,
   * so a transient 503 / pod-churn reset can mask an already-accepted send —
   * retrying would double-send. On a non-terminal failure we stop, leave the
   * item queued, and let the next trigger retry.
   *
   * Stage 0 of each item is its upload (_uploadStage): the bytes move here,
   * not in sendMessage, so the bubble and the composer never wait on them.
   */
  private async _flushOutbox(): Promise<void> {
    // Take the lock for THIS thread. A flush already running for another
    // thread (typically stuck on an abandoned upload) must not block us.
    const lockKey = this.threadId();
    if (!lockKey) return; // nothing is flushable without a thread
    if (this.flushTokens.has(lockKey)) return;
    const token = {};
    this.flushTokens.set(lockKey, token);
    try {
      while (!this.intentionalClose && this.sessionReady() && this.outbox().length > 0) {
        const head = this.outbox()[0];
        const tidAtPost = this.threadId();
        if (!tidAtPost) break;
        // Bump the attempt counter and, for a landing-draft item queued
        // before its thread existed (threadId ''), pin it to the thread
        // that now exists. Through the signal, never in place: the
        // bubble and (Slice 2) the stage line read this item.
        this.outbox.update((q) =>
          q.map((i) =>
            i.localId === head.localId
              ? {
                  ...i,
                  attempts: i.attempts + 1,
                  threadId: i.threadId || tidAtPost,
                }
              : i,
          ),
        );
        if (head.pendingFiles?.length) {
          const up = await this._uploadStage(head, tidAtPost);
          // Same guard the POST gets below: an upload is a much
          // longer await, so a thread switch mid-upload is likelier,
          // and resolving into a foreign queue is the exact
          // cross-thread mutation this phase exists to kill.
          if (this.threadId() !== tidAtPost) {
            queueMicrotask(() => void this._flushOutbox());
            return;
          }
          if (!up.ok) {
            if (up.status === 404 || up.status === 410) {
              // Thread gone. Already-uploaded bytes are abandoned
              // with it — acceptable, there is nothing to send
              // them to (spec §5.5).
              this.outboxStalled.set(false);
              this._drainOutboxWithRollback();
              return;
            }
            this.outboxStalled.set(true);
            return;
          }
        }
        // Re-read: _uploadStage wrote resolved paths through the signal,
        // so `head` is a stale snapshot of the attachments by now. Only
        // files the server confirmed go into the hint.
        const item = this.outbox().find((i) => i.localId === head.localId);
        // Gone from the queue while stage 0 ran — discarded, drained,
        // or carried to another thread. POSTing it now would send a
        // message the queue no longer believes in, and (on the A→B→A
        // path, where the thread ids match again so the guard below
        // cannot see the switch) would deliver stale text into a
        // re-opened thread. The upload stage widened that window from
        // the ~30s POST timeout to the length of a whole transfer.
        //
        // A guard AFTER _postInput would be worse, not better: the send
        // is already accepted server-side by then, so dropping the
        // resolution loses the removal and the next flush double-sends
        // — the exact failure the outbox's no-auto-retry rule exists to
        // prevent. Refuse before committing, never after.
        if (!item) return;
        const expectedRevision = item.expectedConversationRevision ?? 0;
        if (expectedRevision !== this.conversationRevision()) {
          this.outbox.update((queue) =>
            queue.map((queued) =>
              queued.localId === item.localId ? { ...queued, requiresReview: true } : queued,
            ),
          );
          this.outboxStalled.set(true);
          this.error.set(this.transloco.translate('chat.rewind.staleOutbox'));
          return;
        }
        const names = (item.attachments ?? []).filter((a) => a.path).map((a) => a.name);
        const content = composeAgentContent(head.displayContent, names);
        // Record what we're about to POST on the item itself, so the
        // queue can be inspected (and a retry compared) without
        // recomputing. Skipped when unchanged — a retry must not churn
        // the signal for nothing.
        if (item.content !== content) {
          this.outbox.update((q) =>
            q.map((i) => (i.localId === head.localId ? { ...i, content } : i)),
          );
        }
        this.postingLocalIds.add(head.localId);
        let result: { ok: boolean; status: number; stale?: boolean };
        try {
          result = await this._postInput(content, expectedRevision);
        } finally {
          this.postingLocalIds.delete(head.localId);
        }
        // The queue's identity may have changed while the POST was in
        // flight (thread switch, up to the ~30s forward timeout). If so,
        // drop this resolution: mutating another thread's queue here is
        // the cross-thread head-swap swallow this phase exists to kill.
        // Hand off to a fresh flush for whatever thread is current now —
        // its own markSessionReady-triggered flush may have been blocked
        // by this lock, and we don't want its queue to stall.
        if (this.threadId() !== tidAtPost) {
          queueMicrotask(() => void this._flushOutbox());
          return;
        }
        if (result.ok) {
          // The server durably accepted this exact input. Remove by
          // localId (never positionally: the head may have shifted)
          // and keep the bubble; SSE renders the turn.
          // Accepted-and-queued: keep the send visibly alive until its
          // turn.started lands.
          this.pendingTurnCount.update((c) => c + 1);
          this._removeFromOutbox(head.localId);
          // The send that produced any stall banner just landed, so
          // the banner is now lying. Clear both.
          this.outboxStalled.set(false);
          this.error.set(null);
          continue;
        }
        if (result.status === 404 || result.status === 410) {
          // Thread gone — draining is correct; roll back the bubbles.
          this.outboxStalled.set(false);
          this._drainOutboxWithRollback();
          return;
        }
        if (result.stale) {
          this.outbox.update((queue) =>
            queue.map((queued) =>
              queued.localId === item.localId ? { ...queued, requiresReview: true } : queued,
            ),
          );
        }
        // Any other failure: stop, keep the item + bubble queued. The
        // banner (_postInput set it) explains; next trigger retries.
        // Mark the queue stalled so the bubble offers retry/discard —
        // without it a failure here reads as "still sending" forever.
        this.outboxStalled.set(true);
        return;
      }
    } finally {
      // Only ours: a same-key lock taken by a later flush must survive.
      if (this.flushTokens.get(lockKey) === token) this.flushTokens.delete(lockKey);
    }
  }

  private _removeFromOutbox(localId: string): void {
    this.outbox.update((q) => q.filter((i) => i.localId !== localId));
  }

  /**
   * Stage 0 of a flush: upload any files this item still owes, one request
   * per file, and patch their resolved paths onto the item's attachments.
   *
   * Files that already resolved are skipped — the backend has no upload
   * idempotency (_claim_name resolves collisions with a _1 suffix against a
   * live listing), so re-uploading a success would silently duplicate it.
   *
   * Fail-fast, and files after the failure are left `queued`, NOT `failed`:
   * they were never attempted, and stamping them with the failing file's
   * message (what the pre-Task-4 loop did) is a lie the user then has to
   * debug. `queued` is the honest "not attempted" state.
   *
   * The upload goes through ApiService → HttpClient on purpose. A raw
   * XHR/fetch would miss auth.interceptor's `ngsw-bypass: 1` and the service
   * worker would corrupt the multipart body
   * (knowledge-history/done/cockpit_service_worker_breaks_file_uploads.md) — and a SW that
   * answers with respondWith() also destroys the very upload-progress events
   * this stage now reports.
   *
   * Progress: the observable emits `progress` events as the bytes move and
   * one `done` at the end. Progress is patched onto the PendingUpload —
   * throttled, see PROGRESS_WRITE_INTERVAL_MS — while `done` still resolves
   * the single await below, so every guard around it is unchanged.
   *
   * Returns the same {ok, status} shape as _postInput so the flush's existing
   * terminal-vs-retryable branching applies unchanged.
   */
  private async _uploadStage(
    head: OutboxItem,
    threadId: string,
  ): Promise<{ ok: boolean; status: number }> {
    const files = head.pendingFiles ?? [];
    if (files.every((f) => f.status === 'done')) return { ok: true, status: 200 };
    // Iterate ids, not objects: every _patchPendingFile below replaces the
    // whole pendingFiles array, so a captured element goes stale the moment
    // its own status is written. Re-read the live one each turn.
    const fileIds = files.map((f) => f.id);

    this.uploadingLocalIds.add(head.localId);
    try {
      for (const fileId of fileIds) {
        const f = this._pendingFile(head.localId, fileId);
        if (!f || f.status === 'done') continue;
        // The user left this thread mid-batch. Stop pushing bytes into
        // a workspace they've navigated away from — the flush's
        // post-stage guard (which runs before any stall bookkeeping)
        // drops this whole resolution anyway.
        if (this.threadId() !== threadId) return { ok: false, status: 0 };
        this._patchPendingFile(head.localId, f.id, {
          status: 'uploading',
          error: undefined,
          // A retry re-sends the whole file, so a `loaded` left over
          // from the failed attempt would park the bar high until the
          // first new progress event contradicted it.
          loaded: 0,
        });
        // Throttle state, per file and per attempt — scoped to this
        // iteration so nothing survives into the next file or leaks
        // onto the service. See PROGRESS_WRITE_INTERVAL_MS for why the
        // rate matters (the scroll pin re-pins synchronously).
        let lastProgressAt = 0;
        try {
          const results = await firstValueFrom(
            // ADOPT, never restart: an upload started when the file
            // was attached (§5.4) is usually already running or
            // already finished by now, and re-uploading a success
            // is a permanent duplicate (_claim_name suffixes it
            // `_1` and nothing but the delete endpoint cleans that
            // up). The registry hands back a stream shaped exactly
            // like uploadOneToThread's — including a fresh request
            // when there is nothing to adopt — so every guard
            // around this await is unchanged.
            this.uploads.adopt(threadId, f.id, f.file).pipe(
              tap((ev) => {
                if (ev.kind !== 'progress') return;
                const now = Date.now();
                if (!progressWriteDue(lastProgressAt, now)) return;
                lastProgressAt = now;
                // Through the signal, never in place — the
                // bubble's bar reads these. _patchPendingFile
                // no-ops once the item has left the queue.
                this._patchPendingFile(head.localId, f.id, {
                  loaded: ev.loaded,
                  total: ev.total,
                });
              }),
              filter(
                (ev): ev is Extract<ThreadUploadEvent, { kind: 'done' }> => ev.kind === 'done',
              ),
              map((ev) => ev.files),
            ),
          );
          // A .zip expands to one entry per extracted member, so one
          // PendingUpload can resolve into several ChatAttachments.
          const resolved: ChatAttachment[] = results.map((r, i) => ({
            id: i === 0 ? f.id : `${f.id}-${i}`,
            name: r.name,
            size: r.size,
            mimeType: r.mime_type,
            path: r.path,
          }));
          this._patchPendingFile(head.localId, f.id, {
            status: 'done',
            loaded: f.size,
            resolved: resolved[0],
          });
          this._mergeResolvedAttachments(head.localId, f.id, resolved);
        } catch (err) {
          // Intent BEFORE status. Angular reports a user abort and a
          // dead network identically as `status: 0`, so reading the
          // status first would turn a cancellation into "Network
          // error — check your connection" (spec §5.5: a cancel is
          // not an error and is filtered before humanizeUploadError).
          // `queued` is the honest state for a file nobody attempted.
          if (this.uploads.wasCancelled(f.id)) {
            this._patchPendingFile(head.localId, f.id, {
              status: 'queued',
              loaded: 0,
            });
            return { ok: false, status: 0 };
          }
          const status = (err as { status?: number })?.status ?? 0;
          const msg = this.api.humanizeUploadError(err);
          this._patchPendingFile(head.localId, f.id, { status: 'failed', error: msg });
          // 409 means two opposite things on this endpoint; only the
          // `none`-tier refusal is permanent, and only that one gets
          // the banner — a retryable failure is already explained by
          // the queued bubble's retry/discard affordance.
          //
          // Thread-gated: this runs BEFORE the flush's post-stage
          // guard, so without it a thread the user has already left
          // paints its 413 over the thread they just opened.
          if (this.threadId() === threadId && classifyUploadFailure(status, msg) === 'terminal') {
            this.error.set(msg);
          }
          return { ok: false, status };
        }
      }
    } finally {
      this.uploadingLocalIds.delete(head.localId);
    }
    return { ok: true, status: 200 };
  }

  /** The queue's live view of one of an item's files. Never hold the object
   *  across an await — each patch replaces the array it lives in. */
  private _pendingFile(localId: string, fileId: string): PendingUpload | undefined {
    return this.outboxItem(localId)?.pendingFiles?.find((f) => f.id === fileId);
  }

  /** Patch one queued file's upload state through the signal (fresh item,
   *  fresh file object — the bubble renders off these). */
  private _patchPendingFile(localId: string, fileId: string, patch: Partial<PendingUpload>): void {
    // Same guard as _mergeResolvedAttachments: once the item is gone (thread
    // switched, queue drained) the .map below matches nothing, and writing
    // its result would fire a pointless signal change on whatever queue the
    // tab is showing now.
    if (!this.outbox().some((i) => i.localId === localId)) return;
    this.outbox.update((q) =>
      q.map((i) =>
        i.localId === localId
          ? {
              ...i,
              pendingFiles: i.pendingFiles?.map((f) => (f.id === fileId ? { ...f, ...patch } : f)),
            }
          : i,
      ),
    );
  }

  /**
   * Replace one pre-upload chip with what the server actually stored: its
   * real path, its possibly-renamed name (`_1` collision suffix), and one
   * chip per extracted member for a .zip. Re-dispatches the bubble's chips
   * in place — a second `user_message` would append a duplicate bubble.
   */
  private _mergeResolvedAttachments(
    localId: string,
    fileId: string,
    resolved: ChatAttachment[],
  ): void {
    const item = this.outboxItem(localId);
    if (!item) return; // drained under us (thread gone / switched)
    const next = (item.attachments ?? []).flatMap((a) => (a.id === fileId ? resolved : [a]));
    this.outbox.update((q) =>
      q.map((i) => (i.localId === localId ? { ...i, attachments: next } : i)),
    );
    this.dispatch({ type: 'update_attachments', id: localId, attachments: next });
  }

  /**
   * User-initiated retry of a stalled queue. The flush deliberately has no
   * timed auto-retry (a masked accept would double-send), so when the
   * transport fails the only ways out are a reconnect or this. Exposed on
   * the queued bubble.
   */
  retryQueuedSends(): void {
    const threadId = this.threadId();
    if (threadId && this.invalidBindingRuntime?.threadId === threadId) {
      // Exact authority rejected this generation. Keep the item queued, but
      // never turn a user click into another send or erase the actionable
      // error while the durable successor owner is still converging.
      this.outboxStalled.set(true);
      this.error.set(this.transloco.translate('errors.sessions.bindingInvalid'));
      return;
    }
    this.outboxStalled.set(false);
    this.error.set(null);
    void this._flushOutbox();
  }

  /**
   * Drop one queued send and its optimistic bubble — the escape hatch for a
   * message the user no longer wants stuck in the queue.
   */
  discardQueuedSend(localId: string): void {
    // Refuse while the POST is in flight (its fate isn't decided) or while
    // the upload stage is running (dropping the item would orphan bytes in
    // the workspace with no way to delete them). Membership, not identity:
    // another thread's flush may be in flight at the same time and must not
    // be able to answer this question for us.
    if (this.postingLocalIds.has(localId)) return;
    if (this.uploadingLocalIds.has(localId)) return;
    if (!this.outbox().some((i) => i.localId === localId)) return;
    this._removeFromOutbox(localId);
    this.dispatch({ type: 'remove_turn', id: localId });
    if (this.outbox().length === 0) {
      this.outboxStalled.set(false);
      this.error.set(null);
    }
  }

  /** Return a rewind-stale queued message to the composer for explicit review. */
  takeQueuedSendForReview(localId: string): string | null {
    const item = this.outboxItem(localId);
    if (!item?.requiresReview) return null;
    if (this.postingLocalIds.has(localId) || this.uploadingLocalIds.has(localId)) return null;
    this._removeFromOutbox(localId);
    this.dispatch({ type: 'remove_turn', id: localId });
    if (this.outbox().length === 0) this.outboxStalled.set(false);
    return item.displayContent;
  }

  /** Drop the whole outbox and remove its optimistic bubbles (thread gone). */
  private _drainOutboxWithRollback(): void {
    const items = this.outbox();
    this.outbox.set([]);
    for (const item of items) {
      this.dispatch({ type: 'remove_turn', id: item.localId });
    }
  }

  /**
   * Re-dispatch an optimistic user bubble for each queued outbox item — used
   * after a history reload (connect carry / horizon reload / create failure)
   * wholesale-replaces turns. `skipInFlight` omits the item whose POST is
   * currently in flight (its row may already be in the reloaded history).
   */
  private _redispatchOutboxBubbles(skipInFlight = false): void {
    const visibleIds = new Set(this.conversation().turns.map((turn) => turn.id));
    for (const item of this.outbox()) {
      // POST-only: a mid-upload item has never been POSTed, so it cannot
      // be in the reloaded history and skipping it would lose its bubble.
      if (skipInFlight && this.postingLocalIds.has(item.localId)) continue;
      if (visibleIds.has(item.localId)) continue;
      this.dispatch({
        type: 'user_message',
        id: item.localId,
        content: item.displayContent,
        attachments: item.attachments,
        timestamp: Date.now(),
      });
    }
  }

  /** POST the input to the orchestrator's REST endpoint. Returns `{ok,
   *  status}`: ok=true only when this exact input was accepted. No current
   *  409 response proves duplicate identity (the request has no idempotency
   *  key), so every conflict remains visibly queued. The flush uses `status`
   *  to distinguish a terminal 404/410
   *  (drain) from a retriable failure (keep queued). Sets the error banner on
   *  a hard failure. */
  private async _postInput(
    content: string,
    expectedConversationRevision: number,
  ): Promise<{ ok: boolean; status: number; stale?: boolean }> {
    const tid = this.threadId();
    if (!tid) return { ok: false, status: 0 };
    const sentGeneration = this.sessionRuntimeGeneration;
    const sentControlEpoch = this.controlWsOpeningGeneration;
    try {
      const accepted = await firstValueFrom(
        this.http.post<{
          accepted: boolean;
          turn_id: number;
          conversation_revision?: number;
          queue?: SessionQueueState | null;
        }>(
          `${environment.apiUrl}/persistent/threads/${tid}/input`,
          { content, expected_conversation_revision: expectedConversationRevision },
        ),
      );
      if (Number.isInteger(accepted?.conversation_revision)) {
        this.conversationRevision.update((current) =>
          Math.max(current, accepted.conversation_revision as number),
        );
      }
      // The accept carries the unit's durable state: a `parked` unit took the
      // input but nothing can claim it — render that, never "waiting".
      this._applyQueueState(tid, accepted?.queue ?? null);
      // Accepted — the reply must now stream over SSE. Arm the one-shot
      // kickstart so a dead receive path self-heals (covers the direct
      // send and the queued-flush path, both of which route through here).
      this._armSendKickstart(tid);
      return { ok: true, status: 200 };
    } catch (err: any) {
      const status = err?.status ?? 0;
      if (status === 409) {
        if (err?.error?.detail?.code === 'session_view_stale') {
          const currentRevision = err?.error?.detail?.conversation_revision;
          if (Number.isInteger(currentRevision)) {
            this.conversationRevision.update((current) => Math.max(current, currentRevision));
          }
          this.error.set(this.transloco.translate('chat.rewind.staleOutbox'));
          return { ok: false, status, stale: true };
        }
        // A sibling tab may have started a *different* turn. Treating that
        // turn_in_flight as our duplicate used to remove this bubble even
        // though its text never ran. Retirement conflicts additionally carry
        // authoritative lifecycle state: retire only control/moment work,
        // preserve SSE/review, and retain this unsent outbox item honestly.
        const invalidBindingGeneration = this._sessionBindingInvalidGeneration(err);
        const bindingRefusalApplies =
          invalidBindingGeneration !== null &&
          this._inputBindingRefusalApplies(
            tid,
            invalidBindingGeneration,
            sentGeneration,
            sentControlEpoch,
          );
        if (bindingRefusalApplies && invalidBindingGeneration !== null) {
          this._latchSessionBindingInvalid(tid, invalidBindingGeneration);
        } else if (this._isSessionEndingError(err)) {
          this._retireFromSessionRefusal(tid, err);
        } else if (this._isSessionEndedError(err)) {
          this._retireFromSessionRefusal(tid, err);
        }
        if (
          !bindingRefusalApplies &&
          this.threadId() === tid &&
          this.terminalControlThreadId !== tid
        ) {
          const detail = err?.error?.detail;
          this.error.set(
            this.sanitizeError(
              (typeof detail === 'object' && detail !== null && detail.message) ||
                err?.message ||
                "Your message wasn't accepted and remains queued.",
            ),
          );
        }
        return { ok: false, status };
      }
      if (status === 0) {
        // Angular's fetch backend reports a rejected fetch() (network
        // drop, dead connection, blocked preflight) as status 0 with an
        // undefined statusText, whose .message is the internal
        // "Http failure response for <url>: 0 undefined". Never show
        // that: it isn't an HTTP status and it tells the user nothing.
        console.warn('[persistent-chat] input POST transport failure:', err?.message);
        this.error.set(
          "Couldn't reach the server — your message wasn't sent. " +
            "It stays queued below; retry when you're back online.",
        );
        return { ok: false, status };
      }
      this.error.set(this.sanitizeError(err?.error?.detail || err?.message));
      return { ok: false, status };
    }
  }

  /**
   * Arm the one-shot send-liveness kickstart. The input is accepted
   * server-side; if no SSE *data* frame arrives within
   * SEND_KICKSTART_TIMEOUT_MS the receive path is presumed dead and we force
   * a single reopen (replay-from-cursor delivers the turn). Deadline-checked
   * against `sseDataLastAt` (survives hidden-tab timer clamping) and never
   * re-POSTs — there's no idempotency key, so a replayed POST could double-run
   * the turn.
   */
  private _armSendKickstart(threadId: string): void {
    this._clearSendKickstart();
    const armedAt = Date.now();
    this.sendKickstartTimer = setTimeout(() => {
      this.sendKickstartTimer = null;
      if (this.intentionalClose || this.threadId() !== threadId) return;
      // A data frame landed after we armed → the pipeline is alive.
      if (this.sseDataLastAt >= armedAt) return;
      console.warn(
        '[persistent-chat] no SSE data within ' +
          `${SEND_KICKSTART_TIMEOUT_MS}ms of send — forcing reconnect`,
      );
      this.reconnectNow();
    }, SEND_KICKSTART_TIMEOUT_MS);
  }

  /** Cancel the send-liveness kickstart timer if armed. */
  private _clearSendKickstart(): void {
    if (this.sendKickstartTimer) {
      clearTimeout(this.sendKickstartTimer);
      this.sendKickstartTimer = null;
    }
  }

  /** Parse and dispatch slash commands. Returns true if handled. */
  private handleSlashCommand(input: string): boolean {
    const parts = input.split(/\s+/);
    const cmd = parts[0].toLowerCase();
    const arg = parts.slice(1).join(' ');

    switch (cmd) {
      case '/compact':
        // No local "Compacting context..." echo: the agent's
        // compaction.started/progress frames drive the live progress
        // block, and a no-op answers with a summary-less
        // context.compacted (rendered as a system line below).
        this._sendControl({ method: 'compact', focus: arg });
        return true;
      case '/done':
        // _sendControl refuses (and says so) when this session has no
        // socket transport for the verb — don't announce an end that was
        // never dispatched.
        if (this._sendControl({ method: 'archive' })) {
          this._systemMessage('Ending session...');
        }
        return true;
      case '/auto':
        this.setMode('auto_accept');
        return true;
      case '/supervised':
        this.setMode('supervised');
        return true;
      case '/autonomous':
        this.setMode('autonomous');
        return true;
      case '/silent':
        this.setNarrationMode('silent');
        return true;
      case '/verbose':
        this.setNarrationMode('verbose');
        return true;
      case '/undo':
        if (this.controlSocket === 'none') {
          this._sendDurableControl({ method: 'workspace.undo' });
        } else {
          // Pinned sessions retain the legacy direct-WS verb. The
          // orchestrator deliberately refuses workspace.undo on the
          // pinned REST lane because there is no lease-owned queue
          // unit to fence the destructive operation against.
          this._sendControl({ method: 'undo' });
        }
        this._systemMessage('Undoing last file changes...');
        return true;
      case '/upgrade-workspace': {
        const tier = arg.trim().toLowerCase() === 'vm' ? 'vm' : 'sandbox';
        this.upgradeWorkspace(tier);
        return true;
      }
      default:
        return false;
    }
  }

  private _systemMessage(content: string): void {
    this.dispatch({
      type: 'system_message',
      id: makeLocalId('sys'),
      content,
      timestamp: Date.now(),
    });
  }

  /** Map the durable snapshot's aggregated token telemetry onto `UsageState`,
   *  stamping the thread it was fetched for. Null in → null out, which is how
   *  a thread with no usage history clears the panel.
   *
   *  The server already summed output/reasoning across the turn's calls, so
   *  these land as totals rather than being accumulated again; the replay that
   *  follows skips the frames this covers (see the `usage.updated` handler). */
  private _usageFromSnapshot(raw: SessionStateUsage | null | undefined): UsageState | null {
    if (!raw) return null;
    return {
      threadId: this.threadId(),
      turn: raw.turn ?? null,
      inputTokens: raw.input_tokens ?? null,
      outputTokensTurn: raw.output_tokens ?? 0,
      reasoningTokensTurn: raw.reasoning_tokens ?? 0,
      reasoningEstimated: !!raw.reasoning_estimated,
      ctxLimitTokens: raw.ctx_limit_tokens ?? null,
      compactionThresholdTokens: raw.compaction_threshold_tokens ?? null,
    };
  }

  /** Map raw wire entries (snake_case `approval_id`) to `PermissionRequest`s
   *  (camelCase `approvalId`). Shared by `session.state`, `permission.request`,
   *  and `permission.request_batch` — an unmapped `approvalId` silently
   *  degrades every decision to "most-recent-pending" REST resolution. */
  private _toPermissionRequests(raw: unknown): PermissionRequest[] {
    const list = Array.isArray(raw) ? raw : [];
    const newestByToolCall = new Map<string, PermissionRequest>();
    for (const entry of list) {
      const r = entry as Record<string, unknown>;
      if (typeof r?.['id'] !== 'string' || !r['id']) continue;
      const id = r['id'] as string;
      const approvalId = r['approval_id'] as string | undefined;
      // Snapshot queries are oldest-first. A failed claim lookup can
      // leave two pending rows for one tool call; the waiter owns the
      // newer approval_id, so last-wins prevents a stale, unanswerable
      // duplicate card. Delete first so ordering also follows the row
      // that won.
      newestByToolCall.delete(id);
      newestByToolCall.set(id, {
        id,
        ...(approvalId ? { approvalId } : {}),
        tool: (r['tool'] as string) || '',
        args: (r['args'] as Record<string, unknown>) || {},
      });
    }
    return [...newestByToolCall.values()];
  }

  /** Approve every pending gate. Each decision carries its own approval_id:
   *  the no-id REST fallback resolves "most-recent-pending", which is the
   *  wrong gate when a batch is open. */
  approveAll(): void {
    const pending = this.pendingPermissions();
    for (const req of pending) {
      if (req.approvalId) {
        // Durable REST decisions are ack-driven: keep the card until
        // permission.resolved proves which decision won (another tab
        // may race us, or a committed response may be masked).
        this._resolvePermission(req, 'approve');
        continue;
      }
      this.pendingPermissions.update((list) => list.filter((item) => item.id !== req.id));
      this.dispatch({
        type: 'permission_decision',
        toolUseId: req.id,
        decision: 'approved',
        timestamp: Date.now(),
      });
      this._resolvePermission(req, 'approve');
    }
  }

  /** Deny every pending gate. */
  denyAll(): void {
    const pending = this.pendingPermissions();
    for (const req of pending) {
      if (req.approvalId) {
        this._resolvePermission(req, 'deny');
        continue;
      }
      this.pendingPermissions.update((list) => list.filter((item) => item.id !== req.id));
      this.dispatch({
        type: 'permission_decision',
        toolUseId: req.id,
        decision: 'denied',
        timestamp: Date.now(),
      });
      this._resolvePermission(req, 'deny');
    }
  }

  private _resolvePermission(
    pending: PermissionRequest | null,
    decision: 'approve' | 'deny',
  ): void {
    const threadId = this.threadId();
    if (threadId && pending?.approvalId) {
      this._clearPermissionResolutionFailure(pending.id);
      const url =
        `${environment.apiUrl}/persistent/threads/${threadId}` + `/approve/${pending.approvalId}`;
      this.http.post(url, { decision }).subscribe({
        error: (err: unknown) => {
          const status = (err as { status?: number })?.status;
          if (status === 404 || status === 409) {
            // Gone/already decided — a stale card from replay or a
            // double-click. No live waiter can be helped by putting
            // it back, and the permission.resolved event normally
            // supplies the matching transcript outcome.
            this._systemMessage(
              status === 409
                ? 'This permission request was already decided.'
                : 'This permission request is no longer available.',
            );
            this.pendingPermissions.update((list) =>
              list.filter(
                (item) => item.approvalId !== pending.approvalId && item.id !== pending.id,
              ),
            );
            this._clearPermissionResolutionFailure(pending.id);
            return;
          }
          if (this.threadId() !== threadId) return;
          // The REST outcome is unknown or failed. A durable-id
          // request must never fall back to a socket: socketless
          // sessions would queue it forever, while a masked REST
          // commit is safely idempotent on the next click (409).
          const stillPending = this.pendingPermissions().some(
            (item) => item.approvalId === pending.approvalId || item.id === pending.id,
          );
          // A permission.resolved frame may have beaten this error
          // callback after a masked commit. Never resurrect it or
          // replace the successful outcome with a retry banner.
          if (!stillPending) return;
          this.permissionResolutionFailures.add(pending.id);
          this.error.set(
            `Couldn't ${decision === 'approve' ? 'approve' : 'deny'} ` +
              'that request. It is still pending; try again.',
          );
        },
      });
      return;
    }
    this._sendControl({ method: decision });
  }

  private _clearPermissionResolutionFailure(toolCallId: string): void {
    this.permissionResolutionFailures.delete(toolCallId);
    if (
      this.permissionResolutionFailures.size === 0 &&
      this.error()?.endsWith('It is still pending; try again.')
    ) {
      this.error.set(null);
    }
  }

  /** Interrupt the current turn — REST POST. */
  async interrupt(): Promise<void> {
    if (this.isInterrupting()) return;
    const tid = this.threadId();
    const targetTurnId = this.currentTurnId();
    if (!tid || targetTurnId === null || targetTurnId < 1) return;

    const pending: PendingInterruptRequest = {
      threadId: tid,
      clientRequestId: crypto.randomUUID(),
      targetTurnId,
      attempts: 0,
    };
    // Install correlation before starting HTTP. The exact lease owner can
    // commit interrupt.ack over SSE before the admission response reaches
    // this tab, especially when this POST is an idempotent retry.
    this.pendingInterruptRequest = pending;
    this.isInterrupting.set(true);
    this._postPendingInterrupt(pending);
  }

  private _postPendingInterrupt(pending: PendingInterruptRequest): void {
    if (
      this.pendingInterruptRequest !== pending ||
      this.intentionalClose ||
      this.threadId() !== pending.threadId ||
      this.currentTurnId() !== pending.targetTurnId
    ) {
      this._clearPendingInterruptRequest(pending);
      return;
    }

    this.interruptAdmissionInFlight = pending;
    pending.attempts += 1;
    let settledSynchronously = false;
    let response$: Observable<unknown>;
    try {
      response$ = this.http
        .post(`${environment.apiUrl}/persistent/threads/${pending.threadId}/interrupt`, {
          client_request_id: pending.clientRequestId,
          target_turn_id: pending.targetTurnId,
        })
        .pipe(timeout({ first: INTERRUPT_RESPONSE_TIMEOUT_MS }));
    } catch (err) {
      settledSynchronously = true;
      this._handleInterruptAdmissionFailure(pending, err);
      return;
    }

    const subscription = response$.subscribe({
      next: () => {
        settledSynchronously = true;
        this._finishInterruptAdmission(pending);
      },
      error: (err: unknown) => {
        settledSynchronously = true;
        this._handleInterruptAdmissionFailure(pending, err);
      },
      complete: () => {
        settledSynchronously = true;
        this._finishInterruptAdmission(pending);
      },
    });
    if (!settledSynchronously && this.interruptAdmissionInFlight === pending) {
      this.interruptAdmissionSubscription = subscription;
    } else {
      subscription.unsubscribe();
    }
  }

  private _finishInterruptAdmission(pending: PendingInterruptRequest): void {
    if (this.interruptAdmissionInFlight !== pending) return;
    this.interruptAdmissionSubscription?.unsubscribe();
    this.interruptAdmissionSubscription = null;
    this.interruptAdmissionInFlight = null;
    if (this.pendingInterruptRequest !== pending) return;
    // HTTP success is admission only. The owner-written interrupt.ack or
    // exact turn.completed remains the client-visible authority.
    this._armInterruptFallback();
  }

  private _handleInterruptAdmissionFailure(pending: PendingInterruptRequest, err: unknown): void {
    if (this.interruptAdmissionInFlight !== pending) return;
    this.interruptAdmissionSubscription?.unsubscribe();
    this.interruptAdmissionSubscription = null;
    this.interruptAdmissionInFlight = null;

    if (
      this._isRetryableInterruptFailure(err) &&
      this.pendingInterruptRequest === pending &&
      !this.intentionalClose &&
      this.threadId() === pending.threadId &&
      this.currentTurnId() === pending.targetTurnId
    ) {
      const delay =
        INTERRUPT_RETRY_DELAYS_MS[
          Math.min(pending.attempts - 1, INTERRUPT_RETRY_DELAYS_MS.length - 1)
        ];
      console.warn(
        '[persistent-chat] interrupt admission outcome unknown; ' +
          'retrying the same request id and target turn',
      );
      this.interruptRetryTimer = setTimeout(() => {
        this.interruptRetryTimer = null;
        this._postPendingInterrupt(pending);
      }, delay);
      return;
    }

    console.warn('[persistent-chat] interrupt failed:', err);
    this._clearPendingInterruptRequest(pending);
  }

  private _isRetryableInterruptFailure(err: unknown): boolean {
    const status = (err as { status?: unknown })?.status;
    if (typeof status !== 'number') return true;
    return status === 0 || status === 408 || status === 425 || status === 429 || status >= 500;
  }

  private _clearPendingInterruptRequest(expected?: PendingInterruptRequest): void {
    if (expected && this.pendingInterruptRequest !== expected) return;
    if (this.interruptRetryTimer) {
      clearTimeout(this.interruptRetryTimer);
      this.interruptRetryTimer = null;
    }
    this.interruptAdmissionSubscription?.unsubscribe();
    this.interruptAdmissionSubscription = null;
    this.interruptAdmissionInFlight = null;
    this.pendingInterruptRequest = null;
    this.isInterrupting.set(false);
    this._clearInterruptFallback();
  }

  /** Arm the one-shot stuck-"Stopping…" fallback (see interrupt()). */
  private _armInterruptFallback(): void {
    this._clearInterruptFallback();
    this.interruptFallbackTimer = setTimeout(() => {
      this.interruptFallbackTimer = null;
      if (this.isInterrupting()) {
        console.warn(
          '[persistent-chat] interrupt ack not seen within ' +
            `${INTERRUPT_ACK_TIMEOUT_MS}ms — forcing reconnect to re-sync`,
        );
        this.reconnectNow();
      }
    }, INTERRUPT_ACK_TIMEOUT_MS);
  }

  /** Cancel the stuck-"Stopping…" fallback timer if armed. */
  private _clearInterruptFallback(): void {
    if (this.interruptFallbackTimer) {
      clearTimeout(this.interruptFallbackTimer);
      this.interruptFallbackTimer = null;
    }
  }

  /** Stop every pending permission prompt + halt the turn so the user can
   *  type a follow-up. Denies each call so the backend isn't stranded
   *  awaiting a decision (the loop would otherwise block on the
   *  `permission_check` await forever), then sends interrupt so the
   *  next loop iteration bails out instead of acting on the denial. */
  stop(): void {
    this.denyAll();
    void this.interrupt();
  }

  /** Change permission mode. */
  setMode(mode: PermissionMode): void {
    this._sendDurableControl({ method: 'mode.set', mode });
  }

  setNarrationMode(mode: NarrationMode): void {
    this._sendDurableControl({ method: 'narration.set', mode });
  }

  /** Update session config (model, temperature, tools, etc.) at runtime.
   *
   * `datasourceIds` (Slice B) rides the same request as a sibling key: the
   * desired FULL datasource selection (undefined = no change, [] = detach
   * all) — authorized and persisted server-side, re-wired at the next turn
   * boundary.
   *
   * Dispatches by the session's declared transport for `config.update`:
   *
   * - `websocket` (pinned sessions): the frame goes to the agent, which
   *   persists through the orchestrator and rebuilds itself live; the
   *   returned promise settles on the echoed `config.changed` ack, on a
   *   matching error frame, or on the ack timeout.
   * - `rest` (queue-served sessions): the owner PATCH persists the fragment
   *   at admission and the next claim's attach picks it up; the promise
   *   settles on the HTTP response, and the transcript stamp the socket ack
   *   would have journaled is written locally.
   * - `unavailable`: settles `ok: false` immediately and surfaces the error.
   *
   * Always resolves (never rejects) so the caller can settle its own
   * optimistic state either way — Replicache's speculative-then-authoritative
   * shape: apply locally, confirm or revert on the authoritative answer. */
  updateConfig(
    config: Record<string, unknown>,
    datasourceIds?: string[],
  ): Promise<ConfigUpdateOutcome> {
    const requestId = crypto.randomUUID();
    const threadId = this.threadId();
    const transport = this.controlTransport('config.update');
    if (!threadId || transport === 'unavailable') {
      const message = this.transloco.translate('chat.control.unavailable');
      if (threadId) this.error.set(message);
      return Promise.resolve({ ok: false, requestId, transport, message });
    }
    if (transport === 'rest') {
      return this._updateConfigOverRest(threadId, requestId, config, datasourceIds);
    }
    const frame: Record<string, unknown> = {
      method: 'config.update',
      config,
      request_id: requestId,
    };
    if (datasourceIds !== undefined) {
      frame['datasource_ids'] = datasourceIds;
    }
    return new Promise<ConfigUpdateOutcome>((resolve) => {
      const timer = setTimeout(() => {
        const message = this.transloco.translate('chat.control.applyTimeout');
        if (this._settlePendingConfigUpdate(requestId, { ok: false, requestId, transport, message })) {
          if (this.threadId() === threadId) this.error.set(message);
        }
      }, CONFIG_UPDATE_ACK_TIMEOUT_MS);
      this.pendingConfigUpdates.set(requestId, { threadId, resolve, timer });
      if (!this._sendControl(frame)) {
        this._settlePendingConfigUpdate(requestId, {
          ok: false,
          requestId,
          transport,
          message: this.transloco.translate('chat.control.unavailable'),
        });
      }
    });
  }

  /** The socketless half of `updateConfig`: `PATCH /api/persistent/threads/
   *  {id}/config` (the Slice C owner endpoint, whose connected-gate passes by
   *  construction on a session that binds no agent). */
  private async _updateConfigOverRest(
    threadId: string,
    requestId: string,
    config: Record<string, unknown>,
    datasourceIds?: string[],
  ): Promise<ConfigUpdateOutcome> {
    const body: Record<string, unknown> = { config_override: config };
    if (datasourceIds !== undefined) body['datasource_ids'] = datasourceIds;
    try {
      const response = await firstValueFrom(
        this.http.patch<{
          status: string;
          config_override?: Record<string, unknown>;
          datasource_ids?: string[] | null;
          effective?: 'next_turn' | 'next_attach';
        }>(`${environment.apiUrl}/persistent/threads/${threadId}/config`, body),
      );
      if (this.threadId() !== threadId) {
        // Persisted on the old thread; nothing to paint here.
        return { ok: true, requestId, transport: 'rest', applied: {}, effective: 'next_turn' };
      }
      const applied = (response?.config_override ?? config) as Record<string, unknown>;
      // Mirror what the socket ack would have done to the live chips.
      const llm = applied['llm'] as Record<string, unknown> | undefined;
      if (typeof llm?.['model'] === 'string' && llm['model']) {
        this.modelName.set(llm['model'] as string);
      }
      if (typeof llm?.['temperature'] === 'number') {
        this.temperature.set(llm['temperature'] as number);
      }
      const stamp = describeAppliedConfig(applied);
      if (datasourceIds !== undefined) stamp.push('connectors updated');
      if (stamp.length) {
        this._systemMessage(
          this.transloco.translate('chat.control.appliedNextTurn', {
            summary: stamp.join(' · '),
          }),
        );
      }
      return {
        ok: true,
        requestId,
        transport: 'rest',
        applied,
        effective: response?.effective ?? 'next_turn',
      };
    } catch (err: any) {
      const detail = err?.error?.detail;
      const detailText =
        typeof detail === 'string'
          ? detail
          : typeof detail?.message === 'string'
            ? detail.message
            : '';
      const headline = this.transloco.translate('chat.control.applyFailed');
      const message = this.sanitizeError(detailText ? `${headline}: ${detailText}` : headline);
      if (this.threadId() === threadId) this.error.set(message);
      return { ok: false, requestId, transport: 'rest', message };
    }
  }

  /** Resolve the exact boundary the REST rewind confirmation will submit. */
  async prepareRewind(messageId: string): Promise<StatelessRewindPreview | null> {
    this.rewindPreview.set(null);
    if (this.controlTransport('rewind') !== 'rest') return null;
    const threadId = this.threadId();
    if (!threadId || !this.rewindModeAvailable('conversation')) {
      this.error.set(this.transloco.translate('chat.rewind.unavailable'));
      return null;
    }
    this.rewindPreviewLoading.set(true);
    try {
      const preview = await firstValueFrom(
        this.http.get<StatelessRewindPreview>(
          `${environment.apiUrl}/persistent/threads/${threadId}/rewinds/preview` +
            `?message_id=${encodeURIComponent(messageId)}`,
        ),
      );
      if (this.threadId() !== threadId || preview.message_id !== messageId) return null;
      this.rewindPreview.set(preview);
      if (!preview.eligible) this._setRewindRefusal(preview.refusal_code);
      return preview;
    } catch (err: any) {
      if (this.threadId() === threadId) this._setRewindRequestError(err);
      return null;
    } finally {
      if (this.threadId() === threadId) this.rewindPreviewLoading.set(false);
    }
  }

  /** Rewind to an earlier user message using the declared transport. */
  rewind(
    messageId: string,
    mode: 'both' | 'conversation' | 'code',
    draftSnapshot = '',
    draftRevision = 0,
  ): string {
    const requestId = crypto.randomUUID();
    const transport = this.controlTransport('rewind');
    if (!this.rewindModeAvailable(mode)) {
      this.error.set(this.transloco.translate('chat.rewind.unavailable'));
      return requestId;
    }
    if (transport === 'rest') {
      const preview = this.rewindPreview();
      if (
        mode !== 'conversation' ||
        !preview ||
        preview.message_id !== messageId ||
        !preview.eligible ||
        !preview.expected
      ) {
        this.error.set(this.transloco.translate('chat.rewind.previewRequired'));
        return requestId;
      }
      const threadId = this.threadId();
      if (!threadId) return requestId;
      const marker: RewindOperationMarker = {
        threadId,
        clientRequestId: requestId,
        body: {
          client_request_id: requestId,
          message_id: messageId,
          mode: 'conversation',
          expected: preview.expected,
        },
        draftSnapshot,
        draftRevision,
        prefillConsumed: false,
      };
      this._writeRewindMarker(marker);
      this.rewindInFlight.set(true);
      this.pendingRewindRequestId = requestId;
      void this._submitRestRewind(marker);
      return requestId;
    }

    const sent = this._sendImmediateControl({
      method: 'rewind',
      message_id: messageId,
      mode,
      request_id: requestId,
    });
    if (!sent) {
      this.rewindInFlight.set(false);
      return requestId;
    }
    this.pendingRewindDraftSnapshot = draftSnapshot;
    this.pendingRewindDraftRevision = draftRevision;
    this.rewindInFlight.set(true);
    this.pendingRewindRequestId = requestId;
    this._armRewindAckFallback();
    return requestId;
  }

  /** Explicitly retry the exact stored body after an unknown REST outcome. */
  retryPendingRewind(): void {
    const threadId = this.threadId();
    const marker = threadId ? this._readRewindMarker(threadId) : null;
    if (!marker || this.rewindInFlight()) return;
    this.rewindOutcomeUnknown.set(false);
    this.rewindInFlight.set(true);
    this.pendingRewindRequestId = marker.clientRequestId;
    void this._submitRestRewind(marker);
  }

  refreshPendingRewindReceipt(): void {
    const threadId = this.threadId();
    const marker = threadId ? this._readRewindMarker(threadId) : null;
    if (!marker || this.rewindInFlight()) return;
    this.rewindInFlight.set(true);
    void this._lookupRewindReceipt(marker, true).then((found) => {
      if (this.threadId() !== marker.threadId) return;
      this.rewindInFlight.set(false);
      this.rewindOutcomeUnknown.set(!found);
      if (!found) this.error.set(this.transloco.translate('chat.rewind.outcomeUnknown'));
    });
  }

  /** Mark a returned prompt consumed after the component inserts it. */
  consumeRewindPrefill(clientRequestId: string): void {
    const threadId = this.threadId();
    const marker = threadId ? this._readRewindMarker(threadId) : null;
    if (marker?.clientRequestId === clientRequestId) {
      marker.prefillConsumed = true;
      this._writeRewindMarker(marker);
    }
    if (this.rewindPrefill()?.clientRequestId === clientRequestId) {
      this.rewindPrefill.set(null);
    }
  }

  private async _submitRestRewind(marker: RewindOperationMarker): Promise<void> {
    try {
      const result = await firstValueFrom(
        this.http.post<StatelessRewindResult>(
          `${environment.apiUrl}/persistent/threads/${marker.threadId}/rewinds`,
          marker.body,
        ).pipe(timeout(15_000)),
      );
      await this._applyRestRewindResult(marker, result);
    } catch (err: any) {
      if (this.threadId() !== marker.threadId) return;
      const status = Number(err?.status ?? 0);
      if (status === 0 || status === 408 || status >= 500) {
        const recovered = await this._lookupRewindReceipt(marker, false);
        if (!recovered) {
          this.rewindOutcomeUnknown.set(true);
          this.error.set(this.transloco.translate('chat.rewind.outcomeUnknown'));
        }
      } else {
        this._setRewindRequestError(err);
        this._removeRewindMarker(marker);
      }
    } finally {
      if (this.pendingRewindRequestId === marker.clientRequestId) {
        this.pendingRewindRequestId = null;
        this.rewindInFlight.set(false);
      }
    }
  }

  private async _lookupRewindReceipt(
    marker: RewindOperationMarker,
    quiet404: boolean,
  ): Promise<boolean> {
    try {
      const result = await firstValueFrom(
        this.http.get<StatelessRewindResult>(
          `${environment.apiUrl}/persistent/threads/${marker.threadId}/rewinds/` +
            `by-client-request/${marker.clientRequestId}`,
        ),
      );
      await this._applyRestRewindResult(marker, result);
      return true;
    } catch (err: any) {
      if (!quiet404 || err?.status !== 404) this._setRewindRequestError(err);
      return false;
    }
  }

  private async _applyRestRewindResult(
    marker: RewindOperationMarker,
    result: StatelessRewindResult,
  ): Promise<void> {
    if (
      this.threadId() !== marker.threadId ||
      result.client_request_id !== marker.clientRequestId
    )
      return;
    this.conversationRevision.update((current) =>
      Math.max(current, result.conversation_revision),
    );
    this.rewindOutcomeUnknown.set(false);
    this.outbox.update((queue) =>
      queue.map((item) =>
        (item.expectedConversationRevision ?? 0) < result.conversation_revision
          ? { ...item, requiresReview: true }
          : item,
      ),
    );
    if (this.outbox().some((item) => item.requiresReview)) {
      this.outboxStalled.set(true);
    }
    this.rewindPreview.set(null);
    await this._reloadAfterRewind();
    const currentMarker = this._readRewindMarker(marker.threadId);
    if (
      !currentMarker ||
      currentMarker.clientRequestId !== marker.clientRequestId ||
      currentMarker.prefillConsumed ||
      result.conversation_revision !== this.conversationRevision()
    )
      return;
    this.rewindPrefill.set({
      prompt: result.prompt,
      draftSnapshot: currentMarker.draftSnapshot,
      draftRevision: currentMarker.draftRevision,
      clientRequestId: currentMarker.clientRequestId,
    });
  }

  private _rewindMarkerKey(threadId: string): string {
    return `srw.rewind.operation.${threadId}`;
  }

  private _writeRewindMarker(marker: RewindOperationMarker): void {
    try {
      sessionStorage.setItem(this._rewindMarkerKey(marker.threadId), JSON.stringify(marker));
    } catch {
      // Private-mode storage failures do not invalidate the server operation.
    }
  }

  private _readRewindMarker(threadId: string): RewindOperationMarker | null {
    try {
      const raw = sessionStorage.getItem(this._rewindMarkerKey(threadId));
      if (!raw) return null;
      const marker = JSON.parse(raw) as RewindOperationMarker;
      return marker.threadId === threadId && marker.clientRequestId ? marker : null;
    } catch {
      return null;
    }
  }

  private _removeRewindMarker(marker: RewindOperationMarker): void {
    try {
      const current = this._readRewindMarker(marker.threadId);
      if (current?.clientRequestId === marker.clientRequestId) {
        sessionStorage.removeItem(this._rewindMarkerKey(marker.threadId));
      }
    } catch {
      // Best effort only.
    }
  }

  private _setRewindRefusal(reason: string | null): void {
    const known = new Set([
      'feature_disabled',
      'queue_not_idle',
      'queue_leased',
      'input_pending',
      'control_pending',
      'pending_input_delivery',
      'pending_scheduled_wake',
      'pending_child',
      'pending_job_wake',
      'active_job',
      'pending_final_memory',
      'target_invalid',
    ]);
    const key = reason && known.has(reason) ? reason : 'unavailable';
    this.error.set(this.transloco.translate(`chat.rewind.refusal.${key}`));
  }

  private _setRewindRequestError(err: any): void {
    const detail = err?.error?.detail;
    const reason = typeof detail === 'object' && detail ? detail.reason : null;
    if (reason === 'preview_changed') {
      this.error.set(this.transloco.translate('chat.rewind.stalePreview'));
    } else if (reason) {
      this._setRewindRefusal(String(reason));
    } else {
      this.error.set(this.transloco.translate('chat.rewind.unavailable'));
    }
  }

  /** "Summarize up to here" — manual compaction bounded at a message. */
  summarizeUpTo(messageId: string): void {
    this._sendControl({
      method: 'compact',
      focus: '',
      boundary_message_id: messageId,
    });
  }

  /** Arm the one-shot stuck-"Rewinding…" fallback (see rewind() and
   *  REWIND_ACK_TIMEOUT_MS). Mirrors _armInterruptFallback. */
  private _armRewindAckFallback(): void {
    this._clearRewindAckFallback();
    this.rewindAckFallbackTimer = setTimeout(() => {
      this.rewindAckFallbackTimer = null;
      if (this.rewindInFlight()) {
        console.warn(
          '[persistent-chat] rewind.ack not seen within ' +
            `${REWIND_ACK_TIMEOUT_MS}ms — clearing rewindInFlight`,
        );
        this.rewindInFlight.set(false);
        this.pendingRewindRequestId = null;
      }
    }, REWIND_ACK_TIMEOUT_MS);
  }

  /** Cancel the stuck-"Rewinding…" fallback timer if armed. Mirrors
   *  _clearInterruptFallback. */
  private _clearRewindAckFallback(): void {
    if (this.rewindAckFallbackTimer) {
      clearTimeout(this.rewindAckFallbackTimer);
      this.rewindAckFallbackTimer = null;
    }
  }

  /** Upgrade a lite (virtual) session to a real workspace tier.
   *
   * Provisions the workspace, seeds it from the live object-store prefix,
   * and hot-swaps in place so shell/git/file tools become available without
   * dropping the conversation (workspace_tier_upgrade.md §4.2 S3 /
   * Phase 2). `vm` is the explicit human-intent trigger for the privileged
   * tier — the server still gates it (can_use_vm + global kill-switch).
   * Upgrade-only; progress and completion arrive via the
   * `workspace_upgrade.*` frames (see `workspaceUpgradeInProgress`).
   *
   * `thenContinue` auto-sends a continuation once the upgrade lands, so the
   * agent picks the work back up with shell/git live. It's an option rather
   * than a caller-set flag because this method has three callers (the offer
   * card, the settings pane, `/upgrade-workspace`) and must *reset* the flag
   * for the two that don't want it — a set-then-call handler would be
   * silently order-dependent. Clearing the offer here rather than in the card
   * is deliberate for the same reason: accepting from the pane while the card
   * is live has to dismiss it too. */
  upgradeWorkspace(tier: 'sandbox' | 'vm', opts: { thenContinue?: boolean } = {}): void {
    if (this.controlTransport('upgrade-to-workspace') !== 'websocket') {
      // No transport on this session (queue-served sessions have none yet):
      // refuse before arming the in-progress state, and say so.
      this.error.set(this.transloco.translate('chat.control.unavailable'));
      return;
    }
    this.pendingWorkspaceOffer.set(null);
    this.continueAfterUpgrade.set(opts.thenContinue === true);
    this._sendControl({ method: 'upgrade-to-workspace', target_tier: tier });
    this.workspaceUpgradeInProgress.set({ tier });
    this._systemMessage(
      tier === 'vm'
        ? 'Provisioning a VM workspace (requires approval), please wait...'
        : 'Provisioning workspace, please wait...',
    );
  }

  /** Decline a live upgrade offer. Local only — the agent isn't told anything
   *  here. The card hands the user a prefilled composer instead, because a
   *  silent decline would leave the agent's context still claiming approval
   *  is on the way (its tool result said a human would decide), so it would
   *  stall or re-ask. The reason has to arrive as a real message. */
  dismissWorkspaceOffer(): void {
    this.pendingWorkspaceOffer.set(null);
  }

  /** Clear conversation history (local only). */
  clearMessages(): void {
    this.dispatch({ type: 'reset', threadId: this.threadId() });
  }

  // ── Event handling (shared by SSE and historical WS path) ───────────

  private _handleEvent(
    data: { method: string; params?: Record<string, unknown> },
    allowSessionReady = true,
    coveredBySnapshot = false,
  ): void {
    const params = data.params ?? {};
    const now = Date.now();

    // Forward the decoded envelope, never transport ownership or full
    // state. CanvasService treats these frames as invalidations and reloads
    // the authoritative REST representation.
    const threadId = this.threadId();
    if (threadId) this.threadTransport.forwardEvent(threadId, data);

    // Fold any buffered streamed deltas before handling a non-delta frame.
    // Several handlers below read conversation() and may skip dispatching
    // (_closeActiveTurnIfAny, the turn.completed handler); if buffered
    // tokens weren't applied first they'd materialize a placeholder turn
    // *after* the close ran, wedging isStreaming() true. token/thinking
    // frames enqueue further down and must NOT flush here.
    if (data.method !== 'token' && data.method !== 'thinking') {
      this._flushDeltas();
    }

    // Agent-liveness tracking (session_silent_failure_audit.md #8):
    // "Connected" only proves the orchestrator SSE is up. Every frame
    // reaching this dispatcher is agent-origin (orchestrator pings and
    // ws.ping never get here), so its age is a fair proxy for "is the
    // agent producing anything" — except the REST snapshot, which this
    // service injects as a frame itself: counting it made every reconnect
    // restart the silence clock while the agent had been quiet for minutes.
    if (data.method !== 'session.state') {
      this.agentLastEventAt = now;
    }

    switch (data.method) {
      case 'session.state': {
        const durableSnapshot = params['snapshot_source'] === 'durable_journal';
        if (Number.isInteger(params['conversation_revision'])) {
          this.conversationRevision.update((current) =>
            Math.max(current, params['conversation_revision'] as number),
          );
        }
        if (!durableSnapshot) {
          // A pinned agent's exact in-memory welcome frame heals a
          // failed durable read during coexistence. It also proves
          // a prior startup "Agent not ready" response is stale,
          // even when REST readiness already flipped the latch.
          const recoveredSnapshot = this.sessionSnapshotFailed;
          this.sessionSnapshotFailed = false;
          if (
            (recoveredSnapshot && this.error() === 'Session state unavailable') ||
            this.error() === 'Agent not ready'
          ) {
            this.error.set(null);
          }
        }
        if (params['permission_mode']) {
          this.permissionMode.set(params['permission_mode'] as PermissionMode);
        }
        if (params['narration_mode']) {
          this.narrationMode.set(params['narration_mode'] as NarrationMode);
        }
        if (params['turn_count'] != null) {
          this.turnCount.set(params['turn_count'] as number);
        }
        // Legacy pinned fallback: without a durable snapshot floor,
        // join REST's historical prefix to the suffix replayed from a
        // browser cursor. With a durable snapshot, turn.started is
        // deliberately replayed and rebuilds the prefix from scratch.
        if (
          this.sseOpenedWithCursor &&
          ((durableSnapshot && this.snapshotJoinsPreservedTurn) ||
            (!durableSnapshot && !this.sessionSnapshotLoaded)) &&
          params['turn_in_flight'] === true &&
          params['turn_count'] != null
        ) {
          const turnId = String(params['turn_count']);
          this.dispatch({ type: 'reattach_turn', turnId, timestamp: now });
        }
        // The durable snapshot is the authority on whether a turn is open.
        // A retained tab whose turn never got its terminal frame (the loop
        // died in settlement; the journal has turn.started with no
        // turn.completed/turn.error) would otherwise spin forever — the
        // handler above only ever *reopens*. Close it now: the replay floor
        // sits at the journal's tail, so nothing that follows reopens it,
        // and a genuinely new turn arrives as a fresh turn.started.
        if (
          durableSnapshot &&
          params['turn_in_flight'] === false &&
          this.conversation().activeAssistantTurnId != null
        ) {
          this._closeActiveTurnIfAny('turn_interrupted');
          this.runningTool.set(null);
          this.compaction.set(null);
        }
        if (params['model']) {
          this.modelName.set(params['model'] as string);
        }
        if (params['temperature'] != null) {
          this.temperature.set(params['temperature'] as number);
        }
        // Running-command snapshot: only act when the key is present so
        // a metadata-only session.state from another channel can't clobber it.
        if ('running_tool' in params) {
          const rt = params['running_tool'] as Partial<RunningToolInfo> | null;
          this.runningTool.set(
            rt && rt.tool ? { id: rt.id ?? '', tool: rt.tool, args: rt.args ?? {} } : null,
          );
        }
        // Pending supervised gates: re-render the approval card a
        // dropped stream (or a reload) would otherwise strand, leaving
        // the gate unanswerable. Same presence-check discipline as
        // running_tool. See the backend welcome frame.
        if ('pending_permissions' in params) {
          const list = this._toPermissionRequests(params['pending_permissions']);
          // Presence is authoritative, including an explicit empty
          // list. Without the empty write, a horizon refresh can
          // leave a resolved approval card stuck on screen forever.
          this.pendingPermissions.set(list);
          // A durable REST snapshot arrives before replayed
          // turn.started. Keep its list authoritative, but let the
          // journal reconstruct transcript placement in seq order;
          // dispatching here would create a stray recovered bubble.
          if (!durableSnapshot) {
            for (const req of list) {
              this.dispatch({
                type: 'permission_request',
                toolUseId: req.id,
                tool: req.tool,
                args: req.args ?? {},
                timestamp: now,
              });
            }
          }
        }
        // Tasks are durable state, not replay state: a task update may
        // predate replay_cursor. Presence is authoritative (including
        // []), while an older pinned/rolling-deploy welcome frame that
        // omits the key must not erase a newer REST/live task list.
        if ('tasks' in params) {
          const snapshotTasks = params['tasks'];
          this.tasks.set(Array.isArray(snapshotTasks) ? (snapshotTasks as SessionTask[]) : []);
        }
        // Token telemetry, same presence discipline as tasks: an explicit
        // null is authoritative and clears the panel (a never-answered thread
        // reports null, which is what stops a previous session's numbers from
        // being rendered here), while an older peer that omits the key leaves
        // whatever replay has already rebuilt alone. Only the durable REST
        // snapshot is trusted — the pinned WS welcome frame arrives after
        // replay has run, so seeding from it could clobber live state.
        if (durableSnapshot && 'usage' in params) {
          this.usage.set(this._usageFromSnapshot(params['usage'] as SessionStateUsage | null));
          this.snapshotSeededUsage = true;
        }
        if (allowSessionReady) this.markSessionReady();
        break;
      }

      case 'greeting': {
        // Synthetic single-turn assistant message — agent welcome line.
        const id = makeLocalId('greet');
        this.dispatch({ type: 'turn_started', turnId: id, startedAt: now });
        this.dispatch({
          type: 'token',
          content: (params['content'] as string) || '',
          timestamp: now,
        });
        this.dispatch({ type: 'turn_completed', turnId: id, finishedAt: now });
        this.isWaitingForInput.set(true);
        break;
      }

      case 'ready':
        this.isWaitingForInput.set(true);
        // If a turn is still open (race with turn.completed dropped), close it.
        this._closeActiveTurnIfAny('turn_completed');
        // A covered frame is still a transcript boundary; only its
        // readiness side effect is redundant with /connection.
        if (!coveredBySnapshot) this.markSessionReady();
        break;

      case 'turn.started': {
        // The awaited turn is live — isStreaming takes over from the
        // awaiting state. Clamped: a turn can start without a tracked
        // accept (other tab, injected input, reload mid-queue).
        this.pendingTurnCount.update((c) => Math.max(0, c - 1));
        // A live turn means the unit is leased: any parked/queued block is stale.
        this._queueState.set(null);
        const turnId = String(params['turn_id'] ?? makeLocalId('turn'));
        const pendingInterrupt = this.pendingInterruptRequest;
        const pendingWasActive =
          pendingInterrupt !== null &&
          this.conversation().activeAssistantTurnId === String(pendingInterrupt.targetTurnId);
        const reportedTurn = Number(params['turn_id']);
        // Replay may start behind the REST snapshot's event_cursor.
        // Numeric turn ids are authoritative, so max() advances a live
        // edge but cannot count an older replayed start twice.
        this.turnCount.update((c) =>
          Number.isFinite(reportedTurn) ? Math.max(c, reportedTurn) : c + 1,
        );
        this.dispatch({
          type: 'turn_started',
          turnId,
          startedAt: now,
          model: (params['model'] as string) || undefined,
        });
        // A new numeric turn superseding the exact locally-targeted
        // one is also a terminal boundary when its completion frame
        // was lost. Retire only that old request; replay of an older
        // turn.started must not cancel a newer pending interrupt.
        if (
          pendingInterrupt &&
          pendingWasActive &&
          turnId !== String(pendingInterrupt.targetTurnId)
        ) {
          this._clearPendingInterruptRequest(pendingInterrupt);
        }
        break;
      }

      case 'token':
        this.dispatch({
          type: 'token',
          content: (params['content'] as string) || '',
          timestamp: now,
        });
        break;

      case 'thinking':
        this.dispatch({
          type: 'thinking',
          content: (params['content'] as string) || '',
          messageId: (params['message_id'] as string) || undefined,
          timestamp: now,
        });
        break;

      // Drop the in-progress reasoning bubble — the agent's empty-response
      // retry replaces a dead-end reasoning stream with the retry's.
      case 'thinking.reset':
        this.dispatch({
          type: 'thinking_reset',
          messageId: (params['message_id'] as string) || undefined,
          timestamp: now,
        });
        break;

      case 'tool.started':
        this.dispatch({
          type: 'tool_started',
          toolUseId: (params['id'] as string) || '',
          tool: (params['tool'] as string) || '',
          args: (params['args'] as Record<string, unknown>) || {},
          category: (params['category'] as string) || undefined,
          timestamp: now,
        });
        break;

      case 'tool.completed':
        this.dispatch({
          type: 'tool_completed',
          toolUseId: (params['id'] as string) || '',
          result: (params['result'] as string) || '',
          isError: !!params['is_error'],
          timestamp: now,
        });
        if (this.runningTool()?.id === ((params['id'] as string) || '')) {
          this.runningTool.set(null);
        }
        break;

      case 'permission.request_batch': {
        const list = this._toPermissionRequests(params['requests']);
        if (list.length > 0) {
          if (!coveredBySnapshot) this.pendingPermissions.set(list);
          for (const req of list) {
            this.dispatch({
              type: 'permission_request',
              toolUseId: req.id,
              tool: req.tool,
              args: req.args ?? {},
              timestamp: now,
            });
          }
        }
        break;
      }

      case 'permission.request': {
        const id = (params['id'] as string) || '';
        const tool = (params['tool'] as string) || '';
        const args = (params['args'] as Record<string, unknown>) || {};
        const approvalId = (params['approval_id'] as string) || undefined;
        if (id) {
          const entry: PermissionRequest = {
            id,
            ...(approvalId ? { approvalId } : {}),
            tool,
            args,
          };
          // Converge on the authoritative approval_id rather than
          // dropping a frame for a call already listed. The gate's
          // claim SELECT soft-fails on any DB blip and then INSERTs
          // a SECOND pending row, broadcasting its NEW approval_id —
          // the only id the waiter filters NOTIFY on. Keeping the
          // stale announced id would resolve a row nobody waits on:
          // the card vanishes and the agent blocks forever.
          if (!coveredBySnapshot) {
            this.pendingPermissions.update((list) => {
              const idx = list.findIndex(
                (p) => p.id === id || (!!approvalId && p.approvalId === approvalId),
              );
              if (idx < 0) return [...list, entry];
              const next = [...list];
              next[idx] = { ...next[idx], ...entry };
              return next;
            });
          }
        }
        this.dispatch({
          type: 'permission_request',
          toolUseId: id,
          tool,
          args,
          timestamp: now,
        });
        break;
      }

      case 'permission.resolved': {
        // SSE replay re-delivers permission.request frames; without
        // this matching outcome event a reloading client resurrected
        // an already-decided approval card, whose re-click then
        // 409'd (session_silent_failure_audit.md #10).
        const resolvedId = (params['id'] as string) || '';
        this._clearPermissionResolutionFailure(resolvedId);
        if (!coveredBySnapshot) {
          this.pendingPermissions.update((list) => list.filter((p) => p.id !== resolvedId));
        }
        // Three-way, deliberately not a boolean: the backend
        // sweeps un-reached gates as 'expired' (and 'interrupted' on
        // Stop). Neither is a refusal — reporting them as 'denied'
        // recreates the fabricated-denial bug in the UI. See
        // knowledge-history/done/supervised_parallel_gates_timeout_fabricates_denial.md
        const rawDecision = params['decision'];
        const decision =
          rawDecision === 'approved'
            ? 'approved'
            : rawDecision === 'denied'
              ? 'denied'
              : ('expired' as const);
        if (resolvedId) {
          this.dispatch({
            type: 'permission_decision',
            toolUseId: resolvedId,
            decision,
            timestamp: now,
          });
        }
        break;
      }

      case 'citation.verdict': {
        // The aux verifier flipped a citation pending→verified/failed.
        // Patch it in place so the citations panel + inline [N] popover
        // update live, instead of waiting for the next per-turn
        // loadCitations(). No-op if the citation isn't loaded yet — the
        // turn-boundary refresh will pick up its already-final status.
        const cid = Number(params['citation_id']);
        const status =
          typeof params['verification_status'] === 'string'
            ? (params['verification_status'] as string)
            : '';
        if (Number.isFinite(cid) && status) {
          this.citationsByCid.update((m) => {
            const existing = m.get(cid);
            if (!existing) return m;
            const next = new Map(m);
            next.set(cid, { ...existing, verification_status: status });
            return next;
          });
        }
        break;
      }

      case 'turn.interrupted':
      case 'turn.parked': {
        const rawTarget = Number(params['target_turn_id'] ?? params['turn_id']);
        const hasExactTarget = Number.isSafeInteger(rawTarget) && rawTarget > 0;
        // Reaper frames without a target predate the correlated
        // interrupt inbox. They are still an ordered terminal edge in
        // the current journal epoch, so the active turn is the only
        // safe fallback. New frames carry target_turn_id and therefore
        // cannot close a newer turn after replay or cross-tab delay.
        const targetId = hasExactTarget
          ? String(rawTarget)
          : !coveredBySnapshot
            ? this.conversation().activeAssistantTurnId
            : null;
        if (!targetId) break;

        const closedActive = this._interruptTurnIfSafe(targetId, now, coveredBySnapshot);
        if (closedActive) {
          this.runningTool.set(null);
          this.pendingTurnCount.set(0);
          this.isWaitingForInput.set(false);
          this.compaction.set(null);
        }

        const pending = this.pendingInterruptRequest;
        if (pending && targetId === String(pending.targetTurnId)) {
          this._clearPendingInterruptRequest(pending);
        }

        const targetWasHandled = this.conversation().turns.some(
          (turn) => isAssistantTurn(turn) && turn.id === targetId && turn.status === 'interrupted',
        );
        if (data.method === 'turn.parked' && targetWasHandled) {
          this.dispatch({
            type: 'system_message',
            id: `turn-parked-${targetId}`,
            content: this.transloco.translate('chat.turn.parked'),
            timestamp: now,
          });
        }

        break;
      }

      case 'turn.completed': {
        const turnId = String(params['turn_id'] ?? this.conversation().activeAssistantTurnId ?? '');
        const wasActive = !!turnId && this.conversation().activeAssistantTurnId === turnId;
        if (turnId) {
          this.dispatch({ type: 'turn_completed', turnId, finishedAt: now });
        }
        if (!coveredBySnapshot) {
          const pending = this.pendingInterruptRequest;
          if (pending && turnId === String(pending.targetTurnId)) {
            this._clearPendingInterruptRequest(pending);
          }
          if (wasActive) this.runningTool.set(null);
        }
        // A compaction never outlives its turn — clear a stale block
        // (e.g. the pod died mid-fold and the turn was closed).
        this.compaction.set(null);
        // Protected cloud mode (Slice C, Task 14): the agent stages
        // its cloud-diff overlay at turn end, so this is the natural
        // edge to refresh the badge count. Debounced — a burst of
        // rapid turns (e.g. auto-continue) shouldn't fire one GET per
        // turn.
        if (this.protectedCloud()) {
          this._scheduleCloudDiffRefresh();
        }
        break;
      }

      case 'cloud.diff_staged':
        // Publication and this journal frame share the server's exact-runtime
        // CAS. Fetch the durable summary rather than trusting event counts.
        // This works after the finite terminal probe fallback has expired and
        // never reopens /connection, /prepare or the control WebSocket.
        if (this._cloudDiffStagedEventApplies(params)) {
          void this.refreshCloudDiffCount();
        }
        break;

      case 'turn.error': {
        if (coveredBySnapshot) {
          // The snapshot owns current scalars/banner state, but this
          // remains a replayable terminal boundary. Without the
          // close, a dropped turn.completed followed by a durable
          // error/ready frame reopens a forever-streaming bubble.
          this._closeActiveTurnIfAny('turn_interrupted');
          this.compaction.set(null);
          break;
        }
        // A failed turn used to leave the assistant bubble spinning
        // forever (no turn.completed on the error path) with only the
        // transient banner as a signal. Close the turn and append a
        // durable line — the matching role='error' history row keeps
        // it across reloads (session_silent_failure_audit.md #2).
        this._closeActiveTurnIfAny('turn_interrupted');
        this.isInterrupting.set(false);
        this.runningTool.set(null);
        this.compaction.set(null);
        // Conservative reset: a queued send may still run after the
        // failed turn, but its turn.started decrement is clamped —
        // better a missing awaiting state than one stuck forever.
        this.pendingTurnCount.set(0);
        this._systemMessage(`⚠ ${this.sanitizeError(params['message'] as string)}`);
        break;
      }

      case 'interrupt.ack': {
        const pending = this.pendingInterruptRequest;
        const rawTarget = Number(params['target_turn_id']);
        const hasExactTarget = Number.isSafeInteger(rawTarget) && rawTarget > 0;
        // Legacy direct-WS acknowledgements carried no correlation.
        // They are safe only while this tab still has an exact local
        // request for the turn that remains active. Otherwise an old
        // frame must not close a newer assistant turn.
        const targetTurnId = hasExactTarget
          ? rawTarget
          : pending && this.conversation().activeAssistantTurnId === String(pending.targetTurnId)
            ? pending.targetTurnId
            : null;
        if (targetTurnId === null) break;
        if (params['applied'] === false) {
          // The exact owner can durably reject a request that lost
          // the turn/gate race. This is an acknowledgement of the
          // request, not an interruption of the assistant turn. A
          // rejection from another tab for the same turn says
          // nothing about this tab's independently admitted UUID.
          if (
            pending &&
            targetTurnId === pending.targetTurnId &&
            params['client_request_id'] === pending.clientRequestId
          ) {
            this._clearPendingInterruptRequest(pending);
          }
          break;
        }

        const targetId = String(targetTurnId);
        const closedActive = this._interruptTurnIfSafe(targetId, now, coveredBySnapshot);
        if (closedActive) {
          this.runningTool.set(null);
          this.pendingTurnCount.set(0);
        }
        // Any exact acknowledgement for this same turn makes a local
        // duplicate request moot (another tab may have interrupted it
        // first). Correlation still prevents an old turn's ack from
        // clearing a request aimed at the new turn.
        if (pending && targetTurnId === pending.targetTurnId) {
          this._clearPendingInterruptRequest(pending);
        }
        break;
      }

      case 'mode.changed':
        this._clearDurableControlErrorAfter(this._takeDurableControlAck(params, 'mode.set'));
        if (coveredBySnapshot) break;
        this.permissionMode.set((params['mode'] as PermissionMode) || 'supervised');
        break;

      case 'narration.changed':
        this._clearDurableControlErrorAfter(this._takeDurableControlAck(params, 'narration.set'));
        if (coveredBySnapshot) break;
        this.narrationMode.set((params['mode'] as NarrationMode) || 'auto');
        break;

      case 'control.rejected': {
        const rejectedMethod =
          params['method'] === 'mode.set' ||
          params['method'] === 'narration.set' ||
          params['method'] === 'workspace.undo'
            ? params['method']
            : null;
        const rejectedMarker = rejectedMethod
          ? this._takeDurableControlAck(params, rejectedMethod)
          : null;
        if (coveredBySnapshot) break;
        this._setDurableControlError(
          rejectedMarker ?? {
            method: rejectedMethod,
            ordinal: ++this.durableControlOrdinal,
          },
          this.transloco.translate('chat.control.ownerRejected'),
        );
        break;
      }

      case 'config.changed': {
        if (!coveredBySnapshot && params['model']) {
          this.modelName.set(params['model'] as string);
        }
        if (!coveredBySnapshot && params['temperature'] != null) {
          this.temperature.set(params['temperature'] as number);
        }
        if (!coveredBySnapshot && params['permission_mode']) {
          this.permissionMode.set(params['permission_mode'] as PermissionMode);
        }
        // Transcript stamp (live_session_settings.md, principle 5):
        // the ack is broadcast + journaled, so every viewer — live or
        // replaying — sees which config produced the next answers.
        const applied = params['applied'] as Record<string, unknown> | undefined;
        const dsChange = params['datasources'] as AppliedDatasourceChange | undefined;
        const stamp = applied || dsChange ? describeAppliedConfig(applied ?? {}, dsChange) : [];
        if (stamp.length) {
          this._systemMessage(
            `Session settings updated: ${stamp.join(' · ')} — applies from the next response.`,
          );
        }
        // The ack the socket path awaits (P0.3 request_id echo) — the
        // caller's optimistic state is confirmed here, not at send time.
        const ackRequestId = params['request_id'] as string | undefined;
        if (ackRequestId) {
          this._settlePendingConfigUpdate(ackRequestId, {
            ok: true,
            requestId: ackRequestId,
            transport: 'websocket',
            applied: applied ?? {},
            effective: 'now',
          });
        }
        break;
      }

      case 'title.updated':
        if (params['title']) {
          this.sessionTitle.set(params['title'] as string);
        }
        break;

      case 'usage.updated': {
        // The session-state snapshot already aggregated every frame at or
        // below its cursor. Re-accumulating those on replay would double the
        // latest turn's output/reasoning, so let the snapshot own them —
        // but only when it actually seeded them (see snapshotSeededUsage).
        if (coveredBySnapshot && this.snapshotSeededUsage) break;
        const threadId = this.threadId();
        const raw = this.usage();
        // Every carry-over below — the per-turn accumulators AND the sticky
        // "omitted field keeps its last value" fallbacks — is gated on this.
        // Without it a frame that omits input_tokens or a limit would inherit
        // the *previous session's* figure through the `??` chain, which is the
        // leak wearing a different hat.
        const prev = raw !== null && raw.threadId === threadId ? raw : null;
        const turn = (params['turn'] as number) ?? null;
        // Turn numbers restart per session, so same-turn is only meaningful
        // once same-thread is established above.
        const sameTurn = prev !== null && prev.turn === turn;
        this.usage.set({
          threadId,
          turn,
          // Latest call's prompt size ≈ current context fill
          inputTokens: (params['input_tokens'] as number) ?? prev?.inputTokens ?? null,
          outputTokensTurn:
            (sameTurn ? prev.outputTokensTurn : 0) + ((params['output_tokens'] as number) ?? 0),
          reasoningTokensTurn:
            (sameTurn ? prev.reasoningTokensTurn : 0) +
            ((params['reasoning_tokens'] as number) ?? 0),
          // Sticky across the turn: once any call's reasoning is
          // estimated (provider reported no count), the turn reads est.
          reasoningEstimated:
            (sameTurn ? prev.reasoningEstimated : false) || !!params['reasoning_estimated'],
          ctxLimitTokens: (params['ctx_limit_tokens'] as number) ?? prev?.ctxLimitTokens ?? null,
          compactionThresholdTokens:
            (params['compaction_threshold_tokens'] as number) ??
            prev?.compactionThresholdTokens ??
            null,
        });
        break;
      }

      case 'compaction.started': {
        this.compaction.set({
          trigger: (params['trigger'] as string) ?? 'auto',
          totalTokens: (params['total_tokens'] as number) ?? null,
          ctxUsedTokens: (params['ctx_used_tokens'] as number) ?? null,
          ctxLimitTokens: (params['ctx_limit_tokens'] as number) ?? null,
          ctxUsedPct: (params['ctx_used_pct'] as number) ?? null,
          auxLimitTokens: (params['aux_limit_tokens'] as number) ?? null,
          nPasses: (params['n_passes'] as number) ?? 1,
          currentPass: 0,
          firstMsg: null,
          lastMsg: null,
          inTokens: null,
          outTokens: null,
          attempt: 1,
          stage: 'summarizing',
          startedAt: now,
        });
        break;
      }

      case 'compaction.progress': {
        // Replay tolerance: a reload mid-compaction can deliver
        // progress without its started frame — synthesize minimal
        // state instead of dropping the update.
        const prev = this.compaction();
        this.compaction.set({
          trigger: (params['trigger'] as string) ?? prev?.trigger ?? 'auto',
          totalTokens: prev?.totalTokens ?? null,
          ctxUsedTokens: prev?.ctxUsedTokens ?? null,
          ctxLimitTokens: prev?.ctxLimitTokens ?? null,
          ctxUsedPct: prev?.ctxUsedPct ?? null,
          auxLimitTokens: prev?.auxLimitTokens ?? null,
          nPasses: (params['n_passes'] as number) ?? prev?.nPasses ?? 1,
          currentPass: (params['pass'] as number) ?? prev?.currentPass ?? 1,
          firstMsg: (params['first_msg'] as number) ?? null,
          lastMsg: (params['last_msg'] as number) ?? null,
          inTokens: (params['in_tokens'] as number) ?? null,
          outTokens: (params['out_tokens'] as number) ?? null,
          attempt: (params['attempt'] as number) ?? 1,
          stage: (params['stage'] as string) ?? 'summarizing',
          startedAt: prev?.startedAt ?? now,
        });
        break;
      }

      case 'compaction.skipped': {
        // Engine ran but the result wasn't worth adopting (e.g. the
        // summary came out larger than the folded messages). Journaled
        // terminal frame — clears the progress block, including on
        // SSE replay. Manual /compact additionally gets its own
        // summary-less context.compacted for the system line.
        this.compaction.set(null);
        break;
      }

      case 'compaction.failed': {
        this.compaction.set(null);
        const reason = (params['reason'] as string) ?? 'unknown';
        // History is intact by contract (the engine aborts rather than
        // compacting behind a placeholder) — say so explicitly.
        this._systemMessage(
          `⚠ Context compaction failed (${reason}) — conversation kept intact, will retry later.`,
        );
        break;
      }

      case 'context.compacted': {
        // Compaction finished — clear the live progress block.
        this.compaction.set(null);
        const summary = (params['summary'] as string | null) ?? '';
        if (!summary) {
          // Manual /compact no-op: nothing was folded and no banner
          // row was persisted — a transient system line, not a banner.
          this._systemMessage('Nothing to compact — context is within limits.');
          break;
        }
        // Show a compaction banner. Stable id (compaction-<turn>) keeps
        // SSE replay idempotent; the reducer replaces rather than dupes.
        const turn = params['turn'];
        const compactionId = turn != null ? `compaction-${turn}` : makeLocalId('compaction');
        this.dispatch({
          type: 'add_compaction',
          id: compactionId,
          summary,
          timestamp: Date.now(),
        });
        break;
      }

      case 'session.ended':
        if (this._isSupersededLifecycleFrame() || !this._runtimeSessionFrameApplies(params)) {
          break;
        }
        if (this.threadId()) {
          this._retireTerminalControl(
            this.threadId()!,
            this._canonicalRuntimeGeneration(params['session_runtime_generation']),
          );
        }
        this._systemMessage('Session ended.');
        this.isWaitingForInput.set(false);
        this.pendingTurnCount.set(0);
        this.threadStatus.set('ended');
        this.endedAt.set(new Date().toISOString());
        break;

      case 'session.idle_timeout':
        if (this._isSupersededLifecycleFrame() || !this._runtimeSessionFrameApplies(params)) {
          break;
        }
        // This is an intent/diagnostic edge emitted before the pinned
        // retirement transaction settles. Close moment-scoped controls but
        // wait for the authoritative ended/suspended frame before exposing
        // Resume or an ended timestamp.
        if (this.threadId()) {
          this._retireEndingControl(
            this.threadId()!,
            null,
            this._canonicalRuntimeGeneration(params['session_runtime_generation']),
          );
        }
        this._systemMessage(
          `Session paused after ${(params['timeout_minutes'] as number) || 30} minutes of inactivity. Your work has been saved.`,
        );
        this.isWaitingForInput.set(false);
        this.pendingTurnCount.set(0);
        break;

      case 'session.event':
        // A system notice was injected into the running session
        // (currently: a worker job this session created finished —
        // knowledge-base/knowledge/features/session_wake_on_job_completion.md). Required,
        // not polish: /api/input broadcasts nothing on its own and no
        // frame carries user-message content, so without this the user
        // watches a turn start and stream a reply with no visible
        // prompt — the agent apparently talking to itself. Matches the
        // muted system line the same row gets on history reload
        // (role='event' in historyToTurns).
        this._systemMessage((params['content'] as string) || '');
        break;

      case 'session.suspended':
        if (this._isSupersededLifecycleFrame() || !this._runtimeSessionFrameApplies(params)) {
          break;
        }
        // Drift-drain (platform update) suspend. Unlike 'ended', a
        // suspended thread stays live-resumable: the next message
        // restores the workspace on a fresh agent, so keep the
        // composer enabled and don't render the resume card.
        this._systemMessage(
          (params['message'] as string) ||
            'Session suspended. Send a message to resume where you left off.',
        );
        this.isWaitingForInput.set(false);
        if (this.threadId()) {
          this._settleSuspendedControl(
            this.threadId()!,
            this._canonicalRuntimeGeneration(params['session_runtime_generation']),
          );
        }
        break;

      case 'vm_upgrade.needed': {
        // A sandbox session hit a sudo command (vm_upgrade_required
        // freeze). The accept is the SAME /upgrade-workspace command with
        // the `vm` arg: it routes through the unified upgrade handler,
        // which seeds the VM from the sandbox, opens the sudo gate, and
        // persists the tier (workspace_tier_upgrade.md Q8). The old banner
        // pointed at a nonexistent "upgrade button" / `/upgrade` command.
        const cmd = (params['command'] as string) || '';
        const cmdNote = cmd ? ` (\`${cmd}\` needs root)` : '';
        this._systemMessage(
          `VM upgrade needed: ${(params['reason'] as string) || 'sudo detected'}${cmdNote}. ` +
            `Send /upgrade-workspace vm to move this session onto a VM with sudo ` +
            `(your files carry over).`,
        );
        break;
      }

      case 'vm_upgrade.started':
        this._systemMessage('Upgrading workspace to VM, please wait...');
        break;

      case 'vm_upgrade.complete':
        this._systemMessage(
          'VM upgrade complete. Workspace is now running on a VM with sudo access.',
        );
        break;

      case 'vm_upgrade.failed':
        this._systemMessage(
          `VM upgrade failed: ${(params['reason'] as string) || 'unknown error'}`,
        );
        break;

      case 'workspace_upgrade.needed': {
        // The agent called request_workspace_upgrade — offer the upgrade
        // (HITL: a human accepts before anything provisions). The inline
        // card is the accept path; the settings pane and the
        // /upgrade-workspace slash command remain as the after-reload
        // fallback (the card is live-only). Honor the offered tier; the
        // tool only requests `sandbox` today.
        //
        // This fires mid-stream: request_freeze doesn't stop the turn on
        // the session path, so the agent is still talking when the card
        // appears. Last-write-wins on a second offer — only one can be
        // live, and they can't accumulate.
        const tier = (params['target_tier'] as string) || 'sandbox';
        const reason = (params['reason'] as string) || 'shell/git tools needed';
        this.pendingWorkspaceOffer.set({ tier, reason });
        // Keep a line in the stream so the ask survives in scroll-back
        // once the card resolves — but state the fact only. The card is
        // the verb now, and it says "files carry over" itself.
        this._systemMessage(`The agent requested a ${tier} workspace: ${reason}`);
        break;
      }

      case 'workspace_upgrade.started':
        this.workspaceUpgradeInProgress.update(
          (p) => p ?? { tier: (params['target_tier'] as string) || 'sandbox' },
        );
        this._systemMessage('Provisioning workspace, please wait...');
        break;

      case 'workspace_upgrade.progress': {
        // Heartbeat during a slow (cold) VM provision so a multi-minute
        // wait isn't a silent black box (workspace_tier_upgrade.md Q7).
        // The agent emits this ~once a minute while polling readiness.
        const elapsed = params['elapsed_s'] as number | undefined;
        const tier = (params['target_tier'] as string) || 'workspace';
        this.workspaceUpgradeInProgress.set({ tier, elapsed });
        this._systemMessage(
          typeof elapsed === 'number'
            ? `Still provisioning the ${tier} workspace (${elapsed}s elapsed)…`
            : `Still provisioning the ${tier} workspace…`,
        );
        break;
      }

      case 'workspace_upgrade.complete': {
        const seeded = params['seeded_files'] as number | undefined;
        const seededNote = typeof seeded === 'number' ? ` ${seeded} file(s) carried over.` : '';
        const tier = (params['target_tier'] as string) || '';
        if (tier) this.workspaceTier.set(tier);
        this.workspaceUpgradeInProgress.set(null);
        this.pendingWorkspaceOffer.set(null);
        const sudoNote = tier === 'vm' ? ' Running on a VM — sudo is now available.' : '';
        this._systemMessage(
          `Workspace ready — shell, git, and file tools are now available.` +
            `${sudoNote}${seededNote}`,
        );
        // Read-and-clear before the send (mirrors consume_freeze_request)
        // so a repeated .complete — the server short-circuits an
        // already-satisfied tier straight to this frame — can't send the
        // continuation twice, and so sendMessage's own cancel guard
        // can't cancel this very send.
        const shouldContinue = this.continueAfterUpgrade();
        this.continueAfterUpgrade.set(false);
        if (shouldContinue) {
          // Safe to send now: the agent re-derives its toolset when it
          // dequeues (persistent_graph "get_current_tools" at turn
          // start), and resetup_tools_for_backend already ran before
          // this frame — so the continuation cannot pick up a stale
          // toolset even if the agent is still mid-turn.
          void this.sendMessage(this.transloco.translate('chat.workspaceOffer.continueMessage'));
        }
        break;
      }

      case 'workspace_upgrade.failed':
        this.workspaceUpgradeInProgress.set(null);
        // Don't fall back to re-offering: the reason is stale and the
        // pane still has the button.
        this.pendingWorkspaceOffer.set(null);
        this.continueAfterUpgrade.set(false);
        this._systemMessage(
          `Workspace upgrade failed: ${(params['reason'] as string) || 'unknown error'}`,
        );
        break;

      case 'tasks.updated':
        if (!coveredBySnapshot) {
          this.tasks.set((params['tasks'] as SessionTask[]) || []);
        }
        break;

      case 'file.checkpoint':
        this.undoAvailable.set(true);
        break;

      case 'files.restored':
        this._clearDurableControlErrorAfter(this._takeDurableControlAck(params, 'workspace.undo'));
        this.undoAvailable.set(false);
        this._systemMessage(
          `Restored ${(params['paths'] as string[])?.length || 0} file(s) to pre-edit state.`,
        );
        break;

      case 'rewind.ack': {
        this._clearRewindAckFallback();
        const requestId = this.pendingRewindRequestId ?? String(params['request_id'] ?? '');
        this.pendingRewindRequestId = null;
        this.rewindInFlight.set(false);
        const prompt = params['prompt'] as string | undefined;
        if (prompt) {
          this.rewindPrefill.set({
            prompt,
            draftSnapshot: this.pendingRewindDraftSnapshot,
            draftRevision: this.pendingRewindDraftRevision,
            clientRequestId: requestId,
          });
        }
        // Truncate-then-reload: the IndexedDB cache is append-only
        // (loadHistory merges ?after=), so tombstoned rows must be
        // dropped explicitly or they re-render forever.
        void this._reloadAfterRewind();
        break;
      }

      case 'rewind.done': {
        // Journaled all-viewer signal (arrives via SSE in the new
        // epoch). Idempotent with the initiator's ack-driven reload.
        void this._reloadAfterRewind();
        const threadId = this.threadId();
        const marker = threadId ? this._readRewindMarker(threadId) : null;
        if (
          marker &&
          !marker.prefillConsumed &&
          params['client_request_id'] === marker.clientRequestId
        ) {
          void this._lookupRewindReceipt(marker, true);
        }
        break;
      }

      case 'rewind.files_restored': {
        this._clearRewindAckFallback();
        this.pendingRewindRequestId = null;
        this.rewindInFlight.set(false);
        this._systemMessage('Workspace files restored to the selected point.');
        break;
      }

      case 'workspace_sync.error': {
        const op = (params['op'] as string) || 'sync';
        // The initial cloud->workspace seed failed (degraded:true,
        // op:'initial_pull'): sync is OFF for the whole session — the
        // workspace may be missing files from the cloud and edits will
        // NOT be saved back. That is worse than a per-turn push/pull
        // retry, so surface it as a sticky danger toast + a session-long
        // flag instead of a misleading "will retry next turn" note.
        if (params['degraded'] === true || op === 'initial_pull') {
          this.cloudSyncDegraded.set(true);
          this.cloudSyncDegradedToastId = this.toast.danger(
            `Cloud sync could not start for this session. The workspace may be missing files from the cloud, and changes won't be saved back to it. ${this.sanitizeError(params['message'] as string)}`,
            { duration: 0 },
          );
        } else {
          const turn = params['turn_id'] as number | undefined;
          const turnLabel = turn != null ? ` on turn ${turn}` : '';
          this.toast.warning(
            `Workspace sync (${op}) failed${turnLabel}. Your changes are in the workspace but not yet saved to the cloud. Will retry on next turn.`,
          );
        }
        break;
      }

      case 'workspace_sync.recovered': {
        // The agent rebuilt the sync coordinator at a turn boundary
        // after a degraded attach (the target existed seconds later,
        // or a transient failure cleared). Retract the sticky warning
        // — leaving it up would tell the user their edits aren't
        // being saved when they are.
        this.cloudSyncDegraded.set(false);
        if (this.cloudSyncDegradedToastId !== null) {
          this.toast.dismiss(this.cloudSyncDegradedToastId);
          this.cloudSyncDegradedToastId = null;
        }
        this.toast.success(this.transloco.translate('toasts.sessions.cloudSyncRecovered'));
        break;
      }

      case 'error': {
        // Only clear rewindInFlight for the rewind that raised this
        // error — the backend echoes request_id on every rewind
        // error, so an unrelated in-flight error (e.g. a concurrent
        // config.update denial) can't prematurely re-enable the UI
        // while the real rewind is still pending.
        const errorRequestId = params['request_id'] as string | undefined;
        if (errorRequestId && errorRequestId === this.pendingRewindRequestId) {
          this._clearRewindAckFallback();
          this.pendingRewindRequestId = null;
          this.rewindInFlight.set(false);
        }
        // P0.3: config.update denials carry the orchestrator's detail
        // (e.g. the capability-grant reason) — show it, not just the
        // generic headline.
        const detail = params['detail'] as string | undefined;
        const message = params['message'] as string;
        const sanitized = this.sanitizeError(detail ? `${message}: ${detail}` : message);
        this.error.set(sanitized);
        // A config.update the agent refused (grant denial, fit-ladder
        // rejection, invalid override) settles its caller so the pane can
        // roll the edit back instead of showing a value the session never took.
        if (errorRequestId) {
          this._settlePendingConfigUpdate(errorRequestId, {
            ok: false,
            requestId: errorRequestId,
            transport: 'websocket',
            message: sanitized,
          });
        }
        break;
      }
    }
  }

  /** Close the in-flight turn (if any) as either done or interrupted. */
  private _closeActiveTurnIfAny(kind: 'turn_completed' | 'turn_interrupted'): void {
    const activeId = this.conversation().activeAssistantTurnId;
    if (!activeId) return;
    this.dispatch({ type: kind, turnId: activeId, finishedAt: Date.now() });
  }

  /** Apply an exact interrupt boundary without letting an old replay promote
   *  and close a newer recovered placeholder turn. Returns true only when
   *  the active turn was closed (including a safe live placeholder
   *  promotion); historical exact matches may still be updated. */
  private _interruptTurnIfSafe(
    targetId: string,
    finishedAt: number,
    coveredBySnapshot: boolean,
  ): boolean {
    const state = this.conversation();
    const activeId = state.activeAssistantTurnId;
    const directMatch = state.turns.some((turn) => isAssistantTurn(turn) && turn.id === targetId);
    const activeTurn = activeId
      ? state.turns.find(
          (turn): turn is AssistantTurn => isAssistantTurn(turn) && turn.id === activeId,
        )
      : null;
    const numericTarget = Number(targetId);
    const targetIsCurrent =
      Number.isSafeInteger(numericTarget) &&
      numericTarget > 0 &&
      (numericTarget === this.turnCount() ||
        numericTarget === this.pendingInterruptRequest?.targetTurnId);
    const promoteRecovered =
      !coveredBySnapshot && !directMatch && activeTurn?.recovered === true && targetIsCurrent;

    if (!directMatch && activeId !== targetId && !promoteRecovered) {
      return false;
    }
    this.dispatch({
      type: 'turn_interrupted',
      turnId: targetId,
      finishedAt,
    });
    return activeId === targetId || promoteRecovered;
  }

  /** Apply a reducer action to the conversation state. Streamed deltas
   *  (token/thinking) are buffered and coalesced; every other action folds
   *  the buffer first so wire order is preserved. */
  private dispatch(action: ReducerAction): void {
    if (action.type === 'token' || action.type === 'thinking') {
      this._enqueueDelta(action);
      return;
    }
    this._flushDeltas();
    this.conversation.update((s) => reduce(s, action));
  }

  /** Buffer a streamed delta. Adjacent same-kind deltas concatenate (thinking
   *  only when the messageId matches), keeping the first delta's timestamp. */
  private _enqueueDelta(action: Extract<ReducerAction, { type: 'token' | 'thinking' }>): void {
    const last = this.deltaQueue[this.deltaQueue.length - 1];
    // messageId is undefined on token actions, so this comparison is safe
    // for both kinds: tokens always match (undefined === undefined),
    // thinking merges only within the same reasoning message.
    const sameMessage =
      (last as { messageId?: string } | undefined)?.messageId ===
      (action as { messageId?: string }).messageId;
    if (last && last.type === action.type && sameMessage) {
      last.content += action.content;
    } else {
      this.deltaQueue.push({ ...action });
    }
    this.deltaFlushTimer ??= setTimeout(
      () => this._flushDeltas(),
      PersistentChatService.DELTA_FLUSH_MS,
    );
  }

  /** Fold every buffered delta into `conversation` in one signal write. Safe
   *  to call anytime — a no-op when the queue is empty. Also cancels the
   *  pending flush timer so a stale delta can never leak past teardown. */
  private _flushDeltas(): void {
    if (this.deltaFlushTimer) {
      clearTimeout(this.deltaFlushTimer);
      this.deltaFlushTimer = null;
    }
    if (this.deltaQueue.length === 0) return;
    const queued = this.deltaQueue;
    this.deltaQueue = [];
    this.conversation.update((s) => queued.reduce((acc, a) => reduce(acc, a), s));
  }

  /**
   * Convert backend exception strings into friendly user-facing messages.
   * Raw Python tracebacks and library-internal error strings (e.g. LangChain
   * "Got unknown type ...", "'NoneType' object has no attribute ...") leak
   * implementation details and confuse users. Log the original to console
   * for debugging; surface a generic message instead.
   */
  private sanitizeError(raw: string | undefined | null): string {
    if (!raw) return 'Unknown error';
    const msg = String(raw);

    // Always preserve the original for devs.
    console.warn('[persistent-chat] backend error:', msg);

    // Race-condition fallout: session detached mid-turn.
    if (/'NoneType' object has no attribute/i.test(msg)) {
      return 'Session was interrupted. Try sending your message again or refresh the page.';
    }

    // LangChain provider couldn't classify a message — usually fires
    // after a streaming response gets corrupted (e.g. WS reconnect).
    if (/Got unknown type/i.test(msg)) {
      return 'The assistant returned a malformed response. Try sending your message again.';
    }

    // LLM upstream timeout (10 min) — actionable, but the raw string
    // already says it cleanly.
    if (/LLM call timed out/i.test(msg)) {
      return msg;
    }

    // Python traceback leaked through.
    if (/Traceback \(most recent call last\)/i.test(msg)) {
      return 'Something went wrong on the server. Check the console for details.';
    }

    // Cap length so a long error doesn't blow out the banner.
    if (msg.length > 240) {
      return msg.slice(0, 240) + '…';
    }
    return msg;
  }

  /**
   * Readiness observed from durable state instead of the lifecycle stream.
   *
   * `sessionReady` is otherwise only ever flipped by a *live* signal: the
   * agent's `session.state` welcome frame, an SSE `ready` frame the snapshot
   * cursor did not already cover, or `/connection` resolving — and on a
   * socketless (queue-served) session that last path additionally requires
   * the durable `/state` read of the *same* connect to have succeeded
   * (`sessionSnapshotLoaded`). Any interleaving where those don't line up —
   * a `/state` that failed, a `/connection` still polling behind a
   * not-yet-ready protected-cloud runtime, a replayed `ready` suppressed as
   * covered-by-snapshot — leaves the start panel up over a session that has
   * already answered. See
   * knowledge-base/knowledge/issues/session_start_panel_never_yields_to_a_completed_first_turn.md
   *
   * Durable evidence settles it: a thread that has transcript rows AND whose
   * run-queue unit reached `done` has demonstrably run a turn to completion,
   * so the session is admissible whatever the client saw. Both facts already
   * ride payloads this service fetches (`GET …/messages`, and the `queue`
   * block on `/connection`, `POST …/input` and `GET …/queue`) — nothing new
   * is asked of the server. The pinned lane has no unit (`queue` is null
   * there), so this is a queue-served-lane repair by construction, and
   * `markSessionReady` still owns every retirement/ownership guard.
   */
  private _reconcileReadinessFromDurableState(): void {
    if (this.sessionReady()) return;
    // Only `done` proves a completed turn. `queued`/`leased` are in flight,
    // `parked` is stalled, `none` never enqueued — none of them is evidence.
    if (this.queueState()?.state !== 'done') return;
    // Yield the panel only once the transcript the queue is talking about is
    // actually on screen; otherwise the user trades a spinner for a blank.
    if (this.durableMessageCount() <= 0) return;
    this.markSessionReady();
  }

  /** Mark the session as ready and flush any pending message. */
  private markSessionReady(): void {
    const threadId = this.threadId();
    if (!threadId || !this._controlPlaneAllowed(threadId)) return;
    if (!this.sessionReady()) {
      this.sessionReady.set(true);

      // Clear any transient error left over from the WS reconnect storm
      // during session attach: when the orchestrator polls /ready faster
      // than the agent finishes attaching its session, the agent rejects
      // each WS with an "Agent not ready" frame (persistent_app.py:1489)
      // until attach completes. Those errors are stale the moment we get
      // session.state — keep them on screen and the user sees a red
      // banner contradicting a healthy session.
      this.error.set(null);
      this.isWaitingForInput.set(false);
    }

    // Flush anything the user queued before the session was ready. The
    // bubbles are already on screen (dispatched at send time); _flushOutbox
    // just POSTs them FIFO, one in flight at a time.
    //
    // Deliberately OUTSIDE the ready-edge guard above. This used to be a
    // one-shot latch, so a send that failed *after* the session was already
    // ready could never be retried by any readiness signal — every later
    // markSessionReady returned early before reaching the flush, and the
    // message sat showing "sending" forever (observed live: a session fully
    // reattached, /connection 200, and the queued item was still never
    // POSTed). Re-running the flush on every readiness signal costs nothing
    // when the queue is empty (immediate return) and is single-flighted by
    // `flushTokens` when it isn't. It adds no new double-send risk: the
    // false→true edge already flushed after a reconnect, so this only
    // covers the case that edge misses.
    void this._flushOutbox();
  }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

let _localIdCounter = 0;

/** Generate a short, monotonic, locally-unique id for synthetic turns. */
function makeLocalId(prefix: string): string {
  _localIdCounter += 1;
  return `${prefix}-${Date.now()}-${_localIdCounter}`;
}

/**
 * Group flat HistoryMessage rows into Turns for the rehydration path.
 *
 * - Multiple assistant rows that share a `turn_number` collapse into one
 *   AssistantTurn. Each row contributes (in order) an optional ThoughtEvent
 *   (when `thinking` is populated — see migration 0011), an optional
 *   TextEvent (when `content` is non-empty), and one ToolCallEvent per
 *   entry in `tool_calls`.
 * - `role='tool'` rows are matched back to their originating ToolCallEvent
 *   by `tool_call_id` and populate its `result` / `resultStatus`. Rows
 *   without a matching call (pre-0011 historical data) are dropped — same
 *   user-visible behavior as before this migration.
 */
/**
 * Merge two message lists by `id` (server rows win on conflict), sorted in
 * display order (created_at, then turn_number, then id). Combines the
 * IndexedDB cache with an `?after=` incremental fetch without depending on a
 * cache read-back, so it's correct even when IndexedDB is unavailable.
 */
function mergeMessagesById(a: HistoryMessage[], b: HistoryMessage[]): HistoryMessage[] {
  const byId = new Map<string, HistoryMessage>();
  for (const m of a) byId.set(m.id, m);
  for (const m of b) byId.set(m.id, m);
  return Array.from(byId.values()).sort((x, y) => {
    const c = (x.created_at ?? '').localeCompare(y.created_at ?? '');
    if (c !== 0) return c;
    const t = (x.turn_number ?? 0) - (y.turn_number ?? 0);
    if (t !== 0) return t;
    return x.id.localeCompare(y.id);
  });
}

/**
 * Total staged-change count from a thread cloud-diff summary (Slice C,
 * Task 14) — the sum of added/modified/deleted, driving the status-bar
 * badge. `null` (not loaded, or the thread isn't protected) counts as 0.
 */
export function cloudCountFromSummary(s: ThreadCloudDiffSummary | null): number {
  if (!s) return 0;
  return s.counts.added + s.counts.modified + s.counts.deleted;
}

// Synthetic image-delivery messages ("Image content from tool call <id>:")
// hand a tool's screenshot/page image to a multimodal model
// (src/services/image_content.py + src/persistent_graph.py). The base64 is
// dropped at persist, leaving a bare marker that would otherwise render as a
// user bubble — hide it from the transcript entirely.
//
// Matched on content, before the role dispatch, because the row's role
// changed: these used to persist as 'human' (which made the stateless
// run-queue claim them as unanswered user input and re-run a finished turn)
// and now carry the 'event' persist role. Rows written before that fix keep
// the old role until migration 0211 backfills them, and either role must
// stay invisible — an 'event' row renders as a muted system line, which is
// no better here than a user bubble.
const SYNTHETIC_IMAGE_DELIVERY_RE = /^Image content from tool call \S+:\s*$/;

export function historyToTurns(messages: HistoryMessage[]): Turn[] {
  const turns: Turn[] = [];
  const turnByNumber = new Map<number, AssistantTurn>();
  const toolCallById = new Map<string, ToolCallEvent>();
  let lastCompactionSummary: string | null = null;

  for (const m of messages) {
    const isUser = ['human', 'user', 'HumanMessageChunk'].includes(m.role);
    const isAssistant = ['ai', 'assistant', 'AIMessageChunk'].includes(m.role);
    const isTool = m.role === 'tool' || m.role === 'ToolMessageChunk';

    const ts = m.created_at ? Date.parse(m.created_at) || Date.now() : Date.now();

    // Role-independent (see SYNTHETIC_IMAGE_DELIVERY_RE): these carry no
    // reader-facing content under any role, so drop them before the dispatch
    // rather than inside the user branch alone.
    if (SYNTHETIC_IMAGE_DELIVERY_RE.test(m.content || '')) continue;

    // Compaction boundary marker (role='summary'). Consecutive identical
    // summaries collapse to one marker (threads written before the
    // run-counter gate carry duplicate rows — the duplicate-banner bug).
    // A marker whose turn is already open renders as an inline event at
    // its true position in the event stream; the turn block anchors at
    // its first row, so a top-level divider would otherwise trail the
    // whole turn's content. Markers between turns stay top-level dividers.
    if (m.role === 'summary') {
      const summaryText = m.content ?? '';
      if (summaryText && summaryText === lastCompactionSummary) continue;
      if (summaryText) lastCompactionSummary = summaryText;
      const owner = m.turn_number != null ? turnByNumber.get(m.turn_number) : undefined;
      if (owner) {
        owner.events.push({
          kind: 'compaction',
          id: m.id,
          summary: summaryText,
          startedAt: ts,
        });
      } else {
        turns.push({
          kind: 'compaction',
          id: m.id,
          summary: summaryText,
          timestamp: ts,
        });
      }
      continue;
    }

    // System-injected notice (role='event') → muted system line. Today the
    // only producer is the session-wake feature: a worker job this session
    // created reached a terminal state and the orchestrator injected the
    // notice via POST /api/input
    // (knowledge-base/knowledge/features/session_wake_on_job_completion.md).
    //
    // The role exists precisely so this does NOT render as a user bubble —
    // the model was told something, but the user never said it, and a
    // transcript that claims otherwise is a lie the user cannot audit.
    // Joins 'summary' and 'error' as the third non-conversational role.
    if (m.role === 'event') {
      turns.push({
        kind: 'system',
        id: m.id,
        content: m.content || '',
        timestamp: ts,
      });
      continue;
    }

    // Persisted turn failure (role='error') → muted system line, same
    // treatment the live turn.error frame gets
    // (session_silent_failure_audit.md #2).
    if (m.role === 'error') {
      turns.push({
        kind: 'system',
        id: m.id,
        content: `⚠ ${m.content || 'The turn failed.'}`,
        timestamp: ts,
      });
      continue;
    }

    if (!isUser && !isAssistant && !isTool) continue;

    if (isUser) {
      const u: UserTurn = {
        kind: 'user',
        id: m.id,
        content: m.content || '',
        timestamp: ts,
        historical: true,
      };
      turns.push(u);
      continue;
    }

    if (isTool) {
      // Match result back to the originating call. Rows missing
      // tool_call_id (pre-migration 0011 data) can't be linked and
      // are silently dropped — same as the prior behavior.
      const callId = m.tool_call_id;
      if (!callId) continue;
      const tc = toolCallById.get(callId);
      if (!tc) continue;
      tc.result = m.content ?? '';
      tc.resultStatus = 'ok';
      // Don't clobber a 'denied' decision recorded on the AI side.
      if (tc.status !== 'denied') tc.status = 'completed';
      continue;
    }

    // Assistant row.
    let turn = m.turn_number != null ? turnByNumber.get(m.turn_number) : undefined;
    if (!turn) {
      turn = {
        kind: 'assistant',
        id: m.id,
        events: [],
        status: 'done',
        turnNumber: m.turn_number ?? undefined,
        startedAt: ts,
        finishedAt: ts,
        historical: true,
      };
      if (m.turn_number != null) turnByNumber.set(m.turn_number, turn);
      turns.push(turn);
    }

    if (m.thinking) {
      turn.events.push({
        kind: 'thought',
        id: `${turn.id}.b${turn.events.length}`,
        // The individual AI row id (a turn may group several), keyed so
        // a reasoning frame replayed over this rendered bubble dedupes.
        messageId: m.id,
        content: m.thinking,
        status: 'done',
        startedAt: ts,
      });
    }
    if (m.content) {
      turn.events.push({
        kind: 'text',
        id: `${turn.id}.b${turn.events.length}`,
        content: m.content,
        status: 'done',
        startedAt: ts,
      });
    }
    if (m.tool_calls) {
      for (const tc of m.tool_calls) {
        // Same three-way normalization the live permission.resolved path
        // does, for the same reason: the backend settles an unanswered gate
        // as 'expired', 'interrupted' or 'unavailable', and a client that
        // collapses those into a boolean re-creates the fabricated denial it
        // fixed. Here the wrong default was the mirror image — anything but
        // 'denied'/'expired' rendered 'completed', telling the reader a tool
        // ran that never did.
        const decision =
          tc.decision === 'approved' || tc.decision === 'denied'
            ? tc.decision
            : tc.decision
              ? ('expired' as const)
              : undefined;
        const event: ToolCallEvent = {
          kind: 'tool_call',
          id: tc.id || `${turn.id}.tc${turn.events.length}`,
          tool: tc.name || '',
          args: tc.args || {},
          status:
            decision === 'denied' ? 'denied' : decision === 'expired' ? 'expired' : 'completed',
          decision,
          // Keeps a replayed turn's folded chip reading
          // "19× citations · 12× searches" rather than "38× steps" —
          // the live SSE path sets this from the same registry.
          category: tc.category,
          startedAt: ts,
        };
        turn.events.push(event);
        if (tc.id) toolCallById.set(tc.id, event);
      }
    }
    turn.finishedAt = ts;
  }

  return turns;
}

/** Shape of a message from the REST history endpoint. */
interface HistoryMessage {
  id: string;
  role: string;
  content: string | null;
  /**
   * `category` is stamped at read time by the orchestrator from TOOL_REGISTRY
   * (main.py `_stamp_tool_categories`) — it is not stored on the row. Absent
   * for unknown/renamed tools, in which case the folded-chip summary buckets
   * the call as "other".
   */
  tool_calls:
    | {
        name: string;
        args: Record<string, unknown>;
        id: string;
        /**
         * Raw backend status, not a UI state: 'approved' | 'denied' |
         * 'expired' | 'interrupted' | 'unavailable', and whatever a future
         * non-decision adds. Normalized to the card's three states at
         * hydration — only 'denied' is a refusal.
         */
        decision?: string;
        category?: string;
      }[]
    | null;
  turn_number: number | null;
  /** Set only on role='tool' rows — points to the AI message's tool_calls[].id. */
  tool_call_id?: string | null;
  /** Set only on role='ai' rows that carry reasoning content. */
  thinking?: string | null;
  created_at: string | null;
}
