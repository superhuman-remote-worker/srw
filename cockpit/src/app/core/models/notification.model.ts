/**
 * The unified notification feed (knowledge-base/knowledge/features/
 * unified_notification_system.md). One durable row per recipient; every
 * channel (email, ntfy, …) is a delivery OF a row, never an independent send.
 *
 * The center never learns what a category *means*: it renders the row, the
 * server-declared `actions`, and POSTs `{action_type, params}` back (D7).
 * Category-specific meaning lives in server-side handlers.
 */

export type NotificationSeverity = 'low' | 'normal' | 'high' | 'critical';

/** Known categories (server `notification_catalog.CATEGORIES`). Open-ended on
 *  purpose: a new server category renders generically until the cockpit
 *  learns an icon/label for it. */
export type NotificationCategory =
  | 'review_queue'
  | 'vm_upgrade'
  | 'budget_exceeded'
  | 'incident'
  | 'officer_question'
  | 'officer_runtime'
  | 'agent_message'
  | 'session_wake'
  | 'loop_event'
  | 'automation_disabled'
  | 'user_registered'
  | 'chat_executors_unavailable'
  | 'session_permission'
  | 'sudo_request'
  | (string & {});

/** Display order of the category chips; unknown categories append after. */
export const KNOWN_CATEGORIES: readonly string[] = [
  'review_queue',
  'sudo_request',
  'session_permission',
  'vm_upgrade',
  'budget_exceeded',
  'incident',
  'agent_message',
  'officer_question',
  'officer_runtime',
  'session_wake',
  'loop_event',
  'automation_disabled',
  'user_registered',
  'chat_executors_unavailable',
];

export type NotificationActionStyle = 'default' | 'primary' | 'danger';
export type NotificationActionInput = 'text' | 'textarea';

/** One server-declared action. `label_key` is a `notifications.actions.*`
 *  transloco key; `input` actions collect `params[input_name]` before posting. */
export interface NotificationAction {
  type: string;
  label_key: string;
  style: NotificationActionStyle;
  input?: NotificationActionInput | null;
  input_name?: string | null;
  params: Record<string, unknown>;
}

export interface SourceRef {
  kind: string;
  id: string;
}

export interface Notification {
  id: string;
  category: NotificationCategory;
  severity: NotificationSeverity;
  subject: string;
  body: string;
  source_ref: SourceRef | null;
  actions: NotificationAction[];
  payload: Record<string, unknown>;
  created_at: string | null;
  /** Rendered in the feed (Knock "seen"). */
  seen_at: string | null;
  /** Explicitly opened / marked. */
  read_at: string | null;
  /** An action was taken on it. */
  interacted_at: string | null;
  /** The underlying source was settled — by anyone (an officer approving
   *  the job resolves the human's row too). */
  resolved_at: string | null;
  resolved_by: string | null;
  archived_at: string | null;
}

export interface NotificationCounts {
  unseen: number;
  unread: number;
  pending: number;
  by_category: Record<string, {pending: number; unseen: number}>;
}

/** `notification.updated` SSE frame / partial patch: only changed fields. */
export type NotificationUpdate = {id: string} & Partial<
  Pick<
    Notification,
    'seen_at' | 'read_at' | 'interacted_at' | 'resolved_at' | 'resolved_by' | 'archived_at'
  >
>;

export interface NotificationFeedPage {
  items: Notification[];
  next_before: string | null;
  counts: NotificationCounts;
}

/** One deferred channel step of a row ("email at 21:45 unless seen/resolved"). */
export interface NotificationStep {
  id: string;
  step_index: number;
  channel: string;
  due_at: string | null;
  conditions: string[];
  batch_key: string | null;
  state: 'pending' | 'done' | 'skipped' | 'cancelled' | 'failed' | (string & {});
  attempt: number;
  settled_at: string | null;
  detail: string | null;
}

// ── Source payloads (`GET /api/notifications/{id}`), keyed by source_kind ──

export interface SourceJob {
  kind: 'job';
  job: {
    id: string;
    status: string;
    description: string | null;
    config_name: string | null;
    project_id: string | null;
    parent_job_id: string | null;
    created_at: string | null;
    updated_at: string | null;
    completed_at: string | null;
    error_message: string | null;
  };
  freeze_data: Record<string, unknown> | null;
}

export interface SudoRequestRow {
  id: string;
  job_id: string | null;
  thread_id: string | null;
  vm_name: string | null;
  command: string;
  arguments: string[] | null;
  working_directory: string | null;
  requesting_user: string | null;
  target_user: string | null;
  status: string;
  requested_at: string | null;
  expires_at: string | null;
  request_type: 'sudo_command' | 'vm_upgrade' | (string & {});
  metadata: Record<string, unknown> | null;
  decision_reason?: string | null;
}

export interface SourceSudoRequest {
  kind: 'sudo_request';
  request: SudoRequestRow;
}

export interface SourceThread {
  kind: 'thread';
  thread: {
    id: string;
    title: string | null;
    project_id: string | null;
    config_name: string | null;
    status: string | null;
    created_at: string | null;
  };
}

export interface SourceMessage {
  id: string;
  job_id: string | null;
  direction: 'outbound' | 'inbound';
  subject: string | null;
  message: string;
  mode: string | null;
  status: string | null;
  read_at: string | null;
  created_at: string | null;
}

export interface SourceMessageThread {
  kind: 'message_thread';
  thread_id: string;
  job_id: string | null;
  messages: SourceMessage[];
}

export interface SourceLoop {
  kind: 'loop';
  loop: {
    id: string;
    project_id: string | null;
    name?: string | null;
    title?: string | null;
    status?: string | null;
    created_at: string | null;
  };
}

export interface SourceAutomation {
  kind: 'automation';
  automation: {
    id: string;
    name: string | null;
    enabled: boolean;
    disabled_reason: string | null;
    created_at: string | null;
  };
}

export interface SourceUser {
  kind: 'user';
  user: {
    id: string;
    email: string | null;
    display_name: string | null;
    is_approved: boolean;
    created_at: string | null;
  };
}

export interface SourcePermissionRequest {
  kind: 'permission_request';
  request: {
    id: string;
    thread_id: string;
    tool_name: string | null;
    tool_args: Record<string, unknown> | string | null;
    status: string;
    requested_at: string | null;
    decided_at: string | null;
    decided_by: string | null;
  };
}

export type NotificationSource =
  | SourceJob
  | SourceSudoRequest
  | SourceThread
  | SourceMessageThread
  | SourceLoop
  | SourceAutomation
  | SourceUser
  | SourcePermissionRequest;

/** `GET /api/notifications/{id}` — the row, its source's presentation
 *  payload, and its deferred channel steps. */
export interface NotificationDetail {
  notification: Notification;
  source: NotificationSource | null;
  steps?: NotificationStep[];
}

export interface NotificationActResponse {
  status: string;
  result: Record<string, unknown>;
  notification: Notification;
}

export const EMPTY_NOTIFICATION_COUNTS: NotificationCounts = {
  unseen: 0,
  unread: 0,
  pending: 0,
  by_category: {},
};

/** Sort weight for the action-center comparator: pending rows by severity. */
export const SEVERITY_URGENCY: Record<NotificationSeverity, number> = {
  critical: 95,
  high: 75,
  normal: 45,
  low: 25,
};

/** `kind:id` — the key the deep links and the feed share. */
export function sourceKey(ref: SourceRef | null | undefined): string | null {
  return ref ? `${ref.kind}:${ref.id}` : null;
}
