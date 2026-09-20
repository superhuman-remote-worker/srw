import type { VMCreationView, WorkspaceContractProjection, WorkspaceRecoveryView } from './api.model';

/**
 * Audit step types from MongoDB agent_audit collection.
 */
export type AuditStepType =
  | 'initialize'
  | 'llm'           // Combined: replaces llm_call + llm_response
  | 'tool'          // Combined: replaces tool_call + tool_result
  | 'check'
  | 'routing'
  | 'phase_complete'
  | 'error';

/**
 * Filter categories for audit entries.
 */
export type AuditFilterCategory = 'all' | 'messages' | 'tools' | 'errors';

/**
 * Tool execution details within an audit entry.
 * Contains both call info (arguments) and result info (result_preview, success).
 * Result fields are null while the tool is executing.
 */
export interface AuditToolInfo {
  name: string;
  call_id?: string;
  arguments?: Record<string, unknown>;
  // Result fields - null while pending
  result_preview?: string | null;
  result_size_bytes?: number | null;
  success?: boolean | null;
  error?: string | null;
}

/**
 * LLM interaction details within an audit entry.
 * Contains both call info (model, input_message_count) and response info.
 * Response fields are null while waiting for LLM response.
 */
export interface AuditLLMInfo {
  model?: string;
  input_message_count?: number;
  // Response fields - null while pending
  request_id?: string | null;
  response_content_preview?: string | null;
  tool_calls?: Array<{ name: string; call_id?: string }> | null;
  metrics?: {
    output_chars?: number;
    tool_call_count?: number;
  } | null;
}

/**
 * Error details within an audit entry.
 */
export interface AuditErrorInfo {
  type: string;
  message: string;
  traceback?: string;
}

/**
 * Single audit entry from the agent_audit MongoDB collection.
 */
export interface AuditEntry {
  _id: string;
  /**
   * Transitional: the Postgres audit store sends an integer `id`; the Mongo
   * store sends a string `_id`. ApiService normalizes both into `_id` (string).
   */
  id?: number | string;
  job_id: string;
  step_number: number;
  step_type: AuditStepType;
  node_name: string;
  timestamp: string;
  latency_ms?: number;
  iteration: number;
  phase?: string;           // "strategic" | "tactical"
  phase_number?: number;    // 0, 1, 2, ...
  tool?: AuditToolInfo;
  llm?: AuditLLMInfo;
  error?: AuditErrorInfo;
  state?: Record<string, unknown>;
  // Timing fields for combined events
  started_at?: string;
  completed_at?: string | null;  // null = in progress
}

/**
 * Paginated response from the audit API endpoint.
 */
export interface AuditResponse {
  entries: AuditEntry[];
  total: number;
  page: number;
  pageSize: number;
  hasMore: boolean;
  error?: string;
}

/**
 * Job summary from PostgreSQL jobs table.
 */
export interface JobSummary {
  id: string;
  description: string;
  status: string;
  completion_outcome_kind?: 'blocked_undelivered' | null;
  config_name?: string | null;
  user_id?: string | null;
  project_id?: string | null;
  project_name?: string | null;
  parent_job_id?: string | null;
  error_message?: string | null;
  creation_order?: number | null;
  repo_name?: string | null;
  branch_name?: string | null;
  priority?: number;
  created_at: string;
  audit_count?: number | null;
  snapshot_status?: string | null;
  /** Mode B export marker — set when "Export to shared folder" succeeds. */
  exported_at?: string | null;
  /**
   * Backend-resolved browser URL of that export folder (the stored handle is
   * opaque, so only the orchestrator can build it). Drives the "Open cloud
   * folder" button that replaces "Export to cloud" once `exported_at` is set;
   * null when the cloud backend is down, in which case the row falls back to a
   * plain "Exported" badge.
   */
  exported_folder_url?: string | null;
  /**
   * Backend-computed cloud-review routing (job_cloud_export.md). `'open_folder'`
   * (loose / default-project / no-cloud-folder jobs) shows the "Open cloud
   * folder" button; `'diff'` jobs route to the Mode A diff-review instead.
   */
  cloud_review_mode?: 'diff' | 'open_folder' | null;
  /**
   * True while a sudo/VM-upgrade approval request is open for this job — the
   * job is blocked on a human decision in the inbox, not resumable (Resume
   * would reconnect nothing; the decision drives the job).
   */
  pending_approval?: boolean;
  /** Newest open request id, for the inbox deep-link (`/inbox?sudo=<id>`). */
  pending_approval_request_id?: string | null;
  /** Where the job came from: user|session|automation|loop|officer|subjob|lifecycle|bench. */
  origin?: string;
  /**
   * The display root this row belongs to. The server pages over display
   * roots — a matching job whose parent does not match — and sends each
   * root's matching children along with it, so a tree is never split across
   * pages and the page size always counts whole trees.
   */
  display_root_id?: string;
  /** True when this row IS its display root, false when it is a child of one. */
  is_display_root?: boolean;
  /**
   * How many jobs hang under this one in the database — the UNFILTERED tree.
   *
   * Deliberately not the number of child rows the list is showing. Those two
   * disagree by design: the default `origin` filter hides every subjob, so the
   * rendered child count is 0 on every row and cannot distinguish a job with no
   * children from one whose children are merely filtered out. This is the count
   * that tells a reader there is something under here worth opening.
   */
  subjob_count?: number;
  /** Safe requested/assigned/effective workspace tier observation. */
  workspace_contract?: WorkspaceContractProjection;
  workspace_recovery?: WorkspaceRecoveryView | null;
  vm_creation?: VMCreationView | null;
}

/**
 * One page of `/api/jobs`, matching the server envelope.
 *
 * Counts, limit and offset refer to display roots. Matching children accompany
 * their root, so jobs.length can exceed limit. `total` is null when counting was
 * skipped; `total_is_capped=true` makes a numeric total a lower bound. `has_more`
 * is independent of the count. See orchestrator.schemas.job_list; wire coverage
 * shares fixtures/job-list-page.json with the mounted backend route.
 */
export interface JobListPage {
  jobs: JobSummary[];
  total: number | null;
  total_is_capped: boolean;
  has_more: boolean;
  limit: number;
  offset: number;
  /**
   * Creation-time watermark: pass it back to exclude newer inserts on later
   * pages. Status changes and deletions can still change the matching set.
   * Always present on current server success; optional for older metadata and
   * the synthetic error fallback below.
   */
  as_of?: string;
  /**
   * What the server applied, including its defaults. Always present on current
   * server success; optional for older metadata and the synthetic fallback.
   */
  filters?: JobListFilters;
}

export interface JobListFilters {
  status?: string[];
  origin?: string[];
  project_id?: string[];
  has_project?: boolean | null;
  include_archived_projects?: boolean;
  search?: string | null;
  user_id?: string | null;
}

/**
 * REST query params for `/api/jobs` and `/api/stats/jobs`, in wire shape.
 *
 * Snake_case and array-valued because that is what the API takes — repeated
 * keys for multi-values. `job-filters.ts` is the single place that maps
 * filter state onto this; nothing else should re-derive it.
 */
export type JobListParams = Record<
  string,
  string | number | boolean | readonly string[] | null | undefined
>;

/** What `getJobsPage()` yields when the request fails. */
export const EMPTY_JOB_LIST_PAGE: JobListPage = {
  jobs: [],
  total: 0,
  total_is_capped: false,
  has_more: false,
  limit: 0,
  offset: 0,
};
