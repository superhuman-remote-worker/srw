import {inject, Injectable} from '@angular/core';
import {
    HttpClient,
    HttpErrorResponse,
    HttpEventType,
    HttpParams,
    HttpResponse,
    HttpUploadProgressEvent,
} from '@angular/common/http';
import {catchError, filter, map, Observable, of, tap, throwError, timeout} from 'rxjs';
import {TranslocoService} from '@jsverse/transloco';
import {AppToastService} from '../../ui/toast';
import {ErrorMessageService} from './error-message.service';
import {
    Agent,
    AgentStatistics,
    ColumnDef,
    DailyStatistics,
    Datasource,
    DatasourceCatalogFilters,
    DatasourceCatalogResponse,
    DatasourceCreateRequest,
    DatasourceIndexStatus,
    DatasourceReindexResult,
    DatasourceTestResult,
    DatasourceUpdateRequest,
    EligibleDatasource,
    LinkableDatasourceProjectsResponse,
    LinkableProjectDatasourceFilters,
    LinkableProjectDatasourcesResponse,
    SSHKeyGenerateResponse,
    Expert,
    ExpertDefaultForkResult,
    ExpertDefaultsResponse,
    ExpertCreateRequest,
    ExpertDetail,
    ExpertDuplicateResult,
    ExpertRole,
    ExpertUpdateRequest,
    Skill,
    SkillCreateRequest,
    SkillDetail,
    SkillUpdateRequest,
    Job,
    JobAcceptConflict,
    JobAcceptOutcome,
    JobAcceptPartialFailure,
    JobAcceptResult,
    JobCloudExportResult,
    JobCreateRequest,
    DiffLoadOutcome,
    DiffRejectOutcome,
    JobDiffFile,
    JobDiffSummary,
    JobProgress,
    JobUsage,
    JobSubagentRoster,
    JobSubjobRoster,
    JobRejectResult,
    JobReviewSessionResult,
    JobStatistics,
    KnowledgeListResponse,
    KnowledgeNoteDetail,
    KnowledgeSearchResponse,
    KnowledgeSummary,
    MemoryListResponse,
    MemoryStats,
    OfficerCommissionResult,
    OfficerDecommissionResult,
    OfficerPost,
    OfficerPostPatch,
    Project,
    ProjectBacklog,
    ProjectLoop,
    ProjectLoopStartRequest,
    ProjectArchiveReport,
    ProjectCreateRequest,
    ExternalKnowledgeBaseRequest,
    ProjectDatasource,
    ProjectMember,
    ProjectMemberAddRequest,
    ProjectMemberUpdateRequest,
    ProjectRepository,
    ProjectRepositoryCreateRequest,
    ProjectRepositoryUpdateRequest,
    ProjectStatus,
    ProjectUpdateRequest,
    PullRequestStatus,
    PromoteRequest,
    StuckJobsResponse,
    TableDataResponse,
    TableInfo,
    ThreadCloudApplyResult,
    ThreadCloudRejectResult,
    ThreadCloudDiffFile,
    ThreadCloudDiffSummary,
    PersistentThreadHistory,
    SshGatewayHostKeysResponse,
    User,
    UserCapabilities,
    VoiceCapabilities,
} from '../models/api.model';
import type {
  AdminCapacity,
  SessionQueueState,
  SessionQueueRetryOutcome,
  RunQueueUnparkResult,
} from '../models/api.model';
import {
  ThreadUploadEvent,
  ThreadUploadResponse,
  UploadInfo,
  UploadResponse,
} from '../models/file.model';
import {
  AuditEntry,
  AuditFilterCategory,
  AuditResponse,
  EMPTY_JOB_LIST_PAGE,
  JobListPage,
  JobListParams,
  JobSummary,
} from '../models/audit.model';
import {LLMRequest} from '../../workbench/request.model';
import {GraphChangeResponse} from '../../workbench/graph.model';
import {ChatEntry, ChatHistoryResponse} from '../models/chat.model';
import {ThreadDetail} from '../models/action.model';
import {
  TtsVoicesResponse,
  TtsLibraryResponse,
  TtsLibraryFilters,
  TtsLibrarySetting,
} from '../models/tts-voices';
import {environment} from '../environment';

/**
 * Audit-store id normalization (transitional). The store is migrating
 * MongoDB(`_id`: string ObjectId) -> Postgres(`id`: integer), and the backend
 * is flag-selected, so an entry may arrive with either `_id` (string) or `id`
 * (number), and `request_id` as either. Coercing `_id`/`request_id` to strings
 * at ingestion keeps every downstream consumer (track keys, IndexedDB primary
 * key, `.slice()` display, the 24-hex regex) working unchanged on both backends.
 */
function normalizeAuditEntry(e: AuditEntry): AuditEntry {
  e._id = String(e.id ?? e._id ?? '');
  if (e.llm && e.llm.request_id != null) {
    e.llm.request_id = String(e.llm.request_id);
  }
  return e;
}

function normalizeChatEntry(e: ChatEntry): ChatEntry {
  e._id = String(e.id ?? e._id ?? '');
  if (e.request_id != null) {
    e.request_id = String(e.request_id);
  }
  return e;
}

function normalizeLLMRequest(r: LLMRequest): LLMRequest {
  r._id = String(r.id ?? r._id ?? '');
  return r;
}

/**
 * Job version info for cache invalidation.
 */
export interface JobVersionInfo {
  version: number;
  auditEntryCount: number;
  chatEntryCount: number;
  graphDeltaCount: number;
  lastUpdate: string;
}

/**
 * IDE session status from the orchestrator.
 */
export interface IdeSessionStatus {
  status: 'unavailable' | 'available' | 'restoring' | 'active' | 'idle' | 'expired' | 'failed';
  code_server_url?: string | null;
    gitea_url?: string | null;
  snapshot_type?: string;
  estimated_seconds?: number;
  expires_at?: string;
  started_at?: string;
  source?: string;
  restore_type?: 'vm' | 'container' | 'k8s_container';
  error?: string;
  /** Typed refusal code when the orchestrator withheld the IDE URL. */
  code?: string;
}

/**
 * Server-resolved enablement of the closed session tool groups.
 *
 * `source` names the agent path the answer models: `resolved` (the
 * orchestrator-resolved blob the agent hydrates), `legacy` (experts off — an
 * unset group is enabled there), or `error`, in which case `tool_groups` is
 * null and the caller falls back to its own base defaults.
 */
/**
 * Per-category answer from the tool-groups read. `state` is three-valued
 * because a checkbox cannot express the truth: `off` is a promise that ticking
 * would work, and the server only makes it when it can be kept — a group whose
 * config grants tools the agent did not bind is `unavailable`, not `off`.
 */
export interface SessionToolCategory {
  state: 'on' | 'off' | 'unavailable';
  /** Non-null whenever `settable` is false. Safe to show the user. */
  reason: string | null;
  settable: boolean;
  /** Which layer produced the answer: grant / backend / runtime / registry / a config layer. */
  decided_by: string;
  tools: string[];
  /** What the merged config asked for. Measured answers only. */
  configured?: string[];
}

/** One `subagent_type` the expert's `delegate_agent` can name. */
export interface SessionSubagentRosterEntry {
  name: string;
  /** What the parent model sees next to the type; null on a raw `$ref` the server could not resolve. */
  description: string | null;
  /** `subagents/explorer`, `experts/critic`, a DB expert id — or null for an inline entry. */
  ref: string | null;
}

/**
 * What a Delegation tick reaches: the roster as the resolve materialised it.
 * `roster: []` is a real answer — the expert has no roster and every
 * `delegate_agent` call will error — and is rendered as such. Absent on an
 * orchestrator older than this contract, where nothing is claimed.
 */
export interface SessionSubagentRoster {
  default: string | null;
  roster: SessionSubagentRosterEntry[];
}

export interface SessionToolGroupsResponse {
  workspace?: import("../models/workspace.model").WorkspacePreview;
  thread_id: string;
  /** Which agent path the PREDICTION models. Says nothing about `origin`. */
  source: 'resolved' | 'legacy' | 'error';
  /**
   * The discriminator, and the only one — do not infer measured-ness from
   * `observed_at`, which is legitimately null on `agent_partial`.
   *
   * - `agent`         a running pod reported its bound toolset, in full
   * - `agent_partial` a pod reported, but names only (older agent image):
   *                   trust `tools`, do not render a workspace-tier story
   * - `prediction`    no agent to ask; a forecast from the merged config,
   *                   which cannot see the runtime injection layer or the
   *                   backend gate. Never render this as fact.
   */
  origin?: 'agent' | 'agent_partial' | 'prediction';
  observed_at?: string | null;
  /** Why this is a forecast. `origin === 'prediction'` only. */
  prediction_reason?: string | null;
  /** What the measurement is missing. `origin === 'agent_partial'` only. */
  degraded_reason?: string | null;
  /** Workspace capabilities as the agent reported them. `origin === 'agent'` only. */
  backend?: Record<string, boolean> | null;
  tool_groups: Record<string, boolean> | null;
  categories?: Record<string, SessionToolCategory> | null;
  /** The delegation roster the resolve materialised; null when the resolve failed. */
  subagents?: SessionSubagentRoster | null;
  /**
   * Categories that refuse `tools.<c>: true` at the write boundary, mapped to
   * the enumeration to send instead (`{shell: ["run_command", ...]}`).
   *
   * `true` auto-tracks the registry, which is the wrong default for a
   * code-execution category — so `shell` is settable only by naming its tools,
   * and the server names them rather than the cockpit keeping a list that can
   * go stale. Absent from an orchestrator older than this contract; a client
   * then falls back to `true` and gets a 400 naming the rule, which is the
   * correct failure for a request the boundary will not honour.
   */
  enumerate_only?: Record<string, string[]> | null;
}

/**
 * Snapshot storage statistics from the orchestrator.
 */
export interface SnapshotStorageStats {
  available: boolean;
  total_snapshots: number;
  total_size_bytes: number;
  gc_pending_count: number;
  gc_pending_size_bytes: number;
  error?: string;
}

/**
 * Client-side deadline for the tool-groups read, which the settings pane
 * blocks on. The server bounds its own agent probe at 3s; this sits above that
 * so only a genuinely stuck request trips it, and guarantees the pane's
 * `lastApplied` baseline always gets anchored.
 */
export const SESSION_TOOL_GROUPS_TIMEOUT_MS = 8000;

/**
 * Percent-encode an `uploads/`-relative path for the delete route's
 * `{path:path}` segment.
 *
 * Per SEGMENT, never wholesale: the route matches a multi-segment path, so the
 * `/` separators of a zip-extracted member (`bundle/sub/a.txt`) must survive
 * while everything else is escaped. An unencoded `#` truncates the URL at the
 * fragment and an unencoded `?` at the query string, both of which would send a
 * DELETE for a shorter path than the caller asked for.
 */
export function encodeUploadPath(name: string): string {
  return name.split('/').map(encodeURIComponent).join('/');
}

/**
 * HTTP client service for the cockpit API.
 */
/**
 * Build HttpParams from a wire-shaped query record.
 *
 * Array values become repeated keys (`?status=a&status=b`), matching both the
 * REST API and the URL codec; null/undefined are dropped so a caller can pass
 * a sparse record without pruning it first.
 */
function toHttpParams(query: JobListParams): HttpParams {
  let params = new HttpParams();
  for (const [key, value] of Object.entries(query)) {
    if (value === null || value === undefined) continue;
    if (Array.isArray(value)) {
      for (const item of value) params = params.append(key, String(item));
    } else {
      params = params.set(key, String(value));
    }
  }
  return params;
}

@Injectable({ providedIn: 'root' })
export class ApiService {
  private readonly http = inject(HttpClient);
  private readonly toast = inject(AppToastService);
  private readonly transloco = inject(TranslocoService);
  private readonly errors = inject(ErrorMessageService);
  private readonly baseUrl = environment.apiUrl;

  private t(key: string, params?: Record<string, unknown>): string {
    return this.transloco.translate(key, params);
  }

  /**
   * Get list of available tables with row counts.
   */
  getTables(): Observable<TableInfo[]> {
    return this.http.get<TableInfo[]>(`${this.baseUrl}/tables`).pipe(
      catchError((error) => {
        console.error('Failed to fetch tables:', error);
        return of([]);
      }),
    );
  }

  /**
   * Get paginated data from a table.
   */
  getTableData(
    tableName: string,
    page: number = 1,
    pageSize: number = 50,
  ): Observable<TableDataResponse> {
    const params = new HttpParams()
      .set('page', page.toString())
      .set('pageSize', pageSize.toString());

    return this.http
      .get<TableDataResponse>(`${this.baseUrl}/tables/${tableName}`, { params })
      .pipe(
        catchError((error) => {
          console.error(`Failed to fetch data for table ${tableName}:`, error);
          return of({
            columns: [],
            rows: [],
            total: 0,
            page: 1,
            pageSize: 50,
          });
        }),
      );
  }

  /**
   * Get column definitions for a table.
   */
  getTableSchema(tableName: string): Observable<ColumnDef[]> {
    return this.http
      .get<ColumnDef[]>(`${this.baseUrl}/tables/${tableName}/schema`)
      .pipe(
        catchError((error) => {
          console.error(`Failed to fetch schema for table ${tableName}:`, error);
          return of([]);
        }),
      );
  }

  /**
   * Get list of jobs with optional status and user filter.
   *
   * `/api/jobs` returns a paging envelope; this unwraps it so existing
   * callers keep receiving a plain array. The filter bar and pagination
   * controls need `total`/`has_more`, so they call `getJobsPage()` instead.
   */
  getJobs(status?: string, limit: number = 100, userId?: string): Observable<JobSummary[]> {
    const query: JobListParams = {limit};
    if (status) query['status'] = status;
    if (userId) query['user_id'] = userId;
    return this.getJobsPage(query).pipe(map((page) => page.jobs));
  }

  /**
   * Get one page of jobs together with the counts a paging UI needs.
   *
   * Takes REST-shaped params rather than a camelCase options object on
   * purpose: `job-filters.ts` already owns the filter-state → wire mapping,
   * and a second mapping here would be a second thing to keep in step.
   */
  getJobsPage(query: JobListParams = {}): Observable<JobListPage> {
    return this.http
      .get<JobListPage>(`${this.baseUrl}/jobs`, {params: toHttpParams(query)})
      .pipe(
        catchError((error) => {
          console.error('Failed to fetch jobs:', error);
          return of(EMPTY_JOB_LIST_PAGE);
        }),
      );
  }

  /**
   * Get paginated audit entries for a job from MongoDB.
   */
  getJobAudit(
    jobId: string,
    page: number = 1,
    pageSize: number = 50,
    filter: AuditFilterCategory = 'all',
  ): Observable<AuditResponse> {
    const params = new HttpParams()
      .set('page', page.toString())
      .set('pageSize', pageSize.toString())
      .set('filter', filter);

    return this.http
      .get<AuditResponse>(`${this.baseUrl}/jobs/${jobId}/audit`, { params })
      .pipe(
        map((response) => {
          response.entries?.forEach(normalizeAuditEntry);
          return response;
        }),
        catchError((error) => {
          console.error(`Failed to fetch audit for job ${jobId}:`, error);
          return of({
            entries: [],
            total: 0,
            page: 1,
            pageSize: 50,
            hasMore: false,
            error: error.message || 'Failed to fetch audit data',
          });
        }),
      );
  }

  /**
   * Lean, offset-paged audit page for the virtual-scroll trace view.
   *
   * Hits the same `/audit` endpoint as {@link getJobAudit} but with `lean=true`
   * (server drops per-row metadata + heavy expand-only payload sub-keys) and
   * offset/limit paging. Heavy detail is fetched on demand via
   * {@link getAuditStep}. `limit` is capped server-side at 200.
   */
  getAuditPage(
    jobId: string,
    offset: number,
    limit: number,
    filter: AuditFilterCategory = 'all',
    order: 'asc' | 'desc' = 'asc',
  ): Observable<AuditResponse> {
    const params = new HttpParams()
      .set('offset', offset.toString())
      .set('limit', limit.toString())
      .set('filter', filter)
      .set('order', order)
      .set('lean', 'true');

    return this.http
      .get<AuditResponse>(`${this.baseUrl}/jobs/${jobId}/audit`, { params })
      .pipe(
        map((response) => {
          response.entries?.forEach(normalizeAuditEntry);
          return response;
        }),
        catchError((error) => {
          console.error(`Failed to fetch audit page for job ${jobId}:`, error);
          return of({
            entries: [],
            total: 0,
            page: 1,
            pageSize: limit,
            hasMore: false,
            error: error.message || 'Failed to fetch audit page',
          });
        }),
      );
  }

  /**
   * Full detail for a single audit step (heavy payload + metadata), lazy-loaded
   * when a trace row is expanded. Returns null on failure.
   */
  getAuditStep(jobId: string, stepId: number | string): Observable<AuditEntry | null> {
    return this.http
      .get<AuditEntry>(`${this.baseUrl}/jobs/${jobId}/audit/step/${stepId}`)
      .pipe(
        map((entry) => {
          if (entry) normalizeAuditEntry(entry);
          return entry;
        }),
        catchError((error) => {
          console.error(`Failed to fetch audit step ${stepId}:`, error);
          return of(null);
        }),
      );
  }

  /**
   * Get a single LLM request by MongoDB document ID.
   */
  getRequest(docId: string): Observable<LLMRequest | null> {
    return this.http.get<LLMRequest>(`${this.baseUrl}/requests/${docId}`).pipe(
      map((request) => (request ? normalizeLLMRequest(request) : request)),
      catchError((error) => {
        console.error(`Failed to fetch request ${docId}:`, error);
        return of(null);
      }),
    );
  }

  /**
   * Get graph changes for a job (Neo4j operations from audit trail).
   */
  getGraphChanges(jobId: string): Observable<GraphChangeResponse> {
    return this.http
      .get<GraphChangeResponse>(`${this.baseUrl}/graph/changes/${jobId}`)
      .pipe(
        catchError((error) => {
          console.error(`Failed to fetch graph changes for job ${jobId}:`, error);
          throw error;
        }),
      );
  }

  /**
   * Get the time range (first/last timestamps) for a job's audit entries.
   * @deprecated Use DataService.timeRange() computed signal instead.
   */
  getAuditTimeRange(
    jobId: string,
  ): Observable<{ start: string; end: string } | null> {
    return this.http
      .get<{ start: string; end: string } | null>(
        `${this.baseUrl}/jobs/${jobId}/audit/timerange`,
      )
      .pipe(
        catchError((error) => {
          console.error(`Failed to fetch audit time range for job ${jobId}:`, error);
          return of(null);
        }),
      );
  }

  /**
   * Get paginated chat history for a job from the audit store.
   * Returns a clean sequential view of conversation turns.
   *
   * `lean` strips full message bodies (previews + `truncated` markers only);
   * hydrate individual turns via {@link getChatEntry}.
   */
  getChatHistory(
    jobId: string,
    page: number = 1,
    pageSize: number = 50,
    lean: boolean = false,
  ): Observable<ChatHistoryResponse> {
    let params = new HttpParams()
      .set('page', page.toString())
      .set('pageSize', pageSize.toString());
    if (lean) {
      params = params.set('lean', 'true');
    }

    return this.http
      .get<ChatHistoryResponse>(`${this.baseUrl}/jobs/${jobId}/chat`, { params })
      .pipe(
        map((response) => {
          response.entries?.forEach(normalizeChatEntry);
          return response;
        }),
        catchError((error) => {
          console.error(`Failed to fetch chat history for job ${jobId}:`, error);
          return of({
            entries: [],
            total: 0,
            page: 1,
            pageSize: 50,
            hasMore: false,
            error: error.message || 'Failed to fetch chat history',
          });
        }),
      );
  }

  /**
   * Get one full chat turn (complete inputs/response bodies) by entry id.
   * Detail fetch behind the lean listing.
   */
  getChatEntry(jobId: string, entryId: string): Observable<ChatEntry | null> {
    return this.http
      .get<ChatEntry>(`${this.baseUrl}/jobs/${jobId}/chat/entry/${entryId}`)
      .pipe(
        map((entry) => (entry ? normalizeChatEntry(entry) : entry)),
        catchError((error) => {
          console.error(`Failed to fetch chat entry ${entryId}:`, error);
          return of(null);
        }),
      );
  }

  /**
   * Get job data version for cache invalidation.
   */
  getJobVersion(jobId: string): Observable<JobVersionInfo | null> {
    return this.http.get<JobVersionInfo>(`${this.baseUrl}/jobs/${jobId}/version`).pipe(
      catchError((error) => {
        console.error(`Failed to fetch job version for ${jobId}:`, error);
        return of(null);
      }),
    );
  }

  // ===== User Endpoints =====

  /**
   * Get list of users.
   */
  getUsers(): Observable<User[]> {
    return this.http.get<User[]>(`${this.baseUrl}/users`).pipe(
      catchError(() => of([])),
    );
  }

  /**
   * Create a new user.
   */
  createUser(body: { display_name: string; avatar_color?: string }): Observable<User | null> {
    return this.http.post<User>(`${this.baseUrl}/users`, body).pipe(
      catchError(() => of(null)),
    );
  }

  /**
   * Update a user.
   */
  updateUser(id: string, body: { display_name?: string; avatar_color?: string }): Observable<{ status: string } | null> {
    return this.http.put<{ status: string }>(`${this.baseUrl}/users/${id}`, body).pipe(
      catchError(() => of(null)),
    );
  }

  /**
   * Delete a user.
   */
  deleteUser(id: string): Observable<{ status: string } | null> {
    return this.http.delete<{ status: string }>(`${this.baseUrl}/users/${id}`).pipe(
      catchError(() => of(null)),
    );
  }

  // ===== Expert Discovery Endpoints =====

  /**
   * Get list of available expert configurations.
   *
   * `expertType` filters server-side on `expert_type || tags` (a session row
   * tagged `worker` lists under `worker`); `subagent` lists the
   * `config/subagents/*` library (rows with `name = subagents/<id>`, the
   * roster `$ref` spelling) plus every expert tagged `subagent`. `showAll`
   * drops the role filter — the "Show all experts" toggle of the pickers and
   * the roster reference picker; the server accepts a cross-role pick.
   */
  getExperts(expertType?: ExpertRole, opts?: {showAll?: boolean}): Observable<Expert[]> {
    const type = opts?.showAll ? undefined : expertType;
    const params = type ? new HttpParams().set('type', type) : undefined;
    return this.http.get<Expert[]>(`${this.baseUrl}/experts`, {params}).pipe(
      catchError(() => of([])),
    );
  }

  /** Effective application/project/personal expert defaults for this caller. */
  getExpertDefaults(projectId?: string | null): Observable<ExpertDefaultsResponse | null> {
    const params = projectId ? new HttpParams().set('project_id', projectId) : undefined;
    return this.http
      .get<ExpertDefaultsResponse>(`${this.baseUrl}/expert-defaults`, {params})
      .pipe(catchError(() => of(null)));
  }

  setPersonalExpertDefault(type: 'worker' | 'session', expertId: string): Observable<unknown> {
    return this.http.put(`${this.baseUrl}/expert-defaults/${type}`, {expert_id: expertId});
  }

  clearPersonalExpertDefault(type: 'worker' | 'session'): Observable<unknown> {
    return this.http.delete(`${this.baseUrl}/expert-defaults/${type}`);
  }

  /**
   * Atomically fork a visible expert (bundled or DB) and select the copy as
   * this user's personal default. May come back with `dropped` set — same
   * strip-and-report meaning as `duplicateExpert` above (task 4 of the same
   * 2026-08-04 decision): grant keys the source config needed that the
   * caller doesn't hold, stripped rather than refusing the fork. Callers
   * MUST surface `dropped` when non-empty, same reason as `duplicateExpert`.
   */
  forkPersonalExpertDefault(
    type: 'worker' | 'session',
    expertId?: string,
  ): Observable<ExpertDefaultForkResult> {
    return this.http.post<ExpertDefaultForkResult>(`${this.baseUrl}/expert-defaults/${type}/fork`, {
      expert_id: expertId ?? null,
    });
  }

  getApplicationExpertDefaults(): Observable<{defaults: Partial<Record<'worker' | 'session', Expert>>}> {
    return this.http.get<{defaults: Partial<Record<'worker' | 'session', Expert>>}>(
      `${this.baseUrl}/admin/expert-defaults`,
    );
  }

  setApplicationExpertDefault(type: 'worker' | 'session', expertId: string): Observable<unknown> {
    return this.http.put(`${this.baseUrl}/admin/expert-defaults/${type}`, {
      expert_id: expertId,
    });
  }

  /**
   * Get full expert detail including merged config and instructions.
   *
   * `accountDefaults` folds the caller's account fallback layer into `config`
   * at the precedence the server's resolver uses. **Create forms must pass it**
   * — without it the form resolves a different config than dispatch will build
   * (e.g. `workspace.backend` reads the base's `sandbox` while a session
   * actually boots `virtual`, which used to leave repository connectors
   * selectable and 400 every create). The expert editor must NOT pass it: its
   * diff baseline has to stay the pure framework base.
   *
   * `role` resolves the expert in that role (`?role=worker|session|subagent`,
   * cross-role allowed — a session expert previewed as a subagent); the
   * payload keeps `expert_type` and adds `resolved_role`.
   */
  getExpertDetail(
    expertId: string,
    opts?: {accountDefaults?: boolean; role?: ExpertRole},
  ): Observable<ExpertDetail | null> {
    const parts: string[] = [];
    if (opts?.accountDefaults) parts.push('account_defaults=true');
    if (opts?.role) parts.push(`role=${opts.role}`);
    const qs = parts.length ? `?${parts.join('&')}` : '';
    return this.http.get<ExpertDetail>(`${this.baseUrl}/experts/${expertId}${qs}`).pipe(
      catchError(() => of(null)),
    );
  }

  /**
   * Create a DB-backed expert. Errors propagate so callers can surface the
   * 409 (name collision) / 422 (credential section) the server returns.
   */
  createExpert(body: ExpertCreateRequest): Observable<ExpertDetail> {
    return this.http.post<ExpertDetail>(`${this.baseUrl}/experts`, body);
  }

  /**
   * Update an owned DB-backed expert (owner or admin). Bumps ``version``.
   */
  updateExpert(id: string, body: ExpertUpdateRequest): Observable<ExpertDetail> {
    return this.http.put<ExpertDetail>(`${this.baseUrl}/experts/${id}`, body);
  }

  /**
   * Delete an owned DB-backed expert. Rejects (409) while live-referenced —
   * the error body carries ``detail.blockers``.
   */
  deleteExpert(id: string): Observable<{ deleted: boolean }> {
    return this.http.delete<{ deleted: boolean }>(`${this.baseUrl}/experts/${id}`);
  }

  /**
   * Fork any visible expert (bundled or DB) into an owned copy. May come
   * back with `dropped` set: grant keys the source config needed that the
   * caller doesn't hold, stripped rather than refusing the fork. Callers
   * MUST surface `dropped` when non-empty — a silent strip is the exact
   * "silent capability downgrade" decision 9 exists to prevent.
   */
  duplicateExpert(id: string): Observable<ExpertDuplicateResult> {
    return this.http.post<ExpertDuplicateResult>(
      `${this.baseUrl}/experts/${id}/duplicate`,
      {},
    );
  }

  /**
   * Serialize an expert to a portable bundle (raw fragment, no credentials).
   */
  exportExpert(id: string): Observable<Record<string, unknown>> {
    return this.http.get<Record<string, unknown>>(`${this.baseUrl}/experts/${id}/export`);
  }

  /**
   * Create an owned expert from a posted bundle (fork-on-import).
   */
  importExpert(body: ExpertCreateRequest): Observable<ExpertDetail> {
    return this.http.post<ExpertDetail>(`${this.baseUrl}/experts/import`, body);
  }

  // ===== Skill Endpoints (Agent Skills) =====

  /** List skills (bundled + DB-backed). Fails gracefully to []. */
  getSkills(): Observable<Skill[]> {
    return this.http
      .get<Skill[]>(`${this.baseUrl}/skills`)
      .pipe(catchError(() => of([])));
  }

  /** Full skill detail incl. the file tree. */
  getSkillDetail(id: string): Observable<SkillDetail | null> {
    return this.http
      .get<SkillDetail>(`${this.baseUrl}/skills/${id}`)
      .pipe(catchError(() => of(null)));
  }

  /** Create a DB skill. Errors propagate (409 name collision / 422 malformed). */
  createSkill(body: SkillCreateRequest): Observable<SkillDetail> {
    return this.http.post<SkillDetail>(`${this.baseUrl}/skills`, body);
  }

  /** Update an owned DB skill. Bumps version. */
  updateSkill(id: string, body: SkillUpdateRequest): Observable<SkillDetail> {
    return this.http.put<SkillDetail>(`${this.baseUrl}/skills/${id}`, body);
  }

  /** Delete an owned DB skill. */
  deleteSkill(id: string): Observable<{deleted: boolean}> {
    return this.http.delete<{deleted: boolean}>(`${this.baseUrl}/skills/${id}`);
  }

  /** Fork any visible skill into an owned copy. */
  duplicateSkill(id: string): Observable<SkillDetail> {
    return this.http.post<SkillDetail>(`${this.baseUrl}/skills/${id}/duplicate`, {});
  }

  /** Download a skill as a native zip (drops into .claude/skills). */
  exportSkill(id: string): Observable<Blob> {
    return this.http.get(`${this.baseUrl}/skills/${id}/export`, {
      responseType: 'blob',
    });
  }

  /** Import a skill from an uploaded zip (fork-on-collision). */
  importSkill(file: File): Observable<SkillDetail> {
    const fd = new FormData();
    fd.append('file', file);
    return this.http.post<SkillDetail>(`${this.baseUrl}/skills/import`, fd);
  }

  // ===== Agent Management Endpoints =====

  /**
   * Get list of registered agents.
   */
  getAgents(status?: string, limit: number = 100): Observable<Agent[]> {
    let params = new HttpParams().set('limit', limit.toString());
    if (status) {
      params = params.set('status', status);
    }

    return this.http.get<Agent[]>(`${this.baseUrl}/agents`, { params }).pipe(
      catchError((error) => {
        console.error('Failed to fetch agents:', error);
        return of([]);
      }),
    );
  }

  /**
   * Get a single agent by ID.
   */
  getAgent(agentId: string): Observable<Agent | null> {
    return this.http.get<Agent>(`${this.baseUrl}/agents/${agentId}`).pipe(
      catchError((error) => {
        console.error(`Failed to fetch agent ${agentId}:`, error);
        return of(null);
      }),
    );
  }

  /**
   * Delete (deregister) an agent.
   */
  deleteAgent(agentId: string): Observable<{ status: string } | null> {
    return this.http.delete<{ status: string }>(`${this.baseUrl}/agents/${agentId}`).pipe(
      catchError((error) => {
        console.error(`Failed to delete agent ${agentId}:`, error);
        return of(null);
      }),
    );
  }

  // ===== Datasource Management Endpoints =====

  /**
   * Get list of datasources with optional filters.
   */
  getDatasources(jobId?: string, type?: string): Observable<Datasource[]> {
    let params = new HttpParams();
    if (jobId) {
      params = params.set('job_id', jobId);
    }
    if (type) {
      params = params.set('type', type);
    }

    return this.http.get<Datasource[]>(`${this.baseUrl}/datasources`, { params }).pipe(
      catchError((error) => {
        console.error('Failed to fetch datasources:', error);
        return of([]);
      }),
    );
  }

  /** Cursor-paginated management catalog. Authorization and filtering happen
   * before the server applies the limit, so older owned connectors are not
   * hidden behind unrelated rows. */
  getDatasourceCatalog(
    filters: DatasourceCatalogFilters = {},
  ): Observable<DatasourceCatalogResponse> {
    let params = new HttpParams();
    for (const [key, value] of Object.entries(filters)) {
      if (value !== undefined && value !== null && value !== '') {
        params = params.set(key, String(value));
      }
    }
    return this.http.get<DatasourceCatalogResponse>(
      `${this.baseUrl}/datasources/catalog`,
      {params},
    );
  }

  /** Get execution-authorized, scope-matching connectors for an exact project
   * context. The server computes owner-specific `default_selected`; callers
   * must not infer defaults from visibility or `auto_attach` alone. */
  getEligibleDatasources(projectIds?: string[]): Observable<EligibleDatasource[]> {
    let params = new HttpParams();
    for (const pid of projectIds ?? []) {
      if (pid) {
        params = params.append('project_id', pid);
      }
    }
    return this.http
      .get<EligibleDatasource[]>(`${this.baseUrl}/datasources/eligible`, { params });
  }

  /** Projects the caller may use in a connector's availability policy. Current
   * links are included in edit mode even when they are now retained-only. */
  getLinkableDatasourceProjects(options: {
    datasourceId?: string;
    q?: string;
    cursor?: string;
    limit?: number;
  } = {}): Observable<LinkableDatasourceProjectsResponse> {
    let params = new HttpParams();
    if (options.datasourceId) params = params.set('datasource_id', options.datasourceId);
    if (options.q) params = params.set('q', options.q);
    if (options.cursor) params = params.set('cursor', options.cursor);
    params = params.set('limit', String(options.limit ?? 50));
    return this.http.get<LinkableDatasourceProjectsResponse>(
      `${this.baseUrl}/projects/linkable-datasource-targets`,
      {params},
    );
  }

  /** Cursor-paginated, server-authorized connector candidates for widening a
   * target project's links. Unlike execution eligibility, this intentionally
   * includes caller-owned project-scoped connectors not yet linked here. */
  getLinkableProjectDatasources(
    projectId: string,
    filters: LinkableProjectDatasourceFilters = {},
  ): Observable<LinkableProjectDatasourcesResponse> {
    let params = new HttpParams();
    if (filters.q) params = params.set('q', filters.q);
    if (filters.cursor) params = params.set('cursor', filters.cursor);
    params = params.set('limit', String(filters.limit ?? 50));
    return this.http.get<LinkableProjectDatasourcesResponse>(
      `${this.baseUrl}/projects/${projectId}/linkable-datasources`,
      {params},
    );
  }

  /**
   * Get a single datasource by ID.
   */
  getDatasource(id: string): Observable<Datasource | null> {
    return this.http.get<Datasource>(`${this.baseUrl}/datasources/${id}`).pipe(
      catchError((error) => {
        console.error(`Failed to fetch datasource ${id}:`, error);
        return of(null);
      }),
    );
  }

  /**
   * Create a new datasource.
   */
  createDatasource(ds: DatasourceCreateRequest): Observable<Datasource> {
    return this.http.post<Datasource>(`${this.baseUrl}/datasources`, ds);
  }

  /**
   * Update a datasource.
   */
  updateDatasource(id: string, ds: DatasourceUpdateRequest): Observable<Datasource> {
    // Policy and optimistic-concurrency errors must reach the form. Turning a
    // 409/403 into null would erase the actionable server detail.
    return this.http.put<Datasource>(`${this.baseUrl}/datasources/${id}`, ds);
  }

  /**
   * Delete a datasource.
   */
  deleteDatasource(id: string): Observable<{ status: string } | null> {
    return this.http.delete<{ status: string }>(`${this.baseUrl}/datasources/${id}`).pipe(
      catchError((error) => {
        console.error(`Failed to delete datasource ${id}:`, error);
        return of(null);
      }),
    );
  }

  /**
   * Test connectivity to a datasource.
   */
  testDatasource(id: string): Observable<DatasourceTestResult | null> {
    return this.http.post<DatasourceTestResult>(`${this.baseUrl}/datasources/${id}/test`, {}).pipe(
      catchError((error) => {
        console.error(`Failed to test datasource ${id}:`, error);
        return of(null);
      }),
    );
  }

  /** Get credential-free operational index state for an OKF Knowledge Base. */
  getDatasourceIndexStatus(id: string): Observable<DatasourceIndexStatus | null> {
    return this.http
      .get<DatasourceIndexStatus>(`${this.baseUrl}/datasources/${id}/index-status`)
      .pipe(
        catchError((error) => {
          console.error(`Failed to fetch datasource index status ${id}:`, error);
          return of(null);
        }),
      );
  }

  /** Trigger an incremental (or explicitly confirmed full) KB reindex. */
  reindexDatasource(
    id: string,
    full = false,
  ): Observable<DatasourceReindexResult | null> {
    const params = new HttpParams().set('full', String(full));
    return this.http
      .post<DatasourceReindexResult>(
        `${this.baseUrl}/datasources/${id}/reindex`,
        {},
        {params},
      )
      .pipe(
        catchError((error) => {
          console.error(`Failed to reindex datasource ${id}:`, error);
          return of(null);
        }),
      );
  }

  /**
   * Generate a fresh ed25519 SSH keypair for a repository datasource. The
   * caller drops the private key into the form's SSH key textarea and shows
   * the public key to the user so they can register it as a deploy key.
   */
  generateSshKey(comment?: string): Observable<SSHKeyGenerateResponse | null> {
    return this.http
      .post<SSHKeyGenerateResponse>(`${this.baseUrl}/datasources/ssh-keys/generate`, {
        comment: comment ?? null,
      })
      .pipe(
        catchError((error) => {
          console.error('Failed to generate SSH key:', error);
          return of(null);
        }),
      );
  }

  /**
   * Get resolved datasources for a job.
   */
  getJobDatasources(jobId: string): Observable<Datasource[]> {
    return this.http.get<Datasource[]>(`${this.baseUrl}/jobs/${jobId}/datasources`).pipe(
      catchError((error) => {
        console.error(`Failed to fetch datasources for job ${jobId}:`, error);
        return of([]);
      }),
    );
  }

  /** Read live state for the pull request persisted against a job. */
  getJobPullRequestStatus(jobId: string): Observable<PullRequestStatus | null> {
    return this.http
      .get<PullRequestStatus>(`${this.baseUrl}/jobs/${jobId}/pull-request`)
      .pipe(
        catchError((error) => {
          console.error(`Failed to fetch pull request status for job ${jobId}:`, error);
          return of(null);
        }),
      );
  }

  /**
   * Create an interactive review session from an access-checked job.
   *
   * The empty body is deliberate: the server owns all config, scope, connector
   * and branch derivation. Keeping those values out of this client call also
   * keeps them out of every model-authored session-creation surface.
   */
  createJobReviewSession(jobId: string): Observable<JobReviewSessionResult | null> {
    return this.http
      .post<JobReviewSessionResult>(`${this.baseUrl}/jobs/${jobId}/review-session`, {})
      .pipe(
        catchError((error) => {
          console.error(`Failed to create review session for job ${jobId}:`, error);
          return of(null);
        }),
      );
  }

  // ===== Project Datasources (N:M) =====

  /**
   * List datasources linked to a project.
   */
  getProjectDatasources(projectId: string): Observable<ProjectDatasource[]> {
    return this.http.get<ProjectDatasource[]>(`${this.baseUrl}/projects/${projectId}/datasources`).pipe(
      catchError((error) => {
        console.error(`Failed to fetch project datasources:`, error);
        return of([]);
      }),
    );
  }

  /**
   * Link a datasource to a project.
   */
  linkProjectDatasource(
    projectId: string,
    datasourceId: string,
    body: {read_only?: boolean} = {},
  ): Observable<{ status: string } | null> {
    return this.http.post<{ status: string }>(`${this.baseUrl}/projects/${projectId}/datasources/${datasourceId}`, body).pipe(
      catchError((error) => {
        console.error(`Failed to link datasource:`, error);
        return of(null);
      }),
    );
  }

  /**
   * Update project-level settings for a linked datasource.
   */
  updateProjectDatasource(
    projectId: string,
    datasourceId: string,
    body: { read_only?: boolean | null; description?: string | null },
  ): Observable<{ status: string } | null> {
    return this.http.patch<{ status: string }>(`${this.baseUrl}/projects/${projectId}/datasources/${datasourceId}`, body).pipe(
      catchError((error) => {
        console.error(`Failed to update project datasource:`, error);
        return of(null);
      }),
    );
  }

  /**
   * Unlink a datasource from a project.
   */
  unlinkProjectDatasource(projectId: string, datasourceId: string): Observable<{ status: string } | null> {
    return this.http.delete<{ status: string }>(`${this.baseUrl}/projects/${projectId}/datasources/${datasourceId}`).pipe(
      catchError((error) => {
        console.error(`Failed to unlink datasource:`, error);
        return of(null);
      }),
    );
  }

  // ===== File Upload Endpoints =====

  /**
   * Upload files for job creation.
   * @param files Files to upload
   * @returns Observable with upload response containing upload_id
   */
  uploadFiles(files: File[]): Observable<UploadResponse | null> {
    const formData = new FormData();
    files.forEach((file) => formData.append('files', file, file.name));

    return this.http.post<UploadResponse>(`${this.baseUrl}/uploads`, formData).pipe(
      catchError((error) => {
        console.error('Failed to upload files:', error);
        return of(null);
      }),
    );
  }

  /**
   * Upload a config file (YAML) for job creation.
   * Config files override default agent settings.
   * @param file Single YAML config file
   * @returns Observable with upload response containing upload_id
   */
  uploadConfig(file: File): Observable<UploadResponse | null> {
    const formData = new FormData();
    formData.append('files', file, file.name);

    const params = new HttpParams().set('upload_type', 'config');

    return this.http.post<UploadResponse>(`${this.baseUrl}/uploads`, formData, { params }).pipe(
      catchError((error) => {
        console.error('Failed to upload config file:', error);
        return of(null);
      }),
    );
  }

  /**
   * Upload an instructions file (Markdown/Text) for job creation.
   * Instructions files replace the default agent instructions.
   * @param file Single .md or .txt instructions file
   * @returns Observable with upload response containing upload_id
   */
  uploadInstructions(file: File): Observable<UploadResponse | null> {
    const formData = new FormData();
    formData.append('files', file, file.name);

    const params = new HttpParams().set('upload_type', 'instructions');

    return this.http.post<UploadResponse>(`${this.baseUrl}/uploads`, formData, { params }).pipe(
      catchError((error) => {
        console.error('Failed to upload instructions file:', error);
        return of(null);
      }),
    );
  }

  /**
   * Get information about an upload.
   */
  getUploadInfo(uploadId: string): Observable<UploadInfo | null> {
    return this.http.get<UploadInfo>(`${this.baseUrl}/uploads/${uploadId}`).pipe(
      catchError((error) => {
        console.error(`Failed to fetch upload info for ${uploadId}:`, error);
        return of(null);
      }),
    );
  }

  /**
   * Delete an upload and all its files.
   */
  deleteUpload(uploadId: string): Observable<void> {
    return this.http.delete<void>(`${this.baseUrl}/uploads/${uploadId}`).pipe(
      catchError((error) => {
        console.error(`Failed to delete upload ${uploadId}:`, error);
        throw error;
      }),
    );
  }

  /**
   * Push ONE file into the persistent thread's live workspace uploads/ directory.
   *
   * Deliberately one request per file rather than one batched multipart POST.
   * Three reasons, in order of severity:
   *   1. The deployment traverses a Cloudflare Tunnel whose request-body cap is
   *      100MB. A batched send sums every file into that ceiling; per-file keeps
   *      each request under the backend's own 100MB per-file cap.
   *   2. A batch fails atomically from the client's point of view, so one
   *      oversized file failed the whole message.
   *   3. Per-file progress and per-file cancel are not expressible otherwise.
   *
   * Emits `{kind: 'progress'}` as the bytes move, then exactly one
   * `{kind: 'done', files}` — an ARRAY, because a .zip expands into one entry
   * per extracted member (services/thread_uploads.py). Users attach 30-90MB
   * PDFs; without the progress events the bubble can only show an opaque
   * indeterminate label for the whole wait.
   *
   * `reportProgress: true` only does anything on the XHR backend — Angular's
   * FetchBackend emits no UploadProgress events at all — which is why
   * app.config.ts deliberately does NOT install `withFetch()`.
   *
   * Errors are RE-THROWN (not swallowed to `null`) so the caller can read the
   * status and the server-side `detail` field. Use `humanizeUploadError()` to
   * map an arbitrary HttpErrorResponse to a user-facing string.
   */
  uploadOneToThread(threadId: string, file: File): Observable<ThreadUploadEvent> {
    const formData = new FormData();
    formData.append('files', file, file.name);
    return this.http
      .post<ThreadUploadResponse>(
        `${this.baseUrl}/persistent/threads/${threadId}/uploads`,
        formData,
        {reportProgress: true, observe: 'events'},
      )
      .pipe(
        // Sent / ResponseHeader / DownloadProgress carry nothing the send
        // outbox can use, so they never reach the caller.
        filter(
          (e): e is HttpUploadProgressEvent | HttpResponse<ThreadUploadResponse> =>
            e.type === HttpEventType.UploadProgress || e.type === HttpEventType.Response,
        ),
        map((e) =>
          e.type === HttpEventType.UploadProgress
            ? // `total` is optional on the DOM event and absent whenever the
              // body length is not computable. Normalise to null here so no
              // consumer has to remember that it might be undefined.
              {kind: 'progress' as const, loaded: e.loaded, total: e.total ?? null}
            : {kind: 'done' as const, files: e.body?.files ?? []},
        ),
        catchError((error: HttpErrorResponse) => {
          console.error(`Failed to upload ${file.name} to thread ${threadId}:`, error);
          return throwError(() => error);
        }),
      );
  }

  /**
   * Remove one file (or one zip's extracted subtree) from a persistent
   * thread's `uploads/` directory.
   *
   * `name` is the `name` field of a `ThreadUploadedFile` — the path RELATIVE to
   * `uploads/` (`report.pdf`, `bundle/sub/a.txt`). Passing its `path` field
   * instead resolves to `uploads/uploads/…` server-side and 404s.
   *
   * Exists so eager upload (knowledge-base/knowledge/features/session_attachment_send_flow.md
   * §5.4) can be cancelled honestly: removing an attachment chip can arrive
   * after the bytes have already landed, and without this the "cancel" would
   * be a lie that also litters a directory the agent can list.
   */
  deleteThreadUpload(threadId: string, name: string): Observable<void> {
    return this.http
      .delete<{deleted: boolean}>(
        `${this.baseUrl}/persistent/threads/${threadId}/uploads/${encodeUploadPath(name)}`,
      )
      .pipe(
        map(() => undefined),
        catchError((error: HttpErrorResponse) => {
          console.error(`Failed to delete upload ${name} from thread ${threadId}:`, error);
          return throwError(() => error);
        }),
      );
  }

  /** Map an upload HttpErrorResponse to a user-facing message. */
  humanizeUploadError(error: unknown): string {
    if (error instanceof HttpErrorResponse) {
      const detail = (error.error && (error.error as {detail?: unknown}).detail) as
        | string
        | undefined;
      if (typeof detail === 'string' && detail.trim()) return detail;
      if (error.status === 0) return 'Network error — check your connection';
      if (error.status === 413) return 'File too large';
      return `Upload failed (HTTP ${error.status}) — try again`;
    }
    return 'Upload failed — try again';
  }

  /**
   * Generate TTS audio for a chat message in a persistent thread.
   *
   * Returns:
   *   - the MP3 blob on success,
   *   - `null` on transport error,
   *   - `'unavailable'` when the server returns 204 (no TTS model
   *     configured) — distinguishes the "feature disabled" case from a
   *     real failure so the UI can hide rather than show an error.
   */
  generateTTS(
    threadId: string,
    content: string,
    options: {reformulate?: boolean; language?: string} = {},
  ): Observable<{text: string; audio: Blob} | 'unavailable' | {errorCode: string} | null> {
    // The endpoint returns JSON {text, audio} where `text` is the spoken
    // (formulation-rewritten) version actually read aloud and `audio` is the
    // base64-encoded MP3. 204 → no TTS model configured ('unavailable');
    // an *actionable* failure (paid-plan / auth / rate-limit) → {errorCode} so
    // the caller shows a specific message and doesn't pointlessly retry; any
    // other/transient error → null.
    return this.http
      .post<{text: string; audio: string}>(
        `${this.baseUrl}/persistent/threads/${threadId}/tts`,
        {
          content,
          reformulate: options.reformulate ?? true,
          language: options.language ?? 'en',
        },
        {observe: 'response'},
      )
      .pipe(
        map((resp) => {
          if (resp.status === 204 || !resp.body) return 'unavailable' as const;
          return {
            text: resp.body.text ?? '',
            audio: this.decodeBase64ToBlob(resp.body.audio, 'audio/mpeg'),
          };
        }),
        catchError((error) => {
          console.error(`Failed to generate TTS for thread ${threadId}:`, error);
          const code = this.ttsErrorCode(error);
          return of(code ? ({errorCode: code} as const) : null);
        }),
      );
  }

  /** Extract an actionable TTS failure code from an HTTP error. The synth
   * endpoints return `{detail: {code, message}}`; fall back to the status for
   * the two safe codes (402 payment, 429 rate-limit). Returns null for
   * transient/unknown errors (the caller retries those). */
  private ttsErrorCode(error: unknown): string | null {
    const e = error as {status?: number; error?: {detail?: {code?: string}}};
    const code = e?.error?.detail?.code;
    if (code === 'payment_required' || code === 'auth' || code === 'rate_limit') {
      return code;
    }
    if (e?.status === 402) return 'payment_required';
    if (e?.status === 429) return 'rate_limit';
    return null;
  }

  /**
   * Synthesize a short canned phrase in a candidate voice, for the settings
   * voice picker's "preview" button. `voice` may be `''` (Auto — resolved
   * server-side like normal read-aloud). Returns the MP3 blob, `'unavailable'`
   * (204 — no TTS model configured), or `null` on error.
   */
  previewTTSVoice(
    voice: string,
    language = 'en',
    text?: string,
  ): Observable<Blob | 'unavailable' | {errorCode: string} | null> {
    // `text` (optional) auditions the voice on the user's own words; omitted or
    // blank falls back to the server's canned phrase. Only sent when non-empty
    // so the request body stays identical to before when unused. An actionable
    // failure (paid-plan / auth / rate-limit) → {errorCode} so the picker can
    // say "this voice needs a paid plan"; other errors → null.
    const body: {voice: string; language: string; text?: string} = {
      voice,
      language,
    };
    const trimmed = (text ?? '').trim();
    if (trimmed) body.text = trimmed;
    return this.http
      .post<{audio: string}>(
        `${this.baseUrl}/settings/tts/preview`,
        body,
        {observe: 'response'},
      )
      .pipe(
        map((resp) => {
          if (resp.status === 204 || !resp.body) return 'unavailable' as const;
          return this.decodeBase64ToBlob(resp.body.audio, 'audio/mpeg');
        }),
        catchError((error) => {
          console.error('Failed to preview TTS voice:', error);
          const code = this.ttsErrorCode(error);
          return of(code ? ({errorCode: code} as const) : null);
        }),
      );
  }

  /**
   * List the voices the caller's configured TTS backend offers, for the
   * Settings read-aloud picker. Only ElevenLabs returns a populated list
   * (server-fed from the deployment account with accent labels + hosted
   * previews); Kokoro/OpenAI return `[]` (the cockpit holds their static
   * catalogs locally). Never rejects — a failure degrades to
   * `{backend: null, voices: []}` so Settings still renders.
   */
  listTtsVoices(): Observable<TtsVoicesResponse> {
    return this.http
      .get<TtsVoicesResponse>(`${this.baseUrl}/settings/tts/voices`)
      .pipe(
        catchError((error) => {
          console.error('Failed to list TTS voices:', error);
          return of({backend: null, voices: []} as TtsVoicesResponse);
        }),
      );
  }

  /**
   * Search the ElevenLabs community Voice Library (server-proxied, read-only).
   * All filters optional; `search` alone covers the "french english" case.
   * `add_enabled` in the response mirrors the admin add-gate. Never rejects —
   * a failure degrades to an empty list with a readable `error`.
   */
  searchTtsLibrary(
    filters: TtsLibraryFilters = {},
  ): Observable<TtsLibraryResponse> {
    let params = new HttpParams();
    for (const [k, v] of Object.entries(filters)) {
      if (v !== undefined && v !== null && `${v}`.trim() !== '') {
        params = params.set(k, `${v}`);
      }
    }
    return this.http
      .get<TtsLibraryResponse>(`${this.baseUrl}/settings/tts/library`, {params})
      .pipe(
        catchError((error) => {
          console.error('Failed to search TTS voice library:', error);
          return of({
            backend: null,
            voices: [],
            has_more: false,
            error: 'Voice library search failed.',
            add_enabled: false,
          } as TtsLibraryResponse);
        }),
      );
  }

  /**
   * Copy a Library voice into the deployment ElevenLabs account. Behind the
   * admin add-gate server-side. Errors propagate (unlike the resilient search)
   * so the caller can surface the server's readable detail — most importantly
   * the account's voice-slot limit.
   */
  addTtsLibraryVoice(body: {
    public_owner_id: string;
    voice_id: string;
    new_name: string;
  }): Observable<{voice_id: string; name: string}> {
    return this.http.post<{voice_id: string; name: string}>(
      `${this.baseUrl}/settings/tts/library/add`,
      body,
    );
  }

  /** Read the Voice Library add gate (admin-only). Degrades to disabled. */
  getTtsLibrarySetting(): Observable<TtsLibrarySetting> {
    return this.http
      .get<TtsLibrarySetting>(`${this.baseUrl}/admin/system-settings/tts_library`)
      .pipe(
        catchError(() =>
          of({enabled: false, updated_at: null, updated_by: null} as TtsLibrarySetting),
        ),
      );
  }

  /** Toggle the Voice Library add gate (admin-only). */
  setTtsLibrarySetting(enabled: boolean): Observable<TtsLibrarySetting> {
    return this.http.put<TtsLibrarySetting>(
      `${this.baseUrl}/admin/system-settings/tts_library`,
      {enabled},
    );
  }

  /**
   * Plan a (possibly long) message into ordered, speakable chunks for
   * sequential synthesis + playback. Returns `{chunks, rewritten}` — `rewritten`
   * is `false` when the auxiliary LLM was unavailable and the raw markdown was
   * split deterministically, so the UI can say "rewriting skipped". Returns
   * `'unavailable'` (204 — no TTS model configured), or `null` on error.
   */
  planTTS(
    threadId: string,
    content: string,
    options: {reformulate?: boolean} = {},
  ): Observable<{chunks: string[]; rewritten: boolean} | 'unavailable' | null> {
    // `reformulate: false` is the "read it as-is" bailout — skip the aux LLM and
    // get the markdown-stripped deterministic split immediately.
    const body: {content: string; reformulate?: boolean} = {content};
    if (options.reformulate === false) body.reformulate = false;
    return this.http
      .post<{chunks: string[]; rewritten: boolean}>(
        `${this.baseUrl}/persistent/threads/${threadId}/tts/plan`,
        body,
        {observe: 'response'},
      )
      .pipe(
        map((resp) =>
          resp.status === 204 || !resp.body
            ? ('unavailable' as const)
            : {chunks: resp.body.chunks ?? [], rewritten: resp.body.rewritten ?? false},
        ),
        catchError((error) => {
          console.error(`Failed to plan TTS for thread ${threadId}:`, error);
          return of(null);
        }),
      );
  }

  /**
   * Stream the read-aloud chunk plan over SSE, yielding each speakable chunk the
   * moment the auxiliary LLM produces it — so the caller can synthesize + start
   * playing chunk 1 while the rest still generate (time-to-first-audio ≈
   * first-chunk latency, not whole-message latency). Consumed with `for await`.
   *
   * Uses a raw `fetch` (not HttpClient) because the endpoint streams and the body
   * is a POST; it therefore replicates what `authInterceptor` does — cookie auth
   * via `credentials: 'include'` plus the `X-CSRF` header — and bypasses the
   * service worker (`ngsw-bypass`) so the never-ending stream isn't cached.
   * Pass an `AbortSignal` to cancel (used by the component's cancel/bailout).
   */
  async *streamTTSPlan(
    threadId: string,
    content: string,
    signal?: AbortSignal,
  ): AsyncGenerator<
    | {type: 'chunk'; index: number; text: string; rewritten: boolean}
    | {type: 'done'; total: number; rewritten: boolean}
    | {type: 'unavailable'}
    | {type: 'error'; message: string}
  > {
    const url =
      `${this.baseUrl}/persistent/threads/${threadId}/tts/plan/stream` +
      `?ngsw-bypass=true`;
    let response: Response;
    try {
      response = await fetch(url, {
        method: 'POST',
        headers: {'Content-Type': 'application/json', 'X-CSRF': '1'},
        body: JSON.stringify({content}),
        credentials: 'include',
        signal,
      });
    } catch (error) {
      yield {type: 'error', message: `${error}`};
      return;
    }
    if (!response.ok || !response.body) {
      yield {type: 'error', message: `HTTP ${response.status}`};
      return;
    }

    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';
    let currentEvent = '';
    try {
      while (true) {
        const {done, value} = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, {stream: true});
        const lines = buffer.split('\n');
        buffer = lines.pop() ?? ''; // keep the incomplete trailing line
        for (const line of lines) {
          if (line.startsWith('event: ')) {
            currentEvent = line.slice(7).trim();
          } else if (line.startsWith('data: ')) {
            let data: {
              index?: number;
              text?: string;
              rewritten?: boolean;
              total?: number;
              message?: string;
            } = {};
            try {
              data = JSON.parse(line.slice(6));
            } catch {
              data = {};
            }
            if (currentEvent === 'chunk') {
              yield {
                type: 'chunk',
                index: data.index ?? 0,
                text: data.text ?? '',
                rewritten: data.rewritten ?? false,
              };
            } else if (currentEvent === 'done') {
              yield {
                type: 'done',
                total: data.total ?? 0,
                rewritten: data.rewritten ?? false,
              };
            } else if (currentEvent === 'unavailable') {
              yield {type: 'unavailable'};
            } else if (currentEvent === 'error') {
              yield {type: 'error', message: data.message ?? 'stream error'};
            }
            currentEvent = '';
          } else if (line.trim() === '') {
            currentEvent = ''; // event boundary; `: comment` lines are ignored
          }
        }
      }
    } finally {
      try {
        reader.releaseLock();
      } catch {
        /* stream already errored/closed */
      }
    }
  }

  /** Decode a base64 string into a typed Blob (used for TTS MP3 payloads). */
  private decodeBase64ToBlob(base64: string, mime: string): Blob {
    const binary = atob(base64 ?? '');
    const bytes = new Uint8Array(binary.length);
    for (let i = 0; i < binary.length; i++) {
      bytes[i] = binary.charCodeAt(i);
    }
    return new Blob([bytes], {type: mime});
  }

  /**
   * Transcribe a recorded voice message to text (speech-to-text).
   *
   * Returns:
   *   - `{text}` on success,
   *   - `'unavailable'` when the server returns 204 (no STT model configured) —
   *     lets the caller fall back to attaching the audio silently,
   *   - `null` on transport error (caller surfaces a notice, still attaches audio).
   */
  transcribeVoice(
    threadId: string,
    file: Blob,
  ): Observable<{text: string} | 'unavailable' | null> {
    const formData = new FormData();
    const filename = file instanceof File ? file.name : 'voice.webm';
    formData.append('audio', file, filename);
    return this.http
      .post<{text: string}>(
        `${this.baseUrl}/persistent/threads/${threadId}/transcribe`,
        formData,
        {observe: 'response'},
      )
      .pipe(
        map((resp) =>
          resp.status === 204
            ? ('unavailable' as const)
            : (resp.body as {text: string}),
        ),
        catchError((error) => {
          console.error(`Failed to transcribe audio for thread ${threadId}:`, error);
          return of(null);
        }),
      );
  }

  // ===== Job Management Endpoints =====

  /**
   * Create a new job.
   */
  /**
   * Create a job. Errors PROPAGATE — the create form keeps itself open and
   * renders the server's reason inline so a rejected config can be corrected
   * without losing the rest of the form. Swallowing it into `null` here made
   * the component's error branch dead code and reduced every rejection to
   * "Failed to create job. Please try again."
   */
  createJob(job: JobCreateRequest): Observable<Job> {
    return this.http.post<Job>(`${this.baseUrl}/jobs`, job).pipe(
      tap(() => this.toast.success(this.t('toasts.jobs.created'))),
    );
  }

  /**
   * Get a single job by ID.
   */
  getJob(jobId: string): Observable<Job | null> {
    return this.http.get<Job>(`${this.baseUrl}/jobs/${jobId}`).pipe(
      catchError((error) => {
        console.error(`Failed to fetch job ${jobId}:`, error);
        return of(null);
      }),
    );
  }

  /**
   * Delete a job.
   */
  deleteJob(jobId: string): Observable<{ status: string } | null> {
    return this.http.delete<{ status: string }>(`${this.baseUrl}/jobs/${jobId}`).pipe(
      tap(() => this.toast.success(this.t('toasts.jobs.deleted'))),
      catchError((error) => {
        console.error(`Failed to delete job ${jobId}:`, error);
        this.toast.danger(this.errors.translate(error, 'errors.jobs.deleteFailed'));
        return of(null);
      }),
    );
  }

  /**
   * Cancel a running job.
   */
  cancelJob(jobId: string): Observable<{ status: string } | null> {
    return this.http.put<{ status: string }>(`${this.baseUrl}/jobs/${jobId}/cancel`, {}).pipe(
      tap(() => this.toast.success(this.t('toasts.jobs.cancelled'))),
      catchError((error) => {
        console.error(`Failed to cancel job ${jobId}:`, error);
        this.toast.danger(this.errors.translate(error, 'errors.jobs.cancelFailed'));
        return of(null);
      }),
    );
  }

  /**
   * Pause a running job. The agent will cooperatively pause at the next safe point.
   */
  pauseJob(jobId: string): Observable<{ status: string } | null> {
    return this.http.put<{ status: string }>(`${this.baseUrl}/jobs/${jobId}/pause`, {}).pipe(
      tap(() => this.toast.success(this.t('toasts.jobs.paused'))),
      catchError((error) => {
        console.error(`Failed to pause job ${jobId}:`, error);
        this.toast.danger(this.errors.translate(error, 'errors.jobs.pauseFailed'));
        return of(null);
      }),
    );
  }

  /**
   * Resume a failed job from its checkpoint.
   * @param jobId The job ID to resume
   * @param feedback Optional feedback to inject before resuming
   * @param agentId Optional agent ID override if original agent is offline
   */
  resumeJob(
    jobId: string,
    feedback?: string,
    agentId?: string,
  ): Observable<{ status: string; job_id: string; agent_id: string } | null> {
    const body: { feedback?: string; agent_id?: string } = {};
    if (feedback) body.feedback = feedback;
    if (agentId) body.agent_id = agentId;

    return this.http
      .post<{ status: string; job_id: string; agent_id: string }>(
        `${this.baseUrl}/jobs/${jobId}/resume`,
        body,
      )
      .pipe(
        tap(() => this.toast.success(this.t('toasts.jobs.resumed'))),
        catchError((error) => {
          console.error(`Failed to resume job ${jobId}:`, error);
          this.toast.danger(this.errors.translate(error, 'errors.jobs.resumeFailed'));
          return of(null);
        }),
      );
  }

  retryWorkspaceRecovery(
    jobId: string,
    operationId: string,
    requestId: string = crypto.randomUUID(),
  ): Observable<{
    status: string;
    operation_id: string;
    supersedes_operation_id: string;
    deadline_at: string;
  } | null> {
    return this.http
      .post<{
        status: string;
        operation_id: string;
        supersedes_operation_id: string;
        deadline_at: string;
      }>(`${this.baseUrl}/jobs/${jobId}/workspace-recovery/retry`, {
        operation_id: operationId,
        request_id: requestId,
      })
      .pipe(
        tap(() => this.toast.success(this.t('toasts.jobs.recoveryRetry'))),
        catchError((error) => {
          console.error(`Failed to retry workspace recovery for ${jobId}:`, error);
          this.toast.danger(this.errors.translate(error, 'errors.jobs.recoveryRetryFailed'));
          return of(null);
        }),
      );
  }

  /**
   * Get IDE session status for a job.
   */
  getIdeSession(jobId: string): Observable<IdeSessionStatus | null> {
    return this.http
      .get<IdeSessionStatus>(`${this.baseUrl}/jobs/${jobId}/ide`)
      .pipe(
        catchError((error) => {
          console.error(`Failed to get IDE session for ${jobId}:`, error);
          return of(null);
        }),
      );
  }

  /**
   * Ensure the current user has Gitea access to a job's workspace repo.
   * Called before navigating to the workspace URL.
   */
  ensureWorkspaceAccess(jobId: string): Observable<{ granted: boolean } | null> {
    return this.http
      .post<{ granted: boolean }>(`${this.baseUrl}/jobs/${jobId}/ensure-workspace-access`, {})
      .pipe(
        catchError((error) => {
          console.error(`Failed to ensure workspace access for ${jobId}:`, error);
          return of(null);
        }),
      );
  }

  /**
   * Start an IDE session for a job (restores snapshot into a fresh VM).
   */
  startIdeSession(jobId: string): Observable<IdeSessionStatus | null> {
    return this.http
      .post<IdeSessionStatus>(`${this.baseUrl}/jobs/${jobId}/ide`, {})
      .pipe(
        catchError((error) => {
          console.error(`Failed to start IDE session for ${jobId}:`, error);
          this.toast.danger(this.errors.translate(error, 'errors.ide.startFailed'));
          return of(null);
        }),
      );
  }

  /**
   * Stop an active IDE session.
   */
  stopIdeSession(jobId: string): Observable<{ status: string } | null> {
    return this.http
      .delete<{ status: string }>(`${this.baseUrl}/jobs/${jobId}/ide`)
      .pipe(
        tap(() => this.toast.success(this.t('toasts.ide.stopped'))),
        catchError((error) => {
          console.error(`Failed to stop IDE session for ${jobId}:`, error);
          this.toast.danger(this.errors.translate(error, 'errors.ide.stopFailed'));
          return of(null);
        }),
      );
  }

    /**
     * Get a persistent thread's detail row (title, metadata incl. the
     * redacted `config_override` + `datasource_ids`). Used by the live
     * settings pane to prefill current overrides.
     */
    getPersistentThread(threadId: string): Observable<Record<string, unknown> | null> {
        return this.http
            .get<Record<string, unknown>>(`${this.baseUrl}/persistent/threads/${threadId}`)
            .pipe(
                catchError((error) => {
                    console.error(`Failed to get thread ${threadId}:`, error);
                    return of(null);
                }),
            );
    }

    /** Read a persistent thread's durable transcript without attaching its
     * runtime transport. This is the only history path used for child agents. */
    getPersistentThreadHistory(threadId: string): Observable<PersistentThreadHistory | null> {
        return this.http
            .get<PersistentThreadHistory>(
                `${this.baseUrl}/persistent/threads/${threadId}/messages`,
            )
            .pipe(
                catchError((error) => {
                    console.error(`Failed to get transcript for thread ${threadId}:`, error);
                    return of(null);
                }),
            );
    }

    /**
     * The session's resolved toolset: what the running agent actually bound,
     * or a labelled prediction when there is no agent to ask.
     *
     * Returns the WHOLE response, not just `tool_groups`. The four booleans
     * were all the pane could render and all the transport carried, and that
     * is why the live surface showed four of twenty-five categories with no
     * way to say why the other twenty-one were missing.
     *
     * Null on any failure — including a 404 from an orchestrator that predates
     * the endpoint — and the caller then renders its static list in two
     * states. Deliberately silent (no toast): a missing resolved answer
     * degrades the surface, it does not break it.
     */
    getSessionToolGroups(threadId: string): Observable<SessionToolGroupsResponse | null> {
        return this.http
            .get<SessionToolGroupsResponse>(
                `${this.baseUrl}/persistent/threads/${threadId}/tool-groups`,
            )
            .pipe(
                // The server now probes the session pod for its ACTUAL bound
                // toolset, so this request can take seconds against a hung
                // agent. `loadThread` forkJoins it and anchors `lastApplied`
                // only once both arms settle — with no deadline, a pod that
                // never replies leaves the baseline unanchored for the life of
                // the pane and every subsequent edit is silently swallowed.
                // Must exceed the server's own probe budget (3s) so a slow-but-
                // answering agent still wins; the client is the backstop, not
                // the primary bound.
                timeout(SESSION_TOOL_GROUPS_TIMEOUT_MS),
                map((response) => response ?? null),
                catchError((error) => {
                    console.error(`Failed to get tool groups for thread ${threadId}:`, error);
                    return of(null);
                }),
            );
    }

    /**
     * What WOULD a session created with this config bind? Always a prediction.
     *
     * A separate route rather than a mode of the thread read, so the creation
     * form's limitation is structural: there is no agent yet, so this can
     * never return `origin: "agent"`. A forecast rendered as fact is the
     * defect this whole change exists to remove, and the cheapest way to keep
     * that honest is to make it impossible to get a measurement here.
     *
     * Same deadline and same silent-null contract as the thread read.
     */
    previewToolGroups(body: {
        workspace?: Record<string, unknown> | null;
        workspace_preference?: 'none' | 'virtual' | 'sandbox' | 'vm' | null;
        config_name?: string | null;
        expert_id?: string | null;
        project_id?: string | null;
        config_override?: Record<string, unknown> | null;
        /** Which surface is asking. Omitted = `session`, so existing callers
         *  keep their meaning; job create must send `worker` or it gets a
         *  session's prediction (different base, different code floors). */
        expert_type?: 'worker' | 'session';
    }): Observable<SessionToolGroupsResponse | null> {
        return this.http
            .post<SessionToolGroupsResponse>(
                `${this.baseUrl}/persistent/tool-groups/preview`,
                body,
            )
            .pipe(
                timeout(SESSION_TOOL_GROUPS_TIMEOUT_MS),
                map((response) => response ?? null),
                catchError((error) => {
                    console.error('Failed to preview tool groups:', error);
                    return of(null);
                }),
            );
    }

    /**
     * Get IDE status for a persistent thread's workspace.
     */
    getThreadIdeStatus(threadId: string): Observable<IdeSessionStatus | null> {
        return this.http
            .get<IdeSessionStatus>(`${this.baseUrl}/persistent/threads/${threadId}/ide`)
            .pipe(
                catchError((error) => {
                    console.error(`Failed to get IDE status for thread ${threadId}:`, error);
                    return of(null);
                }),
            );
    }

  /**
   * Get snapshot storage statistics (total count, size, GC pending).
   */
  getSnapshotStats(): Observable<SnapshotStorageStats | null> {
    return this.http
      .get<SnapshotStorageStats>(`${this.baseUrl}/snapshots/stats`)
      .pipe(
        catchError((error) => {
          console.error('Failed to get snapshot stats:', error);
          return of(null);
        }),
      );
  }

  /**
   * Toggle pin on a job's snapshot (GC exemption).
   */
  toggleSnapshotPin(jobId: string): Observable<{ pinned: boolean } | null> {
    return this.http
      .put<{ pinned: boolean }>(`${this.baseUrl}/jobs/${jobId}/snapshot/pin`, {})
      .pipe(
        catchError((error) => {
          console.error(`Failed to toggle snapshot pin for ${jobId}:`, error);
          return of(null);
        }),
      );
  }

  /**
   * Approve a frozen job (pending_review → completed).
   * @param jobId The job ID to approve
   * @param notes Optional reviewer notes
   */
  approveJob(
    jobId: string,
    notes?: string,
  ): Observable<{ status: string; job_id: string; summary: string; deliverables: string[] } | null> {
    const body: { notes?: string } = {};
    if (notes) body.notes = notes;

    return this.http
      .post<{ status: string; job_id: string; summary: string; deliverables: string[] }>(
        `${this.baseUrl}/jobs/${jobId}/approve`,
        body,
      )
      .pipe(
        tap(() => this.toast.success(this.t('toasts.jobs.approved'))),
        catchError((error) => {
          console.error(`Failed to approve job ${jobId}:`, error);
          this.toast.danger(this.errors.translate(error, 'errors.jobs.approveFailed'));
          return of(null);
        }),
      );
  }

  /**
   * Upgrade a frozen job from container workspace to a VM.
   * Called when a job freezes with freeze_type: vm_upgrade_required.
   */
  upgradeJobToVm(
    jobId: string,
  ): Observable<{ status: string; job_id: string; vm_provisioner_mode: string } | null> {
    return this.http
      .post<{ status: string; job_id: string; vm_provisioner_mode: string }>(
        `${this.baseUrl}/jobs/${jobId}/upgrade-to-vm`,
        {},
      )
      .pipe(
        tap(() => this.toast.success(this.t('toasts.jobs.vmUpgradeStarted'))),
        catchError((error) => {
          console.error(`Failed to upgrade job ${jobId} to VM:`, error);
          this.toast.danger(this.errors.translate(error, 'errors.jobs.upgradeVmFailed'));
          return of(null);
        }),
      );
  }

  /**
   * Get frozen job data (job_frozen.json) for a pending_review job.
   */
  getFrozenJobData(jobId: string): Observable<Record<string, unknown> | null> {
    return this.http.get<Record<string, unknown>>(`${this.baseUrl}/jobs/${jobId}/frozen`).pipe(
      catchError((error) => {
        console.error(`Failed to fetch frozen data for job ${jobId}:`, error);
        return of(null);
      }),
    );
  }

  /**
   * Get full thread messages for a job's message thread.
   */
  getThreadMessages(jobId: string, threadId: string): Observable<ThreadDetail | null> {
    return this.http
      .get<ThreadDetail>(`${this.baseUrl}/jobs/${jobId}/messages/${threadId}`)
      .pipe(
        catchError((error) => {
          console.error(`Failed to fetch thread ${threadId} for job ${jobId}:`, error);
          return of(null);
        }),
      );
  }

  /**
   * Reply to an agent message thread.
   */
  replyToThread(
    jobId: string,
    threadId: string,
    message: string,
    urgent = false,
  ): Observable<{ status: string; sequence: number } | null> {
    return this.http
      .post<{ status: string; sequence: number }>(
        `${this.baseUrl}/jobs/${jobId}/messages/${threadId}/reply`,
        { message, urgent },
      )
      .pipe(
        tap(() => this.toast.success(this.t('toasts.jobs.replySent'))),
        catchError((error) => {
          console.error(`Failed to reply to thread ${threadId}:`, error);
          this.toast.danger(this.errors.translate(error, 'errors.jobs.replyFailed'));
          return of(null);
        }),
      );
  }

  /**
   * Assign a job to an agent.
   */
  assignJob(jobId: string, agentId: string): Observable<{ status: string; agent_id: string; job_id: string } | null> {
    return this.http
      .post<{ status: string; agent_id: string; job_id: string }>(
        `${this.baseUrl}/jobs/${jobId}/assign/${agentId}`,
        {},
      )
      .pipe(
        catchError((error) => {
          console.error(`Failed to assign job ${jobId} to agent ${agentId}:`, error);
          return of(null);
        }),
      );
  }

  /**
   * Get honest job liveness (state/reasons/last_activity_at). The
   * `progress_percent`/`eta_seconds` fields are kept for shape compatibility
   * and are `null` — render the liveness state, never a fabricated percent.
   */
  getJobProgress(jobId: string): Observable<JobProgress | null> {
    return this.http.get<JobProgress>(`${this.baseUrl}/jobs/${jobId}/progress`).pipe(
      catchError((error) => {
        console.error(`Failed to fetch progress for job ${jobId}:`, error);
        return of(null);
      }),
    );
  }

  /**
   * Metered tokens and cost for one job.
   *
   * Deliberately not `/api/usage?ref_id=` — that endpoint takes its window from
   * a caller-supplied `days`, and a window that misses the job returns
   * `total_cost_usd: 0` with `available: true`. This one derives the window from
   * the job itself. `cost.usd` is null when nothing was priced, which the UI must
   * render as unknown rather than $0.00.
   *
   * @param includeSubjobs sum the job and every descendant. Not cosmetic — a
   *   parent's own spend can be a fraction of its tree's.
   */
  getJobUsage(jobId: string, includeSubjobs = false): Observable<JobUsage | null> {
    const params = includeSubjobs ? '?include_subjobs=true' : '';
    return this.http.get<JobUsage>(`${this.baseUrl}/jobs/${jobId}/usage${params}`).pipe(
      catchError((error) => {
        console.error(`Failed to fetch usage for job ${jobId}:`, error);
        return of(null);
      }),
    );
  }

  /**
   * The subjob roster for one job — what it spawned and where each stands.
   *
   * Independent of the jobs list's filters by construction: the server walks
   * the tree rather than re-running the list query. That is the whole point —
   * under the default `origin` filter a subjob is never in the list's matched
   * set, so a `waiting` parent renders with no children and its status reads as
   * stalled work when a child is in fact running.
   *
   * Degrades to null like the rest of the panel's sources, so a failure here
   * costs the roster and nothing else.
   */
  getJobSubjobs(jobId: string): Observable<JobSubjobRoster | null> {
    return this.http.get<JobSubjobRoster>(`${this.baseUrl}/jobs/${jobId}/subjobs`).pipe(
      catchError((error) => {
        console.error(`Failed to fetch subjobs for job ${jobId}:`, error);
        return of(null);
      }),
    );
  }

  /**
   * The in-process child agents a job ran, including their transcript ids.
   * Degrades independently so a roster outage never blanks the rest of the
   * job detail panel.
   */
  getJobSubagents(jobId: string): Observable<JobSubagentRoster | null> {
    return this.http.get<JobSubagentRoster>(`${this.baseUrl}/jobs/${jobId}/subagents`).pipe(
      catchError((error) => {
        console.error(`Failed to fetch subagents for job ${jobId}:`, error);
        return of(null);
      }),
    );
  }

  // ===== Repo Browser Endpoints (Gitea proxy) =====

  /**
   * List directory contents from a job's Gitea repository.
   * @param jobId Job ID
   * @param path Directory path within the repo (empty for root)
   */
  listRepoContents(
    jobId: string,
    path: string = '',
  ): Observable<{ name: string; path: string; type: string; size: number }[]> {
    const params = path ? new HttpParams().set('path', path) : new HttpParams();
    return this.http
      .get<{ name: string; path: string; type: string; size: number }[]>(
        `${this.baseUrl}/jobs/${jobId}/repo/contents`,
        { params },
      )
      .pipe(
        catchError((error) => {
          console.error(`Failed to list repo contents for job ${jobId} path=${path}:`, error);
          return of([]);
        }),
      );
  }

  /**
   * Get file content from a job's Gitea repository.
   * @param jobId Job ID
   * @param path File path within the repo
   */
  getRepoFile(jobId: string, path: string): Observable<{ path: string; content: string; size: number } | null> {
    const params = new HttpParams().set('path', path);
    return this.http
      .get<{ path: string; content: string; size: number }>(`${this.baseUrl}/jobs/${jobId}/repo/file`, { params })
      .pipe(
        catchError((error) => {
          console.error(`Failed to fetch repo file ${path} for job ${jobId}:`, error);
          return of(null);
        }),
      );
  }

  // ===== Statistics Endpoints =====

  /**
   * Get overall job statistics.
   */
  /**
   * Status-chip counts under the list's filters.
   *
   * Deliberately drops `status`: these are disjunctive facet counts, so a
   * facet's own filter must not narrow it, or selecting one status drops
   * every other chip to zero.
   */
  getJobStatisticsFiltered(query: JobListParams = {}): Observable<JobStatistics | null> {
    const {status: _status, limit: _limit, offset: _offset, include_total: _t, ...rest} = query;
    return this.http
      .get<JobStatistics>(`${this.baseUrl}/stats/jobs`, {params: toHttpParams(rest)})
      .pipe(catchError(() => of(null)));
  }

  getJobStatistics(): Observable<JobStatistics | null> {
    return this.http.get<JobStatistics>(`${this.baseUrl}/stats/jobs`).pipe(
      catchError((error) => {
        console.error('Failed to fetch job statistics:', error);
        return of(null);
      }),
    );
  }

  /**
   * Get daily job statistics.
   */
  getDailyStatistics(days: number = 7): Observable<DailyStatistics[]> {
    const params = new HttpParams().set('days', days.toString());

    return this.http.get<DailyStatistics[]>(`${this.baseUrl}/stats/daily`, { params }).pipe(
      catchError((error) => {
        console.error('Failed to fetch daily statistics:', error);
        return of([]);
      }),
    );
  }

  /**
   * Get agent workforce summary.
   */
  getAgentStatistics(): Observable<AgentStatistics | null> {
    return this.http.get<AgentStatistics>(`${this.baseUrl}/stats/agents`).pipe(
      catchError((error) => {
        console.error('Failed to fetch agent statistics:', error);
        return of(null);
      }),
    );
  }

  /**
   * Get stuck jobs.
   */
  getStuckJobs(thresholdMinutes?: number): Observable<StuckJobsResponse> {
    let params = new HttpParams();
    if (thresholdMinutes !== undefined) {
      params = params.set('threshold_minutes', thresholdMinutes.toString());
    }

    return this.http.get<StuckJobsResponse>(`${this.baseUrl}/stats/stuck`, { params }).pipe(
      catchError((error) => {
        console.error('Failed to fetch stuck jobs:', error);
        return of<StuckJobsResponse>({
          jobs: [],
          threshold_minutes: null,
          threshold_source: 'unavailable',
        });
      }),
    );
  }

  // ===== Project Endpoints =====

  /**
   * `status` is repeatable and the server defaults it to `active`, so an
   * omitted argument no longer means "every project" — archived ones are
   * excluded unless asked for. Pass `['active', 'archived']` for both.
   *
   * Errors propagate. The old blanket `catchError(() => of([]))` turned every
   * failure — including a refusal the user needs to read — into a believable
   * "you have no projects", which is the worst possible lie for a list that
   * decides where work goes.
   */
  getProjects(userId?: string, status?: ProjectStatus[]): Observable<Project[]> {
    let params = new HttpParams();
    if (userId) params = params.set('user_id', userId);
    for (const value of status ?? []) params = params.append('status', value);
    return this.http.get<Project[]>(`${this.baseUrl}/projects`, { params });
  }

  getProject(id: string): Observable<Project | null> {
    return this.http.get<Project>(`${this.baseUrl}/projects/${id}`).pipe(
      catchError(() => of(null)),
    );
  }

  /**
   * `getProject` with the failure reason preserved.
   *
   * `getProject` collapses every failure into `null`, which is right for the
   * dozen callers that just render nothing. It is wrong for the route guard,
   * which turns the failure into a *sentence shown to the user*: a 500 from
   * the orchestrator was reported as "you don't have access to that project",
   * which is not merely vague but the opposite of true — the membership check
   * had already passed. Callers that diagnose need the status; callers that
   * only render should keep using `getProject`.
   */
  getProjectOrError(
    id: string,
  ): Observable<{project: Project | null; status: number | null}> {
    return this.http.get<Project>(`${this.baseUrl}/projects/${id}`).pipe(
      map((project) => ({project, status: 200})),
      catchError((err: unknown) =>
        of({
          project: null,
          status: err instanceof HttpErrorResponse ? err.status : null,
        }),
      ),
    );
  }

  /** Current user's resolved capabilities + the catalog (drives editor greying). */
  getMyCapabilities(): Observable<UserCapabilities | null> {
    return this.http
      .get<UserCapabilities>(`${this.baseUrl}/users/me/capabilities`)
      .pipe(catchError(() => of(null)));
  }

  /** Whether the caller has a usable TTS / STT model configured. Drives the
   * disabled-with-reason state on the read-aloud + mic buttons so they never
   * present as a dead click that silently 204s. `null` on error ⇒ callers fail
   * open (assume available; the 204 path still guards the actual call). */
  getVoiceCapabilities(): Observable<VoiceCapabilities | null> {
    return this.http
      .get<VoiceCapabilities>(`${this.baseUrl}/voice/capabilities`)
      .pipe(catchError(() => of(null)));
  }

  /** The SSH gateway's hostname and public host keys, unauthenticated by
   * design (host keys are public material). Returns `{host_keys: [],
   * hostname: ...}` — never an error — on a deployment with no gateway
   * configured; `null` only on a transport failure. */
  getSshHostKeys(): Observable<SshGatewayHostKeysResponse | null> {
    return this.http
      .get<SshGatewayHostKeysResponse>(`${this.baseUrl}/ssh/host-keys`)
      .pipe(catchError(() => of(null)));
  }

  /** Errors propagate: the create form shows the server's own refusal, which
   *  for an external knowledge base names the exact thing to change. */
  createProject(body: ProjectCreateRequest): Observable<Project | null> {
    return this.http.post<Project>(`${this.baseUrl}/projects`, body);
  }

  /** Errors propagate for the same reason as `createProject`. */
  attachProjectKnowledgeRepository(
    projectId: string,
    body: ExternalKnowledgeBaseRequest,
  ): Observable<Record<string, unknown> | null> {
    return this.http.post<Record<string, unknown>>(
      `${this.baseUrl}/projects/${projectId}/knowledge/repository`,
      body,
    );
  }

  /**
   * Archive / unarchive. Separate from `updateProject` for two reasons: the
   * response is a report of what archiving quiesced (`loop_paused`,
   * `jobs_parked`), not a bare `{status}`; and errors must propagate — a PATCH
   * carrying a status alongside any other field on an archived project is
   * refused with 409 and a sentence the user needs to read, which
   * `updateProject`'s `catchError(() => of(null))` would erase.
   */
  setProjectStatus(id: string, status: ProjectStatus): Observable<ProjectArchiveReport> {
    return this.http.patch<ProjectArchiveReport>(`${this.baseUrl}/projects/${id}`, { status });
  }

  /**
   * The other half of the same PATCH: everything that is *not* `status`.
   *
   * Same endpoint and body as `updateProject`, and the only difference is the
   * one that matters here — errors propagate. An archived project accepts a
   * status-only body and refuses any other field whole, with a 409 whose
   * sentence names the remedy; `updateProject`'s `catchError(() => of(null))`
   * erases that body, so the save reads as a click that did not register and
   * an optimistic rename visibly reverts with no explanation. Every caller can
   * be writing to a project that is archived — another tab can archive it while
   * a form is open — so every caller uses this one and renders the refusal.
   *
   * `updateProject` below keeps its swallowing contract untouched rather than
   * being loosened: `null`-on-error is a published shape, and a caller that
   * genuinely wants "did it stick?" and nothing more can still have it.
   */
  updateProjectFields(id: string, body: ProjectUpdateRequest): Observable<{ status: string }> {
    return this.http.patch<{ status: string }>(`${this.baseUrl}/projects/${id}`, body);
  }

  /** The same PATCH with failures flattened to `null`. Nothing calls it today —
   *  every project write now needs the refusal — but the contract is kept so a
   *  caller that wants a bare success/failure does not have to re-derive it. */
  updateProject(id: string, body: ProjectUpdateRequest): Observable<{ status: string } | null> {
    return this.http.patch<{ status: string }>(`${this.baseUrl}/projects/${id}`, body).pipe(
      catchError(() => of(null)),
    );
  }

  deleteProject(id: string): Observable<{ status: string } | null> {
    return this.http.delete<{ status: string }>(`${this.baseUrl}/projects/${id}`).pipe(
      catchError(() => of(null)),
    );
  }

  // ===== Project self-improvement loop =====
  // See knowledge-base/knowledge/features/project_self_improvement_loop.md. The GET returns null on
  // 404 (no active loop) — the caller treats null as "show the start form".

  getProjectLoop(projectId: string): Observable<ProjectLoop | null> {
    return this.http
      .get<ProjectLoop>(`${this.baseUrl}/projects/${projectId}/loop`)
      .pipe(catchError(() => of(null)));
  }

  startProjectLoop(
    projectId: string,
    body: ProjectLoopStartRequest,
  ): Observable<ProjectLoop | null> {
    return this.http
      .post<ProjectLoop>(`${this.baseUrl}/projects/${projectId}/loop`, body)
      .pipe(catchError(() => of(null)));
  }

  pauseProjectLoop(projectId: string): Observable<ProjectLoop | null> {
    return this.http
      .post<ProjectLoop>(`${this.baseUrl}/projects/${projectId}/loop/pause`, {})
      .pipe(catchError(() => of(null)));
  }

  resumeProjectLoop(projectId: string): Observable<ProjectLoop | null> {
    return this.http
      .post<ProjectLoop>(`${this.baseUrl}/projects/${projectId}/loop/resume`, {})
      .pipe(catchError(() => of(null)));
  }

  stopProjectLoop(projectId: string): Observable<ProjectLoop | null> {
    return this.http
      .post<ProjectLoop>(`${this.baseUrl}/projects/${projectId}/loop/stop`, {})
      .pipe(catchError(() => of(null)));
  }

  listProjectLoopJobs(projectId: string): Observable<Job[]> {
    return this.http
      .get<Job[]>(`${this.baseUrl}/projects/${projectId}/loop/jobs`)
      .pipe(catchError(() => of([])));
  }

  /** The project's ticket pool (knowledge-base/knowledge/superpowers/specs/2026-07-26-project-backlog-pipeline-design.md). */
  getProjectBacklog(projectId: string): Observable<ProjectBacklog | null> {
    return this.http
      .get<ProjectBacklog>(`${this.baseUrl}/projects/${projectId}/backlog`)
      .pipe(catchError(() => of(null)));
  }

  // ===== Officer post (knowledge-base/knowledge/features/officer_post.md) =====
  // The GET always returns the post — vacant or commissioned. The lifecycle
  // POSTs and the PATCH deliberately do NOT swallow errors: their FastAPI
  // `detail` strings (rival-commission 409s, hold fences, kit validation
  // 400s) are the card's messaging, so the component owns the catch.

  /** The post, always present; null only on transport failure. */
  getOfficerPost(projectId: string): Observable<OfficerPost | null> {
    return this.http
      .get<OfficerPost>(`${this.baseUrl}/projects/${projectId}/officer`)
      .pipe(catchError(() => of(null)));
  }

  /**
   * Raise an officer onto the post. `body` is an optional partial config
   * (same fields as the PATCH) merged into the row before the thread boots;
   * his first wake carries the continuity brief.
   */
  commissionOfficer(
    projectId: string,
    body: OfficerPostPatch = {},
  ): Observable<OfficerCommissionResult> {
    return this.http.post<OfficerCommissionResult>(
      `${this.baseUrl}/projects/${projectId}/officer/commission`,
      body,
    );
  }

  /**
   * End the incarnation, harvesting his state onto the row. Non-forced with
   * jobs in flight returns the warning + list instead of decommissioning;
   * `force` proceeds (jobs are left running either way).
   */
  decommissionOfficer(
    projectId: string,
    force = false,
  ): Observable<OfficerDecommissionResult> {
    return this.http.post<OfficerDecommissionResult>(
      `${this.baseUrl}/projects/${projectId}/officer/decommission`,
      force ? {force: true} : {},
    );
  }

  /** Stand him down in place (maintenance hold) — commissioned, not retired. */
  holdOfficer(projectId: string, note = ''): Observable<{status?: string}> {
    const trimmed = note.trim();
    return this.http.post<{status?: string}>(
      `${this.baseUrl}/projects/${projectId}/officer/hold`,
      trimmed ? {note: trimmed} : {},
    );
  }

  /** Lift the hold; queued events drain within one ~20s tick. */
  releaseOfficer(projectId: string): Observable<{status?: string}> {
    return this.http.post<{status?: string}>(
      `${this.baseUrl}/projects/${projectId}/officer/release`,
      {},
    );
  }

  /** Safely replace only the commissioned Officer's dedicated runtime pod. */
  recycleOfficer(projectId: string): Observable<{state?: string; phase?: string}> {
    return this.http.post<{state?: string; phase?: string}>(
      `${this.baseUrl}/projects/${projectId}/officer/recycle`,
      {},
    );
  }

  /**
   * Edit the post — partial kit/budget/brain fields, and the row-only
   * `communication_policy`. When commissioned the server live-merges the
   * fragment into thread metadata; per-field immediacy is the card's job.
   */
  updateOfficerPost(
    projectId: string,
    body: OfficerPostPatch,
  ): Observable<{status?: string}> {
    return this.http.patch<{status?: string}>(
      `${this.baseUrl}/projects/${projectId}/officer`,
      body,
    );
  }

  getProjectMembers(id: string): Observable<ProjectMember[]> {
    return this.http.get<ProjectMember[]>(`${this.baseUrl}/projects/${id}/members`).pipe(
      catchError(() => of([])),
    );
  }

  addProjectMember(id: string, body: ProjectMemberAddRequest): Observable<ProjectMember | null> {
    return this.http.post<ProjectMember>(`${this.baseUrl}/projects/${id}/members`, body).pipe(
      catchError(() => of(null)),
    );
  }

  updateProjectMember(id: string, userId: string, body: ProjectMemberUpdateRequest): Observable<{ status: string } | null> {
    return this.http.patch<{ status: string }>(`${this.baseUrl}/projects/${id}/members/${userId}`, body).pipe(
      catchError(() => of(null)),
    );
  }

  removeProjectMember(id: string, userId: string): Observable<{ status: string } | null> {
    return this.http.delete<{ status: string }>(`${this.baseUrl}/projects/${id}/members/${userId}`).pipe(
      catchError(() => of(null)),
    );
  }

  getProjectRepositories(id: string): Observable<ProjectRepository[]> {
    return this.http.get<ProjectRepository[]>(`${this.baseUrl}/projects/${id}/repositories`).pipe(
      catchError(() => of([])),
    );
  }

  addProjectRepository(id: string, body: ProjectRepositoryCreateRequest): Observable<ProjectRepository | null> {
    return this.http.post<ProjectRepository>(`${this.baseUrl}/projects/${id}/repositories`, body).pipe(
      catchError(() => of(null)),
    );
  }

  updateProjectRepository(id: string, repoId: string, body: ProjectRepositoryUpdateRequest): Observable<{ status: string } | null> {
    return this.http.patch<{ status: string }>(`${this.baseUrl}/projects/${id}/repositories/${repoId}`, body).pipe(
      catchError(() => of(null)),
    );
  }

  removeProjectRepository(id: string, repoId: string): Observable<{ status: string } | null> {
    return this.http.delete<{ status: string }>(`${this.baseUrl}/projects/${id}/repositories/${repoId}`).pipe(
      catchError(() => of(null)),
    );
  }

  getProjectExperts(projectId: string): Observable<Expert[]> {
    return this.http.get<Expert[]>(`${this.baseUrl}/projects/${projectId}/experts`).pipe(
      catchError(() => of([])),
    );
  }

  getProjectExpertDetail(projectId: string, expertName: string): Observable<ExpertDetail | null> {
    return this.http.get<ExpertDetail>(`${this.baseUrl}/projects/${projectId}/experts/${expertName}`).pipe(
      catchError(() => of(null)),
    );
  }

  getProjectJobs(id: string, status?: string, limit: number = 100): Observable<Job[]> {
    let params = new HttpParams().set('limit', limit.toString());
    if (status) params = params.set('status', status);
    return this.http.get<Job[]>(`${this.baseUrl}/projects/${id}/jobs`, { params }).pipe(
      catchError(() => of([])),
    );
  }

  createProjectJob(id: string, body: JobCreateRequest): Observable<Job | null> {
    return this.http.post<Job>(`${this.baseUrl}/projects/${id}/jobs`, body).pipe(
      catchError(() => of(null)),
    );
  }

  promoteJob(jobId: string, body: PromoteRequest): Observable<{ status: string; project_id?: string } | null> {
    return this.http.post<{ status: string; project_id?: string }>(
      `${this.baseUrl}/jobs/${jobId}/promote`, body,
    ).pipe(catchError(() => of(null)));
  }

  /**
   * Mode B of the job cloud workflow — export a completed loose job's output/
   * folder to a freshly-allocated shared cloud folder. Only valid for jobs
   * that are completed AND have no project_id. Returns the folder's
   * browser/WebDAV URLs and the number of files copied.
   *
   * See knowledge-history/done/job_cloud_export.md §3.2.
   */
  exportJobToSharedFolder(jobId: string): Observable<JobCloudExportResult | null> {
    return this.http
      .post<JobCloudExportResult>(
        `${this.baseUrl}/jobs/${jobId}/export-to-shared-folder`,
        {},
      )
      .pipe(
        tap((result) => {
          const params = {
            count: result?.files_copied ?? 0,
            // Where it landed in the user's own cloud. Falls back to the bare
            // name for a pre-`path` orchestrator.
            folder: result?.folder?.path ?? result?.folder?.name ?? '',
          };
          // `shared: false` means the bytes landed but the folder isn't
          // visible to this user yet (no cloud account until their first
          // login), so opening it would 404. Say that instead of "success".
          if (result && result.shared === false) {
            this.toast.warning(this.t('toasts.jobs.exportedNotShared', params));
          } else {
            this.toast.success(this.t('toasts.jobs.exportedToCloud', params));
          }
        }),
        catchError((error) => {
          console.error(`Failed to export job ${jobId} to cloud:`, error);
          this.toast.danger(this.errors.translate(error, 'errors.jobs.exportFailed'));
          return of(null);
        }),
      );
  }

  // ===== Mode A diff review (job_cloud_export.md §3.4–§3.6) =====

  /**
   * Map an HTTP failure on a diff *read* into a `DiffLoadOutcome`.
   *
   * The distinctions the review surface needs are exactly the ones the
   * routes make: 403 from `require_thread_owner` / job ownership, 404 from
   * `_require_protected` and "thread not found" (which the backend does not
   * separate — both are plain-string details), 404 on a per-file read
   * meaning the path left the staged set, and everything else. Angular
   * reports network/CORS/DNS failures as `status === 0`; those get the
   * shared offline copy rather than a bare "Http failure response".
   */
  private diffReadFailure<T>(
    err: HttpErrorResponse,
    fallbackKey: string,
    { fileRead = false }: { fileRead?: boolean } = {},
  ): DiffLoadOutcome<T> {
    if (err.status === 403) return { kind: 'forbidden' };
    if (err.status === 404) {
      if (!fileRead) return { kind: 'unavailable' };
      // The per-file 404 covers three different situations and the copy for
      // them differs; carry the backend's code through when it sends one.
      const detail = err.error?.detail;
      const code =
        detail && typeof detail === 'object' && typeof detail.code === 'string'
          ? (detail.code as string)
          : undefined;
      return { kind: 'missing', code };
    }
    const detail =
      err.status === 0
        ? this.transloco.translate('errors.network')
        : this.errors.translate(err, fallbackKey);
    return { kind: 'error', status: err.status, detail };
  }

  /**
   * Fetch the file-level diff summary for a project-attached job in
   * pending_review, as a tagged outcome. `unavailable` means the
   * orchestrator has no Mode A baseline for the job (loose job, or a
   * pre-Mode-A project job).
   */
  getJobDiffOutcome(jobId: string): Observable<DiffLoadOutcome<JobDiffSummary>> {
    return this.http.get<JobDiffSummary>(`${this.baseUrl}/jobs/${jobId}/diff`).pipe(
      map((data): DiffLoadOutcome<JobDiffSummary> => ({ kind: 'ok', data })),
      catchError((err: HttpErrorResponse) =>
        of(this.diffReadFailure<JobDiffSummary>(err, 'errors.jobs.diffLoadFailed')),
      ),
    );
  }

  /**
   * Nullable view of {@link getJobDiffOutcome}, kept for callers that only
   * need "is there a diff" and have no failure branch to render.
   */
  getJobDiff(jobId: string): Observable<JobDiffSummary | null> {
    return this.getJobDiffOutcome(jobId).pipe(
      map((outcome) => (outcome.kind === 'ok' ? outcome.data : null)),
    );
  }

  /**
   * Fetch one file's old/new content for the Mode A diff view.
   * Paths must be under ``projects/<slug>/`` — the orchestrator rejects
   * anything else with 400. ``old_content`` is null for ``added``,
   * ``new_content`` is null for ``deleted``.
   */
  getJobDiffFileOutcome(
    jobId: string,
    filePath: string,
  ): Observable<DiffLoadOutcome<JobDiffFile>> {
    // Encode each path segment so spaces / unicode / German umlauts in
    // the slug survive the round-trip. FastAPI's ``:path`` converter
    // happily takes encoded slashes.
    const encoded = filePath.split('/').map(encodeURIComponent).join('/');
    return this.http.get<JobDiffFile>(`${this.baseUrl}/jobs/${jobId}/diff/${encoded}`).pipe(
      map((data): DiffLoadOutcome<JobDiffFile> => ({ kind: 'ok', data })),
      catchError((err: HttpErrorResponse) =>
        of(
          this.diffReadFailure<JobDiffFile>(err, 'errors.jobs.diffFileLoadFailed', {
            fileRead: true,
          }),
        ),
      ),
    );
  }

  /** Nullable view of {@link getJobDiffFileOutcome}. */
  getJobDiffFile(jobId: string, filePath: string): Observable<JobDiffFile | null> {
    return this.getJobDiffFileOutcome(jobId, filePath).pipe(
      map((outcome) => (outcome.kind === 'ok' ? outcome.data : null)),
    );
  }

  /**
   * Accept the Mode A diff — orchestrator writes each change back to
   * the project's cloud folder, then transitions diff_status=accepted +
   * status=completed.
   *
   * Returns a tagged outcome so the component can branch on:
   * - `ok`: success, surface applied/deleted counts
   * - `conflict`: 409 external_modifications_detected (show banner +
   *   diverged paths)
   * - `partial`: 502 partial_write_failure (show per-file errors)
   * - `error`: any other failure (toast + log)
   */
  acceptJobDiff(jobId: string): Observable<JobAcceptOutcome> {
    return this.http
      .post<JobAcceptResult>(`${this.baseUrl}/jobs/${jobId}/accept`, {})
      .pipe(
        map((data): JobAcceptOutcome => ({ kind: 'ok', data })),
        catchError((err: HttpErrorResponse): Observable<JobAcceptOutcome> => {
          // FastAPI wraps custom 409 / 502 payloads under {detail: {...}}.
          const detail = err.error?.detail;
          if (err.status === 409 && detail && typeof detail === 'object' &&
              detail.code === 'external_modifications_detected') {
            return of({ kind: 'conflict', data: detail as JobAcceptConflict });
          }
          if (err.status === 502 && detail && typeof detail === 'object' &&
              detail.code === 'partial_write_failure') {
            return of({ kind: 'partial', data: detail as JobAcceptPartialFailure });
          }
          const message = typeof detail === 'string'
            ? detail
            : this.errors.translate(err, 'errors.jobs.acceptFailed');
          return of({ kind: 'error', status: err.status, detail: message });
        }),
      );
  }

  /**
   * Reject the Mode A diff — orchestrator stamps diff_status=rejected
   * and status=completed. No cloud writes happen; the Gitea commits
   * remain as audit trail.
   */
  rejectJobDiff(jobId: string): Observable<DiffRejectOutcome<JobRejectResult>> {
    return this.http.post<JobRejectResult>(`${this.baseUrl}/jobs/${jobId}/reject`, {}).pipe(
      map((data): DiffRejectOutcome<JobRejectResult> => ({ kind: 'ok', data })),
      catchError((err: HttpErrorResponse) =>
        of(this.rejectFailure<JobRejectResult>(err, 'errors.jobs.rejectFailed')),
      ),
    );
  }

  /**
   * Shared failure mapping for the two reject paths.
   *
   * No toast from in here: the review surface renders the outcome itself, and
   * a service-level toast both duplicates that and fires for callers that
   * already have somewhere better to put it.
   */
  private rejectFailure<T>(err: HttpErrorResponse, fallbackKey: string): DiffRejectOutcome<T> {
    const detail = err.error?.detail;
    if (err.status === 409 && detail && typeof detail === 'object') {
      if (detail.code === 'epoch_stale') {
        const staged = detail.staged_epoch;
        return { kind: 'stale', staged_epoch: typeof staged === 'number' ? staged : null };
      }
      if (detail.code === 'nothing_staged') return { kind: 'nothing_staged' };
    }
    const message =
      err.status === 0
        ? this.transloco.translate('errors.network')
        : typeof detail === 'string'
          ? detail
          : this.errors.translate(err, fallbackKey);
    return { kind: 'error', status: err.status, detail: message };
  }

  // ===== Protected cloud mode: thread cloud-diff review (Slice C, Task 8/10) =====
  //
  // Thread-mode counterpart to the Mode A diff review above — same
  // JobDiffReviewComponent, different backend surface:
  // GET/POST .../agents/threads/{id}/cloud-diff[...]. See
  // .superpowers/sdd/task-8-brief.md / task-10-brief.md for the response
  // shapes this mirrors.

  /**
   * Fetch the staged protected-cloud diff summary for a thread. The
   * endpoint itself never 404s for "nothing staged" — it returns the
   * all-zero/epoch-0/empty-files shape — so `null` here means a hard
   * failure (network, thread not protected, not the owner).
   */
  getThreadCloudDiffOutcome(
    threadId: string,
  ): Observable<DiffLoadOutcome<ThreadCloudDiffSummary>> {
    return this.http
      .get<ThreadCloudDiffSummary>(`${this.baseUrl}/agents/threads/${threadId}/cloud-diff`)
      .pipe(
        map((data): DiffLoadOutcome<ThreadCloudDiffSummary> => ({ kind: 'ok', data })),
        catchError((err: HttpErrorResponse) =>
          of(this.diffReadFailure<ThreadCloudDiffSummary>(err, 'errors.cloudDiff.loadFailed')),
        ),
      );
  }

  /**
   * Nullable view of {@link getThreadCloudDiffOutcome}, used by the badge /
   * pending-count path which has no failure branch to render and just keeps
   * its previous count.
   */
  getThreadCloudDiff(threadId: string): Observable<ThreadCloudDiffSummary | null> {
    return this.getThreadCloudDiffOutcome(threadId).pipe(
      map((outcome) => (outcome.kind === 'ok' ? outcome.data : null)),
    );
  }

  /**
   * Fetch one staged file's old/new content for the thread cloud-diff
   * viewer. Mirrors getJobDiffFile's per-segment path encoding. A 404 is
   * `missing`, not an error: the endpoint 404s both for "not in the staged
   * diff" and for "nothing staged at all", which after a successful load of
   * the summary means the staged set moved underneath the reviewer.
   */
  getThreadCloudDiffFileOutcome(
    threadId: string,
    filePath: string,
  ): Observable<DiffLoadOutcome<ThreadCloudDiffFile>> {
    const encoded = filePath.split('/').map(encodeURIComponent).join('/');
    return this.http
      .get<ThreadCloudDiffFile>(`${this.baseUrl}/agents/threads/${threadId}/cloud-diff/${encoded}`)
      .pipe(
        map((data): DiffLoadOutcome<ThreadCloudDiffFile> => ({ kind: 'ok', data })),
        catchError((err: HttpErrorResponse) =>
          of(
            this.diffReadFailure<ThreadCloudDiffFile>(err, 'errors.cloudDiff.fileLoadFailed', {
              fileRead: true,
            }),
          ),
        ),
      );
  }

  /** Nullable view of {@link getThreadCloudDiffFileOutcome}. */
  getThreadCloudDiffFile(threadId: string, filePath: string): Observable<ThreadCloudDiffFile | null> {
    return this.getThreadCloudDiffFileOutcome(threadId, filePath).pipe(
      map((outcome) => (outcome.kind === 'ok' ? outcome.data : null)),
    );
  }

  /**
   * Apply the staged protected-cloud diff, pinned to the epoch the caller
   * last observed via getThreadCloudDiff. Mirrors acceptJobDiff's tagged
   * outcome (conflict / partial / error), plus a `stale` variant for the
   * epoch pin's 409 — someone else applied/rejected/restaged since the
   * summary was read; the caller should reload and retry.
   */
  applyThreadCloudDiff(threadId: string, epoch: number): Observable<JobAcceptOutcome> {
    return this.http
      .post<ThreadCloudApplyResult>(
        `${this.baseUrl}/agents/threads/${threadId}/cloud-diff/apply`, { epoch },
      )
      .pipe(
        map((data): JobAcceptOutcome => ({ kind: 'ok', data })),
        catchError((err: HttpErrorResponse): Observable<JobAcceptOutcome> => {
          // FastAPI wraps custom 409 / 410 / 502 payloads under {detail: {...}}.
          const detail = err.error?.detail;
          if (err.status === 409 && detail && typeof detail === 'object' &&
              detail.code === 'external_modifications_detected') {
            return of({ kind: 'conflict', data: detail as JobAcceptConflict });
          }
          if (err.status === 409 && detail && typeof detail === 'object' &&
              detail.code === 'epoch_stale') {
            return of({ kind: 'stale', staged_epoch: detail.staged_epoch as number });
          }
          if (err.status === 502 && detail && typeof detail === 'object' &&
              detail.code === 'partial_write_failure') {
            return of({ kind: 'partial', data: detail as JobAcceptPartialFailure });
          }
          const message = typeof detail === 'string'
            ? detail
            : this.errors.translate(err, 'errors.cloudDiff.applyFailed');
          return of({ kind: 'error', status: err.status, detail: message });
        }),
      );
  }

  /**
   * Reject the staged protected-cloud diff, same epoch pin as apply. Never
   * touches the cloud — just discards the staged upperdir capture.
   */
  rejectThreadCloudDiff(
    threadId: string,
    epoch: number,
  ): Observable<DiffRejectOutcome<ThreadCloudRejectResult>> {
    return this.http
      .post<ThreadCloudRejectResult>(
        `${this.baseUrl}/agents/threads/${threadId}/cloud-diff/reject`, { epoch },
      )
      .pipe(
        map((data): DiffRejectOutcome<ThreadCloudRejectResult> => ({ kind: 'ok', data })),
        catchError((err: HttpErrorResponse) =>
          of(this.rejectFailure<ThreadCloudRejectResult>(err, 'errors.cloudDiff.rejectFailed')),
        ),
      );
  }

  /**
   * Owner-triggered re-stage (fresh overlay capture) of a thread's
   * protected-cloud diff. Fire-and-forget on the orchestrator side
   * (schedules a background task); the caller re-polls getThreadCloudDiff
   * to observe the refreshed summary.
   */
  restageThreadCloudDiff(threadId: string): Observable<unknown> {
    return this.http
      .post(`${this.baseUrl}/agents/threads/${threadId}/cloud-diff/restage`, {})
      .pipe(
        catchError((err) => {
          console.error(`Failed to restage thread ${threadId} cloud diff:`, err);
          this.toast.danger(this.errors.translate(err, 'errors.cloudDiff.restageFailed'));
          return of(null);
        }),
      );
  }

  // ===== Knowledge Base Endpoints =====

  getKnowledgeSummary(projectId: string): Observable<KnowledgeSummary | null> {
    return this.http.get<KnowledgeSummary>(
      `${this.baseUrl}/projects/${projectId}/knowledge/summary`,
    ).pipe(catchError(() => of(null)));
  }

  getKnowledgeNotes(
    projectId: string,
    filters?: { type?: string; status?: string; tag?: string; job_id?: string; limit?: number; offset?: number },
  ): Observable<KnowledgeListResponse | null> {
    let params = new HttpParams();
    if (filters?.type) params = params.set('type', filters.type);
    if (filters?.status) params = params.set('status', filters.status);
    if (filters?.tag) params = params.set('tag', filters.tag);
    if (filters?.job_id) params = params.set('job_id', filters.job_id);
    if (filters?.limit) params = params.set('limit', filters.limit.toString());
    if (filters?.offset) params = params.set('offset', filters.offset.toString());
    return this.http.get<KnowledgeListResponse>(
      `${this.baseUrl}/projects/${projectId}/knowledge`, { params },
    ).pipe(catchError(() => of(null)));
  }

  getKnowledgeNote(projectId: string, noteId: string): Observable<KnowledgeNoteDetail | null> {
    return this.http.get<KnowledgeNoteDetail>(
      `${this.baseUrl}/projects/${projectId}/knowledge/${noteId}`,
    ).pipe(catchError(() => of(null)));
  }

  searchKnowledge(projectId: string, query: string, limit: number = 10): Observable<KnowledgeSearchResponse | null> {
    return this.http.post<KnowledgeSearchResponse>(
      `${this.baseUrl}/projects/${projectId}/knowledge/search`,
      { query, limit },
    ).pipe(catchError(() => of(null)));
  }

  updateKnowledgeNote(
    projectId: string, noteId: string,
    body: { status?: string; add_tags?: string[]; remove_tags?: string[] },
  ): Observable<{ status: string } | null> {
    return this.http.patch<{ status: string }>(
      `${this.baseUrl}/projects/${projectId}/knowledge/${noteId}`, body,
    ).pipe(catchError(() => of(null)));
  }

  deleteKnowledgeNote(projectId: string, noteId: string): Observable<{ status: string } | null> {
    return this.http.delete<{ status: string }>(
      `${this.baseUrl}/projects/${projectId}/knowledge/${noteId}`,
    ).pipe(catchError(() => of(null)));
  }

  exportKnowledge(projectId: string): Observable<{ status: string; path: string; note_count: number } | null> {
    return this.http.post<{ status: string; path: string; note_count: number }>(
      `${this.baseUrl}/projects/${projectId}/knowledge/export`, {},
    ).pipe(catchError(() => of(null)));
  }

  // ===== Memory Endpoints =====

  getMemoryStats(jobId: string): Observable<MemoryStats | null> {
    return this.http.get<MemoryStats>(`${this.baseUrl}/jobs/${jobId}/memory/stats`).pipe(
      catchError(() => of(null)),
    );
  }

  getMemories(
    jobId: string,
    filters?: {
      memory_type?: string;
      source?: string;
      search?: string;
      sort_by?: string;
      sort_order?: string;
      limit?: number;
      offset?: number;
    },
  ): Observable<MemoryListResponse | null> {
    let params = new HttpParams();
    if (filters?.memory_type) params = params.set('memory_type', filters.memory_type);
    if (filters?.source) params = params.set('source', filters.source);
    if (filters?.search) params = params.set('search', filters.search);
    if (filters?.sort_by) params = params.set('sort_by', filters.sort_by);
    if (filters?.sort_order) params = params.set('sort_order', filters.sort_order);
    if (filters?.limit) params = params.set('limit', filters.limit.toString());
    if (filters?.offset) params = params.set('offset', filters.offset.toString());

    return this.http.get<MemoryListResponse>(
      `${this.baseUrl}/jobs/${jobId}/memories`, { params },
    ).pipe(catchError(() => of(null)));
  }

  // ---------------------------------------------------------------------------
  // Stateless run_queue state (stateless_turn_resilience.md, step 2)
  // ---------------------------------------------------------------------------

  /** Executors, runnable depth, `desired`, and the parked worklist. Admin-only. */
  getAdminCapacity(): Observable<AdminCapacity | null> {
    return this.http.get<AdminCapacity>(`${this.baseUrl}/admin/capacity`).pipe(
      catchError((error) => {
        console.error('Failed to fetch capacity:', error);
        return of(null);
      }),
    );
  }

  /** Operator verb: parked → queued (attempts reset). Null on any refusal. */
  unparkRunQueueUnit(unitId: string): Observable<RunQueueUnparkResult | null> {
    return this.http
      .post<RunQueueUnparkResult>(`${this.baseUrl}/admin/run-queue/${unitId}/unpark`, {})
      .pipe(
        catchError((error) => {
          console.error(`Failed to unpark ${unitId}:`, error);
          return of(null);
        }),
      );
  }

  /** The thread's durable queue block — polled while a send awaits a claim,
   *  and while a start panel is still up on a thread that may already have
   *  finished its turn.
   *
   *  `GET …/queue` answers with the block wrapped in a `{thread_id, queue}`
   *  envelope, unlike `/input` and `/connection`, which carry the same block
   *  inline. Unwrapping here was missing, so every poll resolved to an
   *  envelope whose `state` is undefined and the caller's type guard dropped
   *  it — the poll ran and never applied anything. A bare block is still
   *  accepted so a rolling deploy of either shape lands. */
  getThreadQueue(threadId: string): Observable<SessionQueueState | null> {
    return this.http
      .get<SessionQueueState | { queue?: SessionQueueState | null }>(
        `${this.baseUrl}/persistent/threads/${threadId}/queue`,
      )
      .pipe(
        map((body): SessionQueueState | null => {
          if (!body || typeof body !== 'object') return null;
          const enveloped = (body as { queue?: SessionQueueState | null }).queue;
          if (enveloped && typeof enveloped === 'object') return enveloped;
          return typeof (body as SessionQueueState).state === 'string'
            ? (body as SessionQueueState)
            : null;
        }),
        catchError(() => of(null)),
      );
  }

  /**
   * Owner verb: a parked, retryable unit → queued. 409 carries a `{code}`
   * detail (stop markers / claim-loss hold), 404 means "not parked".
   */
  retryThreadQueue(threadId: string): Observable<SessionQueueRetryOutcome> {
    return this.http
      .post<{ state: string }>(`${this.baseUrl}/persistent/threads/${threadId}/queue/retry`, {})
      .pipe(
        map((data): SessionQueueRetryOutcome => ({ kind: 'ok', state: data?.state ?? 'queued' })),
        catchError((err: HttpErrorResponse): Observable<SessionQueueRetryOutcome> => {
          const detail = err.error?.detail;
          const code =
            detail && typeof detail === 'object' && typeof detail.code === 'string'
              ? detail.code
              : typeof detail === 'string'
                ? detail
                : err.status === 404
                  ? 'not_parked'
                  : 'retry_failed';
          return of({ kind: 'refused', status: err.status ?? 0, code });
        }),
      );
  }
}
