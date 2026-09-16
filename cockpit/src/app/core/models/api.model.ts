/**
 * API data models for the Cockpit.
 */

/**
 * Table metadata from the API.
 */
export interface TableInfo {
  name: string;
  rowCount: number;
}

/**
 * Column definition for a database table.
 */
export interface ColumnDef {
  name: string;
  type: 'string' | 'number' | 'boolean' | 'date' | 'json' | 'binary';
  nullable: boolean;
}

/**
 * Paginated table data response.
 */
export interface TableDataResponse {
  columns: ColumnDef[];
  rows: Record<string, unknown>[];
  total: number;
  page: number;
  pageSize: number;
}

/**
 * Pagination state.
 */
export interface PaginationState {
  page: number;
  pageSize: number;
  total: number;
}

// =============================================================================
// Expert Models
// =============================================================================

/**
 * The roles an expert can run in (universal_experts_and_subagents.md §0 D4).
 * One schema, one role overlay each; every expert stays usable in every role.
 */
export type ExpertRole = 'worker' | 'session' | 'subagent';

export const EXPERT_ROLES: readonly ExpertRole[] = ['worker', 'session', 'subagent'];

/**
 * Expert configuration for discovery and selection.
 */
export interface Expert {
  id: string;
  /** Slug used to reference the expert by name (e.g. a loop's role_sequence).
   *  For bundled experts this equals `id`; for DB experts it's the name column.
   *  Subagent-library entries (`config/subagents/*`) carry `subagents/<id>`
   *  here — the unambiguous `$ref` spelling for a roster entry. */
  name?: string;
  display_name: string;
  description: string;
  icon: string;
  color: string;
  /** Additive metadata — a soft UI filter, never read for behaviour. Every
   *  listed expert carries its role tag(s) (`worker` / `session` /
   *  `subagent`) next to whatever free-text tags the author added; the list
   *  filters and the pickers match on `expert_type || tags`. */
  tags: string[];
  /** 'bundled' (disk config) | 'library' (config/subagents/*) | 'user' |
   *  'global' | 'managed' (DB-backed). DB experts are selected via expert_id;
   *  bundled experts via config_name. */
  source?: string;
  storage_kind?: 'bundled' | 'library' | 'db';
  owner_id?: string | null;
  managed_key?: string | null;
  /** 'worker' | 'session' — the expert's identity role (a library entry
   *  reports its `$extends` chain root, `worker` by default). */
  expert_type?: string;
}

// ---- Subagent roster (`config.subagents`, universal_experts_and_subagents.md §1.1) ----

export type SubagentIsolation = 'shared' | 'worktree';
export type SubagentWritePolicy = 'none' | 'scratch_only' | 'owned_paths' | 'full';
export type SubagentReturnKind = 'summary' | 'structured' | 'evidence' | 'diff';

/** The `llm.model` value that means "run on the parent's model". */
export const SUBAGENT_INHERIT_MODEL = 'inherit';

/**
 * One `subagents.roster` entry. Inline: any expert-schema key (resolved on the
 * subagent overlay). Reference: `$ref` — a bundled expert dir name (`critic`),
 * a library entry (`subagents/explorer`) or a DB expert id — plus optional
 * sibling keys deep-merged over the referenced expert. `isolation`,
 * `write_policy`, `limits.*` budgets and `return` are U3 runtime keys carried
 * verbatim in U1.
 */
export interface SubagentRosterEntry {
  $ref?: string;
  description?: string;
  /** `{model: 'inherit' | <catalog model>, ...provider/transport/params}`. */
  llm?: Record<string, unknown>;
  tools?: Record<string, unknown>;
  prompts?: Record<string, string | null>;
  isolation?: SubagentIsolation;
  write_policy?: SubagentWritePolicy;
  limits?: Record<string, unknown>;
  return?: SubagentReturnKind;
  [key: string]: unknown;
}

/** The expert's `subagents` block. `llm` is the roster-wide model default
 *  (the "Subagent model" picker); `default` names the entry `delegate_agent`
 *  falls back to when a call passes no `subagent_type`. */
export interface SubagentsConfig {
  default?: string | null;
  llm?: Record<string, unknown>;
  roster?: Record<string, SubagentRosterEntry>;
}

export interface ExpertDefaultSlot {
  application: Expert | null;
  personal: Expert | null;
  effective: Expert | null;
  source: 'project' | 'user' | 'application' | 'explicit';
}

export interface ExpertDefaultsResponse {
  personal_defaults_allowed: boolean;
  defaults: {
    worker: ExpertDefaultSlot;
    session: ExpertDefaultSlot;
  };
}

/**
 * Full expert detail including merged config and instructions content.
 * Returned by GET /api/experts/{expert_id}.
 */
/** Provenance of a slot's effective model (what runs if the picker is untouched). */
export type EffectiveModelSource =
  | 'expert'
  | 'account_default'
  | 'system_default'
  | 'project';

export interface EffectiveModelSlot {
  model: string | null;
  source: EffectiveModelSource;
}

/**
 * Per-slot effective model + provenance, computed server-side with the same
 * precedence dispatch uses. Lets the picker show the model that will actually
 * run if the user makes no change. See Layer 3 in the issue doc
 * loop_ran_codex_spark_not_selected_model_then_hung_on_cooldown.md.
 *
 * Since U1 an expert has ONE model (`llm.model`): `model` is what the picker's
 * unset "Default" option names in both the job and the session form
 * (`session` is kept equal to it); `subagent` is the roster-wide
 * `subagents.llm.model` when pinned to a real model, else `model` (`inherit`
 * IS the parent's model). The per-phase `strategic` / `tactical` slots are
 * gone on both sides.
 */
export interface EffectiveModels {
  model: EffectiveModelSlot;
  /** Roster-wide subagent model (`subagents.llm.model`), falling back to
   *  `model` — mirrors the resolver's `inherit` handling. */
  subagent: EffectiveModelSlot;
  /** Always equal to `model`; kept for readers of the session detail. */
  session: EffectiveModelSlot;
}

export interface ExpertDetail extends Expert {
  workspace_preference?: {backend: "none" | "virtual" | "sandbox" | "vm"} | null;
  config: Record<string, unknown>;
  instructions: string | null;
  /**
   * Categories that refuse `tools.<c>: true` at the write boundary, mapped to
   * the enumeration to send instead. Registry-derived, served rather than
   * mirrored — it is what lets a form with no resolved read offer "shell on"
   * instead of emitting a request the boundary rejects.
   */
  enumerate_only?: Record<string, string[]>;
  /** Raw settings_matrix.yaml for client-side model-family resolution. */
  settings_matrix?: Record<string, Record<string, unknown>>;
  /** Effective model + provenance per slot (server-resolved). */
  effective_models?: EffectiveModels | null;
  /** The role the detail was resolved in (`GET /api/experts/{id}?role=`);
   *  `expert_type` stays the expert's identity. */
  resolved_role?: ExpertRole;
  /** DB-backed experts only — present on create/update responses + detail. */
  name?: string;
  owner_id?: string;
  version?: number;
  /** Prompt segments for DB-backed experts: persona, instructions, and (Part 2)
   *  strategic, tactical, summarization. Empty/absent segments inherit the base. */
  prompts?: Record<string, unknown>;
}

/**
 * Response of POST /api/experts/{id}/duplicate. The forked row (same shape
 * as ExpertDetail) plus `dropped` — not folded into ExpertDetail itself
 * because no other expert endpoint ever sets this field.
 *
 * 2026-08-04 decision: a source config may need a capability grant (e.g.
 * `shell_tools`) the copier does not hold. Duplicate strips what's ungranted
 * rather than refusing the fork (refusing blocked "start from scholar" for
 * every default-grants user), and reports what it removed here so the
 * strip is never silent — see knowledge-history/done/global_expert_management.md decision 9.
 */
export interface ExpertDuplicateResult extends ExpertDetail {
  /**
   * Capability grant keys stripped from the copy because the copier doesn't
   * hold them (e.g. `"shell_tools"`, `"delegation"`) — the same names shown
   * in the Admin UI's grants panel. Empty or absent when nothing was
   * stripped (the copier already held everything the source needed).
   */
  dropped?: string[];
}

/**
 * Response of POST /api/expert-defaults/{type}/fork — `duplicate` plus
 * "select the copy as my default" (task 4 of the same 2026-08-04 plan as
 * `ExpertDuplicateResult` above). `default` is the new personal-default row,
 * in the same summary shape `GET /api/expert-defaults` returns for a slot
 * (not a full `ExpertDetail`: no `config`/`instructions`).
 */
export interface ExpertDefaultForkResult {
  default: Expert | null;
  source: 'user';
  /** Same meaning as `ExpertDuplicateResult.dropped` above — the grant keys
   *  the fork needed that this caller doesn't hold, stripped rather than
   *  refusing the fork. Empty or absent when nothing was stripped. */
  dropped?: string[];
}

/**
 * Create a DB-backed expert (POST /api/experts). The save-time hard-deny scan
 * runs server-side on ``config``; per-user grants are a later slice.
 */
export interface ExpertCreateRequest {
  name: string;
  display_name: string;
  expert_type: 'worker' | 'session';
  description?: string | null;
  icon?: string;
  color?: string;
  /** Role tags (`worker` / `session` / `subagent`) + free-text tags. The
   *  server always adds the `expert_type` role tag on write. */
  tags?: string[];
  /** May carry a `subagents` roster — validated server-side (422/400). */
  config?: Record<string, unknown>;
  prompts?: Record<string, unknown>;
}

/**
 * Patch a DB-backed expert (PUT /api/experts/{id}). ``name`` and
 * ``expert_type`` are immutable, so they are absent.
 */
export type ExpertUpdateRequest = Partial<
  Omit<ExpertCreateRequest, 'name' | 'expert_type'>
>;

// =============================================================================
// Skill Models (Agent Skills — the open SKILL.md standard)
// =============================================================================

/** Skill catalog entry (GET /api/skills). */
export interface Skill {
  id: string;
  name: string;
  display_name: string;
  description: string;
  icon: string;
  color: string;
  tags: string[];
  /** 'bundled' (disk) | 'user' | 'global' (DB-backed). */
  source?: string;
}

/** Full skill detail incl. the file tree (GET /api/skills/{id}). */
export interface SkillDetail extends Skill {
  /** path -> content; always includes 'SKILL.md' (the canonical artifact). */
  files: Record<string, string>;
  version?: number;
  owner_id?: string;
}

/**
 * Create a DB-backed skill (POST /api/skills). name + description are parsed
 * from the SKILL.md frontmatter server-side, not sent separately.
 */
export interface SkillCreateRequest {
  files: Record<string, string>;
  display_name?: string | null;
  icon?: string;
  color?: string;
  tags?: string[];
}

/** Patch a DB-backed skill (PUT /api/skills/{id}). name is immutable, so absent. */
export interface SkillUpdateRequest {
  files?: Record<string, string>;
  display_name?: string;
  icon?: string;
  color?: string;
  tags?: string[];
  is_global?: boolean;
}

// =============================================================================
// Datasource Models
// =============================================================================

/**
 * Supported datasource types.
 *
 * ``kubeconfig`` / ``ssh_key`` / ``generic_file`` are credential-file types whose
 * ``credentials.files[]`` payload is materialized as files on the agent's
 * filesystem at job start (see knowledge-base/knowledge/features/credential_file_datasources.md).
 */
export type DatasourceType =
  | 'generic'
  | 'credentials'
  | 'repository'
  | 'kb'
  | 'postgresql'
  | 'neo4j'
  | 'mongodb'
  | 'webdav'
  | 'email'
  | 'mcp'
  | 'kubeconfig'
  | 'ssh_key'
  | 'generic_file';

/**
 * Access tier for an ``email`` datasource (see knowledge-base/knowledge/features/email_datasource.md).
 * ``draft`` is the default: the agent composes drafts, the human sends them.
 */
export type EmailAccessTier = 'read' | 'read_write' | 'draft' | 'send';

/**
 * Forge (git host software) for a ``repository`` datasource. Required by the
 * server (``orchestrator/main.py`` ``_normalize_repository_config``): it
 * infers ``github``/``gitlab`` from a github.com/gitlab.com connection URL,
 * but a self-hosted host must declare this explicitly — a self-hosted Gitea
 * and a self-hosted GitLab are indistinguishable by URL alone.
 */
export type RepositoryForge = 'github' | 'gitea' | 'gitlab';

/**
 * A single file entry inside ``credentials.files[]`` for credential-file types.
 *
 * The server applies type-specific defaults (target_path, mode, env_var) when
 * the client omits them — the cockpit only needs to send ``contents`` for
 * ``kubeconfig`` and ``ssh_key``. ``generic_file`` requires ``target_path``.
 */
export interface CredentialFileEntry {
  name?: string;
  contents: string;
  target_path?: string;
  mode?: string;
  env_var?: string;
}

/** Non-secret, type-specific datasource configuration. */
export interface DatasourceConfig {
  /** Server-owned marker for a project's native knowledge connector. */
  native_project_id?: string;
  /** Repository-relative POSIX root containing OKF Markdown notes. */
  root_path?: string;
  /** Repository: which forge this connector targets — see ``RepositoryForge``. */
  forge?: RepositoryForge;
  /** Email: access tier (tool-layer enforced; ``draft`` is the default). */
  access?: EmailAccessTier;
  /** Email: folder allowlist; empty = whole mailbox (rejected for ``send``). */
  folders?: string[];
  /** Email: fallback Drafts folder (SPECIAL-USE ``\\Drafts`` resolved first). */
  drafts_folder?: string;
  /** Email: From address used for compositions. */
  from_address?: string;
  /** Email: addresses/@domains allowed for new (non-reply) compositions. */
  recipient_allowlist?: string[];
  /** Email: skip the human send-approval freeze (needs the
   *  ``email_autonomous_send`` grant; the server rejects it otherwise). */
  unattended_send?: boolean;
}

export type DatasourceIndexState =
  | 'pending'
  | 'indexing'
  | 'ready'
  | 'partial'
  | 'failed';

/** Where a connector may be used. Project ids remain in project_datasources. */
export type DatasourceScopeMode = 'all' | 'projects';

/** Operational state of a centrally indexed OKF Knowledge Base datasource. */
export interface DatasourceIndexStatus {
  datasource_id: string;
  status: DatasourceIndexState;
  source_head: string | null;
  indexed_commit: string | null;
  pipeline_version: string | null;
  repo_name?: string | null;
  branch?: string | null;
  last_attempt_at: string | null;
  last_success_at: string | null;
  /** Bounded, credential-redacted diagnostic supplied by the orchestrator. */
  last_error: string | null;
  /** Per-run progress counters while a KB indexes (advisory; null when idle). */
  notes_done?: number | null;
  notes_total?: number | null;
}

/** Tolerant summary returned by a manual incremental/full KB reindex. */
export interface DatasourceReindexResult {
  status: string;
  indexed_commit?: string | null;
  full?: boolean;
  upserted?: number;
  deleted?: number;
  reconciled?: number;
  skipped?: number;
  errors?: number;
}

/**
 * Datasource configuration from the orchestrator.
 */
export interface Datasource {
  id: string;
  name: string;
  description: string | null;
  type: DatasourceType;
  connection_url: string | null;
  /** True when REST returned only a sanitized display URL. Editors must omit
   * connection_url on update until the user deliberately replaces it. */
  connection_url_redacted?: boolean;
  /**
   * F3: credentials are never returned over REST anymore — the field is
   * stripped server-side (see `redact_datasource` in
   * `orchestrator/security/access.py`). Edit forms must use the
   * "leave blank to keep existing" UX.
   */
  credentials?: Record<string, unknown>;
  /** Configured names only; values are never returned by connector reads. */
  env_var_names?: string[];
  cli_hint: string | null;
  default_branch: string | null;
  config?: DatasourceConfig;
  job_id: string | null;
  /** Creator id is used only to gate owner/admin management actions in Cockpit. */
  created_by?: string | null;
  /** Whether the datasource is visible to all users (vs owner/project only). */
  is_global?: boolean;
  /** Declared read-only flag for public datasources (null = not applicable).
   *  Declarative — credentials are the enforcement boundary. */
  read_only?: boolean | null;
  /** Execution-context restriction. `all` includes projectless work. */
  scope_mode?: DatasourceScopeMode;
  /** Owner-specific creation default; never a runtime force-attachment. */
  auto_attach?: boolean;
  /** Optimistic-concurrency token for scope/default and project-link edits. */
  policy_revision?: number;
  /** Management reads expose the complete set only to an authorized manager. */
  project_ids?: string[];
  /** Safe catalog summary when full project associations are redacted. */
  project_count?: number;
  /** Client-only live-session placeholder for a persisted attachment that is
   * no longer returned by the current eligibility read. */
  unavailable?: boolean;
  created_at: string;
  updated_at: string;
}

/** Execution-context row. Consumers must seed from `default_selected`, not
 * from `auto_attach`: shared/public connectors can be automatic for their
 * owner while remaining manual for the current caller. */
export interface EligibleDatasource extends Datasource {
  default_selected: boolean;
}

export interface DatasourceCatalogResponse {
  items: Datasource[];
  next_cursor: string | null;
}

export interface DatasourceCatalogFilters {
  q?: string;
  type?: DatasourceType;
  project_id?: string;
  scope_mode?: DatasourceScopeMode;
  auto_attach?: boolean;
  visibility?: 'private' | 'public';
  ownership?: 'mine' | 'shared';
  availability?: 'all' | 'projects' | 'unavailable';
  cursor?: string;
  limit?: number;
}

/**
 * Request body for creating a new datasource.
 */
export interface DatasourceCreateRequest {
  name: string;
  type: DatasourceType;
  connection_url?: string;
  description?: string;
  credentials?: Record<string, unknown>;
  cli_hint?: string;
  default_branch?: string;
  config?: DatasourceConfig;
  /** Publish org-wide; requires the public_datasources capability. */
  is_global?: boolean;
  /** Declared read-only flag; defaults to true server-side on publish. */
  read_only?: boolean;
  scope_mode?: DatasourceScopeMode;
  project_ids?: string[];
  auto_attach?: boolean;
}

/**
 * Request body for updating a datasource.
 */
export interface DatasourceUpdateRequest {
  name?: string;
  description?: string;
  connection_url?: string;
  credentials?: Record<string, unknown>;
  cli_hint?: string;
  default_branch?: string;
  config?: DatasourceConfig;
  /** Publish (true) / unpublish (false); publishing requires the capability. */
  is_global?: boolean;
  /** Declared read-only flag (kb: always true). */
  read_only?: boolean;
  scope_mode?: DatasourceScopeMode;
  project_ids?: string[];
  auto_attach?: boolean;
  /** Required whenever the policy or project set is changed. */
  policy_revision?: number;
}

/** Project option returned by the connector-policy target picker. */
export interface LinkableDatasourceProject {
  id: string;
  name: string;
  is_default?: boolean;
  user_role: ProjectMemberRole | null;
  addable: boolean;
  retained_only: boolean;
  linked: boolean;
  selected?: boolean;
}

export interface LinkableDatasourceProjectsResponse {
  items: LinkableDatasourceProject[];
  /** Every existing link, independent of the search/page in `items`. */
  selected_items?: LinkableDatasourceProject[];
  next_cursor: string | null;
}

/** Cursor page of connectors that may be newly linked to one target project.
 * The server excludes existing links, native project knowledge connectors,
 * and rows the caller is not authorized to add. */
export interface LinkableProjectDatasourcesResponse {
  items: Datasource[];
  next_cursor: string | null;
}

export interface LinkableProjectDatasourceFilters {
  q?: string;
  cursor?: string;
  limit?: number;
}

/**
 * Datasource linked to a project, with project-level overrides.
 */
export interface ProjectDatasource extends Datasource {
  linked_at: string;
  project_read_only: boolean | null;
  project_description: string | null;
}

/**
 * Result from testing a datasource connection.
 */
export interface DatasourceTestResult {
  status: 'ok' | 'error';
  message: string;
}

/**
 * Response from POST /api/datasources/ssh-keys/generate.
 *
 * `private_key` is dropped into the SSH key textarea, `public_key` is shown
 * to the user so they can add it as a deploy key on their provider.
 */
export interface SSHKeyGenerateResponse {
  private_key: string;
  public_key: string;
}

// =============================================================================
// User Models
// =============================================================================

/**
 * User identity with optional email for session-based auth.
 */
export interface User {
  id: string;
  display_name: string;
  avatar_color: string;
  email?: string | null;
  default_project_id?: string | null;
  is_admin?: boolean;
  is_approved?: boolean;
  approved_at?: string | null;
  approved_by?: string | null;
  can_use_vm?: boolean;
  created_at: string;
}

/**
 * Response body from /api/admin/system-settings/vm_workspaces (GET/PUT).
 */
export interface VmWorkspacesSetting {
  enabled: boolean;
  updated_at: string | null;
  updated_by: string | null;
}

// =============================================================================
// API Key Models
// =============================================================================

/**
 * Supported LLM and tool provider slugs for API key management.
 */
export type ApiKeyProvider = 'openai' | 'anthropic' | 'google' | 'groq' | 'openrouter' | 'mistral' | 'codex' | 'vision';

/**
 * An API key entry (as returned by GET endpoints — no full key, prefix only).
 */
export interface ApiKeyEntry {
  id: string;
  provider: ApiKeyProvider;
  key_prefix: string;
  label: string | null;
  created_at: string;
  updated_at: string;
}

/**
 * Request body for setting an API key.
 */
export interface ApiKeySetRequest {
  api_key: string;
  label?: string | null;
}

// =============================================================================
// LLM Endpoint Models
// =============================================================================

/**
 * Which slot a model fills. Chat is the default; non-chat rows are routed
 * to the matching Admin → Defaults selector (embedding/rerank/vision/whisper/tts)
 * or used as auxiliary LLMs for memory extraction / curation / title gen.
 */
export type LlmModelCapability =
  | 'chat'
  | 'vision'
  | 'embedding'
  | 'auxiliary'
  | 'whisper'
  | 'tts'
  | 'search'
  | 'fetch'
  | 'rerank';

/**
 * A user-registered OpenAI-compatible LLM endpoint. Models attached to this
 * endpoint live in the catalog (`models` table) and are managed via Admin →
 * Models, not nested here. The `models` field is kept for shape compat with
 * the legacy serializer and is always an empty array.
 */
export interface LlmEndpoint {
  id: string;
  label: string;
  base_url: string;
  key_prefix: string | null;
  /**
   * Stable routing marker from `llm_endpoints.transport_kind`.
   * `'subscription-proxy'` marks the shared CLIProxyAPI deployment that fronts
   * connected subscription accounts. Branch on this — never on `label`, which
   * an admin may rename, and never on the hostname.
   */
  transport_kind: string | null;
  created_at: string | null;
  updated_at: string | null;
  models: never[];
  /** Who wrote the row last: the `llm.seed` Job, an admin, or an image-shipped seeder. */
  source?: HelmProvenanceSource | null;
  /** Declared with `reconcile: true` in Helm values — re-applied on every `helm upgrade`. */
  managed_by_helm?: boolean;
  /** Managed, but last edited in the Cockpit: the next upgrade reverts it. */
  helm_drift?: boolean;
}

/** `source` column shared by the Helm-reconciled provider tables. */
export type HelmProvenanceSource = 'helm' | 'ui' | 'default';

export interface LlmEndpointCreateRequest {
  label: string;
  base_url: string;
  api_key?: string | null;
  allow_insecure?: boolean;
}

export interface LlmEndpointUpdateRequest {
  label?: string | null;
  base_url?: string | null;
  api_key?: string | null;
  clear_api_key?: boolean;
  allow_insecure?: boolean;
}

/**
 * Response from an endpoint test-connection probe (server-side
 * `GET {base_url}/models`). Used by Admin → Providers.
 */
export interface LlmEndpointTestResult {
  ok: boolean;
  status: number | null;
  error: string | null;
  probe_url: string;
}

/**
 * A single model surfaced by `GET {base_url}/models` via the admin discovery
 * endpoint. Used by Admin → Models as a quick-fill helper when the admin has
 * picked a system endpoint as the catalog row's transport.
 *
 * `capability_hints` is the array of suggested capabilities — chat-capable
 * rows always include `auxiliary`.
 */
export interface LlmEndpointDiscoveredModel {
  id: string;
  owned_by: string | null;
  capability_hints: LlmModelCapability[];
  family: string | null;
  context_window: number | null;
}

export interface LlmEndpointDiscoveryResult {
  ok: boolean;
  status: number | null;
  error: string | null;
  probe_url: string;
  models: LlmEndpointDiscoveredModel[];
}

/**
 * One row in the Admin → Providers post-save discovery dialog. Comes from
 * the orchestrator's `discover_models()` per-provider clients (OpenAI,
 * Google, Groq, OpenRouter); Anthropic + the env-bridge `vision` provider
 * never produce these — admins add those models manually.
 */
export interface DiscoveryCandidate {
  model_id: string;
  detected_family: string;
  /** True iff `detected_family` has a non-default entry in
   * `model_config_matrix.yaml` — i.e. we ship custom prompts/settings. The
   * dialog defaults supported rows to checked. */
  supported: boolean;
  /** Suggested capabilities array. Chat-capable models include `auxiliary`
   * automatically; multimodal chat families also include `vision`. */
  suggested_capabilities: CatalogCapability[];
  suggested_display_label: string;
}

/** Wrapper stored on `system_api_keys.discovery_cache_json`. */
export interface DiscoveryPayload {
  provider: string;
  fetched_at: string;
  count: number;
  candidates: DiscoveryCandidate[];
}

/** Response shape for `GET /api/admin/providers/keys/{provider}/discovery`
 * and `POST .../rediscover`. ``ready=false`` means no probe has completed
 * yet (e.g. the async post-save probe is still in flight). */
export interface DiscoveryResponse {
  ready: boolean;
  fresh: boolean;
  payload: DiscoveryPayload | null;
  cached_at: string | null;
}

// =============================================================================
// Models Catalog (Admin → Models)
// =============================================================================

/** Locked enum for catalog rows. Adding a capability requires schema + resolver work. */
export type CatalogCapability =
  | 'chat'
  | 'auxiliary'
  | 'embedding'
  | 'vision'
  | 'whisper'
  | 'tts'
  | 'search'
  | 'fetch'
  | 'rerank';

/** Provider anchor for a catalog row. */
export type CatalogProviderKind = 'system' | 'endpoint';

export const CATALOG_CAPABILITIES: CatalogCapability[] = [
  'chat', 'auxiliary', 'embedding', 'vision', 'whisper', 'tts', 'search', 'fetch',
  'rerank',
];

/**
 * A row in the admin-curated `models` table. Mirrors orchestrator
 * `_serialize_catalog_model`.
 *
 * `capabilities` is the source of truth: a single row can claim multiple
 * roles — e.g. `['chat','auxiliary']` for a chat-capable LLM,
 * `['chat','auxiliary','vision']` for a multimodal one.
 */
export interface CatalogModel {
  id: string;
  provider_kind: CatalogProviderKind;
  provider_ref: string;
  model_id: string;
  display_label: string;
  capabilities: CatalogCapability[];
  family: string;
  context_window: number | null;
  /** Effective window: explicit `context_window` when set, else the family default. Read-only, computed server-side. */
  resolved_context_window: number;
  /** Whether `resolved_context_window` came from the explicit cap or the inherited family default. */
  context_window_source: 'explicit' | 'family_default';
  reasoning_level: string | null;
  params_json: Record<string, unknown> | null;
  enabled: boolean;
  seeded_from: string | null;
  notes: string | null;
  created_at: string | null;
  updated_at: string | null;
  source?: HelmProvenanceSource | null;
  managed_by_helm?: boolean;
  helm_drift?: boolean;
}

export interface CatalogModelCreateRequest {
  provider_kind: CatalogProviderKind;
  provider_ref: string;
  model_id: string;
  display_label: string;
  /** Capabilities the row claims. Required, non-empty. */
  capabilities: CatalogCapability[];
  family: string;
  context_window?: number | null;
  reasoning_level?: string | null;
  params_json?: Record<string, unknown> | null;
  enabled?: boolean;
  notes?: string | null;
}

export interface CatalogModelUpdateRequest {
  provider_kind?: CatalogProviderKind;
  provider_ref?: string;
  model_id?: string;
  display_label?: string;
  capabilities?: CatalogCapability[];
  family?: string;
  context_window?: number | null;
  reasoning_level?: string | null;
  params_json?: Record<string, unknown> | null;
  enabled?: boolean;
  notes?: string | null;
}

export interface CatalogModelTestResult {
  ok: boolean;
  status: number | null;
  error: string | null;
  probe_url: string | null;
}

export interface CatalogModelDeleteResult {
  status: string;
  id: string;
  warning: string | null;
}

/**
 * Resolved default values for every user preference field.
 * Returned by the backend in the `_resolved` key of GET /api/settings/preferences.
 */
export interface ResolvedDefaults {
  default_model?: string;
  default_autonomy?: string;
  default_reasoning_level?: string;
  default_auxiliary_model?: string;
  default_vision_model?: string;
  default_whisper_model?: string;
  default_tts_model?: string;
  default_embedding_model?: string;
  embedding_provider?: string;
  persistent_agent?: {
    model?: string;
    permission_mode?: string;
    idle_timeout_minutes?: number;
    workspace_backend?: string;
  };
}

/**
 * User preference settings.
 */
export interface UserSettings {
  default_model?: string | null;
  default_autonomy?: string | null;
  default_reasoning_level?: string | null;
  default_auxiliary_model?: string | null;
  default_vision_model?: string | null;
  default_whisper_model?: string | null;
  default_tts_model?: string | null;
  /** User's chosen read-aloud voice (overrides the admin/per-language default). */
  default_tts_voice?: string | null;
  default_embedding_model?: string | null;
  embedding_provider?: string | null;
  language?: 'en' | 'de-DE' | null;
  /** Admin "View as" scope: 'all' = fleet-wide (default), 'me' = own data only. */
  admin_view_mode?: 'me' | 'all' | null;
  communication?: CommunicationSettings | null;
  persistent_agent?: PersistentAgentSettings | null;
  /** How the aux LLM rewrites messages for read-aloud (reasoning + custom prompt). */
  read_aloud?: ReadAloudSettings | null;
  _resolved?: ResolvedDefaults;
}

/** A read-aloud reasoning level. 'off' (default) keeps the fast rewrite path;
 * the rest turn the aux model's thinking on (or set its effort). */
export type ReadAloudReasoningLevel = 'off' | 'low' | 'medium' | 'high';

/**
 * Read-aloud rewrite preferences (stored under users.settings.read_aloud, read by
 * orchestrator/services/tts.py). `reasoning_level` trades latency for a smarter
 * rewrite (off by default); `custom_prompt` is the user's own standing
 * instructions ("skip tables", "give me a TLDR", "omit code file names") — it
 * outranks the default rules, so it can summarize/omit, capped at 1000 chars.
 */
export interface ReadAloudSettings {
  reasoning_level?: ReadAloudReasoningLevel | null;
  custom_prompt?: string | null;
}

/**
 * User settings for persistent agent sessions.
 */
export interface PersistentAgentSettings {
    model?: string | null;
    permission_mode?: string | null;
    /** Default session workspace tier; null tracks the system default (virtual). */
    workspace_backend?: 'virtual' | 'sandbox' | 'none' | null;
    idle_timeout_minutes?: number | null;
    // Headless controls. The backend reads these as direct children of
    // users.settings.persistent_agent (orchestrator/main.py create_thread
    // merge + attention_sleep_sweeper COALESCE).
    headless_mode?: 'eager' | 'polite' | null;
    headless_attention_sleep_minutes?: number | null;
}

/**
 * Codex proxy status (admin-only, from CLIProxyAPI management API).
 */
/**
 * Everything Settings → AI Subscriptions renders, from
 * `GET /api/subscriptions/status`. Reachability and authentication are
 * separate: an unreachable proxy is "not enabled", not "signed out".
 */
export interface SubscriptionsStatus {
  reachable: boolean;
  connected: boolean;
  proxy_url: string | null;
  error: string | null;
  accounts: SubscriptionAccount[];
  model_count: number;
  providers: SubscriptionProviderInfo[];
}

/** One connectable subscription product, from the orchestrator's registry. */
export interface SubscriptionProviderInfo {
  key: string;
  label: string;
  vendor: string;
  /** `browser` opens an authorization page; `device` shows a code to enter. */
  login_flow: 'browser' | 'device';
  channels: string[];
  client_protocol: string;
  has_usage_reader: boolean;
  /** True only where SRW has live-verified inference through this product. */
  inference_verified: boolean;
  notes: string[];
  connected_accounts: number;
}

/** Connection state of one account. Mirrors what the proxy actually reports. */
export type SubscriptionAccountState =
  | 'connected'
  | 'pending'
  | 'refreshing'
  | 'cooldown'
  | 'error'
  | 'disabled'
  | 'unknown';

/** One connected credential, with every secret stripped server-side. */
export interface SubscriptionAccount {
  account_id: string;
  provider: string | null;
  channel: string | null;
  label: string | null;
  email: string | null;
  account_type: string | null;
  state: SubscriptionAccountState;
  state_detail: string | null;
  next_retry_after: string | null;
  updated_at: string | null;
  last_refresh: string | null;
  /** `installation` — shared, admin-managed. Private accounts are future work. */
  scope: string;
}

/** An in-flight authorization attempt. The upstream OAuth state stays server-side. */
export interface SubscriptionLogin {
  login_id: string;
  provider: string;
  flow: 'browser' | 'device';
  /**
   * `verifying` means upstream reported completion but SRW has not yet seen the
   * credential appear — it is deliberately not shown as success.
   */
  status: 'pending' | 'verifying' | 'connected' | 'failed' | 'cancelled';
  error: string | null;
  auth_url: string;
  user_code: string | null;
  expires_at: string;
  account_id: string | null;
  accepts_callback_url: boolean;
}

/** Per-account usage. `available: false` carries a reason, never a fake zero. */
export interface SubscriptionUsage {
  available: boolean;
  reason?: string;
  provider?: string | null;
  account_id?: string;
  account?: string | null;
  plan_type?: string | null;
  limit_reached?: boolean;
  primary?: CodexUsageWindow | null;
  secondary?: CodexUsageWindow | null;
  per_model?: {
    name: string;
    primary: CodexUsageWindow | null;
    secondary: CodexUsageWindow | null;
  }[];
  credits?: { has_credits: boolean; unlimited: boolean; balance?: string | null } | null;
}

/** How much SRW knows about a discovered model. */
export type SubscriptionModelSupport = 'supported' | 'unsupported_modality' | 'needs_review';

/** One row of the subscription Discover list. */
export interface SubscriptionDiscoveredModel {
  id: string;
  display_label: string;
  owned_by: string | null;
  /** Upstream channels that can serve it — the source attribution. */
  sources: string[];
  providers: string[];
  account_ids: string[];
  client_protocol: string | null;
  context_window: number | null;
  max_output_tokens: number | null;
  family: string | null;
  capability_hints: string[];
  support: SubscriptionModelSupport;
  support_reason: string | null;
  registered: boolean;
  catalog_id: string | null;
  routing_drift: boolean;
}

/** Result of a subscription-aware endpoint discovery. */
export interface SubscriptionDiscoveryResult {
  subscription: true;
  ok: boolean;
  probe_url: string;
  error: string | null;
  models: SubscriptionDiscoveredModel[];
  unreadable_account_ids: string[];
  /** False when enrichment failed — sources are unknown, not absent. */
  attribution_complete: boolean;
}

/** Outcome of a bulk catalog import. */
export interface SubscriptionImportResult {
  created: string[];
  skipped: string[];
  rejected: { id: string; reason: string }[];
}

export interface CodexStatus {
  connected: boolean;
  /**
   * Whether the codex-proxy deployment is reachable at all. False when the
   * proxy is disabled (e.g. `codexProxy.enabled: false`) or down — the UI
   * shows an "enable it" disclaimer instead of a Connect button that 502s.
   */
  reachable: boolean;
  accounts: { name: string; status: string; status_message?: string }[];
  model_count: number;
}

/** One Codex rate-limit window (5-hour or weekly), from ChatGPT `wham/usage`. */
export interface CodexUsageWindow {
  used_percent: number | null;
  window_seconds: number | null;
  reset_after_seconds: number | null;
  reset_at: number | null;
}

/**
 * Codex subscription usage / rate-limit windows (admin-only), fetched from the
 * ChatGPT backend via the codex proxy. Powers the capacity bars in
 * Settings → Codex. `available: false` when the proxy is down, no account is
 * connected, or the backend is unreachable (the OAuth token stays server-side —
 * only these aggregates cross to the UI).
 */
export interface CodexUsage {
  available: boolean;
  account?: string | null;
  plan_type?: string | null;
  limit_reached?: boolean;
  /** 5-hour rolling "session" window. */
  primary?: CodexUsageWindow | null;
  /** 7-day "weekly" window. */
  secondary?: CodexUsageWindow | null;
  per_model?: {
    name: string;
    primary: CodexUsageWindow | null;
    secondary: CodexUsageWindow | null;
  }[];
  credits?: { has_credits: boolean; unlimited: boolean; balance?: string | null } | null;
}

/**
 * Communication delivery preferences (Phase 3 Live Communication).
 */
export interface CommunicationSettings {
  delivery?: {
    async_reply?: 'immediate_interrupt' | 'next_strategic_phase' | 'llm_triage';
    urgent_override?: boolean;
  };
  channels?: {
    email?: boolean;
    cockpit?: boolean;
    ntfy?: boolean;
    slack_webhook?: boolean;
    discord_webhook?: boolean;
  };
  quiet_hours?: {
    enabled?: boolean;
    start?: string;
    end?: string;
    timezone?: string;
  };
}

/**
 * An external contact linked to a project.
 */
export interface ExternalContact {
  id: string;
  display_name: string;
  email: string;
  created_at: string | null;
}

// =============================================================================
// MCP Token Models
// =============================================================================

/**
 * An MCP API token (as returned by GET /api/mcp-tokens — no plaintext).
 */
export interface McpToken {
  id: string;
  name: string;
  token_prefix: string;
  scope: string;
  origin: string | null;
  expires_at: string | null;
  revoked_at: string | null;
  last_used_at: string | null;
  created_at: string;
}

/**
 * Request body for creating an MCP token.
 */
export interface McpTokenCreateRequest {
  name: string;
  scope?: string;
  expires_in_days?: number | null;
}

/**
 * Response from POST /api/mcp-tokens — includes plaintext token (shown once).
 */
export interface McpTokenCreateResponse extends McpToken {
  token: string;
}

// =============================================================================
// Project Models
// =============================================================================

/**
 * Project lifecycle. `active | archived` is the whole vocabulary the API
 * accepts and `GET /api/projects?status=` filters on; `deleted` used to be
 * listed here but the DB CHECK rejects it (deletion is a hard row delete), so
 * nothing could ever carry it.
 */
export type ProjectStatus = 'active' | 'archived';

/**
 * What archiving actually did. Archiving never refuses because children are in
 * flight — it quiesces them and reports what it touched, so the UI can say so
 * instead of leaving the user to discover a paused loop later.
 */
export interface ProjectArchiveReport {
  archived: boolean;
  loop_paused?: boolean;
  officer_held?: boolean;
  jobs_parked?: number;
}

/**
 * Project member role types.
 */
export type ProjectMemberRole = 'owner' | 'editor' | 'viewer';

/**
 * Project repository role types.
 */
/** `jobs` is legacy-read-only; `knowledge` is orchestrator-managed. */
export type ProjectRepoRole = 'jobs' | 'source' | 'reference' | 'knowledge';

/**
 * Project from the orchestrator.
 */
export interface Project {
  id: string;
  name: string;
  description?: string | null;
  goal?: string | null;
  status: ProjectStatus;
  is_default: boolean;
  default_config_name?: string | null;
  default_config_override?: Record<string, unknown> | null;
  nextcloud_folder_id?: number | null;
  cloud_storage_read_only?: boolean;
  cloud_storage_url?: string | null;
  main_cloud_backend?: string | null;
  network_tier?: ProjectNetworkTier;
  created_at: string;
  updated_at: string;
  job_count?: number;
  repo_count?: number;
  member_count?: number;
  /** Present on membership-aware project reads. */
  user_role?: ProjectMemberRole;
}

/** Status of a project self-improvement loop. */
export type ProjectLoopStatus =
  | 'running'
  | 'paused'
  | 'stopped'
  | 'completed'
  | 'failed';

/**
 * A multi-job campaign a campaign-scheduled loop's checkpoint critic filed
 * via `loop_plan`. Lives in `project_loops.campaign` (JSONB); shape is what
 * `_advance_planner_campaign` writes (orchestrator/main.py).
 * See knowledge-base/knowledge/features/loop_campaign_scheduling.md.
 */
export interface LoopCampaign {
  /** Deterministic: the plan-filing critic job's id. */
  id: string;
  plan_job_id: string;
  /** KB note carrying the initiative the campaign invests in. */
  initiative_note_id: string;
  title: string;
  /** Ordered execution stages; each spawns one job of that role. */
  stages: {role: string}[];
  /** Pre-registered acceptance checks the closing critic judges against. */
  acceptance: string[];
  /** Next stage index to spawn (the running stage is cursor - 1). */
  cursor: number;
  /** Stages that finished (success or failure). */
  stages_done: number;
  /** Consecutive failed members; hitting the cap aborts the campaign. */
  member_failures: number;
  extensions_used: number;
  /** active → running stages; review/aborted → awaiting the critic's verdict. */
  status: 'active' | 'review' | 'aborted';
}

/** A disposed campaign, archived on `project_loops.campaign_history` (newest last). */
export interface LoopCampaignHistoryEntry {
  id: string | null;
  initiative_note_id: string | null;
  title: string | null;
  stages_total: number;
  stages_done: number | null;
  extensions_used: number | null;
  status_at_close: string | null;
  /** The critic's verdict: ship (done), extend (new campaign, same initiative), kill. */
  outcome: 'ship' | 'extend' | 'kill';
  notes: string | null;
  disposed_by: string;
}

/**
 * A project self-improvement loop — the control row that runs jobs one at a
 * time, rotating role_sequence (e.g. scholar→critic→developer) until a
 * budget (max_iterations / run_until / consecutive-failure cap) stops it.
 * Mirrors the project_loops table. See knowledge-base/knowledge/features/project_self_improvement_loop.md.
 */
export interface ProjectLoop {
  id: string;
  project_id: string;
  owner_id: string | null;
  status: ProjectLoopStatus;
  goal: string | null;
  acceptance_criteria: string | null;
  user_prompt: string | null;
  model: string | null;
  /** Per-loop workspace tier for every spawned job (null = default sandbox). */
  workspace_backend: string | null;
  /**
   * Rotation of stages. A plain string is a one-job stage; a nested array is a
   * parallel *fan-out* stage whose analysis roles run concurrently and barrier
   * before the loop rotates (e.g. `[["scholar","product-qa"], "critic"]`).
   */
  role_sequence: (string | string[])[];
  seq_index: number;
  max_iterations: number | null;
  remaining_iterations: number | null;
  run_until: string | null;
  max_consecutive_failures: number;
  current_job_id: string | null;
  /**
   * Job ids of the in-flight fan-out stage. Populated only while a parallel
   * stage is running (single-role stages use `current_job_id` and leave this
   * empty). Drives the stage-job chips in the live panel.
   */
  current_stage_jobs?: string[];
  total_jobs_run: number;
  consecutive_failures: number;
  last_error: string | null;
  stop_reason: string | null;
  /**
   * Execution-slot scheduling: 'standard' (default — one job per turn) or
   * 'campaign' (the checkpoint critic may expand the execution slot into a
   * multi-stage campaign). Start-time only. loop_campaign_scheduling.md.
   */
  scheduling?: 'standard' | 'campaign';
  /** The live campaign on a campaign-scheduled loop (null between campaigns). */
  campaign?: LoopCampaign | null;
  /** Disposed campaigns, newest last (bounded server-side). */
  campaign_history?: LoopCampaignHistoryEntry[];
  /** Per-loop guardrail overrides ({max_stages, max_extensions, abort_failures}). */
  campaign_caps?: Record<string, number> | null;
  created_at: string;
  updated_at: string;
}

/** Request body for starting a project loop (POST /projects/{id}/loop). */
export interface ProjectLoopStartRequest {
  model?: string | null;
  /** Workspace tier for every spawned job: 'sandbox' | 'vm' | 'virtual' | 'none'. */
  workspace_backend?: string | null;
  /** Stage rotation; a nested array entry is a concurrent analysis fan-out. */
  role_sequence?: (string | string[])[] | null;
  max_iterations?: number | null;
  run_until?: string | null;
  acceptance_criteria?: string | null;
  user_prompt?: string | null;
  goal_override?: string | null;
  max_consecutive_failures?: number;
  /**
   * 'campaign' lets the checkpoint critic file multi-job campaigns; requires
   * exactly one single-role critic step followed by a single-role step
   * (validated server-side; mirrored in plannerIneligibility client-side).
   */
  scheduling?: 'standard' | 'campaign';
}

// =============================================================================
// Project Backlog (loop ticket pool) —
// knowledge-base/knowledge/superpowers/specs/2026-07-26-project-backlog-pipeline-design.md
// =============================================================================

/**
 * Priority label for a backlog ticket. Always a word on the wire — the 0/1/2
 * storage rank (`orchestrator/services/project_backlog.py::PRIORITY_WORDS`) is
 * a server-side implementation detail the cockpit never sees. A label only:
 * it sorts what's shown, nothing gates or reorders work because of it.
 */
export type BacklogPriority = 'high' | 'normal' | 'low';

/** One ticket in the project's backlog pool (GET /projects/{id}/backlog). */
export interface BacklogItem {
  note_id: string;
  note_type: 'feature' | 'issue' | 'idea';
  title: string;
  priority: BacklogPriority;
}

/**
 * The project's ticket pool, as shown to the user. `items` is capped
 * server-side (200, priority-then-age order) but `counts`/`total` are NOT —
 * for a large pool they can exceed `items.length`, which is why the cockpit
 * must render the counts and not just the list (a capped list otherwise
 * hides its own tail). `in_progress` is the active loop's current campaign
 * initiative — excluded from `items` so it's never shown twice; null when no
 * campaign is running.
 */
export interface ProjectBacklog {
  total: number;
  counts: Record<BacklogPriority, number>;
  in_progress: { note_id: string; title: string } | null;
  items: BacklogItem[];
}

// =============================================================================
// Officer post (knowledge-base/knowledge/features/officer_post.md §8) — `project_officers` row +
// runtime projection. GET /projects/{id}/officer ALWAYS returns the post,
// vacant or commissioned; the card never infers existence from a 404.
// =============================================================================

/** One typed worker allocation of the officer's kit (request shape). */
export interface OfficerSlotSpec {
  count: number;
  model?: string;
  backend?: string;
  /**
   * Work category this slot is a POOL for — researcher | tester | executor.
   * A slot with one may be filled automatically from ready, categorized
   * tickets; a slot without one stays officer-directed capacity and the
   * auto-pull tick never touches it (officer_backlog_pools.md §6).
   */
  category?: string;
  /** Optional per-slot daily USD ceiling. Unset means no mechanical brake. */
  spend_ceiling_daily?: number;
}

/**
 * A kit slot as the GET reports it: allocation plus live utilization.
 * `in_flight` is lineage-aware (counts jobs of prior incarnations too, §4).
 * `ready_depth` / `below_floor` are pool-only and ABSENT when the knowledge
 * base could not be read — absent means unknown, never zero.
 */
export interface OfficerKitSlot extends OfficerSlotSpec {
  in_flight?: number;
  ready_depth?: number;
  below_floor?: boolean;
}

/** An open pool circuit breaker, as the tick recorded it. */
export interface OfficerPoolBreaker {
  until?: string;
  cause?: string;
  tickets?: string[];
  tripped_on_job?: string;
}

/** A ticket whose claiming job has stopped moving. Never auto-released. */
export interface OfficerStaleClaim {
  job_id?: string;
  ticket_note_id?: string;
  slot?: string | null;
  status?: string;
  age_hours?: number;
}

export interface OfficerProvisioningPreflight {
  id: string;
  status?: string;
  error_message?: string | null;
  context?: {
    provisioning_preflight?: {
      state?: 'not-attempted' | 'in-progress' | 'retryable-failed' | 'permanent-failed' | 'activated';
      phase?: string | null;
      failure_class?: string | null;
      error?: string | null;
      next_retry_at?: string | null;
    };
  };
}

export interface OfficerKnowledgeMaterialization {
  id: string;
  note_id: string;
  canonical_state: 'pending_sync' | 'canonical' | 'failed' | 'superseded';
  projection_state: 'pending' | 'synced' | 'failed' | 'projection_only';
  retry_state: 'none' | 'retryable' | 'permanent';
  last_error_class?: string | null;
  last_error?: string | null;
  next_retry_at?: string | null;
}

export interface OfficerFloorWakeOutcome {
  id: string;
  pool: string;
  state: 'pending' | 'retryable' | 'queued' | 'delivered' | 'permanent_failed' | 'superseded';
  attempt_count: number;
  last_attempted_at?: string | null;
  last_queued_at?: string | null;
  delivered_at?: string | null;
  failure_class?: string | null;
  last_error?: string | null;
  next_retry_at?: string | null;
}

/** Backlog-pool policy state: what the tick enforces, made visible (§6). */
export interface OfficerBacklogState {
  auto_pull: boolean;
  /** Deployment-owned release fence for the unattended enable transition. */
  auto_pull_control?: {
    enable_available: boolean;
    source: 'deployment_policy';
    reason?: 'release_gate_closed' | null;
  };
  breakers: Record<string, OfficerPoolBreaker>;
  stale_claims: OfficerStaleClaim[];
  stale_claim_policy?: {
    threshold_minutes: number;
    threshold_source: 'deployment_default' | 'request_override';
  };
  worker_spend_ceiling_daily?: number | null;
  provisioning_preflights?: OfficerProvisioningPreflight[];
  knowledge_materialization?: OfficerKnowledgeMaterialization[];
  floor_wakes?: OfficerFloorWakeOutcome[];
}

/** Standing-down marker on a commissioned post (maintenance or conference). */
export interface OfficerHold {
  kind?: string | null;
  since?: string | null;
  note?: string | null;
}

/** Live runtime projection — non-null only while the post is commissioned. */
export interface OfficerLive {
  thread_id: string;
  status: string;
  title?: string | null;
  created_at?: string | null;
  model?: string | null;
  reasoning_level?: string | null;
  sleep_minutes?: {min: number; max: number} | null;
  next_wake_at?: string | null;
  pending_events?: number;
  token_ceiling?: {daily: number; deferred_today?: boolean} | null;
  conference?: {thread_id: string; status?: string | null} | null;
  /** Not yet in the O1–O4 contract; optional so the editor seeds them when the backend adds them. */
  max_actions_per_wake?: number | null;
  max_concurrent_workers?: number | null;
}

/** One entry of the append-only incarnation log (old logs stay readable as ended sessions). */
export interface OfficerIncarnation {
  thread_id: string;
  commissioned_at?: string | null;
  decommissioned_at?: string | null;
  reason?: string | null;
}

/** User-owned routing for worker questions (officer_message_routing.md §6). */
export type WorkerMessagesPolicy =
  | 'user_direct'
  | 'officer_and_user'
  | 'officer_first';

export interface OfficerCommunicationPolicy {
  worker_messages?: WorkerMessagesPolicy;
  officer_response_minutes?: number;
}

/** Terminal-status entries recorded while the post was vacant (ring, cap 20). */
export interface OfficerVacantLedger {
  entries?: {
    at?: string | null;
    job_id?: string | null;
    status?: string | null;
    title?: string | null;
  }[];
  dropped?: number;
}

/** Server-owned 24/7 runtime authorization health; no credential material. */
export interface OfficerRuntimeAuthorization {
  status: 'authorized' | 'unavailable' | 'not_applicable';
  failure_class?: string | null;
  since?: string | null;
  last_attempted_at?: string | null;
  next_retry_at?: string | null;
  recovered_at?: string | null;
  operator_notification?: 'pending' | 'delivered' | 'failed' | string;
  planning_suppressed?: boolean;
}

/** `GET /api/projects/{id}/officer` — the post, always present. */
export interface OfficerPost {
  /** Server-owned owner/admin capability for Officer mutations. */
  can_manage: boolean;
  commissioned: boolean;
  held?: OfficerHold | null;
  officer?: OfficerLive | null;
  kit?: Record<string, OfficerKitSlot> | null;
  spend_today?: {tokens?: number; ceiling?: number | null} | null;
  communication_policy?: OfficerCommunicationPolicy | null;
  incarnations?: OfficerIncarnation[] | null;
  while_vacant?: OfficerVacantLedger | null;
  runtime_authorization?: OfficerRuntimeAuthorization | null;
  runtime_lifecycle?: {
    observed_build_sha?: string | null;
    expected_build_sha?: string | null;
    drift_state?: 'current' | 'drifted' | 'missing' | 'unknown';
    recycle_phase?: string | null;
    last_failure?: string | null;
    automatic_reconciliation_enabled?: boolean;
  } | null;
  backlog?: OfficerBacklogState | null;
  conference?: {thread_id: string; status?: string | null} | null;
}

/** The officer's own brain (distinct from the slot models his workers run on). */
export interface OfficerBrainSpec {
  model?: string | null;
  reasoning_level?: string | null;
}

/**
 * PATCH /api/projects/{id}/officer — partial edit of the post; commission
 * accepts the same fields as its optional config body. `null` clears a field
 * (e.g. `slots: null` = flat cap). `communication_policy` is the row-only,
 * user-owned field — never mirrored into thread metadata.
 */
export interface OfficerPostPatch {
  slots?: Record<string, OfficerSlotSpec> | null;
  /** Let the tick fill categorized pools. Ships off; flipped per century. */
  auto_pull?: boolean | null;
  /** Optional century-wide daily USD ceiling on worker spend. */
  worker_spend_ceiling_daily?: number | null;
  max_concurrent_workers?: number | null;
  max_actions_per_wake?: number | null;
  daily_token_ceiling?: number | null;
  sleep_min_minutes?: number | null;
  sleep_max_minutes?: number | null;
  brain?: OfficerBrainSpec | null;
  communication_policy?: OfficerCommunicationPolicy;
}

export interface OfficerCommissionResult {
  thread_id?: string;
  status?: string;
}

/** A job still running under the officer's command at decommission time. */
export interface OfficerInFlightJob {
  job_id: string;
  slot?: string | null;
  status?: string | null;
  title?: string | null;
}

/**
 * POST .../officer/decommission result. A non-forced call with jobs in flight
 * returns the warning + list INSTEAD of decommissioning; `force: true`
 * proceeds — jobs are left running either way (decommission never cancels).
 */
export interface OfficerDecommissionResult {
  status?: string;
  warning?: string | null;
  in_flight_jobs?: OfficerInFlightJob[] | null;
}

/**
 * Workspace egress tier for a project. The set must stay in sync with
 * the CHECK constraint in 0016_project_network_tier.sql and the
 * `workspace.networkPolicy.tiers` list in helm values.
 */
export type ProjectNetworkTier = 'internet-only' | 'home-allowed';

/**
 * Request body for creating a new project.
 */
export interface ProjectCreateRequest {
  name: string;
  description?: string;
  goal?: string;
  default_config_name?: string;
  default_config_override?: Record<string, unknown>;
  external_kb?: ExternalKnowledgeBaseRequest;
  user_id: string;
}

/** Which private GitHub repository backs the writable project KB.
 *
 * Exactly one form per request: `datasource_id` adopts an existing `kb`
 * connector (what the cockpit sends — the connector already holds the URL,
 * branch and PAT), or the repository and token inline (API/MCP callers).
 */
export type ExternalKnowledgeBaseRequest =
  | {datasource_id: string}
  | {
      repo_url: string;
      branch?: string;
      token: string;
      /** Required for GitHub Enterprise; github.com is inferred. */
      forge?: 'github';
    };

/**
 * Request body for updating a project.
 */
export interface ProjectUpdateRequest {
  name?: string;
  description?: string;
  goal?: string;
  status?: ProjectStatus;
  default_config_name?: string;
  default_config_override?: Record<string, unknown>;
  cloud_storage_read_only?: boolean;
  network_tier?: ProjectNetworkTier;
}

/**
 * Project member with user info.
 */
export interface ProjectMember {
  project_id: string;
  user_id: string;
  role: ProjectMemberRole;
  joined_at: string;
  display_name?: string;
  avatar_color?: string;
}

/**
 * Request body for adding a project member.
 */
export interface ProjectMemberAddRequest {
  user_id: string;
  role?: ProjectMemberRole;
}

/**
 * Request body for updating a project member's role.
 */
export interface ProjectMemberUpdateRequest {
  role: ProjectMemberRole;
}

/**
 * Project repository configuration.
 */
export interface ProjectRepository {
  id: string;
  project_id: string;
  name: string;
  description?: string | null;
  repo_url?: string | null;
  role: ProjectRepoRole;
  read_only: boolean;
  is_managed: boolean;
  branch?: string | null;
  clone_path?: string | null;
  credentials?: Record<string, unknown> | null;
  created_at: string;
  updated_at: string;
}

/**
 * Request body for creating/attaching a project repository.
 */
export interface ProjectRepositoryCreateRequest {
  name: string;
  description?: string;
  repo_url?: string;
  role?: Extract<ProjectRepoRole, 'source' | 'reference'>;
  read_only?: boolean;
  branch?: string;
  clone_path?: string;
  create_managed?: boolean;
}

/**
 * Request body for updating a project repository.
 */
export interface ProjectRepositoryUpdateRequest {
  name?: string;
  description?: string;
  read_only?: boolean;
  branch?: string;
  clone_path?: string;
}

/**
 * Request body for promoting a default-project job into a named project.
 */
export interface PromoteRequest {
  name: string;
  description?: string;
  goal?: string;
  user_id: string;
}

// =============================================================================
// Persistent Thread (Session) Models
// =============================================================================

/**
 * 'awaiting_user' (headless natural pause) and 'suspended' (workspace
 * snapshotted, pods torn down — attention-sleep or drift-drain) are both
 * live-resumable: sending a message wakes the session on a fresh agent.
 * Only 'ended' renders the resume card.
 */
export type ThreadStatus =
  | 'created'
  | 'active'
  | 'awaiting_user'
  | 'suspended'
  | 'ending'
  | 'ended';

/**
 * One remote folder attached to a thread, as projected by
 * `GET /api/persistent/threads/{id}` (orchestrator/main.py:42778-42788).
 *
 * The route has returned this since cloud_collaboration_model.md Phase 1 —
 * its docstring says it exists "so the Cockpit 'Project files' panel can
 * render them without a second round-trip" — but nothing read it until the
 * protected-cloud review needed to name the folder its diff applies to
 * (PC-19). The projection deliberately omits `cloud_handle`, so a browser
 * URL still has to come from the project record.
 *
 * Row order is the backend's `ORDER BY target_path`, and
 * `select_protected_mount` takes the *first* eligible row in that order —
 * so array order here is meaningful, not incidental.
 */
export interface ThreadMount {
  id: string;
  /** 'project' | 'project_default' | … — `project_default` is the owner's
   *  personal cloud home and is outside protected mode's safety contract. */
  mount_kind: string;
  /** Where the mount lands in the workspace; the diff summary's
   *  `protected_mount` resolves to this same value. */
  target_path: string;
  source_kind: string;
  /** Project UUID for a project mount. */
  source_ref: string | null;
  backend_id: string | null;
}

/**
 * Persistent agent session thread.
 */
export interface Thread {
  id: string;
  title: string;
  status: ThreadStatus;
  /** Absent on orchestrators predating child threads. */
  kind?: 'session' | 'subagent';
  config_name: string;
  permission_mode: string;
  user_id?: string | null;
  project_id?: string | null;
  agent_id?: string | null;
  created_at: string;
  last_activity: string;
  ended_at?: string | null;
  /** Safe, derived public projection of an in-progress pinned retirement.
   *  Raw retirement tokens/context never belong in an owner payload. */
  runtime_retirement_pending?: boolean;
  retirement_disposition?: 'ended' | 'suspended' | null;
  total_turns: number;
  total_tokens: number;
  nc_session_folder?: string | null;
  nc_share_id?: number | null;
  cloud_session_url?: string | null;
  /** Short handle used as the SSH username: `ssh s-7f3a91c2@ssh.<domain>`.
   *  Minted once at creation; null on threads predating migration 0202. */
  ssh_handle?: string | null;
  metadata?: Record<string, unknown>;
  /** Attached remote folders, ordered by `target_path`. Absent on the list
   *  endpoint's projection and on older orchestrators. */
  mounts?: ThreadMount[];
  /** Derived: `source_ref` of every `mount_kind === 'project'` row. */
  project_ids?: string[];
  /** Child-thread identity. All fields are absent on ordinary sessions and on
   *  orchestrators predating U3. */
  parent_job_id?: string | null;
  /** Session parent for U5 children. Exactly one parent id is set on a child. */
  parent_thread_id?: string | null;
  subagent_handle?: string | null;
  subagent_type?: string | null;
  subagent_status?: JobSubagentStatus | null;
  subagent_outcome?: string | null;
  subagent_error?: string | null;
  report_path?: string | null;
}

/** One row from the persistent thread transcript endpoint. */
export interface PersistentThreadMessage {
  id: string;
  role: string;
  content: string | null;
  tool_calls: Array<{
    name: string;
    args: Record<string, unknown>;
    id: string;
    decision?: string;
    category?: string;
  }> | null;
  turn_number: number | null;
  tool_call_id?: string | null;
  thinking?: string | null;
  created_at: string | null;
}

export interface PersistentThreadHistory {
  thread_id: string;
  messages: PersistentThreadMessage[];
  total: number;
  has_more: boolean;
}

// =============================================================================
// Knowledge Base Models
// =============================================================================

/**
 * Knowledge note types.
 */
export type KnowledgeNoteType =
  | 'goal'
  | 'plan'
  | 'decision'
  | 'learning'
  | 'code'
  | 'source'
  | 'question'
  | 'state'
  | 'retrospective';

/**
 * Knowledge note status types.
 */
export type KnowledgeNoteStatus = 'active' | 'resolved' | 'superseded' | 'archived';

/**
 * Knowledge note summary (list view).
 */
export interface KnowledgeNote {
  id: string;
  note_id: string;
  title: string;
  note_type: KnowledgeNoteType;
  status: KnowledgeNoteStatus;
  confidence?: string | null;
  tags: string[];
  keywords: string[];
  job_id?: string | null;
  phase?: number | null;
  content_preview?: string;
  content?: string;
  created_at: string;
  modified_at: string;
}

/**
 * Knowledge note relationship (from Neo4j).
 */
export interface KnowledgeRelationship {
  type: string;
  direction: string;
  target_id: string;
  target_title?: string;
}

/**
 * Full knowledge note detail (single note view).
 */
export interface KnowledgeNoteDetail extends KnowledgeNote {
  content: string;
  retrieval_messages?: string[];
  content_hash?: string;
  relationships: KnowledgeRelationship[];
}

/**
 * Knowledge base summary statistics.
 */
export interface KnowledgeSummary {
  total: number;
  by_type: Record<string, number>;
  by_status: Record<string, number>;
  recent: { note_id: string; title: string; note_type: string; status: string; modified_at: string }[];
}

/**
 * Paginated knowledge note list response.
 */
export interface KnowledgeListResponse {
  notes: KnowledgeNote[];
  total: number;
  limit: number;
  offset: number;
}

/**
 * Knowledge search response.
 */
export interface KnowledgeSearchResponse {
  notes: KnowledgeNote[];
  query: string;
  total: number;
}

// =============================================================================
// Memory Models
// =============================================================================

export type MemoryType = 'factual' | 'procedural' | 'error_solution' | 'vocabulary' | 'relational';
export type MemorySource = 'observer' | 'todo' | 'compaction' | 'phase_archive' | 'tool_error';

export interface Memory {
  id: string;
  job_id: string;
  project_id?: string | null;
  agent_id?: string | null;
  content_preview: string;
  summary?: string | null;
  memory_type: MemoryType;
  source: MemorySource;
  keywords: string[];
  importance: number;
  source_turn_start?: number | null;
  source_turn_end?: number | null;
  source_phase?: number | null;
  token_count: number;
  access_count: number;
  created_at: string;
  last_accessed: string;
}

export interface MemoryListResponse {
  memories: Memory[];
  total: number;
  limit: number;
  offset: number;
}

export interface MemoryStats {
  total: number;
  total_tokens: number;
  total_accesses: number;
  factual: number;
  procedural: number;
  error_solution: number;
  vocabulary: number;
  relational: number;
  from_observer: number;
  from_todo: number;
  from_compaction: number;
  from_phase_archive: number;
  from_tool_error: number;
  avg_importance: number | null;
}

export type MemorySortField = 'created_at' | 'importance' | 'access_count' | 'token_count' | 'last_accessed';

// =============================================================================
// Agent Models
// =============================================================================

/**
 * Agent status types.
 */
export type AgentStatus = 'booting' | 'ready' | 'working' | 'session' | 'draining' | 'completed' | 'failed' | 'offline';

/**
 * Registered agent from the orchestrator.
 */
export interface Agent {
  id: string;
  config_name: string;
  hostname?: string;
  pod_ip?: string;
  pod_port: number;
  pid?: number;
  status: AgentStatus;
  current_job_id?: string;
  registered_at: string;
  last_heartbeat: string;
  metadata?: Record<string, unknown>;
  /**
   * Auxiliary-model health from the latest heartbeat (aux Phase 2). True while
   * the agent's auxiliary LLM (memory extraction/curation, session titles) is
   * sustained-failing. Compact failure detail lives in `metadata.aux`.
   */
  aux_degraded?: boolean;
}

// =============================================================================
// Job Models
// =============================================================================

/**
 * Job status types.
 */
export type JobStatus = 'created' | 'pending' | 'processing' | 'completed' | 'failed' | 'cancelled' | 'blocked_undelivered' | 'pending_review' | 'paused' | 'reviewing' | 'waiting';

/** Live, normalized state of the pull request persisted against a job. */
export interface PullRequestStatus {
  forge: RepositoryForge;
  repo: string;
  number: number;
  url: string;
  state: 'open' | 'merged' | 'closed';
  head: string;
  base: string;
  draft: boolean;
}

/** A persistent session created from server-owned job review context. */
export interface JobReviewSessionResult {
  job_id: string;
  thread_id: string;
  status: string;
}

/**
 * Job from the orchestrator.
 */
export interface WorkspaceContractProjection {
  requested_backend?: 'sandbox' | 'vm' | 'virtual' | 'none' | null;
  assigned_backend?: 'sandbox' | 'vm' | 'virtual' | 'none' | null;
  effective_backend?: 'sandbox' | 'vm' | 'virtual' | 'none' | null;
  assignment_source?: string | null;
  state: 'ready' | 'waiting' | 'failed' | 'mismatch' | 'invalid' | string;
  failure?: string | null;
  stale_backend?: 'sandbox' | 'vm' | null;
  compatibility_derived?: boolean;
}

export interface WorkspaceRecoveryView {
  operation_id: string;
  state: string;
  reason_code: string;
  message: string;
  started_at: string;
  deadline_at: string;
  next_check_at: string | null;
  retryable: boolean;
  cleanup_pending: boolean;
}

export interface Job {
  id: string;
  description: string;
  document_path?: string | null;
  config_name: string | null;
  /** JSONB can arrive as JSON text; normalize before reading its fields. */
  config_override?: Record<string, unknown> | string | null;
  assigned_agent_id?: string | null;
  user_id?: string | null;
  project_id?: string | null;
  parent_job_id?: string | null;
  creation_order?: number | null;
  delegation_context?: string | null;
  worktree_path?: string | null;
  repo_name?: string | null;
  branch_name?: string | null;
  merge_status?: string | null;
  delivery_status?: string | null;
  delivery_ref?: string | null;
  delivery_sha?: string | null;
  change_record_type?: 'job_record' | 'loop_record' | null;
  priority?: number;
  status: JobStatus;
  completion_outcome_kind?: 'blocked_undelivered' | null;
  created_at: string;
  updated_at?: string | null;
  completed_at?: string | null;
  error_message?: string | null;
  audit_count?: number | null;
  /**
   * JSONB — **may arrive as a raw JSON STRING, not an object.** asyncpg hands
   * JSONB back as text and the orchestrator passes it through, so indexing
   * straight into this type-checks and then silently yields `undefined` at
   * runtime. Run it through `asRecord()` (core/util/job-status) before reading
   * a key. Verified against dev 2026-07-29.
   */
  context?: Record<string, any> | string | null;
  /**
   * Freeze blob for a job awaiting review — `{summary, confidence,
   * deliverables, freeze_type, …}`. Cleared on approval. Same
   * JSONB-may-be-a-string caveat as `context`.
   */
  freeze_data?: Record<string, any> | string | null;
  /** Mode A diff-review state (job_cloud_export.md). NULL = no diff captured. */
  diff_status?: 'pending' | 'accepted' | 'rejected' | null;
  /** Mode A baseline commit hash for project-folder diff. Internal; only set if attached to project. */
  cloud_diff_baseline_commit?: string | null;
  /** Mode B export marker (set when "Export to shared folder" succeeds). */
  exported_folder_handle?: string | null;
  exported_at?: string | null;
  /**
   * Backend-resolved browser URL of the Mode B export folder — the handle is
   * opaque, so only the orchestrator can build this. Non-null exactly when
   * `exported_at` is set AND the owning cloud backend is up. Drives the
   * "Open cloud folder" button, which must stay reachable after the export
   * response is gone (reload, or a popup the browser blocked).
   */
  exported_folder_url?: string | null;
  /**
   * Backend-computed cloud-review routing (job_cloud_export.md). `'diff'` when
   * the job's project has a main-cloud folder (Mode A diff-review);
   * `'open_folder'` otherwise — loose jobs and default-project / no-cloud-folder
   * jobs get the Mode B "Open cloud folder" button.
   */
  cloud_review_mode?: 'diff' | 'open_folder' | null;
  /**
   * Session thread that created this job (session_wake_on_job_completion.md).
   * NULL for cockpit-, automation- and worker-child-created jobs. Set
   * server-side from the authenticated internal create path — never submitted.
   * The job card uses it to render "launched from this session".
   */
  created_by_thread_id?: string | null;
  /** Whether this job's terminal state owes its creating session a wake. */
  wake_on_complete?: boolean;
  /** Safe, server-owned workspace tier decision; contains no endpoints. */
  workspace_contract?: WorkspaceContractProjection;
  /** Safe recovery state; controller coordinates and raw diagnostics are never included. */
  workspace_recovery?: WorkspaceRecoveryView | null;
}

/**
 * Public job-create projection owned by orchestrator.schemas.job_create.
 * Keep view state and internal delegation/identity commands outside this type.
 */
export interface JobCreateRequest {
  workspace?: Record<string, unknown> | null;
  description: string;
  upload_id?: string;
  config_upload_id?: string;
  instructions_upload_id?: string;
  document_path?: string;
  document_dir?: string;
  /** Unified selector: bundled expert ID or DB expert UUID; omission uses the server default. */
  expert?: string;
  /** Legacy bundled/deployment-config alias. Conflicting expert selectors are refused. */
  config_name?: string;
  /** Legacy DB-expert UUID alias; resolves over worker_base, not another bundled expert. */
  expert_id?: string;
  config_override?: Record<string, unknown>;
  context?: Record<string, unknown>;
  instructions?: string;
  kickoff_message?: string;
  /** Immutable artifact/knowledge/PR contract, validated against authorized repositories. */
  required_deliverables?: string[];
  /** Explicit [] opts out; omission follows the deployment's defaults policy. Null is invalid. */
  datasource_ids?: string[];
  /** Mutually exclusive with datasource_ids, including an explicit empty array. */
  use_datasource_defaults?: boolean;
  /** Ignored compatibility input; the server derives ownership from the authenticated caller. */
  user_id?: string;
  project_id?: string;
  priority?: number;
  /** Deployment-gated execution selection; stateless requires a supported workspace. */
  execution_lane?: 'pinned' | 'stateless';
}

/**
 * Result of the Mode B export (`POST /api/jobs/{id}/export-to-shared-folder`).
 * See knowledge-history/done/job_cloud_export.md §3.2.
 */
export interface JobCloudExportResult {
  job_id: string;
  files_copied: number;
  /**
   * False = the files were copied but the folder could not be shared with the
   * caller, because the cloud backend has no account for them yet (Nextcloud
   * provisions on first OIDC login). The export is real, the folder just
   * isn't visible to them until they've signed in to the cloud once.
   */
  shared: boolean;
  folder: {
    name: string;
    /** Path within the caller's own cloud drive — the share lands at its root,
     *  so this is `/<name>`. Absent on orchestrators older than this field. */
    path?: string;
    browser_url: string | null;
    webdav_url: string | null;
  };
}

/**
 * Mode A diff summary for a project-attached job in `pending_review`.
 * See knowledge-history/done/job_cloud_export.md §3.4–§3.5.
 */
export interface JobDiffFileEntry {
  path: string;
  status: 'added' | 'modified' | 'deleted';
}

export interface JobDiffSummary {
  job_id: string;
  diff_status: 'pending' | 'accepted' | 'rejected' | null;
  baseline_commit: string;
  head_commit: string;
  files: JobDiffFileEntry[];
}

export interface JobDiffFile {
  job_id: string;
  path: string;
  status: 'added' | 'modified' | 'deleted';
  old_content: string | null;
  new_content: string | null;
}

export interface JobAcceptResult {
  job_id: string;
  diff_status: 'accepted';
  status: 'completed';
  applied: number;
  deleted: number;
}

export interface JobRejectResult {
  job_id: string;
  diff_status: 'rejected';
  status: 'completed';
}

/** Returned in the 409 body when the cloud folder diverged since seed. */
export interface JobAcceptConflict {
  code: 'external_modifications_detected';
  message: string;
  diverged: Array<{
    path: string;
    kind: 'etag_mismatch' | 'missing_at_cloud' | 'unexpected_at_cloud';
  }>;
}

/** Returned in the 502 body when partial WebDAV writes failed during apply. */
export interface JobAcceptPartialFailure {
  code: 'partial_write_failure';
  applied: number;
  deleted: number;
  errors: string[];
}

/**
 * Tagged outcome for the two diff *read* paths (summary and per-file).
 *
 * The nullable `Observable<T | null>` forms these replace collapsed every
 * failure into `null`, which the review panel then rendered as "no changes
 * to review" — a false statement on a safety surface for a 403 (not the
 * owner), a 404 (thread gone / not protected), a 5xx, and an offline
 * browser alike. Each of those needs different copy and a different
 * affordance, so the read has to carry which one happened.
 *
 * `missing` only occurs on a per-file read: the summary listed the path but
 * the staged set moved underneath us (resolved elsewhere, or re-staged).
 */
export type DiffLoadOutcome<T> =
  | { kind: 'ok'; data: T }
  | { kind: 'forbidden' }
  | { kind: 'unavailable' }
  /** `code` is the backend's reason when it sends one
   *  (`not_in_staged_diff` / `staged_content_unreadable`). Absent from job
   *  mode and from orchestrators older than that change, in which case the
   *  surface uses copy that is true whichever it was. */
  | { kind: 'missing'; code?: string }
  | { kind: 'error'; status: number; detail: string };

/** Outcome shape the cockpit uses to drive the diff-review UI state. Shared
 *  between the job-mode (`acceptJobDiff`) and thread-mode
 *  (`applyThreadCloudDiff`) apply calls — see api.model.ts's
 *  `ThreadCloudApplyResult` doc comment for why `ok.data` is a union. */
export type JobAcceptOutcome =
  | { kind: 'ok'; data: JobAcceptResult | ThreadCloudApplyResult }
  | { kind: 'conflict'; data: JobAcceptConflict }
  | { kind: 'partial'; data: JobAcceptPartialFailure }
  /** 409 epoch_stale (protected cloud mode only, Task 10): someone else
   *  applied/rejected/restaged since the caller last read the summary. The
   *  component reloads the diff and shows a "changed — reloaded" notice. */
  | { kind: 'stale'; staged_epoch: number }
  | { kind: 'error'; status: number; detail: string };

/**
 * Tagged outcome for the two *reject* paths.
 *
 * Reject used to return `T | null` and toast its own failures from inside the
 * service, which left the review surface unable to tell "discarded" from
 * "refused" — it cleared `submitting`, saw a falsy body, and returned,
 * leaving the decision controls live over a staged set the backend had just
 * refused to touch. It needs the same distinctions apply already had:
 *
 * - `stale` — 409 `epoch_stale`: someone applied/rejected/restaged since this
 *   summary was read. Reload; do not leave the old controls armed.
 * - `nothing_staged` — 409 `nothing_staged`: already resolved elsewhere.
 *   Reload into the resolved state; this is not an error.
 * - `error` — everything else, including 422 `invalid_epoch`, carrying a
 *   message the surface renders itself instead of a detached toast.
 */
export type DiffRejectOutcome<T> =
  | { kind: 'ok'; data: T }
  | { kind: 'stale'; staged_epoch: number | null }
  | { kind: 'nothing_staged' }
  | { kind: 'error'; status: number; detail: string };

// =============================================================================
// Protected cloud mode (Slice C, Task 8/10) — thread cloud-diff review
// =============================================================================
//
// Thread-mode counterpart to the Mode A job diff review above: the same
// JobDiffReviewComponent, generalized to also drive against a persistent
// thread's staged protected-cloud diff instead of a job's Gitea-baseline
// diff. See .superpowers/sdd/task-8-brief.md / task-10-brief.md for the
// endpoint contracts and knowledge-base/knowledge/design/cloud_access_unification.md §5/§11.

/**
 * Staged protected-cloud diff summary for a thread (GET
 * .../agents/threads/{id}/cloud-diff). `epoch` must be threaded back into
 * apply/reject as an optimistic-concurrency pin. All-zero/epoch-0/empty
 * `files` (never null from the endpoint) means nothing has been staged yet.
 */
export interface ThreadCloudDiffSummary {
  thread_id: string;
  epoch: number;
  staged_at: string | null;
  counts: { added: number; modified: number; deleted: number };
  protected_mount: string | null;
  files: Array<{ path: string; status: 'added' | 'modified' | 'deleted'; binary: boolean }>;
}

/** One staged file's old/new content for the thread cloud-diff viewer. */
export interface ThreadCloudDiffFile {
  thread_id: string;
  path: string;
  status: 'added' | 'modified' | 'deleted';
  old_content: string | null;
  new_content: string | null;
  old_binary: boolean;
  new_binary: boolean;
}

/**
 * Success body of POST .../cloud-diff/apply. Deliberately NOT shaped like
 * `JobAcceptResult` (no job_id/diff_status/status — a thread isn't a job):
 * `JobAcceptOutcome`'s `ok.data` is a union of the two so the single tagged
 * outcome type can be reused verbatim for both `acceptJobDiff` and
 * `applyThreadCloudDiff`, per the Task 14 brief.
 */
export interface ThreadCloudApplyResult {
  thread_id: string;
  applied: number;
  deleted: number;
  errors: string[];
  epoch: number;
  overlay_reset: boolean;
}

/** Success body of POST .../cloud-diff/reject. `overlay_reset` carries the
 *  same PC-07 meaning as apply's: false means the agent still holds the
 *  changes and a resume can stage them again. */
export interface ThreadCloudRejectResult {
  thread_id: string;
  rejected: boolean;
  epoch: number;
  overlay_reset: boolean;
}

/**
 * Server-computed job liveness states (officer_supervision_surface E3).
 * Computed from control status, audit movement, and agent heartbeat —
 * `jobs.updated_at` is never consulted. `suspected_stuck` is a prompt to
 * investigate, not a verdict; `unavailable` means telemetry could not be
 * reached and must never render as "no activity".
 */
export type JobLivenessState =
  | 'active'
  | 'waiting'
  | 'paused'
  | 'suspected_stuck'
  | 'unavailable'
  | 'terminal';

/** Per-source availability in a truthful supervision read (E1). */
export interface JobLivenessSource {
  name: string;
  status: 'fresh' | 'empty' | 'degraded' | 'stale' | 'unavailable';
  as_of?: string | null;
  reason?: string;
}

/**
 * Honest job liveness from GET /jobs/:id/progress.
 *
 * `state`/`reasons`/`last_activity_at` are the signal — render the liveness
 * state (badge/text), not a percent. `progress_percent`/`eta_seconds` are
 * retained for payload-shape compatibility and are honest `null` from this
 * producer: no percentage telemetry exists and none is fabricated.
 */
/**
 * One metered (category, resource, unit) line for a job — the shape
 * `GET /api/jobs/{id}/usage` returns in `rows`.
 */
export interface JobUsageRow {
  category: string;
  /** Model id for `llm`, `workspace_pod` for `compute`, etc. */
  resource: string;
  unit: string;
  quantity: number;
  /** **null = not priced** (no rate card covered it). Never render this as $0.00. */
  cost_usd: number | null;
  events: number;
  priced_events: number;
}

export interface JobUsageCategory {
  category: string;
  /** null when nothing in the category carried a rate. */
  cost_usd: number | null;
  events: number;
  priced_events: number;
}

/**
 * Which kind of answer the usage read is. Only `no_usage` means zero:
 * `predates_ledger` is a job older than the materializer's forward-only anchor
 * (it has no rows and never will), `unavailable` is the audit tier being off.
 */
/**
 * One descendant in a job's subjob roster.
 *
 * Every field here is one `GET /api/jobs` already publishes for a child that
 * the filter let through — the roster adds reach, not new data.
 */
export interface JobSubjob {
  id: string;
  parent_job_id: string | null;
  /** 0 = a direct child of the job asked about. */
  depth: number;
  description: string;
  status: string;
  /** The role: `scholar`, `critic`, `curator`. What makes a row readable. */
  config_name: string | null;
  origin: string | null;
  error_message: string | null;
  created_at: string;
  completed_at: string | null;
  updated_at: string | null;
}

/**
 * `GET /api/jobs/{job_id}/subjobs` — the tree under a job, filter-independent.
 *
 * Exists because a parent's status is not self-explanatory: `waiting` means
 * *blocked on a child*, and the jobs list is precisely where those children are
 * missing. Walks the tree rather than the list query, so the answer is a
 * property of the job rather than of the view it is being looked at through.
 */
export interface JobSubjobRoster {
  job_id: string;
  count: number;
  subjobs: JobSubjob[];
}

export type JobSubagentStatus =
  | 'queued'
  | 'running'
  | 'completed'
  | 'parked'
  | 'interrupted'
  | 'capped'
  | 'error'
  | 'cancelled';

/** One child thread published by `GET /api/jobs/{job_id}/subagents`. */
export interface JobSubagent {
  thread_id: string;
  /** Durable run claim used to fence lifecycle writes; null on older rows. */
  runtime_generation: string | null;
  handle: string;
  subagent_type: string;
  status: JobSubagentStatus;
  thread_status: ThreadStatus;
  outcome: string | null;
  error: string | null;
  turns: number;
  tokens: number;
  report_path: string | null;
  parent_tool_call_id: string | null;
  parent_thread_id: string | null;
  description: string;
  isolation: string | null;
  write_policy: string | null;
  parent_iteration: number | null;
  fork: boolean;
  started_at: string;
  ended_at: string | null;
  last_activity: string | null;
}

export interface JobSubagentRoster {
  job_id: string;
  count: number;
  subagents: JobSubagent[];
}

export type JobUsageState = 'measured' | 'no_usage' | 'predates_ledger' | 'unavailable';

/** `GET /api/jobs/{job_id}/usage` — see per_job_cost_and_token_accounting.md §7. */
export interface JobUsage {
  job_id: string;
  /** `subtree` when the caller asked for descendants too (`?include_subjobs=true`). */
  scope: 'job' | 'subtree';
  /** How many jobs the figures cover: 1 for `job`, 1 + descendants for `subtree`. */
  job_count: number;
  state: JobUsageState;
  /** Derived from the job, not from a caller-supplied window. Zulu. */
  window: {from: string; to: string};
  /**
   * `live` is true while the job is not terminal; `lag_seconds` is how far
   * behind the ledger can be (120s materializer poll + 60s aging window), so a
   * running job's figure is a lower bound.
   */
  freshness: {as_of: string; live: boolean; lag_seconds: number};
  rows: JobUsageRow[];
  llm: {
    prompt_tokens: number;
    cached_prompt_tokens: number;
    completion_tokens: number;
    total_tokens: number;
    cache_hit_ratio: number;
  };
  by_category: JobUsageCategory[];
  cost: {
    /** **null = unknown**, not free. */
    usd: number | null;
    /** false = some metered events were unpriced, so `usd` is a floor. */
    complete: boolean;
    priced_events: number;
    events: number;
  };
}

export interface JobProgress {
  job_id: string;
  status: JobStatus;
  /** Liveness verdict; prefer this over any numeric field. */
  state: JobLivenessState;
  /** Human-readable reasons behind `state` (may be empty, never invented). */
  reasons: string[];
  /** Last observed real activity (audit/heartbeat), or null when unknown. */
  last_activity_at: string | null;
  /** When the server computed this verdict. */
  observed_at: string;
  /** Stall threshold (minutes) the verdict was computed against. */
  threshold_minutes?: number;
  /** Server authority for the threshold. */
  threshold_source?: 'deployment_default' | 'request_override';
  /** Which sources were consulted and whether each was reachable. */
  sources?: JobLivenessSource[];
  /** Always null from the current producer; kept for shape compatibility. */
  progress_percent: number | null;
  elapsed_seconds: number;
  /** Always null from the current producer; kept for shape compatibility. */
  eta_seconds?: number | null;
  created_at?: string;
  updated_at?: string;
  completed_at?: string;
}

// =============================================================================
// Statistics Models
// =============================================================================

/**
 * Overall job statistics.
 */
export interface JobStatistics {
  /**
   * The "All" chip: every status, with the list's other filters applied.
   * Deliberately NOT narrowed by the status selection — these are
   * disjunctive facet counts, so selecting `failed` must not drop every
   * other chip to zero.
   */
  total_jobs: number;
  created: number;
  pending: number;
  processing: number;
  completed: number;
  failed: number;
  cancelled: number;
  blocked_undelivered?: number;
  pending_review: number;
  paused: number;
  reviewing: number;
  waiting: number;
  /** Raw server counts, including any status outside the known vocabulary. */
  by_status: Record<string, number>;
}

/**
 * Daily job statistics.
 */
export interface DailyStatistics {
  date: string;
  jobs_created: number;
  jobs_completed: number;
  jobs_failed: number;
  jobs_cancelled: number;
  jobs_blocked_undelivered?: number;
}

/**
 * Agent workforce summary.
 */
export interface AgentStatistics {
  total: number;
  booting: number;
  ready: number;
  working: number;
  completed: number;
  failed: number;
  offline: number;
}

/**
 * Stuck job with reason.
 */
export interface StuckJob {
  id: string;
  description: string;
  status: string;
  created_at: string;
  updated_at: string;
  stuck_reason: string;
  stuck_component: string;
  state?: JobLivenessState;
  reasons?: string[];
  last_activity_at?: string | null;
  threshold_minutes?: number;
  threshold_source?: 'deployment_default' | 'request_override';
}

/** Server-owned stuck-job result, including policy even when no rows match. */
export interface StuckJobsResponse {
  jobs: StuckJob[];
  threshold_minutes: number | null;
  threshold_source: 'deployment_default' | 'request_override' | 'unavailable';
}

// --- Capability grants (User-Defined Experts, Slice 2) ---

/** One entry of the capability catalog (delivered by both grant endpoints). */
export interface GrantCatalogEntry {
  type: 'bool' | 'enum' | 'list';
  default: unknown;
  restrict_only: boolean;
  order?: string[];
}

export type GrantCatalog = Record<string, GrantCatalogEntry>;

/** Deployment-level feature flags surfaced alongside the caller's grants
 * (e.g. protected cloud mode — Slice C's session-create toggle gate). */
export interface UserCapabilityFeatures {
  protected_cloud?: boolean;
  /** Backend supports scoped connector policy reads/writes and defaults. */
  datasource_scope_auto_attach_v1?: boolean;
  /** Root REST omission has crossed the compatibility gate to mean defaults. */
  datasource_defaults_on_omission?: boolean;
}

/** GET /api/users/me/capabilities */
export interface UserCapabilities {
  is_admin: boolean;
  grants: Record<string, unknown> | null; // null ⇒ admin (unrestricted)
  catalog: GrantCatalog;
  features?: UserCapabilityFeatures;
}

/** One SSH gateway host key, as published by GET /api/ssh/host-keys — public
 *  material only, safe for client-side pinning. */
export interface SshHostKeyEntry {
  type: string;
  public_key: string;
  fingerprint: string;
}

/** GET /api/ssh/host-keys — unauthenticated by design. Returns
 * `{host_keys: [], hostname: ...}` on a deployment with no gateway
 * configured, never an error; `CapabilitiesService.sshGateway` folds that
 * shape down to `null` so the UI can hide the connect panel entirely. */
export interface SshGatewayHostKeysResponse {
  host_keys: SshHostKeyEntry[];
  hostname: string;
}

/** GET /api/voice/capabilities — whether a usable TTS/STT model is configured
 * for the caller, so the read-aloud + mic buttons can render disabled-with-reason
 * instead of a dead click that silently answers 204. */
export interface VoiceCapabilities {
  tts: boolean;
  stt: boolean;
}

/** One explicitly-set grant row (GET /api/admin/grants). */
export interface Grant {
  key: string;
  value_json: unknown;
  granted_by: string | null;
  updated_at: string;
}

/** GET /api/admin/grants?scope_kind=&scope_id= */
export interface GrantListResponse {
  grants: Grant[];
  catalog: GrantCatalog;
}

// --- Contacts registry (knowledge-history/done/contacts_registry.md) ---
export type ContactChannel = 'email' | 'whatsapp';
export type ContactOptIn = 'pending' | 'opted_in' | 'opted_out';

export interface ContactAddress {
  id: string;
  channel: ContactChannel;
  address: string;
  is_primary: boolean;
  opt_in_status: ContactOptIn;
  last_inbound_at: string | null;
  created_at: string;
}

export interface ContactProjectRef {
  id: string;
  name: string;
}

export interface Contact {
  id: string;
  owner_user_id: string;
  display_name: string;
  notes: string | null;
  addresses: ContactAddress[];
  projects: ContactProjectRef[];
  created_at: string;
  updated_at: string;
}

// =============================================================================
// Stateless run_queue state (stateless_turn_resilience.md, step 2)
// =============================================================================

/** Why a session's queue unit was parked. Mirrors run_queue.park_reason. */
export type SessionQueueParkReason =
  | 'attach_failed'
  | 'shutdown_cancelled'
  | 'completion_cas_failed'
  | 'reaper_max_attempts'
  | string;

/**
 * The `queue` block the orchestrator returns on the input accept response,
 * on `/connection`, and on `GET /persistent/threads/{id}/queue`. `state` is
 * the run_queue row state ('queued' | 'leased' | 'parked' | 'done'); a parked
 * unit accepts input but nothing can claim it until it is retried.
 */
export interface SessionQueueState {
  state: string;
  park_reason: SessionQueueParkReason | null;
  parked_at: string | null;
  retryable: boolean;
  attempts: number;
  pending_input: boolean;
}

/** One parked unit as listed on GET /api/admin/capacity. */
export interface AdminCapacityParkedRow {
  unit_id: string;
  unit_kind: string;
  thread_id: string | null;
  title: string | null;
  owner: string | null;
  park_reason: SessionQueueParkReason | null;
  parked_at: string | null;
  attempts: number;
  pending_input: boolean;
}

/** GET /api/admin/capacity — what the KEDA scaler sees, plus the parked worklist. */
export interface AdminCapacity {
  observed_at: string;
  executors: { total: number | null; ready: number | null; busy: number };
  queued: { session_turn: number; worker_batch: number; total: number };
  oldest_queued_age_s: number;
  desired: number;
  params: { min_replicas: number; reserve: number };
  parked?: AdminCapacityParkedRow[];
}

/** POST /api/admin/run-queue/{unit}/unpark */
export interface RunQueueUnparkResult {
  unit_id: string;
  state: string;
}

/** POST /api/persistent/threads/{id}/queue/retry, folded to a discriminated outcome. */
export type SessionQueueRetryOutcome =
  | { kind: 'ok'; state: string }
  | { kind: 'refused'; status: number; code: string };
