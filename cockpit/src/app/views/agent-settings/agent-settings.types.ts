/**
 * Shared types for the agent settings component tree.
 */

import type {EffectiveModels} from '../../core/models/api.model';
import {JOB_TOOL_GROUP_CATEGORIES} from '../../core/tools/job-surface.generated';

/**
 * - `job` / `session`: creation forms — overrides collected at submit time.
 * - `live`: a running session's settings pane — pin-only (no reset-to-default
 *   affordances; the live protocol has no clear-override op), per-change apply
 *   via the host's state diff, and only the surface the live path honors.
 */
export type SettingsMode = 'job' | 'session' | 'live';

/** Tool category metadata for toggle display. */
export interface ToolCategoryMeta {
  key: string;
  label: string;
  icon: string;
  description: string;
}

/** Standard tool categories shown in job creation. */
export const JOB_TOOL_CATEGORIES: ToolCategoryMeta[] = [
  { key: 'research', label: 'Research', icon: 'travel_explore', description: 'Ability to search the web, find academic papers, and browse websites' },
  { key: 'browser_direct', label: 'Browser', icon: 'web', description: 'Direct browser control — navigate, click, type, screenshot, and visually verify pages' },
  { key: 'citation', label: 'Citation', icon: 'format_quote', description: 'Ability to track sources, manage citations, and generate bibliographies' },
  { key: 'shell', label: 'Shell', icon: 'terminal', description: 'Ability to run shell commands in a sandboxed terminal' },
  { key: 'communication', label: 'Communication', icon: 'mail', description: 'Ability to send email messages to you or your team' },
  { key: 'delegation', label: 'Delegation', icon: 'account_tree', description: 'Ability to spawn subagents that work in parallel' },
];

/** Session creation also shows SRW, knowledge, and git categories. */
export const SESSION_TOOL_CATEGORIES: ToolCategoryMeta[] = [
  ...JOB_TOOL_CATEGORIES,
  { key: 'canvas', label: 'Canvas', icon: 'dashboard_customize', description: 'Ability to present workspace files in the shared session Canvas' },
  ...JOB_TOOL_GROUP_CATEGORIES,
  { key: 'orchestrator', label: 'SRW Projects', icon: 'hub', description: 'Ability to inspect SRW projects, repositories, session context, and workspace upgrades' },
  { key: 'agent_catalog', label: 'Experts & Skills', icon: 'extension', description: 'Ability to look up experts and skills. Read-only — the row below is what creates them' },
  { key: 'workflows', label: 'Automations & Loops', icon: 'auto_mode', description: 'Ability to inspect automations and project loops, and draft disabled automations' },
  { key: 'catalog_authoring', label: 'Author Experts & Automations', icon: 'edit_note', description: 'Ability to create and update your own experts, skills and automations on your behalf. New automations are created switched off for you to review' },
  { key: 'knowledge', label: 'Knowledge', icon: 'psychology', description: 'Ability to read and write to the project knowledge base' },
  { key: 'git', label: 'Git', icon: 'commit', description: 'Ability to inspect workspace version history' },
];

/**
 * Presentation metadata for the categories the twelve above do not cover.
 *
 * PRESENTATION ONLY — icons, order and copy. It decides nothing about
 * enablement and is never a vocabulary: the resolved read
 * (`GET /api/persistent/threads/{id}/tool-groups`, `POST .../tool-groups/preview`)
 * returns EVERY category the agent can hold, including `mcp`, `unclassified`
 * and anything a config names, and the surfaces render what it returns. A key
 * missing from here still renders — see `humanizeCategoryKey` — so this list
 * going stale costs a nice label and nothing else. That is deliberate: the
 * lists this change deleted were all lists whose staleness cost correctness.
 */
export const AUXILIARY_TOOL_CATEGORIES: ToolCategoryMeta[] = [
  { key: 'workspace', label: 'Workspace Files', icon: 'folder_open', description: 'Read, write, move and search files in the session workspace' },
  { key: 'core', label: 'Core', icon: 'bolt', description: 'Planning, progress and completion — todos, replanning, notifications, sleep' },
  { key: 'session_task', label: 'Session Tasks', icon: 'checklist', description: "The session's own task list" },
  { key: 'product_help', label: 'Product Help', icon: 'help_center', description: 'Read the SRW product guide and capability reference' },
  { key: 'evaluation', label: 'Evaluation', icon: 'rule', description: 'Approve or return worker jobs with feedback' },
  { key: 'loop', label: 'Project Loop', icon: 'restart_alt', description: 'Plan an autonomous project loop' },
  { key: 'sql', label: 'SQL', icon: 'table', description: 'Query attached PostgreSQL datasources' },
  { key: 'mongodb', label: 'MongoDB', icon: 'database', description: 'Query attached MongoDB datasources' },
  { key: 'graph', label: 'Graph', icon: 'share', description: 'Query attached Neo4j datasources' },
  { key: 'webdav', label: 'Cloud Storage', icon: 'cloud', description: 'Read and write files on attached WebDAV / cloud datasources' },
  { key: 'email', label: 'Email', icon: 'inbox', description: 'Read and send through an attached email datasource' },
  { key: 'repo', label: 'Repositories', icon: 'source', description: 'Read and write attached repository datasources' },
  { key: 'mcp', label: 'MCP Servers', icon: 'extension', description: 'Tools discovered from attached MCP servers at session start' },
  { key: 'unclassified', label: 'Other', icon: 'category', description: 'Tools this cockpit build does not recognise — usually discovered at runtime' },
];

/** Every category the cockpit has copy for, session order first. */
export const ALL_TOOL_CATEGORIES: ToolCategoryMeta[] = [
  ...SESSION_TOOL_CATEGORIES,
  ...AUXILIARY_TOOL_CATEGORIES,
];

export const AUTONOMY_LEVELS = [
  { value: 'full', label: 'Full', description: 'Never freezes, runs to completion autonomously' },
  { value: 'review', label: 'Review', description: 'Freezes at job completion for human review' },
  { value: 'partial', label: 'Partial', description: 'Freezes at phase boundaries and job completion' },
  { value: 'guided', label: 'Guided', description: 'Freezes after every tactical phase' },
  { value: 'dependent', label: 'Dependent', description: 'Freezes after every phase (strategic and tactical)' },
] as const;

/** Priority level options. */
export const PRIORITY_LEVELS = [
  { value: 0, label: 'Low (backfill)' },
  { value: 5, label: 'Normal (default)' },
  { value: 10, label: 'High (preempts lower)' },
] as const;

/** Critic feedback round options. */
export const CRITIC_ROUND_OPTIONS = [
  { value: 1, label: '1 round' },
  { value: 3, label: '3 rounds' },
  { value: 5, label: '5 rounds (default)' },
  { value: 10, label: '10 rounds' },
  { value: 0, label: 'Unlimited' },
] as const;

/** Permission mode options for sessions. */
export const PERMISSION_MODES = [
  { value: 'supervised', label: 'Supervised', description: 'Agent asks for approval before executing commands' },
  { value: 'auto_accept', label: 'Auto-accept', description: 'Auto-approve most actions, flag risky ones' },
  { value: 'autonomous', label: 'Autonomous', description: 'Agent runs without asking for approval' },
] as const;

/** Image-quality tiers: resolution of images delivered to the model. Higher =
 *  more visual detail + more image tokens. Mirrors backend
 *  VALID_IMAGE_QUALITY_TIERS (src/core/loader.py) / image_downscale.py. */
export const IMAGE_QUALITY_TIERS = [
  { value: 'economy', label: 'Economy', description: 'Lowest resolution (~768px) — cheapest, coarse detail' },
  { value: 'standard', label: 'Standard', description: 'Balanced (~1568px) — good detail for most tasks' },
  { value: 'high', label: 'High', description: 'Model-family max — best for OCR, charts, UI screenshots' },
] as const;

/** Workspace backends, in the order the selector lists them. `i18nKey` names
 *  the shared label under `advanced.options.*` — one vocabulary for the
 *  creation forms and the live session pane alike. */
export const WORKSPACE_BACKENDS = [
  { value: 'sandbox', i18nKey: 'container' },
  { value: 'vm', i18nKey: 'vmQemu' },
  { value: 'virtual', i18nKey: 'virtual' },
  { value: 'none', i18nKey: 'none' },
] as const;

/**
 * Whether a running session can move to a given workspace tier.
 *
 * `current` is the tier it is on; `ok` is reachable. Everything else is a
 * reason the move is refused, and doubles as the i18n key suffix under
 * `agentSettings.execution.tierUnreachable.*` — the reason renders in the
 * option itself, so the "no" arrives before the click rather than after it.
 *
 * Reachability is the live pane's to decide (it holds the tier and the
 * grants); the settings group only renders what it is told.
 */
export type TierReachability =
  | 'current'
  | 'ok'
  | 'downgrade'
  | 'needsApproval'
  | 'unsupported';

/** Deep-read a nested path from a config object. */
export function readConfigPath(config: Record<string, unknown>, path: string): unknown {
  return path.split('.').reduce((obj: any, key) => obj?.[key], config) ?? null;
}

/** Deep-set a nested path on a config object, creating intermediate objects. */
export function setConfigPath(obj: Record<string, unknown>, path: string, value: unknown): void {
  const keys = path.split('.');
  let current: any = obj;
  for (let i = 0; i < keys.length - 1; i++) {
    if (current[keys[i]] === undefined || typeof current[keys[i]] !== 'object') {
      current[keys[i]] = {};
    }
    current = current[keys[i]];
  }
  current[keys[keys.length - 1]] = value;
}

/**
 * Detect model family from model name for settings_matrix resolution.
 * TypeScript port of src/core/loader.py:detect_model_family().
 */
export function detectModelFamily(model: string): string {
  let name = model.toLowerCase();

  // Strip provider prefixes
  for (const prefix of ['openrouter/', 'groq/', 'codex/']) {
    if (name.startsWith(prefix)) {
      name = name.slice(prefix.length);
      const slash = name.indexOf('/');
      if (slash !== -1) name = name.slice(slash + 1);
      break;
    }
  }
  if (name.startsWith('openai/')) name = name.slice('openai/'.length);

  // Opus 5.5 before Opus 5, Opus 5 before the generic Opus rule — mirrors
  // family_of() in src/shared/runtime/core/model_registry.py (Opus 5.x is the
  // only Opus with the full xhigh/max effort ladder, and 5.5 moved the default
  // effort, so each has its own matrix family). 5.5 matches the hyphen and
  // OpenRouter's dotted form; the digit guard keeps dated Opus 5 ids on 5.
  if (/^claude-opus-5[.-]5(?!\d)/.test(name)) return 'claude-opus-5-5';
  if (name.startsWith('claude-opus-5')) return 'claude-opus-5';
  if (name.startsWith('claude-opus')) return 'claude-opus';
  if (name.startsWith('claude-sonnet')) return 'claude-sonnet';
  if (name.startsWith('claude-haiku')) return 'claude-haiku';
  // Fable 5 and 5.1 share one family — mirrors family_of() on the server.
  if (name.startsWith('claude-fable')) return 'claude-fable';
  if (name.includes('codex-spark')) return 'codex-spark';
  if (name.includes('codex') && (name.startsWith('gpt-5') || name.startsWith('gpt-6'))) return 'codex';
  // GPT-6 (Astra). Mirrors family_of() in src/shared/runtime/core/model_registry.py:
  // below the codex checks, above the gpt-5 prefixes.
  if (name.startsWith('gpt-6')) return 'gpt-6';
  if (name.startsWith('gpt-5.6')) return 'gpt-5.6';
  if (name.startsWith('gpt-5')) return 'gpt-5';
  if (name.startsWith('gpt-4o')) return 'gpt-4o';
  if (name.startsWith('o1') || name.startsWith('o3') || name.startsWith('o4')) return 'o-series';
  if (name.includes('deepseek')) return 'deepseek';
  if (name.includes('glm-5.3-flash')) return 'glm-5.3-flash';
  if (name.includes('glm-5.3')) return 'glm-5.3';
  if (name.includes('glm')) return 'glm';
  if (/(?:^|\/)muse-spark-1\.3(?:$|[-:])/.test(name)) return 'muse-spark-1.3';
  if (
    name.startsWith('mistral') || name.startsWith('codestral') || name.startsWith('magistral') ||
    name.startsWith('ministral') || name.startsWith('devstral') || name.startsWith('pixtral') ||
    name.startsWith('voxtral')
  ) return 'mistral';
  if (name.includes('qwen') || name.includes('qwq')) return 'qwen';
  if (name.includes('llama')) return 'llama';
  if (name.startsWith('gemini')) return 'gemini';
  if (name.startsWith('gpt-oss')) return 'gpt-oss';
  if (name.includes('gemma')) return 'gemma';
  if (name.includes('minimax') && name.includes('m3')) return 'minimax-m3';
  if (name.includes('minimax')) return 'minimax';

  return 'default';
}

/** Deep-merge two plain objects (override replaces arrays and scalars). */
function deepMerge(base: Record<string, unknown>, override: Record<string, unknown>): Record<string, unknown> {
  const result = {...base};
  for (const key of Object.keys(override)) {
    const sv = override[key];
    const tv = result[key];
    if (sv && typeof sv === 'object' && !Array.isArray(sv) && tv && typeof tv === 'object' && !Array.isArray(tv)) {
      result[key] = deepMerge(tv as Record<string, unknown>, sv as Record<string, unknown>);
    } else {
      result[key] = sv;
    }
  }
  return result;
}

/**
 * Resolve settings_matrix for a specific model.
 * Returns the merged default + family-specific settings (flat dict with
 * keys like temperature, top_p, model_max_context_tokens, limits).
 */
export function resolveMatrixForModel(
  matrix: Record<string, Record<string, unknown>>,
  model: string,
): Record<string, unknown> {
  if (!matrix || !model) return {};
  const family = detectModelFamily(model);
  const base = {...(matrix['default'] ?? {})};
  if (family !== 'default' && matrix[family]) {
    return deepMerge(base, matrix[family] as Record<string, unknown>);
  }
  return base;
}

/**
 * Pick the server-resolved effective-models payload the model picker should show
 * for its "Default" option: the selected expert's resolution when present, else
 * the framework defaults' resolution (the no-expert create path). Without the
 * framework fallback the picker drops to the config-literal `llm.model` — the
 * hardcoded YAML placeholder (`RedHatAI/gemma-4-31B-it-FP8-Dynamic`) — instead of
 * the resolved system chat pin. `undefined` (older API, no `effective_models`)
 * falls back the same as `null` (no expert selected).
 */
export function resolveEffectiveModels(
  expertModels: EffectiveModels | null | undefined,
  frameworkModels: EffectiveModels | null,
): EffectiveModels | null {
  return expertModels ?? frameworkModels ?? null;
}

/**
 * Label for a model picker's "inherit the default" option, revealing the model
 * that default currently resolves to. `prefix` is the inherit-marker ("Base
 * default", "Project default"); when a resolved `model` is known it is appended
 * ("Base default · gemma-4-31b") so the option isn't an opaque "default". Falls
 * back to the bare prefix when nothing is resolved yet (picker still loading, or
 * no catalog row), avoiding a dangling separator.
 */
export function defaultModelOptionLabel(prefix: string, model: string | null | undefined): string {
  return model ? `${prefix} · ${model}` : prefix;
}

/**
 * Commit a currently-inherited value into its override signal.
 *
 * Paired with `PinOnInteractDirective`: the controls render
 * `override() ?? resolved()`, so "what the user sees" and "what the user chose"
 * are different things. When they deliberately interact with a control, the
 * displayed value becomes a choice. No-op once an override exists, so repeated
 * interaction never clobbers a real selection — only the reset button clears
 * back to inherit.
 */
export function pinResolvedValue<T>(
  target: {(): T | null; set(value: T | null): void},
  resolved: T,
): boolean {
  if (target() !== null) return false;
  target.set(resolved);
  return true;
}
