/**
 * The creation forms' request: Expert + workspace + Connectors, with no
 * overrides on a baseline (creation_ui_expert_workspace_connectors.md D2).
 *
 * - An untouched template is sent as its catalogue selector.
 * - A changed template is sent as a complete inline copy: the template's
 *   authored fragment (`GET /api/experts/{id}/export`) with the form's changes
 *   deep-set into it. Fields the user never touched stay as authored, so the
 *   model-family matrix still fills them at resolution.
 * - Task- and session-level settings (autonomy, critic, permission mode …) are
 *   not Expert content; they travel separately in `config_override`.
 */
import {deepMergeConfig} from './config-merge';
import type {ExpertTemplateSource} from '../../core/models/api.model';

export type {ExpertTemplateSource};

/** Manifest ExpertSpec for the SRW harness, inline. */
export interface InlineExpertSelection {
  inline: {
    runtime: {
      adapter: 'srw/v1';
      config: ExpertTemplateSource['runtimeConfig'];
    };
    workspacePreference?: {backend: string};
  };
}

/**
 * The complete Expert the form describes: the template's runtime config with
 * the form's changes deep-set into its authored `config` fragment. Everything
 * else (`config_name`, the installed `asset_name`, `layers`) is kept as is.
 *
 * `instructions` is the edited instructions text, or null when the form has no
 * instructions field (sessions) or the user did not touch it.
 */
export function inlineExpertSelection(
  template: ExpertTemplateSource,
  expertOverrides: Record<string, unknown>,
  instructions: string | null,
): InlineExpertSelection {
  const runtimeConfig = clone(template.runtimeConfig);
  runtimeConfig.config = deepMergeConfig(runtimeConfig.config ?? {}, clone(expertOverrides));
  if (instructions !== null) {
    const prompts: Record<string, unknown> = {...(runtimeConfig.prompts ?? {})};
    if (instructions.trim()) prompts['instructions'] = instructions;
    else delete prompts['instructions'];
    runtimeConfig.prompts = prompts;
  }
  const inline: InlineExpertSelection['inline'] = {runtime: {adapter: 'srw/v1', config: runtimeConfig}};
  if (template.workspacePreference?.backend) inline.workspacePreference = {backend: template.workspacePreference.backend};
  return {inline};
}

function clone<T>(value: T): T {
  return JSON.parse(JSON.stringify(value ?? {})) as T;
}

/** Inputs for the read-only manifest view. */
export interface ManifestView {
  kind: 'job' | 'session';
  name?: string | null;
  /** The project's display name; rendered as its slug. */
  projectName?: string | null;
  task?: string | null;
  /** The selector string, or the inline selection. */
  expert: string | InlineExpertSelection | {inline: Record<string, unknown>} | null;
  /** The request's `workspace` field: absent (undefined) = use defaults. */
  workspace?: unknown;
  connectors: Array<{id: string; name: string}>;
  /** Task- or session-level settings sent beside the execution. */
  settings?: Record<string, unknown>;
}

/**
 * The manifest-shaped view of what Create sends. A Job renders as a `Job`
 * document; a Session (no manifest kind yet) as its `execution` block.
 */
export function executionManifest(view: ManifestView): Record<string, unknown> {
  const execution: Record<string, unknown> = {};
  if (typeof view.expert === 'string') execution['expert'] = {ref: {name: view.expert}};
  else if (view.expert) execution['expert'] = view.expert;
  if (view.workspace !== undefined) execution['workspace'] = view.workspace;
  const connectors: Record<string, unknown> = {};
  for (const c of view.connectors) {
    connectors[slug(c.name) || c.id] = {inline: {driver: 'srw.datasource/v1', config: {datasourceId: c.id}}};
  }
  execution['connectors'] = connectors;

  if (view.kind === 'session') {
    const doc: Record<string, unknown> = {execution};
    if (view.settings && Object.keys(view.settings).length) doc['settings'] = view.settings;
    return doc;
  }
  const metadata: Record<string, unknown> = {name: slug(view.name ?? '') || 'new-job'};
  if (view.projectName) metadata['scope'] = {kind: 'Project', name: slug(view.projectName)};
  const spec: Record<string, unknown> = {task: {text: view.task?.trim() || ''}, execution};
  if (view.settings && Object.keys(view.settings).length) spec['settings'] = view.settings;
  return {apiVersion: 'srw/v1alpha1', kind: 'Job', metadata, spec};
}

function slug(text: string): string {
  return text.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '').slice(0, 48);
}

/**
 * Minimal YAML for plain JSON data: objects, arrays, strings, numbers,
 * booleans, null. Strings that YAML could misread are quoted; multi-line
 * strings use block style. Display only — never parsed back.
 */
export function toYaml(value: unknown, indent = 0): string {
  return emit(value, indent).replace(/\n+$/, '') + '\n';
}

function emit(value: unknown, indent: number): string {
  const pad = ' '.repeat(indent);
  if (Array.isArray(value)) {
    if (value.length === 0) return '[]\n';
    return value.map((item) => {
      if (isPlainObject(item) && Object.keys(item).length) {
        const body = emit(item, indent + 2);
        return `${pad}- ${body.slice(indent + 2)}`;
      }
      return `${pad}- ${scalarOrNested(item, indent + 2)}`;
    }).join('');
  }
  if (isPlainObject(value)) {
    const keys = Object.keys(value);
    if (keys.length === 0) return '{}\n';
    return keys.map((key) => `${pad}${quoteKey(key)}:${afterKey(value[key], indent + 2)}`).join('');
  }
  return `${scalar(value)}\n`;
}

function afterKey(value: unknown, indent: number): string {
  if (Array.isArray(value)) return value.length ? `\n${emit(value, indent)}` : ' []\n';
  if (isPlainObject(value)) return Object.keys(value).length ? `\n${emit(value, indent)}` : ' {}\n';
  if (typeof value === 'string' && value.includes('\n')) {
    const pad = ' '.repeat(indent);
    // `|-` strips the final newline the block adds; `|` keeps exactly one.
    const indicator = value.endsWith('\n') ? '|' : '|-';
    return ` ${indicator}\n${value.replace(/\n+$/, '').split('\n').map((l) => (l ? pad + l : '')).join('\n')}\n`;
  }
  return ` ${scalar(value)}\n`;
}

function scalarOrNested(value: unknown, indent: number): string {
  if (Array.isArray(value) || isPlainObject(value)) return afterKey(value, indent).replace(/^ /, '');
  return `${scalar(value)}\n`;
}

function scalar(value: unknown): string {
  if (value === null || value === undefined) return 'null';
  if (typeof value === 'boolean' || typeof value === 'number') return String(value);
  const text = String(value);
  if (text === '' || /^[\s]|[\s]$|[:#\[\]{},&*!|>'"%@`]|^(true|false|null|yes|no|on|off|~)$|^[-?]|^\d/i.test(text)) {
    return JSON.stringify(text);
  }
  return text;
}

function quoteKey(key: string): string {
  return /^[A-Za-z_][A-Za-z0-9_.\/-]*$/.test(key) ? key : JSON.stringify(key);
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}
