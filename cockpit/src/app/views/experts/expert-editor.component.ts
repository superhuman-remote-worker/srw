import {Component, computed, effect, inject, OnInit, signal, viewChild} from '@angular/core';
import {FormsModule} from '@angular/forms';
import {ActivatedRoute, Router} from '@angular/router';
import {TranslocoPipe, TranslocoService} from '@jsverse/transloco';
import {ApiService} from '../../core/services/api.service';
import type {SessionToolGroupsResponse} from '../../core/services/api.service';
import {ModelService} from '../../core/services/model.service';
import type {
  EffectiveModels,
  ExpertCreateRequest,
  ExpertRole,
  ExpertUpdateRequest,
  GrantCatalog,
  SubagentsConfig,
} from '../../core/models/api.model';
import {EXPERT_ROLES, SUBAGENT_INHERIT_MODEL} from '../../core/models/api.model';
import {AppButtonComponent} from '../../ui/button';
import {AppChipComponent} from '../../ui/chip';
import {AppInputComponent} from '../../ui/input';
import {AppTextareaComponent} from '../../ui/textarea';
import {AppSelectComponent} from '../../ui/select';
import {AppFormFieldComponent} from '../../ui/form-field';
import {AppIconComponent} from '../../ui/icon';
import {SidebarToggleComponent} from '../../shell/sidebar-toggle/sidebar-toggle.component';
import {ExecutionGroupComponent} from '../agent-settings/execution-group.component';
import {ToolsGroupComponent} from '../agent-settings/tools-group.component';
import {AdvancedAccordionComponent} from '../agent-settings/advanced-accordion.component';
import {deepMergeConfig} from '../agent-settings/config-merge';
import {assembleExpertConfig, liftLegacyTiers, splitExpertConfig} from './expert-config';
import {SubagentsEditorComponent} from './subagents-editor.component';
import {isModelAllowed} from '../agent-settings/capability-gates';
import {defaultModelOptionLabel} from '../agent-settings/agent-settings.types';

/** Derive a valid expert slug (^[a-z][a-z0-9_-]*$) from a display name. */
export function slugify(s: string): string {
  const base = s
    .normalize('NFKD')
    .replace(/[^\x20-\x7E]/g, '')
    .toLowerCase()
    .trim()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '');
  if (!base) return 'expert';
  return /^[a-z]/.test(base) ? base : `e-${base}`;
}

export interface ParsedConfig {
  config?: Record<string, unknown>;
  error?: string;
}

/** The tool-group preview payload for an expert being edited. */
export interface ExpertToolPreviewRequest {
  expert_type: 'worker' | 'session';
  config_name: string;
  config_override: Record<string, unknown> | null;
}

/**
 * What to ask the preview endpoint for an expert under edit.
 *
 * base ⊕ fragment, never `expert_id`. Two reasons, and both are cases where the
 * id would make the pane describe something other than what is on screen:
 *
 *  - on CREATE there is no id, so the answer would be the bare base and the
 *    editor would forecast a toolset the saved expert will not have;
 *  - on EDIT, layering the fragment on top of the stored row cannot express a
 *    REMOVED key — `tools.shell` deleted in the editor still resolves from the
 *    row underneath, so the pane would show a category the expert is losing.
 *
 * base ⊕ fragment is what an expert *is*, which makes it the only layering that
 * answers for the form's current contents.
 */
export function expertToolPreviewRequest(
  expertType: 'worker' | 'session',
  fragment: Record<string, unknown>,
): ExpertToolPreviewRequest {
  return {
    expert_type: expertType,
    config_name: expertBaseConfigName(expertType),
    // {} would be a no-op layer, but null says "no fragment" to a reader.
    config_override: Object.keys(fragment).length ? fragment : null,
  };
}

/** Parse the raw config-fragment textarea into an object (or an error string). */
export function parseConfigText(text: string): ParsedConfig {
  const t = text.trim();
  if (!t) return {config: {}};
  let parsed: unknown;
  try {
    parsed = JSON.parse(t);
  } catch (e) {
    return {error: (e as Error).message};
  }
  if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) {
    return {error: 'Config must be a JSON object'};
  }
  return {config: parsed as Record<string, unknown>};
}

export function expertBaseConfigName(expertType: 'worker' | 'session'): string {
  return expertType === 'session' ? 'session_base' : 'worker_base';
}

export function expertEditorMode(expertType: 'worker' | 'session'): 'job' | 'session' {
  return expertType === 'session' ? 'session' : 'job';
}

/** Prompt segments the editor can author (one family-agnostic version each). */
export interface PromptFields {
  persona: string;
  instructions: string;
  strategic: string;
  tactical: string;
  summarization: string;
}

/**
 * Assemble the `prompts` save payload from the editor fields. Only non-empty
 * segments are emitted — an empty field inherits the framework default, and
 * because the API replaces the `prompts` column wholesale, a cleared field drops
 * its override. Strategic/tactical are worker-only (session experts don't run the
 * phase loop), so they are excluded in session mode.
 */
export function buildPromptsPayload(
  fields: PromptFields,
  mode: 'job' | 'session',
): Record<string, string> {
  const out: Record<string, string> = {};
  const add = (k: keyof PromptFields): void => {
    if (fields[k].trim()) out[k] = fields[k];
  };
  add('persona');
  add('instructions');
  add('summarization');
  if (mode === 'job') {
    add('strategic');
    add('tactical');
  }
  return out;
}

/**
 * The `tags` save payload: role tags first (the expert's own type always on,
 * in canonical role order), then the free-text tags — de-duplicated, order
 * kept. Tags are additive metadata (U1 D4): a soft UI filter, never read for
 * behaviour, and the server adds the `expert_type` role tag on write anyway.
 */
export function buildTagsPayload(
  expertType: 'worker' | 'session',
  roleTags: readonly string[],
  freeText: string,
): string[] {
  const roles = EXPERT_ROLES.filter((r) => r === expertType || roleTags.includes(r));
  const free = freeText
    .split(',')
    .map((t) => t.trim())
    .filter(Boolean);
  return Array.from(new Set<string>([...roles, ...free]));
}

/** Split stored tags into the role chips and the free-text field. */
export function splitTags(tags: readonly string[]): {roles: ExpertRole[]; free: string} {
  const roleSet = new Set<string>(EXPERT_ROLES);
  return {
    roles: EXPERT_ROLES.filter((r) => tags.includes(r)),
    free: tags.filter((t) => !roleSet.has(t)).join(', '),
  };
}

/**
 * The model-select fragment: ONE model (`llm.model`) since U1 — the same key
 * for worker and session experts. Empty ⇒ inherit (no key).
 */
export function buildModelFragment(model: string): Record<string, unknown> {
  return model ? {llm: {model}} : {};
}

/**
 * The `subagents` block to persist: the roster editor's `{default?, roster?}`
 * (plus its passthrough keys) with the roster-wide model from the Subagent
 * model select merged into `llm` — `''` clears `llm.model` and drops an
 * `llm` that has nothing else left. ALWAYS returned (possibly `{}`) so the
 * host replaces the stored block wholesale; a deep-merge would resurrect
 * every entry the author removed. An empty block is stripped after assembly.
 */
export function buildSubagentsFragment(
  editorValue: SubagentsConfig | null,
  subagentModel: string,
): Record<string, unknown> {
  const block: Record<string, unknown> = {...(editorValue ?? {})};
  const existing = block['llm'];
  const llm: Record<string, unknown> =
    typeof existing === 'object' && existing !== null && !Array.isArray(existing)
      ? {...(existing as Record<string, unknown>)}
      : {};
  if (subagentModel) llm['model'] = subagentModel;
  else delete llm['model'];
  if (Object.keys(llm).length) block['llm'] = llm;
  else delete block['llm'];
  return block;
}

/** Drop a `subagents: {}` left behind by a cleared roster. */
export function stripEmptySubagents(config: Record<string, unknown>): Record<string, unknown> {
  const sub = config['subagents'];
  if (typeof sub === 'object' && sub !== null && !Array.isArray(sub) && Object.keys(sub).length === 0) {
    const rest = {...config};
    delete rest['subagents'];
    return rest;
  }
  return config;
}

/**
 * Router state `settings.component.ts`'s `customizeDefaultExpert` attaches to
 * the `/experts/{id}/edit` navigation it makes right after a successful
 * `POST /api/expert-defaults/{type}/fork` (task 4, 2026-08-04 plan). That
 * route can strip-and-report the same as `duplicate` (task 3) — a bundled or
 * borrowed source config commonly needs a grant this caller doesn't hold —
 * and the settings page unmounts immediately on success, so a banner there
 * would flash and be gone. This is how the notice survives the navigation to
 * land on the editor instead.
 */
export interface ExpertEditorNavigationState {
  dropped?: string[];
}

/**
 * Whether the fork this editor was just navigated FROM stripped anything,
 * and if so which transloco key + params report it — `null` when there is
 * nothing to say (dropped absent or empty), which is the "do not render"
 * half of the notice: unlike `duplicateResultTranslationArgs`
 * (experts-list.component.ts), landing on this editor is already the
 * success signal, so a clean fork gets no message at all, only a stripped
 * one does.
 *
 * Pulled out as a pure function for the same reason
 * `duplicateResultTranslationArgs` is: unit-testable without the component's
 * five injected services. Grant keys are passed through verbatim
 * (comma-joined, not humanized) — same convention, matching how
 * `admin-grants.component.ts` renders them raw in a `<code>` cell.
 */
export function forkNoticeTranslationArgs(
  dropped: string[] | undefined,
): [key: string, params: Record<string, string>] | null {
  if (!dropped || dropped.length === 0) return null;
  return ['experts.forkedMissingGrants', {grants: dropped.join(', ')}];
}

interface EditorForm {
  name: string;
  display_name: string;
  description: string;
  icon: string;
  color: string;
  /** Free-text tags (comma separated); the role chips live in `roleTags`. */
  tags: string;
  expert_type: 'worker' | 'session';
  persona: string;
  instructions: string;
  strategic: string;
  tactical: string;
  summarization: string;
  /** The one model (`llm.model`); '' = inherit the base default. */
  model: string;
  /** The roster-wide subagent model (`subagents.llm.model`); '' = inherit. */
  subagentModel: string;
  configText: string;
}

@Component({
  selector: 'app-expert-editor',
  standalone: true,
  imports: [
    FormsModule,
    TranslocoPipe,
    AppButtonComponent,
    AppChipComponent,
    AppInputComponent,
    AppTextareaComponent,
    AppSelectComponent,
    AppFormFieldComponent,
    AppIconComponent,
    SidebarToggleComponent,
    ExecutionGroupComponent,
    ToolsGroupComponent,
    AdvancedAccordionComponent,
    SubagentsEditorComponent,
  ],
  template: `
    <div class="editor">
      <header class="head">
        <!-- Full-page route (no page shell), so carry the mobile sidebar toggle
             here too — otherwise the off-canvas nav is unreachable on mobile.
             Renders nothing on desktop (sidebar always expanded). -->
        <app-sidebar-toggle />
        <h1>{{ (isEdit() ? 'experts.edit' : 'experts.new') | transloco }}</h1>
      </header>

      @if (forkNotice()) {
        <div class="banner info">{{ forkNotice() }}</div>
      }

      <section class="card">
        <app-form-field label="Display name" [required]="true">
          <app-input [value]="form.display_name" (valueChange)="onDisplayName($event)" />
        </app-form-field>
        <app-form-field label="Name (slug)" [required]="true" hint="lowercase; immutable after create">
          <app-input [value]="form.name" [disabled]="isEdit()" (valueChange)="onNameEdit($event)" />
        </app-form-field>
        <app-form-field label="Type" [required]="true">
          <app-select [value]="form.expert_type" [disabled]="isEdit()" (valueChange)="onTypeChange($event)">
            <option value="worker">worker</option>
            <option value="session">session</option>
          </app-select>
        </app-form-field>
        <app-form-field label="Description">
          <app-input [value]="form.description" (valueChange)="form.description = $event" />
        </app-form-field>
        <div class="row">
          <app-form-field label="Icon (Material Symbol)">
            <app-input [value]="form.icon" (valueChange)="form.icon = $event" placeholder="smart_toy" />
          </app-form-field>
          <app-icon size="xl" [style.color]="form.color">{{ form.icon || 'smart_toy' }}</app-icon>
          <app-form-field label="Color (hex)">
            <app-input [value]="form.color" (valueChange)="form.color = $event" placeholder="#6B7280" />
          </app-form-field>
        </div>
      </section>

      <!-- Tags: role chips (the expert's own type locked on) + free text. A
           soft UI filter — every expert stays usable in every role (U1 D4). -->
      <section class="card">
        <h2>{{ 'experts.tags.title' | transloco }}</h2>
        <p class="hint">{{ 'experts.tags.hint' | transloco }}</p>
        <app-form-field [label]="'experts.tags.roles' | transloco">
          <div class="role-chips">
            @for (role of expertRoles; track role) {
              <app-chip
                [selected]="hasRoleTag(role)"
                [disabled]="role === form.expert_type"
                [ariaLabel]="role === form.expert_type ? ('experts.tags.roleLocked' | transloco: {role}) : role"
                (clicked)="toggleRoleTag(role)"
              >{{ role }}</app-chip>
            }
          </div>
        </app-form-field>
        <app-form-field [label]="'experts.tags.free' | transloco">
          <app-input
            [value]="form.tags"
            [placeholder]="'experts.tags.freePlaceholder' | transloco"
            (valueChange)="form.tags = $event"
          />
        </app-form-field>
      </section>

      <section class="card">
        <h2>Persona</h2>
        <app-textarea
          [value]="form.persona"
          [rows]="8"
          (valueChange)="form.persona = $event"
          placeholder="The expert's persona / system style…"
        />
        <h2>Instructions (optional)</h2>
        <app-textarea [value]="form.instructions" [rows]="5" (valueChange)="form.instructions = $event" />
      </section>

      <section class="card">
        <h2>Phase prompts (advanced)</h2>
        <p class="hint">
          Override the workflow prompts. Leave a field empty to inherit the
          framework default. Custom strategic/tactical text is treated as
          untrusted and stays subordinate to system rules &amp; safety.
        </p>
        @if (mode() === 'job') {
          <h2>Strategic (planning)</h2>
          <app-textarea
            [value]="form.strategic"
            [rows]="6"
            (valueChange)="form.strategic = $event"
            placeholder="Inherit framework default…"
          />
          <h2>Tactical (execution)</h2>
          <app-textarea
            [value]="form.tactical"
            [rows]="6"
            (valueChange)="form.tactical = $event"
            placeholder="Inherit framework default…"
          />
        }
        <h2>Summarization</h2>
        <app-textarea
          [value]="form.summarization"
          [rows]="5"
          (valueChange)="form.summarization = $event"
          placeholder="Inherit framework default…"
        />
      </section>

      <!-- Structured config: reuses the launch-flow groups. [config] = base ⊕ fragment
           drives "default: X" displays; getOverrides() + the model selects feed the
           saved config (see save()). -->
      <section class="card">
        <h2>Execution</h2>
        <app-execution-group
          [config]="baseForGroups()"
          [mode]="mode()"
          [disabled]="false"
          [showProjectMemory]="false"
          [gatedCapabilities]="gatedCapabilities()"
          [catalog]="catalog()"
          [workspaceBackendLabel]="'agentSettings.execution.workspaceRecommended'"
        />
      </section>

      <!-- ONE model since U1 (llm.model, worker and session alike) plus, for a
           worker expert, the roster-wide subagent model (subagents.llm.model). -->
      <section class="card">
        <h2>{{ 'experts.model.title' | transloco }}</h2>
        <label class="ml">{{ 'experts.model.model' | transloco }}
          <select class="model-select" [disabled]="isModelGated()" [ngModel]="form.model" (ngModelChange)="form.model = $event">
            <option [ngValue]="''">{{ baseDefaultLabel('model') }}</option>
            @for (g of models(); track g.group) {
              <optgroup [label]="g.group">
                @for (m of g.models; track m) { <option [ngValue]="m" [disabled]="!modelAllowed(m)">{{ m }}</option> }
              </optgroup>
            }
          </select>
        </label>
        @if (mode() === 'job') {
          <label class="ml">{{ 'experts.model.subagent' | transloco }}
            <select class="model-select" [disabled]="isModelGated()" [ngModel]="form.subagentModel" (ngModelChange)="form.subagentModel = $event">
              <option [ngValue]="''">{{ baseDefaultLabel('subagent') }}</option>
              @for (g of models(); track g.group) {
                <optgroup [label]="g.group">
                  @for (m of g.models; track m) { <option [ngValue]="m" [disabled]="!modelAllowed(m)">{{ m }}</option> }
                </optgroup>
              }
            </select>
          </label>
          <p class="hint">{{ 'experts.model.subagentHint' | transloco }}</p>
        }
        @if (isModelGated()) {
          <small class="lock-hint">🔒 {{ 'grants.locked.model_selection' | transloco }}</small>
        }
      </section>

      <!-- The subagent roster (config.subagents.default / .roster). -->
      <section class="card">
        <h2>{{ 'experts.subagents.title' | transloco }}</h2>
        <p class="hint">{{ 'experts.subagents.hint' | transloco }}</p>
        <app-subagents-editor
          [models]="models()"
          [modelGated]="isModelGated()"
          [modelAllowed]="modelAllowed"
        />
      </section>

      <section class="card">
        <h2>Tools</h2>
        <app-tools-group
          [config]="baseForGroups()"
          [mode]="mode()"
          [disabled]="false"
          [enumerateOnly]="enumerateOnly()"
          [gatedCapabilities]="gatedCapabilities()"
          [resolved]="toolPreview()"
          [readsResolvedToolset]="true"
        />
      </section>

      <section class="card">
        <h2>Advanced</h2>
        <app-advanced-accordion
          [config]="baseForGroups()"
          [mode]="mode()"
          [disabled]="false"
          [settingsMatrix]="settingsMatrix()"
          [modelOverride]="form.model || null"
          [backendOverride]="execBackendOverride()"
        />
      </section>

      <section class="card">
        <h2>Advanced (other keys) — JSON</h2>
        <p class="hint">
          Config keys not covered by the controls above (e.g. <code>instruction_files</code>).
          Credential sections are rejected on save.
        </p>
        <app-textarea
          [value]="form.configText"
          [rows]="6"
          (valueChange)="form.configText = $event"
          placeholder='{ "instruction_files": [] }'
        />
      </section>

      @if (errorMessage()) {
        <div class="banner err">{{ errorMessage() }}</div>
      }

      <footer class="actions">
        <app-button variant="secondary" (clicked)="cancel()">{{ 'experts.cancel' | transloco }}</app-button>
        <app-button variant="primary" [loading]="saving()" (clicked)="save()">
          {{ 'experts.save' | transloco }}
        </app-button>
      </footer>
    </div>
  `,
  styles: [
    `
      :host {
        display: block;
        height: 100%;
        overflow-y: auto;
      }
      .editor {
        padding: 1rem 1.5rem;
      }
      .head {
        display: flex;
        flex-direction: column;
        align-items: flex-start;
        gap: 0.5rem;
        margin-bottom: 1rem;
      }
      .head h1 {
        margin: 0;
      }
      .card {
        background: var(--panel-bg);
        border: 1px solid var(--border-color);
        color: var(--text-primary);
        padding: 1rem;
        border-radius: var(--radius-surface, 8px);
        margin-bottom: 1rem;
        display: flex;
        flex-direction: column;
        gap: 0.75rem;
      }
      .card h2 {
        margin: 0;
        font-size: 0.95rem;
        color: var(--text-primary);
      }
      .row {
        display: flex;
        gap: 1rem;
        align-items: flex-end;
      }
      .role-chips {
        display: flex;
        flex-wrap: wrap;
        gap: 0.5rem;
      }
      .ml {
        display: flex;
        flex-direction: column;
        gap: 0.25rem;
        font-size: 0.85rem;
        color: var(--text-secondary);
      }
      .model-select {
        padding: 7px 10px;
        border: 1px solid var(--border-color);
        border-radius: var(--radius-control, 6px);
        background: var(--surface-0, var(--panel-bg));
        color: var(--text-primary);
        font: inherit;
      }
      .model-select:disabled {
        opacity: 0.55;
        cursor: not-allowed;
      }
      .lock-hint {
        display: block;
        margin-top: 0.35rem;
        color: var(--text-muted);
        font-size: 0.8rem;
      }
      .hint {
        margin: 0;
        color: var(--text-muted);
        font-size: 0.85rem;
      }
      .actions {
        display: flex;
        justify-content: flex-end;
        gap: 0.5rem;
      }
      .banner.err {
        background: var(--danger-tint);
        color: var(--danger);
        padding: 0.5rem 0.75rem;
        border-radius: var(--radius-control);
        margin-bottom: 1rem;
      }
      .banner.info {
        background: var(--info-tint);
        color: var(--info);
        padding: 0.5rem 0.75rem;
        border-radius: var(--radius-control);
        margin-bottom: 1rem;
      }
    `,
  ],
})
export class ExpertEditorComponent implements OnInit {
  private api = inject(ApiService);
  private router = inject(Router);
  private route = inject(ActivatedRoute);
  private transloco = inject(TranslocoService);
  private modelService = inject(ModelService);

  private execGroup = viewChild(ExecutionGroupComponent);
  private toolsGroup = viewChild(ToolsGroupComponent);
  private advancedGroup = viewChild(AdvancedAccordionComponent);
  private subagentsEditor = viewChild(SubagentsEditorComponent);

  /** The backend picked in the Execution card, fed to the Advanced accordion so
   *  it greys the tools a lite tier cannot run. The selector is a level-1
   *  control; only the tuning that hangs off it stays under Advanced. */
  protected readonly execBackendOverride = computed(() => this.execGroup()?.workspaceBackend() ?? null);

  editingId = signal<string | null>(null);
  saving = signal(false);
  errorMessage = signal('');
  /** Set once, from router state, when this editor was landed on right after
   *  a fork-as-default that stripped something (see `forkNoticeTranslationArgs`
   *  and `ExpertEditorNavigationState`). Empty when there is nothing to say. */
  forkNotice = signal('');
  private slugTouched = false;

  // Async-loaded context.
  frameworkDefaults = signal<Record<string, unknown>>({});
  // Server-resolved effective models for the framework base — surfaces the model
  // each unpinned slot inherits, so the "(base default)" option names it instead
  // of being an opaque "default".
  frameworkEffectiveModels = signal<EffectiveModels | null>(null);
  settingsMatrix = signal<Record<string, Record<string, unknown>>>({});
  /** Write vocabulary for categories that refuse `true` (`shell`). Served on
   *  every expert detail; without it the Shell tick writes an expert fragment
   *  the loader refuses. */
  enumerateOnly = signal<Record<string, string[]> | null>(null);
  /** The expert's stored config fragment (the save baseline) — read through
   *  the legacy-tier lift, so a pre-U1 fragment is saved back in the new
   *  shape. {} on create. */
  rawFragment = signal<Record<string, unknown>>({});
  private prefillDone = false;

  /**
   * The server's answer for "what would an agent built from THIS expert bind?".
   *
   * Null until the first answer lands, and null again on failure — the tools
   * group falls back to its static per-mode list in that case, labelled, which
   * is what this surface showed unconditionally until now.
   */
  toolPreview = signal<SessionToolGroupsResponse | null>(null);
  private toolPreviewSerial = 0;
  /**
   * True once a resolved answer has anchored the tool switches.
   *
   * `applyPrefill` and this read both anchor, they race (an effect against an
   * HTTP response), and they are not equal in authority: `prefillFromConfig`
   * infers enablement from config names and on a real session over-reported by
   * 24 tools, so letting it land second would downgrade the answer back to the
   * guess this whole change removes.
   */
  private resolvedAnchorApplied = false;

  readonly models = this.modelService.models;
  readonly expertRoles = EXPERT_ROLES;

  /** Role tags (`worker` / `session` / `subagent`); the expert's own type is
   *  always among them. */
  readonly roleTags = signal<ExpertRole[]>(['worker']);

  // Capability grants → editor control-greying. undefined = loading; null = admin
  // (unrestricted, no gating). A resolved record gates per the deny-default PDP.
  capabilities = signal<Record<string, unknown> | null | undefined>(undefined);
  catalog = signal<GrantCatalog>({});
  /** What the group components consume; null ⇒ no gating (admin / loading). */
  gatedCapabilities = computed(() => this.capabilities() ?? null);
  /** True only when a model_selection restriction is in force. */
  isModelGated = computed(() => {
    const g = this.capabilities();
    return g != null && Array.isArray((g as Record<string, unknown>)['model_selection']);
  });
  modelAllowed = (id: string): boolean => isModelAllowed(this.capabilities() ?? null, id);

  /** "(base default)" option label, naming the model the unpinned slot inherits
   *  from the framework base (account/system chat pin) when known. */
  baseDefaultLabel(slot: 'model' | 'subagent'): string {
    return defaultModelOptionLabel(
      this.transloco.translate('experts.model.baseDefault'),
      this.frameworkEffectiveModels()?.[slot]?.model,
    );
  }

  hasRoleTag(role: ExpertRole): boolean {
    return role === this.form.expert_type || this.roleTags().includes(role);
  }

  toggleRoleTag(role: ExpertRole): void {
    if (role === this.form.expert_type) return; // locked on
    this.roleTags.update((tags) =>
      tags.includes(role) ? tags.filter((t) => t !== role) : [...tags, role],
    );
  }

  form: EditorForm = {
    name: '',
    display_name: '',
    description: '',
    icon: 'smart_toy',
    color: '#6B7280',
    tags: '',
    expert_type: 'worker',
    persona: '',
    instructions: '',
    strategic: '',
    tactical: '',
    summarization: '',
    model: '',
    subagentModel: '',
    configText: '',
  };

  private readonly expertType = signal<'worker' | 'session'>('worker');
  isEdit = computed(() => this.editingId() !== null);
  mode = computed<'job' | 'session'>(() => expertEditorMode(this.expertType()));
  /** Base config for the structured controls' "default: X" displays: the type
   *  base merged with the expert's own fragment. */
  baseForGroups = computed(() => deepMergeConfig(this.frameworkDefaults(), this.rawFragment()));

  constructor() {
    // Router state only exists on `Router.getCurrentNavigation()` DURING the
    // navigation that carried it — here, in the constructor of the routed
    // component — and is gone by `ngOnInit`. That timing is what makes this
    // notice show up exactly once: a plain page reload starts a fresh
    // navigation with no state, so it does not resurface on refresh the way
    // reading `history.state` later would.
    const navState = this.router.getCurrentNavigation()?.extras
      .state as ExpertEditorNavigationState | undefined;
    const notice = forkNoticeTranslationArgs(navState?.dropped);
    if (notice) {
      const [key, params] = notice;
      this.forkNotice.set(this.transloco.translate(key, params));
    }

    // Prefill the structured controls once the fragment is loaded AND the
    // view-children exist (defeats the ViewChild-null race on fast responses).
    effect(() => {
      const frag = this.rawFragment();
      const ready =
        this.execGroup() && this.toolsGroup() && this.advancedGroup() && this.subagentsEditor();
      if (!this.prefillDone && ready && Object.keys(frag).length) {
        this.prefillDone = true;
        this.applyPrefill(frag);
      }
    });
  }

  ngOnInit(): void {
    this.modelService.load();
    // Author's capabilities → grey controls they lack grants for. Fail-open on
    // error (null = no gating); the save-time 422 remains the backstop.
    this.api.getMyCapabilities().subscribe((c) => {
      this.capabilities.set(c ? c.grants : null);
      this.catalog.set(c?.catalog ?? {});
    });
    // Drives the structured controls' inherited-value displays. The request is
    // repeated when the expert type changes because worker and session experts
    // intentionally inherit from different conservative bases.
    this.loadFrameworkDefaults('worker');

    const id = this.route.snapshot.paramMap.get('id');
    if (!id) {
      // Create: no fragment will ever arrive, so this is the only read. The
      // answer is the bare base, which is exactly what a new expert resolves to.
      this.loadToolPreview();
      return;
    }
    this.editingId.set(id);
    // Prefill from the export bundle = the RAW fragment (never the merged result).
    this.api.exportExpert(id).subscribe((bundle) => {
      const b = (bundle ?? {}) as Record<string, unknown>;
      this.form.name = (b['name'] as string) ?? '';
      this.form.display_name = (b['display_name'] as string) ?? '';
      this.form.description = (b['description'] as string) ?? '';
      this.form.icon = (b['icon'] as string) ?? 'smart_toy';
      this.form.color = (b['color'] as string) ?? '#6B7280';
      const {roles, free} = splitTags(Array.isArray(b['tags']) ? (b['tags'] as string[]) : []);
      this.form.tags = free;
      this.roleTags.set(roles);
      this.form.expert_type = b['expert_type'] === 'session' ? 'session' : 'worker';
      this.expertType.set(this.form.expert_type);
      this.loadFrameworkDefaults(this.form.expert_type);
      const prompts = (b['prompts'] ?? {}) as Record<string, unknown>;
      this.form.persona = (prompts['persona'] as string) ?? '';
      this.form.instructions = (prompts['instructions'] as string) ?? '';
      this.form.strategic = (prompts['strategic'] as string) ?? '';
      this.form.tactical = (prompts['tactical'] as string) ?? '';
      this.form.summarization = (prompts['summarization'] as string) ?? '';
      // A pre-U1 fragment's per-phase tiers are lifted onto llm.model /
      // subagents.llm here, once, layer-locally (an explicit llm.model wins) —
      // the same mapping the loader applies — so the controls prefill from
      // the single-model shape and the save writes it back that way.
      const cfg = liftLegacyTiers((b['config'] ?? {}) as Record<string, unknown>);
      // Raw flap shows only the keys the structured controls don't own.
      const {rawRemainderText} = splitExpertConfig(cfg);
      this.form.configText = rawRemainderText;
      this.rawFragment.set(cfg); // triggers the prefill effect
      // The only read on this path: it needs both the stored type (which picks
      // the base) and the fragment, and neither exists until here.
      this.loadToolPreview();
    });
  }

  /** Seed the structured controls + model selects from the (lifted) fragment. */
  private applyPrefill(frag: Record<string, unknown>): void {
    const llm = (frag['llm'] ?? {}) as Record<string, unknown>;
    this.form.model = (llm['model'] as string) ?? '';
    const subagents = frag['subagents'] as SubagentsConfig | undefined;
    const rosterModel = subagents?.llm?.['model'];
    this.form.subagentModel =
      typeof rosterModel === 'string' && rosterModel !== SUBAGENT_INHERIT_MODEL ? rosterModel : '';
    this.subagentsEditor()?.prefill(subagents ?? null);

    // Skipped once the server has answered: see `resolvedAnchorApplied`. The
    // fragment still drives every other control here — only the tool switches
    // have a better source.
    if (!this.resolvedAnchorApplied) this.toolsGroup()?.prefillFromConfig(frag);
    this.advancedGroup()?.prefillFromConfig(frag);

    const exec = this.execGroup();
    exec?.resetAll();
    const autonomy = frag['autonomy'] as string | undefined;
    if (autonomy) exec?.autonomy.set(autonomy);
    const scholar = (frag['scholar'] as Record<string, unknown>)?.['enabled'] as boolean | undefined;
    if (scholar !== undefined) exec?.scholar.set(scholar);
    const verification = frag['verification'] as Record<string, unknown> | undefined;
    if (verification?.['enabled'] !== undefined) exec?.critic.set(verification['enabled'] as boolean);
    if (verification?.['max_rounds'] !== undefined) {
      exec?.criticRounds.set(verification['max_rounds'] as number);
    }
    const interactive = frag['interactive'] as Record<string, unknown> | undefined;
    const pm = interactive?.['permission_mode'] as string | undefined;
    if (pm) exec?.permissionMode.set(pm);
  }

  onDisplayName(v: string): void {
    this.form.display_name = v;
    if (!this.slugTouched && !this.isEdit()) {
      this.form.name = slugify(v);
    }
  }

  onNameEdit(v: string): void {
    this.slugTouched = true;
    this.form.name = v;
  }

  onTypeChange(v: 'worker' | 'session' | null): void {
    if (v !== 'worker' && v !== 'session') return;
    this.form.expert_type = v;
    this.expertType.set(v);
    this.loadFrameworkDefaults(v);
    // Structured state differs by mode (tool categories, autonomy vs permission
    // mode); reset so a worker→session switch on CREATE doesn't carry stale state.
    this.execGroup()?.resetAll();
    this.toolsGroup()?.resetAll();
    this.advancedGroup()?.resetAll();
    // The base changed, so the resolved toolset did too. Ordered after the
    // resets: they have just cleared the switches, so the incoming answer is
    // free to re-anchor them.
    this.resolvedAnchorApplied = false;
    this.loadToolPreview();
    this.form.model = '';
    this.form.subagentModel = '';
    // The new type's role chip locks on; whatever else was ticked stays.
    if (!this.roleTags().includes(v)) this.roleTags.update((tags) => [...tags, v]);
    // Phase-prompt overrides are mode-specific (strategic/tactical are worker-
    // only) — clear them so a worker→session switch on CREATE doesn't carry over.
    this.form.strategic = '';
    this.form.tactical = '';
    this.form.summarization = '';
  }

  /**
   * Ask the server what this expert's toolset actually resolves to.
   *
   * Payload rules live in `expertToolPreviewRequest`. `expert_type` there picks
   * the base (`worker_base` / `session_base`), which is the whole reason the two
   * expert types resolve to different toolsets — not `expert_type` itself, which
   * selects prompt leaves.
   *
   * Reflects the SAVED fragment: it is re-read when the base changes (type
   * switch) and when a stored expert loads, not on every switch click. A tick
   * moves its own row locally and immediately; what the user cannot compute in
   * their head is the base, the grant reasons and the counts, and those are what
   * this fetches. `origin: "prediction"` labels it either way — no agent exists.
   *
   * Serial-guarded so a slow answer for a type the user has switched away from
   * cannot paint over the current one.
   */
  private loadToolPreview(): void {
    const serial = ++this.toolPreviewSerial;
    this.api
      .previewToolGroups(expertToolPreviewRequest(this.expertType(), this.rawFragment()))
      .subscribe((preview) => {
        if (serial !== this.toolPreviewSerial) return;
        this.toolPreview.set(preview);
        const categories = preview?.categories;
        const group = this.toolsGroup();
        if (categories && group && !group.hasToolEdits()) {
          group.prefillFromResolved(categories);
          this.resolvedAnchorApplied = true;
        }
      });
  }

  private loadFrameworkDefaults(expertType: 'worker' | 'session'): void {
    const baseName = expertBaseConfigName(expertType);
    this.api.getExpertDetail(baseName).subscribe((d) => {
      // Ignore a slower response for the type the user just switched away from.
      if (this.expertType() !== expertType) return;
      this.frameworkDefaults.set((d?.config as Record<string, unknown>) ?? {});
      this.settingsMatrix.set(d?.settings_matrix ?? {});
      this.enumerateOnly.set(d?.enumerate_only ?? null);
      this.frameworkEffectiveModels.set(d?.effective_models ?? null);
    });
  }

  /** Assemble the prompts payload for save (see buildPromptsPayload). */
  private buildPrompts(): Record<string, string> {
    return buildPromptsPayload(this.form, this.mode());
  }

  save(): void {
    this.errorMessage.set('');
    const parsed = parseConfigText(this.form.configText);
    if (parsed.error) {
      this.errorMessage.set(`Config: ${parsed.error}`);
      return;
    }
    const roster = this.subagentsEditor();
    if (roster?.hasErrors()) {
      this.errorMessage.set(this.transloco.translate('experts.subagents.invalid'));
      return;
    }
    // Merge the structured controls' fragments + the model selects. The
    // subagents block is emitted whole (replaced, not merged — see
    // REPLACED_CONFIG_KEYS) so a removed roster entry stays removed.
    const groupOverrides = [
      this.execGroup()?.getOverrides() ?? {},
      this.toolsGroup()?.getOverrides() ?? {},
      this.advancedGroup()?.getOverrides() ?? {},
      buildModelFragment(this.form.model),
      {subagents: buildSubagentsFragment(roster?.getValue() ?? null, this.form.subagentModel)},
    ].reduce((acc, frag) => deepMergeConfig(acc, frag), {} as Record<string, unknown>);

    const config = stripEmptySubagents(
      assembleExpertConfig(this.rawFragment(), groupOverrides, parsed.config ?? {}),
    );

    const payload: ExpertCreateRequest = {
      name: this.form.name,
      display_name: this.form.display_name,
      expert_type: this.form.expert_type,
      description: this.form.description || null,
      icon: this.form.icon,
      color: this.form.color,
      tags: buildTagsPayload(this.form.expert_type, this.roleTags(), this.form.tags),
      config,
      prompts: this.buildPrompts(),
    };
    this.saving.set(true);
    const id = this.editingId();
    const obs = id
      ? this.api.updateExpert(id, payload as ExpertUpdateRequest)
      : this.api.createExpert(payload);
    obs.subscribe({
      next: () => this.router.navigate(['/experts']),
      error: (err) => {
        this.saving.set(false);
        const d = (err as {error?: {detail?: unknown}})?.error?.detail;
        this.errorMessage.set(
          typeof d === 'string' ? d : this.transloco.translate('experts.saveFailed'),
        );
      },
    });
  }

  cancel(): void {
    this.router.navigate(['/experts']);
  }
}
