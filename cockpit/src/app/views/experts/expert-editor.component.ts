import {Component, computed, effect, inject, OnInit, signal, viewChild} from '@angular/core';
import {ActivatedRoute, Router, RouterLink} from '@angular/router';
import {TranslocoPipe, TranslocoService} from '@jsverse/transloco';
import {ApiService} from '../../core/services/api.service';
import type {SessionToolGroupsResponse} from '../../core/services/api.service';
import {chatModelLabel, ModelService} from '../../core/services/model.service';
import type {
  EffectiveModels,
  ExpertCreateRequest,
  ExpertExportBundle,
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
import {AppBadgeComponent} from '../../ui/badge';
import {AppExpanderComponent} from '../../ui/expander';
import {AppSwitchComponent} from '../../ui/switch';
import {EditorHeaderComponent} from '../../shell/editor-header/editor-header.component';
import {ExecutionGroupComponent} from '../agent-settings/execution-group.component';
import {ToolsGroupComponent} from '../agent-settings/tools-group.component';
import {AdvancedAccordionComponent} from '../agent-settings/advanced-accordion.component';
import {deepMergeConfig} from '../agent-settings/config-merge';
import {assembleExpertConfig, liftLegacyTiers, splitExpertConfig} from './expert-config';
import {SubagentsEditorComponent} from './subagents-editor.component';
import {isModelAllowed} from '../agent-settings/capability-gates';
import {normalizePermissionMode} from '../../core/models/permission-mode';
import {defaultModelOptionLabel} from '../agent-settings/agent-settings.types';
import {expertPlansInPhases, phasesFragment} from '../../core/util/phases';

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
  summarization: string;
}

/**
 * Assemble the `prompts` save payload from the editor fields. Only non-empty
 * segments are emitted — an empty field inherits the framework default, and
 * because the API replaces the `prompts` column wholesale, a cleared field drops
 * its override. A worker's phase guidance is not an expert prompt: it is the
 * strategic-phase / tactical-phase skills.
 */
export function buildPromptsPayload(fields: PromptFields): Record<string, string> {
  const out: Record<string, string> = {};
  const add = (k: keyof PromptFields): void => {
    if (fields[k].trim()) out[k] = fields[k];
  };
  add('persona');
  add('instructions');
  add('summarization');
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
  /** The fork was the read-only view's "Duplicate to edit", not a new default. */
  duplicated?: boolean;
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
  duplicated = false,
): [key: string, params: Record<string, string>] | null {
  if (!dropped || dropped.length === 0) return null;
  const key = duplicated ? 'experts.duplicatedMissingGrants' : 'experts.forkedMissingGrants';
  return [key, {grants: dropped.join(', ')}];
}

/** The editor's heading: read-only view, edit of a stored expert, or create. */
export function expertEditorHeadingKey(readOnly: boolean, isEdit: boolean): string {
  if (readOnly) return 'experts.view';
  return isEdit ? 'experts.edit' : 'experts.new';
}

/**
 * Suggested looks for an expert. Any Material Symbols name and any hex colour
 * still work through the two small fields under the choices: these only save
 * the author from knowing icon names by heart.
 */
export const EXPERT_ICON_CHOICES: readonly string[] = [
  'smart_toy', 'code', 'terminal', 'bug_report', 'science', 'travel_explore',
  'lightbulb', 'menu_book', 'edit_note', 'palette', 'fact_check', 'gavel',
  'shield', 'support_agent', 'engineering', 'build', 'rate_review', 'radar',
];

/** The bundled experts' palette, plus the neutral default. */
export const EXPERT_COLOR_CHOICES: readonly string[] = [
  '#89b4fa', '#89dceb', '#94e2d5', '#a6e3a1', '#f9e2af',
  '#fab387', '#f38ba8', '#f5c2e7', '#cba6f7', '#6B7280',
];

/** How many settings a config fragment sets: its leaves (a list is one). */
export function countLeaves(value: unknown): number {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) return 1;
  return Object.values(value).reduce<number>((n, child) => n + countLeaves(child), 0);
}

/** Words in a prompt, for a section summary. */
export function wordCount(text: string): number {
  const trimmed = text.trim();
  return trimmed ? trimmed.split(/\s+/).length : 0;
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
    RouterLink,
    TranslocoPipe,
    AppBadgeComponent,
    AppButtonComponent,
    AppChipComponent,
    AppExpanderComponent,
    AppInputComponent,
    AppTextareaComponent,
    AppSelectComponent,
    AppFormFieldComponent,
    AppIconComponent,
    AppSwitchComponent,
    EditorHeaderComponent,
    ExecutionGroupComponent,
    ToolsGroupComponent,
    AdvancedAccordionComponent,
    SubagentsEditorComponent,
  ],
  template: `
    <app-editor-header
      [heading]="heading()"
      [backLabel]="'experts.title' | transloco"
      backLink="/experts"
    >
      @if (isEdit()) {
        <span editorIcon class="title-icon" [style.color]="form.color" aria-hidden="true">
          <app-icon size="lg">{{ form.icon || 'smart_toy' }}</app-icon>
        </span>
      }
      @if (isEdit()) {
        <span editorBadges class="title-badges">
          <app-badge class="type-badge" tone="neutral">{{ form.expert_type }}</app-badge>
          @if (readOnly()) {
            <app-badge tone="info" [title]="'experts.readonlyNotice' | transloco">{{ 'experts.editor.readOnly' | transloco }}</app-badge>
          }
        </span>
      }
      <div editorActions class="actions">
        @if (readOnly()) {
          <app-button variant="primary" [loading]="saving()" (clicked)="duplicate()">
            {{ 'experts.duplicateToEdit' | transloco }}
          </app-button>
        } @else {
          <app-button variant="secondary" (clicked)="cancel()">{{ 'experts.cancel' | transloco }}</app-button>
          <app-button variant="primary" [loading]="saving()" [disabled]="!!loadError()" (clicked)="save()">
            {{ 'experts.save' | transloco }}
          </app-button>
        }
      </div>
      @if (errorMessage()) {
        <div editorAlert class="banner err save-error" role="alert">{{ errorMessage() }}</div>
      }
    </app-editor-header>

    <div class="editor">
      @if (forkNotice()) {
        <div class="banner info">{{ forkNotice() }}</div>
      }
      @if (loadError()) {
        <div class="banner err">{{ loadError() }}</div>
      }

      <!-- Profile: what the expert is called and how it shows up in lists.
           Always in view; everything below is a section that opens. -->
      <section class="profile" [attr.aria-label]="'experts.editor.profile' | transloco">
        <div class="look" [class.only-avatar]="readOnly()">
          <div class="avatar" [style.color]="form.color" aria-hidden="true">
            <app-icon size="xl">{{ form.icon || 'smart_toy' }}</app-icon>
          </div>
          @if (!readOnly()) {
            <div class="swatches" role="group" [attr.aria-label]="'experts.editor.color' | transloco">
              @for (c of colorChoices; track c) {
                <button
                  type="button"
                  class="swatch"
                  [class.selected]="isColor(c)"
                  [style.background]="c"
                  [attr.aria-label]="c"
                  [attr.aria-pressed]="isColor(c)"
                  (click)="form.color = c"
                ></button>
              }
            </div>
            <div class="icon-choices" role="group" [attr.aria-label]="'experts.editor.icon' | transloco">
              @for (i of iconChoices; track i) {
                <button
                  type="button"
                  class="icon-choice"
                  [class.selected]="form.icon === i"
                  [attr.aria-label]="i"
                  [attr.aria-pressed]="form.icon === i"
                  (click)="form.icon = i"
                >
                  <app-icon size="sm">{{ i }}</app-icon>
                </button>
              }
            </div>
            <div class="look-custom">
              <app-input
                size="sm"
                [value]="form.icon"
                [ariaLabel]="'experts.editor.iconName' | transloco"
                [placeholder]="'experts.editor.iconName' | transloco"
                (valueChange)="form.icon = $event"
              />
              <app-input
                size="sm"
                [value]="form.color"
                [ariaLabel]="'experts.editor.colorHex' | transloco"
                placeholder="#6B7280"
                (valueChange)="form.color = $event"
              />
            </div>
          }
        </div>

        <div class="profile-fields">
          <div class="pair wide-narrow">
            <app-form-field [label]="'experts.editor.displayName' | transloco" [required]="true">
              <app-input [value]="form.display_name" [readonly]="readOnly()" (valueChange)="onDisplayName($event)" />
            </app-form-field>
            <app-form-field
              [label]="'experts.editor.type' | transloco"
              [required]="true"
              [hint]="isEdit() ? '' : ('experts.editor.typeHint' | transloco)"
            >
              <app-select [value]="form.expert_type" [disabled]="isEdit()" (valueChange)="onTypeChange($event)">
                <option value="worker">{{ 'experts.editor.typeWorker' | transloco }}</option>
                <option value="session">{{ 'experts.editor.typeSession' | transloco }}</option>
              </app-select>
            </app-form-field>
          </div>
          <app-form-field
            [label]="'experts.editor.description' | transloco"
            [hint]="'experts.editor.descriptionHint' | transloco"
          >
            <app-input [value]="form.description" [readonly]="readOnly()" (valueChange)="form.description = $event" />
          </app-form-field>
          <div class="pair">
            <app-form-field
              [label]="'experts.editor.id' | transloco"
              [required]="true"
              [hint]="isEdit() ? '' : ('experts.editor.idHint' | transloco)"
            >
              <app-input class="mono" [value]="form.name" [disabled]="isEdit()" (valueChange)="onNameEdit($event)" />
            </app-form-field>
            <app-form-field [label]="'experts.editor.tags' | transloco" [hint]="'experts.editor.tagsHint' | transloco">
              <app-input
                [value]="form.tags"
                [readonly]="readOnly()"
                [placeholder]="'experts.tags.freePlaceholder' | transloco"
                (valueChange)="form.tags = $event"
              />
            </app-form-field>
          </div>
          <!-- Role tags: the expert's own type locked on. A soft filter for the
               pickers; every expert stays usable in every role (U1 D4). -->
          <app-form-field [label]="'experts.editor.listedFor' | transloco" [hint]="'experts.editor.listedForHint' | transloco">
            <div class="role-chips">
              @for (role of expertRoles; track role) {
                <app-chip
                  [selected]="hasRoleTag(role)"
                  [disabled]="readOnly() || role === form.expert_type"
                  [ariaLabel]="role === form.expert_type ? ('experts.tags.roleLocked' | transloco: {role}) : role"
                  (clicked)="toggleRoleTag(role)"
                >{{ role }}</app-chip>
              }
            </div>
          </app-form-field>
        </div>
      </section>

      <app-expander
        icon="psychology"
        [heading]="'experts.editor.sections.persona.title' | transloco"
        [summary]="personaSummary()"
        [(expanded)]="personaOpen"
      >
        <div class="section">
          <app-form-field
            [label]="'experts.editor.persona' | transloco"
            [hint]="'experts.editor.personaHint' | transloco"
          >
            <app-textarea
              [value]="form.persona"
              [rows]="12"
              [readonly]="readOnly()"
              [placeholder]="'experts.editor.personaPlaceholder' | transloco"
              (valueChange)="form.persona = $event"
            />
          </app-form-field>
          <div class="field-with-note">
            <app-form-field
              [label]="'experts.editor.instructions' | transloco"
              [optional]="'experts.editor.optional' | transloco"
              [hint]="'experts.editor.instructionsHint' | transloco"
            >
              <app-textarea
                [value]="form.instructions"
                [rows]="6"
                [readonly]="readOnly()"
                (valueChange)="form.instructions = $event"
              />
            </app-form-field>
            <!-- Expert instructions are being phased out in favour of skills
                 (vault: issues/expert_instructions_phase_out.md). A session
                 never reads them at all, so that expert is told so. -->
            <p class="deprecation">
              <app-badge tone="warning" size="xs">{{ 'experts.editor.deprecated' | transloco }}</app-badge>
              <span>
                {{ (mode() === 'session' ? 'experts.editor.instructionsDeprecatedSession' : 'experts.editor.instructionsDeprecated') | transloco }}
                <a routerLink="/skills">{{ 'experts.editor.instructionsDeprecatedLink' | transloco }}</a>
              </span>
            </p>
          </div>
          <div class="more">
            <button type="button" class="more-toggle" [attr.aria-expanded]="promptsMoreOpen()" (click)="promptsMoreOpen.set(!promptsMoreOpen())">
              <app-icon size="sm" class="more-chevron" [class.open]="promptsMoreOpen()">expand_more</app-icon>
              {{ 'experts.editor.more' | transloco }}
              @if (form.summarization.trim()) {
                <span class="more-count">{{ 'experts.editor.moreSet' | transloco: {n: 1} }}</span>
              }
            </button>
            <div class="more-body" [hidden]="!promptsMoreOpen()">
              <app-form-field
                [label]="'experts.editor.summarization' | transloco"
                [hint]="'experts.editor.summarizationHint' | transloco"
              >
                <app-textarea
                  [value]="form.summarization"
                  [rows]="5"
                  [readonly]="readOnly()"
                  [placeholder]="'experts.editor.summarizationPlaceholder' | transloco"
                  (valueChange)="form.summarization = $event"
                />
              </app-form-field>
            </div>
          </div>
        </div>
      </app-expander>

      <!-- ONE model since U1 (llm.model, worker and session alike) plus, for a
           worker expert, the roster-wide subagent model (subagents.llm.model). -->
      <app-expander
        icon="neurology"
        [heading]="'experts.model.title' | transloco"
        [summary]="modelSummary()"
        [(expanded)]="modelOpen"
      >
        <div class="section">
          <div class="pair">
            <app-form-field [label]="'experts.model.model' | transloco">
              <app-select
                [value]="form.model"
                [disabled]="readOnly() || isModelGated()"
                (valueChange)="form.model = $event ?? ''"
              >
                <option value="">{{ baseDefaultLabel('model') }}</option>
                @for (g of models(); track g.group) {
                  <optgroup [label]="g.group">
                    @for (m of g.models; track m) {
                      <option [value]="m" [disabled]="!modelAllowed(m)">{{ modelLabel(g, m) }}</option>
                    }
                  </optgroup>
                }
              </app-select>
            </app-form-field>
            @if (mode() === 'job') {
              <app-form-field
                [label]="'experts.model.subagent' | transloco"
                [hint]="'experts.model.subagentHint' | transloco"
              >
                <app-select
                  [value]="form.subagentModel"
                  [disabled]="readOnly() || isModelGated()"
                  (valueChange)="form.subagentModel = $event ?? ''"
                >
                  <option value="">{{ baseDefaultLabel('subagent') }}</option>
                  @for (g of models(); track g.group) {
                    <optgroup [label]="g.group">
                      @for (m of g.models; track m) {
                        <option [value]="m" [disabled]="!modelAllowed(m)">{{ modelLabel(g, m) }}</option>
                      }
                    </optgroup>
                  }
                </app-select>
              </app-form-field>
            }
          </div>
          @if (isModelGated()) {
            <p class="lock-hint"><app-icon size="xs">lock</app-icon> {{ 'grants.locked.model_selection' | transloco }}</p>
          }
        </div>
      </app-expander>

      <!-- Structured config: reuses the launch-flow groups. [config] = base ⊕ fragment
           drives "default: X" displays; getOverrides() + the model selects feed the
           saved config (see save()). The expander hides a closed section with the
           hidden attribute, so every group stays alive for prefill and save. -->
      <app-expander
        icon="build"
        [heading]="'experts.editor.sections.tools.title' | transloco"
        [summary]="toolsSummary()"
        [(expanded)]="toolsOpen"
      >
        <div class="section">
          <app-tools-group
            [config]="baseForGroups()"
            [mode]="mode()"
            [disabled]="readOnly()"
            [showGroupLabel]="false"
            [enumerateOnly]="enumerateOnly()"
            [gatedCapabilities]="gatedCapabilities()"
            [resolved]="toolPreview()"
            [readsResolvedToolset]="true"
          />
        </div>
      </app-expander>

      <!-- The subagent roster (config.subagents.default / .roster). -->
      <app-expander
        icon="account_tree"
        [heading]="'experts.subagents.title' | transloco"
        [summary]="subagentsSummary()"
        [(expanded)]="subagentsOpen"
      >
        <div class="section">
          <p class="hint">{{ 'experts.subagents.hint' | transloco }}</p>
          <app-subagents-editor
            [models]="models()"
            [modelGated]="isModelGated()"
            [modelAllowed]="modelAllowed"
            [disabled]="readOnly()"
          />
        </div>
      </app-expander>

      <app-expander
        icon="tune"
        [heading]="(mode() === 'job' ? 'experts.editor.sections.defaults.titleJob' : 'experts.editor.sections.defaults.titleSession') | transloco"
        [summary]="defaultsSummary()"
        [(expanded)]="defaultsOpen"
      >
        <div class="section">
          <p class="hint">
            {{ (mode() === 'job' ? 'experts.editor.sections.defaults.hintJob' : 'experts.editor.sections.defaults.hintSession') | transloco }}
          </p>
          <!-- The top-level phases key (core/util/phases.ts). Off when the
               config has none. Job-only, like autonomy: a session expert's
               stored value is kept as it is. -->
          @if (mode() === 'job') {
            <app-form-field class="phases-field" [hint]="'experts.editor.phasesHint' | transloco">
              <app-switch
                data-testid="expert-phases"
                [checked]="phases()"
                [disabled]="readOnly()"
                (changed)="phases.set($event)"
              >{{ 'experts.editor.phases' | transloco }}</app-switch>
            </app-form-field>
          }
          <app-execution-group
            [config]="baseForGroups()"
            [mode]="mode()"
            [disabled]="readOnly()"
            [showGroupLabel]="false"
            [showProjectMemory]="false"
            [gatedCapabilities]="gatedCapabilities()"
            [catalog]="catalog()"
            [workspaceBackendLabel]="'agentSettings.execution.workspaceRecommended'"
          />
        </div>
      </app-expander>

      <app-expander
        icon="settings"
        [heading]="'experts.editor.sections.advanced.title' | transloco"
        [summary]="advancedSummary()"
        [(expanded)]="advancedOpen"
      >
        <div class="section">
          <app-advanced-accordion
            [flat]="true"
            [config]="baseForGroups()"
            [mode]="mode()"
            [disabled]="readOnly()"
            [settingsMatrix]="settingsMatrix()"
            [modelOverride]="form.model || null"
            [backendOverride]="execBackendOverride()"
          />
          <app-form-field
            [label]="'experts.editor.otherKeys' | transloco"
            [hint]="'experts.editor.otherKeysHint' | transloco"
          >
            <app-textarea
              class="mono"
              [value]="form.configText"
              [rows]="6"
              [readonly]="readOnly()"
              (valueChange)="form.configText = $event"
              placeholder='{ "instruction_files": [] }'
            />
          </app-form-field>
        </div>
      </app-expander>
    </div>
  `,
  styles: [
    `
      :host {
        display: block;
        height: 100%;
        overflow-y: auto;
        --editor-width: 920px;
      }
      .editor {
        max-width: var(--editor-width);
        margin: 0 auto;
        padding: 1rem 1.5rem 3rem;
        display: flex;
        flex-direction: column;
        gap: 0.75rem;
      }
      .title-icon {
        display: inline-flex;
        flex-shrink: 0;
      }
      .title-badges {
        display: inline-flex;
        gap: 0.375rem;
        flex-shrink: 0;
      }
      .actions {
        display: flex;
        gap: 0.5rem;
      }

      /* Profile */
      .profile {
        display: flex;
        gap: 1.5rem;
        padding: 1.25rem;
        background: var(--panel-bg);
        border: 1px solid var(--border-hairline);
        border-radius: var(--radius-surface);
      }
      .look {
        flex: 0 0 12.75rem;
        display: flex;
        flex-direction: column;
        gap: 0.75rem;
      }
      /* Read-only: nothing to pick, so the column is just the avatar. */
      .look.only-avatar {
        flex-basis: auto;
      }
      .avatar {
        display: grid;
        place-items: center;
        width: 4.5rem;
        height: 4.5rem;
        background: var(--surface-0);
        border: 1px solid var(--border-color);
        border-radius: var(--radius-surface);
      }
      .swatches,
      .icon-choices {
        display: flex;
        flex-wrap: wrap;
        gap: 0.375rem;
      }
      .swatch {
        width: 1.25rem;
        height: 1.25rem;
        padding: 0;
        border: 2px solid var(--panel-bg);
        border-radius: 50%;
        cursor: pointer;
      }
      .swatch.selected {
        box-shadow: 0 0 0 2px var(--text-primary);
      }
      .icon-choice {
        display: grid;
        place-items: center;
        width: 1.75rem;
        height: 1.75rem;
        padding: 0;
        border: 1px solid transparent;
        border-radius: var(--radius-control);
        background: none;
        color: var(--text-secondary);
        cursor: pointer;
      }
      .icon-choice:hover {
        background: var(--hover);
        color: var(--text-primary);
      }
      .icon-choice.selected {
        border-color: var(--accent-color);
        color: var(--text-primary);
        background: color-mix(in srgb, var(--accent-color) 12%, transparent);
      }
      .swatch:focus-visible,
      .icon-choice:focus-visible,
      .more-toggle:focus-visible {
        outline: 2px solid var(--accent-color);
        outline-offset: 2px;
      }
      .look-custom {
        display: flex;
        flex-direction: column;
        gap: 0.375rem;
      }
      .profile-fields {
        flex: 1;
        min-width: 0;
        display: flex;
        flex-direction: column;
        gap: 0.875rem;
      }
      .pair {
        display: grid;
        grid-template-columns: 1fr 1fr;
        gap: 0.875rem;
        align-items: start;
      }
      .pair.wide-narrow {
        grid-template-columns: 2fr 1fr;
      }
      .role-chips {
        display: flex;
        flex-wrap: wrap;
        gap: 0.5rem;
      }
      .mono {
        font-family: var(--font-mono, monospace);
      }

      /* Sections */
      app-expander {
        display: block;
      }
      .section {
        display: flex;
        flex-direction: column;
        gap: 1rem;
      }
      .field-with-note {
        display: flex;
        flex-direction: column;
        gap: 0.375rem;
      }
      .deprecation {
        display: flex;
        align-items: baseline;
        gap: 0.5rem;
        margin: 0;
        color: var(--text-muted);
        font-size: 0.8125rem;
        line-height: 1.5;
      }
      .deprecation app-badge {
        flex-shrink: 0;
      }
      .deprecation a {
        color: var(--accent-color);
        text-decoration: none;
        white-space: nowrap;
      }
      .deprecation a:hover {
        text-decoration: underline;
      }
      .hint {
        margin: 0;
        max-width: 72ch;
        color: var(--text-muted);
        font-size: 0.8125rem;
        line-height: 1.5;
      }
      .lock-hint {
        display: flex;
        align-items: center;
        gap: 0.375rem;
        margin: 0;
        color: var(--text-muted);
        font-size: 0.8125rem;
      }

      /* One more level, then no further: the rarely-touched fields of a section. */
      .more {
        border-top: 1px solid var(--border-hairline);
        padding-top: 0.5rem;
      }
      .more-toggle {
        display: inline-flex;
        align-items: center;
        gap: 0.25rem;
        padding: 0.25rem 0.5rem 0.25rem 0.125rem;
        border: 0;
        border-radius: var(--radius-control);
        background: none;
        color: var(--text-secondary);
        font: inherit;
        font-size: 0.8125rem;
        font-weight: 500;
        cursor: pointer;
      }
      .more-toggle:hover {
        color: var(--text-primary);
      }
      .more-chevron {
        transition: transform 0.15s ease;
      }
      .more-chevron.open {
        transform: rotate(180deg);
      }
      .more-count {
        margin-left: 0.375rem;
        color: var(--text-muted);
        font-weight: 400;
      }
      .more-body {
        padding-top: 0.75rem;
      }
      .more-body[hidden] {
        display: none;
      }

      .banner {
        padding: 0.5rem 0.75rem;
        border-radius: var(--radius-control);
        font-size: 0.875rem;
      }
      .banner.err {
        background: var(--danger-tint);
        color: var(--danger);
      }
      .banner.info {
        background: var(--info-tint);
        color: var(--info);
      }

      @media (max-width: 720px) {
        .editor {
          padding: 0.75rem 1rem 3rem;
        }
        /* The Type field is right below; the title gets the room. */
        .type-badge {
          display: none;
        }
        .profile {
          flex-direction: column;
          padding: 1rem;
        }
        /* Name first on a phone; the look follows. */
        .look {
          order: 2;
          flex-basis: auto;
        }
        .pair,
        .pair.wide-narrow {
          grid-template-columns: 1fr;
        }
      }
      @media (prefers-reduced-motion: reduce) {
        .more-chevron {
          transition: none;
        }
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
  /** The `/experts/:id/view` route: every control read-only, no save — the
   *  way to read an expert the caller may not edit (every bundled one). */
  readonly readOnly = signal(this.route.snapshot.data['readOnly'] === true);
  saving = signal(false);
  errorMessage = signal('');
  /** Set when the stored expert could not be read. Save stays off then: the
   *  form is still blank, and saving it would wipe the expert's prompts. */
  loadError = signal('');
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
  /** Option text: the catalog label; the value stays the model id. */
  readonly modelLabel = chatModelLabel;
  readonly expertRoles = EXPERT_ROLES;

  /** Role tags (`worker` / `session` / `subagent`); the expert's own type is
   *  always among them. */
  readonly roleTags = signal<ExpertRole[]>(['worker']);

  /** "Plans in phases": the config's top-level `phases`, off when absent.
   *  Saved through `phasesFragment` (see save()). */
  readonly phases = signal(false);

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
    summarization: '',
    model: '',
    subagentModel: '',
    configText: '',
  };

  private readonly expertType = signal<'worker' | 'session'>('worker');
  isEdit = computed(() => this.editingId() !== null);
  headingKey = computed(() => expertEditorHeadingKey(this.readOnly(), this.isEdit()));

  readonly iconChoices = EXPERT_ICON_CHOICES;
  readonly colorChoices = EXPERT_COLOR_CHOICES;

  /** Which sections are open. Persona is where an expert is written, so it
   *  starts open; every other section shows its one-line summary until asked.
   *  A closed section keeps its controls alive (the expander hides, it does
   *  not remove), so prefill and save see every group either way. */
  readonly personaOpen = signal(true);
  readonly promptsMoreOpen = signal(false);
  readonly modelOpen = signal(false);
  readonly toolsOpen = signal(false);
  readonly subagentsOpen = signal(false);
  readonly defaultsOpen = signal(false);
  readonly advancedOpen = signal(false);

  /** The page title: the expert's own name once it has one. */
  heading(): string {
    if (!this.isEdit()) return this.transloco.translate('experts.new');
    return this.form.display_name.trim() || this.transloco.translate(this.headingKey());
  }

  isColor(color: string): boolean {
    return this.form.color.trim().toLowerCase() === color.toLowerCase();
  }

  // --- Section summaries: one line each, readable while the section is
  // closed. Methods, not computeds: `form` is a plain object, so only a
  // template read picks up its edits.

  private summary(key: string, params?: Record<string, unknown>): string {
    return this.transloco.translate(`experts.editor.summary.${key}`, params);
  }

  private modelName(id: string): string {
    for (const group of this.models()) {
      if (group.models.includes(id)) return chatModelLabel(group, id);
    }
    return id;
  }

  personaSummary(): string {
    const persona = wordCount(this.form.persona);
    const instructions = wordCount(this.form.instructions);
    const summarization = !!this.form.summarization.trim();
    if (!persona && !instructions && !summarization) return this.summary('promptsEmpty');
    const parts = [persona ? this.summary('persona', {words: persona}) : this.summary('personaNone')];
    if (instructions) parts.push(this.summary('instructions', {words: instructions}));
    if (summarization) parts.push(this.summary('summarization'));
    return parts.join(' · ');
  }

  modelSummary(): string {
    const main = this.form.model ? this.modelName(this.form.model) : this.baseDefaultLabel('model');
    if (this.mode() !== 'job' || !this.form.subagentModel) return main;
    return `${main} · ${this.summary('subagentModel', {model: this.modelName(this.form.subagentModel)})}`;
  }

  toolsSummary(): string {
    const on = this.toolsGroup()?.onSummary();
    if (!on) return '';
    return on.tools
      ? this.summary('tools', {groups: on.categories, tools: on.tools})
      : this.summary('toolsNoCount', {groups: on.categories});
  }

  subagentsSummary(): string {
    const names = this.subagentsEditor()?.names() ?? [];
    if (!names.length) return this.summary('subagentsNone');
    const shown = names.slice(0, 4).join(', ');
    return names.length > 4 ? this.summary('subagentsMore', {names: shown, n: names.length - 4}) : shown;
  }

  defaultsSummary(): string {
    const exec = this.execGroup();
    if (!exec) return '';
    const parts: string[] = [];
    if (this.mode() === 'job') {
      const autonomy = exec.autonomy() ?? exec.resolvedAutonomy();
      parts.push(this.summary('autonomy', {value: this.transloco.translate(`agentSettings.autonomy.${autonomy}.label`)}));
      if (this.phases()) parts.push(this.summary('phases'));
    } else {
      const permission = exec.permissionMode() ?? exec.resolvedPermissionMode();
      parts.push(
        this.summary('permission', {value: this.transloco.translate(`agentSettings.permissionModes.${permission}.label`)}),
      );
    }
    parts.push(this.summary('workspace', {value: exec.tierLabel(exec.effectiveBackend())}));
    if (this.mode() === 'job') {
      if (exec.scholar() ?? exec.resolvedScholar()) parts.push(this.summary('scholar'));
      if (exec.effectiveCritic()) parts.push(this.summary('critic'));
    }
    return parts.join(' · ');
  }

  advancedSummary(): string {
    const advanced = this.advancedGroup();
    const other = parseConfigText(this.form.configText).config ?? {};
    const n = (advanced ? countLeaves(advanced.getOverrides()) : 0) + Object.keys(other).length;
    return n ? this.summary('advancedSet', {n}) : this.summary('advancedDefaults');
  }
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
    const notice = forkNoticeTranslationArgs(navState?.dropped, navState?.duplicated);
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
    // Prefill from the source bundle = the RAW fragment (never the merged
    // result). Not the export: that 409s for every bundled expert and every
    // copy of one, which used to leave this form blank.
    this.api.getExpertSource(id).subscribe({
      next: (b) => this.applySource(b),
      error: (err) => {
        const d = (err as {error?: {detail?: unknown}})?.error?.detail;
        const detail = typeof d === 'string' ? d : String((err as {message?: unknown})?.message ?? '');
        this.loadError.set(this.transloco.translate('experts.loadFailed', {detail}));
      },
    });
  }

  private applySource(b: ExpertExportBundle): void {
    this.form.name = b.name ?? '';
    this.form.display_name = b.display_name ?? '';
    this.form.description = b.description ?? '';
    this.form.icon = b.icon ?? 'smart_toy';
    this.form.color = b.color ?? '#6B7280';
    const {roles, free} = splitTags(Array.isArray(b.tags) ? b.tags : []);
    this.form.tags = free;
    this.roleTags.set(roles);
    this.form.expert_type = b.expert_type === 'session' ? 'session' : 'worker';
    this.expertType.set(this.form.expert_type);
    this.loadFrameworkDefaults(this.form.expert_type);
    const prompts = b.prompts ?? {};
    this.form.persona = (prompts['persona'] as string) ?? '';
    this.form.instructions = (prompts['instructions'] as string) ?? '';
    this.form.summarization = (prompts['summarization'] as string) ?? '';
    // A pre-U1 fragment's per-phase tiers are lifted onto llm.model /
    // subagents.llm here, once, layer-locally (an explicit llm.model wins) —
    // the same mapping the loader applies — so the controls prefill from
    // the single-model shape and the save writes it back that way.
    const cfg = liftLegacyTiers(b.config ?? {});
    this.phases.set(expertPlansInPhases(cfg));
    // Raw flap shows only the keys the structured controls don't own.
    const {rawRemainderText} = splitExpertConfig(cfg);
    this.form.configText = rawRemainderText;
    this.rawFragment.set(cfg); // triggers the prefill effect
    // The only read on this path: it needs both the stored type (which picks
    // the base) and the fragment, and neither exists until here.
    this.loadToolPreview();
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
    // A config saved before the rename says `supervised`; saving writes read_only.
    if (pm) exec?.permissionMode.set(normalizePermissionMode(pm) ?? pm);
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
    this.phases.set(expertPlansInPhases(this.rawFragment()));
    // The base changed, so the resolved toolset did too. Ordered after the
    // resets: they have just cleared the switches, so the incoming answer is
    // free to re-anchor them.
    this.resolvedAnchorApplied = false;
    this.loadToolPreview();
    this.form.model = '';
    this.form.subagentModel = '';
    // The new type's role chip locks on; whatever else was ticked stays.
    if (!this.roleTags().includes(v)) this.roleTags.update((tags) => [...tags, v]);
    // The summarization override is written for one mode's context — clear it
    // so a worker→session switch on CREATE doesn't carry it over.
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
    return buildPromptsPayload(this.form);
  }

  save(): void {
    if (this.readOnly() || this.loadError()) return;
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
      // Job-only: a session expert's stored phases rides along in the
      // managed base untouched.
      this.mode() === 'job' ? phasesFragment(this.phases(), this.rawFragment()) : {},
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

  /** The read-only view's way to edit: copy the expert, open the copy here. */
  duplicate(): void {
    const id = this.editingId();
    if (!id) return;
    this.errorMessage.set('');
    this.saving.set(true);
    this.api.duplicateExpert(id).subscribe({
      next: (copy) => {
        const state: ExpertEditorNavigationState = {dropped: copy.dropped, duplicated: true};
        this.router.navigate(['/experts', copy.id, 'edit'], {state});
      },
      error: (err) => {
        this.saving.set(false);
        const d = (err as {error?: {detail?: unknown}})?.error?.detail;
        this.errorMessage.set(
          typeof d === 'string' ? d : this.transloco.translate('experts.duplicateFailed'),
        );
      },
    });
  }

  cancel(): void {
    this.router.navigate(['/experts']);
  }
}
