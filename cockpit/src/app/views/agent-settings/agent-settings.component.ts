import {Component, computed, contentChild, inject, input, linkedSignal, output, signal, viewChild, ViewChild} from '@angular/core';
import {TranslocoPipe, TranslocoService} from '@jsverse/transloco';
import {toSignal} from '@angular/core/rxjs-interop';
import {Datasource, EffectiveModels} from '../../core/models/api.model';
import type {
    SessionToolCategory,
    SessionToolGroupsResponse,
} from '../../core/services/api.service';
import {readConfigPath, SettingsMode, TierReachability, WORKSPACE_BACKENDS} from './agent-settings.types';
import {ExecutionGroupComponent} from './execution-group.component';
import {ModelGroupComponent} from './model-group.component';
import {delegationCapScopeForMode, ToolsGroupComponent} from './tools-group.component';
import {DatasourcesGroupComponent} from './datasources-group.component';
import {InstructionsTabComponent} from './instructions-tab.component';
import {AdvancedAccordionComponent} from './advanced-accordion.component';
import {WorkspacePickerComponent} from './workspace-picker.component';
import {AppExpanderComponent} from '../../ui/expander';
import {AppIconComponent} from '../../ui/icon';
import type {BadgeTone} from '../../ui/badge';

/**
 * The execution settings: who the agent is, where it works and what it can
 * reach — one component for job creation, session creation and the live
 * session pane (creation_ui_expert_workspace_connectors.md).
 *
 * Layout: the task (or session) block on top, then three expanders —
 * **Expert**, **Workspace**, **Connectors** — each with a one-line summary
 * that stays readable while collapsed. A collapsed section keeps its groups
 * mounted: they hold the form state, and the hosts read it at submit time
 * through the methods below.
 *
 * The pages project their own pieces into named slots:
 * - `[settingsTop]` — the task fields (prompt, name, project …), shown first;
 * - `[settingsTopAside]` — fields that sit beside autonomy (priority);
 * - `[expertPicker]` — the template grid (create forms);
 * - `[workspacePicker]` — the `app-workspace-picker` (create forms);
 * - `[connectorsExtra]` — controls that belong under the connector list.
 *
 * Live mode shows every section too. What a running session cannot change is
 * shown locked with its reason, never hidden.
 */
@Component({
  selector: 'app-agent-settings',
  standalone: true,
  imports: [
    ExecutionGroupComponent,
    ModelGroupComponent,
    ToolsGroupComponent,
    DatasourcesGroupComponent,
    InstructionsTabComponent,
    AdvancedAccordionComponent,
    AppExpanderComponent,
    AppIconComponent,
    TranslocoPipe,
  ],
  template: `
    <div class="exec-settings" [class.live]="mode() === 'live'">
      <!-- The task (job) or the session itself -->
      <section class="es-block" [attr.aria-label]="(mode() === 'job' ? 'agentSettings.sections.task.title' : 'agentSettings.sections.session.title') | transloco">
        <ng-content select="[settingsTop]" />
        <div class="es-row">
          <div class="es-row-main">
            <app-execution-group
              #execTask
              section="task"
              [config]="config()"
              [mode]="mode()"
              [disabled]="disabled()"
              [gatedCapabilities]="gatedCapabilities()"
              (change)="onChange()"
            />
          </div>
          <ng-content select="[settingsTopAside]" />
        </div>
        @if (mode() === 'live' && projectName()) {
          <div class="locked-field">
            <span class="locked-label">
              {{ 'agentSettings.locked.project' | transloco }}
              <span class="locked-reason"><app-icon size="xs">lock</app-icon>{{ 'agentSettings.locked.atCreation' | transloco }}</span>
            </span>
            <span class="locked-value">{{ projectName() }}</span>
          </div>
        }
        @if (mode() === 'job') {
          <div class="es-more">
            <button type="button" class="es-more-toggle" [attr.aria-expanded]="taskMoreOpen()" (click)="taskMoreOpen.set(!taskMoreOpen())">
              <app-icon size="sm" class="es-more-chevron" [class.open]="taskMoreOpen()">expand_more</app-icon>
              {{ 'agentSettings.sections.task.more' | transloco }}
            </button>
            <div class="es-more-body" [hidden]="!taskMoreOpen()">
              <app-execution-group
                #execTaskMore
                section="taskMore"
                [config]="config()"
                [mode]="mode()"
                [disabled]="disabled()"
                [showProjectMemory]="showProjectMemory()"
                [gatedCapabilities]="gatedCapabilities()"
                (change)="onChange()"
              />
            </div>
          </div>
        }
      </section>

      <!-- Expert: who should the AI be? -->
      <app-expander
        icon="person"
        [heading]="'agentSettings.sections.expert.title' | transloco"
        [question]="'agentSettings.sections.expert.question' | transloco"
        [summary]="expertSummary()"
        [chip]="expertChip().label"
        [chipTone]="expertChip().tone"
        [expanded]="expertOpen()"
        (expandedChange)="expertOpen.set($event)"
      >
        <div class="es-section">
          @if (mode() === 'live') {
            <div class="locked-field">
              <span class="locked-label">
                {{ 'agentSettings.sections.expert.title' | transloco }}
                <span class="locked-reason"><app-icon size="xs">lock</app-icon>{{ 'agentSettings.locked.atCreation' | transloco }}</span>
              </span>
              <span class="locked-value">{{ expertName() || ('agentSettings.sections.expert.unnamed' | transloco) }}</span>
            </div>
          } @else {
            <ng-content select="[expertPicker]" />
            @if (expertChanges() > 0) {
              <div class="based-on" role="status">
                <span class="based-on-text">{{ 'agentSettings.sections.expert.basedOn' | transloco:{ name: expertName() || ('agentSettings.sections.expert.unnamed' | transloco), n: expertChanges() } }}</span>
                <button type="button" class="based-on-reset" [disabled]="disabled()" (click)="resetExpertSection()">
                  {{ 'agentSettings.sections.expert.resetToTemplate' | transloco }}
                </button>
              </div>
            }
          }

          <app-model-group
            [config]="config()"
            [mode]="mode()"
            [disabled]="disabled()"
            [effectiveModels]="effectiveModels()"
            [showSubagent]="delegationEnabled()"
            [rememberLastModel]="false"
            (change)="onChange()"
          />

          @if (mode() === 'job') {
            <app-instructions-tab
              [rows]="6"
              [disabled]="disabled()"
              [loadingExpert]="loadingExpert()"
              (contentChange)="onInstructionsChange($event)"
            />
          }

          <app-tools-group
            [config]="config()"
            [mode]="mode()"
            [delegationCapScope]="delegationCapScope()"
            [disabled]="disabled()"
            [resolved]="resolvedToolset()"
            [readsResolvedToolset]="readsResolvedToolset()"
            [enumerateOnly]="enumerateOnly()"
            [gatedCapabilities]="gatedCapabilities()"
            (change)="onChange()"
          />

          @if (mode() === 'live') {
            <!-- Cache-reset cost disclosure (live_session_settings.md,
                 principle 4 / acceptance #9): static, next to the controls it
                 applies to; cosmetic controls deliberately carry no warning. -->
            <p class="cache-note">{{ 'agentSettings.live.cacheNote' | transloco }}</p>
          }

          <div class="es-more">
            <button type="button" class="es-more-toggle" [attr.aria-expanded]="expertMoreOpen()" (click)="expertMoreOpen.set(!expertMoreOpen())">
              <app-icon size="sm" class="es-more-chevron" [class.open]="expertMoreOpen()">expand_more</app-icon>
              {{ 'agentSettings.sections.expert.more' | transloco }}
              @if (advancedChanges() > 0) {
                <span class="es-more-count">{{ 'agentSettings.sections.expert.moreChanged' | transloco:{ n: advancedChanges() } }}</span>
              }
            </button>
            <div class="es-more-body" [hidden]="!expertMoreOpen()">
              @if (mode() === 'live') {
                <p class="locked-banner">
                  <app-icon size="xs">lock</app-icon>
                  {{ (lockedConfig() ? 'agentSettings.locked.moreLive' : 'agentSettings.locked.moreUnavailable') | transloco }}
                </p>
              }
              @if (mode() !== 'live' || lockedConfig()) {
                <app-execution-group
                  #execExpert
                  section="expert"
                  [config]="mode() === 'live' ? (lockedConfig() ?? config()) : config()"
                  [mode]="mode()"
                  [disabled]="disabled()"
                  (change)="onChange()"
                />
                <app-advanced-accordion
                  [flat]="true"
                  [config]="mode() === 'live' ? (lockedConfig() ?? config()) : config()"
                  [mode]="mode()"
                  [disabled]="disabled() || mode() === 'live'"
                  [settingsMatrix]="settingsMatrix()"
                  [modelOverride]="modelGroup?.model() ?? null"
                  [backendOverride]="workspacePicker() ? pickerBackend() : (liveTier() ?? null)"
                  [vmSizing]="false"
                  (change)="onChange()"
                />
              }
            </div>
          </div>
        </div>
      </app-expander>

      <!-- Workspace: what environment will it work in? -->
      <app-expander
        icon="terminal"
        [heading]="'agentSettings.sections.workspace.title' | transloco"
        [question]="'agentSettings.sections.workspace.question' | transloco"
        [summary]="workspaceSummary()"
        [chip]="workspaceChip().label"
        [chipTone]="workspaceChip().tone"
        [expanded]="workspaceOpen()"
        (expandedChange)="workspaceOpen.set($event)"
      >
        <div class="es-section">
          @if (mode() === 'live') {
            <app-execution-group
              #execWorkspace
              section="workspace"
              [config]="config()"
              [mode]="mode()"
              [disabled]="disabled()"
              [liveTier]="liveTier()"
              [tierReachability]="tierReachability()"
              [upgradeInProgress]="upgradeInProgress()"
              (change)="onChange()"
              (tierChangeRequested)="tierChangeRequested.emit($event)"
            />
            <div class="locked-field">
              <span class="locked-label">
                {{ 'agentSettings.locked.templateAndResources' | transloco }}
                <span class="locked-reason"><app-icon size="xs">lock</app-icon>{{ 'agentSettings.locked.atCreation' | transloco }}</span>
              </span>
              <span class="locked-value">{{ 'agentSettings.locked.templateAndResourcesHint' | transloco }}</span>
            </div>
          } @else {
            <ng-content select="[workspacePicker]" />
          }
        </div>
      </app-expander>

      <!-- Connectors: what should it have access to? -->
      <app-expander
        icon="cable"
        [heading]="'agentSettings.sections.connectors.title' | transloco"
        [question]="'agentSettings.sections.connectors.question' | transloco"
        [summary]="connectorsSummary()"
        [chip]="connectorsChip().label"
        [chipTone]="connectorsChip().tone"
        [expanded]="connectorsOpen()"
        (expandedChange)="connectorsOpen.set($event)"
      >
        <div class="es-section">
          <app-datasources-group
            [showHeader]="false"
            [searchable]="true"
            [datasources]="datasources()"
            [loading]="loadingDatasources()"
            [error]="datasourceLoadError()"
            [contextKey]="datasourceContextKey()"
            [disabled]="disabled()"
            [isLiteBackend]="mode() === 'live' ? liteBackend() : workspacePicker() ? isLite(pickerBackend()) : false"
            [initialSelectedIds]="mode() === 'live' || mode() === 'session' ? initialDatasourceIds() : null"
            [datasourceDefaultsEnabled]="datasourceDefaultsEnabled()"
            [lockedIds]="lockedDatasourceIds()"
            (change)="onChange()"
            (retry)="retryDatasources.emit()"
          />
          @if (mode() === 'live') {
            <p class="cache-note">{{ 'agentSettings.live.datasourcesLive' | transloco }}</p>
          }
          <ng-content select="[connectorsExtra]" />
        </div>
      </app-expander>

    </div>
  `,
  styles: [`
    :host { display: block; }
    .exec-settings {
      display: flex;
      flex-direction: column;
      gap: 12px;
    }
    .es-block {
      display: flex;
      flex-direction: column;
      gap: 12px;
      padding: 16px;
      border: 1px solid var(--border-hairline);
      border-radius: var(--radius-surface);
      background: var(--panel-bg);
    }
    .es-row {
      display: flex;
      flex-wrap: wrap;
      align-items: flex-start;
      gap: 0 16px;
    }
    .es-row > * {
      flex: 1 1 14rem;
      min-width: 0;
    }
    .es-row-main:empty { display: none; }
    .es-section {
      display: flex;
      flex-direction: column;
      gap: 16px;
    }
    .es-more {
      display: flex;
      flex-direction: column;
      gap: 8px;
    }
    .es-more-toggle {
      align-self: flex-start;
      display: inline-flex;
      align-items: center;
      gap: 4px;
      min-height: 32px;
      padding: 0 4px 0 0;
      border: 0;
      background: transparent;
      color: var(--accent-color);
      font: inherit;
      font-size: 13px;
      font-weight: 600;
      cursor: pointer;
    }
    .es-more-toggle:focus-visible {
      outline: none;
      box-shadow: 0 0 0 3px var(--ring);
      border-radius: var(--radius-tag);
    }
    .es-more-chevron { transition: transform 0.15s ease; }
    .es-more-chevron.open { transform: rotate(180deg); }
    .es-more-count {
      font-weight: 400;
      color: var(--text-muted);
    }
    .es-more-body {
      display: flex;
      flex-direction: column;
      gap: 12px;
    }
    .es-more-body[hidden] { display: none; }
    .based-on {
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      gap: 8px 12px;
      padding: 8px 12px;
      border-radius: var(--radius-control);
      background: color-mix(in srgb, var(--accent-color) 10%, transparent);
      color: var(--text-primary);
      font-size: 13px;
    }
    .based-on-text { flex: 1 1 12rem; }
    .based-on-reset {
      min-height: 32px;
      padding: 0 10px;
      border: 1px solid var(--accent-color);
      border-radius: var(--radius-control);
      background: transparent;
      color: var(--accent-color);
      font: inherit;
      font-size: 12px;
      font-weight: 600;
      cursor: pointer;
    }
    .based-on-reset:disabled { opacity: 0.6; cursor: not-allowed; }
    .locked-field {
      display: flex;
      flex-direction: column;
      gap: 4px;
    }
    .locked-label {
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      gap: 2px 8px;
      font-size: 12px;
      font-weight: 500;
      color: var(--text-primary);
    }
    .locked-reason {
      display: inline-flex;
      align-items: center;
      gap: 3px;
      font-weight: 400;
      color: var(--text-muted);
    }
    .locked-value {
      padding: 7px 10px;
      border: 1px solid var(--border-hairline);
      border-radius: var(--radius-control);
      background: var(--surface-0);
      color: var(--text-secondary);
      font-size: 13px;
      overflow-wrap: anywhere;
    }
    .locked-banner {
      display: flex;
      align-items: center;
      gap: 6px;
      margin: 0;
      font-size: 12px;
      color: var(--text-muted);
    }
    .cache-note {
      margin: -8px 0 0;
      font-size: 11px;
      line-height: 1.4;
      color: var(--text-muted);
    }
    @media (prefers-reduced-motion: reduce) {
      .es-more-chevron { transition: none; }
    }
  `],
})
export class AgentSettingsComponent {
  private readonly transloco = inject(TranslocoService);
  private readonly activeLang = toSignal(this.transloco.langChanges$, {
    initialValue: this.transloco.getActiveLang(),
  });

  /** Merged expert/framework config for resolved defaults. */
  config = input<Record<string, unknown>>({});
  mode = input<SettingsMode>('job');
  disabled = input(false);
  /** Caller's resolved capability grants for control-greying; null ⇒ no gating
   * (admin / unrestricted). The New-Session and settings flows hide
   * permission/autonomy options above the user's ceiling and grey the tool
   * categories the user may not grant, the same way the expert editor greys
   * an author's controls. */
  gatedCapabilities = input<Record<string, unknown> | null>(null);

  /** Which delegation cap the Delegation row edits: a session thread's own
   *  cap in the create form and the live pane, the worker cap for a job. */
  readonly delegationCapScope = computed(() => delegationCapScopeForMode(this.mode()));
  /** Whether the selected project has shared memory. */
  showProjectMemory = input(false);
  /** The server's resolved toolset for this surface. See ToolsGroupComponent. */
  resolvedToolset = input<SessionToolGroupsResponse | null>(null);
  /** True when this host performs a resolved read at all — see
   *  ToolsGroupComponent.readsResolvedToolset. */
  readsResolvedToolset = input(false);
  /** Write vocabulary for enumerate-only categories, for hosts without a
   *  resolved read. See ToolsGroupComponent.enumerateOnly. */
  enumerateOnly = input<Record<string, string[]> | null>(null);
  /** Raw settings_matrix for client-side model-family resolution. */
  settingsMatrix = input<Record<string, Record<string, unknown>>>({});
  /** Server-resolved effective model + provenance per slot (forwarded to the
   *  model picker so the unset "Default" option names what will actually run). */
  effectiveModels = input<EffectiveModels | null>(null);
  /** Available datasources. */
  datasources = input<Datasource[]>([]);
  loadingDatasources = input(false);
  datasourceLoadError = input(false);
  datasourceContextKey = input('standalone');
  /** Fail-closed rollout gate for server-computed create defaults. */
  datasourceDefaultsEnabled = input(false);
  /** The picker's default when untouched — live mode: the session's
   *  currently attached selection (live_session_settings.md Slice B).
   *  Session-create mode: a source thread's surviving connectors, already
   *  intersected against `datasources()` by the caller
   *  (session_config_drift_resume.md §8.3 — "Start a new session"). Null
   *  keeps the create-flow server `default_selected` set; job mode never
   *  reads this input. */
  initialDatasourceIds = input<string[] | null>(null);
  /** Live mode: entries frozen at their current state (kb-type — knowledge
   *  bindings only rewire on attach). */
  lockedDatasourceIds = input<string[]>([]);
  /** Live mode: whether the session runs a lite backend (virtual/none). */
  liteBackend = input(false);
  /** Live mode: the running session's workspace tier, and which tiers it can
   *  move to. Forwarded to the workspace section, which renders the tier row
   *  as a launcher for the upgrade verb rather than as a setting. */
  liveTier = input<string | null>(null);
  tierReachability = input<Record<string, TierReachability>>({});
  upgradeInProgress = input<{tier: string; elapsed?: number} | null>(null);
  /** True on the create forms, which project `app-workspace-picker` into the
   *  Workspace section (Slice A3). The legacy tier row and VM sizing then step
   *  aside. */
  workspacePicker = input(false);
  /** The backend the picker's choice runs on, for the lite-tier greying. */
  pickerBackend = input<string | null>(null);
  /** Expert detail loading state. */
  loadingExpert = input(false);
  /** Display name of the Expert template the form started from (create), or
   *  of the running session's Expert (live). */
  expertName = input('');
  /** Where the selected Expert came from when the user has not picked one:
   *  `project` | `user` | `application` | `explicit`. Labels the Expert chip. */
  expertSource = input<string | null>(null);
  /** Live mode: the session's frozen resolved config, so locked settings show
   *  the values the session really runs with. Null shows a note instead. */
  lockedConfig = input<Record<string, unknown> | null>(null);
  /** Live mode: the session's project, shown locked. */
  projectName = input('');

  protected isLite(backend: string | null): boolean {
    return backend === 'virtual' || backend === 'none';
  }

  /** Emitted whenever any setting changes. */
  change = output<void>();
  retryDatasources = output<void>();
  /** Live mode: a workspace tier the user picked. The host confirms it and
   *  dispatches the upgrade verb — this surface only reports the intent. */
  tierChangeRequested = output<string>();
  /** Emitted when instructions content changes. */
  instructionsChange = output<string | null>();

  @ViewChild('execTask', {read: ExecutionGroupComponent}) executionGroup?: ExecutionGroupComponent;
  @ViewChild('execTaskMore', {read: ExecutionGroupComponent}) executionMoreGroup?: ExecutionGroupComponent;
  @ViewChild('execExpert', {read: ExecutionGroupComponent}) executionExpertGroup?: ExecutionGroupComponent;
  @ViewChild('execWorkspace', {read: ExecutionGroupComponent}) executionWorkspaceGroup?: ExecutionGroupComponent;
  @ViewChild(ModelGroupComponent) modelGroup?: ModelGroupComponent;
  @ViewChild(ToolsGroupComponent) toolsGroup?: ToolsGroupComponent;
  @ViewChild(DatasourcesGroupComponent) datasourcesGroup?: DatasourcesGroupComponent;
  @ViewChild(InstructionsTabComponent) instructionsTab?: InstructionsTabComponent;
  @ViewChild(AdvancedAccordionComponent) advancedAccordion?: AdvancedAccordionComponent;

  // Signal mirrors of the groups — a plain @ViewChild isn't reactive, and the
  // section summaries must follow every edit.
  private readonly modelQuery = viewChild(ModelGroupComponent);
  private readonly toolsGroupQuery = viewChild(ToolsGroupComponent);
  private readonly datasourcesQuery = viewChild(DatasourcesGroupComponent);
  private readonly advancedQuery = viewChild(AdvancedAccordionComponent);
  private readonly instructionsQuery = viewChild(InstructionsTabComponent);
  private readonly execExpertQuery = viewChild('execExpert', {read: ExecutionGroupComponent});
  private readonly pickerQuery = contentChild(WorkspacePickerComponent, {descendants: true});

  /** The roster-wide subagent model (`subagents.llm.model`) only applies when
   *  the Delegation tool is enabled — hide its picker otherwise. Shown until
   *  the view resolves (delegation defaults on for the experts that use it). */
  readonly delegationEnabled = computed(() =>
    this.toolsGroupQuery()?.isCategoryEnabled('delegation') ?? true,
  );

  // Section open state. The live pane opens on the Expert section: the header
  // chips that open the pane are the model and the permission mode.
  readonly expertOpen = linkedSignal(() => this.mode() === 'live');
  readonly workspaceOpen = signal(false);
  readonly connectorsOpen = signal(false);
  readonly expertMoreOpen = signal(false);
  readonly taskMoreOpen = signal(false);

  // ===== Expert section =====

  /** Inference and runtime fields the user changed away from the template
   *  (Expert → More). A value pinned by a prefill that equals the template's
   *  own value is not a change. */
  readonly advancedChanges = computed(() => {
    const advanced = this.advancedQuery();
    const imageQuality = this.execExpertQuery();
    let n = 0;
    if (advanced) n += changedLeaves(advanced.getOverrides(), this.config());
    if (imageQuality) n += changedLeaves(imageQuality.getOverrides(), this.config());
    return n;
  });

  /** Everything in the Expert section that differs from the template. */
  readonly expertChanges = computed(() => {
    if (this.mode() === 'live') return 0;
    const model = this.modelQuery();
    const tools = this.toolsGroupQuery();
    const instructions = this.instructionsQuery();
    let n = this.advancedChanges();
    if (model) n += changedLeaves(model.getOverrides(), this.config());
    // What the tools group would actually send, not its row markers: a row
    // can read "modified" against a newer server answer while the fragment
    // (diffed against the anchor) is empty.
    if (tools) n += changedLeaves(tools.getOverrides(), this.config());
    if (instructions?.isModified()) n += 1;
    return n;
  });

  readonly expertSummary = computed(() => {
    this.activeLang();
    const parts: string[] = [];
    parts.push(this.expertName() || this.transloco.translate('agentSettings.sections.expert.unnamed'));
    const model = this.modelQuery()?.modelInEffect();
    if (model) parts.push(model);
    const reasoning = this.reasoningInEffect();
    if (reasoning) parts.push(this.transloco.translate('agentSettings.sections.expert.reasoning', {level: reasoning}));
    const group = this.toolsGroupQuery();
    const tools = group?.onSummary();
    // No rows means no answer (live pane, failed read): say nothing about tools
    // rather than "0 tool groups".
    if (tools && tools.categories > 0) {
      parts.push(tools.tools > 0
        ? this.transloco.translate('agentSettings.sections.expert.tools', {n: tools.tools})
        : this.transloco.translate('agentSettings.sections.expert.toolGroups', {n: tools.categories}));
    }
    if (group?.rows().some((row) => row.key === 'delegation' && group.rowState(row) === 'on')) {
      parts.push(this.transloco.translate('agentSettings.sections.expert.subagents'));
    }
    return parts.join(' · ');
  });

  private readonly reasoningInEffect = computed<string | null>(() => {
    if (this.mode() === 'job') {
      const advanced = this.advancedQuery();
      return advanced ? (advanced.reasoning() ?? advanced.resolvedReasoning() ?? null) : null;
    }
    const model = this.modelQuery();
    return model ? (model.sessionReasoning() ?? model.resolvedSessionReasoning()) : null;
  });

  readonly expertChip = computed<{label: string; tone: BadgeTone}>(() => {
    this.activeLang();
    if (this.mode() === 'live') {
      return {label: this.transloco.translate('agentSettings.sections.chip.partlyLocked'), tone: 'neutral'};
    }
    const n = this.expertChanges();
    if (n > 0) {
      return {label: this.transloco.translate('agentSettings.sections.chip.changes', {n}), tone: 'accent'};
    }
    const source = this.expertSource();
    if (source && source !== 'explicit') {
      return {label: capitalize(this.transloco.translate(`settings.expertDefaults.source.${source}`)), tone: 'info'};
    }
    return {label: this.transloco.translate('agentSettings.sections.chip.template'), tone: 'neutral'};
  });

  // ===== Workspace section =====

  readonly workspaceSummary = computed(() => {
    this.activeLang();
    if (this.mode() === 'live') {
      const tier = this.liveTier();
      return tier ? this.tierLabel(tier) : '';
    }
    return this.pickerQuery()?.summary() ?? '';
  });

  readonly workspaceChip = computed<{label: string; tone: BadgeTone}>(() => {
    this.activeLang();
    if (this.mode() === 'live') {
      return {label: this.transloco.translate('agentSettings.sections.chip.upgradeOnly'), tone: 'neutral'};
    }
    const kind = this.pickerQuery()?.choice().kind ?? 'default';
    const key = kind === 'default' ? 'default' : kind === 'inline' ? 'custom' : kind === 'none' ? 'none' : 'template';
    return {label: this.transloco.translate(`agentSettings.sections.chip.${key}`), tone: kind === 'inline' ? 'accent' : 'neutral'};
  });

  private tierLabel(tier: string): string {
    const known = WORKSPACE_BACKENDS.find((b) => b.value === tier);
    return known ? this.transloco.translate(`advanced.options.${known.i18nKey}`) : tier;
  }

  // ===== Connectors section =====

  readonly connectorsSummary = computed(() => {
    this.activeLang();
    const group = this.datasourcesQuery();
    if (!group) return '';
    if (this.loadingDatasources()) return this.transloco.translate('agentSettings.datasources.loading');
    const names = group.selectedList().map((d) => d.name);
    if (names.length === 0) return this.transloco.translate('agentSettings.sections.connectors.none');
    const shown = names.slice(0, 2).join(', ');
    return names.length > 2
      ? this.transloco.translate('agentSettings.sections.connectors.summaryMore', {n: names.length, names: shown, more: names.length - 2})
      : this.transloco.translate('agentSettings.sections.connectors.summary', {n: names.length, names: shown});
  });

  readonly connectorsChip = computed<{label: string; tone: BadgeTone}>(() => {
    this.activeLang();
    if (this.mode() === 'live') {
      return {label: this.transloco.translate('agentSettings.sections.chip.addOrRemove'), tone: 'neutral'};
    }
    const changed = (this.datasourcesQuery()?.modifiedCount() ?? 0) > 0;
    return changed
      ? {label: this.transloco.translate('agentSettings.sections.chip.changed'), tone: 'accent'}
      : {label: this.transloco.translate('agentSettings.sections.chip.defaults'), tone: 'neutral'};
  });

  // ===== Host API (unchanged from the tabbed layout) =====

  onChange(): void {
    this.change.emit();
  }

  onInstructionsChange(value: string | null): void {
    this.instructionsChange.emit(value);
  }

  /** Open the Expert section so the model picker is on screen (the live
   *  pane's model chip scrolls to it). */
  revealModel(): void {
    this.expertOpen.set(true);
  }

  /** Put the Expert section back to the template: every group re-reads the
   *  config the template prefilled. */
  resetExpertSection(): void {
    const workspace = this.executionGroup?.workspaceBackend() ?? null;
    this.modelGroup?.resetAll();
    this.toolsGroup?.resetAll();
    this.advancedAccordion?.resetAll();
    this.advancedAccordion?.prefillFromConfig(this.config());
    this.executionExpertGroup?.resetAll();
    this.instructionsTab?.resetToExpert();
    this.executionGroup?.workspaceBackend.set(workspace);
    this.change.emit();
  }

  /**
   * Collect all overrides from sub-components into a single config_override object.
   * Called by the parent component at form submission time.
   */
  getOverrides(): Record<string, unknown> {
    const parts = [
      this.executionGroup?.getOverrides() ?? {},
      this.executionMoreGroup?.getOverrides() ?? {},
      this.executionExpertGroup?.getOverrides() ?? {},
      this.executionWorkspaceGroup?.getOverrides() ?? {},
      this.modelGroup?.getOverrides() ?? {},
      this.toolsGroup?.getOverrides() ?? {},
      this.mode() === 'live' ? {} : (this.advancedAccordion?.getOverrides() ?? {}),
    ];

    const result: Record<string, unknown> = {};
    for (const part of parts) {
      deepMerge(result, part);
    }
    return Object.keys(result).length > 0 ? result : {};
  }

  /** Task- and session-level settings — autonomy, critic, scholar, project
   *  memory, permission and narration mode, idle timeout. Not Expert content:
   *  the create forms send these beside the Expert, never inside it. */
  getTaskOverrides(): Record<string, unknown> {
    const result: Record<string, unknown> = {};
    deepMerge(result, this.executionGroup?.getOverrides() ?? {});
    deepMerge(result, this.executionMoreGroup?.getOverrides() ?? {});
    const idle = readConfigPath(this.advancedAccordion?.getOverrides() ?? {}, 'interactive.idle_timeout_minutes');
    if (idle !== undefined && idle !== null) {
      deepMerge(result, {interactive: {idle_timeout_minutes: idle}});
    }
    return result;
  }

  /** The form's Expert content: model, tools, delegation and Expert → More.
   *  What a changed template's inline copy is built from. */
  getExpertOverrides(): Record<string, unknown> {
    const advanced = {...(this.advancedAccordion?.getOverrides() ?? {})};
    const interactive = advanced['interactive'] as Record<string, unknown> | undefined;
    if (interactive) {
      const rest = {...interactive};
      delete rest['idle_timeout_minutes'];
      if (Object.keys(rest).length) advanced['interactive'] = rest;
      else delete advanced['interactive'];
    }
    const result: Record<string, unknown> = {};
    for (const part of [
      this.executionExpertGroup?.getOverrides() ?? {},
      this.modelGroup?.getOverrides() ?? {},
      this.toolsGroup?.getOverrides() ?? {},
      advanced,
    ]) {
      deepMerge(result, part);
    }
    return result;
  }

  /** True when the Expert section differs from the template it started from
   *  — the create forms then send an inline copy instead of the selector. */
  hasExpertChanges(): boolean {
    return this.expertChanges() > 0;
  }

  vmSizingValid(): boolean {
    return this.advancedAccordion?.vmSizingValid() ?? true;
  }

  /** False while the session delegation cap field holds a value the
   *  orchestrator would refuse (a creation form blocks submit on it). */
  delegationCapValid(): boolean {
    return this.toolsGroup?.delegationCapValid() ?? true;
  }

  /** Return selected datasource IDs (not part of config_override). */
  getSelectedDatasourceIds(): string[] {
    return this.datasourcesGroup?.getSelectedIds() ?? [];
  }

  /** Session mode: pin the model picker to an explicit value, the same as the
   *  user picking it themselves — used to carry a source thread's model
   *  forward on "Start a new session" (session_config_drift_resume.md §8.3).
   *  Must be called AFTER `prefillFromConfig` for whichever expert ends up
   *  selected: that call resets the model group to the expert's own
   *  config-derived default, and would silently win over an override applied
   *  before it. */
  setSessionModelOverride(model: string): void {
    this.modelGroup?.onModelChange(model);
  }

  /** Category → the enumeration a requested locked-on addition writes.
   *  See ToolsGroupComponent.getToolAdditions. */
  getToolAdditions(): Record<string, string[]> {
    return this.toolsGroup?.getToolAdditions() ?? {};
  }

  /** Drop any in-flight picker selection back to the default (live mode:
   *  the attached set). The pane calls this on thread load so a pin made
   *  while the fetch was in flight can't leak into the diff baseline. */
  resetDatasourceSelection(): void {
    this.datasourcesGroup?.resetAll();
  }

  /** Return current instructions content (not part of config_override). */
  getInstructions(): string | null {
    return this.instructionsTab?.content() ?? null;
  }

  /**
   * Anchor the tools baseline to the server's resolved answer.
   *
   * Separate from `prefillFromConfig` because the two carry different facts:
   * a merged config says what was asked for, the resolved answer says what the
   * agent holds. Call this AFTER `prefillFromConfig` — it is the stronger
   * statement and must win.
   */
  prefillFromResolvedToolset(categories: Record<string, SessionToolCategory>): void {
    this.toolsGroup?.prefillFromResolved(categories);
  }

  /** True when the user has moved a tool switch since the last anchor. */
  hasToolEdits(): boolean {
    return this.toolsGroup?.hasToolEdits() ?? false;
  }

  /** Called by the parent when the expert changes. Propagates to all groups. */
  prefillFromConfig(config: Record<string, unknown>): void {
    this.modelGroup?.prefillFromConfig(config);
    this.toolsGroup?.prefillFromConfig(config);
    this.advancedAccordion?.prefillFromConfig(config);
    this.executionExpertGroup?.resetAll();

    // The task block reads from the config input directly (via computed
    // signals), but user overrides reset since the expert changed.
    const workspace = this.executionGroup?.workspaceBackend() ?? null;
    this.executionGroup?.resetAll();
    this.executionMoreGroup?.resetAll();
    // Workspace selection belongs to this execution and survives Expert edits.
    this.executionGroup?.workspaceBackend.set(workspace);

    // Autonomy, scholar and critic are NOT pinned from the template any more:
    // the task block already shows the template's values (it reads `config`),
    // and a pin would mark an untouched field as changed. The server resolves
    // the same values from the selected Expert.
  }

  /** Reset all sub-components to defaults. */
  resetAll(): void {
    this.executionGroup?.resetAll();
    this.executionMoreGroup?.resetAll();
    this.executionExpertGroup?.resetAll();
    this.executionWorkspaceGroup?.resetAll();
    this.modelGroup?.resetAll();
    this.toolsGroup?.resetAll();
    this.datasourcesGroup?.resetAll();
    this.instructionsTab?.resetAll();
    this.advancedAccordion?.resetAll();
  }
}

function deepMerge(target: Record<string, unknown>, source: Record<string, unknown>): void {
  for (const key of Object.keys(source)) {
    const sv = source[key];
    const tv = target[key];
    if (
      typeof sv === 'object' && sv !== null && !Array.isArray(sv) &&
      typeof tv === 'object' && tv !== null && !Array.isArray(tv)
    ) {
      deepMerge(tv as Record<string, unknown>, sv as Record<string, unknown>);
    } else {
      target[key] = sv;
    }
  }
}

function capitalize(text: string): string {
  return text ? text.charAt(0).toUpperCase() + text.slice(1) : text;
}

/** Leaves of `fragment` whose value differs from the same path in `config`. */
export function changedLeaves(fragment: Record<string, unknown>, config: Record<string, unknown>, prefix = ''): number {
  let n = 0;
  for (const [key, value] of Object.entries(fragment)) {
    const path = prefix ? `${prefix}.${key}` : key;
    if (value !== null && typeof value === 'object' && !Array.isArray(value)) {
      n += changedLeaves(value as Record<string, unknown>, config, path);
    } else if (JSON.stringify(value) !== JSON.stringify(readConfigPath(config, path) ?? null)) {
      n += 1;
    }
  }
  return n;
}
