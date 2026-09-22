import {Component, computed, effect, ElementRef, inject, input, output, signal, viewChild, ViewChild} from '@angular/core';
import {TranslocoPipe} from '@jsverse/transloco';
import {Datasource, EffectiveModels} from '../../core/models/api.model';
import {ViewportService} from '../../core/services/viewport.service';
import type {
    SessionToolCategory,
    SessionToolGroupsResponse,
} from '../../core/services/api.service';
import {SettingsMode, TierReachability} from './agent-settings.types';
import {ExecutionGroupComponent} from './execution-group.component';
import {ModelGroupComponent} from './model-group.component';
import {ToolsGroupComponent} from './tools-group.component';
import {DatasourcesGroupComponent} from './datasources-group.component';
import {InstructionsTabComponent} from './instructions-tab.component';
import {AdvancedAccordionComponent} from './advanced-accordion.component';
import {AppTabNavComponent, AppTabNavItemComponent} from '../../ui/tab-nav';
import {AppBadgeComponent} from '../../ui/badge';

type AgentSettingsTab = 'settings' | 'instructions' | 'advanced' | 'resolved';

/**
 * Host component for agent settings. Composes all sub-components with a tabbed layout.
 *
 * - **Job creation**: vertical tabs (Settings, Instructions, Advanced)
 * - **Session creation**: horizontal tabs (Settings, Advanced) — no Instructions tab
 *
 * Each sub-component manages its own override state. The parent calls `getOverrides()`
 * at submit time to collect all overrides, and `resetAll()` to clear everything.
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
    AppTabNavComponent,
    AppTabNavItemComponent,
    AppBadgeComponent,
    TranslocoPipe,
  ],
  template: `
    <div class="settings-root" [class.vertical]="useVerticalTabs()" [class.horizontal]="!useVerticalTabs()">
      <!-- Tab navigation -->
      <app-tab-nav
        [orientation]="useVerticalTabs() ? 'vertical' : 'horizontal'"
        [value]="activeTab()"
        (valueChange)="onTabChange($event)"
      >
        <app-tab-nav-item value="settings">
          Settings
          @if (settingsModifiedCount() > 0) {
            <app-badge tone="accent" size="xs" shape="pill">{{ settingsModifiedCount() }}</app-badge>
          }
        </app-tab-nav-item>
        @if (mode() === 'job') {
          <app-tab-nav-item value="instructions">
            Instructions
            @if (instructionsTab?.isModified()) {
              <span class="tab-modified-dot" aria-hidden="true"></span>
            }
          </app-tab-nav-item>
        }
        @if (mode() !== 'live') {
          <app-tab-nav-item value="advanced">Advanced</app-tab-nav-item>
        }
        <app-tab-nav-item value="resolved">Resolved</app-tab-nav-item>
      </app-tab-nav>

      <!-- Tab content -->
      <div class="tab-content" #tabContent>
        <div class="tab-panel settings-panel" [class.tab-hidden]="activeTab() !== 'settings'">
          <app-execution-group
            [config]="config()"
            [mode]="mode()"
            [disabled]="disabled()"
            [showProjectMemory]="showProjectMemory()"
            [gatedCapabilities]="gatedCapabilities()"
            [liveTier]="liveTier()"
            [tierReachability]="tierReachability()"
            [upgradeInProgress]="upgradeInProgress()"
            (change)="onChange()"
            (tierChangeRequested)="tierChangeRequested.emit($event)"
          />
          <app-model-group
            [config]="config()"
            [mode]="mode()"
            [disabled]="disabled()"
            [effectiveModels]="effectiveModels()"
            [showSubagent]="delegationEnabled()"
            (change)="onChange()"
          />
          <app-tools-group
            [config]="config()"
            [mode]="mode()"
            [disabled]="disabled()"
            [resolved]="resolvedToolset()"
            [readsResolvedToolset]="readsResolvedToolset()"
            [enumerateOnly]="enumerateOnly()"
            (change)="onChange()"
          />

          @if (mode() === 'live') {
            <!-- Cache-reset cost disclosure (live_session_settings.md,
                 principle 4 / acceptance #9): static, next to the controls it
                 applies to; cosmetic controls deliberately carry no warning. -->
            <p class="cache-note">{{ 'agentSettings.live.cacheNote' | transloco }}</p>
          }

          <app-datasources-group
            [datasources]="datasources()"
            [loading]="loadingDatasources()"
            [error]="datasourceLoadError()"
            [contextKey]="datasourceContextKey()"
            [disabled]="disabled()"
            [isLiteBackend]="mode() === 'live' ? liteBackend() : (executionGroup?.isLiteBackend() ?? false)"
            [initialSelectedIds]="mode() === 'live' || mode() === 'session' ? initialDatasourceIds() : null"
            [datasourceDefaultsEnabled]="datasourceDefaultsEnabled()"
            [lockedIds]="lockedDatasourceIds()"
            (change)="onChange()"
            (retry)="retryDatasources.emit()"
          />
          @if (mode() === 'live') {
            <p class="cache-note">{{ 'agentSettings.live.datasourcesLive' | transloco }}</p>
          }

          <!-- Modified count summary -->
          @if (settingsModifiedCount() > 0) {
            <div class="modified-summary">
              {{ settingsModifiedCount() }} setting{{ settingsModifiedCount() > 1 ? 's' : '' }} modified
            </div>
          }
        </div>

        @if (mode() === 'job') {
          <div class="tab-panel instructions-panel" [class.tab-hidden]="activeTab() !== 'instructions'">
            <app-instructions-tab
              [disabled]="disabled()"
              [loadingExpert]="loadingExpert()"
              (contentChange)="onInstructionsChange($event)"
            />
          </div>
        }

        @if (mode() !== 'live') {
          <div class="tab-panel advanced-panel" [class.tab-hidden]="activeTab() !== 'advanced'">
            <app-advanced-accordion
              [config]="config()"
              [mode]="mode()"
              [disabled]="disabled()"
              [settingsMatrix]="settingsMatrix()"
              [modelOverride]="modelGroup?.model() ?? null"
              [backendOverride]="executionGroup?.workspaceBackend() ?? null"
              (change)="onChange()"
            />
          </div>
        }

        <div class="tab-panel resolved-panel" [class.tab-hidden]="activeTab() !== 'resolved'">
          <pre class="config-json">{{ resolvedConfigJson() }}</pre>
        </div>
      </div>
    </div>
  `,
  styles: [`
    .settings-root {
      display: flex;
      border: 1px solid var(--border-color, var(--surface-0));
      border-radius: var(--radius-surface);
      overflow: hidden;
      background: var(--panel-bg, var(--panel-bg));
      min-height: 300px;
    }

    .settings-root.vertical { flex-direction: row; }
    .settings-root.horizontal { flex-direction: column; }

    .tab-modified-dot {
      display: inline-block;
      width: 6px;
      height: 6px;
      border-radius: 50%;
      background: var(--accent-color, var(--accent-color));
    }

    .tab-content {
      flex: 1;
      overflow: auto;
      min-width: 0;
    }
    .tab-panel {
      padding: 16px;
    }
    .tab-hidden {
      display: none !important;
    }
    .instructions-panel {
      display: flex;
      flex-direction: column;
      height: 100%;
    }

    .resolved-panel {
      padding: 0;
    }
    .resolved-panel .config-json {
      margin: 0;
      padding: 12px 14px;
      font-family: 'JetBrains Mono', monospace;
      font-size: 11px;
      line-height: 1.5;
      color: var(--text-primary);
      background: var(--surface-0);
      white-space: pre-wrap;
      word-break: break-all;
    }

    .modified-summary {
      text-align: right;
      font-size: 11px;
      color: var(--accent-color, var(--accent-color));
      padding-top: 8px;
      border-top: 1px solid var(--border-color, var(--surface-0));
    }

    .cache-note {
      margin: -8px 0 20px;
      font-size: 11px;
      line-height: 1.4;
      color: var(--text-muted);
    }
  `],
})
export class AgentSettingsComponent {
  /** Merged expert/framework config for resolved defaults. */
  config = input<Record<string, unknown>>({});
  mode = input<SettingsMode>('job');
  disabled = input(false);
  /** Caller's resolved capability grants for control-greying; null ⇒ no gating
   * (admin / unrestricted). Forwarded to the execution group so the New-Session
   * + settings flows hide permission/autonomy options above the user's ceiling,
   * the same way the expert editor greys an author's controls. */
  gatedCapabilities = input<Record<string, unknown> | null>(null);

  private readonly viewport = inject(ViewportService);
  /**
   * Vertical tab rail only in job mode on wide screens. On mobile the rail would
   * eat ~130px and squeeze the content into ~219px (horizontal scroll, clipped
   * Delegation controls), so collapse to horizontal tabs with full-width content.
   */
  readonly useVerticalTabs = computed(() => this.mode() === 'job' && !this.viewport.isMobile());
  /** Whether the selected project has shared memory. */
  showProjectMemory = input(false);
  /** Inherited tool lists from the selected expert's mode base. */
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
  /** Live mode: whether the session runs a lite backend (virtual/none) — the
   *  Advanced accordion that normally derives this is hidden in live mode. */
  liteBackend = input(false);
  /** Live mode: the running session's workspace tier, and which tiers it can
   *  move to. Forwarded to the execution group, which renders the tier row as
   *  a launcher for the upgrade verb rather than as a setting. */
  liveTier = input<string | null>(null);
  tierReachability = input<Record<string, TierReachability>>({});
  upgradeInProgress = input<{tier: string; elapsed?: number} | null>(null);
  /** Expert detail loading state. */
  loadingExpert = input(false);

  /** Emitted whenever any setting changes. */
  change = output<void>();
  retryDatasources = output<void>();
  /** Live mode: a workspace tier the user picked. The host confirms it and
   *  dispatches the upgrade verb — this surface only reports the intent. */
  tierChangeRequested = output<string>();
  /** Emitted when instructions content changes. */
  instructionsChange = output<string | null>();

  @ViewChild(ExecutionGroupComponent) executionGroup?: ExecutionGroupComponent;
  @ViewChild(ModelGroupComponent) modelGroup?: ModelGroupComponent;
  @ViewChild(ToolsGroupComponent) toolsGroup?: ToolsGroupComponent;
  @ViewChild(DatasourcesGroupComponent) datasourcesGroup?: DatasourcesGroupComponent;
  @ViewChild(InstructionsTabComponent) instructionsTab?: InstructionsTabComponent;
  @ViewChild(AdvancedAccordionComponent) advancedAccordion?: AdvancedAccordionComponent;
  @ViewChild('tabContent') private tabContentEl?: ElementRef<HTMLElement>;

  /** Signal-based mirror of {@link toolsGroup} — a plain @ViewChild isn't
   *  reactive, so gating the Subagent model picker on the live Delegation
   *  toggle needs this to re-run the computed when the toggle (or the query
   *  itself) resolves. */
  private readonly toolsGroupQuery = viewChild(ToolsGroupComponent);
  /** The roster-wide subagent model (`subagents.llm.model`) only applies when
   *  the Delegation tool is enabled — hide its picker otherwise. Shown until
   *  the view resolves (delegation defaults on for the experts that use it). */
  readonly delegationEnabled = computed(() =>
    this.toolsGroupQuery()?.isCategoryEnabled('delegation') ?? true,
  );

  readonly activeTab = signal<AgentSettingsTab>('settings');

  protected onTabChange(value: AgentSettingsTab | null): void {
    if (value === 'settings' || value === 'instructions' || value === 'advanced' || value === 'resolved') {
      this.activeTab.set(value);
    }
  }

  readonly resolvedConfigJson = computed(() => {
    try {
      return JSON.stringify(this.config(), null, 2);
    } catch {
      return '{}';
    }
  });

  constructor() {
    effect(() => {
      this.activeTab(); // track tab changes
      this.tabContentEl?.nativeElement.scrollTo(0, 0);
    });
  }

  readonly settingsModifiedCount = computed(() => {
    const exec = this.executionGroup?.modifiedCount() ?? 0;
    const model = this.modelGroup?.modifiedCount() ?? 0;
    const tools = this.toolsGroup?.modifiedCount() ?? 0;
    // The datasources "modified count" is really the SELECTED count. In live
    // mode that's the session's attachment state, not a pending modification,
    // and would permanently pin the badge — exclude it.
    const ds = this.mode() === 'live' ? 0 : (this.datasourcesGroup?.modifiedCount() ?? 0);
    return exec + model + tools + ds;
  });

  onChange(): void {
    this.change.emit();
  }

  onInstructionsChange(value: string | null): void {
    this.instructionsChange.emit(value);
  }

  /**
   * Collect all overrides from sub-components into a single config_override object.
   * Called by the parent component at form submission time.
   */
  getOverrides(): Record<string, unknown> {
    const parts = [
      this.executionGroup?.getOverrides() ?? {},
      this.modelGroup?.getOverrides() ?? {},
      this.toolsGroup?.getOverrides() ?? {},
      this.advancedAccordion?.getOverrides() ?? {},
    ];

    // Deep merge all parts
    const result: Record<string, unknown> = {};
    for (const part of parts) {
      this.deepMerge(result, part);
    }
    return Object.keys(result).length > 0 ? result : {};
  }

  vmSizingValid(): boolean {
    return this.advancedAccordion?.vmSizingValid() ?? true;
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
   *
   *  A creation form needs nothing from this (it submits `getOverrides()`,
   *  which already carries them). The live pane does: its dispatch is a diff
   *  over switch positions, and a locked-on category's position never moves.
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
   * Called by parent when expert changes. Propagates to all sub-components.
   */
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

  prefillFromConfig(config: Record<string, unknown>): void {
    this.modelGroup?.prefillFromConfig(config);
    this.toolsGroup?.prefillFromConfig(config);
    this.advancedAccordion?.prefillFromConfig(config);

    // Execution group reads from the config input directly (via computed signals),
    // but we should reset user overrides since the expert changed
    const workspace = this.executionGroup?.workspaceBackend() ?? null;
    this.executionGroup?.resetAll();
    // Workspace selection belongs to this execution and survives Expert edits.
    this.executionGroup?.workspaceBackend.set(workspace);

    // Prefill execution overrides from expert
    const autonomy = config['autonomy'] as string | undefined;
    if (autonomy) this.executionGroup?.autonomy.set(autonomy);

    const scholar = (config['scholar'] as Record<string, unknown>)?.['enabled'] as boolean | undefined;
    if (scholar !== undefined) this.executionGroup?.scholar.set(scholar);

    const verification = config['verification'] as Record<string, unknown> | undefined;
    if (verification?.['enabled'] !== undefined) {
      this.executionGroup?.critic.set(verification['enabled'] as boolean);
    }
    if (verification?.['max_rounds'] !== undefined) {
      this.executionGroup?.criticRounds.set(verification['max_rounds'] as number);
    }
  }

  /** Reset all sub-components to defaults. */
  resetAll(): void {
    this.executionGroup?.resetAll();
    this.modelGroup?.resetAll();
    this.toolsGroup?.resetAll();
    this.datasourcesGroup?.resetAll();
    this.instructionsTab?.resetAll();
    this.advancedAccordion?.resetAll();
    this.activeTab.set('settings');
  }

  private deepMerge(target: Record<string, unknown>, source: Record<string, unknown>): void {
    for (const key of Object.keys(source)) {
      const sv = source[key];
      const tv = target[key];
      if (
        typeof sv === 'object' && sv !== null && !Array.isArray(sv) &&
        typeof tv === 'object' && tv !== null && !Array.isArray(tv)
      ) {
        this.deepMerge(tv as Record<string, unknown>, sv as Record<string, unknown>);
      } else {
        target[key] = sv;
      }
    }
  }
}
