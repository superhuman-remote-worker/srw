import {Component, computed, effect, inject, input, output, signal} from '@angular/core';
import {FormsModule} from '@angular/forms';
import {TranslocoPipe, TranslocoService} from '@jsverse/transloco';
import {toSignal} from '@angular/core/rxjs-interop';
import {AppIconComponent} from '../../ui/icon';
import {UserService} from '../../core/services/user.service';
import {
  AUTONOMY_LEVELS,
  CRITIC_ROUND_OPTIONS,
  IMAGE_QUALITY_TIERS,
  PERMISSION_MODES,
  readConfigPath,
  pinResolvedValue,
  SettingsMode,
  TierReachability,
  WORKSPACE_BACKENDS,
} from './agent-settings.types';
import {PinOnInteractDirective} from './pin-on-interact.directive';
import {allowedEnumOptions} from './capability-gates';
import type {GrantCatalog} from '../../core/models/api.model';

/** See ExecutionGroupComponent.section. */
export type ExecutionSection = 'all' | 'task' | 'taskMore' | 'expert' | 'workspace';

/**
 * Execution settings group: autonomy, scholar, critic, project memory.
 * In session mode, shows permission mode instead of autonomy.
 */
@Component({
  selector: 'app-execution-group',
  standalone: true,
  imports: [FormsModule, TranslocoPipe, AppIconComponent, PinOnInteractDirective],
  template: `
    <div class="settings-group" [class.sectioned]="section() !== 'all'">
      @if (section() === 'all') {
        <div class="group-label">{{ 'agentSettings.execution.group' | transloco }}</div>
      }

      <!-- Autonomy (job) or Permission Mode (session) -->
      @if (has('task') && mode() === 'job') {
        <div class="field-row" [class.modified]="autonomy() !== null">
          <label class="field-label">{{ 'agentSettings.execution.autonomy' | transloco }}</label>
          <div class="field-control">
            <select
              class="form-input"
              [ngModel]="autonomy() ?? resolvedAutonomy()"
              appPinOnInteract (pin)="pinValue(autonomy, resolvedAutonomy())"
              (ngModelChange)="onAutonomyChange($event)"
              [disabled]="disabled()"
            >
              @for (level of autonomyOptions(); track level.value) {
                <option [value]="level.value">{{ 'agentSettings.autonomy.' + level.value + '.label' | transloco }}</option>
              }
            </select>
            @if (autonomy() !== null) {
              <button type="button" class="reset-btn" (click)="autonomy.set(null)" [title]="'agentSettings.common.resetToDefault' | transloco">
                <app-icon size="xs">close</app-icon>
              </button>
            }
          </div>
          <span class="field-hint">{{ effectiveAutonomyDesc() }}</span>
        </div>
      } @else if (has('task')) {
        <div class="field-row" [class.modified]="permissionMode() !== null">
          <label class="field-label">{{ 'agentSettings.execution.permissionMode' | transloco }}</label>
          <div class="field-control">
            <select
              class="form-input"
              [ngModel]="permissionMode() ?? resolvedPermissionMode()"
              appPinOnInteract (pin)="pinValue(permissionMode, resolvedPermissionMode())"
              (ngModelChange)="onPermissionModeChange($event)"
              [disabled]="disabled()"
            >
              @for (pm of permissionOptions(); track pm.value) {
                <option [value]="pm.value">{{ 'agentSettings.permissionModes.' + pm.value + '.label' | transloco }}</option>
              }
            </select>
            @if (permissionMode() !== null && mode() !== 'live') {
              <button type="button" class="reset-btn" (click)="permissionMode.set(null)" [title]="'agentSettings.common.resetToDefault' | transloco">
                <app-icon size="xs">close</app-icon>
              </button>
            }
          </div>
          <span class="field-hint">{{ effectivePermissionDesc() }}</span>
        </div>
      }

      <!-- Workspace backend. In job/session creation this is an ordinary
           setting. On a live session it is a launcher for the upgrade verb,
           not a setting: it always displays the running tier, unreachable
           tiers carry their reason, and picking one emits intent for the host
           to confirm and dispatch. -->
      @if (has('workspace') && mode() === 'live' && liveTier()) {
        <div class="field-row">
          <label class="field-label">{{ 'agentSettings.execution.workspaceBackend' | transloco }}</label>
          @if (anyTierReachable()) {
            <div class="field-control">
              <select
                class="form-input"
                (change)="onLiveTierPick($event)"
                [disabled]="disabled() || upgradeInProgress() !== null"
              >
                @for (t of liveTierOptions(); track t.value) {
                  <option [value]="t.value" [disabled]="t.disabled" [selected]="t.value === liveTier()">{{ t.label }}</option>
                }
              </select>
            </div>
          } @else {
            <!-- Nothing is reachable (a none-tier session): a select whose
                 every option is refused is worse than no select. -->
            <span class="static-value">{{ currentTierLabel() }}</span>
          }
          @if (upgradeInProgress(); as prog) {
            <span class="field-hint upgrading">
              <span class="progress-spinner"></span>
              {{ 'agentSettings.execution.tierUpgrading' | transloco:{ tier: tierLabel(prog.tier) } }}
              @if (prog.elapsed) { ({{ prog.elapsed }}s) }
            </span>
          } @else {
            <span class="field-hint">{{ 'agentSettings.execution.tierHint' | transloco }}</span>
          }
        </div>
      }

      @if (has('workspace') && mode() !== 'live' && showWorkspaceBackend()) {
        <div class="field-row" [class.modified]="workspaceBackend() !== null">
          <label class="field-label">{{ workspaceBackendLabel() | transloco }}</label>
          <div class="field-control">
            <select
              class="form-input"
              [ngModel]="workspaceBackend() ?? resolvedWorkspaceBackend()"
              appPinOnInteract (pin)="pinValue(workspaceBackend, resolvedWorkspaceBackend())"
              (ngModelChange)="onWorkspaceBackendChange($event)"
              [disabled]="disabled()"
            >
              @for (b of workspaceBackends; track b.value) {
                @if (b.value !== 'vm' || canUseVm()) {
                  <option [value]="b.value">{{ 'advanced.options.' + b.i18nKey | transloco }}</option>
                }
              }
            </select>
            @if (workspaceBackend() !== null) {
              <button type="button" class="reset-btn" (click)="resetWorkspaceBackend()" [title]="'agentSettings.common.resetToDefault' | transloco">
                <app-icon size="xs">close</app-icon>
              </button>
            }
          </div>
          @if (isLiteBackend()) {
            <span class="field-hint">{{ (isNoneBackend() ? 'advanced.hints.noneBackend' : 'advanced.hints.virtualBackend') | transloco }}</span>
          }
        </div>
      }

      <!-- Narration mode (live sessions only — a runtime chat concept, not a
           creation-form field; applied via the dedicated narration.set verb) -->
      @if (has('task') && mode() === 'live') {
        <div class="field-row" [class.modified]="narrationMode() !== null">
          <label class="field-label">{{ 'agentSettings.execution.narration' | transloco }}</label>
          <div class="field-control">
            <select
              class="form-input"
              [ngModel]="narrationMode() ?? resolvedNarrationMode()"
              appPinOnInteract (pin)="pinValue(narrationMode, resolvedNarrationMode())"
              (ngModelChange)="onNarrationModeChange($event)"
              [disabled]="disabled()"
            >
              @for (nm of narrationModes; track nm.value) {
                <option [value]="nm.value">{{ 'agentSettings.narrationModes.' + nm.value | transloco }}</option>
              }
            </select>
          </div>
        </div>
      }

      <!-- Image quality (jobs + session creation; not in the live honored set,
           so a sectioned live host shows it locked instead of hiding it) -->
      @if (has('expert') && (mode() !== 'live' || section() === 'expert')) {
        <div class="field-row" [class.modified]="imageQuality() !== null">
          <label class="field-label">{{ 'agentSettings.execution.imageQuality' | transloco }}
            @if (mode() === 'live') {
              <span class="locked-note">{{ 'agentSettings.locked.atCreation' | transloco }}</span>
            }
          </label>
          <div class="field-control">
            <select
              class="form-input"
              [ngModel]="imageQuality() ?? resolvedImageQuality()"
              appPinOnInteract (pin)="pinValue(imageQuality, resolvedImageQuality())"
              (ngModelChange)="onImageQualityChange($event)"
              [disabled]="disabled() || mode() === 'live'"
            >
              @for (q of imageQualityTiers; track q.value) {
                <option [value]="q.value">{{ 'agentSettings.imageQuality.' + q.value + '.label' | transloco }}</option>
              }
            </select>
            @if (imageQuality() !== null && mode() !== 'live') {
              <button type="button" class="reset-btn" (click)="imageQuality.set(null)" [title]="'agentSettings.common.resetToDefault' | transloco">
                <app-icon size="xs">close</app-icon>
              </button>
            }
          </div>
          <span class="field-hint">{{ effectiveImageQualityDesc() }}</span>
        </div>
      }

      <!-- Scholar toggle -->
      @if (has('taskMore') && mode() === 'job') {
        <div class="field-row toggle-row" [class.modified]="scholar() !== null">
          <label class="toggle-label">
            <input
              type="checkbox"
              [checked]="scholar() ?? resolvedScholar()"
              (change)="onScholarChange($event)"
              [disabled]="disabled()"
            >
            <span>{{ 'agentSettings.execution.scholar' | transloco }}</span>
          </label>
          @if (scholar() !== null) {
            <button type="button" class="reset-btn" (click)="scholar.set(null)" [title]="'agentSettings.common.resetToDefault' | transloco">
              <app-icon size="xs">close</app-icon>
            </button>
          }
        </div>
      }

      <!-- Critic toggle + rounds -->
      @if (has('taskMore') && mode() === 'job') {
        <div class="field-row toggle-row" [class.modified]="critic() !== null || criticRounds() !== null">
          <label class="toggle-label">
            <input
              type="checkbox"
              [checked]="critic() ?? resolvedCritic()"
              (change)="onCriticChange($event)"
              [disabled]="disabled()"
            >
            <span>{{ 'agentSettings.execution.critic' | transloco }}</span>
          </label>
          @if (effectiveCritic()) {
            <select
              class="form-input compact-select"
              [ngModel]="criticRounds() ?? resolvedCriticRounds()"
              appPinOnInteract (pin)="pinValue(criticRounds, resolvedCriticRounds())"
              (ngModelChange)="onCriticRoundsChange($event)"
              [disabled]="disabled()"
            >
              @for (opt of criticRoundOptions; track opt.value) {
                <option [ngValue]="opt.value">{{ 'agentSettings.criticRounds.' + opt.value | transloco }}</option>
              }
            </select>
          }
          @if (critic() !== null || criticRounds() !== null) {
            <button type="button" class="reset-btn" (click)="resetCritic()" [title]="'agentSettings.common.resetToDefault' | transloco">
              <app-icon size="xs">close</app-icon>
            </button>
          }
        </div>
      }

      <!-- Project memory toggle -->
      @if (has('taskMore') && mode() === 'job' && showProjectMemory()) {
        <div class="field-row toggle-row" [class.modified]="projectMemory() !== null">
          <label class="toggle-label">
            <input
              type="checkbox"
              [checked]="projectMemory() ?? resolvedProjectMemory()"
              (change)="onProjectMemoryChange($event)"
              [disabled]="disabled()"
            >
            <span>{{ 'agentSettings.execution.projectMemory' | transloco }}</span>
          </label>
          @if (projectMemory() !== null) {
            <button type="button" class="reset-btn" (click)="projectMemory.set(null)" [title]="'agentSettings.common.resetToDefault' | transloco">
              <app-icon size="xs">close</app-icon>
            </button>
          }
        </div>
      }
    </div>
  `,
  styles: [`
    .settings-group {
      margin-bottom: 20px;
    }
    .settings-group.sectioned {
      margin-bottom: 0;
    }
    .locked-note {
      margin-left: 6px;
      font-weight: 400;
      color: var(--text-muted);
    }
    .group-label {
      font-size: 11px;
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 0.06em;
      color: var(--text-muted);
      margin-bottom: 12px;
      padding-bottom: 6px;
      border-bottom: 1px solid var(--border-color, var(--surface-0));
    }
    .field-row {
      margin-bottom: 12px;
      padding-left: 8px;
      border-left: 2px solid transparent;
      transition: border-color 0.15s;
    }
    .field-row.modified {
      border-left-color: var(--accent-color, var(--accent-color));
    }
    .field-label {
      display: block;
      font-size: 12px;
      font-weight: 500;
      color: var(--text-primary, var(--text-primary));
      margin-bottom: 4px;
    }
    .field-control {
      display: flex;
      align-items: center;
      gap: 6px;
    }
    .form-input {
      flex: 1;
      padding: 7px 10px;
      border: 1px solid var(--border-color, var(--surface-1));
      border-radius: var(--radius-control);
      background: var(--surface-0, var(--surface-0));
      color: var(--text-primary, var(--text-primary));
      font-family: inherit;
      font-size: 13px;
    }
    .form-input:focus {
      outline: none;
      border-color: var(--accent-color, var(--accent-color));
    }
    .form-input:disabled {
      opacity: 0.6;
      cursor: not-allowed;
    }
    .compact-select {
      width: auto;
      min-width: 120px;
      padding: 4px 8px;
      font-size: 12px;
      margin-left: 8px;
    }
    .toggle-row {
      display: flex;
      align-items: center;
      gap: 8px;
      flex-wrap: wrap;
    }
    .toggle-label {
      display: flex;
      align-items: center;
      gap: 6px;
      font-size: 13px;
      color: var(--text-primary, var(--text-primary));
      cursor: pointer;
    }
    .toggle-label input[type="checkbox"] {
      accent-color: var(--accent-color, var(--accent-color));
    }
    .reset-btn {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      width: 20px;
      height: 20px;
      border: none;
      border-radius: 50%;
      background: rgba(255, 255, 255, 0.08);
      color: var(--text-muted);
      cursor: pointer;
      flex-shrink: 0;
    }
    .reset-btn:hover {
      background: var(--danger-tint);
      color: var(--danger);
    }
    .field-hint {
      display: block;
      font-size: 11px;
      color: var(--text-muted);
      margin-top: 2px;
    }
    .static-value {
      display: block;
      font-size: 13px;
      font-weight: 500;
      color: var(--text-primary, var(--text-primary));
    }
    .field-hint.upgrading {
      display: flex;
      align-items: center;
      gap: 6px;
    }
    .progress-spinner {
      width: 10px;
      height: 10px;
      border: 2px solid var(--border-color);
      border-top-color: var(--accent-color);
      border-radius: 50%;
      animation: execution-group-spin 0.8s linear infinite;
      flex-shrink: 0;
    }
    @keyframes execution-group-spin { to { transform: rotate(360deg); } }
  `],
})
export class ExecutionGroupComponent {
  private readonly transloco = inject(TranslocoService);
  private readonly userService = inject(UserService);
  private readonly activeLang = toSignal(this.transloco.langChanges$, {
    initialValue: this.transloco.getActiveLang(),
  });

  constructor() {
    // Snap ineligible users off 'vm' so the form can't submit a denied backend.
    // The server is authoritative — this is a UX safeguard.
    //
    // Creation forms only: a live session already running on a VM is a fact,
    // not a proposal, and the form has no business second-guessing it.
    effect(() => {
      if (this.mode() === 'live' || !this.showWorkspaceBackend() || !this.has('workspace')) return;
      if (this.canUseVm()) return;
      if (this.effectiveBackend() === 'vm') {
        this.workspaceBackend.set('sandbox');
        this.change.emit();
      }
    });
  }

  /** Merged expert/framework config. */
  config = input<Record<string, unknown>>({});
  mode = input<SettingsMode>('job');
  disabled = input(false);
  /** Whether the selected project has shared memory enabled. */
  showProjectMemory = input(false);
  /** Author's resolved capability grants for editor greying; null ⇒ no gating. */
  gatedCapabilities = input<Record<string, unknown> | null>(null);
  /** Capability catalog (supplies the enum `order` for ceiling filtering). */
  catalog = input<GrantCatalog>({});

  // --- Live-session workspace tier (live mode only) ---
  /** The running session's tier. Null outside live mode, or before it is
   *  known — the row hides rather than guessing. */
  liveTier = input<string | null>(null);
  /** Per-tier reachability, decided by the host. Absent entries read as
   *  `unsupported`. */
  tierReachability = input<Record<string, TierReachability>>({});
  /** Non-null while an upgrade is running, which disables the control. */
  upgradeInProgress = input<{tier: string; elapsed?: number} | null>(null);
  /** False when a create form's workspace picker owns the choice (Slice A3):
   *  the row, the VM snap and the backend override all stay out of the way. */
  showWorkspaceBackend = input(true);
  /** Which rows this instance renders and writes. `all` is the single-group
   *  layout the Expert editor uses; the creation forms and the live pane mount
   *  one instance per section: `task` (autonomy or permission mode, live
   *  narration), `taskMore` (scholar, critic, project memory), `expert`
   *  (image quality) and `workspace` (the tier row). */
  section = input<ExecutionSection>('all');

  /** True when this instance owns the given section's rows. */
  has(section: Exclude<ExecutionSection, 'all'>): boolean {
    const own = this.section();
    return own === 'all' || own === section;
  }

  /** The row's label key. The Expert editor passes
   *  'agentSettings.execution.workspaceRecommended', because an Expert only recommends. */
  workspaceBackendLabel = input('agentSettings.execution.workspaceBackend');

  change = output<void>();
  /** A live tier the user picked. Intent only — the host confirms and
   *  dispatches the upgrade verb; this component never mutates tier state. */
  tierChangeRequested = output<string>();

  // Constants
  readonly autonomyLevels = AUTONOMY_LEVELS;
  readonly criticRoundOptions = CRITIC_ROUND_OPTIONS;
  readonly permissionModes = PERMISSION_MODES;
  readonly imageQualityTiers = IMAGE_QUALITY_TIERS;

  /** Autonomy levels at/below the granted ceiling. The currently-selected value
   *  is always kept visible (an admin-authored expert may pin a higher level). */
  readonly autonomyOptions = computed(() => {
    const allowed = new Set(
      allowedEnumOptions(this.gatedCapabilities(), 'autonomy_ceiling',
        this.autonomyLevels.map((l) => l.value), this.catalog()),
    );
    const current = this.autonomy() ?? this.resolvedAutonomy();
    return this.autonomyLevels.filter((l) => allowed.has(l.value) || l.value === current);
  });

  /** Permission modes at/below the granted ceiling (current always kept). */
  readonly permissionOptions = computed(() => {
    const allowed = new Set(
      allowedEnumOptions(this.gatedCapabilities(), 'permission_mode',
        this.permissionModes.map((p) => p.value), this.catalog()),
    );
    const current = this.permissionMode() ?? this.resolvedPermissionMode();
    return this.permissionModes.filter((p) => allowed.has(p.value) || p.value === current);
  });

  readonly narrationModes = [
    {value: 'auto'},
    {value: 'verbose'},
    {value: 'silent'},
  ] as const;

  // User overrides (null = use default)
  readonly autonomy = signal<string | null>(null);
  readonly permissionMode = signal<string | null>(null);
  readonly narrationMode = signal<string | null>(null);
  readonly scholar = signal<boolean | null>(null);
  readonly critic = signal<boolean | null>(null);
  readonly criticRounds = signal<number | null>(null);
  readonly projectMemory = signal<boolean | null>(null);
  readonly imageQuality = signal<string | null>(null);
  readonly workspaceBackend = signal<string | null>(null);
  readonly workspaceBackends = WORKSPACE_BACKENDS;

  /** Label for a tier, from the shared `advanced.options.*` vocabulary. `next`
   *  is not a real backend (it means "whatever tier the server picks"), so it
   *  gets its own label rather than a `WORKSPACE_BACKENDS` lookup. An
   *  otherwise-unrecognised tier renders its raw value rather than vanishing —
   *  better a bare string than silently re-labelling the session as something
   *  else. */
  tierLabel(tier: string): string {
    this.activeLang();
    if (tier === 'next') return this.transloco.translate('agentSettings.execution.tierNext');
    const known = WORKSPACE_BACKENDS.find((b) => b.value === tier);
    return known ? this.transloco.translate(`advanced.options.${known.i18nKey}`) : tier;
  }

  readonly currentTierLabel = computed(() => this.tierLabel(this.liveTier() ?? ''));

  /** Reachability of a tier from the running one. Anything the host did not
   *  speak to is refused — fail closed, the server gates this anyway. */
  private tierState(tier: string): TierReachability {
    if (tier === this.liveTier()) return 'current';
    return this.tierReachability()[tier] ?? 'unsupported';
  }

  /** Every tier, each carrying its own reason when it cannot be picked. */
  readonly liveTierOptions = computed(() => {
    this.activeLang();
    const current = this.liveTier();
    const values: string[] = WORKSPACE_BACKENDS.map((b) => b.value);
    if (current && !values.includes(current)) values.unshift(current);
    return values.map((value) => {
      const state = this.tierState(value);
      const base = this.tierLabel(value);
      const suffix =
        state === 'current' ? this.transloco.translate('agentSettings.execution.tierCurrent')
        : state === 'ok' ? ''
        : this.transloco.translate(`agentSettings.execution.tierUnreachable.${state}`);
      return {
        value,
        label: suffix ? `${base} — ${suffix}` : base,
        // The running tier stays selectable: it is what the select displays.
        disabled: state !== 'ok' && state !== 'current',
      };
    });
  });

  /** False when every tier is refused (a `none` session today). */
  readonly anyTierReachable = computed(() =>
    this.liveTierOptions().some((t) => !t.disabled && t.value !== this.liveTier()),
  );

  /** Whether the current user is allowed to pick the VM backend. Admins always qualify. */
  readonly canUseVm = computed(() => {
    const u = this.userService.currentUser();
    return !!(u?.is_admin || u?.can_use_vm);
  });

  /** Effective backend = override → resolved config default. The lite tiers
   *  (`virtual`/`none`) run with no workspace container, so shell/browser/git
   *  tools are gated off server-side (no_workspace_agent_mode.md §7); the
   *  Advanced accordion reads this through its `backendOverride` input to grey
   *  the matching controls. `none` additionally has no file tools. */
  readonly effectiveBackend = computed(() => this.workspaceBackend() ?? this.resolvedWorkspaceBackend());
  readonly isLiteBackend = computed(() => {
    const b = this.effectiveBackend();
    return b === 'virtual' || b === 'none';
  });
  readonly isNoneBackend = computed(() => this.effectiveBackend() === 'none');

  // Resolved defaults from config
  readonly resolvedAutonomy = computed(() =>
    (readConfigPath(this.config(), 'autonomy') as string) ?? 'review'
  );
  readonly resolvedPermissionMode = computed(() =>
    (readConfigPath(this.config(), 'interactive.permission_mode') as string) ?? 'supervised'
  );
  readonly resolvedNarrationMode = computed(() =>
    (readConfigPath(this.config(), 'interactive.narration_mode') as string) ?? 'auto'
  );
  readonly resolvedScholar = computed(() =>
    (readConfigPath(this.config(), 'scholar.enabled') as boolean) ?? true
  );
  readonly resolvedCritic = computed(() =>
    (readConfigPath(this.config(), 'verification.enabled') as boolean) ?? true
  );
  readonly resolvedCriticRounds = computed(() =>
    (readConfigPath(this.config(), 'verification.max_rounds') as number) ?? 5
  );
  readonly resolvedProjectMemory = computed(() =>
    (readConfigPath(this.config(), 'memory.project_scoped') as boolean) ?? true
  );
  readonly resolvedImageQuality = computed(() =>
    (readConfigPath(this.config(), 'image_quality') as string) ?? 'standard'
  );
  readonly resolvedWorkspaceBackend = computed(() =>
    (readConfigPath(this.config(), 'workspace.backend') as string) ?? 'sandbox'
  );

  readonly effectiveAutonomyDesc = computed(() => {
    this.activeLang();
    const val = this.autonomy() ?? this.resolvedAutonomy();
    const known = this.autonomyLevels.find(l => l.value === val);
    if (!known) return this.transloco.translate('agentSettings.execution.autonomyDefaultHint');
    return this.transloco.translate(`agentSettings.autonomy.${val}.description`);
  });

  readonly effectivePermissionDesc = computed(() => {
    this.activeLang();
    const val = this.permissionMode() ?? this.resolvedPermissionMode();
    const known = this.permissionModes.find(p => p.value === val);
    if (!known) return '';
    return this.transloco.translate(`agentSettings.permissionModes.${val}.description`);
  });

  readonly effectiveCritic = computed(() =>
    this.critic() ?? this.resolvedCritic()
  );

  readonly effectiveImageQualityDesc = computed(() => {
    this.activeLang();
    const val = this.imageQuality() ?? this.resolvedImageQuality();
    const known = this.imageQualityTiers.find(q => q.value === val);
    if (!known) return '';
    return this.transloco.translate(`agentSettings.imageQuality.${val}.description`);
  });

  /** Number of fields that differ from defaults. */
  readonly modifiedCount = computed(() => {
    this.section();
    let count = 0;
    if (this.has('task')) {
      if (this.mode() === 'job') {
        if (this.autonomy() !== null) count++;
      } else {
        if (this.permissionMode() !== null) count++;
        if (this.narrationMode() !== null) count++;
      }
    }
    if (this.has('taskMore')) {
      if (this.scholar() !== null) count++;
      if (this.critic() !== null) count++;
      if (this.criticRounds() !== null) count++;
      if (this.projectMemory() !== null) count++;
    }
    if (this.has('expert') && this.imageQuality() !== null) count++;
    if (this.has('workspace') && this.workspaceBackend() !== null) count++;
    return count;
  });

  /** Commit a displayed-but-inherited value on deliberate interaction.
   *  See PinOnInteractDirective — a <select> emits no change event when the
   *  option already showing is re-picked, so without this the resolved default
   *  is the one value the form cannot express. */
  pinValue<T>(target: {(): T | null; set(value: T | null): void}, resolved: T): void {
    if (pinResolvedValue(target, resolved)) this.change.emit();
  }

  onAutonomyChange(value: string): void {
    this.autonomy.set(value);
    this.change.emit();
  }

  onPermissionModeChange(value: string): void {
    this.permissionMode.set(value);
    this.change.emit();
  }

  onNarrationModeChange(value: string): void {
    this.narrationMode.set(value);
    this.change.emit();
  }

  onScholarChange(event: Event): void {
    const checked = (event.target as HTMLInputElement).checked;
    this.scholar.set(checked);
    this.change.emit();
  }

  onCriticChange(event: Event): void {
    const checked = (event.target as HTMLInputElement).checked;
    this.critic.set(checked);
    this.change.emit();
  }

  onCriticRoundsChange(value: number): void {
    this.criticRounds.set(value);
    this.change.emit();
  }

  onProjectMemoryChange(event: Event): void {
    const checked = (event.target as HTMLInputElement).checked;
    this.projectMemory.set(checked);
    this.change.emit();
  }

  onImageQualityChange(value: string): void {
    this.imageQuality.set(value);
    this.change.emit();
  }

  onWorkspaceBackendChange(value: string): void {
    this.workspaceBackend.set(value);
    this.change.emit();
  }

  /** Reset emits, unlike the other rows here: the backend also drives the
   *  Advanced accordion's greying and the datasource picker's repo filter, so
   *  hosts have to hear about it. */
  resetWorkspaceBackend(): void {
    this.workspaceBackend.set(null);
    this.change.emit();
  }

  /** Live mode: a pick is a request, not a value.
   *
   *  The select displays the running tier and never holds a pending one — it
   *  snaps straight back, so a cancelled confirmation leaves nothing stale and
   *  a successful upgrade moves the row by changing `liveTier`. */
  onLiveTierPick(event: Event): void {
    const el = event.target as HTMLSelectElement;
    const picked = el.value;
    el.value = this.liveTier() ?? '';
    if (picked && picked !== this.liveTier()) this.tierChangeRequested.emit(picked);
  }

  resetCritic(): void {
    this.critic.set(null);
    this.criticRounds.set(null);
    this.change.emit();
  }

  /** Build the execution-related config_override fragment. */
  getOverrides(): Record<string, unknown> {
    const o: Record<string, unknown> = {};

    if (this.mode() === 'job') {
      if (this.has('task') && this.autonomy() !== null) o['autonomy'] = this.autonomy();
      if (this.has('taskMore')) {
        const s = this.scholar();
        if (s !== null) o['scholar'] = { enabled: s };
        const c = this.critic();
        const r = this.criticRounds();
        if (c !== null || r !== null) {
          o['verification'] = {
            enabled: c ?? this.resolvedCritic(),
            max_rounds: r ?? this.resolvedCriticRounds(),
          };
        }
        const pm = this.projectMemory();
        if (pm !== null) o['memory'] = { project_scoped: pm };
      }
    } else if (this.has('task')) {
      const interactive: Record<string, unknown> = {};
      if (this.permissionMode() !== null) interactive['permission_mode'] = this.permissionMode();
      if (this.narrationMode() !== null) interactive['narration_mode'] = this.narrationMode();
      if (Object.keys(interactive).length > 0) o['interactive'] = interactive;
    }

    // Image quality is a top-level knob honored by both worker and persistent
    // agents, so it applies regardless of mode — but never from the live pane,
    // where it is shown locked.
    if (this.has('expert') && this.mode() !== 'live' && this.imageQuality() !== null) {
      o['image_quality'] = this.imageQuality();
    }

    // Workspace backend. The rest of the `workspace` fragment (VM sizing, file
    // limits, git versioning) stays in the Advanced accordion; the host deep-
    // merges the two halves.
    //
    // Never in live mode: there the tier moves through the upgrade verb, and
    // letting it ride the pane's debounced config.update would be a second,
    // silent writer of the same fact.
    if (this.has('workspace') && this.mode() !== 'live' && this.showWorkspaceBackend() && this.workspaceBackend() !== null) {
      o['workspace'] = {backend: this.workspaceBackend()};
    }

    return o;
  }

  /** Reset all fields to defaults. */
  resetAll(): void {
    this.autonomy.set(null);
    this.permissionMode.set(null);
    this.narrationMode.set(null);
    this.scholar.set(null);
    this.critic.set(null);
    this.criticRounds.set(null);
    this.projectMemory.set(null);
    this.imageQuality.set(null);
    this.workspaceBackend.set(null);
  }
}
