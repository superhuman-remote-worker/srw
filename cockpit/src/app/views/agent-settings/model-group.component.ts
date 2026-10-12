import {Component, computed, inject, input, output, signal} from '@angular/core';
import {FormsModule} from '@angular/forms';
import {TranslocoPipe, TranslocoService} from '@jsverse/transloco';
import {AppIconComponent} from '../../ui/icon';
import {ModelService} from '../../core/services/model.service';
import {EffectiveModels, EffectiveModelSlot, SUBAGENT_INHERIT_MODEL} from '../../core/models/api.model';
import {pinResolvedValue, readConfigPath, SettingsMode} from './agent-settings.types';
import {PinOnInteractDirective} from './pin-on-interact.directive';
import {defaultSelectableReasoning, getSelectableReasoningOptions} from './reasoning-options';
import {liftLegacyTiers} from '../experts/expert-config';

// UI-only "last selected model" preselect keys (localStorage). Deliberately NOT
// the account-settings keys (`default_*_model`): a per-job/session control must
// not write a global account preference. See Layer 2 in the issue doc
// loop_ran_codex_spark_not_selected_model_then_hung_on_cooldown.md.
// One key per surface (job form / session form) plus the subagent row — the
// per-phase `strategic` / `tactical` keys went with the tiers (U1).
const STORAGE_KEYS = {
  job: 'ui.lastModel.job',
  session: 'ui.lastModel.session',
  subagent: 'ui.lastModel.subagent',
} as const;

type StorageKey = keyof typeof STORAGE_KEYS;

/**
 * Model settings group: ONE model dropdown for the whole run (`llm.model`) in
 * every mode — an expert has a single model since U1; the per-phase
 * strategic/tactical pickers and their mismatch advisory are gone — plus, in
 * job mode, the roster-wide subagent model (`subagents.llm.model`). Session
 * mode additionally owns the reasoning level (and live mode the temperature).
 */
@Component({
  selector: 'app-model-group',
  standalone: true,
  imports: [FormsModule, TranslocoPipe, AppIconComponent, PinOnInteractDirective],
  template: `
    <div class="settings-group">
      <div class="group-label">{{ 'agentSettings.model.group' | transloco }}</div>

      <!-- Strategic+tactical preset chips were removed in chunk 7 of
           models_yaml_removal — the legacy presets block in models.yaml
           has no DB-backed successor. The catalog carries enough
           metadata to rebuild presets if demand surfaces. -->

      <!-- The model: job and session alike write llm.model. -->
      <div class="field-row" [class.modified]="model() !== null">
        <label class="field-label">{{ 'agentSettings.model.single' | transloco }}</label>
        <div class="field-control">
          <select
            class="form-input"
            [ngModel]="model()"
            (ngModelChange)="onModelChange($event)"
            [disabled]="disabled()"
          >
            <option [ngValue]="null">{{ defaultLabel(effectiveModel(), resolvedModel()) }}</option>
            @for (group of availableModels(); track group.group) {
              <optgroup [label]="providerLabel(group)">
                @for (model of group.models; track model) {
                  <option [value]="model">{{ model }}</option>
                }
              </optgroup>
            }
          </select>
          @if (model() !== null && mode() !== 'live') {
            <button type="button" class="reset-btn" (click)="onModelChange(null)" [title]="'agentSettings.common.resetToDefault' | transloco"><app-icon size="xs">close</app-icon></button>
          }
        </div>
      </div>

      @if (mode() === 'job' && showSubagent()) {
        <!-- Subagent model — the roster-wide default (subagents.llm.model):
             every roster entry without a model of its own runs on it, and an
             unset picker means the children inherit the model above. Gated by
             the parent on the Delegation tool. -->
        <div class="field-row" [class.modified]="subagentModel() !== null">
          <label class="field-label">{{ 'agentSettings.model.subagent' | transloco }}</label>
          <div class="field-control">
            <select
              class="form-input"
              [ngModel]="subagentModel()"
              (ngModelChange)="onSubagentModelChange($event)"
              [disabled]="disabled()"
            >
              <option [ngValue]="null">{{ defaultLabel(effectiveSubagent(), resolvedSubagentModel()) }}</option>
              @for (group of availableModels(); track group.group) {
                <optgroup [label]="providerLabel(group)">
                  @for (model of group.models; track model) {
                    <option [value]="model">{{ model }}</option>
                  }
                </optgroup>
              }
            </select>
            @if (subagentModel() !== null) {
              <button type="button" class="reset-btn" (click)="onSubagentModelChange(null)" [title]="'agentSettings.common.resetToDefault' | transloco"><app-icon size="xs">close</app-icon></button>
            }
          </div>
          <span class="field-hint">{{ 'agentSettings.model.subagentRoster' | transloco }}</span>
        </div>
      }

      <!-- Live sessions: temperature (creation forms leave it to Advanced /
           the family matrix; live mode surfaces it because it has always
           been runtime-mutable and the retired header popover exposed it) -->
      @if (mode() === 'live') {
        <div class="field-row" [class.modified]="temperature() !== null">
          <label class="field-label">
            {{ 'agentSettings.model.temperature' | transloco }}
            <span class="temp-value">{{ temperature() ?? resolvedTemperature() }}</span>
          </label>
          <div class="field-control">
            <input type="range" class="form-input temp-slider" min="0" max="2" step="0.1"
                   [ngModel]="temperature() ?? resolvedTemperature()"
                   (ngModelChange)="onTemperatureChange($event)"
                   [disabled]="disabled()">
          </div>
        </div>
      }

      <!-- Session: reasoning level for the model in effect (options are
           capability-driven per family; the value rides llm.reasoning_level).
           No "Default" sentinel — the resolved default is preselected and
           marked, like the model picker; hidden when the model offers no
           choice (method none / always_on). Job mode keeps its reasoning
           control in the Advanced accordion. -->
      @if (mode() !== 'job' && sessionReasoningOptions().length) {
        <div class="field-row" [class.modified]="sessionReasoning() !== null">
          <label class="field-label">{{ 'agentSettings.model.reasoning' | transloco }}</label>
          <div class="field-control">
            <select
              class="form-input"
              [ngModel]="sessionReasoning() ?? resolvedSessionReasoning()"
              appPinOnInteract (pin)="pinReasoning()"
              (ngModelChange)="onSessionReasoningChange($event)"
              [disabled]="disabled()"
            >
              @for (opt of sessionReasoningOptions(); track opt.value) {
                <!-- "(default)" names the FAMILY default and never moves: a
                     pinned level is shown by being selected, not by borrowing
                     the marker. It used to follow resolvedSessionReasoning(),
                     so pinning max re-labelled max as "(default)" — the
                     family default had not changed, only the label. -->
                <option [value]="opt.value">
                  {{ opt.value === familyReasoningDefault()
                    ? ('agentSettings.model.reasoningDefaultOption' | transloco: {label: opt.label})
                    : opt.label }}
                </option>
              }
            </select>
            @if (sessionReasoning() !== null && mode() !== 'live') {
              <button type="button" class="reset-btn" (click)="onSessionReasoningChange(null)" [title]="'agentSettings.common.resetToDefault' | transloco"><app-icon size="xs">close</app-icon></button>
            }
          </div>
          <!-- task-3: the reset stays (per-family vocabularies don't
               translate) but must no longer be silent — the select alone
               looks identical to "never touched", and gpt-5.6-sol's default
               (High) reads a lot like a forgotten Max at a glance. -->
          @if (reasoningResetNotice()) {
            <div class="reasoning-reset-notice">
              <app-icon size="xs">info</app-icon>
              <span>{{ 'agentSettings.model.reasoningResetNotice' | transloco }}</span>
            </div>
          }
        </div>
      }
    </div>
  `,
  styles: [`
    .settings-group {
      margin-bottom: 20px;
    }
    .temp-value {
      font-weight: 400;
      color: var(--text-muted);
      margin-left: 6px;
    }
    .temp-slider {
      padding: 0;
      accent-color: var(--accent-color);
    }
    .group-label {
      font-size: 11px;
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 0.06em;
      color: var(--text-muted);
      margin-bottom: 12px;
      padding-bottom: 6px;
      border-bottom: 1px solid var(--border-color);
    }
    .field-row {
      margin-bottom: 12px;
      padding-left: 8px;
      border-left: 2px solid transparent;
      transition: border-color 0.15s;
    }
    .field-row.modified {
      border-left-color: var(--accent-color);
    }
    .field-label {
      display: block;
      font-size: 12px;
      font-weight: 500;
      color: var(--text-primary);
      margin-bottom: 4px;
    }
    .field-control {
      display: flex;
      align-items: center;
      gap: 6px;
    }
    .field-hint {
      display: block;
      margin-top: 4px;
      font-size: 11px;
      line-height: 1.4;
      color: var(--text-muted);
    }
    .form-input {
      flex: 1;
      padding: 7px 10px;
      border: 1px solid var(--border-color);
      border-radius: var(--radius-control);
      background: var(--surface-0);
      color: var(--text-primary);
      font-family: inherit;
      font-size: 13px;
    }
    .form-input:focus {
      outline: none;
      border-color: var(--accent-color);
    }
    .form-input:disabled {
      opacity: 0.6;
      cursor: not-allowed;
    }
    .reset-btn {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      width: 20px;
      height: 20px;
      border: none;
      border-radius: 50%;
      background: color-mix(in srgb, var(--text-primary) 8%, transparent);
      color: var(--text-muted);
      cursor: pointer;
      flex-shrink: 0;
    }
    .reset-btn:hover {
      background: var(--danger-tint);
      color: var(--danger);
    }
    .reasoning-reset-notice {
      display: flex;
      align-items: center;
      gap: 5px;
      margin-top: 5px;
      color: var(--text-muted);
      font-size: 11px;
      line-height: 1.4;
    }
    .reasoning-reset-notice app-icon {
      flex-shrink: 0;
    }
  `],
})
export class ModelGroupComponent {
  private readonly modelService = inject(ModelService);
  private readonly transloco = inject(TranslocoService);

  providerLabel(group: {group: string; configured: boolean}): string {
    return group.configured
      ? group.group
      : this.transloco.translate('agentSettings.model.providerNoKey', {provider: group.group});
  }

  config = input<Record<string, unknown>>({});
  mode = input<SettingsMode>('job');
  disabled = input(false);
  /** Server-resolved effective model + provenance per slot (what runs if the
   *  picker is left untouched). Null → fall back to the config-derived default. */
  effectiveModels = input<EffectiveModels | null>(null);
  /** Whether to show the Subagent (roster-wide) model picker. The parent
   *  gates this on the Delegation tool being enabled; defaults to shown so the
   *  component works standalone. */
  showSubagent = input(true);
  /** Preselect the model last picked on this surface (localStorage) when the
   *  config pins none. The three-section host turns it off: there a template
   *  is the starting point, so an untouched form must show the template's own
   *  default instead of a pick carried over from an earlier job. */
  rememberLastModel = input(true);

  change = output<void>();

  readonly availableModels = this.modelService.models;

  // The one model override (llm.model) — job and session alike.
  readonly model = signal<string | null>(null);

  // Job mode: roster-wide subagent model override (subagents.llm.model).
  readonly subagentModel = signal<string | null>(null);

  // Live mode: temperature override (creation forms don't render this row)
  readonly temperature = signal<number | null>(null);

  // Session mode: reasoning-level override for the model in effect. Owned here
  // (not in the Advanced accordion) so session mode has exactly one writer of
  // llm.reasoning_level; cleared on every model change so a level picked for
  // one model can't silently ride into another family's option set.
  readonly sessionReasoning = signal<string | null>(null);

  // True after a model change or a config prefill has cleared an existing
  // reasoning pick out from under the user — task-3 fix. The reset itself is
  // correct and stays (reasoning vocabularies are per-family and don't
  // translate), but until now it was silent: the select just snapped back to
  // the family default, indistinguishable from never having picked at all.
  // The prefill path in particular does not require a deliberate expert
  // switch — SessionCreateComponent's effective-default-expert resolution can
  // invoke it well after initial render (an `/experts` or `/expert-defaults`
  // response landing late, a project selection), so the user may never have
  // touched Model or the expert grid at all. Cleared by the next deliberate
  // reasoning interaction (a fresh pick, or re-confirming the shown default
  // via pinReasoning) or by resetAll.
  readonly reasoningResetNotice = signal(false);

  // The config input is a merged dict (base ⊕ account ⊕ expert) that may still
  // carry a pre-U1 expert's `llm.strategic` / `llm.subagent` tiers as stored;
  // read it through the compat mapping so a legacy pin shows as THE model.
  private readonly normalizedConfig = computed(() => liftLegacyTiers(this.config(), {merged: true}));

  // Config-derived default (fallback when the server `effective_models` is
  // absent — e.g. older API or the "defaults" virtual expert). The server value
  // is authoritative because it includes the account/system default floor this
  // config-only view can't see.
  readonly resolvedModel = computed(() =>
    (readConfigPath(this.normalizedConfig(), 'llm.model') as string) ?? null
  );
  // Roster-wide subagent pin → the model, mirroring the resolver (`inherit`
  // IS the parent's model) and the server effective-model slot.
  readonly resolvedSubagentModel = computed(() => {
    const pin = readConfigPath(this.normalizedConfig(), 'subagents.llm.model') as string | null;
    return pin && pin !== SUBAGENT_INHERIT_MODEL ? pin : this.resolvedModel();
  });
  readonly resolvedTemperature = computed(() =>
    (readConfigPath(this.config(), 'llm.temperature') as number) ?? 0.7
  );
  // The reasoning value in effect when the user picks nothing: a config-pinned
  // level (when the model's option set actually contains it) else the family
  // default. Never an unmatched value — a native select with no matching
  // option renders blank.
  readonly resolvedSessionReasoning = computed(() => {
    const options = this.sessionReasoningOptions();
    if (!options.length) return null;
    const pin = (readConfigPath(this.normalizedConfig(), 'llm.reasoning_level') as string) ?? null;
    if (pin && options.some((o) => o.value === pin)) return pin;
    return defaultSelectableReasoning(this.sessionReasoningCap());
  });
  // The family default of the model in effect — the option that carries the
  // "(default)" marker. Deliberately blind to any config pin: the pin decides
  // what is SELECTED (resolvedSessionReasoning), the family decides what is
  // labelled default, and the two only coincide when nothing is pinned.
  readonly familyReasoningDefault = computed(() =>
    defaultSelectableReasoning(this.sessionReasoningCap()),
  );

  // Server-resolved effective slot ({model, source}) or null when unavailable.
  readonly effectiveModel = computed(() => this.effectiveModels()?.model ?? null);
  readonly effectiveSubagent = computed(() => this.effectiveModels()?.subagent ?? null);

  // The model actually in effect: explicit override > server effective >
  // config-derived fallback. Drives the session reasoning options.
  readonly modelInEffect = computed(() =>
    this.model() ?? this.effectiveModel()?.model ?? this.resolvedModel(),
  );

  /** Reasoning capability of the model in effect (null when unknown). */
  private readonly sessionReasoningCap = computed(() => {
    const model = this.modelInEffect();
    return model ? (this.modelService.reasoningByModel()[model] ?? null) : null;
  });
  /** Concrete reasoning options (no "Default" sentinel — the resolved default
   *  is preselected instead, like the model picker). Empty hides the field. */
  readonly sessionReasoningOptions = computed(() =>
    getSelectableReasoningOptions(this.sessionReasoningCap()),
  );

  /**
   * Label for the unset "Default" option: names the model that will run if the
   * picker is left alone, with provenance. `eff` is the server-resolved slot;
   * `fallback` the config-derived model used when the server didn't resolve one.
   */
  defaultLabel(eff: EffectiveModelSlot | null, fallback: string | null): string {
    const model = eff?.model ?? fallback;
    if (!model) return this.transloco.translate('agentSettings.model.default');
    if (eff?.source) {
      const src = this.transloco.translate('agentSettings.model.source.' + eff.source);
      return this.transloco.translate('agentSettings.model.defaultResolved', {model, source: src});
    }
    return this.transloco.translate('agentSettings.model.defaultResolvedNoSource', {model});
  }

  readonly modifiedCount = computed(() => {
    let count = 0;
    if (this.model() !== null) count++;
    if (this.mode() === 'job') {
      if (this.subagentModel() !== null) count++;
    } else {
      if (this.sessionReasoning() !== null) count++;
      if (this.temperature() !== null) count++;
    }
    return count;
  });

  /** Commit a displayed-but-inherited value on deliberate interaction.
   *  See PinOnInteractDirective — a <select> emits no change event when the
   *  option already showing is re-picked, so without this the resolved default
   *  is the one value the form cannot express. */
  pinValue<T>(target: {(): T | null; set(value: T | null): void}, resolved: T): void {
    if (pinResolvedValue(target, resolved)) this.change.emit();
  }

  onModelChange(value: string | null): void {
    this.model.set(value);
    // A reasoning pick is model-specific intent — drop it with the model so a
    // stale level never leaks into another family's option set. Only raise
    // the notice if there was actually a pick to lose (task-3 fix).
    if (this.sessionReasoning() !== null) this.reasoningResetNotice.set(true);
    this.sessionReasoning.set(null);
    this.persistModel(this.modelStorageKey(), value);
    this.change.emit();
  }

  onSubagentModelChange(value: string | null): void {
    this.subagentModel.set(value);
    this.persistModel('subagent', value);
    this.change.emit();
  }

  onSessionReasoningChange(value: string | null): void {
    // Only the explicit "Default" option clears the override; picking the
    // concrete level that happens to be the default pins it.
    this.reasoningResetNotice.set(false);
    this.sessionReasoning.set(value);
    this.change.emit();
  }

  /** Reasoning select's PinOnInteract handler: re-confirming the shown
   *  (resolved-default) value is also the user acknowledging the field, so it
   *  dismisses a pending reset notice the same as a fresh pick would. */
  pinReasoning(): void {
    this.reasoningResetNotice.set(false);
    this.pinValue(this.sessionReasoning, this.resolvedSessionReasoning());
  }

  onTemperatureChange(value: number): void {
    this.temperature.set(value);
    this.change.emit();
  }

  /** Build the model-related config_override fragment:
   *  `{llm: {model}}` (+ `{subagents: {llm: {model}}}` in job mode; +
   *  `reasoning_level` / `temperature` in session and live mode) — only the
   *  parts the user actually set. */
  getOverrides(): Record<string, unknown> {
    const out: Record<string, unknown> = {};
    const llm: Record<string, unknown> = {};

    const m = this.model();
    if (m) llm['model'] = m;
    if (this.mode() !== 'job') {
      const r = this.sessionReasoning();
      if (r !== null) llm['reasoning_level'] = r;
      const t = this.temperature();
      if (t !== null) llm['temperature'] = t;
    }
    if (Object.keys(llm).length > 0) out['llm'] = llm;

    if (this.mode() === 'job') {
      const subm = this.subagentModel();
      if (subm) out['subagents'] = {llm: {model: subm}};
    }
    return out;
  }

  resetAll(): void {
    this.model.set(null);
    this.subagentModel.set(null);
    this.sessionReasoning.set(null);
    this.reasoningResetNotice.set(false);
    this.temperature.set(null);
  }

  /** Prefill from expert config (called by parent when expert changes).
   *  Falls back to saved user preferences when config doesn't specify a model.
   *  A pre-U1 fragment's `llm.strategic` / `llm.subagent` pins count as the
   *  model / subagent model here (compat mapping), never as user overrides. */
  prefillFromConfig(config: Record<string, unknown>): void {
    const lifted = liftLegacyTiers(config, {merged: true});
    const llm = lifted['llm'] as Record<string, unknown> | undefined;
    const baseModel = (llm?.['model'] as string) ?? null;
    const rosterLlm = (lifted['subagents'] as Record<string, unknown> | undefined)?.['llm'] as
      | Record<string, unknown>
      | undefined;
    const rosterModel = (rosterLlm?.['model'] as string) ?? null;

    const remember = this.rememberLastModel();
    this.model.set(baseModel || !remember ? null : this.loadSavedModel(this.modelStorageKey()));
    // Subagents inherit the model, so a pinned model is also "already
    // resolved" for them — don't preselect a saved subagent model then.
    this.subagentModel.set(rosterModel || baseModel || !remember ? null : this.loadSavedModel('subagent'));
    // Reasoning picks don't survive a config prefill — the new expert's
    // config (and possibly family) makes the old level meaningless. This
    // fires on more than a deliberate expert switch: SessionCreateComponent
    // also calls it from applyEffectiveDefault()'s automatic resolution of
    // the effective default expert, which nothing in this component's own
    // controls gates — so surface it the same as the model-change clearer
    // above (task-3 fix), only when there was a pick to lose.
    if (this.sessionReasoning() !== null) this.reasoningResetNotice.set(true);
    this.sessionReasoning.set(null);
  }

  /** The job form and the session form remember their last pick separately. */
  private modelStorageKey(): StorageKey {
    return this.mode() === 'job' ? 'job' : 'session';
  }

  private loadSavedModel(key: StorageKey): string | null {
    try {
      return localStorage.getItem(STORAGE_KEYS[key]) ?? null;
    } catch {
      return null;
    }
  }

  // UI-only: remember the last picked model in localStorage to preselect the
  // dropdown next time. Does NOT write account preferences — that silent global
  // write (default_strategic_model / default_tactical_model) is what shadowed an
  // explicit loop/job model. Account defaults are edited only on the Settings page.
  private persistModel(key: StorageKey, value: string | null): void {
    const storageKey = STORAGE_KEYS[key];
    try {
      if (value) {
        localStorage.setItem(storageKey, value);
      } else {
        localStorage.removeItem(storageKey);
      }
    } catch {
      // localStorage may be unavailable
    }
  }
}
