import {Component, computed, input, output, signal} from '@angular/core';
import {FormsModule} from '@angular/forms';
import {TranslocoPipe} from '@jsverse/transloco';
import {AppIconComponent} from '../../ui/icon';
import type {
    SessionToolCategory,
    SessionToolGroupsResponse,
} from '../../core/services/api.service';
import {
    ALL_TOOL_CATEGORIES,
    JOB_TOOL_CATEGORIES,
    readConfigPath,
    SESSION_TOOL_CATEGORIES,
    SettingsMode,
    ToolCategoryMeta,
} from './agent-settings.types';
import {
    enabledCategoryKeys,
    humanizeCategoryKey,
    isMeasured,
    lockedOnAdditions,
    resolvedToolRows,
    rosterLines,
    toolsetProvenance,
    toolsFragment,
    type ResolvedToolRow,
} from './resolved-toolset';

/** Source of per-instance ids for the cap field's label and hint. */
let nextCapInputId = 0;

/** True when every selectable category key is enabled (none in the disabled set). */
export function allToolCategoriesSelected(
  selectableKeys: string[],
  disabledCategories: Set<string>,
): boolean {
  return selectableKeys.length > 0 && selectableKeys.every(k => !disabledCategories.has(k));
}

/** Category keys explicitly disabled by a resolved config. */
export function disabledToolCategoriesFromConfig(
  config: Record<string, unknown>,
  categoryKeys: string[],
): Set<string> {
  const tools = config['tools'] as Record<string, unknown[]> | undefined;
  const disabled = new Set<string>();
  if (!tools) return disabled;
  for (const key of categoryKeys) {
    const value = tools[key];
    if (Array.isArray(value) && value.length === 0) disabled.add(key);
  }
  return disabled;
}

/**
 * Which delegation cap the Delegation row's number field edits.
 *
 * - `worker`: `delegation.max_concurrent` — jobs and the expert editor.
 * - `session`: `delegation.session_max_concurrent` — a session thread (the
 *   create form and the live pane). A session allowed to fan out ignores
 *   `max_concurrent` (parallel_subagents.md §6.4, D2), so a session that wrote
 *   it asked for 2 and got 6.
 */
export type DelegationCapScope = 'worker' | 'session';

/**
 * The cap scope for a settings surface's mode. `session` and `live` are both a
 * session THREAD (the create form and the running session's pane); `job` is a
 * worker. The expert editor does not go through this: it edits an expert, and
 * keeps the worker cap.
 */
export function delegationCapScopeForMode(mode: SettingsMode): DelegationCapScope {
  return mode === 'job' ? 'worker' : 'session';
}

/** The key under `delegation` each scope writes. */
export type DelegationCapKey = 'max_concurrent' | 'session_max_concurrent';

/**
 * The session cap's contract, mirrored from
 * `src/shared/runtime/core/delegation_settings.py` (default and clamp range)
 * and the orchestrator's `validate_delegation_override` (what a write may
 * carry: an integer from 1 to 20, booleans refused).
 */
export const SESSION_DELEGATION_CAP = {
  path: 'delegation.session_max_concurrent',
  familyPath: 'delegation.family_session_max_concurrent',
  min: 1,
  max: 20,
  default: 6,
} as const;

/**
 * A session cap as a value the orchestrator accepts, else null.
 *
 * Never clamps: a value the boundary would refuse is not silently turned into
 * a different number the user did not type.
 */
export function sessionDelegationCapValue(raw: unknown): number | null {
  if (typeof raw !== 'number' || !Number.isInteger(raw)) return null;
  return raw >= SESSION_DELEGATION_CAP.min && raw <= SESSION_DELEGATION_CAP.max ? raw : null;
}

/** A stored session cap as the runtime reads it (`clamp_session_max_concurrent`):
 *  a number, truncated and clamped into range; anything else is unset. */
function clampedSessionDelegationCap(raw: unknown): number | null {
  if (typeof raw !== 'number' || !Number.isFinite(raw)) return null;
  const {min, max} = SESSION_DELEGATION_CAP;
  return Math.max(min, Math.min(max, Math.trunc(raw)));
}

/**
 * The session cap a config layer carries.
 *
 * `explicit` is `delegation.session_max_concurrent` on this layer (null =
 * unset). `inherited` is what applies when it is unset: the parent model
 * family's value when the layer carries one, else the code default 6.
 */
export function resolveSessionDelegationCap(
  config: Record<string, unknown>,
): {explicit: number | null; inherited: number} {
  return {
    explicit: clampedSessionDelegationCap(readConfigPath(config, SESSION_DELEGATION_CAP.path)),
    inherited:
      clampedSessionDelegationCap(readConfigPath(config, SESSION_DELEGATION_CAP.familyPath))
      ?? SESSION_DELEGATION_CAP.default,
  };
}

/** The only live delegation knobs after the U3 runtime replacement. `capKey`
 *  is the cap the scope writes; `cap` null = the user set none, so nothing is
 *  written and the value stays inherited. */
export function delegationOverride(
  enabled: boolean,
  baselineEnabled: boolean,
  cap: number | null,
  capKey: DelegationCapKey = 'max_concurrent',
): Record<string, unknown> | null {
  if (enabled === baselineEnabled && cap === null) return null;
  const result: Record<string, unknown> = {};
  if (enabled !== baselineEnabled) result['enabled'] = enabled;
  if (cap !== null) result[capKey] = cap;
  return result;
}

/**
 * Tool category controls — three states, not two.
 *
 * `on` / `off` / `unavailable with a reason`. A checkbox cannot say "you lack
 * the shell_tools grant", "this workspace tier has no shell", or "granted by
 * the runtime, not by config", and forcing those into an unticked box is what
 * produced both defects this change closes: a toggle that silently no-ops, and
 * a form that promises an enablement the agent will not honour.
 *
 * When a resolved answer is supplied (`resolved`), it decides everything: which
 * categories exist, which are on, which cannot be touched and why. Without one
 * — an older orchestrator, or a surface with no read wired up — the component
 * falls back to the static per-mode list and two states, which is what it
 * always did.
 */
@Component({
  selector: 'app-tools-group',
  standalone: true,
  imports: [FormsModule, TranslocoPipe, AppIconComponent],
  template: `
    <div class="settings-group">
      <div class="group-header">
        <span class="group-label">{{ 'agentSettings.tools.group' | transloco }}</span>
        <button
          type="button"
          class="select-all-btn"
          (click)="toggleAll()"
          [disabled]="disabled() || selectableCategories().length === 0"
        >{{ (allSelected() ? 'agentSettings.common.deselectAll' : 'agentSettings.common.selectAll') | transloco }}</button>
      </div>
      @if (showsProvenance()) {
        <div class="toolset-provenance" [attr.data-trust]="provenance().trust">
          <app-icon size="xs">{{ provenanceIcon() }}</app-icon>
          <span class="provenance-headline">
            {{ 'agentSettings.tools.provenance.' + provenance().trust | transloco }}
          </span>
          @if (provenance().detail; as detail) {
            <span class="provenance-detail">{{ detail }}</span>
          }
        </div>
      }

      <div class="tool-toggles">
        @for (row of rows(); track row.key) {
          <label
            class="tool-toggle"
            [class.modified]="!row.pristine"
            [class.unavailable]="rowState(row) === 'unavailable'"
            [class.disabled]="disabled() || !isRowSettable(row)"
            [title]="isCategoryBlocked(row.key) ? ('grants.lockedShort' | transloco) : (row.reason ?? '')"
          >
            @if (rowState(row) === 'unavailable') {
              <app-icon size="sm" class="tool-state-blocked">block</app-icon>
            } @else {
              <input
                type="checkbox"
                [checked]="rowState(row) === 'on'"
                (change)="toggleCategory(row.key)"
                [disabled]="disabled() || isRowLocked(row)"
              >
            }
            <app-icon size="md" class="tool-toggle-icon">{{ row.meta?.icon ?? 'category' }}</app-icon>
            <span class="tool-toggle-info">
              <span class="tool-toggle-name">@if (row.meta) {{{ 'agentSettings.toolCategories.' + row.key + '.label' | transloco }}} @else {{{ humanize(row.key) }}}@if (isRowLocked(row)) { <span class="tool-lock">🔒</span> }@if (row.toolCount) { <span class="tool-count">{{ (measured() ? 'agentSettings.tools.countBound' : 'agentSettings.tools.countPredicted') | transloco:{ n: row.toolCount } }}</span> }</span>
              @if (row.meta) {
                <span class="tool-toggle-desc">{{ 'agentSettings.toolCategories.' + row.key + '.description' | transloco }}</span>
              } @else {
                <span class="tool-toggle-desc">{{ 'agentSettings.tools.unknownCategory' | transloco }}</span>
              }
              @if (row.reason) {
                <!-- On an ON row the sentence explains "you cannot change
                     this", not "this is off". Rendering it in the warning
                     colour would say the second thing. -->
                <span [class]="rowState(row) === 'on' ? 'tool-toggle-note' : 'tool-toggle-reason'"
                >{{ row.reason }}</span>
              } @else if (isCategoryBlocked(row.key)) {
                <span class="tool-toggle-reason">{{ 'grants.lockedShort' | transloco }}</span>
              }
            </span>
            @if (!row.pristine && mode() !== 'live') {
              <button
                class="reset-btn"
                (click)="resetCategory(row.key, $event)"
                [title]="'agentSettings.common.resetToDefault' | transloco"
              ><app-icon size="xs">close</app-icon></button>
            }
          </label>
          <!-- The other direction of a lock.
               settable:false says "you cannot switch this off", and the
               disabled checkbox above says it faithfully. It does NOT say
               "config cannot add here" — a per-tool code grant locks the row
               because everything BOUND is code-granted, which leaves the
               category's config-grantable members untouched. On the default
               topology that is how a running session gains shell.
               A checkbox cannot express an additive-only action: the only
               gesture available from a ticked box is unticking it, which is
               the one thing that must never happen here. So the action is its
               own control, it names what it will add, and it has no off state
               at all. -->
          @if (additionsFor(row).length) {
            <div class="tool-additions" [attr.data-category]="row.key">
              <span class="tool-additions-note">{{
                'agentSettings.tools.addable' | transloco:{ names: additionsFor(row).join(', ') }
              }}</span>
              <button
                type="button"
                class="tool-additions-btn"
                (click)="requestAdditions(row.key)"
                [disabled]="disabled() || additionsRequested(row.key)"
              >{{ (additionsRequested(row.key)
                    ? 'agentSettings.tools.addRequested'
                    : 'agentSettings.tools.addAction') | transloco:{ n: additionsFor(row).length } }}</button>
            </div>
          }
          <!-- The roster a tick reaches, on or off: the empty case is the one
               worth seeing BEFORE ticking — delegate_agent binds either way
               and errors on every call when the expert has no roster. No
               claim (older orchestrator, failed resolve) renders nothing. -->
          @if (row.key === 'delegation') {
            @let lines = roster();
            @if (lines !== null) {
              @if (lines.length) {
                <ul class="roster-list" [attr.aria-label]="'agentSettings.tools.rosterTitle' | transloco">
                  @for (line of lines; track line.name) {
                    <li class="roster-entry">
                      <span class="roster-name">{{ line.name }}@if (line.isDefault) { <span class="roster-default">{{ 'agentSettings.tools.rosterDefault' | transloco }}</span> }</span>
                      @if (line.description) {
                        <span class="roster-desc">{{ line.description }}</span>
                      }
                    </li>
                  }
                </ul>
              } @else {
                <div class="roster-empty">
                  <span class="tool-toggle-reason">{{ 'agentSettings.tools.rosterEmpty' | transloco }}</span>
                </div>
              }
            }
          }
          @if (row.key === 'delegation' && rowState(row) === 'on') {
            @if (delegationCapScope() === 'session') {
              <!-- A session's own cap. Unset renders EMPTY with the inherited
                   value as placeholder: showing 6 as a typed value would make
                   an untouched form look like a choice, and nothing is written
                   until the user types one. A set cap is the placeholder too:
                   ngModel does not rewrite a field the user emptied when the
                   bound value is unchanged (typed 5 over a held 5), and the
                   empty field must then show 5, not the default. -->
              <div class="inline-params inline-params-stacked">
                <div class="inline-field" [class.modified]="delegationCap() !== null">
                  <label class="inline-label" [attr.for]="capInputId">{{ 'agentSettings.tools.sessionCap.label' | transloco }}</label>
                  <input type="number" class="inline-input number-input"
                    [id]="capInputId"
                    [attr.min]="sessionCapMin" [attr.max]="sessionCapMax" step="1"
                    [attr.placeholder]="sessionCap().explicit ?? sessionCap().inherited"
                    [ngModel]="delegationCap() ?? sessionCap().explicit"
                    (ngModelChange)="onDelegationCapChange($event)"
                    [attr.aria-invalid]="delegationCapInvalid()"
                    [attr.aria-describedby]="capInputId + '-hint'"
                    [disabled]="disabled()">
                  <!-- Live mode is pin-only: the protocol has no clear op, so
                       a reset here would show "inherited" over a session that
                       still holds the pin. -->
                  @if (delegationCap() !== null && mode() !== 'live') {
                    <button type="button" class="reset-btn"
                      [title]="'agentSettings.common.resetToDefault' | transloco"
                      [attr.aria-label]="'agentSettings.common.resetToDefault' | transloco"
                      (click)="resetDelegationCap()"><app-icon size="xs">close</app-icon></button>
                  }
                </div>
                <span class="inline-hint" [id]="capInputId + '-hint'">{{ 'agentSettings.tools.sessionCap.hint' | transloco }}</span>
                @if (delegationCapInvalid()) {
                  <span class="tool-toggle-reason" role="alert">{{
                    'agentSettings.tools.sessionCap.invalid' | transloco:{ min: sessionCapMin, max: sessionCapMax }
                  }}</span>
                } @else if (delegationCap() === null && sessionCap().explicit === null) {
                  <span class="inline-hint">{{
                    'agentSettings.tools.sessionCap.inherited' | transloco:{ n: sessionCap().inherited }
                  }}</span>
                }
              </div>
            } @else {
              <div class="inline-params">
                <div class="inline-field" [class.modified]="delegationCap() !== null">
                  <label class="inline-label">{{ 'agentSettings.tools.maxConcurrent' | transloco }}</label>
                  <input type="number" class="inline-input number-input" min="1" step="1"
                    [ngModel]="delegationCap() ?? resolvedDelegationMaxConcurrent()"
                    (ngModelChange)="onDelegationCapChange($event)"
                    [disabled]="disabled()">
                  @if (delegationCap() !== null) {
                    <button type="button" class="reset-btn" (click)="resetDelegationCap()"><app-icon size="xs">close</app-icon></button>
                  }
                </div>
              </div>
            }
          }
        }
      </div>
    </div>
  `,
  styles: [`
    .settings-group {
      margin-bottom: 20px;
    }
    .group-header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
      margin-bottom: 12px;
      padding-bottom: 6px;
      border-bottom: 1px solid var(--border-color, var(--surface-0));
    }
    .group-label {
      font-size: 11px;
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 0.06em;
      color: var(--text-muted);
    }
    .select-all-btn {
      flex-shrink: 0;
      background: none;
      border: none;
      padding: 0;
      cursor: pointer;
      font-family: inherit;
      font-size: 11px;
      font-weight: 600;
      color: var(--accent-color, var(--accent-color));
    }
    .select-all-btn:hover:not(:disabled) {
      text-decoration: underline;
    }
    .select-all-btn:disabled {
      opacity: 0.5;
      cursor: not-allowed;
    }
    .toolset-provenance {
      display: flex;
      align-items: baseline;
      flex-wrap: wrap;
      gap: 4px 8px;
      margin-bottom: 10px;
      padding: 6px 10px;
      border-radius: var(--radius-control);
      border-left: 2px solid var(--border-color, var(--surface-1));
      background: rgba(255, 255, 255, 0.03);
      font-size: 11px;
      line-height: 1.4;
      color: var(--text-muted);
    }
    .toolset-provenance[data-trust="measured"] {
      border-left-color: var(--success, var(--accent-color));
    }
    .toolset-provenance[data-trust="predicted"],
    .toolset-provenance[data-trust="measured_partial"],
    .toolset-provenance[data-trust="unknown"] {
      border-left-color: var(--warning, var(--text-muted));
    }
    .provenance-headline {
      font-weight: 600;
      color: var(--text-primary, var(--text-primary));
    }
    .tool-toggles {
      display: flex;
      flex-direction: column;
      gap: 4px;
    }
    .tool-toggle {
      display: flex;
      align-items: center;
      gap: 10px;
      padding: 8px 10px;
      border-radius: var(--radius-control);
      border-left: 2px solid transparent;
      cursor: pointer;
      transition: background 0.15s, border-color 0.15s;
    }
    .tool-toggle:hover:not(.disabled) {
      background: rgba(255, 255, 255, 0.03);
    }
    .tool-toggle.modified {
      border-left-color: var(--accent-color, var(--accent-color));
    }
    .tool-toggle.disabled {
      opacity: 0.6;
      cursor: not-allowed;
    }
    .tool-toggle.unavailable {
      opacity: 0.75;
    }
    .tool-state-blocked {
      flex-shrink: 0;
      color: var(--text-muted);
    }
    .tool-toggle-reason {
      font-size: 11px;
      line-height: 1.4;
      color: var(--warning, var(--text-muted));
    }
    .tool-toggle-note {
      font-size: 11px;
      line-height: 1.4;
      color: var(--text-muted);
    }
    .tool-count {
      margin-left: 6px;
      font-size: 11px;
      font-weight: 500;
      text-transform: uppercase;
      letter-spacing: 0.06em;
      color: var(--text-muted);
    }
    .tool-toggle input[type="checkbox"] {
      accent-color: var(--accent-color, var(--accent-color));
      flex-shrink: 0;
    }
    .tool-toggle-icon {
      color: var(--text-muted);
      flex-shrink: 0;
    }
    .tool-toggle-info {
      display: flex;
      flex-direction: column;
      gap: 1px;
      flex: 1;
      min-width: 0;
    }
    .tool-toggle-name {
      font-size: 13px;
      font-weight: 500;
      color: var(--text-primary, var(--text-primary));
    }
    .tool-toggle-desc {
      font-size: 11px;
      color: var(--text-muted);
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
    .inline-params {
      display: flex;
      gap: 12px;
      padding: 6px 10px 6px 42px;
    }
    .inline-params-stacked {
      flex-direction: column;
      gap: 3px;
    }
    .inline-hint {
      font-size: 11px;
      line-height: 1.4;
      color: var(--text-muted);
    }
    .roster-list {
      list-style: none;
      margin: 0;
      padding: 0 10px 6px 42px;
      display: flex;
      flex-direction: column;
      gap: 3px;
    }
    .roster-entry {
      font-size: 11px;
      line-height: 1.4;
      color: var(--text-muted);
    }
    .roster-name {
      margin-right: 6px;
      color: var(--text-primary);
      font-weight: 600;
    }
    .roster-default {
      margin-left: 4px;
      color: var(--text-muted);
      font-weight: 400;
    }
    .roster-empty {
      padding: 0 10px 6px 42px;
    }
    .tool-additions {
      display: flex;
      align-items: center;
      flex-wrap: wrap;
      gap: 4px 8px;
      padding: 2px 10px 6px 42px;
    }
    .tool-additions-note {
      font-size: 11px;
      line-height: 1.4;
      color: var(--text-muted);
    }
    .tool-additions-btn {
      flex-shrink: 0;
      padding: 2px 8px;
      border: 1px solid var(--accent-color, var(--surface-1));
      border-radius: var(--radius-control);
      background: none;
      color: var(--accent-color, var(--text-primary));
      cursor: pointer;
      font-family: inherit;
      font-size: 11px;
      font-weight: 600;
    }
    .tool-additions-btn:hover:not(:disabled) {
      background: rgba(255, 255, 255, 0.06);
    }
    .tool-additions-btn:disabled {
      border-color: var(--border-color, var(--surface-1));
      color: var(--text-muted);
      cursor: default;
    }
    .inline-field {
      display: flex;
      align-items: center;
      gap: 6px;
      padding-left: 4px;
      border-left: 2px solid transparent;
    }
    .inline-field.modified {
      border-left-color: var(--accent-color, var(--accent-color));
    }
    .inline-label {
      font-size: 11px;
      color: var(--text-muted);
      white-space: nowrap;
    }
    .inline-input {
      padding: 4px 8px;
      border: 1px solid var(--border-color, var(--surface-1));
      border-radius: var(--radius-control);
      background: var(--surface-0, var(--surface-0));
      color: var(--text-primary, var(--text-primary));
      font-family: inherit;
      font-size: 12px;
    }
    .inline-input:focus {
      outline: none;
      border-color: var(--accent-color, var(--accent-color));
    }
    .number-input {
      max-width: 80px;
    }
  `],
})
export class ToolsGroupComponent {
  config = input<Record<string, unknown>>({});
  mode = input<SettingsMode>('job');
  disabled = input(false);
  /**
   * The server's resolved answer — a measurement from the running agent, or a
   * labelled prediction. When present it supplies the category set, the three
   * states, the reasons and the write vocabulary; the merged `config` supplies
   * none of those, because a config-only view cannot see the runtime injection
   * layer, the backend capability gate, or a tool that failed to instantiate.
   * On a real session those three moved 28 names.
   */
  resolved = input<SessionToolGroupsResponse | null>(null);
  /**
   * True when the HOST performs a resolved read at all.
   *
   * Distinguishes "the read failed" from "nobody asked". A surface with no read
   * wired would otherwise fly a permanent "the resolved toolset could not be
   * read" banner reporting the failure of a request that was never made.
   *
   * Every host now passes `true` — the live pane, both creation forms and the
   * expert editor. Kept as an input rather than assumed because the distinction
   * it draws is the honest one for any surface added later, and defaulting it to
   * `false` means a new host is silent until it opts in rather than lying.
   */
  readsResolvedToolset = input(false);
  /**
   * The write vocabulary for categories that refuse `tools.<c>: true`, for
   * hosts that have it without a resolved read (`enumerate_only` on the expert
   * detail). A resolved answer carries its own and wins.
   *
   * Without it, ticking `shell` emits `true` and the boundary 400s naming a
   * rule the form gives the user no way to satisfy.
   */
  enumerateOnly = input<Record<string, string[]> | null>(null);
  /** Author's resolved capability grants for editor greying; null ⇒ no gating
   *  (launch flow / admin). Maps tool categories → catalog keys. */
  gatedCapabilities = input<Record<string, unknown> | null>(null);

  /**
   * Client mirror of `GRANT_GATED_CATEGORIES` (src/core/tool_report.py), used
   * ONLY for editor greying — `capability_grants.evaluate` is the enforcement.
   *
   * Known-incomplete: the seven `datasource_tools` categories have never been
   * listed here, so an author without that grant is not greyed and learns at
   * save time via 422. Pre-existing; adding them is a behaviour change to the
   * editor, not a fix to this row.
   */
  private readonly CAT_TO_GRANT: Record<string, string> = {
    shell: 'shell_tools',
    delegation: 'delegation',
    browser_direct: 'browser',
    catalog_authoring: 'catalog_authoring',
  };

  /** True if a category is blocked by a missing grant (disable-only — never
   *  mutates the fragment, so opening an admin-authored expert can't strip it). */
  isCategoryBlocked(catKey: string): boolean {
    const g = this.gatedCapabilities();
    if (g === null) return false;
    const grantKey = this.CAT_TO_GRANT[catKey];
    return !!grantKey && g[grantKey] !== true;
  }

  change = output<void>();

  /**
   * The user's own switch positions, or null while they have set none.
   *
   * Null is not "everything on" — it means "defer to the server's answer".
   * A plain set defaulting to empty would render every category ticked until
   * something remembered to anchor it, which is precisely the shape of the
   * bug being fixed: a control showing `on` for a group the agent does not
   * hold. Anchoring is still explicit and synchronous (`prefillFromResolved`)
   * so a host can order it against its own baseline; this only removes the
   * consequence of forgetting.
   */
  private readonly userDisabled = signal<Set<string> | null>(null);
  /** The baseline the diff is taken against, same fallback rule. */
  private readonly anchoredBaseline = signal<Set<string> | null>(null);

  /**
   * Which cap the Delegation row's number field edits — see
   * {@link DelegationCapScope}. The default is the worker cap, so a host that
   * does not say (the expert editor, job creation) keeps writing
   * `delegation.max_concurrent` exactly as before.
   */
  delegationCapScope = input<DelegationCapScope>('worker');

  /** Delegation inline param: what the user typed into the cap field, or null
   *  while they have typed nothing (the value then stays inherited). */
  readonly delegationCap = signal<number | null>(null);

  /** The session cap the config carries, and what applies when it carries none. */
  readonly sessionCap = computed(() => resolveSessionDelegationCap(this.config()));
  readonly sessionCapMin = SESSION_DELEGATION_CAP.min;
  readonly sessionCapMax = SESSION_DELEGATION_CAP.max;
  /** Ties the field's label and hint to its input; unique per instance. */
  readonly capInputId = `delegation-cap-${nextCapInputId++}`;

  /** True when the session cap field holds a value the orchestrator would
   *  refuse (not a whole number from 1 to 20). Such a value is never written;
   *  the create form blocks on it via {@link delegationCapValid}. Only while
   *  the field is shown: switching Delegation off hides the field and its
   *  error, and a hidden error must not keep the create form's submit blocked. */
  readonly delegationCapInvalid = computed(
    () =>
      this.delegationCapScope() === 'session'
      && this.delegationCap() !== null
      && sessionDelegationCapValue(this.delegationCap()) === null
      && this.rows().some((row) => row.key === 'delegation' && this.rowState(row) === 'on'),
  );

  /** The cap to write: the typed value, validated for a session, passed
   *  through unchanged for the worker cap (its behaviour predates this). */
  private readonly writableDelegationCap = computed<number | null>(() =>
    this.delegationCapScope() === 'session'
      ? sessionDelegationCapValue(this.delegationCap())
      : this.delegationCap(),
  );

  /** The categories from the server's answer, or null when there is none. */
  private readonly serverCategories = computed<Record<string, SessionToolCategory> | null>(
    () => this.resolved()?.categories ?? null,
  );

  /** Categories the server's answer says are OFF (or unavailable). Empty when
   *  there is no answer, which is what makes the static fallback render as it
   *  always did. */
  private readonly serverDisabled = computed<Set<string>>(() => {
    const categories = this.serverCategories();
    if (!categories) return new Set<string>();
    const on = enabledCategoryKeys(categories);
    return new Set(Object.keys(categories).filter((key) => !on.has(key)));
  });

  /** Current switch positions: the user's, else the server's answer. */
  readonly disabledCategories = computed<Set<string>>(
    () => this.userDisabled() ?? this.serverDisabled(),
  );

  /** What the diff is taken against: the last anchor, else the server's answer. */
  private readonly expertDisabledCategories = computed<Set<string>>(
    () => this.anchoredBaseline() ?? this.serverDisabled(),
  );

  /** Where the answer came from, and how far it can be trusted. */
  readonly provenance = computed(() => toolsetProvenance(this.resolved()));

  /** True when the answer came off a running agent (including `agent_partial`,
   *  which is a measurement with no timestamp — the common path against the
   *  currently deployed fleet image). Never inferred from `observed_at`. */
  readonly measured = computed(() => isMeasured(this.provenance().trust));

  /**
   * Whether to say anything about provenance at all.
   *
   * Always when there IS an answer — a surface must never render a measurement
   * or a forecast unlabelled. Otherwise only when the host claims to have
   * asked, so "could not be read" reports a real failure rather than the
   * absence of a request.
   */
  readonly showsProvenance = computed(
    () => this.serverCategories() !== null || this.readsResolvedToolset(),
  );

  readonly provenanceIcon = computed(() => {
    switch (this.provenance().trust) {
      case 'measured': return 'sensors';
      case 'measured_partial': return 'sensors_off';
      case 'predicted': return 'schedule';
      default: return 'help';
    }
  });

  /** Presentation metadata for the surface: the full catalogue when the server
   *  answers (it may name any category), the mode's own list otherwise. */
  private readonly categoryMeta = computed<ToolCategoryMeta[]>(() => {
    if (this.serverCategories()) return ALL_TOOL_CATEGORIES;
    return this.mode() === 'job' ? JOB_TOOL_CATEGORIES : SESSION_TOOL_CATEGORIES;
  });

  /**
   * The rendered rows.
   *
   * With a server answer this is EVERY category it returned — including ones
   * the cockpit has no metadata for. Filtering it to a hand-picked subset is
   * what left the live pane showing four of twenty-five, which is the same
   * class of untruth as a toggle that does nothing.
   */
  readonly rows = computed<ResolvedToolRow[]>(() => {
    const categories = this.serverCategories();
    const disabled = this.disabledCategories();
    if (categories) {
      return resolvedToolRows(categories, this.categoryMeta(), disabled);
    }
    // Live mode with no answer renders NOTHING, and the banner says why.
    //
    // The static fallback is only honest where a config supplies the baseline,
    // which is what `prefillFromConfig` gives the creation forms. The live
    // pane has no such config — a stock session's `config_override` carries no
    // `tools` key at all — so the fallback rendered twelve categories all
    // TICKED, six of which ship `[]` in session_base, and every switch was
    // dead because the pane's dispatch is keyed off the resolved answer. Six
    // false assertions and twelve dead toggles is this task's own pair of
    // headline defects, rebuilt on the degraded path. An empty section under
    // an explicit "could not be read" asserts nothing.
    if (this.mode() === 'live') return [];
    // Creation forms: the static list, two states, baseline from the config.
    return this.categoryMeta().map((meta) => ({
      key: meta.key,
      meta,
      state: disabled.has(meta.key) ? ('off' as const) : ('on' as const),
      settable: true,
      reason: null,
      decidedBy: 'unset',
      toolCount: 0,
      pristine: disabled.has(meta.key) === this.expertDisabledCategories().has(meta.key),
    }));
  });

  /** Kept for callers that still want the flat metadata list. */
  readonly categories = computed<ToolCategoryMeta[]>(() =>
    this.rows().map((row) => row.meta ?? {
      key: row.key,
      label: humanizeCategoryKey(row.key),
      icon: 'category',
      description: '',
    }),
  );

  /** Category keys nothing may write: the server refused, or the author's own
   *  capability grants forbid it. Both are real refusals and neither may be
   *  overwritten by a click. */
  private readonly unsettableKeys = computed<Set<string>>(
    () => new Set(this.rows().filter((row) => !this.isRowSettable(row)).map((row) => row.key)),
  );

  /**
   * Per category, the config-grantable tools a LOCKED-ON row could still gain.
   *
   * Read off the untouched server entry, never off a row's post-toggle
   * computed state: the answer must not move because the user clicked
   * something. Non-empty only where the client can NAME the tools — see
   * resolved-toolset.ts::lockedOnAdditions, which refuses to guess.
   *
   * A row the client's own grant gate blocks gets nothing: `isCategoryBlocked`
   * is a refusal in its own right, and offering to add tools the PDP will
   * deny is the same dead end as a checkbox the boundary 400s.
   */
  private readonly additionsByKey = computed<Record<string, string[]>>(() => {
    const categories = this.serverCategories();
    if (!categories) return {};
    const enumerateOnly = this.resolved()?.enumerate_only ?? this.enumerateOnly();
    const out: Record<string, string[]> = {};
    for (const key of Object.keys(categories)) {
      if (this.isCategoryBlocked(key)) continue;
      const names = lockedOnAdditions(key, categories[key], enumerateOnly);
      if (names.length) out[key] = names;
    }
    return out;
  });

  /**
   * Locked-on categories the user has asked to complete.
   *
   * ADDITIVE ONLY, and there is no gesture that removes a key. That is the
   * whole point of a separate channel: the off direction of a locked category
   * is not a request the server was ever asked to honour, so it must not be
   * representable here.
   */
  private readonly requestedAdditions = signal<Set<string>>(new Set());

  /** True when the user may flip this row: the server allows it AND the
   *  client-side grant gate does not grey it. */
  isRowSettable(row: ResolvedToolRow): boolean {
    return row.settable && !this.isCategoryBlocked(row.key);
  }

  /**
   * The state the row RENDERS as.
   *
   * `row.state` is the server's, which knows nothing about the client-side
   * grant gate — that gate exists because the server's grant lookup fails
   * OPEN on error, so it must stay a belt. A category it blocks is
   * *unavailable*: the third state exists precisely so a control does not have
   * to pretend that "you lack the grant" is an unticked box.
   *
   * **`on` survives a lock, from either gate.** A category the agent is
   * holding is on, and no gate makes that untrue — it only makes it
   * unchangeable. Drawing a block glyph over bound tools was the same class of
   * lie as the checkbox this control replaces, pointed the other way.
   */
  rowState(row: ResolvedToolRow): 'on' | 'off' | 'unavailable' {
    if (row.state === 'on') return 'on';
    return this.isRowSettable(row) ? row.state : 'unavailable';
  }

  /** True when the row cannot be changed — server or client gate. Orthogonal
   *  to its state: a locked row can be on, off or unavailable. */
  isRowLocked(row: ResolvedToolRow): boolean {
    return !this.isRowSettable(row);
  }

  /** The config-grantable tools this row could still gain, named. Empty for
   *  every row that is not locked-on, and for every locked-on row whose
   *  additions cannot be proved. */
  additionsFor(row: ResolvedToolRow): string[] {
    return this.additionsByKey()[row.key] ?? [];
  }

  /** True once the user has asked for this category's additions. The button
   *  then says so instead of accepting a second click that would diff to
   *  nothing — a silent no-op control is the defect this series exists to
   *  remove, and re-arming it here would be a small new one. */
  additionsRequested(key: string): boolean {
    return this.requestedAdditions().has(key);
  }

  /**
   * Ask for a locked-on category's config-grantable tools.
   *
   * One-way: there is no companion "un-request". The write is additive, the
   * PDP still enforces the author's own grants, and the pane dispatches it
   * through the same debounce as every other edit.
   */
  requestAdditions(key: string): void {
    if (!this.additionsByKey()[key]?.length) return;
    if (this.requestedAdditions().has(key)) return;
    this.requestedAdditions.update((set) => new Set(set).add(key));
    this.change.emit();
  }

  /**
   * Category → the enumeration to write for a requested addition.
   *
   * The dispatching host reads this INSTEAD of looking for the request in the
   * switch diff, where it is structurally invisible: a locked-on category is
   * on before and after, so its boolean never moves. See
   * settings-pane.component.ts::desiredState.
   */
  getToolAdditions(): Record<string, string[]> {
    const additions = this.additionsByKey();
    const out: Record<string, string[]> = {};
    for (const key of this.requestedAdditions()) {
      const names = additions[key];
      if (names?.length) out[key] = [...names];
    }
    return out;
  }

  humanize(key: string): string {
    return humanizeCategoryKey(key);
  }

  /** Categories the user can actually toggle. */
  readonly selectableCategories = computed<ToolCategoryMeta[]>(() =>
    this.rows()
      .filter((row) => this.isRowSettable(row))
      .map((row) => row.meta ?? {
        key: row.key,
        label: humanizeCategoryKey(row.key),
        icon: 'category',
        description: '',
      }),
  );

  /** True when every selectable category is currently enabled. */
  readonly allSelected = computed(() =>
    allToolCategoriesSelected(
      this.selectableCategories().map(cat => cat.key),
      this.disabledCategories(),
    )
  );

  readonly modifiedCount = computed(() => {
    let count = this.rows().filter((row) => !row.pristine).length;
    // A locked row is `pristine` by construction (its switch cannot move), so
    // a pending addition is invisible to the line above and would otherwise
    // not be counted as the edit it is.
    count += this.requestedAdditions().size;
    if (this.delegationCap() !== null) count++;
    return count;
  });

  /** True when the user has moved a tool switch since the last prefill. A
   *  parent uses this to decide whether a late-arriving read may re-anchor the
   *  baseline or would clobber a click. A pending addition counts: re-anchoring
   *  clears it (see `anchor`), so it is exactly the kind of edit a late read
   *  must not silently discard. A typed delegation cap counts for the same
   *  reason: the creation forms re-read the preview on every change, so
   *  without it the preview answering the cap edit re-anchored and wiped it. */
  hasToolEdits(): boolean {
    return this.requestedAdditions().size > 0
      || this.delegationCap() !== null
      || this.rows().some((row) => !row.pristine);
  }

  // --- Resolved defaults ---
  private r(path: string): unknown { return readConfigPath(this.config(), path); }

  /** What a Delegation tick reaches (resolved-toolset.ts `rosterLines`):
   *  null = no claim, [] = the expert has no roster, else its types. */
  readonly roster = computed(() => rosterLines(this.resolved()));

  readonly resolvedDelegationMaxConcurrent = computed(
    () => (this.r('delegation.max_concurrent') ?? 4) as number,
  );

  isCategoryEnabled(key: string): boolean {
    return !this.disabledCategories().has(key);
  }

  isModified(key: string): boolean {
    const disabled = this.disabledCategories().has(key);
    const wasDisabledByExpert = this.expertDisabledCategories().has(key);
    // Modified if: user disabled something that was enabled, or enabled something that was disabled
    return disabled !== wasDisabledByExpert;
  }

  toggleCategory(key: string): void {
    const next = new Set(this.disabledCategories());
    if (next.has(key)) next.delete(key);
    else next.add(key);
    this.userDisabled.set(next);
    this.change.emit();
  }

  /** Enable every selectable category, or disable them all if already all on.
   *  Grant-blocked categories are left untouched (they can't be toggled). */
  toggleAll(): void {
    const keys = this.selectableCategories().map(cat => cat.key);
    const selectAll = !allToolCategoriesSelected(keys, this.disabledCategories());
    const next = new Set(this.disabledCategories());
    for (const key of keys) {
      if (selectAll) next.delete(key);
      else next.add(key);
    }
    this.userDisabled.set(next);
    this.change.emit();
  }

  resetCategory(key: string, event: Event): void {
    event.preventDefault();
    event.stopPropagation();
    const next = new Set(this.disabledCategories());
    if (this.expertDisabledCategories().has(key)) next.add(key);
    else next.delete(key);
    this.userDisabled.set(next);
    if (key === 'delegation') {
      this.delegationCap.set(null);
    }
    this.change.emit();
  }

  /** The cap field changed. An emptied field is "no edit" (null), which in a
   *  creation form means inherit; an invalid session value is kept for the
   *  field to show beside its error, and is never written. */
  onDelegationCapChange(v: number | null): void {
    this.delegationCap.set(typeof v === 'number' ? v : null);
    this.change.emit();
  }

  resetDelegationCap(): void {
    this.delegationCap.set(null);
    this.change.emit();
  }

  /** False while the session cap field holds a value that would be refused. */
  delegationCapValid(): boolean {
    return !this.delegationCapInvalid();
  }

  /**
   * Build the tools + delegation config_override fragment.
   *
   * A DIFF against the baseline, in both directions: off is `[]`, on is the
   * policy the write boundary expands against the registry (`true`, or the
   * `enumerate_only` enumeration for a category that refuses `true`).
   *
   * The re-enable used to copy names out of `defaultsTools()` — the very layer
   * being overridden — and every category worth re-enabling ships `[]` there,
   * so the branch emitted nothing and ticking a box had never once enabled
   * anything. `true` cannot have that failure mode: the expansion happens
   * against the registry, server-side, at the boundary.
   */
  getOverrides(): Record<string, unknown> {
    const rows = this.rows();
    const tools = toolsFragment(rows.map((row) => row.key), {
      disabled: this.disabledCategories(),
      baselineOn: new Set(
        rows.map((row) => row.key).filter((key) => !this.expertDisabledCategories().has(key)),
      ),
      unsettable: this.unsettableKeys(),
      enumerateOnly: this.resolved()?.enumerate_only ?? this.enumerateOnly(),
    });
    // Requested additions ride the same fragment — they ARE a `tools` write.
    // `toolsFragment` cannot produce them (it refuses every unsettable key, in
    // both directions, which is what keeps the off half impossible), so they
    // are merged in here. Only ever an enumeration, never `[]`.
    for (const [key, names] of Object.entries(this.getToolAdditions())) {
      tools[key] = {only: names};
    }

    const result: Record<string, unknown> = {};
    if (Object.keys(tools).length > 0) result['tools'] = tools;

    // Delegation config: sync delegation.enabled with the tool toggle,
    // and include inline param overrides
    const delegationEnabled = this.isCategoryEnabled('delegation');
    const wasEnabledByExpert = !this.expertDisabledCategories().has('delegation');
    const delegation = delegationOverride(
      delegationEnabled,
      wasEnabledByExpert,
      this.writableDelegationCap(),
      this.delegationCapScope() === 'session' ? 'session_max_concurrent' : 'max_concurrent',
    );
    if (delegation) result['delegation'] = delegation;

    return result;
  }

  /** Called by parent when expert changes to sync disabled state.
   *
   *  The merged config is the WEAKER baseline and only used where no resolved
   *  answer exists — it reports a category as on whenever config granted names,
   *  which on a real session over-reported by 24 tools and under-reported by 4.
   *  Prefer `prefillFromResolved`. */
  prefillFromConfig(config: Record<string, unknown>): void {
    const disabled = disabledToolCategoriesFromConfig(
      config,
      this.categories().map((category) => category.key),
    );
    this.anchor(disabled);
  }

  /**
   * Anchor the baseline to the server's resolved answer.
   *
   * `state === 'on'` is the ONE definition of enabled here, and it is the
   * agent's when the agent answered. An unsettable category anchors as off, so
   * the diff can never emit config against a grant or a workspace tier.
   */
  prefillFromResolved(categories: Record<string, SessionToolCategory>): void {
    const on = enabledCategoryKeys(categories);
    this.anchor(new Set(Object.keys(categories).filter((key) => !on.has(key))));
  }

  private anchor(disabled: Set<string>): void {
    this.userDisabled.set(disabled);
    this.anchoredBaseline.set(new Set(disabled));

    // Reset delegation inline params on expert change
    this.delegationCap.set(null);
    // A re-anchor is a new baseline, so a request made against the old one is
    // no longer meaningful. Hosts guard this with `hasToolEdits()`.
    this.requestedAdditions.set(new Set());
  }

  resetAll(): void {
    this.userDisabled.set(new Set(this.expertDisabledCategories()));
    this.delegationCap.set(null);
    this.requestedAdditions.set(new Set());
  }
}
