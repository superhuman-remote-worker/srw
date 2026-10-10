import {Component, computed, effect, inject, input, output, signal} from '@angular/core';
import {TranslocoPipe} from '@jsverse/transloco';
import {AppIconComponent} from '../../ui/icon';
import {AppSpinnerComponent} from '../../ui/spinner';
import {ApiService} from '../../core/services/api.service';
import {ConnectorDriversService} from '../../core/services/connector-drivers.service';
import {
  ConnectorDriver,
  driverForType,
  publicReadWrite,
} from '../../core/models/connector-driver.model';
import {
  ConnectorAccessChoice,
  ConnectorAccessChoiceLevel,
  Datasource,
  DatasourceIndexStatus,
  DatasourceType,
  EligibleDatasource,
} from '../../core/models/api.model';
import {
  accessLevelLabelKey,
  isPlatformOwned,
  PlatformRow,
  platformKindLabelKey,
  platformOwnedKind,
  USER_CLOUD_ROOT,
} from '../../core/models/platform-connector';

/** A user's explicit picker selection, tagged with the datasource-set identity
 *  it was made against (so a stale tag falls back to the default). */
export type DatasourceSelection = {
  key: string;
  ids: Set<string>;
  /** IDs the user explicitly toggled. Untouched IDs follow current server
   * defaults when eligibility refreshes. Optional for legacy callers/tests. */
  touched?: Set<string>;
} | null;

export function isRepositoryDatasource(type: DatasourceType | string): boolean {
  return (type || '').toString().toLowerCase() === 'repository';
}

/** The workspace tiers without a shell, as the server's
 *  `LITE_WORKSPACE_BACKENDS` names them. */
const LITE_BACKENDS: readonly string[] = ['virtual', 'none'];

/** The built-in drivers that need a shell workspace: a copy of the server's
 *  `workspace_tier_refuses`, used only until the capability matrix loads. */
const SHELL_WORKSPACE_TYPES = new Set([
  'repository',
  'credentials',
  'generic',
  'ssh_key',
  'kubeconfig',
  'generic_file',
  // Mounted into the workspace (sandbox or VM tier).
  'cloud_folder',
  // A session's mount, or a copy from a job's repository.
  'cloud_outbox',
  // Copied into a job's repository before it starts.
  'cloud_folder_checkout',
]);

/** Whether a connector type needs a shell workspace: its driver's
 *  `supported_backends` leaves a lite tier out. The picker knows only that the
 *  backend is lite, not which tier, so a driver missing either one is held
 *  back (the server refuses per tier). Before the matrix loads, or for a type
 *  it does not list, the built-in copy decides. */
export function requiresShellWorkspace(
  type: DatasourceType | string,
  drivers?: readonly ConnectorDriver[] | null,
): boolean {
  const driver = driverForType(drivers, (type || '').toString());
  if (driver) return LITE_BACKENDS.some((tier) => !driver.supported_backends.includes(tier));
  return SHELL_WORKSPACE_TYPES.has((type || '').toString().toLowerCase());
}

/** How the picker shows the access level of a connector with strict levels
 *  (a cloud folder): chosen in a select on a selected row (New Session),
 *  shown (a job binds at its project link's level), or shown in a disabled
 *  select (a live session: the level changes only at the next start). */
export type DatasourceAccessMode = 'choose' | 'show' | 'locked';

/** The connector types a session mounts as cloud folders: one at
 *  `protected` makes the others stay out (`beside_protected`). */
const CLOUD_MOUNT_TYPES: ReadonlySet<string> = new Set(['cloud_folder', 'cloud_outbox']);

/** A row's access choice, when the eligible list sent one with levels. */
export function accessChoiceOf(ds: Datasource): ConnectorAccessChoice | null {
  if (ds.unavailable) return null;
  const access = (ds as Partial<EligibleDatasource>).access;
  return access && Array.isArray(access.levels) && access.levels.length ? access : null;
}

/** The translation key (and params) saying where a row's unchosen level
 *  came from: the project's default, the driver's, or a default lowered to
 *  what is allowed (by the owner's role in the folder's project when a
 *  ceiling is set). */
export function accessOriginKey(access: ConnectorAccessChoice): string {
  switch (access.default_origin) {
    case 'link_default':
      return 'agentSettings.datasources.access.origin.projectDefault';
    case 'clamped':
      return access.ceiling
        ? 'agentSettings.datasources.access.origin.loweredRole'
        : 'agentSettings.datasources.access.origin.lowered';
    case 'explicit':
    case 'materialized':
      return 'agentSettings.datasources.access.origin.chosen';
    default:
      return 'agentSettings.datasources.access.origin.default';
  }
}

const ACCESS_REFUSAL_KEYS: ReadonlyMap<string, string> = new Map([
  ['level_not_offered', 'agentSettings.datasources.access.refused.notOffered'],
  ['no_level_offered', 'agentSettings.datasources.access.refused.notOffered'],
  ['above_ceiling', 'agentSettings.datasources.access.refused.aboveCeiling'],
  ['read_only_unavailable', 'agentSettings.datasources.access.refused.readOnlyUnavailable'],
]);

/** The translation key of why a level may not be chosen, or null. */
export function accessRefusalKey(level: ConnectorAccessChoiceLevel): string | null {
  if (level.offered) return null;
  return ACCESS_REFUSAL_KEYS.get(String(level.reason ?? '')) ??
    'agentSettings.datasources.access.refused.notOffered';
}

/** Stable identity of a datasource set (order-independent). */
export function datasourceSetKey(datasources: {id: string}[]): string {
  return datasources
    .map(d => d.id)
    .sort()
    .join(',');
}

/** A picker row: what the selection rules read. */
type PickerRow = {id: string; default_selected?: boolean} & NonNullable<PlatformRow>;

/** The default selection. With `defaultIds` (live mode: the session's
 *  currently attached set, possibly empty), exactly those. Otherwise the
 *  server-computed `default_selected` rows: the platform-owned ones (the
 *  project's own connectors) always, every other one only while the
 *  rollout gate for server defaults is on. */
export function defaultDatasourceIds(
  datasources: PickerRow[],
  defaultIds?: Set<string> | null,
  serverDefaultsEnabled = false,
): Set<string> {
  if (defaultIds !== undefined && defaultIds !== null) {
    return new Set(datasources.filter(d => defaultIds.has(d.id)).map(d => d.id));
  }
  return new Set(
    datasources
      .filter(d => d.default_selected === true && (serverDefaultsEnabled || isPlatformOwned(d)))
      .map(d => d.id),
  );
}

/** Active selection: the user's tagged choice, or the default
 *  ({@link defaultDatasourceIds}) when the selection is untouched (null) or
 *  stale (made against a different datasource set). */
export function activeDatasourceIds(
  datasources: PickerRow[],
  selection: DatasourceSelection,
  defaultIds?: Set<string> | null,
  serverDefaultsEnabled = false,
): Set<string> {
  const defaults = defaultDatasourceIds(datasources, defaultIds, serverDefaultsEnabled);
  if (!selection) return defaults;

  // Old exact-set selections remain supported. New selections carry `touched`
  // and reconcile per id so adding/removing one eligible connector does not
  // wipe every deliberate choice.
  if (!selection.touched) {
    return selection.key === datasourceSetKey(datasources)
      ? new Set(datasources.filter(d => selection.ids.has(d.id)).map(d => d.id))
      : defaults;
  }
  const active = new Set(defaults);
  for (const ds of datasources) {
    if (!selection.touched.has(ds.id)) continue;
    if (selection.ids.has(ds.id)) active.add(ds.id);
    else active.delete(ds.id);
  }
  return active;
}

/** Number of selected/unselected choices that differ from the current
 * server-computed default. This is what the Settings badge should count. */
export function datasourceSelectionDifferenceCount(
  datasources: PickerRow[],
  selection: DatasourceSelection,
  defaultIds?: Set<string> | null,
  serverDefaultsEnabled = false,
): number {
  const active = activeDatasourceIds(
    datasources, selection, defaultIds, serverDefaultsEnabled,
  );
  const defaults = defaultDatasourceIds(datasources, defaultIds, serverDefaultsEnabled);
  return datasources.filter(d => active.has(d.id) !== defaults.has(d.id)).length;
}

/** Selected datasource IDs to submit: the active set, minus clone-based
 *  repository sources disabled by a lite backend and stale ids. Centrally
 *  indexed `kb` datasources remain available on every tier. */
export function selectedDatasourceIds(
  datasources: Datasource[],
  selection: DatasourceSelection,
  isLiteBackend: boolean,
  defaultIds?: Set<string> | null,
  serverDefaultsEnabled = false,
  drivers?: readonly ConnectorDriver[] | null,
): string[] {
  const active = activeDatasourceIds(
    datasources, selection, defaultIds, serverDefaultsEnabled,
  );
  return datasources
    .filter(d => active.has(d.id) && !(isLiteBackend && requiresShellWorkspace(d.type, drivers)))
    .map(d => d.id);
}

/** True when every selectable datasource (i.e. not lite-excluded and not
 *  locked) is selected. */
export function allDatasourcesSelected(
  datasources: Datasource[],
  selection: DatasourceSelection,
  isLiteBackend: boolean,
  defaultIds?: Set<string> | null,
  lockedIds?: string[],
  serverDefaultsEnabled = false,
  drivers?: readonly ConnectorDriver[] | null,
): boolean {
  const locked = new Set(lockedIds ?? []);
  const selectable = datasources.filter(
    d => !(isLiteBackend && requiresShellWorkspace(d.type, drivers)) && !locked.has(d.id),
  );
  if (selectable.length === 0) return false;
  const active = activeDatasourceIds(
    datasources, selection, defaultIds, serverDefaultsEnabled,
  );
  return selectable.every(d => active.has(d.id));
}

/**
 * Datasource checkbox list. Hidden entirely when no datasources are available.
 */
@Component({
  selector: 'app-datasources-group',
  standalone: true,
  imports: [TranslocoPipe, AppIconComponent, AppSpinnerComponent],
  template: `
    @if (!loading() && datasources().length > 0) {
      <div class="settings-group">
        @if (showHeader()) {
          <div class="group-header">
            <span class="group-label">{{ 'agentSettings.datasources.group' | transloco }}</span>
            <button
              type="button"
              class="select-all-btn"
              (click)="toggleAll()"
              [disabled]="disabled() || error() || selectableDatasources().length === 0"
            >{{ (allSelected() ? 'agentSettings.common.deselectAll' : 'agentSettings.common.selectAll') | transloco }}</button>
          </div>
        }
        @if (searchable()) {
          <div class="ds-toolbar">
            <div class="ds-search">
              <app-icon size="sm" class="ds-search-icon">search</app-icon>
              <input
                type="search"
                class="ds-search-input"
                [value]="query()"
                (input)="query.set($any($event.target).value)"
                [placeholder]="'agentSettings.datasources.searchPlaceholder' | transloco"
                [attr.aria-label]="'agentSettings.datasources.searchLabel' | transloco"
              >
            </div>
            <div class="ds-filter" role="group" [attr.aria-label]="'agentSettings.datasources.filterLabel' | transloco">
              <button type="button" class="ds-filter-btn" [attr.aria-pressed]="!onlyAttached()" (click)="onlyAttached.set(false)">
                {{ 'agentSettings.datasources.filterAll' | transloco }}
              </button>
              <button type="button" class="ds-filter-btn" [attr.aria-pressed]="onlyAttached()" (click)="onlyAttached.set(true)">
                {{ 'agentSettings.datasources.filterAttached' | transloco:{ n: selectedList().length } }}
              </button>
            </div>
          </div>
          <div class="ds-count-row">
            <span class="ds-count">{{ 'agentSettings.datasources.countLine' | transloco:{ n: selectedList().length, total: datasources().length } }}</span>
            <button
              type="button"
              class="select-all-btn"
              (click)="toggleAll()"
              [disabled]="disabled() || error() || selectableDatasources().length === 0"
            >{{ (allSelected() ? 'agentSettings.common.deselectAll' : 'agentSettings.common.selectAll') | transloco }}</button>
          </div>
        }
        <div class="ds-picker">
          @for (ds of visibleDatasources(); track ds.id) {
            <label
              class="ds-option"
              [class.selected]="isChecked(ds)"
              [class.ds-disabled]="isLiteExcluded(ds) || isLocked(ds)"
            >
              <input
                type="checkbox"
                [checked]="isChecked(ds)"
                (change)="toggle(ds.id)"
                [disabled]="disabled() || error() || isLiteExcluded(ds) || isLocked(ds)"
              >
              <app-icon size="md" class="ds-type-icon" [class]="'ds-type-' + ds.type">{{ getTypeIcon(ds.type) }}</app-icon>
              <span class="ds-info">
                <span class="ds-name">
                  @if (ds.unavailable) {
                    {{ 'agentSettings.datasources.unavailableName' | transloco }}
                  } @else {
                    {{ ds.name }}
                  }
                </span>
                @if (isLiteExcluded(ds)) {
                  <span class="ds-desc">Requires a sandbox or VM workspace</span>
                } @else if (ds.unavailable && ds.type === 'credentials') {
                  <span class="ds-desc">{{ 'agentSettings.datasources.credentialsUnavailable' | transloco }}</span>
                } @else if (ds.unavailable) {
                  <span class="ds-desc">{{ 'agentSettings.datasources.unavailableLive' | transloco }}</span>
                } @else if (isLocked(ds)) {
                  <span class="ds-desc" data-locked-hint>{{ (ds.type === 'credentials'
                    ? 'agentSettings.datasources.credentialsLocked'
                    : ds.type === 'kb'
                      ? 'agentSettings.datasources.lockedLive'
                      : 'agentSettings.datasources.lockedNextStart') | transloco }}</span>
                } @else if (ds.description) {
                  <span class="ds-desc">{{ ds.description }}</span>
                }
                @if (ds.driver_env_names?.length) {
                  <span class="ds-desc" data-driver-env>
                    {{ 'datasources.driverEnvNames' | transloco }}
                    <code>{{ ds.driver_env_names!.join(', ') }}</code>
                  </span>
                }
                @if (besideProtected(ds)) {
                  <span class="ds-desc ds-beside-protected" data-beside-protected>
                    {{ 'agentSettings.datasources.access.besideProtected' | transloco }}
                  </span>
                }
                @if (isNotReady(ds)) {
                  <span class="ds-indexing">
                    {{ 'agentSettings.datasources.notReady' | transloco }}
                  </span>
                }
              </span>
              <span class="ds-type-badge" [class.ds-rw-badge]="ds.unavailable">
                {{ (ds.unavailable ? 'agentSettings.datasources.unavailableBadge' : typeLabelKey(ds)) | transloco }}
              </span>
              @if (ds.is_global) {
                <span class="ds-type-badge" [class.ds-rw-badge]="isPublicReadWrite(ds)">
                  {{ (isPublicReadWrite(ds)
                    ? 'datasources.table.badgeRw'
                    : 'datasources.table.badgeRo') | transloco }}
                </span>
              } @else if (isPrivateReadOnly(ds)) {
                <span class="ds-type-badge" data-creator-read-only>
                  {{ 'datasources.table.badgeRo' | transloco }}
                </span>
              }
              <!-- The level the connector binds at, for one whose driver has
                   strict levels (a cloud folder: read-only / protected /
                   read-write). New Session chooses it on a selected row,
                   among the levels offered here; elsewhere it is shown. -->
              @if (accessChoiceOf(ds); as access) {
                @if (accessMode() !== 'show' && isChecked(ds) && hasLevelChoice(access)) {
                  <select
                    class="ds-access-select"
                    data-access-select
                    [attr.data-level]="accessLevel(ds)"
                    [disabled]="accessMode() === 'locked' || disabled() || error() || isLocked(ds)"
                    [attr.aria-label]="'agentSettings.datasources.access.label' | transloco:{ name: ds.name }"
                    (change)="chooseAccess(ds.id, $any($event.target).value)"
                  >
                    @if (accessLevel(ds) === null) {
                      <option value="" selected disabled>{{ 'agentSettings.datasources.access.notMounted' | transloco }}</option>
                    }
                    @for (level of access.levels; track level.id) {
                      <option
                        [value]="level.id"
                        [selected]="level.id === accessLevel(ds)"
                        [disabled]="levelDisabled(ds, level)"
                        [attr.title]="level.detail || null"
                      >
                        @if (accessLevelKey(level.id); as key) {
                          {{ key | transloco }}
                        } @else {
                          {{ level.id }}
                        }
                        @if (level.id === access.default) {
                          · {{ accessOriginKey(access) | transloco }}
                        }
                        @if (levelRefusalKey(ds, level); as refusal) {
                          · {{ refusal | transloco }}
                        }
                      </option>
                    }
                  </select>
                } @else {
                  <span
                    class="ds-type-badge"
                    [class.ds-rw-badge]="accessLevelWritesDirect(ds)"
                    data-access-level
                    [attr.data-level]="accessLevel(ds)"
                    [attr.title]="isUserRoot(ds)
                      ? ('agentSettings.datasources.access.fullAccessHint' | transloco)
                      : accessLevel(ds) === access.default ? (accessOriginKey(access) | transloco) : null"
                  >
                    @if (isUserRoot(ds) && accessLevel(ds)) {
                      <!-- My cloud has one level: an app token can be limited
                           neither to read-only nor to one folder. -->
                      {{ 'agentSettings.datasources.access.fullAccess' | transloco }}
                    } @else if (accessLevel(ds); as level) {
                      @if (accessLevelKey(level); as key) {
                        {{ key | transloco }}
                      } @else {
                        {{ level }}
                      }
                    } @else {
                      {{ 'agentSettings.datasources.access.notMounted' | transloco }}
                    }
                  </span>
                }
              }
            </label>
          }
          @if (searchable() && visibleDatasources().length === 0) {
            <div class="ds-empty">
              {{ (query().trim() ? 'agentSettings.datasources.noMatch' : 'agentSettings.datasources.noneAttached') | transloco:{ query: query().trim() } }}
            </div>
          }
        </div>
        @if (nothingAttached()) {
          <!-- An empty list is sent as such: no connector at all, the
               project's own included. Say so where the choice is made. -->
          <p class="ds-none-note" data-nothing-attached>
            <app-icon size="sm">info</app-icon>
            <span>{{ 'agentSettings.datasources.nothingAttached' | transloco }}</span>
          </p>
        }
        @if (error()) {
          <div class="ds-error" role="alert">
            <span>{{ 'agentSettings.datasources.loadFailed' | transloco }}</span>
            <button type="button" class="select-all-btn" (click)="retry.emit()">
              {{ 'agentSettings.datasources.retry' | transloco }}
            </button>
          </div>
        }
      </div>
    } @else if (loading()) {
      <div class="settings-group">
        @if (showHeader()) {
          <div class="group-header">
            <span class="group-label">{{ 'agentSettings.datasources.group' | transloco }}</span>
          </div>
        }
        <div class="ds-loading">
          <app-spinner size="sm" />

          {{ 'agentSettings.datasources.loading' | transloco }}
        </div>
      </div>
    } @else if (error()) {
      <div class="settings-group">
        @if (showHeader()) {
          <div class="group-header">
            <span class="group-label">{{ 'agentSettings.datasources.group' | transloco }}</span>
          </div>
        }
        <div class="ds-error" role="alert">
          <span>{{ 'agentSettings.datasources.loadFailed' | transloco }}</span>
          <button type="button" class="select-all-btn" (click)="retry.emit()">
            {{ 'agentSettings.datasources.retry' | transloco }}
          </button>
        </div>
      </div>
    } @else if (searchable()) {
      <p class="ds-empty">{{ 'agentSettings.datasources.noneAvailable' | transloco }}</p>
    }
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
      color: var(--text-muted, var(--text-muted));
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
    .ds-picker {
      display: flex;
      flex-direction: column;
      gap: 4px;
    }
    .ds-option {
      display: flex;
      align-items: center;
      gap: 10px;
      padding: 8px 10px;
      border-radius: var(--radius-control);
      cursor: pointer;
      transition: background 0.15s;
    }
    .ds-option:hover {
      background: rgba(255, 255, 255, 0.03);
    }
    .ds-option.selected {
      background: color-mix(in srgb, var(--accent-color) 20%, transparent);
    }
    .ds-option.ds-disabled {
      opacity: 0.5;
      cursor: not-allowed;
    }
    .ds-option input[type="checkbox"] {
      accent-color: var(--accent-color, var(--accent-color));
      flex-shrink: 0;
    }
    .ds-type-icon {
      color: var(--text-muted, var(--text-muted));
      flex-shrink: 0;
    }
    .ds-type-postgresql { color: var(--info); }
    .ds-type-neo4j { color: var(--success); }
    .ds-type-mongodb { color: var(--alert); }
    .ds-type-webdav { color: var(--info); }
    .ds-type-kb { color: var(--accent-color); }
    .ds-type-cloud_folder,
    .ds-type-cloud_folder_checkout,
    .ds-type-cloud_outbox { color: var(--info); }
    .ds-info {
      display: flex;
      flex-direction: column;
      gap: 1px;
      flex: 1;
      min-width: 0;
    }
    .ds-name {
      font-size: 13px;
      font-weight: 500;
      color: var(--text-primary, var(--text-primary));
    }
    .ds-desc {
      font-size: 11px;
      color: var(--text-muted, var(--text-muted));
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    .ds-indexing {
      font-size: 11px;
      color: var(--warning, var(--text-muted));
      white-space: normal;
    }
    .ds-type-badge {
      font-size: 11px;
      font-weight: 500;
      text-transform: uppercase;
      letter-spacing: 0.06em;
      padding: 2px 6px;
      border-radius: var(--radius-tag);
      background: rgba(255, 255, 255, 0.06);
      color: var(--text-muted, var(--text-muted));
      flex-shrink: 0;
    }
    .ds-access-select {
      flex-shrink: 0;
      max-width: 16rem;
      min-height: 28px;
      padding: 2px 6px;
      border: 1px solid var(--border-color);
      border-radius: var(--radius-control);
      background: var(--surface-0);
      color: var(--text-primary);
      font: inherit;
      font-size: 12px;
    }
    .ds-access-select:focus-visible {
      outline: none;
      border-color: var(--accent-color);
      box-shadow: 0 0 0 3px var(--ring);
    }
    .ds-access-select:disabled {
      opacity: 0.7;
      cursor: not-allowed;
    }
    .ds-beside-protected {
      color: var(--warning, var(--text-muted));
      white-space: normal;
    }
    /* Public read-write chip — warning tone so attachers see write access. */
    .ds-rw-badge {
      color: var(--warning, var(--text-primary));
      background: var(--warning-tint, rgba(255, 255, 255, 0.06));
    }
    .ds-loading {
      display: flex;
      align-items: center;
      gap: 8px;
      font-size: 12px;
      color: var(--text-muted, var(--text-muted));
      padding: 8px 0;
    }
    .ds-toolbar {
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      gap: 8px;
      margin-bottom: 8px;
    }
    .ds-search {
      position: relative;
      flex: 1 1 14rem;
      min-width: 0;
    }
    .ds-search-icon {
      position: absolute;
      left: 10px;
      top: 50%;
      transform: translateY(-50%);
      color: var(--text-muted);
      pointer-events: none;
    }
    .ds-search-input {
      box-sizing: border-box;
      width: 100%;
      min-height: 36px;
      padding: 6px 10px 6px 32px;
      border: 1px solid var(--border-color);
      border-radius: var(--radius-control);
      background: var(--surface-0);
      color: var(--text-primary);
      font: inherit;
      font-size: 13px;
    }
    .ds-search-input:focus {
      outline: none;
      border-color: var(--accent-color);
      box-shadow: 0 0 0 3px var(--ring);
    }
    .ds-filter {
      display: inline-flex;
      border: 1px solid var(--border-color);
      border-radius: var(--radius-control);
      overflow: hidden;
    }
    .ds-filter-btn {
      min-height: 36px;
      padding: 0 12px;
      border: 0;
      background: var(--surface-0);
      color: var(--text-secondary);
      font: inherit;
      font-size: 12px;
      font-weight: 600;
      cursor: pointer;
    }
    .ds-filter-btn + .ds-filter-btn {
      border-left: 1px solid var(--border-color);
    }
    .ds-filter-btn[aria-pressed="true"] {
      background: var(--accent-color);
      color: var(--on-accent);
    }
    .ds-filter-btn:focus-visible {
      outline: none;
      box-shadow: inset 0 0 0 2px var(--ring);
    }
    .ds-count-row {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
      margin-bottom: 6px;
    }
    .ds-count {
      font-size: 12px;
      color: var(--text-muted);
    }
    .ds-empty {
      margin: 0;
      padding: 10px 2px;
      font-size: 13px;
      color: var(--text-muted);
    }
    .ds-none-note {
      display: flex;
      align-items: center;
      gap: 6px;
      margin: 8px 0 0;
      font-size: 12px;
      color: var(--text-secondary);
    }
    .ds-error {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      padding: 8px 10px;
      border-radius: var(--radius-control);
      background: var(--danger-tint);
      color: var(--danger);
      font-size: 12px;
    }
  `],
})
export class DatasourcesGroupComponent {
  // Optional so the picker still renders in bare unit tests without an injector.
  private readonly api = inject(ApiService, {optional: true});
  // The capability matrix, for the access badge of public and read-only
  // tagged connectors.
  private readonly connectorDrivers = inject(ConnectorDriversService, {optional: true});

  datasources = input<Datasource[]>([]);
  loading = input(false);
  error = input(false);
  disabled = input(false);
  /** Stable identity of the selected project set / execution context. */
  contextKey = input('standalone');
  /**
   * When a lite workspace backend (virtual/none) is selected, clone-based
   * repositories and credentials are unavailable. `kb` repositories are indexed by
   * the orchestrator, so they remain selectable.
   */
  isLiteBackend = input(false);
  /**
   * Default selection when the user hasn't touched the picker (live mode:
   * the session's currently attached set — possibly empty). Null keeps the
   * create-flow server `default_selected` set.
   */
  initialSelectedIds = input<string[] | null>(null);
  /** Rollout gate for create-flow `default_selected`. Explicit live-session
   * arrays remain authoritative regardless of this flag. */
  datasourceDefaultsEnabled = input(false);
  /**
   * Entries rendered but frozen at their current state (live mode: kb-type
   * datasources — their knowledge bindings only rewire on attach, so live
   * changes are out of scope; live_session_settings.md Slice B).
   */
  lockedIds = input<string[]>([]);
  /** Show the uppercase group label. Off inside the three-section host, whose
   *  Connectors section already titles it. */
  showHeader = input(true);
  /** Show the search box and the All / Attached filter — a production system
   *  accumulates far more connectors than a flat checkbox list can carry. */
  searchable = input(false);
  /** How a connector with access levels shows its level
   *  ({@link DatasourceAccessMode}). */
  accessMode = input<DatasourceAccessMode>('show');
  /** Live mode: the level each attached connector was bound at
   *  (`metadata.datasource_access`), shown instead of a new default. */
  initialAccess = input<Record<string, string> | null>(null);

  /** Search text and the Attached-only filter. Presentation only: a filtered
   *  row keeps its selection, and nothing here reaches `getSelectedIds`. */
  readonly query = signal('');
  readonly onlyAttached = signal(false);

  change = output<void>();
  retry = output<void>();

  // The user's explicit selections are kept per execution context. Untouched
  // rows follow the server-computed default while deliberate choices survive
  // eligibility refreshes within that context.
  private readonly selections = signal<Record<string, NonNullable<DatasourceSelection>>>({});
  /** The levels the user chose, per execution context and connector id. */
  private readonly accessChoices = signal<Record<string, Record<string, string>>>({});
  private readonly accessChoice = computed<Record<string, string>>(
    () => this.accessChoices()[this.contextKey()] ?? {},
  );
  readonly accessChoiceOf = accessChoiceOf;
  readonly accessOriginKey = accessOriginKey;
  private readonly selection = computed<DatasourceSelection>(() =>
    this.selections()[this.contextKey()] ?? null,
  );

  /** Central index status per KB datasource id (for the still-indexing warning). */
  readonly indexStatuses = signal<Record<string, DatasourceIndexStatus>>({});

  constructor() {
    // Fetch central index status for KB rows so the picker can warn when a
    // selected knowledge base isn't fully indexed yet. Re-runs when the eligible
    // list changes (e.g. project switch); the signal write lands in the async
    // HTTP callback, never synchronously inside the effect.
    effect(() => {
      for (const ds of this.datasources()) {
        if (ds.type === 'kb') this.loadIndexStatus(ds.id);
      }
    });
    // The matrix decides a public or read-only-tagged row's badge and, on a
    // lite backend, which rows need a shell; read it once, when either shows.
    effect(() => {
      const rows = this.datasources();
      if (
        rows.some((ds) => ds.is_global || ds.read_only === true) ||
        (this.isLiteBackend() && rows.length > 0)
      ) {
        this.connectorDrivers?.load();
      }
    });
  }

  /** The installed drivers, or null until the matrix loads. */
  private drivers(): readonly ConnectorDriver[] | null {
    return this.connectorDrivers?.drivers() ?? null;
  }

  /** A public row's badge: its declared flag, unless its driver offers one
   *  level only (an MCP server binds every tool it lists). */
  isPublicReadWrite(ds: Datasource): boolean {
    return publicReadWrite(ds, this.connectorDrivers?.forType(ds.type));
  }

  /** A private row its creator tagged read-only: every user's agents get
   *  its read tools only (decision 31). Not for a driver with one
   *  read-write level, which binds every tool whatever the flag says. */
  isPrivateReadOnly(ds: Datasource): boolean {
    return !ds.is_global && ds.read_only === true && !this.isPublicReadWrite(ds);
  }

  readonly modifiedCount = computed(() =>
    datasourceSelectionDifferenceCount(
      this.datasources(), this.selection(), this.defaultIds(),
      this.datasourceDefaultsEnabled(),
    ) + Object.keys(this.getSelectedDatasourceAccessFor(this.selectedList())).length,
  );

  private defaultIds(): Set<string> | null {
    const init = this.initialSelectedIds();
    return init === null ? null : new Set(init);
  }

  /** The connectors that would be attached, in list order — what the
   *  section's one-line summary names. Same rule as `getSelectedIds`. */
  readonly selectedList = computed<Datasource[]>(() => {
    const ids = new Set(selectedDatasourceIds(
      this.datasources(), this.selection(), this.isLiteBackend(), this.defaultIds(),
      this.datasourceDefaultsEnabled(), this.drivers(),
    ));
    return this.datasources().filter(d => ids.has(d.id));
  });

  /** Nothing would be attached although the project's own connectors were
   *  on offer: the explicit empty list a create sends, said in words. */
  readonly nothingAttached = computed(
    () =>
      !this.loading() &&
      !this.error() &&
      this.selectedList().length === 0 &&
      this.datasources().some((ds) => isPlatformOwned(ds) && !this.isLiteExcluded(ds)),
  );

  /** Rows after the search text and the Attached-only filter. */
  readonly visibleDatasources = computed<Datasource[]>(() => {
    const needle = this.query().trim().toLowerCase();
    const attached = this.onlyAttached() ? new Set(this.selectedList().map(d => d.id)) : null;
    return this.datasources().filter(ds => {
      if (attached && !attached.has(ds.id)) return false;
      if (!needle) return true;
      return [ds.name, ds.type, ds.description ?? '']
        .some(text => (text || '').toString().toLowerCase().includes(needle));
    });
  });

  /** Datasources the user can actually toggle (not lite-excluded, not locked). */
  readonly selectableDatasources = computed<Datasource[]>(() =>
    this.datasources().filter(ds => !this.isLiteExcluded(ds) && !this.isLocked(ds))
  );

  /** True when every selectable datasource is currently checked. */
  readonly allSelected = computed(() =>
    allDatasourcesSelected(
      this.datasources(),
      this.selection(),
      this.isLiteBackend(),
      this.defaultIds(),
      this.datasources().filter(ds => this.isLocked(ds)).map(ds => ds.id),
      this.datasourceDefaultsEnabled(),
      this.drivers(),
    )
  );

  /** A driver that needs a shell is held back on a lite workspace. */
  isLiteExcluded(ds: Datasource): boolean {
    return this.isLiteBackend() && requiresShellWorkspace(ds.type, this.drivers());
  }

  /** Frozen at its current state — rendered, never toggleable. */
  isLocked(ds: Datasource): boolean {
    return this.lockedIds().includes(ds.id) ||
      (ds.type === 'credentials' && (this.initialSelectedIds()?.includes(ds.id) ?? false));
  }

  isChecked(ds: Datasource): boolean {
    return (
      !this.isLiteExcluded(ds) &&
      activeDatasourceIds(
        this.datasources(), this.selection(), this.defaultIds(),
        this.datasourceDefaultsEnabled(),
      ).has(ds.id)
    );
  }

  toggle(id: string): void {
    const ds = this.datasources().find(item => item.id === id);
    if (!ds || this.isLocked(ds) || this.isLiteExcluded(ds)) return;
    const next = new Set(
      activeDatasourceIds(
        this.datasources(), this.selection(), this.defaultIds(),
        this.datasourceDefaultsEnabled(),
      ),
    );
    if (next.has(id)) {
      next.delete(id);
    } else {
      next.add(id);
    }
    const touched = new Set(this.selection()?.touched ?? []);
    touched.add(id);
    this.setCurrentSelection({
      key: datasourceSetKey(this.datasources()),
      ids: next,
      touched,
    });
    this.change.emit();
  }

  /** Check every selectable datasource, or clear them all if already all on.
   *  Locked entries keep their current state either way. */
  toggleAll(): void {
    const selectAll = !this.allSelected();
    const active = activeDatasourceIds(
      this.datasources(), this.selection(), this.defaultIds(),
      this.datasourceDefaultsEnabled(),
    );
    const lockedKept = this.datasources()
      .filter(d => this.isLocked(d) && active.has(d.id))
      .map(d => d.id);
    const ids = selectAll
      ? new Set([...this.selectableDatasources().map(d => d.id), ...lockedKept])
      : new Set(lockedKept);
    const touched = new Set(
      this.selectableDatasources().map(d => d.id),
    );
    this.setCurrentSelection({key: datasourceSetKey(this.datasources()), ids, touched});
    this.change.emit();
  }

  getTypeIcon(type: DatasourceType | string): string {
    const icons: Record<string, string> = {
      credentials: 'key',
      postgresql: 'database',
      neo4j: 'hub',
      mongodb: 'eco',
      webdav: 'cloud',
      email: 'mail',
      kb: 'menu_book',
      cloud_folder: 'folder',
      cloud_folder_checkout: 'folder_copy',
      cloud_outbox: 'outbox',
    };
    return icons[type] || 'storage';
  }

  /** The type badge: what a platform-owned row is to its user ("Project
   *  folder", "Knowledge base"), else the connector type. */
  typeLabelKey(ds: Datasource): string {
    return platformKindLabelKey(ds) ?? 'datasources.filter.' + ds.type;
  }

  /** The level a row binds at: the user's choice, else (live) the level the
   *  session recorded, else the eligible list's default; null for a row
   *  without levels, or one no level is allowed for here. */
  accessLevel(ds: Datasource): string | null {
    const access = accessChoiceOf(ds);
    if (!access) return null;
    const known = (level: string | undefined | null) =>
      level && access.levels.some((l) => l.id === level) ? level : null;
    // A choice the list no longer offers (the tier moved) is dropped, never
    // shown as if it would be sent.
    const chosen = this.accessChoice()[ds.id];
    const offered = chosen && access.levels.some((l) => l.id === chosen && l.offered) ? chosen : null;
    return offered ?? known(this.initialAccess()?.[ds.id]) ?? known(access.default);
  }

  accessLevelKey(level: string): string | null {
    return accessLevelLabelKey(level);
  }

  /** Whether the row's level writes straight through to the upstream: the
   *  badge then takes the read-write tone. */
  accessLevelWritesDirect(ds: Datasource): boolean {
    const access = accessChoiceOf(ds);
    const level = this.accessLevel(ds);
    if (!access || level === null) return false;
    const writes = access.levels.find((l) => l.id === level)?.writes;
    return writes ? writes === 'direct' : level === 'read_write';
  }

  /** A select only where there is a choice: one offered level is shown. */
  hasLevelChoice(access: ConnectorAccessChoice): boolean {
    return access.levels.filter((level) => level.offered).length > 1;
  }

  /** The user's own cloud root ("My cloud"): full access, its only level. */
  isUserRoot(ds: Datasource): boolean {
    return platformOwnedKind(ds) === USER_CLOUD_ROOT;
  }

  /** The selected row bound at `protected`, if any: a session protects one
   *  folder at most, and mounts nothing beside it. */
  readonly protectedRowId = computed<string | null>(() => {
    if (this.accessMode() === 'show') return null;
    return this.selectedList().find((ds) => this.accessLevel(ds) === 'protected')?.id ?? null;
  });

  /** A selected cloud folder that stays out because another is protected. */
  besideProtected(ds: Datasource): boolean {
    const protectedId = this.protectedRowId();
    return protectedId !== null && protectedId !== ds.id && CLOUD_MOUNT_TYPES.has(ds.type) &&
      this.isChecked(ds);
  }

  /** A level the select offers but the user may not take: not offered here,
   *  or `protected` while another folder already is. */
  levelDisabled(ds: Datasource, level: ConnectorAccessChoiceLevel): boolean {
    return this.levelRefusalKey(ds, level) !== null;
  }

  levelRefusalKey(ds: Datasource, level: ConnectorAccessChoiceLevel): string | null {
    const refused = accessRefusalKey(level);
    if (refused) return refused;
    const protectedId = this.protectedRowId();
    if (level.id === 'protected' && protectedId !== null && protectedId !== ds.id) {
      return 'agentSettings.datasources.access.refused.oneProtected';
    }
    return null;
  }

  chooseAccess(id: string, level: string): void {
    const ds = this.datasources().find((item) => item.id === id);
    const offered = ds ? accessChoiceOf(ds)?.levels.find((l) => l.id === level) : null;
    if (!ds || !offered || this.levelDisabled(ds, offered) || this.isLocked(ds)) return;
    const contextKey = this.contextKey();
    this.accessChoices.update((all) => ({
      ...all,
      [contextKey]: {...(all[contextKey] ?? {}), [id]: level},
    }));
    this.change.emit();
  }

  /** The levels to send, by connector id: the ones the user chose for a
   *  selected row (`datasource_access`). An unchosen row is left out, so
   *  the server binds it at its default, lowered where needed. */
  getSelectedDatasourceAccess(): Record<string, string> {
    if (this.accessMode() !== 'choose') return {};
    const selected = new Set(this.getSelectedIds());
    return this.getSelectedDatasourceAccessFor(this.datasources().filter((ds) => selected.has(ds.id)));
  }

  private getSelectedDatasourceAccessFor(rows: Datasource[]): Record<string, string> {
    const chosen = this.accessChoice();
    const out: Record<string, string> = {};
    for (const ds of rows) {
      const level = chosen[ds.id];
      const access = accessChoiceOf(ds);
      if (level && access?.levels.some((l) => l.id === level && l.offered) && level !== access.default) {
        out[ds.id] = level;
      }
    }
    return out;
  }

  /**
   * Selected datasource IDs, excluding clone-based repository sources disabled
   * by a lite backend and any ids not in the current datasource set.
   */
  getSelectedIds(): string[] {
    return selectedDatasourceIds(
      this.datasources(),
      this.selection(),
      this.isLiteBackend(),
      this.defaultIds(),
      this.datasourceDefaultsEnabled(),
      this.drivers(),
    );
  }

  private loadIndexStatus(id: string): void {
    this.api?.getDatasourceIndexStatus(id).subscribe((status) => {
      if (!status) return;
      this.indexStatuses.update((m) => ({...m, [id]: status}));
    });
  }

  /** True when a KB datasource is in the index but not fully `ready` yet. */
  isNotReady(ds: Datasource): boolean {
    if (ds.type !== 'kb') return false;
    const status = this.indexStatuses()[ds.id]?.status;
    return !!status && status !== 'ready';
  }

  resetAll(): void {
    // A parent form reset can change the context key in the same turn. Clear
    // every cached context so returning to the default project cannot revive
    // an earlier touched selection.
    this.selections.set({});
    this.accessChoices.set({});
    this.change.emit();
  }

  private setCurrentSelection(selection: NonNullable<DatasourceSelection>): void {
    const contextKey = this.contextKey();
    this.selections.update(all => ({...all, [contextKey]: selection}));
  }
}
