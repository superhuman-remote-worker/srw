import {Component, computed, effect, inject, input, output, signal} from '@angular/core';
import {TranslocoPipe} from '@jsverse/transloco';
import {AppIconComponent} from '../../ui/icon';
import {AppSpinnerComponent} from '../../ui/spinner';
import {ApiService} from '../../core/services/api.service';
import {ConnectorDriversService} from '../../core/services/connector-drivers.service';
import {publicReadWrite} from '../../core/models/connector-driver.model';
import {
  Datasource,
  DatasourceIndexStatus,
  DatasourceType,
} from '../../core/models/api.model';

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

/** Connector types whose driver needs a shell workspace (its spec leaves the
 *  lite tiers out of `supported_backends`). A copy of the server's
 *  `workspace_tier_refuses` until the picker reads the driver specs (D2). */
const SHELL_WORKSPACE_TYPES = new Set(['repository', 'credentials', 'generic', 'ssh_key']);

export function requiresShellWorkspace(type: DatasourceType | string): boolean {
  return SHELL_WORKSPACE_TYPES.has((type || '').toString().toLowerCase());
}

/** Stable identity of a datasource set (order-independent). */
export function datasourceSetKey(datasources: {id: string}[]): string {
  return datasources
    .map(d => d.id)
    .sort()
    .join(',');
}

/** Active selection: the user's tagged choice, or the default when the
 *  selection is untouched (null) or stale (made against a different datasource
 *  set). The default is the server-computed `default_selected` set — except
 *  when `defaultIds` is provided (live mode: the session's currently attached
 *  set, possibly empty). */
export function activeDatasourceIds(
  datasources: Array<{id: string; default_selected?: boolean}>,
  selection: DatasourceSelection,
  defaultIds?: Set<string> | null,
  serverDefaultsEnabled = false,
): Set<string> {
  const defaults = defaultIds !== undefined && defaultIds !== null
    ? new Set(datasources.filter(d => defaultIds.has(d.id)).map(d => d.id))
    : serverDefaultsEnabled
      ? new Set(datasources.filter(d => d.default_selected === true).map(d => d.id))
      : new Set<string>();
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
  datasources: Array<{id: string; default_selected?: boolean}>,
  selection: DatasourceSelection,
  defaultIds?: Set<string> | null,
  serverDefaultsEnabled = false,
): number {
  const active = activeDatasourceIds(
    datasources, selection, defaultIds, serverDefaultsEnabled,
  );
  const defaults = defaultIds !== undefined && defaultIds !== null
    ? new Set(datasources.filter(d => defaultIds.has(d.id)).map(d => d.id))
    : serverDefaultsEnabled
      ? new Set(datasources.filter(d => d.default_selected === true).map(d => d.id))
      : new Set<string>();
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
): string[] {
  const active = activeDatasourceIds(
    datasources, selection, defaultIds, serverDefaultsEnabled,
  );
  return datasources
    .filter(d => active.has(d.id) && !(isLiteBackend && requiresShellWorkspace(d.type)))
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
): boolean {
  const locked = new Set(lockedIds ?? []);
  const selectable = datasources.filter(
    d => !(isLiteBackend && requiresShellWorkspace(d.type)) && !locked.has(d.id),
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
                  <span class="ds-desc">{{ (ds.type === 'credentials' ? 'agentSettings.datasources.credentialsLocked' : 'agentSettings.datasources.lockedLive') | transloco }}</span>
                } @else if (ds.description) {
                  <span class="ds-desc">{{ ds.description }}</span>
                }
                @if (isNotReady(ds)) {
                  <span class="ds-indexing">
                    {{ 'agentSettings.datasources.notReady' | transloco }}
                  </span>
                }
              </span>
              <span class="ds-type-badge" [class.ds-rw-badge]="ds.unavailable">
                {{ (ds.unavailable ? 'agentSettings.datasources.unavailableBadge' : 'datasources.filter.' + ds.type) | transloco }}
              </span>
              @if (ds.is_global) {
                <span class="ds-type-badge" [class.ds-rw-badge]="isPublicReadWrite(ds)">
                  {{ (isPublicReadWrite(ds)
                    ? 'datasources.table.badgeRw'
                    : 'datasources.table.badgeRo') | transloco }}
                </span>
              }
            </label>
          }
          @if (searchable() && visibleDatasources().length === 0) {
            <div class="ds-empty">
              {{ (query().trim() ? 'agentSettings.datasources.noMatch' : 'agentSettings.datasources.noneAttached') | transloco:{ query: query().trim() } }}
            </div>
          }
        </div>
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
  // The capability matrix, for the access badge of public connectors only.
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
    // Only a public row's badge needs the matrix; read it once, when one shows.
    effect(() => {
      if (this.datasources().some((ds) => ds.is_global)) this.connectorDrivers?.load();
    });
  }

  /** A public row's badge: its declared flag, unless its driver offers one
   *  level only (an MCP server binds every tool it lists). */
  isPublicReadWrite(ds: Datasource): boolean {
    return publicReadWrite(ds, this.connectorDrivers?.forType(ds.type));
  }

  readonly modifiedCount = computed(() =>
    datasourceSelectionDifferenceCount(
      this.datasources(), this.selection(), this.defaultIds(),
      this.datasourceDefaultsEnabled(),
    ),
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
      this.datasourceDefaultsEnabled(),
    ));
    return this.datasources().filter(d => ids.has(d.id));
  });

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
    )
  );

  /** Repositories and credentials require a shell-capable workspace. */
  isLiteExcluded(ds: Datasource): boolean {
    return this.isLiteBackend() && requiresShellWorkspace(ds.type);
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
    };
    return icons[type] || 'storage';
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
    this.change.emit();
  }

  private setCurrentSelection(selection: NonNullable<DatasourceSelection>): void {
    const contextKey = this.contextKey();
    this.selections.update(all => ({...all, [contextKey]: selection}));
  }
}
