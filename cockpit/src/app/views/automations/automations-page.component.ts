import {
  ChangeDetectionStrategy,
  Component,
  OnInit,
  computed,
  inject,
  signal,
  viewChild,
} from '@angular/core';
import {ActivatedRoute, Router, RouterLink} from '@angular/router';
import {DatePipe} from '@angular/common';
import {FormsModule} from '@angular/forms';
import {TranslocoPipe, TranslocoService} from '@jsverse/transloco';
import {catchError, of} from 'rxjs';

import {AppButtonComponent} from '../../ui/button';
import {AppIconComponent} from '../../ui/icon';
import {AppInputComponent} from '../../ui/input';
import {AppSelectComponent} from '../../ui/select';
import {AppSwitchComponent} from '../../ui/switch';
import {AppTextareaComponent} from '../../ui/textarea';
import {AppBadgeComponent} from '../../ui/badge';
import {AppSpinnerComponent} from '../../ui/spinner';
import {SidebarToggleComponent} from '../../shell/sidebar-toggle/sidebar-toggle.component';

interface SelectOption {
  value: string;
  label: string;
}

import {ApiService} from '../../core/services/api.service';
import {UserService} from '../../core/services/user.service';
import {
  Automation,
  AutomationsService,
  CreateAutomationRequest,
} from '../../core/services/automations.service';
import {WorkspaceChoice, WorkspaceBinding} from '../../core/models/workspace-template.model';
import {WorkspacePickerComponent} from '../agent-settings/workspace-picker.component';
import {DatasourcesGroupComponent} from '../agent-settings/datasources-group.component';
import {choiceRequestFields} from '../workspaces/workspace-template-utils';
import {Expert, Project, EligibleDatasource} from '../../core/models/api.model';
import {CronPreviewComponent} from './cron-preview.component';
import {EscapeHatchPanelComponent} from './escape-hatch-panel.component';

type CronPreset =
  | 'every_day'
  | 'every_weekday'
  | 'every_monday'
  | 'every_month_first'
  | 'every_hour'
  | 'custom';

interface EditorState {
  mode: 'create' | 'edit';
  triggerType: 'cron' | 'live';
  connectorIds: string[];
  connectorAliases: Record<string, string>;
  primaryConnectorId: string;
  workspaceChoice: WorkspaceChoice;
  id: string | null;
  name: string;
  description: string;
  preset: CronPreset;
  time: string; // HH:MM (24h) for non-custom presets
  customCron: string;
  timezone: string;
  expert: string;
  prompt: string;
  enabled: boolean;
  autonomy: Automation['autonomy'];
  priority: number;
  maxFiresPerDay: number;
  catchupWindowHours: number;
  projectId: string | null;
  showAdvanced: boolean;
}

const SOFT_CAP = 20;

@Component({
  selector: 'app-automations-page',
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [
    DatePipe,
    FormsModule,
    RouterLink,
    TranslocoPipe,
    AppButtonComponent,
    AppIconComponent,
    AppInputComponent,
    AppSelectComponent,
    AppSwitchComponent,
    AppTextareaComponent,
    AppBadgeComponent,
    AppSpinnerComponent,
    SidebarToggleComponent,
    CronPreviewComponent,
    EscapeHatchPanelComponent,
    WorkspacePickerComponent,
    DatasourcesGroupComponent,
  ],
  templateUrl: './automations-page.component.html',
  styleUrl: './automations-page.component.scss',
})
export class AutomationsPageComponent implements OnInit {
  private readonly service = inject(AutomationsService);
  private readonly api = inject(ApiService);
  private readonly route = inject(ActivatedRoute);
  private readonly router = inject(Router);
  private readonly transloco = inject(TranslocoService);
  private readonly users = inject(UserService);

  readonly automations = this.service.automations;
  readonly isLoading = this.service.isLoading;
  readonly experts = signal<Expert[]>([]);
  readonly errorMessage = signal<string | null>(null);
  readonly editor = signal<EditorState | null>(null);
  readonly connectors = signal<EligibleDatasource[]>([]);
  readonly connectorsLoading = signal(false);
  readonly connectorsError = signal(false);
  readonly connectorContext = signal('');
  private connectorRequest = 0;
  private editorGeneration = 0;
  readonly livePicker = viewChild<DatasourcesGroupComponent>('livePicker');
  readonly additionalPicker = viewChild<DatasourcesGroupComponent>('additionalPicker');
  readonly liveConnectors = computed(() => this.connectors().filter((c) => c.type === 'gpt_live_sip'));
  readonly additionalConnectors = computed(() => this.connectors().filter((c) => c.type !== 'gpt_live_sip'));
  readonly selectedLiveConnectors = computed(() => {
    const selected = new Set(this.editor()?.connectorIds ?? []);
    return this.liveConnectors().filter((c) => selected.has(c.id));
  });

  private loadConnectors(projectId: string | null): void {
    const request = ++this.connectorRequest;
    this.connectorsLoading.set(true);
    this.connectorsError.set(false);
    this.api.getEligibleDatasources(projectId ? [projectId] : [], 'job').subscribe({
      next: (rows) => {
        if (request !== this.connectorRequest) return;
        this.connectors.set(rows);
        this.connectorsLoading.set(false);
        this.dropIneligibleConnectors();
      },
      error: () => {
        if (request !== this.connectorRequest) return;
        this.connectors.set([]);
        this.connectorsLoading.set(false);
        this.connectorsError.set(true);
      },
    });
  }

  retryConnectors(): void {
    const current = this.editor();
    this.loadConnectors(current ? current.projectId : this.projectFilter());
  }

  /** A new automation's project can change under its selection: keep only
   *  the connectors the freshly loaded list still offers. An edit's project
   *  is fixed, so its stored bindings are left alone. */
  private dropIneligibleConnectors(): void {
    const current = this.editor();
    if (current?.mode !== 'create') return;
    const eligible = new Set(this.connectors().map((c) => c.id));
    const kept = current.connectorIds.filter((id) => eligible.has(id));
    if (kept.length !== current.connectorIds.length) this.setConnectorIds(kept);
  }

  onConnectorChange(): void {
    const current = this.editor();
    if (!current) return;
    const ids = [
      ...(this.livePicker()?.getSelectedIds() ?? current.connectorIds.filter((id) => this.liveConnectors().some((c) => c.id === id))),
      ...(this.additionalPicker()?.getSelectedIds() ?? current.connectorIds.filter((id) => this.additionalConnectors().some((c) => c.id === id))),
    ];
    this.setConnectorIds(ids);
  }

  setConnectorIds(ids: string[]): void {
    const current = this.editor();
    if (!current) return;
    const selected = this.liveConnectors().filter((c) => ids.includes(c.id));
    const primary = selected.some((c) => c.id === current.primaryConnectorId)
      ? current.primaryConnectorId : selected[0]?.id ?? '';
    this.editor.set({...current, connectorIds: [...new Set(ids)], primaryConnectorId: primary});
  }

  connectorLabel(row: Automation): string {
    const id = row.trigger_connector_id;
    return (
      this.connectors().find((c) => c.id === id)?.name ?? (id ? this.lineNames()[id] : undefined) ?? id ?? ''
    );
  }

  /** The caller's phone lines by id, for list rows whose line is scoped to a
   * project the eligible-connector list was not loaded for. */
  readonly lineNames = signal<Record<string, string>>({});

  private loadLineNames(): void {
    this.api.getDatasources(undefined, 'gpt_live_sip').subscribe((rows) =>
      this.lineNames.set(Object.fromEntries(rows.map((row) => [row.id, row.name]))),
    );
  }

  /** The caller's active projects: the names the editor shows. */
  readonly projects = signal<Project[]>([]);
  /** The projects a new automation can be scoped to — creating one in a
   *  project needs editor or owner (admins pass any membership check). */
  readonly projectChoices = computed(() => {
    const isAdmin = this.users.currentUser()?.is_admin ?? false;
    return this.projects().filter((p) => isAdmin || p.user_role !== 'viewer');
  });
  /** The create editor's project when the choices lack it (a ?project=
   *  prefill the list does not hold): offered anyway, because a select whose
   *  value is not among its options renders blank. */
  readonly unlistedProjectId = computed(() => {
    const id = this.editor()?.projectId;
    return id && !this.projectChoices().some((p) => p.id === id) ? id : null;
  });

  private loadProjects(): void {
    this.api.getProjects(this.users.currentUserId() ?? undefined, ['active'])
      .pipe(catchError(() => of([] as Project[])))
      .subscribe((projects) => this.projects.set(projects));
  }

  /** A project's name, or null when neither the loaded list nor the
   *  ?project= filter knows it (an archived project, say). */
  projectName(id: string): string | null {
    return this.projects().find((p) => p.id === id)?.name
      ?? (id === this.projectFilter() ? this.projectFilterName() : null);
  }

  /** Active project filter (from ?project=<uuid>). null when unscoped. */
  readonly projectFilter = signal<string | null>(null);
  /** Human-readable name of the filtered project. null while loading or on access denial. */
  readonly projectFilterName = signal<string | null>(null);

  readonly expertOptions = computed<SelectOption[]>(() =>
    this.experts().map((e) => ({value: e.id, label: e.display_name})),
  );

  readonly presetOptions: SelectOption[] = [
    {value: 'every_day', label: 'every day'},
    {value: 'every_weekday', label: 'every weekday (Mon–Fri)'},
    {value: 'every_monday', label: 'every Monday'},
    {value: 'every_month_first', label: 'first day of the month'},
    {value: 'every_hour', label: 'every hour'},
    {value: 'custom', label: 'custom cron'},
  ];

  readonly autonomyOptions: SelectOption[] = [
    {value: 'full', label: 'full (never pause)'},
    {value: 'review', label: 'review after completion'},
    {value: 'partial', label: 'partial'},
    {value: 'guided', label: 'guided'},
    {value: 'dependent', label: 'dependent'},
  ];

  /** Pre-selected timezones. Falls back to the IANA browser default. */
  readonly timezoneOptions = computed<SelectOption[]>(() => {
    const browserTz = this._defaultTimezone();
    const common = [
      'UTC',
      'Europe/Berlin',
      'Europe/London',
      'America/New_York',
      'America/Los_Angeles',
      'Asia/Tokyo',
      'Australia/Sydney',
    ];
    const all = new Set<string>([browserTz, ...common]);
    return Array.from(all).map((tz) => ({value: tz, label: tz}));
  });

  readonly cronExpr = computed<string>(() => {
    const e = this.editor();
    if (!e) return '';
    return derivePresetCron(e.preset, e.time, e.customCron);
  });

  /** When at or above the soft cap we surface a warning above the list. */
  readonly softCapHit = computed(() => this.automations().length >= SOFT_CAP);

  ngOnInit(): void {
    // URL semantics: /automations?project=<uuid> scopes the page to that
    // project. The header chip surfaces the filter; the New-automation
    // button defaults to creating inside the scoped project; after-save
    // refresh stays scoped via loadByProject.
    const pending = this.route.snapshot.queryParamMap.get('project');
    this.loadConnectors(pending);
    this.loadLineNames();
    this.loadProjects();
    if (pending) {
      this.projectFilter.set(pending);
      this.service.loadByProject(pending);
      // Fetch the project name for the header chip. Null on access denial
      // (we still keep the filter — the API will already have rejected).
      this.api.getProject(pending).subscribe({
        next: (p: Project | null) => this.projectFilterName.set(p?.name ?? null),
        error: () => this.projectFilterName.set(null),
      });
    } else {
      this.service.loadMine();
    }

    // Wait for the experts list before opening the editor on cross-link —
    // otherwise the editor's `expert` field seeds to '' (this.experts() is
    // empty until the API resolves) and save-time validation rejects the
    // row even though the <select> shows the right native default.
    this.api.getExperts('worker').subscribe({
      next: (list) => {
        this.experts.set(list);
        if (pending && !this.editor()) {
          this.openEditor(null, pending);
        }
      },
      error: () => this.experts.set([]),
    });
  }

  /** Reload the visible list honoring the active project filter. */
  private refreshList(): void {
    const pid = this.projectFilter();
    if (pid) {
      this.service.loadByProject(pid);
    } else {
      this.service.loadMine();
    }
  }

  /** Drop the project filter and reload the list with the caller's full set. */
  clearProjectFilter(): void {
    this.projectFilter.set(null);
    this.projectFilterName.set(null);
    // Replace the URL so the chip doesn't reappear on refresh.
    this.router.navigate(['/automations'], {
      queryParams: {},
      replaceUrl: true,
    });
    this.service.loadMine();
    // An open editor keeps the project it shows; the filter only scoped the list.
    const current = this.editor();
    this.loadConnectors(current ? current.projectId : null);
  }

  /** Open the create editor; honors the active project filter when set. */
  openNewEditor(): void {
    this.openEditor(null, this.projectFilter());
  }

  /** Project name for the filter chip; falls back to a localized placeholder
   *  when getProject() hasn't resolved or access was denied. Called from the
   *  template inside a transloco pipe, so it stays reactive to lang changes
   *  via the pipe's own subscription. */
  projectFilterDisplayName(): string {
    return (
      this.projectFilterName() ??
      this.transloco.translate('automations.list.projectFilter.fallbackName')
    );
  }

  openEditor(automation: Automation | null, prefillProjectId: string | null = null): void {
    this.errorMessage.set(null);
    this.connectorContext.set(`automation:${++this.editorGeneration}`);
    this.loadConnectors(automation?.project_id ?? prefillProjectId);
    if (automation) {
      const {preset, time} = inferPresetFromCron(automation.cron_expr ?? '');
      this.editor.set({
        mode: 'edit',
        triggerType: automation.trigger_type ?? 'cron',
        connectorIds: Object.values(automation.job_shape?.execution?.connectors ?? {}).map((c) => c.ref.uid),
        connectorAliases: Object.fromEntries(Object.entries(automation.job_shape?.execution?.connectors ?? {}).map(([alias, c]) => [c.ref.uid, alias])),
        primaryConnectorId: automation.trigger_connector_id ?? automation.job_shape?.execution?.connectors?.[automation.job_shape?.primary_connector ?? '']?.ref.uid ?? '',
        workspaceChoice: workspaceChoiceOf(automation.job_shape?.workspace),
        id: automation.id,
        name: automation.name,
        description: automation.description ?? '',
        preset,
        time,
        customCron: automation.cron_expr ?? '',
        timezone: automation.timezone,
        expert: this._expertSelectionValue(automation),
        prompt: automation.prompt,
        enabled: automation.enabled,
        autonomy: automation.autonomy,
        priority: automation.priority,
        maxFiresPerDay: automation.max_fires_per_day,
        catchupWindowHours: Math.round(automation.catchup_window_seconds / 3600),
        projectId: automation.project_id,
        showAdvanced: false,
      });
    } else {
      this.editor.set({
        mode: 'create',
        triggerType: 'cron',
        connectorIds: [],
        connectorAliases: {},
        primaryConnectorId: '',
        workspaceChoice: {kind: 'default'},
        id: null,
        name: '',
        description: '',
        preset: 'every_day',
        time: '09:00',
        customCron: '',
        timezone: this._defaultTimezone(),
        expert: this.experts()[0]?.id ?? '',
        prompt: '',
        enabled: true,
        autonomy: 'review',
        priority: 5,
        maxFiresPerDay: 100,
        catchupWindowHours: 24,
        projectId: prefillProjectId,
        showAdvanced: false,
      });
    }
  }

  closeEditor(): void {
    this.editor.set(null);
    this.errorMessage.set(null);
  }

  /** Create only (PATCH cannot move an automation): the project decides
   *  which connectors and workspace templates are eligible, so a change
   *  reloads the connectors and drops selections the new project lacks. */
  onProjectChange(projectId: string | null): void {
    const current = this.editor();
    if (!current || current.mode !== 'create' || current.projectId === projectId) return;
    // A fresh picker context: choices made against the old project's rows
    // must not come back checked when those rows do.
    this.connectorContext.set(`automation:${++this.editorGeneration}`);
    this.editor.set({...current, projectId});
    this.loadConnectors(projectId);
  }

  updateEditor<K extends keyof EditorState>(field: K, value: EditorState[K]): void {
    const current = this.editor();
    if (!current) return;
    if (field === 'triggerType' && current.mode === 'edit') return;
    this.editor.set({...current, [field]: value});
  }

  saveEditor(): void {
    const e = this.editor();
    if (!e) return;
    const cron = this.cronExpr();
    if ((e.triggerType === 'cron' && !cron) || !e.name.trim() || !e.expert || !e.prompt.trim()) {
      this.errorMessage.set(this.transloco.translate('automations.editor.requiredMissing'));
      return;
    }
    const expertSelection = this._expertRequestFields(e.expert);
    const body: CreateAutomationRequest = {
      name: e.name.trim(),
      description: e.description.trim() || null,
      expert: expertSelection.expert,
      expert_id: expertSelection.expert_id,
      prompt: e.prompt.trim(),
      autonomy: e.autonomy,
      priority: e.priority,
      enabled: e.enabled,
      max_fires_per_day: e.maxFiresPerDay,
    };
    if (e.triggerType === 'live') {
      if (this.connectorsLoading() || this.connectorsError() || !this.selectedLiveConnectors().some((c) => c.id === e.primaryConnectorId)) {
        this.errorMessage.set(this.transloco.translate('automations.editor.liveConnectorRequired'));
        return;
      }
      const bindings = connectorBindings(e.connectorIds, this.connectors(), e.connectorAliases);
      body.primary_connector = bindings.aliases[e.primaryConnectorId];
      body.job_shape = {execution: {connectors: bindings.connectors}, ...choiceRequestFields(e.workspaceChoice)};
    } else {
      body.cron_expr = cron;
      body.timezone = e.timezone;
      body.catchup_window_seconds = Math.max(0, e.catchupWindowHours * 3600);
    }
    if (e.mode === 'create') {
      body.trigger_type = e.triggerType;
      body.project_id = e.projectId;
    }
    this.errorMessage.set(null);
    const op$ =
      e.mode === 'create'
        ? this.service.create(body)
        : this.service.update(e.id!, body);
    op$.subscribe({
      next: () => {
        this.closeEditor();
        this.refreshList();
      },
      error: (err) => {
        this.errorMessage.set(this._formatError(err));
      },
    });
  }

  toggleEnabled(row: Automation): void {
    const op$ = row.enabled ? this.service.pause(row.id) : this.service.resume(row.id);
    op$.subscribe({
      error: (err) => this.errorMessage.set(this._formatError(err)),
    });
  }

  runNow(row: Automation): void {
    if (row.trigger_type === 'live') return;
    this.service.runNow(row.id).subscribe({
      next: () => this.refreshList(),
      error: (err) => this.errorMessage.set(this._formatError(err)),
    });
  }

  remove(row: Automation): void {
    const confirmMessage = this.transloco.translate('automations.list.confirmDelete', {
      name: row.name,
    });
    if (!confirm(confirmMessage)) return;
    this.service.delete(row.id).subscribe({
      error: (err) => this.errorMessage.set(this._formatError(err)),
    });
  }

  expertLabel(row: Automation): string {
    const selected = this.experts().find(
      (expert) =>
        expert.id === row.expert_id ||
        expert.id === row.expert ||
        expert.name === row.expert,
    );
    return selected?.display_name ?? row.expert;
  }

  private _expertSelectionValue(automation: Automation): string {
    if (automation.expert_id) return automation.expert_id;
    const selected = this.experts().find(
      (expert) => expert.id === automation.expert || expert.name === automation.expert,
    );
    return selected?.id ?? automation.expert;
  }

  private _expertRequestFields(selectionValue: string): {
    expert: string;
    expert_id: string | null;
  } {
    const selected = this.experts().find((expert) => expert.id === selectionValue);
    if (selected?.storage_kind === 'db') {
      return {expert: 'worker_base', expert_id: selected.id};
    }
    return {
      expert: selected?.name ?? selected?.id ?? selectionValue,
      expert_id: null,
    };
  }

  private _defaultTimezone(): string {
    try {
      return Intl.DateTimeFormat().resolvedOptions().timeZone || 'UTC';
    } catch {
      return 'UTC';
    }
  }

  private _formatError(err: unknown): string {
    if (
      err &&
      typeof err === 'object' &&
      'error' in err &&
      err.error &&
      typeof (err as {error: {detail?: unknown}}).error === 'object'
    ) {
      const detail = (err as {error: {detail?: unknown}}).error.detail;
      if (typeof detail === 'string') return detail;
      if (detail && typeof detail === 'object' && 'code' in detail && detail.code === 'live_connector_taken') {
        const holder = detail as {automation_name?: string};
        return this.transloco.translate('automations.errors.liveConnectorTaken', {name: holder.automation_name ?? ''});
      }
    }
    return this.transloco.translate('automations.errors.generic');
  }
}

/** Convert the preset picker state into a 5-field cron expression. */
export function derivePresetCron(
  preset: CronPreset,
  time: string,
  custom: string,
): string {
  if (preset === 'custom') return custom.trim();
  const [hh, mm] = time.split(':');
  const h = parseInt(hh ?? '0', 10);
  const m = parseInt(mm ?? '0', 10);
  if (Number.isNaN(h) || Number.isNaN(m)) return '';
  switch (preset) {
    case 'every_day':
      return `${m} ${h} * * *`;
    case 'every_weekday':
      return `${m} ${h} * * 1-5`;
    case 'every_monday':
      return `${m} ${h} * * 1`;
    case 'every_month_first':
      return `${m} ${h} 1 * *`;
    case 'every_hour':
      return `${m} * * * *`;
  }
}

/** Reverse-engineer the preset picker state from a stored cron expression.
 *  Falls back to 'custom' when the expression doesn't match a known
 *  preset shape — keeps round-trip editing predictable. */
export function inferPresetFromCron(expr: string): {preset: CronPreset; time: string} {
  const parts = expr.trim().split(/\s+/);
  if (parts.length !== 5) return {preset: 'custom', time: '09:00'};
  const [m, h, dom, _mon, dow] = parts;
  const minute = m.padStart(2, '0');
  const hour = h.padStart(2, '0');
  const looksDaily = dom === '*' && dow === '*';
  if (m !== '*' && h === '*' && dom === '*' && dow === '*') {
    return {preset: 'every_hour', time: '00:00'};
  }
  if (looksDaily && /^\d+$/.test(m) && /^\d+$/.test(h)) {
    return {preset: 'every_day', time: `${hour}:${minute}`};
  }
  if (/^\d+$/.test(m) && /^\d+$/.test(h) && dom === '*' && dow === '1-5') {
    return {preset: 'every_weekday', time: `${hour}:${minute}`};
  }
  if (/^\d+$/.test(m) && /^\d+$/.test(h) && dom === '*' && dow === '1') {
    return {preset: 'every_monday', time: `${hour}:${minute}`};
  }
  if (/^\d+$/.test(m) && /^\d+$/.test(h) && dom === '1' && dow === '*') {
    return {preset: 'every_month_first', time: `${hour}:${minute}`};
  }
  return {preset: 'custom', time: `${hour}:${minute}`};
}


/** Keep stored aliases stable; new ones are bounded DNS-style name slugs. */
export function connectorBindings(
  ids: string[],
  rows: {id: string; name: string}[],
  stored: Record<string, string> = {},
) {
  const aliases: Record<string, string> = {};
  const used = new Set(ids.map((id) => stored[id]).filter(Boolean));
  for (const id of ids) {
    if (stored[id]) {
      aliases[id] = stored[id];
      continue;
    }
    const name = rows.find((c) => c.id === id)?.name ?? 'connector';
    const base = name.normalize('NFKD')
      .replace(/[\u0300-\u036f]/g, '')
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, '-')
      .replace(/^-+|-+$/g, '')
      .slice(0, 63)
      .replace(/-+$/g, '') || 'connector';
    let alias = base;
    let suffix = 2;
    while (used.has(alias)) {
      const tail = `-${suffix++}`;
      alias = `${base.slice(0, 63 - tail.length).replace(/-+$/g, '')}${tail}`;
    }
    used.add(alias);
    aliases[id] = alias;
  }
  return {aliases, connectors: Object.fromEntries(ids.map((id) => [aliases[id], {ref: {uid: id}}]))};
}

function workspaceChoiceOf(binding: WorkspaceBinding | undefined): WorkspaceChoice {
  if (binding === undefined) return {kind: 'default'};
  if (binding === null) return {kind: 'none'};
  if ('inline' in binding.template) return {kind: 'inline', spec: binding.template.inline};
  return {kind: 'ref', ref: binding.template.ref, backend: 'sandbox', label: binding.template.ref.name};
}
