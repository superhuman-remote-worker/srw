import {Component, computed, inject, OnInit, signal} from '@angular/core';
import {Router} from '@angular/router';
import {TranslocoPipe, TranslocoService} from '@jsverse/transloco';
import {ApiService} from '../../core/services/api.service';
import type {Expert, ExpertCreateRequest, ExpertRole} from '../../core/models/api.model';
import {AppButtonComponent} from '../../ui/button';
import {AppIconButtonComponent} from '../../ui/icon-button';
import {AppBadgeComponent} from '../../ui/badge';
import {AppChipComponent} from '../../ui/chip';
import {AppIconComponent} from '../../ui/icon';
import {AppSpinnerComponent} from '../../ui/spinner';
import {AppDialogComponent} from '../../ui/dialog';
import {AppMenuComponent, AppMenuItemComponent, AppMenuTriggerDirective} from '../../ui/menu';
import {ViewportService} from '../../core/services/viewport.service';
import {UserService} from '../../core/services/user.service';

export type ExpertTypeFilter = 'all' | ExpertRole;

/**
 * Pure list filter — exported for unit tests. Matches `expert_type || tags`
 * (U1: tags are additive role metadata — a session expert tagged `worker`
 * lists under Worker, and `subagent` only ever exists as a tag).
 */
export function filterExperts(rows: Expert[], f: ExpertTypeFilter): Expert[] {
  if (f === 'all') return rows;
  return rows.filter((r) => r.expert_type === f || (r.tags ?? []).includes(f));
}

/** Role tags worth showing next to the type column: the ones that are not
 *  simply the expert's own type. */
export function extraRoleTags(e: Expert): string[] {
  const roles: string[] = ['worker', 'session', 'subagent'];
  return (e.tags ?? []).filter((t) => roles.includes(t) && t !== e.expert_type);
}

/** Bundled (disk) and subagent-library experts are read-only; absence of a
 *  source means bundled. */
export function isBundled(e: Expert): boolean {
  const source = e.source ?? 'bundled';
  return source === 'bundled' || source === 'library';
}

/**
 * Which transloco key (+ params) reports a `duplicate` result — pulled out
 * of `duplicate()` so the "was anything dropped" decision is a pure function,
 * unit-testable without standing up the component's five injected services.
 *
 * `dropped` names the capability grants (e.g. `"shell_tools"`) the source
 * config needed that this user doesn't hold; the server strips them rather
 * than refusing the fork (2026-08-04 decision) and reports them here so the
 * strip is never silent — decision 9 in
 * knowledge-history/done/global_expert_management.md is explicit that a silent capability
 * downgrade burns debugging time. Grant keys are passed through verbatim
 * (comma-joined, not humanized), because they're exactly what the Admin
 * UI's grants table shows (`admin-grants.component.ts` renders them raw in
 * a `<code>` cell).
 */
export function duplicateResultTranslationArgs(
  dropped: string[] | undefined,
): [key: string, params?: Record<string, string>] {
  if (dropped && dropped.length > 0) {
    return ['experts.duplicatedMissingGrants', {grants: dropped.join(', ')}];
  }
  return ['experts.duplicated'];
}

@Component({
  selector: 'app-experts-list',
  standalone: true,
  imports: [
    TranslocoPipe,
    AppButtonComponent,
    AppIconButtonComponent,
    AppBadgeComponent,
    AppChipComponent,
    AppIconComponent,
    AppSpinnerComponent,
    AppDialogComponent,
    AppMenuComponent,
    AppMenuItemComponent,
    AppMenuTriggerDirective,
  ],
  template: `
    <div class="experts">
      <header class="head">
        <div class="head-left">
          <h1>{{ 'experts.title' | transloco }}</h1>
        </div>
        <div class="head-actions">
          <app-button variant="secondary" (clicked)="fileInput.click()">
            {{ 'experts.import' | transloco }}
          </app-button>
          <input #fileInput type="file" accept="application/json,.json" hidden (change)="onImport($event)" />
          <app-button variant="primary" (clicked)="newExpert()">
            {{ 'experts.new' | transloco }}
          </app-button>
        </div>
      </header>

      <div class="filters">
        @for (f of filters; track f.value) {
          <app-chip [selected]="typeFilter() === f.value" (clicked)="typeFilter.set(f.value)">
            {{ f.key | transloco }}
          </app-chip>
        }
      </div>

      @if (loading()) {
        <app-spinner />
      } @else if (filtered().length === 0) {
        <p class="empty">{{ 'experts.empty' | transloco }}</p>
      } @else {
        <table class="grid app-table">
          <thead>
            <tr>
              <th>{{ 'experts.colName' | transloco }}</th>
              <th>{{ 'experts.colType' | transloco }}</th>
              <th>{{ 'experts.colSource' | transloco }}</th>
              <th class="actions-col">{{ 'experts.colActions' | transloco }}</th>
            </tr>
          </thead>
          <tbody>
            @for (e of filtered(); track e.id) {
              <tr>
                <td class="name-cell">
                  <span class="name-inner">
                    <app-icon size="sm" [style.color]="e.color">{{ e.icon || 'smart_toy' }}</app-icon>
                    <span>{{ e.display_name }}</span>
                  </span>
                </td>
                <td>
                  {{ e.expert_type || '—' }}
                  @for (role of extraRoles(e); track role) {
                    <app-badge tone="neutral" size="xs">{{ role }}</app-badge>
                  }
                </td>
                <td>
                  <app-badge [tone]="bundled(e) ? 'neutral' : 'info'">{{ e.source || 'bundled' }}</app-badge>
                  @if (bundled(e)) {
                    <app-badge tone="neutral">{{ 'experts.bundledReadonly' | transloco }}</app-badge>
                  }
                </td>
                <td class="actions-col">
                  @if (viewport.isMobile()) {
                    <!-- Mobile: collapse the row actions into a ⋯ overflow menu so the
                         cell is just the kebab and the table fits without h-scroll
                         (mirrors the Jobs / Data Sources lists). -->
                    <app-icon-button
                      variant="ghost"
                      size="sm"
                      [ariaLabel]="'experts.moreActions' | transloco"
                      [appMenuTrigger]="rowMenu"
                      menuPlacement="bottom-end"
                    >
                      <app-icon size="sm">more_vert</app-icon>
                    </app-icon-button>
                    <app-menu #rowMenu>
                      @if (canEdit(e)) {
                        <app-menu-item (activated)="edit(e)">
                          {{ 'experts.edit' | transloco }}
                        </app-menu-item>
                      }
                      <app-menu-item (activated)="duplicate(e)">
                        {{ 'experts.duplicate' | transloco }}
                      </app-menu-item>
                      <app-menu-item (activated)="exportExpert(e)">
                        {{ 'experts.export' | transloco }}
                      </app-menu-item>
                      @if (canDelete(e)) {
                        <app-menu-item tone="danger" (activated)="askDelete(e)">
                          {{ 'experts.delete' | transloco }}
                        </app-menu-item>
                      }
                    </app-menu>
                  } @else {
                    @if (canEdit(e)) {
                      <app-icon-button
                        size="sm"
                        variant="ghost"
                        [ariaLabel]="'experts.edit' | transloco"
                        [tooltip]="'experts.edit' | transloco"
                        (clicked)="edit(e)"
                      >
                        <app-icon size="sm">edit</app-icon>
                      </app-icon-button>
                    }
                    <app-icon-button
                      size="sm"
                      variant="ghost"
                      [ariaLabel]="'experts.duplicate' | transloco"
                      [tooltip]="'experts.duplicate' | transloco"
                      (clicked)="duplicate(e)"
                    >
                      <app-icon size="sm">content_copy</app-icon>
                    </app-icon-button>
                    <app-icon-button
                      size="sm"
                      variant="ghost"
                      [ariaLabel]="'experts.export' | transloco"
                      [tooltip]="'experts.export' | transloco"
                      (clicked)="exportExpert(e)"
                    >
                      <app-icon size="sm">download</app-icon>
                    </app-icon-button>
                    @if (canDelete(e)) {
                      <app-icon-button
                        size="sm"
                        variant="danger"
                        [ariaLabel]="'experts.delete' | transloco"
                        [tooltip]="'experts.delete' | transloco"
                        (clicked)="askDelete(e)"
                      >
                        <app-icon size="sm">delete</app-icon>
                      </app-icon-button>
                    }
                  }
                </td>
              </tr>
            }
          </tbody>
        </table>
      }

      @if (successMessage()) {
        <div class="banner ok">{{ successMessage() }}</div>
      }
      @if (errorMessage()) {
        <div class="banner err">{{ errorMessage() }}</div>
      }

      <app-dialog
        [open]="confirmOpen()"
        [title]="'experts.confirmDeleteTitle' | transloco"
        (closed)="confirmOpen.set(false)"
      >
        <p>{{ 'experts.confirmDeleteBody' | transloco }} <strong>{{ pendingDelete()?.display_name }}</strong>?</p>
        <div appDialogActions>
          <app-button variant="secondary" (clicked)="confirmOpen.set(false)">
            {{ 'experts.cancel' | transloco }}
          </app-button>
          <app-button variant="danger" (clicked)="confirmDelete()">
            {{ 'experts.delete' | transloco }}
          </app-button>
        </div>
      </app-dialog>
    </div>
  `,
  styles: [
    `
      :host {
        display: block;
        height: 100%;
        overflow-y: auto;
      }
      .experts {
        padding: 1rem 1.5rem;
        max-width: 1100px;
        margin: 0 auto;
      }
      .head {
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 1rem;
      }
      .head h1 {
        margin: 0;
      }
      .head-left {
        display: flex;
        align-items: center;
        gap: 12px;
      }
      .head-actions {
        display: flex;
        gap: 0.5rem;
      }
      .filters {
        display: flex;
        gap: 0.5rem;
        margin-bottom: 1rem;
      }
      .name-cell .name-inner {
        display: inline-flex;
        align-items: center;
        gap: 0.5rem;
      }
      .actions-col {
        text-align: right;
        white-space: nowrap;
      }
      .actions-col app-icon-button {
        vertical-align: middle;
      }
      .actions-col app-icon-button + app-icon-button {
        margin-left: 2px;
      }
      .empty {
        color: var(--text-muted);
        padding: 2rem 0;
        text-align: center;
      }
      .banner {
        margin-top: 1rem;
        padding: 0.5rem 0.75rem;
        border-radius: var(--radius-control);
      }
      .banner.ok {
        background: var(--success-tint);
        color: var(--success);
      }
      .banner.err {
        background: var(--danger-tint);
        color: var(--danger);
      }
    `,
  ],
})
export class ExpertsListComponent implements OnInit {
  private api = inject(ApiService);
  private router = inject(Router);
  private transloco = inject(TranslocoService);
  private userService = inject(UserService);
  protected readonly viewport = inject(ViewportService);

  filters: {value: ExpertTypeFilter; key: string}[] = [
    {value: 'all', key: 'experts.filterAll'},
    {value: 'worker', key: 'experts.filterWorker'},
    {value: 'session', key: 'experts.filterSession'},
    {value: 'subagent', key: 'experts.filterSubagent'},
  ];
  typeFilter = signal<ExpertTypeFilter>('all');
  rows = signal<Expert[]>([]);
  loading = signal(true);
  successMessage = signal('');
  errorMessage = signal('');
  confirmOpen = signal(false);
  pendingDelete = signal<Expert | null>(null);

  filtered = computed(() => filterExperts(this.rows(), this.typeFilter()));
  bundled = isBundled;
  extraRoles = extraRoleTags;

  canEdit(e: Expert): boolean {
    if (isBundled(e)) return false;
    const user = this.userService.currentUser();
    return !!user && (user.is_admin || e.owner_id === user.id);
  }

  canDelete(e: Expert): boolean {
    return !e.managed_key && this.canEdit(e);
  }

  ngOnInit(): void {
    this.refresh();
  }

  refresh(): void {
    this.loading.set(true);
    this.api.getExperts().subscribe((rows) => {
      this.rows.set(rows);
      this.loading.set(false);
    });
  }

  newExpert(): void {
    this.router.navigate(['/experts/new']);
  }

  onImport(ev: Event): void {
    const input = ev.target as HTMLInputElement;
    const file = input.files?.[0];
    if (!file) return;
    file.text().then((text) => {
      input.value = '';
      let body: ExpertCreateRequest;
      try {
        body = JSON.parse(text) as ExpertCreateRequest;
      } catch {
        this.errorMessage.set(this.transloco.translate('experts.invalidJson'));
        return;
      }
      this.api.importExpert(body).subscribe({
        next: () => {
          this.successMessage.set(this.transloco.translate('experts.imported'));
          this.refresh();
        },
        error: (err) => this.errorMessage.set(this.detail(err) ?? 'Import failed'),
      });
    });
  }

  edit(e: Expert): void {
    this.router.navigate(['/experts', e.id, 'edit']);
  }

  duplicate(e: Expert): void {
    this.api.duplicateExpert(e.id).subscribe({
      next: (result) => {
        const [key, params] = duplicateResultTranslationArgs(result.dropped);
        this.successMessage.set(this.transloco.translate(key, params));
        this.refresh();
      },
      error: (err) => this.errorMessage.set(this.detail(err) ?? 'Duplicate failed'),
    });
  }

  exportExpert(e: Expert): void {
    this.api.exportExpert(e.id).subscribe((bundle) => {
      const blob = new Blob([JSON.stringify(bundle, null, 2)], {type: 'application/json'});
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = `${e.display_name || e.id}.expert.json`;
      a.click();
      URL.revokeObjectURL(url);
    });
  }

  askDelete(e: Expert): void {
    this.pendingDelete.set(e);
    this.confirmOpen.set(true);
  }

  confirmDelete(): void {
    const e = this.pendingDelete();
    if (!e) return;
    this.api.deleteExpert(e.id).subscribe({
      next: () => {
        this.confirmOpen.set(false);
        this.successMessage.set(this.transloco.translate('experts.deleted'));
        this.refresh();
      },
      error: (err) => {
        this.confirmOpen.set(false);
        const d = err?.error?.detail;
        this.errorMessage.set(
          d && typeof d === 'object' && Array.isArray(d.blockers)
            ? this.transloco.translate('experts.inUse', {count: d.blockers.length})
            : (typeof d === 'string' ? d : 'Delete failed'),
        );
      },
    });
  }

  private detail(err: unknown): string | null {
    const d = (err as {error?: {detail?: unknown}})?.error?.detail;
    return typeof d === 'string' ? d : null;
  }
}
