import {Component, inject, OnInit, signal} from '@angular/core';
import {Router} from '@angular/router';
import {TranslocoPipe, TranslocoService} from '@jsverse/transloco';
import {ApiService} from '../../core/services/api.service';
import type {Skill} from '../../core/models/api.model';
import {AppButtonComponent} from '../../ui/button';
import {AppIconButtonComponent} from '../../ui/icon-button';
import {AppBadgeComponent} from '../../ui/badge';
import {AppIconComponent} from '../../ui/icon';
import {AppSpinnerComponent} from '../../ui/spinner';
import {AppDialogComponent} from '../../ui/dialog';
import {AppMenuComponent, AppMenuItemComponent, AppMenuTriggerDirective} from '../../ui/menu';
import {ViewportService} from '../../core/services/viewport.service';

/** Bundled (disk) skills are read-only; absence of a source means bundled. */
export function isBundledSkill(s: Skill): boolean {
  return (s.source ?? 'bundled') === 'bundled';
}

@Component({
  selector: 'app-skills-list',
  standalone: true,
  imports: [
    TranslocoPipe,
    AppButtonComponent,
    AppIconButtonComponent,
    AppBadgeComponent,
    AppIconComponent,
    AppSpinnerComponent,
    AppDialogComponent,
    AppMenuComponent,
    AppMenuItemComponent,
    AppMenuTriggerDirective,
  ],
  template: `
    <div class="skills">
      <header class="head">
        <div class="head-left">
          <h1>{{ 'skills.title' | transloco }}</h1>
        </div>
        <div class="head-actions">
          <app-button variant="secondary" (clicked)="fileInput.click()">
            {{ 'skills.import' | transloco }}
          </app-button>
          <input #fileInput type="file" accept="application/zip,.zip" hidden (change)="onImport($event)" />
          <app-button variant="primary" (clicked)="newSkill()">
            {{ 'skills.new' | transloco }}
          </app-button>
        </div>
      </header>

      @if (loading()) {
        <app-spinner />
      } @else if (rows().length === 0) {
        <p class="empty">{{ 'skills.empty' | transloco }}</p>
      } @else {
        <table class="grid app-table">
          <thead>
            <tr>
              <th>{{ 'skills.colName' | transloco }}</th>
              <th>{{ 'skills.colSource' | transloco }}</th>
              <th class="actions-col">{{ 'skills.colActions' | transloco }}</th>
            </tr>
          </thead>
          <tbody>
            @for (s of rows(); track s.id) {
              <tr>
                <td class="name-cell">
                  <span class="name-inner">
                    <app-icon size="sm" [style.color]="s.color">{{ s.icon || 'extension' }}</app-icon>
                    <span>{{ s.display_name }}</span>
                  </span>
                  <small class="desc">{{ s.description }}</small>
                </td>
                <td>
                  <app-badge [tone]="bundled(s) ? 'neutral' : 'info'">{{ s.source || 'bundled' }}</app-badge>
                </td>
                <td class="actions-col">
                  @if (viewport.isMobile()) {
                    <!-- Mobile: collapse the row actions into a ⋯ overflow menu so the
                         cell is just the kebab and the table fits without h-scroll
                         (mirrors the Jobs / Data Sources / Experts lists). -->
                    <app-icon-button
                      variant="ghost"
                      size="sm"
                      [ariaLabel]="'skills.moreActions' | transloco"
                      [appMenuTrigger]="rowMenu"
                      menuPlacement="bottom-end"
                    >
                      <app-icon size="sm">more_vert</app-icon>
                    </app-icon-button>
                    <app-menu #rowMenu>
                      @if (!bundled(s)) {
                        <app-menu-item (activated)="edit(s)">
                          {{ 'skills.edit' | transloco }}
                        </app-menu-item>
                      }
                      <app-menu-item (activated)="duplicate(s)">
                        {{ 'skills.duplicate' | transloco }}
                      </app-menu-item>
                      <app-menu-item (activated)="exportSkill(s)">
                        {{ 'skills.export' | transloco }}
                      </app-menu-item>
                      @if (!bundled(s)) {
                        <app-menu-item tone="danger" (activated)="askDelete(s)">
                          {{ 'skills.delete' | transloco }}
                        </app-menu-item>
                      }
                    </app-menu>
                  } @else {
                    @if (!bundled(s)) {
                      <app-icon-button
                        size="sm"
                        variant="ghost"
                        [ariaLabel]="'skills.edit' | transloco"
                        [tooltip]="'skills.edit' | transloco"
                        (clicked)="edit(s)"
                      >
                        <app-icon size="sm">edit</app-icon>
                      </app-icon-button>
                    }
                    <app-icon-button
                      size="sm"
                      variant="ghost"
                      [ariaLabel]="'skills.duplicate' | transloco"
                      [tooltip]="'skills.duplicate' | transloco"
                      (clicked)="duplicate(s)"
                    >
                      <app-icon size="sm">content_copy</app-icon>
                    </app-icon-button>
                    <app-icon-button
                      size="sm"
                      variant="ghost"
                      [ariaLabel]="'skills.export' | transloco"
                      [tooltip]="'skills.export' | transloco"
                      (clicked)="exportSkill(s)"
                    >
                      <app-icon size="sm">download</app-icon>
                    </app-icon-button>
                    @if (!bundled(s)) {
                      <app-icon-button
                        size="sm"
                        variant="danger"
                        [ariaLabel]="'skills.delete' | transloco"
                        [tooltip]="'skills.delete' | transloco"
                        (clicked)="askDelete(s)"
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
        [title]="'skills.confirmDeleteTitle' | transloco"
        (closed)="confirmOpen.set(false)"
      >
        <p>{{ 'skills.confirmDeleteBody' | transloco }} <strong>{{ pendingDelete()?.display_name }}</strong>?</p>
        <div appDialogActions>
          <app-button variant="secondary" (clicked)="confirmOpen.set(false)">
            {{ 'skills.cancel' | transloco }}
          </app-button>
          <app-button variant="danger" (clicked)="confirmDelete()">
            {{ 'skills.delete' | transloco }}
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
      .skills {
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
      .grid th,
      .grid td {
        vertical-align: top;
      }
      .name-cell .name-inner {
        display: inline-flex;
        align-items: center;
        gap: 0.5rem;
      }
      .name-cell .desc {
        display: block;
        color: var(--text-muted);
        margin-top: 2px;
      }
      .actions-col {
        text-align: right;
        white-space: nowrap;
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
export class SkillsListComponent implements OnInit {
  private api = inject(ApiService);
  private router = inject(Router);
  private transloco = inject(TranslocoService);
  protected readonly viewport = inject(ViewportService);

  rows = signal<Skill[]>([]);
  loading = signal(true);
  successMessage = signal('');
  errorMessage = signal('');
  confirmOpen = signal(false);
  pendingDelete = signal<Skill | null>(null);

  bundled = isBundledSkill;

  ngOnInit(): void {
    this.refresh();
  }

  refresh(): void {
    this.loading.set(true);
    this.api.getSkills().subscribe((rows) => {
      this.rows.set(rows);
      this.loading.set(false);
    });
  }

  newSkill(): void {
    this.router.navigate(['/skills/new']);
  }

  onImport(ev: Event): void {
    const input = ev.target as HTMLInputElement;
    const file = input.files?.[0];
    if (!file) return;
    input.value = '';
    this.api.importSkill(file).subscribe({
      next: () => {
        this.successMessage.set(this.transloco.translate('skills.imported'));
        this.refresh();
      },
      error: (err) =>
        this.errorMessage.set(
          this.detail(err) ?? this.transloco.translate('skills.invalidZip'),
        ),
    });
  }

  edit(s: Skill): void {
    this.router.navigate(['/skills', s.id, 'edit']);
  }

  duplicate(s: Skill): void {
    this.api.duplicateSkill(s.id).subscribe({
      next: () => {
        this.successMessage.set(this.transloco.translate('skills.duplicated'));
        this.refresh();
      },
      error: (err) => this.errorMessage.set(this.detail(err) ?? 'Duplicate failed'),
    });
  }

  exportSkill(s: Skill): void {
    this.api.exportSkill(s.id).subscribe((blob) => {
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = `${s.name}.zip`;
      a.click();
      URL.revokeObjectURL(url);
    });
  }

  askDelete(s: Skill): void {
    this.pendingDelete.set(s);
    this.confirmOpen.set(true);
  }

  confirmDelete(): void {
    const s = this.pendingDelete();
    if (!s) return;
    this.api.deleteSkill(s.id).subscribe({
      next: () => {
        this.confirmOpen.set(false);
        this.successMessage.set(this.transloco.translate('skills.deleted'));
        this.refresh();
      },
      error: (err) => {
        this.confirmOpen.set(false);
        this.errorMessage.set(this.detail(err) ?? 'Delete failed');
      },
    });
  }

  private detail(err: unknown): string | null {
    const d = (err as {error?: {detail?: unknown}})?.error?.detail;
    return typeof d === 'string' ? d : null;
  }
}
