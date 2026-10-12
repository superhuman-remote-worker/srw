import {Component, OnInit, computed, inject, signal} from '@angular/core';
import {Router} from '@angular/router';
import {TranslocoPipe, TranslocoService} from '@jsverse/transloco';
import {Subscription, catchError, of} from 'rxjs';
import {ApiService} from '../../core/services/api.service';
import {UserService} from '../../core/services/user.service';
import {ViewportService} from '../../core/services/viewport.service';
import {Project} from '../../core/models/api.model';
import {WorkspaceTemplateItem} from '../../core/models/workspace-template.model';
import {AppBadgeComponent} from '../../ui/badge';
import {AppButtonComponent} from '../../ui/button';
import {AppChipComponent} from '../../ui/chip';
import {AppDialogComponent} from '../../ui/dialog';
import {AppIconComponent} from '../../ui/icon';
import {AppIconButtonComponent} from '../../ui/icon-button';
import {AppMenuComponent, AppMenuItemComponent, AppMenuTriggerDirective} from '../../ui/menu';
import {AppSelectComponent} from '../../ui/select';
import {AppSpinnerComponent} from '../../ui/spinner';
import {WorkspaceEditorNavigationState} from './workspace-template-editor.component';
import {canEditItem, displayName, shortImage, sizeSummary, templateDescription} from './workspace-template-utils';

type Chip = 'available' | 'mine' | 'shared' | 'project';
type ScopeKey = 'shared' | 'mine' | 'project';

interface ScopeState {
  items: WorkspaceTemplateItem[];
  loading: boolean;
  error: boolean;
}

export interface ScopeGroup extends ScopeState {
  key: ScopeKey;
  titleKey: string;
}

const EMPTY: ScopeState = {items: [], loading: false, error: false};

function ordered(items: WorkspaceTemplateItem[]): WorkspaceTemplateItem[] {
  return [...items].sort((a, b) =>
    Number(b.installationManaged) - Number(a.installationManaged) ||
    displayName(a.resource).localeCompare(displayName(b.resource)));
}

/** The Workspaces tab's list (spec §2). */
@Component({
  selector: 'app-workspace-templates-list',
  standalone: true,
  imports: [
    TranslocoPipe, AppBadgeComponent, AppButtonComponent, AppChipComponent, AppDialogComponent, AppIconComponent,
    AppIconButtonComponent, AppMenuComponent, AppMenuItemComponent, AppMenuTriggerDirective, AppSelectComponent,
    AppSpinnerComponent,
  ],
  template: `
    <div class="workspaces">
      <header class="head">
        <h1>{{ 'workspaces.list.title' | transloco }}</h1>
        <app-button variant="primary" (clicked)="newTemplate()">{{ 'workspaces.list.new' | transloco }}</app-button>
      </header>

      <div class="filters">
        @for (c of chips; track c) {
          @if (c !== 'project') {
            <app-chip [selected]="chip() === c" (clicked)="chip.set(c)">{{ 'workspaces.list.chip.' + c | transloco }}</app-chip>
          }
        }
        @if (projects().length > 0) {
          <app-select size="sm" [fullWidth]="false" [value]="projectId()" [ariaLabel]="'workspaces.list.projectAria' | transloco"
            (changed)="selectProject($event ?? '')">
            <option value="">{{ 'workspaces.list.chip.project' | transloco }}</option>
            @for (p of projects(); track p.id) {
              <option [value]="p.id">{{ p.name }}</option>
            }
          </app-select>
        }
      </div>

      @if (errorMessage()) { <div class="banner err" role="alert">{{ errorMessage() }}</div> }

      @for (g of groups(); track g.key) {
        <section class="group">
          <h2>{{ g.titleKey | transloco }}</h2>
          @if (g.loading) {
            <app-spinner />
          } @else if (g.error) {
            <div class="banner err">
              {{ 'workspaces.errors.listFailed' | transloco }}
              <app-button variant="secondary" size="sm" (clicked)="retry(g.key)">{{ 'workspaces.list.retry' | transloco }}</app-button>
            </div>
          } @else if (g.items.length === 0) {
            <p class="empty">{{ 'workspaces.list.empty' | transloco }}</p>
          } @else {
            <table class="grid app-table">
              <thead><tr>
                <th>{{ 'workspaces.list.colName' | transloco }}</th>
                <th>{{ 'workspaces.list.colTier' | transloco }}</th>
                @if (!viewport.isMobile()) {
                  <th>{{ 'workspaces.list.colImage' | transloco }}</th>
                  <th>{{ 'workspaces.list.colSize' | transloco }}</th>
                }
                <th class="actions-col">{{ 'workspaces.list.colActions' | transloco }}</th>
              </tr></thead>
              <tbody>
                @for (i of g.items; track i.uid) {
                  <tr>
                    <td>
                      <button type="button" class="link" (click)="open(i)">{{ label(i) }}</button>
                      @if (i.installationManaged) { <app-badge tone="neutral" size="xs">{{ 'workspaces.list.builtin' | transloco }}</app-badge> }
                      @if (description(i)) { <small class="desc">{{ description(i) }}</small> }
                      @if (viewport.isMobile()) { <small class="desc">{{ size(i) }}</small> }
                    </td>
                    <td><app-badge tone="info">{{ 'workspaces.tier.' + tierKey(i) | transloco }}</app-badge></td>
                    @if (!viewport.isMobile()) {
                      <td class="mono">{{ image(i) }}</td>
                      <td>{{ size(i) }}</td>
                    }
                    <td class="actions-col">
                      @if (viewport.isMobile()) {
                        <app-icon-button variant="ghost" size="sm" [ariaLabel]="'workspaces.list.moreActions' | transloco"
                          [appMenuTrigger]="rowMenu" menuPlacement="bottom-end"><app-icon size="sm">more_vert</app-icon></app-icon-button>
                        <app-menu #rowMenu>
                          <app-menu-item (activated)="duplicate(i)">{{ 'workspaces.editor.duplicate' | transloco }}</app-menu-item>
                          @if (canEdit(i)) { <app-menu-item tone="danger" (activated)="askDelete(i)">{{ 'common.delete' | transloco }}</app-menu-item> }
                        </app-menu>
                      } @else {
                        <app-icon-button size="sm" variant="ghost" [ariaLabel]="'workspaces.editor.duplicate' | transloco"
                          [tooltip]="'workspaces.editor.duplicate' | transloco" (clicked)="duplicate(i)"><app-icon size="sm">content_copy</app-icon></app-icon-button>
                        @if (canEdit(i)) {
                          <app-icon-button size="sm" variant="danger" [ariaLabel]="'common.delete' | transloco"
                            [tooltip]="'common.delete' | transloco" (clicked)="askDelete(i)"><app-icon size="sm">delete</app-icon></app-icon-button>
                        }
                      }
                    </td>
                  </tr>
                }
              </tbody>
            </table>
          }
        </section>
      }

      <app-dialog [open]="pendingDelete() !== null" [title]="'workspaces.editor.deleteTitle' | transloco" (closed)="pendingDelete.set(null)">
        <p>{{ 'workspaces.editor.deleteBody' | transloco: {name: pendingDelete()?.resource?.metadata?.name ?? ''} }}</p>
        <div appDialogActions>
          <app-button variant="secondary" (clicked)="pendingDelete.set(null)">{{ 'common.cancel' | transloco }}</app-button>
          <app-button variant="danger" (clicked)="confirmDelete()">{{ 'common.delete' | transloco }}</app-button>
        </div>
      </app-dialog>
    </div>
  `,
  styles: [`
    :host { display: block; height: 100%; overflow-y: auto; }
    .workspaces { padding: 1rem 1.5rem; max-width: 1100px; margin: 0 auto; }
    .head { display: flex; justify-content: space-between; align-items: center; margin-bottom: 1rem; }
    .filters { display: flex; gap: 0.5rem; flex-wrap: wrap; align-items: center; margin-bottom: 1rem; }
    .group { margin-bottom: 1.5rem; }
    .group h2 { font-size: 0.95rem; margin: 0 0 0.5rem; }
    .grid th, .grid td { vertical-align: top; }
    .link { background: none; border: 0; padding: 0; color: var(--text-primary); font: inherit; font-weight: 500; cursor: pointer; text-align: left; }
    .link:hover { text-decoration: underline; }
    .desc { display: block; color: var(--text-muted); }
    .mono { font-family: var(--font-mono, monospace); font-size: 0.8rem; word-break: break-all; }
    .actions-col { text-align: right; white-space: nowrap; }
    .empty { color: var(--text-muted); padding: 1rem 0; }
    .banner { margin-bottom: 1rem; padding: 0.5rem 0.75rem; border-radius: var(--radius-control); display: flex; gap: 0.75rem; align-items: center; }
    .banner.err { background: var(--danger-tint); color: var(--danger); }
  `],
})
export class WorkspaceTemplatesListComponent implements OnInit {
  private readonly api = inject(ApiService);
  private readonly router = inject(Router);
  private readonly transloco = inject(TranslocoService);
  private readonly users = inject(UserService);
  protected readonly viewport = inject(ViewportService);

  protected readonly chips: Chip[] = ['available', 'mine', 'shared', 'project'];
  readonly chip = signal<Chip>('available');
  readonly projects = signal<Project[]>([]);
  readonly projectId = signal('');
  private readonly states = {
    shared: signal<ScopeState>(EMPTY),
    mine: signal<ScopeState>(EMPTY),
    project: signal<ScopeState>(EMPTY),
  };
  private readonly inflight: Partial<Record<ScopeKey, Subscription>> = {};
  readonly pendingDelete = signal<WorkspaceTemplateItem | null>(null);
  readonly errorMessage = signal('');

  readonly groups = computed<ScopeGroup[]>(() => {
    const group = (key: ScopeKey): ScopeGroup => ({key, titleKey: `workspaces.list.group.${key}`, ...this.states[key]()});
    switch (this.chip()) {
      case 'available': return [group('shared'), group('mine')];
      case 'mine': return [group('mine')];
      case 'shared': return [group('shared')];
      case 'project': return this.projectId() ? [group('project')] : [];
    }
  });

  ngOnInit(): void {
    this.retry('shared');
    this.retry('mine');
    this.api.getProjects(this.users.currentUserId() ?? undefined, ['active'])
      .pipe(catchError(() => of([] as Project[])))
      .subscribe((projects) => this.projects.set(projects));
  }

  retry(key: ScopeKey): void {
    const [kind, name] =
      key === 'shared' ? ['Catalog', 'shared'] : key === 'mine' ? ['Account', 'me'] : ['Project', this.projectId()];
    if (!name) return;
    const state = this.states[key];
    state.set({items: [], loading: true, error: false});
    this.errorMessage.set('');
    // A newer request for the same scope supersedes this one, so a late response can't overwrite it.
    this.inflight[key]?.unsubscribe();
    this.inflight[key] = this.api.listWorkspaceTemplatesStrict(kind, name).subscribe({
      next: (list) => state.set({items: ordered(list.resources), loading: false, error: false}),
      error: () => state.set({items: [], loading: false, error: true}),
    });
  }

  selectProject(id: string): void {
    this.projectId.set(id);
    if (!id) {
      this.chip.set('available');
      return;
    }
    this.chip.set('project');
    this.retry('project');
  }

  newTemplate(): void {
    void this.router.navigate(['/workspaces/new']);
  }

  open(item: WorkspaceTemplateItem): void {
    void this.router.navigate(['/workspaces', item.uid]);
  }

  duplicate(item: WorkspaceTemplateItem): void {
    const state: WorkspaceEditorNavigationState = {duplicateOf: item.resource};
    void this.router.navigate(['/workspaces/new'], {state});
  }

  askDelete(item: WorkspaceTemplateItem): void {
    this.pendingDelete.set(item);
  }

  confirmDelete(): void {
    const item = this.pendingDelete();
    if (!item) return;
    this.errorMessage.set('');
    this.api.deleteResource(item.uid, item.resourceVersion).subscribe({
      next: () => {
        this.pendingDelete.set(null);
        this.retry(this.scopeOf(item));
      },
      error: (err) => {
        this.pendingDelete.set(null);
        // Reload first: a retry clears the banner, and the stale row may be why the delete failed.
        this.retry(this.scopeOf(item));
        const d = (err as {error?: {detail?: unknown}})?.error?.detail;
        this.errorMessage.set(typeof d === 'string' ? d : this.transloco.translate('workspaces.errors.deleteFailed'));
      },
    });
  }

  canEdit(item: WorkspaceTemplateItem): boolean {
    return canEditItem(item, this.users.currentUser());
  }

  protected label(item: WorkspaceTemplateItem): string { return displayName(item.resource); }
  protected description(item: WorkspaceTemplateItem): string { return templateDescription(item.resource); }
  protected size(item: WorkspaceTemplateItem): string { return sizeSummary(item.resource.spec); }
  protected image(item: WorkspaceTemplateItem): string {
    const image = item.resource.spec.environment?.image;
    return image ? shortImage(image) : '—';
  }
  protected tierKey(item: WorkspaceTemplateItem): string {
    const b = item.resource.spec.backend;
    return b === 'sandbox' ? 'container' : b;
  }

  private scopeOf(item: WorkspaceTemplateItem): ScopeKey {
    const kind = item.resource.metadata.scope.kind;
    return kind === 'Catalog' ? 'shared' : kind === 'Account' ? 'mine' : 'project';
  }
}
