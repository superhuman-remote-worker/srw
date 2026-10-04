import {Component, OnInit, computed, inject, signal} from '@angular/core';
import {ActivatedRoute, Router} from '@angular/router';
import {HttpErrorResponse} from '@angular/common/http';
import {TranslocoPipe, TranslocoService} from '@jsverse/transloco';
import {catchError, of, switchMap} from 'rxjs';
import {ApiService} from '../../core/services/api.service';
import {UserService} from '../../core/services/user.service';
import {Project} from '../../core/models/api.model';
import {
  ACCOUNT_ME, CATALOG_SHARED, ManifestErrorDetail, WorkspaceTemplateDocument, WorkspaceTemplateItem,
} from '../../core/models/workspace-template.model';
import {AppButtonComponent} from '../../ui/button';
import {AppDialogComponent} from '../../ui/dialog';
import {SidebarToggleComponent} from '../../shell/sidebar-toggle/sidebar-toggle.component';
import {ScopeOption, WorkspaceTemplateFormComponent} from './workspace-template-form.component';
import {
  FormField, PreservedParts, TemplateFormValue, canEditItem, duplicateValue, emptyFormValue, errorField,
  expectedVersionKey, fromDocument, srwImages, toDocument, validateTemplateForm,
} from './workspace-template-utils';

export interface WorkspaceEditorNavigationState {
  duplicateOf?: WorkspaceTemplateDocument;
}

const INSTALLATION_MANAGED = 'This template is managed by the installation. Duplicate it to change it.';
const NO_PRESERVED: PreservedParts = {metadata: {}, spec: {}, hasAny: false};

/** `/workspaces/new` and `/workspaces/:uid` (spec §3). */
@Component({
  selector: 'app-workspace-template-editor',
  standalone: true,
  imports: [TranslocoPipe, AppButtonComponent, AppDialogComponent, SidebarToggleComponent, WorkspaceTemplateFormComponent],
  template: `
    <div class="editor">
      <header class="head">
        <app-sidebar-toggle />
        <h1>{{ (isNew() ? 'workspaces.editor.newTitle' : 'workspaces.editor.editTitle') | transloco }}</h1>
        <app-button variant="ghost" size="sm" (clicked)="back()">{{ 'workspaces.editor.back' | transloco }}</app-button>
      </header>

      @if (isBuiltin()) {
        <div class="banner info">{{ 'workspaces.editor.builtin' | transloco }}</div>
      } @else if (readOnly()) {
        <div class="banner info">{{ 'workspaces.editor.notEditable' | transloco }}</div>
      }
      @if (conflict(); as c) {
        <div class="banner err">
          {{ c.message }}
          @if (c.kind === 'builtin') {
            <app-button variant="secondary" size="sm" (clicked)="duplicate()">{{ 'workspaces.editor.duplicate' | transloco }}</app-button>
          } @else {
            <app-button variant="secondary" size="sm" (clicked)="reload()">{{ 'workspaces.editor.reload' | transloco }}</app-button>
          }
        </div>
      }
      @if (errorMessage()) { <div class="banner err" role="alert">{{ errorMessage() }}</div> }
      @if (savedMessage()) { <div class="banner ok">{{ savedMessage() }}</div> }

      <section class="card">
        <app-workspace-template-form
          [value]="value()" (valueChange)="value.set($event)"
          [readOnly]="readOnly()" [identityLocked]="!isNew()" [scopeOptions]="scopeOptions()"
          [vmAllowed]="vmAllowed()" [vmUnavailableReasonKey]="vmReasonKey()" [srwImages]="images()"
          [preservedNotice]="preserved().hasAny" [serverErrors]="fieldErrors()" [showErrors]="showErrors()" />
      </section>

      <footer class="actions">
        @if (!isNew() && !readOnly()) {
          <app-button variant="danger" (clicked)="confirmDelete.set(true)">{{ 'common.delete' | transloco }}</app-button>
        }
        <span class="spacer"></span>
        @if (!isNew()) {
          <app-button variant="secondary" (clicked)="duplicate()">{{ 'workspaces.editor.duplicate' | transloco }}</app-button>
        }
        @if (!readOnly()) {
          <app-button variant="primary" [loading]="saving()" (clicked)="save()">{{ 'common.save' | transloco }}</app-button>
        }
      </footer>

      <app-dialog [open]="confirmDelete()" [title]="'workspaces.editor.deleteTitle' | transloco" (closed)="confirmDelete.set(false)">
        <p>{{ 'workspaces.editor.deleteBody' | transloco: {name: value().name} }}</p>
        <div appDialogActions>
          <app-button variant="secondary" (clicked)="confirmDelete.set(false)">{{ 'common.cancel' | transloco }}</app-button>
          <app-button variant="danger" (clicked)="remove()">{{ 'common.delete' | transloco }}</app-button>
        </div>
      </app-dialog>
    </div>
  `,
  styles: [`
    :host { display: block; height: 100%; overflow-y: auto; }
    .editor { padding: 1rem 1.5rem; max-width: 900px; margin: 0 auto; }
    .head { display: flex; align-items: center; gap: 0.5rem; margin-bottom: 1rem; }
    .head h1 { flex: 1; margin: 0; }
    .card { background: var(--panel-bg); border: 1px solid var(--border-color); padding: 1rem; border-radius: var(--radius-surface, 8px); margin-bottom: 1rem; }
    .actions { display: flex; gap: 0.5rem; align-items: center; }
    .spacer { flex: 1; }
    .banner { margin-bottom: 1rem; padding: 0.5rem 0.75rem; border-radius: var(--radius-control); display: flex; gap: 0.75rem; align-items: center; }
    .banner.ok { background: var(--success-tint); color: var(--success); }
    .banner.err { background: var(--danger-tint); color: var(--danger); }
    .banner.info { background: var(--info-tint); color: var(--text-primary); }
  `],
})
export class WorkspaceTemplateEditorComponent implements OnInit {
  private readonly api = inject(ApiService);
  private readonly router = inject(Router);
  private readonly route = inject(ActivatedRoute);
  private readonly transloco = inject(TranslocoService);
  private readonly users = inject(UserService);
  /** Router state is only readable during construction (expert-editor.component.ts does the same). */
  private readonly duplicateOf = (this.router.getCurrentNavigation()?.extras.state as WorkspaceEditorNavigationState | undefined)?.duplicateOf;

  readonly item = signal<WorkspaceTemplateItem | null>(null);
  readonly value = signal<TemplateFormValue>(emptyFormValue(ACCOUNT_ME));
  readonly preserved = signal<PreservedParts>(NO_PRESERVED);
  readonly shared = signal<WorkspaceTemplateItem[]>([]);
  readonly projects = signal<Project[]>([]);
  readonly saving = signal(false);
  readonly showErrors = signal(false);
  readonly fieldErrors = signal<Partial<Record<FormField, string>>>({});
  readonly errorMessage = signal('');
  readonly savedMessage = signal('');
  readonly conflict = signal<{kind: 'builtin' | 'version'; message: string} | null>(null);
  readonly confirmDelete = signal(false);

  readonly isNew = computed(() => this.item() === null);
  readonly isBuiltin = computed(() => !!this.item()?.installationManaged);
  readonly readOnly = computed(() => {
    const item = this.item();
    return !!item && !canEditItem(item, this.users.currentUser());
  });
  readonly images = computed(() => srwImages(this.shared()));
  private readonly vmInstalled = computed(() =>
    this.shared().some((i) => i.installationManaged && i.resource.spec.backend === 'vm'));
  private readonly canUseVm = computed(() => {
    const u = this.users.currentUser();
    return !!(u?.is_admin || u?.can_use_vm);
  });
  readonly vmAllowed = computed(() => this.vmInstalled() && this.canUseVm());
  readonly vmReasonKey = computed(() =>
    !this.vmInstalled() ? 'workspaces.vm.notInstalled' : !this.canUseVm() ? 'workspaces.vm.notAllowed' : '');
  readonly scopeOptions = computed<ScopeOption[]>(() => {
    const stored = this.item()?.resource.metadata.scope;
    if (stored) return [{key: `${stored.kind}/${stored.name}`, scope: stored, label: this.scopeLabel(stored.kind, stored.name)}];
    const user = this.users.currentUser();
    const options: ScopeOption[] = [{key: 'Account/me', scope: ACCOUNT_ME, label: this.transloco.translate('workspaces.scope.mine')}];
    for (const p of this.projects()) {
      if (user?.is_admin || p.user_role === 'owner' || p.user_role === 'editor') {
        options.push({key: `Project/${p.id}`, scope: {kind: 'Project', name: p.id}, label: this.transloco.translate('workspaces.scope.project', {name: p.name})});
      }
    }
    if (user?.is_admin) options.push({key: 'Catalog/shared', scope: CATALOG_SHARED, label: this.transloco.translate('workspaces.scope.shared')});
    return options;
  });

  ngOnInit(): void {
    this.api.listWorkspaceTemplates('Catalog', 'shared').subscribe((list) => this.shared.set(list.resources));
    this.api.getProjects(this.users.currentUserId() ?? undefined, ['active'])
      .pipe(catchError(() => of([] as Project[])))
      .subscribe((projects) => this.projects.set(projects));
    const uid = this.route.snapshot.paramMap.get('uid');
    if (uid) this.load(uid);
    else if (this.duplicateOf) {
      this.value.set(duplicateValue(this.duplicateOf, ACCOUNT_ME));
      this.preserved.set(fromDocument(this.duplicateOf).preserved);
    }
  }

  load(uid: string): void {
    this.conflict.set(null);
    this.errorMessage.set('');
    this.api.getResource(uid).subscribe({
      next: (item) => {
        const {value, preserved} = fromDocument(item.resource);
        this.item.set(item);
        this.value.set(value);
        this.preserved.set(preserved);
      },
      error: (err) => this.errorMessage.set(this.detail(err) ?? this.transloco.translate('workspaces.errors.loadFailed')),
    });
  }

  reload(): void {
    const item = this.item();
    if (item) this.load(item.uid);
  }

  save(): void {
    this.showErrors.set(true);
    this.fieldErrors.set({});
    this.errorMessage.set('');
    this.savedMessage.set('');
    this.conflict.set(null);
    if (Object.keys(validateTemplateForm(this.value())).length) return;
    const doc = toDocument(this.value(), this.preserved());
    const item = this.item();
    const versions = item ? {[expectedVersionKey(item.resource)]: item.resourceVersion} : undefined;
    const projectId = doc.metadata.scope.kind === 'Project' ? doc.metadata.scope.name : null;
    this.saving.set(true);
    this.api.checkWorkspaceRecipe(doc.spec, projectId)
      .pipe(switchMap(() => this.api.applyManifest(doc, versions)))
      .subscribe({
        next: (result) => {
          this.saving.set(false);
          const uid = result.resources[0]?.uid;
          if (item && uid === item.uid) {
            this.savedMessage.set(this.transloco.translate('workspaces.editor.saved'));
            this.load(uid);
          } else {
            void this.router.navigate(uid ? ['/workspaces', uid] : ['/workspaces']);
          }
        },
        error: (err) => {
          this.saving.set(false);
          this.handleError(err, 'workspaces.errors.saveFailed');
        },
      });
  }

  remove(): void {
    const item = this.item();
    if (!item) return;
    this.api.deleteResource(item.uid, item.resourceVersion).subscribe({
      next: () => {
        this.confirmDelete.set(false);
        void this.router.navigate(['/workspaces']);
      },
      error: (err) => {
        this.confirmDelete.set(false);
        this.handleError(err, 'workspaces.errors.deleteFailed');
      },
    });
  }

  duplicate(): void {
    if (this.isNew()) {
      // Already on /workspaces/new: the Router ignores a same-URL navigation, so apply the copy in place.
      const draft = toDocument(this.value(), this.preserved());
      this.conflict.set(null);
      this.value.set(duplicateValue(draft, ACCOUNT_ME));
      this.preserved.set(fromDocument(draft).preserved);
      return;
    }
    const doc = this.item()!.resource;
    const state: WorkspaceEditorNavigationState = {duplicateOf: doc};
    void this.router.navigate(['/workspaces/new'], {state});
  }

  back(): void {
    void this.router.navigate(['/workspaces']);
  }

  private handleError(err: unknown, fallbackKey: string): void {
    const status = err instanceof HttpErrorResponse ? err.status : 0;
    const detail = (err as {error?: {detail?: unknown}})?.error?.detail;
    if (status === 409) {
      const message = typeof detail === 'string' ? detail : this.transloco.translate(fallbackKey);
      this.conflict.set({kind: message === INSTALLATION_MANAGED ? 'builtin' : 'version', message});
      return;
    }
    if (status === 422 && detail && typeof detail === 'object' && 'message' in detail) {
      const issue = detail as ManifestErrorDetail;
      const field = errorField(issue.path);
      if (field === 'form') this.errorMessage.set(issue.message);
      else this.fieldErrors.set({[field]: issue.message});
      return;
    }
    this.errorMessage.set(typeof detail === 'string' ? detail : this.transloco.translate(fallbackKey));
  }

  private detail(err: unknown): string | null {
    const d = (err as {error?: {detail?: unknown}})?.error?.detail;
    return typeof d === 'string' ? d : null;
  }

  private scopeLabel(kind: string, name: string): string {
    if (kind === 'Catalog') return this.transloco.translate('workspaces.scope.shared');
    if (kind === 'Account') return this.transloco.translate('workspaces.scope.mine');
    const project = this.projects().find((p) => p.id === name);
    return this.transloco.translate('workspaces.scope.project', {name: project?.name ?? name});
  }
}
