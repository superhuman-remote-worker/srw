import {Component, computed, effect, inject, input, model, signal, untracked} from '@angular/core';
import {TranslocoPipe, TranslocoService} from '@jsverse/transloco';
import {Observable, Subject, catchError, map, of, switchMap, tap} from 'rxjs';
import {ApiService} from '../../core/services/api.service';
import {UserService} from '../../core/services/user.service';
import {ProjectWorkspaceDefaults} from '../../core/models/api.model';
import {WorkspacePreview} from '../../core/models/workspace.model';
import {
  ACCOUNT_ME, WorkspaceChoice, WorkspaceTemplateItem, WorkspaceTemplateSpec,
} from '../../core/models/workspace-template.model';
import {AppButtonComponent} from '../../ui/button';
import {AppDialogComponent} from '../../ui/dialog';
import {AppFormFieldComponent} from '../../ui/form-field';
import {AppInputComponent} from '../../ui/input';
import {AppSelectComponent} from '../../ui/select';
import {WorkspaceTemplateFormComponent} from '../workspaces/workspace-template-form.component';
import {
  FormField, PreservedParts, TemplateFormValue, displayName, emptyFormValue, fromDocument, itemKey,
  recommendedChoice, refChoice, shortImage, sizeSummary, srwImages, toDocument, validateTemplateForm,
} from '../workspaces/workspace-template-utils';

interface PickerOption {
  value: string;
  label: string;
  disabled: boolean;
}

export interface PickerGroup {
  labelKey: string;
  options: PickerOption[];
}

type Mode = 'none' | 'virtual' | 'container' | 'vm';
const NO_PRESERVED: PreservedParts = {metadata: {}, spec: {}, hasAny: false};

function modeOf(backend: string | null | undefined): Mode | null {
  return backend === 'sandbox' ? 'container' : backend === 'vm' || backend === 'virtual' || backend === 'none' ? backend : null;
}

function builtinsFirst(items: WorkspaceTemplateItem[]): WorkspaceTemplateItem[] {
  return [...items].sort((a, b) =>
    Number(b.installationManaged) - Number(a.installationManaged) ||
    displayName(a.resource).localeCompare(displayName(b.resource)));
}

/** The create forms' workspace choice (spec §4). Loads the templates it offers
 *  and the Project's defaults itself, so every create surface embeds it the same way. */
@Component({
  selector: 'app-workspace-picker',
  standalone: true,
  imports: [
    TranslocoPipe, AppButtonComponent, AppDialogComponent, AppFormFieldComponent, AppInputComponent,
    AppSelectComponent, WorkspaceTemplateFormComponent,
  ],
  template: `
    <app-form-field [label]="'agentSettings.workspacePicker.label' | transloco" [hint]="summary()" [error]="problem()">
      <app-button formFieldAction variant="ghost" size="sm" [disabled]="disabled()" (clicked)="openCustomize()">
        {{ 'agentSettings.workspacePicker.customize' | transloco }}
      </app-button>
      <app-select [value]="selectedValue()" [disabled]="disabled()" [ariaLabel]="'agentSettings.workspacePicker.label' | transloco"
        (changed)="select($event)">
        <option value="default">{{ defaultLabel() }}</option>
        @for (g of groups(); track g.labelKey) {
          <optgroup [label]="g.labelKey | transloco">
            @for (o of g.options; track o.value) {
              <option [value]="o.value" [disabled]="o.disabled">{{ o.label }}</option>
            }
          </optgroup>
        }
        @if (choice().kind === 'inline') {
          <option value="inline">{{ 'agentSettings.workspacePicker.custom' | transloco }}</option>
        }
        <option value="none">{{ 'agentSettings.workspacePicker.none' | transloco }}</option>
      </app-select>
    </app-form-field>
    @if (recommendationNote()) {
      <p class="note">{{ recommendationNote() }}</p>
    }

    <app-dialog [open]="customizing()" size="lg" [title]="'agentSettings.workspacePicker.customizeTitle' | transloco"
      (closed)="customizing.set(false)">
      <app-workspace-template-form purpose="inline" [value]="draft()" (valueChange)="draft.set($event)"
        [vmAllowed]="vmAllowed()" [vmUnavailableReasonKey]="vmReasonKey()" [srwImages]="images()"
        [showErrors]="draftTried()" [serverErrors]="draftErrors()" />
      <app-form-field [label]="'agentSettings.workspacePicker.saveName' | transloco" [error]="nameError()">
        <app-input [value]="draft().name" (valueChange)="patchDraft({name: $event})" />
      </app-form-field>
      <div appDialogActions>
        <app-button variant="secondary" (clicked)="customizing.set(false)">{{ 'common.cancel' | transloco }}</app-button>
        <app-button variant="secondary" [loading]="saving()" (clicked)="saveToMine()">{{ 'agentSettings.workspacePicker.saveToMine' | transloco }}</app-button>
        <app-button variant="primary" (clicked)="useCustom()">{{ 'agentSettings.workspacePicker.useOnce' | transloco }}</app-button>
      </div>
    </app-dialog>
  `,
  styles: [`
    :host { display: block; }
    .note { margin: 0.25rem 0 0; color: var(--text-muted); font-size: 0.85rem; }
  `],
})
export class WorkspacePickerComponent {
  private readonly api = inject(ApiService);
  private readonly transloco = inject(TranslocoService);
  private readonly users = inject(UserService);

  readonly role = input<'job' | 'session'>('job');
  readonly projectId = input<string | null>(null);
  readonly preview = input<WorkspacePreview | null>(null);
  readonly recommendation = input<'none' | 'virtual' | 'sandbox' | 'vm' | null>(null);
  readonly recommendedBy = input('');
  readonly disabled = input(false);
  readonly choice = model<WorkspaceChoice>({kind: 'default'});

  readonly shared = signal<WorkspaceTemplateItem[]>([]);
  readonly mine = signal<WorkspaceTemplateItem[]>([]);
  readonly projectItems = signal<WorkspaceTemplateItem[]>([]);
  readonly defaults = signal<ProjectWorkspaceDefaults | null>(null);
  readonly lastDefaultPreview = signal<WorkspacePreview | null>(null);
  private touched = false;
  private readonly projectChanges = new Subject<string | null>();
  private readonly autoPicked = signal(false);

  readonly customizing = signal(false);
  readonly draft = signal<TemplateFormValue>(emptyFormValue(ACCOUNT_ME));
  private draftPreserved: PreservedParts = NO_PRESERVED;
  readonly draftTried = signal(false);
  readonly draftErrors = signal<Partial<Record<FormField, string>>>({});
  readonly nameError = signal('');
  readonly saving = signal(false);

  private readonly canUseVm = computed(() => {
    const u = this.users.currentUser();
    return !!(u?.is_admin || u?.can_use_vm);
  });
  private readonly vmInstalled = computed(() => {
    const d = this.defaults();
    if (d) return d.vm_available;
    return this.shared().some((i) => i.installationManaged && i.resource.spec.backend === 'vm');
  });
  readonly vmAllowed = computed(() => this.canUseVm() && this.vmInstalled());
  readonly vmReasonKey = computed(() =>
    !this.vmInstalled() ? 'workspaces.vm.notInstalled' : !this.canUseVm() ? 'workspaces.vm.notAllowed' : '');
  readonly images = computed(() => srwImages(this.shared()));

  readonly groups = computed<PickerGroup[]>(() => {
    const option = (i: WorkspaceTemplateItem): PickerOption => {
      const vm = i.resource.spec.backend === 'vm';
      const tier = this.transloco.translate(`workspaces.tier.${modeOf(i.resource.spec.backend) === 'container' ? 'container' : i.resource.spec.backend}`);
      const unavailable = vm && !this.vmAllowed() ? ` (${this.transloco.translate(this.vmReasonKey() || 'workspaces.vm.notAllowed')})` : '';
      const spec = i.resource.spec;
      const image = spec.environment?.image;
      const label = [displayName(i.resource), tier, sizeSummary(spec), image ? shortImage(image) : ''].filter(Boolean).join(' · ');
      return {value: `ref:${itemKey(i)}`, label: `${label}${unavailable}`, disabled: vm && !this.vmAllowed()};
    };
    const groups: PickerGroup[] = [
      {labelKey: 'agentSettings.workspacePicker.group.shared', options: builtinsFirst(this.shared()).map(option)},
    ];
    if (this.projectId()) {
      groups.push({labelKey: 'agentSettings.workspacePicker.group.project', options: builtinsFirst(this.projectItems()).map(option)});
    }
    groups.push({labelKey: 'agentSettings.workspacePicker.group.mine', options: builtinsFirst(this.mine()).map(option)});
    return groups.filter((g) => g.options.length > 0);
  });

  readonly selectedValue = computed(() => {
    const c = this.choice();
    return c.kind === 'ref' ? `ref:${c.ref.scope.kind}/${c.ref.scope.name}/${c.ref.name}` : c.kind;
  });

  /** What "Default" resolves to: from the Project's defaults when a Project is chosen, otherwise from the last preview taken while Default was selected. */
  private readonly resolvedDefault = computed<{mode: Mode | null; template: string | null; layer: string}>(() => {
    const roleKey = this.role() === 'job' ? 'jobs' : 'sessions';
    const d = this.defaults();
    if (d) {
      const mode = d.effective[roleKey].mode as Mode;
      return {
        mode,
        template: mode === 'container' || mode === 'vm' ? d.effective[mode].template_name : null,
        layer: d.effective[roleKey].source === 'project' ? 'project' : 'installation',
      };
    }
    const p = this.lastDefaultPreview();
    return {mode: modeOf(p?.backend), template: p?.template_name ?? null, layer: p?.sources?.tier === 'project' ? 'project' : 'installation'};
  });

  readonly defaultLabel = computed(() => {
    const {mode, template, layer} = this.resolvedDefault();
    if (!mode) return this.transloco.translate('agentSettings.workspacePicker.defaultUnknown');
    const what = [this.transloco.translate(`agentSettings.workspacePicker.mode.${mode}`), template].filter(Boolean).join(' · ');
    return this.transloco.translate('agentSettings.workspacePicker.default', {
      what, layer: this.transloco.translate(`agentSettings.workspacePicker.layer.${layer}`),
    });
  });

  /** A missing Project template, for the tier Default uses. */
  readonly problem = computed(() => {
    if (this.choice().kind !== 'default') return '';
    const d = this.defaults();
    const mode = d?.effective[this.role() === 'job' ? 'jobs' : 'sessions'].mode;
    return (mode === 'container' || mode === 'vm' ? d?.template_problems[mode] : undefined) ?? '';
  });

  readonly summary = computed(() => {
    const c = this.choice();
    if (c.kind === 'default') return this.defaultSummary();
    if (c.kind === 'none') return this.transloco.translate('agentSettings.workspacePicker.summaryNone');
    const spec: WorkspaceTemplateSpec | undefined = c.kind === 'inline' ? c.spec : this.find(c.ref.scope.kind, c.ref.scope.name, c.ref.name)?.resource.spec;
    if (!spec) return c.kind === 'ref' ? c.label : '';
    const image = spec.environment?.image;
    return [
      this.transloco.translate(`workspaces.tier.${modeOf(spec.backend) === 'container' ? 'container' : spec.backend}`),
      sizeSummary(spec),
      image ? shortImage(image) : '',
    ].filter(Boolean).join(' · ');
  });

  readonly recommendationNote = computed(() => {
    const rec = this.recommendation();
    if (!rec) return '';
    const expert = this.recommendedBy();
    if (rec === 'vm' && !this.vmAllowed()) return this.transloco.translate('agentSettings.workspacePicker.recommendedUnavailable', {expert});
    if (this.autoPicked()) return this.transloco.translate('agentSettings.workspacePicker.recommended', {expert});
    return '';
  });

  constructor() {
    this.listOrEmpty('Catalog', 'shared').subscribe((l) => this.shared.set(l));
    this.listOrEmpty('Account', 'me').subscribe((l) => this.mine.set(l));
    effect(() => {
      const id = this.projectId();
      untracked(() => this.projectChanges.next(id));
    });
    // switchMap drops a superseded Project's late responses.
    this.projectChanges.pipe(
      tap((id) => this.onProjectChange(id)),
      switchMap((id) => (id ? this.listOrEmpty('Project', id) : of([] as WorkspaceTemplateItem[]))),
    ).subscribe((l) => this.projectItems.set(l));
    this.projectChanges.pipe(
      switchMap((id) => (id ? this.api.getProjectWorkspaceDefaults(id).pipe(catchError(() => of(null))) : of(null))),
    ).subscribe((d) => this.defaults.set(d));
    effect(() => {
      this.recommendation();
      this.defaults();
      this.shared();
      this.vmAllowed();
      untracked(() => this.applyRecommendation());
    });
    effect(() => {
      const p = this.preview();
      if (p && untracked(() => this.choice().kind === 'default')) this.lastDefaultPreview.set(p);
    });
  }

  private listOrEmpty(kind: string, name: string): Observable<WorkspaceTemplateItem[]> {
    return this.api.listWorkspaceTemplates(kind, name).pipe(
      map((l) => l.resources),
      catchError(() => of([] as WorkspaceTemplateItem[])),
    );
  }

  private onProjectChange(id: string | null): void {
    const c = untracked(() => this.choice());
    if (c.kind === 'ref' && c.ref.scope.kind === 'Project' && c.ref.scope.name !== id) this.choice.set({kind: 'default'});
    this.projectItems.set([]);
    this.defaults.set(null);
  }

  /** The template Default resolves to, looked up in the loaded items. */
  private resolvedDefaultItem(): WorkspaceTemplateItem | undefined {
    const name = this.resolvedDefault().template;
    if (!name) return undefined;
    return [...this.projectItems(), ...this.mine(), ...this.shared()].find((i) => i.resource.metadata.name === name);
  }

  private defaultSummary(): string {
    const {mode, layer} = this.resolvedDefault();
    if (!mode) return '';
    const spec = this.resolvedDefaultItem()?.resource.spec;
    const image = spec?.environment?.image;
    const tier = spec
      ? this.transloco.translate(`workspaces.tier.${modeOf(spec.backend) === 'container' ? 'container' : spec.backend}`)
      : this.transloco.translate(`agentSettings.workspacePicker.mode.${mode}`);
    const what = [tier, spec ? sizeSummary(spec) : '', image ? shortImage(image) : ''].filter(Boolean).join(' · ');
    return `${what} (${this.transloco.translate(`agentSettings.workspacePicker.layer.${layer}`)})`;
  }

  /** Preselect the Expert's recommendation unless the user chose by hand. */
  applyRecommendation(): void {
    if (this.touched) return;
    const rec = recommendedChoice(this.recommendation(), {
      role: this.role(), defaults: this.defaults(), shared: this.shared(), vmAllowed: this.vmAllowed(),
    });
    if (rec) {
      this.choice.set(rec);
      this.autoPicked.set(true);
    } else if (this.autoPicked()) {
      this.choice.set({kind: 'default'});
      this.autoPicked.set(false);
    }
  }

  select(value: string | null): void {
    this.touched = true;
    this.autoPicked.set(false);
    if (value === 'default' || value === 'none') {
      this.choice.set({kind: value});
      return;
    }
    if (value?.startsWith('ref:')) {
      const [kind, name, ...rest] = value.slice(4).split('/');
      const item = this.find(kind, name, rest.join('/'));
      if (item) this.choice.set(refChoice(item));
    }
  }

  openCustomize(): void {
    const base = this.baseForCustomize();
    const {value, preserved} = fromDocument({
      apiVersion: 'srw/v1alpha1', kind: 'WorkspaceTemplate',
      metadata: {name: '', scope: {...ACCOUNT_ME}}, spec: structuredCloneSpec(base),
    });
    this.draft.set(value);
    this.draftPreserved = preserved;
    this.draftTried.set(false);
    this.draftErrors.set({});
    this.nameError.set('');
    this.customizing.set(true);
  }

  patchDraft(change: Partial<TemplateFormValue>): void {
    this.draft.update((v) => ({...v, ...change}));
  }

  useCustom(): void {
    this.draftTried.set(true);
    const errors = validateTemplateForm({...this.draft(), name: 'inline'});
    if (Object.keys(errors).length) return;
    this.touched = true;
    this.autoPicked.set(false);
    this.choice.set({kind: 'inline', spec: toDocument(this.draft(), this.draftPreserved).spec});
    this.customizing.set(false);
  }

  saveToMine(): void {
    this.draftTried.set(true);
    this.nameError.set('');
    const errors = validateTemplateForm(this.draft());
    if (Object.keys(errors).length) {
      if (errors.name) this.nameError.set(this.transloco.translate(errors.name));
      return;
    }
    const doc = toDocument({...this.draft(), scope: {...ACCOUNT_ME}}, this.draftPreserved);
    this.saving.set(true);
    this.api.checkWorkspaceRecipe(doc.spec, this.projectId())
      .pipe(switchMap(() => this.api.applyManifest(doc)))
      .subscribe({
        next: () => {
          this.saving.set(false);
          this.touched = true;
          this.autoPicked.set(false);
          const scope = {kind: 'Account' as const, name: this.users.currentUserId() ?? 'me'};
          this.choice.set({kind: 'ref', ref: {name: doc.metadata.name, scope}, backend: doc.spec.backend, label: doc.metadata.name});
          this.customizing.set(false);
          this.api.listWorkspaceTemplates('Account', 'me').subscribe((l) => this.mine.set(l.resources));
        },
        error: (err) => {
          this.saving.set(false);
          const d = (err as {error?: {detail?: unknown}})?.error?.detail;
          const message = typeof d === 'string' ? d
            : d && typeof d === 'object' && 'message' in d ? String((d as {message: unknown}).message)
            : this.transloco.translate('workspaces.errors.saveFailed');
          this.draftErrors.set({form: message});
          this.nameError.set(message);
        },
      });
  }

  private find(kind: string, name: string, templateName: string): WorkspaceTemplateItem | undefined {
    return [...this.shared(), ...this.projectItems(), ...this.mine()].find((i) => {
      const m = i.resource.metadata;
      return m.scope.kind === kind && m.scope.name === name && m.name === templateName;
    });
  }

  /** What Customize starts from: the current template, else what Default resolves to, else `container-full`. */
  private baseForCustomize(): WorkspaceTemplateSpec {
    const c = this.choice();
    if (c.kind === 'inline') return c.spec;
    if (c.kind === 'ref') {
      const found = this.find(c.ref.scope.kind, c.ref.scope.name, c.ref.name);
      if (found) return found.resource.spec;
    }
    if (c.kind === 'default') {
      const {mode} = this.resolvedDefault();
      const named = this.resolvedDefaultItem();
      if (named) return named.resource.spec;
      if (mode === 'virtual') return {backend: 'virtual'};
      const fallback = mode === 'vm' ? 'vm-full' : 'container-full';
      const found = this.shared().find((i) => i.resource.metadata.name === fallback);
      if (found) return found.resource.spec;
    }
    const full = this.shared().find((i) => i.resource.metadata.name === 'container-full');
    return full?.resource.spec ?? {backend: 'sandbox'};
  }
}

function structuredCloneSpec(spec: WorkspaceTemplateSpec): WorkspaceTemplateSpec {
  return JSON.parse(JSON.stringify(spec)) as WorkspaceTemplateSpec;
}
