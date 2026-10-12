import {beforeAll, describe, expect, it, vi} from 'vitest';
import {TestBed} from '@angular/core/testing';
import {Component, EventEmitter, Input, Output, signal, ɵresolveComponentResources} from '@angular/core';
import {TranslocoPipe, TranslocoTestingModule} from '@jsverse/transloco';
import {TranslocoService} from '@jsverse/transloco';
import {Subject, of, throwError} from 'rxjs';
import {HttpErrorResponse} from '@angular/common/http';
import {WorkspacePickerComponent} from './workspace-picker.component';
import {ApiService} from '../../core/services/api.service';
import {UserService} from '../../core/services/user.service';
import {CATALOG_SHARED, WorkspaceTemplateItem} from '../../core/models/workspace-template.model';
import {ProjectWorkspaceDefaults} from '../../core/models/api.model';
import {choiceRequestFields} from '../workspaces/workspace-template-utils';

const USER = '0a1b2c3d-0000-4000-8000-000000000001';
const t = (name: string, backend: 'sandbox' | 'vm' | 'virtual', scope = CATALOG_SHARED, managed = scope === CATALOG_SHARED): WorkspaceTemplateItem => ({
  resource: {apiVersion: 'srw/v1alpha1', kind: 'WorkspaceTemplate', metadata: {name, scope},
    spec: {backend, ...(backend === 'virtual' ? {} : {environment: {image: `ghcr.io/o/${name}:1`}, resources: {cpu: 2, memory: '4Gi'}})}},
  uid: `uid-${name}`, resourceVersion: 1, installationManaged: managed,
});
const SHARED = [t('virtual', 'virtual'), t('container-full', 'sandbox'), t('vm-full', 'vm')];
const MINE = [t('lean', 'sandbox', {kind: 'Account', name: USER}, false)];
const DEFAULTS = (over: Partial<ProjectWorkspaceDefaults> = {}): ProjectWorkspaceDefaults => ({
  stored: {jobs: null, sessions: null, container: null, vm: null},
  managed_by_manifest: false, can_edit: true, account_templates: false,
  effective: {jobs: {mode: 'container', source: 'installation'}, sessions: {mode: 'virtual', source: 'installation'},
    container: {template_name: 'container-full', source: 'builtin'}, vm: {template_name: 'vm-full', source: 'builtin'}},
  installation: {jobs: 'container', sessions: 'virtual', container: 'container-full', vm: 'vm-full'},
  template_problems: {}, installation_problems: [], vm_available: true, ...over,
});

function create(opts: {projectId?: string | null; defaults?: ProjectWorkspaceDefaults; canUseVm?: boolean; recommendation?: string | null; role?: 'job' | 'session'} = {}) {
  TestBed.resetTestingModule();
  const api = {
    listWorkspaceTemplates: vi.fn().mockImplementation((kind: string, name: string) =>
      of({resources: kind === 'Catalog' ? SHARED : kind === 'Account' ? MINE : [t('web', 'sandbox', {kind: 'Project', name}, false)]})),
    getProjectWorkspaceDefaults: vi.fn().mockReturnValue(of(opts.defaults ?? DEFAULTS())),
    checkWorkspaceRecipe: vi.fn().mockReturnValue(of({})),
    applyManifest: vi.fn().mockReturnValue(of({resources: [{uid: 'u-new', resourceVersion: 1, changed: true}]})),
  };
  TestBed.configureTestingModule({providers: [
    WorkspacePickerComponent,
    {provide: ApiService, useValue: api},
    {provide: UserService, useValue: {currentUser: signal({id: USER, is_admin: false, can_use_vm: opts.canUseVm ?? true}), currentUserId: signal(USER)}},
    {provide: TranslocoService, useValue: {translate: (k: string, p?: Record<string, unknown>) => (p ? `${k}${JSON.stringify(p)}` : k)}},
  ]});
  const c = TestBed.inject(WorkspacePickerComponent);
  const stub = (name: string, value: unknown) => Object.defineProperty(c, name, {value: () => value});
  stub('projectId', opts.projectId ?? null);
  stub('role', opts.role ?? 'job');
  stub('recommendation', opts.recommendation ?? null);
  stub('recommendedBy', 'Engineer');
  stub('preview', null);
  stub('disabled', false);
  TestBed.tick();
  TestBed.tick();
  return {c, api};
}

describe('WorkspacePickerComponent', () => {
  it('groups Shared (built-ins first), this Project and Mine', () => {
    const {c, api} = create({projectId: 'p-1'});
    expect(api.getProjectWorkspaceDefaults).toHaveBeenCalledWith('p-1');
    expect(c.groups().map((g) => g.labelKey)).toEqual([
      'agentSettings.workspacePicker.group.shared', 'agentSettings.workspacePicker.group.project', 'agentSettings.workspacePicker.group.mine',
    ]);
    expect(c.groups()[2].options[0].value).toBe(`ref:Account/${USER}/lean`);
  });

  it('offers Mine in a non-personal Project too (owner ruling)', () => {
    const {c} = create({projectId: 'p-1', defaults: DEFAULTS({account_templates: false})});
    expect(c.groups().some((g) => g.options.some((o) => o.value.includes('/lean')))).toBe(true);
  });

  it('disables VM templates when VMs are unavailable (Review Focus 3)', () => {
    const noGrant = create({canUseVm: false});
    expect(noGrant.c.groups()[0].options.find((o) => o.value.endsWith('/vm-full'))?.disabled).toBe(true);
    const killSwitch = create({projectId: 'p-1', defaults: DEFAULTS({vm_available: false})});
    expect(killSwitch.c.vmAllowed()).toBe(false);
  });

  it('labels a disabled VM template with its reason', () => {
    const {c} = create({canUseVm: false});
    const label = c.groups()[0].options.find((o) => o.value.endsWith('/vm-full'))!.label;
    expect(label).toContain('(workspaces.vm.notAllowed)');
    expect(label).not.toContain('.)');
    // A real sentence ends in a period; the label must not double it.
    TestBed.resetTestingModule();
    TestBed.configureTestingModule({providers: [
      WorkspacePickerComponent,
      {provide: ApiService, useValue: {
        listWorkspaceTemplates: vi.fn().mockImplementation((kind: string) => of({resources: kind === 'Catalog' ? SHARED : []})),
        getProjectWorkspaceDefaults: vi.fn(),
      }},
      {provide: UserService, useValue: {currentUser: signal({id: USER, is_admin: false, can_use_vm: false}), currentUserId: signal(USER)}},
      {provide: TranslocoService, useValue: {translate: (k: string) => k === 'workspaces.vm.notAllowed' ? "VMs aren't enabled on this installation." : k}},
    ]});
    const d = TestBed.inject(WorkspacePickerComponent);
    TestBed.tick();
    TestBed.tick();
    const text = d.groups()[0].options.find((o) => o.value.endsWith('/vm-full'))!.label;
    expect(text).toContain("(VMs aren't enabled on this installation)");
    expect(text).not.toContain('.)');
  });

  it('falls back to Default when the Project changes under a Project template', () => {
    const {c} = create({projectId: 'p-1'});
    c.choice.set({kind: 'ref', ref: {name: 'web', scope: {kind: 'Project', name: 'p-1'}}, backend: 'sandbox', label: 'web'});
    (c as unknown as {onProjectChange(id: string | null): void}).onProjectChange('p-2');
    expect(c.choice()).toEqual({kind: 'default'});
    c.choice.set({kind: 'ref', ref: {name: 'lean', scope: {kind: 'Account', name: USER}}, backend: 'sandbox', label: 'lean'});
    (c as unknown as {onProjectChange(id: string | null): void}).onProjectChange('p-3');
    expect(c.choice().kind).toBe('ref');
  });

  it('reports a missing Project template through problem()', () => {
    const {c} = create({projectId: 'p-1', defaults: DEFAULTS({template_problems: {container: 'Template web was not found'}})});
    expect(c.problem()).toBe('Template web was not found');
    c.select('none');
    expect(c.problem()).toBe('');
  });

  it('auto-picks again after reset()', () => {
    const {c} = create({recommendation: 'vm'});
    c.select('default');
    c.applyRecommendation();
    expect(c.choice()).toEqual({kind: 'default'});
    c.reset();
    c.applyRecommendation();
    expect(c.choice()).toMatchObject({kind: 'ref', ref: {name: 'vm-full'}});
    expect(c.recommendationNote()).toContain('agentSettings.workspacePicker.recommended');
  });

  it('waits for the Project defaults before auto-picking the recommendation', () => {
    const defaults$ = new Subject<ProjectWorkspaceDefaults>();
    TestBed.resetTestingModule();
    const api = {
      listWorkspaceTemplates: vi.fn().mockImplementation((kind: string) => of({resources: kind === 'Catalog' ? SHARED : []})),
      getProjectWorkspaceDefaults: vi.fn().mockReturnValue(defaults$),
    };
    TestBed.configureTestingModule({providers: [
      WorkspacePickerComponent, {provide: ApiService, useValue: api},
      {provide: UserService, useValue: {currentUser: signal({id: USER, is_admin: false, can_use_vm: true}), currentUserId: signal(USER)}},
      {provide: TranslocoService, useValue: {translate: (k: string) => k}},
    ]});
    const c = TestBed.inject(WorkspacePickerComponent);
    Object.defineProperty(c, 'projectId', {value: () => 'p-1'});
    Object.defineProperty(c, 'recommendation', {value: () => 'vm'});
    const emitted: unknown[] = [];
    c.choice.subscribe((v) => emitted.push(v));
    TestBed.tick();
    TestBed.tick();
    expect(c.choice()).toEqual({kind: 'default'});
    expect(emitted).toEqual([]);
    defaults$.next(DEFAULTS());
    TestBed.tick();
    expect(c.choice()).toMatchObject({kind: 'ref', ref: {name: 'vm-full'}});
  });

  it('settles the defaults wait when the load fails', () => {
    TestBed.resetTestingModule();
    const api = {
      listWorkspaceTemplates: vi.fn().mockImplementation((kind: string) => of({resources: kind === 'Catalog' ? SHARED : []})),
      getProjectWorkspaceDefaults: vi.fn().mockReturnValue(throwError(() => new Error('x'))),
    };
    TestBed.configureTestingModule({providers: [
      WorkspacePickerComponent, {provide: ApiService, useValue: api},
      {provide: UserService, useValue: {currentUser: signal({id: USER, is_admin: false, can_use_vm: true}), currentUserId: signal(USER)}},
      {provide: TranslocoService, useValue: {translate: (k: string) => k}},
    ]});
    const c = TestBed.inject(WorkspacePickerComponent);
    Object.defineProperty(c, 'projectId', {value: () => 'p-1'});
    Object.defineProperty(c, 'recommendation', {value: () => 'vm'});
    TestBed.tick();
    TestBed.tick();
    expect(c.choice()).toMatchObject({kind: 'ref', ref: {name: 'vm-full'}});
  });

  it('turns a selection into the matching choice', () => {
    const {c} = create();
    c.select(`ref:Account/${USER}/lean`);
    expect(choiceRequestFields(c.choice())).toEqual({workspace: {template: {ref: {name: 'lean', scope: {kind: 'Account', name: USER}}}}});
    c.select('none');
    expect(c.choice()).toEqual({kind: 'none'});
    c.select('default');
    expect(c.choice()).toEqual({kind: 'default'});
  });

  it('labels the default with what it resolves to and where it comes from', () => {
    const {c} = create({projectId: 'p-1', defaults: DEFAULTS({effective: {...DEFAULTS().effective, jobs: {mode: 'container', source: 'project'}}})});
    expect(c.defaultLabel()).toContain('agentSettings.workspacePicker.mode.container · container-full');
    expect(c.defaultLabel()).toContain('agentSettings.workspacePicker.layer.project');
  });

  it('preselects the recommended tier’s template over an installation default', () => {
    const {c} = create({recommendation: 'vm'});
    expect(c.choice()).toMatchObject({kind: 'ref', ref: {name: 'vm-full', scope: CATALOG_SHARED}});
    expect(c.recommendationNote()).toContain('agentSettings.workspacePicker.recommended');
  });

  it('lets a Project default beat the recommendation', () => {
    const {c} = create({projectId: 'p-1', recommendation: 'vm', defaults: DEFAULTS({effective: {...DEFAULTS().effective, jobs: {mode: 'container', source: 'project'}}})});
    expect(c.choice()).toEqual({kind: 'default'});
  });

  it('only hints a VM recommendation the user cannot run (Review Focus 3)', () => {
    const {c} = create({recommendation: 'vm', canUseVm: false});
    expect(c.choice()).toEqual({kind: 'default'});
    expect(c.recommendationNote()).toContain('agentSettings.workspacePicker.recommendedUnavailable');
  });

  it('keeps a manual choice when the recommendation is re-applied', () => {
    const {c} = create({recommendation: 'vm'});
    c.select('none');
    c.applyRecommendation();
    expect(c.choice()).toEqual({kind: 'none'});
  });

  it('customizes into an inline recipe for this run only', () => {
    const {c} = create();
    c.select('ref:Catalog/shared/container-full');
    c.openCustomize();
    c.draft.update((v) => ({...v, memory: '8Gi'}));
    c.useCustom();
    expect(c.choice()).toEqual({kind: 'inline', spec: {backend: 'sandbox', resources: {cpu: 2, memory: '8Gi'}, environment: {image: 'ghcr.io/o/container-full:1'}}});
  });

  it('saves a customized recipe to Mine and selects it', () => {
    const {c, api} = create({projectId: 'p-1'});
    c.select('ref:Catalog/shared/container-full');
    c.openCustomize();
    c.draft.update((v) => ({...v, name: 'my-lean'}));
    c.saveToMine();
    const [doc] = api.applyManifest.mock.calls[0];
    expect(doc.metadata).toEqual({name: 'my-lean', scope: {kind: 'Account', name: 'me'}});
    expect(api.checkWorkspaceRecipe).toHaveBeenCalledWith(doc.spec, 'p-1');
    expect(c.choice()).toMatchObject({kind: 'ref', ref: {name: 'my-lean', scope: {kind: 'Account', name: USER}}});
  });

  it('shows tier, sizes and image in each option label', () => {
    const {c} = create();
    const label = c.groups()[0].options.find((o) => o.value.endsWith('/container-full'))!.label;
    expect(label).toContain('workspaces.tier.container');
    expect(label).toContain('2 CPU · 4Gi');
    expect(label).toContain('container-full:1');
  });

  it('summarises the Default: found template and not-found fallback', () => {
    const found = create({projectId: 'p-1'});
    expect(found.c.summary()).toContain('2 CPU · 4Gi');
    expect(found.c.summary()).toContain('container-full:1');
    expect(found.c.summary()).toContain('agentSettings.workspacePicker.layer.installation');
    const missing = create({projectId: 'p-1', defaults: DEFAULTS({effective: {...DEFAULTS().effective, container: {template_name: 'gone', source: 'builtin'}}})});
    expect(missing.c.summary()).toBe('agentSettings.workspacePicker.mode.container (agentSettings.workspacePicker.layer.installation)');
  });

  it('customizes from the default preview when there is no Project', () => {
    const {c} = create();
    c.lastDefaultPreview.set({backend: 'virtual', source: 'default', binding: null, template_name: 'virtual'});
    c.openCustomize();
    expect(c.draft().backend).toBe('virtual');
  });

  it('validates the inline run without the template name, and reports a bad name on save', () => {
    const {c, api} = create();
    c.select('ref:Catalog/shared/container-full');
    c.openCustomize();
    c.draft.update((v) => ({...v, name: 'My Lean'}));
    c.useCustom();
    expect(c.choice().kind).toBe('inline');
    c.openCustomize();
    c.draft.update((v) => ({...v, name: 'My Lean'}));
    c.saveToMine();
    expect(c.nameError()).toBe('workspaces.errors.name');
    expect(api.applyManifest).not.toHaveBeenCalled();
  });

  it('reports a taken name when Save to Mine applies unchanged', () => {
    const {c, api} = create();
    api.applyManifest.mockReturnValue(of({resources: [{uid: 'u-old', resourceVersion: 1, changed: false}]}));
    c.select('ref:Catalog/shared/container-full');
    c.openCustomize();
    c.draft.update((v) => ({...v, name: 'lean'}));
    c.saveToMine();
    expect(c.nameError()).toBe('workspaces.errors.nameTaken');
    expect(c.choice()).toMatchObject({kind: 'ref', ref: {name: 'container-full'}});
    expect(c.customizing()).toBe(true);
  });

  it('reports a taken name on Save to Mine instead of a version conflict', () => {
    const {c, api} = create();
    api.applyManifest.mockReturnValue(throwError(() => new HttpErrorResponse({status: 409, error: {detail: 'Updating a resource requires its expected resource version.'}})));
    c.openCustomize();
    c.draft.update((v) => ({...v, name: 'lean'}));
    c.saveToMine();
    expect(c.nameError()).toBe('workspaces.errors.nameTaken');
    expect(c.saving()).toBe(false);
    expect(c.customizing()).toBe(true);
  });

  it.each([
    {path: '/spec/environment/image', field: 'image'},
    {path: '/spec/resources/cpu', field: 'cpu'},
  ])('shows a 422 on $field under that field, not the name', ({path, field}) => {
    const {c, api} = create();
    api.checkWorkspaceRecipe.mockReturnValue(throwError(() => new HttpErrorResponse({status: 422, error: {detail: {path, message: 'bad value'}}})));
    c.select('ref:Catalog/shared/container-full');
    c.openCustomize();
    c.draft.update((v) => ({...v, name: 'my-lean'}));
    c.saveToMine();
    expect(c.draftErrors()).toEqual({[field]: 'bad value'});
    expect(c.nameError()).toBe('');
    expect(c.saving()).toBe(false);
  });

  it('keeps a form-level 422 under the name', () => {
    const {c, api} = create();
    api.checkWorkspaceRecipe.mockReturnValue(throwError(() => new HttpErrorResponse({status: 422, error: {detail: 'Nope'}})));
    c.openCustomize();
    c.draft.update((v) => ({...v, name: 'my-lean'}));
    c.saveToMine();
    expect(c.nameError()).toBe('Nope');
    expect(c.draftErrors()).toEqual({});
  });

  it('blocks Enter from submitting an enclosing form, except in a textarea', () => {
    const {c} = create();
    const press = (el: HTMLElement) => {
      const e = new KeyboardEvent('keydown', {key: 'Enter', cancelable: true});
      Object.defineProperty(e, 'target', {value: el});
      c.blockImplicitSubmit(e);
      return e.defaultPrevented;
    };
    expect(press(document.createElement('input'))).toBe(true);
    expect(press(document.createElement('textarea'))).toBe(false);
  });

  it('drops a superseded Project load', () => {
    const p1 = new Subject<unknown>();
    const p2 = new Subject<unknown>();
    TestBed.resetTestingModule();
    const pid = signal<string | null>('p1');
    const api = {
      listWorkspaceTemplates: vi.fn().mockImplementation((kind: string, name: string) =>
        kind === 'Project' ? (name === 'p1' ? p1 : p2) : of({resources: []})),
      getProjectWorkspaceDefaults: vi.fn().mockReturnValue(of(DEFAULTS())),
      checkWorkspaceRecipe: vi.fn(), applyManifest: vi.fn(),
    };
    TestBed.configureTestingModule({providers: [
      WorkspacePickerComponent, {provide: ApiService, useValue: api},
      {provide: UserService, useValue: {currentUser: signal({id: USER, is_admin: false, can_use_vm: true}), currentUserId: signal(USER)}},
      {provide: TranslocoService, useValue: {translate: (k: string) => k}},
    ]});
    const c = TestBed.inject(WorkspacePickerComponent);
    Object.defineProperty(c, 'projectId', {value: pid});
    TestBed.tick();
    TestBed.tick();
    pid.set('p2');
    TestBed.tick();
    TestBed.tick();
    p2.next({resources: [t('two', 'sandbox', {kind: 'Project', name: 'p2'}, false)]});
    p1.next({resources: [t('one', 'sandbox', {kind: 'Project', name: 'p1'}, false)]});
    expect(c.projectItems().map((i) => i.resource.metadata.name)).toEqual(['two']);
  });

  it('treats a failed listing as empty', () => {
    const {c, api} = create({projectId: 'p-1'});
    expect(c.groups().length).toBe(3);
    TestBed.resetTestingModule();
    api.listWorkspaceTemplates.mockImplementation((kind: string) => kind === 'Account' ? throwError(() => new Error('x')) : of({resources: SHARED}));
    TestBed.configureTestingModule({providers: [
      WorkspacePickerComponent, {provide: ApiService, useValue: api},
      {provide: UserService, useValue: {currentUser: signal({id: USER, is_admin: false, can_use_vm: true}), currentUserId: signal(USER)}},
      {provide: TranslocoService, useValue: {translate: (k: string) => k}},
    ]});
    const d = TestBed.inject(WorkspacePickerComponent);
    Object.defineProperty(d, 'projectId', {value: () => 'p-1'});
    TestBed.tick();
    TestBed.tick();
    expect(d.groups().map((g) => g.labelKey)).toEqual([
      'agentSettings.workspacePicker.group.shared', 'agentSettings.workspacePicker.group.project',
    ]);
  });
});

// This vitest pipeline does not bind signal inputs on the real ui components, so these
// stubs (decorator inputs) render the native `for` / `id` attributes the real ones do.
@Component({selector: 'app-form-field', standalone: true,
  template: '<label class="app-form-field__label" [attr.for]="forId || null">{{ label }}</label><ng-content />'})
class StubFormField {
  @Input() label = '';
  @Input() forId = '';
  @Input() hint = '';
  @Input() error = '';
}
@Component({selector: 'app-input', standalone: true, template: '<input [attr.id]="inputId || null" />'})
class StubInput {
  @Input() value = '';
  @Input() inputId = '';
  @Output() valueChange = new EventEmitter<string>();
}
@Component({selector: 'app-dialog', standalone: true, template: '<ng-content />'})
class StubDialog {
  @Input() open = false;
  @Input() size = '';
  @Input() title = '';
  @Output() closed = new EventEmitter<void>();
}
@Component({selector: 'app-button', standalone: true, template: '<ng-content />'})
class StubButton {
  @Input() variant = '';
  @Input() size = '';
  @Input() disabled = false;
  @Input() loading = false;
  @Output() clicked = new EventEmitter<void>();
}
@Component({selector: 'app-select', standalone: true, template: '<select><ng-content /></select>'})
class StubSelect {
  @Input() value: string | null = null;
  @Input() disabled = false;
  @Input() ariaLabel = '';
  @Output() changed = new EventEmitter<string | null>();
}
@Component({selector: 'app-workspace-template-form', standalone: true, template: ''})
class StubTemplateForm {
  @Input() purpose = '';
  @Input() value: unknown;
  @Input() vmAllowed = false;
  @Input() vmUnavailableReasonKey = '';
  @Input() srwImages: unknown;
  @Input() showErrors = false;
  @Input() serverErrors: unknown;
  @Output() valueChange = new EventEmitter<unknown>();
}

describe('WorkspacePickerComponent rendering', () => {
  beforeAll(async () => {
    await ɵresolveComponentResources(() => Promise.resolve(''));
  });

  it('associates the Save as label with the name input', () => {
    TestBed.resetTestingModule();
    TestBed.configureTestingModule({
      imports: [WorkspacePickerComponent, TranslocoTestingModule.forRoot({langs: {en: {}}, translocoConfig: {availableLangs: ['en'], defaultLang: 'en'}})],
      providers: [
        {provide: ApiService, useValue: {
          listWorkspaceTemplates: vi.fn().mockReturnValue(of({resources: []})),
          getProjectWorkspaceDefaults: vi.fn().mockReturnValue(of(null)),
        }},
        {provide: UserService, useValue: {currentUser: signal({id: USER, is_admin: false, can_use_vm: true}), currentUserId: signal(USER)}},
      ],
    });
    TestBed.overrideComponent(WorkspacePickerComponent, {
      set: {imports: [TranslocoPipe, StubFormField, StubInput, StubDialog, StubButton, StubSelect, StubTemplateForm]},
    });
    const fixture = TestBed.createComponent(WorkspacePickerComponent);
    fixture.detectChanges();
    const root = fixture.nativeElement as HTMLElement;
    const label = Array.from(root.querySelectorAll<HTMLLabelElement>('label.app-form-field__label'))
      .find((l) => l.textContent?.includes('saveName'));
    expect(label, 'Save as label').toBeDefined();
    expect(label!.htmlFor).not.toBe('');
    const control = root.querySelector(`[id="${label!.htmlFor}"]`);
    expect(control?.tagName).toBe('INPUT');
  });
});
