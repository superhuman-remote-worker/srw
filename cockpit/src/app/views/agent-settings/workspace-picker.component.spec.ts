import {describe, expect, it, vi} from 'vitest';
import {TestBed} from '@angular/core/testing';
import {signal} from '@angular/core';
import {TranslocoService} from '@jsverse/transloco';
import {of} from 'rxjs';
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
});
