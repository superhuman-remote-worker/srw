import {describe, expect, it, vi} from 'vitest';
import {Injector, runInInjectionContext, signal} from '@angular/core';
import {Router} from '@angular/router';
import {HttpErrorResponse} from '@angular/common/http';
import {TranslocoService} from '@jsverse/transloco';
import {of, throwError} from 'rxjs';
import {WorkspaceTemplatesListComponent} from './workspace-templates-list.component';
import {ApiService} from '../../core/services/api.service';
import {UserService} from '../../core/services/user.service';
import {ViewportService} from '../../core/services/viewport.service';
import {CATALOG_SHARED, WorkspaceTemplateItem} from '../../core/models/workspace-template.model';

const USER = '0a1b2c3d-0000-4000-8000-000000000001';
const t = (name: string, scope = CATALOG_SHARED, managed = false, backend: 'sandbox' | 'vm' | 'virtual' = 'sandbox'): WorkspaceTemplateItem => ({
  resource: {apiVersion: 'srw/v1alpha1', kind: 'WorkspaceTemplate', metadata: {name, scope}, spec: {backend}},
  uid: `uid-${name}`, resourceVersion: 2, installationManaged: managed,
});

function create(responses: Record<string, unknown>) {
  const api = {
    listWorkspaceTemplatesStrict: vi.fn().mockImplementation((kind: string, name: string) => {
      const r = responses[`${kind}/${name}`];
      // HttpErrorResponse is not an Error subclass in Angular, so test for it explicitly.
      return r instanceof HttpErrorResponse || r instanceof Error ? throwError(() => r) : of(r ?? {resources: []});
    }),
    getProjects: vi.fn().mockReturnValue(of([{id: 'p-1', name: 'Website', status: 'active', is_default: false}])),
    deleteResource: vi.fn().mockReturnValue(of({deleted: true, uid: 'x'})),
  };
  const router = {navigate: vi.fn().mockResolvedValue(true)};
  const injector = Injector.create({providers: [
    {provide: ApiService, useValue: api},
    {provide: Router, useValue: router},
    {provide: UserService, useValue: {currentUser: signal({id: USER, is_admin: false}), currentUserId: signal(USER)}},
    {provide: TranslocoService, useValue: {translate: (k: string) => k}},
    {provide: ViewportService, useValue: {isMobile: signal(false)}},
  ]});
  const c = runInInjectionContext(injector, () => new WorkspaceTemplatesListComponent());
  c.ngOnInit();
  return {c, api, router};
}

describe('WorkspaceTemplatesListComponent', () => {
  it('lists Shared and Mine by default, built-ins first', () => {
    const {c, api} = create({
      'Catalog/shared': {resources: [t('team-base'), t('container-full', CATALOG_SHARED, true)]},
      'Account/me': {resources: [t('lean', {kind: 'Account', name: USER})]},
    });
    expect(api.listWorkspaceTemplatesStrict).toHaveBeenCalledWith('Catalog', 'shared');
    expect(api.listWorkspaceTemplatesStrict).toHaveBeenCalledWith('Account', 'me');
    const groups = c.groups();
    expect(groups.map((g) => g.key)).toEqual(['shared', 'mine']);
    expect(groups[0].items.map((i) => i.resource.metadata.name)).toEqual(['container-full', 'team-base']);
  });

  it('keeps the other groups when one scope fails, and retries just that one (Review Focus 4)', () => {
    const {c, api} = create({
      'Catalog/shared': {resources: [t('container-full', CATALOG_SHARED, true)]},
      'Account/me': new HttpErrorResponse({status: 403}),
    });
    const [shared, mine] = c.groups();
    expect(shared.items).toHaveLength(1);
    expect(mine.error).toBe(true);
    api.listWorkspaceTemplatesStrict.mockClear();
    c.retry('mine');
    expect(api.listWorkspaceTemplatesStrict).toHaveBeenCalledTimes(1);
    expect(api.listWorkspaceTemplatesStrict).toHaveBeenCalledWith('Account', 'me');
  });

  it('loads a Project only when it is picked', () => {
    const {c, api} = create({'Project/p-1': {resources: [t('web', {kind: 'Project', name: 'p-1'})]}});
    expect(api.listWorkspaceTemplatesStrict).not.toHaveBeenCalledWith('Project', 'p-1');
    c.selectProject('p-1');
    expect(api.listWorkspaceTemplatesStrict).toHaveBeenCalledWith('Project', 'p-1');
    expect(c.groups().map((g) => g.key)).toEqual(['project']);
  });

  it('deletes with the row version and reloads that scope', () => {
    const mine = t('lean', {kind: 'Account', name: USER});
    const {c, api} = create({'Account/me': {resources: [mine]}});
    c.askDelete(mine);
    api.listWorkspaceTemplatesStrict.mockClear();
    c.confirmDelete();
    expect(api.deleteResource).toHaveBeenCalledWith('uid-lean', 2);
    expect(api.listWorkspaceTemplatesStrict).toHaveBeenCalledWith('Account', 'me');
  });

  it('duplicates through router state', () => {
    const builtin = t('container-full', CATALOG_SHARED, true);
    const {c, router} = create({});
    c.duplicate(builtin);
    expect(router.navigate).toHaveBeenCalledWith(['/workspaces/new'], {state: {duplicateOf: builtin.resource}});
    expect(c.canEdit(builtin)).toBe(false);
  });
});
