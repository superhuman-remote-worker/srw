import {describe, expect, it, vi} from 'vitest';
import {Injector, runInInjectionContext, signal} from '@angular/core';
import {ActivatedRoute, Router, convertToParamMap} from '@angular/router';
import {HttpErrorResponse} from '@angular/common/http';
import {TranslocoService} from '@jsverse/transloco';
import {of, throwError} from 'rxjs';
import {WorkspaceTemplateEditorComponent} from './workspace-template-editor.component';
import {ApiService} from '../../core/services/api.service';
import {UserService} from '../../core/services/user.service';
import {WorkspaceTemplateDocument, WorkspaceTemplateItem} from '../../core/models/workspace-template.model';

const USER = '0a1b2c3d-0000-4000-8000-000000000001';
const STORED: WorkspaceTemplateDocument = {
  apiVersion: 'srw/v1alpha1', kind: 'WorkspaceTemplate',
  metadata: {name: 'lean', scope: {kind: 'Account', name: USER}},
  spec: {backend: 'vm', resources: {cpu: 2}, environment: {image: 'ghcr.io/srw/vm:1', prepare: [{command: ['apt-get', 'install', 'jq']}]}, retention: 'Retain'},
};
const ITEM: WorkspaceTemplateItem = {resource: STORED, uid: 'u-1', resourceVersion: 7, installationManaged: false};

function create(opts: {uid?: string | null; item?: WorkspaceTemplateItem; navState?: unknown; isAdmin?: boolean} = {}) {
  const api = {
    listWorkspaceTemplates: vi.fn().mockReturnValue(of({resources: []})),
    getProjects: vi.fn().mockReturnValue(of([])),
    getResource: vi.fn().mockReturnValue(of(opts.item ?? ITEM)),
    checkWorkspaceRecipe: vi.fn().mockReturnValue(of({})),
    applyManifest: vi.fn().mockReturnValue(of({resources: [{uid: 'u-1', resourceVersion: 8, changed: true}]})),
    deleteResource: vi.fn().mockReturnValue(of({deleted: true, uid: 'u-1'})),
  };
  const router = {navigate: vi.fn().mockResolvedValue(true), getCurrentNavigation: () => (opts.navState ? {extras: {state: opts.navState}} : null)};
  const injector = Injector.create({providers: [
    {provide: ApiService, useValue: api},
    {provide: Router, useValue: router},
    {provide: ActivatedRoute, useValue: {snapshot: {paramMap: convertToParamMap(opts.uid ? {uid: opts.uid} : {})}}},
    {provide: UserService, useValue: {currentUser: signal({id: USER, is_admin: opts.isAdmin ?? false, can_use_vm: true}), currentUserId: signal(USER)}},
    {provide: TranslocoService, useValue: {translate: (k: string) => k}},
  ]});
  const c = runInInjectionContext(injector, () => new WorkspaceTemplateEditorComponent());
  c.ngOnInit();
  return {c, api, router};
}

describe('WorkspaceTemplateEditorComponent', () => {
  it('loads a template by uid into the form and keeps unsupported parts', () => {
    const {c, api} = create({uid: 'u-1'});
    expect(api.getResource).toHaveBeenCalledWith('u-1');
    expect(c.value().name).toBe('lean');
    expect(c.preserved().hasAny).toBe(true);
  });

  it('saves an edit with the stored scope, its version, and the preserved parts (Review Focus 1 and 2)', () => {
    const {c, api} = create({uid: 'u-1'});
    c.value.update((v) => ({...v, cpu: '4'}));
    c.save();
    const [doc, versions] = api.applyManifest.mock.calls[0];
    expect(versions).toEqual({[`WorkspaceTemplate/Account/${USER}/lean`]: 7});
    expect(doc.metadata.scope).toEqual({kind: 'Account', name: USER});
    expect(doc.spec).toMatchObject({resources: {cpu: 4}, retention: 'Retain', environment: {prepare: [{command: ['apt-get', 'install', 'jq']}]}});
    expect(api.checkWorkspaceRecipe).toHaveBeenCalledBefore(api.applyManifest);
  });

  it('creates a Mine template with scope me and no versions, then opens it', () => {
    const {c, api, router} = create();
    api.applyManifest.mockReturnValue(of({resources: [{uid: 'u-new', resourceVersion: 1, changed: true}]}));
    c.value.update((v) => ({...v, name: 'fresh', image: 'ghcr.io/me/x:1'}));
    c.save();
    const [doc, versions] = api.applyManifest.mock.calls[0];
    expect(doc.metadata.scope).toEqual({kind: 'Account', name: 'me'});
    expect(versions).toBeUndefined();
    expect(router.navigate).toHaveBeenCalledWith(['/workspaces', 'u-new']);
  });

  it('does not call the server while the form is invalid', () => {
    const {c, api} = create();
    c.save();
    expect(c.showErrors()).toBe(true);
    expect(api.checkWorkspaceRecipe).not.toHaveBeenCalled();
  });

  it('stops at the admission check and shows its message', () => {
    const {c, api} = create({uid: 'u-1'});
    api.checkWorkspaceRecipe.mockReturnValue(throwError(() => new HttpErrorResponse({status: 422, error: {detail: 'VM preparation is not enabled.'}})));
    c.save();
    expect(api.applyManifest).not.toHaveBeenCalled();
    expect(c.errorMessage()).toBe('VM preparation is not enabled.');
  });

  it('maps a manifest 422 to its field', () => {
    const {c, api} = create({uid: 'u-1'});
    api.applyManifest.mockReturnValue(throwError(() => new HttpErrorResponse({status: 422, error: {detail: {code: 'SchemaInvalid', message: 'bad image', path: '/spec/environment/image'}}})));
    c.save();
    expect(c.fieldErrors()).toEqual({image: 'bad image'});
  });

  it('tells a built-in refusal from a version conflict', () => {
    const builtin = create({uid: 'u-1'});
    builtin.api.applyManifest.mockReturnValue(throwError(() => new HttpErrorResponse({status: 409, error: {detail: 'This template is managed by the installation. Duplicate it to change it.'}})));
    builtin.c.save();
    expect(builtin.c.conflict()).toEqual({kind: 'builtin', message: 'This template is managed by the installation. Duplicate it to change it.'});
    const stale = create({uid: 'u-1'});
    stale.api.applyManifest.mockReturnValue(throwError(() => new HttpErrorResponse({status: 409, error: {detail: 'Resource version changed.'}})));
    stale.c.save();
    expect(stale.c.conflict()).toEqual({kind: 'version', message: 'Resource version changed.'});
  });

  it('maps a 409 on a NEW template to the name field, not a version conflict', () => {
    const {c, api} = create();
    api.applyManifest.mockReturnValue(throwError(() => new HttpErrorResponse({status: 409, error: {detail: 'Updating a resource requires its expected resource version.'}})));
    c.value.update((v) => ({...v, name: 'fresh', image: 'ghcr.io/me/x:1'}));
    c.save();
    expect(c.conflict()).toBeNull();
    expect(c.fieldErrors()).toEqual({name: 'workspaces.errors.nameTaken'});
  });

  it('opens built-ins read-only and duplicates them into Mine', () => {
    const {c, router} = create({uid: 'b-1', item: {...ITEM, uid: 'b-1', installationManaged: true}});
    expect(c.readOnly()).toBe(true);
    c.duplicate();
    expect(router.navigate).toHaveBeenCalledWith(['/workspaces/new'], {state: {duplicateOf: STORED}});
  });

  it('starts a duplicate from router state', () => {
    const {c} = create({navState: {duplicateOf: STORED}});
    expect(c.value().name).toBe('lean-copy');
    expect(c.value().scope).toEqual({kind: 'Account', name: 'me'});
  });

  it('keeps the source preserved parts when duplicating, and saves them', () => {
    const {c, api} = create({navState: {duplicateOf: STORED}});
    expect(c.preserved().hasAny).toBe(true);
    c.value.update((v) => ({...v, name: 'lean-copy'}));
    c.save();
    const [doc] = api.applyManifest.mock.calls[0];
    expect(doc.spec).toMatchObject({retention: 'Retain', environment: {prepare: [{command: ['apt-get', 'install', 'jq']}]}});
  });

  it('deletes with the version it read', () => {
    const {c, api, router} = create({uid: 'u-1'});
    c.remove();
    expect(api.deleteResource).toHaveBeenCalledWith('u-1', 7);
    expect(router.navigate).toHaveBeenCalledWith(['/workspaces']);
  });

  it('duplicates an unsaved template in place after a built-in 409', () => {
    const {c, api, router} = create();
    api.applyManifest.mockReturnValue(throwError(() => new HttpErrorResponse({status: 409, error: {detail: 'This template is managed by the installation. Duplicate it to change it.'}})));
    c.value.update((v) => ({...v, name: 'fresh', image: 'ghcr.io/me/x:1'}));
    c.save();
    expect(c.conflict()?.kind).toBe('builtin');
    c.duplicate();
    expect(router.navigate).not.toHaveBeenCalled();
    expect(c.value().name).toBe('fresh-copy');
    expect(c.value().scope).toEqual({kind: 'Account', name: 'me'});
    expect(c.conflict()).toBeNull();
  });
});
