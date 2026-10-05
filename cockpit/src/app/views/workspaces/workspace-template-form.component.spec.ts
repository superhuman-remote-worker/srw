import {describe, expect, it} from 'vitest';
import {TestBed} from '@angular/core/testing';
import {TranslocoService} from '@jsverse/transloco';
import {WorkspaceTemplateFormComponent} from './workspace-template-form.component';
import {emptyFormValue} from './workspace-template-utils';
import {ACCOUNT_ME} from '../../core/models/workspace-template.model';

function create(inputs: Record<string, unknown> = {}) {
  TestBed.resetTestingModule();
  TestBed.configureTestingModule({
    providers: [
      WorkspaceTemplateFormComponent,
      {provide: TranslocoService, useValue: {translate: (k: string) => `t:${k}`}},
    ],
  });
  const c = TestBed.inject(WorkspaceTemplateFormComponent);
  for (const [name, value] of Object.entries(inputs)) Object.defineProperty(c, name, {value: () => value});
  c.value.set({...emptyFormValue(ACCOUNT_ME), name: 'lean', image: 'ghcr.io/me/lean:1'});
  return c;
}

describe('WorkspaceTemplateFormComponent', () => {
  it('flags an image that is not one of SRW’s bases', () => {
    const c = create({srwImages: {sandbox: ['ghcr.io/o/srw-workspace:sha-1']}});
    expect(c.customImage()).toBe(true);
    c.patch({image: 'ghcr.io/o/srw-workspace:sha-9'});
    expect(c.customImage()).toBe(false);
  });

  it('never shows the custom-image notice for a virtual template', () => {
    const c = create();
    c.patch({backend: 'virtual'});
    expect(c.customImage()).toBe(false);
  });

  it('refuses the VM tier when VMs are not allowed (Review Focus 3)', () => {
    const c = create({vmAllowed: false});
    c.setBackend('vm');
    expect(c.value().backend).toBe('sandbox');
    const allowed = create({vmAllowed: true});
    allowed.setBackend('vm');
    expect(allowed.value().backend).toBe('vm');
  });

  it('switches the save target by option key', () => {
    const c = create({scopeOptions: [
      {key: 'Account/me', scope: ACCOUNT_ME, label: 'Mine'},
      {key: 'Project/p-1', scope: {kind: 'Project', name: 'p-1'}, label: 'P'},
    ]});
    c.setScope('Project/p-1');
    expect(c.value().scope).toEqual({kind: 'Project', name: 'p-1'});
  });

  it('shows client errors only after a save attempt, server errors always', () => {
    const quiet = create({showErrors: false, serverErrors: {}});
    quiet.patch({name: 'Bad Name'});
    expect(quiet.fieldError('name')).toBe('');
    const loud = create({showErrors: true, serverErrors: {}});
    loud.patch({name: 'Bad Name'});
    expect(loud.fieldError('name')).toBe('t:workspaces.errors.name');
    const server = create({showErrors: false, serverErrors: {image: 'Image refused'}});
    expect(server.fieldError('image')).toBe('Image refused');
  });
});
