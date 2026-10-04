import {describe, expect, it} from 'vitest';
import {
  canEditItem, choiceBackend, choiceRequestFields, duplicateValue, emptyFormValue, errorField,
  expectedVersionKey, fromDocument, imageRepository, isSrwImage, itemKey, recommendedChoice, refChoice,
  shortImage, sizeSummary, srwImages, toDocument, validateTemplateForm,
} from './workspace-template-utils';
import {
  ACCOUNT_ME, CATALOG_SHARED, WorkspaceTemplateDocument, WorkspaceTemplateItem,
} from '../../core/models/workspace-template.model';
import {ProjectWorkspaceDefaults} from '../../core/models/api.model';

const USER = '0a1b2c3d-0000-4000-8000-000000000001';

function doc(over: Partial<WorkspaceTemplateDocument['spec']> = {}, meta: Partial<WorkspaceTemplateDocument['metadata']> = {}): WorkspaceTemplateDocument {
  return {
    apiVersion: 'srw/v1alpha1', kind: 'WorkspaceTemplate',
    metadata: {name: 'lean', scope: {kind: 'Account', name: USER}, ...meta},
    spec: {backend: 'sandbox', resources: {cpu: 1, memory: '2Gi', storage: '15Gi'}, environment: {image: 'ghcr.io/me/lean:1'}, ...over},
  };
}

function item(d: WorkspaceTemplateDocument, over: Partial<WorkspaceTemplateItem> = {}): WorkspaceTemplateItem {
  return {resource: d, uid: 'u-1', resourceVersion: 3, installationManaged: false, ...over};
}

const builtin = (name: string, backend: 'sandbox' | 'vm' | 'virtual', image?: string) =>
  item(doc({backend, environment: image ? {image} : undefined, resources: undefined}, {name, scope: CATALOG_SHARED}), {installationManaged: true, uid: name});

describe('workspace template form round trip', () => {
  it('turns a container document into form values and back unchanged', () => {
    const d = doc({resources: {cpu: 1, memory: '2Gi', storage: '15Gi', requests: {cpu: 0.25, memory: '1Gi'}},
      environment: {image: 'ghcr.io/me/lean:1', pullPolicy: 'Always'}},
    {annotations: {'srw.io/display-name': 'Lean', 'srw.io/description': 'Small'}});
    const {value, preserved} = fromDocument(d);
    expect(value).toMatchObject({name: 'lean', displayName: 'Lean', description: 'Small', backend: 'sandbox',
      image: 'ghcr.io/me/lean:1', cpu: '1', memory: '2Gi', storage: '15Gi', requestCpu: '0.25', requestMemory: '1Gi', pullPolicy: 'Always'});
    expect(toDocument(value, preserved)).toEqual(d);
  });

  it('keeps parts the form cannot edit byte-identical (Review Focus 1)', () => {
    const d = doc({backend: 'vm', resources: {cpu: 4, memory: '8Gi', storage: '40Gi'},
      environment: {image: 'ghcr.io/srw/vm:1', prepare: [{command: ['apt-get', 'install', '-y', 'jq']}], cache: 'Rebuild'},
      initialize: [{command: ['make', 'bootstrap']}], retention: 'Retain'},
    {labels: {team: 'a'}, tags: ['x'], annotations: {'other.io/k': 'v'}});
    const {value, preserved} = fromDocument(d);
    expect(value.setupLines).toBe('');
    expect(preserved.hasAny).toBe(true);
    expect(toDocument(value, preserved)).toEqual(d);
  });

  it('turns setup lines into sh -c steps and back', () => {
    const d = doc({backend: 'vm', resources: {cpu: 2}, environment: {image: 'ghcr.io/srw/vm:1'},
      initialize: [{command: ['/bin/sh', '-c', 'pip install --user ruff']}, {command: ['/bin/sh', '-c', 'make']}]});
    const {value, preserved} = fromDocument(d);
    expect(value.setupLines).toBe('pip install --user ruff\nmake');
    expect(preserved.hasAny).toBe(false);
    expect(toDocument({...value, setupLines: 'make\n\n  ls -la  \n'}, preserved).spec.initialize)
      .toEqual([{command: ['/bin/sh', '-c', 'make']}, {command: ['/bin/sh', '-c', 'ls -la']}]);
  });

  it.each([
    ['a multi-line script', 'if true; then\n  make\nfi'],
    ['leading and trailing whitespace', '  make  '],
  ])('keeps a stored shell step with %s byte-identical', (_label, script) => {
    const d = doc({backend: 'vm', resources: {cpu: 2}, environment: {image: 'ghcr.io/srw/vm:1'},
      initialize: [{command: ['/bin/sh', '-c', 'echo first']}, {command: ['/bin/sh', '-c', script]}]});
    const {value, preserved} = fromDocument(d);
    expect(value.setupLines).toBe('');
    expect(preserved.hasAny).toBe(true);
    expect(toDocument(value, preserved)).toEqual(d);
  });

  it('builds a virtual template with no environment or sizes', () => {
    const value = {...emptyFormValue(ACCOUNT_ME), name: 'files', backend: 'virtual' as const, image: 'ignored', cpu: '2'};
    expect(toDocument(value, fromDocument(doc()).preserved).spec).toEqual({backend: 'virtual'});
  });

  it('drops requests and pull policy for VMs', () => {
    const value = {...emptyFormValue(ACCOUNT_ME), name: 'v', backend: 'vm' as const, image: 'i', cpu: '2', requestCpu: '1', pullPolicy: 'Always' as const};
    const spec = toDocument(value, {metadata: {}, spec: {}, hasAny: false}).spec;
    expect(spec.resources).toEqual({cpu: 2});
    expect(spec.environment).toEqual({image: 'i'});
  });
});

describe('validateTemplateForm', () => {
  const ok = {...emptyFormValue(ACCOUNT_ME), name: 'lean', image: 'ghcr.io/me/lean:1'};
  it('accepts a minimal container template', () => expect(validateTemplateForm(ok)).toEqual({}));
  it.each([
    [{name: 'Lean'}, 'name'], [{name: ''}, 'name'], [{image: ''}, 'image'], [{cpu: '0'}, 'cpu'],
    [{memory: '2GB'}, 'memory'], [{storage: '0Gi'}, 'storage'], [{cpu: '2', requestCpu: '3'}, 'requestCpu'],
    [{memory: '1Gi', requestMemory: '2Gi'}, 'requestMemory'],
  ])('flags %o on %s', (change, field) => {
    expect(Object.keys(validateTemplateForm({...ok, ...change}))).toContain(field);
  });
  it('requires whole CPUs for VMs', () => {
    expect(validateTemplateForm({...ok, backend: 'vm', cpu: '1.5'})).toHaveProperty('cpu', 'workspaces.errors.cpuWhole');
  });
  it('limits setup steps to 32', () => {
    expect(validateTemplateForm({...ok, backend: 'vm', setupLines: Array(33).fill('true').join('\n')})).toHaveProperty('setupLines');
  });
  it('needs no image for a virtual template', () => {
    expect(validateTemplateForm({...ok, backend: 'virtual', image: ''})).toEqual({});
  });
});

describe('display and keys', () => {
  it('summarises sizes', () => expect(sizeSummary(doc().spec)).toBe('1 CPU · 2Gi · 15Gi'));
  it('shortens images', () => {
    expect(shortImage('ghcr.io/org/srw-workspace-minimal:sha-abc')).toBe('srw-workspace-minimal:sha-abc');
    expect(shortImage('ghcr.io/org/x@sha256:0123456789abcdef0123')).toBe('x@sha256:0123456789ab');
  });
  it('finds the repository of tagged, digested and ported references', () => {
    expect(imageRepository('ghcr.io/o/x:1')).toBe('ghcr.io/o/x');
    expect(imageRepository('ghcr.io/o/x@sha256:ab')).toBe('ghcr.io/o/x');
    expect(imageRepository('registry:5000/x')).toBe('registry:5000/x');
  });
  it('treats any tag of an SRW base as SRW', () => {
    const images = srwImages([builtin('container-full', 'sandbox', 'ghcr.io/o/srw-workspace:sha-1'), builtin('virtual', 'virtual')]);
    expect(images.sandbox).toEqual(['ghcr.io/o/srw-workspace:sha-1']);
    expect(isSrwImage('ghcr.io/o/srw-workspace:sha-2', images.sandbox ?? [])).toBe(true);
    expect(isSrwImage('ghcr.io/me/lean:1', images.sandbox ?? [])).toBe(false);
  });
  it('keys expected versions by the stored scope (Review Focus 2)', () => {
    expect(expectedVersionKey(doc())).toBe(`WorkspaceTemplate/Account/${USER}/lean`);
    expect(itemKey(item(doc()))).toBe(`Account/${USER}/lean`);
  });
  it('maps JSON pointers to form fields', () => {
    expect(errorField('/spec/environment/image')).toBe('image');
    expect(errorField('/spec/resources/requests/cpu')).toBe('requestCpu');
    expect(errorField('/metadata/name')).toBe('name');
    expect(errorField('/spec/initialize/0/command')).toBe('setupLines');
    expect(errorField('/')).toBe('form');
  });
});

describe('permissions and duplicates', () => {
  it('never lets anyone edit a built-in', () => {
    expect(canEditItem(builtin('container-full', 'sandbox', 'i'), {id: USER, is_admin: true})).toBe(false);
  });
  it('lets admins edit Shared, owners edit Mine, and leaves Projects to the server', () => {
    const shared = item(doc({}, {scope: CATALOG_SHARED}));
    expect(canEditItem(shared, {id: USER, is_admin: false})).toBe(false);
    expect(canEditItem(shared, {id: USER, is_admin: true})).toBe(true);
    expect(canEditItem(item(doc()), {id: USER})).toBe(true);
    expect(canEditItem(item(doc()), {id: 'someone-else'})).toBe(false);
    expect(canEditItem(item(doc({}, {scope: {kind: 'Project', name: 'p-1'}})), {id: USER})).toBe(true);
  });
  it('duplicates into Mine with a -copy name capped at 63 characters', () => {
    const value = duplicateValue(doc({}, {name: 'a'.repeat(62), annotations: {'srw.io/display-name': 'Lean'}}), ACCOUNT_ME);
    expect(value.name).toHaveLength(63);
    expect(value.name.endsWith('-copy')).toBe(true);
    expect(value.displayName).toBe('Lean (copy)');
    expect(value.scope).toEqual(ACCOUNT_ME);
  });
});

describe('choices', () => {
  it('maps each choice to the request field', () => {
    expect(choiceRequestFields({kind: 'default'})).toEqual({});
    expect(choiceRequestFields({kind: 'none'})).toEqual({workspace: null});
    expect(choiceRequestFields(refChoice(item(doc())))).toEqual({workspace: {template: {ref: {name: 'lean', scope: {kind: 'Account', name: USER}}}}});
    expect(choiceRequestFields({kind: 'inline', spec: {backend: 'vm'}})).toEqual({workspace: {template: {inline: {backend: 'vm'}}}});
  });
  it('reports the backend a choice will run on', () => {
    expect(choiceBackend({kind: 'default'}, {backend: 'virtual', source: 'default', binding: null})).toBe('virtual');
    expect(choiceBackend({kind: 'default'}, null)).toBeNull();
    expect(choiceBackend({kind: 'none'}, null)).toBe('none');
    expect(choiceBackend(refChoice(item(doc())), null)).toBe('sandbox');
  });
});

describe('recommendedChoice', () => {
  const shared = [builtin('virtual', 'virtual'), builtin('container-full', 'sandbox', 'i'), builtin('vm-full', 'vm', 'v')];
  const defaults = (over: Partial<ProjectWorkspaceDefaults['effective']> = {}, stored: Partial<ProjectWorkspaceDefaults['stored']> = {}): ProjectWorkspaceDefaults => ({
    stored: {jobs: null, sessions: null, container: null, vm: null, ...stored},
    managed_by_manifest: false, can_edit: true, account_templates: false,
    effective: {jobs: {mode: 'container', source: 'installation'}, sessions: {mode: 'virtual', source: 'installation'},
      container: {template_name: 'container-full', source: 'builtin'}, vm: {template_name: 'vm-full', source: 'builtin'}, ...over},
    installation: {jobs: 'container', sessions: 'virtual', container: 'container-full', vm: 'vm-full'},
    template_problems: {}, installation_problems: [], vm_available: true,
  });
  const ctx = (over = {}) => ({role: 'session' as const, defaults: null, shared, vmAllowed: true, ...over});

  it('lets a Project default win', () => {
    expect(recommendedChoice('vm', ctx({defaults: defaults({sessions: {mode: 'container', source: 'project'}})}))).toBeNull();
  });
  it('maps none and virtual', () => {
    expect(recommendedChoice('none', ctx())).toEqual({kind: 'none'});
    expect(recommendedChoice('virtual', ctx())).toMatchObject({kind: 'ref', ref: {name: 'virtual', scope: CATALOG_SHARED}});
  });
  it("uses the Project's own container template when the Project set it", () => {
    const ref = {name: 'web', scope: {kind: 'Project', name: 'p-1'}};
    expect(recommendedChoice('sandbox', ctx({defaults: defaults({container: {template_name: 'web', source: 'project'}}, {container: {ref}})})))
      .toEqual({kind: 'ref', ref, backend: 'sandbox', label: 'web'});
  });
  it('uses a manifest-pinned inline template as an inline choice', () => {
    const inline = {backend: 'sandbox' as const, environment: {image: 'x'}};
    expect(recommendedChoice('sandbox', ctx({defaults: defaults({container: {template_name: null, source: 'project'}}, {container: {inline}})})))
      .toEqual({kind: 'inline', spec: inline});
  });
  it('falls back to the installation or built-in template by name in Shared', () => {
    expect(recommendedChoice('sandbox', ctx({defaults: defaults()}))).toMatchObject({kind: 'ref', ref: {name: 'container-full', scope: CATALOG_SHARED}});
    expect(recommendedChoice('vm', ctx())).toMatchObject({kind: 'ref', ref: {name: 'vm-full'}});
  });
  it('never selects a VM the user cannot run (Review Focus 3)', () => {
    expect(recommendedChoice('vm', ctx({vmAllowed: false}))).toBeNull();
  });
});
