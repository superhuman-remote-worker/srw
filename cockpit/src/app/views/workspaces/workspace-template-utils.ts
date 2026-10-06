import {
  CATALOG_SHARED, ResourceScope, TemplateRef, WorkspaceBackend, WorkspaceBinding, WorkspaceChoice,
  WorkspaceStep, WorkspaceTemplateDocument, WorkspaceTemplateItem, WorkspaceTemplateSpec,
} from '../../core/models/workspace-template.model';
import {ProjectWorkspaceDefaults} from '../../core/models/api.model';
import {WorkspacePreview} from '../../core/models/workspace.model';

export const DISPLAY_NAME = 'srw.io/display-name';
export const DESCRIPTION = 'srw.io/description';
const NAME_PATTERN = /^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/;
const QUANTITY_PATTERN = /^[1-9][0-9]*(Mi|Gi|Ti)$/;
const CPU_PATTERN = /^(?:[0-9]+(?:\.[0-9]+)?|\.[0-9]+)$/;
const SHELL = ['/bin/sh', '-c'];
const MAX_SETUP_STEPS = 32;

/** Template content is plain JSON; jsdom (vitest) doesn't always provide structuredClone. */
const clone = <T>(value: T): T => JSON.parse(JSON.stringify(value)) as T;

/** The editor's fields. Numbers stay text until `toDocument`, so a half-typed value never throws. */
export interface TemplateFormValue {
  name: string;
  displayName: string;
  description: string;
  scope: ResourceScope;
  backend: WorkspaceBackend;
  image: string;
  cpu: string;
  memory: string;
  storage: string;
  requestCpu: string;
  requestMemory: string;
  pullPolicy: '' | 'IfNotPresent' | 'Always' | 'Never';
  setupLines: string;
}

/** Parts of a stored document the form doesn't edit. They are kept verbatim on save. */
export interface PreservedParts {
  metadata: {labels?: Record<string, string>; annotations?: Record<string, string>; tags?: string[]};
  spec: Partial<Pick<WorkspaceTemplateSpec, 'retention' | 'network' | 'initialize'>> & {
    prepare?: WorkspaceStep[];
    cache?: 'Reuse' | 'Rebuild';
  };
  hasAny: boolean;
}

export type FormField = keyof TemplateFormValue | 'form';

export function emptyFormValue(scope: ResourceScope): TemplateFormValue {
  return {
    name: '', displayName: '', description: '', scope, backend: 'sandbox', image: '',
    cpu: '', memory: '', storage: '', requestCpu: '', requestMemory: '', pullPolicy: '', setupLines: '',
  };
}

function isShellStep(step: WorkspaceStep): boolean {
  if (step.command.length !== 3 || step.command[0] !== SHELL[0] || step.command[1] !== SHELL[1]) return false;
  // Only single-line, already-trimmed scripts survive the newline-joined textarea unchanged.
  const script = step.command[2];
  return script !== '' && !script.includes('\n') && script === script.trim();
}

const text = (n: number | undefined): string => (n === undefined ? '' : String(n));

export function fromDocument(doc: WorkspaceTemplateDocument): {value: TemplateFormValue; preserved: PreservedParts} {
  const {spec, metadata} = doc;
  const annotations = {...(metadata.annotations ?? {})};
  const displayName = annotations[DISPLAY_NAME] ?? '';
  const description = annotations[DESCRIPTION] ?? '';
  delete annotations[DISPLAY_NAME];
  delete annotations[DESCRIPTION];
  const preserved: PreservedParts = {metadata: {}, spec: {}, hasAny: false};
  if (Object.keys(annotations).length) preserved.metadata.annotations = annotations;
  if (metadata.labels) preserved.metadata.labels = {...metadata.labels};
  if (metadata.tags) preserved.metadata.tags = [...metadata.tags];
  if (spec.retention) preserved.spec.retention = spec.retention;
  if (spec.network) preserved.spec.network = clone(spec.network);
  if (spec.environment?.prepare) preserved.spec.prepare = clone(spec.environment.prepare);
  if (spec.environment?.cache) preserved.spec.cache = spec.environment.cache;
  const steps = spec.initialize ?? [];
  const shellOnly = spec.backend === 'vm' && steps.every(isShellStep);
  if (steps.length && !shellOnly) preserved.spec.initialize = clone(steps);
  preserved.hasAny = Object.keys(preserved.spec).length > 0 || Object.keys(preserved.metadata).length > 0;
  const value: TemplateFormValue = {
    name: metadata.name,
    displayName,
    description,
    scope: {...metadata.scope},
    backend: spec.backend,
    image: spec.environment?.image ?? '',
    cpu: text(spec.resources?.cpu),
    memory: spec.resources?.memory ?? '',
    storage: spec.resources?.storage ?? '',
    requestCpu: text(spec.resources?.requests?.cpu),
    requestMemory: spec.resources?.requests?.memory ?? '',
    pullPolicy: spec.environment?.pullPolicy ?? '',
    setupLines: shellOnly ? steps.map((s) => s.command[2]).join('\n') : '',
  };
  return {value, preserved};
}

function setupSteps(lines: string): WorkspaceStep[] {
  return lines.split('\n').map((l) => l.trim()).filter(Boolean).map((l) => ({command: [...SHELL, l]}));
}

export function toDocument(value: TemplateFormValue, preserved: PreservedParts): WorkspaceTemplateDocument {
  const spec: WorkspaceTemplateSpec = {backend: value.backend};
  if (value.backend !== 'virtual') {
    const resources: NonNullable<WorkspaceTemplateSpec['resources']> = {};
    if (value.cpu.trim()) resources.cpu = Number(value.cpu);
    if (value.memory.trim()) resources.memory = value.memory.trim();
    if (value.storage.trim()) resources.storage = value.storage.trim();
    if (value.backend === 'sandbox') {
      const requests: {cpu?: number; memory?: string} = {};
      if (value.requestCpu.trim()) requests.cpu = Number(value.requestCpu);
      if (value.requestMemory.trim()) requests.memory = value.requestMemory.trim();
      if (Object.keys(requests).length) resources.requests = requests;
    }
    if (Object.keys(resources).length) spec.resources = resources;
    const environment: NonNullable<WorkspaceTemplateSpec['environment']> = {image: value.image.trim()};
    if (value.backend === 'sandbox' && value.pullPolicy) environment.pullPolicy = value.pullPolicy;
    if (preserved.spec.prepare) environment.prepare = clone(preserved.spec.prepare);
    if (preserved.spec.cache) environment.cache = preserved.spec.cache;
    spec.environment = environment;
  }
  if (value.backend !== 'virtual' && preserved.spec.initialize) spec.initialize = clone(preserved.spec.initialize);
  else if (value.backend === 'vm') {
    const steps = setupSteps(value.setupLines);
    if (steps.length) spec.initialize = steps;
  }
  if (preserved.spec.retention) spec.retention = preserved.spec.retention;
  if (preserved.spec.network) spec.network = clone(preserved.spec.network);
  const annotations = {...(preserved.metadata.annotations ?? {})};
  if (value.displayName.trim()) annotations[DISPLAY_NAME] = value.displayName.trim();
  if (value.description.trim()) annotations[DESCRIPTION] = value.description.trim();
  const metadata: WorkspaceTemplateDocument['metadata'] = {name: value.name.trim(), scope: {...value.scope}};
  if (preserved.metadata.labels) metadata.labels = {...preserved.metadata.labels};
  if (Object.keys(annotations).length) metadata.annotations = annotations;
  if (preserved.metadata.tags) metadata.tags = [...preserved.metadata.tags];
  return {apiVersion: 'srw/v1alpha1', kind: 'WorkspaceTemplate', metadata, spec};
}

function quantityMi(q: string): number {
  const m = QUANTITY_PATTERN.exec(q.trim());
  if (!m) return NaN;
  const n = Number(q.trim().slice(0, -2));
  return m[1] === 'Mi' ? n : m[1] === 'Gi' ? n * 1024 : n * 1024 * 1024;
}

/** Field → i18n key under `workspaces.errors`. Empty means valid. */
export function validateTemplateForm(v: TemplateFormValue): Partial<Record<FormField, string>> {
  const e: Partial<Record<FormField, string>> = {};
  if (!NAME_PATTERN.test(v.name.trim())) e.name = 'workspaces.errors.name';
  if (v.backend !== 'virtual') {
    if (!v.image.trim()) e.image = 'workspaces.errors.image';
    const cpu = Number(v.cpu);
    if (v.cpu.trim() && !(CPU_PATTERN.test(v.cpu.trim()) && cpu > 0)) e.cpu = 'workspaces.errors.cpu';
    else if (v.cpu.trim() && v.backend === 'vm' && !Number.isInteger(cpu)) e.cpu = 'workspaces.errors.cpuWhole';
    for (const f of ['memory', 'storage'] as const) {
      if (v[f].trim() && !QUANTITY_PATTERN.test(v[f].trim())) e[f] = 'workspaces.errors.quantity';
    }
    if (v.backend === 'sandbox') {
      const req = Number(v.requestCpu);
      if (v.requestCpu.trim() && (!(CPU_PATTERN.test(v.requestCpu.trim()) && req > 0) || (v.cpu.trim() && req > cpu))) e.requestCpu = 'workspaces.errors.requestCpu';
      if (v.requestMemory.trim()) {
        const reqMi = quantityMi(v.requestMemory);
        if (Number.isNaN(reqMi)) e.requestMemory = 'workspaces.errors.quantity';
        else if (v.memory.trim() && reqMi > quantityMi(v.memory)) e.requestMemory = 'workspaces.errors.requestMemory';
      }
    }
    if (v.backend === 'vm' && setupSteps(v.setupLines).length > MAX_SETUP_STEPS) e.setupLines = 'workspaces.errors.setupSteps';
  }
  return e;
}

export function displayName(doc: WorkspaceTemplateDocument): string {
  return doc.metadata.annotations?.[DISPLAY_NAME] || doc.metadata.name;
}

export function templateDescription(doc: WorkspaceTemplateDocument): string {
  return doc.metadata.annotations?.[DESCRIPTION] ?? '';
}

export function sizeSummary(spec: WorkspaceTemplateSpec): string {
  const r = spec.resources ?? {};
  return [r.cpu !== undefined ? `${r.cpu} CPU` : '', r.memory ?? '', r.storage ?? ''].filter(Boolean).join(' · ');
}

export function imageRepository(ref: string): string {
  const noDigest = ref.split('@')[0];
  const slash = noDigest.lastIndexOf('/');
  const colon = noDigest.lastIndexOf(':');
  return colon > slash ? noDigest.slice(0, colon) : noDigest;
}

export function shortImage(ref: string): string {
  const [path, digest] = ref.split('@');
  const last = path.slice(path.lastIndexOf('/') + 1);
  return digest ? `${last}@${digest.slice(0, 'sha256:'.length + 12)}` : last;
}

/** The SRW-published images per backend, read from the installation-managed built-ins. */
export function srwImages(items: WorkspaceTemplateItem[]): Partial<Record<WorkspaceBackend, string[]>> {
  const out: Partial<Record<WorkspaceBackend, string[]>> = {};
  for (const i of items) {
    const image = i.resource.spec.environment?.image;
    if (!i.installationManaged || !image) continue;
    const list = (out[i.resource.spec.backend] ??= []);
    if (!list.includes(image)) list.push(image);
  }
  return out;
}

export function isSrwImage(image: string, images: string[]): boolean {
  const repo = imageRepository(image.trim());
  return images.some((i) => imageRepository(i) === repo);
}

export function expectedVersionKey(doc: WorkspaceTemplateDocument): string {
  const s = doc.metadata.scope;
  return `WorkspaceTemplate/${s.kind}/${s.name}/${doc.metadata.name}`;
}

export function itemKey(item: WorkspaceTemplateItem): string {
  const m = item.resource.metadata;
  return `${m.scope.kind}/${m.scope.name}/${m.name}`;
}

/** UI hint only; the server decides. Project rows are left to the server's 403. */
export function canEditItem(item: WorkspaceTemplateItem, user: {id?: string; is_admin?: boolean} | null): boolean {
  if (item.installationManaged || !user) return false;
  const scope = item.resource.metadata.scope;
  if (scope.kind === 'Catalog') return !!user.is_admin;
  if (scope.kind === 'Account') return scope.name === user.id || scope.name === 'me';
  return true;
}

export function refChoice(item: WorkspaceTemplateItem): WorkspaceChoice {
  const m = item.resource.metadata;
  return {kind: 'ref', ref: {name: m.name, scope: {...m.scope}}, backend: item.resource.spec.backend, label: displayName(item.resource)};
}

export function duplicateValue(doc: WorkspaceTemplateDocument, scope: ResourceScope): TemplateFormValue {
  const {value} = fromDocument(doc);
  const base = doc.metadata.name.slice(0, 63 - '-copy'.length).replace(/-+$/, '');
  return {...value, name: `${base}-copy`, displayName: value.displayName ? `${value.displayName} (copy)` : '', scope};
}

/** A 422's JSON pointer → the form field it belongs to. */
export function errorField(pointer: string | undefined): FormField {
  const p = pointer ?? '/';
  if (p.startsWith('/metadata/name')) return 'name';
  if (p.startsWith('/spec/environment/image')) return 'image';
  if (p.startsWith('/spec/environment/pullPolicy')) return 'pullPolicy';
  if (p.startsWith('/spec/resources/requests/cpu')) return 'requestCpu';
  if (p.startsWith('/spec/resources/requests/memory')) return 'requestMemory';
  if (p.startsWith('/spec/resources/cpu')) return 'cpu';
  if (p.startsWith('/spec/resources/memory')) return 'memory';
  if (p.startsWith('/spec/resources/storage')) return 'storage';
  if (p.startsWith('/spec/initialize')) return 'setupLines';
  if (p.startsWith('/spec/backend')) return 'backend';
  return 'form';
}

export interface RecommendationContext {
  role: 'job' | 'session';
  defaults: ProjectWorkspaceDefaults | null;
  shared: WorkspaceTemplateItem[];
  vmAllowed: boolean;
}

function sharedByName(shared: WorkspaceTemplateItem[], name: string): WorkspaceChoice | null {
  const found = shared.find((i) => i.resource.metadata.name === name);
  return found ? refChoice(found) : null;
}

/** An Expert's advisory tier → the concrete choice the picker preselects, or null. */
export function recommendedChoice(
  pref: 'none' | 'virtual' | 'sandbox' | 'vm' | null,
  ctx: RecommendationContext,
): WorkspaceChoice | null {
  if (!pref) return null;
  const roleKey = ctx.role === 'job' ? 'jobs' : 'sessions';
  if (ctx.defaults && ctx.defaults.effective[roleKey].source === 'project') return null;
  if (pref === 'none') return {kind: 'none'};
  if (pref === 'virtual') return sharedByName(ctx.shared, 'virtual');
  if (pref === 'vm' && !ctx.vmAllowed) return null;
  const tier = pref === 'sandbox' ? 'container' : 'vm';
  if (ctx.defaults) {
    const effective = ctx.defaults.effective[tier];
    const stored = ctx.defaults.stored[tier] as {ref?: TemplateRef; inline?: WorkspaceTemplateSpec} | null;
    if (effective.source === 'project' && stored?.ref) {
      return {kind: 'ref', ref: stored.ref, backend: pref, label: stored.ref.name};
    }
    if (effective.source === 'project' && stored?.inline) return {kind: 'inline', spec: stored.inline};
    if (effective.template_name) {
      return {kind: 'ref', ref: {name: effective.template_name, scope: {...CATALOG_SHARED}}, backend: pref, label: effective.template_name};
    }
  }
  return sharedByName(ctx.shared, tier === 'container' ? 'container-full' : 'vm-full');
}

export function choiceRequestFields(choice: WorkspaceChoice): {workspace?: WorkspaceBinding} {
  switch (choice.kind) {
    case 'default': return {};
    case 'none': return {workspace: null};
    case 'ref': return {workspace: {template: {ref: {name: choice.ref.name, scope: {...choice.ref.scope}}}}};
    case 'inline': return {workspace: {template: {inline: clone(choice.spec)}}};
  }
}

export function choiceBackend(choice: WorkspaceChoice, preview: WorkspacePreview | null | undefined): string | null {
  switch (choice.kind) {
    case 'default': return preview?.backend ?? null;
    case 'none': return 'none';
    case 'ref': return choice.backend;
    case 'inline': return choice.spec.backend;
  }
}
