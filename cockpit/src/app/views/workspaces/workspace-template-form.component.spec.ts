import {Component, EventEmitter, Input, Output, signal, ɵresolveComponentResources} from '@angular/core';
import {TestBed} from '@angular/core/testing';
import {beforeAll, describe, expect, it} from 'vitest';
import {TranslocoTestingModule} from '@jsverse/transloco';
import {AppFormFieldComponent} from '../../ui/form-field';
import {AppInputComponent} from '../../ui/input';
import {AppSelectComponent} from '../../ui/select';
import {AppTextareaComponent} from '../../ui/textarea';
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

  it('does not flag a custom image before the SRW images have loaded', () => {
    const loading = create({srwImages: {}});
    expect(loading.customImage()).toBe(false);
    const loaded = create({srwImages: {sandbox: ['ghcr.io/o/srw-workspace:sha-1']}});
    expect(loaded.customImage()).toBe(true);
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

/**
 * Render tests. This vitest pipeline does not compile signal inputs, so child
 * bindings to the real ui components are inert (see ui/multi-select spec). The
 * stubs below use decorator inputs, which do bind, and render the same native
 * `id` / `for` / `disabled` attributes the real components do, so the specs
 * prove the form's own wiring.
 */
@Component({selector: 'app-form-field', standalone: true,
  template: '<label class="app-form-field__label" [attr.for]="forId || null">{{ label }}</label><ng-content />'})
class StubFormField {
  @Input() label = '';
  @Input() forId = '';
  @Input() required = false;
  @Input() hint = '';
  @Input() error = '';
}

@Component({selector: 'app-input', standalone: true,
  template: '<input [attr.id]="inputId || null" [disabled]="disabled" [attr.list]="list || null" />'})
class StubInput {
  @Input() value = '';
  @Input() disabled = false;
  @Input() inputId = '';
  @Input() list = '';
  @Input() placeholder = '';
  @Input() inputmode = '';
  @Output() valueChange = new EventEmitter<string>();
}

@Component({selector: 'app-select', standalone: true,
  template: '<select [attr.id]="inputId || null" [disabled]="disabled"><ng-content /></select>'})
class StubSelect {
  @Input() value: string | null = null;
  @Input() disabled = false;
  @Input() inputId = '';
  @Output() changed = new EventEmitter<string | null>();
}

@Component({selector: 'app-textarea', standalone: true,
  template: '<textarea [attr.id]="inputId || null" [disabled]="disabled"></textarea>'})
class StubTextarea {
  @Input() value = '';
  @Input() disabled = false;
  @Input() inputId = '';
  @Input() rows = 3;
  @Output() valueChange = new EventEmitter<string>();
}

function render(inputs: Record<string, unknown> = {}, patch: Partial<ReturnType<typeof emptyFormValue>> = {}) {
  TestBed.resetTestingModule();
  TestBed.configureTestingModule({
    imports: [WorkspaceTemplateFormComponent, TranslocoTestingModule.forRoot({langs: {en: {}}, translocoConfig: {availableLangs: ['en'], defaultLang: 'en'}})],
  });
  TestBed.overrideComponent(WorkspaceTemplateFormComponent, {
    remove: {imports: [AppFormFieldComponent, AppInputComponent, AppSelectComponent, AppTextareaComponent]},
    add: {imports: [StubFormField, StubInput, StubSelect, StubTextarea]},
  });
  const fixture = TestBed.createComponent(WorkspaceTemplateFormComponent);
  const c = fixture.componentInstance as unknown as Record<string, unknown>;
  for (const [name, value] of Object.entries(inputs)) c[name] = signal(value);
  fixture.componentInstance.value.set({...emptyFormValue(ACCOUNT_ME), name: 'lean', image: 'ghcr.io/me/lean:1', ...patch});
  fixture.detectChanges();
  return fixture.nativeElement as HTMLElement;
}

const scopeOptions = [{key: 'Account/me', scope: ACCOUNT_ME, label: 'Mine'}];

describe('WorkspaceTemplateForm rendering', () => {
  beforeAll(async () => {
    // The real ui components (still imported before the override) use external styleUrls.
    await ɵresolveComponentResources(() => Promise.resolve(''));
  });

  it('associates every label with its control', () => {
    for (const backend of ['sandbox', 'vm'] as const) {
      const root = render({scopeOptions}, {backend});
      // Open the advanced section's contents too: they render regardless of <details> state.
      const labels = Array.from(root.querySelectorAll<HTMLLabelElement>('label.app-form-field__label'));
      expect(labels.length).toBeGreaterThanOrEqual(10);
      for (const label of labels) {
        expect(label.htmlFor, label.textContent ?? '').not.toBe('');
        const control = root.querySelector(`[id="${label.htmlFor}"]`);
        expect(control, label.htmlFor).not.toBeNull();
        expect(['INPUT', 'SELECT', 'TEXTAREA']).toContain(control!.tagName);
      }
      const ids = Array.from(root.querySelectorAll('input[id], select[id], textarea[id]')).map((e) => e.id);
      expect(new Set(ids).size).toBe(ids.length);
    }
  });

  it('gives two form instances different ids', () => {
    const a = render({scopeOptions}).querySelector('label')!.htmlFor;
    const b = render({scopeOptions}).querySelector('label')!.htmlFor;
    expect(a).not.toBe(b);
  });

  it('disables every control when readOnly', () => {
    const root = render({scopeOptions, readOnly: true}, {backend: 'vm'});
    const controls = Array.from(root.querySelectorAll<HTMLInputElement>('input, select, textarea'));
    expect(controls.length).toBeGreaterThan(8);
    expect(controls.filter((e) => !e.disabled)).toEqual([]);
  });

  it('locks name and Save to when identityLocked but keeps the other fields editable', () => {
    const root = render({scopeOptions, identityLocked: true});
    const byLabel = (key: string) => {
      const label = Array.from(root.querySelectorAll<HTMLLabelElement>('label')).find((l) => l.textContent === key)!;
      return root.querySelector<HTMLInputElement>(`[id="${label.htmlFor}"]`)!;
    };
    expect(byLabel('workspaces.form.name').disabled).toBe(true);
    expect(byLabel('workspaces.form.saveTo').disabled).toBe(true);
    for (const key of ['displayName', 'description', 'tier', 'image', 'cpu', 'memory', 'storage']) {
      expect(byLabel(`workspaces.form.${key}`).disabled, key).toBe(false);
    }
  });
});
