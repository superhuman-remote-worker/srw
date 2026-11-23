import {Component, EventEmitter, NO_ERRORS_SCHEMA, Output, signal} from '@angular/core';
import {provideHttpClient} from '@angular/common/http';
import {provideHttpClientTesting} from '@angular/common/http/testing';
import {TestBed} from '@angular/core/testing';
import {By} from '@angular/platform-browser';
import {ActivatedRoute, Router, convertToParamMap} from '@angular/router';
import {TranslocoPipe, TranslocoTestingModule} from '@jsverse/transloco';
import {of} from 'rxjs';
import {afterEach, describe, expect, it, vi} from 'vitest';
import en from '../../../assets/i18n/en.json';
import {ApiService} from '../../core/services/api.service';
import {CapabilitiesService} from '../../core/services/capabilities.service';
import {ErrorMessageService} from '../../core/services/error-message.service';
import {ModelService} from '../../core/services/model.service';
import {UserService} from '../../core/services/user.service';
import {ViewportService} from '../../core/services/viewport.service';
import {SessionCreateComponent} from '../session-create/session-create.component';
import {JobCreateComponent} from './job-create.component';

/**
 * A real click on Create after typing the task was lost
 * (create_job_first_click_swallowed_after_typing_description.md).
 *
 * The task fields are projected into `<app-agent-settings>`. A textarea fires a
 * bubbling native `change` when it loses focus, which a real click does between
 * mousedown and click. While the settings output was named `change`, the host
 * binding `(change)="onSettingsChange()"` also listened for that DOM event,
 * started a tool preview, and the preview flag disabled Create before mouseup.
 *
 * These mount the REAL create-form templates. The children are stubs, because
 * this JIT pipeline cannot see `input()`/`output()` (see job-create's spec);
 * the settings stub declares its outputs with `@Output()`, so Angular wires a
 * host binding to it exactly as it does in the app: output AND native listener.
 * jsdom never fires `change` on blur, so the specs dispatch it themselves.
 */

@Component({
  selector: 'app-agent-settings',
  standalone: true,
  template: `
    <ng-content select="[settingsTop]" />
    <ng-content select="[settingsTopAside]" />
    <ng-content select="[expertPicker]" />
    <ng-content select="[workspacePicker]" />
    <ng-content />
  `,
})
class AgentSettingsStub {
  @Output() settingsChange = new EventEmitter<void>();
  @Output() retryDatasources = new EventEmitter<void>();
  @Output() instructionsChange = new EventEmitter<string | null>();
}

@Component({selector: 'app-textarea', standalone: true, template: '<textarea></textarea>'})
class TextareaStub {}

@Component({selector: 'app-input', standalone: true, template: '<input type="text">'})
class InputStub {}

const STUBS = {
  imports: [TranslocoPipe, AgentSettingsStub, TextareaStub, InputStub],
  schemas: [NO_ERRORS_SCHEMA],
};

const TRANSLOCO = TranslocoTestingModule.forRoot({
  langs: {en},
  translocoConfig: {availableLangs: ['en'], defaultLang: 'en'},
});

function bubblingChange(target: Element): void {
  target.dispatchEvent(new Event('change', {bubbles: true}));
}

function enterOn(target: Element): KeyboardEvent {
  const event = new KeyboardEvent('keydown', {key: 'Enter', bubbles: true, cancelable: true});
  target.dispatchEvent(event);
  return event;
}

function api() {
  return {
    getProjects: vi.fn().mockReturnValue(of([{id: 'proj-1', name: 'P', status: 'active', is_default: true, user_role: 'owner'}])),
    getProject: vi.fn().mockReturnValue(of(null)),
    getEligibleDatasources: vi.fn().mockReturnValue(of([])),
    getExpertDefaults: vi.fn().mockReturnValue(of(null)),
    getExperts: vi.fn().mockReturnValue(of([])),
    getExpertDetail: vi.fn().mockReturnValue(of(null)),
    getExpertTemplate: vi.fn().mockReturnValue(of(null)),
    getPersistentThread: vi.fn().mockReturnValue(of(null)),
    previewToolGroups: vi.fn().mockReturnValue(of(null)),
    listWorkspaceTemplates: vi.fn().mockReturnValue(of({resources: []})),
    getProjectWorkspaceDefaults: vi.fn().mockReturnValue(of(null)),
    createJob: vi.fn().mockReturnValue(of({id: 'job-1'})),
  };
}

function providers(stubApi: ReturnType<typeof api>) {
  return [
    provideHttpClient(),
    provideHttpClientTesting(),
    {provide: ApiService, useValue: stubApi},
    {provide: ActivatedRoute, useValue: {snapshot: {queryParamMap: convertToParamMap({})}}},
    {provide: Router, useValue: {navigate: vi.fn()}},
    {provide: UserService, useValue: {currentUserId: signal('user-1'), currentUser: signal(null)}},
    {provide: ErrorMessageService, useValue: {translate: (_e: unknown, k?: string) => k ?? ''}},
    {provide: ModelService, useValue: {load: vi.fn()}},
    {provide: ViewportService, useValue: {isMobile: signal(false)}},
    {
      provide: CapabilitiesService,
      useValue: {datasourceScopeAutoAttachAvailable: () => false, protectedCloudAvailable: signal(false), grants: signal(null)},
    },
  ];
}

describe('Create Job: a native change from a projected field starts no settings preview', () => {
  afterEach(() => TestBed.resetTestingModule());

  function mount() {
    const stubApi = api();
    TestBed.resetTestingModule();
    TestBed.configureTestingModule({imports: [TRANSLOCO], providers: providers(stubApi)});
    TestBed.overrideComponent(JobCreateComponent, {set: STUBS});
    const fixture = TestBed.createComponent(JobCreateComponent);
    fixture.detectChanges();
    const component = fixture.componentInstance;
    const host = fixture.nativeElement.querySelector('app-agent-settings') as HTMLElement;
    const settings = fixture.debugElement.query(By.directive(AgentSettingsStub)).componentInstance as AgentSettingsStub;
    const onSettingsChange = vi.spyOn(component, 'onSettingsChange');
    stubApi.previewToolGroups.mockClear();
    return {fixture, component, host, settings, onSettingsChange, api: stubApi};
  }

  it('the description textarea is projected into the settings host', () => {
    const {host} = mount();
    // The precondition of the bug: the field sits inside the host element.
    expect(host.querySelector('.task-fields textarea')).not.toBeNull();
  });

  it.each([
    ['the task box', '.task-fields textarea'],
    ['the file input', 'input[type="file"]'],
    ['the priority select', '[settingsTopAside] #job-priority'],
  ])('a bubbling change from %s reaches no handler', (_label, selector) => {
    const {host, onSettingsChange, api: stub} = mount();
    const field = host.querySelector(selector);
    expect(field).not.toBeNull();

    bubblingChange(field!);

    expect(onSettingsChange).not.toHaveBeenCalled();
    expect(stub.previewToolGroups).not.toHaveBeenCalled();
  });

  it('a project change starts its own preview, never through the settings host', () => {
    // The project is a grant scope, so picking one legitimately re-reads the
    // tool preview. That is the select's own handler; the host still hears
    // nothing, and a select fires `change` only when its value really changed.
    const {host, onSettingsChange, api: stub} = mount();
    const project = host.querySelector('[settingsTopAside] #job-project');
    expect(project).not.toBeNull();

    bubblingChange(project!);

    expect(onSettingsChange).not.toHaveBeenCalled();
    expect(stub.previewToolGroups).toHaveBeenCalledOnce();
  });

  it('the form offers no phases choice: that is the Expert\'s setting', () => {
    const {host} = mount();
    const labels = [...host.querySelectorAll('[settingsTopAside] label')].map((l) => l.textContent?.trim());
    expect(labels).not.toContain(en.experts.editor.phases);
    expect(host.textContent).not.toContain(en.experts.editor.phases);
  });

  it('the form asks for one text: there is no second message field', () => {
    const {host} = mount();
    expect(host.querySelectorAll('textarea')).toHaveLength(1);
  });

  it('the project list selects the chosen project once it has loaded', () => {
    const {fixture, host} = mount();
    fixture.detectChanges();
    const project = host.querySelector('#job-project') as HTMLSelectElement;
    expect(project.value).toBe('proj-1');
  });

  it('typing in the task box enables Create, and Ctrl+Enter creates the job', async () => {
    const {fixture, host, api: stub} = mount();
    const task = host.querySelector('.task-fields textarea') as HTMLTextAreaElement;
    const create = fixture.nativeElement.querySelector('app-button[type="submit"]') as HTMLElement & {disabled: boolean};
    expect(create.disabled).toBe(true);

    task.value = 'Summarise the logs';
    task.dispatchEvent(new Event('input', {bubbles: true}));
    fixture.detectChanges();
    expect(create.disabled).toBe(false);

    const enter = new KeyboardEvent('keydown', {key: 'Enter', ctrlKey: true, bubbles: true, cancelable: true});
    task.dispatchEvent(enter);
    await fixture.whenStable();

    expect(enter.defaultPrevented).toBe(true);
    expect(stub.createJob).toHaveBeenCalledOnce();
    expect(stub.createJob.mock.calls[0][0].description).toBe('Summarise the logs');
  });

  it('Create sits in the task box, not below the settings', () => {
    const {fixture} = mount();
    const root = fixture.nativeElement as HTMLElement;
    expect(root.querySelector('.composer app-button[type="submit"]')).not.toBeNull();
    expect(root.querySelector('.form-actions app-button[type="submit"]')).toBeNull();
  });

  it('the settings output still reaches onSettingsChange and starts a preview', () => {
    const {settings, onSettingsChange, api: stub} = mount();

    settings.settingsChange.emit();

    expect(onSettingsChange).toHaveBeenCalledOnce();
    expect(stub.previewToolGroups).toHaveBeenCalledOnce();
  });

  it('Enter in a one-line settings field does not submit the job form', () => {
    const {host, api: stub} = mount();
    // A stand-in for the connector search and the number fields, which the
    // real settings component renders inside this host.
    const search = document.createElement('input');
    search.type = 'search';
    host.appendChild(search);

    expect(enterOn(search).defaultPrevented).toBe(true);
    expect(stub.createJob).not.toHaveBeenCalled();
  });

  it('Enter in the task box keeps its newline', () => {
    const {host} = mount();
    const description = host.querySelector('.task-fields textarea')!;

    expect(enterOn(description).defaultPrevented).toBe(false);
  });

  it('Create stays enabled while the settings check loads: the click waits instead', () => {
    const {fixture, component} = mount();
    component.formData.description = 'Summarise the logs';
    component.loadingWorkspacePreview.set(true);
    component.isLoadingExpertDetail.set(true);
    component.isLoadingDatasources.set(true);
    fixture.detectChanges();

    const create = fixture.nativeElement.querySelector('app-button[type="submit"]') as HTMLElement & {disabled: boolean};
    expect(create.disabled).toBe(false);
  });
});

describe('Create Session: a native change from the projected title starts no settings preview', () => {
  afterEach(() => TestBed.resetTestingModule());

  function mount() {
    const stubApi = api();
    TestBed.resetTestingModule();
    TestBed.configureTestingModule({imports: [TRANSLOCO], providers: providers(stubApi)});
    TestBed.overrideComponent(SessionCreateComponent, {set: STUBS});
    const fixture = TestBed.createComponent(SessionCreateComponent);
    fixture.detectChanges();
    const component = fixture.componentInstance;
    const host = fixture.nativeElement.querySelector('app-agent-settings') as HTMLElement;
    const settings = fixture.debugElement.query(By.directive(AgentSettingsStub)).componentInstance as AgentSettingsStub;
    const onSettingsChange = vi.spyOn(component, 'onSettingsChange');
    stubApi.previewToolGroups.mockClear();
    return {fixture, component, host, settings, onSettingsChange, api: stubApi};
  }

  it('Create Session stays enabled while the settings check loads: the click waits instead', () => {
    const {fixture, component} = mount();
    component.loadingWorkspacePreview.set(true);
    component.loadingExpert.set(true);
    component.loadingDatasources.set(true);
    fixture.detectChanges();

    const buttons = fixture.nativeElement.querySelectorAll('.form-actions app-button');
    const create = buttons[buttons.length - 1] as HTMLElement & {disabled: boolean};
    expect(create.disabled).toBe(false);
  });

  it('a bubbling change from the title reaches no handler', () => {
    const {host, onSettingsChange, api: stub} = mount();
    const title = host.querySelector('.session-fields input');
    expect(title).not.toBeNull();

    bubblingChange(title!);

    expect(onSettingsChange).not.toHaveBeenCalled();
    expect(stub.previewToolGroups).not.toHaveBeenCalled();
  });

  it('the settings output still reaches onSettingsChange and starts a preview', () => {
    const {settings, onSettingsChange, api: stub} = mount();

    settings.settingsChange.emit();

    expect(onSettingsChange).toHaveBeenCalledOnce();
    expect(stub.previewToolGroups).toHaveBeenCalledOnce();
  });
});
