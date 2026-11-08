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
    ['the description', '.task-fields app-textarea:nth-of-type(1) textarea'],
    ['the kickoff message', '.task-fields app-form-field:nth-of-type(3) textarea'],
    ['the file input', 'input[type="file"]'],
    ['the project select', '.task-fields app-select'],
    ['the priority select', '[settingsTopAside] app-select'],
  ])('a bubbling change from %s reaches no handler', (_label, selector) => {
    const {host, onSettingsChange, api: stub} = mount();
    const field = host.querySelector(selector);
    expect(field).not.toBeNull();

    bubblingChange(field!);

    expect(onSettingsChange).not.toHaveBeenCalled();
    expect(stub.previewToolGroups).not.toHaveBeenCalled();
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

  it('Enter in the description keeps its newline', () => {
    const {host} = mount();
    const description = host.querySelector('.task-fields textarea')!;

    expect(enterOn(description).defaultPrevented).toBe(false);
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
    return {component, host, settings, onSettingsChange, api: stubApi};
  }

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
