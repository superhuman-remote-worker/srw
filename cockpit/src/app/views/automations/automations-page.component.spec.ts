import {afterEach, beforeAll, describe, expect, it, vi} from 'vitest';
import {readFileSync} from 'node:fs';
import {TestBed} from '@angular/core/testing';
import {DatePipe} from '@angular/common';
import {CUSTOM_ELEMENTS_SCHEMA, ɵresolveComponentResources, Injector, runInInjectionContext, signal} from '@angular/core';
import {ActivatedRoute, Router, RouterLink, convertToParamMap} from '@angular/router';
import {of, throwError} from 'rxjs';
import {Automation} from '../../core/services/automations.service';
import en from '../../../assets/i18n/en.json';
import {TranslocoPipe, TranslocoService, TranslocoTestingModule} from '@jsverse/transloco';

import {
  AutomationsPageComponent,
  derivePresetCron,
  inferPresetFromCron,
  connectorBindings,
} from './automations-page.component';
import {ApiService} from '../../core/services/api.service';
import {AutomationsService} from '../../core/services/automations.service';

/**
 * Construct the component in a minimal injection context with mocked
 * services. We deliberately skip TestBed.createComponent so the spec
 * doesn't pay the cost of compiling the page template — the rendering
 * path is exercised by Playwright in the dev-cluster verification.
 */
function createComponent(queryParams: Record<string, string> = {}) {
  const automationsList = signal<unknown[]>([]);
  const isLoading = signal(false);
  const automationsServiceMock = {
    automations: automationsList,
    isLoading,
    loadMine: vi.fn(),
    loadByProject: vi.fn(),
    create: vi.fn().mockReturnValue(of({})),
    update: vi.fn().mockReturnValue(of({})),
    delete: vi.fn().mockReturnValue(of(undefined)),
    pause: vi.fn().mockReturnValue(of({})),
    resume: vi.fn().mockReturnValue(of({})),
    runNow: vi.fn().mockReturnValue(of({})),
    listRuns: vi.fn().mockReturnValue(of([])),
  };

  const apiServiceMock = {
    getExperts: vi.fn().mockReturnValue(
      of([{id: 'scholar', display_name: 'Scholar'}]),
    ),
    getEligibleDatasources: vi.fn().mockReturnValue(of([])),
    getProject: vi.fn().mockReturnValue(of(null)),
  };

  const routerMock = {navigate: vi.fn()};
  const routeMock = {
    snapshot: {queryParamMap: convertToParamMap(queryParams)},
  };

  const translocoMock = {
    translate: (key: string, params?: {name?: string}) => key === 'automations.errors.liveConnectorTaken'
      ? en.automations.errors.liveConnectorTaken.replace('{{name}}', params?.name ?? '') : key,
    langChanges$: of('en'),
    getActiveLang: () => 'en',
  };

  const injector = Injector.create({
    providers: [
      {provide: AutomationsService, useValue: automationsServiceMock},
      {provide: ApiService, useValue: apiServiceMock},
      {provide: Router, useValue: routerMock},
      {provide: ActivatedRoute, useValue: routeMock},
      {provide: TranslocoService, useValue: translocoMock},
    ],
  });

  const component = runInInjectionContext(
    injector,
    () => new AutomationsPageComponent(),
  );

  return {component, automationsServiceMock, apiServiceMock, routerMock};
}

describe('derivePresetCron', () => {
  it('builds every_day with hour/minute', () => {
    expect(derivePresetCron('every_day', '09:00', '')).toBe('0 9 * * *');
    expect(derivePresetCron('every_day', '07:30', '')).toBe('30 7 * * *');
  });

  it('builds every_weekday with 1-5 DOW', () => {
    expect(derivePresetCron('every_weekday', '09:00', '')).toBe('0 9 * * 1-5');
  });

  it('builds every_monday', () => {
    expect(derivePresetCron('every_monday', '09:00', '')).toBe('0 9 * * 1');
  });

  it('builds every_month_first', () => {
    expect(derivePresetCron('every_month_first', '12:00', '')).toBe(
      '0 12 1 * *',
    );
  });

  it('builds every_hour using the minute slot only', () => {
    expect(derivePresetCron('every_hour', '00:15', '')).toBe('15 * * * *');
  });

  it('passes a custom expression through trimmed', () => {
    expect(derivePresetCron('custom', '09:00', '  */5 * * * *  ')).toBe(
      '*/5 * * * *',
    );
  });

  it('returns empty string when the time is unparseable', () => {
    expect(derivePresetCron('every_day', 'bogus', '')).toBe('');
  });
});

describe('inferPresetFromCron', () => {
  it('round-trips every_day', () => {
    expect(inferPresetFromCron('0 9 * * *')).toEqual({
      preset: 'every_day',
      time: '09:00',
    });
  });

  it('round-trips every_weekday', () => {
    expect(inferPresetFromCron('0 9 * * 1-5')).toEqual({
      preset: 'every_weekday',
      time: '09:00',
    });
  });

  it('round-trips every_monday', () => {
    expect(inferPresetFromCron('0 9 * * 1')).toEqual({
      preset: 'every_monday',
      time: '09:00',
    });
  });

  it('round-trips every_month_first', () => {
    expect(inferPresetFromCron('0 12 1 * *')).toEqual({
      preset: 'every_month_first',
      time: '12:00',
    });
  });

  it('recognizes every_hour and zeroes the time slot', () => {
    expect(inferPresetFromCron('15 * * * *')).toEqual({
      preset: 'every_hour',
      time: '00:00',
    });
  });

  it('falls back to custom for unrecognized shapes', () => {
    // dow=0 (Sunday) doesn't match any of the preset DOW patterns.
    expect(inferPresetFromCron('0 9 * * 0').preset).toBe('custom');
  });

  it('falls back to custom when the field count is wrong', () => {
    expect(inferPresetFromCron('weird').preset).toBe('custom');
  });
});

describe('AutomationsPageComponent', () => {
  afterEach(() => {
    vi.clearAllMocks();
  });

  describe('ngOnInit project-filter wiring', () => {
    it('loads my automations when there is no ?project= param', () => {
      const {component, automationsServiceMock, apiServiceMock} = createComponent();
      component.ngOnInit();
      expect(automationsServiceMock.loadMine).toHaveBeenCalledTimes(1);
      expect(automationsServiceMock.loadByProject).not.toHaveBeenCalled();
      expect(apiServiceMock.getExperts).toHaveBeenCalledWith('worker');
      expect(component.projectFilter()).toBeNull();
    });

    it('loads by project + sets the filter signal when ?project=<id> is present', () => {
      const {component, automationsServiceMock, apiServiceMock} =
        createComponent({project: 'p1'});
      component.ngOnInit();
      expect(automationsServiceMock.loadByProject).toHaveBeenCalledWith('p1');
      expect(automationsServiceMock.loadMine).not.toHaveBeenCalled();
      expect(component.projectFilter()).toBe('p1');
      expect(apiServiceMock.getProject).toHaveBeenCalledWith('p1');
    });

    it('populates projectFilterName when getProject resolves a value', () => {
      const {component, apiServiceMock} = createComponent({project: 'p1'});
      apiServiceMock.getProject.mockReturnValueOnce(
        of({id: 'p1', name: 'My Project'}),
      );
      component.ngOnInit();
      expect(component.projectFilterName()).toBe('My Project');
    });
  });

  describe('clearProjectFilter', () => {
    it('drops the filter, calls loadMine, and replaces the URL', () => {
      const {component, automationsServiceMock, routerMock} = createComponent({
        project: 'p1',
      });
      component.ngOnInit();
      automationsServiceMock.loadMine.mockClear();

      component.clearProjectFilter();

      expect(component.projectFilter()).toBeNull();
      expect(component.projectFilterName()).toBeNull();
      expect(automationsServiceMock.loadMine).toHaveBeenCalledTimes(1);
      expect(routerMock.navigate).toHaveBeenCalledWith(['/automations'], {
        queryParams: {},
        replaceUrl: true,
      });
    });
  });

  describe('openNewEditor', () => {
    it('prefills projectId from the active filter', () => {
      const {component} = createComponent({project: 'p1'});
      component.ngOnInit();
      // ngOnInit opens the editor once for the cross-link entry path;
      // close it so the New-automation-button path is exercised in isolation.
      component.closeEditor();
      component.openNewEditor();
      const editor = component.editor();
      expect(editor).not.toBeNull();
      expect(editor!.projectId).toBe('p1');
      expect(editor!.mode).toBe('create');
    });

    it('leaves projectId null when no filter is active', () => {
      const {component} = createComponent();
      component.ngOnInit();
      component.openNewEditor();
      const editor = component.editor();
      expect(editor).not.toBeNull();
      expect(editor!.projectId).toBeNull();
    });
  });

  describe('projectFilterDisplayName', () => {
    it('returns the loaded project name when available', () => {
      const {component, apiServiceMock} = createComponent({project: 'p1'});
      apiServiceMock.getProject.mockReturnValueOnce(
        of({id: 'p1', name: 'My Project'}),
      );
      component.ngOnInit();
      expect(component.projectFilterDisplayName()).toBe('My Project');
    });

    it('falls back to the localized key when getProject returns null', () => {
      const {component} = createComponent({project: 'p1'});
      component.ngOnInit();
      // Mock transloco.translate returns the key — that's what we assert.
      expect(component.projectFilterDisplayName()).toBe(
        'automations.list.projectFilter.fallbackName',
      );
    });
  });

  describe('expert persistence contract', () => {
    it('submits DB experts through expert_id with worker_base', () => {
      const {component, automationsServiceMock, apiServiceMock} = createComponent();
      apiServiceMock.getExperts.mockReturnValueOnce(
        of([
          {
            id: 'dddddddd-dddd-dddd-dddd-dddddddddddd',
            name: 'my-worker',
            display_name: 'My Worker',
            storage_kind: 'db',
          },
        ]),
      );
      component.ngOnInit();
      component.openNewEditor();
      component.updateEditor('name', 'Daily review');
      component.updateEditor('prompt', 'Review the project.');

      component.saveEditor();

      expect(automationsServiceMock.create).toHaveBeenCalledWith(
        expect.objectContaining({
          expert: 'worker_base',
          expert_id: 'dddddddd-dddd-dddd-dddd-dddddddddddd',
        }),
      );
    });

    it('submits bundled experts by config name and clears expert_id', () => {
      const {component, automationsServiceMock, apiServiceMock} = createComponent();
      apiServiceMock.getExperts.mockReturnValueOnce(
        of([
          {
            id: 'scholar',
            name: 'scholar',
            display_name: 'Scholar',
            storage_kind: 'bundled',
          },
        ]),
      );
      component.ngOnInit();
      component.openNewEditor();
      component.updateEditor('name', 'Daily review');
      component.updateEditor('prompt', 'Review the project.');

      component.saveEditor();

      expect(automationsServiceMock.create).toHaveBeenCalledWith(
        expect.objectContaining({
          expert: 'scholar',
          expert_id: null,
        }),
      );
    });
  });
});


describe('live automations', () => {
  const line = {id: 'phone-1', name: 'Empfang', type: 'gpt_live_sip'};
  const extra = {id: 'kb-1', name: 'Empfang', type: 'kb'};
  function setup() {
    const result = createComponent();
    result.apiServiceMock.getEligibleDatasources.mockReturnValue(of([line, extra]));
    result.component.ngOnInit();
    result.component.openNewEditor();
    result.component.updateEditor('triggerType', 'live');
    result.component.updateEditor('name', 'Incoming clinic enquiry');
    result.component.updateEditor('prompt', 'Help the caller');
    result.component.updateEditor('preset', 'custom');
    result.component.updateEditor('customCron', '');
    result.component.setConnectorIds(['phone-1', 'kb-1']);
    return result;
  }

  it.each(['default', 'none', 'inline'] as const)('creates a live trigger without cron and with %s workspace', (kind) => {
    const {component, automationsServiceMock} = setup();
    component.updateEditor('workspaceChoice', kind === 'inline' ? {kind, spec: {backend: 'virtual'}} : {kind});
    component.saveEditor();
    const body = automationsServiceMock.create.mock.calls[0][0];
    expect(body).toMatchObject({trigger_type: 'live', primary_connector: 'empfang', job_shape: {
      execution: {connectors: {empfang: {ref: {uid: 'phone-1'}}, 'empfang-2': {ref: {uid: 'kb-1'}}}},
    }});
    expect(body).not.toHaveProperty('cron_expr');
    expect(body).not.toHaveProperty('timezone');
    expect(body).not.toHaveProperty('catchup_window_seconds');
    if (kind === 'default') expect(body.job_shape).not.toHaveProperty('workspace');
    if (kind === 'none') expect(body.job_shape.workspace).toBeNull();
    if (kind === 'inline') expect(body.job_shape.workspace).toEqual({template: {inline: {backend: 'virtual'}}});
  });

  it('requires a selected live primary connector and defaults to the first', () => {
    const {component, automationsServiceMock} = setup();
    expect(component.editor()?.primaryConnectorId).toBe('phone-1');
    component.setConnectorIds(['kb-1']);
    component.saveEditor();
    expect(automationsServiceMock.create).not.toHaveBeenCalled();
    expect(component.errorMessage()).toBe('automations.editor.liveConnectorRequired');
  });

  it('shows the conflict holder on create, patch and resume', () => {
    const {component, automationsServiceMock} = setup();
    const refusal = throwError(() => ({status: 409, error: {detail: {
      code: 'live_connector_taken', automation_name: 'Clinic reception', automation_id: 'holder',
    }}}));
    automationsServiceMock.create.mockReturnValue(refusal);
    component.saveEditor();
    expect(component.errorMessage()).toBe('Already used by automation Clinic reception');
    const row = {...component.editor(), id: 'automation-1', enabled: false, trigger_type: 'live'} as unknown as Automation;
    automationsServiceMock.resume.mockReturnValue(refusal);
    component.toggleEnabled(row);
    expect(component.errorMessage()).toBe('Already used by automation Clinic reception');
    component.updateEditor('mode', 'edit');
    component.updateEditor('id', row.id);
    automationsServiceMock.update.mockReturnValue(refusal);
    component.saveEditor();
    expect(component.errorMessage()).toBe('Already used by automation Clinic reception');
  });

  it('preserves aliases and workspace refs on patch without changing the trigger', () => {
    const {component, automationsServiceMock} = setup();
    component.openEditor({id: 'a-1', trigger_type: 'live', trigger_connector_id: 'phone-1', name: 'Live',
      expert: 'scholar', prompt: 'Help', enabled: true, autonomy: 'review', priority: 5, max_fires_per_day: 100,
      catchup_window_seconds: 0, project_id: null, job_shape: {
        primary_connector: 'original', execution: {connectors: {original: {ref: {uid: 'phone-1'}}}},
        workspace: {template: {ref: {name: 'clinic', scope: {kind: 'Account', name: 'me'}}}},
      }} as Automation);
    component.updateEditor('triggerType', 'cron');
    expect(component.editor()?.triggerType).toBe('live');
    component.saveEditor();
    const body = automationsServiceMock.update.mock.calls[0][1];
    expect(body).not.toHaveProperty('trigger_type');
    expect(body.primary_connector).toBe('original');
    expect(body.job_shape.workspace).toEqual({template: {ref: {name: 'clinic', scope: {kind: 'Account', name: 'me'}}}});
  });

  it('labels a live row with its connector and never runs it manually', () => {
    const {component, automationsServiceMock} = setup();
    const row = {id: 'a-1', trigger_type: 'live', trigger_connector_id: 'phone-1'} as Automation;
    expect(component.connectorLabel(row)).toBe('Empfang');
    component.runNow(row);
    expect(automationsServiceMock.runNow).not.toHaveBeenCalled();
  });

  it('creates unique bounded aliases for unicode, punctuation and colliding long names', () => {
    const names = ['Ärzte & Empfang', 'Ärzte / Empfang', '!!!', 'a'.repeat(90), 'a'.repeat(90)];
    const rows = names.map((name, i) => ({id: String(i), name}));
    const result = connectorBindings(rows.map((r) => r.id), rows);
    expect(Object.keys(result.connectors)).toHaveLength(5);
    for (const alias of Object.keys(result.connectors)) expect(alias).toMatch(/^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/);
    expect(result.aliases['0']).toBe('arzte-empfang');
    expect(result.aliases['1']).toBe('arzte-empfang-2');
  });
});


describe('live automation rendering', () => {
  beforeAll(async () => { await ɵresolveComponentResources(() => Promise.resolve('')); });
  afterEach(() => TestBed.resetTestingModule());
  function mount() {
    const {automationsServiceMock: service, apiServiceMock: api} = createComponent();
    api.getEligibleDatasources.mockReturnValue(of([{id: 'phone-1', name: 'Empfang', type: 'gpt_live_sip'}]));
    TestBed.configureTestingModule({imports: [AutomationsPageComponent,
      TranslocoTestingModule.forRoot({langs: {en}, translocoConfig: {availableLangs: ['en'], defaultLang: 'en'}, preloadLangs: true})],
      providers: [
        {provide: AutomationsService, useValue: service}, {provide: ApiService, useValue: api},
        {provide: Router, useValue: {navigate: vi.fn()}},
        {provide: ActivatedRoute, useValue: {snapshot: {queryParamMap: convertToParamMap({})}}},
      ]});
    TestBed.overrideComponent(AutomationsPageComponent, {set: {
      template: readFileSync('src/app/views/automations/automations-page.component.html', 'utf8'),
      templateUrl: undefined, styles: [], styleUrl: undefined, styleUrls: [],
      imports: [DatePipe, TranslocoPipe], schemas: [CUSTOM_ELEMENTS_SCHEMA],
    }});
    const fixture = TestBed.createComponent(AutomationsPageComponent);
    fixture.detectChanges();
    return {fixture, component: fixture.componentInstance, host: fixture.nativeElement as HTMLElement, service};
  }

  it('switches from schedule fields to live connectors and the workspace picker', () => {
    const {fixture, component, host} = mount();
    component.openNewEditor();
    fixture.detectChanges();
    expect(host.querySelector('app-cron-preview')).not.toBeNull();
    component.updateEditor('triggerType', 'live');
    component.updateEditor('showAdvanced', true);
    fixture.detectChanges();
    expect(host.querySelector('app-cron-preview')).toBeNull();
    expect(host.querySelector('input[type="time"]')).toBeNull();
    expect(host.textContent).not.toContain(en.automations.editor.timezone);
    expect(host.textContent).not.toContain(en.automations.editor.catchupHours);
    expect(host.querySelector('app-workspace-picker')).not.toBeNull();
    expect(host.querySelectorAll('app-datasources-group')).toHaveLength(2);
    const pickers = host.querySelectorAll('app-datasources-group');
    expect((pickers[0] as unknown as {datasources: unknown[]}).datasources).toEqual([{id: 'phone-1', name: 'Empfang', type: 'gpt_live_sip'}]);
  });

  it('renders Live plus the connector name and omits Run-now on live rows', () => {
    const {fixture, component, host, service} = mount();
    service.automations.set([{id: 'a-1', name: 'Clinic', trigger_type: 'live', trigger_connector_id: 'phone-1', enabled: true}]);
    fixture.detectChanges();
    expect(host.querySelector('.row-meta')?.textContent).toContain('Live · Empfang');
    expect(host.querySelector('.row-actions')?.textContent).not.toContain(en.automations.list.runNow);
    service.automations.set([{id: 'a-2', name: 'Scheduled', trigger_type: 'cron', cron_expr: '0 9 * * *', enabled: true}]);
    fixture.detectChanges();
    expect(host.querySelector('.row-actions')?.textContent).toContain(en.automations.list.runNow);
  });
});
