import {provideHttpClient} from '@angular/common/http';
import {HttpTestingController, provideHttpClientTesting} from '@angular/common/http/testing';
import {Injector, runInInjectionContext, signal} from '@angular/core';
import {TestBed} from '@angular/core/testing';
import {ActivatedRoute, convertToParamMap, Router} from '@angular/router';
import {Observable, of, Subject} from 'rxjs';
import {TranslocoService} from '@jsverse/transloco';
import {afterEach, describe, expect, it, vi} from 'vitest';
import {CapabilitiesService} from '../../core/services/capabilities.service';
import {ErrorMessageService} from '../../core/services/error-message.service';
import {ModelService} from '../../core/services/model.service';
import {UserService} from '../../core/services/user.service';
import {ApiService} from '../../core/services/api.service';
import {ModelGroupComponent} from '../agent-settings/model-group.component';
import {
  keepEligibleIds,
  mapThreadToPrefill,
  protectedCloudToggleVisible,
  SessionCreateComponent,
} from './session-create.component';

/**
 * The tool-groups preview is the only thing the form uses ApiService for, and
 * it is deliberately stubbed in the existing setups: those tests drive raw
 * HttpTestingController expectations and a real ApiService would drag the
 * whole transloco provider tree in behind it. The preview wiring gets its own
 * describe at the bottom, where the stub IS the subject.
 *
 * `thread` backs `getPersistentThread` — the "Start a new session" prefill's
 * own describe block below is where it matters; every other describe passes
 * no `from` query param, so ngOnInit never calls it at all.
 */
function stubApi(
  preview: unknown = null,
  thread: Observable<Record<string, unknown> | null> = of(null),
) {
  return {
    previewToolGroups: vi.fn().mockReturnValue(of(preview)),
    getPersistentThread: vi.fn().mockReturnValue(thread),
    // Only reached when a prefilled project is missing from the active list;
    // `null` is "gone for good", which is the pre-archive behaviour.
    getProject: vi.fn().mockReturnValue(of(null)),
  };
}

/** `from` present or absent, matching how `route.snapshot.queryParamMap.get`
 *  is actually read (session-create.component.ts's `route` injection is
 *  `{optional: true}` — most describes below omit this provider entirely,
 *  exactly as they did before this prefill existed, to prove that path is
 *  unaffected). */
function activatedRouteWithFrom(from: string) {
  return {
    snapshot: {queryParamMap: convertToParamMap({from})},
  };
}

describe('protectedCloudToggleVisible', () => {
  it('hidden when feature off', () => {
    expect(protectedCloudToggleVisible(false, [{ main_cloud_backend: 'nextcloud' }])).toBe(false);
  });
  it('hidden when only default projects selected', () => {
    expect(protectedCloudToggleVisible(true, [{ is_default: true, main_cloud_backend: 'nextcloud' }])).toBe(false);
  });
  it('hidden for non-nextcloud backends', () => {
    expect(protectedCloudToggleVisible(true, [{ main_cloud_backend: 'opencloud' }])).toBe(false);
  });
  it('visible for a selected non-default nextcloud project', () => {
    expect(protectedCloudToggleVisible(true, [
      { is_default: true, main_cloud_backend: 'nextcloud' },
      { is_default: false, main_cloud_backend: 'nextcloud' },
    ])).toBe(true);
  });
});

// Task 14, item B (session_config_drift_resume.md §8.3): pure mapping/filter
// functions behind the "Start a new session" prefill. Kept separate from
// SessionCreateComponent so they're testable without mounting the form.
describe('mapThreadToPrefill', () => {
  it('a null thread (failed fetch) maps to null', () => {
    expect(mapThreadToPrefill(null)).toBeNull();
  });

  it('extracts project_ids, metadata.expert_id, metadata.config_override.llm.model, and metadata.datasource_ids', () => {
    const thread = {
      project_ids: ['proj-1', 'proj-2'],
      metadata: {
        expert_id: 'expert-9',
        datasource_ids: ['ds-1', 'ds-2'],
        config_override: {llm: {model: 'gpt-5.6-sol'}},
      },
    };
    expect(mapThreadToPrefill(thread)).toEqual({
      projectIds: ['proj-1', 'proj-2'],
      expertId: 'expert-9',
      model: 'gpt-5.6-sol',
      datasourceIds: ['ds-1', 'ds-2'],
    });
  });

  it('a thread with none of these fields maps to an all-empty object, not null — a real answer, just an empty one', () => {
    expect(mapThreadToPrefill({})).toEqual({
      projectIds: [],
      expertId: null,
      model: null,
      datasourceIds: [],
    });
  });

  it('missing metadata/config_override/llm at any level degrades to the empty case rather than throwing', () => {
    expect(mapThreadToPrefill({project_ids: ['p1'], metadata: {}})).toEqual({
      projectIds: ['p1'],
      expertId: null,
      model: null,
      datasourceIds: [],
    });
  });

  it('stringifies non-string array entries defensively, matching settings-pane.component.ts\'s .map(String) precedent', () => {
    const thread = {project_ids: [42], metadata: {datasource_ids: [true]}};
    expect(mapThreadToPrefill(thread)).toEqual({
      projectIds: ['42'],
      expertId: null,
      model: null,
      datasourceIds: ['true'],
    });
  });
});

describe('keepEligibleIds', () => {
  it('drops ids not present in the eligible list — the drift-dropping rule', () => {
    expect(keepEligibleIds(['a', 'b', 'c'], [{id: 'a'}, {id: 'c'}])).toEqual(['a', 'c']);
  });

  it('keeps every id when all of them survive', () => {
    expect(keepEligibleIds(['x', 'y'], [{id: 'y'}, {id: 'x'}])).toEqual(['x', 'y']);
  });

  it('empty ids stays empty regardless of what is eligible', () => {
    expect(keepEligibleIds([], [{id: 'a'}])).toEqual([]);
  });

  it('an empty eligible list drops everything', () => {
    expect(keepEligibleIds(['a', 'b'], [])).toEqual([]);
  });
});

describe('SessionCreateComponent framework defaults', () => {
  afterEach(() => TestBed.resetTestingModule());

  it('loads persistent session defaults instead of worker defaults', () => {
    TestBed.configureTestingModule({
      providers: [
        provideHttpClient(),
        provideHttpClientTesting(),
        {provide: Router, useValue: {navigate: vi.fn()}},
        {provide: UserService, useValue: {currentUserId: signal<string | null>(null)}},
        {provide: ModelService, useValue: {load: vi.fn()}},
        {
          provide: ErrorMessageService,
          useValue: {translate: (_e: unknown, fallback: string) => fallback},
        },
        {
          provide: CapabilitiesService,
          useValue: {
            protectedCloudAvailable: signal(false),
            grants: signal(null),
          },
        },
        {provide: ApiService, useValue: stubApi()},
      ],
    });
    TestBed.overrideComponent(SessionCreateComponent, {
      set: {imports: [], template: ''},
    });

    const fixture = TestBed.createComponent(SessionCreateComponent);
    const http = TestBed.inject(HttpTestingController);
    fixture.detectChanges();
    const prefillTools = vi.fn();
    fixture.componentInstance.agentSettings = {
      getOverrides: vi.fn().mockReturnValue({}),
      toolsGroup: {prefillFromConfig: prefillTools},
    } as any;

    http.expectOne(
      (request) => request.urlWithParams.endsWith('/experts?type=session'),
    ).flush([]);
    http.expectOne((request) => request.url.endsWith('/expert-defaults')).flush({
      personal_defaults_allowed: true,
      defaults: {
        worker: {application: null, personal: null, effective: null, source: 'application'},
        session: {application: null, personal: null, effective: null, source: 'application'},
      },
    });
    http.expectOne((request) => request.url.endsWith('/datasources/eligible')).flush([]);
    const persistentConfig = {
      agent_id: 'session_base',
      tools: {communication: [], delegation: []},
    };
    // account_defaults=true is load-bearing, not decoration: it is what puts
    // the account's workspace.backend (`virtual`) into the config the form
    // resolves. Without it the form reads session_base's `sandbox`, treats the
    // session as non-lite, and submits repository connectors that create_thread
    // rejects with 400.
    const baseRequest = http.expectOne(
      (request) => request.urlWithParams.endsWith(
        '/experts/session_base?type=session&account_defaults=true',
      ),
    );
    baseRequest.flush({config: persistentConfig});

    expect(fixture.componentInstance.frameworkDefaults()?.['agent_id']).toBe('session_base');
    expect(prefillTools).toHaveBeenCalledWith(persistentConfig);
    http.verify();
  });
});

describe('SessionCreateComponent submit flow', () => {
  afterEach(() => TestBed.resetTestingModule());

  function setup() {
    const navigate = vi.fn().mockResolvedValue(true);
    TestBed.configureTestingModule({
      providers: [
        provideHttpClient(),
        provideHttpClientTesting(),
        {provide: Router, useValue: {navigate}},
        {provide: UserService, useValue: {currentUserId: signal<string | null>(null)}},
        {provide: ModelService, useValue: {load: vi.fn()}},
        {
          provide: ErrorMessageService,
          useValue: {translate: (_e: unknown, fallback: string) => fallback},
        },
        {
          provide: CapabilitiesService,
          useValue: {protectedCloudAvailable: signal(false), grants: signal(null)},
        },
        {provide: ApiService, useValue: stubApi()},
      ],
    });
    TestBed.overrideComponent(SessionCreateComponent, {set: {imports: [], template: ''}});
    const fixture = TestBed.createComponent(SessionCreateComponent);
    const http = TestBed.inject(HttpTestingController);
    return {fixture, http, navigate};
  }

  // The form used to hand its body to a `_creating` route and unmount, so a
  // rejected config destroyed every selection and dropped the user on
  // /sessions with a toast. It now creates first and only navigates on success.
  it('navigates to the created thread once the server accepts the config', async () => {
    const {fixture, http, navigate} = setup();
    const component = fixture.componentInstance;

    const pending = component.createSession();
    const request = http.expectOne((r) => r.url.endsWith('/persistent/threads') && r.method === 'POST');
    expect(request.request.body.datasource_ids).toEqual([]);
    request.flush({thread_id: 'thread-xyz'});
    await pending;

    expect(navigate).toHaveBeenCalledWith(['/sessions', 'thread-xyz']);
    expect(component.createError()).toBeNull();
  });

  it('does not create a session while VM sizing is invalid', async () => {
    const {fixture, http, navigate} = setup();
    const component = fixture.componentInstance;
    component.agentSettings = {vmSizingValid: () => false} as SessionCreateComponent['agentSettings'];

    await component.createSession();

    http.expectNone((request) => request.url.endsWith('/persistent/threads'));
    expect(navigate).not.toHaveBeenCalled();
    expect(component.creating()).toBe(false);
  });

  it('stays on the form with the error when the server rejects the config', async () => {
    const {fixture, http, navigate} = setup();
    const component = fixture.componentInstance;
    component.title = 'Keep my settings';

    const pending = component.createSession();
    http.expectOne((r) => r.url.endsWith('/persistent/threads') && r.method === 'POST')
      .flush(
        {detail: 'Lite session backends cannot attach repository connectors'},
        {status: 400, statusText: 'Bad Request'},
      );
    await pending;

    expect(navigate).not.toHaveBeenCalled();
    expect(component.createError()).toBeTruthy();
    expect(component.creating()).toBe(false);
    // Nothing was reset — the user can fix the tier and resubmit.
    expect(component.title).toBe('Keep my settings');
  });
});

// Task 3 — root cause of the vanishing Reasoning pick (dev thread
// 1930dec9-181d-4fd5-a030-90b3d0b363d6). The inference that the user must
// have changed the model or switched expert didn't hold up: `prefillFromConfig`
// — the same sink the original inference already named — is also invoked by
// `SessionCreateComponent.applyEffectiveDefault()` (`:498`), which re-resolves
// the effective default expert and, when it differs from what's currently
// selected (the guard at `:502`, `expert && selectedExpert()?.id !== expert.id`),
// fetches its detail and prefills the form. That guard is what decides whether
// ANY post-load re-resolution can do anything at all — and it is gated only on
// `expertSelectionTouched`, which nothing in the Model/Reasoning controls ever
// sets, so it stays live for the component's whole lifetime unless the user
// clicks an expert card.
//
// Three call sites feed that guard: `loadExperts()`'s response landing (`:474`),
// `loadEffectiveDefault()`'s own response landing (`:493` — itself re-issued
// from `loadExperts()`'s tail call at `:479` AND from `loadProjects()`'s
// response handler at `:464`, once a default project auto-populates), and
// `toggleProject()` (`:531`).
//
// The first two settle in parallel at `ngOnInit`, typically within a few
// hundred ms — a near-zero real-world window for a human to have already
// filled in two dropdowns. The project path has no such bound: the user can
// take as long as they like, and a project chip is a perfectly ordinary,
// easily-overlooked thing to click after already configuring Model/Reasoning
// (it sits above the Settings tab, and re-visiting it is not unusual). Its
// default expert can differ from whatever auto-selected earlier (`source:
// 'project'` vs `'user'`/`'application'`), which is exactly what satisfies the
// `:502` guard on demand, any time, with zero expert-grid interaction. That is
// the best fit for "I don't recall changing the model or the expert" — the
// primary test below reproduces that path; the page-load race is kept as a
// secondary variant, since the mechanism is identical and it's worth having
// regression coverage for both.
describe('SessionCreateComponent — reasoning pick lost to an involuntary prefillFromConfig (Task 3)', () => {
  afterEach(() => {
    TestBed.resetTestingModule();
    localStorage.clear();
  });

  /** Builds a real `ModelGroupComponent` (the sink under test, constructed the
   *  same way model-group.component.spec.ts does) plus a `SessionCreateComponent`
   *  fixture wired so its `agentSettings` ViewChild forwards to that real
   *  instance — reproducing AgentSettingsComponent.prefillFromConfig's own
   *  first line (`this.modelGroup?.prefillFromConfig(config)`). Assertions
   *  made against `modelGroup` exercise the actual production clearing logic,
   *  not a stand-in that only proves a call happened. */
  function setupWithRealModelGroup() {
    const modelServiceMock = {
      models: signal([]),
      auxiliaryModels: signal([]),
      visionModels: signal([]),
      whisperModels: signal([]),
      embeddingModels: signal([]),
      providers: signal([]),
      reasoningByModel: signal<Record<string, {method: string; default: string | null; options: string[]}>>({
        'gpt-5.6-sol': {method: 'effort_enum', default: 'high', options: ['low', 'medium', 'high', 'xhigh', 'max']},
      }),
      loading: signal(false),
      loaded: signal(true),
      load: vi.fn(),
    };
    const translocoMock = {translate: (key: string) => key, langChanges$: {subscribe: () => ({unsubscribe() {}})}, getActiveLang: () => 'en'};
    const modelGroupInjector = Injector.create({
      providers: [
        {provide: ModelService, useValue: modelServiceMock},
        {provide: TranslocoService, useValue: translocoMock},
      ],
    });
    const modelGroup = runInInjectionContext(modelGroupInjector, () => new ModelGroupComponent());

    TestBed.configureTestingModule({
      providers: [
        provideHttpClient(),
        provideHttpClientTesting(),
        {provide: Router, useValue: {navigate: vi.fn()}},
        {provide: UserService, useValue: {currentUserId: signal<string | null>(null)}},
        {provide: ModelService, useValue: modelServiceMock},
        {provide: ErrorMessageService, useValue: {translate: (_e: unknown, fallback: string) => fallback}},
        {
          provide: CapabilitiesService,
          useValue: {protectedCloudAvailable: signal(false), grants: signal(null)},
        },
        {provide: ApiService, useValue: stubApi()},
      ],
    });
    TestBed.overrideComponent(SessionCreateComponent, {set: {imports: [], template: ''}});

    const fixture = TestBed.createComponent(SessionCreateComponent);
    const http = TestBed.inject(HttpTestingController);
    fixture.detectChanges(); // runs ngOnInit — issues every request below at once

    fixture.componentInstance.agentSettings = {
      getOverrides: vi.fn().mockReturnValue({}),
      prefillFromConfig: (config: Record<string, unknown>) => modelGroup.prefillFromConfig(config),
    } as any;

    return {fixture, http, modelGroup};
  }

  it('the primary path: a project-chip click, well after the pick, resolves a different project-scoped default expert', () => {
    const {fixture, http, modelGroup} = setupWithRealModelGroup();

    // The page loads and settles completely and quietly — expert list,
    // account defaults, datasources, and the (project-less) effective-default
    // lookup all resolve, auto-selecting expert-1. Nothing has been picked
    // yet, so this settling is inert.
    http.expectOne(
      (r) => r.urlWithParams.endsWith('/experts/session_base?type=session&account_defaults=true'),
    ).flush({config: {}});
    http.expectOne((r) => r.url.includes('/datasources/eligible')).flush([]);
    http.expectOne((r) => r.urlWithParams.endsWith('/experts?type=session')).flush([
      {id: 'expert-1', display_name: 'Coder', description: '', icon: 'code', color: '#fff', tags: []},
      {id: 'expert-2', display_name: 'Researcher', description: '', icon: 'science', color: '#fff', tags: []},
    ]);
    http.expectOne((r) => r.url.includes('/expert-defaults')).flush({
      personal_defaults_allowed: true,
      defaults: {
        worker: {application: null, personal: null, effective: null, source: 'application'},
        session: {application: {id: 'expert-1'}, personal: null, effective: {id: 'expert-1'}, source: 'application'},
      },
    });
    http.expectOne(
      (r) => r.urlWithParams.endsWith('/experts/expert-1?account_defaults=true&role=session'),
    ).flush({config: {llm: {}}});
    expect(fixture.componentInstance.selectedExpert()?.id).toBe('expert-1');
    expect(modelGroup.reasoningResetNotice()).toBe(false); // quiescent — nothing to lose yet

    // Unbounded time later, the user fills in the Settings tab: a model, then
    // Reasoning: Max. No pending requests, nothing racing — a plain, settled
    // page. No expert card is ever clicked.
    modelGroup.onModelChange('gpt-5.6-sol');
    modelGroup.onSessionReasoningChange('max');
    expect(modelGroup.sessionReasoning()).toBe('max');

    // The user clicks a project chip — an ordinary action on this form, and
    // the only one taken besides the Model/Reasoning picks. toggleExpert()
    // never runs; expertSelectionTouched stays false throughout.
    fixture.componentInstance.toggleProject('project-x');

    http.expectOne((r) => r.url.includes('/datasources/eligible')).flush([]);
    // The project's own effective default expert differs from expert-1 —
    // exactly what the `:502` guard checks for, and exactly what a per-project
    // override (`source: 'project'`) ordinarily produces.
    http.expectOne((r) => r.url.includes('/expert-defaults')).flush({
      personal_defaults_allowed: true,
      defaults: {
        worker: {application: null, personal: null, effective: null, source: 'application'},
        session: {application: {id: 'expert-1'}, personal: null, effective: {id: 'expert-2'}, source: 'project'},
      },
    });
    // That resolution just auto-selected expert-2 and fetched its detail — a
    // request the user never asked for and a control (the expert grid) they
    // never touched.
    http.expectOne(
      (r) => r.urlWithParams.endsWith('/experts/expert-2?account_defaults=true&role=session'),
    ).flush({config: {llm: {}}});

    // The reasoning pick is gone, with no trace it ever reset — the bug as
    // reported — reached via a click on a project chip, minutes after the
    // pick, with no model change and no expert-card click at all.
    expect(modelGroup.sessionReasoning()).toBeNull();
    // The fix (task 3): the reset itself is correct and stays — reasoning
    // vocabularies don't translate across families — but it must no longer
    // be silent.
    expect(modelGroup.reasoningResetNotice()).toBe(true);
    // The model pick "survives" only by coincidence: prefillFromConfig's
    // no-base-model branch restores from the very localStorage key
    // onModelChange had just written, so it silently re-lands on the
    // identical value — indistinguishable from "my pick stuck." This is why
    // the user only noticed the Reasoning field, not the Model field.
    expect(modelGroup.model()).toBe('gpt-5.6-sol');

    http.verify();
  });

  it('a secondary, narrow-window variant: the plain page-load default-expert resolution lands after an unusually fast pick', () => {
    // Kept for completeness — the mechanism (an involuntary prefillFromConfig)
    // is identical to the primary test above, but this variant's real-world
    // window is only as wide as the page's initial parallel requests take to
    // settle (typically well under a second), which is implausible for a
    // human to beat with two dropdown picks. The project-chip path above is
    // the better fit for the reported incident.
    const {fixture, http, modelGroup} = setupWithRealModelGroup();

    http.expectOne(
      (r) => r.urlWithParams.endsWith('/experts/session_base?type=session&account_defaults=true'),
    ).flush({config: {}});
    http.expectOne((r) => r.url.includes('/datasources/eligible')).flush([]);

    // The user acts before the plain expert-list/effective-default round
    // (already in flight since ngOnInit) has settled. No expert card is ever
    // clicked — expertSelectionTouched stays false for the rest of the test.
    modelGroup.onModelChange('gpt-5.6-sol');
    modelGroup.onSessionReasoningChange('max');
    expect(modelGroup.sessionReasoning()).toBe('max');

    // Only now do the two requests that were already in flight before the
    // user acted — the plain expert list and the effective-default lookup
    // every session-create load issues, with no project or expert
    // interaction required — resolve.
    http.expectOne(
      (r) => r.urlWithParams.endsWith('/experts?type=session'),
    ).flush([
      {id: 'expert-1', display_name: 'Coder', description: '', icon: 'code', color: '#fff', tags: []},
    ]);
    http.expectOne((r) => r.url.includes('/expert-defaults')).flush({
      personal_defaults_allowed: true,
      defaults: {
        worker: {application: null, personal: null, effective: null, source: 'application'},
        session: {application: {id: 'expert-1'}, personal: null, effective: {id: 'expert-1'}, source: 'application'},
      },
    });
    http.expectOne(
      (r) => r.urlWithParams.endsWith('/experts/expert-1?account_defaults=true&role=session'),
    ).flush({config: {llm: {}}});

    expect(modelGroup.sessionReasoning()).toBeNull();
    expect(modelGroup.reasoningResetNotice()).toBe(true);
    expect(modelGroup.model()).toBe('gpt-5.6-sol');

    http.verify();
  });
});


describe('SessionCreateComponent tool preview', () => {
  afterEach(() => TestBed.resetTestingModule());

  function setup(preview: unknown) {
    const api = stubApi(preview);
    TestBed.configureTestingModule({
      providers: [
        provideHttpClient(),
        provideHttpClientTesting(),
        {provide: Router, useValue: {navigate: vi.fn()}},
        {provide: UserService, useValue: {currentUserId: signal<string | null>(null)}},
        {provide: ModelService, useValue: {load: vi.fn()}},
        {provide: ErrorMessageService, useValue: {translate: (_e: unknown, f: string) => f}},
        {
          provide: CapabilitiesService,
          useValue: {protectedCloudAvailable: signal(false), grants: signal(null)},
        },
        {provide: ApiService, useValue: api},
      ],
    });
    TestBed.overrideComponent(SessionCreateComponent, {set: {imports: [], template: ''}});
    const fixture = TestBed.createComponent(SessionCreateComponent);
    const http = TestBed.inject(HttpTestingController);
    return {fixture, http, api};
  }

  const ANSWER = {
    source: 'resolved',
    origin: 'prediction',
    observed_at: null,
    prediction_reason: 'no agent exists for an unsaved session',
    enumerate_only: {shell: ['run_command']},
    tool_groups: {},
    categories: {
      canvas: {state: 'on', settable: true, reason: null, decided_by: 'base', tools: ['get_canvas']},
      shell: {
        state: 'unavailable',
        settable: false,
        reason: 'requires the shell_tools capability grant',
        decided_by: 'grant',
        tools: [],
      },
    },
  };

  it('routes the preview EXACTLY as createSession routes the create', () => {
    // A preview that resolved a different expert layer from the create it
    // previews would be this series' defect rebuilt in the surface built to
    // prevent it. `source: 'user'` marks a DB expert, which must go by
    // expert_id with config_name held at the persistent base.
    const {fixture, api} = setup(ANSWER);
    const component = fixture.componentInstance;
    component.toggleExpert({
      id: 'db-expert-1',
      display_name: 'Coder',
      description: '',
      icon: 'code',
      color: '#fff',
      tags: [],
      source: 'user',
    } as never);

    expect(api.previewToolGroups).toHaveBeenCalledWith({
      config_override: {},
      workspace_preference: null,
      config_name: 'session_base',
      expert_id: 'db-expert-1',
      project_id: null,
    });
  });

  it('a bundled expert previews by config_name, matching create', () => {
    const {fixture, api} = setup(ANSWER);
    fixture.componentInstance.toggleExpert({
      id: 'scholar',
      display_name: 'Scholar',
      description: '',
      icon: 'school',
      color: '#fff',
      tags: [],
      source: 'bundled',
    } as never);

    expect(api.previewToolGroups).toHaveBeenCalledWith({
      config_override: {},
      workspace_preference: null,
      config_name: 'scholar',
      expert_id: null,
      project_id: null,
    });
  });

  it('the answer reaches the settings surface and re-anchors the tool switches', () => {
    const {fixture, api} = setup(ANSWER);
    const component = fixture.componentInstance;
    const prefill = vi.fn();
    component.agentSettings = {
      getOverrides: vi.fn().mockReturnValue({}),
      prefillFromConfig: vi.fn(),
      prefillFromResolvedToolset: prefill,
      hasToolEdits: () => false,
    } as never;

    component.toggleExpert({
      id: 'scholar', display_name: 'S', description: '', icon: 'x', color: '#fff', tags: [],
      source: 'bundled',
    } as never);

    expect(component.toolPreview()).toEqual(ANSWER);
    expect(prefill).toHaveBeenCalledWith(ANSWER.categories);
    expect(api.previewToolGroups).toHaveBeenCalled();
  });

  it('a late answer never clobbers a switch the user has already flipped', () => {
    // Same class of bug as the live pane's, on the other surface: the config
    // prefill has already run by the time this lands.
    const {fixture} = setup(ANSWER);
    const component = fixture.componentInstance;
    const prefill = vi.fn();
    component.agentSettings = {
      getOverrides: vi.fn().mockReturnValue({}),
      prefillFromConfig: vi.fn(),
      prefillFromResolvedToolset: prefill,
      hasToolEdits: () => true,
    } as never;

    component.toggleExpert({
      id: 'scholar', display_name: 'S', description: '', icon: 'x', color: '#fff', tags: [],
      source: 'bundled',
    } as never);

    // The prediction still renders — it is what the rows read — but the
    // baseline is left alone.
    expect(component.toolPreview()).toEqual(ANSWER);
    expect(prefill).not.toHaveBeenCalled();
  });

  it('the project selection moves the prediction, because the project layer can', () => {
    const {fixture, api} = setup(ANSWER);
    fixture.componentInstance.toggleProject('proj-1');

    expect(api.previewToolGroups).toHaveBeenLastCalledWith({
      config_override: {},
      workspace_preference: null,
      config_name: 'session_base',
      expert_id: null,
      project_id: 'proj-1',
    });
  });

  it('a failed preview leaves the surface with no answer rather than a wrong one', () => {
    const {fixture} = setup(null);
    const component = fixture.componentInstance;
    const prefill = vi.fn();
    component.agentSettings = {
      getOverrides: vi.fn().mockReturnValue({}),
      prefillFromConfig: vi.fn(),
      prefillFromResolvedToolset: prefill,
      hasToolEdits: () => false,
    } as never;

    component.toggleProject('proj-1');

    expect(component.toolPreview()).toBeNull();
    expect(prefill).not.toHaveBeenCalled();
  });
});

// Task 14, item B (session_config_drift_resume.md §8.3): "Start a new
// session" prefills project/expert/model/connectors from the source thread
// named by `?from=`. The design point under test is that none of the four
// depend on network ordering between the source-thread fetch and this form's
// own loads (experts, projects, eligible datasources) — whichever settles
// second is what decides, so both orderings must converge on the same answer.
describe('SessionCreateComponent "Start a new session" prefill (session_config_drift_resume.md §8.3)', () => {
  afterEach(() => TestBed.resetTestingModule());

  const THREAD = {
    project_ids: ['proj-1'],
    metadata: {
      expert_id: 'expert-2',
      // ds-2 is deliberately NOT in DATASOURCES below — it drifted (deleted/
      // revoked) between the source thread and this page.
      datasource_ids: ['ds-1', 'ds-2'],
      config_override: {llm: {model: 'gpt-5.6-sol'}},
    },
  };
  const PROJECTS = [{id: 'proj-1', name: 'Proj One', status: 'active', is_default: true}];
  const EXPERTS = [
    {id: 'expert-1', display_name: 'Default', description: '', icon: 'x', color: '#fff', tags: []},
    {id: 'expert-2', display_name: 'Prefilled', description: '', icon: 'x', color: '#fff', tags: []},
  ];
  const DATASOURCES = [
    {
      id: 'ds-1', name: 'DS1', description: null, type: 'kb', connection_url: null,
      cli_hint: null, default_branch: null, job_id: null, created_at: '', updated_at: '',
      default_selected: false,
    },
  ];
  // The ordinary effective-default expert (expert-1) — deliberately DIFFERENT
  // from the thread's expert-2, so a test can tell "the prefill won" apart
  // from "the normal default happened to match".
  const EXPERT_DEFAULTS = {
    personal_defaults_allowed: true,
    defaults: {
      worker: {application: null, personal: null, effective: null, source: 'application'},
      session: {application: {id: 'expert-1'}, personal: null, effective: {id: 'expert-1'}, source: 'application'},
    },
  };

  function setup(from: string | undefined, threadSource: Observable<Record<string, unknown> | null>) {
    const navigate = vi.fn().mockResolvedValue(true);
    const api = stubApi(null, threadSource);
    const providers: unknown[] = [
      provideHttpClient(),
      provideHttpClientTesting(),
      {provide: Router, useValue: {navigate}},
      {provide: UserService, useValue: {currentUserId: signal<string | null>('user-1')}},
      {provide: ModelService, useValue: {load: vi.fn()}},
      {provide: ErrorMessageService, useValue: {translate: (_e: unknown, f: string) => f}},
      {
        provide: CapabilitiesService,
        useValue: {protectedCloudAvailable: signal(false), grants: signal(null)},
      },
      {provide: ApiService, useValue: api},
    ];
    if (from !== undefined) {
      providers.push({provide: ActivatedRoute, useValue: activatedRouteWithFrom(from)});
    }
    TestBed.configureTestingModule({providers});
    TestBed.overrideComponent(SessionCreateComponent, {set: {imports: [], template: ''}});
    const fixture = TestBed.createComponent(SessionCreateComponent);
    const http = TestBed.inject(HttpTestingController);
    const component = fixture.componentInstance;
    const setSessionModelOverride = vi.fn();
    return {fixture, component, http, navigate, api, setSessionModelOverride};
  }

  /**
   * The real AgentSettingsComponent isn't mounted (template overridden away,
   * same as every other describe in this file) — stub exactly the surface
   * this component's prefill logic calls on it.
   *
   * Must run AFTER `fixture.detectChanges()`, not before: `agentSettings` is
   * an `@ViewChild`, and Angular's own view-query resolution — which runs
   * as part of change detection — finds no matching element against the
   * empty overridden template and overwrites the field back to `undefined`,
   * clobbering an earlier assignment. Every other describe in this file that
   * stubs `agentSettings` follows the same order for the same reason.
   */
  function attachAgentSettingsStub(
    component: SessionCreateComponent,
    setSessionModelOverride: ReturnType<typeof vi.fn>,
  ): void {
    component.agentSettings = {
      getOverrides: vi.fn().mockReturnValue({}),
      prefillFromConfig: vi.fn(),
      toolsGroup: {prefillFromConfig: vi.fn()},
      setSessionModelOverride,
      hasToolEdits: () => false,
      prefillFromResolvedToolset: vi.fn(),
    } as never;
  }

  /** Flush every currently-pending request with a canned response chosen by
   *  URL, repeating until nothing is left pending (flushing one can
   *  synchronously spawn another — e.g. selecting a project re-fires
   *  eligible-datasources and expert-defaults). Tolerates any interleaving
   *  and any number of duplicate calls to the same endpoint, which is the
   *  point: these tests care about the FINAL converged state, not the exact
   *  request count for any one ordering. */
  function drainAll(http: HttpTestingController): void {
    for (let round = 0; round < 20; round++) {
      const pending = http.match(() => true);
      if (pending.length === 0) return;
      for (const req of pending) {
        const url = req.request.url;
        if (url.includes('/experts?type=session')) {
          req.flush(EXPERTS);
        } else if (url.includes('/expert-defaults')) {
          req.flush(EXPERT_DEFAULTS);
        } else if (url.includes('/datasources/eligible')) {
          req.flush(DATASOURCES);
        } else if (url.includes('/experts/session_base')) {
          req.flush({config: {}});
        } else if (url.includes('/projects?user_id=')) {
          req.flush(PROJECTS);
        } else if (/\/experts\/(expert-\d)\?/.test(url)) {
          const id = url.match(/\/experts\/(expert-\d)\?/)![1];
          req.flush({...EXPERTS.find((e) => e.id === id), config: {llm: {}}, id});
        } else {
          req.flush(null);
        }
      }
    }
    throw new Error('drainAll: requests kept spawning past the round cap');
  }

  it('with no `from`, the source-thread fetch never happens and the ordinary defaults apply unchanged', () => {
    const {fixture, component, http, api, setSessionModelOverride} = setup(undefined, of(null));
    fixture.detectChanges(); // runs the constructor's effect() + ngOnInit
    attachAgentSettingsStub(component, setSessionModelOverride);
    drainAll(http);

    expect(api.getPersistentThread).not.toHaveBeenCalled();
    expect(component.selectedProjectIds()).toEqual(new Set(['proj-1'])); // account default
    expect(component.selectedExpert()?.id).toBe('expert-1'); // ordinary effective default
    expect(component.prefillDatasourceIds()).toBeNull(); // picker keeps server default_selected
  });

  it('all four prefills apply when the source-thread fetch settles BEFORE this form\'s own loads', () => {
    const subject = new Subject<Record<string, unknown> | null>();
    const {fixture, component, http, setSessionModelOverride} = setup('thread-77', subject.asObservable());
    fixture.detectChanges(); // runs the constructor's effect() + ngOnInit
    attachAgentSettingsStub(component, setSessionModelOverride);

    subject.next(THREAD);
    subject.complete();
    drainAll(http);

    expect(component.selectedProjectIds()).toEqual(new Set(['proj-1']));
    expect(component.selectedExpert()?.id).toBe('expert-2'); // prefill won over the ordinary default (expert-1)
    expect(component.prefillDatasourceIds()).toEqual(['ds-1']); // ds-2 dropped — drifted
    expect(setSessionModelOverride).toHaveBeenCalledWith('gpt-5.6-sol');
  });

  it('all four prefills STILL apply, identically, when the source-thread fetch settles AFTER this form\'s own loads', () => {
    const subject = new Subject<Record<string, unknown> | null>();
    const {fixture, component, http, setSessionModelOverride} = setup('thread-77', subject.asObservable());
    fixture.detectChanges(); // runs the constructor's effect() + ngOnInit
    attachAgentSettingsStub(component, setSessionModelOverride);

    // Everything else settles first. Fix round 1: NEITHER field guesses
    // early — project already didn't (its guard predates this fix); expert
    // now matches it. Before the fix, this is exactly where the ordinary
    // default expert got auto-selected and fetched, calling
    // agentSettings.prefillFromConfig — the unconditional reset (no
    // hasToolEdits() guard) that would go on to fire AGAIN when the prefill
    // corrected the selection below, silently wiping anything a user edited
    // in between. Deferring removes the window a concurrent edit could be
    // lost in, rather than merely converging past it.
    drainAll(http);
    expect(component.selectedExpert()).toBeNull(); // NOT yet defaulted — waiting on the source-thread fetch
    expect(component.selectedProjectIds()).toEqual(new Set()); // NOT yet defaulted — waiting on the source-thread fetch
    expect(setSessionModelOverride).not.toHaveBeenCalled();

    // The source-thread fetch settles late and corrects both fields, and
    // whatever new requests that spawns (a fresh fetchExpertDetail for
    // expert-2, a re-scoped eligible-datasources/expert-defaults call) get
    // drained too.
    subject.next(THREAD);
    subject.complete();
    drainAll(http);

    expect(component.selectedProjectIds()).toEqual(new Set(['proj-1']));
    expect(component.selectedExpert()?.id).toBe('expert-2');
    expect(component.prefillDatasourceIds()).toEqual(['ds-1']);
    expect(setSessionModelOverride).toHaveBeenCalledWith('gpt-5.6-sol');
  });

  // Fix round 1 — the reviewer's specific gap: the two tests above prove the
  // FINAL state converges either way, not that a user's own edit made
  // DURING the window survives. `agentSettings.prefillFromConfig` is the
  // mechanism that would destroy it (AgentSettingsComponent.prefillFromConfig
  // resets modelGroup/toolsGroup/executionGroup unconditionally — no
  // hasToolEdits() check, unlike loadToolPreview's re-anchor), so the
  // load-bearing assertion is how many times it fires and with what: once,
  // for the correct expert, never for a discarded intermediate one.
  it('a tool edit made while the source-thread fetch is still in flight survives the prefill landing', () => {
    const subject = new Subject<Record<string, unknown> | null>();
    const {fixture, component, http, setSessionModelOverride} = setup('thread-77', subject.asObservable());
    fixture.detectChanges();
    attachAgentSettingsStub(component, setSessionModelOverride);
    const prefillFromConfig = (
      component.agentSettings as unknown as {prefillFromConfig: ReturnType<typeof vi.fn>}
    ).prefillFromConfig;

    // Settle every OTHER load while the thread fetch is still pending. Before
    // fix round 1, this alone auto-selected and fetched the ordinary default
    // expert, calling prefillFromConfig and rendering its config into the
    // form — precisely the moment a user could open the tools tab and flip a
    // switch. Post-fix, nothing is selected yet, so there is nothing loaded
    // for an edit to race against in the first place.
    drainAll(http);
    expect(component.selectedExpert()).toBeNull();
    expect(prefillFromConfig).not.toHaveBeenCalled();

    // The user's edit would happen here, against a form that has rendered
    // no expert config at all — there is no in-flight state for it to lose
    // to a later reset.

    // The source-thread fetch settles late and selects/fetches expert-2 —
    // the only expert this session ever renders.
    subject.next(THREAD);
    subject.complete();
    drainAll(http);

    expect(component.selectedExpert()?.id).toBe('expert-2');
    // Called exactly once. Two calls (once for a discarded expert-1, once
    // for the real expert-2) is exactly the shape that would have silently
    // clobbered an edit made in between — this is what fails without the
    // round-1 fix (temporarily reverting it reproduces `toHaveBeenCalledTimes(2)`).
    expect(prefillFromConfig).toHaveBeenCalledTimes(1);
    expect(prefillFromConfig).toHaveBeenCalledWith({llm: {}});
  });

  it('a failed source-thread fetch leaves the form open and usable, applying the ordinary defaults instead', () => {
    // ApiService.getPersistentThread never throws (it catchErrors to `of(null)`
    // internally) — a failed fetch and "no `from`" are the same signal here.
    const subject = new Subject<Record<string, unknown> | null>();
    const {fixture, component, http, setSessionModelOverride} = setup('thread-dead', subject.asObservable());
    fixture.detectChanges(); // runs the constructor's effect() + ngOnInit
    attachAgentSettingsStub(component, setSessionModelOverride);

    subject.next(null);
    subject.complete();
    drainAll(http);

    expect(component.createError()).toBeNull();
    expect(component.selectedProjectIds()).toEqual(new Set(['proj-1'])); // account default, unaffected
    expect(component.selectedExpert()?.id).toBe('expert-1');
    expect(component.prefillDatasourceIds()).toBeNull();
    expect(setSessionModelOverride).not.toHaveBeenCalled();
  });

  it('asks the server for active projects only', () => {
    // An archived project cannot take a new session, so offering one in this
    // picker is offering a guaranteed refusal.
    const {fixture, http} = setup(undefined, of(null));
    fixture.detectChanges(); // runs the constructor's effect() + ngOnInit

    const projectReads = http.match((r) => r.url.includes('/projects'));
    expect(projectReads).toHaveLength(1);
    expect(projectReads[0].request.url).toContain('status=active');
    projectReads[0].flush(PROJECTS);
    drainAll(http);
  });

  it("keeps a source thread's now-archived project, flagged and still selected", () => {
    // The alternative is silently creating the session with a narrower scope
    // than the one being copied — a change the user never asked for and would
    // not see. Creating against it is refused server-side; the form renders
    // that refusal.
    const subject = new Subject<Record<string, unknown> | null>();
    const {fixture, component, http, api, setSessionModelOverride} = setup(
      'thread-77',
      subject.asObservable(),
    );
    api.getProject.mockReturnValue(
      of({
        id: 'proj-archived',
        name: 'Better Resavio (pre-split archive)',
        status: 'archived',
        description: null,
      }),
    );
    fixture.detectChanges(); // runs the constructor's effect() + ngOnInit
    attachAgentSettingsStub(component, setSessionModelOverride);

    subject.next({...THREAD, project_ids: ['proj-archived']}); // absent from the active list
    subject.complete();
    drainAll(http);

    expect(api.getProject).toHaveBeenCalledWith('proj-archived');
    expect(component.projects().map((p) => p.id)).toContain('proj-archived');
    expect(component.selectedProjectIds()).toEqual(new Set(['proj-archived']));
    expect(component.archivedSelected()).toBe(true);
  });

  it('a project the source thread had, but this account can no longer see, is dropped rather than falling back to the account default', () => {
    const subject = new Subject<Record<string, unknown> | null>();
    const {fixture, component, http} = setup('thread-77', subject.asObservable());
    fixture.detectChanges(); // runs the constructor's effect() + ngOnInit

    subject.next({...THREAD, project_ids: ['proj-drifted']}); // not in PROJECTS
    subject.complete();
    drainAll(http);

    // PROJECTS' one entry is marked is_default: true — if this were empty
    // because the prefill logic had fallen through to it, the test above
    // ("no `from`") would look identical and this assertion would be
    // meaningless. It stays empty on purpose.
    expect(component.selectedProjectIds()).toEqual(new Set());
  });
});
