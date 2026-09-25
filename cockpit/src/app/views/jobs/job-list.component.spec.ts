import {signal, ɵresolveComponentResources} from '@angular/core';
import {TestBed} from '@angular/core/testing';
import {ActivatedRoute, Router} from '@angular/router';
import {TranslocoService, TranslocoTestingModule} from '@jsverse/transloco';
import {BehaviorSubject, of} from 'rxjs';
import {afterEach, beforeAll, beforeEach, describe, expect, it, vi} from 'vitest';
import {ApiService} from '../../core/services/api.service';
import {DataService} from '../../core/services/data.service';
import {UserService} from '../../core/services/user.service';
import {ViewportService} from '../../core/services/viewport.service';
import {JobSummary} from '../../core/models/audit.model';
import {JobListComponent, jobCloudAction} from './job-list.component';

// The real catalogue, so these specs double as proof the jobs.* keys the
// template asks for actually exist — a key missing from BOTH locales passes
// the i18n parity gate and ships as the raw key string.
import en from '../../../assets/i18n/en.json';

/**
 * The template is replaced with an empty one for every spec here.
 *
 * Not laziness — the JIT compiler vitest runs cannot see initializer-based
 * inputs (`input()`/`model()`), so property-binding any signal input in a
 * TestBed template is NG0303, and this component binds a dozen of them across
 * `ui/` and its own children. Stubbing them all would test the stubs. The
 * template is covered by the AOT build and by driving the real page; what is
 * worth asserting here is the class logic, which the template only displays.
 */
function mountLogic(overrides: {
  api?: Partial<ApiService>;
  params?: BehaviorSubject<ReturnType<typeof paramMap>>;
} = {}) {
  const navigate = vi.fn().mockResolvedValue(true);
  const api = {
    getJobsPage: vi.fn().mockReturnValue(of(page([]))),
    getJobStatisticsFiltered: vi.fn().mockReturnValue(of({total_jobs: 0, by_status: {}})),
    getSnapshotStats: vi.fn().mockReturnValue(of(null)),
    getProjects: vi.fn().mockReturnValue(of([])),
    // Expanding a row lazily loads the three things the list payload cannot
    // supply (model, duration, usage). Defaulted here so every test can expand.
    getJob: vi.fn().mockReturnValue(of(null)),
    getJobUsage: vi.fn().mockReturnValue(of(null)),
    getJobProgress: vi.fn().mockReturnValue(of(null)),
    getJobSubjobs: vi.fn().mockReturnValue(of(null)),
    getJobSubagents: vi.fn().mockReturnValue(of(null)),
    resumeJob: vi.fn().mockReturnValue(of({status: 'resumed'})),
    retryWorkspaceRecovery: vi.fn().mockReturnValue(of({status: 'recovering_workspace'})),
    cancelJob: vi.fn().mockReturnValue(of({status: 'cancelled'})),
    ...overrides.api,
  } as unknown as ApiService;

  TestBed.configureTestingModule({
    imports: [
      JobListComponent,
      TranslocoTestingModule.forRoot({
        langs: {en},
        translocoConfig: {availableLangs: ['en'], defaultLang: 'en'},
      }),
    ],
    providers: [
      {provide: ApiService, useValue: api},
      {provide: DataService, useValue: {setCurrentJob: vi.fn()}},
      {provide: UserService, useValue: {currentUser: signal({id: 'user-1'})}},
      {provide: ViewportService, useValue: {isMobile: signal(false)}},
      {provide: Router, useValue: {navigate}},
      {
        provide: ActivatedRoute,
        useValue: {
          // A bare param map is the steady state and the default view.
          queryParamMap: overrides.params ?? new BehaviorSubject(paramMap({})),
        },
      },
    ],
  });
  TestBed.overrideComponent(JobListComponent, {set: {template: '', imports: []}});
  const transloco = TestBed.inject(TranslocoService);
  transloco.setTranslation(en, 'en');
  transloco.setActiveLang('en');
  const fixture = TestBed.createComponent(JobListComponent);
  return {fixture, component: fixture.componentInstance, api, navigate};
}

function paramMap(values: Record<string, string | string[]>) {
  const normalised = new Map<string, string[]>(
    Object.entries(values).map(([key, value]) => [key, Array.isArray(value) ? value : [value]]),
  );
  return {
    keys: [...normalised.keys()],
    has: (key: string) => normalised.has(key),
    get: (key: string) => normalised.get(key)?.[0] ?? null,
    getAll: (key: string) => normalised.get(key) ?? [],
  };
}

function page(jobs: Partial<JobSummary>[], extra: Record<string, unknown> = {}) {
  return {
    jobs: jobs as JobSummary[],
    total: jobs.length,
    total_is_capped: false,
    has_more: false,
    limit: 25,
    offset: 0,
    as_of: '2026-08-21T00:00:00Z',
    ...extra,
  };
}

function job(id: string, over: Partial<JobSummary> = {}): Partial<JobSummary> {
  return {
    id,
    description: id,
    status: 'completed',
    created_at: '2026-08-20T00:00:00Z',
    is_display_root: true,
    display_root_id: id,
    ...over,
  };
}

describe('JobListComponent — server-resolved tree', () => {
  beforeAll(async () => {
    await ɵresolveComponentResources(() => Promise.resolve(''));
  });
  afterEach(() => TestBed.resetTestingModule());

  it('opens a suspended VM IDE from the click and uses the admitted URL', () => {
    const startIdeSession = vi.fn().mockReturnValue(of({
      status: 'active', access_lease_id: 'lease-1',
      code_server_url: 'https://example.test/api/ide/vm/proxy/_vm/lease-1/',
    }));
    const getIdeSession = vi.fn();
    const {fixture, component} = mountLogic({
      api: {startIdeSession, getIdeSession} as Partial<ApiService>,
    });
    fixture.detectChanges();
    const vm = job('vm', {
      status: 'waiting', workspace_lifecycle: {state: 'suspended'},
    }) as JobSummary;
    component.jobs.set([vm]);
    const tab = {
      opener: null, document: {title: '', body: {textContent: ''}},
      location: {href: ''}, close: vi.fn(), closed: false,
    };
    const opened = vi.spyOn(window, 'open').mockReturnValue(tab as unknown as Window);
    try {
      expect(component.canOpenIde(vm)).toBe(true);
      component.openIde('vm');
      expect(opened).toHaveBeenCalledWith('', '_blank');
      expect(startIdeSession).toHaveBeenCalledWith('vm');
      expect(getIdeSession).not.toHaveBeenCalled();
      expect(tab.location.href).toContain('/api/ide/vm/proxy/_vm/lease-1/');
      expect(tab.close).not.toHaveBeenCalled();
    } finally {
      opened.mockRestore();
    }
  });

  it('keeps blocked/undelivered distinct from cancellation and non-actionable', () => {
    const {fixture, component} = mountLogic();
    fixture.detectChanges();
    const blocked = job('blocked-1', {
      status: 'cancelled',
      completion_outcome_kind: 'blocked_undelivered',
    }) as JobSummary;

    expect(component.effectiveJobStatus(blocked)).toBe('blocked_undelivered');
    expect(component.isBlockedUndelivered(blocked)).toBe(true);
    expect(component.jobStatusTone(component.effectiveJobStatus(blocked))).toBe('warning');
    expect(en.jobs.status.blocked_undelivered).toBe('Blocked / undelivered');
  });

  it('does not send stale Resume after creation advice becomes non-resumable', () => {
    const {fixture, component, api} = mountLogic();
    fixture.detectChanges();
    component.jobs.set([job('creation-job', {
      status: 'paused', vm_creation: {
        request_id: 'creation-id', state: 'reconciling', stage: 'creation',
        reason_code: 'capacity_wait', message: 'Waiting for VM capacity.', resumable: false,
      },
    }) as JobSummary]);
    component.resumeJob('creation-job');
    expect(api.resumeJob).not.toHaveBeenCalled();
    component.jobs.update(rows => rows.map(row => ({...row, vm_creation: {...row.vm_creation!, resumable: true}})));
    component.resumeJob('creation-job');
    expect(api.resumeJob).toHaveBeenCalledWith('creation-job');
  });

  it('routes paused workspace recovery through Retry and keeps Cancel available', () => {
    const retryWorkspaceRecovery = vi
      .fn()
      .mockReturnValue(of({status: 'recovering_workspace'}));
    const resumeJob = vi.fn().mockReturnValue(of({status: 'resumed'}));
    const cancelJob = vi.fn().mockReturnValue(of({status: 'cancelled'}));
    const recovering = job('recovery-owner', {
      status: 'paused',
      workspace_recovery: {
        operation_id: '22222222-bbbb-4222-8222-222222222222',
        state: 'paused_attention',
        reason_code: 'prior_runtime_unfenced',
        message: 'Previous workspace execution could not be proven stopped.',
        started_at: '2026-09-16T08:00:00Z',
        deadline_at: '2026-09-16T08:15:00Z',
        next_check_at: null,
        retryable: true,
        cleanup_pending: true,
      },
    }) as JobSummary;
    const {fixture, component} = mountLogic({
      api: {
        getJobsPage: vi.fn().mockReturnValue(of(page([recovering]))),
        retryWorkspaceRecovery,
        resumeJob,
        cancelJob,
      } as Partial<ApiService>,
    });
    fixture.detectChanges();

    expect(component.effectiveJobStatus(recovering)).toBe('recovery_paused');
    component.retryWorkspaceRecovery(recovering);
    component.cancelJob(recovering.id);

    expect(retryWorkspaceRecovery).toHaveBeenCalledWith(
      recovering.id,
      recovering.workspace_recovery?.operation_id,
      expect.any(String),
    );
    expect(cancelJob).toHaveBeenCalledWith(recovering.id);
    expect(resumeJob).not.toHaveBeenCalled();
    expect(en.jobs.action.retryRecovery).toBe('Retry recovery');
    expect(en.jobs.action.cancel).toBe('Cancel');
  });

  it('does not offer recovery retry for a shared child participant', () => {
    const retryWorkspaceRecovery = vi.fn().mockReturnValue(of(null));
    const child = job('recovery-child', {
      is_display_root: false,
      display_root_id: 'recovery-owner',
      status: 'paused',
      workspace_recovery: {
        operation_id: '22222222-bbbb-4222-8222-222222222222',
        state: 'paused_attention',
        reason_code: 'prior_runtime_unfenced',
        message: 'Previous workspace execution could not be proven stopped.',
        started_at: '2026-09-16T08:00:00Z',
        deadline_at: '2026-09-16T08:15:00Z',
        next_check_at: null,
        retryable: false,
        cleanup_pending: true,
      },
    }) as JobSummary;
    const {fixture, component} = mountLogic({
      api: {retryWorkspaceRecovery} as Partial<ApiService>,
    });
    fixture.detectChanges();

    component.retryWorkspaceRecovery(child);

    expect(retryWorkspaceRecovery).not.toHaveBeenCalled();
  });

  it('replays one recovery request id until read-back shows a successor', () => {
    const randomUUID = vi
      .spyOn(crypto, 'randomUUID')
      .mockReturnValueOnce('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa')
      .mockReturnValueOnce('bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb');
    const paused = job('recovery-owner', {
      status: 'paused',
      workspace_recovery: {
        operation_id: '22222222-bbbb-4222-8222-222222222222',
        state: 'paused_attention',
        reason_code: 'prior_runtime_unfenced',
        message: 'Previous workspace execution could not be proven stopped.',
        started_at: '2026-09-16T08:00:00Z',
        deadline_at: '2026-09-16T08:15:00Z',
        next_check_at: null,
        retryable: true,
        cleanup_pending: true,
      },
    }) as JobSummary;
    const successor = job('recovery-owner', {
      ...paused,
      workspace_recovery: {
        ...paused.workspace_recovery!,
        operation_id: '33333333-cccc-4333-8333-333333333333',
      },
    }) as JobSummary;
    const getJobsPage = vi
      .fn()
      .mockReturnValueOnce(of(page([paused])))
      .mockReturnValueOnce(of(page([paused])))
      .mockReturnValueOnce(of(page([successor])))
      .mockReturnValue(of(page([successor])));
    const retryWorkspaceRecovery = vi.fn().mockReturnValue(of(null));
    const {fixture, component} = mountLogic({
      api: {getJobsPage, retryWorkspaceRecovery} as Partial<ApiService>,
    });
    fixture.detectChanges();

    component.retryWorkspaceRecovery(component.jobs()[0]);
    component.retryWorkspaceRecovery(component.jobs()[0]);
    component.retryWorkspaceRecovery(component.jobs()[0]);

    expect(retryWorkspaceRecovery.mock.calls).toEqual([
      [
        'recovery-owner',
        '22222222-bbbb-4222-8222-222222222222',
        'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
      ],
      [
        'recovery-owner',
        '22222222-bbbb-4222-8222-222222222222',
        'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
      ],
      [
        'recovery-owner',
        '33333333-cccc-4333-8333-333333333333',
        'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb',
      ],
    ]);
    expect(getJobsPage).toHaveBeenCalledTimes(4);
    randomUUID.mockRestore();
  });

  it('renders each display root once, with children only when expanded', () => {
    const rows = [
      job('root-1'),
      job('kid-a', {is_display_root: false, display_root_id: 'root-1', parent_job_id: 'root-1'}),
      job('kid-b', {is_display_root: false, display_root_id: 'root-1', parent_job_id: 'root-1'}),
      job('root-2'),
    ];
    const {fixture, component} = mountLogic({
      api: {getJobsPage: vi.fn().mockReturnValue(of(page(rows)))} as Partial<ApiService>,
    });
    fixture.detectChanges();

    expect(component.displayRows().map((row) => row.job.id)).toEqual(['root-1', 'root-2']);
    expect(component.displayRows()[0].hasChildren).toBe(true);

    component.toggleExpand('root-1');
    // Expanding now yields the detail panel first, then the children still
    // nested under it — one gesture, one expansion, both meanings. The panel
    // shares the root's job id, which is why the @for tracks kind + id.
    expect(component.displayRows().map((row) => `${row.kind}:${row.job.id}`)).toEqual([
      'job:root-1',
      'detail:root-1',
      'job:kid-a',
      'job:kid-b',
      'job:root-2',
    ]);
  });

  it('loads panel data once per job and never again on re-expand', () => {
    const getJob = vi.fn().mockReturnValue(of(null));
    const getJobUsage = vi.fn().mockReturnValue(of(null));
    const {fixture, component} = mountLogic({
      api: {
        getJobsPage: vi.fn().mockReturnValue(of(page([job('root-1')]))),
        getJob,
        getJobUsage,
      } as Partial<ApiService>,
    });
    fixture.detectChanges();

    component.toggleExpand('root-1');
    component.toggleExpand('root-1'); // collapse
    component.toggleExpand('root-1'); // re-expand — must be free

    expect(getJob).toHaveBeenCalledTimes(1);
    expect(getJobUsage).toHaveBeenCalledTimes(1);
  });

  it('opens the panel when the row itself is clicked, not just the chevron', () => {
    const {fixture, component} = mountLogic({
      api: {
        getJobsPage: vi.fn().mockReturnValue(of(page([job('root-1')]))),
      } as Partial<ApiService>,
    });
    fixture.detectChanges();

    component.onRowClick('root-1');
    expect(component.isExpanded('root-1')).toBe(true);
    expect(component.selectedJobId()).toBe('root-1');

    component.onRowClick('root-1');
    expect(component.isExpanded('root-1')).toBe(false);
  });

  it('does not toggle when the click ended a text selection', () => {
    // The job id lives in the row and gets copied constantly. Selecting it must
    // not collapse the panel out from under the cursor.
    const {fixture, component} = mountLogic({
      api: {
        getJobsPage: vi.fn().mockReturnValue(of(page([job('root-1')]))),
      } as Partial<ApiService>,
    });
    fixture.detectChanges();

    const selection = {
      isCollapsed: false,
      toString: () => 'root-1',
    } as unknown as Selection;
    const spy = vi.spyOn(window, 'getSelection').mockReturnValue(selection);
    try {
      component.onRowClick('root-1', new MouseEvent('click'));
      expect(component.isExpanded('root-1')).toBe(false);
      // Selection still counts as picking the row out, just not as expanding it.
      expect(component.selectedJobId()).toBe('root-1');
    } finally {
      spy.mockRestore();
    }
  });

  it('fetches the subtree figure only when asked, and only once', () => {
    // Most jobs are leaves, so a subtree call made eagerly would usually return
    // the number already on screen. It is worth an extra request only when
    // someone actually reaches for it — a tree can be many times its root.
    const getJobUsage = vi.fn().mockReturnValue(of(null));
    const {fixture, component} = mountLogic({
      api: {
        getJobsPage: vi.fn().mockReturnValue(of(page([job('root-1')]))),
        getJobUsage,
      } as Partial<ApiService>,
    });
    fixture.detectChanges();

    component.toggleExpand('root-1');
    expect(getJobUsage).toHaveBeenCalledTimes(1);
    expect(getJobUsage).toHaveBeenLastCalledWith('root-1');

    component.loadSubtreeUsage('root-1');
    expect(getJobUsage).toHaveBeenCalledTimes(2);
    expect(getJobUsage).toHaveBeenLastCalledWith('root-1', true);

    // A null result means the call failed; asking again on every click would
    // hammer it, so the loaded flag has to stop that.
    component.loadSubtreeUsage('root-1');
    expect(getJobUsage).toHaveBeenCalledTimes(2);
  });

  it('does not request a subtree for a job whose panel was never opened', () => {
    const getJobUsage = vi.fn().mockReturnValue(of(null));
    const {fixture, component} = mountLogic({
      api: {
        getJobsPage: vi.fn().mockReturnValue(of(page([job('root-1')]))),
        getJobUsage,
      } as Partial<ApiService>,
    });
    fixture.detectChanges();

    component.loadSubtreeUsage('root-1');
    expect(getJobUsage).not.toHaveBeenCalled();
  });

  it('keeps the own-scope figure when the subtree loads', () => {
    // Two independent figures, not one that overwrites the other — switching
    // scope back must not refetch or show the wrong number.
    const own = {job_count: 1, state: 'measured'} as never;
    const subtree = {job_count: 3, state: 'measured'} as never;
    const getJobUsage = vi
      .fn()
      .mockReturnValueOnce(of(own))
      .mockReturnValueOnce(of(subtree));
    const {fixture, component} = mountLogic({
      api: {
        getJobsPage: vi.fn().mockReturnValue(of(page([job('root-1')]))),
        getJobUsage,
      } as Partial<ApiService>,
    });
    fixture.detectChanges();

    component.toggleExpand('root-1');
    component.loadSubtreeUsage('root-1');

    const state = component.jobDetails()['root-1'];
    expect(state.usage).toBe(own);
    expect(state.usageSubtree).toBe(subtree);
    expect(state.loadingSubtree).toBe(false);
  });

  it('refreshes an open panel — cost must not stay frozen at first-open', () => {
    // The cache lives for the whole view, so before this an explicit Refresh
    // reloaded the rows underneath an open panel while its tokens and cost
    // stayed at whatever they were when it was first expanded.
    const getJob = vi.fn().mockReturnValue(of(null));
    const getJobUsage = vi.fn().mockReturnValue(of(null));
    const {fixture, component} = mountLogic({
      api: {
        getJobsPage: vi.fn().mockReturnValue(of(page([job('root-1')]))),
        getJob,
        getJobUsage,
      } as Partial<ApiService>,
    });
    fixture.detectChanges();

    component.toggleExpand('root-1');
    expect(getJobUsage).toHaveBeenCalledTimes(1);

    component.refresh();
    expect(getJobUsage).toHaveBeenCalledTimes(2);
    expect(getJob).toHaveBeenCalledTimes(2);
  });

  it('keeps the subtree scope loaded across a refresh', () => {
    // Dropping the reader back to own-spend on refresh would be its own lie —
    // the two figures differ by more than 10x on a real tree.
    const getJobUsage = vi.fn().mockReturnValue(of(null));
    const {fixture, component} = mountLogic({
      api: {
        getJobsPage: vi.fn().mockReturnValue(of(page([job('root-1')]))),
        getJobUsage,
      } as Partial<ApiService>,
    });
    fixture.detectChanges();

    component.toggleExpand('root-1');
    component.loadSubtreeUsage('root-1');
    expect(getJobUsage).toHaveBeenCalledTimes(2);

    component.refresh();
    // own + subtree again, not own alone.
    expect(getJobUsage).toHaveBeenCalledTimes(4);
    expect(getJobUsage).toHaveBeenLastCalledWith('root-1', true);
  });

  it('refreshes nothing when no panel is open', () => {
    const getJob = vi.fn().mockReturnValue(of(null));
    const {fixture, component} = mountLogic({
      api: {
        getJobsPage: vi.fn().mockReturnValue(of(page([job('root-1')]))),
        getJob,
      } as Partial<ApiService>,
    });
    fixture.detectChanges();

    component.refresh();
    expect(getJob).not.toHaveBeenCalled();
  });

  it('expands a row that has no children at all', () => {
    // The chevron used to appear only on parents. Details are worth reading on
    // a leaf job too, so every row now carries the gesture.
    const {fixture, component} = mountLogic({
      api: {
        getJobsPage: vi.fn().mockReturnValue(of(page([job('lonely')]))),
      } as Partial<ApiService>,
    });
    fixture.detectChanges();

    expect(component.displayRows()[0].hasChildren).toBe(false);
    component.toggleExpand('lonely');
    expect(component.displayRows().map((row) => row.kind)).toEqual(['job', 'detail']);
  });

  it('counts display roots, not rows — the page size is expressed in roots', () => {
    const rows = [
      job('root-1'),
      job('kid-a', {is_display_root: false, display_root_id: 'root-1'}),
      job('root-2'),
    ];
    const {fixture, component} = mountLogic({
      api: {getJobsPage: vi.fn().mockReturnValue(of(page(rows)))} as Partial<ApiService>,
    });
    fixture.detectChanges();

    expect(component.rootCount()).toBe(2);
    expect(component.getChildCount('root-1')).toBe(1);
  });

  /**
   * The subjob roster.
   *
   * A parent parked in `waiting` is blocked on a child, but the default origin
   * filter (`user`, `session`) excludes every subjob from the matched set, so
   * the row rides along with no children and reads as stalled work. The panel
   * asks the server for the real tree instead; these pin the wiring that makes
   * that possible and the escape hatch back to real rows.
   */

  it('fetches the roster with the rest of the panel, not lazily', () => {
    // Unlike the subtree COST figure, which is lazy because most jobs are
    // leaves and its answer usually repeats what is already shown. The roster
    // is the opposite: the reader cannot know to ask for it.
    const getJobSubjobs = vi.fn().mockReturnValue(of(null));
    const {fixture, component} = mountLogic({
      api: {
        getJobsPage: vi.fn().mockReturnValue(of(page([job('root-1')]))),
        getJobSubjobs,
      } as Partial<ApiService>,
    });
    fixture.detectChanges();

    component.toggleExpand('root-1');

    expect(getJobSubjobs).toHaveBeenCalledTimes(1);
    expect(getJobSubjobs).toHaveBeenCalledWith('root-1');
  });

  it('fetches and unwraps subagents with the rest of the panel', () => {
    const roster = {
      job_id: 'root-1',
      count: 1,
      subagents: [{thread_id: 'thread-a', handle: 'tester-7f3a', status: 'running'}],
    };
    const getJobSubagents = vi.fn().mockReturnValue(of(roster));
    const {fixture, component} = mountLogic({
      api: {
        getJobsPage: vi.fn().mockReturnValue(of(page([job('root-1')]))),
        getJobSubagents,
      } as Partial<ApiService>,
    });
    fixture.detectChanges();

    component.toggleExpand('root-1');

    expect(getJobSubagents).toHaveBeenCalledWith('root-1');
    expect(component.jobDetails()['root-1'].subagents).toEqual(roster.subagents);
  });

  it('unwraps the roster envelope into the panel state', () => {
    const roster = {
      job_id: 'root-1',
      count: 1,
      subjobs: [{id: 'kid-a', status: 'processing', config_name: 'scholar'}],
    };
    const {fixture, component} = mountLogic({
      api: {
        getJobsPage: vi.fn().mockReturnValue(of(page([job('root-1')]))),
        getJobSubjobs: vi.fn().mockReturnValue(of(roster)),
      } as Partial<ApiService>,
    });
    fixture.detectChanges();

    component.toggleExpand('root-1');

    expect(component.jobDetails()['root-1'].subjobs).toEqual(roster.subjobs);
  });

  it('keeps the panel alive when only the roster call fails', () => {
    // Each source degrades to null independently; `error` is reserved for a
    // total failure, so a missing roster must not blank out the cost figures.
    const {fixture, component} = mountLogic({
      api: {
        getJobsPage: vi.fn().mockReturnValue(of(page([job('root-1')]))),
        getJob: vi.fn().mockReturnValue(of({id: 'root-1'})),
        getJobSubjobs: vi.fn().mockReturnValue(of(null)),
      } as Partial<ApiService>,
    });
    fixture.detectChanges();

    component.toggleExpand('root-1');

    const state = component.jobDetails()['root-1'];
    expect(state.error).toBe(false);
    expect(state.subjobs).toBeNull();
  });

  it('adds subjob to the origin filter when asked to show hidden rows', () => {
    const {fixture, component, navigate} = mountLogic({
      api: {
        getJobsPage: vi.fn().mockReturnValue(of(page([job('root-1')]))),
      } as Partial<ApiService>,
    });
    fixture.detectChanges();
    navigate.mockClear();

    component.revealSubjobRows();

    const extras = navigate.mock.calls[0][1];
    expect(extras.queryParams.origin).toEqual(['user', 'session', 'subjob']);
  });

  it('widens the filter rather than clearing it', () => {
    // The reader asked to see subjobs, not to also unhide automation, loop,
    // officer and bench work. Clearing would be a much larger change than the
    // one the button offers.
    const {fixture, component, navigate} = mountLogic({
      api: {
        getJobsPage: vi.fn().mockReturnValue(of(page([job('root-1')]))),
      } as Partial<ApiService>,
    });
    fixture.detectChanges();
    navigate.mockClear();

    component.revealSubjobRows();

    expect(navigate.mock.calls[0][1].queryParams.origin).not.toBe('all');
  });

  it('does not touch an origin filter that already shows everything', () => {
    // An empty origin list is the "every origin" sentinel; appending `subjob`
    // to it would NARROW the view to three origins while the reader was asking
    // to see more.
    const params = new BehaviorSubject(paramMap({origin: 'all'}));
    const {fixture, component, navigate} = mountLogic({
      api: {getJobsPage: vi.fn().mockReturnValue(of(page([job('root-1')])))} as Partial<ApiService>,
      params,
    });
    fixture.detectChanges();
    navigate.mockClear();

    component.revealSubjobRows();

    expect(navigate).not.toHaveBeenCalled();
  });

  it('does not re-add subjob when it is already in the filter', () => {
    const params = new BehaviorSubject(paramMap({origin: ['user', 'subjob']}));
    const {fixture, component, navigate} = mountLogic({
      api: {getJobsPage: vi.fn().mockReturnValue(of(page([job('root-1')])))} as Partial<ApiService>,
      params,
    });
    fixture.detectChanges();
    navigate.mockClear();

    component.revealSubjobRows();

    expect(navigate).not.toHaveBeenCalled();
  });

  it('opens a roster subjob that is also on screen, expanding its panel', () => {
    const rows = [
      job('root-1'),
      job('kid-a', {is_display_root: false, display_root_id: 'root-1'}),
    ];
    const {fixture, component} = mountLogic({
      api: {getJobsPage: vi.fn().mockReturnValue(of(page(rows)))} as Partial<ApiService>,
    });
    fixture.detectChanges();

    component.openSubjob('kid-a');

    expect(component.selectedJobId()).toBe('kid-a');
    expect(component.isExpanded('kid-a')).toBe(true);
  });

  it('selects a roster subjob that is not on screen without inventing a row', () => {
    // The common case: the roster lists a child the filter is hiding, so there
    // is nothing in the list to expand. Selecting it must still work rather
    // than throwing or leaving a phantom expanded id behind.
    const {fixture, component} = mountLogic({
      api: {getJobsPage: vi.fn().mockReturnValue(of(page([job('root-1')])))} as Partial<ApiService>,
    });
    fixture.detectChanges();

    component.openSubjob('hidden-kid');

    expect(component.selectedJobId()).toBe('hidden-kid');
    expect(component.isExpanded('hidden-kid')).toBe(false);
  });

  it('reports the FILTERED child count, so the expander cannot overpromise', () => {
    // The server sent one child because the filter excluded the sibling.
    const rows = [
      job('root-1'),
      job('kid-a', {is_display_root: false, display_root_id: 'root-1'}),
    ];
    const {fixture, component} = mountLogic({
      api: {getJobsPage: vi.fn().mockReturnValue(of(page(rows)))} as Partial<ApiService>,
    });
    fixture.detectChanges();
    expect(component.getChildCount('root-1')).toBe(1);
  });

  it('treats a row with no tree fields as a root, so an older server still renders', () => {
    const {fixture, component} = mountLogic({
      api: {
        getJobsPage: vi.fn().mockReturnValue(of(page([{id: 'a'} as Partial<JobSummary>]))),
      } as Partial<ApiService>,
    });
    fixture.detectChanges();
    expect(component.displayRows().map((row) => row.job.id)).toEqual(['a']);
  });

  it('formats the safe requested, assigned, effective and mismatch projection', async () => {
    const workspaceJob = job('vm-job', {
      workspace_contract: {
        requested_backend: 'vm',
        assigned_backend: 'vm',
        effective_backend: null,
        state: 'mismatch',
        failure: 'sandbox_ready_for_vm_assignment',
        stale_backend: 'sandbox',
      },
    }) as JobSummary;
    const {fixture, component} = mountLogic();
    fixture.detectChanges();
    await fixture.whenStable();
    fixture.detectChanges();

    expect(component.workspaceContractSummary(workspaceJob)).toContain(
      'requested vm · assigned vm · effective unavailable',
    );
    expect(component.workspaceContractTitle(workspaceJob)).toContain(
      'mismatch · detail: sandbox_ready_for_vm_assignment',
    );
    expect(component.workspaceContractSummary(workspaceJob)).not.toContain('ssh');
  });

  it('describes a terminal VM without a runtime without claiming cleanup finished', () => {
    const {fixture, component} = mountLogic();
    fixture.detectChanges();
    const missingRuntime = {
      requested_backend: 'vm' as const,
      assigned_backend: 'vm' as const,
      effective_backend: null,
      state: 'waiting',
      failure: 'vm_runtime_not_ready',
    };
    const disposed = job('disposed', {
      status: 'cancelled', workspace_contract: missingRuntime,
    }) as JobSummary;
    const cleanupHeld = job('held', {
      status: 'cancelled', workspace_contract: missingRuntime,
      vm_creation: {
        request_id: 'd4c87f23-fa0b-4d5a-93dc-2a37b2e4a6a9',
        stage: 'creation', state: 'cancel_requested', reason_code: 'job_cancelled',
        message: 'Waiting for VM creation cancellation to be reconciled.',
        resumable: false,
      },
    }) as JobSummary;

    expect(component.workspaceContractTitle(disposed)).toBe('VM workspace unavailable');
    for (const status of ['completed', 'failed']) {
      expect(component.workspaceContractTitle(job(status, {
        status, workspace_contract: missingRuntime,
      }) as JobSummary)).toBe('VM workspace unavailable');
    }
    expect(component.workspaceContractTitle(job('blocked', {
      status: 'cancelled', completion_outcome_kind: 'blocked_undelivered',
      workspace_contract: missingRuntime,
    }) as JobSummary)).toBe('VM workspace unavailable');
    expect(component.workspaceContractTitle(cleanupHeld)).toBe(
      'VM workspace unavailable · Waiting for VM creation cancellation to be reconciled.',
    );
    expect(component.workspaceContractSummary(disposed)).toBe(
      'Workspace: requested vm · assigned vm · effective unavailable',
    );
  });

  it('keeps active waits and terminal retained runtimes in their existing workspace states', () => {
    const {fixture, component} = mountLogic();
    fixture.detectChanges();
    const waiting = {
      requested_backend: 'vm' as const,
      assigned_backend: 'vm' as const,
      effective_backend: null,
      state: 'waiting',
      failure: 'vm_runtime_not_ready',
    };

    for (const status of ['processing', 'paused', 'pending_review']) {
      expect(component.workspaceContractTitle(job(status, {
        status, workspace_contract: waiting,
      }) as JobSummary)).toBe('Workspace state: waiting · detail: vm_runtime_not_ready');
    }
    expect(component.workspaceContractTitle(job('capacity', {
      status: 'paused', workspace_contract: waiting,
      vm_creation: {
        request_id: 'd4c87f23-fa0b-4d5a-93dc-2a37b2e4a6a9',
        stage: 'creation', state: 'reconciling', reason_code: 'capacity_wait',
        message: 'Waiting for VM capacity.', resumable: false,
      },
    }) as JobSummary)).toBe('Workspace state: waiting · detail: vm_runtime_not_ready');
    expect(component.workspaceContractTitle(job('retained', {
      status: 'completed',
      workspace_contract: {...waiting, state: 'ready', failure: null, effective_backend: 'vm'},
    }) as JobSummary)).toBe('Workspace state: ready · detail: none');
    expect(component.workspaceContractTitle(job('recovering', {
      status: 'cancelled', workspace_contract: waiting,
      workspace_recovery: {
        operation_id: '22222222-bbbb-4222-8222-222222222222',
        state: 'recovering', reason_code: 'prior_runtime_unfenced',
        message: 'Workspace recovery is running.',
        started_at: '2026-09-16T08:00:00Z',
        deadline_at: '2026-09-16T08:15:00Z',
        next_check_at: null, retryable: false, cleanup_pending: true,
      },
    }) as JobSummary)).toBe('Workspace state: waiting · detail: vm_runtime_not_ready');
    expect(component.workspaceContractTitle(job('sandbox', {
      status: 'cancelled',
      workspace_contract: {...waiting, assigned_backend: 'sandbox', failure: 'sandbox_runtime_not_ready'},
    }) as JobSummary)).toBe('Workspace state: waiting · detail: sandbox_runtime_not_ready');
  });
});

describe('JobListComponent — filters drive the URL', () => {
  beforeAll(async () => {
    await ɵresolveComponentResources(() => Promise.resolve(''));
  });
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => {
    vi.useRealTimers();
    TestBed.resetTestingModule();
  });

  it('debounces search into a single replaceUrl navigation', async () => {
    const {fixture, component, navigate} = mountLogic();
    fixture.detectChanges();
    navigate.mockClear();

    component.onSearchInput('d3');
    component.onSearchInput('d30');
    component.onSearchInput('d30d6e8a');
    expect(navigate).not.toHaveBeenCalled();

    await vi.advanceTimersByTimeAsync(300);

    expect(navigate).toHaveBeenCalledTimes(1);
    const [, extras] = navigate.mock.calls[0];
    expect(extras.queryParams.search).toBe('d30d6e8a');
    // replaceUrl while typing, or the back button steps through every keystroke.
    expect(extras.replaceUrl).toBe(true);
  });

  it('resets to page 1 on any filter change', () => {
    const params = new BehaviorSubject(paramMap({page: '3'}));
    const {fixture, component, navigate} = mountLogic({params});
    fixture.detectChanges();
    expect(component.filters().page).toBe(3);
    navigate.mockClear();

    component.toggleStatus('failed');

    const [, extras] = navigate.mock.calls[0];
    expect(extras.queryParams.status).toEqual(['failed']);
    expect(extras.queryParams.page).toBeNull();
    expect(extras.replaceUrl).toBe(false);
  });

  it('a bare URL IS the default view — hides system-created work, no redirect', async () => {
    const params = new BehaviorSubject(paramMap({}));
    const {fixture, component, api, navigate} = mountLogic({params});
    fixture.detectChanges();
    await Promise.resolve();

    // parseJobFilters treats an absent `origin` as the default rather than as
    // "no filter", so there is nothing to redirect to and the URL stays clean.
    expect(component.filters().origin).toEqual(['user', 'session']);
    expect(navigate).not.toHaveBeenCalled();
    // The default is the cockpit's, not the API's — so it must be SENT.
    expect(api.getJobsPage).toHaveBeenCalled();
    const query = (api.getJobsPage as unknown as {mock: {calls: unknown[][]}}).mock.calls[0][0];
    expect((query as Record<string, unknown>)['origin']).toEqual(['user', 'session']);
  });

  it('collapses the default pair into one honest token', () => {
    const {fixture, component} = mountLogic();
    fixture.detectChanges();

    // Two tokens reading "Source: user" and "Source: session" would describe
    // a default the user never set.
    const tokens = component.tokens();
    expect(tokens.filter((t) => t.kind === 'origin')).toHaveLength(0);
    expect(tokens.map((t) => t.id)).toContain('systemHidden');
  });

  it('removing that token shows every origin, and says so in the URL', () => {
    const {fixture, component, navigate} = mountLogic();
    fixture.detectChanges();
    navigate.mockClear();

    const token = component.tokens().find((t) => t.id === 'systemHidden')!;
    component.onRemoveToken(token);

    // NOT an absent param: absence means the default, so clearing the filter
    // would silently re-apply it. The sentinel is what makes "everything"
    // expressible and shareable.
    const [, extras] = navigate.mock.calls[0];
    expect(extras.queryParams.origin).toBe('all');
  });

  it('round-trips the all-origins sentinel back to every origin', () => {
    const params = new BehaviorSubject(paramMap({origin: 'all'}));
    const {fixture, component} = mountLogic({params});
    fixture.detectChanges();
    expect(component.filters().origin).toEqual([]);
  });

  it('discards an invalid status from the URL instead of erroring', () => {
    const params = new BehaviorSubject(paramMap({status: ['failed', 'nonsense']}));
    const {fixture, component} = mountLogic({params});
    fixture.detectChanges();
    expect(component.filters().status).toEqual(['failed']);
  });

  it('freezes the window when leaving page 1 and thaws on return', () => {
    const {fixture, component, navigate} = mountLogic();
    fixture.detectChanges();
    navigate.mockClear();

    component.goToPage(2);
    expect(navigate.mock.calls[0][1].queryParams.as_of).toBe('2026-08-21T00:00:00Z');

    navigate.mockClear();
    component.goToPage(1);
    expect(navigate.mock.calls[0][1].queryParams.as_of).toBeNull();
  });

  it('keeps a capped total when the next HTTP page skips counting', () => {
    const params = new BehaviorSubject(paramMap({}));
    const stamp = '2026-09-06T09:00:00Z';
    const getJobsPage = vi.fn()
      .mockReturnValueOnce(of(page([job('a')], {total: 10_000, total_is_capped: true, has_more: true, as_of: stamp})))
      .mockReturnValueOnce(of(page([job('b')], {total: null, total_is_capped: false, has_more: false, offset: 25, as_of: stamp})));
    const {fixture, component} = mountLogic({params, api: {getJobsPage} as Partial<ApiService>});
    fixture.detectChanges();
    params.next(paramMap({page: '2', as_of: stamp}));

    const query = getJobsPage.mock.calls[1][0] as Record<string, unknown>;
    expect(query['include_total']).toBe(false);
    expect(query['as_of']).toBe(stamp);
    expect(component.total()).toBe(10_000);
    expect(component.totalIsCapped()).toBe(true);
    expect(component.hasMore()).toBe(false);
    expect(component.jobs().map((row) => row.id)).toEqual(['b']);
  });
});

describe('JobListComponent — live refresh', () => {
  beforeAll(async () => {
    await ɵresolveComponentResources(() => Promise.resolve(''));
  });
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => {
    vi.useRealTimers();
    TestBed.resetTestingModule();
  });

  it('polls on page 1 but not past it', async () => {
    const getJobsPage = vi.fn().mockReturnValue(of(page([job('a')])));
    const {fixture, component, api} = mountLogic({api: {getJobsPage} as Partial<ApiService>});
    fixture.detectChanges();
    const afterLoad = getJobsPage.mock.calls.length;

    await vi.advanceTimersByTimeAsync(30_000);
    expect(getJobsPage.mock.calls.length).toBeGreaterThan(afterLoad);

    // Auto-refresh plus offset paging skips and duplicates rows, so the poller
    // is hard-gated to the first page.
    component.filters.set({...component.filters(), page: 3});
    const beforeIdle = getJobsPage.mock.calls.length;
    await vi.advanceTimersByTimeAsync(30_000);
    expect(getJobsPage.mock.calls.length).toBe(beforeIdle);
    expect(api).toBeDefined();
  });

  it('does not poll while paused', async () => {
    const getJobsPage = vi.fn().mockReturnValue(of(page([job('a')])));
    const {fixture, component} = mountLogic({api: {getJobsPage} as Partial<ApiService>});
    fixture.detectChanges();

    component.toggleLive();
    expect(component.livePaused()).toBe(true);
    const before = getJobsPage.mock.calls.length;
    await vi.advanceTimersByTimeAsync(30_000);
    expect(getJobsPage.mock.calls.length).toBe(before);
  });

  it('refreshes an expanded live job roster and stops after the parent is terminal', async () => {
    let row = job('root-1', {status: 'processing'});
    const getJobsPage = vi.fn().mockImplementation(() => of(page([row])));
    const getJobSubagents = vi.fn().mockReturnValue(of({
      job_id: 'root-1',
      count: 0,
      subagents: [],
    }));
    const {fixture, component} = mountLogic({
      api: {getJobsPage, getJobSubagents} as Partial<ApiService>,
    });
    fixture.detectChanges();
    component.toggleExpand('root-1');
    expect(getJobSubagents).toHaveBeenCalledTimes(1);

    await vi.advanceTimersByTimeAsync(30_000);
    expect(getJobSubagents).toHaveBeenCalledTimes(2);

    row = job('root-1', {status: 'completed'});
    component.jobs.set([row as JobSummary]);
    await vi.advanceTimersByTimeAsync(30_000);
    expect(getJobSubagents).toHaveBeenCalledTimes(2);
  });

  it('announces new jobs instead of splicing them above the cursor', async () => {
    const first = page([job('a')]);
    const grown = page([job('new'), job('a')]);
    const getJobsPage = vi.fn().mockReturnValueOnce(of(first)).mockReturnValue(of(grown));
    const {fixture, component} = mountLogic({api: {getJobsPage} as Partial<ApiService>});
    fixture.detectChanges();
    expect(component.jobs().map((j) => j.id)).toEqual(['a']);

    await vi.advanceTimersByTimeAsync(30_000);

    // Every row here carries Cancel and Delete; inserting above the pointer
    // moves the target under the user's cursor.
    expect(component.pendingNewCount()).toBe(1);
    expect(component.jobs().map((j) => j.id)).toEqual(['a']);
  });
});

describe('jobCloudAction', () => {
  const base = {status: 'completed', cloud_review_mode: 'open_folder'} as Partial<JobSummary>;

  it('offers nothing until the job is completed and in folder mode', () => {
    expect(jobCloudAction({...base, status: 'processing'} as JobSummary)).toBe('none');
    expect(jobCloudAction({...base, cloud_review_mode: 'diff'} as JobSummary)).toBe('none');
  });

  it('offers export first, then the folder once a URL exists', () => {
    expect(jobCloudAction(base as JobSummary)).toBe('export');
    expect(
      jobCloudAction({
        ...base,
        exported_at: '2026-08-20T00:00:00Z',
        exported_folder_url: 'https://cloud.example/f/1',
      } as JobSummary),
    ).toBe('open');
  });

  it('degrades to "exported" when the backend cannot hand back a URL', () => {
    // Export happened, but there is nothing to link to — two separate actions
    // exist precisely so this state is representable.
    expect(
      jobCloudAction({...base, exported_at: '2026-08-20T00:00:00Z'} as JobSummary),
    ).toBe('exported');
  });
});
