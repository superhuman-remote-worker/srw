import {signal, ɵresolveComponentResources} from '@angular/core';
import {TestBed} from '@angular/core/testing';
import {provideRouter} from '@angular/router';
import {TranslocoService, TranslocoTestingModule} from '@jsverse/transloco';
import {afterEach, beforeAll, describe, expect, it} from 'vitest';
import {
  costDisplay,
  formatCount,
  formatDurationSeconds,
  formatUsd,
  heldForReviewReason,
  jobModelLabel,
  liveSubjobCount,
  JobDetailPanelComponent,
  shortJobId,
  subagentElapsedSeconds,
  subagentStatusTone,
  subjobBlockedKey,
  subjobElapsedSeconds,
  workspaceBackendLabel,
} from './job-detail-panel.component';
import type {Job, JobSubagent, JobSubjob, JobUsage} from '../../core/models/api.model';
import type {JobSummary} from '../../core/models/audit.model';
import en from '../../../assets/i18n/en.json';

/**
 * The panel's job is to be honest about what it does not know, so that is what
 * is pinned here. `GET /api/jobs/{id}/usage` distinguishes four states and a
 * nullable price precisely because "unknown" and "zero" are different claims —
 * on the dev cluster a job with 1.05M tokens legitimately has no cost at all,
 * because the model it ran on has no rate card. Rendering that as $0.00 is the
 * failure this component exists to avoid.
 *
 * Pure helpers, tested directly: vitest runs Angular JIT, where signal inputs
 * cannot be property-bound (NG0303/NG0950) and `componentRef.setInput()` does
 * not work around it, so a rendered test would assert the harness.
 */

function usage(over: Partial<JobUsage> = {}): JobUsage {
  return {
    job_id: 'j',
    scope: 'job',
    job_count: 1,
    state: 'measured',
    window: {from: '2026-08-01T00:00:00Z', to: '2026-08-23T00:00:00Z'},
    freshness: {as_of: '2026-08-23T00:00:00Z', live: false, lag_seconds: 180},
    rows: [],
    llm: {
      prompt_tokens: 0,
      cached_prompt_tokens: 0,
      completion_tokens: 0,
      total_tokens: 0,
      cache_hit_ratio: 0,
    },
    by_category: [],
    cost: {usd: 0.94, complete: true, priced_events: 10, events: 10},
    ...over,
  };
}

describe('workspace recovery detail', () => {
  beforeAll(async () => {
    await ɵresolveComponentResources(() => Promise.resolve(''));
  });
  afterEach(() => TestBed.resetTestingModule());

  it('renders only the safe recovery message, deadline, and cleanup state', () => {
    TestBed.configureTestingModule({
      imports: [
        JobDetailPanelComponent,
        TranslocoTestingModule.forRoot({
          langs: {en},
          translocoConfig: {availableLangs: ['en'], defaultLang: 'en'},
        }),
      ],
      providers: [provideRouter([])],
    });
    const transloco = TestBed.inject(TranslocoService);
    transloco.setTranslation(en, 'en');
    transloco.setActiveLang('en');
    const fixture = TestBed.createComponent(JobDetailPanelComponent);
    Object.defineProperty(fixture.componentInstance, 'job', {
      value: signal({
        id: 'job-1',
        description: 'Recover the workspace',
        status: 'paused',
        created_at: '2026-09-16T08:00:00Z',
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
          private_endpoint: '10.0.0.9',
          raw_diagnostic: 'controller credential material',
        },
      } as unknown as JobSummary),
    });
    Object.defineProperty(fixture.componentInstance, 'data', {value: signal(null)});

    fixture.detectChanges();

    const text = (fixture.nativeElement as HTMLElement).textContent ?? '';
    expect(text).toContain('Previous workspace execution could not be proven stopped.');
    expect(text).toContain('Recovery deadline');
    expect(text).toContain('Workspace cleanup remains pending');
    expect(text).not.toContain('10.0.0.9');
    expect(text).not.toContain('controller credential material');
  });
});

describe('held-for-review reason', () => {
  const reason = 'Delivery unproven: the final commit did not land, so deliverable path(s) output/report.md are not in the pushed revision.';

  it('names why a job is waiting on review instead of sealed', () => {
    expect(heldForReviewReason({status: 'pending_review', error_message: reason})).toBe(reason);
  });

  it('stays silent for ordinary review items and other statuses', () => {
    expect(heldForReviewReason({status: 'pending_review', error_message: null})).toBeNull();
    expect(heldForReviewReason({status: 'pending_review', error_message: '  '})).toBeNull();
    // A failed job's reason already renders on the row itself.
    expect(heldForReviewReason({status: 'failed', error_message: reason})).toBeNull();
    expect(heldForReviewReason(null)).toBeNull();
  });

  it('defers to the VM-creation block, which already shows error_message', () => {
    expect(
      heldForReviewReason({
        status: 'pending_review',
        error_message: reason,
        vm_creation: {state: 'attention', message: 'x'} as unknown as JobSummary['vm_creation'],
      }),
    ).toBeNull();
  });

  describe('rendered', () => {
    beforeAll(async () => {
      await ɵresolveComponentResources(() => Promise.resolve(''));
    });
    afterEach(() => TestBed.resetTestingModule());

    it('shows the hold on the panel', () => {
      TestBed.configureTestingModule({
        imports: [
          JobDetailPanelComponent,
          TranslocoTestingModule.forRoot({
            langs: {en},
            translocoConfig: {availableLangs: ['en'], defaultLang: 'en'},
          }),
        ],
        providers: [provideRouter([])],
      });
      const transloco = TestBed.inject(TranslocoService);
      transloco.setTranslation(en, 'en');
      transloco.setActiveLang('en');
      const fixture = TestBed.createComponent(JobDetailPanelComponent);
      Object.defineProperty(fixture.componentInstance, 'job', {
        value: signal({
          id: 'job-2',
          description: 'Probe the stack',
          status: 'pending_review',
          created_at: '2026-09-16T08:00:00Z',
          error_message: reason,
        } as unknown as JobSummary),
      });
      Object.defineProperty(fixture.componentInstance, 'data', {value: signal(null)});

      fixture.detectChanges();

      const text = (fixture.nativeElement as HTMLElement).textContent ?? '';
      expect(text).toContain('Held for review');
      expect(text).toContain('output/report.md are not in the pushed revision');
    });
  });
});

describe('costDisplay', () => {
  it('shows a complete price plainly', () => {
    expect(costDisplay(usage())).toEqual({amount: 0.94, isFloor: false, reasonKey: null});
  });

  it('marks a partially priced job as a floor', () => {
    // Real shape from job 5c0ed235: 599 of 827 events priced, because part of
    // the run used a model with no rate card. $0.94 is a lower bound.
    const display = costDisplay(
      usage({cost: {usd: 0.94, complete: false, priced_events: 599, events: 827}}),
    );
    expect(display.amount).toBe(0.94);
    expect(display.isFloor).toBe(true);
  });

  it('refuses to show a number when nothing was priced', () => {
    // Job 77bbb5bd: 1,053,666 tokens, 0 of 136 events priced.
    const display = costDisplay(
      usage({cost: {usd: null, complete: false, priced_events: 0, events: 136}}),
    );
    expect(display.amount).toBeNull();
    expect(display.reasonKey).toBe('jobs.detail.costUnpriced');
  });

  it.each([
    ['no_usage', 'jobs.detail.costNoUsage'],
    ['predates_ledger', 'jobs.detail.costPredatesLedger'],
    ['unavailable', 'jobs.detail.costMeteringOff'],
  ] as const)('explains the %s state instead of showing zero', (state, key) => {
    const display = costDisplay(usage({state}));
    expect(display.amount).toBeNull();
    expect(display.reasonKey).toBe(key);
  });

  it('does not let a stale cost leak through a non-measured state', () => {
    // The endpoint zeroes these itself, but the guard must not depend on that:
    // a state that says "no figures" outranks any number in the payload.
    const display = costDisplay(
      usage({state: 'predates_ledger', cost: {usd: 9.99, complete: true, priced_events: 1, events: 1}}),
    );
    expect(display.amount).toBeNull();
  });

  it('treats a failed load as unknown, not free', () => {
    expect(costDisplay(null)).toEqual({
      amount: null,
      isFloor: false,
      reasonKey: 'jobs.detail.costUnknown',
    });
  });
});

describe('formatUsd', () => {
  it('keeps sub-cent amounts visible instead of rounding them to $0.00', () => {
    // Most jobs on the cluster cost fractions of a cent; toFixed(2) would render
    // every one of them as free, which is the same lie by a different route.
    expect(formatUsd(0.0004)).not.toBe('$0.00');
    expect(formatUsd(0.0004)).toBe('$0.00040');
  });

  it('formats ordinary amounts to two decimals', () => {
    expect(formatUsd(0.94011774)).toBe('$0.94');
    expect(formatUsd(12)).toBe('$12.00');
  });

  it('renders a genuine zero as zero', () => {
    // A measured zero is a real answer and must be distinguishable from unknown,
    // which never reaches this function at all.
    expect(formatUsd(0)).toBe('$0.00');
  });
});

describe('jobModelLabel', () => {
  it('reads the override the job actually ran with', () => {
    expect(jobModelLabel({config_override: {llm: {model: 'MiniMax-M3'}}} as unknown as Job)).toBe(
      'MiniMax-M3',
    );
  });

  it('survives config_override arriving as a JSON string', () => {
    // JSONB comes back from asyncpg as text and is passed through, so indexing
    // straight in type-checks and then silently yields undefined at runtime.
    expect(
      jobModelLabel({config_override: '{"llm":{"model":"gemma-4-moe"}}'} as unknown as Job),
    ).toBe('gemma-4-moe');
  });

  it('returns null rather than inventing a model when none is pinned', () => {
    // resolved_config comes back empty for ordinary jobs, so the client cannot
    // know what the expert default resolved to. The panel says so.
    expect(jobModelLabel({config_override: {autonomy: 'full'}} as unknown as Job)).toBeNull();
    expect(jobModelLabel(null)).toBeNull();
    expect(jobModelLabel({config_override: 'not json'} as unknown as Job)).toBeNull();
  });
});

describe('workspaceBackendLabel', () => {
  it('prefers effective over assigned over requested', () => {
    expect(
      workspaceBackendLabel({
        requested_backend: 'vm',
        assigned_backend: 'sandbox',
        effective_backend: 'virtual',
        state: 'ready',
      }),
    ).toBe('virtual');
    expect(workspaceBackendLabel({requested_backend: 'vm', state: 'waiting'})).toBe('vm');
  });

  it('is null when there is no contract at all', () => {
    expect(workspaceBackendLabel(null)).toBeNull();
    expect(workspaceBackendLabel({state: 'waiting'})).toBeNull();
  });
});

describe('formatDurationSeconds', () => {
  it('scales the unit to the magnitude', () => {
    expect(formatDurationSeconds(42)).toBe('42s');
    expect(formatDurationSeconds(90)).toBe('1m 30s');
    expect(formatDurationSeconds(3700)).toBe('1h 1m');
    expect(formatDurationSeconds(90000)).toBe('1d 1h');
  });

  it('returns null for absent or nonsensical input rather than "0s"', () => {
    expect(formatDurationSeconds(null)).toBeNull();
    expect(formatDurationSeconds(undefined)).toBeNull();
    expect(formatDurationSeconds(-1)).toBeNull();
    expect(formatDurationSeconds(NaN)).toBeNull();
  });
});

describe('formatCount', () => {
  it('separates thousands and dashes absent values', () => {
    expect(formatCount(1053666)).toBe((1053666).toLocaleString());
    expect(formatCount(0)).toBe('0');
    expect(formatCount(null)).toBe('—');
    expect(formatCount(undefined)).toBe('—');
  });
});

/**
 * The roster half of the panel.
 *
 * These helpers exist because a parent's status is not self-explanatory:
 * `waiting` means *blocked on a child*, and the jobs list is precisely where
 * those children are missing — the default `origin` filter excludes every
 * subjob, so a parked parent renders with no children and reads as stalled.
 * The panel is the only surface that can say otherwise, and `subjobBlockedKey`
 * is where it decides to.
 */

const HOUR = 3600_000;
const NOW = Date.parse('2026-08-23T12:00:00Z');

function sub(over: Partial<JobSubjob> = {}): JobSubjob {
  return {
    id: 'a2826a91-38a7-4d25-b889-46fabcc93b96',
    parent_job_id: 'cac3f2b1-be31-4e6a-9c07-ef84d535ae9b',
    depth: 0,
    description: 'Research phase for: build a calculator',
    status: 'processing',
    config_name: 'scholar',
    origin: 'subjob',
    error_message: null,
    created_at: '2026-08-23T10:00:00Z',
    completed_at: null,
    updated_at: '2026-08-23T11:00:00Z',
    ...over,
  };
}

describe('shortJobId', () => {
  it('keeps the first segment, which is what people actually match on', () => {
    expect(shortJobId('a2826a91-38a7-4d25-b889-46fabcc93b96')).toBe('a2826a91');
  });

  it('does not throw on an id shorter than the slice', () => {
    expect(shortJobId('abc')).toBe('abc');
  });
});

describe('subjobElapsedSeconds', () => {
  it('measures a running subjob against now, not against its last update', () => {
    // The whole point of the number: "is this thing progressing". A live row
    // measured to `updated_at` would freeze the moment the row stopped being
    // written to, which is exactly when a reader starts to worry.
    expect(subjobElapsedSeconds(sub(), NOW)).toBe(2 * 3600);
  });

  it('prefers completed_at once the subjob is terminal', () => {
    const elapsed = subjobElapsedSeconds(
      sub({status: 'completed', completed_at: '2026-08-23T10:30:00Z'}),
      NOW,
    );
    expect(elapsed).toBe(30 * 60);
  });

  it('falls back to updated_at for a terminal row that never stamped completed_at', () => {
    // Cancellation and older rows reach a terminal status without stamping it;
    // measuring those against `now` would show a finished job still counting up.
    const elapsed = subjobElapsedSeconds(
      sub({status: 'cancelled', completed_at: null, updated_at: '2026-08-23T11:00:00Z'}),
      NOW,
    );
    expect(elapsed).toBe(3600);
  });

  it('returns null rather than a negative age when the clocks disagree', () => {
    const elapsed = subjobElapsedSeconds(
      sub({status: 'completed', completed_at: '2026-08-23T09:00:00Z'}),
      NOW,
    );
    expect(elapsed).toBeNull();
  });

  it('returns null on an unparseable timestamp instead of NaN', () => {
    expect(subjobElapsedSeconds(sub({created_at: 'not a date'}), NOW)).toBeNull();
  });
});

describe('liveSubjobCount', () => {
  it('counts only what can still move on its own', () => {
    const rows = [
      sub({status: 'processing'}),
      sub({status: 'completed'}),
      sub({status: 'failed'}),
      sub({status: 'waiting'}),
    ];
    expect(liveSubjobCount(rows)).toBe(2);
  });

  it('treats pending_review as live, matching the shared vocabulary', () => {
    // Not terminal: an approval flips it to completed. A panel that called it
    // finished would stop explaining a parent that is still genuinely blocked.
    expect(liveSubjobCount([sub({status: 'pending_review'})])).toBe(1);
  });

  it('is zero for an empty roster', () => {
    expect(liveSubjobCount([])).toBe(0);
  });
});

describe('subjobBlockedKey', () => {
  it('explains a waiting parent that has a live child', () => {
    expect(subjobBlockedKey('waiting', [sub({status: 'processing'})])).toBe(
      'jobs.detail.waitingOnSubjobs',
    );
  });

  it('says so when a waiting parent has no live child left', () => {
    // A real state, and the more alarming one: the parent is parked but nothing
    // is working. Collapsing it into the same message would hide a stuck job.
    expect(subjobBlockedKey('waiting', [sub({status: 'completed'})])).toBe(
      'jobs.detail.waitingNoLiveSubjobs',
    );
  });

  it('stays silent for every other parent status', () => {
    // `waiting` is the only status whose meaning depends on the children.
    for (const status of ['processing', 'completed', 'failed', 'pending_review', 'paused']) {
      expect(subjobBlockedKey(status, [sub()])).toBeNull();
    }
  });

  it('stays silent when the status is missing', () => {
    expect(subjobBlockedKey(null, [sub()])).toBeNull();
    expect(subjobBlockedKey(undefined, [sub()])).toBeNull();
  });
});

function child(over: Partial<JobSubagent> = {}): JobSubagent {
  return {
    thread_id: '209c55c3-9fac-4a17-8dfa-40628688dd72',
    runtime_generation: null,
    handle: 'tester-7f3a',
    subagent_type: 'tester',
    status: 'running',
    thread_status: 'active',
    outcome: null,
    error: null,
    turns: 4,
    tokens: 12345,
    report_path: null,
    parent_tool_call_id: 'call-1',
    parent_thread_id: null,
    description: 'Run the focused Cockpit tests and report exact failures.',
    isolation: 'shared',
    write_policy: 'none',
    parent_iteration: 3,
    fork: false,
    started_at: '2026-08-23T10:00:00Z',
    ended_at: null,
    last_activity: '2026-08-23T11:00:00Z',
    ...over,
  };
}

describe('subagent roster', () => {
  beforeAll(async () => {
    await ɵresolveComponentResources(() => Promise.resolve(''));
  });
  afterEach(() => TestBed.resetTestingModule());

  function render(subagents: JobSubagent[] | null) {
    TestBed.configureTestingModule({
      imports: [
        JobDetailPanelComponent,
        TranslocoTestingModule.forRoot({
          langs: {en},
          translocoConfig: {availableLangs: ['en'], defaultLang: 'en'},
        }),
      ],
      providers: [provideRouter([])],
    });
    const transloco = TestBed.inject(TranslocoService);
    transloco.setTranslation(en, 'en');
    transloco.setActiveLang('en');
    const fixture = TestBed.createComponent(JobDetailPanelComponent);
    const component = fixture.componentInstance;
    Object.defineProperty(component, 'job', {
      value: signal({
        id: 'job-1',
        description: 'Parent job',
        status: 'processing',
        created_at: '2026-08-23T09:00:00Z',
      } as JobSummary),
    });
    Object.defineProperty(component, 'data', {
      value: signal({
        loading: false,
        error: false,
        detail: null,
        usage: null,
        progress: null,
        usageSubtree: null,
        loadingSubtree: false,
        subtreeAttempted: false,
        subjobs: [],
        subagents,
      }),
    });
    fixture.detectChanges();
    return fixture;
  }

  it('renders child rows, transcript links, and the running marker', () => {
    const fixture = render([
      child(),
      child({
        thread_id: '32ca2166-1719-48d8-905f-8d846835ac1e',
        handle: 'reviewer-2a1c',
        subagent_type: 'reviewer',
        status: 'completed',
        thread_status: 'ended',
        ended_at: '2026-08-23T10:15:00Z',
      }),
    ]);
    const root = fixture.nativeElement as HTMLElement;
    const rows = root.querySelectorAll('.subagent-row');

    expect(rows).toHaveLength(2);
    expect(rows[0].textContent).toContain('tester-7f3a');
    expect(rows[0].textContent).toContain('Run the focused Cockpit tests');
    expect(rows[0].classList.contains('subagent-live')).toBe(true);
    expect(rows[1].classList.contains('subagent-live')).toBe(false);
    expect(rows[0].querySelector('a')?.getAttribute('href')).toBe(
      '/sessions/209c55c3-9fac-4a17-8dfa-40628688dd72',
    );
  });

  it.each([[], null])('hides the section for %s', (subagents) => {
    const root = render(subagents as JobSubagent[] | null).nativeElement as HTMLElement;
    expect(root.querySelector('.detail-subagents')).toBeNull();
  });

  it('renders queued children as live and surfaces terminal outcome and error evidence', () => {
    const fixture = render([
      child({status: 'queued', handle: 'probe-queued'}),
      child({
        status: 'interrupted',
        handle: 'probe-stopped',
        outcome: 'Partial report persisted.',
        error: 'Stopped after the grace window.',
      }),
    ]);
    const rows = fixture.nativeElement.querySelectorAll(
      '.subagent-row',
    ) as NodeListOf<HTMLElement>;

    expect(rows[0].classList.contains('subagent-live')).toBe(true);
    expect(rows[0].textContent).toContain('Queued');
    expect(rows[1].textContent).toContain('Outcome: Partial report persisted.');
    expect(rows[1].textContent).toContain('Error: Stopped after the grace window.');
  });

  it('uses child timestamps for elapsed time and lifecycle tones', () => {
    expect(subagentElapsedSeconds(child(), NOW)).toBe(2 * 3600);
    expect(subagentElapsedSeconds(child({status: 'queued'}), NOW)).toBe(2 * 3600);
    expect(
      subagentElapsedSeconds(child({status: 'completed', ended_at: '2026-08-23T10:30:00Z'}), NOW),
    ).toBe(30 * 60);
    expect(subagentStatusTone('running')).toBe('accent');
    expect(subagentStatusTone('queued')).toBe('info');
    expect(subagentStatusTone('completed')).toBe('success');
    expect(subagentStatusTone('error')).toBe('danger');
  });
});

/**
 * The prompt clamp.
 *
 * Whether the paragraph overflows is a layout question, and jsdom has no
 * layout — `scrollHeight` is 0 for everything, so the ResizeObserver path
 * cannot be exercised here (it is measured in the browser instead). What is
 * worth pinning is the wiring the measurement drives: the cap is on by
 * default, the toggle appears only when something is actually hidden, and
 * pressing it lifts the cap rather than merely relabelling itself.
 */
describe('prompt clamp', () => {
  beforeAll(async () => {
    await ɵresolveComponentResources(() => Promise.resolve(''));
  });
  afterEach(() => TestBed.resetTestingModule());

  function render(description: string) {
    TestBed.configureTestingModule({
      imports: [
        JobDetailPanelComponent,
        TranslocoTestingModule.forRoot({
          langs: {en},
          translocoConfig: {availableLangs: ['en'], defaultLang: 'en'},
        }),
      ],
      providers: [provideRouter([])],
    });
    const transloco = TestBed.inject(TranslocoService);
    transloco.setTranslation(en, 'en');
    transloco.setActiveLang('en');
    const fixture = TestBed.createComponent(JobDetailPanelComponent);
    Object.defineProperty(fixture.componentInstance, 'job', {
      value: signal({
        id: 'job-1',
        description,
        status: 'processing',
        created_at: '2026-08-23T09:00:00Z',
      } as JobSummary),
    });
    Object.defineProperty(fixture.componentInstance, 'data', {value: signal(null)});
    fixture.detectChanges();
    return fixture;
  }

  it('caps the prompt and offers no toggle while nothing is hidden', () => {
    const fixture = render('Short prompt.');
    const root = fixture.nativeElement as HTMLElement;

    expect(root.querySelector('.detail-description')?.textContent).toBe('Short prompt.');
    expect(root.querySelector('.detail-description--capped')).not.toBeNull();
    expect(root.querySelector('.desc-toggle')).toBeNull();
    // The cap is harmless on text that never reaches it; the fade is not — a
    // gradient taller than a one-line prompt would dim the prompt itself.
    expect(root.querySelector('.detail-description--faded')).toBeNull();
  });

  it('lifts the cap when the toggle is pressed, and puts it back', () => {
    const fixture = render('A prompt far taller than the panel.');
    const component = fixture.componentInstance;
    component.descOverflowed.set(true);
    fixture.detectChanges();
    const root = fixture.nativeElement as HTMLElement;

    const toggle = root.querySelector('.desc-toggle') as HTMLButtonElement;
    expect(toggle.textContent?.trim()).toBe('Show more');
    expect(toggle.getAttribute('aria-expanded')).toBe('false');

    expect(root.querySelector('.detail-description--faded')).not.toBeNull();

    toggle.click();
    fixture.detectChanges();
    expect(root.querySelector('.detail-description--capped')).toBeNull();
    expect(root.querySelector('.detail-description--faded')).toBeNull();
    expect(root.querySelector('.desc-toggle')?.textContent?.trim()).toBe('Show less');
    expect(root.querySelector('.desc-toggle')?.getAttribute('aria-expanded')).toBe('true');

    (root.querySelector('.desc-toggle') as HTMLButtonElement).click();
    fixture.detectChanges();
    expect(root.querySelector('.detail-description--capped')).not.toBeNull();
  });

  it('renders no description block at all when the job has no prompt', () => {
    const root = render('').nativeElement as HTMLElement;
    expect(root.querySelector('.description-block')).toBeNull();
  });
});
