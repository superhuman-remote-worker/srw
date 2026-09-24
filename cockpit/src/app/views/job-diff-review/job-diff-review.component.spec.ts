import { readFileSync } from 'node:fs';
import {
  Component,
  EventEmitter,
  Input,
  Output,
  signal,
  ɵresolveComponentResources,
} from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { TranslocoPipe, TranslocoTestingModule } from '@jsverse/transloco';
import { Subject, of } from 'rxjs';
import { afterEach, beforeAll, describe, expect, it, vi } from 'vitest';

import en from '../../../assets/i18n/en.json';
import { ApiService } from '../../core/services/api.service';
import { MonacoEditorLoaderService } from '../../core/services/monaco-editor-loader.service';
import { AppToastService } from '../../ui/toast';
import { JobDiffReviewComponent, diffApiFor } from './job-diff-review.component';
import { binaryKindFromPath, isBinaryEntry, looksBinaryContent } from './binary-sniff';
import {
  clearReceipt,
  readReceipt,
  receiptAppliesTo,
  writeReceipt,
} from './cloud-review-receipt';
import { folderLinkMatches, selectProtectedProjectMount } from './protected-folder-link';

// ============================================================================
// Pure helpers
// ============================================================================

describe('diffApiFor', () => {
  it('returns "job" when only jobId is set', () => {
    expect(diffApiFor('job-1', null)).toBe('job');
  });

  it('returns "session" when only threadId is set', () => {
    // Renamed from 'thread': the axis is now the product surface being
    // presented, not the backend that serves it, so every piece of
    // context-varying copy can key off one explicit value (PC-23).
    expect(diffApiFor(null, 'thread-1')).toBe('session');
  });

  it('throws when both are set', () => {
    expect(() => diffApiFor('job-1', 'thread-1')).toThrow();
  });

  it('throws when neither is set', () => {
    expect(() => diffApiFor(null, null)).toThrow();
  });
});

describe('looksBinaryContent', () => {
  it('is false for null / empty content', () => {
    // An added file has null old_content and a deleted file has null
    // new_content; neither says anything about the side that exists.
    expect(looksBinaryContent(null)).toBe(false);
    expect(looksBinaryContent('')).toBe(false);
  });

  it('is false for ordinary prose and source', () => {
    expect(looksBinaryContent('Hello world\nsecond line\n\tindented\r\n')).toBe(false);
    expect(looksBinaryContent('export function x() { return 1; }')).toBe(false);
  });

  it('detects a NUL byte', () => {
    expect(looksBinaryContent('some text\u0000more')).toBe(true);
  });

  it('detects a PDF that decoded cleanly as UTF-8', () => {
    // PC-17: staging flags binary only on a NUL byte in the first 8 KiB, so a
    // PDF whose bytes happen to decode without one is reported binary:false
    // and its raw syntax would reach the text editor. This is the guard.
    expect(looksBinaryContent('%PDF-1.7\n1 0 obj\n<< /Type /Catalog >>')).toBe(true);
  });

  it('detects ZIP-family containers (docx, xlsx, odt)', () => {
    expect(looksBinaryContent('PK\u0003\u0004\u0014\u0000')).toBe(true);
  });

  it('detects a replacement character left by a lossy decode', () => {
    expect(looksBinaryContent('caf\uFFFD data')).toBe(true);
  });

  it('detects a high ratio of C0 control characters', () => {
    expect(looksBinaryContent('a\u0001\u0002\u0003\u0004b')).toBe(true);
  });

  it('tolerates the occasional stray control character in real text', () => {
    // A form feed in a long generated file must not blank the diff view.
    expect(looksBinaryContent('x'.repeat(1000) + '\u000c' + 'y'.repeat(1000))).toBe(false);
  });

  it('detects a PDF behind a byte-order mark or leading whitespace', () => {
    // ISO 32000 does not require the header at byte 0, and a BOM or a stray
    // newline in front of it is common enough that anchoring strictly at
    // index 0 loses the exact case this guard exists for.
    expect(looksBinaryContent('\uFEFF%PDF-1.7\n1 0 obj')).toBe(true);
    expect(looksBinaryContent('\n\n   %PDF-1.4\n1 0 obj')).toBe(true);
    expect(looksBinaryContent('\uFEFFPK\u0003\u0004\u0014\u0000')).toBe(true);
  });

  it('does not call prose binary for mentioning a signature further in', () => {
    // The complementary hazard: a scan for %PDF- anywhere in the head would
    // turn documentation about PDFs into an unreviewable placeholder.
    expect(
      looksBinaryContent('A PDF file starts with the bytes %PDF-1.7 followed by objects.'),
    ).toBe(false);
    expect(
      looksBinaryContent('# Notes\n\nGIF files begin GIF89a; PostScript begins %!PS-Adobe.'),
    ).toBe(false);
  });
});

describe('isBinaryEntry', () => {
  it('trusts the summary flag when set', () => {
    expect(isBinaryEntry({ binary: true }, null)).toBe(true);
  });

  it('trusts either side of the per-file reader', () => {
    expect(isBinaryEntry({}, { old_binary: true })).toBe(true);
    expect(isBinaryEntry({}, { new_binary: true })).toBe(true);
  });

  it('falls back to a content sniff when no flag is set', () => {
    // The case the flags miss entirely — and job mode has no flags at all.
    expect(isBinaryEntry({}, { new_content: '%PDF-1.4 ...' })).toBe(true);
  });

  it('is false for text with no flag and no binary signal', () => {
    expect(isBinaryEntry({}, { old_content: 'a', new_content: 'b' })).toBe(false);
    expect(isBinaryEntry({ binary: false }, null)).toBe(false);
  });
});

describe('binaryKindFromPath', () => {
  it('names the common review formats', () => {
    expect(binaryKindFromPath('a/b/report.pdf')).toBe('pdf');
    expect(binaryKindFromPath('a/edit-me.docx')).toBe('document');
    expect(binaryKindFromPath('a/chart.PNG')).toBe('image');
    expect(binaryKindFromPath('a/bundle.zip')).toBe('archive');
  });

  it('falls back to a generic kind', () => {
    expect(binaryKindFromPath('a/blob.bin')).toBe('unknown');
    expect(binaryKindFromPath('Dockerfile')).toBe('unknown');
  });
});

describe('selectProtectedProjectMount', () => {
  const mount = (over: Record<string, unknown> = {}) => ({
    id: 'm1',
    mount_kind: 'project',
    target_path: 'cloud',
    source_kind: 'project',
    source_ref: 'proj-1',
    backend_id: 'nextcloud',
    ...over,
  });

  it('picks the first eligible project mount, matching the backend rule', () => {
    const rows = [mount({ id: 'a' }), mount({ id: 'b', target_path: 'other' })];
    expect(selectProtectedProjectMount(rows)?.id).toBe('a');
  });

  it('skips the default-project mount (the owner personal home)', () => {
    const rows = [mount({ id: 'home', mount_kind: 'project_default' }), mount({ id: 'real' })];
    expect(selectProtectedProjectMount(rows)?.id).toBe('real');
  });

  it('skips non-Nextcloud backends — protected mode v1 is Nextcloud-only', () => {
    expect(selectProtectedProjectMount([mount({ backend_id: 'opencloud' })])).toBeNull();
  });

  it('skips a mount with no project reference to resolve a URL from', () => {
    expect(selectProtectedProjectMount([mount({ source_ref: null })])).toBeNull();
  });

  it('returns null for an empty or absent mount list', () => {
    expect(selectProtectedProjectMount([])).toBeNull();
    expect(selectProtectedProjectMount(null)).toBeNull();
  });
});

describe('folderLinkMatches', () => {
  const link = { url: 'https://cloud/x', name: 'Docs', targetPath: 'cloud' };

  it('accepts a link whose mount matches the summary', () => {
    expect(folderLinkMatches(link, 'cloud')).toBe(true);
  });

  it('refuses a mismatch rather than guessing', () => {
    // PC-19 was a guess (the legacy sessions/<id> handle). A mismatch here
    // means the frontend and the backend picked different mounts.
    expect(folderLinkMatches(link, 'other')).toBe(false);
  });

  it('refuses when either side is absent', () => {
    expect(folderLinkMatches(null, 'cloud')).toBe(false);
    expect(folderLinkMatches(link, null)).toBe(false);
  });
});

describe('cloud review receipt store', () => {
  const RECEIPT = {
    decision: 'applied' as const,
    epoch: 5,
    applied: 3,
    deleted: 1,
    overlayReset: false,
    // Relative, not pinned: the store drops receipts older than its 7-day
    // retention window, so a wall-clock date here becomes a time bomb.
    at: new Date(Date.now() - 60_000).toISOString(),
  };

  afterEach(() => localStorage.clear());

  it('round-trips a receipt per thread', () => {
    writeReceipt('t1', RECEIPT);
    expect(readReceipt('t1')).toEqual(RECEIPT);
    expect(readReceipt('t2')).toBeNull();
  });

  it('clears a receipt', () => {
    writeReceipt('t1', RECEIPT);
    clearReceipt('t1');
    expect(readReceipt('t1')).toBeNull();
  });

  it('ignores a corrupt or foreign-versioned record instead of throwing', () => {
    localStorage.setItem('srw:cloud-review-receipt:t1', 'not json');
    expect(readReceipt('t1')).toBeNull();
    localStorage.setItem('srw:cloud-review-receipt:t1', JSON.stringify({ v: 99, receipt: RECEIPT }));
    expect(readReceipt('t1')).toBeNull();
  });

  it('drops a receipt older than the retention window', () => {
    // These accumulate one key per reviewed thread and nothing else deletes
    // them; a months-old record must not be re-displayed as "the last result".
    const old = new Date(Date.now() - 8 * 24 * 60 * 60 * 1000).toISOString();
    writeReceipt('t-old', { decision: 'applied', epoch: 1, applied: 1, deleted: 0, overlayReset: true, at: old });
    expect(readReceipt('t-old')).toBeNull();
    expect(localStorage.getItem('srw:cloud-review-receipt:t-old')).toBeNull();
  });

  it('keeps a receipt inside the retention window', () => {
    const recent = new Date(Date.now() - 60_000).toISOString();
    writeReceipt('t-new', { decision: 'rejected', epoch: 2, applied: 0, deleted: 0, overlayReset: true, at: recent });
    expect(readReceipt('t-new')).toMatchObject({ decision: 'rejected' });
  });

  it('prunes every expired record when a new one is written', () => {
    const old = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString();
    for (const id of ['a', 'b', 'c']) {
      localStorage.setItem(
        `srw:cloud-review-receipt:${id}`,
        JSON.stringify({ v: 1, receipt: { decision: 'applied', epoch: 1, applied: 1, deleted: 0, overlayReset: true, at: old } }),
      );
    }
    localStorage.setItem('unrelated:key', 'keep me');
    writeReceipt('fresh', { decision: 'applied', epoch: 9, applied: 1, deleted: 0, overlayReset: true, at: new Date().toISOString() });
    expect(localStorage.getItem('srw:cloud-review-receipt:a')).toBeNull();
    expect(localStorage.getItem('srw:cloud-review-receipt:fresh')).toBeTruthy();
    // Other owners' keys are none of this module's business.
    expect(localStorage.getItem('unrelated:key')).toBe('keep me');
  });

  it('reports whether a write actually reached storage', () => {
    // The UI says "recorded in this browser only" — a claim that is false in
    // job context, where there is no thread key to store under.
    expect(writeReceipt(null, { decision: 'applied', epoch: null, applied: 1, deleted: 0, overlayReset: true, at: new Date().toISOString() })).toBe(false);
    expect(writeReceipt('t9', { decision: 'applied', epoch: 1, applied: 1, deleted: 0, overlayReset: true, at: new Date().toISOString() })).toBe(true);
  });

  it('is a no-op without a thread id', () => {
    expect(readReceipt(null)).toBeNull();
    expect(() => writeReceipt(null, RECEIPT)).not.toThrow();
  });

  describe('receiptAppliesTo', () => {
    it('is false when a diff is currently pending', () => {
      // A past decision must never render above a live pending diff.
      expect(receiptAppliesTo(RECEIPT, 6, 3)).toBe(false);
    });

    it('is false without a receipt', () => {
      expect(receiptAppliesTo(null, 6, 0)).toBe(false);
    });

    it('is true for the epoch it resolved', () => {
      expect(receiptAppliesTo(RECEIPT, 6, 0)).toBe(true);
    });

    it('refuses to claim currency for a receipt with no epoch to compare', () => {
      // Job context has no epoch. Guessing "still current" there would put a
      // stale outcome over a live diff.
      expect(
        receiptAppliesTo(
          { decision: 'applied', epoch: null, applied: 1, deleted: 0, overlayReset: true, at: 'x' },
          6,
          0,
        ),
      ).toBe(false);
    });

    it('is false once a newer epoch has been staged and resolved past it', () => {
      expect(receiptAppliesTo(RECEIPT, 9, 0)).toBe(false);
    });
  });
});

// ============================================================================
// Rendered surface
// ============================================================================

// Design-system children as inert, drivable stubs. Decorator inputs (not
// signal inputs) on purpose: this vitest JIT pipeline drops signal-input
// metadata, so a stub built the modern way would never receive its bindings.
@Component({
  selector: 'app-button',
  standalone: true,
  template: '<button type="button" [disabled]="disabled || loading" (click)="clicked.emit()"><ng-content /></button>',
})
class ButtonStub {
  @Input() variant = '';
  @Input() size = '';
  @Input() disabled = false;
  @Input() loading = false;
  @Output() readonly clicked = new EventEmitter<void>();
}

@Component({ selector: 'app-spinner', standalone: true, template: '' })
class SpinnerStub {
  @Input() size = '';
  @Input() tone = '';
  @Input() ariaLabel = '';
}

@Component({ selector: 'app-icon', standalone: true, template: '<ng-content />' })
class IconStub {
  @Input() size = '';
}

const TEMPLATE = readFileSync(
  'src/app/views/job-diff-review/job-diff-review.component.html',
  'utf8',
);

type Status = 'added' | 'modified' | 'deleted';

const FILES = [
  { path: 'session_apply/change-me.txt', status: 'modified' as Status, binary: false },
  { path: 'session_apply/delete-me.pdf', status: 'deleted' as Status, binary: false },
  { path: 'session_apply/edit-me.docx', status: 'modified' as Status, binary: true },
  { path: 'session_apply/new-report.pdf', status: 'added' as Status, binary: false },
];

function threadSummary(over: Record<string, unknown> = {}) {
  return {
    thread_id: 't1',
    epoch: 5,
    staged_at: '2026-08-24T09:18:00.000Z',
    counts: { added: 1, modified: 2, deleted: 1 },
    protected_mount: 'cloud',
    files: FILES,
    ...over,
  };
}

function jobSummary(over: Record<string, unknown> = {}) {
  return {
    job_id: 'j1',
    diff_status: 'pending',
    baseline_commit: 'abcdef1234567',
    head_commit: '9876543210fed',
    files: FILES.map(({ path, status }) => ({ path, status })),
    ...over,
  };
}

describe('JobDiffReviewComponent surface', () => {
  let fixture: ComponentFixture<JobDiffReviewComponent>;
  let api: Record<string, ReturnType<typeof vi.fn>>;
  let toast: Record<string, ReturnType<typeof vi.fn>>;
  let monacoLoad: ReturnType<typeof vi.fn>;

  // templateUrl/styleUrl must be resolved before TestBed will accept the
  // component at all; the content is irrelevant because overrideComponent()
  // below swaps in the real template and blanks the styles.
  beforeAll(async () => {
    await ɵresolveComponentResources(() => Promise.resolve(''));
  });

  afterEach(() => {
    localStorage.clear();
    TestBed.resetTestingModule();
  });

  /** Enough of Monaco's surface for `renderDiff` to complete without a DOM
   *  layout engine. The editor itself is not assertable under jsdom, so the
   *  tests assert the branch that decides whether it is mounted at all. */
  function fakeMonaco() {
    return {
      editor: {
        createModel: () => ({ dispose: () => {} }),
        createDiffEditor: () => ({ setModel: () => {}, dispose: () => {} }),
      },
    };
  }

  interface RenderOpts {
    threadId?: string | null;
    jobId?: string | null;
    projectFolder?: { url: string; name: string; targetPath: string } | null;
    summary?: unknown;
    summaryOutcome?: unknown;
    fileOutcome?: unknown;
    applyOutcome?: unknown;
    rejectOutcome?: unknown;
  }

  async function render(opts: RenderOpts = {}): Promise<HTMLElement> {
    const threadId = opts.threadId === undefined && !opts.jobId ? 't1' : (opts.threadId ?? null);
    const jobId = opts.jobId ?? null;

    const summaryOutcome =
      opts.summaryOutcome ??
      ({ kind: 'ok', data: opts.summary ?? (jobId ? jobSummary() : threadSummary()) } as const);
    const fileOutcome =
      opts.fileOutcome ??
      ({
        kind: 'ok',
        data: {
          thread_id: 't1',
          path: FILES[0].path,
          status: 'modified',
          old_content: 'VERSION=1\n',
          new_content: 'VERSION=2\n',
          old_binary: false,
          new_binary: false,
        },
      } as const);

    api = {
      getThreadCloudDiffOutcome: vi.fn().mockReturnValue(of(summaryOutcome)),
      getJobDiffOutcome: vi.fn().mockReturnValue(of(summaryOutcome)),
      getThreadCloudDiffFileOutcome: vi.fn().mockReturnValue(of(fileOutcome)),
      getJobDiffFileOutcome: vi.fn().mockReturnValue(of(fileOutcome)),
      applyThreadCloudDiff: vi
        .fn()
        .mockReturnValue(
          of(
            opts.applyOutcome ?? {
              kind: 'ok',
              data: { thread_id: 't1', applied: 3, deleted: 1, errors: [], epoch: 6, overlay_reset: true },
            },
          ),
        ),
      acceptJobDiff: vi
        .fn()
        .mockReturnValue(
          of(
            opts.applyOutcome ?? {
              kind: 'ok',
              data: { job_id: 'j1', diff_status: 'accepted', status: 'completed', applied: 3, deleted: 1 },
            },
          ),
        ),
      rejectThreadCloudDiff: vi
        .fn()
        .mockReturnValue(
          of(
            opts.rejectOutcome ?? {
              kind: 'ok',
              data: { thread_id: 't1', rejected: true, epoch: 6, overlay_reset: true },
            },
          ),
        ),
      rejectJobDiff: vi
        .fn()
        .mockReturnValue(
          of(
            opts.rejectOutcome ?? {
              kind: 'ok',
              data: { job_id: 'j1', diff_status: 'rejected', status: 'completed' },
            },
          ),
        ),
    };
    toast = { success: vi.fn(), info: vi.fn(), danger: vi.fn(), warning: vi.fn() };
    monacoLoad = vi.fn().mockResolvedValue(fakeMonaco());

    TestBed.configureTestingModule({
      imports: [
        JobDiffReviewComponent,
        TranslocoTestingModule.forRoot({
          langs: { en },
          translocoConfig: { availableLangs: ['en'], defaultLang: 'en' },
        }),
      ],
      providers: [
        { provide: ApiService, useValue: api },
        { provide: AppToastService, useValue: toast },
        { provide: MonacoEditorLoaderService, useValue: { load: monacoLoad } },
      ],
    });

    // The real template, with the design-system children swapped for drivable
    // stubs. `template` replaces `templateUrl` so the JIT pipeline never has
    // to resolve component resources.
    TestBed.overrideComponent(JobDiffReviewComponent, {
      set: {
        template: TEMPLATE,
        // Cleared, or overrideComponent re-queues them as pending resources
        // and compileComponents() fails the same way configureTestingModule
        // would have.
        templateUrl: undefined,
        styleUrl: undefined,
        styles: [''],
        imports: [TranslocoPipe, ButtonStub, SpinnerStub, IconStub],
      },
    });
    await TestBed.compileComponents();
    fixture = TestBed.createComponent(JobDiffReviewComponent);

    // Assigned, not setInput(): this vitest pipeline drops signal-input
    // metadata, and the constructor effect throws unless exactly one id is
    // bound before the first detectChanges().
    const inst = fixture.componentInstance as unknown as Record<string, unknown>;
    inst['threadId'] = signal<string | null>(threadId);
    inst['jobId'] = signal<string | null>(jobId);
    inst['projectFolder'] = signal(opts.projectFolder ?? null);

    fixture.detectChanges();
    await settle();
    return fixture.nativeElement as HTMLElement;
  }

  /** Drain microtasks (the Monaco mount is async) and re-render. */
  async function settle(): Promise<void> {
    // Two rounds: the first publishes #diffContainer to the view query, the
    // second lets the Monaco-mount effect see it and its await resolve.
    for (let i = 0; i < 3; i++) {
      await new Promise((resolve) => setTimeout(resolve, 0));
      const flush = (TestBed as unknown as { flushEffects?: () => void }).flushEffects;
      if (flush) flush.call(TestBed);
      fixture.detectChanges();
      await fixture.whenStable();
      fixture.detectChanges();
    }
  }

  const root = () => fixture.nativeElement as HTMLElement;
  const text = () => (root().textContent ?? '').replace(/\s+/g, ' ').trim();
  const options = () => Array.from(root().querySelectorAll<HTMLElement>('[role="option"]'));
  const buttons = () => Array.from(root().querySelectorAll<HTMLButtonElement>('button'));
  const byLabel = (label: string) =>
    buttons().find((b) => (b.textContent ?? '').replace(/\s+/g, ' ').trim() === label);
  async function click(el: HTMLButtonElement | undefined): Promise<void> {
    expect(el, 'button not found').toBeTruthy();
    el!.click();
    await settle();
  }

  // -- connected pending review ---------------------------------------------

  describe('pending review (session)', () => {
    it('lists every staged file with a per-status breakdown', async () => {
      await render();
      expect(options()).toHaveLength(4);
      expect(options().map((o) => o.querySelector('.review__file-path')?.textContent?.trim())).toEqual(
        FILES.map((f) => f.path),
      );
      // Mixed add/modify/delete: the counts arrive on the summary and are no
      // longer collapsed into one number.
      const tallies = Array.from(root().querySelectorAll('.review__tally')).map((t) => [
        t.getAttribute('data-status'),
        t.querySelector('.review__tally-count')?.textContent?.trim(),
        t.querySelector('.review__tally-label')?.textContent?.trim(),
      ]);
      expect(tallies).toEqual([
        ['added', '1', 'Added'],
        ['modified', '2', 'Modified'],
        ['deleted', '1', 'Deleted'],
      ]);
    });

    it('shows the protected folder, staged time and epoch', async () => {
      await render();
      expect(root().querySelector('.review__title')?.textContent?.trim()).toBe('cloud');
      const stagedAt = new Intl.DateTimeFormat('en', {
        dateStyle: 'medium',
        timeStyle: 'short',
      }).format(new Date('2026-08-24T09:18:00Z'));
      const meta = Array.from(root().querySelectorAll('.review__meta li')).map((li) => [
        li.querySelector('.review__meta-key')?.textContent?.trim(),
        li.lastElementChild?.textContent?.trim(),
      ]);
      expect(meta).toEqual([
        ['Staged', stagedAt],
        ['Mount', 'cloud'],
        ['Epoch', '5'],
      ]);
    });

    it('loads without any connection state — only a thread id', async () => {
      // PC-25: the review API serves ENDED threads. Nothing in this surface
      // consults chat.isConnected(), so an ended/paused/disconnected session
      // renders exactly the same review.
      await render({ threadId: 'ended-thread' });
      expect(api['getThreadCloudDiffOutcome']).toHaveBeenCalledWith('ended-thread');
      expect(options()).toHaveLength(4);
      expect(byLabel('Apply to cloud')).toBeTruthy();
    });

    it('auto-selects the first file so the pane is never blank', async () => {
      await render();
      expect(options()[0].getAttribute('aria-selected')).toBe('true');
      expect(api['getThreadCloudDiffFileOutcome']).toHaveBeenCalledWith('t1', FILES[0].path);
    });
  });

  // -- session vs job wording ------------------------------------------------

  describe('presentation context', () => {
    it('uses session wording, never job wording', async () => {
      await render();
      const t = text();
      expect(t).toContain('changed in this session');
      expect(t).not.toContain('in this job');
      expect(t).toContain('Protected cloud · review required');
      expect(byLabel('Apply to cloud')).toBeTruthy();
      expect(byLabel('Accept all changes')).toBeUndefined();
      expect(byLabel('Reject staged changes')).toBeTruthy();
    });

    it('uses job wording in job context', async () => {
      await render({ jobId: 'j1', threadId: null });
      const t = text();
      expect(t).toContain('changed in this job');
      expect(t).not.toContain('in this session');
      expect(byLabel('Accept all changes')).toBeTruthy();
      expect(byLabel('Apply to cloud')).toBeUndefined();
      // Job context surfaces the Gitea baseline it diffs against; session
      // context has no such thing and shows the staged epoch instead.
      expect(t).toContain('abcdef1 → 9876543');
    });

    it('explains reject as discarding without touching the cloud (session)', async () => {
      await render();
      await click(byLabel('Reject staged changes'));
      const t = text();
      expect(t).toContain('permanently discards');
      expect(t).toContain('Nothing is written to or removed from your cloud');
    });

    it('explains apply as writing the complete reviewed set (session)', async () => {
      await render();
      await click(byLabel('Apply to cloud'));
      expect(text()).toContain('writes all 4 reviewed changes to cloud');
      expect(text()).toContain('cannot be undone from Cockpit');
    });

    it('keeps the toast in job context, where the host unmounts the surface', async () => {
      await render({ jobId: 'j1', threadId: null });
      await click(byLabel('Accept all changes'));
      await click(byLabel('Yes, accept them'));
      expect(toast['success']).toHaveBeenCalledWith(
        'Applied 3 change(s), 1 deletion(s) to the cloud folder.',
      );
    });

    it('keeps the Gitea audit-trail promise only in job context', async () => {
      await render({ jobId: 'j1', threadId: null });
      await click(byLabel('Reject all changes'));
      expect(text()).toContain('Gitea commits stay as an audit trail');
    });
  });

  // -- text and binary -------------------------------------------------------

  describe('file rendering', () => {
    it('renders the diff editor for a text file', async () => {
      await render();
      // The editor itself needs a layout engine, so the assertable contract
      // here is the branch: a text file gets the editor mount point, and the
      // binary placeholder is nowhere near it.
      expect(root().querySelector('.review__monaco')).toBeTruthy();
      expect(root().querySelector('.review__binary')).toBeNull();
      // Direction is stated, not left to column order.
      expect(text()).toContain('Cloud now');
      expect(text()).toContain('After you apply');
    });

    it('never renders the editor mount point for a flagged binary file', async () => {
      await render({
        summary: threadSummary({
          files: [{ path: 'session_apply/edit-me.docx', status: 'modified', binary: true }],
          counts: { added: 0, modified: 1, deleted: 0 },
        }),
        fileOutcome: {
          kind: 'ok',
          data: {
            thread_id: 't1',
            path: 'session_apply/edit-me.docx',
            status: 'modified',
            old_content: null,
            new_content: null,
            old_binary: true,
            new_binary: true,
          },
        },
      });
      expect(root().querySelector('.review__monaco')).toBeNull();
      expect(text()).toContain('Binary file — no preview');
      expect(text()).toContain('Word-processor document');
    });

    it('never renders the editor mount point for a PDF the backend called text', async () => {
      // PC-17: the summary's `binary` flag is a NUL-byte heuristic. This entry
      // claims binary:false and the per-file reader agrees; only the content
      // sniff catches it — and the editor's mount point must not even exist.
      await render({
        summary: threadSummary({
          files: [{ path: 'session_apply/new-report.pdf', status: 'added', binary: false }],
          counts: { added: 1, modified: 0, deleted: 0 },
        }),
        fileOutcome: {
          kind: 'ok',
          data: {
            thread_id: 't1',
            path: 'session_apply/new-report.pdf',
            status: 'added',
            old_content: null,
            new_content: '%PDF-1.7\n1 0 obj\n<< /Type /Catalog >>\nendobj\n',
            old_binary: false,
            new_binary: false,
          },
        },
      });
      expect(root().querySelector('.review__monaco')).toBeNull();
      expect(text()).toContain('Binary file — no preview');
      expect(text()).toContain('PDF document');
      // And the raw syntax is nowhere in the rendered output.
      expect(text()).not.toContain('/Type /Catalog');
    });

    it('offers no open or download action for binary content', async () => {
      // There is no endpoint serving staged bytes; anything offered here would
      // be reconstructed from a decoded string and therefore corrupt.
      await render({
        summary: threadSummary({
          files: [{ path: 'session_apply/edit-me.docx', status: 'modified', binary: true }],
          counts: { added: 0, modified: 1, deleted: 0 },
        }),
        fileOutcome: {
          kind: 'ok',
          data: {
            thread_id: 't1',
            path: 'session_apply/edit-me.docx',
            status: 'modified',
            old_content: null,
            new_content: null,
            old_binary: true,
            new_binary: true,
          },
        },
      });
      const binary = root().querySelector('.review__binary')!;
      expect(binary.querySelectorAll('a')).toHaveLength(0);
      expect(binary.querySelectorAll('button')).toHaveLength(0);
      expect(text()).toContain('cannot be opened or downloaded from here');
    });

    it('falls back to a labelled two-column text view when the editor cannot load', async () => {
      await render();
      (fixture.componentInstance as unknown as { monacoFailed: { set(v: boolean): void } })
        .monacoFailed.set(true);
      await settle();
      expect(text()).toContain('The diff editor failed to load');
      const columns = Array.from(root().querySelectorAll('.review__fallback-grid h3')).map((h) =>
        h.textContent?.trim(),
      );
      // Old/new direction must stay unmistakable in the fallback too.
      expect(columns).toEqual(['Cloud now', 'After you apply']);
      const panes = Array.from(root().querySelectorAll<HTMLElement>('.review__fallback-grid pre'));
      expect(panes.map((pre) => pre.textContent)).toEqual(['VERSION=1\n', 'VERSION=2\n']);
      // Scrollable regions must be keyboard reachable (WCAG 2.1.1).
      expect(panes.every((pre) => pre.getAttribute('tabindex') === '0')).toBe(true);
    });
  });

  // -- load states -----------------------------------------------------------

  describe('load states', () => {
    it('shows a loading state before the summary resolves', async () => {
      // A never-emitting observable holds the surface in `loading`.
      await render({ summaryOutcome: undefined, summary: threadSummary() });
      api['getThreadCloudDiffOutcome'].mockReturnValue(of());
      (fixture.componentInstance as unknown as { reload(): void }).reload();
      fixture.detectChanges();
      expect(text()).toContain('Loading changes…');
      expect(root().querySelector('app-spinner')).toBeTruthy();
    });

    it('distinguishes an empty diff from a failure', async () => {
      await render({
        summary: threadSummary({ files: [], counts: { added: 0, modified: 0, deleted: 0 }, epoch: 0 }),
      });
      expect(text()).toContain('Nothing staged for review');
      // No decision controls when there is nothing to decide.
      expect(byLabel('Apply to cloud')).toBeUndefined();
    });

    it('reports permission denied instead of "no changes"', async () => {
      await render({ summaryOutcome: { kind: 'forbidden' } });
      expect(text()).toContain("You can't review these changes");
      expect(text()).toContain('Only the owner of this session');
      expect(text()).not.toContain('Nothing staged for review');
    });

    it('reports an unprotected or missing thread instead of "no changes"', async () => {
      await render({ summaryOutcome: { kind: 'unavailable' } });
      expect(text()).toContain('No protected review for this session');
      expect(text()).not.toContain('Nothing staged for review');
    });

    it('reports a network error and offers a retry that re-requests', async () => {
      await render({
        summaryOutcome: { kind: 'error', status: 0, detail: 'Server unreachable' },
      });
      expect(text()).toContain("Couldn't load the changes");
      expect(text()).toContain('Server unreachable');
      expect(api['getThreadCloudDiffOutcome']).toHaveBeenCalledTimes(1);
      api['getThreadCloudDiffOutcome'].mockReturnValue(
        of({ kind: 'ok', data: threadSummary() }),
      );
      await click(byLabel('Try again'));
      expect(api['getThreadCloudDiffOutcome']).toHaveBeenCalledTimes(2);
      expect(options()).toHaveLength(4);
    });

    it('reports a per-file read that 404s as no-longer-staged', async () => {
      await render({ fileOutcome: { kind: 'missing' } });
      expect(text()).toContain('This file is no longer staged');
    });

    it('reports a per-file read failure with a retry', async () => {
      await render({ fileOutcome: { kind: 'error', status: 500, detail: 'Upstream broke' } });
      expect(text()).toContain("Couldn't load this file");
      expect(text()).toContain('Upstream broke');
    });
  });

  // -- conflict / stale / partial -------------------------------------------

  describe('apply outcomes', () => {
    async function applyWith(outcome: unknown): Promise<void> {
      await render({ applyOutcome: outcome });
      await click(byLabel('Apply to cloud'));
      await click(byLabel('Yes, apply to cloud'));
    }

    it('surfaces an external-modification conflict and blocks apply', async () => {
      await applyWith({
        kind: 'conflict',
        data: {
          code: 'external_modifications_detected',
          message: 'x',
          diverged: [{ path: 'cloud/a.txt', kind: 'etag_mismatch' }],
        },
      });
      expect(text()).toContain('The cloud folder changed underneath this review');
      expect(text()).toContain('Edited externally');
      // There is no force option by design, so Apply must stay disabled.
      expect(byLabel('Apply to cloud')?.disabled).toBe(true);
      // Re-check reloads; it does not merely dismiss and re-arm a doomed apply.
      expect(byLabel('Re-check')).toBeTruthy();
    });

    it('surfaces a partial write failure with its counts and per-file errors', async () => {
      await applyWith({
        kind: 'partial',
        data: {
          code: 'partial_write_failure',
          applied: 2,
          deleted: 1,
          errors: ['cloud/b.pdf: 507 Insufficient Storage'],
        },
      });
      expect(text()).toContain('Some writes failed');
      // Singular copy: "1 files failed" was the visible symptom.
      expect(text()).toContain('Applied 2 changes and 1 deletions, but one file failed');
      expect(text()).toContain('507 Insufficient Storage');
    });

    it('reloads on a stale epoch rather than applying stale content', async () => {
      await applyWith({ kind: 'stale', staged_epoch: 9 });
      expect(toast['info']).toHaveBeenCalled();
      expect(api['getThreadCloudDiffOutcome']).toHaveBeenCalledTimes(2);
    });

    it('reloads when the backend says nothing is staged any more', async () => {
      await applyWith({ kind: 'error', status: 409, detail: 'There are no staged changes left' });
      expect(toast['danger']).not.toHaveBeenCalled();
      expect(api['getThreadCloudDiffOutcome']).toHaveBeenCalledTimes(2);
    });

    it('reports any other apply failure where the controls are', async () => {
      await applyWith({ kind: 'error', status: 500, detail: 'Boom' });
      // In the decision bar, not a toast: a toast outlives the surface by
      // four seconds and leaves the buttons looking live over a refusal.
      expect(root().querySelector('.review__decision-copy--error')?.textContent).toContain('Boom');
      expect(byLabel('Apply to cloud')).toBeTruthy();
    });
  });

  // -- confirmation and double-submit ---------------------------------------

  describe('decision controls', () => {
    it('requires an explicit confirmation before applying', async () => {
      await render();
      await click(byLabel('Apply to cloud'));
      expect(api['applyThreadCloudDiff']).not.toHaveBeenCalled();
      await click(byLabel('Yes, apply to cloud'));
      expect(api['applyThreadCloudDiff']).toHaveBeenCalledTimes(1);
      expect(api['applyThreadCloudDiff']).toHaveBeenCalledWith('t1', 5);
    });

    it('lets the confirmation be cancelled without acting', async () => {
      await render();
      await click(byLabel('Apply to cloud'));
      await click(byLabel('Cancel'));
      expect(api['applyThreadCloudDiff']).not.toHaveBeenCalled();
      expect(byLabel('Apply to cloud')).toBeTruthy();
    });

    it('cancels an armed confirmation on Escape instead of closing the host', async () => {
      await render();
      await click(byLabel('Apply to cloud'));
      const event = new KeyboardEvent('keydown', {
        key: 'Escape',
        bubbles: true,
        cancelable: true,
      });
      root().dispatchEvent(event);
      await settle();
      // Stopped on the way up, so a hosting dialog never sees it.
      expect(event.defaultPrevented).toBe(true);
      expect(byLabel('Apply to cloud')).toBeTruthy();
      expect(api['applyThreadCloudDiff']).not.toHaveBeenCalled();
    });

    it('issues exactly one request when confirm is activated twice', async () => {
      await render();
      await click(byLabel('Apply to cloud'));
      const confirm = byLabel('Yes, apply to cloud')!;
      confirm.click();
      confirm.click();
      await settle();
      expect(api['applyThreadCloudDiff']).toHaveBeenCalledTimes(1);
    });

    it('issues exactly one reject when confirm is activated twice', async () => {
      await render();
      await click(byLabel('Reject staged changes'));
      const confirm = byLabel('Yes, discard them')!;
      confirm.click();
      confirm.click();
      await settle();
      expect(api['rejectThreadCloudDiff']).toHaveBeenCalledTimes(1);
      expect(api['rejectThreadCloudDiff']).toHaveBeenCalledWith('t1', 5);
    });

    it('reports busy to the host while a decision is in flight', async () => {
      // A Subject, not of(): with a synchronous response the in-flight window
      // never spans a change-detection boundary and the state is unobservable.
      const applied = new Subject<unknown>();
      await render();
      api['applyThreadCloudDiff'].mockReturnValue(applied.asObservable());
      const seen: boolean[] = [];
      (
        fixture.componentInstance as unknown as {
          busyChange: { subscribe(f: (v: boolean) => void): void };
        }
      ).busyChange.subscribe((v) => seen.push(v));

      await click(byLabel('Apply to cloud'));
      await click(byLabel('Yes, apply to cloud'));
      // The host uses this to refuse every close path mid-write (PC-20).
      expect(seen).toContain(true);
      // Both decision buttons are inert while it is true.
      expect(buttons().every((b) => b.disabled || b.textContent?.trim() === '')).toBe(true);

      applied.next({
        kind: 'ok',
        data: { thread_id: 't1', applied: 3, deleted: 1, errors: [], epoch: 6, overlay_reset: true },
      });
      applied.complete();
      await settle();
      expect(seen[seen.length - 1]).toBe(false);
    });
  });

  // -- outcome receipt -------------------------------------------------------

  describe('outcome receipt', () => {
    it('keeps the result on screen with the backend counts, not a toast alone', async () => {
      await render();
      await click(byLabel('Apply to cloud'));
      await click(byLabel('Yes, apply to cloud'));
      expect(text()).toContain('Applied to your cloud');
      expect(text()).toContain('3 written, 1 deleted');
      // The pending framing must not outlive the decision.
      expect(text()).toContain('Protected cloud · resolved');
      expect(text()).not.toContain('changed in this session');
      expect(root().querySelectorAll('.review__tally')).toHaveLength(0);
      // No toast in session context: the receipt is the confirmation, and a
      // bottom-right toast lands on top of its Done button.
      expect(toast['success']).not.toHaveBeenCalled();
      const facts = Array.from(root().querySelectorAll('.review__receipt-facts div')).map((d) => [
        d.querySelector('dt')?.textContent?.trim(),
        d.querySelector('dd')?.textContent?.trim(),
      ]);
      expect(facts).toContainEqual(['Epoch', '5']);
      expect(facts).toContainEqual(['Working copy cleared', 'Yes']);
      expect(byLabel('Done')).toBeTruthy();
    });

    it('surfaces overlay_reset:false with the duplicate-diff warning', async () => {
      await render({
        applyOutcome: {
          kind: 'ok',
          data: { thread_id: 't1', applied: 3, deleted: 1, errors: [], epoch: 6, overlay_reset: false },
        },
      });
      await click(byLabel('Apply to cloud'));
      await click(byLabel('Yes, apply to cloud'));
      const facts = Array.from(root().querySelectorAll('.review__receipt-facts div')).map((d) => [
        d.querySelector('dt')?.textContent?.trim(),
        d.querySelector('dd')?.textContent?.trim(),
      ]);
      expect(facts).toContainEqual(['Working copy cleared', 'No']);
      expect(text()).toContain('may stage the same changes again');
    });

    it('states that the record is browser-local, not a server receipt', async () => {
      await render();
      await click(byLabel('Apply to cloud'));
      await click(byLabel('Yes, apply to cloud'));
      // PC-20 is not resolved by this; the copy must not imply it is.
      expect(text()).toContain('recorded in this browser only');
      expect(readReceipt('t1')).toMatchObject({ decision: 'applied', applied: 3, deleted: 1 });
    });

    it('shows a reject receipt that does not claim a cloud write', async () => {
      await render();
      await click(byLabel('Reject staged changes'));
      await click(byLabel('Yes, discard them'));
      expect(toast['success']).not.toHaveBeenCalled();
      expect(text()).toContain('Staged changes discarded');
      expect(text()).toContain('Nothing was written to or removed from your cloud');
    });

    it('restores a receipt across a reload while it is still current', async () => {
      // Covers the code path, not the whole user journey: reopening the
      // surface after a decision has no entry point today, because the
      // pending-review banner is gone once nothing is pending. See the design
      // note §8 — PC-20 is not resolved by this.
      writeReceipt('t1', {
        decision: 'applied',
        epoch: 5,
        applied: 3,
        deleted: 1,
        overlayReset: true,
        at: new Date(Date.now() - 60_000).toISOString(),
      });
      await render({
        summary: threadSummary({ files: [], counts: { added: 0, modified: 0, deleted: 0 }, epoch: 6 }),
      });
      expect(text()).toContain('Applied to your cloud');
    });

    it('hides a stored receipt when a newer diff is pending', async () => {
      writeReceipt('t1', {
        decision: 'applied',
        epoch: 1,
        applied: 3,
        deleted: 1,
        overlayReset: true,
        at: new Date(Date.now() - 60_000).toISOString(),
      });
      await render();
      expect(text()).not.toContain('Applied to your cloud');
      expect(options()).toHaveLength(4);
    });
  });

  // -- project folder link (PC-19) ------------------------------------------

  describe('project folder action', () => {
    it('links to the protected project folder when it provably matches', async () => {
      await render({
        projectFolder: { url: 'https://cloud.example/apps/files/?dir=/Docs', name: 'Docs', targetPath: 'cloud' },
      });
      const link = root().querySelector<HTMLAnchorElement>('.review__folder-link')!;
      expect(link).toBeTruthy();
      // PC-01: name the folder the way the owner thinks of it, with the raw
      // workspace mount path still visible beside it.
      expect(root().querySelector('.review__title')?.textContent?.trim()).toBe('Docs');
      expect(text()).toContain('cloud');
      expect(link.getAttribute('href')).toBe('https://cloud.example/apps/files/?dir=/Docs');
      expect(link.getAttribute('rel')).toBe('noopener noreferrer');
      expect(link.getAttribute('aria-label')).toContain('Docs');
    });

    it('omits the action when the resolved mount does not match the summary', async () => {
      await render({
        projectFolder: { url: 'https://cloud.example/x', name: 'Other', targetPath: 'some/other/mount' },
      });
      expect(root().querySelector('.review__folder-link')).toBeNull();
    });

    it('omits the action when nothing could be resolved', async () => {
      await render();
      expect(root().querySelector('.review__folder-link')).toBeNull();
    });
  });

  // -- accessibility ---------------------------------------------------------

  describe('accessibility', () => {
    it('exposes the file list as a labelled listbox with selection state', async () => {
      await render();
      const list = root().querySelector('[role="listbox"]')!;
      // aria-label, not aria-labelledby: the visible heading is replaced by a
      // disclosure button on a phone, and a label that disappears with the
      // layout is worse than one that does not.
      expect(list.getAttribute('aria-label')).toBe('Files');
      expect(options().map((o) => o.getAttribute('aria-selected'))).toEqual([
        'true',
        'false',
        'false',
        'false',
      ]);
      expect(options()[0].getAttribute('aria-controls')).toBe(
        root().querySelector('[role="region"]')?.id,
      );
    });

    it('keeps the whole list to one tab stop via a roving tabindex', async () => {
      await render();
      expect(options().map((o) => o.getAttribute('tabindex'))).toEqual(['0', '-1', '-1', '-1']);
    });

    it('moves selection with the arrow keys', async () => {
      await render();
      const list = root().querySelector('[role="listbox"]')!;
      list.dispatchEvent(new KeyboardEvent('keydown', { key: 'ArrowDown', bubbles: true }));
      await settle();
      expect(options()[1].getAttribute('aria-selected')).toBe('true');
      list.dispatchEvent(new KeyboardEvent('keydown', { key: 'End', bubbles: true }));
      await settle();
      expect(options()[3].getAttribute('aria-selected')).toBe('true');
      list.dispatchEvent(new KeyboardEvent('keydown', { key: 'Home', bubbles: true }));
      await settle();
      expect(options()[0].getAttribute('aria-selected')).toBe('true');
    });

    it('announces the status word rather than a bare glyph', async () => {
      await render();
      const glyphs = Array.from(root().querySelectorAll<HTMLElement>('.review__file-glyph'));
      // role="img" is what makes the name reachable — aria-label on a bare
      // span is ignored by most screen readers.
      expect(glyphs.map((g) => g.getAttribute('role'))).toEqual(['img', 'img', 'img', 'img']);
      expect(glyphs.map((g) => g.getAttribute('aria-label'))).toEqual([
        'Modified',
        'Deleted',
        'Modified',
        'Added',
      ]);
    });

    it('does not encode status by colour alone', async () => {
      await render();
      const glyphs = Array.from(root().querySelectorAll<HTMLElement>('.review__file-glyph'));
      // Distinct symbols per status, plus the spelled-out tallies in the header.
      expect(glyphs.map((g) => g.textContent?.trim())).toEqual(['~', '−', '~', '+']);
      // Deleted paths also carry a non-colour cue in the stylesheet.
      const styles = readFileSync(
        'src/app/views/job-diff-review/job-diff-review.component.scss',
        'utf8',
      );
      expect(styles).toContain('text-decoration: line-through');
      // And the header spells the totals out in words.
      expect(
        Array.from(root().querySelectorAll('.review__tally-label')).map((l) =>
          l.textContent?.trim(),
        ),
      ).toEqual(['Added', 'Modified', 'Deleted']);
    });

    it('exposes a polite live region that reports async transitions', async () => {
      await render();
      const live = root().querySelector('[aria-live="polite"]')!;
      expect(live.getAttribute('role')).toBe('status');
      // Held as key+params and translated in the template, so the message is
      // real copy rather than a raw key baked in before the language loaded.
      expect(live.textContent).toContain('Showing changes for');
    });

    it('gives the review pane a labelled region tied to the selected file', async () => {
      await render();
      const region = root().querySelector('[role="region"]')!;
      expect(region.getAttribute('aria-label')).toBe(FILES[0].path);
    });

    it('marks conflict and partial notices as alerts', async () => {
      await render({
        applyOutcome: {
          kind: 'conflict',
          data: { code: 'external_modifications_detected', message: 'x', diverged: [] },
        },
      });
      await click(byLabel('Apply to cloud'));
      await click(byLabel('Yes, apply to cloud'));
      expect(root().querySelector('.review__notice--conflict')?.getAttribute('role')).toBe('alert');
    });
  });

  // -- request/action binding (the safety invariant) -------------------------

  /**
   * An observable-like whose emissions survive `unsubscribe()`.
   *
   * Cancellation and the generation guard are two independent defences and
   * they have to be tested independently: a real Subject stops delivering the
   * moment it is torn down, which would make every ordering test pass whether
   * or not the guard exists. This delivers anyway, so what is under test is
   * the guard.
   */
  function manual<T>() {
    const handlers: Array<(v: T) => void> = [];
    const state = { subscribes: 0, unsubscribes: 0 };
    return {
      obs: {
        subscribe(fn: (v: T) => void) {
          state.subscribes++;
          handlers.push(fn);
          return {
            unsubscribe() {
              state.unsubscribes++;
            },
          };
        },
      } as never,
      emit(value: T) {
        for (const fn of handlers) fn(value);
      },
      state,
    };
  }

  const inst = () => fixture.componentInstance as unknown as Record<string, any>;
  const setThread = (id: string | null) => inst()['threadId'].set(id);

  const okFile = (newContent: string) => ({
    kind: 'ok' as const,
    data: {
      thread_id: 't1',
      path: FILES[0].path,
      status: 'modified',
      old_content: 'VERSION=1\n',
      new_content: newContent,
      old_binary: false,
      new_binary: false,
    },
  });

  describe('binding reviewed bytes to the action target', () => {
    it('drops a summary for a target the host has already moved away from', async () => {
      await render();
      const stale = manual<unknown>();
      api['getThreadCloudDiffOutcome'].mockReturnValue(stale.obs);
      setThread('t2');
      await settle();

      api['getThreadCloudDiffOutcome'].mockReturnValue(
        of({ kind: 'ok', data: threadSummary({ thread_id: 't3', files: [FILES[0]] }) }),
      );
      setThread('t3');
      await settle();
      expect(options()).toHaveLength(1);

      // t2's answer finally arrives. It describes a review nobody is looking at.
      stale.emit({ kind: 'ok', data: threadSummary({ thread_id: 't2' }) });
      await settle();
      expect(options()).toHaveLength(1);
    });

    it('drops a stale response even when the host comes back to the same thread', async () => {
      // A -> B -> A. The ids match again, so an id check alone would accept
      // this; only the generation counter can tell the two loads apart.
      await render();
      const firstA = manual<unknown>();
      api['getThreadCloudDiffOutcome'].mockReturnValue(firstA.obs);
      setThread('t2');
      await settle();

      api['getThreadCloudDiffOutcome'].mockReturnValue(
        of({ kind: 'ok', data: threadSummary({ thread_id: 't2', files: [FILES[0], FILES[1]] }) }),
      );
      setThread('t1');
      await settle();
      setThread('t2');
      await settle();
      expect(options()).toHaveLength(2);

      firstA.emit({ kind: 'ok', data: threadSummary({ thread_id: 't2' }) });
      await settle();
      expect(options()).toHaveLength(2);
    });

    it('refuses an older epoch body for the same path', async () => {
      await render();
      const older = manual<unknown>();
      const newer = manual<unknown>();

      api['getThreadCloudDiffFileOutcome'].mockReturnValue(older.obs);
      api['getThreadCloudDiffOutcome'].mockReturnValue(
        of({ kind: 'ok', data: threadSummary({ epoch: 6 }) }),
      );
      inst()['reload']();
      await settle();

      api['getThreadCloudDiffFileOutcome'].mockReturnValue(newer.obs);
      api['getThreadCloudDiffOutcome'].mockReturnValue(
        of({ kind: 'ok', data: threadSummary({ epoch: 7 }) }),
      );
      inst()['reload']();
      await settle();

      newer.emit(okFile('EPOCH=7\n'));
      await settle();
      expect(inst()['selectedFile']().new_content).toBe('EPOCH=7\n');

      // Same path, same thread, older epoch. Nothing about the path
      // distinguishes it — the generation does.
      older.emit(okFile('EPOCH=5\n'));
      await settle();
      expect(inst()['selectedFile']().new_content).toBe('EPOCH=7\n');
      expect(inst()['epoch']()).toBe(7);
    });

    it('cancels the reads in flight when the target changes', async () => {
      await render();
      const summary = new Subject<unknown>();
      api['getThreadCloudDiffOutcome'].mockImplementation((id: string) =>
        id === 't2' ? summary : of({ kind: 'ok', data: threadSummary({ thread_id: id }) }),
      );
      setThread('t2');
      await settle();
      expect(summary.observed).toBe(true);

      setThread('t3');
      await settle();
      // Torn down, not merely ignored: a review nobody is looking at should
      // not still be holding a request open.
      expect(summary.observed).toBe(false);
    });

    it('cancels the reads in flight on destruction', async () => {
      await render();
      const summary = new Subject<unknown>();
      api['getThreadCloudDiffOutcome'].mockReturnValue(summary);
      setThread('t2');
      await settle();
      expect(summary.observed).toBe(true);
      fixture.destroy();
      expect(summary.observed).toBe(false);
    });

    it('cannot confirm a decision armed against a review that has gone away', async () => {
      await render();
      await click(byLabel('Apply to cloud'));
      expect(byLabel('Yes, apply to cloud')).toBeTruthy();

      // The host re-points mid-confirmation. Fail closed: the armed decision
      // goes with the review it belonged to.
      api['getThreadCloudDiffOutcome'].mockReturnValue(new Subject<unknown>());
      setThread('t2');
      await settle();
      expect(byLabel('Yes, apply to cloud')).toBeFalsy();
      expect(api['applyThreadCloudDiff']).not.toHaveBeenCalled();
    });

    it('never paints one review outcome onto another review', async () => {
      await render();
      const apply = manual<unknown>();
      api['applyThreadCloudDiff'].mockReturnValue(apply.obs);
      await click(byLabel('Apply to cloud'));
      await click(byLabel('Yes, apply to cloud'));

      api['getThreadCloudDiffOutcome'].mockReturnValue(
        of({ kind: 'ok', data: threadSummary({ thread_id: 't2', epoch: 2 }) }),
      );
      setThread('t2');
      await settle();

      // The latch defers t2 rather than swapping it in, so the write is still
      // bound to — and still displayed against — t1.
      expect(root().querySelector('.review')?.getAttribute('aria-busy')).toBe('true');
      expect(options()).toHaveLength(4);

      apply.emit({
        kind: 'ok',
        data: { thread_id: 't1', applied: 3, deleted: 1, errors: [], epoch: 6, overlay_reset: true },
      });
      await settle();

      // t2's pending diff is untouched by t1's result...
      expect(text()).not.toContain('Applied to your cloud');
      expect(options()).toHaveLength(4);
      // ...and the result is not lost either: it is recorded against the
      // thread it actually belongs to.
      expect(readReceipt('t1')).toMatchObject({ decision: 'applied', applied: 3, deleted: 1 });
      expect(readReceipt('t2')).toBeNull();
    });

    it('binds a per-file read to the loaded review, not to a freshly changed input', async () => {
      await render();
      const files = inst()['files']();
      api['getThreadCloudDiffFileOutcome'].mockClear();

      // The adversarial window: the host has set a new id and the reload
      // effect has NOT run yet, so `loaded` is still t1 while `threadId()`
      // already reads t2. Choosing the endpoint from the input here fetched
      // target B's bytes and painted them under target A's summary.
      inst()['threadId'].set('t2');
      inst()['selectFile'](files[1]);

      expect(api['getThreadCloudDiffFileOutcome']).toHaveBeenCalledTimes(1);
      expect(api['getThreadCloudDiffFileOutcome']).toHaveBeenCalledWith('t1', files[1].path);
      const targets = api['getThreadCloudDiffFileOutcome'].mock.calls.map((c: unknown[]) => c[0]);
      expect(targets).not.toContain('t2');
      expect(api['getJobDiffFileOutcome']).not.toHaveBeenCalled();
    });

    it('drops a per-file body once its review is no longer loaded', async () => {
      await render();
      const files = inst()['files']();
      const late = manual<unknown>();
      api['getThreadCloudDiffFileOutcome'].mockReturnValue(late.obs);
      inst()['selectFile'](files[1]);

      // t2's summary never resolves, so nothing is loaded at all.
      api['getThreadCloudDiffOutcome'].mockReturnValue(new Subject());
      setThread('t2');
      await settle();
      expect(inst()['loaded']()).toBeNull();

      late.emit(okFile('LEAKED\n'));
      await settle();
      expect(inst()['selectedFile']()).toBeNull();
      expect(text()).not.toContain('LEAKED');
    });

    it('issues no per-file request at all when no review is loaded', async () => {
      await render();
      const files = inst()['files']();
      api['getThreadCloudDiffOutcome'].mockReturnValue(new Subject());
      setThread('t2');
      await settle();
      expect(inst()['loaded']()).toBeNull();

      api['getThreadCloudDiffFileOutcome'].mockClear();
      inst()['selectFile'](files[0]);
      // Fail closed: no identity, no guess, no request.
      expect(api['getThreadCloudDiffFileOutcome']).not.toHaveBeenCalled();
      expect(api['getJobDiffFileOutcome']).not.toHaveBeenCalled();
    });
  });

  // -- the decision latch ----------------------------------------------------

  describe('decision latch across host re-pointing', () => {
    /** Start a job apply that will not settle until the returned handle is
     *  emitted, and return that handle. */
    async function startJobApply() {
      await render({ jobId: 'j1', threadId: null });
      const apply = manual<unknown>();
      api['acceptJobDiff'].mockReturnValue(apply.obs);
      await click(byLabel('Accept all changes'));
      await click(byLabel('Yes, accept them'));
      return apply;
    }

    async function startJobReject() {
      await render({ jobId: 'j1', threadId: null });
      const reject = manual<unknown>();
      api['rejectJobDiff'].mockReturnValue(reject.obs);
      await click(byLabel('Reject all changes'));
      await click(byLabel('Yes, reject them'));
      return reject;
    }

    /** Point the host at job B and let the reload effect run. */
    async function repointToJobB(): Promise<void> {
      api['getJobDiffOutcome']
        .mockClear()
        .mockReturnValue(of({ kind: 'ok', data: jobSummary({ job_id: 'j2' }) }));
      inst()['jobId'].set('j2');
      await settle();
    }

    const busy = () => root().querySelector('.review')?.getAttribute('aria-busy');

    it('holds the latch and refuses a second decision when re-pointed mid-apply', async () => {
      const apply = await startJobApply();
      const resolutions: string[] = [];
      (
        fixture.componentInstance as unknown as {
          resolved: { subscribe(f: (v: string) => void): void };
        }
      ).resolved.subscribe((v) => resolutions.push(v));

      expect(busy()).toBe('true');
      await repointToJobB();

      // Still A's write, still A's surface. Dropping the latch here is what
      // made the surface actionable again over a continuing cloud write.
      expect(busy()).toBe('true');
      expect(text()).toContain('Writing the changes to the cloud folder');
      expect(byLabel('Accept all changes')).toBeFalsy();
      expect(byLabel('Yes, accept them')).toBeFalsy();
      expect(byLabel('Reject all changes')).toBeFalsy();
      // No second write, and B has not been fetched — B must not be presented
      // as actionable while A is still being written.
      expect(api['acceptJobDiff']).toHaveBeenCalledTimes(1);
      expect(api['acceptJobDiff']).toHaveBeenCalledWith('j1');
      expect(api['getJobDiffOutcome']).not.toHaveBeenCalled();

      apply.emit({
        kind: 'ok',
        data: { job_id: 'j1', diff_status: 'accepted', status: 'completed', applied: 3, deleted: 1 },
      });
      await settle();

      // A's outcome reported through the one channel that survives the swap.
      // A job has no thread id, so the receipt store cannot hold it — which is
      // exactly how the result used to be lost.
      expect(toast['success']).toHaveBeenCalled();
      expect(resolutions).toEqual(['accepted']);
      expect(localStorage.length).toBe(0);
      // Only now does the latest requested target load.
      expect(api['getJobDiffOutcome']).toHaveBeenCalledTimes(1);
      expect(api['getJobDiffOutcome']).toHaveBeenCalledWith('j2');
      expect(busy()).toBeNull();
      // ...and B is a fresh, actionable review, not A's outcome.
      expect(byLabel('Accept all changes')).toBeTruthy();
      expect(text()).not.toContain('Changes accepted');
    });

    it('holds the latch and refuses a second decision when re-pointed mid-reject', async () => {
      const reject = await startJobReject();
      expect(busy()).toBe('true');
      await repointToJobB();

      expect(busy()).toBe('true');
      expect(text()).toContain('Discarding the changes');
      expect(byLabel('Reject all changes')).toBeFalsy();
      expect(byLabel('Yes, reject them')).toBeFalsy();
      expect(api['rejectJobDiff']).toHaveBeenCalledTimes(1);
      expect(api['rejectJobDiff']).toHaveBeenCalledWith('j1');
      expect(api['getJobDiffOutcome']).not.toHaveBeenCalled();

      reject.emit({
        kind: 'ok',
        data: { job_id: 'j1', diff_status: 'rejected', status: 'completed' },
      });
      await settle();

      expect(toast['success']).toHaveBeenCalled();
      expect(api['getJobDiffOutcome']).toHaveBeenCalledTimes(1);
      expect(api['getJobDiffOutcome']).toHaveBeenCalledWith('j2');
      expect(busy()).toBeNull();
    });

    it('takes the LAST requested target when the host re-points twice', async () => {
      const apply = await startJobApply();
      inst()['jobId'].set('j2');
      await settle();
      api['getJobDiffOutcome']
        .mockClear()
        .mockReturnValue(of({ kind: 'ok', data: jobSummary({ job_id: 'j3' }) }));
      inst()['jobId'].set('j3');
      await settle();
      expect(api['getJobDiffOutcome']).not.toHaveBeenCalled();

      apply.emit({
        kind: 'ok',
        data: { job_id: 'j1', diff_status: 'accepted', status: 'completed', applied: 1, deleted: 0 },
      });
      await settle();
      expect(api['getJobDiffOutcome']).toHaveBeenCalledTimes(1);
      expect(api['getJobDiffOutcome']).toHaveBeenCalledWith('j3');
    });

    for (const [label, outcome, expected] of [
      [
        'conflict',
        {
          kind: 'conflict',
          data: {
            code: 'external_modifications_detected',
            message: 'x',
            diverged: [{ path: 'a', kind: 'etag_mismatch' }],
          },
        },
        'The cloud folder changed underneath this review',
      ],
      [
        'partial failure',
        {
          kind: 'partial',
          data: { code: 'partial_write_failure', applied: 2, deleted: 1, errors: ['a: 507'] },
        },
        'Some writes failed',
      ],
      ['error', { kind: 'error', status: 500, detail: 'Boom' }, 'Boom'],
    ] as const) {
      it(`does not lose a job ${label} when the host re-points mid-apply`, async () => {
        const apply = await startJobApply();
        await repointToJobB();
        apply.emit(outcome);
        await settle();

        // The notice and the decision-bar error both live on the surface that
        // is about to be replaced, so the failure has to leave with it.
        expect(toast['danger']).toHaveBeenCalledWith(expected);
        expect(api['getJobDiffOutcome']).toHaveBeenCalledWith('j2');
        expect(busy()).toBeNull();
      });
    }

    it('reloads only once for a stale outcome that arrives while re-pointed', async () => {
      const apply = await startJobApply();
      await repointToJobB();
      apply.emit({ kind: 'stale', staged_epoch: 9 });
      await settle();
      // The deferred target is the reload; `reload()` must not also fire and
      // pull job A back.
      expect(toast['info']).toHaveBeenCalled();
      expect(api['getJobDiffOutcome']).toHaveBeenCalledTimes(1);
      expect(api['getJobDiffOutcome']).toHaveBeenCalledWith('j2');
    });

    it('changes nothing when the host never re-points', async () => {
      const apply = await startJobApply();
      api['getJobDiffOutcome'].mockClear();
      apply.emit({
        kind: 'ok',
        data: { job_id: 'j1', diff_status: 'accepted', status: 'completed', applied: 3, deleted: 1 },
      });
      await settle();

      // The receipt stays on its own surface and nothing is reloaded.
      expect(api['getJobDiffOutcome']).not.toHaveBeenCalled();
      expect(text()).toContain('Changes accepted');
      expect(byLabel('Done')).toBeTruthy();
      expect(busy()).toBeNull();
      // A job receipt claims no browser persistence, because there is none.
      expect(text()).not.toContain('recorded in this browser only');
      expect(localStorage.length).toBe(0);
    });
  });

  // -- in-flight progress ----------------------------------------------------

  describe('in-flight decisions', () => {
    it('shows an explicit, persistent progress state while applying', async () => {
      const applied = new Subject<unknown>();
      await render();
      api['applyThreadCloudDiff'].mockReturnValue(applied.asObservable());
      await click(byLabel('Apply to cloud'));
      await click(byLabel('Yes, apply to cloud'));

      // The observed apply took 34 seconds. Two disabled buttons is what a
      // frozen page looks like; this says what is happening and why to wait.
      expect(text()).toContain('Applying your changes to the cloud');
      expect(text()).toContain('Keep this open');
      expect(root().querySelector('.review__decision-copy--running app-spinner')).toBeTruthy();
      expect(root().querySelector('.review')?.getAttribute('aria-busy')).toBe('true');
      // No decision control is even mounted, so a second submit is impossible.
      expect(byLabel('Yes, apply to cloud')).toBeFalsy();
      expect(byLabel('Apply to cloud')).toBeFalsy();

      applied.next({
        kind: 'ok',
        data: { thread_id: 't1', applied: 3, deleted: 1, errors: [], epoch: 6, overlay_reset: true },
      });
      applied.complete();
      await settle();
      expect(root().querySelector('.review')?.getAttribute('aria-busy')).toBeNull();
      expect(text()).toContain('Applied to your cloud');
    });

    it('shows the reject wording while rejecting', async () => {
      const rejected = new Subject<unknown>();
      await render();
      api['rejectThreadCloudDiff'].mockReturnValue(rejected.asObservable());
      await click(byLabel('Reject staged changes'));
      await click(byLabel('Yes, discard them'));
      expect(text()).toContain('Discarding the staged changes');
      expect(root().querySelector('.review')?.getAttribute('aria-busy')).toBe('true');
    });
  });

  // -- reject outcomes (parity with apply) -----------------------------------

  describe('reject outcomes', () => {
    async function rejectWith(outcome: unknown): Promise<void> {
      await render({ rejectOutcome: outcome });
      await click(byLabel('Reject staged changes'));
      await click(byLabel('Yes, discard them'));
    }

    it('reloads on a stale epoch instead of leaving the controls live', async () => {
      await rejectWith({ kind: 'stale', staged_epoch: 9 });
      expect(toast['info']).toHaveBeenCalled();
      expect(api['getThreadCloudDiffOutcome']).toHaveBeenCalledTimes(2);
      // Crucially NOT a receipt: nothing was discarded.
      expect(text()).not.toContain('Staged changes discarded');
    });

    it('reloads into the resolved state when nothing is staged any more', async () => {
      await rejectWith({ kind: 'nothing_staged' });
      expect(api['getThreadCloudDiffOutcome']).toHaveBeenCalledTimes(2);
      expect(text()).not.toContain('Staged changes discarded');
    });

    it('reports a refused reject where the controls are', async () => {
      await rejectWith({ kind: 'error', status: 422, detail: 'Invalid epoch pin' });
      expect(root().querySelector('.review__decision-copy--error')?.textContent).toContain(
        'Invalid epoch pin',
      );
      // The staged set is untouched, so the controls stay — but they are
      // sitting under an explanation now, not under silence.
      expect(byLabel('Reject staged changes')).toBeTruthy();
      expect(text()).not.toContain('Staged changes discarded');
    });
  });

  // -- per-file retry --------------------------------------------------------

  describe('per-file retry', () => {
    it('issues a fresh request for the same path', async () => {
      await render({ fileOutcome: { kind: 'error', status: 500, detail: 'Read failed' } });
      expect(api['getThreadCloudDiffFileOutcome']).toHaveBeenCalledTimes(1);
      expect(text()).toContain('Read failed');

      // The old handler called selectFile(entry), which the same-path guard
      // turned into a no-op — the button was decoration.
      await click(byLabel('Try again'));
      expect(api['getThreadCloudDiffFileOutcome']).toHaveBeenCalledTimes(2);
      expect(api['getThreadCloudDiffFileOutcome']).toHaveBeenLastCalledWith('t1', FILES[0].path);
    });

    it('recovers when the retry succeeds', async () => {
      await render({ fileOutcome: { kind: 'error', status: 500, detail: 'Read failed' } });
      api['getThreadCloudDiffFileOutcome'].mockReturnValue(of(okFile('VERSION=2\n')));
      await click(byLabel('Try again'));
      expect(text()).not.toContain('Read failed');
      expect(root().querySelector('.review__monaco')).toBeTruthy();
    });
  });

  // -- truthful per-file 404 copy --------------------------------------------

  describe('per-file 404 copy', () => {
    it('says the path left the staged set when the backend says so', async () => {
      await render({ fileOutcome: { kind: 'missing', code: 'not_in_staged_diff' } });
      expect(text()).toContain('it is not any more');
    });

    it('says the staged copy is unreadable when the backend says so', async () => {
      await render({ fileOutcome: { kind: 'missing', code: 'staged_content_unreadable' } });
      expect(text()).toContain('stored copy cannot be read');
      expect(text()).not.toContain('re-staged, or the changes were resolved');
    });

    it('falls back to copy true for every case on an untagged 404', async () => {
      // Older orchestrators send a plain-string detail, and job mode has no
      // equivalent code at all.
      await render({ fileOutcome: { kind: 'missing' } });
      expect(text()).toContain('It was in the staged set when this review loaded');
      expect(text()).toContain('Reload');
    });
  });

  // -- Escape ownership ------------------------------------------------------

  describe('escape ownership', () => {
    function pressEscape(target: EventTarget = document.body): KeyboardEvent {
      const event = new KeyboardEvent('keydown', {
        key: 'Escape',
        bubbles: true,
        cancelable: true,
      });
      target.dispatchEvent(event);
      return event;
    }

    it('cancels the confirmation when focus has fallen back to the body', async () => {
      // Arming removes the button that was pressed, so this is the normal
      // case, not an edge one: a host-element listener never sees the key.
      await render();
      await click(byLabel('Apply to cloud'));
      const event = pressEscape(document.body);
      await settle();
      expect(event.defaultPrevented).toBe(true);
      expect(byLabel('Apply to cloud')).toBeTruthy();
    });

    it('leaves Escape alone when nothing is armed', async () => {
      // An inline job review must not swallow a key meant for the page.
      await render();
      const event = pressEscape(document.body);
      await settle();
      expect(event.defaultPrevented).toBe(false);
    });

    it('refuses every dismissal path while a decision is in flight', async () => {
      const applied = new Subject<unknown>();
      await render();
      api['applyThreadCloudDiff'].mockReturnValue(applied.asObservable());
      await click(byLabel('Apply to cloud'));
      await click(byLabel('Yes, apply to cloud'));
      const event = pressEscape(document.body);
      await settle();
      // Swallowed before the hosting dialog can act on it: a dismissal must
      // not be able to race an in-flight cloud write (PC-20).
      expect(event.defaultPrevented).toBe(true);
      expect(root().querySelector('.review')?.getAttribute('aria-busy')).toBe('true');
    });

    it('yields Escape to a modal opened above an inline review', async () => {
      await render();
      await click(byLabel('Apply to cloud'));
      const above = document.createElement('div');
      above.setAttribute('role', 'dialog');
      document.body.appendChild(above);
      try {
        const event = pressEscape(document.body);
        await settle();
        expect(event.defaultPrevented).toBe(false);
        // The confirmation is still armed — the key was not ours to consume.
        expect(byLabel('Yes, apply to cloud')).toBeTruthy();
      } finally {
        above.remove();
      }
    });

    it('yields Escape to a dialog stacked above its own', async () => {
      await render();
      await click(byLabel('Apply to cloud'));
      const host = root();
      const own = document.createElement('div');
      own.setAttribute('role', 'dialog');
      host.parentElement!.insertBefore(own, host);
      own.appendChild(host);
      const above = document.createElement('div');
      above.setAttribute('role', 'dialog');
      document.body.appendChild(above);
      try {
        const event = pressEscape(document.body);
        await settle();
        expect(event.defaultPrevented).toBe(false);
      } finally {
        above.remove();
        own.parentElement!.insertBefore(host, own);
        own.remove();
      }
    });

    it('still owns Escape inside its own dialog', async () => {
      await render();
      await click(byLabel('Apply to cloud'));
      const host = root();
      const own = document.createElement('div');
      own.setAttribute('role', 'dialog');
      host.parentElement!.insertBefore(own, host);
      own.appendChild(host);
      try {
        const event = pressEscape(document.body);
        await settle();
        expect(event.defaultPrevented).toBe(true);
        expect(byLabel('Apply to cloud')).toBeTruthy();
      } finally {
        own.parentElement!.insertBefore(host, own);
        own.remove();
      }
    });

    it('returns focus to the control that armed the confirmation', async () => {
      await render();
      await click(byLabel('Apply to cloud'));
      // Focus lands on the confirm action, or the second step is unreachable.
      expect(document.activeElement?.textContent?.trim()).toBe('Yes, apply to cloud');
      await click(byLabel('Cancel'));
      // ...and comes back, rather than being dropped on <body>.
      expect(document.activeElement?.textContent?.trim()).toBe('Apply to cloud');
    });
  });

  // -- receipt context correctness -------------------------------------------

  describe('receipt context', () => {
    it('claims neither an epoch nor browser storage in job context', async () => {
      await render({ jobId: 'j1', threadId: null });
      await click(byLabel('Accept all changes'));
      await click(byLabel('Yes, accept them'));
      expect(text()).toContain('Changes accepted');
      const facts = Array.from(root().querySelectorAll('.review__receipt-facts div')).map((d) =>
        d.querySelector('dt')?.textContent?.trim(),
      );
      // A job diff has no epoch; the receipt used to print a fabricated 0.
      expect(facts).not.toContain('Epoch');
      // And nothing was written to localStorage, so nothing may say it was.
      expect(text()).not.toContain('recorded in this browser only');
      expect(localStorage.length).toBe(0);
    });

    it('drops a receipt that has aged out', async () => {
      const old = new Date(Date.now() - 8 * 24 * 60 * 60 * 1000).toISOString();
      writeReceipt('t1', {
        decision: 'applied',
        epoch: 5,
        applied: 3,
        deleted: 1,
        overlayReset: true,
        at: old,
      });
      expect(readReceipt('t1')).toBeNull();
      await render({
        summary: threadSummary({ files: [], counts: { added: 0, modified: 0, deleted: 0 }, epoch: 6 }),
      });
      expect(text()).not.toContain('Applied to your cloud');
      expect(text()).toContain('Nothing staged for review');
    });
  });

  // -- small-screen composition ---------------------------------------------

  describe('small-screen composition', () => {
    function compactViewport(matches: boolean) {
      const original = window.matchMedia;
      (window as unknown as { matchMedia: unknown }).matchMedia = vi.fn().mockReturnValue({
        matches,
        media: '',
        addEventListener: () => {},
        removeEventListener: () => {},
      });
      return () => {
        (window as unknown as { matchMedia: unknown }).matchMedia = original;
      };
    }

    it('replaces the always-open list with a collapsed chooser on a phone', async () => {
      const restore = compactViewport(true);
      try {
        await render();
        // At 375x667 an always-open list left the viewer about 79px, which is
        // not a review.
        expect(root().querySelector('[role="listbox"]')).toBeNull();
        const toggle = root().querySelector<HTMLButtonElement>('.review__files-toggle')!;
        expect(toggle.getAttribute('aria-expanded')).toBe('false');
        expect(toggle.getAttribute('aria-controls')).toBe(
          root().querySelector('.review__files')?.querySelector('ul')?.id ?? toggle.getAttribute('aria-controls'),
        );
        expect(toggle.textContent).toContain('File 1 of 4');
        // The selection is still made, and its diff is what the viewer shows.
        expect(root().querySelector('.review__viewer-path')?.textContent).toContain(FILES[0].path);

        await click(toggle);
        expect(root().querySelector('[role="listbox"]')).toBeTruthy();
        options()[2].click();
        await settle();
        // Picking collapses it again, handing the height back to the viewer.
        expect(root().querySelector('[role="listbox"]')).toBeNull();
        expect(root().querySelector('.review__viewer-path')?.textContent).toContain(FILES[2].path);
      } finally {
        restore();
      }
    });

    it('folds the technical details away on a phone', async () => {
      const restore = compactViewport(true);
      try {
        await render();
        expect(root().querySelector('details.review__tech')?.hasAttribute('open')).toBe(false);
        // Folded, not removed: the mount and epoch are still one tap away.
        expect(text()).toContain('cloud');
        expect(text()).toContain('Details');
      } finally {
        restore();
      }
    });

    it('leaves the technical details open and the list permanent on a desktop', async () => {
      const restore = compactViewport(false);
      try {
        await render();
        expect(root().querySelector('details.review__tech')?.hasAttribute('open')).toBe(true);
        expect(root().querySelector('[role="listbox"]')).toBeTruthy();
        expect(root().querySelector('.review__files-toggle')).toBeNull();
      } finally {
        restore();
      }
    });
  });

  describe('stylesheet contract', () => {
    // jsdom has no layout engine, so the source is the only place these are
    // assertable — the pattern canvas-browser-renderer.component.spec.ts uses.
    const styles = readFileSync(
      'src/app/views/job-diff-review/job-diff-review.component.scss',
      'utf8',
    );

    it('honours reduced motion locally (the global rule only clamps transitions)', () => {
      expect(styles).toContain('@media (prefers-reduced-motion: reduce)');
      expect(styles).toContain('animation: none !important');
      expect(styles).toContain('transition: none !important');
    });

    it('keeps the decision bar visually separated and pinned', () => {
      expect(styles).toContain('position: sticky');
      expect(styles).toContain('box-shadow: var(--shadow-md)');
    });

    it('uses no fixed heights for the surface itself', () => {
      // The old drawer was height:70vh; the panes now size to the host.
      expect(styles).not.toMatch(/^\s*height:\s*\d+vh/m);
    });

    it('references only design tokens that actually exist', () => {
      // The previous revision read 13 undefined custom properties, which is
      // why it rendered unpadded and borderless. These are the names that
      // never existed in this codebase.
      for (const phantom of [
        '--space-1',
        '--space-2',
        '--space-3',
        '--space-4',
        '--font-size-sm)',
        '--text-strong',
        '--border)',
        '--accent-tint',
        '--accent-strong',
      ]) {
        expect(styles).not.toContain(`var(${phantom}`);
      }
    });
  });
});

// ============================================================================
// Host wiring regression guards
// ============================================================================

describe('persistent-chat host wiring', () => {
  const src = readFileSync(
    'src/app/views/persistent-chat/persistent-chat.component.ts',
    'utf8',
  );

  /** The balanced `{ … }` block that follows `marker`. */
  function blockAfter(source: string, marker: string): string {
    const start = source.indexOf(marker);
    expect(start, `marker not found: ${marker}`).toBeGreaterThan(-1);
    const open = source.indexOf('{', start);
    let depth = 0;
    let i = open;
    for (; i < source.length; i++) {
      if (source[i] === '{') depth++;
      else if (source[i] === '}') {
        depth--;
        if (depth === 0) break;
      }
    }
    return source.slice(open, i + 1);
  }

  /** Net `{`/`}` depth of `source`: 0 means every brace opened in it has
   *  also closed, i.e. its end sits at the same nesting level as its start. */
  function netBraceDepth(source: string): number {
    let depth = 0;
    for (const ch of source) {
      if (ch === '{') depth++;
      else if (ch === '}') depth--;
    }
    return depth;
  }

  it('renders the pending-review action OUTSIDE the connection gate', () => {
    // PC-25: the header actions used to be rendered only while
    // chat.isConnected() (first inside a dedicated status bar, and still
    // today inside `.header-left`'s badge row and `.header-right`'s action
    // list), and one of those gated spots used to be the only way to open a
    // review. An ended thread with a genuine staged diff could not reach it.
    expect(src).toContain('<app-cloud-review-banner');
    const headerIndex = src.indexOf('<div class="chat-header"');
    const bannerIndex = src.indexOf('<app-cloud-review-banner');
    expect(headerIndex).toBeGreaterThan(-1);
    expect(bannerIndex).toBeGreaterThan(headerIndex);
    // The banner must sit at the same brace-nesting depth as the header's own
    // opening tag — i.e. every `@if (chat.isConnected()) { … }` opened inside
    // the header (there are two today: the row-1 badges and the action menu)
    // has already closed by the time the banner appears. A depth other than
    // zero means the banner ended up a descendant of a gate, not a sibling.
    expect(netBraceDepth(src.slice(headerIndex, bannerIndex))).toBe(0);
    // The invariant is only meaningful if the file actually still gates
    // connected-only header content — guard against both sides degrading
    // into a vacuous pass (e.g. the gate being deleted entirely).
    expect(src.slice(headerIndex, bannerIndex)).toContain(
      '@if (chat.isConnected())',
    );
  });

  it('no longer ships the passive status badge as the review opener', () => {
    expect(src).not.toContain("'chat.status.cloudChanges'");
  });

  it('opens the review from exactly one place — the banner', () => {
    const opens = src.match(/cloudDiffPanelOpen\.set\(true\)/g) ?? [];
    expect(opens).toHaveLength(1);
    expect(src).toContain('(review)="chat.cloudDiffPanelOpen.set(true)"');
  });

  it('keeps the review surface lazy so it cannot grow the initial bundle', () => {
    // The initial bundle is at 2.69MB against a 2.75MB hard-error budget, and
    // @defer only lazy-loads a component if EVERY usage site defers it.
    expect(src).toContain('@defer (when chat.cloudDiffPanelOpen() || !!jobDiffId())');
    const jobReview = readFileSync(
      'src/app/views/job-review/job-review.component.ts',
      'utf8',
    );
    expect(blockAfter(jobReview, '@defer')).toContain('<app-job-diff-review');
  });

  it('hosts the review as a dialog rather than an inline fixed-height drawer', () => {
    expect(src).toContain('<app-cloud-review-dialog');
    expect(src).not.toContain('flex-direction:column;height:70vh');
  });

  it('keeps the correct project folder reachable outside the review (PC-19)', () => {
    // The link existed only inside an open review, so after Done or a reload
    // the permanent Files button still opened the empty legacy session folder.
    expect(src).toContain('openProjectFiles()');
    expect(src).toContain("'chat.header.projectFilesButton'");
    // Two unambiguous actions rather than one relabelled guess: the session
    // folder is only renamed when a project action sits beside it.
    expect(src).toContain('sessionFilesLabelKey()');
    // The asleep/ended branch must offer it too — that is where PC-19 was hit.
    // Its gate also admits a VM lifecycle (a suspended VM's IDE/SSH wake), but
    // it must still admit the verified project folder and render its action.
    const asleepBranch =
      '@else if (chat.workspaceLifecycle() || chat.cloudSessionUrl() || chat.ncSessionFolder() || chat.verifiedProjectFolder())';
    expect(src).toContain(asleepBranch);
    expect(blockAfter(src, asleepBranch)).toContain('(click)="openProjectFiles()"');
  });

  it('never offers a project folder that was not cross-checked', () => {
    // verifiedProjectFolder is protectedFolderLink AND folderLinkMatches
    // against the summary's protected_mount; the raw candidate must not reach
    // any navigation or the review surface.
    expect(src).not.toContain('chat.protectedFolderLink()');
    expect(src).toContain('[projectFolder]="chat.verifiedProjectFolder()"');
  });

  it('wires the banner to the probe state, not just the count', () => {
    // A failed count probe used to render as "nothing staged".
    expect(src).toContain('[probe]="chat.cloudDiffProbe()"');
    expect(src).toContain('(recheck)="chat.refreshCloudDiffCount()"');
    // And never the raw mount path as the folder's name.
    expect(src).not.toContain('[mount]="chat.protectedMountName()"');
  });
});
