/**
 * Phase 3 — the outbox: queued sends are user intent, not transport state.
 * (knowledge-base/knowledge/features/session_reliability_and_transport_simplification.md)
 *
 * These tests cover the categorical fix for the "Creating thread" send-swallow:
 * a send committed by the user survives disconnect/reconnect/thread-creation,
 * flushes FIFO with one POST in flight, and never silently double-sends or
 * cross-mutates another thread's queue.
 */
import {afterEach, beforeEach, describe, expect, it, vi} from 'vitest';
import {signal} from '@angular/core';
import {TestBed} from '@angular/core/testing';
import {HttpClient, HttpErrorResponse} from '@angular/common/http';
import {Observable, of, throwError, Subject} from 'rxjs';
import {TranslocoService} from '@jsverse/transloco';
import {PersistentChatService} from './persistent-chat.service';
import {ApiService} from './api.service';
import {CapabilitiesService} from './capabilities.service';
import {IndexedDbService} from './indexed-db.service';
import {NotificationService} from './notification.service';
import {AppToastService} from '../../ui/toast';
import {isUserTurn, UserTurn} from '../models/turn.model';
import {
    FilePreview,
    FileType,
    ThreadUploadedFile,
    ThreadUploadEvent,
    UploadStatus,
} from '../models/file.model';

// --- Minimal transport mocks (mirrors the main service spec's scaffolding) ---

interface MockEventSource {
    url: string;
    readyState: number;
    close: ReturnType<typeof vi.fn>;
    onopen: ((e: any) => void) | null;
    onmessage: ((e: MessageEvent) => void) | null;
    onerror: ((e: any) => void) | null;
    listeners: Record<string, ((e: any) => void)[]>;
}

function fireSseOpen(es: MockEventSource): void {
    es.readyState = 1;
    es.onopen?.({});
}

function fireSseMessage(es: MockEventSource, frame: Record<string, unknown>, lastEventId = ''): void {
    es.onmessage?.({data: JSON.stringify(frame), lastEventId} as MessageEvent);
}

function fireSseNamedEvent(es: MockEventSource, name: string, frame: Record<string, unknown>): void {
    (es.listeners[name] || []).forEach((h) => h({data: JSON.stringify(frame), lastEventId: ''} as MessageEvent));
}

/** Let queued microtasks + a macrotask boundary drain (flush POSTs resolve). */
const flushTick = () => new Promise((r) => setTimeout(r, 0));

const INPUT = '/input';
const isInput = (url: unknown) => String(url).endsWith(INPUT);

function createService() {
    const mockHttp: any = {
        get: vi.fn().mockReturnValue(of({status: 'active', total_turns: 0, messages: [], total: 0})),
        post: vi.fn().mockReturnValue(of({})),
        patch: vi.fn().mockReturnValue(of({status: 'updated'})),
        delete: vi.fn().mockReturnValue(of({})),
    };
    const mockApi: any = {
        uploadOneToThread: vi.fn().mockReturnValue(of(done())),
        deleteThreadUpload: vi.fn().mockReturnValue(of(undefined)),
        humanizeUploadError: vi.fn().mockReturnValue('upload failed'),
    };
    const mockCache: any = {
        getThreadCursor: vi.fn().mockResolvedValue(null),
        setThreadCursor: vi.fn().mockResolvedValue(undefined),
        deleteThreadCursor: vi.fn().mockResolvedValue(undefined),
        getThreadMessages: vi.fn().mockResolvedValue([]),
        getNewestCachedCreatedAt: vi.fn().mockResolvedValue(null),
        upsertThreadMessages: vi.fn().mockResolvedValue(undefined),
        clearThreadMessages: vi.fn().mockResolvedValue(undefined),
    };
    const mockToast: any = {
        show: vi.fn(), info: vi.fn(), success: vi.fn(), warning: vi.fn(), danger: vi.fn(),
        dismiss: vi.fn(), dismissAll: vi.fn(),
    };

    const sseInstances: MockEventSource[] = [];
    function MockEventSourceCtor(this: any, url: string) {
        const es: MockEventSource = {
            url, readyState: 0, close: vi.fn(),
            onopen: null, onmessage: null, onerror: null, listeners: {},
        };
        (es as any).addEventListener = (name: string, cb: (e: any) => void) => {
            (es.listeners[name] ||= []).push(cb);
        };
        sseInstances.push(es);
        return es as any;
    }
    (MockEventSourceCtor as any).CONNECTING = 0;
    (MockEventSourceCtor as any).OPEN = 1;
    (MockEventSourceCtor as any).CLOSED = 2;
    (globalThis as any).EventSource = MockEventSourceCtor;

    function MockWebSocketCtor(this: any, url: string) {
        return {
            url, readyState: 1, send: vi.fn(), close: vi.fn(),
            onopen: null, onmessage: null, onclose: null, onerror: null,
            addEventListener: vi.fn(), removeEventListener: vi.fn(),
        } as any;
    }
    (MockWebSocketCtor as any).OPEN = 1;
    (MockWebSocketCtor as any).CONNECTING = 0;
    (globalThis as any).WebSocket = MockWebSocketCtor;

    const mockNotifications: any = {
        isConnected: signal(false),
        lifecycleEvent: signal<{thread_id: string; state: string; reason?: string} | null>(null),
        cloudDiffStagedEvent: signal(null),
    };

    TestBed.resetTestingModule();
    TestBed.configureTestingModule({
        providers: [
            {provide: HttpClient, useValue: mockHttp},
            {provide: ApiService, useValue: mockApi},
            {
                provide: CapabilitiesService,
                useValue: {
                    datasourceScopeAutoAttachAvailable: () => true,
                    datasourceScopeAutoAttachAvailability$: of(true),
                },
            },
            {provide: IndexedDbService, useValue: mockCache},
            {provide: AppToastService, useValue: mockToast},
            {provide: NotificationService, useValue: mockNotifications},
            {provide: TranslocoService, useValue: {translate: (k: string) => k}},
            PersistentChatService,
        ],
    });
    const service = TestBed.inject(PersistentChatService);
    return {service, mockHttp, mockApi, mockCache, sseInstances};
}

/** Connect + drive the session to ready so the outbox flushes immediately. */
async function readySession(threadId = 'thread-a') {
    const ctx = createService();
    await ctx.service.connect(threadId);
    fireSseOpen(ctx.sseInstances[0]);
    fireSseMessage(ctx.sseInstances[0], {method: 'ready', params: {}}, '1:1');
    ctx.mockHttp.post.mockClear();
    return ctx;
}

const inputPosts = (ctx: {mockHttp: any}) =>
    ctx.mockHttp.post.mock.calls.filter((c: any) => isInput(c[0]));

/** The body of the Nth POST /input (mock.calls entries are [url, body]). */
const inputBody = (ctx: {mockHttp: any}, n = 0) => inputPosts(ctx)[n][1];

function filePreview(name: string, id = `preview-${name}`): FilePreview {
    return {
        id,
        // Fixed `lastModified`: it is a third of the dedupe key
        // (name|size|lastModified), and Date.now() would make two previews of
        // the same file compare equal or not depending on the clock.
        file: new File(['x'], name, {lastModified: 111}),
        name,
        size: 1,
        sizeFormatted: '1 B',
        type: FileType.DOCUMENT,
        mimeType: 'application/pdf',
        uploadStatus: UploadStatus.PENDING,
    };
}

const uploaded = (name: string): ThreadUploadedFile => ({
    name,
    size: 1,
    mime_type: 'application/pdf',
    path: `uploads/${name}`,
});

/**
 * The terminal event of an upload. Since Slice 2 `uploadOneToThread` emits an
 * event union (progress*, then exactly one done) rather than a bare array, so
 * every mock here has to speak it — a mock still returning `of([])` would be
 * filtered out as a non-`done` event and read as an upload that never landed.
 */
const done = (...files: ThreadUploadedFile[]): ThreadUploadEvent => ({kind: 'done', files});

describe('PersistentChatService — Phase 3 outbox', () => {
    let originalEs: any;
    let originalWs: any;

    beforeEach(() => {
        originalEs = (globalThis as any).EventSource;
        originalWs = (globalThis as any).WebSocket;
    });

    afterEach(() => {
        (globalThis as any).EventSource = originalEs;
        (globalThis as any).WebSocket = originalWs;
        vi.clearAllMocks();
    });

    // --- 1. N sends while not ready ---------------------------------------

    it('queues N sends while not ready — N items, N bubbles, no overwrite', async () => {
        const ctx = createService();
        await ctx.service.connect('t1'); // no 'ready' frame → not ready
        await ctx.service.sendMessage('one');
        await ctx.service.sendMessage('two');
        await ctx.service.sendMessage('three');

        // `content` (agent-facing, hint included) is only computed when an
        // item reaches the POST stage; `displayContent` is the typed text and
        // exists from the moment the user hit send.
        expect(ctx.service.outbox().map((i) => i.displayContent)).toEqual(['one', 'two', 'three']);
        const bubbles = ctx.service.turns().filter(isUserTurn).map((t) => (t as UserTurn).content);
        expect(bubbles).toEqual(['one', 'two', 'three']);
        expect(inputPosts(ctx).length).toBe(0); // nothing POSTed while not ready
    });

    it('outboxItem(localId) looks up a queued item by its bubble id, including pendingFiles', async () => {
        const ctx = createService();
        await ctx.service.connect('t1'); // no 'ready' frame → not ready
        ctx.service.addAttachments([filePreview('a.pdf')]);
        await ctx.service.sendMessage('with a file');

        const localId = ctx.service.outbox()[0].localId;
        expect(ctx.service.outboxItem(localId)?.displayContent).toBe('with a file');
        expect(ctx.service.outboxItem(localId)?.pendingFiles?.map((f) => f.name)).toEqual(['a.pdf']);
        // Task 5's stage line reads this for an id that has already flushed or
        // never existed — must resolve to nothing, not throw.
        expect(ctx.service.outboxItem('no-such-id')).toBeUndefined();
    });

    // --- 2. ownership: disconnect keeps, thread-switch clears, carry re-adds -

    it('disconnect() preserves the outbox; connect(other) clears it', async () => {
        const ctx = createService();
        await ctx.service.connect('t1');
        await ctx.service.sendMessage('keep me');
        expect(ctx.service.outbox().length).toBe(1);

        ctx.service.disconnect();
        expect(ctx.service.outbox().length).toBe(1); // survives transport teardown

        await ctx.service.connect('t2'); // genuine thread switch
        expect(ctx.service.outbox().length).toBe(0);
    });

    it('connect(id, {carryOutbox}) re-dispatches queued bubbles after load_history', async () => {
        const ctx = createService();
        (ctx.service as any).outbox.set([
            {localId: 'user-c', displayContent: 'hi', threadId: 't-new', attempts: 0},
        ]);
        await ctx.service.connect('t-new', {carryOutbox: true});

        const bubbles = ctx.service.turns().filter(isUserTurn).map((t) => (t as UserTurn).content);
        expect(bubbles).toContain('hi');
        expect(ctx.service.outbox().length).toBe(1); // still queued (not ready)
    });

    // --- 3. FIFO, single POST in flight -----------------------------------

    it('flushes FIFO with exactly one POST in flight', async () => {
        const ctx = await readySession();
        const subjects: Subject<any>[] = [];
        ctx.mockHttp.post.mockImplementation((url: string) => {
            if (isInput(url)) {
                const s = new Subject<any>();
                subjects.push(s);
                return s;
            }
            return of({});
        });

        await ctx.service.sendMessage('a');
        await ctx.service.sendMessage('b');
        await ctx.service.sendMessage('c');

        expect(ctx.service.outbox().length).toBe(3);
        expect(subjects.length).toBe(1); // only the head is in flight

        subjects[0].next({});
        subjects[0].complete();
        await flushTick();
        expect(subjects.length).toBe(2);
        expect(ctx.service.outbox().map((i) => i.displayContent)).toEqual(['b', 'c']);

        subjects[1].next({});
        subjects[1].complete();
        await flushTick();
        subjects[2].next({});
        subjects[2].complete();
        await flushTick();
        expect(ctx.service.outbox().length).toBe(0);
    });

    // --- 4. flush outcomes: 409 / 503 / 404 -------------------------------

    it('keeps a distinct sibling-tab message queued on generic 409', async () => {
        const ctx = await readySession();
        ctx.mockHttp.post.mockReturnValue(throwError(() => ({status: 409})));
        // Tab A's already-running "alpha" is not proof that this tab's
        // distinct "beta" was accepted: /input has no client idempotency key.
        await ctx.service.sendMessage('beta');
        await flushTick();

        expect(ctx.service.outbox().map((item) => item.displayContent)).toEqual(['beta']);
        expect(ctx.service.outbox()[0].attempts).toBe(1);
        expect(ctx.service.outboxStalled()).toBe(true);
        expect(ctx.service.pendingTurnCount()).toBe(0);
        expect(ctx.service.error()).not.toBeNull();
        expect(ctx.service.turns().some((t) => isUserTurn(t) && t.content === 'beta')).toBe(true);
    });

    it.each([
        ['session_ending', 'ending'],
        ['session_ended', 'ended'],
    ] as const)('keeps the unsent item and applies typed %s lifecycle state', async (code, status) => {
        const ctx = await readySession();
        const es = ctx.sseInstances[0];
        ctx.mockHttp.post.mockReturnValue(
            throwError(() => ({
                status: 409,
                error: {
                    detail: {
                        code,
                        message: 'Session is retiring',
                        retirement_disposition: 'ended',
                    },
                },
            })),
        );

        await ctx.service.sendMessage('beta must remain visible');
        await flushTick();

        expect(ctx.service.threadStatus()).toBe(status);
        expect(ctx.service.sessionReady()).toBe(false);
        expect(ctx.service.outbox().map((item) => item.displayContent)).toEqual([
            'beta must remain visible',
        ]);
        expect(ctx.service.outboxStalled()).toBe(true);
        expect(ctx.service.pendingTurnCount()).toBe(0);
        expect(es.close).not.toHaveBeenCalled();
    });

    it('503 keeps item + bubble + banner, no timer retry; next send retriggers', async () => {
        const ctx = await readySession();
        ctx.mockHttp.post.mockReturnValue(throwError(() => ({status: 503, error: {detail: 'busy'}})));
        await ctx.service.sendMessage('later');
        await flushTick();

        expect(ctx.service.outbox().length).toBe(1);
        expect(ctx.service.turns().some((t) => isUserTurn(t) && t.content === 'later')).toBe(true);
        expect(ctx.service.error()).not.toBeNull();

        // No auto-retry: another tick issues no further POST on its own.
        const before = inputPosts(ctx).length;
        await flushTick();
        expect(inputPosts(ctx).length).toBe(before);

        // Next send retriggers the flush (POST succeeds now) → both drain FIFO.
        ctx.mockHttp.post.mockReturnValue(of({}));
        await ctx.service.sendMessage('again');
        await flushTick();
        expect(ctx.service.outbox().length).toBe(0);
    });

    it('404 drains the outbox and rolls back its bubbles', async () => {
        const ctx = await readySession();
        ctx.mockHttp.post.mockReturnValue(throwError(() => ({status: 404})));
        await ctx.service.sendMessage('gone');
        await flushTick();

        expect(ctx.service.outbox().length).toBe(0);
        expect(ctx.service.turns().some((t) => isUserTurn(t) && t.content === 'gone')).toBe(false);
    });

    // --- 5. thread switch mid-POST → resolution dropped -------------------

    it('drops a POST resolution after a thread switch (no cross-thread mutation)', async () => {
        const ctx = await readySession('t1');
        const s = new Subject<any>();
        ctx.mockHttp.post.mockImplementation((url: string) => (isInput(url) ? s : of({})));

        await ctx.service.sendMessage('from-t1'); // POST in flight (deferred)
        expect(ctx.service.outbox().length).toBe(1);

        await ctx.service.connect('t2'); // switch clears the outbox
        await ctx.service.sendMessage('from-t2'); // t2 not ready → queued
        const t2Len = ctx.service.outbox().length;
        expect(t2Len).toBe(1);

        // The stale t1 POST resolves now — must NOT touch t2's queue.
        s.next({});
        s.complete();
        await flushTick();
        expect(ctx.service.outbox().length).toBe(t2Len);
        expect(ctx.service.outbox()[0].displayContent).toBe('from-t2');
    });

    // --- 6. single-flight + horizon re-dispatch ---------------------------

    it('concurrent _flushOutbox calls stay single-flight', async () => {
        const ctx = await readySession();
        const subjects: Subject<any>[] = [];
        ctx.mockHttp.post.mockImplementation((url: string) => {
            if (isInput(url)) {
                const s = new Subject<any>();
                subjects.push(s);
                return s;
            }
            return of({});
        });
        (ctx.service as any).outbox.set([
            {localId: 'u1', displayContent: 'x', threadId: 'thread-a', attempts: 0},
        ]);

        (ctx.service as any)._flushOutbox();
        (ctx.service as any)._flushOutbox();

        expect(subjects.length).toBe(1); // second call is a no-op
    });

    it('horizon reload re-dispatches an unflushed bubble once, skipping the in-flight head', async () => {
        const ctx = await readySession('t1');
        const s = new Subject<any>();
        ctx.mockHttp.post.mockImplementation((url: string) => (isInput(url) ? s : of({})));

        await ctx.service.sendMessage('head'); // POST(head) deferred → in flight
        await ctx.service.sendMessage('second'); // queued behind
        expect(ctx.service.outbox().length).toBe(2);

        // gone_beyond_horizon → loadHistory (empty) then re-dispatch bubbles.
        fireSseNamedEvent(ctx.sseInstances[0], 'gone_beyond_horizon', {
            params: {epoch: 2, server_seq: 0},
        });
        await flushTick();
        await flushTick();

        const bubbles = ctx.service.turns().filter(isUserTurn).map((t) => (t as UserTurn).content);
        // 'second' re-dispatched exactly once; the in-flight 'head' skipped
        // (its row may already be in the reloaded history).
        expect(bubbles.filter((c) => c === 'second').length).toBe(1);
        expect(bubbles).not.toContain('head');
    });

    // --- 7. upload as outbox stage 0 --------------------------------------

    describe('upload as outbox stage 0', () => {
        it('dispatches the bubble and clears the composer before the upload resolves', async () => {
            const ctx = await readySession();
            const gate = new Subject<ThreadUploadEvent>();
            ctx.mockApi.uploadOneToThread = vi.fn().mockReturnValue(gate);

            ctx.service.addAttachments([filePreview('a.pdf')]);
            void ctx.service.sendMessage('look at this');
            // The commit block is synchronous, but sendMessage awaits the
            // office-frame flush gate (a local in-memory commit, not a
            // network call) one line above it — so drain the microtask queue
            // before asserting. The gate Subject is still un-emitted here, so
            // this proves the bubble does not wait on the UPLOAD, which is
            // the property under test.
            await flushTick();

            expect(
                ctx.service.turns().some((t) => isUserTurn(t) && t.content === 'look at this'),
            ).toBe(true);
            expect(ctx.service.pendingAttachments()).toEqual([]);
            expect(inputPosts(ctx).length).toBe(0);

            gate.next(done(uploaded('a.pdf')));
            gate.complete();
            await flushTick();

            expect(inputPosts(ctx).length).toBe(1);
            expect(inputBody(ctx).content).toBe(
                'look at this\n\n[Attached files in uploads/: a.pdf]',
            );
        });

        it('writes upload progress onto the queued file, throttled to ~4 writes a second', async () => {
            // Users attach 30-90MB PDFs; without this the bubble can only show
            // an opaque indeterminate label for the whole wait. The throttle is
            // not cosmetic: every write replaces the outbox array, which
            // re-renders the bubble and trips the chat view's scroll
            // ResizeObserver — and that observer re-pins to the bottom
            // SYNCHRONOUSLY by design, so an unthrottled progress signal
            // fights a user who scrolls up mid-upload.
            const ctx = await readySession();
            const gate = new Subject<ThreadUploadEvent>();
            ctx.mockApi.uploadOneToThread = vi.fn().mockReturnValue(gate);
            const t0 = 1_000_000;
            const now = vi.spyOn(Date, 'now').mockReturnValue(t0);

            ctx.service.addAttachments([filePreview('a.pdf')]);
            void ctx.service.sendMessage('big one');
            await flushTick();

            const loaded = () => ctx.service.outbox()[0].pendingFiles![0].loaded;

            // Leading edge: the first event of a burst lands immediately, so
            // the bar never sits at 0 waiting out an interval.
            gate.next({kind: 'progress', loaded: 10, total: 100});
            expect(loaded()).toBe(10);

            // Same 250ms window — dropped, even though the bytes did move.
            now.mockReturnValue(t0 + 100);
            gate.next({kind: 'progress', loaded: 20, total: 100});
            now.mockReturnValue(t0 + 249);
            gate.next({kind: 'progress', loaded: 30, total: 100});
            expect(loaded()).toBe(10);

            // Window elapsed — the next one goes through.
            now.mockReturnValue(t0 + 250);
            gate.next({kind: 'progress', loaded: 40, total: 100});
            expect(loaded()).toBe(40);
            expect(ctx.service.outbox()[0].pendingFiles![0].total).toBe(100);

            // A dropped trailing event is invisible: `done` writes the
            // terminal state, not the last progress event.
            now.mockReturnValue(t0 + 260);
            gate.next({kind: 'progress', loaded: 100, total: 100});
            gate.next(done(uploaded('a.pdf')));
            gate.complete();
            await flushTick();

            expect(ctx.service.outbox().length).toBe(0);
            expect(inputPosts(ctx).length).toBe(1);
            now.mockRestore();
        });

        it('drops a progress event whose item has left the queue', async () => {
            // Same guard the `done` path gets: writing into a queue the user
            // has navigated away from is the cross-thread mutation this phase
            // exists to kill, and progress fires far more often than `done`.
            const ctx = await readySession('t1');
            const gate = new Subject<ThreadUploadEvent>();
            ctx.mockApi.uploadOneToThread = vi.fn().mockReturnValue(gate);

            ctx.service.addAttachments([filePreview('a.pdf')]);
            void ctx.service.sendMessage('from-t1');
            await flushTick();

            await ctx.service.connect('t2'); // switch clears t1's queue
            await ctx.service.sendMessage('from-t2'); // t2 not ready → queued
            const queueBefore = ctx.service.outbox();

            gate.next({kind: 'progress', loaded: 10, total: 100});

            // Array identity is the observable difference an unguarded write
            // makes: its .map matches nothing, so the contents are equal but
            // the reference (and every downstream computed) changes.
            expect(ctx.service.outbox()).toBe(queueBefore);
        });

        it('patches the server path onto the bubble chip once the upload lands', async () => {
            const ctx = await readySession();
            // The server renamed it (a `_1` collision suffix): the chip must
            // show what actually landed, in place — not a second bubble.
            ctx.mockApi.uploadOneToThread = vi.fn().mockReturnValue(of(done(uploaded('a_1.pdf'))));

            ctx.service.addAttachments([filePreview('a.pdf')]);
            await ctx.service.sendMessage('here');
            await flushTick();

            const bubbles = ctx.service.turns().filter(isUserTurn);
            expect(bubbles.length).toBe(1);
            expect(bubbles[0].attachments).toEqual([
                {
                    id: 'preview-a.pdf',
                    name: 'a_1.pdf',
                    size: 1,
                    mimeType: 'application/pdf',
                    path: 'uploads/a_1.pdf',
                },
            ]);
        });

        it('keeps the bubble and stalls the queue when the upload fails', async () => {
            const ctx = await readySession();
            ctx.mockApi.uploadOneToThread = vi
                .fn()
                .mockReturnValue(throwError(() => new HttpErrorResponse({status: 503})));

            ctx.service.addAttachments([filePreview('a.pdf')]);
            await ctx.service.sendMessage('hi');
            await flushTick();

            expect(ctx.service.turns().some((t) => isUserTurn(t) && t.content === 'hi')).toBe(true);
            expect(ctx.service.outbox().length).toBe(1);
            expect(ctx.service.outboxStalled()).toBe(true);
            expect(inputPosts(ctx).length).toBe(0);
        });

        it('leaves a file behind a failure queued, not failed with the other file\'s error', async () => {
            const ctx = await readySession();
            ctx.mockApi.uploadOneToThread = vi
                .fn()
                .mockReturnValue(throwError(() => new HttpErrorResponse({status: 503})));

            ctx.service.addAttachments([filePreview('a.pdf'), filePreview('b.pdf')]);
            await ctx.service.sendMessage('two files');
            await flushTick();

            const files = ctx.service.outbox()[0].pendingFiles!;
            expect(files.map((f) => f.status)).toEqual(['failed', 'queued']);
            expect(files[1].error).toBeUndefined();
        });

        it('does not re-upload a file that already succeeded when the queue is retried', async () => {
            const ctx = await readySession();
            const upload = vi
                .fn()
                .mockReturnValueOnce(of(done(uploaded('a.pdf'))))
                .mockReturnValue(throwError(() => new HttpErrorResponse({status: 503})));
            ctx.mockApi.uploadOneToThread = upload;
            /** How many requests carried this file, across eager and deferred. */
            const sends = (name: string) =>
                upload.mock.calls.filter((c: any) => (c[1] as File).name === name).length;

            // Attaching starts both uploads eagerly (§5.4): a.pdf lands, b.pdf
            // fails. b's failure leaves no trace, so the send falls back to the
            // deferred path for it — and fails again.
            ctx.service.addAttachments([filePreview('a.pdf'), filePreview('b.pdf')]);
            await ctx.service.sendMessage('two files');
            await flushTick();
            expect(ctx.service.outboxStalled()).toBe(true);
            expect(sends('a.pdf')).toBe(1); // adopted, not re-sent
            expect(sends('b.pdf')).toBe(2); // eager attempt + deferred attempt

            upload.mockReturnValue(of(done(uploaded('b.pdf'))));
            ctx.service.retryQueuedSends();
            await flushTick();

            // The invariant: a.pdf crossed the wire exactly once for the whole
            // life of the send. The backend has no idempotency — a re-upload
            // would land as a_1.pdf and nothing but the delete endpoint could
            // take it back.
            expect(sends('a.pdf')).toBe(1);
            expect(sends('b.pdf')).toBe(3);
            expect(inputBody(ctx).content).toContain('a.pdf, b.pdf');
        });

        it('drops the resolution when the thread changed mid-upload', async () => {
            // Mirrors the POST-path test: the foreign queue is POPULATED, so
            // "the guard dropped it" is distinguishable from "connect emptied
            // the queue", and t2's own item must survive untouched.
            const ctx = await readySession('t1');
            const gate = new Subject<ThreadUploadEvent>();
            ctx.mockApi.uploadOneToThread = vi.fn().mockReturnValue(gate);

            ctx.service.addAttachments([filePreview('a.pdf')]);
            void ctx.service.sendMessage('from-t1');
            await flushTick();

            await ctx.service.connect('t2'); // switch clears t1's queue
            await ctx.service.sendMessage('from-t2'); // t2 not ready → queued
            expect(ctx.service.outbox().length).toBe(1);

            // The stale t1 upload resolves now — it must not POST, and must not
            // touch t2's queue.
            gate.next(done(uploaded('a.pdf')));
            gate.complete();
            await flushTick();

            expect(inputPosts(ctx).length).toBe(0);
            expect(ctx.service.outbox().length).toBe(1);
            expect(ctx.service.outbox()[0].displayContent).toBe('from-t2');
            expect(ctx.service.outbox()[0].pendingFiles).toBeUndefined();
        });

        it('stays single-flight WITHIN a thread while an upload is in flight', async () => {
            // The safety half of the per-thread lock: a second send on the same
            // thread must not overtake the stalled head. Both would collide on
            // the default turn_id and the second would be lost behind a 409.
            const ctx = await readySession('t1');
            const gate = new Subject<ThreadUploadEvent>();
            ctx.mockApi.uploadOneToThread = vi.fn().mockReturnValue(gate);

            ctx.service.addAttachments([filePreview('a.pdf')]);
            void ctx.service.sendMessage('with a file');
            await flushTick();

            await ctx.service.sendMessage('right behind it');
            await flushTick();

            expect(inputPosts(ctx).length).toBe(0);
            expect(ctx.service.outbox().map((i) => i.displayContent)).toEqual([
                'with a file',
                'right behind it',
            ]);

            // Upload lands → the queue drains FIFO, head first.
            gate.next(done(uploaded('a.pdf')));
            gate.complete();
            await flushTick();

            expect(inputPosts(ctx).map((c: any) => c[1].content)).toEqual([
                'with a file\n\n[Attached files in uploads/: a.pdf]',
                'right behind it',
            ]);
        });

        it('a thread switch mid-upload does not stall the new thread\'s queue', async () => {
            // The upload runs INSIDE the flush's single-flight lock and is not
            // cancelled on a thread switch (Slice 3 owns cancellation). With a
            // tab-wide lock the newly-opened thread could not POST at all until
            // the abandoned upload finished — an unbounded silent stall. The
            // lock is per-thread: `turn_id` is per-thread, so two threads can
            // never collide on it.
            const ctx = await readySession('t1');
            const gate = new Subject<ThreadUploadEvent>(); // never resolves
            ctx.mockApi.uploadOneToThread = vi.fn().mockReturnValue(gate);

            ctx.service.addAttachments([filePreview('a.pdf')]);
            void ctx.service.sendMessage('from-t1');
            await flushTick();
            expect(ctx.mockApi.uploadOneToThread).toHaveBeenCalledTimes(1);

            await ctx.service.connect('t2');
            fireSseOpen(ctx.sseInstances[1]);
            fireSseMessage(ctx.sseInstances[1], {method: 'ready', params: {}}, '1:1');
            await ctx.service.sendMessage('from-t2');
            await flushTick();

            expect(inputPosts(ctx).length).toBe(1);
            expect(inputBody(ctx).content).toBe('from-t2');
            expect(ctx.service.outbox().length).toBe(0);
        });

        it('a stale thread\'s upload resolution does not un-refuse a discard on the live thread', async () => {
            // Per-thread locking makes two flushes coexist by design. With
            // scalar in-flight markers, whichever finishes first cleared the
            // other's: t1's abandoned upload resolving would un-refuse a
            // discard of t2's item while ITS bytes are still on the wire, and
            // the dropped item's upload then orphans in t2's workspace — no
            // delete endpoint, no way back.
            const ctx = await readySession('t1');
            const gate1 = new Subject<ThreadUploadEvent>();
            const gate2 = new Subject<ThreadUploadEvent>();
            ctx.mockApi.uploadOneToThread = vi
                .fn()
                .mockReturnValueOnce(gate1)
                .mockReturnValueOnce(gate2);

            ctx.service.addAttachments([filePreview('a.pdf')]);
            void ctx.service.sendMessage('from-t1');
            await flushTick();

            await ctx.service.connect('t2');
            fireSseOpen(ctx.sseInstances[1]);
            fireSseMessage(ctx.sseInstances[1], {method: 'ready', params: {}}, '1:1');
            ctx.service.addAttachments([filePreview('b.pdf')]);
            void ctx.service.sendMessage('from-t2');
            await flushTick();
            expect(ctx.mockApi.uploadOneToThread).toHaveBeenCalledTimes(2);

            const queueBefore = ctx.service.outbox();
            const t2LocalId = queueBefore[0].localId;

            gate1.next(done(uploaded('a.pdf'))); // t1's abandoned upload lands
            gate1.complete();
            await flushTick();

            // It must not write to t2's queue at all — array identity is the
            // observable difference an unguarded _patchPendingFile makes (its
            // .map matches nothing, so the contents are equal but the reference
            // and every downstream computed change).
            expect(ctx.service.outbox()).toBe(queueBefore);

            ctx.service.discardQueuedSend(t2LocalId);
            expect(ctx.service.outbox().length).toBe(1);
        });

        it('does not paint a stale thread\'s terminal upload error on the thread now open', async () => {
            // The banner is set inside _uploadStage, which runs BEFORE the
            // flush's post-stage thread guard — so a 413 from a thread the user
            // already left would otherwise appear over the one they just
            // opened, attached to nothing they can see.
            const ctx = await readySession('t1');
            const gate = new Subject<ThreadUploadEvent>();
            ctx.mockApi.uploadOneToThread = vi.fn().mockReturnValue(gate);

            ctx.service.addAttachments([filePreview('a.pdf')]);
            void ctx.service.sendMessage('from-t1');
            await flushTick();

            await ctx.service.connect('t2');
            // This suite's deliberately minimal GET double predates the
            // durable /state payload and therefore leaves an unrelated state
            // hydration banner. Establish a clean live-thread baseline before
            // the old thread's upload fails.
            ctx.service.error.set(null);
            expect(ctx.service.error()).toBeNull();

            gate.error(new HttpErrorResponse({status: 413})); // terminal
            await flushTick();

            expect(ctx.service.error()).toBeNull();
        });

        it('a stale thread\'s POST resolution does not un-refuse a discard on the live thread', async () => {
            // Same shape without attachments, for the POST marker.
            const ctx = await readySession('t1');
            const posts: Subject<any>[] = [];
            ctx.mockHttp.post.mockImplementation((url: string) => {
                if (isInput(url)) {
                    const s = new Subject<any>();
                    posts.push(s);
                    return s;
                }
                return of({});
            });

            await ctx.service.sendMessage('from-t1'); // POST 1 in flight
            await ctx.service.connect('t2');
            fireSseOpen(ctx.sseInstances[1]);
            fireSseMessage(ctx.sseInstances[1], {method: 'ready', params: {}}, '1:1');
            await ctx.service.sendMessage('from-t2'); // POST 2 in flight
            await flushTick();
            expect(posts.length).toBe(2);

            const t2LocalId = ctx.service.outbox()[0].localId;
            posts[0].next({}); // t1's stale POST resolves
            posts[0].complete();
            await flushTick();

            // t2's POST is still on the wire: discarding it would remove a
            // bubble whose message the agent is about to accept.
            ctx.service.discardQueuedSend(t2LocalId);
            expect(ctx.service.outbox().length).toBe(1);
        });

        it('refuses to discard an item whose upload is in flight', async () => {
            const ctx = await readySession();
            ctx.mockApi.uploadOneToThread = vi.fn().mockReturnValue(new Subject<ThreadUploadEvent>());

            ctx.service.addAttachments([filePreview('a.pdf')]);
            void ctx.service.sendMessage('hi');
            await flushTick();

            const localId = ctx.service.outbox()[0].localId;
            ctx.service.discardQueuedSend(localId);
            expect(ctx.service.outbox().length).toBe(1);
        });

        it('does not POST an item that left the queue while its upload ran (A→B→A)', async () => {
            // The flush's post-stage guard compares thread IDENTITY, so the
            // A→B→A round trip defeats it: by the time the upload resolves the
            // ids match again and the guard waves it through — but connect(B)
            // emptied the outbox on the way past, so the item being POSTed no
            // longer exists. Re-reading it and refusing is what closes this.
            // The upload stage widened the window from the ~30s POST timeout to
            // the length of a whole transfer.
            const ctx = await readySession('t1');
            const gate = new Subject<ThreadUploadEvent>();
            ctx.mockApi.uploadOneToThread = vi.fn().mockReturnValue(gate);

            ctx.service.addAttachments([filePreview('a.pdf')]);
            void ctx.service.sendMessage('stale text');
            await flushTick();
            expect(inputPosts(ctx).length).toBe(0); // still in stage 0

            await ctx.service.connect('t2'); // clears t1's outbox
            await ctx.service.connect('t1'); // ids match again

            gate.next(done(uploaded('a.pdf')));
            gate.complete();
            await flushTick();

            expect(ctx.service.outbox()).toEqual([]);
            expect(inputPosts(ctx).length).toBe(0);
        });

        it('a horizon reload KEEPS a mid-upload bubble — only the POST set may skip', async () => {
            // The postingLocalIds / uploadingLocalIds asymmetry, pinned.
            //
            // `_redispatchOutboxBubbles(skipInFlight)` consults ONLY the POST
            // set. A mid-upload item has never been POSTed, so it cannot be in
            // the reloaded history, and skipping it would delete its bubble
            // outright — the user's message vanishing mid-transfer with the
            // outbox still holding it. `discardQueuedSend` legitimately checks
            // both sets, so the two look interchangeable at a glance and a
            // future "tidy-up" that merges them would go green everywhere else.
            const ctx = await readySession('t1');
            ctx.mockApi.uploadOneToThread = vi.fn().mockReturnValue(new Subject<ThreadUploadEvent>());

            ctx.service.addAttachments([filePreview('a.pdf')]);
            void ctx.service.sendMessage('with a file');
            await flushTick();

            // Precondition, so this can never pass vacuously on an upload that
            // already finished: uploading yes, posting no.
            const localId = ctx.service.outbox()[0].localId;
            expect((ctx.service as any).uploadingLocalIds.has(localId)).toBe(true);
            expect((ctx.service as any).postingLocalIds.has(localId)).toBe(false);

            fireSseNamedEvent(ctx.sseInstances[0], 'gone_beyond_horizon', {
                params: {epoch: 2, server_seq: 0},
            });
            await flushTick();
            await flushTick();

            const bubbles = ctx.service.turns().filter(isUserTurn).map((t) => (t as UserTurn).content);
            expect(bubbles.filter((c) => c === 'with a file').length).toBe(1);
        });
    });

    // --- 8. eager upload on attach (§5.4) ---------------------------------

    describe('eager upload on attach', () => {
        /** An upload that can be observed and ABORTED — the mock backends used
         *  above cannot express an abort, and the abort is the property under
         *  test for a cancel. */
        function abortableUpload() {
            const requests: {file: File; aborted: boolean; settle: () => void}[] = [];
            const fn = vi.fn(
                (_threadId: string, file: File) =>
                    new Observable<ThreadUploadEvent>((sub) => {
                        let settled = false;
                        const rec = {
                            file,
                            aborted: false,
                            settle: () => {
                                settled = true;
                                sub.next(done(uploaded(file.name)));
                                sub.complete();
                            },
                        };
                        requests.push(rec);
                        return () => {
                            rec.aborted = !settled;
                        };
                    }),
            );
            return {fn, requests};
        }

        it('uploads the moment a file is attached to a live thread', async () => {
            // Most users attach first and type second. Starting here is what
            // makes the send instant once they finally hit Enter.
            const ctx = await readySession();
            ctx.mockApi.uploadOneToThread = vi.fn().mockReturnValue(new Subject<ThreadUploadEvent>());

            ctx.service.addAttachments([filePreview('a.pdf')]);

            expect(ctx.mockApi.uploadOneToThread).toHaveBeenCalledTimes(1);
            expect(ctx.mockApi.uploadOneToThread.mock.calls[0][0]).toBe('thread-a');
        });

        it('waits for the flush when the session is not ready yet', async () => {
            // The workspace is provisioned lazily and 409s until it is up.
            const ctx = createService();
            await ctx.service.connect('t1'); // no 'ready' frame
            ctx.service.addAttachments([filePreview('a.pdf')]);

            expect(ctx.mockApi.uploadOneToThread).not.toHaveBeenCalled();
        });

        it('waits for the flush when the workspace tier is none', async () => {
            // Permanent refusal, not a wait — but the message can still be sent
            // without the files, so the deferred path owns the error.
            const ctx = await readySession();
            ctx.service.workspaceTier.set('none');

            ctx.service.addAttachments([filePreview('a.pdf')]);

            expect(ctx.mockApi.uploadOneToThread).not.toHaveBeenCalled();
        });

        it('adopts the eager upload at send time instead of starting a second one', async () => {
            const ctx = await readySession();
            const {fn, requests} = abortableUpload();
            ctx.mockApi.uploadOneToThread = fn;

            ctx.service.addAttachments([filePreview('a.pdf')]);
            expect(requests).toHaveLength(1);
            requests[0].settle(); // lands while the user is still typing

            await ctx.service.sendMessage('look at this');
            await flushTick();

            // One request for the whole life of the file. Re-uploading a
            // success is a permanent duplicate.
            expect(requests).toHaveLength(1);
            expect(inputBody(ctx).content).toBe(
                'look at this\n\n[Attached files in uploads/: a.pdf]',
            );
        });

        it('removing the chip aborts an eager upload that is still transferring', async () => {
            const ctx = await readySession();
            const {fn, requests} = abortableUpload();
            ctx.mockApi.uploadOneToThread = fn;

            ctx.service.addAttachments([filePreview('a.pdf')]);
            ctx.service.removeAttachment('preview-a.pdf');

            expect(requests[0].aborted).toBe(true);
            expect(ctx.service.pendingAttachments()).toEqual([]);
            // Not an error: a cancel is intent, not a failure, and must never
            // reach humanizeUploadError ("Network error — check your
            // connection" for an abort is the service-worker incident's lie).
            expect(ctx.service.error()).toBeNull();
            expect(ctx.service.attachmentError()).toBeNull();
            expect(ctx.mockApi.humanizeUploadError).not.toHaveBeenCalled();
        });

        it('removing the chip DELETES an eager upload that already landed', async () => {
            const ctx = await readySession();
            const {fn, requests} = abortableUpload();
            ctx.mockApi.uploadOneToThread = fn;

            ctx.service.addAttachments([filePreview('a.pdf')]);
            requests[0].settle();
            ctx.service.removeAttachment('preview-a.pdf');
            await flushTick();

            expect(ctx.mockApi.deleteThreadUpload).toHaveBeenCalledWith('thread-a', 'a.pdf');
        });

        it('a thread switch clears the chips, the banner and the eager uploads', async () => {
            // Before eager upload the chips merely followed the user between
            // threads (nothing cleared them). With it, that would push bytes
            // into a workspace they had already left.
            const ctx = await readySession('t1');
            const {fn, requests} = abortableUpload();
            ctx.mockApi.uploadOneToThread = fn;

            ctx.service.addAttachments([filePreview('a.pdf')]);
            ctx.service.attachmentError.set('some earlier complaint');

            await ctx.service.connect('t2');

            expect(ctx.service.pendingAttachments()).toEqual([]);
            expect(ctx.service.attachmentError()).toBeNull();
            expect(requests[0].aborted).toBe(true);
        });

        it('a reconnect to the SAME thread keeps the chips and the upload', async () => {
            // The cold path also runs for the current thread when history has
            // not loaded — a failed connect the user retries. Treating that as
            // a thread switch would take their staged attachments away AND
            // delete the bytes, over a transient reconnect. Losing staged work
            // is worse than the leak the clear exists to close.
            const ctx = await readySession('t1');
            const {fn, requests} = abortableUpload();
            ctx.mockApi.uploadOneToThread = fn;

            ctx.service.addAttachments([filePreview('a.pdf')]);
            requests[0].settle(); // it even landed — a discard would DELETE it
            ctx.service.historyLoaded.set(false); // the failed-load state

            await ctx.service.connect('t1'); // same thread, cold path

            expect(ctx.service.pendingAttachments().map((p) => p.name)).toEqual(['a.pdf']);
            expect(ctx.mockApi.deleteThreadUpload).not.toHaveBeenCalled();
        });

        it('entering the landing draft clears the chips and the eager uploads', async () => {
            const ctx = await readySession('t1');
            const {fn, requests} = abortableUpload();
            ctx.mockApi.uploadOneToThread = fn;

            ctx.service.addAttachments([filePreview('a.pdf')]);
            ctx.service.enterDraftSession();

            expect(ctx.service.pendingAttachments()).toEqual([]);
            expect(requests[0].aborted).toBe(true);
        });

        it('refuses a re-add of the same name|size|lastModified', async () => {
            // Uppy's key. The backend has no upload idempotency, so a second
            // chip for one file becomes a_1.pdf next to a.pdf, with the message
            // hint naming both.
            const ctx = await readySession();
            ctx.mockApi.uploadOneToThread = vi.fn().mockReturnValue(new Subject<ThreadUploadEvent>());

            ctx.service.addAttachments([filePreview('a.pdf', 'first')]);
            ctx.service.addAttachments([filePreview('a.pdf', 'second')]);

            expect(ctx.service.pendingAttachments().map((p) => p.id)).toEqual(['first']);
            expect(ctx.service.attachmentError()).toBe('chat.upload.duplicateFile');
            expect(ctx.mockApi.uploadOneToThread).toHaveBeenCalledTimes(1);
        });

        it('keeps the non-duplicate half of a mixed selection', async () => {
            const ctx = await readySession();
            ctx.mockApi.uploadOneToThread = vi.fn().mockReturnValue(new Subject<ThreadUploadEvent>());

            ctx.service.addAttachments([filePreview('a.pdf', 'first')]);
            ctx.service.addAttachments([
                filePreview('a.pdf', 'dupe'),
                filePreview('b.pdf', 'fresh'),
            ]);

            expect(ctx.service.pendingAttachments().map((p) => p.id)).toEqual(['first', 'fresh']);
            expect(ctx.service.attachmentError()).toBe('chat.upload.duplicateFile');
        });
    });
});
