/**
 * The "Starting session" panel vs. a first turn that already finished.
 *
 * knowledge-base/knowledge/issues/session_start_panel_never_yields_to_a_completed_first_turn.md
 *
 * `sessionReady` (which `isStartingSession` — and therefore the startup card —
 * is gated on) is flipped only by *live* signals: the agent's `session.state`
 * welcome frame, an SSE `ready` frame the snapshot cursor did not already
 * cover, or `/connection` resolving. On a queue-served (socketless) session
 * that last path additionally requires the durable `/state` read of the same
 * connect to have succeeded, so two independent live facts have to line up.
 * When they don't, the panel outlives a session that has already answered:
 * the reply is durable, the run_queue unit is `done`, and the browser still
 * says "Provisioning agent" until the user reloads.
 *
 * These specs drive the interleavings that lose that race — a create slow
 * enough that the turn completes before the client's startup sequence
 * resolves, and startup lifecycle signals that never arrive or arrive out of
 * order — and assert the user-visible property: the panel yields and the
 * reply is on screen, with no reload.
 */
import {afterEach, beforeEach, describe, expect, it, vi} from 'vitest';
import {signal} from '@angular/core';
import {TestBed} from '@angular/core/testing';
import {HttpClient} from '@angular/common/http';
import {of, Subject, throwError} from 'rxjs';
import {TranslocoService} from '@jsverse/transloco';
import {PersistentChatService, QUEUE_POLL_MS} from './persistent-chat.service';
import {ApiService} from './api.service';
import {CapabilitiesService} from './capabilities.service';
import {IndexedDbService} from './indexed-db.service';
import {NotificationService} from './notification.service';
import {AppToastService} from '../../ui/toast';
import {isAssistantTurn} from '../models/turn.model';

interface MockEventSource {
    url: string;
    readyState: number;
    close: ReturnType<typeof vi.fn>;
    onopen: ((e: any) => void) | null;
    onmessage: ((e: MessageEvent) => void) | null;
    onerror: ((e: any) => void) | null;
    listeners: Record<string, ((e: any) => void)[]>;
}

/** Let queued microtasks + a macrotask boundary drain. */
const flushTick = () => new Promise((r) => setTimeout(r, 0));
/** Several await points chain inside connect(); give them room to settle. */
async function settle(times = 6): Promise<void> {
    for (let i = 0; i < times; i++) await flushTick();
}

const THREAD_ID = 't-slow-create';
const USER_TEXT = 'first message through the live application';
const REPLY_TEXT = 'E2E_REPLY:48ce31b2';

const isThreadsCreate = (url: unknown) => String(url).endsWith('/persistent/threads');
const isInput = (url: unknown) => String(url).endsWith('/input');
const isState = (url: unknown) => String(url).endsWith('/state');
const isMessages = (url: unknown) => String(url).includes('/messages');
const isConnection = (url: unknown) => String(url).endsWith('/connection');
const isThreadMeta = (url: unknown) => /\/persistent\/threads\/[^/?]+$/.test(String(url));

/** The durable transcript once the queued first turn has run. */
function answeredTranscript() {
    return [
        {
            id: 'm1',
            role: 'human',
            content: USER_TEXT,
            tool_calls: null,
            turn_number: 1,
            created_at: '2026-09-08T22:52:49.943Z',
        },
        {
            id: 'm2',
            role: 'ai',
            content: REPLY_TEXT,
            tool_calls: null,
            turn_number: 1,
            created_at: '2026-09-08T22:53:03.476Z',
        },
    ];
}

function durableSnapshot() {
    return {
        thread_id: THREAD_ID,
        permission_mode: 'default',
        narration_mode: 'normal',
        turn_count: 1,
        turn_in_flight: false,
        message_count: 2,
        model: 'test-model',
        temperature: 0,
        running_tool: null,
        pending_permissions: [],
        tasks: [],
        usage: null,
        event_cursor: {epoch: 1, seq: 12},
        replay_cursor: {epoch: 1, seq: 0},
        snapshot_source: 'durable_journal',
    };
}

/** `GET /api/sessions/{id}/connection` on the queue-served lane. */
function statelessConnection(queue: Record<string, unknown> | null) {
    return {
        state: 'ready',
        control_socket: 'none',
        ws_url: null,
        token: null,
        expires_at: null,
        pinned_runtime_generation_contract: 1,
        session_runtime_generation: 'g1',
        controls: {
            'config.update': 'rest',
            'workspace.undo': 'rest',
            'mode.set': 'rest',
            'narration.set': 'rest',
        },
        queue,
    };
}

/** `queue_block()` shapes, straight off the orchestrator contract. */
const QUEUE_NONE = {
    state: 'none',
    park_reason: null,
    parked_at: null,
    retryable: false,
    attempts: 0,
    pending_input: false,
    cloud_push: null,
};
const QUEUE_QUEUED = {...QUEUE_NONE, state: 'queued', attempts: 1, pending_input: true};
const QUEUE_DONE = {...QUEUE_NONE, state: 'done', attempts: 1, pending_input: false};

/**
 * A mutable server. Every route reads the current `server` object, so a test
 * can move the backend forward (turn completes, unit closes) between the
 * client's own steps and reproduce a real interleaving rather than a fixture.
 */
function createHarness() {
    const server = {
        /** Durable transcript rows `GET …/messages` returns. */
        messages: [] as ReturnType<typeof answeredTranscript>,
        /** `queue` block riding `/connection` and `GET …/queue`. */
        queue: QUEUE_NONE as Record<string, unknown>,
        /** Make the durable `/state` read fail (a 503, a cursor-contract violation). */
        stateFails: false,
        /** Hold `/connection` open — what a not-yet-ready protected-cloud
         *  runtime does to the cockpit's readiness poll. */
        connectionPending: false,
    };

    const createSubject = new Subject<{thread_id: string}>();
    const connectionSubject = new Subject<any>();
    let inputPosts = 0;

    const mockHttp: any = {
        get: vi.fn().mockImplementation((url: string) => {
            if (isState(url)) {
                return server.stateFails
                    ? throwError(() => ({status: 503}))
                    : of(durableSnapshot());
            }
            if (isMessages(url)) {
                return of({messages: server.messages, total: server.messages.length});
            }
            if (isConnection(url)) {
                return server.connectionPending
                    ? connectionSubject.asObservable()
                    : of(statelessConnection(server.queue));
            }
            if (String(url).includes('/citations')) return of({citations: []});
            if (String(url).includes('/projects')) return of([]);
            if (isThreadMeta(url)) {
                return of({
                    id: THREAD_ID,
                    status: 'active',
                    total_turns: server.messages.length ? 1 : 0,
                    title: 'slow create',
                    metadata: {},
                    mounts: [],
                });
            }
            return of({});
        }),
        post: vi.fn().mockImplementation((url: string) => {
            if (url.endsWith('/persistent/threads/preview')) return of({
                project_ids: [], workspace_backend: 'virtual', datasource_ids: [],
            });
            if (isThreadsCreate(url)) return createSubject.asObservable();
            if (isInput(url)) {
                inputPosts += 1;
                return of({accepted: true, turn_id: 1, queue: QUEUE_QUEUED});
            }
            return of({});
        }),
        patch: vi.fn().mockReturnValue(of({})),
        delete: vi.fn().mockReturnValue(of({})),
    };

    const mockApi: any = {
        uploadOneToThread: vi.fn().mockReturnValue(of({kind: 'done', files: []})),
        deleteThreadUpload: vi.fn().mockReturnValue(of(undefined)),
        humanizeUploadError: vi.fn().mockReturnValue('upload failed'),
        getEligibleDatasources: vi.fn().mockReturnValue(of([])),
        getThreadQueue: vi.fn().mockImplementation(() => of(server.queue)),
        retryThreadQueue: vi.fn().mockReturnValue(of({kind: 'ok', state: 'queued'})),
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
        lifecycleEvent: signal<Record<string, unknown> | null>(null),
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

    return {
        service,
        server,
        mockApi,
        mockNotifications,
        sseInstances,
        inputPostCount: () => inputPosts,
        /** Resolve the deliberately slow `POST /persistent/threads`. */
        resolveCreate: () => {
            createSubject.next({thread_id: THREAD_ID});
            createSubject.complete();
        },
        /** Release a `/connection` that was held open. */
        resolveConnection: () => {
            server.connectionPending = false;
            connectionSubject.next(statelessConnection(server.queue));
            connectionSubject.complete();
        },
        /** The queued first turn ran to completion, server-side. */
        completeFirstTurnServerSide: () => {
            server.messages = answeredTranscript();
            server.queue = QUEUE_DONE;
        },
    };
}

function assistantTexts(service: PersistentChatService): string[] {
    return service
        .turns()
        .filter(isAssistantTurn)
        .flatMap((t) => t.events.filter((e) => e.kind === 'text').map((e) => e.content));
}

describe('PersistentChatService — the start panel and a completed first turn', () => {
    let originalEs: any;
    let originalWs: any;

    beforeEach(() => {
        originalEs = (globalThis as any).EventSource;
        originalWs = (globalThis as any).WebSocket;
    });

    afterEach(() => {
        (globalThis as any).EventSource = originalEs;
        (globalThis as any).WebSocket = originalWs;
        vi.useRealTimers();
        vi.clearAllMocks();
    });

    it('yields once the turn is done, even when every live readiness signal is missed', async () => {
        const ctx = createHarness();

        // The landing draft: nothing exists server-side yet.
        ctx.service.enterDraftSession();
        await flushTick();
        await ctx.service.sendMessage(USER_TEXT);
        await flushTick();

        // A live cloud backend makes `POST /persistent/threads` take ~13s. The
        // card is legitimately up for that whole stretch.
        expect(ctx.service.isStartingSession()).toBe(true);
        expect(ctx.service.threadId()).toBeNull();

        // The turn the queue accepted runs and finishes while the client is
        // still working through its own startup sequence: the thread has both
        // messages and a `done` unit by the time the client looks.
        ctx.completeFirstTurnServerSide();
        // ...and the one live fact `/connection`'s readiness is conditioned on
        // (the durable `/state` read of this same connect) is the one that
        // fails. No lifecycle event, no `ready` frame, no `session.state`.
        ctx.server.stateFails = true;

        ctx.resolveCreate();
        await settle();

        expect(ctx.service.threadId()).toBe(THREAD_ID);
        expect(ctx.service.queueState()?.state).not.toBe('none');
        // The user-visible property: the panel is gone and the reply is here.
        expect(ctx.service.sessionReady()).toBe(true);
        expect(ctx.service.isStartingSession()).toBe(false);
        expect(assistantTexts(ctx.service)).toContain(REPLY_TEXT);
    });

    it('yields from the durable queue read when /connection never resolves', async () => {
        vi.useFakeTimers();
        const ctx = createHarness();

        // Reconnect (a route landing, a second connect, a reload) to a thread
        // whose first turn already ran. `/connection` is held open — what a
        // protected-cloud runtime that is not ready yet does to the readiness
        // poll — so the provisioning/ready signals never arrive at all.
        ctx.completeFirstTurnServerSide();
        ctx.server.connectionPending = true;
        void ctx.service.connect(THREAD_ID);
        await vi.advanceTimersByTimeAsync(50);

        // History is on screen, the panel is still up: nothing has told this
        // tab the session is admissible.
        expect(assistantTexts(ctx.service)).toContain(REPLY_TEXT);

        // The durable queue read is the only remaining source of truth.
        await vi.advanceTimersByTimeAsync(QUEUE_POLL_MS + 50);

        expect(ctx.mockApi.getThreadQueue).toHaveBeenCalledWith(THREAD_ID);
        expect(ctx.service.queueState()?.state).toBe('done');
        expect(ctx.service.sessionReady()).toBe(true);
        expect(ctx.service.isStartingSession()).toBe(false);
    });

    it('a late provisioning lifecycle event cannot reopen a panel the turn already closed', async () => {
        const ctx = createHarness();
        ctx.completeFirstTurnServerSide();
        ctx.server.stateFails = true;

        void ctx.service.connect(THREAD_ID);
        await settle();
        expect(ctx.service.isStartingSession()).toBe(false);

        // The orchestrator's `session.lifecycle` events arrive on the app-wide
        // notification SSE and can land after the turn they were announcing.
        ctx.mockNotifications.lifecycleEvent.set({
            thread_id: THREAD_ID,
            state: 'provisioning',
        });
        await settle(2);

        expect(ctx.service.startupPhase()).not.toBe('provisioning');
        expect(ctx.service.isStartingSession()).toBe(false);
        expect(assistantTexts(ctx.service)).toContain(REPLY_TEXT);
    });

    it('leaves the panel up while the unit has not finished', async () => {
        const ctx = createHarness();
        // Messages exist (a resumed thread), but this turn is still queued.
        ctx.server.messages = answeredTranscript();
        ctx.server.queue = QUEUE_QUEUED;
        ctx.server.stateFails = true;

        void ctx.service.connect(THREAD_ID);
        await settle();

        expect(ctx.service.queueState()?.state).toBe('queued');
        expect(ctx.service.sessionReady()).toBe(false);
        expect(ctx.service.isStartingSession()).toBe(true);
    });

    it('leaves the panel up while there is no transcript to show', async () => {
        const ctx = createHarness();
        // A `done` unit with nothing to render is not evidence a turn ran on
        // anything this tab can display — never trade a spinner for a blank.
        ctx.server.messages = [];
        ctx.server.queue = QUEUE_DONE;
        ctx.server.stateFails = true;

        void ctx.service.connect(THREAD_ID);
        await settle();

        expect(ctx.service.queueState()?.state).toBe('done');
        expect(ctx.service.sessionReady()).toBe(false);
        expect(ctx.service.isStartingSession()).toBe(true);
    });

    it('a pinned session (no run-queue unit) is unaffected', async () => {
        const ctx = createHarness();
        ctx.server.messages = answeredTranscript();
        ctx.server.queue = null as any; // PinnedConnectionResponse.queue is always null
        ctx.server.stateFails = true;

        void ctx.service.connect(THREAD_ID);
        await settle();

        expect(ctx.service.queueState()).toBeNull();
        expect(ctx.service.sessionReady()).toBe(false);
        expect(ctx.service.isStartingSession()).toBe(true);
    });
});
