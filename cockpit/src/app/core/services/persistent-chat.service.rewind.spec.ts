/**
 * Task 8 — cockpit service surface for session rewind
 * (knowledge-base/knowledge/features/session_rewind.md).
 *
 * rewind()/summarizeUpTo() only touch _sendControl, so this harness skips the
 * EventSource/WebSocket constructor mocks and connect() flow that
 * persistent-chat.service.spec.ts / .outbox.spec.ts need: it builds the
 * service through the same TestBed provider set, then assigns `controlWs`
 * directly the way "PersistentChatService — control frame delivery across a
 * reconnect" does in the main spec, and asserts on the mock socket's `send`
 * calls.
 */
import {describe, expect, it, vi} from 'vitest';
import {signal} from '@angular/core';
import {TestBed} from '@angular/core/testing';
import {HttpClient} from '@angular/common/http';
import {of, throwError} from 'rxjs';
import {TranslocoService} from '@jsverse/transloco';
import {PersistentChatService} from './persistent-chat.service';
import {ApiService} from './api.service';
import {CapabilitiesService} from './capabilities.service';
import {IndexedDbService} from './indexed-db.service';
import {NotificationService} from './notification.service';
import {AppToastService} from '../../ui/toast';

function createService() {
    const mockHttp: any = {
        get: vi.fn().mockReturnValue(of({status: 'active', total_turns: 0, messages: [], total: 0})),
        post: vi.fn().mockReturnValue(of({})),
        patch: vi.fn().mockReturnValue(of({status: 'updated'})),
        delete: vi.fn().mockReturnValue(of({})),
    };
    const mockApi: any = {
        uploadOneToThread: vi.fn().mockReturnValue(of({kind: 'done', files: []})),
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
        show: vi.fn(),
        info: vi.fn(),
        success: vi.fn(),
        warning: vi.fn(),
        danger: vi.fn(),
        dismiss: vi.fn(),
        dismissAll: vi.fn(),
    };
    const mockNotifications: any = {
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
    return {service, mockHttp, mockCache};
}

/** A control WebSocket that is already OPEN — _sendControl writes straight
 *  through it instead of queueing on controlOutbox. */
function createMockWs() {
    return {
        readyState: WebSocket.OPEN,
        send: vi.fn(),
        close: vi.fn(),
        onopen: null,
        onmessage: null,
        onclose: null,
        onerror: null,
        addEventListener: vi.fn(),
        removeEventListener: vi.fn(),
    } as any;
}

/** A pinned session whose `/connection` resolved to a control socket, with
 *  `ws` as that socket (open, connecting, or null for a dropped one). */
function pinned(service: PersistentChatService, ws: any): void {
    service.threadId.set('thread-rw');
    (service as any).controlSocket = 'websocket';
    (service as any).controlWs = ws;
}

function framesOn(ws: any): Record<string, unknown>[] {
    return ws.send.mock.calls.map((c: any) => JSON.parse(c[0]));
}

describe('PersistentChatService rewind', () => {
    it('sends a flat rewind frame with a request_id and flags in-flight', () => {
        const {service} = createService();
        const live = createMockWs();
        pinned(service, live);

        const requestId = service.rewind('row-1', 'conversation');

        const frame = framesOn(live)[0];
        expect(frame.method).toBe('rewind');
        expect(frame.message_id).toBe('row-1');
        expect(frame.mode).toBe('conversation');
        expect(frame.request_id).toBe(requestId);
        expect(frame.params).toBeUndefined();
        expect(service.rewindInFlight()).toBe(true);
    });

    it('summarizeUpTo rides the compact verb with a boundary', () => {
        const {service} = createService();
        const live = createMockWs();
        pinned(service, live);

        service.summarizeUpTo('row-2');

        const frame = framesOn(live)[0];
        expect(frame.method).toBe('compact');
        expect(frame.boundary_message_id).toBe('row-2');
    });

    it('uses preview plus the unchanged expected boundary for REST rewind', async () => {
        const {service, mockHttp} = createService();
        service.threadId.set('thread-rw');
        (service as any).controlSocket = 'none';
        (service as any).controlCapabilities.set({
            threadId: 'thread-rw',
            controls: {rewind: 'rest'},
            options: {rewind: {version: 1, modes: ['conversation'], requires_idle: true}},
        });
        const expected = {
            session_runtime_generation: '00000000-0000-4000-8000-000000000001',
            conversation_revision: 0,
            events_epoch: 3,
            transcript_tail_seq: '7',
            input_seq: '2',
            consumed_seq: '2',
        };
        mockHttp.get.mockImplementation((url: string) => {
            if (url.includes('/rewinds/preview')) {
                return of({
                    message_id: 'row-1',
                    mode: 'conversation',
                    prompt: 'original prompt',
                    eligible: true,
                    refusal_code: null,
                    swept_count: 2,
                    expected,
                });
            }
            if (url.endsWith('/messages')) {
                return of({messages: [], total: 0, events_epoch: 4, conversation_revision: 1});
            }
            if (url.endsWith('/state')) {
                return of({
                    thread_id: 'thread-rw',
                    permission_mode: 'supervised',
                    narration_mode: 'verbose',
                    turn_count: 0,
                    turn_in_flight: false,
                    message_count: 0,
                    model: null,
                    temperature: null,
                    running_tool: null,
                    pending_permissions: [],
                    event_cursor: {epoch: 4, seq: 1},
                    replay_cursor: {epoch: 4, seq: 1},
                    conversation_revision: 1,
                    snapshot_source: 'durable_journal',
                });
            }
            return of({status: 'active', total_turns: 0});
        });
        mockHttp.post.mockImplementation((_url: string, body: any) =>
            of({
                state: 'applied',
                rewind_id: '00000000-0000-4000-8000-000000000003',
                client_request_id: body.client_request_id,
                message_id: body.message_id,
                mode: 'conversation',
                prompt: 'original prompt',
                swept_count: 2,
                surviving_turn: 0,
                conversation_revision: 1,
                events_epoch: 4,
                event_seq: '1',
                duplicate: false,
            }),
        );

        await service.prepareRewind('row-1');
        const requestId = service.rewind('row-1', 'conversation', 'draft');

        await vi.waitFor(() => expect(service.rewindInFlight()).toBe(false));
        const rewindCall = mockHttp.post.mock.calls.find((call: any[]) =>
            String(call[0]).endsWith('/rewinds'),
        );
        expect(rewindCall?.[1]).toEqual({
            client_request_id: requestId,
            message_id: 'row-1',
            mode: 'conversation',
            expected,
        });
        expect(service.conversationRevision()).toBe(1);
        expect(service.rewindPrefill()).toEqual({
            prompt: 'original prompt',
            draftSnapshot: 'draft',
            draftRevision: 0,
            clientRequestId: requestId,
        });
        expect((service as any).controlOutbox).toEqual([]);
    });

    it('does not flush a pre-rewind outbox entry and returns it for review', async () => {
        const {service, mockHttp} = createService();
        service.threadId.set('thread-rw');
        service.conversationRevision.set(2);
        service.sessionReady.set(true);
        service.outbox.set([{
            localId: 'local-old',
            displayContent: 'draft from the old view',
            threadId: 'thread-rw',
            attempts: 0,
            expectedConversationRevision: 1,
        }]);

        await (service as any)._flushOutbox();

        expect(mockHttp.post).not.toHaveBeenCalled();
        expect(service.outbox()[0].requiresReview).toBe(true);
        expect(service.takeQueuedSendForReview('local-old')).toBe('draft from the old view');
        expect(service.outbox()).toEqual([]);
    });

    it('keeps an input rejected by a concurrent rewind and marks it for review', async () => {
        const {service, mockHttp} = createService();
        service.threadId.set('thread-rw');
        service.conversationRevision.set(1);
        service.sessionReady.set(true);
        service.outbox.set([{
            localId: 'local-raced',
            displayContent: 'draft sent while another tab rewound',
            threadId: 'thread-rw',
            attempts: 0,
            expectedConversationRevision: 1,
        }]);
        mockHttp.post.mockReturnValue(throwError(() => ({
            status: 409,
            error: {
                detail: {
                    code: 'session_view_stale',
                    conversation_revision: 2,
                },
            },
        })));

        await (service as any)._flushOutbox();

        expect(mockHttp.post).toHaveBeenCalledWith(
            expect.stringMatching(/\/persistent\/threads\/thread-rw\/input$/),
            {
                content: 'draft sent while another tab rewound',
                expected_conversation_revision: 1,
            },
        );
        expect(service.conversationRevision()).toBe(2);
        expect(service.outbox()[0].requiresReview).toBe(true);
        expect(service.error()).toBe('chat.rewind.staleOutbox');
    });
});

/**
 * Fix 6 (final review): rewind must never ride _sendControl's
 * queue-and-replay fallback. Other control verbs are fine to queue while the
 * socket reconnects, but a queued rewind could fire against a session the
 * user resumed much later for an unrelated reason — destructive verbs must
 * be sent now or refused, never deferred.
 */
describe('PersistentChatService — rewind refuses to queue when the control WS is down', () => {
    it('controlWs = null: no frame queued on controlOutbox, flag stays false, error is set', () => {
        const {service} = createService();
        pinned(service, null);

        const requestId = service.rewind('row-1', 'conversation');

        expect(requestId).toBeTruthy();
        expect((service as any).controlOutbox).toEqual([]);
        expect(service.rewindInFlight()).toBe(false);
        expect(service.error()).toBe('chat.rewind.connectionDown');
    });

    it('controlWs present but not OPEN (e.g. CONNECTING): same refusal, no queueing', () => {
        const {service} = createService();
        const connecting = createMockWs();
        connecting.readyState = WebSocket.CONNECTING;
        pinned(service, connecting);

        service.rewind('row-1', 'both');

        expect(connecting.send).not.toHaveBeenCalled();
        expect((service as any).controlOutbox).toEqual([]);
        expect(service.rewindInFlight()).toBe(false);
        expect(service.error()).toBe('chat.rewind.connectionDown');
    });

    it('does not arm the ack-fallback timer on refusal (nothing to disarm later)', async () => {
        vi.useFakeTimers();
        try {
            const {service} = createService();
            pinned(service, null);

            service.rewind('row-1', 'conversation');

            const warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {});
            await vi.advanceTimersByTimeAsync(90_001);
            expect(warnSpy).not.toHaveBeenCalled();
            warnSpy.mockRestore();
        } finally {
            vi.useRealTimers();
        }
    });

    it('a normal open-socket rewind is unaffected: still sends and flags in-flight', () => {
        const {service} = createService();
        const live = createMockWs();
        pinned(service, live);

        service.rewind('row-1', 'conversation');

        expect(live.send).toHaveBeenCalledTimes(1);
        expect(service.rewindInFlight()).toBe(true);
        expect(service.error()).toBeNull();
    });

    it('a send throw is refused and never enters the reconnect outbox', () => {
        const {service} = createService();
        const live = createMockWs();
        live.send.mockImplementation(() => { throw new Error('closed'); });
        pinned(service, live);

        service.rewind('row-1', 'conversation');

        expect((service as any).controlOutbox).toEqual([]);
        expect(service.rewindInFlight()).toBe(false);
        expect(service.error()).toBe('chat.rewind.connectionDown');
    });
});

/**
 * Fix round 1 (review finding): rewindInFlight used to be set only in
 * rewind() and cleared only by rewind.ack / rewind.files_restored / a
 * blanket 'error' — but the ack is WS-direct to the originating socket
 * only, so a drop/reconnect between send and ack lost it forever, and an
 * unrelated in-flight error (e.g. a concurrent config.update denial) could
 * clear it prematurely. Mirrors the file's own interrupt()/
 * _armInterruptFallback/_clearInterruptFallback self-healing pattern; see
 * "PersistentChatService — interrupt self-healing" in the main spec for the
 * precedent this borrows its style from.
 */
describe('PersistentChatService — rewind self-healing', () => {
    beforeEach(() => {
        vi.useFakeTimers();
    });

    afterEach(() => {
        vi.useRealTimers();
    });

    it('force-clears rewindInFlight if rewind.ack never arrives (lost/dropped frame)', async () => {
        const {service} = createService();
        const live = createMockWs();
        pinned(service, live);

        service.rewind('row-1', 'conversation');
        expect(service.rewindInFlight()).toBe(true);

        // No rewind.ack / rewind.files_restored / matching error arrives —
        // the fallback fires at REWIND_ACK_TIMEOUT_MS (90s) and un-wedges
        // the UI rather than leaving "Rewinding…" stuck forever.
        await vi.advanceTimersByTimeAsync(90_001);

        expect(service.rewindInFlight()).toBe(false);
    });

    it('does not fire the fallback once rewind.ack has already cleared it', async () => {
        const {service} = createService();
        const live = createMockWs();
        pinned(service, live);

        const requestId = service.rewind('row-1', 'conversation');
        (service as any)._handleEvent({
            method: 'rewind.ack',
            params: {request_id: requestId, message_id: 'row-1', mode: 'conversation'},
        });
        expect(service.rewindInFlight()).toBe(false);

        // The ack disarmed the timer — advancing past the deadline must not
        // resurrect rewindInFlight or throw from a stale callback.
        await vi.advanceTimersByTimeAsync(90_001);
        expect(service.rewindInFlight()).toBe(false);
    });

    it('error only clears rewindInFlight for the matching request_id', () => {
        const {service} = createService();
        const live = createMockWs();
        pinned(service, live);

        const requestId = service.rewind('row-1', 'conversation');
        expect(service.rewindInFlight()).toBe(true);

        // An unrelated in-flight request's error (e.g. a concurrent
        // config.update denial) must not prematurely re-enable the UI.
        (service as any)._handleEvent({
            method: 'error',
            params: {message: 'denied', request_id: 'unrelated-request'},
        });
        expect(service.rewindInFlight()).toBe(true);

        (service as any)._handleEvent({
            method: 'error',
            params: {message: 'rewind failed', request_id: requestId},
        });
        expect(service.rewindInFlight()).toBe(false);
    });

    it('an error with no request_id at all leaves rewindInFlight untouched', () => {
        const {service} = createService();
        const live = createMockWs();
        pinned(service, live);

        service.rewind('row-1', 'conversation');
        (service as any)._handleEvent({method: 'error', params: {message: 'boom'}});

        expect(service.rewindInFlight()).toBe(true);
    });

    it('disconnect() resets rewindInFlight/rewindPrefill left over from a mid-flight rewind', async () => {
        // The scenario the Important finding described: a WS drop/reconnect
        // between send and ack. disconnect() must not leave the flag wedged.
        const {service} = createService();
        const live = createMockWs();
        pinned(service, live);

        service.rewind('row-1', 'conversation');
        expect(service.rewindInFlight()).toBe(true);

        service.disconnect();

        expect(service.rewindInFlight()).toBe(false);
        expect(service.rewindPrefill()).toBeNull();

        // The armed fallback timer was disarmed too — advancing past its
        // deadline must not warn or touch state again.
        const warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {});
        await vi.advanceTimersByTimeAsync(90_001);
        expect(warnSpy).not.toHaveBeenCalled();
        warnSpy.mockRestore();
    });
});

/**
 * `rewind` rides the declared-controls choke point
 * (knowledge-base/knowledge/issues/live_settings_silently_dropped_on_stateless_sessions.md
 * §"rewind escapes the choke point"). The verb is offered only where a live
 * control plane has DECLARED a transport for it, and a dispatch the session
 * cannot carry is refused out loud — never "connection is down" for a verb the
 * lane does not have, and never a silent drop.
 */
describe('PersistentChatService — rewind follows the declared controls', () => {
    const STATELESS_WITHOUT_REWIND = {
        'config.update': 'rest',
        'workspace.undo': 'rest',
        'mode.set': 'rest',
        'narration.set': 'rest',
    };

    it('a stateless declaration without rewind offers no mode and refuses loudly', () => {
        const {service, mockHttp} = createService();
        // An earlier REST rewind in this file leaves its receipt marker behind.
        sessionStorage.removeItem('srw.rewind.operation.thread-rw');
        service.threadId.set('thread-rw');
        (service as any).controlSocket = 'none';
        (service as any).controlCapabilities.set({
            threadId: 'thread-rw',
            controls: STATELESS_WITHOUT_REWIND,
            options: {},
        });

        expect(service.controlTransport('rewind')).toBe('unavailable');
        for (const mode of ['both', 'conversation', 'code'] as const) {
            expect(service.rewindModeAvailable(mode)).toBe(false);
        }
        service.rewind('row-1', 'conversation');

        expect(service.error()).toBe('chat.rewind.unavailable');
        expect(service.rewindInFlight()).toBe(false);
        expect(mockHttp.post).not.toHaveBeenCalled();
        expect((service as any).controlOutbox).toEqual([]);
        expect(sessionStorage.getItem('srw.rewind.operation.thread-rw')).toBeNull();
    });

    it('is not offered before /connection has declared a transport', () => {
        // Until the declaration lands, controlTransport assumes a socket — so a
        // stateless session showed the rewind button and answered a click
        // with "connection is down" for a verb it never had.
        const {service} = createService();
        service.threadId.set('thread-rw');

        expect(service.rewindModeAvailable('conversation')).toBe(false);
        service.rewind('row-1', 'conversation');
        expect(service.error()).toBe('chat.rewind.unavailable');

        // Once a pinned session resolves with a socket, it is offered again.
        (service as any).controlSocket = 'websocket';
        expect(service.rewindModeAvailable('conversation')).toBe(true);
    });

    it('is not offered on a retired (ended) session, whose dispatch would drop silently', () => {
        const {service} = createService();
        pinned(service, createMockWs());
        (service as any).controlCapabilities.set({
            threadId: 'thread-rw',
            controls: {rewind: 'websocket'},
            options: {},
        });
        expect(service.rewindModeAvailable('conversation')).toBe(true);

        (service as any)._retireTerminalControl('thread-rw');

        // The declaration outlives the retirement, but the control plane does
        // not: _sendImmediateControl's guard would return without a word.
        expect(service.rewindModeAvailable('conversation')).toBe(false);
        service.rewind('row-1', 'conversation');
        expect(service.error()).toBe('chat.rewind.unavailable');
        expect(service.rewindInFlight()).toBe(false);
    });
});
