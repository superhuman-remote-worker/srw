import {afterEach, beforeAll, beforeEach, describe, expect, it, vi} from 'vitest';
import {
    CUSTOM_ELEMENTS_SCHEMA,
    DestroyableInjector,
    Injector,
    Pipe,
    PipeTransform,
    runInInjectionContext,
    signal,
    ɵresolveComponentResources,
} from '@angular/core';
import {TestBed} from '@angular/core/testing';
import {TitleCasePipe} from '@angular/common';
import {HttpClient, HttpErrorResponse} from '@angular/common/http';
import {Router} from '@angular/router';
import {Observable, of, Subject, throwError} from 'rxjs';
import {SessionsPageComponent} from './sessions-page.component';
import {SessionListService} from '../../core/services/session-list.service';
import {TranslocoService} from '@jsverse/transloco';
import {PersistentChatService} from '../../core/services/persistent-chat.service';
import {ModelService} from '../../core/services/model.service';
import {SettingsService} from '../../core/services/settings.service';
import {AppToastService} from '../../ui/toast';
import {ErrorMessageService} from '../../core/services/error-message.service';
import {UserService} from '../../core/services/user.service';

/**
 * The page's collaborators as mocks, plus the providers that wire them. Shared
 * by the direct-construction harness and the rendered (TestBed) one.
 */
function createMocks() {
    const mockHttp: any = {
        get: vi.fn().mockReturnValue(of({threads: []})),
        post: vi.fn().mockReturnValue(of({thread_id: 'new-thread-123'})),
        delete: vi.fn().mockReturnValue(of({})),
    };

    const mockRouter: any = {
        navigate: vi.fn(),
    };

    const mockChat: any = {
        isConnected: () => false,
        threadId: () => null,
        renameThread: vi.fn().mockResolvedValue(undefined),
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

    const mockUserService: any = {
        currentUserId: () => null,
        currentUser: () => null,
        users: () => [],
        isAuthenticated: () => false,
        isApproved: () => false,
        sessionReady: () => true,
    };

    const mockSettings: any = {
        apiKeys: signal([]),
        preferences: signal({}),
        updatePreferences: vi.fn().mockReturnValue(of({status: 'ok'})),
    };

    const mockModelService: any = {
        models: signal([]),
        presets: signal([]),
        auxiliaryModels: signal([]),
        visionModels: signal([]),
        whisperModels: signal([]),
        embeddingModels: signal([]),
        providers: signal([]),
        loaded: signal(false),
        loading: signal(false),
        load: vi.fn(),
    };

    const providers = [
        {provide: HttpClient, useValue: mockHttp},
        {provide: Router, useValue: mockRouter},
        {provide: PersistentChatService, useValue: mockChat},
        {provide: AppToastService, useValue: mockToast},
        {provide: ErrorMessageService, useValue: {translate: (_e: unknown, fallback: string) => fallback}},
        {provide: UserService, useValue: mockUserService},
        {provide: SettingsService, useValue: mockSettings},
        {provide: ModelService, useValue: mockModelService},
        {provide: TranslocoService, useValue: {translate: (key: string) => key, getActiveLang: () => 'en'}},
        // Real service, not a hand-rolled mock: it has no logic of its own
        // worth stubbing, and wiring it for real here means it resolves
        // against the same mockHttp above — every existing assertion on
        // mockHttp.get keeps working unchanged.
        SessionListService,
    ];
    return {mockHttp, mockRouter, mockChat, mockToast, providers};
}

/**
 * Create a SessionsPageComponent in a minimal injection context. The injector
 * is returned so a test can destroy it: outside a rendered view the page's
 * DestroyRef resolves to this injector, so destroying it is the page's destroy.
 */
function createComponent() {
    const {mockHttp, mockRouter, mockChat, mockToast, providers} = createMocks();
    const injector = Injector.create({providers});

    const component = runInInjectionContext(injector, () => new SessionsPageComponent());
    const sessionList = injector.get(SessionListService);
    return {component, injector, mockHttp, mockRouter, mockChat, mockToast, sessionList};
}

function makeThread(overrides: Partial<any> = {}) {
    return {
        id: 'thread-1',
        title: 'Test Session',
        status: 'active',
        config_name: 'persistent_defaults',
        permission_mode: 'supervised',
        created_at: '2026-01-01T00:00:00Z',
        last_activity: '2026-01-01T01:00:00Z',
        ended_at: null,
        total_turns: 5,
        project_id: null,
        ...overrides,
    };
}

/**
 * The gaps between background re-reads while any card is `ending` (a pinned
 * retirement settles in about 60–75 s): the first re-read 2 s after the card
 * is seen, doubling, capped at 15 s — so a settled card updates within the
 * server's settlement time plus at most one 15 s interval.
 */
const ENDING_POLL_GAPS_MS = [2_000, 4_000, 8_000, 15_000, 15_000, 15_000];

/**
 * Route GETs by URL. Each `/persistent/threads` read answers with the next
 * entry of `lists` (the last one repeats); an array is served as the thread
 * list, an Observable is returned as-is so a test can hold a read in flight.
 * Anything else (the projects read) answers empty.
 */
function serveThreadLists(mockHttp: any, ...lists: Array<any[] | Observable<unknown>>): void {
    let next = 0;
    mockHttp.get.mockImplementation((url: string) => {
        if (!String(url).includes('/persistent/threads')) return of([]);
        const list = lists[Math.min(next++, lists.length - 1)];
        return Array.isArray(list) ? of({threads: list}) : list;
    });
}

/** How many times the page has read the thread list. */
function threadListReads(mockHttp: any): number {
    return mockHttp.get.mock.calls.filter((call: any[]) =>
        String(call[0]).includes('/persistent/threads'),
    ).length;
}


describe('SessionsPageComponent', () => {
    let component: SessionsPageComponent;
    let injector: DestroyableInjector;
    let mockHttp: any;
    let mockRouter: any;
    let mockChat: any;
    let mockToast: any;
    let sessionList: SessionListService;
    let pageDestroyed: boolean;

    /** The page's destroy; idempotent (an injector throws if destroyed twice). */
    const destroyPage = () => {
        if (pageDestroyed) return;
        pageDestroyed = true;
        injector.destroy();
    };

    beforeEach(() => {
        const created = createComponent();
        component = created.component;
        injector = created.injector;
        mockHttp = created.mockHttp;
        mockRouter = created.mockRouter;
        mockChat = created.mockChat;
        mockToast = created.mockToast;
        sessionList = created.sessionList;
        pageDestroyed = false;
    });

    afterEach(() => {
        // Destroy the page so nothing it scheduled outlives its test.
        destroyPage();
        vi.clearAllMocks();
    });

    // =========================================================================
    // 9.2.1: Initial state
    // =========================================================================

    describe('initial state', () => {
        it('should start with empty threads', () => {
            expect(component.threads()).toEqual([]);
        });

        it('should start with loading true', () => {
            expect(component.loading()).toBe(true);
        });

        it('should start with creating false', () => {
            expect(component.creating()).toBe(false);
        });

        it('should start with null status filter', () => {
            expect(component.statusFilter()).toBeNull();
        });

        it('should have default form values', () => {
            expect(component.newConfig).toBe('session_base');
            expect(component.newPermission).toBe('supervised');
            expect(component.newModel).toBe('');
            expect(component.newTitle).toBe('');
        });
    });

    // =========================================================================
    // 9.2.2: loadThreads()
    // =========================================================================

    describe('loadThreads()', () => {
        it('should fetch threads from API', async () => {
            const threads = [makeThread(), makeThread({id: 'thread-2'})];
            mockHttp.get.mockReturnValue(of({threads}));

            await component.loadThreads();

            expect(mockHttp.get).toHaveBeenCalled();
            expect(component.threads()).toEqual(threads);
            expect(component.loading()).toBe(false);
        });

        it('should set loading false on error', async () => {
            mockHttp.get.mockReturnValue(throwError(() => new Error('fail')));

            await component.loadThreads();

            expect(component.loading()).toBe(false);
        });

        it('should handle missing threads in response', async () => {
            mockHttp.get.mockReturnValue(of({}));

            await component.loadThreads();

            expect(component.threads()).toEqual([]);
        });

        it('maps the safe retirement-pending projection to the non-resumable ending state', async () => {
            mockHttp.get.mockReturnValue(of({
                threads: [
                    makeThread({
                        runtime_retirement_pending: true,
                        retirement_disposition: 'ended',
                    }),
                ],
            }));

            await component.loadThreads();

            expect(component.threads()[0].status).toBe('ending');
            expect(component.threads()[0].runtime_retirement_pending).toBe(true);
        });

        it('fails closed if a server or cache includes a child thread', async () => {
            mockHttp.get.mockReturnValue(of({
                threads: [
                    makeThread({kind: 'session'}),
                    makeThread({id: 'child-1', kind: 'subagent'}),
                ],
            }));

            await component.loadThreads();

            expect(component.threads().map(thread => thread.id)).toEqual(['thread-1']);
        });
    });

    // =========================================================================
    // F3: onRenameThread() must keep SessionListService (the rail's copy of
    // the list) in sync — this page's own `threads` is a filtered/mapped
    // snapshot, not a computed over the service, so nothing does that for
    // free.
    // =========================================================================

    describe('onRenameThread()', () => {
        it('patches the rail copy (SessionListService) alongside its own list', async () => {
            mockHttp.get.mockReturnValue(of({threads: [makeThread({id: 'thread-1', title: 'Old title'})]}));
            await component.loadThreads();
            mockChat.renameThread = vi.fn().mockResolvedValue(undefined);

            await component.onRenameThread(component.threads()[0], 'New title');

            expect(component.threads()[0].title).toBe('New title');
            expect(sessionList.threads()[0].title).toBe('New title');
        });

        it('reverts both lists and toasts when the rename PATCH fails', async () => {
            mockHttp.get.mockReturnValue(of({threads: [makeThread({id: 'thread-1', title: 'Old title'})]}));
            await component.loadThreads();
            mockChat.renameThread = vi.fn().mockRejectedValue(new Error('boom'));

            await component.onRenameThread(component.threads()[0], 'New title');

            expect(component.threads()[0].title).toBe('Old title');
            expect(sessionList.threads()[0].title).toBe('Old title');
            expect(mockToast.danger).toHaveBeenCalled();
        });
    });

    // =========================================================================
    // 9.2.3: filteredThreads()
    // =========================================================================

    describe('filteredThreads()', () => {
        it('should return all threads when filter is null', () => {
            const threads = [
                makeThread({status: 'active'}),
                makeThread({id: 't2', status: 'ended'}),
            ];
            component.threads.set(threads);
            component.statusFilter.set(null);

            expect(component.filteredThreads().length).toBe(2);
        });

        it('keeps the Active bucket consistent with its non-ended count', () => {
            component.threads.set([
                makeThread({status: 'active'}),
                makeThread({id: 't2', status: 'ending'}),
                makeThread({id: 't3', status: 'suspended'}),
                makeThread({id: 't4', status: 'awaiting_user'}),
                makeThread({id: 't5', status: 'ended'}),
            ]);
            component.statusFilter.set('active');

            const filtered = component.filteredThreads();
            expect(filtered.map(thread => thread.status)).toEqual([
                'active',
                'ending',
                'suspended',
                'awaiting_user',
            ]);
            expect(component.activeCount()).toBe(filtered.length);
        });

        it('should filter by ended status', () => {
            component.threads.set([
                makeThread({status: 'active'}),
                makeThread({id: 't2', status: 'ended'}),
            ]);
            component.statusFilter.set('ended');

            const filtered = component.filteredThreads();
            expect(filtered.length).toBe(1);
            expect(filtered[0].status).toBe('ended');
        });
    });

    // =========================================================================
    // 9.2.4: createSession()
    // =========================================================================

    describe('createSession()', () => {
        // The dialog creates the thread itself and only then navigates. It used
        // to hand the body to a `_creating` route and dismiss, so a rejected
        // config cleared the fields and bounced back with nothing to correct.
        const postedBody = () => mockHttp.post.mock.calls[0][1];

        it('should POST the create body before navigating', async () => {
            await component.createSession();

            expect(mockHttp.post).toHaveBeenCalledWith(
                expect.stringContaining('/persistent/threads'),
                expect.any(Object),
            );
            expect(mockRouter.navigate).toHaveBeenCalledWith(
                ['/sessions', 'new-thread-123'],
            );
        });

        it('should include title, config, and permission in body', async () => {
            component.newTitle = 'My Session';
            component.newConfig = 'developer';
            component.newPermission = 'autonomous';

            await component.createSession();

            expect(postedBody().title).toBe('My Session');
            expect(postedBody().config_name).toBe('developer');
            expect(postedBody().permission_mode).toBe('autonomous');
        });

        it('should use "Untitled Session" when title is empty', async () => {
            component.newTitle = '';

            await component.createSession();

            expect(postedBody().title).toBe('Untitled Session');
        });

        it('should include model when specified', async () => {
            component.newModel = 'gpt-5.4';

            await component.createSession();

            expect(postedBody().model).toBe('gpt-5.4');
        });

        it('should NOT include model when empty', async () => {
            component.newModel = '';

            await component.createSession();

            expect(postedBody().model).toBeUndefined();
        });

        it('should include project_ids when selected', async () => {
            component.selectedProjectIds.set(['proj-1', 'proj-2']);

            await component.createSession();

            expect(postedBody().project_ids).toEqual(['proj-1', 'proj-2']);
        });

        it('should reset form state after creation', async () => {
            component.newTitle = 'Title';
            component.newModel = 'gpt-5.4';
            component.selectedProjectIds.set(['proj-1']);
            component.showCreate = true;

            await component.createSession();

            expect(component.newTitle).toBe('');
            expect(component.newModel).toBe('gpt-5.4');
            expect(component.selectedProjectIds()).toEqual([]);
            expect(component.showCreate).toBe(false);
        });

        it('should set creating signal during creation', async () => {
            await component.createSession();

            // After completion, creating should be false
            expect(component.creating()).toBe(false);
        });

        it('keeps the dialog and its selections when the server rejects the config', async () => {
            mockHttp.post.mockReturnValueOnce(
                throwError(() => new HttpErrorResponse({
                    status: 400,
                    error: {detail: 'Lite session backends cannot attach repository connectors'},
                })),
            );
            component.newTitle = 'Keep me';
            component.selectedProjectIds.set(['proj-1']);
            component.showCreate = true;

            await component.createSession();

            expect(component.showCreate).toBe(true);
            expect(component.newTitle).toBe('Keep me');
            expect(component.selectedProjectIds()).toEqual(['proj-1']);
            expect(component.creating()).toBe(false);
            expect(mockRouter.navigate).not.toHaveBeenCalled();
            // Surfaced in-dialog (this harness stubs ErrorMessageService to echo
            // the fallback key; detail extraction is covered in its own spec).
            expect(component.createError()).toBeTruthy();
        });
    });

    // =========================================================================
    // 9.2.5: resumeSession()
    // =========================================================================

    describe('resumeSession()', () => {
        it('should navigate to session page for active thread', () => {
            const thread = makeThread({id: 'thread-abc'});
            component.resumeSession(thread);

            expect(mockRouter.navigate).toHaveBeenCalledWith(['/sessions', 'thread-abc']);
        });

        it('should POST to resume endpoint when thread is ended', async () => {
            mockHttp.post.mockReturnValue(of({}));
            const thread = makeThread({id: 'thread-end', status: 'ended'});
            await component.resumeSession(thread);

            const resumeCall = mockHttp.post.mock.calls.find(
                (c: any[]) => c[0]?.includes(`/persistent/threads/thread-end/resume`),
            );
            expect(resumeCall).toBeTruthy();
            expect(mockRouter.navigate).toHaveBeenCalledWith(['/sessions', 'thread-end']);
        });

        // Fix round 1, Finding 3: this page has no drift dialog of its own —
        // the chat page does (config-drift-dialog.component.ts). A 428 here
        // used to fall into the generic catch and show the same toast a 500
        // gets, dead-ending the user exactly like the feature exists to stop.
        it('hands off to the chat page on a config-drift 428, with an info toast and no danger toast', async () => {
            mockHttp.post.mockReturnValue(throwError(() => ({
                status: 428,
                error: {
                    detail: {
                        code: 'config_drift',
                        drift: [{id: 'connector:abc', kind: 'connector',
                                 reason: 'deleted', label: 'KurortEngine'}],
                    },
                },
            })));
            const thread = makeThread({id: 'thread-drift', status: 'ended'});

            await component.resumeSession(thread);

            expect(mockRouter.navigate).toHaveBeenCalledWith(['/sessions', 'thread-drift']);
            expect(mockToast.danger).not.toHaveBeenCalled();
            // Task 14, item A: the first click used to look like it did
            // nothing (428 precedes the status flip, so the chat page renders
            // its generic ended-card). An info toast — not danger, this isn't
            // an error — says the setup needs attention before navigating.
            expect(mockToast.info).toHaveBeenCalledWith('sessions.configDrift.attentionNeeded');
        });

        it('still shows the danger toast, no info toast, and does not navigate on a non-drift resume failure', async () => {
            mockHttp.post.mockReturnValue(throwError(() => ({status: 500})));
            const thread = makeThread({id: 'thread-500', status: 'ended'});

            await component.resumeSession(thread);

            expect(mockToast.danger).toHaveBeenCalled();
            expect(mockToast.info).not.toHaveBeenCalled();
            expect(mockRouter.navigate).not.toHaveBeenCalled();
        });

        // R1 follow-up: Resume on a stateless thread whose soft End is still
        // pending is the server-supported retry — the server finishes the
        // pending End cleanup, then resumes.
        it('POSTs resume for a stateless soft-pending (ending) card', async () => {
            mockHttp.post.mockReturnValue(of({status: 'created'}));
            const thread = makeThread({
                id: 't-soft',
                status: 'ending',
                execution_lane: 'stateless',
                runtime_retirement_pending: true,
                retirement_disposition: 'ended',
                retirement_permanent: false,
            });

            await component.resumeSession(thread);

            expect(mockHttp.post).toHaveBeenCalledTimes(1);
            expect(mockHttp.post.mock.calls[0][0]).toContain('/persistent/threads/t-soft/resume');
            expect(mockRouter.navigate).toHaveBeenCalledWith(['/sessions', 't-soft']);
            expect(mockToast.danger).not.toHaveBeenCalled();
        });

        it('stays and re-reads the list when resuming a soft-pending card is refused', async () => {
            const soft = makeThread({
                id: 't-soft',
                status: 'active',
                execution_lane: 'stateless',
                runtime_retirement_pending: true,
                retirement_disposition: 'ended',
                retirement_permanent: false,
            });
            serveThreadLists(mockHttp, [soft]);
            await component.loadThreads();
            const readsBefore = threadListReads(mockHttp);
            mockHttp.post.mockReturnValue(throwError(() => new HttpErrorResponse({
                status: 503,
                error: {detail: 'Stateless workspace lifecycle lock unavailable'},
            })));

            await component.resumeSession(component.threads()[0]);

            expect(mockHttp.post).toHaveBeenCalledTimes(1);
            expect(mockToast.danger).toHaveBeenCalledOnce();
            expect(mockRouter.navigate).not.toHaveBeenCalled();
            expect(threadListReads(mockHttp)).toBe(readsBefore + 1);
            expect(component.threads()[0].status).toBe('ending');
        });
    });

    describe('confirmDelete()', () => {
        it('offers force only for the exact turn_in_flight conflict', async () => {
            component.deleteSession(makeThread());
            mockHttp.delete.mockReturnValueOnce(throwError(() => new HttpErrorResponse({
                status: 409,
                error: {detail: {code: 'turn_in_flight'}},
            })));

            await component.confirmDelete();

            expect(component.confirmForceOpen()).toBe(true);
            expect(mockToast.danger).not.toHaveBeenCalled();
        });

        it('does not convert a generic or retirement 409 into force delete', async () => {
            component.deleteSession(makeThread({status: 'ending'}));
            mockHttp.delete.mockReturnValueOnce(throwError(() => new HttpErrorResponse({
                status: 409,
                error: {detail: {code: 'session_ending'}},
            })));

            await component.confirmDelete();

            expect(component.confirmForceOpen()).toBe(false);
            expect(mockToast.danger).toHaveBeenCalledOnce();
            expect(
                mockHttp.delete.mock.calls.some((call: any[]) =>
                    String(call[0]).includes('force=true'),
                ),
            ).toBe(false);
        });

        // R1.B12 guard: the retry path for a fenced (503) delete must not
        // change the mid-turn escalation — a 409 turn_in_flight still opens
        // the force confirm, raises no toast, and force goes out only after
        // that second confirmation.
        it('keeps the 409 turn_in_flight escalation to a confirmed force delete', async () => {
            component.deleteSession(makeThread({id: 't-live'}));
            mockHttp.delete.mockReturnValueOnce(throwError(() => new HttpErrorResponse({
                status: 409,
                error: {detail: {code: 'turn_in_flight'}},
            })));

            await component.confirmDelete();

            expect(component.confirmForceOpen()).toBe(true);
            expect(mockToast.danger).not.toHaveBeenCalled();
            expect(mockToast.warning).not.toHaveBeenCalled();
            expect(mockHttp.delete).toHaveBeenCalledTimes(1);
            expect(mockHttp.delete.mock.calls[0][0]).not.toContain('force=true');

            await component.confirmForceDelete();

            expect(component.confirmForceOpen()).toBe(false);
            expect(mockHttp.delete).toHaveBeenCalledTimes(2);
            expect(mockHttp.delete.mock.calls[1][0]).toContain(
                '/persistent/threads/t-live?permanent=true&force=true',
            );
        });

        // R1 follow-up: a stateless permanent Delete meets the same busy
        // refusal as a stateless End (`409 stateless_end_busy`; `force=true`
        // stops the unfinished turn). It escalates exactly like turn_in_flight.
        it('escalates a busy stateless delete (409 stateless_end_busy) to the force confirm', async () => {
            component.deleteSession(makeThread({id: 't-busy', execution_lane: 'stateless'}));
            mockHttp.delete.mockReturnValueOnce(throwError(() => new HttpErrorResponse({
                status: 409,
                error: {detail: {code: 'stateless_end_busy', queue_state: 'leased', pending_input: true}},
            })));

            await component.confirmDelete();

            expect(component.confirmForceOpen()).toBe(true);
            expect(mockToast.danger).not.toHaveBeenCalled();
            expect(mockHttp.delete).toHaveBeenCalledTimes(1);

            await component.confirmForceDelete();

            expect(mockHttp.delete).toHaveBeenCalledTimes(2);
            expect(mockHttp.delete.mock.calls[1][0]).toContain(
                '/persistent/threads/t-busy?permanent=true&force=true',
            );
        });

        // Guard (holds before and after): a Delete refused because the
        // session changed underneath it (e.g. a just-Resumed workspace) shows
        // the server's own reason, arms no retry and is never replayed.
        it('shows the server reason for a lifecycle-changed 409 and never replays it', async () => {
            const mocks = createMocks();
            const providers = mocks.providers.map((provider: any) =>
                provider?.provide === ErrorMessageService
                    ? {provide: ErrorMessageService, useFactory: () => new ErrorMessageService(), deps: []}
                    : provider,
            );
            const pageInjector = Injector.create({providers});
            try {
                const page = runInInjectionContext(pageInjector, () => new SessionsPageComponent());
                const thread = makeThread({id: 't-changed', execution_lane: 'stateless'});
                mocks.mockHttp.delete.mockReturnValue(throwError(() => new HttpErrorResponse({
                    status: 409,
                    error: {detail: 'Thread lifecycle generation changed while waiting for cleanup ownership'},
                })));

                page.deleteSession(thread);
                await page.confirmDelete();

                expect(mocks.mockToast.danger).toHaveBeenCalledWith(
                    'Thread lifecycle generation changed while waiting for cleanup ownership',
                );
                expect(mocks.mockToast.warning).not.toHaveBeenCalled();
                expect(page.confirmForceOpen()).toBe(false);
                expect(page.isDeleteRetry(thread)).toBe(false);
                expect(mocks.mockHttp.delete).toHaveBeenCalledTimes(1);
            } finally {
                pageInjector.destroy();
            }
        });
    });

    // =========================================================================
    // R1.B12: an `ending` card refreshes itself instead of waiting for a
    // manual reload (knowledge-base/knowledge/issues/
    // cockpit_ending_session_card_never_refreshes.md).
    // =========================================================================

    describe('ending cards refresh in the background', () => {
        beforeEach(() => {
            vi.useFakeTimers();
        });

        afterEach(() => {
            // Destroy while the fake clock is still installed, so the page
            // clears its own timer rather than one the real clock never had.
            destroyPage();
            vi.useRealTimers();
        });

        it('re-reads the list while a card is ending and shows it ended, with no loading placeholder', async () => {
            const inFlight = new Subject<unknown>();
            serveThreadLists(mockHttp, [makeThread({id: 't-end', status: 'ending'})], inFlight);

            component.ngOnInit();
            await vi.advanceTimersByTimeAsync(0);
            expect(component.threads()[0].status).toBe('ending');
            expect(threadListReads(mockHttp)).toBe(1);

            await vi.advanceTimersByTimeAsync(ENDING_POLL_GAPS_MS[0]);

            // The background re-read is in flight and the list stays on
            // screen: the page's placeholder is exactly `@if (loading())`.
            expect(threadListReads(mockHttp)).toBe(2);
            expect(component.loading()).toBe(false);
            expect(component.threads().map(thread => thread.id)).toEqual(['t-end']);

            inFlight.next({threads: [makeThread({id: 't-end', status: 'ended'})]});
            inFlight.complete();
            await vi.advanceTimersByTimeAsync(0);

            expect(component.threads()[0].status).toBe('ended');
            expect(component.loading()).toBe(false);
        });

        it('re-reads a retirement-pending card and drops it once the server no longer lists it', async () => {
            const kept = makeThread({id: 't-keep', status: 'active'});
            serveThreadLists(
                mockHttp,
                [kept, makeThread({id: 't-gone', status: 'active', runtime_retirement_pending: true})],
                [kept],
            );

            component.ngOnInit();
            await vi.advanceTimersByTimeAsync(0);
            expect(component.threads().find(thread => thread.id === 't-gone')?.status).toBe('ending');
            const loadingWrites = vi.spyOn(component.loading, 'set');

            await vi.advanceTimersByTimeAsync(ENDING_POLL_GAPS_MS[0]);

            expect(threadListReads(mockHttp)).toBe(2);
            expect(component.threads().map(thread => thread.id)).toEqual(['t-keep']);
            expect(loadingWrites).not.toHaveBeenCalledWith(true);
            expect(component.loading()).toBe(false);
        });

        // Guard (holds before and after the fix): the poll exists only for
        // `ending` cards; an ordinary list is read once.
        it('does not poll when no card is ending', async () => {
            serveThreadLists(mockHttp, [
                makeThread({id: 't-active', status: 'active'}),
                makeThread({id: 't-ended', status: 'ended'}),
            ]);

            component.ngOnInit();
            await vi.advanceTimersByTimeAsync(120_000);

            expect(threadListReads(mockHttp)).toBe(1);
            expect(vi.getTimerCount()).toBe(0);
        });

        it('stops polling once no card is ending', async () => {
            serveThreadLists(
                mockHttp,
                [makeThread({id: 't-end', status: 'ending'})],
                [makeThread({id: 't-end', status: 'ending'})],
                [makeThread({id: 't-end', status: 'ended'})],
            );

            component.ngOnInit();
            await vi.advanceTimersByTimeAsync(0);
            await vi.advanceTimersByTimeAsync(ENDING_POLL_GAPS_MS[0]);
            expect(threadListReads(mockHttp)).toBe(2);
            await vi.advanceTimersByTimeAsync(ENDING_POLL_GAPS_MS[1]);
            expect(threadListReads(mockHttp)).toBe(3);
            expect(component.threads()[0].status).toBe('ended');

            expect(vi.getTimerCount()).toBe(0);
            await vi.advanceTimersByTimeAsync(120_000);
            expect(threadListReads(mockHttp)).toBe(3);
        });

        it('stops polling when the page is destroyed', async () => {
            serveThreadLists(mockHttp, [makeThread({id: 't-end', status: 'ending'})]);

            component.ngOnInit();
            await vi.advanceTimersByTimeAsync(0);
            expect(vi.getTimerCount()).toBe(1);

            destroyPage();

            expect(vi.getTimerCount()).toBe(0);
            await vi.advanceTimersByTimeAsync(120_000);
            expect(threadListReads(mockHttp)).toBe(1);
        });

        it('does not re-arm from a re-read that lands after the page is destroyed', async () => {
            const inFlight = new Subject<unknown>();
            serveThreadLists(mockHttp, [makeThread({id: 't-end', status: 'ending'})], inFlight);

            component.ngOnInit();
            await vi.advanceTimersByTimeAsync(0);
            await vi.advanceTimersByTimeAsync(ENDING_POLL_GAPS_MS[0]);
            expect(threadListReads(mockHttp)).toBe(2);

            destroyPage();
            inFlight.next({threads: [makeThread({id: 't-end', status: 'ending'})]});
            inFlight.complete();
            await vi.advanceTimersByTimeAsync(120_000);

            expect(vi.getTimerCount()).toBe(0);
            expect(threadListReads(mockHttp)).toBe(2);
        });

        // R1 follow-up: a stateless retirement left pending by a fenced
        // End/Delete makes no progress until the user retries End, Delete or
        // Resume (each re-reads the list itself), so it never arms the poll.
        it('does not poll for a stateless pending retirement', async () => {
            serveThreadLists(mockHttp, [
                makeThread({
                    id: 't-stateless',
                    status: 'active',
                    execution_lane: 'stateless',
                    runtime_retirement_pending: true,
                    retirement_disposition: 'ended',
                    retirement_permanent: true,
                }),
            ]);

            component.ngOnInit();
            await vi.advanceTimersByTimeAsync(0);
            expect(component.threads()[0].status).toBe('ending');
            await vi.advanceTimersByTimeAsync(120_000);

            expect(threadListReads(mockHttp)).toBe(1);
            expect(vi.getTimerCount()).toBe(0);
        });

        // Guard (holds before and after): a pinned ending card beside it
        // still polls exactly as before.
        it('keeps polling for a pinned ending card beside a stateless pending one', async () => {
            serveThreadLists(mockHttp, [
                makeThread({
                    id: 't-stateless',
                    status: 'active',
                    execution_lane: 'stateless',
                    runtime_retirement_pending: true,
                    retirement_permanent: false,
                }),
                makeThread({
                    id: 't-pinned',
                    status: 'active',
                    execution_lane: 'pinned',
                    runtime_retirement_pending: true,
                    retirement_permanent: false,
                }),
            ]);

            component.ngOnInit();
            await vi.advanceTimersByTimeAsync(0);
            await vi.advanceTimersByTimeAsync(ENDING_POLL_GAPS_MS[0]);

            expect(threadListReads(mockHttp)).toBe(2);
            expect(vi.getTimerCount()).toBe(1);
        });

        it('backs off 2 s → 4 s → 8 s → 15 s and holds the 15 s cap', async () => {
            serveThreadLists(mockHttp, [makeThread({id: 't-end', status: 'ending'})]);

            component.ngOnInit();
            await vi.advanceTimersByTimeAsync(0);

            let reads = 1;
            for (const gap of ENDING_POLL_GAPS_MS) {
                await vi.advanceTimersByTimeAsync(gap - 1);
                expect(threadListReads(mockHttp)).toBe(reads);
                await vi.advanceTimersByTimeAsync(1);
                expect(threadListReads(mockHttp)).toBe(++reads);
            }
        });
    });

    describe('openSession()', () => {
        it('should navigate without POSTing /resume even for ended threads', () => {
            const thread = makeThread({id: 'thread-end', status: 'ended'});
            component.openSession(thread);

            expect(mockRouter.navigate).toHaveBeenCalledWith(['/sessions', 'thread-end']);
            const resumeCall = mockHttp.post.mock.calls.find(
                (c: any[]) => c[0]?.includes('/resume'),
            );
            expect(resumeCall).toBeUndefined();
        });
    });

    describe('talkToOfficer()', () => {
        it('navigates to the conference launcher for the thread project', () => {
            const thread = makeThread({
                id: 'officer-1',
                project_id: 'proj-9',
                metadata: {config_override: {officer: {enabled: true}}},
            });
            component.talkToOfficer(thread);
            expect(mockRouter.navigate).toHaveBeenCalledWith(['/projects', 'proj-9', 'officer', 'conference']);
        });

        it('does nothing without a project', () => {
            component.talkToOfficer(makeThread({project_id: null}));
            expect(mockRouter.navigate).not.toHaveBeenCalled();
        });

        it('only officer rows offer it', () => {
            expect(component.canTalk(makeThread({project_id: 'p'}))).toBe(false);
            expect(
                component.canTalk(
                    makeThread({project_id: 'p', metadata: {config_override: {officer: {enabled: true}}}}),
                ),
            ).toBe(true);
            expect(
                component.canTalk(
                    makeThread({project_id: 'p', metadata: {config_override: {officer: {conference: true}}}}),
                ),
            ).toBe(false);
        });
    });

    // =========================================================================
    // 9.2.6: toggleProject() / isProjectSelected()
    // =========================================================================

    describe('project selection', () => {
        it('should add project to selection', () => {
            component.toggleProject('proj-1');
            expect(component.selectedProjectIds()).toContain('proj-1');
            expect(component.isProjectSelected('proj-1')).toBe(true);
        });

        it('should remove project from selection on second toggle', () => {
            component.toggleProject('proj-1');
            component.toggleProject('proj-1');
            expect(component.selectedProjectIds()).not.toContain('proj-1');
            expect(component.isProjectSelected('proj-1')).toBe(false);
        });

        it('should handle multiple project selections', () => {
            component.toggleProject('proj-1');
            component.toggleProject('proj-2');
            expect(component.selectedProjectIds()).toEqual(['proj-1', 'proj-2']);
        });
    });

    // =========================================================================
    // 9.2.8: returnToActive()
    // =========================================================================

    describe('returnToActive()', () => {
        it('should navigate to thread when threadId exists', () => {
            mockChat.threadId = () => 'thread-abc';
            component.returnToActive();

            expect(mockRouter.navigate).toHaveBeenCalledWith(['/sessions', 'thread-abc']);
        });

        it('should not navigate when threadId is null', () => {
            mockChat.threadId = () => null;
            component.returnToActive();

            expect(mockRouter.navigate).not.toHaveBeenCalled();
        });
    });
});

/** Keys straight through: the card is asserted on its bindings, not its copy. */
@Pipe({name: 'transloco', standalone: true})
class TranslocoStubPipe implements PipeTransform {
    transform(key: string): string {
        return key;
    }
}

@Pipe({name: 'translocoDate', standalone: true})
class TranslocoDateStubPipe implements PipeTransform {
    transform(value: unknown): string {
        return String(value ?? '');
    }
}

// =============================================================================
// R1.B12: a permanent Delete fenced by a retryable 503 keeps a retry path on
// the card. Rendered, because the defect is the Delete control's `disabled`
// binding on an `ending` card. The design-system children are stubbed as
// custom elements, so a binding is read back as an element property and an
// `(clicked)` output is driven with a `clicked` DOM event.
// =============================================================================

describe('SessionsPageComponent (rendered): a fenced permanent delete', () => {
    type IconButton = HTMLElement & {disabled: boolean; tooltip: string; ariaLabel: string};
    let mocks: ReturnType<typeof createMocks>;

    beforeAll(async () => {
        // The real children carry styleUrl resources JIT cannot fetch; they
        // are stubbed below, but TestBed still resolves them on import.
        await ɵresolveComponentResources(() => Promise.resolve(''));
    });

    beforeEach(() => {
        mocks = createMocks();
        TestBed.configureTestingModule({
            imports: [SessionsPageComponent],
            providers: mocks.providers,
        });
        TestBed.overrideComponent(SessionsPageComponent, {
            set: {
                imports: [TranslocoStubPipe, TranslocoDateStubPipe, TitleCasePipe],
                schemas: [CUSTOM_ELEMENTS_SCHEMA],
            },
        });
    });

    afterEach(() => {
        TestBed.resetTestingModule();
        vi.clearAllMocks();
    });

    /** Let the page's HTTP promise chains settle, then re-render. */
    async function settle(fixture: {detectChanges(): void}): Promise<void> {
        await new Promise(resolve => setTimeout(resolve, 0));
        fixture.detectChanges();
    }

    function card(host: HTMLElement, id: string): HTMLElement {
        return host.querySelector(`[data-thread-id="${id}"]`) as HTMLElement;
    }

    function deleteButton(host: HTMLElement, id: string): IconButton {
        return card(host, id).querySelector('app-icon-button[variant="danger"]') as IconButton;
    }

    it('keeps Delete enabled as a retry on the ending card, re-sends it on click, and drops the retry on a 200', async () => {
        const live = makeThread({id: 't-del', status: 'active'});
        // What the list reports once the fenced delete has begun retirement.
        const retiring = makeThread({id: 't-del', status: 'active', runtime_retirement_pending: true});
        serveThreadLists(mocks.mockHttp, [live], [retiring]);
        mocks.mockHttp.delete
            .mockReturnValueOnce(throwError(() => new HttpErrorResponse({
                status: 503,
                error: {detail: 'Terminal workspace authority advanced; retry retirement'},
            })))
            .mockReturnValueOnce(of({status: 'ending'}));

        const fixture = TestBed.createComponent(SessionsPageComponent);
        const component = fixture.componentInstance;
        const host = fixture.nativeElement as HTMLElement;
        fixture.detectChanges();
        await settle(fixture);
        expect(deleteButton(host, 't-del').disabled).toBe(false);

        deleteButton(host, 't-del').dispatchEvent(new CustomEvent('clicked'));
        expect(component.confirmDeleteOpen()).toBe(true);
        await component.confirmDelete();
        // The user's reload: the card now shows the pending retirement.
        await component.loadThreads();
        await settle(fixture);

        expect(card(host, 't-del').classList.contains('ending')).toBe(true);
        expect(deleteButton(host, 't-del').disabled).toBe(false);
        expect(deleteButton(host, 't-del').tooltip).toBe('sessions.tooltip.retryDelete');
        expect(deleteButton(host, 't-del').ariaLabel).toBe('sessions.tooltip.retryDelete');
        expect(mocks.mockToast.warning).toHaveBeenCalledWith('errors.sessions.deleteRetryable');
        expect(mocks.mockToast.danger).not.toHaveBeenCalled();

        // The retry re-sends the permanent delete the user already confirmed.
        deleteButton(host, 't-del').dispatchEvent(new CustomEvent('clicked'));
        await settle(fixture);

        expect(component.confirmDeleteOpen()).toBe(false);
        expect(mocks.mockHttp.delete).toHaveBeenCalledTimes(2);
        expect(mocks.mockHttp.delete.mock.calls[1][0]).toContain(
            '/persistent/threads/t-del?permanent=true',
        );
        expect(mocks.mockHttp.delete.mock.calls[1][0]).not.toContain('force=true');
        // A 200 hands the delete to the server; the card is a plain ending
        // card again until the poll sees it gone.
        expect(card(host, 't-del').classList.contains('ending')).toBe(true);
        expect(deleteButton(host, 't-del').disabled).toBe(true);
        expect(deleteButton(host, 't-del').tooltip).toBe('sessions.tooltip.delete');
    });

    it('retries a fenced force delete as a force delete, and drops the retry once the card leaves ending', async () => {
        const live = makeThread({id: 't-live', status: 'active'});
        const retiring = makeThread({id: 't-live', status: 'active', runtime_retirement_pending: true});
        serveThreadLists(mocks.mockHttp, [live], [retiring]);
        const fenced = () => throwError(() => new HttpErrorResponse({
            status: 503,
            error: {detail: 'Terminal workspace successor authority is not yet safe'},
        }));
        mocks.mockHttp.delete
            .mockReturnValueOnce(throwError(() => new HttpErrorResponse({
                status: 409,
                error: {detail: {code: 'turn_in_flight'}},
            })))
            .mockReturnValueOnce(fenced())
            .mockReturnValueOnce(fenced());

        const fixture = TestBed.createComponent(SessionsPageComponent);
        const component = fixture.componentInstance;
        const host = fixture.nativeElement as HTMLElement;
        fixture.detectChanges();
        await settle(fixture);

        deleteButton(host, 't-live').dispatchEvent(new CustomEvent('clicked'));
        await component.confirmDelete();
        expect(component.confirmForceOpen()).toBe(true);
        await component.confirmForceDelete();
        await component.loadThreads();
        await settle(fixture);

        expect(card(host, 't-live').classList.contains('ending')).toBe(true);
        expect(deleteButton(host, 't-live').disabled).toBe(false);

        // A retry that meets the fence again keeps the retry, and keeps force.
        deleteButton(host, 't-live').dispatchEvent(new CustomEvent('clicked'));
        await settle(fixture);

        expect(component.confirmForceOpen()).toBe(false);
        expect(mocks.mockHttp.delete).toHaveBeenCalledTimes(3);
        expect(mocks.mockHttp.delete.mock.calls[2][0]).toContain(
            '/persistent/threads/t-live?permanent=true&force=true',
        );
        expect(deleteButton(host, 't-live').disabled).toBe(false);
        expect(deleteButton(host, 't-live').tooltip).toBe('sessions.tooltip.retryDelete');

        // The retirement settles: the card leaves `ending`, the retry goes,
        // and Delete is the ordinary confirmed action again.
        serveThreadLists(mocks.mockHttp, [makeThread({id: 't-live', status: 'ended'})]);
        await component.loadThreads();
        await settle(fixture);

        expect(deleteButton(host, 't-live').disabled).toBe(false);
        expect(deleteButton(host, 't-live').tooltip).toBe('sessions.tooltip.delete');
        deleteButton(host, 't-live').dispatchEvent(new CustomEvent('clicked'));
        expect(component.confirmDeleteOpen()).toBe(true);
        expect(mocks.mockHttp.delete).toHaveBeenCalledTimes(3);
    });

    it('drops the retry when a retried delete fails for a reason that is not the fence', async () => {
        const live = makeThread({id: 't-del', status: 'active'});
        const retiring = makeThread({id: 't-del', status: 'active', runtime_retirement_pending: true});
        serveThreadLists(mocks.mockHttp, [live], [retiring]);
        mocks.mockHttp.delete
            .mockReturnValueOnce(throwError(() => new HttpErrorResponse({
                status: 503,
                error: {detail: 'Terminal workspace authority advanced; retry retirement'},
            })))
            .mockReturnValueOnce(throwError(() => new HttpErrorResponse({
                status: 409,
                error: {detail: {code: 'pinned_retirement_conflict'}},
            })));

        const fixture = TestBed.createComponent(SessionsPageComponent);
        const component = fixture.componentInstance;
        const host = fixture.nativeElement as HTMLElement;
        fixture.detectChanges();
        await settle(fixture);

        deleteButton(host, 't-del').dispatchEvent(new CustomEvent('clicked'));
        await component.confirmDelete();
        await component.loadThreads();
        await settle(fixture);
        expect(deleteButton(host, 't-del').disabled).toBe(false);

        deleteButton(host, 't-del').dispatchEvent(new CustomEvent('clicked'));
        await settle(fixture);

        expect(mocks.mockHttp.delete).toHaveBeenCalledTimes(2);
        expect(mocks.mockToast.danger).toHaveBeenCalledOnce();
        expect(component.confirmForceOpen()).toBe(false);
        expect(deleteButton(host, 't-del').disabled).toBe(true);
        expect(deleteButton(host, 't-del').tooltip).toBe('sessions.tooltip.delete');
    });

    // =========================================================================
    // R1 follow-up: a stateless retirement left pending by a fenced End or
    // Delete is finished only by the user's retry. The owner projection says
    // which retry the server accepts: `retirement_permanent` → Delete (the
    // same already-confirmed permanent request), otherwise → Resume. A pinned
    // retirement finishes on the server, so its card is unchanged.
    // =========================================================================

    function resumeButton(host: HTMLElement, id: string): IconButton {
        return [...card(host, id).querySelectorAll('app-icon-button')].find(
            button => (button as IconButton).ariaLabel === 'sessions.tooltip.resume',
        ) as IconButton;
    }

    const statelessPending = (id: string, permanent: boolean) => makeThread({
        id,
        status: 'active',
        execution_lane: 'stateless',
        runtime_retirement_pending: true,
        retirement_disposition: 'ended',
        retirement_permanent: permanent,
    });

    it('offers Delete as the retry on a reloaded stateless permanent-pending card, without a second confirmation', async () => {
        // A fresh page: no in-memory retry was armed in this browser session.
        serveThreadLists(mocks.mockHttp, [statelessPending('t-perm', true)]);
        mocks.mockHttp.delete.mockReturnValueOnce(of({status: 'deleted'}));

        const fixture = TestBed.createComponent(SessionsPageComponent);
        const component = fixture.componentInstance;
        const host = fixture.nativeElement as HTMLElement;
        fixture.detectChanges();
        await settle(fixture);

        expect(card(host, 't-perm').classList.contains('ending')).toBe(true);
        expect(deleteButton(host, 't-perm').disabled).toBe(false);
        expect(deleteButton(host, 't-perm').tooltip).toBe('sessions.tooltip.retryDelete');
        expect(resumeButton(host, 't-perm').disabled).toBe(true);

        deleteButton(host, 't-perm').dispatchEvent(new CustomEvent('clicked'));
        await settle(fixture);

        expect(component.confirmDeleteOpen()).toBe(false);
        expect(mocks.mockHttp.delete).toHaveBeenCalledTimes(1);
        expect(mocks.mockHttp.delete.mock.calls[0][0]).toContain(
            '/persistent/threads/t-perm?permanent=true',
        );
        expect(mocks.mockHttp.delete.mock.calls[0][0]).not.toContain('force=true');
    });

    it('keeps Resume enabled as the retry on a stateless soft-pending card, with Delete still closed', async () => {
        serveThreadLists(mocks.mockHttp, [statelessPending('t-soft', false)]);
        mocks.mockHttp.post.mockReturnValueOnce(of({status: 'created'}));

        const fixture = TestBed.createComponent(SessionsPageComponent);
        const host = fixture.nativeElement as HTMLElement;
        fixture.detectChanges();
        await settle(fixture);

        expect(card(host, 't-soft').classList.contains('ending')).toBe(true);
        expect(resumeButton(host, 't-soft').disabled).toBe(false);
        expect(deleteButton(host, 't-soft').disabled).toBe(true);

        resumeButton(host, 't-soft').dispatchEvent(new CustomEvent('clicked'));
        await settle(fixture);

        expect(mocks.mockHttp.post).toHaveBeenCalledTimes(1);
        expect(mocks.mockHttp.post.mock.calls[0][0]).toContain('/persistent/threads/t-soft/resume');
        expect(mocks.mockRouter.navigate).toHaveBeenCalledWith(['/sessions', 't-soft']);
    });

    // Guard (holds before and after): the server finishes a pinned
    // retirement by itself, so neither Resume nor Delete opens on its card —
    // whatever its permanence.
    it('leaves a pinned ending card with Resume and Delete disabled', async () => {
        serveThreadLists(mocks.mockHttp, [
            makeThread({
                id: 't-pin-soft',
                status: 'active',
                execution_lane: 'pinned',
                runtime_retirement_pending: true,
                retirement_disposition: 'ended',
                retirement_permanent: false,
            }),
            makeThread({
                id: 't-pin-perm',
                status: 'active',
                execution_lane: 'pinned',
                runtime_retirement_pending: true,
                retirement_disposition: 'ended',
                retirement_permanent: true,
            }),
        ]);

        const fixture = TestBed.createComponent(SessionsPageComponent);
        const host = fixture.nativeElement as HTMLElement;
        fixture.detectChanges();
        await settle(fixture);

        for (const id of ['t-pin-soft', 't-pin-perm']) {
            expect(card(host, id).classList.contains('ending')).toBe(true);
            expect(resumeButton(host, id).disabled).toBe(true);
            expect(deleteButton(host, id).disabled).toBe(true);
            expect(deleteButton(host, id).tooltip).toBe('sessions.tooltip.delete');
        }
        fixture.destroy();
    });
});
