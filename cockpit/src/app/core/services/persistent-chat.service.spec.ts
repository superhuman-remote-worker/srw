import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { Injector, NgZone, PLATFORM_ID, runInInjectionContext, signal } from '@angular/core';
import Dexie from 'dexie';
import { TestBed } from '@angular/core/testing';
import { HttpClient } from '@angular/common/http';
import { NEVER, from, of, Subject, throwError } from 'rxjs';
import { TranslocoService } from '@jsverse/transloco';
import {
  PersistentChatService,
  historyToTurns,
  cloudCountFromSummary,
  describeAppliedConfig,
} from './persistent-chat.service';
import { ApiService } from './api.service';
import { CapabilitiesService } from './capabilities.service';
import { IndexedDbService } from './indexed-db.service';
import { NotificationService } from './notification.service';
import { AppToastService } from '../../ui/toast';
import {
  AssistantTurn,
  isAssistantTurn,
  isSystemTurn,
  isToolCall,
  isUserTurn,
  TextEvent,
  ToolCallEvent,
  UserTurn,
} from '../models/turn.model';
import { ThreadCloudDiffSummary } from '../models/api.model';
import { UploadStatus } from '../models/file.model';
import { PersistentThreadTransportBridge } from './persistent-thread-transport-bridge.service';
import { CanvasService } from './canvas.service';

const SESSION_RUNTIME_GENERATION = '55555555-5555-4555-8555-555555555555';
const SESSION_RUNTIME_GENERATION_B = '66666666-6666-4666-8666-666666666666';

// ---------------------------------------------------------------------------
// Test scaffolding
// ---------------------------------------------------------------------------

/**
 * EventSource mock — captures the URL, exposes hooks for triggering open,
 * message, gone_beyond_horizon, and error events, and tracks readyState
 * transitions so tests can exercise the transient-vs-terminal error split.
 */
interface MockEventSource {
  url: string;
  readyState: number;
  close: ReturnType<typeof vi.fn>;
  onopen: ((e: any) => void) | null;
  onmessage: ((e: MessageEvent) => void) | null;
  onerror: ((e: any) => void) | null;
  listeners: Record<string, ((e: any) => void)[]>;
}

function createMockEventSource(): MockEventSource {
  return {
    url: '',
    readyState: 0, // CONNECTING
    close: vi.fn(),
    onopen: null,
    onmessage: null,
    onerror: null,
    listeners: {},
  };
}

function fireSseOpen(es: MockEventSource): void {
  es.readyState = 1; // OPEN
  es.onopen?.({});
}

function fireSseMessage(
  es: MockEventSource,
  frame: Record<string, unknown>,
  lastEventId = '',
): void {
  es.onmessage?.({
    data: JSON.stringify(frame),
    lastEventId,
  } as MessageEvent);
}

function fireSseNamedEvent(
  es: MockEventSource,
  name: string,
  frame: Record<string, unknown>,
  lastEventId = '',
): void {
  const handlers = es.listeners[name] || [];
  handlers.forEach((h) => h({ data: JSON.stringify(frame), lastEventId } as MessageEvent));
}

function fireSseTransientError(es: MockEventSource): void {
  // Browser is retrying — readyState moves back to CONNECTING.
  es.readyState = 0;
  es.onerror?.({});
}

function fireSseTerminalError(es: MockEventSource): void {
  es.readyState = 2; // CLOSED
  es.onerror?.({});
}

/**
 * WebSocket mock for the control plane.
 */
function createMockWs() {
  const ws: any = {
    readyState: 1, // OPEN
    send: vi.fn(),
    close: vi.fn(),
    onopen: null,
    onmessage: null,
    onclose: null,
    onerror: null,
    addEventListener: vi.fn((event: string, cb: any) => {
      if (event === 'open') ws._openCb = cb;
    }),
    removeEventListener: vi.fn(),
  };
  return ws;
}

/**
 * Build the service in a minimal injection context. Returns the spies the
 * tests assert against, plus a hook to capture every constructed
 * EventSource so handlers can be driven by tests.
 */
function createService(
  opts: {
    cursor?: { epoch: number; seq: number } | null;
    cache?: IndexedDbService;
  } = {},
) {
  const mockHttp: any = {
    get: vi.fn().mockReturnValue(of({ messages: [], total: 0 })),
    post: vi.fn().mockReturnValue(of({})),
    patch: vi.fn().mockReturnValue(of({ status: 'updated' })),
    delete: vi.fn().mockReturnValue(of({})),
  };

  const mockApi: any = {
    uploadOneToThread: vi.fn().mockReturnValue(of({ kind: 'done', files: [] })),
    deleteThreadUpload: vi.fn().mockReturnValue(of(undefined)),
    humanizeUploadError: vi.fn().mockReturnValue('upload failed'),
    // Durable queue block (stateless_turn_resilience.md step 2): the awaiting
    // poll and the owner retry must never throw inside a timer in tests that
    // don't care about them.
    getThreadQueue: vi.fn().mockReturnValue(of(null)),
    retryThreadQueue: vi.fn().mockReturnValue(of({ kind: 'ok', state: 'queued' })),
  };

  const mockCache: any = opts.cache ?? {
    getThreadCursor: vi.fn().mockResolvedValue(opts.cursor ?? null),
    setThreadCursor: vi.fn().mockResolvedValue(undefined),
    deleteThreadCursor: vi.fn().mockResolvedValue(undefined),
    getThreadMessages: vi.fn().mockResolvedValue([]),
    getNewestCachedCreatedAt: vi.fn().mockResolvedValue(null),
    upsertThreadMessages: vi.fn().mockResolvedValue(undefined),
    clearThreadMessages: vi.fn().mockResolvedValue(undefined),
  };

  // NgZone stub: just run callbacks synchronously. Tests don't depend on
  // change-detection scheduling, only on signal mutations being observed.
  // NOTE: we don't provide this as the NgZone token because Angular's
  // ChangeDetectionScheduler subscribes to NgZone's onStable/onMicrotaskEmpty
  // streams. TestBed's default NgZoneNoop satisfies that contract; this stub
  // is unused in the TestBed configuration but kept for reference.
  const mockZone: any = {
    run: <T>(fn: () => T) => fn(),
  };
  void mockZone;

  const mockToast: any = {
    show: vi.fn(),
    info: vi.fn(),
    success: vi.fn(),
    warning: vi.fn(),
    danger: vi.fn(),
    dismiss: vi.fn(),
    dismissAll: vi.fn(),
  };

  const sseInstances: MockEventSource[] = [];

  function MockEventSourceCtor(this: any, url: string, _init?: EventSourceInit) {
    const es = createMockEventSource();
    es.url = url;
    // Patch addEventListener so the service's gone_beyond_horizon
    // subscription becomes a fireable handler in tests.
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

  const wsInstances: any[] = [];
  function MockWebSocketCtor(this: any, url: string) {
    const ws = createMockWs();
    ws.url = url;
    wsInstances.push(ws);
    return ws as any;
  }
  (MockWebSocketCtor as any).OPEN = 1;
  (MockWebSocketCtor as any).CONNECTING = 0;
  (globalThis as any).WebSocket = MockWebSocketCtor;

  // Minimal NotificationService stub — just the lifecycleEvent signal
  // the PersistentChatService constructor effect reads. Tests fire phase
  // transitions by setting this signal directly.
  const mockNotifications: any = {
    lifecycleEvent: signal<{
      thread_id: string;
      state: string;
      reason?: string;
      session_runtime_generation?: string;
    } | null>(null),
    cloudDiffStagedEvent: signal<{
      thread_id: string;
      session_runtime_generation: string;
      staged_epoch: number;
      file_count: number;
      counts: { added: number; modified: number; deleted: number };
      mount_id: string;
    } | null>(null),
  };

  // TestBed gives us the ChangeDetectionScheduler that effect() needs.
  // Manual Injector.create() doesn't wire that up. We let TestBed use
  // its default NgZone — the scheduler subscribes to its lifecycle
  // streams, and a thin stub would break that subscription.
  TestBed.resetTestingModule();
  TestBed.configureTestingModule({
    providers: [
      { provide: HttpClient, useValue: mockHttp },
      { provide: ApiService, useValue: mockApi },
      {
        provide: CapabilitiesService,
        useValue: {
          datasourceScopeAutoAttachAvailable: () => true,
          datasourceScopeAutoAttachAvailability$: of(true),
        },
      },
      { provide: IndexedDbService, useValue: mockCache },
      { provide: AppToastService, useValue: mockToast },
      { provide: NotificationService, useValue: mockNotifications },
      { provide: TranslocoService, useValue: { translate: (k: string) => k } },
      PersistentChatService,
    ],
  });
  const service = TestBed.inject(PersistentChatService);
  const threadTransport = TestBed.inject(PersistentThreadTransportBridge);
  const canvas = TestBed.inject(CanvasService);
  return {
    service,
    threadTransport,
    canvas,
    mockHttp,
    mockApi,
    mockCache,
    sseInstances,
    wsInstances,
    notifications: mockNotifications,
  };
}

/** One internally consistent response object for older tests that mock every
 * connect-time GET with one function. Each endpoint reads only its own fields. */
function activeSessionGet(url: string) {
  const threadId = url.match(/\/(?:persistent\/threads|sessions)\/([^/?]+)/)?.[1] ?? 'thread';
  return of({
    status: 'active',
    total_turns: 0,
    messages: [],
    total: 0,
    citations: [],
    thread_id: threadId,
    permission_mode: 'supervised',
    narration_mode: 'auto',
    turn_count: 0,
    turn_in_flight: false,
    message_count: 0,
    model: null,
    temperature: null,
    running_tool: null,
    pending_permissions: [],
    event_cursor: { epoch: 0, seq: 0 },
    replay_cursor: { epoch: 0, seq: 0 },
    snapshot_source: 'durable_journal',
    state: 'ready',
    control_socket: 'websocket',
    ws_url: 'ws://agent.test',
    token: 'test-token',
    expires_at: 0,
    pinned_runtime_generation_contract: 1,
    session_runtime_generation: SESSION_RUNTIME_GENERATION,
  });
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('cloudCountFromSummary', () => {
  // Protected cloud mode (Slice C, Task 14): the badge count is the sum of
  // the staged added/modified/deleted counts.
  const summary = (counts: {
    added: number;
    modified: number;
    deleted: number;
  }): ThreadCloudDiffSummary => ({
    thread_id: 't1',
    epoch: 3,
    staged_at: '2026-07-12T00:00:00Z',
    counts,
    protected_mount: '/mnt/project',
    files: [],
  });

  it('returns 0 for a null summary (nothing loaded / not protected)', () => {
    expect(cloudCountFromSummary(null)).toBe(0);
  });

  it('sums added + modified + deleted', () => {
    expect(cloudCountFromSummary(summary({ added: 2, modified: 1, deleted: 4 }))).toBe(7);
  });

  it('returns 0 for an all-zero counts summary (empty staging)', () => {
    expect(cloudCountFromSummary(summary({ added: 0, modified: 0, deleted: 0 }))).toBe(0);
  });
});

describe('PersistentChatService — protected cloud probe', () => {
  const summary = (over: Record<string, unknown> = {}) => ({
    thread_id: 't1',
    epoch: 5,
    staged_at: '2026-08-24T09:18:00Z',
    counts: { added: 1, modified: 2, deleted: 1 },
    protected_mount: 'cloud',
    files: [],
    ...over,
  });

  it('records a successful probe and its summary facts', async () => {
    const { service, mockApi } = createService();
    mockApi.getThreadCloudDiffOutcome = vi
      .fn()
      .mockReturnValue(of({ kind: 'ok', data: summary() }));
    service.threadId.set('t1');
    await service.refreshCloudDiffCount();
    expect(service.cloudDiffProbe()).toBe('ready');
    expect(service.cloudChangesCount()).toBe(4);
    expect(service.protectedMountName()).toBe('cloud');
  });

  it('distinguishes a failed probe from "nothing staged"', async () => {
    // Both used to render as no banner at all, which left a protected ended
    // session with no entry point to the review and no way to ask again.
    const { service, mockApi } = createService();
    mockApi.getThreadCloudDiffOutcome = vi
      .fn()
      .mockReturnValue(of({ kind: 'error', status: 0, detail: 'offline' }));
    service.threadId.set('t1');
    await service.refreshCloudDiffCount();
    expect(service.cloudDiffProbe()).toBe('error');
  });

  it('keeps the previous count on a failed probe', async () => {
    // A transient failure is not evidence that a staged diff went away.
    const { service, mockApi } = createService();
    mockApi.getThreadCloudDiffOutcome = vi
      .fn()
      .mockReturnValueOnce(of({ kind: 'ok', data: summary() }))
      .mockReturnValueOnce(of({ kind: 'error', status: 503, detail: 'busy' }));
    service.threadId.set('t1');
    await service.refreshCloudDiffCount();
    await service.refreshCloudDiffCount();
    expect(service.cloudChangesCount()).toBe(4);
    expect(service.cloudDiffProbe()).toBe('error');
  });

  it('ignores a probe that lands after a thread switch', async () => {
    const { service, mockApi } = createService();
    mockApi.getThreadCloudDiffOutcome = vi.fn().mockImplementation(() => {
      service.threadId.set('t2'); // the user moved on mid-request
      return of({ kind: 'ok', data: summary({ counts: { added: 9, modified: 0, deleted: 0 } }) });
    });
    service.threadId.set('t1');
    await service.refreshCloudDiffCount();
    expect(service.cloudChangesCount()).toBe(0);
  });

  it('does not let an older same-thread probe erase a newer staged summary', async () => {
    const { service, mockApi } = createService();
    const old = new Subject<any>();
    const fresh = new Subject<any>();
    mockApi.getThreadCloudDiffOutcome = vi.fn().mockReturnValueOnce(old).mockReturnValueOnce(fresh);
    service.threadId.set('t1');

    const oldRead = service.refreshCloudDiffCount();
    const freshRead = service.refreshCloudDiffCount();
    fresh.next({ kind: 'ok', data: summary() });
    fresh.complete();
    await freshRead;
    old.next({
      kind: 'ok',
      data: summary({
        staged_at: null,
        counts: { added: 0, modified: 0, deleted: 0 },
      }),
    });
    old.complete();
    await oldRead;

    expect(service.cloudChangesCount()).toBe(4);
    expect(service.cloudStagedAt()).toBe('2026-08-24T09:18:00Z');
  });

  it('refreshes from the authoritative summary on a matching live stage notification', async () => {
    const { service, mockApi, notifications } = createService();
    mockApi.getThreadCloudDiffOutcome = vi
      .fn()
      .mockReturnValue(of({ kind: 'ok', data: summary() }));
    service.threadId.set('t1');
    (service as any).intentionalClose = false;
    (service as any).sessionRuntimeGeneration = SESSION_RUNTIME_GENERATION;

    notifications.cloudDiffStagedEvent.set({
      thread_id: 't1',
      session_runtime_generation: SESSION_RUNTIME_GENERATION,
      staged_epoch: 5,
      // Deliberately disagree with the endpoint. The event is a wake-up edge,
      // not reviewed summary authority.
      file_count: 999,
      counts: { added: 999, modified: 0, deleted: 0 },
      mount_id: 'reader-1',
    });
    TestBed.tick();
    await Promise.resolve();
    await Promise.resolve();

    expect(mockApi.getThreadCloudDiffOutcome).toHaveBeenCalledTimes(1);
    expect(service.cloudChangesCount()).toBe(4);
  });

  it('offers a project folder only once it matches the protected mount', async () => {
    // PC-19: the frontend cannot see cloud_handle, so its candidate mount is
    // only provisional. The summary's protected_mount is what proves it.
    const { service, mockApi } = createService();
    mockApi.getThreadCloudDiffOutcome = vi
      .fn()
      .mockReturnValue(of({ kind: 'ok', data: summary() }));
    service.threadId.set('t1');
    await service.refreshCloudDiffCount();

    service.protectedFolderLink.set({
      url: 'https://cloud.example.invalid/apps/files/?dir=/Docs',
      name: 'Protected Docs',
      targetPath: 'somewhere-else',
    });
    expect(service.verifiedProjectFolder()).toBeNull();

    service.protectedFolderLink.set({
      url: 'https://cloud.example.invalid/apps/files/?dir=/Docs',
      name: 'Protected Docs',
      targetPath: 'cloud',
    });
    expect(service.verifiedProjectFolder()?.name).toBe('Protected Docs');
  });

  it('offers no project folder before a summary has been read', () => {
    const { service } = createService();
    service.protectedFolderLink.set({
      url: 'https://cloud.example.invalid/apps/files/?dir=/Docs',
      name: 'Protected Docs',
      targetPath: 'cloud',
    });
    expect(service.verifiedProjectFolder()).toBeNull();
  });
});

describe('describeAppliedConfig', () => {
  it('uses connector terminology for live attachment changes', () => {
    expect(describeAppliedConfig({}, { added: ['GitHub'], removed: ['Analytics'] })).toEqual([
      'connector "GitHub" attached',
      'connector "Analytics" detached',
    ]);
  });
});

describe('PersistentChatService — initial state', () => {
  it('starts disconnected with default signals', () => {
    const { service } = createService();
    expect(service.connectionState()).toBe('disconnected');
    expect(service.isConnected()).toBe(false);
    expect(service.threadId()).toBeNull();
    expect(service.turns()).toEqual([]);
    expect(service.currentStreamingTurn()).toBeNull();
    expect(service.isStreaming()).toBe(false);
    expect(service.historyLoaded()).toBe(false);
    expect(service.permissionMode()).toBe('supervised');
    expect(service.narrationMode()).toBe('auto');
    expect(service.reconnectAttempt()).toBe(0);
    expect(service.reconnectGaveUp()).toBe(false);
    expect(service.cloudSyncDegraded()).toBe(false);
  });
});

describe('PersistentChatService — render windowing', () => {
  function makeTurns(n: number) {
    return Array.from({ length: n }, (_, i) => ({
      kind: 'user' as const,
      id: `u${i}`,
      content: `m${i}`,
      timestamp: i,
    }));
  }

  function seed(service: PersistentChatService, n: number): void {
    service.conversation.set({
      threadId: 't-window',
      turns: makeTurns(n),
      activeAssistantTurnId: null,
    });
  }

  it('renders all turns when under the window size', () => {
    const { service } = createService();
    seed(service, 30);
    expect(service.visibleTurns().length).toBe(30);
    expect(service.hasOlderTurns()).toBe(false);
  });

  it('renders only the most recent window when over the size', () => {
    const { service } = createService();
    seed(service, 120);
    const visible = service.visibleTurns();
    expect(visible.length).toBe(50);
    expect(visible[0].id).toBe('u70'); // slice(-50) of 120
    expect(visible[49].id).toBe('u119');
    expect(service.hasOlderTurns()).toBe(true);
  });

  it('loadOlderTurns widens the window by the step, capped at length', () => {
    const { service } = createService();
    seed(service, 120);
    service.loadOlderTurns();
    expect(service.visibleTurns().length).toBe(100);
    expect(service.hasOlderTurns()).toBe(true);
    service.loadOlderTurns(); // 150 → capped at 120
    expect(service.visibleTurns().length).toBe(120);
    expect(service.hasOlderTurns()).toBe(false);
  });

  it('growWindow anchors the visible top by the delta', () => {
    const { service } = createService();
    seed(service, 120);
    const topBefore = service.visibleTurns()[0].id; // u70
    seed(service, 123); // 3 more turns present
    service.growWindow(3);
    const visible = service.visibleTurns();
    expect(visible.length).toBe(53);
    expect(visible[0].id).toBe(topBefore); // visible top unchanged
  });

  it('resetWindow re-bounds to the default window', () => {
    const { service } = createService();
    seed(service, 120);
    service.loadOlderTurns();
    expect(service.visibleTurns().length).toBe(100);
    service.resetWindow();
    expect(service.visibleTurns().length).toBe(50);
  });
});

describe('PersistentChatService — message cache (loadHistory)', () => {
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

  it('restores the complete conversation when a legacy cache first receives versioned history', async () => {
    for (const name of ['cockpit-cache', 'srw-thread-history-v2']) await Dexie.delete(name);
    const cache = runInInjectionContext(
      Injector.create({ providers: [{ provide: PLATFORM_ID, useValue: 'browser' }] }),
      () => new IndexedDbService(),
    );
    const { service, mockHttp } = createService({ cache });
    const threadId = 'legacy-to-versioned';
    const rows = [
      {
        id: 'earlier-user',
        threadId,
        role: 'human',
        content: 'Earlier question',
        tool_calls: null,
        turn_number: 1,
        created_at: '2026-09-07T08:00:00Z',
      },
      {
        id: 'earlier-answer',
        threadId,
        role: 'ai',
        content: 'Earlier answer',
        tool_calls: null,
        turn_number: 1,
        created_at: '2026-09-07T08:01:00Z',
      },
      {
        id: 'latest-answer',
        threadId,
        role: 'ai',
        content: 'Latest answer',
        tool_calls: null,
        turn_number: 17,
        created_at: '2026-09-16T14:57:13Z',
      },
    ];
    try {
      await vi.waitFor(() => expect(cache.isReady()).toBe(true));
      await cache.upsertThreadMessages(rows);
      mockHttp.get.mockImplementation((url: string) => {
        if (url.includes('/messages')) {
          const messages = url.includes('after=') ? rows.slice(-1) : rows;
          return of({
            messages,
            total: messages.length,
            events_epoch: 0,
            conversation_revision: 0,
          });
        }
        return activeSessionGet(url);
      });
      await service.connect(threadId);
      expect(service.turns().map((t) => t.id)).toEqual([
        'earlier-user',
        'earlier-answer',
        'latest-answer',
      ]);
      expect((await cache.getThreadMessages(threadId)).map((m) => m.id)).toEqual([
        'earlier-user',
        'earlier-answer',
        'latest-answer',
      ]);
    } finally {
      service.disconnect();
      TestBed.resetTestingModule();
      const resources = cache as unknown as {
        db: Dexie;
        historyDb: Dexie;
        historyChannel: BroadcastChannel | null;
      };
      resources.db.close();
      resources.historyDb.close();
      resources.historyChannel?.close();
      for (const name of ['cockpit-cache', 'srw-thread-history-v2']) await Dexie.delete(name);
    }
  });

  it('full-loads when nothing is cached (no ?after=) and caches the result', async () => {
    const { service, mockHttp, mockCache } = createService();
    mockHttp.get.mockImplementation((url: string) => {
      if (url.includes('/messages')) {
        return of({
          messages: [
            {
              id: 'm1',
              role: 'human',
              content: 'hi',
              tool_calls: null,
              turn_number: 1,
              created_at: '2026-05-15T08:00:00Z',
            },
          ],
          total: 1,
        });
      }
      return of({ status: 'active', total_turns: 1 });
    });

    await service.connect('thread-nocache');

    const msgUrls = mockHttp.get.mock.calls
      .map((c: any) => c[0] as string)
      .filter((u: string) => u.includes('/messages'));
    expect(msgUrls.length).toBeGreaterThan(0);
    expect(msgUrls.every((u: string) => !u.includes('after='))).toBe(true);
    expect(mockCache.upsertThreadMessages).toHaveBeenCalled(); // cached for next time
    expect(service.turns().length).toBe(1);
  });

  it('paints cached history first, then refreshes incrementally via ?after=', async () => {
    const { service, mockHttp, mockCache } = createService();
    mockCache.getThreadMessages.mockResolvedValue([
      {
        id: 'm1',
        threadId: 'thread-cache',
        role: 'human',
        content: 'old',
        tool_calls: null,
        turn_number: 1,
        created_at: '2026-05-15T08:00:00Z',
      },
    ]);
    mockHttp.get.mockImplementation((url: string) => {
      if (url.includes('/messages')) {
        // Server returns one NEW message after the cached cursor.
        return of({
          messages: [
            {
              id: 'm2',
              role: 'ai',
              content: 'new reply',
              tool_calls: null,
              turn_number: 2,
              created_at: '2026-05-15T08:01:00Z',
            },
          ],
          total: 2,
        });
      }
      return of({ status: 'active', total_turns: 2 });
    });

    await service.connect('thread-cache');

    const msgUrls = mockHttp.get.mock.calls
      .map((c: any) => c[0] as string)
      .filter((u: string) => u.includes('/messages'));
    expect(msgUrls.some((u: string) => u.includes('after='))).toBe(true);
    expect(mockCache.upsertThreadMessages).toHaveBeenCalled();
    // Merged cached + fetched: user (m1) then assistant (m2).
    const turns = service.turns();
    expect(turns.length).toBe(2);
    expect(turns[0].kind).toBe('user');
    expect(turns[1].kind).toBe('assistant');
  });

  it('renders a role="summary" history row as a compaction banner', async () => {
    const { service, mockHttp } = createService();
    mockHttp.get.mockImplementation((url: string) => {
      if (url.includes('/messages')) {
        return of({
          messages: [
            {
              id: 'u1',
              role: 'human',
              content: 'hi',
              tool_calls: null,
              turn_number: 1,
              created_at: '2026-05-15T08:00:00Z',
            },
            {
              id: 's1',
              role: 'summary',
              content: 'We discussed X and Y.',
              tool_calls: null,
              turn_number: 2,
              created_at: '2026-05-15T08:01:00Z',
            },
          ],
          total: 2,
        });
      }
      return of({ status: 'active', total_turns: 2 });
    });

    await service.connect('thread-summary');

    const banner = service.turns().find((t: { kind: string }) => t.kind === 'compaction');
    expect(banner).toBeTruthy();
    expect((banner as { summary: string }).summary).toBe('We discussed X and Y.');
  });

  it('renders a mid-turn summary row as an inline event at its position', async () => {
    // The assistant turn anchors at its first row; a summary row that
    // falls inside the turn must render IN the event stream, not as a
    // top-level divider trailing the whole turn's content.
    const { service, mockHttp } = createService();
    mockHttp.get.mockImplementation((url: string) => {
      if (url.includes('/messages')) {
        return of({
          messages: [
            {
              id: 'u1',
              role: 'human',
              content: 'go',
              tool_calls: null,
              turn_number: 5,
              created_at: '2026-05-15T08:00:00Z',
            },
            {
              id: 'a1',
              role: 'ai',
              content: 'working on it',
              tool_calls: null,
              turn_number: 5,
              created_at: '2026-05-15T08:00:10Z',
            },
            {
              id: 's1',
              role: 'summary',
              content: 'recap text',
              tool_calls: null,
              turn_number: 5,
              created_at: '2026-05-15T08:00:20Z',
            },
            {
              id: 'a2',
              role: 'ai',
              content: 'final answer',
              tool_calls: null,
              turn_number: 5,
              created_at: '2026-05-15T08:00:30Z',
            },
          ],
          total: 4,
        });
      }
      return of({ status: 'active', total_turns: 5 });
    });

    await service.connect('thread-midturn-summary');

    const turns = service.turns();
    expect(turns.some((t: { kind: string }) => t.kind === 'compaction')).toBe(false);
    const assistant = turns.find((t: { kind: string }) => t.kind === 'assistant') as any;
    expect(assistant).toBeTruthy();
    const kinds = assistant.events.map((e: { kind: string }) => e.kind);
    expect(kinds).toEqual(['text', 'compaction', 'text']);
    expect(assistant.events[1].summary).toBe('recap text');
  });

  it('collapses consecutive duplicate summary rows into one banner', async () => {
    // Threads written before the run-counter gate carry repeated
    // role='summary' rows with identical content (duplicate-banner bug).
    const { service, mockHttp } = createService();
    mockHttp.get.mockImplementation((url: string) => {
      if (url.includes('/messages')) {
        return of({
          messages: [
            {
              id: 'u1',
              role: 'human',
              content: 'hi',
              tool_calls: null,
              turn_number: 1,
              created_at: '2026-05-15T08:00:00Z',
            },
            {
              id: 's1',
              role: 'summary',
              content: 'same recap',
              tool_calls: null,
              turn_number: 2,
              created_at: '2026-05-15T08:01:00Z',
            },
            {
              id: 's2',
              role: 'summary',
              content: 'same recap',
              tool_calls: null,
              turn_number: 2,
              created_at: '2026-05-15T08:01:30Z',
            },
            {
              id: 's3',
              role: 'summary',
              content: 'same recap',
              tool_calls: null,
              turn_number: 2,
              created_at: '2026-05-15T08:02:00Z',
            },
          ],
          total: 4,
        });
      }
      return of({ status: 'active', total_turns: 2 });
    });

    await service.connect('thread-dup-summaries');

    const banners = service.turns().filter((t: { kind: string }) => t.kind === 'compaction');
    expect(banners.length).toBe(1);
  });
});

describe('PersistentChatService — connect()', () => {
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

  it('loads transcript history + thread meta via REST before opening SSE', async () => {
    const { service, mockHttp, sseInstances } = createService();
    mockHttp.get.mockImplementation((url: string) => {
      if (url.endsWith('/messages')) {
        return of({ messages: [], total: 0 });
      }
      return of({ status: 'active', title: 'My session', total_turns: 0 });
    });

    await service.connect('thread-A');

    // Both REST endpoints were called.
    const urls = mockHttp.get.mock.calls.map((c: any) => c[0]);
    expect(urls.some((u: string) => u.endsWith('/persistent/threads/thread-A/messages'))).toBe(
      true,
    );
    expect(urls.some((u: string) => u.endsWith('/persistent/threads/thread-A'))).toBe(true);
    // Then SSE.
    expect(sseInstances).toHaveLength(1);
    expect(sseInstances[0].url).toContain('/persistent/threads/thread-A/stream');
    expect(service.historyLoaded()).toBe(true);
  });

  it('rehydrates thinking + tool results from history (migration 0011)', async () => {
    const { service, mockHttp } = createService();
    mockHttp.get.mockImplementation((url: string) => {
      if (url.endsWith('/messages')) {
        return of({
          messages: [
            {
              id: 'u1',
              role: 'human',
              content: 'Read the file',
              tool_calls: null,
              turn_number: 1,
              created_at: '2026-05-15T08:00:00Z',
            },
            {
              id: 'a1',
              role: 'ai',
              content: null,
              tool_calls: [{ name: 'read_file', args: { path: 'x' }, id: 'tc-1' }],
              turn_number: 1,
              thinking: 'I should read the file first.',
              created_at: '2026-05-15T08:00:01Z',
            },
            {
              id: 't1',
              role: 'tool',
              content: 'file contents here',
              tool_calls: null,
              turn_number: 1,
              tool_call_id: 'tc-1',
              created_at: '2026-05-15T08:00:02Z',
            },
            {
              id: 'a2',
              role: 'ai',
              content: 'Here is what I found.',
              tool_calls: null,
              turn_number: 1,
              created_at: '2026-05-15T08:00:03Z',
            },
          ],
          total: 4,
        });
      }
      return of({ status: 'active', total_turns: 1 });
    });

    await service.connect('thread-hist');

    const turns = service.turns();
    // user turn + one collapsed assistant turn
    const assistant = turns.find(isAssistantTurn) as AssistantTurn;
    expect(assistant).toBeDefined();
    expect(assistant.turnNumber).toBe(1);
    // Events arrive in order: thought, tool_call (now with result), text
    const kinds = assistant.events.map((e) => e.kind);
    expect(kinds).toEqual(['thought', 'tool_call', 'text']);
    const tool = assistant.events.find(isToolCall) as ToolCallEvent;
    expect(tool.result).toBe('file contents here');
    expect(tool.status).toBe('completed');
    expect(tool.resultStatus).toBe('ok');
    const thought = assistant.events[0] as any;
    expect(thought.content).toBe('I should read the file first.');
    // Keyed by the AI row id so a replayed reasoning frame for the same
    // message dedupes against this rendered bubble.
    expect(thought.messageId).toBe('a1');
  });

  it('rehydrates an unanswered gate as not-run, never as completed or denied', async () => {
    // knowledge-history/done/supervised_parallel_gates_timeout_fabricates_denial.md — the
    // backend settles an unanswered gate as 'expired' | 'interrupted' |
    // 'unavailable'. History used to paint everything but 'denied'/'expired'
    // as completed, so a reload claimed tools had run that never did.
    const { service, mockHttp } = createService();
    mockHttp.get.mockImplementation((url: string) => {
      if (url.endsWith('/messages')) {
        return of({
          messages: [
            {
              id: 'a1',
              role: 'ai',
              content: null,
              tool_calls: [
                { name: 'web_search', args: {}, id: 'tc-ok' },
                { name: 'web_search', args: {}, id: 'tc-yes', decision: 'approved' },
                { name: 'web_search', args: {}, id: 'tc-no', decision: 'denied' },
                { name: 'web_search', args: {}, id: 'tc-ttl', decision: 'expired' },
                { name: 'web_search', args: {}, id: 'tc-stop', decision: 'interrupted' },
                { name: 'web_search', args: {}, id: 'tc-err', decision: 'unavailable' },
              ],
              turn_number: 1,
              created_at: '2026-05-15T08:00:01Z',
            },
          ],
          total: 1,
        });
      }
      return of({ status: 'active', total_turns: 1 });
    });

    await service.connect('thread-gates');

    const assistant = service.turns().find(isAssistantTurn) as AssistantTurn;
    const byId = new Map(
      assistant.events.filter(isToolCall).map((e) => [e.id, e as ToolCallEvent]),
    );
    // Un-gated and approved calls really did run.
    expect(byId.get('tc-ok')!.status).toBe('completed');
    expect(byId.get('tc-yes')!.status).toBe('completed');
    // A real refusal stays a refusal.
    expect(byId.get('tc-no')!.status).toBe('denied');
    // Every non-decision renders as not-run — and none of them as a denial.
    for (const id of ['tc-ttl', 'tc-stop', 'tc-err']) {
      expect(byId.get(id)!.status).toBe('expired');
    }
  });

  it('dedupes a replayed thinking frame against the rendered history bubble', async () => {
    // knowledge-base/knowledge/issues/persistent_chat_reasoning_after_answer_and_replay_duplication.md
    // After a cold connect paints the completed turn, the SSE replay cursor
    // can re-emit the trailing reasoning frame (gemma journals it after the
    // token run). It must not spawn a second `recovered:` thought bubble.
    const { service, mockHttp, sseInstances } = createService();
    mockHttp.get.mockImplementation((url: string) => {
      if (url.endsWith('/messages')) {
        return of({
          messages: [
            {
              id: 'a1',
              role: 'ai',
              content: 'The answer is 42.',
              tool_calls: null,
              turn_number: 1,
              thinking: 'Let me reason about this.',
              created_at: '2026-05-15T08:00:01Z',
            },
          ],
          total: 1,
        });
      }
      return of({ status: 'active', total_turns: 1 });
    });

    await service.connect('thread-replay');
    fireSseOpen(sseInstances[0]);

    // Replay re-emits just the reasoning frame, keyed to the same row id.
    fireSseMessage(
      sseInstances[0],
      { method: 'thinking', params: { content: 'Let me reason about this.', message_id: 'a1' } },
      '1:1',
    );

    const assistantTurns = service.turns().filter(isAssistantTurn) as AssistantTurn[];
    expect(assistantTurns).toHaveLength(1);
    expect(assistantTurns[0].recovered).not.toBe(true);
    const thoughts = assistantTurns[0].events.filter((e) => e.kind === 'thought');
    expect(thoughts).toHaveLength(1);
  });

  it('rejoins a persisted in-flight prefix when the reattach welcome frame arrives', async () => {
    const { service, mockHttp, sseInstances } = createService({
      cursor: { epoch: 2, seq: 40 },
    });
    mockHttp.get.mockImplementation((url: string) => {
      if (url.endsWith('/messages')) {
        return of({
          messages: [
            {
              id: 'u7',
              role: 'human',
              content: 'Revise the draft',
              tool_calls: null,
              turn_number: 7,
              created_at: '2026-08-05T14:00:00Z',
            },
            {
              id: 'a7-prefix',
              role: 'ai',
              content: 'I will apply those revisions now.',
              tool_calls: null,
              turn_number: 7,
              created_at: '2026-08-05T14:00:01Z',
            },
          ],
          total: 2,
        });
      }
      return of({ status: 'active', total_turns: 7 });
    });
    await service.connect('thread-midturn');
    fireSseOpen(sseInstances[0]);

    // Replay begins after the cached turn.started frame, producing the
    // recovered suffix seen below "SESSION RESUMED" in the regression.
    fireSseMessage(
      sseInstances[0],
      { method: 'token', params: { content: 'The matrix is updated.' } },
      '2:41',
    );
    (service as any)._flushDeltas();
    expect(service.turns().filter(isAssistantTurn)).toHaveLength(2);

    (service as any)._handleEvent({
      method: 'session.state',
      params: { turn_count: 7, turn_in_flight: true },
    });

    const assistants = service.turns().filter(isAssistantTurn) as AssistantTurn[];
    expect(assistants).toHaveLength(1);
    expect(assistants[0].id).toBe('7');
    expect(assistants[0].status).toBe('streaming');
    expect(assistants[0].historical).toBe(true);
    expect(assistants[0].events.map((event) => (event as TextEvent).content)).toEqual([
      'I will apply those revisions now.',
      'The matrix is updated.',
    ]);
    expect(service.currentStreamingTurn()?.id).toBe('7');

    (service as any)._handleEvent({
      method: 'turn.completed',
      params: { turn_id: 7 },
    });
    expect(service.turns().filter(isAssistantTurn)).toHaveLength(1);
    expect((service.turns().find(isAssistantTurn) as AssistantTurn).status).toBe('done');
  });

  it('closes a retained open turn when the durable snapshot proves the runtime idle', async () => {
    // A turn whose terminal frame never arrived (the loop died in
    // settlement; the journal holds turn.started with no turn.completed)
    // stayed "generating" forever: the snapshot handler only ever reopened.
    const { service, sseInstances } = createService();
    await service.connect('thread-stranded');
    fireSseOpen(sseInstances[0]);
    fireSseMessage(sseInstances[0], { method: 'turn.started', params: { turn_id: 4 } }, '0:4303');
    fireSseMessage(sseInstances[0], { method: 'token', params: { content: 'Completed.' } }, '0:5519');
    (service as any)._flushDeltas();
    expect(service.isStreaming()).toBe(true);
    service.runningTool.set({ id: 'c1', tool: 'shell_execute', args: {} });

    (service as any)._handleEvent({
      method: 'session.state',
      params: {
        turn_count: 4,
        turn_in_flight: false,
        snapshot_source: 'durable_journal',
        event_cursor: { epoch: 0, seq: 5520 },
        replay_cursor: { epoch: 0, seq: 5520 },
      },
    });

    expect(service.isStreaming()).toBe(false);
    const turn = service.turns().find(isAssistantTurn) as AssistantTurn;
    expect(turn.status).toBe('interrupted');
    expect((turn.events[0] as TextEvent).content).toBe('Completed.');
    expect(service.runningTool()).toBeNull();
    // A pinned welcome frame (no durable source) keeps the historical
    // reopen-only behaviour: it is followed by the exact WS state.
    fireSseMessage(sseInstances[0], { method: 'turn.started', params: { turn_id: 5 } }, '0:5521');
    (service as any)._handleEvent({
      method: 'session.state',
      params: { turn_count: 5, turn_in_flight: false },
    });
    expect(service.isStreaming()).toBe(true);
  });

  it('measures agent silence from agent-origin frames, not snapshots or stream reopens', async () => {
    const { service, sseInstances } = createService();
    await service.connect('thread-quiet');
    fireSseOpen(sseInstances[0]);
    (service as any).agentLastEventAt = 1000;

    (service as any)._handleEvent({
      method: 'session.state',
      params: { turn_count: 4, turn_in_flight: false, snapshot_source: 'durable_journal' },
    });
    expect((service as any).agentLastEventAt).toBe(1000);

    // A stream reopen must not make a silent agent look freshly active.
    (service as any)._startSseWatchdog('thread-quiet');
    expect((service as any).agentLastEventAt).toBe(1000);
    (service as any)._stopSseWatchdog();

    (service as any)._handleEvent({ method: 'usage.updated', params: { turn: 4 } });
    expect((service as any).agentLastEventAt).toBeGreaterThan(1000);
  });

  it('passes the cached cursor as ?last_event_id=<epoch>:<seq> on initial open', async () => {
    const { service, mockHttp, sseInstances } = createService({
      cursor: { epoch: 7, seq: 42, threadId: 'thread-B', updatedAt: '' } as any,
    });
    mockHttp.get.mockImplementation(() =>
      of({ status: 'active', total_turns: 0, messages: [], total: 0 }),
    );

    await service.connect('thread-B');

    expect(sseInstances[0].url).toContain('last_event_id=7%3A42');
  });

  it('opens SSE without ?last_event_id= when no cursor is cached', async () => {
    const { service, mockHttp, sseInstances } = createService({ cursor: null });
    mockHttp.get.mockImplementation(() =>
      of({ status: 'active', total_turns: 0, messages: [], total: 0 }),
    );

    await service.connect('thread-C');

    expect(sseInstances[0].url).not.toContain('last_event_id');
  });

  it('opens an SSE-only review plane for ended threads and never opens control', async () => {
    const { service, mockHttp, sseInstances } = createService();
    mockHttp.get.mockImplementation((url: string) => {
      if (url.endsWith('/messages')) return of({ messages: [], total: 0 });
      return of({ status: 'ended', total_turns: 0 });
    });

    await service.connect('thread-ended');

    expect(sseInstances).toHaveLength(1);
    expect((service as any).controlWs).toBeNull();
    expect(service.connectionState()).toBe('disconnected');
    expect(service.threadStatus()).toBe('ended');
  });

  it('flips connectionState to connected on SSE open', async () => {
    const { service, mockHttp, sseInstances } = createService();
    mockHttp.get.mockImplementation(activeSessionGet);

    await service.connect('thread-D');
    expect(service.connectionState()).toBe('connecting');

    fireSseOpen(sseInstances[0]);
    expect(service.connectionState()).toBe('connected');
    expect(service.isConnected()).toBe(true);
    expect(service.error()).toBeNull();
  });

  it('does not let a slow stale connect reclaim a newer thread route', async () => {
    const { service, mockHttp, sseInstances, wsInstances } = createService();
    const slowAMeta = new Subject<Record<string, unknown>>();
    let markAMetaRequested: () => void = () => {};
    const aMetaRequested = new Promise<void>((resolve) => {
      markAMetaRequested = resolve;
    });

    mockHttp.get.mockImplementation((url: string) => {
      if (url.endsWith('/persistent/threads/thread-A/messages')) {
        return of({
          messages: [
            {
              id: 'a-user',
              role: 'human',
              content: 'history A',
              tool_calls: null,
              turn_number: 1,
              created_at: '2026-07-13T08:00:00Z',
            },
          ],
          total: 1,
        });
      }
      if (url.endsWith('/persistent/threads/thread-A')) {
        markAMetaRequested();
        return slowAMeta.asObservable();
      }
      if (url.endsWith('/persistent/threads/thread-B/messages')) {
        return of({
          messages: [
            {
              id: 'b-user',
              role: 'human',
              content: 'history B',
              tool_calls: null,
              turn_number: 1,
              created_at: '2026-07-13T08:01:00Z',
            },
          ],
          total: 1,
        });
      }
      if (url.endsWith('/persistent/threads/thread-B')) {
        return of({ status: 'active', title: 'Thread B', total_turns: 1 });
      }
      if (url.endsWith('/sessions/thread-B/connection')) {
        return of({ state: 'ready', ws_url: 'ws://thread-B' });
      }
      if (url.endsWith('/citations')) return of({ citations: [] });
      return of({ status: 'active' });
    });

    const connectA = service.connect('thread-A');
    await aMetaRequested;
    await service.connect('thread-B');

    // Resolve A only after B owns the state and both transports.
    slowAMeta.next({ status: 'active', title: 'Thread A', total_turns: 1 });
    slowAMeta.complete();
    await connectA;

    expect(service.threadId()).toBe('thread-B');
    expect(service.sessionTitle()).toBe('Thread B');
    expect(service.turns().map((turn) => turn.id)).toEqual(['b-user']);
    expect(sseInstances).toHaveLength(1);
    expect(sseInstances[0].url).toContain('/persistent/threads/thread-B/stream');
    expect(wsInstances).toHaveLength(1);
    expect(wsInstances[0].url).toBe('ws://thread-B');
  });

  it('lets the newer thread replace a stale control-WebSocket open', async () => {
    const { service, mockHttp, sseInstances, wsInstances } = createService();
    const slowAConnection = new Subject<Record<string, unknown>>();
    let markAConnectionRequested: () => void = () => {};
    const aConnectionRequested = new Promise<void>((resolve) => {
      markAConnectionRequested = resolve;
    });

    mockHttp.get.mockImplementation((url: string) => {
      if (url.endsWith('/messages')) return of({ messages: [], total: 0 });
      if (url.endsWith('/persistent/threads/thread-A')) {
        return of({ status: 'active', title: 'Thread A', total_turns: 0 });
      }
      if (url.endsWith('/persistent/threads/thread-B')) {
        return of({ status: 'active', title: 'Thread B', total_turns: 0 });
      }
      if (url.endsWith('/sessions/thread-A/connection')) {
        markAConnectionRequested();
        return slowAConnection.asObservable();
      }
      if (url.endsWith('/sessions/thread-B/connection')) {
        return of({ state: 'ready', ws_url: 'ws://thread-B' });
      }
      if (url.endsWith('/citations')) return of({ citations: [] });
      return of({ status: 'active' });
    });

    const connectA = service.connect('thread-A');
    await aConnectionRequested;
    await service.connect('thread-B');
    slowAConnection.next({ state: 'ready', ws_url: 'ws://thread-A' });
    slowAConnection.complete();
    await connectA;

    expect(service.threadId()).toBe('thread-B');
    expect(sseInstances).toHaveLength(2);
    expect(sseInstances[0].close).toHaveBeenCalled();
    expect(sseInstances[1].url).toContain('/persistent/threads/thread-B/stream');
    expect(wsInstances).toHaveLength(1);
    expect(wsInstances[0].url).toBe('ws://thread-B');
  });

  it('ignores a queued direct frame from the previous thread WebSocket', async () => {
    const { service, mockHttp, wsInstances, threadTransport } = createService();
    mockHttp.get.mockImplementation((url: string) => {
      if (url.endsWith('/messages')) return of({ messages: [], total: 0 });
      if (url.endsWith('/persistent/threads/thread-A')) {
        return of({ status: 'active', title: 'Thread A', total_turns: 0 });
      }
      if (url.endsWith('/persistent/threads/thread-B')) {
        return of({ status: 'active', title: 'Thread B', total_turns: 0 });
      }
      if (url.endsWith('/sessions/thread-A/connection')) {
        return of({ state: 'ready', ws_url: 'ws://thread-A' });
      }
      if (url.endsWith('/sessions/thread-B/connection')) {
        return of({ state: 'ready', ws_url: 'ws://thread-B' });
      }
      if (url.endsWith('/citations')) return of({ citations: [] });
      return of({ status: 'active' });
    });

    await service.connect('thread-A');
    const staleOnMessage = wsInstances[0].onmessage;
    const staleOnClose = wsInstances[0].onclose;
    await service.connect('thread-B');
    expect(wsInstances[0].onmessage).toBeNull();

    const invalidations: unknown[] = [];
    const subscription = threadTransport.canvasInvalidations$.subscribe((event) =>
      invalidations.push(event),
    );
    staleOnClose({ code: 1006, reason: 'late A close' } as CloseEvent);
    staleOnMessage({
      data: JSON.stringify({
        method: 'canvas.reconcile_required',
        params: { canvas_id: 'main', reason: 'stale-A-frame' },
      }),
    } as MessageEvent);

    expect(service.threadId()).toBe('thread-B');
    expect((service as any).controlWs).toBe(wsInstances[1]);
    expect(invalidations).toEqual([]);
    subscription.unsubscribe();
  });

  it('forwards Canvas SSE invalidations through the typed bridge without resetting chat', async () => {
    const { service, mockHttp, sseInstances, threadTransport } = createService();
    mockHttp.get.mockImplementation(() =>
      of({ status: 'active', total_turns: 0, messages: [], total: 0 }),
    );
    const invalidations: unknown[] = [];
    const subscription = threadTransport.canvasInvalidations$.subscribe((event) =>
      invalidations.push(event),
    );

    await service.connect('thread-canvas');
    fireSseMessage(sseInstances[0], {
      method: 'canvas.updated',
      params: {
        canvas_id: 'main',
        presentation_revision: 3,
        source_type: 'workspace_file',
        _seq: [1, 4],
      },
    });

    expect(invalidations).toEqual([
      expect.objectContaining({
        threadId: 'thread-canvas',
        method: 'canvas.updated',
        presentationRevision: 3,
      }),
    ]);
    expect(service.turns()).toEqual([]);
    subscription.unsubscribe();
  });

  it('keeps Canvas control sending on the existing thread WebSocket owner', async () => {
    const { service, mockHttp, wsInstances, threadTransport } = createService();
    mockHttp.get.mockImplementation(activeSessionGet);
    await service.connect('thread-canvas');

    const accepted = threadTransport.sendCanvasControl('thread-canvas', {
      method: 'canvas.source_updated',
      canvas_id: 'main',
      path: 'output/report.md',
      presentation_revision: 4,
      source_version: 'sha256:abc',
    });

    expect(accepted).toBe(true);
    expect(wsInstances).toHaveLength(1);
    expect(wsInstances[0].send).toHaveBeenCalledWith(
      JSON.stringify({
        method: 'canvas.source_updated',
        canvas_id: 'main',
        path: 'output/report.md',
        presentation_revision: 4,
        source_version: 'sha256:abc',
      }),
    );

    expect(
      threadTransport.sendCanvasControl('another-thread', {
        method: 'canvas.source_updated',
        canvas_id: 'main',
        path: 'output/report.md',
        presentation_revision: 4,
        source_version: 'sha256:abc',
      }),
    ).toBe(false);
    expect(wsInstances[0].send).toHaveBeenCalledTimes(1);
  });

  it('sends a committed presentation invalidation without file identity', async () => {
    const { service, mockHttp, wsInstances, threadTransport } = createService();
    mockHttp.get.mockImplementation(activeSessionGet);
    await service.connect('thread-canvas');

    const accepted = threadTransport.sendCanvasControl('thread-canvas', {
      method: 'canvas.presentation_updated',
      canvas_id: 'main',
      presentation_revision: 6,
    });

    expect(accepted).toBe(true);
    expect(wsInstances[0].send).toHaveBeenCalledWith(
      JSON.stringify({
        method: 'canvas.presentation_updated',
        canvas_id: 'main',
        presentation_revision: 6,
      }),
    );
  });

  it('truthfully rejects a Canvas control while the WebSocket is still connecting', () => {
    const { service, threadTransport } = createService();
    const connectingWs = createMockWs();
    connectingWs.readyState = WebSocket.CONNECTING;
    service.threadId.set('thread-canvas');
    (service as any).controlWs = connectingWs;

    const accepted = threadTransport.sendCanvasControl('thread-canvas', {
      method: 'canvas.source_updated',
      canvas_id: 'main',
      path: 'output/report.md',
      presentation_revision: 4,
      source_version: 'sha256:abc',
    });

    expect(accepted).toBe(false);
    expect(connectingWs.send).not.toHaveBeenCalled();
  });

  it('flushes only the latest committed source update when the control socket reconnects', () => {
    const { service, threadTransport, wsInstances } = createService();
    const connectingWs = createMockWs();
    connectingWs.readyState = WebSocket.CONNECTING;
    service.threadId.set('thread-canvas');
    (service as any).controlWs = connectingWs;

    // A newer REST mutation can complete before an older in-flight one.
    // The late response must not replace the queued latest revision.
    for (const revision of [5, 4]) {
      expect(
        threadTransport.sendCanvasControl('thread-canvas', {
          method: 'canvas.source_updated',
          canvas_id: 'main',
          path: 'output/report.md',
          presentation_revision: revision,
          source_version: `sha256:revision-${revision}`,
        }),
      ).toBe(false);
    }

    (service as any)._installControlWs('thread-canvas', 'ws://reconnected');
    const reconnected = wsInstances.at(-1);
    reconnected.onopen();

    expect(reconnected.send).toHaveBeenCalledTimes(1);
    expect(JSON.parse(reconnected.send.mock.calls[0][0])).toEqual({
      method: 'canvas.source_updated',
      canvas_id: 'main',
      path: 'output/report.md',
      presentation_revision: 5,
      source_version: 'sha256:revision-5',
    });

    // Duplicate browser open notifications cannot replay an accepted frame.
    reconnected.onopen();
    expect(reconnected.send).toHaveBeenCalledTimes(1);
  });

  it('forwards a direct reconcile-required control frame without an SSE sequence', async () => {
    const { service, mockHttp, wsInstances, threadTransport } = createService();
    mockHttp.get.mockImplementation(activeSessionGet);
    const invalidations: unknown[] = [];
    threadTransport.canvasInvalidations$.subscribe((event) => invalidations.push(event));
    await service.connect('thread-canvas');

    wsInstances[0].onmessage({
      data: JSON.stringify({
        method: 'canvas.reconcile_required',
        params: { canvas_id: 'main', reason: 'write_failed' },
      }),
    } as MessageEvent);

    expect(invalidations).toEqual([
      expect.objectContaining({
        threadId: 'thread-canvas',
        method: 'canvas.reconcile_required',
        presentationRevision: null,
      }),
    ]);
  });
});

describe('PersistentChatService — createAndConnect()', () => {
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

  it('clears prior session turns + threadId synchronously, before the POST resolves', async () => {
    const { service, mockHttp } = createService();

    // Seed a prior session so the bug reproduces: connect to thread-A,
    // then create a new thread. Without the synchronous reset in
    // createAndConnect(), thread-A's turns would still be visible
    // throughout the (potentially multi-second) POST + boot phase.
    mockHttp.get.mockImplementation((url: string) => {
      if (url.endsWith('/messages')) {
        return of({
          messages: [
            {
              id: 'u-prev',
              role: 'human',
              content: 'Hello from thread A',
              tool_calls: null,
              turn_number: 1,
              created_at: '2026-05-15T08:00:00Z',
            },
          ],
          total: 1,
        });
      }
      return of({ status: 'active', title: 'Old session', total_turns: 1 });
    });
    await service.connect('thread-A');
    expect(service.turns().length).toBeGreaterThan(0);
    expect(service.threadId()).toBe('thread-A');

    // Make the POST hang so we can observe the state during the await.
    let resolvePost: (v: any) => void = () => {};
    mockHttp.post.mockReturnValue({
      subscribe(observer: any) {
        resolvePost = (v) => {
          observer.next(v);
          observer.complete();
        };
        return { unsubscribe: () => {} };
      },
    });

    const promise = service.createAndConnect({ config_name: 'scholar' });

    // Synchronous part of createAndConnect must have already cleared
    // the prior session's content. Without the fix, turns would still
    // contain thread-A's user message.
    expect(service.turns()).toEqual([]);
    expect(service.threadId()).toBeNull();
    expect(service.isCreating()).toBe(true);
    expect(service.startupPhase()).toBe('creating');

    // Let the POST resolve so the test cleans up.
    mockHttp.get.mockImplementation((url: string) => {
      if (url.endsWith('/messages')) return of({ messages: [], total: 0 });
      return of({ status: 'active', total_turns: 0 });
    });
    resolvePost({ thread_id: 'thread-new' });
    await promise;
    expect(service.threadId()).toBe('thread-new');
  });

  it('does not connect a newly-created thread after the user navigates elsewhere', async () => {
    const { service, mockHttp, sseInstances } = createService();
    const createResponse = new Subject<{ thread_id: string }>();
    mockHttp.post.mockImplementation((url: string) =>
      url.endsWith('/persistent/threads') ? createResponse.asObservable() : of({}),
    );
    mockHttp.get.mockImplementation((url: string) => {
      if (url.endsWith('/messages')) return of({ messages: [], total: 0 });
      if (url.endsWith('/persistent/threads/thread-B')) {
        return of({ status: 'active', title: 'Thread B', total_turns: 0 });
      }
      if (url.endsWith('/sessions/thread-B/connection')) {
        return of({ state: 'ready', ws_url: 'ws://thread-B' });
      }
      if (url.endsWith('/citations')) return of({ citations: [] });
      return of({ status: 'active' });
    });

    const create = service.createAndConnect({ config_name: 'scholar' });
    await service.connect('thread-B');
    createResponse.next({ thread_id: 'thread-created' });
    createResponse.complete();

    await expect(create).resolves.toBe('thread-created');
    expect(service.threadId()).toBe('thread-B');
    expect(service.sessionTitle()).toBe('Thread B');
    expect(sseInstances).toHaveLength(1);
    expect(sseInstances[0].url).toContain('/persistent/threads/thread-B/stream');
  });
});

describe('PersistentChatService — resume navigation safety', () => {
  it('does not reconnect the resumed thread after navigation to another thread', async () => {
    const { service, mockHttp, sseInstances } = createService();
    const resumeResponse = new Subject<Record<string, never>>();
    service.threadId.set('thread-A');
    mockHttp.post.mockImplementation((url: string) =>
      url.endsWith('/persistent/threads/thread-A/resume') ? resumeResponse.asObservable() : of({}),
    );
    mockHttp.get.mockImplementation((url: string) => {
      if (url.endsWith('/messages')) return of({ messages: [], total: 0 });
      if (url.endsWith('/persistent/threads/thread-B')) {
        return of({ status: 'active', title: 'Thread B', total_turns: 0 });
      }
      if (url.endsWith('/sessions/thread-B/connection')) {
        return of({ state: 'ready', ws_url: 'ws://thread-B' });
      }
      if (url.endsWith('/citations')) return of({ citations: [] });
      return of({ status: 'active' });
    });

    const resume = service.resumeSession();
    await service.connect('thread-B');
    resumeResponse.next({});
    resumeResponse.complete();
    await resume;

    expect(service.threadId()).toBe('thread-B');
    expect(service.sessionTitle()).toBe('Thread B');
    expect(sseInstances).toHaveLength(1);
    expect(sseInstances[0].url).toContain('/persistent/threads/thread-B/stream');
  });

  it('does not surface error or pendingDrift for a resume failure on an abandoned thread', async () => {
    // Fix round 1, Finding 1: a late failure for a thread the user has
    // already navigated away from must not mutate the shared
    // error/pendingDrift signals — they are current-thread-scoped.
    // Reachable by ordinary navigation: click Resume on thread A,
    // immediately click thread B, thread A's request 403/428s late.
    // Without the currency guard at the top of the catch, thread-B's
    // view would show a resume error/dialog for a session it never
    // touched.
    const { service, mockHttp, sseInstances } = createService();
    const resumeResponse = new Subject<Record<string, never>>();
    service.threadId.set('thread-A');
    mockHttp.post.mockImplementation((url: string) =>
      url.endsWith('/persistent/threads/thread-A/resume') ? resumeResponse.asObservable() : of({}),
    );
    mockHttp.get.mockImplementation((url: string) => {
      if (url.endsWith('/messages')) return of({ messages: [], total: 0 });
      if (url.endsWith('/persistent/threads/thread-B')) {
        return of({ status: 'active', title: 'Thread B', total_turns: 0 });
      }
      if (url.endsWith('/sessions/thread-B/connection')) {
        return of({ state: 'ready', ws_url: 'ws://thread-B' });
      }
      if (url.endsWith('/citations')) return of({ citations: [] });
      return of({ status: 'active' });
    });

    const resume = service.resumeSession();
    await service.connect('thread-B'); // user navigates away mid-resume
    resumeResponse.error({
      status: 428,
      error: {
        detail: {
          code: 'config_drift',
          drift: [
            { id: 'connector:abc', kind: 'connector', reason: 'deleted', label: 'KurortEngine' },
          ],
        },
      },
    });
    await resume;

    expect(service.pendingDrift()).toBeNull();
    expect(service.error()).toBeNull();
    // Thread B's own connect must be unaffected by A's stale failure.
    expect(service.threadId()).toBe('thread-B');
    expect(sseInstances).toHaveLength(1);
    expect(sseInstances[0].url).toContain('/persistent/threads/thread-B/stream');
  });

  it("does not clear the current thread's pendingDrift when an abandoned resume succeeds late", async () => {
    // Fix round 1, Finding 2: the SUCCESS branch of resumeSession() used
    // to call pendingDrift.set(null) unconditionally, with no currency
    // guard, while the catch branch (test above) already had one. The
    // sibling success-path test above ('does not reconnect...') never
    // seeds pendingDrift, so it passes with or without this guard — this
    // test seeds it, so a regression back to the unconditional clear
    // fails it: a late SUCCESS for thread-A (abandoned) must not wipe
    // the drift dialog thread-B (current) is showing for its own,
    // unrelated resume.
    const { service, mockHttp, sseInstances } = createService();
    const resumeResponse = new Subject<Record<string, never>>();
    service.threadId.set('thread-A');
    mockHttp.post.mockImplementation((url: string) =>
      url.endsWith('/persistent/threads/thread-A/resume') ? resumeResponse.asObservable() : of({}),
    );
    mockHttp.get.mockImplementation((url: string) => {
      if (url.endsWith('/messages')) return of({ messages: [], total: 0 });
      if (url.endsWith('/persistent/threads/thread-B')) {
        return of({ status: 'active', title: 'Thread B', total_turns: 0 });
      }
      if (url.endsWith('/sessions/thread-B/connection')) {
        return of({ state: 'ready', ws_url: 'ws://thread-B' });
      }
      if (url.endsWith('/citations')) return of({ citations: [] });
      return of({ status: 'active' });
    });

    const resume = service.resumeSession();
    await service.connect('thread-B'); // user navigates away mid-resume
    // Thread B has its own, unrelated drift dialog up (connect()'s
    // cold-path reset already cleared the null it inherited from the
    // switch — this is thread-B's own, separately-populated state).
    const currentThreadsDrift = [
      {
        id: 'grant:shell_tools',
        kind: 'grant' as const,
        reason: 'revoked' as const,
        label: 'shell tools',
      },
    ];
    service.pendingDrift.set(currentThreadsDrift);
    resumeResponse.next({}); // thread-A's abandoned resume now succeeds
    resumeResponse.complete();
    await resume;

    expect(service.pendingDrift()).toEqual(currentThreadsDrift);
    expect(service.threadId()).toBe('thread-B');
    expect(sseInstances).toHaveLength(1);
    expect(sseInstances[0].url).toContain('/persistent/threads/thread-B/stream');
  });
});

describe('PersistentChatService — resume config drift', () => {
  it('sets pendingDrift from a 428 on the current thread and does not connect', async () => {
    const { service, mockHttp, sseInstances } = createService();
    service.threadId.set('thread-A');
    mockHttp.post.mockImplementation((url: string) =>
      url.endsWith('/persistent/threads/thread-A/resume')
        ? throwError(() => ({
            status: 428,
            error: {
              detail: {
                code: 'config_drift',
                drift: [
                  {
                    id: 'connector:abc',
                    kind: 'connector',
                    reason: 'deleted',
                    label: 'KurortEngine',
                  },
                ],
              },
            },
          }))
        : of({}),
    );

    await service.resumeSession();

    expect(service.pendingDrift()).toEqual([
      { id: 'connector:abc', kind: 'connector', reason: 'deleted', label: 'KurortEngine' },
    ]);
    expect(service.error()).toBeNull();
    // connect() must not run against a still-ended thread when drift blocks it.
    expect(sseInstances).toHaveLength(0);
  });

  it('does not treat typed session_not_ended as proof of a successor life', async () => {
    const { service, mockHttp, sseInstances } = createService();
    service.threadId.set('thread-A');
    mockHttp.post.mockImplementation((url: string) =>
      url.endsWith('/persistent/threads/thread-A/resume')
        ? throwError(() => ({
            status: 409,
            error: { detail: { code: 'session_not_ended' } },
          }))
        : of({}),
    );
    mockHttp.get.mockImplementation((url: string) => {
      if (url.endsWith('/messages')) return of({ messages: [], total: 0 });
      if (url.endsWith('/persistent/threads/thread-A')) {
        return of({ status: 'active', title: 'Thread A', total_turns: 0 });
      }
      if (url.endsWith('/sessions/thread-A/connection')) {
        return of({ state: 'ready', ws_url: 'ws://thread-A' });
      }
      if (url.endsWith('/citations')) return of({ citations: [] });
      return of({ status: 'active' });
    });

    await service.resumeSession();

    expect(service.error()).not.toBeNull();
    expect(service.pendingDrift()).toBeNull();
    expect(sseInstances).toHaveLength(0);
  });

  it('keeps terminal review live when Resume races pre-settlement End', async () => {
    vi.useFakeTimers();
    const ctx = createService();
    try {
      let staged = false;
      ctx.mockApi.getThreadCloudDiffOutcome = vi.fn().mockImplementation(() =>
        of({
          kind: 'ok',
          data: {
            thread_id: 'thread-ending',
            epoch: staged ? 10 : 9,
            staged_at: staged ? '2026-08-26T06:00:00Z' : null,
            protected_mount: 'Project cloud',
            counts: { added: staged ? 4 : 0, modified: 0, deleted: 0 },
            files: [],
          },
        }),
      );
      ctx.mockHttp.get.mockImplementation(activeSessionGet);
      ctx.mockHttp.post.mockImplementation((url: string) =>
        url.endsWith('/persistent/threads/thread-ending/resume')
          ? throwError(() => ({
              status: 409,
              error: { detail: { code: 'session_not_ended' } },
            }))
          : of({}),
      );

      await ctx.service.connect('thread-ending');
      (ctx.service as any)._protectedCloud.set(true);
      const endedSse = ctx.sseInstances[0];
      const oldWs = ctx.wsInstances[0];
      fireSseMessage(
        endedSse,
        {
          method: 'session.ended',
          params: { session_runtime_generation: SESSION_RUNTIME_GENERATION },
        },
        '9:42',
      );
      expect((ctx.service as any).terminalControlThreadId).toBe('thread-ending');
      expect(oldWs.close).toHaveBeenCalled();

      const connectionCallsBefore = ctx.mockHttp.get.mock.calls.filter((call: any[]) =>
        String(call[0]).endsWith('/api/sessions/thread-ending/connection'),
      ).length;
      await ctx.service.resumeSession();
      await Promise.resolve();

      expect(ctx.service.error()).toBe('errors.sessions.stillEnding');
      expect((ctx.service as any).terminalControlThreadId).toBe('thread-ending');
      expect((ctx.service as any).terminalCloudProbeThreadId).toBe('thread-ending');
      expect((ctx.service as any).resumedFromEpoch).toBeNull();
      expect(endedSse.close).not.toHaveBeenCalled();
      expect(ctx.wsInstances).toHaveLength(1);
      expect(
        ctx.mockHttp.get.mock.calls.filter((call: any[]) =>
          String(call[0]).endsWith('/api/sessions/thread-ending/connection'),
        ),
      ).toHaveLength(connectionCallsBefore);
      expect(ctx.service.cloudChangesCount()).toBe(0);

      staged = true;
      (ctx.service as any)._handleEvent({
        method: 'cloud.diff_staged',
        params: {
          thread_id: 'thread-ending',
          session_runtime_generation: SESSION_RUNTIME_GENERATION,
          staged_epoch: 10,
          file_count: 4,
          counts: { added: 4, modified: 0, deleted: 0 },
          mount_id: 'reader-1',
        },
      });
      await Promise.resolve();
      await Promise.resolve();

      expect(ctx.service.cloudChangesCount()).toBe(4);
      expect(ctx.service.cloudStagedAt()).toBe('2026-08-26T06:00:00Z');
      expect((ctx.service as any).terminalControlThreadId).toBe('thread-ending');
      expect(ctx.wsInstances).toHaveLength(1);
    } finally {
      ctx.service.disconnect();
      vi.useRealTimers();
    }
  });

  it('surfaces a protected-cloud 409 and preserves the ended SSE review plane', async () => {
    const ctx = createService();
    ctx.mockApi.getThreadCloudDiffOutcome = vi.fn().mockReturnValue(
      of({
        kind: 'ok',
        data: {
          thread_id: 'thread-protected',
          epoch: 2,
          staged_at: '2026-08-26T00:00:00Z',
          protected_mount: 'Project cloud',
          counts: { added: 2, modified: 1, deleted: 1 },
          files: [],
        },
      }),
    );
    ctx.mockHttp.get.mockImplementation((url: string) => {
      if (url.endsWith('/messages')) return of({ messages: [], total: 0 });
      if (url.endsWith('/persistent/threads/thread-protected')) {
        return of({
          status: 'ended',
          title: 'Protected',
          total_turns: 0,
          metadata: { protected_cloud: true },
        });
      }
      if (url.endsWith('/cloud/staged-summary')) {
        return of({ total: 4, files: [], protected_mount: 'Project cloud' });
      }
      if (url.endsWith('/citations')) return of({ citations: [] });
      return of({ status: 'ended' });
    });
    ctx.mockHttp.post.mockImplementation((url: string) =>
      url.endsWith('/persistent/threads/thread-protected/resume')
        ? throwError(() => ({
            status: 409,
            error: {
              detail: { code: 'protected_cloud_unsupported_session_class' },
            },
          }))
        : of({}),
    );

    await ctx.service.connect('thread-protected');
    ctx.service.cloudChangesCount.set(4);
    ctx.service.cloudDiffPanelOpen.set(true);
    const preservedSse = ctx.sseInstances[0];
    await ctx.service.resumeSession();

    expect(ctx.service.error()).not.toBeNull();
    expect(ctx.service.threadId()).toBe('thread-protected');
    expect(ctx.service.cloudChangesCount()).toBe(4);
    expect(ctx.service.cloudDiffPanelOpen()).toBe(true);
    expect(preservedSse.close).not.toHaveBeenCalled();
    expect(ctx.wsInstances).toHaveLength(0);
  });

  it('disconnect() clears a pending drift', () => {
    const { service } = createService();
    service.pendingDrift.set([
      { id: 'connector:abc', kind: 'connector', reason: 'deleted', label: 'KurortEngine' },
    ]);

    service.disconnect();

    expect(service.pendingDrift()).toBeNull();
  });
});

describe('PersistentChatService — SSE event dispatch', () => {
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

  async function setup() {
    const ctx = createService();
    ctx.mockHttp.get.mockImplementation(() =>
      of({ status: 'active', total_turns: 0, messages: [], total: 0 }),
    );
    await ctx.service.connect('thread-X');
    const es = ctx.sseInstances[0];
    fireSseOpen(es);
    return { ...ctx, es };
  }

  it('appends token frames into the active turn as a streaming TextEvent', async () => {
    const { service, es } = await setup();
    fireSseMessage(es, { method: 'turn.started', params: { turn_id: 1 } }, '1:1');
    fireSseMessage(es, { method: 'token', params: { content: 'Hello ' } }, '1:2');
    fireSseMessage(es, { method: 'token', params: { content: 'world' } }, '1:3');
    // Token deltas coalesce on an 80ms timer (Phase 4 de-flicker); wait for
    // the flush before asserting the rendered content.
    await new Promise((r) => setTimeout(r, 90));
    const turn = service.currentStreamingTurn();
    expect(turn).not.toBeNull();
    const text = turn!.events.find((e) => e.kind === 'text') as TextEvent;
    expect(text.content).toBe('Hello world');
    expect(text.status).toBe('streaming');
    expect(service.isStreaming()).toBe(true);
  });

  it('handles turn.completed by closing the active turn and clearing the streaming flag', async () => {
    const { service, es } = await setup();
    fireSseMessage(es, { method: 'turn.started', params: { turn_id: 1 } }, '1:1');
    fireSseMessage(es, { method: 'token', params: { content: 'done' } }, '1:2');
    fireSseMessage(es, { method: 'turn.completed', params: { turn_id: 1 } }, '1:3');
    expect(service.isStreaming()).toBe(false);
    expect(service.currentStreamingTurn()).toBeNull();
    const assistantTurns = service.turns().filter(isAssistantTurn);
    const last = assistantTurns[assistantTurns.length - 1] as AssistantTurn;
    expect(last.status).toBe('done');
    const text = last.events.find((e) => e.kind === 'text') as TextEvent;
    expect(text.content).toBe('done');
    expect(text.status).toBe('done');
  });

  it('drops the streaming reasoning bubble on thinking.reset (empty-response replace)', async () => {
    const { service, es } = await setup();
    fireSseMessage(es, { method: 'turn.started', params: { turn_id: 1 } }, '1:1');
    fireSseMessage(
      es,
      { method: 'thinking', params: { content: 'dead-end reasoning', message_id: 'a1' } },
      '1:2',
    );
    // thinking deltas coalesce on an 80ms timer; wait for the flush.
    await new Promise((r) => setTimeout(r, 90));
    expect(service.currentStreamingTurn()!.events.filter((e) => e.kind === 'thought')).toHaveLength(
      1,
    );
    // The agent's empty-response retry asks the client to clear the bubble.
    // thinking.reset is a non-delta frame → flushes the buffer then drops.
    fireSseMessage(es, { method: 'thinking.reset', params: { message_id: 'a1' } }, '1:3');
    expect(service.currentStreamingTurn()!.events.filter((e) => e.kind === 'thought')).toHaveLength(
      0,
    );
  });

  it('adds to pendingPermissions on permission.request', async () => {
    const { service, es } = await setup();
    fireSseMessage(
      es,
      {
        method: 'permission.request',
        params: { id: 'tc-1', tool: 'run_command', args: { cmd: 'ls' } },
      },
      '1:1',
    );
    expect(service.pendingPermissions()).toEqual([
      {
        id: 'tc-1',
        tool: 'run_command',
        args: { cmd: 'ls' },
      },
    ]);
  });

  it('keeps approval_id from permission.request for durable approval', async () => {
    const { service, es } = await setup();
    fireSseMessage(
      es,
      {
        method: 'permission.request',
        params: {
          id: 'tc-1',
          approval_id: 'approval-1',
          tool: 'run_command',
          args: { cmd: 'ls' },
        },
      },
      '1:1',
    );
    expect(service.pendingPermissions()).toEqual([
      {
        id: 'tc-1',
        approvalId: 'approval-1',
        tool: 'run_command',
        args: { cmd: 'ls' },
      },
    ]);
  });

  it('re-surfaces a pending gate from the session.state welcome frame', async () => {
    // A dropped live stream (or a reload) must not strand a gate that is
    // still waiting on the user — otherwise it can never be answered and
    // times out. See
    // knowledge-history/done/supervised_parallel_gates_timeout_fabricates_denial.md.
    const { service, es } = await setup();
    fireSseMessage(
      es,
      {
        method: 'session.state',
        params: {
          pending_permissions: [
            {
              id: 'tc-9',
              approval_id: 'approval-9',
              tool: 'web_search',
              args: { query: 'capital of Japan' },
            },
          ],
        },
      },
      '1:1',
    );
    expect(service.pendingPermissions()).toEqual([
      {
        id: 'tc-9',
        approvalId: 'approval-9',
        tool: 'web_search',
        args: { query: 'capital of Japan' },
      },
    ]);
  });

  it('leaves pendingPermissions alone when session.state omits the key', async () => {
    // Presence-check discipline: a metadata-only session.state from
    // another channel must not clobber a live approval card.
    const { service, es } = await setup();
    (service as any).pendingPermissions.set([
      {
        id: 'tc-live',
        tool: 'run_command',
        args: { cmd: 'ls' },
      },
    ]);
    fireSseMessage(
      es,
      {
        method: 'session.state',
        params: { permission_mode: 'supervised' },
      },
      '1:1',
    );
    expect(service.pendingPermissions()[0]?.id).toBe('tc-live');
  });

  it('clears stale pendingPermissions when session.state explicitly sends an empty list', async () => {
    const { service, es } = await setup();
    (service as any).pendingPermissions.set([
      {
        id: 'tc-resolved',
        approvalId: 'approval-resolved',
        tool: 'run_command',
        args: { cmd: 'ls' },
      },
    ]);
    fireSseMessage(
      es,
      {
        method: 'session.state',
        params: { pending_permissions: [] },
      },
      '1:2',
    );
    expect(service.pendingPermissions()).toEqual([]);
  });

  it('keeps the newest approval row when a snapshot contains a duplicate tool call', async () => {
    const { service, es } = await setup();
    fireSseMessage(
      es,
      {
        method: 'session.state',
        params: {
          snapshot_source: 'durable_journal',
          pending_permissions: [
            {
              id: 'tc-duplicate',
              approval_id: 'approval-stale',
              tool: 'run_command',
              args: { cmd: 'old' },
            },
            {
              id: 'tc-duplicate',
              approval_id: 'approval-waiter-owns',
              tool: 'run_command',
              args: { cmd: 'new' },
            },
          ],
        },
      },
      '1:2',
    );

    expect(service.pendingPermissions()).toEqual([
      {
        id: 'tc-duplicate',
        approvalId: 'approval-waiter-owns',
        tool: 'run_command',
        args: { cmd: 'new' },
      },
    ]);
  });

  it('promotes ready event to sessionReady=true and flushes the outbox', async () => {
    const { service, es, mockHttp } = await setup();
    // Queue a send as if the user typed while the session wasn't ready.
    (service as any).outbox.set([
      { localId: 'user-q', content: 'queued msg', displayContent: 'queued msg', attempts: 0 },
    ]);

    fireSseMessage(es, { method: 'ready', params: {} }, '1:1');
    // The flush POSTs on a microtask.
    await new Promise((r) => setTimeout(r, 0));

    expect(service.sessionReady()).toBe(true);
    // Accepted item removed from the outbox.
    expect(service.outbox().length).toBe(0);
    const postCalls = mockHttp.post.mock.calls.filter((c: any) =>
      String(c[0]).endsWith('/persistent/threads/thread-X/input'),
    );
    expect(postCalls.length).toBeGreaterThanOrEqual(1);
  });

  it('flips threadStatus to ended on session.ended event', async () => {
    const { service, es } = await setup();
    fireSseMessage(es, { method: 'session.ended', params: {} }, '1:1');
    expect(service.threadStatus()).toBe('ended');
  });

  it('flips threadStatus to suspended (not ended) on session.suspended', async () => {
    const { service, es } = await setup();
    fireSseMessage(
      es,
      {
        method: 'session.suspended',
        params: { message: 'Session suspended for a platform update.' },
      },
      '1:1',
    );
    // Suspended threads stay live-resumable — the composer must remain
    // enabled (no 'ended' resume card) and the next send wakes the session.
    expect(service.threadStatus()).toBe('suspended');
    expect(service.isWaitingForInput()).toBe(false);
  });

  it('generation-fences delayed suspend frames after the same thread reconnects', async () => {
    const { service, es } = await setup();
    const generation2 = '66666666-6666-4666-8666-666666666666';
    const connection = (generation: string) => ({
      state: 'ready',
      control_socket: 'none',
      ws_url: null,
      token: null,
      expires_at: null,
      pinned_runtime_generation_contract: 1,
      session_runtime_generation: generation,
    });

    (service as any)._installControlTransport('thread-X', connection(SESSION_RUNTIME_GENERATION));
    fireSseMessage(
      es,
      {
        method: 'session.suspended',
        params: {
          session_runtime_generation: SESSION_RUNTIME_GENERATION,
          message: 'G1 suspended',
        },
      },
      '1:1',
    );
    expect(service.threadStatus()).toBe('suspended');

    // A successor life on the same thread has installed G2. Replayed G1 and
    // legacy/malformed frames cannot regress its UI; only exact G2 applies.
    service.threadStatus.set('active');
    (service as any)._reopenTerminalControl('thread-X');
    (service as any)._installControlTransport('thread-X', connection(generation2));
    for (const generation of [SESSION_RUNTIME_GENERATION, undefined, 'not-a-uuid']) {
      fireSseMessage(
        es,
        {
          method: 'session.suspended',
          params: {
            ...(generation === undefined ? {} : { session_runtime_generation: generation }),
            message: 'stale suspend',
          },
        },
        '1:2',
      );
      expect(service.threadStatus()).toBe('active');
    }

    fireSseMessage(
      es,
      {
        method: 'session.suspended',
        params: {
          session_runtime_generation: generation2,
          message: 'G2 suspended',
        },
      },
      '2:1',
    );
    expect(service.threadStatus()).toBe('suspended');
  });

  it('remembers the exact runtime contract per thread across A → B → A navigation', async () => {
    const { service, es } = await setup();
    const generationB = '77777777-7777-4777-8777-777777777777';
    const connection = (generation: string) => ({
      state: 'ready',
      control_socket: 'none',
      ws_url: null,
      token: null,
      expires_at: null,
      pinned_runtime_generation_contract: 1,
      session_runtime_generation: generation,
    });

    (service as any)._installControlTransport('thread-X', connection(SESSION_RUNTIME_GENERATION));
    service.threadId.set('thread-B');
    (service as any)._installControlTransport('thread-B', connection(generationB));

    // Returning to A clears the currently installed generation while its
    // /connection request is still in flight. Exact-contract evidence for B
    // must not overwrite A's evidence and reopen the legacy no-generation
    // compatibility path during this gap.
    service.threadId.set('thread-X');
    service.threadStatus.set('active');
    (service as any).sessionRuntimeGeneration = null;
    fireSseMessage(
      es,
      {
        method: 'session.suspended',
        params: { message: 'legacy replay from A' },
      },
      '1:2',
    );

    expect(service.threadStatus()).toBe('active');
  });

  it('surfaces error frames via sanitized error signal', async () => {
    const { service, es } = await setup();
    fireSseMessage(es, { method: 'error', params: { message: 'something broke' } }, '1:1');
    expect(service.error()).toContain('something broke');
  });

  it('marks cloudSyncDegraded on a degraded workspace_sync.error (initial pull)', async () => {
    const { service, es } = await setup();
    expect(service.cloudSyncDegraded()).toBe(false);
    fireSseMessage(
      es,
      {
        method: 'workspace_sync.error',
        params: {
          op: 'initial_pull',
          turn_id: 0,
          message: 'token exchange 400',
          degraded: true,
        },
      },
      '1:1',
    );
    // Sticky, session-long: the initial cloud->workspace seed failed, so
    // sync is OFF for the whole session (not a per-turn retry).
    expect(service.cloudSyncDegraded()).toBe(true);
  });

  it('clears cloudSyncDegraded when the agent recovers sync at a turn boundary', async () => {
    // The agent retries the coordinator build each turn after a degraded
    // attach (knowledge-history/done/session_resume_cloud_sync_race_late_provision.md).
    // Leaving the warning up after it succeeds would tell the user their
    // edits aren't being saved when they are.
    const { service, es } = await setup();
    fireSseMessage(
      es,
      {
        method: 'workspace_sync.error',
        params: { op: 'provision', turn_id: 0, message: 'no target', degraded: true },
      },
      '1:1',
    );
    expect(service.cloudSyncDegraded()).toBe(true);

    fireSseMessage(es, { method: 'workspace_sync.recovered', params: { turn_id: 1 } }, '1:2');
    expect(service.cloudSyncDegraded()).toBe(false);
  });

  it('does NOT mark cloudSyncDegraded for a retryable per-turn workspace_sync.error', async () => {
    const { service, es } = await setup();
    fireSseMessage(
      es,
      {
        method: 'workspace_sync.error',
        params: { op: 'push', turn_id: 3, message: 'transient 502' },
      },
      '1:1',
    );
    // Turn-loop push/pull failures retry next turn; not a degraded session.
    expect(service.cloudSyncDegraded()).toBe(false);
  });

  it('shows every call in a batch at once', async () => {
    const { service, es } = await setup();
    fireSseMessage(
      es,
      {
        method: 'permission.request_batch',
        params: {
          requests: [
            { id: 'tc-0', approval_id: 'a-0', tool: 'web_search', args: { q: 'fr' } },
            { id: 'tc-1', approval_id: 'a-1', tool: 'web_search', args: { q: 'jp' } },
          ],
        },
      },
      '1:1',
    );
    expect(service.pendingPermissions().map((p) => p.id)).toEqual(['tc-0', 'tc-1']);
    expect(service.pendingPermissions()[1].approvalId).toBe('a-1');
  });

  it('approveAll sends an explicit approval_id per entry', async () => {
    // _resolve_pending_permission falls back to "most-recent-pending"
    // when no id is given — with N gates open that resolves the WRONG one.
    const { service, es, mockHttp } = await setup();
    fireSseMessage(
      es,
      {
        method: 'permission.request_batch',
        params: {
          requests: [
            { id: 'tc-0', approval_id: 'a-0', tool: 'web_search', args: {} },
            { id: 'tc-1', approval_id: 'a-1', tool: 'web_search', args: {} },
          ],
        },
      },
      '1:1',
    );

    service.approveAll();

    const urls = mockHttp.post.mock.calls.map((c: unknown[]) => c[0] as string);
    expect(urls.some((u) => u.endsWith('/approve/a-0'))).toBe(true);
    expect(urls.some((u) => u.endsWith('/approve/a-1'))).toBe(true);
    expect(service.pendingPermissions().map((p) => p.id)).toEqual(['tc-0', 'tc-1']);
    fireSseMessage(
      es,
      {
        method: 'permission.resolved',
        params: { id: 'tc-0', decision: 'approved' },
      },
      '1:2',
    );
    fireSseMessage(
      es,
      {
        method: 'permission.resolved',
        params: { id: 'tc-1', decision: 'approved' },
      },
      '1:3',
    );
    expect(service.pendingPermissions()).toEqual([]);
  });

  it('permission.resolved with decision=expired does not report a denial', async () => {
    // The backend sweeps un-reached announced gates as "expired" at turn
    // end. Mapping anything != approved to 'denied' told the user they
    // refused calls they never saw — the same fabricated-denial class the
    // backend fix removed. See
    // knowledge-history/done/supervised_parallel_gates_timeout_fabricates_denial.md
    const { service, es } = await setup();
    const dispatched: { decision?: string }[] = [];
    const origDispatch = (service as any).dispatch.bind(service);
    (service as any).dispatch = (a: { decision?: string }) => {
      dispatched.push(a);
      return origDispatch(a);
    };

    fireSseMessage(
      es,
      {
        method: 'permission.request_batch',
        params: {
          requests: [{ id: 'tc-0', approval_id: 'a-0', tool: 'web_search', args: {} }],
        },
      },
      '1:1',
    );
    fireSseMessage(
      es,
      {
        method: 'permission.resolved',
        params: { id: 'tc-0', approval_id: 'a-0', decision: 'expired' },
      },
      '1:2',
    );

    expect(service.pendingPermissions()).toEqual([]);
    const decisions = dispatched.filter((a) => 'decision' in a).map((a) => a.decision);
    expect(decisions).toContain('expired');
    expect(decisions).not.toContain('denied');
  });

  it('permission.resolved with decision=interrupted is also not a denial', async () => {
    const { service, es } = await setup();
    const dispatched: { decision?: string }[] = [];
    const origDispatch = (service as any).dispatch.bind(service);
    (service as any).dispatch = (a: { decision?: string }) => {
      dispatched.push(a);
      return origDispatch(a);
    };

    fireSseMessage(
      es,
      {
        method: 'permission.request',
        params: { id: 'tc-9', approval_id: 'a-9', tool: 'run_command', args: {} },
      },
      '1:1',
    );
    fireSseMessage(
      es,
      {
        method: 'permission.resolved',
        params: { id: 'tc-9', approval_id: 'a-9', decision: 'interrupted' },
      },
      '1:2',
    );

    const decisions = dispatched.filter((a) => 'decision' in a).map((a) => a.decision);
    expect(decisions).not.toContain('denied');
  });

  it('permission.resolved with decision=denied still reports a real denial', async () => {
    const { service, es } = await setup();
    const dispatched: { decision?: string }[] = [];
    const origDispatch = (service as any).dispatch.bind(service);
    (service as any).dispatch = (a: { decision?: string }) => {
      dispatched.push(a);
      return origDispatch(a);
    };

    fireSseMessage(
      es,
      {
        method: 'permission.request',
        params: { id: 'tc-d', approval_id: 'a-d', tool: 'run_command', args: {} },
      },
      '1:1',
    );
    fireSseMessage(
      es,
      {
        method: 'permission.resolved',
        params: { id: 'tc-d', approval_id: 'a-d', decision: 'denied' },
      },
      '1:2',
    );

    const decisions = dispatched.filter((a) => 'decision' in a).map((a) => a.decision);
    expect(decisions).toContain('denied');
  });

  it('permission.resolved removes only its own entry', async () => {
    const { service, es } = await setup();
    fireSseMessage(
      es,
      {
        method: 'permission.request_batch',
        params: {
          requests: [
            { id: 'tc-0', approval_id: 'a-0', tool: 'web_search', args: {} },
            { id: 'tc-1', approval_id: 'a-1', tool: 'web_search', args: {} },
          ],
        },
      },
      '1:1',
    );
    fireSseMessage(
      es,
      {
        method: 'permission.resolved',
        params: { id: 'tc-0', approval_id: 'a-0', decision: 'approved' },
      },
      '1:2',
    );
    expect(service.pendingPermissions().map((p) => p.id)).toEqual(['tc-1']);
  });

  it('restores a multi-entry batch from the session.state welcome frame', async () => {
    const { service, es } = await setup();
    fireSseMessage(
      es,
      {
        method: 'session.state',
        params: {
          pending_permissions: [
            { id: 'tc-0', approval_id: 'a-0', tool: 'web_search', args: {} },
            { id: 'tc-1', approval_id: 'a-1', tool: 'web_search', args: {} },
          ],
        },
      },
      '1:1',
    );
    expect(service.pendingPermissions().map((p) => p.id)).toEqual(['tc-0', 'tc-1']);
  });

  it('converges on the approval_id of a re-announced call instead of dropping it', async () => {
    // The claim SELECT in _loop_permission_check soft-fails on any DB
    // blip: the gate then INSERTs a SECOND pending row and broadcasts
    // permission.request carrying the NEW approval_id — the only id the
    // waiter is filtering NOTIFY on. Appending only when the tool_call_id
    // is absent DROPS that frame, so the card keeps the stale announced
    // id; "Approve all" resolves the announced row, the NOTIFY id never
    // matches the waiter's, the card vanishes and the agent blocks
    // forever with no recovery path.
    const { service, es, mockHttp } = await setup();
    fireSseMessage(
      es,
      {
        method: 'permission.request_batch',
        params: {
          requests: [
            {
              id: 'tc-0',
              approval_id: 'a-stale',
              tool: 'run_command',
              args: { command: 'rm -rf /var/data' },
            },
            { id: 'tc-1', approval_id: 'a-1', tool: 'web_search', args: {} },
          ],
        },
      },
      '1:1',
    );
    fireSseMessage(
      es,
      {
        method: 'permission.request',
        params: {
          id: 'tc-0',
          approval_id: 'a-authoritative',
          tool: 'run_command',
          args: { command: 'rm -rf /var/data' },
        },
      },
      '1:2',
    );

    // Updated in place — not appended as a duplicate row, not dropped.
    expect(service.pendingPermissions().map((p) => p.id)).toEqual(['tc-0', 'tc-1']);
    expect(service.pendingPermissions()[0].approvalId).toBe('a-authoritative');
    // Full args survive the update: the destructive command must stay
    // visible before the single "Approve all" click.
    expect(service.pendingPermissions()[0].args).toEqual({ command: 'rm -rf /var/data' });

    service.approveAll();

    const urls = mockHttp.post.mock.calls.map((c: unknown[]) => c[0] as string);
    expect(urls.some((u) => u.endsWith('/approve/a-authoritative'))).toBe(true);
    expect(urls.some((u) => u.endsWith('/approve/a-stale'))).toBe(false);
  });

  it('drops the announced card when the backend expires an unclaimed row', async () => {
    // Turn-end sweep: rows the turn never reached are CAS-expired and
    // journalled as permission.resolved, so an attached client must not
    // keep offering a gate nobody is waiting on.
    const { service, es } = await setup();
    fireSseMessage(
      es,
      {
        method: 'permission.request_batch',
        params: {
          requests: [
            { id: 'tc-0', approval_id: 'a-0', tool: 'web_search', args: {} },
            { id: 'tc-1', approval_id: 'a-1', tool: 'web_search', args: {} },
          ],
        },
      },
      '1:1',
    );
    fireSseMessage(
      es,
      {
        method: 'permission.resolved',
        params: { id: 'tc-1', approval_id: 'a-1', decision: 'expired' },
      },
      '1:2',
    );
    expect(service.pendingPermissions().map((p) => p.id)).toEqual(['tc-0']);
  });

  it.each(['expired', 'interrupted'])(
    'retires a %s permission card without fabricating a denial',
    async (decision) => {
      const { service, es } = await setup();
      fireSseMessage(
        es,
        {
          method: 'permission.request',
          params: {
            id: 'tc-unanswered',
            approval_id: 'a-unanswered',
            tool: 'run_command',
            args: { command: 'echo untouched' },
          },
        },
        '1:1',
      );

      fireSseMessage(
        es,
        {
          method: 'permission.resolved',
          params: {
            id: 'tc-unanswered',
            approval_id: 'a-unanswered',
            decision,
          },
        },
        '1:2',
      );

      expect(service.pendingPermissions()).toEqual([]);
      const tool = service
        .currentStreamingTurn()
        ?.events.find((event) => event.kind === 'tool_call' && event.id === 'tc-unanswered') as
        | ToolCallEvent
        | undefined;
      expect(tool).toBeDefined();
      expect(tool?.decision).toBe('expired');
      expect(tool?.status).toBe('expired');
      expect(tool?.status).not.toBe('denied');
    },
  );
});

describe('PersistentChatService — cursor persistence', () => {
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

  it('saves the cursor to IndexedDB on each event with a parseable lastEventId', async () => {
    const ctx = createService();
    ctx.mockHttp.get.mockImplementation(() =>
      of({ status: 'active', total_turns: 0, messages: [], total: 0 }),
    );
    await ctx.service.connect('thread-cur');
    const es = ctx.sseInstances[0];
    fireSseOpen(es);

    fireSseMessage(es, { method: 'token', params: { content: 'x' } }, '5:101');
    expect(ctx.mockCache.setThreadCursor).toHaveBeenCalledWith('thread-cur', 5, 101);

    fireSseMessage(es, { method: 'token', params: { content: 'y' } }, '5:102');
    expect(ctx.mockCache.setThreadCursor).toHaveBeenLastCalledWith('thread-cur', 5, 102);
  });

  it('ignores malformed lastEventId values silently', async () => {
    const ctx = createService();
    ctx.mockHttp.get.mockImplementation(() =>
      of({ status: 'active', total_turns: 0, messages: [], total: 0 }),
    );
    await ctx.service.connect('thread-mal');
    const es = ctx.sseInstances[0];
    fireSseOpen(es);

    ctx.mockCache.setThreadCursor.mockClear();
    fireSseMessage(es, { method: 'token', params: { content: 'x' } }, ''); // empty
    fireSseMessage(es, { method: 'token', params: { content: 'y' } }, 'nope'); // no colon
    fireSseMessage(es, { method: 'token', params: { content: 'z' } }, 'a:b'); // NaN
    expect(ctx.mockCache.setThreadCursor).not.toHaveBeenCalled();
  });
});

describe('PersistentChatService — gone_beyond_horizon recovery', () => {
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

  it('re-anchors cursor to the server tail, reloads history, reopens from tail', async () => {
    // Stateful cursor so the reopened stream reflects the re-anchor.
    let stored: any = { epoch: 1, seq: 5, threadId: 'thread-g', updatedAt: '' };
    const ctx = createService({ cursor: stored });
    ctx.mockCache.getThreadCursor.mockImplementation(async () => stored);
    ctx.mockCache.setThreadCursor.mockImplementation(
      async (tid: string, epoch: number, seq: number) => {
        stored = { epoch, seq, threadId: tid, updatedAt: '' };
      },
    );
    ctx.mockCache.deleteThreadCursor.mockImplementation(async () => {
      stored = null;
    });
    ctx.mockHttp.get.mockImplementation(() =>
      of({ status: 'active', total_turns: 0, messages: [], total: 0 }),
    );
    await ctx.service.connect('thread-g');
    const firstSse = ctx.sseInstances[0];
    fireSseOpen(firstSse);

    // Server reports the live epoch (2) and its tail seq (7).
    fireSseNamedEvent(
      firstSse,
      'gone_beyond_horizon',
      {
        method: 'gone_beyond_horizon',
        params: { epoch: 2, server_seq: 7, reason: 'epoch_mismatch' },
      },
      '2:7',
    );

    // The handler chain awaits: parse → close → loadHistory →
    // setThreadCursor → openSse → getThreadCursor. setTimeout(0) lets all
    // queued microtasks drain.
    await new Promise((r) => setTimeout(r, 0));

    // Re-anchored to the reported tail rather than dropped — this is what
    // stops the replay-from-0 that re-renders loaded turns as a duplicate
    // "live" copy (the SESSION RESUMED double-render). deleteThreadCursor
    // IS called once, ahead of the reload (Fix 2: an unconditional
    // clear-cache-before-reload step — see the "clears the message cache"
    // test below) — but setThreadCursor's re-anchor runs after it and
    // wins, so the cursor still ends up pinned to the reported tail, not
    // dropped.
    expect(ctx.mockCache.setThreadCursor).toHaveBeenCalledWith('thread-g', 2, 7);
    expect(stored).toEqual({ epoch: 2, seq: 7, threadId: 'thread-g', updatedAt: '' });
    expect(firstSse.close).toHaveBeenCalled();

    // History was reloaded — an additional GET /messages after the initial.
    const historyCalls = ctx.mockHttp.get.mock.calls.filter((c: any) =>
      String(c[0]).endsWith('/persistent/threads/thread-g/messages'),
    );
    expect(historyCalls.length).toBeGreaterThanOrEqual(2);

    // A second SSE was opened, resuming from the re-anchored tail so it
    // replays only events newer than the just-loaded history.
    expect(ctx.sseInstances.length).toBeGreaterThanOrEqual(2);
    expect(ctx.sseInstances[ctx.sseInstances.length - 1].url).toContain('last_event_id=2%3A7');
  });

  it('falls back to dropping the cursor when the frame lacks a server tail', async () => {
    const ctx = createService({
      cursor: { epoch: 1, seq: 5, threadId: 'thread-h', updatedAt: '' } as any,
    });
    ctx.mockHttp.get.mockImplementation(() =>
      of({ status: 'active', total_turns: 0, messages: [], total: 0 }),
    );
    await ctx.service.connect('thread-h');
    const firstSse = ctx.sseInstances[0];
    fireSseOpen(firstSse);

    // Malformed frame: no numeric server_seq → can't re-anchor.
    fireSseNamedEvent(
      firstSse,
      'gone_beyond_horizon',
      {
        method: 'gone_beyond_horizon',
        params: { epoch: 2, reason: 'epoch_mismatch' },
      },
      '2:0',
    );
    await new Promise((r) => setTimeout(r, 0));

    expect(ctx.mockCache.deleteThreadCursor).toHaveBeenCalledWith('thread-h');
    expect(ctx.mockCache.setThreadCursor).not.toHaveBeenCalled();
    expect(firstSse.close).toHaveBeenCalled();
    expect(ctx.sseInstances.length).toBeGreaterThanOrEqual(2);
  });

  it('re-reads thread status on re-anchor, healing a stale "ended"', async () => {
    // An epoch bump means a new agent attached, so an 'ended' status the
    // client is still holding — e.g. from a terminal frame replayed off the
    // previous epoch's tail — is stale by definition. loadHistory restores
    // the transcript but never touches thread meta, so without this the
    // ended UI stays pinned over a live, streaming session for good.
    const ctx = createService({
      cursor: { epoch: 1, seq: 5, threadId: 'thread-s', updatedAt: '' } as any,
    });
    ctx.mockHttp.get.mockImplementation(() =>
      of({ status: 'active', total_turns: 0, messages: [], total: 0 }),
    );
    await ctx.service.connect('thread-s');
    const firstSse = ctx.sseInstances[0];
    fireSseOpen(firstSse);

    ctx.service.threadStatus.set('ended');

    fireSseNamedEvent(
      firstSse,
      'gone_beyond_horizon',
      {
        method: 'gone_beyond_horizon',
        params: { epoch: 2, server_seq: 7, reason: 'epoch_bumped_mid_stream' },
      },
      '2:7',
    );
    await new Promise((r) => setTimeout(r, 0));

    expect(ctx.service.threadStatus()).toBe('active');
  });

  it('clears the message cache before reloading history (Fix 2)', async () => {
    // A missed rewind.done (client offline/reconnecting when it fired)
    // otherwise leaves swept rows sitting in IndexedDB forever — the next
    // gone_beyond_horizon repaint is the self-heal, but only if it clears
    // the append-only cache before merging in the freshly loaded history.
    const ctx = createService({
      cursor: { epoch: 1, seq: 5, threadId: 'thread-clear', updatedAt: '' } as any,
    });
    const order: string[] = [];
    ctx.mockCache.clearThreadMessages.mockImplementation(async (tid: string) => {
      if (tid === 'thread-clear') order.push('clear');
    });
    ctx.mockHttp.get.mockImplementation((url: string) => {
      if (String(url).endsWith('/persistent/threads/thread-clear/messages')) {
        order.push('history-get');
      }
      return of({ status: 'active', total_turns: 0, messages: [], total: 0 });
    });
    await ctx.service.connect('thread-clear');
    const firstSse = ctx.sseInstances[0];
    fireSseOpen(firstSse);
    order.length = 0; // only care about ordering from here on

    fireSseNamedEvent(
      firstSse,
      'gone_beyond_horizon',
      {
        method: 'gone_beyond_horizon',
        params: { epoch: 2, server_seq: 7, reason: 'epoch_mismatch' },
      },
      '2:7',
    );
    await new Promise((r) => setTimeout(r, 0));

    expect(ctx.mockCache.clearThreadMessages).toHaveBeenCalledWith('thread-clear');
    expect(order).toEqual(['clear', 'history-get']);
  });
});

describe('PersistentChatService — SSE error handling', () => {
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

  it('bumps reconnectAttempt on transient onerror (browser retrying)', async () => {
    const ctx = createService();
    ctx.mockHttp.get.mockImplementation(() =>
      of({ status: 'active', total_turns: 0, messages: [], total: 0 }),
    );
    await ctx.service.connect('thread-tr');
    const es = ctx.sseInstances[0];
    fireSseOpen(es);

    fireSseTransientError(es);
    expect(ctx.service.reconnectAttempt()).toBe(1);
    expect(ctx.service.connectionState()).toBe('connecting');
    expect(ctx.service.reconnectGaveUp()).toBe(false);

    fireSseTransientError(es);
    expect(ctx.service.reconnectAttempt()).toBe(2);
  });

  it('sets reconnectGaveUp + error state on terminal CLOSED', async () => {
    const ctx = createService();
    ctx.mockHttp.get.mockImplementation(() =>
      of({ status: 'active', total_turns: 0, messages: [], total: 0 }),
    );
    await ctx.service.connect('thread-term');
    const es = ctx.sseInstances[0];
    fireSseOpen(es);

    fireSseTerminalError(es);
    expect(ctx.service.connectionState()).toBe('error');
    expect(ctx.service.reconnectGaveUp()).toBe(true);
  });

  it('reconnectNow() drops the existing SSE and opens a fresh one', async () => {
    const ctx = createService();
    ctx.mockHttp.get.mockImplementation(() =>
      of({ status: 'active', total_turns: 0, messages: [], total: 0 }),
    );
    await ctx.service.connect('thread-rn');
    const first = ctx.sseInstances[0];
    fireSseOpen(first);
    fireSseTerminalError(first);
    expect(ctx.service.reconnectGaveUp()).toBe(true);

    ctx.service.reconnectNow();
    // _openSse awaits cache.getThreadCursor — drain microtasks.
    await new Promise((r) => setTimeout(r, 0));

    expect(first.close).toHaveBeenCalled();
    expect(ctx.sseInstances.length).toBe(2);
    expect(ctx.service.reconnectGaveUp()).toBe(false);
    expect(ctx.service.reconnectAttempt()).toBe(0);
  });
});

describe('PersistentChatService — SSE liveness watchdog', () => {
  let originalEs: any;
  let originalWs: any;

  beforeEach(() => {
    originalEs = (globalThis as any).EventSource;
    originalWs = (globalThis as any).WebSocket;
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
    (globalThis as any).EventSource = originalEs;
    (globalThis as any).WebSocket = originalWs;
    vi.clearAllMocks();
  });

  it('force-reopens the SSE when no event arrives for > 45s', async () => {
    const ctx = createService();
    ctx.mockHttp.get.mockImplementation(() =>
      of({ status: 'active', total_turns: 0, messages: [], total: 0 }),
    );
    await ctx.service.connect('thread-wd');
    // Drain the _openSse microtask chain (await getThreadCursor) so the
    // first EventSource has been constructed.
    await vi.advanceTimersByTimeAsync(0);

    const first = ctx.sseInstances[0];
    fireSseOpen(first);
    expect(ctx.sseInstances.length).toBe(1);

    // 50s of silence — past the 45s watchdog threshold.
    await vi.advanceTimersByTimeAsync(50_000);

    expect(first.close).toHaveBeenCalled();
    expect(ctx.sseInstances.length).toBe(2);
    expect(ctx.service.connectionState()).toBe('connecting');
    expect(ctx.service.reconnectAttempt()).toBeGreaterThan(0);
  });

  it('treats a typed `ping` event as liveness and does not trip the watchdog', async () => {
    const ctx = createService();
    ctx.mockHttp.get.mockImplementation(() =>
      of({ status: 'active', total_turns: 0, messages: [], total: 0 }),
    );
    await ctx.service.connect('thread-ping');
    await vi.advanceTimersByTimeAsync(0);

    const es = ctx.sseInstances[0];
    fireSseOpen(es);

    // 30s of silence, then a ping, then another 30s. Total 60s elapsed,
    // but only 30s since the most recent ping — under the 45s threshold.
    await vi.advanceTimersByTimeAsync(30_000);
    fireSseNamedEvent(es, 'ping', {});
    await vi.advanceTimersByTimeAsync(30_000);

    expect(es.close).not.toHaveBeenCalled();
    expect(ctx.sseInstances.length).toBe(1);
  });

  it('treats a regular onmessage frame as liveness', async () => {
    const ctx = createService();
    ctx.mockHttp.get.mockImplementation(() =>
      of({ status: 'active', total_turns: 0, messages: [], total: 0 }),
    );
    await ctx.service.connect('thread-msg');
    await vi.advanceTimersByTimeAsync(0);

    const es = ctx.sseInstances[0];
    fireSseOpen(es);

    await vi.advanceTimersByTimeAsync(40_000);
    fireSseMessage(es, { method: 'thinking', params: { text: 'still here' } }, '1:1');
    await vi.advanceTimersByTimeAsync(40_000);

    expect(es.close).not.toHaveBeenCalled();
    expect(ctx.sseInstances.length).toBe(1);
  });

  it('stops the watchdog on disconnect()', async () => {
    const ctx = createService();
    ctx.mockHttp.get.mockImplementation(() =>
      of({ status: 'active', total_turns: 0, messages: [], total: 0 }),
    );
    await ctx.service.connect('thread-dc');
    await vi.advanceTimersByTimeAsync(0);

    fireSseOpen(ctx.sseInstances[0]);

    ctx.service.disconnect();
    await vi.advanceTimersByTimeAsync(60_000);

    // Disconnect intentionally torn down — no auto-reopen.
    expect(ctx.sseInstances.length).toBe(1);
  });

  it('does not start a watchdog on terminal CLOSED', async () => {
    const ctx = createService();
    ctx.mockHttp.get.mockImplementation(() =>
      of({ status: 'active', total_turns: 0, messages: [], total: 0 }),
    );
    await ctx.service.connect('thread-cl');
    await vi.advanceTimersByTimeAsync(0);

    const first = ctx.sseInstances[0];
    fireSseOpen(first);
    fireSseTerminalError(first);
    expect(ctx.service.reconnectGaveUp()).toBe(true);

    // After terminal close, no further reopens should happen no matter
    // how much time passes.
    await vi.advanceTimersByTimeAsync(120_000);
    expect(ctx.sseInstances.length).toBe(1);
  });
});

describe('PersistentChatService — REST sends', () => {
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

  async function readySession() {
    const ctx = createService();
    ctx.mockHttp.get.mockImplementation(() =>
      of({ status: 'active', total_turns: 0, messages: [], total: 0 }),
    );
    await ctx.service.connect('thread-r');
    fireSseOpen(ctx.sseInstances[0]);
    // Drive the agent ready event so sendMessage POSTs immediately.
    fireSseMessage(ctx.sseInstances[0], { method: 'ready', params: {} }, '1:1');
    ctx.mockHttp.post.mockClear();
    return ctx;
  }

  it('sendMessage POSTs to /input with the user content', async () => {
    const ctx = await readySession();
    await ctx.service.sendMessage('hello');
    const calls = ctx.mockHttp.post.mock.calls;
    const inputCall = calls.find((c: any) =>
      String(c[0]).endsWith('/persistent/threads/thread-r/input'),
    );
    expect(inputCall).toBeDefined();
    expect(inputCall![1]).toEqual({ content: 'hello', expected_conversation_revision: 0 });
    // Local optimistic UserTurn added.
    const userTurns = ctx.service.turns().filter(isUserTurn);
    const last = userTurns[userTurns.length - 1] as UserTurn;
    expect(last.content).toBe('hello');
  });

  it('waits for the mounted Office editor save before accepting a user turn', async () => {
    const ctx = await readySession();
    const order: string[] = [];
    let resolveSave!: (value: boolean) => void;
    const save = new Promise<boolean>((resolve) => {
      resolveSave = resolve;
    });
    ctx.canvas.registerOfficeTurnAdapter({
      saveBeforeUserMessage: () => {
        order.push('Action_Save');
        return save;
      },
    });

    const sending = ctx.service.sendMessage('use my spreadsheet changes');
    await Promise.resolve();
    expect(order).toEqual(['Action_Save']);
    expect(ctx.service.outbox()).toEqual([]);
    expect(
      ctx.mockHttp.post.mock.calls.some((call: any[]) =>
        String(call[0]).endsWith('/persistent/threads/thread-r/input'),
      ),
    ).toBe(false);

    resolveSave(true);
    await expect(sending).resolves.toBe(true);
    await Promise.resolve();
    order.push('message');
    expect(order).toEqual(['Action_Save', 'message']);
    expect(
      ctx.mockHttp.post.mock.calls.some((call: any[]) =>
        String(call[0]).endsWith('/persistent/threads/thread-r/input'),
      ),
    ).toBe(true);
  });

  it('sendMessage queues content if session is not yet ready', async () => {
    const ctx = createService();
    ctx.mockHttp.get.mockImplementation(() =>
      of({ status: 'active', total_turns: 0, messages: [], total: 0 }),
    );
    await ctx.service.connect('thread-q');
    fireSseOpen(ctx.sseInstances[0]);
    // No 'ready' event yet → sessionReady is false.

    await ctx.service.sendMessage('queued');
    // No POST — the flush no-ops while !sessionReady.
    const inputCalls = ctx.mockHttp.post.mock.calls.filter((c: any) =>
      String(c[0]).endsWith('/persistent/threads/thread-q/input'),
    );
    expect(inputCalls).toHaveLength(0);
    expect(ctx.service.outbox().length).toBe(1);
    expect(ctx.service.outbox()[0].displayContent).toBe('queued');
  });

  it('sendMessage on an ended thread resumes it and queues the message', async () => {
    // Ended sessions keep the composer live so a draft can be written
    // first. SENDING is what brings the agent back — the message rides
    // the resume in the outbox and flushes on markSessionReady.
    const ctx = createService();
    ctx.mockHttp.get.mockImplementation(() =>
      of({ status: 'ended', total_turns: 0, messages: [], total: 0 }),
    );
    await ctx.service.connect('thread-e');
    ctx.service.threadStatus.set('ended');
    ctx.mockHttp.post.mockClear();

    await ctx.service.sendMessage('bring it back');

    const resumeCalls = ctx.mockHttp.post.mock.calls.filter((c: any) =>
      String(c[0]).endsWith('/persistent/threads/thread-e/resume'),
    );
    const inputCalls = ctx.mockHttp.post.mock.calls.filter((c: any) =>
      String(c[0]).endsWith('/persistent/threads/thread-e/input'),
    );
    expect(resumeCalls).toHaveLength(1);
    // Not sent yet — the agent isn't up. It waits in the outbox.
    expect(inputCalls).toHaveLength(0);
    expect(ctx.service.outbox().map((i) => i.displayContent)).toEqual(['bring it back']);
  });

  it('wakes a suspended thread without /resume and flushes its queued send once', async () => {
    const ctx = createService();
    ctx.service.threadId.set('thread-suspended-send');
    ctx.service.threadStatus.set('active');
    (ctx.service as any).sessionRuntimeGeneration = SESSION_RUNTIME_GENERATION;
    (ctx.service as any).runtimeGenerationContractThreads.add('thread-suspended-send');
    (ctx.service as any)._settleSuspendedControl('thread-suspended-send');
    expect(ctx.service.threadStatus()).toBe('suspended');

    const connect = vi.spyOn(ctx.service, 'connect').mockResolvedValue();
    ctx.mockHttp.post.mockClear();

    await expect(ctx.service.sendMessage('wake and continue')).resolves.toBe(true);

    expect(connect).toHaveBeenCalledOnce();
    expect(connect).toHaveBeenCalledWith('thread-suspended-send', {
      preserveReviewPlane: true,
    });
    expect(
      ctx.mockHttp.post.mock.calls.some((call: any[]) =>
        String(call[0]).endsWith('/persistent/threads/thread-suspended-send/resume'),
      ),
    ).toBe(false);
    expect(ctx.service.outbox().map((item) => item.displayContent)).toEqual(['wake and continue']);

    // The successor runtime is now authoritative. Its first ready edge owns
    // the one outbox flush; the stale suspended generation cannot settle or
    // stage anything into this control moment.
    (ctx.service as any).sessionRuntimeGeneration = SESSION_RUNTIME_GENERATION_B;
    ctx.service.sessionReady.set(true);
    await (ctx.service as any)._flushOutbox();
    expect(
      ctx.mockHttp.post.mock.calls.filter((call: any[]) =>
        String(call[0]).endsWith('/persistent/threads/thread-suspended-send/input'),
      ),
    ).toHaveLength(1);
    expect(ctx.service.outbox()).toEqual([]);

    const staleApplies = (ctx.service as any)._runtimeSessionFrameApplies({
      session_runtime_generation: SESSION_RUNTIME_GENERATION,
    });
    expect(staleApplies).toBe(false);
  });

  it('sendMessage with an attachment on an ended thread resumes FIRST and uploads after', async () => {
    // Pre-Task-4 the upload ran above the ended-thread branch, so on a
    // pod/VM-tier ended thread it 409'd against a torn-down workspace,
    // returned false, and resumeSession() was never reached: attaching a
    // file to an ended session silently refused to bring it back.
    const ctx = createService();
    ctx.mockHttp.get.mockImplementation(() =>
      of({ status: 'ended', total_turns: 0, messages: [], total: 0 }),
    );
    await ctx.service.connect('thread-e2');
    ctx.service.threadStatus.set('ended');
    ctx.mockHttp.post.mockClear();
    ctx.mockApi.uploadOneToThread.mockClear();

    const file = new File(['a'], 'a.png', { type: 'image/png' });
    ctx.service.addAttachments([
      {
        id: '1',
        file,
        name: 'a.png',
        size: file.size,
        mimeType: 'image/png',
        uploadStatus: UploadStatus.PENDING,
      } as any,
    ]);

    const ok = await ctx.service.sendMessage('look at this');

    expect(ok).toBe(true);
    expect(
      ctx.mockHttp.post.mock.calls.filter((c: any) =>
        String(c[0]).endsWith('/persistent/threads/thread-e2/resume'),
      ),
    ).toHaveLength(1);
    // The bytes wait for the workspace the resume is bringing up.
    expect(ctx.mockApi.uploadOneToThread).not.toHaveBeenCalled();
    expect(ctx.service.pendingAttachments()).toEqual([]);
    expect(ctx.service.outbox()[0].pendingFiles?.map((f) => f.name)).toEqual(['a.png']);
  });

  it('opening an ended thread does NOT resume it — only a send does', async () => {
    // Resume reserves an agent pod + workspace, so it must stay strictly
    // send-triggered: merely opening the session (or typing, which reaches
    // no service method — the component's onInputChange only writes the
    // sessionStorage draft) leaves the thread ended.
    const ctx = createService();
    ctx.mockHttp.get.mockImplementation(() =>
      of({ status: 'ended', total_turns: 0, messages: [], total: 0 }),
    );
    await ctx.service.connect('thread-t');
    ctx.service.threadStatus.set('ended');

    const resumeCalls = ctx.mockHttp.post.mock.calls.filter((c: any) =>
      String(c[0]).endsWith('/resume'),
    );
    expect(resumeCalls).toHaveLength(0);
    expect(ctx.service.isResuming()).toBe(false);
  });

  it('ignores a replayed session.ended from the epoch we resumed past', async () => {
    // A resume reopens the SSE while the thread is still on its OLD epoch,
    // so the server streams that epoch's tail — which ends in the
    // idle_timeout/ended pair that put us there. Applying it pins the
    // ended UI (end marker, resume card, "sending resumes" placeholder)
    // over a live, streaming session and never clears.
    const ctx = createService();
    ctx.mockHttp.get.mockImplementation(() =>
      of({ status: 'ended', total_turns: 0, messages: [], total: 0 }),
    );
    ctx.mockCache.getThreadCursor.mockResolvedValue({ epoch: 9, seq: 40 });
    await ctx.service.connect('thread-r');
    ctx.service.cloudChangesCount.set(4);
    ctx.service.cloudDiffPanelOpen.set(true);
    (ctx.service as any).controlOutbox = [
      { threadId: 'thread-r', frame: JSON.stringify({ method: 'approve' }) },
    ];
    (ctx.service as any).durableControlOutbox = [
      {
        threadId: 'thread-r',
        request: { method: 'mode.set', mode: 'autonomous', client_request_id: 'old' },
        attempts: 0,
        ordinal: 1,
      },
    ];
    (ctx.service as any).pendingCanvasSourceUpdate = {
      threadId: 'thread-r',
      control: { method: 'canvas.source_updated', presentation_revision: 3 },
    };
    (ctx.service as any)._retireTerminalControl('thread-r');
    expect((ctx.service as any).controlOutbox).toEqual([]);
    expect((ctx.service as any).durableControlOutbox).toEqual([]);
    expect((ctx.service as any).pendingCanvasSourceUpdate).toBeNull();

    ctx.mockHttp.get.mockImplementation(activeSessionGet);

    await ctx.service.resumeSession();
    const es = ctx.sseInstances[ctx.sseInstances.length - 1];
    fireSseOpen(es);
    const resumedWs = ctx.wsInstances.at(-1);
    resumedWs?.onopen?.();
    expect(
      resumedWs?.send.mock.calls.some((call: any[]) => String(call[0]).includes('approve')),
    ).toBe(false);
    expect(ctx.service.cloudChangesCount()).toBe(4);
    expect(ctx.service.cloudDiffPanelOpen()).toBe(true);

    // Old epoch's tail replays.
    fireSseMessage(es, { method: 'session.idle_timeout', params: { timeout_minutes: 30 } }, '9:41');
    fireSseMessage(es, { method: 'session.ended', params: {} }, '9:42');
    expect(ctx.service.threadStatus()).not.toBe('ended');
    expect(ctx.service.cloudChangesCount()).toBe(4);
    expect(ctx.service.cloudDiffPanelOpen()).toBe(true);

    // A genuine terminal frame on the NEW epoch must still land.
    fireSseMessage(
      es,
      {
        method: 'session.ended',
        params: { session_runtime_generation: SESSION_RUNTIME_GENERATION },
      },
      '10:7',
    );
    expect(ctx.service.threadStatus()).toBe('ended');
  });

  it('uses the tab-local SSE cursor when the cache cannot provide the Resume watermark', async () => {
    const ctx = createService();
    ctx.mockHttp.get.mockImplementation(() =>
      of({ status: 'ended', total_turns: 0, messages: [], total: 0 }),
    );
    await ctx.service.connect('thread-local-cursor');
    const endedEs = ctx.sseInstances.at(-1)!;
    fireSseMessage(endedEs, { method: 'session.ended', params: {} }, '9:42');

    ctx.mockCache.getThreadCursor.mockRejectedValue(new Error('indexeddb unavailable'));
    ctx.mockHttp.get.mockImplementation(activeSessionGet);
    await ctx.service.resumeSession();
    const resumedEs = ctx.sseInstances.at(-1)!;

    fireSseMessage(resumedEs, { method: 'session.ended', params: {} }, '9:43');

    expect(ctx.service.threadStatus()).not.toBe('ended');
    expect((ctx.service as any).resumedFromEpoch).toBe(9);
  });

  it('does not let an old SSE metadata request retire the same thread after Resume', async () => {
    const ctx = createService();
    ctx.mockHttp.get.mockImplementation(() =>
      of({ status: 'ended', total_turns: 0, messages: [], total: 0 }),
    );
    await ctx.service.connect('thread-meta-resume');
    const oldEs = ctx.sseInstances.at(-1)!;
    const staleMeta = new Subject<any>();
    let holdOldMeta = true;
    ctx.mockHttp.get.mockImplementation((url: string) => {
      if (holdOldMeta && url.endsWith('/persistent/threads/thread-meta-resume')) {
        holdOldMeta = false;
        return staleMeta;
      }
      return activeSessionGet(url);
    });

    fireSseOpen(oldEs);
    await ctx.service.resumeSession();
    const resumedWs = ctx.wsInstances.at(-1)!;
    const resumedEs = ctx.sseInstances.at(-1)!;

    staleMeta.next({ status: 'ended', title: 'stale', total_turns: 0 });
    staleMeta.complete();
    await Promise.resolve();

    expect(ctx.service.threadStatus()).not.toBe('ended');
    expect((ctx.service as any).terminalControlThreadId).toBeNull();
    expect(resumedWs.close).not.toHaveBeenCalled();
    expect(resumedEs.close).not.toHaveBeenCalled();
  });

  it('does not let pre-terminal active metadata hide a terminal SSE state', async () => {
    const ctx = createService();
    ctx.mockHttp.get.mockImplementation(activeSessionGet);
    await ctx.service.connect('thread-meta-end');
    const es = ctx.sseInstances.at(-1)!;
    const ws = ctx.wsInstances.at(-1)!;
    const staleMeta = new Subject<any>();
    ctx.mockHttp.get.mockImplementation((url: string) =>
      url.endsWith('/persistent/threads/thread-meta-end') ? staleMeta : activeSessionGet(url),
    );

    fireSseOpen(es);
    fireSseMessage(
      es,
      {
        method: 'session.ended',
        params: { session_runtime_generation: SESSION_RUNTIME_GENERATION },
      },
      '4:9',
    );
    staleMeta.next({ status: 'active', title: 'stale active', total_turns: 0 });
    staleMeta.complete();
    await Promise.resolve();

    expect(ctx.service.threadStatus()).toBe('ended');
    expect((ctx.service as any).terminalControlThreadId).toBe('thread-meta-end');
    expect(ws.close).toHaveBeenCalled();
    expect(es.close).not.toHaveBeenCalled();
  });

  it('applies a terminal frame that carries no event id', async () => {
    // Can't prove staleness without an id — never swallow a live end.
    const ctx = createService();
    ctx.mockHttp.get.mockImplementation(() =>
      of({ status: 'ended', total_turns: 0, messages: [], total: 0 }),
    );
    ctx.mockCache.getThreadCursor.mockResolvedValue({ epoch: 9, seq: 40 });
    await ctx.service.connect('thread-n');
    ctx.mockHttp.get.mockImplementation(() =>
      of({ status: 'active', total_turns: 0, messages: [], total: 0 }),
    );
    await ctx.service.resumeSession();
    const es = ctx.sseInstances[ctx.sseInstances.length - 1];
    fireSseOpen(es);

    fireSseMessage(es, { method: 'session.ended', params: {} });
    expect(ctx.service.threadStatus()).toBe('ended');
  });

  it('keeps a distinct sibling-tab message queued on turn_in_flight 409', async () => {
    const ctx = await readySession();
    ctx.mockHttp.post.mockReturnValue(
      throwError(() => ({
        status: 409,
        error: { error: 'turn_in_flight' },
      })),
    );
    await ctx.service.sendMessage('beta from tab B');
    await Promise.resolve();
    expect(ctx.service.outbox().map((item) => item.displayContent)).toEqual(['beta from tab B']);
    expect(ctx.service.outboxStalled()).toBe(true);
    expect(ctx.service.pendingTurnCount()).toBe(0);
    expect(ctx.service.error()).not.toBeNull();
  });

  it('keeps the bubble + queued item and sets the banner on a hard POST failure (no drop, no retry)', async () => {
    const ctx = await readySession();
    ctx.mockHttp.post.mockReturnValue(
      throwError(() => ({ status: 500, error: { detail: 'boom' } })),
    );
    // Optimistic-send contract: sendMessage accepts into the outbox and
    // returns true; the flush handles the transport outcome.
    const ok = await ctx.service.sendMessage('will fail');
    expect(ok).toBe(true);
    // Drain the flush's rejected POST microtask.
    await new Promise((r) => setTimeout(r, 0));
    // A 500 is NOT terminal: the send may have been accepted server-side
    // (accept-time persistence), so the bubble + queued item are KEPT (no
    // silent loss, no auto-retry double-send). The banner explains.
    const stillThere = ctx.service.turns().some((t) => isUserTurn(t) && t.content === 'will fail');
    expect(stillThere).toBe(true);
    expect(ctx.service.outbox().length).toBe(1);
    expect(ctx.service.error()).not.toBeNull();
  });

  it('re-flushes a stalled outbox on a readiness signal received while already ready', async () => {
    // Regression: markSessionReady used to early-return when sessionReady
    // was already true, so a send that failed *after* the session came up
    // could never be retried by any readiness signal — the message sat
    // showing "sending" forever even across a full reattach. Observed live
    // on thread b1758f38: /connection 200, queued item never POSTed.
    const ctx = await readySession();
    ctx.mockHttp.post.mockReturnValue(throwError(() => ({ status: 0 })));
    await ctx.service.sendMessage('stalled by transport');
    await new Promise((r) => setTimeout(r, 0));
    expect(ctx.service.outbox().length).toBe(1);
    expect(ctx.service.outboxStalled()).toBe(true);
    expect(ctx.service.sessionReady()).toBe(true);

    // Transport recovers; a further readiness frame arrives. sessionReady
    // never transitioned false→true, so this is the case the old latch
    // dropped on the floor.
    ctx.mockHttp.post.mockReturnValue(of({ accepted: true, turn_id: 1 }));
    ctx.mockHttp.post.mockClear();
    fireSseMessage(ctx.sseInstances[0], { method: 'ready', params: {} }, '1:2');
    await new Promise((r) => setTimeout(r, 0));

    const inputCalls = ctx.mockHttp.post.mock.calls.filter((c: any) =>
      String(c[0]).endsWith('/persistent/threads/thread-r/input'),
    );
    expect(inputCalls).toHaveLength(1);
    expect(inputCalls[0][1]).toEqual({
      content: 'stalled by transport',
      expected_conversation_revision: 0,
    });
    expect(ctx.service.outbox()).toEqual([]);
    expect(ctx.service.outboxStalled()).toBe(false);
    expect(ctx.service.error()).toBeNull();
  });

  it('surfaces human copy (not Angular internals) when the fetch itself fails', async () => {
    const ctx = await readySession();
    // Exactly what Angular's fetch backend emits when fetch() rejects:
    // status 0, statusText undefined, message built from both.
    ctx.mockHttp.post.mockReturnValue(
      throwError(() => ({
        status: 0,
        message:
          'Http failure response for https://api.example/api/persistent/threads/t/input: 0 undefined',
      })),
    );
    await ctx.service.sendMessage('offline send');
    await new Promise((r) => setTimeout(r, 0));

    const banner = ctx.service.error();
    expect(banner).not.toBeNull();
    expect(banner).not.toContain('Http failure response');
    expect(banner).not.toContain('undefined');
    expect(banner).toContain("Couldn't reach the server");
    // Not terminal: the message is kept for a retry.
    expect(ctx.service.outbox().length).toBe(1);
    expect(ctx.service.outboxStalled()).toBe(true);
  });

  it('retryQueuedSends re-POSTs a stalled item and clears the stall', async () => {
    const ctx = await readySession();
    ctx.mockHttp.post.mockReturnValue(throwError(() => ({ status: 0 })));
    await ctx.service.sendMessage('retry me');
    await new Promise((r) => setTimeout(r, 0));
    expect(ctx.service.outboxStalled()).toBe(true);

    ctx.mockHttp.post.mockReturnValue(of({ accepted: true, turn_id: 1 }));
    ctx.mockHttp.post.mockClear();
    ctx.service.retryQueuedSends();
    await new Promise((r) => setTimeout(r, 0));

    const inputCalls = ctx.mockHttp.post.mock.calls.filter((c: any) =>
      String(c[0]).endsWith('/persistent/threads/thread-r/input'),
    );
    expect(inputCalls).toHaveLength(1);
    expect(ctx.service.outbox()).toEqual([]);
    expect(ctx.service.outboxStalled()).toBe(false);
    expect(ctx.service.error()).toBeNull();
    // The bubble stays — SSE renders the turn from here.
    expect(ctx.service.turns().some((t) => isUserTurn(t) && t.content === 'retry me')).toBe(true);
  });

  it('discardQueuedSend drops the queued item and its optimistic bubble', async () => {
    const ctx = await readySession();
    ctx.mockHttp.post.mockReturnValue(throwError(() => ({ status: 0 })));
    await ctx.service.sendMessage('forget this');
    await new Promise((r) => setTimeout(r, 0));
    expect(ctx.service.outbox().length).toBe(1);

    const localId = ctx.service.outbox()[0].localId;
    ctx.service.discardQueuedSend(localId);

    expect(ctx.service.outbox()).toEqual([]);
    expect(ctx.service.outboxStalled()).toBe(false);
    expect(ctx.service.error()).toBeNull();
    expect(ctx.service.turns().some((t) => isUserTurn(t) && t.content === 'forget this')).toBe(
      false,
    );
  });

  it('keeps the bubble and queued identity on a generic 409', async () => {
    const ctx = await readySession();
    ctx.mockHttp.post.mockReturnValue(throwError(() => ({ status: 409 })));
    const ok = await ctx.service.sendMessage('dup');
    expect(ok).toBe(true);
    const present = ctx.service.turns().some((t) => isUserTurn(t) && t.content === 'dup');
    expect(present).toBe(true);
    expect(ctx.service.outbox()).toHaveLength(1);
    expect(ctx.service.error()).not.toBeNull();
  });

  it('retires sibling-tab control on session_ending without claiming its input', async () => {
    const ctx = await readySession();
    const es = ctx.sseInstances[0];
    ctx.mockHttp.post.mockReturnValue(
      throwError(() => ({
        status: 409,
        error: {
          detail: {
            code: 'session_ending',
            message: 'Session retirement is in progress',
            retirement_disposition: 'ended',
          },
        },
      })),
    );

    await ctx.service.sendMessage('must not be mistaken for tab A');
    await Promise.resolve();

    expect(ctx.service.threadStatus()).toBe('ending');
    expect(ctx.service.sessionReady()).toBe(false);
    expect(ctx.service.outbox().map((item) => item.displayContent)).toEqual([
      'must not be mistaken for tab A',
    ]);
    expect(ctx.service.pendingTurnCount()).toBe(0);
    expect(es.close).not.toHaveBeenCalled();
  });

  it('interrupt POSTs to /interrupt', async () => {
    const ctx = await readySession();
    fireSseMessage(ctx.sseInstances[0], { method: 'turn.started', params: { turn_id: 3 } }, '1:2');
    await ctx.service.interrupt();
    const calls = ctx.mockHttp.post.mock.calls;
    const intCall = calls.find((c: any) =>
      String(c[0]).endsWith('/persistent/threads/thread-r/interrupt'),
    );
    expect(intCall).toBeDefined();
    expect(intCall![1]).toMatchObject({ target_turn_id: 3 });
    expect(intCall![1].client_request_id).toMatch(
      /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i,
    );
    expect(ctx.service.isInterrupting()).toBe(true);
  });

  it('does not create an uncorrelated interrupt without a numeric active turn', async () => {
    const ctx = await readySession();

    await ctx.service.interrupt();

    expect(
      ctx.mockHttp.post.mock.calls.some((c: any) =>
        String(c[0]).endsWith('/persistent/threads/thread-r/interrupt'),
      ),
    ).toBe(false);
    expect(ctx.service.isInterrupting()).toBe(false);
  });

  it('a mid-stage upload failure keeps the landed file done, and a retry only re-uploads the failed one', async () => {
    // Regression coverage for the per-file upload stage: file 1 succeeds,
    // file 2 hits the 100MB cap (413). Pre-Task-2 the catch blanket-marked
    // BOTH files failed and discarded file 1's result — a retry then
    // re-uploaded file 1 too, permanently duplicating it server-side (no
    // delete endpoint, no idempotency key). Since Task 4 the per-file
    // result is cached on the OUTBOX ITEM (the previews are gone from the
    // composer the moment the user sends), so that is where the
    // never-re-upload-a-success invariant now has to hold.
    const ctx = await readySession();

    const fileA = new File(['a'], 'a.png', { type: 'image/png' });
    const fileB = new File(['b'], 'huge.bin', { type: 'application/octet-stream' });
    const previewA: any = {
      id: '1',
      file: fileA,
      name: 'a.png',
      size: fileA.size,
      mimeType: 'image/png',
      uploadStatus: UploadStatus.PENDING,
    };
    const previewB: any = {
      id: '2',
      file: fileB,
      name: 'huge.bin',
      size: fileB.size,
      mimeType: 'application/octet-stream',
      uploadStatus: UploadStatus.PENDING,
    };
    // Set BEFORE attaching: since §5.4 the upload starts on attach, so the
    // default mock would otherwise answer the eager requests.
    ctx.mockApi.uploadOneToThread.mockImplementation((_threadId: string, file: File) =>
      file === fileB
        ? throwError(() => ({ status: 413, error: { detail: "File 'huge.bin' exceeds 100MB" } }))
        : of({
            kind: 'done',
            files: [
              {
                name: file.name,
                size: file.size,
                mime_type: file.type,
                path: `uploads/${file.name}`,
              },
            ],
          }),
    );
    const sends = (f: File) =>
      ctx.mockApi.uploadOneToThread.mock.calls.filter((c: any) => c[1] === f).length;

    ctx.service.addAttachments([previewA, previewB]);

    // The send is committed regardless of the upload's fate: bubble,
    // queue entry, cleared composer.
    const ok1 = await ctx.service.sendMessage('two files');
    await new Promise((r) => setTimeout(r, 0));

    expect(ok1).toBe(true);
    expect(ctx.service.pendingAttachments()).toEqual([]);
    // a.png was uploaded eagerly on attach and ADOPTED by the send, so it
    // crossed the wire once; huge.bin's eager attempt failed and left no
    // trace, so the send retried it on the deferred path.
    expect(sends(fileA)).toBe(1);
    expect(sends(fileB)).toBe(2);
    // Nothing was POSTed — the queue stalled at stage 0.
    const failedInput = ctx.mockHttp.post.mock.calls.filter((c: any) =>
      String(c[0]).endsWith('/persistent/threads/thread-r/input'),
    );
    expect(failedInput).toHaveLength(0);
    expect(ctx.service.outboxStalled()).toBe(true);
    // File 1 already landed server-side — its `done` status and
    // server-assigned path must survive the file-2 failure.
    const files1 = ctx.service.outbox()[0].pendingFiles!;
    expect(files1.map((f) => f.status)).toEqual(['done', 'failed']);
    expect(files1[0].resolved).toEqual({
      id: '1',
      name: 'a.png',
      size: fileA.size,
      mimeType: 'image/png',
      path: 'uploads/a.png',
    });
    expect(files1[1].error).toBe('upload failed');
    // 413 is terminal, so the banner explains rather than just stalling.
    expect(ctx.service.error()).toBe('upload failed');

    // Retry the queued item — only the failed file should be re-sent.
    ctx.mockApi.uploadOneToThread.mockClear();
    ctx.mockApi.uploadOneToThread.mockImplementation((_threadId: string, file: File) =>
      of({
        kind: 'done',
        files: [
          { name: file.name, size: file.size, mime_type: file.type, path: `uploads/${file.name}` },
        ],
      }),
    );

    ctx.service.retryQueuedSends();
    await new Promise((r) => setTimeout(r, 0));

    expect(ctx.mockApi.uploadOneToThread).toHaveBeenCalledTimes(1);
    expect(ctx.mockApi.uploadOneToThread).toHaveBeenCalledWith('thread-r', fileB);
    const sentInput = ctx.mockHttp.post.mock.calls.filter((c: any) =>
      String(c[0]).endsWith('/persistent/threads/thread-r/input'),
    );
    expect(sentInput).toHaveLength(1);
    expect(sentInput[0][1].content).toBe(
      'two files\n\n[Attached files in uploads/: a.png, huge.bin]',
    );
    expect(ctx.service.outbox()).toEqual([]);
  });
});

describe('PersistentChatService — resume re-validation (visibility/online)', () => {
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

  async function connectOpen(threadId: string) {
    const ctx = createService();
    ctx.mockHttp.get.mockImplementation(activeSessionGet);
    await ctx.service.connect(threadId);
    await new Promise((r) => setTimeout(r, 0));
    fireSseOpen(ctx.sseInstances[0]);
    return ctx;
  }

  it('force-reopens a stale SSE when the tab regains liveness (online event)', async () => {
    const ctx = await connectOpen('thread-resume-stale');
    const first = ctx.sseInstances[0];
    // Simulate the watchdog having been frozen while the tab was
    // backgrounded: the last SSE event is now past the 45s threshold.
    (ctx.service as any).sseLastEventAt = Date.now() - 60_000;

    window.dispatchEvent(new Event('online'));
    await new Promise((r) => setTimeout(r, 0));

    expect(first.close).toHaveBeenCalled();
    expect(ctx.sseInstances.length).toBe(2);
    expect(ctx.service.connectionState()).toBe('connecting');
  });

  it('does not tear down a healthy SSE on resume', async () => {
    const ctx = await connectOpen('thread-resume-fresh');
    const first = ctx.sseInstances[0];
    // Fresh liveness — an event arrived just now.
    (ctx.service as any).sseLastEventAt = Date.now();

    window.dispatchEvent(new Event('online'));
    await new Promise((r) => setTimeout(r, 0));

    expect(first.close).not.toHaveBeenCalled();
    expect(ctx.sseInstances.length).toBe(1);
  });

  it('is a no-op when there is no active thread', async () => {
    const ctx = createService();
    // No connect() — threadId is null.
    window.dispatchEvent(new Event('online'));
    await new Promise((r) => setTimeout(r, 0));
    expect(ctx.sseInstances.length).toBe(0);
  });

  it('re-ensures the control WS when the SSE recovers (slaved liveness)', async () => {
    const ctx = await connectOpen('thread-ws-slave');
    expect(ctx.wsInstances.length).toBe(1);

    // Simulate the control WS having silently died — on a real drop the
    // readyState moves to CLOSED, but no onclose fires to reconnect it.
    (ctx.service as any).controlWs.readyState = 3; // CLOSED

    // Force an SSE reconnect; firing open on the new stream is the
    // "wasReconnecting" path, which re-ensures the control WS.
    ctx.service.reconnectNow();
    await new Promise((r) => setTimeout(r, 0));
    fireSseOpen(ctx.sseInstances[1]);
    await new Promise((r) => setTimeout(r, 0));

    expect(ctx.wsInstances.length).toBe(2);
  });
});

describe('PersistentChatService — control commands', () => {
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

  async function readySession() {
    const ctx = createService();
    ctx.mockHttp.get.mockImplementation(activeSessionGet);
    await ctx.service.connect('thread-c');
    fireSseOpen(ctx.sseInstances[0]);
    fireSseMessage(ctx.sseInstances[0], { method: 'ready', params: {} }, '1:1');
    // Clear the send spy so test sees only its own calls.
    ctx.wsInstances[0].send.mockClear();
    return ctx;
  }

  async function readySocketlessSession(threadId = 'thread-socketless-undo') {
    const ctx = createService();
    ctx.mockHttp.get.mockImplementation((url: string) =>
      url.endsWith('/connection')
        ? of({
            state: 'ready',
            control_socket: 'none',
            ws_url: null,
            token: null,
            expires_at: null,
            pinned_runtime_generation_contract: 1,
            session_runtime_generation: SESSION_RUNTIME_GENERATION,
          })
        : activeSessionGet(url),
    );
    await ctx.service.connect(threadId);
    fireSseOpen(ctx.sseInstances[0]);
    ctx.mockHttp.post.mockClear();
    return ctx;
  }

  it('approveAll() sends {method: "approve"} over the control WS', async () => {
    const ctx = await readySession();
    // Stage a pending permission so approveAll() has something to clear.
    (ctx.service as any).pendingPermissions.set([
      {
        id: 'tc-1',
        tool: 'run_command',
        args: {},
      },
    ]);

    ctx.service.approveAll();
    const sent = ctx.wsInstances[0].send.mock.calls.map((c: any) => JSON.parse(c[0]));
    expect(sent).toContainEqual({ method: 'approve' });
    expect(ctx.service.pendingPermissions()).toEqual([]);
  });

  it('approveAll() resolves durable approval requests through REST', async () => {
    const ctx = await readySession();
    ctx.mockHttp.post.mockClear();
    ctx.wsInstances[0].send.mockClear();
    (ctx.service as any).pendingPermissions.set([
      {
        id: 'tc-rest',
        approvalId: 'approval-1',
        tool: 'run_command',
        args: {},
      },
    ]);

    ctx.service.approveAll();

    expect(ctx.mockHttp.post).toHaveBeenCalledWith(
      expect.stringContaining('/persistent/threads/thread-c/approve/approval-1'),
      { decision: 'approve' },
    );
    expect(ctx.wsInstances[0].send).not.toHaveBeenCalled();
    expect(ctx.service.pendingPermissions()).toHaveLength(1);
    fireSseMessage(
      ctx.sseInstances[0],
      {
        method: 'permission.resolved',
        params: { id: 'tc-rest', decision: 'approved' },
      },
      '1:2',
    );
    expect(ctx.service.pendingPermissions()).toEqual([]);
  });

  it('denyAll() resolves durable approval requests through REST', async () => {
    const ctx = await readySession();
    ctx.mockHttp.post.mockClear();
    ctx.wsInstances[0].send.mockClear();
    (ctx.service as any).pendingPermissions.set([
      {
        id: 'tc-deny-rest',
        approvalId: 'approval-2',
        tool: 'run_command',
        args: {},
      },
    ]);

    ctx.service.denyAll();

    expect(ctx.mockHttp.post).toHaveBeenCalledWith(
      expect.stringContaining('/persistent/threads/thread-c/approve/approval-2'),
      { decision: 'deny' },
    );
    expect(ctx.wsInstances[0].send).not.toHaveBeenCalled();
    expect(ctx.service.pendingPermissions()).toHaveLength(1);
    fireSseMessage(
      ctx.sseInstances[0],
      {
        method: 'permission.resolved',
        params: { id: 'tc-deny-rest', decision: 'denied' },
      },
      '1:2',
    );
    expect(ctx.service.pendingPermissions()).toEqual([]);
  });

  it('does not resurrect a durable card when journal resolution beats a masked POST error', async () => {
    const ctx = await readySession();
    const response = new Subject<Record<string, unknown>>();
    ctx.mockHttp.post.mockReturnValue(response.asObservable());
    ctx.wsInstances[0].send.mockClear();
    (ctx.service as any).pendingPermissions.set([
      {
        id: 'tc-masked',
        approvalId: 'approval-masked',
        tool: 'run_command',
        args: {},
      },
    ]);

    ctx.service.approveAll();
    expect(ctx.service.pendingPermissions()).toHaveLength(1);
    fireSseMessage(
      ctx.sseInstances[0],
      {
        method: 'permission.resolved',
        params: { id: 'tc-masked', decision: 'approved' },
      },
      '1:2',
    );
    response.error({ status: 0 });

    expect(ctx.service.pendingPermissions()).toEqual([]);
    expect(ctx.service.error()).toBeNull();
    expect(ctx.wsInstances[0].send).not.toHaveBeenCalled();
    expect((ctx.service as any).controlOutbox).toEqual([]);
  });

  it('denyAll() sends {method: "deny"} and seeds the denied tool call in the active turn', async () => {
    const ctx = await readySession();
    // Real permission.request always fires inside a turn — set that up.
    fireSseMessage(ctx.sseInstances[0], { method: 'turn.started', params: { turn_id: 1 } }, '1:2');
    (ctx.service as any).pendingPermissions.set([
      {
        id: 'tc-2',
        tool: 'rm_rf',
        args: { path: '/' },
      },
    ]);

    ctx.service.denyAll();
    const sent = ctx.wsInstances[0].send.mock.calls.map((c: any) => JSON.parse(c[0]));
    expect(sent).toContainEqual({ method: 'deny' });
    const turn = ctx.service.currentStreamingTurn()!;
    const denied = turn.events.filter(isToolCall).find((tc) => tc.id === 'tc-2') as ToolCallEvent;
    expect(denied).toBeDefined();
    expect(denied.status).toBe('denied');
    expect(denied.decision).toBe('denied');
  });

  it('/compact slash command sends compact without a local echo', async () => {
    // No "Compacting context..." system turn: the agent's
    // compaction.started/progress frames drive the live progress block,
    // and a no-op answers with a summary-less context.compacted.
    const ctx = await readySession();
    await ctx.service.sendMessage('/compact recent edits');
    const sent = ctx.wsInstances[0].send.mock.calls.map((c: any) => JSON.parse(c[0]));
    expect(sent).toContainEqual({ method: 'compact', focus: 'recent edits' });
    const systemTurns = ctx.service.turns().filter(isSystemTurn);
    expect(systemTurns.some((t) => /Compacting/.test(String(t.content)))).toBe(false);
  });

  it('refuses a slash command while files are attached instead of stranding the chips', async () => {
    // The slash bypass returns before the attachment path, so `/compact`
    // with a queued file used to run the command and silently leave the
    // chips in the composer with no message and no error (spec §2).
    const ctx = await readySession();
    const file = new File(['a'], 'a.png', { type: 'image/png' });
    ctx.service.addAttachments([
      {
        id: '1',
        file,
        name: 'a.png',
        size: file.size,
        mimeType: 'image/png',
        uploadStatus: UploadStatus.PENDING,
      } as any,
    ]);

    const ok = await ctx.service.sendMessage('/compact recent edits');

    expect(ok).toBe(false);
    expect(ctx.service.attachmentError()).toBe('chat.upload.slashCommandWithAttachments');
    // Command not run, files not sent, chips still there to be removed.
    const sent = ctx.wsInstances[0].send.mock.calls.map((c: any) => JSON.parse(c[0]));
    expect(sent.some((m: any) => m.method === 'compact')).toBe(false);
    expect(ctx.service.pendingAttachments()).toHaveLength(1);
    expect(ctx.service.outbox()).toEqual([]);
  });

  it('/done sends archive', async () => {
    const ctx = await readySession();
    await ctx.service.sendMessage('/done');
    const sent = ctx.wsInstances[0].send.mock.calls.map((c: any) => JSON.parse(c[0]));
    expect(sent).toContainEqual({ method: 'archive' });
  });

  it('/undo preserves the pinned legacy WebSocket transport exactly', async () => {
    const ctx = await readySession();
    ctx.mockHttp.post.mockClear();
    await ctx.service.sendMessage('/undo');
    const sent = ctx.wsInstances[0].send.mock.calls.map((c: any) => JSON.parse(c[0]));
    expect(sent).toContainEqual({ method: 'undo' });
    expect(
      ctx.mockHttp.post.mock.calls.some((call: any[]) => String(call[0]).endsWith('/controls')),
    ).toBe(false);
  });

  it('/undo submits workspace.undo through REST for a socketless session', async () => {
    const ctx = await readySocketlessSession();

    await ctx.service.sendMessage('/undo');

    expect(ctx.wsInstances).toHaveLength(0);
    expect(ctx.mockHttp.post).toHaveBeenCalledWith(
      expect.stringMatching(/\/persistent\/threads\/thread-socketless-undo\/controls$/),
      {
        method: 'workspace.undo',
        client_request_id: expect.any(String),
        session_runtime_generation: SESSION_RUNTIME_GENERATION,
      },
    );
  });

  it('/undo retries an ambiguous socketless admission with the same UUID', async () => {
    vi.useFakeTimers();
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => undefined);
    const ctx = await readySocketlessSession('thread-undo-retry');
    try {
      ctx.mockHttp.post
        .mockReturnValueOnce(throwError(() => ({ status: 0 })))
        .mockReturnValueOnce(of({ accepted: true, duplicate: true }));

      await ctx.service.sendMessage('/undo');
      const firstRequest = ctx.mockHttp.post.mock.calls[0][1];
      await vi.advanceTimersByTimeAsync(250);

      expect(ctx.mockHttp.post).toHaveBeenCalledTimes(2);
      expect(ctx.mockHttp.post.mock.calls[1][1]).toEqual(firstRequest);
      expect(firstRequest).toMatchObject({ method: 'workspace.undo' });
    } finally {
      ctx.service.disconnect();
      warn.mockRestore();
      vi.useRealTimers();
    }
  });

  it('/undo clears only its request-correlated durable acknowledgement', async () => {
    const ctx = await readySocketlessSession('thread-undo-ack');
    await ctx.service.sendMessage('/undo');
    const requestId = ctx.mockHttp.post.mock.calls[0][1].client_request_id;
    expect((ctx.service as any).durableControlAwaitingAck.size).toBe(1);

    fireSseMessage(
      ctx.sseInstances[0],
      {
        method: 'files.restored',
        params: {
          method: 'workspace.undo',
          client_request_id: crypto.randomUUID(),
          paths: ['unrelated.txt'],
        },
      },
      '1:2',
    );
    expect((ctx.service as any).durableControlAwaitingAck.size).toBe(1);

    fireSseMessage(
      ctx.sseInstances[0],
      {
        method: 'files.restored',
        params: {
          method: 'workspace.undo',
          client_request_id: requestId,
          paths: ['restored.txt'],
        },
      },
      '1:3',
    );
    expect((ctx.service as any).durableControlAwaitingAck.size).toBe(0);
  });

  it('/undo never coalesces repeated socketless operations', async () => {
    const ctx = await readySocketlessSession('thread-undo-fifo');
    const firstAdmission = new Subject<Record<string, unknown>>();
    ctx.mockHttp.post
      .mockReturnValueOnce(firstAdmission.asObservable())
      .mockReturnValue(of({ accepted: true }));

    await ctx.service.sendMessage('/undo');
    await ctx.service.sendMessage('/undo');
    await ctx.service.sendMessage('/undo');

    expect(ctx.mockHttp.post).toHaveBeenCalledTimes(1);
    const queued = (ctx.service as any).durableControlOutbox.map((item: any) => item.request);
    expect(queued.map((request: any) => request.method)).toEqual([
      'workspace.undo',
      'workspace.undo',
      'workspace.undo',
    ]);
    expect(new Set(queued.map((request: any) => request.client_request_id)).size).toBe(3);

    firstAdmission.next({ accepted: true });
    expect(ctx.mockHttp.post).toHaveBeenCalledTimes(3);
  });

  it('/upgrade-workspace defaults to the sandbox tier', async () => {
    const ctx = await readySession();
    await ctx.service.sendMessage('/upgrade-workspace');
    const sent = ctx.wsInstances[0].send.mock.calls.map((c: any) => JSON.parse(c[0]));
    expect(sent).toContainEqual({ method: 'upgrade-to-workspace', target_tier: 'sandbox' });
  });

  it('/upgrade-workspace vm requests the vm tier (Phase 2, server-gated)', async () => {
    const ctx = await readySession();
    await ctx.service.sendMessage('/upgrade-workspace vm');
    const sent = ctx.wsInstances[0].send.mock.calls.map((c: any) => JSON.parse(c[0]));
    expect(sent).toContainEqual({ method: 'upgrade-to-workspace', target_tier: 'vm' });
  });

  it('setMode uses the lane-agnostic REST inbox and never the control WS', async () => {
    const ctx = await readySession();
    ctx.mockHttp.post.mockClear();
    ctx.service.setMode('auto_accept');
    expect(ctx.mockHttp.post).toHaveBeenCalledWith(
      expect.stringMatching(/\/persistent\/threads\/thread-c\/controls$/),
      {
        method: 'mode.set',
        mode: 'auto_accept',
        client_request_id: expect.any(String),
        session_runtime_generation: SESSION_RUNTIME_GENERATION,
      },
    );
    expect(ctx.wsInstances[0].send).not.toHaveBeenCalled();
  });

  it('setNarrationMode uses the same REST inbox and never the control WS', async () => {
    const ctx = await readySession();
    ctx.mockHttp.post.mockClear();
    ctx.service.setNarrationMode('silent');
    expect(ctx.mockHttp.post).toHaveBeenCalledWith(
      expect.stringMatching(/\/persistent\/threads\/thread-c\/controls$/),
      {
        method: 'narration.set',
        mode: 'silent',
        client_request_id: expect.any(String),
        session_runtime_generation: SESSION_RUNTIME_GENERATION,
      },
    );
    expect(ctx.wsInstances[0].send).not.toHaveBeenCalled();
  });

  it('submits a setting when the session has no control socket', async () => {
    const ctx = createService();
    ctx.mockHttp.get.mockImplementation((url: string) =>
      url.endsWith('/connection')
        ? of({
            state: 'ready',
            control_socket: 'none',
            ws_url: null,
            token: null,
            expires_at: null,
            pinned_runtime_generation_contract: 1,
            session_runtime_generation: SESSION_RUNTIME_GENERATION,
          })
        : activeSessionGet(url),
    );
    await ctx.service.connect('thread-socketless-control');
    fireSseOpen(ctx.sseInstances[0]);
    ctx.mockHttp.post.mockClear();

    ctx.service.setMode('autonomous');

    expect(ctx.wsInstances).toHaveLength(0);
    expect(ctx.mockHttp.post).toHaveBeenCalledWith(
      expect.stringMatching(/\/persistent\/threads\/thread-socketless-control\/controls$/),
      expect.objectContaining({ method: 'mode.set', mode: 'autonomous' }),
    );
  });

  it.each([
    ['missing generation', undefined, 1],
    ['malformed generation', 'not-a-runtime-generation', 1],
    ['malformed contract', SESSION_RUNTIME_GENERATION, true],
  ])('fails durable controls closed for %s', async (_label, generation, contract) => {
    const ctx = createService();
    ctx.mockHttp.get.mockImplementation((url: string) =>
      url.endsWith('/connection')
        ? of({
            state: 'ready',
            control_socket: 'none',
            ws_url: null,
            token: null,
            expires_at: null,
            pinned_runtime_generation_contract: contract,
            session_runtime_generation: generation,
          })
        : activeSessionGet(url),
    );
    await ctx.service.connect('thread-control-fail-closed');
    fireSseOpen(ctx.sseInstances[0]);
    ctx.mockHttp.post.mockClear();

    ctx.service.setMode('autonomous');

    expect(
      ctx.mockHttp.post.mock.calls.some((call: any[]) => String(call[0]).endsWith('/controls')),
    ).toBe(false);
    expect(ctx.service.error()).not.toBeNull();
  });

  it('treats HTTP success as admission only and applies the journal result', async () => {
    const ctx = await readySession();
    ctx.mockHttp.post.mockClear();
    ctx.mockHttp.post.mockReturnValue(
      of({
        accepted: true,
        method: 'mode.set',
        // A response-body scalar must never masquerade as the owner ack.
        mode: 'autonomous',
      }),
    );

    ctx.service.setMode('auto_accept');
    // Neither the optimistic click nor the HTTP response is authority.
    expect(ctx.service.permissionMode()).toBe('supervised');
    const requestId = ctx.mockHttp.post.mock.calls[0][1].client_request_id;

    fireSseMessage(
      ctx.sseInstances[0],
      {
        method: 'mode.changed',
        params: { mode: 'autonomous', client_request_id: requestId },
      },
      '1:2',
    );
    expect(ctx.service.permissionMode()).toBe('autonomous');
  });

  it('correlates an owner acknowledgement that beats the HTTP 202', async () => {
    const ctx = await readySession();
    const delayedAdmission = new Subject<Record<string, unknown>>();
    ctx.mockHttp.post.mockClear();
    ctx.mockHttp.post.mockReturnValue(delayedAdmission.asObservable());

    ctx.service.setMode('auto_accept');
    const requestId = ctx.mockHttp.post.mock.calls[0][1].client_request_id;
    expect((ctx.service as any).durableControlAwaitingAck.size).toBe(1);

    fireSseMessage(
      ctx.sseInstances[0],
      {
        method: 'mode.changed',
        params: { mode: 'auto_accept', client_request_id: requestId },
      },
      '1:2',
    );
    expect((ctx.service as any).durableControlAwaitingAck.size).toBe(0);

    delayedAdmission.next({ accepted: true });
    delayedAdmission.complete();
    expect((ctx.service as any).durableControlAwaitingAck.size).toBe(0);
    expect(ctx.service.permissionMode()).toBe('auto_accept');
  });

  it('serializes REST controls so a later setting cannot overtake admission', async () => {
    const ctx = await readySession();
    const firstAdmission = new Subject<Record<string, unknown>>();
    ctx.mockHttp.post.mockClear();
    ctx.mockHttp.post
      .mockReturnValueOnce(firstAdmission.asObservable())
      .mockReturnValueOnce(of({ accepted: true }));

    ctx.service.setMode('auto_accept');
    ctx.service.setNarrationMode('silent');

    expect(ctx.mockHttp.post).toHaveBeenCalledTimes(1);
    expect(ctx.mockHttp.post.mock.calls[0][1]).toMatchObject({ method: 'mode.set' });

    firstAdmission.next({ accepted: true });

    expect(ctx.mockHttp.post).toHaveBeenCalledTimes(2);
    expect(ctx.mockHttp.post.mock.calls[1][1]).toMatchObject({
      method: 'narration.set',
    });
  });

  it('coalesces only unsent controls of the same scalar without reordering the other scalar', async () => {
    const ctx = await readySession();
    const firstAdmission = new Subject<Record<string, unknown>>();
    ctx.mockHttp.post.mockClear();
    ctx.mockHttp.post
      .mockReturnValueOnce(firstAdmission.asObservable())
      .mockReturnValue(of({ accepted: true }));

    ctx.service.setMode('auto_accept');
    ctx.service.setNarrationMode('silent');
    ctx.service.setMode('autonomous');
    ctx.service.setMode('supervised');

    expect(ctx.mockHttp.post).toHaveBeenCalledTimes(1);
    const queued = (ctx.service as any).durableControlOutbox.map((item: any) => item.request);
    expect(queued.map((request: any) => request.method)).toEqual([
      'mode.set',
      'narration.set',
      'mode.set',
    ]);
    expect(queued.map((request: any) => request.mode)).toEqual([
      'auto_accept',
      'silent',
      'supervised',
    ]);

    firstAdmission.next({ accepted: true });

    expect(ctx.mockHttp.post).toHaveBeenCalledTimes(3);
    expect(ctx.mockHttp.post.mock.calls.map((call: any[]) => call[1].mode)).toEqual([
      'auto_accept',
      'silent',
      'supervised',
    ]);
  });

  it('caps the durable control outbox and reports backpressure', async () => {
    const ctx = await readySession();
    ctx.mockHttp.post.mockClear();
    (ctx.service as any).durableControlOutbox = Array.from({ length: 32 }, (_, index) => ({
      threadId: `other-thread-${index}`,
      request: {
        method: 'mode.set',
        mode: 'supervised',
        client_request_id: `existing-${index}`,
      },
      attempts: 0,
    }));

    ctx.service.setMode('auto_accept');

    expect(ctx.mockHttp.post).not.toHaveBeenCalled();
    expect((ctx.service as any).durableControlOutbox).toHaveLength(32);
    expect(ctx.service.error()).toBe('chat.control.backpressure');
  });

  it('retries an ambiguous admission with the same UUID before later controls', async () => {
    vi.useFakeTimers();
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => undefined);
    const ctx = await readySession();
    try {
      ctx.mockHttp.post.mockClear();
      ctx.mockHttp.post
        .mockReturnValueOnce(throwError(() => ({ status: 0 })))
        .mockReturnValueOnce(of({ accepted: true, duplicate: true }))
        .mockReturnValueOnce(of({ accepted: true }));

      ctx.service.setMode('auto_accept');
      ctx.service.setNarrationMode('verbose');

      expect(ctx.mockHttp.post).toHaveBeenCalledTimes(1);
      await vi.advanceTimersByTimeAsync(250);

      expect(ctx.mockHttp.post).toHaveBeenCalledTimes(3);
      const requests = ctx.mockHttp.post.mock.calls.map((call: any[]) => call[1]);
      expect(requests.map((request: any) => request.method)).toEqual([
        'mode.set',
        'mode.set',
        'narration.set',
      ]);
      expect(requests[1].client_request_id).toBe(requests[0].client_request_id);
      expect(requests[2].client_request_id).not.toBe(requests[0].client_request_id);
    } finally {
      ctx.service.disconnect();
      warn.mockRestore();
      vi.useRealTimers();
    }
  });

  it('retries a 425 owner-not-ready response with the same UUID', async () => {
    vi.useFakeTimers();
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => undefined);
    const ctx = await readySession();
    try {
      ctx.mockHttp.post.mockClear();
      ctx.mockHttp.post
        .mockReturnValueOnce(throwError(() => ({ status: 425 })))
        .mockReturnValueOnce(of({ accepted: true }));

      ctx.service.setNarrationMode('verbose');
      const firstRequest = ctx.mockHttp.post.mock.calls[0][1];
      await vi.advanceTimersByTimeAsync(250);

      expect(ctx.mockHttp.post).toHaveBeenCalledTimes(2);
      expect(ctx.mockHttp.post.mock.calls[1][1]).toEqual(firstRequest);
    } finally {
      ctx.service.disconnect();
      warn.mockRestore();
      vi.useRealTimers();
    }
  });

  it('never replaces an ambiguous retry head with a newer same-scalar UUID', async () => {
    vi.useFakeTimers();
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => undefined);
    const ctx = await readySession();
    try {
      ctx.mockHttp.post.mockClear();
      ctx.mockHttp.post
        .mockReturnValueOnce(throwError(() => ({ status: 0 })))
        .mockReturnValueOnce(of({ accepted: true, duplicate: true }))
        .mockReturnValueOnce(of({ accepted: true }));

      ctx.service.setMode('auto_accept');
      const ambiguous = ctx.mockHttp.post.mock.calls[0][1];
      ctx.service.setMode('autonomous');

      const queued = (ctx.service as any).durableControlOutbox.map((item: any) => item.request);
      expect(queued.map((request: any) => request.mode)).toEqual(['auto_accept', 'autonomous']);
      expect(queued[0].client_request_id).toBe(ambiguous.client_request_id);

      await vi.advanceTimersByTimeAsync(250);

      expect(ctx.mockHttp.post).toHaveBeenCalledTimes(3);
      const requests = ctx.mockHttp.post.mock.calls.map((call: any[]) => call[1]);
      expect(requests.map((request: any) => request.mode)).toEqual([
        'auto_accept',
        'auto_accept',
        'autonomous',
      ]);
      expect(requests[1].client_request_id).toBe(ambiguous.client_request_id);
      expect(requests[2].client_request_id).not.toBe(ambiguous.client_request_id);
    } finally {
      ctx.service.disconnect();
      warn.mockRestore();
      vi.useRealTimers();
    }
  });

  it('times out a hung admission and retries it with the same UUID', async () => {
    vi.useFakeTimers();
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => undefined);
    const ctx = await readySession();
    try {
      ctx.mockHttp.post.mockClear();
      ctx.mockHttp.post
        .mockReturnValueOnce(NEVER)
        .mockReturnValueOnce(of({ accepted: true, duplicate: true }));

      ctx.service.setMode('auto_accept');
      const firstRequest = ctx.mockHttp.post.mock.calls[0][1];

      await vi.advanceTimersByTimeAsync(14_999);
      expect(ctx.mockHttp.post).toHaveBeenCalledTimes(1);

      await vi.advanceTimersByTimeAsync(1);
      expect(ctx.mockHttp.post).toHaveBeenCalledTimes(1);

      await vi.advanceTimersByTimeAsync(250);
      expect(ctx.mockHttp.post).toHaveBeenCalledTimes(2);
      const retryRequest = ctx.mockHttp.post.mock.calls[1][1];
      expect(retryRequest.client_request_id).toBe(firstRequest.client_request_id);
      expect(retryRequest).toMatchObject({ method: 'mode.set', mode: 'auto_accept' });
    } finally {
      ctx.service.disconnect();
      warn.mockRestore();
      vi.useRealTimers();
    }
  });

  it('surfaces an owner control rejection from the durable journal', async () => {
    const ctx = await readySession();

    fireSseMessage(
      ctx.sseInstances[0],
      {
        method: 'control.rejected',
        params: { method: 'mode.set', error_code: 'stale_owner' },
      },
      '1:2',
    );

    expect(ctx.service.error()).toBe('chat.control.ownerRejected');
  });

  it('only a newer same-scalar acknowledgement clears a local admission error', async () => {
    const ctx = await readySession();
    ctx.mockHttp.post.mockClear();
    ctx.mockHttp.post
      .mockReturnValueOnce(of({ accepted: true }))
      .mockReturnValueOnce(throwError(() => ({ status: 409 })))
      .mockReturnValueOnce(of({ accepted: true }));

    ctx.service.setMode('auto_accept');
    const olderRequestId = ctx.mockHttp.post.mock.calls[0][1].client_request_id;
    ctx.service.setMode('autonomous');
    expect(ctx.service.error()).toBe('chat.control.admissionFailed');

    fireSseMessage(
      ctx.sseInstances[0],
      {
        method: 'mode.changed',
        params: { mode: 'auto_accept', client_request_id: olderRequestId },
      },
      '1:2',
    );
    expect(ctx.service.permissionMode()).toBe('auto_accept');
    expect(ctx.service.error()).toBe('chat.control.admissionFailed');

    ctx.service.setMode('supervised');
    const newerRequestId = ctx.mockHttp.post.mock.calls[2][1].client_request_id;
    fireSseMessage(
      ctx.sseInstances[0],
      {
        method: 'mode.changed',
        params: { mode: 'supervised', client_request_id: newerRequestId },
      },
      '1:3',
    );
    expect(ctx.service.permissionMode()).toBe('supervised');
    expect(ctx.service.error()).toBeNull();
  });

  it('does not clear a mode failure for an unrelated narration acknowledgement', async () => {
    const ctx = await readySession();
    ctx.mockHttp.post.mockClear();
    ctx.mockHttp.post
      .mockReturnValueOnce(throwError(() => ({ status: 409 })))
      .mockReturnValueOnce(of({ accepted: true }));

    ctx.service.setMode('autonomous');
    expect(ctx.service.error()).toBe('chat.control.admissionFailed');
    ctx.service.setNarrationMode('silent');
    const narrationId = ctx.mockHttp.post.mock.calls[1][1].client_request_id;
    fireSseMessage(
      ctx.sseInstances[0],
      {
        method: 'narration.changed',
        params: { mode: 'silent', client_request_id: narrationId },
      },
      '1:2',
    );

    expect(ctx.service.narrationMode()).toBe('silent');
    expect(ctx.service.error()).toBe('chat.control.admissionFailed');
  });

  it('updateConfig forwards the config object over the WS with a request_id and settles on the ack', async () => {
    const ctx = await readySession();
    const outcome = ctx.service.updateConfig({ model: 'claude-sonnet-4-6', temperature: 0.3 });
    const sent = ctx.wsInstances[0].send.mock.calls.map((c: any) => JSON.parse(c[0]));
    const frame = sent.find((f: any) => f.method === 'config.update');
    expect(frame.config).toEqual({ model: 'claude-sonnet-4-6', temperature: 0.3 });
    // The id is what config.changed / error frames echo back — the promise
    // settles on that echo, not at send time (P0.3, and the Replicache
    // speculative-then-authoritative shape the pane relies on).
    expect(typeof frame.request_id).toBe('string');
    expect(frame.request_id.length).toBeGreaterThan(0);
    fireSseMessage(
      ctx.sseInstances[0],
      {
        method: 'config.changed',
        params: {
          model: 'claude-sonnet-4-6',
          temperature: 0.3,
          applied: { llm: { model: 'claude-sonnet-4-6' } },
          request_id: frame.request_id,
        },
      },
      '1:7',
    );
    await expect(outcome).resolves.toMatchObject({
      ok: true,
      requestId: frame.request_id,
      transport: 'websocket',
      effective: 'now',
      applied: { llm: { model: 'claude-sonnet-4-6' } },
    });
  });

  it('a config.update the agent refuses settles its caller as not applied', async () => {
    const ctx = await readySession();
    const outcome = ctx.service.updateConfig({ llm: { model: 'forbidden' } });
    const frame = ctx.wsInstances[0].send.mock.calls
      .map((c: any) => JSON.parse(c[0]))
      .find((f: any) => f.method === 'config.update');
    ctx.wsInstances[0].onmessage({
      data: JSON.stringify({
        method: 'error',
        params: {
          message: 'Session config update rejected',
          detail: 'grant denied',
          request_id: frame.request_id,
        },
      }),
    } as MessageEvent);
    await expect(outcome).resolves.toMatchObject({ ok: false, transport: 'websocket' });
    expect(ctx.service.error()).toContain('grant denied');
  });
});

describe('PersistentChatService — control WS frame filtering', () => {
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

  async function readySession() {
    const ctx = createService();
    ctx.mockHttp.get.mockImplementation(activeSessionGet);
    await ctx.service.connect('thread-status');
    fireSseOpen(ctx.sseInstances[0]);
    // These tests isolate whether a direct WS frame flips readiness. The
    // valid REST snapshot + /connection normally set it during connect.
    ctx.service.sessionReady.set(false);
    return ctx;
  }

  function fireWsFrame(ws: any, frame: Record<string, unknown>): void {
    ws.onmessage?.({ data: JSON.stringify(frame) } as MessageEvent);
  }

  it('drops _seq-stamped frames on the control WS to avoid double-dispatch with SSE', async () => {
    const { service, wsInstances } = await readySession();
    const turnsBefore = service.turns().length;

    // Broadcast events carry params._seq = [epoch, seq] from the agent's
    // _broadcast() — SSE will redeliver them, so the WS copy is discarded.
    fireWsFrame(wsInstances[0], {
      method: 'turn.started',
      params: { turn_id: 99, _seq: [0, 12] },
    });
    expect(service.currentStreamingTurn()).toBeNull();

    fireWsFrame(wsInstances[0], {
      method: 'token',
      params: { content: 'should-be-dropped', _seq: [0, 13] },
    });
    expect(service.currentStreamingTurn()).toBeNull();

    fireWsFrame(wsInstances[0], { method: 'ready', params: { _seq: [0, 14] } });
    expect(service.sessionReady()).toBe(false);

    expect(service.turns().length).toBe(turnsBefore);
  });

  it('processes session.state from the control WS (WS-direct, no _seq)', async () => {
    // Regression: reconnect to an idle session whose cached SSE cursor sits
    // past the most recent `ready` event. The agent's session.state welcome
    // frame is the only thing that arrives over the WS, and it must flip
    // sessionReady so the UI clears the "Establishing connection" card.
    const { service, wsInstances } = await readySession();
    expect(service.sessionReady()).toBe(false);

    fireWsFrame(wsInstances[0], {
      method: 'session.state',
      params: {
        thread_id: 'thread-status',
        permission_mode: 'manual',
        narration_mode: 'verbose',
        turn_count: 1,
        model: 'claude-opus-4-7',
        temperature: 0.5,
        tasks: [
          {
            id: 'task_9',
            description: 'Hydrated by the pinned owner',
            status: 'pending',
            priority: 'medium',
            notes: '',
            created_at: '2026-08-10T10:00:00+00:00',
            completed_at: null,
          },
        ],
      },
    });

    expect(service.sessionReady()).toBe(true);
    expect(service.permissionMode()).toBe('manual');
    expect(service.modelName()).toBe('claude-opus-4-7');
    expect(service.tasks().map((task) => task.id)).toEqual(['task_9']);

    // Rolling-deploy/metadata-only welcomes omit tasks. Absence is not an
    // instruction to erase the newer authoritative list.
    fireWsFrame(wsInstances[0], {
      method: 'session.state',
      params: { thread_id: 'thread-status', turn_count: 1 },
    });
    expect(service.tasks().map((task) => task.id)).toEqual(['task_9']);

    // An explicit empty list is authoritative.
    fireWsFrame(wsInstances[0], {
      method: 'session.state',
      params: { thread_id: 'thread-status', tasks: [] },
    });
    expect(service.tasks()).toEqual([]);
  });

  it('clears a stale "Agent not ready" error once session.state arrives', async () => {
    // Regression: during the WS reconnect storm at session attach the agent
    // rejects each /ws/chat with an `error: Agent not ready` frame until
    // attach completes. The eventual session.state must wipe the stale
    // banner so the UI doesn't show a red error contradicting a healthy
    // session.
    const { service, wsInstances } = await readySession();

    fireWsFrame(wsInstances[0], {
      method: 'error',
      params: { message: 'Agent not ready' },
    });
    expect(service.error()).toBe('Agent not ready');

    fireWsFrame(wsInstances[0], {
      method: 'session.state',
      params: { thread_id: 'thread-status' },
    });
    expect(service.error()).toBeNull();
    expect(service.sessionReady()).toBe(true);
  });

  it('silently ignores malformed WS frames', async () => {
    const { service, wsInstances } = await readySession();
    wsInstances[0].onmessage?.({ data: 'not-json{' } as MessageEvent);
    expect(service.startupPhase()).toBeNull();
  });
});

describe('PersistentChatService — direct session WS (prepare + connection)', () => {
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

  /** Build a URL-aware GET mock that handles all the endpoints connect() hits. */
  function connectGetMock(
    opts: {
      connectionResponses?: any[]; // array of values/errors to return on successive /connection calls
      threadMeta?: Record<string, unknown>;
      messages?: any[];
      sessionState?: Record<string, unknown>;
    } = {},
  ) {
    const connectionResponses = opts.connectionResponses ?? [
      of({
        state: 'ready',
        control_socket: 'websocket',
        ws_url: 'wss://api.example.com/p/t/ws?t=jwt',
        token: 'jwt',
        expires_at: 0,
      }),
    ];
    let connectionCallIdx = 0;
    return (url: string) => {
      if (url.includes('/api/sessions/') && url.endsWith('/connection')) {
        const r = connectionResponses[Math.min(connectionCallIdx, connectionResponses.length - 1)];
        connectionCallIdx += 1;
        return r;
      }
      if (url.endsWith('/messages')) {
        return of({ messages: opts.messages ?? [], total: 0 });
      }
      if (url.endsWith('/state')) {
        const segments = url.split('/');
        const threadId = segments[segments.length - 2];
        return of(
          opts.sessionState ?? {
            thread_id: threadId,
            permission_mode: 'supervised',
            narration_mode: 'auto',
            turn_count: 0,
            turn_in_flight: false,
            message_count: 0,
            model: null,
            temperature: null,
            running_tool: null,
            pending_permissions: [],
            event_cursor: { epoch: 0, seq: 0 },
            replay_cursor: { epoch: 0, seq: 0 },
            snapshot_source: 'durable_journal',
          },
        );
      }
      return of(opts.threadMeta ?? { status: 'active', total_turns: 0 });
    };
  }

  async function flushMicrotasks(rounds = 8): Promise<void> {
    for (let index = 0; index < rounds; index++) await Promise.resolve();
  }

  it('warm reconnect (already bound): GETs /connection, opens WS at returned ws_url, skips /prepare', async () => {
    const ctx = createService();
    ctx.mockHttp.get.mockImplementation(
      connectGetMock({
        connectionResponses: [
          of({
            state: 'ready',
            control_socket: 'websocket',
            ws_url: 'wss://api.example.com/p/t1/ws?t=tok-warm',
            token: 'tok-warm',
            expires_at: 0,
          }),
        ],
      }),
    );

    await ctx.service.connect('t1');

    // GET /api/sessions/t1/connection was called.
    const getUrls = ctx.mockHttp.get.mock.calls.map((c: any) => c[0]);
    expect(getUrls.some((u: string) => u.endsWith('/api/sessions/t1/connection'))).toBe(true);

    // POST /api/sessions/t1/prepare was NOT called.
    const prepareCalls = ctx.mockHttp.post.mock.calls.filter((c: any) =>
      String(c[0]).endsWith('/api/sessions/t1/prepare'),
    );
    expect(prepareCalls).toHaveLength(0);

    // WebSocket was opened at the URL returned by /connection.
    expect(ctx.wsInstances).toHaveLength(1);
    expect(ctx.wsInstances[0].url).toBe('wss://api.example.com/p/t1/ws?t=tok-warm');
    expect(ctx.service.sessionReady()).toBe(true);
  });

  it('accepts an older orchestrator pinned response during a rolling deploy', async () => {
    const ctx = createService();
    ctx.mockHttp.get.mockImplementation(
      connectGetMock({
        connectionResponses: [
          of({
            state: 'ready',
            ws_url: 'wss://api.example.com/p/legacy/ws?t=old-token',
            token: 'old-token',
            expires_at: 0,
          }),
        ],
      }),
    );

    await ctx.service.connect('legacy-pinned');

    expect(ctx.wsInstances).toHaveLength(1);
    expect(ctx.wsInstances[0].url).toBe('wss://api.example.com/p/legacy/ws?t=old-token');
    expect((ctx.service as any).controlSocket).toBe('websocket');
  });

  it('treats a null control socket as ready without constructing or retrying a WebSocket', async () => {
    const ctx = createService();
    ctx.mockHttp.get.mockImplementation(
      connectGetMock({
        connectionResponses: [
          of({
            state: 'ready',
            control_socket: 'none',
            ws_url: null,
            token: null,
            expires_at: null,
          }),
        ],
        threadMeta: {
          status: 'active',
          total_turns: 3,
          config_name: 'session_base',
        },
        sessionState: {
          thread_id: 'socketless',
          permission_mode: 'supervised',
          narration_mode: 'verbose',
          turn_count: 3,
          turn_in_flight: false,
          message_count: 6,
          model: null,
          temperature: 0.2,
          running_tool: null,
          pending_permissions: [],
          event_cursor: { epoch: 2, seq: 19 },
          replay_cursor: { epoch: 2, seq: 18 },
          snapshot_source: 'durable_journal',
        },
      }),
    );

    await ctx.service.connect('socketless');
    fireSseOpen(ctx.sseInstances[0]);
    (ctx.service as any)._ensureControlWs();
    (ctx.service as any)._revalidateConnection();

    expect(ctx.service.sessionReady()).toBe(true);
    expect(ctx.service.permissionMode()).toBe('supervised');
    expect(ctx.service.narrationMode()).toBe('verbose');
    // An expert profile is not a model name; an unresolved model stays
    // unknown instead of rendering `session_base` as though it were one.
    expect(ctx.service.modelName()).toBeNull();
    expect(ctx.wsInstances).toHaveLength(0);
    expect((ctx.service as any).controlWsReconnectAttempt).toBe(0);
    expect((ctx.service as any).controlWsReconnectTimer).toBeNull();
    const connectionGets = ctx.mockHttp.get.mock.calls.filter((c: any) =>
      String(c[0]).endsWith('/api/sessions/socketless/connection'),
    );
    expect(connectionGets).toHaveLength(1);
  });

  it('hydrates stateless tasks that are older than the SSE replay floor', async () => {
    const ctx = createService();
    ctx.mockHttp.get.mockImplementation(
      connectGetMock({
        connectionResponses: [
          of({
            state: 'ready',
            control_socket: 'none',
            ws_url: null,
            token: null,
            expires_at: null,
          }),
        ],
        sessionState: {
          thread_id: 'socketless-tasks',
          permission_mode: 'supervised',
          narration_mode: 'auto',
          turn_count: 8,
          turn_in_flight: false,
          message_count: 16,
          model: null,
          temperature: null,
          running_tool: null,
          pending_permissions: [],
          tasks: [
            {
              id: 'task_3',
              description: 'Survive the pod handoff',
              status: 'in_progress',
              priority: 'high',
              notes: 'created before the replay window',
              created_at: '2026-08-10T08:30:00+00:00',
              completed_at: null,
            },
          ],
          event_cursor: { epoch: 5, seq: 80 },
          replay_cursor: { epoch: 5, seq: 72 },
          snapshot_source: 'durable_journal',
        },
      }),
    );

    await ctx.service.connect('socketless-tasks');

    expect(ctx.service.tasks()).toEqual([
      {
        id: 'task_3',
        description: 'Survive the pod handoff',
        status: 'in_progress',
        priority: 'high',
        notes: 'created before the replay window',
        created_at: '2026-08-10T08:30:00+00:00',
        completed_at: null,
      },
    ]);
    expect(ctx.sseInstances[0].url).toContain('last_event_id=5%3A72');
    expect(ctx.wsInstances).toHaveLength(0);
  });

  it('keeps snapshot tasks across covered replay and applies the live suffix', async () => {
    const ctx = createService();
    const task = (status: 'pending' | 'in_progress' | 'completed', notes: string) => ({
      id: 'task_5',
      description: 'Fence task replay',
      status,
      priority: 'high',
      notes,
      created_at: '2026-08-10T08:30:00+00:00',
      completed_at: status === 'completed' ? '2026-08-10T09:00:00+00:00' : null,
    });
    ctx.mockHttp.get.mockImplementation(
      connectGetMock({
        connectionResponses: [
          of({
            state: 'ready',
            control_socket: 'none',
            ws_url: null,
            token: null,
            expires_at: null,
          }),
        ],
        sessionState: {
          thread_id: 'socketless-task-fence',
          permission_mode: 'supervised',
          narration_mode: 'auto',
          turn_count: 10,
          turn_in_flight: false,
          message_count: 20,
          model: null,
          temperature: null,
          running_tool: null,
          pending_permissions: [],
          tasks: [task('completed', 'snapshot at seq 100')],
          event_cursor: { epoch: 6, seq: 100 },
          replay_cursor: { epoch: 6, seq: 90 },
          snapshot_source: 'durable_journal',
        },
      }),
    );

    await ctx.service.connect('socketless-task-fence');
    expect(ctx.service.tasks()).toEqual([task('completed', 'snapshot at seq 100')]);

    fireSseMessage(
      ctx.sseInstances[0],
      {
        method: 'tasks.updated',
        params: { tasks: [task('pending', 'stale replay at seq 95')] },
      },
      '6:95',
    );
    expect(ctx.service.tasks()).toEqual([task('completed', 'snapshot at seq 100')]);

    fireSseMessage(
      ctx.sseInstances[0],
      {
        method: 'tasks.updated',
        params: { tasks: [task('in_progress', 'live update at seq 101')] },
      },
      '6:101',
    );
    expect(ctx.service.tasks()).toEqual([task('in_progress', 'live update at seq 101')]);
  });

  it('keeps a socketless session unready when its REST state cannot be loaded', async () => {
    const ctx = createService();
    const get = connectGetMock({
      connectionResponses: [
        of({
          state: 'ready',
          control_socket: 'none',
          ws_url: null,
          token: null,
          expires_at: null,
        }),
      ],
    });
    ctx.mockHttp.get.mockImplementation((url: string) =>
      url.endsWith('/state') ? throwError(() => ({ status: 503 })) : get(url),
    );

    await ctx.service.connect('state-unavailable');
    fireSseOpen(ctx.sseInstances[0]);
    (ctx.service as any)._ensureControlWs();
    (ctx.service as any)._revalidateConnection(true);
    fireSseOpen(ctx.sseInstances[ctx.sseInstances.length - 1]);

    expect(ctx.service.sessionReady()).toBe(false);
    expect(ctx.service.error()).toBe('Session state unavailable');
    expect(ctx.wsInstances).toHaveLength(0);
    expect((ctx.service as any).controlWsReconnectTimer).toBeNull();
  });

  it('retries a failed socketless snapshot on explicit reconnect', async () => {
    const ctx = createService();
    const get = connectGetMock({
      connectionResponses: [
        of({
          state: 'ready',
          control_socket: 'none',
          ws_url: null,
          token: null,
          expires_at: null,
        }),
      ],
    });
    let stateReads = 0;
    ctx.mockHttp.get.mockImplementation((url: string) => {
      if (url.endsWith('/state') && stateReads++ === 0) {
        return throwError(() => ({ status: 503 }));
      }
      return get(url);
    });

    await ctx.service.connect('state-recovers');
    expect(ctx.service.sessionReady()).toBe(false);
    expect(ctx.service.error()).toBe('Session state unavailable');

    ctx.service.reconnectNow();
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(stateReads).toBe(2);
    expect(ctx.sseInstances).toHaveLength(2);
    expect(ctx.service.sessionReady()).toBe(true);
    expect(ctx.service.error()).toBeNull();
    expect(ctx.wsInstances).toHaveLength(0);
  });

  it('re-gates an already-ready socketless session when snapshot refresh fails', async () => {
    const ctx = createService();
    const get = connectGetMock({
      connectionResponses: [
        of({
          state: 'ready',
          control_socket: 'none',
          ws_url: null,
          token: null,
          expires_at: null,
        }),
      ],
    });
    ctx.mockHttp.get.mockImplementation(get);
    await ctx.service.connect('state-refresh-fails');
    expect(ctx.service.sessionReady()).toBe(true);

    ctx.mockHttp.get.mockImplementation((url: string) =>
      url.endsWith('/state') ? throwError(() => ({ status: 503 })) : get(url),
    );
    await (ctx.service as any)._loadSessionState(
      'state-refresh-fails',
      (ctx.service as any).connectGeneration,
    );

    expect(ctx.service.sessionReady()).toBe(false);
    expect(ctx.service.error()).toBe('Session state unavailable');
    expect((ctx.service as any).controlSocket).toBe('none');
  });

  it.each([
    {
      label: 'declared websocket',
      connection: {
        state: 'ready',
        control_socket: 'websocket',
        ws_url: null,
        token: null,
        expires_at: null,
      },
    },
    {
      label: 'legacy response without a discriminator',
      connection: {
        state: 'ready',
        ws_url: null,
        token: null,
        expires_at: null,
      },
    },
  ])(
    'normalizes a null ws_url from $label to stable socketless transport',
    async ({ connection }) => {
      const ctx = createService();
      ctx.mockHttp.get.mockImplementation(
        connectGetMock({
          connectionResponses: [of(connection)],
        }),
      );

      await ctx.service.connect('null-ws-version-skew');
      (ctx.service as any)._ensureControlWs();
      (ctx.service as any)._revalidateConnection();

      expect(ctx.service.sessionReady()).toBe(true);
      expect(ctx.wsInstances).toHaveLength(0);
      expect((ctx.service as any).controlSocket).toBe('none');
      expect((ctx.service as any).controlWsReconnectTimer).toBeNull();
    },
  );

  it('does not require IndexedDB when the durable snapshot is available', async () => {
    const ctx = createService();
    ctx.mockCache.getThreadCursor.mockRejectedValue(new Error('IDB unavailable'));
    ctx.mockHttp.get.mockImplementation(
      connectGetMock({
        connectionResponses: [
          of({
            state: 'ready',
            control_socket: 'none',
            ws_url: null,
            token: null,
            expires_at: null,
          }),
        ],
      }),
    );

    await ctx.service.connect('no-idb');

    expect(ctx.service.sessionReady()).toBe(true);
    expect(ctx.sseInstances).toHaveLength(1);
    expect(ctx.sseInstances[0].url).toContain('last_event_id=0%3A0');
    expect(ctx.wsInstances).toHaveLength(0);
  });

  it('hydrates a socketless pending gate and running tool from REST state', async () => {
    const ctx = createService({ cursor: { epoch: 5, seq: 80 } });
    ctx.mockHttp.get.mockImplementation(
      connectGetMock({
        connectionResponses: [
          of({
            state: 'ready',
            control_socket: 'none',
            ws_url: null,
            token: null,
            expires_at: null,
          }),
        ],
        sessionState: {
          thread_id: 'stateful',
          permission_mode: 'supervised',
          narration_mode: 'auto',
          turn_count: 8,
          turn_in_flight: true,
          message_count: 17,
          model: 'claude-sonnet-4-6',
          temperature: 0.4,
          running_tool: { id: 'tool-live', tool: 'run_command', args: { cmd: 'make' } },
          pending_permissions: [
            {
              id: 'tool-gated',
              approval_id: 'approval-gated',
              tool: 'write_file',
              args: { path: 'README.md' },
            },
          ],
          event_cursor: { epoch: 5, seq: 82 },
          replay_cursor: { epoch: 5, seq: 78 },
          snapshot_source: 'durable_journal',
        },
      }),
    );

    await ctx.service.connect('stateful');

    // Replay begins at the server's tab-local floor, not at the shared
    // IndexedDB cursor. These covered frames reconstruct transcript order
    // without replacing the snapshot's authoritative pending list.
    ctx.mockCache.getThreadCursor.mockResolvedValue({ epoch: 5, seq: 999 });
    fireSseMessage(ctx.sseInstances[0], { method: 'ready', params: {} }, '5:79');
    fireSseMessage(ctx.sseInstances[0], { method: 'turn.started', params: { turn_id: 8 } }, '5:80');
    fireSseMessage(
      ctx.sseInstances[0],
      {
        method: 'permission.request',
        params: {
          id: 'tool-gated',
          approval_id: 'approval-gated',
          tool: 'write_file',
          args: { path: 'README.md' },
        },
      },
      '5:81',
    );

    expect(ctx.service.runningTool()).toEqual({
      id: 'tool-live',
      tool: 'run_command',
      args: { cmd: 'make' },
    });
    expect(ctx.service.pendingPermissions()).toEqual([
      {
        id: 'tool-gated',
        approvalId: 'approval-gated',
        tool: 'write_file',
        args: { path: 'README.md' },
      },
    ]);
    expect(ctx.service.isStreaming()).toBe(true);
    expect(ctx.wsInstances).toHaveLength(0);
    expect(ctx.sseInstances[0].url).toContain('last_event_id=5%3A78');
    expect(ctx.mockCache.getThreadCursor).not.toHaveBeenCalled();
    const liveTurns = ctx.service.turns().filter(isAssistantTurn) as AssistantTurn[];
    expect(liveTurns).toHaveLength(1);
    expect(liveTurns[0].id).toBe('8');
    expect(liveTurns[0].events).toEqual([
      expect.objectContaining({ kind: 'tool_call', id: 'tool-gated', status: 'pending' }),
    ]);

    // A second tab has advanced the shared cursor to 999, but this tab has
    // only folded through 81. Focus recovery must resume from 81 so the
    // resolution at 83 cannot be skipped.
    (ctx.service as any)._revalidateConnection(true);
    expect(ctx.sseInstances[1].url).toContain('last_event_id=5%3A81');
    fireSseMessage(
      ctx.sseInstances[1],
      {
        method: 'permission.resolved',
        params: { id: 'tool-gated', decision: 'approved' },
      },
      '5:83',
    );
    expect(ctx.service.pendingPermissions()).toEqual([]);
  });

  it('restores a socketless approval card when its durable REST decision fails', async () => {
    const ctx = createService();
    ctx.mockHttp.get.mockImplementation(
      connectGetMock({
        connectionResponses: [
          of({
            state: 'ready',
            control_socket: 'none',
            ws_url: null,
            token: null,
            expires_at: null,
          }),
        ],
        sessionState: {
          thread_id: 'approval-retry',
          permission_mode: 'supervised',
          narration_mode: 'auto',
          turn_count: 3,
          turn_in_flight: true,
          message_count: 1,
          model: 'gpt-5.4',
          temperature: 0.2,
          running_tool: null,
          pending_permissions: [
            {
              id: 'tool-retry',
              approval_id: 'approval-retry-id',
              tool: 'run_command',
              args: { cmd: 'make test' },
            },
          ],
          event_cursor: { epoch: 3, seq: 2 },
          replay_cursor: { epoch: 3, seq: 0 },
          snapshot_source: 'durable_journal',
        },
      }),
    );
    ctx.mockHttp.post.mockImplementation((url: string) =>
      url.includes('/approve/') ? throwError(() => ({ status: 503 })) : of({}),
    );

    await ctx.service.connect('approval-retry');
    fireSseMessage(ctx.sseInstances[0], { method: 'turn.started', params: { turn_id: 3 } }, '3:1');
    fireSseMessage(
      ctx.sseInstances[0],
      {
        method: 'permission.request',
        params: {
          id: 'tool-retry',
          approval_id: 'approval-retry-id',
          tool: 'run_command',
          args: { cmd: 'make test' },
        },
      },
      '3:2',
    );

    ctx.service.approveAll();

    expect(ctx.service.pendingPermissions()).toEqual([
      {
        id: 'tool-retry',
        approvalId: 'approval-retry-id',
        tool: 'run_command',
        args: { cmd: 'make test' },
      },
    ]);
    expect((ctx.service as any).controlOutbox).toEqual([]);
    expect(ctx.wsInstances).toHaveLength(0);
    expect(ctx.service.error()).toContain('still pending');
    const assistant = ctx.service.turns().find(isAssistantTurn) as AssistantTurn;
    const toolEvent = assistant.events.find((event) => event.id === 'tool-retry');
    expect(toolEvent).toEqual(
      expect.objectContaining({
        kind: 'tool_call',
        id: 'tool-retry',
        status: 'pending',
      }),
    );
    expect((toolEvent as ToolCallEvent).decision).toBeUndefined();

    // A masked commit or another tab can resolve after the error. That
    // proof clears both the card and the now-stale retry banner.
    fireSseMessage(
      ctx.sseInstances[0],
      {
        method: 'permission.resolved',
        params: { id: 'tool-retry', decision: 'approved' },
      },
      '3:3',
    );
    expect(ctx.service.pendingPermissions()).toEqual([]);
    expect(ctx.service.error()).toBeNull();
  });

  it('applies REST state before cursor replay so a mid-turn prefix stays one bubble', async () => {
    const ctx = createService({ cursor: { epoch: 2, seq: 40 } });
    ctx.mockHttp.get.mockImplementation(
      connectGetMock({
        messages: [
          {
            id: 'u7',
            role: 'human',
            content: 'Revise the draft',
            tool_calls: null,
            turn_number: 7,
            created_at: '2026-08-05T14:00:00Z',
          },
          {
            id: 'a7-prefix',
            role: 'ai',
            content: 'I will apply those revisions now.',
            tool_calls: null,
            turn_number: 7,
            created_at: '2026-08-05T14:00:01Z',
          },
        ],
        connectionResponses: [
          of({
            state: 'ready',
            control_socket: 'none',
            ws_url: null,
            token: null,
            expires_at: null,
          }),
        ],
        sessionState: {
          thread_id: 'midturn-rest',
          permission_mode: 'supervised',
          narration_mode: 'auto',
          turn_count: 7,
          turn_in_flight: true,
          message_count: 2,
          model: 'gpt-5.4',
          temperature: 0.2,
          running_tool: null,
          pending_permissions: [],
          event_cursor: { epoch: 2, seq: 40 },
          replay_cursor: { epoch: 2, seq: 38 },
          snapshot_source: 'durable_journal',
        },
      }),
    );

    await ctx.service.connect('midturn-rest');
    fireSseMessage(ctx.sseInstances[0], { method: 'turn.started', params: { turn_id: 7 } }, '2:39');
    fireSseMessage(
      ctx.sseInstances[0],
      { method: 'token', params: { content: 'I will apply those revisions now.' } },
      '2:40',
    );
    fireSseMessage(
      ctx.sseInstances[0],
      { method: 'token', params: { content: 'The matrix is updated.' } },
      '2:41',
    );
    (ctx.service as any)._flushDeltas();

    const assistants = ctx.service.turns().filter(isAssistantTurn) as AssistantTurn[];
    expect(assistants).toHaveLength(1);
    expect(assistants[0].id).toBe('7');
    expect(assistants[0].status).toBe('streaming');
    // Adjacent token frames are coalesced, but the persisted prefix is
    // rebuilt exactly once before the live suffix.
    expect(assistants[0].events.map((event) => (event as TextEvent).content)).toEqual([
      'I will apply those revisions now.The matrix is updated.',
    ]);
    expect(ctx.service.turnCount()).toBe(7);
  });

  it('resumes a same-thread reconnect from this tab cursor without replaying its prefix', async () => {
    const ctx = createService();
    ctx.mockHttp.get.mockImplementation((url: string) => {
      if (url.endsWith('/messages')) return of({ messages: [], total: 0 });
      if (url.endsWith('/state')) {
        return of({
          thread_id: 'same-thread-live',
          permission_mode: 'supervised',
          narration_mode: 'auto',
          turn_count: 7,
          turn_in_flight: true,
          message_count: 1,
          model: 'gpt-5.4',
          temperature: 0.2,
          running_tool: null,
          pending_permissions: [],
          event_cursor: { epoch: 2, seq: 40 },
          replay_cursor: { epoch: 2, seq: 38 },
          snapshot_source: 'durable_journal',
        });
      }
      if (url.endsWith('/connection')) {
        return of({
          state: 'ready',
          control_socket: 'none',
          ws_url: null,
          token: null,
          expires_at: null,
        });
      }
      if (url.endsWith('/citations')) return of({ citations: [] });
      return of({ status: 'active', total_turns: 7 });
    });

    await ctx.service.connect('same-thread-live');
    fireSseMessage(ctx.sseInstances[0], { method: 'turn.started', params: { turn_id: 7 } }, '2:39');
    fireSseMessage(ctx.sseInstances[0], { method: 'token', params: { content: 'prefix' } }, '2:40');
    (ctx.service as any)._flushDeltas();

    await ctx.service.connect('same-thread-live');

    expect(ctx.sseInstances[1].url).toContain('last_event_id=2%3A40');
    fireSseMessage(
      ctx.sseInstances[1],
      { method: 'token', params: { content: '-suffix' } },
      '2:41',
    );
    (ctx.service as any)._flushDeltas();

    const assistants = ctx.service.turns().filter(isAssistantTurn) as AssistantTurn[];
    expect(assistants).toHaveLength(1);
    expect(assistants[0].status).toBe('streaming');
    expect(assistants[0].events.map((event) => (event as TextEvent).content).join('')).toBe(
      'prefix-suffix',
    );
    expect(ctx.mockCache.getThreadCursor).not.toHaveBeenCalled();
  });

  it('cold-repaints retained same-thread state when the journal epoch changed', async () => {
    const ctx = createService();
    let stateRead = 0;
    ctx.mockHttp.get.mockImplementation((url: string) => {
      if (url.includes('/messages')) return of({ messages: [], total: 0 });
      if (url.endsWith('/state')) {
        stateRead += 1;
        const epoch = stateRead === 1 ? 1 : 2;
        return of({
          thread_id: 'same-thread-new-epoch',
          permission_mode: 'supervised',
          narration_mode: 'auto',
          turn_count: 1,
          turn_in_flight: true,
          message_count: 1,
          model: 'gpt-5.4',
          temperature: 0.2,
          running_tool: null,
          pending_permissions: [],
          event_cursor: { epoch, seq: 2 },
          replay_cursor: { epoch, seq: 0 },
          snapshot_source: 'durable_journal',
        });
      }
      if (url.endsWith('/connection')) {
        return of({
          state: 'ready',
          control_socket: 'none',
          ws_url: null,
          token: null,
          expires_at: null,
        });
      }
      if (url.endsWith('/citations')) return of({ citations: [] });
      return of({ status: 'active', total_turns: 1 });
    });

    await ctx.service.connect('same-thread-new-epoch');
    fireSseMessage(ctx.sseInstances[0], { method: 'turn.started', params: { turn_id: 1 } }, '1:1');
    fireSseMessage(
      ctx.sseInstances[0],
      { method: 'token', params: { content: 'old epoch' } },
      '1:2',
    );
    (ctx.service as any)._flushDeltas();

    await ctx.service.connect('same-thread-new-epoch');

    expect(ctx.mockCache.clearThreadMessages).toHaveBeenCalledWith('same-thread-new-epoch');
    expect(ctx.mockCache.deleteThreadCursor).toHaveBeenCalledWith('same-thread-new-epoch');
    expect(ctx.sseInstances[1].url).toContain('last_event_id=2%3A0');
    fireSseMessage(ctx.sseInstances[1], { method: 'turn.started', params: { turn_id: 1 } }, '2:1');
    fireSseMessage(
      ctx.sseInstances[1],
      { method: 'token', params: { content: 'new epoch' } },
      '2:2',
    );
    (ctx.service as any)._flushDeltas();

    const assistants = ctx.service.turns().filter(isAssistantTurn) as AssistantTurn[];
    expect(assistants).toHaveLength(1);
    expect(assistants[0].events.map((event) => (event as TextEvent).content).join('')).toBe(
      'new epoch',
    );
  });

  it('cold-repaints when a same-epoch retained cursor fell behind the replay floor', async () => {
    const ctx = createService();
    let stateRead = 0;
    let messageRead = 0;
    ctx.mockHttp.get.mockImplementation((url: string) => {
      if (url.includes('/messages')) {
        messageRead += 1;
        if (messageRead === 1) return of({ messages: [], total: 0 });
        return of({
          messages: [
            {
              id: 'a9-history',
              role: 'ai',
              content: 'completed in another tab',
              tool_calls: null,
              turn_number: 9,
              created_at: '2026-08-08T20:00:00Z',
            },
          ],
          total: 1,
        });
      }
      if (url.endsWith('/state')) {
        stateRead += 1;
        const advanced = stateRead > 1;
        return of({
          thread_id: 'same-epoch-gap',
          permission_mode: 'supervised',
          narration_mode: 'auto',
          turn_count: advanced ? 10 : 1,
          turn_in_flight: true,
          message_count: advanced ? 10 : 1,
          model: 'gpt-5.4',
          temperature: 0.2,
          running_tool: null,
          pending_permissions: [],
          event_cursor: { epoch: 2, seq: advanced ? 102 : 2 },
          replay_cursor: { epoch: 2, seq: advanced ? 99 : 0 },
          snapshot_source: 'durable_journal',
        });
      }
      if (url.endsWith('/connection')) {
        return of({
          state: 'ready',
          control_socket: 'none',
          ws_url: null,
          token: null,
          expires_at: null,
        });
      }
      if (url.endsWith('/citations')) return of({ citations: [] });
      return of({ status: 'active', total_turns: stateRead > 1 ? 10 : 1 });
    });

    await ctx.service.connect('same-epoch-gap');
    fireSseMessage(ctx.sseInstances[0], { method: 'turn.started', params: { turn_id: 1 } }, '2:1');
    fireSseMessage(
      ctx.sseInstances[0],
      { method: 'token', params: { content: 'old local turn' } },
      '2:2',
    );
    (ctx.service as any)._flushDeltas();

    await ctx.service.connect('same-epoch-gap');

    expect(ctx.mockCache.clearThreadMessages).toHaveBeenCalledWith('same-epoch-gap');
    expect(ctx.sseInstances[1].url).toContain('last_event_id=2%3A99');
    expect(
      (ctx.service.turns().find(isAssistantTurn) as AssistantTurn).events
        .map((event) => (event as TextEvent).content)
        .join(''),
    ).toBe('completed in another tab');

    fireSseMessage(
      ctx.sseInstances[1],
      { method: 'turn.started', params: { turn_id: 10 } },
      '2:100',
    );
    fireSseMessage(
      ctx.sseInstances[1],
      { method: 'token', params: { content: 'latest live turn' } },
      '2:101',
    );
    (ctx.service as any)._flushDeltas();

    const assistants = ctx.service.turns().filter(isAssistantTurn) as AssistantTurn[];
    expect(assistants).toHaveLength(2);
    expect(
      assistants.map((turn) => turn.events.map((event) => (event as TextEvent).content).join('')),
    ).toEqual(['completed in another tab', 'latest live turn']);
  });

  it.each([
    { method: 'ready', params: {} },
    { method: 'turn.error', params: { message: 'covered failure', turn_id: 7 } },
  ])('uses a covered $method frame as a transcript terminal boundary', async (terminal) => {
    const ctx = createService();
    ctx.mockHttp.get.mockImplementation(
      connectGetMock({
        connectionResponses: [
          of({
            state: 'ready',
            control_socket: 'none',
            ws_url: null,
            token: null,
            expires_at: null,
          }),
        ],
        sessionState: {
          thread_id: 'covered-terminal',
          permission_mode: 'supervised',
          narration_mode: 'auto',
          turn_count: 7,
          turn_in_flight: false,
          message_count: 1,
          model: null,
          temperature: null,
          running_tool: null,
          pending_permissions: [],
          event_cursor: { epoch: 2, seq: 42 },
          replay_cursor: { epoch: 2, seq: 38 },
          snapshot_source: 'durable_journal',
        },
      }),
    );

    await ctx.service.connect('covered-terminal');
    fireSseMessage(ctx.sseInstances[0], { method: 'turn.started', params: { turn_id: 7 } }, '2:39');
    fireSseMessage(
      ctx.sseInstances[0],
      { method: 'token', params: { content: 'partial' } },
      '2:40',
    );
    fireSseMessage(ctx.sseInstances[0], terminal, '2:41');
    (ctx.service as any)._flushDeltas();

    expect(ctx.service.isStreaming()).toBe(false);
    const assistant = ctx.service.turns().find(isAssistantTurn) as AssistantTurn;
    expect(assistant.status).not.toBe('streaming');
  });

  it('cold start (425 → prepare → poll /connection until ready): WS opens at final ws_url', async () => {
    const ctx = createService();

    // /connection: 425, then 425 again (still booting), then 200.
    ctx.mockHttp.get.mockImplementation(
      connectGetMock({
        connectionResponses: [
          throwError(() => ({ status: 425 })),
          throwError(() => ({ status: 425 })),
          of({
            state: 'ready',
            control_socket: 'websocket',
            ws_url: 'wss://api.example.com/p/t2/ws?t=tok-cold',
            token: 'tok-cold',
            expires_at: 0,
          }),
        ],
      }),
    );

    ctx.mockHttp.post.mockImplementation((url: string) => {
      if (url.endsWith('/api/sessions/t2/prepare')) {
        return of({ state: 'provisioning' });
      }
      return of({});
    });

    await ctx.service.connect('t2');

    // POST /prepare was called exactly once.
    const prepareCalls = ctx.mockHttp.post.mock.calls.filter((c: any) =>
      String(c[0]).endsWith('/api/sessions/t2/prepare'),
    );
    expect(prepareCalls).toHaveLength(1);

    // GET /connection was called until it succeeded.
    const connCalls = ctx.mockHttp.get.mock.calls.filter((c: any) =>
      String(c[0]).endsWith('/api/sessions/t2/connection'),
    );
    expect(connCalls.length).toBeGreaterThanOrEqual(2);

    // WS opened at the URL returned by the successful /connection.
    const sessionWs = ctx.wsInstances.find((ws) =>
      String(ws.url || '').includes('wss://api.example.com/p/t2/ws'),
    );
    expect(sessionWs).toBeDefined();
    expect(sessionWs.url).toBe('wss://api.example.com/p/t2/ws?t=tok-cold');
    expect(ctx.service.sessionReady()).toBe(true);

    // No transient SSE on /notifications/events is opened — phase
    // signals come from the always-on NotificationService feed (owned
    // by the app shell), not from a per-connect listener.
    const lifecycleEs = ctx.sseInstances.find((es) => es.url.includes('/notifications/events'));
    expect(lifecycleEs).toBeUndefined();
  });

  it('a terminal SSE cancels a delayed 425 before it can POST prepare or restart control', async () => {
    vi.useFakeTimers();
    try {
      const ctx = createService();
      const connection = new Subject<any>();
      ctx.mockHttp.get.mockImplementation((url: string) => {
        if (url.endsWith('/connection')) return connection;
        if (url.endsWith('/messages')) return of({ messages: [], total: 0 });
        if (url.endsWith('/state')) {
          return of({
            thread_id: 'terminal-425',
            permission_mode: 'supervised',
            narration_mode: 'auto',
            turn_count: 0,
            turn_in_flight: false,
            message_count: 0,
            pending_permissions: [],
            event_cursor: { epoch: 4, seq: 0 },
            replay_cursor: { epoch: 4, seq: 0 },
            snapshot_source: 'durable_journal',
          });
        }
        if (url.endsWith('/citations')) return of({ citations: [] });
        return of({ status: 'active', total_turns: 0 });
      });

      const connecting = ctx.service.connect('terminal-425');
      await flushMicrotasks();
      expect(ctx.sseInstances).toHaveLength(1);
      fireSseMessage(ctx.sseInstances[0], { method: 'session.ended', params: {} }, '4:1');
      expect(ctx.sseInstances[0].close).not.toHaveBeenCalled();

      connection.error({ status: 425 });
      await connecting;
      await vi.advanceTimersByTimeAsync(610_000);
      window.dispatchEvent(new Event('focus'));
      window.dispatchEvent(new Event('online'));
      await flushMicrotasks();

      const connectionCalls = ctx.mockHttp.get.mock.calls.filter((call: any[]) =>
        String(call[0]).endsWith('/api/sessions/terminal-425/connection'),
      );
      const prepareCalls = ctx.mockHttp.post.mock.calls.filter((call: any[]) =>
        String(call[0]).endsWith('/api/sessions/terminal-425/prepare'),
      );
      expect(connectionCalls).toHaveLength(1);
      expect(prepareCalls).toHaveLength(0);
      expect(ctx.wsInstances).toHaveLength(0);
      expect(ctx.service.sessionReady()).toBe(false);
    } finally {
      vi.useRealTimers();
    }
  });

  it('a terminal SSE cancels an unresolved prepare response and every continuation', async () => {
    vi.useFakeTimers();
    try {
      const ctx = createService();
      const prepare = new Subject<any>();
      ctx.mockHttp.get.mockImplementation(
        connectGetMock({ connectionResponses: [throwError(() => ({ status: 425 }))] }),
      );
      ctx.mockHttp.post.mockImplementation((url: string) =>
        url.endsWith('/prepare') ? prepare : of({}),
      );

      const connecting = ctx.service.connect('terminal-during-prepare');
      await flushMicrotasks(20);
      expect(
        ctx.mockHttp.post.mock.calls.filter((call: any[]) =>
          String(call[0]).endsWith('/api/sessions/terminal-during-prepare/prepare'),
        ),
      ).toHaveLength(1);

      fireSseMessage(ctx.sseInstances[0], { method: 'session.ended', params: {} }, '5:1');
      expect(ctx.sseInstances[0].close).not.toHaveBeenCalled();
      prepare.next({ state: 'provisioning' });
      prepare.complete();
      await connecting;
      await vi.advanceTimersByTimeAsync(1_100_000);
      window.dispatchEvent(new Event('focus'));
      window.dispatchEvent(new Event('online'));
      await flushMicrotasks();

      expect(
        ctx.mockHttp.get.mock.calls.filter((call: any[]) =>
          String(call[0]).endsWith('/api/sessions/terminal-during-prepare/connection'),
        ),
      ).toHaveLength(1);
      expect(ctx.wsInstances).toHaveLength(0);
      expect(ctx.service.sessionReady()).toBe(false);
    } finally {
      vi.useRealTimers();
    }
  });

  it('keeps terminal review SSE liveness separate from agent Connected state', async () => {
    const ctx = createService();
    ctx.mockHttp.get.mockImplementation(connectGetMock());
    await ctx.service.connect('terminal-sse-only');
    const firstSse = ctx.sseInstances[0];
    const controlCount = ctx.wsInstances.length;

    fireSseMessage(firstSse, { method: 'session.ended', params: {} }, '5:1');
    fireSseOpen(firstSse);
    expect(ctx.service.connectionState()).toBe('disconnected');
    expect(ctx.service.isConnected()).toBe(false);
    expect(firstSse.close).not.toHaveBeenCalled();
    expect(ctx.wsInstances).toHaveLength(controlCount);

    // A watchdog/focus-style journal reconnect stays SSE-only too. It may
    // reconcile metadata/review but can never advertise or reopen control.
    ctx.service.reconnectNow();
    await flushMicrotasks();
    const reopened = ctx.sseInstances.at(-1)!;
    expect(reopened).not.toBe(firstSse);
    fireSseOpen(reopened);
    await flushMicrotasks();
    expect(ctx.service.connectionState()).toBe('disconnected');
    expect(ctx.service.isConnected()).toBe(false);
    expect(reopened.close).not.toHaveBeenCalled();
    expect(ctx.wsInstances).toHaveLength(controlCount);
    expect(
      ctx.mockHttp.get.mock.calls.filter((call: any[]) =>
        String(call[0]).endsWith('/api/sessions/terminal-sse-only/connection'),
      ),
    ).toHaveLength(1);
  });

  it('a terminal SSE drops a late ready poll response without opening control', async () => {
    vi.useFakeTimers();
    try {
      const ctx = createService();
      const poll = new Subject<any>();
      ctx.mockHttp.get.mockImplementation(
        connectGetMock({
          connectionResponses: [throwError(() => ({ status: 425 })), poll],
        }),
      );
      ctx.mockHttp.post.mockReturnValue(of({ state: 'provisioning' }));

      const connecting = ctx.service.connect('terminal-during-poll');
      await flushMicrotasks(20);
      expect(
        ctx.mockHttp.get.mock.calls.filter((call: any[]) =>
          String(call[0]).endsWith('/api/sessions/terminal-during-poll/connection'),
        ),
      ).toHaveLength(2);

      fireSseMessage(ctx.sseInstances[0], { method: 'session.ended', params: {} }, '6:1');
      poll.next({
        state: 'ready',
        control_socket: 'websocket',
        ws_url: 'wss://api.example.com/stale-ready',
        token: 'stale',
        expires_at: 0,
      });
      poll.complete();
      await connecting;
      await vi.advanceTimersByTimeAsync(1_100_000);

      expect(ctx.wsInstances).toHaveLength(0);
      expect(ctx.service.sessionReady()).toBe(false);
      expect(ctx.sseInstances[0].close).not.toHaveBeenCalled();
      expect(
        ctx.mockHttp.get.mock.calls.filter((call: any[]) =>
          String(call[0]).endsWith('/api/sessions/terminal-during-poll/connection'),
        ),
      ).toHaveLength(2);
    } finally {
      vi.useRealTimers();
    }
  });

  it('a terminal SSE cancels the readiness backoff before another poll', async () => {
    vi.useFakeTimers();
    try {
      const ctx = createService();
      ctx.mockHttp.get.mockImplementation(
        connectGetMock({
          connectionResponses: [
            throwError(() => ({ status: 425 })),
            throwError(() => ({ status: 425 })),
          ],
        }),
      );
      ctx.mockHttp.post.mockReturnValue(of({ state: 'provisioning' }));

      const connecting = ctx.service.connect('terminal-during-backoff');
      await flushMicrotasks(20);
      expect(
        ctx.mockHttp.get.mock.calls.filter((call: any[]) =>
          String(call[0]).endsWith('/api/sessions/terminal-during-backoff/connection'),
        ),
      ).toHaveLength(2);

      fireSseMessage(ctx.sseInstances[0], { method: 'session.ended', params: {} }, '7:1');
      expect(ctx.sseInstances[0].close).not.toHaveBeenCalled();
      await vi.advanceTimersByTimeAsync(1_100_000);
      await connecting;
      window.dispatchEvent(new Event('focus'));
      window.dispatchEvent(new Event('online'));
      await flushMicrotasks();

      expect(
        ctx.mockHttp.get.mock.calls.filter((call: any[]) =>
          String(call[0]).endsWith('/api/sessions/terminal-during-backoff/connection'),
        ),
      ).toHaveLength(2);
      expect(ctx.mockHttp.post).toHaveBeenCalledTimes(1);
      expect(ctx.wsInstances).toHaveLength(0);
      expect(ctx.service.sessionReady()).toBe(false);
    } finally {
      vi.useRealTimers();
    }
  });

  it('terminal retirement cancels an armed control reconnect and ignores late lifecycle ready', async () => {
    vi.useFakeTimers();
    try {
      const ctx = createService();
      ctx.mockHttp.get.mockImplementation(connectGetMock());
      await ctx.service.connect('terminal-reconnect-timer');
      expect(ctx.wsInstances).toHaveLength(1);

      ctx.wsInstances[0].onclose?.({ code: 1006, reason: 'drop' } as CloseEvent);
      fireSseMessage(ctx.sseInstances[0], { method: 'session.ended', params: {} }, '8:1');
      ctx.notifications.lifecycleEvent.set({
        thread_id: 'terminal-reconnect-timer',
        state: 'ready',
      });
      TestBed.tick();
      await vi.advanceTimersByTimeAsync(1_100_000);

      expect(ctx.wsInstances).toHaveLength(1);
      expect(ctx.service.sessionReady()).toBe(false);
      expect(ctx.service.startupPhase()).toBeNull();
      expect(ctx.sseInstances[0].close).not.toHaveBeenCalled();
      expect(
        ctx.mockHttp.get.mock.calls.filter((call: any[]) =>
          String(call[0]).endsWith('/api/sessions/terminal-reconnect-timer/connection'),
        ),
      ).toHaveLength(1);
    } finally {
      vi.useRealTimers();
    }
  });

  it('terminal retirement drops a late fresh-token response after a 4401 close', async () => {
    vi.useFakeTimers();
    try {
      const ctx = createService();
      const freshToken = new Subject<any>();
      ctx.mockHttp.get.mockImplementation(
        connectGetMock({
          connectionResponses: [
            of({
              state: 'ready',
              control_socket: 'websocket',
              ws_url: 'wss://api.example.com/initial-token',
              token: 'initial',
              expires_at: 0,
            }),
            freshToken,
          ],
        }),
      );
      await ctx.service.connect('terminal-fresh-token');
      ctx.wsInstances[0].onclose?.({ code: 4401, reason: 'expired' } as CloseEvent);
      await flushMicrotasks();

      fireSseMessage(ctx.sseInstances[0], { method: 'session.ended', params: {} }, '9:1');
      freshToken.next({
        state: 'ready',
        control_socket: 'websocket',
        ws_url: 'wss://api.example.com/stale-token',
        token: 'stale',
        expires_at: 0,
      });
      freshToken.complete();
      await flushMicrotasks();
      await vi.advanceTimersByTimeAsync(1_100_000);

      expect(ctx.wsInstances).toHaveLength(1);
      expect(ctx.service.sessionReady()).toBe(false);
      expect(ctx.sseInstances[0].close).not.toHaveBeenCalled();
      expect(
        ctx.mockHttp.get.mock.calls.filter((call: any[]) =>
          String(call[0]).endsWith('/api/sessions/terminal-fresh-token/connection'),
        ),
      ).toHaveLength(2);
    } finally {
      vi.useRealTimers();
    }
  });

  it('only the typed nested session_ended 409 is terminal; a generic 409 retries', async () => {
    vi.useFakeTimers();
    try {
      const terminal = createService();
      terminal.mockHttp.get.mockImplementation(
        connectGetMock({
          connectionResponses: [
            throwError(() => ({
              status: 409,
              error: { detail: { code: 'session_ended' } },
            })),
          ],
        }),
      );
      await terminal.service.connect('typed-ended');
      expect(terminal.service.threadStatus()).toBe('ended');
      expect(terminal.sseInstances[0].close).not.toHaveBeenCalled();
      await vi.advanceTimersByTimeAsync(20_000);
      expect(terminal.wsInstances).toHaveLength(0);

      const transient = createService();
      transient.mockHttp.get.mockImplementation(
        connectGetMock({
          connectionResponses: [
            throwError(() => ({ status: 409, error: { detail: 'agent booting' } })),
            of({
              state: 'ready',
              control_socket: 'websocket',
              ws_url: 'wss://api.example.com/p/generic/ws?t=fresh',
              token: 'fresh',
              expires_at: 0,
            }),
          ],
        }),
      );
      await transient.service.connect('generic-409');
      expect(transient.service.threadStatus()).not.toBe('ended');
      await vi.advanceTimersByTimeAsync(500);
      await flushMicrotasks();
      expect(transient.wsInstances.at(-1)?.url).toContain('/p/generic/ws');
    } finally {
      vi.useRealTimers();
    }
  });

  it('keeps the exact-generation contract after terminal refusal and Resume', async () => {
    const ctx = createService();
    const resumedConnection = new Subject<any>();
    ctx.mockHttp.get.mockImplementation(
      connectGetMock({
        connectionResponses: [
          throwError(() => ({
            status: 409,
            error: {
              detail: {
                code: 'session_ended',
                pinned_runtime_generation_contract: 1,
                session_runtime_generation: SESSION_RUNTIME_GENERATION,
              },
            },
          })),
          resumedConnection,
        ],
      }),
    );

    await ctx.service.connect('terminal-contract-resume');
    expect(ctx.service.threadStatus()).toBe('ended');

    const resume = ctx.service.resumeSession();
    await flushMicrotasks(20);
    const resumedSse = ctx.sseInstances.at(-1)!;
    fireSseOpen(resumedSse);

    // The exact G1 REST refusal permanently upgrades this thread to the
    // generation-bound event contract. Resume reopens control for G2, but a
    // delayed legacy/generationless G1 journal tail must not retire it while
    // the exact G2 /connection response is still in flight.
    fireSseMessage(resumedSse, { method: 'session.ended', params: {} }, '18:1');
    expect(ctx.service.threadStatus()).not.toBe('ended');
    expect(ctx.service.sessionReady()).toBe(false);
    expect(ctx.wsInstances).toHaveLength(0);

    resumedConnection.next({
      state: 'ready',
      control_socket: 'websocket',
      ws_url: 'wss://api.example.com/p/terminal-contract-g2/ws?t=fresh',
      token: 'fresh',
      expires_at: 0,
      pinned_runtime_generation_contract: 1,
      session_runtime_generation: SESSION_RUNTIME_GENERATION_B,
    });
    resumedConnection.complete();
    await resume;

    expect(ctx.wsInstances).toHaveLength(1);
    expect(ctx.wsInstances[0].url).toContain('/p/terminal-contract-g2/ws');
    expect(ctx.service.sessionReady()).toBe(true);
    expect(ctx.service.threadStatus()).not.toBe('ended');
  });

  it('latches an exact binding refusal without hiding SSE review or consuming queued input', async () => {
    vi.useFakeTimers();
    try {
      const ctx = createService();
      ctx.mockApi.getThreadCloudDiffOutcome = vi.fn().mockReturnValue(
        of({
          kind: 'ok',
          data: {
            thread_id: 'binding-invalid',
            epoch: 3,
            staged_at: '2026-08-26T14:00:00Z',
            counts: { added: 1, modified: 0, deleted: 0 },
            protected_mount: 'cloud',
            files: [],
          },
        }),
      );
      ctx.mockHttp.get.mockImplementation(
        connectGetMock({
          connectionResponses: [
            throwError(() => ({
              status: 409,
              error: {
                detail: {
                  code: 'session_binding_invalid',
                  pinned_runtime_generation_contract: 1,
                  session_runtime_generation: SESSION_RUNTIME_GENERATION,
                },
              },
            })),
          ],
        }),
      );

      await ctx.service.connect('binding-invalid');
      const initialSse = ctx.sseInstances[0];
      fireSseOpen(initialSse);
      await ctx.service.sendMessage('keep this queued');

      expect(ctx.service.connectionState()).toBe('error');
      expect(ctx.service.error()).toBe('errors.sessions.bindingInvalid');
      expect(ctx.service.threadStatus()).toBe('active');
      expect(ctx.service.sessionReady()).toBe(false);
      expect(ctx.service.outbox().map((item) => item.displayContent)).toEqual(['keep this queued']);
      expect(initialSse.close).not.toHaveBeenCalled();
      expect(ctx.wsInstances).toHaveLength(0);

      ctx.notifications.cloudDiffStagedEvent.set({
        thread_id: 'binding-invalid',
        session_runtime_generation: SESSION_RUNTIME_GENERATION,
        staged_epoch: 3,
        file_count: 1,
        counts: { added: 1, modified: 0, deleted: 0 },
        mount_id: 'reader-1',
      });
      TestBed.tick();
      await flushMicrotasks();
      expect(ctx.mockApi.getThreadCloudDiffOutcome).toHaveBeenCalledTimes(1);
      expect(ctx.service.cloudChangesCount()).toBe(1);

      // Manual and watchdog review-plane reopens must preserve the explicit
      // control error and never restart connection/prepare for the same G.
      ctx.service.reconnectNow();
      await vi.advanceTimersByTimeAsync(0);
      const manualSse = ctx.sseInstances.at(-1)!;
      fireSseOpen(manualSse);
      expect(ctx.service.connectionState()).toBe('error');
      expect(ctx.service.error()).toBe('errors.sessions.bindingInvalid');

      await vi.advanceTimersByTimeAsync(50_000);
      const watchdogSse = ctx.sseInstances.at(-1)!;
      expect(watchdogSse).not.toBe(manualSse);
      fireSseOpen(watchdogSse);
      expect(ctx.service.connectionState()).toBe('error');
      expect(ctx.service.error()).toBe('errors.sessions.bindingInvalid');
      expect(ctx.service.outbox().map((item) => item.displayContent)).toEqual(['keep this queued']);

      const connectionCalls = ctx.mockHttp.get.mock.calls.filter((call: any[]) =>
        String(call[0]).endsWith('/api/sessions/binding-invalid/connection'),
      );
      const prepareCalls = ctx.mockHttp.post.mock.calls.filter((call: any[]) =>
        String(call[0]).endsWith('/api/sessions/binding-invalid/prepare'),
      );
      expect(connectionCalls).toHaveLength(1);
      expect(prepareCalls).toHaveLength(0);
      expect(ctx.wsInstances).toHaveLength(0);
    } finally {
      vi.useRealTimers();
    }
  });

  it('latches an exact input refusal, retires G1 controls, and keeps the message queued', async () => {
    vi.useFakeTimers();
    try {
      const ctx = createService();
      const liveWs = createMockWs();
      const reviewSse = createMockEventSource();
      liveWs.onopen = vi.fn();
      liveWs.onmessage = vi.fn();
      liveWs.onerror = vi.fn();
      liveWs.onclose = vi.fn();
      ctx.service.threadId.set('binding-invalid-input');
      ctx.service.threadStatus.set('active');
      ctx.service.sessionReady.set(true);
      (ctx.service as any).sessionRuntimeGeneration = SESSION_RUNTIME_GENERATION;
      (ctx.service as any).controlWs = liveWs;
      (ctx.service as any).sse = reviewSse;
      (ctx.service as any).controlWsOpening = true;
      (ctx.service as any).controlWsReconnectTimer = setTimeout(() => undefined, 60_000);
      (ctx.service as any).controlWsWatchdogTimer = setInterval(() => undefined, 60_000);
      (ctx.service as any).controlOutbox = [
        { threadId: 'binding-invalid-input', frame: JSON.stringify({ method: 'approve' }) },
      ];
      (ctx.service as any).pendingPermissions.set([{ id: 'approval-g1', tool: 'shell' }]);
      const openingGeneration = (ctx.service as any).controlWsOpeningGeneration;
      ctx.mockHttp.post.mockReturnValue(
        throwError(() => ({
          status: 409,
          error: {
            detail: {
              code: 'session_binding_invalid',
              pinned_runtime_generation_contract: 1,
              session_runtime_generation: SESSION_RUNTIME_GENERATION,
            },
          },
        })),
      );

      await ctx.service.sendMessage('keep this exact input queued');
      await flushMicrotasks(20);

      expect(ctx.mockHttp.post).toHaveBeenCalledTimes(1);
      expect(ctx.service.outbox().map((item) => item.displayContent)).toEqual([
        'keep this exact input queued',
      ]);
      expect(ctx.service.sessionReady()).toBe(false);
      expect(ctx.service.connectionState()).toBe('error');
      expect(ctx.service.error()).toBe('errors.sessions.bindingInvalid');
      expect(ctx.service.outboxStalled()).toBe(true);
      expect((ctx.service as any).controlWsOpeningGeneration).toBe(openingGeneration + 1);
      expect((ctx.service as any).controlWsOpening).toBe(false);
      expect((ctx.service as any).controlWsReconnectTimer).toBeNull();
      expect((ctx.service as any).controlWsWatchdogTimer).toBeNull();
      expect((ctx.service as any).controlWs).toBeNull();
      expect((ctx.service as any).controlOutbox).toEqual([]);
      expect(ctx.service.pendingPermissions()).toEqual([]);
      expect(liveWs.close).toHaveBeenCalledWith(1000);
      expect(liveWs.onopen).toBeNull();
      expect(liveWs.onmessage).toBeNull();
      expect(liveWs.onerror).toBeNull();
      expect(liveWs.onclose).toBeNull();
      expect(reviewSse.close).not.toHaveBeenCalled();

      (ctx.service as any)._sendControl({ method: 'approve' });
      expect(liveWs.send).not.toHaveBeenCalled();
      expect((ctx.service as any).controlOutbox).toEqual([]);

      ctx.service.retryQueuedSends();
      await flushMicrotasks(10);
      expect(ctx.mockHttp.post).toHaveBeenCalledTimes(1);
      expect(ctx.service.error()).toBe('errors.sessions.bindingInvalid');
      expect(ctx.service.outbox()).toHaveLength(1);
    } finally {
      vi.useRealTimers();
    }
  });

  it('does not let a delayed G1 input refusal overwrite terminal retirement', async () => {
    const ctx = createService();
    const response = new Subject<unknown>();
    ctx.service.threadId.set('binding-input-terminal-race');
    ctx.service.threadStatus.set('active');
    ctx.service.sessionReady.set(true);
    (ctx.service as any).sessionRuntimeGeneration = SESSION_RUNTIME_GENERATION;
    ctx.mockHttp.post.mockReturnValue(response);

    await ctx.service.sendMessage('remain queued after End');
    await flushMicrotasks(10);
    expect(ctx.mockHttp.post).toHaveBeenCalledTimes(1);

    (ctx.service as any)._retireTerminalControl(
      'binding-input-terminal-race',
      SESSION_RUNTIME_GENERATION,
    );
    response.error({
      status: 409,
      error: {
        detail: {
          code: 'session_binding_invalid',
          pinned_runtime_generation_contract: 1,
          session_runtime_generation: SESSION_RUNTIME_GENERATION,
        },
      },
    });
    await flushMicrotasks(20);

    expect(ctx.service.threadStatus()).toBe('ended');
    expect(ctx.service.connectionState()).toBe('disconnected');
    expect((ctx.service as any).terminalControlThreadId).toBe('binding-input-terminal-race');
    expect((ctx.service as any).invalidBindingRuntime).toBeNull();
    expect(ctx.service.error()).not.toBe('errors.sessions.bindingInvalid');
    expect(ctx.service.outbox()).toHaveLength(1);
    expect(ctx.service.outboxStalled()).toBe(true);
  });

  it('does not let a delayed G1 input refusal poison an installed G2 binding', async () => {
    const ctx = createService();
    const response = new Subject<unknown>();
    const g2Ws = createMockWs();
    ctx.service.threadId.set('binding-input-successor-race');
    ctx.service.threadStatus.set('active');
    ctx.service.sessionReady.set(true);
    (ctx.service as any).sessionRuntimeGeneration = SESSION_RUNTIME_GENERATION;
    ctx.mockHttp.post.mockReturnValue(response);

    await ctx.service.sendMessage('retry me under G2');
    await flushMicrotasks(10);
    expect(ctx.mockHttp.post).toHaveBeenCalledTimes(1);

    (ctx.service as any).controlWsOpeningGeneration++;
    (ctx.service as any).sessionRuntimeGeneration = SESSION_RUNTIME_GENERATION_B;
    (ctx.service as any).controlWs = g2Ws;
    response.error({
      status: 409,
      error: {
        detail: {
          code: 'session_binding_invalid',
          pinned_runtime_generation_contract: 1,
          session_runtime_generation: SESSION_RUNTIME_GENERATION,
        },
      },
    });
    await flushMicrotasks(20);

    expect((ctx.service as any).sessionRuntimeGeneration).toBe(SESSION_RUNTIME_GENERATION_B);
    expect((ctx.service as any).invalidBindingRuntime).toBeNull();
    expect((ctx.service as any).controlWs).toBe(g2Ws);
    expect(g2Ws.close).not.toHaveBeenCalled();
    expect(ctx.service.sessionReady()).toBe(true);
    expect(ctx.service.outbox()).toHaveLength(1);
    expect(ctx.service.outboxStalled()).toBe(true);

    (ctx.service as any)._sendControl({ method: 'approve' });
    expect(g2Ws.send).toHaveBeenCalledWith(JSON.stringify({ method: 'approve' }));
    ctx.mockHttp.post.mockReturnValue(of({ accepted: true, turn_id: 2 }));
    ctx.service.retryQueuedSends();
    await flushMicrotasks(20);
    expect(ctx.mockHttp.post).toHaveBeenCalledTimes(2);
    expect(ctx.service.outbox()).toEqual([]);
  });

  it('ignores same-G lifecycle noise after binding refusal and reconnects only for G2', async () => {
    const ctx = createService();
    ctx.mockHttp.get.mockImplementation(
      connectGetMock({
        connectionResponses: [
          throwError(() => ({
            status: 409,
            error: {
              detail: {
                code: 'session_binding_invalid',
                pinned_runtime_generation_contract: 1,
                session_runtime_generation: SESSION_RUNTIME_GENERATION,
              },
            },
          })),
          of({
            state: 'ready',
            control_socket: 'websocket',
            ws_url: 'wss://api.example.com/p/binding-g2/ws?t=fresh',
            token: 'fresh',
            expires_at: 0,
            pinned_runtime_generation_contract: 1,
            session_runtime_generation: SESSION_RUNTIME_GENERATION_B,
          }),
        ],
      }),
    );

    await ctx.service.connect('binding-successor');
    fireSseOpen(ctx.sseInstances[0]);
    await ctx.service.sendMessage('run on the successor');

    ctx.notifications.lifecycleEvent.set({
      thread_id: 'binding-successor',
      state: 'ready',
      session_runtime_generation: SESSION_RUNTIME_GENERATION,
    });
    TestBed.tick();
    await flushMicrotasks();
    expect(ctx.wsInstances).toHaveLength(0);
    expect(ctx.service.connectionState()).toBe('error');
    expect(ctx.service.outbox()).toHaveLength(1);

    ctx.notifications.lifecycleEvent.set({
      thread_id: 'binding-successor',
      state: 'ready',
      session_runtime_generation: SESSION_RUNTIME_GENERATION_B,
    });
    TestBed.tick();
    await flushMicrotasks(20);

    expect(ctx.wsInstances).toHaveLength(1);
    expect(ctx.wsInstances[0].url).toContain('/p/binding-g2/ws');
    expect(ctx.service.connectionState()).toBe('connected');
    expect(ctx.service.sessionReady()).toBe(true);
    expect(ctx.service.error()).toBeNull();
    expect(ctx.service.outbox()).toEqual([]);
    expect(
      ctx.mockHttp.post.mock.calls.some((call: any[]) =>
        String(call[0]).endsWith('/api/sessions/binding-successor/prepare'),
      ),
    ).toBe(false);
  });

  it('lets exact G2 terminal authority cancel a paused binding-recovery GET', async () => {
    const ctx = createService();
    const recoveryConnection = new Subject<any>();
    ctx.mockApi.getThreadCloudDiffOutcome = vi.fn().mockReturnValue(
      of({
        kind: 'ok',
        data: {
          thread_id: 'binding-recovery-terminal',
          epoch: 4,
          staged_at: '2026-08-26T14:01:00Z',
          counts: { added: 0, modified: 1, deleted: 0 },
          protected_mount: 'cloud',
          files: [],
        },
      }),
    );
    ctx.mockHttp.get.mockImplementation(
      connectGetMock({
        connectionResponses: [
          throwError(() => ({
            status: 409,
            error: {
              detail: {
                code: 'session_binding_invalid',
                pinned_runtime_generation_contract: 1,
                session_runtime_generation: SESSION_RUNTIME_GENERATION,
              },
            },
          })),
          recoveryConnection,
        ],
      }),
    );

    await ctx.service.connect('binding-recovery-terminal');
    const es = ctx.sseInstances[0];
    fireSseOpen(es);
    ctx.notifications.lifecycleEvent.set({
      thread_id: 'binding-recovery-terminal',
      state: 'ready',
      session_runtime_generation: SESSION_RUNTIME_GENERATION_B,
    });
    TestBed.tick();
    await flushMicrotasks(20);

    expect(ctx.service.connectionState()).toBe('connecting');
    expect(
      ctx.mockHttp.get.mock.calls.filter((call: any[]) =>
        String(call[0]).endsWith('/api/sessions/binding-recovery-terminal/connection'),
      ),
    ).toHaveLength(2);

    ctx.notifications.cloudDiffStagedEvent.set({
      thread_id: 'binding-recovery-terminal',
      session_runtime_generation: SESSION_RUNTIME_GENERATION_B,
      staged_epoch: 4,
      file_count: 1,
      counts: { added: 0, modified: 1, deleted: 0 },
      mount_id: 'reader-2',
    });
    TestBed.tick();
    await flushMicrotasks();
    expect(ctx.mockApi.getThreadCloudDiffOutcome).toHaveBeenCalledTimes(1);
    expect(ctx.service.cloudChangesCount()).toBe(1);

    fireSseMessage(
      es,
      {
        method: 'session.ended',
        params: { session_runtime_generation: SESSION_RUNTIME_GENERATION_B },
      },
      '13:1',
    );
    expect(ctx.service.threadStatus()).toBe('ended');
    expect(ctx.service.connectionState()).toBe('disconnected');

    recoveryConnection.next({
      state: 'ready',
      control_socket: 'websocket',
      ws_url: 'wss://api.example.com/p/binding-recovery-terminal/ws?t=stale',
      token: 'stale',
      expires_at: 0,
      pinned_runtime_generation_contract: 1,
      session_runtime_generation: SESSION_RUNTIME_GENERATION_B,
    });
    recoveryConnection.complete();
    await flushMicrotasks(20);

    expect(ctx.wsInstances).toHaveLength(0);
    expect(ctx.service.sessionReady()).toBe(false);
    expect(ctx.service.threadStatus()).toBe('ended');
    expect(es.close).not.toHaveBeenCalled();
  });

  it('ignores delayed G1 terminal journal after G2 wakes binding recovery', async () => {
    const ctx = createService();
    const recoveryConnection = new Subject<any>();
    ctx.mockHttp.get.mockImplementation(
      connectGetMock({
        connectionResponses: [
          throwError(() => ({
            status: 409,
            error: {
              detail: {
                code: 'session_binding_invalid',
                pinned_runtime_generation_contract: 1,
                session_runtime_generation: SESSION_RUNTIME_GENERATION,
              },
            },
          })),
          recoveryConnection,
        ],
      }),
    );

    await ctx.service.connect('binding-recovery-stale-terminal');
    const es = ctx.sseInstances[0];
    fireSseOpen(es);
    ctx.notifications.lifecycleEvent.set({
      thread_id: 'binding-recovery-stale-terminal',
      state: 'ready',
      session_runtime_generation: SESSION_RUNTIME_GENERATION_B,
    });
    TestBed.tick();
    await flushMicrotasks(20);

    fireSseMessage(
      es,
      {
        method: 'session.ended',
        params: { session_runtime_generation: SESSION_RUNTIME_GENERATION },
      },
      '14:1',
    );
    expect(ctx.service.threadStatus()).toBe('active');
    expect(ctx.service.connectionState()).toBe('connecting');

    recoveryConnection.next({
      state: 'ready',
      control_socket: 'websocket',
      ws_url: 'wss://api.example.com/p/binding-recovery-g2/ws?t=fresh',
      token: 'fresh',
      expires_at: 0,
      pinned_runtime_generation_contract: 1,
      session_runtime_generation: SESSION_RUNTIME_GENERATION_B,
    });
    recoveryConnection.complete();
    await flushMicrotasks(20);

    expect(ctx.wsInstances).toHaveLength(1);
    expect(ctx.wsInstances[0].url).toContain('/p/binding-recovery-g2/ws');
    expect(ctx.service.sessionReady()).toBe(true);
    expect(ctx.service.threadStatus()).toBe('active');
    expect(ctx.service.connectionState()).toBe('connected');
  });

  it('does not install a rejected generation returned during G2 recovery', async () => {
    const ctx = createService();
    ctx.mockHttp.get.mockImplementation(
      connectGetMock({
        connectionResponses: [
          throwError(() => ({
            status: 409,
            error: {
              detail: {
                code: 'session_binding_invalid',
                pinned_runtime_generation_contract: 1,
                session_runtime_generation: SESSION_RUNTIME_GENERATION,
              },
            },
          })),
          of({
            state: 'ready',
            control_socket: 'websocket',
            ws_url: 'wss://api.example.com/p/rejected-generation/ws?t=stale',
            token: 'stale',
            expires_at: 0,
            pinned_runtime_generation_contract: 1,
            session_runtime_generation: SESSION_RUNTIME_GENERATION,
          }),
        ],
      }),
    );

    await ctx.service.connect('binding-rejected-response');
    fireSseOpen(ctx.sseInstances[0]);
    ctx.notifications.lifecycleEvent.set({
      thread_id: 'binding-rejected-response',
      state: 'ready',
      session_runtime_generation: SESSION_RUNTIME_GENERATION_B,
    });
    TestBed.tick();
    await flushMicrotasks(20);

    expect(ctx.wsInstances).toHaveLength(0);
    expect(ctx.service.sessionReady()).toBe(false);
    expect(ctx.service.connectionState()).toBe('error');
    expect(ctx.service.error()).toBe('errors.sessions.bindingInvalid');
  });

  it('records exact G3 terminal refusal during G2 recovery for late staged review', async () => {
    const generationC = '77777777-7777-4777-8777-777777777777';
    const ctx = createService();
    ctx.mockApi.getThreadCloudDiffOutcome = vi.fn().mockReturnValue(
      of({
        kind: 'ok',
        data: {
          thread_id: 'binding-terminal-g3',
          epoch: 5,
          staged_at: '2026-08-26T14:02:00Z',
          counts: { added: 0, modified: 0, deleted: 2 },
          protected_mount: 'cloud',
          files: [],
        },
      }),
    );
    ctx.mockHttp.get.mockImplementation(
      connectGetMock({
        connectionResponses: [
          throwError(() => ({
            status: 409,
            error: {
              detail: {
                code: 'session_binding_invalid',
                pinned_runtime_generation_contract: 1,
                session_runtime_generation: SESSION_RUNTIME_GENERATION,
              },
            },
          })),
          throwError(() => ({
            status: 409,
            error: {
              detail: {
                code: 'session_ended',
                pinned_runtime_generation_contract: 1,
                session_runtime_generation: generationC,
              },
            },
          })),
        ],
      }),
    );

    await ctx.service.connect('binding-terminal-g3');
    fireSseOpen(ctx.sseInstances[0]);
    ctx.notifications.lifecycleEvent.set({
      thread_id: 'binding-terminal-g3',
      state: 'ready',
      session_runtime_generation: SESSION_RUNTIME_GENERATION_B,
    });
    TestBed.tick();
    await flushMicrotasks(20);

    expect(ctx.service.threadStatus()).toBe('ended');
    expect(ctx.service.connectionState()).toBe('disconnected');
    expect(ctx.wsInstances).toHaveLength(0);

    ctx.notifications.cloudDiffStagedEvent.set({
      thread_id: 'binding-terminal-g3',
      session_runtime_generation: generationC,
      staged_epoch: 5,
      file_count: 2,
      counts: { added: 0, modified: 0, deleted: 2 },
      mount_id: 'reader-3',
    });
    TestBed.tick();
    await flushMicrotasks();

    expect(ctx.mockApi.getThreadCloudDiffOutcome).toHaveBeenCalledTimes(1);
    expect(ctx.service.cloudChangesCount()).toBe(2);
  });

  it('does not terminal-latch a malformed binding-invalid 409', async () => {
    const ctx = createService();
    ctx.mockHttp.get.mockImplementation(
      connectGetMock({
        connectionResponses: [
          throwError(() => ({
            status: 409,
            error: {
              detail: {
                code: 'session_binding_invalid',
                pinned_runtime_generation_contract: 1,
                session_runtime_generation: 'not-a-generation',
              },
            },
          })),
          of({
            state: 'ready',
            control_socket: 'websocket',
            ws_url: 'wss://api.example.com/p/malformed-retry/ws?t=fresh',
            token: 'fresh',
            expires_at: 0,
            pinned_runtime_generation_contract: 1,
            session_runtime_generation: SESSION_RUNTIME_GENERATION,
          }),
        ],
      }),
    );

    await ctx.service.connect('binding-malformed');
    fireSseOpen(ctx.sseInstances[0]);

    expect(ctx.wsInstances).toHaveLength(1);
    expect(ctx.wsInstances[0].url).toContain('/p/malformed-retry/ws');
    expect(ctx.service.connectionState()).toBe('connected');
    expect(ctx.service.sessionReady()).toBe(true);
  });

  it('polls a bound-but-booting generic 409 past the short reconnect budget without prepare', async () => {
    vi.useFakeTimers();
    try {
      const ctx = createService();
      const booting = Array.from({ length: 15 }, () =>
        throwError(() => ({ status: 409, error: { detail: 'agent booting' } })),
      );
      ctx.mockHttp.get.mockImplementation(
        connectGetMock({
          connectionResponses: [
            ...booting,
            of({
              state: 'ready',
              control_socket: 'websocket',
              ws_url: 'wss://api.example.com/p/slow-boot/ws?t=fresh',
              token: 'fresh',
              expires_at: 0,
              pinned_runtime_generation_contract: 1,
              session_runtime_generation: SESSION_RUNTIME_GENERATION,
            }),
          ],
        }),
      );

      const connecting = ctx.service.connect('generic-409-slow-boot');
      await flushMicrotasks(20);
      await vi.advanceTimersByTimeAsync(35_000);
      await connecting;

      const connectionCalls = ctx.mockHttp.get.mock.calls.filter((call: any[]) =>
        String(call[0]).endsWith('/api/sessions/generic-409-slow-boot/connection'),
      );
      expect(connectionCalls.length).toBe(16);
      expect(
        ctx.mockHttp.post.mock.calls.some((call: any[]) =>
          String(call[0]).endsWith('/api/sessions/generic-409-slow-boot/prepare'),
        ),
      ).toBe(false);
      expect(ctx.wsInstances.at(-1)?.url).toContain('/p/slow-boot/ws');
      expect(ctx.service.sessionReady()).toBe(true);
    } finally {
      vi.useRealTimers();
    }
  });

  it('terminal retirement cancels the long generic-409 readiness poll', async () => {
    vi.useFakeTimers();
    try {
      const ctx = createService();
      ctx.mockHttp.get.mockImplementation(
        connectGetMock({
          connectionResponses: [
            throwError(() => ({ status: 409, error: { detail: 'agent booting' } })),
            throwError(() => ({ status: 409, error: { detail: 'agent booting' } })),
          ],
        }),
      );

      const connecting = ctx.service.connect('generic-409-terminal');
      await flushMicrotasks(20);
      expect(
        ctx.mockHttp.get.mock.calls.filter((call: any[]) =>
          String(call[0]).endsWith('/api/sessions/generic-409-terminal/connection'),
        ),
      ).toHaveLength(2);

      fireSseMessage(ctx.sseInstances[0], { method: 'session.ended', params: {} }, '9:2');
      await vi.advanceTimersByTimeAsync(1_100_000);
      await connecting;

      expect(
        ctx.mockHttp.get.mock.calls.filter((call: any[]) =>
          String(call[0]).endsWith('/api/sessions/generic-409-terminal/connection'),
        ),
      ).toHaveLength(2);
      expect(ctx.mockHttp.post).not.toHaveBeenCalled();
      expect(ctx.wsInstances).toHaveLength(0);
      expect(ctx.service.sessionReady()).toBe(false);
      expect(ctx.sseInstances[0].close).not.toHaveBeenCalled();
    } finally {
      vi.useRealTimers();
    }
  });

  it('keeps the full bounded terminal probe chain after an existing diff is resolved', async () => {
    vi.useFakeTimers();
    try {
      const ctx = createService();
      const summary = (added: number, stagedAt: string | null) => ({
        thread_id: 'late-stage',
        epoch: added ? 8 : 7,
        staged_at: stagedAt,
        counts: { added, modified: 0, deleted: 0 },
        protected_mount: 'cloud',
        files: [],
      });
      ctx.mockApi.getThreadCloudDiffOutcome = vi
        .fn()
        .mockReturnValueOnce(of({ kind: 'ok', data: summary(4, '2026-08-26T00:00:00Z') }))
        .mockReturnValueOnce(of({ kind: 'ok', data: summary(0, null) }))
        .mockReturnValueOnce(of({ kind: 'ok', data: summary(6, '2026-08-26T00:00:10Z') }))
        .mockReturnValue(of({ kind: 'ok', data: summary(6, '2026-08-26T00:00:10Z') }));
      ctx.service.threadId.set('late-stage');
      (ctx.service as any).intentionalClose = false;
      (ctx.service as any)._protectedCloud.set(true);
      ctx.service.cloudChangesCount.set(4);

      (ctx.service as any)._retireTerminalControl('late-stage');
      await vi.advanceTimersByTimeAsync(0);
      expect(ctx.service.cloudChangesCount()).toBe(4);
      await vi.advanceTimersByTimeAsync(1_000);
      expect(ctx.service.cloudChangesCount()).toBe(0);
      await vi.advanceTimersByTimeAsync(3_000);
      expect(ctx.service.cloudChangesCount()).toBe(6);
      expect(ctx.service.cloudStagedAt()).toBe('2026-08-26T00:00:10Z');
      expect(ctx.mockApi.getThreadCloudDiffOutcome).toHaveBeenCalledTimes(3);
      expect(ctx.mockHttp.post).not.toHaveBeenCalled();
      expect(ctx.wsInstances).toHaveLength(0);
    } finally {
      vi.useRealTimers();
    }
  });

  it('discovers a durable staged diff after the final terminal fallback probe', async () => {
    vi.useFakeTimers();
    try {
      const ctx = createService();
      let staged = false;
      ctx.mockApi.getThreadCloudDiffOutcome = vi.fn().mockImplementation(() =>
        of({
          kind: 'ok',
          data: {
            thread_id: 'late-stage-event',
            epoch: staged ? 12 : 11,
            staged_at: staged ? '2026-08-26T05:00:00Z' : null,
            counts: { added: staged ? 4 : 0, modified: 0, deleted: 0 },
            protected_mount: 'cloud',
            files: [],
          },
        }),
      );
      ctx.service.threadId.set('late-stage-event');
      (ctx.service as any).intentionalClose = false;
      (ctx.service as any)._protectedCloud.set(true);
      (ctx.service as any).sessionRuntimeGeneration = SESSION_RUNTIME_GENERATION;
      (ctx.service as any)._retireTerminalControl('late-stage-event');

      // Exhaust the complete bounded fallback schedule. The durable event is
      // the edge that closes the valid >9 minute archive-stage blind spot.
      await vi.advanceTimersByTimeAsync(600_000);
      const callsAfterFallback = ctx.mockApi.getThreadCloudDiffOutcome.mock.calls.length;
      expect(callsAfterFallback).toBe(9);
      expect(ctx.service.cloudChangesCount()).toBe(0);

      staged = true;
      (ctx.service as any)._handleEvent({
        method: 'cloud.diff_staged',
        params: {
          thread_id: 'late-stage-event',
          session_runtime_generation: SESSION_RUNTIME_GENERATION,
          staged_epoch: 12,
          // Event counts are a wake-up hint only; the summary below is the
          // reviewed-data authority and must win even under mutation.
          file_count: 999,
          counts: { added: 999, modified: 0, deleted: 0 },
          mount_id: 'reader-1',
        },
      });
      await flushMicrotasks();

      expect(ctx.mockApi.getThreadCloudDiffOutcome).toHaveBeenCalledTimes(callsAfterFallback + 1);
      expect(ctx.service.cloudChangesCount()).toBe(4);
      expect(ctx.service.cloudStagedAt()).toBe('2026-08-26T05:00:00Z');
      expect(ctx.mockHttp.post).not.toHaveBeenCalled();
      expect(ctx.wsInstances).toHaveLength(0);
    } finally {
      vi.useRealTimers();
    }
  });

  it('rejects a retired staged event across quick Resume and accepts only G2', async () => {
    const ctx = createService();
    const generation2 = '66666666-6666-4666-8666-666666666666';
    ctx.mockApi.getThreadCloudDiffOutcome = vi.fn().mockReturnValue(
      of({
        kind: 'ok',
        data: {
          thread_id: 'stage-resume',
          epoch: 13,
          staged_at: '2026-08-26T05:10:00Z',
          counts: { added: 1, modified: 0, deleted: 0 },
          protected_mount: 'cloud',
          files: [],
        },
      }),
    );
    ctx.service.threadId.set('stage-resume');
    (ctx.service as any).intentionalClose = false;
    (ctx.service as any)._protectedCloud.set(true);
    (ctx.service as any).sessionRuntimeGeneration = SESSION_RUNTIME_GENERATION;
    (ctx.service as any)._retireTerminalControl('stage-resume');
    (ctx.service as any)._reopenTerminalControl('stage-resume');

    const frame = (generation: string) => ({
      method: 'cloud.diff_staged',
      params: {
        thread_id: 'stage-resume',
        session_runtime_generation: generation,
        staged_epoch: 13,
        file_count: 1,
        counts: { added: 1, modified: 0, deleted: 0 },
        mount_id: 'reader-1',
      },
    });

    // Before G2 /connection installs its identity, a delayed G1 publication
    // cannot use the reopened review plane as authority.
    (ctx.service as any)._handleEvent(frame(SESSION_RUNTIME_GENERATION));
    await flushMicrotasks();
    expect(ctx.mockApi.getThreadCloudDiffOutcome).not.toHaveBeenCalled();

    (ctx.service as any).sessionRuntimeGeneration = generation2;
    (ctx.service as any)._handleEvent(frame(SESSION_RUNTIME_GENERATION));
    await flushMicrotasks();
    expect(ctx.mockApi.getThreadCloudDiffOutcome).not.toHaveBeenCalled();

    (ctx.service as any)._handleEvent(frame(generation2));
    await flushMicrotasks();
    expect(ctx.mockApi.getThreadCloudDiffOutcome).toHaveBeenCalledTimes(1);
    expect(ctx.service.cloudChangesCount()).toBe(1);
  });

  it('starts late-diff probing when protected metadata arrives after the terminal latch', async () => {
    vi.useFakeTimers();
    try {
      const ctx = createService();
      ctx.service.threadId.set('late-meta');
      (ctx.service as any).intentionalClose = false;
      ctx.mockApi.getThreadCloudDiffOutcome = vi.fn().mockReturnValue(
        of({
          kind: 'ok',
          data: {
            thread_id: 'late-meta',
            epoch: 3,
            staged_at: null,
            counts: { added: 0, modified: 0, deleted: 0 },
            protected_mount: 'cloud',
            files: [],
          },
        }),
      );
      ctx.mockHttp.get.mockReturnValue(
        of({
          id: 'late-meta',
          status: 'ended',
          metadata: { protected_cloud: true },
          mounts: [],
        }),
      );

      (ctx.service as any)._retireTerminalControl('late-meta');
      expect((ctx.service as any).terminalCloudProbeThreadId).toBeNull();
      await (ctx.service as any).loadThreadMeta('late-meta');
      await flushMicrotasks();
      await vi.advanceTimersByTimeAsync(0);

      expect((ctx.service as any).terminalCloudProbeThreadId).toBe('late-meta');
      // One metadata-triggered read plus the first bounded terminal retry.
      expect(ctx.mockApi.getThreadCloudDiffOutcome.mock.calls.length).toBeGreaterThanOrEqual(2);
      expect(ctx.mockHttp.post).not.toHaveBeenCalled();
      expect(ctx.wsInstances).toHaveLength(0);
    } finally {
      vi.useRealTimers();
    }
  });

  it('cold-loads an ended protected thread as SSE-only and discovers its review', async () => {
    const ctx = createService();
    ctx.mockApi.getThreadCloudDiffOutcome = vi.fn().mockReturnValue(
      of({
        kind: 'ok',
        data: {
          thread_id: 'cold-ended',
          epoch: 5,
          staged_at: '2026-08-26T01:00:00Z',
          counts: { added: 2, modified: 1, deleted: 1 },
          protected_mount: 'cloud',
          files: [],
        },
      }),
    );
    ctx.mockHttp.get.mockImplementation(
      connectGetMock({
        threadMeta: {
          id: 'cold-ended',
          status: 'ended',
          metadata: { protected_cloud: true },
          mounts: [],
        },
      }),
    );

    await ctx.service.connect('cold-ended');
    await flushMicrotasks();

    expect(ctx.sseInstances).toHaveLength(1);
    expect(ctx.sseInstances[0].close).not.toHaveBeenCalled();
    expect(ctx.wsInstances).toHaveLength(0);
    expect(ctx.service.cloudChangesCount()).toBe(4);
    expect(
      ctx.mockHttp.get.mock.calls.some((call: any[]) =>
        String(call[0]).endsWith('/api/sessions/cold-ended/connection'),
      ),
    ).toBe(false);
    expect(
      ctx.mockHttp.post.mock.calls.some((call: any[]) =>
        String(call[0]).endsWith('/api/sessions/cold-ended/prepare'),
      ),
    ).toBe(false);
  });

  it('NotificationService lifecycle events update startupPhase for the active thread', async () => {
    const ctx = createService();
    ctx.mockHttp.get.mockImplementation(
      connectGetMock({
        connectionResponses: [
          of({
            state: 'ready',
            ws_url: 'wss://api.example.com/p/t-life/ws?t=tok',
            token: 'tok',
            expires_at: 0,
          }),
        ],
      }),
    );
    await ctx.service.connect('t-life');

    // The constructor effect filters on threadId() — confirm the
    // active thread matches before firing events.
    expect(ctx.service.threadId()).toBe('t-life');
    ctx.service.sessionReady.set(false);
    ctx.service.startupPhase.set(null);

    ctx.notifications.lifecycleEvent.set({
      thread_id: 't-life',
      state: 'provisioning',
    });
    // Flush the constructor effect so it observes the signal change.
    TestBed.tick();
    expect(ctx.service.startupPhase()).toBe('provisioning');

    ctx.notifications.lifecycleEvent.set({
      thread_id: 't-life',
      state: 'booting',
    });
    TestBed.tick();
    expect(ctx.service.startupPhase()).toBe('booting');

    ctx.notifications.lifecycleEvent.set({
      thread_id: 't-life',
      state: 'ready',
    });
    TestBed.tick();
    // 'ready' from the server means agent is session-ready — the
    // cockpit now opens the WS, which is the "connecting" phase
    // client-side.
    expect(ctx.service.startupPhase()).toBe('connecting');

    // Events for a different thread are ignored.
    ctx.notifications.lifecycleEvent.set({
      thread_id: 'other-thread',
      state: 'provisioning',
    });
    TestBed.tick();
    expect(ctx.service.startupPhase()).toBe('connecting');
  });

  it('ignores a delayed lifecycle frame from a retired runtime generation', async () => {
    const generationA = SESSION_RUNTIME_GENERATION;
    const generationB = '66666666-6666-4666-8666-666666666666';
    const ctx = createService();
    let connectionGeneration = generationA;
    ctx.mockHttp.get.mockImplementation((url: string) =>
      url.endsWith('/connection')
        ? of({
            state: 'ready',
            control_socket: 'none',
            ws_url: null,
            token: null,
            expires_at: null,
            pinned_runtime_generation_contract: 1,
            session_runtime_generation: connectionGeneration,
          })
        : activeSessionGet(url),
    );
    await ctx.service.connect('lifecycle-generation-fence');
    (ctx.service as any)._retireTerminalControl('lifecycle-generation-fence');
    (ctx.service as any)._reopenTerminalControl('lifecycle-generation-fence');
    connectionGeneration = generationB;
    await (ctx.service as any)._openControlWs('lifecycle-generation-fence');
    ctx.service.sessionReady.set(false);
    ctx.service.startupPhase.set(null);

    ctx.notifications.lifecycleEvent.set({
      thread_id: 'lifecycle-generation-fence',
      state: 'failed',
      reason: 'stale G1',
      session_runtime_generation: generationA,
    });
    TestBed.tick();
    expect(ctx.service.startupPhase()).toBeNull();
    expect(ctx.service.error()).not.toBe('stale G1');

    ctx.notifications.lifecycleEvent.set({
      thread_id: 'lifecycle-generation-fence',
      state: 'booting',
      session_runtime_generation: generationB,
    });
    TestBed.tick();
    expect(ctx.service.startupPhase()).toBe('booting');
  });

  it('REST-observed End after a CLOSED SSE retires control and reopens only the SSE review plane', async () => {
    vi.useFakeTimers();
    try {
      const ctx = createService();
      let ended = false;
      ctx.mockApi.getThreadCloudDiffOutcome = vi.fn().mockReturnValue(
        of({
          kind: 'ok',
          data: {
            thread_id: 'rest-ended',
            epoch: 7,
            staged_at: '2026-08-26T02:00:00Z',
            counts: { added: 1, modified: 0, deleted: 0 },
            protected_mount: 'cloud',
            files: [],
          },
        }),
      );
      ctx.mockHttp.get.mockImplementation((url: string) => {
        if (url.endsWith('/connection')) {
          return of({
            state: 'ready',
            control_socket: 'websocket',
            ws_url: 'wss://api.example.com/p/rest-ended/ws?t=one',
            token: 'one',
            expires_at: 0,
          });
        }
        if (url.endsWith('/messages')) return of({ messages: [], total: 0 });
        if (url.endsWith('/state')) {
          return of({
            thread_id: 'rest-ended',
            permission_mode: 'supervised',
            narration_mode: 'auto',
            turn_count: 0,
            turn_in_flight: false,
            message_count: 0,
            pending_permissions: [],
            event_cursor: { epoch: 7, seq: 0 },
            replay_cursor: { epoch: 7, seq: 0 },
            snapshot_source: 'durable_journal',
          });
        }
        if (url.endsWith('/citations')) return of({ citations: [] });
        return of({
          id: 'rest-ended',
          status: ended ? 'ended' : 'active',
          metadata: ended ? { protected_cloud: true } : {},
          mounts: [],
        });
      });

      await ctx.service.connect('rest-ended');
      fireSseOpen(ctx.sseInstances[0]);
      ended = true;
      fireSseTerminalError(ctx.sseInstances[0]);
      await vi.advanceTimersByTimeAsync(1_500);
      await flushMicrotasks();

      expect(ctx.service.threadStatus()).toBe('ended');
      expect(ctx.wsInstances[0].close).toHaveBeenCalledWith(1000);
      expect(ctx.sseInstances.length).toBeGreaterThanOrEqual(2);
      expect(ctx.service.cloudChangesCount()).toBe(1);
      const connectionCalls = ctx.mockHttp.get.mock.calls.filter((call: any[]) =>
        String(call[0]).endsWith('/api/sessions/rest-ended/connection'),
      );
      expect(connectionCalls).toHaveLength(1);
      expect(
        ctx.mockHttp.post.mock.calls.some((call: any[]) =>
          String(call[0]).endsWith('/api/sessions/rest-ended/prepare'),
        ),
      ).toBe(false);
    } finally {
      vi.useRealTimers();
    }
  });

  it('WS close code 4401 re-fetches /connection and reopens WS with the fresh token', async () => {
    const ctx = createService();
    ctx.mockHttp.get.mockImplementation(
      connectGetMock({
        connectionResponses: [
          of({
            state: 'ready',
            ws_url: 'wss://api.example.com/p/t3/ws?t=tok-A',
            token: 'tok-A',
            expires_at: 0,
          }),
          of({
            state: 'ready',
            ws_url: 'wss://api.example.com/p/t3/ws?t=tok-B',
            token: 'tok-B',
            expires_at: 0,
          }),
        ],
      }),
    );

    await ctx.service.connect('t3');

    // The first WS should be at the first ws_url.
    expect(ctx.wsInstances).toHaveLength(1);
    expect(ctx.wsInstances[0].url).toBe('wss://api.example.com/p/t3/ws?t=tok-A');

    // Fire a 4401 close on the first WS.
    ctx.wsInstances[0].onclose?.({ code: 4401, reason: 'token expired' } as CloseEvent);

    // Let microtasks flush so the re-fetch + WS reopen happens.
    await new Promise((r) => setTimeout(r, 0));
    await new Promise((r) => setTimeout(r, 0));

    // /connection was called again (twice total: initial + post-4401 refresh).
    const connCalls = ctx.mockHttp.get.mock.calls.filter((c: any) =>
      String(c[0]).endsWith('/api/sessions/t3/connection'),
    );
    expect(connCalls.length).toBeGreaterThanOrEqual(2);

    // A new WS was opened at the refreshed ws_url.
    expect(ctx.wsInstances.length).toBeGreaterThanOrEqual(2);
    const secondWs = ctx.wsInstances[ctx.wsInstances.length - 1];
    expect(secondWs.url).toBe('wss://api.example.com/p/t3/ws?t=tok-B');
  });

  it('latches a typed binding refusal during token refresh and yields to exact terminal state', async () => {
    const ctx = createService();
    ctx.mockHttp.get.mockImplementation(
      connectGetMock({
        connectionResponses: [
          of({
            state: 'ready',
            control_socket: 'websocket',
            ws_url: 'wss://api.example.com/p/token-binding/ws?t=old',
            token: 'old',
            expires_at: 0,
            pinned_runtime_generation_contract: 1,
            session_runtime_generation: SESSION_RUNTIME_GENERATION,
          }),
          throwError(() => ({
            status: 409,
            error: {
              detail: {
                code: 'session_binding_invalid',
                pinned_runtime_generation_contract: 1,
                session_runtime_generation: SESSION_RUNTIME_GENERATION,
              },
            },
          })),
        ],
      }),
    );

    await ctx.service.connect('token-binding');
    const es = ctx.sseInstances[0];
    fireSseOpen(es);
    ctx.wsInstances[0].onclose?.({ code: 4401, reason: 'token expired' } as CloseEvent);
    await flushMicrotasks(20);

    expect(ctx.wsInstances).toHaveLength(1);
    expect(ctx.service.connectionState()).toBe('error');
    expect(ctx.service.error()).toBe('errors.sessions.bindingInvalid');
    expect(ctx.service.threadStatus()).toBe('active');

    // Exact lifecycle authority supersedes a transport refusal. The ended
    // review plane must not remain covered by stale binding-error copy.
    fireSseMessage(
      es,
      {
        method: 'session.ended',
        params: { session_runtime_generation: SESSION_RUNTIME_GENERATION },
      },
      '12:1',
    );

    expect(ctx.service.threadStatus()).toBe('ended');
    expect(ctx.service.connectionState()).toBe('disconnected');
    expect(ctx.service.error()).toBeNull();
    expect(es.close).not.toHaveBeenCalled();
  });
});

describe('PersistentChatService — disconnect()', () => {
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

  it('closes both SSE and control WS, resets signals', async () => {
    const ctx = createService();
    ctx.mockHttp.get.mockImplementation(activeSessionGet);
    await ctx.service.connect('thread-d');
    fireSseOpen(ctx.sseInstances[0]);

    ctx.service.disconnect();

    expect(ctx.sseInstances[0].close).toHaveBeenCalled();
    expect(ctx.wsInstances[0].close).toHaveBeenCalledWith(1000);
    expect(ctx.service.connectionState()).toBe('disconnected');
    expect(ctx.service.sessionReady()).toBe(false);
  });
});

describe('PersistentChatService — endSession()', () => {
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

  it('renders soft End as ending, retires control, and preserves the SSE review plane', async () => {
    const ctx = createService();
    ctx.mockHttp.get.mockImplementation(activeSessionGet);
    ctx.mockHttp.delete.mockReturnValue(of({ status: 'ending', retirement_disposition: 'ended' }));
    await ctx.service.connect('thread-e');
    fireSseOpen(ctx.sseInstances[0]);

    await ctx.service.endSession();

    const deleteCalls = ctx.mockHttp.delete.mock.calls;
    expect(deleteCalls.length).toBe(1);
    expect(deleteCalls[0][0]).toContain('/persistent/threads/thread-e');
    expect(deleteCalls[0][0]).not.toContain('permanent=true');
    expect(ctx.sseInstances[0].close).not.toHaveBeenCalled();
    expect(ctx.wsInstances[0].close).toHaveBeenCalledWith(1000);
    expect(ctx.service.connectionState()).toBe('disconnected');
    expect(ctx.service.sessionReady()).toBe(false);
    expect(ctx.service.threadStatus()).toBe('ending');
    expect(ctx.service.endedAt()).toBeNull();
  });

  it('converges a terminal frame before DELETE completion without losing review or buffered text', async () => {
    vi.useFakeTimers();
    try {
      const ctx = createService();
      ctx.mockHttp.get.mockImplementation(activeSessionGet);
      const deletion = new Subject<any>();
      ctx.mockHttp.delete.mockReturnValue(deletion);
      await ctx.service.connect('thread-end-race');
      fireSseOpen(ctx.sseInstances[0]);
      ctx.service.cloudChangesCount.set(3);
      ctx.service.cloudDiffPanelOpen.set(true);

      fireSseMessage(
        ctx.sseInstances[0],
        { method: 'turn.started', params: { turn_id: 9 } },
        '5:1',
      );
      fireSseMessage(
        ctx.sseInstances[0],
        { method: 'token', params: { content: 'kept before end' } },
        '5:2',
      );
      const ending = ctx.service.endSession();
      await Promise.resolve();

      fireSseMessage(
        ctx.sseInstances[0],
        {
          method: 'session.ended',
          params: { session_runtime_generation: SESSION_RUNTIME_GENERATION },
        },
        '5:3',
      );
      deletion.next({ status: 'ending', retirement_disposition: 'ended' });
      deletion.complete();
      await ending;
      await vi.advanceTimersByTimeAsync(500);

      const assistant = ctx.service.turns().find(isAssistantTurn) as AssistantTurn;
      expect(assistant.events.map((event) => (event as TextEvent).content).join('')).toContain(
        'kept before end',
      );
      expect(assistant.status).not.toBe('streaming');
      expect(ctx.service.conversation().activeAssistantTurnId).toBeNull();
      expect(ctx.service.isWaitingForInput()).toBe(false);
      expect((ctx.service as any).pendingTurnCount()).toBe(0);
      expect(ctx.service.threadStatus()).toBe('ended');
      expect(ctx.sseInstances[0].close).not.toHaveBeenCalled();
      expect(ctx.service.cloudChangesCount()).toBe(3);
      expect(ctx.service.cloudDiffPanelOpen()).toBe(true);
    } finally {
      vi.useRealTimers();
    }
  });

  it('preserves control, SSE, and review when DELETE fails ambiguously', async () => {
    const ctx = createService();
    ctx.mockHttp.get.mockImplementation(activeSessionGet);
    await ctx.service.connect('thread-f');
    fireSseOpen(ctx.sseInstances[0]);
    ctx.service.cloudChangesCount.set(2);
    ctx.service.cloudDiffPanelOpen.set(true);

    ctx.mockHttp.delete.mockImplementation(() => throwError(() => new Error('boom')));

    await expect(ctx.service.endSession()).rejects.toThrow('boom');
    expect(ctx.service.connectionState()).toBe('connected');
    expect(ctx.sseInstances[0].close).not.toHaveBeenCalled();
    expect(ctx.wsInstances[0].close).not.toHaveBeenCalled();
    expect(ctx.service.cloudChangesCount()).toBe(2);
    expect(ctx.service.cloudDiffPanelOpen()).toBe(true);
  });

  it('prompts for force only on the exact typed turn_in_flight 409', async () => {
    const confirmSpy = vi.spyOn(globalThis, 'confirm').mockReturnValue(true);
    try {
      const generic = createService();
      generic.mockHttp.get.mockImplementation(activeSessionGet);
      await generic.service.connect('thread-generic-conflict');
      generic.mockHttp.delete.mockReturnValue(
        throwError(() => ({
          status: 409,
          error: { detail: { code: 'pinned_retirement_conflict' } },
        })),
      );

      await expect(generic.service.endSession()).rejects.toMatchObject({ status: 409 });
      expect(confirmSpy).not.toHaveBeenCalled();
      expect(generic.service.threadStatus()).toBe('active');
      expect(generic.wsInstances[0].close).not.toHaveBeenCalled();

      const guarded = createService();
      guarded.mockHttp.get.mockImplementation(activeSessionGet);
      await guarded.service.connect('thread-turn-in-flight');
      guarded.mockHttp.delete
        .mockReturnValueOnce(
          throwError(() => ({
            status: 409,
            error: { detail: { code: 'turn_in_flight' } },
          })),
        )
        .mockReturnValueOnce(of({ status: 'ending', retirement_disposition: 'ended' }));

      await guarded.service.endSession();
      expect(confirmSpy).toHaveBeenCalledTimes(1);
      expect(guarded.mockHttp.delete.mock.calls[1][0]).toContain('force=true');
      expect(guarded.service.threadStatus()).toBe('ending');
    } finally {
      confirmSpy.mockRestore();
    }
  });

  it('reconstructs an SSE-only ending view from the safe pending boolean', async () => {
    const ctx = createService();
    ctx.mockHttp.get.mockImplementation(() =>
      of({
        status: 'active',
        runtime_retirement_pending: true,
        retirement_disposition: 'ended',
        ended_at: null,
        total_turns: 0,
        messages: [],
        total: 0,
        thread_id: 'thread-reload-ending',
        permission_mode: 'supervised',
        narration_mode: 'auto',
        turn_count: 0,
        turn_in_flight: false,
        message_count: 0,
        running_tool: null,
        pending_permissions: [],
        event_cursor: { epoch: 1, seq: 0 },
        replay_cursor: { epoch: 1, seq: 0 },
        snapshot_source: 'durable_journal',
      }),
    );

    await ctx.service.connect('thread-reload-ending');

    expect(ctx.service.threadStatus()).toBe('ending');
    expect(ctx.service.retirementDisposition()).toBe('ended');
    expect(ctx.service.endedAt()).toBeNull();
    expect(ctx.sseInstances).toHaveLength(1);
    expect(ctx.wsInstances).toHaveLength(0);
    expect(
      ctx.mockHttp.get.mock.calls.some((call: any[]) =>
        String(call[0]).endsWith('/api/sessions/thread-reload-ending/connection'),
      ),
    ).toBe(false);
    expect(
      ctx.mockHttp.post.mock.calls.some((call: any[]) =>
        String(call[0]).endsWith('/sessions/thread-reload-ending/prepare'),
      ),
    ).toBe(false);
  });

  it('does not queue input while retirement is pending', async () => {
    const ctx = createService();
    ctx.service.threadId.set('thread-ending-input');
    ctx.service.threadStatus.set('ending');

    await expect(ctx.service.sendMessage('must not cross retirement')).resolves.toBe(false);
    expect(ctx.service.outbox()).toEqual([]);
    expect(ctx.mockHttp.post).not.toHaveBeenCalled();
  });

  it('skips DELETE when no thread is connected', async () => {
    const ctx = createService();
    await ctx.service.endSession();
    expect(ctx.mockHttp.delete).not.toHaveBeenCalled();
    expect(ctx.service.connectionState()).toBe('disconnected');
  });
});

describe('PersistentChatService — attachments', () => {
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

  it('addAttachments appends, removeAttachment drops by id, clearAttachments empties', () => {
    const { service } = createService();
    const a: any = { id: '1', file: {} as File, name: 'a.png' };
    const b: any = { id: '2', file: {} as File, name: 'b.png' };
    service.addAttachments([a, b]);
    expect(service.pendingAttachments()).toHaveLength(2);
    service.removeAttachment('1');
    expect(service.pendingAttachments()).toEqual([b]);
    service.clearAttachments();
    expect(service.pendingAttachments()).toEqual([]);
  });
});

describe('PersistentChatService — interrupt self-healing', () => {
  let originalEs: any;
  let originalWs: any;

  beforeEach(() => {
    originalEs = (globalThis as any).EventSource;
    originalWs = (globalThis as any).WebSocket;
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
    (globalThis as any).EventSource = originalEs;
    (globalThis as any).WebSocket = originalWs;
    vi.clearAllMocks();
  });

  // Connect, open the SSE, and start a turn so isStreaming() is true and
  // "Stopping…" is meaningful.
  async function setupStreaming() {
    const ctx = createService();
    ctx.mockHttp.get.mockImplementation(() =>
      of({ status: 'active', total_turns: 0, messages: [], total: 0 }),
    );
    await ctx.service.connect('thread-int');
    await vi.advanceTimersByTimeAsync(0); // drain _openSse microtasks
    const es = ctx.sseInstances[0];
    fireSseOpen(es);
    fireSseMessage(es, { method: 'turn.started', params: { turn_id: 1 } }, '1:1');
    return { ...ctx, es };
  }

  it('forces a reconnect if the interrupt ack never arrives', async () => {
    const { service } = await setupStreaming();
    const reconnectSpy = vi.spyOn(service, 'reconnectNow').mockImplementation(() => {});

    await service.interrupt();
    expect(service.isInterrupting()).toBe(true);
    expect(reconnectSpy).not.toHaveBeenCalled();

    // No interrupt.ack / turn.completed arrives — fallback fires at ~8s and
    // forces a replay-from-cursor reconnect to re-sync.
    await vi.advanceTimersByTimeAsync(8_001);

    expect(reconnectSpy).toHaveBeenCalledTimes(1);
  });

  it('does not reconnect when the turn boundary arrives in time', async () => {
    const { service, es } = await setupStreaming();
    const reconnectSpy = vi.spyOn(service, 'reconnectNow').mockImplementation(() => {});

    await service.interrupt();
    // turn.completed lands promptly and clears "Stopping…".
    fireSseMessage(es, { method: 'turn.completed', params: { turn_id: 1 } }, '1:9');
    expect(service.isInterrupting()).toBe(false);

    await vi.advanceTimersByTimeAsync(8_001);
    expect(reconnectSpy).not.toHaveBeenCalled();
  });

  it('closes the exact turn on a reaper turn.interrupted frame', async () => {
    const { service, es, mockHttp } = await setupStreaming();
    fireSseMessage(
      es,
      {
        method: 'tool.started',
        params: { id: 'tool-1', tool: 'shell', args: {} },
      },
      '1:2',
    );
    mockHttp.post.mockClear();
    await service.interrupt();

    fireSseMessage(
      es,
      {
        method: 'turn.interrupted',
        params: { target_turn_id: 1, reason: 'lease_expired' },
      },
      '2:1',
    );

    expect(service.isStreaming()).toBe(false);
    expect(service.currentTurnId()).toBeNull();
    expect(service.runningTool()).toBeNull();
    expect(service.isInterrupting()).toBe(false);
    expect(service.pendingTurnCount()).toBe(0);
    expect(service.turns().find((turn) => turn.id === '1')).toMatchObject({
      status: 'interrupted',
    });
  });

  it('renders turn.parked as a terminal edge without a successor turn', async () => {
    const { service, es } = await setupStreaming();

    const frame = {
      method: 'turn.parked',
      params: { target_turn_id: 1, reason: 'lease_expired' },
    };
    fireSseMessage(es, frame, '2:1');
    fireSseMessage(es, frame, '2:2');

    expect(service.isStreaming()).toBe(false);
    expect(service.currentTurnId()).toBeNull();
    expect(service.pendingTurnCount()).toBe(0);
    expect(service.turns().find((turn) => turn.id === '1')).toMatchObject({
      status: 'interrupted',
    });
    expect(
      service.turns().filter((turn) => isSystemTurn(turn) && turn.id === 'turn-parked-1'),
    ).toHaveLength(1);
  });

  it('does not let an old reaper terminal frame close a newer turn', async () => {
    const { service, es } = await setupStreaming();
    fireSseMessage(es, { method: 'turn.completed', params: { turn_id: 1 } }, '1:2');
    fireSseMessage(es, { method: 'turn.started', params: { turn_id: 2 } }, '2:1');

    fireSseMessage(
      es,
      {
        method: 'turn.interrupted',
        params: { target_turn_id: 1, reason: 'lease_expired' },
      },
      '2:2',
    );

    expect(service.currentTurnId()).toBe(2);
    expect(service.isStreaming()).toBe(true);
  });

  it.each(['interrupt.ack', 'turn.interrupted'])(
    'does not let a covered old %s promote and close a recovered placeholder',
    async (method) => {
      const { service } = createService();
      (service as any)._handleEvent({
        method: 'token',
        params: { content: 'new recovered response' },
      });
      (service as any)._flushDeltas();
      const recoveredId = service.conversation().activeAssistantTurnId;
      expect(recoveredId).toMatch(/^recovered:/);

      (service as any)._handleEvent(
        {
          method,
          params: {
            target_turn_id: 1,
            client_request_id: crypto.randomUUID(),
            applied: true,
            mode: 'hard',
          },
        },
        true,
        true,
      );

      expect(service.conversation().activeAssistantTurnId).toBe(recoveredId);
      expect(service.isStreaming()).toBe(true);
    },
  );

  it('clears a stuck isInterrupting when the connection drops (invariant)', async () => {
    const { service } = await setupStreaming();

    await service.interrupt();
    expect(service.isInterrupting()).toBe(true);
    expect(service.isStreaming()).toBe(true);

    // disconnect() closes the active turn but does not explicitly reset
    // isInterrupting — the invariant effect (not streaming ⇒ not stopping)
    // is what must clear it.
    service.disconnect();
    TestBed.tick();

    expect(service.isStreaming()).toBe(false);
    expect(service.isInterrupting()).toBe(false);
  });

  it('retries an ambiguous admission with the same UUID and target turn', async () => {
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => undefined);
    const { service, mockHttp } = await setupStreaming();
    mockHttp.post.mockClear();
    mockHttp.post
      .mockReturnValueOnce(throwError(() => ({ status: 0 })))
      .mockReturnValueOnce(of({ accepted: true, duplicate: true }));

    await service.interrupt();
    const firstBody = mockHttp.post.mock.calls[0][1];
    await vi.advanceTimersByTimeAsync(250);

    expect(mockHttp.post).toHaveBeenCalledTimes(2);
    expect(mockHttp.post.mock.calls[1][1]).toEqual(firstBody);
    expect(firstBody.target_turn_id).toBe(1);
    expect(service.isInterrupting()).toBe(true);
    warn.mockRestore();
  });

  it('accepts a correlated ack that beats the HTTP admission response', async () => {
    const { service, es, mockHttp } = await setupStreaming();
    const response = new Subject<Record<string, unknown>>();
    mockHttp.post.mockClear();
    mockHttp.post.mockReturnValue(response.asObservable());

    await service.interrupt();
    const requestId = mockHttp.post.mock.calls[0][1].client_request_id;
    fireSseMessage(
      es,
      {
        method: 'interrupt.ack',
        params: {
          client_request_id: requestId,
          target_turn_id: 1,
          mode: 'hard',
        },
      },
      '1:2',
    );

    expect(service.isStreaming()).toBe(false);
    expect(service.isInterrupting()).toBe(false);
    expect((service as any).pendingInterruptRequest).toBeNull();

    // A late HTTP result cannot resurrect the request or fallback timer.
    response.next({ accepted: true });
    response.complete();
    await vi.advanceTimersByTimeAsync(8_001);
    expect(service.isInterrupting()).toBe(false);
  });

  it('keeps an acknowledged stop visibly interrupted after turn.completed', async () => {
    const { service, es, mockHttp } = await setupStreaming();
    mockHttp.post.mockClear();
    await service.interrupt();
    const requestId = mockHttp.post.mock.calls[0][1].client_request_id;

    fireSseMessage(
      es,
      {
        method: 'interrupt.ack',
        params: {
          client_request_id: requestId,
          target_turn_id: 1,
          applied: true,
          mode: 'hard',
        },
      },
      '1:2',
    );
    fireSseMessage(es, { method: 'turn.completed', params: { turn_id: 1 } }, '1:3');

    expect(service.turns().find((turn) => turn.id === '1')).toMatchObject({
      status: 'interrupted',
    });
  });

  it('does not let an old ack close or settle a newer turn interrupt', async () => {
    const { service, es, mockHttp } = await setupStreaming();
    mockHttp.post.mockClear();

    await service.interrupt();
    const oldRequestId = mockHttp.post.mock.calls[0][1].client_request_id;
    fireSseMessage(es, { method: 'turn.completed', params: { turn_id: 1 } }, '1:2');
    fireSseMessage(es, { method: 'turn.started', params: { turn_id: 2 } }, '1:3');
    await service.interrupt();
    const newRequestId = mockHttp.post.mock.calls[1][1].client_request_id;
    expect(newRequestId).not.toBe(oldRequestId);
    expect(service.currentTurnId()).toBe(2);
    expect(service.isInterrupting()).toBe(true);

    fireSseMessage(
      es,
      {
        method: 'interrupt.ack',
        params: {
          client_request_id: oldRequestId,
          target_turn_id: 1,
          mode: 'hard',
        },
      },
      '1:4',
    );

    expect(service.currentTurnId()).toBe(2);
    expect(service.isStreaming()).toBe(true);
    expect(service.isInterrupting()).toBe(true);
    expect((service as any).pendingInterruptRequest.clientRequestId).toBe(newRequestId);

    fireSseMessage(
      es,
      {
        method: 'interrupt.ack',
        params: {
          client_request_id: newRequestId,
          target_turn_id: 2,
          mode: 'hard',
        },
      },
      '1:5',
    );
    expect(service.isStreaming()).toBe(false);
    expect(service.isInterrupting()).toBe(false);
  });

  it('does not let an old turn.completed clear a newer interrupt', async () => {
    const { service, es, mockHttp } = await setupStreaming();
    fireSseMessage(es, { method: 'turn.completed', params: { turn_id: 1 } }, '1:2');
    fireSseMessage(es, { method: 'turn.started', params: { turn_id: 2 } }, '1:3');
    mockHttp.post.mockClear();
    await service.interrupt();
    const requestId = mockHttp.post.mock.calls[0][1].client_request_id;

    fireSseMessage(es, { method: 'turn.completed', params: { turn_id: 1 } }, '1:4');

    expect(service.currentTurnId()).toBe(2);
    expect(service.isStreaming()).toBe(true);
    expect(service.isInterrupting()).toBe(true);
    expect((service as any).pendingInterruptRequest.clientRequestId).toBe(requestId);
  });

  it('ignores an uncorrelated legacy ack when no local target is pending', async () => {
    const { service, es } = await setupStreaming();

    fireSseMessage(es, { method: 'interrupt.ack', params: { mode: 'hard' } }, '1:2');

    expect(service.currentTurnId()).toBe(1);
    expect(service.isStreaming()).toBe(true);
  });

  it('settles a rejected exact ack without closing the target turn', async () => {
    const { service, es, mockHttp } = await setupStreaming();
    mockHttp.post.mockClear();
    await service.interrupt();
    const requestId = mockHttp.post.mock.calls[0][1].client_request_id;

    fireSseMessage(
      es,
      {
        method: 'interrupt.ack',
        params: {
          client_request_id: requestId,
          target_turn_id: 1,
          applied: false,
          error_code: 'target_turn_not_active',
        },
      },
      '1:2',
    );

    expect(service.currentTurnId()).toBe(1);
    expect(service.isStreaming()).toBe(true);
    expect(service.isInterrupting()).toBe(false);
  });

  it("does not let another tab's rejection settle this tab's request", async () => {
    const { service, es, mockHttp } = await setupStreaming();
    mockHttp.post.mockClear();
    await service.interrupt();
    const requestId = mockHttp.post.mock.calls[0][1].client_request_id;

    fireSseMessage(
      es,
      {
        method: 'interrupt.ack',
        params: {
          client_request_id: crypto.randomUUID(),
          target_turn_id: 1,
          applied: false,
          error_code: 'target_turn_not_active',
        },
      },
      '1:2',
    );

    expect(service.currentTurnId()).toBe(1);
    expect(service.isStreaming()).toBe(true);
    expect(service.isInterrupting()).toBe(true);
    expect((service as any).pendingInterruptRequest.clientRequestId).toBe(requestId);
  });
});

describe('PersistentChatService — compaction progress frames', () => {
  afterEach(() => vi.clearAllMocks());

  async function setup() {
    const ctx = createService();
    ctx.mockHttp.get.mockImplementation(() =>
      of({ status: 'active', total_turns: 0, messages: [], total: 0 }),
    );
    await ctx.service.connect('thread-X');
    const es = ctx.sseInstances[0];
    fireSseOpen(es);
    return { ...ctx, es };
  }

  it('builds compaction state from started + progress frames', async () => {
    const { service, es } = await setup();
    fireSseMessage(
      es,
      {
        method: 'compaction.started',
        params: {
          trigger: 'auto',
          total_tokens: 951_682,
          ctx_used_tokens: 951_682,
          ctx_limit_tokens: 1_047_576,
          ctx_used_pct: 91,
          aux_limit_tokens: 131_072,
          n_passes: 10,
          plan: [{ pass: 1, first_msg: 1, last_msg: 112, tokens: 98_000 }],
        },
      },
      '1:1',
    );
    expect(service.compaction()).not.toBeNull();
    expect(service.compaction()!.nPasses).toBe(10);
    expect(service.compaction()!.currentPass).toBe(0); // planning

    fireSseMessage(
      es,
      {
        method: 'compaction.progress',
        params: {
          pass: 4,
          n_passes: 10,
          first_msg: 113,
          last_msg: 141,
          in_tokens: 38_000,
          out_tokens: 2_500,
          stage: 'summarizing',
          attempt: 1,
        },
      },
      '1:2',
    );
    const comp = service.compaction()!;
    expect(comp.currentPass).toBe(4);
    expect(comp.firstMsg).toBe(113);
    expect(comp.outTokens).toBe(2_500);
    // started-frame fields survive progress updates
    expect(comp.ctxUsedPct).toBe(91);
  });

  it('synthesizes state from a replayed progress frame without started (reload mid-fold)', async () => {
    const { service, es } = await setup();
    fireSseMessage(
      es,
      {
        method: 'compaction.progress',
        params: {
          pass: 7,
          n_passes: 10,
          first_msg: 500,
          last_msg: 540,
          in_tokens: 40_000,
          attempt: 2,
        },
      },
      '1:1',
    );
    const comp = service.compaction()!;
    expect(comp.currentPass).toBe(7);
    expect(comp.nPasses).toBe(10);
    expect(comp.attempt).toBe(2);
  });

  it('clears compaction state on context.compacted (success path)', async () => {
    const { service, es } = await setup();
    fireSseMessage(es, { method: 'compaction.started', params: { n_passes: 2 } }, '1:1');
    expect(service.compaction()).not.toBeNull();
    fireSseMessage(
      es,
      {
        method: 'context.compacted',
        params: { before: 100, after: 12, trigger: 'auto', summary: 'did things', turn: 3 },
      },
      '1:2',
    );
    expect(service.compaction()).toBeNull();
  });

  it('clears compaction state and surfaces a system line on compaction.failed', async () => {
    const { service, es } = await setup();
    fireSseMessage(es, { method: 'compaction.started', params: { n_passes: 3 } }, '1:1');
    fireSseMessage(
      es,
      {
        method: 'compaction.failed',
        params: { reason: 'aux_unavailable', pass: 2, n_passes: 3, kept_messages: true },
      },
      '1:2',
    );
    expect(service.compaction()).toBeNull();
    const sys = service.turns().filter((t: any) => t.kind === 'system');
    expect(sys.some((t: any) => String(t.content).includes('aux_unavailable'))).toBe(true);
  });

  it('clears stale compaction state when the turn ends', async () => {
    const { service, es } = await setup();
    fireSseMessage(es, { method: 'turn.started', params: { turn_id: 1 } }, '1:1');
    fireSseMessage(es, { method: 'compaction.started', params: { n_passes: 5 } }, '1:2');
    fireSseMessage(es, { method: 'turn.completed', params: { turn_id: 1 } }, '1:3');
    expect(service.compaction()).toBeNull();
  });

  it('clears the progress block on compaction.skipped', async () => {
    // Engine ran but the size guard rejected the summary — the journaled
    // terminal frame must clear the block (incl. on SSE replay).
    const { service, es } = await setup();
    fireSseMessage(
      es,
      { method: 'compaction.started', params: { n_passes: 1, trigger: 'manual' } },
      '1:1',
    );
    expect(service.compaction()).not.toBeNull();
    fireSseMessage(
      es,
      {
        method: 'compaction.skipped',
        params: { trigger: 'manual', reason: 'summary_not_smaller' },
      },
      '1:2',
    );
    expect(service.compaction()).toBeNull();
  });

  it('uses the trigger carried by a replayed progress frame (no started)', async () => {
    const { service, es } = await setup();
    fireSseMessage(
      es,
      {
        method: 'compaction.progress',
        params: {
          trigger: 'manual',
          pass: 1,
          n_passes: 1,
          first_msg: 1,
          last_msg: 34,
          in_tokens: 1176,
          out_tokens: null,
          attempt: 1,
          stage: 'summarizing',
        },
      },
      '1:1',
    );
    expect(service.compaction()!.trigger).toBe('manual');
  });

  it('renders a summary-less context.compacted as a system line, not a banner', async () => {
    // Manual /compact no-op: the agent answers with summary=null and
    // persists no row — the UI must not add an (empty) banner.
    const { service, es } = await setup();
    fireSseMessage(
      es,
      { method: 'compaction.started', params: { n_passes: 1, trigger: 'manual' } },
      '1:1',
    );
    fireSseMessage(
      es,
      {
        method: 'context.compacted',
        params: { before: 10, after: 10, trigger: 'manual', summary: null, turn: 2 },
      },
      '1:2',
    );
    expect(service.compaction()).toBeNull();
    expect(service.turns().some((t: any) => t.kind === 'compaction')).toBe(false);
    const sys = service.turns().filter((t: any) => t.kind === 'system');
    expect(sys.some((t: any) => String(t.content).includes('Nothing to compact'))).toBe(true);
  });
});

describe('PersistentChatService — workspace/VM upgrade notices (Q7/Q8)', () => {
  afterEach(() => vi.clearAllMocks());

  async function setup() {
    const ctx = createService();
    ctx.mockHttp.get.mockImplementation(() =>
      of({ status: 'active', total_turns: 0, messages: [], total: 0 }),
    );
    await ctx.service.connect('thread-X');
    fireSseOpen(ctx.sseInstances[0]);
    return { ...ctx, es: ctx.sseInstances[0] };
  }

  function sysLines(service: any): string[] {
    return service
      .turns()
      .filter((t: any) => t.kind === 'system')
      .map((t: any) => String(t.content));
  }

  it('vm_upgrade.needed points at the real /upgrade-workspace vm command, not a phantom button', async () => {
    const { service, es } = await setup();
    fireSseMessage(
      es,
      {
        method: 'vm_upgrade.needed',
        params: { reason: 'sudo detected', command: 'sudo apt-get install foo' },
      },
      '1:1',
    );
    const lines = sysLines(service);
    expect(lines.some((l) => l.includes('/upgrade-workspace vm'))).toBe(true);
    // The misleading "upgrade button or send /upgrade" wording is gone.
    expect(lines.some((l) => l.includes('upgrade button'))).toBe(false);
    // The triggering command is surfaced for context.
    expect(lines.some((l) => l.includes('sudo apt-get install foo'))).toBe(true);
  });

  it('workspace_upgrade.progress surfaces a live heartbeat during a slow provision', async () => {
    const { service, es } = await setup();
    fireSseMessage(
      es,
      {
        method: 'workspace_upgrade.progress',
        params: { target_tier: 'vm', elapsed_s: 120, timeout_s: 900 },
      },
      '1:1',
    );
    expect(sysLines(service).some((l) => l.includes('120s'))).toBe(true);
  });

  it('workspace_upgrade.complete mentions sudo for a vm target only', async () => {
    const { service, es } = await setup();
    fireSseMessage(
      es,
      {
        method: 'workspace_upgrade.complete',
        params: { target_tier: 'vm', seeded_files: 3 },
      },
      '1:1',
    );
    const lines = sysLines(service);
    expect(lines.some((l) => l.toLowerCase().includes('sudo'))).toBe(true);
    expect(lines.some((l) => l.includes('3 file(s) carried over'))).toBe(true);
  });

  it('workspace_upgrade.complete for a sandbox target does not mention sudo', async () => {
    const { service, es } = await setup();
    fireSseMessage(
      es,
      {
        method: 'workspace_upgrade.complete',
        params: { target_tier: 'sandbox' },
      },
      '1:1',
    );
    expect(sysLines(service).some((l) => l.toLowerCase().includes('sudo'))).toBe(false);
  });
});

describe('PersistentChatService — control frame delivery across a reconnect', () => {
  // readyState numerals: the mock ctor only publishes OPEN/CONNECTING.
  const CLOSED = 3;

  /** Queue a frame with no live socket, then reconnect and drain. */
  function reconnect(service: any, wsInstances: any[], threadId: string) {
    service.intentionalClose = false;
    service._installControlWs(threadId, 'ws://reconnected');
    const ws = wsInstances.at(-1);
    ws.onopen();
    return ws;
  }

  function framesOn(ws: any) {
    return ws.send.mock.calls.map((c: any) => JSON.parse(c[0]));
  }

  it('delivers an upgrade click issued while the socket is CLOSED', () => {
    const { service, wsInstances } = createService();
    const dead = createMockWs();
    dead.readyState = CLOSED;
    service.threadId.set('thread-cx');
    (service as any).controlWs = dead;

    // The real user path: card/pane click while the socket happens to be
    // down. This used to vanish — the frame was hung on `dead`, which never
    // fires 'open' again, so the spinner span forever.
    service.upgradeWorkspace('sandbox');
    expect(dead.send).not.toHaveBeenCalled();

    const ws = reconnect(service, wsInstances, 'thread-cx');
    expect(framesOn(ws)).toContainEqual({
      method: 'upgrade-to-workspace',
      target_tier: 'sandbox',
    });
  });

  it('delivers a command issued when there is no socket at all', () => {
    const { service, wsInstances } = createService();
    service.threadId.set('thread-cx');
    (service as any).controlWs = null;

    // The old code hit `if (!ws) return` and dropped this outright.
    (service as any)._sendControl({ method: 'approve' });

    const ws = reconnect(service, wsInstances, 'thread-cx');
    expect(framesOn(ws)).toContainEqual({ method: 'approve' });
  });

  it('sends straight out on an open socket without queueing', () => {
    const { service } = createService();
    const live = createMockWs();
    live.readyState = WebSocket.OPEN;
    service.threadId.set('thread-cx');
    (service as any).controlWs = live;

    (service as any)._sendControl({ method: 'approve' });

    expect(framesOn(live)).toEqual([{ method: 'approve' }]);
    expect((service as any).controlOutbox).toHaveLength(0);
  });

  it('preserves the order commands were issued in', () => {
    const { service, wsInstances } = createService();
    service.threadId.set('thread-cx');
    (service as any).controlWs = null;

    (service as any)._sendControl({ method: 'first' });
    (service as any)._sendControl({ method: 'second' });

    const ws = reconnect(service, wsInstances, 'thread-cx');
    expect(framesOn(ws)).toEqual([{ method: 'first' }, { method: 'second' }]);
  });

  it('drops a stale command tagged for another thread rather than misfiring it', () => {
    const { service, wsInstances } = createService();
    service.threadId.set('thread-b');
    (service as any).controlWs = null;
    // A leftover from thread-a reaching thread-b's socket. disconnect()
    // normally clears these; the tag is the backstop if one survives, since
    // replaying "approve" here would act on whatever thread-b has pending.
    (service as any).controlOutbox = [
      { threadId: 'thread-a', frame: JSON.stringify({ method: 'approve' }) },
    ];

    (service as any).intentionalClose = false;
    service._installControlWs('thread-b', 'ws://reconnected');
    const ws = wsInstances.at(-1);
    service.threadId.set('thread-b');
    ws.onopen();
    expect(ws.send).not.toHaveBeenCalled();
    expect((service as any).controlOutbox).toHaveLength(0);
  });

  it('does not drain over a socket opened for a different thread', () => {
    const { service, wsInstances } = createService();
    service.threadId.set('thread-a');
    (service as any).controlWs = null;
    (service as any)._sendControl({ method: 'approve' });

    // onopen's own ownership guard rejects the mismatched socket before the
    // drain, so the command stays queued for thread-a's real socket.
    (service as any).intentionalClose = false;
    service._installControlWs('thread-a', 'ws://reconnected');
    const ws = wsInstances.at(-1);
    service.threadId.set('thread-b');
    ws.onopen();
    expect(ws.send).not.toHaveBeenCalled();
    expect((service as any).controlOutbox).toHaveLength(1);
  });

  it('clears queued commands on disconnect so they cannot replay later', () => {
    const { service } = createService();
    service.threadId.set('thread-cx');
    (service as any).controlWs = null;
    (service as any)._sendControl({ method: 'approve' });
    expect((service as any).controlOutbox).toHaveLength(1);

    service.disconnect();
    expect((service as any).controlOutbox).toHaveLength(0);
  });

  it('caps the queue so a wedged socket cannot grow it without bound', () => {
    const { service } = createService();
    service.threadId.set('thread-cx');
    (service as any).controlWs = null;

    for (let i = 0; i < 40; i++) (service as any)._sendControl({ method: `m${i}` });

    const outbox = (service as any).controlOutbox;
    expect(outbox).toHaveLength(32);
    // Oldest dropped, newest kept.
    expect(JSON.parse(outbox[outbox.length - 1].frame)).toEqual({ method: 'm39' });
  });

  it('re-queues a frame when the socket dies mid-write', () => {
    const { service, wsInstances } = createService();
    const live = createMockWs();
    live.readyState = WebSocket.OPEN;
    live.send = vi.fn(() => {
      throw new Error('INVALID_STATE_ERR');
    });
    service.threadId.set('thread-cx');
    (service as any).controlWs = live;

    (service as any)._sendControl({ method: 'approve' });

    const ws = reconnect(service, wsInstances, 'thread-cx');
    expect(framesOn(ws)).toContainEqual({ method: 'approve' });
  });
});

describe('PersistentChatService — inline workspace upgrade offer', () => {
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

  /** Connected + agent-ready, so sendMessage POSTs instead of queueing. */
  async function readySession() {
    const ctx = createService();
    ctx.mockHttp.get.mockImplementation(activeSessionGet);
    await ctx.service.connect('thread-wo');
    fireSseOpen(ctx.sseInstances[0]);
    fireSseMessage(ctx.sseInstances[0], { method: 'ready', params: {} }, '1:1');
    ctx.wsInstances[0].send.mockClear();
    ctx.mockHttp.post.mockClear();
    return { ...ctx, es: ctx.sseInstances[0] };
  }

  function offer(es: any, params: Record<string, unknown>, seq = '1:2') {
    fireSseMessage(es, { method: 'workspace_upgrade.needed', params }, seq);
  }

  function inputCalls(ctx: any) {
    return ctx.mockHttp.post.mock.calls.filter((c: any) =>
      String(c[0]).endsWith('/persistent/threads/thread-wo/input'),
    );
  }

  function sentControl(ctx: any) {
    return ctx.wsInstances[0].send.mock.calls.map((c: any) => JSON.parse(c[0]));
  }

  it('workspace_upgrade.needed raises the offer and still records a system line', async () => {
    const ctx = await readySession();
    offer(ctx.es, { target_tier: 'sandbox', reason: 'need to run pytest' });
    expect(ctx.service.pendingWorkspaceOffer()).toEqual({
      tier: 'sandbox',
      reason: 'need to run pytest',
    });
    const lines = ctx.service
      .turns()
      .filter((t: any) => t.kind === 'system')
      .map((t: any) => String(t.content));
    expect(lines.some((l: string) => l.includes('need to run pytest'))).toBe(true);
    // The card is the verb now — the line must not send users to the pane.
    expect(lines.some((l: string) => l.includes('session settings'))).toBe(false);
  });

  it('defaults the offered tier to sandbox when the server omits it', async () => {
    const ctx = await readySession();
    offer(ctx.es, { reason: 'shell needed' });
    expect(ctx.service.pendingWorkspaceOffer()?.tier).toBe('sandbox');
  });

  it('a second offer replaces the first rather than accumulating', async () => {
    const ctx = await readySession();
    offer(ctx.es, { target_tier: 'sandbox', reason: 'first' });
    offer(ctx.es, { target_tier: 'sandbox', reason: 'second' }, '1:3');
    expect(ctx.service.pendingWorkspaceOffer()?.reason).toBe('second');
  });

  it('accepting clears the offer and sends the upgrade control message', async () => {
    const ctx = await readySession();
    offer(ctx.es, { target_tier: 'sandbox', reason: 'need a shell' });
    ctx.service.upgradeWorkspace('sandbox');
    expect(ctx.service.pendingWorkspaceOffer()).toBeNull();
    expect(ctx.service.workspaceUpgradeInProgress()).toEqual({ tier: 'sandbox' });
    expect(sentControl(ctx)).toContainEqual({
      method: 'upgrade-to-workspace',
      target_tier: 'sandbox',
    });
  });

  it('thenContinue arms the continuation; a plain upgrade disarms it', async () => {
    const ctx = await readySession();
    ctx.service.upgradeWorkspace('sandbox', { thenContinue: true });
    expect(ctx.service.continueAfterUpgrade()).toBe(true);
    // The pane and /upgrade-workspace pass no opts — they must reset it,
    // or a stale flag would resume the agent behind the user's back.
    ctx.service.upgradeWorkspace('sandbox');
    expect(ctx.service.continueAfterUpgrade()).toBe(false);
  });

  it('completing an armed upgrade sends the continuation', async () => {
    const ctx = await readySession();
    ctx.service.upgradeWorkspace('sandbox', { thenContinue: true });
    fireSseMessage(
      ctx.es,
      {
        method: 'workspace_upgrade.complete',
        params: { target_tier: 'sandbox' },
      },
      '1:2',
    );
    await Promise.resolve();
    // Transloco is mocked identity, so the raw key is the content.
    expect(inputCalls(ctx)[0]?.[1]).toEqual({
      content: 'chat.workspaceOffer.continueMessage',
      expected_conversation_revision: 0,
    });
    expect(ctx.service.continueAfterUpgrade()).toBe(false);
  });

  it('completing an unarmed upgrade sends nothing', async () => {
    const ctx = await readySession();
    ctx.service.upgradeWorkspace('sandbox');
    fireSseMessage(
      ctx.es,
      {
        method: 'workspace_upgrade.complete',
        params: { target_tier: 'sandbox' },
      },
      '1:2',
    );
    await Promise.resolve();
    expect(inputCalls(ctx)).toHaveLength(0);
  });

  it('a repeated complete does not send the continuation twice', async () => {
    const ctx = await readySession();
    ctx.service.upgradeWorkspace('sandbox', { thenContinue: true });
    const complete = () =>
      fireSseMessage(
        ctx.es,
        {
          method: 'workspace_upgrade.complete',
          params: { target_tier: 'sandbox' },
        },
        '1:2',
      );
    complete();
    await Promise.resolve();
    // The server short-circuits an already-satisfied tier straight to
    // .complete, so a second frame is reachable, not hypothetical.
    complete();
    await Promise.resolve();
    expect(inputCalls(ctx)).toHaveLength(1);
  });

  it('a user message mid-provision cancels the pending continuation', async () => {
    const ctx = await readySession();
    ctx.service.upgradeWorkspace('sandbox', { thenContinue: true });
    await ctx.service.sendMessage('actually, do Y instead');
    expect(ctx.service.continueAfterUpgrade()).toBe(false);
    fireSseMessage(
      ctx.es,
      {
        method: 'workspace_upgrade.complete',
        params: { target_tier: 'sandbox' },
      },
      '1:2',
    );
    await Promise.resolve();
    // Only the user's own message — no "continue where you left off"
    // stacked behind it.
    expect(inputCalls(ctx)).toHaveLength(1);
    expect(inputCalls(ctx)[0][1]).toEqual({
      content: 'actually, do Y instead',
      expected_conversation_revision: 0,
    });
  });

  it('a failed upgrade clears the offer and sends nothing', async () => {
    const ctx = await readySession();
    offer(ctx.es, { target_tier: 'sandbox', reason: 'need a shell' });
    ctx.service.upgradeWorkspace('sandbox', { thenContinue: true });
    fireSseMessage(
      ctx.es,
      {
        method: 'workspace_upgrade.failed',
        params: { reason: 'quota exceeded' },
      },
      '1:2',
    );
    await Promise.resolve();
    expect(ctx.service.pendingWorkspaceOffer()).toBeNull();
    expect(ctx.service.continueAfterUpgrade()).toBe(false);
    expect(ctx.service.workspaceUpgradeInProgress()).toBeNull();
    expect(inputCalls(ctx)).toHaveLength(0);
  });

  it('dismissing the offer touches only local state', async () => {
    const ctx = await readySession();
    offer(ctx.es, { target_tier: 'sandbox', reason: 'need a shell' });
    ctx.service.dismissWorkspaceOffer();
    expect(ctx.service.pendingWorkspaceOffer()).toBeNull();
    expect(sentControl(ctx)).toHaveLength(0);
    expect(inputCalls(ctx)).toHaveLength(0);
  });

  it('disconnect clears the upgrade state so it cannot bleed across threads', async () => {
    const ctx = await readySession();
    offer(ctx.es, { target_tier: 'sandbox', reason: 'need a shell' });
    ctx.service.upgradeWorkspace('sandbox', { thenContinue: true });
    ctx.service.disconnect();
    // .complete only ever arrives over the control WS disconnect() just
    // closed, so a surviving spinner would hang forever.
    expect(ctx.service.workspaceUpgradeInProgress()).toBeNull();
    expect(ctx.service.pendingWorkspaceOffer()).toBeNull();
    expect(ctx.service.continueAfterUpgrade()).toBe(false);
  });
});

describe('PersistentChatService — usage.updated telemetry', () => {
  afterEach(() => vi.clearAllMocks());

  async function setup() {
    const ctx = createService();
    ctx.mockHttp.get.mockImplementation(() =>
      of({ status: 'active', total_turns: 0, messages: [], total: 0 }),
    );
    await ctx.service.connect('thread-X');
    const es = ctx.sseInstances[0];
    fireSseOpen(es);
    return { ...ctx, es };
  }

  /** Connect-time GETs carrying a *valid* durable session-state snapshot, so
   *  the snapshot path — and the `coveredBySnapshot` cursor derived from it —
   *  is actually exercised. `setup()` above deliberately serves a malformed
   *  one, which makes every frame uncovered. */
  function snapshotGetMock(
    threadId: string,
    opts: { usage?: unknown; cursorSeq?: number; omitUsageKey?: boolean } = {},
  ) {
    const seq = opts.cursorSeq ?? 10;
    return (url: string) => {
      if (url.includes('/api/sessions/') && url.endsWith('/connection')) {
        return of({
          state: 'ready',
          control_socket: 'none',
          ws_url: null,
          token: null,
          expires_at: null,
        });
      }
      if (url.endsWith('/messages')) return of({ messages: [], total: 0 });
      if (url.endsWith('/state')) {
        const snapshot: Record<string, unknown> = {
          thread_id: threadId,
          permission_mode: 'supervised',
          narration_mode: 'auto',
          turn_count: 1,
          turn_in_flight: false,
          message_count: 2,
          model: null,
          temperature: null,
          running_tool: null,
          pending_permissions: [],
          event_cursor: { epoch: 1, seq },
          replay_cursor: { epoch: 1, seq: 0 },
          snapshot_source: 'durable_journal',
        };
        if (!opts.omitUsageKey) snapshot['usage'] = opts.usage ?? null;
        return of(snapshot);
      }
      return of({ status: 'active', total_turns: 1 });
    };
  }

  async function setupWithSnapshot(
    threadId: string,
    opts: { usage?: unknown; cursorSeq?: number; omitUsageKey?: boolean } = {},
  ) {
    const ctx = createService();
    ctx.mockHttp.get.mockImplementation(snapshotGetMock(threadId, opts));
    await ctx.service.connect(threadId);
    const es = ctx.sseInstances[0];
    fireSseOpen(es);
    return { ...ctx, es };
  }

  /** Connect, then report one turn's usage. The shape the leak came from. */
  async function connectWithUsage(threadId: string, params: Record<string, unknown>) {
    const ctx = createService();
    ctx.mockHttp.get.mockImplementation(() =>
      of({ status: 'active', total_turns: 0, messages: [], total: 0 }),
    );
    await ctx.service.connect(threadId);
    fireSseOpen(ctx.sseInstances[0]);
    fireSseMessage(ctx.sseInstances[0], { method: 'usage.updated', params }, '1:1');
    return ctx;
  }

  // ── Thread isolation ───────────────────────────────────────────────────
  // The service is a root singleton, so `usage` outlives any one session.
  // Regression net for
  // knowledge-history/done/session_usage_panel_leaks_previous_session_counters.md —
  // a brand-new session rendered the previous one's 154.6k input at Turn 0.

  it('does not render a previous session’s counters after a thread switch', async () => {
    const ctx = await connectWithUsage('thread-old', {
      turn: 4,
      input_tokens: 154_600,
      output_tokens: 118,
      reasoning_tokens: 28,
      ctx_limit_tokens: 320_000,
      compaction_threshold_tokens: 320_000,
    });
    expect(ctx.service.currentUsage()!.inputTokens).toBe(154_600);

    await ctx.service.connect('thread-new');
    fireSseOpen(ctx.sseInstances[1]);

    // Nothing to show yet on the new thread — and crucially not the old one's.
    expect(ctx.service.currentUsage()).toBeNull();
    expect(ctx.service.usage()).toBeNull();
  });

  it('refuses to render telemetry stamped with a different thread', async () => {
    const { service } = await setup();
    // The pre-fix residue, injected directly: a value some reset path missed.
    // The thread binding has to make it unrenderable on its own.
    service.usage.set({
      threadId: 'some-other-thread',
      turn: 9,
      inputTokens: 154_600,
      outputTokensTurn: 118,
      reasoningTokensTurn: 28,
      reasoningEstimated: false,
      ctxLimitTokens: 320_000,
      compactionThresholdTokens: 320_000,
    });
    expect(service.currentUsage()).toBeNull();
  });

  it('clears telemetry when leaving a session for the landing draft', async () => {
    const ctx = await connectWithUsage('thread-old', {
      turn: 1,
      input_tokens: 10_000,
      output_tokens: 40,
    });
    expect(ctx.service.currentUsage()).not.toBeNull();

    ctx.service.enterDraftSession();

    expect(ctx.service.threadId()).toBeNull();
    expect(ctx.service.usage()).toBeNull();
    // A null-threaded value must never match the draft's null threadId.
    expect(ctx.service.currentUsage()).toBeNull();
  });

  it('starts a new thread’s accumulation from zero, not the old thread’s total', async () => {
    const ctx = await connectWithUsage('thread-old', {
      turn: 1,
      input_tokens: 90_000,
      output_tokens: 400,
      ctx_limit_tokens: 200_000,
    });
    await ctx.service.connect('thread-new');
    fireSseOpen(ctx.sseInstances[1]);
    // Same turn number as the old thread, and it omits input + the limits:
    // neither the accumulators nor the sticky `?? prev` fallbacks may reach
    // across the thread boundary.
    fireSseMessage(
      ctx.sseInstances[1],
      { method: 'usage.updated', params: { turn: 1, output_tokens: 25 } },
      '1:1',
    );
    const u = ctx.service.currentUsage()!;
    expect(u.threadId).toBe('thread-new');
    expect(u.outputTokensTurn).toBe(25);
    expect(u.inputTokens).toBeNull();
    expect(u.ctxLimitTokens).toBeNull();
  });

  // ── Durable snapshot restore ───────────────────────────────────────────

  it('seeds the panel from the durable session-state snapshot on reload', async () => {
    const { service } = await setupWithSnapshot('thread-reload', {
      usage: {
        turn: 7,
        input_tokens: 61_000,
        output_tokens: 1_200,
        reasoning_tokens: 300,
        reasoning_estimated: true,
        ctx_limit_tokens: 200_000,
        compaction_threshold_tokens: 160_000,
      },
    });
    const u = service.currentUsage()!;
    expect(u.threadId).toBe('thread-reload');
    expect(u.turn).toBe(7);
    expect(u.inputTokens).toBe(61_000);
    // Server-side totals land as totals — they are not re-accumulated.
    expect(u.outputTokensTurn).toBe(1_200);
    expect(u.reasoningTokensTurn).toBe(300);
    expect(u.reasoningEstimated).toBe(true);
    expect(u.compactionThresholdTokens).toBe(160_000);
  });

  it('treats an explicit null snapshot usage as authoritative', async () => {
    const ctx = await connectWithUsage('thread-old', { turn: 1, input_tokens: 154_600 });
    // Reconnect to a thread whose journal carries no usage at all.
    ctx.mockHttp.get.mockImplementation(snapshotGetMock('thread-fresh', { usage: null }));
    await ctx.service.connect('thread-fresh');
    fireSseOpen(ctx.sseInstances[1]);
    expect(ctx.service.usage()).toBeNull();
    expect(ctx.service.currentUsage()).toBeNull();
  });

  it('does not double-count replayed frames the snapshot already aggregated', async () => {
    const { service, es } = await setupWithSnapshot('thread-replay', {
      cursorSeq: 10,
      usage: {
        turn: 3,
        input_tokens: 50_000,
        output_tokens: 900,
        reasoning_tokens: 100,
        reasoning_estimated: false,
        ctx_limit_tokens: 200_000,
        compaction_threshold_tokens: 160_000,
      },
    });
    // Guard the guard: if the snapshot had failed to load, the assertions
    // below would pass on coincidence (the replayed frames happen to sum to
    // the same 900). `reasoning` is the discriminator — only seeding supplies
    // it, since neither replayed frame carries a reasoning count.
    expect(service.currentUsage()!.outputTokensTurn).toBe(900);
    expect(service.currentUsage()!.reasoningTokensTurn).toBe(100);

    // Replay re-delivers the very frames the snapshot summed (seq <= 10).
    // Unskipped, these would drive the total to 1800.
    fireSseMessage(
      es,
      { method: 'usage.updated', params: { turn: 3, input_tokens: 49_000, output_tokens: 600 } },
      '1:9',
    );
    fireSseMessage(
      es,
      { method: 'usage.updated', params: { turn: 3, input_tokens: 50_000, output_tokens: 300 } },
      '1:10',
    );
    let u = service.currentUsage()!;
    expect(u.outputTokensTurn).toBe(900);
    expect(u.reasoningTokensTurn).toBe(100);
    expect(u.inputTokens).toBe(50_000);

    // A genuinely newer frame (seq > cursor) still accumulates on top.
    fireSseMessage(
      es,
      { method: 'usage.updated', params: { turn: 3, input_tokens: 52_000, output_tokens: 150 } },
      '1:11',
    );
    u = service.currentUsage()!;
    expect(u.outputTokensTurn).toBe(1_050);
    expect(u.inputTokens).toBe(52_000);
  });

  it('rebuilds from replay when an older peer omits the usage key', async () => {
    // Rolling deploy: an older orchestrator's snapshot has no `usage` key, so
    // nothing is seeded. Its covered frames must therefore still accumulate —
    // dropping them as "already counted" would leave the panel blank after a
    // reload until the next LLM call.
    const { service, es } = await setupWithSnapshot('thread-legacy', {
      omitUsageKey: true,
      cursorSeq: 10,
    });
    expect(service.currentUsage()).toBeNull();
    // Below the snapshot cursor — covered, but uncounted by an old peer.
    fireSseMessage(
      es,
      { method: 'usage.updated', params: { turn: 2, input_tokens: 33_000, output_tokens: 70 } },
      '1:5',
    );
    expect(service.currentUsage()!.inputTokens).toBe(33_000);
    expect(service.currentUsage()!.outputTokensTurn).toBe(70);
  });

  it('accumulates output/reasoning within a turn, latest input wins', async () => {
    const { service, es } = await setup();
    fireSseMessage(
      es,
      {
        method: 'usage.updated',
        params: {
          turn: 1,
          input_tokens: 10_000,
          output_tokens: 500,
          reasoning_tokens: 200,
          ctx_limit_tokens: 128_000,
          compaction_threshold_tokens: 80_000,
        },
      },
      '1:1',
    );
    fireSseMessage(
      es,
      {
        method: 'usage.updated',
        params: { turn: 1, input_tokens: 12_000, output_tokens: 700, reasoning_tokens: 100 },
      },
      '1:2',
    );
    const u = service.usage()!;
    expect(u.inputTokens).toBe(12_000);
    expect(u.outputTokensTurn).toBe(1_200);
    expect(u.reasoningTokensTurn).toBe(300);
    expect(u.ctxLimitTokens).toBe(128_000);
    // Compaction threshold sticks across same-turn frames that omit it.
    expect(u.compactionThresholdTokens).toBe(80_000);
  });

  it('resets per-turn accumulators when the turn changes', async () => {
    const { service, es } = await setup();
    fireSseMessage(
      es,
      {
        method: 'usage.updated',
        params: {
          turn: 1,
          input_tokens: 10_000,
          output_tokens: 500,
          ctx_limit_tokens: 128_000,
          compaction_threshold_tokens: 80_000,
        },
      },
      '1:1',
    );
    fireSseMessage(
      es,
      {
        method: 'usage.updated',
        params: { turn: 2, input_tokens: 11_000, output_tokens: 50 },
      },
      '1:2',
    );
    const u = service.usage()!;
    expect(u.turn).toBe(2);
    expect(u.outputTokensTurn).toBe(50);
    // limit + compaction threshold carried over from the earlier frame
    expect(u.ctxLimitTokens).toBe(128_000);
    expect(u.compactionThresholdTokens).toBe(80_000);
  });

  it('marks reasoning estimated when the agent derives it; sticky within the turn', async () => {
    const { service, es } = await setup();
    // gemma-style: provider gave no reasoning count, agent derived + flagged it.
    fireSseMessage(
      es,
      {
        method: 'usage.updated',
        params: {
          turn: 1,
          input_tokens: 10_000,
          output_tokens: 81,
          reasoning_tokens: 70,
          reasoning_estimated: true,
          ctx_limit_tokens: 128_000,
        },
      },
      '1:1',
    );
    let u = service.usage()!;
    expect(u.reasoningTokensTurn).toBe(70);
    expect(u.reasoningEstimated).toBe(true);
    // A later same-turn frame keeps the turn flagged estimated.
    fireSseMessage(
      es,
      {
        method: 'usage.updated',
        params: { turn: 1, output_tokens: 20, reasoning_tokens: 30, reasoning_estimated: true },
      },
      '1:2',
    );
    u = service.usage()!;
    expect(u.reasoningTokensTurn).toBe(100);
    expect(u.reasoningEstimated).toBe(true);
  });

  it('leaves reasoningEstimated false for provider-reported reasoning, and resets on a new turn', async () => {
    const { service, es } = await setup();
    fireSseMessage(
      es,
      {
        method: 'usage.updated',
        params: { turn: 1, input_tokens: 10_000, output_tokens: 500, reasoning_tokens: 200 },
      },
      '1:1',
    );
    expect(service.usage()!.reasoningEstimated).toBe(false);
    // New turn that happens to be estimated → flips; proves the per-turn reset.
    fireSseMessage(
      es,
      {
        method: 'usage.updated',
        params: {
          turn: 2,
          input_tokens: 11_000,
          output_tokens: 40,
          reasoning_tokens: 33,
          reasoning_estimated: true,
        },
      },
      '1:2',
    );
    const u = service.usage()!;
    expect(u.turn).toBe(2);
    expect(u.reasoningEstimated).toBe(true);
  });

  describe('historyToTurns — tool categories survive the replay', () => {
    // The folded-chip summary (session_turn_rendering.md "Phase 2 — the live
    // edge") groups by category. The live SSE `tool.started` frame carries
    // it; history gets it stamped by the orchestrator at read time
    // (main.py _stamp_tool_categories). If this passthrough breaks, a
    // reloaded turn silently degrades from "19× citations · 12× searches"
    // to "38× steps" — no error, just a worse answer than the live view.
    const aiMsg = (toolCalls: unknown[]) => ({
      id: 'm1',
      role: 'ai',
      content: null,
      tool_calls: toolCalls,
      turn_number: 1,
      created_at: null,
    });

    it('carries category from the history payload onto the event', () => {
      const turns = historyToTurns([
        aiMsg([
          { name: 'cite_web', args: {}, id: 'tc1', category: 'citation' },
          { name: 'web_search', args: {}, id: 'tc2', category: 'research' },
        ]),
      ] as never);
      const tools = (turns[0] as AssistantTurn).events.filter(isToolCall);
      expect(tools.map((t) => t.category)).toEqual(['citation', 'research']);
    });

    it('leaves category undefined when the payload omits it', () => {
      // Unknown/renamed tool, or an orchestrator that predates the stamp.
      // Must stay undefined so the chip buckets it as "other" rather than
      // guessing a category client-side.
      const turns = historyToTurns([
        aiMsg([{ name: 'no_such_tool', args: {}, id: 'tc1' }]),
      ] as never);
      const tools = (turns[0] as AssistantTurn).events.filter(isToolCall);
      expect(tools[0].category).toBeUndefined();
    });

    it('keeps category alongside a denied decision', () => {
      const turns = historyToTurns([
        aiMsg([
          { name: 'run_command', args: {}, id: 'tc1', category: 'shell', decision: 'denied' },
        ]),
      ] as never);
      const tools = (turns[0] as AssistantTurn).events.filter(isToolCall);
      expect(tools[0]).toMatchObject({ status: 'denied', category: 'shell' });
    });

    it('keeps an expired decision visibly expired after history replay', () => {
      const turns = historyToTurns([
        aiMsg([
          { name: 'run_command', args: {}, id: 'tc1', category: 'shell', decision: 'expired' },
        ]),
      ] as never);
      const tools = (turns[0] as AssistantTurn).events.filter(isToolCall);
      expect(tools[0]).toMatchObject({ decision: 'expired', status: 'expired' });
    });
  });

  describe('historyToTurns — synthetic image-delivery messages', () => {
    const msg = (over: Record<string, unknown>) => ({
      id: 'x',
      role: 'human',
      content: '',
      tool_calls: null,
      turn_number: 1,
      created_at: null,
      ...over,
    });

    it('hides "Image content from tool call <id>:" markers from the transcript', () => {
      const turns = historyToTurns([
        msg({ id: '1', content: 'Check this page' }),
        msg({ id: '2', content: 'Image content from tool call call_ABC123:' }),
        msg({ id: '3', role: 'ai', content: 'Looks good.' }),
      ] as never);
      const users = turns.filter((t) => t.kind === 'user');
      expect(users).toHaveLength(1);
      expect((users[0] as { content: string }).content).toBe('Check this page');
    });

    it('keeps a real user message that merely starts with the phrase', () => {
      const turns = historyToTurns([
        msg({ content: 'Image content from tool call handling is broken — fix it' }),
      ] as never);
      expect(turns.filter((t) => t.kind === 'user')).toHaveLength(1);
    });

    it("hides the marker under the 'event' role too", () => {
      // These rows used to persist as role='human', which made the stateless
      // run-queue claim them as unanswered user input and re-run a finished
      // turn; they now carry the 'event' persist role. 'event' renders as a
      // muted system line, which is no better here than a user bubble — so
      // the suppression is keyed on content, ahead of the role dispatch, and
      // covers both the new rows and the pre-migration-0211 ones.
      const turns = historyToTurns([
        msg({ id: '1', content: 'Check this page' }),
        msg({ id: '2', role: 'event', content: 'Image content from tool call call_ABC123:' }),
      ] as never);
      expect(turns.filter((t) => t.kind === 'system')).toHaveLength(0);
      expect(turns.filter((t) => t.kind === 'user')).toHaveLength(1);
    });

    it("still renders a genuine 'event' notice", () => {
      const turns = historyToTurns([
        msg({ id: '1', role: 'event', content: '[JOB_FINISHED] worker job completed' }),
      ] as never);
      expect(turns.filter((t) => t.kind === 'system')).toHaveLength(1);
    });
  });

  describe('citation drift + snapshot fetch (Half-B v2)', () => {
    it('fetchCitationDrift GETs the by-citation drift endpoint and returns the result', async () => {
      const { service, mockHttp } = createService();
      const payload = { citation_id: 7, live_state: 'changed', snapshot_available: true };
      mockHttp.get.mockReturnValue(of(payload));
      const res = await service.fetchCitationDrift(7);
      expect(res).toEqual(payload);
      expect(mockHttp.get).toHaveBeenCalledWith(expect.stringContaining('/citations/7/drift'));
    });

    it('fetchCitationDrift returns null when the request errors', async () => {
      const { service, mockHttp } = createService();
      mockHttp.get.mockReturnValue(throwError(() => new Error('boom')));
      expect(await service.fetchCitationDrift(7)).toBeNull();
    });

    it('fetchCitationSnapshotBlob requests a blob and returns it', async () => {
      const { service, mockHttp } = createService();
      const blob = new Blob(['pdf'], { type: 'application/pdf' });
      mockHttp.get.mockReturnValue(of(blob));
      const res = await service.fetchCitationSnapshotBlob(9);
      expect(res).toBe(blob);
      expect(mockHttp.get).toHaveBeenCalledWith(
        expect.stringContaining('/citations/9/snapshot'),
        expect.objectContaining({ responseType: 'blob' }),
      );
    });

    it('fetchCitationSnapshotBlob returns null on error', async () => {
      const { service, mockHttp } = createService();
      mockHttp.get.mockReturnValue(throwError(() => new Error('boom')));
      expect(await service.fetchCitationSnapshotBlob(9)).toBeNull();
    });
  });

  // Live verdict push: the aux verifier broadcasts a citation.verdict frame
  // when a citation flips pending→verified/failed; the handler patches it in
  // place so the panel updates without a per-turn refetch. Driven through
  // _handleEvent directly (no connect) so the per-turn loadCitations effect
  // stays dormant and can't clobber the seeded map.
  describe('citation.verdict live patch', () => {
    it('patches a loaded citation in place', () => {
      const { service } = createService();
      service.citationsByCid.set(new Map([[5, { id: 5, verification_status: 'pending' } as any]]));
      (service as any)._handleEvent({
        method: 'citation.verdict',
        params: { citation_id: 5, verification_status: 'verified' },
      });
      expect(service.citationsByCid().get(5)?.verification_status).toBe('verified');
    });

    it('is a no-op when the citation is not loaded yet', () => {
      const { service } = createService();
      service.citationsByCid.set(new Map());
      (service as any)._handleEvent({
        method: 'citation.verdict',
        params: { citation_id: 99, verification_status: 'failed' },
      });
      expect(service.citationsByCid().has(99)).toBe(false);
    });

    it('ignores a malformed citation_id', () => {
      const { service } = createService();
      service.citationsByCid.set(new Map([[5, { id: 5, verification_status: 'pending' } as any]]));
      (service as any)._handleEvent({
        method: 'citation.verdict',
        params: { citation_id: 'nope', verification_status: 'verified' },
      });
      expect(service.citationsByCid().get(5)?.verification_status).toBe('pending');
    });
  });
});

describe('PersistentChatService — renameThread', () => {
  it('issues a PATCH to the thread with the new title', async () => {
    const { service, mockHttp } = createService();
    await service.renameThread('thread-1', 'New name');
    expect(mockHttp.patch).toHaveBeenCalledWith(
      expect.stringContaining('/persistent/threads/thread-1'),
      { title: 'New name' },
    );
  });

  it('updates sessionTitle when renaming the active thread', async () => {
    const { service } = createService();
    service.threadId.set('thread-1');
    await service.renameThread('thread-1', 'New name');
    expect(service.sessionTitle()).toBe('New name');
  });

  it('leaves sessionTitle untouched when renaming a different thread', async () => {
    const { service } = createService();
    service.threadId.set('thread-active');
    service.sessionTitle.set('Active title');
    await service.renameThread('thread-other', 'Other name');
    expect(service.sessionTitle()).toBe('Active title');
  });
});

// ---------------------------------------------------------------------------
// Phase 2 — send-liveness kickstart, wake recovery, _openSse single-flight
// (knowledge-base/knowledge/features/session_reliability_and_transport_simplification.md)
// ---------------------------------------------------------------------------

describe('PersistentChatService — Phase 2: send-liveness kickstart', () => {
  let originalEs: any;
  let originalWs: any;

  beforeEach(() => {
    originalEs = (globalThis as any).EventSource;
    originalWs = (globalThis as any).WebSocket;
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
    (globalThis as any).EventSource = originalEs;
    (globalThis as any).WebSocket = originalWs;
    vi.clearAllMocks();
  });

  async function connectOpened() {
    const ctx = createService();
    ctx.mockHttp.get.mockImplementation(() =>
      of({ status: 'active', total_turns: 0, messages: [], total: 0 }),
    );
    await ctx.service.connect('thread-ks');
    await vi.advanceTimersByTimeAsync(0); // drain _openSse cursor await
    const es = ctx.sseInstances[0];
    fireSseOpen(es);
    return { ...ctx, es };
  }

  it('forces a reconnect when a send is accepted but no SSE data follows', async () => {
    const { service, es } = await connectOpened();
    const reconnectSpy = vi.spyOn(service, 'reconnectNow').mockImplementation(() => {});

    // 200 → kickstart armed.
    await (service as any)._postInput('hi');

    // Contaminated liveness signals that must NOT defuse the kickstart:
    // an onopen already fired (bumped sseLastEventAt + agentLastEventAt), a
    // ping bumps sseLastEventAt, and a control-WS frame routed through
    // _handleEvent bumps agentLastEventAt. None are real SSE data.
    fireSseNamedEvent(es, 'ping', {});
    (service as any)._handleEvent({ method: 'session.state', params: {} });

    await vi.advanceTimersByTimeAsync(5_001);
    expect(reconnectSpy).toHaveBeenCalledTimes(1);
  });

  it('does not reconnect when an SSE data frame arrives after the send', async () => {
    const { service, es } = await connectOpened();
    const reconnectSpy = vi.spyOn(service, 'reconnectNow').mockImplementation(() => {});

    await (service as any)._postInput('hi');
    // A real journal frame lands (onmessage) → data clock advances.
    fireSseMessage(es, { method: 'turn.started', params: { turn_id: 1 } }, '1:1');

    await vi.advanceTimersByTimeAsync(5_001);
    expect(reconnectSpy).not.toHaveBeenCalled();
  });

  it('does not arm the kickstart for an unproven 409 conflict', async () => {
    const { service } = await connectOpened();
    const reconnectSpy = vi.spyOn(service, 'reconnectNow').mockImplementation(() => {});

    (service as any).http.post = vi.fn().mockReturnValue(throwError(() => ({ status: 409 })));

    await (service as any)._postInput('dup');
    await vi.advanceTimersByTimeAsync(5_001);
    expect(reconnectSpy).not.toHaveBeenCalled();
  });

  it('disconnect() clears a pending kickstart timer', async () => {
    const { service } = await connectOpened();
    const reconnectSpy = vi.spyOn(service, 'reconnectNow').mockImplementation(() => {});

    await (service as any)._postInput('hi');
    service.disconnect();
    await vi.advanceTimersByTimeAsync(5_001);
    expect(reconnectSpy).not.toHaveBeenCalled();
  });
});

describe('PersistentChatService — Phase 2: wake recovery', () => {
  let originalEs: any;
  let originalWs: any;

  function setVisibility(state: string): void {
    Object.defineProperty(document, 'visibilityState', {
      value: state,
      configurable: true,
    });
  }

  beforeEach(() => {
    originalEs = (globalThis as any).EventSource;
    originalWs = (globalThis as any).WebSocket;
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
    (globalThis as any).EventSource = originalEs;
    (globalThis as any).WebSocket = originalWs;
    setVisibility('visible');
    vi.clearAllMocks();
  });

  async function connectOpened() {
    const ctx = createService();
    ctx.mockHttp.get.mockImplementation(() =>
      of({ status: 'active', total_turns: 0, messages: [], total: 0 }),
    );
    await ctx.service.connect('thread-wake');
    await vi.advanceTimersByTimeAsync(0);
    const es = ctx.sseInstances[0];
    fireSseOpen(es); // sse OPEN + fresh sseLastEventAt
    return { ...ctx, es };
  }

  it('records hiddenAt when the tab goes hidden', async () => {
    const { service } = await connectOpened();
    setVisibility('hidden');
    document.dispatchEvent(new Event('visibilitychange'));
    expect((service as any).hiddenAt).toBeGreaterThan(0);
  });

  it('forces a reopen after a long hide even with an OPEN, fresh SSE', async () => {
    const { service } = await connectOpened();
    const reconnectSpy = vi.spyOn(service, 'reconnectNow').mockImplementation(() => {});

    // Backdate the hide past the watchdog window without advancing timers,
    // so the SSE stays OPEN + sseLastEventAt fresh: only `force` can trigger
    // the reopen.
    (service as any).hiddenAt = Date.now() - 46_000;
    setVisibility('visible');
    document.dispatchEvent(new Event('visibilitychange'));

    expect(reconnectSpy).toHaveBeenCalledTimes(1);
  });

  it('does not reopen after a short hide when the SSE is healthy', async () => {
    const { service } = await connectOpened();
    const reconnectSpy = vi.spyOn(service, 'reconnectNow').mockImplementation(() => {});

    (service as any).hiddenAt = Date.now() - 1_000;
    setVisibility('visible');
    document.dispatchEvent(new Event('visibilitychange'));

    expect(reconnectSpy).not.toHaveBeenCalled();
  });

  it('pageshow(persisted) forces a revalidate; persisted=false does not', async () => {
    const { service } = await connectOpened();
    const reconnectSpy = vi.spyOn(service, 'reconnectNow').mockImplementation(() => {});

    const persisted = new Event('pageshow');
    Object.defineProperty(persisted, 'persisted', { value: true });
    window.dispatchEvent(persisted);
    expect(reconnectSpy).toHaveBeenCalledTimes(1);

    reconnectSpy.mockClear();
    const fresh = new Event('pageshow');
    Object.defineProperty(fresh, 'persisted', { value: false });
    window.dispatchEvent(fresh);
    expect(reconnectSpy).not.toHaveBeenCalled();
  });

  it('document resume forces a revalidate', async () => {
    const { service } = await connectOpened();
    const reconnectSpy = vi.spyOn(service, 'reconnectNow').mockImplementation(() => {});

    document.dispatchEvent(new Event('resume'));
    expect(reconnectSpy).toHaveBeenCalledTimes(1);
  });
});

// knowledge-base/knowledge/issues/persistent_chat_silent_disconnect.md: a
// laptop sleep kills both links underneath the page without a close frame,
// so the EventSource and the pinned control WS keep reporting OPEN.
describe('PersistentChatService — waking from sleep with silently dead transports', () => {
  let originalEs: any;
  let originalWs: any;

  function setVisibility(state: string): void {
    Object.defineProperty(document, 'visibilityState', {
      value: state,
      configurable: true,
    });
  }

  function framesOn(ws: any) {
    return ws.send.mock.calls.map((c: any) => JSON.parse(c[0]));
  }

  beforeEach(() => {
    originalEs = (globalThis as any).EventSource;
    originalWs = (globalThis as any).WebSocket;
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
    (globalThis as any).EventSource = originalEs;
    (globalThis as any).WebSocket = originalWs;
    setVisibility('visible');
    vi.clearAllMocks();
  });

  /** A pinned session with the stream and the control WS both up. */
  async function connectPinned(threadId: string) {
    const ctx = createService();
    ctx.mockHttp.get.mockImplementation(activeSessionGet);
    await ctx.service.connect(threadId);
    await vi.advanceTimersByTimeAsync(0);
    fireSseOpen(ctx.sseInstances[0]);
    expect(ctx.service.connectionState()).toBe('connected');
    expect(ctx.wsInstances).toHaveLength(1);
    return { ...ctx, zombieWs: ctx.wsInstances[0] };
  }

  /** Suspend the machine: the wall clock jumps, but no timer fires and no
   *  frame arrives. Pending timers keep their remaining delay, as browser
   *  timers on a monotonic clock that stops while suspended do. */
  function sleepFor(ms: number): void {
    vi.setSystemTime(Date.now() + ms);
  }

  const TWO_HOURS = 2 * 60 * 60_000;

  it('leaves Connected on the first watchdog tick after waking, with no wake event at all', async () => {
    const ctx = await connectPinned('thread-sleep-tick');

    sleepFor(TWO_HOURS);
    await vi.advanceTimersByTimeAsync(5_000);

    expect(ctx.sseInstances[0].close).toHaveBeenCalled();
    expect(ctx.sseInstances).toHaveLength(2);
    expect(ctx.service.connectionState()).toBe('connecting');
  });

  it('a wake event retires the dead control WS too, instead of trusting its OPEN readyState', async () => {
    const ctx = await connectPinned('thread-sleep-wake');

    setVisibility('hidden');
    document.dispatchEvent(new Event('visibilitychange'));
    sleepFor(TWO_HOURS);
    setVisibility('visible');
    document.dispatchEvent(new Event('visibilitychange'));
    await vi.advanceTimersByTimeAsync(0);

    expect(ctx.service.connectionState()).toBe('connecting');
    expect(ctx.sseInstances).toHaveLength(2);
    expect(ctx.zombieWs.close).toHaveBeenCalled();

    // The stream recovers and brings a fresh control socket with it.
    fireSseOpen(ctx.sseInstances[1]);
    await vi.advanceTimersByTimeAsync(0);
    expect(ctx.service.connectionState()).toBe('connected');
    expect(ctx.wsInstances).toHaveLength(2);

    // Commands now go out on the fresh socket, never into the dead one.
    const fresh = ctx.wsInstances[1];
    (ctx.service as any)._sendControl({ method: 'approve' });
    expect(framesOn(fresh)).toContainEqual({ method: 'approve' });
    expect(framesOn(ctx.zombieWs)).not.toContainEqual({ method: 'approve' });
  });

  it('a command issued right after waking, before any tick or wake event, is not written into the dead socket', async () => {
    const ctx = await connectPinned('thread-sleep-click');

    sleepFor(TWO_HOURS);
    (ctx.service as any)._sendControl({ method: 'approve' });
    await vi.advanceTimersByTimeAsync(0);

    expect(framesOn(ctx.zombieWs)).not.toContainEqual({ method: 'approve' });
    expect(ctx.zombieWs.close).toHaveBeenCalled();
    // It rides the replacement socket once that opens.
    expect(ctx.wsInstances).toHaveLength(2);
    const fresh = ctx.wsInstances[1];
    fresh.onopen();
    expect(framesOn(fresh)).toContainEqual({ method: 'approve' });
  });

  it('a destructive control right after waking is refused, not lost, and its retry has a socket to use', async () => {
    const ctx = await connectPinned('thread-sleep-rewind');

    sleepFor(TWO_HOURS);
    const sent = (ctx.service as any)._sendImmediateControl({ method: 'rewind', mode: 'both' });
    await vi.advanceTimersByTimeAsync(0);

    expect(sent).toBe(false);
    expect(ctx.service.error()).toBe('chat.rewind.connectionDown');
    expect(ctx.zombieWs.send).not.toHaveBeenCalled();
    expect(ctx.wsInstances).toHaveLength(2);
  });

  it('going offline leaves Connected at once; coming back online reopens both transports', async () => {
    const ctx = await connectPinned('thread-offline');

    window.dispatchEvent(new Event('offline'));
    await vi.advanceTimersByTimeAsync(0);

    expect(ctx.service.connectionState()).toBe('connecting');
    expect(ctx.sseInstances[0].close).toHaveBeenCalled();
    expect(ctx.zombieWs.close).toHaveBeenCalled();

    // Offline, the reopened stream cannot connect (the browser keeps
    // retrying); `online` makes the next attempt immediately.
    fireSseTransientError(ctx.sseInstances[1]);
    window.dispatchEvent(new Event('online'));
    await vi.advanceTimersByTimeAsync(0);
    expect(ctx.sseInstances).toHaveLength(3);
    fireSseOpen(ctx.sseInstances[2]);
    await vi.advanceTimersByTimeAsync(0);
    expect(ctx.service.connectionState()).toBe('connected');
    expect(ctx.wsInstances).toHaveLength(2);
  });

  it('the control WS watchdog replaces a half-open socket without waiting for its close event', async () => {
    const ctx = await connectPinned('thread-ws-halfopen');

    // The stream stays healthy; only the control socket goes quiet.
    for (let i = 0; i < 3; i++) {
      await vi.advanceTimersByTimeAsync(20_000);
      fireSseNamedEvent(ctx.sseInstances[0], 'ping', {});
    }
    expect(ctx.zombieWs.close).toHaveBeenCalledWith(4002, 'heartbeat timeout');

    // close() on an unreachable peer starts a closing handshake that cannot
    // complete, so no close event arrives (the mock never fires one). The
    // replacement must not wait for it.
    await vi.advanceTimersByTimeAsync(1_000);
    expect(ctx.wsInstances).toHaveLength(2);
    expect(ctx.sseInstances).toHaveLength(1);
    expect(ctx.service.connectionState()).toBe('connected');
  });

  it('a healthy control WS survives a focus revalidate', async () => {
    const ctx = await connectPinned('thread-ws-healthy');

    await vi.advanceTimersByTimeAsync(20_000);
    fireSseNamedEvent(ctx.sseInstances[0], 'ping', {});
    ctx.zombieWs.onmessage({ data: JSON.stringify({ method: 'ws.ping', params: {} }) });
    window.dispatchEvent(new Event('focus'));
    await vi.advanceTimersByTimeAsync(0);

    expect(ctx.zombieWs.close).not.toHaveBeenCalled();
    expect(ctx.wsInstances).toHaveLength(1);
    expect(ctx.sseInstances).toHaveLength(1);
  });
});

describe('PersistentChatService — Phase 2: _openSse single-flight guard', () => {
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

  it('two concurrent opens retain exactly one EventSource', async () => {
    const ctx = createService();
    let resolveCursor: (v: any) => void = () => {};
    ctx.mockCache.getThreadCursor.mockReturnValue(
      new Promise((r) => {
        resolveCursor = r;
      }),
    );
    ctx.service.threadId.set('tid');

    const p1 = (ctx.service as any)._openSse('tid');
    const p2 = (ctx.service as any)._openSse('tid');
    resolveCursor(null);
    await p1;
    await p2;

    // The first open is superseded post-await and bails before constructing
    // an EventSource; only the second installs one.
    expect(ctx.sseInstances.length).toBe(1);
  });

  it('disconnect() during the cursor await assigns no EventSource', async () => {
    const ctx = createService();
    let resolveCursor: (v: any) => void = () => {};
    ctx.mockCache.getThreadCursor.mockReturnValue(
      new Promise((r) => {
        resolveCursor = r;
      }),
    );
    ctx.service.threadId.set('tid');

    const p = (ctx.service as any)._openSse('tid');
    ctx.service.disconnect(); // bumps sseGeneration → supersedes the open
    resolveCursor(null);
    await p;

    expect(ctx.sseInstances.length).toBe(0);
    expect((ctx.service as any).sse).toBeNull();
  });
});

describe('PersistentChatService — Phase 4: delta coalescing', () => {
  let originalEs: any;
  let originalWs: any;

  beforeEach(() => {
    originalEs = (globalThis as any).EventSource;
    originalWs = (globalThis as any).WebSocket;
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
    (globalThis as any).EventSource = originalEs;
    (globalThis as any).WebSocket = originalWs;
    vi.clearAllMocks();
  });

  // Connect, open the SSE, and open a turn so tokens have somewhere to land.
  async function streamingTurn() {
    const ctx = createService();
    ctx.mockHttp.get.mockImplementation(() =>
      of({ status: 'active', total_turns: 0, messages: [], total: 0 }),
    );
    await ctx.service.connect('t-p4');
    await vi.advanceTimersByTimeAsync(0);
    const es = ctx.sseInstances[0];
    fireSseOpen(es);
    fireSseMessage(es, { method: 'turn.started', params: { turn_id: 1 } }, '1:1');
    return { ...ctx, es };
  }

  it('coalesces N token frames into a single conversation update at flush', async () => {
    const ctx = await streamingTurn();
    const updateSpy = vi.spyOn(ctx.service.conversation, 'update');

    for (const c of ['a', 'b', 'c', 'd', 'e']) {
      fireSseMessage(ctx.es, { method: 'token', params: { content: c } }, `1:${c}`);
    }
    // Nothing written to the signal before the 80ms flush.
    expect(updateSpy).not.toHaveBeenCalled();

    await vi.advanceTimersByTimeAsync(80);
    // Exactly one signal write folds the whole burst, in wire order.
    expect(updateSpy).toHaveBeenCalledTimes(1);
    const text = ctx.service
      .currentStreamingTurn()!
      .events.find((e) => e.kind === 'text') as TextEvent;
    expect(text.content).toBe('abcde');
  });

  it('flushes buffered text before a non-delta frame; a post-tool token opens a new block', async () => {
    const ctx = await streamingTurn();
    fireSseMessage(ctx.es, { method: 'token', params: { content: 'before ' } }, '1:2');
    fireSseMessage(ctx.es, { method: 'token', params: { content: 'tool' } }, '1:3');
    // tool.started is non-delta → flushes the buffered text synchronously.
    fireSseMessage(
      ctx.es,
      { method: 'tool.started', params: { id: 'tc1', tool: 'run_command', args: {} } },
      '1:4',
    );

    const evs = ctx.service.currentStreamingTurn()!.events;
    const textIdx = evs.findIndex((e) => e.kind === 'text');
    const toolIdx = evs.findIndex((e) => e.kind === 'tool_call');
    expect(textIdx).toBeGreaterThanOrEqual(0);
    expect(toolIdx).toBeGreaterThan(textIdx); // text precedes the tool

    fireSseMessage(ctx.es, { method: 'token', params: { content: 'after' } }, '1:5');
    await vi.advanceTimersByTimeAsync(80);
    const texts = ctx.service.currentStreamingTurn()!.events.filter((e) => e.kind === 'text');
    expect(texts.length).toBe(2); // post-tool token is a new block
  });

  it('never cross-merges thinking deltas with different message_ids', async () => {
    const ctx = await streamingTurn();
    fireSseMessage(
      ctx.es,
      { method: 'thinking', params: { content: 'first', message_id: 'm1' } },
      '1:2',
    );
    fireSseMessage(
      ctx.es,
      { method: 'thinking', params: { content: 'second', message_id: 'm2' } },
      '1:3',
    );
    await vi.advanceTimersByTimeAsync(80);

    const thoughts = ctx.service.currentStreamingTurn()!.events.filter((e) => e.kind === 'thought');
    expect(thoughts.length).toBe(2);
  });

  it('turn.completed mid-buffer applies the token then closes — no stuck streaming (S1 wedge)', async () => {
    const ctx = await streamingTurn();
    fireSseMessage(ctx.es, { method: 'token', params: { content: 'partial' } }, '1:2');
    // Buffered (not flushed). The turn boundary arrives.
    fireSseMessage(ctx.es, { method: 'turn.completed', params: { turn_id: 1 } }, '1:3');

    expect(ctx.service.isStreaming()).toBe(false);
    expect(ctx.service.currentStreamingTurn()).toBeNull();
    const last = ctx.service.turns().filter(isAssistantTurn).at(-1) as AssistantTurn;
    expect(last.status).toBe('done');
    expect((last.events.find((e) => e.kind === 'text') as TextEvent).content).toBe('partial');

    // The (already-cleared) flush timer must not resurrect a placeholder.
    await vi.advanceTimersByTimeAsync(80);
    expect(ctx.service.isStreaming()).toBe(false);
    expect(ctx.service.currentStreamingTurn()).toBeNull();
  });

  it('turn.error mid-buffer closes via _closeActiveTurnIfAny — no stranded placeholder', async () => {
    const ctx = await streamingTurn();
    fireSseMessage(ctx.es, { method: 'token', params: { content: 'oops' } }, '1:2');
    fireSseMessage(ctx.es, { method: 'turn.error', params: { message: 'boom' } }, '1:3');

    // The buffered token was applied before _closeActiveTurnIfAny read the
    // active turn id, so the turn is properly closed, not left spinning.
    expect(ctx.service.isStreaming()).toBe(false);
    expect(ctx.service.currentStreamingTurn()).toBeNull();
    await vi.advanceTimersByTimeAsync(80);
    expect(ctx.service.isStreaming()).toBe(false);
  });

  it('disconnect() mid-buffer flushes/cancels — timer no-ops, nothing leaks', async () => {
    const ctx = await streamingTurn();
    fireSseMessage(ctx.es, { method: 'token', params: { content: 'x' } }, '1:2');

    ctx.service.disconnect();
    expect(ctx.service.isStreaming()).toBe(false);
    const turnsAfterDisconnect = ctx.service.turns().length;

    // Stale timer would otherwise fire here and resurrect a bubble.
    await vi.advanceTimersByTimeAsync(80);
    expect(ctx.service.turns().length).toBe(turnsAfterDisconnect);
    expect(ctx.service.isStreaming()).toBe(false);
  });
});

describe('PersistentChatService — session wake on job completion', () => {
  // A worker job the session created reached a terminal state and the
  // orchestrator injected the notice via POST /api/input with role='event'
  // (knowledge-base/knowledge/features/session_wake_on_job_completion.md).
  //
  // Both halves matter and fail differently:
  //   * the live frame — without it the user watches a turn start and stream
  //     a reply with no visible prompt, because /api/input broadcasts nothing
  //     and no frame carries user-message content;
  //   * the history branch — without it the reloaded transcript renders the
  //     notice as a USER bubble, i.e. claims the user said something they
  //     never said.
  // They must agree, or the transcript changes shape on reload.
  let originalEs: unknown;
  let originalWs: unknown;

  beforeEach(() => {
    originalEs = (globalThis as any).EventSource;
    originalWs = (globalThis as any).WebSocket;
  });

  afterEach(() => {
    (globalThis as any).EventSource = originalEs;
    (globalThis as any).WebSocket = originalWs;
    vi.clearAllMocks();
  });

  const NOTICE = '[JOB_FINISHED] A worker job you created has reached a terminal state.';

  async function readySession() {
    const ctx = createService();
    ctx.mockHttp.get.mockImplementation(() =>
      of({ status: 'active', total_turns: 0, messages: [], total: 0 }),
    );
    await ctx.service.connect('thread-wake');
    fireSseOpen(ctx.sseInstances[0]);
    fireSseMessage(ctx.sseInstances[0], { method: 'ready', params: {} }, '1:1');
    return { ...ctx, es: ctx.sseInstances[0] };
  }

  it('session.event renders a system line, not a user bubble', async () => {
    const ctx = await readySession();

    fireSseMessage(
      ctx.es,
      { method: 'session.event', params: { content: NOTICE, id: 'msg_1', role: 'event' } },
      '1:2',
    );

    const systemTurns = ctx.service.turns().filter(isSystemTurn);
    expect(systemTurns.some((t) => String(t.content).includes('[JOB_FINISHED]'))).toBe(true);
    expect(ctx.service.turns().filter(isUserTurn)).toHaveLength(0);
  });

  it('an unknown session.event payload degrades to an empty line, never throws', async () => {
    const ctx = await readySession();
    expect(() =>
      fireSseMessage(ctx.es, { method: 'session.event', params: {} }, '1:2'),
    ).not.toThrow();
  });

  it("history role='event' renders the same system line", () => {
    const turns = historyToTurns([
      {
        id: 'u1',
        role: 'human',
        content: 'launch three designers',
        tool_calls: null,
        turn_number: 1,
        created_at: null,
      },
      {
        id: 'e1',
        role: 'event',
        content: NOTICE,
        tool_calls: null,
        turn_number: 2,
        created_at: null,
      },
    ] as never);

    expect(turns).toHaveLength(2);
    expect(isUserTurn(turns[0])).toBe(true);
    expect(isSystemTurn(turns[1])).toBe(true);
    expect(String((turns[1] as { content: string }).content)).toContain('[JOB_FINISHED]');
  });

  it("history role='event' is never folded into an assistant turn", () => {
    // Unlike role='summary' (which attaches inline to an open turn), an
    // event is a top-level fact about the session, not a step inside a turn.
    const turns = historyToTurns([
      {
        id: 'a1',
        role: 'ai',
        content: 'working',
        tool_calls: null,
        turn_number: 1,
        created_at: null,
      },
      {
        id: 'e1',
        role: 'event',
        content: NOTICE,
        tool_calls: null,
        turn_number: 1,
        created_at: null,
      },
    ] as never);

    expect(turns.filter(isSystemTurn)).toHaveLength(1);
    expect(turns.filter(isAssistantTurn)).toHaveLength(1);
  });
});

describe('PersistentChatService — awaiting-turn state (queued input visibility)', () => {
  // An accepted /input whose turn hasn't started yet used to be invisible:
  // outbox empty, no active turn — composer back to idle/mic while the agent
  // was still flushing the previous turn's cloud push. pendingTurnCount is
  // the client-side ledger that keeps the send visibly alive.
  // knowledge-base/knowledge/issues/session_turn_end_cloud_push_blocks_queued_input.md
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

  async function readySession() {
    const ctx = createService();
    ctx.mockHttp.get.mockImplementation(() =>
      of({ status: 'active', total_turns: 0, messages: [], total: 0 }),
    );
    await ctx.service.connect('thread-w');
    fireSseOpen(ctx.sseInstances[0]);
    fireSseMessage(ctx.sseInstances[0], { method: 'ready', params: {} }, '1:1');
    ctx.mockHttp.post.mockClear();
    return ctx;
  }

  it('an accepted send with no active turn sets isAwaitingTurn', async () => {
    const ctx = await readySession();
    await ctx.service.sendMessage('professor feedback');
    await Promise.resolve();

    expect(ctx.service.outbox()).toEqual([]); // accepted, left the outbox
    expect(ctx.service.pendingTurnCount()).toBe(1);
    expect(ctx.service.isAwaitingTurn()).toBe(true);
    expect(ctx.service.isStreaming()).toBe(false);
  });

  it('turn.started hands off to isStreaming and clears the awaiting state', async () => {
    const ctx = await readySession();
    await ctx.service.sendMessage('hello');
    await Promise.resolve();
    expect(ctx.service.isAwaitingTurn()).toBe(true);

    fireSseMessage(ctx.sseInstances[0], { method: 'turn.started', params: { turn_id: 1 } }, '1:2');

    expect(ctx.service.pendingTurnCount()).toBe(0);
    expect(ctx.service.isAwaitingTurn()).toBe(false);
    expect(ctx.service.isStreaming()).toBe(true);
  });

  it('a 409 conflict stays queued and does not inflate the ledger', async () => {
    const ctx = await readySession();
    ctx.mockHttp.post.mockImplementation((url: string) =>
      String(url).endsWith('/input') ? throwError(() => ({ status: 409 })) : of({}),
    );
    await ctx.service.sendMessage('dupe');
    await Promise.resolve();

    expect(ctx.service.outbox()).toHaveLength(1);
    expect(ctx.service.pendingTurnCount()).toBe(0);
    expect(ctx.service.isAwaitingTurn()).toBe(false);
  });

  // The queued bubble escalates on time alone ("busier than usual" at 10 s,
  // elapsed counter at 60 s) — never on a queue position. The clock lives in
  // the service, keyed off isAwaitingTurn, so every pendingTurnCount reset
  // ends it without each site knowing.
  it('stamps awaitingSince and advances awaitingElapsedMs once a second while queued', async () => {
    const ctx = await readySession();
    vi.useFakeTimers();
    try {
      await ctx.service.sendMessage('queued behind a busy pool');
      await Promise.resolve();
      TestBed.tick();
      expect(ctx.service.awaitingSince()).not.toBeNull();
      expect(ctx.service.awaitingElapsedMs()).toBe(0);

      vi.advanceTimersByTime(10_000);
      expect(ctx.service.awaitingElapsedMs()).toBe(10_000);
      vi.advanceTimersByTime(55_000);
      expect(ctx.service.awaitingElapsedMs()).toBe(65_000);
    } finally {
      vi.useRealTimers();
    }
  });

  it('turn.started ends the awaiting stretch and stops the clock', async () => {
    const ctx = await readySession();
    vi.useFakeTimers();
    try {
      await ctx.service.sendMessage('hello');
      await Promise.resolve();
      TestBed.tick();
      vi.advanceTimersByTime(3_000);
      expect(ctx.service.awaitingElapsedMs()).toBe(3_000);

      fireSseMessage(ctx.sseInstances[0], { method: 'turn.started', params: { turn_id: 1 } }, '1:2');
      TestBed.tick();
      expect(ctx.service.awaitingSince()).toBeNull();
      expect(ctx.service.awaitingElapsedMs()).toBe(0);
      vi.advanceTimersByTime(5_000);
      expect(ctx.service.awaitingElapsedMs()).toBe(0);
    } finally {
      vi.useRealTimers();
    }
  });

  it('a thread switch ends the awaiting stretch (accounting is per thread)', async () => {
    const ctx = await readySession();
    await ctx.service.sendMessage('first thread');
    await Promise.resolve();
    TestBed.tick();
    expect(ctx.service.awaitingSince()).not.toBeNull();

    await ctx.service.connect('thread-x');
    TestBed.tick();
    expect(ctx.service.pendingTurnCount()).toBe(0);
    expect(ctx.service.awaitingSince()).toBeNull();
    expect(ctx.service.awaitingElapsedMs()).toBe(0);
  });

  it('an unstarted turn can never decrement below zero', async () => {
    const ctx = await readySession();
    // turn.started with no tracked accept (other tab / injected input).
    fireSseMessage(ctx.sseInstances[0], { method: 'turn.started', params: { turn_id: 1 } }, '1:2');
    expect(ctx.service.pendingTurnCount()).toBe(0);
  });

  it('session.ended zeroes the ledger', async () => {
    const ctx = await readySession();
    await ctx.service.sendMessage('bye');
    await Promise.resolve();
    expect(ctx.service.pendingTurnCount()).toBe(1);

    fireSseMessage(ctx.sseInstances[0], { method: 'session.ended', params: {} }, '1:2');

    expect(ctx.service.pendingTurnCount()).toBe(0);
    expect(ctx.service.isAwaitingTurn()).toBe(false);
  });

  it('disconnect zeroes the ledger — never a composer stuck on working', async () => {
    const ctx = await readySession();
    await ctx.service.sendMessage('going away');
    await Promise.resolve();
    expect(ctx.service.pendingTurnCount()).toBe(1);

    ctx.service.disconnect();

    expect(ctx.service.pendingTurnCount()).toBe(0);
    expect(ctx.service.isAwaitingTurn()).toBe(false);
  });
});

describe('PersistentChatService — control transport is declared, never inferred', () => {
  // live_settings_silently_dropped_on_stateless_sessions: on a queue-served
  // session `config.update` used to be pushed into the control outbox and
  // wait for a socket `_ensureControlWs` refuses to open — forever, silently.
  // `/connection` now declares the transport per verb (`controls`); the
  // Cockpit dispatches from that declaration, routes `config.update` over the
  // owner PATCH when it says `rest`, and refuses LOUDLY when a verb has no
  // transport at all. An older orchestrator without `controls` gets the
  // legacy derivation, which is exactly what such a server implements.
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

  function socketlessGet(connection: Record<string, unknown>) {
    return (url: string) => {
      if (url.includes('/api/sessions/') && url.endsWith('/connection')) return of(connection);
      if (url.endsWith('/messages')) return of({ messages: [], total: 0 });
      if (url.endsWith('/state')) {
        return of({
          thread_id: 'socketless',
          permission_mode: 'autonomous',
          narration_mode: 'auto',
          turn_count: 2,
          turn_in_flight: false,
          message_count: 4,
          model: 'gpt-5.6-sol',
          temperature: 0,
          running_tool: null,
          pending_permissions: [],
          event_cursor: { epoch: 1, seq: 9 },
          replay_cursor: { epoch: 1, seq: 8 },
          snapshot_source: 'durable_journal',
        });
      }
      return of({ status: 'active', total_turns: 2, config_name: 'session_base' });
    };
  }

  const declaredSocketless = {
    state: 'ready',
    control_socket: 'none',
    ws_url: null,
    token: null,
    expires_at: null,
    pinned_runtime_generation_contract: 1,
    session_runtime_generation: '99999999-9999-4999-8999-999999999999',
    controls: {
      'config.update': 'rest',
      'workspace.undo': 'rest',
      'mode.set': 'rest',
      'narration.set': 'rest',
    },
  };

  async function socketlessSession(connection: Record<string, unknown> = declaredSocketless) {
    const ctx = createService();
    ctx.mockHttp.get.mockImplementation(socketlessGet(connection));
    await ctx.service.connect('socketless');
    fireSseOpen(ctx.sseInstances[0]);
    (ctx.service as any)._ensureControlWs();
    return ctx;
  }

  it('routes config.update over the owner PATCH when /connection declares rest', async () => {
    const ctx = await socketlessSession();
    ctx.mockHttp.patch.mockReturnValue(
      of({
        status: 'updated',
        config_override: { llm: { reasoning_level: 'max' } },
        datasource_ids: null,
        effective: 'next_turn',
      }),
    );
    const stamps = vi.spyOn(ctx.service as any, '_systemMessage');

    const outcome = await ctx.service.updateConfig({ llm: { reasoning_level: 'max' } });

    expect(ctx.wsInstances).toHaveLength(0);
    expect((ctx.service as any).controlOutbox).toEqual([]);
    expect(ctx.mockHttp.patch).toHaveBeenCalledWith(
      expect.stringMatching(/\/persistent\/threads\/socketless\/config$/),
      { config_override: { llm: { reasoning_level: 'max' } } },
    );
    expect(outcome).toMatchObject({
      ok: true,
      transport: 'rest',
      effective: 'next_turn',
      applied: { llm: { reasoning_level: 'max' } },
    });
    // The stamp the socket ack would have journaled is written locally.
    expect(stamps).toHaveBeenCalledWith('chat.control.appliedNextTurn');
    expect(ctx.service.error()).toBeNull();
  });

  it('carries the desired datasource set as the PATCH sibling key', async () => {
    const ctx = await socketlessSession();
    ctx.mockHttp.patch.mockReturnValue(
      of({ status: 'updated', config_override: {}, datasource_ids: ['ds-1'], effective: 'next_turn' }),
    );
    await ctx.service.updateConfig({}, ['ds-1']);
    expect(ctx.mockHttp.patch).toHaveBeenCalledWith(expect.any(String), {
      config_override: {},
      datasource_ids: ['ds-1'],
    });
  });

  it('a rejected PATCH settles as not applied and surfaces the detail', async () => {
    const ctx = await socketlessSession();
    ctx.mockHttp.patch.mockReturnValue(
      throwError(() => ({ status: 409, error: { detail: 'protected sessions cannot change tier' } })),
    );
    const outcome = await ctx.service.updateConfig({ workspace: { backend: 'vm' } });
    expect(outcome).toMatchObject({ ok: false, transport: 'rest' });
    expect(ctx.service.error()).toContain('protected sessions cannot change tier');
  });

  it('an older orchestrator without `controls` still routes a socketless config.update over REST', async () => {
    const { controls: _omitted, ...legacy } = declaredSocketless;
    void _omitted;
    const ctx = await socketlessSession(legacy);
    ctx.mockHttp.patch.mockReturnValue(of({ status: 'updated', config_override: { llm: { temperature: 0.4 } } }));
    const outcome = await ctx.service.updateConfig({ llm: { temperature: 0.4 } });
    expect(outcome.ok).toBe(true);
    expect(ctx.mockHttp.patch).toHaveBeenCalledTimes(1);
    expect(ctx.service.temperature()).toBe(0.4);
  });

  it('the declaration wins over the lane: a socket session whose controls say rest PATCHes', async () => {
    const ctx = createService();
    ctx.mockHttp.get.mockImplementation(
      socketlessGet({
        ...declaredSocketless,
        control_socket: 'websocket',
        ws_url: 'wss://api.example.com/p/socketless/ws?t=jwt',
        token: 'jwt',
        expires_at: 0,
      }),
    );
    await ctx.service.connect('socketless');
    fireSseOpen(ctx.sseInstances[0]);
    ctx.mockHttp.patch.mockReturnValue(of({ status: 'updated', config_override: {} }));

    await ctx.service.updateConfig({ llm: { reasoning_level: 'low' } });

    expect(ctx.mockHttp.patch).toHaveBeenCalledTimes(1);
    const socketFrames = ctx.wsInstances[0].send.mock.calls
      .map((c: any) => JSON.parse(c[0]))
      .filter((f: any) => f.method === 'config.update');
    expect(socketFrames).toEqual([]);
  });

  it('a verb with no transport on this session is refused loudly, not queued', async () => {
    const ctx = await socketlessSession();

    ctx.service.upgradeWorkspace('sandbox');
    expect(ctx.service.error()).toBe('chat.control.unavailable');
    expect(ctx.service.workspaceUpgradeInProgress()).toBeNull();
    expect((ctx.service as any).controlOutbox).toEqual([]);

    ctx.service.error.set(null);
    const stamps = vi.spyOn(ctx.service as any, '_systemMessage');
    expect((ctx.service as any).handleSlashCommand('/done')).toBe(true);
    expect(stamps).not.toHaveBeenCalledWith('Ending session...');
    expect(ctx.service.error()).toBe('chat.control.unavailable');
    expect((ctx.service as any).controlOutbox).toEqual([]);
    expect(ctx.wsInstances).toHaveLength(0);
  });

  it('frames queued before /connection resolved are failed once it declares no socket', async () => {
    const ctx = createService();
    let releaseConnection!: (value: unknown) => void;
    const gate = new Promise((resolve) => (releaseConnection = resolve));
    const get = socketlessGet(declaredSocketless);
    ctx.mockHttp.get.mockImplementation((url: string) => {
      if (url.includes('/api/sessions/') && url.endsWith('/connection')) {
        return from(gate.then(() => declaredSocketless));
      }
      return get(url);
    });
    const connecting = ctx.service.connect('socketless');
    // Let the connect reach its /connection await (thread meta + snapshot
    // resolve first); the gate keeps the transport unknown.
    for (let i = 0; i < 8; i++) await Promise.resolve();
    expect((ctx.service as any).controlSocket).toBe('unknown');
    // Transport still unknown: a socket verb queues, as it always did.
    (ctx.service as any)._sendControl({ method: 'compact', focus: '' });
    expect((ctx.service as any).controlOutbox).toHaveLength(1);

    releaseConnection(undefined);
    await connecting;
    for (let i = 0; i < 8; i++) await Promise.resolve();

    // ...and is failed the moment the session turns out to have no socket,
    // instead of sitting in the outbox for the life of the tab.
    expect((ctx.service as any).controlOutbox).toEqual([]);
    expect(ctx.service.error()).toBe('chat.control.unavailable');
  });
});

// ---------------------------------------------------------------------------
// Durable queue state: parked units, the awaiting poll, owner retry
// (stateless_turn_resilience.md, step 2)
// ---------------------------------------------------------------------------
describe('PersistentChatService — queue state (parked / poll / retry)', () => {
  const parkedBlock = {
    state: 'parked',
    park_reason: 'attach_failed',
    parked_at: '2026-09-08T14:00:00Z',
    retryable: true,
    attempts: 5,
    pending_input: true,
  };

  async function readyOn(threadId: string, connection: Record<string, unknown> = {}) {
    const ctx = createService();
    ctx.mockHttp.get.mockImplementation((url: string) => {
      if (url.endsWith(`/sessions/${threadId}/connection`)) {
        return of({ state: 'ready', ws_url: null, control_socket: 'none', ...connection });
      }
      return of({ status: 'active', total_turns: 0, messages: [], total: 0 });
    });
    await ctx.service.connect(threadId);
    fireSseOpen(ctx.sseInstances[0]);
    fireSseMessage(ctx.sseInstances[0], { method: 'ready', params: {} }, '1:1');
    ctx.mockHttp.post.mockClear();
    return ctx;
  }

  it('renders a parked accept as parked, never as awaiting', async () => {
    const ctx = await readyOn('thread-p');
    ctx.mockHttp.post.mockReturnValue(
      of({ accepted: true, turn_id: 3, queue: parkedBlock }),
    );
    await ctx.service.sendMessage('hello?');
    await Promise.resolve();
    expect(ctx.service.queueState()).toEqual(parkedBlock);
    expect(ctx.service.isParked()).toBe(true);
    // The accept still bumped pendingTurnCount, but a parked unit is not "waiting".
    expect(ctx.service.pendingTurnCount()).toBe(1);
    expect(ctx.service.isAwaitingTurn()).toBe(false);
  });

  it('takes the queue block from /connection so a reload shows the parked unit', async () => {
    const ctx = await readyOn('thread-c', { queue: parkedBlock });
    // The control-plane resolve reads /connection; give it a tick.
    await new Promise((r) => setTimeout(r, 0));
    expect(ctx.service.queueState()?.state).toBe('parked');
    expect(ctx.service.isParked()).toBe(true);
  });

  it('polls GET /queue every 5 s only while awaiting, and stops on turn.started', async () => {
    vi.useFakeTimers();
    try {
      const ctx = await readyOn('thread-q');
      const getQueue = vi.fn().mockReturnValue(of({ ...parkedBlock, state: 'queued', park_reason: null }));
      (ctx.mockApi as any).getThreadQueue = getQueue;
      ctx.mockHttp.post.mockReturnValue(
        of({ accepted: true, turn_id: 1, queue: { ...parkedBlock, state: 'queued', park_reason: null } }),
      );
      await ctx.service.sendMessage('ping');
      await Promise.resolve();
      expect(ctx.service.isAwaitingTurn()).toBe(true);
      expect(getQueue).not.toHaveBeenCalled();
      await vi.advanceTimersByTimeAsync(5_000);
      expect(getQueue).toHaveBeenCalledTimes(1);
      await vi.advanceTimersByTimeAsync(5_000);
      expect(getQueue).toHaveBeenCalledTimes(2);

      fireSseMessage(ctx.sseInstances[0], { method: 'turn.started', params: { turn_id: 1 } }, '1:2');
      expect(ctx.service.isAwaitingTurn()).toBe(false);
      expect(ctx.service.queueState()).toBeNull();
      await vi.advanceTimersByTimeAsync(15_000);
      expect(getQueue).toHaveBeenCalledTimes(2);
    } finally {
      vi.useRealTimers();
    }
  });

  it('a poll that reports parked ends the awaiting stretch', async () => {
    vi.useFakeTimers();
    try {
      const ctx = await readyOn('thread-pp');
      (ctx.mockApi as any).getThreadQueue = vi.fn().mockReturnValue(of(parkedBlock));
      ctx.mockHttp.post.mockReturnValue(
        of({ accepted: true, turn_id: 1, queue: { ...parkedBlock, state: 'queued', park_reason: null } }),
      );
      await ctx.service.sendMessage('ping');
      await Promise.resolve();
      expect(ctx.service.isAwaitingTurn()).toBe(true);
      await vi.advanceTimersByTimeAsync(5_000);
      expect(ctx.service.isParked()).toBe(true);
      expect(ctx.service.isAwaitingTurn()).toBe(false);
    } finally {
      vi.useRealTimers();
    }
  });

  it('a thread switch clears the parked state (accounting is per thread)', async () => {
    const ctx = await readyOn('thread-a');
    ctx.mockHttp.post.mockReturnValue(of({ accepted: true, turn_id: 1, queue: parkedBlock }));
    await ctx.service.sendMessage('x');
    await Promise.resolve();
    expect(ctx.service.isParked()).toBe(true);
    ctx.mockHttp.get.mockImplementation(() =>
      of({ status: 'active', total_turns: 0, messages: [], total: 0 }),
    );
    await ctx.service.connect('thread-b');
    expect(ctx.service.queueState()).toBeNull();
    expect(ctx.service.isParked()).toBe(false);
  });

  it('retryParked re-queues on 200 and shows the waiting state again', async () => {
    const ctx = await readyOn('thread-r');
    ctx.mockHttp.post.mockReturnValue(of({ accepted: true, turn_id: 1, queue: parkedBlock }));
    await ctx.service.sendMessage('x');
    await Promise.resolve();
    (ctx.mockApi as any).retryThreadQueue = vi.fn().mockReturnValue(of({ kind: 'ok', state: 'queued' }));
    expect(await ctx.service.retryParked()).toBe('ok');
    expect((ctx.mockApi as any).retryThreadQueue).toHaveBeenCalledWith('thread-r');
    expect(ctx.service.isParked()).toBe(false);
    expect(ctx.service.queueState()?.state).toBe('queued');
    expect(ctx.service.isAwaitingTurn()).toBe(true);
  });

  it('retryParked leaves the parked bubble and toasts the server code on refusal', async () => {
    const ctx = await readyOn('thread-x');
    ctx.mockHttp.post.mockReturnValue(of({ accepted: true, turn_id: 1, queue: parkedBlock }));
    await ctx.service.sendMessage('x');
    await Promise.resolve();
    (ctx.mockApi as any).retryThreadQueue = vi
      .fn()
      .mockReturnValue(of({ kind: 'refused', status: 409, code: 'claim_loss_hold' }));
    expect(await ctx.service.retryParked()).toBe('refused');
    expect(ctx.service.isParked()).toBe(true);
    expect((TestBed.inject(AppToastService) as any).danger).toHaveBeenCalled();
  });
});
