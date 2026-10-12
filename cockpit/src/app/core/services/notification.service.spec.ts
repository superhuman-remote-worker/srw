import {describe, expect, it, vi} from 'vitest';
import {Injector, NgZone, runInInjectionContext} from '@angular/core';
import {HttpClient} from '@angular/common/http';
import {TranslocoService} from '@jsverse/transloco';
import {of, throwError} from 'rxjs';
import {NotificationService} from './notification.service';
import {AppToastService} from '../../ui/toast';
import {EMPTY_NOTIFICATION_COUNTS, Notification} from '../models/notification.model';

function createService() {
  const http = {
    get: vi.fn().mockReturnValue(of({items: [], next_before: null, counts: EMPTY_NOTIFICATION_COUNTS})),
    patch: vi.fn().mockReturnValue(of({status: 'ok'})),
    post: vi.fn().mockReturnValue(of({})),
  };
  const toast = {info: vi.fn(), warning: vi.fn(), success: vi.fn(), danger: vi.fn()};
  const transloco = {translate: vi.fn((key: string) => key)};

  const injector = Injector.create({
    providers: [
      {provide: HttpClient, useValue: http},
      {provide: AppToastService, useValue: toast},
      {provide: TranslocoService, useValue: transloco},
      {provide: NgZone, useValue: {run: (fn: () => void) => fn()}},
    ],
  });

  const service = runInInjectionContext(injector, () => new NotificationService());
  return {service, http, toast, transloco};
}

function row(overrides: Partial<Notification> = {}): Notification {
  return {
    id: 'n-1',
    category: 'review_queue',
    severity: 'normal',
    subject: 's',
    body: 'b',
    source_ref: {kind: 'job', id: 'j-1'},
    actions: [],
    payload: {},
    created_at: '2026-08-26T09:00:00Z',
    seen_at: null,
    read_at: null,
    interacted_at: null,
    resolved_at: null,
    resolved_by: null,
    archived_at: null,
    ...overrides,
  };
}

describe('NotificationService — closed SSE recovery', () => {
  it('reopens the owner feed after a temporary API outage without another init call', async () => {
    vi.useFakeTimers();
    const original = globalThis.EventSource;
    const streams: any[] = [];
    class FakeEventSource {
      static readonly CONNECTING = 0;
      static readonly OPEN = 1;
      static readonly CLOSED = 2;
      readyState = 0;
      onopen: (() => void) | null = null;
      onerror: (() => void) | null = null;
      onmessage: (() => void) | null = null;
      close = vi.fn(() => { this.readyState = 2; });
      constructor() { streams.push(this); }
    }
    (globalThis as any).EventSource = FakeEventSource;
    try {
      const {service, http} = createService();
      let available = true;
      http.get.mockImplementation(() => available
        ? of({items: [], next_before: null, counts: EMPTY_NOTIFICATION_COUNTS})
        : throwError(() => ({status: 503})));
      service.connectSSE();
      streams[0].readyState = 1;
      streams[0].onopen();
      expect(service.isConnected()).toBe(true);

      available = false;
      streams[0].readyState = 2;
      streams[0].onerror();
      expect(service.isConnected()).toBe(false);
      await vi.advanceTimersByTimeAsync(2_000);
      expect(streams).toHaveLength(1);

      available = true;
      await vi.advanceTimersByTimeAsync(35_000);
      expect(streams.length).toBeGreaterThan(1);
      streams.at(-1).readyState = 1;
      streams.at(-1).onopen();
      await vi.advanceTimersByTimeAsync(0);
      expect(service.isConnected()).toBe(true);
      service.disconnectSSE();
    } finally {
      (globalThis as any).EventSource = original;
      vi.useRealTimers();
    }
  });

  it('retries a CLOSED initial handshake without a prior successful open', async () => {
    vi.useFakeTimers();
    const original = globalThis.EventSource;
    const streams: any[] = [];
    class FakeEventSource {
      static readonly CLOSED = 2;
      readyState = 0;
      onopen: (() => void) | null = null;
      onerror: (() => void) | null = null;
      close = vi.fn(() => { this.readyState = 2; });
      constructor() { streams.push(this); }
    }
    (globalThis as any).EventSource = FakeEventSource;
    try {
      const {service} = createService();
      service.connectSSE();
      streams[0].readyState = 2;
      streams[0].onerror();
      await vi.advanceTimersByTimeAsync(2_000);
      expect(streams).toHaveLength(2);
      streams[1].readyState = 1;
      streams[1].onopen();
      expect(service.isConnected()).toBe(true);
      service.disconnectSSE();
    } finally {
      (globalThis as any).EventSource = original;
      vi.useRealTimers();
    }
  });

  it.each([403, 404])('does not auto-reopen a feed rejected with %i, even on wake', async (status) => {
    vi.useFakeTimers();
    const original = globalThis.EventSource;
    const streams: any[] = [];
    class FakeEventSource {
      static readonly CLOSED = 2;
      readyState = 0;
      onopen: (() => void) | null = null;
      onerror: (() => void) | null = null;
      close = vi.fn(() => { this.readyState = 2; });
      constructor() { streams.push(this); }
    }
    (globalThis as any).EventSource = FakeEventSource;
    try {
      const {service, http} = createService();
      http.get.mockReturnValue(throwError(() => ({status})));
      service.connectSSE();
      streams[0].readyState = 2;
      streams[0].onerror();
      await vi.advanceTimersByTimeAsync(120_000);
      expect(streams).toHaveLength(1);
      expect(service.isConnected()).toBe(false);
      const reads = http.get.mock.calls.length;
      window.dispatchEvent(new Event('online'));
      document.dispatchEvent(new Event('visibilitychange'));
      await vi.advanceTimersByTimeAsync(0);
      expect(http.get.mock.calls).toHaveLength(reads);
      expect(streams).toHaveLength(1);
      service.disconnectSSE();
    } finally {
      (globalThis as any).EventSource = original;
      vi.useRealTimers();
    }
  });

  it('cancels the recovery timer when the feed is intentionally disconnected', async () => {
    vi.useFakeTimers();
    const original = globalThis.EventSource;
    const streams: any[] = [];
    class FakeEventSource {
      static readonly CLOSED = 2;
      readyState = 0;
      onerror: (() => void) | null = null;
      close = vi.fn(() => { this.readyState = 2; });
      constructor() { streams.push(this); }
    }
    (globalThis as any).EventSource = FakeEventSource;
    try {
      const {service} = createService();
      service.connectSSE();
      streams[0].readyState = 2;
      streams[0].onerror();
      service.disconnectSSE();
      await vi.advanceTimersByTimeAsync(60_000);
      expect(streams).toHaveLength(1);
    } finally {
      (globalThis as any).EventSource = original;
      vi.useRealTimers();
    }
  });
});

describe('NotificationService.handleSseEvent — legacy frames that still ride the stream', () => {
  it('user_registered → info toast + adminUserRegistered signal', () => {
    const {service, toast, transloco} = createService();

    service.handleSseEvent({
      type: 'user_registered',
      user_id: 'u-1',
      display_name: 'pending4',
      email: 'p4@test.local',
    });

    expect(service.adminUserRegistered()).toEqual({
      user_id: 'u-1',
      display_name: 'pending4',
      email: 'p4@test.local',
    });
    expect(transloco.translate).toHaveBeenCalledWith('toasts.admin.userRegistered', {
      name: 'pending4',
    });
    expect(toast.info).toHaveBeenCalledOnce();
  });

  it('user_registered falls back to email when display_name missing', () => {
    const {service, transloco} = createService();

    service.handleSseEvent({type: 'user_registered', user_id: 'u-2', email: 'x@y.z'});

    expect(transloco.translate).toHaveBeenCalledWith('toasts.admin.userRegistered', {
      name: 'x@y.z',
    });
  });

  it('session.lifecycle lands on lifecycleEvent', () => {
    const {service} = createService();
    service.handleSseEvent({type: 'session.lifecycle', thread_id: 't-1', state: 'booting', backend: 'vm'});
    expect(service.lifecycleEvent()).toEqual({thread_id: 't-1', state: 'booting', reason: undefined, backend: 'vm'});
  });

  it('session.* frames collect in sessionEvents and session.resolved clears the thread', () => {
    const {service} = createService();
    service.handleSseEvent({type: 'session.permission_request', event_id: 'e-1', thread_id: 't-1', tool: 'bash'});
    service.handleSseEvent({type: 'session.vm_upgrade', event_id: 'e-2', thread_id: 't-2'});
    expect(service.sessionEvents().map((e) => e.event_id)).toEqual(['e-2', 'e-1']);
    service.handleSseEvent({type: 'session.resolved', thread_id: 't-1'});
    expect(service.sessionEvents().map((e) => e.event_id)).toEqual(['e-2']);
  });

  it('reply_delivered reloads the feed', () => {
    const {service, http} = createService();
    service.handleSseEvent({type: 'reply_delivered', thread_id: 'ab12cd'});
    expect(http.get).toHaveBeenCalledTimes(1);
  });

  it('the retired frames (new_message, loop_*, automation_auto_disabled, review_returned_to_manual) are ignored', () => {
    const {service, toast} = createService();

    service.handleSseEvent({type: 'new_message', id: 'n-1', job_id: 'j-1', subject: 'Hi'});
    service.handleSseEvent({type: 'loop_user_question', loop_id: 'l', subject: 'Q'});
    service.handleSseEvent({type: 'loop_campaign_disposition', loop_id: 'l', subject: 'D'});
    service.handleSseEvent({type: 'automation_auto_disabled', automation_name: 'Nightly'});
    service.handleSseEvent({type: 'review_returned_to_manual', job_id: 'j-1'});
    service.handleSseEvent({type: 'something_else', foo: 'bar'});

    expect(service.feed()).toEqual([]);
    expect(service.feedCounts()).toEqual(EMPTY_NOTIFICATION_COUNTS);
    expect(toast.info).not.toHaveBeenCalled();
    expect(toast.warning).not.toHaveBeenCalled();
    expect(service.adminUserRegistered()).toBeNull();
  });

  it('preserves exact runtime identity on lifecycle frames', () => {
    const {service} = createService();

    service.handleSseEvent({
      type: 'session.lifecycle',
      thread_id: 't-1',
      session_runtime_generation: '55555555-5555-4555-8555-555555555555',
      state: 'booting',
    });

    expect(service.lifecycleEvent()).toMatchObject({
      thread_id: 't-1',
      session_runtime_generation: '55555555-5555-4555-8555-555555555555',
      state: 'booting',
    });
  });

  it('exposes cloud.diff_staged as a typed review wake-up edge', () => {
    const {service} = createService();
    const event = {
      type: 'cloud.diff_staged',
      thread_id: 't-1',
      session_runtime_generation: '55555555-5555-4555-8555-555555555555',
      staged_epoch: 8,
      file_count: 3,
      counts: {added: 1, modified: 1, deleted: 1},
      mount_id: 'reader-1',
    };

    service.handleSseEvent(event);

    expect(service.cloudDiffStagedEvent()).toEqual({
      thread_id: event.thread_id,
      session_runtime_generation: event.session_runtime_generation,
      staged_epoch: 8,
      file_count: 3,
      counts: event.counts,
      mount_id: 'reader-1',
    });
  });
});

describe('NotificationService — unified feed frames', () => {
  it('notification frame prepends the row once and bumps the counts (incl. by_category)', () => {
    const {service, toast} = createService();

    service.handleSseEvent({type: 'notification', notification: row()});
    service.handleSseEvent({type: 'notification', notification: row()}); // a replay never re-broadcasts, but be safe

    expect(service.feed().map((n) => n.id)).toEqual(['n-1']);
    expect(service.feedCounts()).toMatchObject({
      unseen: 1,
      unread: 1,
      pending: 1,
      by_category: {review_queue: {pending: 1, unseen: 1}},
    });
    expect(toast.info).not.toHaveBeenCalled();
  });

  it('notification.updated patches engagement and decrements the counts', () => {
    const {service} = createService();
    service.handleSseEvent({type: 'notification', notification: row()});

    service.handleSseEvent({type: 'notification.updated', id: 'n-1', seen_at: '2026-08-26T09:01:00Z'});
    expect(service.feed()[0].seen_at).toBe('2026-08-26T09:01:00Z');
    expect(service.feedCounts().unseen).toBe(0);
    expect(service.feedCounts().by_category['review_queue'].unseen).toBe(0);

    service.handleSseEvent({
      type: 'notification.updated',
      id: 'n-1',
      resolved_at: '2026-08-26T09:02:00Z',
      resolved_by: 'officer:t-1',
    });
    expect(service.feed()[0].resolved_by).toBe('officer:t-1');
    expect(service.feedCounts().pending).toBe(0);
    expect(service.feedCounts().by_category['review_queue'].pending).toBe(0);
    // Unknown id is ignored, not crashed on.
    service.handleSseEvent({type: 'notification.updated', id: 'ghost', read_at: 'x'});
    expect(service.feed()).toHaveLength(1);
  });

  describe('category toasts derived from the row', () => {
    it('automation_disabled → warning toast named from the payload', () => {
      const {service, toast, transloco} = createService();
      service.handleSseEvent({
        type: 'notification',
        notification: row({
          id: 'a-1',
          category: 'automation_disabled',
          source_ref: {kind: 'automation', id: 'auto-1'},
          payload: {automation_name: 'Nightly digest', reason: 'max_fires_per_day'},
        }),
      });
      expect(transloco.translate).toHaveBeenCalledWith('toasts.automations.autoDisabled', {
        name: 'Nightly digest',
      });
      expect(toast.warning).toHaveBeenCalledOnce();
    });

    it('loop_event → info toast with the row subject', () => {
      const {service, toast} = createService();
      service.handleSseEvent({
        type: 'notification',
        notification: row({
          id: 'l-1',
          category: 'loop_event',
          subject: 'Loop question: Should the dice roller support D20 notation?',
          source_ref: {kind: 'loop', id: 'loop-942ef0'},
        }),
      });
      expect(toast.info).toHaveBeenCalledWith(
        'Loop question: Should the dice roller support D20 notation?',
      );
    });

    it('user_registered rows stay silent — the legacy frame already toasts admins', () => {
      const {service, toast} = createService();
      service.handleSseEvent({
        type: 'notification',
        notification: row({id: 'u-1', category: 'user_registered', source_ref: {kind: 'user', id: 'user-9'}}),
      });
      expect(toast.info).not.toHaveBeenCalled();
      expect(toast.warning).not.toHaveBeenCalled();
    });

    it('a re-broadcast of a known row never toasts twice', () => {
      const {service, toast} = createService();
      const n = row({id: 'l-2', category: 'loop_event', subject: 'Loop update'});
      service.handleSseEvent({type: 'notification', notification: n});
      service.handleSseEvent({type: 'notification', notification: n});
      expect(toast.info).toHaveBeenCalledTimes(1);
    });
  });

  it('a feed load reads {items, next_before, counts} and nothing else', () => {
    const {service, http} = createService();
    http.get.mockReturnValue(
      of({
        items: [row()],
        next_before: 'cursor',
        counts: {unseen: 1, unread: 1, pending: 1, by_category: {review_queue: {pending: 1, unseen: 1}}},
      }),
    );
    service.loadNotifications();
    expect(http.get.mock.calls[0][1]).toEqual({params: {limit: '100'}});
    expect(service.feed()).toHaveLength(1);
    expect(service.feedNextBefore()).toBe('cursor');
    expect(service.feedCounts().by_category['review_queue'].pending).toBe(1);
    expect(service.isLoading()).toBe(false);
  });

  it('loadMoreFeed pages behind the cursor and dedupes', () => {
    const {service, http} = createService();
    service.upsertFeedRow(row());
    service.feedNextBefore.set('c-1');
    http.get.mockReturnValue(
      of({items: [row(), row({id: 'n-0'})], next_before: null, counts: EMPTY_NOTIFICATION_COUNTS}),
    );
    service.loadMoreFeed();
    expect(http.get.mock.calls[0][1]).toEqual({params: {before: 'c-1', limit: '50'}});
    expect(service.feed().map((n) => n.id)).toEqual(['n-1', 'n-0']);
    expect(service.feedNextBefore()).toBeNull();
  });

  it('listBySource passes source_kind + source_id together and leaves the feed alone', () => {
    const {service, http} = createService();
    http.get.mockReturnValue(
      of({items: [row({id: 'p-1', source_ref: {kind: 'thread', id: 't-1'}})], next_before: null, counts: EMPTY_NOTIFICATION_COUNTS}),
    );
    let page: unknown = null;
    service.listBySource('thread', 't-1', 10).subscribe((p) => (page = p));
    expect(http.get.mock.calls[0][1]).toEqual({params: {source_kind: 'thread', source_id: 't-1', limit: '10'}});
    expect((page as {items: Notification[]}).items[0].id).toBe('p-1');
    expect(service.feed()).toEqual([]);
  });

  it('markReadV2 PATCHes the /read endpoint (the bare PATCH is gone)', () => {
    const {service, http} = createService();
    service.markReadV2('n-1').subscribe();
    expect(http.patch).toHaveBeenCalledWith(expect.stringMatching(/\/notifications\/n-1\/read$/), {});
  });
});
