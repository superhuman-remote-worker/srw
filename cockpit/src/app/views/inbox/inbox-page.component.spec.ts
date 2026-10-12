import {afterEach, describe, expect, it, vi} from 'vitest';
import {DestroyRef, Injector, NgZone, TemplateRef, runInInjectionContext, signal} from '@angular/core';
import {ActivatedRoute} from '@angular/router';
import {of} from 'rxjs';
import {TranslocoService} from '@jsverse/transloco';

import {InboxPageComponent} from './inbox-page.component';
import {ActionCenterService} from '../../core/services/action-center.service';
import {NotificationService} from '../../core/services/notification.service';
import {ViewportService} from '../../core/services/viewport.service';
import {SidebarService} from '../../core/services/sidebar.service';
import {RailTakeoverService} from '../../core/services/rail-takeover.service';
import {
  EMPTY_NOTIFICATION_COUNTS,
  Notification,
  NotificationCounts,
} from '../../core/models/notification.model';

/**
 * The inbox over the unified feed (slice 3): every item is a feed row, the
 * chips are categories, and the legacy email deep links (`?sudo=`,
 * `?job=&thread=`, `?job=&review=1`) resolve to the row whose `source_ref`
 * matches — fetched from the server when it is not in the loaded page.
 *
 * Construction mirrors automations-page.component.spec.ts: minimal injection
 * context, no TestBed.createComponent — the spec asserts the data the
 * template binds and the component behavior; the rendering path is
 * exercised on the dev cluster.
 */

const THREAD_ID = 'a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d';
const JOB_ID = 'f0e1d2c3-b4a5-4697-8877-665544332211';

function feedRow(id: string, overrides: Partial<Notification> = {}): Notification {
  return {
    id,
    category: 'review_queue',
    severity: 'normal',
    subject: 'Job abc completed — review required',
    body: '**Job** …',
    source_ref: {kind: 'job', id: JOB_ID},
    actions: [
      {type: 'approve', label_key: 'notifications.actions.approve', style: 'primary', params: {job_id: JOB_ID}},
    ],
    payload: {job_id: JOB_ID, config_name: 'worker_base'},
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

function createComponent(
  feed: Notification[] = [],
  queryParams: Record<string, string> = {},
  counts: NotificationCounts = EMPTY_NOTIFICATION_COUNTS,
  layout: {mobile?: boolean; collapsed?: boolean} = {},
) {
  const feedSignal = signal<Notification[]>(feed);
  const notificationsMock = {
    feed: feedSignal,
    feedCounts: signal(counts),
    feedNextBefore: signal<string | null>(null),
    isConnected: signal(false),
    loadNotifications: vi.fn(),
    loadMoreFeed: vi.fn(),
    listBySource: vi.fn().mockReturnValue(of({items: [], next_before: null, counts: EMPTY_NOTIFICATION_COUNTS})),
    connectSSE: vi.fn(),
    disconnectSSE: vi.fn(),
    markSeen: vi.fn().mockReturnValue(of({updated: []})),
    markReadV2: vi.fn().mockImplementation((id: string) =>
      of({notification: {...feedRow(id), read_at: '2026-08-26T09:05:00Z', seen_at: '2026-08-26T09:05:00Z'}}),
    ),
    act: vi.fn().mockReturnValue(of({status: 'ok', result: {}, notification: feedRow('n-1')})),
    getNotification: vi.fn().mockReturnValue(of(null)),
    patchFeedRow: vi.fn(),
    upsertFeedRow: vi.fn((row: Notification) => {
      feedSignal.update((rows) =>
        rows.some((r) => r.id === row.id) ? rows.map((r) => (r.id === row.id ? row : r)) : [row, ...rows],
      );
    }),
  };

  const baseInjector = Injector.create({
    providers: [
      {provide: NotificationService, useValue: notificationsMock},
      {provide: DestroyRef, useValue: {onDestroy: vi.fn()}},
    ],
  });
  const actionCenter = runInInjectionContext(baseInjector, () => new ActionCenterService());

  const isMobile = signal(layout.mobile ?? false);
  const collapsed = signal(layout.collapsed ?? false);
  const takeover = new RailTakeoverService();

  const zoneMock = {
    run: (fn: () => unknown) => fn(),
    runOutsideAngular: (fn: () => unknown) => fn(),
  };

  const injector = Injector.create({
    providers: [
      {provide: ActionCenterService, useValue: actionCenter},
      {provide: NotificationService, useValue: notificationsMock},
      {provide: ActivatedRoute, useValue: {queryParams: of(queryParams)}},
      {provide: NgZone, useValue: zoneMock},
      {provide: TranslocoService, useValue: {translate: (key: string) => key, getActiveLang: () => 'en'}},
      {provide: ViewportService, useValue: {isMobile}},
      {provide: SidebarService, useValue: {collapsed}},
      {provide: RailTakeoverService, useValue: takeover},
    ],
  });

  const component = runInInjectionContext(injector, () => new InboxPageComponent());
  return {component, actionCenter, notificationsMock, isMobile, collapsed, takeover};
}

describe('InboxPageComponent — unified feed', () => {
  let cleanup: (() => void) | null = null;

  afterEach(() => {
    cleanup?.();
    cleanup = null;
  });

  it('a feed row renders as an item and selecting it marks it read', () => {
    const {component, actionCenter, notificationsMock} = createComponent([feedRow('n-1')]);
    cleanup = () => component.ngOnDestroy();
    component.ngOnInit();

    expect(notificationsMock.loadNotifications).toHaveBeenCalledTimes(1);
    const item = actionCenter.items()[0];
    expect(item.id).toBe('ntf:n-1');
    expect(item.category).toBe('review_queue');

    component.selectItem(item);
    expect(component.selectedItem()?.id).toBe('ntf:n-1');
    expect(notificationsMock.markReadV2).toHaveBeenCalledWith('n-1');
  });

  it('resolves the email deep link ?n=<id> to the feed row', () => {
    const {component} = createComponent([feedRow('n-1')], {n: 'n-1'});
    cleanup = () => component.ngOnDestroy();

    component.ngOnInit();

    expect(component.selectedItem()?.id).toBe('ntf:n-1');
  });

  it('fetches a deep-linked row that is not in the loaded page', () => {
    const {component, notificationsMock} = createComponent([], {n: 'deep'});
    notificationsMock.getNotification.mockReturnValue(of({notification: feedRow('deep'), source: null}));
    cleanup = () => component.ngOnDestroy();

    component.ngOnInit();

    expect(notificationsMock.getNotification).toHaveBeenCalledWith('deep');
    expect(component.selectedItem()?.id).toBe('ntf:deep');
  });

  describe('legacy email deep links resolve by source', () => {
    it('?sudo=<request_id> selects the sudo_request row from the loaded page', () => {
      const {component, notificationsMock} = createComponent(
        [feedRow('s-1', {category: 'sudo_request', source_ref: {kind: 'sudo_request', id: 'req-9'}})],
        {sudo: 'req-9'},
      );
      cleanup = () => component.ngOnDestroy();

      component.ngOnInit();

      expect(component.selectedItem()?.id).toBe('ntf:s-1');
      expect(notificationsMock.listBySource).not.toHaveBeenCalled();
    });

    it('?sudo=<request_id> asks the server when the row is older than the loaded page', () => {
      const {component, notificationsMock} = createComponent([], {sudo: 'req-old'});
      notificationsMock.listBySource.mockReturnValue(
        of({
          items: [feedRow('s-old', {category: 'sudo_request', source_ref: {kind: 'sudo_request', id: 'req-old'}})],
          next_before: null,
          counts: EMPTY_NOTIFICATION_COUNTS,
        }),
      );
      cleanup = () => component.ngOnDestroy();

      component.ngOnInit();

      expect(notificationsMock.listBySource).toHaveBeenCalledWith('sudo_request', 'req-old', 1);
      expect(component.selectedItem()?.id).toBe('ntf:s-old');
    });

    it('?job=<id>&thread=<tid> selects the agent message thread row', () => {
      const {component} = createComponent(
        [feedRow('m-1', {category: 'agent_message', source_ref: {kind: 'message_thread', id: 'ab12cd'}})],
        {job: JOB_ID, thread: 'ab12cd'},
      );
      cleanup = () => component.ngOnDestroy();

      component.ngOnInit();

      expect(component.selectedItem()?.id).toBe('ntf:m-1');
    });

    it('the officer-page shape ?job={thread}&thread={thread} falls back to the session-keyed row', () => {
      const {component, notificationsMock} = createComponent(
        [feedRow('p-1', {category: 'officer_question', source_ref: {kind: 'thread', id: THREAD_ID}})],
        {job: THREAD_ID, thread: THREAD_ID},
      );
      cleanup = () => component.ngOnDestroy();

      component.ngOnInit();

      // message_thread miss goes to the server once, then the thread row is found locally.
      expect(notificationsMock.listBySource).toHaveBeenCalledWith('message_thread', THREAD_ID, 1);
      expect(component.selectedItem()?.id).toBe('ntf:p-1');
    });

    it('?job=<id>&review=1 selects the job-keyed review row', () => {
      const {component} = createComponent([feedRow('n-1')], {job: JOB_ID, review: '1'});
      cleanup = () => component.ngOnDestroy();

      component.ngOnInit();

      expect(component.selectedItem()?.id).toBe('ntf:n-1');
    });

    it('a link to a source nothing was recorded about selects nothing', () => {
      const {component} = createComponent([feedRow('n-1')], {job: 'ghost', review: '1'});
      cleanup = () => component.ngOnDestroy();

      component.ngOnInit();

      expect(component.selectedItem()).toBeNull();
    });
  });

  describe('category chips and filters', () => {
    it('chips follow the catalog order with server pending counts; unknown categories append', () => {
      const {component} = createComponent(
        [
          feedRow('z-1', {category: 'zebra_future'}),
          feedRow('n-1'),
          feedRow('s-1', {category: 'sudo_request'}),
        ],
        {},
        {
          unseen: 0,
          unread: 0,
          pending: 3,
          by_category: {review_queue: {pending: 1, unseen: 0}, agent_message: {pending: 4, unseen: 2}},
        },
      );
      cleanup = () => component.ngOnDestroy();

      const chips = component.categoryChips();
      expect(chips.map((c) => c.category)).toEqual([
        'review_queue',
        'sudo_request',
        'agent_message',
        'zebra_future',
      ]);
      expect(chips.find((c) => c.category === 'agent_message')?.pending).toBe(4);
      expect(chips.find((c) => c.category === 'sudo_request')?.pending).toBe(0);
    });

    it('a category filter and its numeric shortcut narrow the list', () => {
      const {component} = createComponent([
        feedRow('n-1'),
        feedRow('s-1', {category: 'sudo_request', severity: 'critical'}),
      ]);
      cleanup = () => component.ngOnDestroy();
      component.ngOnInit();

      expect(component.filteredItems()).toHaveLength(2);
      component.setFilter('sudo_request');
      expect(component.filteredItems().map((i) => i.id)).toEqual(['ntf:s-1']);

      // `2` = the second chip (sudo_request sorts after review_queue); `0` clears.
      component.onKeydown({key: '1', target: document.body, preventDefault: () => undefined} as unknown as KeyboardEvent);
      expect(component.filteredItems().map((i) => i.id)).toEqual(['ntf:n-1']);
      component.onKeydown({key: '0', target: document.body, preventDefault: () => undefined} as unknown as KeyboardEvent);
      expect(component.filteredItems()).toHaveLength(2);
    });
  });

  it('severity drives the urgency bar colour', () => {
    const {component, actionCenter} = createComponent([feedRow('crit')]);
    const item = {...actionCenter.items()[0]};
    item.notification = {...item.notification, severity: 'critical'};
    expect(component.urgencyColor(item)).toBe('red');
    item.notification = {...item.notification, severity: 'high'};
    expect(component.urgencyColor(item)).toBe('amber');
    item.notification = {...item.notification, severity: 'low'};
    expect(component.urgencyColor(item)).toBe('muted');
    item.status = 'resolved';
    expect(component.urgencyColor(item)).toBe('muted');
  });

  it('j/k walk the filtered list and Escape deselects', () => {
    const {component} = createComponent([feedRow('a'), feedRow('b', {severity: 'low'})]);
    cleanup = () => component.ngOnDestroy();
    component.ngOnInit();
    const key = (k: string) =>
      component.onKeydown({key: k, target: document.body, preventDefault: () => undefined} as unknown as KeyboardEvent);

    key('j');
    expect(component.selectedItem()?.id).toBe('ntf:a');
    key('j');
    expect(component.selectedItem()?.id).toBe('ntf:b');
    key('k');
    expect(component.selectedItem()?.id).toBe('ntf:a');
    key('Escape');
    expect(component.selectedItem()).toBeNull();
  });
});

// navigation_fixed_rail.md F9: on desktop the feed replaces the rail's rows
// instead of standing beside them as a second list column.
describe('InboxPageComponent — feed in the rail', () => {
  const railFeed = {} as TemplateRef<unknown>;

  function lent(layout: {mobile?: boolean; collapsed?: boolean} = {}) {
    const created = createComponent([], {}, EMPTY_NOTIFICATION_COUNTS, layout);
    // The spec never renders the template; stand in for the static query.
    (created.component as unknown as {railFeed: TemplateRef<unknown>}).railFeed = railFeed;
    created.component.ngOnInit();
    return created;
  }

  it('lends the feed to the rail on desktop while the rail is open', () => {
    const {component, takeover} = lent();
    expect(component.listInRail()).toBe(true);
    expect(takeover.template()).toBe(railFeed);
    component.ngOnDestroy();
  });

  it('keeps the feed in the page on a phone, where the rail is a drawer', () => {
    const {component, takeover} = lent({mobile: true});
    expect(component.listInRail()).toBe(false);
    expect(takeover.template()).toBeNull();
    component.ngOnDestroy();
  });

  // A collapsed rail would otherwise hide the feed altogether.
  it('takes the feed back into the page while the rail is collapsed', () => {
    const {component, takeover, collapsed} = lent();
    collapsed.set(true);
    expect(component.listInRail()).toBe(false);
    expect(takeover.template()).toBeNull();
    collapsed.set(false);
    expect(takeover.template()).toBe(railFeed);
    component.ngOnDestroy();
  });

  it('hands the rail back when the page goes away', () => {
    const {component, takeover} = lent();
    component.ngOnDestroy();
    expect(takeover.template()).toBeNull();
  });
});
