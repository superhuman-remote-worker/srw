import {describe, expect, it, vi} from 'vitest';
import {Injector, TemplateRef, computed, runInInjectionContext, signal} from '@angular/core';
import {Location} from '@angular/common';
import {NavigationEnd, Router} from '@angular/router';
import {Subject} from 'rxjs';
import {SidebarComponent, railViewFor} from './sidebar.component';
import {UserService} from '../../core/services/user.service';
import {SidebarService} from '../../core/services/sidebar.service';
import {ViewportService} from '../../core/services/viewport.service';
import {PersistentChatService} from '../../core/services/persistent-chat.service';
import {SessionListService} from '../../core/services/session-list.service';
import {ActionCenterService} from '../../core/services/action-center.service';
import {RailTakeoverService} from '../../core/services/rail-takeover.service';
import {environment} from '../../core/environment';
import type {Thread} from '../../core/models/api.model';

/**
 * Builds the component directly with stub providers — no TestBed. Every
 * dependency is a plain object exposing only what the component reads.
 */
function create(opts: {
  url: string;
  threads?: Partial<Thread>[];
  isAdmin?: boolean;
  /** Router.navigated: true once a first navigation has occurred. Defaults to
   *  true (steady state) — tests exercising the cold-boot distinction set it
   *  explicitly. */
  navigated?: boolean;
  /** What the browser's address bar holds (Location.path()). Defaults to
   *  `url`; a cold boot is where the two differ — router.url is still '/'. */
  locationPath?: string;
  /** SidebarService.collapsed's initial value. Defaults to false (expanded) —
   *  the ⌘K collapse-then-focus test sets it explicitly. */
  collapsed?: boolean;
  /** Server counts per notification category, as the action center holds them. */
  byCategory?: Record<string, {pending: number; unseen: number}>;
  /** ViewportService.isMobile's initial value. Defaults to false (desktop). */
  mobile?: boolean;
}) {
  const threads = signal((opts.threads ?? []) as Thread[]);
  const router = {
    url: opts.url,
    navigated: opts.navigated ?? true,
    navigate: vi.fn(),
    navigateByUrl: vi.fn(),
    events: new Subject(),
  };
  const sessions = {
    threads,
    loading: signal(false),
    refresh: vi.fn(),
    grouped: computed(() =>
      threads().length ? [{label: 'today' as const, threads: threads()}] : [],
    ),
  };
  const sidebarService = {
    collapse: vi.fn(),
    expand: vi.fn(),
    collapsed: signal(opts.collapsed ?? false),
  };
  const counts = signal({notifications: 0, unseen: 0, total: 0, byCategory: opts.byCategory ?? {}});
  const takeover = new RailTakeoverService();
  const injector = Injector.create({
    providers: [
      {provide: Router, useValue: router},
      {provide: Location, useValue: {path: () => opts.locationPath ?? opts.url}},
      {provide: SessionListService, useValue: sessions},
      {provide: UserService, useValue: {
        currentUser: signal({is_admin: opts.isAdmin ?? false}),
        logout: vi.fn(),
      }},
      {provide: SidebarService, useValue: sidebarService},
      {provide: ViewportService, useValue: {isMobile: signal(opts.mobile ?? false)}},
      {provide: RailTakeoverService, useValue: takeover},
      {provide: PersistentChatService, useValue: {threadId: signal(null)}},
      {provide: ActionCenterService, useValue: {counts}},
    ],
  });
  const component = runInInjectionContext(injector, () => new SidebarComponent());
  return {component, router, sessions, sidebarService, counts, takeover};
}

function navigate(router: ReturnType<typeof create>['router'], url: string): void {
  router.events.next(new NavigationEnd(1, url, url));
}

describe('SidebarComponent rail view', () => {
  // navigation_fixed_rail.md F1: every app route shows the same rail — the
  // retired mode switcher left it empty on two of its three modes.
  it.each(['/', '/?foo=bar', '/sessions/abc-123', '/jobs', '/jobs/review', '/projects/p-1', '/automations', '/experts', '/workbench'])(
    'shows the main rail on %s',
    (url) => {
      expect(create({url}).component.railView()).toBe('main');
    },
  );

  // F4: Settings and Admin are one Settings, and it takes the rail over.
  it.each(['/settings/general', '/settings/api-keys', '/settings/ssh-keys', '/admin/models', '/admin/models?tab=catalog', '/admin/subscriptions'])(
    'hands the rail to Settings on %s',
    (url) => {
      expect(create({url}).component.railView()).toBe('settings');
    },
  );

  // F9: the Action Center lends the rail its feed instead of opening a
  // second list column beside it.
  it('hands the rail to the Action Center on /inbox', () => {
    expect(create({url: '/inbox'}).component.railView()).toBe('page');
    expect(create({url: '/inbox?n=abc'}).component.railView()).toBe('page');
  });

  // On a phone the rail is a drawer the user has to open, so the feed stays
  // in the page there and the drawer keeps the main rows.
  it('keeps the main rail on /inbox on a phone', () => {
    expect(create({url: '/inbox', mobile: true}).component.railView()).toBe('main');
    expect(railViewFor('/inbox', true)).toBe('main');
  });

  it('keeps Settings a takeover on a phone', () => {
    expect(railViewFor('/settings/general', true)).toBe('settings');
  });

  it('matches whole path segments, not bare prefixes', () => {
    expect(railViewFor('/settingsfoo')).toBe('main');
    expect(railViewFor('/administrator')).toBe('main');
  });

  // The load flash this fixes: router.url is '/' until the first navigation
  // ends, so seeding from it drew the landing page's rail on the first frame
  // of a hard load of /settings/general (or lit Chat on /projects, before
  // the mode switcher went).
  it('draws the right rail on the first frame of a hard load, before the router has a url', () => {
    const {component} = create({url: '/', navigated: false, locationPath: '/settings/general'});
    expect(component.railView()).toBe('settings');
  });

  it('follows navigation once it ends', () => {
    const {component, router} = create({url: '/'});
    navigate(router, '/admin/users');
    expect(component.railView()).toBe('settings');
    navigate(router, '/jobs');
    expect(component.railView()).toBe('main');
  });
});

describe('SidebarComponent primary rows', () => {
  it.each(['/experts', '/experts/new', '/skills', '/skills/s-1/edit', '/datasources', '/contacts'])(
    'lights Customize on %s',
    (url) => {
      expect(create({url}).component.customizeActive()).toBe(true);
    },
  );

  it.each(['/', '/jobs', '/automations', '/expertsish'])('leaves Customize dark on %s', (url) => {
    expect(create({url}).component.customizeActive()).toBe(false);
  });

  it('badges Jobs with the pending review count', () => {
    const {component} = create({url: '/', byCategory: {review_queue: {pending: 3, unseen: 1}}});
    expect(component.jobsAwaitingReview()).toBe(3);
  });

  it('shows no Jobs badge when nothing awaits review', () => {
    const {component} = create({url: '/', byCategory: {sudo_request: {pending: 2, unseen: 2}}});
    expect(component.jobsAwaitingReview()).toBe(0);
  });

  it('follows the live count', () => {
    const {component, counts} = create({url: '/'});
    counts.update((c) => ({...c, byCategory: {review_queue: {pending: 1, unseen: 1}}}));
    expect(component.jobsAwaitingReview()).toBe(1);
  });
});

describe('SidebarComponent settings rail', () => {
  const paths = (component: SidebarComponent) =>
    component.settingsGroups().flatMap((g) => g.items.map((i) => i.path));

  it('gives every user the Settings and Access groups', () => {
    const {component} = create({url: '/settings/general'});
    expect(component.settingsGroups().map((g) => g.labelKey)).toEqual([
      'settings.nav.groupSettings',
      'settings.nav.groupAccess',
    ]);
  });

  it('shows a non-admin no Administration entry', () => {
    const {component} = create({url: '/settings/general', isAdmin: false});
    expect(paths(component).some((p) => p.startsWith('/admin'))).toBe(false);
  });

  it('adds the Administration group, last, for an admin', () => {
    const {component} = create({url: '/settings/general', isAdmin: true});
    const groups = component.settingsGroups();
    expect(groups.at(-1)?.labelKey).toBe('settings.nav.groupAdmin');
    expect(groups.at(-1)?.items.map((i) => i.path)).toEqual([
      '/admin/models',
      '/admin/subscriptions',
      '/admin/users',
      '/admin/config',
      '/admin/grants',
      '/admin/cloud',
      '/admin/usage',
      '/admin/capacity',
    ]);
  });

  it.each([false, true])('lists MCP and SSH keys only with external clients enabled (%s)', (enabled) => {
    const previous = environment.externalClientsEnabled;
    environment.externalClientsEnabled = enabled;
    try {
      const listed = paths(create({url: '/settings/general'}).component);
      expect(listed).toContain('/settings/api-keys');
      expect(listed.includes('/settings/mcp')).toBe(enabled);
      expect(listed.includes('/settings/ssh-keys')).toBe(enabled);
    } finally {
      environment.externalClientsEnabled = previous;
    }
  });
});

describe('SidebarComponent back to app', () => {
  it('returns to the last app page visited before Settings', () => {
    const {component, router} = create({url: '/'});
    navigate(router, '/sessions/abc-123');
    navigate(router, '/settings/general');
    navigate(router, '/admin/models');
    component.backToApp();
    expect(router.navigateByUrl).toHaveBeenCalledWith('/sessions/abc-123');
  });

  it('keeps the query string of the page it returns to', () => {
    const {component, router} = create({url: '/'});
    navigate(router, '/jobs?status=failed');
    navigate(router, '/settings/defaults');
    component.backToApp();
    expect(router.navigateByUrl).toHaveBeenCalledWith('/jobs?status=failed');
  });

  it('falls back to the draft landing after a hard load straight into Settings', () => {
    const {component, router} = create({url: '/', navigated: false, locationPath: '/settings/general'});
    navigate(router, '/settings/general');
    component.backToApp();
    expect(router.navigateByUrl).toHaveBeenCalledWith('/');
  });

  it('remembers the page it was constructed on once the router has navigated', () => {
    const {component, router} = create({url: '/projects/p-1'});
    navigate(router, '/settings/general');
    component.backToApp();
    expect(router.navigateByUrl).toHaveBeenCalledWith('/projects/p-1');
  });
});

describe('SidebarComponent page takeover', () => {
  it('renders what the page lends it', () => {
    const {component, takeover} = create({url: '/inbox'});
    const feed = {} as TemplateRef<unknown>;
    takeover.claim(feed);
    expect(component.railView()).toBe('page');
    expect((component as unknown as {takeover: RailTakeoverService}).takeover.template()).toBe(feed);
  });

  it('returns from the Action Center to the last app page', () => {
    const {component, router} = create({url: '/'});
    navigate(router, '/jobs');
    navigate(router, '/inbox');
    component.backToApp();
    expect(router.navigateByUrl).toHaveBeenCalledWith('/jobs');
  });
});

describe('SidebarComponent session list', () => {
  it('lists sessions grouped by recency', () => {
    const {component} = create({url: '/', threads: [
      {id: 'a', title: 'Comparing take-home pay', last_activity: new Date().toISOString()},
    ]});
    expect(component.sessionGroups().map((g) => g.label)).toEqual(['today']);
  });

  // Recents is on every main-rail page now, not only in Chat.
  it('lists sessions on a non-chat page too', () => {
    const {component} = create({url: '/jobs', threads: [
      {id: 'a', title: 'Comparing take-home pay', last_activity: new Date().toISOString()},
    ]});
    expect(component.sessionGroups()).toHaveLength(1);
  });

  // The rail must have sessions ready the instant the user is looking at
  // them, even when the app boots somewhere else — so this doesn't gate on
  // the starting *route*. (It does gate on Router.navigated — see the
  // cold-boot pair of tests below; this test relies on create()'s default
  // of navigated: true.)
  it('refreshes the session list once on construction, regardless of the starting route', () => {
    const {sessions} = create({url: '/settings/general'});
    expect(sessions.refresh).toHaveBeenCalledTimes(1);
  });

  it.each(['/', '/jobs', '/projects', '/experts', '/workbench'])('refreshes on navigating to %s, where Recents shows', (url) => {
    const {router, sessions} = create({url: '/'});
    expect(sessions.refresh).toHaveBeenCalledTimes(1); // the construction-time call
    navigate(router, url);
    expect(sessions.refresh).toHaveBeenCalledTimes(2);
  });

  // Settings and the Action Center take the rail over — Recents is not on
  // screen there, and the navigation back out refreshes it anyway.
  it.each(['/settings/general', '/admin/users', '/inbox'])('does not refresh on navigating to %s', (url) => {
    const {router, sessions} = create({url: '/'});
    expect(sessions.refresh).toHaveBeenCalledTimes(1); // the construction-time call
    navigate(router, url);
    expect(sessions.refresh).toHaveBeenCalledTimes(1);
  });

  // Cold-boot double fetch (fix round 1): under Angular's default
  // enabledNonBlocking initial navigation, the sidebar is constructed BEFORE
  // the first navigation completes, so Router.navigated is still false. An
  // unconditional refresh() here would double up with the NavigationEnd
  // handler below firing for that same first navigation.
  it('does not refresh on construction when the router has not navigated yet, but the subsequent NavigationEnd does', () => {
    const {router, sessions} = create({url: '/', navigated: false});
    expect(sessions.refresh).toHaveBeenCalledTimes(0);
    navigate(router, '/');
    expect(sessions.refresh).toHaveBeenCalledTimes(1);
  });

  // A desktop /inbox or /settings can turn back into the main rail without
  // any navigation (narrowed to phone width); Recents must have loaded once.
  it.each(['/inbox', '/settings/general'])('loads the session list on a cold boot straight into %s', (url) => {
    const {router, sessions} = create({url: '/', navigated: false, locationPath: url});
    navigate(router, url);
    expect(sessions.refresh).toHaveBeenCalledTimes(1);
    navigate(router, '/admin/users');
    expect(sessions.refresh).toHaveBeenCalledTimes(1);
  });

  // The other side of the guard: enabledBlocking, SSR, or a remount can
  // construct the sidebar AFTER the first navigation already resolved — no
  // NavigationEnd is coming for it, so construction must fetch directly.
  it('refreshes on construction when the router has already navigated', () => {
    const {sessions} = create({url: '/', navigated: true});
    expect(sessions.refresh).toHaveBeenCalledTimes(1);
  });
});

describe('SidebarComponent session filter', () => {
  it('filters the session list case-insensitively', () => {
    const {component} = create({url: '/', threads: [
      {id: 'a', title: 'Comparing take-home pay', last_activity: new Date().toISOString()},
      {id: 'b', title: 'Kubernetes manifest review', last_activity: new Date().toISOString()},
    ]});
    component.filterText.set('TAKE-HOME');
    expect(component.sessionGroups()[0].threads.map((t) => t.id)).toEqual(['a']);
  });

  it('an empty filter shows every session', () => {
    const {component} = create({url: '/', threads: [
      {id: 'a', title: 'Comparing take-home pay', last_activity: new Date().toISOString()},
      {id: 'b', title: 'Kubernetes manifest review', last_activity: new Date().toISOString()},
    ]});
    component.filterText.set('');
    expect(component.sessionGroups()[0].threads).toHaveLength(2);
  });

  it('drops a group whose every thread was filtered out', () => {
    const {component} = create({url: '/', threads: [
      {id: 'a', title: 'Comparing take-home pay', last_activity: new Date().toISOString()},
    ]});
    component.filterText.set('nothing matches this');
    expect(component.sessionGroups()).toEqual([]);
  });
});

describe('SidebarComponent ⌘K shortcut', () => {
  // filterInput is a @ViewChild, only ever resolved by rendering the real
  // template — this spec never does (see the dynamic-query note on the
  // field). Stand in for it directly; onKeydown only ever reads
  // `.nativeElement.focus()` off it.
  function stubFilterInput(component: ReturnType<typeof create>['component']) {
    const focus = vi.fn();
    (component as any).filterInput = {nativeElement: {focus}};
    return focus;
  }

  function cmdK(): KeyboardEvent {
    return new KeyboardEvent('keydown', {key: 'k', metaKey: true});
  }

  // Regression guard for F1: the rail can be collapsed (width: 0, overflow:
  // hidden) while the filter stays mounted — mobile's default state after
  // every navigation (SidebarService.collapsed defaults true at <=768px, and
  // the rail auto-collapses post-navigation on mobile). Focusing straight
  // into that would strand focus on an invisible control while having
  // already swallowed the browser's own Ctrl+K/⌘K. Asserting call ORDER
  // (not just that both were called) is deliberate: the fix expands before
  // it focuses, and a version that focused first would still pass a looser
  // "both were called" assertion.
  it('expands a collapsed rail before focusing the filter, and swallows the browser shortcut', () => {
    const {component, sidebarService} = create({url: '/', collapsed: true});
    const focus = stubFilterInput(component);
    const event = cmdK();
    const preventDefault = vi.spyOn(event, 'preventDefault');

    component.onKeydown(event);

    expect(preventDefault).toHaveBeenCalled();
    expect(sidebarService.expand).toHaveBeenCalledTimes(1);
    expect(focus).toHaveBeenCalledTimes(1);
    const expandOrder = sidebarService.expand.mock.invocationCallOrder[0];
    const focusOrder = focus.mock.invocationCallOrder[0];
    expect(expandOrder).toBeLessThan(focusOrder);
  });

  it('does not expand an already-expanded rail, but still focuses the filter', () => {
    const {component, sidebarService} = create({url: '/', collapsed: false});
    const focus = stubFilterInput(component);

    component.onKeydown(cmdK());

    expect(sidebarService.expand).not.toHaveBeenCalled();
    expect(focus).toHaveBeenCalledTimes(1);
  });

  // Settings and a page's list replace the main rail, so filterInput is
  // never set there (the @switch in the template), and the browser's own
  // Ctrl+K must survive.
  it('leaves the browser shortcut alone when the filter is not on screen', () => {
    const {component, sidebarService} = create({url: '/settings/general', collapsed: true});
    const event = cmdK();
    const preventDefault = vi.spyOn(event, 'preventDefault');

    component.onKeydown(event);

    expect(preventDefault).not.toHaveBeenCalled();
    expect(sidebarService.expand).not.toHaveBeenCalled();
  });
});

// The "See all sessions" row and the empty-state copy are template-only (a
// static routerLink and an @if on sessionGroups().length) — this component's
// spec never renders the template (see the dynamic-query note on
// filterInput). hasSessions() and clearFilter() are component-level logic,
// so those get real cases.
describe('SidebarComponent rail empty state', () => {
  it('hasSessions reflects the UNFILTERED list, not the filtered one — this is what tells "no sessions yet" apart from "no matches"', () => {
    const {component} = create({url: '/', threads: [
      {id: 'a', title: 'Comparing take-home pay', last_activity: new Date().toISOString()},
    ]});
    component.filterText.set('nothing matches this');

    expect(component.sessionGroups()).toEqual([]);
    expect(component.hasSessions()).toBe(true);
  });

  it('hasSessions is false for a genuinely empty account', () => {
    const {component} = create({url: '/', threads: []});
    expect(component.hasSessions()).toBe(false);
  });
});

describe('SidebarComponent clearFilter', () => {
  it('resets filterText and returns focus to the input', () => {
    const {component} = create({url: '/'});
    component.filterText.set('something');
    const focus = vi.fn();
    (component as any).filterInput = {nativeElement: {focus}};

    component.clearFilter();

    expect(component.filterText()).toBe('');
    expect(focus).toHaveBeenCalledTimes(1);
  });

  it('does not throw when the filter input is not resolved', () => {
    const {component} = create({url: '/'});
    component.filterText.set('something');
    expect(() => component.clearFilter()).not.toThrow();
    expect(component.filterText()).toBe('');
  });
});
