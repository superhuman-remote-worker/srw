import {CUSTOM_ELEMENTS_SCHEMA, Pipe, signal, ɵresolveComponentResources} from '@angular/core';
import {TestBed} from '@angular/core/testing';
import {provideRouter} from '@angular/router';
import {beforeAll, beforeEach, describe, expect, it, vi} from 'vitest';

import {App} from './app';
import {UserService, UserLoadState} from './core/services/user.service';
import {ViewportService} from './core/services/viewport.service';
import {SidebarService} from './core/services/sidebar.service';
import {ComponentRegistryService} from './core/services/component-registry.service';
import {ActionCenterService} from './core/services/action-center.service';
import {User} from './core/models/api.model';

@Pipe({name: 'transloco', standalone: true})
class TestTranslocoPipe {
  transform(key: string): string {
    return key;
  }
}

describe('App shell when the user cannot be loaded', () => {
  let user: {
    currentUser: ReturnType<typeof signal<User | null>>;
    loadState: ReturnType<typeof signal<UserLoadState>>;
    loadErrorStatus: ReturnType<typeof signal<number | null>>;
    isAuthenticated: () => boolean;
    isApproved: () => boolean;
    logout: ReturnType<typeof vi.fn>;
  };

  beforeAll(async () => {
    await ɵresolveComponentResources(() => Promise.resolve(''));
  });

  beforeEach(() => {
    const currentUser = signal<User | null>(null);
    user = {
      currentUser,
      loadState: signal<UserLoadState>('loading'),
      loadErrorStatus: signal<number | null>(null),
      isAuthenticated: () => currentUser() !== null,
      isApproved: () => currentUser()?.is_approved ?? false,
      logout: vi.fn(),
    };
    TestBed.configureTestingModule({
      imports: [App],
      providers: [
        provideRouter([]),
        {provide: UserService, useValue: user},
        {provide: ViewportService, useValue: {isMobile: signal(false)}},
        {provide: SidebarService, useValue: {collapsed: signal(false), collapse: vi.fn()}},
        {provide: ComponentRegistryService, useValue: {register: vi.fn()}},
        {provide: ActionCenterService, useValue: {initSSE: vi.fn()}},
      ],
    });
    // Shell children (sidebar, banners, toasts) are not under test.
    TestBed.overrideComponent(App, {
      set: {imports: [TestTranslocoPipe], schemas: [CUSTOM_ELEMENTS_SCHEMA]},
    });
  });

  function render(state: UserLoadState, status: number | null = null) {
    user.loadState.set(state);
    user.loadErrorStatus.set(status);
    const fixture = TestBed.createComponent(App);
    fixture.detectChanges();
    return fixture;
  }

  it('shows Retry and Log out instead of a blank shell when the API is unreachable', () => {
    const root = render('failed', 0).nativeElement as HTMLElement;

    const card = root.querySelector('[role="alert"]');
    expect(card?.textContent).toContain('errors.http.network');
    const buttons = Array.from(card?.querySelectorAll('button') ?? []).map((b) =>
      b.textContent?.trim(),
    );
    expect(buttons).toEqual(['workspaces.list.retry', 'nav.logout']);
    expect(root.querySelector('router-outlet')).toBeNull();
  });

  it('names a server error apart from an unreachable server', () => {
    const root = render('failed', 500).nativeElement as HTMLElement;

    expect(root.querySelector('[role="alert"]')?.textContent).toContain('errors.http.5xx');
  });

  it('Retry reloads the page so the bootstrap and first navigation run again', () => {
    const fixture = render('failed', 503);
    const reload = vi.fn();
    vi.spyOn(document, 'defaultView', 'get').mockReturnValue({
      location: {reload},
    } as unknown as Window & typeof globalThis);

    const [retry] = (fixture.nativeElement as HTMLElement).querySelectorAll('button');
    retry.click();

    expect(reload).toHaveBeenCalledOnce();
    vi.restoreAllMocks();
  });

  it('Log out logs out', () => {
    const fixture = render('failed', 0);

    const buttons = (fixture.nativeElement as HTMLElement).querySelectorAll('button');
    buttons[1].click();

    expect(user.logout).toHaveBeenCalledOnce();
  });

  it('renders the routed shell once the user is loaded', () => {
    user.currentUser.set({id: 'u1', is_approved: true} as User);
    const root = render('loaded').nativeElement as HTMLElement;

    expect(root.querySelector('[role="alert"]')).toBeNull();
    expect(root.querySelector('router-outlet')).not.toBeNull();
  });
});
