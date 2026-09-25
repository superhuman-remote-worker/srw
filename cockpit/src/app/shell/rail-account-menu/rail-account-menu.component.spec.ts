import {describe, expect, it, vi} from 'vitest';
import {Injector, runInInjectionContext, signal} from '@angular/core';
import {Router} from '@angular/router';
import {RailAccountMenuComponent} from './rail-account-menu.component';
import {UserService} from '../../core/services/user.service';
import {ViewportService} from '../../core/services/viewport.service';
import type {User} from '../../core/models/api.model';

/**
 * Builds the component directly with stub providers — no TestBed, matching
 * the sidebar's own spec (see sidebar.component.spec.ts). The menu's gating
 * is pure signal logic; it needs no rendered DOM.
 */
function create(opts: {mobile?: boolean} = {}) {
  const router = {navigate: vi.fn()};
  const userService = {
    currentUser: signal({is_admin: false} as User | null),
    logout: vi.fn(),
  };
  const injector = Injector.create({
    providers: [
      {provide: Router, useValue: router},
      {provide: UserService, useValue: userService},
      {provide: ViewportService, useValue: {isMobile: signal(opts.mobile ?? false)}},
    ],
  });
  const component = runInInjectionContext(injector, () => new RailAccountMenuComponent());
  return {component, router, userService};
}

describe('RailAccountMenuComponent', () => {
  it('offers the Workbench on desktop', () => {
    expect(create().component.showWorkbench()).toBe(true);
  });

  it('withholds the Workbench on mobile', () => {
    expect(create({mobile: true}).component.showWorkbench()).toBe(false);
  });

  it('opens Settings at its door, which lands on the first section', () => {
    const {component, router} = create();
    component.go('/settings');
    expect(router.navigate).toHaveBeenCalledWith(['/settings']);
  });

  it('logs out through the user service', () => {
    const {component, userService} = create();
    component.logout();
    expect(userService.logout).toHaveBeenCalled();
  });
});
