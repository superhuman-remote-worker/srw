import {afterEach, describe, expect, it, vi} from 'vitest';
import {CUSTOM_ELEMENTS_SCHEMA, Injectable, Injector, runInInjectionContext, signal} from '@angular/core';
import {TestBed} from '@angular/core/testing';
import {Router} from '@angular/router';
import {Subject} from 'rxjs';
import {
  provideTransloco,
  Translation,
  TranslocoDirective,
  TranslocoLoader,
} from '@jsverse/transloco';

import {NotificationBellComponent} from './notification-bell.component';
import {ActionCenterService} from '../../core/services/action-center.service';

function create(counts: {notifications: number; unseen: number; total: number}, badge: number) {
  const actionCenter = {
    counts: signal({...counts, byCategory: {}}),
    badgeCount: signal(badge),
  };
  const router = {navigate: vi.fn()};
  const injector = Injector.create({
    providers: [
      {provide: ActionCenterService, useValue: actionCenter},
      {provide: Router, useValue: router},
    ],
  });
  const component = runInInjectionContext(injector, () => new NotificationBellComponent());
  return {component, router};
}

/** Stand-in for the `*transloco` directive's `t`. */
const t = (key: string, params?: Record<string, unknown>) => (params ? `${key}:${params['n']}` : key);

describe('NotificationBellComponent', () => {
  it('leads the tooltip with the server unseen count and adds the pending total when it differs', () => {
    const {component} = create({notifications: 3, unseen: 2, total: 3}, 2);
    expect(component.tooltipText(t)).toBe('notificationBell.unseenPlural:2, notificationBell.pendingPlural:3');
  });

  it('singular unseen copy, no pending suffix when every pending row is the unseen one', () => {
    const {component} = create({notifications: 1, unseen: 1, total: 1}, 1);
    expect(component.tooltipText(t)).toBe('notificationBell.unseenSingle:1');
  });

  it('falls back to the title when nothing is unseen (the badge is unseen-driven)', () => {
    const {component} = create({notifications: 4, unseen: 0, total: 4}, 0);
    expect(component.tooltipText(t)).toBe('notificationBell.title');
  });

  it('routes to the inbox', () => {
    const {component, router} = create({notifications: 0, unseen: 0, total: 0}, 0);
    component.goToInbox();
    expect(router.navigate).toHaveBeenCalledWith(['/inbox']);
  });
});

/** The app's HTTP loader, with the response held back until the test lets it
 *  through — the bell is in the rail, which renders before en.json arrives. */
@Injectable()
class HeldLoader implements TranslocoLoader {
  static readonly response = new Subject<Translation>();
  getTranslation() {
    return HeldLoader.response;
  }
}

describe('NotificationBellComponent — startup, before the locale has loaded', () => {
  afterEach(() => vi.restoreAllMocks());

  it('never asks for a translation the locale cannot answer yet, and titles itself once it can', () => {
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
    TestBed.configureTestingModule({
      providers: [
        provideTransloco({
          config: {
            availableLangs: ['en'],
            defaultLang: 'en',
            reRenderOnLangChange: true,
            prodMode: false,
            missingHandler: {logMissingKey: true},
          },
          loader: HeldLoader,
        }),
        {
          provide: ActionCenterService,
          useValue: {counts: signal({notifications: 0, unseen: 0, total: 0, byCategory: {}}), badgeCount: signal(0)},
        },
        {provide: Router, useValue: {navigate: vi.fn()}},
      ],
    });
    // app-icon is left unknown: the vitest JIT harness does not wire its
    // signal inputs (reference_directive_output_needs_decorator_in_specs).
    TestBed.overrideComponent(NotificationBellComponent, {
      set: {imports: [TranslocoDirective], schemas: [CUSTOM_ELEMENTS_SCHEMA]},
    });
    const fixture = TestBed.createComponent(NotificationBellComponent);
    fixture.detectChanges();

    expect(warn.mock.calls.flat().filter((m) => String(m).includes('Missing translation'))).toEqual([]);

    HeldLoader.response.next({notificationBell: {title: 'Action Center'}});
    HeldLoader.response.complete();
    fixture.detectChanges();

    const bell = fixture.nativeElement.querySelector('.bell-btn');
    expect(bell?.getAttribute('title')).toBe('Action Center');
    expect(warn.mock.calls.flat().filter((m) => String(m).includes('Missing translation'))).toEqual([]);
  });
});
