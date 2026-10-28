import {signal} from '@angular/core';
import {TestBed} from '@angular/core/testing';
import {ActivatedRouteSnapshot, RouterStateSnapshot} from '@angular/router';
import {afterEach, beforeEach, describe, expect, it, vi} from 'vitest';

import {authGuard} from './auth.guard';
import {SessionService} from '../services/session.service';
import {UserService, UserLoadState} from '../services/user.service';
import {User} from '../models/api.model';

describe('authGuard', () => {
  const session = {login: vi.fn()};
  const currentUser = signal<User | null>(null);
  const loadState = signal<UserLoadState>('loading');

  beforeEach(() => {
    vi.useFakeTimers();
    session.login.mockReset();
    currentUser.set(null);
    loadState.set('loading');
    TestBed.configureTestingModule({
      providers: [
        {provide: SessionService, useValue: session},
        {provide: UserService, useValue: {currentUser, loadState}},
      ],
    });
  });
  afterEach(() => vi.useRealTimers());

  const run = () =>
    TestBed.runInInjectionContext(() =>
      authGuard({} as ActivatedRouteSnapshot, {} as RouterStateSnapshot),
    );

  it('admits a loaded user', () => {
    currentUser.set({id: 'u1'} as User);
    expect(run()).toBe(true);
  });

  it('refuses without a login redirect when the API could not be reached', () => {
    loadState.set('failed');

    expect(run()).toBe(false);
    vi.runAllTimers();
    expect(session.login).not.toHaveBeenCalled();
  });

  it('still sends a user with no session to login', async () => {
    const decision = run() as Promise<boolean>;
    vi.advanceTimersByTime(1500);

    expect(await decision).toBe(false);
    expect(session.login).toHaveBeenCalledOnce();
  });
});
