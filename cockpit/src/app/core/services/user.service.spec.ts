import {TestBed} from '@angular/core/testing';
import {provideHttpClient} from '@angular/common/http';
import {HttpTestingController, provideHttpClientTesting} from '@angular/common/http/testing';
import {afterEach, beforeEach, describe, expect, it} from 'vitest';

import {UserService} from './user.service';
import {SessionService} from './session.service';
import {environment} from '../environment';

describe('UserService.loadCurrentUser', () => {
  let service: UserService;
  let http: HttpTestingController;
  const session = {authenticated: {set: (_: boolean) => undefined}};

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [
        provideHttpClient(),
        provideHttpClientTesting(),
        {provide: SessionService, useValue: session},
      ],
    });
    service = TestBed.inject(UserService);
    http = TestBed.inject(HttpTestingController);
  });
  afterEach(() => http.verify());

  const me = () => http.expectOne(`${environment.apiUrl}/auth/me`);

  it('starts loading and ends loaded with the user', async () => {
    expect(service.loadState()).toBe('loading');
    const done = service.loadCurrentUser();
    me().flush({user: {id: 'u1', is_approved: true}});

    expect(await done).toBe(true);
    expect(service.loadState()).toBe('loaded');
    expect(service.currentUserId()).toBe('u1');
  });

  it('fails with status 0 when the API is unreachable', async () => {
    const done = service.loadCurrentUser();
    me().error(new ProgressEvent('error'), {status: 0, statusText: ''});

    expect(await done).toBe(false);
    expect(service.loadState()).toBe('failed');
    expect(service.loadErrorStatus()).toBe(0);
    expect(service.currentUser()).toBeNull();
  });

  it('fails with the status of a server error', async () => {
    const done = service.loadCurrentUser();
    me().flush('Bad Gateway', {status: 502, statusText: 'Bad Gateway'});

    expect(await done).toBe(false);
    expect(service.loadState()).toBe('failed');
    expect(service.loadErrorStatus()).toBe(502);
  });

  it('leaves a 401 to the login redirect instead of failing', async () => {
    const done = service.loadCurrentUser();
    me().flush({detail: 'Not authenticated'}, {status: 401, statusText: 'Unauthorized'});

    expect(await done).toBe(false);
    expect(service.loadState()).toBe('loading');
  });

  it('a later success clears the failure', async () => {
    const first = service.loadCurrentUser();
    me().flush('down', {status: 503, statusText: 'Service Unavailable'});
    await first;

    const second = service.loadCurrentUser();
    me().flush({user: {id: 'u1', is_approved: true}});

    expect(await second).toBe(true);
    expect(service.loadState()).toBe('loaded');
    expect(service.loadErrorStatus()).toBeNull();
  });
});
