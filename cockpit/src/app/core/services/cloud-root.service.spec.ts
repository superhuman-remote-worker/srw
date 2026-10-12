import {TestBed} from '@angular/core/testing';
import {HttpErrorResponse} from '@angular/common/http';
import {of, throwError} from 'rxjs';
import {afterEach, beforeEach, describe, expect, it, vi} from 'vitest';
import {ApiService} from './api.service';
import {CLOUD_ROOT_POLL_MS, CloudRootService, isWebPage} from './cloud-root.service';
import {CloudRootStatus} from '../models/api.model';

function status(over: Partial<CloudRootStatus> = {}): CloudRootStatus {
  return {
    supported: true,
    reason: null,
    state: 'not_connected',
    connector_id: 'root-1',
    name: 'My cloud',
    account_id: null,
    granted_at: null,
    client_name: null,
    revoked_at: null,
    revoked_reason: null,
    failure: null,
    connecting: null,
    ...over,
  };
}

/**
 * "Connect your cloud" (main cloud slice 4): the tab opens inside the click,
 * the state is read every few seconds while the user signs in, and polling
 * stops at a terminal state or when the flow expires.
 */
describe('CloudRootService', () => {
  let api: Record<string, ReturnType<typeof vi.fn>>;
  let tab: {opener: unknown; location: {href: string}; close: ReturnType<typeof vi.fn>};
  let open: ReturnType<typeof vi.spyOn>;

  function make(): CloudRootService {
    TestBed.configureTestingModule({providers: [CloudRootService, {provide: ApiService, useValue: api}]});
    return TestBed.inject(CloudRootService);
  }

  beforeEach(() => {
    vi.useFakeTimers();
    api = {
      getCloudRoot: vi.fn().mockReturnValue(of(status())),
      connectCloudRoot: vi.fn().mockReturnValue(of({
        state: 'connecting',
        connector_id: 'root-1',
        login_url: 'https://cloud.example/login/v2/flow/abc',
        expires_at: new Date(Date.now() + 60_000).toISOString(),
      })),
      cancelCloudRootConnect: vi.fn().mockReturnValue(of({...status(), cancelled: true})),
      disconnectCloudRoot: vi.fn(),
    };
    tab = {opener: window, location: {href: ''}, close: vi.fn()};
    open = vi.spyOn(window, 'open').mockReturnValue(tab as unknown as Window);
  });

  afterEach(() => {
    vi.useRealTimers();
    open.mockRestore();
    TestBed.resetTestingModule();
  });

  it('opens the cloud sign-in in a tab cut from this page, then reads until connected', () => {
    const service = make();
    service.load();
    service.connect();
    // The tab is opened before the request, inside the click.
    expect(open).toHaveBeenCalledWith('', '_blank');
    expect(tab.opener).toBeNull();
    expect(tab.location.href).toBe('https://cloud.example/login/v2/flow/abc');
    expect(service.status()?.state).toBe('connecting');
    expect(service.status()?.connecting?.login_url).toBe('https://cloud.example/login/v2/flow/abc');

    api['getCloudRoot'].mockReturnValue(of(status({state: 'connecting', connecting: {login_url: 'x', expires_at: new Date(Date.now() + 60_000).toISOString()}})));
    vi.advanceTimersByTime(CLOUD_ROOT_POLL_MS);
    expect(api['getCloudRoot']).toHaveBeenCalledTimes(2);
    api['getCloudRoot'].mockReturnValue(of(status({state: 'connected', account_id: 'alice'})));
    vi.advanceTimersByTime(CLOUD_ROOT_POLL_MS);
    expect(service.status()?.state).toBe('connected');
    // A terminal state ends the polling.
    vi.advanceTimersByTime(CLOUD_ROOT_POLL_MS * 5);
    expect(api['getCloudRoot']).toHaveBeenCalledTimes(3);
  });

  it('stops asking once the flow expired', () => {
    api['connectCloudRoot'].mockReturnValue(of({
      state: 'connecting',
      connector_id: 'root-1',
      login_url: 'https://cloud.example/login',
      expires_at: new Date(Date.now() + 1000).toISOString(),
    }));
    const service = make();
    service.connect();
    vi.advanceTimersByTime(2000);
    api['getCloudRoot'].mockReturnValue(of(status({state: 'connecting', connecting: {login_url: 'x', expires_at: new Date(Date.now() - 1).toISOString()}})));
    const before = api['getCloudRoot'].mock.calls.length;
    vi.advanceTimersByTime(CLOUD_ROOT_POLL_MS);
    // One last read says how it ended, then nothing more.
    vi.advanceTimersByTime(CLOUD_ROOT_POLL_MS * 10);
    expect(api['getCloudRoot'].mock.calls.length - before).toBeLessThanOrEqual(2);
  });

  it('closes the tab and says why when there is nothing to connect', () => {
    api['connectCloudRoot'].mockReturnValue(throwError(() => new HttpErrorResponse({status: 409, error: {detail: 'grant_mode_off'}})));
    api['getCloudRoot'].mockReturnValue(of(status({supported: false, state: 'unsupported', reason: 'grant_mode_off'})));
    const service = make();
    service.connect();
    expect(tab.close).toHaveBeenCalled();
    expect(service.actionError()).toBe('cloudRoot.errors.unsupported');
    expect(service.status()?.reason).toBe('grant_mode_off');
  });

  it.each(['javascript:alert(1)', 'data:text/html,hi', '/relative/login', ''])(
    'never sends the tab to a sign-in page that is not a web page (%s)',
    (loginUrl) => {
      api['connectCloudRoot'].mockReturnValue(of({
        state: 'connecting',
        connector_id: 'root-1',
        login_url: loginUrl,
        expires_at: new Date(Date.now() + 60_000).toISOString(),
      }));
      const service = make();
      service.connect();
      expect(tab.location.href).toBe('');
      expect(tab.close).toHaveBeenCalled();
      expect(service.actionError()).toBe('cloudRoot.errors.startFailed');
      expect(service.busy()).toBe(false);
    },
  );

  it('opens only http(s) pages', () => {
    expect(isWebPage('https://cloud.example/login')).toBe(true);
    expect(isWebPage('http://cloud.localhost/login')).toBe(true);
    expect(isWebPage('javascript:alert(1)')).toBe(false);
    expect(isWebPage('not a url')).toBe(false);
    expect(isWebPage(null)).toBe(false);
  });

  it('says the cloud could not start the connection', () => {
    api['connectCloudRoot'].mockReturnValue(throwError(() => new HttpErrorResponse({status: 502})));
    const service = make();
    service.connect();
    expect(service.actionError()).toBe('cloudRoot.errors.startFailed');
    expect(service.busy()).toBe(false);
  });

  it('cancels a waiting connection', () => {
    const service = make();
    service.connect();
    service.cancel();
    expect(api['cancelCloudRootConnect']).toHaveBeenCalled();
    expect(service.status()?.state).toBe('not_connected');
    vi.advanceTimersByTime(CLOUD_ROOT_POLL_MS * 3);
    expect(api['getCloudRoot']).not.toHaveBeenCalled();
  });

  it('says when the cloud could not be asked to revoke on disconnect', () => {
    api['disconnectCloudRoot'].mockReturnValue(of({...status({state: 'revoked', revoked_reason: 'user', client_name: 'SRW (alice)'}), provider_revoked: false}));
    const service = make();
    service.disconnect();
    expect(service.status()?.state).toBe('revoked');
    expect(service.revokeIncomplete()).toBe(true);
    api['disconnectCloudRoot'].mockReturnValue(of({...status({state: 'revoked'}), provider_revoked: true}));
    service.disconnect();
    expect(service.revokeIncomplete()).toBe(false);
  });
});
