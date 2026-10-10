import {CUSTOM_ELEMENTS_SCHEMA, signal, ɵresolveComponentResources} from '@angular/core';
import {TestBed} from '@angular/core/testing';
import {TranslocoPipe, TranslocoTestingModule} from '@jsverse/transloco';
import {afterEach, beforeAll, describe, expect, it, vi} from 'vitest';
import de from '../../../assets/i18n/de-DE.json';
import en from '../../../assets/i18n/en.json';
import {CloudRootStatus} from '../../core/models/api.model';
import {CloudRootService} from '../../core/services/cloud-root.service';
import {CloudRootCardComponent, cloudRootDetail} from './cloud-root-card.component';

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

function lookup(tree: unknown, key: string): unknown {
  return key.split('.').reduce<unknown>((node, part) => (node as Record<string, unknown> | undefined)?.[part], tree);
}

describe('cloudRootDetail', () => {
  it('names each failure, revocation and missing root with words both catalogues carry', () => {
    const keys = [
      cloudRootDetail(status({state: 'connected', account_id: 'alice'})),
      cloudRootDetail(status({state: 'connected'})),
      cloudRootDetail(status({state: 'connecting'})),
      cloudRootDetail(status()),
      ...['wrong_account', 'admin_account', 'account_disabled', 'verify_failed', 'expired', 'provider_error', 'account_missing', 'error']
        .map((reason) => cloudRootDetail(status({state: 'failed', failure: {reason, at: null}}))),
      cloudRootDetail(status({state: 'revoked', revoked_reason: 'user'})),
      cloudRootDetail(status({state: 'revoked', revoked_reason: 'rejected_by_cloud'})),
      ...['provider_unsupported', 'grant_mode_off', 'no_personal_project']
        .map((reason) => cloudRootDetail(status({supported: false, state: 'unsupported', reason}))),
    ].map((detail) => detail!.key);
    for (const key of keys) {
      expect(typeof lookup(en, key)).toBe('string');
      expect(typeof lookup(de, key)).toBe('string');
    }
    expect(lookup(en, 'cloudRoot.failure.wrong_account')).toContain("e-mail isn't yours");
    expect(lookup(en, 'cloudRoot.failure.admin_account')).toContain("administrator accounts can't be connected");
    // An unknown reason never reaches the page raw.
    expect(cloudRootDetail(status({state: 'failed', failure: {reason: 'later', at: null}}))!.key).toBe('cloudRoot.failure.error');
    expect(cloudRootDetail(status({state: 'connected', account_id: 'alice'}))!.params).toEqual({account: 'alice'});
  });
});

describe('CloudRootCardComponent', () => {
  beforeAll(async () => {
    await ɵresolveComponentResources(() => Promise.resolve(''));
  });
  afterEach(() => TestBed.resetTestingModule());

  async function render(state: CloudRootStatus | null) {
    const service = {
      status: signal(state),
      busy: signal(false),
      actionError: signal<string | null>(null),
      revokeIncomplete: signal(false),
      load: vi.fn(),
      connect: vi.fn(),
      cancel: vi.fn(),
      disconnect: vi.fn(),
    };
    TestBed.configureTestingModule({
      imports: [
        CloudRootCardComponent,
        TranslocoTestingModule.forRoot({langs: {en}, translocoConfig: {availableLangs: ['en'], defaultLang: 'en'}}),
      ],
      providers: [{provide: CloudRootService, useValue: service}],
    });
    // The design-system children bind signal inputs, which this JIT pipeline
    // cannot; they render as inert elements here.
    TestBed.overrideComponent(CloudRootCardComponent, {
      set: {imports: [TranslocoPipe], schemas: [CUSTOM_ELEMENTS_SCHEMA]},
    });
    await TestBed.compileComponents();
    const fixture = TestBed.createComponent(CloudRootCardComponent);
    fixture.detectChanges();
    await fixture.whenStable();
    fixture.detectChanges();
    return {fixture, service, root: fixture.nativeElement as HTMLElement};
  }

  const actions = (root: HTMLElement) =>
    [...root.querySelectorAll<HTMLElement>('[data-cloud-root-action]')].map((el) => el.dataset['cloudRootAction']);

  it('reads the state when it shows, and offers Connect', async () => {
    const {root, service} = await render(status());
    expect(service.load).toHaveBeenCalled();
    expect(root.querySelector('[data-cloud-root]')?.getAttribute('data-state')).toBe('not_connected');
    expect(actions(root)).toEqual(['connect']);
    expect(root.querySelector('[data-cloud-root-detail]')?.textContent?.trim()).toBe(en.cloudRoot.detail.notConnected);
  });

  it('waits for the sign-in with Cancel and a link to the page', async () => {
    const {root} = await render(status({
      state: 'connecting',
      connecting: {login_url: 'https://cloud.example/login', expires_at: '2026-10-10T23:00:00Z'},
    }));
    expect(actions(root)).toEqual(['cancel']);
    const link = root.querySelector<HTMLAnchorElement>('[data-cloud-root-login]')!;
    expect(link.getAttribute('href')).toBe('https://cloud.example/login');
    expect(link.getAttribute('rel')).toContain('noopener');
  });

  it('names the account it is connected as, and offers Disconnect behind a confirm', async () => {
    const {root, fixture, service} = await render(status({state: 'connected', account_id: 'alice'}));
    expect(actions(root)).toEqual(['disconnect']);
    expect(root.querySelector('[data-cloud-root-detail]')?.textContent?.trim()).toBe('Connected as alice.');
    const confirmSpy = vi.spyOn(globalThis, 'confirm').mockReturnValue(false);
    fixture.componentInstance.disconnect();
    expect(service.disconnect).not.toHaveBeenCalled();
    confirmSpy.mockReturnValue(true);
    fixture.componentInstance.disconnect();
    expect(service.disconnect).toHaveBeenCalled();
    confirmSpy.mockRestore();
  });

  it('says why a connection failed or was revoked, and offers to try again', async () => {
    const failed = await render(status({state: 'failed', failure: {reason: 'admin_account', at: null}}));
    expect(actions(failed.root)).toEqual(['connect']);
    expect(failed.root.querySelector('[data-cloud-root-detail]')?.textContent?.trim()).toBe(en.cloudRoot.failure.admin_account);
    TestBed.resetTestingModule();
    const revoked = await render(status({state: 'revoked', revoked_reason: 'rejected_by_cloud'}));
    expect(actions(revoked.root)).toEqual(['reconnect']);
    expect(revoked.root.querySelector('[data-cloud-root-detail]')?.textContent?.trim()).toBe(en.cloudRoot.revoked.rejected_by_cloud);
  });

  it('says a disconnect left the credential in the cloud', async () => {
    const {root, fixture, service} = await render(status({state: 'revoked', revoked_reason: 'user', client_name: 'SRW (alice)'}));
    service.revokeIncomplete.set(true);
    fixture.detectChanges();
    expect(root.querySelector('[data-cloud-root-revoke-incomplete]')?.textContent).toContain('“SRW (alice)”');
  });

  it('shows nothing where the deployment has no main cloud', async () => {
    const {root} = await render(status({supported: false, state: 'unsupported', reason: 'no_main_cloud'}));
    expect(root.querySelector('[data-cloud-root]')).toBeNull();
  });

  it('says why a deployment with a cloud has no root to connect', async () => {
    const {root} = await render(status({supported: false, state: 'unsupported', reason: 'grant_mode_off'}));
    expect(actions(root)).toEqual([]);
    expect(root.querySelector('[data-cloud-root-detail]')?.textContent?.trim()).toBe(en.cloudRoot.unsupported.grant_mode_off);
  });
});
