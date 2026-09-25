/**
 * Settings → AI Subscriptions: login flow states.
 *
 * The states worth pinning are the honest ones. `verifying` exists precisely so
 * the UI does not claim a connection the moment the provider redirects back —
 * upstream reporting "ok" is not proof the credential was persisted, so only
 * `connected` may refresh the account list.
 */
import {CUSTOM_ELEMENTS_SCHEMA, Pipe, signal, ɵresolveComponentResources} from '@angular/core';
import {TestBed} from '@angular/core/testing';
import {afterEach, beforeAll, beforeEach, describe, expect, it, vi} from 'vitest';
import {of, throwError} from 'rxjs';
import {SettingsComponent, type SettingsSection} from './settings.component';
import {SettingsService} from '../../core/services/settings.service';
import {UserService} from '../../core/services/user.service';
import {McpTokenService} from '../../core/services/mcp-token.service';
import {ModelService} from '../../core/services/model.service';
import {ApiService} from '../../core/services/api.service';
import {ViewModeService} from '../../core/services/view-mode.service';
import {CapabilitiesService} from '../../core/services/capabilities.service';
import {I18nService} from '../../core/services/i18n.service';
import {TranslocoService} from '@jsverse/transloco';
import {ActivatedRoute, Router} from '@angular/router';
import {SubscriptionLogin, SubscriptionsStatus} from '../../core/models/api.model';
import {environment} from '../../core/environment';
import de from '../../../assets/i18n/de-DE.json';
import en from '../../../assets/i18n/en.json';

function status(overrides: Partial<SubscriptionsStatus> = {}): SubscriptionsStatus {
  return {
    reachable: true,
    connected: false,
    proxy_url: 'http://proxy:8317',
    error: null,
    accounts: [],
    model_count: 0,
    providers: [
      {
        key: 'openai-codex',
        label: 'OpenAI · ChatGPT / Codex',
        vendor: 'openai',
        login_flow: 'browser',
        channels: ['codex'],
        client_protocol: 'openai-responses',
        has_usage_reader: true,
        inference_verified: true,
        notes: [],
        connected_accounts: 0,
      },
      {
        key: 'xai-grok-build',
        label: 'xAI · Grok Build',
        vendor: 'xai',
        login_flow: 'device',
        channels: ['xai'],
        client_protocol: 'openai-responses',
        has_usage_reader: false,
        inference_verified: false,
        notes: ['Grok Build is its own entitlement.'],
        connected_accounts: 0,
      },
    ],
    ...overrides,
  };
}

function login(overrides: Partial<SubscriptionLogin> = {}): SubscriptionLogin {
  return {
    login_id: 'lg-1',
    provider: 'openai-codex',
    flow: 'browser',
    status: 'pending',
    error: null,
    auth_url: 'https://auth.example/authorize',
    user_code: null,
    expires_at: new Date(Date.now() + 600_000).toISOString(),
    account_id: null,
    accepts_callback_url: true,
    ...overrides,
  };
}

function makeSettingsService() {
  return {
    apiKeys: signal([]),
    preferences: signal({}),
    resolvedDefaults: signal({}),
    loadApiKeys: vi.fn(),
    loadPreferences: vi.fn(),
    updatePreferences: vi.fn(() => of({status: 'ok'})),
    setApiKey: vi.fn(() => of({})),
    deleteApiKey: vi.fn(() => of({status: 'ok'})),
    getSubscriptionsStatus: vi.fn(() => of(status())),
    getSubscriptionAccounts: vi.fn(() => of({accounts: []})),
    getSubscriptionUsage: vi.fn(() => of({available: false, reason: 'unsupported_provider'})),
    disconnectSubscriptionAccount: vi.fn(() => of({status: 'deleted'})),
    startSubscriptionLogin: vi.fn(() => of(login())),
    pollSubscriptionLogin: vi.fn(() => of(login())),
    submitSubscriptionCallback: vi.fn(() => of(login({status: 'verifying'}))),
    cancelSubscriptionLogin: vi.fn(() => of(login({status: 'cancelled'}))),
    getMainCloudSettings: vi.fn(() =>
      of({
        effective: {backend_id: 'nextcloud', is_initialized: false, is_configured: false},
        activation_revision: 0,
        backend_instance: null,
        overlay: {present: false, value: {}, credentials_ref: null, updated_at: null, updated_by: null},
        secrets: {},
        allowed_backends: ['nextcloud'],
      }),
    ),
    putMainCloudSettings: vi.fn(() => of({})),
    testMainCloudSettings: vi.fn(() => of({})),
    deleteMainCloudSettings: vi.fn(() => of({})),
  };
}

@Pipe({name: 'transloco', standalone: true})
class TestTranslocoPipe {
  transform(key: string): string { return key; }
}

/** Each Settings section is its own route; most cases here exercise the
 * subscriptions one, so that is the default. */
function setup(
  service: ReturnType<typeof makeSettingsService>,
  renderTemplate = false,
  section: SettingsSection = 'subscriptions',
) {
  TestBed.resetTestingModule();
  TestBed.configureTestingModule({
    imports: [SettingsComponent],
    providers: [
      {provide: SettingsService, useValue: service},
      {
        provide: UserService,
        useValue: {
          currentUser: signal({id: 'u1', is_admin: true}),
          currentUserId: signal('u1'),
          isAdmin: signal(true),
        },
      },
      {provide: McpTokenService, useValue: {tokens: signal([]), loadTokens: vi.fn()}},
      {
        provide: ModelService,
        useValue: {
          models: signal([]),
          auxiliaryModels: signal([]), embeddingModels: signal([]), ttsModels: signal([]),
          visionModels: signal([]), whisperModels: signal([]),
          groups: signal([]),
          load: vi.fn(),
          loading: signal(false),
        },
      },
      {
        provide: ApiService,
        useValue: {
          getMyCapabilities: () => of(null),
          getProjects: () => of([]),
          getExperts: () => of([]),
          getExpertDefaults: () => of({defaults: {worker: {personal: null, effective: null}, session: {personal: null, effective: null}}}),
          getTtsLibrarySetting: () => of({enabled: false}),
          listTtsVoices: () => of([]),
        },
      },
      {provide: ViewModeService, useValue: {viewMode: signal('desktop'), setMode: vi.fn()}},
      {
        provide: CapabilitiesService,
        useValue: {
          grants: signal(null),
          loadFailed: signal(false),
          load: vi.fn(),
          permissionRestricted: signal(false),
          allowsPermissionMode: () => true,
        },
      },
      {provide: I18nService, useValue: {activeLang: signal('en'), setLanguage: vi.fn()}},
      {
        provide: TranslocoService,
        // Echo the key back: the component treats "translation === key" as
        // "no translation", which is exactly the fallback path we want here.
        useValue: {translate: (key: string) => key, getActiveLang: () => 'en'},
      },
      {provide: Router, useValue: {navigate: vi.fn(), navigateByUrl: vi.fn()}},
      {provide: ActivatedRoute, useValue: {snapshot: {data: {section}}}},
    ],
  });
  TestBed.overrideComponent(SettingsComponent, {
    set: renderTemplate
      ? {imports: [TestTranslocoPipe], schemas: [CUSTOM_ELEMENTS_SCHEMA]}
      : {imports: [], schemas: [CUSTOM_ELEMENTS_SCHEMA], template: ''},
  });
  const fixture = TestBed.createComponent(SettingsComponent);
  // Flush ngOnInit + the admin-gated constructor effect (which loads the
  // subscription status itself) so a test can measure its own calls.
  fixture.detectChanges();
  return fixture;
}

describe('SettingsComponent — AI Subscriptions', () => {
  beforeAll(async () => {
    await ɵresolveComponentResources(() => Promise.resolve(''));
  });

  beforeEach(() => {
    vi.useFakeTimers();
    (globalThis as {open?: unknown}).open = vi.fn();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('withholds MCP setup and token loading in the self-signed browser preset', () => {
    const previous = environment.externalClientsEnabled;
    environment.externalClientsEnabled = false;
    try {
      const component = setup(makeSettingsService(), false, 'mcp').componentInstance;
      expect(component.externalClientsEnabled).toBe(false);
      expect(TestBed.inject(McpTokenService).loadTokens).not.toHaveBeenCalled();
      // No external clients, no MCP page — same rule as the SSH keys page.
      expect(TestBed.inject(Router).navigateByUrl).toHaveBeenCalledWith('/settings/general');
      expect(component.mcpJsonSnippet()).toBe('');
      expect(component.mcpServerUrl()).toBe('');
    } finally {
      environment.externalClientsEnabled = previous;
    }
  });

  it.each([false, true])('renders MCP setup only when external clients are enabled (%s)', (enabled) => {
    const previous = environment.externalClientsEnabled;
    environment.externalClientsEnabled = enabled;
    try {
      // Keep the actual template and its control flow; only unrelated child
      // controls/translations are shallow stubs in this component test.
      const fixture = setup(makeSettingsService(), true, 'mcp');
      const headings = [...fixture.nativeElement.querySelectorAll('h2')]
        .map((node: any) => node.textContent.trim());
      expect(headings.includes('settings.mcp.title')).toBe(enabled);
      fixture.destroy();
    } finally {
      environment.externalClientsEnabled = previous;
    }
  });

  it('opens the authorization page for a browser flow', () => {
    const service = makeSettingsService();
    const component = setup(service).componentInstance;

    component.connectProvider('openai-codex');
    expect(service.startSubscriptionLogin).toHaveBeenCalledWith('openai-codex');
    expect(globalThis.open).toHaveBeenCalledWith('https://auth.example/authorize', '_blank');
    expect(component.activeLogin()?.status).toBe('pending');
  });

  it('does not hijack a tab for a device flow', () => {
    const service = makeSettingsService();
    service.startSubscriptionLogin = vi.fn(() =>
      of(login({provider: 'xai-grok-build', flow: 'device', user_code: 'ABCD-1234'})),
    ) as never;
    const component = setup(service).componentInstance;

    component.connectProvider('xai-grok-build');
    expect(globalThis.open).not.toHaveBeenCalled();
    expect(component.activeLogin()?.user_code).toBe('ABCD-1234');
  });

  it('refuses to start a second login while one is in flight', () => {
    const service = makeSettingsService();
    const component = setup(service).componentInstance;

    component.connectProvider('openai-codex');
    component.connectProvider('xai-grok-build');
    expect(service.startSubscriptionLogin).toHaveBeenCalledTimes(1);
  });

  it('holds at "verifying" without refreshing the account list', () => {
    const service = makeSettingsService();
    service.pollSubscriptionLogin = vi.fn(() => of(login({status: 'verifying'}))) as never;
    const component = setup(service).componentInstance;

    component.connectProvider('openai-codex');
    const statusCallsBefore = service.getSubscriptionsStatus.mock.calls.length;
    vi.advanceTimersByTime(2000);

    expect(component.activeLogin()?.status).toBe('verifying');
    expect(service.getSubscriptionsStatus.mock.calls.length).toBe(statusCallsBefore);
  });

  it('refreshes only once the credential is confirmed', () => {
    const service = makeSettingsService();
    service.pollSubscriptionLogin = vi.fn(() =>
      of(login({status: 'connected', account_id: 'acct-1'})),
    ) as never;
    const component = setup(service).componentInstance;

    component.connectProvider('openai-codex');
    const statusCallsBefore = service.getSubscriptionsStatus.mock.calls.length;
    vi.advanceTimersByTime(2000);

    expect(component.activeLogin()?.status).toBe('connected');
    expect(service.getSubscriptionsStatus.mock.calls.length).toBe(statusCallsBefore + 1);

    // Poll stops once terminal — no further upstream calls.
    const pollCalls = service.pollSubscriptionLogin.mock.calls.length;
    vi.advanceTimersByTime(6000);
    expect(service.pollSubscriptionLogin.mock.calls.length).toBe(pollCalls);
  });

  it('stops polling and surfaces the reason on failure', () => {
    const service = makeSettingsService();
    service.pollSubscriptionLogin = vi.fn(() =>
      of(login({status: 'failed', error: 'credential_not_persisted'})),
    ) as never;
    const component = setup(service).componentInstance;

    component.connectProvider('openai-codex');
    vi.advanceTimersByTime(2000);

    expect(component.activeLogin()?.status).toBe('failed');
    expect(component.loginError()).toBeTruthy();
    const pollCalls = service.pollSubscriptionLogin.mock.calls.length;
    vi.advanceTimersByTime(6000);
    expect(service.pollSubscriptionLogin.mock.calls.length).toBe(pollCalls);
  });

  it('cancels upstream and clears the in-flight login', () => {
    const service = makeSettingsService();
    const component = setup(service).componentInstance;

    component.connectProvider('openai-codex');
    component.cancelLogin();
    expect(service.cancelSubscriptionLogin).toHaveBeenCalledWith('lg-1');
    expect(component.activeLogin()).toBeNull();
  });

  it('relays a pasted callback and keeps waiting until connected', () => {
    const service = makeSettingsService();
    const component = setup(service).componentInstance;

    component.connectProvider('openai-codex');
    component.callbackUrl.set('http://localhost:1455/cb?code=abc&state=st');
    component.submitCallback();

    expect(service.submitSubscriptionCallback).toHaveBeenCalledWith(
      'lg-1',
      'http://localhost:1455/cb?code=abc&state=st',
    );
    // The server answered "verifying" — the UI must not claim success.
    expect(component.activeLogin()?.status).toBe('verifying');
    expect(component.callbackUrl()).toBe('');
  });

  it('shows the server error when the callback is rejected', () => {
    const service = makeSettingsService();
    service.submitSubscriptionCallback = vi.fn(() =>
      throwError(() => ({error: {detail: 'That callback belongs to a different sign-in attempt.'}})),
    ) as never;
    const component = setup(service).componentInstance;

    component.connectProvider('openai-codex');
    component.callbackUrl.set('http://localhost:1455/cb?code=abc&state=other');
    component.submitCallback();

    expect(component.loginError()).toBe('That callback belongs to a different sign-in attempt.');
  });

  it('fetches usage on demand and reports unavailable without a fake zero', () => {
    const service = makeSettingsService();
    const component = setup(service).componentInstance;

    component.loadUsage('acct-1');
    expect(service.getSubscriptionUsage).toHaveBeenCalledWith('acct-1');
    expect(component.usageFor('acct-1')?.available).toBe(false);
    // An unavailable reading never expands into bars.
    expect(component.expandedUsage()).toBeNull();
  });

  it('drops cached usage when an account is disconnected', () => {
    const service = makeSettingsService();
    service.getSubscriptionUsage = vi.fn(() =>
      of({available: true, primary: {used_percent: 12}} as never),
    ) as never;
    const component = setup(service).componentInstance;

    component.loadUsage('acct-1');
    expect(component.usageFor('acct-1')).toBeDefined();

    component.disconnectAccount('acct-1');
    expect(service.disconnectSubscriptionAccount).toHaveBeenCalledWith('acct-1');
    expect(component.usageFor('acct-1')).toBeUndefined();
  });

  it('stops the poll when the view is destroyed', () => {
    const service = makeSettingsService();
    const component = setup(service).componentInstance;

    component.connectProvider('openai-codex');
    component.ngOnDestroy();
    const pollCalls = service.pollSubscriptionLogin.mock.calls.length;
    vi.advanceTimersByTime(6000);
    expect(service.pollSubscriptionLogin.mock.calls.length).toBe(pollCalls);
  });

  it('usage bar tone bands on the fill percentage', () => {
    const component = setup(makeSettingsService()).componentInstance;
    expect(component.usageTone(10)).toBe('ok');
    expect(component.usageTone(75)).toBe('warn');
    expect(component.usageTone(95)).toBe('crit');
    expect(component.usageTone(null)).toBe('ok');
  });

  it('formats a reset countdown', () => {
    const component = setup(makeSettingsService()).componentInstance;
    expect(component.formatResetIn(0)).toBe('');
    expect(component.formatResetIn(720)).toBe('12m');
    expect(component.formatResetIn(9120)).toBe('2h 32m');
    expect(component.formatResetIn(360000)).toBe('4d 4h');
  });
});

// The provider-key card used to borrow the PAT page's settings.apiKeys.title /
// .desc / .empty and reference seven settings.apiKeys.* keys that no longer
// existed, so /settings showed "Personal Access Tokens" twice and never named
// the LLM-provider card (issues/settings_menu_dead_and_unwired_controls.md §9).
describe('SettingsComponent — LLM provider key card', () => {
  beforeAll(async () => {
    await ɵresolveComponentResources(() => Promise.resolve(''));
  });

  it('names itself rather than the PAT page and renders its own column and form keys', () => {
    const service = makeSettingsService();
    service.apiKeys.set([
      {id: 'k1', provider: 'openai', key_prefix: 'sk-ab', label: null, updated_at: '2026-09-01T00:00:00Z'},
    ] as never);
    const fixture = setup(service, true, 'provider-keys');
    const el = fixture.nativeElement as HTMLElement;
    const text = (root: Element, sel: string) =>
      [...root.querySelectorAll(sel)].map((node) => node.textContent!.trim()).filter(Boolean);

    const headings = text(el, 'h2');
    expect(headings).toContain('settings.providerKeys.title');
    // The PAT heading belongs to the PAT page alone.
    expect(headings).not.toContain('settings.apiKeys.title');
    const card = [...el.querySelectorAll('section')].find(
      (section) => section.querySelector('h2')?.textContent?.trim() === 'settings.providerKeys.title',
    )!;
    expect(text(card, '.section-desc')).toEqual(['settings.providerKeys.desc']);
    expect(text(card, '.key-header span')).toEqual([
      'settings.providerKeys.colProvider',
      'settings.providerKeys.colKey',
      'settings.providerKeys.colLabel',
      'settings.providerKeys.colUpdated',
    ]);
    expect(text(card, '.form-title')).toEqual(['settings.providerKeys.addTitle']);
    fixture.destroy();
  });

  it('carries copy distinct from the PAT card in both locales', () => {
    for (const locale of [en, de]) {
      const {providerKeys, apiKeys} = locale.settings;
      expect(providerKeys.title).not.toBe(apiKeys.title);
      expect(providerKeys.desc).not.toBe(apiKeys.desc);
      expect(providerKeys.empty).not.toBe(apiKeys.empty);
    }
  });
});

// navigation_fixed_rail.md F5: one route per section, and each renders — and
// fetches for — only its own cards.
describe('SettingsComponent — sections', () => {
  beforeAll(async () => {
    await ɵresolveComponentResources(() => Promise.resolve(''));
  });

  const headingsOf = (fixture: ReturnType<typeof setup>) =>
    [...(fixture.nativeElement as HTMLElement).querySelectorAll('h2')].map((node) =>
      node.textContent!.trim(),
    );

  it.each<[SettingsSection, string[]]>([
    ['general', ['settings.appearance.title', 'settings.language.title', 'settings.dataVisibility.title']],
    ['defaults', ['settings.expertDefaults.title', 'settings.preferences.title', 'settings.persistent.title']],
    ['provider-keys', ['settings.providerKeys.title']],
    ['notifications', ['settings.communication.title']],
    ['subscriptions', ['settings.subscriptions.title']],
    ['cloud', ['settings.cloud.title']],
  ])('renders only the %s cards', (section, expected) => {
    const fixture = setup(makeSettingsService(), true, section);
    expect(headingsOf(fixture)).toEqual(expected);
    fixture.destroy();
  });

  it.each<[SettingsSection, string]>([
    ['general', 'settings.nav.general'],
    ['provider-keys', 'settings.nav.providerKeys'],
    ['cloud', 'settings.nav.cloud'],
  ])('titles the %s page with its rail label', (section, key) => {
    const fixture = setup(makeSettingsService(), true, section);
    expect(fixture.nativeElement.querySelector('h1').textContent.trim()).toBe(key);
    fixture.destroy();
  });

  it('asks the proxy for subscription status only on the subscriptions page', () => {
    const general = makeSettingsService();
    setup(general, false, 'general');
    expect(general.getSubscriptionsStatus).not.toHaveBeenCalled();
    expect(general.getMainCloudSettings).not.toHaveBeenCalled();

    const subscriptions = makeSettingsService();
    setup(subscriptions, false, 'subscriptions');
    expect(subscriptions.getSubscriptionsStatus).toHaveBeenCalled();
    expect(subscriptions.getMainCloudSettings).not.toHaveBeenCalled();
  });

  it('loads cloud settings only on the cloud page', () => {
    const service = makeSettingsService();
    setup(service, false, 'cloud');
    expect(service.getMainCloudSettings).toHaveBeenCalled();
    expect(service.getSubscriptionsStatus).not.toHaveBeenCalled();
  });

  it('loads provider keys only on their own page', () => {
    const elsewhere = makeSettingsService();
    setup(elsewhere, false, 'general');
    expect(elsewhere.loadApiKeys).not.toHaveBeenCalled();

    const own = makeSettingsService();
    setup(own, false, 'provider-keys');
    expect(own.loadApiKeys).toHaveBeenCalled();
  });
});
