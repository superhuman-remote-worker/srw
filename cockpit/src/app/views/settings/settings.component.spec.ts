/**
 * Settings → Persistent Agent: the "Workspace backend" field is gone (Slice
 * A2b §6, "Settings page") — the default now lives on the personal project,
 * and this page only links to it.
 */
import { CUSTOM_ELEMENTS_SCHEMA, Pipe, signal, ɵresolveComponentResources } from '@angular/core';
import { TestBed } from '@angular/core/testing';
import { beforeAll, describe, expect, it, vi } from 'vitest';
import { of } from 'rxjs';
import { SettingsComponent, type SettingsSection } from './settings.component';
import { SettingsService } from '../../core/services/settings.service';
import { UserService } from '../../core/services/user.service';
import { McpTokenService } from '../../core/services/mcp-token.service';
import { ModelService } from '../../core/services/model.service';
import { ApiService } from '../../core/services/api.service';
import { ViewModeService } from '../../core/services/view-mode.service';
import { CapabilitiesService } from '../../core/services/capabilities.service';
import { I18nService } from '../../core/services/i18n.service';
import { TranslocoService } from '@jsverse/transloco';
import { ActivatedRoute, Router } from '@angular/router';
import { User } from '../../core/models/api.model';

function makeSettingsService() {
  return {
    apiKeys: signal([]),
    preferences: signal({}),
    resolvedDefaults: signal({}),
    loadApiKeys: vi.fn(),
    loadPreferences: vi.fn(),
    updatePreferences: vi.fn(() => of({ status: 'ok' })),
    setApiKey: vi.fn(() => of({})),
    deleteApiKey: vi.fn(() => of({ status: 'ok' })),
    getSubscriptionsStatus: vi.fn(() => of({ reachable: false, connected: false, proxy_url: '', error: null, accounts: [], model_count: 0, providers: [] })),
    getSubscriptionAccounts: vi.fn(() => of({ accounts: [] })),
    getSubscriptionUsage: vi.fn(() => of({ available: false, reason: 'unsupported_provider' })),
    disconnectSubscriptionAccount: vi.fn(() => of({ status: 'deleted' })),
    startSubscriptionLogin: vi.fn(),
    pollSubscriptionLogin: vi.fn(),
    submitSubscriptionCallback: vi.fn(),
    cancelSubscriptionLogin: vi.fn(),
    getMainCloud: vi.fn(() => of(null)),
  };
}

@Pipe({ name: 'transloco', standalone: true })
class TestTranslocoPipe {
  transform(key: string): string {
    return key;
  }
}

function setup(
  service: ReturnType<typeof makeSettingsService>,
  renderTemplate = false,
  section: SettingsSection = 'defaults',
  user: Partial<User> | null = { id: 'u1', is_admin: true },
) {
  TestBed.resetTestingModule();
  TestBed.configureTestingModule({
    imports: [SettingsComponent],
    providers: [
      { provide: SettingsService, useValue: service },
      {
        provide: UserService,
        useValue: {
          currentUser: signal(user),
          currentUserId: signal(user?.id ?? null),
          isAdmin: signal(!!user?.is_admin),
        },
      },
      { provide: McpTokenService, useValue: { tokens: signal([]), loadTokens: vi.fn() } },
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
          getExpertDefaults: () => of({ defaults: { worker: { personal: null, effective: null }, session: { personal: null, effective: null } } }),
          getTtsLibrarySetting: () => of({ enabled: false }),
          listTtsVoices: () => of([]),
        },
      },
      { provide: ViewModeService, useValue: { viewMode: signal('desktop'), setMode: vi.fn() } },
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
      { provide: I18nService, useValue: { activeLang: signal('en'), setLanguage: vi.fn() } },
      {
        provide: TranslocoService,
        useValue: { translate: (key: string) => key, getActiveLang: () => 'en' },
      },
      { provide: Router, useValue: { navigate: vi.fn(), navigateByUrl: vi.fn() } },
      { provide: ActivatedRoute, useValue: { snapshot: { data: { section } } } },
    ],
  });
  TestBed.overrideComponent(SettingsComponent, {
    set: renderTemplate
      ? { imports: [TestTranslocoPipe], schemas: [CUSTOM_ELEMENTS_SCHEMA] }
      : { imports: [], schemas: [CUSTOM_ELEMENTS_SCHEMA], template: '' },
  });
  const fixture = TestBed.createComponent(SettingsComponent);
  fixture.detectChanges();
  return fixture;
}

describe('SettingsComponent — persistent agent workspace field', () => {
  beforeAll(async () => {
    await ɵresolveComponentResources(() => Promise.resolve(''));
  });

  it('no longer saves a workspace backend', () => {
    const service = makeSettingsService();
    service.preferences.set({
      persistent_agent: {
        model: 'claude-sonnet-4-6',
        permission_mode: 'supervised',
        idle_timeout_minutes: 30,
      },
    } as never);
    const fixture = setup(service);
    const component = fixture.componentInstance;

    component.savePersistentAgent();

    expect(service.updatePreferences).toHaveBeenCalledTimes(1);
    const sent = service.updatePreferences.mock.calls.at(-1)![0] as {
      persistent_agent: Record<string, unknown>;
    };
    expect(sent.persistent_agent).not.toHaveProperty('workspace_backend');
  });

  it('replaces the field with a link to the personal project when one exists', () => {
    const fixture = setup(makeSettingsService(), true, 'defaults', {
      id: 'u1',
      is_admin: true,
      default_project_id: 'proj-1',
    } as never);
    const el = fixture.nativeElement as HTMLElement;

    const hint = [...el.querySelectorAll('p.field-hint')].find((p) =>
      p.textContent!.includes('settings.persistent.workspaceMoved'),
    );
    expect(hint).toBeTruthy();
    const link = hint!.querySelector('a');
    expect(link?.textContent?.trim()).toBe('settings.persistent.workspaceMovedLink');

    // The field it replaced is gone.
    expect(el.textContent).not.toContain('settings.persistent.workspaceBackend');
  });

  it('shows the text without a link when the user has no personal project', () => {
    const fixture = setup(makeSettingsService(), true, 'defaults', {
      id: 'u1',
      is_admin: true,
      default_project_id: null,
    } as never);
    const el = fixture.nativeElement as HTMLElement;

    const hint = [...el.querySelectorAll('p.field-hint')].find((p) =>
      p.textContent!.includes('settings.persistent.workspaceMoved'),
    );
    expect(hint).toBeTruthy();
    expect(hint!.querySelector('a')).toBeNull();
  });
});
