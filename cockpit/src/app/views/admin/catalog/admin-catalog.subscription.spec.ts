import {CUSTOM_ELEMENTS_SCHEMA, signal, ɵresolveComponentResources} from '@angular/core';
import {TestBed} from '@angular/core/testing';
import {TranslocoService} from '@jsverse/transloco';
import {beforeAll, beforeEach, describe, expect, it, vi} from 'vitest';
import {of, Subject, throwError} from 'rxjs';
import {AdminModelsService} from '../../../core/services/admin-models.service';
import {AdminProvidersService} from '../../../core/services/admin-providers.service';
import {AdminModelsCoordinatorService} from '../models/admin-models-coordinator.service';
import {AdminCatalogComponent} from './admin-catalog.component';
import {
  LlmEndpoint,
  SubscriptionDiscoveredModel,
  SubscriptionDiscoveryResult,
} from '../../../core/models/api.model';

const SUBSCRIPTION_ENDPOINT: LlmEndpoint = {
  id: 'ep-subs',
  // Deliberately a renamed label: identity must come from transport_kind.
  label: 'Our shared AI logins',
  base_url: 'http://ai.internal/v1',
  key_prefix: null,
  transport_kind: 'subscription-proxy',
  created_at: null,
  updated_at: null,
  models: [],
};

const PLAIN_ENDPOINT: LlmEndpoint = {
  id: 'ep-vllm',
  label: 'Local Gemma',
  base_url: 'http://vllm.ai.svc:8000/v1',
  key_prefix: null,
  transport_kind: null,
  created_at: null,
  updated_at: null,
  models: [],
};

function candidate(overrides: Partial<SubscriptionDiscoveredModel> = {}): SubscriptionDiscoveredModel {
  return {
    id: 'gpt-5.6-sol',
    display_label: 'GPT 5.6 Sol',
    owned_by: 'openai',
    sources: ['codex'],
    providers: ['openai-codex'],
    account_ids: ['acct-1'],
    client_protocol: 'openai-responses',
    context_window: 372000,
    max_output_tokens: 128000,
    family: 'codex',
    capability_hints: ['chat', 'auxiliary'],
    support: 'supported',
    support_reason: null,
    registered: false,
    catalog_id: null,
    routing_drift: false,
    ...overrides,
  };
}

function discovery(
  models: SubscriptionDiscoveredModel[],
  overrides: Partial<SubscriptionDiscoveryResult> = {},
): SubscriptionDiscoveryResult {
  return {
    subscription: true,
    ok: true,
    probe_url: 'http://ai.internal/v1/models',
    error: null,
    models,
    unreadable_account_ids: [],
    attribution_complete: true,
    ...overrides,
  };
}

const models = {
  models: signal([]),
  families: signal(['default', 'codex']),
  familyDefaults: signal<Record<string, number>>({}),
  loading: signal(false),
  loadModels: vi.fn(),
  loadFamilies: vi.fn(),
  detectFamily: vi.fn(() => of({family: 'default', source: 'fallback'})),
  createModel: vi.fn(() => of({} as never)),
  updateModel: vi.fn(() => of({})),
  deleteModel: vi.fn(() => of({})),
  testModel: vi.fn(() => of({})),
};

function makeProviders(endpoints: LlmEndpoint[]) {
  return {
    systemApiKeys: signal([]),
    systemEndpoints: signal(endpoints),
    subscriptionAvailability: signal({
      available: true,
      reachable: true,
      error: null,
      account_count: 1,
      accounts: [],
      models: [],
      proxy_url: 'http://ai.internal',
      endpoint_id: 'ep-subs',
    }),
    loadSystemApiKeys: vi.fn(),
    loadSystemEndpoints: vi.fn(),
    loadSubscriptionAvailability: vi.fn(),
    discoverSystemEndpointModels: vi.fn(() => of(discovery([]))),
    importSubscriptionModels: vi.fn(() => of({created: [], skipped: [], rejected: []})),
  };
}

function setup(providers: ReturnType<typeof makeProviders>) {
  TestBed.resetTestingModule();
  TestBed.configureTestingModule({
    imports: [AdminCatalogComponent],
    providers: [
      {provide: TranslocoService, useValue: {translate: vi.fn((key: string) => key)}},
      {provide: AdminModelsService, useValue: models},
      {provide: AdminProvidersService, useValue: providers},
      {
        provide: AdminModelsCoordinatorService,
        useValue: {discoverEndpoint$: new Subject<string>()},
      },
    ],
  });
  TestBed.overrideComponent(AdminCatalogComponent, {
    set: {imports: [], schemas: [CUSTOM_ELEMENTS_SCHEMA]},
  });
  return TestBed.createComponent(AdminCatalogComponent);
}

describe('AdminCatalogComponent — subscription proxy source', () => {
  beforeAll(async () => {
    await ɵresolveComponentResources(() => Promise.resolve(''));
  });

  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('identifies the source by transport marker, not by its label', () => {
    const providers = makeProviders([SUBSCRIPTION_ENDPOINT, PLAIN_ENDPOINT]);
    const component = setup(providers).componentInstance;

    component.formProviderKey.set('endpoint:ep-subs');
    expect(component.selectedIsSubscription()).toBe(true);

    component.formProviderKey.set('endpoint:ep-vllm');
    expect(component.selectedIsSubscription()).toBe(false);
  });

  it('names the source "Subscription proxy" regardless of the stored label', () => {
    const providers = makeProviders([SUBSCRIPTION_ENDPOINT, PLAIN_ENDPOINT]);
    const component = setup(providers).componentInstance;
    const labels = component.providerOptions().map((o) => o.label);
    expect(labels).toContain('Subscription proxy');
    expect(labels).toContain('Local Gemma (endpoint)');
  });

  it('keeps the plain-endpoint quick-fill path untouched', () => {
    const providers = makeProviders([PLAIN_ENDPOINT]);
    providers.discoverSystemEndpointModels = vi.fn(() =>
      of({
        ok: true,
        status: 200,
        error: null,
        probe_url: 'http://vllm.ai.svc:8000/v1/models',
        models: [
          {id: 'gemma-4', owned_by: null, capability_hints: ['chat'], family: 'gemma', context_window: null},
        ],
      }),
    ) as never;
    const component = setup(providers).componentInstance;

    component.discoverFromEndpoint('ep-vllm');
    expect(component.discoveredModels().length).toBe(1);
    expect(component.subscriptionModels().length).toBe(0);
  });

  it('routes the enriched shape into the subscription list', () => {
    const providers = makeProviders([SUBSCRIPTION_ENDPOINT]);
    providers.discoverSystemEndpointModels = vi.fn(() =>
      of(discovery([candidate()])),
    ) as never;
    const component = setup(providers).componentInstance;

    component.discoverFromEndpoint('ep-subs');
    expect(component.subscriptionModels().length).toBe(1);
    expect(component.discoveredModels().length).toBe(0);
    expect(component.subscriptionModels()[0].sources).toEqual(['codex']);
  });

  it('does not treat a failed discovery as an empty inventory', () => {
    const providers = makeProviders([SUBSCRIPTION_ENDPOINT]);
    providers.discoverSystemEndpointModels = vi.fn(() =>
      of(discovery([], {ok: false, error: 'proxy unreachable'})),
    ) as never;
    const component = setup(providers).componentInstance;

    component.discoverFromEndpoint('ep-subs');
    expect(component.discoverError()).toBe('proxy unreachable');
    expect(component.subscriptionModels()).toEqual([]);
  });

  it('flags incomplete attribution instead of implying "no source"', () => {
    const providers = makeProviders([SUBSCRIPTION_ENDPOINT]);
    providers.discoverSystemEndpointModels = vi.fn(() =>
      of(
        discovery([candidate({sources: [], providers: []})], {
          attribution_complete: false,
          unreadable_account_ids: ['acct-1'],
        }),
      ),
    ) as never;
    const component = setup(providers).componentInstance;

    component.discoverFromEndpoint('ep-subs');
    expect(component.subscriptionAttributionComplete()).toBe(false);
  });

  describe('selection', () => {
    it('refuses to select a registered or unsupported model', () => {
      const providers = makeProviders([SUBSCRIPTION_ENDPOINT]);
      const component = setup(providers).componentInstance;

      const registered = candidate({id: 'already', registered: true});
      const media = candidate({id: 'gpt-image-2', support: 'unsupported_modality'});
      component.toggleSelection(registered, true);
      component.toggleSelection(media, true);
      expect(component.subscriptionSelection().size).toBe(0);

      component.toggleSelection(candidate({id: 'fresh'}), true);
      expect(component.isSelected('fresh')).toBe(true);
    });

    it('"select supported" skips registered, unsupported and review rows', () => {
      const providers = makeProviders([SUBSCRIPTION_ENDPOINT]);
      providers.discoverSystemEndpointModels = vi.fn(() =>
        of(
          discovery([
            candidate({id: 'fresh'}),
            candidate({id: 'already', registered: true}),
            candidate({id: 'gpt-image-2', support: 'unsupported_modality'}),
            candidate({id: 'mystery', support: 'needs_review'}),
          ]),
        ),
      ) as never;
      const component = setup(providers).componentInstance;

      component.discoverFromEndpoint('ep-subs');
      component.selectAllSupported();
      expect(Array.from(component.subscriptionSelection())).toEqual(['fresh']);
      expect(component.importableCount()).toBe(1);
    });

    it('filters by id and by source', () => {
      const providers = makeProviders([SUBSCRIPTION_ENDPOINT]);
      providers.discoverSystemEndpointModels = vi.fn(() =>
        of(
          discovery([
            candidate({id: 'gpt-5.6-sol', sources: ['codex']}),
            candidate({id: 'kimi-k2.6', sources: ['kimi']}),
          ]),
        ),
      ) as never;
      const component = setup(providers).componentInstance;

      component.discoverFromEndpoint('ep-subs');
      component.modelFilter.set('kimi');
      expect(component.filteredSubscriptionModels().map((m) => m.id)).toEqual(['kimi-k2.6']);
      component.modelFilter.set('codex');
      expect(component.filteredSubscriptionModels().map((m) => m.id)).toEqual(['gpt-5.6-sol']);
    });
  });

  describe('bulk import', () => {
    it('"Add selected" sends exactly the checked ids', () => {
      const providers = makeProviders([SUBSCRIPTION_ENDPOINT]);
      providers.discoverSystemEndpointModels = vi.fn(() =>
        of(discovery([candidate({id: 'a'}), candidate({id: 'b'})])),
      ) as never;
      const component = setup(providers).componentInstance;

      component.formProviderKey.set('endpoint:ep-subs');
      component.discoverFromEndpoint('ep-subs');
      component.toggleSelection(candidate({id: 'a'}), true);
      component.addSelectedModels();

      expect(providers.importSubscriptionModels).toHaveBeenCalledWith('ep-subs', ['a'], false, null);
    });

    it('opts into review only when a review row was explicitly selected', () => {
      const providers = makeProviders([SUBSCRIPTION_ENDPOINT]);
      providers.discoverSystemEndpointModels = vi.fn(() =>
        of(discovery([candidate({id: 'mystery', support: 'needs_review'})])),
      ) as never;
      const component = setup(providers).componentInstance;

      component.formProviderKey.set('endpoint:ep-subs');
      component.discoverFromEndpoint('ep-subs');
      component.toggleSelection(candidate({id: 'mystery', support: 'needs_review'}), true);
      component.addSelectedModels();

      expect(providers.importSubscriptionModels).toHaveBeenCalledWith('ep-subs', ['mystery'], true, null);
    });

    it('"Add all supported" lets the server pick — never the client filter', () => {
      const providers = makeProviders([SUBSCRIPTION_ENDPOINT]);
      providers.discoverSystemEndpointModels = vi.fn(() =>
        of(discovery([candidate({id: 'a'}), candidate({id: 'b'})])),
      ) as never;
      const component = setup(providers).componentInstance;

      component.formProviderKey.set('endpoint:ep-subs');
      component.discoverFromEndpoint('ep-subs');
      component.modelFilter.set('a');
      component.addAllSupportedModels();

      expect(providers.importSubscriptionModels).toHaveBeenCalledWith('ep-subs', undefined, false, null);
    });

    it('re-reads registration state from the catalog after an import', () => {
      const providers = makeProviders([SUBSCRIPTION_ENDPOINT]);
      providers.discoverSystemEndpointModels = vi.fn(() =>
        of(discovery([candidate({id: 'a'})])),
      ) as never;
      const component = setup(providers).componentInstance;

      component.formProviderKey.set('endpoint:ep-subs');
      component.discoverFromEndpoint('ep-subs');
      expect(providers.discoverSystemEndpointModels).toHaveBeenCalledTimes(1);

      component.toggleSelection(candidate({id: 'a'}), true);
      component.addSelectedModels();
      expect(providers.discoverSystemEndpointModels).toHaveBeenCalledTimes(2);
      expect(component.subscriptionSelection().size).toBe(0);
    });

    it('surfaces an import failure instead of reporting success', () => {
      const providers = makeProviders([SUBSCRIPTION_ENDPOINT]);
      providers.discoverSystemEndpointModels = vi.fn(() =>
        of(discovery([candidate({id: 'a'})])),
      ) as never;
      providers.importSubscriptionModels = vi.fn(() =>
        throwError(() => ({error: {detail: 'inventory unreadable'}})),
      ) as never;
      const component = setup(providers).componentInstance;

      component.formProviderKey.set('endpoint:ep-subs');
      component.discoverFromEndpoint('ep-subs');
      component.toggleSelection(candidate({id: 'a'}), true);
      component.addSelectedModels();

      expect(component.discoverError()).toBe('inventory unreadable');
      expect(component.importResult()).toBeNull();
    });

    it('keeps the import summary visible after the post-import rediscovery', () => {
      const providers = makeProviders([SUBSCRIPTION_ENDPOINT]);
      providers.discoverSystemEndpointModels = vi.fn(() =>
        of(discovery([candidate({id: 'a'})])),
      ) as never;
      const summary = {created: ['a'], skipped: [], rejected: []};
      providers.importSubscriptionModels = vi.fn(() => of(summary)) as never;
      const component = setup(providers).componentInstance;

      component.formProviderKey.set('endpoint:ep-subs');
      component.discoverFromEndpoint('ep-subs');
      component.toggleSelection(candidate({id: 'a'}), true);
      component.addSelectedModels();

      expect(component.importResult()).toEqual(summary);
    });

    it('sends the context cap, parsed from a preset label', () => {
      const providers = makeProviders([SUBSCRIPTION_ENDPOINT]);
      providers.discoverSystemEndpointModels = vi.fn(() =>
        of(discovery([candidate({id: 'a'})])),
      ) as never;
      const component = setup(providers).componentInstance;

      component.formProviderKey.set('endpoint:ep-subs');
      component.discoverFromEndpoint('ep-subs');
      component.importCapText.set('256k');
      component.addAllSupportedModels();

      expect(providers.importSubscriptionModels).toHaveBeenCalledWith(
        'ep-subs',
        undefined,
        false,
        262144,
      );
    });

    it('refuses to import while the cap is not a token count', () => {
      const providers = makeProviders([SUBSCRIPTION_ENDPOINT]);
      providers.discoverSystemEndpointModels = vi.fn(() =>
        of(discovery([candidate({id: 'a'})])),
      ) as never;
      const component = setup(providers).componentInstance;

      component.formProviderKey.set('endpoint:ep-subs');
      component.discoverFromEndpoint('ep-subs');
      component.importCapText.set('lots');
      expect(component.importCapInvalid()).toBe(true);
      component.addAllSupportedModels();

      expect(providers.importSubscriptionModels).not.toHaveBeenCalled();
    });
  });

  describe('fill form', () => {
    it('fills the single-model form, context window included', () => {
      const providers = makeProviders([SUBSCRIPTION_ENDPOINT]);
      const component = setup(providers).componentInstance;

      component.formProviderKey.set('endpoint:ep-subs');
      component.applySubscriptionModel(candidate());

      expect(component.formModelId()).toBe('gpt-5.6-sol');
      expect(component.formDisplayLabel()).toBe('GPT 5.6 Sol');
      expect(component.formFamily()).toBe('codex');
      expect(component.formContextWindow()).toBe(372000);
      expect(component.formFlash()).toBe(true);
    });

    it('adds a filled model with the routing a bulk import would store', () => {
      const providers = makeProviders([SUBSCRIPTION_ENDPOINT]);
      providers.discoverSystemEndpointModels = vi.fn(() =>
        of(discovery([candidate(), candidate({id: 'other'})])),
      ) as never;
      const component = setup(providers).componentInstance;

      component.formProviderKey.set('endpoint:ep-subs');
      component.discoverFromEndpoint('ep-subs');
      component.toggleSelection(candidate(), true);
      component.toggleSelection(candidate({id: 'other'}), true);
      component.applySubscriptionModel(candidate());
      component.onContextWindowChange('200000');
      component.submit();

      expect(models.createModel).toHaveBeenCalledWith(
        expect.objectContaining({
          provider_kind: 'endpoint',
          provider_ref: 'ep-subs',
          model_id: 'gpt-5.6-sol',
          context_window: 200000,
          params_json: {
            routing: {client_protocol: 'openai-responses', subscription_sources: ['codex']},
            max_output_tokens: 128000,
          },
        }),
      );
      // The added model reads as registered; the rest of the selection stays.
      const added = component.subscriptionModels().find((m) => m.id === 'gpt-5.6-sol');
      expect(added?.registered).toBe(true);
      expect(Array.from(component.subscriptionSelection())).toEqual(['other']);
    });

    it('stores a needs-review model on the neutral protocol, flagged', () => {
      const providers = makeProviders([SUBSCRIPTION_ENDPOINT]);
      const mystery = candidate({
        id: 'mystery',
        client_protocol: null,
        sources: [],
        max_output_tokens: null,
        support: 'needs_review',
      });
      providers.discoverSystemEndpointModels = vi.fn(() => of(discovery([mystery]))) as never;
      const component = setup(providers).componentInstance;

      component.formProviderKey.set('endpoint:ep-subs');
      component.discoverFromEndpoint('ep-subs');
      component.applySubscriptionModel(mystery);
      component.submit();

      expect(models.createModel).toHaveBeenCalledWith(
        expect.objectContaining({
          params_json: {routing: {client_protocol: 'openai-chat', needs_review: true}},
        }),
      );
    });

    it('sends no routing for an id discovery never advertised', () => {
      const providers = makeProviders([SUBSCRIPTION_ENDPOINT]);
      providers.discoverSystemEndpointModels = vi.fn(() =>
        of(discovery([candidate()])),
      ) as never;
      const component = setup(providers).componentInstance;

      component.formProviderKey.set('endpoint:ep-subs');
      component.discoverFromEndpoint('ep-subs');
      component.applySubscriptionModel(candidate());
      component.formModelId.set('something-else');
      component.submit();

      expect(models.createModel).toHaveBeenCalledWith(
        expect.objectContaining({model_id: 'something-else', params_json: undefined}),
      );
    });

    it('drops the subscription list when the provider changes', () => {
      const providers = makeProviders([SUBSCRIPTION_ENDPOINT, PLAIN_ENDPOINT]);
      providers.discoverSystemEndpointModels = vi.fn(() =>
        of(discovery([candidate()])),
      ) as never;
      const component = setup(providers).componentInstance;

      component.formProviderKey.set('endpoint:ep-subs');
      component.discoverFromEndpoint('ep-subs');
      component.toggleSelection(candidate(), true);
      component.onProviderKeyChange('endpoint:ep-vllm');

      expect(component.subscriptionModels()).toEqual([]);
      expect(component.subscriptionSelection().size).toBe(0);
    });
  });
});
