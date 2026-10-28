import {CUSTOM_ELEMENTS_SCHEMA, signal, ɵresolveComponentResources} from '@angular/core';
import {TestBed} from '@angular/core/testing';
import {TranslocoService} from '@jsverse/transloco';
import {beforeAll, beforeEach, describe, expect, it, vi} from 'vitest';
import {of, Subject, throwError} from 'rxjs';
import {AdminModelsService} from '../../../core/services/admin-models.service';
import {AdminProvidersService} from '../../../core/services/admin-providers.service';
import {AdminModelsCoordinatorService} from '../models/admin-models-coordinator.service';
import {AdminCatalogComponent, mergeRoutingProtocol} from './admin-catalog.component';
import {CatalogModel, LlmEndpoint} from '../../../core/models/api.model';

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
    discoverSystemEndpointModels: vi.fn(() => of({ok: true, status: 200, error: null, probe_url: '', models: []})),
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

const ENDPOINT: LlmEndpoint = {
  id: 'ep-vllm',
  label: 'Local Gemma',
  base_url: 'http://vllm.ai.svc:8000/v1',
  key_prefix: null,
  transport_kind: null,
  created_at: null,
  updated_at: null,
  models: [],
};

function row(overrides: Partial<CatalogModel> = {}): CatalogModel {
  return {
    id: 'row-1',
    provider_kind: 'endpoint',
    provider_ref: 'ep-vllm',
    model_id: 'gemma-4',
    display_label: 'Gemma 4',
    capabilities: ['chat'],
    family: 'gemma',
    context_window: null,
    resolved_context_window: 128000,
    context_window_source: 'family_default',
    reasoning_level: null,
    params_json: null,
    enabled: true,
    seeded_from: null,
    notes: null,
    created_at: null,
    updated_at: null,
    client_protocol: 'openai-chat',
    ...overrides,
  } as CatalogModel;
}

describe('API shape', () => {
  beforeAll(async () => {
    await ɵresolveComponentResources(() => Promise.resolve(''));
  });

  beforeEach(() => {
    vi.clearAllMocks();
    models.models.set([]);
  });

  it('merges the protocol into params without touching other keys', () => {
    expect(
      mergeRoutingProtocol(
        {pricing: {id: 'p'}, routing: {subscription_sources: ['claude']}},
        'anthropic-messages',
      ),
    ).toEqual({
      pricing: {id: 'p'},
      routing: {subscription_sources: ['claude'], client_protocol: 'anthropic-messages'},
    });
    expect(mergeRoutingProtocol(null, 'openai-chat')).toEqual({
      routing: {client_protocol: 'openai-chat'},
    });
  });

  it('removes only the protocol key when the protocol is null', () => {
    expect(
      mergeRoutingProtocol(
        {voice: 'x', routing: {subscription_sources: ['claude'], client_protocol: 'openai-chat'}},
        null,
      ),
    ).toEqual({voice: 'x', routing: {subscription_sources: ['claude']}});
  });

  it('shows a select on endpoint rows and patches the row on change', () => {
    models.models.set([row()]);
    const fixture = setup(makeProviders([ENDPOINT]));
    fixture.detectChanges();
    const select = fixture.nativeElement.querySelector('.model-row .col-protocol app-select');
    expect(select).not.toBeNull();

    fixture.componentInstance.setProtocol(row(), 'anthropic-messages');
    expect(models.updateModel).toHaveBeenCalledWith('row-1', {
      params_json: {routing: {client_protocol: 'anthropic-messages'}},
    });
  });

  it('keeps the other params keys when patching a row', () => {
    const component = setup(makeProviders([ENDPOINT])).componentInstance;
    component.setProtocol(
      row({params_json: {pricing_id: 'p', routing: {needs_review: true}}}),
      'openai-responses',
    );
    expect(models.updateModel).toHaveBeenCalledWith('row-1', {
      params_json: {
        pricing_id: 'p',
        routing: {needs_review: true, client_protocol: 'openai-responses'},
      },
    });
  });

  it('sends the chosen shape for a new endpoint model', () => {
    const component = setup(makeProviders([ENDPOINT])).componentInstance;
    component.formProviderKey.set('endpoint:ep-vllm');
    expect(component.formClientProtocol()).toBe('openai-chat');
    component.formModelId.set('claude-x');
    component.formClientProtocol.set('anthropic-messages');
    component.submit();
    expect(models.createModel).toHaveBeenCalledTimes(1);
    const body = (models.createModel.mock.calls[0] as unknown[])[0] as {
      params_json: {routing: {client_protocol: string}};
    };
    expect(body.params_json.routing.client_protocol).toBe('anthropic-messages');
  });

  it('hides the select for provider-anchored rows', () => {
    models.models.set([row({provider_kind: 'system', provider_ref: 'openai', client_protocol: null})]);
    const fixture = setup(makeProviders([ENDPOINT]));
    fixture.detectChanges();
    expect(fixture.nativeElement.querySelector('.model-row .col-protocol app-select')).toBeNull();
    expect(fixture.nativeElement.querySelector('.model-row .col-protocol')).not.toBeNull();
  });

  it('drives the row select through the template and patches the row', () => {
    models.models.set([row()]);
    const fixture = setup(makeProviders([ENDPOINT]));
    fixture.detectChanges();
    const select = fixture.nativeElement.querySelector('.model-row .col-protocol app-select');
    // app-select is a stub element here; Angular listens to its `changed` output
    // as a DOM event, so a CustomEvent reaches (changed)="setProtocol(m, $event)".
    const spy = vi.spyOn(fixture.componentInstance, 'setProtocol');
    select.dispatchEvent(new CustomEvent('changed'));
    expect(spy).toHaveBeenCalledTimes(1);
    expect(spy.mock.calls[0][0].id).toBe('row-1');
  });

  it('reloads, resyncs and surfaces the error when the PATCH fails', () => {
    models.updateModel.mockReturnValueOnce(
      throwError(() => ({error: {detail: 'client_protocol not allowed'}})) as never,
    );
    const fixture = setup(makeProviders([ENDPOINT]));
    const component = fixture.componentInstance;
    const before = component.protocolResync();
    component.setProtocol(row(), 'anthropic-messages');
    expect(models.loadModels).toHaveBeenCalled();
    expect(component.protocolResync()).toBe(before + 1);
    expect(component.protocolError()).toContain('client_protocol not allowed');
  });

  it('resyncs the select without a PATCH when the Helm override is cancelled', () => {
    vi.spyOn(window, 'confirm').mockReturnValue(false);
    const component = setup(makeProviders([ENDPOINT])).componentInstance;
    const before = component.protocolResync();
    component.setProtocol(row({managed_by_helm: true}), 'anthropic-messages');
    expect(models.updateModel).not.toHaveBeenCalled();
    expect(component.protocolResync()).toBe(before + 1);
  });

  it('shows the add-form API shape for a plain endpoint but not the subscription proxy', () => {
    const proxy: LlmEndpoint = {...ENDPOINT, id: 'ep-subs', label: 'x', transport_kind: 'subscription-proxy'};
    const fixture = setup(makeProviders([ENDPOINT, proxy]));
    const el = fixture.nativeElement as HTMLElement;
    const hasField = () =>
      Array.from(el.querySelectorAll('app-form-field')).some((f) => f.getAttribute('label') === 'API shape');

    fixture.componentInstance.formProviderKey.set('endpoint:ep-vllm');
    fixture.detectChanges();
    expect(hasField()).toBe(true);

    fixture.componentInstance.formProviderKey.set('endpoint:ep-subs');
    fixture.detectChanges();
    expect(hasField()).toBe(false);
  });
});
