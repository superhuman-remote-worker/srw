import {CUSTOM_ELEMENTS_SCHEMA, signal, ɵresolveComponentResources} from '@angular/core';
import {TestBed} from '@angular/core/testing';
import {TranslocoService} from '@jsverse/transloco';
import {beforeAll, beforeEach, describe, expect, it, vi} from 'vitest';
import {of, Subject} from 'rxjs';
import {AdminModelsService} from '../../../core/services/admin-models.service';
import {AdminProvidersService} from '../../../core/services/admin-providers.service';
import {AdminModelsCoordinatorService} from '../models/admin-models-coordinator.service';
import {AdminCatalogComponent} from './admin-catalog.component';
import {CatalogModel, LlmEndpoint} from '../../../core/models/api.model';

function endpoint(id: string, label: string): LlmEndpoint {
  return {
    id,
    label,
    base_url: `http://${id}:8080`,
    key_prefix: null,
    transport_kind: null,
    created_at: null,
    updated_at: null,
    models: [],
  };
}

function row(providerRef: string, overrides: Partial<CatalogModel> = {}): CatalogModel {
  return {
    id: `${providerRef}-row`,
    provider_kind: 'endpoint',
    provider_ref: providerRef,
    model_id: 'm',
    display_label: 'M',
    capabilities: ['chat'],
    family: 'default',
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
    ...overrides,
  };
}

const SEARXNG_ROW = row('ep-searx', {
  model_id: 'searxng',
  capabilities: ['search'],
  params_json: {provider: 'searxng', ops: ['search']},
});

function setup(rows: CatalogModel[]) {
  const coordinator = {discoverEndpoint$: new Subject<string>()};
  const providers = {
    systemApiKeys: signal([]),
    systemEndpoints: signal([endpoint('ep-searx', 'SearXNG'), endpoint('ep-vllm', 'Local Gemma')]),
    subscriptionAvailability: signal(null),
    loadSystemApiKeys: vi.fn(),
    loadSystemEndpoints: vi.fn(),
    loadSubscriptionAvailability: vi.fn(),
    discoverSystemEndpointModels: vi.fn(() =>
      of({ok: true, status: 200, error: null, probe_url: 'x', models: []}),
    ),
  };
  const models = {
    models: signal(rows),
    families: signal(['default']),
    familyDefaults: signal<Record<string, number>>({}),
    loading: signal(false),
    loadModels: vi.fn(),
    loadFamilies: vi.fn(),
    detectFamily: vi.fn(() => of({family: 'default', source: 'fallback'})),
  };
  TestBed.resetTestingModule();
  TestBed.configureTestingModule({
    imports: [AdminCatalogComponent],
    providers: [
      {provide: TranslocoService, useValue: {translate: vi.fn((key: string) => key)}},
      {provide: AdminModelsService, useValue: models},
      {provide: AdminProvidersService, useValue: providers},
      {provide: AdminModelsCoordinatorService, useValue: coordinator},
    ],
  });
  TestBed.overrideComponent(AdminCatalogComponent, {
    set: {imports: [], schemas: [CUSTOM_ELEMENTS_SCHEMA]},
  });
  const fixture = TestBed.createComponent(AdminCatalogComponent);
  fixture.detectChanges();
  return {fixture, component: fixture.componentInstance, providers, coordinator};
}

describe('AdminCatalogComponent — search/fetch/TTS service endpoints', () => {
  beforeAll(async () => {
    await ɵresolveComponentResources(() => Promise.resolve(''));
  });

  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('explains instead of offering discovery on a service endpoint', () => {
    const {fixture, component} = setup([SEARXNG_ROW]);

    component.formProviderKey.set('endpoint:ep-searx');
    fixture.detectChanges();

    expect(component.selectedService()).toEqual({label: 'SearXNG', adapter: 'searxng'});
    const pane = fixture.nativeElement.querySelector('.discover-pane') as HTMLElement;
    expect(pane.textContent).toContain('SearXNG is served by the searxng adapter');
    expect(pane.textContent).not.toContain('Discover available models');
  });

  it('keeps discovery for a model endpoint, and for one without rows yet', () => {
    const {fixture, component} = setup([
      row('ep-vllm', {model_id: 'tts-1', capabilities: ['tts'], params_json: {provider: 'openai'}}),
    ]);

    component.formProviderKey.set('endpoint:ep-vllm');
    fixture.detectChanges();
    expect(component.selectedService()).toBeNull();
    expect(fixture.nativeElement.textContent).toContain('Discover available models');

    const empty = setup([]);
    empty.component.formProviderKey.set('endpoint:ep-searx');
    expect(empty.component.selectedService()).toBeNull();
  });

  it('does not probe a known service on the Providers-tab handoff', () => {
    const {component, providers, coordinator} = setup([SEARXNG_ROW]);

    coordinator.discoverEndpoint$.next('ep-searx');

    expect(component.formProviderKey()).toBe('endpoint:ep-searx');
    expect(providers.discoverSystemEndpointModels).not.toHaveBeenCalled();
  });
});
