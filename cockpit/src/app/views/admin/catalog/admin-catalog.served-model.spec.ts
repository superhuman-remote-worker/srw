import {CUSTOM_ELEMENTS_SCHEMA, signal, ɵresolveComponentResources} from '@angular/core';
import {TestBed} from '@angular/core/testing';
import {TranslocoService} from '@jsverse/transloco';
import {beforeAll, beforeEach, describe, expect, it, vi} from 'vitest';
import {of, Subject, throwError} from 'rxjs';
import {AdminModelsService, catalogErrorDetail} from '../../../core/services/admin-models.service';
import {AdminProvidersService} from '../../../core/services/admin-providers.service';
import {AdminModelsCoordinatorService} from '../models/admin-models-coordinator.service';
import {AdminCatalogComponent} from './admin-catalog.component';
import {CatalogModel, LlmEndpoint} from '../../../core/models/api.model';

/**
 * Admin → Models: one model ID names one catalog row across every provider
 * (same_model_id_on_two_providers_routes_to_the_api_key_row.md). The form
 * carries the upstream name as `served_model_id`, warns before the server's
 * 409 when another provider already holds the typed ID, and shows the
 * server's refusal verbatim.
 */

const PROXY: LlmEndpoint = {
  id: 'ep-proxy',
  label: 'Claude subscription proxy',
  base_url: 'http://ai.internal/v1',
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
    provider_ref: 'ep-proxy',
    model_id: 'claude-opus-5-5',
    served_model_id: null,
    display_label: 'Claude Opus 5.5',
    capabilities: ['chat', 'auxiliary'],
    family: 'claude-opus-5-5',
    context_window: null,
    resolved_context_window: 1000000,
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

const createModel = vi.fn();
const models = {
  models: signal<CatalogModel[]>([]),
  families: signal(['default', 'claude-opus-5-5']),
  familyDefaults: signal<Record<string, number>>({}),
  loading: signal(false),
  loadModels: vi.fn(),
  loadFamilies: vi.fn(),
  detectFamily: vi.fn(() => of({family: 'default', source: 'fallback'})),
  createModel,
  updateModel: vi.fn(() => of({})),
  deleteModel: vi.fn(() => of({})),
  testModel: vi.fn(() => of({})),
};

const providers = {
  systemApiKeys: signal([{provider: 'anthropic'}]),
  systemEndpoints: signal([PROXY]),
  subscriptionAvailability: signal(null),
  loadSystemApiKeys: vi.fn(),
  loadSystemEndpoints: vi.fn(),
  loadSubscriptionAvailability: vi.fn(),
  discoverSystemEndpointModels: vi.fn(),
  importSubscriptionModels: vi.fn(),
};

function setup() {
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

describe('AdminCatalogComponent — served model id', () => {
  beforeAll(async () => {
    await ɵresolveComponentResources(() => Promise.resolve(''));
  });

  beforeEach(() => {
    vi.clearAllMocks();
    createModel.mockReturnValue(of({} as never));
    models.models.set([]);
  });

  function fillApiRow(component: AdminCatalogComponent) {
    component.formProviderKey.set('system:anthropic');
    component.formModelId.set('claude-opus-5-5-api');
    component.formFamily.set('claude-opus-5-5');
  }

  it('sends no served_model_id when the field is empty', () => {
    const component = setup().componentInstance;
    fillApiRow(component);
    component.submit();
    expect(createModel).toHaveBeenCalledTimes(1);
    expect(createModel.mock.calls[0][0]).not.toHaveProperty('served_model_id');
  });

  it('sends the trimmed served_model_id when one is set, and clears it after', () => {
    const component = setup().componentInstance;
    fillApiRow(component);
    component.formServedModelId.set('  claude-opus-5-5 ');
    component.submit();
    expect(createModel).toHaveBeenCalledWith(
      expect.objectContaining({model_id: 'claude-opus-5-5-api', served_model_id: 'claude-opus-5-5'}),
    );
    expect(component.formServedModelId()).toBe('');
  });

  it('names the provider that already holds the typed id, with a suggestion', () => {
    models.models.set([row()]);
    const component = setup().componentInstance;
    component.formProviderKey.set('system:anthropic');
    component.formModelId.set('claude-opus-5-5');
    expect(component.modelIdHeldElsewhere()).toBe('Claude subscription proxy (endpoint)');
    expect(component.suggestedModelId()).toBe('claude-opus-5-5-anthropic');
  });

  it('suggests a slug of the endpoint label when the form targets an endpoint', () => {
    models.models.set([row({provider_kind: 'system', provider_ref: 'anthropic'})]);
    const component = setup().componentInstance;
    component.formProviderKey.set('endpoint:ep-proxy');
    component.formModelId.set('claude-opus-5-5');
    expect(component.modelIdHeldElsewhere()).toBe('anthropic');
    expect(component.suggestedModelId()).toBe('claude-opus-5-5-claude-subscription-proxy');
  });

  it('gives no hint for a free id or for the same provider (the server reports that)', () => {
    models.models.set([row()]);
    const component = setup().componentInstance;
    component.formProviderKey.set('system:anthropic');
    component.formModelId.set('claude-opus-5-5-api');
    expect(component.modelIdHeldElsewhere()).toBeNull();
    component.formProviderKey.set('endpoint:ep-proxy');
    component.formModelId.set('claude-opus-5-5');
    expect(component.modelIdHeldElsewhere()).toBeNull();
  });

  it('renders the duplicate hint while the id is typed', () => {
    models.models.set([row()]);
    const fixture = setup();
    const component = fixture.componentInstance;
    component.formProviderKey.set('system:anthropic');
    component.formModelId.set('claude-opus-5-5');
    fixture.detectChanges();
    const hint = (fixture.nativeElement as HTMLElement).querySelector('[data-testid="model-id-taken"]');
    expect(hint?.textContent).toContain('already in the catalog under Claude subscription proxy (endpoint)');
    expect(hint?.textContent).toContain('claude-opus-5-5-anthropic');
  });

  it('shows the server 409 detail verbatim', () => {
    const detail =
      "Model id 'claude-opus-5-5' is already in the catalog under endpoint 'Claude subscription proxy'.";
    createModel.mockReturnValue(throwError(() => ({status: 409, error: {detail}})));
    const component = setup().componentInstance;
    fillApiRow(component);
    component.submit();
    expect(component.formError()).toBe(detail);
  });

  it('joins a validation list into readable text', () => {
    createModel.mockReturnValue(
      throwError(() => ({status: 422, error: {detail: [{msg: 'served_model_id: bad'}, {msg: 'two'}]}})),
    );
    const component = setup().componentInstance;
    fillApiRow(component);
    component.submit();
    expect(component.formError()).toBe('served_model_id: bad; two');
  });

  it('lists a row with its served id next to the model id', () => {
    models.models.set([
      row({
        id: 'row-api',
        provider_kind: 'system',
        provider_ref: 'anthropic',
        model_id: 'claude-opus-5-5-anthropic',
        served_model_id: 'claude-opus-5-5',
      }),
    ]);
    const fixture = setup();
    fixture.detectChanges();
    const ids = Array.from(
      (fixture.nativeElement as HTMLElement).querySelectorAll('.model-row .col-id'),
    ).map((el) => (el.textContent ?? '').trim());
    expect(ids).toEqual(['claude-opus-5-5-anthropic → claude-opus-5-5']);
  });
});

describe('catalogErrorDetail', () => {
  it('prefers a string detail, then a validation list, then the fallback', () => {
    expect(catalogErrorDetail({error: {detail: 'taken'}}, 'x')).toBe('taken');
    expect(catalogErrorDetail({error: {detail: [{msg: 'a'}, 'b']}}, 'x')).toBe('a; b');
    expect(catalogErrorDetail({error: {detail: []}}, 'x')).toBe('x');
    expect(catalogErrorDetail({status: 500}, 'x')).toBe('x');
    expect(catalogErrorDetail(null, 'x')).toBe('x');
  });
});
