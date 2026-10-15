import {CUSTOM_ELEMENTS_SCHEMA, signal, ɵresolveComponentResources} from '@angular/core';
import {TestBed} from '@angular/core/testing';
import {TranslocoPipe, TranslocoTestingModule, TranslocoService} from '@jsverse/transloco';
import {beforeAll, beforeEach, describe, expect, it, vi} from 'vitest';
import {of, Subject} from 'rxjs';
import {AdminModelsService} from '../../../core/services/admin-models.service';
import {AdminProvidersService} from '../../../core/services/admin-providers.service';
import {AdminModelsCoordinatorService} from '../models/admin-models-coordinator.service';
import {AdminCatalogComponent} from './admin-catalog.component';
import en from '../../../../assets/i18n/en.json';

const createModel = vi.fn(() => of({} as never));

const models = {
  models: signal([]),
  families: signal(['default']),
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
  systemApiKeys: signal([]),
  systemEndpoints: signal([]),
  subscriptionAvailability: signal({
    available: false,
    reachable: false,
    error: null,
    account_count: 0,
    accounts: [],
    models: [],
    proxy_url: null,
    endpoint_id: null,
  }),
  loadSystemApiKeys: vi.fn(),
  loadSystemEndpoints: vi.fn(),
  loadSubscriptionAvailability: vi.fn(),
  discoverSystemEndpointModels: vi.fn(() => of({ok: true, models: []})),
  importSubscriptionModels: vi.fn(() => of({created: [], skipped: [], rejected: []})),
};

describe('AdminCatalogComponent search/fetch form', () => {
  beforeAll(async () => {
    await ɵresolveComponentResources(() => Promise.resolve(''));
  });

  beforeEach(() => {
    createModel.mockClear();
    TestBed.configureTestingModule({
      imports: [AdminCatalogComponent, TranslocoTestingModule.forRoot({langs: {en}, translocoConfig: {availableLangs: ['en'], defaultLang: 'en'}, preloadLangs: true})],
      providers: [
        {provide: AdminModelsService, useValue: models},
        {provide: AdminProvidersService, useValue: providers},
        {
          provide: AdminModelsCoordinatorService,
          useValue: {discoverEndpoint$: new Subject<string>()},
        },
      ],
    });
    TestBed.overrideComponent(AdminCatalogComponent, {
      set: {imports: [TranslocoPipe], schemas: [CUSTOM_ELEMENTS_SCHEMA]},
    });
  });

  it('offers live voice alone, hides chat fields and writes adapter and optional voice', () => {
    const fixture = TestBed.createComponent(AdminCatalogComponent);
    const c = fixture.componentInstance;
    c.toggleFormCapability('live_voice', true);
    expect(c.formCapabilities()).toEqual(['live_voice']);
    c.formProviderKey.set('system:openai');
    c.formModelId.set('gpt-live-1');
    c.formDisplayLabel.set('GPT-Live');
    c.formFamily.set('');
    c.formVoice.set('marin');
    fixture.detectChanges();
    expect(fixture.nativeElement.querySelector('.chat-model-config')).toBeNull();
    expect(fixture.nativeElement.querySelector('.research-config')).toBeNull();
    expect(fixture.nativeElement.querySelector('.live-voice-config')).not.toBeNull();
    expect(c.canSubmit()).toBe(true);
    c.submit();
    expect(createModel).toHaveBeenCalledWith(expect.objectContaining({capabilities: ['live_voice'],
      family: 'default', context_window: null, params_json: {adapter: 'openai-live', voice: 'marin'}}));
    c.toggleFormCapability('chat', true);
    expect(c.formCapabilities()).toEqual(['chat']);
  });

  it('submits without a display label and uses the model ID instead', () => {
    const fixture = TestBed.createComponent(AdminCatalogComponent);
    const c = fixture.componentInstance;
    c.formProviderKey.set('system:openrouter');
    c.formCapabilities.set(['embedding']);
    c.formModelId.set('qwen/qwen3-embedding-8b');
    fixture.detectChanges();
    const labelInput = fixture.nativeElement.querySelectorAll('app-input')[1];
    expect(labelInput.placeholder).toBe('qwen/qwen3-embedding-8b');
    expect(c.canSubmit()).toBe(true);
    c.submit();
    expect(createModel).toHaveBeenCalledWith(expect.objectContaining({
      model_id: 'qwen/qwen3-embedding-8b',
      display_label: 'qwen/qwen3-embedding-8b',
    }));
  });

  it('rejects mixed live voice capabilities even if discovery sets them directly', () => {
    const c = TestBed.createComponent(AdminCatalogComponent).componentInstance;
    c.formProviderKey.set('system:openai');
    c.formCapabilities.set(['live_voice', 'chat']);
    expect(c.canSubmit()).toBe(false);
    c.submit();
    expect(createModel).not.toHaveBeenCalled();
    expect(c.formError()).toBe(en.admin.catalog.liveVoice.onlyCapability);
  });

  it('hides chat-only fields and offers only registered adapter names', () => {
    const fixture = TestBed.createComponent(AdminCatalogComponent);
    const component = fixture.componentInstance;
    component.formCapabilities.set(['search']);
    fixture.detectChanges();

    const host = fixture.nativeElement as HTMLElement;
    expect(host.querySelector('.chat-model-config')).toBeNull();
    expect(host.querySelector('.research-config')).not.toBeNull();
    const adapters = Array.from(
      host.querySelectorAll<HTMLOptionElement>('.research-config app-select option'),
    )
      .map((option) => option.value)
      .filter(Boolean);
    expect(adapters).toEqual(['brave', 'crawl4ai', 'firecrawl', 'searxng', 'tavily']);
  });

  it('requires adapter and matching operations, then writes params_json', () => {
    const fixture = TestBed.createComponent(AdminCatalogComponent);
    const component = fixture.componentInstance;
    component.formProviderKey.set('system:tavily');
    component.formModelId.set('tavily');
    component.formDisplayLabel.set('Tavily');
    component.formCapabilities.set(['search', 'fetch']);

    expect(component.canSubmit()).toBe(false);
    component.onSearchProviderChange('tavily');
    expect(component.canSubmit()).toBe(false);
    component.toggleFormSearchOp('search', true);
    expect(component.canSubmit()).toBe(false);
    component.toggleFormSearchOp('extract', true);
    expect(component.canSubmit()).toBe(true);

    component.submit();
    expect(createModel).toHaveBeenCalledWith(expect.objectContaining({
      capabilities: ['search', 'fetch'],
      family: 'default',
      context_window: null,
      params_json: {provider: 'tavily', ops: ['search', 'extract']},
    }));
  });

  it('limits Crawl4AI rows to extract, crawl and map', () => {
    const fixture = TestBed.createComponent(AdminCatalogComponent);
    const component = fixture.componentInstance;
    component.formSearchOps.set(['search', 'extract', 'crawl', 'map']);

    component.onSearchProviderChange('crawl4ai');

    expect(component.formSearchOps()).toEqual(['extract', 'crawl', 'map']);
    expect(component.isResearchOpSupported('extract')).toBe(true);
    expect(component.isResearchOpSupported('crawl')).toBe(true);
    expect(component.isResearchOpSupported('map')).toBe(true);
    expect(component.isResearchOpSupported('search')).toBe(false);
  });
});
