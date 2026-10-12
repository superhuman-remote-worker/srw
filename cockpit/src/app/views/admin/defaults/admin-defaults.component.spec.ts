import {CUSTOM_ELEMENTS_SCHEMA, signal, ɵresolveComponentResources} from '@angular/core';
import {TestBed} from '@angular/core/testing';
import {TranslocoTestingModule} from '@jsverse/transloco';
import {TranslocoPipe} from '@jsverse/transloco';
import {beforeAll, beforeEach, describe, expect, it, vi} from 'vitest';
import {of} from 'rxjs';
import en from '../../../../assets/i18n/en.json';
import {AdminProvidersService, HelmManagedOverview} from '../../../core/services/admin-providers.service';
import {ModelService} from '../../../core/services/model.service';
import {AdminDefaultsComponent} from './admin-defaults.component';

const defaults = signal<Record<string, string | null>>({
  chat: null,
  browser: null,
  citation: null,
  embedding: null,
  vision: null,
  auxiliary: null,
  whisper: null,
  tts: null,
  search: 'tavily',
  fetch: 'tavily',
  search_fallback: 'searxng',
  rerank: null,
});
const setDefault = vi.fn(() => of({kind: 'search_fallback', model: null}));

const helmManaged = signal<HelmManagedOverview | null>(null);
const admin = {
  defaults,
  loadDefaults: vi.fn(),
  setDefault,
  helmManaged,
  loadHelmManaged: vi.fn(),
};

const helperModels = [
  {id: 'tavily', label: 'Tavily', configured: true},
  {id: 'searxng', label: 'SearXNG', configured: true},
];
const modelService = {
  models: signal([]),
  auxiliaryModels: signal([]),
  embeddingModels: signal([]),
  visionModels: signal([]),
  whisperModels: signal([]),
  ttsModels: signal([]),
  searchModels: signal(helperModels),
  fetchModels: signal(helperModels.slice(0, 1)),
  rerankModels: signal([]),
  load: vi.fn(),
};

describe('AdminDefaultsComponent search providers', () => {
  beforeAll(async () => {
    await ɵresolveComponentResources(() => Promise.resolve(''));
  });

  beforeEach(() => {
    defaults.set({
      ...defaults(),
      search: 'tavily',
      search_fallback: 'searxng',
    });
    setDefault.mockClear();
    TestBed.configureTestingModule({
      imports: [
        AdminDefaultsComponent,
        TranslocoTestingModule.forRoot({
          langs: {en},
          translocoConfig: {availableLangs: ['en'], defaultLang: 'en'},
        }),
      ],
      providers: [
        {provide: AdminProvidersService, useValue: admin},
        {provide: ModelService, useValue: modelService},
      ],
    });
    TestBed.overrideComponent(AdminDefaultsComponent, {
      set: {imports: [TranslocoPipe], schemas: [CUSTOM_ELEMENTS_SCHEMA]},
    });
  });

  it('offers an explicit None fallback and round-trips clearing it', () => {
    const fixture = TestBed.createComponent(AdminDefaultsComponent);
    fixture.detectChanges();
    const selects = (fixture.nativeElement as HTMLElement).querySelectorAll('app-select');
    const fallback = selects.item(selects.length - 1);
    const none = fallback.querySelector<HTMLOptionElement>('option[value=""]');
    expect(none?.textContent?.trim()).toBe('None');

    fixture.componentInstance.setDefault('search_fallback', '');
    expect(setDefault).toHaveBeenCalledWith('search_fallback', '');
  });

  it('warns when fallback and primary resolve to the same row', () => {
    defaults.update((current) => ({...current, search_fallback: current['search']}));
    const fixture = TestBed.createComponent(AdminDefaultsComponent);
    fixture.detectChanges();
    expect(
      (fixture.nativeElement as HTMLElement).querySelector('.default-warning')
        ?.textContent,
    ).toContain('runtime failover is suppressed');
  });

  it('labels a pin the system chose until the admin picks another', () => {
    const autoPin = {model: 'qwen3-reranker-8b', source: 'default' as const, managed_by_helm: false, helm_drift: false, auto_pinned: true};
    helmManaged.set({
      manifest: {systemApiKeys: [], systemEndpoints: [], models: [], defaults: []},
      applied_at: null,
      keys: [],
      endpoints: [],
      models: [],
      defaults: {rerank: autoPin},
    });
    defaults.update((current) => ({...current, rerank: 'qwen3-reranker-8b'}));
    const fixture = TestBed.createComponent(AdminDefaultsComponent);
    fixture.detectChanges();
    const hints = () => (fixture.nativeElement as HTMLElement).querySelectorAll('.default-hint');
    expect(hints().length).toBe(1);
    expect(hints()[0].textContent).toContain('Picked automatically');

    // The selection moved off the auto pin: the label goes with it.
    defaults.update((current) => ({...current, rerank: 'other-reranker'}));
    fixture.detectChanges();
    expect(hints().length).toBe(0);

    helmManaged.set(null);
    defaults.update((current) => ({...current, rerank: null}));
  });
});
