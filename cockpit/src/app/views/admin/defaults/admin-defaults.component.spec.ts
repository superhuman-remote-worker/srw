import {CUSTOM_ELEMENTS_SCHEMA, signal, ɵresolveComponentResources} from '@angular/core';
import {TestBed} from '@angular/core/testing';
import {TranslocoTestingModule} from '@jsverse/transloco';
import {TranslocoPipe} from '@jsverse/transloco';
import {beforeAll, beforeEach, describe, expect, it, vi} from 'vitest';
import {of} from 'rxjs';
import en from '../../../../assets/i18n/en.json';
import {AdminProvidersService, HelmManagedOverview} from '../../../core/services/admin-providers.service';
import {ModelService} from '../../../core/services/model.service';
import {ImageGenerationSettings} from '../../../core/models/api.model';
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
const imageGenerationSettings = signal<ImageGenerationSettings | null>({
  max_paid_images: 20,
  proxy_quota_threshold_percent: 80,
  updated_at: null,
  updated_by: null,
});
const saveImageGenerationSettings = vi.fn(() => of(imageGenerationSettings()!));
const admin = {
  defaults,
  loadDefaults: vi.fn(),
  setDefault,
  helmManaged,
  loadHelmManaged: vi.fn(),
  imageGenerationSettings,
  loadImageGenerationSettings: vi.fn(),
  saveImageGenerationSettings,
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
  liveVoiceModels: signal([{id: 'gpt-live-1', label: 'GPT-Live', configured: true}]),
  imageGenerationModels: signal([{id: 'gpt-image-2.6-flare', label: 'GPT Image 2.6 Flare', configured: true}]),
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

  it('offers an optional live voice default from its helper bucket', () => {
    const fixture = TestBed.createComponent(AdminDefaultsComponent);
    fixture.detectChanges();
    const fields = Array.from((fixture.nativeElement as HTMLElement).querySelectorAll('app-form-field'));
    const live = fields.find((field) => (field as unknown as {label: string}).label === 'Live voice');
    expect(live).toBeDefined();
    expect(live?.querySelector('option[value=""]')?.textContent?.trim()).toBe('None');
    expect(live?.querySelector('option[value="gpt-live-1"]')?.textContent?.trim()).toBe('GPT-Live');
    fixture.componentInstance.setDefault('live_voice', 'gpt-live-1');
    expect(setDefault).toHaveBeenCalledWith('live_voice', 'gpt-live-1');
  });

  it('offers a pin-only image generation default from its helper bucket', () => {
    const fixture = TestBed.createComponent(AdminDefaultsComponent);
    fixture.detectChanges();
    const fields = Array.from((fixture.nativeElement as HTMLElement).querySelectorAll('app-form-field'));
    const image = fields.find(
      (field) => (field as unknown as {label: string}).label === 'Image generation (agent image tool)',
    );
    expect(image).toBeDefined();
    // Pin-only: the empty choice means "no image tool", so it reads None.
    expect(image?.querySelector('option[value=""]')?.textContent?.trim()).toBe('None');
    expect(image?.querySelector('option[value="gpt-image-2.6-flare"]')?.textContent?.trim())
      .toBe('GPT Image 2.6 Flare');
    fixture.componentInstance.setDefault('image_generation', 'gpt-image-2.6-flare');
    expect(setDefault).toHaveBeenCalledWith('image_generation', 'gpt-image-2.6-flare');
  });

  it('shows the paid-image cap and the subscription threshold under the image pin', () => {
    saveImageGenerationSettings.mockClear();
    const fixture = TestBed.createComponent(AdminDefaultsComponent);
    fixture.detectChanges();
    const root = fixture.nativeElement as HTMLElement;
    const block = root.querySelector('[data-testid="image-generation-settings"]');
    expect(block).not.toBeNull();
    const labels = Array.from(block!.querySelectorAll('app-form-field')).map(
      (field) => (field as unknown as {label: string}).label,
    );
    expect(labels).toEqual(['Paid images per job or Session', 'Subscription threshold (%)']);
    expect(admin.loadImageGenerationSettings).toHaveBeenCalled();

    fixture.componentInstance.saveImageSetting('max_paid_images', '0', 0, 10000);
    expect(saveImageGenerationSettings).toHaveBeenCalledWith({max_paid_images: 0});
    fixture.componentInstance.saveImageSetting('proxy_quota_threshold_percent', '90', 1, 100);
    expect(saveImageGenerationSettings).toHaveBeenCalledWith({proxy_quota_threshold_percent: 90});
  });

  it('refuses an image setting out of range without calling the server', () => {
    saveImageGenerationSettings.mockClear();
    const fixture = TestBed.createComponent(AdminDefaultsComponent);
    fixture.detectChanges();
    fixture.componentInstance.saveImageSetting('proxy_quota_threshold_percent', '0', 1, 100);
    fixture.componentInstance.saveImageSetting('max_paid_images', '2.5', 0, 10000);
    expect(saveImageGenerationSettings).not.toHaveBeenCalled();
    fixture.detectChanges();
    expect(
      (fixture.nativeElement as HTMLElement).querySelector('[role="alert"]')?.textContent,
    ).toContain('Enter a whole number in range.');
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
