import {describe, expect, it, vi} from 'vitest';
import {Injector, runInInjectionContext} from '@angular/core';
import {HttpClient} from '@angular/common/http';
import {of} from 'rxjs';
import {AdminModelsService} from './admin-models.service';
import {AdminProvidersService} from './admin-providers.service';
import {ReadinessService} from './readiness.service';
import {ModelService} from './model.service';

function createService(mockHttp?: any) {
  const http = mockHttp ?? {
    get: vi.fn().mockReturnValue(of([])),
    put: vi.fn().mockReturnValue(of({})),
    post: vi.fn().mockReturnValue(of({})),
    patch: vi.fn().mockReturnValue(of({})),
    delete: vi.fn().mockReturnValue(of({status: 'deleted'})),
  };

  const injector = Injector.create({
    providers: [
      {provide: HttpClient, useValue: http},
      {provide: ReadinessService, useValue: {load: vi.fn()}},
      {provide: ModelService, useValue: {load: vi.fn()}},
      {provide: AdminModelsService, useValue: {loadModels: vi.fn()}},
    ],
  });

  const service = runInInjectionContext(injector, () => new AdminProvidersService());
  return {service, http, adminModels: injector.get(AdminModelsService)};
}

describe('AdminProvidersService', () => {
  describe('system api keys', () => {
    it('loads keys from /admin/providers/keys', () => {
      const http = {
        get: vi.fn().mockReturnValue(of([
          {
            id: '1', provider: 'openai', key_prefix: 'sk-proj-',
            label: 'seed', seeded_from: 'helm:llm.seed',
            created_at: null, updated_at: null,
          },
        ])),
        put: vi.fn(), post: vi.fn(), patch: vi.fn(), delete: vi.fn(),
      };
      const {service} = createService(http);
      service.loadSystemApiKeys();
      expect(http.get).toHaveBeenCalledWith(expect.stringContaining('/admin/providers/keys'));
      expect(service.systemApiKeys()).toHaveLength(1);
      expect(service.systemApiKeys()[0].seeded_from).toBe('helm:llm.seed');
    });

    it('PUTs a key to /admin/providers/keys/{provider}', () => {
      const http = {
        get: vi.fn().mockReturnValue(of([])),
        put: vi.fn().mockReturnValue(of({id: '1', provider: 'openai', key_prefix: 'sk-new-',
          label: null, seeded_from: null, created_at: null, updated_at: null})),
        post: vi.fn(), patch: vi.fn(), delete: vi.fn(),
      };
      const {service} = createService(http);
      service.setSystemApiKey('openai', {api_key: 'sk-new-abc'}).subscribe();
      expect(http.put).toHaveBeenCalledWith(
        expect.stringContaining('/admin/providers/keys/openai'),
        {api_key: 'sk-new-abc'},
      );
      expect(http.get).toHaveBeenCalled(); // tap reloads
    });

    it('DELETEs a key and refreshes the list', () => {
      const http = {
        get: vi.fn().mockReturnValue(of([])),
        delete: vi.fn().mockReturnValue(of({status: 'deleted'})),
        put: vi.fn(), post: vi.fn(), patch: vi.fn(),
      };
      const {service} = createService(http);
      service.deleteSystemApiKey('anthropic').subscribe();
      expect(http.delete).toHaveBeenCalledWith(
        expect.stringContaining('/admin/providers/keys/anthropic'),
      );
      expect(http.get).toHaveBeenCalled();
    });
  });

  describe('system endpoints', () => {
    it('loads endpoints from /admin/providers/endpoints', () => {
      const http = {
        get: vi.fn().mockReturnValue(of([
          {id: 'ep-1', label: 'Local vLLM', base_url: 'http://vllm/v1', key_prefix: null,
           created_at: null, updated_at: null, models: []},
        ])),
        put: vi.fn(), post: vi.fn(), patch: vi.fn(), delete: vi.fn(),
      };
      const {service} = createService(http);
      service.loadSystemEndpoints();
      expect(http.get).toHaveBeenCalledWith(expect.stringContaining('/admin/providers/endpoints'));
      expect(service.systemEndpoints()).toHaveLength(1);
    });

    it('POSTs to the test endpoint', () => {
      const http = {
        get: vi.fn(),
        post: vi.fn().mockReturnValue(of({ok: true, status: 200, error: null, probe_url: ''})),
        put: vi.fn(), patch: vi.fn(), delete: vi.fn(),
      };
      const {service} = createService(http);
      service.testSystemEndpoint('ep-1').subscribe();
      expect(http.post).toHaveBeenCalledWith(
        expect.stringContaining('/admin/providers/endpoints/ep-1/test'),
        {},
      );
    });

    it('POSTs to /discover and returns the parsed model list', () => {
      const http = {
        get: vi.fn(),
        post: vi.fn().mockReturnValue(of({
          ok: true, status: 200, error: null, probe_url: 'http://vllm/v1/models',
          models: [{id: 'gemma', owned_by: 'vllm',
                    capability_hints: ['chat', 'auxiliary'],
                    family: null, context_window: null}],
        })),
        put: vi.fn(), patch: vi.fn(), delete: vi.fn(),
      };
      const {service} = createService(http);
      let got: any = null;
      service.discoverSystemEndpointModels('ep-1').subscribe((r) => (got = r));
      expect(http.post).toHaveBeenCalledWith(
        expect.stringContaining('/admin/providers/endpoints/ep-1/discover'),
        {},
      );
      expect(got.models[0].id).toBe('gemma');
    });

    it('sends the context cap on a bulk import and reloads the catalog table', () => {
      const http = {
        get: vi.fn(),
        post: vi.fn().mockReturnValue(of({created: ['gpt-5.5'], skipped: [], rejected: []})),
        put: vi.fn(), patch: vi.fn(), delete: vi.fn(),
      };
      const {service, adminModels} = createService(http);
      service.importSubscriptionModels('ep-subs', ['gpt-5.5'], false, 200000).subscribe();
      expect(http.post).toHaveBeenCalledWith(
        expect.stringContaining('/admin/providers/endpoints/ep-subs/models/import'),
        {model_ids: ['gpt-5.5'], include_needs_review: false, context_window_cap: 200000},
      );
      expect(adminModels.loadModels).toHaveBeenCalledTimes(1);
    });
  });

  describe('defaults', () => {
    it('loads all default-kind values including search and fetch slots', () => {
      const http = {
        get: vi.fn().mockReturnValue(of({
          builder: 'gpt-4o', browser: null, citation: 'gpt-4',
          embedding: 'text-embedding-3-large', vision: null, auxiliary: 'gpt-4o-mini',
          search: 'tavily', fetch: 'tavily', search_fallback: 'searxng',
        })),
        put: vi.fn(), post: vi.fn(), patch: vi.fn(), delete: vi.fn(),
      };
      const {service} = createService(http);
      service.loadDefaults();
      expect(http.get).toHaveBeenCalledWith(expect.stringContaining('/admin/providers/defaults'));
      const defaults = service.defaults();
      expect(defaults.builder).toBe('gpt-4o');
      expect(defaults.embedding).toBe('text-embedding-3-large');
      expect(defaults.auxiliary).toBe('gpt-4o-mini');
      expect(defaults.vision).toBe(null);
      expect(defaults.search).toBe('tavily');
      expect(defaults.fetch).toBe('tavily');
      expect(defaults.search_fallback).toBe('searxng');
    });

    it('fills missing slots with null when the server omits them', () => {
      // Older backends return only builder/browser/citation — the client
      // must still render nulls for the new slots rather than crashing.
      const http = {
        get: vi.fn().mockReturnValue(of({builder: 'gpt-4o'})),
        put: vi.fn(), post: vi.fn(), patch: vi.fn(), delete: vi.fn(),
      };
      const {service} = createService(http);
      service.loadDefaults();
      expect(service.defaults().embedding).toBeNull();
      expect(service.defaults().vision).toBeNull();
      expect(service.defaults().auxiliary).toBeNull();
      expect(service.defaults().search).toBeNull();
      expect(service.defaults().fetch).toBeNull();
      expect(service.defaults().search_fallback).toBeNull();
      expect(service.defaults().rerank).toBeNull();
    });

    it('PUTs a new embedding default', () => {
      const http = {
        get: vi.fn().mockReturnValue(of({})),
        put: vi.fn().mockReturnValue(of({kind: 'embedding', model: 'qwen3-embedding-8b'})),
        post: vi.fn(), patch: vi.fn(), delete: vi.fn(),
      };
      const {service} = createService(http);
      service.setDefault('embedding', 'qwen3-embedding-8b').subscribe();
      expect(http.put).toHaveBeenCalledWith(
        expect.stringContaining('/admin/providers/defaults/embedding'),
        {model: 'qwen3-embedding-8b'},
      );
    });

    it('PUTs a whisper default', () => {
      const http = {
        get: vi.fn().mockReturnValue(of({})),
        put: vi.fn().mockReturnValue(of({kind: 'whisper', model: 'whisper-large-v3'})),
        post: vi.fn(), patch: vi.fn(), delete: vi.fn(),
      };
      const {service} = createService(http);
      service.setDefault('whisper', 'whisper-large-v3').subscribe();
      expect(http.put).toHaveBeenCalledWith(
        expect.stringContaining('/admin/providers/defaults/whisper'),
        {model: 'whisper-large-v3'},
      );
    });

    it('PUTs a tts default', () => {
      const http = {
        get: vi.fn().mockReturnValue(of({})),
        put: vi.fn().mockReturnValue(of({kind: 'tts', model: 'tts-1'})),
        post: vi.fn(), patch: vi.fn(), delete: vi.fn(),
      };
      const {service} = createService(http);
      service.setDefault('tts', 'tts-1').subscribe();
      expect(http.put).toHaveBeenCalledWith(
        expect.stringContaining('/admin/providers/defaults/tts'),
        {model: 'tts-1'},
      );
    });

    it('exposes whisper and tts in EMPTY_DEFAULTS shape', () => {
      const http = {
        get: vi.fn().mockReturnValue(of({})),
        put: vi.fn(), post: vi.fn(), patch: vi.fn(), delete: vi.fn(),
      };
      const {service} = createService(http);
      service.loadDefaults();
      // New slots default to null when the server omits them.
      expect(service.defaults().whisper).toBeNull();
      expect(service.defaults().tts).toBeNull();
    });

    it('exposes chat slot in EMPTY_DEFAULTS shape', () => {
      // Chunk 4 of model_capabilities_array surfaces `chat` in the
      // Defaults panel so the readiness gate's "Pin a default for: chat"
      // requirement has a UI to fulfill.
      const http = {
        get: vi.fn().mockReturnValue(of({})),
        put: vi.fn(), post: vi.fn(), patch: vi.fn(), delete: vi.fn(),
      };
      const {service} = createService(http);
      service.loadDefaults();
      expect(service.defaults().chat).toBeNull();
    });

    it('PUTs a chat default', () => {
      const http = {
        get: vi.fn().mockReturnValue(of({})),
        put: vi.fn().mockReturnValue(of({kind: 'chat', model: 'gpt-4o'})),
        post: vi.fn(), patch: vi.fn(), delete: vi.fn(),
      };
      const {service} = createService(http);
      service.setDefault('chat', 'gpt-4o').subscribe();
      expect(http.put).toHaveBeenCalledWith(
        expect.stringContaining('/admin/providers/defaults/chat'),
        {model: 'gpt-4o'},
      );
    });

    it('round-trips an explicit None search fallback as an empty model', () => {
      const http = {
        get: vi.fn().mockReturnValue(of({})),
        put: vi.fn().mockReturnValue(of({kind: 'search_fallback', model: null})),
        post: vi.fn(), patch: vi.fn(), delete: vi.fn(),
      };
      const {service} = createService(http);
      service.setDefault('search_fallback', '').subscribe();
      expect(http.put).toHaveBeenCalledWith(
        expect.stringContaining('/admin/providers/defaults/search_fallback'),
        {model: ''},
      );
    });
  });
});
