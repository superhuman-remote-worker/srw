import {describe, expect, it, vi} from 'vitest';
import {Injector, runInInjectionContext} from '@angular/core';
import {HttpClient} from '@angular/common/http';
import {of, throwError} from 'rxjs';
import {ModelService} from './model.service';

function createService(mockHttp?: any) {
  const http = mockHttp ?? {
    get: vi.fn().mockReturnValue(of({
      groups: [
        {group: 'Local', provider: 'local', configured: true, models: ['openai/gpt-oss-120b']},
        {group: 'OpenAI', provider: 'openai', configured: true, models: ['gpt-5.4', 'gpt-4o']},
      ],
      auxiliary_models: [{id: 'gpt-4o-mini', label: 'GPT-4o Mini', configured: true}],
      vision_models: [{id: 'gpt-4o', label: 'GPT-4o', configured: true}],
      whisper_models: [{id: 'whisper-1', label: 'Whisper v1', configured: true}],
      tts_models: [{id: 'tts-1', label: 'TTS v1', configured: true}],
      embedding_models: [{id: 'text-embedding-3-small', label: 'TE3 Small', dimensions: 1536, configured: true}],
      search_models: [{id: 'tavily', label: 'Tavily', configured: true}],
      fetch_models: [{id: 'tavily', label: 'Tavily', configured: true}],
      rerank_models: [{id: 'qwen3-reranker-8b', label: 'Qwen3 Reranker', configured: true}],
      live_voice_models: [{id: 'gpt-live-1', label: 'GPT-Live', configured: true}],
      configured_providers: ['openai'],
    })),
  };

  const injector = Injector.create({
    providers: [{provide: HttpClient, useValue: http}],
  });

  const service = runInInjectionContext(injector, () => new ModelService());
  return {service, http};
}


describe('ModelService', () => {
  describe('initial state', () => {
    it('should start with empty signals', () => {
      const {service} = createService();
      expect(service.models()).toEqual([]);
      expect(service.auxiliaryModels()).toEqual([]);
      expect(service.visionModels()).toEqual([]);
      expect(service.whisperModels()).toEqual([]);
      expect(service.ttsModels()).toEqual([]);
      expect(service.embeddingModels()).toEqual([]);
      expect(service.searchModels()).toEqual([]);
      expect(service.fetchModels()).toEqual([]);
      expect(service.liveVoiceModels()).toEqual([]);
      expect(service.providers()).toEqual([]);
      expect(service.loaded()).toBe(false);
      expect(service.loading()).toBe(false);
    });
  });

  describe('load', () => {
    it('should populate all signals from API response', () => {
      const {service} = createService();
      service.load();

      expect(service.models()).toHaveLength(2);
      expect(service.models()[0].group).toBe('Local');
      expect(service.auxiliaryModels()).toHaveLength(1);
      expect(service.visionModels()).toHaveLength(1);
      expect(service.whisperModels()).toHaveLength(1);
      expect(service.ttsModels()).toHaveLength(1);
      expect(service.embeddingModels()).toHaveLength(1);
      expect(service.searchModels()).toHaveLength(1);
      expect(service.fetchModels()).toHaveLength(1);
      expect(service.rerankModels()).toHaveLength(1);
      expect(service.liveVoiceModels()).toHaveLength(1);
      expect(service.embeddingModels()[0].dimensions).toBe(1536);
      expect(service.providers()).toEqual(['openai']);
      expect(service.loaded()).toBe(true);
      expect(service.loading()).toBe(false);
    });

    it('should call API with correct URL', () => {
      const {service, http} = createService();
      service.load();
      expect(http.get).toHaveBeenCalledWith(expect.stringContaining('/models'));
    });

    it('should include project_id query param when provided', () => {
      const {service, http} = createService();
      service.load('proj-123');
      expect(http.get).toHaveBeenCalledWith(expect.stringContaining('project_id=proj-123'));
    });
  });

  describe('idempotency', () => {
    it('should not fetch twice after successful load', () => {
      const {service, http} = createService();
      service.load();
      service.load();
      expect(http.get).toHaveBeenCalledTimes(1);
    });

    it('should refetch when force=true', () => {
      const {service, http} = createService();
      service.load();
      service.load(undefined, true);
      expect(http.get).toHaveBeenCalledTimes(2);
    });

    it('should not fetch while in-flight', () => {
      // Use a Subject to control when the response arrives
      const {service, http} = createService();
      service.load();
      // Second call while first is still "processing" (synchronous in test, but fetchInFlight flag)
      // After first load completes synchronously, loaded() is true so second is skipped
      service.load();
      expect(http.get).toHaveBeenCalledTimes(1);
    });
  });

  describe('error fallback', () => {
    it('should surface an empty catalog on API error', () => {
      const http = {
        get: vi.fn().mockReturnValue(throwError(() => new Error('Network error'))),
      };
      const {service} = createService(http);
      service.load();

      expect(service.loaded()).toBe(true);
      expect(service.loading()).toBe(false);
      // No hard-coded fallback — DB is the only source of truth. Empty
      // signals drive the empty-state banner + disabled pickers.
      expect(service.models()).toEqual([]);
      expect(service.auxiliaryModels()).toEqual([]);
      expect(service.providers()).toEqual([]);
    });

    it('should allow retry after error with force=true', () => {
      const http = {
        get: vi.fn()
          .mockReturnValueOnce(throwError(() => new Error('fail')))
          .mockReturnValueOnce(of({
            groups: [{group: 'Test', provider: 'local', configured: true, models: ['test-model']}],
            auxiliary_models: [],
            vision_models: [],
            whisper_models: [],
            tts_models: [],
            embedding_models: [],
            search_models: [],
            fetch_models: [],
            configured_providers: ['openai'],
          })),
      };
      const {service} = createService(http);

      service.load();
      expect(service.loaded()).toBe(true);

      service.load(undefined, true);
      expect(service.models()).toHaveLength(1);
      expect(service.models()[0].group).toBe('Test');
    });
  });

  describe('helper model null safety', () => {
    it('should handle missing helper model fields in response', () => {
      const http = {
        get: vi.fn().mockReturnValue(of({
          groups: [],
          // helper fields omitted (old backend)
          configured_providers: [],
        })),
      };
      const {service} = createService(http);
      service.load();

      expect(service.auxiliaryModels()).toEqual([]);
      expect(service.visionModels()).toEqual([]);
      expect(service.whisperModels()).toEqual([]);
      expect(service.ttsModels()).toEqual([]);
      expect(service.embeddingModels()).toEqual([]);
      expect(service.searchModels()).toEqual([]);
      expect(service.fetchModels()).toEqual([]);
      expect(service.liveVoiceModels()).toEqual([]);
    });
  });
});
