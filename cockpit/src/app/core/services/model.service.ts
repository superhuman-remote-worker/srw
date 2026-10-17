import {inject, Injectable, signal} from '@angular/core';
import {HttpClient} from '@angular/common/http';
import {environment} from '../environment';

export interface ModelGroup {
  group: string;
  provider: string;
  configured: boolean;
  /** Catalog model ids: the value a picker stores and dispatch resolves. */
  models: string[];
  /** model id → the row's display label. Optional for older backends. */
  labels?: Record<string, string>;
}

/**
 * The text a chat picker shows for catalog id `id` in `group`: the row's
 * display label, else the id. The option value stays the id, so two rows
 * serving the same upstream model stay apart by their ids.
 */
export function chatModelLabel(group: Pick<ModelGroup, 'labels'>, id: string): string {
  return group.labels?.[id] || id;
}

/** model id → picker text across every chat group (see {@link chatModelLabel}). */
export function chatModelLabels(groups: readonly ModelGroup[]): Record<string, string> {
  const labels: Record<string, string> = {};
  for (const group of groups) {
    for (const id of group.models) labels[id] = chatModelLabel(group, id);
  }
  return labels;
}

export interface HelperModel {
  id: string;
  label: string;
  configured: boolean;
}

export interface EmbeddingModel extends HelperModel {
  dimensions?: number;
}

/**
 * Per-model reasoning control capability, derived server-side from the model
 * family's `reasoning` block (config/model_config_matrix.yaml). Drives the
 * Cockpit reasoning dropdown so it can't drift from the backend.
 * `method`: effort_enum | binary_toggle | token_budget | always_on | none.
 */
export interface ReasoningCapability {
  method: string;
  default: string | null;
  options: string[];
}

interface ModelsResponse {
  groups: ModelGroup[];
  auxiliary_models: HelperModel[];
  vision_models: HelperModel[];
  whisper_models: HelperModel[];
  tts_models: HelperModel[];
  embedding_models: EmbeddingModel[];
  search_models: HelperModel[];
  fetch_models: HelperModel[];
  /** Memory reranker rows (`rerank` capability). Optional for older backends. */
  rerank_models?: HelperModel[];
  live_voice_models?: HelperModel[];
  configured_providers: string[];
  reasoning_by_model?: Record<string, ReasoningCapability>;
}

/**
 * Fetches the available models from the backend API.
 * Models are filtered server-side based on the user's API key access
 * (system env vars, user keys, project keys).
 */
@Injectable({providedIn: 'root'})
export class ModelService {
  private readonly http = inject(HttpClient);
  private fetchInFlight = false;

  readonly models = signal<ModelGroup[]>([]);
  readonly auxiliaryModels = signal<HelperModel[]>([]);
  readonly visionModels = signal<HelperModel[]>([]);
  readonly whisperModels = signal<HelperModel[]>([]);
  readonly ttsModels = signal<HelperModel[]>([]);
  readonly embeddingModels = signal<EmbeddingModel[]>([]);
  readonly searchModels = signal<HelperModel[]>([]);
  readonly fetchModels = signal<HelperModel[]>([]);
  readonly rerankModels = signal<HelperModel[]>([]);
  readonly liveVoiceModels = signal<HelperModel[]>([]);
  readonly providers = signal<string[]>([]);
  /** model_id → reasoning capability (family-derived); drives the reasoning UI. */
  readonly reasoningByModel = signal<Record<string, ReasoningCapability>>({});
  readonly loading = signal(false);
  readonly loaded = signal(false);

  /**
   * Load available models from the API. Idempotent — skips if already loaded
   * unless `force` is true. Pass `projectId` to include project-level API keys.
   */
  load(projectId?: string, force = false): void {
    if ((this.loaded() || this.fetchInFlight) && !force) return;
    this.fetchInFlight = true;
    this.loading.set(true);

    let url = `${environment.apiUrl}/models`;
    if (projectId) url += `?project_id=${encodeURIComponent(projectId)}`;

    this.http.get<ModelsResponse>(url).subscribe({
      next: (resp) => {
        this.models.set(resp.groups);
        this.auxiliaryModels.set(resp.auxiliary_models ?? []);
        this.visionModels.set(resp.vision_models ?? []);
        this.whisperModels.set(resp.whisper_models ?? []);
        this.ttsModels.set(resp.tts_models ?? []);
        this.embeddingModels.set(resp.embedding_models ?? []);
        this.searchModels.set(resp.search_models ?? []);
        this.fetchModels.set(resp.fetch_models ?? []);
        this.rerankModels.set(resp.rerank_models ?? []);
        this.liveVoiceModels.set(resp.live_voice_models ?? []);
        this.providers.set(resp.configured_providers);
        this.reasoningByModel.set(resp.reasoning_by_model ?? {});
        this.loading.set(false);
        this.loaded.set(true);
        this.fetchInFlight = false;
      },
      error: () => {
        // The DB is the source of truth — on failure, surface an empty
        // catalog so the empty-state banner + disabled pickers render
        // instead of stale, hard-coded fallbacks.
        this.models.set([]);
        this.auxiliaryModels.set([]);
        this.visionModels.set([]);
        this.whisperModels.set([]);
        this.ttsModels.set([]);
        this.embeddingModels.set([]);
        this.searchModels.set([]);
        this.fetchModels.set([]);
        this.rerankModels.set([]);
        this.liveVoiceModels.set([]);
        this.providers.set([]);
        this.reasoningByModel.set({});
        this.loading.set(false);
        this.loaded.set(true);
        this.fetchInFlight = false;
      },
    });
  }
}
