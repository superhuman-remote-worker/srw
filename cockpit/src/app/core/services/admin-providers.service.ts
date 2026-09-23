import {inject, Injectable, signal} from '@angular/core';
import {HttpClient} from '@angular/common/http';
import {catchError, Observable, of, tap} from 'rxjs';
import {
  ApiKeyProvider,
  ApiKeySetRequest,
  DiscoveryResponse,
  HelmProvenanceSource,
  LlmEndpoint,
  LlmEndpointCreateRequest,
  LlmEndpointDiscoveryResult,
  LlmEndpointTestResult,
  LlmEndpointUpdateRequest,
  SubscriptionAccount,
  SubscriptionDiscoveryResult,
  SubscriptionImportResult,
} from '../models/api.model';
import {environment} from '../environment';
import {ModelService} from './model.service';
import {ReadinessService} from './readiness.service';

/**
 * A system-scoped provider API key row. Mirrors `user_api_keys` but with a
 * `seeded_from` breadcrumb identifying rows created by `helm.llm.seed`.
 */
export interface SystemApiKeyEntry {
  id: string;
  provider: ApiKeyProvider;
  key_prefix: string | null;
  label: string | null;
  seeded_from: string | null;
  created_at: string | null;
  updated_at: string | null;
  source?: HelmProvenanceSource | null;
  managed_by_helm?: boolean;
  helm_drift?: boolean;
}

/** One default pin as `GET /api/admin/helm-managed` reports it. */
export interface HelmManagedDefault {
  model: string | null;
  source: HelmProvenanceSource | null;
  managed_by_helm: boolean;
  helm_drift: boolean;
  /** The system pinned it because a required kind had models but no pin. */
  auto_pinned?: boolean;
}

/**
 * `GET /api/admin/helm-managed`: the reconcile manifest the seed Job last
 * wrote plus per-row provenance. Keys/endpoints/models carry the same
 * annotation on their list endpoints; the defaults are only available here.
 */
export interface HelmManagedOverview {
  manifest: {
    systemApiKeys: string[];
    systemEndpoints: string[];
    models: string[];
    defaults: string[];
  };
  applied_at: string | null;
  keys: SystemApiKeyEntry[];
  endpoints: LlmEndpoint[];
  models: {
    id: string;
    provider_kind: string;
    provider_ref: string;
    model_id: string;
    source: HelmProvenanceSource | null;
    managed_by_helm: boolean;
    helm_drift: boolean;
  }[];
  defaults: Partial<Record<DefaultModelKind, HelmManagedDefault>>;
}

/**
 * Admins can pin cluster-wide defaults for each of these slots via the
 * Admin → Providers → Defaults section. Keep this in sync with the
 * orchestrator's `VALID_DEFAULT_MODEL_KINDS` — unknown kinds are rejected
 * server-side. The `chat` slot is the cluster-wide chat default used by
 * the dispatcher when a job/session has no override; surfacing it here
 * fulfills the readiness gate's `Pin a default for: chat` requirement.
 */
export type DefaultModelKind =
  | 'chat'
  | 'browser'
  | 'citation'
  | 'embedding'
  | 'vision'
  | 'auxiliary'
  | 'whisper'
  | 'tts'
  | 'search'
  | 'fetch'
  | 'search_fallback'
  | 'rerank';

export const DEFAULT_MODEL_KINDS: DefaultModelKind[] = [
  'chat',
  'browser',
  'citation',
  'embedding',
  'rerank',
  'vision',
  'auxiliary',
  'whisper',
  'tts',
  'search',
  'fetch',
  'search_fallback',
];

const EMPTY_DEFAULTS: Record<DefaultModelKind, string | null> = {
  chat: null,
  browser: null,
  citation: null,
  embedding: null,
  vision: null,
  auxiliary: null,
  whisper: null,
  tts: null,
  search: null,
  fetch: null,
  search_fallback: null,
  rerank: null,
};

/**
 * Reported by `GET /api/admin/providers/subscriptions/availability`. Used by
 * Admin → Models to decide whether the seeded subscription-proxy endpoint is
 * usable right now (proxy reachable + at least one healthy account).
 */
export interface SubscriptionAvailability {
  available: boolean;
  /** False when the proxy itself cannot be reached — distinct from signed-out. */
  reachable: boolean;
  error: string | null;
  account_count: number;
  accounts: SubscriptionAccount[];
  models: string[];
  proxy_url: string | null;
  endpoint_id: string | null;
  transport_kind?: string;
}

const EMPTY_SUBSCRIPTION_AVAILABILITY: SubscriptionAvailability = {
  available: false,
  reachable: false,
  error: null,
  account_count: 0,
  accounts: [],
  models: [],
  proxy_url: null,
  endpoint_id: null,
};

/**
 * REST client for the `/api/admin/providers/*` surface. Every call is gated
 * by the `srw-admin` role server-side; the client does not re-check.
 */
@Injectable({providedIn: 'root'})
export class AdminProvidersService {
  private readonly http = inject(HttpClient);
  private readonly readiness = inject(ReadinessService);
  private readonly modelService = inject(ModelService);
  private readonly baseUrl = environment.apiUrl;

  readonly systemApiKeys = signal<SystemApiKeyEntry[]>([]);
  readonly systemEndpoints = signal<LlmEndpoint[]>([]);
  readonly defaults = signal<Record<DefaultModelKind, string | null>>({...EMPTY_DEFAULTS});
  /** Helm reconcile manifest + provenance; null until loaded or when unavailable. */
  readonly helmManaged = signal<HelmManagedOverview | null>(null);
  readonly subscriptionAvailability = signal<SubscriptionAvailability>({
    ...EMPTY_SUBSCRIPTION_AVAILABILITY,
  });

  // ── System API Keys ───────────────────────────────────────────────

  loadSystemApiKeys(): void {
    this.http
      .get<SystemApiKeyEntry[]>(`${this.baseUrl}/admin/providers/keys`)
      .pipe(catchError(() => of([] as SystemApiKeyEntry[])))
      .subscribe((rows) => this.systemApiKeys.set(rows));
  }

  setSystemApiKey(provider: string, body: ApiKeySetRequest): Observable<SystemApiKeyEntry> {
    return this.http
      .put<SystemApiKeyEntry>(`${this.baseUrl}/admin/providers/keys/${provider}`, body)
      .pipe(
        tap(() => {
          this.loadSystemApiKeys();
          this.readiness.load();
          this.modelService.load(undefined, true);
        }),
      );
  }

  deleteSystemApiKey(provider: string): Observable<{status: string}> {
    return this.http
      .delete<{status: string}>(`${this.baseUrl}/admin/providers/keys/${provider}`)
      .pipe(
        tap(() => {
          this.loadSystemApiKeys();
          this.readiness.load();
          this.modelService.load(undefined, true);
        }),
      );
  }

  /**
   * Read the staged discovery payload for a provider key. Returns
   * ``ready=false`` while the orchestrator's async probe is still
   * in-flight (post-save) — the cockpit polls this until the dialog can
   * render. The route never throws; transient backend failures resolve
   * to ``ready=false`` so the UI degrades gracefully.
   */
  getDiscoveryPayload(provider: string): Observable<DiscoveryResponse> {
    return this.http
      .get<DiscoveryResponse>(
        `${this.baseUrl}/admin/providers/keys/${provider}/discovery`,
      )
      .pipe(
        catchError(() =>
          of<DiscoveryResponse>({ready: false, fresh: false, payload: null, cached_at: null}),
        ),
      );
  }

  /**
   * Force-refresh the discovery cache for a provider key (synchronous —
   * the route blocks until the probe returns). Used by the explicit
   * "Rediscover" button; the response carries the freshly-cached payload
   * so the dialog can re-render without an extra GET.
   */
  rediscoverProvider(provider: string): Observable<DiscoveryResponse> {
    return this.http.post<DiscoveryResponse>(
      `${this.baseUrl}/admin/providers/keys/${provider}/rediscover`,
      {},
    );
  }

  // ── System Endpoints ──────────────────────────────────────────────

  loadSystemEndpoints(): void {
    this.http
      .get<LlmEndpoint[]>(`${this.baseUrl}/admin/providers/endpoints`)
      .pipe(catchError(() => of([] as LlmEndpoint[])))
      .subscribe((rows) => this.systemEndpoints.set(rows));
  }

  createSystemEndpoint(body: LlmEndpointCreateRequest): Observable<LlmEndpoint> {
    return this.http
      .post<LlmEndpoint>(`${this.baseUrl}/admin/providers/endpoints`, body)
      .pipe(
        tap(() => {
          this.loadSystemEndpoints();
          this.readiness.load();
          this.modelService.load(undefined, true);
        }),
      );
  }

  updateSystemEndpoint(
    endpointId: string,
    body: LlmEndpointUpdateRequest,
  ): Observable<LlmEndpoint> {
    return this.http
      .patch<LlmEndpoint>(`${this.baseUrl}/admin/providers/endpoints/${endpointId}`, body)
      .pipe(
        tap(() => {
          this.loadSystemEndpoints();
          this.readiness.load();
          this.modelService.load(undefined, true);
        }),
      );
  }

  deleteSystemEndpoint(endpointId: string): Observable<{status: string}> {
    return this.http
      .delete<{status: string}>(`${this.baseUrl}/admin/providers/endpoints/${endpointId}`)
      .pipe(
        tap(() => {
          this.loadSystemEndpoints();
          this.readiness.load();
          this.modelService.load(undefined, true);
        }),
      );
  }

  testSystemEndpoint(endpointId: string): Observable<LlmEndpointTestResult> {
    return this.http.post<LlmEndpointTestResult>(
      `${this.baseUrl}/admin/providers/endpoints/${endpointId}/test`,
      {},
    );
  }

  /**
   * Read-only probe of `GET {base_url}/models`. Admin → Models uses this as
   * a quick-fill helper after the admin picks an endpoint provider.
   *
   * The subscription proxy answers the enriched shape (`subscription: true`)
   * from the same route; discriminate on that field rather than on the
   * endpoint's label.
   */
  discoverSystemEndpointModels(
    endpointId: string,
  ): Observable<LlmEndpointDiscoveryResult | SubscriptionDiscoveryResult> {
    return this.http.post<LlmEndpointDiscoveryResult | SubscriptionDiscoveryResult>(
      `${this.baseUrl}/admin/providers/endpoints/${endpointId}/discover`,
      {},
    );
  }

  // ── System Defaults ───────────────────────────────────────────────

  loadDefaults(): void {
    this.http
      .get<Record<DefaultModelKind, string | null>>(`${this.baseUrl}/admin/providers/defaults`)
      .pipe(catchError(() => of({...EMPTY_DEFAULTS})))
      .subscribe((rec) => this.defaults.set({...EMPTY_DEFAULTS, ...rec}));
  }

  setDefault(kind: DefaultModelKind, model: string): Observable<{kind: string; model: string | null}> {
    return this.http
      .put<{kind: string; model: string | null}>(
        `${this.baseUrl}/admin/providers/defaults/${kind}`,
        {model},
      )
      .pipe(
        tap(() => {
          this.loadDefaults();
          this.loadHelmManaged();
          this.readiness.load();
        }),
      );
  }

  /** Refresh which rows Helm reconciles (badges + drift on the Defaults page). */
  loadHelmManaged(): void {
    this.http
      .get<HelmManagedOverview>(`${this.baseUrl}/admin/helm-managed`)
      .pipe(catchError(() => of(null)))
      .subscribe((overview) => this.helmManaged.set(overview));
  }

  // ── Subscription Proxy ────────────────────────────────────────

  /**
   * Refresh the subscription-proxy availability signal. Admin → Models reads
   * this to decide whether to surface a "connect one in Settings → AI
   * Subscriptions" hint when the admin picks the proxy with nothing signed in.
   */
  loadSubscriptionAvailability(): void {
    this.http
      .get<SubscriptionAvailability>(`${this.baseUrl}/admin/providers/subscriptions/availability`)
      .pipe(catchError(() => of({...EMPTY_SUBSCRIPTION_AVAILABILITY})))
      .subscribe((info) => this.subscriptionAvailability.set(info));
  }

  /**
   * Bulk-register discovered models. Omit `modelIds` for "add all supported
   * models". Idempotent — already-registered rows are skipped, never rewritten,
   * so manual catalog edits survive a rediscovery.
   */
  importSubscriptionModels(
    endpointId: string,
    modelIds?: string[],
    includeNeedsReview = false,
  ): Observable<SubscriptionImportResult> {
    return this.http
      .post<SubscriptionImportResult>(
        `${this.baseUrl}/admin/providers/endpoints/${endpointId}/models/import`,
        {model_ids: modelIds ?? null, include_needs_review: includeNeedsReview},
      )
      .pipe(
        tap(() => {
          this.readiness.load();
          this.modelService.load(undefined, true);
        }),
      );
  }
}
