import {inject, Injectable, signal} from '@angular/core';
import {HttpClient} from '@angular/common/http';
import {catchError, Observable, of, tap} from 'rxjs';
import {
  CatalogModel,
  CatalogModelCreateRequest,
  CatalogModelDeleteResult,
  CatalogModelTestResult,
  CatalogModelUpdateRequest,
} from '../models/api.model';
import {environment} from '../environment';
import {ModelService} from './model.service';
import {ReadinessService} from './readiness.service';

/**
 * The server's reason for a refused catalog write: a plain-string `detail`
 * (a 409 naming the provider that already holds the model ID, a 422 naming the
 * served-model-id rule), FastAPI's validation list, or `fallback`.
 */
export function catalogErrorDetail(err: unknown, fallback: string): string {
  const detail = (err as {error?: {detail?: unknown}} | null)?.error?.detail;
  if (typeof detail === 'string' && detail) return detail;
  if (Array.isArray(detail)) {
    const messages = detail
      .map((item) => (typeof item === 'string' ? item : (item as {msg?: unknown})?.msg))
      .filter((message): message is string => typeof message === 'string' && !!message);
    if (messages.length) return messages.join('; ');
  }
  return fallback;
}

/**
 * REST client for the `/api/admin/providers/models` surface and the
 * sibling `/api/admin/families` family-list endpoint. Every call is gated
 * by the `srw-admin` role server-side; the client does not re-check.
 *
 * Mirrors the shape of `AdminProvidersService` — signal-cached lists,
 * `tap(() => loadModels())` after mutations so subscribers see the new
 * state without a manual refresh.
 */
@Injectable({providedIn: 'root'})
export class AdminModelsService {
  private readonly http = inject(HttpClient);
  private readonly readiness = inject(ReadinessService);
  private readonly modelService = inject(ModelService);
  private readonly baseUrl = environment.apiUrl;

  readonly models = signal<CatalogModel[]>([]);
  readonly families = signal<string[]>([]);
  /** family key → default context window (from the config matrix). Powers the
   * context-window field's "family default" placeholder. */
  readonly familyDefaults = signal<Record<string, number>>({});
  readonly loading = signal(false);

  loadModels(): void {
    this.loading.set(true);
    this.http
      .get<CatalogModel[]>(`${this.baseUrl}/admin/providers/models`)
      .pipe(catchError(() => of([] as CatalogModel[])))
      .subscribe((rows) => {
        this.models.set(rows);
        this.loading.set(false);
      });
  }

  loadFamilies(): void {
    this.http
      .get<{families: string[]; defaults?: Record<string, number>}>(
        `${this.baseUrl}/admin/families`,
      )
      .pipe(catchError(() => of({families: [] as string[], defaults: {}})))
      .subscribe((res) => {
        this.families.set(res.families);
        this.familyDefaults.set(res.defaults ?? {});
      });
  }

  /** Ask the orchestrator's regex matcher which family the given model_id
   * looks like. Returns ``family: 'default'`` with ``source: 'fallback'``
   * when no rule matches; the caller can still pre-fill the dropdown and
   * let the admin override. Errors quietly resolve to the same fallback so
   * a transient backend failure never blocks the form. */
  detectFamily(modelId: string): Observable<{family: string; source: string}> {
    return this.http
      .get<{family: string; source: string}>(
        `${this.baseUrl}/admin/families/detect`,
        {params: {model_id: modelId}},
      )
      .pipe(
        catchError(() => of({family: 'default', source: 'fallback'})),
      );
  }

  createModel(body: CatalogModelCreateRequest): Observable<CatalogModel> {
    return this.http
      .post<CatalogModel>(`${this.baseUrl}/admin/providers/models`, body)
      .pipe(
        tap(() => {
          this.loadModels();
          this.readiness.load();
          this.modelService.load(undefined, true);
        }),
      );
  }

  updateModel(id: string, body: CatalogModelUpdateRequest): Observable<CatalogModel> {
    return this.http
      .patch<CatalogModel>(`${this.baseUrl}/admin/providers/models/${id}`, body)
      .pipe(
        tap(() => {
          this.loadModels();
          this.readiness.load();
          this.modelService.load(undefined, true);
        }),
      );
  }

  deleteModel(id: string): Observable<CatalogModelDeleteResult> {
    return this.http
      .delete<CatalogModelDeleteResult>(`${this.baseUrl}/admin/providers/models/${id}`)
      .pipe(
        tap(() => {
          this.loadModels();
          this.readiness.load();
          this.modelService.load(undefined, true);
        }),
      );
  }

  testModel(id: string): Observable<CatalogModelTestResult> {
    return this.http.post<CatalogModelTestResult>(
      `${this.baseUrl}/admin/providers/models/${id}/test`,
      {},
    );
  }
}
