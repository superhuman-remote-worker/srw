import {Injectable, inject, signal} from '@angular/core';
import {HttpClient, HttpParams} from '@angular/common/http';
import {Observable, catchError, of, tap} from 'rxjs';
import {WorkspaceBinding} from '../models/workspace-template.model';
import {environment} from '../environment';

/**
 * An automation row as returned by `GET /api/automations` and friends.
 *
 * Live triggers bind a primary connector; scheduled triggers carry cron fields.
 */
export interface Automation {
  id: string;
  owner_id: string;
  project_id: string | null;

  name: string;
  description: string | null;
  trigger_type: 'cron' | 'live';
  trigger_connector_id?: string | null;
  job_shape?: AutomationJobShape | null;

  cron_expr: string | null;
  timezone: string;
  catchup_window_seconds: number;
  event_filter: Record<string, unknown> | null;

  enabled: boolean;

  expert: string;
  expert_id: string | null;
  prompt: string;
  config_override: Record<string, unknown>;
  autonomy: 'full' | 'review' | 'partial' | 'guided' | 'dependent';
  priority: number;

  max_chain_depth: number;
  max_fires_per_day: number;
  fires_today_count: number;
  fires_today_date: string | null;

  next_run_at: string | null;
  last_scheduled_at: string | null;
  last_dispatched_at: string | null;
  last_fired_at: string | null;
  last_job_id: string | null;
  last_status: string | null;

  run_count: number;
  created_at: string;
  updated_at: string;
}

export interface AutomationJobShape {
  primary_connector?: string;
  execution?: {connectors: Record<string, {ref: {uid: string}}>};
  /** Omitted = default; null = no workspace. */
  workspace?: WorkspaceBinding;
}

/** Request shape for `POST /api/automations`. */
export interface CreateAutomationRequest {
  name: string;
  description?: string | null;
  trigger_type?: 'cron' | 'live';
  primary_connector?: string;
  job_shape?: AutomationJobShape;
  cron_expr?: string;
  timezone?: string;
  catchup_window_seconds?: number;
  expert: string;
  expert_id?: string | null;
  prompt: string;
  config_override?: Record<string, unknown> | null;
  autonomy?: 'full' | 'review' | 'partial' | 'guided' | 'dependent';
  priority?: number;
  enabled?: boolean;
  max_chain_depth?: number;
  max_fires_per_day?: number;
  project_id?: string | null;
}

/** Patchable subset for `PATCH /api/automations/{id}`. Server recomputes
 *  next_run_at when cron_expr / timezone / enabled changes. */
export type UpdateAutomationRequest = Partial<Omit<CreateAutomationRequest, 'trigger_type'>>;

/** Minimal job shape returned by the runs list — full job detail lives in
 *  the existing job-detail view we link to. */
export interface AutomationRun {
  id: string;
  status: string;
  description: string;
  priority: number;
  created_at: string;
  updated_at: string;
  config_name: string;
  project_id: string | null;
  parent_job_id: string | null;
}

@Injectable({providedIn: 'root'})
export class AutomationsService {
  private readonly http = inject(HttpClient);
  private readonly baseUrl = environment.apiUrl;

  /** Current visible automations for the active view (caller-scoped by
   *  default; project-scoped when `loadByProject` is called). */
  readonly automations = signal<Automation[]>([]);
  readonly isLoading = signal(false);

  /** Load the caller's own automations. */
  loadMine(): void {
    this.isLoading.set(true);
    this.http
      .get<Automation[]>(`${this.baseUrl}/automations`)
      .pipe(catchError(() => of([] as Automation[])))
      .subscribe((rows) => {
        this.automations.set(rows);
        this.isLoading.set(false);
      });
  }

  /** Load all automations on a project the caller can see. */
  loadByProject(projectId: string): void {
    this.isLoading.set(true);
    const params = new HttpParams().set('project_id', projectId);
    this.http
      .get<Automation[]>(`${this.baseUrl}/automations`, {params})
      .pipe(catchError(() => of([] as Automation[])))
      .subscribe((rows) => {
        this.automations.set(rows);
        this.isLoading.set(false);
      });
  }

  /** Fetch one automation by id without touching the list signal. */
  get(id: string): Observable<Automation> {
    return this.http.get<Automation>(`${this.baseUrl}/automations/${id}`);
  }

  /** Create a new automation; list signal is not auto-refreshed —
   *  call `loadMine()` / `loadByProject()` from the caller. */
  create(body: CreateAutomationRequest): Observable<Automation> {
    return this.http.post<Automation>(`${this.baseUrl}/automations`, body);
  }

  update(id: string, body: UpdateAutomationRequest): Observable<Automation> {
    return this.http
      .patch<Automation>(`${this.baseUrl}/automations/${id}`, body)
      .pipe(tap((row) => this._mergeIntoSignal(row)));
  }

  delete(id: string): Observable<void> {
    return this.http
      .delete<void>(`${this.baseUrl}/automations/${id}`)
      .pipe(
        tap(() =>
          this.automations.update((rows) => rows.filter((r) => r.id !== id)),
        ),
      );
  }

  /** Fire the automation immediately (in addition to its schedule).
   *  Returns the newly-created job summary. */
  runNow(id: string): Observable<{automation_id: string; job: {id: string}}> {
    return this.http.post<{automation_id: string; job: {id: string}}>(
      `${this.baseUrl}/automations/${id}/run-now`,
      {},
    );
  }

  pause(id: string): Observable<Automation> {
    return this.http
      .post<Automation>(`${this.baseUrl}/automations/${id}/pause`, {})
      .pipe(tap((row) => this._mergeIntoSignal(row)));
  }

  resume(id: string): Observable<Automation> {
    return this.http
      .post<Automation>(`${this.baseUrl}/automations/${id}/resume`, {})
      .pipe(tap((row) => this._mergeIntoSignal(row)));
  }

  listRuns(id: string, limit = 50): Observable<AutomationRun[]> {
    const params = new HttpParams().set('limit', limit);
    return this.http.get<AutomationRun[]>(
      `${this.baseUrl}/automations/${id}/runs`,
      {params},
    );
  }

  private _mergeIntoSignal(row: Automation): void {
    this.automations.update((rows) => {
      const idx = rows.findIndex((r) => r.id === row.id);
      if (idx === -1) return rows;
      const next = rows.slice();
      next[idx] = row;
      return next;
    });
  }
}
