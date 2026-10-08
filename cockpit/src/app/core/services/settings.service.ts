import { inject, Injectable, signal } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { catchError, Observable, of, tap } from 'rxjs';
import {
  ApiKeyEntry,
  ApiKeySetRequest,
  ResolvedDefaults,
  SubscriptionAccount,
  SubscriptionLogin,
  SubscriptionsStatus,
  SubscriptionUsage,
  UserSettings,
} from '../models/api.model';
import { environment } from '../environment';
import type { MainCloudPage } from '../models/main-cloud.model';
import { AdminProvidersService } from './admin-providers.service';
import { ReadinessService } from './readiness.service';

@Injectable({ providedIn: 'root' })
export class SettingsService {
  private readonly http = inject(HttpClient);
  private readonly readiness = inject(ReadinessService);
  private readonly adminProviders = inject(AdminProvidersService);
  private readonly baseUrl = environment.apiUrl;

  /** Current user's API keys (prefix only, no full keys). */
  readonly apiKeys = signal<ApiKeyEntry[]>([]);

  /** Current user's preference settings (user overrides only). */
  readonly preferences = signal<UserSettings>({});

  /** Resolved framework/env defaults for every preference field. */
  readonly resolvedDefaults = signal<ResolvedDefaults>({});

  // ── User API Keys ──────────────────────────────────────────────────

  loadApiKeys(): void {
    this.http
      .get<ApiKeyEntry[]>(`${this.baseUrl}/settings/api-keys`)
      .pipe(catchError(() => of([])))
      .subscribe((keys) => this.apiKeys.set(keys));
  }

  setApiKey(provider: string, body: ApiKeySetRequest): Observable<ApiKeyEntry> {
    return this.http
      .put<ApiKeyEntry>(`${this.baseUrl}/settings/api-keys/${provider}`, body)
      .pipe(tap(() => this.loadApiKeys()));
  }

  deleteApiKey(provider: string): Observable<{ status: string }> {
    return this.http
      .delete<{ status: string }>(`${this.baseUrl}/settings/api-keys/${provider}`)
      .pipe(tap(() => this.loadApiKeys()));
  }

  // ── User Preferences ──────────────────────────────────────────────

  loadPreferences(): void {
    this.http
      .get<UserSettings>(`${this.baseUrl}/settings/preferences`)
      .pipe(catchError(() => of({} as UserSettings)))
      .subscribe((prefs) => {
        const resolved = prefs._resolved ?? {};
        delete prefs._resolved;
        this.resolvedDefaults.set(resolved);
        this.preferences.set(prefs);
      });
  }

  updatePreferences(settings: Partial<UserSettings>): Observable<{ status: string }> {
    return this.http
      .patch<{ status: string }>(`${this.baseUrl}/settings/preferences`, settings)
      .pipe(tap(() => this.loadPreferences()));
  }

  // Per-project LLM provider keys (`/api/projects/{id}/api-keys`) have no
  // cockpit surface; the endpoints and their place in the system > project >
  // user precedence chain are exercised at dispatch, not from the browser.

  // ── AI Subscriptions (Admin) ──────────────────────────────────
  //
  // One surface for every subscription product the proxy can sign in to.
  // The orchestrator owns the provider registry, the login sessions and the
  // management credential; the browser only ever sees an SRW login id.

  /** Proxy reachability, supported providers and connected accounts. */
  getSubscriptionsStatus(): Observable<SubscriptionsStatus> {
    return this.http.get<SubscriptionsStatus>(`${this.baseUrl}/subscriptions/status`).pipe(
      catchError(() =>
        of<SubscriptionsStatus>({
          reachable: false,
          connected: false,
          proxy_url: null,
          error: null,
          accounts: [],
          model_count: 0,
          providers: [],
        }),
      ),
    );
  }

  getSubscriptionAccounts(): Observable<{ accounts: SubscriptionAccount[] }> {
    return this.http
      .get<{ accounts: SubscriptionAccount[] }>(`${this.baseUrl}/subscriptions/accounts`)
      .pipe(catchError(() => of({ accounts: [] })));
  }

  /**
   * Usage for one account. Degrades to `available: false` rather than throwing,
   * and never renders a zero for a provider without a reader.
   */
  getSubscriptionUsage(accountId: string): Observable<SubscriptionUsage> {
    return this.http
      .get<SubscriptionUsage>(
        `${this.baseUrl}/subscriptions/accounts/${encodeURIComponent(accountId)}/usage`,
      )
      .pipe(catchError(() => of<SubscriptionUsage>({ available: false, reason: 'unavailable' })));
  }

  disconnectSubscriptionAccount(accountId: string): Observable<{ status: string }> {
    return this.http
      .delete<{ status: string }>(
        `${this.baseUrl}/subscriptions/accounts/${encodeURIComponent(accountId)}`,
      )
      .pipe(
        tap(() => {
          this.readiness.load();
          this.adminProviders.loadSubscriptionAvailability();
        }),
      );
  }

  startSubscriptionLogin(provider: string): Observable<SubscriptionLogin> {
    return this.http.post<SubscriptionLogin>(`${this.baseUrl}/subscriptions/logins`, { provider });
  }

  pollSubscriptionLogin(loginId: string): Observable<SubscriptionLogin> {
    return this.http.get<SubscriptionLogin>(
      `${this.baseUrl}/subscriptions/logins/${encodeURIComponent(loginId)}`,
    );
  }

  /** Relay a pasted browser callback. Parsed server-side; never used as a URL. */
  submitSubscriptionCallback(loginId: string, url: string): Observable<SubscriptionLogin> {
    return this.http
      .post<SubscriptionLogin>(
        `${this.baseUrl}/subscriptions/logins/${encodeURIComponent(loginId)}/callback`,
        { url },
      )
      .pipe(
        tap(() => {
          this.readiness.load();
          this.adminProviders.loadSubscriptionAvailability();
        }),
      );
  }

  cancelSubscriptionLogin(loginId: string): Observable<SubscriptionLogin> {
    return this.http.delete<SubscriptionLogin>(
      `${this.baseUrl}/subscriptions/logins/${encodeURIComponent(loginId)}`,
    );
  }

  // ── Main cloud (admin) ──────────────────────────────────────

  /** The read-only Main cloud page: Helm configures the main cloud. */
  getMainCloud(): Observable<MainCloudPage> {
    return this.http.get<MainCloudPage>(`${this.baseUrl}/admin/main-cloud`);
  }
}
