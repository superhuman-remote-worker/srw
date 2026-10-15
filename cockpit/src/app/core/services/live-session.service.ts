import { HttpClient } from '@angular/common/http';
import { Injectable, NgZone, Signal, inject, signal } from '@angular/core';
import { Observable, Subject, firstValueFrom, takeUntil, timeout } from 'rxjs';
import { environment } from '../environment';
import { LiveSession, LiveSessionEvent, LiveSessionEventsPage, isActiveLiveSession } from '../models/live-session.model';
import { classifySseProbeFailure, sseRetryDelayMs } from './sse-recovery';

export interface LiveSessionWatch {
  readonly session: Signal<LiveSession>;
  readonly events: Signal<LiveSessionEvent[]>;
  readonly connected: Signal<boolean>;
  readonly loading: Signal<boolean>;
  readonly error: Signal<boolean>;
  /** Also used by the job-detail panel's existing 30 second refresh. */
  updateSession(session: LiveSession): void;
  refresh(): Promise<boolean>;
  destroy(): void;
}

@Injectable({ providedIn: 'root' })
export class LiveSessionService {
  private readonly http = inject(HttpClient);
  private readonly zone = inject(NgZone);
  private readonly baseUrl = environment.apiUrl;

  getSessions(jobId: string): Observable<{ sessions: LiveSession[] }> {
    return this.http.get<{ sessions: LiveSession[] }>(
      `${this.baseUrl}/jobs/${encodeURIComponent(jobId)}/live-sessions`,
      {
        withCredentials: true,
        headers: { 'ngsw-bypass': '1' },
      },
    );
  }

  getEvents(jobId: string, sessionId: string, afterId = 0, limit = 500): Observable<LiveSessionEventsPage> {
    return this.http.get<LiveSessionEventsPage>(`${this.sessionUrl(jobId, sessionId)}/events`, {
      withCredentials: true,
      headers: { 'ngsw-bypass': '1' },
      params: { after_id: String(afterId), limit: String(Math.min(500, Math.max(1, limit))) },
    });
  }

  /** One stream owner per mounted session. No global cache survives panel closure. */
  watchSession(jobId: string, initial: LiveSession): LiveSessionWatch {
    const session = signal(initial);
    const events = signal<LiveSessionEvent[]>([]);
    const connected = signal(false);
    const loading = signal(true);
    const error = signal(false);
    let source: EventSource | null = null;
    let stopped = false;
    let halted = false;
    let afterId = 0;
    let retryAttempt = 0;
    let retryTimer: ReturnType<typeof setTimeout> | null = null;
    let pollTimer: ReturnType<typeof setInterval> | null = null;
    let refreshInFlight: Promise<boolean> | null = null;
    let refreshAgain = false;
    const disposed = new Subject<void>();

    const merge = (rows: LiveSessionEvent[]) => {
      const byId = new Map(events().map((row) => [row.id, row]));
      for (const row of rows) {
        if (Number.isSafeInteger(row.id) && row.id > 0) {
          byId.set(row.id, row);
        }
      }
      events.set([...byId.values()].sort((a, b) => a.id - b.id));
    };
    const stopTransport = () => {
      source?.close();
      source = null;
      connected.set(false);
      if (retryTimer !== null) clearTimeout(retryTimer);
      if (pollTimer !== null) clearInterval(pollTimer);
      retryTimer = pollTimer = null;
    };

    const refresh = (): Promise<boolean> => {
      if (stopped || halted) return Promise.resolve(false);
      if (refreshInFlight) {
        refreshAgain = true;
        return refreshInFlight;
      }
      // Keep REST's cursor separate from live frames while replaying. Advancing
      // to a newer SSE id here would skip older pages still waiting in the DB.
      refreshInFlight = (async () => {
        let cursor = afterId;
        try {
          while (!stopped) {
            const page = await firstValueFrom(
              this.getEvents(jobId, initial.id, cursor).pipe(
                timeout({ first: 15_000 }),
                takeUntil(disposed),
              ),
            );
            if (stopped) return false;
            merge(page.events);
            const next = page.next_after_id;
            if (
              !Number.isSafeInteger(next) ||
              next < cursor ||
              (page.events.length >= 500 && next === cursor)
            ) {
              throw new Error('LiveSession events cursor did not advance');
            }
            afterId = cursor = next;
            if (page.events.length < 500) break;
          }
          if (connected() && !refreshAgain) {
            afterId = Math.max(afterId, events().at(-1)?.id ?? 0);
          }
          error.set(false);
          return true;
        } catch (failure) {
          if (!stopped) {
            error.set(true);
            const category = classifySseProbeFailure(failure);
            if (category === 'auth' || category === 'gone') {
              halted = true;
              stopTransport();
            }
          }
          return false;
        } finally {
          if (!stopped) loading.set(false);
        }
      })().finally(() => {
        refreshInFlight = null;
        if (refreshAgain && !stopped && !halted) {
          refreshAgain = false;
          void refresh();
        }
      });
      return refreshInFlight;
    };

    const startPolling = () => {
      if (stopped || halted || !isActiveLiveSession(session()) || pollTimer !== null) return;
      pollTimer = setInterval(() => {
        void refresh();
      }, 2_000);
    };
    const scheduleRetry = () => {
      if (stopped || halted || !isActiveLiveSession(session()) || retryTimer !== null) return;
      retryTimer = setTimeout(async () => {
        retryTimer = null;
        // EventSource hides HTTP status. Probe through the authenticated REST
        // path before reopening, just like the notification SSE owner.
        const recovered = await refresh();
        if (stopped || halted || !isActiveLiveSession(session())) return;
        if (recovered) openStream();
        else scheduleRetry();
      }, sseRetryDelayMs(retryAttempt++));
    };
    const updateSession = (next: LiveSession) => {
      if (stopped || next.id !== initial.id || next.job_id !== jobId) return;
      // A list poll begun before hang-up must not resurrect a closed stream.
      if (!isActiveLiveSession(session()) && isActiveLiveSession(next)) return;
      session.set(next);
      if (!isActiveLiveSession(next)) stopTransport();
      void refresh();
    };
    const openStream = () => {
      if (stopped || halted || !isActiveLiveSession(session()) || source) return;
      if (typeof EventSource === 'undefined') {
        startPolling();
        return;
      }
      let es: EventSource;
      try {
        // Cookie BFF authentication; no bearer tokens in URLs. Bypass ngsw so
        // the service worker cannot buffer the streaming response.
        es = new EventSource(`${this.sessionUrl(jobId, initial.id)}/stream?after_id=${afterId}&ngsw-bypass=true`, {
          withCredentials: true,
        });
      } catch {
        startPolling();
        scheduleRetry();
        return;
      }
      source = es;
      es.addEventListener('session_event', (frame: MessageEvent) =>
        this.zone.run(() => {
          if (stopped || source !== es) return;
          try {
            const row = JSON.parse(frame.data) as LiveSessionEvent;
            if (row && typeof row.kind === 'string') {
              merge([row]);
              if (connected() && !refreshInFlight && Number.isSafeInteger(row.id)) {
                afterId = Math.max(afterId, row.id);
              }
            }
          } catch {
            /* Ignore malformed frames and keep the journal cursor. */
          }
        }),
      );
      es.addEventListener('session', (frame: MessageEvent) =>
        this.zone.run(() => {
          if (stopped || source !== es) return;
          try {
            const next = JSON.parse(frame.data) as LiveSession;
            if (
              next &&
              ['ringing', 'active', 'ended', 'rejected', 'failed'].includes(next.status)
            ) {
              updateSession(next);
            }
          } catch {
            /* A malformed row cannot close a valid session. */
          }
        }),
      );
      es.onopen = () =>
        this.zone.run(() => {
          if (stopped || source !== es) return;
          retryAttempt = 0;
          connected.set(true);
          if (pollTimer !== null) clearInterval(pollTimer);
          pollTimer = null;
          // Replay after every open. Keep the saved cursor while that replay
          // is in flight so newer live frames cannot skip historical pages.
          void refresh();
        });
      es.onerror = () =>
        this.zone.run(() => {
          if (stopped || source !== es) return;
          es.close();
          source = null;
          connected.set(false);
          startPolling();
          scheduleRetry();
        });
    };
    const wake = () => {
      if (typeof document !== 'undefined' && document.visibilityState === 'hidden') return;
      if (!source && isActiveLiveSession(session())) scheduleRetry();
    };
    if (typeof window !== 'undefined') window.addEventListener('online', wake);
    if (typeof document !== 'undefined') document.addEventListener('visibilitychange', wake);
    openStream();
    void refresh();

    return {
      session: session.asReadonly(),
      events: events.asReadonly(),
      connected: connected.asReadonly(),
      loading: loading.asReadonly(),
      error: error.asReadonly(),
      updateSession,
      refresh,
      destroy: () => {
        stopped = true;
        disposed.next();
        disposed.complete();
        stopTransport();
        if (typeof window !== 'undefined') window.removeEventListener('online', wake);
        if (typeof document !== 'undefined') document.removeEventListener('visibilitychange', wake);
      },
    };
  }

  private sessionUrl(jobId: string, sessionId: string): string {
    return `${this.baseUrl}/jobs/${encodeURIComponent(jobId)}/live-sessions/${encodeURIComponent(sessionId)}`;
  }
}
