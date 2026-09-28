import { Injectable, inject, signal, NgZone } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { TranslocoService } from '@jsverse/transloco';
import { catchError, firstValueFrom, Observable, of, timeout } from 'rxjs';
import {
  EMPTY_NOTIFICATION_COUNTS,
  Notification,
  NotificationActResponse,
  NotificationCounts,
  NotificationDetail,
  NotificationFeedPage,
  NotificationUpdate,
} from '../models/notification.model';
import { environment } from '../environment';
import { AppToastService } from '../../ui/toast';
import { classifySseProbeFailure, sseRetryDelayMs } from './sse-recovery';

/** A session event from a persistent agent (permission request, VM upgrade, etc.). */
export interface SessionEvent {
    event_id: string;
    type: string;
    thread_id: string;
    title: string;
    config_name?: string;
    tool?: string;
    args?: Record<string, unknown>;
    reason?: string;
    command?: string;
    created_at: string;
}

/**
 * One session-lifecycle phase transition emitted by the orchestrator while
 * a session is starting up. Single source of truth for the cockpit's
 * "Starting session" card — see notification.service.connectSSE().
 */
export interface LifecycleEvent {
    thread_id: string;
    /** Exact session life that emitted this phase. */
    session_runtime_generation?: string;
    state: 'provisioning' | 'booting' | 'ready' | 'failed' | (string & {});
    reason?: string;
    /**
     * Workspace backend the session is starting on, tagged by the orchestrator's
     * binding paths. Present as `'vm'` for VM-backed sessions so the startup card
     * can show the longer "Booting VM" copy and the client can extend its
     * readiness budget for a cold KubeVirt boot. Absent for sandbox/lite.
     */
    backend?: string;
}

/** A protected cloud stage that passed the server's exact publication CAS. */
export interface CloudDiffStagedEvent {
    thread_id: string;
    session_runtime_generation: string;
    staged_epoch: number;
    file_count: number;
    counts: { added: number; modified: number; deleted: number };
    mount_id: string;
}

/**
 * A `user_registered` SSE frame — the orchestrator fans this out to admins
 * only (app-side admission, notify_admins_user_registered) when a new user
 * registers and lands pending.
 */
export interface AdminUserRegisteredEvent {
    user_id: string;
    display_name?: string | null;
    email?: string | null;
}

/**
 * The durable notification feed (unified notification system, slice 3: the
 * feed is the only store — every producer records here, so there is no
 * legacy `message_log` view to merge any more).
 */
@Injectable({ providedIn: 'root' })
export class NotificationService {
  private readonly http = inject(HttpClient);
  private readonly zone = inject(NgZone);
  private readonly toast = inject(AppToastService);
  private readonly transloco = inject(TranslocoService);
  private readonly baseUrl = environment.apiUrl;

  /** SSE connection state. */
  readonly isConnected = signal(false);

  /** Whether the feed is loading. */
  readonly isLoading = signal(false);

    /**
     * Persistent session events (`session.*` frames from the NATS bridge:
     * permission requests, VM/workspace upgrades, waiting). Still on the
     * stream; the open session view handles each inline, and a headless
     * permission gate lands on the feed as a `session_permission` row.
     */
    readonly sessionEvents = signal<SessionEvent[]>([]);

    /**
     * Latest session.lifecycle phase transition (or null if none seen this
     * session). Consumers filter by `thread_id` to react only to events
     * matching the thread they care about. Cleared on disconnectSSE.
     */
    readonly lifecycleEvent = signal<LifecycleEvent | null>(null);

    /** Latest exact-runtime protected stage publication. */
    readonly cloudDiffStagedEvent = signal<CloudDiffStagedEvent | null>(null);

    /**
     * Latest `user_registered` frame (admins only receive these). The admin
     * Users page reacts via effect() to refresh its pending list live.
     */
    readonly adminUserRegistered = signal<AdminUserRegisteredEvent | null>(null);

  // ── The durable feed ────────────────────────────────────────────────
  /** Feed rows, newest first. Live frames upsert into it; `loadMoreFeed`
   *  appends older pages behind `feedNextBefore`. */
  readonly feed = signal<Notification[]>([]);
  /** Server counts — `unseen` drives the bell badge. Live frames adjust
   *  them optimistically; the next load corrects any drift. */
  readonly feedCounts = signal<NotificationCounts>(EMPTY_NOTIFICATION_COUNTS);
  readonly feedNextBefore = signal<string | null>(null);

  private eventSource: EventSource | null = null;
  private sseWanted = false;
  private sseGeneration = 0;
  private sseRetryAttempt = 0;
  private sseHalted = false;
  private sseRetryTimer: ReturnType<typeof setTimeout> | null = null;
  private sseProbeInFlight: { generation: number } | null = null;
  private readonly wakeSse = () => {
    if (!this.sseWanted || this.sseHalted || this.eventSource || this.sseProbeInFlight) return;
    if (this.sseRetryTimer) clearTimeout(this.sseRetryTimer);
    this.sseRetryTimer = null;
    void this.retryClosedSse(this.sseGeneration);
  };

  /** Load the first feed page from REST. */
  loadNotifications(limit = 100): void {
    this.isLoading.set(true);
    this.http
      .get<NotificationFeedPage>(`${this.baseUrl}/notifications`, {
        params: { limit: String(limit) },
      })
      .pipe(catchError(() => of(null)))
      .subscribe((page) => {
        if (page) {
          this.feed.set(page.items ?? []);
          this.feedCounts.set(page.counts ?? EMPTY_NOTIFICATION_COUNTS);
          this.feedNextBefore.set(page.next_before ?? null);
        }
        this.isLoading.set(false);
      });
  }

  /** Append the next (older) feed page behind the keyset cursor. */
  loadMoreFeed(limit = 50): void {
    const before = this.feedNextBefore();
    if (!before) return;
    this.http
      .get<NotificationFeedPage>(`${this.baseUrl}/notifications`, {
        params: { before, limit: String(limit) },
      })
      .pipe(catchError(() => of(null)))
      .subscribe((page) => {
        if (!page) return;
        this.feed.update((rows) => {
          const known = new Set(rows.map((r) => r.id));
          return [...rows, ...(page.items ?? []).filter((r) => !known.has(r.id))];
        });
        this.feedNextBefore.set(page.next_before ?? null);
      });
  }

  /** The rows about one source (`source_kind` + `source_id` go together on
   *  the server): the officer card's "recent notifications from this
   *  officer", and the legacy `?sudo=` / `?job=&thread=` deep links. Does
   *  not touch the feed signals — callers upsert what they want. */
  listBySource(kind: string, id: string, limit = 10): Observable<NotificationFeedPage | null> {
    return this.http
      .get<NotificationFeedPage>(`${this.baseUrl}/notifications`, {
        params: { source_kind: kind, source_id: id, limit: String(limit) },
      })
      .pipe(catchError(() => of(null)));
  }

  getNotification(id: string): Observable<NotificationDetail | null> {
    return this.http
      .get<NotificationDetail>(`${this.baseUrl}/notifications/${id}`)
      .pipe(catchError(() => of(null)));
  }

  markSeen(ids: string[]): Observable<{ updated: string[] }> {
    return this.http.post<{ updated: string[] }>(`${this.baseUrl}/notifications/seen`, { ids });
  }

  markReadV2(id: string): Observable<{ notification: Notification }> {
    return this.http.patch<{ notification: Notification }>(
      `${this.baseUrl}/notifications/${id}/read`,
      {},
    );
  }

  act(
    id: string,
    actionType: string,
    params: Record<string, unknown>,
  ): Observable<NotificationActResponse> {
    return this.http.post<NotificationActResponse>(`${this.baseUrl}/notifications/${id}/act`, {
      action_type: actionType,
      params,
    });
  }

  /** Insert-or-replace one feed row (live `notification` frame, action
   *  response, deep-link fetch). A brand-new row bumps the counts. */
  upsertFeedRow(row: Notification): void {
    const existing = this.feed().find((r) => r.id === row.id);
    if (existing) {
      this.feed.update((rows) => rows.map((r) => (r.id === row.id ? { ...r, ...row } : r)));
      this.adjustCounts(existing, { ...existing, ...row });
      return;
    }
    this.feed.update((rows) => [row, ...rows]);
    this.feedCounts.update((c) => {
      const cat = c.by_category[row.category] ?? { pending: 0, unseen: 0 };
      return {
        ...c,
        unseen: c.unseen + (row.seen_at ? 0 : 1),
        unread: c.unread + (row.read_at ? 0 : 1),
        pending: c.pending + (row.resolved_at ? 0 : 1),
        by_category: {
          ...c.by_category,
          [row.category]: {
            pending: cat.pending + (row.resolved_at ? 0 : 1),
            unseen: cat.unseen + (row.seen_at ? 0 : 1),
          },
        },
      };
    });
  }

  /** Apply a `notification.updated` patch (seen/read/resolved/archived). */
  patchFeedRow(update: NotificationUpdate): void {
    const prev = this.feed().find((r) => r.id === update.id);
    if (!prev) return;
    const next: Notification = { ...prev, ...update };
    this.feed.update((rows) => rows.map((r) => (r.id === update.id ? next : r)));
    this.adjustCounts(prev, next);
  }

  private adjustCounts(prev: Notification, next: Notification): void {
    const delta = (before: string | null, after: string | null) =>
      !before && after ? -1 : before && !after ? 1 : 0;
    const dUnseen = delta(prev.seen_at, next.seen_at);
    const dPending = delta(prev.resolved_at, next.resolved_at);
    this.feedCounts.update((c) => {
      const cat = c.by_category[next.category] ?? { pending: 0, unseen: 0 };
      return {
        ...c,
        unseen: Math.max(0, c.unseen + dUnseen),
        unread: Math.max(0, c.unread + delta(prev.read_at, next.read_at)),
        pending: Math.max(0, c.pending + dPending),
        by_category: {
          ...c.by_category,
          [next.category]: {
            pending: Math.max(0, cat.pending + dPending),
            unseen: Math.max(0, cat.unseen + dUnseen),
          },
        },
      };
    });
  }

  /** Connect to SSE stream for real-time notification updates. */
  connectSSE(): void {
    if (this.sseWanted) return;
    this.sseWanted = true;
    if (typeof window !== 'undefined') window.addEventListener('online', this.wakeSse);
    if (typeof document !== 'undefined') document.addEventListener('visibilitychange', this.wakeSse);
    this.openSse();
  }

  private openSse(): void {
    if (!this.sseWanted || this.eventSource) return;
    const generation = ++this.sseGeneration;

    // ngsw-bypass keeps the service worker from buffering this SSE stream.
    const es = new EventSource(`${this.baseUrl}/notifications/events?ngsw-bypass=true`, {
      withCredentials: true,
    });
    this.eventSource = es;

    es.onmessage = (e: MessageEvent) => {
      if (this.eventSource !== es) return;
      this.zone.run(() => {
        try {
          this.handleSseEvent(JSON.parse(e.data));
        } catch {
          // Ignore parse errors (keepalive, malformed events)
        }
      });
    };

    es.onopen = () => {
      if (this.eventSource !== es || generation !== this.sseGeneration) return;
      this.sseRetryAttempt = 0;
      this.zone.run(() => this.isConnected.set(true));
    };

    es.onerror = () => {
      if (this.eventSource !== es || generation !== this.sseGeneration) return;
      this.zone.run(() => this.isConnected.set(false));
      if (es.readyState !== EventSource.CLOSED) return;
      es.close();
      this.eventSource = null;
      this.scheduleSseRetry(generation);
    };
  }

  private scheduleSseRetry(generation: number): void {
    if (!this.sseWanted || this.sseHalted || generation !== this.sseGeneration || this.sseRetryTimer || this.sseProbeInFlight) return;
    const delay = sseRetryDelayMs(this.sseRetryAttempt++);
    this.sseRetryTimer = setTimeout(() => {
      this.sseRetryTimer = null;
      void this.retryClosedSse(generation);
    }, delay);
  }

  private async retryClosedSse(generation: number): Promise<void> {
    if (!this.sseWanted || this.sseHalted || generation !== this.sseGeneration || this.eventSource || this.sseProbeInFlight) return;
    const probe = { generation };
    this.sseProbeInFlight = probe;
    let failure: 'auth' | 'gone' | 'retry' | null = null;
    try {
      // Unlike the SSE route, the feed read requires an approved user. A 200
      // stream opened as anonymous after cookie loss must not claim recovery.
      await firstValueFrom(this.http.get(`${this.baseUrl}/notifications`, {
        params: { limit: '1' },
      }).pipe(timeout({ first: 15_000 })));
    } catch (error) {
      failure = classifySseProbeFailure(error);
    } finally {
      if (this.sseProbeInFlight === probe) this.sseProbeInFlight = null;
    }
    if (!this.sseWanted || generation !== this.sseGeneration) return;
    if (failure === 'auth' || failure === 'gone') {
      this.sseHalted = true;
      return;
    }
    if (failure) {
      this.scheduleSseRetry(generation);
      return;
    }
    this.openSse();
  }

  /**
   * Dispatch one parsed SSE frame. Extracted from the EventSource callback
   * so specs can drive frames through without a live stream — connectSSE()
   * is the only production caller.
   */
  // The frame is a raw JSON.parse result — same effective typing as the
  // previous inline handler (strict tsconfig forbids dot access on index
  // signatures, so Record<string, unknown> would force bracket noise).
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  handleSseEvent(data: any): void {
    if (data.type === 'notification' && data.notification?.id) {
      // Unified feed: the inserting record() call broadcasts the full row
      // exactly once; a replay never re-broadcasts.
      const row = data.notification as Notification;
      const fresh = !this.feed().some((r) => r.id === row.id);
      this.upsertFeedRow(row);
      if (fresh) this.toastForRow(row);
    } else if (data.type === 'notification.updated' && data.id) {
      // Engagement / resolution patch — only the changed fields travel.
      this.patchFeedRow(data as NotificationUpdate);
    } else if (data.type === 'reply_delivered') {
      // The thread moved on; the next feed load shows the resolved row.
      this.loadNotifications();
    } else if (
        data.type === 'session.permission_request' ||
        data.type === 'session.vm_upgrade' ||
        data.type === 'session.workspace_upgrade' ||
        data.type === 'session.waiting'
    ) {
        this.sessionEvents.update((events) => [
            {...data, created_at: data.created_at || new Date().toISOString()} as SessionEvent,
            ...events,
        ]);
    } else if (data.type === 'session.resolved') {
        this.sessionEvents.update((events) =>
            events.filter((e) => e.thread_id !== data.thread_id),
        );
    } else if (data.type === 'session.lifecycle') {
        // Single source of truth for the cockpit's "Starting
        // session" card phase transitions. Both server-side
        // binding paths (orchestrator/services/provision_or_assign.py
        // for the create-thread fast path, and
        // orchestrator/routers/sessions.py::_do_prepare for the
        // cold path) emit on this channel — the cockpit's
        // PersistentChatService reacts via an effect() that
        // filters on the active thread id.
        this.lifecycleEvent.set({
            thread_id: data.thread_id,
            session_runtime_generation: data.session_runtime_generation,
            state: data.state,
            reason: data.reason,
            backend: data.backend,
        });
    } else if (data.type === 'cloud.diff_staged') {
        // The persistent-chat service generation-fences this wake-up edge and
        // re-reads the staged summary; these counts are never painted directly.
        this.cloudDiffStagedEvent.set({
            thread_id: data.thread_id,
            session_runtime_generation: data.session_runtime_generation,
            staged_epoch: data.staged_epoch,
            file_count: data.file_count,
            counts: data.counts,
            mount_id: data.mount_id,
        });
    } else if (data.type === 'user_registered') {
        // Admin-only fan-out (app-side admission): a new user
        // registered and awaits approval. The toast rides this legacy
        // frame; the `user_registered` feed row is deliberately silent so
        // an admin is not toasted twice.
        this.adminUserRegistered.set({
            user_id: data.user_id,
            display_name: data.display_name,
            email: data.email,
        });
        this.toast.info(
            this.transloco.translate('toasts.admin.userRegistered', {
                name: data.display_name || data.email || data.user_id,
            }),
        );
    }
  }

  /** Live toasts derived from the feed row's category — the old
   *  `automation_auto_disabled` / `loop_*` frames are gone. */
  private toastForRow(row: Notification): void {
    if (row.category === 'automation_disabled') {
      const name = row.payload?.['automation_name'];
      this.toast.warning(
        this.transloco.translate('toasts.automations.autoDisabled', {
          name: typeof name === 'string' ? name : '',
        }),
      );
    } else if (row.category === 'loop_event') {
      this.toast.info(row.subject || 'Loop update');
    }
  }

  /** Disconnect from SSE stream. */
  disconnectSSE(): void {
    this.sseWanted = false;
    this.sseHalted = false;
    this.sseGeneration++;
    this.sseProbeInFlight = null;
    if (this.sseRetryTimer) clearTimeout(this.sseRetryTimer);
    this.sseRetryTimer = null;
    if (typeof window !== 'undefined') window.removeEventListener('online', this.wakeSse);
    if (typeof document !== 'undefined') document.removeEventListener('visibilitychange', this.wakeSse);
    if (this.eventSource) {
      this.eventSource.close();
      this.eventSource = null;
    }
    this.isConnected.set(false);
    this.lifecycleEvent.set(null);
    this.cloudDiffStagedEvent.set(null);
  }
}
