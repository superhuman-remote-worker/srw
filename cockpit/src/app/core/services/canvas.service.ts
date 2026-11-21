import {HttpClient, HttpErrorResponse, HttpResponse} from '@angular/common/http';
import {DestroyRef, inject, Injectable, signal} from '@angular/core';
import {takeUntilDestroyed} from '@angular/core/rxjs-interop';
import {firstValueFrom, map, Observable, Subscription, tap, throwError} from 'rxjs';
import {
  BrowserCapability,
  BrowserCapabilityStatus,
  BrowserOpenStatus,
  CanvasContentSaveRequest,
  CanvasLoadStatus,
  CanvasMutationResponse,
  CanvasRequestError,
  CanvasState,
  CanvasStateMutationResponse,
  MAIN_CANVAS_ID,
} from '../models/canvas.model';
import {environment} from '../environment';
import {
  CanvasInvalidation,
  PersistentThreadTransportBridge,
} from './persistent-thread-transport-bridge.service';

/** Avoid a REST request on every focus bounce while still repairing missed SSE. */
export const CANVAS_RECONCILE_STALE_MS = 30_000;
export const CANVAS_BROADCAST_CHANNEL = 'srw.canvas.presentation.v1';
export const BROWSER_OPEN_TIMEOUT_MS = 5 * 60_000;
/** How often to ask again while a workspace is not yet attested (booting). */
export const BROWSER_CAPABILITY_RECHECK_MS = 10_000;
export const BROWSER_OPEN_RETRY_DEFAULT_MS = 1_000;
export const BROWSER_OPEN_RETRY_MAX_MS = 10_000;

export interface CanvasOfficeTurnAdapter {
  saveBeforeUserMessage(): Promise<boolean>;
}

interface CanvasBroadcastInvalidation {
  readonly type: 'canvas.presentation_invalidated';
  readonly threadId: string;
  readonly canvasId: typeof MAIN_CANVAS_ID;
  readonly presentationRevision: number;
}

interface BrowserOpenMutationResponse extends CanvasStateMutationResponse {
  readonly changed: boolean;
}

const DARK_BROWSER_CAPABILITY: BrowserCapability = Object.freeze({
  feature_enabled: false,
  can_open_browser: false,
  workspace_ready: false,
  reason: 'feature_disabled',
});

/**
 * Authoritative Dynamic Canvas state for the selected persistent thread.
 *
 * Transport events are invalidations only. Every accepted invalidation is
 * reconciled through the REST representation (and its ETag); event payloads
 * never become Canvas state directly.
 */
@Injectable({providedIn: 'root'})
export class CanvasService {
  private readonly http = inject(HttpClient);
  private readonly transport = inject(PersistentThreadTransportBridge);
  private readonly destroyRef = inject(DestroyRef);

  readonly threadId = signal<string | null>(null);
  readonly state = signal<CanvasState | null>(null);
  readonly stateEtag = signal<string | null>(null);
  readonly loadStatus = signal<CanvasLoadStatus>('idle');
  readonly requestError = signal<CanvasRequestError | null>(null);
  readonly browserCapability = signal<BrowserCapability | null>(null);
  readonly browserCapabilityStatus = signal<BrowserCapabilityStatus>('idle');
  readonly browserOpenStatus = signal<BrowserOpenStatus>('idle');
  readonly browserOpenError = signal<string | null>(null);
  /** Browser time of the last authoritative GET or mutation applied locally. */
  readonly lastSuccessfulSyncAt = signal<number | null>(null);

  private requestSubscription: Subscription | null = null;
  private activeRequestToken: symbol | null = null;
  private activeTargetRevision: number | null = null;
  private activeTargetFollowUpAllowed = true;
  private pendingRevision: number | null = null;
  private pendingRevisionFollowUpAllowed = false;
  private pendingForcedReconcile = false;
  private broadcastChannel: BroadcastChannel | null = null;
  private browserCapabilitySubscription: Subscription | null = null;
  private browserCapabilityRequestToken: symbol | null = null;
  private browserOpenSubscription: Subscription | null = null;
  private browserOpenToken: symbol | null = null;
  private browserOpenTimer: ReturnType<typeof setTimeout> | null = null;
  /** Re-asks while the answer is `workspace_unattested`, which is transient. */
  private browserCapabilityRecheckTimer: ReturnType<typeof setTimeout> | null = null;
  private browserOpenStartedAt = 0;
  private browserOpenTitle: string | undefined;
  private browserOpenExpectedPresentationRevision: number | undefined;
  private officeTurnAdapter: CanvasOfficeTurnAdapter | null = null;
  private lastOfficeRuntimeUpdate:
    | {readonly threadId: string; readonly revision: number; readonly sourceVersion: string}
    | null = null;

  constructor() {
    this.transport.canvasInvalidations$
      .pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe((event) => this.handleInvalidation(event, true));

    if (typeof window !== 'undefined' && typeof document !== 'undefined') {
      const reconcileIfVisible = () => {
        if (document.visibilityState !== 'visible') return;
        this.reconcileIfStale();
      };
      window.addEventListener('focus', reconcileIfVisible);
      document.addEventListener('visibilitychange', reconcileIfVisible);
      this.destroyRef.onDestroy(() => {
        window.removeEventListener('focus', reconcileIfVisible);
        document.removeEventListener('visibilitychange', reconcileIfVisible);
      });

      if (typeof window.BroadcastChannel === 'function') {
        try {
          this.broadcastChannel = new window.BroadcastChannel(CANVAS_BROADCAST_CHANNEL);
          this.broadcastChannel.addEventListener('message', this.handleBroadcastMessage);
          this.destroyRef.onDestroy(() => {
            this.broadcastChannel?.removeEventListener('message', this.handleBroadcastMessage);
            this.broadcastChannel?.close();
            this.broadcastChannel = null;
          });
        } catch {
          // Some privacy modes expose the constructor but reject channel use.
          // Focus/visibility reconciliation remains the bounded fallback.
          this.broadcastChannel = null;
        }
      }
    }

    this.destroyRef.onDestroy(() => {
      this.cancelRequest();
      this.cancelBrowserWork();
    });
  }

  /**
   * Select the route's thread. A null draft selection synchronously clears all
   * prior-thread state and cannot fall back to the last viewed Canvas.
   */
  selectThread(threadId: string | null): void {
    if (this.threadId() === threadId) {
      if (threadId) {
        this.reconcileIfStale();
        if (
          this.browserCapabilityStatus() === 'idle' ||
          this.browserCapabilityStatus() === 'error'
        ) {
          this.startBrowserCapabilityRequest(threadId);
        }
      }
      return;
    }

    this.cancelRequest();
    this.cancelBrowserWork();
    this.pendingRevision = null;
    this.pendingRevisionFollowUpAllowed = false;
    this.pendingForcedReconcile = false;
    this.lastOfficeRuntimeUpdate = null;
    this.lastSuccessfulSyncAt.set(null);
    this.threadId.set(threadId);
    this.state.set(null);
    this.stateEtag.set(null);
    this.requestError.set(null);
    this.browserCapability.set(null);
    this.browserCapabilityStatus.set('idle');
    this.browserOpenStatus.set('idle');
    this.browserOpenError.set(null);

    if (!threadId) {
      this.loadStatus.set('idle');
      return;
    }

    this.startReconcile(null);
    this.startBrowserCapabilityRequest(threadId);
  }

  /**
   * Ask again whether this thread can open its shared browser. The answer
   * changes when the workspace does (an upgrade from Virtual, a VM that
   * finished booting), which no Canvas frame announces.
   */
  refreshBrowserCapability(): void {
    const threadId = this.threadId();
    if (!threadId || this.browserOpenToken) return;
    this.startBrowserCapabilityRequest(threadId);
  }

  /** Force an authoritative reload, coalescing with a request already running. */
  reconcile(): void {
    if (!this.threadId()) return;
    if (!this.browserCapability()?.can_open_browser) this.refreshBrowserCapability();
    if (this.activeRequestToken) {
      this.pendingForcedReconcile = true;
      return;
    }
    this.startReconcile(null);
  }

  /** Open or recover the one shared browser for the selected thread. */
  openBrowser(title?: string, expectedPresentationRevision?: number): void {
    if (this.browserOpenToken) return;
    const threadId = this.threadId();
    const capability = this.browserCapability();
    if (!threadId || !capability?.can_open_browser) {
      this.browserOpenStatus.set('error');
      this.browserOpenError.set(capability?.reason ?? 'browser_not_available');
      return;
    }

    const normalizedTitle = title?.trim();
    this.browserOpenTitle = normalizedTitle || undefined;
    this.browserOpenExpectedPresentationRevision =
      expectedPresentationRevision ?? this.state()?.presentation_revision;
    this.browserOpenStartedAt = Date.now();
    this.browserOpenError.set(null);
    const token = Symbol('canvas-browser-open');
    this.browserOpenToken = token;
    this.postBrowserOpen(token, threadId);
  }

  /** Retry the last bounded open attempt without multiplying active work. */
  retryOpenBrowser(): void {
    if (this.browserOpenToken || this.browserOpenStatus() !== 'error') return;
    this.openBrowser(
      this.browserOpenTitle,
      this.browserOpenExpectedPresentationRevision,
    );
  }

  /**
   * Register the one mounted Office frame that can flush edits before a turn.
   *
   * The identity guard prevents a retiring frame from detaching its successor.
   */
  registerOfficeTurnAdapter(adapter: CanvasOfficeTurnAdapter): () => void {
    this.officeTurnAdapter = adapter;
    return () => {
      if (this.officeTurnAdapter === adapter) this.officeTurnAdapter = null;
    };
  }

  async prepareOfficeForUserMessage(): Promise<boolean> {
    const adapter = this.officeTurnAdapter;
    if (!adapter) return true;
    try {
      return await adapter.saveBeforeUserMessage();
    } catch {
      return false;
    }
  }

  /**
   * Reconcile a completed WOPI save and invalidate the agent's recent read.
   *
   * PutFile is called by Collabora rather than this Angular app, so its compact
   * response cannot carry the new Canvas state. This authoritative GET closes
   * that bridge before the pending chat turn is admitted.
   */
  async reconcileOfficeSave(): Promise<number | null> {
    const threadId = this.threadId();
    if (!threadId) return null;
    const url =
      `${environment.apiUrl}/persistent/threads/${encodeURIComponent(threadId)}` +
      `/canvases/${MAIN_CANVAS_ID}`;
    const response = await firstValueFrom(
      this.http.get<CanvasState>(url, {observe: 'response'}),
    );
    const source = response.body?.source;
    if (
      this.threadId() !== threadId ||
      !isCanvasState(response.body) ||
      response.body.renderer !== 'office' ||
      source?.type !== 'workspace_file' ||
      typeof source.path !== 'string' ||
      !response.body.source_version
    ) {
      return null;
    }
    const stateEtag = response.headers.get('ETag');
    if (!stateEtag) return null;

    const currentRevision = this.state()?.presentation_revision ?? 0;
    if (response.body.presentation_revision < currentRevision) {
      // Preserve the newer mounted pointer and let the renderer treat this
      // completed save as the older version it actually observed. It will run
      // the restore handshake for the already-known newer presentation.
      return response.body.presentation_revision;
    }
    if (response.body.presentation_revision >= currentRevision) {
      this.state.set(response.body);
      this.stateEtag.set(stateEtag);
      this.loadStatus.set('ready');
      this.requestError.set(null);
      this.lastSuccessfulSyncAt.set(Date.now());
    }
    if (response.body.presentation_revision > currentRevision) {
      this.broadcastPresentationInvalidation(
        threadId,
        response.body.presentation_revision,
      );
    }

    const update = {
      threadId,
      revision: response.body.presentation_revision,
      sourceVersion: response.body.source_version,
    };
    if (
      this.lastOfficeRuntimeUpdate?.threadId !== update.threadId ||
      this.lastOfficeRuntimeUpdate.revision !== update.revision ||
      this.lastOfficeRuntimeUpdate.sourceVersion !== update.sourceVersion
    ) {
      this.transport.sendCanvasControl(threadId, {
        method: 'canvas.source_updated',
        canvas_id: MAIN_CANVAS_ID,
        path: source.path,
        presentation_revision: response.body.presentation_revision,
        source_version: response.body.source_version,
      });
      this.lastOfficeRuntimeUpdate = update;
    }
    return response.body.presentation_revision;
  }

  /**
   * Save UTF-8 source bytes through the server-issued content pointer.
   *
   * The caller must supply the ETag captured with those exact bytes and the
   * presentation revision they were loaded under. The backend revalidates both
   * before reading the body and returns the new authoritative state.
   */
  saveContent(request: CanvasContentSaveRequest): Observable<CanvasMutationResponse> {
    const threadId = this.threadId();
    if (!threadId) {
      return throwError(() => new Error('Cannot save Canvas content without a thread'));
    }
    return this.http.put<CanvasState>(request.contentUrl, request.content, {
      headers: {
        'Content-Type': 'text/plain; charset=utf-8',
        'If-Match': request.contentEtag,
        'X-Canvas-Presentation-Revision': String(request.presentationRevision),
      },
      observe: 'response',
    }).pipe(
      map(response => this.parseMutationResponse(response)),
      tap(response => this.applyMutationResponse(threadId, response, true)),
    );
  }

  /** Adopt the current workspace bytes after an out-of-band source change. */
  refreshSource(): Observable<CanvasMutationResponse> {
    const threadId = this.threadId();
    const stateEtag = this.stateEtag();
    if (!threadId || !stateEtag) {
      return throwError(() => new Error('Cannot refresh Canvas without current state'));
    }
    const url =
      `${environment.apiUrl}/persistent/threads/${encodeURIComponent(threadId)}` +
      `/canvases/${MAIN_CANVAS_ID}/refresh`;
    return this.http.post<CanvasState>(url, null, {
      headers: {'If-Match': stateEtag},
      observe: 'response',
    }).pipe(
      map(response => this.parseMutationResponse(response)),
      tap(response => this.applyMutationResponse(threadId, response, true)),
    );
  }

  /**
   * Rotate a live app onto a fresh isolated origin.
   *
   * This is deliberately a pointer-free mutation: the server derives the
   * current app, retires its old origin generation, and returns the new
   * authoritative Canvas representation. It never returns a content ETag
   * because workspace-app presentations do not expose editable file bytes.
   */
  resetOrigin(expectedStateEtag?: string): Observable<CanvasStateMutationResponse> {
    const threadId = this.threadId();
    const stateEtag = expectedStateEtag ?? this.stateEtag();
    if (!threadId || !stateEtag) {
      return throwError(() => new Error('Cannot reset Canvas app origin without current state'));
    }
    const url =
      `${environment.apiUrl}/persistent/threads/${encodeURIComponent(threadId)}` +
      `/canvases/${MAIN_CANVAS_ID}/reset-origin`;
    return this.http.post<CanvasState>(url, null, {
      headers: {'If-Match': stateEtag},
      observe: 'response',
    }).pipe(
      map(response => this.parseStateMutationResponse(response)),
      tap(response => this.applyMutationResponse(threadId, response, true)),
    );
  }

  private startBrowserCapabilityRequest(
    threadId: string,
    settled?: (capability: BrowserCapability | null, errorCode: string | null) => void,
  ): void {
    this.cancelBrowserCapabilityRequest();
    if (this.threadId() !== threadId) return;

    const token = Symbol('browser-capability-request');
    this.browserCapabilityRequestToken = token;
    this.browserCapabilityStatus.set('loading');
    const url =
      `${environment.apiUrl}/persistent/threads/${encodeURIComponent(threadId)}` +
      '/browser/capability';
    const subscription = this.http.get<unknown>(url).subscribe({
      next: value => {
        if (!this.isCurrentBrowserCapabilityRequest(token, threadId)) return;
        if (!isBrowserCapability(value)) {
          this.browserCapability.set(null);
          this.browserCapabilityStatus.set('error');
          settled?.(null, 'invalid_browser_capability');
          return;
        }
        this.browserCapability.set(value);
        this.browserCapabilityStatus.set('ready');
        if (!value.can_open_browser && value.reason === 'workspace_unattested') {
          this.scheduleBrowserCapabilityRecheck(threadId);
        }
        settled?.(value, null);
      },
      error: (error: unknown) => {
        if (!this.isCurrentBrowserCapabilityRequest(token, threadId)) return;
        this.browserCapabilityRequestToken = null;
        this.browserCapabilitySubscription = null;
        const requestError = toCanvasRequestError(error);
        if (requestError.status === 404) {
          this.browserCapability.set(DARK_BROWSER_CAPABILITY);
          this.browserCapabilityStatus.set('ready');
          settled?.(DARK_BROWSER_CAPABILITY, null);
          return;
        }
        this.browserCapability.set(null);
        this.browserCapabilityStatus.set('error');
        settled?.(null, requestError.code ?? 'browser_capability_unavailable');
      },
      complete: () => {
        if (!this.isCurrentBrowserCapabilityRequest(token, threadId)) return;
        this.browserCapabilityRequestToken = null;
        this.browserCapabilitySubscription = null;
      },
    });
    if (this.browserCapabilityRequestToken === token) {
      this.browserCapabilitySubscription = subscription;
    } else {
      subscription.unsubscribe();
    }
  }

  private postBrowserOpen(token: symbol, threadId: string): void {
    if (!this.isCurrentBrowserOpen(token, threadId)) return;
    if (Date.now() - this.browserOpenStartedAt >= BROWSER_OPEN_TIMEOUT_MS) {
      this.failBrowserOpen(token, threadId, 'browser_open_timeout');
      return;
    }

    const capability = this.browserCapability();
    if (!capability?.can_open_browser) {
      this.failBrowserOpen(
        token,
        threadId,
        capability?.reason ?? 'browser_not_available',
      );
      return;
    }
    this.browserOpenStatus.set(capability.workspace_ready ? 'browser' : 'workspace');
    const url =
      `${environment.apiUrl}/persistent/threads/${encodeURIComponent(threadId)}` +
      '/browser/open';
    const body: {title: string | null; expected_presentation_revision?: number} = {
      title: this.browserOpenTitle ?? null,
    };
    if (this.browserOpenExpectedPresentationRevision !== undefined) {
      body.expected_presentation_revision = this.browserOpenExpectedPresentationRevision;
    }
    let subscription: Subscription;
    subscription = this.http.post<CanvasState>(
      url,
      body,
      {observe: 'response'},
    ).subscribe({
      next: response => {
        if (!this.isCurrentBrowserOpen(token, threadId)) return;
        if (response.status === 202) {
          this.scheduleBrowserOpenRetry(token, threadId, response.headers.get('Retry-After'));
          return;
        }
        try {
          const mutation = this.parseBrowserOpenMutationResponse(response);
          this.applyMutationResponse(threadId, mutation, true, mutation.changed);
          this.completeBrowserOpen(token, threadId);
        } catch {
          this.failBrowserOpen(token, threadId, 'invalid_browser_open_response');
        }
      },
      error: (error: unknown) => {
        if (!this.isCurrentBrowserOpen(token, threadId)) return;
        const requestError = toCanvasRequestError(error);
        const errorCode = requestError.code ?? 'browser_open_failed';
        this.failBrowserOpen(
          token,
          threadId,
          errorCode,
        );
        // A conditional replacement conflict proves our presentation is
        // stale even if its invalidation was missed. Pull the winning state so
        // the next user click confirms against what is actually on stage.
        if (errorCode === 'canvas_presentation_changed') this.reconcile();
      },
      complete: () => {
        if (
          this.isCurrentBrowserOpen(token, threadId) &&
          this.browserOpenSubscription === subscription
        ) {
          this.browserOpenSubscription = null;
        }
      },
    });
    if (this.isCurrentBrowserOpen(token, threadId)) {
      this.browserOpenSubscription = subscription;
    } else {
      subscription.unsubscribe();
    }
  }

  private scheduleBrowserOpenRetry(
    token: symbol,
    threadId: string,
    retryAfter: string | null,
  ): void {
    this.browserOpenSubscription = null;
    const elapsed = Date.now() - this.browserOpenStartedAt;
    const remaining = BROWSER_OPEN_TIMEOUT_MS - elapsed;
    if (remaining <= 0) {
      this.failBrowserOpen(token, threadId, 'browser_open_timeout');
      return;
    }
    const delay = Math.min(browserOpenRetryDelayMs(retryAfter), remaining);
    this.clearBrowserOpenTimer();
    this.browserOpenTimer = setTimeout(() => {
      this.browserOpenTimer = null;
      if (!this.isCurrentBrowserOpen(token, threadId)) return;
      if (Date.now() - this.browserOpenStartedAt >= BROWSER_OPEN_TIMEOUT_MS) {
        this.failBrowserOpen(token, threadId, 'browser_open_timeout');
        return;
      }
      this.startBrowserCapabilityRequest(threadId, (capability, errorCode) => {
        if (!this.isCurrentBrowserOpen(token, threadId)) return;
        if (errorCode) {
          this.failBrowserOpen(token, threadId, errorCode);
          return;
        }
        if (!capability?.can_open_browser) {
          this.failBrowserOpen(
            token,
            threadId,
            capability?.reason ?? 'browser_not_available',
          );
          return;
        }
        this.postBrowserOpen(token, threadId);
      });
    }, delay);
  }

  private parseBrowserOpenMutationResponse(
    response: HttpResponse<CanvasState>,
  ): BrowserOpenMutationResponse {
    if (response.status !== 200) {
      throw new Error('Shared-browser open returned an unexpected status');
    }
    const stateResponse = this.parseStateMutationResponse(response);
    const changedHeader = response.headers.get('X-Canvas-Mutation-Changed');
    if (changedHeader !== 'true' && changedHeader !== 'false') {
      throw new Error('Shared-browser open returned invalid mutation metadata');
    }
    return {...stateResponse, changed: changedHeader === 'true'};
  }

  private completeBrowserOpen(token: symbol, threadId: string): void {
    if (!this.isCurrentBrowserOpen(token, threadId)) return;
    this.browserOpenToken = null;
    this.clearBrowserOpenTimer();
    this.browserOpenSubscription?.unsubscribe();
    this.browserOpenSubscription = null;
    this.browserOpenStatus.set('idle');
    this.browserOpenError.set(null);
    this.browserOpenExpectedPresentationRevision = undefined;
  }

  private failBrowserOpen(token: symbol, threadId: string, code: string): void {
    if (!this.isCurrentBrowserOpen(token, threadId)) return;
    this.browserOpenToken = null;
    this.clearBrowserOpenTimer();
    this.browserOpenSubscription?.unsubscribe();
    this.browserOpenSubscription = null;
    this.browserOpenStatus.set('error');
    this.browserOpenError.set(code);
  }

  private isCurrentBrowserOpen(token: symbol, threadId: string): boolean {
    return this.browserOpenToken === token && this.threadId() === threadId;
  }

  private isCurrentBrowserCapabilityRequest(token: symbol, threadId: string): boolean {
    return this.browserCapabilityRequestToken === token && this.threadId() === threadId;
  }

  private handleInvalidation(event: CanvasInvalidation, rebroadcast: boolean): void {
    if (event.threadId !== this.threadId() || event.canvasId !== MAIN_CANVAS_ID) return;

    // The chat tab owns the server transport; wrapper-only tabs intentionally
    // do not. Relay only the bounded pointer so sibling wrappers reconcile via
    // their own authenticated REST request. A BroadcastChannel receiver passes
    // `false` to prevent rebroadcast loops.
    if (rebroadcast && event.presentationRevision !== null) {
      this.broadcastPresentationInvalidation(event.threadId, event.presentationRevision);
    }

    const appliedRevision = this.state()?.presentation_revision ?? 0;
    if (
      event.presentationRevision !== null &&
      event.presentationRevision <= appliedRevision
    ) {
      return;
    }

    if (this.activeRequestToken) {
      if (event.presentationRevision === null) {
        // A direct reconcile_required frame exists specifically because the
        // durable event write failed. Ensure a request that may have started
        // before the state transition is followed by another authoritative GET.
        this.pendingForcedReconcile = true;
      } else {
        this.queuePendingRevision(event.presentationRevision, true);
      }
      return;
    }

    this.startReconcile(event.presentationRevision);
  }

  private readonly handleBroadcastMessage = (event: MessageEvent<unknown>): void => {
    const invalidation = parseCanvasBroadcastInvalidation(event.data);
    if (!invalidation) return;
    this.handleInvalidation({
      threadId: invalidation.threadId,
      method: 'canvas.updated',
      canvasId: MAIN_CANVAS_ID,
      presentationRevision: invalidation.presentationRevision,
      sourceType: null,
      updatedAt: null,
    }, false);
  };

  private reconcileIfStale(): void {
    if (!this.threadId() || this.activeRequestToken) return;
    if (
      this.lastSuccessfulSyncAt() !== null &&
      Date.now() - this.lastSuccessfulSyncAt()! < CANVAS_RECONCILE_STALE_MS
    ) {
      return;
    }
    this.startReconcile(null);
  }

  private startReconcile(
    targetRevision: number | null,
    targetFollowUpAllowed = true,
  ): void {
    const threadId = this.threadId();
    if (!threadId || this.activeRequestToken) return;

    const token = Symbol('canvas-state-request');
    this.activeRequestToken = token;
    this.activeTargetRevision = targetRevision;
    this.activeTargetFollowUpAllowed = targetFollowUpAllowed;
    this.loadStatus.set('loading');
    this.requestError.set(null);

    const url =
      `${environment.apiUrl}/persistent/threads/${encodeURIComponent(threadId)}` +
      `/canvases/${MAIN_CANVAS_ID}`;
    const subscription = this.http
      .get<CanvasState>(url, {observe: 'response'})
      .subscribe({
        next: (response) => this.applyResponse(token, threadId, response),
        error: (error: unknown) => this.failRequest(token, threadId, error),
        complete: () => this.completeRequest(token, threadId),
      });

    // HttpClient is asynchronous in production, but this guard also handles a
    // synchronous test double which completes during subscribe().
    if (this.activeRequestToken === token) {
      this.requestSubscription = subscription;
    } else {
      subscription.unsubscribe();
    }
  }

  private applyResponse(
    token: symbol,
    threadId: string,
    response: HttpResponse<CanvasState>,
  ): void {
    if (!this.isCurrentRequest(token, threadId)) return;

    if (response.status === 204 || response.body === null) {
      this.state.set(null);
      this.stateEtag.set(null);
      this.queueTargetFollowUpIfNeeded(0);
    } else if (isCanvasState(response.body)) {
      const currentRevision = this.state()?.presentation_revision ?? 0;
      if (response.body.presentation_revision >= currentRevision) {
        this.state.set(response.body);
        this.stateEtag.set(response.headers.get('ETag'));
      }
      this.queueTargetFollowUpIfNeeded(response.body.presentation_revision);
    } else {
      this.requestError.set({status: response.status, code: 'invalid_canvas_state'});
      this.loadStatus.set('error');
      return;
    }

    this.lastSuccessfulSyncAt.set(Date.now());
    this.requestError.set(null);
    this.loadStatus.set('ready');
  }

  private failRequest(token: symbol, threadId: string, error: unknown): void {
    if (!this.isCurrentRequest(token, threadId)) return;
    this.activeRequestToken = null;
    this.activeTargetRevision = null;
    this.activeTargetFollowUpAllowed = true;
    this.requestSubscription = null;
    this.loadStatus.set('error');
    const requestError = toCanvasRequestError(error);
    this.requestError.set(requestError);

    if (isTerminalCanvasStateFailure(requestError.status)) {
      // Authorization loss and a missing thread are teardown boundaries, not
      // transient overlays. Do not leave bytes or an ETag from a state the
      // current browser may no longer inspect, and do not immediately replay a
      // queued invalidation against the same terminal response.
      this.state.set(null);
      this.stateEtag.set(null);
      this.pendingRevision = null;
      this.pendingRevisionFollowUpAllowed = false;
      this.pendingForcedReconcile = false;
      this.lastSuccessfulSyncAt.set(Date.now());
      return;
    }

    this.drainPendingReconcile();
  }

  private completeRequest(token: symbol, threadId: string): void {
    if (!this.isCurrentRequest(token, threadId)) return;
    this.activeRequestToken = null;
    this.activeTargetRevision = null;
    this.activeTargetFollowUpAllowed = true;
    this.requestSubscription = null;
    this.drainPendingReconcile();
  }

  private drainPendingReconcile(): void {
    if (!this.threadId() || this.activeRequestToken) return;

    const appliedRevision = this.state()?.presentation_revision ?? 0;
    const pendingRevision = this.pendingRevision;
    const pendingRevisionFollowUpAllowed = this.pendingRevisionFollowUpAllowed;
    const force = this.pendingForcedReconcile;
    this.pendingRevision = null;
    this.pendingRevisionFollowUpAllowed = false;
    this.pendingForcedReconcile = false;

    if (force) {
      this.startReconcile(null);
    } else if (pendingRevision !== null && pendingRevision > appliedRevision) {
      this.startReconcile(pendingRevision, pendingRevisionFollowUpAllowed);
    }
  }

  private cancelRequest(): void {
    this.activeRequestToken = null;
    this.activeTargetRevision = null;
    this.activeTargetFollowUpAllowed = true;
    this.requestSubscription?.unsubscribe();
    this.requestSubscription = null;
  }

  private cancelBrowserCapabilityRequest(): void {
    this.browserCapabilityRequestToken = null;
    this.browserCapabilitySubscription?.unsubscribe();
    this.browserCapabilitySubscription = null;
    this.clearBrowserCapabilityRecheck();
  }

  private scheduleBrowserCapabilityRecheck(threadId: string): void {
    this.clearBrowserCapabilityRecheck();
    this.browserCapabilityRecheckTimer = setTimeout(() => {
      this.browserCapabilityRecheckTimer = null;
      if (this.threadId() !== threadId || this.browserOpenToken) return;
      this.startBrowserCapabilityRequest(threadId);
    }, BROWSER_CAPABILITY_RECHECK_MS);
  }

  private clearBrowserCapabilityRecheck(): void {
    if (this.browserCapabilityRecheckTimer !== null) {
      clearTimeout(this.browserCapabilityRecheckTimer);
    }
    this.browserCapabilityRecheckTimer = null;
  }

  private clearBrowserOpenTimer(): void {
    if (this.browserOpenTimer !== null) clearTimeout(this.browserOpenTimer);
    this.browserOpenTimer = null;
  }

  private cancelBrowserWork(): void {
    this.cancelBrowserCapabilityRequest();
    this.browserOpenToken = null;
    this.browserOpenSubscription?.unsubscribe();
    this.browserOpenSubscription = null;
    this.clearBrowserOpenTimer();
    this.browserOpenStartedAt = 0;
    this.browserOpenTitle = undefined;
    this.browserOpenExpectedPresentationRevision = undefined;
  }

  private isCurrentRequest(token: symbol, threadId: string): boolean {
    return this.activeRequestToken === token && this.threadId() === threadId;
  }

  private queueTargetFollowUpIfNeeded(receivedRevision: number): void {
    const targetRevision = this.activeTargetRevision;
    if (
      targetRevision === null ||
      receivedRevision >= targetRevision ||
      !this.activeTargetFollowUpAllowed
    ) {
      return;
    }
    // One bounded follow-up protects against a GET that raced a committed
    // invalidation through a lagging read path. A second below-floor response
    // waits for the normal focus/reconnect reconciliation instead of looping.
    this.queuePendingRevision(targetRevision, false);
  }

  private queuePendingRevision(revision: number, allowTargetFollowUp: boolean): void {
    if (this.pendingRevision === null || revision > this.pendingRevision) {
      this.pendingRevision = revision;
      this.pendingRevisionFollowUpAllowed = allowTargetFollowUp;
    } else if (revision === this.pendingRevision) {
      this.pendingRevisionFollowUpAllowed ||= allowTargetFollowUp;
    }
  }

  private parseMutationResponse(response: HttpResponse<CanvasState>): CanvasMutationResponse {
    const stateResponse = this.parseStateMutationResponse(response);
    const contentEtag = response.headers.get('X-Canvas-Content-ETag');
    const expectedContentEtag = stateResponse.state.source_version
      ? `"${stateResponse.state.source_version}"`
      : null;
    if (!contentEtag || contentEtag !== expectedContentEtag) {
      throw new Error('Canvas mutation returned invalid precondition metadata');
    }
    return {...stateResponse, contentEtag};
  }

  private parseStateMutationResponse(
    response: HttpResponse<CanvasState>,
  ): CanvasStateMutationResponse {
    if (!isCanvasState(response.body)) {
      throw new Error('Canvas mutation returned invalid state');
    }
    const stateEtag = response.headers.get('ETag');
    if (!stateEtag) {
      throw new Error('Canvas mutation returned invalid precondition metadata');
    }
    return {state: response.body, stateEtag};
  }

  private applyMutationResponse(
    threadId: string,
    response: CanvasStateMutationResponse,
    notifyRuntime: boolean,
    changed = true,
  ): void {
    if (this.threadId() !== threadId) return;
    const currentRevision = this.state()?.presentation_revision ?? 0;
    if (response.state.presentation_revision < currentRevision) return;
    this.state.set(response.state);
    this.stateEtag.set(response.stateEtag);
    this.loadStatus.set('ready');
    this.requestError.set(null);
    this.lastSuccessfulSyncAt.set(Date.now());
    if (changed) {
      this.broadcastPresentationInvalidation(
        threadId,
        response.state.presentation_revision,
      );
    }

    const source = response.state.source;
    if (!notifyRuntime || !changed) return;
    if (
      source?.type === 'workspace_file' &&
      typeof source.path === 'string' &&
      response.state.source_version
    ) {
      this.transport.sendCanvasControl(threadId, {
        method: 'canvas.source_updated',
        canvas_id: MAIN_CANVAS_ID,
        path: source.path,
        presentation_revision: response.state.presentation_revision,
        source_version: response.state.source_version,
      });
    } else if (source?.type === 'workspace_app' || source?.type === 'browser') {
      this.transport.sendCanvasControl(threadId, {
        method: 'canvas.presentation_updated',
        canvas_id: MAIN_CANVAS_ID,
        presentation_revision: response.state.presentation_revision,
      });
    }
  }

  private broadcastPresentationInvalidation(threadId: string, revision: number): void {
    try {
      this.broadcastChannel?.postMessage({
        type: 'canvas.presentation_invalidated',
        threadId,
        canvasId: MAIN_CANVAS_ID,
        presentationRevision: revision,
      } satisfies CanvasBroadcastInvalidation);
    } catch {
      // A browser may revoke storage-backed channel access after creation.
      // The mutation is already authoritative; focus reconciliation repairs
      // sibling tabs without turning this local optimization into a failure.
      this.broadcastChannel?.close();
      this.broadcastChannel = null;
    }
  }
}

export function parseCanvasBroadcastInvalidation(
  value: unknown,
): CanvasBroadcastInvalidation | null {
  if (!isRecord(value)) return null;
  const revision = value['presentationRevision'];
  return value['type'] === 'canvas.presentation_invalidated' &&
    typeof value['threadId'] === 'string' &&
    value['threadId'].length > 0 &&
    value['threadId'].length <= 256 &&
    value['canvasId'] === MAIN_CANVAS_ID &&
    typeof revision === 'number' &&
    Number.isSafeInteger(revision) &&
    revision >= 0
    ? {
        type: 'canvas.presentation_invalidated',
        threadId: value['threadId'],
        canvasId: MAIN_CANVAS_ID,
        presentationRevision: revision,
      }
    : null;
}

const CANVAS_RENDERERS = new Set([
  'auto',
  'markdown',
  'text',
  'html',
  'html-interactive',
  'image',
  'office',
]);
const CANVAS_STATUSES = new Set([
  'ready',
  'starting',
  'source_changed',
  'unavailable',
  'ended',
  'error',
  'cleared',
]);
const BROWSER_CAPABILITY_REASONS = new Set([
  'feature_disabled',
  'workspace_required',
  'workspace_unattested',
  'workspace_unroutable',
  'transport_unavailable',
]);
const BROWSER_CAPABILITY_KEYS = [
  'can_open_browser',
  'feature_enabled',
  'reason',
  'workspace_ready',
];

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

export function isBrowserCapability(value: unknown): value is BrowserCapability {
  if (!isRecord(value)) return false;
  const keys = Object.keys(value).sort();
  const reason = value['reason'];
  return (
    keys.length === BROWSER_CAPABILITY_KEYS.length &&
    keys.every((key, index) => key === BROWSER_CAPABILITY_KEYS[index]) &&
    typeof value['feature_enabled'] === 'boolean' &&
    typeof value['can_open_browser'] === 'boolean' &&
    typeof value['workspace_ready'] === 'boolean' &&
    (reason === null || (typeof reason === 'string' && BROWSER_CAPABILITY_REASONS.has(reason)))
  );
}

export function browserOpenRetryDelayMs(retryAfter: string | null): number {
  if (retryAfter === null || retryAfter.trim() === '') return BROWSER_OPEN_RETRY_DEFAULT_MS;
  const seconds = Number(retryAfter);
  if (!Number.isFinite(seconds) || seconds < 0) return BROWSER_OPEN_RETRY_DEFAULT_MS;
  return Math.min(Math.max(seconds * 1_000, 250), BROWSER_OPEN_RETRY_MAX_MS);
}

export function isCanvasState(value: unknown): value is CanvasState {
  if (!isRecord(value)) return false;
  const source = value['source'];
  const capabilities = value['capabilities'];
  return (
    value['canvas_id'] === MAIN_CANVAS_ID &&
    (source === null || (isRecord(source) && typeof source['type'] === 'string')) &&
    (value['title'] === null || typeof value['title'] === 'string') &&
    typeof value['renderer'] === 'string' &&
    CANVAS_RENDERERS.has(value['renderer']) &&
    typeof value['editable'] === 'boolean' &&
    (value['alt_text'] === null || typeof value['alt_text'] === 'string') &&
    typeof value['presentation_revision'] === 'number' &&
    Number.isSafeInteger(value['presentation_revision']) &&
    value['presentation_revision'] >= 0 &&
    (value['source_version'] === null || typeof value['source_version'] === 'string') &&
    (value['content_url'] === undefined ||
      value['content_url'] === null ||
      typeof value['content_url'] === 'string') &&
    typeof value['status'] === 'string' &&
    CANVAS_STATUSES.has(value['status']) &&
    isRecord(capabilities) &&
    typeof capabilities['can_edit'] === 'boolean' &&
    typeof capabilities['can_pop_out'] === 'boolean' &&
    typeof capabilities['can_take_control'] === 'boolean' &&
    (capabilities['can_view_office'] === undefined ||
      typeof capabilities['can_view_office'] === 'boolean') &&
    (capabilities['can_stream_browser'] === undefined ||
      typeof capabilities['can_stream_browser'] === 'boolean') &&
    typeof value['updated_at'] === 'string'
  );
}

function toCanvasRequestError(error: unknown): CanvasRequestError {
  if (!(error instanceof HttpErrorResponse)) return {status: null, code: null};
  const detail = isRecord(error.error) ? error.error['detail'] : null;
  const code =
    isRecord(detail) && typeof detail['code'] === 'string'
      ? detail['code']
      : isRecord(error.error) && typeof error.error['code'] === 'string'
        ? error.error['code']
        : null;
  return {status: error.status || null, code};
}

function isTerminalCanvasStateFailure(status: number | null): boolean {
  return status === 401 || status === 403 || status === 404;
}
