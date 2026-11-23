import { DestroyRef, Injectable, InjectionToken, inject, signal } from '@angular/core';
import { CanvasState } from '../../core/models/canvas.model';
import {
  BROWSER_CLIENT_FEATURES,
  BrowserBaton,
  BrowserControl,
  BrowserInput,
  BrowserPageState,
  browserStreamUrl,
  encodeBrowserControl,
  encodeBrowserInput,
  parseBrowserServerMessage,
} from './canvas-browser-protocol';

export type CanvasBrowserConnectionStatus =
  | 'idle'
  | 'connecting'
  | 'ready'
  | 'reconnecting'
  | 'ended'
  | 'viewer_limit'
  | 'unauthorized'
  | 'unavailable'
  | 'error';

export type CanvasBrowserSocketFactory = (url: string) => WebSocket;
export type CanvasBrowserBitmapFactory = (blob: Blob) => Promise<ImageBitmap>;

export interface CanvasBrowserTimeouts {
  set(callback: () => void, delayMs: number): ReturnType<typeof setTimeout>;
  clear(handle: ReturnType<typeof setTimeout>): void;
}

export interface CanvasBrowserVisibility {
  readonly visibilityState: DocumentVisibilityState;
  addEventListener(type: 'visibilitychange', listener: () => void): void;
  removeEventListener(type: 'visibilitychange', listener: () => void): void;
}

export const CANVAS_BROWSER_SOCKET_FACTORY = new InjectionToken<CanvasBrowserSocketFactory>(
  'CANVAS_BROWSER_SOCKET_FACTORY',
  { factory: () => (url) => new WebSocket(url) },
);

export const CANVAS_BROWSER_BITMAP_FACTORY = new InjectionToken<CanvasBrowserBitmapFactory>(
  'CANVAS_BROWSER_BITMAP_FACTORY',
  { factory: () => (blob) => globalThis.createImageBitmap(blob) },
);

export const CANVAS_BROWSER_TIMEOUTS = new InjectionToken<CanvasBrowserTimeouts>(
  'CANVAS_BROWSER_TIMEOUTS',
  {
    factory: () => ({
      set: (callback, delayMs) => setTimeout(callback, delayMs),
      clear: (handle) => clearTimeout(handle),
    }),
  },
);

export const CANVAS_BROWSER_VISIBILITY = new InjectionToken<CanvasBrowserVisibility | null>(
  'CANVAS_BROWSER_VISIBILITY',
  { factory: () => (typeof document === 'undefined' ? null : document) },
);

interface DesiredBrowser {
  readonly threadId: string;
  readonly revision: number;
  readonly identity: string;
  readonly url: string | null;
}

interface LastPresentation {
  readonly active: boolean;
  readonly threadId: string | null;
  readonly state: CanvasState | null;
}

const RECONNECT_DELAYS_MS = [250, 500, 1_000, 2_000, 5_000] as const;
/**
 * Longest a stream socket may stay in CONNECTING. The broker runs the
 * workspace's `browser-exec stream_info` (which may start Chromium) before it
 * accepts, so a healthy handshake can take seconds; one sent to an
 * orchestrator pod that is going away can hang for minutes and never fail
 * (WP1 k3d run, answer 8), and the backoff only runs after a close or error.
 */
export const BROWSER_HANDSHAKE_TIMEOUT_MS = 10_000;

/** CSS pixels. */
export interface BrowserViewportSize {
  readonly width: number;
  readonly height: number;
}

/** Pane-local owner of one shared-browser socket and decoded bitmap pipeline. */
@Injectable()
export class CanvasBrowserController {
  private readonly socketFactory = inject(CANVAS_BROWSER_SOCKET_FACTORY);
  private readonly bitmapFactory = inject(CANVAS_BROWSER_BITMAP_FACTORY);
  private readonly timeouts = inject(CANVAS_BROWSER_TIMEOUTS);
  private readonly visibility = inject(CANVAS_BROWSER_VISIBILITY);
  private readonly destroyRef = inject(DestroyRef);

  readonly connectionStatus = signal<CanvasBrowserConnectionStatus>('idle');
  readonly pageState = signal<BrowserPageState | null>(null);
  readonly frame = signal<ImageBitmap | null>(null);
  readonly errorCode = signal<string | null>(null);
  readonly errorMessage = signal<string | null>(null);
  /** Expected holder after a sent baton control; cleared only by authoritative STATE. */
  readonly pendingBaton = signal<BrowserBaton | null>(null);
  /** The daemon sizes its page to `requestViewport`; false on older daemons. */
  readonly viewportControl = signal(false);
  /**
   * CSS size of the page shown in `frame` (the FRAME header), set together
   * with it: the exact space viewer input maps onto, even while a resize is
   * on its way. Null when the daemon did not say.
   */
  readonly frameSize = signal<BrowserViewportSize | null>(null);

  private presentation: LastPresentation | null = null;
  private desired: DesiredBrowser | null = null;
  private socket: WebSocket | null = null;
  private expectedGeneration: string | null = null;
  private epoch = 0;
  private decodeEpoch: number | null = null;
  private reconnectAttempt = 0;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private handshakeTimer: ReturnType<typeof setTimeout> | null = null;
  private terminalIdentity: string | null = null;
  private destroyed = false;

  private readonly visibilityListener = (): void => this.evaluatePresentation();

  constructor() {
    this.visibility?.addEventListener('visibilitychange', this.visibilityListener);
    this.destroyRef.onDestroy(() => {
      this.destroyed = true;
      this.visibility?.removeEventListener('visibilitychange', this.visibilityListener);
      this.desired = null;
      this.presentation = null;
      this.terminalIdentity = null;
      this.teardown('idle', true);
    });
  }

  syncPresentation(active: boolean, threadId: string | null, state: CanvasState | null): void {
    this.presentation = { active, threadId, state };
    this.evaluatePresentation();
  }

  /** Explicitly retry a still-eligible source after a terminal/manual state. */
  retry(): void {
    if (!this.desired || this.destroyed || !this.isVisible()) return;
    this.terminalIdentity = null;
    this.reconnectAttempt = 0;
    this.teardown('idle', true);
    if (!this.desired.url) {
      this.evaluatePresentation();
      return;
    }
    this.connect(false);
  }

  sendControl(message: BrowserControl): boolean {
    const baton = this.pageState()?.baton;
    if (message.op === 'take_baton') {
      if (baton !== 'agent' || this.pendingBaton() !== null) return false;
    } else if (message.op === 'release_baton') {
      if (baton !== 'user' || this.pendingBaton() !== null) return false;
    } else if (baton !== 'user') {
      return false;
    }
    const sent = this.send(encodeBrowserControl, message);
    if (!sent) return false;
    if (message.op === 'take_baton') this.pendingBaton.set('user');
    if (message.op === 'release_baton') this.pendingBaton.set('agent');
    if (
      message.op === 'navigate' ||
      message.op === 'back' ||
      message.op === 'reload'
    ) {
      if (this.errorCode() === 'navigation_rejected') {
        this.errorCode.set(null);
        this.errorMessage.set(null);
      }
    }
    return true;
  }

  sendInput(message: BrowserInput): boolean {
    if (this.pageState()?.baton !== 'user' || this.pendingBaton() !== null) return false;
    return this.send(encodeBrowserInput, message);
  }

  /**
   * Ask the daemon to size its page's viewport (CSS pixels). Only while this
   * viewer drives and the daemon advertised the capability; a size the page
   * already has is not sent again. The daemon clamps and reports the real
   * size in STATE.
   */
  requestViewport(width: number, height: number): boolean {
    if (
      !this.viewportControl() ||
      !Number.isInteger(width) ||
      !Number.isInteger(height) ||
      width < 1 ||
      height < 1
    ) {
      return false;
    }
    const state = this.pageState();
    if (state?.baton !== 'user' || this.pendingBaton() !== null) return false;
    if (state.viewport.width === width && state.viewport.height === height) return false;
    return this.send(encodeBrowserControl, {op: 'set_viewport', width, height});
  }

  private evaluatePresentation(): void {
    if (this.destroyed) return;
    const candidate = this.toDesired(this.presentation);
    if (!candidate) {
      this.desired = null;
      this.terminalIdentity = null;
      this.reconnectAttempt = 0;
      this.teardown('idle', true);
      return;
    }

    const changed = this.desired?.identity !== candidate.identity;
    if (changed) {
      this.teardown('idle', true);
      this.desired = candidate;
      this.terminalIdentity = null;
      this.reconnectAttempt = 0;
    } else {
      this.desired = candidate;
    }

    if (!candidate.url) {
      this.teardown('error', true);
      this.errorCode.set('invalid_browser_stream_url');
      this.errorMessage.set(null);
      this.terminalIdentity = candidate.identity;
      return;
    }

    if (!this.isVisible()) {
      this.teardown('idle', true);
      return;
    }
    if (
      this.terminalIdentity === candidate.identity ||
      this.socket !== null ||
      this.reconnectTimer !== null
    ) {
      return;
    }
    this.connect(false);
  }

  private toDesired(presentation: LastPresentation | null): DesiredBrowser | null {
    const state = presentation?.state;
    if (
      !presentation?.active ||
      !presentation.threadId ||
      state?.source?.type !== 'browser' ||
      state.renderer !== 'auto' ||
      state.capabilities.can_stream_browser !== true ||
      (state.status !== 'ready' && state.status !== 'starting')
    ) {
      return null;
    }
    const identity = `${presentation.threadId}:${state.presentation_revision}`;
    const url = browserStreamUrl(presentation.threadId);
    return {
      threadId: presentation.threadId,
      revision: state.presentation_revision,
      identity,
      url,
    };
  }

  private connect(reconnecting: boolean): void {
    const desired = this.desired;
    if (!desired?.url || !this.isVisible() || this.destroyed) return;
    this.clearReconnectTimer();
    this.expectedGeneration = null;
    this.errorCode.set(null);
    this.errorMessage.set(null);
    this.connectionStatus.set(reconnecting ? 'reconnecting' : 'connecting');

    const epoch = ++this.epoch;
    let socket: WebSocket;
    try {
      socket = this.socketFactory(desired.url);
      socket.binaryType = 'arraybuffer';
    } catch {
      this.scheduleReconnect('browser_stream_connect_failed');
      return;
    }
    this.socket = socket;
    socket.onopen = () => {
      if (this.isCurrent(epoch, socket)) this.clearHandshakeTimer();
    };
    socket.onmessage = (event) => this.handleMessage(epoch, socket, event);
    socket.onclose = (event) => this.handleClose(epoch, socket, event);
    socket.onerror = () => this.handleSocketError(epoch, socket);
    this.handshakeTimer = this.timeouts.set(() => {
      this.handshakeTimer = null;
      if (this.isCurrent(epoch, socket) && socket.readyState === 0) {
        this.scheduleReconnect('browser_stream_handshake_timeout');
      }
    }, BROWSER_HANDSHAKE_TIMEOUT_MS);
  }

  private handleMessage(epoch: number, socket: WebSocket, event: MessageEvent<unknown>): void {
    if (!this.isCurrent(epoch, socket)) return;
    this.clearHandshakeTimer();
    if (!(event.data instanceof ArrayBuffer)) {
      this.finishTerminal('error', 'invalid_browser_protocol');
      return;
    }
    const message = parseBrowserServerMessage(event.data);
    if (!message) {
      this.finishTerminal('error', 'invalid_browser_protocol');
      return;
    }

    if (message.type === 'state') {
      const first = this.expectedGeneration === null;
      if (first) {
        this.expectedGeneration = message.generation;
        this.reconnectAttempt = 0;
        this.connectionStatus.set('ready');
        this.errorCode.set(null);
        this.errorMessage.set(null);
      } else if (message.generation !== this.expectedGeneration) {
        this.finishTerminal('ended', 'browser_generation_ended');
        return;
      }
      this.pageState.set(message.state);
      this.viewportControl.set(message.viewportControl);
      if (this.pendingBaton() === message.state.baton) this.pendingBaton.set(null);
      // Opt in to the STATE keys this Cockpit understands. An older daemon
      // ignores the op, so viewportControl stays false and the pane keeps
      // scaling the page to fit.
      if (first) {
        this.send(encodeBrowserControl, {
          op: 'client_features',
          features: [...BROWSER_CLIENT_FEATURES],
        });
      }
      return;
    }

    if (message.type === 'frame') {
      if (
        this.expectedGeneration === null ||
        message.generation !== this.expectedGeneration ||
        this.decodeEpoch !== null
      ) {
        return;
      }
      this.decodeFrame(epoch, message.jpeg, frameSizeOf(message.headerWidth, message.headerHeight));
      return;
    }

    this.handleServerError(message.code, message.message);
  }

  private decodeFrame(epoch: number, jpeg: Uint8Array, size: BrowserViewportSize | null): void {
    this.decodeEpoch = epoch;
    let decoded: Promise<ImageBitmap>;
    try {
      const jpegBytes = new Uint8Array(jpeg.byteLength);
      jpegBytes.set(jpeg);
      decoded = this.bitmapFactory(new Blob([jpegBytes.buffer], { type: 'image/jpeg' }));
    } catch {
      this.decodeEpoch = null;
      this.errorCode.set('browser_frame_decode_failed');
      return;
    }
    void decoded
      .then((bitmap) => {
        if (this.epoch !== epoch || this.decodeEpoch !== epoch || !this.socket) {
          closeBitmap(bitmap);
          return;
        }
        const previous = this.frame();
        this.frameSize.set(sameSize(this.frameSize(), size) ? this.frameSize() : size);
        this.frame.set(bitmap);
        if (previous && previous !== bitmap) closeBitmap(previous);
        if (this.errorCode() === 'browser_frame_decode_failed') this.errorCode.set(null);
      })
      .catch(() => {
        if (this.epoch === epoch && this.decodeEpoch === epoch) {
          this.errorCode.set('browser_frame_decode_failed');
        }
      })
      .finally(() => {
        if (this.decodeEpoch === epoch) this.decodeEpoch = null;
      });
  }

  private handleServerError(code: string, message: string): void {
    if (code === 'navigation_rejected') {
      this.errorCode.set(code);
      this.errorMessage.set(message);
      return;
    }
    if (code === 'browser_gone') {
      this.finishTerminal('ended', code, message);
      return;
    }
    if (code === 'viewer_limit') {
      this.finishTerminal('viewer_limit', code, message);
      return;
    }
    this.finishTerminal('error', code, message);
  }

  private handleClose(epoch: number, socket: WebSocket, event: CloseEvent): void {
    if (!this.isCurrent(epoch, socket)) return;
    this.detachSocket(false);
    switch (event.code) {
      case 4400:
        this.finishTerminal('error', 'invalid_browser_protocol');
        break;
      case 4401:
      case 4403:
        this.finishTerminal('unauthorized', 'browser_unauthorized');
        break;
      case 4404:
        this.finishTerminal('unavailable', 'shared_browser_disabled');
        break;
      case 4409:
        this.finishTerminal('ended', 'browser_generation_ended');
        break;
      case 4429:
        this.finishTerminal('viewer_limit', 'viewer_limit');
        break;
      case 1012:
        // Uvicorn sends Service Restart during a graceful pod rollout. The
        // close is clean, but the browser source and generation are still
        // valid; reconnect through the replacement orchestrator pod.
      case 4502:
        this.scheduleReconnect('browser_stream_unavailable');
        break;
      case 4503:
        this.finishTerminal('unavailable', 'browser_workspace_unavailable');
        break;
      default:
        if (!event.wasClean || event.code === 1006) {
          this.scheduleReconnect('browser_stream_disconnected');
        } else {
          this.finishTerminal('error', 'browser_stream_closed');
        }
        break;
    }
  }

  private handleSocketError(epoch: number, socket: WebSocket): void {
    if (!this.isCurrent(epoch, socket)) return;
    this.detachSocket(true);
    this.scheduleReconnect('browser_stream_unavailable');
  }

  private scheduleReconnect(code: string): void {
    if (!this.desired || !this.isVisible() || this.destroyed) {
      this.teardown('idle', true);
      return;
    }
    this.detachSocket(true);
    this.clearFrame();
    this.expectedGeneration = null;
    this.errorCode.set(code);
    this.errorMessage.set(null);
    this.pendingBaton.set(null);
    this.viewportControl.set(false);
    this.connectionStatus.set('reconnecting');
    const desiredIdentity = this.desired.identity;
    const delay =
      RECONNECT_DELAYS_MS[Math.min(this.reconnectAttempt, RECONNECT_DELAYS_MS.length - 1)];
    this.reconnectAttempt += 1;
    this.reconnectTimer = this.timeouts.set(() => {
      this.reconnectTimer = null;
      if (
        this.desired?.identity === desiredIdentity &&
        this.terminalIdentity !== desiredIdentity &&
        this.isVisible()
      ) {
        this.connect(true);
      }
    }, delay);
  }

  private finishTerminal(
    status: Exclude<CanvasBrowserConnectionStatus, 'connecting' | 'ready' | 'reconnecting'>,
    code: string,
    message: string | null = null,
  ): void {
    const identity = this.desired?.identity ?? null;
    this.detachSocket(true);
    this.clearReconnectTimer();
    this.clearFrame();
    this.pageState.set(null);
    this.expectedGeneration = null;
    this.connectionStatus.set(status);
    this.errorCode.set(code);
    this.errorMessage.set(message);
    this.pendingBaton.set(null);
    this.viewportControl.set(false);
    this.terminalIdentity = identity;
  }

  private teardown(status: 'idle' | 'error', clearPageState: boolean): void {
    this.detachSocket(true);
    this.clearReconnectTimer();
    this.clearFrame();
    if (clearPageState) this.pageState.set(null);
    this.expectedGeneration = null;
    this.decodeEpoch = null;
    this.pendingBaton.set(null);
    this.viewportControl.set(false);
    this.connectionStatus.set(status);
    if (status === 'idle') {
      this.errorCode.set(null);
      this.errorMessage.set(null);
    }
  }

  private detachSocket(close: boolean): void {
    const socket = this.socket;
    this.socket = null;
    this.epoch += 1;
    this.clearHandshakeTimer();
    if (!socket) return;
    socket.onopen = null;
    socket.onmessage = null;
    socket.onclose = null;
    socket.onerror = null;
    if (close) {
      try {
        socket.close(1000, 'Canvas browser detached');
      } catch {
        // A browser may reject close while its constructor is still failing.
      }
    }
  }

  private clearReconnectTimer(): void {
    if (this.reconnectTimer !== null) this.timeouts.clear(this.reconnectTimer);
    this.reconnectTimer = null;
  }

  private clearHandshakeTimer(): void {
    if (this.handshakeTimer !== null) this.timeouts.clear(this.handshakeTimer);
    this.handshakeTimer = null;
  }

  private clearFrame(): void {
    const current = this.frame();
    this.frame.set(null);
    this.frameSize.set(null);
    if (current) closeBitmap(current);
  }

  private isVisible(): boolean {
    return this.visibility === null || this.visibility.visibilityState === 'visible';
  }

  private isCurrent(epoch: number, socket: WebSocket): boolean {
    return !this.destroyed && this.epoch === epoch && this.socket === socket;
  }

  private send<T>(encoder: (message: T) => ArrayBuffer, message: T): boolean {
    const socket = this.socket;
    if (
      !socket ||
      socket.readyState !== 1 ||
      this.connectionStatus() !== 'ready' ||
      !this.desired ||
      !this.isVisible()
    ) {
      return false;
    }
    try {
      socket.send(encoder(message));
      return true;
    } catch {
      return false;
    }
  }
}

function frameSizeOf(width: number | null, height: number | null): BrowserViewportSize | null {
  if (width === null || height === null) return null;
  const size = {width: Math.round(width), height: Math.round(height)};
  return size.width >= 1 && size.height >= 1 ? size : null;
}

function sameSize(a: BrowserViewportSize | null, b: BrowserViewportSize | null): boolean {
  return a === b || (!!a && !!b && a.width === b.width && a.height === b.height);
}

function closeBitmap(bitmap: ImageBitmap): void {
  try {
    bitmap.close();
  } catch {
    // Closing is idempotent in browsers, but test/polyfill implementations may throw.
  }
}
