import {
  AfterViewInit,
  ChangeDetectionStrategy,
  Component,
  ElementRef,
  EffectRef,
  Injector,
  OnDestroy,
  ViewChild,
  computed,
  effect,
  inject,
  signal,
  untracked,
} from '@angular/core';
import { TranslocoPipe } from '@jsverse/transloco';
import { CanvasService } from '../../core/services/canvas.service';
import { PersistentThreadTransportBridge } from '../../core/services/persistent-thread-transport-bridge.service';
import { AppButtonComponent } from '../../ui/button';
import { AppIconComponent } from '../../ui/icon';
import { AppIconButtonComponent } from '../../ui/icon-button';
import { AppSpinnerComponent } from '../../ui/spinner';
import { AppTooltipDirective } from '../../ui/tooltip';
import {
  BrowserMouseButton,
  BrowserPoint,
  browserKeyText,
  browserModifiers,
  browserMouseButton,
  browserVirtualKeyCode,
  browserWheelDeltas,
  mapBrowserPoint,
} from './canvas-browser-input';
import { BROWSER_MAX_INSERT_TEXT_CHARS, normalizeBrowserAddress } from './canvas-browser-protocol';
import {
  BrowserViewportSize,
  CanvasBrowserConnectionStatus,
  CanvasBrowserController,
} from './canvas-browser.controller';

/** Fit follows the pane; the others are fixed CSS-pixel page sizes. */
export type BrowserViewportMode = 'fit' | 'phone' | 'tablet' | 'desktop';

/**
 * Common device sizes that fit the workspace screen (1920x1200) with the
 * browser's own toolbar. Size only: the page gets no phone user agent or
 * touch events.
 */
export const BROWSER_VIEWPORT_PRESETS: Readonly<
  Record<Exclude<BrowserViewportMode, 'fit'>, BrowserViewportSize>
> = Object.freeze({
  phone: Object.freeze({width: 390, height: 844}),
  tablet: Object.freeze({width: 768, height: 1024}),
  desktop: Object.freeze({width: 1440, height: 900}),
});

const BROWSER_VIEWPORT_MODES: readonly {
  readonly id: BrowserViewportMode;
  readonly icon: string;
  readonly size: BrowserViewportSize | null;
}[] = [
  {id: 'fit', icon: 'fit_screen', size: null},
  {id: 'phone', icon: 'smartphone', size: BROWSER_VIEWPORT_PRESETS.phone},
  {id: 'tablet', icon: 'tablet', size: BROWSER_VIEWPORT_PRESETS.tablet},
  {id: 'desktop', icon: 'desktop_windows', size: BROWSER_VIEWPORT_PRESETS.desktop},
];

/** Quiet time after the last pane resize before the page is resized. */
export const BROWSER_VIEWPORT_DEBOUNCE_MS = 250;
const FALLBACK_FRAME: BrowserViewportSize = {width: 1280, height: 720};

export interface BrowserDisplayBox extends BrowserViewportSize {
  readonly left: number;
  readonly top: number;
}

/**
 * Where the page goes on the stage: one CSS pixel per page pixel, scaled down
 * (never up) when it is larger than the stage, centred on whole pixels when
 * smaller. Without a measured stage it is drawn 1:1 at the origin.
 */
export function fitBrowserFrame(
  frame: BrowserViewportSize,
  stage: BrowserViewportSize | null,
): BrowserDisplayBox {
  if (!stage || stage.width <= 0 || stage.height <= 0) {
    return {width: frame.width, height: frame.height, left: 0, top: 0};
  }
  const scale = Math.min(1, stage.width / frame.width, stage.height / frame.height);
  const width = frame.width * scale;
  const height = frame.height * scale;
  return {
    width,
    height,
    left: Math.max(0, Math.floor((stage.width - width) / 2)),
    top: Math.max(0, Math.floor((stage.height - height) / 2)),
  };
}

function sameSize(a: BrowserViewportSize | null, b: BrowserViewportSize | null): boolean {
  return a === b || (!!a && !!b && a.width === b.width && a.height === b.height);
}

const BROWSER_STATUS_KEYS: Record<CanvasBrowserConnectionStatus, string> = {
  idle: 'canvas.browser.status.connecting',
  connecting: 'canvas.browser.status.connecting',
  ready: 'canvas.browser.status.ready',
  reconnecting: 'canvas.browser.status.reconnecting',
  ended: 'canvas.browser.status.ended',
  viewer_limit: 'canvas.browser.status.viewerLimit',
  unauthorized: 'canvas.browser.status.unauthorized',
  unavailable: 'canvas.browser.status.unavailable',
  error: 'canvas.browser.status.error',
};

export function canvasBrowserStatusKey(
  status: CanvasBrowserConnectionStatus,
  errorCode: string | null = null,
): string {
  if (errorCode === 'shared_browser_disabled') return 'canvas.browser.status.disabled';
  return BROWSER_STATUS_KEYS[status];
}

export function paintBrowserBitmap(canvas: HTMLCanvasElement, bitmap: ImageBitmap | null): void {
  const context = canvas.getContext('2d');
  if (!context) return;
  if (!bitmap) {
    context.clearRect(0, 0, canvas.width, canvas.height);
    return;
  }
  // The decoded image is authoritative for the backing store. Protocol
  // metadata is intentionally absent from this paint path.
  canvas.width = bitmap.width;
  canvas.height = bitmap.height;
  context.clearRect(0, 0, bitmap.width, bitmap.height);
  context.drawImage(bitmap, 0, 0);
}

interface CapturedBrowserPointer {
  readonly id: number;
  readonly button: BrowserMouseButton;
  point: BrowserPoint;
}

interface PendingBrowserMove extends BrowserPoint {
  readonly buttons: number;
  readonly modifiers: number;
}

interface PressedBrowserKey {
  readonly key: string;
  readonly code: string;
  readonly location: number;
  readonly virtualKey: number;
}

/** Trusted view-only chrome and bitmap surface for the shared browser. */
@Component({
  selector: 'app-canvas-browser-renderer',
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [
    AppButtonComponent,
    AppIconComponent,
    AppIconButtonComponent,
    AppSpinnerComponent,
    AppTooltipDirective,
    TranslocoPipe,
  ],
  template: `
    <section class="browser-renderer" [attr.data-connection-status]="browser.connectionStatus()">
      <header class="browser-toolbar" role="toolbar"
              [attr.aria-label]="'canvas.browser.toolbar.label' | transloco">
        <strong class="browser-title" [attr.title]="browser.pageState()?.title || null">
          {{ browser.pageState()?.title || ('canvas.browser.untitled' | transloco) }}
        </strong>
        <form class="browser-navigation" (submit)="navigate($event)">
          <app-icon-button size="sm"
                           [ariaLabel]="'canvas.browser.toolbar.back' | transloco"
                           [tooltip]="'canvas.browser.toolbar.back' | transloco"
                           [disabled]="!canDrive()"
                           (clicked)="goBack()">
            <app-icon size="sm">arrow_back</app-icon>
          </app-icon-button>
          <app-icon-button size="sm"
                           [ariaLabel]="'canvas.browser.toolbar.reload' | transloco"
                           [tooltip]="'canvas.browser.toolbar.reload' | transloco"
                           [disabled]="!canDrive()"
                           (clicked)="reload()">
            <app-icon size="sm">refresh</app-icon>
          </app-icon-button>
          <label class="browser-url-label">
            <span>{{ 'canvas.browser.toolbar.address' | transloco }}</span>
            <input type="text" inputmode="url" autocomplete="off" spellcheck="false"
                   maxlength="8192" [value]="urlValue()"
                   [placeholder]="'canvas.browser.noUrl' | transloco"
                   [attr.title]="urlValue() || null"
                   [disabled]="!canDrive()"
                   (focus)="urlEditing.set(true)"
                   (blur)="urlEditing.set(false)"
                   (input)="onUrlInput($event)" />
          </label>
          @if (browser.viewportControl()) {
            <div class="browser-size" role="group"
                 [attr.aria-label]="'canvas.browser.size.label' | transloco">
              @for (mode of viewportModes; track mode.id) {
                <button type="button" class="browser-size-option"
                        [attr.data-mode]="mode.id"
                        [attr.aria-pressed]="viewportMode() === mode.id"
                        [attr.aria-label]="('canvas.browser.size.' + mode.id) | transloco: (mode.size ?? {})"
                        [appTooltip]="('canvas.browser.size.' + mode.id) | transloco: (mode.size ?? {})"
                        [disabled]="!canDrive()"
                        (click)="setViewportMode(mode.id)">
                  <app-icon size="sm">{{ mode.icon }}</app-icon>
                </button>
              }
            </div>
          }
        </form>
        <div class="browser-indicators">
          @if (browser.pageState()?.loading) {
            <span class="browser-loading">
              <app-spinner size="sm" tone="accent" />
              {{ 'canvas.browser.loading' | transloco }}
            </span>
          }
          @if (browser.connectionStatus() !== 'ready') {
            <span class="browser-connection">
              {{ connectionStatusKey() | transloco }}
            </span>
          }
          @if (batonKey(); as key) {
            <span class="browser-baton" role="status" aria-live="polite"
                  [attr.data-baton]="browser.pageState()?.baton">
              <span>{{ key | transloco }}</span>
              @if (browser.pendingBaton()) {
                <span class="browser-baton-pending">
                  {{ 'canvas.browser.baton.pending' | transloco }}
                </span>
              }
              <button type="button" class="browser-baton-action"
                      [disabled]="batonDisabled()"
                      [attr.aria-label]="batonActionKey() | transloco"
                      [attr.aria-pressed]="browser.pageState()?.baton === 'user'"
                      [attr.aria-busy]="browser.pendingBaton() !== null || null"
                      (click)="toggleBaton()">
                {{ batonActionKey() | transloco }}
              </button>
            </span>
          }
        </div>
      </header>

      @if (navigationError(); as message) {
        <div class="browser-navigation-error" role="alert">
          <app-icon size="sm">warning</app-icon>
          <span>{{ 'canvas.browser.toolbar.navigationRejected' | transloco }} {{ message }}</span>
        </div>
      }

      <div class="browser-stage" #stage>
        <canvas
          #surface
          class="browser-surface"
          tabindex="0"
          [style.width.px]="displayBox().width"
          [style.height.px]="displayBox().height"
          [style.left.px]="displayBox().left"
          [style.top.px]="displayBox().top"
          [attr.aria-label]="'canvas.browser.surfaceLabel' | transloco"
          (pointermove)="onPointerMove($event)"
          (pointerdown)="onPointerDown($event)"
          (pointerup)="onPointerUp($event)"
          (pointercancel)="onPointerCancel($event)"
          (wheel)="onWheel($event)"
          (keydown)="onKeyDown($event)"
          (keyup)="onKeyUp($event)"
          (paste)="onPaste($event)"
          (blur)="releaseHeldInput()"
          (contextmenu)="onContextMenu($event)"
          (compositionstart)="composing = true"
          (compositionend)="composing = false"
        >
        </canvas>
        @if (!browser.frame()) {
          <div class="browser-empty-state"
               [attr.role]="emptyStateRole()"
               aria-live="polite" aria-atomic="true">
            @if (isConnecting() || restartPending()) {
              <app-spinner size="md" tone="accent" />
            }
            <span>{{ connectionStatusKey() | transloco }}</span>
            @if (canRetryConnection() || canRestart()) {
              <div class="browser-lifecycle-actions">
                @if (canRetryConnection()) {
                  <app-button size="sm" variant="secondary"
                              [disabled]="restartPending()" (clicked)="retryConnection()">
                    {{ 'canvas.browser.retryConnection' | transloco }}
                  </app-button>
                }
                @if (canRestart()) {
                  <app-button size="sm" [loading]="restartPending()"
                              (clicked)="restartBrowser()">
                    {{ 'canvas.browser.restart' | transloco }}
                  </app-button>
                }
              </div>
            }
          </div>
        }
      </div>
    </section>
  `,
  styleUrl: './canvas-browser-renderer.component.scss',
})
export class CanvasBrowserRendererComponent implements AfterViewInit, OnDestroy {
  readonly browser = inject(CanvasBrowserController);
  readonly canvas = inject(CanvasService);
  private readonly injector = inject(Injector);
  private readonly transportBridge = inject(PersistentThreadTransportBridge);

  /**
   * Turn-boundary baton automation: when the agent's turn starts the baton
   * returns to the agent; when it completes while this surface is connected
   * the user gets it, ready to drive. sendControl's own guards make both
   * no-ops when the baton is already right or a flip is in flight. The
   * first observation only records the baseline, so opening the pane
   * mid-turn never steals control.
   */
  private lastAgentTurnActive: boolean | null = null;
  private readonly batonAutomation = effect(() => {
    const active = this.transportBridge.agentTurnActive();
    const previous = this.lastAgentTurnActive;
    this.lastAgentTurnActive = active;
    if (previous === null || previous === active) return;
    untracked(() => {
      if (active) {
        this.browser.sendControl({op: 'release_baton'});
      } else if (this.browser.connectionStatus() === 'ready') {
        this.browser.sendControl({op: 'take_baton'});
      }
    });
  });
  private paintEffect: EffectRef | null = null;
  private pointerFrame: number | null = null;
  private pendingPointerMove: PendingBrowserMove | null = null;
  private capturedPointer: CapturedBrowserPointer | null = null;
  private readonly pressedKeys = new Map<string, PressedBrowserKey>();
  @ViewChild('surface', { static: true })
  private surface!: ElementRef<HTMLCanvasElement>;
  @ViewChild('stage', { static: true })
  private stage!: ElementRef<HTMLElement>;
  private resizeObserver: ResizeObserver | null = null;
  private stageSettleTimer: ReturnType<typeof setTimeout> | null = null;

  readonly urlValue = signal('');
  readonly urlEditing = signal(false);
  composing = false;

  readonly viewportModes = BROWSER_VIEWPORT_MODES;
  /** Per pane: Fit follows the pane; a preset asks for a fixed page size. */
  readonly viewportMode = signal<BrowserViewportMode>('fit');
  /** The stage's CSS size, live; null until measured. */
  readonly stageSize = signal<BrowserViewportSize | null>(null, {equal: sameSize});
  /** The stage's size once it has been still for the debounce window. */
  readonly settledStageSize = signal<BrowserViewportSize | null>(null, {equal: sameSize});

  /** CSS size of the page in the current frame: what input maps onto. */
  readonly frameViewport = computed<BrowserViewportSize | null>(() =>
    this.browser.frameSize() ?? this.browser.pageState()?.viewport ?? null,
  );
  readonly displayBox = computed(() =>
    fitBrowserFrame(this.frameViewport() ?? FALLBACK_FRAME, this.stageSize()),
  );
  /** The page size this viewer wants: the pane in Fit, else the preset. */
  readonly requestedViewport = computed<BrowserViewportSize | null>(() => {
    const mode = this.viewportMode();
    if (mode !== 'fit') return BROWSER_VIEWPORT_PRESETS[mode];
    const stage = this.settledStageSize();
    if (!stage) return null;
    const size = {width: Math.floor(stage.width), height: Math.floor(stage.height)};
    return size.width >= 1 && size.height >= 1 ? size : null;
  }, {equal: sameSize});
  readonly connectionStatusKey = computed(() =>
    canvasBrowserStatusKey(this.browser.connectionStatus(), this.browser.errorCode()),
  );
  restartPending(): boolean {
    const status = this.canvas.browserOpenStatus();
    return status === 'workspace' || status === 'browser';
  }

  emptyStateRole(): 'status' | 'alert' {
    const status = this.browser.connectionStatus();
    return status === 'idle' || status === 'connecting' || status === 'reconnecting'
      ? 'status'
      : 'alert';
  }
  readonly batonKey = computed(() => {
    const baton = this.browser.pageState()?.baton;
    return baton === 'agent'
      ? 'canvas.browser.baton.agent'
      : baton === 'user'
        ? 'canvas.browser.baton.user'
        : null;
  });
  readonly isConnecting = computed(() => {
    const status = this.browser.connectionStatus();
    return status === 'connecting' || status === 'reconnecting';
  });
  readonly canDrive = computed(() =>
    this.browser.connectionStatus() === 'ready' &&
    this.browser.pageState()?.baton === 'user' &&
    this.browser.pendingBaton() === null,
  );
  readonly batonDisabled = computed(() =>
    this.browser.connectionStatus() !== 'ready' ||
    this.browser.pageState() === null ||
    this.browser.pendingBaton() !== null,
  );
  readonly batonActionKey = computed(() =>
    this.browser.pageState()?.baton === 'user'
      ? 'canvas.browser.baton.release'
      : 'canvas.browser.baton.take',
  );
  readonly navigationError = computed(() =>
    this.browser.errorCode() === 'navigation_rejected'
      ? this.browser.errorMessage()
      : null,
  );
  canRetryConnection(): boolean {
    const status = this.browser.connectionStatus();
    return status === 'viewer_limit' ||
      (status === 'unavailable' && this.browser.errorCode() !== 'shared_browser_disabled');
  }

  canRestart(): boolean {
    const status = this.browser.connectionStatus();
    return (status === 'ended' || status === 'unavailable') &&
      this.browser.errorCode() !== 'shared_browser_disabled' &&
      this.canvas.browserCapability()?.can_open_browser === true;
  }

  private readonly visibilityListener = (): void => {
    if (typeof document !== 'undefined' && document.visibilityState !== 'visible') {
      this.releaseHeldInput();
    }
  };

  constructor() {
    effect(() => {
      const pageUrl = this.browser.pageState()?.url ?? '';
      if (!this.urlEditing()) this.urlValue.set(pageUrl);
    });
    effect(() => {
      const eligible = this.canDrive();
      if (!eligible) this.releaseHeldInput();
    });
    if (typeof document !== 'undefined') {
      document.addEventListener('visibilitychange', this.visibilityListener);
    }
    // The picture follows the pane: whenever this viewer can drive, the
    // daemon advertises the capability, or the wanted size changes, ask for
    // it. Deliberately not driven by STATE updates, so two viewers of one
    // browser never echo each other's sizes; the last one to resize wins.
    effect(() => {
      const size = this.requestedViewport();
      const canDrive = this.canDrive();
      const supported = this.browser.viewportControl();
      if (!size || !canDrive || !supported) return;
      untracked(() => this.browser.requestViewport(size.width, size.height));
    });
  }

  ngAfterViewInit(): void {
    this.paintEffect = effect(
      () => paintBrowserBitmap(this.surface.nativeElement, this.browser.frame()),
      { injector: this.injector },
    );
    if (typeof ResizeObserver === 'function') {
      this.resizeObserver = new ResizeObserver((entries) => {
        const box = entries[entries.length - 1]?.contentRect;
        if (box) this.onStageResize(box.width, box.height);
      });
      this.resizeObserver.observe(this.stage.nativeElement);
    }
  }

  setViewportMode(mode: BrowserViewportMode): void {
    if (this.canDrive()) this.viewportMode.set(mode);
  }

  /** Draw at once; resize the remote page only once the pane holds still. */
  onStageResize(width: number, height: number): void {
    if (!Number.isFinite(width) || !Number.isFinite(height)) return;
    const size = {width: Math.max(0, width), height: Math.max(0, height)};
    this.stageSize.set(size);
    if (this.settledStageSize() === null) {
      this.settledStageSize.set(size);
      return;
    }
    if (this.stageSettleTimer !== null) clearTimeout(this.stageSettleTimer);
    this.stageSettleTimer = setTimeout(() => {
      this.stageSettleTimer = null;
      this.settledStageSize.set(this.stageSize());
    }, BROWSER_VIEWPORT_DEBOUNCE_MS);
  }

  ngOnDestroy(): void {
    this.resizeObserver?.disconnect();
    this.resizeObserver = null;
    if (this.stageSettleTimer !== null) clearTimeout(this.stageSettleTimer);
    this.stageSettleTimer = null;
    this.releaseHeldInput();
    this.cancelPointerFrame();
    if (typeof document !== 'undefined') {
      document.removeEventListener('visibilitychange', this.visibilityListener);
    }
    this.paintEffect?.destroy();
    this.paintEffect = null;
  }

  onUrlInput(event: Event): void {
    const target = event.target;
    if (target instanceof HTMLInputElement) this.urlValue.set(target.value);
  }

  navigate(event: Event): void {
    event.preventDefault();
    const url = normalizeBrowserAddress(this.urlValue());
    if (!this.canDrive() || !url) return;
    this.browser.sendControl({op: 'navigate', url});
  }

  goBack(): void {
    if (this.canDrive()) this.browser.sendControl({op: 'back'});
  }

  reload(): void {
    if (this.canDrive()) this.browser.sendControl({op: 'reload'});
  }

  toggleBaton(): void {
    if (this.batonDisabled()) return;
    if (this.browser.pageState()?.baton === 'user') {
      this.releaseHeldInput();
      this.browser.sendControl({op: 'release_baton'});
    } else {
      this.browser.sendControl({op: 'take_baton'});
    }
  }

  retryConnection(): void {
    if (!this.canRetryConnection() || this.restartPending()) return;
    this.browser.retry();
  }

  restartBrowser(): void {
    if (!this.canRestart() || this.restartPending()) return;
    this.canvas.openBrowser();
  }

  onPointerMove(event: PointerEvent): void {
    if (!this.canDrive() || event.isPrimary === false) return;
    const point = this.mapPoint(event.clientX, event.clientY);
    if (!point) return;
    if (this.capturedPointer?.id === event.pointerId) this.capturedPointer.point = point;
    this.pendingPointerMove = {
      ...point,
      buttons: event.buttons,
      modifiers: browserModifiers(event),
    };
    if (this.pointerFrame !== null) return;
    this.pointerFrame = this.requestPointerFrame(() => {
      this.pointerFrame = null;
      this.flushPointerMove();
    });
  }

  onPointerDown(event: PointerEvent): void {
    if (!this.canDrive() || event.isPrimary === false) return;
    const button = browserMouseButton(event.button);
    const point = this.mapPoint(event.clientX, event.clientY);
    if (!button || !point || (this.capturedPointer && this.capturedPointer.id !== event.pointerId)) {
      return;
    }
    this.surface.nativeElement.focus({preventScroll: true});
    const sent = this.browser.sendInput({
      kind: 'mouse',
      params: {
        type: 'mousePressed',
        ...point,
        button,
        buttons: event.buttons,
        modifiers: browserModifiers(event),
        clickCount: Math.max(1, Math.min(3, Math.trunc(event.detail) || 1)),
      },
    });
    if (!sent) return;
    this.capturedPointer = {id: event.pointerId, button, point};
    try {
      this.surface.nativeElement.setPointerCapture?.(event.pointerId);
    } catch {
      // Capture may fail if the pointer ended between dispatch and this call.
    }
    event.preventDefault();
  }

  onPointerUp(event: PointerEvent): void {
    const captured = this.capturedPointer;
    if (!captured || captured.id !== event.pointerId) return;
    this.flushPointerMove();
    const point = this.mapPoint(event.clientX, event.clientY) ?? captured.point;
    if (this.canDrive()) {
      this.browser.sendInput({
        kind: 'mouse',
        params: {
          type: 'mouseReleased',
          ...point,
          button: captured.button,
          buttons: event.buttons,
          modifiers: browserModifiers(event),
          clickCount: Math.max(1, Math.min(3, Math.trunc(event.detail) || 1)),
        },
      });
      event.preventDefault();
    }
    this.clearCapturedPointer();
  }

  onPointerCancel(event: PointerEvent): void {
    const captured = this.capturedPointer;
    if (!captured || captured.id !== event.pointerId) return;
    this.flushPointerMove();
    if (this.canDrive()) {
      this.browser.sendInput({
        kind: 'mouse',
        params: {
          type: 'mouseReleased',
          ...captured.point,
          button: captured.button,
          buttons: 0,
          modifiers: browserModifiers(event),
          clickCount: 1,
        },
      });
    }
    this.clearCapturedPointer();
  }

  onWheel(event: WheelEvent): void {
    if (!this.canDrive() || !this.surfaceFocused()) return;
    const viewport = this.frameViewport();
    const bounds = this.surface.nativeElement.getBoundingClientRect();
    const point = viewport
      ? mapBrowserPoint(event.clientX, event.clientY, bounds, viewport)
      : null;
    const deltas = viewport
      ? browserWheelDeltas(event.deltaX, event.deltaY, event.deltaMode, bounds, viewport)
      : null;
    if (!point || !deltas) return;
    if (this.browser.sendInput({
      kind: 'wheel',
      params: {...point, ...deltas, modifiers: browserModifiers(event)},
    })) {
      event.preventDefault();
    }
  }

  onKeyDown(event: KeyboardEvent): void {
    if (
      !this.canDrive() ||
      !this.surfaceFocused() ||
      this.composing ||
      event.isComposing
    ) {
      return;
    }
    // Paste stays a local concern: the user's clipboard lives in THIS
    // browser, so the chord becomes a remote text insertion instead of a
    // forwarded Ctrl+V (which would paste the workspace Chromium's own,
    // usually empty, clipboard).
    if ((event.ctrlKey || event.metaKey) && !event.altKey && event.key.toLowerCase() === 'v') {
      event.preventDefault();
      this.pasteFromClipboard();
      return;
    }
    const text = browserKeyText(event);
    const virtualKey = browserVirtualKeyCode(event);
    const params = {
      type: 'keyDown',
      key: event.key,
      code: event.code,
      location: event.location,
      autoRepeat: event.repeat,
      modifiers: browserModifiers(event),
      ...(virtualKey ? {windowsVirtualKeyCode: virtualKey, nativeVirtualKeyCode: virtualKey} : {}),
      ...(text ? {text} : {}),
    };
    if (!this.browser.sendInput({kind: 'key', params})) return;
    this.pressedKeys.set(this.keyIdentity(event), {
      key: event.key,
      code: event.code,
      location: event.location,
      virtualKey,
    });
    event.preventDefault();
  }

  onKeyUp(event: KeyboardEvent): void {
    const identity = this.keyIdentity(event);
    const pressed = this.pressedKeys.get(identity);
    if (!pressed) return;
    this.pressedKeys.delete(identity);
    if (
      this.canDrive() &&
      this.surfaceFocused() &&
      !this.composing &&
      !event.isComposing &&
      this.browser.sendInput({
        kind: 'key',
        params: {
          type: 'keyUp',
          key: pressed.key,
          code: pressed.code,
          location: pressed.location,
          modifiers: browserModifiers(event),
          ...(pressed.virtualKey
            ? {windowsVirtualKeyCode: pressed.virtualKey, nativeVirtualKeyCode: pressed.virtualKey}
            : {}),
        },
      })
    ) {
      event.preventDefault();
    }
  }

  onContextMenu(event: MouseEvent): void {
    if (this.canDrive() && this.surfaceFocused()) event.preventDefault();
  }

  /** Browser-menu Edit→Paste path; the keyboard chord never reaches here. */
  onPaste(event: ClipboardEvent): void {
    if (!this.canDrive() || !this.surfaceFocused()) return;
    const text = event.clipboardData?.getData('text/plain') ?? '';
    if (this.insertRemoteText(text)) event.preventDefault();
  }

  private pasteFromClipboard(): void {
    const clipboard = navigator.clipboard;
    if (!clipboard?.readText) return;
    clipboard.readText().then(
      text => {
        this.insertRemoteText(text);
      },
      () => {
        // Clipboard permission denied — pasting is simply unavailable.
      },
    );
  }

  private insertRemoteText(text: string): boolean {
    const bounded = text.slice(0, BROWSER_MAX_INSERT_TEXT_CHARS);
    if (!bounded) return false;
    return this.browser.sendInput({kind: 'insertText', params: {text: bounded}});
  }

  /** Best-effort release for blur, baton/lifecycle changes, and destruction. */
  releaseHeldInput(): void {
    this.cancelPointerFrame();
    this.pendingPointerMove = null;
    const captured = this.capturedPointer;
    if (captured) {
      this.browser.sendInput({
        kind: 'mouse',
        params: {
          type: 'mouseReleased',
          ...captured.point,
          button: captured.button,
          buttons: 0,
          modifiers: 0,
          clickCount: 1,
        },
      });
      this.clearCapturedPointer();
    }
    const pressed = [...this.pressedKeys.values()];
    this.pressedKeys.clear();
    for (const key of pressed) {
      this.browser.sendInput({
        kind: 'key',
        params: {
          type: 'keyUp',
          key: key.key,
          code: key.code,
          location: key.location,
          modifiers: 0,
          ...(key.virtualKey
            ? {windowsVirtualKeyCode: key.virtualKey, nativeVirtualKeyCode: key.virtualKey}
            : {}),
        },
      });
    }
  }

  private mapPoint(clientX: number, clientY: number): BrowserPoint | null {
    const viewport = this.frameViewport();
    return viewport
      ? mapBrowserPoint(
          clientX,
          clientY,
          this.surface.nativeElement.getBoundingClientRect(),
          viewport,
        )
      : null;
  }

  private flushPointerMove(): void {
    this.cancelPointerFrame();
    const move = this.pendingPointerMove;
    this.pendingPointerMove = null;
    if (!move || !this.canDrive()) return;
    this.browser.sendInput({
      kind: 'mouse',
      params: {
        type: 'mouseMoved',
        x: move.x,
        y: move.y,
        buttons: move.buttons,
        modifiers: move.modifiers,
      },
    });
  }

  private clearCapturedPointer(): void {
    const captured = this.capturedPointer;
    this.capturedPointer = null;
    if (!captured) return;
    try {
      this.surface.nativeElement.releasePointerCapture?.(captured.id);
    } catch {
      // Capture may already have been released by the browser.
    }
  }

  private requestPointerFrame(callback: () => void): number {
    if (typeof requestAnimationFrame === 'function') return requestAnimationFrame(callback);
    return setTimeout(callback, 16) as unknown as number;
  }

  private cancelPointerFrame(): void {
    if (this.pointerFrame === null) return;
    if (typeof cancelAnimationFrame === 'function') cancelAnimationFrame(this.pointerFrame);
    else clearTimeout(this.pointerFrame);
    this.pointerFrame = null;
  }

  private surfaceFocused(): boolean {
    return typeof document === 'undefined' || document.activeElement === this.surface.nativeElement;
  }

  private keyIdentity(event: Pick<KeyboardEvent, 'code' | 'key' | 'location'>): string {
    return `${event.code || event.key}:${event.location}`;
  }
}
