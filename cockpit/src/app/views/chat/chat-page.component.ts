import {
    Component,
    DestroyRef,
    ElementRef,
    computed,
    effect,
    inject,
    OnDestroy,
    OnInit,
    signal,
    viewChild,
} from '@angular/core';
import {takeUntilDestroyed} from '@angular/core/rxjs-interop';
import {ActivatedRoute, Router} from '@angular/router';
import {TranslocoPipe, TranslocoService} from '@jsverse/transloco';
import {AngularSplitModule, SplitGutterInteractionEvent} from 'angular-split';
import {distinctUntilChanged, finalize, forkJoin, map, of} from 'rxjs';
import {PersistentChatComponent} from '../../views/persistent-chat/persistent-chat.component';
import {PersistentChatService} from '../../core/services/persistent-chat.service';
import {AppToastService} from '../../ui/toast';
import {ErrorMessageService} from '../../core/services/error-message.service';
import {CanvasService} from '../../core/services/canvas.service';
import {CanvasState} from '../../core/models/canvas.model';
import {ViewportService} from '../../core/services/viewport.service';
import {AppIconComponent} from '../../ui/icon';
import {AppIconButtonComponent} from '../../ui/icon-button';
import {AppButtonComponent} from '../../ui/button';
import {AppDialogComponent} from '../../ui/dialog';
import {CanvasPaneComponent} from '../canvas/canvas-pane.component';
import {canvasSourceKey} from '../canvas/canvas-rendering';
import {normalizeWorkspacePath} from '../../core/markdown/link-extension';
import {SettingsPaneComponent} from './settings-pane.component';
import {ConfigDriftDialogComponent} from './config-drift-dialog.component';
import {ApiService} from '../../core/services/api.service';
import {PersistentThreadMessage, Thread} from '../../core/models/api.model';
import {SubagentTranscriptComponent} from './subagent-transcript.component';

export interface BrowserReplacementTarget {
    readonly threadId: string;
    readonly presentationRevision: number;
    readonly sourceKey: string;
}

/** Child threads have no event journal; refresh their durable rows at a bounded rate. */
const SUBAGENT_REFRESH_MS = 5_000;

/** Keep a replacement confirmation scoped to the presentation the user saw. */
export function browserReplacementTargetMatches(
    target: BrowserReplacementTarget,
    threadId: string | null,
    state: CanvasState | null,
): boolean {
    return threadId === target.threadId &&
        state?.status !== 'cleared' &&
        state?.source?.type !== 'browser' &&
        state?.presentation_revision === target.presentationRevision &&
        canvasSourceKey(state) === target.sourceKey;
}

@Component({
    selector: 'app-chat-page',
    standalone: true,
    imports: [
        AngularSplitModule,
        TranslocoPipe,
        PersistentChatComponent,
        CanvasPaneComponent,
        SettingsPaneComponent,
        AppIconComponent,
        AppIconButtonComponent,
        AppButtonComponent,
        AppDialogComponent,
        ConfigDriftDialogComponent,
        SubagentTranscriptComponent,
    ],
    template: `
      @if (subagentThread(); as thread) {
        <app-subagent-transcript
          [thread]="thread"
          [messages]="subagentMessages()"
          [loading]="subagentLoading()"
          [error]="subagentError()"
          (refresh)="refreshSubagentTranscript()"
        />
      } @else {
      @if (canvasAvailable()) {
        <button type="button" class="canvas-skip-link" (click)="openCanvas(true)">
          {{ 'canvas.skipToCanvas' | transloco }}
        </button>
      }
      <as-split #split direction="horizontal" unit="percent"
                [gutterSize]="rightAreaVisible() && !viewport.isMobile() ? 8 : 0"
                [gutterStep]="2" [disabled]="viewport.isMobile() || !rightPaneOpen()"
                [restrictMove]="true" [useTransition]="false"
                [gutterAriaLabel]="'canvas.resize' | transloco"
                (dragEnd)="onSplitDragEnd($event)"
                (gutterDblClick)="closeRightPane()">
        <as-split-area [size]="chatAreaSize()" [minSize]="viewport.isMobile() ? 100 : 25"
                       [maxSize]="viewport.isMobile() ? 100 : 75"
                       [visible]="chatAreaVisible()">
          <div id="chat-panel" class="chat-panel"
               [attr.inert]="chatAreaHidden() ? '' : null"
               [attr.aria-hidden]="chatAreaHidden() ? 'true' : null">
            <app-persistent-chat (canvasRequested)="openCanvas(true)"
                                 (settingsRequested)="openSettings($event)"
                                 (workspaceFileRequested)="openWorkspaceFile($event)">
              @if (browserActionVisible()) {
                <span chatHeaderAction class="canvas-toggle-wrap">
                  <app-icon-button size="sm"
                                   [ariaLabel]="'canvas.browser.open.action' | transloco"
                                   [tooltip]="browserActionTooltipKey() | transloco"
                                   [disabled]="browserActionDisabled()"
                                   [loading]="browserActionLoading()"
                                   (clicked)="openSharedBrowser()">
                    <app-icon size="sm">web</app-icon>
                  </app-icon-button>
                </span>
              }
              @if (canvasAvailable()) {
                <span chatHeaderAction #canvasToggle class="canvas-toggle-wrap">
                  <app-icon-button size="sm"
                                   [ariaLabel]="(canvasContentVisible() ? 'canvas.hide' : 'canvas.open') | transloco"
                                   [tooltip]="(canvasContentVisible() ? 'canvas.hide' : 'canvas.open') | transloco"
                                   (clicked)="canvasContentVisible() && !viewport.isMobile() ? closeCanvas() : openCanvas(true)">
                    <app-icon size="sm">dashboard_customize</app-icon>
                  </app-icon-button>
                  @if (canvasPending()) {
                    <span class="canvas-pending-dot"
                          [attr.aria-label]="'canvas.newContent' | transloco"></span>
                  }
                </span>
              }
            </app-persistent-chat>
          </div>
        </as-split-area>

        <as-split-area [size]="rightAreaSize()" [minSize]="viewport.isMobile() ? 100 : 25"
                       [maxSize]="viewport.isMobile() ? 100 : 75"
                       [visible]="rightAreaVisible()">
          <!-- Canvas stays mounted while settings covers it — an @if would
               destroy the renderer (iframe state, unsaved edits). -->
          <div id="canvas-panel" class="canvas-panel"
               [style.display]="settingsOpen() ? 'none' : ''"
               [attr.inert]="canvasAreaHidden() ? '' : null"
               [attr.aria-hidden]="canvasAreaHidden() ? 'true' : null">
            <app-canvas-pane #canvasPane [active]="canvasContentVisible()" [mobile]="viewport.isMobile()"
                             (closeRequested)="closeCanvas()"
                             (returnToChat)="returnToChat()"
                             (dirtyChange)="canvasDirty.set($event)" />
          </div>
          @if (settingsOpen()) {
            <div id="settings-panel" class="canvas-panel">
              <app-settings-pane #settingsPane [active]="settingsOpen()"
                                 (closeRequested)="closeSettings()" />
            </div>
          }
        </as-split-area>
      </as-split>

      <app-dialog
        [open]="browserReplacementTarget() !== null"
        (closed)="cancelSharedBrowserReplacement()"
        [title]="'canvas.browser.open.replace.title' | transloco"
        size="sm">
        <p>{{ 'canvas.browser.open.replace.body' | transloco }}</p>
        <ng-container appDialogActions>
          <app-button variant="ghost" (clicked)="cancelSharedBrowserReplacement()">
            {{ 'common.cancel' | transloco }}
          </app-button>
          <app-button variant="warning" (clicked)="confirmSharedBrowserReplacement()">
            {{ 'canvas.browser.open.replace.confirm' | transloco }}
          </app-button>
        </ng-container>
      </app-dialog>

      @if (chat.pendingDrift(); as drift) {
        <app-config-drift-dialog
          [items]="drift"
          (resumeAnyway)="onResumeAnyway($event)"
          (startNew)="onStartNewSession()"
          (dismissed)="chat.pendingDrift.set(null)"
        />
      }
      }
    `,
    styles: `
      :host {
        position: relative;
        display: block;
        width: 100%;
        height: 100%;
        min-width: 0;
        overflow: hidden;
      }

      as-split {
        --as-gutter-background-color: var(--border-color);
        --as-gutter-disabled-cursor: default;

        width: 100%;
        height: 100%;
      }

      .chat-panel,
      .canvas-panel,
      app-persistent-chat,
      app-canvas-pane {
        display: block;
        width: 100%;
        height: 100%;
        min-width: 0;
      }

      .canvas-panel { border-left: 1px solid var(--border-color); }

      app-settings-pane {
        display: block;
        width: 100%;
        height: 100%;
        min-width: 0;
      }

      .canvas-toggle-wrap {
        position: relative;
        display: inline-flex;
      }

      .canvas-pending-dot {
        position: absolute;
        top: 0;
        right: 0;
        width: 8px;
        height: 8px;
        border-radius: 50%;
        background: var(--accent-color);
        pointer-events: none;
      }

      .canvas-skip-link {
        position: absolute;
        z-index: 100;
        top: 8px;
        left: 50%;
        min-height: 32px;
        padding: 5px 10px;
        color: var(--text-primary);
        background: var(--panel-bg);
        border: 1px solid var(--accent-color);
        border-radius: var(--radius-control);
        opacity: 0;
        pointer-events: none;
        transform: translate(-50%, -150%);
      }

      .canvas-skip-link:focus-visible {
        opacity: 1;
        pointer-events: auto;
        transform: translate(-50%, 0);
      }

      @media (width <= 768px) {
        .canvas-panel { border-left: 0; }
      }

      @media (prefers-reduced-motion: reduce) {
        .canvas-skip-link { transition: none; }
      }
    `,
})
export class ChatPageComponent implements OnInit, OnDestroy {
    private readonly route = inject(ActivatedRoute);
    private readonly router = inject(Router);
    // Not private: the template reads chat.pendingDrift() directly for the
    // config-drift dialog, matching how persistent-chat.component.ts exposes
    // the same service.
    readonly chat = inject(PersistentChatService);
    private readonly toast = inject(AppToastService);
    private readonly errors = inject(ErrorMessageService);
    private readonly transloco = inject(TranslocoService);
    private readonly api = inject(ApiService);
    private readonly canvas = inject(CanvasService);
    readonly viewport = inject(ViewportService);
    private readonly destroyRef = inject(DestroyRef);
    private routeGeneration = 0;
    private readonly routeThreadId = signal<string | null>(null);
    private subagentRefreshInterval: ReturnType<typeof setInterval> | null = null;
    /** The route generation whose child detail/history pair is currently in flight. */
    private subagentRefreshInFlightGeneration: number | null = null;
    readonly subagentThread = signal<Thread | null>(null);
    readonly subagentMessages = signal<PersistentThreadMessage[]>([]);
    readonly subagentLoading = signal(false);
    readonly subagentError = signal(false);
    readonly canvasOpen = signal(false);
    readonly canvasFocus = signal(false);
    readonly canvasDirty = signal(false);
    /** Settings pane shown in the right split area (content-switched with the
     *  canvas — settings wins while open; the canvas stays mounted behind it). */
    readonly settingsOpen = signal(false);
    /** Mobile-only: settings takes the full screen (canvasFocus analogue). */
    readonly settingsFocus = signal(false);
    /** Exact non-browser presentation awaiting destructive replacement approval. */
    readonly browserReplacementTarget = signal<BrowserReplacementTarget | null>(null);
    /** A new canvas source arrived while settings held the pane — badge the
     *  canvas toggle instead of stealing (live_session_settings.md Slice A). */
    readonly canvasPending = signal(false);
    readonly chatPercent = signal(56);
    private previousCanvasThread: string | null = null;
    private previousCanvasSource: string | null = null;
    private canvasOpener: HTMLElement | null = null;
    private settingsOpener: HTMLElement | null = null;

    private readonly pane = viewChild<CanvasPaneComponent>('canvasPane');
    private readonly settingsPane = viewChild<SettingsPaneComponent>('settingsPane');
    private readonly split = viewChild<unknown, ElementRef<HTMLElement>>('split', {read: ElementRef});
    private readonly toggle = viewChild<unknown, ElementRef<HTMLElement>>('canvasToggle', {read: ElementRef});

    readonly canvasAvailable = computed(() => {
        const state = this.canvas.state();
        return this.canvasDirty() ||
            this.canvas.browserCapability()?.feature_enabled === true ||
            (!!state?.source && state.status !== 'cleared');
    });
    readonly browserActionVisible = computed(
        () => this.canvas.browserCapability()?.feature_enabled === true,
    );
    /** The workspace file the Canvas is presenting, in the one spelling an
     *  agent-written Markdown path normalizes to. Null for every other kind
     *  of source (live app, shared browser) and for a cleared Canvas. */
    readonly canvasFilePath = computed(() => {
        const state = this.canvas.state();
        if (!state || state.status === 'cleared') return null;
        const source = state.source;
        if (!source || source.type !== 'workspace_file') return null;
        const path = source['path'];
        return typeof path === 'string' ? normalizeWorkspacePath(path) : null;
    });
    readonly browserActionLoading = computed(() => {
        const status = this.canvas.browserOpenStatus();
        return status === 'workspace' || status === 'browser';
    });
    readonly browserActionDisabled = computed(() =>
        this.canvasDirty() || this.canvas.browserCapability()?.can_open_browser !== true,
    );
    readonly browserActionTooltipKey = computed(() => {
        if (this.canvasDirty()) return 'canvas.browser.open.dirty';
        const capability = this.canvas.browserCapability();
        if (!capability?.can_open_browser && capability?.reason) {
            return `canvas.browser.reason.${capability.reason}`;
        }
        return 'canvas.browser.open.action';
    });
    readonly chatAreaVisible = computed(
        () => !this.viewport.isMobile() || (!this.canvasFocus() && !this.settingsFocus()),
    );
    /** Right split area shows either settings (priority) or the canvas. */
    readonly rightPaneOpen = computed(() => this.settingsOpen() || this.canvasOpen());
    readonly settingsVisible = computed(
        () => this.settingsOpen() && (!this.viewport.isMobile() || this.settingsFocus()),
    );
    readonly canvasContentVisible = computed(
        () => this.canvasOpen() && !this.settingsOpen()
            && (!this.viewport.isMobile() || this.canvasFocus()),
    );
    readonly rightAreaVisible = computed(
        () => this.settingsVisible() || this.canvasContentVisible(),
    );
    readonly chatAreaHidden = computed(
        () => this.viewport.isMobile() && (this.canvasFocus() || this.settingsFocus()),
    );
    readonly canvasAreaHidden = computed(() => !this.canvasContentVisible());
    readonly chatAreaSize = computed(() => {
        if (this.viewport.isMobile() || !this.rightAreaVisible()) return 100;
        return this.chatPercent();
    });
    readonly rightAreaSize = computed(() => {
        if (this.viewport.isMobile()) return 100;
        return this.rightAreaVisible() ? 100 - this.chatPercent() : 0;
    });

    /** Instant-landing draft chat at `/` (route data, not a URL param). */
    private readonly isDraftRoute = this.route.snapshot.data['draft'] === true;

    constructor() {
        // A viewed ending thread can disappear after a permanent Delete
        // settles in the background. The service reports only an exact 404;
        // route identity keeps a late observation from moving another view.
        effect(() => {
            const deleted = this.chat.deletedThreadId();
            if (deleted && deleted === this.routeThreadId() && deleted === this.chat.threadId()) {
                void this.router.navigate(['/sessions']);
            }
        });

        // Draft flow: when the first send creates the thread
        // (_createFromDraftSession → createAndConnect sets threadId), move the
        // URL from / to the session. No replaceUrl — Back returns to a fresh
        // draft. The destination ChatPage instance skips reconnecting via the
        // ngOnInit same-thread guard below.
        effect(() => {
            const id = this.chat.threadId();
            if (this.isDraftRoute && id) {
                void this.router.navigate(['/sessions', id]);
            }
        });

        // New logical sources open automatically. A same-source republish only
        // refreshes the mounted renderer, so it never steals focus or reopens a
        // pane the user explicitly closed.
        effect(() => {
            const threadId = this.canvas.threadId();
            const state = this.canvas.state();
            const source = canvasSourceKey(state);
            const dirty = this.canvasDirty();
            const browserHostable = this.canvas.browserCapability()?.feature_enabled === true;
            if (threadId !== this.previousCanvasThread) {
                this.previousCanvasThread = threadId;
                this.previousCanvasSource = null;
                this.canvasOpen.set(false);
                this.canvasFocus.set(false);
                this.canvasDirty.set(false);
                this.settingsOpen.set(false);
                this.settingsFocus.set(false);
                this.canvasPending.set(false);
            }
            if (state?.status === 'cleared') {
                if (dirty) {
                    this.configureSplitterAccessibility();
                    return;
                }
                const restoreFocus = this.canvasOpen() && this.shouldRestoreFocusFromCanvas();
                this.previousCanvasSource = null;
                this.canvasOpen.set(false);
                this.canvasFocus.set(false);
                if (restoreFocus) queueMicrotask(() => this.focusCanvasOpener());
            } else if (!source) {
                this.previousCanvasSource = null;
                if (!browserHostable) {
                    const restoreFocus = this.canvasOpen() && this.shouldRestoreFocusFromCanvas();
                    this.canvasOpen.set(false);
                    this.canvasFocus.set(false);
                    if (restoreFocus) queueMicrotask(() => this.focusCanvasOpener());
                }
            } else if (source !== this.previousCanvasSource) {
                this.previousCanvasSource = source;
                this.canvasOpen.set(true);
                // Desktop can reveal a sibling without disturbing the active
                // chat control. On mobile, making Canvas full-screen would put
                // that focused control inside a newly inert subtree. Keep the
                // new stage mounted/announced and let the user enter it through
                // the trusted toggle, tool card, or skip link.
                this.canvasFocus.set(false);
                // Whenever the push isn't actually on screen — settings holds
                // the pane, or mobile is keeping the new stage behind the
                // toggle — badge the toggle rather than letting new content
                // arrive with no signal at all.
                if (this.settingsOpen() || this.viewport.isMobile()) {
                    this.canvasPending.set(true);
                }
            }
            this.configureSplitterAccessibility();
        });

        // A confirmation must never authorize replacement of a presentation
        // that arrived after the user clicked Open browser.
        effect(() => {
            const target = this.browserReplacementTarget();
            if (
                target &&
                (!browserReplacementTargetMatches(
                    target,
                    this.canvas.threadId(),
                    this.canvas.state(),
                ) || this.browserActionDisabled())
            ) {
                this.browserReplacementTarget.set(null);
            }
        });
    }

    /** Open the settings pane (header action / status chips). Takes the right
     *  split area over from the canvas; the canvas stays mounted behind it. */
    openSettings(section?: string): void {
        if (typeof document !== 'undefined') {
            this.settingsOpener = document.activeElement instanceof HTMLElement
                ? document.activeElement
                : null;
        }
        this.settingsOpen.set(true);
        if (this.viewport.isMobile()) this.settingsFocus.set(true);
        this.configureSplitterAccessibility();
        if (section === 'model') {
            queueMicrotask(() => this.settingsPane()?.scrollToModel());
        }
    }

    closeSettings(): void {
        this.settingsOpen.set(false);
        this.settingsFocus.set(false);
        // The canvas is visible again — its pending push is delivered.
        if (this.canvasOpen()) this.canvasPending.set(false);
        if (this.settingsOpener?.isConnected) {
            const opener = this.settingsOpener;
            queueMicrotask(() => opener.focus({preventScroll: true}));
        }
        this.settingsOpener = null;
    }

    /** Gutter double-click closes whichever content owns the pane. */
    closeRightPane(): void {
        if (this.settingsOpen()) this.closeSettings();
        else this.closeCanvas();
    }

    openCanvas(focus = false): void {
        if (!this.canvasAvailable()) return;
        if (focus && typeof document !== 'undefined') {
            this.canvasOpener = document.activeElement instanceof HTMLElement
                ? document.activeElement
                : null;
        }
        // Explicitly choosing the canvas reclaims the pane from settings.
        this.settingsOpen.set(false);
        this.settingsFocus.set(false);
        this.canvasPending.set(false);
        this.canvasOpen.set(true);
        if (this.viewport.isMobile()) this.canvasFocus.set(true);
        this.configureSplitterAccessibility();
        if (focus) queueMicrotask(() => this.pane()?.focusContent());
    }

    /**
     * A workspace path written into agent prose was activated.
     *
     * The Canvas is the agent's stage — the Cockpit cannot present an
     * arbitrary file onto it — so this brings the Canvas forward exactly when
     * it already holds that file, which is what the link promised the reader.
     * Any other path gets named rather than silently swallowed.
     */
    openWorkspaceFile(path: string): void {
        const requested = normalizeWorkspacePath(path);
        if (!requested) return;
        if (requested === this.canvasFilePath()) {
            this.openCanvas(true);
            return;
        }
        this.toast.info(
            this.transloco.translate('canvas.workspaceFile.notPresented', {path: requested}),
        );
    }

    openSharedBrowser(): void {
        if (this.browserActionDisabled()) return;
        const state = this.canvas.state();
        const threadId = this.canvas.threadId();
        const sourceKey = canvasSourceKey(state);
        if (
            threadId &&
            state &&
            sourceKey &&
            state.status !== 'cleared' &&
            state.source?.type !== 'browser'
        ) {
            this.browserReplacementTarget.set({
                threadId,
                presentationRevision: state.presentation_revision,
                sourceKey,
            });
            return;
        }
        this.startSharedBrowser(state?.presentation_revision);
    }

    cancelSharedBrowserReplacement(): void {
        this.browserReplacementTarget.set(null);
    }

    confirmSharedBrowserReplacement(): void {
        const target = this.browserReplacementTarget();
        this.browserReplacementTarget.set(null);
        if (
            !target ||
            this.browserActionDisabled() ||
            !browserReplacementTargetMatches(
                target,
                this.canvas.threadId(),
                this.canvas.state(),
            )
        ) return;
        this.startSharedBrowser(target.presentationRevision);
    }

    /** "Resume without them": the acknowledged ids re-drive POST /resume,
     *  which either succeeds (drifted config is dropped) or 428s again with
     *  whatever still disagrees. Clearing pendingDrift up front — rather than
     *  waiting on the response — is what unmounts the dialog immediately, so
     *  there is nothing left in the DOM to double-click. */
    async onResumeAnyway(ids: string[]): Promise<void> {
        this.chat.pendingDrift.set(null);
        await this.chat.resumeSession(ids);
    }

    /** "Start a new session": leaves this thread ended and hands off to
     *  session-create with a single `from=<threadId>` query param (§8.3).
     *  session-create fetches that thread itself and prefills project/expert/
     *  model/connectors from whatever of its config still resolves —
     *  deliberately NOT a list of surviving ids in the URL, so a connector
     *  that drifts between this click and the create page loading is still
     *  dropped correctly. */
    onStartNewSession(): void {
        this.chat.pendingDrift.set(null);
        const threadId = this.chat.threadId();
        void this.router.navigate(
            ['/sessions/new'],
            threadId ? {queryParams: {from: threadId}} : undefined,
        );
    }

    private startSharedBrowser(expectedPresentationRevision?: number): void {
        this.openCanvas(true);
        this.canvas.openBrowser(undefined, expectedPresentationRevision);
    }

    returnToChat(): void {
        this.canvasFocus.set(false);
        queueMicrotask(() => this.focusCanvasOpener());
    }

    closeCanvas(): void {
        const restoreFocus = this.shouldRestoreFocusFromCanvas();
        this.canvasOpen.set(false);
        this.canvasFocus.set(false);
        if (restoreFocus) queueMicrotask(() => this.focusCanvasOpener());
    }

    onSplitDragEnd(event: SplitGutterInteractionEvent): void {
        const chatSize = event.sizes[0];
        if (typeof chatSize === 'number') {
            this.chatPercent.set(Math.max(25, Math.min(75, chatSize)));
        }
        this.configureSplitterAccessibility();
    }

    private configureSplitterAccessibility(): void {
        queueMicrotask(() => {
            const gutter = this.split()?.nativeElement.querySelector<HTMLElement>('[role="separator"]');
            if (gutter) {
                gutter.setAttribute('aria-controls', 'chat-panel canvas-panel');
                gutter.setAttribute('aria-orientation', 'vertical');
            }
        });
    }

    private focusCanvasOpener(): void {
        if (this.canvasOpener?.isConnected) {
            this.canvasOpener.focus({preventScroll: true});
            this.canvasOpener = null;
            return;
        }
        const fallback =
            this.toggle()?.nativeElement.querySelector<HTMLElement>('button') ??
            this.split()?.nativeElement.querySelector<HTMLElement>(
                '#chat-panel textarea:not([disabled]), #chat-panel button:not([disabled])',
            );
        fallback?.focus({preventScroll: true});
        this.canvasOpener = null;
    }

    private shouldRestoreFocusFromCanvas(): boolean {
        const activeElement = typeof document === 'undefined' ? null : document.activeElement;
        const focusWasInCanvas =
            activeElement instanceof Element && activeElement.closest('#canvas-panel') !== null;
        const focusWasOnSplitter =
            activeElement instanceof Element && activeElement.getAttribute('role') === 'separator';
        return this.viewport.isMobile() ||
            this.canvasFocus() ||
            focusWasInCanvas ||
            focusWasOnSplitter ||
            this.canvasOpener !== null;
    }

    ngOnInit(): void {
        if (this.isDraftRoute) {
            this.canvas.selectThread(null);
            this.chat.enterDraftSession();
            return;
        }

        // Angular reuses this component for /sessions/:threadId → another
        // /sessions/:threadId navigation. Observe params for the component's
        // whole lifetime so both chat and Canvas switch together.
        this.route.paramMap.pipe(
            map(params => params.get('threadId')),
            distinctUntilChanged(),
            takeUntilDestroyed(this.destroyRef),
        ).subscribe(threadId => this.handleThreadRoute(threadId));
    }

    private handleThreadRoute(threadId: string | null): void {
        const routeGeneration = ++this.routeGeneration;
        this.routeThreadId.set(threadId);
        this.stopSubagentRefresh();
        // A stale request from the previous route is still subscribed until it
        // settles, but its generation guard cannot paint this route. Releasing
        // the latch lets the new route perform its own initial read.
        this.subagentRefreshInFlightGeneration = null;
        this.subagentThread.set(null);
        this.subagentMessages.set([]);
        this.subagentLoading.set(false);
        this.subagentError.set(false);

        if (threadId === '_creating') {
            this.canvas.selectThread(null);
            // LEGACY bridge — nothing in the app navigates here any more. Both
            // create forms now POST first and route to the real thread id, so a
            // rejected config leaves the form standing instead of unmounting
            // into this view and bouncing back to /sessions with a toast. Kept
            // for history entries and service-worker-cached bundles from before
            // that change: they still arrive with `createBody` in state.
            const state = history.state as { createBody?: Record<string, any> };
            if (state?.createBody) {
                this.chat.createAndConnect(state.createBody).then(
                    id => {
                        if (routeGeneration !== this.routeGeneration) return false;
                        this.canvas.selectThread(id);
                        return this.router.navigate(['/sessions', id], {replaceUrl: true});
                    },
                    err => {
                        if (routeGeneration !== this.routeGeneration) return;
                        this.toast.danger(this.errors.translate(err, 'errors.sessions.createFailed'));
                        void this.router.navigate(['/sessions']);
                    }
                );
            } else {
                void this.router.navigate(['/sessions']);
            }
        } else if (threadId) {
            // Identify a child before touching any session transport. A child
            // runs inside its parent job's pod; attaching it as a session would
            // provision/connect a runtime that does not exist.
            this.api.getPersistentThread(threadId)
                .pipe(takeUntilDestroyed(this.destroyRef))
                .subscribe(row => {
                    if (routeGeneration !== this.routeGeneration) return;
                    // A failed read (older orchestrator, transient error) is
                    // not a verdict on the thread: take the ordinary session
                    // path exactly as before the preflight existed.
                    const thread = row ? (row as unknown as Thread) : null;
                    if (thread?.kind === 'subagent') {
                        this.canvas.selectThread(null);
                        this.subagentThread.set(thread);
                        this.loadSubagentTranscript(thread.id, routeGeneration, {
                            refreshThread: false,
                            showLoading: true,
                        });
                        this.startSubagentRefresh(thread.id, routeGeneration);
                        return;
                    }

                    // Canvas state reconciles independently from chat history
                    // and may remain available while transport is offline.
                    this.canvas.selectThread(threadId);
                    // Already connected, ready, or mid-start on this thread?
                    // Don't reconnect. The mid-start case is the draft flow
                    // landing here right after createAndConnect.
                    //
                    // `sessionReady` is in the test because the other two miss
                    // a real window: `/connection` can resolve ready before the
                    // EventSource has opened, so `isStartingSession` is already
                    // false (it is gated on `sessionReady`) while
                    // `connectionState` is still 'connecting'. Reconnecting
                    // there tears down a live session, wipes the transcript,
                    // and drops readiness back to false — the startup card
                    // returns over a session that has already answered. See
                    // knowledge-base/knowledge/issues/session_start_panel_never_yields_to_a_completed_first_turn.md
                    if (
                        this.chat.threadId() === threadId &&
                        (this.chat.isConnected() ||
                            this.chat.sessionReady() ||
                            this.chat.isStartingSession())
                    ) return;

                    void this.chat.connect(threadId);
                });
        } else {
            this.canvas.selectThread(null);
            void this.router.navigate(['/sessions']);
        }
    }

    refreshSubagentTranscript(): void {
        const thread = this.subagentThread();
        if (thread) {
            this.loadSubagentTranscript(thread.id, this.routeGeneration, {
                refreshThread: true,
                showLoading: this.subagentMessages().length === 0,
            });
        }
    }

    private loadSubagentTranscript(
        threadId: string,
        routeGeneration: number,
        options: {refreshThread: boolean; showLoading: boolean},
    ): void {
        // A slow REST response must not build a queue of identical reads. The
        // next 5s tick will catch up after this pair settles.
        if (this.subagentRefreshInFlightGeneration === routeGeneration) return;
        this.subagentRefreshInFlightGeneration = routeGeneration;
        if (options.showLoading) {
            this.subagentLoading.set(true);
            this.subagentError.set(false);
        }

        const current = this.subagentThread();
        const thread$ = options.refreshThread
            ? this.api.getPersistentThread(threadId)
            : of(current as unknown as Record<string, unknown> | null);

        forkJoin({
            row: thread$,
            history: this.api.getPersistentThreadHistory(threadId),
        })
            .pipe(
                finalize(() => {
                    // A previous route's late finalizer must not release the
                    // current route's in-flight latch.
                    if (this.subagentRefreshInFlightGeneration === routeGeneration) {
                        this.subagentRefreshInFlightGeneration = null;
                    }
                }),
                takeUntilDestroyed(this.destroyRef),
            )
            .subscribe(({row, history}) => {
                if (
                    routeGeneration !== this.routeGeneration ||
                    this.subagentThread()?.id !== threadId
                ) return;

                const refreshed = row ? (row as unknown as Thread) : null;
                if (refreshed?.kind === 'subagent' && refreshed.id === threadId) {
                    this.subagentThread.set(refreshed);
                    if (!this.isLiveSubagent(refreshed)) this.stopSubagentRefresh();
                }

                this.subagentLoading.set(false);
                if (history) {
                    this.subagentError.set(false);
                    this.subagentMessages.set(history.messages ?? []);
                } else if (this.subagentMessages().length === 0) {
                    // A transient poll failure does not replace an already-read
                    // transcript with an error screen.
                    this.subagentError.set(true);
                }
            });
    }

    private startSubagentRefresh(threadId: string, routeGeneration: number): void {
        this.stopSubagentRefresh();
        const thread = this.subagentThread();
        if (!thread || !this.isLiveSubagent(thread)) return;
        this.subagentRefreshInterval = setInterval(() => {
            const current = this.subagentThread();
            if (
                routeGeneration !== this.routeGeneration ||
                current?.id !== threadId ||
                !this.isLiveSubagent(current)
            ) {
                this.stopSubagentRefresh();
                return;
            }
            if (
                typeof document !== 'undefined' &&
                document.visibilityState !== 'visible'
            ) return;
            this.loadSubagentTranscript(threadId, routeGeneration, {
                refreshThread: true,
                showLoading: false,
            });
        }, SUBAGENT_REFRESH_MS);
    }

    private stopSubagentRefresh(): void {
        if (this.subagentRefreshInterval !== null) {
            clearInterval(this.subagentRefreshInterval);
            this.subagentRefreshInterval = null;
        }
    }

    private isLiveSubagent(thread: Thread): boolean {
        return thread.subagent_status === 'queued' || thread.subagent_status === 'running';
    }

    ngOnDestroy(): void {
        this.routeGeneration++;
        this.stopSubagentRefresh();
        this.subagentRefreshInFlightGeneration = null;
        // Don't disconnect — keep session alive across navigation
    }
}
