import {
  ChangeDetectionStrategy,
  Component,
  DestroyRef,
  ElementRef,
  computed,
  effect,
  inject,
  input,
  output,
  signal,
  viewChild,
} from '@angular/core';
import {DatePipe} from '@angular/common';
import {takeUntilDestroyed} from '@angular/core/rxjs-interop';
import {Router} from '@angular/router';
import {TranslocoPipe, TranslocoService} from '@jsverse/transloco';
import {CanvasService} from '../../core/services/canvas.service';
import {WorkspaceFileDownloadService} from '../../core/services/workspace-file-download.service';
import {AppBadgeComponent} from '../../ui/badge';
import {AppButtonComponent} from '../../ui/button';
import {AppIconComponent} from '../../ui/icon';
import {AppIconButtonComponent} from '../../ui/icon-button';
import {AppSpinnerComponent} from '../../ui/spinner';
import {
  CanvasHtmlRendererComponent,
  CanvasImageRendererComponent,
  CanvasInteractiveHtmlRendererComponent,
  CanvasMarkdownRendererComponent,
  CanvasTextRendererComponent,
} from './canvas-renderers.component';
import {
  canvasSourceKey,
  declaredCanvasRenderer,
  selectCanvasChromeState,
  selectCanvasRenderer,
} from './canvas-rendering';
import {CanvasContentController} from './canvas-content.controller';
import {CanvasAwarenessController} from './canvas-awareness.controller';
import {CanvasEditController} from './canvas-edit.controller';
import {CanvasEditorComponent} from './canvas-editor.component';
import {CanvasViewerController} from './canvas-viewer.controller';
import {CanvasBrowserController} from './canvas-browser.controller';
import {
  CanvasBrowserRendererComponent,
  canvasBrowserStatusKey,
} from './canvas-browser-renderer.component';
import {CanvasLiveAppRendererComponent} from './canvas-live-app-renderer.component';
import {CanvasOfficeController} from './canvas-office.controller';
import {CanvasOfficeRendererComponent} from './canvas-office-renderer.component';
import {
  CanvasLiveAppUnavailableComponent,
  canvasLiveAppFailureKind,
} from './canvas-live-app-unavailable.component';

export type CanvasResetStatus = 'idle' | 'confirming' | 'resetting' | 'success' | 'error';

export interface CanvasResetTarget {
  readonly stateEtag: string;
  readonly presentationRevision: number;
  readonly sourceKey: string;
}

export function canvasResetTargetMatches(
  target: CanvasResetTarget,
  stateEtag: string | null,
  presentationRevision: number | null,
  sourceKey: string | null,
): boolean {
  return target.stateEtag === stateEtag &&
    target.presentationRevision === presentationRevision &&
    target.sourceKey === sourceKey;
}

/** Keep untrusted Canvas content in a wrapper and sever opener/referrer state. */
export function openCanvasPopOut(
  url: string,
  openWindow: (url: string, target: string, features: string) => unknown,
): void {
  openWindow(url, '_blank', 'noopener,noreferrer');
}

export function browserOpenErrorKey(code: string | null): string {
  if (code === 'browser_open_timeout') return 'canvas.browser.open.error.timeout';
  if (code === 'invalid_browser_open_response' || code === 'invalid_browser_capability') {
    return 'canvas.browser.open.error.invalidResponse';
  }
  if (
    code === 'feature_disabled' ||
    code === 'workspace_required' ||
    code === 'workspace_unattested' ||
    code === 'workspace_unroutable' ||
    code === 'transport_unavailable'
  ) {
    return `canvas.browser.reason.${code}`;
  }
  return 'canvas.browser.open.error.failed';
}

@Component({
  selector: 'app-canvas-pane',
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  providers: [
    CanvasAwarenessController,
    CanvasBrowserController,
    CanvasContentController,
    CanvasEditController,
    CanvasOfficeController,
    CanvasViewerController,
  ],
  imports: [
    DatePipe,
    TranslocoPipe,
    AppBadgeComponent,
    AppButtonComponent,
    AppIconComponent,
    AppIconButtonComponent,
    AppSpinnerComponent,
    CanvasBrowserRendererComponent,
    CanvasHtmlRendererComponent,
    CanvasImageRendererComponent,
    CanvasInteractiveHtmlRendererComponent,
    CanvasLiveAppRendererComponent,
    CanvasLiveAppUnavailableComponent,
    CanvasOfficeRendererComponent,
    CanvasMarkdownRendererComponent,
    CanvasTextRendererComponent,
    CanvasEditorComponent,
  ],
  template: `
    <section class="canvas-shell" [attr.aria-label]="'canvas.paneLabel' | transloco">
      <header class="canvas-header">
        <div class="canvas-heading">
          @if (mobile()) {
            <app-icon-button #returnButton size="sm"
                             [ariaLabel]="'canvas.returnToChat' | transloco"
                             [tooltip]="'canvas.returnToChat' | transloco"
                             (clicked)="returnToChat.emit()">
              <app-icon size="sm">arrow_back</app-icon>
            </app-icon-button>
          }
          <app-icon class="canvas-mark" size="md">dashboard_customize</app-icon>
          <div class="canvas-title-group">
            <h2>{{ title() }}</h2>
            <div class="canvas-source" [title]="sourceSummary()">{{ sourceSummary() }}</div>
          </div>
        </div>
        <div class="canvas-trust">
          <app-badge tone="warning" size="xs">{{ 'canvas.untrusted' | transloco }}</app-badge>
          <app-badge tone="neutral" size="xs">{{ sourceKindLabel() }}</app-badge>
          <span>{{ rendererLabel() }}</span>
          @if (snapshotBacked()) {
            <app-badge tone="neutral" size="xs">{{ 'canvas.offline.badge' | transloco }}</app-badge>
          } @else if (lastSyncedAt(); as syncedAt) {
            <time [attr.datetime]="syncedAt | date:'yyyy-MM-ddTHH:mm:ssXXX'"
                  [title]="syncedAt | date:'medium'">
              {{ 'canvas.lastSynced' | transloco }} {{ syncedAt | date:'short' }}
            </time>
          }
        </div>
        <div class="canvas-actions">
          @if (editor.hasSession()) {
            <app-icon-button size="sm"
                             [ariaLabel]="(editor.editMode() ? 'canvas.editor.preview' : 'canvas.editor.edit') | transloco"
                             [tooltip]="(editor.editMode() ? 'canvas.editor.preview' : 'canvas.editor.edit') | transloco"
                             [disabled]="!editor.editMode() && !editor.canEnterEdit()"
                             (clicked)="toggleEditor()">
              <app-icon size="sm">{{ editor.editMode() ? 'preview' : 'edit' }}</app-icon>
            </app-icon-button>
          }
          @if (editor.dirty()) {
            <app-button size="sm" [loading]="editor.saveStatus() === 'saving'"
                        [disabled]="!editor.canSave()" (clicked)="editor.save()">
              {{ (editor.saveStatus() === 'saving' ? 'canvas.editor.saving' : 'canvas.editor.save') | transloco }}
            </app-button>
          }
          <app-icon-button size="sm" [ariaLabel]="'canvas.refresh' | transloco"
                           [tooltip]="'canvas.refresh' | transloco"
                           [loading]="canvas.loadStatus() === 'loading'"
                           (clicked)="canvas.reconcile()">
            <app-icon size="sm">refresh</app-icon>
          </app-icon-button>
          @if (downloadPath()) {
            <app-icon-button size="sm" [ariaLabel]="'canvas.download' | transloco"
                             [tooltip]="'canvas.download' | transloco"
                             [loading]="downloading()"
                             (clicked)="downloadFile()">
              <app-icon size="sm">download</app-icon>
            </app-icon-button>
          }
          @if (isLiveAppSource()) {
            <app-icon-button size="sm"
                             [ariaLabel]="'canvas.app.reset.action' | transloco"
                             [tooltip]="'canvas.app.reset.action' | transloco"
                             [loading]="resetStatus() === 'resetting'"
                             [disabled]="!canResetOrigin()"
                             (clicked)="requestOriginReset()">
              <app-icon size="sm">delete_sweep</app-icon>
            </app-icon-button>
          }
          @if (canPopOut()) {
            <app-icon-button size="sm" [ariaLabel]="'canvas.popOut' | transloco"
                             [tooltip]="'canvas.popOut' | transloco"
                             (clicked)="popOutCanvas()">
              <app-icon size="sm">open_in_new</app-icon>
            </app-icon-button>
          }
          <app-icon-button size="sm" [ariaLabel]="'canvas.close' | transloco"
                           [tooltip]="'canvas.close' | transloco"
                           (clicked)="closeRequested.emit()">
            <app-icon size="sm">close</app-icon>
          </app-icon-button>
        </div>
      </header>

      <div class="canvas-announcement" aria-live="polite" aria-atomic="true">
        {{ statusText() }}
      </div>

      @if (snapshotBacked()) {
        <div class="canvas-offline-notice" role="status">
          <app-icon size="sm">bedtime</app-icon>
          <span>
            @if (snapshotCapturedAt(); as capturedAt) {
              {{ 'canvas.offline.body' | transloco:{date: (capturedAt | date:'medium')} }}
            } @else {
              {{ 'canvas.offline.bodyUndated' | transloco }}
            }
          </span>
        </div>
      }

      @if (staticHtmlPreview()) {
        <div class="canvas-offline-notice" role="note">
          <app-icon size="sm">code_off</app-icon>
          <span>{{ 'canvas.staticHtmlNotice' | transloco }}</span>
        </div>
      }

      @if (resetStatus() !== 'idle') {
        <div class="canvas-reset-notice"
             [class.canvas-reset-notice--error]="resetStatus() === 'error'"
             [class.canvas-reset-notice--success]="resetStatus() === 'success'"
             [attr.role]="resetStatus() === 'confirming' || resetStatus() === 'error' ? 'alert' : 'status'"
             aria-labelledby="canvas-reset-title" aria-describedby="canvas-reset-body">
          <app-icon size="sm">
            {{ resetStatus() === 'error' ? 'error' : resetStatus() === 'success' ? 'check_circle' : 'delete_sweep' }}
          </app-icon>
          <div class="canvas-reset-notice__copy">
            <strong id="canvas-reset-title">
              {{ ('canvas.app.reset.' + resetStatus() + '.title') | transloco }}
            </strong>
            <span id="canvas-reset-body">
              {{ ('canvas.app.reset.' + resetStatus() + '.body') | transloco }}
            </span>
          </div>
          <div class="canvas-reset-notice__actions">
            @if (resetStatus() === 'confirming') {
              <app-button size="sm" variant="ghost" (clicked)="cancelOriginReset()">
                {{ 'common.cancel' | transloco }}
              </app-button>
              <app-button size="sm" variant="danger" (clicked)="confirmOriginReset()">
                {{ 'canvas.app.reset.confirm' | transloco }}
              </app-button>
            } @else if (resetStatus() !== 'resetting') {
              <app-button size="sm" variant="ghost" (clicked)="dismissOriginResetNotice()">
                {{ 'common.dismiss' | transloco }}
              </app-button>
            }
          </div>
        </div>
      }

      @if (editor.conflict(); as conflict) {
        <div class="canvas-edit-notice" role="alert">
          <app-icon size="sm">warning</app-icon>
          <div class="canvas-edit-notice__copy">
            <strong>{{ ('canvas.editor.conflict.' + conflict + '.title') | transloco }}</strong>
            <span>{{ ('canvas.editor.conflict.' + conflict + '.body') | transloco }}</span>
          </div>
          <div class="canvas-edit-notice__actions">
            <app-button size="sm" variant="ghost" (clicked)="editor.keepEditing()">
              {{ 'canvas.editor.keepEditing' | transloco }}
            </app-button>
            @if (isReloadConflict()) {
              <app-button size="sm" variant="warning" [loading]="editor.refreshPending()"
                          (clicked)="editor.loadCurrentVersion()">
                {{ reloadLabel() }}
              </app-button>
            }
          </div>
        </div>
      }
      @if (office.conflictCode()) {
        <div class="canvas-edit-notice" role="alert">
          <app-icon size="sm">warning</app-icon>
          <div class="canvas-edit-notice__copy">
            <strong>{{ 'canvas.office.conflict.title' | transloco }}</strong>
            <span>{{ 'canvas.office.conflict.body' | transloco }}</span>
          </div>
          <div class="canvas-edit-notice__actions">
            <app-button size="sm" variant="warning" (clicked)="office.reloadSession()">
              {{ 'canvas.office.conflict.reload' | transloco }}
            </app-button>
          </div>
        </div>
      }
      @if (editor.remoteEditing()) {
        <div class="canvas-remote-editing" role="status">
          <app-icon size="sm">edit_note</app-icon>
          <span>{{ 'canvas.editor.remoteEditing' | transloco }}</span>
        </div>
      }
      @if (sourceNeedsRefresh() && !editor.conflict()) {
        <div class="canvas-source-refresh" role="alert">
          <app-icon size="sm">sync_problem</app-icon>
          <span>{{ 'canvas.editor.sourceRefresh' | transloco }}</span>
          <app-button size="sm" variant="warning" [loading]="editor.refreshPending()"
                      (clicked)="editor.refreshPresentedSource()">
            {{ 'canvas.editor.loadWorkspace' | transloco }}
          </app-button>
        </div>
      }

      <div #contentViewport id="canvas-content" class="canvas-content" tabindex="-1">
        @if (editor.editorMounted() && editor.hasSession()) {
          @if (editor.sessionPath(); as editPath) {
            <app-canvas-editor #canvasEditor [path]="editPath" [value]="editor.buffer()"
                               [class.canvas-editor--hidden]="!editor.editMode()"
                               [disabled]="editor.saveStatus() === 'saving'"
                               (valueChange)="editor.updateBuffer($event)"
                               (editorFocus)="editor.editorFocused()"
                               (editorBlur)="editor.editorBlurred()" />
          }
        }
        @if (!editor.editMode()) {
          @if (liveAppFailureVisible()) {
            <app-canvas-live-app-unavailable [errorCode]="liveAppErrorCode()"
                                             (retry)="viewer.retry()" />
          } @else if (hasVisual()) {
            @switch (effectiveRenderer()) {
              @case ('markdown') {
                <app-canvas-markdown-renderer [content]="previewContent()" />
              }
              @case ('text') {
                <app-canvas-text-renderer [content]="previewContent()" />
              }
              @case ('html') {
                <app-canvas-html-renderer [content]="previewContent()" [title]="frameTitle()" />
              }
              @case ('html-interactive') {
                <app-canvas-interactive-html-renderer
                  [content]="previewContent()"
                  [title]="interactiveFrameTitle()" />
              }
              @case ('image') {
                <app-canvas-image-renderer [src]="imageUrl()!" [alt]="imageAlt()"
                                           [sourceKey]="displaySourceKey()!" />
              }
              @case ('app') {
                <app-canvas-live-app-renderer [src]="viewer.frameUrl()!"
                                              [title]="liveAppFrameTitle()"
                                              [warning]="'canvas.app.untrustedWarning' | transloco"
                                              (frameBound)="viewer.bindFrame($event)"
                                              (frameUnbound)="viewer.unbindFrame($event)"
                                              (frameMessage)="viewer.handleFrameMessage($event)" />
              }
              @case ('office') {
                <app-canvas-office-renderer
                  [session]="office.session()!"
                  [officeOrigin]="office.officeOrigin()!"
                  [title]="officeFrameTitle()"
                  [editable]="state()?.editable === true"
                  [presentationRevision]="state()?.presentation_revision ?? 0"
                  [refreshToken]="office.refreshToken"
                  [reloadSession]="office.reloadSession"
                  (documentLoaded)="office.markDocumentLoaded()"
                  (modifiedChange)="office.markModified($event)"
                  (conflict)="office.markConflict($event)" />
              }
              @case ('browser') {
                <app-canvas-browser-renderer />
              }
            }
            @if (showOverlay()) {
              <div class="canvas-overlay" role="status">
                @if (contentStatus() === 'loading' || canvas.loadStatus() === 'loading' ||
                     viewer.viewerStatus() === 'loading' || viewer.viewerStatus() === 'renewing' ||
                     office.officeStatus() === 'loading') {
                  <app-spinner size="sm" tone="accent" />
                } @else {
                  <app-icon size="sm">info</app-icon>
                }
                <span>{{ statusText() }}</span>
              </div>
            }
          } @else {
            <div class="canvas-placeholder">
              @if (browserEmptyState()) {
                @if (browserOpenPending()) {
                  <app-spinner size="md" tone="accent" />
                } @else {
                  <app-icon size="xl">web</app-icon>
                }
                <p [attr.role]="canvas.browserOpenStatus() === 'error' ? 'alert' : null">
                  {{ browserEmptyTextKey() | transloco }}
                </p>
                <div class="canvas-browser-actions">
                  @if (canvas.browserOpenStatus() === 'error') {
                    <app-button size="sm" [disabled]="browserOpenDisabled()"
                                (clicked)="canvas.retryOpenBrowser()">
                      {{ 'canvas.browser.open.retry' | transloco }}
                    </app-button>
                  } @else if (!browserOpenPending()) {
                    <app-button size="sm" [disabled]="browserOpenDisabled()"
                                (clicked)="canvas.openBrowser()">
                      {{ 'canvas.browser.open.action' | transloco }}
                    </app-button>
                  }
                </div>
              } @else {
                @if (contentStatus() === 'loading' || canvas.loadStatus() === 'loading' ||
                     viewer.viewerStatus() === 'loading' || viewer.viewerStatus() === 'renewing' ||
                     office.officeStatus() === 'loading') {
                  <app-spinner size="md" tone="accent" />
                } @else {
                  <app-icon size="xl">preview</app-icon>
                }
                <p>{{ emptyText() }}</p>
              }
            </div>
          }
        }
      </div>
    </section>
  `,
  styleUrl: './canvas-pane.component.scss',
})
export class CanvasPaneComponent {
  readonly active = input(true);
  readonly mobile = input(false);
  readonly popout = input(false);
  readonly closeRequested = output<void>();
  readonly returnToChat = output<void>();
  readonly dirtyChange = output<boolean>();

  readonly canvas = inject(CanvasService);
  private readonly downloads = inject(WorkspaceFileDownloadService);
  private readonly content = inject(CanvasContentController);
  readonly editor = inject(CanvasEditController);
  readonly viewer = inject(CanvasViewerController);
  readonly office = inject(CanvasOfficeController);
  readonly browser = inject(CanvasBrowserController);
  private readonly transloco = inject(TranslocoService);
  private readonly router = inject(Router);
  private readonly destroyRef = inject(DestroyRef);
  private readonly contentViewport = viewChild<ElementRef<HTMLElement>>('contentViewport');
  private readonly canvasEditor = viewChild<CanvasEditorComponent>('canvasEditor');

  readonly displayRenderer = this.content.displayRenderer;
  readonly displayState = this.content.displayState;
  readonly displaySourceKey = this.content.displaySourceKey;
  readonly textContent = this.content.textContent;
  readonly contentEtag = this.content.contentEtag;
  readonly imageUrl = this.content.imageUrl;
  readonly contentStatus = this.content.contentStatus;
  readonly contentErrorCode = this.content.contentErrorCode;
  readonly resetStatus = signal<CanvasResetStatus>('idle');
  readonly resetTarget = signal<CanvasResetTarget | null>(null);

  readonly state = this.canvas.state;
  readonly chromeState = computed(() =>
    selectCanvasChromeState(
      this.state(),
      this.editor.sessionState(),
      this.editor.hasSession() && !!(this.editor.dirty() || this.editor.conflict()),
    ),
  );
  readonly preservingEditSession = computed(() =>
    this.editor.hasSession() && !!(this.editor.dirty() || this.editor.conflict()),
  );
  readonly effectiveRenderer = computed(() => {
    if (this.editor.hasSession() && (this.editor.dirty() || this.editor.conflict())) {
      return this.editor.sessionRenderer();
    }
    const selected = selectCanvasRenderer(this.state());
    return selected === 'app' || selected === 'office' || selected === 'browser'
      ? selected
      : this.displayRenderer();
  });
  readonly previewContent = computed(() =>
    this.editor.hasSession() && this.editor.dirty()
      ? this.editor.buffer()
      : this.textContent(),
  );
  readonly hasVisual = computed(() => {
    if (this.editor.editMode() && this.editor.hasSession()) return true;
    if (this.effectiveRenderer() === 'app') return this.viewer.frameUrl() !== null;
    if (this.effectiveRenderer() === 'office') return this.office.session() !== null;
    if (this.effectiveRenderer() === 'browser') return true;
    if (this.effectiveRenderer() === 'image') return this.imageUrl() !== null;
    return this.effectiveRenderer() !== 'unsupported' &&
      (this.contentStatus() !== 'idle' || this.editor.hasSession());
  });
  readonly title = computed(() =>
    this.chromeState()?.title?.trim() || this.transloco.translate('canvas.defaultTitle'),
  );
  readonly sourceSummary = computed(() => {
    const source = this.chromeState()?.source;
    if (source?.type === 'workspace_file' && typeof source.path === 'string') return source.path;
    if (source?.type === 'workspace_app') {
      return typeof source.entry_path === 'string' && source.entry_path
        ? source.entry_path
        : this.transloco.translate('canvas.app.source');
    }
    if (source?.type === 'browser') {
      return this.transloco.translate('canvas.browser.source');
    }
    return this.transloco.translate('canvas.noSource');
  });
  /**
   * Label the source for what it is, not for what happens to be mounted.
   *
   * `effectiveRenderer()` drives what the template renders and correctly falls
   * back to `'unsupported'` when there is nothing to draw. Reusing it as the
   * chip label made a sleeping workspace claim the document was unsupported.
   * An active edit session still wins, because then the mounted renderer IS
   * what the user is working in.
   */
  readonly rendererLabel = computed(() => {
    const editing = this.editor.hasSession() && (this.editor.dirty() || this.editor.conflict());
    const declared = declaredCanvasRenderer(this.state());
    const renderer = editing || declared === 'unsupported'
      ? this.effectiveRenderer()
      : declared;
    return this.transloco.translate(`canvas.renderer.${renderer}`);
  });
  readonly snapshotBacked = computed(() => this.state()?.content_origin === 'snapshot');
  readonly snapshotCapturedAt = computed(() => this.state()?.content_captured_at ?? null);
  readonly isLiveAppSource = computed(() => this.chromeState()?.source?.type === 'workspace_app');
  /** The presented workspace file, which the toolbar offers to download. */
  readonly downloadPath = computed(() => {
    const source = this.chromeState()?.source;
    return source?.type === 'workspace_file' && typeof source.path === 'string'
      ? source.path
      : null;
  });
  readonly downloading = signal(false);
  /** The static preview strips scripts and styles; say so, so a page that
   *  looks broken isn't mistaken for a broken file. */
  readonly staticHtmlPreview = computed(() =>
    declaredCanvasRenderer(this.state()) === 'html' && !this.editor.editMode(),
  );
  readonly sourceKindLabel = computed(() => {
    const type = this.chromeState()?.source?.type;
    const kind = type === 'workspace_file'
      ? 'file'
      : type === 'workspace_app'
        ? 'app'
        : type === 'browser'
          ? 'browser'
          : 'unknown';
    return this.transloco.translate(`canvas.sourceKind.${kind}`);
  });
  readonly lastSyncedAt = computed(() =>
    this.preservingEditSession() ? null : this.canvas.lastSuccessfulSyncAt(),
  );
  readonly canPopOut = computed(() =>
    !this.popout() &&
    !this.preservingEditSession() &&
    !!this.canvas.threadId() &&
    this.state()?.capabilities.can_pop_out === true,
  );
  readonly canResetOrigin = computed(() =>
    this.isLiveAppSource() &&
    !!this.canvas.stateEtag() &&
    this.canvas.loadStatus() !== 'loading' &&
    this.resetStatus() !== 'resetting',
  );
  readonly liveAppErrorCode = computed(() => {
    if (!this.isLiveAppSource()) return null;
    if (
      this.state()?.status === 'ready' &&
      this.state()?.capabilities.can_create_viewer_session !== true
    ) {
      return 'canvas_viewer_not_configured';
    }
    return this.viewer.viewerStatus() === 'error' ? this.viewer.viewerErrorCode() : null;
  });
  readonly liveAppFailureVisible = computed(() => this.liveAppErrorCode() !== null);
  readonly imageAlt = computed(() =>
    this.displayState()?.alt_text?.trim() || this.transloco.translate('canvas.image.missingAlt'),
  );
  readonly frameTitle = computed(() =>
    this.transloco.translate('canvas.html.frameTitle', {title: this.title()}),
  );
  readonly interactiveFrameTitle = computed(() =>
    this.transloco.translate('canvas.html.interactiveFrameTitle', {title: this.title()}),
  );
  readonly liveAppFrameTitle = computed(() =>
    this.transloco.translate('canvas.app.frameTitle', {title: this.title()}),
  );
  readonly officeFrameTitle = computed(() =>
    this.transloco.translate('canvas.office.frameTitle', {title: this.title()}),
  );
  readonly sourceNeedsRefresh = computed(() =>
    this.state()?.status === 'source_changed' || this.contentStatus() === 'source_changed',
  );
  readonly browserEmptyState = computed(() =>
    this.canvas.browserCapability()?.feature_enabled === true &&
    !this.state()?.source &&
    this.state()?.status !== 'cleared',
  );
  readonly browserOpenPending = computed(() => {
    const status = this.canvas.browserOpenStatus();
    return status === 'workspace' || status === 'browser';
  });
  readonly browserOpenDisabled = computed(() =>
    this.editor.dirty() ||
    this.canvas.loadStatus() === 'loading' ||
    this.canvas.browserCapability()?.can_open_browser !== true,
  );
  readonly browserEmptyTextKey = computed(() => {
    const status = this.canvas.browserOpenStatus();
    if (status === 'workspace') return 'canvas.browser.open.phase.workspace';
    if (status === 'browser') return 'canvas.browser.open.phase.browser';
    if (status === 'error') return browserOpenErrorKey(this.canvas.browserOpenError());
    if (this.editor.dirty()) return 'canvas.browser.open.dirty';
    const capability = this.canvas.browserCapability();
    if (!capability?.can_open_browser && capability?.reason) {
      return `canvas.browser.reason.${capability.reason}`;
    }
    return 'canvas.browser.open.explanation';
  });
  readonly showOverlay = computed(() => {
    const state = this.state();
    return (
      this.hasVisual() &&
      !this.editor.editMode() &&
      (this.contentStatus() === 'loading' ||
        this.contentStatus() === 'source_changed' ||
        this.viewer.viewerStatus() === 'loading' ||
        this.viewer.viewerStatus() === 'renewing' ||
        this.viewer.viewerStatus() === 'error' ||
        this.office.officeStatus() === 'loading' ||
        this.office.officeStatus() === 'error' ||
        this.canvas.loadStatus() === 'loading' ||
        state?.status === 'source_changed' ||
        state?.status === 'unavailable' ||
        state?.status === 'error' ||
        state?.status === 'ended')
    );
  });
  readonly statusText = computed(() => {
    const state = this.state();
    if (this.browserEmptyState()) return this.transloco.translate(this.browserEmptyTextKey());
    if (this.effectiveRenderer() === 'browser') {
      return this.transloco.translate(
        canvasBrowserStatusKey(this.browser.connectionStatus(), this.browser.errorCode()),
      );
    }
    if (this.liveAppFailureVisible()) {
      return this.transloco.translate(
        `canvas.app.failure.${canvasLiveAppFailureKind(this.liveAppErrorCode())}.title`,
      );
    }
    if (this.resetStatus() === 'success' || this.resetStatus() === 'error') {
      return this.transloco.translate(`canvas.app.reset.${this.resetStatus()}.title`);
    }
    if (state?.status === 'source_changed' || this.contentStatus() === 'source_changed') {
      return this.transloco.translate('canvas.status.sourceChanged');
    }
    if (this.editor.saveStatus() === 'saved') {
      return this.transloco.translate('canvas.editor.saved');
    }
    if (this.editor.dirty() || this.office.modified()) {
      return this.transloco.translate('canvas.editor.unsaved');
    }
    if (this.contentStatus() === 'loading') return this.transloco.translate('canvas.status.loading');
    if (this.viewer.viewerStatus() === 'loading') {
      return this.transloco.translate('canvas.app.connecting');
    }
    if (this.viewer.viewerStatus() === 'renewing') {
      return this.transloco.translate('canvas.app.renewing');
    }
    if (this.viewer.viewerStatus() === 'error') {
      return this.transloco.translate('canvas.app.unavailable');
    }
    if (this.effectiveRenderer() === 'office' && this.office.officeStatus() === 'loading') {
      return this.transloco.translate('canvas.office.connecting');
    }
    if (this.effectiveRenderer() === 'office' && this.office.officeStatus() === 'error') {
      return this.transloco.translate('canvas.office.unavailable');
    }
    if (this.canvas.loadStatus() === 'loading') {
      return this.transloco.translate(this.hasVisual() ? 'canvas.status.updating' : 'canvas.status.loading');
    }
    if (this.contentStatus() === 'error') return this.transloco.translate('canvas.status.contentError');
    if (state?.status && state.status !== 'ready') {
      return this.transloco.translate(`canvas.status.${state.status}`);
    }
    if (this.hasVisual()) {
      return this.transloco.translate('canvas.status.readyRevision', {
        revision: state?.presentation_revision ?? 0,
      });
    }
    return '';
  });
  readonly emptyText = computed(() => {
    if (this.isLiveAppSource() && this.state()?.status !== 'ready') {
      return this.statusText() || this.transloco.translate('canvas.app.unavailable');
    }
    if (this.effectiveRenderer() === 'office' && this.office.officeStatus() !== 'ready') {
      return this.statusText() || this.transloco.translate('canvas.office.unavailable');
    }
    if (selectCanvasRenderer(this.state()) === 'unsupported') {
      return this.transloco.translate('canvas.unsupported');
    }
    return this.statusText() || this.transloco.translate('canvas.empty');
  });

  constructor() {
    effect(() => this.content.syncPresentation(
      this.active() && this.effectiveRenderer() !== 'browser',
      this.canvas.threadId(),
      this.state(),
      () => this.contentViewport()?.nativeElement.scrollTop ?? 0,
      scrollTop => {
        const viewport = this.contentViewport()?.nativeElement;
        if (viewport) viewport.scrollTop = scrollTop;
      },
    ));
    effect(() => this.editor.sync(
      this.active() && this.effectiveRenderer() !== 'browser',
      this.canvas.threadId(),
      this.state(),
      this.displayState(),
      this.textContent(),
      this.contentEtag(),
      this.contentStatus() === 'ready',
      this.contentStatus() === 'source_changed',
      this.popout(),
    ));
    effect(() => this.viewer.syncPresentation(
      this.active() && this.effectiveRenderer() === 'app',
      this.canvas.threadId(),
      this.state(),
      this.canvas.stateEtag(),
    ));
    effect(() => this.office.syncPresentation(
      this.active() && this.effectiveRenderer() === 'office',
      this.canvas.threadId(),
      this.state(),
      this.canvas.stateEtag(),
    ));
    effect(() => this.browser.syncPresentation(
      this.active() && this.effectiveRenderer() === 'browser',
      this.canvas.threadId(),
      this.state(),
    ));
    effect(() => this.dirtyChange.emit(this.editor.dirty() || this.office.modified()));
    effect(() => {
      if (!this.isLiveAppSource() && this.resetStatus() !== 'idle') {
        this.resetStatus.set('idle');
        this.resetTarget.set(null);
      }
    });
    effect(() => {
      const target = this.resetTarget();
      if (
        target &&
        this.resetStatus() === 'confirming' &&
        !this.currentResetTargetMatches(target)
      ) {
        this.resetStatus.set('idle');
        this.resetTarget.set(null);
      }
    });
  }

  focusContent(): void {
    if (this.editor.editMode()) {
      this.canvasEditor()?.focus();
      return;
    }
    this.contentViewport()?.nativeElement.focus({preventScroll: true});
  }

  toggleEditor(): void {
    if (this.editor.editMode()) {
      this.editor.showPreview();
      return;
    }
    this.editor.enterEdit();
    queueMicrotask(() => this.canvasEditor()?.focus());
  }

  async downloadFile(): Promise<void> {
    const threadId = this.canvas.threadId();
    const path = this.downloadPath();
    if (!threadId || !path || this.downloading()) return;
    this.downloading.set(true);
    try {
      await this.downloads.download(threadId, path);
    } finally {
      this.downloading.set(false);
    }
  }

  popOutCanvas(): void {
    const threadId = this.canvas.threadId();
    if (!threadId || !this.canPopOut() || typeof window === 'undefined') return;
    const url = this.router.serializeUrl(
      this.router.createUrlTree(['/sessions', threadId, 'canvas']),
    );
    openCanvasPopOut(url, (href, target, features) => window.open(href, target, features));
  }

  requestOriginReset(): void {
    const state = this.state();
    const stateEtag = this.canvas.stateEtag();
    const sourceKey = canvasSourceKey(state);
    if (!this.canResetOrigin() || !state || !stateEtag || !sourceKey) return;
    // Origin rotation remains available for a live-app source even when this
    // deployment has viewing disabled: it is a trusted revocation action, not
    // a viewer-session capability. Bind confirmation to this exact app state.
    this.resetTarget.set({
      stateEtag,
      presentationRevision: state.presentation_revision,
      sourceKey,
    });
    this.resetStatus.set('confirming');
  }

  cancelOriginReset(): void {
    if (this.resetStatus() === 'confirming') {
      this.resetStatus.set('idle');
      this.resetTarget.set(null);
    }
  }

  confirmOriginReset(): void {
    const target = this.resetTarget();
    if (
      this.resetStatus() !== 'confirming' ||
      !this.canResetOrigin() ||
      !target ||
      !this.currentResetTargetMatches(target)
    ) {
      this.resetStatus.set('idle');
      this.resetTarget.set(null);
      return;
    }
    this.resetStatus.set('resetting');
    this.canvas.resetOrigin(target.stateEtag).pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: () => {
        this.resetTarget.set(null);
        this.resetStatus.set('success');
      },
      error: () => {
        this.resetTarget.set(null);
        this.resetStatus.set('error');
        this.canvas.reconcile();
      },
    });
  }

  dismissOriginResetNotice(): void {
    if (this.resetStatus() === 'success' || this.resetStatus() === 'error') {
      this.resetStatus.set('idle');
      this.resetTarget.set(null);
    }
  }

  private currentResetTargetMatches(target: CanvasResetTarget): boolean {
    return canvasResetTargetMatches(
      target,
      this.canvas.stateEtag(),
      this.state()?.presentation_revision ?? null,
      canvasSourceKey(this.state()),
    );
  }

  isReloadConflict(): boolean {
    const conflict = this.editor.conflict();
    return conflict === 'content_changed' ||
      conflict === 'presentation_changed' ||
      conflict === 'replaced' ||
      conflict === 'cleared';
  }

  reloadLabel(): string {
    const key = this.editor.conflict() === 'cleared'
      ? 'canvas.editor.discardCleared'
      : 'canvas.editor.loadCurrent';
    return this.transloco.translate(key);
  }
}
