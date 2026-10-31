import {
  AfterViewInit,
  ChangeDetectorRef,
  Component,
  computed,
  ElementRef,
  inject,
  OnDestroy,
  output,
  signal,
  ViewChild,
} from '@angular/core';
import {DOCUMENT, Location} from '@angular/common';
import {HttpClient} from '@angular/common/http';
import {Router} from '@angular/router';
import {TranslocoPipe, TranslocoService} from '@jsverse/transloco';
import {firstValueFrom} from 'rxjs';
import {environment} from '../../core/environment';
import type {Thread} from '../../core/models/api.model';
import {ErrorMessageService} from '../../core/services/error-message.service';
import {PersistentChatService} from '../../core/services/persistent-chat.service';
import {classifyResumeError} from '../../core/services/resume-error';
import {SessionListService} from '../../core/services/session-list.service';
import {AppButtonComponent} from '../../ui/button';
import {copyText} from '../../ui/copy-field/copy-text';
import {AppDialogComponent} from '../../ui/dialog';
import {AppIconComponent} from '../../ui/icon';
import {AppMenuComponent, AppMenuItemComponent, type MenuPoint} from '../../ui/menu';
import {AppToastService} from '../../ui/toast';

export interface SessionMenuActions {
  end: boolean;
  resume: boolean;
  delete: boolean;
}

const NO_ACTIONS: SessionMenuActions = {end: false, resume: false, delete: false};

/**
 * The lifecycle actions a session's menu offers, matching the /sessions
 * cards and the chat header.
 */
export function sessionMenuActions(thread: Thread): SessionMenuActions {
  const pending = thread.runtime_retirement_pending === true;
  if (thread.execution_lane === 'stateless' && pending) {
    // A stateless End or Delete that a retryable fence left pending: nothing
    // on the server finishes it, so choosing the same action again retries it.
    return {end: thread.retirement_permanent !== true, resume: false, delete: true};
  }
  if (thread.status === 'ending' || pending) {
    // A pinned retirement settles by itself.
    return NO_ACTIONS;
  }
  const ended = thread.status === 'ended';
  return {end: !ended, resume: ended, delete: true};
}

type BusyCode = 'turn_in_flight' | 'stateless_end_busy';

/** The mid-turn guard's 409 (session_silent_failure_audit.md #11): End or
 *  Delete would stop a turn that is still running. */
function busyCode(err: unknown): BusyCode | null {
  const e = err as {status?: number; error?: {detail?: {code?: unknown}}} | null;
  if (e?.status !== 409) return null;
  const code = e.error?.detail?.code;
  return code === 'turn_in_flight' || code === 'stateless_end_busy' ? code : null;
}

interface ForceRequest {
  thread: Thread;
  action: 'end' | 'delete';
  messageKey: string;
}

/**
 * The rail's per-session menu: right-click a session, or its ⋯ button, for
 * the actions /sessions offers on its cards, so managing a chat no longer
 * means leaving for that page. One instance serves every row.
 *
 * Rename is the rail's own — the row turns into its input — so this only
 * asks for it through `renameRequested`.
 */
@Component({
  selector: 'app-rail-session-menu',
  standalone: true,
  imports: [AppMenuComponent, AppMenuItemComponent, AppIconComponent, AppDialogComponent, AppButtonComponent, TranslocoPipe],
  template: `
    <app-menu #menu>
      <app-menu-item (activated)="openInNewTab()">
        <app-icon size="sm">open_in_new</app-icon>{{ 'nav.sessionMenu.openInNewTab' | transloco }}
      </app-menu-item>
      <app-menu-item (activated)="copyLink()">
        <app-icon size="sm">link</app-icon>{{ 'nav.sessionMenu.copyLink' | transloco }}
      </app-menu-item>
      <app-menu-item class="menu-divider" (activated)="rename()">
        <app-icon size="sm">edit</app-icon>{{ 'common.rename' | transloco }}
      </app-menu-item>
      @if (hasFiles()) {
        <app-menu-item (activated)="openFiles()">
          <app-icon size="sm">cloud</app-icon>{{ 'nav.sessionMenu.sessionFiles' | transloco }}
        </app-menu-item>
      }
      @if (actions().end) {
        <app-menu-item (activated)="end()">
          <app-icon size="sm">stop_circle</app-icon>{{ 'nav.sessionMenu.end' | transloco }}
        </app-menu-item>
      }
      @if (actions().resume) {
        <app-menu-item (activated)="resume()">
          <app-icon size="sm">play_arrow</app-icon>{{ 'nav.sessionMenu.resume' | transloco }}
        </app-menu-item>
      }
      <app-menu-item class="menu-divider" tone="danger" [disabled]="!actions().delete" (activated)="requestDelete()">
        <app-icon size="sm">delete</app-icon>{{ 'common.delete' | transloco }}
      </app-menu-item>
    </app-menu>

    <!-- Hoisted to <body> on init (see ngAfterViewInit). -->
    <div #dialogs>
      <app-dialog
        [open]="pendingDelete() !== null"
        [title]="'sessions.confirmDelete' | transloco"
        (closed)="pendingDelete.set(null)"
      >
        <p>{{ pendingDelete()?.title || ('sessions.untitledSession' | transloco) }}</p>
        <div appDialogActions>
          <app-button variant="secondary" (clicked)="pendingDelete.set(null)">
            {{ 'common.cancel' | transloco }}
          </app-button>
          <app-button variant="danger" (clicked)="confirmDelete()">
            {{ 'common.delete' | transloco }}
          </app-button>
        </div>
      </app-dialog>

      <app-dialog
        [open]="pendingForce() !== null"
        [title]="forceLabelKey() | transloco"
        (closed)="pendingForce.set(null)"
      >
        <p>{{ forceMessageKey() | transloco }}</p>
        <div appDialogActions>
          <app-button variant="secondary" (clicked)="pendingForce.set(null)">
            {{ 'common.cancel' | transloco }}
          </app-button>
          <app-button variant="danger" (clicked)="confirmForce()">
            {{ forceLabelKey() | transloco }}
          </app-button>
        </div>
      </app-dialog>
    </div>
  `,
  styles: [`
    :host {
      display: contents;
    }

    app-menu-item:not([data-tone='danger']) app-icon {
      color: var(--text-muted);
    }

    /* app-menu only projects <app-menu-item> children, so a group starts with
       a border on its first item rather than a divider element (as in the
       rail's account menu). */
    .menu-divider {
      margin-top: 6px;
      border-top: 1px solid var(--border-color);
    }
  `],
})
export class RailSessionMenuComponent implements AfterViewInit, OnDestroy {
  private readonly http = inject(HttpClient);
  private readonly router = inject(Router);
  private readonly location = inject(Location);
  private readonly doc = inject(DOCUMENT);
  private readonly cdr = inject(ChangeDetectorRef);
  private readonly chat = inject(PersistentChatService);
  private readonly sessions = inject(SessionListService);
  private readonly toast = inject(AppToastService);
  private readonly errors = inject(ErrorMessageService);
  private readonly transloco = inject(TranslocoService);

  /** The row's own inline rename. */
  readonly renameRequested = output<Thread>();

  @ViewChild('menu', {static: true}) private readonly menu?: AppMenuComponent;
  @ViewChild('dialogs', {static: true}) private readonly dialogs?: ElementRef<HTMLElement>;

  /** The session the menu was last opened for. */
  readonly target = signal<Thread | null>(null);
  readonly actions = computed(() => {
    const thread = this.target();
    return thread ? sessionMenuActions(thread) : NO_ACTIONS;
  });
  readonly hasFiles = computed(() => {
    const thread = this.target();
    return !!(thread?.cloud_session_url || (thread?.nc_session_folder && environment.cloudUrl));
  });

  readonly pendingDelete = signal<Thread | null>(null);
  readonly pendingForce = signal<ForceRequest | null>(null);
  readonly forceLabelKey = computed(() =>
    this.pendingForce()?.action === 'end' ? 'nav.sessionMenu.end' : 'common.delete',
  );
  readonly forceMessageKey = computed(
    () => this.pendingForce()?.messageKey ?? 'sessions.confirmForceDelete',
  );

  // On a phone the rail is a transformed drawer, which would make it the
  // containing block of the dialogs' fixed backdrop and trap them in its
  // 300px — so they live in <body>, as app-menu's panel does.
  ngAfterViewInit(): void {
    if (this.dialogs) this.doc.body.appendChild(this.dialogs.nativeElement);
  }

  ngOnDestroy(): void {
    this.dialogs?.nativeElement.remove();
  }

  openFor(thread: Thread, anchor: HTMLElement, at?: MenuPoint): void {
    if (!this.menu) return;
    this.menu.close();
    this.target.set(thread);
    // Which items show depends on the thread: render them before the menu
    // collects its items for keyboard navigation.
    this.cdr.detectChanges();
    this.menu.open(anchor, 'bottom-start', at);
  }

  /** The row's ⋯ button. */
  toggleFor(thread: Thread, anchor: HTMLElement): void {
    if (this.isOpenFor(thread.id)) {
      this.menu?.close();
      return;
    }
    this.openFor(thread, anchor);
  }

  /** Right-click (or a long press) on a row. */
  onContextMenu(event: MouseEvent, thread: Thread): void {
    event.preventDefault();
    // A context menu opened from the keyboard (the Menu key, Shift+F10)
    // has no pointer position: open beside the row instead.
    const at = event.clientX || event.clientY ? {x: event.clientX, y: event.clientY} : undefined;
    this.openFor(thread, event.currentTarget as HTMLElement, at);
  }

  isOpenFor(id: string): boolean {
    return this.menu?.isOpen() === true && this.target()?.id === id;
  }

  openInNewTab(): void {
    const thread = this.target();
    if (thread) window.open(this.sessionUrl(thread), '_blank', 'noopener');
  }

  async copyLink(): Promise<void> {
    const thread = this.target();
    if (!thread) return;
    const url = new URL(this.sessionUrl(thread), this.doc.baseURI).href;
    if (await copyText(url)) {
      this.toast.success(this.transloco.translate('nav.sessionMenu.linkCopied'));
    } else {
      this.toast.danger(this.transloco.translate('common.copyFailed'));
    }
  }

  rename(): void {
    const thread = this.target();
    if (thread) this.renameRequested.emit(thread);
  }

  openFiles(): void {
    const thread = this.target();
    if (!thread) return;
    if (thread.cloud_session_url) {
      window.open(thread.cloud_session_url, '_blank');
      return;
    }
    // Legacy Nextcloud sessions without a computed URL, as on /sessions.
    if (!thread.nc_session_folder || !environment.cloudUrl) return;
    const folderName = thread.nc_session_folder.split('/').pop();
    window.open(`${environment.cloudUrl}/apps/files/?dir=/${folderName}`, '_blank');
  }

  async end(): Promise<void> {
    const thread = this.target();
    if (thread) await this.endThread(thread, false);
  }

  async resume(): Promise<void> {
    const thread = this.target();
    if (!thread) return;
    if (this.chat.threadId() === thread.id) {
      // Resumed in place: the chat service owns the open session's
      // controls, and its drift dialog.
      try {
        await this.chat.resumeSession();
      } catch (e) {
        this.toast.danger(this.errors.translate(e, 'errors.sessions.resumeFailed'));
      }
      await this.sessions.refresh();
      return;
    }
    try {
      await firstValueFrom(
        this.http.post(`${environment.apiUrl}/persistent/threads/${thread.id}/resume`, {}),
      );
    } catch (e) {
      // Config drift (428) is resolved on the chat page, whose Resume card
      // owns the drift dialog — go there, as /sessions does.
      if (classifyResumeError(e).kind !== 'drift') {
        this.toast.danger(this.errors.translate(e, 'errors.sessions.resumeFailed'));
        return;
      }
      this.toast.info(this.transloco.translate('sessions.configDrift.attentionNeeded'));
    }
    await this.router.navigate(['/sessions', thread.id]);
  }

  requestDelete(): void {
    const thread = this.target();
    if (thread) this.pendingDelete.set(thread);
  }

  async confirmDelete(): Promise<void> {
    const thread = this.pendingDelete();
    this.pendingDelete.set(null);
    if (thread) await this.deleteThread(thread, false);
  }

  async confirmForce(): Promise<void> {
    const request = this.pendingForce();
    this.pendingForce.set(null);
    if (!request) return;
    if (request.action === 'end') {
      await this.endThread(request.thread, true);
    } else {
      await this.deleteThread(request.thread, true);
    }
  }

  private async endThread(thread: Thread, force: boolean): Promise<void> {
    if (this.chat.threadId() === thread.id) {
      // The open session ends through the chat service: it owns that
      // session's controls and asks before stopping a busy turn.
      try {
        if ((await this.chat.endSession()) === 'retryable') {
          this.toast.warning(this.transloco.translate('nav.sessionMenu.endRetryable'));
        }
      } catch (e) {
        this.toast.danger(this.errors.translate(e, 'errors.sessions.endFailed'));
      }
      await this.sessions.refresh();
      return;
    }
    try {
      await firstValueFrom(
        this.http.delete(`${environment.apiUrl}/persistent/threads/${thread.id}${force ? '?force=true' : ''}`),
      );
    } catch (e) {
      const busy = force ? null : busyCode(e);
      if (busy) {
        const messageKey =
          busy === 'turn_in_flight' ? 'sessions.confirmEndMidTurn' : 'sessions.confirmEndStatelessBusy';
        this.pendingForce.set({thread, action: 'end', messageKey});
        return;
      }
      if ((e as {status?: number})?.status === 503) {
        // The retirement fence: End may already have begun, and choosing it
        // again is the retry.
        this.toast.warning(this.transloco.translate('nav.sessionMenu.endRetryable'));
      } else {
        this.toast.danger(this.errors.translate(e, 'errors.sessions.endFailed'));
      }
    }
    await this.sessions.refresh();
  }

  private async deleteThread(thread: Thread, force: boolean): Promise<void> {
    const query = force ? 'permanent=true&force=true' : 'permanent=true';
    try {
      await firstValueFrom(
        this.http.delete(`${environment.apiUrl}/persistent/threads/${thread.id}?${query}`),
      );
    } catch (e) {
      if (!force && busyCode(e)) {
        this.pendingForce.set({thread, action: 'delete', messageKey: 'sessions.confirmForceDelete'});
        return;
      }
      if ((e as {status?: number})?.status === 503) {
        // The retirement fence: the Delete may have begun. /sessions keeps
        // the card, with Retry delete.
        this.toast.warning(this.transloco.translate('nav.sessionMenu.deleteRetryable'));
      } else {
        this.toast.danger(this.errors.translate(e, 'errors.sessions.deleteFailed'));
      }
      await this.sessions.refresh();
      return;
    }
    this.sessions.removeLocal(thread.id);
    // The chat on screen is gone: land on a new one.
    if (this.isShowing(thread.id)) await this.router.navigate(['/']);
    await this.sessions.refresh();
  }

  private isShowing(threadId: string): boolean {
    const path = this.router.url.split(/[?#]/)[0];
    return path === `/sessions/${threadId}` || path.startsWith(`/sessions/${threadId}/`);
  }

  /** The session's URL as the browser addresses it — under the base href. */
  private sessionUrl(thread: Thread): string {
    return this.location.prepareExternalUrl(
      this.router.serializeUrl(this.router.createUrlTree(['/sessions', thread.id])),
    );
  }
}
