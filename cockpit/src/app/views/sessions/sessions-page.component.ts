import {Component, computed, DestroyRef, inject, OnInit, signal} from '@angular/core';
import {Router} from '@angular/router';
import {HttpClient} from '@angular/common/http';
import {TitleCasePipe} from '@angular/common';
import {TranslocoDatePipe} from '@jsverse/transloco-locale';
import {firstValueFrom} from 'rxjs';
import {environment} from '../../core/environment';
import {conferenceLauncherCommands} from '../../core/officer/conference';
import {PersistentChatService} from '../../core/services/persistent-chat.service';
import {classifyResumeError} from '../../core/services/resume-error';
import {ModelService} from '../../core/services/model.service';
import {SessionListService} from '../../core/services/session-list.service';
import {AppToastService} from '../../ui/toast';
import {ErrorMessageService} from '../../core/services/error-message.service';
import {UserService} from '../../core/services/user.service';
import {Thread} from '../../core/models/api.model';
import {VMCreationWaitComponent} from '../../core/components/vm-creation-wait.component';
import {SidebarToggleComponent} from '../../shell/sidebar-toggle/sidebar-toggle.component';
import {TranslocoPipe, TranslocoService} from '@jsverse/transloco';
import {AppButtonComponent} from '../../ui/button';
import {AppDialogComponent} from '../../ui/dialog';
import {AppIconButtonComponent} from '../../ui/icon-button';
import {AppTabBarComponent, AppTabComponent} from '../../ui/tab-bar';
import {AppInputComponent} from '../../ui/input';
import {AppInlineEditableTextComponent} from '../../ui/inline-editable-text';
import {AppSelectComponent} from '../../ui/select';
import {AppChipComponent} from '../../ui/chip';
import {AppIconComponent} from '../../ui/icon';
import {AppFormFieldComponent} from '../../ui/form-field';

interface Project {
    id: string;
    name: string;
    status: string;
    description?: string;
    is_default?: boolean;
}

/**
 * While any card is `ending` (a pinned retirement after End or a permanent
 * Delete, about 60–75 s) the list is re-read in the background: first 2 s
 * after the card is seen, doubling, capped at 15 s — so a settled card updates
 * within the server's settlement time plus at most one 15 s interval. The poll
 * stops as soon as no card is ending, and on destroy.
 */
const ENDING_POLL_INITIAL_MS = 2_000;
const ENDING_POLL_MAX_MS = 15_000;

@Component({
    selector: 'app-sessions-page',
    standalone: true,
    imports: [
        TranslocoDatePipe,
        TitleCasePipe,
        SidebarToggleComponent,
        TranslocoPipe,
        AppButtonComponent,
        AppIconButtonComponent,
        AppTabBarComponent,
        AppTabComponent,
        AppInputComponent,
        AppInlineEditableTextComponent,
        AppSelectComponent,
        AppChipComponent,
        AppIconComponent,
        AppFormFieldComponent,
        AppDialogComponent,
        VMCreationWaitComponent,
    ],
    template: `
    <div class="sessions-page">
      <div class="page-header">
        <div class="header-left">
          <app-sidebar-toggle />
          <h2>{{ 'sessions.title' | transloco }}</h2>
        </div>
      </div>

      <!-- Active session banner (hidden when filtering to ended sessions) -->
      @if (chat.isConnected() && statusFilter() !== 'ended') {
        <div class="active-banner" (click)="returnToActive()">
          <span class="active-dot"></span>
          <span>{{ 'sessions.activeBanner' | transloco }}</span>
          <span class="active-action">{{ 'sessions.returnToChat' | transloco }}</span>
        </div>
      }

      <!-- Create dialog -->
      @if (showCreate) {
        <div class="create-dialog">
          <h3>{{ 'sessions.create.title' | transloco }}</h3>
          <p class="dialog-hint">{{ 'sessions.create.hint' | transloco }}</p>
          <app-form-field [label]="'sessions.create.titleLabel' | transloco">
            <app-input [(value)]="newTitle" [placeholder]="'sessions.create.titlePlaceholder' | transloco" />
          </app-form-field>
          <app-form-field [label]="'sessions.create.configLabel' | transloco">
            <app-select [(value)]="newConfig">
              <option value="session_base">{{ 'sessions.create.configDefault' | transloco }}</option>
              <option value="developer">{{ 'sessions.create.configDeveloper' | transloco }}</option>
              <option value="scholar">{{ 'sessions.create.configScholar' | transloco }}</option>
            </app-select>
          </app-form-field>
          <app-form-field [label]="'sessions.create.modelLabel' | transloco">
            <app-select [(value)]="newModel">
              <option value="">{{ 'sessions.create.modelConfigDefault' | transloco }}</option>
              @for (group of modelService.models(); track group.group) {
                <optgroup [label]="group.group">
                  @for (model of group.models; track model) {
                    <option [value]="model">{{ model }}</option>
                  }
                </optgroup>
              }
            </app-select>
          </app-form-field>
          <app-form-field [label]="'sessions.create.projectsLabel' | transloco" [hint]="'sessions.create.projectsHint' | transloco">
            <div class="project-chips">
              @if (projects().length === 0) {
                <span class="chip-hint">{{ 'sessions.create.projectsEmpty' | transloco }}</span>
              } @else {
                @for (project of projects(); track project.id) {
                  <app-chip
                    [selected]="isProjectSelected(project.id)"
                    [ariaLabel]="project.description || project.name"
                    (clicked)="toggleProject(project.id)"
                  >{{ project.name }}</app-chip>
                }
              }
            </div>
          </app-form-field>
          <app-form-field [label]="'sessions.create.permissionLabel' | transloco">
            <app-select [(value)]="newPermission">
              <option value="supervised">{{ 'sessions.create.permissionSupervised' | transloco }}</option>
              <option value="auto_accept">{{ 'sessions.create.permissionAutoAccept' | transloco }}</option>
              <option value="autonomous">{{ 'sessions.create.permissionAutonomous' | transloco }}</option>
            </app-select>
          </app-form-field>
          @if (createError()) {
            <div class="create-error" role="alert">{{ createError() }}</div>
          }
          <div class="dialog-actions">
            <app-button variant="primary" size="sm" [loading]="creating()" (clicked)="createSession()">
              {{ 'sessions.create.create' | transloco }}
            </app-button>
            <app-button variant="secondary" size="sm" (clicked)="showCreate = false">
              {{ 'sessions.create.cancel' | transloco }}
            </app-button>
          </div>
        </div>
      }

      <!-- Session list -->
      <div class="session-list">
        @if (loading()) {
          <div class="loading">{{ 'sessions.loading' | transloco }}</div>
        } @else if (threads().length === 0) {
          <div class="empty-state">
            <app-icon size="inherit" class="empty-icon">chat_bubble_outline</app-icon>
            <p>{{ 'sessions.empty' | transloco }}</p>
            <app-button variant="primary" size="sm" (clicked)="goToDraft()">
              {{ 'sessions.emptyCta' | transloco }}
            </app-button>
          </div>
        } @else {
          <!-- Filter tabs -->
          <app-tab-bar class="filter-tabs" [value]="statusFilter()" (valueChange)="statusFilter.set($event)">
            <app-tab [value]="null">{{ 'sessions.filter.all' | transloco:{ count: threads().length } }}</app-tab>
            <app-tab [value]="'active'">{{ 'sessions.filter.active' | transloco:{ count: activeCount() } }}</app-tab>
            <app-tab [value]="'ended'">{{ 'sessions.filter.ended' | transloco:{ count: endedCount() } }}</app-tab>
          </app-tab-bar>

          @if (filteredThreads().length === 0) {
            <div class="filter-empty">
              @if (statusFilter() === 'active') {
                <app-icon size="inherit" class="empty-icon">check_circle</app-icon>
                <p>{{ 'sessions.emptyFilterActive' | transloco }}</p>
              } @else if (statusFilter() === 'ended') {
                <app-icon size="inherit" class="empty-icon">history</app-icon>
                <p>{{ 'sessions.emptyFilterEnded' | transloco }}</p>
              }
            </div>
          }

          @for (thread of filteredThreads(); track thread.id) {
            <div class="session-card"
                 data-testid="session-card"
                 [attr.data-thread-id]="thread.id"
                 [class.ended]="thread.status === 'ended'"
                 [class.ending]="thread.status === 'ending'">
              <div class="session-heading" (click)="openSession(thread)">
                <span class="session-status-dot" [class]="thread.status"></span>
                <span class="session-title">
                  <app-inline-editable-text
                    [value]="thread.title || ('sessions.untitledSession' | transloco)"
                    [ariaLabel]="'common.rename' | transloco"
                    (save)="onRenameThread(thread, $event)"
                  />
                </span>
              </div>
              <div class="session-meta" (click)="openSession(thread)">
                <span class="session-id" title="Session ID">{{ thread.id.slice(0, 8) }}</span>
                <span class="session-config">{{ thread.config_name | titlecase }}</span>
                @if (officerBadge(thread); as ob) {
                  <span class="session-officer-badge" [attr.data-kind]="ob">{{
                    ob === 'centurion' ? 'Centurion' : 'Conference'
                  }}</span>
                }
                <span class="meta-item">{{ thread.total_turns || 0 }} {{ ((thread.total_turns || 0) === 1 ? 'sessions.turnsOne' : 'sessions.turnsMany') | transloco }}</span>
                <span class="meta-item">{{ thread.last_activity | translocoDate:{dateStyle:'short', timeStyle:'short'} }}</span>
              </div>
              @if (thread.vm_creation; as creation) {
                @if (creation.wait) { <app-vm-creation-wait [creation]="creation" /> }
              }
              <div class="session-actions">
                @if (canTalk(thread)) {
                  <app-icon-button
                    [ariaLabel]="'sessions.tooltip.talk' | transloco"
                    [tooltip]="'sessions.tooltip.talk' | transloco"
                    (clicked)="talkToOfficer(thread)"
                  >
                    <app-icon size="sm">forum</app-icon>
                  </app-icon-button>
                }
                @if (thread.cloud_session_url || thread.nc_session_folder) {
                  <app-icon-button
                    [ariaLabel]="'sessions.tooltip.sessionFiles' | transloco"
                    [tooltip]="'sessions.tooltip.sessionFiles' | transloco"
                    (clicked)="openSessionFiles(thread)"
                  >
                    <app-icon size="sm">cloud</app-icon>
                  </app-icon-button>
                }
                <app-icon-button
                  [ariaLabel]="'sessions.tooltip.resume' | transloco"
                  [tooltip]="'sessions.tooltip.resume' | transloco"
                  [disabled]="thread.status === 'ending'"
                  (clicked)="resumeSession(thread)"
                >
                  <app-icon size="sm">play_arrow</app-icon>
                </app-icon-button>
                <app-icon-button
                  variant="danger"
                  [ariaLabel]="(isDeleteRetry(thread) ? 'sessions.tooltip.retryDelete' : 'sessions.tooltip.delete') | transloco"
                  [tooltip]="(isDeleteRetry(thread) ? 'sessions.tooltip.retryDelete' : 'sessions.tooltip.delete') | transloco"
                  [disabled]="thread.status === 'ending' && !isDeleteRetry(thread)"
                  (clicked)="deleteSession(thread)"
                >
                  <app-icon size="sm">delete</app-icon>
                </app-icon-button>
              </div>
            </div>
          }
        }
      </div>

      <app-dialog
        [open]="confirmDeleteOpen()"
        [title]="'sessions.confirmDelete' | transloco"
        (closed)="confirmDeleteOpen.set(false)"
      >
        <p>{{ pendingDelete()?.title || ('sessions.untitledSession' | transloco) }}</p>
        <div appDialogActions>
          <app-button variant="secondary" (clicked)="confirmDeleteOpen.set(false)">
            {{ 'common.cancel' | transloco }}
          </app-button>
          <app-button variant="danger" (clicked)="confirmDelete()">
            {{ 'common.delete' | transloco }}
          </app-button>
        </div>
      </app-dialog>

      <app-dialog
        [open]="confirmForceOpen()"
        [title]="'common.delete' | transloco"
        (closed)="confirmForceOpen.set(false)"
      >
        <p>{{ 'sessions.confirmForceDelete' | transloco }}</p>
        <div appDialogActions>
          <app-button variant="secondary" (clicked)="confirmForceOpen.set(false)">
            {{ 'common.cancel' | transloco }}
          </app-button>
          <app-button variant="danger" (clicked)="confirmForceDelete()">
            {{ 'common.delete' | transloco }}
          </app-button>
        </div>
      </app-dialog>
    </div>
  `,
    styles: [`
    :host {
      display: block;
      height: 100%;
      overflow-y: auto;
      background: var(--app-bg);
    }

    .sessions-page {
      max-width: var(--content-max-width);
      margin: 0 auto;
      padding: 24px;
    }

    .page-header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      margin-bottom: 20px;
    }

    .header-left {
      display: flex;
      align-items: center;
      gap: 12px;
    }

    .page-header h2 {
      font-size: 18px;
      font-weight: 600;
      color: var(--text-primary, var(--text-primary));
      margin: 0;
    }

    /* Active session banner */
    .active-banner {
      display: flex;
      align-items: center;
      gap: 8px;
      padding: 10px 14px;
      background: var(--success-tint);
      border: 1px solid var(--success);
      border-radius: var(--radius-surface);
      margin-bottom: 16px;
      cursor: pointer;
      font-size: 13px;
      color: var(--success);
      transition: background 0.15s ease;
    }

    .active-banner:hover { background: var(--success-tint); }

    .active-dot {
      width: 8px;
      height: 8px;
      border-radius: 50%;
      background: var(--success);
      flex-shrink: 0;
      animation: pulse 1.5s infinite;
    }

    .active-action {
      margin-left: auto;
      font-size: 12px;
      font-weight: 600;
      text-decoration: underline;
    }

    .dialog-hint {
      font-size: 11px;
      color: var(--text-muted);
      line-height: 1.5;
      margin-bottom: 8px;
    }

    .dialog-hint { margin-top: -4px; }

    /* Create dialog */
    .create-dialog {
      padding: 16px;
      background: var(--panel-bg, var(--panel-bg));
      border: 1px solid var(--border-color, var(--surface-0));
      border-radius: var(--radius-surface);
      margin-bottom: 16px;
    }

    .create-dialog h3 {
      font-size: 14px;
      font-weight: 600;
      color: var(--text-primary, var(--text-primary));
      margin: 0 0 12px;
    }


    .project-chips {
      display: flex;
      flex-wrap: wrap;
      gap: 6px;
      margin-bottom: 4px;
    }

    .chip-hint {
      font-size: 10px;
      color: var(--text-muted);
    }

    .create-error {
      padding: 8px 12px;
      margin-bottom: 12px;
      border-radius: var(--radius-control);
      background: var(--danger-tint);
      border: 1px solid var(--danger-tint);
      color: var(--danger);
      font-size: 12px;
    }

    .dialog-actions {
      display: flex;
      gap: 8px;
      margin-top: 12px;
    }

    .filter-tabs {
      margin-bottom: 12px;
    }

    /* Session cards */
    .session-card {
      display: grid;
      grid-template-columns: 1fr auto;
      grid-template-areas:
        "heading heading"
        "meta    actions";
      align-items: center;
      column-gap: 8px;
      row-gap: 6px;
      padding: 12px;
      border: 1px solid var(--border-color, var(--surface-0));
      border-radius: var(--radius-surface);
      background: var(--panel-bg, var(--panel-bg));
      margin-bottom: 8px;
      transition: border-color 0.15s ease;
    }

    .session-card:hover { border-color: var(--accent-color, var(--accent-color)); }
    .session-card.ended { opacity: 0.6; }

    /* The title gets its own full-width row so it shows as much as possible;
       the id/config/meta and the action buttons sit on the row beneath it. */
    .session-heading {
      grid-area: heading;
      display: flex;
      align-items: center;
      gap: 8px;
      min-width: 0;
      cursor: pointer;
    }

    .session-status-dot {
      width: 8px;
      height: 8px;
      border-radius: 50%;
      flex-shrink: 0;
    }

    .session-status-dot.active, .session-status-dot.created { background: var(--success); }
    .session-status-dot.ending { background: var(--warning, #f59e0b); }
    .session-status-dot.ended { background: var(--surface-2); }

    .session-title {
      font-size: 13px;
      font-weight: 600;
      color: var(--text-primary, var(--text-primary));
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
      flex: 1;
      min-width: 0;
    }

    .session-officer-badge {
      font-size: 10px;
      padding: 1px 6px;
      border-radius: var(--radius-tag);
      border: 1px solid color-mix(in srgb, var(--accent, #6366f1) 45%, transparent);
      color: var(--accent, #6366f1);
      white-space: nowrap;
    }
    .session-officer-badge[data-kind='conference'] {
      border-style: dashed;
    }
    .session-config {
      font-size: 10px;
      padding: 1px 6px;
      border-radius: var(--radius-tag);
      background: var(--surface-0, var(--surface-0));
      color: var(--text-muted);
      /* Shrink + ellipsis so a long config (e.g. a project UUID) truncates
         instead of pushing the title to 0 width or overlapping the actions. */
      flex-shrink: 1;
      min-width: 0;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }

    .session-id {
      font-family: var(--font-mono, monospace);
      font-size: 10px;
      color: var(--text-muted);
      flex-shrink: 0;
    }

    .session-meta {
      grid-area: meta;
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      gap: 4px 12px;
      min-width: 0;
      cursor: pointer;
    }

    .meta-item {
      font-size: 11px;
      color: var(--text-muted);
    }

    .session-actions {
      grid-area: actions;
      display: flex;
      gap: 4px;
      justify-self: end;
    }

    /* Empty / loading */
    .loading, .empty-state {
      text-align: center;
      padding: 40px;
      color: var(--text-muted);
      font-size: 13px;
    }

    @keyframes pulse {
      0%, 100% { opacity: 1; }
      50% { opacity: 0.4; }
    }

    .empty-icon {
      display: block;
      font-size: 48px;
      margin-bottom: 12px;
      opacity: 0.3;
    }

    .filter-empty {
      text-align: center;
      padding: 32px;
      color: var(--text-muted);
      font-size: 13px;
    }

    @media (max-width: 768px) {
      .sessions-page {
        max-width: 100%;
        padding: 12px;
      }

      .sessions-header {
        flex-wrap: wrap;
        gap: 8px;
      }

      .filter-tabs {
        flex-wrap: wrap;
      }

      .session-card {
        padding: 10px;
      }

      .session-actions button {
        min-height: 36px;
        font-size: 11px;
      }
    }
  `],
})
export class SessionsPageComponent implements OnInit {
    private readonly http = inject(HttpClient);
    private readonly router = inject(Router);
    private readonly toast = inject(AppToastService);
    private readonly errors = inject(ErrorMessageService);
    private readonly userService = inject(UserService);
    readonly modelService = inject(ModelService);
    readonly chat = inject(PersistentChatService);
    private readonly transloco = inject(TranslocoService);
    private readonly sessionList = inject(SessionListService);

    threads = signal<Thread[]>([]);
    projects = signal<Project[]>([]);
    loading = signal(true);
    creating = signal(false);
    statusFilter = signal<string | null>(null);
    selectedProjectIds = signal<string[]>([]);

    // Themed delete-confirmation dialogs (replace the native confirm()).
    confirmDeleteOpen = signal(false);
    confirmForceOpen = signal(false);
    pendingDelete = signal<Thread | null>(null);

    /**
     * Thread id → the `force` flag of a confirmed permanent Delete that the
     * server fenced with a retryable 503. Such a card keeps its Delete control
     * while `ending`, as a retry of that same request. Dropped on a 2xx, on a
     * non-503 failure, and once the card leaves `ending` (or the list).
     */
    readonly deleteRetries = signal<ReadonlyMap<string, boolean>>(new Map());

    private endingPollTimer: ReturnType<typeof setTimeout> | null = null;
    private endingPollDelayMs = ENDING_POLL_INITIAL_MS;
    private destroyed = false;

    showCreate = false;
    /** In-dialog server rejection, so a bad config is correctable in place. */
    readonly createError = signal<string | null>(null);
    newTitle = '';
    newConfig = 'session_base';
    newModel = this.loadSavedSessionModel();
    newPermission = 'supervised';

    filteredThreads = () => {
        const filter = this.statusFilter();
        const all = this.threads();
        if (!filter) return all;
        // "Active" is the non-terminal bucket shown by activeCount: created,
        // awaiting, suspended and the non-resumable ending handoff all remain
        // visible until the authoritative ended transition lands.
        if (filter === 'active') return all.filter(t => t.status !== 'ended');
        return all.filter(t => t.status === filter);
    };

    readonly activeCount = computed(() => this.threads().filter(t => t.status !== 'ended').length);
    readonly endedCount = computed(() => this.threads().filter(t => t.status === 'ended').length);

    constructor() {
        inject(DestroyRef).onDestroy(() => {
            this.destroyed = true;
            this.cancelEndingPoll();
        });
    }

    ngOnInit(): void {
        this.loadThreads();
        this.loadProjects();
        this.modelService.load();
    }

    async loadThreads(): Promise<void> {
        this.loading.set(true);
        await this.refreshThreads();
        this.loading.set(false);
    }

    /**
     * Re-read the list without the loading placeholder (which would swap the
     * whole list out on every poll), then re-arm or stop the ending poll.
     */
    private async refreshThreads(): Promise<void> {
        // No try/catch: SessionListService.refresh() never rejects — both its
        // success and failure paths resolve, updating its own signals either
        // way (a failed fetch leaves sessionList.threads() as whatever it
        // already had, so re-reading it below is safe on either outcome).
        await this.sessionList.refresh();
        if (this.destroyed) return;
        this.threads.set(
            this.sessionList.threads()
                // The server filters children, but keep the mutation-heavy
                // session controls fail-closed against an older/cached list.
                .filter(thread => thread.kind !== 'subagent')
                .map(thread =>
                    thread.runtime_retirement_pending === true
                        ? { ...thread, status: 'ending' as const }
                        : thread,
                ),
        );
        this.dropSettledDeleteRetries();
        this.syncEndingPoll();
    }

    /**
     * Keep one background re-read scheduled while any card is `ending`, each
     * gap doubling up to the cap; stop and reset the backoff once none is.
     * House idiom: a cleared setTimeout, not rxjs.
     */
    private syncEndingPoll(): void {
        if (this.destroyed || !this.threads().some(t => t.status === 'ending')) {
            this.cancelEndingPoll();
            this.endingPollDelayMs = ENDING_POLL_INITIAL_MS;
            return;
        }
        if (this.endingPollTimer !== null) return;
        const delay = this.endingPollDelayMs;
        this.endingPollDelayMs = Math.min(delay * 2, ENDING_POLL_MAX_MS);
        this.endingPollTimer = setTimeout(() => {
            this.endingPollTimer = null;
            void this.refreshThreads();
        }, delay);
    }

    private cancelEndingPoll(): void {
        if (this.endingPollTimer !== null) clearTimeout(this.endingPollTimer);
        this.endingPollTimer = null;
    }

    /** A delete retry only means something on a card that is still `ending`. */
    private dropSettledDeleteRetries(): void {
        const retries = this.deleteRetries();
        if (retries.size === 0) return;
        const ending = new Set(
            this.threads().filter(t => t.status === 'ending').map(t => t.id),
        );
        const kept = new Map([...retries].filter(([id]) => ending.has(id)));
        if (kept.size !== retries.size) this.deleteRetries.set(kept);
    }

    private setDeleteRetry(threadId: string, force: boolean | null): void {
        const retries = new Map(this.deleteRetries());
        if (force === null) {
            if (!retries.delete(threadId)) return;
        } else {
            retries.set(threadId, force);
        }
        this.deleteRetries.set(retries);
    }

    /** True while this card's last confirmed permanent Delete was fenced (503). */
    isDeleteRetry(thread: Thread): boolean {
        return this.deleteRetries().has(thread.id);
    }

    async loadProjects(): Promise<void> {
        try {
            const userId = this.userService.currentUserId();
            const params = userId ? `?user_id=${userId}` : '';
            const data = await firstValueFrom(
                this.http.get<Project[]>(`${environment.apiUrl}/projects${params}`)
            );
            this.projects.set(data || []);
            const defaultProject = (data || []).find(p => p.is_default);
            if (defaultProject) {
                this.selectedProjectIds.set([defaultProject.id]);
            }
        } catch (e) {
            // Silent — projects not available
        }
    }

    toggleProject(id: string): void {
        const current = this.selectedProjectIds();
        if (current.includes(id)) {
            this.selectedProjectIds.set(current.filter(p => p !== id));
        } else {
            this.selectedProjectIds.set([...current, id]);
        }
    }

    isProjectSelected(id: string): boolean {
        return this.selectedProjectIds().includes(id);
    }

    async createSession(): Promise<void> {
        this.creating.set(true);
        this.createError.set(null);
        const body: Record<string, any> = {
            title: this.newTitle || 'Untitled Session',
            config_name: this.newConfig,
            permission_mode: this.newPermission,
            // This legacy dialog has no connector picker and is no longer
            // reachable from the New Session button. If invoked by an older
            // host, be explicit instead of silently applying defaults.
            datasource_ids: [],
        };
        if (this.newModel) {
            body['model'] = this.newModel;
            this.persistSessionModel(this.newModel);
        }
        if (this.selectedProjectIds().length > 0) {
            body['project_ids'] = this.selectedProjectIds();
        }
        // Create before dismissing the dialog: a rejected config used to close
        // it, clear the fields and bounce back here with a toast, so there was
        // nothing left to correct. Mirrors the full New Session form.
        try {
            const resp = await firstValueFrom(
                this.http.post<{ thread_id: string }>(
                    `${environment.apiUrl}/persistent/threads`,
                    body,
                ),
            );
            this.showCreate = false;
            this.newTitle = '';
            this.selectedProjectIds.set([]);
            await this.router.navigate(['/sessions', resp.thread_id]);
        } catch (err) {
            this.createError.set(this.errors.translate(err, 'sessions.create.failed'));
        } finally {
            this.creating.set(false);
        }
    }

    async onRenameThread(thread: Thread, title: string): Promise<void> {
        const previous = thread.title;
        // Optimistic: update the card immediately, revert if the PATCH fails.
        // This page's own `threads` is a filtered/mapped snapshot of
        // SessionListService, not a computed over it (see loadThreads), so it
        // doesn't pick up sessionList.renameLocal below for free — both need
        // the explicit update, and both revert together on failure.
        this.threads.update((list) =>
            list.map((t) => (t.id === thread.id ? {...t, title} : t)),
        );
        this.sessionList.renameLocal(thread.id, title);
        try {
            await this.chat.renameThread(thread.id, title);
        } catch (e) {
            this.threads.update((list) =>
                list.map((t) => (t.id === thread.id ? {...t, title: previous} : t)),
            );
            this.sessionList.renameLocal(thread.id, previous);
            this.toast.danger(this.errors.translate(e, 'errors.sessions.renameFailed'));
        }
    }

    /**
     * Open a session in the chat view without resuming. For ended threads this
     * gives a read-only history view + the in-chat resume card; for active
     * threads it just navigates. No POST to /resume — the user opts in to
     * spinning the agent back up via the resume card or the dedicated icon.
     */
    /**
     * 'centurion' for a standing officer thread, 'conference' for his
     * interactive embodiment, null otherwise (centurion.md S9). Reads the
     * denormalized officer block from thread metadata; lists that omit
     * metadata simply show no badge.
     */
    officerBadge(thread: Thread): 'centurion' | 'conference' | null {
        const metadata = thread.metadata as
            | {config_override?: {officer?: {enabled?: unknown; conference?: unknown}}}
            | undefined;
        const officer = metadata?.config_override?.officer;
        if (!officer) return null;
        if (officer.enabled === true || officer.enabled === 'true') return 'centurion';
        if (officer.conference === true || officer.conference === 'true') {
            return 'conference';
        }
        return null;
    }

    openSession(thread: Thread): void {
        this.router.navigate(['/sessions', thread.id]);
    }

    /** Only a standing officer's row can convene him; conferences are already meetings. */
    canTalk(thread: Thread): boolean {
        return this.officerBadge(thread) === 'centurion' && !!thread.project_id;
    }

    /** Talk = create-or-resume his conference via the launcher route (§3.5). */
    talkToOfficer(thread: Thread): void {
        if (!thread.project_id) return;
        void this.router.navigate(conferenceLauncherCommands(thread.project_id));
    }

    async resumeSession(thread: Thread): Promise<void> {
        if (thread.status === 'ended') {
            try {
                await firstValueFrom(
                    this.http.post(`${environment.apiUrl}/persistent/threads/${thread.id}/resume`, {})
                );
                thread.status = 'created';
            } catch (e: any) {
                // This page has no drift dialog of its own. A config-drift
                // 428 falling into the generic toast below would show the
                // same "Failed to resume session" wording a 500 gets and
                // dead-end here forever — the exact problem this feature
                // exists to remove, just moved from silence to a toast.
                // Fall through to the plain navigate instead: the chat page
                // DOES own the drift dialog (config-drift-dialog.component.ts),
                // and its in-chat Resume card re-POSTs /resume, which
                // repopulates PersistentChatService.pendingDrift and surfaces
                // the real dialog there. Anything else (403/500/...) still
                // dead-ends here with the toast.
                if (classifyResumeError(e).kind !== 'drift') {
                    this.toast.danger(this.errors.translate(e, 'errors.sessions.resumeFailed'));
                    return;
                }
                // The 428 arrives before the thread's status flips, so the
                // chat page would otherwise render its generic ended-card
                // with no sign that a resume was already attempted — the
                // first click looks like it did nothing. An informational
                // toast (not danger — this isn't an error, it's recoverable
                // via the in-chat drift dialog on the very next click) is
                // honest about what happened before navigating there.
                this.toast.info(this.transloco.translate('sessions.configDrift.attentionNeeded'));
            }
        }
        this.router.navigate(['/sessions', thread.id]);
    }

    openSessionFiles(thread: Thread): void {
        // Prefer the backend-computed URL (works for all backends).
        if (thread.cloud_session_url) {
            window.open(thread.cloud_session_url, '_blank');
            return;
        }
        // Legacy fallback for Nextcloud sessions without a computed URL.
        if (!thread.nc_session_folder || !environment.cloudUrl) return;
        const folderName = thread.nc_session_folder.split('/').pop();
        window.open(`${environment.cloudUrl}/apps/files/?dir=/${folderName}`, '_blank');
    }

    deleteSession(thread: Thread): void {
        // A card whose confirmed permanent Delete hit the retryable 503 fence
        // re-sends that same request (force kept): the user already confirmed
        // it, and the server contract for the fence is "retry".
        const force = this.deleteRetries().get(thread.id);
        if (force !== undefined) {
            void this.sendPermanentDelete(thread, force);
            return;
        }
        // Open the themed confirmation dialog instead of a native confirm().
        this.pendingDelete.set(thread);
        this.confirmDeleteOpen.set(true);
    }

    async confirmDelete(): Promise<void> {
        const thread = this.pendingDelete();
        if (!thread) return;
        this.confirmDeleteOpen.set(false);
        await this.sendPermanentDelete(thread, false);
    }

    async confirmForceDelete(): Promise<void> {
        const thread = this.pendingDelete();
        if (!thread) return;
        this.confirmForceOpen.set(false);
        await this.sendPermanentDelete(thread, true);
    }

    private async sendPermanentDelete(thread: Thread, force: boolean): Promise<void> {
        const query = force ? 'permanent=true&force=true' : 'permanent=true';
        try {
            await firstValueFrom(
                this.http.delete(`${environment.apiUrl}/persistent/threads/${thread.id}?${query}`)
            );
            this.setDeleteRetry(thread.id, null);
        } catch (e: any) {
            // Mid-turn guard (session_silent_failure_audit.md #11): a
            // cleanup sweep used to tear down live sessions silently.
            if (
                !force &&
                e?.status === 409 &&
                e?.error?.detail?.code === 'turn_in_flight'
            ) {
                // Live/mid-turn session — escalate to a force-delete confirm.
                this.pendingDelete.set(thread);
                this.confirmForceOpen.set(true);
                return;
            }
            if (e?.status !== 503) {
                this.setDeleteRetry(thread.id, null);
                this.toast.danger(this.errors.translate(e, 'errors.sessions.deleteFailed'));
                return;
            }
            // The retirement fence: the delete may already have begun the
            // card's retirement (it turns `ending`), so keep Delete usable on
            // it as the retry the server asks for.
            this.setDeleteRetry(thread.id, force);
            this.toast.warning(this.transloco.translate('errors.sessions.deleteRetryable'));
        }
        // Accepted or fenced, the card has moved (gone, or `ending` while the
        // server finishes): re-read quietly and restart the poll from its
        // shortest interval.
        this.cancelEndingPoll();
        this.endingPollDelayMs = ENDING_POLL_INITIAL_MS;
        await this.refreshThreads();
    }

    goToDraft(): void {
        // Instant landing: `/` is an open draft chat — type first, the
        // session is created on send (knowledge-base/knowledge/features/instant_landing_session.md).
        this.router.navigate(['/']);
    }

    returnToActive(): void {
        const threadId = this.chat.threadId();
        if (threadId) {
            this.router.navigate(['/sessions', threadId]);
        }
    }

    private loadSavedSessionModel(): string {
        try {
            return localStorage.getItem('default_session_model') ?? '';
        } catch {
            return '';
        }
    }

    private persistSessionModel(model: string): void {
        // UI-only preselect (localStorage). Does NOT write account preferences —
        // a per-session control must not set a global default. See Layer 2 in
        // loop_ran_codex_spark_not_selected_model_then_hung_on_cooldown.md.
        try {
            localStorage.setItem('default_session_model', model);
        } catch { /* localStorage may be unavailable */ }
    }
}
