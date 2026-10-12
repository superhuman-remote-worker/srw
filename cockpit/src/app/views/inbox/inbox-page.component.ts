import {
    Component,
    computed,
    HostListener,
    inject,
    NgZone,
    OnDestroy,
    OnInit,
    signal,
    TemplateRef,
    ViewChild,
} from '@angular/core';
import {NgTemplateOutlet} from '@angular/common';
import {ActivatedRoute} from '@angular/router';
import {TranslocoPipe, TranslocoService} from '@jsverse/transloco';
import {ViewportService} from '../../core/services/viewport.service';
import {SidebarService} from '../../core/services/sidebar.service';
import {RailTakeoverService} from '../../core/services/rail-takeover.service';
import {ActionCenterService} from '../../core/services/action-center.service';
import {NotificationService} from '../../core/services/notification.service';
import {ActionItem} from '../../core/models/action.model';
import {KNOWN_CATEGORIES, SourceRef} from '../../core/models/notification.model';
import {SidebarToggleComponent} from '../../shell/sidebar-toggle/sidebar-toggle.component';
import {AppChipComponent} from '../../ui/chip';
import {AppIconButtonComponent} from '../../ui/icon-button';
import {AppBadgeComponent} from '../../ui/badge';
import {AppDialogComponent} from '../../ui/dialog';
import {AppButtonComponent} from '../../ui/button';
import {AppIconComponent} from '../../ui/icon';
import {categoryIcon, NotificationDetailComponent} from './notification-detail.component';
import {SeenObserverDirective} from './seen-observer.directive';

/**
 * `now` is passed in (a signal read once per tick) rather than `Date.now()`
 * so the label is stable within one change-detection pass — a minute
 * boundary crossed between dev-mode's two passes is NG0100 otherwise.
 */
function relativeTime(iso: string, nowLabel: string, now: number): string {
  if (!iso) return '';
  const diff = now - new Date(iso).getTime();
  const mins = Math.floor(diff / 60000);
  if (mins < 1) return nowLabel;
  if (mins < 60) return `${mins}m`;
  const hrs = Math.floor(mins / 60);
  if (hrs < 24) return `${hrs}h`;
  return `${Math.floor(hrs / 24)}d`;
}

/**
 * The action center over the unified notification feed. Every row is a
 * feed notification; the chips are its categories (server counts), the
 * detail pane renders the row's declared actions and its source payload
 * (`NotificationDetailComponent`). The legacy email deep links
 * (`?sudo=`, `?job=&thread=`, `?job=&review=1`) resolve to the feed row
 * whose `source_ref` matches.
 */
@Component({
  selector: 'app-inbox-page',
  standalone: true,
  imports: [
    NgTemplateOutlet,
    SidebarToggleComponent,
    TranslocoPipe,
    AppChipComponent,
    AppIconButtonComponent,
    AppBadgeComponent,
    AppDialogComponent,
    AppButtonComponent,
    AppIconComponent,
    NotificationDetailComponent,
    SeenObserverDirective,
  ],
  template: `
    <!-- The feed's parts, rendered in the page or — on desktop with the rail
         open — lent to the rail, so the page never needs a list column of its
         own beside it (navigation_fixed_rail.md F9). -->
    <ng-template #chipsTpl>
      <app-chip
        [selected]="activeFilter() === null"
        (clicked)="setFilter(null)"
      >
        {{ 'inbox.filters.all' | transloco }}
        @if (actionCenter.counts().total > 0) {
          <span class="chip-count">{{ actionCenter.counts().total }}</span>
        }
      </app-chip>
      @for (chip of categoryChips(); track chip.category) {
        <app-chip
          [selected]="activeFilter() === chip.category"
          (clicked)="setFilter(chip.category)"
        >
          <app-icon size="sm">{{ chip.icon }}</app-icon>
          {{ ('notifications.category.' + chip.category) | transloco }}
          @if (chip.pending > 0) {
            <span class="chip-count">{{ chip.pending }}</span>
          }
        </app-chip>
      }
    </ng-template>

    <ng-template #statusTpl>
      <span
        class="sse-dot"
        [class.connected]="notifications.isConnected()"
        [title]="(notifications.isConnected() ? 'inbox.sseConnected' : 'inbox.sseDisconnected') | transloco"
      ></span>
      <app-icon-button
        size="sm"
        [ariaLabel]="'inbox.refresh' | transloco"
        [tooltip]="'inbox.refresh' | transloco"
        (clicked)="refresh()"
      >
        <app-icon size="sm">refresh</app-icon>
      </app-icon-button>
    </ng-template>

    <ng-template #feedTpl>
      @if (filteredItems().length === 0) {
        <div class="empty-list">
          <app-icon size="inherit" class="empty-icon">inbox</app-icon>
          <span class="empty-text">
            @if (activeFilter()) {
              {{ 'inbox.list.emptyCategory' | transloco }}
            } @else {
              {{ 'inbox.list.emptyAll' | transloco }}
            }
          </span>
        </div>
      } @else {
        @for (item of filteredItems(); track item.id; let i = $index) {
          <button
            class="list-item"
            [class.selected]="selectedItem()?.id === item.id"
            [class.pending]="item.status === 'pending'"
            [class.resolved]="item.status === 'resolved'"
            [attr.data-index]="i"
            [appSeenObserver]="item.notification.id"
            (click)="selectItem(item)"
          >
            <div class="item-urgency-bar" [class]="'urgency-' + urgencyColor(item)"></div>
            <app-icon size="lg" class="item-type-icon" [class]="'cat-' + item.category">
              {{ iconFor(item) }}
            </app-icon>
            <div class="item-content">
              <div class="item-title-row">
                <span class="item-title">{{ item.title }}</span>
                @if (!item.notification.seen_at) {
                  <span class="unread-dot"></span>
                }
                <span class="item-time">{{ relativeTime(item.timestamp) }}</span>
              </div>
              <div class="item-subtitle">
                @if (item.status === 'resolved') {
                  <app-badge tone="neutral" size="xs" [uppercase]="true">
                    {{ 'inbox.list.resolved' | transloco }}
                  </app-badge>
                } @else if (item.notification.severity === 'critical' || item.notification.severity === 'high') {
                  <app-badge [tone]="item.notification.severity === 'critical' ? 'danger' : 'warning'" size="xs" [uppercase]="true">
                    {{ ('notifications.severity.' + item.notification.severity) | transloco }}
                  </app-badge>
                }
                <span class="item-subtitle-text">{{ item.subtitle || (('notifications.category.' + item.category) | transloco) }}</span>
              </div>
            </div>
          </button>
        }
        @if (notifications.feedNextBefore()) {
          <div class="load-more">
            <app-button variant="ghost" size="sm" (clicked)="actionCenter.loadMore()">
              {{ 'notifications.loadMore' | transloco }}
            </app-button>
          </div>
        }
      }
    </ng-template>

    <!-- What the rail shows: the feed with its own title, status and filters. -->
    <ng-template #railFeed>
      <div class="rail-feed-head">
        <h1 class="header-title">{{ 'inbox.title' | transloco }}</h1>
        <div class="header-right"><ng-container [ngTemplateOutlet]="statusTpl" /></div>
      </div>
      <div class="filter-chips filter-chips--wrap"><ng-container [ngTemplateOutlet]="chipsTpl" /></div>
      <div class="rail-feed" role="feed" [attr.aria-label]="'inbox.listAriaLabel' | transloco">
        <ng-container [ngTemplateOutlet]="feedTpl" />
      </div>
    </ng-template>

    <div class="inbox">
      @if (!listInRail()) {
        <header class="inbox-header">
          <div class="header-left">
            <app-sidebar-toggle />
            @if (isMobileDetail()) {
              <app-icon-button
                size="sm"
                [ariaLabel]="'inbox.backBtn' | transloco"
                [tooltip]="'inbox.backBtn' | transloco"
                (clicked)="deselect()"
              >
                <app-icon size="sm">arrow_back</app-icon>
              </app-icon-button>
            }
            <h1 class="header-title">{{ 'inbox.title' | transloco }}</h1>
          </div>
          <div class="filter-chips"><ng-container [ngTemplateOutlet]="chipsTpl" /></div>
          <div class="header-right"><ng-container [ngTemplateOutlet]="statusTpl" /></div>
        </header>
      }

      <!-- Body: list + detail, or the detail alone while the rail holds the list -->
      <div class="inbox-body" [class.mobile-detail]="isMobileDetail()">
        @if (!listInRail()) {
          <div class="list-panel" role="feed" [attr.aria-label]="'inbox.listAriaLabel' | transloco">
            <ng-container [ngTemplateOutlet]="feedTpl" />
          </div>
        }

        <div class="detail-panel">
          @if (selectedItem(); as item) {
            <app-notification-detail [item]="item" />
          } @else {
            <div class="detail-empty">
              <app-icon size="inherit" class="detail-empty-icon">select_all</app-icon>
              <span class="detail-empty-text">{{ 'inbox.detail.emptyText' | transloco }}</span>
              <span class="detail-empty-hint">
                {{ 'inbox.detail.hintUse' | transloco }} <kbd>j</kbd>/<kbd>k</kbd> {{ 'inbox.detail.hintToNavigate' | transloco }} <kbd>Enter</kbd> {{ 'inbox.detail.hintToSelect' | transloco }}
              </span>
            </div>
          }
        </div>
      </div>
    </div>

    <!-- Keyboard shortcuts dialog -->
    <app-dialog
      [open]="showShortcuts()"
      size="md"
      [title]="'inbox.shortcuts.title' | transloco"
      (closed)="showShortcuts.set(false)"
    >
      <div class="shortcuts-grid">
        <kbd>j</kbd><span>{{ 'inbox.shortcuts.nextItem' | transloco }}</span>
        <kbd>k</kbd><span>{{ 'inbox.shortcuts.prevItem' | transloco }}</span>
        <kbd>Enter</kbd><span>{{ 'inbox.shortcuts.selectItem' | transloco }}</span>
        <kbd>Escape</kbd><span>{{ 'inbox.shortcuts.deselect' | transloco }}</span>
        <kbd>1</kbd>–<kbd>9</kbd><span>{{ 'inbox.shortcuts.filterCategory' | transloco }}</span>
        <kbd>0</kbd><span>{{ 'inbox.shortcuts.clearFilter' | transloco }}</span>
        <kbd>?</kbd><span>{{ 'inbox.shortcuts.toggleOverlay' | transloco }}</span>
      </div>
      <ng-container appDialogActions>
        <app-button variant="ghost" size="sm" (clicked)="showShortcuts.set(false)">
          {{ 'inbox.shortcuts.close' | transloco }}
        </app-button>
      </ng-container>
    </app-dialog>
  `,
  styles: [`
    :host { display: block; height: 100%; }

    .inbox {
      display: flex;
      flex-direction: column;
      height: 100%;
      background: var(--app-bg);
    }

    /* ===== HEADER ===== */

    .inbox-header {
      display: flex;
      align-items: center;
      gap: 12px;
      padding: 8px 16px;
      border-bottom: 1px solid var(--surface-0);
      flex-shrink: 0;
      min-height: 44px;
    }

    .header-left {
      display: flex;
      align-items: center;
      gap: 8px;
      flex-shrink: 0;
    }

    .header-title {
      margin: 0;
      font-size: 15px;
      font-weight: 600;
      color: var(--text-primary);
      white-space: nowrap;
    }

    .filter-chips {
      display: flex;
      gap: 4px;
      flex: 1;
      overflow-x: auto;
      scrollbar-width: none;
    }
    .filter-chips::-webkit-scrollbar { display: none; }

    .chip-count {
      background: var(--accent-color);
      color: var(--on-accent, var(--timeline-bg));
      font-size: 9px;
      font-weight: 700;
      padding: 0 5px;
      border-radius: var(--radius-pill);
      min-width: 14px;
      text-align: center;
      line-height: 14px;
    }

    .header-right {
      display: flex;
      align-items: center;
      gap: 6px;
      flex-shrink: 0;
    }

    .sse-dot {
      width: 7px;
      height: 7px;
      border-radius: 50%;
      background: var(--text-muted);
      transition: background 0.3s;
    }
    .sse-dot.connected { background: var(--success); }

    /* ===== FEED IN THE RAIL ===== */

    /* These rules travel with the template: the rail renders the page's own
       template, so the page's scoped styles still apply there. */
    .rail-feed-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
      padding: 8px 12px 6px 16px;
    }

    .filter-chips--wrap {
      flex-wrap: wrap;
      overflow: visible;
      padding: 0 12px 10px;
    }

    .rail-feed {
      border-top: 1px solid var(--surface-0);
    }

    /* ===== BODY: TWO-PANEL ===== */

    .inbox-body {
      display: flex;
      flex: 1;
      overflow: hidden;
    }

    .list-panel {
      width: 360px;
      min-width: 280px;
      flex-shrink: 0;
      border-right: 1px solid var(--surface-0);
      overflow-y: auto;
      scrollbar-width: thin;
      scrollbar-color: var(--surface-0) transparent;
    }

    .detail-panel {
      flex: 1;
      overflow-y: auto;
      scrollbar-width: thin;
      scrollbar-color: var(--surface-0) transparent;
    }

    /* ===== LIST ITEMS ===== */

    .list-item {
      display: flex;
      align-items: flex-start;
      gap: 10px;
      width: 100%;
      padding: 10px 12px 10px 0;
      background: transparent;
      border: none;
      border-bottom: 1px solid var(--surface-0);
      cursor: pointer;
      text-align: left;
      font-family: inherit;
      color: var(--text-primary);
      transition: background 0.1s;
      position: relative;
    }
    .list-item:hover { background: color-mix(in srgb, var(--surface-1) 40%, transparent); }
    .list-item.selected { background: var(--surface-0); }
    .list-item.resolved { opacity: 0.55; }

    .item-urgency-bar {
      width: 3px;
      align-self: stretch;
      border-radius: 0 var(--radius-tag) var(--radius-tag) 0;
      flex-shrink: 0;
    }
    .urgency-green { background: var(--success); }
    .urgency-amber { background: var(--warning); }
    .urgency-red { background: var(--danger); }
    .urgency-muted { background: var(--surface-0); }

    .item-type-icon {
      font-size: 18px;
      flex-shrink: 0;
      margin-top: 1px;
      color: var(--text-muted);
    }
    .cat-agent_message, .cat-officer_question { color: var(--info); }
    .cat-sudo_request, .cat-session_permission { color: var(--alert); }
    .cat-review_queue { color: var(--success); }
    .cat-session_wake, .cat-loop_event { color: var(--accent-color); }
    .cat-incident, .cat-vm_upgrade { color: var(--danger); }

    .item-content { flex: 1; min-width: 0; }

    .item-title-row {
      display: flex;
      align-items: center;
      gap: 6px;
    }

    .item-title {
      font-size: 12px;
      font-weight: 500;
      color: var(--text-primary);
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
      flex: 1;
      min-width: 0;
    }

    .unread-dot {
      width: 6px;
      height: 6px;
      border-radius: 50%;
      background: var(--info);
      flex-shrink: 0;
    }

    .item-time {
      font-size: 10px;
      color: var(--text-muted);
      flex-shrink: 0;
      font-variant-numeric: tabular-nums;
    }

    .item-subtitle {
      font-size: 11px;
      color: var(--text-muted);
      margin-top: 2px;
      display: flex;
      align-items: center;
      gap: 4px;
      min-width: 0;
    }

    /* The ellipsis has to live on a real element: text-overflow never applies
       to the anonymous flex item a bare text node becomes, so the subtitle
       used to clip mid-word at the pane edge. */
    .item-subtitle-text {
      flex: 1;
      min-width: 0;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }

    .load-more {
      display: flex;
      justify-content: center;
      padding: 10px;
    }

    /* ===== EMPTY STATES ===== */

    .empty-list {
      display: flex;
      flex-direction: column;
      align-items: center;
      justify-content: center;
      padding: 60px 20px;
      gap: 8px;
    }

    .empty-icon {
      font-size: 40px;
      color: var(--surface-0);
    }

    .empty-text {
      font-size: 13px;
      color: var(--text-muted);
    }

    .detail-empty {
      display: flex;
      flex-direction: column;
      align-items: center;
      justify-content: center;
      height: 100%;
      gap: 8px;
    }

    .detail-empty-icon {
      font-size: 48px;
      color: var(--surface-0);
    }

    .detail-empty-text {
      font-size: 14px;
      color: var(--text-muted);
    }

    .detail-empty-hint {
      font-size: 11px;
      color: var(--text-muted);
      opacity: 0.6;
    }

    .detail-empty-hint kbd {
      display: inline-block;
      padding: 1px 5px;
      background: var(--surface-0);
      border-radius: var(--radius-tag);
      font-size: 10px;
      font-family: inherit;
    }

    /* ===== DETAIL ===== */

    /* With the feed in the rail the pane spans the whole content area; keep
       the detail at a readable measure instead of stretching its lines. */
    app-notification-detail {
      display: block;
      max-width: var(--content-max-width);
      padding: 16px 20px;
    }

    /* ===== SHORTCUTS DIALOG BODY ===== */

    .shortcuts-grid {
      display: grid;
      grid-template-columns: auto 1fr;
      gap: 6px 14px;
    }

    .shortcuts-grid kbd {
      display: inline-block;
      padding: 2px 7px;
      background: var(--surface-0);
      border-radius: var(--radius-tag);
      font-size: 11px;
      font-family: inherit;
      color: var(--accent-color);
      text-align: center;
      min-width: 24px;
    }

    .shortcuts-grid span {
      font-size: 12px;
      color: var(--text-secondary);
      padding-top: 2px;
    }

    /* ===== RESPONSIVE ===== */

    @media (max-width: 768px) {
      .list-panel {
        width: 100%;
        min-width: unset;
        border-right: none;
      }
      .detail-panel { display: none; }
      .inbox-body.mobile-detail .list-panel { display: none; }
      .inbox-body.mobile-detail .detail-panel { display: block; }

      /* Give the filter chips their own full-width row instead of a sliver
         squeezed between the title and the SSE/refresh cluster. Title +
         status stay on row 1; the chips wrap to row 2 and scroll there. */
      .inbox-header { flex-wrap: wrap; row-gap: 6px; }
      .header-right { margin-left: auto; }
      .filter-chips { gap: 3px; order: 3; flex-basis: 100%; }
    }
  `],
})
export class InboxPageComponent implements OnInit, OnDestroy {
  readonly actionCenter = inject(ActionCenterService);
  readonly notifications = inject(NotificationService);
  private readonly route = inject(ActivatedRoute);
  private readonly zone = inject(NgZone);
  private readonly transloco = inject(TranslocoService);

  // --- State ---
  /** Category filter (a `notifications.category.*` key), or null for all. */
  readonly activeFilter = signal<string | null>(null);
  readonly selectedItem = signal<ActionItem | null>(null);
  readonly showShortcuts = signal(false);

  // Relative-time / countdown tick
  private countdownTimer: ReturnType<typeof setInterval> | null = null;
  readonly tick = signal(0);
  /** Wall clock the relative-time labels are computed against; advanced by the tick. */
  readonly now = signal(Date.now());

  // Navigation
  private focusIndex = -1;

  /**
   * Category chips: every category with a server count or a loaded row, in
   * the catalog's display order; categories the cockpit does not know yet
   * append at the end and render with the generic bell.
   */
  readonly categoryChips = computed(() => {
    const byCategory = this.actionCenter.counts().byCategory;
    const present = new Set<string>(Object.keys(byCategory));
    for (const item of this.actionCenter.items()) present.add(item.category);
    const rank = (c: string) => {
      const i = KNOWN_CATEGORIES.indexOf(c);
      return i === -1 ? KNOWN_CATEGORIES.length : i;
    };
    return Array.from(present)
      .sort((a, b) => rank(a) - rank(b) || a.localeCompare(b))
      .map((category) => ({
        category,
        icon: categoryIcon(category),
        pending: byCategory[category]?.pending ?? 0,
      }));
  });

  readonly filteredItems = computed(() => {
    // Touch tick to force re-evaluation for relative times
    this.tick();
    const filter = this.activeFilter();
    const items = this.actionCenter.items();
    if (!filter) return items;
    return items.filter((i) => i.category === filter);
  });

  private readonly viewport = inject(ViewportService);
  readonly isMobile = this.viewport.isMobile;
  readonly isMobileDetail = computed(() => {
    return !!this.selectedItem() && this.isMobile();
  });

  private readonly sidebar = inject(SidebarService);
  private readonly takeover = inject(RailTakeoverService);

  /**
   * The feed lives in the rail on desktop while the rail is open, and in the
   * page otherwise: on a phone the rail is a drawer the user has to open, and
   * a collapsed rail would hide the feed altogether.
   */
  readonly listInRail = computed(() => !this.isMobile() && !this.sidebar.collapsed());

  /** Top level of the template, so it resolves before ngOnInit. */
  @ViewChild('railFeed', {static: true}) private readonly railFeed?: TemplateRef<unknown>;

  ngOnInit(): void {
    // SSE is opened at app-shell init (App constructor effect) so it's
    // already up by the time the user navigates to Inbox.
    this.actionCenter.refreshAll();

    // The same condition that stops the page rendering the feed, so the feed
    // is always in exactly one place as the viewport or the rail changes.
    if (this.railFeed) this.takeover.claim(this.railFeed, this.listInRail);

    // Tick every second — run outside Angular zone to avoid
    // ExpressionChangedAfterItHasBeenChecked errors, then re-enter zone
    this.zone.runOutsideAngular(() => {
      this.countdownTimer = setInterval(() => {
        this.zone.run(() => {
          this.now.set(Date.now());
          this.tick.update((t) => t + 1);
        });
      }, 1000);
    });

    // Deep-link support: `?n=<id>` is what every email carries now; the
    // older links name a source and still work.
    this.route.queryParams.subscribe((params) => {
      const notificationId = params['n'];
      const jobId = params['job'];
      const threadId = params['thread'];
      const sudoId = params['sudo'];
      const review = params['review'];

      if (notificationId) {
        this.trySelectNotification(notificationId);
      } else if (sudoId) {
        this.trySelectBySource([{kind: 'sudo_request', id: sudoId}]);
      } else if (jobId && threadId) {
        // An agent message thread, or an officer page keyed to its session
        // (the old `?job={thread}&thread={thread}` shape).
        this.trySelectBySource([
          {kind: 'message_thread', id: threadId},
          {kind: 'thread', id: threadId},
        ]);
      } else if (jobId && review) {
        this.trySelectBySource([{kind: 'job', id: jobId}]);
      }
    });
  }

  ngOnDestroy(): void {
    if (this.countdownTimer) clearInterval(this.countdownTimer);
    if (this.railFeed) this.takeover.release(this.railFeed);
  }

  // --- Deep-link ---

  /**
   * `?n=<id>`: the row is normally in the loaded page; otherwise fetch it
   * by id and upsert.
   */
  private trySelectNotification(notificationId: string): void {
    const id = `ntf:${notificationId}`;
    const item = this.actionCenter.items().find((i) => i.id === id);
    if (item) {
      this.selectItem(item);
      return;
    }
    this.actionCenter.fetchNotification(notificationId).subscribe((row) => {
      if (!row) return;
      const fetched = this.actionCenter.items().find((i) => i.id === id);
      if (fetched) this.selectItem(fetched);
    });
  }

  /** Try each candidate source in order; the first with a feed row wins. */
  private trySelectBySource(candidates: SourceRef[]): void {
    const [head, ...rest] = candidates;
    if (!head) return;
    this.actionCenter.fetchBySource(head).subscribe((row) => {
      if (!row) {
        this.trySelectBySource(rest);
        return;
      }
      const item = this.actionCenter.items().find((i) => i.id === `ntf:${row.id}`);
      if (item) this.selectItem(item);
    });
  }

  // --- Filters ---
  setFilter(category: string | null): void {
    this.activeFilter.set(category);
  }

  refresh(): void {
    this.actionCenter.refreshAll();
  }

  // --- Selection ---
  selectItem(item: ActionItem): void {
    this.selectedItem.set(item);
    this.focusIndex = this.filteredItems().findIndex((i) => i.id === item.id);
    // Opening the row is reading it; the pane loads its own source payload.
    this.actionCenter.markRead(item.notification.id);
  }

  deselect(): void {
    this.selectedItem.set(null);
  }

  // --- Helpers ---
  iconFor(item: ActionItem): string { return categoryIcon(item.category); }
  relativeTime(iso: string): string {
    return relativeTime(iso, this.transloco.translate('inbox.time.now'), this.now());
  }

  urgencyColor(item: ActionItem): string {
    if (item.status === 'resolved') return 'muted';
    switch (item.notification.severity) {
      case 'critical': return 'red';
      case 'high': return 'amber';
      case 'normal': return 'green';
      default: return 'muted';
    }
  }

  // --- Keyboard navigation ---
  @HostListener('document:keydown', ['$event'])
  onKeydown(e: KeyboardEvent): void {
    // Ignore when typing in inputs
    const tag = (e.target as HTMLElement).tagName;
    const isInput = tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT';

    if (e.key === '?' && !isInput) {
      e.preventDefault();
      this.showShortcuts.update((v) => !v);
      return;
    }

    if (e.key === 'Escape') {
      if (this.showShortcuts()) {
        this.showShortcuts.set(false);
      } else {
        this.deselect();
      }
      return;
    }

    if (isInput) return;

    const items = this.filteredItems();
    switch (e.key) {
      case 'j':
      case 'ArrowDown':
        e.preventDefault();
        this.focusIndex = Math.min(this.focusIndex + 1, items.length - 1);
        if (items[this.focusIndex]) this.selectItem(items[this.focusIndex]);
        break;
      case 'k':
      case 'ArrowUp':
        e.preventDefault();
        this.focusIndex = Math.max(this.focusIndex - 1, 0);
        if (items[this.focusIndex]) this.selectItem(items[this.focusIndex]);
        break;
      case 'Enter':
        if (this.focusIndex >= 0 && items[this.focusIndex]) {
          this.selectItem(items[this.focusIndex]);
        }
        break;
      case '0':
        this.setFilter(null);
        break;
      default: {
        // 1–9 select the n-th category chip.
        const n = parseInt(e.key, 10);
        if (n >= 1 && n <= 9) {
          const chip = this.categoryChips()[n - 1];
          if (chip) this.setFilter(chip.category);
        }
      }
    }
  }
}
