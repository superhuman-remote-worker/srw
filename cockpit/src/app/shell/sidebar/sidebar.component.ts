import {Component, computed, ElementRef, HostListener, inject, signal, ViewChild} from '@angular/core';
import {NavigationEnd, Router, RouterLink, RouterLinkActive} from '@angular/router';
import {takeUntilDestroyed, toSignal} from '@angular/core/rxjs-interop';
import {filter, map} from 'rxjs';
import {SidebarService} from '../../core/services/sidebar.service';
import {ViewportService} from '../../core/services/viewport.service';
import {SessionListService} from '../../core/services/session-list.service';
import {LayoutService} from '../../workbench/services/layout.service';
import {LayoutPickerComponent} from '../../workbench/components/layout-picker/layout-picker.component';
import {NotificationBellComponent} from '../notification-bell/notification-bell.component';
import {PersistentChatService} from '../../core/services/persistent-chat.service';
import {environment} from '../../core/environment';
import {TranslocoPipe} from '@jsverse/transloco';
import {AppIconComponent} from '../../ui/icon';
import {LegionMarkComponent} from '../../ui/legion-mark';
import {AppTabNavComponent, AppTabNavItemComponent} from '../../ui/tab-nav';
import {RailMoreMenuComponent} from '../rail-more-menu/rail-more-menu.component';
import {RailAccountMenuComponent} from '../rail-account-menu/rail-account-menu.component';

export type RailMode = 'chat' | 'jobs' | 'projects';

const MODE_ROUTES: Record<RailMode, string> = {
  chat: '/',
  jobs: '/jobs',
  projects: '/projects',
};

@Component({
  selector: 'app-sidebar',
  standalone: true,
  imports: [RouterLink, RouterLinkActive, LayoutPickerComponent, NotificationBellComponent, TranslocoPipe, AppIconComponent, LegionMarkComponent, AppTabNavComponent, AppTabNavItemComponent, RailMoreMenuComponent, RailAccountMenuComponent],
  template: `
    <nav class="sidebar" id="sidebar-rail" (click)="onSidebarClick($event)">
      <div class="sidebar-header">
        <div class="sidebar-brand">
          <srw-legion-mark [size]="22" />
          <div class="sidebar-brand-stack">
            <span class="sidebar-logo">SRW</span>
            <span class="sidebar-label">{{ 'nav.cockpit' | transloco }}</span>
          </div>
        </div>
        <button class="collapse-btn" (click)="sidebar.collapse()" [title]="'nav.collapseSidebar' | transloco">
          <app-icon size="md" class="collapse-icon">chevron_left</app-icon>
        </button>
        <app-notification-bell />
      </div>

      <div class="sidebar-body">
        <app-tab-nav class="mode-switcher" [value]="mode()" (valueChange)="selectMode($event)">
          <app-tab-nav-item value="chat">{{ 'nav.modeChat' | transloco }}</app-tab-nav-item>
          <app-tab-nav-item value="jobs">{{ 'nav.modeJobs' | transloco }}</app-tab-nav-item>
          <app-tab-nav-item value="projects">{{ 'nav.modeProjects' | transloco }}</app-tab-nav-item>
        </app-tab-nav>

        <a class="rail-new" routerLink="/">
          <app-icon size="md">edit_square</app-icon> {{ 'nav.newChat' | transloco }}
        </a>

        @if (showFilter()) {
          <label class="rail-search">
            <app-icon size="md">search</app-icon>
            <input #filterInput type="search" [value]="filterText()"
                   (input)="filterText.set($any($event.target).value)"
                   [placeholder]="'nav.searchSessions' | transloco"
                   [attr.aria-label]="'nav.searchSessions' | transloco">
            <!-- The native ::-webkit-search-cancel-button is suppressed
                 below (it doesn't double up with this), so an empty vs.
                 filled filter needs its own way to clear — otherwise
                 clearing means select-all plus backspace. -->
            @if (filterText()) {
              <button type="button" class="rail-search-clear" (click)="clearFilter()"
                      [attr.aria-label]="'nav.clearFilter' | transloco">
                <app-icon size="sm">close</app-icon>
              </button>
            } @else {
              <kbd aria-hidden="true">⌘K</kbd>
            }
          </label>
        }

        <!-- Gated on mode(), not on sessionGroups().length — same allowlist
             reason as showFilter()/sessionGroups() above. A fresh account
             with zero sessions is exactly the user "See all sessions" must
             stay reachable for, and the empty-state copy below only makes
             sense while the session list is the thing on screen. -->
        @if (mode() === 'chat') {
          @for (group of sessionGroups(); track group.label) {
            <div class="rail-group">{{ ('nav.recency.' + group.label) | transloco }}</div>
            @for (t of group.threads; track t.id) {
              <a class="rail-item" [routerLink]="['/sessions', t.id]" routerLinkActive="active">
                {{ t.title }}
              </a>
            }
          }

          <!-- sessionGroups() is [] both when the account has no sessions
               and when the filter matched none — tell those apart, or a
               forgotten filter reads as "my sessions disappeared". -->
          @if (sessionGroups().length === 0) {
            <div class="rail-empty">{{ (hasSessions() ? 'nav.noMatches' : 'nav.noSessionsYet') | transloco }}</div>
          }

          <a class="rail-see-all" routerLink="/sessions">
            <app-icon size="sm">arrow_forward</app-icon> {{ 'nav.seeAllSessions' | transloco }}
          </a>
        }

        @if (isWorkbenchRoute()) {
          @if (adminToolsEnabled) {
          <div class="section">
            <div class="section-title">Databases</div>
            <a class="section-link" [href]="neo4jUrl" target="_blank" rel="noopener">
              <span class="link-icon">&#x1F535;</span>Neo4j Browser
            </a>
            <a class="section-link" [href]="pgadminUrl" target="_blank" rel="noopener">
              <span class="link-icon">&#x1F418;</span>PostgreSQL
            </a>
          </div>

          }
          <div class="section">
            <div class="section-title">Tools</div>
            <a class="section-link" [href]="giteaUrl" target="_blank" rel="noopener">
              <span class="link-icon">&#x1F375;</span>Gitea
            </a>
            @if (adminToolsEnabled) {
            <a class="section-link" [href]="dozzleUrl" target="_blank" rel="noopener">
              <span class="link-icon">&#x1F4CB;</span>Dozzle
            </a>
            }
            @if (adminToolsEnabled && minioConsoleUrl) {
              <a class="section-link" [href]="minioConsoleUrl" target="_blank" rel="noopener">
                <span class="link-icon">&#x1F4E6;</span>MinIO
              </a>
            }
            @if (cloudUrl) {
              <a class="section-link" [href]="cloudUrl" target="_blank" rel="noopener">
                <span class="link-icon">&#x2601;</span>Cloud
              </a>
            }
          </div>

          <div class="section">
            <div class="section-title">Layouts</div>
            <button class="section-link" #layoutBtn (click)="toggleLayoutPicker(layoutBtn)">
              <span class="link-icon">&#x1F4D0;</span>Choose Layout
            </button>
            <button class="section-link" (click)="resetLayout()">
              <span class="link-icon">&#x1F504;</span>Reset Layout
            </button>
            @if (isLayoutPickerOpen()) {
              <app-layout-picker
                [top]="pickerTop()"
                [left]="pickerLeft()"
                (closed)="closeLayoutPicker()"
              />
            }
          </div>
        }
      </div>

      <div class="sidebar-footer">
        <app-rail-more-menu />
        <div class="rail-divider"></div>
        <app-rail-account-menu />
      </div>
    </nav>
  `,
  styles: [
    `
      /* --sidebar-width is bound on this host from SidebarService (see the
         host block below); the literal is the fallback for the frame before
         the binding applies, and for SSR. */
      :host {
        display: block;
        width: var(--sidebar-width, 260px);
        flex-shrink: 0;
        overflow: hidden;
        transition: width 0.2s ease;
      }

      :host(.collapsed) {
        width: 0;
      }

      /* The width animation belongs to collapse/expand, where it reads as a
         wipe. During a drag the same transition makes the rail's edge lag the
         pointer by 200ms, which feels like the handle has come loose. */
      :host(.resizing) {
        transition: none;
      }

      .sidebar {
        display: flex;
        flex-direction: column;
        /* The same width as the host, not 100%: while the host animates to 0 on
           collapse, this keeps its width and is clipped by the host's
           overflow: hidden — a wipe. At 100% the content would reflow through
           every intermediate width instead, thrashing the title ellipses. */
        width: var(--sidebar-width, 260px);
        height: 100%;
        background: var(--panel-bg);
        border-right: 1px solid var(--border-color);
      }

      .sidebar-header {
        display: flex;
        align-items: center;
        /* 6px, not 8px: the row now holds three items instead of two (Task
           10 fix round 1), and the two fixed-size icon controls
           (.collapse-btn, the notification bell) are flex: none — they no
           longer absorb a tight fit by shrinking, so the space this row was
           short on desktop has to come from chrome instead. Combined with
           the padding trim below, this leaves ~6px of real slack rather
           than an exact, zero-margin fit — measured in a real browser
           against the compiled CSS (see task-10-report.md fix round 1).
           A hairline fit was reachable with padding alone, but --font-display
           has a 4-deep fallback chain (Cinzel → Cormorant Garamond → Times
           New Roman → serif) and font-display: swap, so the brand text's
           actual width can shift slightly if the primary webfont hasn't
           loaded yet — a razor's-edge fit isn't worth it for ~2px. */
        gap: 6px;
        /* 16px 12px, not a flat 16px: horizontal only, so row height
           (vertical rhythm) is unchanged. */
        padding: 16px 12px;
        border-bottom: 1px solid var(--border-hairline);
        flex-shrink: 0;
      }

      .sidebar-brand {
        display: flex;
        align-items: center;
        gap: 10px;
        color: var(--accent-color);
      }

      /* Task 10 fix round 1 gave flex: none to the row's two icon controls
         (.collapse-btn, the notification bell) but not to this block,
         leaving it the row's only shrinkable item. --font-display has a
         4-deep webfont fallback chain (see the .sidebar-header comment
         above), so under a wide fallback face the mark squeezed before the
         text gave up any width — flex: none on the mark's host and
         min-width: 0 on the stack flip that: the mark never shrinks, and
         the text is the thing that gives. */
      .sidebar-brand srw-legion-mark {
        flex: none;
      }

      .sidebar-brand-stack {
        display: flex;
        flex-direction: column;
        line-height: 1;
        gap: 3px;
        min-width: 0;
      }

      .sidebar-logo {
        font-family: var(--font-display, inherit);
        font-size: 18px;
        font-weight: 700;
        color: var(--accent-color);
        letter-spacing: 1px; /* brand */
      }

      .sidebar-label {
        font-family: var(--font-display, inherit);
        font-size: 11px;
        letter-spacing: 0.18em; /* brand */
        text-transform: uppercase;
        color: var(--text-muted);
      }

      .collapse-btn {
        margin-left: auto;
        /* Fixed-size icon control: never let the header's flexbox shrink
           this to make room for a sibling. Task 10 fix round 1 — before
           this, default flex-shrink: 1 (no flex shorthand here) let the
           row squeeze this to ~22x28 once the bell became a third sibling. */
        flex: none;
        display: flex;
        align-items: center;
        justify-content: center;
        width: 28px;
        height: 28px;
        background: transparent;
        border: none;
        border-radius: var(--radius-control);
        color: var(--text-muted);
        cursor: pointer;
        padding: 0;
        transition:
          color 0.15s ease,
          background 0.15s ease;
      }

      .collapse-btn:hover {
        color: var(--text-primary);
        background: var(--surface-0);
      }


      .sidebar-body {
        flex: 1;
        overflow-y: auto;
        scrollbar-width: thin;
        scrollbar-color: var(--border-color) transparent;
      }

      .mode-switcher {
        margin: 8px;
      }

      /* Three modes share one row in the rail, 200px wide at its narrowest.
         The tab primitive's 20px side padding plus wrap-on-overflow pushed
         "Projects" onto a second line; distribute the slack across the items
         instead. */
      .mode-switcher[data-orientation='horizontal'] {
        flex-wrap: nowrap;
      }

      .mode-switcher ::ng-deep app-tab-nav-item {
        flex: 1 1 auto;
        min-width: 0;
        justify-content: center;
        padding-inline: 6px;
        white-space: nowrap;
      }

      /* Rail session list: the Chat mode's "New chat" action and the
         recency-grouped thread list that fills the space below the
         switcher. */

      .rail-new {
        display: flex;
        align-items: center;
        gap: 10px;
        margin: 0 8px;
        padding: 8px 12px;
        border-radius: var(--radius-control);
        color: var(--text-secondary);
        text-decoration: none;
        font-size: 13px;
        transition:
          background 0.15s ease,
          color 0.15s ease;
      }

      .rail-new:hover {
        background: var(--surface-0);
        color: var(--text-primary);
      }

      .rail-search {
        display: flex;
        align-items: center;
        gap: 8px;
        margin: 4px 8px 8px;
        padding: 7px 10px;
        border: 1px solid var(--border-color);
        border-radius: var(--radius-control);
        color: var(--text-muted);
        transition: border-color 0.15s ease;
      }

      .rail-search:focus-within {
        border-color: var(--accent-color);
      }

      .rail-search input {
        flex: 1;
        min-width: 0;
        border: none;
        outline: none;
        background: transparent;
        font: inherit;
        font-size: 13px;
        color: var(--text-primary);
        text-overflow: ellipsis;
      }

      .rail-search input::placeholder {
        color: var(--text-muted);
        text-overflow: ellipsis;
      }

      /* The custom icon + <kbd> hint own this affordance — suppress the
         native search-field magnifier/clear-button decorations so they
         don't double up. */
      .rail-search input[type='search']::-webkit-search-decoration,
      .rail-search input[type='search']::-webkit-search-cancel-button,
      .rail-search input[type='search']::-webkit-search-results-button,
      .rail-search input[type='search']::-webkit-search-results-decoration {
        -webkit-appearance: none;
      }

      .rail-search kbd {
        flex-shrink: 0;
        padding: 1px 5px;
        border: 1px solid var(--border-color);
        border-radius: var(--radius-tag);
        font-family: inherit;
        font-size: 10px;
        line-height: 1.4;
        color: var(--text-muted);
      }

      /* Replaces the suppressed native ::-webkit-search-cancel-button —
         occupies the same slot the <kbd> hint does when there's nothing to
         clear. */
      .rail-search-clear {
        flex-shrink: 0;
        display: flex;
        align-items: center;
        justify-content: center;
        width: 20px;
        height: 20px;
        padding: 0;
        border: none;
        border-radius: var(--radius-control);
        background: transparent;
        color: var(--text-muted);
        cursor: pointer;
        transition:
          color 0.15s ease,
          background 0.15s ease;
      }

      .rail-search-clear:hover {
        color: var(--text-primary);
        background: var(--surface-0);
      }

      .rail-group {
        margin: 0 8px;
        padding: 12px 4px 4px;
        font-size: 11px;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.06em;
        color: var(--text-muted);
      }

      .rail-item {
        display: block;
        margin: 0 8px;
        padding: 8px 12px;
        border-radius: var(--radius-control);
        color: var(--text-secondary);
        text-decoration: none;
        font-size: 13px;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
        transition:
          background 0.15s ease,
          color 0.15s ease;
      }

      .rail-item:hover {
        background: var(--surface-0);
        color: var(--text-primary);
      }

      .rail-item.active {
        background: var(--surface-0);
        color: var(--accent-color);
        font-weight: 600;
      }

      /* sessionGroups() is [] both for "no sessions yet" and "no matches" —
         this is the copy that tells them apart. */
      .rail-empty {
        margin: 4px 8px;
        padding: 8px 12px;
        font-size: 12px;
        color: var(--text-muted);
      }

      /* Bulk-management escape hatch: routes to /sessions, the only
         discoverable path there (see the finding this fixes). Styled as an
         action row like .rail-new, not a session row like .rail-item. */
      .rail-see-all {
        display: flex;
        align-items: center;
        gap: 10px;
        margin: 4px 8px 0;
        padding: 8px 12px;
        border-radius: var(--radius-control);
        color: var(--text-secondary);
        text-decoration: none;
        font-size: 13px;
        transition:
          background 0.15s ease,
          color 0.15s ease;
      }

      .rail-see-all:hover {
        background: var(--surface-0);
        color: var(--text-primary);
      }

      /* Mobile drawer sizing: the desktop rail's width/13px type reads cramped
         as an overlay drawer. Widen it (capped below the viewport so the
         backdrop stays tappable) and scale the type/targets for thumbs. The
         width:0 collapse still wins via :host(.collapsed) specificity,
         unchanged. Overriding the width property outright — rather than
         --sidebar-width — is what keeps the drawer off the resizable desktop
         width: a drawer sized by a handle the shell doesn't even render below
         this breakpoint would be a width the user can't get back. */
      @media (max-width: 768px) {
        :host,
        .sidebar {
          width: min(300px, 84vw);
        }

        .sidebar-logo {
          font-size: 20px;
        }

        .sidebar-label {
          font-size: 12px;
        }

        /* Tap-target restoration (Task 8 step 5): Task 6 deleted the old flat
           nav's .nav-link rule — min-height: 44px; padding: 10px 14px;
           gap: 12px — along with the links it sized. Every control the rail
           has grown since (Tasks 6, 7, this one, 12, and the "See all
           sessions" row) needs that minimum back. .rail-new, .rail-item,
           .rail-search (Task 12's filter label — the whole label focuses the
           input on tap, an implicit label/input association, so sizing the
           label covers the target) and .rail-see-all are rendered directly
           in this template, so one rule reaches all four. The More and
           avatar triggers (.rail-nav, .rail-account) are owned by their own
           components now and restore this same rule in their own
           stylesheets — Emulated encapsulation means a rule here can't reach
           into their templates. The mode switcher's tabs are
           app-tab-nav-item, a shared ui/ component with the same
           encapsulation boundary; ::ng-deep reaches its host element,
           scoped under .mode-switcher so the other app-tab-nav consumers
           (admin-models, agent-settings) are unaffected. */
        .rail-new,
        .rail-item,
        .rail-search,
        .rail-see-all {
          min-height: 44px;
          padding: 10px 14px;
          gap: 12px;
        }

        .mode-switcher ::ng-deep app-tab-nav-item {
          min-height: 44px;
          padding: 10px 14px;
          gap: 12px;
        }
      }

      /* Workbench sections */

      .section {
        padding: 8px;
        border-top: 1px solid var(--border-hairline);
      }

      .section-title {
        font-size: 11px;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.06em;
        color: var(--text-muted);
        padding: 4px 8px 6px;
        margin: 0;
      }

      .section-link {
        display: flex;
        align-items: center;
        gap: 8px;
        padding: 6px 12px;
        border-radius: var(--radius-control);
        color: var(--text-secondary);
        text-decoration: none;
        font-size: 12px;
        cursor: pointer;
        border: none;
        background: transparent;
        width: 100%;
        text-align: left;
        font-family: inherit;
        transition:
          background 0.15s ease,
          color 0.15s ease;
      }

      .section-link:hover {
        background: var(--surface-0);
        color: var(--text-primary);
      }

      .link-icon {
        font-size: 14px;
        width: 18px;
        text-align: center;
        flex-shrink: 0;
      }

      /* Footer */

      .sidebar-footer {
        padding: 12px;
        border-top: 1px solid var(--border-hairline);
        display: flex;
        flex-direction: column;
        gap: 8px;
        flex-shrink: 0;
      }

      .rail-divider {
        border-top: 1px solid var(--border-hairline);
      }
    `,
  ],
  host: {
    // The rail's width, as a CSS custom property both :host and .sidebar read.
    // A custom property rather than a direct [style.width]: an inline width
    // would beat the collapse and mobile-drawer rules that have to override it.
    '[style.--sidebar-width]': 'sidebar.widthPx()',
    '[class.resizing]': 'sidebar.resizing()',
  },
})
export class SidebarComponent {
  readonly sidebar = inject(SidebarService);
  readonly layoutService = inject(LayoutService);
  private readonly router = inject(Router);
  private readonly chatService = inject(PersistentChatService);
  readonly viewport = inject(ViewportService);
  private readonly sessions = inject(SessionListService);

  readonly mode = computed<RailMode | null>(() => {
    // Allowlist, deliberately not a fallback: a route that is none of the three
    // modes must light no tab, rather than defaulting to Chat and telling the
    // user they are somewhere they are not. Routes outside these three
    // (/experts, /settings, /admin/*, ...) are reached from the More and avatar
    // menus and have no mode of their own.
    // router.url carries the query string and fragment (e.g. '/?foo=bar') —
    // strip both before matching, or the landing page itself falls through.
    const path = this.currentUrl().split(/[?#]/)[0];
    if (path === '/' || path.startsWith('/sessions')) return 'chat';
    if (path.startsWith('/jobs')) return 'jobs';
    if (path.startsWith('/projects')) return 'projects';
    return null;
  });

  readonly filterText = signal('');

  // Fix round 1: the filter box is a control over the session list, not a
  // create action like "New chat" — a control over a list that isn't on
  // screen is noise, so it renders in chat mode only. Deliberately
  // `=== 'chat'`, not `!== 'jobs'`, for the same allowlist reason as
  // sessionGroups() below: mode() returns null outside the three modes
  // (/admin/*, /experts, /settings, ...), and a negated rewrite would show
  // the box there too. Kept separate from sessionGroups() rather than
  // derived from `sessionGroups().length > 0` — a search with zero matches
  // must still show the (now empty) box so the user can see and clear it.
  readonly showFilter = computed(() => this.mode() === 'chat');

  // Null, same as 'jobs'/'projects': outside chat mode the rail shows no
  // session groups at all (see the mode() allowlist above). Deliberately
  // `=== 'chat'`, not `!== 'jobs'` — mode() is an allowlist that returns
  // null for routes outside the three modes (/admin/*, /experts,
  // /settings, ...), and a negated rewrite would show sessions there too.
  readonly sessionGroups = computed(() => {
    if (this.mode() !== 'chat') return [];
    const q = this.filterText().trim().toLowerCase();
    if (!q) return this.sessions.grouped();
    return this.sessions.grouped()
      .map((g) => ({...g, threads: g.threads.filter((t) => t.title.toLowerCase().includes(q))}))
      .filter((g) => g.threads.length > 0);
  });

  // Reads the UNFILTERED list on purpose (sessions.grouped(), not
  // sessionGroups()) — this is how the empty state tells "no sessions yet"
  // (nothing to filter) apart from "no matches" (the filter hid everything)
  // when sessionGroups() is empty for either reason.
  readonly hasSessions = computed(() => this.sessions.grouped().length > 0);

  // Not `{static: true}`: the filter now only exists in the DOM in chat mode
  // (the @if in the template using showFilter() above), so this must be a
  // dynamic query that re-resolves as mode() changes — a static query
  // resolves once, before the first change detection, and would stay
  // undefined forever if the component happened to construct outside chat
  // mode. A decorator query is still used rather than the signal-based
  // viewChild() function: this repo's vitest JIT pipeline never resolves
  // those (see multi-select.component.ts), while decorator queries resolve
  // under both JIT and AOT — moot for this component's own spec (it never
  // renders the template at all) but kept for consistency with the rest of
  // the codebase.
  @ViewChild('filterInput') private readonly filterInput?: ElementRef<HTMLInputElement>;

  // ⌘K/Ctrl+K focuses the rail filter. Temporary key ownership: the command
  // palette (knowledge-base/knowledge/features/command_palette.md) will
  // claim ⌘K app-wide when it lands, and this filter will drop the binding
  // and the <kbd> hint — the filter itself stays.
  @HostListener('window:keydown', ['$event'])
  onKeydown(event: KeyboardEvent): void {
    if (!(event.metaKey || event.ctrlKey) || event.key.toLowerCase() !== 'k') return;
    // Outside chat mode filterInput is undefined (see the dynamic-query note
    // above) — leave the browser's own Ctrl+K alone rather than pre-empting
    // it for a control that isn't on screen to focus.
    if (!this.filterInput) return;
    event.preventDefault();
    // The rail can be collapsed (:host(.collapsed){width:0} + overflow:
    // hidden) while the filter stays mounted in the DOM — the default state
    // on mobile after every navigation. Focusing a zero-width, hidden input
    // would strand focus somewhere invisible after we've already swallowed
    // the browser's own Ctrl+K/⌘K — expand first so the target is visible.
    if (this.sidebar.collapsed()) this.sidebar.expand();
    this.filterInput.nativeElement.focus();
  }

  /** The filter's clear affordance (replaces the suppressed native
   *  ::-webkit-search-cancel-button). Returns focus to the input so clearing
   *  doesn't strand the next keystroke on the button. */
  clearFilter(): void {
    this.filterText.set('');
    this.filterInput?.nativeElement.focus();
  }

  selectMode(mode: RailMode | null): void {
    // Always the mode's own route. Never a thread id — see the April 2026
    // hijack regression recorded in coding_agent_ui_assessment.md §3.
    if (mode === null) return;
    this.router.navigate([MODE_ROUTES[mode]]);
  }

  constructor() {
    // enabledNonBlocking (Angular's default) constructs this component before the
    // first navigation, so the subscription below covers the initial load. If the
    // sidebar is instead constructed after navigation already finished
    // (enabledBlocking, SSR, or a remount), no event is coming and we must fetch here.
    if (this.router.navigated) {
      this.sessions.refresh();
    }

    // Auto-collapse sidebar on mobile after navigation, and keep the rail's
    // session list current — reusing this subscription rather than adding a
    // second one. Gated on chat mode so navigating within Jobs/Admin doesn't
    // refetch threads for a list that isn't even shown.
    this.router.events.pipe(
      filter(e => e instanceof NavigationEnd),
      takeUntilDestroyed(),
    ).subscribe(() => {
      if (this.viewport.isMobile()) {
        this.sidebar.collapse();
      }
      if (this.mode() === 'chat') {
        this.sessions.refresh();
      }
    });
  }

  /**
   * Close the mobile drawer on any link tap, delegated from the nav root.
   * The NavigationEnd subscription above misses the most intuitive dismiss
   * gesture: tapping the page you're already on — a same-URL navigation is
   * skipped by the router and emits nothing, so the drawer just sat there.
   * Buttons (bell, logout, collapse) are exempt on purpose.
   */
  onSidebarClick(event: Event): void {
    if (!this.viewport.isMobile()) return;
    if ((event.target as HTMLElement).closest('a[href]')) {
      this.sidebar.collapse();
    }
  }

  private readonly currentUrl = toSignal(
    this.router.events.pipe(
      filter((e): e is NavigationEnd => e instanceof NavigationEnd),
      map((e) => e.urlAfterRedirects),
    ),
    { initialValue: this.router.url },
  );

  readonly isWorkbenchRoute = computed(
    () => this.currentUrl()?.startsWith('/workbench') ?? false,
  );

  readonly adminToolsEnabled = environment.adminToolsEnabled;
  readonly giteaUrl = environment.giteaUrl;
  readonly dozzleUrl = environment.dozzleUrl;
  readonly neo4jUrl = environment.neo4jUrl;
  readonly pgadminUrl = environment.pgadminUrl;
  readonly minioConsoleUrl = environment.minioConsoleUrl;
  readonly cloudUrl = environment.cloudUrl;

  readonly isLayoutPickerOpen = signal(false);
  readonly pickerTop = signal(0);
  readonly pickerLeft = signal(0);

  toggleLayoutPicker(buttonEl: HTMLButtonElement): void {
    if (!this.isLayoutPickerOpen()) {
      const rect = buttonEl.getBoundingClientRect();
      this.pickerTop.set(rect.top);
      this.pickerLeft.set(rect.right + 8);
    }
    this.isLayoutPickerOpen.update((v) => !v);
  }

  closeLayoutPicker(): void {
    this.isLayoutPickerOpen.set(false);
  }

  resetLayout(): void {
    this.layoutService.resetLayout();
  }
}
