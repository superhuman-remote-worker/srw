import {Component, computed, ElementRef, HostListener, inject, signal, ViewChild} from '@angular/core';
import {Location} from '@angular/common';
import {IsActiveMatchOptions, NavigationEnd, Router, RouterLink, RouterLinkActive} from '@angular/router';
import {takeUntilDestroyed, toSignal} from '@angular/core/rxjs-interop';
import {filter, map} from 'rxjs';
import {SidebarService} from '../../core/services/sidebar.service';
import {ViewportService} from '../../core/services/viewport.service';
import {SessionListService} from '../../core/services/session-list.service';
import {UserService} from '../../core/services/user.service';
import {ActionCenterService} from '../../core/services/action-center.service';
import {LayoutService} from '../../workbench/services/layout.service';
import {LayoutPickerComponent} from '../../workbench/components/layout-picker/layout-picker.component';
import {NotificationBellComponent} from '../notification-bell/notification-bell.component';
import {PersistentChatService} from '../../core/services/persistent-chat.service';
import {environment} from '../../core/environment';
import {TranslocoPipe} from '@jsverse/transloco';
import {AppIconComponent} from '../../ui/icon';
import {LegionMarkComponent} from '../../ui/legion-mark';
import {RailAccountMenuComponent} from '../rail-account-menu/rail-account-menu.component';

/**
 * What the rail body shows. `main` everywhere except the two areas that take
 * the rail over, each with a way back (navigation_fixed_rail.md F1, F4, F6).
 */
export type RailView = 'main' | 'settings' | 'workbench';

export interface RailLink {
  path: string;
  labelKey: string;
}

export interface RailLinkGroup {
  labelKey: string;
  items: RailLink[];
}

/** The four pages the Customize row stands for — they share a tab bar. */
const CUSTOMIZE_ROUTES = ['/experts', '/skills', '/datasources', '/contacts'];

/** `path` itself or anything below it — never a sibling that merely shares
 *  the prefix. */
function isUnder(path: string, prefix: string): boolean {
  return path === prefix || path.startsWith(prefix + '/');
}

export function railViewFor(path: string): RailView {
  if (isUnder(path, '/settings') || isUnder(path, '/admin')) return 'settings';
  if (isUnder(path, '/workbench')) return 'workbench';
  return 'main';
}

/** router.url carries the query string and fragment (e.g. '/?foo=bar') —
 *  every route test works on the bare path. */
function pathOf(url: string): string {
  return url.split(/[?#]/)[0] || '/';
}

@Component({
  selector: 'app-sidebar',
  standalone: true,
  imports: [RouterLink, RouterLinkActive, LayoutPickerComponent, NotificationBellComponent, TranslocoPipe, AppIconComponent, LegionMarkComponent, RailAccountMenuComponent],
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
        @switch (railView()) {
          <!-- Settings and Admin are one Settings (navigation_fixed_rail.md
               F4): the rail becomes its section list, with admins getting
               the Administration group at the bottom. -->
          @case ('settings') {
            <button type="button" class="rail-back" (click)="backToApp()">
              <app-icon size="md">arrow_back</app-icon> {{ 'nav.backToApp' | transloco }}
            </button>
            @for (group of settingsGroups(); track group.labelKey) {
              <div class="rail-group">{{ group.labelKey | transloco }}</div>
              @for (item of group.items; track item.path) {
                <a class="rail-item" [routerLink]="item.path" routerLinkActive="active">
                  {{ item.labelKey | transloco }}
                </a>
              }
            }
          }
          @case ('workbench') {
            <button type="button" class="rail-back" (click)="backToApp()">
              <app-icon size="md">arrow_back</app-icon> {{ 'nav.backToApp' | transloco }}
            </button>
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
          <!-- Every other route: the same rows and Recents, whatever the page
               (navigation_fixed_rail.md F1). -->
          @default {
            <div class="rail-primary">
              <!-- Always the draft landing, never a thread — see the April
                   2026 hijack regression in coding_agent_ui_assessment.md §3. -->
              <a class="rail-nav" routerLink="/" routerLinkActive="active"
                 [routerLinkActiveOptions]="exactPath">
                <app-icon size="md">edit_square</app-icon>
                <span class="rail-nav-label">{{ 'nav.newChat' | transloco }}</span>
              </a>
              <a class="rail-nav" routerLink="/jobs" routerLinkActive="active">
                <app-icon size="md">work</app-icon>
                <span class="rail-nav-label">{{ 'nav.jobs' | transloco }}</span>
                @if (jobsAwaitingReview(); as count) {
                  <span class="rail-badge" [attr.aria-label]="'nav.jobsAwaitingReview' | transloco: {count: count}">
                    {{ count }}
                  </span>
                }
              </a>
              <a class="rail-nav" routerLink="/projects" routerLinkActive="active">
                <app-icon size="md">folder</app-icon>
                <span class="rail-nav-label">{{ 'nav.projects' | transloco }}</span>
              </a>
              <a class="rail-nav" routerLink="/automations" routerLinkActive="active">
                <app-icon size="md">schedule</app-icon>
                <span class="rail-nav-label">{{ 'nav.automations' | transloco }}</span>
              </a>
              <a class="rail-nav" routerLink="/experts" [class.active]="customizeActive()">
                <app-icon size="md">extension</app-icon>
                <span class="rail-nav-label">{{ 'nav.customize' | transloco }}</span>
              </a>
            </div>

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

            <!-- Rendered whatever the list length: a fresh account with zero
                 sessions is exactly the user this door must stay reachable
                 for. -->
            <a class="rail-see-all" routerLink="/sessions">
              <app-icon size="sm">arrow_forward</app-icon> {{ 'nav.seeAllSessions' | transloco }}
            </a>
          }
        }
      </div>

      <div class="sidebar-footer">
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

      /* Primary rows: the same five on every main-rail route
         (navigation_fixed_rail.md F1). */
      .rail-primary {
        display: flex;
        flex-direction: column;
        gap: 2px;
        padding: 8px 0 4px;
      }

      .rail-nav {
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

      .rail-nav:hover {
        background: var(--surface-0);
        color: var(--text-primary);
      }

      .rail-nav.active {
        background: var(--surface-0);
        color: var(--accent-color);
        font-weight: 600;
      }

      .rail-nav-label {
        flex: 1;
        min-width: 0;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
      }

      /* Jobs waiting for review. Same tone as the bell's badge: it is the
         same kind of "someone needs you" signal, placed where the work is. */
      .rail-badge {
        flex: none;
        min-width: 18px;
        padding: 1px 6px;
        border-radius: var(--radius-pill);
        background: var(--accent-color);
        color: var(--on-accent);
        font-size: 11px;
        font-weight: 600;
        line-height: 16px;
        text-align: center;
      }

      /* The way out of a takeover (Settings, Workbench): an action row, not
         a destination, so it is a button and never lights up. */
      .rail-back {
        display: flex;
        align-items: center;
        gap: 10px;
        width: calc(100% - 16px);
        margin: 8px 8px 4px;
        padding: 8px 12px;
        border: none;
        border-radius: var(--radius-control);
        background: transparent;
        color: var(--text-secondary);
        font-family: inherit;
        font-size: 13px;
        text-align: left;
        cursor: pointer;
        transition:
          background 0.15s ease,
          color 0.15s ease;
      }

      .rail-back:hover {
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
         action row like .rail-nav, not a session row like .rail-item. */
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

        /* Tap-target restoration (Task 8 step 5): every row this template
           renders — the primary rows, the takeover's back button, session
           and settings rows, the filter (the whole label focuses the input
           on tap, an implicit label/input association, so sizing the label
           covers the target) and "See all sessions" — gets the 44px
           minimum back. The avatar trigger (.rail-account) is owned by its
           own component and restores the same rule in its own stylesheet —
           Emulated encapsulation means a rule here can't reach into its
           template. */
        .rail-nav,
        .rail-back,
        .rail-item,
        .rail-search,
        .rail-see-all {
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
  private readonly location = inject(Location);
  private readonly chatService = inject(PersistentChatService);
  readonly viewport = inject(ViewportService);
  private readonly sessions = inject(SessionListService);
  private readonly userService = inject(UserService);
  private readonly actionCenter = inject(ActionCenterService);

  /**
   * The URL as of the last completed navigation. Before the first one ends,
   * router.url is still '/', so seeding from it drew the landing page's rail
   * on the first frame of a hard load of /projects or /settings/general —
   * read the browser's own path until the router has one.
   */
  private readonly currentUrl = toSignal(
    this.router.events.pipe(
      filter((e): e is NavigationEnd => e instanceof NavigationEnd),
      map((e) => e.urlAfterRedirects),
    ),
    {initialValue: this.router.navigated ? this.router.url : this.location.path() || '/'},
  );

  private readonly currentPath = computed(() => pathOf(this.currentUrl()));

  readonly railView = computed(() => railViewFor(this.currentPath()));

  /** The Customize row stands for four pages and has no route of its own to
   *  match with routerLinkActive. */
  readonly customizeActive = computed(() =>
    CUSTOMIZE_ROUTES.some((prefix) => isUnder(this.currentPath(), prefix)),
  );

  /** New chat lights only on the draft landing itself, not on every route. */
  readonly exactPath: IsActiveMatchOptions = {
    paths: 'exact',
    queryParams: 'ignored',
    fragment: 'ignored',
    matrixParams: 'ignored',
  };

  /** Jobs waiting for the user's review: the server's own count, live over
   *  the notification stream. 0 hides the badge. */
  readonly jobsAwaitingReview = computed(
    () => this.actionCenter.counts().byCategory['review_queue']?.pending ?? 0,
  );

  /**
   * The settings rail: every user's sections, then — for admins — the
   * Administration group (navigation_fixed_rail.md §4). Hiding the group is
   * UX; /admin/* keeps its own adminGuard.
   */
  readonly settingsGroups = computed<RailLinkGroup[]>(() => {
    const access: RailLink[] = [{path: '/settings/api-keys', labelKey: 'settings.nav.apiKeys'}];
    if (this.externalClientsEnabled) {
      access.push(
        {path: '/settings/mcp', labelKey: 'settings.nav.mcp'},
        {path: '/settings/ssh-keys', labelKey: 'settings.nav.sshKeys'},
      );
    }
    const groups: RailLinkGroup[] = [
      {
        labelKey: 'settings.nav.groupSettings',
        items: [
          {path: '/settings/general', labelKey: 'settings.nav.general'},
          {path: '/settings/defaults', labelKey: 'settings.nav.defaults'},
          {path: '/settings/provider-keys', labelKey: 'settings.nav.providerKeys'},
          {path: '/settings/notifications', labelKey: 'settings.nav.notifications'},
        ],
      },
      {labelKey: 'settings.nav.groupAccess', items: access},
    ];
    if (this.userService.currentUser()?.is_admin) {
      groups.push({
        labelKey: 'settings.nav.groupAdmin',
        items: [
          {path: '/admin/models', labelKey: 'admin.nav.models'},
          {path: '/admin/subscriptions', labelKey: 'settings.nav.subscriptions'},
          {path: '/admin/users', labelKey: 'admin.nav.users'},
          {path: '/admin/config', labelKey: 'admin.nav.config'},
          {path: '/admin/grants', labelKey: 'admin.nav.grants'},
          {path: '/admin/cloud', labelKey: 'settings.nav.cloud'},
          {path: '/admin/usage', labelKey: 'admin.nav.usage'},
          {path: '/admin/capacity', labelKey: 'admin.nav.capacity'},
        ],
      });
    }
    return groups;
  });

  /** Where "Back to app" returns: the last URL the main rail was showing.
   *  A hard load straight into Settings has none, so it falls back to the
   *  draft landing. */
  private readonly lastAppUrl = signal('/');

  backToApp(): void {
    void this.router.navigateByUrl(this.lastAppUrl());
  }

  readonly filterText = signal('');

  readonly sessionGroups = computed(() => {
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

  // Not `{static: true}`: the filter only exists in the DOM while the main
  // rail is showing (Settings and the Workbench replace it), so this must be
  // a dynamic query that re-resolves as railView() changes — a static query
  // resolves once, before the first change detection, and would stay
  // undefined forever if the component happened to construct on a takeover
  // route. A decorator query is still used rather than the signal-based
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
    // On a takeover route filterInput is undefined (see the dynamic-query
    // note above) — leave the browser's own Ctrl+K alone rather than
    // pre-empting it for a control that isn't on screen to focus.
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

  constructor() {
    // enabledNonBlocking (Angular's default) constructs this component before the
    // first navigation, so the subscription below covers the initial load. If the
    // sidebar is instead constructed after navigation already finished
    // (enabledBlocking, SSR, or a remount), no event is coming and we must fetch here.
    if (this.router.navigated) {
      this.sessions.refresh();
      if (railViewFor(pathOf(this.router.url)) === 'main') this.lastAppUrl.set(this.router.url);
    }

    // Auto-collapse sidebar on mobile after navigation, and keep Recents
    // current — reusing this subscription rather than adding a second one.
    // Gated on the main rail: Recents is not on screen in Settings or the
    // Workbench, and the navigation back out of them refreshes it anyway.
    this.router.events.pipe(
      filter((e): e is NavigationEnd => e instanceof NavigationEnd),
      takeUntilDestroyed(),
    ).subscribe((e) => {
      if (this.viewport.isMobile()) {
        this.sidebar.collapse();
      }
      if (railViewFor(pathOf(e.urlAfterRedirects)) === 'main') {
        this.lastAppUrl.set(e.urlAfterRedirects);
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

  readonly externalClientsEnabled = environment.externalClientsEnabled;
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
