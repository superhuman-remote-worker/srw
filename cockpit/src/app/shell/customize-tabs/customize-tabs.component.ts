import {Component} from '@angular/core';
import {RouterLink, RouterLinkActive} from '@angular/router';
import {TranslocoPipe} from '@jsverse/transloco';
import {SidebarToggleComponent} from '../sidebar-toggle/sidebar-toggle.component';

/**
 * The tab bar the four Customize pages share (navigation_fixed_rail.md F3):
 * one rail row, "Customize", stands for Experts, Skills, Connectors and
 * Contacts, and this bar moves between them. Their routes are unchanged.
 *
 * Real links rather than the app-tab-nav tablist: each tab is a page, so it
 * should open in a new tab, show its URL and be bookmarkable. The bar also
 * owns the page's sidebar toggle — it is the top edge of all four pages, so
 * the list headers below it no longer carry one.
 */
@Component({
  selector: 'app-customize-tabs',
  standalone: true,
  imports: [RouterLink, RouterLinkActive, TranslocoPipe, SidebarToggleComponent],
  template: `
    <div class="customize-bar">
      <app-sidebar-toggle />
      <nav class="customize-tabs" [attr.aria-label]="'nav.customize' | transloco">
        @for (tab of tabs; track tab.path) {
          <a #link class="customize-tab" [routerLink]="tab.path" routerLinkActive="active"
             ariaCurrentWhenActive="page" (isActiveChange)="revealIfActive($event, link)">
            {{ tab.labelKey | transloco }}
          </a>
        }
      </nav>
    </div>
  `,
  styles: [`
    :host {
      display: block;
      flex-shrink: 0;
    }

    .customize-bar {
      display: flex;
      align-items: center;
      gap: 12px;
      padding: 0 16px;
      border-bottom: 1px solid var(--border-color);
    }

    /* Four tabs fit a 360px phone; anything narrower scrolls rather than
       wrapping to a second row, with the current page's tab kept in view
       (revealIfActive). */
    .customize-tabs {
      display: flex;
      min-width: 0;
      overflow-x: auto;
      scrollbar-width: none;
    }

    .customize-tab {
      flex: none;
      padding: 12px 16px;
      border-bottom: 2px solid transparent;
      color: var(--text-muted);
      font-size: 13px;
      font-weight: 500;
      text-decoration: none;
      white-space: nowrap;
      transition:
        color 0.15s ease,
        border-color 0.15s ease;
    }

    .customize-tab:hover {
      color: var(--text-primary);
    }

    .customize-tab.active {
      color: var(--text-primary);
      border-bottom-color: var(--accent-color);
    }

    .customize-tab:focus-visible {
      outline: 2px solid var(--accent-color);
      outline-offset: -2px;
    }

    @media (max-width: 768px) {
      .customize-bar {
        padding: 0 8px;
      }

      .customize-tab {
        min-height: 44px;
        padding: 12px 10px;
      }
    }
  `],
})
export class CustomizeTabsComponent {
  protected readonly tabs = [
    {path: '/experts', labelKey: 'nav.experts'},
    {path: '/skills', labelKey: 'nav.skills'},
    {path: '/datasources', labelKey: 'nav.datasources'},
    {path: '/contacts', labelKey: 'nav.contacts'},
  ];

  /** On a bar too narrow for all four tabs, the one the user is on must not
   *  be the one scrolled out of sight. `nearest` leaves a tab that is already
   *  visible — and the page's vertical scroll — where they are. */
  protected revealIfActive(active: boolean, link: HTMLElement): void {
    if (active) link.scrollIntoView({block: 'nearest', inline: 'nearest'});
  }
}
