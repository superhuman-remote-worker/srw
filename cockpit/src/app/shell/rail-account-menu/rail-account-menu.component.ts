import {Component, computed, inject} from '@angular/core';
import {Router} from '@angular/router';
import {TranslocoPipe} from '@jsverse/transloco';
import {AppMenuComponent, AppMenuItemComponent, AppMenuTriggerDirective} from '../../ui/menu';
import {UserService} from '../../core/services/user.service';
import {ViewportService} from '../../core/services/viewport.service';

/**
 * The rail's avatar flyout: Settings, the Workbench (desktop only), Log out.
 *
 * Settings is the one door to everything about you and this instance — API
 * and SSH keys, and for admins the Administration group — because Settings
 * takes over the rail and lists them itself (navigation_fixed_rail.md F4).
 * The Workbench moved here from the retired More menu (F6): it is an
 * operator surface, kept out of the primary rows a first-time user reads.
 */
@Component({
  selector: 'app-rail-account-menu',
  standalone: true,
  imports: [AppMenuComponent, AppMenuItemComponent, AppMenuTriggerDirective, TranslocoPipe],
  template: `
    @if (userService.currentUser(); as user) {
      <button class="rail-account" [appMenuTrigger]="accountMenu" menuPlacement="top-start">
        <span class="rail-account-avatar" [style.background]="user.avatar_color">{{ initials(user.display_name) }}</span>
        <span class="rail-account-name">{{ user.display_name }}</span>
      </button>
      <app-menu #accountMenu>
        <app-menu-item (activated)="go('/settings')">{{ 'nav.settings' | transloco }}</app-menu-item>
        @if (showWorkbench()) {
          <app-menu-item (activated)="go('/workbench')">{{ 'nav.workbench' | transloco }}</app-menu-item>
        }
        <app-menu-item class="menu-divider" (activated)="logout()">{{ 'nav.logout' | transloco }}</app-menu-item>
      </app-menu>
    }
  `,
  styles: [`
    .rail-account {
      display: flex;
      align-items: center;
      gap: 8px;
      width: 100%;
      padding: 8px 12px;
      border: none;
      background: transparent;
      border-radius: var(--radius-control);
      color: var(--text-primary);
      font-family: inherit;
      font-size: 12px;
      cursor: pointer;
      text-align: left;
      transition: background 0.15s ease;
    }

    .rail-account:hover,
    .rail-account[aria-expanded='true'] {
      background: var(--surface-0);
    }

    .rail-account-avatar {
      width: 28px;
      height: 28px;
      border-radius: 50%;
      display: flex;
      align-items: center;
      justify-content: center;
      font-size: 11px;
      font-weight: 600;
      color: var(--timeline-bg);
      flex-shrink: 0;
    }

    .rail-account-name {
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }

    /* Content-projection note: app-menu only forwards <app-menu-item>
       children (see AppMenuComponent's template) — a separate divider
       element placed here would silently not render. A border on the item
       that starts the new group draws the same line without needing one. */
    .menu-divider {
      margin-top: 6px;
      border-top: 1px solid var(--border-color);
    }

    @media (max-width: 768px) {
      .rail-account {
        min-height: 44px;
        padding: 10px 14px;
        gap: 12px;
      }
    }
  `],
})
export class RailAccountMenuComponent {
  protected readonly userService = inject(UserService);
  private readonly viewport = inject(ViewportService);
  private readonly router = inject(Router);

  /** The Workbench is a multi-panel desktop surface; gated exactly as the
   *  More menu's entry was. */
  readonly showWorkbench = computed(() => !this.viewport.isMobile());

  go(path: string): void {
    this.router.navigate([path]);
  }

  logout(): void {
    this.userService.logout();
  }

  initials(name: string): string {
    return name
      .split(' ')
      .map((w) => w[0])
      .join('')
      .toUpperCase()
      .slice(0, 2);
  }
}
