import {Component, computed, inject} from '@angular/core';
import {Router} from '@angular/router';
import {TranslocoPipe} from '@jsverse/transloco';
import {AppMenuComponent, AppMenuItemComponent, AppMenuTriggerDirective} from '../../ui/menu';
import {UserService} from '../../core/services/user.service';
import {environment} from '../../core/environment';

/**
 * The rail's avatar flyout — the split rule's instance-facing half (see
 * RailMoreMenuComponent for the "what the agent uses" half).
 *
 * Holds what is about the signed-in user and this instance: Settings, API
 * keys, SSH keys, then — gated on is_admin — Admin, then Log out. The
 * /admin/* routes carry their own adminGuard; hiding the entry here for a
 * non-admin is UX, not the access-control boundary.
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
        <app-menu-item (activated)="go('/settings/api-keys')">{{ 'nav.apiKeys' | transloco }}</app-menu-item>
        @if (externalClientsEnabled) {
          <app-menu-item (activated)="go('/settings/ssh-keys')">{{ 'nav.sshKeys' | transloco }}</app-menu-item>
        }
        @if (showAdmin()) {
          <app-menu-item class="menu-divider" (activated)="go('/admin/models')">{{ 'nav.admin' | transloco }}</app-menu-item>
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
  readonly externalClientsEnabled = environment.externalClientsEnabled;
  protected readonly userService = inject(UserService);
  private readonly router = inject(Router);

  readonly showAdmin = computed(() => this.userService.currentUser()?.is_admin === true);

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
