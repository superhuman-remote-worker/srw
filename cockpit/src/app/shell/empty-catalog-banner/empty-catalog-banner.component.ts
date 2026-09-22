import {Component, computed, inject, OnInit, signal} from '@angular/core';
import {RouterLink} from '@angular/router';
import {TranslocoPipe} from '@jsverse/transloco';
import {ModelService} from '../../core/services/model.service';
import {UserService} from '../../core/services/user.service';

/**
 * Persistent top banner shown when the model catalog (`GET /api/models`) is
 * empty. Copy is role-aware: admins are pointed at Admin → Models → Providers, regular
 * users at Settings → API Keys. Dismiss is session-local and resets on reload
 * — deliberately not stored, so the nag returns until the catalog is populated.
 */
@Component({
  selector: 'app-empty-catalog-banner',
  standalone: true,
  imports: [RouterLink, TranslocoPipe],
  template: `
    @if (shouldShow()) {
      <div class="empty-catalog-banner" role="status" aria-live="polite">
        <span class="banner-text">
          @if (isAdmin()) {
            {{ 'emptyCatalog.bannerAdmin' | transloco }}
            <a routerLink="/admin/models" class="banner-link">
              {{ 'admin.providers.title' | transloco }} →
            </a>
          } @else {
            {{ 'emptyCatalog.bannerUser' | transloco }}
            <a routerLink="/settings" class="banner-link">
              {{ 'nav.settings' | transloco }} →
            </a>
          }
        </span>
        <button
          type="button"
          class="banner-dismiss"
          (click)="dismiss()"
          [attr.aria-label]="'emptyCatalog.dismiss' | transloco"
        >×</button>
      </div>
    }
  `,
  styles: [`
    .empty-catalog-banner {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      padding: 10px 16px;
      background: color-mix(in srgb, var(--warning) 12%, transparent);
      border-bottom: 1px solid color-mix(in srgb, var(--warning) 30%, transparent);
      color: var(--warning);
      font-size: 13px;
      line-height: 1.4;
    }
    .banner-text { flex: 1; }
    .banner-link {
      color: inherit;
      font-weight: 600;
      text-decoration: underline;
      margin-left: 4px;
      cursor: pointer;
    }
    .banner-dismiss {
      background: transparent;
      border: none;
      color: inherit;
      font-size: 18px;
      line-height: 1;
      cursor: pointer;
      padding: 0 4px;
      opacity: 0.7;
    }
    .banner-dismiss:hover { opacity: 1; }
  `],
})
export class EmptyCatalogBannerComponent implements OnInit {
  private readonly modelService = inject(ModelService);
  private readonly userService = inject(UserService);
  private readonly dismissed = signal(false);

  readonly isAdmin = computed(
    () => this.userService.currentUser()?.is_admin === true,
  );

  readonly shouldShow = computed(() => {
    if (this.dismissed()) return false;
    if (!this.userService.isAuthenticated()) return false;
    if (!this.modelService.loaded()) return false;
    return this.modelService.models().length === 0;
  });

  ngOnInit(): void {
    this.modelService.load();
  }

  dismiss(): void {
    this.dismissed.set(true);
  }
}
