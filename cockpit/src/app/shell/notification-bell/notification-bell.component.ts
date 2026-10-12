import { Component, inject } from '@angular/core';
import { Router } from '@angular/router';
import { TranslocoDirective } from '@jsverse/transloco';
import { ActionCenterService } from '../../core/services/action-center.service';
import { AppIconComponent } from '../../ui/icon';

/** The `*transloco` directive's translate function. */
type Translate = (key: string, params?: Record<string, unknown>) => string;

@Component({
  selector: 'app-notification-bell',
  standalone: true,
  imports: [AppIconComponent, TranslocoDirective],
  template: `
    <button
      *transloco="let t"
      class="bell-btn"
      (click)="goToInbox()"
      [title]="tooltipText(t)"
    >
      <app-icon size="lg">notifications</app-icon>
      @if (actionCenter.badgeCount() > 0) {
        <span class="badge">{{ actionCenter.badgeCount() > 99 ? '99+' : actionCenter.badgeCount() }}</span>
      }
    </button>
  `,
  styles: [`
    :host {
      position: relative;
      display: inline-flex;
      /* Fixed-size icon control: never let the rail header's flexbox shrink
         this to make room for a sibling (that's what happened before Task
         10's fix round 1 — the collapse button absorbed the whole deficit
         because nothing in the row opted out of default flex-shrink: 1). */
      flex: none;
    }

    .bell-btn {
      background: none;
      border: none;
      cursor: pointer;
      position: relative;
      padding: 6px;
      border-radius: var(--radius-control);
      color: var(--text-secondary);
      font-size: 20px;
      line-height: 1;
      transition: color 0.15s, background 0.15s;
    }

    .bell-btn:hover {
      color: var(--text-primary);
      background: var(--surface-0);
    }

    .badge {
      position: absolute;
      top: 2px;
      right: 0;
      min-width: 16px;
      height: 16px;
      padding: 0 4px;
      border-radius: var(--radius-pill);
      background: var(--accent-color);
      color: var(--on-accent, var(--panel-bg));
      font-size: 10px;
      font-weight: 700;
      line-height: 16px;
      text-align: center;
    }

    /* Mobile tap target (Task 8's 44px convention, applied here in Task 10
       now that the rail header is this button's home). .rail-new/.rail-item
       reach 44px through padding alone because they're full-width rows; this
       is a square icon button, so padding would just push the icon
       off-center as the box grows. Force both dimensions and re-center
       instead. */
    @media (max-width: 768px) {
      .bell-btn {
        display: flex;
        align-items: center;
        justify-content: center;
        min-width: 44px;
        min-height: 44px;
      }
    }
  `],
})
export class NotificationBellComponent {
  readonly actionCenter = inject(ActionCenterService);
  private readonly router = inject(Router);

  goToInbox(): void {
    this.router.navigate(['/inbox']);
  }

  /** "N new notifications" (server `unseen`), plus how many still need
   *  someone when that differs. Translates through the `*transloco`
   *  directive's `t`, not the service's synchronous translate(): the rail
   *  renders the bell before the locale file has loaded, and the directive
   *  holds the button back until it has (and re-renders it on a language
   *  switch). */
  tooltipText(t: Translate): string {
    const c = this.actionCenter.counts();
    if (this.actionCenter.badgeCount() === 0) return t('notificationBell.title');
    const parts: string[] = [];
    if (c.unseen > 0) {
      parts.push(t(c.unseen === 1 ? 'notificationBell.unseenSingle' : 'notificationBell.unseenPlural', {n: c.unseen}));
    }
    if (c.total > 0 && c.total !== c.unseen) {
      parts.push(t(c.total === 1 ? 'notificationBell.pendingSingle' : 'notificationBell.pendingPlural', {n: c.total}));
    }
    return parts.join(', ');
  }
}
