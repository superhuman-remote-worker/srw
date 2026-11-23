import {Component, computed, effect, inject, OnInit, signal, untracked, viewChild} from '@angular/core';
import {TranslocoPipe, TranslocoService} from '@jsverse/transloco';
import {ApiService} from '../../core/services/api.service';
import {DataService} from '../../core/services/data.service';
import {JobSummary} from '../../core/models/audit.model';
import {jobDisplayName} from '../../core/util/job-name';
import {SidebarToggleComponent} from '../../shell/sidebar-toggle/sidebar-toggle.component';
import {JobReviewComponent} from './job-review.component';
import {AppIconComponent} from '../../ui/icon';
import {AppIconButtonComponent} from '../../ui/icon-button';
import {AppSpinnerComponent} from '../../ui/spinner';

function relativeTime(iso: string | null | undefined, nowLabel: string): string {
  if (!iso) return '';
  const diff = Date.now() - new Date(iso).getTime();
  const mins = Math.floor(diff / 60000);
  if (mins < 1) return nowLabel;
  if (mins < 60) return `${mins}m`;
  const hrs = Math.floor(mins / 60);
  if (hrs < 24) return `${hrs}h`;
  return `${Math.floor(hrs / 24)}d`;
}

@Component({
  selector: 'app-job-review-page',
  standalone: true,
  imports: [
    TranslocoPipe,
    SidebarToggleComponent,
    JobReviewComponent,
    AppIconComponent,
    AppIconButtonComponent,
    AppSpinnerComponent,
  ],
  template: `
    <div class="page">
      <header class="page-header">
        <div class="header-left">
          <app-sidebar-toggle />
          <h1 class="header-title">{{ 'jobReviewPage.title' | transloco }}</h1>
          @if (pendingJobs().length > 0) {
            <span class="header-count">({{ pendingJobs().length }})</span>
          }
        </div>
        <app-icon-button
          size="sm"
          [ariaLabel]="'jobReviewPage.refresh' | transloco"
          [tooltip]="'jobReviewPage.refresh' | transloco"
          [disabled]="isLoading()"
          (clicked)="refresh()"
        >
          <app-icon size="sm">refresh</app-icon>
        </app-icon-button>
      </header>

      <div class="page-body">
        <aside class="list-panel" [attr.aria-label]="'jobReviewPage.listAriaLabel' | transloco">
          @if (isLoading() && pendingJobs().length === 0) {
            <div class="state">
              <app-spinner size="md" tone="accent" />
              <span>{{ 'jobReviewPage.loading' | transloco }}</span>
            </div>
          } @else if (pendingJobs().length === 0) {
            <div class="state">
              <app-icon size="inherit" class="empty-icon">task_alt</app-icon>
              <span class="empty-text">{{ 'jobReviewPage.empty' | transloco }}</span>
            </div>
          } @else {
            @for (job of pendingJobs(); track job.id) {
              <button
                class="list-item"
                [class.selected]="currentJobId() === job.id"
                (click)="selectJob(job.id)"
              >
                <span class="item-desc">{{ jobName(job) || job.id.slice(0, 8) }}</span>
                <span class="item-meta">
                  <span class="item-badge">{{ 'jobReviewPage.pendingBadge' | transloco }}</span>
                  <span class="item-time">{{ formatRelative(job.created_at) }}</span>
                </span>
              </button>
            }
          }
        </aside>

        <main class="detail-panel">
          <app-job-review />
        </main>
      </div>
    </div>
  `,
  styles: [
    `
      :host {
        display: block;
        height: 100%;
      }

      .page {
        display: flex;
        flex-direction: column;
        height: 100%;
      }

      .page-header {
        display: flex;
        align-items: center;
        gap: 12px;
        padding: 8px 12px;
        border-bottom: 1px solid var(--border-color);
        flex-shrink: 0;
      }

      .header-left {
        display: flex;
        align-items: center;
        gap: 8px;
        flex: 1;
        min-width: 0;
      }

      .header-title {
        font-size: 14px;
        font-weight: 600;
        color: var(--text-primary);
        margin: 0;
      }

      .header-count {
        font-size: 12px;
        color: var(--text-muted);
        font-family: 'JetBrains Mono', monospace;
      }

      .page-body {
        flex: 1;
        display: grid;
        grid-template-columns: 280px 1fr;
        min-height: 0;
        overflow: hidden;
      }

      .list-panel {
        display: flex;
        flex-direction: column;
        gap: 2px;
        padding: 8px;
        overflow-y: auto;
        border-right: 1px solid var(--border-color);
        background: var(--panel-bg);
      }

      .detail-panel {
        min-width: 0;
        overflow: hidden;
        background: var(--panel-bg);
      }

      .state {
        display: flex;
        flex-direction: column;
        align-items: center;
        gap: 8px;
        padding: 32px 12px;
        color: var(--text-muted);
        font-size: 12px;
        text-align: center;
      }

      .empty-icon {
        font-size: 32px;
        opacity: 0.6;
      }

      .empty-text {
        max-width: 220px;
        line-height: 1.4;
      }

      .list-item {
        display: flex;
        flex-direction: column;
        gap: 6px;
        padding: 10px 12px;
        background: transparent;
        border: 1px solid transparent;
        border-radius: var(--radius-control);
        color: var(--text-secondary);
        text-align: left;
        cursor: pointer;
        font-family: inherit;
        transition:
          background 0.15s ease,
          color 0.15s ease,
          border-color 0.15s ease;
      }

      .list-item:hover {
        background: var(--surface-0);
        color: var(--text-primary);
      }

      .list-item.selected {
        background: var(--surface-0);
        color: var(--text-primary);
        border-color: var(--accent-color);
      }

      .item-desc {
        font-size: 13px;
        line-height: 1.3;
        overflow: hidden;
        text-overflow: ellipsis;
        display: -webkit-box;
        -webkit-line-clamp: 2;
        -webkit-box-orient: vertical;
        word-break: break-word;
      }

      .item-meta {
        display: flex;
        align-items: center;
        gap: 8px;
        font-size: 11px;
        color: var(--text-muted);
      }

      .item-badge {
        text-transform: uppercase;
        letter-spacing: 0.06em;
        font-weight: 600;
        font-size: 11px;
        /* Theme token (was the non-existent --warning-color → invisible
           Catppuccin fallback on the light theme). Mixed toward text for WCAG AA at 10px. */
        color: color-mix(in srgb, var(--warning) 55%, var(--text-primary));
      }

      .item-time {
        font-family: 'JetBrains Mono', monospace;
      }

      @media (max-width: 768px) {
        .page-body {
          grid-template-columns: 1fr;
          grid-template-rows: auto 1fr;
        }

        .list-panel {
          max-height: 200px;
          border-right: none;
          border-bottom: 1px solid var(--border-color);
        }
      }
    `,
  ],
})
export class JobReviewPageComponent implements OnInit {
  private readonly api = inject(ApiService);
  private readonly data = inject(DataService);
  private readonly transloco = inject(TranslocoService);

  readonly pendingJobs = signal<JobSummary[]>([]);
  readonly jobName = jobDisplayName;
  readonly isLoading = signal<boolean>(false);
  readonly currentJobId = this.data.currentJobId;
  private readonly review = viewChild(JobReviewComponent);

  constructor() {
    // Auto-select the first pending job when the list loads and nothing is selected.
    // `untracked` so we don't re-trigger on every list refresh after a manual selection.
    effect(() => {
      const jobs = this.pendingJobs();
      untracked(() => {
        if (jobs.length === 0) return;
        if (this.currentJobId()) return;
        this.data.setCurrentJob(jobs[0].id);
      });
    });
  }

  ngOnInit(): void {
    this.refresh();
  }

  refresh(): void {
    // The review panel used to carry its own title + reload button under this
    // header — one "Job Review" over another. This is the single refresh now,
    // so it reloads the open job as well as the queue.
    this.review()?.loadJob();
    this.isLoading.set(true);
    this.api.getJobs('pending_review').subscribe((jobs) => {
      this.pendingJobs.set(jobs);
      this.isLoading.set(false);
    });
  }

  selectJob(jobId: string): void {
    this.data.setCurrentJob(jobId);
  }

  formatRelative(iso: string | null | undefined): string {
    return relativeTime(iso, this.transloco.translate('jobReviewPage.timeNow'));
  }
}
