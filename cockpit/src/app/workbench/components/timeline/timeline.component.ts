import { Component, inject, OnInit, OnDestroy } from '@angular/core';
import { DataService } from '../../../core/services/data.service';
import { JobContextService } from '../../../core/services/job-context.service';

/**
 * Timeline scrubber component for playback control.
 * Fixed 60px height bar at the top of the app.
 *
 * Uses index-based navigation via DataService for instant seeking
 * without backend round-trips.
 */
@Component({
  selector: 'app-timeline',
  imports: [],
  template: `
    <div class="timeline">
      <select
        class="job-selector"
        [value]="jobContext.activeJobId() || ''"
        (change)="onJobSelect($event)"
      >
        <option value="">Select a job...</option>
        @for (job of jobContext.jobs(); track job.id) {
          <option [value]="job.id">
            {{ job.description ? truncate(job.description, 28) : job.id.slice(0, 8) }} · {{ job.status }}
            @if (job.audit_count !== null) {
              ({{ job.audit_count }} steps)
            }
          </option>
        }
      </select>
      <button
        class="refresh-btn"
        (click)="onRefresh()"
        [disabled]="data.isLoading()"
        title="Refresh jobs"
      >
        &#x21bb;
      </button>
      <button
        class="auto-refresh-btn"
        [class.active]="data.autoRefreshEnabled()"
        (click)="toggleAutoRefresh()"
        [title]="data.autoRefreshEnabled() ? 'Disable auto-refresh (15s)' : 'Enable auto-refresh (15s)'"
      >
        @if (data.autoRefreshEnabled()) {
          <span class="auto-indicator"></span>
        }
        AUTO
      </button>

    </div>
  `,
  styles: [
    `
      /* Surface and bottom border come from the workbench header this bar
         sits in, which also carries the sidebar toggle and layout controls. */
      .timeline {
        display: flex;
        align-items: center;
        gap: 16px;
        height: 60px;
        padding: 0 20px;
      }

      .play-button {
        display: flex;
        align-items: center;
        justify-content: center;
        width: 36px;
        height: 36px;
        border: none;
        border-radius: 50%;
        background: var(--accent-color);
        color: var(--timeline-bg);
        cursor: pointer;
        transition: transform 0.1s, background 0.2s;
      }

      .play-button:hover {
        background: var(--accent-hover);
        transform: scale(1.05);
      }

      .play-button:active {
        transform: scale(0.95);
      }

      .play-button:disabled {
        opacity: 0.4;
        cursor: not-allowed;
        transform: none;
      }

      .play-button:disabled:hover {
        background: var(--accent-color);
        transform: none;
      }

      .play-button svg {
        width: 16px;
        height: 16px;
      }

      .time-display {
        font-family: 'JetBrains Mono', monospace;
        font-size: 13px;
        color: var(--text-secondary);
        min-width: 70px;
      }

      .scrubber-container {
        flex: 1;
        position: relative;
        height: 20px;
        display: flex;
        align-items: center;
      }

      .scrubber {
        width: 100%;
        height: 6px;
        -webkit-appearance: none;
        appearance: none;
        background: var(--track-bg);
        border-radius: var(--radius-pill);
        cursor: pointer;
        margin: 0;
      }

      .scrubber::-webkit-slider-thumb {
        -webkit-appearance: none;
        appearance: none;
        width: 14px;
        height: 14px;
        background: var(--accent-color);
        border-radius: 50%;
        cursor: pointer;
        border: 2px solid var(--timeline-bg);
        box-shadow: 0 1px 3px rgba(0, 0, 0, 0.3);
      }

      .scrubber::-moz-range-thumb {
        width: 14px;
        height: 14px;
        background: var(--accent-color);
        border-radius: 50%;
        cursor: pointer;
        border: 2px solid var(--timeline-bg);
        box-shadow: 0 1px 3px rgba(0, 0, 0, 0.3);
      }

      .scrubber:hover::-webkit-slider-thumb {
        background: var(--accent-hover);
        transform: scale(1.1);
      }

      .scrubber:hover::-moz-range-thumb {
        background: var(--accent-hover);
        transform: scale(1.1);
      }

      .scrubber:disabled {
        opacity: 0.4;
        cursor: not-allowed;
      }

      .scrubber:disabled::-webkit-slider-thumb {
        cursor: not-allowed;
      }

      .scrubber:disabled::-moz-range-thumb {
        cursor: not-allowed;
      }

      .scrubber-container.disabled {
        opacity: 0.4;
        pointer-events: none;
      }

      .divider {
        width: 1px;
        height: 24px;
        background: var(--border-color);
      }

      .job-selector {
        padding: 6px 12px;
        border: 1px solid var(--border-color);
        border-radius: var(--radius-control);
        background: var(--panel-bg);
        color: var(--text-primary);
        font-size: 12px;
        font-family: 'JetBrains Mono', monospace;
        cursor: pointer;
        min-width: 200px;
      }

      .job-selector:hover {
        border-color: var(--text-muted);
      }

      .job-selector:focus {
        outline: none;
        border-color: var(--accent-color);
      }

      .refresh-btn {
        padding: 6px 10px;
        border: none;
        border-radius: var(--radius-control);
        background: transparent;
        color: var(--text-secondary);
        font-size: 14px;
        cursor: pointer;
        transition: all 0.15s ease;
      }

      .refresh-btn:hover:not(:disabled) {
        background: var(--surface-0);
        color: var(--text-primary);
      }

      .refresh-btn:disabled {
        opacity: 0.5;
        cursor: not-allowed;
      }

      .auto-refresh-btn {
        display: flex;
        align-items: center;
        gap: 4px;
        padding: 4px 8px;
        border: 1px solid var(--border-color);
        border-radius: var(--radius-control);
        background: transparent;
        color: var(--text-secondary);
        font-size: 10px;
        font-family: 'JetBrains Mono', monospace;
        font-weight: 500;
        cursor: pointer;
        transition: all 0.15s ease;
      }

      .auto-refresh-btn:hover {
        background: var(--surface-0);
        color: var(--text-primary);
      }

      .auto-refresh-btn.active {
        background: var(--accent-color);
        color: var(--timeline-bg);
        border-color: var(--accent-color);
      }

      .auto-refresh-btn.active:hover {
        background: var(--accent-hover);
        border-color: var(--accent-hover);
      }

      .auto-indicator {
        width: 6px;
        height: 6px;
        background: currentColor;
        border-radius: 50%;
        animation: pulse 1.5s ease-in-out infinite;
      }

      @keyframes pulse {
        0%, 100% { opacity: 1; }
        50% { opacity: 0.4; }
      }

      .loading-indicator {
        display: flex;
        align-items: center;
        gap: 6px;
        font-size: 11px;
        color: var(--text-muted);
      }

      .progress-text {
        font-family: 'JetBrains Mono', monospace;
      }

      .cache-indicator {
        font-size: 14px;
        color: var(--success);
        cursor: help;
      }
    `,
  ],
})
export class TimelineComponent implements OnInit, OnDestroy {
  readonly data = inject(DataService);
  readonly jobContext = inject(JobContextService);

  ngOnInit(): void {
    this.jobContext.loadJobs();
  }

  ngOnDestroy(): void {
    this.data.stopAutoRefresh();
  }

  onJobSelect(event: Event): void {
    const value = (event.target as HTMLSelectElement).value;
    this.data.setCurrentJob(value || null);
  }

  onRefresh(): void {
    this.jobContext.loadJobs();
    if (this.data.currentJobId()) {
      this.data.refresh();
    }
  }

  toggleAutoRefresh(): void {
    this.data.toggleAutoRefresh();
  }

  truncate(text: string, maxLength: number): string {
    return text.length <= maxLength ? text : text.slice(0, maxLength) + '…';
  }
}
