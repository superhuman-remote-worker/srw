import {Component, inject, OnDestroy, OnInit, signal} from '@angular/core';
import {TranslocoPipe, TranslocoService} from '@jsverse/transloco';
import {ApiService} from '../../core/services/api.service';
import {Agent, AgentStatus} from '../../core/models/api.model';
import {JobSummary} from '../../core/models/audit.model';
import {AppButtonComponent} from '../../ui/button';
import {AppBadgeComponent, type BadgeTone} from '../../ui/badge';
import {AppDialogComponent} from '../../ui/dialog';
import {AppSpinnerComponent} from '../../ui/spinner';

/**
 * Agent List component that displays registered agents.
 * Shows agent status, current job, and last heartbeat with auto-refresh.
 * Supports assigning jobs to ready agents.
 */
@Component({
  selector: 'app-agent-list',
  standalone: true,
  imports: [
    TranslocoPipe,
    AppButtonComponent,
    AppBadgeComponent,
    AppDialogComponent,
    AppSpinnerComponent,
  ],
  template: `
    <div class="agent-list-container">
      <!-- Header -->
      <div class="header-bar">
        <span class="title">{{ 'agentList.title' | transloco }}</span>
        <span class="agent-count">{{ 'agentList.count' | transloco: {n: agents().length} }}</span>
        <app-button variant="secondary" size="sm" [disabled]="isLoading()" (clicked)="refresh()">
          {{ (isLoading() ? 'agentList.loading' : 'agentList.refresh') | transloco }}
        </app-button>
      </div>

      <!-- Assignment Dialog -->
      <app-dialog
        [open]="showAssignDialog()"
        size="md"
        [title]="'agentList.assign.title' | transloco"
        (closed)="closeAssignDialog()"
      >
        @if (availableJobs().length === 0) {
          <div class="no-jobs">
            <span>{{ 'agentList.assign.noJobs' | transloco }}</span>
            <span class="hint">{{ 'agentList.assign.noJobsHint' | transloco }}</span>
          </div>
        } @else {
          <div class="job-select-list">
            @for (job of availableJobs(); track job.id) {
              <div
                class="job-option"
                [class.selected]="selectedJobId() === job.id"
                (click)="selectJob(job.id)"
              >
                <span class="job-prompt">{{ truncatePrompt(job.description) }}</span>
                <span class="job-meta">{{ job.id.slice(0, 8) }}... | {{ formatDate(job.created_at) }}</span>
              </div>
            }
          </div>
        }
        <ng-container appDialogActions>
          <app-button variant="secondary" size="sm" (clicked)="closeAssignDialog()">
            {{ 'agentList.assign.cancel' | transloco }}
          </app-button>
          <app-button
            variant="primary"
            size="sm"
            [disabled]="!selectedJobId() || isAssigning()"
            [loading]="isAssigning()"
            (clicked)="confirmAssignment()"
          >
            {{ (isAssigning() ? 'agentList.assign.assigning' : 'agentList.assign.confirm') | transloco }}
          </app-button>
        </ng-container>
      </app-dialog>

      <!-- Loading State -->
      @if (isLoading() && agents().length === 0) {
        <div class="loading-state">
          <app-spinner size="lg" tone="accent" />
          <span>{{ 'agentList.loadingAgents' | transloco }}</span>
        </div>
      }

      <!-- Empty State (or the list could not be loaded at all) -->
      @if (!isLoading() && agents().length === 0) {
        @if (loadFailed()) {
          <div class="empty-state load-failed">
            <span class="load-failed-title">{{ 'agentList.loadFailed' | transloco }}</span>
            <span class="empty-hint">{{ 'agentList.loadFailedHint' | transloco }}</span>
          </div>
        } @else {
          <div class="empty-state">
            <span class="empty-icon">&#x1F916;</span>
            <span>{{ 'agentList.empty' | transloco }}</span>
            <span class="empty-hint">{{ 'agentList.emptyHint' | transloco }}</span>
          </div>
        }
      }

      <!-- Agent Table -->
      @if (agents().length > 0) {
        <div class="table-container">
          <table class="agent-table app-table">
            <thead>
              <tr>
                <th>{{ 'agentList.table.status' | transloco }}</th>
                <th>{{ 'agentList.table.config' | transloco }}</th>
                <th>{{ 'agentList.table.hostname' | transloco }}</th>
                <th>{{ 'agentList.table.currentJob' | transloco }}</th>
                <th>{{ 'agentList.table.lastHeartbeat' | transloco }}</th>
                <th>{{ 'agentList.table.actions' | transloco }}</th>
              </tr>
            </thead>
            <tbody>
              @for (agent of agents(); track agent.id) {
                <tr [class]="'status-' + agent.status">
                  <td>
                    <app-badge [tone]="agentStatusTone(agent.status)" size="sm">
                      {{ getStatusIcon(agent.status) }} {{ 'agentList.status.' + agent.status | transloco }}
                    </app-badge>
                    @if (agent.aux_degraded) {
                      <span class="aux-degraded" [title]="auxTooltip(agent)">
                        <app-badge tone="danger" size="sm">
                          &#x26A0; {{ 'agentList.auxDegraded' | transloco }}
                        </app-badge>
                      </span>
                    }
                  </td>
                  <td class="config-name">{{ agent.config_name }}</td>
                  <td class="hostname">{{ agent.hostname || agent.pod_ip || '-' }}</td>
                  <td class="job-id">
                    @if (agent.current_job_id) {
                      <span class="job-link" title="{{ agent.current_job_id }}">
                        {{ agent.current_job_id.slice(0, 8) }}...
                      </span>
                    } @else {
                      <span class="no-job">-</span>
                    }
                  </td>
                  <td class="heartbeat">{{ formatTimestamp(agent.last_heartbeat) }}</td>
                  <td class="actions">
                    @if (agent.status === 'ready') {
                      <app-button
                        variant="success"
                        size="sm"
                        [ariaLabel]="'agentList.actions.assignTitle' | transloco"
                        (clicked)="openAssignDialog(agent.id)"
                      >
                        {{ 'agentList.actions.assign' | transloco }}
                      </app-button>
                    }
                    @if (agent.status === 'offline' || agent.status === 'failed') {
                      <app-button variant="danger" size="sm" (clicked)="removeAgent(agent.id)">
                        {{ 'agentList.actions.remove' | transloco }}
                      </app-button>
                    }
                  </td>
                </tr>
              }
            </tbody>
          </table>
        </div>
      }

      <!-- Footer with status message -->
      <div class="footer-bar">
        @if (statusMessage()) {
          <span class="status-message" [class.error]="statusIsError()">
            {{ statusMessage() }}
          </span>
        } @else {
          <span class="auto-refresh">
            {{ (autoRefreshEnabled() ? 'agentList.footer.autoRefreshOn' : 'agentList.footer.autoRefreshOff') | transloco }}
          </span>
        }
        <app-button variant="secondary" size="sm" (clicked)="toggleAutoRefresh()">
          {{ (autoRefreshEnabled() ? 'agentList.footer.disable' : 'agentList.footer.enable') | transloco }}
        </app-button>
      </div>
    </div>
  `,
  styles: [
    `
      :host {
        display: block;
        height: 100%;
        overflow: hidden;
      }

      .agent-list-container {
        display: flex;
        flex-direction: column;
        height: 100%;
        background: var(--panel-bg, var(--panel-bg));
        position: relative;
      }

      /* Header */
      .header-bar {
        display: flex;
        align-items: center;
        gap: 12px;
        padding: 10px 12px;
        background: var(--panel-header-bg);
        border-bottom: 1px solid var(--border-color, var(--surface-0));
        flex-shrink: 0;
      }

      .title {
        font-weight: 600;
        color: var(--text-primary, var(--text-primary));
      }

      .agent-count {
        font-size: 12px;
        color: var(--text-muted);
        margin-left: auto;
      }

      .no-jobs {
        text-align: center;
        padding: 20px;
        color: var(--text-muted);
      }

      .no-jobs .hint {
        display: block;
        font-size: 11px;
        margin-top: 8px;
        opacity: 0.7;
      }

      .job-select-list {
        display: flex;
        flex-direction: column;
        gap: 8px;
      }

      .job-option {
        padding: 10px 12px;
        background: var(--surface-0, var(--surface-0));
        border: 1px solid var(--border-color, var(--surface-1));
        border-radius: var(--radius-control);
        cursor: pointer;
        transition: all 0.15s ease;
      }

      .job-option:hover {
        background: var(--panel-header-bg);
      }

      .job-option.selected {
        border-color: var(--accent-color, var(--accent-color));
        background: color-mix(in srgb, var(--accent-color) 20%, transparent);
      }

      .job-prompt {
        display: block;
        color: var(--text-primary, var(--text-primary));
        font-size: 12px;
        margin-bottom: 4px;
      }

      .job-meta {
        display: block;
        font-size: 10px;
        font-family: 'JetBrains Mono', monospace;
        color: var(--text-muted);
      }

      /* Loading State */
      .loading-state {
        display: flex;
        flex-direction: column;
        align-items: center;
        justify-content: center;
        gap: 12px;
        padding: 40px;
        flex: 1;
      }

      /* Empty State */
      .empty-state {
        display: flex;
        flex-direction: column;
        align-items: center;
        justify-content: center;
        gap: 12px;
        padding: 40px;
        color: var(--text-muted);
        flex: 1;
      }

      .empty-icon {
        font-size: 48px;
        opacity: 0.5;
      }

      .empty-hint {
        font-size: 11px;
        opacity: 0.6;
      }

      .load-failed-title {
        color: var(--danger);
      }

      /* Table */
      .table-container {
        flex: 1;
        overflow: auto;
        padding: 8px;
      }

      .aux-degraded {
        margin-left: 6px;
        cursor: help;
      }

      .config-name {
        font-family: 'JetBrains Mono', monospace;
        font-size: 11px;
      }

      .hostname {
        font-family: 'JetBrains Mono', monospace;
        font-size: 11px;
        color: var(--text-secondary, var(--text-secondary));
      }

      .job-link {
        font-family: 'JetBrains Mono', monospace;
        font-size: 11px;
        color: var(--info);
        cursor: pointer;
      }

      .job-link:hover {
        text-decoration: underline;
      }

      .no-job {
        color: var(--text-muted);
      }

      .heartbeat {
        font-family: 'JetBrains Mono', monospace;
        font-size: 11px;
        color: var(--text-muted);
      }

      /* Action Buttons */
      .actions {
        white-space: nowrap;
        text-align: right;
      }

      .actions app-button {
        vertical-align: middle;
      }

      .actions app-button + app-button {
        margin-left: 6px;
      }

      /* Footer */
      .footer-bar {
        display: flex;
        align-items: center;
        justify-content: space-between;
        padding: 8px 12px;
        background: var(--surface-0, var(--surface-0));
        border-top: 1px solid var(--border-color, var(--surface-0));
        flex-shrink: 0;
      }

      .auto-refresh {
        font-size: 11px;
        color: var(--text-muted);
      }

      .status-message {
        font-size: 11px;
        color: var(--success);
      }

      .status-message.error {
        color: var(--danger);
      }

    `,
  ],
})
export class AgentListComponent implements OnInit, OnDestroy {
  private readonly api = inject(ApiService);
  private readonly transloco = inject(TranslocoService);

  readonly agents = signal<Agent[]>([]);
  readonly availableJobs = signal<JobSummary[]>([]);
  readonly isLoading = signal(false);
  /** The last refresh could not load the list (not the same as "no agents"). */
  readonly loadFailed = signal(false);
  readonly autoRefreshEnabled = signal(true);

  // Assignment dialog state
  readonly showAssignDialog = signal(false);
  readonly selectedAgentId = signal<string | null>(null);
  readonly selectedJobId = signal<string | null>(null);
  readonly isAssigning = signal(false);

  // Status message
  readonly statusMessage = signal<string | null>(null);
  readonly statusIsError = signal(false);

  private refreshInterval: ReturnType<typeof setInterval> | null = null;
  private statusTimeout: ReturnType<typeof setTimeout> | null = null;

  ngOnInit(): void {
    this.refresh();
    this.startAutoRefresh();
  }

  ngOnDestroy(): void {
    this.stopAutoRefresh();
    if (this.statusTimeout) {
      clearTimeout(this.statusTimeout);
    }
  }

  refresh(): void {
    this.isLoading.set(true);
    this.api.getAgents().subscribe((agents) => {
      // `null` = the request failed. Keep the rows already shown rather than
      // presenting the failure as an empty fleet, and say so.
      if (agents === null) {
        this.loadFailed.set(true);
        this.showStatus(this.transloco.translate('agentList.loadFailed'), true);
      } else {
        this.loadFailed.set(false);
        this.agents.set(agents);
      }
      this.isLoading.set(false);
    });
  }

  loadAvailableJobs(): void {
    // Load jobs with 'created' status that can be assigned
    this.api.getJobs('created', 50).subscribe((jobs) => {
      this.availableJobs.set(jobs);
    });
  }

  openAssignDialog(agentId: string): void {
    this.selectedAgentId.set(agentId);
    this.selectedJobId.set(null);
    this.loadAvailableJobs();
    this.showAssignDialog.set(true);
  }

  closeAssignDialog(): void {
    this.showAssignDialog.set(false);
    this.selectedAgentId.set(null);
    this.selectedJobId.set(null);
  }

  selectJob(jobId: string): void {
    this.selectedJobId.set(jobId);
  }

  confirmAssignment(): void {
    const agentId = this.selectedAgentId();
    const jobId = this.selectedJobId();

    if (!agentId || !jobId) {
      return;
    }

    this.isAssigning.set(true);

    this.api.assignJob(jobId, agentId).subscribe({
      next: (result) => {
        this.isAssigning.set(false);
        this.closeAssignDialog();

        if (result) {
          this.showStatus(this.transloco.translate('agentList.messages.assigned'), false);
          this.refresh();
        } else {
          this.showStatus(this.transloco.translate('agentList.messages.assignFailed'), true);
        }
      },
      error: () => {
        this.isAssigning.set(false);
        this.showStatus(this.transloco.translate('agentList.messages.assignError'), true);
      },
    });
  }

  private showStatus(message: string, isError: boolean): void {
    this.statusMessage.set(message);
    this.statusIsError.set(isError);

    if (this.statusTimeout) {
      clearTimeout(this.statusTimeout);
    }

    this.statusTimeout = setTimeout(() => {
      this.statusMessage.set(null);
    }, 5000);
  }

  toggleAutoRefresh(): void {
    if (this.autoRefreshEnabled()) {
      this.stopAutoRefresh();
      this.autoRefreshEnabled.set(false);
    } else {
      this.startAutoRefresh();
      this.autoRefreshEnabled.set(true);
    }
  }

  private startAutoRefresh(): void {
    this.stopAutoRefresh();
    this.refreshInterval = setInterval(() => {
      if (!this.isLoading()) {
        this.refresh();
      }
    }, 30000);
  }

  private stopAutoRefresh(): void {
    if (this.refreshInterval) {
      clearInterval(this.refreshInterval);
      this.refreshInterval = null;
    }
  }

  removeAgent(agentId: string): void {
    this.api.deleteAgent(agentId).subscribe((result) => {
      if (result) {
        this.refresh();
      }
    });
  }

  agentStatusTone(status: AgentStatus): BadgeTone {
    switch (status) {
      case 'ready': return 'success';
      case 'completed': return 'success';
      case 'working':
      case 'draining': return 'warning';
      case 'booting':
      case 'session': return 'info';
      case 'failed': return 'danger';
      case 'offline': return 'neutral';
      default: return 'neutral';
    }
  }

  getStatusIcon(status: AgentStatus): string {
    const icons: Record<AgentStatus, string> = {
      booting: '\u23F3',
      ready: '\u2705',
      working: '\u26A1',
      session: '\uD83D\uDCAC',
      draining: '\u23F3',
      completed: '\u2714',
      failed: '\u274C',
      offline: '\u26AA',
    };
    return icons[status] || '\u2753';
  }

  /**
   * Human tooltip for the aux-degraded badge, built from the compact
   * `metadata.aux` summary the agent reports on its heartbeat (aux Phase 2).
   * e.g. "Auxiliary model degraded: gemma-4-moe \u2014 7 consecutive failures
   * (memory_extraction: ConnectionError)".
   */
  auxTooltip(agent: Agent): string {
    const prefix = this.transloco.translate('agentList.auxDegradedTip');
    const aux = (agent.metadata?.['aux'] ?? null) as {
      model?: string;
      consecutive_failures?: number;
      failing_tasks?: Record<string, {last_error_type?: string}>;
    } | null;
    if (!aux) {
      return prefix;
    }

    const parts: string[] = [];
    if (aux.model) {
      parts.push(aux.model);
    }
    if (aux.consecutive_failures) {
      parts.push(`${aux.consecutive_failures} consecutive failures`);
    }

    const taskDetail = Object.entries(aux.failing_tasks ?? {})
      .map(([name, t]) => `${name}: ${t?.last_error_type ?? 'error'}`)
      .join(', ');

    let detail = parts.join(' \u2014 ');
    if (taskDetail) {
      detail = detail ? `${detail} (${taskDetail})` : taskDetail;
    }
    return detail ? `${prefix}: ${detail}` : prefix;
  }

  formatTimestamp(timestamp: string): string {
    const date = new Date(timestamp);
    const now = new Date();
    const diffMs = now.getTime() - date.getTime();
    const diffSec = Math.floor(diffMs / 1000);

    if (diffSec < 60) {
      return this.transloco.translate('agentList.time.secondsAgo', {n: diffSec});
    }
    if (diffSec < 3600) {
      return this.transloco.translate('agentList.time.minutesAgo', {n: Math.floor(diffSec / 60)});
    }
    if (diffSec < 86400) {
      return this.transloco.translate('agentList.time.hoursAgo', {n: Math.floor(diffSec / 3600)});
    }
    return date.toLocaleDateString(this.transloco.getActiveLang());
  }

  truncatePrompt(prompt: string | undefined, maxLength: number = 60): string {
    if (!prompt) {
      return '';
    }
    if (prompt.length <= maxLength) {
      return prompt;
    }
    return prompt.slice(0, maxLength) + '...';
  }

  formatDate(dateString: string): string {
    const date = new Date(dateString);
    return date.toLocaleDateString(this.transloco.getActiveLang(), {
      day: '2-digit',
      month: 'short',
      hour: '2-digit',
      minute: '2-digit',
      hour12: false,
    });
  }
}
