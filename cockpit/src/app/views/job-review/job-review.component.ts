import {Component, DestroyRef, computed, effect, inject, signal} from '@angular/core';
import {Router} from '@angular/router';
import {TranslocoPipe, TranslocoService} from '@jsverse/transloco';
import {ApiService} from '../../core/services/api.service';
import {DataService} from '../../core/services/data.service';
import {Datasource, Job, PullRequestStatus, RepositoryForge} from '../../core/models/api.model';
import {environment} from '../../core/environment';
import {AppButtonComponent} from '../../ui/button';
import {AppBadgeComponent, type BadgeTone} from '../../ui/badge';
import {AppTextareaComponent} from '../../ui/textarea';
import {AppSpinnerComponent} from '../../ui/spinner';
import {AppCopyFieldComponent} from '../../ui/copy-field';
import {JobDiffReviewComponent} from '../job-diff-review/job-diff-review.component';
import {
  asRecord,
  effectiveJobStatus,
  jobStatusLabelKey,
  jobStatusTone as sharedJobStatusTone,
} from '../../core/util/job-status';

interface FrozenJobData {
  freeze_type?: string;    // "phase_boundary" | "job_complete" | "vm_upgrade_required"
  phase_type?: string;     // "strategic" | "tactical"
  summary?: string;
  deliverables?: string[];
  confidence?: number;
  notes?: string;
  phase_number?: number;
  frozen_at?: string;
  command?: string;        // sudo command that triggered vm_upgrade_required freeze
  reason?: string;         // why the freeze happened
}

export interface JobPullRequest {
  forge: RepositoryForge;
  repo: string;
  number: number;
  url: string;
  head: string;
  base: string;
}

const REPOSITORY_FORGES = new Set<RepositoryForge>(['github', 'gitea', 'gitlab']);

function safeHttpUrl(value: unknown): string | null {
  if (typeof value !== 'string' || !value.trim()) return null;
  try {
    const parsed = new URL(value.trim());
    if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') return null;
    parsed.username = '';
    parsed.password = '';
    return parsed.toString();
  } catch {
    return null;
  }
}

/** Convert a credential-redacted HTTPS/SSH clone URL into its repository page. */
export function repositoryWebUrl(connectionUrl: string | null | undefined): string | null {
  const raw = connectionUrl?.trim();
  if (!raw) return null;

  // SCP-like Git syntax is not understood by URL, but is common for repository
  // connectors: git@github.com:owner/repo.git.
  const scp = raw.match(/^(?:[^@/:\s]+@)?([^/:\s]+):(.+)$/);
  if (scp && !raw.includes('://')) {
    return repositoryWebUrl(`https://${scp[1]}/${scp[2]}`);
  }

  try {
    const parsed = new URL(raw);
    if (!['http:', 'https:', 'ssh:', 'git:'].includes(parsed.protocol)) return null;
    const protocol = parsed.protocol === 'http:' ? 'http:' : 'https:';
    const path = parsed.pathname.replace(/\/+$/, '').replace(/\.git$/i, '');
    if (!parsed.hostname || !path || path === '/') return null;
    const port = parsed.port ? `:${parsed.port}` : '';
    return `${protocol}//${parsed.hostname}${port}${path}`;
  } catch {
    return null;
  }
}

/** Build the forge's browser route for a branch in an attached repository. */
export function repositoryBranchUrl(
  connectionUrl: string | null | undefined,
  branch: string | null | undefined,
  forge: RepositoryForge | null | undefined,
): string | null {
  const repoUrl = repositoryWebUrl(connectionUrl);
  const ref = branch?.trim();
  if (!repoUrl || !ref || !forge) return null;
  const encodedRef = ref.split('/').map(encodeURIComponent).join('/');
  const route = forge === 'github' ? 'tree' : forge === 'gitlab' ? '-/tree' : 'src/branch';
  return `${repoUrl}/${route}/${encodedRef}`;
}

/** Read and validate the structured record written by ``repo_open_pr``. */
export function pullRequestFromJob(job: Job | null): JobPullRequest | null {
  const raw = asRecord(asRecord(job?.context)?.['pull_request']);
  const forge = raw?.['forge'];
  const repo = raw?.['repo'];
  const number = raw?.['number'];
  const head = raw?.['head'];
  const base = raw?.['base'];
  const url = safeHttpUrl(raw?.['url']);
  if (
    typeof forge !== 'string' ||
    !REPOSITORY_FORGES.has(forge as RepositoryForge) ||
    typeof repo !== 'string' ||
    !repo.trim() ||
    typeof number !== 'number' ||
    !Number.isInteger(number) ||
    number < 1 ||
    typeof head !== 'string' ||
    !head.trim() ||
    typeof base !== 'string' ||
    !base.trim() ||
    !url
  ) {
    return null;
  }
  return {
    forge: forge as RepositoryForge,
    repo: repo.trim(),
    number,
    url,
    head: head.trim(),
    base: base.trim(),
  };
}

/** Prefer the connector named by the PR when a job attached several repos. */
export function selectDeliveryRepository(
  datasources: Datasource[],
  pullRequest: JobPullRequest | null,
): Datasource | null {
  const repositories = datasources.filter((item) => item.type === 'repository');
  if (!pullRequest) return repositories.length === 1 ? repositories[0] : null;
  const wanted = pullRequest.repo.replace(/^\/+|\/+$/g, '').toLowerCase();
  const wantedHost = new URL(pullRequest.url).hostname.toLowerCase();
  const matched = repositories.find((item) => {
    const webUrl = repositoryWebUrl(item.connection_url);
    if (!webUrl) return false;
    try {
      const parsed = new URL(webUrl);
      const path = parsed.pathname.replace(/^\/+|\/+$/g, '').toLowerCase();
      return (
        parsed.hostname.toLowerCase() === wantedHost &&
        (path === wanted || path.endsWith(`/${wanted}`))
      );
    } catch {
      return false;
    }
  });
  return matched ?? null;
}

/**
 * Job Review component for approving or continuing frozen jobs.
 *
 * Displayed as a panel in the cockpit layout. When a job in `pending_review`
 * status is selected, shows the frozen job metadata and provides:
 * - Approve button (marks job as completed)
 * - Feedback textarea + Continue button (resumes with feedback)
 */
@Component({
  selector: 'app-job-review',
  standalone: true,
  imports: [
    TranslocoPipe,
    AppButtonComponent,
    AppBadgeComponent,
    AppTextareaComponent,
    AppSpinnerComponent,
    AppCopyFieldComponent,
    JobDiffReviewComponent,
  ],
  template: `
    <div class="review-container">
      @if (!currentJobId()) {
        <div class="empty-state">
          <span class="empty-hint">{{ 'jobReview.empty.selectJob' | transloco }}</span>
        </div>
      } @else if (isLoading()) {
        <div class="loading-state">
          <app-spinner size="lg" tone="accent" />
          <span>{{ 'jobReview.loading' | transloco }}</span>
        </div>
      } @else if (!job()) {
        <div class="empty-state">
          <span class="empty-hint">{{ 'jobReview.empty.notFound' | transloco }}</span>
        </div>
      } @else if (job()!.status !== 'pending_review') {
        <div class="not-review-state">
          <div class="status-info">
            <app-badge [tone]="jobStatusTone(effectiveJobStatus(job()!))" size="sm">
              @if (jobStatusLabelKey(effectiveJobStatus(job()!)); as statusKey) {
                {{ statusKey | transloco }}
              } @else {
                {{ effectiveJobStatus(job()!) }}
              }
            </app-badge>
          </div>
          <span class="status-message">
            {{ 'jobReview.empty.notPending' | transloco }}
          </span>
          <span class="job-desc">{{ job()!.description }}</span>
        </div>
      } @else if (job()!.diff_status === 'pending') {
        <!-- Mode A diff review (see knowledge-history/done/job_cloud_export.md).
             @defer keeps the review surface (and Monaco's loader with it) out
             of the initial bundle, which sits at 2.70MB against a 2.75MB
             hard-error budget. It is only lazy if EVERY usage site defers it —
             the two in persistent-chat.component.ts do too. -->
        @defer {
          <app-job-diff-review
            [jobId]="job()!.id"
            (resolved)="onDiffResolved()"
          />
        } @placeholder {
          <div class="diff-defer-placeholder"></div>
        }
      } @else {
        <!-- Pending Review State -->
        <div class="review-content">
          <!-- Job Info -->
          <div class="section">
            <div class="section-header">{{ 'jobReview.sections.job' | transloco }}</div>
            <div class="job-description">{{ job()!.description }}</div>
            <div class="job-meta">
              <app-copy-field class="meta-item" [label]="'jobReview.meta.jobId' | transloco" [value]="job()!.id" />
              <span class="meta-item">{{ 'jobReview.meta.created' | transloco: {date: formatDate(job()!.created_at)} }}</span>
              @if (job()!.workspace_contract) {
                <span
                  class="meta-item workspace-contract"
                  [class.warning]="job()!.workspace_contract!.state !== 'ready'"
                  [title]="workspaceContractTitle()"
                >{{ workspaceContractSummary() }}</span>
              }
              @if (job()!.workspace_lifecycle; as lifecycle) {
                <span class="meta-item" role="status">{{ ('jobs.lifecycle.state.' + lifecycle.state) | transloco }}</span>
              }
            </div>
          </div>

          <!-- Frozen Job Summary -->
          @if (frozenData()) {
            <div class="section">
              <div class="section-header">{{ 'jobReview.sections.summary' | transloco }}</div>
              <div class="summary-text">{{ frozenData()!.summary || ('jobReview.summaryEmpty' | transloco) }}</div>
            </div>

            <!-- Confidence -->
            @if (frozenData()!.confidence !== undefined) {
              <div class="section">
                <div class="section-header">{{ 'jobReview.sections.confidence' | transloco }}</div>
                <div class="confidence-bar">
                  <div
                    class="confidence-fill"
                    [style.width.%]="(frozenData()!.confidence || 0) * 100"
                    [class.low]="(frozenData()!.confidence || 0) < 0.5"
                    [class.medium]="(frozenData()!.confidence || 0) >= 0.5 && (frozenData()!.confidence || 0) < 0.8"
                    [class.high]="(frozenData()!.confidence || 0) >= 0.8"
                  ></div>
                </div>
                <span class="confidence-label">{{ ((frozenData()!.confidence || 0) * 100).toFixed(0) }}%</span>
              </div>
            }

            <!-- Deliverables -->
            @if (frozenData()!.deliverables && frozenData()!.deliverables!.length > 0) {
              <div class="section">
                <div class="section-header">{{ 'jobReview.sections.deliverables' | transloco }}</div>
                <ul class="deliverables-list">
                  @for (d of frozenData()!.deliverables!; track d) {
                    <li class="deliverable-item">{{ d }}</li>
                  }
                </ul>
              </div>
            }

            <!-- Agent Notes -->
            @if (frozenData()!.notes) {
              <div class="section">
                <div class="section-header">{{ 'jobReview.sections.agentNotes' | transloco }}</div>
                <div class="notes-text">{{ frozenData()!.notes }}</div>
              </div>
            }
          } @else {
            <div class="section">
              <div class="section-header">{{ 'jobReview.sections.summary' | transloco }}</div>
              <div class="summary-text muted">{{ 'jobReview.noFrozenData' | transloco }}</div>
            </div>
          }

          <!-- Delivery Links -->
          @if (
            sourceRepositoryUrl() ||
            deliveryBranchUrl() ||
            pullRequest() ||
            getJobWorkspaceUrl() ||
            hasSnapshot()
          ) {
            <div class="section delivery-section">
              <div class="section-header">{{ 'jobReview.sections.delivery' | transloco }}</div>
              <div class="delivery-links">
                @if (sourceRepositoryUrl()) {
                  <app-button
                    variant="secondary"
                    size="sm"
                    (clicked)="openExternal(sourceRepositoryUrl()!)"
                  >
                    {{ 'jobReview.links.sourceRepository' | transloco }}
                  </app-button>
                }
                @if (deliveryBranchUrl()) {
                  <app-button
                    variant="secondary"
                    size="sm"
                    (clicked)="openExternal(deliveryBranchUrl()!)"
                  >
                    {{ 'jobReview.links.branch' | transloco: { branch: deliveryBranch() } }}
                  </app-button>
                }
                @if (pullRequest(); as pr) {
                  <app-button variant="info" size="sm" (clicked)="openExternal(pr.url)">
                    {{ 'jobReview.links.pullRequest' | transloco: { number: pr.number } }}
                    @if (pullRequestStatus(); as live) {
                      · {{ ('jobReview.links.pullRequestState.' + live.state) | transloco }}
                    } @else if (pullRequestStatusUnavailable()) {
                      · {{ 'jobReview.links.pullRequestState.unavailable' | transloco }}
                    }
                  </app-button>
                }
                @if (pullRequest() && sourceRepository()) {
                  <app-button
                    variant="primary"
                    size="sm"
                    [loading]="reviewSessionLoading()"
                    (clicked)="reviewInSession()"
                  >
                    @if (reviewSessionLoading()) {
                      {{ 'jobReview.actions.preparingReviewSession' | transloco }}
                    } @else {
                      {{ 'jobReview.actions.reviewInSession' | transloco }}
                    }
                  </app-button>
                }
                @if (getJobWorkspaceUrl()) {
                  <app-button variant="ghost" size="sm" (clicked)="openJobWorkspace()">
                    {{ 'jobReview.links.jobWorkspace' | transloco }}
                  </app-button>
                }
                @if (hasSnapshot()) {
                  <app-button
                    variant="ghost"
                    size="sm"
                    [loading]="ideLoading()"
                    (clicked)="openIde()"
                  >
                    @if (ideLoading()) {
                      {{ 'jobReview.links.startingIde' | transloco }}
                    } @else {
                      {{ 'jobReview.links.openIde' | transloco }}
                    }
                  </app-button>
                }
              </div>
              @if (pullRequest() && sourceRepository()) {
                <div class="review-session-hint">
                  {{ 'jobReview.reviewSessionHint' | transloco }}
                </div>
              }
            </div>
          }

          <!-- Cloud folder preview (Mode B "open folder"). Export and open are
               two separate clicks: the export takes longer than the browser's
               transient-activation window, so an auto-open after it would be
               popup-blocked. -->
          @if (job()!.cloud_review_mode === 'open_folder') {
            <div class="section cloud-folder">
              @if (job()!.exported_folder_url) {
                <app-button
                  variant="info"
                  size="sm"
                  (clicked)="openExportedFolder()"
                >
                  {{ 'jobReview.links.openCloudFolder' | transloco }}
                </app-button>
              }
              <app-button
                [variant]="job()!.exported_at ? 'secondary' : 'info'"
                size="sm"
                [loading]="isExportingToCloud()"
                (clicked)="exportToCloud()"
              >
                @if (isExportingToCloud()) {
                  {{ 'jobReview.actions.exportingToCloud' | transloco }}
                } @else if (job()!.exported_at) {
                  {{ 'jobReview.actions.resyncCloudFolder' | transloco }}
                } @else {
                  {{ 'jobReview.actions.exportToCloud' | transloco }}
                }
              </app-button>
              <div class="cloud-folder-hint">
                {{ 'jobReview.cloudFolderDisclaimer' | transloco }}
              </div>
            </div>
          }

          <!-- Actions -->
          <div class="actions-section">
            @if (frozenData()?.freeze_type === 'vm_upgrade_required') {
              <!-- VM Upgrade Required -->
              <div class="vm-upgrade-section">
                <div class="upgrade-info">
                  <div class="upgrade-title">{{ 'jobReview.vmUpgrade.title' | transloco }}</div>
                  <div class="upgrade-reason">
                    {{ 'jobReview.vmUpgrade.reason' | transloco }}
                  </div>
                  @if (frozenData()!.command) {
                    <code class="upgrade-command">{{ frozenData()!.command }}</code>
                  }
                  <div class="upgrade-hint">
                    {{ 'jobReview.vmUpgrade.hint' | transloco }}
                  </div>
                </div>
                <div class="action-group">
                  <app-button
                    variant="warning"
                    [loading]="isUpgrading()"
                    (clicked)="upgradeToVm()"
                  >
                    @if (isUpgrading()) {
                      {{ 'jobReview.vmUpgrade.upgrading' | transloco }}
                    } @else {
                      {{ 'jobReview.vmUpgrade.upgradeToVm' | transloco }}
                    }
                  </app-button>
                  <app-button
                    variant="secondary"
                    [loading]="isResuming()"
                    (clicked)="continueJob()"
                  >
                    @if (isResuming()) {
                      {{ 'jobReview.vmUpgrade.resuming' | transloco }}
                    } @else {
                      {{ 'jobReview.vmUpgrade.resumeWithoutVm' | transloco }}
                    }
                  </app-button>
                </div>
              </div>
            } @else {
              <!-- Approve / Continue (depends on freeze type) -->
              <div class="action-group">
                @if (frozenData()?.freeze_type === 'phase_boundary') {
                  <app-button
                    variant="warning"
                    [loading]="isResuming()"
                    (clicked)="continueJob()"
                  >
                    @if (isResuming()) {
                      {{ 'jobReview.actions.continuing' | transloco }}
                    } @else {
                      {{ 'jobReview.actions.continue' | transloco }}
                    }
                  </app-button>
                } @else {
                  <app-button
                    variant="success"
                    [loading]="isApproving()"
                    (clicked)="confirmingApprove() ? approveJob() : confirmApprove()"
                  >
                    @if (isApproving()) {
                      {{ 'jobReview.actions.approving' | transloco }}
                    } @else if (confirmingApprove()) {
                      {{ 'jobReview.actions.confirmApprove' | transloco }}
                    } @else {
                      {{ 'jobReview.actions.approve' | transloco }}
                    }
                  </app-button>
                }
              </div>
            }

            <!-- Divider -->
            <div class="divider">
              <span class="divider-text">{{ 'jobReview.actions.orContinueFeedback' | transloco }}</span>
            </div>

            <!-- Feedback + Continue -->
            <div class="action-group">
              <app-textarea
                [(value)]="feedbackText"
                [placeholder]="'jobReview.actions.feedbackPlaceholder' | transloco"
                [rows]="4"
              />
              <app-button
                variant="warning"
                [loading]="isResuming()"
                [disabled]="!feedbackText.trim()"
                (clicked)="continueWithFeedback()"
              >
                @if (isResuming()) {
                  {{ 'jobReview.actions.resuming' | transloco }}
                } @else {
                  {{ 'jobReview.actions.continueWithFeedback' | transloco }}
                }
              </app-button>
            </div>
          </div>

          <!-- Result Message -->
          @if (resultMessage()) {
            <div class="result-message" [class.error]="resultIsError()">
              {{ resultMessage() }}
            </div>
          }
        </div>
      }
    </div>
  `,
  styles: [
    `
      :host {
        display: block;
        height: 100%;
        overflow: hidden;
      }

      .review-container {
        display: flex;
        flex-direction: column;
        height: 100%;
        background: var(--panel-bg, var(--panel-bg));
      }

      /* Empty / Loading States */
      .empty-state,
      .loading-state,
      .not-review-state {
        display: flex;
        flex-direction: column;
        align-items: center;
        justify-content: center;
        gap: 12px;
        padding: 40px 20px;
        flex: 1;
        color: var(--text-muted);
        text-align: center;
      }

      .empty-hint {
        font-size: 12px;
        opacity: 0.7;
      }

      .status-message {
        font-size: 12px;
      }

      .job-desc {
        font-size: 11px;
        max-width: 300px;
        opacity: 0.6;
      }

      /* Review Content */
      .review-content {
        flex: 1;
        overflow: auto;
        padding: 12px;
        display: flex;
        flex-direction: column;
        gap: 16px;
      }

      .section {
        display: flex;
        flex-direction: column;
        gap: 6px;
      }

      .section-header {
        font-size: 11px;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.06em;
        color: var(--text-muted);
      }

      .job-description {
        font-size: 13px;
        color: var(--text-primary, var(--text-primary));
        line-height: 1.4;
      }

      .job-meta {
        display: flex;
        gap: 12px;
        font-size: 10px;
        font-family: 'JetBrains Mono', monospace;
        color: var(--text-muted);
      }

      .workspace-contract.warning {
        color: var(--warning);
      }

      .summary-text {
        font-size: 12px;
        color: var(--text-primary, var(--text-primary));
        line-height: 1.5;
        white-space: pre-wrap;
      }

      .summary-text.muted {
        color: var(--text-muted);
        font-style: italic;
      }

      /* Confidence */
      .confidence-bar {
        height: 6px;
        background: var(--surface-0, var(--surface-0));
        border-radius: var(--radius-pill);
        overflow: hidden;
      }

      .confidence-fill {
        height: 100%;
        border-radius: var(--radius-pill);
        transition: width 0.3s ease;
      }

      .confidence-fill.low { background: var(--danger); }
      .confidence-fill.medium { background: var(--warning); }
      .confidence-fill.high { background: var(--success); }

      .confidence-label {
        font-size: 11px;
        font-family: 'JetBrains Mono', monospace;
        color: var(--text-secondary, var(--text-secondary));
      }

      /* Deliverables */
      .deliverables-list {
        margin: 0;
        padding-left: 20px;
        font-size: 12px;
        color: var(--text-primary, var(--text-primary));
      }

      .deliverable-item {
        margin-bottom: 4px;
        font-family: 'JetBrains Mono', monospace;
        font-size: 11px;
      }

      /* Notes */
      .notes-text {
        font-size: 12px;
        color: var(--text-secondary, var(--text-secondary));
        line-height: 1.4;
        white-space: pre-wrap;
        font-style: italic;
      }

      .delivery-section {
        gap: 8px;
      }

      .delivery-links {
        display: flex;
        gap: 8px;
        flex-wrap: wrap;
      }

      .review-session-hint {
        color: var(--text-secondary, var(--text-secondary));
        font-size: 0.8em;
        line-height: 1.4;
      }

      .cloud-folder {
        gap: 6px;
        align-items: flex-start;
      }

      .cloud-folder-hint {
        color: var(--text-secondary, var(--text-secondary));
        font-size: 0.8em;
        font-style: italic;
        line-height: 1.4;
      }

      /* Actions */
      .actions-section {
        display: flex;
        flex-direction: column;
        gap: 12px;
        padding-top: 8px;
        border-top: 1px solid var(--border-color, var(--surface-0));
      }

      .action-group {
        display: flex;
        flex-direction: column;
        gap: 8px;
      }

      .divider {
        text-align: center;
        position: relative;
      }

      .divider::before {
        content: '';
        position: absolute;
        left: 0;
        right: 0;
        top: 50%;
        height: 1px;
        background: var(--border-color, var(--surface-0));
      }

      .divider-text {
        position: relative;
        padding: 0 12px;
        background: var(--panel-bg, var(--panel-bg));
        font-size: 11px;
        color: var(--text-muted);
        text-transform: uppercase;
        letter-spacing: 0.06em;
      }

      /* Result Message */
      .result-message {
        padding: 8px 12px;
        border-radius: var(--radius-control);
        font-size: 12px;
        background: var(--success-tint);
        color: var(--success);
        border: 1px solid var(--success-tint);
      }

      .result-message.error {
        background: var(--danger-tint);
        color: var(--danger);
        border-color: var(--danger-tint);
      }

      /* VM Upgrade Section */
      .vm-upgrade-section {
        display: flex;
        flex-direction: column;
        gap: 12px;
      }

      .upgrade-info {
        display: flex;
        flex-direction: column;
        gap: 6px;
      }

      .upgrade-title {
        font-weight: 600;
        color: var(--warning);
      }

      .upgrade-reason {
        color: var(--text-secondary, var(--text-secondary));
        font-size: 0.85em;
      }

      .upgrade-command {
        display: block;
        padding: 8px 12px;
        background: var(--code-bg, var(--timeline-bg));
        border: 1px solid var(--border-color, var(--surface-0));
        border-radius: var(--radius-tag);
        font-family: monospace;
        font-size: 0.9em;
        color: var(--text-primary, var(--text-primary));
        word-break: break-all;
      }

      .upgrade-hint {
        color: var(--text-secondary, var(--text-secondary));
        font-size: 0.8em;
        font-style: italic;
      }

    `,
  ],
})
export class JobReviewComponent {
  private readonly api = inject(ApiService);
  private readonly data = inject(DataService);
  private readonly transloco = inject(TranslocoService);
  private readonly router = inject(Router);
  private readonly destroyRef = inject(DestroyRef);

  readonly currentJobId = this.data.currentJobId;
  readonly job = signal<Job | null>(null);
  readonly repositoryDatasources = signal<Datasource[]>([]);
  readonly pullRequestStatus = signal<PullRequestStatus | null>(null);
  readonly pullRequestStatusUnavailable = signal(false);
  readonly pullRequest = computed(() => pullRequestFromJob(this.job()));
  readonly sourceRepository = computed(() =>
    selectDeliveryRepository(this.repositoryDatasources(), this.pullRequest()),
  );
  readonly sourceRepositoryUrl = computed(() =>
    repositoryWebUrl(this.sourceRepository()?.connection_url),
  );
  readonly deliveryBranch = computed(() => this.pullRequest()?.head || null);
  readonly deliveryForge = computed(
    () => this.pullRequest()?.forge || this.sourceRepository()?.config?.forge || null,
  );
  readonly deliveryBranchUrl = computed(() =>
    repositoryBranchUrl(
      this.sourceRepository()?.connection_url,
      this.deliveryBranch(),
      this.deliveryForge(),
    ),
  );

  workspaceContractSummary(): string {
    const workspace = this.job()?.workspace_contract;
    if (!workspace) return '';
    return this.transloco.translate('jobs.workspace.summary', {
      requested:
        workspace.requested_backend ?? this.transloco.translate('jobs.workspace.default'),
      assigned:
        workspace.assigned_backend ?? this.transloco.translate('jobs.workspace.unavailable'),
      effective:
        workspace.effective_backend ?? this.transloco.translate('jobs.workspace.unavailable'),
    });
  }

  workspaceContractTitle(): string {
    const workspace = this.job()?.workspace_contract;
    if (!workspace) return '';
    return this.transloco.translate('jobs.workspace.state', {
      state: workspace.state,
      failure: workspace.failure ?? this.transloco.translate('jobs.workspace.none'),
    });
  }
  readonly frozenData = signal<FrozenJobData | null>(null);
  readonly isLoading = signal(false);
  readonly isApproving = signal(false);
  readonly isResuming = signal(false);
  readonly isUpgrading = signal(false);
  readonly resultMessage = signal<string | null>(null);
  readonly resultIsError = signal(false);
  readonly confirmingApprove = signal(false);
  readonly ideLoading = signal(false);
  readonly isExportingToCloud = signal(false);
  readonly reviewSessionLoading = signal(false);
  private readonly maxIdePollAttempts = 100;
  private idePollAttempts = 0;
  private confirmTimeout: ReturnType<typeof setTimeout> | null = null;
  private idePollingInterval: ReturnType<typeof setInterval> | null = null;

  feedbackText = '';

  constructor() {
    this.destroyRef.onDestroy(() => {
      if (this.idePollingInterval) clearInterval(this.idePollingInterval);
    });
    // React to job selection changes
    effect(() => {
      const jobId = this.currentJobId();
      if (jobId) {
        this.loadJob();
      } else {
        this.job.set(null);
        this.repositoryDatasources.set([]);
        this.pullRequestStatus.set(null);
        this.pullRequestStatusUnavailable.set(false);
        this.frozenData.set(null);
        this.resultMessage.set(null);
      }
    });
  }

  /**
   * Called by the Mode A diff-review child after a successful
   * accept/reject. Refresh the job from the server so the parent panel
   * re-renders with the new status (completed) and the diff UI
   * disappears.
   */
  onDiffResolved(): void {
    this.loadJob();
  }

  loadJob(): void {
    const jobId = this.currentJobId();
    if (!jobId) return;

    this.isLoading.set(true);
    this.resultMessage.set(null);
    this.repositoryDatasources.set([]);
    this.pullRequestStatus.set(null);
    this.pullRequestStatusUnavailable.set(false);

    this.api.getJob(jobId).subscribe((job) => {
      this.job.set(job);
      this.isLoading.set(false);

      if (job) {
        this.loadRepositoryDatasources(jobId);
        if (this.pullRequest()) {
          this.loadPullRequestStatus(jobId);
        }
      }

      // If pending_review, also fetch workspace to get frozen job data
      if (job?.status === 'pending_review') {
        this.loadFrozenData(jobId);
      } else {
        this.frozenData.set(null);
      }
    });
  }

  private loadFrozenData(jobId: string): void {
    this.api.getFrozenJobData(jobId).subscribe((data) => {
      this.frozenData.set(data as FrozenJobData | null);
    });
  }

  private loadRepositoryDatasources(jobId: string): void {
    this.api.getJobDatasources(jobId).subscribe((datasources) => {
      if (this.currentJobId() !== jobId) return;
      this.repositoryDatasources.set(datasources.filter((item) => item.type === 'repository'));
    });
  }

  private loadPullRequestStatus(jobId: string): void {
    this.api.getJobPullRequestStatus(jobId).subscribe((status) => {
      if (this.currentJobId() !== jobId) return;
      this.pullRequestStatus.set(status);
      this.pullRequestStatusUnavailable.set(status === null);
    });
  }

  getJobWorkspaceUrl(): string | null {
    const currentJob = this.job();
    const giteaUrl = environment.giteaUrl;
    if (!giteaUrl || !currentJob) return null;
    const repoName = currentJob.repo_name || `job-${currentJob.id}`;
    if (currentJob.branch_name) {
      return `${giteaUrl}/${repoName}/src/branch/${currentJob.branch_name}`;
    }
    return `${giteaUrl}/${repoName}`;
  }

  openJobWorkspace(): void {
    const currentJob = this.job();
    if (!currentJob) return;
    const url = this.getJobWorkspaceUrl();
    if (!url) return;
    this.api.ensureWorkspaceAccess(currentJob.id).subscribe(() => {
      window.open(url, '_blank');
    });
  }

  openExternal(url: string): void {
    const safeUrl = safeHttpUrl(url);
    if (safeUrl) window.open(safeUrl, '_blank', 'noopener');
  }

  reviewInSession(): void {
    const jobId = this.currentJobId();
    if (!jobId || this.reviewSessionLoading()) return;

    this.reviewSessionLoading.set(true);
    this.resultMessage.set(null);
    this.resultIsError.set(false);
    this.api.createJobReviewSession(jobId).subscribe((result) => {
      this.reviewSessionLoading.set(false);
      if (!result?.thread_id) {
        this.resultMessage.set(
          this.transloco.translate('jobReview.messages.reviewSessionFailed'),
        );
        this.resultIsError.set(true);
        return;
      }
      void this.router.navigate(['/sessions', result.thread_id]);
    });
  }

  hasSnapshot(): boolean {
    const currentJob = this.job();
    if (!currentJob) return false;
    // Hide IDE on subjobs — they share the parent's workspace
    if (currentJob.parent_job_id) return false;
    if (currentJob.workspace_lifecycle && currentJob.workspace_lifecycle.state !== 'unsupported') return true;
    // Show IDE button if: live VM, snapshot available, or has Gitea repo
    if (currentJob.status === 'processing') return true;
    // context is JSONB and may arrive as a raw JSON string, so it must be
    // coerced before indexing — a direct index compiles and then silently
    // yields undefined, which here quietly hid the IDE button for any
    // snapshot-only job. Surfaced when Job.context's type was corrected
    // (2026-07-29).
    const snapshot = asRecord(asRecord(currentJob.context)?.['snapshot']);
    if (snapshot?.['status'] === 'available') return true;
    return !!currentJob.repo_name;
  }

  openIde(): void {
    const currentJob = this.job();
    if (!currentJob) return;
    const jobId = currentJob.id;
    if (currentJob.workspace_lifecycle && currentJob.workspace_lifecycle.state !== 'unsupported') {
      this.openVmIde(jobId);
      return;
    }

    this.ideLoading.set(true);
    this.resultMessage.set(null);
    this.resultIsError.set(false);

    this.api.getIdeSession(jobId).subscribe((result) => {
      if (!result) {
        this.ideLoading.set(false);
        return;
      }

      if (result.status === 'active' || result.status === 'idle') {
        this.ideLoading.set(false);
        if (result.code_server_url) {
          window.open(result.code_server_url, '_blank');
        }
        return;
      }

      if (result.status === 'available' || result.status === 'expired' || result.status === 'failed') {
        this.api.startIdeSession(jobId).subscribe((startResult) => {
          if (!startResult) {
            this.resultMessage.set('Failed to start IDE session');
            this.resultIsError.set(true);
            this.ideLoading.set(false);
            return;
          }
          if (startResult.status === 'unavailable' || startResult.status === 'failed') {
            this.resultMessage.set(
              startResult.error || 'Failed to start IDE session',
            );
            this.resultIsError.set(true);
            this.ideLoading.set(false);
            return;
          }
          this.pollIdeSession(jobId);
        });
        return;
      }

      if (result.status === 'restoring') {
        this.pollIdeSession(jobId);
        return;
      }

      if (result.status === 'unavailable') {
        // The orchestrator says why when it knows (a contained browser
        // transport, an unreachable VM); the generic line is the last resort.
        this.resultMessage.set(result.error || 'IDE session is currently unavailable');
        this.resultIsError.set(true);
      }
      this.ideLoading.set(false);
    });
  }

  private openVmIde(jobId: string): void {
    const tab = window.open('', '_blank');
    if (!tab) {
      this.resultMessage.set(this.transloco.translate('jobs.lifecycle.popupBlocked'));
      this.resultIsError.set(true);
      return;
    }
    tab.opener = null;
    tab.document.title = this.transloco.translate('jobs.lifecycle.wakingTab');
    tab.document.body.textContent = this.transloco.translate('jobs.lifecycle.wakingTab');
    this.ideLoading.set(true);
    this.api.startIdeSession(jobId).subscribe(start => {
      const leaseId = start?.access_lease_id;
      if (!start || !leaseId || start.status === 'unavailable') {
        tab.close();
        this.ideLoading.set(false);
        this.resultMessage.set(this.transloco.translate('jobs.lifecycle.ideUnavailable'));
        this.resultIsError.set(true);
        return;
      }
      if (start.status === 'active' && start.code_server_url) {
        tab.location.href = start.code_server_url;
        this.ideLoading.set(false);
        return;
      }
      if (this.idePollingInterval) clearInterval(this.idePollingInterval);
      this.idePollAttempts = 0;
      this.idePollingInterval = setInterval(() => {
        if (tab.closed || ++this.idePollAttempts > this.maxIdePollAttempts) {
          if (this.idePollingInterval) clearInterval(this.idePollingInterval);
          this.idePollingInterval = null;
          this.ideLoading.set(false);
          if (!tab.closed) {
            tab.close();
            this.resultMessage.set(this.transloco.translate('jobs.lifecycle.wakeTimedOut'));
            this.resultIsError.set(true);
          }
          this.api.closeVmIdeLease(jobId, leaseId).subscribe();
          return;
        }
        this.api.getIdeSession(jobId, leaseId).subscribe(status => {
          if (status?.status === 'active' && status.code_server_url) {
            if (this.idePollingInterval) clearInterval(this.idePollingInterval);
            this.idePollingInterval = null;
            this.ideLoading.set(false);
            tab.location.href = status.code_server_url;
          } else if (!status || status.status === 'unavailable' || status.status === 'failed') {
            if (this.idePollingInterval) clearInterval(this.idePollingInterval);
            this.idePollingInterval = null;
            this.ideLoading.set(false);
            tab.close();
            this.api.closeVmIdeLease(jobId, leaseId).subscribe();
            this.resultMessage.set(this.transloco.translate('jobs.lifecycle.ideUnavailable'));
            this.resultIsError.set(true);
          }
        });
      }, 3000);
    });
  }

  private pollIdeSession(jobId: string): void {
    if (this.idePollingInterval) clearInterval(this.idePollingInterval);
    this.idePollAttempts = 0;

    this.idePollingInterval = setInterval(() => {
      this.idePollAttempts += 1;
      if (this.idePollAttempts > this.maxIdePollAttempts) {
        if (this.idePollingInterval) clearInterval(this.idePollingInterval);
        this.idePollingInterval = null;
        this.ideLoading.set(false);
        this.resultMessage.set('IDE session still restoring after 5 minutes');
        this.resultIsError.set(true);
        return;
      }

      this.api.getIdeSession(jobId).subscribe((result) => {
        if (!result) return;

        if (result.status === 'active' || result.status === 'idle') {
          if (this.idePollingInterval) clearInterval(this.idePollingInterval);
          this.idePollingInterval = null;
          this.ideLoading.set(false);
          if (result.code_server_url) {
            window.open(result.code_server_url, '_blank');
          }
        } else if (result.status === 'failed' || result.status === 'unavailable') {
          if (this.idePollingInterval) clearInterval(this.idePollingInterval);
          this.idePollingInterval = null;
          this.ideLoading.set(false);
          this.resultMessage.set(
            result.error || 'Failed to prepare IDE session',
          );
          this.resultIsError.set(true);
        }
      });
    }, 3000);
  }

  confirmApprove(): void {
    this.confirmingApprove.set(true);
    if (this.confirmTimeout) clearTimeout(this.confirmTimeout);
    this.confirmTimeout = setTimeout(() => this.confirmingApprove.set(false), 3000);
  }

  approveJob(): void {
    this.confirmingApprove.set(false);
    if (this.confirmTimeout) {
      clearTimeout(this.confirmTimeout);
      this.confirmTimeout = null;
    }

    const jobId = this.currentJobId();
    if (!jobId) return;

    this.isApproving.set(true);
    this.resultMessage.set(null);

    this.api.approveJob(jobId).subscribe((result) => {
      this.isApproving.set(false);
      if (result) {
        this.resultMessage.set(this.transloco.translate('jobReview.messages.approved'));
        this.resultIsError.set(false);
        // Reload to reflect new status
        this.loadJob();
      } else {
        this.resultMessage.set(this.transloco.translate('jobReview.messages.approveFailed'));
        this.resultIsError.set(true);
      }
    });
  }

  upgradeToVm(): void {
    const jobId = this.currentJobId();
    if (!jobId) return;

    this.isUpgrading.set(true);
    this.resultMessage.set(null);

    this.api.upgradeJobToVm(jobId).subscribe((result) => {
      this.isUpgrading.set(false);
      if (result) {
        this.resultMessage.set(this.transloco.translate('jobReview.messages.upgradeInitiated'));
        this.resultIsError.set(false);
        this.loadJob();
      } else {
        this.resultMessage.set(this.transloco.translate('jobReview.messages.upgradeFailed'));
        this.resultIsError.set(true);
      }
    });
  }

  continueJob(): void {
    const jobId = this.currentJobId();
    if (!jobId) return;

    this.isResuming.set(true);
    this.resultMessage.set(null);

    this.api.resumeJob(jobId).subscribe((result) => {
      this.isResuming.set(false);
      if (result) {
        this.resultMessage.set(this.transloco.translate('jobReview.messages.continuing'));
        this.resultIsError.set(false);
        this.loadJob();
      } else {
        this.resultMessage.set(this.transloco.translate('jobReview.messages.continueFailed'));
        this.resultIsError.set(true);
      }
    });
  }

  continueWithFeedback(): void {
    const jobId = this.currentJobId();
    if (!jobId || !this.feedbackText.trim()) return;

    this.isResuming.set(true);
    this.resultMessage.set(null);

    this.api.resumeJob(jobId, this.feedbackText.trim()).subscribe((result) => {
      this.isResuming.set(false);
      if (result) {
        this.resultMessage.set(this.transloco.translate('jobReview.messages.resumedWithFeedback'));
        this.resultIsError.set(false);
        this.feedbackText = '';
        // Reload to reflect new status
        this.loadJob();
      } else {
        this.resultMessage.set(this.transloco.translate('jobReview.messages.resumeFailed'));
        this.resultIsError.set(true);
      }
    });
  }

  /**
   * Copy the job's declared deliverables into its shared cloud folder. The
   * endpoint is re-syncable, so this doubles as "re-sync" after a
   * resume-with-feedback. Does not open the folder — that's
   * {@link openExportedFolder}, a separate click, because the copy regularly
   * outlives the ~5s of transient user activation a popup needs.
   * exportJobToSharedFolder toasts success/error.
   */
  exportToCloud(): void {
    const jobId = this.currentJobId();
    if (!jobId) return;

    this.isExportingToCloud.set(true);
    this.resultMessage.set(null);

    this.api.exportJobToSharedFolder(jobId).subscribe((result) => {
      this.isExportingToCloud.set(false);
      if (result) {
        this.resultIsError.set(false);
        // Refresh so exported_at ("last synced at") and exported_folder_url
        // land — the latter is what reveals the "Open cloud folder" button.
        this.loadJob();
      } else {
        this.resultMessage.set(
          this.transloco.translate('jobReview.messages.openCloudFolderFailed'),
        );
        this.resultIsError.set(true);
      }
    });
  }

  /** Open the already-exported folder. Synchronous inside the click handler so
   *  the browser keeps user activation and allows the new tab. */
  openExportedFolder(): void {
    const url = this.job()?.exported_folder_url;
    if (url) {
      window.open(url, '_blank', 'noopener');
    }
  }

  /** Delegates to the shared helper — this was copy-pasted in two
   *  components before the job tool card became the third consumer. */
  jobStatusTone(status: string): BadgeTone {
    return sharedJobStatusTone(status);
  }

  protected readonly effectiveJobStatus = effectiveJobStatus;
  protected readonly jobStatusLabelKey = jobStatusLabelKey;

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
