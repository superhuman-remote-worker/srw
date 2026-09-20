import {SidebarToggleComponent} from '../../shell/sidebar-toggle/sidebar-toggle.component';
import {
  Component,
  DestroyRef,
  ElementRef,
  computed,
  inject,
  OnDestroy,
  OnInit,
  signal,
  viewChild,
} from '@angular/core';
import {takeUntilDestroyed} from '@angular/core/rxjs-interop';
import {forkJoin} from 'rxjs';
import {ActivatedRoute, ParamMap, Router} from '@angular/router';
import {ApiService} from '../../core/services/api.service';
import {DataService} from '../../core/services/data.service';
import {UserService} from '../../core/services/user.service';
import {environment} from '../../core/environment';
import {JobSummary} from '../../core/models/audit.model';
import {
  JobDetailPanelComponent,
  type JobDetailState,
} from './job-detail-panel.component';
import {TranslocoPipe, TranslocoService} from '@jsverse/transloco';
import {AppButtonComponent} from '../../ui/button';
import {AppBadgeComponent, type BadgeTone} from '../../ui/badge';
import {AppInputComponent} from '../../ui/input';
import {AppSpinnerComponent} from '../../ui/spinner';
import {AppIconComponent} from '../../ui/icon';
import {AppIconButtonComponent} from '../../ui/icon-button';
import {AppDialogComponent} from '../../ui/dialog';
import {AppToastService} from '../../ui/toast';
import {AppMenuComponent, AppMenuItemComponent, AppMenuTriggerDirective} from '../../ui/menu';
import {ViewportService} from '../../core/services/viewport.service';
import {
  effectiveJobStatus,
  isTerminalJobStatus,
  jobStatusTone as sharedJobStatusTone,
} from '../../core/util/job-status';
import {JobListParams} from '../../core/models/audit.model';
import {MultiSelectOption} from '../../ui/multi-select';
import {JobFilterBarComponent} from './job-filter-bar.component';
import {JobFilterPanelComponent} from './job-filter-panel.component';
import {JobListFooterComponent} from './job-list-footer.component';
import {JobPageSizePreference} from './job-list-preferences';
import {
  DEFAULT_JOB_FILTERS,
  DEFAULT_PAGE_SIZE,
  JobFilterToken,
  JobListFilters,
  PAGE_SIZE_OPTIONS,
  activeFilterTokens,
  clearJobFilters,
  jobFiltersToApiQuery,
  jobFiltersToQueryParams,
  parseJobFilters,
  removeFilterToken,
  setPageSize,
} from './job-filters';

/**
 * Angular's ParamMap keeps repeated keys, which is how multi-value filters
 * travel (`?status=failed&status=paused`). Flatten to the shape the codec
 * takes: a single string when there is one, an array when there are several.
 */
function readParamMap(params: ParamMap): Record<string, string | string[]> {
  const out: Record<string, string | string[]> = {};
  for (const key of params.keys) {
    const all = params.getAll(key);
    out[key] = all.length > 1 ? all : all[0];
  }
  return out;
}

/** A row in the hierarchical job list. */
interface JobRow {
  job: JobSummary;
  depth: number;        // 0 = root, 1 = child
  hasChildren: boolean;
  isChild: boolean;
  /**
   * `'detail'` is the expanded panel for `job`, rendered as its own row so the
   * table keeps one cell grid. It shares the job's id, so the @for must track
   * on kind + id or the two collide.
   */
  kind: 'job' | 'detail';
}

/**
 * Which Mode B cloud affordance a row offers, if any.
 *
 * `'export'` and `'open'` are deliberately two separate actions rather than one
 * button that exports-then-opens: the export routinely runs longer than the ~5s
 * of transient user activation a browser grants a click, so opening from its
 * completion callback gets swallowed by the popup blocker. `'exported'` is the
 * degraded state — the export happened but the cloud backend can't currently
 * hand us a URL, so there's nothing to link to.
 */
export type JobCloudAction = 'none' | 'export' | 'open' | 'exported';

/** Pure decision helper for the above — exported for unit tests, and shared by
 *  the wide-layout button row and the narrow-layout overflow menu so the two
 *  can't drift. */
export function jobCloudAction(job: JobSummary): JobCloudAction {
  if (job.status !== 'completed' || job.cloud_review_mode !== 'open_folder') {
    return 'none';
  }
  if (!job.exported_at) return 'export';
  return job.exported_folder_url ? 'open' : 'exported';
}

/**
 * Job List component that displays jobs with filtering and actions.
 */
@Component({
  selector: 'app-job-list',
  standalone: true,
  imports: [
    SidebarToggleComponent, TranslocoPipe,
    AppButtonComponent,
    AppBadgeComponent,
    AppInputComponent,
    AppSpinnerComponent,
    AppIconComponent,
    AppIconButtonComponent,
    AppDialogComponent,
    AppMenuComponent,
    AppMenuItemComponent,
    AppMenuTriggerDirective,
    JobFilterBarComponent,
    JobFilterPanelComponent,
    JobListFooterComponent,
    JobDetailPanelComponent,
  ],
  template: `
    <div class="job-list-container">
      <!-- Header with filters -->
      <div class="header-bar">
        <app-sidebar-toggle />
        <span class="title">{{ 'jobs.title' | transloco }}</span>
        <div class="header-actions">
          @if (snapshotStats()?.available) {
            <span class="snapshot-stats" [title]="'jobs.tooltip.snapshotStats' | transloco">
              {{ 'jobs.snapshotsSummary' | transloco:{ count: snapshotStats()!.total_snapshots, size: formatBytes(snapshotStats()!.total_size_bytes) } }}
            </span>
          }
          @if (pendingReviewCount() > 0) {
            <app-button
              variant="warning"
              size="sm"
              [ariaLabel]="'jobs.tooltip.reviewQueue' | transloco"
              (clicked)="goToReview()"
            >
              {{ 'jobs.reviewQueue' | transloco:{ count: pendingReviewCount() } }}
            </app-button>
          }
          <!-- Refresh is desktop-only: on mobile the list auto-refreshes every
               30s (and the browser's pull-to-refresh works), so the button is
               dropped to keep the header to the two actions that matter. -->
          @if (!viewport.isMobile()) {
            <app-button
              variant="ghost"
              size="sm"
              [ariaLabel]="
                (livePaused() ? 'jobs.live.resume' : 'jobs.live.pause') | transloco
              "
              (clicked)="toggleLive()"
            >
              {{ (livePaused() ? 'jobs.live.paused' : 'jobs.live.on') | transloco }}
            </app-button>
            <app-button
              variant="secondary"
              size="sm"
              [disabled]="isLoading()"
              (clicked)="refresh()"
            >
              {{ 'jobs.refresh' | transloco }}
            </app-button>
          }
          <app-button
            variant="primary"
            size="sm"
            (clicked)="goToCreate()"
          >
            <app-icon size="sm">add</app-icon> {{ 'jobs.newJob' | transloco }}
          </app-button>
        </div>
      </div>

      <div class="filter-row">
        <app-job-filter-bar
          [filters]="filters()"
          [tokens]="tokens()"
          [statusCounts]="statusCounts()"
          [totalCount]="facetTotal()"
          [search]="searchDraft()"
          [panelOpen]="panelOpen()"
          (statusToggle)="toggleStatus($event)"
          (clearStatuses)="patchFilters({status: []})"
          (searchInput)="onSearchInput($event)"
          (togglePanel)="panelOpen.set(!panelOpen())"
          (removeToken)="onRemoveToken($event)"
          (clearAll)="onClearAll()"
        />
        <app-job-filter-panel
          [open]="panelOpen()"
          [filters]="filters()"
          [projects]="projectOptions()"
          (closed)="panelOpen.set(false)"
          (patch)="patchFilters($event)"
        />
      </div>

      @if (filters().page > 1) {
        <p class="live-note">
          {{ 'jobs.live.pausedOnPage' | transloco:{ page: filters().page } }}
          <button type="button" class="live-note__jump" (click)="goToPage(1)">
            {{ 'jobs.live.backToLive' | transloco }}
          </button>
        </p>
      }

      @if (pendingNewCount() > 0) {
        <button type="button" class="new-jobs-pill" (click)="showNewJobs()">
          {{ 'jobs.live.newJobs' | transloco:{ count: pendingNewCount() } }}
        </button>
      }

      <p class="sr-only" role="status">
        {{ 'jobs.filter.resultAnnouncement' | transloco:{ count: rootCount() } }}
      </p>

      <!-- Loading State -->
      @if (isLoading() && jobs().length === 0) {
        <div class="loading-state">
          <app-spinner size="lg" tone="accent" />
          <span>{{ 'jobs.loading' | transloco }}</span>
        </div>
      }

      <!-- Empty State -->
      @if (!isLoading() && displayRows().length === 0) {
        <div class="empty-state">
          <span class="empty-icon">&#x1F4CB;</span>
          <span>{{ 'jobs.noJobsFound' | transloco }}</span>
          @if (tokens().length > 0) {
            <span class="empty-hint">{{ 'jobs.noJobsHintFilter' | transloco }}</span>
          } @else {
            <span class="empty-hint">{{ 'jobs.noJobsHintEmpty' | transloco }}</span>
          }
        </div>
      }

      <!-- Job Table -->
      <div #tableTop tabindex="-1" class="table-anchor"></div>
      @if (displayRows().length > 0) {
        <div class="table-container">
          <table class="job-table app-table">
            <thead>
              <tr>
                <th class="col-prompt">{{ 'jobs.colJob' | transloco }}</th>
                <th class="col-project">{{ 'jobs.colProject' | transloco }}</th>
                <th class="col-status">{{ 'jobs.colStatus' | transloco }}</th>
                <th class="col-created">{{ 'jobs.colCreated' | transloco }}</th>
                <th class="col-actions">{{ 'jobs.colActions' | transloco }}</th>
              </tr>
            </thead>
            <tbody>
              @for (row of displayRows(); track row.kind + ':' + row.job.id) {
                @if (row.kind === 'detail') {
                  <tr class="detail-row">
                    <td colspan="5">
                      <app-job-detail-panel
                        [job]="row.job"
                        [data]="jobDetails()[row.job.id] ?? null"
                        [childCount]="getChildCount(row.job.id)"
                        (subtreeRequested)="loadSubtreeUsage(row.job.id)"
                        (subjobSelected)="openSubjob($event)"
                        (revealRows)="revealSubjobRows()"
                      />
                    </td>
                  </tr>
                } @else {
                <tr
                  [class.selected]="selectedJobId() === row.job.id"
                  [class.child-row]="row.isChild"
                  (click)="onRowClick(row.job.id, $event)"
                >
                  <td class="prompt-cell">
                    <div class="prompt-inner" [style.padding-left.px]="row.isChild ? 16 : 0">
                      <button
                        class="expand-btn"
                        [class.expanded]="isExpanded(row.job.id)"
                        (click)="toggleExpand(row.job.id); $event.stopPropagation()"
                        [title]="(isExpanded(row.job.id) ? 'jobs.tooltip.collapseDetails' : 'jobs.tooltip.expandDetails') | transloco"
                        [attr.aria-expanded]="isExpanded(row.job.id)"
                        [attr.aria-label]="(isExpanded(row.job.id) ? 'jobs.tooltip.collapseDetails' : 'jobs.tooltip.expandDetails') | transloco"
                      >
                        <span class="expand-chevron">&#9206;</span>
                      </button>
                      @if (!row.isChild && (row.job.subjob_count ?? 0) > 0) {
                        <span
                          class="subjob-count"
                          [class.live]="row.job.status === 'waiting'"
                          [title]="'jobs.tooltip.subjobCount' | transloco: {count: row.job.subjob_count}"
                        >{{ row.job.subjob_count }}</span>
                      }
                      @if (row.isChild) {
                        <span class="child-connector">\u2514</span>
                      }
                      @if (getUserColor(row.job.user_id)) {
                        <span
                          class="user-dot"
                          [style.background]="getUserColor(row.job.user_id)"
                          [title]="getUserName(row.job.user_id)"
                        ></span>
                      }
                      <span class="prompt-text" [title]="row.job.description">
                        {{ row.job.description }}
                      </span>
                    </div>
                    <div class="job-id" [style.padding-left.px]="row.isChild ? 16 : 0">
                      {{ row.job.id }}
                      @if (row.isChild && row.job.config_name) {
                        <span class="config-badge">{{ row.job.config_name }}</span>
                      }
                    </div>
                    @if (row.job.workspace_contract) {
                      <div
                        class="workspace-contract"
                        [class.warning]="row.job.workspace_contract.state !== 'ready'"
                        [style.padding-left.px]="row.isChild ? 16 : 0"
                        [title]="workspaceContractTitle(row.job)"
                      >
                        {{ workspaceContractSummary(row.job) }}
                      </div>
                    }
                    @if (row.job.workspace_recovery) {
                      <div
                        class="workspace-recovery"
                        [class.attention]="row.job.workspace_recovery.state === 'paused_attention'"
                        [style.padding-left.px]="row.isChild ? 16 : 0"
                      >
                        {{ row.job.workspace_recovery.message }}
                      </div>
                    }
                    @if (row.job.status === 'failed' && row.job.error_message) {
                      <div class="job-error" [style.padding-left.px]="row.isChild ? 16 : 0" [title]="row.job.error_message">
                        {{ 'jobs.failureReason' | transloco }}: {{ row.job.error_message }}
                      </div>
                    }
                  </td>
                  <td class="project-cell">
                    <span
                      class="project-name"
                      [class.empty]="!row.job.project_id"
                      [title]="row.job.project_id ? ((row.job.project_name || ('jobs.projectUnknown' | transloco)) + ' · ' + row.job.project_id) : ('jobs.projectNone' | transloco)"
                    >
                      {{ row.job.project_name || (row.job.project_id ? ('jobs.projectUnknown' | transloco) : ('jobs.projectNone' | transloco)) }}
                    </span>
                  </td>
                  <td>
                    <div class="status-cell-inner">
                      @if (row.job.pending_approval) {
                        <app-badge tone="warning" size="sm">
                          {{ 'jobs.status.waiting_approval' | transloco }}
                        </app-badge>
                      } @else {
                        <app-badge [tone]="jobStatusTone(effectiveJobStatus(row.job))" size="sm">
                          {{ 'jobs.status.' + effectiveJobStatus(row.job) | transloco }}
                        </app-badge>
                      }
                      @if (row.job.status === 'waiting' && row.hasChildren) {
                        <span class="delegation-badge" [title]="'jobs.tooltip.delegationWaiting' | transloco">
                          {{ 'jobs.delegationChildren' | transloco:{ count: getChildCount(row.job.id) } }}
                        </span>
                      }
                      @if (row.isChild && row.job.creation_order != null) {
                        <span class="delegation-badge" [title]="'jobs.tooltip.delegationChild' | transloco:{ order: row.job.creation_order }">
                          #{{ row.job.creation_order }}
                        </span>
                      }
                      @if (row.job.snapshot_status === 'available') {
                        <span class="snapshot-badge" [title]="'jobs.tooltip.snapshotAvailable' | transloco">S</span>
                      }
                    </div>
                  </td>
                  <td class="created-cell">
                    {{ formatDate(row.job.created_at) }}
                  </td>
                  <td class="actions-cell">
                    <!-- One primary per row: View (ghost), at most one contextual
                         action the status demands, and the overflow menu that holds
                         everything else. On mobile only the menu renders. -->
                    @if (!viewport.isMobile()) {
                      <app-button
                        variant="ghost"
                        size="sm"
                        [ariaLabel]="'jobs.tooltip.view' | transloco"
                        (clicked)="viewJob(row.job.id); $event.stopPropagation()"
                      >
                        {{ 'jobs.action.view' | transloco }}
                      </app-button>
                      @if (row.job.pending_approval) {
                        <!-- The job is blocked on a sudo/VM-upgrade decision:
                             Resume would do nothing — route to the request. -->
                        <app-button
                          variant="warning"
                          size="sm"
                          [ariaLabel]="'jobs.tooltip.approveRequest' | transloco"
                          (clicked)="goToApproveRequest(row.job); $event.stopPropagation()"
                        >
                          {{ 'jobs.action.approveRequest' | transloco }}
                        </app-button>
                      } @else if (row.job.status === 'pending_review') {
                        <app-button
                          variant="warning"
                          size="sm"
                          [ariaLabel]="'jobs.tooltip.reviewJob' | transloco"
                          (clicked)="reviewJob(row.job.id); $event.stopPropagation()"
                        >
                          {{ 'jobs.action.review' | transloco }}
                        </app-button>
                      } @else if (row.job.workspace_recovery?.retryable) {
                        <app-button
                          variant="success"
                          size="sm"
                          [ariaLabel]="'jobs.tooltip.retryRecovery' | transloco"
                          (clicked)="retryWorkspaceRecovery(row.job); $event.stopPropagation()"
                        >
                          {{ 'jobs.action.retryRecovery' | transloco }}
                        </app-button>
                      } @else if (!row.job.workspace_recovery && !isBlockedUndelivered(row.job) && (row.job.status === 'failed' || row.job.status === 'cancelled' || row.job.status === 'paused' || row.job.status === 'created')) {
                        <app-button
                          variant="success"
                          size="sm"
                          [ariaLabel]="'jobs.tooltip.resumeJob' | transloco"
                          (clicked)="resumeJob(row.job.id); $event.stopPropagation()"
                        >
                          {{ 'jobs.action.resume' | transloco }}
                        </app-button>
                      }
                    }
                    <app-icon-button
                      size="sm"
                      [ariaLabel]="'jobs.tooltip.moreActions' | transloco"
                      [appMenuTrigger]="rowMenu"
                      menuPlacement="bottom-end"
                      (click)="$event.stopPropagation()"
                    >
                      <app-icon size="sm">more_vert</app-icon>
                    </app-icon-button>
                    <app-menu #rowMenu>
                      <app-menu-item (activated)="viewJob(row.job.id)">{{ 'jobs.action.view' | transloco }}</app-menu-item>
                      @if (row.job.pending_approval) {
                        <app-menu-item (activated)="goToApproveRequest(row.job)">{{ 'jobs.action.approveRequest' | transloco }}</app-menu-item>
                      } @else if (row.job.status === 'pending_review') {
                        <app-menu-item (activated)="reviewJob(row.job.id)">{{ 'jobs.action.review' | transloco }}</app-menu-item>
                      } @else if (row.job.workspace_recovery?.retryable) {
                        <app-menu-item (activated)="retryWorkspaceRecovery(row.job)">{{ 'jobs.action.retryRecovery' | transloco }}</app-menu-item>
                      } @else if (row.job.status === 'processing') {
                        <app-menu-item (activated)="pauseJob(row.job.id)">{{ 'jobs.action.pause' | transloco }}</app-menu-item>
                      } @else if (!row.job.workspace_recovery && !isBlockedUndelivered(row.job) && (row.job.status === 'failed' || row.job.status === 'cancelled' || row.job.status === 'paused' || row.job.status === 'created')) {
                        <app-menu-item (activated)="resumeJob(row.job.id)">{{ 'jobs.action.resume' | transloco }}</app-menu-item>
                      }
                      @if (getWorkspaceUrl(row.job)) {
                        <app-menu-item (activated)="openWorkspace(row.job)">{{ 'jobs.action.workspace' | transloco }}</app-menu-item>
                      }
                      @if (canOpenIde(row.job)) {
                        <app-menu-item (activated)="openIde(row.job.id)">
                          @if (ideLoadingJobIds().has(row.job.id)) {
                            {{ 'jobs.action.starting' | transloco }}
                          } @else {
                            {{ 'jobs.action.ide' | transloco }}
                          }
                        </app-menu-item>
                      }
                      @if (row.job.status !== 'completed' && row.job.status !== 'cancelled') {
                        <app-menu-item (activated)="askCancel(row.job)">{{ 'jobs.action.cancel' | transloco }}</app-menu-item>
                      }
                      @if (row.job.status === 'completed' && !row.job.project_id) {
                        <app-menu-item (activated)="togglePromote(row.job.id)">{{ 'jobs.action.promote' | transloco }}</app-menu-item>
                      }
                      @switch (cloudAction(row.job)) {
                        @case ('export') {
                          <app-menu-item (activated)="exportJobToSharedFolder(row.job.id)">{{ 'jobs.action.exportToCloud' | transloco }}</app-menu-item>
                        }
                        @case ('open') {
                          <app-menu-item (activated)="openExportedFolder(row.job)">{{ 'jobs.action.openCloudFolder' | transloco }}</app-menu-item>
                        }
                      }
                      @if (row.job.status !== 'processing' && row.job.status !== 'paused' && row.job.status !== 'reviewing' && row.job.status !== 'waiting') {
                        <app-menu-item tone="danger" (activated)="askDelete(row.job)">{{ 'jobs.action.delete' | transloco }}</app-menu-item>
                      }
                    </app-menu>
                  </td>
                </tr>
                @if (promoteJobId() === row.job.id) {
                  <tr class="promote-row" (click)="$event.stopPropagation()">
                    <td colspan="5">
                      <div class="promote-form">
                        <app-input
                          size="sm"
                          [placeholder]="'jobs.promote.namePlaceholder' | transloco"
                          [value]="promoteName()"
                          (valueChange)="promoteName.set($event)"
                        />
                        <app-input
                          size="sm"
                          [placeholder]="'jobs.promote.descriptionPlaceholder' | transloco"
                          [value]="promoteDescription()"
                          (valueChange)="promoteDescription.set($event)"
                        />
                        <app-input
                          size="sm"
                          [placeholder]="'jobs.promote.goalPlaceholder' | transloco"
                          [value]="promoteGoal()"
                          (valueChange)="promoteGoal.set($event)"
                        />
                        <app-button
                          variant="info"
                          size="sm"
                          [disabled]="!promoteName().trim()"
                          (clicked)="submitPromote(row.job.id)"
                        >
                          {{ 'jobs.action.createProject' | transloco }}
                        </app-button>
                        <app-button
                          variant="secondary"
                          size="sm"
                          (clicked)="promoteJobId.set(null)"
                        >
                          {{ 'jobs.action.cancel' | transloco }}
                        </app-button>
                      </div>
                    </td>
                  </tr>
                }
                }
              }
            </tbody>
          </table>
        </div>
      }

      <!-- Footer -->
      <app-job-list-footer
        [page]="filters().page"
        [pageSize]="filters().pageSize"
        [count]="rootCount()"
        [total]="total()"
        [totalIsCapped]="totalIsCapped()"
        [hasMore]="hasMore()"
        [loading]="isLoading()"
        [pageSizeOptions]="pageSizeOptions"
        (pageChange)="goToPage($event)"
        (pageSizeChange)="onPageSizeChange($event)"
      />

      <!-- Confirm delete — themed dialog (replaces the old inline two-tap; mirrors Sessions). -->
      <app-dialog
        [open]="confirmDeleteOpen()"
        [title]="'jobs.confirmDelete' | transloco"
        size="sm"
        (closed)="confirmDeleteOpen.set(false)"
      >
        {{ pendingJob()?.description }}
        <div appDialogActions>
          <app-button variant="secondary" (clicked)="confirmDeleteOpen.set(false)">
            {{ 'common.cancel' | transloco }}
          </app-button>
          <app-button variant="danger" (clicked)="onConfirmDelete()">
            {{ 'common.delete' | transloco }}
          </app-button>
        </div>
      </app-dialog>

      <!-- Confirm cancel — themed dialog. -->
      <app-dialog
        [open]="confirmCancelOpen()"
        [title]="'jobs.confirmCancel' | transloco"
        size="sm"
        (closed)="confirmCancelOpen.set(false)"
      >
        {{ 'jobs.confirmCancelHint' | transloco }}
        <div appDialogActions>
          <app-button variant="secondary" (clicked)="confirmCancelOpen.set(false)">
            {{ 'jobs.confirmCancelDismiss' | transloco }}
          </app-button>
          <app-button variant="warning" (clicked)="onConfirmCancel()">
            {{ 'jobs.confirmCancelConfirm' | transloco }}
          </app-button>
        </div>
      </app-dialog>
    </div>
  `,
  styles: [
    `
      .filter-row {
        position: relative;
        padding: 0 1rem 0.5rem;
      }

      .table-anchor {
        outline: none;
      }

      .sr-only {
        position: absolute;
        width: 1px;
        height: 1px;
        margin: -1px;
        padding: 0;
        overflow: hidden;
        clip: rect(0 0 0 0);
        white-space: nowrap;
        border: 0;
      }

      .new-jobs-pill {
        position: sticky;
        top: 0.5rem;
        z-index: 100; /* $z-sticky */
        align-self: center;
        margin: 0 auto 0.5rem;
        padding: 0.25rem 0.9rem;
        border: 1px solid var(--accent-color);
        border-radius: var(--radius-pill);
        background: var(--surface-2, var(--surface-1));
        color: var(--accent-color);
        font-size: 0.875rem;
        cursor: pointer;
      }

      .live-note {
        margin: 0 1rem 0.5rem;
        font-size: 0.75rem;
        color: var(--text-muted, #7f849c);
      }

      .live-note__jump {
        margin-left: 0.5rem;
        border: 0;
        background: none;
        color: var(--accent-color);
        cursor: pointer;
        text-decoration: underline;
      }
      :host {
        display: block;
        height: 100%;
        overflow: hidden;
      }

      .job-list-container {
        display: flex;
        flex-direction: column;
        height: 100%;
        /* Short viewports with stacked banners may not fit the filters and
           footer. Keep those controls reachable by scrolling the list. */
        overflow-y: auto;
        background: var(--panel-bg, var(--panel-bg));
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
        flex-wrap: wrap;
      }

      .title {
        font-weight: 600;
        color: var(--text-primary, var(--text-primary));
      }

      .filter-chips {
        display: flex;
        gap: 4px;
        flex-wrap: wrap;
      }

      .filter-chips .count {
        opacity: 0.7;
        font-size: 10px;
        margin-left: 2px;
      }

      .header-actions {
        display: flex;
        align-items: center;
        gap: 8px;
        margin-left: auto;
        flex-wrap: wrap;
      }

      .snapshot-stats {
        font-size: 10px;
        color: var(--text-muted);
        flex-shrink: 0;
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

      /* Table */
      .table-container {
        flex: 1;
        min-height: 6rem;
        overflow-y: auto;
        overflow-x: hidden;
      }

      /* Table look comes from the global .app-table (src/styles/_app-table.scss);
         only column widths and row semantics live here. */

      .col-prompt { width: 100%; }
      .col-project { width: 180px; white-space: nowrap; }
      .col-status { white-space: nowrap; }
      .col-created { white-space: nowrap; }
      .col-actions { white-space: nowrap; }

      .job-table tbody tr {
        cursor: pointer;
      }

      /* Hierarchy */
      .status-cell-inner {
        display: flex;
        align-items: center;
        gap: 4px;
      }

      .detail-row > td {
        padding: 0 12px 10px;
        background: var(--surface-0);
      }
      .expand-btn {
        background: color-mix(in srgb, var(--text-muted) 10%, transparent);
        border: 1px solid color-mix(in srgb, var(--text-muted) 25%, transparent);
        border-radius: var(--radius-control);
        color: var(--text-muted);
        cursor: pointer;
        padding: 2px;
        width: 22px;
        height: 22px;
        display: inline-flex;
        align-items: center;
        justify-content: center;
        flex-shrink: 0;
        transition: all 0.15s ease;
      }

      .expand-btn:hover {
        color: var(--text-primary, var(--text-primary));
        background: color-mix(in srgb, var(--text-muted) 20%, transparent);
        border-color: color-mix(in srgb, var(--text-muted) 40%, transparent);
      }

      .expand-chevron {
        display: inline-block;
        font-size: 14px;
        line-height: 1;
        transition: transform 0.15s ease;
        transform: rotate(90deg);
      }

      .expand-btn.expanded .expand-chevron {
        transform: rotate(180deg);
      }

      /* The UNFILTERED size of the tree under this row. It exists because the
         rendered child count cannot carry this signal: the default origin
         filter hides every subjob, so the row would otherwise look childless
         whether or not it has a whole research job running underneath it. */
      .subjob-count {
        display: inline-flex;
        align-items: center;
        justify-content: center;
        min-width: 16px;
        height: 16px;
        padding: 0 4px;
        border-radius: var(--radius-pill);
        background: var(--surface-2);
        color: var(--text-secondary);
        font-size: 10px;
        line-height: 1;
        flex-shrink: 0;
      }
      /* A waiting job is blocked on a child, so on that status the count is
         not a footnote — it is the explanation for the row's own state. */
      .subjob-count.live {
        background: var(--warning-tint);
        color: var(--warning);
      }

      .child-connector {
        color: var(--text-muted);
        font-size: 11px;
        opacity: 0.5;
        flex-shrink: 0;
      }

      .child-row {
        background: rgba(0, 0, 0, 0.15);
      }

      .child-row:hover {
        background: rgba(0, 0, 0, 0.25) !important;
      }

      .config-badge {
        display: inline-block;
        padding: 1px 5px;
        border-radius: var(--radius-tag);
        font-size: 11px;
        font-weight: 500;
        text-transform: uppercase;
        letter-spacing: 0.06em;
        background: color-mix(in srgb, var(--accent-color) 15%, transparent);
        color: var(--accent-color);
        margin-left: 2px;
        flex-shrink: 0;
      }

      .delegation-badge {
        display: inline-block;
        padding: 1px 5px;
        border-radius: var(--radius-tag);
        font-size: 9px;
        font-weight: 500;
        background: var(--info-tint);
        color: var(--info);
        margin-left: 4px;
        flex-shrink: 0;
        cursor: help;
      }

      .snapshot-badge {
        display: inline-block;
        width: 16px;
        height: 16px;
        border-radius: var(--radius-tag);
        font-size: 9px;
        font-weight: 600;
        line-height: 16px;
        text-align: center;
        background: var(--success-tint);
        color: var(--success);
        margin-left: 4px;
        flex-shrink: 0;
        cursor: help;
      }

      /* User dot */
      .user-dot {
        display: inline-block;
        width: 8px;
        height: 8px;
        border-radius: 50%;
        margin-right: 4px;
        vertical-align: middle;
        flex-shrink: 0;
      }

      /* Prompt/Job Cell */
      .prompt-cell {
        max-width: 0;
        overflow: hidden;
      }

      .prompt-inner {
        display: flex;
        align-items: center;
        gap: 4px;
      }

      .prompt-text {
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
      }

      .job-id {
        font-family: 'JetBrains Mono', monospace;
        font-size: 10px;
        color: var(--text-muted);
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
      }

      .job-error {
        margin-top: 3px;
        font-size: 10px;
        color: var(--danger);
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
      }

      .workspace-contract {
        margin-top: 2px;
        color: var(--text-muted);
        font-size: 10px;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
      }

      .workspace-contract.warning {
        color: var(--warning);
      }

      .workspace-recovery {
        margin-top: 3px;
        color: var(--info);
        font-size: 10px;
      }

      .workspace-recovery.attention {
        color: var(--warning);
      }

      /* Project Cell */
      .project-cell {
        max-width: 180px;
      }

      .project-name {
        display: inline-block;
        max-width: 180px;
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
        vertical-align: middle;
      }

      .project-name.empty {
        color: var(--text-muted);
      }

      /* Created Cell */
      .created-cell {
        font-family: 'JetBrains Mono', monospace;
        font-size: 11px;
        color: var(--text-muted);
      }

      /* Actions */
      .actions-cell {
        white-space: nowrap;
        text-align: right;
      }

      .actions-cell app-button {
        vertical-align: middle;
      }

      .actions-cell app-button + app-button {
        margin-left: 3px;
      }

      /* Footer */
      .footer-bar {
        display: flex;
        align-items: center;
        padding: 8px 12px;
        background: var(--surface-0, var(--surface-0));
        border-top: 1px solid var(--border-color, var(--surface-0));
        flex-shrink: 0;
      }

      .job-count {
        font-size: 11px;
        color: var(--text-muted);
      }

      .promote-row td {
        padding: 0 12px 10px !important;
        border-bottom: 1px solid var(--border-color, var(--surface-0));
      }

      .promote-form {
        display: flex;
        align-items: center;
        gap: 8px;
        flex-wrap: wrap;
        padding: 8px 0;
      }

      .promote-form app-input {
        flex: 1 1 160px;
        min-width: 140px;
      }

      @media (max-width: 768px) {
        .col-created {
          display: none;
        }

        .col-project {
          display: none;
        }

        .created-cell {
          display: none;
        }

        .project-cell {
          display: none;
        }

        .job-id {
          display: none;
        }

        .job-table td {
          padding: 8px 6px;
        }

        .job-table th {
          padding: 8px 6px;
        }

        .job-table {
          table-layout: fixed;
        }

        /* Mobile actions are a single "⋯" kebab (View + everything else live
           inside its menu), so the column only needs room for one icon button.
           That, plus a slightly tighter status column, hands the bulk of the
           width to the job description. */
        .col-prompt {
          width: 62%;
        }

        .col-status {
          width: 24%;
        }

        .col-actions {
          width: 14%;
        }

        /* The column is a lone kebab on mobile; the "ACTIONS" label doesn't
           fit its 14% share and clipped mid-word at the screen edge. (Full
           .job-table prefix: emulated encapsulation stamps [_ngcontent] on
           every selector part, so a bare th.col-actions loses to the base
           .job-table th font-size on specificity.) */
        .job-table th.col-actions {
          font-size: 0;
        }

        /* In the narrower status column, let a secondary badge (delegation
           "N children", snapshot "S", child "#N") wrap under the status pill
           rather than drift right toward the kebab. */
        .status-cell-inner {
          flex-wrap: wrap;
        }

        /* Job description: the only identifier on mobile (id + created columns
           are hidden), so let it wrap to two lines instead of a ~15-char
           single-line ellipsis. */
        .prompt-text {
          font-size: 11px;
          flex: 1;
          min-width: 0;
          white-space: normal;
          overflow: hidden;
          display: -webkit-box;
          -webkit-line-clamp: 2;
          -webkit-box-orient: vertical;
        }

        /* Actions: a single kebab pinned to the right edge — the standard
           overflow-menu position, and it pairs with the menu's bottom-end
           placement (panel right edge aligns under the trigger). */
        .actions-cell {
          text-align: right;
        }

        .actions-cell app-icon-button {
          vertical-align: middle;
        }

        .header-bar {
          padding: 8px;
          gap: 6px 8px;
        }

        /* One-line title bar: the title and the action cluster share row 1 (the
           actions are pushed right by their margin-left:auto), and the filter
           chips drop to row 2 via order:1 below. */
        .snapshot-stats {
          /* Low signal on a phone — often just "0 snapshots". Reclaim the space. */
          display: none;
        }

        /* Filter chips: a single horizontally-scrollable row instead of wrapping
           to three rows (~102px). order:1 puts it on its own line below the
           title + actions; it then scrolls sideways. */
        .filter-chips {
          order: 1;
          gap: 6px;
          flex-wrap: nowrap;
          flex-basis: 100%;
          overflow-x: auto;
          -webkit-overflow-scrolling: touch;
          scrollbar-width: none;
          padding-bottom: 2px;
          /* Soft-fade the right edge so a partially-visible chip reads as
             "swipe for more" rather than looking cut off at the screen edge.
             The mask is fixed to the strip box (not the content), so it always
             feathers the rightmost ~24px of the visible row. */
          -webkit-mask-image: linear-gradient(to right, #000 calc(100% - 24px), transparent);
          mask-image: linear-gradient(to right, #000 calc(100% - 24px), transparent);
        }

        .filter-chips::-webkit-scrollbar {
          display: none;
        }

        .filter-chips app-chip {
          flex-shrink: 0;
        }

        /* Trailing scroll space so that, when scrolled fully right, the last
           chip sits past the fade zone and stays fully legible (otherwise the
           end of the list would look faded too). */
        .filter-chips app-chip:last-child {
          margin-right: 28px;
        }

        /* The global mobile rule gives every selectable chip a 44px touch target,
           which is chunky for a scrollable filter strip. Scope a more compact
           size to this strip so more chips fit on screen: shorter, tighter
           padding, slightly smaller text. (::ng-deep reaches the chip's inner
           button; the .filter-chips prefix keeps it local to the Jobs header.) */
        .filter-chips ::ng-deep .app-chip__btn[data-selectable] {
          min-height: 0;
          height: 30px;
          padding: 0 7px;
          font-size: 10px;
        }

        .filter-chips .count {
          font-size: 9px;
        }

        .table-container {
          overflow-x: hidden;
        }
      }
    `,
  ],
})
export class JobListComponent implements OnInit, OnDestroy {
  private readonly api = inject(ApiService);
  private readonly data = inject(DataService);
  private readonly userService = inject(UserService);
  private readonly transloco = inject(TranslocoService);
  private readonly router = inject(Router);
  protected readonly viewport = inject(ViewportService);
  private readonly route = inject(ActivatedRoute);
  private readonly destroyRef = inject(DestroyRef);
  private readonly pageSizePreference = inject(JobPageSizePreference);
  private readonly toast = inject(AppToastService);

  /** Guards against a slow earlier response overwriting a newer one. */
  private requestSerial = 0;
  private searchDebounce: ReturnType<typeof setTimeout> | null = null;
  private readonly tableTop = viewChild<ElementRef<HTMLElement>>('tableTop');

  readonly jobs = signal<JobSummary[]>([]);
  readonly isLoading = signal(false);
  readonly filters = signal<JobListFilters>(clearJobFilters(DEFAULT_JOB_FILTERS));
  /** Draft of the search box, debounced into `filters` (§7.6). */
  readonly searchDraft = signal('');
  readonly panelOpen = signal(false);

  /** Server-authoritative paging state. */
  readonly total = signal<number | null>(null);
  readonly totalIsCapped = signal(false);
  readonly hasMore = signal(false);
  readonly asOf = signal<string | null>(null);

  /** Disjunctive facet counts — never narrowed by the status selection. */
  readonly statusCounts = signal<Record<string, number>>({});
  readonly facetTotal = signal<number | null>(null);

  readonly projectOptions = signal<MultiSelectOption[]>([]);

  /** Live-refresh state (§7.5). */
  readonly livePaused = signal(false);
  readonly pendingNewCount = signal(0);
  readonly lastUpdatedAt = signal<number | null>(null);

  protected readonly pageSizeOptions = PAGE_SIZE_OPTIONS;
  readonly snapshotStats = signal<{ available: boolean; total_snapshots: number; total_size_bytes: number } | null>(null);
  readonly selectedJobId = signal<string | null>(null);

  // Expand/collapse state for parent jobs
  readonly expandedJobIds = signal<Set<string>>(new Set());
  /**
   * Lazily-loaded panel data, keyed by job id and kept for the life of the view.
   *
   * Cached rather than refetched so collapsing and re-expanding is free, and so
   * expanding five rows costs five loads rather than five per re-open. The list
   * payload cannot supply these: it carries no `config_override` (so no model),
   * no `completed_at` (so no duration) and no usage — verified against the live
   * `/api/jobs` response, which contradicts what the plan assumed.
   */
  readonly jobDetails = signal<Record<string, JobDetailState>>({});

  // In-flight action tracking
  readonly cancelingJobIds = signal<Set<string>>(new Set());
  readonly exportingJobIds = signal<Set<string>>(new Set());
  readonly ideLoadingJobIds = signal<Set<string>>(new Set());
  private idePollingIntervals = new Map<string, ReturnType<typeof setInterval>>();
  /**
   * One idempotency key per displayed recovery operation. A transport error
   * or 409 cannot tell us whether the write committed, so the key survives
   * until a later list read proves that job moved to a successor or resolved.
   */
  private readonly recoveryRetryRequests = new Map<
    string,
    {jobId: string; requestId: string}
  >();

  // Themed confirm-dialog state (delete + cancel), replacing the old inline
  // two-tap confirms — consistent with the Sessions page.
  readonly confirmDeleteOpen = signal(false);
  readonly confirmCancelOpen = signal(false);
  readonly pendingJob = signal<JobSummary | null>(null);

  // Promote form state
  readonly promoteJobId = signal<string | null>(null);
  readonly promoteName = signal('');
  readonly promoteDescription = signal('');
  readonly promoteGoal = signal('');

  private refreshInterval: ReturnType<typeof setInterval> | null = null;

  private readonly projectNames = computed(
    () => new Map(this.projectOptions().map((option) => [option.value, option.label])),
  );

  readonly tokens = computed(() => activeFilterTokens(this.filters(), this.projectNames()));

  /**
   * Display roots on this page. The footer counts roots, not rows, because
   * that is the unit the server pages in — a parent's children ride along
   * with it and are never counted against the page size.
   */
  readonly rootCount = computed(
    () => this.jobs().filter((job) => job.is_display_root !== false).length,
  );

  /** Count of jobs awaiting human review — drives the header Review button. */
  readonly pendingReviewCount = computed(
    () =>
      this.statusCounts()['pending_review'] ??
      this.jobs().filter((job) => job.status === 'pending_review').length,
  );

  /**
   * Flatten the server's tree into renderable rows.
   *
   * The hierarchy arrives already resolved: every row carries the
   * `display_root_id` of the tree it belongs to, and `is_display_root` says
   * which one is the head. There is nothing to re-derive — and nothing can be
   * missing, because the server never splits a tree across pages.
   */
  readonly displayRows = computed<JobRow[]>(() => {
    const jobs = this.jobs();
    const expanded = this.expandedJobIds();

    const childrenByRoot = new Map<string, JobSummary[]>();
    const roots: JobSummary[] = [];
    for (const job of jobs) {
      if (job.is_display_root === false && job.display_root_id) {
        const siblings = childrenByRoot.get(job.display_root_id) ?? [];
        siblings.push(job);
        childrenByRoot.set(job.display_root_id, siblings);
      } else {
        roots.push(job);
      }
    }

    const rows: JobRow[] = [];
    for (const root of roots) {
      const children = childrenByRoot.get(root.id) ?? [];
      const isExpanded = expanded.has(root.id);
      rows.push({
        job: root,
        depth: 0,
        hasChildren: children.length > 0,
        isChild: false,
        kind: 'job',
      });
      if (isExpanded) {
        // One gesture, one expansion, both meanings: the panel first, then the
        // children still nested underneath it. Children keep their own rows
        // rather than moving inside the panel so they stay clickable, sortable
        // and visually nested exactly as before.
        rows.push({
          job: root,
          depth: 0,
          hasChildren: children.length > 0,
          isChild: false,
          kind: 'detail',
        });
        for (const child of children) {
          rows.push({
            job: child,
            depth: 1,
            hasChildren: false,
            isChild: true,
            kind: 'job',
          });
          if (expanded.has(child.id)) {
            rows.push({
              job: child,
              depth: 1,
              hasChildren: false,
              isChild: true,
              kind: 'detail',
            });
          }
        }
      }
    }
    return rows;
  });

  ngOnInit(): void {
    this.pageSizePreference.restore();
    this.route.queryParamMap.pipe(takeUntilDestroyed(this.destroyRef)).subscribe((params) => {
      // A bare /jobs is the default view — human-initiated work only, 61 of
      // 118 rows on the dev cluster — because parseJobFilters treats an absent
      // `origin` as the default rather than as "no filter". No redirect is
      // needed, and the URL stays clean.
      //
      // The default lives in the cockpit rather than in /api/jobs on purpose:
      // the API hides what a user explicitly retired (an archived project),
      // the UI hides what is merely noisy. That keeps "no filters" meaning
      // "every job you can see" for MCP, agents and API-key callers.
      const parsed = parseJobFilters(readParamMap(params));
      // The URL is the source of truth; a hand-edited or stale link degrades
      // silently rather than erroring, because parseJobFilters discards what
      // it cannot validate.
      const next =
        parsed.pageSize === DEFAULT_PAGE_SIZE && !params.has('page_size')
          ? {...parsed, pageSize: this.pageSizePreference.value()}
          : parsed;
      this.filters.set(next);
      this.searchDraft.set(next.search);
      this.load();
      this.loadFacets();
    });

    this.api.getSnapshotStats().subscribe((stats) => {
      if (stats) this.snapshotStats.set(stats);
    });
    this.loadProjects();

    // Poll for changes, but only on page 1. Auto-refresh plus offset paging
    // skips and duplicates rows, so past page 1 the poller is off and the UI
    // says so rather than lying quietly. WCAG 2.2.2 (Level A) also requires a
    // way to pause auto-updating content; livePaused is it.
    this.refreshInterval = setInterval(() => {
      if (this.canPoll()) {
        this.pollForChanges();
        this.reloadOpenPanels({liveOnly: true});
      }
    }, 30000);
  }

  ngOnDestroy(): void {
    if (this.refreshInterval) {
      clearInterval(this.refreshInterval);
    }
    if (this.searchDebounce) {
      clearTimeout(this.searchDebounce);
    }
    for (const interval of this.idePollingIntervals.values()) {
      clearInterval(interval);
    }
    this.idePollingIntervals.clear();
  }

  /** Page 1, not paused, not already loading, and the tab is visible. */
  private canPoll(): boolean {
    return (
      this.filters().page === 1 &&
      !this.livePaused() &&
      !this.isLoading() &&
      !this.panelOpen() &&
      typeof document !== 'undefined' &&
      document.visibilityState === 'visible'
    );
  }

  refresh(): void {
    this.pendingNewCount.set(0);
    this.load();
    this.loadFacets();
    // The panel cache is keyed per job id and otherwise lives for the whole
    // view, so without this an explicit Refresh reloaded the rows underneath
    // an open panel while its tokens and cost stayed frozen at whatever they
    // were when it was first opened.
    this.reloadOpenPanels({liveOnly: false});
  }

  /**
   * Re-fetch the panels that are currently open.
   *
   * `liveOnly` is what separates the two callers. An explicit Refresh means
   * "show me current numbers", so it reloads every open panel. The 30s poller
   * reloads only non-terminal jobs: a finished job's figures cannot change, and
   * refetching three endpoints per open panel every half minute to re-render
   * identical numbers is a poor trade.
   *
   * A panel mid-load is skipped rather than restarted — its request is already
   * newer than the data on screen.
   */
  private reloadOpenPanels(opts: {liveOnly: boolean}): void {
    const expanded = this.expandedJobIds();
    if (expanded.size === 0) return;
    const cache = this.jobDetails();
    const rows = new Map(this.jobs().map((job) => [job.id, job]));
    for (const jobId of expanded) {
      const state = cache[jobId];
      if (!state || state.loading) continue;
      if (opts.liveOnly && isTerminalJobStatus(rows.get(jobId)?.status)) continue;
      // Whether the reader had asked for the subtree figure; a refresh that
      // silently dropped them back to own-spend would be its own small lie.
      const wantSubtree = state.subtreeAttempted;
      this.jobDetails.update((current) => {
        const next = {...current};
        delete next[jobId];
        return next;
      });
      this.loadJobDetail(jobId);
      if (wantSubtree) this.loadSubtreeUsage(jobId);
    }
  }

  /**
   * Fetch one page.
   *
   * A monotonic serial guards against a slow earlier response overwriting a
   * newer one — the specific race a debounced search creates, where the
   * request for "d3" can land after the request for "d30".
   */
  private load(): void {
    const serial = ++this.requestSerial;
    this.isLoading.set(true);
    const query = jobFiltersToApiQuery(this.filters()) as JobListParams;
    // The codec drops the count past page 1 because the client normally
    // carries it from page 1. A cold load of a shared deep link has no page 1
    // to have carried it from, so ask — otherwise the recipient of a "page 3"
    // link sees "76–100" with nothing to anchor it against.
    if (this.total() === null) {
      delete query['include_total'];
    }
    this.api.getJobsPage(query).subscribe({
      next: (page) => {
        if (serial !== this.requestSerial) return;
        this.reconcileRecoveryRetryRequests(page.jobs);
        this.jobs.set(page.jobs);
        this.hasMore.set(page.has_more);
        if (page.total !== null && page.total !== undefined) {
          this.total.set(page.total);
          this.totalIsCapped.set(page.total_is_capped);
        }
        if (page.as_of) this.asOf.set(page.as_of);
        this.pendingNewCount.set(0);
        this.lastUpdatedAt.set(Date.now());
        this.isLoading.set(false);
      },
      error: () => {
        if (serial === this.requestSerial) this.isLoading.set(false);
      },
    });
  }

  /**
   * Chip counts come from a second request on purpose: a facet's own filter
   * must be removed from its counts, or selecting `failed` drops every other
   * chip to (0) and the counts become worse than useless.
   */
  private loadFacets(): void {
    const {status: _status, page: _page, ...rest} = this.filters();
    this.api
      .getJobStatisticsFiltered(jobFiltersToApiQuery({...rest, status: [], page: 1}) as JobListParams)
      .subscribe((stats) => {
        if (!stats) return;
        this.statusCounts.set(stats.by_status ?? {});
        this.facetTotal.set(stats.total_jobs ?? null);
      });
  }

  private loadProjects(): void {
    this.api.getProjects().subscribe((projects) => {
      this.projectOptions.set(
        (projects ?? []).map((project) => ({value: project.id, label: project.name})),
      );
    });
  }

  /**
   * Ask the server whether page 1 has grown, without touching the rendered
   * rows. Splicing rows in would move the row under the user's cursor, and
   * every row here carries Cancel and Delete.
   */
  private pollForChanges(): void {
    const probe = {...this.filters(), page: 1, asOf: null};
    this.api.getJobsPage(jobFiltersToApiQuery(probe) as JobListParams).subscribe((page) => {
      const known = new Set(this.jobs().map((job) => job.id));
      const fresh = page.jobs.filter((job) => !known.has(job.id)).length;
      this.pendingNewCount.set(fresh);
      this.lastUpdatedAt.set(Date.now());
      if (fresh === 0) {
        // Nothing new above the fold: refresh cell state in place. Changing a
        // rendered row's contents is free; inserting above it is not.
        this.jobs.update((current) => {
          const byId = new Map(page.jobs.map((job) => [job.id, job]));
          return current.map((job) => byId.get(job.id) ?? job);
        });
      }
    });
  }

  showNewJobs(): void {
    this.pendingNewCount.set(0);
    this.patchFilters({asOf: null, page: 1});
  }

  toggleLive(): void {
    this.livePaused.update((paused) => !paused);
  }

  // ---------------------------------------------------------------- filters

  /** Every filter change resets to page 1 — offset arithmetic under changed
   *  filters lands somewhere arbitrary. */
  patchFilters(patch: Partial<JobListFilters>): void {
    const next: JobListFilters = {...this.filters(), ...patch};
    if (!('page' in patch)) next.page = 1;
    this.writeUrl(next, {replaceUrl: false});
  }

  toggleStatus(status: string): void {
    const current = this.filters().status;
    this.patchFilters({
      status: current.includes(status)
        ? current.filter((value) => value !== status)
        : [...current, status],
    });
  }

  onSearchInput(value: string): void {
    this.searchDraft.set(value);
    if (this.searchDebounce) clearTimeout(this.searchDebounce);
    // House idiom: a cleared setTimeout, not rxjs. replaceUrl while typing so
    // the back button does not step through every keystroke.
    this.searchDebounce = setTimeout(() => {
      this.searchDebounce = null;
      this.writeUrl({...this.filters(), search: value, page: 1}, {replaceUrl: true});
    }, 250);
  }

  onRemoveToken(token: JobFilterToken): void {
    this.writeUrl(removeFilterToken(this.filters(), token), {replaceUrl: false});
  }

  onClearAll(): void {
    this.searchDraft.set('');
    this.writeUrl(clearJobFilters(this.filters()), {replaceUrl: false});
  }

  goToPage(page: number): void {
    if (page < 1) return;
    // Freeze the window as soon as the user leaves page 1, so rows inserted
    // while they page cannot shift the offset underneath them.
    const asOf = page > 1 ? (this.filters().asOf ?? this.asOf()) : null;
    this.writeUrl({...this.filters(), page, asOf}, {replaceUrl: false});
    this.tableTop()?.nativeElement.focus?.();
  }

  onPageSizeChange(pageSize: number): void {
    this.pageSizePreference.set(pageSize);
    this.writeUrl(setPageSize(this.filters(), pageSize), {replaceUrl: false});
  }

  /**
   * The URL is the source of truth: write it and let the queryParamMap
   * subscription drive state, so a reload, a shared link and an in-app
   * interaction all take exactly the same path.
   */
  private writeUrl(next: JobListFilters, opts: {replaceUrl: boolean}): void {
    this.router.navigate([], {
      relativeTo: this.route,
      queryParams: jobFiltersToQueryParams(next),
      queryParamsHandling: 'merge',
      replaceUrl: opts.replaceUrl,
    });
  }

  /** Delegates to the shared helper — this was copy-pasted in two
   *  components before the job tool card became the third consumer. */
  jobStatusTone(status: string): BadgeTone {
    return sharedJobStatusTone(status);
  }

  effectiveJobStatus(job: JobSummary): string {
    return effectiveJobStatus(job);
  }

  isBlockedUndelivered(job: JobSummary): boolean {
    return job.completion_outcome_kind === 'blocked_undelivered';
  }

  workspaceContractSummary(job: JobSummary): string {
    const workspace = job.workspace_contract;
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

  workspaceContractTitle(job: JobSummary): string {
    const workspace = job.workspace_contract;
    if (!workspace) return '';
    return this.transloco.translate('jobs.workspace.state', {
      state: workspace.state,
      failure: workspace.failure ?? this.transloco.translate('jobs.workspace.none'),
    });
  }

  selectJob(jobId: string): void {
    this.selectedJobId.set(jobId);
  }

  /**
   * Clicking anywhere on a row opens its detail panel.
   *
   * The chevron stays — it is the only keyboard-reachable way in (a `<tr>` with
   * a click handler is not focusable) and it carries the expanded/collapsed
   * state for screen readers. It stops propagation, so a click on it toggles
   * once rather than twice.
   *
   * A drag that selected text is not a click: the job id sits in the row and
   * people copy it constantly, so selecting it must not toggle the panel
   * underneath their cursor.
   */
  onRowClick(jobId: string, event?: MouseEvent): void {
    this.selectJob(jobId);
    if (event && this.hasTextSelection()) return;
    this.toggleExpand(jobId);
  }

  private hasTextSelection(): boolean {
    const selection = typeof window === 'undefined' ? null : window.getSelection();
    return !!selection && !selection.isCollapsed && selection.toString().trim().length > 0;
  }

  toggleExpand(jobId: string): void {
    const current = this.expandedJobIds();
    const next = new Set(current);
    if (next.has(jobId)) {
      next.delete(jobId);
    } else {
      next.add(jobId);
      this.loadJobDetail(jobId);
    }
    this.expandedJobIds.set(next);
  }

  /**
   * Fetch the three things the list row cannot supply, once per job.
   *
   * Runs them concurrently rather than in sequence — a panel that appears in
   * three steps reads as broken — and each call already degrades to null on
   * error inside ApiService, so one failing source still renders the rest. Only
   * a total failure sets `error`.
   */
  private loadJobDetail(jobId: string): void {
    if (this.jobDetails()[jobId]) return; // cached, including a failed attempt
    this.jobDetails.update((cache) => ({
      ...cache,
      [jobId]: {
        loading: true,
        error: false,
        detail: null,
        usage: null,
        progress: null,
        usageSubtree: null,
        loadingSubtree: false,
        subtreeAttempted: false,
        subjobs: null,
        subagents: null,
      },
    }));
    forkJoin({
      detail: this.api.getJob(jobId),
      usage: this.api.getJobUsage(jobId),
      progress: this.api.getJobProgress(jobId),
      // Fetched eagerly, unlike the subtree cost figure. That one is lazy
      // because most jobs are leaves and its answer usually repeats what is
      // already on screen; this one is the opposite — the reader cannot know
      // to ask for it, and for a `waiting` parent it is the single thing that
      // explains the status they opened the panel to understand.
      subjobs: this.api.getJobSubjobs(jobId),
      subagents: this.api.getJobSubagents(jobId),
    })
      .pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe((result) => {
        this.jobDetails.update((cache) => ({
          ...cache,
          [jobId]: {
            ...cache[jobId],
            loading: false,
            error:
              !result.detail &&
              !result.usage &&
              !result.progress &&
              !result.subjobs &&
              !result.subagents,
            detail: result.detail,
            usage: result.usage,
            progress: result.progress,
            subjobs: result.subjobs?.subjobs ?? null,
            subagents: result.subagents?.subagents ?? null,
          },
        }));
      });
  }

  /**
   * Fetch the subtree figure for one job, once, when the panel asks for it.
   *
   * Not fetched alongside the other three: most jobs are leaves, and a request
   * whose answer equals the one already on screen is pure waste. A parent's tree
   * can be many times its own spend, so the toggle is worth the extra call when
   * someone actually reaches for it.
   */
  loadSubtreeUsage(jobId: string): void {
    const state = this.jobDetails()[jobId];
    // `subtreeAttempted`, not `usageSubtree`: a failed call leaves the data null
    // and a data-keyed guard would then refetch on every click.
    if (!state || state.subtreeAttempted) return;
    this.jobDetails.update((cache) => ({
      ...cache,
      [jobId]: {...cache[jobId], loadingSubtree: true, subtreeAttempted: true},
    }));
    this.api
      .getJobUsage(jobId, true)
      .pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe((usageSubtree) => {
        this.jobDetails.update((cache) => ({
          ...cache,
          [jobId]: {...cache[jobId], loadingSubtree: false, usageSubtree},
        }));
      });
  }

  isExpanded(jobId: string): boolean {
    return this.expandedJobIds().has(jobId);
  }

  getWorkspaceUrl(job: JobSummary): string | null {
    const giteaUrl = environment.giteaUrl;
    if (!giteaUrl) return null;
    const repoName = job.repo_name || `job-${job.id}`;
    if (job.branch_name) {
      return `${giteaUrl}/${repoName}/src/branch/${job.branch_name}`;
    }
    return `${giteaUrl}/${repoName}`;
  }

  openWorkspace(job: JobSummary): void {
    const url = this.getWorkspaceUrl(job);
    if (!url) return;
    // Ensure Gitea access is granted (may have been skipped at job creation
    // if user hadn't logged into Gitea yet), then navigate.
    this.api.ensureWorkspaceAccess(job.id).subscribe(() => {
      window.open(url, '_blank');
    });
  }

  hasLiveVm(job: JobSummary): boolean {
    return job.status === 'processing';
  }

  canOpenIde(job: JobSummary): boolean {
    // Show IDE button only on root jobs (subjobs share the parent's workspace)
    if (job.parent_job_id) return false;
    // Show if: live VM, snapshot available, or has Gitea repo
    return this.hasLiveVm(job) || job.snapshot_status === 'available' || !!job.repo_name;
  }

  openIde(jobId: string): void {
    // Mark as loading
    const next = new Set(this.ideLoadingJobIds());
    next.add(jobId);
    this.ideLoadingJobIds.set(next);

    // First check current session status
    this.api.getIdeSession(jobId).subscribe((result) => {
      if (!result) {
        this.removeIdeLoading(jobId);
        return;
      }

      if (result.status === 'active' || result.status === 'idle') {
        // Already active — open directly
        this.removeIdeLoading(jobId);
        if (result.code_server_url) {
          window.open(result.code_server_url, '_blank');
        }
        return;
      }

      if (result.status === 'available' || result.status === 'expired' || result.status === 'failed') {
        // Start a new session
        this.api.startIdeSession(jobId).subscribe((startResult) => {
          if (!startResult || startResult.status === 'unavailable' || startResult.status === 'failed') {
            this.removeIdeLoading(jobId);
            if (startResult?.error) this.toast.warning(startResult.error);
            return;
          }
          // Poll until active
          this.pollIdeSession(jobId);
        });
        return;
      }

      if (result.status === 'restoring') {
        // Already restoring (started from another tab) — just poll
        this.pollIdeSession(jobId);
        return;
      }

      // unavailable or other — stop loading. Say why when the orchestrator
      // told us; a button that silently does nothing reads as a broken page.
      this.removeIdeLoading(jobId);
      if (result.error) this.toast.warning(result.error);
    });
  }

  private pollIdeSession(jobId: string): void {
    // Clear any existing poll for this job
    const existing = this.idePollingIntervals.get(jobId);
    if (existing) clearInterval(existing);

    const interval = setInterval(() => {
      this.api.getIdeSession(jobId).subscribe((result) => {
        if (!result) return;

        if (result.status === 'active' || result.status === 'idle') {
          clearInterval(interval);
          this.idePollingIntervals.delete(jobId);
          this.removeIdeLoading(jobId);
          if (result.code_server_url) {
            window.open(result.code_server_url, '_blank');
          }
        } else if (result.status === 'failed' || result.status === 'unavailable') {
          clearInterval(interval);
          this.idePollingIntervals.delete(jobId);
          this.removeIdeLoading(jobId);
          if (result.error) this.toast.warning(result.error);
        }
        // else 'restoring' — keep polling
      });
    }, 3000);

    this.idePollingIntervals.set(jobId, interval);
  }

  private removeIdeLoading(jobId: string): void {
    const next = new Set(this.ideLoadingJobIds());
    next.delete(jobId);
    this.ideLoadingJobIds.set(next);
  }

  /**
   * Open a subjob picked from a panel's roster.
   *
   * `viewJob` is what the row's own View button does, so a roster click behaves
   * the same way. If the subjob also happens to be on screen as a row — which
   * it is once the reader has widened the origin filter — its panel opens too,
   * so the click lands somewhere visible rather than only changing state the
   * workbench can see.
   */
  openSubjob(jobId: string): void {
    this.viewJob(jobId);
    if (!this.jobs().some((job) => job.id === jobId)) return;
    if (!this.expandedJobIds().has(jobId)) this.toggleExpand(jobId);
  }

  /**
   * Bring hidden subjobs back as real list rows.
   *
   * Adds `subjob` to the origin filter rather than clearing it: the reader
   * asked to see subjobs, not to also unhide automation, loop, officer and
   * bench work. The filter chips then say so, and the change is in the URL like
   * every other filter, so it is shareable and reversible by the back button.
   */
  revealSubjobRows(): void {
    const current = this.filters().origin;
    if (current.length === 0 || current.includes('subjob')) return;
    this.patchFilters({origin: [...current, 'subjob']});
  }

  viewJob(jobId: string): void {
    // Use DataService to switch to this job for workbench panels
    this.data.setCurrentJob(jobId);
    this.selectedJobId.set(jobId);
  }

  /** Open the create form (full page, mirrors Sessions' "New Session"). */
  goToCreate(): void {
    this.router.navigate(['/jobs/new']);
  }

  /**
   * Open the review queue with no job preselected — the review page then
   * auto-selects the first pending_review job. Clearing the current job
   * avoids landing on a previously-viewed (non-pending) job.
   */
  goToReview(): void {
    this.data.setCurrentJob(null);
    this.router.navigate(['/jobs/review']);
  }

  reviewJob(jobId: string): void {
    this.data.setCurrentJob(jobId);
    this.selectedJobId.set(jobId);
    this.router.navigate(['/jobs/review']);
  }

  /**
   * Jump to the job's open sudo/VM-upgrade request in the inbox (same
   * deep-link the notification email uses). Shown in place of Resume while
   * an approval is pending — resuming an approval-blocked job does nothing;
   * the approve/deny decision is what drives it.
   */
  goToApproveRequest(job: JobSummary): void {
    const queryParams = job.pending_approval_request_id
      ? { sudo: job.pending_approval_request_id }
      : undefined;
    this.router.navigate(['/inbox'], { queryParams });
  }

  pauseJob(jobId: string): void {
    this.api.pauseJob(jobId).subscribe((result) => {
      if (result) {
        this.refresh();
      }
    });
  }

  cancelJob(jobId: string): void {
    const next = new Set(this.cancelingJobIds());
    next.add(jobId);
    this.cancelingJobIds.set(next);

    this.api.cancelJob(jobId).subscribe({
      next: (result) => {
        this.removeCanceling(jobId);
        if (result) this.refresh();
      },
      error: () => this.removeCanceling(jobId),
    });
  }

  private removeCanceling(jobId: string): void {
    const next = new Set(this.cancelingJobIds());
    next.delete(jobId);
    this.cancelingJobIds.set(next);
  }

  resumeJob(jobId: string): void {
    this.api.resumeJob(jobId).subscribe((result) => {
      if (result) {
        this.refresh();
      }
    });
  }

  retryWorkspaceRecovery(job: JobSummary): void {
    const recovery = job.workspace_recovery;
    if (!recovery?.retryable) return;
    let pending = this.recoveryRetryRequests.get(recovery.operation_id);
    if (!pending || pending.jobId !== job.id) {
      pending = {jobId: job.id, requestId: crypto.randomUUID()};
      this.recoveryRetryRequests.set(recovery.operation_id, pending);
    }
    this.api
      .retryWorkspaceRecovery(job.id, recovery.operation_id, pending.requestId)
      .subscribe(() => this.refresh());
  }

  private reconcileRecoveryRetryRequests(jobs: JobSummary[]): void {
    if (this.recoveryRetryRequests.size === 0) return;
    const displayed = new Map(jobs.map((job) => [job.id, job]));
    for (const [operationId, pending] of this.recoveryRetryRequests) {
      const job = displayed.get(pending.jobId);
      if (job && job.workspace_recovery?.operation_id !== operationId) {
        this.recoveryRetryRequests.delete(operationId);
      }
    }
  }

  askDelete(job: JobSummary): void {
    this.pendingJob.set(job);
    this.confirmDeleteOpen.set(true);
  }

  askCancel(job: JobSummary): void {
    this.pendingJob.set(job);
    this.confirmCancelOpen.set(true);
  }

  onConfirmDelete(): void {
    const job = this.pendingJob();
    this.confirmDeleteOpen.set(false);
    if (job) this.deleteJob(job.id);
  }

  onConfirmCancel(): void {
    const job = this.pendingJob();
    this.confirmCancelOpen.set(false);
    if (job) this.cancelJob(job.id);
  }

  deleteJob(jobId: string): void {
    this.api.deleteJob(jobId).subscribe((result) => {
      if (result) {
        this.refresh();
        if (this.selectedJobId() === jobId) {
          this.selectedJobId.set(null);
        }
      }
    });
  }

  togglePromote(jobId: string): void {
    if (this.promoteJobId() === jobId) {
      this.promoteJobId.set(null);
    } else {
      this.promoteJobId.set(jobId);
      this.promoteName.set('');
      this.promoteDescription.set('');
      this.promoteGoal.set('');
    }
  }

  submitPromote(jobId: string): void {
    const name = this.promoteName().trim();
    if (!name) return;
    const userId = this.userService.currentUserId();
    if (!userId) return;

    this.api.promoteJob(jobId, {
      name,
      description: this.promoteDescription().trim() || undefined,
      goal: this.promoteGoal().trim() || undefined,
      user_id: userId,
    }).subscribe((result) => {
      if (result) {
        this.promoteJobId.set(null);
        this.refresh();
      }
    });
  }

  /**
   * Export (copy the deliverables into a cloud folder) — deliberately does NOT
   * open the folder. The export routinely takes >5s, which is the lifetime of
   * the browser's transient user activation, so a `window.open` in this
   * callback gets swallowed by the popup blocker. Opening is a second, separate
   * click on the button this refresh reveals ({@link openExportedFolder}).
   */
  exportJobToSharedFolder(jobId: string): void {
    const next = new Set(this.exportingJobIds());
    next.add(jobId);
    this.exportingJobIds.set(next);

    this.api.exportJobToSharedFolder(jobId).subscribe({
      next: (result) => {
        this.removeExporting(jobId);
        if (result) {
          // Re-fetch so exported_at / exported_folder_url land on the row and
          // the button flips to "Open cloud folder".
          this.refresh();
        }
      },
      error: () => this.removeExporting(jobId),
    });
  }

  /** Template binding for {@link jobCloudAction}. */
  cloudAction(job: JobSummary): JobCloudAction {
    return jobCloudAction(job);
  }

  /** Open an already-exported job's cloud folder. Runs synchronously inside
   *  the click handler, so the browser still sees user activation and won't
   *  treat the new tab as an unsolicited popup. */
  openExportedFolder(job: JobSummary): void {
    const url = job.exported_folder_url;
    if (url) {
      window.open(url, '_blank', 'noopener');
    }
  }

  private removeExporting(jobId: string): void {
    const next = new Set(this.exportingJobIds());
    next.delete(jobId);
    this.exportingJobIds.set(next);
  }

  getUserColor(userId?: string | null): string | null {
    if (!userId) return null;
    const user = this.userService.users().find((u) => u.id === userId);
    return user?.avatar_color ?? null;
  }

  getUserName(userId?: string | null): string {
    if (!userId) return 'Unassigned';
    const user = this.userService.users().find((u) => u.id === userId);
    return user?.display_name ?? 'Unknown';
  }

  truncatePrompt(prompt: string | undefined, maxLength: number = 80): string {
    if (!prompt) {
      return '';
    }
    if (prompt.length <= maxLength) {
      return prompt;
    }
    return prompt.slice(0, maxLength) + '...';
  }

  /**
   * Children of a display root, as the server resolved them.
   *
   * This is the FILTERED count: siblings the filter excluded are not here,
   * and are not counted. A bare unfiltered count would make the expander lie
   * about what it is going to reveal.
   */
  getChildCount(parentId: string): number {
    return this.jobs().filter(
      (job) => job.is_display_root === false && job.display_root_id === parentId,
    ).length;
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

  formatBytes(bytes: number): string {
    if (bytes === 0) return '0 B';
    const units = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(1024));
    return `${(bytes / Math.pow(1024, i)).toFixed(1)} ${units[i]}`;
  }
}
