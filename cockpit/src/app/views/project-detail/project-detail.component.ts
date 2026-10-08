import {ChangeDetectionStrategy, Component, computed, DestroyRef, inject, OnDestroy, OnInit, signal} from '@angular/core';
import {takeUntilDestroyed} from '@angular/core/rxjs-interop';
import {forkJoin} from 'rxjs';
import {ActivatedRoute, Router} from '@angular/router';
import {TranslocoPipe, TranslocoService} from '@jsverse/transloco';
import {MarkdownComponent} from 'ngx-markdown';
import {stripMarkdown} from '../../core/util/strip-markdown';
import {effectiveJobStatus} from '../../core/util/job-status';
import {ApiService} from '../../core/services/api.service';
import {CapabilitiesService} from '../../core/services/capabilities.service';
import {ConnectorDriversService} from '../../core/services/connector-drivers.service';
import {offeredAccess} from '../../core/models/connector-driver.model';
import {ErrorMessageService} from '../../core/services/error-message.service';
import {UserService} from '../../core/services/user.service';
import {ViewportService} from '../../core/services/viewport.service';
import {SidebarToggleComponent} from '../../shell/sidebar-toggle/sidebar-toggle.component';
import {AppIconComponent} from '../../ui/icon';
import {AppSpinnerComponent} from '../../ui/spinner';
import {AppButtonComponent} from '../../ui/button';
import {AppIconButtonComponent} from '../../ui/icon-button';
import {AppDialogComponent} from '../../ui/dialog';
import {AppInputComponent} from '../../ui/input';
import {AppInlineEditableTextComponent} from '../../ui/inline-editable-text';
import {AppTextareaComponent} from '../../ui/textarea';
import {AppSelectComponent} from '../../ui/select';
import {AppCheckboxComponent} from '../../ui/checkbox';
import {AppBadgeComponent} from '../../ui/badge';
import {AppFormFieldComponent} from '../../ui/form-field';
import {ProjectLoopComponent} from './project-loop.component';
import {ProjectOfficerComponent} from './project-officer.component';
import {ProjectBacklogComponent} from './project-backlog.component';
import {ExternalImageDirective} from '../../ui/external-image';
import {
    Datasource,
    Expert,
    Job,
    KnowledgeNote,
    KnowledgeNoteDetail,
    KnowledgeSummary,
    Project,
    ProjectArchiveReport,
    ProjectDatasource,
    ProjectMember,
    ProjectMemberRole,
    ProjectRepoRole,
    ProjectRepository,
    ProjectStatus,
    ProjectWorkspaceDefaults,
    User,
    WorkspaceMode,
    WorkspaceTemplateRef,
} from '../../core/models/api.model';

/** A stored template selection (`ProjectWorkspaceDefaults.stored.container`
 *  or `.vm`) is `{"ref": {...}}` for a Settings row or `{"inline": {...}}`
 *  for one the manifest owns; only a `ref` maps to one of this tab's own
 *  `<option>` values. */
function templateKey(selection: Record<string, unknown> | null): string | null {
  const ref = selection?.['ref'] as WorkspaceTemplateRef | undefined;
  return ref ? `${ref.scope.kind}/${ref.scope.name}/${ref.name}` : null;
}

/** Inverse of the option `key` built in `loadWorkspaceDefaults`
 *  (`${scope.kind}/${scope.name}/${name}`) — split on the first two slashes
 *  only, since a template name may itself contain one. */
function templateRef(key: string | null): WorkspaceTemplateRef | null {
  if (!key) return null;
  const [kind, name, ...rest] = key.split('/');
  return {name: rest.join('/'), scope: {kind, name}};
}

type Tab = 'overview' | 'jobs' | 'knowledge' | 'datasources' | 'repos' | 'experts' | 'members' | 'loop' | 'centurion' | 'settings';

@Component({
  selector: 'app-project-detail-page',
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [
    SidebarToggleComponent,
    TranslocoPipe,
    AppIconComponent,
    AppSpinnerComponent,
    AppButtonComponent,
    AppIconButtonComponent,
    AppDialogComponent,
    AppInputComponent,
    AppInlineEditableTextComponent,
    AppTextareaComponent,
    AppSelectComponent,
    AppCheckboxComponent,
    AppBadgeComponent,
    AppFormFieldComponent,
    MarkdownComponent,
    ExternalImageDirective,
    ProjectLoopComponent,
    ProjectOfficerComponent,
    ProjectBacklogComponent,
  ],
  template: `
    <div class="page-container">
      @if (isLoading() && !project()) {
        <div class="loading-state">
          <app-spinner size="lg" tone="accent" />
          <span>{{ 'projectDetail.loading' | transloco }}</span>
        </div>
      }

      @if (project(); as proj) {
        <!-- Header -->
        <div class="page-header">
          <app-sidebar-toggle />
          <app-icon-button
            variant="ghost"
            size="sm"
            [ariaLabel]="'projectDetail.back' | transloco"
            (clicked)="goBack()"
          >
            <app-icon size="sm">arrow_back</app-icon>
          </app-icon-button>
          <div class="header-info">
            <h1 class="page-title">
              @if (isArchived()) {
                <!-- An archived project takes a status-only PATCH, so a rename
                     is refused whole. Do not offer the control at all rather
                     than let the title be typed into and then snap back. -->
                <span class="page-title-static" [title]="proj.name">{{ proj.name }}</span>
              } @else {
                <app-inline-editable-text
                  [value]="proj.name"
                  [clickToEdit]="true"
                  [ariaLabel]="'common.rename' | transloco"
                  (save)="onRenameProject($event)"
                />
              }
            </h1>
            <div class="header-badges">
              @if (proj.is_default) {
                <app-badge tone="accent" size="xs" [uppercase]="true">
                  {{ 'projectDetail.badgePersonal' | transloco }}
                </app-badge>
              }
              <app-badge [tone]="statusTone(proj.status)" size="xs" [uppercase]="true">
                {{ proj.status }}
              </app-badge>
            </div>
          </div>
        </div>

        @if (renameError(); as message) {
          <p class="edit-error" role="alert">{{ message }}</p>
        }

        <!-- Tabs -->
        <div class="tab-bar">
          @for (t of tabList(); track t.id) {
            <button
              class="tab-btn"
              [class.active]="activeTab() === t.id"
              (click)="activeTab.set(t.id)"
            >
              {{ t.labelKey | transloco }}
            </button>
          }
        </div>

        <!-- Tab Content -->
        <div class="tab-content">
          <!-- OVERVIEW TAB -->
          @if (activeTab() === 'overview') {
            <div class="overview-section">
              <div class="detail-grid">
                <div class="detail-card">
                  <label>{{ 'projectDetail.overview.description' | transloco }}</label>
                  @if (isEditingOverview()) {
                    <app-textarea
                      [rows]="3"
                      [value]="editDescription()"
                      (changed)="editDescription.set($event)"
                    />
                  } @else {
                    <p class="detail-value">{{ proj.description || ('projectDetail.overview.noDescription' | transloco) }}</p>
                  }
                </div>
                <div class="detail-card">
                  <label>{{ 'projectDetail.overview.goal' | transloco }}</label>
                  @if (isEditingOverview()) {
                    <app-textarea
                      [rows]="3"
                      [value]="editGoal()"
                      (changed)="editGoal.set($event)"
                    />
                  } @else {
                    <p class="detail-value">{{ proj.goal || ('projectDetail.overview.noGoal' | transloco) }}</p>
                  }
                </div>
              </div>
              <div class="stats-row">
                <div class="stat-card">
                  <span class="stat-value">{{ proj.job_count ?? jobs().length }}</span>
                  <span class="stat-label">{{ 'projectDetail.overview.statsJobs' | transloco }}</span>
                </div>
                <div class="stat-card">
                  <span class="stat-value">{{ projectDatasources().length }}</span>
                  <span class="stat-label">{{ 'projectDetail.overview.statsDatasources' | transloco }}</span>
                </div>
                <div class="stat-card">
                  <span class="stat-value">{{ repos().length }}</span>
                  <span class="stat-label">{{ 'projectDetail.overview.statsRepos' | transloco }}</span>
                </div>
                <div class="stat-card">
                  <span class="stat-value">{{ members().length }}</span>
                  <span class="stat-label">{{ 'projectDetail.overview.statsMembers' | transloco }}</span>
                </div>
              </div>
              @if (proj.default_config_name) {
                <div class="detail-card">
                  <label>{{ 'projectDetail.overview.defaultConfig' | transloco }}</label>
                  <p class="detail-value mono">{{ proj.default_config_name }}</p>
                </div>
              }
              <div class="detail-card">
                <label>{{ 'projectDetail.overview.created' | transloco }}</label>
                <p class="detail-value mono">{{ formatDate(proj.created_at) }}</p>
              </div>
              @if (isArchived()) {
                <p class="archived-note" role="note">
                  {{ 'projectDetail.overview.archivedReadOnly' | transloco }}
                </p>
              }
              @if (editError(); as message) {
                <p class="edit-error" role="alert">{{ message }}</p>
              }
              <div class="overview-actions">
                @if (isEditingOverview()) {
                  <app-button variant="primary" size="md" (clicked)="saveOverview()">
                    {{ 'projectDetail.overview.save' | transloco }}
                  </app-button>
                  <app-button variant="ghost" size="md" (clicked)="cancelEditOverview()">
                    {{ 'projectDetail.overview.cancel' | transloco }}
                  </app-button>
                } @else {
                  @if (!isArchived()) {
                    <app-button variant="ghost" size="md" (clicked)="startEditOverview()">
                      {{ 'projectDetail.overview.edit' | transloco }}
                    </app-button>
                  }
                  <app-button variant="ghost" size="md" (clicked)="openAutomationsForProject()">
                    {{ 'projectDetail.overview.manageAutomations' | transloco }}
                  </app-button>
                  @if (proj.cloud_storage_url) {
                    <a class="ghost-link" [href]="proj.cloud_storage_url" target="_blank" rel="noopener">
                      {{ 'projectDetail.overview.openFolder' | transloco }}
                    </a>
                  }
                }
              </div>
            </div>
          }

          <!-- JOBS TAB -->
          @if (activeTab() === 'jobs') {
            <div class="table-section">
              <div class="tab-toolbar">
                <app-button variant="primary" size="sm" (clicked)="createJobInProject()">
                  {{ 'projectDetail.jobs.new' | transloco }}
                </app-button>
              </div>
              @if (jobs().length === 0) {
                <div class="empty-inline">{{ 'projectDetail.jobs.empty' | transloco }}</div>
              } @else {
                <div class="table-scroll">
                <table class="data-table app-table">
                  <thead>
                    <tr>
                      <th>{{ 'projectDetail.jobs.colStatus' | transloco }}</th>
                      <th>{{ 'projectDetail.jobs.colDescription' | transloco }}</th>
                      <th>{{ 'projectDetail.jobs.colConfig' | transloco }}</th>
                      <th>{{ 'projectDetail.jobs.colBranch' | transloco }}</th>
                      <th>{{ 'projectDetail.jobs.colMerge' | transloco }}</th>
                    </tr>
                  </thead>
                  <tbody>
                    @for (job of jobs(); track job.id) {
                      <tr>
                        <td>
                          <span class="status-badge" [class]="'status-' + effectiveJobStatus(job)">
                            {{ 'jobs.status.' + effectiveJobStatus(job) | transloco }}
                          </span>
                        </td>
                        <td class="desc-cell">{{ truncate(job.description, 60) }}</td>
                        <td class="mono">{{ job.config_name }}</td>
                        <td class="mono">
                          @if (job.repo_name) {
                            {{ job.repo_name }}&#64;{{ job.branch_name || 'main' }}
                          } @else {
                            -
                          }
                        </td>
                        <td>
                          @if (job.delivery_status || job.merge_status) {
                            <span class="merge-badge" [class]="'merge-' + (job.delivery_status || job.merge_status)">
                              {{ job.delivery_status || job.merge_status }}
                            </span>
                          } @else {
                            <span class="text-muted">-</span>
                          }
                        </td>
                      </tr>
                    }
                  </tbody>
                </table>
                </div>
              }
            </div>
          }

          <!-- KNOWLEDGE TAB -->
          @if (activeTab() === 'knowledge') {
            <div class="kb-section">
              <!-- No vault yet: offer to point this project at a knowledge
                   base connector. Replacing an existing vault is deliberately
                   not offered — there is no approved note/history migration
                   (external_forge_knowledge_base.md §8.2). -->
              @if (!hasKnowledgeRepo()) {
                <div class="kb-attach">
                  <div class="kb-attach-title">
                    {{ 'projectDetail.knowledge.attachTitle' | transloco }}
                  </div>
                  <p class="kb-attach-hint">
                    {{ 'projectDetail.knowledge.attachHint' | transloco }}
                  </p>
                  @if (kbConnectors().length === 0) {
                    <p class="kb-attach-hint">
                      {{ 'projectDetail.knowledge.attachNoConnectors' | transloco }}
                    </p>
                    <app-button variant="secondary" size="sm" (clicked)="openConnectors()">
                      {{ 'projectDetail.knowledge.attachCreateConnector' | transloco }}
                    </app-button>
                  } @else {
                    <div class="kb-attach-row">
                      <app-select
                        size="sm"
                        [fullWidth]="false"
                        [value]="kbAttachSelection()"
                        (changed)="kbAttachSelection.set($event ?? '')"
                      >
                        <option value="">
                          {{ 'projectDetail.knowledge.attachSelectPlaceholder' | transloco }}
                        </option>
                        @for (connector of kbConnectors(); track connector.id) {
                          <option [value]="connector.id">{{ connector.name }}</option>
                        }
                      </app-select>
                      <app-button
                        variant="primary"
                        size="sm"
                        [disabled]="!kbAttachSelection() || isAttachingKb()"
                        (clicked)="attachKnowledgeConnector()"
                      >
                        {{ (isAttachingKb()
                              ? 'projectDetail.knowledge.attachSubmitting'
                              : 'projectDetail.knowledge.attachSubmit') | transloco }}
                      </app-button>
                    </div>
                    <p class="kb-attach-hint">
                      {{ 'projectDetail.knowledge.attachAdoptHint' | transloco }}
                    </p>
                  }
                  @if (kbAttachError()) {
                    <p class="kb-attach-error" role="alert">{{ kbAttachError() }}</p>
                  }
                </div>
              }
              <!-- Stats + search/filters act on the list; on mobile, hide them while a
                   single note is open so the note isn't buried under chrome. Desktop
                   keeps them visible (master/detail, plenty of vertical room). -->
              @if (!viewport.isMobile() || !kbSelectedNote()) {
              <!-- Summary Stats -->
              @if (kbSummary(); as summary) {
                <div class="kb-stats-row">
                  <div class="kb-stat">
                    <span class="kb-stat-value">{{ summary.total }}</span>
                    <span class="kb-stat-label">{{ 'projectDetail.knowledge.totalLabel' | transloco }}</span>
                  </div>
                  @for (entry of kbTypeEntries(summary); track entry[0]) {
                    <div class="kb-stat">
                      <span class="kb-stat-value">{{ entry[1] }}</span>
                      <span class="kb-stat-label">{{ entry[0] }}</span>
                    </div>
                  }
                </div>
              }

              <!-- Filters + Search -->
              <div class="kb-toolbar">
                <div class="kb-search-wrap">
                  <app-input
                    size="sm"
                    [placeholder]="'projectDetail.knowledge.searchPlaceholder' | transloco"
                    [value]="kbSearchQuery()"
                    (changed)="kbSearchQuery.set($event)"
                    (keydown.enter)="searchKB()"
                  />
                </div>
                <app-button
                  variant="primary"
                  size="sm"
                  [disabled]="!kbSearchQuery()"
                  (clicked)="searchKB()"
                >
                  {{ 'projectDetail.knowledge.search' | transloco }}
                </app-button>
                <app-select
                  size="sm"
                  [fullWidth]="false"
                  [value]="kbFilterType()"
                  (changed)="onKbFilterTypeChange($event ?? '')"
                >
                  <option value="">{{ 'projectDetail.knowledge.filterAllTypes' | transloco }}</option>
                  <option value="decision">{{ 'projectDetail.knowledge.typeDecision' | transloco }}</option>
                  <option value="learning">{{ 'projectDetail.knowledge.typeLearning' | transloco }}</option>
                  <option value="goal">{{ 'projectDetail.knowledge.typeGoal' | transloco }}</option>
                  <option value="plan">{{ 'projectDetail.knowledge.typePlan' | transloco }}</option>
                  <option value="code">{{ 'projectDetail.knowledge.typeCode' | transloco }}</option>
                  <option value="question">{{ 'projectDetail.knowledge.typeQuestion' | transloco }}</option>
                  <option value="state">{{ 'projectDetail.knowledge.typeState' | transloco }}</option>
                  <option value="source">{{ 'projectDetail.knowledge.typeSource' | transloco }}</option>
                  <option value="retrospective">{{ 'projectDetail.knowledge.typeRetrospective' | transloco }}</option>
                </app-select>
                <app-select
                  size="sm"
                  [fullWidth]="false"
                  [value]="kbFilterStatus()"
                  (changed)="onKbFilterStatusChange($event ?? '')"
                >
                  <option value="">{{ 'projectDetail.knowledge.filterAllStatuses' | transloco }}</option>
                  <option value="active">{{ 'projectDetail.knowledge.statusActive' | transloco }}</option>
                  <option value="resolved">{{ 'projectDetail.knowledge.statusResolved' | transloco }}</option>
                  <option value="superseded">{{ 'projectDetail.knowledge.statusSuperseded' | transloco }}</option>
                  <option value="archived">{{ 'projectDetail.knowledge.statusArchived' | transloco }}</option>
                </app-select>
                <app-button variant="ghost" size="sm" (clicked)="clearKBFilters()">
                  {{ 'projectDetail.knowledge.clear' | transloco }}
                </app-button>
                <span class="grow-spacer"></span>
                <app-button variant="ghost" size="sm" (clicked)="exportKB()">
                  {{ 'projectDetail.knowledge.export' | transloco }}
                </app-button>
              </div>
              }

              <!-- Note Detail View -->
              @if (kbSelectedNote(); as note) {
                <div class="kb-detail">
                  <div class="kb-detail-header">
                    <app-button variant="ghost" size="sm" (clicked)="kbSelectedNote.set(null)">
                      <app-icon size="sm">arrow_back</app-icon>
                      {{ 'projectDetail.knowledge.back' | transloco }}
                    </app-button>
                    <span class="grow-spacer"></span>
                    <app-select
                      size="sm"
                      [fullWidth]="false"
                      [value]="note.status"
                      (changed)="updateNoteStatus(note.note_id, $event ?? '')"
                    >
                      <option value="active">{{ 'projectDetail.knowledge.statusActive' | transloco }}</option>
                      <option value="resolved">{{ 'projectDetail.knowledge.statusResolved' | transloco }}</option>
                      <option value="superseded">{{ 'projectDetail.knowledge.statusSuperseded' | transloco }}</option>
                      <option value="archived">{{ 'projectDetail.knowledge.statusArchived' | transloco }}</option>
                    </app-select>
                    <app-button variant="danger" size="sm" (clicked)="deleteNote(note.note_id)">
                      {{ 'projectDetail.knowledge.delete' | transloco }}
                    </app-button>
                  </div>
                  <div class="kb-detail-meta">
                    <span class="kb-type-badge" [attr.data-type]="note.note_type">{{ note.note_type }}</span>
                    @if (note.confidence) {
                      <span class="kb-confidence">{{ note.confidence }}</span>
                    }
                    @if (note.phase) {
                      <span class="text-muted">{{ 'projectDetail.knowledge.phaseLabel' | transloco:{ phase: note.phase } }}</span>
                    }
                    <span class="text-muted">{{ formatDate(note.modified_at) }}</span>
                  </div>
                  <h2 class="kb-detail-title">{{ note.title }}</h2>
                  @if (note.tags && note.tags.length > 0) {
                    <div class="kb-tags">
                      @for (tag of note.tags; track tag) {
                        <span class="kb-tag">{{ tag }}</span>
                      }
                    </div>
                  }
                  <div class="kb-detail-content"><markdown [data]="note.content"></markdown></div>
                  @if (note.relationships && note.relationships.length > 0) {
                    <div class="kb-relationships">
                      <h4>{{ 'projectDetail.knowledge.relationships' | transloco }}</h4>
                      @for (rel of note.relationships; track rel.target_id) {
                        <div class="kb-rel-item">
                          <span class="kb-rel-type">{{ rel.type }}</span>
                          <button class="kb-rel-link" (click)="openNote(rel.target_id)">
                            {{ rel.target_title || rel.target_id }}
                          </button>
                        </div>
                      }
                    </div>
                  }
                </div>
              } @else {
                <!-- Note List -->
                @if (kbIsLoading()) {
                  <div class="loading-state">
                    <app-spinner size="lg" tone="accent" />
                    <span>{{ 'projectDetail.knowledge.loading' | transloco }}</span>
                  </div>
                } @else if (kbNotes().length === 0) {
                  <div class="empty-inline">{{ 'projectDetail.knowledge.empty' | transloco }}</div>
                } @else {
                  <div class="kb-note-list">
                    @for (note of kbNotes(); track note.note_id) {
                      <div class="kb-note-card" (click)="openNote(note.note_id)">
                        <div class="kb-note-header">
                          <span class="kb-type-badge" [attr.data-type]="note.note_type">{{ note.note_type }}</span>
                          <span class="kb-note-title">{{ note.title }}</span>
                          <span class="kb-note-status" [attr.data-status]="note.status">{{ note.status }}</span>
                        </div>
                        @if (note.content_preview) {
                          <div class="kb-note-preview">{{ notePreview(note.content_preview) }}</div>
                        }
                        <div class="kb-note-footer">
                          @if (note.tags && note.tags.length > 0) {
                            @for (tag of note.tags.slice(0, 4); track tag) {
                              <span class="kb-tag-sm">{{ tag }}</span>
                            }
                            @if (note.tags.length > 4) {
                              <span class="text-muted">+{{ note.tags.length - 4 }}</span>
                            }
                          }
                          @if (note.confidence) {
                            <span class="kb-confidence-sm">{{ note.confidence }}</span>
                          }
                          <span class="text-muted footer-date">{{ formatDate(note.modified_at) }}</span>
                        </div>
                      </div>
                    }
                  </div>
                  @if (kbTotal() > kbNotes().length) {
                    <div class="kb-pagination">
                      <span class="text-muted">{{ 'projectDetail.knowledge.pagination' | transloco:{ current: kbNotes().length, total: kbTotal() } }}</span>
                      <app-button variant="ghost" size="sm" (clicked)="loadMoreKBNotes()">
                        {{ 'projectDetail.knowledge.loadMore' | transloco }}
                      </app-button>
                    </div>
                  }
                }
              }
            </div>
          }

          <!-- DATASOURCES TAB -->
          @if (activeTab() === 'datasources') {
            <div class="table-section">
              <!-- Link Datasource Form -->
              <div class="inline-form">
                <app-input
                  class="datasource-search"
                  size="sm"
                  [value]="datasourceCandidateSearch()"
                  (valueChange)="onDatasourceCandidateSearch($event)"
                  [placeholder]="'projectDetail.datasources.searchPlaceholder' | transloco"
                  [disabled]="datasourceCandidatesLoading()"
                />
                <app-select
                  size="sm"
                  [value]="dsLinkId()"
                  (changed)="dsLinkId.set($event ?? '')"
                  [disabled]="datasourceCandidatesLoading() || datasourceCandidatesError()"
                >
                  <option value="">{{ 'projectDetail.datasources.selectPlaceholder' | transloco }}</option>
                  @for (ds of availableDatasources(); track ds.id) {
                    <option [value]="ds.id">
                      {{ ds.name }} ({{ 'datasources.filter.' + ds.type | transloco }})
                    </option>
                  }
                </app-select>
                <app-button
                  variant="primary"
                  size="sm"
                  [disabled]="!dsLinkId() || datasourceCandidatesLoading() || datasourceCandidatesError()"
                  (clicked)="linkDatasource()"
                >
                  {{ 'projectDetail.datasources.link' | transloco }}
                </app-button>
              </div>
              @if (datasourceCandidatesLoading()) {
                <div class="datasource-candidate-state">
                  <app-spinner size="sm" />
                  <span>{{ 'projectDetail.datasources.loadingCandidates' | transloco }}</span>
                </div>
              } @else if (datasourceCandidatesError()) {
                <div class="datasource-candidate-state error" role="alert">
                  <span>{{ 'projectDetail.datasources.candidatesLoadFailed' | transloco }}</span>
                  <app-button variant="secondary" size="sm" (clicked)="loadDatasourceCandidates()">
                    {{ 'projectDetail.datasources.retry' | transloco }}
                  </app-button>
                </div>
              } @else if (availableDatasources().length === 0) {
                <div class="datasource-candidate-state">
                  {{ 'projectDetail.datasources.noCandidates' | transloco }}
                </div>
              }
              @if (datasourceCandidatesNextCursor() && !datasourceCandidatesError()) {
                <div class="datasource-candidate-actions">
                  <app-button
                    variant="secondary"
                    size="sm"
                    [loading]="datasourceCandidatesLoadingMore()"
                    [disabled]="datasourceCandidatesLoadingMore()"
                    (clicked)="loadMoreDatasourceCandidates()"
                  >
                    {{ 'datasources.catalog.loadMore' | transloco }}
                  </app-button>
                </div>
              }

              @if (projectDatasources().length === 0) {
                <div class="empty-inline">{{ 'projectDetail.datasources.empty' | transloco }}</div>
              } @else {
                <div class="table-scroll">
                <table class="data-table app-table">
                  <thead>
                    <tr>
                      <th>{{ 'projectDetail.datasources.colName' | transloco }}</th>
                      <th>{{ 'projectDetail.datasources.colType' | transloco }}</th>
                      <th>{{ 'projectDetail.datasources.colDescription' | transloco }}</th>
                      <th>{{ 'projectDetail.datasources.colAccess' | transloco }}</th>
                      <th>{{ 'projectDetail.datasources.colActions' | transloco }}</th>
                    </tr>
                  </thead>
                  <tbody>
                    @for (ds of projectDatasources(); track ds.id) {
                      <tr>
                        <td>{{ ds.name }}</td>
                        <td>
                          <span class="role-badge" [class]="'role-' + ds.type">
                            {{ 'datasources.filter.' + ds.type | transloco }}
                          </span>
                        </td>
                        <td>
                          <app-input
                            size="sm"
                            [value]="ds.project_description || ds.description || ''"
                            [placeholder]="'projectDetail.datasources.descriptionPlaceholder' | transloco"
                            (blurred)="updateDatasourceDescription(ds.id, $any($event.target).value)"
                          />
                        </td>
                        <td>
                          <!-- Only the levels the connector's driver offers
                               (capability matrix): one level is shown, not chosen. -->
                          @switch (linkAccess(ds)) {
                            @case ('read_only') {
                              <app-badge
                                tone="info"
                                size="sm"
                                [title]="linkAccessHint(ds) || ('projectDetail.datasources.accessKbReadOnlyHint' | transloco)"
                              >
                                {{ 'projectDetail.datasources.accessReadOnly' | transloco }}
                              </app-badge>
                            }
                            @case ('read_write') {
                              <app-badge tone="neutral" size="sm" [title]="linkAccessHint(ds)">
                                {{ 'projectDetail.datasources.accessReadWrite' | transloco }}
                              </app-badge>
                            }
                            @default {
                              <app-select
                                size="sm"
                                [value]="boolToText(ds.project_read_only)"
                                (changed)="updateDatasourceReadOnly(ds.id, $event ?? '')"
                              >
                                <option value="">{{ 'projectDetail.datasources.accessDefault' | transloco }}</option>
                                <option value="true">{{ 'projectDetail.datasources.accessReadOnly' | transloco }}</option>
                                <option value="false">{{ 'projectDetail.datasources.accessReadWrite' | transloco }}</option>
                              </app-select>
                            }
                          }
                        </td>
                        <td>
                          <app-button variant="danger" size="sm" (clicked)="unlinkDatasource(ds.id)">
                            {{ 'projectDetail.datasources.unlink' | transloco }}
                          </app-button>
                        </td>
                      </tr>
                    }
                  </tbody>
                </table>
                </div>
              }
            </div>
          }

          <!-- REPOS TAB -->
          @if (activeTab() === 'repos') {
            <div class="table-section">
              <!-- Add Repo Form -->
              <div class="inline-form">
                <app-input
                  size="sm"
                  [placeholder]="'projectDetail.repos.namePlaceholder' | transloco"
                  [value]="repoName()"
                  (changed)="repoName.set($event)"
                />
                <app-input
                  size="sm"
                  [placeholder]="'projectDetail.repos.urlPlaceholder' | transloco"
                  [value]="repoUrl()"
                  (changed)="repoUrl.set($event)"
                />
                <app-select
                  size="sm"
                  [fullWidth]="false"
                  [value]="repoRole()"
                  (changed)="onRepoRoleChange($event)"
                >
                  <option value="source">{{ 'projectDetail.repos.roleSource' | transloco }}</option>
                  <option value="reference">{{ 'projectDetail.repos.roleReference' | transloco }}</option>
                </app-select>
                <app-checkbox
                  size="sm"
                  [checked]="repoReadOnly()"
                  (changed)="repoReadOnly.set($event)"
                >
                  {{ 'projectDetail.repos.readOnlyLabel' | transloco }}
                </app-checkbox>
                <app-checkbox
                  size="sm"
                  [checked]="repoCreateManaged()"
                  (changed)="repoCreateManaged.set($event)"
                >
                  {{ 'projectDetail.repos.managedLabel' | transloco }}
                </app-checkbox>
                <app-button
                  variant="primary"
                  size="sm"
                  [disabled]="!repoName().trim()"
                  (clicked)="addRepo()"
                >
                  {{ 'projectDetail.repos.add' | transloco }}
                </app-button>
              </div>

              @if (repos().length === 0) {
                <div class="empty-inline">{{ 'projectDetail.repos.empty' | transloco }}</div>
              } @else {
                <div class="table-scroll">
                <table class="data-table app-table">
                  <thead>
                    <tr>
                      <th>{{ 'projectDetail.repos.colRole' | transloco }}</th>
                      <th>{{ 'projectDetail.repos.colName' | transloco }}</th>
                      <th>{{ 'projectDetail.repos.colUrl' | transloco }}</th>
                      <th>{{ 'projectDetail.repos.colReadOnly' | transloco }}</th>
                      <th>{{ 'projectDetail.repos.colManaged' | transloco }}</th>
                      <th>{{ 'projectDetail.repos.colActions' | transloco }}</th>
                    </tr>
                  </thead>
                  <tbody>
                    @for (repo of repos(); track repo.id) {
                      <tr>
                        <td>
                          <span class="role-badge" [class]="'role-' + repo.role">
                            {{ repo.role }}
                          </span>
                        </td>
                        <td>{{ repo.name }}</td>
                        <td class="mono url-cell">{{ repo.repo_url || '-' }}</td>
                        <td>{{ (repo.read_only ? 'projectDetail.repos.yes' : 'projectDetail.repos.no') | transloco }}</td>
                        <td>{{ (repo.is_managed ? 'projectDetail.repos.yes' : 'projectDetail.repos.no') | transloco }}</td>
                        <td>
                          <!-- The jobs repo is the project's own repo; the API
                               rejects removing it (400). Don't offer a button
                               that can only fail. -->
                          @if (repo.role !== 'jobs') {
                            <app-button variant="danger" size="sm" (clicked)="removeRepo(repo.id)">
                              {{ 'projectDetail.repos.remove' | transloco }}
                            </app-button>
                          } @else {
                            <span class="text-muted">{{ 'projectDetail.repos.protected' | transloco }}</span>
                          }
                        </td>
                      </tr>
                    }
                  </tbody>
                </table>
                </div>
              }
            </div>
          }

          <!-- EXPERTS TAB -->
          @if (activeTab() === 'experts') {
            <div class="table-section">
              @if (isLoadingExperts()) {
                <div class="empty-inline">
                  <app-spinner size="sm" tone="accent" />
                  {{ 'projectDetail.experts.loading' | transloco }}
                </div>
              } @else if (projectExperts().length === 0) {
                <div class="empty-inline">
                  {{ 'projectDetail.experts.emptyPrefix' | transloco }}
                </div>
              } @else {
                <div class="expert-grid">
                  @for (expert of projectExperts(); track expert.id) {
                    <div
                      class="expert-card"
                      [style.--expert-color]="expert.color"
                    >
                      <app-icon size="inherit" class="expert-icon" [style.color]="expert.color">{{ expert.icon }}</app-icon>
                      <span class="expert-name">{{ expert.display_name }}</span>
                      <span class="expert-desc">{{ expert.description }}</span>
                      @if (expert.tags.length > 0) {
                        <div class="expert-tags">
                          @for (tag of expert.tags; track tag) {
                            <span class="expert-tag">{{ tag }}</span>
                          }
                        </div>
                      }
                    </div>
                  }
                </div>
              }
            </div>
          }

          <!-- MEMBERS TAB -->
          @if (activeTab() === 'members') {
            <div class="table-section">
              <!-- Add Member Form -->
              <div class="inline-form">
                <app-select
                  size="sm"
                  [value]="memberUserId()"
                  (changed)="memberUserId.set($event ?? '')"
                >
                  <option value="">{{ 'projectDetail.members.selectUser' | transloco }}</option>
                  @for (user of availableUsers(); track user.id) {
                    <option [value]="user.id">{{ user.display_name }}</option>
                  }
                </app-select>
                <app-select
                  size="sm"
                  [fullWidth]="false"
                  [value]="memberRole()"
                  (changed)="onMemberRoleSelectChange($event)"
                >
                  <option value="editor">{{ 'projectDetail.members.roleEditor' | transloco }}</option>
                  <option value="viewer">{{ 'projectDetail.members.roleViewer' | transloco }}</option>
                  <option value="owner">{{ 'projectDetail.members.roleOwner' | transloco }}</option>
                </app-select>
                <app-button
                  variant="primary"
                  size="sm"
                  [disabled]="!memberUserId()"
                  (clicked)="addMember()"
                >
                  {{ 'projectDetail.members.add' | transloco }}
                </app-button>
              </div>

              @if (members().length === 0) {
                <div class="empty-inline">{{ 'projectDetail.members.empty' | transloco }}</div>
              } @else {
                <div class="table-scroll">
                <table class="data-table app-table">
                  <thead>
                    <tr>
                      <th>{{ 'projectDetail.members.colUser' | transloco }}</th>
                      <th>{{ 'projectDetail.members.colRole' | transloco }}</th>
                      <th>{{ 'projectDetail.members.colJoined' | transloco }}</th>
                      <th>{{ 'projectDetail.members.colActions' | transloco }}</th>
                    </tr>
                  </thead>
                  <tbody>
                    @for (member of members(); track member.user_id) {
                      <tr>
                        <td>
                          <div class="user-cell">
                            <span
                              class="user-avatar-sm"
                              [style.background]="member.avatar_color || 'var(--accent-color)'"
                            >{{ getInitials(member.display_name || '?') }}</span>
                            <span>{{ member.display_name || member.user_id.slice(0, 8) }}</span>
                          </div>
                        </td>
                        <td>
                          <app-select
                            size="sm"
                            [fullWidth]="false"
                            [value]="member.role"
                            (changed)="onRowMemberRoleChange(member.user_id, $event)"
                          >
                            <option value="owner">{{ 'projectDetail.members.roleOwnerLower' | transloco }}</option>
                            <option value="editor">{{ 'projectDetail.members.roleEditorLower' | transloco }}</option>
                            <option value="viewer">{{ 'projectDetail.members.roleViewerLower' | transloco }}</option>
                          </app-select>
                        </td>
                        <td class="mono">{{ formatDate(member.joined_at) }}</td>
                        <td>
                          <app-button
                            variant="danger"
                            size="sm"
                            [disabled]="member.role === 'owner' && ownerCount() <= 1"
                            (clicked)="removeMember(member.user_id)"
                          >
                            {{ 'projectDetail.members.remove' | transloco }}
                          </app-button>
                        </td>
                      </tr>
                    }
                  </tbody>
                </table>
                </div>
              }
            </div>
          }

          <!-- SETTINGS TAB -->
          <!-- Loop + Centurion are gated on unattended_operations. The tab
               buttons are already filtered out below; these guards additionally
               cover a grant revoked while the tab is open (capabilities reload)
               so the surface disappears rather than lingering. -->
          @if (activeTab() === 'loop' && canRunUnattendedOperations()) {
            <app-project-loop [projectId]="project()?.id ?? ''" />
            <app-project-backlog [projectId]="project()?.id ?? ''" />
          }

          @if (activeTab() === 'centurion' && canRunUnattendedOperations()) {
            <app-project-officer
              [projectId]="project()?.id ?? ''"
              [projectName]="project()?.name ?? ''"
            />
          }

          @if (activeTab() === 'settings') {
            <div class="settings-section">
              <!-- Archived is read-only, not hidden: every value below stays
                   legible and stops being editable, and this names the one
                   click that gives them back. Letting the user type and then
                   eat a 409 is the failure this replaces. Only the Danger Zone
                   stays live, because unarchiving is the way out. -->
              @if (isArchived()) {
                <p class="archived-note" role="note">
                  {{ 'projectDetail.settings.archivedReadOnly' | transloco }}
                </p>
              }
              <!-- Any of the four groups below can be refused, so the refusal
                   belongs to the panel rather than to one of them. -->
              @if (editError(); as message) {
                <p class="edit-error" role="alert">{{ message }}</p>
              }

              <!-- General -->
              <div class="settings-group">
                <h3 class="settings-heading">{{ 'projectDetail.settings.general' | transloco }}</h3>
                <app-form-field [label]="'projectDetail.settings.projectName' | transloco">
                  <app-input
                    [value]="settingsName()"
                    [disabled]="proj.is_default || isArchived()"
                    (changed)="settingsName.set($event)"
                  />
                </app-form-field>
                <app-form-field [label]="'projectDetail.settings.defaultConfig' | transloco">
                  <app-input
                    [placeholder]="'projectDetail.settings.configPlaceholder' | transloco"
                    [value]="settingsConfigName()"
                    [disabled]="isArchived()"
                    (changed)="settingsConfigName.set($event)"
                  />
                </app-form-field>
                <div class="settings-actions">
                  <app-button
                    variant="primary"
                    size="sm"
                    [loading]="isSavingSettings()"
                    [disabled]="isSavingSettings() || isArchived()"
                    (clicked)="saveSettings()"
                  >
                    {{ 'projectDetail.settings.save' | transloco }}
                  </app-button>
                </div>
              </div>

              <!-- Memory -->
              <div class="settings-group">
                <h3 class="settings-heading">{{ 'projectDetail.settings.memory' | transloco }}</h3>
                <app-checkbox
                  [checked]="projectMemoryShared()"
                  [disabled]="isArchived()"
                  (changed)="toggleProjectMemory($event)"
                >
                  {{ 'projectDetail.settings.shareMemories' | transloco }}
                </app-checkbox>
                <p class="text-muted setting-desc">
                  {{ 'projectDetail.settings.shareMemoriesDesc' | transloco }}
                </p>
              </div>

              <!-- Workspace Network (admin-only) -->
              @if (isAdmin()) {
                <div class="settings-group">
                  <h3 class="settings-heading">{{ 'projectDetail.settings.workspaceNetwork' | transloco }}</h3>
                  <app-form-field [label]="'projectDetail.settings.networkTier' | transloco">
                    <app-select
                      [value]="settingsNetworkTier()"
                      [disabled]="isArchived()"
                      (changed)="onNetworkTierChange($event ?? '')"
                    >
                      <option value="internet-only">{{ 'projectDetail.settings.networkTierInternetOnly' | transloco }}</option>
                      <option value="home-allowed">{{ 'projectDetail.settings.networkTierHomeAllowed' | transloco }}</option>
                    </app-select>
                  </app-form-field>
                  <p class="text-muted setting-desc">
                    {{ 'projectDetail.settings.networkTierDesc' | transloco }}
                  </p>
                </div>
              }

              <!-- Workspace defaults: what Jobs and Sessions in this Project
                   get when they don't pick a workspace (spec §6). Read-only
                   while archived, manifest-owned, or the caller isn't an
                   owner/admin — canEditWorkspaceDefaults() covers all three,
                   the fields stay visible either way. -->
              @if (workspaceDefaults(); as wd) {
                <div class="settings-group">
                  <h3 class="settings-heading">{{ 'projectDetail.settings.workspaceDefaults' | transloco }}</h3>
                  <p class="text-muted setting-desc">
                    {{ 'projectDetail.settings.workspaceDefaultsHint' | transloco }}
                  </p>
                  @if (wd.managed_by_manifest) {
                    <p class="archived-note" role="note">
                      {{ 'projectDetail.settings.workspaceDefaultsManaged' | transloco }}
                    </p>
                  }
                  @for (problem of wd.installation_problems; track problem) {
                    <p class="edit-error" role="alert">{{ problem }}</p>
                  }
                  <app-form-field [label]="'projectDetail.settings.workspaceJobs' | transloco">
                    <app-select
                      [value]="wdJobs() ?? ''"
                      [disabled]="!canEditWorkspaceDefaults()"
                      (changed)="wdJobs.set($any($event) || null)"
                    >
                      <option value="">{{
                        'projectDetail.settings.installationDefault' | transloco: { value: modeLabel(wd.installation.jobs) }
                      }}</option>
                      @for (mode of workspaceModes(); track mode) {
                        <option [value]="mode">{{ modeLabel(mode) }}</option>
                      }
                    </app-select>
                  </app-form-field>
                  <app-form-field [label]="'projectDetail.settings.workspaceSessions' | transloco">
                    <app-select
                      [value]="wdSessions() ?? ''"
                      [disabled]="!canEditWorkspaceDefaults()"
                      (changed)="wdSessions.set($any($event) || null)"
                    >
                      <option value="">{{
                        'projectDetail.settings.installationDefault' | transloco: { value: modeLabel(wd.installation.sessions) }
                      }}</option>
                      @for (mode of workspaceModes(); track mode) {
                        <option [value]="mode">{{ modeLabel(mode) }}</option>
                      }
                    </app-select>
                  </app-form-field>
                  <app-form-field
                    [label]="'projectDetail.settings.workspaceContainerTemplate' | transloco"
                    [hint]="wd.template_problems.container ?? ''"
                  >
                    <app-select
                      [value]="wd.managed_by_manifest && wd.stored.container ? 'manifest' : (wdContainer() ?? '')"
                      [disabled]="!canEditWorkspaceDefaults()"
                      (changed)="wdContainer.set($event || null)"
                    >
                      <option value="">{{
                        'projectDetail.settings.installationDefault' | transloco: { value: wd.installation.container ?? modeLabel('container') }
                      }}</option>
                      @if (wd.managed_by_manifest && wd.stored.container) {
                        <option value="manifest">{{ 'projectDetail.settings.fromManifest' | transloco }}</option>
                      }
                      @for (t of containerTemplates(); track t.key) {
                        <option [value]="t.key">{{ t.label }}</option>
                      }
                    </app-select>
                  </app-form-field>
                  @if (wd.vm_available) {
                    <app-form-field
                      [label]="'projectDetail.settings.workspaceVmTemplate' | transloco"
                      [hint]="wd.template_problems.vm ?? ''"
                    >
                      <app-select
                        [value]="wd.managed_by_manifest && wd.stored.vm ? 'manifest' : (wdVm() ?? '')"
                        [disabled]="!canEditWorkspaceDefaults()"
                        (changed)="wdVm.set($event || null)"
                      >
                        <option value="">{{
                          'projectDetail.settings.installationDefault' | transloco: { value: wd.installation.vm ?? modeLabel('vm') }
                        }}</option>
                        @if (wd.managed_by_manifest && wd.stored.vm) {
                          <option value="manifest">{{ 'projectDetail.settings.fromManifest' | transloco }}</option>
                        }
                        @for (t of vmTemplates(); track t.key) {
                          <option [value]="t.key">{{ t.label }}</option>
                        }
                      </app-select>
                    </app-form-field>
                  }
                  @if (canEditWorkspaceDefaults()) {
                    <div class="settings-actions">
                      <app-button
                        variant="primary"
                        size="sm"
                        [loading]="isSavingWorkspaceDefaults()"
                        [disabled]="isSavingWorkspaceDefaults()"
                        (clicked)="saveWorkspaceDefaults()"
                      >
                        {{ 'projectDetail.settings.save' | transloco }}
                      </app-button>
                    </div>
                  }
                </div>
              }

              <!-- Cloud Storage -->
              <div class="settings-group">
                <h3 class="settings-heading">{{ 'projectDetail.settings.cloudStorage' | transloco }}</h3>
                @if (proj.cloud_storage_url) {
                  <div class="cloud-folder-row">
                    <label class="form-label-sm">{{ 'projectDetail.settings.folder' | transloco }}</label>
                    <a [href]="proj.cloud_storage_url" target="_blank" rel="noopener" class="cloud-folder-link">
                      {{ proj.name }}
                      <app-icon size="xs">open_in_new</app-icon>
                    </a>
                  </div>
                  <app-checkbox
                    [checked]="settingsCloudReadOnly()"
                    [disabled]="isArchived()"
                    (changed)="toggleCloudReadOnly($event)"
                  >
                    {{ 'projectDetail.settings.readOnlyAgent' | transloco }}
                  </app-checkbox>
                  <p class="text-muted setting-desc">
                    {{ 'projectDetail.settings.readOnlyAgentDesc' | transloco }}
                  </p>
                } @else {
                  <p class="text-muted setting-desc">
                    {{ 'projectDetail.settings.cloudUnavailable' | transloco }}
                  </p>
                }
              </div>

              <!-- Danger Zone -->
              <div class="settings-group danger-zone">
                <h3 class="settings-heading danger-heading">{{ 'projectDetail.settings.dangerZone' | transloco }}</h3>
                @if (proj.is_default) {
                  <p class="text-muted setting-desc">
                    {{ 'projectDetail.settings.defaultProjectWarning' | transloco }}
                  </p>
                } @else {
                  <div class="danger-actions">
                    @if (lifecycleError(); as message) {
                      <p class="lifecycle-error" role="alert">{{ message }}</p>
                    }
                    @if (archiveReport(); as report) {
                      <p class="lifecycle-note" role="status">{{ report }}</p>
                    }
                    @if (proj.status === 'archived') {
                      <!-- The way back. Reads, detaching and unarchiving all stay
                           possible on an archived project; only new work is
                           refused, or archiving would be a trap. -->
                      <div class="danger-row">
                        <div class="danger-info">
                          <span class="danger-title">{{ 'projectDetail.settings.unarchiveTitle' | transloco }}</span>
                          <span class="danger-desc">
                            {{ 'projectDetail.settings.unarchiveDesc' | transloco }}
                          </span>
                        </div>
                        <app-button
                          variant="ghost"
                          size="sm"
                          [disabled]="lifecycleBusy()"
                          (clicked)="confirmUnarchive()"
                        >
                          {{ 'projectDetail.settings.unarchive' | transloco }}
                        </app-button>
                      </div>
                    } @else {
                      <div class="danger-row">
                        <div class="danger-info">
                          <span class="danger-title">{{ 'projectDetail.settings.archiveTitle' | transloco }}</span>
                          <span class="danger-desc">
                            {{ 'projectDetail.settings.archiveDesc' | transloco }}
                          </span>
                        </div>
                        <app-button
                          variant="ghost"
                          size="sm"
                          [disabled]="lifecycleBusy()"
                          (clicked)="confirmArchive()"
                        >
                          {{ 'projectDetail.settings.archive' | transloco }}
                        </app-button>
                      </div>
                    }
                    <div class="danger-row">
                      <div class="danger-info">
                        <span class="danger-title">{{ 'projectDetail.settings.deleteTitle' | transloco }}</span>
                        <span class="danger-desc">
                          {{ 'projectDetail.settings.deleteDesc' | transloco }}
                        </span>
                      </div>
                      <app-button variant="danger" size="sm" (clicked)="deleteProject()">
                        {{ 'projectDetail.settings.delete' | transloco }}
                      </app-button>
                    </div>
                  </div>
                }
              </div>
            </div>
          }
        </div>
      }

      <!-- Lifecycle confirmation. A native confirm() cannot be themed, cannot
           be translated by the app's own catalogue, and is what the rest of the
           app has already moved off. -->
      <app-dialog
        [open]="pendingLifecycle() !== null"
        [title]="(pendingLifecycle() === 'archived'
          ? 'projectDetail.settings.archiveConfirmTitle'
          : 'projectDetail.settings.unarchiveConfirmTitle') | transloco"
        (closed)="pendingLifecycle.set(null)"
      >
        <p>
          {{ (pendingLifecycle() === 'archived'
            ? 'projectDetail.settings.archiveConfirm'
            : 'projectDetail.settings.unarchiveConfirm') | transloco }}
        </p>
        <div appDialogActions>
          <app-button variant="secondary" (clicked)="pendingLifecycle.set(null)">
            {{ 'common.cancel' | transloco }}
          </app-button>
          <app-button variant="primary" [disabled]="lifecycleBusy()" (clicked)="applyLifecycle()">
            {{ (pendingLifecycle() === 'archived'
              ? 'projectDetail.settings.archive'
              : 'projectDetail.settings.unarchive') | transloco }}
          </app-button>
        </div>
      </app-dialog>
    </div>
  `,
  styles: [`
    :host { display: block; height: 100%; overflow: auto; overflow-x: hidden; }

    .page-container {
      padding: 24px;
      max-width: var(--content-max-width-wide);
      margin: 0 auto;
      overflow-x: hidden;
    }

    .page-header {
      display: flex;
      align-items: center;
      gap: 12px;
      margin-bottom: 20px;
    }

    .header-info { display: flex; align-items: center; gap: 12px; flex-wrap: wrap; flex: 1 1 auto; min-width: 0; }

    .page-title {
      font-size: 22px;
      font-weight: 700;
      color: var(--text-primary);
      margin: 0;
      min-width: 0;
      max-width: 100%;
    }

    .header-badges { display: flex; gap: 6px; }

    /* The archived title: the same one-line, ellipsised shape the inline
       rename control renders, minus the affordance. */
    .page-title-static {
      display: inline-block;
      max-width: 100%;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
      vertical-align: bottom;
    }

    /* A refused write, wherever the user was when it was refused. */
    .edit-error {
      margin: 0 0 12px 0;
      padding: 8px 12px;
      border-radius: var(--radius-control);
      background: var(--danger-tint);
      color: var(--danger);
      font-size: 12px;
    }

    .archived-note {
      margin: 0;
      padding: 8px 12px;
      border-radius: var(--radius-control);
      background: var(--surface-0);
      color: var(--text-secondary);
      font-size: 12px;
      line-height: 1.4;
    }

    /* Tabs */
    .tab-bar {
      display: flex;
      gap: 2px;
      border-bottom: 1px solid var(--border-color);
      margin-bottom: 20px;
      overflow-x: auto;
      -webkit-overflow-scrolling: touch;
      scrollbar-width: none;
    }

    .tab-bar::-webkit-scrollbar { display: none; }

    .tab-btn {
      padding: 10px 20px;
      background: none;
      border: none;
      border-bottom: 2px solid transparent;
      color: var(--text-secondary);
      font-size: 13px;
      font-family: inherit;
      cursor: pointer;
      transition: all 0.15s ease;
      white-space: nowrap;
      flex-shrink: 0;
    }

    .tab-btn:hover { color: var(--text-primary); }

    .tab-btn.active {
      color: var(--accent-color);
      border-bottom-color: var(--accent-color);
    }

    /* Mirrors app-button[ghost][md] so it sits level with its button siblings. */
    .ghost-link {
      display: inline-flex;
      align-items: center;
      height: 32px;
      padding: 0 12px;
      border-radius: var(--radius-control);
      font-size: 13px;
      font-weight: 500;
      line-height: 1.25;
      color: var(--text-secondary);
      text-decoration: none;
      border: 1px solid transparent;
    }
    .ghost-link:hover { background: var(--surface-0); }

    .loading-state, .empty-state {
      display: flex;
      flex-direction: column;
      align-items: center;
      gap: 12px;
      padding: 60px 20px;
      color: var(--text-muted);
    }

    /* Overview */
    .overview-section { display: flex; flex-direction: column; gap: 16px; }

    .detail-grid {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 16px;
    }

    .detail-card {
      background: var(--panel-bg);
      border: 1px solid var(--border-color);
      border-radius: var(--radius-surface);
      padding: 14px;
    }

    .detail-card label {
      display: block;
      font-size: 11px;
      color: var(--text-muted);
      text-transform: uppercase;
      letter-spacing: 0.06em;
      margin-bottom: 6px;
    }

    .detail-value {
      margin: 0;
      font-size: 13px;
      color: var(--text-primary);
      line-height: 1.4;
    }

    .stats-row { display: flex; gap: 16px; }

    .stat-card {
      flex: 1;
      background: var(--panel-bg);
      border: 1px solid var(--border-color);
      border-radius: var(--radius-surface);
      padding: 16px;
      text-align: center;
    }

    .stat-value {
      display: block;
      font-size: 28px;
      font-weight: 700;
      color: var(--accent-color);
    }

    .stat-label {
      display: block;
      font-size: 11px;
      color: var(--text-muted);
      margin-top: 4px;
    }

    .overview-actions { display: flex; gap: 8px; align-items: center; flex-wrap: wrap; }

    .mono { font-family: 'JetBrains Mono', monospace; font-size: 12px; }

    /* Tab toolbar */
    .tab-toolbar {
      display: flex;
      justify-content: flex-end;
      margin-bottom: 4px;
    }

    /* Tables */
    .table-section { display: flex; flex-direction: column; gap: 12px; }
    .desc-cell { max-width: 250px; }
    .url-cell { max-width: 200px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }

    .empty-inline {
      padding: 20px;
      text-align: center;
      color: var(--text-muted);
      font-size: 13px;
      display: flex;
      align-items: center;
      justify-content: center;
      gap: 8px;
    }

    /* Status badges (kept custom — semantic palette per status) */
    .status-badge {
      display: inline-block;
      padding: 3px 8px;
      border-radius: var(--radius-tag);
      font-size: 11px;
      font-weight: 500;
      text-transform: capitalize;
    }

    .status-created,
    .status-pending { background: var(--accent-color); color: var(--app-bg); opacity: 0.85; }
    .status-processing { background: var(--warning-tint); color: var(--warning); }
    .status-completed { background: var(--success-tint); color: var(--success); }
    .status-failed { background: var(--danger-tint); color: var(--danger); }
    .status-cancelled { background: var(--surface-0); color: var(--text-muted); }
    .status-pending_review { background: var(--warning-tint); color: var(--warning); }
    .status-blocked_undelivered { background: var(--warning-tint); color: var(--warning); }

    /* Merge badges */
    .merge-badge {
      display: inline-block;
      padding: 3px 8px;
      border-radius: var(--radius-tag);
      font-size: 11px;
      font-weight: 500;
    }

    .merge-merged { background: var(--success-tint); color: var(--success); }
    .merge-conflict { background: var(--danger-tint); color: var(--danger); }
    .merge-skipped { background: var(--surface-0); color: var(--text-muted); }
    .merge-pending { background: var(--warning-tint); color: var(--warning); }
    .merge-empty { background: var(--warning-tint); color: var(--warning); }
    .merge-merge-failed { background: var(--danger-tint); color: var(--danger); }
    .merge-cloud-applied { background: var(--success-tint); color: var(--success); }
    .merge-no-changes,
    .merge-cloud-rejected { background: var(--surface-0); color: var(--text-muted); }
    .merge-cloud-conflict,
    .merge-cloud-partial,
    .merge-cloud-unavailable { background: var(--danger-tint); color: var(--danger); }

    /* Role badges */
    .role-badge {
      display: inline-block;
      padding: 3px 8px;
      border-radius: var(--radius-tag);
      font-size: 11px;
      font-weight: 500;
      background: var(--surface-0);
      color: var(--text-secondary);
    }

    .role-jobs { background: var(--accent-color); color: var(--app-bg); }
    .role-source { background: var(--success-tint); color: var(--success); }
    .role-reference { background: var(--warning-tint); color: var(--warning); }

    /* Inline form */
    .inline-form {
      display: flex;
      align-items: center;
      gap: 8px;
      padding: 12px;
      background: var(--panel-bg);
      border: 1px solid var(--border-color);
      border-radius: var(--radius-surface);
      flex-wrap: wrap;
    }

    .datasource-search { min-width: 180px; flex: 1 1 220px; }

    .datasource-candidate-state {
      display: flex;
      align-items: center;
      gap: 8px;
      color: var(--text-muted);
      font-size: 12px;
    }

    .datasource-candidate-state.error { color: var(--danger); }

    .datasource-candidate-actions { display: flex; justify-content: flex-start; }

    /* User cell */
    .user-cell {
      display: flex;
      align-items: center;
      gap: 8px;
    }

    .user-avatar-sm {
      width: 24px;
      height: 24px;
      border-radius: 50%;
      display: flex;
      align-items: center;
      justify-content: center;
      font-size: 10px;
      font-weight: 600;
      color: var(--app-bg);
      flex-shrink: 0;
    }

    .text-muted { color: var(--text-muted); }
    .grow-spacer { flex: 1; }

    /* Expert grid */
    .expert-grid {
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
      gap: 12px;
    }

    .expert-card {
      display: flex;
      flex-direction: column;
      gap: 6px;
      padding: 14px;
      border: 1px solid var(--border-color);
      border-radius: var(--radius-surface);
      background: var(--panel-bg);
    }

    .expert-icon { font-size: 28px; }

    .expert-name {
      font-size: 13px;
      font-weight: 600;
      color: var(--text-primary);
    }

    .expert-desc {
      font-size: 11px;
      color: var(--text-muted);
      line-height: 1.4;
      display: -webkit-box;
      -webkit-line-clamp: 2;
      -webkit-box-orient: vertical;
      overflow: hidden;
    }

    .expert-tags { display: flex; flex-wrap: wrap; gap: 4px; margin-top: 2px; }

    .expert-tag {
      font-size: 10px;
      padding: 1px 6px;
      border-radius: var(--radius-tag);
      background: var(--surface-0);
      color: var(--text-muted);
    }

    /* Settings */
    .settings-section { display: flex; flex-direction: column; gap: 20px; }

    .settings-group {
      background: var(--panel-bg);
      border: 1px solid var(--border-color);
      border-radius: var(--radius-surface);
      padding: 16px;
    }

    .settings-heading {
      font-size: 14px;
      font-weight: 600;
      color: var(--text-primary);
      margin: 0 0 14px 0;
    }

    .form-label-sm {
      display: block;
      font-size: 11px;
      color: var(--text-muted);
      margin-bottom: 4px;
    }

    .settings-actions { display: flex; gap: 8px; margin-top: 8px; }
    .setting-desc { font-size: 12px; margin-top: 4px; }

    .cloud-folder-row {
      display: flex;
      align-items: center;
      gap: 8px;
      margin-bottom: 12px;
    }
    .cloud-folder-link {
      color: var(--accent-color);
      text-decoration: none;
      display: inline-flex;
      align-items: center;
      gap: 4px;
    }
    .cloud-folder-link:hover { text-decoration: underline; }

    /* Danger Zone */
    .danger-zone { border-color: var(--danger); }
    .danger-heading { color: var(--danger); }

    .danger-actions { display: flex; flex-direction: column; gap: 12px; }

    .lifecycle-error {
      margin: 0;
      padding: 8px 12px;
      border-radius: var(--radius-control);
      background: var(--danger-tint);
      color: var(--danger);
      font-size: 12px;
    }

    .lifecycle-note {
      margin: 0;
      padding: 8px 12px;
      border-radius: var(--radius-control);
      background: var(--surface-0);
      color: var(--text-secondary);
      font-size: 12px;
      line-height: 1.4;
    }

    .danger-row {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      padding: 12px;
      border: 1px solid var(--border-color);
      border-radius: var(--radius-control);
    }

    .danger-info { flex: 1; min-width: 0; }

    .danger-title {
      display: block;
      font-size: 13px;
      font-weight: 500;
      color: var(--text-primary);
    }

    .danger-desc {
      display: block;
      font-size: 11px;
      color: var(--text-muted);
      margin-top: 2px;
    }

    /* Knowledge Base */
    .kb-section { display: flex; flex-direction: column; gap: 16px; }

    .kb-attach {
      display: flex; flex-direction: column; gap: 8px;
      background: var(--panel-bg);
      border: 1px solid var(--border-color);
      border-radius: var(--radius-surface);
      padding: 14px;
    }

    .kb-attach-title { font-size: 13px; font-weight: 600; color: var(--text-primary); }

    .kb-attach-hint {
      margin: 0;
      font-size: 12px;
      color: var(--text-muted);
      line-height: 1.45;
    }

    .kb-attach-row { display: flex; gap: 8px; align-items: center; flex-wrap: wrap; }

    .kb-attach-error {
      margin: 0;
      font-size: 12px;
      color: var(--danger);
    }

    .kb-stats-row {
      display: flex; gap: 12px; flex-wrap: wrap;
    }

    .kb-stat {
      flex: 1; min-width: 70px;
      background: var(--panel-bg);
      border: 1px solid var(--border-color);
      border-radius: var(--radius-surface);
      padding: 12px;
      text-align: center;
    }

    .kb-stat-value {
      display: block; font-size: 22px; font-weight: 700;
      color: var(--accent-color);
    }

    .kb-stat-label {
      display: block; font-size: 10px;
      color: var(--text-muted);
      text-transform: capitalize; margin-top: 2px;
    }

    .kb-toolbar {
      display: flex; gap: 8px; align-items: center; flex-wrap: wrap;
    }

    .kb-search-wrap { flex: 1; min-width: 180px; }

    .kb-note-list { display: flex; flex-direction: column; gap: 8px; }

    .kb-note-card {
      background: var(--panel-bg);
      border: 1px solid var(--border-color);
      border-radius: var(--radius-surface);
      padding: 14px;
      cursor: pointer;
      transition: border-color 0.15s ease;
    }

    .kb-note-card:hover { border-color: var(--accent-color); }

    .kb-note-header {
      display: flex; align-items: center; gap: 8px; margin-bottom: 6px;
    }

    .kb-note-title {
      font-size: 14px; font-weight: 600;
      color: var(--text-primary);
      flex: 1; min-width: 0;
      overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
    }

    .kb-note-status {
      font-size: 10px; padding: 2px 6px; border-radius: var(--radius-tag);
      font-weight: 500; text-transform: capitalize;
    }

    .kb-note-status[data-status="active"] { background: var(--success-tint); color: var(--success); }
    .kb-note-status[data-status="resolved"] { background: var(--accent-color); color: var(--app-bg); }
    .kb-note-status[data-status="superseded"] { background: var(--warning-tint); color: var(--warning); }
    .kb-note-status[data-status="archived"] { background: var(--surface-0); color: var(--text-muted); }

    .kb-type-badge {
      font-size: 10px; padding: 2px 8px; border-radius: var(--radius-tag);
      font-weight: 600; text-transform: capitalize; white-space: nowrap;
      background: var(--surface-0); color: var(--text-secondary);
    }

    .kb-type-badge[data-type="decision"] { background: color-mix(in srgb, var(--accent-color) 20%, transparent); color: var(--accent-color); }
    .kb-type-badge[data-type="learning"] { background: var(--success-tint); color: var(--success); }
    .kb-type-badge[data-type="goal"] { background: var(--warning-tint); color: var(--warning); }

    .kb-note-preview {
      font-size: 12px; color: var(--text-secondary);
      line-height: 1.5; margin-bottom: 8px;
      display: -webkit-box; -webkit-line-clamp: 2;
      -webkit-box-orient: vertical; overflow: hidden;
    }

    .kb-note-footer {
      display: flex; align-items: center; gap: 6px; flex-wrap: wrap;
    }
    .kb-note-footer .footer-date { margin-left: auto; }

    .kb-tag-sm {
      font-size: 10px; padding: 1px 5px; border-radius: var(--radius-tag);
      background: var(--surface-0); color: var(--text-muted);
    }

    .kb-confidence-sm {
      font-size: 10px; padding: 1px 5px; border-radius: var(--radius-tag);
      background: var(--surface-0); color: var(--accent-color);
      text-transform: capitalize;
    }

    .kb-pagination {
      display: flex; align-items: center; justify-content: center; gap: 12px;
      padding: 12px;
    }

    /* Detail view */
    .kb-detail {
      background: var(--panel-bg);
      border: 1px solid var(--border-color);
      border-radius: var(--radius-surface);
      padding: 20px;
    }

    .kb-detail-header {
      display: flex; align-items: center; gap: 8px; margin-bottom: 12px;
    }

    .kb-detail-meta {
      display: flex; align-items: center; gap: 8px; margin-bottom: 8px;
      font-size: 12px;
    }

    .kb-detail-title {
      font-size: 20px; font-weight: 700; margin: 0 0 12px 0;
      color: var(--text-primary);
    }

    .kb-tags { display: flex; gap: 6px; flex-wrap: wrap; margin-bottom: 14px; }

    .kb-tag {
      font-size: 11px; padding: 3px 8px; border-radius: var(--radius-tag);
      background: var(--surface-0); color: var(--accent-color);
    }

    .kb-confidence {
      font-size: 12px; padding: 2px 6px; border-radius: var(--radius-tag);
      background: var(--surface-0); color: var(--accent-color);
      text-transform: capitalize;
    }

    .kb-detail-content {
      font-size: 13px; line-height: 1.7;
      color: var(--text-primary);
      word-wrap: break-word;
    }

    /* Rendered markdown — mirrors the chat renderer's cascade so KB notes
       read the same as agent messages. */
    .kb-detail-content ::ng-deep > :first-child { margin-top: 0; }
    .kb-detail-content ::ng-deep > :last-child { margin-bottom: 0; }

    .kb-detail-content ::ng-deep p { margin: 8px 0; }

    .kb-detail-content ::ng-deep h1,
    .kb-detail-content ::ng-deep h2,
    .kb-detail-content ::ng-deep h3,
    .kb-detail-content ::ng-deep h4,
    .kb-detail-content ::ng-deep h5,
    .kb-detail-content ::ng-deep h6 {
      color: var(--text-primary);
      font-weight: 700; line-height: 1.3;
      margin: 16px 0 8px 0;
    }

    .kb-detail-content ::ng-deep h1 { font-size: 18px; }
    .kb-detail-content ::ng-deep h2 { font-size: 16px; }
    .kb-detail-content ::ng-deep h3 { font-size: 14px; }
    .kb-detail-content ::ng-deep h4,
    .kb-detail-content ::ng-deep h5,
    .kb-detail-content ::ng-deep h6 {
      font-size: 13px;
      color: var(--text-secondary);
    }

    .kb-detail-content ::ng-deep ul,
    .kb-detail-content ::ng-deep ol {
      margin: 6px 0; padding-left: 20px;
    }
    .kb-detail-content ::ng-deep ul { list-style-type: disc; }
    .kb-detail-content ::ng-deep ol { list-style-type: decimal; }
    .kb-detail-content ::ng-deep li { margin: 3px 0; }
    .kb-detail-content ::ng-deep li > ul,
    .kb-detail-content ::ng-deep li > ol { margin: 2px 0; }

    .kb-detail-content ::ng-deep code {
      background: color-mix(in srgb, var(--accent-color) 20%, transparent);
      padding: 1px 5px; border-radius: var(--radius-tag);
      font-family: 'JetBrains Mono', monospace; font-size: 0.9em;
    }

    .kb-detail-content ::ng-deep pre {
      background: var(--surface-0);
      border: 1px solid var(--border-color);
      border-radius: var(--radius-surface);
      padding: 12px 16px; margin: 8px 0;
      overflow-x: auto;
      font-family: 'JetBrains Mono', monospace; font-size: 12px; line-height: 1.5;
    }
    .kb-detail-content ::ng-deep pre code { background: transparent; padding: 0; }

    .kb-detail-content ::ng-deep blockquote {
      border-left: 3px solid var(--accent-color);
      margin: 8px 0; padding: 4px 12px;
      color: var(--text-secondary);
    }

    .kb-detail-content ::ng-deep a { color: var(--info); text-decoration: none; }
    .kb-detail-content ::ng-deep a:hover { text-decoration: underline; }

    .kb-detail-content ::ng-deep table {
      border-collapse: collapse;
      display: block; width: max-content; max-width: 100%;
      overflow-x: auto; margin: 8px 0; font-size: 12px;
    }
    .kb-detail-content ::ng-deep th,
    .kb-detail-content ::ng-deep td {
      padding: 6px 12px;
      border-bottom: 1px solid var(--border-color);
      text-align: left;
    }
    .kb-detail-content ::ng-deep th {
      font-weight: 600; color: var(--accent-color);
      font-size: 11px; text-transform: uppercase; letter-spacing: 0.06em;
    }

    .kb-detail-content ::ng-deep hr {
      border: none; border-top: 1px solid var(--border-color);
      margin: 16px 0;
    }

    .kb-detail-content ::ng-deep img { max-width: 100%; height: auto; }

    .kb-relationships {
      margin-top: 16px; padding-top: 16px;
      border-top: 1px solid var(--border-color);
    }

    .kb-relationships h4 {
      font-size: 12px; text-transform: uppercase; letter-spacing: 0.06em;
      color: var(--text-muted); margin: 0 0 8px 0;
    }

    .kb-rel-item {
      display: flex; align-items: center; gap: 8px;
      font-size: 12px; margin-bottom: 4px;
    }

    .kb-rel-type {
      font-size: 10px; padding: 2px 6px; border-radius: var(--radius-tag);
      background: var(--surface-0); color: var(--text-secondary);
      font-weight: 500;
    }

    .kb-rel-link {
      background: none; border: none; padding: 0;
      color: var(--accent-color);
      cursor: pointer; font-size: 12px; font-family: inherit;
      text-decoration: underline;
    }

    .kb-rel-link:hover { opacity: 0.8; }

    .table-scroll {
      overflow-x: auto;
      -webkit-overflow-scrolling: touch;
    }

    @media (max-width: 768px) {
      .page-container { padding: 12px; }

      .page-header { flex-wrap: wrap; gap: 8px; }
      .page-title { font-size: 18px; }
      .header-info { gap: 8px; }

      .tab-bar {
        position: relative;
        mask-image: linear-gradient(to right, black calc(100% - 24px), transparent);
        -webkit-mask-image: linear-gradient(to right, black calc(100% - 24px), transparent);
      }

      .tab-btn { padding: 10px 14px; font-size: 12px; }

      .detail-grid { grid-template-columns: 1fr; }

      .stats-row {
        display: grid;
        grid-template-columns: 1fr 1fr;
        gap: 8px;
      }
      .stat-card { padding: 12px; }
      .stat-value { font-size: 22px; }

      .data-table { min-width: 500px; }

      .inline-form { flex-direction: column; align-items: stretch; }
      .kb-stats-row { display: grid; grid-template-columns: repeat(3, 1fr); gap: 8px; }
      .kb-toolbar { flex-direction: column; align-items: stretch; gap: 8px; }
    }
  `],
})
export class ProjectDetailPageComponent implements OnInit, OnDestroy {
  private readonly route = inject(ActivatedRoute);
  private readonly router = inject(Router);
  private readonly api = inject(ApiService);
  private readonly capabilities = inject(CapabilitiesService);
  private readonly userService = inject(UserService);
  private readonly transloco = inject(TranslocoService);
  private readonly errors = inject(ErrorMessageService);
  private readonly destroyRef = inject(DestroyRef);
  private readonly connectorDrivers = inject(ConnectorDriversService);
  protected readonly viewport = inject(ViewportService);
  readonly effectiveJobStatus = effectiveJobStatus;

  readonly project = signal<Project | null>(null);
  /** Which lifecycle change the confirmation dialog is holding, if any. */
  readonly pendingLifecycle = signal<ProjectStatus | null>(null);
  readonly lifecycleBusy = signal(false);
  /** The server's own refusal — for an archived project that is a 409 naming
   *  the fix, which is exactly the sentence worth showing. */
  readonly lifecycleError = signal<string | null>(null);
  /** What archiving quiesced on the way through. */
  readonly archiveReport = signal<string | null>(null);
  /**
   * Archived means read-only apart from `status`: the server accepts a
   * status-only PATCH and refuses any other field whole with a 409. The UI
   * prevents rather than reports — the fields stay visible and stop being
   * editable — and the error signals below cover the case prevention cannot,
   * which is another tab archiving the project while this form is open.
   */
  readonly isArchived = computed(() => this.project()?.status === 'archived');
  /** A refused rename. Lives beside the header, where the rename is. */
  readonly renameError = signal<string | null>(null);
  /** A refused overview or settings save. The two panels are different tabs,
   *  so one signal never renders twice at once. */
  readonly editError = signal<string | null>(null);
  readonly jobs = signal<Job[]>([]);
  readonly repos = signal<ProjectRepository[]>([]);
  /** A project has at most one writable vault, and it is never replaced in
   *  place, so this is what decides whether attaching is offered at all. */
  readonly hasKnowledgeRepo = computed(() =>
    this.repos().some((repo) => repo.role === 'knowledge'),
  );
  /** Knowledge base connectors this project could adopt as its vault. */
  readonly kbConnectors = signal<Datasource[]>([]);
  readonly kbAttachSelection = signal('');
  readonly isAttachingKb = signal(false);
  readonly kbAttachError = signal('');
  readonly members = signal<ProjectMember[]>([]);
  readonly isLoading = signal(false);
  readonly activeTab = signal<Tab>('overview');

  // Overview editing
  readonly isEditingOverview = signal(false);
  readonly editDescription = signal('');
  readonly editGoal = signal('');

  // Repos tab
  readonly repoName = signal('');
  readonly repoUrl = signal('');
  readonly repoRole = signal<Extract<ProjectRepoRole, 'source' | 'reference'>>('source');
  readonly repoReadOnly = signal(false);
  readonly repoCreateManaged = signal(false);

  // Members tab
  readonly memberUserId = signal('');
  readonly memberRole = signal<ProjectMemberRole>('editor');
  readonly allUsers = signal<User[]>([]);

  readonly ownerCount = computed(() =>
    this.members().filter((m) => m.role === 'owner').length,
  );

  readonly availableUsers = computed(() => {
    const memberIds = new Set(this.members().map((m) => m.user_id));
    return this.allUsers().filter((u) => !memberIds.has(u.id));
  });

  // Datasources tab
  readonly projectDatasources = signal<ProjectDatasource[]>([]);
  readonly allDatasources = signal<Datasource[]>([]);
  readonly dsLinkId = signal('');
  readonly datasourceCandidateSearch = signal('');
  readonly datasourceCandidatesLoading = signal(false);
  readonly datasourceCandidatesLoadingMore = signal(false);
  readonly datasourceCandidatesError = signal(false);
  readonly datasourceCandidatesNextCursor = signal<string | null>(null);
  private datasourceCandidatesRequestSerial = 0;
  private datasourceCandidateSearchTimer: ReturnType<typeof setTimeout> | null = null;

  readonly availableDatasources = computed(() => {
    const linked = new Set(this.projectDatasources().map((d) => d.id));
    const query = this.datasourceCandidateSearch().trim().toLocaleLowerCase();
    return this.allDatasources().filter((d) => {
      if (linked.has(d.id)) return false;
      if (!this.canLinkDatasourceCandidate(d)) return false;
      if (!query) return true;
      return [d.name, d.description ?? '', d.type]
        .some(value => value.toLocaleLowerCase().includes(query));
    });
  });

  // Experts tab
  readonly projectExperts = signal<Expert[]>([]);
  readonly isLoadingExperts = signal(false);

  // Settings tab
  readonly settingsName = signal('');
  readonly settingsConfigName = signal('');
  readonly settingsCloudReadOnly = signal(false);
  readonly settingsNetworkTier = signal<'internet-only' | 'home-allowed'>('internet-only');
  readonly isSavingSettings = signal(false);
  /** Admin-only fields (e.g. workspace network tier) are hidden unless this is true. */
  readonly isAdmin = computed(() => !!this.userService.currentUser()?.is_admin);
  /** Worker mode base from GET /api/experts/worker_base — toggle fallback. */
  private readonly frameworkDefaults = signal<Record<string, unknown> | null>(null);
  readonly projectMemoryShared = computed(() => {
    const p = this.project();
    const val = (p?.default_config_override as any)?.memory?.project_scoped;
    if (typeof val === 'boolean') return val;
    // Fall back to the conservative worker mode base.
    const defaults = this.frameworkDefaults();
    const defaultVal = (defaults?.['memory'] as any)?.['project_scoped'];
    return typeof defaultVal === 'boolean' ? defaultVal : true;
  });

  // Settings tab: workspace defaults
  readonly workspaceDefaults = signal<ProjectWorkspaceDefaults | null>(null);
  readonly wdJobs = signal<WorkspaceMode | null>(null);
  readonly wdSessions = signal<WorkspaceMode | null>(null);
  readonly wdContainer = signal<string | null>(null);
  readonly wdVm = signal<string | null>(null);
  readonly isSavingWorkspaceDefaults = signal(false);
  readonly workspaceTemplates = signal<{key: string; label: string; backend: string}[]>([]);
  readonly containerTemplates = computed(() => this.workspaceTemplates().filter((t) => t.backend === 'sandbox'));
  readonly vmTemplates = computed(() => this.workspaceTemplates().filter((t) => t.backend === 'vm'));
  /** Manifest-owned rows, archived Projects and non-owner/non-admin members
   *  all render the same read-only fields (spec §6, Project page Settings
   *  tab) — one gate for the template and the save button alike. The server
   *  decides `can_edit` (role, manifest ownership); `isArchived()` is kept
   *  here too for instant feedback the moment this page archives the
   *  Project, before a fresh defaults GET lands. */
  readonly canEditWorkspaceDefaults = computed(
    () => !this.isArchived() && this.workspaceDefaults()?.can_edit === true,
  );
  readonly workspaceModes = computed<WorkspaceMode[]>(() =>
    this.workspaceDefaults()?.vm_available === false
      ? ['none', 'virtual', 'container']
      : ['none', 'virtual', 'container', 'vm'],
  );

  // Knowledge tab
  readonly kbSummary = signal<KnowledgeSummary | null>(null);
  readonly kbNotes = signal<KnowledgeNote[]>([]);
  readonly kbTotal = signal(0);
  readonly kbSelectedNote = signal<KnowledgeNoteDetail | null>(null);
  readonly kbIsLoading = signal(false);
  readonly kbSearchQuery = signal('');
  readonly kbFilterType = signal('');
  readonly kbFilterStatus = signal('');

  /** True when the caller may run project loops and commission an officer.
   * Fails closed while capabilities load, so the two tabs appear only once
   * entitlement is proven. */
  readonly canRunUnattendedOperations = this.capabilities.canRunUnattendedOperations;

  /** Tabs that are always available, in display order. */
  private readonly baseTabs: { id: Tab; labelKey: string }[] = [
    { id: 'overview', labelKey: 'projectDetail.tabs.overview' },
    { id: 'jobs', labelKey: 'projectDetail.tabs.jobs' },
    { id: 'knowledge', labelKey: 'projectDetail.tabs.knowledge' },
    { id: 'datasources', labelKey: 'projectDetail.tabs.datasources' },
    { id: 'repos', labelKey: 'projectDetail.tabs.repos' },
    { id: 'experts', labelKey: 'projectDetail.tabs.experts' },
    { id: 'members', labelKey: 'projectDetail.tabs.members' },
    { id: 'loop', labelKey: 'projectDetail.tabs.loop' },
    { id: 'centurion', labelKey: 'projectDetail.tabs.centurion' },
    { id: 'settings', labelKey: 'projectDetail.tabs.settings' },
  ];

  /** `baseTabs` minus the unattended-operations surfaces when the caller lacks
   * the grant. Hiding rather than disabling: there is nothing useful to show a
   * user who cannot start either one, and the orchestrator refuses the writes
   * either way. */
  readonly tabList = computed(() =>
    this.canRunUnattendedOperations()
      ? this.baseTabs
      : this.baseTabs.filter((t) => t.id !== 'loop' && t.id !== 'centurion'),
  );

  private projectId = '';
  private refreshInterval: ReturnType<typeof setInterval> | null = null;

  ngOnInit(): void {
    // Start with the legacy list while capabilities are unresolved. If the v1
    // contract becomes available (or is withdrawn after a reload), replace
    // the candidate set from the matching source without mixing responses.
    let lastPolicyAvailability = this.capabilities.datasourceScopeAutoAttachAvailable();
    this.capabilities.datasourceScopeAutoAttachAvailability$
      .pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe((available) => {
        if (available === lastPolicyAvailability) return;
        lastPolicyAvailability = available;
        if (this.datasourceCandidateSearchTimer) {
          clearTimeout(this.datasourceCandidateSearchTimer);
          this.datasourceCandidateSearchTimer = null;
        }
        if (this.projectId) this.loadDatasourceCandidates(true);
      });
    this.route.paramMap.subscribe((params) => {
      this.projectId = params.get('id') ?? '';
      if (this.projectId) this.loadAll();
    });
    this.api.getUsers().subscribe((users) => this.allUsers.set(users));
    // Load the worker base so project job toggles reflect the actual fallback.
    this.api.getExpertDetail('worker_base').subscribe((d) => {
      if (d?.config) this.frameworkDefaults.set(d.config);
    });

    this.refreshInterval = setInterval(() => {
      if (this.activeTab() === 'jobs' && this.projectId) {
        this.loadJobs();
      }
    }, 30000);
  }

  ngOnDestroy(): void {
    if (this.refreshInterval) clearInterval(this.refreshInterval);
    if (this.datasourceCandidateSearchTimer) {
      clearTimeout(this.datasourceCandidateSearchTimer);
    }
  }

  goBack(): void {
    this.router.navigate(['/projects']);
  }

  loadAll(): void {
    this.isLoading.set(true);
    this.api.getProject(this.projectId).subscribe((p) => {
      this.project.set(p);
      this.isLoading.set(false);
      // Initialize settings signals from project
      if (p) {
        this.settingsName.set(p.name);
        this.settingsConfigName.set(p.default_config_name ?? '');
        this.settingsCloudReadOnly.set(p.cloud_storage_read_only ?? false);
        this.settingsNetworkTier.set(p.network_tier ?? 'internet-only');
      }
      this.loadWorkspaceDefaults();
    });
    this.loadJobs();
    this.loadRepos();
    this.loadMembers();
    this.loadExperts();
    this.loadProjectDatasources();
    this.loadKBSummary();
  }

  loadJobs(): void {
    this.api.getProjectJobs(this.projectId).subscribe((j) => this.jobs.set(j));
  }

  loadRepos(): void {
    this.api.getProjectRepositories(this.projectId).subscribe((r) => {
      this.repos.set(r);
      // Only a vault-less project can attach one, so the candidate list is
      // fetched only for those.
      if (!this.hasKnowledgeRepo()) this.loadKbConnectors();
    });
  }

  private loadKbConnectors(): void {
    this.api.getDatasources(undefined, 'kb').subscribe({
      next: (connectors) => {
        // The server-owned marker means some project already adopted it;
        // offering it here could only ever produce a 409.
        this.kbConnectors.set(
          connectors.filter((connector) => !connector.config?.native_project_id),
        );
      },
      error: () => this.kbConnectors.set([]),
    });
  }

  openConnectors(): void {
    this.router.navigate(['/datasources']);
  }

  /** Hand the selected connector to this project as its writable vault. The
   *  connector already carries the repository, branch and PAT; the request
   *  names nothing else. */
  attachKnowledgeConnector(): void {
    const datasourceId = this.kbAttachSelection();
    if (!datasourceId || this.isAttachingKb()) return;
    this.isAttachingKb.set(true);
    this.kbAttachError.set('');
    this.api.attachProjectKnowledgeRepository(this.projectId, {datasource_id: datasourceId}).subscribe({
      next: (result) => {
        this.isAttachingKb.set(false);
        if (!result) {
          this.kbAttachError.set(this.transloco.translate('projectDetail.knowledge.attachFailed'));
          return;
        }
        this.kbAttachSelection.set('');
        this.loadRepos();
        this.loadKBSummary();
      },
      error: (err: unknown) => {
        this.isAttachingKb.set(false);
        const detail = (err as {error?: {detail?: unknown}} | null)?.error?.detail;
        this.kbAttachError.set(
          typeof detail === 'string' && detail
            ? detail
            : this.transloco.translate('projectDetail.knowledge.attachFailed'),
        );
      },
    });
  }

  loadMembers(): void {
    this.api.getProjectMembers(this.projectId).subscribe((m) => this.members.set(m));
  }

  loadExperts(): void {
    this.isLoadingExperts.set(true);
    this.api.getProjectExperts(this.projectId).subscribe({
      next: (e) => {
        this.projectExperts.set(e);
        this.isLoadingExperts.set(false);
      },
      error: () => this.isLoadingExperts.set(false),
    });
  }

  // Datasources
  loadProjectDatasources(): void {
    this.connectorDrivers.load();
    this.api.getProjectDatasources(this.projectId).subscribe((ds) => {
      this.projectDatasources.set(ds);
      this.reconcileDatasourceSelection();
    });
    this.loadDatasourceCandidates();
  }

  /** Load connector candidates from the rollout-appropriate contract. V1 is
   * target-aware and paginated; older orchestrators keep the legacy visible
   * connector list and client-side search. */
  loadDatasourceCandidates(reset = true): void {
    if (!this.projectId) return;
    const policyAvailable = this.capabilities.datasourceScopeAutoAttachAvailable();
    if (!reset && !policyAvailable) return;
    const cursor = reset ? null : this.datasourceCandidatesNextCursor();
    if (!reset && !cursor) return;

    const serial = ++this.datasourceCandidatesRequestSerial;
    if (reset) {
      this.datasourceCandidatesLoading.set(true);
      this.datasourceCandidatesLoadingMore.set(false);
      this.datasourceCandidatesNextCursor.set(null);
    } else {
      this.datasourceCandidatesLoadingMore.set(true);
    }
    this.datasourceCandidatesError.set(false);

    if (!policyAvailable) {
      this.api.getDatasources().subscribe({
        next: (datasources) => {
          if (serial !== this.datasourceCandidatesRequestSerial) return;
          this.allDatasources.set(datasources);
          this.datasourceCandidatesLoading.set(false);
          this.datasourceCandidatesLoadingMore.set(false);
          this.datasourceCandidatesNextCursor.set(null);
          this.reconcileDatasourceSelection();
        },
        error: () => {
          if (serial !== this.datasourceCandidatesRequestSerial) return;
          this.allDatasources.set([]);
          this.dsLinkId.set('');
          this.datasourceCandidatesLoading.set(false);
          this.datasourceCandidatesLoadingMore.set(false);
          this.datasourceCandidatesNextCursor.set(null);
        },
      });
      return;
    }

    this.api.getLinkableProjectDatasources(this.projectId, {
      q: this.datasourceCandidateSearch().trim() || undefined,
      cursor: cursor ?? undefined,
      limit: 50,
    }).subscribe({
      next: (response) => {
        if (serial !== this.datasourceCandidatesRequestSerial) return;
        const previous = reset ? [] : this.allDatasources();
        this.allDatasources.set([
          ...previous,
          ...response.items.filter(
            incoming => !previous.some(existing => existing.id === incoming.id),
          ),
        ]);
        this.datasourceCandidatesNextCursor.set(response.next_cursor);
        this.datasourceCandidatesLoading.set(false);
        this.datasourceCandidatesLoadingMore.set(false);
        this.reconcileDatasourceSelection();
      },
      error: () => {
        if (serial !== this.datasourceCandidatesRequestSerial) return;
        // Fail closed: never leave stale candidate rows selectable after an
        // authorization-aware v1 request fails.
        this.allDatasources.set([]);
        this.dsLinkId.set('');
        this.datasourceCandidatesNextCursor.set(null);
        this.datasourceCandidatesLoading.set(false);
        this.datasourceCandidatesLoadingMore.set(false);
        this.datasourceCandidatesError.set(true);
      },
    });
  }

  loadMoreDatasourceCandidates(): void {
    this.loadDatasourceCandidates(false);
  }

  onDatasourceCandidateSearch(value: string): void {
    this.datasourceCandidateSearch.set(value);
    this.reconcileDatasourceSelection();
    if (!this.capabilities.datasourceScopeAutoAttachAvailable()) return;

    if (this.datasourceCandidateSearchTimer) {
      clearTimeout(this.datasourceCandidateSearchTimer);
    }
    // Invalidate any older page immediately; the delayed request owns the
    // loading flags and only its serial may publish results.
    this.datasourceCandidatesRequestSerial += 1;
    this.datasourceCandidatesLoading.set(true);
    this.datasourceCandidatesLoadingMore.set(false);
    this.datasourceCandidatesError.set(false);
    this.datasourceCandidatesNextCursor.set(null);
    this.datasourceCandidateSearchTimer = setTimeout(() => {
      this.datasourceCandidateSearchTimer = null;
      this.loadDatasourceCandidates(true);
    }, 250);
  }

  private reconcileDatasourceSelection(): void {
    const selected = this.dsLinkId();
    if (selected && !this.availableDatasources().some(ds => ds.id === selected)) {
      this.dsLinkId.set('');
    }
  }

  /** Defence in depth for stale/mixed-version responses. The v1 endpoint is
   * authoritative, while the legacy list was only visibility-filtered. These
   * mirror the connector-side link rules before a row becomes selectable. */
  private canLinkDatasourceCandidate(datasource: Datasource): boolean {
    if (datasource.config?.native_project_id) return false;
    const user = this.userService.currentUser();
    if (!user) return false;
    if (user.is_admin || datasource.created_by === user.id) return true;
    return datasource.is_global === true && (datasource.scope_mode ?? 'all') === 'all';
  }

  linkDatasource(): void {
    const dsId = this.dsLinkId();
    if (!dsId) return;
    const datasource = this.availableDatasources().find((ds) => ds.id === dsId);
    if (!datasource) {
      this.dsLinkId.set('');
      return;
    }
    const settings = this.linkAccess(datasource) === 'read_only' ? {read_only: true} : {};
    this.api.linkProjectDatasource(this.projectId, dsId, settings).subscribe((res) => {
      if (res) {
        this.dsLinkId.set('');
        this.loadProjectDatasources();
      }
    });
  }

  unlinkDatasource(datasourceId: string): void {
    this.api.unlinkProjectDatasource(this.projectId, datasourceId).subscribe((res) => {
      if (res) {
        this.loadProjectDatasources();
      }
    });
  }

  /** A link's access choice: one fixed level, or the read-only switch.
   *  Before the capability matrix loads, the KB rule the page always had. */
  linkAccess(ds: Pick<Datasource, 'type'>): 'read_only' | 'read_write' | 'choice' {
    const access = offeredAccess(this.connectorDrivers.forType(ds.type));
    if (!access) return ds.type === 'kb' ? 'read_only' : 'choice';
    if (access.readOnly && access.readWrite) return 'choice';
    if (access.readWrite) return 'read_write';
    return access.readOnly ? 'read_only' : 'choice';
  }

  /** What enforces a fixed level, from the driver's spec. */
  linkAccessHint(ds: Pick<Datasource, 'type'>): string {
    const access = offeredAccess(this.connectorDrivers.forType(ds.type));
    return (access?.readOnly ?? access?.readWrite)?.enforced_by ?? '';
  }

  updateDatasourceReadOnly(datasourceId: string, value: string): void {
    if (
      this.projectDatasources().some(
        (ds) => ds.id === datasourceId && this.linkAccess(ds) !== 'choice',
      )
    ) {
      return;
    }
    const readOnly = value === '' ? null : value === 'true';
    this.api.updateProjectDatasource(this.projectId, datasourceId, { read_only: readOnly }).subscribe((res) => {
      if (res) this.loadProjectDatasources();
    });
  }

  updateDatasourceDescription(datasourceId: string, value: string): void {
    const desc = value.trim() || null;
    this.api.updateProjectDatasource(this.projectId, datasourceId, { description: desc }).subscribe((res) => {
      if (res) this.loadProjectDatasources();
    });
  }

  // Overview
  startEditOverview(): void {
    if (this.isArchived()) return;
    const p = this.project();
    this.editDescription.set(p?.description ?? '');
    this.editGoal.set(p?.goal ?? '');
    this.isEditingOverview.set(true);
  }

  cancelEditOverview(): void {
    this.isEditingOverview.set(false);
    this.editError.set(null);
  }

  saveOverview(): void {
    if (this.isArchived()) return;
    this.editError.set(null);
    this.api.updateProjectFields(this.projectId, {
      description: this.editDescription(),
      goal: this.editGoal(),
    }).subscribe({
      next: () => {
        this.isEditingOverview.set(false);
        this.api.getProject(this.projectId).subscribe((p) => this.project.set(p));
      },
      error: (err: unknown) => this.reportEditFailure(err),
    });
  }

  // Repos
  addRepo(): void {
    const name = this.repoName().trim();
    if (!name) return;

    this.api.addProjectRepository(this.projectId, {
      name,
      repo_url: this.repoUrl().trim() || undefined,
      role: this.repoRole(),
      read_only: this.repoReadOnly(),
      create_managed: this.repoCreateManaged(),
    }).subscribe((res) => {
      if (res) {
        this.repoName.set('');
        this.repoUrl.set('');
        this.loadRepos();
      }
    });
  }

  removeRepo(repoId: string): void {
    this.api.removeProjectRepository(this.projectId, repoId).subscribe(() => {
      this.loadRepos();
    });
  }

  onRepoRoleChange(value: string | null): void {
    if (value === 'source' || value === 'reference') this.repoRole.set(value);
  }

  // Members
  addMember(): void {
    const userId = this.memberUserId();
    if (!userId) return;

    this.api.addProjectMember(this.projectId, {
      user_id: userId,
      role: this.memberRole(),
    }).subscribe((res) => {
      if (res) {
        this.memberUserId.set('');
        this.loadMembers();
      }
    });
  }

  onMemberRoleSelectChange(value: string | null): void {
    if (value) this.memberRole.set(value as ProjectMemberRole);
  }

  onRowMemberRoleChange(userId: string, role: string | null): void {
    if (role) this.changeMemberRole(userId, role as ProjectMemberRole);
  }

  changeMemberRole(userId: string, role: ProjectMemberRole): void {
    this.api.updateProjectMember(this.projectId, userId, { role }).subscribe(() => {
      this.loadMembers();
    });
  }

  removeMember(userId: string): void {
    this.api.removeProjectMember(this.projectId, userId).subscribe(() => {
      this.loadMembers();
    });
  }

  // Settings
  //
  // Every write below carries a field other than `status`, which is exactly
  // what an archived project refuses (409, whole request). Each one is gated on
  // `isArchived()` so it is never attempted, and each one goes through
  // `updateProjectFields` so that the race prevention cannot cover — another
  // tab archiving between load and save — is reported instead of swallowed.
  saveSettings(): void {
    if (this.isArchived()) return;
    const update: Record<string, string> = {};
    const p = this.project();
    const name = this.settingsName().trim();
    const config = this.settingsConfigName().trim();
    if (name && name !== p?.name) update['name'] = name;
    if (config !== (p?.default_config_name ?? '')) {
      update['default_config_name'] = config || '';
    }
    if (Object.keys(update).length === 0) return;
    this.isSavingSettings.set(true);
    this.editError.set(null);
    this.api.updateProjectFields(this.projectId, update).subscribe({
      next: () => {
        this.isSavingSettings.set(false);
        this.api.getProject(this.projectId).subscribe((p) => this.project.set(p));
      },
      error: (err: unknown) => {
        this.isSavingSettings.set(false);
        this.reportEditFailure(err);
      },
    });
  }

  onRenameProject(name: string): void {
    const p = this.project();
    if (!p || !name || name === p.name || this.isArchived()) return;
    const previous = p.name;
    this.renameError.set(null);
    // Optimistic: reflect the new name immediately, revert if the PATCH fails.
    this.project.set({...p, name});
    this.api.updateProjectFields(this.projectId, {name}).subscribe({
      error: (err: unknown) => {
        const cur = this.project();
        if (cur) this.project.set({...cur, name: previous});
        // A revert with no explanation is the bug this closes: the title used
        // to snap back and say nothing.
        this.renameError.set(this.errors.translate(err, 'errors.projects.renameFailed'));
      },
    });
  }

  toggleProjectMemory(checked: boolean): void {
    const p = this.project();
    if (!p || this.isArchived()) return;
    const existing = (p.default_config_override ?? {}) as Record<string, any>;
    const override = {
      ...existing,
      memory: { ...(existing['memory'] ?? {}), project_scoped: checked },
    };
    this.editError.set(null);
    this.api.updateProjectFields(this.projectId, { default_config_override: override }).subscribe({
      next: () => this.api.getProject(this.projectId).subscribe((proj) => this.project.set(proj)),
      error: (err: unknown) => this.reportEditFailure(err),
    });
  }

  toggleCloudReadOnly(checked: boolean): void {
    if (this.isArchived()) return;
    const previous = this.settingsCloudReadOnly();
    this.settingsCloudReadOnly.set(checked);
    this.editError.set(null);
    this.api.updateProjectFields(this.projectId, { cloud_storage_read_only: checked }).subscribe({
      next: () => this.api.getProject(this.projectId).subscribe((proj) => this.project.set(proj)),
      error: (err: unknown) => {
        this.settingsCloudReadOnly.set(previous);
        this.reportEditFailure(err);
      },
    });
  }

  onNetworkTierChange(value: string): void {
    if (value !== 'internet-only' && value !== 'home-allowed') return;
    if (this.isArchived()) return;
    const previous = this.settingsNetworkTier();
    if (value === previous) return;
    this.settingsNetworkTier.set(value);
    this.editError.set(null);
    this.api.updateProjectFields(this.projectId, { network_tier: value }).subscribe({
      next: () => {
        this.api.getProject(this.projectId).subscribe((proj) => this.project.set(proj));
      },
      error: (err: unknown) => {
        this.settingsNetworkTier.set(previous);
        this.reportEditFailure(err);
      },
    });
  }

  /** Workspace defaults: the Project's row in the chain (spec §6, Project
   *  page Settings tab). Loads the stored/effective/installation view, then
   *  the templates this Project may pick from, in the Catalog, Project and
   *  (per the view's `account_templates` flag) Account scopes. Templates
   *  load only once the view answers, since only it knows which scopes this
   *  caller may see. */
  loadWorkspaceDefaults(): void {
    this.api.getProjectWorkspaceDefaults(this.projectId).subscribe({
      next: (wd) => {
        this.applyWorkspaceDefaults(wd);
        const scopes: [string, string][] = [
          ['Catalog', 'shared'],
          ['Project', this.projectId],
        ];
        // The server's account_templates flag decides whether this Project's
        // caller may see their own personal Account templates.
        if (wd.account_templates) scopes.push(['Account', 'me']);
        const me = this.userService.currentUser()?.id ?? 'me';
        forkJoin(scopes.map(([kind, name]) => this.api.listWorkspaceTemplates(kind, name))).subscribe((lists) =>
          this.workspaceTemplates.set(
            lists.flatMap((list) =>
              list.resources.map(({resource}) => {
                const scope = resource.metadata.scope;
                // The option list keys an Account scope to the caller's own id,
                // not the literal 'me' sent in the query, so a stored personal-
                // project Account ref (normalized to the caller's id server-side)
                // matches an option here — see templateKey above.
                const scopeName = scope.kind === 'Account' ? me : scope.name;
                return {
                  key: `${scope.kind}/${scopeName}/${resource.metadata.name}`,
                  label: resource.metadata.annotations?.['srw.io/display-name'] ?? resource.metadata.name,
                  backend: resource.spec.backend ?? '',
                };
              }),
            ),
          ),
        );
      },
      error: () => this.workspaceDefaults.set(null),
    });
  }

  private applyWorkspaceDefaults(wd: ProjectWorkspaceDefaults): void {
    this.workspaceDefaults.set(wd);
    this.wdJobs.set(wd.stored.jobs);
    this.wdSessions.set(wd.stored.sessions);
    this.wdContainer.set(templateKey(wd.stored.container));
    this.wdVm.set(templateKey(wd.stored.vm));
  }

  saveWorkspaceDefaults(): void {
    if (!this.canEditWorkspaceDefaults()) return;
    // Without VMs the VM fields are hidden and the server refuses VM values
    // (422): a VM mode or template kept from before goes back to the default.
    const vms = this.workspaceDefaults()?.vm_available !== false;
    const mode = (value: WorkspaceMode | null) => (vms || value !== 'vm' ? value : null);
    this.isSavingWorkspaceDefaults.set(true);
    this.editError.set(null);
    this.api
      .putProjectWorkspaceDefaults(this.projectId, {
        jobs: mode(this.wdJobs()),
        sessions: mode(this.wdSessions()),
        container: templateRef(this.wdContainer()),
        vm: vms ? templateRef(this.wdVm()) : null,
      })
      .subscribe({
        next: (wd) => {
          this.applyWorkspaceDefaults(wd);
          this.isSavingWorkspaceDefaults.set(false);
        },
        error: (err: unknown) => {
          this.editError.set(this.errors.translate(err, 'projectDetail.settings.workspaceDefaultsSaveFailed'));
          this.isSavingWorkspaceDefaults.set(false);
        },
      });
  }

  /** Renders a `WorkspaceMode` ('none' | 'virtual' | 'container' | 'vm') as
   *  its translated label, for the "Installation default (…)" option text. */
  modeLabel(mode: string | null | undefined): string {
    return mode ? this.transloco.translate(`projectDetail.settings.workspaceMode.${mode}`) : '';
  }

  /** One place for "the server refused this edit". The 409 an archived project
   *  answers with is a plain-string `detail` naming the remedy, so it is
   *  rendered verbatim; anything else falls back to a translated line. */
  private reportEditFailure(err: unknown): void {
    this.editError.set(this.errors.translate(err, 'errors.projects.updateFailed'));
  }

  confirmArchive(): void {
    this.lifecycleError.set(null);
    this.archiveReport.set(null);
    this.pendingLifecycle.set('archived');
  }

  confirmUnarchive(): void {
    this.lifecycleError.set(null);
    this.archiveReport.set(null);
    this.pendingLifecycle.set('active');
  }

  /**
   * The one PATCH an archived project still accepts, and the one whose refusal
   * the user has to be able to read: this went through `updateProject`, whose
   * `catchError(() => of(null))` erased the body, under a subscribe with no
   * error callback at all — so a 409 landed as a no-op that looked like a
   * click that did not register.
   */
  applyLifecycle(): void {
    const status = this.pendingLifecycle();
    if (!status || this.lifecycleBusy()) return;
    this.lifecycleBusy.set(true);
    this.lifecycleError.set(null);
    this.api.setProjectStatus(this.projectId, status).subscribe({
      next: (report) => {
        this.lifecycleBusy.set(false);
        this.pendingLifecycle.set(null);
        if (status === 'archived') this.archiveReport.set(this.describeArchive(report));
        this.api.getProject(this.projectId).subscribe((p) => {
          this.project.set(p);
          // can_edit follows the archive/unarchive without a page reload.
          this.loadWorkspaceDefaults();
        });
      },
      error: (err: unknown) => {
        this.lifecycleBusy.set(false);
        this.pendingLifecycle.set(null);
        this.lifecycleError.set(
          this.errors.translate(
            err,
            status === 'archived'
              ? 'errors.projects.archiveFailed'
              : 'errors.projects.unarchiveFailed',
          ),
        );
      },
    });
  }

  /** Archiving quiesces the project's unattended machinery rather than
   *  refusing, so say what it stopped — a loop that silently paused is the
   *  kind of thing a user finds out about a week later. */
  private describeArchive(report: ProjectArchiveReport | null): string {
    const parts = [this.transloco.translate('projectDetail.settings.archivedReportBase')];
    if (report?.loop_paused) {
      parts.push(this.transloco.translate('projectDetail.settings.archivedReportLoop'));
    }
    if (report?.officer_held) {
      parts.push(this.transloco.translate('projectDetail.settings.archivedReportOfficer'));
    }
    const parked = report?.jobs_parked ?? 0;
    if (parked > 0) {
      parts.push(
        this.transloco.translate(
          parked === 1
            ? 'projectDetail.settings.archivedReportJobsOne'
            : 'projectDetail.settings.archivedReportJobs',
          {count: parked},
        ),
      );
    }
    return parts.join(' ');
  }

  deleteProject(): void {
    if (!confirm(this.transloco.translate('projectDetail.settings.deleteConfirm'))) return;
    this.api.deleteProject(this.projectId).subscribe((res) => {
      if (res) this.router.navigate(['/projects']);
    });
  }

  createJobInProject(): void {
    this.router.navigate(['/jobs/new'], { queryParams: { project: this.projectId } });
  }

  /** Cross-link to the Automations page with this project preselected.
   *  The /automations page opens its editor when the ?project= query
   *  param is present. */
  openAutomationsForProject(): void {
    this.router.navigate(['/automations'], {
      queryParams: { project: this.projectId },
    });
  }

  // Knowledge
  loadKBSummary(): void {
    this.api.getKnowledgeSummary(this.projectId).subscribe((s) => {
      this.kbSummary.set(s);
      if (s && s.total > 0) this.loadKBNotes();
    });
  }

  loadKBNotes(): void {
    this.kbIsLoading.set(true);
    this.api.getKnowledgeNotes(this.projectId, {
      type: this.kbFilterType() || undefined,
      status: this.kbFilterStatus() || undefined,
      limit: 50,
      offset: 0,
    }).subscribe({
      next: (res) => {
        this.kbNotes.set(res?.notes ?? []);
        this.kbTotal.set(res?.total ?? 0);
        this.kbIsLoading.set(false);
      },
      error: () => this.kbIsLoading.set(false),
    });
  }

  onKbFilterTypeChange(value: string): void {
    this.kbFilterType.set(value);
    this.loadKBNotes();
  }

  onKbFilterStatusChange(value: string): void {
    this.kbFilterStatus.set(value);
    this.loadKBNotes();
  }

  loadMoreKBNotes(): void {
    const current = this.kbNotes();
    this.api.getKnowledgeNotes(this.projectId, {
      type: this.kbFilterType() || undefined,
      status: this.kbFilterStatus() || undefined,
      limit: 50,
      offset: current.length,
    }).subscribe((res) => {
      if (res) this.kbNotes.set([...current, ...res.notes]);
    });
  }

  searchKB(): void {
    const q = this.kbSearchQuery().trim();
    if (!q) { this.loadKBNotes(); return; }
    this.kbIsLoading.set(true);
    this.api.searchKnowledge(this.projectId, q, 20).subscribe({
      next: (res) => {
        this.kbNotes.set(res?.notes ?? []);
        this.kbTotal.set(res?.total ?? 0);
        this.kbIsLoading.set(false);
      },
      error: () => this.kbIsLoading.set(false),
    });
  }

  clearKBFilters(): void {
    this.kbSearchQuery.set('');
    this.kbFilterType.set('');
    this.kbFilterStatus.set('');
    this.loadKBNotes();
  }

  openNote(noteId: string): void {
    this.api.getKnowledgeNote(this.projectId, noteId).subscribe((note) => {
      if (note) this.kbSelectedNote.set(note);
    });
  }

  updateNoteStatus(noteId: string, status: string): void {
    this.api.updateKnowledgeNote(this.projectId, noteId, { status }).subscribe((res) => {
      if (res) {
        this.openNote(noteId);
        this.loadKBSummary();
      }
    });
  }

  deleteNote(noteId: string): void {
    if (!confirm(this.transloco.translate('projectDetail.knowledge.deleteConfirm', {noteId}))) return;
    this.api.deleteKnowledgeNote(this.projectId, noteId).subscribe((res) => {
      if (res) {
        this.kbSelectedNote.set(null);
        this.loadKBNotes();
        this.loadKBSummary();
      }
    });
  }

  exportKB(): void {
    this.api.exportKnowledge(this.projectId).subscribe((res) => {
      if (res) alert(this.transloco.translate('projectDetail.knowledge.exportedAlert', {count: res.note_count, path: res.path}));
    });
  }

  kbTypeEntries(summary: KnowledgeSummary): [string, number][] {
    return Object.entries(summary.by_type).slice(0, 6);
  }

  // Helpers
  boolToText(v: boolean | null | undefined): string {
    if (v === null || v === undefined) return '';
    return v ? 'true' : 'false';
  }

  statusTone(status: string): 'success' | 'neutral' | 'danger' | 'warning' | 'accent' {
    switch (status) {
      case 'active': return 'success';
      case 'archived': return 'neutral';
      default: return 'neutral';
    }
  }

  getInitials(name: string): string {
    return name.split(' ').map((w) => w[0]).join('').toUpperCase().slice(0, 2);
  }

  truncate(text: string | undefined, max: number): string {
    if (!text) return '';
    return text.length <= max ? text : text.slice(0, max) + '...';
  }

  /** Flatten a note's Markdown to plain prose, then truncate, for the card preview. */
  notePreview(text: string | undefined): string {
    return this.truncate(stripMarkdown(text), 180);
  }

  formatDate(dateString: string): string {
    const date = new Date(dateString);
    return date.toLocaleDateString(this.transloco.getActiveLang(), {
      day: '2-digit',
      month: 'short',
      year: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
      hour12: false,
    });
  }
}
