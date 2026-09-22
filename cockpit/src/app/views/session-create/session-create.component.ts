import {workspaceCreationFields, workspacePreviewConfig} from "../agent-settings/workspace-selection";
import {Component, computed, effect, inject, OnInit, signal, ViewChild} from '@angular/core';
import {ActivatedRoute, Router} from '@angular/router';
import {HttpClient} from '@angular/common/http';
import {firstValueFrom} from 'rxjs';
import {environment} from '../../core/environment';
import {UserService} from '../../core/services/user.service';
import {AgentSettingsComponent} from '../../views/agent-settings/agent-settings.component';
import {ApiService, SessionToolGroupsResponse} from '../../core/services/api.service';
import {resolveEffectiveModels} from '../../views/agent-settings/agent-settings.types';
import {ModelService} from '../../core/services/model.service';
import {CapabilitiesService} from '../../core/services/capabilities.service';
import {ErrorMessageService} from '../../core/services/error-message.service';
import {SidebarToggleComponent} from '../../shell/sidebar-toggle/sidebar-toggle.component';
import {TranslocoPipe} from '@jsverse/transloco';
import {AppButtonComponent} from '../../ui/button';
import {AppInputComponent} from '../../ui/input';
import {AppChipComponent} from '../../ui/chip';
import {AppIconComponent} from '../../ui/icon';
import {AppFormFieldComponent} from '../../ui/form-field';
import {AppSwitchComponent} from '../../ui/switch';
import {EffectiveModels, EligibleDatasource, ExpertDefaultsResponse} from '../../core/models/api.model';

interface Project {
  id: string;
  name: string;
  status: string;
  description?: string;
  is_default?: boolean;
  main_cloud_backend?: string | null;
}

/**
 * Whether the protected-cloud session-create checkbox should render.
 *
 * Visible only when the deployment flag is on AND at least one selected
 * project is a non-default Nextcloud project (spec §2/§4: default projects
 * excluded in v1; Nextcloud-only per design §9.2).
 */
export function protectedCloudToggleVisible(
  featureOn: boolean,
  selected: Array<{ is_default?: boolean; main_cloud_backend?: string | null }>,
): boolean {
  return featureOn && selected.some(
    (p) => !p.is_default && p.main_cloud_backend === 'nextcloud',
  );
}

/**
 * What "Start a new session" carries forward from a drifted session's source
 * thread (session_config_drift_resume.md §8.3, chat-page.component.ts's
 * `onStartNewSession`). Built from the raw `GET /api/persistent/threads/{id}`
 * response (the same shape settings-pane.component.ts already reads).
 *
 * `null` only when the fetch itself failed or returned nothing — every other
 * field is present-but-possibly-empty, so the caller can tell "there is no
 * prefill" from "the source thread genuinely had none of this."
 */
export interface SessionCreatePrefill {
  projectIds: string[];
  expertId: string | null;
  model: string | null;
  datasourceIds: string[];
}

export function mapThreadToPrefill(thread: Record<string, unknown> | null): SessionCreatePrefill | null {
  if (!thread) return null;
  const metadata = (thread['metadata'] ?? {}) as Record<string, unknown>;
  const configOverride = (metadata['config_override'] ?? {}) as Record<string, unknown>;
  const llm = (configOverride['llm'] ?? {}) as Record<string, unknown>;
  const rawProjectIds = (thread['project_ids'] as unknown[] | undefined) ?? [];
  const rawDatasourceIds = (metadata['datasource_ids'] as unknown[] | undefined) ?? [];
  const expertId = metadata['expert_id'];
  const model = llm['model'];
  return {
    projectIds: rawProjectIds.map(String),
    expertId: typeof expertId === 'string' && expertId ? expertId : null,
    model: typeof model === 'string' && model ? model : null,
    datasourceIds: rawDatasourceIds.map(String),
  };
}

/**
 * Keep only ids the create form currently offers as eligible. Used for both
 * the project and connector prefill so an id that drifted between the source
 * thread and this page (deleted, revoked, out of scope) is silently dropped
 * rather than submitted — the create page never needs to know anything about
 * *why* an id is gone, only that it isn't in the eligible list it already
 * loaded for an unrelated reason.
 */
export function keepEligibleIds(ids: string[], eligible: Array<{ id: string }>): string[] {
  const set = new Set(eligible.map((item) => item.id));
  return ids.filter((id) => set.has(id));
}

interface Expert {
  id: string;
  display_name: string;
  description: string;
  icon: string;
  color: string;
  tags: string[];
  /** 'bundled' (disk) | 'user' | 'global' (DB). DB experts → expert_id. */
  source?: string;
  storage_kind?: 'bundled' | 'db';
  expert_type?: string;
}

interface ExpertDetail extends Expert {
  workspace_preference?: {backend: "none" | "virtual" | "sandbox" | "vm"} | null;
  config: Record<string, unknown>;
  instructions: string | null;
  settings_matrix?: Record<string, Record<string, unknown>>;
  /** Effective model + provenance per slot (server-resolved). */
  effective_models?: EffectiveModels | null;
}

/**
 * Full-page session creation component.
 * Uses the shared AgentSettingsComponent in session mode (horizontal tabs).
 */
@Component({
  selector: 'app-session-create',
  standalone: true,
  imports: [
    AgentSettingsComponent,
    SidebarToggleComponent,
    TranslocoPipe,
    AppButtonComponent,
    AppInputComponent,
    AppChipComponent,
    AppIconComponent,
    AppFormFieldComponent,
    AppSwitchComponent,
  ],
  template: `
    <div class="session-create-page">
      <div class="page-header">
        <app-sidebar-toggle />
        <h2>{{ 'sessions.create.title' | transloco }}</h2>
        <app-button variant="secondary" size="sm" (clicked)="cancel()">
          {{ 'sessions.create.cancel' | transloco }}
        </app-button>
      </div>

      <div class="form-container">
        <!-- Title -->
        <app-form-field [label]="'sessions.create.titleLabel' | transloco">
          <app-input
            [(value)]="title"
            [placeholder]="'sessions.create.titlePlaceholder' | transloco"
            [disabled]="creating()"
          />
        </app-form-field>

        <!-- Projects (multi-select chips) -->
        @if (projects().length > 0) {
          <app-form-field
            [label]="'sessions.create.projectsLabel' | transloco"
            [hint]="'sessions.create.projectsHint' | transloco"
            [error]="archivedSelected() ? ('sessions.create.projectArchivedWarning' | transloco) : ''"
          >
            <div class="project-chips">
              @for (project of projects(); track project.id) {
                <app-chip
                  [selected]="selectedProjectIds().has(project.id)"
                  [disabled]="creating()"
                  [ariaLabel]="project.description || project.name"
                  (clicked)="toggleProject(project.id)"
                >{{ project.name }}@if (project.status === 'archived') { {{ 'sessions.create.projectArchived' | transloco }}}</app-chip>
              }
            </div>
          </app-form-field>
        }

        <!-- Protected cloud toggle: only for non-default Nextcloud projects,
             gated on the deployment feature flag (Slice C). -->
        @if (protectedCloudVisible()) {
          <label class="protected-cloud-toggle">
            <input type="checkbox" [checked]="protectedCloud()"
                   [disabled]="creating()"
                   (change)="protectedCloud.set($any($event.target).checked)" />
            {{ 'sessions.create.protectedCloud' | transloco }}
          </label>
          <span class="field-hint">{{ 'sessions.create.protectedCloudHint' | transloco }}</span>
        }

        <!-- Expert selector: session experts by default (expert_type || tags);
             "Show all experts" lists every role — the server accepts a
             cross-role pick and resolves it on the session overlay. -->
        <app-form-field [label]="'sessions.create.expertLabel' | transloco">
          <app-switch formFieldAction size="sm" [checked]="showAllExperts()" [disabled]="creating()" (changed)="setShowAllExperts($event)">
            {{ 'experts.showAll' | transloco }}
          </app-switch>
          @if (loadingExperts()) {
            <div class="loading-hint">{{ 'sessions.create.expertLoading' | transloco }}</div>
          } @else if (experts().length > 0) {
            <div class="expert-grid">
              @for (expert of experts(); track expert.id) {
                <button
                  type="button"
                  class="expert-card"
                  [class.selected]="selectedExpert()?.id === expert.id"
                  [style.--expert-color]="expert.color"
                  (click)="toggleExpert(expert)"
                  [disabled]="creating()"
                >
                  @if (selectedExpert()?.id === expert.id) {
                    <app-icon size="lg" class="expert-check">check_circle</app-icon>
                  }
                  <app-icon size="inherit" class="expert-icon" [style.color]="expert.color">{{ expert.icon }}</app-icon>
                  <span class="expert-name">{{ expert.display_name }}</span>
                  <span class="expert-desc">{{ expert.description }}</span>
                  @if (expert.expert_type && expert.expert_type !== 'session') {
                    <span class="expert-role">{{ expert.expert_type }}</span>
                  }
                </button>
              }
            </div>
          }
          @if (selectedExpert()) {
            <span class="field-hint">
              {{ 'sessions.create.expertSelectedPrefix' | transloco }} {{ selectedExpert()!.display_name }}
              @if (selectedExpertSource()) {
                · {{ ('settings.expertDefaults.source.' + selectedExpertSource()) | transloco }}
              }
            </span>
          }
        </app-form-field>

        <!-- Agent Settings (horizontal tabs: Settings / Advanced) -->
        @if (expertDetail()?.workspace_preference?.backend; as preference) {
          <p class="field-hint">{{ 'agentSettings.execution.workspaceRecommendation' | transloco:{tier: preference} }}</p>
        }
        <app-agent-settings
          mode="session"
          [config]="workspaceConfig()"
          [resolvedToolset]="toolPreview()"
          [readsResolvedToolset]="true"
          [disabled]="creating()"
          [settingsMatrix]="expertDetail()?.settings_matrix ?? frameworkSettingsMatrix()"
          [effectiveModels]="resolvedEffectiveModels()"
          [datasources]="datasources()"
          [loadingDatasources]="loadingDatasources()"
          [datasourceLoadError]="datasourceLoadError()"
          [datasourceContextKey]="datasourceContextKey()"
          [datasourceDefaultsEnabled]="capabilities.datasourceScopeAutoAttachAvailable()"
          [initialDatasourceIds]="prefillDatasourceIds()"
          [loadingExpert]="loadingExpert()"
          [gatedCapabilities]="capabilities.grants() ?? null"
          (change)="loadToolPreview()"
          (retryDatasources)="loadDatasourcesList()"
        />

        <!-- A rejected config is correctable, so the error lands here and the
             form keeps every selection instead of unmounting into the chat
             view and bouncing back to the sessions list. -->
        @if (createError()) {
          <div class="create-error" role="alert">
            <app-icon size="md">error</app-icon>
            <span>{{ createError() }}</span>
          </div>
        }

        <!-- Footer -->
        <div class="form-actions">
          <app-button variant="secondary" (clicked)="cancel()" [disabled]="creating()">
            {{ 'sessions.create.cancel' | transloco }}
          </app-button>
          <app-button
            variant="primary"
            [loading]="creating()"
            [disabled]="loadingDatasources() || datasourceLoadError() || loadingExpert() || loadingWorkspacePreview() || !vmSizingValid()"
            (clicked)="createSession()"
          >
            {{ creating() ? ('sessions.create.creating' | transloco) : ('sessions.create.createSession' | transloco) }}
          </app-button>
        </div>
      </div>
    </div>
  `,
  styles: [`
    :host {
      display: block;
      height: 100%;
      overflow: hidden;
    }
    .session-create-page {
      display: flex;
      flex-direction: column;
      height: 100%;
      background: var(--panel-bg, var(--panel-bg));
    }
    .page-header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 12px 16px;
      background: var(--panel-header-bg);
      border-bottom: 1px solid var(--border-color, var(--surface-0));
      flex-shrink: 0;
    }
    .page-header h2 {
      margin: 0;
      font-size: 16px;
      font-weight: 600;
      color: var(--text-primary, var(--text-primary));
    }
    .form-container {
      flex: 1;
      overflow: auto;
      display: flex;
      flex-direction: column;
      gap: 20px;
      padding: 20px;
      max-width: var(--content-max-width);
      width: 100%;
      margin: 0 auto;
    }
    /* The settings block is a solid bordered panel; without this it butts
       flush against the expert card grid and the cards look like they tuck
       under it. The gap above gives every section consistent separation. */
    app-agent-settings {
      display: block;
    }
    .create-error {
      display: flex;
      align-items: center;
      gap: 8px;
      padding: 10px 14px;
      margin-top: 16px;
      border-radius: var(--radius-control);
      background: var(--danger-tint);
      border: 1px solid var(--danger-tint);
      color: var(--danger);
      font-size: 13px;
    }

    .field-hint {
      display: block;
      margin-top: 4px;
      font-size: 11px;
      color: var(--text-muted);
    }
    .project-chips {
      display: flex;
      flex-wrap: wrap;
      gap: 6px;
    }
    .protected-cloud-toggle {
      display: flex;
      align-items: center;
      gap: 8px;
      font-size: 13px;
      color: var(--text-primary, var(--text-primary));
      cursor: pointer;
    }
    .loading-hint {
      font-size: 12px;
      color: var(--text-muted);
      padding: 8px 0;
    }
    .expert-grid {
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(180px, 1fr));
      gap: 10px;
    }
    .expert-role {
      font-size: 10px;
      padding: 1px 6px;
      border-radius: var(--radius-tag);
      background: color-mix(in srgb, var(--accent-color) 8%, transparent);
      color: var(--text-secondary);
    }
    .expert-card {
      position: relative;
      display: flex;
      flex-direction: column;
      align-items: flex-start;
      gap: 6px;
      padding: 14px;
      border: 1px solid var(--border-color, var(--surface-1));
      border-radius: var(--radius-surface);
      background: var(--surface-0, var(--surface-0));
      cursor: pointer;
      text-align: left;
      transition: all 0.15s;
      font-family: inherit;
      color: var(--text-primary, var(--text-primary));
    }
    .expert-card:hover:not(:disabled) {
      border-color: var(--expert-color, var(--accent-color));
      background: color-mix(in srgb, var(--accent-color) 20%, transparent);
    }
    .expert-card.selected {
      border-color: var(--expert-color, var(--accent-color));
      background: color-mix(in srgb, var(--accent-color) 20%, transparent);
      box-shadow: 0 0 0 1px var(--expert-color, var(--accent-color));
    }
    .expert-card:disabled {
      opacity: 0.6;
      cursor: not-allowed;
    }
    .expert-check {
      position: absolute;
      top: 8px;
      right: 8px;
      color: var(--expert-color, var(--accent-color));
    }
    .expert-icon {
      font-size: 28px;
    }
    .expert-name {
      font-size: 13px;
      font-weight: 600;
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
    .form-actions {
      display: flex;
      justify-content: flex-end;
      gap: 10px;
      padding-top: 16px;
      border-top: 1px solid var(--border-color, var(--surface-0));
    }

    @media (max-width: 768px) {
      .form-container {
        max-width: 100%;
        padding: 12px;
      }

      .page-header {
        padding: 8px 12px;
      }

      .expert-grid {
        grid-template-columns: 1fr;
      }

      .form-actions {
        flex-direction: column;
      }

      .form-actions app-button {
        width: 100%;
      }
    }
  `],
})
export class SessionCreateComponent implements OnInit {
  private readonly http = inject(HttpClient);
  private readonly router = inject(Router);
  // Optional: this component is constructed without a router context in
  // several existing bare-TestBed specs that predate the `from` prefill (no
  // ActivatedRoute provider). Real navigation always supplies one; `route`
  // being null there is equivalent to "no `from` param".
  private readonly route = inject(ActivatedRoute, {optional: true});
  private readonly userService = inject(UserService);
  private readonly modelService = inject(ModelService);
  private readonly errorMessages = inject(ErrorMessageService);
  private readonly api = inject(ApiService);
  readonly capabilities = inject(CapabilitiesService);

  @ViewChild(AgentSettingsComponent) agentSettings!: AgentSettingsComponent;

  title = '';
  readonly creating = signal(false);
  /** Server rejection of the submitted config, rendered in-form so the user
   *  can correct it without losing the rest of their selections. */
  readonly createError = signal<string | null>(null);
  readonly projects = signal<Project[]>([]);
  readonly selectedProjectIds = signal<Set<string>>(new Set());
  readonly selectedProjects = computed(() =>
    this.projects().filter(p => this.selectedProjectIds().has(p.id)),
  );
  /** An archived project can only be here because the source thread was on it;
   *  say so rather than letting the create fail unexplained. */
  readonly archivedSelected = computed(() =>
    this.selectedProjects().some(p => p.status === 'archived'),
  );
  readonly protectedCloud = signal(false);
  readonly protectedCloudVisible = computed(() =>
    protectedCloudToggleVisible(this.capabilities.protectedCloudAvailable(), this.selectedProjects()),
  );
  readonly experts = signal<Expert[]>([]);
  /** "Show all experts": drop the session role filter (U1 — every expert stays
   *  usable in every role; the picker's role is pre-selected, not enforced). */
  readonly showAllExperts = signal(false);
  readonly selectedExpert = signal<Expert | null>(null);
  private readonly effectiveDefaultExpertId = signal<string | null>(null);
  readonly selectedExpertSource = signal<'project' | 'user' | 'application' | 'explicit' | null>(null);
  private expertSelectionTouched = false;
  private defaultRequestSerial = 0;
  readonly expertDetail = signal<ExpertDetail | null>(null);
  readonly frameworkDefaults = signal<Record<string, unknown> | null>(null);
  readonly frameworkSettingsMatrix = signal<Record<string, Record<string, unknown>>>({});
  // Server-resolved effective models for the framework "defaults" expert — the
  // floor used when no expert is selected, so the model picker's "Default"
  // option shows the resolved chat pin instead of the config-literal placeholder.
  readonly frameworkEffectiveModels = signal<EffectiveModels | null>(null);
  readonly resolvedEffectiveModels = computed(() =>
    resolveEffectiveModels(this.expertDetail()?.effective_models, this.frameworkEffectiveModels()),
  );
  /**
   * What a session created with the CURRENT selection would bind — a
   * prediction, and labelled as one by the endpoint that produces it.
   *
   * The form has no agent to ask, so it cannot do better; what it must not do
   * is present the forecast as fact. The route it calls structurally cannot
   * return a measurement, which is what keeps that honest without relying on
   * anyone remembering to check a flag.
   */
  readonly toolPreview = signal<SessionToolGroupsResponse | null>(null);
  readonly workspaceConfig = computed(() => workspacePreviewConfig(
    this.expertDetail()?.config ?? this.frameworkDefaults() ?? {}, this.toolPreview()?.workspace,
  ));
  private toolPreviewSerial = 0;
  private expertDetailSerial = 0;
  readonly loadingWorkspacePreview = signal(false);
  readonly loadingExperts = signal(false);
  readonly loadingExpert = signal(false);
  readonly datasources = signal<EligibleDatasource[]>([]);
  readonly loadingDatasources = signal(false);
  readonly datasourceLoadError = signal(false);
  readonly datasourceContextKey = computed(() => {
    const ids = Array.from(this.selectedProjectIds()).sort();
    return ids.length > 0 ? `projects:${ids.join(',')}` : 'standalone';
  });
  private datasourceRequestSerial = 0;

  // --- "Start a new session" prefill (session_config_drift_resume.md §8.3) ---
  // `undefined` = the source-thread fetch (if any) hasn't settled yet; `null`
  // = there is no `from` param, or the fetch failed — either way, every
  // prefill-aware method below behaves exactly as if this feature didn't
  // exist. A real (possibly all-empty) object = the fetch succeeded, and is
  // authoritative for the fields it names, per-field, even when a field is
  // empty (e.g. a standalone source session with no project).
  private threadPrefill: SessionCreatePrefill | null | undefined = undefined;
  private projectSelectionTouched = false;
  private projectsLoaded = false;
  private modelPrefillApplied = false;
  private readonly prefillThreadDatasourceIds = signal<string[] | null>(null);
  /** Null keeps the connector picker's normal server-default behavior; a
   *  (possibly empty) array means the source thread's surviving connectors
   *  are authoritative for this field. A `computed()`, not a value copied out
   *  once, so it stays correct however the source-thread fetch and this
   *  form's own eligible-connectors load interleave, and if eligibility
   *  changes again before this is read. */
  readonly prefillDatasourceIds = computed<string[] | null>(() => {
    const raw = this.prefillThreadDatasourceIds();
    return raw === null ? null : keepEligibleIds(raw, this.datasources());
  });

  constructor() {
    effect(() => {
      const userId = this.userService.currentUserId();
      if (userId) this.loadProjects(userId);
    });
  }

  ngOnInit(): void {
    const fromThreadId = this.route?.snapshot.queryParamMap.get('from') ?? null;
    if (fromThreadId) {
      // Fire this first — its result gates the project/expert prefill below,
      // so giving it a head start (rather than parallel with everything else
      // that ALSO starts in this method) improves the odds it settles before
      // the lists it needs to be reconciled against.
      this.api.getPersistentThread(fromThreadId).subscribe((thread) => {
        this.applyThreadPrefillResult(thread);
      });
    } else {
      // No source thread — settle immediately (synchronously, before any of
      // the loads below even fire) so every prefill-aware method downstream
      // takes its original, unprefilled branch with zero added latency.
      this.threadPrefill = null;
    }
    this.modelService.load();
    this.loadExperts();
    this.loadDatasourcesList();
    // account_defaults: the session account layer (notably workspace.backend,
    // `virtual` by default — NOT session_base's `sandbox`) has to be in the
    // resolved config the form renders, or controls keyed off it disagree with
    // what create_thread resolves. That mismatch is what made the datasource
    // picker offer clone-based repository connectors on a lite tier and 400
    // every create.
    this.http.get<ExpertDetail>(
      `${environment.apiUrl}/experts/session_base?type=session&account_defaults=true`,
    ).subscribe({
      next: (d) => {
        if (d?.config) {
          this.frameworkDefaults.set(d.config);
          // Tool toggles keep their own override state rather than deriving it
          // directly from the config input. Synchronize the asynchronously
          // loaded session base so empty persistent categories render disabled.
          if (!this.expertDetail()) {
            this.agentSettings?.toolsGroup?.prefillFromConfig(d.config);
          }
        }
        if (d?.settings_matrix) this.frameworkSettingsMatrix.set(d.settings_matrix);
        if (d?.effective_models) this.frameworkEffectiveModels.set(d.effective_models);
      },
    });
  }

  /**
   * `GET /api/persistent/threads/{id}` settled — success or failure, since a
   * failed fetch must never block session creation and is treated identically
   * to "no `from` at all" (`mapThreadToPrefill(null)` is `null` either way;
   * `ApiService.getPersistentThread` already logs the failure and resolves to
   * `null` rather than throwing).
   *
   * Re-runs the project/expert resolution now that the prefill target is
   * known, so whichever of "this" and "the list it needs" settles second is
   * what actually decides — see `applyProjectPrefillOrDefault` and
   * `applyEffectiveDefault`.
   */
  private applyThreadPrefillResult(thread: Record<string, unknown> | null): void {
    this.threadPrefill = mapThreadToPrefill(thread);
    if (this.threadPrefill) {
      this.prefillThreadDatasourceIds.set(this.threadPrefill.datasourceIds);
    }
    this.applyProjectPrefillOrDefault();
    this.applyEffectiveDefault();
  }

  /** `status=active`: an archived project cannot take a new session, so
   *  offering one in this picker offers a guaranteed refusal. A project the
   *  source thread was on is the one exception — see
   *  `resolveArchivedPrefillProjects`. */
  private loadProjects(userId: string): void {
    this.http.get<Project[]>(`${environment.apiUrl}/projects?user_id=${userId}&status=active`).subscribe({
      next: (projects) => {
        this.projects.set(projects);
        this.projectsLoaded = true;
        this.applyProjectPrefillOrDefault();
        this.loadEffectiveDefault();
      },
    });
  }

  /**
   * Project selection on initial load: prefer the source thread's still-
   * visible projects ("Start a new session", session_config_drift_resume.md
   * §8.3) over the account default — the same "intersect with what's
   * currently offered" rule the connector prefill uses, so a project that
   * drifted between the two pages (deleted, membership revoked) is silently
   * dropped rather than submitted. Falls back to the account default exactly
   * as before this prefill existed, once it's known for certain there is no
   * usable prefill.
   *
   * Called from two places — here, and `applyThreadPrefillResult` once the
   * source-thread fetch settles — and is a no-op until BOTH the project list
   * and the prefill target are known, so whichever happens second is what
   * actually decides; network order can't produce a wrong final answer, only
   * a briefly-later correct one. Never re-applies once the user has touched a
   * project chip themselves.
   */
  private applyProjectPrefillOrDefault(): void {
    if (this.projectSelectionTouched) return;
    if (this.threadPrefill === undefined) return; // source-thread fetch (if any) still in flight
    if (!this.projectsLoaded) return; // this form's own project list still in flight
    const projects = this.projects();
    if (this.threadPrefill) {
      const survivors = keepEligibleIds(this.threadPrefill.projectIds, projects);
      if (survivors.length > 0) {
        this.selectedProjectIds.set(new Set(survivors));
        this.loadDatasourcesList();
        this.loadEffectiveDefault();
        this.loadToolPreview();
      }
      // else: the source thread had no project, or none it had are still
      // accessible here — leave unselected. Faithful to what actually
      // survived on the source thread, rather than substituting the
      // unrelated account default.
      //
      // "Not in the list" now has one benign cause — merely archived — which
      // is kept rather than dropped.
      this.resolveArchivedPrefillProjects(
        this.threadPrefill.projectIds.filter((id) => !survivors.includes(id)),
      );
      return;
    }
    const defaultProject = projects.find(p => p.is_default);
    if (defaultProject) {
      this.selectedProjectIds.set(new Set([defaultProject.id]));
      // Refresh eligible datasources now that a project is selected.
      this.loadDatasourcesList();
    }
  }

  /**
   * Prefilled project ids the active list does not contain. Most are genuinely
   * gone (deleted, membership revoked) and stay dropped; an ARCHIVED one is
   * kept, flagged and still selected, because silently narrowing the session's
   * scope is a change the user never asked for and would not see. Creating
   * against it is refused server-side, and this form renders that refusal.
   *
   * Fires at most once per missing id, and only when there is one — the
   * ordinary path issues no extra request at all.
   */
  private resolveArchivedPrefillProjects(missingIds: string[]): void {
    for (const id of missingIds) {
      this.api.getProject(id).subscribe((project) => {
        if (project?.status !== 'archived') return;
        if (this.projects().some((p) => p.id === project.id)) return;
        // This form models a project more narrowly than the API does.
        this.projects.update((list) => [...list, {
          id: project.id,
          name: project.name,
          status: project.status,
          description: project.description ?? undefined,
          is_default: project.is_default,
          main_cloud_backend: project.main_cloud_backend,
        }]);
        if (this.projectSelectionTouched) return;
        this.selectedProjectIds.update((ids) => new Set([...ids, project.id]));
        this.loadDatasourcesList();
        this.loadEffectiveDefault();
        this.loadToolPreview();
      });
    }
  }

  private loadExperts(): void {
    this.loadExpertList();
    this.loadEffectiveDefault();
  }

  /** The picker's rows: session experts (`expert_type || tags`), or every
   *  expert when "Show all experts" is on (no `type` filter). */
  private loadExpertList(): void {
    this.loadingExperts.set(true);
    const url = this.showAllExperts()
      ? `${environment.apiUrl}/experts`
      : `${environment.apiUrl}/experts?type=session`;
    this.http.get<Expert[]>(url).subscribe({
      next: (experts) => {
        this.experts.set(experts);
        this.applyEffectiveDefault();
        this.loadingExperts.set(false);
      },
      error: () => this.loadingExperts.set(false),
    });
  }

  setShowAllExperts(value: boolean): void {
    if (value === this.showAllExperts()) return;
    this.showAllExperts.set(value);
    this.loadExpertList();
  }

  private loadEffectiveDefault(): void {
    if (this.expertSelectionTouched) return;
    const serial = ++this.defaultRequestSerial;
    const projectIds = Array.from(this.selectedProjectIds());
    const projectId = projectIds.length === 1 ? projectIds[0] : undefined;
    const suffix = projectId ? `?project_id=${encodeURIComponent(projectId)}` : '';
    this.http.get<ExpertDefaultsResponse>(`${environment.apiUrl}/expert-defaults${suffix}`).subscribe({
      next: (response) => {
        if (serial !== this.defaultRequestSerial || this.expertSelectionTouched) return;
        this.effectiveDefaultExpertId.set(response?.defaults?.session?.effective?.id ?? null);
        this.selectedExpertSource.set(response?.defaults?.session?.source ?? null);
        this.applyEffectiveDefault();
      },
    });
  }

  /**
   * Resolve which expert should be selected: prefer the source thread's
   * expert ("Start a new session", session_config_drift_resume.md §8.3) over
   * the ordinary effective-default resolution, whenever it names one that is
   * currently in the eligible list. Re-checked on every call — not applied
   * once and left alone — so it stays correct regardless of whether
   * `experts()` or the source-thread fetch settles first.
   *
   * Unlike `applyProjectPrefillOrDefault`, an unresolved or invalid prefill
   * expert falls through to the normal default rather than leaving nothing
   * selected: a session always runs with *some* expert config, so an empty
   * expert grid would read as broken rather than as an intentional "none",
   * the way an empty project or connector selection reads.
   *
   * Fix round 1: gated on `threadPrefill === undefined`, matching its
   * sibling exactly, for a reason that only shows up under a slow `from`
   * fetch. Without this guard, a call landing before the thread-prefill
   * settles falls through to the ORDINARY default (below) and selects and
   * fetches it — and `fetchExpertDetail`'s `agentSettings.prefillFromConfig`
   * has no `hasToolEdits()` guard, unlike `loadToolPreview`'s re-anchor, so
   * it unconditionally resets the tools/model/execution groups. If the
   * thread-prefill then resolves late and names a DIFFERENT expert, this
   * function runs again, reselects, and fetches THAT expert too —
   * triggering a second `prefillFromConfig` that silently wipes whatever the
   * user touched in the gap between the two. Deferring the ordinary default
   * the same way the project prefill already does removes the window
   * entirely: no expert (right or wrong) is selected until we know whether
   * there's a prefill to prefer. See task-14-report.md, "Fix round 1".
   */
  private applyEffectiveDefault(): void {
    if (this.expertSelectionTouched) return;
    if (this.threadPrefill === undefined) return; // source-thread fetch (if any) still in flight
    const prefillId = this.threadPrefill?.expertId;
    if (prefillId) {
      const prefillExpert = this.experts().find(e => e.id === prefillId);
      if (prefillExpert) {
        // A successful prefill match settles this field the same way a
        // deliberate click does: it must not keep re-winning over a project
        // change the user makes afterward, the way the live
        // effective-default resolution is designed to.
        this.expertSelectionTouched = true;
        if (this.selectedExpert()?.id !== prefillExpert.id) {
          this.selectedExpert.set(prefillExpert);
          this.fetchExpertDetail(prefillExpert.id);
        } else if (this.expertDetail()?.id === prefillExpert.id) {
          // Already selected AND already fetched — e.g. it happened to be
          // the ordinary default too, and that fetch landed before the
          // source-thread fetch did. That fetch's prefillFromConfig already
          // ran and will not run again for this expert, so it's safe to
          // layer the model override on right now instead of waiting on a
          // fetchExpertDetail call that isn't coming.
          this.applyModelPrefillOnce();
        }
        return;
      }
      // Named an expert that isn't (yet, or ever) in the eligible list —
      // fall through and retry on the next call, exactly like the
      // unresolved-id case below.
    }
    const id = this.effectiveDefaultExpertId();
    const expert = id ? this.experts().find(item => item.id === id) : undefined;
    if (expert && this.selectedExpert()?.id !== expert.id) {
      this.selectedExpert.set(expert);
      this.fetchExpertDetail(expert.id);
    }
  }

  loadDatasourcesList(): void {
    const serial = ++this.datasourceRequestSerial;
    this.loadingDatasources.set(true);
    this.datasourceLoadError.set(false);
    // The server applies all-project scope matching and computes owner-specific
    // defaults for this exact session context.
    const qs = Array.from(this.selectedProjectIds())
      .map(id => `project_id=${encodeURIComponent(id)}`)
      .join('&');
    const url = `${environment.apiUrl}/datasources/eligible${qs ? '?' + qs : ''}`;
    this.http.get<EligibleDatasource[]>(url).subscribe({
      next: (ds) => {
        if (serial !== this.datasourceRequestSerial) return;
        this.datasources.set(ds);
        this.loadingDatasources.set(false);
      },
      error: () => {
        if (serial !== this.datasourceRequestSerial) return;
        // Preserve the last authorized rows and their explicit selection as
        // read-only context; the error state blocks create until retry wins.
        this.datasourceLoadError.set(true);
        this.loadingDatasources.set(false);
      },
    });
  }

  toggleProject(id: string): void {
    this.projectSelectionTouched = true;
    this.selectedProjectIds.update(current => {
      const next = new Set(current);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
    // Refresh eligible datasources for the new project selection.
    this.loadDatasourcesList();
    this.loadEffectiveDefault();
    // The project layer can override the expert's tools, so the prediction
    // moves with the selection.
    this.loadToolPreview();
  }

  toggleExpert(expert: Expert): void {
    this.expertSelectionTouched = true;
    this.selectedExpertSource.set('explicit');
    if (this.selectedExpert()?.id === expert.id) return;
    this.selectedExpert.set(expert);
    this.fetchExpertDetail(expert.id);
  }

  private fetchExpertDetail(expertId: string): void {
    const serial = ++this.expertDetailSerial;
    this.expertDetail.set(null);
    this.loadingExpert.set(true);
    this.http.get<ExpertDetail>(
      `${environment.apiUrl}/experts/${expertId}?account_defaults=true&role=session`,
    ).subscribe({
      next: (detail) => {
        if (serial !== this.expertDetailSerial) return;
        this.expertDetail.set(detail);
        if (detail?.config) this.agentSettings?.prefillFromConfig(detail.config);
        // Must run AFTER prefillFromConfig: that call resets the model group
        // to this expert's own config-derived default, and would silently
        // win over a model override applied before it.
        this.applyModelPrefillOnce();
        this.loadingExpert.set(false);
        this.loadToolPreview();
      },
      error: () => { if (serial === this.expertDetailSerial) this.loadingExpert.set(false); },
    });
    this.loadToolPreview();
  }

  /**
   * Pin the model picker to the source thread's model
   * (`metadata.config_override.llm.model`, session_config_drift_resume.md
   * §8.3), once, the first time it's safe to do so without a later
   * `prefillFromConfig` call silently overwriting it — i.e. right after
   * *some* expert's own config-driven reset has just run (from
   * `fetchExpertDetail`'s callback, or from `applyEffectiveDefault` finding
   * one was already fetched earlier). Guarded so a later, deliberate expert
   * switch (`toggleExpert`) resets the model to that expert's own default
   * normally, the same as it would without a prefill in play.
   *
   * Known gap, accepted rather than chased further: if no expert is *ever*
   * resolved (no default session expert configured at all, so the form falls
   * back to the bare framework config) this never fires, because nothing
   * else resets the model group in that case either — see the task report.
   */
  private applyModelPrefillOnce(): void {
    if (this.modelPrefillApplied) return;
    const model = this.threadPrefill?.model;
    if (!model) return;
    this.modelPrefillApplied = true;
    this.agentSettings?.setSessionModelOverride(model);
  }

  /**
   * Ask what this configuration would bind, and re-anchor the tool switches to
   * the answer.
   *
   * Serial-guarded: a slower answer for an expert the user has already
   * switched away from must not paint over the current one. And it re-anchors
   * ONLY while the user has not touched a tool switch — the config prefill has
   * already run by the time this lands, so clobbering a click here would be
   * the same late-response bug the live pane's forkJoin exists to prevent,
   * rebuilt on the other surface.
   */
  loadToolPreview(): void {
    const serial = ++this.toolPreviewSerial;
    this.loadingWorkspacePreview.set(true);
    const expert = this.selectedExpert();
    const projectIds = Array.from(this.selectedProjectIds());
    // Routed EXACTLY as createSession routes it. A preview that resolved a
    // different expert layer from the create it previews would be this
    // series' defect in the surface built to prevent it.
    const {configName, expertId} = this.expertRouting(expert);
    this.api.previewToolGroups({
      config_override: this.agentSettings?.getOverrides() ?? {},
      workspace_preference: this.expertDetail()?.workspace_preference?.backend ?? null,
      config_name: configName,
      expert_id: expertId ?? null,
      project_id: projectIds.length === 1 ? projectIds[0] : null,
    }).subscribe((preview) => {
      if (serial !== this.toolPreviewSerial) return;
      this.loadingWorkspacePreview.set(false);
      this.toolPreview.set(preview);
      const categories = preview?.categories;
      if (categories && !this.agentSettings?.hasToolEdits()) {
        this.agentSettings?.prefillFromResolvedToolset(categories);
      }
    });
  }

  /** How an expert selection maps onto the create/preview request.
   *
   *  DB-backed experts (source user/global/managed) go via `expert_id`;
   *  `config_name` stays the persistent base. Bundled experts keep the
   *  `config_name` path. Fixes the `config_name=<uuid>` conflation that
   *  crashed session boot — and shared with the tool preview so the two
   *  cannot resolve different expert layers. */
  private expertRouting(expert: Expert | null): {
    configName: string;
    expertId: string | undefined;
  } {
    const isDbExpert = expert?.storage_kind === 'db' ||
      ['user', 'global', 'managed'].includes(expert?.source ?? '');
    return {
      configName: expert && !isDbExpert ? expert.id : 'session_base',
      expertId: isDbExpert ? expert!.id : undefined,
    };
  }

  vmSizingValid(): boolean {
    return this.agentSettings?.vmSizingValid() ?? true;
  }

  async createSession(): Promise<void> {
    if (this.loadingDatasources() || this.datasourceLoadError() || this.loadingExpert() || this.loadingWorkspacePreview() || !this.vmSizingValid()) return;
    this.creating.set(true);

    const expert = this.selectedExpert();
    const {configName, expertId} = this.expertRouting(expert);
    const projectIds = Array.from(this.selectedProjectIds());

    // Build config_override from settings component
    const workspaceFields = workspaceCreationFields(this.agentSettings?.getOverrides() ?? {}, this.toolPreview()?.workspace);
    const configOverride = workspaceFields.config_override;

    // Extract permission_mode and model from overrides (session-specific handling).
    // Only send permission_mode when the user actually picked a per-session
    // override; omitting it lets the backend fall back to the user's saved
    // default, then the config default. Sending a hardcoded 'supervised' here
    // would clobber that saved default and forced every session to Supervised.
    const permissionMode = (configOverride['interactive'] as any)?.['permission_mode'] ?? null;
    const model = (configOverride['llm'] as any)?.['model'] ?? null;

    const body: Record<string, unknown> = {
      title: this.title || 'Untitled Session',
      config_name: configName,
      expert_id: expertId,
      project_ids: projectIds.length > 0 ? projectIds : undefined,
    };

    if (permissionMode) body['permission_mode'] = permissionMode;
    if (model) body['model'] = model;

    // Include full config_override if there are non-model overrides
    const hasNonTrivialOverrides = Object.keys(configOverride).some(
      k => k !== 'interactive' && k !== 'llm'
    ) || (configOverride['llm'] && Object.keys(configOverride['llm'] as any).some(k => k !== 'model'));
    if (hasNonTrivialOverrides) {
      body['config_override'] = configOverride;
    }

    if ("workspace" in workspaceFields) body["workspace"] = workspaceFields.workspace;

    // Datasource IDs
    const dsIds = this.agentSettings?.getSelectedDatasourceIds() ?? [];
    // Empty is an explicit opt-out; omission means "apply server defaults".
    body['datasource_ids'] = dsIds;

    if (this.protectedCloud() && this.protectedCloudVisible()) {
      body['protected_cloud'] = true;
    }

    // Create BEFORE navigating. The form used to hand the body to the chat
    // route and unmount, so a rejected config destroyed every selection and
    // dumped the user back on /sessions with nothing to correct. The POST is
    // fast (tens of ms — it returns as soon as the thread row exists; the slow
    // part is provisioning, which happens after); paying that here buys a
    // recoverable failure. On success we route to the real thread id and the
    // chat view runs its normal connect, showing the setup progress.
    this.createError.set(null);
    try {
      const resp = await firstValueFrom(
        this.http.post<{thread_id: string}>(
          `${environment.apiUrl}/persistent/threads`,
          body,
        ),
      );
      await this.router.navigate(['/sessions', resp.thread_id]);
    } catch (err) {
      this.createError.set(
        this.errorMessages.translate(err, 'sessions.create.failed'),
      );
      this.creating.set(false);
    }
  }

  cancel(): void {
    this.router.navigate(['/sessions']);
  }
}
