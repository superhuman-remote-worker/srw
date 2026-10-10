import {Injector, runInInjectionContext, signal} from '@angular/core';
import {HttpErrorResponse} from '@angular/common/http';
import {ActivatedRoute, Router} from '@angular/router';
import {TranslocoService} from '@jsverse/transloco';
import {of, Subject, throwError} from 'rxjs';
import {afterEach, describe, expect, it, vi} from 'vitest';

import {
  Datasource,
  Job,
  ProjectDatasource,
  ProjectMemberRole,
  ProjectWorkspaceDefaults,
} from '../../core/models/api.model';
import {ApiService} from '../../core/services/api.service';
import {CapabilitiesService} from '../../core/services/capabilities.service';
import {ErrorMessageService} from '../../core/services/error-message.service';
import {UserService} from '../../core/services/user.service';
import {ViewportService} from '../../core/services/viewport.service';
import {ProjectDetailPageComponent} from './project-detail.component';
import {ConnectorDriversService} from '../../core/services/connector-drivers.service';
import {
  ConnectorDriver,
  ConnectorDriverMatrix,
  driverForType,
} from '../../core/models/connector-driver.model';
// The API's own capability matrix for the built-in drivers.
import driversFixture from '../../core/models/fixtures/connector-drivers.json';
// The real catalogue, so these specs also prove the keys they name exist.
import en from '../../../assets/i18n/en.json';
import de from '../../../assets/i18n/de-DE.json';

const BUILTIN_DRIVERS = (driversFixture as unknown as ConnectorDriverMatrix).drivers;

function datasource(id: string, overrides: Partial<Datasource> = {}): Datasource {
  return {
    id,
    name: id,
    description: null,
    type: 'postgresql',
    connection_url: 'postgresql://db/app',
    cli_hint: null,
    default_branch: null,
    job_id: null,
    created_by: 'user-1',
    is_global: false,
    scope_mode: 'all',
    created_at: '',
    updated_at: '',
    ...overrides,
  };
}

function linkedDatasource(id: string): ProjectDatasource {
  return {
    ...datasource(id),
    linked_at: '',
    project_read_only: null,
    project_description: null,
  };
}

/** The GET's shape for a bare installation, no Project row, VMs on: what a
 *  freshly-created project's "Workspace defaults" section renders. */
const WORKSPACE_DEFAULTS: ProjectWorkspaceDefaults = {
  stored: {jobs: null, sessions: null, container: null, vm: null},
  managed_by_manifest: false,
  can_edit: true,
  account_templates: false,
  effective: {
    jobs: {mode: 'container', source: 'installation'},
    sessions: {mode: 'virtual', source: 'installation'},
    container: {template_name: 'container-full', source: 'builtin'},
    vm: {template_name: 'vm-full', source: 'builtin'},
  },
  installation: {jobs: 'container', sessions: 'virtual', container: 'container-full', vm: 'vm-full'},
  template_problems: {},
  installation_problems: [],
  vm_available: true,
};

/** `createComponent(false)` (a bare boolean) keeps its old meaning:
 *  `datasourceScopeAutoAttachAvailable`. An options object adds `userRole`,
 *  which switches the stubbed project from the suite's default (archived —
 *  see the "archived read-only settings" describe below) to an active one.
 *  `GET /api/projects/{id}` never returns `user_role` (only the list
 *  endpoints do), so the mock no longer carries it; the workspace-defaults
 *  tests drive edit/template access through the `getProjectWorkspaceDefaults`
 *  mock's `can_edit`/`account_templates` instead — see WORKSPACE_DEFAULTS. */
function createComponent(
  options:
    | boolean
    | {policyAvailable?: boolean; userRole?: ProjectMemberRole; drivers?: ConnectorDriver[] | null} = true,
) {
  const opts = typeof options === 'boolean' ? {policyAvailable: options} : options;
  // null: the capability matrix has not loaded, so the pre-matrix rules hold.
  const drivers = 'drivers' in opts ? (opts.drivers ?? null) : null;
  const connectorDrivers = {
    drivers: signal(drivers),
    load: vi.fn(),
    forType: (type: string) => driverForType(drivers, type),
  };
  const policyAvailable = opts.policyAvailable ?? true;
  const api = {
    getProjectDatasources: vi.fn().mockReturnValue(of([])),
    getLinkableProjectDatasources: vi.fn().mockReturnValue(
      of({items: [], next_cursor: null}),
    ),
    getEligibleDatasources: vi.fn().mockReturnValue(of([])),
    getDatasources: vi.fn().mockReturnValue(of([])),
    linkProjectDatasource: vi.fn().mockReturnValue(of({status: 'linked'})),
    getProjectRepositories: vi.fn().mockReturnValue(of([])),
    attachProjectKnowledgeRepository: vi.fn().mockReturnValue(of({status: 'attached'})),
    getKnowledgeSummary: vi.fn().mockReturnValue(of(null)),
    getProject: vi.fn().mockReturnValue(
      of(
        opts.userRole
          ? {id: 'project-a', status: 'active', is_default: false}
          : {id: 'project-a', status: 'archived'},
      ),
    ),
    setProjectStatus: vi.fn().mockReturnValue(of({archived: true})),
    updateProject: vi.fn().mockReturnValue(of({status: 'updated'})),
    updateProjectFields: vi.fn().mockReturnValue(of({status: 'updated'})),
    getProjectJobs: vi.fn().mockReturnValue(of([])),
    getProjectMembers: vi.fn().mockReturnValue(of([])),
    getProjectExperts: vi.fn().mockReturnValue(of([])),
    getProjectWorkspaceDefaults: vi.fn().mockReturnValue(of(WORKSPACE_DEFAULTS)),
    putProjectWorkspaceDefaults: vi.fn().mockReturnValue(of(WORKSPACE_DEFAULTS)),
    listWorkspaceTemplates: vi.fn().mockReturnValue(of({resources: []})),
  };
  const currentUser = signal({id: 'user-1', is_admin: false});
  const injector = Injector.create({
    providers: [
      {provide: ApiService, useValue: api},
      {provide: ActivatedRoute, useValue: {paramMap: of({get: () => 'project-a'})}},
      {provide: Router, useValue: {navigate: vi.fn()}},
      {provide: UserService, useValue: {currentUser}},
      {
        provide: CapabilitiesService,
        useValue: {
          datasourceScopeAutoAttachAvailable: () => policyAvailable,
          datasourceScopeAutoAttachAvailability$: of(policyAvailable),
        },
      },
      {
        provide: TranslocoService,
        useValue: {translate: (key: string) => key, getActiveLang: () => 'en'},
      },
      // The real service, so the lifecycle specs below prove the whole chain:
      // a 409 body reaching the sentence the user reads.
      {provide: ErrorMessageService, useClass: ErrorMessageService, deps: []},
      {provide: ViewportService, useValue: {isMobile: signal(false)}},
      {provide: ConnectorDriversService, useValue: connectorDrivers},
    ],
  });
  const component = runInInjectionContext(
    injector,
    () => new ProjectDetailPageComponent(),
  );
  (component as unknown as {projectId: string}).projectId = 'project-a';
  return {api, component, currentUser, connectorDrivers};
}

describe('ProjectDetailPageComponent connector candidates', () => {
  afterEach(() => vi.useRealTimers());

  it('loads v1 candidates from the target-aware linkable catalog', () => {
    const {api, component} = createComponent();
    const candidate = datasource('owned');
    api.getLinkableProjectDatasources.mockReturnValue(
      of({items: [candidate], next_cursor: 'next-page'}),
    );

    component.loadProjectDatasources();

    expect(api.getLinkableProjectDatasources).toHaveBeenCalledWith('project-a', {
      q: undefined,
      cursor: undefined,
      limit: 50,
    });
    expect(api.getEligibleDatasources).not.toHaveBeenCalled();
    expect(api.getDatasources).not.toHaveBeenCalled();
    expect(component.availableDatasources()).toEqual([candidate]);
    expect(component.datasourceCandidatesNextCursor()).toBe('next-page');
  });

  it('retains the legacy visible-connector list while v1 is unavailable', () => {
    const {api, component} = createComponent(false);
    const candidate = datasource('legacy');
    api.getDatasources.mockReturnValue(of([candidate]));

    component.loadProjectDatasources();

    expect(api.getDatasources).toHaveBeenCalledOnce();
    expect(api.getLinkableProjectDatasources).not.toHaveBeenCalled();
    expect(component.availableDatasources()).toEqual([candidate]);
  });

  it('never presents restricted, native, or already-linked rows as candidates', () => {
    const {api, component} = createComponent();
    api.getProjectDatasources.mockReturnValue(of([linkedDatasource('already-linked')]));
    api.getLinkableProjectDatasources.mockReturnValue(of({
      items: [
        datasource('owned-restricted', {
          scope_mode: 'projects',
          project_ids: ['project-b'],
        }),
        datasource('shared-public', {
          created_by: 'user-2',
          is_global: true,
          scope_mode: 'all',
        }),
        datasource('other-private', {created_by: 'user-2'}),
        datasource('other-project-scoped', {
          created_by: 'user-2',
          is_global: true,
          scope_mode: 'projects',
        }),
        datasource('native', {config: {native_project_id: 'project-b'}}),
        datasource('already-linked'),
      ],
      next_cursor: null,
    }));

    component.loadProjectDatasources();

    expect(component.availableDatasources().map(ds => ds.id)).toEqual([
      'owned-restricted',
      'shared-public',
    ]);
  });

  it('debounces search into the server contract and clears a hidden selection', () => {
    vi.useFakeTimers();
    const {api, component} = createComponent();
    const database = datasource('database', {name: 'Application Database'});
    const repository = datasource('repository', {
      name: 'Source Repository',
      type: 'repository',
      connection_url: 'https://example.test/repo.git',
    });
    api.getLinkableProjectDatasources
      .mockReturnValueOnce(of({items: [database, repository], next_cursor: null}))
      .mockReturnValueOnce(of({items: [repository], next_cursor: null}));
    component.loadDatasourceCandidates();
    component.dsLinkId.set('database');

    component.onDatasourceCandidateSearch('source');
    vi.advanceTimersByTime(250);

    expect(api.getLinkableProjectDatasources).toHaveBeenLastCalledWith('project-a', {
      q: 'source',
      cursor: undefined,
      limit: 50,
    });
    expect(component.availableDatasources().map(ds => ds.id)).toEqual(['repository']);
    expect(component.dsLinkId()).toBe('');
  });

  it('paginates and de-duplicates server-authorized candidates', () => {
    const {api, component} = createComponent();
    const first = datasource('first');
    const second = datasource('second');
    api.getLinkableProjectDatasources
      .mockReturnValueOnce(of({items: [first], next_cursor: 'cursor-1'}))
      .mockReturnValueOnce(of({items: [first, second], next_cursor: null}));

    component.loadDatasourceCandidates();
    component.loadMoreDatasourceCandidates();

    expect(api.getLinkableProjectDatasources).toHaveBeenLastCalledWith('project-a', {
      q: undefined,
      cursor: 'cursor-1',
      limit: 50,
    });
    expect(component.allDatasources().map(ds => ds.id)).toEqual(['first', 'second']);
    expect(component.datasourceCandidatesNextCursor()).toBeNull();
  });

  it('ignores stale v1 responses after a newer request starts', () => {
    const {api, component} = createComponent();
    const stale = new Subject<{items: Datasource[]; next_cursor: string | null}>();
    const fresh = new Subject<{items: Datasource[]; next_cursor: string | null}>();
    api.getLinkableProjectDatasources
      .mockReturnValueOnce(stale)
      .mockReturnValueOnce(fresh);

    component.loadDatasourceCandidates();
    component.loadDatasourceCandidates();
    stale.next({items: [datasource('stale')], next_cursor: null});
    fresh.next({items: [datasource('fresh')], next_cursor: null});

    expect(component.allDatasources().map(ds => ds.id)).toEqual(['fresh']);
  });

  it('fails closed and removes stale candidates when v1 loading fails', () => {
    const {api, component} = createComponent();
    component.allDatasources.set([datasource('stale')]);
    component.dsLinkId.set('stale');
    api.getLinkableProjectDatasources.mockReturnValue(
      throwError(() => new Error('unavailable')),
    );

    component.loadDatasourceCandidates();

    expect(component.datasourceCandidatesError()).toBe(true);
    expect(component.allDatasources()).toEqual([]);
    expect(component.dsLinkId()).toBe('');
  });

  it('links only a currently authorized candidate and keeps KB links read-only', () => {
    const {api, component} = createComponent();
    const kb = datasource('kb', {
      type: 'kb',
      connection_url: 'https://example.test/knowledge.git',
    });
    component.allDatasources.set([kb]);
    component.dsLinkId.set(kb.id);

    component.linkDatasource();

    expect(api.linkProjectDatasource).toHaveBeenCalledWith(
      'project-a',
      kb.id,
      {read_only: true},
    );
  });
});

describe('ProjectDetailPageComponent job outcomes', () => {
  it('presents blocked delivery distinctly from its rolling-safe storage status', () => {
    const {component} = createComponent();
    const job = {
      status: 'cancelled',
      completion_outcome_kind: 'blocked_undelivered',
    } as Job;

    expect(component.effectiveJobStatus(job)).toBe('blocked_undelivered');
  });
});


function kbConnector(id: string, overrides: Partial<Datasource> = {}): Datasource {
  return datasource(id, {
    type: 'kb',
    connection_url: 'https://github.com/acme/vault.git',
    default_branch: 'main',
    config: {root_path: 'knowledge'},
    ...overrides,
  });
}

describe('ProjectDetailPageComponent external knowledge base', () => {
  it('looks for attachable connectors only while the project has no vault', () => {
    const {api, component} = createComponent();
    api.getProjectRepositories.mockReturnValue(
      of([{id: 'r1', role: 'knowledge', name: 'vault'}]),
    );

    component.loadRepos();

    expect(component.hasKnowledgeRepo()).toBe(true);
    // Replacing a vault is not supported, so there is nothing to offer.
    expect(api.getDatasources).not.toHaveBeenCalled();
  });

  it('offers the knowledge connectors that no project has taken yet', () => {
    const {api, component} = createComponent();
    api.getProjectRepositories.mockReturnValue(of([{id: 'r1', role: 'source', name: 'code'}]));
    api.getDatasources.mockReturnValue(
      of([
        kbConnector('free'),
        kbConnector('taken', {
          config: {root_path: 'knowledge', native_project_id: 'project-b'},
        }),
      ]),
    );

    component.loadRepos();

    expect(api.getDatasources).toHaveBeenCalledWith(undefined, 'kb');
    expect(component.hasKnowledgeRepo()).toBe(false);
    expect(component.kbConnectors().map((c) => c.id)).toEqual(['free']);
  });

  it('attaches the chosen connector and reloads the repositories', () => {
    const {api, component} = createComponent();
    api.getProjectRepositories.mockReturnValue(of([]));
    component.kbAttachSelection.set('free');

    component.attachKnowledgeConnector();

    expect(api.attachProjectKnowledgeRepository).toHaveBeenCalledWith('project-a', {
      datasource_id: 'free',
    });
    // The repo list is the source of truth for "this project has a vault".
    expect(api.getProjectRepositories).toHaveBeenCalled();
    expect(component.kbAttachSelection()).toBe('');
    expect(component.isAttachingKb()).toBe(false);
  });

  it('does nothing without a selected connector', () => {
    const {api, component} = createComponent();
    component.kbAttachSelection.set('');

    component.attachKnowledgeConnector();

    expect(api.attachProjectKnowledgeRepository).not.toHaveBeenCalled();
  });

  it("shows the server's reason when the attach is refused", () => {
    const {api, component} = createComponent();
    api.attachProjectKnowledgeRepository.mockReturnValue(
      throwError(() => ({error: {detail: 'Set its note root to knowledge first'}})),
    );
    component.kbAttachSelection.set('free');

    component.attachKnowledgeConnector();

    expect(component.kbAttachError()).toBe('Set its note root to knowledge first');
    expect(component.isAttachingKb()).toBe(false);
  });
});

describe('ProjectDetailPageComponent archive lifecycle', () => {
  it('writes nothing until the confirmation is accepted', () => {
    // The raw confirm() this replaced was a browser dialog the app could
    // neither theme nor translate; what has to survive the swap is that the
    // write still waits for an answer.
    const {api, component} = createComponent();

    component.confirmArchive();

    expect(component.pendingLifecycle()).toBe('archived');
    expect(api.setProjectStatus).not.toHaveBeenCalled();
  });

  it('archives with a status-only body and says what it quiesced', () => {
    const {api, component} = createComponent();
    api.setProjectStatus.mockReturnValue(
      of({archived: true, loop_paused: true, officer_held: true, jobs_parked: 3}),
    );

    component.confirmArchive();
    component.applyLifecycle();

    expect(api.setProjectStatus).toHaveBeenCalledWith('project-a', 'archived');
    // Never refused because children are in flight — they are stopped, and the
    // user is told which ones.
    expect(component.archiveReport()).toContain(
      'projectDetail.settings.archivedReportLoop',
    );
    expect(component.archiveReport()).toContain(
      'projectDetail.settings.archivedReportJobs',
    );
    expect(component.pendingLifecycle()).toBeNull();
    expect(component.lifecycleError()).toBeNull();
  });

  it('reloads workspace defaults after the project refetch succeeds', () => {
    // can_edit follows an archive or unarchive without a page reload.
    const {api, component} = createComponent();
    api.setProjectStatus.mockReturnValue(of({archived: true}));
    api.getProjectWorkspaceDefaults.mockClear();

    component.confirmArchive();
    component.applyLifecycle();

    expect(api.getProjectWorkspaceDefaults).toHaveBeenCalledWith('project-a');
  });

  it('unarchives through the same path', () => {
    const {api, component} = createComponent();
    api.setProjectStatus.mockReturnValue(of({archived: false}));

    component.confirmUnarchive();
    component.applyLifecycle();

    expect(api.setProjectStatus).toHaveBeenCalledWith('project-a', 'active');
    // Unarchiving reports nothing: it deliberately resumes none of what
    // archiving paused.
    expect(component.archiveReport()).toBeNull();
  });

  it("surfaces the server's refusal instead of failing silently", () => {
    // This subscribe had no error callback at all, and the PATCH went through
    // `updateProject`, whose catchError erased the body first. A 409 landed
    // as a click that appeared to do nothing.
    const {api, component} = createComponent();
    const detail = 'This project is archived. Unarchive it before creating new work.';
    api.setProjectStatus.mockReturnValue(
      throwError(() => new HttpErrorResponse({status: 409, error: {detail}})),
    );

    component.confirmArchive();
    component.applyLifecycle();

    expect(component.lifecycleError()).toBe(detail);
    expect(component.lifecycleBusy()).toBe(false);
    expect(component.pendingLifecycle()).toBeNull();
  });
});

describe('ProjectDetailPageComponent archived read-only settings', () => {
  const ARCHIVED = {
    id: 'project-a',
    name: 'Better Resavio',
    description: 'A description',
    goal: 'A goal',
    status: 'archived' as const,
    is_default: false,
    default_config_name: 'developer',
    cloud_storage_read_only: false,
    network_tier: 'internet-only' as const,
  };

  function archived() {
    const made = createComponent();
    made.component.project.set(ARCHIVED as never);
    return made;
  }

  it('reads the archived lock off the project, not off a separate flag', () => {
    const {component} = archived();
    expect(component.isArchived()).toBe(true);

    component.project.set({...ARCHIVED, status: 'active'} as never);
    expect(component.isArchived()).toBe(false);
  });

  it('keeps the settings values loaded and readable while archived', () => {
    // Archived is read-only, not hidden. The panel still has to show what the
    // project is configured to do, so the load path seeds every field
    // regardless of status; only the writes are refused.
    const {api, component} = archived();
    api.getProject.mockReturnValue(of(ARCHIVED));

    component.loadAll();

    expect(component.settingsName()).toBe('Better Resavio');
    expect(component.settingsConfigName()).toBe('developer');
    expect(component.settingsNetworkTier()).toBe('internet-only');
    expect(component.isArchived()).toBe(true);
  });

  it('attempts no field write at all while the project is archived', () => {
    // Prevention over reporting: the server would refuse each of these whole
    // with a 409, so none of them is sent.
    const {api, component} = archived();
    component.settingsName.set('Renamed');
    component.settingsConfigName.set('scholar');

    component.saveSettings();
    component.onRenameProject('Renamed');
    component.toggleProjectMemory(true);
    component.toggleCloudReadOnly(true);
    component.onNetworkTierChange('home-allowed');

    expect(api.updateProjectFields).not.toHaveBeenCalled();
    expect(api.updateProject).not.toHaveBeenCalled();
    // The name the header shows is still the stored one — no optimistic edit
    // was applied and then rolled back.
    expect(component.project()?.name).toBe('Better Resavio');
    expect(component.settingsNetworkTier()).toBe('internet-only');
  });

  it('offers no way into overview edit mode while archived', () => {
    const {api, component} = archived();

    component.startEditOverview();

    expect(component.isEditingOverview()).toBe(false);
    component.saveOverview();
    expect(api.updateProjectFields).not.toHaveBeenCalled();
  });

  it('writes through the propagating call once the project is active again', () => {
    const {api, component} = archived();
    component.project.set({...ARCHIVED, status: 'active'} as never);
    component.settingsName.set('Renamed');
    component.settingsConfigName.set('developer');

    component.saveSettings();

    expect(api.updateProjectFields).toHaveBeenCalledWith('project-a', {name: 'Renamed'});
    // Never the swallowing one: its `null` on error is what hid the 409.
    expect(api.updateProject).not.toHaveBeenCalled();
  });

  it("shows the server's refusal when a save races an archive in another tab", () => {
    // Prevention cannot cover this: the form was opened on an active project.
    const {api, component} = archived();
    component.project.set({...ARCHIVED, status: 'active'} as never);
    const detail =
      'This project is archived and is read-only apart from its status. ' +
      'Unarchive it before editing anything else.';
    api.updateProjectFields.mockReturnValue(
      throwError(() => new HttpErrorResponse({status: 409, error: {detail}})),
    );
    component.settingsName.set('Renamed');

    component.saveSettings();

    expect(component.editError()).toBe(detail);
    expect(component.isSavingSettings()).toBe(false);
  });

  it('says why an inline rename snapped back instead of silently reverting', () => {
    // The old path was `updateProject` under a `next` that saw `null`: the
    // title reverted and nothing explained it.
    const {api, component} = archived();
    component.project.set({...ARCHIVED, status: 'active'} as never);
    const detail =
      'This project is archived and is read-only apart from its status. ' +
      'Unarchive it before editing anything else.';
    api.updateProjectFields.mockReturnValue(
      throwError(() => new HttpErrorResponse({status: 409, error: {detail}})),
    );

    component.onRenameProject('Renamed');

    expect(component.project()?.name).toBe('Better Resavio');
    expect(component.renameError()).toBe(detail);
  });

  it('rolls back a refused toggle so the control never lies about the server', () => {
    const {api, component} = archived();
    api.getProject.mockReturnValue(of({...ARCHIVED, status: 'active'}));
    component.loadAll();
    api.updateProjectFields.mockReturnValue(
      throwError(() => new HttpErrorResponse({status: 409, error: {detail: 'refused'}})),
    );

    component.toggleCloudReadOnly(true);
    component.onNetworkTierChange('home-allowed');

    expect(component.settingsCloudReadOnly()).toBe(false);
    expect(component.settingsNetworkTier()).toBe('internet-only');
    expect(component.editError()).toBe('refused');
  });
});

describe('ProjectDetailPageComponent project-memory toggle', () => {
  it('writes back the override it read with only memory.project_scoped changed', () => {
    // GET /api/projects/{id} serves default_config_override redacted (no
    // api_key, rclone_spec or workspace.remote) and the PATCH restores those
    // values only into sections the write leaves unchanged. So the toggle must
    // send every section it read, as read: dropping or reshaping one deletes
    // the stored secrets under it.
    const {api, component} = createComponent();
    const override = {
      memory: {project_scoped: true, recall_limit: 5},
      llm: {model: 'openrouter/some-model', base_url: 'https://openrouter.ai/api/v1'},
      workspace: {backend: 'sandbox', mounts: [{name: 'drive', path: '/mnt/d'}]},
    };
    component.project.set({
      id: 'project-a',
      name: 'P',
      status: 'active',
      default_config_override: override,
    } as never);

    component.toggleProjectMemory(false);

    expect(api.updateProjectFields).toHaveBeenCalledWith('project-a', {
      default_config_override: {
        ...override,
        memory: {project_scoped: false, recall_limit: 5},
      },
    });
  });
});

describe('ProjectDetailPageComponent workspace defaults', () => {
  it('loads the defaults with the project', () => {
    const {api, component} = createComponent();
    component.loadAll();
    expect(api.getProjectWorkspaceDefaults).toHaveBeenCalledWith('project-a');
    expect(component.workspaceDefaults()?.installation.jobs).toBe('container');
    expect(component.wdJobs()).toBeNull();
  });

  it('lets an owner save template references', () => {
    const {api, component} = createComponent({userRole: 'owner'});
    component.loadAll();
    component.wdJobs.set('container');
    component.wdContainer.set('Catalog/shared/container-minimal');
    component.saveWorkspaceDefaults();
    expect(api.putProjectWorkspaceDefaults).toHaveBeenCalledWith('project-a', {
      jobs: 'container',
      sessions: null,
      container: {name: 'container-minimal', scope: {kind: 'Catalog', name: 'shared'}},
      vm: null,
    });
  });

  it('can_edit: false makes the section read-only, even for an admin', () => {
    const {api, component, currentUser} = createComponent({userRole: 'owner'});
    currentUser.set({id: 'user-1', is_admin: true});
    api.getProjectWorkspaceDefaults.mockReturnValue(of({...WORKSPACE_DEFAULTS, can_edit: false}));
    component.loadAll();
    expect(component.canEditWorkspaceDefaults()).toBe(false);
  });

  it('can_edit: true makes it editable for a non-admin', () => {
    const {component} = createComponent({userRole: 'owner'});
    component.loadAll();
    expect(component.canEditWorkspaceDefaults()).toBe(true);
  });

  it('can_edit: true on an archived Project stays read-only', () => {
    const {api, component} = createComponent();
    api.getProjectWorkspaceDefaults.mockReturnValue(of({...WORKSPACE_DEFAULTS, can_edit: true}));
    component.loadAll();
    expect(component.canEditWorkspaceDefaults()).toBe(false);
  });

  it('lists no templates when the defaults GET fails', () => {
    const {api, component} = createComponent({userRole: 'owner'});
    api.getProjectWorkspaceDefaults.mockReturnValue(
      throwError(() => new HttpErrorResponse({status: 500})),
    );
    component.loadAll();
    expect(component.workspaceDefaults()).toBeNull();
    expect(api.listWorkspaceTemplates).not.toHaveBeenCalled();
  });

  it('hides VM choices when VMs are unavailable', () => {
    const {api, component} = createComponent({userRole: 'owner'});
    api.getProjectWorkspaceDefaults.mockReturnValue(of({...WORKSPACE_DEFAULTS, vm_available: false}));
    component.loadAll();
    expect(component.workspaceModes()).toEqual(['none', 'virtual', 'container']);
  });

  it('saves no VM values while VMs are unavailable', () => {
    // A row saved while VMs were on still carries them; the fields are
    // hidden now, and the server refuses VM values (422).
    const {api, component} = createComponent({userRole: 'owner'});
    api.getProjectWorkspaceDefaults.mockReturnValue(
      of({
        ...WORKSPACE_DEFAULTS,
        stored: {
          jobs: 'vm',
          sessions: 'vm',
          container: null,
          vm: {ref: {name: 'vm-full', scope: {kind: 'Catalog', name: 'shared'}}},
        },
        vm_available: false,
      }),
    );
    component.loadAll();
    component.wdContainer.set('Catalog/shared/container-minimal');
    component.saveWorkspaceDefaults();
    expect(api.putProjectWorkspaceDefaults).toHaveBeenCalledWith('project-a', {
      jobs: null,
      sessions: null,
      container: {name: 'container-minimal', scope: {kind: 'Catalog', name: 'shared'}},
      vm: null,
    });
  });

  function personalProject(accountTemplates: boolean) {
    const created = createComponent({userRole: 'owner'});
    created.api.getProject.mockReturnValue(
      of({id: 'project-a', status: 'active', is_default: true}),
    );
    created.api.getProjectWorkspaceDefaults.mockReturnValue(
      of({...WORKSPACE_DEFAULTS, account_templates: accountTemplates}),
    );
    created.component.loadAll();
    return created.api.listWorkspaceTemplates.mock.calls.map(([kind, name]) => `${kind}/${name}`);
  }

  it('account_templates: true lists Account/me', () => {
    expect(personalProject(true)).toEqual(['Catalog/shared', 'Project/project-a', 'Account/me']);
  });

  it("account_templates: false does not list Account/me, even when the Project is_default", () => {
    expect(personalProject(false)).toEqual(['Catalog/shared', 'Project/project-a']);
  });
});

describe('ProjectDetailPageComponent link access from the capability matrix', () => {
  it('offers the read-only switch only where the driver has both levels', () => {
    const {component} = createComponent({drivers: BUILTIN_DRIVERS});
    expect(component.linkAccess({type: 'postgresql'})).toBe('choice');
    expect(component.linkAccess({type: 'kb'})).toBe('read_only');
    // An MCP server binds every tool it lists: there is no read-only level.
    expect(component.linkAccess({type: 'mcp'})).toBe('read_write');
  });

  it('names what enforces the level a link binds, which is where it is true', () => {
    const {component} = createComponent({drivers: BUILTIN_DRIVERS});
    const level = (type: Datasource['type'], readOnly: boolean | null, config?: object) =>
      component.linkAccessLevel({type, project_read_only: readOnly, config});
    // A read-only link floors the connector: Postgres's READ ONLY transaction.
    expect(level('postgresql', true)?.enforced_by).toContain('READ ONLY transaction');
    expect(level('postgresql', null)?.id).toBe('ReadWrite');
    expect(level('postgresql', false)?.id).toBe('ReadWrite');
    expect(level('mcp', null)?.enforced_by).toContain('MCP server');
    expect(level('kb', null)?.enforced_by).toContain('never the URL');
    // The read-only of a declared-only driver is advisory, and says so.
    expect(level('generic', true)?.advisory).toBe(true);
    // A tiered driver keeps its stored tier, else the driver's default.
    expect(level('email', true)?.id).toBe('read');
    expect(level('email', null, {access: 'send'})?.id).toBe('send');
    expect(level('email', null)?.id).toBe('draft');
  });

  it('says a GitHub App connector\'s read-only is enforced, at its own rule too', () => {
    const {component} = createComponent({drivers: BUILTIN_DRIVERS});
    const app = {forge: 'github', github_app: {app_id: '1', installation_id: '2'}};
    const level = (over: object) =>
      component.linkAccessLevel({type: 'repository', config: app, ...over});
    // A read-only link: enforced (the token's contents: read), not advisory.
    expect(level({project_read_only: true})).toMatchObject({id: 'ReadOnly', advisory: false});
    // The connector's own read-only, or public with the flag never set.
    expect(level({read_only: true})?.id).toBe('ReadOnly');
    expect(level({is_global: true, read_only: null})?.id).toBe('ReadOnly');
    // Published read-write by its owner, on a read-write link.
    expect(level({is_global: true, read_only: false})?.id).toBe('ReadWrite');
    expect(component.githubAppReadOnly({type: 'repository', config: app, read_only: true})).toBe(true);
    // A token repository keeps the declared-only line.
    expect(
      component.linkAccessLevel({type: 'repository', config: {forge: 'github'}, project_read_only: true})
        ?.advisory,
    ).toBe(true);
    expect(component.githubAppReadOnly({type: 'repository', config: {forge: 'github'}, read_only: true})).toBe(false);
  });

  it("binds the stricter of the link and the creator's read-only tag", () => {
    const {component} = createComponent({drivers: BUILTIN_DRIVERS});
    const row = (over: object) => ({type: 'postgresql' as const, config: {}, ...over});
    // A read-write link cannot lift the creator's tag (decision 31) ...
    expect(component.linkAccessLevel(row({read_only: true, project_read_only: false}))?.id)
      .toBe('ReadOnly');
    // ... nor a public connector's with no mode set (decision 32).
    expect(component.linkAccessLevel(row({is_global: true, read_only: null}))?.id).toBe('ReadOnly');
    // Published read-write, the link decides.
    expect(component.linkAccessLevel(row({is_global: true, read_only: false}))?.id).toBe('ReadWrite');
    expect(
      component.linkAccessLevel(row({is_global: true, read_only: false, project_read_only: true}))?.id,
    ).toBe('ReadOnly');
    expect(component.linkAccessLevel(row({read_only: false}))?.id).toBe('ReadWrite');
  });

  it("shows a creator's read-only tag instead of the switch, and says why", () => {
    const {component} = createComponent({drivers: BUILTIN_DRIVERS});
    const tagged = {type: 'postgresql' as const, read_only: true};
    expect(component.creatorReadOnly(tagged)).toBe(true);
    expect(component.linkAccessShown(tagged)).toBe('read_only');
    expect(component.creatorReadOnly({type: 'postgresql', is_global: true, read_only: null})).toBe(true);
    expect(component.creatorReadOnly({type: 'postgresql', read_only: false})).toBe(false);
    expect(component.linkAccessShown({type: 'postgresql'})).toBe('choice');
    // A driver that is read-only anyway needs no note.
    expect(component.creatorReadOnly({type: 'kb', read_only: true})).toBe(false);
    const enText = (en as {projectDetail: {datasources: Record<string, string>}})
      .projectDetail.datasources['accessCreatorReadOnly'];
    const deText = (de as {projectDetail: {datasources: Record<string, string>}})
      .projectDetail.datasources['accessCreatorReadOnly'];
    expect(enText).toMatch(/everyone who uses it/);
    expect(deText).toMatch(/alle, die ihn nutzen/);
  });

  it("ignores a link change the creator's tag would override", () => {
    const {api, component} = createComponent({drivers: BUILTIN_DRIVERS});
    const updateProjectDatasource = vi.fn().mockReturnValue(of({status: 'updated'}));
    (api as unknown as {updateProjectDatasource: unknown}).updateProjectDatasource =
      updateProjectDatasource;
    component.projectDatasources.set([
      {
        ...datasource('pg-1'),
        read_only: true,
        linked_at: '',
        project_read_only: null,
        project_description: null,
      },
    ]);
    component.updateDatasourceReadOnly('pg-1', 'false');
    expect(updateProjectDatasource).not.toHaveBeenCalled();
  });

  it('keeps the KB rule until the matrix loads', () => {
    const {component} = createComponent({drivers: null});
    expect(component.linkAccess({type: 'kb'})).toBe('read_only');
    expect(component.linkAccess({type: 'mcp'})).toBe('choice');
    expect(component.linkAccessLevel({type: 'kb'})).toBeNull();
  });

  it('ignores a read-only change for a link with one level', () => {
    const {api, component} = createComponent({drivers: BUILTIN_DRIVERS});
    const updateProjectDatasource = vi.fn().mockReturnValue(of({status: 'updated'}));
    (api as unknown as {updateProjectDatasource: unknown}).updateProjectDatasource =
      updateProjectDatasource;
    component.projectDatasources.set([
      {...datasource('mcp-1', {type: 'mcp'}), linked_at: '', project_read_only: null, project_description: null},
      {...datasource('pg-1'), linked_at: '', project_read_only: null, project_description: null},
    ]);
    component.updateDatasourceReadOnly('mcp-1', 'true');
    expect(updateProjectDatasource).not.toHaveBeenCalled();
    component.updateDatasourceReadOnly('pg-1', 'true');
    expect(updateProjectDatasource).toHaveBeenCalledWith('project-a', 'pg-1', {read_only: true});
  });

  it('loads the matrix with the linked connectors', () => {
    const {component, connectorDrivers} = createComponent({drivers: BUILTIN_DRIVERS});
    component.loadProjectDatasources();
    expect(connectorDrivers.load).toHaveBeenCalled();
  });
});
