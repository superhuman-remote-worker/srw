import {Injector, runInInjectionContext, signal} from '@angular/core';
import {TranslocoService} from '@jsverse/transloco';
import {of, throwError} from 'rxjs';
import {describe, expect, it, vi} from 'vitest';

import {Datasource, DatasourceIndexStatus} from '../../core/models/api.model';
import {ApiService} from '../../core/services/api.service';
import {CapabilitiesService} from '../../core/services/capabilities.service';
import {UserService} from '../../core/services/user.service';
import {ViewportService} from '../../core/services/viewport.service';
import {ActivatedRoute} from '@angular/router';
import {DatasourceListComponent} from './datasource-list.component';
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

function emailDatasource(overrides: Partial<Datasource> = {}): Datasource {
  return {
    id: 'email-1',
    name: 'Personal Mailbox',
    description: 'AI folder triage',
    type: 'email',
    connection_url: null,
    cli_hint: null,
    default_branch: null,
    config: {
      access: 'read_write',
      folders: ['AI', 'AI/Processed'],
      drafts_folder: 'Entwürfe',
      from_address: 'user@example.com',
      recipient_allowlist: ['@example.org'],
      unattended_send: false,
    },
    job_id: null,
    created_by: 'user-1',
    created_at: '',
    updated_at: '',
    ...overrides,
  };
}

function kbDatasource(overrides: Partial<Datasource> = {}): Datasource {
  return {
    id: 'kb-1',
    name: 'Engineering Handbook',
    description: 'Team knowledge',
    type: 'kb',
    connection_url: 'https://github.com/acme/handbook.git',
    cli_hint: null,
    default_branch: 'main',
    config: {root_path: 'vault'},
    job_id: null,
    created_by: 'user-1',
    created_at: '',
    updated_at: '',
    ...overrides,
  };
}

const BUILTIN_DRIVERS = (driversFixture as unknown as ConnectorDriverMatrix).drivers;

/** `drivers: null` is the matrix not (yet) loaded: the pre-matrix rules. */
function createComponent(
  policyAvailable = true,
  contextProjectId: string | null = null,
  drivers: ConnectorDriver[] | null = null,
) {
  const ds = kbDatasource();
  const api = {
    getDatasources: vi.fn().mockReturnValue(of([])),
    getDatasourceCatalog: vi.fn().mockReturnValue(of({items: [], next_cursor: null})),
    getLinkableDatasourceProjects: vi.fn().mockReturnValue(
      of({items: [], next_cursor: null}),
    ),
    getProject: vi.fn().mockReturnValue(of(null)),
    getProjectMembers: vi.fn().mockReturnValue(of([])),
    getDatasourceIndexStatus: vi.fn().mockReturnValue(of(null)),
    createDatasource: vi.fn().mockReturnValue(of(ds)),
    updateDatasource: vi.fn().mockReturnValue(of({status: 'updated'})),
    testDatasource: vi.fn().mockReturnValue(of({status: 'ok', message: 'ok'})),
    reindexDatasource: vi.fn().mockReturnValue(
      of({status: 'ready', indexed_commit: 'abcdef123456', full: false}),
    ),
    deleteDatasource: vi.fn().mockReturnValue(of({status: 'deleted'})),
    generateSshKey: vi.fn().mockReturnValue(of(null)),
  };
  const currentUser = signal({id: 'user-1', is_admin: false});
  const injector = Injector.create({
    providers: [
      {provide: ApiService, useValue: api},
      {
        provide: TranslocoService,
        useValue: {
          translate: (key: string, params?: Record<string, string>) =>
            params?.['sha'] ? `${key}:${params['sha']}` : key,
        },
      },
      {provide: ViewportService, useValue: {isMobile: signal(false)}},
      {provide: UserService, useValue: {currentUser}},
      {
        provide: ConnectorDriversService,
        useValue: {
          drivers: signal(drivers),
          load: vi.fn(),
          forType: (type: string) => driverForType(drivers, type),
        },
      },
      {
        provide: ActivatedRoute,
        useValue: {
          snapshot: {
            queryParamMap: {get: (key: string) => key === 'project' ? contextProjectId : null},
          },
        },
      },
      {
        provide: CapabilitiesService,
        useValue: {
          canPublishDatasources: () => true,
          datasourceScopeAutoAttachAvailable: () => policyAvailable,
          datasourceScopeAutoAttachAvailability$: of(policyAvailable),
        },
      },
    ],
  });
  const component = runInInjectionContext(
    injector,
    () => new DatasourceListComponent(),
  );
  return {api, component, currentUser, ds};
}

describe('DatasourceListComponent OKF Knowledge Base support', () => {
  it('authors a KB with repository auth, branch, and OKF root config', () => {
    const {api, component} = createComponent();
    component.openCreateForm();
    component.formData = {
      name: 'Engineering Handbook',
      type: 'kb',
      connection_url: 'https://github.com/acme/handbook.git',
      description: 'Team knowledge',
      cli_hint: '',
      default_branch: 'main',
      root_path: 'vault',
      mcpTransport: 'http',
      mcpToken: '',
      mcpHeaders: [],
      mcpCommand: '',
      mcpArgs: '',
      mcpEnv: [],
      is_global: false,
      read_only: true,
      scope_mode: 'all',
      auto_attach: false,
      policy_revision: null,
    };
    component.gitAuthMethod = 'token';
    component.formCredentials.password = 'secret-token';

    component.saveForm();

    expect(api.createDatasource).toHaveBeenCalledWith(
      expect.objectContaining({
        type: 'kb',
        default_branch: 'main',
        config: {root_path: 'vault'},
        credentials: {auth_method: 'token', token: 'secret-token'},
      }),
    );
  });

  it('loads root config for editing and leaves blank credentials untouched', () => {
    const {api, component, ds} = createComponent();
    component.openEditForm(ds);
    expect(component.formData.root_path).toBe('vault');
    component.gitAuthMethod = 'ssh';
    component.gitSshKey = '';

    component.saveForm();

    expect(api.updateDatasource).toHaveBeenCalledWith(
      ds.id,
      expect.objectContaining({
        config: {root_path: 'vault'},
        credentials: undefined,
      }),
    );
  });

  it('runs normal and confirmed full rebuilds', () => {
    const {api, component, ds} = createComponent();
    const confirmSpy = vi.spyOn(globalThis, 'confirm').mockReturnValue(true);

    component.reindexDatasource(ds, false);
    component.reindexDatasource(ds, true);

    expect(api.reindexDatasource).toHaveBeenNthCalledWith(1, ds.id, false);
    expect(api.reindexDatasource).toHaveBeenNthCalledWith(2, ds.id, true);
    expect(confirmSpy).toHaveBeenCalledOnce();
    confirmSpy.mockRestore();
  });

  it('loads and formats credential-redacted index status', () => {
    const {api, component, ds} = createComponent();
    const status: DatasourceIndexStatus = {
      datasource_id: ds.id,
      status: 'ready',
      source_head: 'abcdef1234567890',
      indexed_commit: 'abcdef1234567890',
      pipeline_version: null,
      repo_name: null,
      branch: 'main',
      last_attempt_at: '2026-07-11T12:00:00Z',
      last_success_at: '2026-07-11T12:00:00Z',
      last_error: null,
    };
    api.getDatasourceCatalog.mockReturnValue(of({items: [ds], next_cursor: null}));
    api.getDatasourceIndexStatus.mockReturnValue(of(status));

    component.refresh();

    expect(component.indexStatuses()[ds.id]).toEqual(status);
    expect(component.indexStatusLabel(status)).toBe(
      'datasources.table.indexReady:abcdef12',
    );
    expect(
      component.redactIndexError(
        'fetch https://token-value@example.test failed token=token-value',
      ),
    ).toBe('fetch https://***@example.test failed token=***');
  });

  it('limits management actions to the creator or an admin', () => {
    const {component, currentUser, ds} = createComponent();
    expect(component.canManage(ds)).toBe(true);
    currentUser.set({id: 'other-user', is_admin: false});
    expect(component.canManage(ds)).toBe(false);
    currentUser.set({id: 'admin', is_admin: true});
    expect(component.canManage(ds)).toBe(true);
  });
});

describe('Credentials connector', () => {
  it('creates an ENV connector without requiring a URL or description', () => {
    const {component, api} = createComponent(false);
    component.openCreateForm();
    component.formData.name = 'My API key';
    component.formData.type = 'credentials';
    component.envVars = [{key: 'API_KEY', value: 'synthetic-key'}];
    expect(component.canSave()).toBe(true);
    component.saveForm();
    expect(api.createDatasource).toHaveBeenCalledWith(expect.objectContaining({
      type: 'credentials',
      credentials: {env_vars: {API_KEY: 'synthetic-key'}},
    }));
  });

  it('keeps stored credentials on an edit without replacement values', () => {
    const {component, api} = createComponent(false);
    component.openEditForm(emailDatasource({type: 'credentials', env_var_names: ['API_KEY']}));
    expect(component.envVars).toEqual([{key: 'API_KEY', value: ''}]);
    component.formData.name = 'Renamed credentials';
    component.saveForm();
    expect(api.updateDatasource).toHaveBeenCalledWith('email-1', expect.objectContaining({
      name: 'Renamed credentials', credentials: undefined,
    }));
  });
});

describe('DatasourceListComponent email support', () => {
  it('creates a draft-tier mailbox with imap credentials and email config, no smtp', () => {
    const {api, component} = createComponent();
    component.openCreateForm();
    component.onScopeModeChange('all');
    component.formData.name = 'Personal Mailbox';
    component.onTypeSelect('email');
    component.formCredentials = {username: 'user@example.com', password: 'app-pass'};
    component.emailForm.imap_host = 'imap.example.com';
    component.emailForm.folders = 'AI, AI/Processed';
    component.emailForm.from_address = 'user@example.com';

    expect(component.canSave()).toBe(true);
    component.saveForm();

    expect(api.createDatasource).toHaveBeenCalledWith(
      expect.objectContaining({
        type: 'email',
        connection_url: undefined,
        is_global: false,
        credentials: {
          backend: 'imap_smtp',
          username: 'user@example.com',
          password: 'app-pass',
          imap: {host: 'imap.example.com', port: 993, security: 'ssl'},
        },
        config: {
          access: 'draft',
          folders: ['AI', 'AI/Processed'],
          drafts_folder: 'Drafts',
          from_address: 'user@example.com',
          recipient_allowlist: [],
          unattended_send: false,
        },
      }),
    );
  });

  it('includes the smtp block only at the send tier', () => {
    const {api, component} = createComponent();
    component.openCreateForm();
    component.formData.name = 'Sender Mailbox';
    component.onTypeSelect('email');
    component.formCredentials = {username: 'user@example.com', password: 'app-pass'};
    component.onEmailProviderSelect('yahoo');
    component.onEmailAccessChange('send');
    component.emailForm.folders = 'AI';
    component.emailForm.unattended_send = true;

    component.saveForm();

    const payload = api.createDatasource.mock.calls[0][0];
    expect(payload.credentials.imap).toEqual({
      host: 'imap.mail.yahoo.com', port: 993, security: 'ssl',
    });
    expect(payload.credentials.smtp).toEqual({
      host: 'smtp.mail.yahoo.com', port: 465, security: 'ssl',
    });
    expect(payload.config).toEqual(
      expect.objectContaining({access: 'send', folders: ['AI'], unattended_send: true}),
    );
  });

  it('requires username, app password, and imap host to create', () => {
    const {component} = createComponent();
    component.openCreateForm();
    component.onScopeModeChange('all');
    component.formData.name = 'Mailbox';
    component.onTypeSelect('email');
    expect(component.canSave()).toBe(false);
    component.formCredentials = {username: 'u@e.com', password: 'pw'};
    expect(component.canSave()).toBe(false);
    component.emailForm.imap_host = 'imap.e.com';
    expect(component.canSave()).toBe(true);
    // Send tier additionally needs an SMTP host.
    component.onEmailAccessChange('send');
    expect(component.canSave()).toBe(false);
    component.emailForm.smtp_host = 'smtp.e.com';
    expect(component.canSave()).toBe(true);
  });

  it('round-trips email config on edit and keeps blank credentials untouched', () => {
    const {api, component} = createComponent();
    const ds = emailDatasource();
    component.openEditForm(ds);

    expect(component.emailForm.access).toBe('read_write');
    expect(component.emailForm.folders).toBe('AI, AI/Processed');
    expect(component.emailForm.drafts_folder).toBe('Entwürfe');
    expect(component.emailForm.from_address).toBe('user@example.com');
    expect(component.emailForm.recipient_allowlist).toBe('@example.org');
    expect(component.emailForm.unattended_send).toBe(false);
    expect(component.canSave()).toBe(true);

    component.saveForm();

    expect(api.updateDatasource).toHaveBeenCalledWith(
      ds.id,
      expect.objectContaining({
        credentials: undefined,
        config: {
          access: 'read_write',
          folders: ['AI', 'AI/Processed'],
          drafts_folder: 'Entwürfe',
          from_address: 'user@example.com',
          recipient_allowlist: ['@example.org'],
          unattended_send: false,
        },
      }),
    );
  });

  it('prefills host/port/security from a provider preset', () => {
    const {component} = createComponent();
    component.openCreateForm();
    component.onTypeSelect('email');
    component.onEmailProviderSelect('gmail');
    expect(component.emailForm.imap_host).toBe('imap.gmail.com');
    expect(component.emailForm.imap_port).toBe('993');
    expect(component.emailForm.imap_security).toBe('ssl');
    expect(component.emailForm.smtp_host).toBe('smtp.gmail.com');
    expect(component.emailForm.smtp_port).toBe('587');
    expect(component.emailForm.smtp_security).toBe('starttls');
  });
});

describe('DatasourceListComponent MCP support', () => {
  it('offers the MCP type option', () => {
    const {component} = createComponent();

    expect(component.typeFilters).toContainEqual({
      labelKey: 'datasources.filter.mcp',
      value: 'mcp',
    });
  });

  it('builds http credentials with bearer auth on save', () => {
    const {api, component} = createComponent();
    component.openCreateForm();
    component.formData.name = 'Remote tools';
    component.onTypeSelect('mcp');
    component.formData.connection_url = 'https://mcp.example.com/mcp';
    component.formData.mcpTransport = 'http';
    component.formData.mcpToken = 'secret-token';

    component.saveForm();

    expect(api.createDatasource).toHaveBeenCalledWith(
      expect.objectContaining({
        type: 'mcp',
        connection_url: 'https://mcp.example.com/mcp',
        credentials: {
          transport: 'http',
          auth: {type: 'bearer', token: 'secret-token'},
        },
      }),
    );
  });

  it('builds stdio credentials with args split per line', () => {
    const {api, component} = createComponent();
    component.openCreateForm();
    component.formData.name = 'GitHub tools';
    component.onTypeSelect('mcp');
    component.formData.mcpTransport = 'stdio';
    component.formData.mcpCommand = 'npx';
    component.formData.mcpArgs =
      '-y\n@modelcontextprotocol/server-github\n\n';

    component.saveForm();

    expect(api.createDatasource).toHaveBeenCalledWith(
      expect.objectContaining({
        type: 'mcp',
        connection_url: undefined,
        credentials: {
          transport: 'stdio',
          command: 'npx',
          args: ['-y', '@modelcontextprotocol/server-github'],
          env: {},
        },
      }),
    );
  });
});

describe('DatasourceListComponent publish confirmation tiers', () => {
  it('needs no confirmation for a private save', () => {
    const {component} = createComponent();
    component.openCreateForm();
    component.formData.name = 'Mine';
    expect(component.publishConfirmTier()).toBeNull();
  });

  it('warns on a read-only publish (create)', () => {
    const {component} = createComponent();
    component.openCreateForm();
    component.formData.name = 'Org Wiki';
    component.formData.is_global = true;
    expect(component.publishConfirmTier()).toBe('warn');
  });

  it('requires the typed name on a read-write publish (create)', () => {
    const {component} = createComponent();
    component.openCreateForm();
    component.formData.name = 'Org Wiki';
    component.formData.is_global = true;
    component.formData.read_only = false;
    expect(component.publishConfirmTier()).toBe('name');
  });

  it('requires the typed name on a public RO→RW flip (edit)', () => {
    const {component, ds} = createComponent();
    component.openEditForm({...ds, is_global: true, read_only: true});
    component.formData.read_only = false;
    // kb locks to read-only; use a repository-shaped edit for the flip.
    component.formData.type = 'repository';
    expect(component.publishConfirmTier()).toBe('name');
  });

  it('needs no confirmation for unpublish or RW→RO', () => {
    const {component, ds} = createComponent();
    component.openEditForm({
      ...ds,
      type: 'repository',
      is_global: true,
      read_only: false,
    });
    component.formData.is_global = false;
    expect(component.publishConfirmTier()).toBeNull();

    component.openEditForm({
      ...ds,
      type: 'repository',
      is_global: true,
      read_only: false,
    });
    component.formData.read_only = true;
    expect(component.publishConfirmTier()).toBeNull();
  });

  it('stays silent when an already-public RW datasource is edited unchanged', () => {
    const {component, ds} = createComponent();
    component.openEditForm({
      ...ds,
      type: 'repository',
      is_global: true,
      read_only: false,
    });
    expect(component.publishConfirmTier()).toBeNull();
  });

  it('sends is_global and read_only in the create payload', () => {
    const {api, component} = createComponent();
    component.openCreateForm();
    component.formData.name = 'Org Wiki';
    component.formData.type = 'repository';
    component.formData.is_global = true;
    component.gitAuthMethod = 'token';
    component.formCredentials.password = 'tok';
    component.doSave();
    const payload = api.createDatasource.mock.calls[0][0];
    expect(payload.is_global).toBe(true);
    expect(payload.read_only).toBe(true);
  });

  it('forces read_only=true for kb in the payload', () => {
    const {api, component} = createComponent();
    component.openCreateForm();
    component.formData.name = 'Org KB';
    component.formData.type = 'kb';
    component.formData.is_global = true;
    component.formData.read_only = false; // UI forbids this; belt-and-braces
    component.doSave();
    expect(api.createDatasource.mock.calls[0][0].read_only).toBe(true);
  });

  it('openEditForm seeds visibility from the datasource', () => {
    const {component, ds} = createComponent();
    component.openEditForm({...ds, is_global: true, read_only: false});
    expect(component.formData.is_global).toBe(true);
    expect(component.formData.read_only).toBe(false);
  });

  it('clears is_global when switching the type to email', () => {
    const {component} = createComponent();
    component.openCreateForm();
    component.formData.is_global = true;
    component.onTypeSelect('email');
    expect(component.formData.is_global).toBe(false);
  });

  it('saveForm defers to the dialog when confirmation is needed', () => {
    const {api, component} = createComponent();
    component.openCreateForm();
    component.formData.name = 'Org Wiki';
    component.formData.type = 'repository';
    component.formData.is_global = true;
    component.gitAuthMethod = 'token';
    component.formCredentials.password = 'tok';

    component.saveForm();
    expect(api.createDatasource).not.toHaveBeenCalled();
    expect(component.showPublishConfirm()).toBe(true);
    expect(component.publishConfirmName()).toBeNull(); // warn tier

    component.onPublishConfirmed();
    expect(api.createDatasource).toHaveBeenCalledOnce();
  });
});

describe('DatasourceListComponent repository forge selection', () => {
  it('leaves forge blank and blocks save for a self-hosted host', () => {
    const {component} = createComponent();
    component.openCreateForm();
    component.formData.name = 'Widget';
    component.formData.type = 'repository';
    component.gitAuthMethod = 'token';
    component.formCredentials.password = 'tok';

    component.onConnectionUrlChange('https://git.example.com/acme/widget');

    expect(component.formData.forge).toBe('');
    expect(component.canSave()).toBe(false);
  });

  it('defaults forge to github for a github.com URL', () => {
    const {component} = createComponent();
    component.openCreateForm();
    component.formData.type = 'repository';

    component.onConnectionUrlChange('https://github.com/acme/widget');

    expect(component.formData.forge).toBe('github');
  });

  it('defaults forge to gitlab for a gitlab.com URL', () => {
    const {component} = createComponent();
    component.openCreateForm();
    component.formData.type = 'repository';

    component.onConnectionUrlChange('https://gitlab.com/acme/widget');

    expect(component.formData.forge).toBe('gitlab');
  });

  it('includes the chosen forge in the create payload config', () => {
    const {api, component} = createComponent();
    component.openCreateForm();
    component.formData.name = 'Widget';
    component.formData.type = 'repository';
    component.onConnectionUrlChange('https://github.com/acme/widget');
    component.gitAuthMethod = 'token';
    component.formCredentials.password = 'tok';

    component.doSave();

    const payload = api.createDatasource.mock.calls[0][0];
    expect(payload.config).toEqual({forge: 'github'});
  });

  it('lets an explicit forge selection override a self-hosted host', () => {
    const {api, component} = createComponent();
    component.openCreateForm();
    component.onScopeModeChange('all');
    component.formData.name = 'Widget';
    component.formData.type = 'repository';
    component.onConnectionUrlChange('https://git.example.com/acme/widget');
    component.onForgeSelect('gitea');
    component.gitAuthMethod = 'token';
    component.formCredentials.password = 'tok';

    expect(component.canSave()).toBe(true);
    component.doSave();

    const payload = api.createDatasource.mock.calls[0][0];
    expect(payload.config).toEqual({forge: 'gitea'});
  });

  it('keeps an explicit forge choice through further URL edits', () => {
    const {component} = createComponent();
    component.openCreateForm();
    component.onScopeModeChange('all');
    component.formData.name = 'Widget';
    component.formData.type = 'repository';
    component.onForgeSelect('gitea');

    // The bug: every keystroke on the URL re-inferred forge from the host
    // and clobbered the explicit pick back to '', re-disabling Save.
    component.onConnectionUrlChange('https://git.example.com/acme/w');
    component.onConnectionUrlChange('https://git.example.com/acme/wi');
    component.onConnectionUrlChange('https://git.example.com/acme/widget');

    expect(component.formData.forge).toBe('gitea');
    expect(component.canSave()).toBe(true);
  });

  it('round-trips the stored forge into the edit form', () => {
    const {component, ds} = createComponent();
    component.openEditForm({
      ...ds,
      type: 'repository',
      connection_url: 'https://git.example.com/acme/widget',
      config: {forge: 'gitea'},
    });

    expect(component.formData.forge).toBe('gitea');
  });

  it('includes the chosen forge in the update payload config', () => {
    const {api, component, ds} = createComponent();
    component.openEditForm({
      ...ds,
      type: 'repository',
      connection_url: 'https://git.example.com/acme/widget',
      config: {forge: 'gitea'},
    });
    component.onForgeSelect('github');
    component.gitAuthMethod = 'token';

    component.saveForm();

    expect(api.updateDatasource).toHaveBeenCalledWith(
      ds.id,
      expect.objectContaining({config: {forge: 'github'}}),
    );
  });
});

describe('DatasourceListComponent SSH connector host keys', () => {
  const hostKey = 'ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GKJl';

  it('sends an ssh_key connector its host, user, port and pin', () => {
    const {api, component} = createComponent();
    component.openCreateForm();
    component.onScopeModeChange('all');
    component.formData.name = 'Bastion';
    component.formData.type = 'ssh_key';
    component.gitSshKey = '-----BEGIN OPENSSH PRIVATE KEY-----\nAAAA\n-----END OPENSSH PRIVATE KEY-----';
    component.sshHost = ' bastion.example.com ';
    component.sshUser = 'deploy';
    component.sshPort = '2200';
    component.sshKnownHosts = hostKey;

    component.doSave();

    expect(api.createDatasource.mock.calls[0][0].config).toEqual({
      host: 'bastion.example.com',
      user: 'deploy',
      port: 2200,
      known_hosts: hostKey,
    });
  });

  it('round-trips the endpoint on edit and clears it when the host is removed', () => {
    const {api, component, ds} = createComponent();
    component.openEditForm({
      ...ds,
      type: 'ssh_key',
      connection_url: null,
      config: {host: 'bastion.example.com', port: 2200, known_hosts: hostKey},
    });
    expect(component.sshHost).toBe('bastion.example.com');
    expect(component.sshPort).toBe('2200');
    expect(component.sshKnownHosts).toBe(hostKey);

    component.sshHost = '';
    component.saveForm();

    expect(api.updateDatasource).toHaveBeenCalledWith(
      ds.id,
      expect.objectContaining({config: {}}),
    );
  });

  it('edits the host, tests the edited host, and pins what Test reports', () => {
    const {api, component, ds} = createComponent();
    component.openEditForm({
      ...ds,
      type: 'ssh_key',
      connection_url: null,
      config: {host: 'bastion.example.com', known_hosts: `bastion.example.com ${hostKey}`},
    });
    expect(component.formTestAvailable()).toBe(true);

    // The old pin named the old host: editing the host drops it.
    component.onSshEndpointChange('host', 'new-bastion.example.com');
    component.onSshEndpointChange('port', '2200');
    expect(component.sshKnownHosts).toBe('');

    const reported = `[new-bastion.example.com]:2200 ${hostKey}`;
    api.testDatasource.mockReturnValue(
      of({status: 'ok', message: 'Reached', details: {host_key: reported}}),
    );
    component.testFromForm();

    // Test reached the endpoint being typed, not the saved one.
    expect(api.testDatasource).toHaveBeenCalledWith(ds.id, {
      config: {host: 'new-bastion.example.com', port: 2200},
    });
    expect(component.testedHostKeyToPin()).toBe(reported);
    component.pinTestedHostKey(component.testedHostKeyToPin()!);
    expect(component.testedHostKeyToPin()).toBeNull();

    component.saveForm();
    expect(api.updateDatasource).toHaveBeenCalledWith(
      ds.id,
      expect.objectContaining({
        config: {host: 'new-bastion.example.com', port: 2200, known_hosts: reported},
      }),
    );
  });

  it('offers Test for SSH-key repositories and sends the edited URL', () => {
    const {api, component, ds} = createComponent();
    component.openEditForm({
      ...ds,
      type: 'repository',
      connection_url: 'ssh://git@git.example.com:2222/acme/widget.git',
      cli_hint: 'git over ssh',
      config: {forge: 'gitea', known_hosts: `[git.example.com]:2222 ${hostKey}`},
    });
    expect(component.gitAuthMethod).toBe('ssh');
    expect(component.formTestAvailable()).toBe(true);

    component.onConnectionUrlChange('ssh://git@git2.example.com:2222/acme/widget.git');
    expect(component.sshKnownHosts).toBe('');
    component.onForgeSelect('gitea');
    component.testFromForm();
    expect(api.testDatasource).toHaveBeenCalledWith(ds.id, {
      connection_url: 'ssh://git@git2.example.com:2222/acme/widget.git',
      config: {forge: 'gitea'},
    });

    component.gitAuthMethod = 'token';
    expect(component.formTestAvailable()).toBe(false);
  });

  it('offers no Test for an ssh_key without a host', () => {
    const {component, ds} = createComponent();
    component.openEditForm({...ds, type: 'ssh_key', connection_url: null, config: {}});
    expect(component.formTestAvailable()).toBe(false);
  });

  it.each([
    ['ssh', {forge: 'github', known_hosts: hostKey}],
    ['token', {forge: 'github'}],
  ] as const)('sends the pin only with an SSH-key repository (%s)', (auth, config) => {
    const {api, component} = createComponent();
    component.openCreateForm();
    component.onScopeModeChange('all');
    component.formData.name = 'Widget';
    component.formData.type = 'repository';
    component.onConnectionUrlChange('https://github.com/acme/widget');
    component.sshKnownHosts = hostKey;
    component.gitAuthMethod = auth;
    component.gitSshKey = 'key';
    component.formCredentials.password = 'tok';

    component.doSave();

    expect(api.createDatasource.mock.calls[0][0].config).toEqual(config);
  });

  it('names every new string in the catalogue', () => {
    const form = (en as {datasources: {form: Record<string, string>}}).datasources.form;
    for (const key of [
      'sshHostLabel',
      'sshUserLabel',
      'sshPortLabel',
      'sshHostHint',
      'sshKnownHostsLabel',
      'sshKnownHostsHint',
      'sshPinHostKey',
      'sshAgentNoticeTitle',
      'sshAgentNotice',
    ]) {
      expect(form[key], key).toBeTruthy();
    }
  });
});

describe('DatasourceListComponent availability policy', () => {
  it('passes the selected project through the server-side catalog filter', () => {
    const {api, component} = createComponent();

    component.onCatalogProjectFilter('project-a');

    expect(api.getDatasourceCatalog).toHaveBeenCalledWith(
      expect.objectContaining({project_id: 'project-a'}),
    );
  });

  it('searches and paginates authorized catalog project choices', () => {
    const {api, component} = createComponent();
    const first = {
      id: 'project-a', name: 'Application', user_role: 'owner',
      addable: true, retained_only: false, linked: false,
    } as const;
    const second = {...first, id: 'project-b', name: 'Backend'};
    api.getLinkableDatasourceProjects
      .mockReturnValueOnce(of({items: [first], next_cursor: 'next'}))
      .mockReturnValueOnce(of({items: [second], next_cursor: null}));
    component.catalogProjectSearch.set('app');

    component.loadCatalogProjects(true);
    component.loadMoreCatalogProjects();

    expect(api.getLinkableDatasourceProjects).toHaveBeenNthCalledWith(1, {
      q: 'app', cursor: undefined, limit: 50,
    });
    expect(api.getLinkableDatasourceProjects).toHaveBeenNthCalledWith(2, {
      q: 'app', cursor: 'next', limit: 50,
    });
    expect(component.catalogProjects().map(project => project.id))
      .toEqual(['project-a', 'project-b']);
  });

  it('preselects an authorized route project outside the first target page', () => {
    const {api, component} = createComponent(true, 'project-99');
    api.getLinkableDatasourceProjects.mockReturnValue(of({items: [], next_cursor: 'next'}));
    api.getProject.mockReturnValue(of({
      id: 'project-99', name: 'Distant Project', status: 'active', is_default: false,
      created_at: '', updated_at: '',
    }));
    api.getProjectMembers.mockReturnValue(of([{
      project_id: 'project-99', user_id: 'user-1', role: 'owner', joined_at: '',
    }]));

    component.openCreateForm();

    expect(component.formData.scope_mode).toBe('projects');
    expect(component.formProjectIds()).toEqual(new Set(['project-99']));
    expect(component.scopeTargets().map(project => project.id)).toContain('project-99');
  });

  it('uses legacy connector listing while the policy capability is unavailable', () => {
    const {api, component, ds} = createComponent(false);
    api.getDatasources.mockReturnValue(of([ds]));

    component.refresh();

    expect(api.getDatasources).toHaveBeenCalledWith(undefined, undefined);
    expect(api.getDatasourceCatalog).not.toHaveBeenCalled();
    expect(component.datasources()).toEqual([ds]);
  });

  it('keeps legacy create available without loading or writing policy fields', () => {
    const {api, component} = createComponent(false);
    component.openCreateForm();
    component.formData.name = 'CLI docs';
    component.formData.type = 'generic';
    component.formData.description = 'CLI docs';

    expect(api.getLinkableDatasourceProjects).not.toHaveBeenCalled();
    expect(component.canSave()).toBe(true);
    component.saveForm();

    const payload = api.createDatasource.mock.calls[0][0];
    expect(payload).not.toHaveProperty('scope_mode');
    expect(payload).not.toHaveProperty('project_ids');
    expect(payload).not.toHaveProperty('auto_attach');
  });

  it('does not write an existing policy through the legacy edit form', () => {
    const {api, component, ds} = createComponent(false);
    component.openEditForm({
      ...ds,
      scope_mode: 'projects',
      project_ids: ['project-a'],
      auto_attach: true,
      policy_revision: 9,
    });

    expect(api.getLinkableDatasourceProjects).not.toHaveBeenCalled();
    component.doSave();

    const payload = api.updateDatasource.mock.calls[0][1];
    expect(payload).not.toHaveProperty('scope_mode');
    expect(payload).not.toHaveProperty('project_ids');
    expect(payload).not.toHaveProperty('auto_attach');
    expect(payload).not.toHaveProperty('policy_revision');
  });

  it('preserves an unchanged redacted connection URL and sends a deliberate replacement', () => {
    const {api, component, ds} = createComponent();
    api.updateDatasource.mockReturnValue(of(null));
    const redacted = {
      ...ds,
      type: 'postgresql' as const,
      connection_url: null,
      connection_url_redacted: true,
    };
    component.openEditForm(redacted);

    expect(component.canSave()).toBe(true);
    component.doSave();
    expect(api.updateDatasource.mock.calls[0][1]).not.toHaveProperty('connection_url');

    component.onConnectionUrlChange('postgresql://replacement.example/app');
    component.doSave();
    expect(api.updateDatasource.mock.calls[1][1].connection_url)
      .toBe('postgresql://replacement.example/app');
  });

  it('requires a deliberate scope choice and at least one project in projects mode', () => {
    const {component} = createComponent();
    component.openCreateForm();
    component.formData.name = 'CLI docs';
    component.formData.type = 'generic';
    component.formData.description = 'CLI docs';

    expect(component.canSave()).toBe(false);
    component.onScopeModeChange('projects');
    expect(component.canSave()).toBe(false);
    component.formProjectIds.set(new Set(['project-1']));
    expect(component.canSave()).toBe(true);
  });

  it('creates a project-scoped automatic connector with an explicit full project set', () => {
    const {api, component} = createComponent();
    component.openCreateForm();
    component.formData.name = 'Application DB';
    component.formData.type = 'postgresql';
    component.formData.connection_url = 'postgresql://db/app';
    component.onScopeModeChange('projects');
    component.formData.auto_attach = true;
    component.formProjectIds.set(new Set(['project-a', 'project-b']));

    component.doSave();

    expect(api.createDatasource).toHaveBeenCalledWith(expect.objectContaining({
      scope_mode: 'projects',
      project_ids: ['project-a', 'project-b'],
      auto_attach: true,
    }));
  });

  it('sends the loaded policy revision and preserves links when switching to all mode', () => {
    const {api, component, ds} = createComponent();
    component.openEditForm({
      ...ds,
      scope_mode: 'projects',
      auto_attach: true,
      policy_revision: 7,
      project_ids: ['project-a'],
    });
    component.onScopeModeChange('all');

    component.doSave();

    expect(api.updateDatasource).toHaveBeenCalledWith(
      ds.id,
      expect.objectContaining({
        scope_mode: 'all',
        auto_attach: true,
        policy_revision: 7,
        project_ids: undefined,
      }),
    );
  });

  it('distinguishes visibility from availability labels', () => {
    const {component, ds} = createComponent();
    expect(component.scopeLabelKey({...ds, is_global: true})).toBe(
      'datasources.table.scopeGlobal',
    );
    expect(component.availabilityLabel({...ds, scope_mode: 'projects', project_count: 2}))
      .toBe('datasources.table.availabilityProjects');
    expect(component.availabilityLabel({...ds, scope_mode: 'projects', project_count: 0}))
      .toBe('datasources.table.availabilityNone');
  });

  it('does not expose a partial project count for a shared connector', () => {
    const {component, currentUser, ds} = createComponent();
    currentUser.set({id: 'other-user', is_admin: false});

    expect(component.availabilityLabel({
      ...ds,
      scope_mode: 'projects',
      project_ids: ['visible-project'],
    })).toBe('datasources.table.availabilityScoped');
  });

  it('keeps selected projects removable when search or paging hides their row', () => {
    const {component} = createComponent();
    component.openCreateForm();
    component.scopeTargets.set([{
      id: 'project-a',
      name: 'Application',
      user_role: 'owner',
      addable: true,
      retained_only: false,
      linked: false,
    }]);
    component.formProjectIds.set(new Set(['project-a', 'project-off-page']));

    expect(component.selectedScopeTargets().map(project => project.name)).toEqual([
      'Application',
      'project-off-page',
    ]);
    component.toggleScopeProject(component.selectedScopeTargets()[1]);
    expect(component.formProjectIds()).toEqual(new Set(['project-a']));
  });

  it('merges unpaginated retained links into the project picker', () => {
    const {api, component, ds} = createComponent();
    api.getLinkableDatasourceProjects.mockReturnValue(of({
      items: [{
        id: 'project-a',
        name: 'Application',
        user_role: 'owner',
        addable: true,
        retained_only: false,
        linked: false,
      }],
      selected_items: [{
        id: 'project-retained',
        name: 'Former team',
        user_role: null,
        addable: false,
        retained_only: true,
        linked: true,
      }],
      next_cursor: null,
    }));

    component.openEditForm({
      ...ds,
      scope_mode: 'projects',
      project_ids: [],
      policy_revision: 3,
    });

    expect(component.formProjectIds()).toEqual(new Set(['project-retained']));
    expect(component.scopeTargets().map(project => project.name)).toEqual([
      'Former team',
      'Application',
    ]);
  });

  it('does not restore a deliberately removed final link on a search refresh', () => {
    const {api, component, ds} = createComponent();
    const retained = {
      id: 'project-retained',
      name: 'Former team',
      user_role: null,
      addable: false,
      retained_only: true,
      linked: true,
    } as const;
    api.getLinkableDatasourceProjects.mockReturnValue(of({
      items: [],
      selected_items: [retained],
      next_cursor: null,
    }));
    const confirmSpy = vi.spyOn(globalThis, 'confirm').mockReturnValue(true);
    component.openEditForm({
      ...ds,
      scope_mode: 'projects',
      project_ids: [retained.id],
      policy_revision: 3,
    });

    component.toggleScopeProject(retained);
    component.loadScopeTargets(true);

    expect(component.formProjectIds()).toEqual(new Set());
    confirmSpy.mockRestore();
  });

  it('renders native project knowledge policy as managed and omits policy writes', () => {
    const {api, component, ds} = createComponent();
    component.openEditForm({
      ...ds,
      config: {root_path: 'vault', native_project_id: 'project-a'},
      scope_mode: 'projects',
      project_ids: ['project-a'],
      auto_attach: true,
      policy_revision: 4,
    });

    expect(component.isNativeProjectConnector()).toBe(true);
    expect(component.canSave()).toBe(true);
    expect(api.getLinkableDatasourceProjects).not.toHaveBeenCalled();
    component.doSave();

    const payload = api.updateDatasource.mock.calls[0][1];
    expect(payload).not.toHaveProperty('scope_mode');
    expect(payload).not.toHaveProperty('project_ids');
    expect(payload).not.toHaveProperty('auto_attach');
    expect(payload).not.toHaveProperty('policy_revision');
  });

  it('retains the returned policy revision after create-then-test', () => {
    const {api, component, ds} = createComponent();
    api.createDatasource.mockReturnValue(of({
      ...ds,
      scope_mode: 'all',
      project_ids: [],
      auto_attach: true,
      policy_revision: 6,
    }));
    component.openCreateForm();
    component.formData.name = 'CLI docs';
    component.formData.type = 'generic';
    component.formData.description = 'CLI docs';
    component.onScopeModeChange('all');
    component.formData.auto_attach = true;

    component.testFromForm();
    component.doSave();

    expect(api.updateDatasource).toHaveBeenCalledWith(
      ds.id,
      expect.objectContaining({policy_revision: 6, auto_attach: true}),
    );
  });
});


describe('DatasourceListComponent token permission guidance', () => {
  // A PAT with the wrong permissions fails at first use, in an agent run,
  // with a forge error nobody sees. The form is where that is cheap to fix.
  it('names the permissions a knowledge base token needs', () => {
    const {component} = createComponent();
    component.openCreateForm();
    component.formData.type = 'kb';
    component.gitAuthMethod = 'token';

    expect(component.patScopeHintKey()).toBe('datasources.form.patScopesKb');
    expect(en.datasources.form.patScopesKb).toBeTruthy();
  });

  it('names the permissions a repository token needs', () => {
    const {component} = createComponent();
    component.openCreateForm();
    component.formData.type = 'repository';
    component.gitAuthMethod = 'token';

    expect(component.patScopeHintKey()).toBe('datasources.form.patScopesRepo');
    expect(en.datasources.form.patScopesRepo).toBeTruthy();
  });

  it('stays quiet where no token is being collected', () => {
    const {component} = createComponent();
    component.openCreateForm();
    component.formData.type = 'kb';
    component.gitAuthMethod = 'ssh';
    expect(component.patScopeHintKey()).toBe('');

    component.formData.type = 'postgresql';
    component.gitAuthMethod = 'token';
    expect(component.patScopeHintKey()).toBe('');
  });
});

describe('DatasourceListComponent Test connection results', () => {
  it('shows a type without a test as neither a pass nor a failure', () => {
    const {component} = createComponent();

    expect(component.testResultIcon({status: 'ok', message: 'Connected'})).toBe('check_circle');
    expect(component.testResultIcon({status: 'error', message: 'refused'})).toBe('error');
    expect(
      component.testResultIcon({status: 'unsupported', message: 'no connection test'}),
    ).toBe('info');
  });

  it('keeps an unsupported answer as it came from the API', () => {
    const {api, component} = createComponent();
    const answer = {
      status: 'unsupported' as const,
      message: 'Kubeconfig connectors have no connection test',
    };
    api.testDatasource.mockReturnValue(of(answer));

    component.testDatasource('kube-1');

    expect(component.testResults()['kube-1']).toEqual(answer);
  });
});

describe('DatasourceListComponent access levels from the capability matrix', () => {
  function create(type: Datasource['type'], drivers: ConnectorDriver[] | null = BUILTIN_DRIVERS) {
    const created = createComponent(false, null, drivers);
    created.component.openCreateForm();
    created.component.formData.name = 'Org connector';
    created.component.onTypeSelect(type);
    created.component.formData.is_global = true;
    return created;
  }

  it('offers both levels for a driver that has both, and says the flag is only declared', () => {
    const {component} = create('postgresql');
    expect(component.offersReadOnly()).toBe(true);
    expect(component.offersReadWrite()).toBe(true);
    // A public connector's read-only binds nothing: no "enforced by" claim,
    // the old advice to scope the credentials instead.
    expect(component.publicHintKey()).toBe('datasources.form.visibilityCredentialHint');
    expect('publicAccessLevel' in component).toBe(false);
    component.formData.read_only = false;
    expect(component.publicReadOnly()).toBe(false);
  });

  it('keeps the credential hint for every driver that is not forced read-only', () => {
    for (const type of ['generic', 'repository', 'neo4j', 'mongodb', 'webdav', 'mcp', 'kubeconfig', 'ssh_key'] as const) {
      expect(create(type).component.publicHintKey()).toBe('datasources.form.visibilityCredentialHint');
    }
    expect(create('kb').component.publicHintKey()).toBe('datasources.form.visibilityKbHint');
  });

  it('hides read-write for a driver forced read-only and saves it read-only', () => {
    const {api, component} = create('kb');
    expect(component.offersReadOnly()).toBe(true);
    expect(component.offersReadWrite()).toBe(false);
    component.formData.read_only = false;
    expect(component.publicReadOnly()).toBe(true);
    expect(component.publishConfirmTier()).toBe('warn');
    component.doSave();
    expect(api.createDatasource.mock.calls[0][0].read_only).toBe(true);
  });

  it('hides read-only for a driver with one level and saves what it binds', () => {
    const {api, component} = create('mcp');
    expect(component.offersReadOnly()).toBe(false);
    expect(component.offersReadWrite()).toBe(true);
    // The form's default is read-only; an MCP server is bound read-write.
    expect(component.formData.read_only).toBe(true);
    expect(component.publicReadOnly()).toBe(false);
    expect(component.publishConfirmTier()).toBe('name');
    component.formData.mcpTransport = 'http';
    component.formData.connection_url = 'https://mcp.example.com';
    component.doSave();
    expect(api.createDatasource.mock.calls[0][0].read_only).toBe(false);
  });

  it('edits a stored public MCP row without the read-write gate, though its flag is fixed up', () => {
    const {api, component} = createComponent(false, null, BUILTIN_DRIVERS);
    component.openEditForm({
      ...kbDatasource(),
      id: 'mcp-1',
      type: 'mcp',
      connection_url: 'https://mcp.example.com',
      config: {},
      is_global: true,
      read_only: true,
    });
    component.formData.description = 'Only the description changed';
    expect(component.publishConfirmTier()).toBeNull();
    component.saveForm();
    expect(component.showPublishConfirm()).toBe(false);
    expect(api.updateDatasource.mock.calls[0][1].read_only).toBe(false);
  });

  it('badges a listed public row by what its driver binds, not a contradicting flag', () => {
    const {component} = createComponent(false, null, BUILTIN_DRIVERS);
    const row = (type: Datasource['type'], readOnly: boolean) => ({
      ...kbDatasource(), type, is_global: true, read_only: readOnly,
    });
    expect(component.isPublicReadWrite(row('mcp', true))).toBe(true);
    expect(component.isPublicReadWrite(row('kb', false))).toBe(false);
    expect(component.isPublicReadWrite(row('postgresql', true))).toBe(false);
    expect(component.isPublicReadWrite(row('postgresql', false))).toBe(true);
  });

  it('keeps the pre-matrix rule until the matrix loads', () => {
    const kb = create('kb', null).component;
    expect(kb.offersReadWrite()).toBe(false);
    expect(kb.publicHintKey()).toBe('datasources.form.visibilityKbHint');
    const mcp = create('mcp', null).component;
    expect(mcp.offersReadOnly()).toBe(true);
    expect(mcp.offersReadWrite()).toBe(true);
    expect(mcp.isPublicReadWrite({...kbDatasource(), type: 'mcp', read_only: true})).toBe(false);
  });
});

describe('DatasourceListComponent bespoke and generic forms', () => {
  const IMAGE_DRIVER: ConnectorDriver = {
    ...BUILTIN_DRIVERS.find((driver) => driver.name === 'srw.generic/v1')!,
    name: 'community.ticketing/v1',
    legacy_type: 'generic',
  };

  it('uses the bespoke section of every built-in, and the generic form only in the dev preview', () => {
    const {component} = createComponent(false, null, BUILTIN_DRIVERS);
    component.openCreateForm();
    component.onTypeSelect('postgresql');
    expect(component.hasBespokeForm()).toBe(true);
    expect(component.useGenericForm()).toBe(false);
    component.setGenericFormPreview(true);
    expect(component.useGenericForm()).toBe(true);
    component.closeForm();
    expect(component.genericFormPreview()).toBe(false);
  });

  it('keeps the bespoke form while the matrix is unknown', () => {
    const {component} = createComponent(false, null, null);
    component.openCreateForm();
    component.setGenericFormPreview(true);
    expect(component.useGenericForm()).toBe(false);
  });

  it('gives a driver without a bespoke form the generic form', () => {
    const {component} = createComponent(false, null, [IMAGE_DRIVER]);
    component.openCreateForm();
    component.onTypeSelect('generic');
    expect(component.hasBespokeForm()).toBe(false);
    expect(component.useGenericForm()).toBe(true);
  });

  it('submits what the generic form holds, and lets the API decide', () => {
    const {api, component} = createComponent(false, null, BUILTIN_DRIVERS);
    component.openCreateForm();
    component.formData.name = 'Graph';
    component.onTypeSelect('neo4j');
    component.setGenericFormPreview(true);
    expect(component.canSave()).toBe(false);
    component.genericFormValue.set({
      connection_url: 'bolt://graph:7687',
      credentials: {username: 'neo', password: 'pw'},
      problems: [],
    });
    expect(component.canSave()).toBe(true);
    component.doSave();
    expect(api.createDatasource).toHaveBeenCalledWith(
      expect.objectContaining({
        type: 'neo4j',
        connection_url: 'bolt://graph:7687',
        credentials: {username: 'neo', password: 'pw'},
        config: undefined,
      }),
    );
  });

  it("shows the API's refusal in the generic form, at the field it names", () => {
    const {api, component} = createComponent(false, null, BUILTIN_DRIVERS);
    api.createDatasource.mockReturnValue(
      throwError(() => ({error: {detail: {message: 'bad host', field: '/config/host'}}})),
    );
    component.openCreateForm();
    component.formData.name = 'Box';
    component.onTypeSelect('ssh_key');
    component.setGenericFormPreview(true);
    component.genericFormValue.set({config: {host: '-oProxyCommand=x'}, problems: []});
    component.doSave();
    expect(component.genericFormError()).toEqual({message: 'bad host', field: '/config/host'});
    expect(component.errorMessage()).toBeNull();
  });

  it('keeps the bespoke error banner outside the generic form', () => {
    const {api, component} = createComponent(false, null, BUILTIN_DRIVERS);
    api.createDatasource.mockReturnValue(throwError(() => ({error: {detail: 'refused'}})));
    component.openCreateForm();
    component.formData.name = 'Graph';
    component.onTypeSelect('neo4j');
    component.formData.connection_url = 'bolt://graph:7687';
    component.doSave();
    expect(component.errorMessage()).toBe('refused');
    expect(component.genericFormError()).toBeNull();
  });

  it('prefills the generic form on an edit from config and URL only', () => {
    const {component} = createComponent(false, null, BUILTIN_DRIVERS);
    const email = emailDatasource();
    component.openEditForm(email);
    expect(component.genericFormExisting).toEqual({
      connection_url: null,
      connection_url_redacted: undefined,
      config: email.config,
    });
  });
});
