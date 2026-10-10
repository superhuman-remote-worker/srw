import {describe, expect, it} from 'vitest';
import {
  activeDatasourceIds,
  allDatasourcesSelected,
  datasourceSelectionDifferenceCount,
  datasourceSetKey,
  isRepositoryDatasource,
  requiresShellWorkspace,
  selectedDatasourceIds,
  DatasourcesGroupComponent,
} from './datasources-group.component';
import {DatasourceType, EligibleDatasource} from '../../core/models/api.model';
import {
  ConnectorDriverMatrix,
  driverForType,
} from '../../core/models/connector-driver.model';
// The API's own capability matrix for the built-in drivers.
import driversFixture from '../../core/models/fixtures/connector-drivers.json';

const BUILTIN_DRIVERS = (driversFixture as unknown as ConnectorDriverMatrix).drivers;

/**
 * Selection logic for the explicit-only datasource picker: server-computed
 * defaults are selected, the
 * user can opt out, clone-based repository sources are excluded under a lite
 * (virtual/none) backend, centrally indexed KB sources remain available, and a
 * stale selection (after switching project) re-applies defaults.
 */
function makeDs(id: string, type: string, defaultSelected = false): EligibleDatasource {
  return {
    id,
    name: id,
    description: null,
    type: type as DatasourceType,
    connection_url: null,
    cli_hint: null,
    default_branch: null,
    job_id: null,
    created_at: '',
    updated_at: '',
    default_selected: defaultSelected,
  };
}

describe('datasources-group selection logic', () => {
  it('excludes credentials from workspaces without a shell', () => {
    const ds = [makeDs('key', 'credentials'), makeDs('kb', 'kb')];
    const selection = {key: datasourceSetKey(ds), ids: new Set(['key', 'kb'])};
    expect(selectedDatasourceIds(ds, selection, true)).toEqual(['kb']);
    expect(selectedDatasourceIds(ds, selection, false)).toEqual(['key', 'kb']);
  });

  it('locks existing session credentials but permits selecting new ones', () => {
    const component = Object.create(DatasourcesGroupComponent.prototype);
    component.lockedIds = () => [];
    component.initialSelectedIds = () => ['attached'];
    expect(component.isLocked(makeDs('attached', 'credentials'))).toBe(true);
    expect(component.isLocked(makeDs('new', 'credentials'))).toBe(false);
    expect(component.isLocked(makeDs('attached', 'postgresql'))).toBe(false);
  });
  it('defaults only server-selected eligible rows when untouched', () => {
    const ds = [makeDs('a', 'postgresql', true), makeDs('b', 'webdav')];
    expect(selectedDatasourceIds(ds, null, false, undefined, true)).toEqual(['a']);
  });

  it('respects an explicit opt-out for the current set', () => {
    const ds = [makeDs('a', 'postgresql'), makeDs('b', 'webdav')];
    const selection = {key: datasourceSetKey(ds), ids: new Set(['b'])};
    expect(selectedDatasourceIds(ds, selection, false)).toEqual(['b']);
  });

  it('excludes repository datasources under a lite backend', () => {
    const ds = [
      makeDs('repo', 'repository', true),
      makeDs('kb', 'kb', true),
      makeDs('pg', 'postgresql', true),
    ];
    expect(selectedDatasourceIds(ds, null, true, undefined, true)).toEqual(['kb', 'pg']);
  });

  it('includes OKF Knowledge Base datasources on every workspace tier', () => {
    const ds = [makeDs('kb', 'kb', true)];
    expect(selectedDatasourceIds(ds, null, true, undefined, true)).toEqual(['kb']);
    expect(selectedDatasourceIds(ds, null, false, undefined, true)).toEqual(['kb']);
  });

  it('includes repository datasources when the backend is not lite', () => {
    const ds = [makeDs('repo', 'repository', true), makeDs('pg', 'postgresql', true)];
    expect(new Set(selectedDatasourceIds(ds, null, false, undefined, true))).toEqual(
      new Set(['repo', 'pg']),
    );
  });

  it('re-applies the default when the selection is stale (set changed)', () => {
    const oldDs = [makeDs('a', 'postgresql')];
    // Opted out of everything while project A was selected.
    const stale = {key: datasourceSetKey(oldDs), ids: new Set<string>()};
    const newDs = [makeDs('x', 'webdav', true), makeDs('y', 'neo4j')];
    expect(new Set(selectedDatasourceIds(newDs, stale, false, undefined, true)))
      .toEqual(new Set(['x']));
  });

  it('drops ids no longer present in the datasource set', () => {
    const ds = [makeDs('a', 'postgresql')];
    const selection = {key: datasourceSetKey(ds), ids: new Set(['a', 'ghost'])};
    expect(selectedDatasourceIds(ds, selection, false)).toEqual(['a']);
  });

  it('activeDatasourceIds returns defaults when untouched, the choice when tagged', () => {
    const ds = [makeDs('a', 'postgresql', true), makeDs('b', 'webdav')];
    expect(activeDatasourceIds(ds, null, undefined, true)).toEqual(new Set(['a']));
    const sel = {key: datasourceSetKey(ds), ids: new Set(['a'])};
    expect(activeDatasourceIds(ds, sel)).toEqual(new Set(['a']));
  });

  it('preserves touched choices while newly eligible ids use server defaults', () => {
    const oldDs = [makeDs('a', 'postgresql', true), makeDs('b', 'webdav')];
    const selection = {
      key: datasourceSetKey(oldDs),
      ids: new Set(['b']),
      touched: new Set(['a', 'b']),
    };
    const refreshed = [
      makeDs('a', 'postgresql', true),
      makeDs('b', 'webdav'),
      makeDs('c', 'neo4j', true),
    ];
    expect(activeDatasourceIds(refreshed, selection, undefined, true))
      .toEqual(new Set(['b', 'c']));
    expect(datasourceSelectionDifferenceCount(refreshed, selection, undefined, true)).toBe(2);
  });

  it('fails closed on server defaults while preserving explicit choices and live arrays', () => {
    const ds = [makeDs('auto', 'postgresql', true), makeDs('manual', 'webdav')];
    expect(activeDatasourceIds(ds, null)).toEqual(new Set());

    const explicit = {key: datasourceSetKey(ds), ids: new Set(['manual'])};
    expect(activeDatasourceIds(ds, explicit)).toEqual(new Set(['manual']));
    expect(activeDatasourceIds(ds, null, new Set(['auto']))).toEqual(new Set(['auto']));
  });

  it('isRepositoryDatasource is case-insensitive', () => {
    expect(isRepositoryDatasource('repository')).toBe(true);
    expect(isRepositoryDatasource('Repository')).toBe(true);
    expect(isRepositoryDatasource('postgresql')).toBe(false);
  });

  it('requiresShellWorkspace matches the drivers that need a shell', () => {
    for (const type of [
      'repository',
      'credentials',
      'generic',
      'ssh_key',
      'Generic',
      'kubeconfig',
      'generic_file',
    ]) {
      expect(requiresShellWorkspace(type)).toBe(true);
    }
    for (const type of ['kb', 'postgresql', 'email', 'mcp']) {
      expect(requiresShellWorkspace(type)).toBe(false);
    }
  });

  it('excludes generic and SSH-key connectors under a lite backend', () => {
    const ds = [
      makeDs('env', 'generic', true),
      makeDs('key', 'ssh_key', true),
      makeDs('pg', 'postgresql', true),
    ];
    expect(selectedDatasourceIds(ds, null, true, undefined, true)).toEqual(['pg']);
  });

  it('reads the matrix: a driver needs a shell when its backends leave a lite tier out', () => {
    // The built-ins agree with the fallback copy, type by type.
    for (const driver of BUILTIN_DRIVERS) {
      if (!driver.legacy_type) continue;
      expect(requiresShellWorkspace(driver.legacy_type, BUILTIN_DRIVERS)).toBe(
        requiresShellWorkspace(driver.legacy_type),
      );
    }
    // A driver that changes its backends is followed, both ways.
    const shellGeneric = BUILTIN_DRIVERS.map((driver) =>
      driver.legacy_type === 'generic'
        ? {...driver, supported_backends: ['none', 'sandbox', 'virtual', 'vm']}
        : driver.legacy_type === 'postgresql'
          ? {...driver, supported_backends: ['sandbox', 'virtual', 'vm']}
          : driver,
    );
    expect(requiresShellWorkspace('generic', shellGeneric)).toBe(false);
    expect(requiresShellWorkspace('postgresql', shellGeneric)).toBe(true);
    const ds = [makeDs('env', 'generic', true), makeDs('pg', 'postgresql', true)];
    expect(selectedDatasourceIds(ds, null, true, undefined, true, shellGeneric)).toEqual(['env']);
    expect(allDatasourcesSelected(ds, null, true, undefined, undefined, true, shellGeneric)).toBe(true);
  });

  it('keeps the fallback copy for a type the matrix does not list', () => {
    expect(requiresShellWorkspace('repository', [])).toBe(true);
    expect(requiresShellWorkspace('kb', null)).toBe(false);
  });
});

describe("datasources-group the creator's read-only tag", () => {
  function picker(): DatasourcesGroupComponent {
    const component = Object.create(DatasourcesGroupComponent.prototype);
    component.connectorDrivers = {
      forType: (type: string) => driverForType(BUILTIN_DRIVERS, type),
    };
    return component;
  }

  it('badges a private row its creator tagged read-only', () => {
    const component = picker();
    const row = (type: string, over: Partial<EligibleDatasource>) => ({
      ...makeDs('ds', type),
      ...over,
    });
    // Every user's agents get its read tools only (decision 31).
    expect(component.isPrivateReadOnly(row('postgresql', {read_only: true}))).toBe(true);
    expect(component.isPrivateReadOnly(row('postgresql', {read_only: false}))).toBe(false);
    expect(component.isPrivateReadOnly(row('postgresql', {read_only: null}))).toBe(false);
    // A public row keeps its own badge.
    expect(
      component.isPrivateReadOnly(row('postgresql', {read_only: true, is_global: true})),
    ).toBe(false);
    // An MCP server binds every tool it lists, whatever the flag says.
    expect(component.isPrivateReadOnly(row('mcp', {read_only: true}))).toBe(false);
  });
});

describe('datasources-group select-all state', () => {
  it('is true when every eligible row is default-selected', () => {
    const ds = [makeDs('a', 'postgresql', true), makeDs('b', 'webdav', true)];
    expect(allDatasourcesSelected(ds, null, false, undefined, undefined, true)).toBe(true);
  });

  it('is false when one is opted out', () => {
    const ds = [makeDs('a', 'postgresql'), makeDs('b', 'webdav')];
    const selection = {key: datasourceSetKey(ds), ids: new Set(['a'])};
    expect(allDatasourcesSelected(ds, selection, false)).toBe(false);
  });

  it('is false when nothing is selected', () => {
    const ds = [makeDs('a', 'postgresql'), makeDs('b', 'webdav')];
    const selection = {key: datasourceSetKey(ds), ids: new Set<string>()};
    expect(allDatasourcesSelected(ds, selection, false)).toBe(false);
  });

  it('ignores lite-excluded repository sources (all toggleable selected ⇒ true)', () => {
    const ds = [
      makeDs('repo', 'repository', true),
      makeDs('kb', 'kb', true),
      makeDs('pg', 'postgresql', true),
    ];
    // repo is excluded under a lite backend; KB and pg remain selectable.
    const selection = {key: datasourceSetKey(ds), ids: new Set(['kb', 'pg'])};
    expect(allDatasourcesSelected(ds, selection, true)).toBe(true);
  });

  it('is false when there are no selectable datasources', () => {
    const ds = [makeDs('repo', 'repository')];
    expect(allDatasourcesSelected(ds, null, true)).toBe(false);
  });
});
