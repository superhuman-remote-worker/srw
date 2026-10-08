import {describe, expect, it} from 'vitest';
import {
  ConnectorDriver,
  ConnectorDriverMatrix,
  driverForType,
  managedConnectorDrivers,
  offeredAccess,
  publicReadWrite,
} from './connector-driver.model';
// The API's own response for the built-in drivers, and the rows the managed
// MCP servers add where the chart installs them
// (tests/test_connector_capability_matrix.py pins both).
import fixture from './fixtures/connector-drivers.json';
import managedFixture from './fixtures/connector-drivers-managed.json';

const matrix = fixture as unknown as ConnectorDriverMatrix;
const managed = managedFixture as unknown as ConnectorDriverMatrix;
const byName = (name: string): ConnectorDriver =>
  matrix.drivers.find((driver) => driver.name === name)!;

describe('offeredAccess', () => {
  it('offers both choices for a read/read-write driver, each with its level', () => {
    const access = offeredAccess(byName('srw.postgresql/v1'))!;
    expect(access.readOnly?.id).toBe('ReadOnly');
    expect(access.readWrite?.id).toBe('ReadWrite');
    expect(access.readOnly?.enforced_by).toContain('READ ONLY transaction');
  });

  it('offers only read-only for a driver forced read-only', () => {
    expect(offeredAccess(byName('srw.kb/v1'))).toEqual({
      readOnly: byName('srw.kb/v1').access_levels[0],
      readWrite: null,
    });
  });

  it('offers no read-only switch for a driver with one level', () => {
    const access = offeredAccess(byName('srw.mcp/v1'))!;
    expect(access.readOnly).toBeNull();
    expect(access.readWrite?.tools).toBe('*');
  });

  it('floors read-only at the lowest of several levels', () => {
    const access = offeredAccess(byName('srw.email/v1'))!;
    expect(access.readOnly?.id).toBe('read');
    expect(access.readWrite?.id).toBe('send');
  });

  it('offers nothing for delivery that cannot enforce a level', () => {
    expect(offeredAccess(byName('srw.env/v1'))).toEqual({readOnly: null, readWrite: null});
  });

  it('is unknown without a driver', () => {
    expect(offeredAccess(null)).toBeNull();
  });

  it('marks the declared-only read-only levels advisory', () => {
    for (const name of ['srw.generic/v1', 'srw.credentials/v1', 'srw.ssh-key/v1', 'srw.repository/v1']) {
      expect(offeredAccess(byName(name))!.readOnly!.advisory).toBe(true);
    }
  });
});

describe('publicReadWrite', () => {
  it("follows a public connector's declared flag when the driver has both levels", () => {
    const postgres = byName('srw.postgresql/v1');
    expect(publicReadWrite({read_only: false}, postgres)).toBe(true);
    expect(publicReadWrite({read_only: true}, postgres)).toBe(false);
  });

  it("shows a one-level driver's only level, whatever the stored flag says", () => {
    expect(publicReadWrite({read_only: true}, byName('srw.mcp/v1'))).toBe(true);
    expect(publicReadWrite({read_only: false}, byName('srw.kb/v1'))).toBe(false);
  });

  it('reads the stored flag without a driver', () => {
    expect(publicReadWrite({read_only: false}, null)).toBe(true);
    expect(publicReadWrite({read_only: null}, null)).toBe(false);
  });
});

describe('driverForType', () => {
  it('finds a built-in driver by its stored type', () => {
    expect(driverForType(matrix.drivers, 'ssh_key')?.name).toBe('srw.ssh-key/v1');
    expect(driverForType(matrix.drivers, 'nope')).toBeNull();
    expect(driverForType(null, 'kb')).toBeNull();
  });

  it('answers with the driver that owns the type, never a variant serving some of its rows', () => {
    const remoteFirst = [...matrix.drivers].sort((a, b) =>
      a.name === 'srw.mcp-remote/v1' ? -1 : b.name === 'srw.mcp-remote/v1' ? 1 : 0,
    );
    expect(remoteFirst[0].name).toBe('srw.mcp-remote/v1');
    expect(remoteFirst[0].serves_stored_type).toBe(false);
    expect(driverForType(remoteFirst, 'mcp')?.name).toBe('srw.mcp/v1');
  });
});

describe('managedConnectorDrivers', () => {
  it('lists the installed managed MCP servers, each owning its type', () => {
    const installed = [...matrix.drivers, ...managed.drivers];
    expect(managed.drivers.map((driver) => driver.name)).toContain('srw.gitea-mcp/v1');
    expect(managedConnectorDrivers(installed)).toEqual(managed.drivers);
    expect(driverForType(installed, 'gitea_mcp')?.name).toBe('srw.gitea-mcp/v1');
  });

  it('lists none the deployment does not install, and none before the matrix loads', () => {
    expect(managedConnectorDrivers(matrix.drivers)).toEqual([]);
    expect(managedConnectorDrivers(null)).toEqual([]);
  });

  it('never lists another tier, though it runs the same way', () => {
    const [gitea] = managed.drivers;
    for (const tier of ['development', 'custom', 'trusted'] as const) {
      expect(managedConnectorDrivers([{...gitea, trust: {...gitea.trust, tier}}])).toEqual([]);
    }
  });
});
