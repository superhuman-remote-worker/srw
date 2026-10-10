import {describe, expect, it} from 'vitest';
import {BESPOKE_CONNECTOR_FORMS, bespokeFormFor} from './connector-form-registry';
import {ConnectorDriverMatrix} from '../../../core/models/connector-driver.model';
import fixture from '../../../core/models/fixtures/connector-drivers.json';
import managedFixture from '../../../core/models/fixtures/connector-drivers-managed.json';

const matrix = fixture as unknown as ConnectorDriverMatrix;
const managed = managedFixture as unknown as ConnectorDriverMatrix;

describe('the bespoke connector form registry', () => {
  it('registers every installed datasource driver by name, at its own section', () => {
    // A connector only SRW creates (the main cloud's folder) has no form.
    const datasourceDrivers = matrix.drivers.filter(
      (driver) => driver.legacy_type !== null && driver.user_creatable !== false,
    );
    expect(matrix.drivers.filter((driver) => driver.user_creatable === false).map((d) => d.name))
      .toEqual(['srw.cloud-folder/v1']);
    expect(bespokeFormFor('srw.cloud-folder/v1')).toBeNull();
    expect(Object.keys(BESPOKE_CONNECTOR_FORMS).sort()).toEqual(
      datasourceDrivers.map((driver) => driver.name).sort(),
    );
    for (const driver of datasourceDrivers) {
      expect(bespokeFormFor(driver.name)).toBe(driver.legacy_type);
    }
  });

  it('sends a managed MCP server to the generic form: its spec is its form', () => {
    for (const driver of managed.drivers) {
      expect(bespokeFormFor(driver.name)).toBeNull();
    }
  });

  it('sends any other driver to the generic form', () => {
    expect(bespokeFormFor('community.neo4j-mcp/v2')).toBeNull();
    expect(bespokeFormFor('toString')).toBeNull();
    expect(bespokeFormFor(null)).toBeNull();
  });
});
