import {DatasourceType} from '../../../core/models/api.model';

/**
 * Bespoke connector forms, by driver name (connector_drivers.md, slice D2).
 *
 * Each built-in driver keeps the hand-built section of the connector editor
 * (datasource-list.component.ts) that knows its extras: env key-value merge,
 * file paste or upload, SSH key generation, forge inference from the URL,
 * the email provider presets and send rules, the MCP transports, the native
 * knowledge-base lock. The value names that section; the editor still
 * switches its blocks on the connector type, which is the same word.
 *
 * A driver missing here — a managed MCP server (srw.gitea-mcp/v1), any
 * driver a later slice registers — gets the generic form, rendered from its
 * spec (generic-connector-form.component.ts).
 */
export const BESPOKE_CONNECTOR_FORMS: Readonly<Record<string, DatasourceType>> = {
  'srw.generic/v1': 'generic',
  'srw.credentials/v1': 'credentials',
  'srw.repository/v1': 'repository',
  'srw.kb/v1': 'kb',
  'srw.postgresql/v1': 'postgresql',
  'srw.neo4j/v1': 'neo4j',
  'srw.mongodb/v1': 'mongodb',
  'srw.webdav/v1': 'webdav',
  'srw.email/v1': 'email',
  'srw.mcp/v1': 'mcp',
  // The remote transports of the same stored type: one bespoke form for both.
  'srw.mcp-remote/v1': 'mcp',
  'srw.kubeconfig/v1': 'kubeconfig',
  'srw.ssh-key/v1': 'ssh_key',
  'srw.generic-file/v1': 'generic_file',
};

/** The bespoke section for a driver, or null when it takes the generic form. */
export function bespokeFormFor(driverName: string | null | undefined): DatasourceType | null {
  if (!driverName) return null;
  return Object.prototype.hasOwnProperty.call(BESPOKE_CONNECTOR_FORMS, driverName)
    ? BESPOKE_CONNECTOR_FORMS[driverName]
    : null;
}
