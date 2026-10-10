/**
 * The generated capability matrix: `GET /api/datasources/drivers`
 * (orchestrator/services/connector_drivers/matrix.py). One entry per
 * installed connector driver, built from its spec — never from a connector,
 * so nothing here is a credential value.
 *
 * Design: knowledge-base/knowledge/features/connector_drivers.md,
 * "A generated capability matrix" and slice D2. `fixtures/connector-drivers.json`
 * is the API's own response for the built-in drivers, and
 * `fixtures/connector-drivers-managed.json` the rows the managed MCP servers
 * add where the chart installs them, both pinned by
 * tests/test_connector_capability_matrix.py.
 */

/** The JSON Schema 2020-12 subset driver specs use, plus SRW's UI hints. */
export interface JsonSchema {
  type?: string | string[];
  title?: string;
  description?: string;
  properties?: Record<string, JsonSchema>;
  required?: string[];
  additionalProperties?: boolean | JsonSchema;
  propertyNames?: JsonSchema;
  items?: JsonSchema;
  enum?: unknown[];
  const?: unknown;
  oneOf?: JsonSchema[];
  anyOf?: JsonSchema[];
  default?: unknown;
  minimum?: number;
  maximum?: number;
  minLength?: number;
  pattern?: string;
  minItems?: number;
  maxItems?: number;
  maxProperties?: number;
  /** A secret: never read back, so an edit's blank keeps the stored value. */
  writeOnly?: boolean;
  /** Derived by SRW (a connector row's mirrored fields): never authored. */
  readOnly?: boolean;
  /** `file` (paste or upload), `textarea`, `password` or `json`. */
  'x-srw-widget'?: string;
  /** Sort key among siblings; unordered properties follow, in schema order. */
  'x-srw-order'?: number;
  /** Siblings sharing a group render under one heading. */
  'x-srw-group'?: string;
  'x-srw-multiline'?: boolean;
}

export interface ConnectorAccessLevel {
  id: string;
  /** Across connectors of one tool category the highest rank wins. */
  rank: number;
  /** Built-in tools the level binds; '*' for tools discovered at runtime. */
  tools: string[] | '*';
  /** The mechanism that makes the level true, in one line. */
  enforced_by: string;
  /** Only told to the agent; nothing stops it from doing more. */
  advisory: boolean;
}

export type CredentialSlotKind = 'secret_string' | 'file' | 'ssh_private_key' | 'oauth2' | 'kubeconfig';

export interface ConnectorCredentialSlot {
  name: string;
  kind: CredentialSlotKind;
  required: boolean;
  rotatable: boolean;
  /** The access levels that need the slot; empty means every level. */
  access_levels: string[];
  /** How it reaches the workspace; null when SRW or the agent process holds it. */
  delivery: 'env' | 'file' | 'ssh_agent' | null;
  /** How an edit treats the stored value. */
  update: 'keep_if_blank' | 'merge' | 'replace';
  /** The connector read field listing the slot's key names (never values),
   *  e.g. `env_var_names`; null when the read shows none. */
  names_field: string | null;
  /** The keys of the credentials object this slot owns. */
  schema: JsonSchema;
}

export interface ConnectorEgressRule {
  /** A literal host, a CIDR, or `${config.<key>}`. */
  host: string;
  ports: Array<number | string>;
  protocol: 'tcp' | 'udp';
}

/** A column SRW fills from a running driver pod (D5); until then, why not. */
export interface ConnectorEgressStatus {
  status: 'not_applicable' | 'not_enforced' | string;
  reason: string;
}

export interface ConnectorDriverTrust {
  /** `builtin` ships with SRW; `development` too, but only where a
   *  deployment switch installs it (the lease probe), never trusted;
   *  `managed` is a managed MCP server from SRW's catalogue (the Gitea MCP
   *  server, D5a): SRW wrote its spec and SRW's front enforces its levels,
   *  but its image is a third party's, so never trusted; `trusted` is SRW's
   *  own service driver image (the git swap driver) or a registered image
   *  (D6) on the trusted list, `custom` any other registered image. */
  tier: 'builtin' | 'development' | 'managed' | 'trusted' | 'custom';
  trusted: boolean;
  /** The image a driver's pods run; null for drivers inside SRW. */
  image: string | null;
  /** Outside the trusted list every claim is the author's, unverified. */
  claims_declared_by_author: boolean;
  /** Registered images only: whether the image may have privilege. */
  privileged?: boolean;
}

/** Where a registered image driver lives (D6); absent on SRW's own. */
export interface ConnectorDriverRegistration {
  id: string;
  scope: {kind: 'Account' | 'Project' | 'Catalog'; name: string};
  image_reference: string;
  image_digest: string;
  spec_source: 'label' | 'spec_operation' | 'server_json';
  /** The variables its bind may set, declared in its spec. */
  env_names?: string[];
  /** Disabled: it binds nothing new and its bindings are revoked. */
  disabled?: boolean;
  /** Whether the caller may disable, enable or delete it (the server's
   *  answer: an administrator for the Catalog, the owner for an Account,
   *  editors and up for a Project). */
  can_manage?: boolean;
  /** For a manager: its connectors and live bindings (what a Disable
   *  revokes). */
  usage?: {connectors: number; live_bindings: number};
}

/** `POST /api/connector-drivers`: the spec comes from the image itself. */
export interface RegisterConnectorDriverRequest {
  image: string;
  scope?: {kind: 'Account' | 'Project' | 'Catalog'; name: string};
  name?: string;
}

export interface ConnectorDriver {
  name: string;
  title: string;
  /** The `datasources.type` a built-in driver serves; null for the rest. */
  legacy_type: string | null;
  /** Whether this driver owns its stored type; false for a variant serving
   *  some of the type's rows (srw.mcp-remote/v1 beside srw.mcp/v1). */
  serves_stored_type: boolean;
  protocol_version: string;
  plane: 'harness' | 'bind_time' | 'service' | 'in_pod';
  delivery_forms: string[];
  supported_backends: string[];
  workspace_requirements: string;
  tool_category: string | null;
  operations: string[];
  /** Lowest rank first. */
  access_levels: ConnectorAccessLevel[];
  default_access: string | null;
  forced_read_only: boolean;
  holds_upstream_credentials: boolean;
  credential_slots: ConnectorCredentialSlot[];
  config_schema: JsonSchema;
  /** Built-in drivers only: whether the row's connection URL is used. */
  legacy_connection_url: 'required' | 'optional' | 'forbidden';
  egress: {
    declared: {rules: ConnectorEgressRule[]; needs_dns: string | null};
    enforced: ConnectorEgressStatus;
    installation: ConnectorEgressStatus;
  };
  publishable: boolean;
  live_attach: boolean;
  live_detach: 'immediate' | 'next_attach' | 'refused';
  delete_while_attached: boolean;
  max_per_execution: number | null;
  needs_knowledge_profile: boolean;
  deployment_gate: string | null;
  service: Record<string, unknown> | null;
  trust: ConnectorDriverTrust;
  /** A registered image driver's registration (D6). */
  registration?: ConnectorDriverRegistration;
}

export interface ConnectorDriverMatrix {
  protocol_version: string;
  drivers: ConnectorDriver[];
}

/**
 * The choices of the read-only switch that a driver offers, each with the
 * level it stands for.
 *
 * The same switch serves a project link and the connector's creator: a
 * read-only link or creator's tag floors the connector at its lowest level
 * for everyone who uses it (the stricter of the two binds; the level's
 * `enforced_by` says how), so read-only is offered only when a lower level
 * exists to floor to, and read-write leaves the connector at its own level,
 * so a driver forced read-only has none. A driver with no access levels
 * (env and file delivery) offers neither.
 */
export interface OfferedAccess {
  readOnly: ConnectorAccessLevel | null;
  readWrite: ConnectorAccessLevel | null;
}

export function offeredAccess(driver: ConnectorDriver | null | undefined): OfferedAccess | null {
  if (!driver) return null;
  const levels = [...driver.access_levels].sort((a, b) => a.rank - b.rank);
  if (levels.length === 0) return {readOnly: null, readWrite: null};
  const lowest = levels[0];
  if (driver.forced_read_only) return {readOnly: lowest, readWrite: null};
  if (levels.length === 1) return {readOnly: null, readWrite: lowest};
  return {readOnly: lowest, readWrite: levels[levels.length - 1]};
}

/**
 * Whether a public connector reads as read-write: its declared flag, unless
 * its driver offers one level only — an MCP server binds every tool it lists
 * whatever the flag says, and a KB is read-only whatever it says. Rows
 * stored before the matrix keep their flag; nothing is migrated.
 */
export function publicReadWrite(
  row: {read_only?: boolean | null},
  driver: ConnectorDriver | null | undefined,
): boolean {
  const access = offeredAccess(driver);
  if (access?.readWrite && !access.readOnly) return true;
  if (access?.readOnly && !access.readWrite) return false;
  return row.read_only === false;
}

/** The installed driver that owns a stored connector type (not a variant
 *  serving only some of its rows). */
export function driverForType(
  drivers: readonly ConnectorDriver[] | null | undefined,
  type: string | null | undefined,
): ConnectorDriver | null {
  if (!drivers || !type) return null;
  return (
    drivers.find((driver) => driver.legacy_type === type && driver.serves_stored_type) ?? null
  );
}

/** The managed MCP servers this deployment installs (tier `managed`), each
 *  a connector type of its own, in matrix order. None until the matrix
 *  loads: the chart turns each on by naming its image. */
export function managedConnectorDrivers(
  drivers: readonly ConnectorDriver[] | null | undefined,
): ConnectorDriver[] {
  return (drivers ?? []).filter(
    (driver) =>
      driver.trust.tier === 'managed' && driver.legacy_type !== null && driver.serves_stored_type,
  );
}
