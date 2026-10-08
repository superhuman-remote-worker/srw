/**
 * The generated capability matrix: `GET /api/datasources/drivers`
 * (orchestrator/services/connector_drivers/matrix.py). One entry per
 * installed connector driver, built from its spec — never from a connector,
 * so nothing here is a credential value.
 *
 * Design: knowledge-base/knowledge/features/connector_drivers.md,
 * "A generated capability matrix" and slice D2. `fixtures/connector-drivers.json`
 * is the API's own response for the built-in drivers, pinned by
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
  /** `builtin` ships with SRW; `trusted` and `custom` are registered images (D6). */
  tier: 'builtin' | 'trusted' | 'custom';
  trusted: boolean;
  /** The registered image reference; null for drivers inside SRW. */
  image: string | null;
  /** Outside the trusted list every claim is the author's, unverified. */
  claims_declared_by_author: boolean;
}

export interface ConnectorDriver {
  name: string;
  title: string;
  /** The `datasources.type` a built-in driver serves; null for the rest. */
  legacy_type: string | null;
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
}

export interface ConnectorDriverMatrix {
  protocol_version: string;
  drivers: ConnectorDriver[];
}

/**
 * The choices of today's read-only switch (a public connector's access, a
 * project link's access) that a driver offers, each with the level it binds.
 *
 * Read-only floors the connector at its lowest level, so it is offered only
 * when a lower level exists to floor to; read-write leaves the connector at
 * its own level, so a driver that is forced read-only has none. A driver
 * with no access levels (env and file delivery) offers neither.
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

/** The installed driver serving a stored connector type. */
export function driverForType(
  drivers: readonly ConnectorDriver[] | null | undefined,
  type: string | null | undefined,
): ConnectorDriver | null {
  if (!drivers || !type) return null;
  return drivers.find((driver) => driver.legacy_type === type) ?? null;
}
