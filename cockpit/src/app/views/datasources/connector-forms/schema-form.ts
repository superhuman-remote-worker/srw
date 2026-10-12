/**
 * The generic connector form's model: a driver's `config_schema` and
 * credential slots turned into fields, the editable state behind them, and
 * the create/update payload they produce.
 *
 * Covers what driver specs use (connector_drivers.md, "The driver contract"):
 * objects, strings, numbers, booleans, enums, `const`, nullable types, arrays
 * of strings or objects, string maps (`additionalProperties`), free-form
 * objects (edited as JSON), `oneOf`/`anyOf` choices — discriminated by one
 * `const` property or picked by position — and `writeOnly` secrets, plus the
 * UI hints `x-srw-widget` (`file`, `textarea`, `password`, `json`),
 * `x-srw-order`, `x-srw-group` and `x-srw-multiline`. A `readOnly` property
 * is SRW's to set (a connector row's mirrored fields), so the form neither
 * shows nor sends it.
 *
 * The checks here stop an obviously incomplete submit (the editor's Save waits
 * for them), including an edit that would wipe stored secrets (`formValue`).
 * The orchestrator's driver `validate` is the authority on the rest: its 400
 * detail is shown at the field it names, or above the form.
 */
import {
  ConnectorCredentialSlot,
  ConnectorDriver,
  JsonSchema,
} from '../../../core/models/connector-driver.model';

export type FieldKind =
  | 'text'
  | 'number'
  | 'boolean'
  | 'enum'
  | 'const'
  | 'object'
  | 'map'
  | 'list'
  | 'choice'
  | 'json';
export type TextWidget = 'input' | 'password' | 'textarea' | 'file';

export interface FieldGroup {
  /** `x-srw-group`; null for properties without one. */
  name: string | null;
  fields: FormNode[];
}

export interface ChoiceBranch {
  label: string;
  node: FormNode;
}

/** One field. A flat shape (unused members keep their defaults) so the
 *  recursive template needs no type narrowing. */
export interface FormNode {
  kind: FieldKind;
  key: string;
  label: string;
  description: string | null;
  required: boolean;
  /** `writeOnly` here or above: never read back, blank keeps on an edit. */
  secret: boolean;
  defaultValue: unknown;
  widget: TextWidget;
  nullable: boolean;
  pattern: string | null;
  integer: boolean;
  minimum: number | null;
  maximum: number | null;
  /** `enum` choices; the state holds the chosen index. */
  options: unknown[];
  /** `const`. */
  value: unknown;
  /** `object`: properties in display order, grouped. */
  groups: FieldGroup[];
  /** `map`: the value schema and the key rule. */
  entry: FormNode | null;
  keyPattern: string | null;
  /** `list`: the item schema. */
  item: FormNode | null;
  minItems: number;
  /** `list` items or `map` entries at most. */
  maxItems: number | null;
  /** `choice`: the `const` property telling branches apart, if any. */
  discriminator: string | null;
  branches: ChoiceBranch[];
  /** A discriminated branch: its `const` is sent even with nothing else. */
  branch: boolean;
}

export interface MapRow {
  key: string;
  value: FieldState;
}

export interface ChoiceState {
  branch: number;
  /** One state per branch, so switching back keeps what was typed. */
  values: FieldState[];
}

/** text/number/json: the raw string; enum: the option index ('' = none);
 *  boolean; object: by key; map: rows; list: items; choice: ChoiceState. */
export type FieldState =
  | string
  | boolean
  | null
  | FieldState[]
  | MapRow[]
  | ChoiceState
  | {[key: string]: FieldState};

export type ProblemReason = 'required' | 'pattern' | 'not_a_number' | 'invalid_json' | 'too_few';

export interface FormProblem {
  /** JSON pointer into the request body, e.g. `/config/host`. */
  pointer: string;
  reason: ProblemReason;
}

export interface SlotModel {
  slot: ConnectorCredentialSlot;
  node: FormNode;
}

export interface GenericFormModel {
  driver: ConnectorDriver;
  /** A built-in driver's connection URL column; null when it has none. */
  connectionUrl: 'required' | 'optional' | null;
  config: FormNode;
  slots: SlotModel[];
}

export interface GenericFormState {
  connection_url: string;
  config: FieldState;
  slots: Record<string, FieldState>;
}

export interface GenericFormValue {
  connection_url?: string;
  config?: Record<string, unknown>;
  credentials?: Record<string, unknown>;
  problems: FormProblem[];
}

/** What an existing connector contributes on an edit (secrets never do). */
export interface ExistingConnector {
  connection_url?: string | null;
  connection_url_redacted?: boolean;
  config?: unknown;
}

// ---------------------------------------------------------------------------
// Schema -> fields
// ---------------------------------------------------------------------------

function blankNode(fields: Partial<FormNode>): FormNode {
  return {
    kind: 'json',
    key: '',
    label: '',
    description: null,
    required: false,
    secret: false,
    defaultValue: undefined,
    widget: 'input',
    nullable: false,
    pattern: null,
    integer: false,
    minimum: null,
    maximum: null,
    options: [],
    value: undefined,
    groups: [],
    entry: null,
    keyPattern: null,
    item: null,
    minItems: 0,
    maxItems: null,
    discriminator: null,
    branches: [],
    branch: false,
    ...fields,
  };
}

/** `known_hosts` -> `Known hosts`; a schema `title` wins over this. */
export function humanize(key: string): string {
  const words = key.replace(/[_-]+/g, ' ').trim();
  return words ? words.charAt(0).toUpperCase() + words.slice(1) : '';
}

function isSchema(value: unknown): value is JsonSchema {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function typesOf(schema: JsonSchema): string[] {
  if (Array.isArray(schema.type)) return schema.type;
  return schema.type ? [schema.type] : [];
}

function textWidget(schema: JsonSchema, secret: boolean): TextWidget {
  const hint = schema['x-srw-widget'];
  if (hint === 'file' || hint === 'textarea' || hint === 'password') return hint;
  if (schema['x-srw-multiline']) return 'textarea';
  return secret ? 'password' : 'input';
}

export function parseSchema(
  schema: JsonSchema,
  key = '',
  required = false,
  inheritedSecret = false,
): FormNode {
  const secret = inheritedSecret || schema.writeOnly === true;
  const base = blankNode({
    key,
    label: schema.title ?? humanize(key),
    description: schema.description ?? null,
    required,
    secret,
    defaultValue: schema.default,
  });
  if (schema['x-srw-widget'] === 'json') return base;
  if (schema.const !== undefined) return {...base, kind: 'const', value: schema.const};
  const choices = schema.oneOf ?? schema.anyOf;
  if (Array.isArray(choices) && choices.length > 0) {
    return parseChoice(base, choices, key, required, secret);
  }
  if (Array.isArray(schema.enum)) {
    return {
      ...base,
      kind: 'enum',
      options: schema.enum.filter((option) => option !== null),
      nullable: schema.enum.includes(null),
    };
  }
  const types = typesOf(schema);
  const nullable = types.includes('null');
  const type =
    types.find((t) => t !== 'null') ??
    (schema.properties ? 'object' : schema.items ? 'array' : undefined);
  switch (type) {
    case 'string':
      return {
        ...base,
        kind: 'text',
        nullable,
        widget: textWidget(schema, secret),
        pattern: schema.pattern ?? null,
      };
    case 'integer':
    case 'number':
      return {
        ...base,
        kind: 'number',
        nullable,
        integer: type === 'integer',
        minimum: schema.minimum ?? null,
        maximum: schema.maximum ?? null,
      };
    case 'boolean':
      return {...base, kind: 'boolean'};
    case 'array':
      if (!isSchema(schema.items)) return base;
      return {
        ...base,
        kind: 'list',
        item: parseSchema(schema.items, '', false, secret),
        minItems: schema.minItems ?? 0,
        maxItems: schema.maxItems ?? null,
      };
    case 'object':
      if (schema.properties) {
        return {...base, kind: 'object', groups: parseProperties(schema, secret)};
      }
      if (isSchema(schema.additionalProperties)) {
        return {
          ...base,
          kind: 'map',
          entry: parseSchema(schema.additionalProperties, '', false, secret),
          keyPattern: schema.propertyNames?.pattern ?? null,
          maxItems: schema.maxProperties ?? null,
        };
      }
      // No properties and no further keys allowed: a driver without config.
      if (schema.maxProperties === 0 || schema.additionalProperties === false) {
        return {...base, kind: 'object'};
      }
      return base;
  }
  return base;
}

function parseProperties(schema: JsonSchema, secret: boolean): FieldGroup[] {
  const required = new Set(schema.required ?? []);
  const entries = Object.entries(schema.properties ?? {})
    .map(([name, child], index) => ({
      name,
      index,
      child,
      order: typeof child['x-srw-order'] === 'number' ? child['x-srw-order'] : Infinity,
    }))
    .filter(({child}) => child.readOnly !== true);
  entries.sort((a, b) => a.order - b.order || a.index - b.index);
  // Groups appear where their first (sorted) property does.
  const groups: FieldGroup[] = [];
  for (const {name, child} of entries) {
    const group = child['x-srw-group'] ?? null;
    let target = groups.find((g) => g.name === group);
    if (!target) {
      target = {name: group, fields: []};
      groups.push(target);
    }
    target.fields.push(parseSchema(child, name, required.has(name), secret));
  }
  return groups;
}

/** The property every branch fixes to a distinct `const`, if one exists. */
function discriminatorOf(branches: JsonSchema[]): string | null {
  const first = branches[0]?.properties ?? {};
  for (const name of Object.keys(first)) {
    const values = branches.map((branch) => branch.properties?.[name]?.const);
    if (values.some((value) => value === undefined)) continue;
    if (new Set(values.map((value) => JSON.stringify(value))).size === values.length) {
      return name;
    }
  }
  return null;
}

function parseChoice(
  base: FormNode,
  choices: JsonSchema[],
  key: string,
  required: boolean,
  secret: boolean,
): FormNode {
  const discriminator = discriminatorOf(choices);
  const branches = choices.map((choice, index) => {
    const node = {...parseSchema(choice, key, required, secret), branch: discriminator !== null};
    let label = choice.title;
    if (!label && discriminator) label = String(choice.properties?.[discriminator]?.const);
    if (!label && choice.properties) label = Object.keys(choice.properties).join(', ');
    if (!label) label = typesOf(choice).join(' | ') || `#${index + 1}`;
    return {label, node};
  });
  return {...base, kind: 'choice', discriminator, branches};
}

/** Every field of a driver: the connection URL, config, then each slot. */
export function buildFormModel(driver: ConnectorDriver): GenericFormModel {
  return {
    driver,
    connectionUrl:
      driver.legacy_connection_url === 'forbidden' ? null : driver.legacy_connection_url,
    config: parseSchema(driver.config_schema),
    slots: driver.credential_slots.map((slot) => ({
      slot,
      node: {...parseSchema(slot.schema, slot.name, slot.required), label: humanize(slot.name)},
    })),
  };
}

/** The fields of an object node in display order. */
export function fieldsOf(node: FormNode): FormNode[] {
  return node.groups.flatMap((group) => group.fields);
}

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

function asRecord(value: unknown): Record<string, unknown> {
  return isSchema(value) ? (value as Record<string, unknown>) : {};
}

export function initialState(node: FormNode, existing: unknown): FieldState {
  // A secret is never prefilled: not from a stored config that holds one by
  // mistake, not from an author's default.
  const known = node.secret ? undefined : existing === undefined ? node.defaultValue : existing;
  switch (node.kind) {
    case 'text':
      return typeof known === 'string' ? known : '';
    case 'number':
      return typeof known === 'number' ? String(known) : '';
    case 'boolean':
      return known === true;
    case 'enum': {
      const index = node.options.findIndex((option) => option === known);
      return index >= 0 ? String(index) : '';
    }
    case 'const':
      return null;
    case 'object': {
      const values = asRecord(known);
      const state: {[key: string]: FieldState} = {};
      for (const field of fieldsOf(node)) state[field.key] = initialState(field, values[field.key]);
      return state;
    }
    case 'map':
      return Object.entries(asRecord(known)).map(([key, value]) => ({
        key,
        value: initialState(node.entry!, value),
      }));
    case 'list': {
      const items = Array.isArray(known) ? known : [];
      const count = Math.max(items.length, node.minItems);
      return Array.from({length: count}, (_, i) => initialState(node.item!, items[i]));
    }
    case 'choice': {
      const branch = matchingBranch(node, known);
      return {
        branch,
        values: node.branches.map((b, i) => initialState(b.node, i === branch ? known : undefined)),
      };
    }
    case 'json':
      return known === undefined ? '' : JSON.stringify(known, null, 2);
  }
}

function matchingBranch(node: FormNode, value: unknown): number {
  if (value === undefined) return 0;
  if (node.discriminator) {
    const tag = asRecord(value)[node.discriminator];
    const index = node.branches.findIndex(
      (b) => fieldsOf(b.node).find((f) => f.key === node.discriminator)?.value === tag,
    );
    return Math.max(index, 0);
  }
  const shape = typeof value === 'object' && value !== null ? 'object' : 'scalar';
  const index = node.branches.findIndex((b) =>
    shape === 'object' ? b.node.kind === 'object' || b.node.kind === 'map' : b.node.kind !== 'object',
  );
  return Math.max(index, 0);
}

export function initialFormState(
  model: GenericFormModel,
  existing: ExistingConnector | null = null,
): GenericFormState {
  const url = existing?.connection_url_redacted ? '' : (existing?.connection_url ?? '');
  const slots: Record<string, FieldState> = {};
  for (const {slot, node} of model.slots) slots[slot.name] = initialState(node, undefined);
  return {
    connection_url: url,
    config: initialState(model.config, existing?.config),
    slots,
  };
}

/** A blank list item or map value, for the add buttons. */
export function blankState(node: FormNode): FieldState {
  return initialState(node, undefined);
}

/** A state path: `/`-joined, each key escaped as in a JSON pointer, e.g.
 *  `config/files/0/contents`. Strings, so template bindings stay stable. */
export function childPath(path: string, key: string | number): string {
  const segment = typeof key === 'number' ? String(key) : escapePointer(key);
  return path ? `${path}/${segment}` : segment;
}

function segmentsOf(path: string): string[] {
  return path.split('/').map((part) => part.replace(/~1/g, '/').replace(/~0/g, '~'));
}

export function stateAt(root: unknown, path: string): FieldState {
  let current: unknown = root;
  for (const segment of segmentsOf(path)) {
    if (current === null || typeof current !== 'object') return null;
    current = (current as Record<string, unknown>)[segment];
  }
  return (current ?? null) as FieldState;
}

/** Assign in place; the caller signals the change. */
export function setStateAt(root: unknown, path: string, value: FieldState | number): void {
  // Escaped segments hold no '/', so the last one starts after the last '/'.
  const cut = path.lastIndexOf('/');
  const parent = cut < 0 ? root : stateAt(root, path.slice(0, cut));
  const [key] = segmentsOf(path.slice(cut + 1));
  if (parent !== null && typeof parent === 'object') {
    (parent as Record<string, unknown>)[key] = value;
  }
}

// ---------------------------------------------------------------------------
// State -> payload
// ---------------------------------------------------------------------------

/** The value a field sends, or undefined to send nothing (blank). */
export function toValue(node: FormNode, state: FieldState): unknown {
  switch (node.kind) {
    case 'text':
      return typeof state === 'string' && state !== '' ? state : undefined;
    case 'number': {
      const raw = typeof state === 'string' ? state.trim() : '';
      if (!raw) return undefined;
      const parsed = Number(raw);
      return Number.isFinite(parsed) ? parsed : raw;
    }
    case 'boolean':
      return state === true ? true : node.required ? false : undefined;
    case 'enum':
      return typeof state === 'string' && state !== '' ? node.options[Number(state)] : undefined;
    case 'const':
      return node.value;
    case 'object': {
      const values = (state ?? {}) as {[key: string]: FieldState};
      const out: Record<string, unknown> = {};
      let filled = false;
      for (const field of fieldsOf(node)) {
        if (field.kind === 'const') continue;
        const value = toValue(field, values[field.key]);
        if (value === undefined) continue;
        out[field.key] = value;
        filled = true;
      }
      if (!filled && !node.branch) return undefined;
      for (const field of fieldsOf(node)) {
        if (field.kind === 'const') out[field.key] = field.value;
      }
      return out;
    }
    case 'map': {
      const out: Record<string, unknown> = {};
      for (const row of (state ?? []) as MapRow[]) {
        const key = row.key.trim();
        const value = toValue(node.entry!, row.value);
        if (key && value !== undefined) out[key] = value;
      }
      return Object.keys(out).length ? out : undefined;
    }
    case 'list': {
      const items = ((state ?? []) as FieldState[])
        .map((item) => toValue(node.item!, item))
        .filter((value) => value !== undefined);
      return items.length ? items : undefined;
    }
    case 'choice': {
      const choice = state as ChoiceState;
      const branch = node.branches[choice?.branch ?? 0];
      return branch ? toValue(branch.node, choice.values[choice.branch]) : undefined;
    }
    case 'json': {
      const raw = typeof state === 'string' ? state.trim() : '';
      if (!raw) return undefined;
      try {
        return JSON.parse(raw);
      } catch {
        return raw;
      }
    }
  }
}

/** Fields that stop a submit before the API sees it. */
export function problemsOf(
  node: FormNode,
  state: FieldState,
  pointer: string,
  editing: boolean,
  mustExist: boolean,
): FormProblem[] {
  const value = toValue(node, state);
  if (value === undefined) {
    const keeps = editing && node.secret;
    return mustExist && !keeps && node.kind !== 'const' ? [{pointer, reason: 'required'}] : [];
  }
  switch (node.kind) {
    case 'text':
      return node.pattern && !matches(node.pattern, value as string)
        ? [{pointer, reason: 'pattern'}]
        : [];
    case 'number':
      return typeof value === 'number' ? [] : [{pointer, reason: 'not_a_number'}];
    case 'json':
      return typeof value === 'string' ? [{pointer, reason: 'invalid_json'}] : [];
    case 'object': {
      const values = state as {[key: string]: FieldState};
      return fieldsOf(node).flatMap((field) =>
        problemsOf(field, values[field.key], `${pointer}/${escapePointer(field.key)}`, editing, field.required),
      );
    }
    case 'map':
      return (state as MapRow[]).flatMap((row) => {
        const key = row.key.trim();
        if (!key) return [];
        const at = `${pointer}/${escapePointer(key)}`;
        const own: FormProblem[] =
          node.keyPattern && !matches(node.keyPattern, key) ? [{pointer: at, reason: 'pattern'}] : [];
        return [...own, ...problemsOf(node.entry!, row.value, at, editing, false)];
      });
    case 'list': {
      const items = state as FieldState[];
      const filled = (value as unknown[]).length;
      const own: FormProblem[] = filled < node.minItems ? [{pointer, reason: 'too_few'}] : [];
      return [
        ...own,
        ...items.flatMap((item, i) => problemsOf(node.item!, item, `${pointer}/${i}`, editing, false)),
      ];
    }
    case 'choice': {
      const choice = state as ChoiceState;
      const branch = node.branches[choice.branch];
      return problemsOf(branch.node, choice.values[choice.branch], pointer, editing, mustExist);
    }
    default:
      return [];
  }
}

function matches(pattern: string, value: string): boolean {
  try {
    return new RegExp(pattern, 'u').test(value);
  } catch {
    return true;
  }
}

/** JSON pointer escaping (RFC 6901). */
function escapePointer(key: string): string {
  return key.replace(/~/g, '~0').replace(/\//g, '~1');
}

export function slotPointer(field: FormNode): string {
  return `/credentials/${escapePointer(field.key)}`;
}

/** The request body parts the form owns, and what still blocks a submit. */
export function formValue(
  model: GenericFormModel,
  state: GenericFormState,
  editing: boolean,
): GenericFormValue {
  const problems: FormProblem[] = [];
  const value: GenericFormValue = {problems};
  const url = state.connection_url.trim();
  if (url) value.connection_url = url;
  else if (model.connectionUrl === 'required' && !editing) {
    problems.push({pointer: '/connection_url', reason: 'required'});
  }

  const config = toValue(model.config, state.config);
  if (config !== undefined) value.config = config as Record<string, unknown>;
  problems.push(...problemsOf(model.config, state.config, '/config', editing, false));

  // The API replaces the whole stored credentials object whenever an update
  // carries any (only the `credentials` driver merges). So an edit either
  // sends none, keeping every stored secret, or re-enters them all: once a
  // credential is typed, the required slots are checked as on a create and
  // every secret in a slot in use is required, a blank one no longer keeping
  // anything. The built-in slots declare this as their `update` rule
  // (`replace`, and `merge` for the `credentials` driver); no built-in slot
  // keeps a blank field of an edit that sends others (`keep_if_blank`).
  const replacing =
    editing && model.slots.some(({slot, node}) => toValue(node, state.slots[slot.name]) !== undefined);
  const keepsSecrets = editing && !replacing;
  const credentials: Record<string, unknown> = {};
  for (const {slot, node} of model.slots) {
    const slotState = state.slots[slot.name];
    Object.assign(credentials, (toValue(node, slotState) ?? {}) as Record<string, unknown>);
    // A slot's keys sit at the top of the credentials object.
    const values = slotState as {[key: string]: FieldState};
    const blank = toValue(node, slotState) === undefined;
    if (blank && !(slot.required && !keepsSecrets)) continue;
    const before = problems.length;
    for (const field of fieldsOf(node)) {
      const needed = field.required || (replacing && field.secret && field.kind === 'text');
      problems.push(...problemsOf(field, values[field.key], slotPointer(field), keepsSecrets, needed));
    }
    // A required slot whose schema requires no key still needs one.
    const first = fieldsOf(node).find((field) => field.kind !== 'const');
    if (blank && problems.length === before && first) {
      problems.push({pointer: slotPointer(first), reason: 'required'});
    }
  }
  if (Object.keys(credentials).length) value.credentials = credentials;
  return value;
}

// ---------------------------------------------------------------------------
// The API's refusal
// ---------------------------------------------------------------------------

export interface ConnectorFormError {
  message: string;
  /** The JSON pointer the API named, or null for the whole form. */
  field: string | null;
}

/**
 * The API's refusal as a message and the field it names. Connector writes
 * answer `{detail: "<message>"}`; a driver error envelope answers
 * `{detail: {message, field}}` with a JSON pointer; FastAPI's own validation
 * answers `{detail: [{loc: ["body", ...], msg}]}`.
 */
export function connectorFormError(err: unknown): ConnectorFormError | null {
  const detail = (err as {error?: {detail?: unknown}} | null)?.error?.detail;
  if (typeof detail === 'string') return {message: detail, field: null};
  if (Array.isArray(detail) && detail.length > 0) {
    const first = detail[0] as {loc?: unknown[]; msg?: string};
    const loc = (first.loc ?? []).filter((part, i) => !(i === 0 && part === 'body'));
    return {
      message: first.msg ?? JSON.stringify(first),
      field: loc.length ? '/' + loc.map((part) => escapePointer(String(part))).join('/') : null,
    };
  }
  if (isSchema(detail)) {
    const record = detail as {message?: unknown; field?: unknown};
    return {
      message: typeof record.message === 'string' ? record.message : JSON.stringify(detail),
      field: typeof record.field === 'string' && record.field.startsWith('/') ? record.field : null,
    };
  }
  return null;
}

/** The pointer of every field the form renders for `state`. */
export function renderedPointers(model: GenericFormModel, state: GenericFormState): Set<string> {
  const out = new Set<string>();
  if (model.connectionUrl) out.add('/connection_url');
  const config = (state.config ?? {}) as {[key: string]: FieldState};
  for (const field of fieldsOf(model.config)) {
    collectPointers(field, config[field.key], `/config/${escapePointer(field.key)}`, out);
  }
  for (const {slot, node} of model.slots) {
    const values = (state.slots[slot.name] ?? {}) as {[key: string]: FieldState};
    for (const field of fieldsOf(node)) {
      collectPointers(field, values[field.key], slotPointer(field), out);
    }
  }
  return out;
}

function collectPointers(node: FormNode, state: FieldState, pointer: string, out: Set<string>): void {
  out.add(pointer);
  switch (node.kind) {
    case 'object': {
      const values = (state ?? {}) as {[key: string]: FieldState};
      for (const field of fieldsOf(node)) {
        collectPointers(field, values[field.key], `${pointer}/${escapePointer(field.key)}`, out);
      }
      return;
    }
    case 'list':
      ((state ?? []) as FieldState[]).forEach((item, i) =>
        collectPointers(node.item!, item, `${pointer}/${i}`, out),
      );
      return;
    case 'map':
      for (const row of (state ?? []) as MapRow[]) {
        if (row.key.trim()) {
          collectPointers(node.entry!, row.value, `${pointer}/${escapePointer(row.key.trim())}`, out);
        }
      }
      return;
    case 'choice': {
      const choice = state as ChoiceState;
      const branch = node.branches[choice?.branch ?? 0];
      if (branch) collectPointers(branch.node, choice.values[choice.branch], pointer, out);
      return;
    }
  }
}

/** The deepest rendered pointer at or above `field`, or null. */
export function errorAnchor(field: string | null, rendered: ReadonlySet<string>): string | null {
  let pointer = field;
  while (pointer) {
    if (rendered.has(pointer)) return pointer;
    const cut = pointer.lastIndexOf('/');
    pointer = cut > 0 ? pointer.slice(0, cut) : null;
  }
  return null;
}
