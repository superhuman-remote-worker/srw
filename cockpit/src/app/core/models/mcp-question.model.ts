/**
 * A question an MCP server asks the user while one of its tool calls runs
 * ("elicitation"): Civitai's "Spend 12 Buzz to run imageGen?". The agent
 * announces it as `question.request` (and lists the open ones in the
 * `session.state` snapshot's `pending_questions`), the answer goes to the
 * same decision endpoint a tool approval uses, and `question.resolved`
 * retires the card.
 *
 * Design: knowledge-base/knowledge/features/mcp_client_2026_07_28_elicitation.md
 * ("Phase C", "C2 Sessions", and the owner decisions of 2026-11-17). The
 * schema rules are the 2026-07-28 specification's flat form, the ones
 * `src/shared/mcp_questions.py` checks: a schema outside them never reaches
 * the Cockpit, the agent declines it.
 *
 * Pure: no Angular, so the service, the card and their specs share it.
 */

export type McpQuestionMode = 'form' | 'url';

/** One property of a form question's schema (the flat-primitive subset). */
export interface McpQuestionPropertySchema {
  type?: string;
  title?: string;
  description?: string;
  default?: unknown;
  minLength?: number;
  maxLength?: number;
  format?: string;
  enum?: unknown[];
  enumNames?: string[];
  oneOf?: Array<{const?: unknown; title?: string}>;
  anyOf?: Array<{const?: unknown; title?: string}>;
  minimum?: number;
  maximum?: number;
  items?: McpQuestionPropertySchema;
  minItems?: number;
  maxItems?: number;
}

export interface McpQuestionSchema {
  type?: string;
  properties?: Record<string, McpQuestionPropertySchema>;
  required?: string[];
}

/** A pending question, as the card shows it. */
export interface McpQuestion {
  /** The tool call the question belongs to. */
  id: string;
  /** The durable request id the decision is posted to. */
  approvalId: string;
  /** The connector's display name ("Civitai"). */
  server: string;
  serverSlug: string;
  /** The tool the call runs (`mcp__civitai__generate`), when known. */
  tool: string | null;
  mode: McpQuestionMode;
  /** The server's own text, shown as plain text. */
  message: string;
  schema: McpQuestionSchema | null;
  url: string | null;
  /** Set for a plain yes/no: Accept sends exactly this as the content. */
  confirmation: Record<string, unknown> | null;
}

/** What the user chose on the card. */
export type McpQuestionAnswer =
  | {decision: 'approve'; content?: Record<string, unknown>; alwaysAccept?: boolean}
  | {decision: 'deny'}
  | {decision: 'cancel'};

/** The card's transient state while (and after) an answer is sent. */
export interface McpQuestionUiState {
  /** The decision POST is in flight. */
  busy?: boolean;
  /** The server took the answer; the card waits for `question.resolved`. */
  sent?: boolean;
  /** Why the last answer did not land (a 422 detail, or a send failure). */
  error?: string | null;
}

/** What the transcript says once the question is settled. */
export type McpQuestionOutcome = 'accepted' | 'declined' | 'unanswered';

function isRecord(value: unknown): value is Record<string, unknown> {
  return !!value && typeof value === 'object' && !Array.isArray(value);
}

function str(value: unknown): string {
  return typeof value === 'string' ? value : '';
}

/** The schema with its properties in the server's order. The request
 *  travels through JSONB, which keeps an array's order but not an object's
 *  keys', so the agent sends the order beside the schema (`field_order`). */
function inFieldOrder(
  schema: McpQuestionSchema | null,
  order: unknown,
): McpQuestionSchema | null {
  if (!schema || !isRecord(schema.properties) || !Array.isArray(order)) return schema;
  const properties = schema.properties;
  const ordered: Record<string, McpQuestionPropertySchema> = {};
  for (const key of order) {
    if (typeof key === 'string' && key in properties) ordered[key] = properties[key];
  }
  for (const [key, value] of Object.entries(properties)) {
    if (!(key in ordered)) ordered[key] = value;
  }
  return {...schema, properties: ordered};
}

/** One wire entry (`question.request` data, or a `pending_questions` row),
 *  or null when it cannot be answered (no tool call or request id). */
export function toMcpQuestion(raw: unknown): McpQuestion | null {
  if (!isRecord(raw)) return null;
  const id = str(raw['id']);
  const approvalId = str(raw['approval_id']);
  if (!id || !approvalId) return null;
  const serverSlug = str(raw['server_slug']);
  return {
    id,
    approvalId,
    server: str(raw['server']) || serverSlug,
    serverSlug,
    tool: str(raw['tool']) || null,
    mode: raw['mode'] === 'url' ? 'url' : 'form',
    message: str(raw['message']),
    schema: inFieldOrder(
      isRecord(raw['schema']) ? (raw['schema'] as McpQuestionSchema) : null,
      raw['field_order'],
    ),
    url: str(raw['url']) || null,
    confirmation: isRecord(raw['confirmation']) ? raw['confirmation'] : null,
  };
}

/** A `pending_questions` list. A tool call holds one open question at a
 *  time; when a list names a call twice the later row wins, the way the
 *  pending-permission list converges on the newest request id. */
export function toMcpQuestions(raw: unknown): McpQuestion[] {
  const list = Array.isArray(raw) ? raw : [];
  const byCall = new Map<string, McpQuestion>();
  for (const entry of list) {
    const question = toMcpQuestion(entry);
    if (!question) continue;
    byCall.delete(question.id);
    byCall.set(question.id, question);
  }
  return [...byCall.values()];
}

/** `question.resolved`'s decision as the transcript states it: anything but
 *  an acceptance or a refusal is "not answered", never a refusal. */
export function questionOutcome(decision: unknown): McpQuestionOutcome {
  if (decision === 'approved') return 'accepted';
  if (decision === 'denied') return 'declined';
  return 'unanswered';
}

/** The decision endpoint's body for an answer. */
export function questionDecisionBody(answer: McpQuestionAnswer): Record<string, unknown> {
  if (answer.decision !== 'approve') return {decision: answer.decision};
  const body: Record<string, unknown> = {decision: 'approve'};
  if (answer.content !== undefined) body['content'] = answer.content;
  if (answer.alwaysAccept) body['always_accept'] = true;
  return body;
}

/** A 422's `detail` as one readable line, or null when it carries none. */
export function questionErrorDetail(error: unknown): string | null {
  const detail = isRecord(error) ? error['detail'] : error;
  if (typeof detail === 'string') return detail.trim() || null;
  if (Array.isArray(detail)) {
    const lines = detail
      .map((entry) => (isRecord(entry) ? str(entry['msg']) : str(entry)))
      .filter(Boolean);
    return lines.length ? lines.join(' ') : null;
  }
  return null;
}

/** The tool's own name, without the `mcp__<server>__` prefix the agent adds. */
export function questionToolLabel(tool: string | null, serverSlug = ''): string | null {
  if (!tool) return null;
  const prefix = serverSlug ? `mcp__${serverSlug}__` : '';
  if (prefix && tool.startsWith(prefix) && tool.length > prefix.length) {
    return tool.slice(prefix.length);
  }
  const match = /^mcp__.+?__(.+)$/.exec(tool);
  return match ? match[1] : tool;
}

// ── URL mode ─────────────────────────────────────────────────────────

/** A URL-mode question's address split for display, host highlighted. */
export interface QuestionUrlParts {
  /** The full address, as it opens. */
  href: string;
  /** Everything before the host (`https://`, and any `user@`). */
  before: string;
  host: string;
  /** The path, query and fragment. */
  after: string;
  /** The host is punycode (`xn--`): it may imitate another one. */
  lookAlike: boolean;
  /** Only http(s) opens; anything else (a `javascript:` URL) never does. */
  openable: boolean;
}

export function questionUrlParts(raw: string | null): QuestionUrlParts | null {
  if (!raw) return null;
  const unparsed: QuestionUrlParts = {
    href: raw,
    before: '',
    host: '',
    after: raw,
    lookAlike: raw.toLowerCase().includes('xn--'),
    openable: false,
  };
  let url: URL;
  try {
    url = new URL(raw);
  } catch {
    return unparsed;
  }
  // Only a web address opens, and is split: anything else is shown whole.
  if ((url.protocol !== 'https:' && url.protocol !== 'http:') || !url.host) {
    return {...unparsed, href: url.href, after: url.href};
  }
  // The parser has already turned a Unicode host into its punycode form,
  // so a look-alike host shows (and is caught) as `xn--…`.
  const userinfo =
    url.username || url.password
      ? `${url.username}${url.password ? `:${url.password}` : ''}@`
      : '';
  return {
    href: url.href,
    before: `${url.protocol}//${userinfo}`,
    host: url.host,
    after: `${url.pathname}${url.search}${url.hash}`,
    lookAlike: url.hostname.toLowerCase().includes('xn--'),
    openable: true,
  };
}

// ── Form mode ────────────────────────────────────────────────────────

export type QuestionFieldKind =
  | 'text'
  | 'email'
  | 'url'
  | 'date'
  | 'datetime'
  | 'number'
  | 'integer'
  | 'boolean'
  | 'select'
  | 'multi';

export interface QuestionOption {
  value: unknown;
  label: string;
}

export interface QuestionField {
  key: string;
  kind: QuestionFieldKind;
  /** The property's `title`, else its key. */
  label: string;
  description: string | null;
  required: boolean;
  minLength: number | null;
  maxLength: number | null;
  minimum: number | null;
  maximum: number | null;
  minItems: number | null;
  maxItems: number | null;
  /** A select's or multi-select's choices, in schema order. */
  options: QuestionOption[];
  default: unknown;
}

/**
 * A field's value as the card holds it: the input's text (text, number and
 * date kinds), a checkbox, a select's option index ('' for none) or a
 * multi-select's option indexes.
 */
export type QuestionFieldValue = string | boolean | number[];

export type QuestionValues = Record<string, QuestionFieldValue>;

/** A validation problem, as an i18n key under `chat.question.error`. */
export interface QuestionFieldError {
  key: string;
  params?: Record<string, unknown>;
}

function num(value: unknown): number | null {
  return typeof value === 'number' && Number.isFinite(value) ? value : null;
}

function optionsOf(schema: McpQuestionPropertySchema | undefined): QuestionOption[] {
  if (!schema) return [];
  if (Array.isArray(schema.enum)) {
    const names = Array.isArray(schema.enumNames) ? schema.enumNames : [];
    return schema.enum.map((value, i) => ({
      value,
      label: typeof names[i] === 'string' && names[i] ? names[i] : String(value),
    }));
  }
  const choices = Array.isArray(schema.oneOf)
    ? schema.oneOf
    : Array.isArray(schema.anyOf)
      ? schema.anyOf
      : null;
  if (choices) {
    return choices
      .filter((choice) => isRecord(choice) && 'const' in choice)
      .map((choice) => ({
        value: choice.const,
        label: typeof choice.title === 'string' && choice.title ? choice.title : String(choice.const),
      }));
  }
  return [];
}

function kindOf(schema: McpQuestionPropertySchema): QuestionFieldKind {
  switch (schema.type) {
    case 'boolean':
      return 'boolean';
    case 'number':
      return 'number';
    case 'integer':
      return 'integer';
    case 'array':
      return 'multi';
  }
  if (optionsOf(schema).length > 0) return 'select';
  switch (schema.format) {
    case 'email':
      return 'email';
    case 'uri':
      return 'url';
    case 'date':
      return 'date';
    case 'date-time':
      return 'datetime';
  }
  return 'text';
}

/** The fields of a form question, in the schema's property order. */
export function questionFields(schema: McpQuestionSchema | null): QuestionField[] {
  const properties = schema && isRecord(schema.properties) ? schema.properties : {};
  const required = new Set(Array.isArray(schema?.required) ? schema!.required : []);
  return Object.entries(properties)
    .filter(([, property]) => isRecord(property))
    .map(([key, property]) => {
      const kind = kindOf(property);
      return {
        key,
        kind,
        label: str(property.title) || key,
        description: str(property.description) || null,
        required: required.has(key),
        minLength: num(property.minLength),
        maxLength: num(property.maxLength),
        minimum: num(property.minimum),
        maximum: num(property.maximum),
        minItems: num(property.minItems),
        maxItems: num(property.maxItems),
        options: kind === 'multi' ? optionsOf(property.items) : optionsOf(property),
        default: property.default,
      };
    });
}

function pad(n: number): string {
  return String(n).padStart(2, '0');
}

/** An RFC 3339 date-time as a `datetime-local` input's local value. */
export function toLocalDateTimeInput(value: unknown): string {
  if (typeof value !== 'string' || !value) return '';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return '';
  return (
    `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}` +
    `T${pad(date.getHours())}:${pad(date.getMinutes())}`
  );
}

function sameValue(a: unknown, b: unknown): boolean {
  return a === b || JSON.stringify(a) === JSON.stringify(b);
}

/** The card's starting values: each property's default, prefilled. */
export function initialQuestionValues(fields: QuestionField[]): QuestionValues {
  const values: QuestionValues = {};
  for (const field of fields) {
    const fallback = field.default;
    switch (field.kind) {
      case 'boolean':
        values[field.key] = fallback === true;
        break;
      case 'select': {
        const index = field.options.findIndex((option) => sameValue(option.value, fallback));
        values[field.key] = index >= 0 ? String(index) : '';
        break;
      }
      case 'multi': {
        const wanted = Array.isArray(fallback) ? fallback : [];
        values[field.key] = field.options
          .map((option, index) => (wanted.some((w) => sameValue(w, option.value)) ? index : -1))
          .filter((index) => index >= 0);
        break;
      }
      case 'number':
      case 'integer':
        values[field.key] = typeof fallback === 'number' ? String(fallback) : '';
        break;
      case 'datetime':
        values[field.key] = toLocalDateTimeInput(fallback);
        break;
      default:
        values[field.key] = typeof fallback === 'string' ? fallback : '';
    }
  }
  return values;
}

const EMAIL = /^[^@\s]+@[^@\s]+\.[^@\s]+$/;
const URI = /^[A-Za-z][A-Za-z0-9+.-]*:\S+$/;
const DATE = /^\d{4}-\d{2}-\d{2}$/;

/** What the card sends, or the problems that keep it from sending. */
export interface QuestionAnswerDraft {
  content: Record<string, unknown>;
  errors: Record<string, QuestionFieldError>;
}

/**
 * Collect a form question's answer: every field with a value, optional
 * empty ones left out, and each value checked against the field's bounds
 * before it is sent (the server checks again and answers 422).
 */
export function collectQuestionAnswer(
  fields: QuestionField[],
  values: QuestionValues,
): QuestionAnswerDraft {
  const content: Record<string, unknown> = {};
  const errors: Record<string, QuestionFieldError> = {};
  for (const field of fields) {
    const raw = values[field.key];
    switch (field.kind) {
      case 'boolean':
        content[field.key] = raw === true;
        break;
      case 'select': {
        const index = typeof raw === 'string' && raw !== '' ? Number(raw) : -1;
        const option = field.options[index];
        if (!option) {
          if (field.required) errors[field.key] = {key: 'required'};
          break;
        }
        content[field.key] = option.value;
        break;
      }
      case 'multi': {
        const indexes = Array.isArray(raw) ? raw : [];
        const picked = field.options
          .filter((_, index) => indexes.includes(index))
          .map((option) => option.value);
        // Nothing picked on an optional field leaves it out.
        if (picked.length === 0 && !field.required) break;
        if (field.minItems != null && picked.length < field.minItems) {
          errors[field.key] = {key: 'minItems', params: {min: field.minItems}};
          break;
        }
        if (field.maxItems != null && picked.length > field.maxItems) {
          errors[field.key] = {key: 'maxItems', params: {max: field.maxItems}};
          break;
        }
        content[field.key] = picked;
        break;
      }
      case 'number':
      case 'integer': {
        const text = typeof raw === 'string' ? raw.trim() : '';
        if (!text) {
          if (field.required) errors[field.key] = {key: 'required'};
          break;
        }
        const value = Number(text);
        if (!Number.isFinite(value)) {
          errors[field.key] = {key: 'number'};
          break;
        }
        if (field.kind === 'integer' && !Number.isInteger(value)) {
          errors[field.key] = {key: 'integer'};
          break;
        }
        if (field.minimum != null && value < field.minimum) {
          errors[field.key] = {key: 'minimum', params: {min: field.minimum}};
          break;
        }
        if (field.maximum != null && value > field.maximum) {
          errors[field.key] = {key: 'maximum', params: {max: field.maximum}};
          break;
        }
        content[field.key] = value;
        break;
      }
      default: {
        const text = typeof raw === 'string' ? raw : '';
        const trimmed = text.trim();
        if (!trimmed) {
          if (field.required) errors[field.key] = {key: 'required'};
          break;
        }
        const error = textError(field, field.kind === 'text' ? text : trimmed);
        if (error) {
          errors[field.key] = error;
          break;
        }
        content[field.key] =
          field.kind === 'datetime'
            ? new Date(trimmed).toISOString()
            : field.kind === 'text'
              ? text
              : trimmed;
      }
    }
  }
  return {content, errors};
}

function textError(field: QuestionField, value: string): QuestionFieldError | null {
  // JSON Schema counts characters (code points), not UTF-16 units.
  const length = [...value].length;
  if (field.minLength != null && length < field.minLength) {
    return {key: 'minLength', params: {min: field.minLength}};
  }
  if (field.maxLength != null && length > field.maxLength) {
    return {key: 'maxLength', params: {max: field.maxLength}};
  }
  switch (field.kind) {
    case 'email':
      return EMAIL.test(value) ? null : {key: 'email'};
    case 'url':
      return URI.test(value) ? null : {key: 'url'};
    case 'date':
      return DATE.test(value) && !Number.isNaN(Date.parse(value)) ? null : {key: 'date'};
    case 'datetime':
      return Number.isNaN(new Date(value).getTime()) ? {key: 'date'} : null;
  }
  return null;
}
