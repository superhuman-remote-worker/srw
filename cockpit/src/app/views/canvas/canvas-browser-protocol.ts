import { environment } from '../../core/environment';
import { isCanonicalCanvasUuid } from './canvas-viewer-protocol';

/** WebSocket payload types relayed from the workspace stream protocol. */
export const BROWSER_MESSAGE_TYPE = Object.freeze({
  HELLO: 1,
  FRAME: 2,
  STATE: 3,
  INPUT: 4,
  CONTROL: 5,
  ERROR: 6,
} as const);

export const BROWSER_MAX_SERVER_MESSAGE_BYTES = 8 * 1024 * 1024;
export const BROWSER_MAX_JSON_BYTES = 64 * 1024;
export const BROWSER_MAX_VIEWPORT_DIMENSION = 8192;
export const BROWSER_MAX_URL_CHARS = 8192;
export const BROWSER_MAX_TITLE_CHARS = 1024;
export const BROWSER_MAX_ERROR_CODE_CHARS = 64;
export const BROWSER_MAX_ERROR_MESSAGE_CHARS = 2048;

export type BrowserBaton = 'agent' | 'user';

/** Generation-free state safe to expose from the pane-local controller. */
export interface BrowserPageState {
  readonly baton: BrowserBaton;
  readonly viewport: { readonly width: number; readonly height: number };
  readonly url: string | null;
  readonly title: string | null;
  readonly loading: boolean;
}

export interface BrowserStateServerMessage {
  readonly type: 'state';
  /** Protocol-private identity used by the controller to pin subsequent frames. */
  readonly generation: string;
  readonly state: BrowserPageState;
  /**
   * Whether the daemon sizes its page to `set_viewport`. Present only after
   * this viewer opted in with `client_features`; false when absent, which is
   * what an older daemon (one that ignores the opt-in) always looks like.
   */
  readonly viewportControl: boolean;
}

export interface BrowserFrameServerMessage {
  readonly type: 'frame';
  /** Protocol-private identity; never copied into public Canvas state. */
  readonly generation: string;
  readonly timestamp: number;
  readonly headerWidth: number | null;
  readonly headerHeight: number | null;
  readonly jpeg: Uint8Array;
}

export interface BrowserErrorServerMessage {
  readonly type: 'error';
  readonly code: string;
  readonly message: string;
}

export type BrowserServerMessage =
  | BrowserStateServerMessage
  | BrowserFrameServerMessage
  | BrowserErrorServerMessage;

/** Opt-in protocol features this Cockpit understands (see `client_features`). */
export const BROWSER_CLIENT_FEATURES = Object.freeze(['viewport_control'] as const);
export type BrowserClientFeature = (typeof BROWSER_CLIENT_FEATURES)[number];

export type BrowserControl =
  | { readonly op: 'take_baton' | 'release_baton' | 'back' | 'reload' }
  | { readonly op: 'navigate'; readonly url: string }
  /** CSS pixels of the page's viewport; honoured only from the baton holder. */
  | { readonly op: 'set_viewport'; readonly width: number; readonly height: number }
  | { readonly op: 'client_features'; readonly features: readonly BrowserClientFeature[] };

export type BrowserInputParameter = string | number | boolean;

/** Clipboard text cap: bounds the encoded INPUT message far below the relay's
 * 64 KiB client-message limit even at 4-byte UTF-8 code points. */
export const BROWSER_MAX_INSERT_TEXT_CHARS = 8000;

export interface BrowserInput {
  readonly kind: 'mouse' | 'key' | 'wheel' | 'insertText';
  readonly params: Readonly<Record<string, BrowserInputParameter>>;
}

const fatalUtf8 = new TextDecoder('utf-8', { fatal: true });
const utf8 = new TextEncoder();

/** Parse one broker WebSocket message without accepting partial future shapes. */
export function parseBrowserServerMessage(data: ArrayBuffer): BrowserServerMessage | null {
  if (data.byteLength < 2 || data.byteLength > BROWSER_MAX_SERVER_MESSAGE_BYTES) return null;
  const bytes = new Uint8Array(data);
  switch (bytes[0]) {
    case BROWSER_MESSAGE_TYPE.STATE:
      return parseState(bytes.subarray(1));
    case BROWSER_MESSAGE_TYPE.FRAME:
      return parseFrame(data);
    case BROWSER_MESSAGE_TYPE.ERROR:
      return parseError(bytes.subarray(1));
    default:
      return null;
  }
}

/** Encode one closed CONTROL command as `[type][UTF-8 JSON]`. */
export function encodeBrowserControl(message: BrowserControl): ArrayBuffer {
  if (!isRecord(message) || typeof message['op'] !== 'string') {
    throw new TypeError('Invalid browser control');
  }
  const op = message['op'];
  if (op === 'navigate') {
    if (
      !hasExactKeys(message, ['op', 'url']) ||
      !isBoundedString(message['url'], 1, BROWSER_MAX_URL_CHARS)
    ) {
      throw new TypeError('Invalid browser navigation control');
    }
  } else if (op === 'set_viewport') {
    if (
      !hasExactKeys(message, ['op', 'width', 'height']) ||
      !isViewportDimension(message['width']) ||
      !isViewportDimension(message['height'])
    ) {
      throw new TypeError('Invalid browser viewport control');
    }
  } else if (op === 'client_features') {
    const features = message['features'];
    if (
      !hasExactKeys(message, ['op', 'features']) ||
      !Array.isArray(features) ||
      features.length > BROWSER_CLIENT_FEATURES.length ||
      !features.every((feature) =>
        (BROWSER_CLIENT_FEATURES as readonly unknown[]).includes(feature))
    ) {
      throw new TypeError('Invalid browser client features');
    }
  } else if (
    !['take_baton', 'release_baton', 'back', 'reload'].includes(op) ||
    !hasExactKeys(message, ['op'])
  ) {
    throw new TypeError('Invalid browser control');
  }
  return encodeClientJson(BROWSER_MESSAGE_TYPE.CONTROL, message);
}

/** Encode one closed INPUT command as `[type][UTF-8 JSON]`. */
export function encodeBrowserInput(message: BrowserInput): ArrayBuffer {
  if (
    !isRecord(message) ||
    !hasExactKeys(message, ['kind', 'params']) ||
    typeof message['kind'] !== 'string' ||
    !['mouse', 'key', 'wheel', 'insertText'].includes(message['kind']) ||
    !isRecord(message['params'])
  ) {
    throw new TypeError('Invalid browser input');
  }
  if (message['kind'] === 'insertText') {
    if (
      !hasExactKeys(message['params'], ['text']) ||
      !isBoundedString(message['params']['text'], 1, BROWSER_MAX_INSERT_TEXT_CHARS)
    ) {
      throw new TypeError('Invalid browser text insertion');
    }
    return encodeClientJson(BROWSER_MESSAGE_TYPE.INPUT, message);
  }
  for (const value of Object.values(message['params'])) {
    if (
      (typeof value !== 'string' && typeof value !== 'boolean' && typeof value !== 'number') ||
      (typeof value === 'number' && !Number.isFinite(value))
    ) {
      throw new TypeError('Invalid browser input parameter');
    }
  }
  return encodeClientJson(BROWSER_MESSAGE_TYPE.INPUT, message);
}

const LOOPBACK_IPV4 = /^127(?:\.\d{1,3}){3}$/;

function isWorkspaceLoopbackHost(hostname: string): boolean {
  const host = hostname.toLowerCase().replace(/\.$/, '');
  return (
    host === 'localhost' ||
    host.endsWith('.localhost') ||
    LOOPBACK_IPV4.test(host) ||
    host === '0.0.0.0' ||
    host === '[::1]' ||
    host === '::1' ||
    host === '[::]'
  );
}

/**
 * Give a typed address a scheme the way the workspace daemon does
 * (`validate_user_nav` in docker/browser-exec): a colon followed by a port is
 * a host and port, not a scheme; a bare loopback address gets `http://`, as a
 * dev server in the workspace speaks plain HTTP; anything else bare gets
 * `https://`. Doing it here too keeps older daemons, which sent
 * `localhost:4200` to https or rejected it, on the same rule.
 */
export function normalizeBrowserAddress(typed: string): string {
  const address = typed.trim();
  if (!address) return address;
  const scheme = /^([A-Za-z][A-Za-z0-9+.-]*):/.exec(address);
  if (scheme) {
    const rest = address.slice(scheme[0].length);
    if (rest.startsWith('//') || !/^\d+(?:[/?#]|$)/.test(rest)) return address;
  }
  const authority = (/^[^/?#]*/.exec(address)?.[0] ?? '').replace(/^.*@/, '');
  const hostname = authority.startsWith('[')
    ? authority.slice(0, authority.indexOf(']') + 1)
    : authority.replace(/:\d*$/, '');
  return `${isWorkspaceLoopbackHost(hostname) ? 'http' : 'https'}://${address}`;
}

/**
 * Derive the cookie-authenticated stream endpoint from the configured API base.
 * No token, generation, query, or fragment is ever added.
 */
export function browserStreamUrl(threadId: string, apiUrl = environment.apiUrl): string | null {
  if (
    !isBoundedString(threadId, 1, 256) ||
    /[\\/\u0000-\u001f\u007f]/.test(threadId) ||
    !isBoundedString(apiUrl, 1, 4096) ||
    apiUrl !== apiUrl.trim() ||
    /[\\\u0000-\u001f\u007f]/.test(apiUrl) ||
    /%(?:2f|5c)/i.test(apiUrl)
  ) {
    return null;
  }

  // Inspect the raw path before URL parsing can normalize dot segments.
  const rawMatch = /^(https?):\/\/([^/?#]+)(\/[^?#]*)?$/.exec(apiUrl);
  if (!rawMatch) return null;
  const rawPath = rawMatch[3] ?? '';
  if (
    rawPath.startsWith('//') ||
    rawPath.includes('%') ||
    rawPath.includes('\\') ||
    rawPath.includes('//') ||
    rawPath.endsWith('//') ||
    rawPath.split('/').some((segment) => segment === '.' || segment === '..')
  ) {
    return null;
  }

  try {
    const api = new URL(apiUrl);
    if (
      (api.protocol !== 'http:' && api.protocol !== 'https:') ||
      api.username ||
      api.password ||
      api.search ||
      api.hash
    ) {
      return null;
    }
    const basePath = api.pathname === '/' ? '' : api.pathname.replace(/\/$/, '');
    const protocol = api.protocol === 'https:' ? 'wss:' : 'ws:';
    return (
      `${protocol}//${api.host}${basePath}/persistent/threads/` +
      `${encodeURIComponent(threadId)}/browser/stream`
    );
  } catch {
    return null;
  }
}

function parseState(payload: Uint8Array): BrowserStateServerMessage | null {
  const value = parseJsonObject(payload);
  if (
    !value ||
    !hasOnlyKeys(
      value,
      ['baton', 'generation', 'loading', 'title', 'url', 'viewport'],
      ['viewport_control'],
    ) ||
    (value['viewport_control'] !== undefined && typeof value['viewport_control'] !== 'boolean') ||
    !isCanonicalCanvasUuid(value['generation']) ||
    (value['baton'] !== 'agent' && value['baton'] !== 'user') ||
    !isRecord(value['viewport']) ||
    !hasExactKeys(value['viewport'], ['height', 'width']) ||
    !isViewportDimension(value['viewport']['width']) ||
    !isViewportDimension(value['viewport']['height']) ||
    !isNullableBoundedString(value['url'], BROWSER_MAX_URL_CHARS) ||
    !isNullableBoundedString(value['title'], BROWSER_MAX_TITLE_CHARS) ||
    typeof value['loading'] !== 'boolean'
  ) {
    return null;
  }
  return {
    type: 'state',
    generation: value['generation'],
    state: {
      baton: value['baton'],
      viewport: {
        width: value['viewport']['width'],
        height: value['viewport']['height'],
      },
      url: value['url'],
      title: value['title'],
      loading: value['loading'],
    },
    viewportControl: value['viewport_control'] === true,
  };
}

function parseFrame(data: ArrayBuffer): BrowserFrameServerMessage | null {
  // type byte + two-byte header length + JSON object + JPEG SOI
  if (data.byteLength < 7) return null;
  const view = new DataView(data);
  const headerLength = view.getUint16(1, false);
  const headerStart = 3;
  const jpegStart = headerStart + headerLength;
  if (
    headerLength < 2 ||
    headerLength > BROWSER_MAX_JSON_BYTES ||
    jpegStart + 2 > data.byteLength
  ) {
    return null;
  }
  const header = parseJsonObject(new Uint8Array(data, headerStart, headerLength));
  if (
    !header ||
    !hasOnlyKeys(header, ['generation', 'ts'], ['h', 'w']) ||
    !isCanonicalCanvasUuid(header['generation']) ||
    typeof header['ts'] !== 'number' ||
    !Number.isFinite(header['ts'])
  ) {
    return null;
  }
  const headerWidth = optionalFrameDimension(header['w']);
  const headerHeight = optionalFrameDimension(header['h']);
  if (headerWidth === undefined || headerHeight === undefined) return null;

  const jpegView = new Uint8Array(data, jpegStart);
  if (jpegView[0] !== 0xff || jpegView[1] !== 0xd8) return null;
  return {
    type: 'frame',
    generation: header['generation'],
    timestamp: header['ts'],
    headerWidth,
    headerHeight,
    // Copy only the already-bounded JPEG. The caller cannot retain a view over
    // unvalidated header bytes or an oversized backing buffer.
    jpeg: new Uint8Array(data.slice(jpegStart)),
  };
}

function parseError(payload: Uint8Array): BrowserErrorServerMessage | null {
  const value = parseJsonObject(payload);
  if (
    !value ||
    !hasExactKeys(value, ['code', 'message']) ||
    !isBoundedString(value['code'], 1, BROWSER_MAX_ERROR_CODE_CHARS) ||
    !isBoundedString(value['message'], 1, BROWSER_MAX_ERROR_MESSAGE_CHARS)
  ) {
    return null;
  }
  return { type: 'error', code: value['code'], message: value['message'] };
}

function parseJsonObject(payload: Uint8Array): Record<string, unknown> | null {
  if (payload.byteLength === 0 || payload.byteLength > BROWSER_MAX_JSON_BYTES) return null;
  try {
    const parsed: unknown = JSON.parse(fatalUtf8.decode(payload));
    return isRecord(parsed) ? parsed : null;
  } catch {
    return null;
  }
}

function encodeClientJson(type: number, message: object): ArrayBuffer {
  let payload: Uint8Array;
  try {
    payload = utf8.encode(JSON.stringify(message));
  } catch {
    throw new TypeError('Browser command is not serializable');
  }
  if (payload.byteLength + 1 > BROWSER_MAX_JSON_BYTES) {
    throw new RangeError('Browser command exceeds the protocol limit');
  }
  const wire = new Uint8Array(payload.byteLength + 1);
  wire[0] = type;
  wire.set(payload, 1);
  return wire.buffer;
}

function isViewportDimension(value: unknown): value is number {
  return (
    typeof value === 'number' &&
    Number.isInteger(value) &&
    value >= 1 &&
    value <= BROWSER_MAX_VIEWPORT_DIMENSION
  );
}

/** `undefined` means invalid; `null` is a valid omitted diagnostic dimension. */
function optionalFrameDimension(value: unknown): number | null | undefined {
  if (value === undefined || value === null) return null;
  return typeof value === 'number' &&
    Number.isFinite(value) &&
    value > 0 &&
    value <= BROWSER_MAX_VIEWPORT_DIMENSION
    ? value
    : undefined;
}

function isNullableBoundedString(value: unknown, max: number): value is string | null {
  return value === null || isBoundedString(value, 0, max);
}

function isBoundedString(value: unknown, min: number, max: number): value is string {
  return typeof value === 'string' && value.length >= min && value.length <= max;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function hasExactKeys(value: Record<string, unknown>, expected: readonly string[]): boolean {
  const keys = Object.keys(value).sort();
  const sortedExpected = [...expected].sort();
  return (
    keys.length === sortedExpected.length &&
    keys.every((key, index) => key === sortedExpected[index])
  );
}

function hasOnlyKeys(
  value: Record<string, unknown>,
  required: readonly string[],
  optional: readonly string[],
): boolean {
  const allowed = new Set([...required, ...optional]);
  return (
    required.every((key) => Object.hasOwn(value, key)) &&
    Object.keys(value).every((key) => allowed.has(key))
  );
}
