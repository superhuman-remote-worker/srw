import { describe, expect, it } from 'vitest';
import {
  BROWSER_MAX_ERROR_CODE_CHARS,
  BROWSER_MAX_ERROR_MESSAGE_CHARS,
  BROWSER_MAX_INSERT_TEXT_CHARS,
  BROWSER_MAX_JSON_BYTES,
  BROWSER_MAX_SERVER_MESSAGE_BYTES,
  BROWSER_MAX_TITLE_CHARS,
  BROWSER_MAX_URL_CHARS,
  BROWSER_MESSAGE_TYPE,
  browserStreamUrl,
  encodeBrowserControl,
  encodeBrowserInput,
  normalizeBrowserAddress,
  parseBrowserServerMessage,
} from './canvas-browser-protocol';

const GENERATION = '5f0a9f5e-0000-4000-8000-000000000001';
const utf8 = new TextEncoder();
const decoder = new TextDecoder();

function state(overrides: Record<string, unknown> = {}) {
  return {
    generation: GENERATION,
    baton: 'agent',
    viewport: { width: 1280, height: 720 },
    url: 'https://example.test/',
    title: 'Example',
    loading: false,
    ...overrides,
  };
}

function serverJson(type: number, value: unknown): ArrayBuffer {
  const payload = utf8.encode(JSON.stringify(value));
  const wire = new Uint8Array(payload.byteLength + 1);
  wire[0] = type;
  wire.set(payload, 1);
  return wire.buffer;
}

function frame(
  header: Record<string, unknown> = {
    generation: GENERATION,
    w: 1280,
    h: 720,
    ts: 1_753_200_000.25,
  },
  jpeg = new Uint8Array([0xff, 0xd8, 0xff, 0xdb, 1, 2, 3]),
): ArrayBuffer {
  const headerBytes = utf8.encode(JSON.stringify(header));
  const wire = new Uint8Array(3 + headerBytes.byteLength + jpeg.byteLength);
  wire[0] = BROWSER_MESSAGE_TYPE.FRAME;
  new DataView(wire.buffer).setUint16(1, headerBytes.byteLength, false);
  wire.set(headerBytes, 3);
  wire.set(jpeg, 3 + headerBytes.byteLength);
  return wire.buffer;
}

function decodeClient(data: ArrayBuffer): { type: number; body: unknown } {
  const bytes = new Uint8Array(data);
  return {
    type: bytes[0],
    body: JSON.parse(decoder.decode(bytes.subarray(1))) as unknown,
  };
}

describe('shared-browser binary protocol', () => {
  it('locks the six wire type numbers and accepts only server directions', () => {
    expect(BROWSER_MESSAGE_TYPE).toEqual({
      HELLO: 1,
      FRAME: 2,
      STATE: 3,
      INPUT: 4,
      CONTROL: 5,
      ERROR: 6,
    });
    for (const type of [
      BROWSER_MESSAGE_TYPE.HELLO,
      BROWSER_MESSAGE_TYPE.INPUT,
      BROWSER_MESSAGE_TYPE.CONTROL,
      0,
      7,
    ]) {
      expect(parseBrowserServerMessage(serverJson(type, state()))).toBeNull();
    }
  });

  it('parses strict STATE while keeping generation out of public page state', () => {
    const parsed = parseBrowserServerMessage(
      serverJson(BROWSER_MESSAGE_TYPE.STATE, state({ baton: 'user' })),
    );

    expect(parsed).toEqual({
      type: 'state',
      generation: GENERATION,
      state: {
        baton: 'user',
        viewport: { width: 1280, height: 720 },
        url: 'https://example.test/',
        title: 'Example',
        loading: false,
      },
      viewportControl: false,
    });
    expect(parsed?.type === 'state' && 'generation' in parsed.state).toBe(false);
  });

  it('accepts the opt-in viewport capability only as a boolean outside page state', () => {
    for (const advertised of [true, false]) {
      const parsed = parseBrowserServerMessage(
        serverJson(BROWSER_MESSAGE_TYPE.STATE, state({ viewport_control: advertised })),
      );
      expect(parsed?.type === 'state' && parsed.viewportControl).toBe(advertised);
      expect(parsed?.type === 'state' && 'viewport_control' in parsed.state).toBe(false);
    }
  });

  it.each([
    state({ generation: GENERATION.toUpperCase() }),
    state({ generation: 'not-a-uuid' }),
    state({ baton: 'observer' }),
    state({ viewport: { width: 0, height: 720 } }),
    state({ viewport: { width: 1280.5, height: 720 } }),
    state({ viewport: { width: 1280, height: 8193 } }),
    state({ viewport: { width: '1280', height: 720 } }),
    state({ url: 42 }),
    state({ url: 'u'.repeat(BROWSER_MAX_URL_CHARS + 1) }),
    state({ title: 't'.repeat(BROWSER_MAX_TITLE_CHARS + 1) }),
    state({ loading: 0 }),
    state({ viewport_control: 'yes' }),
    state({ viewport_control: null }),
    { ...state(), extra: true },
    { ...state(), viewport: { width: 1280, height: 720, scale: 1 } },
    [],
  ])('rejects malformed STATE %#', (rejected) => {
    expect(parseBrowserServerMessage(serverJson(BROWSER_MESSAGE_TYPE.STATE, rejected))).toBeNull();
  });

  it('rejects invalid and oversized STATE JSON', () => {
    const invalidUtf8 = new Uint8Array([BROWSER_MESSAGE_TYPE.STATE, 0xc3, 0x28]).buffer;
    expect(parseBrowserServerMessage(invalidUtf8)).toBeNull();

    const oversized = new Uint8Array(BROWSER_MAX_JSON_BYTES + 2);
    oversized[0] = BROWSER_MESSAGE_TYPE.STATE;
    oversized.fill(0x20, 1);
    expect(parseBrowserServerMessage(oversized.buffer)).toBeNull();
  });

  it('parses FRAME into a bounded JPEG copy with diagnostic dimensions', () => {
    const wire = frame();
    const parsed = parseBrowserServerMessage(wire);

    expect(parsed?.type).toBe('frame');
    if (parsed?.type !== 'frame') throw new Error('expected frame');
    expect(parsed.generation).toBe(GENERATION);
    expect(parsed.timestamp).toBe(1_753_200_000.25);
    expect(parsed.headerWidth).toBe(1280);
    expect(parsed.headerHeight).toBe(720);
    expect(Array.from(parsed.jpeg)).toEqual([0xff, 0xd8, 0xff, 0xdb, 1, 2, 3]);
    expect(parsed.jpeg.buffer).not.toBe(wire);

    expect(parseBrowserServerMessage(frame({ generation: GENERATION, ts: 1 }))).toEqual(
      expect.objectContaining({ headerWidth: null, headerHeight: null }),
    );
    expect(
      parseBrowserServerMessage(
        frame({
          generation: GENERATION,
          w: null,
          h: null,
          ts: 1,
        }),
      ),
    ).toEqual(expect.objectContaining({ headerWidth: null, headerHeight: null }));
  });

  it.each([
    { generation: GENERATION.toUpperCase(), w: 1280, h: 720, ts: 1 },
    { generation: GENERATION, w: 0, h: 720, ts: 1 },
    { generation: GENERATION, w: 1280, h: -1, ts: 1 },
    { generation: GENERATION, w: '1280', h: 720, ts: 1 },
    { generation: GENERATION, w: 1280, h: 720, ts: null },
    { generation: GENERATION, w: 1280, h: 720, ts: 1, extra: true },
  ])('rejects malformed FRAME header %#', (header) => {
    expect(parseBrowserServerMessage(frame(header))).toBeNull();
  });

  it('rejects truncated, non-JPEG, and oversized FRAME payloads', () => {
    const truncated = new Uint8Array(frame());
    new DataView(truncated.buffer).setUint16(1, 65_535, false);
    expect(parseBrowserServerMessage(truncated.buffer)).toBeNull();
    expect(parseBrowserServerMessage(frame(undefined, new Uint8Array([1, 2, 3])))).toBeNull();

    const oversized = new Uint8Array(BROWSER_MAX_SERVER_MESSAGE_BYTES + 1);
    oversized[0] = BROWSER_MESSAGE_TYPE.FRAME;
    expect(parseBrowserServerMessage(oversized.buffer)).toBeNull();
  });

  it('parses only bounded exact ERROR objects', () => {
    expect(
      parseBrowserServerMessage(
        serverJson(BROWSER_MESSAGE_TYPE.ERROR, {
          code: 'navigation_rejected',
          message: 'Blocked hostname',
        }),
      ),
    ).toEqual({
      type: 'error',
      code: 'navigation_rejected',
      message: 'Blocked hostname',
    });

    for (const rejected of [
      { code: '', message: 'message' },
      { code: 'x'.repeat(BROWSER_MAX_ERROR_CODE_CHARS + 1), message: 'message' },
      { code: 'error', message: '' },
      { code: 'error', message: 'x'.repeat(BROWSER_MAX_ERROR_MESSAGE_CHARS + 1) },
      { code: 'error', message: 'message', extra: true },
      ['error', 'message'],
    ]) {
      expect(
        parseBrowserServerMessage(serverJson(BROWSER_MESSAGE_TYPE.ERROR, rejected)),
      ).toBeNull();
    }
  });

  it('encodes only INPUT and CONTROL client message shapes', () => {
    expect(decodeClient(encodeBrowserControl({ op: 'take_baton' }))).toEqual({
      type: BROWSER_MESSAGE_TYPE.CONTROL,
      body: { op: 'take_baton' },
    });
    expect(
      decodeClient(
        encodeBrowserControl({
          op: 'navigate',
          url: 'https://example.test/',
        }),
      ),
    ).toEqual({
      type: BROWSER_MESSAGE_TYPE.CONTROL,
      body: { op: 'navigate', url: 'https://example.test/' },
    });
    expect(
      decodeClient(
        encodeBrowserInput({
          kind: 'mouse',
          params: { type: 'mouseMoved', x: 10.5, y: 20, modifiers: 0 },
        }),
      ),
    ).toEqual({
      type: BROWSER_MESSAGE_TYPE.INPUT,
      body: { kind: 'mouse', params: { type: 'mouseMoved', x: 10.5, y: 20, modifiers: 0 } },
    });

    expect(
      decodeClient(
        encodeBrowserInput({ kind: 'insertText', params: { text: 'hunter2' } }),
      ),
    ).toEqual({
      type: BROWSER_MESSAGE_TYPE.INPUT,
      body: { kind: 'insertText', params: { text: 'hunter2' } },
    });

    expect(
      decodeClient(encodeBrowserControl({ op: 'set_viewport', width: 390, height: 844 })),
    ).toEqual({
      type: BROWSER_MESSAGE_TYPE.CONTROL,
      body: { op: 'set_viewport', width: 390, height: 844 },
    });
    expect(
      decodeClient(
        encodeBrowserControl({ op: 'client_features', features: ['viewport_control'] }),
      ),
    ).toEqual({
      type: BROWSER_MESSAGE_TYPE.CONTROL,
      body: { op: 'client_features', features: ['viewport_control'] },
    });
    for (const bad of [
      { op: 'set_viewport', width: 390.5, height: 844 },
      { op: 'set_viewport', width: 0, height: 844 },
      { op: 'set_viewport', width: 8193, height: 844 },
      { op: 'set_viewport', width: '390', height: 844 },
      { op: 'set_viewport', width: 390 },
      { op: 'set_viewport', width: 390, height: 844, scale: 2 },
      { op: 'client_features', features: ['telepathy'] },
      { op: 'client_features', features: 'viewport_control' },
      { op: 'client_features', features: ['viewport_control', 'viewport_control'] },
    ]) {
      expect(() => encodeBrowserControl(bad as never)).toThrow(TypeError);
    }
    expect(() => encodeBrowserControl({ op: 'other' } as never)).toThrow(TypeError);
    expect(() => encodeBrowserControl({ op: 'back', url: 'extra' } as never)).toThrow(TypeError);
    expect(() => encodeBrowserInput({ kind: 'other', params: {} } as never)).toThrow(TypeError);
    expect(() => encodeBrowserInput({ kind: 'insertText', params: {} } as never)).toThrow(TypeError);
    expect(() =>
      encodeBrowserInput({
        kind: 'insertText',
        params: { text: 'x'.repeat(BROWSER_MAX_INSERT_TEXT_CHARS + 1) },
      }),
    ).toThrow(TypeError);
    expect(() =>
      encodeBrowserInput({
        kind: 'insertText',
        params: { text: 'ok', extra: 'nope' },
      }),
    ).toThrow(TypeError);
    expect(() =>
      encodeBrowserInput({
        kind: 'key',
        params: { type: 'keyDown', value: Number.NaN },
      }),
    ).toThrow(TypeError);
    expect(() =>
      encodeBrowserControl({
        op: 'navigate',
        url: 'x'.repeat(BROWSER_MAX_JSON_BYTES),
      }),
    ).toThrow();
  });
});

describe('shared-browser address bar', () => {
  it.each([
    ['localhost:4200', 'http://localhost:4200'],
    ['localhost', 'http://localhost'],
    ['LOCALHOST:3000/path?q=1', 'http://LOCALHOST:3000/path?q=1'],
    ['127.0.0.1:8000', 'http://127.0.0.1:8000'],
    ['127.0.0.1', 'http://127.0.0.1'],
    ['[::1]:3000/x', 'http://[::1]:3000/x'],
    ['0.0.0.0:5173', 'http://0.0.0.0:5173'],
    ['app.localhost:5173/', 'http://app.localhost:5173/'],
    ['  localhost:4200  ', 'http://localhost:4200'],
    ['example.com', 'https://example.com'],
    ['example.com:8080/x', 'https://example.com:8080/x'],
    ['localhost.example.com', 'https://localhost.example.com'],
    ['https://localhost:8443/', 'https://localhost:8443/'],
    ['http://example.com/', 'http://example.com/'],
    ['javascript:alert(1)', 'javascript:alert(1)'],
    ['', ''],
  ])('gives %j the scheme the workspace daemon would', (typed, expected) => {
    expect(normalizeBrowserAddress(typed)).toBe(expected);
  });
});

describe('shared-browser stream URL', () => {
  it('maps normalized HTTP(S) API bases to cookie-authenticated WS(S) paths', () => {
    expect(browserStreamUrl('thread-1', 'http://localhost:8085/api')).toBe(
      'ws://localhost:8085/api/persistent/threads/thread-1/browser/stream',
    );
    expect(browserStreamUrl('thread id', 'https://api.example.test/root/api/')).toBe(
      'wss://api.example.test/root/api/persistent/threads/thread%20id/browser/stream',
    );
    expect(browserStreamUrl('thread-1', 'https://api.example.test')).toBe(
      'wss://api.example.test/persistent/threads/thread-1/browser/stream',
    );
  });

  it.each([
    ['', 'https://api.example.test/api'],
    ['thread/one', 'https://api.example.test/api'],
    ['thread\\one', 'https://api.example.test/api'],
    ['thread\none', 'https://api.example.test/api'],
    ['thread-1', 'ftp://api.example.test/api'],
    ['thread-1', '//api.example.test/api'],
    ['thread-1', ' https://api.example.test/api'],
    ['thread-1', 'https://user@api.example.test/api'],
    ['thread-1', 'https://api.example.test/api?token=x'],
    ['thread-1', 'https://api.example.test/api#fragment'],
    ['thread-1', 'https://api.example.test//api'],
    ['thread-1', 'https://api.example.test/a/../api'],
    ['thread-1', 'https://api.example.test/api\\other'],
    ['thread-1', 'https://api.example.test/%2fapi'],
    ['thread-1', 'https://api.example.test/%255capi'],
  ])('rejects ambiguous stream URL input %#', ([threadId, apiUrl]) => {
    expect(browserStreamUrl(threadId, apiUrl)).toBeNull();
  });
});
