import { TestBed } from '@angular/core/testing';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { environment } from '../../core/environment';
import { CanvasState } from '../../core/models/canvas.model';
import { BROWSER_MESSAGE_TYPE, BrowserPageState } from './canvas-browser-protocol';
import {
  BROWSER_HANDSHAKE_TIMEOUT_MS,
  CANVAS_BROWSER_BITMAP_FACTORY,
  CANVAS_BROWSER_SOCKET_FACTORY,
  CANVAS_BROWSER_TIMEOUTS,
  CANVAS_BROWSER_VISIBILITY,
  CanvasBrowserController,
  CanvasBrowserTimeouts,
  CanvasBrowserVisibility,
} from './canvas-browser.controller';

const GENERATION = '5f0a9f5e-0000-4000-8000-000000000001';
const OTHER_GENERATION = '6f0a9f5e-0000-4000-8000-000000000002';
const utf8 = new TextEncoder();

function browserState(revision = 1, overrides: Partial<CanvasState> = {}): CanvasState {
  return {
    canvas_id: 'main',
    source: { type: 'browser' },
    title: 'Shared browser',
    renderer: 'auto',
    editable: false,
    alt_text: null,
    presentation_revision: revision,
    source_version: null,
    status: 'ready',
    capabilities: {
      can_edit: false,
      can_pop_out: true,
      can_take_control: true,
      can_stream_browser: true,
    },
    updated_at: `2026-07-22T10:00:0${revision}Z`,
    ...overrides,
  };
}

function pageState(overrides: Partial<BrowserPageState> = {}) {
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

function decodeClient(data: ArrayBuffer): {type: number; body: unknown} {
  const bytes = new Uint8Array(data);
  return {
    type: bytes[0],
    body: JSON.parse(new TextDecoder().decode(bytes.subarray(1))),
  };
}

function frame(generation = GENERATION): ArrayBuffer {
  const header = utf8.encode(JSON.stringify({ generation, w: 1280, h: 720, ts: 1_753_200_000 }));
  const jpeg = new Uint8Array([0xff, 0xd8, 0xff, 0xdb, 1, 2, 3]);
  const wire = new Uint8Array(3 + header.byteLength + jpeg.byteLength);
  wire[0] = BROWSER_MESSAGE_TYPE.FRAME;
  new DataView(wire.buffer).setUint16(1, header.byteLength, false);
  wire.set(header, 3);
  wire.set(jpeg, 3 + header.byteLength);
  return wire.buffer;
}

class FakeSocket {
  readonly sent: ArrayBuffer[] = [];
  readonly closes: Array<{ code?: number; reason?: string }> = [];
  binaryType: BinaryType = 'blob';
  readyState = 0;
  onopen: ((event: Event) => void) | null = null;
  onmessage: ((event: MessageEvent<unknown>) => void) | null = null;
  onclose: ((event: CloseEvent) => void) | null = null;
  onerror: ((event: Event) => void) | null = null;

  constructor(readonly url: string) {}

  open(): void {
    this.readyState = 1;
    this.onopen?.(new Event('open'));
  }

  send(data: ArrayBuffer): void {
    this.sent.push(data);
  }

  close(code?: number, reason?: string): void {
    this.readyState = 3;
    this.closes.push({ code, reason });
  }

  message(data: unknown): void {
    this.onmessage?.({ data } as MessageEvent<unknown>);
  }

  serverClose(code: number, wasClean: boolean): void {
    const listener = this.onclose;
    this.readyState = 3;
    listener?.({ code, wasClean } as CloseEvent);
  }

  fail(): void {
    this.onerror?.(new Event('error'));
  }
}

class FakeVisibility implements CanvasBrowserVisibility {
  visibilityState: DocumentVisibilityState = 'visible';
  readonly listeners = new Set<() => void>();

  addEventListener(type: 'visibilitychange', listener: () => void): void {
    if (type === 'visibilitychange') this.listeners.add(listener);
  }

  removeEventListener(type: 'visibilitychange', listener: () => void): void {
    if (type === 'visibilitychange') this.listeners.delete(listener);
  }

  set(value: DocumentVisibilityState): void {
    this.visibilityState = value;
    for (const listener of [...this.listeners]) listener();
  }
}

class FakeTimeouts implements CanvasBrowserTimeouts {
  readonly delays: number[] = [];
  private nextId = 1;
  private readonly tasks = new Map<number, () => void>();

  set(callback: () => void, delayMs: number): ReturnType<typeof setTimeout> {
    const id = this.nextId++;
    this.delays.push(delayMs);
    this.tasks.set(id, callback);
    return id as unknown as ReturnType<typeof setTimeout>;
  }

  clear(handle: ReturnType<typeof setTimeout>): void {
    this.tasks.delete(handle as unknown as number);
  }

  runNext(): void {
    const entry = this.tasks.entries().next().value as [number, () => void] | undefined;
    if (!entry) throw new Error('No pending timer');
    this.tasks.delete(entry[0]);
    entry[1]();
  }

  get pending(): number {
    return this.tasks.size;
  }
}

function bitmap(name: string) {
  return {
    name,
    width: 1280,
    height: 720,
    close: vi.fn(),
  } as unknown as ImageBitmap & { name: string; close: ReturnType<typeof vi.fn> };
}

async function flushPromises(): Promise<void> {
  await Promise.resolve();
  await Promise.resolve();
}

describe('Canvas shared-browser controller', () => {
  let controller: CanvasBrowserController;
  let sockets: FakeSocket[];
  let visibility: FakeVisibility;
  let timeouts: FakeTimeouts;
  let bitmapFactory: ReturnType<typeof vi.fn>;
  let originalApiUrl: string;

  beforeEach(() => {
    originalApiUrl = environment.apiUrl;
    environment.apiUrl = 'https://api.example.test/api';
    sockets = [];
    visibility = new FakeVisibility();
    timeouts = new FakeTimeouts();
    bitmapFactory = vi.fn(async () => bitmap(`bitmap-${bitmapFactory.mock.calls.length}`));
    TestBed.configureTestingModule({
      providers: [
        CanvasBrowserController,
        {
          provide: CANVAS_BROWSER_SOCKET_FACTORY,
          useValue: (url: string) => {
            const socket = new FakeSocket(url);
            sockets.push(socket);
            return socket as unknown as WebSocket;
          },
        },
        { provide: CANVAS_BROWSER_BITMAP_FACTORY, useValue: bitmapFactory },
        { provide: CANVAS_BROWSER_TIMEOUTS, useValue: timeouts },
        { provide: CANVAS_BROWSER_VISIBILITY, useValue: visibility },
      ],
    });
    controller = TestBed.inject(CanvasBrowserController);
  });

  afterEach(() => {
    TestBed.resetTestingModule();
    expect(timeouts.pending).toBe(0);
    expect(visibility.listeners.size).toBe(0);
    environment.apiUrl = originalApiUrl;
  });

  it('connects only for an active, visible, positively capable browser source', () => {
    controller.syncPresentation(false, 'thread-1', browserState());
    controller.syncPresentation(
      true,
      'thread-1',
      browserState(1, {
        capabilities: { can_edit: false, can_pop_out: true, can_take_control: true },
      }),
    );
    controller.syncPresentation(
      true,
      'thread-1',
      browserState(1, {
        source: { type: 'workspace_file', path: 'report.md' },
      }),
    );
    expect(sockets).toHaveLength(0);

    controller.syncPresentation(true, 'thread-1', browserState());

    expect(sockets).toHaveLength(1);
    expect(sockets[0].url).toBe(
      'wss://api.example.test/api/persistent/threads/thread-1/browser/stream',
    );
    expect(sockets[0].binaryType).toBe('arraybuffer');
    expect(controller.connectionStatus()).toBe('connecting');
  });

  it('fails closed when the configured API base cannot derive a stream URL', () => {
    environment.apiUrl = 'https://user@api.example.test/api?token=private';

    controller.syncPresentation(true, 'thread-1', browserState());

    expect(sockets).toHaveLength(0);
    expect(controller.connectionStatus()).toBe('error');
    expect(controller.errorCode()).toBe('invalid_browser_stream_url');
  });

  it('requires the first valid STATE and pins frames to its private generation', async () => {
    controller.syncPresentation(true, 'thread-1', browserState());
    const socket = sockets[0];
    socket.message(frame());
    expect(bitmapFactory).not.toHaveBeenCalled();

    socket.message(serverJson(BROWSER_MESSAGE_TYPE.STATE, pageState({ baton: 'user' })));
    expect(controller.connectionStatus()).toBe('ready');
    expect(controller.pageState()).toEqual({
      baton: 'user',
      viewport: { width: 1280, height: 720 },
      url: 'https://example.test/',
      title: 'Example',
      loading: false,
    });
    expect(JSON.stringify(controller.pageState())).not.toContain(GENERATION);

    socket.message(frame(OTHER_GENERATION));
    expect(bitmapFactory).not.toHaveBeenCalled();
    socket.message(frame());
    await flushPromises();
    expect(bitmapFactory).toHaveBeenCalledOnce();
    expect(controller.frame()).not.toBeNull();
  });

  it('ends rather than silently repinning a later STATE generation', async () => {
    controller.syncPresentation(true, 'thread-1', browserState());
    const socket = sockets[0];
    socket.message(serverJson(BROWSER_MESSAGE_TYPE.STATE, pageState()));
    socket.message(frame());
    await flushPromises();
    const current = controller.frame() as ImageBitmap & { close: ReturnType<typeof vi.fn> };

    socket.message(
      serverJson(BROWSER_MESSAGE_TYPE.STATE, {
        ...pageState(),
        generation: OTHER_GENERATION,
      }),
    );

    expect(controller.connectionStatus()).toBe('ended');
    expect(controller.errorCode()).toBe('browser_generation_ended');
    expect(controller.frame()).toBeNull();
    expect(current.close).toHaveBeenCalledOnce();
    expect(socket.closes).toHaveLength(1);
  });

  it('closes the socket and bitmap on source replacement and inactivity', async () => {
    controller.syncPresentation(true, 'thread-1', browserState(1));
    const firstSocket = sockets[0];
    firstSocket.message(serverJson(BROWSER_MESSAGE_TYPE.STATE, pageState()));
    firstSocket.message(frame());
    await flushPromises();
    const firstBitmap = controller.frame() as ImageBitmap & { close: ReturnType<typeof vi.fn> };

    controller.syncPresentation(true, 'thread-1', browserState(2));
    expect(firstSocket.closes).toHaveLength(1);
    expect(firstBitmap.close).toHaveBeenCalledOnce();
    expect(sockets).toHaveLength(2);

    controller.syncPresentation(false, 'thread-1', browserState(2));
    expect(sockets[1].closes).toHaveLength(1);
    expect(controller.connectionStatus()).toBe('idle');
    expect(controller.frame()).toBeNull();
  });

  it('detaches while the document is hidden and re-evaluates on show', () => {
    controller.syncPresentation(true, 'thread-1', browserState());
    expect(sockets).toHaveLength(1);

    visibility.set('hidden');
    expect(sockets[0].closes).toHaveLength(1);
    expect(controller.connectionStatus()).toBe('idle');

    visibility.set('visible');
    expect(sockets).toHaveLength(2);
    expect(controller.connectionStatus()).toBe('connecting');
  });

  it('drops concurrent frames and closes a decoded bitmap from a stale epoch', async () => {
    let resolveBitmap!: (value: ImageBitmap) => void;
    bitmapFactory.mockImplementationOnce(
      () => new Promise<ImageBitmap>((resolve) => (resolveBitmap = resolve)),
    );
    controller.syncPresentation(true, 'thread-1', browserState(1));
    const socket = sockets[0];
    socket.message(serverJson(BROWSER_MESSAGE_TYPE.STATE, pageState()));
    socket.message(frame());
    socket.message(frame());
    expect(bitmapFactory).toHaveBeenCalledOnce();

    controller.syncPresentation(true, 'thread-1', browserState(2));
    const stale = bitmap('stale');
    resolveBitmap(stale);
    await flushPromises();

    expect(stale.close).toHaveBeenCalledOnce();
    expect(controller.frame()).toBeNull();
  });

  it('treats decode rejection as recoverable and continues the stream', async () => {
    bitmapFactory.mockRejectedValueOnce(new Error('decode failed'));
    controller.syncPresentation(true, 'thread-1', browserState());
    const socket = sockets[0];
    socket.message(serverJson(BROWSER_MESSAGE_TYPE.STATE, pageState()));
    socket.message(frame());
    await flushPromises();

    expect(controller.connectionStatus()).toBe('ready');
    expect(controller.errorCode()).toBe('browser_frame_decode_failed');
    expect(socket.closes).toHaveLength(0);

    socket.message(frame());
    await flushPromises();
    expect(controller.frame()).not.toBeNull();
    expect(controller.errorCode()).toBeNull();
  });

  it('uses capped reconnect backoff and resets it after a valid STATE', () => {
    controller.syncPresentation(true, 'thread-1', browserState());
    for (const expected of [250, 500, 1_000, 2_000, 5_000, 5_000]) {
      sockets.at(-1)!.serverClose(4502, false);
      expect(controller.connectionStatus()).toBe('reconnecting');
      expect(timeouts.delays.at(-1)).toBe(expected);
      timeouts.runNext();
    }

    const recovered = sockets.at(-1)!;
    recovered.message(serverJson(BROWSER_MESSAGE_TYPE.STATE, pageState()));
    expect(controller.connectionStatus()).toBe('ready');
    recovered.serverClose(4502, false);
    expect(timeouts.delays.at(-1)).toBe(250);
  });

  it.each([
    [4400, 'error', 'invalid_browser_protocol'],
    [4401, 'unauthorized', 'browser_unauthorized'],
    [4403, 'unauthorized', 'browser_unauthorized'],
    [4404, 'unavailable', 'shared_browser_disabled'],
    [4409, 'ended', 'browser_generation_ended'],
    [4429, 'viewer_limit', 'viewer_limit'],
    [4503, 'unavailable', 'browser_workspace_unavailable'],
    [1000, 'error', 'browser_stream_closed'],
  ] as const)('maps close %s to %s without auto-retry', (code, status, errorCode) => {
    controller.syncPresentation(true, 'thread-1', browserState());
    sockets[0].serverClose(code, true);

    expect(controller.connectionStatus()).toBe(status);
    expect(controller.errorCode()).toBe(errorCode);
    expect(timeouts.pending).toBe(0);
  });

  it('reconnects on an unclean network close and on socket error', () => {
    controller.syncPresentation(true, 'thread-1', browserState());
    sockets[0].serverClose(1006, false);
    expect(controller.connectionStatus()).toBe('reconnecting');
    timeouts.runNext();

    sockets[1].fail();
    expect(controller.connectionStatus()).toBe('reconnecting');
    expect(sockets[1].closes).toHaveLength(1);
    timeouts.runNext();
    expect(sockets).toHaveLength(3);
  });

  it('reconnects after the clean service-restart close used by orchestrator rollouts', () => {
    controller.syncPresentation(true, 'thread-1', browserState());
    sockets[0].serverClose(1012, true);

    expect(controller.connectionStatus()).toBe('reconnecting');
    expect(controller.errorCode()).toBe('browser_stream_unavailable');
    expect(timeouts.delays).toEqual([BROWSER_HANDSHAKE_TIMEOUT_MS, 250]);

    timeouts.runNext();
    expect(sockets).toHaveLength(2);
  });

  it('maps bounded daemon errors without detaching navigation rejection', () => {
    controller.syncPresentation(true, 'thread-1', browserState(1));
    const first = sockets[0];
    first.message(serverJson(BROWSER_MESSAGE_TYPE.STATE, pageState()));
    first.message(
      serverJson(BROWSER_MESSAGE_TYPE.ERROR, {
        code: 'navigation_rejected',
        message: 'Blocked hostname',
      }),
    );
    expect(controller.connectionStatus()).toBe('ready');
    expect(controller.errorCode()).toBe('navigation_rejected');
    expect(controller.errorMessage()).toBe('Blocked hostname');
    expect(first.closes).toHaveLength(0);

    first.message(
      serverJson(BROWSER_MESSAGE_TYPE.ERROR, {
        code: 'browser_gone',
        message: 'Browser ended',
      }),
    );
    expect(controller.connectionStatus()).toBe('ended');

    controller.syncPresentation(true, 'thread-1', browserState(2));
    sockets[1].message(
      serverJson(BROWSER_MESSAGE_TYPE.ERROR, {
        code: 'viewer_limit',
        message: 'Too many viewers',
      }),
    );
    expect(controller.connectionStatus()).toBe('viewer_limit');
  });

  it('gates and encodes mouse, wheel, key, navigation, and baton frames exactly', () => {
    controller.syncPresentation(true, 'thread-1', browserState());
    const socket = sockets[0];
    socket.open();
    socket.message(serverJson(BROWSER_MESSAGE_TYPE.STATE, pageState()));

    expect(controller.sendInput({
      kind: 'mouse',
      params: {type: 'mouseMoved', x: 10, y: 20, modifiers: 0},
    })).toBe(false);
    expect(controller.sendControl({op: 'navigate', url: 'https://example.test/next'})).toBe(false);
    expect(controller.sendControl({op: 'release_baton'})).toBe(false);
    expect(controller.sendControl({op: 'take_baton'})).toBe(true);
    expect(controller.sendControl({op: 'take_baton'})).toBe(false);
    expect(controller.pageState()?.baton).toBe('agent');
    expect(controller.pendingBaton()).toBe('user');
    expect(decodeClient(socket.sent[0])).toEqual({
      type: BROWSER_MESSAGE_TYPE.CONTROL,
      body: {op: 'take_baton'},
    });

    socket.message(serverJson(BROWSER_MESSAGE_TYPE.STATE, pageState({baton: 'user'})));
    expect(controller.pendingBaton()).toBeNull();
    expect(controller.sendInput({
      kind: 'mouse',
      params: {
        type: 'mousePressed',
        x: 100.5,
        y: 200.25,
        button: 'left',
        buttons: 1,
        modifiers: 8,
        clickCount: 1,
      },
    })).toBe(true);
    expect(controller.sendInput({
      kind: 'wheel',
      params: {x: 100.5, y: 200.25, deltaX: 0, deltaY: 32, modifiers: 0},
    })).toBe(true);
    expect(controller.sendInput({
      kind: 'key',
      params: {
        type: 'keyDown',
        key: 'A',
        code: 'KeyA',
        location: 0,
        autoRepeat: false,
        modifiers: 8,
        text: 'A',
      },
    })).toBe(true);
    expect(controller.sendControl({op: 'navigate', url: 'https://example.test/next'})).toBe(true);
    expect(controller.sendControl({op: 'back'})).toBe(true);
    expect(controller.sendControl({op: 'reload'})).toBe(true);
    expect(controller.sendControl({op: 'release_baton'})).toBe(true);
    expect(controller.sendControl({op: 'release_baton'})).toBe(false);
    expect(controller.sendInput({
      kind: 'key',
      params: {type: 'keyUp', key: 'A', code: 'KeyA', location: 0, modifiers: 0},
    })).toBe(false);

    expect(socket.sent.slice(1).map(decodeClient)).toEqual([
      {
        type: BROWSER_MESSAGE_TYPE.INPUT,
        body: {
          kind: 'mouse',
          params: {
            type: 'mousePressed',
            x: 100.5,
            y: 200.25,
            button: 'left',
            buttons: 1,
            modifiers: 8,
            clickCount: 1,
          },
        },
      },
      {
        type: BROWSER_MESSAGE_TYPE.INPUT,
        body: {
          kind: 'wheel',
          params: {x: 100.5, y: 200.25, deltaX: 0, deltaY: 32, modifiers: 0},
        },
      },
      {
        type: BROWSER_MESSAGE_TYPE.INPUT,
        body: {
          kind: 'key',
          params: {
            type: 'keyDown',
            key: 'A',
            code: 'KeyA',
            location: 0,
            autoRepeat: false,
            modifiers: 8,
            text: 'A',
          },
        },
      },
      {type: BROWSER_MESSAGE_TYPE.CONTROL, body: {op: 'navigate', url: 'https://example.test/next'}},
      {type: BROWSER_MESSAGE_TYPE.CONTROL, body: {op: 'back'}},
      {type: BROWSER_MESSAGE_TYPE.CONTROL, body: {op: 'reload'}},
      {type: BROWSER_MESSAGE_TYPE.CONTROL, body: {op: 'release_baton'}},
    ]);
  });

  it('allows two same-user views to drive only after each receives authoritative user STATE', () => {
    const second = TestBed.runInInjectionContext(() => new CanvasBrowserController());
    controller.syncPresentation(true, 'thread-1', browserState());
    second.syncPresentation(true, 'thread-1', browserState());
    const firstSocket = sockets[0];
    const secondSocket = sockets[1];
    firstSocket.open();
    secondSocket.open();

    firstSocket.message(serverJson(BROWSER_MESSAGE_TYPE.STATE, pageState({baton: 'user'})));
    expect(controller.sendInput({
      kind: 'mouse',
      params: {type: 'mouseMoved', x: 1, y: 2},
    })).toBe(true);
    expect(second.sendInput({
      kind: 'mouse',
      params: {type: 'mouseMoved', x: 3, y: 4},
    })).toBe(false);

    secondSocket.message(serverJson(BROWSER_MESSAGE_TYPE.STATE, pageState({baton: 'user'})));
    expect(second.sendInput({
      kind: 'mouse',
      params: {type: 'mouseMoved', x: 3, y: 4},
    })).toBe(true);
    expect(firstSocket.sent).toHaveLength(1);
    expect(secondSocket.sent).toHaveLength(1);
  });

  it('rejects input after visibility teardown or source replacement', () => {
    controller.syncPresentation(true, 'thread-1', browserState(1));
    const first = sockets[0];
    first.open();
    first.message(serverJson(BROWSER_MESSAGE_TYPE.STATE, pageState({baton: 'user'})));
    visibility.set('hidden');
    expect(controller.sendInput({
      kind: 'mouse',
      params: {type: 'mouseMoved', x: 1, y: 2},
    })).toBe(false);

    visibility.set('visible');
    controller.syncPresentation(true, 'thread-1', browserState(2));
    expect(controller.sendInput({
      kind: 'mouse',
      params: {type: 'mouseMoved', x: 1, y: 2},
    })).toBe(false);
    expect(first.sent).toHaveLength(0);
  });

  it('treats malformed server data as terminal and supports explicit retry', () => {
    controller.syncPresentation(true, 'thread-1', browserState());
    sockets[0].message('not binary');
    expect(controller.connectionStatus()).toBe('error');
    expect(controller.errorCode()).toBe('invalid_browser_protocol');

    controller.retry();
    expect(sockets).toHaveLength(2);
    expect(controller.connectionStatus()).toBe('connecting');
  });

  it('gives up on a handshake that never completes and reconnects', () => {
    controller.syncPresentation(true, 'thread-1', browserState());
    const stuck = sockets[0];
    expect(timeouts.delays).toEqual([BROWSER_HANDSHAKE_TIMEOUT_MS]);

    timeouts.runNext();

    expect(stuck.closes).toHaveLength(1);
    expect(controller.connectionStatus()).toBe('reconnecting');
    expect(controller.errorCode()).toBe('browser_stream_handshake_timeout');
    expect(timeouts.delays.at(-1)).toBe(250);
    stuck.open();
    stuck.message(serverJson(BROWSER_MESSAGE_TYPE.STATE, pageState()));
    expect(controller.connectionStatus()).toBe('reconnecting');

    timeouts.runNext();
    expect(sockets).toHaveLength(2);
    sockets[1].open();
    expect(timeouts.pending).toBe(0);
    sockets[1].message(serverJson(BROWSER_MESSAGE_TYPE.STATE, pageState()));
    expect(controller.connectionStatus()).toBe('ready');
  });

  it('keeps a slow but opened handshake and a message-first socket alive', () => {
    controller.syncPresentation(true, 'thread-1', browserState());
    sockets[0].open();
    expect(timeouts.pending).toBe(0);

    controller.syncPresentation(true, 'thread-1', browserState(2));
    expect(timeouts.pending).toBe(1);
    sockets[1].message(serverJson(BROWSER_MESSAGE_TYPE.STATE, pageState()));
    expect(timeouts.pending).toBe(0);
    expect(sockets[1].closes).toHaveLength(0);
  });

  it('destroys with no live listener, timer, socket, or bitmap', async () => {
    controller.syncPresentation(true, 'thread-1', browserState());
    const socket = sockets[0];
    socket.message(serverJson(BROWSER_MESSAGE_TYPE.STATE, pageState()));
    socket.message(frame());
    await flushPromises();
    const current = controller.frame() as ImageBitmap & { close: ReturnType<typeof vi.fn> };
    socket.serverClose(4502, false);
    expect(timeouts.pending).toBe(1);

    TestBed.resetTestingModule();

    expect(timeouts.pending).toBe(0);
    expect(visibility.listeners.size).toBe(0);
    expect(current.close).toHaveBeenCalledOnce();
  });
});
