import {
  APIRequestContext,
  BrowserContext,
  Page,
  WebSocketRoute,
  expect,
  test,
} from '@playwright/test';

const BASE_URL = 'http://127.0.0.1:4173';
const THREAD_ID = '11111111-1111-4111-8111-111111111111';
const SESSION_PATH = `/sessions/${THREAD_ID}`;
const STREAM_PATH = `/api/persistent/threads/${THREAD_ID}/browser/stream`;
const STREAM_ROUTE = new RegExp(`${STREAM_PATH.replaceAll('/', '\\/')}$`);
const PARENT_SESSION = 'parent-session-secret-not-for-browser-stream';
const GENERATION_ONE = '70000000-0000-4000-8000-000000000001';
const GENERATION_TWO = '70000000-0000-4000-8000-000000000002';
const GENERATION_THREE = '70000000-0000-4000-8000-000000000003';
const MESSAGE = Object.freeze({FRAME: 2, STATE: 3, INPUT: 4, CONTROL: 5, ERROR: 6});
const STATE_VIEWPORT = Object.freeze({width: 800, height: 400});
// The CSS size every FRAME header reports. It deliberately disagrees with the
// decoded 8x8 JPEG, which stays authoritative for the backing store, and with
// STATE_VIEWPORT: viewer input maps onto the size of the page in the frame on
// screen, so a pointer position must come out in this space, not STATE's.
const FRAME_HEADER_SIZE = Object.freeze({width: 999, height: 777});
const KNOWN_RED_JPEG = Buffer.from(
  '/9j/4AAQSkZJRgABAQAAAQABAAD/2wBDAAEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQH/2wBDAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQH/wAARCAAIAAgDAREAAhEBAxEB/8QAHwAAAQUBAQEBAQEAAAAAAAAAAAECAwQFBgcICQoL/8QAtRAAAgEDAwIEAwUFBAQAAAF9AQIDAAQRBRIhMUEGE1FhByJxFDKBkaEII0KxwRVS0fAkM2JyggkKFhcYGRolJicoKSo0NTY3ODk6Q0RFRkdISUpTVFVWV1hZWmNkZWZnaGlqc3R1dnd4eXqDhIWGh4iJipKTlJWWl5iZmqKjpKWmp6ipqrKztLW2t7i5usLDxMXGx8jJytLT1NXW19jZ2uHi4+Tl5ufo6erx8vP09fb3+Pn6/8QAHwEAAwEBAQEBAQEBAQAAAAAAAAECAwQFBgcICQoL/8QAtREAAgECBAQDBAcFBAQAAQJ3AAECAxEEBSExBhJBUQdhcRMiMoEIFEKRobHBCSMzUvAVYnLRChYkNOEl8RcYGRomJygpKjU2Nzg5OkNERUZHSElKU1RVVldYWVpjZGVmZ2hpanN0dXZ3eHl6goOEhYaHiImKkpOUlZaXmJmaoqOkpaanqKmqsrO0tba3uLm6wsPExcbHyMnK0tPU1dbX2Nna4uPk5ebn6Onq8vP09fb3+Pn6/9oADAMBAAIRAxEAPwD856/znP8ArYP/2Q==',
  'base64',
);

interface SafeRequest {
  method: string;
  path: string;
  headerNames: string[];
  cookieNames: string[];
  hasAuthorization: boolean;
  hasCsrf: boolean;
  ifMatch: string | null;
  origin: string | null;
}

interface FixtureState {
  browserOpened: boolean;
  browserOpenCount: number;
  presentationRevision: number;
  requests: SafeRequest[];
  browserOpen: Array<{bodyKeys: string[]}>;
}

interface ClientMessage {
  type: number;
  body: Record<string, unknown>;
}

interface RoutedBrowserSocket {
  readonly route: WebSocketRoute;
  readonly url: string;
  readonly clientBuffers: Buffer[];
  closed: boolean;
  closeCode: number | undefined;
}

class BrowserSocketHarness {
  readonly sockets: RoutedBrowserSocket[] = [];

  async install(context: BrowserContext, page: Page): Promise<void> {
    const handler = (route: WebSocketRoute): void => this.attach(route);
    // Page routing is the primary production-page mock required by this gate.
    // The context route gives the separately-created popout its own server.
    await context.routeWebSocket(STREAM_ROUTE, handler);
    await page.routeWebSocket(STREAM_ROUTE, handler);
  }

  async next(index: number): Promise<RoutedBrowserSocket> {
    await expect.poll(() => this.sockets.length).toBeGreaterThan(index);
    return this.sockets[index];
  }

  sendState(
    socket: RoutedBrowserSocket,
    generation: string,
    overrides: Partial<{
      baton: 'agent' | 'user';
      viewport: {width: number; height: number};
      url: string | null;
      title: string | null;
      loading: boolean;
    }> = {},
  ): void {
    socket.route.send(serverJson(MESSAGE.STATE, {
      generation,
      baton: 'agent',
      viewport: STATE_VIEWPORT,
      url: 'https://workspace.example.test/start',
      title: 'Workspace start',
      loading: false,
      ...overrides,
    }));
  }

  sendFrame(socket: RoutedBrowserSocket, generation: string): void {
    // Header dimensions deliberately disagree with the JPEG and with STATE
    // (see FRAME_HEADER_SIZE): the decoded 8x8 image must remain authoritative
    // for the backing store, and input must map onto the header's size.
    const header = Buffer.from(JSON.stringify({
      generation,
      w: FRAME_HEADER_SIZE.width,
      h: FRAME_HEADER_SIZE.height,
      ts: 1_753_200_000.25,
    }));
    const prefix = Buffer.alloc(3);
    prefix[0] = MESSAGE.FRAME;
    prefix.writeUInt16BE(header.length, 1);
    socket.route.send(Buffer.concat([prefix, header, KNOWN_RED_JPEG]));
  }

  sendError(socket: RoutedBrowserSocket, code: string, message: string): void {
    socket.route.send(serverJson(MESSAGE.ERROR, {code, message}));
  }

  async close(socket: RoutedBrowserSocket, code: number, reason: string): Promise<void> {
    socket.closed = true;
    socket.closeCode = code;
    await socket.route.close({code, reason});
  }

  decoded(socket: RoutedBrowserSocket): ClientMessage[] {
    return socket.clientBuffers.map(buffer => ({
      type: buffer[0],
      body: JSON.parse(buffer.subarray(1).toString('utf8')) as Record<string, unknown>,
    }));
  }

  private attach(route: WebSocketRoute): void {
    const socket: RoutedBrowserSocket = {
      route,
      url: route.url(),
      clientBuffers: [],
      closed: false,
      closeCode: undefined,
    };
    this.sockets.push(socket);
    route.onMessage(message => {
      if (!Buffer.isBuffer(message)) {
        throw new Error('Shared-browser client sent a non-binary WebSocket message');
      }
      socket.clientBuffers.push(message);
    });
    route.onClose((code, reason) => {
      socket.closed = true;
      socket.closeCode = code;
      void route.close({code: code || 1000, reason});
    });
  }
}

test.describe('Shared browser Cockpit handoff', () => {
  test.beforeEach(async ({context, request}) => {
    const reset = await request.post('/__e2e/reset', {data: {scenario: 'shared-browser'}});
    expect(reset.ok()).toBe(true);
    await context.addCookies([{
      name: 'srw_session',
      value: PARENT_SESSION,
      url: BASE_URL,
      httpOnly: true,
      sameSite: 'Lax',
    }]);
    await context.addInitScript(() => {
      const key = '__canvasBroadcastMessages';
      (globalThis as typeof globalThis & Record<string, unknown>)[key] = [];
      if (typeof BroadcastChannel !== 'function') return;
      const original = BroadcastChannel.prototype.postMessage;
      BroadcastChannel.prototype.postMessage = function(message: unknown): void {
        const target = (globalThis as typeof globalThis & Record<string, unknown>)[key];
        if (Array.isArray(target)) target.push({channel: this.name, message});
        original.call(this, message);
      };
    });
  });

  test('opens, drives, fans out, detaches, and replaces a generation safely', async ({
    context,
    page,
    request,
  }) => {
    const consoleMessages: string[] = [];
    page.on('console', message => consoleMessages.push(message.text()));
    const harness = new BrowserSocketHarness();
    await harness.install(context, page);

    await page.goto(SESSION_PATH, {waitUntil: 'load'});
    const openBrowser = page.getByRole('button', {name: 'Open browser', exact: true}).first();
    await expect(openBrowser).toBeVisible();
    expect(await fixtureState(request)).toMatchObject({
      browserOpened: false,
      browserOpenCount: 0,
    });

    await openBrowser.click();
    const firstSocket = await harness.next(0);
    expect(new URL(firstSocket.url).pathname).toBe(STREAM_PATH);
    expect(new URL(firstSocket.url).search).toBe('');

    const opened = await fixtureState(request);
    expect(opened.browserOpenCount).toBe(1);
    expect(opened.browserOpen).toEqual([{bodyKeys: ['title']}]);
    const openRequest = opened.requests.find(entry => entry.path.endsWith('/browser/open'));
    expect(openRequest?.method).toBe('POST');
    expect(openRequest?.cookieNames).toContain('srw_session');
    expect(openRequest?.hasCsrf).toBe(true);
    expect(openRequest?.hasAuthorization).toBe(false);

    harness.sendState(firstSocket, GENERATION_ONE, {
      loading: true,
      title: 'Loading shared page',
      url: 'https://workspace.example.test/loading',
    });
    await expect(page.locator('.browser-title')).toHaveText('Loading shared page');
    await expect(page.getByText('Page loading…', {exact: true})).toBeVisible();
    harness.sendState(firstSocket, GENERATION_ONE, {
      loading: false,
      title: 'Known red page',
      url: 'https://workspace.example.test/known-red',
    });
    harness.sendFrame(firstSocket, GENERATION_ONE);

    const surface = page.locator('canvas[aria-label="Shared browser page"]');
    await expect(surface).toBeVisible();
    await expect.poll(() => canvasPixel(surface)).toEqual([221, 40, 31, 255]);
    expect(await surface.evaluate(element => {
      const canvas = element as HTMLCanvasElement;
      return {width: canvas.width, height: canvas.height};
    })).toEqual({
      width: 8,
      height: 8,
    });
    await expect(page.getByLabel('Address')).toHaveValue(
      'https://workspace.example.test/known-red',
    );

    const take = page.getByRole('button', {name: 'Take control', exact: true});
    await take.click();
    await expect.poll(() => harness.decoded(firstSocket)).toContainEqual({
      type: MESSAGE.CONTROL,
      body: {op: 'take_baton'},
    });
    await expect(take).toBeDisabled();
    await expect(take).toHaveAttribute('aria-busy', 'true');
    await expect(page.getByText('Agent is driving', {exact: true})).toBeVisible();
    harness.sendState(firstSocket, GENERATION_ONE, {
      baton: 'user',
      title: 'Known red page',
      url: 'https://workspace.example.test/known-red',
    });
    const release = page.getByRole('button', {name: 'Release control', exact: true});
    await expect(release).toBeEnabled();
    await expect(release).toHaveAttribute('aria-pressed', 'true');

    // A click a quarter across and three quarters down the surface lands at
    // that fraction of the frame header's CSS size, not of STATE's viewport.
    const mapped = {x: FRAME_HEADER_SIZE.width / 4, y: FRAME_HEADER_SIZE.height * 3 / 4};
    await surface.evaluate(canvas => {
      const bounds = canvas.getBoundingClientRect();
      const clientX = bounds.left + bounds.width / 4;
      const clientY = bounds.top + bounds.height * 3 / 4;
      canvas.dispatchEvent(new PointerEvent('pointerdown', {
        bubbles: true,
        pointerId: 7,
        isPrimary: true,
        clientX,
        clientY,
        button: 0,
        buttons: 1,
        detail: 1,
      }));
      canvas.dispatchEvent(new PointerEvent('pointerup', {
        bubbles: true,
        pointerId: 7,
        isPrimary: true,
        clientX,
        clientY,
        button: 0,
        buttons: 0,
        detail: 1,
      }));
    });
    await surface.focus();
    await page.keyboard.down('a');
    await page.keyboard.up('a');
    await expect.poll(() => harness.decoded(firstSocket)).toContainEqual({
      type: MESSAGE.INPUT,
      body: {
        kind: 'mouse',
        params: {
          type: 'mousePressed',
          x: mapped.x,
          y: mapped.y,
          button: 'left',
          buttons: 1,
          modifiers: 0,
          clickCount: 1,
        },
      },
    });
    // The virtual key codes are load-bearing, not incidental: without them the
    // remote page sees keyCode 0 and fires no default action, which is what
    // made Enter dead in the shared browser. toContainEqual is a deep equality,
    // so this assertion is also what keeps them from being dropped again.
    await expect.poll(() => harness.decoded(firstSocket)).toContainEqual({
      type: MESSAGE.INPUT,
      body: {
        kind: 'key',
        params: {
          type: 'keyDown',
          key: 'a',
          code: 'KeyA',
          location: 0,
          autoRepeat: false,
          modifiers: 0,
          windowsVirtualKeyCode: 65,
          nativeVirtualKeyCode: 65,
          text: 'a',
        },
      },
    });

    const address = page.getByLabel('Address');
    await address.fill('https://workspace.example.test/rejected');
    await address.press('Enter');
    await expect.poll(() => harness.decoded(firstSocket)).toContainEqual({
      type: MESSAGE.CONTROL,
      body: {op: 'navigate', url: 'https://workspace.example.test/rejected'},
    });
    harness.sendError(firstSocket, 'navigation_rejected', 'Blocked hostname');
    await expect(page.locator('.browser-navigation-error')).toContainText('Blocked hostname');

    await release.click();
    await expect.poll(() => harness.decoded(firstSocket)).toContainEqual({
      type: MESSAGE.CONTROL,
      body: {op: 'release_baton'},
    });
    await expect(release).toBeDisabled();
    await expect(page.getByText("You're driving", {exact: true})).toBeVisible();
    harness.sendState(firstSocket, GENERATION_ONE, {
      baton: 'agent',
      title: 'Known red page',
      url: 'https://workspace.example.test/known-red',
    });
    await expect(page.getByText('Agent is driving', {exact: true})).toBeVisible();
    expect(firstSocket.clientBuffers.every(Buffer.isBuffer)).toBe(true);

    await page.getByRole('button', {name: 'Hide Canvas', exact: true}).click();
    await expect.poll(() => firstSocket.closed).toBe(true);
    await page.getByRole('button', {name: 'Open Canvas', exact: true}).click();
    const revealedSocket = await harness.next(1);
    harness.sendState(revealedSocket, GENERATION_ONE, {
      title: 'Known red page',
      url: 'https://workspace.example.test/known-red',
    });
    harness.sendFrame(revealedSocket, GENERATION_ONE);
    await expect.poll(() => canvasPixel(surface)).toEqual([221, 40, 31, 255]);

    const popupPromise = context.waitForEvent('page');
    await page.getByRole('button', {name: 'Open Canvas in a new window', exact: true}).click();
    const popup = await popupPromise;
    popup.on('console', message => consoleMessages.push(message.text()));
    await popup.waitForLoadState('domcontentloaded');
    const popoutSocket = await harness.next(2);
    harness.sendState(revealedSocket, GENERATION_ONE, {
      baton: 'user',
      title: 'Two-view browser',
      url: 'https://workspace.example.test/two-views',
    });
    harness.sendState(popoutSocket, GENERATION_ONE, {
      baton: 'user',
      title: 'Two-view browser',
      url: 'https://workspace.example.test/two-views',
    });
    harness.sendFrame(revealedSocket, GENERATION_ONE);
    harness.sendFrame(popoutSocket, GENERATION_ONE);
    const popoutSurface = popup.locator('canvas[aria-label="Shared browser page"]');
    await expect.poll(() => canvasPixel(surface)).toEqual([221, 40, 31, 255]);
    await expect.poll(() => canvasPixel(popoutSurface)).toEqual([221, 40, 31, 255]);
    await expect(page.getByText("You're driving", {exact: true})).toBeVisible();
    await expect(popup.getByText("You're driving", {exact: true})).toBeVisible();

    await popup.close();
    await expect.poll(() => popoutSocket.closed || popup.isClosed()).toBe(true);
    expect(revealedSocket.closed).toBe(false);
    await expect(page.locator('.browser-title')).toHaveText('Two-view browser');

    await harness.close(revealedSocket, 4409, 'Browser generation replaced');
    await expect(page.locator('.browser-empty-state[role="alert"]')).toContainText(
      'This browser session has ended.',
    );
    await expect.poll(() => canvasPixel(surface)).toEqual([0, 0, 0, 0]);
    await page.getByRole('button', {name: 'Restart browser', exact: true}).click();
    const replacementSocket = await harness.next(3);
    harness.sendState(replacementSocket, GENERATION_TWO, {
      title: 'Replacement browser',
      url: 'https://workspace.example.test/replacement',
    });
    harness.sendFrame(replacementSocket, GENERATION_TWO);
    await expect(page.locator('.browser-title')).toHaveText('Replacement browser');
    await expect.poll(() => canvasPixel(surface)).toEqual([221, 40, 31, 255]);
    expect((await fixtureState(request)).presentationRevision).toBe(2);

    // A malformed frame fails terminally and clears old pixels. The trusted
    // shell remains responsive and can stage another authoritative revision.
    replacementSocket.route.send(Buffer.from([MESSAGE.FRAME, 0, 0, 0xff, 0xd8]));
    await expect(page.locator('.browser-empty-state[role="alert"]')).toContainText(
      'protocol error',
    );
    await expect.poll(() => canvasPixel(surface)).toEqual([0, 0, 0, 0]);
    await page.getByRole('button', {name: 'Open browser', exact: true}).first().click();
    const oversizedSocket = await harness.next(4);
    harness.sendState(oversizedSocket, GENERATION_THREE, {
      title: 'Bounded browser',
      url: 'https://workspace.example.test/bounded',
    });
    harness.sendFrame(oversizedSocket, GENERATION_THREE);
    await expect.poll(() => canvasPixel(surface)).toEqual([221, 40, 31, 255]);
    const oversized = Buffer.alloc(8 * 1024 * 1024 + 1);
    oversized[0] = MESSAGE.FRAME;
    oversizedSocket.route.send(oversized);
    await expect(page.locator('.browser-empty-state[role="alert"]')).toContainText(
      'protocol error',
    );
    await expect.poll(() => canvasPixel(surface)).toEqual([0, 0, 0, 0]);
    await expect(page.getByRole('button', {name: 'Open browser', exact: true}).first()).toBeEnabled();

    const finalFixture = await fixtureState(request);
    expect(finalFixture.browserOpenCount).toBe(3);
    const privateValues = [GENERATION_ONE, GENERATION_TWO, GENERATION_THREE];
    const publicText = [
      await page.locator('body').innerText(),
      page.url(),
      JSON.stringify(await page.evaluate(() => ({
        localStorage: {...localStorage},
        sessionStorage: {...sessionStorage},
      }))),
      JSON.stringify(finalFixture),
      consoleMessages.join('\n'),
    ].join('\n');
    for (const generation of privateValues) expect(publicText).not.toContain(generation);

    const broadcasts = await page.evaluate(() => (
      (globalThis as typeof globalThis & {__canvasBroadcastMessages?: unknown[]})
        .__canvasBroadcastMessages ?? []
    ));
    const canvasBroadcasts = broadcasts.filter(entry => (
      typeof entry === 'object' &&
      entry !== null &&
      (entry as Record<string, unknown>)['channel'] === 'srw.canvas.presentation.v1'
    ));
    expect(canvasBroadcasts.length).toBeGreaterThan(0);
    expect(canvasBroadcasts.every(entry => (
      typeof (entry as Record<string, unknown>)['message'] === 'object' &&
      (entry as Record<string, unknown>)['message'] !== null &&
      ((entry as Record<string, unknown>)['message'] as Record<string, unknown>)['type'] ===
        'canvas.presentation_invalidated'
    ))).toBe(true);
    expect(JSON.stringify(canvasBroadcasts)).not.toContain('generation');
    expect(JSON.stringify(canvasBroadcasts)).not.toContain('jpeg');
  });
});

function serverJson(type: number, value: unknown): Buffer {
  return Buffer.concat([Buffer.from([type]), Buffer.from(JSON.stringify(value), 'utf8')]);
}

async function fixtureState(request: APIRequestContext): Promise<FixtureState> {
  const response = await request.get('/__e2e/state');
  expect(response.ok()).toBe(true);
  return (await response.json()) as FixtureState;
}

async function canvasPixel(locator: ReturnType<Page['locator']>): Promise<number[]> {
  return locator.evaluate(element => {
    const canvas = element as HTMLCanvasElement;
    const context = canvas.getContext('2d');
    if (!context || canvas.width < 1 || canvas.height < 1) return [];
    return [...context.getImageData(
      Math.floor(canvas.width / 2),
      Math.floor(canvas.height / 2),
      1,
      1,
    ).data];
  });
}
