import {mkdtemp, rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {chromium, firefox, expect, test as base, type Page, type BrowserContext} from '@playwright/test';

function required(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`${name} must explicitly target a disposable single-origin deployment.`);
  return value;
}

export function target(): string {
  const url = new URL(required('SRW_SINGLE_ORIGIN_URL'));
  if (url.protocol !== 'https:' || url.username || url.password ||
      url.pathname !== '/' || url.search || url.hash) {
    throw new Error('SRW_SINGLE_ORIGIN_URL must be a bare HTTPS origin.');
  }
  return url.origin;
}

export async function browserJson(page: Page, path: string, method = 'GET') {
  // APIRequestContext does not inherit a browser certificate exception.
  // Fetch in the actual page, with the same TLS validation and cookie jar.
  return page.evaluate(async ({path, method}) => {
    const response = await fetch(path, {
      method, credentials: 'same-origin',
      headers: method === 'GET' ? {} : {'X-CSRF': '1'},
    });
    const text = await response.text();
    return {status: response.status, body: text ? JSON.parse(text) : null};
  }, {path, method});
}

export async function login(page: Page): Promise<void> {
  await page.goto(`${target()}/auth/login?return_to=/`, {waitUntil: 'domcontentloaded'});
  // Identity is on the SAME origin. An origin comparison cannot detect the
  // Keycloak form; check its path instead, including when SSO skips it.
  if (new URL(page.url()).pathname.startsWith('/identity/')) {
    await expect(page.locator('#username')).toBeVisible();
    await page.locator('#username').fill(required('SRW_SINGLE_ORIGIN_USERNAME'));
    await page.locator('#password').fill(required('SRW_SINGLE_ORIGIN_PASSWORD'));
    await page.locator('#kc-login').click();
  }
  await expect.poll(async () => (await page.context().cookies(target()))
    .some(cookie => cookie.name === 'srw_session')).toBe(true);
  await page.waitForURL(url => url.origin === target() && !url.pathname.startsWith('/identity/'));
  expect((await browserJson(page, '/auth/me')).status).toBe(200);
}

type ProviderRunState = {
  run_id: string;
  scenario: string;
  required_responses: number;
  consumed_required_responses: number;
  reserved_required_responses: number;
  remaining_required_responses: number;
  unexpected_count: number;
  pending_calls: number;
};

type ProviderOverview = {
  runs: ProviderRunState[];
  unscoped_unexpected_calls: number;
};

function providerControlOrigin(): string {
  const url = new URL(required('SRW_SINGLE_ORIGIN_PROVIDER_CONTROL_URL'));
  const loopbackHosts = new Set(['localhost', '127.0.0.1', '[::1]']);
  if (url.protocol !== 'http:' || !loopbackHosts.has(url.hostname.toLowerCase()) ||
      url.username || url.password || url.pathname !== '/' || url.search || url.hash) {
    throw new Error('SRW_SINGLE_ORIGIN_PROVIDER_CONTROL_URL must be a bare HTTP loopback origin.');
  }
  return url.origin;
}

async function providerJson<T>(origin: string, token: string, path: string, init: RequestInit = {}): Promise<T> {
  const response = await fetch(new URL(path, `${origin}/`), {
    ...init,
    headers: {
      Accept: 'application/json',
      Authorization: `Bearer ${token}`,
      ...(init.body === undefined ? {} : {'Content-Type': 'application/json'}),
    },
    signal: AbortSignal.timeout(30_000),
  });
  if (!response.ok) throw new Error(`Provider control ${init.method ?? 'GET'} ${path} returned ${response.status}.`);
  return await response.json() as T;
}

function providerOverview(value: unknown): ProviderOverview {
  if (typeof value !== 'object' || value === null) throw new Error('Provider control returned an invalid overview.');
  const overview = value as Partial<ProviderOverview>;
  if (!Array.isArray(overview.runs) || !Number.isSafeInteger(overview.unscoped_unexpected_calls) ||
      (overview.unscoped_unexpected_calls ?? -1) < 0) {
    throw new Error('Provider control returned an invalid overview.');
  }
  return overview as ProviderOverview;
}

function assertProviderRun(state: ProviderRunState, runId: string): void {
  const expected: Partial<ProviderRunState> = {
    run_id: runId,
    scenario: 'reply',
    required_responses: 1,
    consumed_required_responses: 1,
    reserved_required_responses: 0,
    remaining_required_responses: 0,
    unexpected_count: 0,
    pending_calls: 0,
  };
  const mismatches = Object.entries(expected)
    .filter(([key, value]) => state[key as keyof ProviderRunState] !== value)
    .map(([key, value]) => `${key}=${JSON.stringify(state[key as keyof ProviderRunState])} (expected ${JSON.stringify(value)})`);
  if (mismatches.length) throw new Error(`Provider run ${runId} failed accounting: ${mismatches.join(', ')}.`);
}

function combinedError(message: string, errors: unknown[]): Error {
  return new Error(`${message}: ${errors.map(error => error instanceof Error ? error.message : String(error)).join('; ')}`);
}

type Fixtures = {
  trustedPage: Page;
  origins: Set<string>;
  cookieHeaders: string[];
  workerRequests: string[];
  providerRunId: string;
};
export const test = base.extend<Fixtures>({
  origins: async ({}, use) => { await use(new Set()); },
  cookieHeaders: async ({}, use) => { await use([]); },
  workerRequests: async ({}, use) => { await use([]); },
  trustedPage: async ({origins, cookieHeaders, workerRequests}, use, testInfo) => {
    const origin = target();
    const profile = await mkdtemp(join(tmpdir(), 'srw-single-origin-browser-'));
    const isFirefox = testInfo.project.name === 'firefox';
    const engine = isFirefox ? firefox : chromium;
    let context: BrowserContext | undefined;
    const consoleMessages: string[] = [];
    const headerReads: Promise<void>[] = [];
    try {
      context = await engine.launchPersistentContext(profile, {
        headless: process.env['SRW_SINGLE_ORIGIN_HEADED'] !== '1',
        ignoreHTTPSErrors: false,
        serviceWorkers: 'allow',
        viewport: {width: 1440, height: 960},
        ...(isFirefox ? {} : {executablePath: process.env['SRW_CHROMIUM_EXECUTABLE']}),
      });
      context.setDefaultTimeout(30_000);
      context.setDefaultNavigationTimeout(60_000);
      await context.tracing.start({screenshots: true, snapshots: true, sources: true});
      const observe = (url: string) => {
        const parsed = new URL(url);
        if (['https:', 'http:', 'wss:', 'ws:'].includes(parsed.protocol)) {
          origins.add(parsed.origin.replace(/^ws/, 'http'));
        }
      };
      // Track application navigations/transports. Optional font/image assets
      // may use trusted third-party CDNs and are not backend trust surfaces.
      context.on('response', response => {
        if (new URL(response.url()).pathname === '/auth/callback') {
          headerReads.push(response.headersArray().then(headers => {
            for (const header of headers) {
              if (header.name.toLowerCase() === 'set-cookie') cookieHeaders.push(header.value);
            }
          }));
        }
      });
      context.on('request', request => {
        if (new URL(request.url()).pathname.endsWith('/ngsw-worker.js')) workerRequests.push(request.url());
        if (['document', 'fetch', 'xhr', 'eventsource'].includes(request.resourceType())) observe(request.url());
      });
      const watchPage = (page: Page) => {
        page.on('websocket', socket => observe(socket.url()));
        page.on('console', message => {
          if (['error', 'warning'].includes(message.type())) consoleMessages.push(`${message.type()}: ${message.text()}`);
        });
        page.on('pageerror', error => consoleMessages.push(`pageerror: ${error.message}`));
      };
      context.on('page', watchPage);
      const page = context.pages()[0] ?? await context.newPage();
      watchPage(page);
      try {
        await page.goto(origin, {waitUntil: 'domcontentloaded'}).catch(error => {
          // A certificate failure is expected only for this initial navigation.
          if (!/CERT|SSL|SEC_ERROR|NS_ERROR_FAILURE/i.test(String(error))) throw error;
        });
        await page.screenshot({path: testInfo.outputPath('certificate-warning.png')});
        if (isFirefox) {
          await page.locator('#advancedButton').click();
          const proceed = page.locator('#exceptionDialogButton, #acceptAndContinueButton').filter({visible: true});
          await expect(proceed).toHaveCount(1);
          await proceed.click();
        } else {
          await page.locator('#details-button').click();
          await page.locator('#proceed-link').click();
        }
        await page.waitForURL(url => url.origin === origin);
        testInfo.annotations.push({type: 'browser', description: context.browser()?.version() ?? testInfo.project.name});
        // There are no further warning clicks in any journey.
        await use(page);
        await Promise.all(headerReads);
      } finally {
        await Promise.allSettled([
          testInfo.attach('observed-origins', {body: JSON.stringify([...origins].sort(), null, 2), contentType: 'application/json'}),
          testInfo.attach('browser-console', {body: consoleMessages.join('\n'), contentType: 'text/plain'}),
          testInfo.attach('service-worker-requests', {body: JSON.stringify(workerRequests), contentType: 'application/json'}),
        ]);
        if (testInfo.status !== testInfo.expectedStatus) {
          await page.screenshot({path: testInfo.outputPath('failure.png')}).catch(() => undefined);
        }
        await context.tracing.stop({path: testInfo.outputPath('trace.zip')}).catch(() => undefined);
      }
    } finally {
      try { await context?.close(); }
      finally { await rm(profile, {recursive: true, force: true}); }
    }
  },
  providerRunId: async ({}, use, testInfo) => {
    const prefix = required('SRW_SINGLE_ORIGIN_RUN_ID');
    const runId = `${prefix}-${testInfo.project.name}`;
    if (!/^[A-Za-z0-9][A-Za-z0-9_-]{2,127}$/.test(runId)) {
      throw new Error('SRW_SINGLE_ORIGIN_RUN_ID plus the Playwright project name must form a valid provider run ID.');
    }
    const origin = providerControlOrigin();
    const token = required('SRW_SINGLE_ORIGIN_PROVIDER_CONTROL_TOKEN');
    const initial = providerOverview(await providerJson<unknown>(origin, token, '/control/scenarios'));
    if (initial.runs.length) {
      throw new Error(`Provider control already has active runs (${initial.runs.map(run => run.run_id).join(', ')}); refusing to alter them.`);
    }
    const initialUnscoped = initial.unscoped_unexpected_calls;
    const path = `/control/scenarios/${encodeURIComponent(runId)}`;
    await providerJson(origin, token, `${path}/arm`, {
      method: 'POST',
      body: JSON.stringify({scenario: 'reply', required_responses: 1}),
    });

    const teardownErrors: unknown[] = [];
    let bodyThrew = true;
    try {
      await use(runId);
      bodyThrew = false;
    } finally {
      try {
        const state = await providerJson<ProviderRunState>(origin, token, path);
        assertProviderRun(state, runId);
      } catch (error) {
        teardownErrors.push(error);
      }
      try {
        const final = providerOverview(await providerJson<unknown>(origin, token, '/control/scenarios'));
        const activeRunIds = final.runs.map(run => run.run_id);
        if (activeRunIds.length !== 1 || activeRunIds[0] !== runId) {
          throw new Error(`Provider active-run set changed during ${runId}: ${activeRunIds.join(', ') || '(none)'}.`);
        }
        if (final.unscoped_unexpected_calls !== initialUnscoped) {
          throw new Error(`Provider unscoped call count changed from ${initialUnscoped} to ${final.unscoped_unexpected_calls}.`);
        }
      } catch (error) {
        teardownErrors.push(error);
      }
      try {
        await providerJson(origin, token, path, {method: 'DELETE'});
      } catch (error) {
        teardownErrors.push(error);
      }
      if (teardownErrors.length && (bodyThrew || testInfo.status !== testInfo.expectedStatus)) {
        console.error(combinedError(`Provider audit/cleanup also failed for ${runId}`, teardownErrors));
      }
    }
    if (teardownErrors.length && testInfo.status === testInfo.expectedStatus) {
      throw combinedError(`Provider audit/cleanup failed for ${runId}`, teardownErrors);
    }
  },
});
export {expect};
