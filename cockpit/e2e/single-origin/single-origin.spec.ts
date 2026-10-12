import {browserJson, expect, login, target, test} from './browser.fixture';

test('one warning covers login, API, Git and cloud; browser capabilities remain', async ({trustedPage: page, origins, cookieHeaders, workerRequests}) => {
  await login(page);
  await expect.poll(() => cookieHeaders.some(header => /^srw_session=/.test(header))).toBe(true);
  for (const header of cookieHeaders.filter(header => /^srw_session=/.test(header))) {
    expect(header).not.toMatch(/;\s*Domain=/i);
  }
  const session = (await page.context().cookies(target())).find(cookie => cookie.name === 'srw_session');
  expect(session).toMatchObject({secure: true, httpOnly: true, sameSite: 'Lax', path: '/', domain: new URL(target()).hostname});
  const identity = await browserJson(page, '/auth/me');
  expect(identity.status).toBe(200);
  expect(identity.body.id).toBeTruthy();
  expect(await browserJson(page, '/auth/refresh', 'POST')).toMatchObject({status: 200, body: {refreshed: true}});

  const capabilities = await page.evaluate(async () => ({
    secureContext: isSecureContext,
    digest: Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256', new TextEncoder().encode('srw')))).length,
    randomUuid: /^[0-9a-f-]{36}$/.test(crypto.randomUUID()),
    media: typeof navigator.mediaDevices?.getUserMedia,
    mediaEnumeration: Array.isArray(await navigator.mediaDevices.enumerateDevices()),
    serviceWorkerEnabled: (window as any).env?.serviceWorkerEnabled,
    externalClientsEnabled: (window as any).env?.externalClientsEnabled,
  }));
  expect(capabilities).toEqual({secureContext: true, digest: 32, randomUuid: true, media: 'function', mediaEnumeration: true, serviceWorkerEnabled: false, externalClientsEnabled: false});

  await page.goto(`${target()}/settings`);
  await expect(page).toHaveURL(`${target()}/settings/general`);
  await expect(page.locator('app-settings')).toBeVisible();
  await expect(page.locator('app-settings a[href*="ssh-keys"]')).toHaveCount(0);
  await expect(page.getByRole('heading', {name: 'MCP Tokens', exact: true})).toHaveCount(0);
  // Neither page exists without external clients; both send the user back
  // to General, and the settings rail lists neither.
  await expect(page.locator('app-sidebar a[href="/settings/ssh-keys"], app-sidebar a[href="/settings/mcp"]')).toHaveCount(0);
  await page.goto(`${target()}/settings/ssh-keys`);
  await expect(page).toHaveURL(`${target()}/settings/general`);
  await page.goto(`${target()}/settings/mcp`);
  await expect(page).toHaveURL(`${target()}/settings/general`);

  // Follow the real provider links; successful rendered pages after the
  // callback prove that server-side discovery/token exchange also works.
  await page.goto(`${target()}/git/user/login`);
  const gitSso = page.locator('a[href*="/user/oauth2/"]').first();
  await expect(gitSso).toBeVisible();
  await gitSso.click();
  await page.waitForURL(url => url.pathname.startsWith('/git/') && !url.pathname.includes('/oauth2/'));
  await page.getByRole('menu', {name: /Profile and Settings/}).click();
  await expect(page.locator('a[href$="/user/settings"]')).toBeVisible();

  const gitCookies = (await page.context().cookies(`${target()}/git/`)).filter(cookie => ['i_like_gitea', '_csrf'].includes(cookie.name));
  expect(gitCookies.length).toBeGreaterThan(0);
  for (const cookie of gitCookies) expect(cookie.path).toMatch(/^\/git\/?$/);

  await page.goto(`${target()}/cloud/`);
  const cloudSso = page.locator('a[href*="/user_oidc/login/"]').first();
  if (await cloudSso.isVisible()) await cloudSso.click();
  await expect(page.locator('#user-menu, #header .header-right')).toBeVisible();
  expect(new URL(page.url()).pathname).toMatch(/^\/cloud\//);

  const cloudCookies = (await page.context().cookies(`${target()}/cloud/`)).filter(cookie => /^oc|^nc_/.test(cookie.name));
  expect(cloudCookies.length).toBeGreaterThan(0);
  for (const cookie of cloudCookies) expect(cookie.path).toMatch(/^\/cloud\/?$/);

  // Exercise prefix preservation, encoded filenames and a nontrivial body
  // using the authenticated Nextcloud browser session. Always remove the
  // disposable file, including when the download assertion fails.
  const transfer = await page.evaluate(async () => {
    const oc = (window as any).OC;
    const uid = oc.getCurrentUser().uid;
    const filename = `single origin # % é ${crypto.randomUUID()}.txt`;
    const path = `/cloud/remote.php/dav/files/${encodeURIComponent(uid)}/${encodeURIComponent(filename)}`;
    const headers = {requesttoken: oc.requestToken};
    const content = 'single-origin-test\n'.repeat(64 * 1024);
    try {
      const upload = await fetch(path, {method: 'PUT', headers, body: content});
      const download = await fetch(path, {headers});
      return {upload: upload.status, download: download.status, identical: await download.text() === content};
    } finally {
      const cleanup = await fetch(path, {method: 'DELETE', headers});
      if (![204, 404].includes(cleanup.status)) throw new Error(`Cloud test-file cleanup failed: ${cleanup.status}`);
    }
  });
  expect([201, 204]).toContain(transfer.upload);
  expect(transfer).toMatchObject({download: 200, identical: true});

  await page.goto(target());
  // Intentional observation window: Angular's production registration
  // strategy has a 30-second fallback even when the app never becomes idle.
  await page.waitForTimeout(35_000);
  const registrations = await page.evaluate(async () => {
    if (!('serviceWorker' in navigator)) return [];
    return (await navigator.serviceWorker.getRegistrations()).map(registration => registration.scope);
  });
  expect(registrations).toEqual([]);
  expect(workerRequests).toEqual([]);
  const logout = await browserJson(page, '/auth/logout', 'POST');
  expect(logout.status).toBe(200);
  expect(new URL(logout.body.kc_logout_url).origin).toBe(target());
  await page.goto(logout.body.kc_logout_url);
  expect((await page.context().cookies(target())).some(cookie => cookie.name === 'srw_session')).toBe(false);
  await login(page);
  expect([...origins]).toEqual([target()]);
});

test('a disposable session streams, reconnects and opens the browser IDE', async ({trustedPage: page, origins, providerRunId}, testInfo) => {
  test.setTimeout(600_000);
  const runId = providerRunId;
  const reply = `E2E_REPLY:${runId}`;
  await login(page);
  const threadId = await page.evaluate(async ({runId}) => {
    const response = await fetch('/api/persistent/threads', {
      method: 'POST', credentials: 'same-origin',
      headers: {'Content-Type': 'application/json', 'X-CSRF': '1'},
      body: JSON.stringify({title: `Single origin ${runId}`, model: 'e2e-chat', datasource_ids: [], config_override: {workspace: {backend: 'sandbox'}}}),
    });
    if (!response.ok) throw new Error(`Session creation returned ${response.status}: ${await response.text()}`);
    return (await response.json()).thread_id as string;
  }, {runId});
  await testInfo.attach('owned-thread', {body: threadId, contentType: 'text/plain'});
  try {
    const socketOpened = page.waitForEvent('websocket', {predicate: socket => new URL(socket.url()).pathname === `/p/${threadId}/ws`, timeout: 300_000});
    void socketOpened.catch(() => undefined);
    await page.goto(`${target()}/sessions/${threadId}`);
    const socket = await socketOpened;
    await socket.waitForEvent('framereceived');
    const connection = await browserJson(page, `/api/sessions/${threadId}/connection`);
    expect(connection.status).toBe(200);
    const socketUrl = new URL(connection.body.ws_url);
    expect(socketUrl.origin.replace(/^wss/, 'https')).toBe(target());
    expect(socketUrl.pathname).toBe(`/p/${threadId}/ws`);
    await page.getByTestId('chat-composer').fill(`E2E-${runId} first message through the live application`);
    await page.getByTestId('chat-send').click();
    await expect(page.getByTestId('chat-message-assistant').filter({hasText: reply})).toHaveCount(1, {timeout: 120_000});
    const reconnected = page.waitForEvent('websocket', ws => new URL(ws.url()).pathname === socketUrl.pathname);
    await page.reload({waitUntil: 'domcontentloaded'});
    await (await reconnected).waitForEvent('framereceived');
    await expect(page.getByTestId('chat-message-assistant').filter({hasText: reply})).toHaveCount(1);
    await expect(page.getByTestId('chat-composer')).toBeEnabled();
    await expect.poll(async () => (await browserJson(page, `/api/persistent/threads/${threadId}/ide`)).body.status).toBe('active');
    await page.getByRole('button', {name: 'More actions', exact: true}).click();
    await expect(page.getByRole('menuitem', {name: /^IDE$/i})).toBeVisible();
    await expect(page.getByRole('menuitem', {name: /SSH|JetBrains/i})).toHaveCount(0);
    const popupPromise = page.waitForEvent('popup');
    await page.getByRole('menuitem', {name: /^IDE$/i}).click();
    const popup = await popupPromise;
    // The popup initially exists at about:blank while its authenticated proxy
    // navigation starts. Wait for the actual workbench before reloading it.
    await popup.waitForURL(url => url.origin === target() && url.pathname === `/api/ide/${threadId}/proxy/`, {waitUntil: 'domcontentloaded', timeout: 120_000});
    await expect(popup.locator('.monaco-workbench')).toBeVisible({timeout: 120_000});
    // This is a newly created, test-owned workspace. Complete VS Code's
    // normal workspace-trust prompt so its terminal can run.
    const trustWorkspace = popup.getByRole('button', {name: 'Yes, I trust the authors', exact: true});
    await trustWorkspace.click();
    await expect(trustWorkspace).toBeHidden();
    expect(new URL(popup.url()).origin).toBe(target());
    expect(new URL(popup.url()).pathname).toBe(`/api/ide/${threadId}/proxy/`);
    // Focus a normal editor. The first-run welcome walkthrough contains an
    // embedded webview, which is intentionally outside this mode's support.
    await popup.getByRole('treeitem', {name: /README\.md/}).dblclick();
    await expect(popup.locator('.monaco-editor').first()).toBeVisible();
    // Create a file through the integrated terminal, then read/edit/save it
    // through Monaco and reload, proving both transports through the proxy.
    const basename = `single-origin-${testInfo.project.name}.txt`;
    const filename = `/home/agent-host/workspace/${basename}`;
    const terminalMarker = `SRW_TERMINAL_${runId}`;
    const editorMarker = `SRW_EDITOR_${runId}`;
    // Use the visible command center; browser-reserved shortcuts differ.
    await popup.locator('.command-center-quick-pick').click();
    await popup.locator('.quick-input-widget input').fill('>Terminal: Create New Terminal');
    await expect(popup.getByRole('option', {name: /^Terminal: Create New Terminal\b/}).first()).toBeVisible();
    await popup.keyboard.press('Enter');
    const terminal = popup.locator('.xterm-helper-textarea').last();
    await terminal.waitFor({state: 'attached', timeout: 30_000});
    await terminal.pressSequentially(`printf '%s\\n' '${terminalMarker}' > ${filename}`, {delay: 25});
    await terminal.press('Enter');
    // Wait for the terminal's filesystem effect before moving focus away
    // from xterm, then open that real file through the Explorer.
    const explorerFile = popup.getByRole('treeitem').filter({hasText: basename});
    await expect(explorerFile).toBeVisible({timeout: 30_000});
    await explorerFile.dblclick();
    const terminalLine = popup.locator('.monaco-editor .view-line').filter({hasText: terminalMarker});
    await expect(terminalLine).toBeVisible();
    await terminalLine.click();
    await popup.keyboard.press('Control+a');
    await popup.keyboard.type(editorMarker, {delay: 20});
    await popup.keyboard.press('Enter');
    await popup.keyboard.press('Control+s');
    await expect(popup.locator('.tab.active.dirty')).toHaveCount(0);
    const mainResponse = await popup.reload({waitUntil: 'domcontentloaded'});
    expect(mainResponse?.ok()).toBe(true);
    await expect(popup.locator('.monaco-workbench')).toBeVisible({timeout: 120_000});
    // Verify the saved file independently of VS Code's debounced tab-layout
    // persistence, which may restore the earlier README tab after a reload.
    await expect(explorerFile).toBeVisible();
    await explorerFile.dblclick();
    const savedLine = popup.locator('.monaco-editor .view-line').filter({hasText: editorMarker});
    await expect(savedLine).toHaveCount(1, {timeout: 120_000});
    await expect(savedLine).toBeVisible();
    await popup.screenshot({path: testInfo.outputPath('browser-ide.png')});
    expect([...origins]).toEqual([target()]);
  } catch (error) {
    console.error('Session journey failed before cleanup:', error instanceof Error ? error.stack : String(error));
    for (const [index, openPage] of page.context().pages().entries()) {
      await openPage.screenshot({path: testInfo.outputPath(`session-page-${index}-before-cleanup.png`)}).catch(() => undefined);
    }
    await page.screenshot({path: testInfo.outputPath('session-before-cleanup.png')});
    await testInfo.attach('session-before-cleanup', {body: await page.locator('body').ariaSnapshot(), contentType: 'text/plain'});
    throw error;
  } finally {
    // Retire this exact owned session through its normal lifecycle. Never
    // delete pods or synthesize readiness/ownership state for this test.
    if (new URL(page.url()).origin !== target()) await page.goto(target());
    await expect.poll(async () => {
      const response = await browserJson(page, `/api/persistent/threads/${threadId}?permanent=true`, 'DELETE');
      if (![200, 202, 404, 409, 503].includes(response.status)) throw new Error(`Session cleanup returned ${response.status}`);
      return (await browserJson(page, `/api/persistent/threads/${threadId}`)).status;
    }, {timeout: 180_000, intervals: [1000, 2000, 5000]}).toBe(404);
  }
});
