import {createReadStream, existsSync, readFileSync, statSync} from 'node:fs';
import {createServer} from 'node:http';
import {extname, resolve} from 'node:path';
import {fileURLToPath} from 'node:url';

const COCKPIT_ROOT = fileURLToPath(new URL('../..', import.meta.url));
const REPOSITORY_ROOT = resolve(COCKPIT_ROOT, '..');
const DIST_ROOT = resolve(COCKPIT_ROOT, 'dist/cockpit/browser');
const NGINX_CONFIG = resolve(REPOSITORY_ROOT, 'docker/cockpit-nginx.conf');
const HOST = process.env['CANVAS_E2E_HOST'] || '127.0.0.1';
const PORT = Number.parseInt(process.env['CANVAS_E2E_PORT'] || '4173', 10);
const PARENT_ORIGIN = `http://${HOST}:${PORT}`;
const VIEWER_SUFFIX = '.canvas.invalid';

export const THREAD_ID = '11111111-1111-4111-8111-111111111111';
const CANVAS_PATH = `/api/persistent/threads/${THREAD_ID}/canvases/main`;
const BROWSER_CAPABILITY_PATH = `/api/persistent/threads/${THREAD_ID}/browser/capability`;
const BROWSER_OPEN_PATH = `/api/persistent/threads/${THREAD_ID}/browser/open`;
const STATE_ETAG_DIGEST = 'a'.repeat(64);
const FILE_SOURCE_FINGERPRINT = 'f'.repeat(64);
const FILE_SOURCE_VERSION = `sha256:${'d'.repeat(64)}`;
const CHALLENGE = 'c'.repeat(43);
const READY_RECEIPT = 'r'.repeat(43);
const EXCHANGE_CODE = 'e'.repeat(43);

const STATIC_HTML_FILE = `<!doctype html>
<html><head><base href="https://canvas-base-probe.invalid/" target="_top"></head>
<body>
  <a id="static-jump" href="#static-target">Jump to target</a>
  <button id="static-script-control" onclick="document.body.dataset.scriptRan='yes'">Run</button>
  <div id="static-marker" style="background-color:rgb(18,52,86);color:white">Static Canvas ready</div>
  <section id="static-target" style="margin-top:1200px">Static fragment target</section>
  <script>document.body.dataset.scriptRan='yes'</script>
</body></html>`;

const INTERACTIVE_HTML_FILE = `<!doctype html>
<html>
  <head>
    <base href="https://canvas-base-probe.invalid/" target="_top">
    <style>
      html,body { background:rgb(18,52,86); color:white; }
      #interactive-target { margin-top:1200px; }
    </style>
  </head>
  <body>
    <a id="interactive-jump" href="#interactive-target">Jump to target</a>
    <button id="interactive-action"
            onclick="document.getElementById('interactive-result').textContent='clicked'">Act</button>
    <div id="interactive-result">cold</div>
    <section id="interactive-target">Interactive fragment target</section>
    <div id="interactive-marker">Interactive Canvas ready</div>
    <script>document.body.dataset.scriptReady='yes'; window.canvasProbe='ready';</script>
  </body>
</html>`;

const COCKPIT_CSP = "frame-ancestors 'none'; img-src 'self' blob: data:";
const X_FRAME_OPTIONS = 'DENY';
const MIME_TYPES = new Map([
  ['.css', 'text/css; charset=utf-8'],
  ['.html', 'text/html; charset=utf-8'],
  ['.ico', 'image/x-icon'],
  ['.js', 'text/javascript; charset=utf-8'],
  ['.json', 'application/json; charset=utf-8'],
  ['.map', 'application/json; charset=utf-8'],
  ['.png', 'image/png'],
  ['.svg', 'image/svg+xml'],
  ['.webmanifest', 'application/manifest+json'],
  ['.woff', 'font/woff'],
  ['.woff2', 'font/woff2'],
]);

if (!Number.isInteger(PORT) || PORT < 1 || PORT > 65535) {
  throw new Error('CANVAS_E2E_PORT must be a valid TCP port');
}
if (!existsSync(resolve(DIST_ROOT, 'index.html'))) {
  throw new Error(
    `Cockpit production output is missing at ${DIST_ROOT}. Run \`npm run build\` first.`,
  );
}

const nginx = readFileSync(NGINX_CONFIG, 'utf8');
if (
  !nginx.includes(`Content-Security-Policy \"${COCKPIT_CSP}\"`) ||
  !nginx.includes(`X-Frame-Options \"${X_FRAME_OPTIONS}\"`)
) {
  throw new Error(
    'The Canvas browser fixture no longer matches docker/cockpit-nginx.conf anti-framing headers',
  );
}

let state = freshState();
const eventStreams = new Set();
const pendingRenewals = new Set();

function freshState(scenario = 'normal') {
  return {
    scenario,
    authenticated: true,
    revoked: false,
    presentationRevision: 1,
    browserOpened: false,
    browserOpenCount: 0,
    browserOpen: [],
    originGeneration: 1,
    attachmentCounter: 0,
    requests: [],
    attachments: [],
    closedAttachmentIds: [],
    serverRevokedAttachmentIds: [],
    authorize: [],
    resetOrigin: [],
    logoutCount: 0,
  };
}

function stateEtag() {
  return `\"canvas:${state.presentationRevision}:${STATE_ETAG_DIGEST}\"`;
}

function canvasState() {
  if (state.scenario === 'shared-browser') return browserCanvasState();
  if (state.scenario === 'file-static') return fileCanvasState('html');
  if (state.scenario === 'file-interactive') return fileCanvasState('html-interactive');
  const cleared = state.revoked;
  return {
    canvas_id: 'main',
    source: cleared
      ? null
      : {
          type: 'workspace_app',
          manifest_path: '.srw/canvas-app.json',
          entry_path: '/',
        },
    title: cleared ? null : 'Browser conformance app',
    renderer: 'auto',
    editable: false,
    alt_text: null,
    presentation_revision: state.presentationRevision,
    source_version: null,
    status: cleared ? 'cleared' : 'ready',
    capabilities: {
      can_edit: false,
      can_pop_out: !cleared,
      can_take_control: false,
      can_create_viewer_session: !cleared,
      can_reset_origin: !cleared,
    },
    updated_at: new Date().toISOString(),
  };
}

function fileCanvasState(renderer) {
  const cleared = state.revoked;
  const query = new URLSearchParams({
    presentation_revision: String(state.presentationRevision),
    source_fingerprint: FILE_SOURCE_FINGERPRINT,
    source_version: FILE_SOURCE_VERSION,
    'ngsw-bypass': 'true',
  });
  return {
    canvas_id: 'main',
    source: cleared
      ? null
      : {
          type: 'workspace_file',
          path: renderer === 'html' ? 'canvas/static.html' : 'canvas/interactive.html',
        },
    title: cleared
      ? null
      : renderer === 'html'
        ? 'Static HTML conformance'
        : 'Interactive HTML conformance',
    renderer: cleared ? 'auto' : renderer,
    editable: false,
    alt_text: null,
    presentation_revision: state.presentationRevision,
    source_version: cleared ? null : FILE_SOURCE_VERSION,
    content_url: cleared ? null : `${CANVAS_PATH}/content?${query}`,
    status: cleared ? 'cleared' : 'ready',
    capabilities: {
      can_edit: false,
      can_pop_out: false,
      can_take_control: false,
    },
    updated_at: new Date().toISOString(),
  };
}

function browserCanvasState() {
  return {
    canvas_id: 'main',
    source: {type: 'browser'},
    title: 'Shared browser',
    renderer: 'auto',
    editable: false,
    alt_text: null,
    presentation_revision: state.presentationRevision,
    source_version: null,
    status: 'ready',
    capabilities: {
      can_edit: false,
      can_pop_out: true,
      can_take_control: true,
      can_stream_browser: true,
    },
    updated_at: new Date().toISOString(),
  };
}

function uuid(kind, value) {
  return `${kind}0000000-0000-4000-8000-${String(value).padStart(12, '0')}`;
}

function safeRequest(req, pathname) {
  const headers = req.headers;
  return {
    method: req.method,
    path: pathname,
    headerNames: Object.keys(headers).sort(),
    cookieNames: cookieNames(headers.cookie),
    hasAuthorization: typeof headers.authorization === 'string',
    hasCsrf: headers['x-csrf'] === '1',
    ifMatch: typeof headers['if-match'] === 'string' ? headers['if-match'] : null,
    origin: typeof headers.origin === 'string' ? headers.origin : null,
    secFetchDest: typeof headers['sec-fetch-dest'] === 'string' ? headers['sec-fetch-dest'] : null,
    secFetchMode: typeof headers['sec-fetch-mode'] === 'string' ? headers['sec-fetch-mode'] : null,
    secFetchSite: typeof headers['sec-fetch-site'] === 'string' ? headers['sec-fetch-site'] : null,
  };
}

function cookieNames(header) {
  if (typeof header !== 'string' || !header.trim()) return [];
  return header
    .split(';')
    .map(value => value.trim().split('=', 1)[0])
    .filter(Boolean)
    .sort();
}

function hasParentSession(req) {
  return cookieNames(req.headers.cookie).includes('srw_session');
}

function record(req, pathname) {
  state.requests.push(safeRequest(req, pathname));
}

function json(res, status, body, headers = {}) {
  const payload = JSON.stringify(body);
  res.writeHead(status, {
    'Cache-Control': 'private, no-store',
    'Content-Type': 'application/json; charset=utf-8',
    'Content-Length': Buffer.byteLength(payload),
    ...headers,
  });
  res.end(payload);
}

function html(res, status, body, headers = {}) {
  res.writeHead(status, {
    'Cache-Control': 'private, no-store',
    'Content-Type': 'text/html; charset=utf-8',
    'Content-Security-Policy': COCKPIT_CSP,
    'X-Frame-Options': X_FRAME_OPTIONS,
    'Content-Length': Buffer.byteLength(body),
    ...headers,
  });
  res.end(body);
}

async function readJson(req) {
  const chunks = [];
  let length = 0;
  for await (const chunk of req) {
    length += chunk.length;
    if (length > 64 * 1024) throw new Error('fixture request body too large');
    chunks.push(chunk);
  }
  if (length === 0) return null;
  return JSON.parse(Buffer.concat(chunks).toString('utf8'));
}

function timings() {
  const now = Date.now();
  if (state.scenario === 'lease-expiry') {
    return {
      bootstrap_expires_at: new Date(now + 30_000).toISOString(),
      expires_at: new Date(now + 60_000).toISOString(),
      renew_after: new Date(now + 1_200).toISOString(),
    };
  }
  return {
    bootstrap_expires_at: new Date(now + 30_000).toISOString(),
    expires_at: new Date(now + 120_000).toISOString(),
    renew_after: new Date(now + 80_000).toISOString(),
  };
}

function renewalTimings() {
  const now = Date.now();
  return {
    expires_at: new Date(now + 120_000).toISOString(),
    renew_after: new Date(now + 80_000).toISOString(),
  };
}

function retireActiveAttachments() {
  for (const attachment of state.attachments) {
    if (
      !state.closedAttachmentIds.includes(attachment.attachmentId) &&
      !state.serverRevokedAttachmentIds.includes(attachment.attachmentId)
    ) {
      state.serverRevokedAttachmentIds.push(attachment.attachmentId);
    }
  }
}

const server = createServer(async (req, res) => {
  try {
    const url = new URL(req.url || '/', PARENT_ORIGIN);
    const pathname = url.pathname;

    if (pathname === '/__e2e/health') {
      return json(res, 200, {ok: true});
    }
    if (pathname === '/__e2e/reset' && req.method === 'POST') {
      const body = await readJson(req);
      state = freshState(body?.scenario || 'normal');
      return json(res, 200, {ok: true});
    }
    if (pathname === '/__e2e/state' && req.method === 'GET') {
      return json(res, 200, {
        scenario: state.scenario,
        authenticated: state.authenticated,
        revoked: state.revoked,
        presentationRevision: state.presentationRevision,
        browserOpened: state.browserOpened,
        browserOpenCount: state.browserOpenCount,
        browserOpen: state.browserOpen,
        originGeneration: state.originGeneration,
        requests: state.requests,
        attachments: state.attachments,
        closedAttachmentIds: state.closedAttachmentIds,
        serverRevokedAttachmentIds: state.serverRevokedAttachmentIds,
        authorize: state.authorize,
        resetOrigin: state.resetOrigin,
        logoutCount: state.logoutCount,
      });
    }
    if (pathname === '/__e2e/revoke' && req.method === 'POST') {
      state.revoked = true;
      state.presentationRevision += 1;
      retireActiveAttachments();
      return json(res, 200, {ok: true});
    }
    if (pathname === '/__e2e/expire-auth' && req.method === 'POST') {
      state.authenticated = false;
      retireActiveAttachments();
      return json(res, 200, {ok: true});
    }

    if (pathname === '/auth/login' && req.method === 'GET') {
      return html(
        res,
        200,
        '<!doctype html><meta charset="utf-8"><title>Sign in</title>' +
          '<main data-testid="fixture-login">Authentication required</main>',
      );
    }

    if (pathname === '/trusted-target' && req.method === 'GET') {
      record(req, pathname);
      return html(
        res,
        200,
        '<!doctype html><meta charset="utf-8"><main data-testid="trusted-target">Trusted Cockpit content</main>',
      );
    }

    if (pathname === '/auth/logout' && req.method === 'POST') {
      record(req, pathname);
      if (!hasParentSession(req)) return json(res, 401, {detail: 'not authenticated'});
      state.logoutCount += 1;
      state.authenticated = false;
      retireActiveAttachments();
      return json(
        res,
        200,
        {status: 'ok', kc_logout_url: null},
        {'Set-Cookie': 'srw_session=; Path=/; Max-Age=0; HttpOnly; SameSite=Lax'},
      );
    }

    if (pathname === '/assets/env.js' && req.method === 'GET') {
      const body = `(function(window){window.env=window.env||{};` +
        `window.env.apiUrl=${JSON.stringify(`${PARENT_ORIGIN}/api`)};` +
        `window.env.canvasViewerHostSuffix=${JSON.stringify(VIEWER_SUFFIX)};` +
        `window.env.models=[];window.env.modelPresets=[];window.env.builderModels=[];` +
        `})(window);`;
      res.writeHead(200, {
        'Cache-Control': 'private, no-store',
        'Content-Type': 'text/javascript; charset=utf-8',
        'Content-Length': Buffer.byteLength(body),
      });
      return res.end(body);
    }

    if (pathname.startsWith('/api/') && (!state.authenticated || !hasParentSession(req))) {
      record(req, pathname);
      return json(res, 401, {detail: {code: 'authentication_required'}});
    }

    if (pathname === '/api/auth/me' && req.method === 'GET') {
      record(req, pathname);
      return json(res, 200, {
        user: {
          id: '22222222-2222-4222-8222-222222222222',
          display_name: 'Canvas Test User',
          avatar_color: '#7c3aed',
          email: 'canvas-test@example.invalid',
          is_admin: false,
          is_approved: true,
          can_use_vm: false,
          created_at: '2026-01-01T00:00:00Z',
        },
      });
    }
    if (pathname === '/api/users' && req.method === 'GET') {
      record(req, pathname);
      return json(res, 200, []);
    }
    if (pathname === '/api/settings/preferences' && req.method === 'GET') {
      record(req, pathname);
      return json(res, 200, {});
    }
    if (pathname === `/api/persistent/threads/${THREAD_ID}` && req.method === 'GET') {
      record(req, pathname);
      return json(res, 200, {
        thread_id: THREAD_ID,
        title: 'Shared browser conformance',
        status: 'ended',
        config_name: 'assistant',
        total_turns: 0,
        metadata: {},
        ended_at: '2026-07-22T12:00:00Z',
      });
    }
    if (
      pathname === `/api/persistent/threads/${THREAD_ID}/messages` &&
      req.method === 'GET'
    ) {
      record(req, pathname);
      return json(res, 200, {messages: [], total: 0});
    }
    if (
      pathname === `/api/persistent/threads/${THREAD_ID}/citations` &&
      req.method === 'GET'
    ) {
      record(req, pathname);
      return json(res, 200, {citations: []});
    }
    if (
      (pathname === '/api/notifications/events' || pathname === '/api/sudo/events') &&
      req.method === 'GET'
    ) {
      record(req, pathname);
      res.writeHead(200, {
        'Cache-Control': 'no-cache, no-transform',
        'Content-Type': 'text/event-stream',
        Connection: 'keep-alive',
      });
      res.write(': fixture connected\n\n');
      eventStreams.add(res);
      res.on('close', () => eventStreams.delete(res));
      return;
    }

    if (pathname === CANVAS_PATH && req.method === 'GET') {
      record(req, pathname);
      if (state.scenario === 'shared-browser' && !state.browserOpened) {
        res.writeHead(204, {'Cache-Control': 'private, no-store'});
        return res.end();
      }
      return json(res, 200, canvasState(), {ETag: stateEtag()});
    }

    if (pathname === `${CANVAS_PATH}/content` && req.method === 'GET') {
      record(req, pathname);
      const content = state.scenario === 'file-static'
        ? STATIC_HTML_FILE
        : state.scenario === 'file-interactive'
          ? INTERACTIVE_HTML_FILE
          : null;
      if (content === null) {
        return json(res, 404, {detail: {code: 'fixture_content_missing'}});
      }
      res.writeHead(200, {
        'Cache-Control': 'private, no-store',
        'Content-Type': 'text/html; charset=utf-8',
        'Content-Length': Buffer.byteLength(content),
        'Content-Disposition': 'attachment; filename="canvas.html"',
        'Content-Security-Policy':
          "default-src 'none'; script-src 'none'; style-src 'none'; " +
          "img-src 'none'; connect-src 'none'; form-action 'none'; " +
          "base-uri 'none'; object-src 'none'; sandbox",
        'Referrer-Policy': 'no-referrer',
        'X-Content-Type-Options': 'nosniff',
        ETag: `"${FILE_SOURCE_VERSION}"`,
      });
      return res.end(content);
    }

    if (pathname === BROWSER_CAPABILITY_PATH && req.method === 'GET') {
      record(req, pathname);
      if (state.scenario !== 'shared-browser') {
        return json(res, 404, {detail: {code: 'fixture_route_missing', path: pathname}});
      }
      return json(res, 200, {
        feature_enabled: true,
        can_open_browser: true,
        workspace_ready: true,
        reason: null,
      });
    }

    if (pathname === BROWSER_OPEN_PATH && req.method === 'POST') {
      record(req, pathname);
      if (state.scenario !== 'shared-browser') {
        return json(res, 404, {detail: {code: 'fixture_route_missing', path: pathname}});
      }
      const body = await readJson(req);
      state.browserOpen.push({
        bodyKeys: body && typeof body === 'object' ? Object.keys(body).sort() : [],
      });
      state.browserOpenCount += 1;
      state.browserOpened = true;
      state.presentationRevision = state.browserOpenCount;
      return json(res, 200, browserCanvasState(), {
        ETag: stateEtag(),
        'X-Canvas-Mutation-Changed': 'true',
      });
    }

    if (pathname === `${CANVAS_PATH}/view-attachments` && req.method === 'POST') {
      record(req, pathname);
      if (state.revoked) return json(res, 409, {detail: {code: 'canvas_cleared'}});
      if (
        !['same-origin', 'same-site'].includes(req.headers['sec-fetch-site']) ||
        req.headers['sec-fetch-mode'] !== 'cors' ||
        req.headers['sec-fetch-dest'] !== 'empty'
      ) {
        return json(res, 409, {detail: {code: 'canvas_browser_unsupported'}});
      }
      state.attachmentCounter += 1;
      const attachmentId = uuid('3', state.attachmentCounter);
      const originId = uuid('2', state.originGeneration);
      const origin = `https://${originId}${VIEWER_SUFFIX}`;
      state.attachments.push({
        attachmentId,
        origin,
        originGeneration: state.originGeneration,
        ifMatch: req.headers['if-match'] || null,
      });
      const lease = timings();
      return json(res, 200, {
        attachment_id: attachmentId,
        origin,
        bootstrap_url: `${origin}/_canvas/bootstrap?attachment_id=${attachmentId}`,
        bridge_nonce: 'b'.repeat(43),
        ...lease,
      });
    }

    const authorizeMatch = pathname.match(
      new RegExp(`^${CANVAS_PATH}/view-attachments/([0-9a-f-]+)/authorize$`),
    );
    if (authorizeMatch && req.method === 'POST') {
      record(req, pathname);
      const body = await readJson(req);
      const bodyKeys = body && typeof body === 'object' ? Object.keys(body).sort() : [];
      const proofMatches =
        body?.challenge === CHALLENGE &&
        body?.ready_receipt === READY_RECEIPT &&
        body?.bridge_nonce === 'b'.repeat(43);
      state.authorize.push({
        attachmentId: authorizeMatch[1],
        bodyKeys,
        proofMatches,
      });
      if (!proofMatches) return json(res, 400, {detail: {code: 'invalid_proof'}});
      return json(res, 200, {
        challenge: CHALLENGE,
        ready_receipt: READY_RECEIPT,
        exchange_code: EXCHANGE_CODE,
        expires_at: new Date(Date.now() + 2_000).toISOString(),
      });
    }

    const renewMatch = pathname.match(
      new RegExp(`^${CANVAS_PATH}/view-attachments/([0-9a-f-]+)/renew$`),
    );
    if (renewMatch && req.method === 'POST') {
      record(req, pathname);
      if (state.serverRevokedAttachmentIds.includes(renewMatch[1])) {
        return json(res, 410, {detail: {code: 'canvas_view_attachment_expired'}});
      }
      if (state.scenario === 'lease-expiry') {
        pendingRenewals.add(res);
        res.on('close', () => pendingRenewals.delete(res));
        return;
      }
      return json(res, 200, renewalTimings());
    }

    const closeMatch = pathname.match(
      new RegExp(`^${CANVAS_PATH}/view-attachments/([0-9a-f-]+)$`),
    );
    if (closeMatch && req.method === 'DELETE') {
      record(req, pathname);
      if (!state.closedAttachmentIds.includes(closeMatch[1])) {
        state.closedAttachmentIds.push(closeMatch[1]);
      }
      res.writeHead(204, {'Cache-Control': 'private, no-store'});
      return res.end();
    }

    if (pathname === `${CANVAS_PATH}/reset-origin` && req.method === 'POST') {
      record(req, pathname);
      if (state.revoked) return json(res, 409, {detail: {code: 'canvas_cleared'}});
      state.resetOrigin.push({
        ifMatch: req.headers['if-match'] || null,
        hasCsrf: req.headers['x-csrf'] === '1',
      });
      retireActiveAttachments();
      state.originGeneration += 1;
      state.presentationRevision += 1;
      return json(res, 200, canvasState(), {ETag: stateEtag()});
    }

    if (pathname.startsWith('/api/')) {
      record(req, pathname);
      return json(res, 404, {detail: {code: 'fixture_route_missing', path: pathname}});
    }

    return serveStatic(req, res, pathname);
  } catch (error) {
    const message = error instanceof Error ? error.message : 'fixture failure';
    json(res, 500, {detail: message});
  }
});

function serveStatic(req, res, pathname) {
  if (req.method !== 'GET' && req.method !== 'HEAD') {
    return json(res, 405, {detail: 'method not allowed'});
  }
  let relativePath;
  try {
    relativePath = decodeURIComponent(pathname).replace(/^\/+/, '');
  } catch {
    return json(res, 400, {detail: 'invalid path'});
  }
  let filePath = resolve(DIST_ROOT, relativePath || 'index.html');
  if (!filePath.startsWith(`${DIST_ROOT}/`) && filePath !== DIST_ROOT) {
    return json(res, 403, {detail: 'path escapes production output'});
  }

  try {
    if (statSync(filePath).isDirectory()) filePath = resolve(filePath, 'index.html');
  } catch {
    filePath = resolve(DIST_ROOT, 'index.html');
  }

  let info;
  try {
    info = statSync(filePath);
  } catch {
    return json(res, 404, {detail: 'not found'});
  }
  if (!info.isFile()) return json(res, 404, {detail: 'not found'});

  const headers = {
    'Cache-Control': 'private, no-store',
    'Content-Length': info.size,
    'Content-Type': MIME_TYPES.get(extname(filePath)) || 'application/octet-stream',
    'Content-Security-Policy': COCKPIT_CSP,
    'X-Frame-Options': X_FRAME_OPTIONS,
  };
  res.writeHead(200, headers);
  if (req.method === 'HEAD') return res.end();
  createReadStream(filePath).pipe(res);
}

function close() {
  for (const stream of eventStreams) stream.end();
  for (const renewal of pendingRenewals) renewal.destroy();
  server.close(() => process.exit(0));
}

process.on('SIGINT', close);
process.on('SIGTERM', close);

server.listen(PORT, HOST, () => {
  process.stdout.write(`Canvas browser fixture listening on ${PARENT_ORIGIN}\n`);
});
