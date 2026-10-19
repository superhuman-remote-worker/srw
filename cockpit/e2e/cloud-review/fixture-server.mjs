/**
 * Disposable fixture backend for the protected-cloud review surface.
 *
 * Serves the real production build from `dist/cockpit/browser` (override with
 * CLOUD_REVIEW_DIST for a before/after comparison against an older build) and
 * mocks exactly the endpoints a protected persistent session touches on load.
 *
 * Two deliberate properties:
 *
 * - The thread is `ended` and no WebSocket is served, so `chat.isConnected()`
 *   is false for the whole run. That is the PC-25 scenario: a genuine staged
 *   diff attached to a session whose agent is gone. If the review is reachable
 *   here, it is reachable independently of the agent lifecycle.
 * - Apply and reject are mocked. Nothing in this fixture can reach a real
 *   orchestrator, a real cloud, or the preserved epoch-5 evidence thread
 *   34743d6c-9224-4866-94a9-18c3828b8b29.
 *
 * Modelled on e2e/canvas/fixture-server.mjs.
 */
import { createReadStream, existsSync, readFileSync, statSync } from 'node:fs';
import { createServer } from 'node:http';
import { extname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const COCKPIT_ROOT = fileURLToPath(new URL('../..', import.meta.url));
const DIST_ROOT = process.env['CLOUD_REVIEW_DIST']
  ? resolve(process.env['CLOUD_REVIEW_DIST'])
  : resolve(COCKPIT_ROOT, 'dist/cockpit/browser');
const HOST = process.env['CLOUD_REVIEW_HOST'] || '127.0.0.1';
const PORT = Number.parseInt(process.env['CLOUD_REVIEW_PORT'] || '4174', 10);

export const THREAD_ID = '33333333-3333-4333-8333-333333333333';
export const PROJECT_ID = '44444444-4444-4444-8444-444444444444';
const USER_ID = '55555555-5555-4555-8555-555555555555';
export const JOB_ID = '77777777-7777-4777-8777-777777777777';

const JOB_SUMMARY = {
  id: JOB_ID,
  description: 'Produce the quarterly report into the project cloud folder',
  status: 'pending_review',
  diff_status: 'pending',
  cloud_review_mode: 'diff',
  created_at: '2026-08-24T08:00:00Z',
  updated_at: '2026-08-24T09:30:00Z',
  project_id: PROJECT_ID,
  user_id: USER_ID,
  config_name: 'writer',
};

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

if (!existsSync(resolve(DIST_ROOT, 'index.html'))) {
  throw new Error(`Cockpit build missing at ${DIST_ROOT}. Run \`npm run build\` first.`);
}

/** The four-file staged epoch from the PS-03/PS-05 runbook cases. */
const FILES = [
  { path: 'session_apply/change-me.txt', status: 'modified', binary: false },
  { path: 'session_apply/delete-me.pdf', status: 'deleted', binary: false },
  { path: 'session_apply/edit-me.docx', status: 'modified', binary: true },
  { path: 'session_apply/new-report.pdf', status: 'added', binary: false },
];

const FILE_CONTENT = {
  'session_apply/change-me.txt': {
    status: 'modified',
    old_content:
      'RUN=20260824-0918-e853\nPATH=session_apply/change-me.txt\nVERSION=1\n' +
      'NOTE=this line is unchanged\n',
    new_content:
      'RUN=20260824-0918-e853\nPATH=session_apply/change-me.txt\nVERSION=2\n' +
      'NOTE=this line is unchanged\n',
    old_binary: false,
    new_binary: false,
  },
  'session_apply/delete-me.pdf': {
    status: 'deleted',
    old_content: null,
    new_content: null,
    old_binary: true,
    new_binary: false,
  },
  'session_apply/edit-me.docx': {
    status: 'modified',
    old_content: null,
    new_content: null,
    old_binary: true,
    new_binary: true,
  },
  // Reported binary:false by the summary (PC-17's heuristic gap) but the
  // bytes are PDF syntax: the client-side sniff has to catch this one.
  'session_apply/new-report.pdf': {
    status: 'added',
    old_content: null,
    new_content: '%PDF-1.7\n1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n',
    old_binary: false,
    new_binary: false,
  },
};

let state = freshState();

function freshState(scenario = 'pending') {
  return {
    scenario,
    probeBlocked: scenario === 'probeFail',
    epoch: 5,
    resolved: null,
    applyCalls: 0,
    rejectCalls: 0,
    summaryCalls: 0,
    fileCalls: 0,
  };
}

/**
 * Held apply, released explicitly by POST /__e2e/release.
 *
 * A timed delay was tried first and is a flake generator: too short and the
 * response lands before the assertions run, too long and every run pays for
 * it. Holding the request makes the in-flight window deterministic and the
 * test fast.
 */
let applyGate = null;

function releaseApply() {
  const resolve = applyGate;
  applyGate = null;
  resolve?.();
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

async function readJson(req) {
  const chunks = [];
  for await (const chunk of req) chunks.push(chunk);
  if (!chunks.length) return null;
  try {
    return JSON.parse(Buffer.concat(chunks).toString('utf8'));
  } catch {
    return null;
  }
}

function summaryBody() {
  const pending = state.scenario !== 'empty' && !state.resolved;
  const files = pending ? FILES : [];
  return {
    thread_id: THREAD_ID,
    epoch: state.epoch,
    staged_at: pending ? '2026-08-24T09:18:12Z' : null,
    counts: {
      added: files.filter((f) => f.status === 'added').length,
      modified: files.filter((f) => f.status === 'modified').length,
      deleted: files.filter((f) => f.status === 'deleted').length,
    },
    protected_mount: 'cloud',
    files,
  };
}

const server = createServer(async (req, res) => {
  try {
    const url = new URL(req.url ?? '/', `http://${HOST}:${PORT}`);
    const pathname = url.pathname;
    if (process.env['CLOUD_REVIEW_LOG'] && (pathname.startsWith('/api/') || pathname.endsWith('.js'))) {
      process.stdout.write(`${req.method} ${pathname}\n`);
    }

    // ---- fixture control ----
    if (pathname === '/__e2e/health') return json(res, 200, { ok: true });
    if (pathname === '/__e2e/reset' && req.method === 'POST') {
      const body = await readJson(req);
      releaseApply(); // never leave a previous test's request hanging
      state = freshState(body?.scenario || 'pending');
      return json(res, 200, { ok: true });
    }
    if (pathname === '/__e2e/release' && req.method === 'POST') {
      releaseApply();
      return json(res, 200, { ok: true });
    }
    if (pathname === '/__e2e/release-probe' && req.method === 'POST') {
      state.probeBlocked = false;
      return json(res, 200, { ok: true });
    }
    if (pathname === '/__e2e/state') return json(res, 200, state);

    // Runtime config: the built app defaults apiUrl to localhost:8085, so
    // without this every call leaves the fixture and the shell bounces to
    // /auth/login.
    if (pathname === '/assets/env.js' && req.method === 'GET') {
      const body =
        `(function(window){window.env=window.env||{};` +
        // Relative, so the page origin and the API origin are identical no
        // matter which of localhost / 127.0.0.1 the runner navigates to.
        `window.env.apiUrl='/api';` +
        `window.env.cloudUrl=${JSON.stringify('https://cloud.example.invalid')};` +
        `window.env.models=[];window.env.modelPresets=[];window.env.builderModels=[];` +
        `})(window);`;
      res.writeHead(200, {
        'Cache-Control': 'private, no-store',
        'Content-Type': 'text/javascript; charset=utf-8',
        'Content-Length': Buffer.byteLength(body),
      });
      return res.end(body);
    }

    if (pathname === '/api/system/readiness') return json(res, 200, { ready: true });
    // `groups`, not a bare array: ModelService does `models.set(resp.groups)`
    // unguarded, and an undefined there throws out of EmptyCatalogBanner's
    // computed on every change-detection pass — which aborts the pass before
    // the review dialog can mount.
    if (pathname === '/api/models') {
      return json(res, 200, {
        groups: [{ provider: 'fixture', models: ['fixture-model'] }],
        auxiliary_models: [],
        vision_models: [],
        whisper_models: [],
        tts_models: [],
        embedding_models: [],
      });
    }
    // Shapes matter: a bare {} here makes shell computeds read `.length` off
    // undefined, which aborts the change-detection pass and stops the review
    // dialog from ever mounting. Fixture noise, not a product defect — but it
    // masks the thing under test, so give each probe its real shape.
    if (pathname === '/api/users/me/capabilities') {
      return json(res, 200, {
        is_admin: false,
        grants: null,
        catalog: { actions: [], config: [], groups: [] },
        features: { protected_cloud: true },
      });
    }
    if (pathname === '/api/voice/capabilities') return json(res, 200, { tts: false, stt: false });
    if (pathname.endsWith('/browser/capability')) {
      return json(res, 200, {
        feature_enabled: false,
        can_open_browser: false,
        workspace_ready: false,
        reason: 'fixture',
      });
    }
    if (pathname.endsWith('/canvases/main')) return json(res, 404, { detail: 'no canvas' });

    // ---- app shell ----
    if (pathname === '/api/auth/me' && req.method === 'GET') {
      return json(res, 200, {
        user: {
          id: USER_ID,
          display_name: 'Review Fixture User',
          avatar_color: '#9c2832',
          email: 'review-fixture@example.invalid',
          is_admin: false,
          is_approved: true,
          can_use_vm: false,
          created_at: '2026-01-01T00:00:00Z',
        },
      });
    }
    if (pathname === '/api/settings/preferences') return json(res, 200, {});
    if (pathname === '/api/users') return json(res, 200, []);

    // ---- the protected thread ----
    if (pathname === `/api/persistent/threads/${THREAD_ID}` && req.method === 'GET') {
      return json(res, 200, {
        id: THREAD_ID,
        thread_id: THREAD_ID,
        title: 'PS Protected Cloud review fixture',
        status: 'ended',
        config_name: 'session_base',
        permission_mode: 'read_only',
        user_id: USER_ID,
        total_turns: 8,
        total_tokens: 22000,
        created_at: '2026-08-24T09:00:00Z',
        last_activity: '2026-08-24T11:07:00Z',
        ended_at: '2026-08-24T11:07:00Z',
        nc_session_folder: `sessions/${THREAD_ID}`,
        cloud_session_url: 'https://cloud.example.invalid/apps/files/?dir=/sessions',
        metadata: { protected_cloud: true },
        // The mount projection PC-19 needs, ordered by target_path exactly as
        // the orchestrator returns it.
        mounts: [
          {
            id: '66666666-6666-4666-8666-666666666666',
            mount_kind: 'project',
            target_path: 'cloud',
            source_kind: 'project',
            source_ref: PROJECT_ID,
            backend_id: 'nextcloud',
          },
        ],
        project_ids: [PROJECT_ID],
      });
    }
    if (pathname === `/api/projects/${PROJECT_ID}` && req.method === 'GET') {
      return json(res, 200, {
        id: PROJECT_ID,
        name: 'Protected Docs',
        cloud_storage_url: 'https://cloud.example.invalid/apps/files/?dir=/Protected%20Docs',
        main_cloud_backend: 'nextcloud',
      });
    }
    if (pathname.startsWith(`/api/persistent/threads/${THREAD_ID}/messages`)) {
      return json(res, 200, { messages: [], total: 0 });
    }
    if (pathname.startsWith(`/api/persistent/threads/${THREAD_ID}/citations`)) {
      return json(res, 200, { citations: [] });
    }
    if (pathname === `/api/persistent/threads/${THREAD_ID}/state`) {
      return json(res, 200, { thread_id: THREAD_ID, status: 'ended', turns: [] });
    }

    // ---- Mode A job review, hosted on /jobs/review -----------------------
    // Present so the SAME surface can be captured in job context, which is the
    // only host that renders it inline (no WebSocket needed) — that makes a
    // like-for-like before/after against the pre-redesign build possible.
    if (pathname === '/api/jobs' && req.method === 'GET') {
      return json(res, 200, { jobs: [JOB_SUMMARY], total: 1, counts: { pending_review: 1 } });
    }
    if (pathname === `/api/jobs/${JOB_ID}` && req.method === 'GET') {
      return json(res, 200, JOB_SUMMARY);
    }
    if (pathname === `/api/jobs/${JOB_ID}/diff` && req.method === 'GET') {
      return json(res, 200, {
        job_id: JOB_ID,
        diff_status: 'pending',
        baseline_commit: 'abcdef1234567890',
        head_commit: '9876543210fedcba',
        files: FILES.map(({ path, status }) => ({ path, status })),
      });
    }
    if (pathname.startsWith(`/api/jobs/${JOB_ID}/diff/`) && req.method === 'GET') {
      const path = decodeURIComponent(pathname.slice(`/api/jobs/${JOB_ID}/diff/`.length));
      const content = FILE_CONTENT[path];
      if (!content) return json(res, 404, { detail: 'not in diff' });
      return json(res, 200, {
        job_id: JOB_ID,
        path,
        status: content.status,
        old_content: content.old_content,
        new_content: content.new_content,
      });
    }
    if (pathname.startsWith(`/api/jobs/${JOB_ID}/`)) return json(res, 200, {});

    // ---- the review API ----
    if (pathname === `/api/agents/threads/${THREAD_ID}/cloud-diff` && req.method === 'GET') {
      state.summaryCalls += 1;
      if (state.scenario === 'forbidden') return json(res, 403, { detail: 'Not your thread' });
      if (state.scenario === 'offline') {
        res.destroy();
        return;
      }
      // Keep the hidden pending-count probe failed until the test explicitly
      // releases it. Ended sessions run a bounded post-terminal probe series,
      // so "fail the first call" races that legitimate recovery loop and can
      // turn green or red before the user-facing state is asserted.
      if (state.scenario === 'probeFail' && state.probeBlocked) {
        return json(res, 503, { detail: 'Staging service unavailable' });
      }
      return json(res, 200, summaryBody());
    }
    if (
      pathname.startsWith(`/api/agents/threads/${THREAD_ID}/cloud-diff/`) &&
      req.method === 'GET'
    ) {
      const path = decodeURIComponent(
        pathname.slice(`/api/agents/threads/${THREAD_ID}/cloud-diff/`.length),
      );
      state.fileCalls += 1;
      // Fails once, then succeeds: the per-file Retry button used to be inert
      // because the same-path guard swallowed it.
      if (state.scenario === 'fileFlaky' && state.fileCalls === 1) {
        return json(res, 500, { detail: 'Staged content read failed' });
      }
      if (state.scenario === 'fileGone') {
        return json(res, 404, {
          detail: { code: 'not_in_staged_diff', message: `Path '${path}' is not in the staged diff.` },
        });
      }
      if (state.scenario === 'fileUnreadable') {
        return json(res, 404, {
          detail: {
            code: 'staged_content_unreadable',
            message: `Path '${path}' is staged but its content could not be read.`,
          },
        });
      }
      const content = FILE_CONTENT[path];
      if (!content) {
        return json(res, 404, {
          detail: { code: 'not_in_staged_diff', message: `Path '${path}' is not in the staged diff.` },
        });
      }
      return json(res, 200, { thread_id: THREAD_ID, path, ...content });
    }
    if (pathname === `/api/agents/threads/${THREAD_ID}/cloud-diff/apply`) {
      state.applyCalls += 1;
      // A real apply has been observed at 34.4 seconds. This one waits until
      // the test says so, which is the same window without the wall clock.
      if (state.scenario === 'holdApply') {
        await new Promise((resolve) => {
          applyGate = resolve;
        });
      }
      if (state.scenario === 'conflict') {
        return json(res, 409, {
          detail: {
            code: 'external_modifications_detected',
            message: 'Cloud folder was modified externally since staging.',
            diverged: [
              { path: 'session_apply/change-me.txt', kind: 'etag_mismatch' },
              { path: 'session_apply/untouched.pdf', kind: 'unexpected_at_cloud' },
            ],
          },
        });
      }
      if (state.scenario === 'partial') {
        return json(res, 502, {
          detail: {
            code: 'partial_write_failure',
            applied: 2,
            deleted: 1,
            errors: ['session_apply/new-report.pdf: 507 Insufficient Storage'],
          },
        });
      }
      state.resolved = 'applied';
      state.epoch += 1;
      return json(res, 200, {
        thread_id: THREAD_ID,
        applied: 3,
        deleted: 1,
        errors: [],
        epoch: state.epoch,
        // False on purpose: the workspace pod is gone, which is the normal
        // case for an ended thread and the PC-07 duplicate-diff warning path.
        overlay_reset: false,
      });
    }
    if (pathname === `/api/agents/threads/${THREAD_ID}/cloud-diff/reject`) {
      state.rejectCalls += 1;
      if (state.scenario === 'rejectStale') {
        return json(res, 409, { detail: { code: 'epoch_stale', staged_epoch: 9 } });
      }
      if (state.scenario === 'rejectRefused') {
        return json(res, 422, { detail: { code: 'invalid_epoch' } });
      }
      state.resolved = 'rejected';
      state.epoch += 1;
      return json(res, 200, {
        thread_id: THREAD_ID,
        rejected: true,
        epoch: state.epoch,
        overlay_reset: true,
      });
    }

    // Server-sent-event probes the shell opens on load. They must be a real
    // event-stream or the browser aborts them noisily.
    if (pathname.endsWith('/events')) {
      res.writeHead(200, {
        'Cache-Control': 'no-cache, no-transform',
        'Content-Type': 'text/event-stream',
        Connection: 'keep-alive',
      });
      res.write(': fixture\n\n');
      return;
    }

    // Everything else the shell probes: answer benignly rather than 404, so a
    // missing capability endpoint never masquerades as a review failure.
    if (pathname.startsWith('/api/')) return json(res, 200, {});

    return serveStatic(req, res, pathname);
  } catch (error) {
    json(res, 500, { detail: error instanceof Error ? error.message : 'fixture failure' });
  }
});

function serveStatic(req, res, pathname) {
  let relativePath;
  try {
    relativePath = decodeURIComponent(pathname).replace(/^\/+/, '');
  } catch {
    return json(res, 400, { detail: 'invalid path' });
  }
  let filePath = resolve(DIST_ROOT, relativePath || 'index.html');
  if (!filePath.startsWith(`${DIST_ROOT}/`) && filePath !== DIST_ROOT) {
    return json(res, 403, { detail: 'path escapes build output' });
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
    return json(res, 404, { detail: 'not found' });
  }
  res.writeHead(200, {
    'Cache-Control': 'private, no-store',
    'Content-Length': info.size,
    'Content-Type': MIME_TYPES.get(extname(filePath)) || 'application/octet-stream',
  });
  createReadStream(filePath).pipe(res);
}

function close() {
  server.close(() => process.exit(0));
}
process.on('SIGINT', close);
process.on('SIGTERM', close);

server.listen(PORT, HOST, () => {
  process.stdout.write(`Cloud-review fixture listening on http://${HOST}:${PORT} (${DIST_ROOT})\n`);
});
