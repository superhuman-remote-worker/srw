import {
  expect,
  test as setup,
  type Browser,
  type BrowserContext,
  type Page,
} from '@playwright/test';
import { normalizedUrl, requireJson } from './api';
import {
  ATTACH_MODE,
  AUTH_STATE_CANDIDATE_PATH,
  AUTH_STATE_PATH,
  BASE_ORIGIN,
  BASE_URL,
  OWNED_BROWSER_RERUN,
  appUrl,
  removePrivateFile,
  runtimeEnvironment,
  writePrivateJsonFile,
  type RuntimeEnvironment,
} from './environment';

const CSRF_HEADERS = { 'X-CSRF': '1' };

interface UserIdentity {
  id: string;
  default_project_id?: string | null;
  email?: string | null;
  is_admin: boolean;
  is_approved: boolean;
}

interface AuthMeResponse {
  user: UserIdentity;
}

interface AdminUser extends UserIdentity {
  display_name?: string;
}

interface CatalogModel {
  provider_kind: string;
  provider_ref: string;
  model_id: string;
  capabilities: string[];
  enabled: boolean;
}

interface ProviderEndpoint {
  id: string;
  base_url: string;
  key_prefix?: string | null;
}

interface ExpertDefaults {
  defaults: {
    worker?: { id: string; expert_type: string } | null;
    session?: { id: string; expert_type: string } | null;
  };
}

interface Readiness {
  ready: boolean;
  missing_providers: string[];
  missing_capabilities: string[];
  missing_defaults: string[];
  missing_expert_defaults: string[];
}

async function login(context: BrowserContext, username: string, password: string): Promise<Page> {
  const page = await context.newPage();
  await page.goto(appUrl('/auth/login?return_to=/'), { waitUntil: 'domcontentloaded' });

  // A Keycloak SSO cookie can omit the login form. The contexts are initially
  // empty in the authoritative run, but handling that redirect keeps an
  // explicit attach-mode rerun correct without weakening identity checks.
  if (new URL(page.url()).origin !== BASE_ORIGIN) {
    const usernameInput = page.locator('#username');
    await expect(usernameInput).toBeVisible({ timeout: 30_000 });
    await usernameInput.fill(username);
    await page.locator('#password').fill(password);
    await page.locator('#kc-login').click();
  }

  await page.waitForURL((url) => url.origin === BASE_ORIGIN, { timeout: 60_000 });
  const cookies = await context.cookies(BASE_URL);
  expect(cookies.some(({ name }) => name === 'srw_session')).toBe(true);
  return page;
}

async function currentUser(context: BrowserContext, label: string): Promise<UserIdentity> {
  const body = await requireJson<AuthMeResponse>(
    await context.request.get(appUrl('/api/auth/me')),
    `${label} identity check`,
  );
  expect(typeof body.user?.id).toBe('string');
  expect(body.user.id.length).toBeGreaterThan(0);
  return body.user;
}

async function bootstrapCatalogAndReadiness(
  admin: BrowserContext,
  journey: BrowserContext,
  environment: RuntimeEnvironment,
  journeyIdentity: UserIdentity,
): Promise<void> {
  const keys = await requireJson<unknown[]>(
    await admin.request.get(appUrl('/api/admin/providers/keys')),
    'system provider-key inventory',
  );
  expect(keys, 'The deterministic stack must not mount real system provider keys.').toEqual([]);

  const models = await requireJson<CatalogModel[]>(
    await admin.request.get(appUrl('/api/admin/providers/models')),
    'fixture model catalog',
  );
  const enabledModels = models.filter(({ enabled }) => enabled);
  // `rerank` is a required capability with its own catalog slot, so an enabled
  // rerank row is expected: this profile seeds one through `llm.seed`, and an
  // upgraded deployment gets one from migration 0242 instead. Either way the
  // row is still checked — it must carry the rerank capability and nothing
  // else, and every OTHER enabled row must still be exactly the two fixture
  // models, so a stray enabled chat model would still fail here.
  const autoRerankRows = enabledModels.filter(({ capabilities }) =>
    capabilities.includes('rerank'),
  );
  for (const row of autoRerankRows) {
    expect(row.capabilities, 'the auto-seeded rerank row carries only rerank').toEqual([
      'rerank',
    ]);
  }
  expect(
    enabledModels
      .filter(({ capabilities }) => !capabilities.includes('rerank'))
      .map(({ model_id }) => model_id)
      .sort(),
  ).toEqual([environment.chatModel, environment.embeddingModel].sort());

  const chatRows = enabledModels.filter(({ model_id }) => model_id === environment.chatModel);
  const embeddingRows = enabledModels.filter(
    ({ model_id }) => model_id === environment.embeddingModel,
  );
  expect(chatRows).toHaveLength(1);
  expect(embeddingRows).toHaveLength(1);
  expect([...chatRows[0].capabilities].sort()).toEqual(['auxiliary', 'chat']);
  expect([...embeddingRows[0].capabilities].sort()).toEqual(['embedding']);
  expect(chatRows[0].provider_kind).toBe('endpoint');
  expect(embeddingRows[0].provider_kind).toBe('endpoint');
  expect(embeddingRows[0].provider_ref).toBe(chatRows[0].provider_ref);

  const endpoints = await requireJson<ProviderEndpoint[]>(
    await admin.request.get(appUrl('/api/admin/providers/endpoints')),
    'fixture provider endpoint inventory',
  );
  expect(endpoints).toHaveLength(1);
  expect(endpoints[0].id).toBe(chatRows[0].provider_ref);
  expect(normalizedUrl(endpoints[0].base_url)).toBe(normalizedUrl(environment.providerBaseUrl));
  expect(
    endpoints[0].key_prefix,
    'The endpoint-backed embedding contract requires a dummy key.',
  ).toBeTruthy();

  if (!ATTACH_MODE) {
    for (const [kind, model] of [
      ['chat', environment.chatModel],
      ['auxiliary', environment.chatModel],
      ['embedding', environment.embeddingModel],
    ] as const) {
      const response = await admin.request.put(appUrl(`/api/admin/providers/defaults/${kind}`), {
        headers: CSRF_HEADERS,
        data: { model },
      });
      const pinned = await requireJson<{ kind: string; model: string | null }>(
        response,
        `${kind} model-default pin`,
      );
      expect(pinned).toEqual({ kind, model });
    }
  }

  const defaults = await requireJson<Record<string, string | null>>(
    await admin.request.get(appUrl('/api/admin/providers/defaults')),
    'model-default verification',
  );
  expect(defaults['chat']).toBe(environment.chatModel);
  expect(defaults['auxiliary']).toBe(environment.chatModel);
  expect(defaults['embedding']).toBe(environment.embeddingModel);

  const experts = await requireJson<ExpertDefaults>(
    await admin.request.get(appUrl('/api/admin/expert-defaults')),
    'managed expert-default verification',
  );
  expect(experts.defaults.worker).toMatchObject({ expert_type: 'worker' });
  expect(experts.defaults.session).toMatchObject({ expert_type: 'session' });

  // Workspace selection belongs to Project defaults; the retired account
  // preference is deliberately ignored. Only the generated journey identity's
  // personal Project may be changed, and attach mode remains read-only.
  const projectId = journeyIdentity.default_project_id;
  expect(typeof projectId, 'The journey user must have a personal Project.').toBe('string');
  expect(projectId).toBeTruthy();
  const workspaceDefaultsUrl = appUrl(
    `/api/projects/${encodeURIComponent(projectId as string)}/workspace-defaults`,
  );
  type WorkspaceDefaults = {
    can_edit: boolean;
    managed_by_manifest: boolean;
    account_templates: boolean;
    stored: {
      jobs: string | null;
      sessions: string | null;
      container: { ref: { name: string; scope: Record<string, string> } } | null;
      vm: { ref: { name: string; scope: Record<string, string> } } | null;
    };
    effective: { sessions: { mode: string } };
  };
  const before = await requireJson<WorkspaceDefaults>(
    await journey.request.get(workspaceDefaultsUrl),
    'personal Project workspace-default inventory',
  );
  if (!ATTACH_MODE && environment.workspaceBackend === 'sandbox') {
    expect(before.can_edit).toBe(true);
    expect(before.managed_by_manifest).toBe(false);
    expect(before.account_templates, 'Only the journey user\'s personal Project may change.').toBe(true);
    expect(Object.keys(before.stored).sort()).toEqual(['container', 'jobs', 'sessions', 'vm']);
    for (const tier of ['container', 'vm'] as const) {
      if (before.stored[tier] !== null) {
        expect(before.stored[tier]).toHaveProperty('ref');
        expect(before.stored[tier]?.ref).toBeTruthy();
      }
    }
    const updated = await requireJson<WorkspaceDefaults>(
      await journey.request.put(workspaceDefaultsUrl, {
        headers: CSRF_HEADERS,
        // PUT replaces the whole record; retain all other Project defaults.
        data: {
          jobs: before.stored.jobs,
          sessions: 'container',
          container: before.stored.container?.ref ?? null,
          vm: before.stored.vm?.ref ?? null,
        },
      }),
      'journey Project workspace-profile selection',
    );
    expect(updated.effective.sessions.mode).toBe('container');
  }
  const workspaceDefaults = await requireJson<WorkspaceDefaults>(
    await journey.request.get(workspaceDefaultsUrl),
    'journey Project workspace-profile verification',
  );
  expect(workspaceDefaults.effective.sessions.mode).toBe(
    environment.workspaceBackend === 'sandbox' ? 'container' : 'virtual',
  );

  const preferences = await requireJson<Record<string, unknown>>(
    await journey.request.get(appUrl('/api/settings/preferences')),
    'journey preference verification',
  );
  // Underscore keys are computed by the server, not stored: _resolved holds
  // the defaults, _unavailable the stored models that can no longer run.
  const explicitPreferenceKeys = Object.keys(preferences)
    .filter((key) => !key.startsWith('_'))
    .sort();
  expect(preferences['_unavailable']).toEqual({});
  // An owned browser rerun can retain the empty object written by the old
  // bootstrap before workspace_backend was retired. It is not a preference.
  if (OWNED_BROWSER_RERUN && explicitPreferenceKeys.includes('persistent_agent')) {
    expect(preferences['persistent_agent']).toEqual({});
    expect(explicitPreferenceKeys).toEqual(['persistent_agent']);
  } else {
    expect(explicitPreferenceKeys).toEqual([]);
  }
  expect(preferences['_resolved']).toMatchObject({
    default_model: environment.chatModel,
    default_auxiliary_model: environment.chatModel,
    persistent_agent: {
      model: environment.chatModel,
    },
  });
  expect(preferences['_resolved']).not.toHaveProperty('persistent_agent.workspace_backend');

  const readiness = await requireJson<Readiness>(
    await journey.request.get(appUrl('/api/system/readiness')),
    'authenticated system readiness',
  );
  expect(readiness).toMatchObject({
    ready: true,
    missing_providers: [],
    missing_capabilities: [],
    missing_defaults: [],
    missing_expert_defaults: [],
  });
}

async function persistAndProveJourneyState(
  browser: Browser,
  journey: BrowserContext,
): Promise<void> {
  removePrivateFile(AUTH_STATE_CANDIDATE_PATH);
  removePrivateFile(AUTH_STATE_PATH);

  writePrivateJsonFile(AUTH_STATE_CANDIDATE_PATH, await journey.storageState());

  const restored = await browser.newContext({
    baseURL: BASE_URL,
    storageState: AUTH_STATE_CANDIDATE_PATH,
    ignoreHTTPSErrors: true,
    serviceWorkers: 'block',
  });
  try {
    const identity = await currentUser(restored, 'fresh-context journey');
    expect(identity.is_approved).toBe(true);
    expect(identity.is_admin).toBe(false);
    writePrivateJsonFile(AUTH_STATE_PATH, await restored.storageState());
  } finally {
    await restored.close();
    removePrivateFile(AUTH_STATE_CANDIDATE_PATH);
  }
}

setup(
  'authenticate generated identities and prove the deterministic stack',
  async ({ browser }) => {
    setup.setTimeout(90_000);
    const environment = runtimeEnvironment();
    const admin = await browser.newContext({
      baseURL: BASE_URL,
      ignoreHTTPSErrors: true,
      serviceWorkers: 'block',
    });
    const journey = await browser.newContext({
      baseURL: BASE_URL,
      ignoreHTTPSErrors: true,
      serviceWorkers: 'block',
    });

    try {
      await login(admin, environment.adminUsername, environment.adminPassword);
      const adminIdentity = await currentUser(admin, 'bootstrap administrator');
      expect(adminIdentity.is_admin).toBe(true);
      expect(adminIdentity.is_approved).toBe(true);

      await login(journey, environment.username, environment.password);
      let journeyIdentity = await currentUser(journey, 'journey user');
      expect(journeyIdentity.is_admin).toBe(false);

      const users = await requireJson<AdminUser[]>(
        await admin.request.get(appUrl('/api/admin/users')),
        'administrator user inventory',
      );
      const exactJourneyRows = users.filter(({ id }) => id === journeyIdentity.id);
      expect(exactJourneyRows).toHaveLength(1);

      if (ATTACH_MODE) {
        expect(
          journeyIdentity.is_approved,
          'Attach mode is verification-only; supply an already-approved journey user.',
        ).toBe(true);
      } else if (!OWNED_BROWSER_RERUN) {
        expect(
          journeyIdentity.is_approved,
          'An owned-cluster journey identity must begin pending so admission is exercised.',
        ).toBe(false);
        const approved = await admin.request.patch(
          appUrl(`/api/admin/users/${encodeURIComponent(journeyIdentity.id)}`),
          { headers: CSRF_HEADERS, data: { is_approved: true } },
        );
        await requireJson<{ status: string }>(approved, 'exact journey-user approval');
      } else if (!journeyIdentity.is_approved) {
        // A prior owned browser attempt may have failed either before or after
        // approval. Preserve the strict pending-user assertion on attempt one,
        // while making the documented browser-only rerun path idempotent.
        const approved = await admin.request.patch(
          appUrl(`/api/admin/users/${encodeURIComponent(journeyIdentity.id)}`),
          { headers: CSRF_HEADERS, data: { is_approved: true } },
        );
        await requireJson<{ status: string }>(approved, 'rerun journey-user approval');
      }

      journeyIdentity = await currentUser(journey, 'approved journey user');
      expect(journeyIdentity.is_approved).toBe(true);
      expect(journeyIdentity.is_admin).toBe(false);

      await bootstrapCatalogAndReadiness(admin, journey, environment, journeyIdentity);
      await persistAndProveJourneyState(browser, journey);
    } finally {
      await Promise.allSettled([admin.close(), journey.close()]);
    }
  },
);
