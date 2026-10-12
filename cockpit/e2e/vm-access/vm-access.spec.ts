import {expect, test, type Page} from '@playwright/test';

const JOB_ID = '77777777-7777-4777-8777-777777777777';
const THREAD_ID = '33333333-3333-4333-8333-333333333333';
const LEASE_ID = '88888888-8888-4888-8888-888888888888';

async function openJobs(
  page: Page,
  state: 'suspended' | 'wake_held' | 'unsupported' = 'suspended',
  ide: 'ready' | 'error' | 'restoring' = 'ready',
) {
  const calls = {post: 0, get: 0, delete: 0};
  const job = {
    id: JOB_ID,
    description: 'Retained VM report workspace',
    status: 'paused',
    user_id: '55555555-5555-4555-8555-555555555555',
    created_at: '2026-09-24T07:00:00Z',
    updated_at: '2026-09-24T07:00:00Z',
    is_display_root: true,
    workspace_lifecycle: {
      state,
      reason_code: state === 'wake_held' ? 'resource_reservation_unavailable' : null,
      idle_expires_at: null,
      next_retry_at: null,
    },
  };
  await page.route(`**/api/jobs/${JOB_ID}/ide**`, async route => {
    const method = route.request().method();
    if (method === 'DELETE') {
      calls.delete++;
      return route.fulfill({json: {status: 'closed'}});
    }
    if (method === 'POST') {
      calls.post++;
      if (ide === 'error') return route.fulfill({status: 503, json: {detail: 'Guest IDE unavailable'}});
      if (ide === 'restoring') {
        return route.fulfill({json: {status: 'restoring', access_lease_id: LEASE_ID}});
      }
      return route.fulfill({json: {
        status: 'active', access_lease_id: LEASE_ID,
        code_server_url: 'http://127.0.0.1:4176/__e2e/health',
      }});
    }
    calls.get++;
    expect(new URL(route.request().url()).searchParams.get('lease_id')).toBe(LEASE_ID);
    return route.fulfill({json: {
      status: 'active', access_lease_id: LEASE_ID,
      code_server_url: 'http://127.0.0.1:4176/__e2e/health',
    }});
  });
  await page.route(url => url.pathname === '/api/jobs', route => route.fulfill({json: {
    jobs: [job], total: 1, has_more: false, total_is_capped: false,
    counts: {paused: 1}, status_counts: {paused: 1},
  }}));
  await page.goto('/jobs', {waitUntil: 'domcontentloaded'});
  await expect(page.locator('.job-table tbody tr').first()).toContainText(job.description);
  return calls;
}

async function openActionMenu(page: Page, expectIde = true) {
  const trigger = page.locator('.job-table tbody tr').first().getByRole('button', {
    name: /More actions|Weitere Aktionen/,
  });
  await trigger.focus();
  await expect(trigger).toBeFocused();
  await trigger.press('ArrowDown');
  if (expectIde) await expect(page.getByRole('menuitem', {name: /IDE/})).toBeVisible();
}

test('owner sees lifecycle and opens a suspended VM IDE from the keyboard', async ({page}, info) => {
  const calls = await openJobs(page);
  await expect(page.locator('.job-table tbody tr').first()).toContainText(
    info.project.name === 'phone-de' ? 'Arbeitsbereich ruht' : 'Workspace asleep',
  );
  await openActionMenu(page);
  const ide = page.getByRole('menuitem', {name: /IDE/});
  await ide.focus();
  const popupPromise = page.waitForEvent('popup');
  await ide.press('Enter');
  const popup = await popupPromise;
  await expect(popup).toHaveURL('http://127.0.0.1:4176/__e2e/health');
  expect(calls).toEqual({post: 1, get: 0, delete: 0});
  expect(await page.evaluate(() => document.documentElement.scrollWidth - innerWidth)).toBeLessThanOrEqual(1);
  await page.screenshot({path: `test-results/vm-access/${info.project.name}-suspended.png`});
  await popup.close();
});

test('owner sees a capacity reason and an IDE error without exposing runtime identifiers', async ({page}, info) => {
  const calls = await openJobs(page, 'wake_held', 'error');
  const row = page.locator('.job-table tbody tr').first();
  await expect(row).toContainText(info.project.name === 'phone-de'
    ? 'Warten auf freie Kapazität' : 'Waiting for workspace capacity');
  await expect(row).not.toContainText('vm_uid');
  await openActionMenu(page);
  const popupPromise = page.waitForEvent('popup');
  await page.getByRole('menuitem', {name: /IDE/}).click();
  const popup = await popupPromise;
  await expect.poll(() => popup.isClosed()).toBe(true);
  await expect(page.getByText(info.project.name === 'phone-de'
    ? 'Die IDE des Arbeitsbereichs ist nicht verfügbar. Versuchen Sie es in Kürze erneut.'
    : 'The workspace IDE is unavailable. Try opening it again shortly.')).toBeVisible();
  expect(calls.post).toBe(1);
  expect(calls.get).toBe(0);
  await page.screenshot({path: `test-results/vm-access/${info.project.name}-error.png`});
});

test('a waking VM polls only its admitted lease before opening', async ({page}) => {
  const calls = await openJobs(page, 'suspended', 'restoring');
  await openActionMenu(page);
  const popupPromise = page.waitForEvent('popup');
  await page.getByRole('menuitem', {name: /IDE/}).click();
  const popup = await popupPromise;
  await expect(popup).toHaveURL('http://127.0.0.1:4176/__e2e/health', {timeout: 10_000});
  expect(calls).toEqual({post: 1, get: 1, delete: 0});
  await popup.close();
});

test('unsupported workspace does not offer VM IDE action', async ({page}) => {
  const calls = await openJobs(page, 'unsupported');
  await openActionMenu(page, false);
  await expect(page.getByRole('menuitem', {name: /IDE/})).toHaveCount(0);
  expect(calls).toEqual({post: 0, get: 0, delete: 0});
});

test('asleep pinned session keeps its IDE access action without a connected agent', async ({page}, info) => {
  let admissions = 0;
  await page.route(url => url.pathname === `/api/persistent/threads/${THREAD_ID}`, route =>
    route.fulfill({json: {
      id: THREAD_ID, thread_id: THREAD_ID, title: 'Retained pinned session',
      status: 'suspended', config_name: 'session_base', permission_mode: 'supervised',
      user_id: '55555555-5555-4555-8555-555555555555',
      total_turns: 1, total_tokens: 10, created_at: '2026-09-24T07:00:00Z',
      last_activity: '2026-09-24T07:00:00Z', metadata: {},
      workspace_lifecycle: {state: 'suspended', reason_code: null},
    }}));
  await page.route(url => url.pathname === `/api/persistent/threads/${THREAD_ID}/state`, route =>
    route.fulfill({json: {
      thread_id: THREAD_ID, permission_mode: 'supervised', narration_mode: 'auto',
      turn_count: 1, turn_in_flight: false, message_count: 0,
      model: null, temperature: null, running_tool: null, pending_permissions: [],
      event_cursor: {epoch: 1, seq: 0}, replay_cursor: {epoch: 1, seq: 0},
      snapshot_source: 'durable_journal',
    }}));
  await page.route(`**/api/persistent/threads/${THREAD_ID}/ide**`, route => {
    if (route.request().method() === 'POST') {
      admissions++;
      return route.fulfill({json: {
        status: 'active', access_lease_id: LEASE_ID,
        code_server_url: 'http://127.0.0.1:4176/__e2e/health',
      }});
    }
    return route.fulfill({json: {status: 'available'}});
  });
  await page.goto(`/sessions/${THREAD_ID}`, {waitUntil: 'domcontentloaded'});
  await expect(page.locator('.vm-lifecycle')).toContainText(info.project.name === 'phone-de'
    ? 'Arbeitsbereich ruht' : 'Workspace asleep');
  const menu = page.locator('.chat-header').getByRole('button', {
    name: info.project.name === 'phone-de' ? 'Weitere Aktionen' : 'More actions',
  });
  await menu.press('ArrowDown');
  const ide = page.getByRole('menuitem', {name: /IDE/});
  await expect(ide).toBeVisible();
  const popupPromise = page.waitForEvent('popup');
  await ide.click();
  const popup = await popupPromise;
  await expect(popup).toHaveURL('http://127.0.0.1:4176/__e2e/health');
  expect(admissions).toBe(1);
  await page.screenshot({path: `test-results/vm-access/${info.project.name}-thread-suspended.png`});
  await popup.close();
});
