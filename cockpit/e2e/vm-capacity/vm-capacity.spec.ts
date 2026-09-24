import {expect, test, type Page} from '@playwright/test';

const USER_ID = '55555555-5555-4555-8555-555555555555';
const JOB_ID = '77777777-7777-4777-8777-777777777777';
const THREAD_ID = '33333333-3333-4333-8333-333333333333';
const REQUEST_ID = '88888888-8888-4888-8888-888888888888';
const zero = {
  cpu_millicores: 0, memory_bytes: 0, ephemeral_storage_bytes: 0,
  kvm_devices: 0, tun_devices: 0, vhost_net_devices: 0,
};
const categories = [
  'allocatable', 'headroom', 'external', 'unbound', 'bound_reserved',
  'active', 'warm', 'teardown', 'available', 'shortfall',
] as const;
const totals = Object.fromEntries(categories.map(category => [category, zero]));
const held = {
  unbound: zero, bound_reserved: zero, active: zero, warm: zero, teardown: zero,
  total: {...zero, cpu_millicores: 2000, ephemeral_storage_bytes: null},
};

async function admin(page: Page) {
  await page.route('**/api/auth/me', route => route.fulfill({json: {user: {
    id: USER_ID, email: 'admin@example.invalid', display_name: 'Capacity Operator',
    is_admin: true, is_approved: true, can_use_vm: true,
  }}}));
  await page.route('**/api/users/me/capabilities', route => route.fulfill({json: {
    is_admin: true, grants: null, catalog: {actions: [], config: [], groups: []},
    features: {protected_cloud: true},
  }}));
}

function capacity(fresh = false) {
  return {
    observed_at: '2026-09-24T04:00:00Z',
    executors: {total: 3, ready: 2, busy: 1},
    queued: {session_turn: 0, worker_batch: 0, total: 0},
    oldest_queued_age_s: 0, desired: 2,
    params: {min_replicas: 1, reserve: 1}, parked: [],
    vm: {available: fresh, reason: fresh ? null : 'inventory_stale',
      observed_at: '2026-09-24T04:00:00Z', clusters: [{
        cluster_id: 'campus-a', namespace: 'workers', mode: fresh ? 'shadow' : 'drain',
        policy_digest: 'sha256:' + 'a'.repeat(64),
        available: fresh, reason: fresh ? null : 'inventory_stale',
        inventory: fresh ? {observed_at: '2026-09-24T04:00:00Z',
          received_at: '2026-09-24T04:00:01Z', age_seconds: 1, complete: true,
          fresh: true, stale_after_seconds: 60} : null,
        held, waiting: {count: 2, nonfit: 1, oldest_age_seconds: 80, bypasses: 3, protected: 1},
        teardown: {count: 1, unknown_age: 1, overdue: 0,
          oldest_progress_age_seconds: null, overdue_after_seconds: 300},
        totals: fresh ? totals : null,
        nodes: fresh ? [{name: 'worker-a', general_exclusion: null,
          request_fit_required: true, resources: totals}] : null,
        orphaned_held: fresh ? {count: 0, resources: zero} : null,
        pending_external: fresh ? zero : null,
        count_backstop: {observed: fresh ? 0 : null, maximum: null,
          reason: 'maximum_not_observed'},
      }]},
  };
}

test('operator sees durable holds and unknown count without guessed availability', async ({page}, info) => {
  await admin(page);
  await page.route('**/api/admin/capacity', route => route.fulfill({json: capacity()}));
  await page.goto('/admin/capacity', {waitUntil: 'domcontentloaded'});
  const vm = page.getByTestId('vm-capacity');
  await expect(vm).toBeVisible();
  await expect(vm.getByTestId('vm-held-table')).toContainText(info.project.name === 'phone-de' ? '2.000' : '2,000');
  await expect(vm.getByTestId('vm-held-table')).toContainText('–');
  await expect(vm.getByTestId('vm-totals-table')).toHaveCount(0);
  await expect(vm).toContainText('– / –');
  expect(await page.evaluate(() => document.documentElement.scrollWidth - innerWidth)).toBeLessThanOrEqual(1);
  await vm.getByTestId('vm-held-table').scrollIntoViewIfNeeded();
  await page.screenshot({path: `test-results/vm-capacity/${info.project.name}-stale.png`});
});

test('operator can inspect node accounting with keyboard and sees observed zero', async ({page}, info) => {
  await admin(page);
  await page.route('**/api/admin/capacity', route => route.fulfill({json: capacity(true)}));
  await page.goto('/admin/capacity', {waitUntil: 'domcontentloaded'});
  const vm = page.getByTestId('vm-capacity');
  await expect(vm).toContainText('0 / –');
  const summary = vm.locator('details summary');
  await summary.focus();
  await expect(summary).toBeFocused();
  await summary.press('Enter');
  await expect(vm.getByText('worker-a')).toBeVisible();
  await expect(vm.getByTestId('vm-totals-table').locator('tbody tr')).toHaveCount(10);
  expect(await page.evaluate(() => document.documentElement.scrollWidth - innerWidth)).toBeLessThanOrEqual(1);
  await vm.getByText('worker-a').scrollIntoViewIfNeeded();
  await page.screenshot({path: `test-results/vm-capacity/${info.project.name}-nodes.png`});
});

test('operator receives a visible capacity load error', async ({page}) => {
  await admin(page);
  await page.route('**/api/admin/capacity', route => route.fulfill({status: 503, json: {detail: 'unavailable'}}));
  await page.goto('/admin/capacity', {waitUntil: 'domcontentloaded'});
  await expect(page.getByRole('alert')).toBeVisible();
  await expect(page.getByTestId('vm-capacity')).toHaveCount(0);
});

test('owners see only their exact Job and session wait, with no fleet budget', async ({page}, info) => {
  const resource = {kind: 'resource', since: '2026-09-24T04:00:00Z', size_nonfit: true,
    guest_vcpus: 2, guest_memory_bytes: 2147483648};
  const creation = {request_id: REQUEST_ID, state: 'queued', stage: 'creation',
    reason_code: 'resource_wait', message: 'Waiting for VM workspace resources.',
    wait: resource, resumable: false};
  await page.route(url => url.pathname === '/api/jobs', route => route.fulfill({json: {
    jobs: [{id: JOB_ID, description: 'Owner VM job', status: 'created',
      user_id: USER_ID, created_at: '2026-09-24T04:00:00Z',
      updated_at: '2026-09-24T04:00:00Z', is_display_root: true, vm_creation: creation}],
    total: 1, has_more: false, total_is_capped: false,
    counts: {created: 1}, status_counts: {created: 1},
  }}));
  await page.goto('/jobs', {waitUntil: 'domcontentloaded'});
  await expect(page.getByTestId('vm-owner-wait')).toHaveAttribute('data-kind', 'resource');
  await expect(page.getByTestId('vm-owner-wait')).toContainText('2 vCPU');
  await expect(page.getByTestId('vm-owner-wait')).toContainText('2.0 GiB');
  await expect(page.locator('body')).not.toContainText('campus-a');
  await page.screenshot({path: `test-results/vm-capacity/${info.project.name}-job-wait.png`});

  await page.route(url => url.pathname === '/api/persistent/threads', route => route.fulfill({json: {
    threads: [{id: THREAD_ID, title: 'Owner pinned session', kind: 'session',
      status: 'created', config_name: 'session_base', permission_mode: 'supervised',
      user_id: USER_ID, created_at: '2026-09-24T04:00:00Z',
      last_activity: '2026-09-24T04:00:00Z', total_turns: 0, total_tokens: 0,
      vm_creation: {...creation, wait: {kind: 'count', since: null,
        size_nonfit: false, guest_vcpus: null, guest_memory_bytes: null}}}],
  }}));
  await page.goto('/sessions', {waitUntil: 'domcontentloaded'});
  await expect(page.getByTestId('vm-owner-wait')).toHaveAttribute('data-kind', 'count');
  await expect(page.getByTestId('vm-owner-wait')).not.toContainText('campus-a');
  await expect(page.getByTestId('vm-owner-wait')).not.toContainText('2 vCPU');
  await page.screenshot({path: `test-results/vm-capacity/${info.project.name}-session-wait.png`});
});
