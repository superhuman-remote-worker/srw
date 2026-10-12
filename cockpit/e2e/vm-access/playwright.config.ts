import {defineConfig, devices} from '@playwright/test';

const baseURL = 'http://127.0.0.1:4176';

export default defineConfig({
  testDir: '.',
  testMatch: ['vm-access.spec.ts'],
  timeout: 45_000,
  expect: {timeout: 10_000},
  workers: 1,
  reporter: [['list']],
  outputDir: '../../test-results/vm-access',
  use: {baseURL, serviceWorkers: 'block', trace: 'retain-on-failure'},
  webServer: {
    command: 'CLOUD_REVIEW_PORT=4176 node e2e/cloud-review/fixture-server.mjs',
    cwd: process.cwd(),
    url: `${baseURL}/__e2e/health`,
    reuseExistingServer: false,
    timeout: 20_000,
  },
  projects: [
    {name: 'desktop-en', use: {...devices['Desktop Chrome'], viewport: {width: 1440, height: 900}}},
    {name: 'phone-de', use: {...devices['Desktop Chrome'], viewport: {width: 375, height: 667}, locale: 'de-DE'}},
  ],
});
