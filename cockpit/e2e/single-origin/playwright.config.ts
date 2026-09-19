import {defineConfig} from '@playwright/test';

export default defineConfig({
  testDir: '.',
  testMatch: '*.spec.ts',
  timeout: 180_000,
  expect: {timeout: 20_000},
  workers: 1,
  retries: 0,
  forbidOnly: !!process.env['CI'],
  outputDir: '../../test-results/single-origin',
  reporter: [['list']],
  // These are also explicitly applied to the fresh persistent contexts in
  // the fixture. Neither TLS errors nor service workers may be hidden here.
  use: {ignoreHTTPSErrors: false, serviceWorkers: 'allow'},
  projects: [{name: 'chromium'}, {name: 'firefox'}],
});
