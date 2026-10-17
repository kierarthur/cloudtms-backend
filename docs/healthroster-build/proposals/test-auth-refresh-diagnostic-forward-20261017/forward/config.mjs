import { defineConfig, devices } from 'file:///D:/Development/GitHub/TEST-Frontend/node_modules/@playwright/test/index.mjs';

export default defineConfig({
  // Consumed by the standalone Playwright child; the test runner is never invoked.
  timeout: 60_000,
  expect: {
    timeout: 15_000
  },
  retries:0, reporter:[],
  use: {
    baseURL: 'https://testmode.arthur-rai.co.uk',
    ignoreHTTPSErrors: false,
    trace: 'off',
    screenshot: 'off',
    video: 'off'
  },
  headless:true,
  serviceWorkers:'block',
  viewport:devices['Desktop Chrome'].viewport,
  storageState:{cookies:[],origins:[]}
});
