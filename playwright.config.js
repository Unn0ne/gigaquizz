const { defineConfig } = require('@playwright/test');
const fs = require('node:fs');
const localChrome = '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome';
const envURL = fs.existsSync('.env') ? fs.readFileSync('.env', 'utf8').split('\n').find(line => line.startsWith('PUBLIC_URL='))?.slice('PUBLIC_URL='.length).trim() : '';
const baseURL = process.env.E2E_BASE_URL || envURL || 'http://127.0.0.1:8080';
if (!['127.0.0.1', '[::1]', 'localhost'].includes(new URL(baseURL).hostname)) {
  throw new Error('Browser tests create real polls and require a local target');
}

module.exports = defineConfig({
  testDir: './tests/browser',
  timeout: 100000,
  expect: { timeout: 10000 },
  workers: 1,
  retries: 0,
  reporter: 'list',
  use: {
    baseURL,
    browserName: 'chromium',
    launchOptions: fs.existsSync(localChrome) ? { executablePath: localChrome } : {},
    screenshot: 'only-on-failure',
    // Traces can capture tokens/passwords. Keep them disabled by default.
    trace: 'off',
  },
});
