const { test, expect } = require('@playwright/test');
const fs = require('node:fs');

function administratorPassword() {
  if (process.env.ADMIN_PASSWORD) return process.env.ADMIN_PASSWORD;
  const line = fs.readFileSync('.env', 'utf8').split('\n').find(value => value.startsWith('ADMIN_PASSWORD='));
  if (!line) throw new Error('Configure ADMIN_PASSWORD before running browser tests');
  return line.slice('ADMIN_PASSWORD='.length);
}

// Artifacts must not contain authentication material, tokens or poll IDs.
test.use({ screenshot: 'off', trace: 'off', video: 'off' });
test('durable attempts, browser identity, lost responses, real deadline and exact final results', async ({ browser, baseURL }) => {
  test.setTimeout(170000);
  const { runE2E } = await import('../../scripts/e2e.mjs');
  const result = await runE2E({ browser, baseURL, password: administratorPassword() });
  expect(result.passed).toBe(true);
  expect(result.exact_unique_votes).toBe(3);
});
