const { test, expect } = require('@playwright/test');
const fs = require('node:fs');
const path = require('node:path');
test.use({ screenshot: 'off', trace: 'off', video: 'off' });

test('Retry-After delays manual retry; closed refusal preserves uncertainty without automatic requests', async ({ page }) => {
  const assetRoot = path.resolve(__dirname, '../../internal/web/static');
  await page.route('**/static/*', route => {
    const file = path.basename(new URL(route.request().url()).pathname);
    return route.fulfill({ body: fs.readFileSync(path.join(assetRoot, file)), contentType: file.endsWith('.js') ? 'text/javascript' : 'text/css' });
  });
  await page.route('**/p/*', route => route.fulfill({ body: fs.readFileSync(path.join(assetRoot, 'poll.html')), contentType: 'text/html' }));
  const id = '00112233-4455-6677-8899-aabbccddeeff';
  const now = Date.now();
  const bodies = [];
  await page.route(`**/api/polls/${id}`, route => route.fulfill({ json: {
    id, question: 'Проверка повтора', type: 'single', options: [{ id: 1, label: 'Да' }, { id: 2, label: 'Нет' }],
    starts_at: new Date(now - 57000).toISOString(), ends_at: new Date(now + 3000).toISOString(),
    state: 'open', server_time: new Date().toISOString(),
  } }));
  await page.route(`**/api/polls/${id}/votes`, route => {
    bodies.push(route.request().postDataJSON());
    return bodies.length === 1
      ? route.fulfill({ status: 503, headers: { 'Retry-After': '4' }, json: { error: 'Temporary failure', outcome: 'unknown' } })
      : route.fulfill({ status: 410, json: { status: 'closed' } });
  });
  await page.goto(`/p/${id}`);
  await page.getByLabel('Да', { exact: true }).check();
  await page.locator('#vote-button').click();
  await expect(page.locator('#vote-button')).toBeDisabled();
  await expect(page.locator('#vote-button')).toContainText('Повторить через');
  await expect(page.locator('#vote-button')).toBeEnabled({ timeout: 10000 });
  await expect(page.locator('#countdown')).toHaveText('Приём ответов закрыт');
  expect(bodies.length).toBe(1);
  await page.locator('#vote-button').click();
  await expect(page.locator('#receipt-panel')).toBeHidden();
  await expect(page.locator('#vote-button')).toBeDisabled();
  await expect(page.locator('#vote-message')).toContainText('её исход здесь неизвестен');
  expect(bodies.length).toBe(2);
  expect(bodies[0].token === bodies[1].token).toBe(true);
  expect(bodies[0].choices).toEqual(bodies[1].choices);
});
