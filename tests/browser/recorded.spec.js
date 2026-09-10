const { test, expect } = require('@playwright/test');
const fs = require('node:fs');
const path = require('node:path');

test.use({ screenshot: 'off', trace: 'off', video: 'off' });
const id = '00112233-4455-6677-8899-aabbccddeeff';
const assetRoot = path.resolve(__dirname, '../../internal/web/static');

async function assets(page, name) {
  await page.route('**/static/*', route => {
    const file = path.basename(new URL(route.request().url()).pathname);
    return route.fulfill({ body: fs.readFileSync(path.join(assetRoot, file)), contentType: file.endsWith('.js') ? 'text/javascript' : 'text/css' });
  });
  await page.route(name === 'admin' ? '**/admin' : '**/p/*', route => route.fulfill({ body: fs.readFileSync(path.join(assetRoot, name + '.html')), contentType: 'text/html' }));
}

function poll() {
  const now = Date.now();
  return { id, question: 'Выбор встречи', type: 'single', options: [{ id: 1, label: 'Очно' }, { id: 2, label: 'Онлайн' }], starts_at: new Date(now - 1000).toISOString(), ends_at: new Date(now + 59000).toISOString(), state: 'open', server_time: new Date(now).toISOString() };
}

test('202 persists the sent attempt and does not claim a canonical choice', async ({ page }) => {
  await assets(page, 'poll');
  await page.route(`**/api/polls/${id}`, route => route.fulfill({ json: poll() }));
  let requests = 0;
  await page.route(`**/api/polls/${id}/votes`, route => {
    requests++;
    return route.fulfill({ status: 202, json: { status: 'recorded', choices: [2] } });
  });
  await page.goto(`/p/${id}`);
  await page.getByLabel('Очно', { exact: true }).check();
  await page.locator('#vote-button').click();
  await expect(page.locator('#receipt-panel')).toBeVisible();
  await expect(page.locator('#receipt-title')).toHaveText('Ответ сохранён');
  await expect(page.locator('#receipt-description')).toHaveText('При повторных ответах учитывается первый.');
  await expect(page.getByLabel('Очно', { exact: true })).toBeChecked();
  await expect(page.getByLabel('Онлайн', { exact: true })).not.toBeChecked();
  await page.reload();
  await expect(page.locator('#receipt-panel')).toBeVisible();
  await expect(page.getByLabel('Очно', { exact: true })).toBeChecked();
  expect(requests).toBe(1);
});

test('pending results hide numbers until a final aggregate is available', async ({ page }) => {
  await assets(page, 'admin');
  const current = poll();
  let pending = true;
  await page.route('**/api/admin/session', route => route.fulfill({ json: { authenticated: true } }));
  await page.route('**/api/admin/polls', route => route.fulfill({ json: { polls: [current] } }));
  await page.route(`**/api/admin/polls/${id}/results`, route => route.fulfill({ json: {
    poll_id: id, pending, state: pending ? 'processing' : 'final', total_votes: pending ? 777 : 2,
    options: [{ id: 1, label: 'Очно', votes: pending ? 777 : 2 }, { id: 2, label: 'Онлайн', votes: 0 }], calculated_at: new Date().toISOString(),
  } }));
  await page.goto('/admin');
  await page.locator('.poll-item').click();
  await expect(page.locator('#results-note')).toContainText('Результаты будут доступны после обработки');
  await expect(page.locator('#total-votes')).toHaveText('—');
  await expect(page.locator('.result-label span:last-child')).toHaveText(['—', '—']);
  await expect(page.locator('#results-updated')).toBeEmpty();
  expect(await page.locator('.result-progress:visible').count()).toBe(0);
  pending = false;
  await page.locator('#refresh-results').click();
  await expect(page.locator('#total-votes')).toHaveText('2');
  await expect(page.locator('.result-label span:last-child')).toHaveText(['2 · 100%', '0 · 0%']);
  await expect(page.locator('#results-note')).toHaveText('Итоги готовы.');
});

test('busy is a definite refusal with a delayed manual retry', async ({ page }) => {
  await assets(page, 'poll');
  await page.route(`**/api/polls/${id}`, route => route.fulfill({ json: poll() }));
  const bodies = [];
  await page.route(`**/api/polls/${id}/votes`, route => {
    bodies.push(route.request().postDataJSON());
    return bodies.length === 1
      ? route.fulfill({ status: 503, headers: { 'Retry-After': '1' }, json: { outcome: 'not_admitted' } })
      : route.fulfill({ status: 202, json: { status: 'recorded' } });
  });
  await page.goto(`/p/${id}`);
  await page.getByLabel('Очно', { exact: true }).check();
  await page.locator('#vote-button').click();
  await expect(page.locator('#vote-message')).toContainText('Эта попытка не принята');
  await expect(page.locator('#vote-button')).toBeDisabled();
  await expect(page.locator('#vote-button')).toBeEnabled({ timeout: 10000 });
  expect(bodies.length).toBe(1);
  await page.locator('#vote-button').click();
  await expect(page.locator('#receipt-panel')).toBeVisible();
  expect(bodies.length).toBe(2);
  expect(bodies[0].token === bodies[1].token).toBe(true);
  expect(JSON.stringify(bodies[0].choices) === JSON.stringify(bodies[1].choices)).toBe(true);
});
