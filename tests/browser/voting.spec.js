const { test, expect } = require('@playwright/test');
const fs = require('node:fs');
const crypto = require('node:crypto');

function administratorPassword() {
  if (process.env.ADMIN_PASSWORD) return process.env.ADMIN_PASSWORD;
  const line = fs.readFileSync('.env', 'utf8').split('\n').find(line => line.startsWith('ADMIN_PASSWORD='));
  if (!line) throw new Error('Configure ADMIN_PASSWORD before running browser tests');
  return line.slice('ADMIN_PASSWORD='.length);
}

test('create, concurrent tabs, lost acknowledgement, close, final results and logout', async ({ page, browser, request, baseURL }) => {
  const pageErrors = [];
  page.on('pageerror', error => pageErrors.push(error.message));
  expect((await request.get('/api/admin/polls')).status()).toBe(401);
  await page.goto('/admin');
  await page.getByLabel('Пароль администратора').fill(administratorPassword());
  await page.getByRole('button', { name: /^Войти/ }).click();
  await expect(page.locator('#dashboard')).toBeVisible();
  await page.locator('#show-create').click();
  await page.getByLabel('Вопрос', { exact: true }).fill('Какие форматы встречи вам подходят?');
  await page.getByLabel('Тип ответа').selectOption('multiple');
  await page.getByLabel('Варианты ответа').fill('Очно\nОнлайн\nГибридно');
  await page.getByLabel('Когда начнём?').selectOption('now');
  const createdResponse = page.waitForResponse(r => r.url().endsWith('/api/admin/polls') && r.request().method() === 'POST');
  await page.locator('#create-button').click();
  const response = await createdResponse;
  expect(response.status()).toBe(201);
  const poll = await response.json();
  expect(Date.parse(poll.ends_at) - Date.parse(poll.starts_at)).toBe(60000);
  await expect(page.locator('#share-link')).toHaveValue(`${baseURL}/p/${poll.id}`);

  const context = await browser.newContext({ baseURL });
  const a = await context.newPage();
  const b = await context.newPage();
  const submissions = [];
  let release;
  const barrier = new Promise(resolve => { release = resolve; });
  await context.route(`**/api/polls/${poll.id}/votes`, async route => {
    submissions.push(route.request().postDataJSON());
    if (submissions.length === 2) release();
    await barrier;
    await route.continue();
  });
  await Promise.all([a.goto(`/p/${poll.id}`), b.goto(`/p/${poll.id}`)]);
  await expect(a.locator('#choices input').first()).toBeEnabled();
  await expect(b.locator('#choices input').first()).toBeEnabled();
  await a.getByLabel('Очно', { exact: true }).check();
  await a.getByLabel('Онлайн', { exact: true }).check();
  await b.getByLabel('Гибридно', { exact: true }).check();
  await Promise.all([a.evaluate(() => document.querySelector('#vote-form').requestSubmit()), b.evaluate(() => document.querySelector('#vote-form').requestSubmit())]);
  await expect(a.locator('#receipt-panel')).toBeVisible();
  await expect(b.locator('#receipt-panel')).toBeVisible();
  expect(submissions.length).toBe(2);
  // Avoid printing identifiers in assertion output.
  expect(submissions[0].token === submissions[1].token).toBe(true);
  expect(JSON.stringify(submissions[0].choices) === JSON.stringify(submissions[1].choices)).toBe(true);
  await a.reload();
  await expect(a.locator('#receipt-panel')).toBeVisible();
  expect(submissions.length).toBe(2);
  await context.close();

  // Simulate a lost HTTP acknowledgement after the real server committed.
  const uncertain = await browser.newContext({ baseURL, viewport: { width: 390, height: 844 } });
  const mobile = await uncertain.newPage();
  let discarded = false;
  await uncertain.route(`**/api/polls/${poll.id}/votes`, async route => {
    if (discarded) { await route.continue(); return; }
    discarded = true;
    const reply = await route.fetch();
    expect(reply.status()).toBe(201);
    await route.abort('failed');
  });
  await mobile.goto(`/p/${poll.id}`);
  await mobile.getByLabel('Онлайн', { exact: true }).check();
  await mobile.locator('#vote-button').click();
  await expect(mobile.locator('#vote-message')).toContainText('исход пока неизвестен');
  await mobile.reload();
  await expect(mobile.locator('#vote-button')).toContainText('Проверить');
  await mobile.locator('#vote-button').click();
  await expect(mobile.locator('#receipt-panel')).toBeVisible();

  fs.mkdirSync('.local/screenshots', { recursive: true });
  await mobile.screenshot({ path: '.local/screenshots/vote-mobile.png', fullPage: true });
  await page.locator('#refresh-results').click();
  await expect(page.locator('#total-votes')).toHaveText('2');
  await page.screenshot({ path: '.local/screenshots/admin.png', fullPage: true });

  // Real 60-second deadline, without changing the server clock or database.
  await expect.poll(async () => {
    const r = await page.request.get(`/api/admin/polls/${poll.id}/results`);
    return (await r.json()).state;
  }, { timeout: 70000, intervals: [1000] }).toBe('final');
  const final = await (await page.request.get(`/api/admin/polls/${poll.id}/results`)).json();
  expect(final.total_votes).toBe(2);
  const expected = [0, 0, 0];
  submissions[0].choices.forEach(id => expected[id - 1]++);
  expected[1]++;
  expect(final.options.map(option => option.votes)).toEqual(expected);
  expect((await request.post(`/api/polls/${poll.id}/votes`, { data: { token: crypto.randomBytes(16).toString('hex'), choices: [1] } })).status()).toBe(410);
  const replay = await request.post(`/api/polls/${poll.id}/votes`, { data: submissions[0] });
  expect(replay.status()).toBe(200);
  await page.locator('#refresh-results').click();
  await expect(page.locator('#total-votes')).toHaveText('2');
  await page.locator('#logout').click();
  await expect(page.locator('#login-panel')).toBeVisible();
  expect((await page.request.get('/api/admin/polls')).status()).toBe(401);
  expect(pageErrors).toEqual([]);
  await uncertain.close();
});
