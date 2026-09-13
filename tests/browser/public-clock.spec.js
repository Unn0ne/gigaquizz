const { test, expect } = require('@playwright/test');
const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');

// A real loopback server preserves Chromium's cache/revalidation behavior.
// Routing mocks disable that cache and cannot establish the 304 contract.
// All identifiers, votes and clocks here are synthetic; no app or credentials.
test.use({ screenshot: 'off', trace: 'off', video: 'off' });
test.setTimeout(30000);
const id = '33333333-3333-4333-8333-333333333333';
const assetRoot = path.resolve(__dirname, '../../internal/web/static');

async function fixture(options = {}) {
  const now = Date.now();
  const definition = {
    id, question: 'Синтетический вопрос', type: 'single',
    options: [{ id: 1, label: 'Первый' }, { id: 2, label: 'Второй' }],
    starts_at: new Date(now - 10000).toISOString(), ends_at: new Date(now + 50000).toISOString(),
  };
  const counters = { full: 0, conditional: 0, time: 0, votes: [] };
  const server = http.createServer(async (req, res) => {
    const url = new URL(req.url, 'http://127.0.0.1');
    res.setHeader('Cache-Control', 'no-store');
    if (url.pathname === `/api/polls/${id}/definition`) {
      res.setHeader('Cache-Control', 'public, max-age=0, s-maxage=86400, must-revalidate');
      res.setHeader('ETag', '"synthetic-immutable-definition"');
      res.setHeader('Date', new Date(Date.now() + (options.dateOffset ?? -120000)).toUTCString());
      res.setHeader('Age', options.age ?? '120');
      res.setHeader('Content-Type', 'application/json');
      if (req.headers['if-none-match'] === '"synthetic-immutable-definition"') {
        counters.conditional++;
        res.writeHead(304); res.end(); return;
      }
      counters.full++;
      const body = JSON.stringify(definition);
      if (options.slowBody) {
        res.write(body.slice(0, 1));
        setTimeout(() => res.end(body.slice(1)), 80);
      } else res.end(body);
      return;
    }
    if (url.pathname === '/api/time') {
      counters.time++;
      res.setHeader('Content-Type', 'application/json');
      if (options.timeFailure) { res.writeHead(503); res.end('{}'); }
      else res.end(JSON.stringify({ server_time: new Date().toISOString() }));
      return;
    }
    if (url.pathname === `/api/polls/${id}/votes` && req.method === 'POST') {
      const chunks = [];
      for await (const chunk of req) chunks.push(chunk);
      const body = JSON.parse(Buffer.concat(chunks));
      counters.votes.push(body);
      const reply = options.voteReply || { status: 'recorded', choices: body.choices, accepted_at: new Date().toISOString() };
      res.setHeader('Content-Type', 'application/json');
      res.writeHead(202); res.end(JSON.stringify(reply)); return;
    }
    const filename = url.pathname === `/p/${id}` ? 'poll.html' : url.pathname.startsWith('/static/') ? url.pathname.slice(8) : '';
    // Explicit basename validation keeps this fixture from serving other files.
    if (filename && path.basename(filename) === filename && fs.existsSync(path.join(assetRoot, filename))) {
      res.setHeader('Content-Type', filename.endsWith('.js') ? 'text/javascript' : filename.endsWith('.css') ? 'text/css' : 'text/html');
      res.end(fs.readFileSync(path.join(assetRoot, filename))); return;
    }
    res.writeHead(404); res.end();
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  return {
    origin: `http://127.0.0.1:${server.address().port}`, definition, counters,
    async close() { server.closeAllConnections(); await new Promise(resolve => server.close(resolve)); },
  };
}

async function open(page, f) {
  await page.goto(`${f.origin}/p/${id}`);
  await expect(page.locator('#poll-panel')).toBeVisible();
}

async function localRecord(page) {
  return page.evaluate(id => new Promise((resolve, reject) => {
    const opening = indexedDB.open('gigaquizz-votes', 1);
    opening.onerror = () => reject(opening.error);
    opening.onsuccess = () => {
      const db = opening.result;
      const reading = db.transaction('votes').objectStore('votes').get(id);
      reading.onsuccess = () => { resolve(reading.result); db.close(); };
      reading.onerror = () => { reject(reading.error); db.close(); };
    };
  }), id);
}

test('aged Date/Age survives real cache revalidation and Date.now jumps do not close voting', async ({ page }) => {
  const f = await fixture();
  try {
    await open(page, f);
    await expect(page.locator('#poll-state')).toHaveText('Идёт голосование');
    const cached = await page.evaluate(async id => {
      const response = await window.Gigaquizz.request(`/api/polls/${id}/definition`);
      const clock = window.Gigaquizz.clockFromResponse(response);
      return { status: response.status, question: response.data.question, clock: Boolean(clock), age: response.timing.ageHeader };
    }, id);
    expect(f.counters.conditional).toBeGreaterThan(0);
    expect(cached).toEqual({ status: 200, question: f.definition.question, clock: true, age: '120' });
    await page.reload();
    await expect(page.locator('#poll-state')).toHaveText('Идёт голосование');
    expect(f.counters.full).toBe(1);
    expect(f.counters.time).toBe(0);
    await page.evaluate(() => { const original = Date.now; Date.now = () => original() + 3600000; });
    await page.getByLabel('Первый', { exact: true }).check();
    await expect(page.locator('#vote-button')).toBeEnabled();
    await page.waitForTimeout(350); // One real renderClock tick after the wall-clock jump.
    await expect(page.locator('#poll-state')).toHaveText('Идёт голосование');
    await page.locator('#vote-button').click();
    await expect(page.locator('#receipt-panel')).toBeVisible();
    expect(f.counters.votes).toHaveLength(1);
  } finally { await f.close(); }
});

test('clock samples include body-read time and reject malformed or unbounded Age', async ({ page }) => {
  const f = await fixture({ slowBody: true });
  try {
    await open(page, f);
    const checks = await page.evaluate(async id => {
      const response = await window.Gigaquizz.request(`/api/polls/${id}/definition`, { cache: 'no-store' });
      const rejected = ['NaN', '-1', 'Infinity', '86401', '9999999999999999999'].map(age => {
        const invalid = { ...response, timing: { ...response.timing, ageHeader: age } };
        return window.Gigaquizz.clockFromResponse(invalid) === null;
      });
      const t = response.timing;
      return { headersAfterSent: t.headersMono >= t.sentMono, bodyDelay: t.bodyMono - t.headersMono, rejected };
    }, id);
    expect(checks.headersAfterSent).toBe(true);
    expect(checks.bodyDelay).toBeGreaterThan(30);
    expect(checks.rejected).toEqual([true, true, true, true, true]);
  } finally { await f.close(); }
});

test('invalid cached clock with failed fallback leaves server admission available', async ({ page }) => {
  const f = await fixture({ age: 'NaN', timeFailure: true });
  try {
    await open(page, f);
    await expect(page.locator('#clock-warning')).toContainText('сервер проверит, открыт ли опрос');
    await expect(page.locator('#poll-state')).toHaveText('Проверяем время');
    await page.getByLabel('Первый', { exact: true }).check();
    await expect(page.locator('#vote-button')).toBeEnabled();
    await page.locator('#vote-button').click();
    await expect(page.locator('#receipt-panel')).toBeVisible();
    expect(f.counters.time).toBe(1);
  } finally { await f.close(); }
});

test('an apparent close from cache clock skew is verified before disabling voting', async ({ page }) => {
  const f = await fixture({ dateOffset: 120000, age: '0' });
  try {
    await open(page, f);
    await expect(page.locator('#poll-state')).toHaveText('Идёт голосование');
    expect(f.counters.time).toBe(1);
    await page.getByLabel('Первый', { exact: true }).check();
    await expect(page.locator('#vote-button')).toBeEnabled();
  } finally { await f.close(); }
});

test('malformed durable replies never become local ACKs and retain the exact retry identity', async ({ page }) => {
  const f = await fixture({ voteReply: { status: 'recorded', choices: [1] } });
  try {
    await open(page, f);
    await page.getByLabel('Первый', { exact: true }).check();
    await page.locator('#vote-button').click();
    await expect(page.locator('#vote-message')).toContainText('Ответ мог сохраниться');
    await expect(page.locator('#receipt-panel')).toBeHidden();
    const before = await localRecord(page);
    expect(before.receipt).toBeUndefined();
    expect(before.pending.choices).toEqual([1]);
    await page.reload();
    await expect(page.locator('#vote-button')).toBeEnabled();
    await page.locator('#vote-button').click();
    await expect(page.locator('#vote-message')).toContainText('Ответ мог сохраниться');
    expect(f.counters.votes).toHaveLength(2);
    expect(f.counters.votes[0]).toEqual(f.counters.votes[1]);
    const checks = await page.evaluate(definition => {
      const accepted_at = new Date(Date.parse(definition.starts_at) + 1000).toISOString();
      const valid = { status: 'recorded', choices: [1], accepted_at };
      const candidates = [
        [202, { ...valid, choices: [2] }],
        [202, { ...valid, choices: [4294967297] }],
        [202, { ...valid, choices: [-4294967295] }],
        [202, { ...valid, choices: [1, 1] }],
        [202, { ...valid, accepted_at: definition.ends_at }],
        [202, { ...valid, accepted_at: 'not-a-time' }],
        [200, valid],
        [503, { choices: [1], accepted_at }],
        [201, { ...valid, status: 'accepted', choices: [2] }],
      ];
      return { valid: window.Gigaquizz.validReceipt(valid, 202, [1], definition), rejected: candidates.map(([status, body]) => !window.Gigaquizz.validReceipt(body, status, [1], definition)) };
    }, f.definition);
    expect(checks.valid).toBe(true);
    expect(checks.rejected.every(Boolean)).toBe(true);
  } finally { await f.close(); }
});


test('a valid but lagging cached clock is verified before scheduled-state blocking', async ({ page }) => {
  const f = await fixture({ dateOffset: -120000, age: '0' });
  try {
    await open(page, f);
    await expect(page.locator('#poll-state')).toHaveText('Идёт голосование');
    expect(f.counters.time).toBe(1);
    await page.getByLabel('Первый', { exact: true }).check();
    await expect(page.locator('#vote-button')).toBeEnabled();
    await page.locator('#vote-button').click();
    await expect(page.locator('#receipt-panel')).toBeVisible();
    expect(f.counters.votes).toHaveLength(1);
    expect(f.counters.time).toBe(1);
  } finally { await f.close(); }
});


test('a durable HTTP ACK one nanosecond before closing is retained without rounding', async ({ page }) => {
  const reply = { status: 'recorded', choices: [1] };
  const f = await fixture({ voteReply: reply });
  try {
    f.definition.starts_at = f.definition.starts_at.split('.')[0] + '.123456789Z';
    f.definition.ends_at = f.definition.ends_at.split('.')[0] + '.123456789Z';
    reply.accepted_at = f.definition.ends_at.replace('.123456789Z', '.123456788Z');
    await open(page, f);
    await page.getByLabel('Первый', { exact: true }).check();
    await page.locator('#vote-button').click();
    await expect(page.locator('#receipt-panel')).toBeVisible();
    const stored = await localRecord(page);
    expect(stored.pending).toBeUndefined();
    expect(stored.receipt.accepted_at).toBe(reply.accepted_at);
    expect(f.counters.votes).toHaveLength(1);
  } finally { await f.close(); }
});

test('receipt timestamps preserve nanosecond boundaries and validate calendar fields and RFC3339 offsets', async ({ page }) => {
  const f = await fixture();
  try {
    await open(page, f);
    const results = await page.evaluate(() => {
      const definition = {
        type: 'single', options: [{ id: 1 }, { id: 2 }],
        starts_at: '2026-09-13T12:00:00.123456789Z', ends_at: '2026-09-13T12:01:00.123456789Z',
      };
      const valid = (accepted_at, d = definition) => window.Gigaquizz.validReceipt({ status: 'recorded', choices: [1], accepted_at }, 202, [1], d);
      const accepted = [
        definition.starts_at,
        '2026-09-13T12:00:00.123456790Z',
        '2026-09-13T12:00:00.12345679Z',
        '2026-09-13T12:00:01Z',
        '2026-09-13T12:01:00.123456788Z',
        '2026-09-13T15:01:00.123456788+03:00',
        '2026-09-13T07:31:00.123456788-04:30',
        '2026-09-14T02:01:00.123456788+14:00',
        '2026-09-13T12:01:00.123456788-00:00',
      ].map(value => valid(value));
      const rejected = [
        '2026-09-13T12:00:00.123456788Z', // starts_at - 1ns, same Date.parse millisecond.
        definition.ends_at,
        '2026-09-13T15:01:00.123456789+03:00',
        '2026-09-13T12:00:30.1234567890Z',
        '2026-09-13T12:00:30.Z',
        '2026-09-13T12:00:30',
        '2026-09-13 12:00:30Z',
        '2026-09-13T12:00:30+0300',
        '2026-09-13T12:00:30+24:00',
        '2026-09-13T12:00:30+00:60',
        '2026-09-13T12:00:30+0a:00',
        '2026-09-13T12:00:30Z trailing',
      ].map(value => !valid(value));
      const october = { ...definition, starts_at: '2026-10-01T12:00:00Z', ends_at: '2026-10-01T12:01:00Z' };
      const march = { ...definition, starts_at: '2025-03-01T12:00:00Z', ends_at: '2025-03-01T12:01:00Z' };
      const midnight = { ...definition, starts_at: '2026-09-14T00:00:00Z', ends_at: '2026-09-14T00:01:00Z' };
      const leap = { ...definition, starts_at: '2024-02-29T12:00:00Z', ends_at: '2024-02-29T12:01:00Z' };
      const offsetDefinition = { ...definition, starts_at: '2026-09-13T15:00:00.123456789+03:00', ends_at: '2026-09-13T07:31:00.123456789-04:30' };
      return { accepted, rejected,
        invalidCalendarRejected: !valid('2026-09-31T12:00:30Z', october) && !valid('2025-02-29T12:00:30Z', march) && !valid('2026-09-13T24:00:00Z', midnight),
        leapDayAccepted: valid('2024-02-29T12:00:30.000000001Z', leap),
        offsetDefinitionAccepted: valid('2026-09-13T12:01:00.123456788Z', offsetDefinition),
        invalidDefinitionRejected: !valid(definition.starts_at, { ...definition, starts_at: '2026-09-13T12:00:00.123456789+24:00' }),
      };
    });
    expect(results.accepted.every(Boolean)).toBe(true);
    expect(results.rejected.every(Boolean)).toBe(true);
    expect(results.invalidCalendarRejected).toBe(true);
    expect(results.leapDayAccepted).toBe(true);
    expect(results.offsetDefinitionAccepted).toBe(true);
    expect(results.invalidDefinitionRejected).toBe(true);
  } finally { await f.close(); }
});
