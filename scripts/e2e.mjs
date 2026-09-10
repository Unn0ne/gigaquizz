// Real-browser, real-storage end-to-end check for either application branch.
// Run: BASE_URL=http://127.0.0.1:8091 ADMIN_PASSWORD=... node scripts/e2e.mjs
// Creates one future poll and observes its complete 60-second window. Does not
// launch services, alter clocks, delete data, or collect traces/screenshots.
import { chromium, expect } from '@playwright/test';
import { existsSync } from 'node:fs';
import { pathToFileURL } from 'node:url';
import { randomBytes } from 'node:crypto';

class E2EInvariantError extends Error {}
function check(condition, message) { if (!condition) throw new E2EInvariantError(message); }
function validateTarget(baseURL, password) {
  const target = new URL(baseURL);
  check(['http:', 'https:'].includes(target.protocol) && ['127.0.0.1', '[::1]', 'localhost'].includes(target.hostname), 'Only a local application target is supported');
  check(!target.username && !target.password && target.pathname === '/' && !target.search && !target.hash, 'BASE_URL must be a local origin');
  check(typeof password === 'string' && password.length >= 16, 'ADMIN_PASSWORD must be configured');
}

async function localVote(page, pollID) {
  return page.evaluate(id => new Promise((resolve, reject) => {
    const opening = indexedDB.open('gigaquizz-votes', 1);
    opening.onerror = () => reject(new Error('Cannot read browser identity'));
    opening.onsuccess = () => {
      const db = opening.result;
      const tx = db.transaction('votes', 'readonly');
      const read = tx.objectStore('votes').get(id);
      let value;
      read.onsuccess = () => { value = read.result; };
      tx.oncomplete = () => { db.close(); resolve(value); };
      tx.onabort = () => { db.close(); reject(new Error('Cannot read browser identity')); };
    };
  }), pollID);
}

export async function runE2E({ browser, baseURL, password, progress = () => {} }) {
  validateTarget(baseURL, password);
  const contexts = [];
  let stage = 'initialization';
  let pageErrors = 0;
  const mark = value => { stage = value; progress(value); };
  const newContext = async (options = {}) => {
    const context = await browser.newContext({ baseURL, ...options });
    context.on('page', page => page.on('pageerror', () => { pageErrors++; }));
    contexts.push(context);
    return context;
  };
  try {
    mark('administrator_login');
    const adminContext = await newContext();
    const admin = await adminContext.newPage();
    check((await admin.request.get('/api/admin/polls')).status() === 401, 'Unauthenticated admin endpoint exposed');
    await admin.goto('/admin');
    await admin.locator('#password').fill(password);
    await admin.locator('#login-button').click();
    await expect(admin.locator('#dashboard')).toBeVisible();

    mark('create_future_poll');
    const question = 'Проверка сохранения ответов ' + Date.now();
    const creation = await admin.request.post('/api/admin/polls', { data: {
      question, type: 'multiple', options: ['Очно', 'Онлайн', 'Гибридно'],
      starts_at: new Date(Date.now() + 30000).toISOString(),
    } });
    check(creation.status() === 201, 'Poll creation failed');
    const poll = await creation.json();
    check(Date.parse(poll.ends_at) - Date.parse(poll.starts_at) === 60000, 'Poll duration changed');
    const votePath = `/api/polls/${poll.id}/votes`;
    const pagePath = `/p/${poll.id}`;
    const resultsPath = `/api/admin/polls/${poll.id}/results`;
    await admin.locator('#refresh-polls').click();
    await admin.locator('.poll-item').filter({ hasText: question }).click();
    await expect(admin.locator('#total-votes')).toHaveText('—');
    await expect(admin.locator('#results-note')).toContainText('Результаты будут доступны после обработки');
    const pendingLabels = await admin.locator('.result-label span:last-child').allTextContents();
    check(pendingLabels.length === 3 && pendingLabels.every(value => value === '—'), 'Pending result rows were missing or showed numeric counts');
    const pending = await (await admin.request.get(resultsPath)).json();
    check(pending.pending === true, 'Pending result marker absent');

    mark('two_tabs_same_identity');
    const tabs = await newContext();
    const a = await tabs.newPage();
    const b = await tabs.newPage();
    const submissions = [];
    const statuses = [];
    let release;
    const barrier = new Promise(resolve => { release = resolve; });
    let routeError = false;
    await tabs.route(`**${votePath}`, async route => {
      try {
        submissions.push(route.request().postDataJSON());
        if (submissions.length >= 2) release();
        await Promise.race([barrier, new Promise((_, reject) => setTimeout(() => reject(new Error('Second tab did not submit')), 5000))]);
        const upstream = await route.fetch();
        statuses.push(upstream.status());
        await route.fulfill({ response: upstream });
      } catch { routeError = true; await route.abort().catch(() => {}); }
    });
    mark('two_tabs_navigation');
    await Promise.all([a.goto(pagePath), b.goto(pagePath)]);
    // Enabled does not imply visible. During initialization the inputs exist
    // inside a hidden panel before IndexedDB initialization applies scheduling.
    // Wait for initialization first, then allow the full future-start interval
    // for both tabs; a default 10-second assertion is insufficient here.
    mark('two_tabs_initialized');
    await Promise.all([a, b].map(page => expect(page.locator('#poll-panel')).toBeVisible()));
    mark('two_tabs_wait_open');
    await Promise.all([a, b].map(page => expect(page.locator('#choices input').first()).toBeEnabled({ timeout: 45000 })));
    mark('two_tabs_select_choices');
    await a.getByLabel('Очно', { exact: true }).check();
    await a.getByLabel('Онлайн', { exact: true }).check();
    await b.getByLabel('Гибридно', { exact: true }).check();
    mark('two_tabs_submit');
    await Promise.all([a, b].map(page => page.evaluate(() => document.querySelector('#vote-form').requestSubmit())));
    mark('two_tabs_receipts');
    await expect(a.locator('#receipt-panel')).toBeVisible();
    await expect(b.locator('#receipt-panel')).toBeVisible();
    await expect(a.locator('#receipt-title')).toHaveText('Ответ сохранён');
    await expect(b.locator('#receipt-title')).toHaveText('Ответ сохранён');
    check(!routeError && submissions.length === 2 && statuses.every(status => status === 202), 'Concurrent attempt ACK contract failed');
    check(submissions[0].token === submissions[1].token, 'Tabs generated different identities');
    check(JSON.stringify(submissions[0].choices) === JSON.stringify(submissions[1].choices), 'Tabs changed the first pending choice');
    const remembered = await localVote(a, poll.id);
    check(remembered.receipt?.status === 'recorded' && !remembered.pending, 'Recorded receipt not persisted');
    check(JSON.stringify(remembered.receipt.attempt_choices) === JSON.stringify(submissions[0].choices), 'Sent choices not retained');
    await a.reload();
    await expect(a.locator('#receipt-panel')).toBeVisible();
    check((await localVote(a, poll.id)).token === remembered.token && submissions.length === 2, 'Reload changed identity or resent a confirmed attempt');

    mark('lost_response_reload_same_attempt');
    const uncertain = await newContext({ viewport: { width: 390, height: 844 } });
    const mobile = await uncertain.newPage();
    const retries = [];
    const retryStatuses = [];
    await uncertain.route(`**${votePath}`, async route => {
      try {
        retries.push(route.request().postDataJSON());
        const upstream = await route.fetch(); // Real storage must ACK first.
        retryStatuses.push(upstream.status());
        if (retries.length === 1) await route.abort('failed');
        else await route.fulfill({ response: upstream });
      } catch { routeError = true; await route.abort().catch(() => {}); }
    });
    await mobile.goto(pagePath);
    await mobile.getByLabel('Онлайн', { exact: true }).check();
    await mobile.locator('#vote-button').click();
    await expect(mobile.locator('#vote-message')).toContainText('исход пока неизвестен');
    const beforeReload = await localVote(mobile, poll.id);
    check(!beforeReload.receipt && beforeReload.pending && retryStatuses[0] === 202, 'Lost response was not after real durable ACK');
    await mobile.reload();
    await expect(mobile.locator('#vote-button')).toHaveText('Повторить отправку');
    const afterReload = await localVote(mobile, poll.id);
    check(beforeReload.token === afterReload.token && JSON.stringify(beforeReload.pending.choices) === JSON.stringify(afterReload.pending.choices), 'Pending retry lost identity or choices');
    check(retries.length === 1, 'Reload automatically submitted a vote');
    await mobile.locator('#vote-button').click();
    await expect(mobile.locator('#receipt-panel')).toBeVisible();
    await expect(mobile.locator('#receipt-title')).toHaveText('Ответ сохранён');
    check(retries.length === 2 && retryStatuses.every(status => status === 202), 'Manual retry did not get a recorded attempt ACK');
    check(retries[0].token === retries[1].token && JSON.stringify(retries[0].choices) === JSON.stringify(retries[1].choices), 'Retry changed identity or choices');
    check(retries[0].token !== submissions[0].token, 'Independent browser context reused identity');

    mark('unknown_attempt_at_closure');
    const lateContext = await newContext();
    const late = await lateContext.newPage();
    const lateBodies = [];
    const lateStatuses = [];
    await lateContext.route(`**${votePath}`, async route => {
      try {
        lateBodies.push(route.request().postDataJSON());
        const upstream = await route.fetch();
        lateStatuses.push(upstream.status());
        if (lateBodies.length === 1) await route.abort('failed');
        else await route.fulfill({ response: upstream });
      } catch { routeError = true; await route.abort().catch(() => {}); }
    });
    await late.goto(pagePath);
    await late.getByLabel('Гибридно', { exact: true }).check();
    await late.locator('#vote-button').click();
    await expect(late.locator('#vote-message')).toContainText('исход пока неизвестен');
    check(lateStatuses[0] === 202, 'Second lost response was not after real durable ACK');

    mark('wait_real_deadline_and_final_results');
    let final;
    const deadline = Date.now() + 90000;
    while (Date.now() < deadline) {
      const response = await admin.request.get(resultsPath);
      if (response.ok()) {
        const value = await response.json();
        if (value.state === 'final' && value.pending !== true) { final = value; break; }
      }
      await new Promise(resolve => setTimeout(resolve, 1000));
    }
    check(final && final.total_votes === 3, 'Final exact unique count differs');
    const expected = [0, 1, 1];
    submissions[0].choices.forEach(choice => { expected[choice - 1]++; });
    check(JSON.stringify(final.options.map(option => option.votes)) === JSON.stringify(expected), 'Final exact option counts differ');
    await admin.locator('#refresh-results').click();
    await expect(admin.locator('#total-votes')).toHaveText('3');
    await expect(admin.locator('#results-note')).toContainText('Итоги готовы');

    mark('closed_rejects_new_and_pending_attempts');
    const freshContext = await newContext();
    const fresh = await freshContext.newPage();
    await fresh.goto(pagePath);
    await expect(fresh.locator('#poll-panel')).toBeVisible();
    await expect(fresh.locator('#vote-button')).toBeDisabled();
    check((await fresh.request.post(votePath, { data: { token: randomBytes(16).toString('hex'), choices: [1] } })).status() === 410, 'Closed poll admitted a new token');
    check((await fresh.request.post(votePath, { data: submissions[0] })).status() === 410, 'Closed poll admitted a known token');
    await late.reload();
    await expect(late.locator('#vote-button')).toHaveText('Повторить отправку');
    await late.locator('#vote-button').click();
    await expect(late.locator('#vote-button')).toBeDisabled();
    await expect(late.locator('#vote-message')).toContainText('её исход здесь неизвестен');
    check(lateStatuses.length === 2 && lateStatuses[1] === 410 && lateBodies[0].token === lateBodies[1].token, 'Closed pending attempt handled incorrectly');
    const closedRecord = await localVote(late, poll.id);
    check(closedRecord.pending?.closed === true && !closedRecord.receipt, 'Closed refusal fabricated a receipt');
    await late.reload();
    await expect(late.locator('#vote-message')).toContainText('её исход здесь неизвестен');
    await expect(late.locator('#vote-button')).toBeDisabled();
    check(lateBodies.length === 2 && !routeError, 'Unexpected automatic or failed routed request');

    mark('logout_and_public_privacy');
    check((await fresh.request.get(resultsPath)).status() === 401, 'Public context accessed private results');
    const publicPoll = await (await fresh.request.get(`/api/polls/${poll.id}`)).json();
    check(!Object.hasOwn(publicPoll, 'total_votes') && !Object.hasOwn(publicPoll, 'pending'), 'Public metadata leaked private aggregates');
    await admin.locator('#logout').click();
    await expect(admin.locator('#login-panel')).toBeVisible();
    check((await admin.request.get('/api/admin/polls')).status() === 401, 'Logout did not revoke authorization');
    check(pageErrors === 0, 'Browser reported a script error');
    mark('complete');
    return { passed: true, real_poll_window_seconds: 60, recorded_attempt_responses: 5, exact_unique_votes: 3, lost_responses_after_real_ack: 2, closed_request_responses: 3, same_browser_identity_preserved: true, pending_choice_and_receipt_persisted: true, private_results_protected: true, page_errors: pageErrors };
  } catch (error) {
    // Avoid Playwright assertion/request dumps containing tokens, poll IDs or
    // authentication material. The last aggregate stage identifies the failure.
    // Invariant messages are authored here and contain no private values.
    // For library failures retain only the matcher name, never its argument,
    // request URL, response body, DOM dump or authentication context.
    const matcher = /expect\([^\n]*\)\.(to[A-Za-z]+)/.exec(error.message || '')?.[1];
    const reason = error instanceof E2EInvariantError ? error.message
      : matcher ? `assertion ${matcher} failed`
      : error.name === 'TimeoutError' ? 'browser action timed out'
      : 'browser or request action failed';
    throw new Error(`Browser E2E failed at stage: ${stage}; ${reason}`);
  } finally {
    await Promise.allSettled(contexts.map(context => context.close()));
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  let browser;
  try {
    const baseURL = process.env.BASE_URL || process.env.E2E_BASE_URL;
    const password = process.env.ADMIN_PASSWORD;
    validateTarget(baseURL, password);
    const chrome = '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome';
    browser = await chromium.launch({ headless: true, ...(existsSync(chrome) ? { executablePath: chrome } : {}) });
    const result = await runE2E({ browser, baseURL, password, progress: stage => process.stderr.write(`e2e: ${stage}\n`) });
    process.stdout.write(JSON.stringify(result, null, 2) + '\n');
  } catch (error) {
    process.stderr.write(error.message.startsWith('Browser E2E failed at stage:') ? error.message + '\n' : 'Browser E2E could not initialize; check local BASE_URL, ADMIN_PASSWORD and browser installation.\n');
    process.exitCode = 1;
  } finally { if (browser) await browser.close(); }
}
