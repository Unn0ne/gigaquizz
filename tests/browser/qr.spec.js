const { test, expect } = require('@playwright/test');
const fs = require('node:fs');
const path = require('node:path');
const jsQR = require('jsqr');

// Independent decoder: https://github.com/cozmo/jsQR, pinned as a dev-only
// dependency. All API responses and poll identifiers in this file are synthetic.
// Every request is intercepted; these tests need no running app or credentials.
test.use({ screenshot: 'off', trace: 'off', video: 'off', acceptDownloads: true });
test.setTimeout(30000);

const origin = 'http://127.0.0.1:18273';
const assetRoot = path.resolve(__dirname, '../../internal/web/static');
const fixtureIDs = [
  '11111111-1111-4111-8111-111111111111',
  '22222222-2222-4222-8222-222222222222',
];

async function fixture(page) {
  const now = Date.now();
  const polls = fixtureIDs.map((id, index) => ({
    id, question: `Синтетический опрос ${index + 1}`, type: 'single',
    options: [{ id: 1, label: 'Да' }, { id: 2, label: 'Нет' }],
    starts_at: new Date(now + (index + 1) * 120000).toISOString(),
    ends_at: new Date(now + (index + 1) * 120000 + 60000).toISOString(),
    state: 'scheduled',
  }));
  const unexpected = [];
  await page.route('**/*', async route => {
    const url = new URL(route.request().url());
    if (url.origin !== origin) {
      unexpected.push(url.origin);
      return route.abort();
    }
    if (url.pathname === '/admin') {
      return route.fulfill({ body: fs.readFileSync(path.join(assetRoot, 'admin.html')), contentType: 'text/html' });
    }
    if (url.pathname.startsWith('/static/')) {
      const relative = decodeURIComponent(url.pathname.slice('/static/'.length));
      const filename = path.resolve(assetRoot, relative);
      if (!filename.startsWith(assetRoot + path.sep) || !fs.existsSync(filename) || !fs.statSync(filename).isFile()) {
        unexpected.push('missing local asset');
        return route.abort();
      }
      const type = filename.endsWith('.js') ? 'text/javascript' : filename.endsWith('.css') ? 'text/css' : 'application/octet-stream';
      return route.fulfill({ body: fs.readFileSync(filename), contentType: type });
    }
    if (url.pathname === '/api/admin/session') return route.fulfill({ json: { authenticated: true } });
    if (url.pathname === '/api/admin/logout' && route.request().method() === 'POST') return route.fulfill({ status: 204 });
    if (url.pathname === '/api/admin/polls' && route.request().method() === 'GET') return route.fulfill({ json: { polls } });
    const selected = polls.find(poll => url.pathname === `/api/admin/polls/${poll.id}/results`);
    if (selected) {
      return route.fulfill({ json: {
        poll_id: selected.id, pending: true, state: 'scheduled', total_votes: 0,
        options: selected.options.map(option => ({ ...option, votes: 0 })), calculated_at: new Date(now).toISOString(),
      } });
    }
    if (url.pathname === '/favicon.ico') return route.fulfill({ status: 204 });
    unexpected.push(url.pathname);
    return route.abort();
  });
  await page.goto(origin + '/admin');
  await expect(page.locator('.poll-item')).toHaveCount(2);
  return { unexpected };
}

// Raster pixels are extracted by Chromium. Neither the QR encoder's module
// matrix nor its claimed metadata participates in decoding or quiet-zone tests.
async function canvasPixels(page, selector = '#poll-qr') {
  return page.locator(selector).evaluate(canvas => {
    if (!canvas.width || !canvas.height) return null;
    const image = canvas.getContext('2d').getImageData(0, 0, canvas.width, canvas.height);
    let binary = '';
    for (let start = 0; start < image.data.length; start += 8192) {
      binary += String.fromCharCode(...image.data.subarray(start, start + 8192));
    }
    return { width: image.width, height: image.height, rgba: btoa(binary) };
  });
}

async function downloadedPixels(page, bytes) {
  return page.evaluate(async base64 => {
    const raw = Uint8Array.from(atob(base64), character => character.charCodeAt(0));
    const bitmap = await createImageBitmap(new Blob([raw], { type: 'image/png' }));
    const canvas = document.createElement('canvas');
    canvas.width = bitmap.width;
    canvas.height = bitmap.height;
    canvas.getContext('2d').drawImage(bitmap, 0, 0);
    bitmap.close();
    const image = canvas.getContext('2d').getImageData(0, 0, canvas.width, canvas.height);
    let binary = '';
    for (let start = 0; start < image.data.length; start += 8192) {
      binary += String.fromCharCode(...image.data.subarray(start, start + 8192));
    }
    return { width: image.width, height: image.height, rgba: btoa(binary) };
  }, bytes.toString('base64'));
}

function decodedImage(image) {
  expect(image, 'a nonempty QR raster exists').not.toBeNull();
  const pixels = new Uint8ClampedArray(Buffer.from(image.rgba, 'base64'));
  expect(pixels.length).toBe(image.width * image.height * 4);
  const decoded = jsQR(pixels, image.width, image.height, { inversionAttempts: 'dontInvert' });
  expect(decoded, 'independent jsQR must decode the actual raster').not.toBeNull();
  return { pixels, decoded };
}

function assertExactQR(image, expectedURL) {
  const { pixels, decoded } = decodedImage(image);
  expect(decoded.data).toBe(expectedURL);
  expect(Buffer.from(decoded.binaryData).equals(Buffer.from(expectedURL, 'utf8'))).toBe(true);
  expect(image.width).toBe(image.height);
  let minX = image.width, minY = image.height, maxX = -1, maxY = -1, invalid = 0;
  for (let y = 0; y < image.height; y++) {
    for (let x = 0; x < image.width; x++) {
      const offset = (y * image.width + x) * 4;
      const red = pixels[offset];
      if ((red !== 0 && red !== 255) || pixels[offset + 1] !== red || pixels[offset + 2] !== red || pixels[offset + 3] !== 255) invalid++;
      if (red === 0) {
        minX = Math.min(minX, x); minY = Math.min(minY, y);
        maxX = Math.max(maxX, x); maxY = Math.max(maxY, y);
      }
    }
  }
  expect(invalid, 'QR pixels are opaque black or white, without antialiasing').toBe(0);
  const modules = 17 + 4 * decoded.version;
  const scale = (maxX - minX + 1) / modules;
  expect(Number.isInteger(scale), 'whole pixels per module').toBe(true);
  expect(scale).toBeGreaterThanOrEqual(8);
  expect(maxY - minY + 1).toBe(modules * scale);
  for (const margin of [minX, minY, image.width - maxX - 1, image.height - maxY - 1]) {
    expect(margin, 'at least four complete white modules on every edge').toBeGreaterThanOrEqual(4 * scale);
  }
}

async function pngDownload(page) {
  const event = page.waitForEvent('download');
  await page.locator('#download-qr').click();
  const download = await event;
  try {
    expect(download.suggestedFilename()).toBe('gigaquizz-qr.png');
    expect(await download.failure()).toBeNull();
    const stream = await download.createReadStream();
    const chunks = [];
    for await (const chunk of stream) chunks.push(chunk);
    const bytes = Buffer.concat(chunks);
    expect(bytes.subarray(0, 8).equals(Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]))).toBe(true);
    return downloadedPixels(page, bytes);
  } finally {
    await download.delete();
  }
}

async function assertNoRetainedQR(page) {
  const image = await canvasPixels(page);
  if (image) {
    const pixels = new Uint8ClampedArray(Buffer.from(image.rgba, 'base64'));
    expect(jsQR(pixels, image.width, image.height), 'old QR raster is cleared').toBeNull();
  }
}

test('QR canvas and downloaded PNG independently decode to the exact share link with a quiet zone', async ({ page }) => {
  const { unexpected } = await fixture(page);
  await page.locator('.poll-item').nth(0).click();
  const expectedURL = origin + '/p/' + fixtureIDs[0];
  await expect(page.locator('#share-link')).toHaveValue(expectedURL);
  await expect(page.locator('#qr-panel')).toBeHidden();
  await expect(page.locator('#toggle-qr')).toHaveAttribute('aria-expanded', 'false');
  await page.locator('#toggle-qr').click();
  await expect(page.locator('#qr-panel')).toBeVisible();
  await expect(page.locator('#toggle-qr')).toHaveAttribute('aria-expanded', 'true');
  assertExactQR(await canvasPixels(page), expectedURL);
  assertExactQR(await pngDownload(page), expectedURL);
  expect(unexpected).toEqual([]);
});

test('switching poll cards clears the old QR and the next canvas and PNG contain the new URL', async ({ page }) => {
  const { unexpected } = await fixture(page);
  await page.locator('.poll-item').nth(0).click();
  await page.locator('#toggle-qr').click();
  assertExactQR(await canvasPixels(page), origin + '/p/' + fixtureIDs[0]);
  await page.locator('.poll-item').nth(1).click();
  await expect(page.locator('#share-link')).toHaveValue(origin + '/p/' + fixtureIDs[1]);
  await expect(page.locator('#qr-panel')).toBeHidden();
  await expect(page.locator('#toggle-qr')).toHaveAttribute('aria-expanded', 'false');
  await assertNoRetainedQR(page);
  await page.locator('#toggle-qr').click();
  assertExactQR(await canvasPixels(page), origin + '/p/' + fixtureIDs[1]);
  assertExactQR(await pngDownload(page), origin + '/p/' + fixtureIDs[1]);
  await page.locator('#logout').click();
  await expect(page.locator('#login-panel')).toBeVisible();
  await expect(page.locator('#qr-panel')).toBeHidden();
  await assertNoRetainedQR(page);
  expect(unexpected).toEqual([]);
});

test('logout discards a PNG callback that finishes after the QR selection was cleared', async ({ page }) => {
  const { unexpected } = await fixture(page);
  await page.locator('.poll-item').nth(0).click();
  await page.locator('#toggle-qr').click();
  await page.evaluate(() => {
    const originalToBlob = HTMLCanvasElement.prototype.toBlob;
    window.__qrDownloadClicks = 0;
    const originalClick = HTMLAnchorElement.prototype.click;
    HTMLAnchorElement.prototype.click = function () {
      if (this.download) window.__qrDownloadClicks++;
      return originalClick.call(this);
    };
    HTMLCanvasElement.prototype.toBlob = function (callback, ...args) {
      return originalToBlob.call(this, blob => {
        window.__qrReleaseBlob = () => callback(blob);
      }, ...args);
    };
  });
  await page.locator('#download-qr').click();
  await page.waitForFunction(() => typeof window.__qrReleaseBlob === 'function');
  await page.locator('#logout').click();
  await expect(page.locator('#login-panel')).toBeVisible();
  await page.evaluate(async () => {
    window.__qrReleaseBlob();
    await new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve)));
  });
  expect(await page.evaluate(() => window.__qrDownloadClicks)).toBe(0);
  await assertNoRetainedQR(page);
  expect(unexpected).toEqual([]);
});

test('Unicode and IDN URL bytes survive independent raster decoding without normalization', async ({ page }) => {
  const { unexpected } = await fixture(page);
  const unicode = 'https://пример.рф/опрос/сентябрь?тема=Кофе%20и%20чай&значок=☕';
  for (const expected of [unicode, new URL(unicode).href]) {
    await page.evaluate(text => {
      const canvas = document.createElement('canvas');
      canvas.id = 'unicode-qr';
      document.body.append(canvas);
      window.GigaquizzQR.render(canvas, text);
    }, expected);
    assertExactQR(await canvasPixels(page, '#unicode-qr'), expected);
    await page.locator('#unicode-qr').evaluate(canvas => canvas.remove());
  }
  expect(unexpected).toEqual([]);
});
