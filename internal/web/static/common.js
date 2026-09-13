'use strict';

window.Gigaquizz = (() => {
  const numberFormat = new Intl.NumberFormat('ru-RU');
  const dateFormat = new Intl.DateTimeFormat('ru-RU', { day: 'numeric', month: 'long', hour: '2-digit', minute: '2-digit' });
  const timeFormat = new Intl.DateTimeFormat('ru-RU', { hour: '2-digit', minute: '2-digit', second: '2-digit' });
  async function request(path, options = {}) {
    const controller = new AbortController();
    const timeout = window.setTimeout(() => controller.abort(), 20000);
    const sentAt = Date.now();
    const sentMono = performance.now();
    try {
      const response = await fetch(path, { ...options, credentials: 'same-origin', signal: controller.signal, headers: { Accept: 'application/json', ...(options.body ? { 'Content-Type': 'application/json' } : {}), ...options.headers } });
      const headersMono = performance.now();
      const dateHeader = response.headers.get('Date');
      const ageHeader = response.headers.get('Age');
      const retryHeader = response.headers.get('Retry-After')?.trim();
      const retrySeconds = retryHeader && /^\d+$/.test(retryHeader) ? Number(retryHeader) : NaN;
      const retryAfter = Number.isSafeInteger(retrySeconds) ? retrySeconds : null;
      let body;
      try { body = await response.text(); } catch (error) {
        error.retryAfter = retryAfter;
        throw error;
      }
      const bodyMono = performance.now();
      let data = {};
      try { if (body) data = JSON.parse(body); } catch {
        const error = new Error('Ответ сервера не удалось прочитать.');
        error.retryAfter = retryAfter;
        throw error;
      }
      return { ok: response.ok, status: response.status, data, retryAfter, midpoint: (sentAt + Date.now()) / 2, timing: { sentMono, headersMono, bodyMono, dateHeader, ageHeader } };
    } finally {
      window.clearTimeout(timeout);
    }
  }
  function notice(node, message, kind = '') {
    node.textContent = message;
    node.className = 'notice' + (kind ? ' ' + kind : '');
    node.hidden = !message;
  }
  function stateBadge(node, state) {
    const labels = { scheduled: 'Скоро начнётся', open: 'Идёт голосование', processing: 'Подводим итоги', final: 'Опрос завершён', unknown: 'Проверяем время' };
    node.textContent = labels[state] || 'Опрос';
    node.className = 'badge badge-' + (Object.hasOwn(labels, state) ? state : 'scheduled');
  }
  function clockFromResponse(response, freshTime = false) {
    const t = response.timing;
    if (!t || ![t.sentMono, t.headersMono, t.bodyMono].every(Number.isFinite) || t.headersMono < t.sentMono || t.bodyMono < t.headersMono || t.bodyMono - t.sentMono > 30000) return null;
    let point;
    let padding;
    if (freshTime) {
      point = typeof response.data?.server_time === 'string' ? Date.parse(response.data.server_time) : NaN;
      padding = 2;
    } else {
      if (typeof t.dateHeader !== 'string' || !/^[A-Z][a-z]{2}, \d{2} [A-Z][a-z]{2} \d{4} \d{2}:\d{2}:\d{2} GMT$/.test(t.dateHeader)) return null;
      const age = t.ageHeader === null ? 0 : /^\d{1,10}$/.test(t.ageHeader) ? Number(t.ageHeader) : NaN;
      if (!Number.isSafeInteger(age) || age < 0 || age > 86400) return null;
      point = Date.parse(t.dateHeader) + age * 1000;
      padding = 2000; // Date/Age rounding, plus RTT below. Edge skew is checked before closure.
    }
    if (!Number.isFinite(point) || point < Date.UTC(2020, 0, 1) || point > Date.UTC(2200, 0, 1)) return null;
    const lower = point - padding;
    const upper = point + padding + t.bodyMono - t.sentMono;
    return Object.freeze({ source: freshTime ? 'time' : 'headers', bounds() {
      const elapsed = Math.max(0, performance.now() - t.bodyMono);
      // 100ppm drift allowance for pages opened well before the poll.
      const drift = elapsed / 10000;
      return { lower: lower + elapsed - drift, upper: upper + elapsed + drift };
    } });
  }
  function receiptTimeNanoseconds(value) {
    // Go's RFC3339Nano JSON has explicit zones and up to nine fractional
    // digits. Parse calendar fields strictly: Date.parse alone both truncates
    // submilliseconds and normalizes some invalid dates (for example Sep 31).
    if (typeof value !== 'string' || value.length > 35) return null;
    const parts = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?(Z|([+-])(\d{2}):(\d{2}))$/.exec(value);
    if (!parts) return null;
    const fields = parts.slice(1, 7).map(Number);
    const [year, month, day, hour, minute, second] = fields;
    if (month < 1 || month > 12 || day < 1 || day > 31 || hour > 23 || minute > 59 || second > 59) return null;
    const milliseconds = Date.parse(`${parts[1]}-${parts[2]}-${parts[3]}T${parts[4]}:${parts[5]}:${parts[6]}Z`);
    if (!Number.isSafeInteger(milliseconds)) return null;
    const calendar = new Date(milliseconds);
    if (calendar.getUTCFullYear() !== year || calendar.getUTCMonth() + 1 !== month || calendar.getUTCDate() !== day) return null;
    const offsetHour = parts[10] === undefined ? 0 : Number(parts[10]);
    const offsetMinute = parts[11] === undefined ? 0 : Number(parts[11]);
    if (offsetHour > 23 || offsetMinute > 59) return null;
    const offsetSeconds = (offsetHour * 60 + offsetMinute) * 60 * (parts[9] === '-' ? -1 : 1);
    return BigInt(milliseconds) * 1000000n + BigInt((parts[7] || '').padEnd(9, '0')) - BigInt(offsetSeconds) * 1000000000n;
  }
  function validReceipt(data, status, choices, definition) {
    if (!data || !Array.isArray(data.choices) || data.choices.length === 0 || data.choices.length > definition.options.length) return false;
    const allowed = new Set(definition.options.map(option => option.id));
    if (!data.choices.every((choice, index) => Number.isSafeInteger(choice) && allowed.has(choice) && (index === 0 || data.choices[index - 1] < choice))) return false;
    if (definition.type !== 'multiple' && data.choices.length !== 1) return false;
    const admitted = receiptTimeNanoseconds(data.accepted_at);
    const starts = receiptTimeNanoseconds(definition.starts_at);
    const ends = receiptTimeNanoseconds(definition.ends_at);
    if (admitted === null || starts === null || ends === null || ends - starts !== 60000000000n || admitted < starts || admitted >= ends) return false;
    const exact = choices.length === data.choices.length && choices.every((choice, index) => choice === data.choices[index]);
    if (status === 202) return data.status === 'recorded' && exact;
    const expected = ({ 200: 'duplicate', 201: 'accepted', 409: 'conflict' })[status];
    return typeof expected === 'string' && expected === data.status && (status !== 201 || exact);
  }
  return { request, notice, stateBadge, clockFromResponse, validReceipt, number: (value) => numberFormat.format(value), date: (value) => dateFormat.format(new Date(value)), time: (value) => timeFormat.format(new Date(value)) };
})();
