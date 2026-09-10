'use strict';

window.Gigaquizz = (() => {
  const numberFormat = new Intl.NumberFormat('ru-RU');
  const dateFormat = new Intl.DateTimeFormat('ru-RU', { day: 'numeric', month: 'long', hour: '2-digit', minute: '2-digit' });
  const timeFormat = new Intl.DateTimeFormat('ru-RU', { hour: '2-digit', minute: '2-digit', second: '2-digit' });
  async function request(path, options = {}) {
    const controller = new AbortController();
    const timeout = window.setTimeout(() => controller.abort(), 20000);
    const sentAt = Date.now();
    try {
      const response = await fetch(path, { ...options, credentials: 'same-origin', signal: controller.signal, headers: { Accept: 'application/json', ...(options.body ? { 'Content-Type': 'application/json' } : {}), ...options.headers } });
      const retryHeader = response.headers.get('Retry-After')?.trim();
      const retrySeconds = retryHeader && /^\d+$/.test(retryHeader) ? Number(retryHeader) : NaN;
      const retryAfter = Number.isSafeInteger(retrySeconds) ? retrySeconds : null;
      let body;
      try { body = await response.text(); } catch (error) {
        error.retryAfter = retryAfter;
        throw error;
      }
      let data = {};
      try { if (body) data = JSON.parse(body); } catch {
        const error = new Error('Ответ сервера не удалось прочитать.');
        error.retryAfter = retryAfter;
        throw error;
      }
      return { ok: response.ok, status: response.status, data, retryAfter, midpoint: (sentAt + Date.now()) / 2 };
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
    const labels = { scheduled: 'Скоро начнётся', open: 'Идёт голосование', processing: 'Подводим итоги', final: 'Опрос завершён' };
    node.textContent = labels[state] || 'Опрос';
    node.className = 'badge badge-' + (Object.hasOwn(labels, state) ? state : 'scheduled');
  }
  return { request, notice, stateBadge, number: (value) => numberFormat.format(value), date: (value) => dateFormat.format(new Date(value)), time: (value) => timeFormat.format(new Date(value)) };
})();
