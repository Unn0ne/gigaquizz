'use strict';

(() => {
  const ui = window.Gigaquizz;
  const byId = (id) => document.getElementById(id);
  const pollID = location.pathname.split('/').filter(Boolean)[1];
  const storageWarning = byId('storage-warning');
  const message = byId('vote-message');
  const button = byId('vote-button');
  let poll;
  let clockOffset = 0;
  let record;
  let busy = false;
  let store;
  let channel;
  let retryFailures = 0;
  let retryAvailableAt = 0;

  // This only delays the next manual click. No timer sends a vote.
  function delayManualRetry(retryAfter = null) {
    retryFailures = Math.min(retryFailures + 1, 3);
    const ceiling = 1000 * 2 ** retryFailures;
    const serverDelay = Number.isFinite(retryAfter) ? Math.max(0, retryAfter) * 1000 : 0;
    const minimum = Math.max(ceiling / 2, serverDelay);
    const maximum = Math.max(ceiling, minimum + 1000);
    retryAvailableAt = performance.now() + minimum + Math.random() * (maximum - minimum);
  }

  function randomToken() {
    const bytes = new Uint8Array(16);
    crypto.getRandomValues(bytes);
    return Array.from(bytes, (byte) => byte.toString(16).padStart(2, '0')).join('');
  }

  // The read and first creation share one transaction across all tabs.
  // Never perform a network request inside an IndexedDB transaction.
  async function createStore() {
    let database = null;
    let memoryRecord = null;
    const fallback = () => { storageWarning.hidden = false; };
    try {
      database = await new Promise((resolve, reject) => {
        let settled = false;
        const timeout = setTimeout(() => { settled = true; reject(new Error('Storage timeout')); }, 3500);
        const opening = indexedDB.open('gigaquizz-votes', 1);
        opening.onupgradeneeded = () => {
          if (!opening.result.objectStoreNames.contains('votes')) opening.result.createObjectStore('votes', { keyPath: 'poll_id' });
        };
        opening.onsuccess = () => {
          clearTimeout(timeout);
          if (settled) { opening.result.close(); return; }
          settled = true;
          resolve(opening.result);
        };
        opening.onerror = () => { clearTimeout(timeout); settled = true; reject(opening.error); };
      });
      database.onversionchange = () => { database.close(); database = null; fallback(); };
    } catch {
      fallback();
    }

    async function update(mutate = (value) => value) {
      if (!database) {
        memoryRecord = mutate(memoryRecord || { poll_id: pollID, token: randomToken() });
        return structuredClone(memoryRecord);
      }
      try {
        const next = await new Promise((resolve, reject) => {
          const transaction = database.transaction('votes', 'readwrite');
          const objects = transaction.objectStore('votes');
          const reading = objects.get(pollID);
          let value;
          transaction.oncomplete = () => resolve(value);
          transaction.onabort = () => reject(transaction.error || new Error('Storage transaction aborted'));
          transaction.onerror = () => { /* onabort handles the final outcome */ };
          reading.onsuccess = () => {
            try {
              const current = reading.result || memoryRecord || { poll_id: pollID, token: randomToken() };
              // Retain an already read token/receipt even when the write aborts.
              memoryRecord = structuredClone(current);
              value = mutate(current);
              objects.put(value);
            } catch {
              transaction.abort();
            }
          };
        });
        memoryRecord = structuredClone(next);
        return next;
      } catch {
        database.close();
        database = null;
        fallback();
        // Keep the existing token and pending choice when storage fails later.
        memoryRecord = mutate(memoryRecord || { poll_id: pollID, token: randomToken() });
        return structuredClone(memoryRecord);
      }
    }
    return { update };
  }

  function announceChange() {
    if (channel) channel.postMessage(pollID);
  }

  function liveState() {
    if (poll.state === 'final') return 'final';
    const now = Date.now() + clockOffset;
    if (now < Date.parse(poll.starts_at)) return 'scheduled';
    if (now < Date.parse(poll.ends_at)) return 'open';
    return 'processing';
  }

  function selectedChoices() {
    return Array.from(document.querySelectorAll('#choices input:checked'), (input) => Number(input.value)).sort((a, b) => a - b);
  }

  function renderRecord() {
    if (!record) return;
    const remembered = record.receipt ? record.receipt.choices : record.pending?.choices;
    for (const input of document.querySelectorAll('#choices input')) {
      if (remembered) input.checked = remembered.includes(Number(input.value));
    }
    if (record.receipt) {
      retryFailures = 0;
      retryAvailableAt = 0;
      byId('receipt-panel').hidden = false;
      byId('receipt-title').textContent = record.receipt.status === 'conflict' ? 'Сохранён ваш предыдущий ответ' : 'Ваш голос принят';
      const labels = (record.receipt.choices || []).map((id) => poll.options.find((option) => option.id === id)?.label).filter(Boolean);
      byId('receipt-description').textContent = labels.join(' · ');
      byId('poll-footnote').textContent = 'Спасибо за участие. Повторно отправлять ответ не нужно.';
      ui.notice(message, '');
    }
    renderClock();
  }

  function renderClock() {
    if (!poll || !record) return;
    const state = liveState();
    ui.stateBadge(byId('poll-state'), state);
    const until = state === 'scheduled' ? poll.starts_at : poll.ends_at;
    const seconds = Math.max(0, Math.ceil((Date.parse(until) - Date.now() - clockOffset) / 1000));
    const timer = byId('countdown');
    if (state === 'open' || state === 'scheduled') {
      const minutes = Math.floor(seconds / 60);
      timer.textContent = (state === 'scheduled' ? 'До начала ' : 'Осталось ') + minutes + ':' + String(seconds % 60).padStart(2, '0');
    } else timer.textContent = 'Приём ответов закрыт';
    byId('scheduled-note').hidden = state !== 'scheduled';
    if (state === 'scheduled') byId('scheduled-note').textContent = 'Начало ' + ui.date(poll.starts_at) + '. Страницу можно оставить открытой.';
    byId('choices-fieldset').disabled = busy || Boolean(record.pending) || Boolean(record.receipt) || state !== 'open';
    const retrySeconds = Math.max(0, Math.ceil((retryAvailableAt - performance.now()) / 1000));
    button.hidden = Boolean(record.receipt);
    button.disabled = busy || retrySeconds > 0 || (!record.pending && state !== 'open');
    if (busy) button.textContent = 'Проверяем ответ…';
    else if (retrySeconds > 0) button.textContent = 'Повторить через ' + retrySeconds + ' с';
    else if (record.pending) button.textContent = 'Проверить и повторить отправку';
    else if (state === 'scheduled') button.textContent = 'Голосование скоро начнётся';
    else if (state !== 'open') button.textContent = 'Голосование завершено';
    else button.textContent = 'Отправить ответ →';
  }

  async function sendVote(event) {
    event.preventDefault();
    if (busy || record.receipt || performance.now() < retryAvailableAt) return;
    if (!record.pending && liveState() !== 'open') {
      renderClock();
      return;
    }
    const chosen = selectedChoices();
    if (!record.pending && chosen.length === 0) {
      ui.notice(message, 'Выберите хотя бы один вариант ответа.', 'warning');
      return;
    }
    busy = true;
    renderClock();
    try {
      // The first pending choice wins locally, even if two tabs submit together.
      record = await store.update((current) => {
        if (!current.receipt && !current.pending) current.pending = { choices: chosen };
        return current;
      });
      announceChange();
      renderRecord();
      if (record.receipt) return;
      const response = await ui.request('/api/polls/' + encodeURIComponent(pollID) + '/votes', { method: 'POST', body: JSON.stringify({ token: record.token, choices: record.pending.choices }) });
      const data = response.data;
      if ([200, 201, 409].includes(response.status) && Array.isArray(data.choices) && data.choices.length > 0) {
        record = await store.update((current) => {
          current.receipt = { status: response.status === 409 ? 'conflict' : data.status, choices: data.choices, accepted_at: data.accepted_at || null };
          delete current.pending;
          return current;
        });
        announceChange();
        renderRecord();
      } else if (response.status === 410) {
        ui.notice(message, 'Приём ответов закрыт. Сервер не подтвердил ваш ответ. Если вы отправляли его до закрытия, можно проверить ещё раз.', 'warning');
      } else if (response.status === 425) {
        ui.notice(message, 'Опрос ещё не начался. Ваш выбор сохранён на этой странице; повторите отправку после начала.', 'warning');
      } else if (response.status === 422) {
        ui.notice(message, 'Сервер не принял выбранные варианты. Обратитесь к организатору опроса.', 'error');
      } else if (response.status === 429) {
        delayManualRetry(response.retryAfter);
        ui.notice(message, 'Слишком много попыток. Подождите немного и проверьте ответ ещё раз.', 'warning');
      } else {
        delayManualRetry(response.retryAfter);
        ui.notice(message, 'Подтверждение пока не получено. Голос мог быть принят. Проверьте ещё раз: мы отправим тот же выбор без повторного учёта.', 'warning');
      }
    } catch (error) {
      delayManualRetry(error.retryAfter);
      ui.notice(message, 'Связь прервалась, и исход пока неизвестен. Голос мог быть принят. Проверьте ещё раз с тем же ответом.', 'warning');
    } finally {
      busy = false;
      renderClock();
    }
  }

  async function refreshLocalRecord() {
    if (!store || busy) return;
    record = await store.update();
    renderRecord();
    if (record.pending && !record.receipt && message.hidden) ui.notice(message, 'Этот ответ уже отправляли, но подтверждение ещё не получено. Проверьте его — выбранные варианты останутся прежними.', 'warning');
  }

  async function init() {
    try {
      const response = await ui.request('/api/polls/' + encodeURIComponent(pollID || ''));
      if (!response.ok) throw new Error(response.status === 404 ? 'Такого опроса нет. Проверьте ссылку у организатора.' : 'Сервис временно недоступен. Попробуйте ещё раз.');
      poll = response.data;
      clockOffset = Date.parse(poll.server_time) - response.midpoint;
      if (!Number.isFinite(clockOffset)) throw new Error('Не удалось определить время начала опроса.');
      document.title = poll.question + ' — Gigaquizz';
      byId('question').textContent = poll.question;
      byId('selection-hint').textContent = poll.type === 'multiple' ? 'Можно выбрать несколько вариантов.' : 'Выберите один вариант.';
      const fragment = document.createDocumentFragment();
      for (const option of poll.options) {
        const label = document.createElement('label');
        label.className = 'choice';
        const input = document.createElement('input');
        input.type = poll.type === 'multiple' ? 'checkbox' : 'radio';
        input.name = 'choice';
        input.value = option.id;
        const check = document.createElement('span');
        check.className = 'choice-control';
        check.setAttribute('aria-hidden', 'true');
        const text = document.createElement('span');
        text.className = 'choice-text';
        text.textContent = option.label;
        label.append(input, check, text);
        fragment.append(label);
      }
      byId('choices').replaceChildren(fragment);
      store = await createStore();
      record = await store.update();
      byId('loading').hidden = true;
      byId('poll-panel').hidden = false;
      renderRecord();
      if (record.pending && !record.receipt) ui.notice(message, 'Вы уже отправляли этот ответ. Подтверждение ещё не сохранено — проверьте его, чтобы узнать исход.', 'warning');
      byId('vote-form').addEventListener('submit', sendVote);
      if ('BroadcastChannel' in window) {
        channel = new BroadcastChannel('gigaquizz-votes');
        channel.onmessage = (event) => { if (event.data === pollID) refreshLocalRecord(); };
      }
      window.addEventListener('focus', refreshLocalRecord);
      document.addEventListener('visibilitychange', () => { if (!document.hidden) refreshLocalRecord(); });
      setInterval(renderClock, 250);
    } catch (error) {
      byId('loading').hidden = true;
      byId('page-error').hidden = false;
      byId('page-error-message').textContent = error.message || 'Проверьте соединение и попробуйте ещё раз.';
    }
  }
  byId('reload-page').addEventListener('click', () => location.reload());
  init();
})();
