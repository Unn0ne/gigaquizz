'use strict';

(() => {
  const ui = window.Gigaquizz;
  const byId = (id) => document.getElementById(id);
  let polls = [];
  let selectedID = null;
  let listSequence = 0;
  let resultsSequence = 0;
  let authenticated = false;

  function showLogin(message = '') {
    authenticated = false;
    listSequence++;
    resultsSequence++;
    polls = [];
    selectedID = null;
    byId('session-loading').hidden = true;
    byId('dashboard').hidden = true;
    byId('logout').hidden = true;
    byId('login-panel').hidden = false;
    byId('poll-list').replaceChildren();
    byId('results-panel').hidden = true;
    byId('results-placeholder').hidden = false;
    ui.notice(byId('login-message'), message, 'error');
  }

  async function protectedRequest(path, options) {
    const response = await ui.request(path, options);
    if (response.status === 401) {
      showLogin('Сессия закончилась. Войдите ещё раз.');
      throw new Error('session_expired');
    }
    return response;
  }

  async function showDashboard() {
    authenticated = true;
    byId('session-loading').hidden = true;
    byId('login-panel').hidden = true;
    byId('dashboard').hidden = false;
    byId('logout').hidden = false;
    await loadPolls();
  }

  function openCreate() {
    byId('create-panel').hidden = false;
    byId('new-question').focus();
    byId('create-panel').scrollIntoView({ block: 'start', behavior: 'smooth' });
  }

  function resetSchedule() {
    const date = new Date(Date.now() + 5 * 60 * 1000);
    date.setSeconds(0, 0);
    const localDate = new Date(date.getTime() - date.getTimezoneOffset() * 60000);
    byId('new-start').value = localDate.toISOString().slice(0, 16);
    changeSchedule();
  }

  function changeSchedule() {
    const scheduled = byId('start-mode').value === 'scheduled';
    byId('schedule-fields').hidden = !scheduled;
    byId('new-start').required = scheduled;
  }

  function renderPollList() {
    const fragment = document.createDocumentFragment();
    for (const poll of polls) {
      const card = document.createElement('button');
      card.type = 'button';
      card.className = 'poll-item' + (poll.id === selectedID ? ' active' : '');
      card.setAttribute('aria-pressed', String(poll.id === selectedID));
      const top = document.createElement('div');
      top.className = 'poll-item-top';
      const badge = document.createElement('span');
      ui.stateBadge(badge, poll.state);
      const arrow = document.createElement('span');
      arrow.className = 'poll-item-arrow';
      arrow.textContent = '↗';
      arrow.setAttribute('aria-hidden', 'true');
      top.append(badge, arrow);
      const title = document.createElement('h3');
      title.textContent = poll.question;
      const meta = document.createElement('p');
      const typeLabel = { ab: 'A/B', single: 'Один ответ', multiple: 'Несколько ответов' };
      meta.textContent = ui.date(poll.starts_at) + ' · ' + (typeLabel[poll.type] || 'Опрос');
      card.append(top, title, meta);
      card.addEventListener('click', () => selectPoll(poll.id));
      fragment.append(card);
    }
    byId('poll-list').replaceChildren(fragment);
    byId('poll-count').textContent = ui.number(polls.length);
    byId('polls-empty').hidden = polls.length > 0;
  }

  async function loadPolls(selectID = null) {
    const sequence = ++listSequence;
    byId('refresh-polls').disabled = true;
    try {
      const response = await protectedRequest('/api/admin/polls');
      if (sequence !== listSequence || !authenticated) return;
      if (!response.ok || !Array.isArray(response.data.polls)) throw new Error('load_failed');
      polls = response.data.polls;
      renderPollList();
      if (selectID) await selectPoll(selectID);
      else if (selectedID && polls.some((poll) => poll.id === selectedID)) {
        const poll = polls.find((item) => item.id === selectedID);
        ui.stateBadge(byId('results-state'), poll.state);
      }
    } catch (error) {
      if (error.message !== 'session_expired') ui.notice(byId('admin-message'), 'Не удалось обновить список опросов. Проверьте соединение и попробуйте ещё раз.', 'error');
    } finally {
      if (sequence === listSequence) byId('refresh-polls').disabled = false;
    }
  }

  async function selectPoll(id) {
    const poll = polls.find((item) => item.id === id);
    if (!poll) return;
    selectedID = id;
    renderPollList();
    byId('results-placeholder').hidden = true;
    byId('results-panel').hidden = false;
    byId('results-question').textContent = poll.question;
    byId('results-schedule').textContent = ui.date(poll.starts_at) + ' · 60 секунд на ответы';
    ui.stateBadge(byId('results-state'), poll.state);
    const url = location.origin + '/p/' + encodeURIComponent(id);
    byId('share-link').value = url;
    byId('open-poll').href = url;
    byId('total-votes').textContent = '—';
    byId('results-options').replaceChildren();
    byId('results-updated').textContent = '';
    byId('results-note').textContent = 'Получаем результаты…';
    ui.notice(byId('results-message'), '');
    await loadResults();
  }

  async function loadResults() {
    const id = selectedID;
    if (!id) return;
    const sequence = ++resultsSequence;
    byId('refresh-results').disabled = true;
    try {
      const response = await protectedRequest('/api/admin/polls/' + encodeURIComponent(id) + '/results');
      if (sequence !== resultsSequence || id !== selectedID || !authenticated) return;
      if (!response.ok || !Array.isArray(response.data.options)) throw new Error('load_failed');
      const results = response.data;
      const poll = polls.find((item) => item.id === id);
      if (poll) poll.state = results.state;
      ui.stateBadge(byId('results-state'), results.state);
      byId('total-votes').textContent = ui.number(results.total_votes);
      const fragment = document.createDocumentFragment();
      for (const option of results.options) {
        const row = document.createElement('div');
        const label = document.createElement('div');
        label.className = 'result-label';
        const text = document.createElement('span');
        text.textContent = option.label;
        const count = document.createElement('span');
        const percent = results.total_votes > 0 ? Math.round(option.votes / results.total_votes * 100) : 0;
        count.textContent = ui.number(option.votes) + ' · ' + percent + '%';
        label.append(text, count);
        const bar = document.createElement('progress');
        bar.className = 'result-progress';
        bar.max = Math.max(1, results.total_votes);
        bar.value = option.votes;
        bar.setAttribute('aria-label', option.label + ': ' + ui.number(option.votes));
        row.append(label, bar);
        fragment.append(row);
      }
      byId('results-options').replaceChildren(fragment);
      const finalText = results.state === 'final' ? 'Итоги готовы.' : results.state === 'scheduled' ? 'Опрос ещё не начался.' : 'Промежуточные результаты. Для новых данных нажмите «Обновить».';
      byId('results-note').textContent = finalText + (poll?.type === 'multiple' ? ' Можно выбрать несколько вариантов, поэтому сумма процентов может превышать 100%.' : '');
      byId('results-updated').textContent = 'Рассчитано ' + ui.time(results.calculated_at);
      ui.notice(byId('results-message'), '');
      renderPollList();
    } catch (error) {
      if (error.message !== 'session_expired' && id === selectedID) {
        byId('results-note').textContent = 'Новые результаты пока недоступны.';
        ui.notice(byId('results-message'), 'Не удалось получить результаты. Попробуйте обновить их ещё раз.', 'error');
      }
    } finally {
      if (sequence === resultsSequence) byId('refresh-results').disabled = false;
    }
  }

  async function createPoll(event) {
    event.preventDefault();
    const question = byId('new-question').value.trim();
    const type = byId('new-type').value;
    const options = byId('new-options').value.split(/\r?\n/).map((option) => option.trim()).filter(Boolean);
    let validation = '';
    if (!question || [...question].length > 300) validation = 'Вопрос должен содержать от 1 до 300 символов.';
    else if (options.length < 2 || options.length > 20) validation = 'Добавьте от 2 до 20 вариантов, каждый с новой строки.';
    else if (type === 'ab' && options.length !== 2) validation = 'Для A/B-опроса нужны ровно два варианта.';
    else if (options.some((option) => [...option].length > 100)) validation = 'Сократите каждый вариант до 100 символов.';
    else if (new Set(options.map((option) => option.toLowerCase())).size !== options.length) validation = 'Варианты ответа должны отличаться друг от друга.';
    let startsAt = null;
    if (!validation && byId('start-mode').value === 'scheduled') {
      const start = new Date(byId('new-start').value);
      if (!Number.isFinite(start.getTime()) || start.getTime() <= Date.now()) validation = 'Выберите время начала в будущем.';
      else startsAt = start.toISOString();
    }
    if (validation) { ui.notice(byId('create-message'), validation, 'error'); return; }
    byId('create-button').disabled = true;
    byId('create-button').textContent = 'Создаём опрос…';
    ui.notice(byId('create-message'), '');
    try {
      const response = await protectedRequest('/api/admin/polls', { method: 'POST', body: JSON.stringify({ question, type, options, starts_at: startsAt }) });
      if (response.status === 409) {
        ui.notice(byId('create-message'), 'На это время уже назначен другой опрос. Выберите время после его завершения.', 'error');
        return;
      }
      if (response.status === 422 || response.status === 400) {
        ui.notice(byId('create-message'), 'Проверьте вопрос, варианты и время начала. Сервер не смог создать опрос с этими данными.', 'error');
        return;
      }
      if (!response.ok || !response.data.id) throw new Error('creation_unknown');
      byId('create-form').reset();
      resetSchedule();
      updateOptionsHint();
      byId('create-panel').hidden = true;
      ui.notice(byId('admin-message'), 'Опрос создан. Скопируйте ссылку и отправьте участникам.');
      await loadPolls(response.data.id);
      byId('results-panel').scrollIntoView({ block: 'nearest', behavior: 'smooth' });
    } catch (error) {
      if (error.message !== 'session_expired') ui.notice(byId('create-message'), 'Не получили подтверждение создания. Обновите список опросов, прежде чем пробовать ещё раз.', 'warning');
    } finally {
      byId('create-button').disabled = false;
      byId('create-button').textContent = 'Создать опрос →';
    }
  }

  function updateOptionsHint() {
    byId('options-hint').textContent = (byId('new-type').value === 'ab' ? 'Два варианта' : 'От 2 до 20 вариантов') + ', каждый с новой строки. До 100 символов в каждом.';
  }

  byId('login-form').addEventListener('submit', async (event) => {
    event.preventDefault();
    byId('login-button').disabled = true;
    ui.notice(byId('login-message'), '');
    try {
      const response = await ui.request('/api/admin/login', { method: 'POST', body: JSON.stringify({ password: byId('password').value }) });
      if (response.status === 401) ui.notice(byId('login-message'), 'Неверный пароль. Попробуйте ещё раз.', 'error');
      else if (response.status === 429) ui.notice(byId('login-message'), 'Слишком много попыток входа. Подождите немного.', 'warning');
      else if (!response.ok) ui.notice(byId('login-message'), 'Не удалось войти. Попробуйте позже.', 'error');
      else { byId('password').value = ''; await showDashboard(); }
    } catch {
      ui.notice(byId('login-message'), 'Не удалось связаться с сервером. Проверьте соединение.', 'error');
    } finally { byId('login-button').disabled = false; }
  });
  byId('logout').addEventListener('click', async () => {
    byId('logout').disabled = true;
    try {
      const response = await ui.request('/api/admin/logout', { method: 'POST' });
      if (response.ok || response.status === 401) showLogin();
      else ui.notice(byId('admin-message'), 'Не удалось завершить сессию. Попробуйте выйти ещё раз.', 'error');
    } catch { ui.notice(byId('admin-message'), 'Не удалось завершить сессию. Проверьте соединение и попробуйте ещё раз.', 'error'); }
    finally { byId('logout').disabled = false; }
  });
  byId('copy-link').addEventListener('click', async () => {
    try {
      await navigator.clipboard.writeText(byId('share-link').value);
      byId('copy-link').textContent = 'Скопировано';
      setTimeout(() => { byId('copy-link').textContent = 'Копировать'; }, 2200);
    } catch {
      byId('share-link').focus();
      byId('share-link').select();
      ui.notice(byId('results-message'), 'Ссылка выделена. Скопируйте её через меню браузера или сочетанием клавиш.');
    }
  });
  byId('show-create').addEventListener('click', openCreate);
  byId('empty-create').addEventListener('click', openCreate);
  byId('hide-create').addEventListener('click', () => { byId('create-panel').hidden = true; });
  byId('create-form').addEventListener('submit', createPoll);
  byId('start-mode').addEventListener('change', changeSchedule);
  byId('new-type').addEventListener('change', updateOptionsHint);
  byId('refresh-polls').addEventListener('click', async () => { ui.notice(byId('admin-message'), ''); await loadPolls(); });
  byId('refresh-results').addEventListener('click', loadResults);
  resetSchedule();
  (async () => {
    try {
      const response = await ui.request('/api/admin/session');
      if (response.ok) await showDashboard();
      else showLogin(response.status === 401 ? '' : 'Не удалось проверить доступ. Попробуйте войти ещё раз.');
    } catch { showLogin('Не удалось связаться с сервером. Проверьте соединение.'); }
  })();
})();
