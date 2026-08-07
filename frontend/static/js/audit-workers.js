/* Экран «Аудит-воркеры» (этап 0).
 *
 * Самодостаточная страница по образцу model-control.js: без бандлера и без
 * правок 19-тысячестрочного app.js — так экран не может сломать основной SPA.
 *
 * Правила отображения, взятые из техпроекта:
 *   * состояние СВЯЗИ и состояние ИСПОЛНЕНИЯ показываются раздельно; молчание
 *     воркера никогда не рисуется как ошибка задания;
 *   * процент прогресса рисуется ТОЛЬКО при percent_reliable, иначе —
 *     неопределённый индикатор, длительность и последний лог;
 *   * при потере связи метрики ресурсов сереют с отметкой времени, но НЕ
 *     обнуляются (обнулить = соврать), а свободные слоты обнуляются, потому
 *     что назначать вслепую нельзя;
 *   * результат без подтверждения приёма помечается retention_unconfirmed.
 */
(() => {
  'use strict';

  const REFRESH_MS = 5000;
  const $ = (id) => document.getElementById(id);

  const state = { enabled: false, timer: null, workers: [], jobs: [], logsJobId: null };

  // ─── Утилиты ───────────────────────────────────────────────────────────────
  const esc = (value) => String(value ?? '').replace(/[&<>"']/g, (c) => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
  }[c]));

  function humanAge(seconds) {
    if (seconds === null || seconds === undefined) return '—';
    const s = Math.max(0, Math.round(seconds));
    if (s < 60) return `${s} с назад`;
    if (s < 3600) return `${Math.round(s / 60)} мин назад`;
    return `${Math.round(s / 3600)} ч назад`;
  }

  function humanDuration(seconds) {
    if (!seconds && seconds !== 0) return '—';
    const s = Math.round(seconds);
    if (s < 60) return `${s} с`;
    if (s < 3600) return `${Math.floor(s / 60)} мин ${s % 60} с`;
    return `${Math.floor(s / 3600)} ч ${Math.floor((s % 3600) / 60)} мин`;
  }

  const CONNECTION_LABEL = {
    online: '● онлайн', stale: '● связь нестабильна',
    offline: '● связь потеряна', reconnecting: '● догоняет события',
  };

  async function api(path, options) {
    const response = await fetch(path, options);
    let body = null;
    try { body = await response.json(); } catch (_) { body = null; }
    if (!response.ok) {
      const detail = body && body.detail ? body.detail : `HTTP ${response.status}`;
      throw new Error(typeof detail === 'string' ? detail : JSON.stringify(detail));
    }
    return body;
  }

  // ─── Карточка VPS ──────────────────────────────────────────────────────────
  function renderWorker(worker) {
    const conn = worker.connection_status;
    const offline = conn === 'offline';
    const snapshot = worker.resource_snapshot || {};
    const ram = snapshot.ram || {};
    const cpu = snapshot.cpu || {};
    const disk = snapshot.disk || {};
    const slots = snapshot.slots || {};
    // Свободные слоты обнуляются при потере связи: назначать вслепую нельзя.
    const freeSlots = offline ? 0 : (worker.calculated_free_slots ?? 0);

    const pending = worker.registration_status === 'pending';
    const warnings = (worker.warnings || []).map(
      (w) => `<li class="warn">⚠ ${esc(w.message || w.code)}</li>`).join('');

    const actions = pending
      ? `<button class="btn btn--primary" data-approve="${esc(worker.worker_id)}">Одобрить</button>`
      : `<button class="btn" data-revoke="${esc(worker.worker_id)}">Отозвать</button>`;

    return `
      <article class="card ${offline ? 'card--offline' : ''}">
        <header class="card-head">
          <div>
            <h3>${esc(worker.display_name)}</h3>
            <p class="mono small">${esc(worker.worker_id)}</p>
          </div>
          <span class="status status--${esc(conn)}">
            ${CONNECTION_LABEL[conn] || esc(conn)}, ${humanAge(worker.seconds_since_seen)}
          </span>
        </header>
        <dl class="kv">
          <div><dt>Регистрация</dt><dd>${esc(worker.registration_status)}</dd></div>
          <div><dt>Состояние</dt><dd>${esc(worker.worker_state)}</dd></div>
          <div><dt>Версия агента</dt><dd>${esc(worker.worker_version || '—')}</dd></div>
          <div><dt>Протокол</dt><dd>v${esc(worker.protocol_version)}</dd></div>
          <div><dt>RAM</dt><dd>${ram.available_gb ?? '—'} / ${ram.total_gb ?? '—'} ГБ${
            ram.swap_used_gb ? ` · своп ${ram.swap_used_gb} ГБ` : ''}</dd></div>
          <div><dt>CPU</dt><dd>${cpu.cores ?? '—'} ядер · LA5 ${cpu.la5 ?? '—'}</dd></div>
          <div><dt>Диск</dt><dd>${disk.free_gb ?? '—'} / ${disk.total_gb ?? '—'} ГБ</dd></div>
          <div><dt>Слоты</dt><dd>
            свободно ${freeSlots} из ${esc(worker.configured_max_slots)}
            ${slots.binding_constraint
              ? `<span class="hint" title="${esc(slots.explanation || '')}">ⓘ ограничивает ${esc(slots.binding_constraint)}</span>`
              : ''}
          </dd></div>
          <div><dt>Активных заданий</dt><dd>${(worker.active_jobs || []).length}</dd></div>
        </dl>
        ${offline ? '<p class="hint">Метрики — последние известные, на момент связи.</p>' : ''}
        ${warnings ? `<ul class="warnings">${warnings}</ul>` : ''}
        <footer class="card-actions">${actions}</footer>
      </article>`;
  }

  // ─── Строка задания ────────────────────────────────────────────────────────
  function renderProgress(progress) {
    if (!progress) return '';
    if (progress.percent_reliable && progress.percent !== null) {
      return `
        <div class="progress">
          <div class="progress-bar"><span style="width:${progress.percent}%"></span></div>
          <span class="mono">${progress.processed} / ${progress.total} ${esc(progress.unit || '')}
            (${progress.percent}%)</span>
        </div>`;
    }
    // Достоверного процента нет — показываем неопределённый индикатор,
    // длительность, последний лог и число завершённых операций.
    return `
      <div class="progress">
        <div class="progress-bar progress-bar--indeterminate"><span></span></div>
        <span class="hint">
          прогресс не оценивается · ${humanDuration(progress.elapsed_sec)} ·
          операций: ${progress.completed_operations ?? 0}
        </span>
      </div>`;
  }

  function renderJob(job) {
    const progress = job.progress || null;
    const eta = progress && progress.eta_sec
      ? ` · осталось ~${humanDuration(progress.eta_sec)}`
      : '';
    const last = progress && progress.last_significant_event
      ? `<p class="hint">последнее: ${esc(progress.last_significant_event)}</p>` : '';
    const unconfirmed = job.retention_unconfirmed
      ? `<p class="warn">⚠ ${esc(job.retention_warning || 'Центр не подтвердил приём')}</p>` : '';
    const canDownload = job.state === 'completed';
    return `
      <article class="job job--${esc(job.state)}">
        <header>
          <div>
            <strong>${esc(job.project_id)}</strong>
            <span class="mono small">${esc(job.job_id.slice(0, 8))} · попытка ${esc(job.attempt_no)}</span>
          </div>
          <span class="badge">${esc(job.display_status)}</span>
        </header>
        ${renderProgress(progress)}
        ${eta ? `<p class="hint">${eta.slice(3)}</p>` : ''}
        ${last}
        ${unconfirmed}
        <footer class="job-actions">
          <button class="btn btn--small" data-logs="${esc(job.job_id)}">Логи</button>
          ${canDownload
            ? `<a class="btn btn--small" href="/api/workers/jobs/${esc(job.job_id)}/result">Скачать результат</a>`
            : ''}
        </footer>
      </article>`;
  }

  // ─── Загрузка данных ───────────────────────────────────────────────────────
  async function refresh() {
    try {
      const status = await api('/api/workers/status');
      state.enabled = !!status.enabled;
      $('disabledBanner').hidden = state.enabled;
      $('content').hidden = !state.enabled;
      if (!state.enabled) {
        $('disabledReason').textContent = status.reason || '';
        return;
      }
      if (status.config_error) {
        $('configError').hidden = false;
        $('configError').textContent = status.config_error;
      } else {
        $('configError').hidden = true;
      }

      const [workersData, jobsData] = await Promise.all([
        api('/api/workers'),
        api('/api/workers/jobs/list?limit=50'),
      ]);
      state.workers = workersData.workers || [];
      state.jobs = jobsData.jobs || [];

      const s = workersData.summary || {};
      $('summary').innerHTML = `
        <span>VPS: <strong>${s.total ?? 0}</strong></span>
        <span>онлайн: <strong>${s.online ?? 0}</strong></span>
        <span>свободных слотов: <strong>${s.free_slots ?? 0}</strong></span>
        <span>активных заданий: <strong>${s.active_jobs ?? 0}</strong></span>`;

      $('workers').innerHTML = state.workers.map(renderWorker).join('');
      $('workersEmpty').hidden = state.workers.length > 0;
      $('jobs').innerHTML = state.jobs.map(renderJob).join('');
      $('jobsEmpty').hidden = state.jobs.length > 0;

      const select = $('jobWorker');
      const previous = select.value;
      select.innerHTML = state.workers
        .filter((w) => w.registration_status === 'approved')
        .map((w) => {
          const ready = w.connection_status === 'online' && w.calculated_free_slots > 0;
          return `<option value="${esc(w.worker_id)}" ${ready ? '' : 'disabled'}>
            ${esc(w.display_name)}${ready ? '' : ' — недоступен'}
          </option>`;
        }).join('');
      if (previous) select.value = previous;

      if (state.logsJobId) await loadLogs(state.logsJobId);
    } catch (error) {
      $('configError').hidden = false;
      $('configError').textContent = `Не удалось обновить: ${error.message}`;
    }
  }

  async function loadLogs(jobId) {
    state.logsJobId = jobId;
    $('logsBlock').hidden = false;
    $('logsJobId').textContent = jobId.slice(0, 8);
    try {
      const data = await api(`/api/workers/jobs/${jobId}/logs?limit=500`);
      const lines = (data.lines || []).map(
        (l) => `[${String(l.seq).padStart(4, '0')}] ${l.level.toUpperCase()} ${l.message}`);
      $('logs').textContent = lines.length ? lines.join('\n') : '(лога пока нет)';
    } catch (error) {
      $('logs').textContent = `Ошибка загрузки логов: ${error.message}`;
    }
  }

  // ─── Действия ──────────────────────────────────────────────────────────────
  document.addEventListener('click', async (event) => {
    const approve = event.target.closest('[data-approve]');
    const revoke = event.target.closest('[data-revoke]');
    const logs = event.target.closest('[data-logs]');
    try {
      if (approve) {
        await api(`/api/workers/${approve.dataset.approve}/approve`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ configured_max_slots: 1 }),
        });
        await refresh();
      } else if (revoke) {
        // Отзыв — опасное действие: подтверждаем явно.
        if (!window.confirm('Отозвать доступ воркера? Токен будет погашен, '
          + 'новые задания выдаваться не будут.')) return;
        await api(`/api/workers/${revoke.dataset.revoke}/revoke`, { method: 'POST' });
        await refresh();
      } else if (logs) {
        await loadLogs(logs.dataset.logs);
      }
    } catch (error) {
      window.alert(`Не удалось выполнить действие: ${error.message}`);
    }
  });

  $('createForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    $('createHint').textContent = 'отправка…';
    try {
      const body = {
        worker_id: $('jobWorker').value,
        project_id: $('jobProject').value.trim(),
        params: {
          label: $('jobLabel').value.trim(),
          steps: Number($('jobSteps').value),
          step_seconds: Number($('jobStepSeconds').value),
          result_bytes: Number($('jobResultBytes').value),
        },
      };
      const created = await api('/api/workers/jobs', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      $('createHint').textContent = `создано: ${created.job.job_id.slice(0, 8)}`;
      await refresh();
    } catch (error) {
      $('createHint').textContent = `ошибка: ${error.message}`;
    }
  });

  $('refreshBtn').addEventListener('click', refresh);
  $('autoRefresh').addEventListener('change', (event) => {
    if (event.target.checked) startTimer(); else stopTimer();
  });

  function startTimer() {
    stopTimer();
    state.timer = window.setInterval(refresh, REFRESH_MS);
  }
  function stopTimer() {
    if (state.timer) window.clearInterval(state.timer);
    state.timer = null;
  }

  refresh();
  startTimer();
})();
