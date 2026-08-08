/* Экран «Аудит-воркеры» (этапы 0 и 3.5).
 *
 * Самодостаточная страница по образцу model-control.js: без бандлера и без
 * правок 19-тысячестрочного app.js — так экран не может сломать основной SPA.
 *
 * Правила отображения, взятые из техпроекта:
 *   * состояние СВЯЗИ и состояние ИСПОЛНЕНИЯ показываются раздельно; молчание
 *     воркера никогда не рисуется как ошибка задания;
 *   * агент и исполнитель показываются ОТДЕЛЬНО: «агент онлайн» ещё не значит,
 *     что VPS способен работать;
 *   * процент прогресса рисуется ТОЛЬКО при percent_reliable, иначе —
 *     неопределённый индикатор, длительность и последний лог;
 *   * при потере связи метрики ресурсов сереют с отметкой времени, но НЕ
 *     обнуляются (обнулить = соврать), а свободные слоты обнуляются, потому
 *     что назначать вслепую нельзя;
 *   * результат без подтверждения приёма помечается retention_unconfirmed;
 *   * результат устаревшей попытки подписывается явно и никогда не выдаётся
 *     за актуальный.
 *
 * Безопасность разметки. Данные приходят с ПОЛУ-ДОВЕРЕННОГО воркера и от
 * оператора (причины, заметки). Карточки и списки строятся DOM-API
 * (createElement + textContent), а не склейкой строк: один забытый esc() в
 * шаблоне давал исполнение чужого скрипта в аутентифицированной сессии
 * оператора — с доступом к ротации токена.
 */
(() => {
  'use strict';

  const REFRESH_MS = 5000;
  const $ = (id) => document.getElementById(id);

  const state = {
    enabled: false, timer: null, workers: [], jobs: [],
    logsJobId: null, attemptsJobId: null,
    // Права приходят С СЕРВЕРА на каждый цикл обновления и нигде не хранятся
    // между сессиями. localStorage тут нет намеренно: правка локального
    // хранилища не должна давать ни одной кнопки, а тем более права.
    perms: { canView: false, canOperate: false, canAdmin: false,
             subject: null, role: null, authenticated: false, diagnostics: null },
  };

  const PERM = {
    view: 'distributed_workers.view',
    operate: 'distributed_workers.operate',
    admin: 'distributed_workers.admin',
  };

  const DENIED_HINT = 'Недостаточно прав для этого действия';

  // Подтверждающие фразы обязаны совпадать с attempt_service на центре.
  const CONFIRM = {
    cancel: 'ОТМЕНИТЬ',
    markLost: 'ПОПЫТКА ПОТЕРЯНА',
    newAttempt: 'НОВАЯ ПОПЫТКА',
    deleteData: 'УДАЛИТЬ ДАННЫЕ',
  };

  // ─── Безопасное построение DOM ─────────────────────────────────────────────
  function el(tag, options = {}, children = []) {
    const node = document.createElement(tag);
    if (options.className) node.className = options.className;
    if (options.title) node.title = String(options.title);
    // ВСЕГДА textContent: разметку из данных не собираем.
    if (options.text !== undefined && options.text !== null) {
      node.textContent = String(options.text);
    }
    if (options.dataset) {
      Object.entries(options.dataset).forEach(([k, v]) => {
        node.dataset[k] = String(v);
      });
    }
    if (options.attrs) {
      Object.entries(options.attrs).forEach(([k, v]) => node.setAttribute(k, String(v)));
    }
    (Array.isArray(children) ? children : [children])
      .filter(Boolean)
      .forEach((child) => node.appendChild(child));
    return node;
  }

  const text = (value) => document.createTextNode(String(value ?? ''));

  function kv(label, value, extraClass) {
    return el('div', { className: extraClass || '' }, [
      el('dt', { text: label }),
      el('dd', { text: value }),
    ]);
  }

  /** Кнопка опасного действия. Без права — отключена и объясняет почему.
   *
   * Отключённая кнопка НИЧЕГО не защищает: сервер проверяет право сам и
   * ответит 403 на прямой HTTP-запрос. Она нужна только чтобы человек не
   * гадал, куда делось действие.
   */
  function actionButton(options, allowed) {
    const node = el('button', {
      className: options.className || 'btn btn--small',
      text: options.text,
      dataset: allowed ? (options.dataset || {}) : {},
      title: allowed ? (options.title || '') : DENIED_HINT,
    });
    if (!allowed) {
      node.disabled = true;
      node.classList.add('btn--denied');
    }
    return node;
  }

  function replaceChildren(container, nodes) {
    container.textContent = '';
    (Array.isArray(nodes) ? nodes : [nodes]).filter(Boolean)
      .forEach((n) => container.appendChild(n));
  }

  // ─── Форматирование ────────────────────────────────────────────────────────
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

  // Данные ресурсов приходят с ПОЛУ-ДОВЕРЕННОГО воркера. Не число — значит не
  // показываем: число здесь единственный осмысленный тип.
  function num(value, fallback = '—') {
    return typeof value === 'number' && Number.isFinite(value) ? value : fallback;
  }

  function humanBytes(bytes) {
    if (typeof bytes !== 'number' || !Number.isFinite(bytes)) return '—';
    if (bytes < 1024) return `${bytes} Б`;
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} КБ`;
    if (bytes < 1024 * 1024 * 1024) return `${(bytes / 1024 / 1024).toFixed(2)} МБ`;
    return `${(bytes / 1024 / 1024 / 1024).toFixed(2)} ГБ`;
  }

  function humanStamp(epochSeconds) {
    if (!epochSeconds) return '—';
    return new Date(epochSeconds * 1000).toLocaleString('ru-RU');
  }

  const CONNECTION_LABEL = {
    online: '● онлайн', stale: '● связь нестабильна',
    offline: '● связь потеряна', reconnecting: '● догоняет события',
  };

  const EXECUTOR_LABEL = {
    online: '● исполнитель работает',
    stale: '● исполнитель молчит',
    offline: '● исполнитель остановлен',
    interrupted: '● исполнитель прерван',
    unknown: '● исполнитель неизвестен',
  };

  const DISK_LABEL = {
    ok: 'диск в норме', warning: 'мало места', critical: 'критически мало места',
    unknown: 'нет данных о диске',
  };

  // ─── HTTP ──────────────────────────────────────────────────────────────────
  function idempotencyKey() {
    // Ключ на КЛИК: повтор того же запроса (двойной клик, ретрай) не должен
    // выполнять действие второй раз.
    const rnd = (window.crypto && window.crypto.randomUUID)
      ? window.crypto.randomUUID()
      : `${Date.now()}-${Math.random().toString(36).slice(2)}`;
    return `ui-${rnd}`;
  }

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

  /** Опасное операторское действие: заголовок намерения + ключ идемпотентности. */
  async function dangerousPost(path, body) {
    return api(path, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        // Вместе с SameSite=lax у портальной cookie это и есть CSRF-защита:
        // межсайтовый простой запрос такого заголовка не поставит.
        'X-Requested-With': 'audit-workers',
        'Idempotency-Key': idempotencyKey(),
      },
      body: JSON.stringify(body || {}),
    });
  }

  /** Диалог опасного действия: причина + подтверждающая фраза. */
  function askConfirmation(title, warning, phrase) {
    const reason = window.prompt(`${title}\n\n${warning}\n\nПричина (обязательно):`, '');
    if (reason === null || !reason.trim()) return null;
    const typed = window.prompt(
      `Для подтверждения введите ровно: ${phrase}`, '');
    if (typed === null || typed.trim() !== phrase) {
      if (typed !== null) window.alert('Подтверждающая фраза не совпала — действие отменено.');
      return null;
    }
    return { reason: reason.trim(), confirmation: phrase };
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
    const executor = worker.executor || { status: 'unknown' };
    const diskReport = worker.disk || { level: 'unknown' };
    const freeSlots = offline ? 0 : (worker.calculated_free_slots ?? 0);
    const pending = worker.registration_status === 'pending';

    const head = el('header', { className: 'card-head' }, [
      el('div', {}, [
        el('h3', { text: worker.display_name || worker.worker_id }),
        el('p', { className: 'mono small', text: worker.worker_id }),
      ]),
      el('div', { className: 'card-status' }, [
        el('span', { className: `status status--${conn}`,
          text: `${CONNECTION_LABEL[conn] || conn}, ${humanAge(worker.seconds_since_seen)}` }),
        // Отдельная строка про исполнителя: агент онлайн ≠ VPS работает.
        el('span', {
          className: `status status--exec status--exec-${executor.status}`,
          text: EXECUTOR_LABEL[executor.status] || EXECUTOR_LABEL.unknown,
          title: executor.executor_instance_id
            ? `executor_instance_id: ${executor.executor_instance_id}`
            : 'исполнитель ещё не отчитывался',
        }),
      ]),
    ]);

    const slotInfo = worker.slots || null;
    const slotLine = slotInfo
      ? `занято ${slotInfo.occupancy_label}`
        + ` · центр насчитал свободных ${num(slotInfo.center_free_slots, 0)}`
        + ` · воркер заявил ${num(slotInfo.worker_claimed_free_slots, 0)}`
      : `свободно ${num(freeSlots, 0)} из ${num(worker.configured_max_slots, 1)}`;

    const list = el('dl', { className: 'kv' }, [
      kv('Регистрация', worker.registration_status),
      kv('Состояние', worker.worker_state),
      kv('Версия агента', worker.worker_version || '—'),
      kv('Протокол', `v${num(worker.protocol_version, 1)}`),
      kv('RAM', `${num(ram.available_gb)} / ${num(ram.total_gb)} ГБ`),
      kv('CPU', `${num(cpu.cores)} ядер · LA5 ${num(cpu.la5)}`),
      kv('Диск', `${num(disk.free_gb)} / ${num(disk.total_gb)} ГБ`),
      kv('Слоты', slotLine,
         slotInfo && slotInfo.slot_count_mismatch ? 'kv-critical' : ''),
      kv('Лимит слотов', slotInfo
        ? `${num(slotInfo.effective_limit, 1)} (ограничивает: ${slotInfo.limit_binding || '—'}`
          + ` · доказанный максимум этапа ${num(slotInfo.max_verified_slots, 1)})`
        : `${num(worker.configured_max_slots, 1)}`),
      kv('Активных заданий', (worker.active_jobs || []).length),
      kv('Исполнитель', [
        EXECUTOR_LABEL[executor.status] || executor.status,
        executor.last_heartbeat_at ? `· ${humanStamp(executor.last_heartbeat_at)}` : '',
        `· процессов: ${num(executor.running_processes, 0)}`,
        executor.ambiguous_processes ? `· неоднозначных: ${executor.ambiguous_processes}` : '',
      ].filter(Boolean).join(' ')),
      kv('Хранение', [
        DISK_LABEL[diskReport.level] || DISK_LABEL.unknown,
        `· свободно ${humanBytes(diskReport.free_bytes)}`,
        `· кандидатов на очистку ${num(diskReport.cleanup_candidates, 0)}`,
        `(${humanBytes(diskReport.cleanup_candidates_bytes)})`,
        `· неподтверждённых ${humanBytes(diskReport.unconfirmed_results_bytes)}`,
      ].join(' '), diskReport.level === 'critical' ? 'kv-critical' : ''),
    ]);

    const warnings = (worker.warnings || [])
      .filter((w) => w && typeof w === 'object')
      .map((w) => el('li', { className: 'warn', text: `⚠ ${w.message || w.code || ''}` }));

    // Управление воркерами и токенами — административные действия (§9 задания).
    const admin = state.perms.canAdmin;
    const actions = pending
      ? [
        actionButton({ className: 'btn btn--primary', text: 'Одобрить',
          dataset: { approve: worker.worker_id } }, admin),
        actionButton({ className: 'btn btn--danger', text: 'Отклонить',
          dataset: { reject: worker.worker_id } }, admin),
      ]
      : [
        actionButton({ className: 'btn', text: 'Отозвать',
          dataset: { revoke: worker.worker_id } }, admin),
      ];

    const card = el('article', {
      className: `card${offline ? ' card--offline' : ''}${pending ? ' card--pending' : ''}`,
    }, [head, list]);

    // Два активных проекта — ОТДЕЛЬНЫМИ строками: «активных заданий: 2» не
    // говорит ни какие это проекты, ни сколько каждое идёт.
    const active = (worker.active_jobs || []).filter((j) => j && typeof j === 'object');
    if (active.length) {
      card.appendChild(el('ul', { className: 'slot-jobs' }, active.map((job) => el(
        'li', { className: 'slot-job' }, [
          el('span', { className: 'mono small',
            text: String(job.job_id || '').slice(0, 8) }),
          el('span', { text: String(job.project_id || '—') }),
          el('span', { className: 'hint',
            text: `этап: ${job.stage || '—'} · событий: ${num(job.last_event_seq, 0)}`
                + (job.started_at
                  ? ` · идёт ${humanDuration((Date.now() / 1000) - job.started_at)}`
                  : '') }),
        ],
      ))));
    }
    if (slotInfo && slotInfo.slot_count_mismatch) {
      card.appendChild(el('p', { className: 'warn',
        text: 'slot_count_mismatch: воркер заявляет больше свободных слотов, чем '
            + 'насчитал центр. Используется меньшее — лишнее задание не выдаётся.' }));
    }
    if (slotInfo && slotInfo.unproven_warning) {
      card.appendChild(el('p', { className: 'warn', text: `⚠ ${slotInfo.unproven_warning}` }));
    }
    (slotInfo ? slotInfo.notices || [] : []).forEach((notice) => {
      card.appendChild(el('p', { className: 'hint', text: `⚠ ${notice}` }));
    });
    if (slotInfo && slotInfo.blocked_reason) {
      card.appendChild(el('p', { className: 'warn',
        text: `Новые задания не выдаются: ${slotInfo.blocked_reason}` }));
    }

    if (offline) {
      card.appendChild(el('p', { className: 'hint',
        text: 'Метрики — последние известные, на момент связи.' }));
    }
    if (executor.status !== 'online' && !pending) {
      card.appendChild(el('p', { className: 'warn',
        text: 'Локальный исполнитель не работает — новые задания выполняться не будут.' }));
    }
    if (diskReport.level === 'critical') {
      card.appendChild(el('p', { className: 'warn',
        text: 'Критически мало места: новые задания не выдаются. Текущие продолжают '
            + 'работу, неподтверждённые результаты не удаляются.' }));
    }
    if (warnings.length) card.appendChild(el('ul', { className: 'warnings' }, warnings));
    card.appendChild(el('footer', { className: 'card-actions' }, actions));
    return card;
  }

  // ─── Строка задания ────────────────────────────────────────────────────────
  function renderProgress(progress) {
    if (!progress) return null;
    if (progress.percent_reliable && progress.percent !== null) {
      const bar = el('div', { className: 'progress-bar' }, [el('span')]);
      bar.firstChild.style.width = `${num(progress.percent, 0)}%`;
      return el('div', { className: 'progress' }, [
        bar,
        el('span', { className: 'mono',
          text: `${num(progress.processed)} / ${num(progress.total)} `
              + `${progress.unit || ''} (${num(progress.percent)}%)` }),
      ]);
    }
    return el('div', { className: 'progress' }, [
      el('div', { className: 'progress-bar progress-bar--indeterminate' }, [el('span')]),
      el('span', { className: 'hint',
        text: `прогресс не оценивается · ${humanDuration(progress.elapsed_sec)} · `
            + `операций: ${num(progress.completed_operations, 0)}` }),
    ]);
  }

  function renderJob(job) {
    const progress = job.progress || null;
    const nodes = [];

    nodes.push(el('header', {}, [
      el('div', {}, [
        el('strong', { text: job.project_display_name || job.project_id }),
        el('span', { className: 'mono small',
          text: `${String(job.job_id).slice(0, 8)} · попытка ${num(job.attempt_no, 1)}` }),
      ]),
      el('span', { className: 'badge', text: job.display_status || job.state }),
    ]));

    const bar = renderProgress(progress);
    if (bar) nodes.push(bar);
    if (progress && progress.eta_sec) {
      nodes.push(el('p', { className: 'hint',
        text: `осталось ~${humanDuration(progress.eta_sec)}` }));
    }
    if (progress && progress.last_significant_event) {
      nodes.push(el('p', { className: 'hint',
        text: `последнее: ${progress.last_significant_event}` }));
    }
    if (job.validated_at) {
      nodes.push(el('dl', { className: 'kv kv--result' }, [
        kv('SHA-256', job.result_package_hash || '—'),
        kv('Размер', humanBytes(job.result_package_size)),
        kv('Принят', humanStamp(job.validated_at)),
        kv('Хранится до', humanStamp(job.retention_until)),
      ]));
    }
    if (job.retention_unconfirmed) {
      nodes.push(el('p', { className: 'warn',
        text: `⚠ ${job.retention_warning || 'Центр не подтвердил приём'}` }));
    }

    const actions = [
      el('button', { className: 'btn btn--small', text: 'Логи',
        dataset: { logs: job.job_id } }),
      el('button', { className: 'btn btn--small', text: 'Попытки',
        dataset: { attempts: job.job_id } }),
    ];
    if (job.state === 'completed') {
      const link = el('a', { className: 'btn btn--small', text: 'Скачать результат' });
      link.href = `/api/workers/jobs/${encodeURIComponent(job.job_id)}/result`;
      actions.push(link);
    }
    nodes.push(el('footer', { className: 'job-actions' }, actions));
    return el('article', { className: `job job--${job.state}` }, nodes);
  }

  // ─── История попыток ───────────────────────────────────────────────────────
  function renderAttempt(attempt, jobId) {
    const disposition = attempt.attempt_disposition || 'active';
    const nodes = [];

    nodes.push(el('header', { className: 'attempt-head' }, [
      el('div', {}, [
        el('strong', { text: `Попытка №${num(attempt.attempt_number, 1)}` }),
        el('span', { className: 'mono small', text: attempt.attempt_id }),
      ]),
      el('span', {
        className: `badge badge--${disposition}`,
        text: attempt.is_current
          ? `текущая · ${attempt.disposition_label || disposition}`
          : `устаревшая · ${attempt.disposition_label || disposition}`,
      }),
    ]));

    nodes.push(el('dl', { className: 'kv' }, [
      kv('VPS', attempt.assigned_worker_id || '—'),
      kv('Состояние исполнения', attempt.display_status || attempt.state),
      kv('Расположение', attempt.disposition_label || disposition),
      kv('Начата', humanStamp(attempt.started_at || attempt.assigned_at)),
      kv('Длительность', attempt.progress
        ? humanDuration(attempt.progress.elapsed_sec) : '—'),
      kv('Поколение назначения', num(attempt.assignment_generation, 1)),
      kv('Результат', attempt.result_storage_class || 'none'),
      kv('SHA-256', attempt.result_package_hash || '—'),
      kv('Приём подтверждён', attempt.result_acknowledged ? 'да' : 'нет'),
      kv('Хранится до', humanStamp(attempt.retention_until)),
      kv('Удалено с воркера', attempt.deleted_from_worker ? 'да' : 'нет'),
    ]));

    if (attempt.retention_unconfirmed) {
      nodes.push(el('p', { className: 'warn',
        text: '⚠ Центр не подтвердил приём — автоматическое удаление запрещено.' }));
    }
    if (attempt.error && attempt.error.message) {
      nodes.push(el('p', { className: 'warn', text: `Ошибка: ${attempt.error.message}` }));
    }

    if (attempt.superseded_result) {
      const sr = attempt.superseded_result;
      const link = el('a', { className: 'btn btn--small',
        text: 'Скачать пакет устаревшей попытки' });
      link.href = `/api/workers/jobs/${encodeURIComponent(jobId)}`
        + `/attempts/${encodeURIComponent(attempt.attempt_id)}/result`;
      nodes.push(el('div', { className: 'superseded' }, [
        el('p', { className: 'warn',
          text: '⚠ Не является актуальным результатом задания. '
              + 'Результат устаревшей попытки — автоматически не используется.' }),
        el('dl', { className: 'kv' }, [
          kv('SHA-256', sr.sha256 || '—'),
          kv('Размер', humanBytes(sr.size)),
          kv('Сохранён', humanStamp(sr.stored_at)),
        ]),
        link,
      ]));
    }

    (attempt.commands || []).forEach((command) => {
      const result = command.result && typeof command.result === 'object'
        ? (command.result.detail && command.result.detail.outcome)
          || command.result.status || ''
        : '';
      nodes.push(el('p', { className: 'hint',
        text: `Команда ${command.command_type}: ${command.status || '—'}`
            + ` · создана ${humanStamp(command.created_at)}`
            + ` · доставлена ${humanStamp(command.delivered_at)}`
            + ` · подтверждена ${humanStamp(command.acknowledged_at)}`
            + (result ? ` · результат: ${result}` : '') }));
    });

    (attempt.operator_actions || []).forEach((action) => {
      nodes.push(el('p', { className: 'hint',
        text: `${humanStamp(action.at)} — ${action.action_type} `
            + `(${action.actor}): ${action.reason || ''}` }));
    });

    // Управление попытками — уровень operate. Наблюдатель видит те же строки,
    // но кнопки у него отключены: право проверяет сервер, экран лишь честно
    // показывает, что действие недоступно.
    const canOperate = state.perms.canOperate;
    const actions = [];
    if (attempt.can_cancel) {
      actions.push(actionButton({ className: 'btn btn--small btn--danger',
        text: 'Отменить', dataset: { cancel: attempt.attempt_id, job: jobId } },
        canOperate));
    }
    if (attempt.can_mark_lost) {
      actions.push(actionButton({ className: 'btn btn--small btn--danger',
        text: 'Признать попытку потерянной',
        dataset: { marklost: attempt.attempt_id, job: jobId } }, canOperate));
    }
    if (!attempt.can_mark_lost) {
      actions.push(actionButton({ className: 'btn btn--small',
        text: 'Создать новую попытку',
        dataset: { newattempt: attempt.attempt_id, job: jobId } }, canOperate));
    }
    if (attempt.result_acknowledged && !attempt.deleted_from_worker) {
      actions.push(actionButton({ className: 'btn btn--small',
        text: 'Запросить удаление данных с VPS',
        dataset: { deletedata: attempt.attempt_id, job: jobId } }, canOperate));
    }
    if (actions.length) {
      nodes.push(el('footer', { className: 'attempt-actions' }, actions));
    }
    return el('article', {
      className: `attempt attempt--${disposition}${attempt.is_current ? ' attempt--current' : ''}`,
    }, nodes);
  }

  async function loadAttempts(jobId) {
    state.attemptsJobId = jobId;
    $('attemptsBlock').hidden = false;
    $('attemptsJobId').textContent = String(jobId).slice(0, 8);
    try {
      const data = await api(`/api/workers/jobs/${encodeURIComponent(jobId)}/attempts`);
      const job = data.job || {};
      $('attemptsJobTitle').textContent =
        `${job.project_display_name || job.project_external_id || ''}`
        + ` · сводно: ${job.overall_state || '—'}`;
      replaceChildren(
        $('attempts'),
        (data.attempts || []).map((a) => renderAttempt(a, jobId)),
      );
      // Сводный журнал решений — административные сведения (§9). Наблюдателю
      // и оператору сервер ответит 403, и дёргать его незачем.
      if (state.perms.canAdmin) {
        await loadAdminActions(jobId);
      } else {
        $('actionsBlock').hidden = true;
      }
    } catch (error) {
      replaceChildren($('attempts'),
        el('p', { className: 'warn', text: `Не удалось загрузить попытки: ${error.message}` }));
    }
  }

  async function loadAdminActions(jobId) {
    $('actionsBlock').hidden = false;
    try {
      const data = await api(
        `/api/workers/admin-actions?job_id=${encodeURIComponent(jobId)}&limit=100`);
      const rows = (data.actions || []).map((action) => el('div', { className: 'action-row' }, [
        el('span', { className: 'mono small', text: humanStamp(action.created_at) }),
        el('span', { className: 'badge', text: action.action_type }),
        el('span', { text: action.actor_display_name || action.actor_id }),
        el('span', { text: action.reason || '' }),
        el('span', { className: 'mono small', text: action.result_status || '' }),
      ]));
      replaceChildren($('adminActions'),
        rows.length ? rows : el('p', { className: 'hint', text: 'Действий пока не было.' }));
    } catch (error) {
      replaceChildren($('adminActions'),
        el('p', { className: 'warn', text: `Журнал недоступен: ${error.message}` }));
    }
  }

  // ─── Права текущего пользователя ───────────────────────────────────────────
  const ROLE_LABEL = {
    admin: 'администратор подсистемы',
    operator: 'оператор заданий',
    viewer: 'наблюдатель',
  };

  async function loadPermissions() {
    let me = null;
    try {
      me = await api('/api/workers/me');
    } catch (error) {
      me = null;
    }
    const perms = new Set((me && me.permissions) || []);
    state.perms = {
      subject: me ? me.subject : null,
      role: me ? me.role : null,
      authenticated: !!(me && me.authenticated),
      canView: perms.has(PERM.view),
      canOperate: perms.has(PERM.operate),
      canAdmin: perms.has(PERM.admin),
      diagnostics: me ? me.diagnostics : null,
    };
    renderPermissionsBanner();
    return state.perms;
  }

  function renderPermissionsBanner() {
    const banner = $('permsBanner');
    const p = state.perms;
    const parts = [];
    if (p.authenticated) {
      parts.push(el('strong', { text: `Вы вошли как ${p.subject || '—'}` }));
      parts.push(text(` · роль в подсистеме: ${ROLE_LABEL[p.role] || 'нет роли'}`));
    } else {
      parts.push(el('strong', { text: 'Сессия портала не найдена' }));
    }
    if (!p.canOperate) {
      parts.push(el('p', { className: 'hint',
        text: 'Управление заданиями недоступно: нужна роль оператора или '
            + 'администратора. Экран работает только на просмотр.' }));
    } else if (!p.canAdmin) {
      parts.push(el('p', { className: 'hint',
        text: 'Управление воркерами и токенами недоступно: это административные '
            + 'действия.' }));
    }
    if (p.diagnostics) {
      parts.push(el('p', { className: 'hint', text: p.diagnostics }));
    }
    replaceChildren(banner, parts);
    banner.hidden = false;
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
        $('permsBanner').hidden = true;
        return;
      }
      await loadPermissions();
      // Форма выдачи задания — уровень operate. Кнопка отключается, но это
      // косметика: сервер и так вернёт 403 на прямой POST.
      const submit = $('createForm').querySelector('button[type="submit"]');
      if (submit) {
        submit.disabled = !state.perms.canOperate;
        submit.title = state.perms.canOperate ? '' : DENIED_HINT;
      }
      if (status.config_error) {
        $('configError').hidden = false;
        $('configError').textContent = status.config_error;
      } else {
        $('configError').hidden = true;
      }
      if (!state.perms.canView) {
        // Без права просмотра списки всё равно вернут 403 — не мигаем ошибкой,
        // а честно говорим, чего не хватает.
        replaceChildren($('workers'), []);
        replaceChildren($('jobs'), []);
        replaceChildren($('summary'), []);
        return;
      }

      const [workersData, jobsData] = await Promise.all([
        api('/api/workers'),
        api('/api/workers/jobs/list?limit=50'),
      ]);
      state.workers = workersData.workers || [];
      state.jobs = jobsData.jobs || [];

      const s = workersData.summary || {};
      const summary = [
        el('span', {}, [text('VPS: '), el('strong', { text: num(s.total, 0) })]),
        el('span', {}, [text('онлайн: '), el('strong', { text: num(s.online, 0) })]),
        // Свободные слоты — РАССЧИТАННЫЕ ЦЕНТРОМ. Сумма обещаний воркеров не то
        // число, по которому назначают работу.
        el('span', {}, [text('свободных слотов (расчёт центра): '),
          el('strong', { text: num(s.free_slots, 0) })]),
        el('span', {}, [text('активных заданий: '),
          el('strong', { text: num(s.active_jobs, 0) })]),
      ];
      if (s.pending) {
        summary.push(el('span', { className: 'warn' },
          [text('ждут одобрения: '), el('strong', { text: num(s.pending, 0) })]));
      }
      if (s.slot_mismatch) {
        summary.push(el('span', { className: 'warn' },
          [text('расхождение по слотам: '),
           el('strong', { text: num(s.slot_mismatch, 0) })]));
      }
      replaceChildren($('summary'), summary);

      const pendingWorkers = state.workers.filter((w) => w.registration_status === 'pending');
      const activeWorkers = state.workers.filter((w) => w.registration_status !== 'pending');
      $('pendingBlock').hidden = pendingWorkers.length === 0;
      replaceChildren($('pending'), pendingWorkers.map(renderWorker));
      replaceChildren($('workers'), activeWorkers.map(renderWorker));
      $('workersEmpty').hidden = activeWorkers.length > 0;
      replaceChildren($('jobs'), state.jobs.map(renderJob));
      $('jobsEmpty').hidden = state.jobs.length > 0;

      const select = $('jobWorker');
      const previous = select.value;
      replaceChildren(select, state.workers
        .filter((w) => w.registration_status === 'approved')
        .map((w) => {
          const info = w.slots || {};
          const online = w.connection_status === 'online';
          const free = num(info.center_free_slots, 0);
          // Занятость НЕ запрещает создать задание: оно встанет в очередь и
          // уйдёт в работу после освобождения слота. Прятать эту возможность
          // за disabled значило бы расходиться с сервером, который её даёт.
          const suffix = !online
            ? ' — нет связи'
            : (free > 0 ? ` — свободно ${free}` : ' — слотов нет, встанет в очередь');
          const option = el('option', {
            text: `${w.display_name || w.worker_id}${suffix}`,
          });
          option.value = w.worker_id;
          option.disabled = !online;
          return option;
        }));
      if (previous) select.value = previous;

      if (state.logsJobId) await loadLogs(state.logsJobId);
      if (state.attemptsJobId) await loadAttempts(state.attemptsJobId);
    } catch (error) {
      $('configError').hidden = false;
      $('configError').textContent = `Не удалось обновить: ${error.message}`;
    }
  }

  async function loadLogs(jobId) {
    state.logsJobId = jobId;
    $('logsBlock').hidden = false;
    $('logsJobId').textContent = String(jobId).slice(0, 8);
    try {
      const data = await api(
        `/api/workers/jobs/${encodeURIComponent(jobId)}/logs?limit=500`);
      const lines = (data.lines || []).map(
        (l) => `[${String(l.seq).padStart(4, '0')}] `
             + `${String(l.level || '').toUpperCase()} ${l.message}`);
      $('logs').textContent = lines.length ? lines.join('\n') : '(лога пока нет)';
    } catch (error) {
      $('logs').textContent = `Ошибка загрузки логов: ${error.message}`;
    }
  }

  // ─── Операторские действия ─────────────────────────────────────────────────
  async function doCancel(jobId, attemptId) {
    const answer = askConfirmation(
      'Отменить попытку?',
      'Мгновенная остановка НЕ гарантируется. Если VPS сейчас офлайн, команда '
      + 'будет доставлена после восстановления связи. Уже готовый результат '
      + 'не уничтожается.',
      CONFIRM.cancel);
    if (!answer) return;
    const result = await dangerousPost(
      `/api/workers/jobs/${encodeURIComponent(jobId)}`
      + `/attempts/${encodeURIComponent(attemptId)}/cancel`,
      { reason: answer.reason, confirmation: answer.confirmation, grace_period_sec: 30 });
    window.alert(result.message || 'Отмена запрошена.');
  }

  async function doMarkLost(jobId, attemptId) {
    const worker = (state.workers || [])[0] || {};
    const answer = askConfirmation(
      'Признать попытку потерянной?',
      'Удалённый процесс может продолжать работу. После создания новой попытки '
      + 'результаты старой будут считаться устаревшими.\n'
      + `Последняя связь с VPS: ${humanAge(worker.seconds_since_seen)}.`,
      CONFIRM.markLost);
    if (!answer) return;
    const observed = window.prompt(
      'Что наблюдалось на стороне VPS (необязательно)?', '') || '';
    const result = await dangerousPost(
      `/api/workers/jobs/${encodeURIComponent(jobId)}`
      + `/attempts/${encodeURIComponent(attemptId)}/mark-lost`,
      {
        mandatory_reason: answer.reason,
        typed_confirmation: answer.confirmation,
        observed_worker_state: observed.slice(0, 200),
        optional_operator_note: '',
      });
    window.alert(result.message || 'Попытка признана потерянной.');
  }

  async function doNewAttempt(jobId, sourceAttemptId) {
    const workerId = $('jobWorker').value;
    if (!workerId) {
      window.alert('Выберите VPS в форме выдачи задания — новая попытка уйдёт на него.');
      return;
    }
    const answer = askConfirmation(
      `Создать новую попытку на ${workerId}?`,
      'Старая попытка сохраняется целиком: её события, результат и журнал '
      + 'остаются доступны. Автоматически её результат использован не будет.',
      CONFIRM.newAttempt);
    if (!answer) return;
    const result = await dangerousPost(
      `/api/workers/jobs/${encodeURIComponent(jobId)}/attempts`,
      {
        worker_id: workerId,
        reason: answer.reason,
        source_attempt_id: sourceAttemptId,
        confirmation: answer.confirmation,
      });
    window.alert(`Создана попытка №${result.attempt_number}.`);
  }

  async function doRequestDeletion(jobId, attemptId) {
    const answer = askConfirmation(
      'Запросить удаление локальных данных попытки?',
      'Удаляется только копия НА ВОРКЕРЕ. Центральная копия результата '
      + 'остаётся. Неподтверждённый результат не удаляется даже этой командой.',
      CONFIRM.deleteData);
    if (!answer) return;
    const result = await dangerousPost(
      `/api/workers/jobs/${encodeURIComponent(jobId)}`
      + `/attempts/${encodeURIComponent(attemptId)}/request-deletion`,
      { reason: answer.reason, confirmation: answer.confirmation });
    window.alert(result.message || 'Команда удаления поставлена в очередь.');
  }

  document.addEventListener('click', async (event) => {
    const target = event.target;
    if (!target || !target.closest) return;
    const approve = target.closest('[data-approve]');
    const reject = target.closest('[data-reject]');
    const revoke = target.closest('[data-revoke]');
    const logs = target.closest('[data-logs]');
    const attempts = target.closest('[data-attempts]');
    const cancel = target.closest('[data-cancel]');
    const markLost = target.closest('[data-marklost]');
    const newAttempt = target.closest('[data-newattempt]');
    const deleteData = target.closest('[data-deletedata]');
    try {
      if (reject) {
        if (!window.confirm('Отклонить заявку на регистрацию? Одноразовый '
          + 'claim-secret будет погашен, воркер токен не получит.')) return;
        await dangerousPost(
          `/api/workers/${encodeURIComponent(reject.dataset.reject)}/reject`, {});
        await refresh();
      } else if (approve) {
        await dangerousPost(
          `/api/workers/${encodeURIComponent(approve.dataset.approve)}/approve`,
          { configured_max_slots: 1 });
        await refresh();
      } else if (revoke) {
        if (!window.confirm('Отозвать доступ воркера? Токен будет погашен, '
          + 'новые задания выдаваться не будут.')) return;
        await dangerousPost(
          `/api/workers/${encodeURIComponent(revoke.dataset.revoke)}/revoke`, {});
        await refresh();
      } else if (logs) {
        await loadLogs(logs.dataset.logs);
      } else if (attempts) {
        await loadAttempts(attempts.dataset.attempts);
      } else if (cancel) {
        await doCancel(cancel.dataset.job, cancel.dataset.cancel);
        await refresh();
      } else if (markLost) {
        await doMarkLost(markLost.dataset.job, markLost.dataset.marklost);
        await refresh();
      } else if (newAttempt) {
        await doNewAttempt(newAttempt.dataset.job, newAttempt.dataset.newattempt);
        await refresh();
      } else if (deleteData) {
        await doRequestDeletion(deleteData.dataset.job, deleteData.dataset.deletedata);
        await refresh();
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
      const created = await dangerousPost('/api/workers/jobs', body);
      $('createHint').textContent = `создано: ${created.job.job_id.slice(0, 8)}`
        + (created.will_wait_for_slot ? ` · ${created.queue_note || 'ждёт слот'}` : '');
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
