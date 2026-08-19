// @ts-check
/**
 * Vue composition controller and reusable view components for the distributed UI.
 */
(function initDistributedFeature(root) {
    'use strict';

    const TAB_DEFINITIONS = Object.freeze([
        { key: 'overview', label: 'Обзор', path: '/distributed' },
        { key: 'queue', label: 'Очередь', path: '/distributed/queue' },
        { key: 'tasks', label: 'Задачи', path: '/distributed/tasks' },
        { key: 'workers', label: 'Воркеры', path: '/distributed/workers' },
        { key: 'limits', label: 'Лимиты', path: '/distributed/limits' },
        { key: 'diagnostics', label: 'Диагностика', path: '/distributed/diagnostics' },
    ]);

    const MODE_LABELS = Object.freeze({ full_codex: 'Full Codex', hybrid: 'Гибрид Claude + Codex', distributed_audit: 'Распределённый аудит' });
    const PRIORITY_LABELS = Object.freeze({ critical: 'Критический', high: 'Высокий', normal: 'Обычный', low: 'Низкий' });
    const STAGE_LABELS = Object.freeze({ queued: 'В очереди', transfer: 'Передача', preparing: 'Подготовка', auditing: 'Проверка', collecting: 'Сбор результата', returning: 'Возврат результата', importing: 'Импорт', done: 'Готово', error: 'Ошибка' });
    const TASK_STAGE_ORDER = Object.freeze(['queued', 'transfer', 'preparing', 'auditing', 'collecting', 'returning', 'importing', 'done']);

    /**
     * Почему остаток такой, какой есть, — словами.
     *
     * Воркер присылает КОД, а не текст: свободная строка с полу-доверенной
     * стороны, показанная в браузере, — это чужой текст на нашем экране.
     * Разворачивание кода в предложение живёт здесь, и здесь же видно, что
     * недокументированный источник не выдаётся за официальный.
     */
    const QUOTA_REASON_TEXT = Object.freeze({
        organization_subscription_access_disabled: 'Вход выполнен, но владелец организации запретил доступ Claude Code для этой учётной записи. Повторный вход не поможет: нужен доступ от администратора организации либо другая учётная запись.',
        local_cache_available: 'Локальные данные Claude Code. Официального машиночитаемого остатка у Claude Code нет — показано то, что CLI сохранил у себя.',
        local_cache_stale: 'Локальные данные Claude Code, уже неактуальные: показано последнее известное значение, а не текущее.',
        local_cache_missing: 'Локальные данные об использовании Claude пока не появились — они возникают после обращений к модели на этом воркере.',
        local_cache_schema_unsupported: 'Формат локальных данных Claude Code не распознан: источник недокументирован и мог измениться с обновлением CLI.',
        no_safe_supported_source: 'Claude Code не сообщает остаток лимита без обращения к модели, а локальных данных об использовании на воркере пока нет.',
    });

    /** @param {any} quota */
    function quotaReasonText(quota) {
        const code = quota && quota.reason;
        return (code && QUOTA_REASON_TEXT[code]) || '';
    }

    /** Возраст НАБЛЮДЕНИЯ. Не «когда мы посмотрели», а «когда это было верно».
     * @param {any} quota */
    function quotaAgeText(quota) {
        const age = quota && quota.ageSec;
        if (!Number.isFinite(age)) return '';
        if (age < 90) return 'данные только что';
        if (age < 5400) return `данные ${Math.round(age / 60)} мин назад`;
        if (age < 172800) return `данные ${Math.round(age / 3600)} ч назад`;
        return `данные ${Math.round(age / 86400)} сут назад`;
    }

    /** @param {any} quota */
    function quotaWindows(quota) {
        return (quota && Array.isArray(quota.windows)) ? quota.windows : [];
    }

    /** @param {string} path */
    function routeToTab(path) {
        const clean = String(path || '').replace(/\/+$/, '') || '/';
        if (clean === '/distributed') return 'overview';
        const match = clean.match(/^\/distributed\/(queue|tasks|workers|limits|diagnostics)$/);
        return match ? match[1] : null;
    }

    /** @param {number|null|undefined} value */
    function usageTone(value) {
        if (!Number.isFinite(value)) return 'unavailable';
        if (value >= 90) return 'danger';
        if (value >= 70) return 'warning';
        return 'normal';
    }

    /** @param {number} bytes */
    function formatBytes(bytes) {
        if (!Number.isFinite(bytes) || bytes <= 0) return '—';
        return `${(bytes / 1024 / 1024).toFixed(bytes >= 104857600 ? 0 : 1)} МБ`;
    }

    /** @param {Date} date */
    function localDate(date) {
        const year = date.getFullYear();
        const month = String(date.getMonth() + 1).padStart(2, '0');
        const day = String(date.getDate()).padStart(2, '0');
        return `${year}-${month}-${day}`;
    }

    /** @param {any[]} rows @param {string} period @param {string=} customFrom @param {string=} customTo @param {Date=} anchor */
    function filterCompletedTasks(rows, period, customFrom, customTo, anchor = new Date()) {
        const toDate = new Date(anchor.getTime());
        const fromDate = new Date(anchor.getTime());
        if (period === '7d') fromDate.setDate(fromDate.getDate() - 6);
        if (period === '30d') fromDate.setDate(fromDate.getDate() - 29);
        let from = localDate(fromDate);
        let to = localDate(toDate);
        if (period === 'custom') {
            from = customFrom || '0000-01-01';
            to = customTo || '9999-12-31';
        }
        return (rows || []).filter((task) => {
            const raw = String(task.completedAtIso || '');
            const parsed = raw ? new Date(raw) : null;
            const date = parsed && Number.isFinite(parsed.getTime()) ? localDate(parsed) : raw.slice(0, 10);
            return date && date >= from && date <= to;
        });
    }

    /** @param {{service?:any}=} options */
    function createManager(options = {}) {
        const VueRuntime = root.Vue;
        const { ref, computed, reactive } = VueRuntime;
        const service = options.service || root.DistributedData.createDefaultService();
        const isDemo = service.mode === 'mock';
        const readOnly = Boolean(service.readOnly);
        const today = new Date();

        const activeTab = ref('overview');
        const taskSubtab = ref('active');
        const historyPeriod = ref('today');
        const historyFrom = ref(localDate(new Date(today.getFullYear(), today.getMonth(), 1)));
        const historyTo = ref(localDate(today));
        const loading = ref(false);
        const loaded = ref(false);
        const error = ref('');
        const overview = ref(null);
        const workers = ref([]);
        const queue = ref([]);
        const tasks = ref({ active: [], completed: [], errors: [] });
        const limits = ref([]);
        const diagnostics = ref([]);
        const assignmentByProject = ref({});
        const selectedTask = ref(null);
        const selectedWorker = ref(null);
        const selectedDiagnostic = ref(null);
        const modal = ref(null);
        const transferWorkerId = ref('worker-mow-03');
        const toast = ref(null);
        let toastTimer = 0;

        const recommendationProject = computed(() => {
            const recommendation = overview.value && overview.value.recommendation;
            return recommendation && overview.value.projects.find((project) => project.id === recommendation.projectId);
        });
        const recommendationWorker = computed(() => {
            const recommendation = overview.value && overview.value.recommendation;
            return recommendation && overview.value.workers.find((worker) => worker.id === recommendation.workerId);
        });
        const limitsSummary = computed(() => {
            const online = limits.value.filter((item) => item.online);
            const withValue = (provider) => online.filter((item) => Number.isFinite(item[provider] && item[provider].percentageRemaining));
            const average = (provider) => {
                const rows = withValue(provider);
                return rows.length ? Math.round(rows.reduce((sum, item) => sum + item[provider].percentageRemaining, 0) / rows.length) : null;
            };
            const best = (provider) => [...withValue(provider)].sort((a, b) => b[provider].percentageRemaining - a[provider].percentageRemaining)[0] || null;
            const resetEntries = online.flatMap((item) => [
                { provider: 'Claude', resetAt: item.claude.resetAt, resetIn: item.claude.resetIn, workerName: item.workerName },
                { provider: 'Codex', resetAt: item.codex.resetAt, resetIn: item.codex.resetIn, workerName: item.workerName },
            ]).filter((item) => item.resetAt).sort((a, b) => String(a.resetAt).localeCompare(String(b.resetAt)));
            const next = resetEntries[0];
            return { claude: average('claude'), codex: average('codex'), nextReset: next ? `${next.resetIn || 'время недоступно'} · ${next.provider}, ${next.workerName}` : 'Недоступно', bestClaude: best('claude')?.workerName || 'Недоступно', bestCodex: best('codex')?.workerName || 'Недоступно' };
        });
        const visibleCompletedTasks = computed(() => {
            return filterCompletedTasks(tasks.value.completed, historyPeriod.value, historyFrom.value, historyTo.value, new Date());
        });

        async function load(force = false) {
            if (loaded.value && !force) return;
            loading.value = true;
            error.value = '';
            try {
                let overviewData, workersData, queueData, tasksData, limitsData, diagnosticsData;
                if (typeof service.getSnapshot === 'function') {
                    const snapshot = await service.getSnapshot();
                    overviewData = snapshot.overview;
                    workersData = snapshot.workers;
                    queueData = snapshot.queue;
                    tasksData = snapshot.tasks;
                    limitsData = snapshot.limits;
                    diagnosticsData = snapshot.diagnostics;
                } else {
                    [overviewData, workersData, queueData, tasksData, limitsData, diagnosticsData] = await Promise.all([
                        service.getOverview(), service.getWorkers(), service.getQueue(), service.getTasks(), service.getProviderLimits(), service.getDiagnostics(),
                    ]);
                }
                overview.value = overviewData;
                workers.value = workersData;
                queue.value = queueData;
                tasks.value = tasksData;
                limits.value = limitsData;
                diagnostics.value = diagnosticsData;
                const assignments = {};
                for (const project of overviewData.projects || []) assignments[project.id] = project.assignment || 'auto';
                assignmentByProject.value = assignments;
                loaded.value = true;
            } catch (loadError) {
                error.value = String(loadError && loadError.message ? loadError.message : loadError);
            } finally {
                loading.value = false;
            }
        }

        async function refresh() { await load(true); }

        /** @param {string} tab */
        function setTab(tab) {
            if (TAB_DEFINITIONS.some((item) => item.key === tab)) activeTab.value = tab;
        }

        /** @param {string} tab */
        function goToTab(tab) {
            const definition = TAB_DEFINITIONS.find((item) => item.key === tab) || TAB_DEFINITIONS[0];
            root.location.hash = definition.path;
        }

        /** @param {string} message @param {'success'|'warning'|'error'=} tone */
        function notify(message, tone = 'success') {
            root.clearTimeout(toastTimer);
            toast.value = { message, tone };
            toastTimer = root.setTimeout(() => { toast.value = null; }, 3600);
        }

        /** @param {string} workerId */
        function workerById(workerId) { return workers.value.find((worker) => worker.id === workerId) || null; }
        /** @param {string} workerId */
        function workerName(workerId) { const worker = workerById(workerId); return worker ? worker.name : 'Автоматически'; }
        /** @param {string} mode */
        function modeLabel(mode) { return MODE_LABELS[mode] || mode; }
        /** @param {string} priority */
        function priorityLabel(priority) { return PRIORITY_LABELS[priority] || priority; }
        /** @param {string} stage */
        function stageLabel(stage) { return STAGE_LABELS[stage] || stage; }
        /** @param {number|null|undefined} value */
        function progressStyle(value) { return { width: `${Number.isFinite(value) ? Math.min(100, Math.max(0, Number(value))) : 0}%` }; }
        /** @param {any} task */
        function progressText(task) {
            if (!task || !Number.isFinite(task.progressPercent)) return 'Прогресс недоступен';
            return `${task.progressKind === 'estimated' ? '≈ ' : ''}${task.progressPercent}%`;
        }
        /** @param {number|null|undefined} value @param {string=} suffix */
        function metricText(value, suffix = '%') { return Number.isFinite(value) ? `${value}${suffix}` : 'Нет телеметрии'; }
        /** «Ещё не опрашивали» — это не «недоступен» и не «нет лимита».
         *  @param {any} quota */
        function quotaText(quota) {
            if (quota && quota.status === 'not_observed') return 'Ещё не опрошен';
            return quota && Number.isFinite(quota.percentageRemaining) ? `${quota.percentageRemaining}%` : 'Остаток недоступен';
        }
        const OUTBOX_STATUS_LABELS = {
            synced: 'Синхронизировано', pending: 'Ожидается синхронизация',
            stale: 'Данные устарели', unavailable: 'Нет данных',
        };
        /** Состояние журнала событий словом. `null` пользователю не показываем:
         *  он читается либо как ноль, либо как поломка.
         *  @param {any} diagnostic */
        function outboxStatusText(diagnostic) {
            const outbox = diagnostic && diagnostic.eventOutbox || {};
            return OUTBOX_STATUS_LABELS[outbox.status] || OUTBOX_STATUS_LABELS.unavailable;
        }
        /** @param {any} diagnostic */
        function outboxAvailable(diagnostic) {
            const outbox = diagnostic && diagnostic.eventOutbox || {};
            return [outbox.lastAckedSeq, outbox.lastWrittenSeq, outbox.pending].every(Number.isFinite);
        }
        /** @param {any} diagnostic */
        function outboxText(diagnostic) {
            const outbox = diagnostic && diagnostic.eventOutbox || {};
            return outboxAvailable(diagnostic)
                ? `${outboxStatusText(diagnostic)} · последняя попытка ${outbox.lastAckedSeq}/${outbox.lastWrittenSeq}, ожидает всего ${outbox.pending}`
                : OUTBOX_STATUS_LABELS.unavailable;
        }
        /** @param {string} projectId @param {string} workerId */
        async function setAssignment(projectId, workerId) {
            try {
                const project = await service.assignTask(projectId, workerId);
                assignmentByProject.value = { ...assignmentByProject.value, [projectId]: workerId };
                const target = workerId === 'auto' ? 'автоматическое распределение' : `VPS ${workerName(workerId)}`;
                notify(`${project.project} → ${project.packageName}: выбрано ${target}`);
                return true;
            } catch (actionError) {
                notify(String(actionError.message || actionError), 'error');
                return false;
            }
        }

        /** @param {any} project @param {string=} explicitWorkerId */
        function openSend(project, explicitWorkerId) {
            const selected = explicitWorkerId || assignmentByProject.value[project.id] || project.assignment || 'auto';
            const resolved = selected === 'auto' && overview.value && overview.value.recommendation && overview.value.recommendation.projectId === project.id
                ? overview.value.recommendation.workerId : selected;
            modal.value = { type: 'send', project, workerId: resolved };
        }

        async function confirmSend() {
            if (!modal.value || modal.value.type !== 'send') return;
            const { project, workerId } = modal.value;
            try {
                const result = await service.sendTask(project.id, workerId);
                modal.value = null;
                const target = workerId === 'auto' ? 'с автоматическим назначением' : `на VPS ${workerName(workerId)}`;
                notify(`${result.project} → ${result.packageName} добавлен ${isDemo ? 'в демо-очередь' : 'в очередь'} ${target}`);
                await refresh();
            } catch (actionError) { notify(String(actionError.message || actionError), 'error'); }
        }

        function openWhy() { modal.value = { type: 'why', recommendation: overview.value && overview.value.recommendation }; }
        function openAlternative() { modal.value = { type: 'alternative', project: recommendationProject.value, workerId: recommendationWorker.value && recommendationWorker.value.id }; }
        async function applyAlternative() {
            if (!modal.value || modal.value.type !== 'alternative') return;
            const chosenWorkerId = modal.value.workerId;
            const project = modal.value.project;
            const changed = await setAssignment(project.id, chosenWorkerId);
            if (!changed) return;
            if (overview.value && overview.value.recommendation) {
                const chosenWorker = workerById(chosenWorkerId);
                overview.value.recommendation.workerId = chosenWorkerId;
                if (chosenWorker) {
                    overview.value.recommendation.freeSlots = chosenWorker.slots.total - chosenWorker.slots.used;
                    overview.value.recommendation.gpu = chosenWorker.resources.gpu;
                    overview.value.recommendation.claude = chosenWorker.quotas.claude.percentageRemaining;
                    overview.value.recommendation.codex = chosenWorker.quotas.codex.percentageRemaining;
                    overview.value.recommendation.reasons = [
                        `свободно ${chosenWorker.slots.total - chosenWorker.slots.used} из ${chosenWorker.slots.total} слотов`,
                        `GPU загружен на ${chosenWorker.resources.gpu}%`,
                        `Claude: осталось примерно ${chosenWorker.quotas.claude.percentageRemaining}%`,
                        `Codex: осталось примерно ${chosenWorker.quotas.codex.percentageRemaining}%`,
                        `сброс Claude через ${chosenWorker.quotas.claude.resetIn}`,
                        `узел подходит под режим ${modeLabel(project.mode)}`,
                    ];
                }
            }
            modal.value = null;
        }

        /** @param {any} item */
        function openQueueWhy(item) {
            const worker = workerById(item.suggestedWorkerId);
            modal.value = { type: 'queue-why', item, worker };
        }

        /** @param {any} item @param {'first'|'up'|'down'|'last'} direction */
        async function moveQueue(item, direction) {
            try {
                queue.value = await service.moveQueueItem(item.id, direction);
                notify(`${item.project} → ${item.packageName}: позиция в очереди изменена`);
                if (overview.value) overview.value.queuePreview = queue.value.slice(0, 5);
            } catch (actionError) { notify(String(actionError.message || actionError), 'error'); }
        }

        /** @param {any} item @param {string} priority */
        async function changePriority(item, priority) {
            try {
                await service.changePriority(item.id, priority);
                item.priority = priority;
                notify(`${item.project} → ${item.packageName}: приоритет — ${priorityLabel(priority)}`);
            } catch (actionError) { notify(String(actionError.message || actionError), 'error'); }
        }

        /** @param {any} task */
        function openTask(task) {
            if (!task) return;
            const fullTask = [...tasks.value.active, ...tasks.value.completed, ...tasks.value.errors].find((item) => item.id === task.id);
            selectedTask.value = fullTask || task;
        }
        /** @param {string} taskId */
        function taskById(taskId) { return [...tasks.value.active, ...tasks.value.completed, ...tasks.value.errors].find((item) => item.id === taskId) || null; }
        /** @param {string} taskId */
        function openAttentionTask(taskId) { openTask(taskById(taskId)); }
        /** @param {string} taskId */
        function retryAttentionTask(taskId) { const task = taskById(taskId); if (task) retryTask(task); }
        /** @param {string} taskId */
        function transferAttentionTask(taskId) { const task = taskById(taskId); if (task) openTransfer(task); }
        /** @param {any} worker */
        function openWorker(worker) { selectedWorker.value = worker; }
        /** @param {any} row */
        function openDiagnostic(row) { selectedDiagnostic.value = row; }
        function openWorkerDiagnostic() {
            if (!selectedWorker.value) return;
            const workerId = selectedWorker.value.diagnostic.workerId;
            const row = diagnostics.value.find((item) => item.diagnostic.workerId === workerId);
            selectedWorker.value = null;
            if (row) selectedDiagnostic.value = row;
        }

        /** @param {any} worker @param {boolean} accepts */
        async function toggleWorkerIntake(worker, accepts) {
            try {
                const updated = await service.setWorkerIntake(worker.id, accepts);
                worker.acceptsNewTasks = updated.acceptsNewTasks;
                notify(`VPS ${worker.name}: ${accepts ? 'приём новых задач включён' : 'новые назначения приостановлены'}`, accepts ? 'success' : 'warning');
            } catch (actionError) { notify(String(actionError.message || actionError), 'error'); }
        }

        /** @param {any} task */
        async function retryTask(task) {
            try {
                const updated = await service.retryTask(task.id);
                notify(`${updated.project} → ${updated.packageName}: повтор запущен${isDemo ? ' в демо-режиме' : ''}`);
                selectedTask.value = null;
                await refresh();
            } catch (actionError) { notify(String(actionError.message || actionError), 'error'); }
        }

        /** @param {any} task */
        function openTransfer(task) {
            transferWorkerId.value = workers.value.find((worker) => worker.status !== 'offline' && worker.id !== task.workerId)?.id || task.workerId;
            modal.value = { type: 'transfer', task };
        }

        async function confirmTransfer() {
            if (!modal.value || modal.value.type !== 'transfer') return;
            try {
                const updated = await service.transferTask(modal.value.task.id, transferWorkerId.value);
                modal.value = null;
                selectedTask.value = null;
                notify(`${updated.project} → ${updated.packageName} переносится на VPS ${workerName(transferWorkerId.value)}${isDemo ? ' (демо)' : ''}`);
                await refresh();
            } catch (actionError) { notify(String(actionError.message || actionError), 'error'); }
        }

        /** @param {any} task @param {string} stage */
        function taskStageState(task, stage) {
            if (task.stage === 'error') return stage === 'auditing' ? 'error' : 'pending';
            const currentIndex = TASK_STAGE_ORDER.indexOf(task.stage);
            const stageIndex = TASK_STAGE_ORDER.indexOf(stage);
            if (stageIndex < currentIndex || task.stage === 'done') return 'done';
            if (stageIndex === currentIndex) return 'current';
            return 'pending';
        }

        /** @param {any} task @param {string} stage */
        function taskStageText(task, stage) {
            const state = taskStageState(task, stage);
            if (state === 'done') return '✓';
            if (state === 'current') return stage === 'auditing' ? progressText(task) : 'сейчас';
            if (state === 'error') return '!';
            return '';
        }

        async function copyDiagnostics() {
            const text = service.getSafeDiagnosticsText();
            try {
                if (root.navigator && root.navigator.clipboard) await root.navigator.clipboard.writeText(text);
                else {
                    const area = root.document.createElement('textarea');
                    area.value = text;
                    root.document.body.appendChild(area);
                    area.select();
                    root.document.execCommand('copy');
                    area.remove();
                }
                notify('Безопасная диагностика скопирована');
            } catch (_) { notify('Не удалось скопировать диагностику', 'error'); }
        }

        /** @param {any} diagnostic */
        function diagnosticRows(diagnostic) {
            if (!diagnostic) return [];
            const outbox = diagnostic.eventOutbox || {};
            const releases = diagnostic.releases || {};
            const show = (value) => value === null || value === undefined || value === '' ? 'Нет данных' : value;
            return [
                ['worker_id', show(diagnostic.workerId)], ['instance_id', show(diagnostic.instanceId)], ['transport', show(diagnostic.transport)], ['grpc_stream', show(diagnostic.grpcStream)], ['connection_id', show(diagnostic.connectionId)], ['mTLS', show(diagnostic.mtls)], ['heartbeat', show(diagnostic.heartbeat)],
                ['Gateway target', show(diagnostic.gatewayTarget) === 'Нет данных' && diagnostic.gatewayTargetNote ? diagnostic.gatewayTargetNote : show(diagnostic.gatewayTarget)],
                ['source host', show(diagnostic.sourceHost)], ['result host', show(diagnostic.resultHost)], ['nginx', show(diagnostic.nginx)], ['Agent status', show(diagnostic.agentStatus)], ['Executor status', show(diagnostic.executorStatus)],
                ['EventOutbox', outboxStatusText(diagnostic)],
                ['EventOutbox · записано (последняя попытка)', outboxAvailable(diagnostic) ? outbox.lastWrittenSeq : 'Нет данных'],
                ['EventOutbox · подтверждено (последняя попытка)', outboxAvailable(diagnostic) ? outbox.lastAckedSeq : 'Нет данных'],
                ['EventOutbox · ожидает (всего по попыткам)', outboxAvailable(diagnostic) ? outbox.pending : 'Нет данных'],
                ['EventOutbox · попыток учтено', Number.isFinite(outbox.attempts) ? outbox.attempts : 'Нет данных'],
                ['EventOutbox · последнее подтверждение', show(outbox.lastAckAt)],
                ['ResultAck', show(diagnostic.resultAck)], ['worker version', show(diagnostic.workerVersion)],
                ['Релиз центра', show(releases.centerRelease)], ['Релиз шлюза', show(releases.gatewayRelease)], ['Релиз воркера', show(diagnostic.workerRelease)],
                ['Совместимость релизов', releases.status === 'ok' ? `OK — ${releases.reason}` : show(releases.reason)],
                ['runtime version', show(diagnostic.runtimeVersion)], ['uptime', show(diagnostic.uptime)], ['cert expiry', show(diagnostic.certExpiry)],
            ];
        }

        return reactive({
            tabs: TAB_DEFINITIONS, activeTab, taskSubtab, historyPeriod, historyFrom, historyTo,
            loading, loaded, error, overview, workers, queue, tasks, limits, diagnostics, isDemo, readOnly,
            assignmentByProject, selectedTask, selectedWorker, selectedDiagnostic, modal, transferWorkerId, toast,
            recommendationProject, recommendationWorker, limitsSummary, visibleCompletedTasks,
            load, refresh, setTab, goToTab, notify, workerById, workerName,
            modeLabel, priorityLabel, stageLabel, progressStyle, progressText, metricText, quotaText, outboxText, usageTone, formatBytes,
            quotaReasonText, quotaAgeText, quotaWindows,
            setAssignment, openSend, confirmSend, openWhy, openAlternative, applyAlternative,
            openQueueWhy, moveQueue, changePriority, openTask, taskById,
            openAttentionTask, retryAttentionTask, transferAttentionTask,
            openWorker, openDiagnostic, openWorkerDiagnostic,
            toggleWorkerIntake, retryTask, openTransfer, confirmTransfer,
            taskStageState, taskStageText, copyDiagnostics, diagnosticRows,
            closeModal: () => { modal.value = null; },
            closeTask: () => { selectedTask.value = null; },
            closeWorker: () => { selectedWorker.value = null; },
            closeDiagnostic: () => { selectedDiagnostic.value = null; },
        });
    }

    function registerComponents(app) {
        app.component('distributed-dispatcher-page', root.DistributedPage);
        app.component('distributed-quota-bar', {
            props: { label: String, quota: Object, stale: Boolean },
            computed: {
                // Окна лимита приходят отсортированными «самое ограничивающее
                // первым», и главное число карточки относится именно к нему.
                windows() { return quotaWindows(this.quota); },
                primaryWindow() { return this.windows[0] || null; },
                otherWindows() { return this.windows.slice(1); },
                reasonText() { return quotaReasonText(this.quota); },
                ageText() { return quotaAgeText(this.quota); },
                undocumented() { return this.quota && this.quota.sourceStability === 'undocumented'; },
                // Доказанный отказ провайдера: авторизация исправна, работать
                // нельзя. Показывается вместо процента — числа тут нет и быть
                // не может, а «Остаток недоступен» увело бы к поиску квоты.
                entitlementBlocked() { return this.quota && this.quota.status === 'entitlement_blocked'; },
            },
            template: `
                <div class="distributed-quota" :class="'distributed-quota--' + (quota.status || 'unknown')">
                    <div class="distributed-quota__top">
                        <span class="distributed-quota__name">{{ label }}</span>
                        <strong v-if="entitlementBlocked" class="distributed-quota__blocked">Доступ запрещён организацией</strong>
                        <strong v-else-if="Number.isFinite(quota.percentageRemaining)">{{ quota.percentageRemaining }}% <small>осталось</small></strong>
                        <strong v-else>Остаток недоступен</strong>
                    </div>
                    <div v-if="entitlementBlocked" class="distributed-quota__window">Вход выполнен · работа запрещена</div>
                    <div v-else-if="primaryWindow" class="distributed-quota__window">{{ primaryWindow.label }}</div>
                    <div v-if="entitlementBlocked" class="distributed-progress distributed-progress--unavailable" aria-label="Доступ запрещён организацией"><span></span></div>
                    <div v-else-if="Number.isFinite(quota.percentageRemaining)" class="distributed-progress distributed-progress--quota" role="progressbar" :aria-label="label + ': осталось ' + quota.percentageRemaining + '%'" :aria-valuenow="quota.percentageRemaining" aria-valuemin="0" aria-valuemax="100">
                        <span :style="{width: quota.percentageRemaining + '%'}"></span>
                    </div>
                    <div v-else class="distributed-progress distributed-progress--unavailable" aria-label="Остаток недоступен"><span></span></div>
                    <div class="distributed-quota__reset">
                        <span>{{ entitlementBlocked ? 'Задания этому провайдеру не назначаются' : stale ? 'Последние известные данные' : quota.resetIn ? 'Сброс через ' + quota.resetIn : 'Дата сброса недоступна' }}</span>
                        <span v-if="quota.isEstimated" title="Провайдер передаёт приблизительное значение">≈ оценка</span>
                    </div>
                    <ul v-if="otherWindows.length" class="distributed-quota__windows">
                        <li v-for="window in otherWindows" :key="window.windowId">
                            <span>{{ window.label }}</span>
                            <span>{{ Number.isFinite(window.remainingPercent) ? window.remainingPercent + '% осталось' : 'остаток недоступен' }}<template v-if="window.resetIn"> · сброс через {{ window.resetIn }}</template></span>
                        </li>
                    </ul>
                    <div v-if="stale && ageText" class="distributed-quota__age distributed-quota__age--stale">⚠ {{ ageText }}</div>
                    <div v-else-if="ageText" class="distributed-quota__age">{{ ageText }}</div>
                    <div v-if="reasonText" class="distributed-quota__note" :class="{'distributed-quota__note--undocumented': undocumented}">{{ reasonText }}</div>
                </div>`,
        });

        app.component('distributed-worker-card', {
            props: { worker: Object, detailed: Boolean },
            emits: ['detail', 'toggle-intake', 'task'],
            methods: {
                modeLabel(mode) { return MODE_LABELS[mode] || mode; },
                stageLabel(stage) { return STAGE_LABELS[stage] || stage; },
                usageTone,
                metricText(value, suffix = '%') { return Number.isFinite(value) ? value + suffix : 'Нет телеметрии'; },
                progressText(task) { return !Number.isFinite(task.progressPercent) ? 'Прогресс недоступен' : (task.progressKind === 'estimated' ? '≈ ' : '') + task.progressPercent + '%'; },
            },
            template: `
                <article class="distributed-worker-card" :class="['distributed-worker-card--' + worker.status, {'distributed-worker-card--detailed': detailed}]">
                    <header class="distributed-worker-card__header">
                        <div>
                            <h3>VPS {{ worker.name }}</h3>
                            <span class="distributed-worker-card__heartbeat">{{ worker.status === 'offline' ? 'Последняя связь ' : 'Heartbeat ' }}{{ worker.lastHeartbeat }}</span>
                        </div>
                        <span class="distributed-status" :class="'distributed-status--' + worker.status"><i></i>{{ worker.status === 'offline' ? 'Offline' : worker.status === 'busy' ? 'Занят' : 'Online' }}</span>
                    </header>
                    <div class="distributed-resources">
                        <div v-for="metric in [{key:'cpu',label:'CPU',value:worker.resources.cpu},{key:'ram',label:'RAM',value:worker.resources.ram},{key:'gpu',label:'GPU',value:worker.resources.gpu}]" :key="metric.key" class="distributed-resource">
                            <div><span>{{ metric.label }}</span><strong>{{ worker.status === 'offline' ? '—' : metricText(metric.value) }}</strong></div>
                            <div class="distributed-progress" :class="'distributed-progress--' + usageTone(metric.value)"><span :style="{width: (worker.status === 'offline' ? 0 : metric.value) + '%'}"></span></div>
                        </div>
                        <div class="distributed-resource">
                            <div><span>VRAM</span><strong>{{ worker.status === 'offline' ? '—' : Number.isFinite(worker.resources.vramUsedGb) && Number.isFinite(worker.resources.vramTotalGb) ? worker.resources.vramUsedGb + ' / ' + worker.resources.vramTotalGb + ' ГБ' : 'Нет телеметрии' }}</strong></div>
                            <div class="distributed-progress" :class="'distributed-progress--' + usageTone(worker.resources.vramUsedGb / worker.resources.vramTotalGb * 100)"><span :style="{width: (worker.status === 'offline' || !Number.isFinite(worker.resources.vramUsedGb) || !Number.isFinite(worker.resources.vramTotalGb) ? 0 : worker.resources.vramUsedGb / worker.resources.vramTotalGb * 100) + '%'}"></span></div>
                        </div>
                    </div>
                    <div class="distributed-worker-card__slots">
                        <span>Слоты <b>{{ worker.slots.occupiedSlots ?? worker.slots.used }} / {{ worker.slots.totalSlots ?? worker.slots.total }}</b></span>
                        <span :class="{'is-free': (worker.slots.physicalFreeSlots ?? (worker.slots.total - worker.slots.used)) > 0}">{{ worker.status === 'offline' ? 'недоступен' : (worker.slots.physicalFreeSlots ?? (worker.slots.total - worker.slots.used)) + ' физически свободно' }}</span>
                    </div>
                    <div class="distributed-quota-grid">
                        <distributed-quota-bar label="Claude" :quota="worker.quotas.claude" :stale="worker.quotas.claude.stale"></distributed-quota-bar>
                        <distributed-quota-bar label="Codex" :quota="worker.quotas.codex" :stale="worker.quotas.codex.stale"></distributed-quota-bar>
                    </div>
                    <div class="distributed-worker-tasks" v-if="worker.currentTasks.length">
                        <div class="distributed-worker-tasks__title">Сейчас</div>
                        <button v-for="task in worker.currentTasks.slice(0, detailed ? 4 : 2)" :key="task.id" class="distributed-worker-task" @click="$emit('task', task)">
                            <span><strong>{{ task.project }}</strong> → {{ task.packageName }}</span>
                            <span>{{ modeLabel(task.mode) }} · {{ task.duration }}</span>
                            <div class="distributed-worker-task__progress"><i :style="{width: (Number.isFinite(task.progressPercent) ? task.progressPercent : 0) + '%'}"></i></div>
                            <b>{{ progressText(task) }}</b>
                        </button>
                    </div>
                    <div v-else class="distributed-worker-card__idle">{{ worker.status === 'offline' ? 'Текущих задач нет' : 'Свободен для новой задачи' }}</div>
                    <footer v-if="detailed" class="distributed-worker-card__footer">
                        <label class="distributed-switch" :class="{'is-disabled': worker.status === 'offline' || worker.readOnly}">
                            <input type="checkbox" :checked="worker.acceptsNewTasks" :disabled="worker.status === 'offline' || worker.readOnly" @change="$emit('toggle-intake', $event.target.checked)">
                            <span></span>Принимать новые задачи
                        </label>
                        <button class="btn btn-outline btn-sm" @click="$emit('detail', worker)">Подробнее</button>
                    </footer>
                </article>`,
        });
    }

    root.DistributedFeature = Object.freeze({ createManager, registerComponents, routeToTab, filterCompletedTasks, quotaReasonText, quotaAgeText, quotaWindows, constants: { TAB_DEFINITIONS, MODE_LABELS, PRIORITY_LABELS, STAGE_LABELS, QUOTA_REASON_TEXT } });
})(typeof window !== 'undefined' ? window : globalThis);
