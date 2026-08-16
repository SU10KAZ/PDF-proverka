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

    const MODE_LABELS = Object.freeze({ full_codex: 'Full Codex', hybrid: 'Гибрид Claude + Codex' });
    const PRIORITY_LABELS = Object.freeze({ critical: 'Критический', high: 'Высокий', normal: 'Обычный', low: 'Низкий' });
    const STAGE_LABELS = Object.freeze({ queued: 'В очереди', transfer: 'Передача', preparing: 'Подготовка', auditing: 'Проверка', collecting: 'Сбор результата', returning: 'Возврат результата', done: 'Готово', error: 'Ошибка' });
    const TASK_STAGE_ORDER = Object.freeze(['transfer', 'preparing', 'auditing', 'collecting', 'returning', 'done']);

    /** @param {string} path */
    function routeToTab(path) {
        const clean = String(path || '').replace(/\/+$/, '') || '/';
        if (clean === '/distributed') return 'overview';
        const match = clean.match(/^\/distributed\/(queue|tasks|workers|limits|diagnostics)$/);
        return match ? match[1] : null;
    }

    /** @param {number} value */
    function usageTone(value) {
        if (value >= 90) return 'danger';
        if (value >= 70) return 'warning';
        return 'normal';
    }

    /** @param {number} bytes */
    function formatBytes(bytes) {
        if (!Number.isFinite(bytes) || bytes <= 0) return '—';
        return `${(bytes / 1024 / 1024).toFixed(bytes >= 104857600 ? 0 : 1)} МБ`;
    }

    /** @param {any[]} rows @param {string} period @param {string=} customFrom @param {string=} customTo */
    function filterCompletedTasks(rows, period, customFrom, customTo) {
        let from = '2026-08-16';
        let to = '2026-08-16';
        if (period === '7d') from = '2026-08-10';
        if (period === '30d') from = '2026-07-18';
        if (period === 'custom') {
            from = customFrom || '0000-01-01';
            to = customTo || '9999-12-31';
        }
        return (rows || []).filter((task) => {
            const date = String(task.completedAtIso || '').slice(0, 10);
            return date && date >= from && date <= to;
        });
    }

    /** @param {{service?:any}=} options */
    function createManager(options = {}) {
        const VueRuntime = root.Vue;
        const { ref, computed, reactive } = VueRuntime;
        const service = options.service || root.DistributedData.createMockService();

        const activeTab = ref('overview');
        const taskSubtab = ref('active');
        const historyPeriod = ref('today');
        const historyFrom = ref('2026-08-01');
        const historyTo = ref('2026-08-16');
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
            if (!online.length) return { claude: 0, codex: 0, nextReset: '—', bestClaude: '—', bestCodex: '—' };
            const average = (provider) => Math.round(online.reduce((sum, item) => sum + item[provider].percentageRemaining, 0) / online.length);
            const best = (provider) => [...online].sort((a, b) => b[provider].percentageRemaining - a[provider].percentageRemaining)[0];
            const resetEntries = online.flatMap((item) => [
                { provider: 'Claude', resetAt: item.claude.resetAt, resetIn: item.claude.resetIn, workerName: item.workerName },
                { provider: 'Codex', resetAt: item.codex.resetAt, resetIn: item.codex.resetIn, workerName: item.workerName },
            ]).sort((a, b) => String(a.resetAt).localeCompare(String(b.resetAt)));
            return { claude: average('claude'), codex: average('codex'), nextReset: `${resetEntries[0].resetIn} · ${resetEntries[0].provider}, ${resetEntries[0].workerName}`, bestClaude: best('claude').workerName, bestCodex: best('codex').workerName };
        });
        const visibleCompletedTasks = computed(() => {
            return filterCompletedTasks(tasks.value.completed, historyPeriod.value, historyFrom.value, historyTo.value);
        });

        async function load(force = false) {
            if (loaded.value && !force) return;
            loading.value = true;
            error.value = '';
            try {
                const [overviewData, workersData, queueData, tasksData, limitsData, diagnosticsData] = await Promise.all([
                    service.getOverview(), service.getWorkers(), service.getQueue(), service.getTasks(), service.getProviderLimits(), service.getDiagnostics(),
                ]);
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
        /** @param {number} value */
        function progressStyle(value) { return { width: `${Math.min(100, Math.max(0, Number(value) || 0))}%` }; }
        /** @param {string} projectId @param {string} workerId */
        async function setAssignment(projectId, workerId) {
            assignmentByProject.value = { ...assignmentByProject.value, [projectId]: workerId };
            try {
                const project = await service.assignTask(projectId, workerId);
                const target = workerId === 'auto' ? 'автоматическое распределение' : `VPS ${workerName(workerId)}`;
                notify(`${project.project} → ${project.packageName}: выбрано ${target}`);
            } catch (actionError) {
                notify(String(actionError.message || actionError), 'error');
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
                notify(`${result.project} → ${result.packageName} добавлен в демо-очередь ${target}`);
                await refresh();
            } catch (actionError) { notify(String(actionError.message || actionError), 'error'); }
        }

        function openWhy() { modal.value = { type: 'why', recommendation: overview.value && overview.value.recommendation }; }
        function openAlternative() { modal.value = { type: 'alternative', project: recommendationProject.value, workerId: recommendationWorker.value && recommendationWorker.value.id }; }
        async function applyAlternative() {
            if (!modal.value || modal.value.type !== 'alternative') return;
            const chosenWorkerId = modal.value.workerId;
            const project = modal.value.project;
            await setAssignment(project.id, chosenWorkerId);
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
                notify(`${updated.project} → ${updated.packageName}: повтор запущен в демо-режиме`);
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
                notify(`${updated.project} → ${updated.packageName} переносится на VPS ${workerName(transferWorkerId.value)} (демо)`);
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
            if (state === 'current') return stage === 'auditing' ? `${task.progress}%` : 'сейчас';
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
            return [
                ['worker_id', diagnostic.workerId], ['instance_id', diagnostic.instanceId], ['transport', diagnostic.transport], ['grpc_stream', diagnostic.grpcStream], ['connection_id', diagnostic.connectionId], ['mTLS', diagnostic.mtls], ['heartbeat', diagnostic.heartbeat], ['Gateway target', diagnostic.gatewayTarget], ['source host', diagnostic.sourceHost], ['result host', diagnostic.resultHost], ['nginx', diagnostic.nginx], ['Agent status', diagnostic.agentStatus], ['Executor status', diagnostic.executorStatus], ['EventOutbox', `${diagnostic.eventOutbox.lastAckedSeq} / ${diagnostic.eventOutbox.lastWrittenSeq}; pending ${diagnostic.eventOutbox.pending}`], ['ResultAck', diagnostic.resultAck], ['worker version', diagnostic.workerVersion], ['runtime version', diagnostic.runtimeVersion], ['uptime', diagnostic.uptime], ['cert expiry', diagnostic.certExpiry],
            ];
        }

        return reactive({
            tabs: TAB_DEFINITIONS, activeTab, taskSubtab, historyPeriod, historyFrom, historyTo,
            loading, loaded, error, overview, workers, queue, tasks, limits, diagnostics,
            assignmentByProject, selectedTask, selectedWorker, selectedDiagnostic, modal, transferWorkerId, toast,
            recommendationProject, recommendationWorker, limitsSummary, visibleCompletedTasks,
            load, refresh, setTab, goToTab, notify, workerById, workerName,
            modeLabel, priorityLabel, stageLabel, progressStyle, usageTone, formatBytes,
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
            template: `
                <div class="distributed-quota" :class="'distributed-quota--' + (quota.status || 'unknown')">
                    <div class="distributed-quota__top">
                        <span class="distributed-quota__name">{{ label }}</span>
                        <strong>{{ quota.percentageRemaining }}% <small>осталось</small></strong>
                    </div>
                    <div class="distributed-progress distributed-progress--quota" role="progressbar" :aria-label="label + ': осталось ' + quota.percentageRemaining + '%'" :aria-valuenow="quota.percentageRemaining" aria-valuemin="0" aria-valuemax="100">
                        <span :style="{width: quota.percentageRemaining + '%'}"></span>
                    </div>
                    <div class="distributed-quota__reset">
                        <span>{{ stale ? 'Последние известные данные' : 'Сброс через ' + quota.resetIn }}</span>
                        <span v-if="quota.isEstimated" title="Провайдер передаёт приблизительное значение">≈ оценка</span>
                    </div>
                </div>`,
        });

        app.component('distributed-worker-card', {
            props: { worker: Object, detailed: Boolean },
            emits: ['detail', 'toggle-intake', 'task'],
            methods: {
                modeLabel(mode) { return MODE_LABELS[mode] || mode; },
                stageLabel(stage) { return STAGE_LABELS[stage] || stage; },
                usageTone,
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
                            <div><span>{{ metric.label }}</span><strong>{{ worker.status === 'offline' ? '—' : metric.value + '%' }}</strong></div>
                            <div class="distributed-progress" :class="'distributed-progress--' + usageTone(metric.value)"><span :style="{width: (worker.status === 'offline' ? 0 : metric.value) + '%'}"></span></div>
                        </div>
                        <div class="distributed-resource">
                            <div><span>VRAM</span><strong>{{ worker.status === 'offline' ? '—' : worker.resources.vramUsedGb + ' / ' + worker.resources.vramTotalGb + ' ГБ' }}</strong></div>
                            <div class="distributed-progress" :class="'distributed-progress--' + usageTone(worker.resources.vramUsedGb / worker.resources.vramTotalGb * 100)"><span :style="{width: (worker.status === 'offline' ? 0 : worker.resources.vramUsedGb / worker.resources.vramTotalGb * 100) + '%'}"></span></div>
                        </div>
                    </div>
                    <div class="distributed-worker-card__slots">
                        <span>Слоты <b>{{ worker.slots.used }} / {{ worker.slots.total }}</b></span>
                        <span :class="{'is-free': worker.slots.total - worker.slots.used > 0}">{{ worker.status === 'offline' ? 'недоступен' : worker.slots.total - worker.slots.used + ' свободно' }}</span>
                    </div>
                    <div class="distributed-quota-grid">
                        <distributed-quota-bar label="Claude" :quota="worker.quotas.claude" :stale="worker.quotaDataStale"></distributed-quota-bar>
                        <distributed-quota-bar label="Codex" :quota="worker.quotas.codex" :stale="worker.quotaDataStale"></distributed-quota-bar>
                    </div>
                    <div class="distributed-worker-tasks" v-if="worker.currentTasks.length">
                        <div class="distributed-worker-tasks__title">Сейчас</div>
                        <button v-for="task in worker.currentTasks.slice(0, detailed ? 4 : 2)" :key="task.id" class="distributed-worker-task" @click="$emit('task', task)">
                            <span><strong>{{ task.project }}</strong> → {{ task.packageName }}</span>
                            <span>{{ modeLabel(task.mode) }} · {{ task.duration }}</span>
                            <div class="distributed-worker-task__progress"><i :style="{width: task.progress + '%'}"></i></div>
                            <b>{{ task.progress }}%</b>
                        </button>
                    </div>
                    <div v-else class="distributed-worker-card__idle">{{ worker.status === 'offline' ? 'Текущих задач нет' : 'Свободен для новой задачи' }}</div>
                    <footer v-if="detailed" class="distributed-worker-card__footer">
                        <label class="distributed-switch" :class="{'is-disabled': worker.status === 'offline'}">
                            <input type="checkbox" :checked="worker.acceptsNewTasks" :disabled="worker.status === 'offline'" @change="$emit('toggle-intake', $event.target.checked)">
                            <span></span>Принимать новые задачи
                        </label>
                        <button class="btn btn-outline btn-sm" @click="$emit('detail', worker)">Подробнее</button>
                    </footer>
                </article>`,
        });
    }

    root.DistributedFeature = Object.freeze({ createManager, registerComponents, routeToTab, filterCompletedTasks, constants: { TAB_DEFINITIONS, MODE_LABELS, PRIORITY_LABELS, STAGE_LABELS } });
})(typeof window !== 'undefined' ? window : globalThis);
