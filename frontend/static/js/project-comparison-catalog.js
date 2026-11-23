(function (root) {
    'use strict';
    // Compact mirror of the configured source rows, in their displayed order.
    // Launch requests go through the existing parent comparison dialog.
    const API = '/api/project-comparison/catalog';
    const SOURCE_LABEL = {LIVE_RUN: 'прогон V3', SEALED_SNAPSHOT: 'запечатанный снимок'};


    function date(value) {
        if (!value) return '—';
        const d = new Date(value);
        return Number.isNaN(d.getTime()) ? '—' : d.toLocaleDateString('ru-RU');
    }
    function model(entry) {
        const e = entry.engine || {};
        return [e.comparison_mode_label, e.model_display || e.model || '—', e.reasoning].filter(Boolean).join(' · ');
    }
    function effort(entry) {
        const m = entry.comparison_metrics;
        if (!m) return '—';
        const parts = [];
        if (Number.isFinite(m.model_calls)) parts.push(m.model_calls + ' вызовов');
        const usage = m.usage_total;
        if (usage && usage.calls_with_usage > 0) {
            parts.push(Number(usage.input_tokens || 0).toLocaleString('ru-RU') + ' входных токенов');
            parts.push(Number(usage.output_tokens || 0).toLocaleString('ru-RU') + ' выходных токенов');
            if (usage.calls_without_usage) parts.push('расход известен не для всех вызовов');
        }
        if (Number.isFinite(m.elapsed_seconds)) parts.push(Math.ceil(m.elapsed_seconds / 60) + ' мин');
        if (m.resumed) parts.push('с продолжением; учитывать предыдущие запуски');
        return parts.join(' · ') || '—';
    }
    function isRunning(entry) {
        const state = entry?.selection_pair?.comparisonRun?.state;
        return state?.status === 'RUNNING' && state.orphaned_run !== true;
    }
    function pairEntries(results, rows, objectId, objectName, sessionId) {
        const catalog = (results || []).filter(e => !objectId || e.object_id === objectId);
        const entries = [];
        for (const row of rows || []) {
            if (!row.left || !row.right) continue;
            const matches = catalog.filter(e =>
                ((row.pair?.id && e.open?.session_id === sessionId && e.open?.pair_id === row.pair.id)
                    || (row.left.pdf_path && row.right.pdf_path
                        && e.documents?.old?.pdf_path === row.left.pdf_path
                        && e.documents?.new?.pdf_path === row.right.pdf_path)));
            const title = row.left?.document_code || row.left?.filename || row.right?.document_code || row.right?.filename;
            const leftCount = row.group ? row.leftMembers?.length || 0 : 0;
            const rightCount = row.group ? row.members?.length || 0 : 0;
            // Склеенную левую сторону называет документ справа (или якорь строки при N ↔ M).
            const rowTitle = leftCount && rightCount
                ? row.group.anchor?.document_code + ' · сборки ' + leftCount + ' ↔ ' + rightCount
                : leftCount
                    ? (row.right?.document_code || row.right?.filename) + ' · сборка слева из ' + leftCount
                    : title + (rightCount ? ' · сборка из ' + rightCount : '');
            // Multiple completed runs describe one configured pair, not extra
            // rows. Prefer its current run, then the current/primary result.
            const runId = row.comparisonRun?.state?.run_id;
            const rank = e => [Boolean(runId && e.run_id === runId),
                Boolean(row.pair?.id && e.open?.session_id === sessionId && e.open?.pair_id === row.pair.id),
                Boolean(e.is_current), Boolean(e.is_primary)];
            matches.sort((a, b) => {
                const ar = rank(a), br = rank(b);
                for (let i = 0; i < ar.length; i++) if (ar[i] !== br[i]) return Number(br[i]) - Number(ar[i]);
                return String(b.frozen_at || '').localeCompare(String(a.frozen_at || ''))
                    || String(a.catalog_entry_id).localeCompare(String(b.catalog_entry_id));
            });
            if (matches.length) {
                entries.push({...matches[0], title: rowTitle, selection_pair: row});
                continue;
            }
            entries.push({catalog_entry_id: 'pair:' + JSON.stringify([row.left?.pdf_path, row.right?.pdf_path, row.index]),
                pending_pair: row, selection_pair: row, object_id: objectId, object_name: objectName,
                title: rowTitle,
                documents: {old: row.left || {}, new: row.right || {}}, engine: {}, counts: {}});
        }
        return entries;
    }

    function progressLabel(state) {
        if (!state) return '—';
        const pipeline = root.ProjectChangeView?.comparisonPipeline(state);
        if (!pipeline) return '—';
        const rows = pipeline.stages;
        const row = rows.find(s => s.status === 'RUNNING')
            || rows.find(s => ['FAILED', 'INTERRUPTED', 'CANCELLED', 'PAUSED'].includes(s.status))
            || (state.consolidation ? rows.find(s => s.id === 'RESULT') : null);
        if (!row) return pipeline.title;
        const title = row.id === 'RESULT' && state.consolidation ? 'Сведение дублей' : row.title;
        return `${row.number}/7 · ${title} · ${row.status === 'RUNNING' ? row.counter || row.label : row.label}`;
    }

    root.ProjectComparisonCatalog = {
        date, model, effort, pairEntries, isRunning, progressLabel,
        register(app) {
            app.component('project-comparison-catalog', {
                props: {activeEntryId: {type: String, default: ''}, opening: Boolean, error: {type: String, default: ''},
                    // The source table owns the row list; results only enrich those rows.
                    objectId: {type: String, default: ''}, objectName: {type: String, default: ''},
                    // Версия сравнения объекта (cvNNN): каталог и ход выполнения — только её.
                    comparisonVersion: {type: String, default: ''},
                    sessionId: {type: String, default: ''}, pairRows: {type: Array, default: () => []},
                    pairsLoading: Boolean, canStart: {type: Function, default: () => false},
                    pairStatus: {type: Function, default: () => 'Не запускалось'},
                    rowChecked: {type: Function, default: () => false},
                    selectionDisabled: Boolean, allChecked: Boolean,
                    // «Отработано»: отметка пары (кто и когда) или null; markingWorked — идёт отметка.
                    workedMark: {type: Function, default: () => null}, markingWorked: Boolean},
                emits: ['open', 'open-pair', 'start', 'select-pair', 'select-all', 'mark-worked'],
                setup(props, {emit}) {
                    const {ref, computed, onMounted, onUnmounted, watch} = root.Vue;
                    const catalog = ref(null);
                    const loading = ref(true);
                    const loadError = ref('');
                    const overviewLoading = computed(() => !!props.pairsLoading && !props.pairRows?.length);
                    const entries = computed(() => pairEntries(catalog.value?.entries, props.pairRows,
                        props.objectId, props.objectName, props.sessionId));
                    const progress = ref([]);
                    const progressError = ref(false);
                    let progressTimer = null, progressToken = 0, disposed = false;
                    function versioned(url) {
                        return /^cv\d{3}$/.test(props.comparisonVersion || '')
                            ? url + '&comparison_version=' + encodeURIComponent(props.comparisonVersion) : url;
                    }
                    // Ход выполнения: часто — только пока что-то идёт, на скрытой вкладке
                    // не опрашиваем вовсе (десяток вкладок занимал всё ядро backend, 23.11.2026).
                    const PROGRESS_ACTIVE_MS = 5000, PROGRESS_IDLE_MS = 60000;
                    const ACTIVE = new Set(['RUNNING', 'QUEUED', 'PENDING', 'STARTING']);
                    function progressActive() {
                        return progress.value.some(p => ACTIVE.has(p.state?.status)
                                || ACTIVE.has(p.state?.consolidation?.status))
                            || entries.value.some(isRunning);
                    }
                    function pageHidden() {
                        return typeof document !== 'undefined' && document.visibilityState === 'hidden';
                    }
                    let progressInFlight = false;
                    async function pollProgress() {
                        clearTimeout(progressTimer); progressTimer = null;
                        const token = ++progressToken;
                        progressInFlight = false;
                        if (!props.objectId || disposed) return;
                        // Скрытая вкладка: следующий опрос — по возврату на неё (visibilitychange).
                        if (pageHidden()) return;
                        progressInFlight = true;
                        try {
                            const response = await fetch(versioned(API + '/progress?object_id=' + encodeURIComponent(props.objectId)));
                            if (!response.ok) throw new Error('progress unavailable');
                            const data = await response.json();
                            if (data.schema !== 'project-comparison-progress/1' || !Array.isArray(data.items)) throw new Error('invalid progress');
                            if (token !== progressToken || disposed) return;
                            progress.value = data.items; progressError.value = false;
                        } catch (_) {
                            if (token === progressToken) progressError.value = true;
                        } finally {
                            if (token === progressToken) progressInFlight = false;
                            if (token === progressToken && !disposed && !pageHidden())
                                progressTimer = setTimeout(pollProgress, progressActive() ? PROGRESS_ACTIVE_MS : PROGRESS_IDLE_MS);
                        }
                    }
                    function onVisibility() {
                        if (!pageHidden() && !disposed && !progressInFlight) pollProgress();
                    }
                    if (typeof document !== 'undefined' && document.addEventListener)
                        document.addEventListener('visibilitychange', onVisibility);
                    function rowProgress(entry) {
                        if (progressError.value) return 'Обновление недоступно';
                        const sid = entry.open?.session_id || props.sessionId;
                        const pid = entry.open?.pair_id || entry.selection_pair?.pair?.id;
                        const current = progress.value.find(p => p.session_id === sid && p.pair_id === pid);
                        return progressLabel(current?.state || entry.selection_pair?.comparisonRun?.state);
                    }
                    let loadId = 0;
                    async function load() {
                        const requestId = ++loadId;
                        loading.value = true; loadError.value = '';
                        try {
                            const url = props.objectId ? versioned(API + '?object_id=' + encodeURIComponent(props.objectId)) : API;
                            const response = await fetch(url);
                            const data = await response.json().catch(() => ({}));
                            if (requestId !== loadId) return;
                            if (!response.ok) throw new Error(data.detail || ('HTTP ' + response.status));
                            if (data.schema !== 'project-comparison-catalog/1' || !Array.isArray(data.entries))
                                throw new Error('Неверный контракт каталога.');
                            catalog.value = data;
                        } catch (error) {
                            if (requestId !== loadId) return;
                            loadError.value = 'Каталог недоступен: ' + String(error.message || error);
                        } finally { if (requestId === loadId) loading.value = false; }
                    }
                    function startDisabled(entry) {
                        return loading.value || !!loadError.value || props.pairsLoading || props.opening
                            || !props.canStart?.(entry.pending_pair);
                    }
                    function start(entry) {
                        if (entry?.pending_pair && !startDisabled(entry)) emit('start', entry.pending_pair);
                    }
                    function selectPair(entry, checked) {
                        if (entry.selection_pair && !props.selectionDisabled)
                            emit('select-pair', entry.selection_pair, checked);
                    }
                    function selectAll(checked) {
                        if (!props.selectionDisabled) emit('select-all', checked);
                    }
                    onMounted(load);
                    onMounted(pollProgress);
                    onUnmounted?.(() => { disposed = true; progressToken++; loadId++; clearTimeout(progressTimer);
                        if (typeof document !== 'undefined' && document.removeEventListener)
                            document.removeEventListener('visibilitychange', onVisibility);
                    });
                    watch([() => props.objectId, () => props.comparisonVersion], () => { catalog.value = null; load(); });
                    watch([() => props.objectId, () => props.comparisonVersion], () => { progress.value = []; progressError.value = false; pollProgress(); });
                    // Запуск пары из таблицы: не ждать редкого опроса, сразу перейти на частый.
                    watch(() => entries.value.some(isRunning), running => { if (running && !progressInFlight) pollProgress(); });
                    // viewer (portal_auth.js): только просмотр — без запуска и выбора пар для анализа.
                    const canOperate = !(typeof window !== 'undefined' && window.portalCan && !window.portalCan.operate());
                    // «Отработано» ставят и инженеры-viewer с просмотром раздела.
                    const canMarkWorked = !(typeof window !== 'undefined' && window.portalCan
                        && !(window.portalCan.markWorked ? window.portalCan.markWorked() : window.portalCan.operate()));
                    function workedTitle(row) {
                        const mark = props.workedMark(row);
                        if (!mark) return '';
                        const at = mark.at ? new Date(mark.at) : null;
                        return 'Отработано: ' + (mark.by || 'инженер')
                            + (at && !Number.isNaN(at.getTime()) ? ', ' + at.toLocaleString('ru-RU') : '');
                    }
                    return {rowProgress, catalog, entries, loading, overviewLoading, loadError, load, date, model, effort, isRunning, start, startDisabled, selectPair, selectAll, canOperate, canMarkWorked, workedTitle,
                        sourceLabel: s => SOURCE_LABEL[s] || s,
                        open: (entry, options) => (options ? emit('open', entry, options) : emit('open', entry))};
                },
                template: `
                <section class="pc-catalog" id="pc-catalog" aria-label="Сравнения проектов">
                    <div v-if="loadError" class="pc-notice" role="alert">{{ loadError }}
                        <button type="button" class="pc-link" @click="load">Повторить</button></div>
                    <div v-if="error" class="pc-notice" role="alert">{{ error }}</div>
                    <p v-if="overviewLoading" class="pc-catalog__empty" role="status">Загрузка сравнений…</p>
                    <p v-else-if="catalog && !pairsLoading && !entries.length" class="pc-catalog__empty">В этом объекте пока нет пар для сравнения.</p>
                    <div v-if="entries.length" class="pc-catalog__scroll">
                    <table class="pc-catalog__table">
                        <colgroup><col class="pc-catalog__col-select"><col class="pc-catalog__col-project">
                            <col class="pc-catalog__col-model"><col class="pc-catalog__col-date">
                            <col class="pc-catalog__col-count"><col class="pc-catalog__col-review"><col class="pc-catalog__col-status"><col class="pc-catalog__col-progress"><col class="pc-catalog__col-actions"></colgroup>
                        <thead><tr><th><input v-if="canOperate" type="checkbox" aria-label="Выбрать все пары для анализа"
                            :checked="allChecked" :disabled="selectionDisabled || !entries.some(e => e.selection_pair)"
                            @change="selectAll($event.target.checked)"></th><th>Проект / раздел</th><th>Метод и модель</th><th>Дата</th>
                            <th class="num">Изменения</th><th class="num">Требуют проверки</th><th>Статус</th><th>Ход выполнения</th><th></th></tr></thead>
                        <tbody>
                        <tr v-for="entry in entries" :key="entry.catalog_entry_id" :data-entry-id="entry.catalog_entry_id"
                            :class="{'is-active': entry.catalog_entry_id === activeEntryId}">
                            <td><input v-if="canOperate && entry.selection_pair" type="checkbox"
                                :aria-label="'Выбрать для анализа: ' + entry.title"
                                :checked="rowChecked(entry.selection_pair)" :disabled="selectionDisabled"
                                @change="selectPair(entry, $event.target.checked)"></td>
                            <td class="pc-catalog__project" :title="entry.title"><b>{{ entry.title }}</b></td>
                            <td class="pc-catalog__model" :title="model(entry)">{{ entry.pending_pair ? '—' : model(entry) }}</td>
                            <td :title="entry.frozen_at_source">{{ date(entry.frozen_at) }}</td>
                            <td class="num pc-catalog__count">{{ entry.counts.projectchanges ?? '—' }}</td>
                            <td class="num">{{ entry.counts.unresolved_hints ?? '—' }}</td>
                            <td class="pc-catalog__state"><span class="pc-catalog__state-line"
                                :title="pairStatus(entry.selection_pair)">{{ pairStatus(entry.selection_pair) }}</span></td>
                            <td class="pc-catalog__progress" :title="rowProgress(entry)">{{ rowProgress(entry) }}</td>
                            <td v-if="entry.pending_pair" class="pc-catalog__actions">
                                <span v-if="isRunning(entry)" class="pc-catalog__running" role="status"
                                    aria-label="Сравнение выполняется" title="По этой паре выполняется сравнение">
                                    <span class="pc-catalog__running-spinner" aria-hidden="true"></span>
                                </span>
                                <button v-if="entry.pending_pair.comparisonRun" type="button" class="btn btn-sm btn-primary"
                                    :disabled="opening || pairsLoading" @click="$emit('open-pair', entry.pending_pair)">Открыть</button>
                                <button v-else-if="canOperate" type="button" class="btn btn-sm btn-primary" :disabled="startDisabled(entry)"
                                    @click="start(entry)">Запустить сравнение</button>
                                <span v-if="workedMark(entry.selection_pair)" class="pc-catalog__worked"
                                    :title="workedTitle(entry.selection_pair)">Отработано · {{ workedMark(entry.selection_pair).by }}</span>
                                <button v-else-if="canMarkWorked" type="button" class="btn btn-sm btn-secondary pc-catalog__worked-button"
                                    :disabled="markingWorked" @click="$emit('mark-worked', entry.selection_pair)">Отработано</button>
                            </td>
                            <td v-else class="pc-catalog__actions">
                                <span v-if="isRunning(entry)" class="pc-catalog__running" role="status"
                                    aria-label="Сравнение выполняется" title="По этой паре выполняется сравнение">
                                    <span class="pc-catalog__running-spinner" aria-hidden="true"></span>
                                </span>
                                <button type="button" class="btn btn-sm btn-primary" :disabled="opening || !entry.open.available"
                                    :title="entry.open.available ? 'Открыть в сравнении проекта' : 'В ленте пары сейчас показан другой прогон'"
                                    @click="open(entry)">Открыть</button>
                                <span v-if="workedMark(entry.selection_pair)" class="pc-catalog__worked"
                                    :title="workedTitle(entry.selection_pair)">Отработано · {{ workedMark(entry.selection_pair).by }}</span>
                                <button v-else-if="canMarkWorked" type="button" class="btn btn-sm btn-secondary pc-catalog__worked-button"
                                    :disabled="markingWorked" @click="$emit('mark-worked', entry.selection_pair)">Отработано</button>
                            </td>
                        </tr>
                        </tbody>
                    </table>
                    </div>
                </section>`,
            });
        },
    };
}(typeof globalThis !== 'undefined' ? globalThis : this));
