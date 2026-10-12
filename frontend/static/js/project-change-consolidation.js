(function (root, factory) {
    const api = factory(root);
    if (typeof module === 'object' && module.exports) module.exports = api;
    root.ProjectChangeConsolidation = api;
}(typeof globalThis !== 'undefined' ? globalThis : this, function (root) {
    'use strict';
    // Read-only view of COMPLETED shadow consolidations of the opened pair, opened automatically.
    // Итоговые come only from the backend (bound by source run + sha256); the browser never builds them.
    const CHANNELS = {ENGINEERING_CHANGE: 'Инженерное изменение', DOCUMENTARY_CHANGE: 'Документальная правка',
        REVIEW: 'Нужна проверка инженера', NOT_ASSESSED: 'Не оценено Консолидатором'};
    const SCOPES = {SYSTEM_WIDE: 'Вся система / весь объект', MULTI_BUILDING: 'Несколько зданий',
        BUILDING_SPECIFIC: 'Одно здание', LOCAL_ELEMENT: 'Локальный элемент',
        DESIGN_BASIS: 'Расчётные исходные данные', NOT_ASSESSED: 'Охват не оценивался'};
    const BASES = {EXPLICIT_GENERAL_STATEMENT: 'общее требование в тексте', ALL_LOCATION_UNITS_COVERED: 'все здания объекта',
        UNION_OF_MANIFESTATIONS: 'по проявлениям', SINGLE_MANIFESTATION: 'одно проявление',
        CALCULATION_BASIS: 'расчётная таблица'};
    const PARAM_STATUS = {CONSISTENT: 'одинаково', LOCATION_VARIANT: 'различается по местам', CONFLICT: 'противоречие'};
    const DECISIONS = {KEEP_SEPARATE: 'Отдельное событие', UNCERTAIN: 'Возможно, одно событие с другими — не объединено',
        FALLBACK: 'Решение Консолидатора не прошло проверку — показана исходная карточка',
        CLUSTER_FALLBACK: 'Решение Консолидатора не прошло проверку — показана исходная карточка',
        PROVIDER_FAILED: 'Консолидатор не ответил — показана исходная карточка',
        NOT_SENT: 'Кластер не отправлялся — показана исходная карточка',
        NOT_CLUSTERED: 'Кандидатов на объединение не найдено'};
    const CLAIMS = {CONTAINS_UNRESOLVED_CONFLICT: 'Содержит нерешённое противоречие источников',
        PARTIALLY_SUPPORTED_BY_MEMBERS: 'Утверждение подтверждено не всеми исходными карточками'};
    const arr = v => Array.isArray(v) ? v : [];
    const str = v => typeof v === 'string' ? v : '';

    // The consolidation of the displayed run first, then the latest completed one.
    function pick(list) {
        const items = arr(list).filter(x => x && str(x.source_run_id) && str(x.consolidator_run_id));
        const ordered = [...items].sort((a, b) => str(b.completed_at).localeCompare(str(a.completed_at)));
        return ordered.find(x => x.source_is_current_run) || ordered[0] || null;
    }
    function key(entry) { return entry ? entry.source_run_id + '/' + entry.consolidator_run_id : ''; }
    function summary(view) {
        const items = arr(view?.consolidated);
        const merged = items.filter(x => x.kind === 'CONSOLIDATED');
        return {original: arr(view?.original).length, consolidated: items.length, merged: merged.length,
            absorbed: merged.reduce((n, x) => n + arr(x.lineage?.members).length, 0),
            review: items.filter(x => x.channel === 'REVIEW').length,
            documentary: items.filter(x => x.channel === 'DOCUMENTARY_CHANGE').length,
            conflicts: items.filter(x => arr(x.conflicts).length).length};
    }
    // Same navigation target as ProjectChangeView.destination: both sides of one event of this pair.
    function destination(card, selected) {
        const evidence = arr(card?.evidence);
        const e = selected || evidence.find(x => x.page && x.pair_id);
        if (!e?.pair_id || !e.page || !e.document?.id || !e.document?.version || !['OLD', 'NEW'].includes(e.side)) return null;
        const other = evidence.find(x => x.side && x.side !== e.side && x.pair_id === e.pair_id && x.page);
        return {change_id: str(card.id), pair_id: e.pair_id,
            OLD: e.side === 'OLD' ? e : other?.side === 'OLD' ? other : null,
            NEW: e.side === 'NEW' ? e : other?.side === 'NEW' ? other : null};
    }
    function title(card) { return str(card?.title) || str(card?.summary) || 'Изменение'; }

    // Parameter provenance comes from its source cards, not every member of the merged event.
    function parameterSources(card, parameter, originals) {
        const members = new Map(arr(originals).map(c => [c.id, c]));
        const sourceCards = card.kind === 'CONSOLIDATED'
            ? [...new Set(arr(parameter.values).map(v => v.card_id))].map(id => members.get(id)).filter(Boolean)
            : [card];
        return Object.fromEntries(['OLD', 'NEW'].map(side => {
            const groups = new Map();
            for (const source of sourceCards) for (const e of arr(source.evidence)) {
                if (e.side !== side) continue;
                // Assemblies carry the original cipher and the page within that source PDF.
                const origin = e.origin;
                const doc = origin ? origin.source : e.document;
                const cipher = str(origin ? doc?.document_code : doc?.label) || '—';
                const page = Number(origin ? origin.page?.source_page : e.page);
                const sourceType = ['TEXT', 'TABLE', 'GRAPHIC'].includes(e.source_type) ? e.source_type : '—';
                const key = JSON.stringify([cipher, doc?.version_id || doc?.version, doc?.pdf_path || doc?.id, sourceType]);
                if (!groups.has(key)) groups.set(key, {cipher, sourceType, pages: new Set()});
                if (Number.isInteger(page) && page > 0) groups.get(key).pages.add(page);
            }
            return [side, [...groups.values()].map(g => ({cipher: g.cipher, sourceType: g.sourceType,
                pages: [...g.pages].sort((a, b) => a - b).join(', ') || '—'}))];
        }));
    }

    // Table rows for a card's parameters. A consolidated parameter whose value
    // differs by place has no single old/new: it becomes a group row followed by
    // one row per place, each with its own values and its own member's sources.
    function parameterTableRows(card, originals) {
        const rows = [];
        for (const p of arr(card.parameters)) {
            const values = arr(p.values);
            if (card.kind === 'CONSOLIDATED' && p.status !== 'CONSISTENT' && values.length) {
                rows.push({...p, row: 'group', sources: {OLD: [], NEW: []}});
                for (const v of values) rows.push({
                    row: 'place', name: str(v.location) || str(v.card_id) || '—',
                    old: v.old_value, new: v.new_value, unit: v.unit,
                    sources: parameterSources(card, {values: [v]}, originals)});
            } else {
                rows.push({...p, row: 'param', sources: parameterSources(card, p, originals)});
            }
        }
        return rows;
    }

    function register(app) {
        app.component('project-change-consolidation', {
            props: {sessionId: {type: String, default: ''}, pairId: {type: String, default: ''}},
            emits: ['panel', 'open-evidence'],
            setup(props, {emit}) {
                const {ref, computed, watch} = root.Vue;
                const list = ref([]);
                const selectedKey = ref('');
                const listing = ref(true);
                const view = ref(null);
                const error = ref('');
                const loading = ref(false);
                const expandedId = ref('');
                const failedImages = ref({});
                let listToken = 0;
                let viewToken = 0;
                const base = computed(() => '/api/stage-comparison/sessions/' + encodeURIComponent(props.sessionId)
                    + '/pairs/' + encodeURIComponent(props.pairId) + '/consolidated');
                const selected = computed(() => list.value.find(x => key(x) === selectedKey.value) || null);
                const ownList = computed(() => listing.value || Boolean(selected.value));
                const cards = computed(() => arr(view.value?.consolidated));
                const counts = computed(() => summary(view.value));
                // Excel «Итоговые · Карточки ИТ · Краткая сводка»: разбор по корзинам загружает инженер.
                const reportBase = computed(() => selected.value ? base.value + '/' + encodeURIComponent(selected.value.source_run_id)
                    + '/' + encodeURIComponent(selected.value.consolidator_run_id) : '');
                const analysis = ref(null);
                const uploading = ref(false);
                const uploadMessage = ref('');
                const uploadErrors = ref([]);
                async function loadAnalysis() {
                    const url = reportBase.value;
                    analysis.value = null; uploadMessage.value = ''; uploadErrors.value = [];
                    if (!url) return;
                    try {
                        const response = await fetch(url + '/it-analysis');
                        const data = response.ok ? await response.json() : null;
                        if (url === reportBase.value && data?.schema === 'projectchange-it-analysis-status/1') analysis.value = data;
                    } catch (_) { /* статус разбора не обязателен для просмотра */ }
                }
                async function uploadAnalysis(event) {
                    const input = event.target;
                    const file = input.files && input.files[0];
                    if (!file || !reportBase.value) return;
                    uploading.value = true; uploadMessage.value = ''; uploadErrors.value = [];
                    try {
                        const form = new root.FormData();
                        form.append('file', file);
                        const response = await fetch(reportBase.value + '/it-analysis', {method: 'POST', body: form});
                        const data = await response.json().catch(() => null);
                        if (!response.ok) {
                            uploadErrors.value = arr(data?.detail?.errors);
                            uploadMessage.value = str(data?.detail?.message) || str(data?.detail) || 'Разбор не принят.';
                            return;
                        }
                        analysis.value = data;
                        uploadMessage.value = 'Разбор загружен: ' + data.rows + ' карточек. «Классический отчёт» выдаст его вместе с «Итоговыми».';
                    } catch (e) {
                        uploadMessage.value = 'Не удалось загрузить разбор: ' + String(e.message || e);
                    } finally { uploading.value = false; input.value = ''; }
                }
                // Classic export always downloads existing data; the AI report may start a model run.
                const modelRun = ref(null);
                const starting = ref(false);
                const now = ref(Date.now());
                let pollTimer = null;
                let downloadWhenDone = '';
                let disposed = false;
                const running = computed(() => ['QUEUED', 'RUNNING'].includes(modelRun.value?.status));
                const ready = computed(() => Boolean(analysis.value?.available) && !arr(analysis.value?.failed_ids).length);
                // kind 'zip' — «Классический отчёт»: Excel + папки «Было»/«Стало» с PDF и MD;
                // the AI report flow keeps downloading the bare Excel.
                function download(kind = 'xlsx') {
                    const url = reportBase.value + (kind === 'zip' ? '/report.zip' : '/report.xlsx');
                    if (root.document?.createElement) {
                        const a = root.document.createElement('a');
                        a.href = url; a.download = ''; root.document.body.appendChild(a); a.click(); a.remove();
                    } else if (root.location) { root.location.href = url; }
                }
                function stopPolling() { if (pollTimer) { root.clearTimeout(pollTimer); pollTimer = null; } }
                async function pollRun() {
                    stopPolling();
                    const url = reportBase.value;
                    if (!url || disposed) return;
                    try {
                        const response = await fetch(url + '/it-analysis/model-run');
                        if (!response.ok) throw new Error('Не удалось получить состояние разбора');
                        const data = await response.json();
                        if (disposed || url !== reportBase.value || !data) return;
                        modelRun.value = data; now.value = Date.now();
                        if (['QUEUED', 'RUNNING'].includes(data.status)) { pollTimer = root.setTimeout(pollRun, 5000); return; }
                        await loadAnalysis();
                        if (disposed || url !== reportBase.value) return;
                        if (downloadWhenDone === url && ['COMPLETED', 'PARTIAL'].includes(data.status)
                            && analysis.value?.available) download();
                        downloadWhenDone = '';
                    } catch (_) {
                        if (!disposed && url === reportBase.value) pollTimer = root.setTimeout(pollRun, 10000);
                    }
                }
                async function startRun(force) {
                    if (starting.value || running.value || !reportBase.value) return false;
                    const url = reportBase.value;
                    starting.value = true;
                    uploadMessage.value = ''; uploadErrors.value = [];
                    try {
                        const response = await fetch(url + '/it-analysis/model-run', {method: 'POST',
                            headers: {'Content-Type': 'application/json'}, body: JSON.stringify({force: Boolean(force)})});
                        const data = await response.json().catch(() => null);
                        if (disposed || url !== reportBase.value) return false;
                        if (!response.ok) { uploadMessage.value = str(data?.detail?.message) || 'Разбор моделью не запущен.'; return false; }
                        modelRun.value = data; downloadWhenDone = url;
                        stopPolling();
                        pollTimer = root.setTimeout(pollRun, 3000);
                        return true;
                    } catch (_) {
                        if (!disposed && url === reportBase.value) uploadMessage.value = 'Не удалось запустить разбор: ошибка связи.';
                        return false;
                    } finally { starting.value = false; }
                }
                async function makeReport() {
                    if (running.value || starting.value) return;
                    if (ready.value) { download(); return; }
                    await startRun(false);
                }
                async function recompute() {
                    const manual = analysis.value?.source === 'manual';
                    const question = manual ? 'Загружен ручной разбор инженера. Заменить его разбором модели? Прежний уйдёт в историю.'
                        : 'Пересчитать весь разбор моделью заново (около 30–40 минут)?';
                    if (root.confirm && !root.confirm(question)) return;
                    await startRun(true);
                }
                async function cancelRun() {
                    await fetch(reportBase.value + '/it-analysis/model-run/cancel', {method: 'POST'}).catch(() => null);
                    downloadWhenDone = '';
                    pollRun();
                }
                const elapsed = computed(() => {
                    const t = Date.parse(modelRun.value?.started_at || '');
                    return Number.isFinite(t) ? Math.max(0, Math.round((now.value - t) / 60000)) : 0;
                });
                watch(reportBase, () => { stopPolling(); downloadWhenDone = ''; modelRun.value = null; loadAnalysis(); pollRun(); }, {immediate: true});
                if (root.Vue.getCurrentInstance?.()) root.Vue.onBeforeUnmount(() => {
                    disposed = true; downloadWhenDone = ''; stopPolling();
                });
                watch(ownList, value => emit('panel', Boolean(value)), {immediate: true});
                async function loadList() {
                    const my = ++listToken;
                    viewToken++; loading.value = false; listing.value = true;
                    list.value = []; view.value = null; error.value = ''; selectedKey.value = '';
                    try {
                        if (!props.sessionId || !props.pairId) return;
                        const response = await fetch(base.value);
                        if (my !== listToken) return;
                        if (!response.ok) return;
                        const data = await response.json();
                        if (my !== listToken || data?.schema !== 'projectchange-consolidations/1') return;
                        list.value = arr(data.consolidations);
                        selectedKey.value = key(pick(list.value));
                    } catch (_) { /* no consolidation: keep the ordinary comparison available */ }
                    finally { if (my === listToken) listing.value = false; }
                }
                async function loadView() {
                    const entry = selected.value;
                    if (!entry) return false;
                    if (view.value && view.value.source_run_id === entry.source_run_id
                            && view.value.consolidator_run_id === entry.consolidator_run_id) return true;
                    const my = ++viewToken;
                    loading.value = true; error.value = '';
                    try {
                        const response = await fetch(base.value + '/' + encodeURIComponent(entry.source_run_id)
                            + '/' + encodeURIComponent(entry.consolidator_run_id));
                        const data = await response.json();
                        if (my !== viewToken) return false;
                        if (!response.ok || data?.schema !== 'projectchange-consolidated-view/1'
                                || data.source_run_id !== entry.source_run_id
                                || data.source_result_sha256 !== entry.source_result_sha256
                                || data.shadow_result_sha256 !== entry.shadow_result_sha256)
                            throw new Error('Итоговый результат недоступен.');
                        view.value = data;
                        return true;
                    } catch (e) {
                        if (my === viewToken) { error.value = String(e.message || e); view.value = null; }
                        return false;
                    } finally { if (my === viewToken) loading.value = false; }
                }
                watch(() => [props.sessionId, props.pairId], loadList, {immediate: true});
                watch(selectedKey, () => { view.value = null; expandedId.value = ''; loadView(); });
                function toggle(c) { expandedId.value = expandedId.value === c.id ? '' : c.id; }
                function open(c, e) { const t = destination(c, e); if (t) emit('open-evidence', t); }
                const sides = c => ['OLD', 'NEW'].map(side => ({side, items: arr(c.evidence).filter(e => e.side === side)}));
                return {list, selectedKey, selected, view, error, loading, expandedId, failedImages, cards, counts,
                    ownList, toggle, open, sides, key, title, destination,
                    parameterRows: c => parameterTableRows(c, view.value?.original),
                    reportBase, analysis, uploading, uploadMessage, uploadErrors, uploadAnalysis,
                    modelRun, running, starting, ready, download, makeReport, recompute, cancelRun, elapsed, pollRun,
                    rowId: (c, i) => 'ИТ-' + String(i + 1).padStart(3, '0'),
                    presentSources: c => [...new Set(arr(c.evidence).map(e => e.source_type).filter(Boolean))],
                    channels: CHANNELS, scopes: SCOPES, bases: BASES, paramStatus: PARAM_STATUS, decisions: DECISIONS,
                    claims: CLAIMS};
            },
            template: `
            <section v-if="list.length" class="pcc" aria-label="Итоговые карточки Консолидатора">
                <div class="pc-run-banner pcc-bar" role="group" aria-label="Итоговые карточки">
                    <strong>Итоговые карточки</strong>
                    <span>Консолидатор (теневой режим, только просмотр) · прогон-источник {{ selected?.source_run_id?.slice(0, 8) }}
                        <template v-if="selected?.source_label"> · {{ selected.source_label }}</template>
                        <template v-if="selected && !selected.source_is_current_run"> · не текущий прогон пары</template>
                        · {{ selected?.model }} {{ selected?.reasoning }}</span>
                    <select v-if="list.length > 1" v-model="selectedKey" aria-label="Консолидация">
                        <option v-for="x in list" :key="key(x)" :value="key(x)">{{ x.source_run_id.slice(0, 8) }} · {{ x.completed_at }}</option>
                    </select>
                    <div v-if="reportBase" class="pcc-report" role="group" aria-label="Отчёт Excel">
                        <button type="button" class="btn btn-sm btn-primary" @click="download('zip')"
                            title="Архив: Excel с готовыми результатами + папки «Было» и «Стало» с PDF и MD каждого документа, без запуска модели">Классический отчёт</button>
                        <button type="button" class="btn btn-sm btn-secondary" :disabled="running || starting" @click="makeReport"
                            :title="ready ? 'Скачать заполненный отчёт' : 'Разбор по корзинам моделью Claude Opus 5.5, затем отчёт'">
                            {{ running || starting ? 'ИИ-разбор выполняется…' : 'Отчёт с ИИ-разбором' }}</button>
                        <button v-if="running" type="button" class="btn btn-sm btn-secondary" @click="cancelRun">Остановить</button>
                        <button v-if="analysis?.available && !running" type="button" class="btn btn-sm btn-secondary"
                            @click="recompute">Пересчитать моделью</button>
                        <label v-if="!running" class="btn btn-sm btn-secondary" :class="{'is-disabled': uploading}"
                            title="xlsx с заполненным листом «Карточки ИТ» (ручной разбор главнее модели)">{{ uploading ? 'Загрузка…' : 'Загрузить разбор' }}
                            <input type="file" accept=".xlsx" hidden :disabled="uploading" @change="uploadAnalysis"></label>
                        <span v-if="running" class="pc-meta" role="status">Модель разбирает карточки: пачка
                            {{ (modelRun.batches_done || 0) + 1 }} из {{ modelRun.batches_total }}<template
                            v-if="modelRun.current"> ({{ modelRun.current.ids[0] }}…{{ modelRun.current.ids[modelRun.current.ids.length - 1] }})</template>
                            · прошло {{ elapsed }} мин · отчёт скачается сам</span>
                        <span v-else-if="analysis?.available" class="pc-meta">{{ analysis.source === 'model' ? 'Разбор моделью' : 'Разбор инженера' }}:
                            {{ analysis.filled }} из {{ analysis.rows }} карточек · {{ analysis.uploaded_at.slice(0, 10) }}</span>
                        <span v-else class="pc-meta">Классический отчёт доступен сразу. «Отчёт с ИИ-разбором» запустит модель (≈30–40 мин)
                            · <a :href="reportBase + '/report.xlsx'" download>пустой шаблон</a></span>
                    </div>
                </div>
                <div v-if="uploadMessage" :class="uploadErrors.length ? 'sc-shell-error' : 'pc-notice'" role="status">
                    <p>{{ uploadMessage }}</p>
                    <ul v-if="uploadErrors.length"><li v-for="(x, i) in uploadErrors" :key="i">{{ x }}</li></ul>
                </div>
                <p v-if="!running && ['FAILED', 'PARTIAL', 'INTERRUPTED', 'CANCELLED'].includes(modelRun?.status)" class="sc-shell-error" role="status">
                    Разбор моделью {{ {FAILED: 'не удался', PARTIAL: 'выполнен не полностью', INTERRUPTED: 'прерван перезапуском портала',
                    CANCELLED: 'остановлен'}[modelRun.status] }}<template v-if="(modelRun.failed_ids || []).length">: не разобраны
                    {{ modelRun.failed_ids.join(', ') }}</template>. «Отчёт с ИИ-разбором» досчитает недостающее.
                    <template v-if="(modelRun.errors || []).length"><br><small>{{ modelRun.errors.join(' · ') }}</small></template></p>
                <p v-if="analysis?.available && analysis.warnings.length" class="pc-notice" role="status">
                    Замечания к разбору: {{ analysis.warnings.join('; ') }}</p>
                <p v-if="error" class="sc-shell-error" role="alert">{{ error }}</p>
                <p v-if="loading" class="pc-notice" role="status">Загрузка итогового результата…</p>
                <template v-if="ownList && view">
                    <div class="pc-table-scroll" tabindex="0" aria-label="Таблица итоговых изменений">
                    <table class="pc-table">
                        <colgroup><col class="pc-col-id"><col class="pc-col-summary"><col class="pc-col-old">
                            <col class="pc-col-new"><col class="pc-col-source"><col class="pc-col-status"><col class="pc-col-action"></colgroup>
                        <thead><tr><th scope="col">ID</th><th scope="col">Изменение</th><th scope="col">Было</th>
                            <th scope="col">Стало</th><th scope="col">Источник</th><th scope="col">Статус</th>
                            <th scope="col"><span class="pc-sr-only">Подробности</span></th></tr></thead>
                        <tbody><template v-for="(c, i) in cards" :key="c.id">
                        <tr class="pc-row" :id="'pcc-' + c.id" :data-kind="c.kind" :data-channel="c.channel || ''"
                            :class="{'is-expanded': expandedId === c.id}" @click="toggle(c)">
                            <td class="pc-row-id" :title="c.id">{{ rowId(c, i) }}</td>
                            <td class="pc-row-summary"><span class="pc-cell-text">{{ title(c) }}</span>
                                <span v-if="c.summary && c.summary !== title(c)" class="pc-cell-text">{{ c.summary }}</span>
                                <span class="pc-cell-secondary" v-if="c.kind === 'CONSOLIDATED'">Сводная · {{ c.lineage.members.length }} исходных</span>
                                <span class="pc-cell-secondary" v-else>Исходная карточка {{ c.projectchange_id || c.id }}</span></td>
                            <td><span class="pc-cell-text">{{ c.old_state || 'Состояние не установлено' }}</span></td>
                            <td><span class="pc-cell-text">{{ c.new_state || 'Состояние не установлено' }}</span></td>
                            <td><div class="pc-row-sources"><span v-for="s in presentSources(c)" :key="s" class="pc-source is-present">{{ s }}</span></div></td>
                            <td><span v-if="c.channel" class="pc-status" :class="'pcc-channel--' + c.channel.toLowerCase()">{{ channels[c.channel] || c.channel }}</span></td>
                            <td><button type="button" class="pc-expand" :aria-label="(expandedId === c.id ? 'Свернуть ' : 'Раскрыть ') + rowId(c, i)"
                                :aria-expanded="expandedId === c.id" :aria-controls="'pcc-details-' + c.id"
                                @click.stop="toggle(c)">{{ expandedId === c.id ? '−' : '+' }}</button></td>
                        </tr>
                        <tr v-if="expandedId === c.id" class="pc-expanded-row"><td colspan="7">
                        <div :id="'pcc-details-' + c.id" class="pc-card pcc-card pcc-expanded-details">
                        <p v-if="c.region_id" class="pc-meta">Регион {{ c.region_id }}</p>
                        <p v-if="c.decision && decisions[c.decision]" class="pc-meta">{{ decisions[c.decision] }}</p>
                        <p v-if="c.flags && c.flags.includes('COMPOSITE_CARD')" class="pc-review">Карточка смешивает несколько событий — не объединялась, нужна проверка инженера.</p>
                        <p v-if="c.claim_status && claims[c.claim_status]" class="pc-review">{{ claims[c.claim_status] }}</p>
                        <div class="pc-card-context">
                            <span>Охват: {{ scopes[c.scope?.scope_type] || '—' }}<template v-if="bases[c.scope?.scope_basis]"> ({{ bases[c.scope.scope_basis] }})</template></span>
                            <span v-if="(c.scope?.locations || c.locations || []).length">Места: {{ (c.scope?.locations || c.locations).join('; ') }}</span>
                            <span v-if="c.system_designations && c.system_designations.length">Системы: {{ c.system_designations.join(', ') }}</span>
                        </div>
                        <div v-if="c.parameters && c.parameters.length" class="pcc-params-wrap">
                        <table class="pcc-params">
                            <thead>
                                <tr><th rowspan="2" class="pcc-param-name">Характеристика</th>
                                    <th colspan="4" class="pcc-side pcc-side--old">Было (ПД)</th>
                                    <th colspan="4" class="pcc-side pcc-side--new">Стало (РД)</th></tr>
                                <tr><template v-for="side in ['old', 'new']" :key="side">
                                    <th :class="'pcc-col-value pcc-side--' + side">Значение</th><th :class="'pcc-side--' + side">Шифр</th>
                                    <th :class="'pcc-side--' + side">Источник</th><th :class="'pcc-col-pages pcc-side--' + side">Страница по PDF</th>
                                </template></tr>
                            </thead>
                            <tbody><template v-for="(p, i) in parameterRows(c)" :key="i">
                            <tr v-if="p.row === 'group'" class="pcc-param-group">
                                <td class="pcc-param-name"><strong>{{ p.name }}</strong><small v-if="p.location"> · {{ p.location }}</small></td>
                                <td colspan="8"><span class="pcc-param-status">{{ paramStatus[p.status] || p.status }}</span>
                                    <span class="pc-meta">значения по каждому месту — ниже</span></td>
                            </tr>
                            <tr v-else :class="p.row === 'place' ? 'pcc-param-place' : 'pcc-param-row'">
                                <td class="pcc-param-name"><template v-if="p.row === 'place'"><span class="pcc-place-mark">место</span> </template>{{ p.name }}<small v-if="p.location"> · {{ p.location }}</small>
                                    <div v-if="c.kind === 'CONSOLIDATED' && p.row === 'param'" class="pc-meta">По местам: {{ paramStatus[p.status] || p.status }}</div></td>
                                <template v-for="side in ['OLD', 'NEW']" :key="side">
                                    <td :class="'pcc-col-value pcc-side--' + side.toLowerCase()">{{ p[side === 'OLD' ? 'old' : 'new'] ? p[side === 'OLD' ? 'old' : 'new'] + (p.unit ? ' ' + p.unit : '') : '—' }}</td>
                                    <td :class="'pcc-param-cipher pcc-side--' + side.toLowerCase()"><div v-for="(s, j) in p.sources[side]" :key="j" class="pcc-src">{{ s.cipher }}</div><template v-if="!p.sources[side].length">—</template></td>
                                    <td :class="'pcc-param-source pcc-side--' + side.toLowerCase()"><div v-for="(s, j) in p.sources[side]" :key="j" class="pcc-src"><span :class="'pcc-src-type pcc-src-type--' + s.sourceType.toLowerCase()">{{ s.sourceType }}</span></div><template v-if="!p.sources[side].length">—</template></td>
                                    <td :class="'pcc-param-pages pcc-col-pages pcc-side--' + side.toLowerCase()"><div v-for="(s, j) in p.sources[side]" :key="j" class="pcc-src">{{ s.pages }}</div><template v-if="!p.sources[side].length">—</template></td>
                                </template>
                            </tr>
                            </template></tbody>
                        </table>
                        </div>
                        <div v-for="(x, n) in c.conflicts || []" :key="'k' + n" class="pc-conflict" role="status">
                            <strong>{{ x.source === 'MEMBER_DISAGREEMENT' ? 'Исходные карточки расходятся' : 'Противоречие / недоказанность источников' }}</strong>
                            <p>{{ x.statement }}</p>
                            <p v-if="x.hint" class="pc-meta">Подсказка {{ x.hint.hint_ref || x.hint.hint_key }} · {{ x.hint.subject }}</p>
                            <p v-if="x.hint && x.hint.text && x.hint.text !== x.statement" class="pc-meta">{{ x.hint.text }}</p>
                            <span v-for="e in (x.hint?.evidence || [])" :key="e.id"><button class="pc-link" :disabled="!destination(c, e)"
                                @click="open({...c, evidence: x.hint.evidence}, e)">{{ e.side }} · стр. {{ e.page }} ↗</button></span>
                        </div>
                        <p v-if="c.possible_same_event && c.possible_same_event.length" class="pc-meta">Возможно то же событие, что: {{ c.possible_same_event.join(', ') }}</p>
                        <div class="pc-evidence-bar"><button class="pc-link" :disabled="!destination(c)" @click="open(c)">Открыть в PDF ↗</button>
                            <span>Доказательства · {{ (c.evidence || []).length }}</span></div>
                            <div class="pc-evidence-sides"><section v-for="s in sides(c)" :key="s.side" :aria-label="s.side + ' доказательства'">
                                <strong class="pc-side-label">{{ s.side }} · {{ s.items.length }}</strong>
                                <div class="pc-evidence-grid"><figure v-for="e in s.items" :key="e.id">
                                    <a v-if="e.image_url && !failedImages[e.image_url]" class="pc-crop" :href="e.image_url" target="_blank" rel="noopener">
                                        <img :src="e.image_url" :alt="e.short_explanation_ru" loading="lazy" @error="failedImages[e.image_url] = true"></a>
                                    <p v-else class="pc-missing">Фрагмент недоступен.</p>
                                    <figcaption><b>{{ e.document?.label }} · стр. {{ e.page }} · {{ e.source_type }}</b>
                                        <blockquote v-if="e.quote">{{ e.quote }}</blockquote>
                                        <button class="pc-link" :disabled="!destination(c, e)" @click="open(c, e)">Открыть в PDF</button></figcaption>
                                </figure></div>
                            </section></div>
                            <details v-if="c.kind === 'CONSOLIDATED'" class="pc-details pcc-lineage">
                                <summary>Из каких исходных карточек собрано · {{ c.lineage.members.length }}</summary>
                                <ul><li v-for="m in c.lineage.members" :key="m.id"><b>{{ m.id }}</b> · {{ m.region_id }} · {{ m.title }}<br><small>{{ m.summary }}</small></li></ul>
                                <p class="pc-meta">Почему одно событие: {{ c.why_one_event }}</p>
                                <p class="pc-meta">Проверка различий: {{ c.lineage.distinguishing_check }}</p>
                                <p class="pc-meta" v-if="c.manifestations && c.manifestations.length">Проявления:
                                    <span v-for="(m, i) in c.manifestations" :key="i">{{ m.locations.join(', ') }}<template v-if="m.note"> ({{ m.note }})</template>; </span></p>
                            </details>
                        </div>
                        </td></tr>
                        </template></tbody>
                    </table>
                    </div>
                </template>
            </section>`,
        });
    }
    return {CHANNELS, SCOPES, BASES, PARAM_STATUS, DECISIONS, pick, key, summary, destination, title, parameterSources, parameterTableRows, register};
}));
