(function (root, factory) {
    const api = factory(root);
    if (typeof module === 'object' && module.exports) module.exports = api;
    root.ProjectChangeConsolidation = api;
}(typeof globalThis !== 'undefined' ? globalThis : this, function (root) {
    'use strict';
    // «Исходные | Итоговые»: read-only view of COMPLETED shadow consolidations of the opened pair.
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

    function register(app) {
        app.component('project-change-consolidation', {
            props: {sessionId: {type: String, default: ''}, pairId: {type: String, default: ''}},
            emits: ['panel', 'open-evidence'],
            setup(props, {emit}) {
                const {ref, computed, watch} = root.Vue;
                const list = ref([]);
                const selectedKey = ref('');
                const mode = ref('');
                const view = ref(null);
                const error = ref('');
                const loading = ref(false);
                const expandedId = ref('');
                const failedImages = ref({});
                let token = 0;
                const base = computed(() => '/api/stage-comparison/sessions/' + encodeURIComponent(props.sessionId)
                    + '/pairs/' + encodeURIComponent(props.pairId) + '/consolidated');
                const selected = computed(() => list.value.find(x => key(x) === selectedKey.value) || null);
                const ownList = computed(() => mode.value === 'consolidated'
                    || (mode.value === 'original' && selected.value && !selected.value.source_is_current_run));
                const cards = computed(() => !view.value ? [] : mode.value === 'consolidated'
                    ? arr(view.value.consolidated) : arr(view.value.original).map(c => ({...c, kind: 'SOURCE_CARD'})));
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
                        uploadMessage.value = 'Разбор загружен: ' + data.rows + ' карточек. «Выгрузить отчёт» выдаст его вместе с «Итоговыми».';
                    } catch (e) {
                        uploadMessage.value = 'Не удалось загрузить разбор: ' + String(e.message || e);
                    } finally { uploading.value = false; input.value = ''; }
                }
                watch(reportBase, loadAnalysis, {immediate: true});
                watch(ownList, value => emit('panel', Boolean(value)), {immediate: true});
                async function loadList() {
                    const my = ++token;
                    list.value = []; view.value = null; mode.value = ''; error.value = ''; selectedKey.value = '';
                    if (!props.sessionId || !props.pairId) return;
                    try {
                        const response = await fetch(base.value);
                        if (my !== token) return;
                        if (!response.ok) return;
                        const data = await response.json();
                        if (my !== token || data?.schema !== 'projectchange-consolidations/1') return;
                        list.value = arr(data.consolidations);
                        selectedKey.value = key(pick(list.value));
                    } catch (_) { /* no consolidation: the original view stays as it is */ }
                }
                async function loadView() {
                    const entry = selected.value;
                    if (!entry) return false;
                    if (view.value && view.value.source_run_id === entry.source_run_id
                            && view.value.consolidator_run_id === entry.consolidator_run_id) return true;
                    const my = ++token;
                    loading.value = true; error.value = '';
                    try {
                        const response = await fetch(base.value + '/' + encodeURIComponent(entry.source_run_id)
                            + '/' + encodeURIComponent(entry.consolidator_run_id));
                        const data = await response.json();
                        if (my !== token) return false;
                        if (!response.ok || data?.schema !== 'projectchange-consolidated-view/1'
                                || data.source_run_id !== entry.source_run_id
                                || data.source_result_sha256 !== entry.source_result_sha256
                                || data.shadow_result_sha256 !== entry.shadow_result_sha256)
                            throw new Error('Итоговый результат недоступен.');
                        view.value = data;
                        return true;
                    } catch (e) {
                        if (my === token) { error.value = String(e.message || e); view.value = null; }
                        return false;
                    } finally { if (my === token) loading.value = false; }
                }
                async function show(next) {
                    if (next === mode.value) return;
                    if (next && !(await loadView())) { mode.value = ''; return; }
                    mode.value = next; expandedId.value = '';
                }
                watch(() => [props.sessionId, props.pairId], loadList, {immediate: true});
                watch(selectedKey, () => { view.value = null; const m = mode.value; mode.value = ''; if (m) show(m); });
                function toggle(c) { expandedId.value = expandedId.value === c.id ? '' : c.id; }
                function open(c, e) { const t = destination(c, e); if (t) emit('open-evidence', t); }
                const sides = c => ['OLD', 'NEW'].map(side => ({side, items: arr(c.evidence).filter(e => e.side === side)}));
                return {list, selectedKey, selected, mode, view, error, loading, expandedId, failedImages, cards, counts,
                    ownList, show, toggle, open, sides, key, title, destination,
                    reportBase, analysis, uploading, uploadMessage, uploadErrors, uploadAnalysis,
                    rowId: (c, i) => mode.value === 'consolidated' ? 'ИТ-' + String(i + 1).padStart(3, '0') : (c.projectchange_id || c.id),
                    presentSources: c => [...new Set(arr(c.evidence).map(e => e.source_type).filter(Boolean))],
                    channels: CHANNELS, scopes: SCOPES, bases: BASES, paramStatus: PARAM_STATUS, decisions: DECISIONS,
                    claims: CLAIMS};
            },
            template: `
            <section v-if="list.length" class="pcc" aria-label="Итоговые карточки Консолидатора">
                <div class="pc-run-banner pcc-bar" role="group" aria-label="Исходные или итоговые карточки">
                    <strong>Итоговые карточки</strong>
                    <span>Консолидатор (теневой режим, только просмотр) · прогон-источник {{ selected?.source_run_id?.slice(0, 8) }}
                        <template v-if="selected?.source_label"> · {{ selected.source_label }}</template>
                        <template v-if="selected && !selected.source_is_current_run"> · не текущий прогон пары</template>
                        · {{ selected?.model }} {{ selected?.reasoning }}</span>
                    <select v-if="list.length > 1" v-model="selectedKey" aria-label="Консолидация">
                        <option v-for="x in list" :key="key(x)" :value="key(x)">{{ x.source_run_id.slice(0, 8) }} · {{ x.completed_at }}</option>
                    </select>
                    <div class="pcc-switch" role="tablist">
                        <button type="button" role="tab" class="btn btn-sm btn-secondary" :aria-selected="String(mode === 'original')"
                            :class="{'pc-selected': mode === 'original'}" @click="show('original')">Исходные</button>
                        <button type="button" role="tab" class="btn btn-sm btn-secondary" :aria-selected="String(mode === 'consolidated')"
                            :class="{'pc-selected': mode === 'consolidated'}" @click="show('consolidated')">Итоговые</button>
                        <button v-if="mode" type="button" class="pc-link" @click="show('')">Скрыть</button>
                    </div>
                    <div v-if="reportBase" class="pcc-report" role="group" aria-label="Отчёт Excel">
                        <a class="btn btn-sm btn-secondary" :href="reportBase + '/report.xlsx'" download
                            :title="analysis?.available ? 'Итоговые + загруженный разбор по корзинам' : 'Итоговые + шаблон разбора по корзинам'">Выгрузить отчёт</a>
                        <label class="btn btn-sm btn-secondary" :class="{'is-disabled': uploading}"
                            title="xlsx с заполненным листом «Карточки ИТ»">{{ uploading ? 'Загрузка…' : 'Загрузить разбор' }}
                            <input type="file" accept=".xlsx" hidden :disabled="uploading" @change="uploadAnalysis"></label>
                        <span v-if="analysis?.available" class="pc-meta">Разбор: {{ analysis.filled }} из {{ analysis.rows }} карточек
                            заполнено · {{ analysis.uploaded_at.slice(0, 10) }}</span>
                        <span v-else class="pc-meta">Разбор по корзинам не загружен — в отчёте будет шаблон</span>
                    </div>
                </div>
                <div v-if="uploadMessage" :class="uploadErrors.length ? 'sc-shell-error' : 'pc-notice'" role="status">
                    <p>{{ uploadMessage }}</p>
                    <ul v-if="uploadErrors.length"><li v-for="(x, i) in uploadErrors" :key="i">{{ x }}</li></ul>
                </div>
                <p v-if="analysis?.available && analysis.warnings.length" class="pc-notice" role="status">
                    Замечания к разбору: {{ analysis.warnings.join('; ') }}</p>
                <p v-if="error" class="sc-shell-error" role="alert">{{ error }}</p>
                <p v-if="loading" class="pc-notice" role="status">Загрузка итогового результата…</p>
                <p v-if="mode === 'original' && selected?.source_is_current_run" class="pc-notice" role="status">
                    Исходные карточки — это результат текущего прогона пары ниже (авторитетный результат Dedupe).</p>
                <template v-if="ownList && view">
                    <p class="pc-notice" role="status" v-if="mode === 'consolidated'">Итоговые карточки собраны Консолидатором из исходных
                        ({{ counts.original }} → {{ counts.consolidated }}; объединено {{ counts.absorbed }} карточек в {{ counts.merged }};
                        документальных {{ counts.documentary }}; на проверку {{ counts.review }}). Исходный результат не изменён.</p>
                    <p class="pc-notice" role="status" v-else>Исходные карточки прогона-источника {{ view.source_run_id.slice(0, 8) }}
                        ({{ counts.original }}) — авторитетный результат Dedupe этого прогона.</p>
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
                        <table v-if="c.parameters && c.parameters.length" class="pcc-params">
                            <thead><tr><th>Характеристика</th><th>Было</th><th>Стало</th><th v-if="c.kind === 'CONSOLIDATED'">По местам</th></tr></thead>
                            <tbody><tr v-for="(p, i) in c.parameters" :key="i">
                                <td>{{ p.name }}<small v-if="p.location"> · {{ p.location }}</small></td>
                                <td>{{ p.old ? p.old + (p.unit ? ' ' + p.unit : '') : '—' }}</td>
                                <td>{{ p.new ? p.new + (p.unit ? ' ' + p.unit : '') : '—' }}</td>
                                <td v-if="c.kind === 'CONSOLIDATED'"><span>{{ paramStatus[p.status] || p.status }}</span>
                                    <template v-if="p.status !== 'CONSISTENT'"><br><small v-for="(v, j) in p.values" :key="j">{{ v.location || v.card_id }}: {{ v.old_value }} → {{ v.new_value }} {{ v.unit }}<br></small></template></td>
                            </tr></tbody>
                        </table>
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
    return {CHANNELS, SCOPES, BASES, PARAM_STATUS, DECISIONS, pick, key, summary, destination, title, register};
}));
