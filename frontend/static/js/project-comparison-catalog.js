(function (root) {
    'use strict';
    // Pair overview: saved results plus current source pairs without a result.
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
        return [e.model_display || e.model || '—', e.reasoning].filter(Boolean).join(' · ');
    }
    function pairEntries(results, rows, objectId, objectName, sessionId) {
        const catalog = (results || []).filter(e => !objectId || e.object_id === objectId);
        const used = new Set();
        const entries = [];
        for (const row of rows || []) {
            if (!row.left && !row.right) continue;
            const matches = catalog.filter(e => !used.has(e.catalog_entry_id) && row.left && row.right
                && ((row.pair?.id && e.open?.session_id === sessionId && e.open?.pair_id === row.pair.id)
                    || (row.left.pdf_path && row.right.pdf_path
                        && e.documents?.old?.pdf_path === row.left.pdf_path
                        && e.documents?.new?.pdf_path === row.right.pdf_path)));
            if (matches.length) {
                matches.forEach(e => { entries.push(e); used.add(e.catalog_entry_id); });
                continue;
            }
            const title = row.left?.document_code || row.left?.filename || row.right?.document_code || row.right?.filename;
            entries.push({catalog_entry_id: 'pair:' + JSON.stringify([row.left?.pdf_path, row.right?.pdf_path, row.index]),
                pending_pair: row, object_id: objectId, object_name: objectName,
                title: title + (row.group && row.members?.length ? ' · сборка из ' + row.members.length : ''),
                documents: {old: row.left || {}, new: row.right || {}}, engine: {}, counts: {}});
        }
        // Keep earlier source versions and saved variants accessible too.
        return entries.concat(catalog.filter(e => !used.has(e.catalog_entry_id)));
    }

    root.ProjectComparisonCatalog = {
        date, model, pairEntries,
        register(app) {
            app.component('project-comparison-catalog', {
                props: {activeEntryId: {type: String, default: ''}, opening: Boolean, error: {type: String, default: ''},
                    // Current object: the catalog lists only its results (empty = no object chosen, show all).
                    objectId: {type: String, default: ''}, objectName: {type: String, default: ''},
                    sessionId: {type: String, default: ''}, pairRows: {type: Array, default: () => []},
                    pairsLoading: Boolean, canStart: {type: Function, default: () => false},
                    pairStatus: {type: Function, default: () => 'Не запускалось'}},
                emits: ['open', 'start'],
                setup(props, {emit}) {
                    const {ref, computed, onMounted} = root.Vue;
                    const catalog = ref(null);
                    const loading = ref(false);
                    const loadError = ref('');
                    const entries = computed(() => pairEntries(catalog.value?.entries, props.pairsLoading ? [] : props.pairRows,
                        props.objectId, props.objectName, props.sessionId));
                    async function load() {
                        loading.value = true; loadError.value = '';
                        try {
                            const response = await fetch(API);
                            const data = await response.json().catch(() => ({}));
                            if (!response.ok) throw new Error(data.detail || ('HTTP ' + response.status));
                            if (data.schema !== 'project-comparison-catalog/1' || !Array.isArray(data.entries))
                                throw new Error('Неверный контракт каталога.');
                            catalog.value = data;
                        } catch (error) {
                            loadError.value = 'Каталог недоступен: ' + String(error.message || error);
                        } finally { loading.value = false; }
                    }
                    function startDisabled(entry) {
                        return loading.value || !!loadError.value || props.pairsLoading || props.opening
                            || !props.canStart?.(entry.pending_pair);
                    }
                    function start(entry) {
                        if (entry.pending_pair && !startDisabled(entry)) emit('start', entry.pending_pair);
                    }
                    onMounted(load);
                    return {catalog, entries, loading, loadError, load, date, model, start, startDisabled,
                        sourceLabel: s => SOURCE_LABEL[s] || s,
                        open: (entry, options) => (options ? emit('open', entry, options) : emit('open', entry))};
                },
                template: `
                <section class="pc-catalog" id="pc-catalog" aria-label="Сравнения проектов">
                    <div v-if="loadError" class="pc-notice" role="alert">{{ loadError }}
                        <button type="button" class="pc-link" @click="load">Повторить</button></div>
                    <div v-if="error" class="pc-notice" role="alert">{{ error }}</div>
                    <p v-if="loading && !catalog" class="pc-catalog__empty">Загрузка сравнений…</p>
                    <p v-else-if="catalog && !pairsLoading && !entries.length" class="pc-catalog__empty">В этом объекте пока нет пар для сравнения.</p>
                    <div v-if="entries.length" class="pc-catalog__scroll">
                    <table class="pc-catalog__table">
                        <thead><tr><th>Проект / раздел</th><th>OLD → NEW</th><th>Модель</th><th>V3</th><th>Дата</th>
                            <th class="num">Изменения</th><th class="num">Требуют проверки</th><th>Статус</th><th></th></tr></thead>
                        <tbody>
                        <tr v-for="entry in entries" :key="entry.catalog_entry_id" :data-entry-id="entry.catalog_entry_id"
                            :class="{'is-active': entry.catalog_entry_id === activeEntryId}">
                            <td><b>{{ entry.title }}</b><small>{{ entry.object_name }}</small></td>
                            <td class="pc-catalog__docs">{{ entry.documents.old.document_code || entry.documents.old.filename || '—' }}
                                → {{ entry.documents.new.document_code || entry.documents.new.filename || '—' }}</td>
                            <td class="pc-catalog__model">{{ model(entry) }}</td>
                            <td>{{ entry.engine.engine_version || '—' }}</td>
                            <td :title="entry.frozen_at_source">{{ date(entry.frozen_at) }}</td>
                            <td class="num pc-catalog__count">{{ entry.counts.projectchanges ?? '—' }}</td>
                            <td class="num">{{ entry.counts.unresolved_hints ?? '—' }}</td>
                            <td v-if="entry.pending_pair"><span>{{ pairStatus(entry.pending_pair) }}</span></td>
                            <td v-else><span class="pc-catalog__status">Заморожен</span>
                                <small>{{ sourceLabel(entry.result_source) }}<template v-if="entry.variants_in_pair > 1">
                                    · {{ entry.is_current ? 'Текущий' : (entry.is_primary ? 'основной' : 'вариант') }}</template></small></td>
                            <td v-if="entry.pending_pair" class="pc-catalog__actions">
                                <button type="button" class="btn btn-sm btn-primary" :disabled="startDisabled(entry)"
                                    @click="start(entry)">Запустить сравнение</button>
                            </td>
                            <td v-else class="pc-catalog__actions">
                                <button type="button" class="btn btn-sm btn-primary" :disabled="opening || !entry.open.available"
                                    :title="entry.open.available ? 'Открыть в сравнении проекта' : 'В ленте пары сейчас показан другой прогон'"
                                    @click="open(entry)">Открыть</button>
                                <button type="button" class="btn btn-sm btn-secondary" :disabled="opening || !entry.open.available"
                                    :title="entry.open.available ? 'Смысловые блоки этого результата в шаге 2' : 'В ленте пары сейчас показан другой прогон'"
                                    @click="open(entry, {target: 'blocks'})">Смысловые блоки в шаге 2</button>

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
