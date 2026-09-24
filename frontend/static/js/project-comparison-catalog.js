(function (root) {
    'use strict';
    // Catalog of accepted, frozen Project Comparison results. Read-only: it
    // never starts an analysis; opening an entry uses the normal comparison UI.
    const API = '/api/project-comparison/catalog';
    const COLLAPSE_KEY = 'stage-comparison:catalog-collapsed';
    const SOURCE_LABEL = {LIVE_RUN: 'прогон V3', SEALED_SNAPSHOT: 'запечатанный снимок'};
    const DIAG_LABEL = {FAILED: 'Ошибка', TRANSPORT_FAILED: 'Сбой транспорта', CANCELLED: 'Остановлен',
        PARTIAL: 'Не завершён', DIAGNOSTIC_ONLY: 'Только диагностика'};

    function date(value) {
        if (!value) return '—';
        const d = new Date(value);
        return Number.isNaN(d.getTime()) ? '—' : d.toLocaleDateString('ru-RU');
    }
    function model(entry) {
        const e = entry.engine || {};
        return [e.model_display || e.model || '—', e.reasoning].filter(Boolean).join(' · ');
    }
    function readCollapsed() {
        try { return localStorage.getItem(COLLAPSE_KEY) === '1'; } catch (_) { return false; }
    }
    function writeCollapsed(value) {
        try { localStorage.setItem(COLLAPSE_KEY, value ? '1' : '0'); } catch (_) { /* per-viewer convenience only */ }
    }

    root.ProjectComparisonCatalog = {
        date, model,
        register(app) {
            app.component('project-comparison-catalog', {
                props: {activeEntryId: {type: String, default: ''}, opening: Boolean, error: {type: String, default: ''}},
                emits: ['open'],
                setup(props, {emit}) {
                    const {ref, computed, onMounted} = root.Vue;
                    const catalog = ref(null);
                    const loading = ref(false);
                    const loadError = ref('');
                    const collapsed = ref(readCollapsed());
                    const diagnostics = ref(null);
                    const diagnosticsError = ref('');
                    const entries = computed(() => catalog.value?.entries || []);
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
                    async function loadDiagnostics(event) {
                        if (!event.target.open || diagnostics.value) return;
                        try {
                            const response = await fetch(API + '?diagnostics=true');
                            const data = await response.json().catch(() => ({}));
                            if (!response.ok) throw new Error(data.detail || ('HTTP ' + response.status));
                            diagnostics.value = Array.isArray(data.diagnostics) ? data.diagnostics : [];
                        } catch (error) { diagnosticsError.value = String(error.message || error); }
                    }
                    function toggle(event) {
                        collapsed.value = !event.target.open;
                        writeCollapsed(collapsed.value);
                    }
                    function openHumanMapping(entry) {
                        const url = entry.human_mapping?.available ? entry.human_mapping.url : '';
                        if (url) root.open(url, '_blank', 'noopener');
                    }
                    onMounted(load);
                    return {catalog, entries, loading, loadError, collapsed, diagnostics, diagnosticsError,
                        load, loadDiagnostics, toggle, openHumanMapping, date, model,
                        sourceLabel: s => SOURCE_LABEL[s] || s, diagLabel: s => DIAG_LABEL[s] || s,
                        // options {target:'blocks'} opens stage 2 on the semantic blocks (C13); emits stays ['open'].
                        open: (entry, options) => (options ? emit('open', entry, options) : emit('open', entry))};
                },
                template: `
                <details class="pc-catalog" id="pc-catalog" :open="!collapsed" @toggle="toggle">
                    <summary><strong>Проверенные сравнения</strong>
                        <small v-if="catalog"> · {{ entries.length }}</small>
                        <small class="pc-catalog__hint">завершённые и замороженные результаты · только просмотр</small></summary>
                    <div v-if="loadError" class="pc-notice" role="alert">{{ loadError }}
                        <button type="button" class="pc-link" @click="load">Повторить</button></div>
                    <div v-if="error" class="pc-notice" role="alert">{{ error }}</div>
                    <p v-if="loading && !catalog" class="pc-catalog__empty">Загрузка каталога…</p>
                    <p v-else-if="catalog && !entries.length" class="pc-catalog__empty">Завершённых результатов пока нет.</p>
                    <div v-if="entries.length" class="pc-catalog__scroll">
                    <table class="pc-catalog__table">
                        <thead><tr><th>Проект / раздел</th><th>OLD → NEW</th><th>Модель</th><th>V3</th><th>Дата</th>
                            <th class="num">Изменения</th><th class="num">Требуют проверки</th><th>Статус</th><th></th></tr></thead>
                        <tbody>
                        <tr v-for="entry in entries" :key="entry.catalog_entry_id" :data-entry-id="entry.catalog_entry_id"
                            :class="{'is-active': entry.catalog_entry_id === activeEntryId}">
                            <td><b>{{ entry.title }}</b><small>{{ entry.object_name }}</small></td>
                            <td class="pc-catalog__docs">{{ entry.documents.old.document_code || entry.documents.old.filename }}
                                → {{ entry.documents.new.document_code || entry.documents.new.filename }}</td>
                            <td class="pc-catalog__model">{{ model(entry) }}</td>
                            <td>{{ entry.engine.engine_version || '—' }}</td>
                            <td :title="entry.frozen_at_source">{{ date(entry.frozen_at) }}</td>
                            <td class="num pc-catalog__count">{{ entry.counts.projectchanges }}</td>
                            <td class="num">{{ entry.counts.unresolved_hints ?? '—' }}</td>
                            <td><span class="pc-catalog__status">Заморожен</span>
                                <small>{{ sourceLabel(entry.result_source) }}<template v-if="entry.variants_in_pair > 1">
                                    · {{ entry.is_current ? 'Текущий' : (entry.is_primary ? 'основной' : 'вариант') }}</template></small></td>
                            <td class="pc-catalog__actions">
                                <button type="button" class="btn btn-sm btn-primary" :disabled="opening || !entry.open.available"
                                    :title="entry.open.available ? 'Открыть в сравнении проекта' : 'В ленте пары сейчас показан другой прогон'"
                                    @click="open(entry)">Открыть</button>
                                <button type="button" class="btn btn-sm btn-secondary" :disabled="opening || !entry.open.available"
                                    :title="entry.open.available ? 'Смысловые блоки этого результата в шаге 2' : 'В ленте пары сейчас показан другой прогон'"
                                    @click="open(entry, {target: 'blocks'})">Смысловые блоки в шаге 2</button>
                                <button type="button" class="btn btn-sm btn-secondary" :disabled="!entry.human_mapping.available"
                                    :title="entry.human_mapping.available ? 'Регионов: ' + entry.human_mapping.regions : 'Human Mapping этого результата недоступен'"
                                    @click="openHumanMapping(entry)">Human Mapping<template v-if="entry.human_mapping.available">
                                    · {{ entry.human_mapping.regions }}</template></button>
                            </td>
                        </tr>
                        </tbody>
                    </table>
                    </div>
                    <details class="pc-catalog__diagnostics" @toggle="loadDiagnostics">
                        <summary>Диагностика</summary>
                        <p class="pc-catalog__empty">Не результаты сравнения: незавершённые и отклонённые прогоны. Карточки изменений не показываются.</p>
                        <div v-if="diagnosticsError" class="pc-notice" role="alert">{{ diagnosticsError }}</div>
                        <p v-else-if="diagnostics && !diagnostics.length" class="pc-catalog__empty">Нет.</p>
                        <table v-else-if="diagnostics" class="pc-catalog__table pc-catalog__table--diag">
                            <thead><tr><th>Статус</th><th>Причина</th><th>Источник</th><th>Прогон</th><th>Модель</th></tr></thead>
                            <tbody><tr v-for="(d, index) in diagnostics" :key="index">
                                <td>{{ diagLabel(d.label) }}</td><td><code>{{ d.reason }}</code></td>
                                <td>{{ d.session_id ? d.session_id + ' / ' + d.pair_id : (d.snapshot || '') }}</td>
                                <td>{{ d.run_id || '—' }}<small v-if="d.engine_version"> · V3 {{ d.engine_version }}</small></td>
                                <td>{{ [d.model_display || d.model, d.reasoning].filter(Boolean).join(' · ') || '—' }}</td>
                            </tr></tbody>
                        </table>
                    </details>
                </details>`,
            });
        },
    };
}(typeof globalThis !== 'undefined' ? globalThis : this));
